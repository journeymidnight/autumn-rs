//! System test — manager leader failover preserves full state.
//! System test — manager crash during split, state consistent.
//!
//! Both tests stop the leader M1 for real and read the state back from M2
//! once M2 has taken over: a standby answers `GET_REGIONS` with NOT_LEADER,
//! so a standby's replayed regions are not observable over the wire.
//!
//! These tests spawn the `etcd` binary (override via `AUTUMN_TEST_ETCD_BIN`).
//! Marked `#[ignore]` so CI without etcd skips them — run explicitly:
//!   cargo test -p autumn-manager --test system_manager_failover -- --ignored

mod support;

use std::net::SocketAddr;
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;

use support::*;

/// Start an etcd-backed manager whose thread ends when the flag is set. Ending
/// the thread drops its runtime, so its leader keepalive stops without revoking
/// the lease: to the standby this is a crash, and the leader key goes away when
/// the lease expires.
fn start_etcd_manager_stoppable(
    mgr_addr: SocketAddr,
    etcd_endpoint: String,
) -> (ShutdownFlag, std::thread::JoinHandle<()>) {
    let flag = ShutdownFlag::new();
    let flag_thread = flag.clone();
    let handle = std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new_with_etcd(vec![etcd_endpoint])
                .await
                .expect("new manager with etcd");
            let serve = manager.serve(mgr_addr);
            let stop = async {
                while !flag_thread.is_shutdown() {
                    compio::time::sleep(Duration::from_millis(50)).await;
                }
            };
            futures::pin_mut!(serve, stop);
            if let futures::future::Either::Left((r, _)) =
                futures::future::select(serve, stop).await
            {
                panic!("manager serve ended on its own: {r:?}");
            }
        });
    });
    std::thread::sleep(Duration::from_millis(300));
    (flag, handle)
}

fn start_etcd_manager(mgr_addr: SocketAddr, etcd_endpoint: String) {
    // Never stopped: the handle and flag are dropped, the thread serves on.
    drop(start_etcd_manager_stoppable(mgr_addr, etcd_endpoint));
}

/// Stop M1 and wait until M2 serves routing as the leader. M1's lease has a
/// 10 s TTL, so the handover takes about that long.
async fn fail_over(
    m1: (ShutdownFlag, std::thread::JoinHandle<()>),
    mgr2: &RpcClient,
) -> GetRegionsResp {
    let (flag, handle) = m1;
    flag.shutdown();
    handle.join().expect("join M1");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let regions = get_regions(mgr2).await;
        if regions.code == CODE_OK {
            return regions;
        }
        assert_eq!(
            regions.code, CODE_NOT_LEADER,
            "M2 get_regions: {}",
            regions.message
        );
        assert!(
            std::time::Instant::now() < deadline,
            "M2 did not take over within 30 s of M1 stopping"
        );
        compio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ── Manager failover preserves full state ───────────────────────

#[test]
#[ignore] // requires embedded etcd (go runtime)
fn manager_failover_preserves_streams_and_partitions() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_etcd().await;

        // Start M1, extent nodes
        let mgr1_addr = pick_addr();
        let m1 = start_etcd_manager_stoppable(mgr1_addr, etcd_endpoint.clone());
        let mgr1 = RpcClient::connect(mgr1_addr).await.expect("connect mgr1");

        let n1_dir = tempfile::tempdir().expect("n1");
        let n2_dir = tempfile::tempdir().expect("n2");
        let n1_addr = pick_addr();
        let n2_addr = pick_addr();
        start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
        start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

        register_two_nodes(&mgr1, n1_addr, n2_addr, 68).await;

        // Create streams and partition on M1
        let (log, row, meta) = create_three_streams(&mgr1).await;
        upsert_partition(&mgr1, 801, log, row, meta, b"a", b"z").await;

        // Write data via PS connected to M1
        let ps_addr = pick_addr();
        let ps_stop = start_partition_server_stoppable(91, mgr1_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // An etcd-backed leader seeds the built-in namespaces, which turns on
        // the PS's Layer-A check: a key under no registered namespace fails
        // with NamespaceUnknown. `mem/` is a built-in, so no registration.
        for i in 0..10 {
            ps_put(
                &ps,
                801,
                format!("mem/k-{i:02}").as_bytes(),
                format!("v-{i}").as_bytes(),
            )
            .await;
        }

        // Start M2 (as follower while M1 is alive)
        let mgr2_addr = pick_addr();
        start_etcd_manager(mgr2_addr, etcd_endpoint.clone());
        compio::time::sleep(Duration::from_millis(500)).await;

        let mgr2 = RpcClient::connect(mgr2_addr).await.expect("connect mgr2");

        // M2 should have replayed the streams
        let resp = mgr2
            .call(
                MSG_STREAM_INFO,
                rkyv_encode(&StreamInfoReq {
                    stream_ids: vec![log, row, meta],
                }),
            )
            .await
            .expect("m2 stream_info");
        let info: StreamInfoResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(info.code, CODE_OK);
        assert_eq!(
            info.streams.len(),
            3,
            "M2 should have all 3 streams from replay"
        );

        // M2 is a standby: it refuses both writes and routing.
        let resp = mgr2
            .call(
                MSG_CREATE_STREAM,
                rkyv_encode(&CreateStreamReq {
                    replicates: 2,
                    ec_data_shard: 2,
                    ec_parity_shard: 0,
                }),
            )
            .await
            .expect("create on follower");
        let cr: CreateStreamResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(
            cr.code, CODE_NOT_LEADER,
            "writes on follower must be rejected"
        );
        assert_eq!(get_regions(&mgr2).await.code, CODE_NOT_LEADER);

        // M1 dies; M2 takes over with the partition it replayed.
        let regions = fail_over(m1, &mgr2).await;
        assert!(
            regions.regions.iter().any(|(_, r)| r.part_id == 801),
            "M2 should have partition 801 from etcd replay"
        );
        let resp = mgr2
            .call(
                MSG_CREATE_STREAM,
                rkyv_encode(&CreateStreamReq {
                    replicates: 2,
                    ec_data_shard: 2,
                    ec_parity_shard: 0,
                }),
            )
            .await
            .expect("create on new leader");
        let cr: CreateStreamResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(
            cr.code, CODE_OK,
            "new leader must accept writes: {}",
            cr.message
        );

        // The PS knew only M1 and exits the process once its heartbeats have
        // failed for long enough; stop it before that can happen.
        ps_stop.0.shutdown();
        ps_stop.1.join().expect("join PS");
    });
}

// ── Manager crash during split — state consistent ───────────────

#[test]
#[ignore] // requires embedded etcd (go runtime)
fn manager_crash_during_split_state_consistent() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_etcd().await;

        // M1 + extent nodes
        let mgr1_addr = pick_addr();
        let m1 = start_etcd_manager_stoppable(mgr1_addr, etcd_endpoint.clone());
        let mgr1 = RpcClient::connect(mgr1_addr).await.expect("connect mgr1");

        let n1_dir = tempfile::tempdir().expect("n1");
        let n2_dir = tempfile::tempdir().expect("n2");
        let n1_addr = pick_addr();
        let n2_addr = pick_addr();
        start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
        start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

        register_two_nodes(&mgr1, n1_addr, n2_addr, 71).await;

        let (log, row, meta) = create_three_streams(&mgr1).await;
        upsert_partition(&mgr1, 901, log, row, meta, b"a", b"z").await;

        // PS writes and splits via M1
        let ps_addr = pick_addr();
        let ps_stop = start_partition_server_stoppable(92, mgr1_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        for i in 0..10 {
            ps_put(
                &ps,
                901,
                format!("mem/d-{i:02}").as_bytes(),
                format!("v-{i}").as_bytes(),
            )
            .await;
        }
        ps_flush(&ps, 901).await;

        // Split completes on M1
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq {
                    part_id: 901,
                    at_key: None,
                }),
            )
            .await
            .expect("split");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(
            sr.code,
            partition_rpc::CODE_OK,
            "split failed: {}",
            sr.message
        );

        // Verify M1 has 2 partitions
        let regions = get_regions(&mgr1).await;
        assert_eq!(
            regions.regions.len(),
            2,
            "M1 should have 2 partitions after split"
        );
        let right_id = regions
            .regions
            .iter()
            .find(|(_, r)| r.part_id != 901)
            .unwrap()
            .1
            .part_id;

        // M2 starts and replays from etcd — should also see 2 partitions
        let mgr2_addr = pick_addr();
        start_etcd_manager(mgr2_addr, etcd_endpoint.clone());
        compio::time::sleep(Duration::from_millis(500)).await;

        let mgr2 = RpcClient::connect(mgr2_addr).await.expect("connect mgr2");

        // M1 dies after the split; M2 takes over.
        let regions2 = fail_over(m1, &mgr2).await;
        assert_eq!(
            regions2.regions.len(),
            2,
            "M2 should have 2 partitions from etcd replay after split"
        );
        assert!(
            regions2.regions.iter().any(|(_, r)| r.part_id == 901),
            "M2 should have left partition"
        );
        assert!(
            regions2.regions.iter().any(|(_, r)| r.part_id == right_id),
            "M2 should have right partition"
        );

        // Verify stream structure is consistent: the new streams created by split
        // should exist in M2
        let resp = mgr2
            .call(
                MSG_STREAM_INFO,
                rkyv_encode(&StreamInfoReq {
                    stream_ids: vec![log, row, meta],
                }),
            )
            .await
            .expect("m2 stream_info");
        let info: StreamInfoResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(info.code, CODE_OK);
        // Original streams should still exist (left partition uses them)
        assert_eq!(
            info.streams.len(),
            3,
            "original streams should survive split replay"
        );

        // The PS knew only M1 and exits the process once its heartbeats have
        // failed for long enough; stop it before that can happen.
        ps_stop.0.shutdown();
        ps_stop.1.join().expect("join PS");
    });
}
