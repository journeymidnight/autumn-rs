//! A partition reload with many open client connections must not stop the PS.
//!
//! Dropping a partition (reload, move, merge) signals every connection task of
//! that partition to close. That signal crosses from the PS main thread to the
//! partition thread, and compio queues cross-thread wakes in a bounded queue
//! (64 slots) whose producer spins while it is full. When the main thread woke
//! each connection itself while holding the shared-future lock the partition
//! thread needed to drain that queue, the two waited on each other: the main
//! thread stopped (no heartbeat, the manager evicted the PS) with the process
//! still alive.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;

use support::*;

const PART: u64 = 24001;
const PS_ID: u64 = 150;
const CONNECTIONS: usize = 300;

#[test]
fn a_reload_with_many_open_connections_keeps_the_ps_alive() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().unwrap();
    let n2_dir = tempfile::tempdir().unwrap();
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 150).await;
        let ready = poll_until_async(Duration::from_secs(20), Duration::from_millis(200), || async {
            let req = rkyv_encode(&CreateStreamReq {
                replicates: 2,
                ec_data_shard: 2,
                ec_parity_shard: 0,
            });
            let resp = mgr.call(MSG_CREATE_STREAM, req).await.unwrap();
            rkyv_decode::<CreateStreamResp>(&resp).unwrap().stream.is_some()
        })
        .await;
        assert!(ready, "extent nodes never took an allocation");
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, PART, log, row, meta, b"a", b"y").await;

        let ps_addr = pick_addr();
        let _ps = start_partition_server_killable(PS_ID, mgr_addr, ps_addr);
        let router = PsRouter::new(mgr_addr, ps_addr);
        psr_put(&router, PART, b"k-before", b"v").await;

        let part_addr = get_regions(&mgr)
            .await
            .part_addrs
            .into_iter()
            .find(|(p, _)| *p == PART)
            .map(|(_, a)| a.parse::<std::net::SocketAddr>().unwrap())
            .expect("partition listener");
        let mut conns = Vec::with_capacity(CONNECTIONS);
        for _ in 0..CONNECTIONS {
            conns.push(RpcClient::connect(part_addr).await.expect("connect partition"));
        }

        // A range change reloads the partition on its PS.
        upsert_partition(&mgr, PART, log, row, meta, b"a", b"z").await;

        // `y/…` lies outside the old range [a, y): only the reopened partition
        // accepts it.
        let reopened = poll_until_async(Duration::from_secs(20), Duration::from_millis(250), || async {
            let Ok(c) = router.try_client_for(PART).await else {
                return false;
            };
            let req = autumn_rpc::partition_rpc::rkyv_encode(&autumn_rpc::partition_rpc::PutReq {
                part_id: PART,
                key: b"y/after-reload".to_vec(),
                value: b"v".to_vec(),
                expires_at: 0,
                region_epoch: 0,
                inode_hint: 0,
                lease_epoch: 0,
            });
            match compio::time::timeout(Duration::from_secs(2), c.call(autumn_rpc::partition_rpc::MSG_PUT, req)).await {
                Ok(Ok(resp)) => autumn_rpc::partition_rpc::rkyv_decode::<autumn_rpc::partition_rpc::PutResp>(&resp)
                    .map(|r| r.code == autumn_rpc::partition_rpc::CODE_OK)
                    .unwrap_or(false),
                _ => false,
            }
        })
        .await;
        // The retired partition must close every connection it accepted, or
        // they keep serving the old instance.
        let closed = poll_until_async(Duration::from_secs(10), Duration::from_millis(200), || {
            let all = conns.iter().all(|c| c.is_closed());
            async move { all }
        })
        .await;
        // The PS-level liveness: a stopped main thread stops heartbeating and
        // is evicted after 10 s.
        compio::time::sleep(Duration::from_secs(12)).await;
        let alive = get_regions(&mgr)
            .await
            .ps_details
            .iter()
            .any(|(id, _)| *id == PS_ID);
        drop(conns);
        assert!(alive, "PS {PS_ID} was evicted after the reload (main thread stopped)");
        assert!(reopened, "partition {PART} never reopened after the reload");
        assert!(closed, "the retired partition left client connections open");
        let got = psr_get(&router, PART, b"k-before").await;
        assert_eq!(got.code, autumn_rpc::partition_rpc::CODE_OK, "read after reload");
    });
}
