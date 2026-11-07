//! Etcd-backed stream-manager integration tests.
//!
//! Spawns the `etcd` binary on `$PATH` (override via
//! `AUTUMN_TEST_ETCD_BIN`).

mod support;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ExtentNode, ExtentNodeConfig};

use support::{pick_addr, start_etcd};

/// Like `start_etcd_manager`; setting the returned flag drops the manager's
/// runtime (its listener and its leader lease's keepalive with it).
fn start_stoppable_etcd_manager(mgr_addr: SocketAddr, etcd_endpoint: String) -> Arc<AtomicBool> {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_t = stop.clone();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new_with_etcd(vec![etcd_endpoint], support::manager_identity())
                .await
                .expect("new manager with etcd");
            compio::runtime::spawn(async move {
                if let Err(e) = manager.serve(mgr_addr).await {
                    eprintln!("manager serve: {e:#}");
                }
            })
            .detach();
            while !stop_t.load(Ordering::Acquire) {
                compio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    });
    support::wait_accepting(mgr_addr, "start_stoppable_etcd_manager");
    stop
}

/// Start manager with etcd on its own thread, return addr.
fn start_etcd_manager(mgr_addr: SocketAddr, etcd_endpoint: String) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new_with_etcd(vec![etcd_endpoint], support::manager_identity())
                .await
                .expect("new manager with etcd");
            let _ = manager.serve(mgr_addr).await;
        });
    });
    support::wait_accepting(mgr_addr, "start_etcd_manager");
}

fn start_extent_node(addr: SocketAddr, dir: PathBuf, disk_id: u64) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let n = ExtentNode::new(ExtentNodeConfig::new(dir, disk_id))
                .await
                .expect("extent node");
            let _ = n.serve(addr).await;
        });
    });
    support::wait_accepting(addr, "start_extent_node");
}

async fn register_node(mgr: &RpcClient, addr: &str, disk_uuid: &str) -> RegisterNodeResp {
    let resp = mgr
        .call(
            MSG_REGISTER_NODE,
            rkyv_encode(&RegisterNodeReq {
                addr: addr.to_string(),
                disk_uuids: vec![disk_uuid.to_string()],
                shard_ports: vec![],
                control_address: String::new(),
                node_uuid: String::new(),
            }),
        )
        .await
        .expect("register node");
    rkyv_decode::<RegisterNodeResp>(&resp).expect("decode RegisterNodeResp")
}

async fn create_stream(mgr: &RpcClient, replicates: u32) -> u64 {
    let resp = mgr
        .call(
            MSG_CREATE_STREAM,
            rkyv_encode(&CreateStreamReq {
                replicates,
                ec_data_shard: replicates,
                ec_parity_shard: 0,
            }),
        )
        .await
        .expect("create stream");
    let created: CreateStreamResp = rkyv_decode(&resp).expect("decode CreateStreamResp");
    assert_eq!(
        created.code, CODE_OK,
        "create stream failed: {}",
        created.message
    );
    created.stream.expect("stream").stream_id
}

#[test]
fn stream_manager_with_real_etcd() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (etcd_guard, etcd_endpoint) = start_etcd().await;

        let mgr_addr = pick_addr();
        start_etcd_manager(mgr_addr, etcd_endpoint.clone());

        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("connect mgr");

        let n1_addr = pick_addr();
        let n2_addr = pick_addr();
        let n1_dir = tempfile::tempdir().expect("n1 tempdir");
        let n2_dir = tempfile::tempdir().expect("n2 tempdir");
        start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
        start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

        let reg1 = register_node(&mgr, &n1_addr.to_string(), "disk-e1").await;
        assert_eq!(
            reg1.code, CODE_OK,
            "register node1 failed: {}",
            reg1.message
        );

        let reg2 = register_node(&mgr, &n2_addr.to_string(), "disk-e2").await;
        assert_eq!(
            reg2.code, CODE_OK,
            "register node2 failed: {}",
            reg2.message
        );

        let stream_id = create_stream(&mgr, 1).await;

        let resp = mgr
            .call(
                MSG_ACQUIRE_OWNER_LOCK,
                rkyv_encode(&AcquireOwnerLockReq {
                    owner_key: "owner/etcd/stream/1".to_string(),
                }),
            )
            .await
            .expect("acquire owner lock");
        let lock: AcquireOwnerLockResp = rkyv_decode(&resp).expect("decode");

        let resp = mgr
            .call(
                MSG_STREAM_ALLOC_EXTENT,
                rkyv_encode(&StreamAllocExtentReq {
                    stream_id,
                    owner_key: "owner/etcd/stream/1".to_string(),
                    owner_epoch: lock.owner_epoch,
                    seal_commit: Some(64),
                    exclude_node_ids: vec![],
                seal_extent_id: 0,
                }),
            )
            .await
            .expect("stream alloc extent");
        let _alloc: StreamAllocExtentResp = rkyv_decode(&resp).expect("decode");

        // Verify etcd state
        let etcd = autumn_etcd::EtcdClient::connect(&etcd_endpoint)
            .await
            .expect("connect etcd");

        let nodes = etcd.get_prefix("nodes/").await.expect("get nodes");
        assert_eq!(nodes.kvs.len(), 2);

        let streams = etcd.get_prefix("streams/").await.expect("get streams");
        assert_eq!(streams.kvs.len(), 1);

        let extents = etcd.get_prefix("extents/").await.expect("get extents");
        assert_eq!(extents.kvs.len(), 2);

        drop(etcd_guard);
    });
}

#[test]
fn etcd_replay_owner_lock_allows_check_commit_length_without_reacquire() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (etcd_guard, etcd_endpoint) = start_etcd().await;

        // Manager 1
        let mgr1_addr = pick_addr();
        let stop1 = start_stoppable_etcd_manager(mgr1_addr, etcd_endpoint.clone());

        let mgr1 = RpcClient::connect_as(mgr1_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("connect mgr1");

        let n1_addr = pick_addr();
        let n1_dir = tempfile::tempdir().expect("n1 tempdir");
        start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);

        let reg = register_node(&mgr1, &n1_addr.to_string(), "disk-replay-owner-1").await;
        assert_eq!(reg.code, CODE_OK, "{}", reg.message);

        let stream_id = create_stream(&mgr1, 1).await;

        let owner_key = "owner/replay/commit-check".to_string();
        let resp = mgr1
            .call(
                MSG_ACQUIRE_OWNER_LOCK,
                rkyv_encode(&AcquireOwnerLockReq {
                    owner_key: owner_key.clone(),
                }),
            )
            .await
            .expect("acquire owner lock");
        let lock: AcquireOwnerLockResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(lock.code, CODE_OK, "{}", lock.message);
        let owner_epoch = lock.owner_epoch;

        let resp = mgr1
            .call(
                MSG_CHECK_COMMIT_LENGTH,
                rkyv_encode(&CheckCommitLengthReq {
                    stream_id,
                    owner_key: owner_key.clone(),
                    owner_epoch: owner_epoch,
                }),
            )
            .await
            .expect("check commit length on manager1");
        let check1: CheckCommitLengthResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(check1.code, CODE_OK, "{}", check1.message);

        // Manager 2 starts as a standby; manager 1 stops and its leader
        // lease runs out, so manager 2 takes over and replays.
        let mgr2_addr = pick_addr();
        start_etcd_manager(mgr2_addr, etcd_endpoint.clone());
        drop(mgr1);
        stop1.store(true, Ordering::Release);
        let mgr2 = RpcClient::connect_as(mgr2_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("connect mgr2");

        // key point: do not reacquire owner lock on manager2
        let start = Instant::now();
        let check2 = loop {
            let resp = mgr2
                .call(
                    MSG_CHECK_COMMIT_LENGTH,
                    rkyv_encode(&CheckCommitLengthReq {
                        stream_id,
                        owner_key: owner_key.clone(),
                        owner_epoch,
                    }),
                )
                .await
                .expect("check commit length on manager2");
            let check: CheckCommitLengthResp = rkyv_decode(&resp).expect("decode");
            if check.code != CODE_NOT_LEADER {
                break check;
            }
            assert!(start.elapsed() < Duration::from_secs(40), "manager2 never took over");
            compio::time::sleep(Duration::from_millis(200)).await;
        };
        assert_eq!(
            check2.code, CODE_OK,
            "owner lock replay failed: {}",
            check2.message
        );

        drop(etcd_guard);
    });
}
