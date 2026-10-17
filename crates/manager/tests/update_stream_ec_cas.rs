//! `update_stream_ec` rewrites the whole `streams/<id>` record. Its bytes can
//! reach etcd AFTER a membership change committed later: the etcd client's
//! timeout re-sends a request on a new connection, and a request still held
//! on the old path lands when that path drains. Without a value CAS the stale
//! record overwrites the new tail, and the next leader replays a stream
//! without it. Spawns `etcd` and `toxiproxy-server` from `$PATH`.

mod support;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_params::manager::ManagerParams;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::version_hello::Role;

use support::{pick_addr, start_etcd, start_extent_node, start_toxiproxy, ToxiproxyCli};

const OWNER_KEY: &str = "owner/update-ec-test";

/// A manager whose etcd requests time out after 1 s; stops when the flag is set.
fn start_manager(mgr_addr: SocketAddr, endpoints: Vec<String>) -> Arc<AtomicBool> {
    let flag = Arc::new(AtomicBool::new(false));
    let flag_thread = flag.clone();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let params = ManagerParams::default();
            params.etcd_request_timeout_ms.set(1000);
            let manager = AutumnManager::new_with_etcd_params(endpoints, support::manager_identity(), params)
                .await
                .expect("new manager with etcd");
            compio::runtime::spawn(async move {
                let _ = manager.serve(mgr_addr).await;
            })
            .detach();
            while !flag_thread.load(Ordering::Acquire) {
                compio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    });
    flag
}

/// Connects, then waits until this manager answers as the leader.
async fn connect_leader(addr: SocketAddr) -> std::rc::Rc<RpcClient> {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mgr = loop {
        match RpcClient::connect_as(addr, Role::Admin, None).await {
            Ok(c) => break c,
            Err(_) if std::time::Instant::now() < deadline => {
                compio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("connect {addr}: {e}"),
        }
    };
    loop {
        let resp = mgr.call(MSG_GET_CLUSTER_OVERVIEW, bytes::Bytes::new()).await.expect("overview");
        let ov: GetClusterOverviewResp = rkyv_decode(&resp).expect("decode");
        if ov.code == CODE_OK {
            return mgr;
        }
        assert!(std::time::Instant::now() < deadline, "manager at {addr} never led");
        compio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn acquire(mgr: &RpcClient, owner_key: &str) -> AcquireOwnerLockResp {
    let resp = mgr
        .call(MSG_ACQUIRE_OWNER_LOCK, rkyv_encode(&AcquireOwnerLockReq { owner_key: owner_key.to_string() }))
        .await
        .expect("acquire owner lock");
    rkyv_decode(&resp).expect("decode")
}

async fn stream_extents(mgr: &RpcClient, stream_id: u64) -> (Vec<u64>, u32, u32) {
    let resp = mgr
        .call(MSG_STREAM_INFO, rkyv_encode(&StreamInfoReq { stream_ids: vec![stream_id] }))
        .await
        .expect("stream info");
    let info: StreamInfoResp = rkyv_decode(&resp).expect("decode");
    assert_eq!(info.code, CODE_OK, "{}", info.message);
    let (_, st) = info.streams.into_iter().find(|(id, _)| *id == stream_id).expect("stream");
    (st.extent_ids, st.ec_data_shard, st.ec_parity_shard)
}

#[test]
fn a_late_update_stream_ec_does_not_overwrite_a_newer_tail() {
    let _ = tracing_subscriber::fmt().with_env_filter("autumn_etcd=warn,autumn_manager=warn").try_init();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_url) = start_etcd().await;
        let etcd_addr = etcd_url.trim_start_matches("http://").to_string();
        let (_toxi_guard, toxi_admin) = start_toxiproxy().await;
        let toxi = ToxiproxyCli::new(toxi_admin);
        let proxy_addr = pick_addr().to_string();
        toxi.create("etcd", &proxy_addr, &etcd_addr).expect("create proxy");

        let n1_dir = tempfile::tempdir().expect("n1");
        let n1_addr = pick_addr();
        start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);

        let mgr1_addr = pick_addr();
        let mgr1_flag = start_manager(mgr1_addr, vec![format!("http://{proxy_addr}"), etcd_url.clone()]);
        let mgr1 = connect_leader(mgr1_addr).await;

        let reg = mgr1
            .call(
                MSG_REGISTER_NODE,
                rkyv_encode(&RegisterNodeReq {
                    addr: n1_addr.to_string(),
                    disk_uuids: vec!["disk-ec-1".to_string()],
                    shard_ports: vec![],
                    control_address: String::new(),
                    node_uuid: String::new(),
                }),
            )
            .await
            .expect("register node");
        let reg: RegisterNodeResp = rkyv_decode(&reg).expect("decode");
        assert_eq!(reg.code, CODE_OK, "{}", reg.message);

        let created = mgr1
            .call(
                MSG_CREATE_STREAM,
                rkyv_encode(&CreateStreamReq { replicates: 1, ec_data_shard: 1, ec_parity_shard: 0 }),
            )
            .await
            .expect("create stream");
        let created: CreateStreamResp = rkyv_decode(&created).expect("decode");
        assert_eq!(created.code, CODE_OK, "{}", created.message);
        let stream_id = created.stream.expect("stream").stream_id;
        let lock = acquire(&mgr1, OWNER_KEY).await;
        assert_eq!(lock.code, CODE_OK, "{}", lock.message);
        let (before, _, _) = stream_extents(&mgr1, stream_id).await;
        assert_eq!(before.len(), 1);

        // Hold every request on its way to etcd (the reply direction is
        // free). Removing the toxic releases what it holds.
        toxi.add_toxic("etcd", "latency", "hold", true, &[("latency", "5000"), ("jitter", "0")])
            .expect("toxic add");
        let t0 = std::time::Instant::now();
        // X: any etcd write. Its timeout at t=1.0 s moves the manager's
        // channel to the direct endpoint; X is re-sent there.
        let mgr_x = connect_leader(mgr1_addr).await;
        let x = compio::runtime::spawn(async move {
            let r = acquire(&mgr_x, "owner/throwaway").await;
            eprintln!("X answered at {:?}: code {}", t0.elapsed(), r.code);
            (t0.elapsed(), r.code)
        });
        compio::time::sleep(Duration::from_millis(300)).await;
        // U: snapshots the stream with one extent; its write is held.
        let mgr_u = connect_leader(mgr1_addr).await;
        let u = compio::runtime::spawn(async move {
            let resp = mgr_u
                .call(
                    MSG_UPDATE_STREAM_EC,
                    rkyv_encode(&UpdateStreamEcReq { stream_id, ec_data_shard: 2, ec_parity_shard: 1 }),
                )
                .await
                .expect("update stream ec");
            let r: UpdateStreamEcResp = rkyv_decode(&resp).expect("decode");
            eprintln!("U answered at {:?}: code {} {}", t0.elapsed(), r.code, r.message);
            (t0.elapsed(), r.code)
        });
        compio::time::sleep(Duration::from_millis(800)).await;
        // t=1.1 s: the channel is direct now. A new tail commits at once.
        let resp = mgr1
            .call(
                MSG_STREAM_ALLOC_EXTENT,
                rkyv_encode(&StreamAllocExtentReq {
                    stream_id,
                    owner_key: OWNER_KEY.to_string(),
                    owner_epoch: lock.owner_epoch,
                    seal_commit: Some(0),
                    exclude_node_ids: vec![],
                    seal_extent_id: 0,
                }),
            )
            .await
            .expect("alloc");
        let alloc: StreamAllocExtentResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(alloc.code, CODE_OK, "alloc at {:?}: {}", t0.elapsed(), alloc.message);
        let (with_tail, _, _) = stream_extents(&mgr1, stream_id).await;
        assert_eq!(with_tail.len(), 2, "alloc did not add a tail: {with_tail:?}");
        eprintln!("alloc done at {:?}: {with_tail:?}", t0.elapsed());
        // t=1.6 s: the held bytes drain; U's stale record reaches etcd now.
        compio::time::sleep(Duration::from_millis(500)).await;
        let released_at = t0.elapsed();
        toxi.remove_toxic("etcd", "hold").expect("toxic remove");
        // The hold must have happened, or the write landed in order and the
        // replay check below passes with or without the CAS.
        let (x_at, x_code) = x.await.expect("x");
        assert!(x_code == CODE_OK && x_at >= Duration::from_secs(1), "harness timing: X was not held past its timeout");
        let (u_at, u_code) = u.await.expect("u");
        assert!(u_at > released_at, "harness timing: U was answered before the release");
        assert_eq!(u_code, CODE_PRECONDITION, "the late write must be refused, not applied");
        compio::time::sleep(Duration::from_millis(500)).await;
        let (memory, _, _) = stream_extents(&mgr1, stream_id).await;
        assert_eq!(memory, with_tail, "the leader's own view changed");

        // The next leader replays the stream from etcd alone.
        let mgr2_addr = pick_addr();
        let _mgr2_flag = start_manager(mgr2_addr, vec![etcd_url.clone()]);
        mgr1_flag.store(true, Ordering::Release);
        let mgr2 = connect_leader(mgr2_addr).await;
        let (replayed, k, m) = stream_extents(&mgr2, stream_id).await;
        eprintln!("replayed: {replayed:?} ec {k}/{m}");
        assert_eq!(replayed, with_tail, "the new leader lost the tail allocated during the hold");
    });
}
