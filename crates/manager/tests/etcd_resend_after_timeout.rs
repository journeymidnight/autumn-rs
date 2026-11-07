//! The etcd client re-sends a request after its timeout. When the first copy
//! had already committed, a value-CAS'd write's re-send fails its own
//! compare: the handler answers Precondition and never applies the change to
//! memory, so memory falls behind etcd and every later CAS on that record is
//! refused until a leader change. Spawns `etcd` and `toxiproxy-server` from
//! `$PATH`.

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

const OWNER_KEY: &str = "owner/resend-test";

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

async fn stream_extents(mgr: &RpcClient, stream_id: u64) -> Vec<u64> {
    let resp = mgr
        .call(MSG_STREAM_INFO, rkyv_encode(&StreamInfoReq { stream_ids: vec![stream_id] }))
        .await
        .expect("stream info");
    let info: StreamInfoResp = rkyv_decode(&resp).expect("decode");
    assert_eq!(info.code, CODE_OK, "{}", info.message);
    let (_, st) = info.streams.into_iter().find(|(id, _)| *id == stream_id).expect("stream");
    st.extent_ids
}

async fn alloc(mgr: &RpcClient, stream_id: u64, owner_epoch: i64) -> StreamAllocExtentResp {
    let resp = mgr
        .call(
            MSG_STREAM_ALLOC_EXTENT,
            rkyv_encode(&StreamAllocExtentReq {
                stream_id,
                owner_key: OWNER_KEY.to_string(),
                owner_epoch,
                seal_commit: Some(0),
                exclude_node_ids: vec![],
                seal_extent_id: 0,
            }),
        )
        .await
        .expect("alloc");
    rkyv_decode(&resp).expect("decode")
}

#[test]
fn an_alloc_whose_reply_was_lost_does_not_wedge_the_stream() {
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
                    disk_uuids: vec!["disk-resend-1".to_string()],
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
        let lock_resp = mgr1
            .call(MSG_ACQUIRE_OWNER_LOCK, rkyv_encode(&AcquireOwnerLockReq { owner_key: OWNER_KEY.to_string() }))
            .await
            .expect("acquire");
        let lock: AcquireOwnerLockResp = rkyv_decode(&lock_resp).expect("decode");
        assert_eq!(lock.code, CODE_OK, "{}", lock.message);
        let before = stream_extents(&mgr1, stream_id).await;
        assert_eq!(before.len(), 1);

        // Hold every etcd reply; requests still reach etcd and commit.
        toxi.add_toxic("etcd", "latency", "hold", false, &[("latency", "5000"), ("jitter", "0")])
            .expect("toxic add");
        let t0 = std::time::Instant::now();
        // The alloc's txn commits; its reply is held; at t=1.0 s the client
        // re-sends it on the direct endpoint, where its CAS meets its own write.
        let first = alloc(&mgr1, stream_id, lock.owner_epoch).await;
        let first_at = t0.elapsed();
        eprintln!("first alloc at {first_at:?}: code {} {}", first.code, first.message);
        assert!(first_at >= Duration::from_secs(1), "harness timing: the first reply was not held");
        toxi.remove_toxic("etcd", "hold").expect("toxic remove");
        compio::time::sleep(Duration::from_millis(300)).await;

        // Whatever the first alloc answered, the stream must stay usable: a
        // fresh alloc succeeds and memory agrees with what the next leader
        // replays from etcd.
        let second = alloc(&mgr1, stream_id, lock.owner_epoch).await;
        eprintln!("second alloc: code {} {}", second.code, second.message);
        let memory = stream_extents(&mgr1, stream_id).await;
        eprintln!("memory: {memory:?}");
        assert_eq!(second.code, CODE_OK, "a later alloc is refused: {}", second.message);

        let mgr2_addr = pick_addr();
        let _mgr2_flag = start_manager(mgr2_addr, vec![etcd_url.clone()]);
        mgr1_flag.store(true, Ordering::Release);
        let mgr2 = connect_leader(mgr2_addr).await;
        let replayed = stream_extents(&mgr2, stream_id).await;
        eprintln!("replayed: {replayed:?}");
        assert_eq!(replayed, memory, "the leader's memory and etcd disagree");
    });
}

/// The other half of an uncertain outcome: the request never reached etcd
/// before the timeout and arrives AFTER the manager has settled it as not
/// landed and sent it again. Rewriting the leader key first makes the late
/// copy fail its fence compare, so etcd ends at the re-sent write.
#[test]
fn a_txn_arriving_after_it_was_settled_is_fenced_out() {
    let _ = tracing_subscriber::fmt().with_env_filter("autumn_etcd=warn,autumn_manager=warn").try_init();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_url) = start_etcd().await;
        let etcd_addr = etcd_url.trim_start_matches("http://").to_string();
        let (_toxi_guard, toxi_admin) = start_toxiproxy().await;
        let toxi = ToxiproxyCli::new(toxi_admin);
        // Both endpoints behind a proxy of their own, so the manager can be
        // put back on A before each cycle (B disabled, one request fails over).
        let proxy_a = pick_addr().to_string();
        let proxy_b = pick_addr().to_string();
        toxi.create("etcd-a", &proxy_a, &etcd_addr).expect("create proxy a");
        toxi.create("etcd-b", &proxy_b, &etcd_addr).expect("create proxy b");

        let mgr_addr = pick_addr();
        let _flag = start_manager(mgr_addr, vec![format!("http://{proxy_a}"), format!("http://{proxy_b}")]);
        let mgr = connect_leader(mgr_addr).await;
        let acquire = |mgr: std::rc::Rc<RpcClient>| async move {
            let resp = mgr
                .call(MSG_ACQUIRE_OWNER_LOCK, rkyv_encode(&AcquireOwnerLockReq { owner_key: OWNER_KEY.to_string() }))
                .await
                .expect("acquire");
            let r: AcquireOwnerLockResp = rkyv_decode(&resp).expect("decode");
            assert_eq!(r.code, CODE_OK, "{}", r.message);
            r.owner_epoch
        };
        let mut first = acquire(mgr.clone()).await;
        let etcd = autumn_etcd::EtcdClient::connect(&etcd_url).await.expect("etcd");

        // Whether the late copy commits is a race inside etcd (its proposal
        // against the stream reset that follows the held bytes); unfenced it
        // committed in 4 of 10 runs, so the cycle runs several times.
        for cycle in 0..5 {
            // The manager's background reads time out under the hold too and
            // rotate its channel, so where it is after a cycle is not known.
            toxi.set_enabled("etcd-b", false).expect("disable b");
            let probe = mgr
                .call(MSG_ACQUIRE_OWNER_LOCK, rkyv_encode(&AcquireOwnerLockReq { owner_key: "owner/probe".to_string() }))
                .await
                .expect("probe");
            let probe: AcquireOwnerLockResp = rkyv_decode(&probe).expect("decode");
            assert_eq!(probe.code, CODE_OK, "{}", probe.message);
            toxi.set_enabled("etcd-b", true).expect("enable b");
            let held = "etcd-a";
            // Hold requests on their way to etcd; replies are free.
            toxi.add_toxic(held, "latency", "hold", true, &[("latency", "5000"), ("jitter", "0")])
                .expect("toxic add");
            let t0 = std::time::Instant::now();
            // X times out at 1 s, is settled as not landed, and is sent again on
            // the new connection.
            let mgr_x = mgr.clone();
            let x = compio::runtime::spawn(async move {
                let e = acquire(mgr_x).await;
                (t0.elapsed(), e)
            });
            // Y keeps the OLD connection open past X's reconnect, so X's held
            // original is still delivered when the hold is released. Without it
            // the connection closes with the hold and the copy is simply lost.
            compio::time::sleep(Duration::from_millis(600)).await;
            let mgr_y = mgr.clone();
            let y = compio::runtime::spawn(async move {
                let resp = mgr_y
                    .call(
                        MSG_ACQUIRE_OWNER_LOCK,
                        rkyv_encode(&AcquireOwnerLockReq { owner_key: "owner/other".to_string() }),
                    )
                    .await
                    .expect("acquire");
                let r: AcquireOwnerLockResp = rkyv_decode(&resp).expect("decode");
                (t0.elapsed(), r.code)
            });
            compio::time::sleep(Duration::from_millis(700)).await;
            let (settled_at, settled) = x.await.expect("x");
            eprintln!("cycle {cycle}: settled acquire at {settled_at:?}: epoch {settled} (first {first})");
            assert!(settled_at >= Duration::from_secs(1), "harness timing: the request was not held");
            assert!(settled > first);
            // t=1.3 s: the held originals arrive now; Y's is answered, X's meets
            // the fence it was sent under.
            let released_at = t0.elapsed();
            toxi.remove_toxic(held, "hold").expect("toxic remove");
            let (y_at, y_code) = y.await.expect("y");
            assert!(
                y_code == CODE_OK && y_at > released_at,
                "harness timing: Y was not delivered by the old connection after the release ({y_at:?}, code {y_code})"
            );
            compio::time::sleep(Duration::from_millis(300)).await;

            let kv = etcd.get(format!("ownerLocks/{OWNER_KEY}")).await.expect("get");
            eprintln!("cycle {cycle}: etcd mod_rev={} version={}", kv.kvs[0].mod_revision, kv.kvs[0].version);
            assert_eq!(kv.kvs[0].mod_revision, settled, "the late copy committed over the settled write");
            first = settled;
        }
        // And the manager still leads under the moved fence.
        let next = acquire(mgr.clone()).await;
        assert!(next > first);
    });
}
