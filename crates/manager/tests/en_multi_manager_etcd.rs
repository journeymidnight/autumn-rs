//! An extent node given `--manager m2,m1` reaches the leader for its authz and
//! runtime-config polls, and follows the next leader when the first one dies.
//!
//! The list names the standby first, so the first poll is answered NOT_LEADER
//! and must move on; after the leader stops, the poll's next address refuses
//! the connection and must move on again.
//!
//! Ablations: handing the raw "a,b" string to the connection pool never loads
//! a config; not moving on after NOT_LEADER never loads one either; not moving
//! on after a transport error stops the installs once the leader dies.

mod support;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_client::ClusterClient;
use autumn_stream::{ExtentNode, ExtentNodeConfig, SharedAuthz};
use support::{pick_addr, start_etcd};

/// An etcd-backed manager on its own thread; setting the flag drops its
/// runtime, which closes its listener.
fn start_manager(addr: SocketAddr, etcd: String) -> Arc<AtomicBool> {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_t = stop.clone();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let m = autumn_manager::AutumnManager::new_with_etcd(
                vec![etcd],
                support::manager_identity(),
            )
            .await
            .expect("manager");
            compio::runtime::spawn(async move {
                if let Err(e) = m.serve(addr).await {
                    eprintln!("manager serve: {e:#}");
                }
            })
            .detach();
            while !stop_t.load(Ordering::Acquire) {
                compio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    });
    support::wait_accepting(addr, "start_manager");
    stop
}

async fn wait_until(what: &str, deadline: Duration, cond: impl Fn() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(start.elapsed() < deadline, "timed out waiting for {what}");
        compio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[test]
fn en_polls_reach_the_leader_through_a_manager_list() {
    let en = autumn_stream::en_params();
    en.config_poll_secs.set(1);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd) = start_etcd().await;
        let mgr1 = pick_addr();
        let stop1 = start_manager(mgr1, etcd.clone());
        compio::time::sleep(Duration::from_secs(2)).await;
        let mgr2 = pick_addr();
        let _stop2 = start_manager(mgr2, etcd.clone());

        let authz = SharedAuthz::new(true);
        let en_authz = authz.clone();
        let en_addr = pick_addr();
        let en_dir = tempfile::tempdir().unwrap();
        let en_dir_path = en_dir.path().to_path_buf();
        std::thread::spawn(move || {
            compio::runtime::Runtime::new().unwrap().block_on(async move {
                let cfg = ExtentNodeConfig::new(en_dir_path, 1)
                    .with_manager_endpoint(format!("{mgr2},{mgr1}"))
                    .with_shared_authz(en_authz);
                let n = ExtentNode::new(cfg).await.expect("extent node");
                if let Err(e) = n.serve(en_addr).await {
                    eprintln!("extent node serve: {e:#}");
                }
            });
        });

        let t = Duration::from_secs(10);
        wait_until("the EN's first authz config", t, || authz.installed_at_ms() > 0).await;

        let op = ClusterClient::connect_admin(&format!("{mgr1},{mgr2}"))
            .await
            .expect("admin client");
        op.runtime_config_set("en.recovery-parallelism", Some("5".into()), "test")
            .await
            .unwrap();
        wait_until("EN override", t, || en.recovery_parallelism.get() == 5).await;

        stop1.store(true, Ordering::Release);
        let t = Duration::from_secs(40);
        let start = Instant::now();
        while let Err(e) = op.runtime_config_get().await {
            assert!(start.elapsed() < t, "new leader never answered: {e:#}");
            compio::time::sleep(Duration::from_millis(200)).await;
        }
        // Only the new leader can answer now.
        let before = authz.installed_at_ms();
        wait_until("an authz install from the new leader", t, || {
            authz.installed_at_ms() > before
        })
        .await;
        op.runtime_config_set("en.recovery-parallelism", Some("3".into()), "test")
            .await
            .unwrap();
        wait_until("EN override from the new leader", t, || {
            en.recovery_parallelism.get() == 3
        })
        .await;
    });
}

/// A standby's metadata is what it replayed when it started, so the lookups an
/// extent node acts on (is this extent sealed? where is that node?) must come
/// from the leader: a standby answers them NOT_LEADER and the caller moves on.
///
/// Ablation: drop the routing gate from any of the four handlers.
#[test]
fn a_standby_refuses_metadata_lookups() {
    use autumn_rpc::manager_rpc::{
        rkyv_decode, rkyv_encode, CheckCommitLengthReq, CheckCommitLengthResp, ExtentInfoReq,
        ExtentInfoResp, NodesInfoResp, StreamInfoReq, StreamInfoResp, CODE_NOT_LEADER,
        MSG_CHECK_COMMIT_LENGTH, MSG_EXTENT_INFO, MSG_NODES_INFO, MSG_STREAM_INFO,
    };
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd) = start_etcd().await;
        let leader = pick_addr();
        let _stop1 = start_manager(leader, etcd.clone());
        compio::time::sleep(Duration::from_secs(2)).await;
        let standby = pick_addr();
        let _stop2 = start_manager(standby, etcd.clone());

        // A peer connection, as the extent node's.
        let codes = |addr: SocketAddr| async move {
            let c = autumn_rpc::client::RpcClient::connect(addr).await.expect("connect");
            let call = |msg, req| {
                let c = c.clone();
                async move { c.call(msg, req).await.expect("call") }
            };
            let s: StreamInfoResp = rkyv_decode(
                &call(MSG_STREAM_INFO, rkyv_encode(&StreamInfoReq { stream_ids: vec![] })).await,
            )
            .unwrap();
            let e: ExtentInfoResp =
                rkyv_decode(&call(MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id: 1 })).await)
                    .unwrap();
            let n: NodesInfoResp =
                rkyv_decode(&call(MSG_NODES_INFO, bytes::Bytes::new()).await).unwrap();
            let req = CheckCommitLengthReq { stream_id: 1, owner_key: "o".into(), owner_epoch: 1 };
            let l: CheckCommitLengthResp =
                rkyv_decode(&call(MSG_CHECK_COMMIT_LENGTH, rkyv_encode(&req)).await).unwrap();
            [s.code, e.code, n.code, l.code]
        };
        assert_eq!(codes(standby).await, [CODE_NOT_LEADER; 4]);
        assert!(codes(leader).await.iter().all(|c| *c != CODE_NOT_LEADER));
    });
}
