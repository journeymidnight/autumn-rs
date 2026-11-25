//! A leader whose etcd stops answering without closing the connection (the
//! process frozen, a network black hole) must step down once its lease can no
//! longer be confirmed: etcd expires the lease and another manager may lead.
//! Its keepalive waited on that connection with no deadline, so it went on
//! answering as the leader. With other members alive, the renewal moves to
//! one of them in time and the leader stays. Needs `etcd` on `PATH`.

mod support;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use autumn_etcd::{Credentials, EtcdClient, EtcdConfig};
use autumn_manager::AutumnManager;
use autumn_params::manager::ManagerParams;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::version_hello::Role;

use support::{pick_addr, start_etcd, start_etcd_cluster};

fn start_manager(addr: SocketAddr, etcd: Vec<String>) {
    start_manager_with(addr, EtcdConfig::plain(&etcd));
}

fn start_manager_with(addr: SocketAddr, etcd: EtcdConfig) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let m = AutumnManager::new_with_etcd_config(
                etcd,
                support::manager_identity(),
                ManagerParams::default(),
            )
            .await
            .expect("manager");
            if let Err(e) = m.serve(addr).await {
                eprintln!("manager serve: {e:#}");
            }
        });
    });
}

/// The overview reads `leader` from memory, never etcd: it answers at once.
async fn overview_code(mgr: &RpcClient) -> u8 {
    let call = mgr.call(MSG_GET_CLUSTER_OVERVIEW, bytes::Bytes::new());
    let resp = compio::time::timeout(Duration::from_secs(5), call)
        .await
        .expect("overview unanswered for 5 s")
        .expect("overview");
    rkyv_decode::<GetClusterOverviewResp>(&resp)
        .expect("decode")
        .code
}

/// Connects and waits until the manager leads.
async fn leader(addr: SocketAddr) -> std::rc::Rc<RpcClient> {
    support::wait_accepting(addr, "leader_keepalive_half_open");
    let mgr = RpcClient::connect_as(addr, Role::Admin, None)
        .await
        .expect("connect");
    let deadline = Instant::now() + Duration::from_secs(30);
    while overview_code(&mgr).await != CODE_OK {
        assert!(Instant::now() < deadline, "manager never led");
        compio::time::sleep(Duration::from_millis(200)).await;
    }
    mgr
}

const LEADER_KEY: &[u8] = b"autumn-rs/stream-manager/leader";

/// The leader key's lease, read from a member that stays up: unchanged
/// across the freeze = the same term held throughout (a step-down and a
/// re-win between two polls would show a new lease).
async fn leader_lease(etcd: &EtcdClient) -> i64 {
    let got = etcd.get(LEADER_KEY).await.expect("read leader key");
    got.kvs.first().expect("leader key").lease
}

fn signal(pid: u32, sig: &str) {
    let ok = std::process::Command::new("kill")
        .args([sig, &pid.to_string()])
        .status()
        .expect("kill")
        .success();
    assert!(ok, "kill {sig} {pid}");
}

#[test]
fn leader_steps_down_when_etcd_stops_answering() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (etcd, etcd_url) = start_etcd().await;
        let addr = pick_addr();
        start_manager(addr, vec![etcd_url]);
        let mgr = leader(addr).await;

        // Frozen, not killed: its sockets stay open and the kernel still
        // acknowledges what the manager sends.
        signal(etcd.pid(), "-STOP");
        let frozen = Instant::now();
        // Lease TTL 10 s, renewed every 2 s: past ~10 s the lease may be gone.
        let bound = Duration::from_secs(20);
        loop {
            if overview_code(&mgr).await == CODE_NOT_LEADER {
                eprintln!("stepped down {:?} after etcd froze", frozen.elapsed());
                break;
            }
            if frozen.elapsed() > bound {
                signal(etcd.pid(), "-CONT");
                panic!(
                    "still answers as the leader {:?} after etcd froze",
                    frozen.elapsed()
                );
            }
            compio::time::sleep(Duration::from_millis(500)).await;
        }
        signal(etcd.pid(), "-CONT");
    });
}

/// The member the manager talks to freezes; two others still form a quorum.
/// Each renewal step gives up after 2 s and moves to the next member, well
/// before the 10 s lease runs out, so the leader never steps down.
#[test]
fn leader_rides_out_one_frozen_member() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (members, urls) = start_etcd_cluster(3).await;
        let addr = pick_addr();
        start_manager(addr, urls.clone());
        let mgr = leader(addr).await;
        let witness = EtcdClient::connect(&urls[2]).await.expect("etcd");
        let lease = leader_lease(&witness).await;

        // The client connects to the first member that accepts: the first.
        signal(members[0].pid(), "-STOP");
        let frozen = Instant::now();
        while frozen.elapsed() < Duration::from_secs(25) {
            if overview_code(&mgr).await != CODE_OK {
                signal(members[0].pid(), "-CONT");
                panic!(
                    "stepped down {:?} after one of three members froze",
                    frozen.elapsed()
                );
            }
            compio::time::sleep(Duration::from_millis(500)).await;
        }
        assert_eq!(
            leader_lease(&witness).await,
            lease,
            "a new term: it stepped down and won again"
        );
        signal(members[0].pid(), "-CONT");
    });
}

#[derive(Clone, prost::Message)]
struct UserAddRequest {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    password: String,
}
#[derive(Clone, prost::Message)]
struct UserGrantRoleRequest {
    #[prost(string, tag = "1")]
    user: String,
    #[prost(string, tag = "2")]
    role: String,
}
#[derive(Clone, prost::Message)]
struct Empty {}

/// Create `root` with `password` and turn auth on.
async fn enable_auth(url: &str, password: &str) {
    let c = EtcdClient::connect(url).await.expect("etcd");
    let add = UserAddRequest {
        name: "root".into(),
        password: password.into(),
    };
    let _: Empty = c
        .unary("/etcdserverpb.Auth/UserAdd", &add)
        .await
        .expect("add root");
    let grant = UserGrantRoleRequest {
        user: "root".into(),
        role: "root".into(),
    };
    let _: Empty = c
        .unary("/etcdserverpb.Auth/UserGrantRole", &grant)
        .await
        .expect("grant root");
    let _: Empty = c
        .unary("/etcdserverpb.Auth/AuthEnable", &Empty {})
        .await
        .expect("enable auth");
}

/// With auth on, a connect to a frozen member hangs in `Authenticate` (TCP and
/// h2 complete without it). The manager's member dies and the next one in
/// line freezes; three of five still form a quorum. Each connect gives up on
/// the frozen member in time and goes on to the next, so the leader stays.
#[test]
fn leader_reconnects_past_a_frozen_member() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (members, urls) = start_etcd_cluster(5).await;
        enable_auth(&urls[0], "pw").await;
        let mut config = EtcdConfig::plain(&urls);
        config.credentials = Some(Credentials {
            user: "root".into(),
            password: "pw".into(),
        });
        let mut witness = EtcdConfig::plain(&urls[2..3]);
        witness.credentials = config.credentials.clone();
        let witness = EtcdClient::connect_with(witness, Duration::from_secs(10))
            .await
            .expect("etcd");
        let addr = pick_addr();
        start_manager_with(addr, config);
        let mgr = leader(addr).await;
        let lease = leader_lease(&witness).await;

        // The client connects to the first member that accepts: the first.
        signal(members[0].pid(), "-KILL");
        signal(members[1].pid(), "-STOP");
        let frozen = Instant::now();
        while frozen.elapsed() < Duration::from_secs(25) {
            if overview_code(&mgr).await != CODE_OK {
                signal(members[1].pid(), "-CONT");
                panic!(
                    "stepped down {:?} after its member died and the next froze",
                    frozen.elapsed()
                );
            }
            compio::time::sleep(Duration::from_millis(500)).await;
        }
        assert_eq!(
            leader_lease(&witness).await,
            lease,
            "a new term: it stepped down and won again"
        );
        signal(members[1].pid(), "-CONT");
    });
}
