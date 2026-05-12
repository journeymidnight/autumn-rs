//! F149: leader-fence on every manager etcd write txn.
//!
//! Asserts that a manager whose leader-key value has been overwritten
//! (simulating a clean failover where a new leader has taken the slot)
//! cannot stomp on the new leader's etcd state via any mirror_* path.
//! The fence-failed call returns `CODE_NOT_LEADER`, the in-process
//! `leader` flag flips to false, and subsequent mutating RPCs continue
//! to return `CODE_NOT_LEADER` until the manager re-acquires.
//!
//! Requires embedded etcd (Go toolchain). Marked `#[ignore]` to follow
//! the repo convention for etcd-dependent tests.

mod support;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;

use support::pick_addr;

const LEADER_KEY: &str = "autumn-rs/stream-manager/leader";

// ── Embedded etcd plumbing ─────────────────────────────────────────────

struct EtcdGuard {
    child: Option<Child>,
    _data_dir: tempfile::TempDir,
}

impl Drop for EtcdGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn repo_root() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest.ancestors().nth(3).expect("repo root").to_path_buf()
}

async fn wait_for_etcd(endpoint: &str, timeout: Duration) {
    let start = Instant::now();
    loop {
        if let Ok(c) = autumn_etcd::EtcdClient::connect(endpoint).await {
            if c.get("health-check").await.is_ok() {
                return;
            }
        }
        assert!(start.elapsed() < timeout, "etcd did not become ready");
        compio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn start_embedded_etcd() -> (EtcdGuard, String) {
    let client_addr = pick_addr();
    let peer_addr = pick_addr();
    let client_url = format!("http://{}", client_addr);
    let peer_url = format!("http://{}", peer_addr);

    let data_dir = tempfile::tempdir().expect("tempdir");
    let data_path = data_dir.path().join("etcd-data");

    let helper = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/embedded_etcd/main.go");

    let mut cmd = Command::new("go");
    cmd.current_dir(repo_root())
        .arg("run")
        .arg(helper)
        .arg("--name")
        .arg("n1")
        .arg("--dir")
        .arg(data_path)
        .arg("--client")
        .arg(client_url.clone())
        .arg("--peer")
        .arg(peer_url.clone())
        .arg("--cluster")
        .arg(format!("n1={peer_url}"))
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let child = cmd.spawn().expect("spawn embedded etcd");
    wait_for_etcd(&client_url, Duration::from_secs(30)).await;

    (
        EtcdGuard {
            child: Some(child),
            _data_dir: data_dir,
        },
        client_url,
    )
}

fn start_etcd_manager(mgr_addr: SocketAddr, etcd_endpoint: String) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new_with_etcd(vec![etcd_endpoint])
                .await
                .expect("new manager with etcd");
            let _ = manager.serve(mgr_addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(400));
}

// ── F149: deposed leader's writes are fence-rejected ──────────────────

#[test]
#[ignore] // requires embedded etcd (go runtime)
fn f149_deposed_leader_etcd_writes_are_fenced() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_endpoint) = start_embedded_etcd().await;

        // Spin up M1, register a node — confirm baseline writes succeed
        // while M1 holds the leader key.
        let mgr_addr = pick_addr();
        start_etcd_manager(mgr_addr, etcd_endpoint.clone());
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");

        let resp = mgr
            .call(
                MSG_REGISTER_NODE,
                rkyv_encode(&RegisterNodeReq {
                    addr: "127.0.0.1:7771".to_string(),
                    disk_uuids: vec!["uuid-149-a".to_string()],
                    shard_ports: vec![],
                    control_address: String::new(),
                }),
            )
            .await
            .expect("register node baseline");
        let r: RegisterNodeResp = rkyv_decode(&resp).expect("decode RegisterNodeResp");
        assert_eq!(
            r.code, CODE_OK,
            "baseline register_node must succeed while M1 owns leader key"
        );

        // Externally overwrite the leader key so the fence compare
        // (`Cmp::value(LEADER_KEY) == M1.instance_id`) starts failing
        // for any subsequent etcd write from M1. This simulates a clean
        // failover where a new leader has taken the slot before M1's
        // keepalive notices, which is exactly the F149 split-brain
        // window we want to close.
        let aux = autumn_etcd::EtcdClient::connect(&etcd_endpoint)
            .await
            .expect("aux etcd client");
        // Deleting the leased key first ensures a clean overwrite. We do
        // a non-leased put because we are simulating a different
        // identity, not actually trying to acquire leadership.
        let _ = aux.delete(LEADER_KEY.as_bytes()).await;
        aux.put(LEADER_KEY.as_bytes(), b"f149-impostor-instance-id")
            .await
            .expect("aux overwrite of leader key");

        // M1 still believes it is the leader (its lease has not expired
        // and its keepalive loop has not yet observed deposition). The
        // very next mutating RPC must fence-fail and bubble up as
        // CODE_NOT_LEADER.
        let resp = mgr
            .call(
                MSG_REGISTER_NODE,
                rkyv_encode(&RegisterNodeReq {
                    addr: "127.0.0.1:7772".to_string(),
                    disk_uuids: vec!["uuid-149-b".to_string()],
                    shard_ports: vec![],
                    control_address: String::new(),
                }),
            )
            .await
            .expect("register node after fence break");
        let r: RegisterNodeResp = rkyv_decode(&resp).expect("decode RegisterNodeResp");
        assert_eq!(
            r.code, CODE_NOT_LEADER,
            "deposed M1's etcd write must be fence-rejected"
        );

        // Subsequent mutating RPCs must keep returning CODE_NOT_LEADER
        // because the in-process leader Cell flipped on the first
        // fence failure (so `ensure_leader` short-circuits without
        // even hitting etcd).
        let resp = mgr
            .call(
                MSG_REGISTER_NODE,
                rkyv_encode(&RegisterNodeReq {
                    addr: "127.0.0.1:7773".to_string(),
                    disk_uuids: vec!["uuid-149-c".to_string()],
                    shard_ports: vec![],
                    control_address: String::new(),
                }),
            )
            .await
            .expect("register node sticky NotLeader");
        let r: RegisterNodeResp = rkyv_decode(&resp).expect("decode RegisterNodeResp");
        assert_eq!(
            r.code, CODE_NOT_LEADER,
            "sticky CODE_NOT_LEADER after first fence break"
        );

        // The first registered node is still in etcd — fence rejected
        // only the second register, the first one's mirror_register_node
        // committed before the fence broke.
        let got = aux
            .get_prefix("nodes/")
            .await
            .expect("get nodes prefix");
        assert_eq!(
            got.kvs.len(),
            1,
            "only the pre-deposition node should be in etcd; got {} entries",
            got.kvs.len()
        );
    });
}
