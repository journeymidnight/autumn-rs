//! `MSG_COMPARE_WRITE` — the fenced conditional put/delete the S3 gateway
//! publishes objects with.
//!
//! Through the SDK against a real partition server: create-if-absent,
//! replace-on-match, conditional delete, a stale epoch refused, and the
//! property recovery depends on — a comparison that FAILS still raises and
//! persists the fence floor, so a dead owner's late writes stay refused
//! across a partition-server crash.
//!
//! The PS runs as a real `autumn-ps` subprocess so it can be killed -9 and
//! restarted with the same psid (`bug_lease_2_phase2_persistence.rs` pattern).

mod support;

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use autumn_client::{AutumnError, ClusterClient, WriteLease};
use autumn_rpc::client::RpcClient;

use support::*;

fn ps_binary() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest.parent().and_then(|p| p.parent()).expect("workspace root").to_path_buf();
    let target = std::env::var("CARGO_TARGET_DIR").map(PathBuf::from).unwrap_or_else(|_| workspace.join("target"));
    // The newest build across profiles: a stale binary would not know the opcode.
    let mut best: Option<(std::time::SystemTime, PathBuf)> = None;
    for profile in ["debug", "release"] {
        let p = target.join(profile).join("autumn-ps");
        if let Ok(meta) = std::fs::metadata(&p) {
            let mt = meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            if best.as_ref().is_none_or(|(bt, _)| mt > *bt) {
                best = Some((mt, p));
            }
        }
    }
    best.map(|(_, p)| p).unwrap_or_else(|| panic!("autumn-ps not found under {}", target.display()))
}

fn spawn_ps(psid: u64, mgr: std::net::SocketAddr, ps: std::net::SocketAddr) -> Child {
    Command::new(ps_binary())
        .args(["--psid", &psid.to_string(), "--port", &ps.port().to_string()])
        .args(["--manager", &mgr.to_string(), "--listen", "127.0.0.1", "--advertise", &ps.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn autumn-ps")
}

fn lease(inode_hint: u64, lease_epoch: u64) -> WriteLease {
    WriteLease { inode_hint, lease_epoch }
}

async fn get(c: &ClusterClient, k: &[u8]) -> Option<Vec<u8>> {
    c.get(k).await.expect("get")
}

#[test]
#[ignore] // requires built binaries + full cluster
fn compare_write_conditions_fences_and_persists_a_bump_from_a_failed_compare() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 171).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 17101, log, row, meta, b"", b"\xff\xff\xff\xff").await;
        let ps_addr = pick_addr();
        let mut child = spawn_ps(171, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(2500)).await;

        let c = ClusterClient::connect_raw(&mgr_addr.to_string()).await.expect("connect");
        c.set_rpc_timeout(Duration::from_secs(30));
        let anon = WriteLease::ANON;

        // Create-if-absent, and only once.
        assert!(c.compare_write(b"cw/a", None, Some(b"v1"), anon).await.unwrap());
        assert!(!c.compare_write(b"cw/a", None, Some(b"v2"), anon).await.unwrap());
        assert_eq!(get(&c, b"cw/a").await.as_deref(), Some(&b"v1"[..]));

        // Replace only on an exact match.
        assert!(!c.compare_write(b"cw/a", Some(b"zz"), Some(b"v3"), anon).await.unwrap());
        assert!(c.compare_write(b"cw/a", Some(b"v1"), Some(b"v3"), anon).await.unwrap());
        assert_eq!(get(&c, b"cw/a").await.as_deref(), Some(&b"v3"[..]));

        // Conditional delete.
        assert!(!c.compare_write(b"cw/a", Some(b"v1"), None, anon).await.unwrap());
        assert_eq!(get(&c, b"cw/a").await.as_deref(), Some(&b"v3"[..]));
        assert!(c.compare_write(b"cw/a", Some(b"v3"), None, anon).await.unwrap());
        assert_eq!(get(&c, b"cw/a").await, None);
        // A tombstone reads as absent, so create-if-absent succeeds over it.
        assert!(c.compare_write(b"cw/a", None, Some(b"v4"), anon).await.unwrap());
        assert_eq!(get(&c, b"cw/a").await.as_deref(), Some(&b"v4"[..]));

        // A stale epoch is refused before the comparison is even looked at.
        assert!(c.compare_write(b"cw/f", None, Some(b"x"), lease(77, 5)).await.unwrap());
        match c.compare_write(b"cw/g", None, Some(b"y"), lease(77, 4)).await {
            Err(AutumnError::Fenced(_)) => {}
            other => panic!("stale epoch must be fenced, got {other:?}"),
        }
        assert_eq!(get(&c, b"cw/g").await, None);

        // A comparison that cannot hold is a pure floor bump: nothing is
        // written, and the raised floor is durable. No other write for ino 88
        // precedes the crash, so only the fence-only record can carry it.
        assert!(!c.compare_write(b"cw/h", Some(b"never"), None, lease(88, 10)).await.unwrap());
        assert_eq!(get(&c, b"cw/h").await, None);

        child.kill().expect("kill -9 PS");
        let _ = child.wait();
        let mut child = spawn_ps(171, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(3000)).await;

        match c.put_fenced(b"cw/i", b"late", lease(88, 9)).await {
            Err(AutumnError::Fenced(_)) => {}
            other => panic!("the bump from a failed compare must survive a PS crash, got {other:?}"),
        }
        assert_eq!(get(&c, b"cw/i").await, None);
        c.put_fenced(b"cw/i", b"current", lease(88, 10)).await.expect("current epoch writes");

        child.kill().ok();
        let _ = child.wait();
    });
}
