//! `fusermount3 -u` on an autumn-fuse mount must end the daemon: the session
//! loop returns, the dispatch loop is told to stop, and `main` returns.
//! Spawns the real `target/debug/autumn-fuse` (build it first) against an
//! in-process cluster; needs `/dev/fuse` and `fusermount3`.

mod support;

use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;

use support::*;

fn mounted(dir: &std::path::Path) -> bool {
    std::fs::read_to_string("/proc/mounts")
        .expect("/proc/mounts")
        .lines()
        .any(|l| l.split_whitespace().nth(1) == Some(dir.to_str().unwrap()))
}

/// Kills the daemon and unmounts on every exit path, so a failed assertion
/// leaves neither a process nor a mount behind.
struct Daemon {
    child: std::process::Child,
    dir: std::path::PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _killed = self.child.kill();
            let _reaped = self.child.wait();
        }
        if mounted(&self.dir) {
            let _unmounted = std::process::Command::new("fusermount3").args(["-u", "-z"]).arg(&self.dir).status();
        }
    }
}

#[test]
fn the_daemon_exits_after_a_plain_unmount() {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 125).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 12501, log, row, meta, b"", b"\xff\xff\xff\xff").await;
        let ps_addr = pick_addr();
        start_partition_server(125, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(1500)).await;
        let _ = RpcClient::connect(ps_addr).await.unwrap();
    });

    let mount_dir = tempfile::tempdir().expect("mountpoint");
    let log_path = mount_dir.path().with_extension("fuse.log");
    // tracing writes to stdout; panics to stderr.
    let log = std::fs::File::create(&log_path).expect("log file");
    let log_err = log.try_clone().expect("log file");
    let child = std::process::Command::new(support_binary_path("autumn-fuse"))
        .arg("--mountpoint")
        .arg(mount_dir.path())
        .arg("--manager")
        .arg(mgr_addr.to_string())
        .stdout(log)
        .stderr(log_err)
        .spawn()
        .expect("spawn autumn-fuse");
    let mut daemon = Daemon { child, dir: mount_dir.path().to_path_buf() };

    let deadline = Instant::now() + Duration::from_secs(90);
    while !mounted(&daemon.dir) {
        if let Some(status) = daemon.child.try_wait().expect("try_wait") {
            panic!("autumn-fuse exited before mounting: {status}");
        }
        assert!(Instant::now() < deadline, "autumn-fuse never mounted");
        std::thread::sleep(Duration::from_millis(200));
    }
    // A request the daemon served, so the dispatch loop is known to run.
    let entries = std::fs::read_dir(&daemon.dir).expect("readdir").count();
    assert_eq!(entries, 0, "a fresh mount lists nothing");

    let status = std::process::Command::new("fusermount3").arg("-u").arg(&daemon.dir).status().expect("fusermount3");
    assert!(status.success(), "fusermount3 -u failed: {status}");
    assert!(!mounted(&daemon.dir), "still mounted after fusermount3 -u");

    let deadline = Instant::now() + Duration::from_secs(10);
    let exited = loop {
        if let Some(status) = daemon.child.try_wait().expect("try_wait") {
            break Some(status);
        }
        if Instant::now() >= deadline {
            break None;
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let status = exited.expect("autumn-fuse was still running 10 s after the unmount");
    assert!(status.success(), "autumn-fuse exited with {status}");
    // It exited BECAUSE the dispatcher was told to stop, not because the
    // compio thread had already died.
    let log = std::fs::read_to_string(&log_path).expect("read log");
    assert!(log.contains("received Destroy, shutting down"), "no Destroy in the daemon log:\n{log}");
}
