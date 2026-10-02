//! The `autumn-extent-node` binary writes large append bursts with O_DIRECT
//! unless told `--no-direct-io`.
//!
//! The library config (`ExtentNodeConfig`) keeps it off; the binary is where the
//! default lives, so this starts the real binary on a formatted data dir and
//! reads its startup log: `direct-io on` is logged exactly when the node runs
//! with O_DIRECT, once per shard. The data dir is under Cargo's target tmp dir
//! (not `/tmp`, which may be tmpfs and refuse O_DIRECT).

#![cfg(target_os = "linux")] // O_DIRECT is Linux-only

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const MANAGER_BIN: &str = env!("CARGO_BIN_EXE_autumn-manager-server");
const EXTENT_NODE_BIN: &str = env!("CARGO_BIN_EXE_autumn-extent-node");
const AUTUMN_OP_BIN: &str = env!("CARGO_BIN_EXE_autumn-op");

/// The cluster secret file every server binary this test spawns requires, and
/// autumn-op proves (it connects as an operator).
fn cluster_secret_file() -> &'static std::path::Path {
    static FILE: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    FILE.get_or_init(|| {
        let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "cluster-secret-{}",
            std::process::id()
        ));
        std::fs::write(&path, "autumn-server-tests-cluster-secret-0123456789")
            .expect("write test cluster secret");
        path
    })
}

fn pick_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
    l.local_addr().expect("local_addr").port()
}

fn wait_port_open(port: u16, deadline: Duration) -> bool {
    let start = Instant::now();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    while start.elapsed() < deadline {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(name: String) -> Self {
        let p = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The first core this process may run on: the EN gets exactly one shard only
/// with an explicit one-core `--cpuset` (without it, one shard per core).
fn one_allowed_core() -> String {
    let status = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    let list = status
        .lines()
        .find_map(|l| l.strip_prefix("Cpus_allowed_list:"))
        .expect("Cpus_allowed_list")
        .trim();
    let cores = autumn_common::parse_cpuset(list).expect("parse Cpus_allowed_list");
    cores[0].to_string()
}

/// Start a single-shard EN on a freshly formatted dir with `extra` flags, wait
/// until it serves, and return its log.
fn en_startup_log(tmp: &std::path::Path, mgr_addr: &str, name: &str, extra: &[&str]) -> String {
    let dir = tmp.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    let out = Command::new(AUTUMN_OP_BIN)
        .arg("--cluster-secret-file")
        .arg(cluster_secret_file())
        .args(["--manager", mgr_addr, "format", dir.to_str().unwrap()])
        .output()
        .expect("spawn autumn-op format");
    assert!(out.status.success(), "autumn-op format: {out:?}");

    let log_path = tmp.join(format!("{name}.log"));
    let log = std::fs::File::create(&log_path).unwrap();
    let port = pick_port();
    let _en = ChildGuard(
        Command::new(EXTENT_NODE_BIN)
            .arg("--cluster-secret-file")
            .arg(cluster_secret_file())
            .args(["--port", &port.to_string(), "--listen", "127.0.0.1"])
            .args(["--control-port", &pick_port().to_string()])
            .args(["--cpuset", &one_allowed_core()])
            .args(["--data", dir.to_str().unwrap()])
            .args(extra)
            .env("RUST_LOG", "info")
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .expect("spawn extent-node"),
    );
    let serving = wait_port_open(port, Duration::from_secs(10));
    let text = std::fs::read_to_string(&log_path).unwrap();
    assert!(serving, "extent-node {extra:?} never listened; log:\n{text}");
    text
}

#[test]
fn extent_node_binary_defaults_to_direct_io() {
    let tmp = TempDir::new(format!("direct-io-default-{}", std::process::id()));
    let tmp = &tmp.0;

    let mgr_port = pick_port();
    let mgr_addr = format!("127.0.0.1:{mgr_port}");
    let _manager = ChildGuard(
        Command::new(MANAGER_BIN)
            .arg("--cluster-secret-file")
            .arg(cluster_secret_file())
            .args(["--port", &mgr_port.to_string(), "--listen", "127.0.0.1"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn manager"),
    );
    assert!(
        wait_port_open(mgr_port, Duration::from_secs(10)),
        "manager never listened"
    );

    let default = en_startup_log(tmp, &mgr_addr, "default", &[]);
    assert_eq!(
        default.matches("direct-io on").count(),
        1,
        "no flag: expected O_DIRECT on in the one shard; log:\n{default}"
    );

    let off = en_startup_log(tmp, &mgr_addr, "off", &["--no-direct-io"]);
    assert!(
        !off.contains("direct-io on"),
        "--no-direct-io: expected O_DIRECT off; log:\n{off}"
    );
}
