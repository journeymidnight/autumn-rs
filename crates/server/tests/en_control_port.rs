//! An extent node started with an explicit `--control-port` registers that
//! port, so the manager's node-health poll (`df`, sent to the control address)
//! reaches it. The node used to register its data port + 1000 whatever the
//! flag said: the manager then polled a port nobody listened on, the node's
//! last heartbeat only aged, and its disks were marked offline while it ran.

use std::net::{SocketAddr, TcpListener};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const MANAGER_BIN: &str = env!("CARGO_BIN_EXE_autumn-manager-server");
const EXTENT_NODE_BIN: &str = env!("CARGO_BIN_EXE_autumn-extent-node");
const AUTUMN_OP_BIN: &str = env!("CARGO_BIN_EXE_autumn-op");

fn pick_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
    l.local_addr().expect("local_addr").port()
}

fn wait_port_open(port: u16) {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(20) {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("port {port} never opened");
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct TempDir(std::path::PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The first `n` cores this process may run on, as a `--cpuset`: the EN runs
/// one shard per core.
fn allowed_cores(n: usize) -> String {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let list = status
        .lines()
        .find_map(|l| l.strip_prefix("Cpus_allowed_list:"))
        .unwrap()
        .trim();
    let cores = autumn_common::parse_cpuset(list).unwrap();
    assert!(cores.len() >= n, "need {n} allowed cores, have {}", cores.len());
    cores[..n].iter().map(|c| c.to_string()).collect::<Vec<_>>().join(",")
}

/// `last_heartbeat_secs_ago` of the node at `addr`, if it is registered.
fn heartbeat_age(secret: &str, mgr: &str, addr: &str) -> Option<u64> {
    let out = Command::new(AUTUMN_OP_BIN)
        .args(["--cluster-secret-file", secret, "--manager", mgr, "--json", "list-nodes"])
        .output()
        .expect("run autumn-op");
    let nodes: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
    nodes
        .as_array()?
        .iter()
        .find(|n| n["address"] == addr)?["last_heartbeat_secs_ago"]
        .as_u64()
}

/// The single-shard and the multi-shard node register through different
/// paths; both must name the control port they bind.
#[test]
fn an_explicit_control_port_is_the_one_the_manager_polls_single_shard() {
    explicit_control_port_is_polled(1);
}

#[test]
fn an_explicit_control_port_is_the_one_the_manager_polls_two_shards() {
    explicit_control_port_is_polled(2);
}

fn explicit_control_port_is_polled(shards: usize) {
    let tmp = TempDir(
        std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("en-control-port-{shards}-{}", std::process::id())),
    );
    let _ = std::fs::remove_dir_all(&tmp.0);
    std::fs::create_dir_all(tmp.0.join("en")).unwrap();
    let secret_file = tmp.0.join("secret");
    std::fs::write(&secret_file, "en-control-port-test-secret-0123456789abcdef").unwrap();
    let secret = secret_file.to_str().unwrap();

    let mgr_port = pick_port();
    let mgr = format!("127.0.0.1:{mgr_port}");
    let _manager = ChildGuard(
        Command::new(MANAGER_BIN)
            .args(["--cluster-secret-file", secret])
            .args(["--port", &mgr_port.to_string(), "--listen", "127.0.0.1"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn manager"),
    );
    wait_port_open(mgr_port);
    let out = Command::new(AUTUMN_OP_BIN)
        .args(["--cluster-secret-file", secret, "--manager", &mgr, "format"])
        .arg(tmp.0.join("en"))
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));

    // Any free port but the default data port + 1000.
    let en_port = pick_port();
    let control_port = std::iter::repeat_with(pick_port)
        .find(|p| u32::from(*p) != u32::from(en_port) + 1000)
        .unwrap();
    let addr = format!("127.0.0.1:{en_port}");
    let _en = ChildGuard(
        Command::new(EXTENT_NODE_BIN)
            .args(["--cluster-secret-file", secret])
            .args(["--port", &en_port.to_string(), "--listen", "127.0.0.1"])
            .args(["--control-port", &control_port.to_string()])
            .arg("--data")
            .arg(tmp.0.join("en"))
            .args(["--manager", &mgr, "--advertise", &addr])
            .args(["--cpuset", &allowed_cores(shards)])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn extent node"),
    );
    wait_port_open(control_port);

    // The manager polls every 2 s. Past a few polls, a node it reaches has
    // just been heard from; one it cannot reach ages with the clock.
    std::thread::sleep(Duration::from_secs(7));
    let age = heartbeat_age(secret, &mgr, &addr).expect("the extent node is not registered");
    assert!(age <= 3, "last heartbeat {age} s ago: the manager is not reaching the control port");
}
