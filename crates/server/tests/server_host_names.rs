//! The manager, extent node and partition server take host names in their
//! address flags (`--listen`, `--advertise`, `--manager`): each resolves them
//! once at startup, so what they register and dial is an IP literal. A name
//! that does not resolve stops the process at startup.

use std::net::{SocketAddr, ToSocketAddrs};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const MANAGER_BIN: &str = env!("CARGO_BIN_EXE_autumn-manager-server");
const EXTENT_NODE_BIN: &str = env!("CARGO_BIN_EXE_autumn-extent-node");
const PARTITION_SERVER_BIN: &str = env!("CARGO_BIN_EXE_autumn-ps");
const AUTUMN_OP_BIN: &str = env!("CARGO_BIN_EXE_autumn-op");
const AUTUMN_CLIENT_BIN: &str = env!("CARGO_BIN_EXE_autumn-client");

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

/// Until some address of `localhost:port` accepts.
fn wait_open(port: u16, what: &str) {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(20) {
        let addrs: Vec<SocketAddr> = ("localhost", port).to_socket_addrs().unwrap().collect();
        if addrs
            .iter()
            .any(|a| std::net::TcpStream::connect_timeout(a, Duration::from_millis(200)).is_ok())
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("{what} never opened localhost:{port}");
}

fn run(name: &str, mut cmd: Command) -> String {
    let out = cmd.output().unwrap_or_else(|e| panic!("spawn {name}: {e}"));
    assert!(
        out.status.success(),
        "{name} exited {:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
fn a_cluster_given_only_host_names_serves() {
    let tmp = TempDir(
        std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("server-host-names-{}", std::process::id())),
    );
    let _ = std::fs::remove_dir_all(&tmp.0);
    let data = tmp.0.join("en");
    std::fs::create_dir_all(&data).unwrap();
    let secret = tmp.0.join("secret");
    std::fs::write(&secret, "server-host-names-test-secret-0123456789abcdef").unwrap();
    let secret = secret.to_str().unwrap();
    let val = tmp.0.join("val");
    std::fs::write(&val, b"reached-by-name").unwrap();

    let mgr_port = autumn_test_ports::port_with_control();
    let en_port = autumn_test_ports::port_with_control();
    let ps_port = autumn_test_ports::reserve(&(0..4).collect::<Vec<_>>());
    let mgr = format!("localhost:{mgr_port}");
    let op = |args: &[&str]| {
        let mut cmd = Command::new(AUTUMN_OP_BIN);
        cmd.args(["--cluster-secret-file", secret, "--manager", &mgr]).args(args);
        cmd
    };

    let _manager = ChildGuard(
        Command::new(MANAGER_BIN)
            .args(["--cluster-secret-file", secret])
            .args(["--port", &mgr_port.to_string(), "--listen", "localhost"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn manager"),
    );
    wait_open(mgr_port, "manager");
    run("autumn-op format", op(&["format", data.to_str().unwrap()]));

    let _en = ChildGuard(
        Command::new(EXTENT_NODE_BIN)
            .args(["--cluster-secret-file", secret])
            .args(["--port", &en_port.to_string(), "--listen", "localhost"])
            .args(["--advertise", &format!("localhost:{en_port}"), "--manager", &mgr])
            .arg("--data")
            .arg(&data)
            .args(["--cpuset", "0"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn extent node"),
    );
    wait_open(en_port, "extent node");

    // A list of several managers, the first one dead: what a name with
    // several addresses becomes. The namespace cut below is a split, whose
    // range lookup has to walk it.
    let ps_managers = format!("127.0.0.1:1,{mgr}");
    let _ps = ChildGuard(
        Command::new(PARTITION_SERVER_BIN)
            .args(["--cluster-secret-file", secret, "--psid", "1"])
            .args(["--port", &ps_port.to_string(), "--listen", "localhost"])
            .args(["--advertise", &format!("localhost:{ps_port}"), "--manager", &ps_managers])
            .args(["--cpuset", "1"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn partition server"),
    );
    std::thread::sleep(Duration::from_secs(2));
    run("autumn-op bootstrap", op(&["bootstrap", "--replication", "1+0"]));
    wait_open(ps_port, "partition server");
    run("autumn-op namespace-create", op(&["namespace-create", "--name", "t"]));

    let client = |args: &[&str]| {
        let mut cmd = Command::new(AUTUMN_CLIENT_BIN);
        cmd.args(["--manager", &mgr, "--namespace", "t"]).args(args);
        cmd
    };
    run("autumn-client put", client(&["put", "k", val.to_str().unwrap()]));
    let got = run("autumn-client get", client(&["get", "k"]));
    assert_eq!(got.trim_end(), "reached-by-name");

    // What the manager holds is what it dials: IP literals, not the name.
    let nodes: serde_json::Value =
        serde_json::from_str(&run("autumn-op list-nodes", op(&["--json", "list-nodes"]))).unwrap();
    let en_addr = nodes[0]["address"].as_str().expect("the extent node is registered");
    let en_addr: SocketAddr = en_addr.parse().unwrap_or_else(|e| panic!("{en_addr:?}: {e}"));
    assert!(en_addr.ip().is_loopback() && en_addr.port() == en_port, "{en_addr}");
    let info = run("autumn-op info", op(&["--json", "info"]));
    assert!(!info.contains("localhost"), "a name was registered:\n{info}");
}

#[test]
fn a_name_that_does_not_resolve_stops_the_server() {
    let tmp = TempDir(
        std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("server-host-names-bad-{}", std::process::id())),
    );
    std::fs::create_dir_all(&tmp.0).unwrap();
    let secret = tmp.0.join("secret");
    std::fs::write(&secret, "server-host-names-test-secret-0123456789abcdef").unwrap();
    let bad = "no-such-host.invalid:9001";
    for (name, bin, args) in [
        ("manager", MANAGER_BIN, vec!["--listen", "no-such-host.invalid"]),
        ("partition server", PARTITION_SERVER_BIN, vec!["--psid", "1", "--manager", bad]),
        (
            "partition server advertise",
            PARTITION_SERVER_BIN,
            vec!["--psid", "1", "--manager", "127.0.0.1:1", "--advertise", "no-such-host.invalid:9201"],
        ),
        ("extent node", EXTENT_NODE_BIN, vec!["--data", "/nonexistent", "--manager", bad, "--advertise", "127.0.0.1:9101"]),
    ] {
        let out = Command::new(bin)
            .arg("--cluster-secret-file")
            .arg(&secret)
            .args(&args)
            .output()
            .expect("spawn");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(2), "{name}: {stderr}");
        assert!(stderr.contains("no-such-host.invalid"), "{name}: {stderr}");
    }
}
