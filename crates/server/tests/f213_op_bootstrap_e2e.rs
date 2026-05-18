//! F213 e2e — exercise the autumn-op `bootstrap` + autumn-client
//! `put`/`get` round-trip through the actual binaries.
//!
//! Regression target: if anyone deletes a subcommand from autumn-op or
//! re-routes the data-plane through autumn-op, this test fails.
//!
//! Topology (single-node, in-memory manager, 1 EN + 1 PS, replication=1):
//!
//!   autumn-manager-server  (in-memory, no etcd)
//!   autumn-extent-node     (one disk dir)
//!   autumn-ps              (single partition, range = [..])
//!
//! Sequence mirrors `cluster.sh` for a 1-replica cluster:
//!   1. start manager
//!   2. start extent-node
//!   3. `autumn-op register-node` (registers the EN with the manager)
//!   4. start partition-server (auto-registers via PartitionServer::connect)
//!   5. `autumn-op bootstrap --replication 1+0`
//!   6. wait for PS region_sync to pick up the new partition (~2-3 s)
//!   7. `autumn-client put k1 <file>` ; `autumn-client get k1`
//!   8. assert stdout of `get` equals the put payload
//!
//! Cleanup: every spawned child is wrapped in a `ChildGuard` that kills
//! the process on Drop, so a panic mid-test still cleans up.

use std::io::Write;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const MANAGER_BIN: &str = env!("CARGO_BIN_EXE_autumn-manager-server");
const EXTENT_NODE_BIN: &str = env!("CARGO_BIN_EXE_autumn-extent-node");
const PARTITION_SERVER_BIN: &str = env!("CARGO_BIN_EXE_autumn-ps");
const AUTUMN_OP_BIN: &str = env!("CARGO_BIN_EXE_autumn-op");
const AUTUMN_CLIENT_BIN: &str = env!("CARGO_BIN_EXE_autumn-client");

/// Bind a TCP listener on an OS-allocated port, capture the port,
/// drop the listener. Race window between drop and the child re-binding
/// is small enough in practice for a single-threaded test.
fn pick_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
    let p = l.local_addr().expect("local_addr").port();
    drop(l);
    p
}

/// Poll a `127.0.0.1:port` until we can TCP-connect or the deadline elapses.
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

/// SIGKILLs the child on drop. We don't try to be graceful — these are
/// test-only processes with no persistent state we care about.
struct ChildGuard {
    name: &'static str,
    child: Option<Child>,
}

impl ChildGuard {
    fn new(name: &'static str, child: Child) -> Self {
        Self {
            name,
            child: Some(child),
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
            eprintln!("[e2e] killed {} (pid was {})", self.name, c.id());
        }
    }
}

/// Wrapper around `Command::output` that includes stdout + stderr in
/// the panic message so failures are debuggable.
fn run_or_panic(name: &str, mut cmd: Command) -> Vec<u8> {
    let out = cmd
        .output()
        .unwrap_or_else(|e| panic!("spawn {name}: {e}"));
    if !out.status.success() {
        panic!(
            "{name} exited {:?}\nstdout:\n{}\nstderr:\n{}",
            out.status.code(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
    }
    out.stdout
}

#[test]
fn autumn_op_bootstrap_then_put_get_roundtrip() {
    // Use a unique temp dir per test invocation. We do NOT rely on the
    // OS tmp dir; the binaries hash-allocate sub-dirs (`{data}/{hash}/...`)
    // and tests historically have collided on `/tmp/autumn-test-*`. A
    // per-PID dir avoids cross-test contention.
    let tmp = std::env::temp_dir().join(format!("f213-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    let data_dir = tmp.join("en1");
    let val_path = tmp.join("val.txt");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(&val_path, b"hello-from-f213-e2e").unwrap();

    let mgr_port = pick_port();
    let en_port = pick_port();
    let ps_port = pick_port();
    let mgr_addr = format!("127.0.0.1:{mgr_port}");
    let en_addr = format!("127.0.0.1:{en_port}");
    let ps_advertise = format!("127.0.0.1:{ps_port}");

    eprintln!("[e2e] mgr={mgr_addr} en={en_addr} ps={ps_advertise} data={data_dir:?}");

    // ── manager ─────────────────────────────────────────────────────
    let _manager = ChildGuard::new(
        "manager",
        Command::new(MANAGER_BIN)
            .args([
                "--port",
                &mgr_port.to_string(),
                "--listen",
                "127.0.0.1",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn manager"),
    );
    assert!(
        wait_port_open(mgr_port, Duration::from_secs(10)),
        "manager did not open port {mgr_port}"
    );

    // ── extent-node ─────────────────────────────────────────────────
    // `--cpuset 0` pins to a single core / single shard, so we don't
    // need to reserve a port range for sibling shards (F099-M sibling
    // ports at base+10/+20/... can collide with neighbouring tests).
    let _extent_node = ChildGuard::new(
        "extent-node",
        Command::new(EXTENT_NODE_BIN)
            .args([
                "--port",
                &en_port.to_string(),
                "--listen",
                "127.0.0.1",
                "--manager",
                &mgr_addr,
                "--data",
                data_dir.to_str().unwrap(),
                "--disk-id",
                "1",
                "--cpuset",
                "0",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn extent-node"),
    );
    assert!(
        wait_port_open(en_port, Duration::from_secs(10)),
        "extent-node did not open port {en_port}"
    );

    // ── register EN via autumn-op ───────────────────────────────────
    {
        let mut cmd = Command::new(AUTUMN_OP_BIN);
        cmd.args([
            "--manager",
            &mgr_addr,
            "register-node",
            "--addr",
            &en_addr,
            "--disk",
            "disk-1",
        ]);
        let stdout = run_or_panic("autumn-op register-node", cmd);
        let text = String::from_utf8_lossy(&stdout);
        assert!(
            text.contains("node registered"),
            "register-node output unexpected: {text}"
        );
    }

    // ── partition-server ────────────────────────────────────────────
    // F099-K note: PS does NOT bind `--port` itself; instead each
    // partition assigned to this PS binds `base_port + ord` (so the
    // first partition lands on `--port` = ps_port). Before bootstrap,
    // PS has zero partitions and therefore no listening sockets — we
    // can't wait_port_open here. Sleep briefly to let PS finish
    // RegisterPs / GetRegions with the manager, then bootstrap.
    let _ps = ChildGuard::new(
        "partition-server",
        Command::new(PARTITION_SERVER_BIN)
            .args([
                "--psid",
                "1",
                "--port",
                &ps_port.to_string(),
                "--listen",
                "127.0.0.1",
                "--manager",
                &mgr_addr,
                "--advertise",
                &ps_advertise,
                "--cpuset",
                "1",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn partition-server"),
    );
    std::thread::sleep(Duration::from_secs(2));

    // ── bootstrap ───────────────────────────────────────────────────
    {
        let mut cmd = Command::new(AUTUMN_OP_BIN);
        cmd.args([
            "--manager",
            &mgr_addr,
            "bootstrap",
            "--replication",
            "1+0",
        ]);
        let stdout = run_or_panic("autumn-op bootstrap", cmd);
        let text = String::from_utf8_lossy(&stdout);
        assert!(
            text.contains("bootstrap succeeded"),
            "bootstrap output unexpected: {text}"
        );
    }

    // PS picks up the new partition on the next region_sync tick (~2 s)
    // and binds its per-partition listener on `ps_port` (F099-K: first
    // partition = base_port + 0). Wait for that listener.
    assert!(
        wait_port_open(ps_port, Duration::from_secs(15)),
        "partition-server's first-partition listener did not open port {ps_port}"
    );
    std::thread::sleep(Duration::from_millis(500));

    // ── put + get round-trip via autumn-client ──────────────────────
    {
        let mut cmd = Command::new(AUTUMN_CLIENT_BIN);
        cmd.args([
            "--manager",
            &mgr_addr,
            "put",
            "k1",
            val_path.to_str().unwrap(),
        ]);
        let stdout = run_or_panic("autumn-client put", cmd);
        assert_eq!(
            String::from_utf8_lossy(&stdout).trim(),
            "ok",
            "put stdout unexpected"
        );
    }

    {
        let mut cmd = Command::new(AUTUMN_CLIENT_BIN);
        cmd.args(["--manager", &mgr_addr, "get", "k1"]);
        let stdout = run_or_panic("autumn-client get", cmd);
        assert_eq!(
            stdout,
            b"hello-from-f213-e2e",
            "get returned wrong bytes: {:?}",
            String::from_utf8_lossy(&stdout)
        );
    }

    // ── invariant probe: autumn-client `op` stub fires without manager ─
    // (the binary should NOT attempt to connect for `op` invocations).
    {
        let out = Command::new(AUTUMN_CLIENT_BIN)
            .args(["--manager", "127.0.0.1:1", "op", "split", "1"])
            .output()
            .expect("spawn autumn-client op");
        assert!(
            !out.status.success(),
            "op stub should exit non-zero, got {:?}",
            out.status
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("autumn-op")
                && stderr.contains("split 1"),
            "op stub stderr should mention autumn-op + the forwarded args, got:\n{stderr}"
        );
    }

    // Flush so test output shows the success line even if a Drop later
    // racy-prints kill notices.
    let _ = std::io::stdout().flush();
    eprintln!("[e2e] roundtrip OK");

    // _ps / _extent_node / _manager drop here in reverse order; the
    // ChildGuard impl kills + waits on each.
    let _ = std::fs::remove_dir_all(&tmp);
}
