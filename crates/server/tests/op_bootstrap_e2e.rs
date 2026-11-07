//! e2e — exercise the autumn-op `bootstrap` + autumn-client
//! `put`/`get` round-trip through the actual binaries.
//!
//! Regression target: if anyone deletes a subcommand from autumn-op or
//! re-routes the data-plane through autumn-op, this test fails.
//!
//! Topology (single-node, in-memory manager, 1 EN + 1 PS, replication=1):
//!
//!   autumn-manager-server  (in-memory, no etcd)
//!   autumn-extent-node     (one disk dir)
//!   autumn-ps              (one partition from bootstrap; kvc then cut at
//!                           each hash bucket `kvc/<hex>`)
//!
//! Sequence mirrors `cluster.sh` for a 1-replica cluster:
//!   1. start manager
//!   2. start extent-node
//!   3. `autumn-op register-node` (registers the EN with the manager)
//!   4. start partition-server (auto-registers via PartitionServer::connect)
//!   5. `autumn-op bootstrap --replication 1+0`
//!   6. wait for PS region_sync to pick up the new partition (~2-3 s)
//!   7. nothing is registered: a client put is refused
//!   8. `namespace-create` fs, kvc and mem, each `--parts N`
//!   9. `autumn-client put k1 <file>` ; `autumn-client get k1`
//!  10. assert stdout of `get` equals the put payload
//!  11. kvc keys of the first and last hash bucket round-trip through their
//!      own partitions; the kvc bucket grid is recorded on the namespace
//!
//! Cleanup: every spawned child is wrapped in a `ChildGuard` that kills
//! the process on Drop, so a panic mid-test still cleans up.

use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const MANAGER_BIN: &str = env!("CARGO_BIN_EXE_autumn-manager-server");
const EXTENT_NODE_BIN: &str = env!("CARGO_BIN_EXE_autumn-extent-node");
const PARTITION_SERVER_BIN: &str = env!("CARGO_BIN_EXE_autumn-ps");
const AUTUMN_OP_BIN: &str = env!("CARGO_BIN_EXE_autumn-op");
const AUTUMN_CLIENT_BIN: &str = env!("CARGO_BIN_EXE_autumn-client");

/// The cluster secret file every server binary this test spawns requires, and
/// autumn-op proves (it connects as an operator).
fn cluster_secret_file() -> &'static std::path::Path {
    static FILE: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    FILE.get_or_init(|| {
        // One file shared by every test process, so none is left behind per
        // process: each writes the same bytes beside it and renames them over.
        let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("cluster-secret-server-tests");
        let tmp = path.with_extension(std::process::id().to_string());
        std::fs::write(&tmp, "autumn-server-tests-cluster-secret-0123456789")
            .expect("write test cluster secret");
        std::fs::rename(&tmp, &path).expect("install test cluster secret file");
        path
    })
}

/// Bind a TCP listener on an OS-allocated port, capture the port,
/// drop the listener. Race window between drop and the child re-binding
/// is small enough in practice for a single-threaded test.
fn pick_port() -> u16 {
    autumn_test_ports::port_with_control()
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
    let out = cmd.output().unwrap_or_else(|e| panic!("spawn {name}: {e}"));
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
    let tmp = std::env::temp_dir().join(format!("op-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    let data_dir = tmp.join("en1");
    let val_path = tmp.join("val.txt");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(&val_path, b"hello-from-op-e2e").unwrap();

    let mgr_port = pick_port();
    let en_port = pick_port();
    // One listener per partition: 25 once fs, kvc and mem are cut.
    let ps_port = autumn_test_ports::reserve(&(0..32).collect::<Vec<_>>());
    let mgr_addr = format!("127.0.0.1:{mgr_port}");
    let en_addr = format!("127.0.0.1:{en_port}");
    let ps_advertise = format!("127.0.0.1:{ps_port}");

    eprintln!("[e2e] mgr={mgr_addr} en={en_addr} ps={ps_advertise} data={data_dir:?}");

    // ── manager ─────────────────────────────────────────────────────
    let _manager = ChildGuard::new(
        "manager",
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
        "manager did not open port {mgr_port}"
    );

    // ── format the EN dir via autumn-op ────────────────────
    // unified `register-node` into `format`: one call fetches the
    // manager's cluster_id, allocates disk_uuid, registers the node,
    // and stamps sentinel files (`cluster_id` / `disk_uuid` /
    // `node_id` / `disk_id`) per data dir. The EN binary refuses to
    // start without these files, so format MUST run before
    // the extent-node spawn.
    {
        let mut cmd = Command::new(AUTUMN_OP_BIN);
        cmd.arg("--cluster-secret-file").arg(cluster_secret_file());
        // Format is identity-only — no location flags.
        cmd.args(["--manager", &mgr_addr, "format", data_dir.to_str().unwrap()]);
        let stdout = run_or_panic("autumn-op format", cmd);
        let text = String::from_utf8_lossy(&stdout);
        assert!(
            text.contains("node registered"),
            "format output unexpected: {text}"
        );
        assert!(
            text.contains("cluster_id="),
            "format output missing cluster_id: {text}"
        );
    }

    // ── extent-node ─────────────────────────────────────────────────
    // `--cpuset 0` pins to a single core / single shard. An earlier
    // change removed `--disk-id`; disk_id now comes from the sentinel file
    // written by `autumn-op format` above.
    let _extent_node = ChildGuard::new(
        "extent-node",
        Command::new(EXTENT_NODE_BIN)
            .arg("--cluster-secret-file")
            .arg(cluster_secret_file())
            .args([
                "--port",
                &en_port.to_string(),
                "--listen",
                "127.0.0.1",
                // --advertise is required with --manager;
                // the EN self-registers its live location (format is identity-only).
                "--advertise",
                &en_addr,
                "--manager",
                &mgr_addr,
                "--data",
                data_dir.to_str().unwrap(),
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

    // ── partition-server ────────────────────────────────────────────
    // note: PS does NOT bind `--port` itself; instead each
    // partition assigned to this PS binds `base_port + ord` (so the
    // first partition lands on `--port` = ps_port). Before bootstrap,
    // PS has zero partitions and therefore no listening sockets — we
    // can't wait_port_open here. Sleep briefly to let PS finish
    // RegisterPs / GetRegions with the manager, then bootstrap.
    let _ps = ChildGuard::new(
        "partition-server",
        Command::new(PARTITION_SERVER_BIN)
            .arg("--cluster-secret-file")
            .arg(cluster_secret_file())
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
        cmd.arg("--cluster-secret-file").arg(cluster_secret_file());
        cmd.args(["--manager", &mgr_addr, "bootstrap", "--replication", "1+0"]);
        let stdout = run_or_panic("autumn-op bootstrap", cmd);
        let text = String::from_utf8_lossy(&stdout);
        assert!(
            text.contains("bootstrap succeeded: 1 partition(s)"),
            "bootstrap output unexpected: {text}"
        );
    }

    // PS picks up the new partition on the next region_sync tick (~2 s)
    // and binds its per-partition listener on `ps_port` (first
    // partition = base_port + 0). Wait for that listener.
    assert!(
        wait_port_open(ps_port, Duration::from_secs(15)),
        "partition-server's first-partition listener did not open port {ps_port}"
    );
    std::thread::sleep(Duration::from_millis(500));

    // ── nothing is registered: a client write is refused ───────────
    {
        let out = Command::new(AUTUMN_CLIENT_BIN)
            .args(["--manager", &mgr_addr, "--namespace", "fs", "put", "k0", val_path.to_str().unwrap()])
            .output()
            .expect("spawn autumn-client put");
        assert!(!out.status.success(), "a put before any namespace-create must fail");
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("namespace"),
            "refused for its namespace: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // ── enable fs and kvc (docs/ops.md "Enabling fs / kvc") ─────────
    // fs with stripe lanes, whose geometry key sorts above every lane point;
    // then kvc and mem, each landing in a partition a previous cut left
    // sharing SSTs out of its range (refused until compacted).
    for args in [
        &["--name", "fs", "--parts", "4"][..],
        &["--name", "kvc", "--parts", "16"][..],
        &["--name", "mem", "--parts", "4"][..],
    ] {
        let mut cmd = Command::new(AUTUMN_OP_BIN);
        cmd.arg("--cluster-secret-file").arg(cluster_secret_file());
        cmd.args(["--manager", &mgr_addr, "namespace-create"]).args(args);
        let text = String::from_utf8_lossy(&run_or_panic("autumn-op namespace-create", cmd)).into_owned();
        // Each namespace is cut at its head; kvc also at its 15 bucket points.
        let want = if args.contains(&"kvc") { "16/16 cut points applied" } else { "4/4 cut points applied" };
        assert!(text.contains(want), "{args:?}: {text}");
    }
    {
        let mut cmd = Command::new(AUTUMN_OP_BIN);
        cmd.arg("--cluster-secret-file").arg(cluster_secret_file());
        cmd.args(["--manager", &mgr_addr, "--json", "info"]);
        let overview: serde_json::Value = serde_json::from_slice(&run_or_panic("autumn-op info", cmd)).unwrap();
        let starts: Vec<&str> =
            overview["partitions"].as_array().unwrap().iter().filter_map(|p| p["range_start"].as_str()).collect();
        for ns in ["fs/", "kvc/", "mem/"] {
            assert!(starts.contains(&ns), "{ns} starts its own partition: {starts:?}");
        }
        assert_eq!(starts.len(), 25, "\"\", fs/ + 3 lanes, kvc/ + kvc/1..kvc/f, mem/ + 3: {starts:?}");
    }

    // ── put + get round-trip via autumn-client ──────────────────────
    {
        let mut cmd = Command::new(AUTUMN_CLIENT_BIN);
        // Every KV command must declare its namespace; the client refuses
        // without one, which is what made this test exit 2.
        cmd.args([
            "--manager",
            &mgr_addr,
            "--namespace",
            "fs",
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
        cmd.args(["--manager", &mgr_addr, "--namespace", "fs", "get", "k1"]);
        let stdout = run_or_panic("autumn-client get", cmd);
        assert_eq!(
            stdout,
            b"hello-from-op-e2e",
            "get returned wrong bytes: {:?}",
            String::from_utf8_lossy(&stdout)
        );
    }

    // ── kvc: each hash bucket is its own partition ──────────────────
    // A model's keys sit under `kvc/<first hash char>/`, so buckets 0 and f
    // are served by the first and the last partition. The PS opens the 16
    // partitions on region sync; retry until the last one answers.
    for key in ["0/qwen_0_1/kv/0abc", "f/qwen_0_1/kv/fabc"] {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let out = Command::new(AUTUMN_CLIENT_BIN)
                .args(["--manager", &mgr_addr, "--namespace", "kvc", "put", key, val_path.to_str().unwrap()])
                .output()
                .expect("spawn autumn-client put");
            if out.status.success() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "kvc put {key}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            std::thread::sleep(Duration::from_millis(500));
        }
        let mut cmd = Command::new(AUTUMN_CLIENT_BIN);
        cmd.args(["--manager", &mgr_addr, "--namespace", "kvc", "get", key]);
        assert_eq!(run_or_panic("autumn-client get kvc", cmd), b"hello-from-op-e2e");
    }
    {
        let mut cmd = Command::new(AUTUMN_OP_BIN);
        cmd.arg("--cluster-secret-file").arg(cluster_secret_file());
        cmd.args(["--manager", &mgr_addr, "--json", "namespace-list"]);
        let rows: serde_json::Value =
            serde_json::from_slice(&run_or_panic("autumn-op namespace-list", cmd)).unwrap();
        let kvc = rows.as_array().unwrap().iter().find(|r| r["name"] == "kvc").unwrap_or_else(|| panic!("no kvc row: {rows}"));
        assert_eq!(kvc["presplit_points"], 16, "kvc's head and bucket grid are recorded: {rows}");
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
            stderr.contains("autumn-op") && stderr.contains("split 1"),
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
