//! The autumnfs CLI's namespace commands go through the fs core
//! (`autumn_fs::dir`), so they get its conditions: an existing name is
//! refused, not overwritten. Driven through the real binaries.

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const MANAGER_BIN: &str = env!("CARGO_BIN_EXE_autumn-manager-server");
const EXTENT_NODE_BIN: &str = env!("CARGO_BIN_EXE_autumn-extent-node");
const PARTITION_SERVER_BIN: &str = env!("CARGO_BIN_EXE_autumn-ps");
const AUTUMN_OP_BIN: &str = env!("CARGO_BIN_EXE_autumn-op");
const AUTUMNFS_BIN: &str = env!("CARGO_BIN_EXE_autumnfs");

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

/// Runs autumnfs against `mgr`; exit code and stdout+stderr.
fn autumnfs(mgr: &str, args: &[&str]) -> (i32, String) {
    let out = Command::new(AUTUMNFS_BIN).args(["--manager", mgr]).args(args).output().expect("spawn autumnfs");
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    (out.status.code().unwrap_or(-1), text)
}

/// Removed on drop, after the processes declared later have been killed.
struct TmpDir(std::path::PathBuf);

impl Drop for TmpDir {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.0) {
            eprintln!("test dir {} not removed: {e}", self.0.display());
        }
    }
}

#[test]
fn autumnfs_refuses_an_existing_name() {
    let tmp = std::env::temp_dir().join(format!("autumnfs-cli-{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();
    let _tmp = TmpDir(tmp.clone());
    let data_dir = tmp.join("en1");
    std::fs::create_dir_all(&data_dir).unwrap();

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
        // M1c: format is identity-only — no location flags.
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
                // M1a/M1c: --advertise is required with --manager;
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
            text.contains("bootstrap succeeded"),
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

    let local = tmp.join("local.bin");
    std::fs::write(&local, vec![9u8; 100_000]).unwrap();
    let local = local.to_str().unwrap();

    let ok = |args: &[&str]| {
        let (code, text) = autumnfs(&mgr_addr, args);
        assert_eq!(code, 0, "autumnfs {args:?}: {text}");
        text
    };
    let refused = |args: &[&str]| {
        let (code, text) = autumnfs(&mgr_addr, args);
        assert_ne!(code, 0, "autumnfs {args:?} succeeded: {text}");
        text
    };
    ok(&["mkdir", "/d"]);
    let text = refused(&["mkdir", "/d"]);
    assert!(text.contains("EEXIST"), "{text}");
    ok(&["touch", "/d/f"]);
    ok(&["touch", "/d/f"]);
    ok(&["put", local, "/d/g"]);
    let text = refused(&["put", local, "/d/g"]);
    assert!(text.contains("already exists"), "{text}");
    let names = |listing: String| listing.lines().map(str::to_owned).collect::<Vec<_>>();
    assert_eq!(names(ok(&["ls", "/d"])), ["f", "g"]);
    // A file is no directory: nothing is created under it, and its link
    // count stays 1.
    for args in [["mkdir", "/d/g/sub"], ["touch", "/d/g/x"]] {
        let text = refused(&args);
        assert!(text.contains("ENOTDIR"), "{text}");
    }
    assert!(ok(&["stat", "/d/g"]).contains("nlink=1"));
    ok(&["rm", "/d/f"]);
    assert_eq!(names(ok(&["ls", "/d"])), ["g"]);
}
