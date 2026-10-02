//! `--cpuset` must move every work-unit thread onto its core even when the
//! process was started under a narrower affinity mask.
//!
//! Regression target: the P-log thread and the extent-node shard threads were
//! pinned through compio's `RuntimeBuilder::thread_affinity`, which intersects
//! the requested core with the mask the thread inherited and binds nothing when
//! the two are disjoint. A cluster launched as `taskset -c A,B cluster.sh start`
//! therefore ran every EN shard and P-log thread on A,B while each logged its
//! "assigned" core. Only P-sst, which pinned with `sched_setaffinity` directly,
//! actually moved.
//!
//! Every binary below is started under `taskset -c <launcher core>` with a
//! `--cpuset` that excludes that core, then each thread's `Cpus_allowed_list`
//! is read back from `/proc`.
//!
//! Topology: in-memory manager, one multi-shard EN, one single-shard EN (its
//! runtime lives on the main thread — a separate code path), one PS with one
//! partition (P-log + P-sst), replication 1.

#![cfg(target_os = "linux")] // /proc and taskset

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const MANAGER_BIN: &str = env!("CARGO_BIN_EXE_autumn-manager-server");
const EXTENT_NODE_BIN: &str = env!("CARGO_BIN_EXE_autumn-extent-node");
const PARTITION_SERVER_BIN: &str = env!("CARGO_BIN_EXE_autumn-ps");
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

impl ChildGuard {
    fn pid(&self) -> u32 {
        self.0.id()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(name: String) -> Self {
        let p = std::env::temp_dir().join(name);
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

fn run_or_panic(name: &str, mut cmd: Command) -> String {
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

/// `(thread name, allowed cores)` for every thread of `pid`.
fn thread_affinities(pid: u32) -> Vec<(String, Vec<usize>)> {
    let mut out = Vec::new();
    for task in std::fs::read_dir(format!("/proc/{pid}/task")).expect("read task dir") {
        let task = task.expect("task entry").path();
        let (Ok(comm), Ok(status)) = (
            std::fs::read_to_string(task.join("comm")),
            std::fs::read_to_string(task.join("status")),
        ) else {
            continue; // thread exited between readdir and read
        };
        let list = status
            .lines()
            .find_map(|l| l.strip_prefix("Cpus_allowed_list:"))
            .expect("Cpus_allowed_list")
            .trim();
        let cores = autumn_common::parse_cpuset(list).expect("parse Cpus_allowed_list");
        out.push((comm.trim().to_string(), cores));
    }
    out
}

/// The allowed cores of the threads of `pid` named `name`, which must agree.
/// A runtime's helper threads inherit the name of the thread that spawned them
/// (and, spawned after the pin, its affinity), so one work unit can show up as
/// several threads.
fn affinity_of(pid: u32, name: &str) -> Vec<usize> {
    let mut hits: Vec<Vec<usize>> = thread_affinities(pid)
        .into_iter()
        .filter(|(n, _)| n == name)
        .map(|(_, c)| c)
        .collect();
    assert!(!hits.is_empty(), "no thread named {name} in pid {pid}");
    hits.sort();
    hits.dedup();
    assert_eq!(
        hits.len(),
        1,
        "threads named {name} in pid {pid} disagree: {hits:?}"
    );
    hits.pop().unwrap()
}

fn format_dir(mgr_addr: &str, dir: &std::path::Path) {
    std::fs::create_dir_all(dir).unwrap();
    let mut cmd = Command::new(AUTUMN_OP_BIN);
    cmd.arg("--cluster-secret-file").arg(cluster_secret_file());
    cmd.args(["--manager", mgr_addr, "format", dir.to_str().unwrap()]);
    run_or_panic("autumn-op format", cmd);
}

/// `taskset -c <launcher> <bin> <args...>`, i.e. the binary starts with its
/// whole affinity mask set to `launcher`.
fn spawn_under_taskset(launcher: usize, bin: &str, args: &[&str]) -> ChildGuard {
    ChildGuard(
        Command::new("taskset")
            .arg("-c")
            .arg(launcher.to_string())
            .arg(bin)
            .arg("--cluster-secret-file")
            .arg(cluster_secret_file())
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {bin} under taskset: {e}")),
    )
}

#[test]
fn cpuset_moves_threads_off_a_narrower_launcher_mask() {
    let own = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    let allowed = autumn_common::parse_cpuset(
        own.lines()
            .find_map(|l| l.strip_prefix("Cpus_allowed_list:"))
            .expect("Cpus_allowed_list")
            .trim(),
    )
    .expect("parse own Cpus_allowed_list");
    assert!(
        allowed.len() >= 6,
        "this test needs 6 allowed cores (launcher + 2 EN shards + 1 single-shard EN + P-log + P-sst), have {allowed:?}"
    );
    // Take the LAST six allowed cores: the launcher core must not be in any
    // --cpuset, and each work unit gets its own.
    let c = &allowed[allowed.len() - 6..];
    let (launcher, en_a, en_b, en_single, p_log, p_sst) = (c[0], c[1], c[2], c[3], c[4], c[5]);

    // Declared before every child, so it drops after they are killed — on a
    // failed assertion too.
    let tmp = TempDir::new(format!("cpuset-pin-{}", std::process::id()));
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

    // Multi-shard EN: one thread per --cpuset core (`extent-shard-N`).
    let en_dir = tmp.join("en");
    format_dir(&mgr_addr, &en_dir);
    let en_port = pick_port();
    let en_addr = format!("127.0.0.1:{en_port}");
    let en_cpuset = format!("{en_a},{en_b}");
    let en = spawn_under_taskset(
        launcher,
        EXTENT_NODE_BIN,
        &[
            "--port",
            &en_port.to_string(),
            "--listen",
            "127.0.0.1",
            "--advertise",
            &en_addr,
            "--manager",
            &mgr_addr,
            "--data",
            en_dir.to_str().unwrap(),
            "--cpuset",
            &en_cpuset,
        ],
    );
    assert!(
        wait_port_open(en_port, Duration::from_secs(10)),
        "extent-node never listened"
    );

    // Single-shard EN: its runtime runs on the process's main thread. Not
    // registered for placement (no --manager), so it holds no replicas.
    let single_dir = tmp.join("single");
    format_dir(&mgr_addr, &single_dir);
    let single_port = pick_port();
    let single = spawn_under_taskset(
        launcher,
        EXTENT_NODE_BIN,
        &[
            "--port",
            &single_port.to_string(),
            "--listen",
            "127.0.0.1",
            "--data",
            single_dir.to_str().unwrap(),
            "--cpuset",
            &en_single.to_string(),
        ],
    );
    assert!(
        wait_port_open(single_port, Duration::from_secs(10)),
        "single-shard extent-node never listened"
    );

    // PS: partition ord 0 takes cpuset[0] for P-log and cpuset[1] for P-sst.
    let ps_port = pick_port();
    let ps_advertise = format!("127.0.0.1:{ps_port}");
    let ps_cpuset = format!("{p_log},{p_sst}");
    let ps = spawn_under_taskset(
        launcher,
        PARTITION_SERVER_BIN,
        &[
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
            &ps_cpuset,
        ],
    );
    std::thread::sleep(Duration::from_secs(2));
    let mut cmd = Command::new(AUTUMN_OP_BIN);
    cmd.arg("--cluster-secret-file").arg(cluster_secret_file());
    cmd.args(["--manager", &mgr_addr, "bootstrap", "--replication", "1+0"]);
    let out = run_or_panic("autumn-op bootstrap", cmd);
    let part_id: u64 = out
        .lines()
        .find_map(|l| l.split("created: id=").nth(1))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|id| id.parse().ok())
        .unwrap_or_else(|| panic!("no partition id in bootstrap output:\n{out}"));
    assert!(
        wait_port_open(ps_port, Duration::from_secs(15)),
        "partition {part_id} never listened on {ps_port}"
    );

    assert_eq!(
        affinity_of(en.pid(), "extent-shard-0"),
        vec![en_a],
        "EN shard 0"
    );
    assert_eq!(
        affinity_of(en.pid(), "extent-shard-1"),
        vec![en_b],
        "EN shard 1"
    );
    assert_eq!(
        affinity_of(single.pid(), "autumn-extent-n"),
        vec![en_single],
        "single-shard EN main thread"
    );
    assert_eq!(
        affinity_of(ps.pid(), &format!("part-{part_id}")),
        vec![p_log],
        "P-log"
    );
    assert_eq!(
        affinity_of(ps.pid(), &format!("part-{part_id}-sst")),
        vec![p_sst],
        "P-sst"
    );

    // io_uring's worker threads (`iou-wrk-*`) run the work the ring punts —
    // buffered writes, fsync — and take the NUMA node's cores unless the
    // runtime registers the cpuset for them. Opening the partition allocated
    // extents on the EN, so its shards have punted work by now.
    let en_workers = io_workers(en.pid());
    eprintln!(
        "iou-wrk: en={} single={} ps={}",
        en_workers.len(),
        io_workers(single.pid()).len(),
        io_workers(ps.pid()).len()
    );
    assert!(!en_workers.is_empty(), "the EN spawned no io_uring worker");
    for (pid, cpuset, what) in [
        (en.pid(), vec![en_a, en_b], "EN"),
        (single.pid(), vec![en_single], "single-shard EN"),
        (ps.pid(), vec![p_log, p_sst], "PS"),
    ] {
        for w in io_workers(pid) {
            assert!(
                w.iter().all(|c| cpuset.contains(c)),
                "{what}: io_uring worker allowed on {w:?}, outside --cpuset {cpuset:?}"
            );
        }
    }

    drop((ps, single, en));
}

/// `Cpus_allowed_list` of every io_uring worker thread of `pid`.
fn io_workers(pid: u32) -> Vec<Vec<usize>> {
    thread_affinities(pid)
        .into_iter()
        .filter(|(comm, _)| comm.starts_with("iou-wrk"))
        .map(|(_, cores)| cores)
        .collect()
}
