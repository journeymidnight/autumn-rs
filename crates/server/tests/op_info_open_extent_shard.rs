//! e2e — `autumn-op info` must report an open extent's live length when the
//! extent lives on a sibling shard of a multi-shard extent node.
//!
//! An open extent's manager `sealed_length` is 0, so `info --part` (the
//! dashboard drawer) and `info --full` probe the EN for the length. Each EN
//! shard owns only its own extents and refuses a probe for another shard's, so
//! a probe sent to the node's base address (shard 0) fails and the extent
//! renders as 0 B — while the cluster overview, fed by the PS's own
//! shard-routed probe, counts its bytes.
//!
//! Topology: in-memory manager, one 8-shard EN, one PS, replication 1.

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const MANAGER_BIN: &str = env!("CARGO_BIN_EXE_autumn-manager-server");
const EXTENT_NODE_BIN: &str = env!("CARGO_BIN_EXE_autumn-extent-node");
const PARTITION_SERVER_BIN: &str = env!("CARGO_BIN_EXE_autumn-ps");
const AUTUMN_OP_BIN: &str = env!("CARGO_BIN_EXE_autumn-op");
const AUTUMN_CLIENT_BIN: &str = env!("CARGO_BIN_EXE_autumn-client");

const EN_SHARDS: u32 = 8;

fn cluster_secret_file() -> &'static std::path::Path {
    static FILE: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    FILE.get_or_init(|| {
        let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "cluster-secret-shard-{}",
            std::process::id()
        ));
        std::fs::write(&path, "autumn-server-tests-cluster-secret-0123456789")
            .expect("write test cluster secret");
        path
    })
}

fn pick_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
    let p = l.local_addr().expect("local_addr").port();
    drop(l);
    p
}

/// An EN base port whose shard ports (`+ i*10`) and control ports (`+ 1000`)
/// all fit in u16 and are free right now.
fn pick_en_port(shards: u16) -> u16 {
    loop {
        let base = pick_port();
        if base.checked_add(1000 + (shards - 1) * 10).is_none() {
            continue;
        }
        let all_free = (0..shards)
            .flat_map(|i| [base + i * 10, base + 1000 + i * 10])
            .all(|p| TcpListener::bind(("127.0.0.1", p)).is_ok());
        if all_free {
            return base;
        }
    }
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

fn spawn(bin: &str, args: &[&str]) -> ChildGuard {
    ChildGuard(
        Command::new(bin)
            .arg("--cluster-secret-file")
            .arg(cluster_secret_file())
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {bin}: {e}")),
    )
}

fn run(bin: &str, args: &[&str]) -> String {
    let mut cmd = Command::new(bin);
    if bin == AUTUMN_OP_BIN {
        cmd.arg("--cluster-secret-file").arg(cluster_secret_file());
    }
    let out = cmd.args(args).output().unwrap_or_else(|e| panic!("spawn {bin}: {e}"));
    assert!(
        out.status.success(),
        "{bin} {args:?} exited {:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8(out.stdout).expect("utf8 stdout")
}

fn json(text: &str) -> serde_json::Value {
    serde_json::from_str(text).unwrap_or_else(|e| panic!("bad json ({e}):\n{text}"))
}

/// The first `n` cores this process may run on, as a `--cpuset` list.
fn allowed_cores(n: usize) -> String {
    let status = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    let list = status
        .lines()
        .find_map(|l| l.strip_prefix("Cpus_allowed_list:"))
        .expect("Cpus_allowed_list")
        .trim();
    let cores = autumn_common::parse_cpuset(list).expect("parse Cpus_allowed_list");
    assert!(cores.len() >= n, "need {n} allowed cores, have {cores:?}");
    cores[..n].iter().map(|c| c.to_string()).collect::<Vec<_>>().join(",")
}

#[test]
fn info_probes_open_extents_on_their_own_shard() {
    let tmp = std::env::temp_dir().join(format!("op-info-shard-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    let data_dir = tmp.join("en1");
    std::fs::create_dir_all(&data_dir).unwrap();
    let val_path = tmp.join("val.bin");
    std::fs::write(&val_path, vec![7u8; 4096]).unwrap();

    let mgr_port = pick_port();
    let en_port = pick_en_port(EN_SHARDS as u16);
    let ps_port = pick_port();
    let mgr_addr = format!("127.0.0.1:{mgr_port}");
    let en_addr = format!("127.0.0.1:{en_port}");
    let ps_addr = format!("127.0.0.1:{ps_port}");

    let _manager = spawn(
        MANAGER_BIN,
        &["--port", &mgr_port.to_string(), "--listen", "127.0.0.1"],
    );
    assert!(wait_port_open(mgr_port, Duration::from_secs(10)), "manager port");

    run(
        AUTUMN_OP_BIN,
        &["--manager", &mgr_addr, "format", data_dir.to_str().unwrap()],
    );

    // Shard count = cpuset length.
    let en_cores = allowed_cores(EN_SHARDS as usize);
    let _extent_node = spawn(
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
            data_dir.to_str().unwrap(),
            "--cpuset",
            &en_cores,
        ],
    );
    assert!(wait_port_open(en_port, Duration::from_secs(10)), "extent-node port");

    let _ps = spawn(
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
            &ps_addr,
            "--cpuset",
            "1",
        ],
    );
    std::thread::sleep(Duration::from_secs(2));

    let out = run(
        AUTUMN_OP_BIN,
        &["--manager", &mgr_addr, "bootstrap", "--replication", "1+0"],
    );
    assert!(out.contains("bootstrap succeeded"), "bootstrap: {out}");
    assert!(
        wait_port_open(ps_port, Duration::from_secs(15)),
        "partition listener did not open"
    );
    std::thread::sleep(Duration::from_millis(500));

    for i in 0..8 {
        let key = format!("k{i}");
        let out = run(
            AUTUMN_CLIENT_BIN,
            &[
                "--manager",
                &mgr_addr,
                "--namespace",
                "fs",
                "put",
                &key,
                val_path.to_str().unwrap(),
            ],
        );
        assert_eq!(out.trim(), "ok", "put {key}");
    }

    let overview = json(&run(AUTUMN_OP_BIN, &["--json", "--manager", &mgr_addr, "info"]));
    let pid = overview["partitions"][0]["part_id"]
        .as_u64()
        .unwrap_or_else(|| panic!("no partition in overview:\n{overview:#}"));

    // The drawer's view.
    let part = json(&run(
        AUTUMN_OP_BIN,
        &["--json", "--manager", &mgr_addr, "info", "--part", &pid.to_string()],
    ));
    let extents = part["extents"].as_array().expect("extents array");
    let log = extents
        .iter()
        .find(|e| e["role"] == "log" && e["open"] == true)
        .unwrap_or_else(|| panic!("no open log extent:\n{part:#}"));
    let log_id = log["extent_id"].as_u64().unwrap();
    // Without this the test cannot tell a shard-routed probe from one sent to
    // the base address. Extent ids in a fresh cluster are deterministic.
    assert_ne!(
        autumn_rpc::shard_for_extent(log_id, EN_SHARDS),
        0,
        "open log extent {log_id} is on shard 0; the test needs it on a sibling shard"
    );
    let log_size = log["size"].as_u64().unwrap();
    assert!(
        log_size >= 8 * 4096,
        "info --part: open log extent {log_id} on shard {} reports {log_size} B after 8 x 4 KiB puts",
        autumn_rpc::shard_for_extent(log_id, EN_SHARDS)
    );

    // The full view probes through its own path.
    let full = json(&run(
        AUTUMN_OP_BIN,
        &["--json", "--manager", &mgr_addr, "info", "--full"],
    ));
    let full_log = full["extents"]
        .as_array()
        .expect("extents array")
        .iter()
        .find(|e| e["extent_id"].as_u64() == Some(log_id))
        .unwrap_or_else(|| panic!("extent {log_id} missing from info --full:\n{full:#}"));
    assert_eq!(
        full_log["size"].as_u64(),
        Some(log_size),
        "info --full disagrees with info --part on open extent {log_id}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}
