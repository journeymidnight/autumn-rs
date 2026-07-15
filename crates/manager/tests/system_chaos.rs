//! Jepsen-style chaos e2e — workload + nemesis + checker.
//!
//! **Three pieces** (Aphyr's pattern):
//!   1. *Workload* — concurrent client tasks doing put/get + per-key
//!      register expectations.
//!   2. *Nemesis* — independent task that injects faults on a schedule:
//!      split / merge / EC convert / flush / compact / GC /
//!      fence+unfence / **real process SIGKILL** of an extent node /
//!      kill-then-fence (operator declares dead node).
//!   3. *Checker* — at end of run, verify every acked put still
//!      reads back the correct value, AND that `range()` per partition
//!      returns every expected key in that range.
//!
//! **Real process kills.** ENs run as `autumn-extent-node` SUBPROCESSES
//! (formatted via `autumn-op format` first), so SIGKILL exercises the
//! same disk-state-recovery + df-failure path as a production crash.
//! Manager + PS stay in-process for simplicity — the failure scenarios
//! we care about (fence + recovery + EC convert + split/merge) all
//! exercise EN-side persistence, which is the surface this test
//! validates.
//!
//! **Build requirements.** This test needs:
//!   - The workspace binaries at `target/debug/` — run `cargo build
//!     --workspace` first.
//!   - The `etcd` binary on `$PATH` (or `AUTUMN_TEST_ETCD_BIN` set).
//!     The manager runs in etcd-persistent mode so F149 leader fence,
//!     F207 inflight ledger, F211-D owner_epoch bumps, and F198 rich EC
//!     markers all exercise the real durable code paths (memory-only
//!     mode disables most of these).
//!
//! Env knobs:
//!   - AUTUMN_CHAOS_DURATION_SECS (default 30)
//!   - AUTUMN_CHAOS_NEMESIS_INTERVAL_MS (default 3000)
//!   - AUTUMN_CHAOS_EC_K (default 3)  — data shards
//!   - AUTUMN_CHAOS_EC_M (default 1)  — parity shards (0 = pure replication)
//!   - AUTUMN_CHAOS_SEED (default = system time millis)
//!   - AUTUMN_CHAOS_NUM_ENS (default = max(K+M+1, 5)) — at least one spare
//!
//! Run:
//!     cargo build --workspace
//!     cargo test -p autumn-manager --test system_chaos -- --ignored --nocapture

mod support;

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;

use support::*;

/// Spawn manager in etcd-persistent mode on a background thread. Mirrors
/// `start_etcd_manager` in `f149_leader_fence.rs` — kept inline here so
/// the chaos test stays a single-file deliverable.
fn start_etcd_manager(mgr_addr: SocketAddr, etcd_endpoint: String) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new_with_etcd(vec![etcd_endpoint])
                .await
                .expect("new manager with etcd");
            let _ = manager.serve(mgr_addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(500));
}

// ── Config ─────────────────────────────────────────────────────────────

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct ChaosConfig {
    duration_secs: u64,
    nemesis_interval_ms: u64,
    ec_k: u32,
    ec_m: u32,
    num_ens: u32,
    seed: u64,
    /// Comma-separated subset of action names to enable. Empty = all.
    /// Names: split,merge,ec,fence,flush,compact,gc,forcegc,kill,killfence,partition,latency
    /// Useful for bisecting which action triggers a failure.
    actions: Vec<Action>,
    /// `AUTUMN_CHAOS_DECOMMISSION=1` runs a terminal node-decommission phase
    /// after the nemesis loop stops (fence → drain → MSG_REMOVE_NODE → tombstone),
    /// then the existing verify confirms no data loss with the node gone. Off by
    /// default — it is a NON-reversible one-shot, unlike the per-cycle nemesis
    /// actions (which must all be reversible).
    decommission: bool,
}

impl ChaosConfig {
    fn from_env() -> Self {
        let ec_k = env_u32("AUTUMN_CHAOS_EC_K", 3);
        let ec_m = env_u32("AUTUMN_CHAOS_EC_M", 1);
        let min_ens = (ec_k + ec_m).max(3) + 1;
        let num_ens = env_u32("AUTUMN_CHAOS_NUM_ENS", min_ens.max(5));
        assert!(
            num_ens >= ec_k + ec_m,
            "AUTUMN_CHAOS_NUM_ENS ({num_ens}) must be ≥ K+M ({}+{})",
            ec_k,
            ec_m
        );
        let seed = std::env::var("AUTUMN_CHAOS_SEED")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0xDEADBEEF)
            });
        let actions = match std::env::var("AUTUMN_CHAOS_ACTIONS").ok() {
            None => ALL_ACTIONS.to_vec(),
            Some(s) => s
                .split(',')
                .map(|n| match n.trim() {
                    "split" => Action::Split,
                    "merge" => Action::Merge,
                    "ec" => Action::EcConvert,
                    "fence" => Action::FenceUnfence,
                    "flush" => Action::Flush,
                    "compact" => Action::Compact,
                    "gc" => Action::Gc,
                    "forcegc" => Action::ForceGc,
                    "kill" => Action::KillEn,
                    "killfence" => Action::KillThenFence,
                    "partition" => Action::NetworkPartition,
                    "latency" => Action::LatencySpike,
                    other => panic!("unknown action name: {other}"),
                })
                .collect(),
        };
        assert!(
            !actions.is_empty(),
            "AUTUMN_CHAOS_ACTIONS must have at least one action"
        );
        let decommission = std::env::var("AUTUMN_CHAOS_DECOMMISSION")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        Self {
            duration_secs: env_u64("AUTUMN_CHAOS_DURATION_SECS", 30),
            nemesis_interval_ms: env_u64("AUTUMN_CHAOS_NEMESIS_INTERVAL_MS", 3000),
            ec_k,
            ec_m,
            num_ens,
            seed,
            actions,
            decommission,
        }
    }
}

// ── Deterministic LCG ──────────────────────────────────────────────────

#[derive(Clone)]
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }
    fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo)
    }
}

// ── Binary path discovery ──────────────────────────────────────────────

fn workspace_target_dir() -> PathBuf {
    // CARGO_MANIFEST_DIR points at crates/manager. Workspace root is two
    // up. `target/debug` is the conventional output dir.
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf();
    // Respect CARGO_TARGET_DIR if set.
    match std::env::var("CARGO_TARGET_DIR") {
        Ok(d) => PathBuf::from(d).join("debug"),
        Err(_) => workspace.join("target").join("debug"),
    }
}

fn binary_path(name: &str) -> PathBuf {
    let p = workspace_target_dir().join(name);
    if !p.exists() {
        panic!(
            "binary {name} not found at {}. Run `cargo build --workspace` first.",
            p.display()
        );
    }
    p
}

/// Pick a free port `p` such that `p + 1000` is ALSO free — the EN's
/// toxiproxy pair binds the data proxy at `p` and the control proxy at
/// `p + 1000` (the manager derives `control_address = advertise + 1000`,
/// so the control proxy's listen port is forced, not free-choice).
/// Unlike `pick_stable_port_pair` this need not dodge the ephemeral
/// range: proxies live for the whole run and are never re-bound.
fn pick_proxy_port_pair() -> u16 {
    for _ in 0..1000 {
        let p = pick_addr().port();
        let Some(ctl) = p.checked_add(1000) else {
            continue;
        };
        if let Ok(l) = std::net::TcpListener::bind(("127.0.0.1", ctl)) {
            drop(l);
            return p;
        }
    }
    panic!("pick_proxy_port_pair: no free (p, p+1000) proxy port pair found");
}

// ── ProcessGuard: managed subprocess EN ────────────────────────────────

struct EnProcess {
    child: Option<Child>,
    /// Real port the EN binds (loopback). Manager/PS NEVER connect to
    /// this directly — they go through `proxy_port`.
    port: u16,
    /// Toxiproxy listener that fronts `port`. This is the advertise
    /// address handed to the manager via `autumn-op format`, so all
    /// traffic from manager + PS to this EN routes through it. Stored
    /// for diagnostics only — nemesis identifies the proxy by `proxy_name`.
    #[allow(dead_code)]
    proxy_port: u16,
    /// Toxiproxy proxy name (stable across kill/restart). Used by
    /// nemesis actions to disable/poison this EN's network link.
    proxy_name: String,
    data_dir: PathBuf,
    /// Node-id assigned by the manager after `autumn-op format`'s
    /// `register_node` call. Stable across kill/restart (sentinel files
    /// carry it).
    node_id: u64,
    /// Where to find logs for diagnosis.
    log_path: PathBuf,
}

impl EnProcess {
    fn is_alive(&self) -> bool {
        self.child.is_some()
    }

    /// SIGKILL the EN and wait for it to reap. Data dir + sentinel files
    /// stay so `restart` can bring it back.
    fn kill(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// Spawn a fresh `autumn-extent-node` against the same data dir.
    /// Format has already stamped sentinel files; we just relaunch.
    fn restart(&mut self, en_binary: &Path, manager_addr: &SocketAddr) {
        assert!(self.child.is_none(), "EN must be killed before restart");
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
            .expect("open log");
        // F-EN-DYNSHARD M1a/M1c: the EN self-registers its location now (format
        // is identity-only). It BINDS its real port (`self.port`) but ADVERTISES
        // the toxiproxy data port (`self.proxy_port`) — so the manager routes
        // data → proxy_port and control → proxy_port+1000 (both toxiproxy-fronted),
        // exactly what `format --advertise proxy_port` did pre-M1c. advertise_port
        // != --port here is the legitimate proxy case (the EN warns, not fails).
        let advertise = format!("127.0.0.1:{}", self.proxy_port);
        let child = Command::new(en_binary)
            .args([
                "--port",
                &self.port.to_string(),
                "--data",
                self.data_dir.to_str().unwrap(),
                "--manager",
                &manager_addr.to_string(),
                "--listen",
                "127.0.0.1",
                "--advertise",
                &advertise,
                // F196: cap shard count to 1 (default = cpuset_len; on a
                // 192-core test box that's 192 listeners per EN × N ENs).
                // Single-shard is enough for the chaos contract — F099-M
                // routing is exercised by the multi-EN cluster, not
                // multi-shard per EN.
                "--cpuset",
                "0",
            ])
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawn extent-node");
        self.child = Some(child);
    }
}

impl Drop for EnProcess {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Format a fresh EN dir via `autumn-op format`, then spawn an
/// `autumn-extent-node` subprocess. The format step is what stamps
/// `cluster_id` / `disk_id` / `disk_uuid` / `node_id` sentinel files
/// that the EN startup requires (F214-D).
///
/// **Toxiproxy ordering** (load-bearing): the proxy MUST be created
/// *before* `format` runs, because format's advertise address (= proxy
/// port) gets persisted to the manager's `nodes/` etcd entry. After
/// that, manager + PS see only the proxy address; the real EN port is
/// internal. Then we spawn the actual EN listening on the real port.
fn bootstrap_en(
    op_binary: &Path,
    en_binary: &Path,
    manager_addr: &SocketAddr,
    port: u16,
    proxy_port: u16,
    proxy_name: String,
    toxi: &ToxiproxyCli,
    data_dir: PathBuf,
    log_dir: &Path,
) -> EnProcess {
    // 1. Create the toxiproxy proxy now, so format's advertise address
    //    (= proxy listener) is already bound and reachable. Upstream is
    //    the real EN port we'll spawn last.
    toxi.create(
        &proxy_name,
        &format!("127.0.0.1:{proxy_port}"),
        &format!("127.0.0.1:{port}"),
    )
    .expect("create toxiproxy proxy");

    // 1b. F-CHAOS-DECOMMISSION root-cause fix: ALSO proxy the EN CONTROL
    //     port. `autumn-op format` registers `control_address` derived from
    //     the ADVERTISE address (+1000 → proxy_port+1000), and the manager's
    //     `node_health_loop` sends every `EXT_MSG_DF` there — the ONLY
    //     channel that drains the EN's `recovery_done` and drives
    //     `apply_recovery_done`. Pre-fix nothing listened on
    //     proxy_port+1000 (toxiproxy fronted only the data port), so EVERY
    //     df in this harness failed silently: recovery completions were
    //     never applied, a fenced node's slots were never rewritten, and
    //     `MSG_REMOVE_NODE` stayed Precondition forever (the decommission
    //     "drain wedge"). The caller guarantees proxy_port+1000 is free
    //     (pick_proxy_port_pair).
    toxi.create(
        &format!("{proxy_name}-ctl"),
        &format!("127.0.0.1:{}", proxy_port + 1000),
        &format!("127.0.0.1:{}", port + 1000),
    )
    .expect("create toxiproxy control proxy");

    // 2. autumn-op format <DIR> — F-EN-DYNSHARD M1c: IDENTITY-ONLY, no location
    //    flags. Talks to the manager, allocates node_uuid + disk_uuid(s), stamps
    //    sentinel files. The EN's own --advertise (EnProcess::restart) reports
    //    the PROXY address at startup. Synchronous.
    let format_log = log_dir.join(format!("format-{port}.log"));
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&format_log)
        .expect("open format log");
    let status = Command::new(op_binary)
        .args([
            "--manager",
            &manager_addr.to_string(),
            "format",
            data_dir.to_str().unwrap(),
        ])
        .stdout(Stdio::from(log_file.try_clone().unwrap()))
        .stderr(Stdio::from(log_file))
        .status()
        .expect("run autumn-op format");
    assert!(
        status.success(),
        "autumn-op format failed for {} — see {}",
        data_dir.display(),
        format_log.display()
    );

    // 3. Read `node_id` from the sentinel file. Path:
    //    <data_dir>/node_id  (raw u64 decimal text).
    let nid_path = data_dir.join("node_id");
    let nid_str = std::fs::read_to_string(&nid_path).expect("read node_id sentinel after format");
    let node_id: u64 = nid_str.trim().parse().expect("parse node_id");

    // 4. Spawn the EN subprocess on its real port (upstream of the proxy).
    let en_log = log_dir.join(format!("en-{port}.log"));
    let mut guard = EnProcess {
        child: None,
        port,
        proxy_port,
        proxy_name,
        data_dir,
        node_id,
        log_path: en_log,
    };
    guard.restart(en_binary, manager_addr);
    guard
}

// ── Workload state ─────────────────────────────────────────────────────

/// Topology snapshot from `GetRegions`. Workload routes by lookup:
/// largest `start_key ≤ user_key` wins.
struct Topology {
    parts: RefCell<Vec<(Vec<u8>, Vec<u8>, u64)>>, // (start, end, part_id)
}

impl Topology {
    fn new() -> Self {
        Self {
            parts: RefCell::new(Vec::new()),
        }
    }

    fn route(&self, key: &[u8]) -> u64 {
        let parts = self.parts.borrow();
        // pick the partition whose range contains key
        for (start, end, pid) in parts.iter() {
            let after_start = key >= start.as_slice();
            // end_key == b"\xff\xff\xff\xff" is the sentinel for last
            // partition; we treat any end > key as "in range".
            let before_end = end.is_empty() || key < end.as_slice();
            if after_start && before_end {
                return *pid;
            }
        }
        // Fallback: first partition.
        parts[0].2
    }

    fn snapshot(&self) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
        self.parts.borrow().clone()
    }
}

async fn refresh_topology(mgr: &RpcClient, topo: &Topology) {
    let regions = get_regions(mgr).await;
    let mut new_parts: Vec<(Vec<u8>, Vec<u8>, u64)> = regions
        .regions
        .iter()
        .filter_map(|(_, r)| {
            r.rg.as_ref()
                .map(|rg| (rg.start_key.clone(), rg.end_key.clone(), r.part_id))
        })
        .collect();
    new_parts.sort_by(|a, b| a.0.cmp(&b.0));
    *topo.parts.borrow_mut() = new_parts;
}

/// Number of distinct keys per writer prefix. Both the writers and the
/// no-phantom range checker use this so the "valid key space" is one source of
/// truth: keys are exactly `{b|q}{kid:06}` for `kid in [0, CHAOS_KEY_COUNT)`.
const CHAOS_KEY_COUNT: u32 = 200;

/// Parse a chaos key `{b|q}{6 ASCII digits}` → its `kid`, or None if it is not a
/// well-formed key any writer could have produced. Used by the no-phantom range
/// check: a range MUST NOT return a key outside this space (a malformed key, a
/// kid the writers never use, or a sibling key leaked across a split/merge
/// boundary would all be data-corruption signals).
fn chaos_kid(key: &[u8]) -> Option<u32> {
    if key.len() == 7
        && (key[0] == b'b' || key[0] == b'q')
        && key[1..].iter().all(u8::is_ascii_digit)
    {
        std::str::from_utf8(&key[1..]).ok()?.parse::<u32>().ok()
    } else {
        None
    }
}

/// True iff `key` is a key a writer could legitimately have written.
fn is_valid_chaos_key(key: &[u8]) -> bool {
    matches!(chaos_kid(key), Some(kid) if kid < CHAOS_KEY_COUNT)
}

fn make_value(key: &[u8], seq: u64) -> Vec<u8> {
    // VP/large-value coverage (coco arch gap #5/#9): ~1/8 of keys carry a value
    // ABOVE the 4 KiB VALUE_THROTTLE so they take the ValuePointer path
    // (value stored in log_stream, VP in the SSTable) and get exercised by the
    // GC punch_holes / dangling-VP-read path on overwrite. The size is a
    // deterministic function of the key, so a given key is ALWAYS the same
    // length (VP-path consistency), and `seq` + `key` are encoded at the front
    // so any byte corruption — small OR large — is caught by `verify_per_key`.
    let big = matches!(chaos_kid(key), Some(kid) if kid.is_multiple_of(8));
    let target = if big { 8192 } else { 256 };
    let mut out = Vec::with_capacity(target);
    out.extend_from_slice(b"chaos-");
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(b":");
    out.extend_from_slice(key);
    while out.len() < target {
        out.extend_from_slice(b"x");
    }
    out
}

// ── Writer / Reader tasks ──────────────────────────────────────────────

async fn writer_loop(
    name: &'static str,
    router: Rc<PsRouter>,
    topo: Rc<Topology>,
    expected: Rc<RefCell<HashMap<Vec<u8>, Vec<u8>>>>,
    key_prefix: u8,
    key_count: u32,
    stop: Arc<AtomicBool>,
    writes_acked: Arc<AtomicU64>,
    writes_failed: Arc<AtomicU64>,
    mut lcg: Lcg,
) {
    let mut seq: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        seq += 1;
        let kid = lcg.range(0, key_count as u64) as u32;
        let key = format!("{}{:06}", key_prefix as char, kid).into_bytes();
        let value = make_value(&key, seq);
        let part_id = topo.route(&key);

        let payload = partition_rpc::rkyv_encode(&partition_rpc::PutReq {
            part_id,
            key: key.clone(),
            value: value.clone(),
            expires_at: 0,
            region_epoch: 0,
        inode_hint: 0,
        lease_epoch: 0,
        });

        // try_client_for: if partition is transiently unreachable
        // (mid-split, mid-merge, region_sync lag), skip this put
        // rather than panic the writer task.
        let client = match router.try_client_for(part_id).await {
            Ok(c) => c,
            Err(_) => {
                writes_failed.fetch_add(1, Ordering::Relaxed);
                compio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        // Bounded: a wedged PS that accepts the connection but never
        // replies would otherwise hang this writer forever (it only checks
        // `stop` between calls), so shutdown-join would hang the whole test.
        // Treat a timeout as a failed write and loop (re-checks `stop`).
        let put_call = match compio::time::timeout(
            Duration::from_secs(5),
            client.call(partition_rpc::MSG_PUT, payload),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => {
                // A client-side timeout is an UNKNOWN outcome: the append may
                // still land server-side after a transient stall (e.g. F227
                // all-replica blocks while a replica is network-partitioned,
                // then completes once it heals). So we must NOT treat the key
                // as "old value" — that produced false "got seq > expected"
                // mismatches. Forget the key entirely; its state is uncertain
                // until a later write to it succeeds and re-records it.
                eprintln!("writer[{name}]: PUT TIMED OUT (5s) part_id={part_id} — uncertain, dropping key from expected");
                expected.borrow_mut().remove(&key);
                writes_failed.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        match put_call {
            Ok(resp) => match partition_rpc::rkyv_decode::<partition_rpc::PutResp>(&resp) {
                // Only record `expected[]` when the PS actually accepted
                // the put — `CODE_OK`. A successful wire decode with
                // (e.g.) `CODE_INVALID_ARGUMENT` ("key out of range"
                // when topo is stale post-split, or
                // `CODE_FAILED_PRECONDITION` region_epoch mismatch) is
                // a REJECTED write, not an acked one. Pre-fix this was
                // unconditional and produced false "data loss"
                // mismatches at verify time on b*/q* boundary keys.
                Ok(r) if r.code == partition_rpc::CODE_OK => {
                    expected.borrow_mut().insert(key, value);
                    writes_acked.fetch_add(1, Ordering::Relaxed);
                }
                Ok(_) => {
                    writes_failed.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    writes_failed.fetch_add(1, Ordering::Relaxed);
                }
            },
            Err(_) => {
                writes_failed.fetch_add(1, Ordering::Relaxed);
                compio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        if seq.is_multiple_of(16) {
            compio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    eprintln!("writer[{name}] stopped: seq={seq}");
}

async fn reader_loop(
    name: &'static str,
    router: Rc<PsRouter>,
    topo: Rc<Topology>,
    expected: Rc<RefCell<HashMap<Vec<u8>, Vec<u8>>>>,
    stop: Arc<AtomicBool>,
    reads_ok: Arc<AtomicU64>,
    reads_miss: Arc<AtomicU64>,
    mut lcg: Lcg,
) {
    while !stop.load(Ordering::Relaxed) {
        let sample: Option<(Vec<u8>, Vec<u8>)> = {
            let exp = expected.borrow();
            if exp.is_empty() {
                None
            } else {
                let n = exp.len();
                let idx = (lcg.next() as usize) % n;
                exp.iter().nth(idx).map(|(k, v)| (k.clone(), v.clone()))
            }
        };
        let Some((key, want)) = sample else {
            compio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };

        let part_id = topo.route(&key);
        let client = match router.try_client_for(part_id).await {
            Ok(c) => c,
            Err(_) => {
                reads_miss.fetch_add(1, Ordering::Relaxed);
                compio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let payload = partition_rpc::rkyv_encode(&partition_rpc::GetReq {
            part_id,
            key: key.clone(),
            offset: 0,
            length: 0,
            region_epoch: 0,
        });
        // Bounded for the same reason as the writer (no forever-hang on a
        // wedged PS → shutdown-join stays responsive).
        let get_call = match compio::time::timeout(
            Duration::from_secs(5),
            client.call(partition_rpc::MSG_GET, payload),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => {
                eprintln!("reader[{name}]: GET TIMED OUT (5s) part_id={part_id} — PS wedged?");
                reads_miss.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        match get_call {
            Ok(resp) => match partition_rpc::rkyv_decode::<partition_rpc::GetResp>(&resp) {
                Ok(r) if r.code == partition_rpc::CODE_OK => {
                    // Sanity-only live check: the value must be a
                    // `make_value(key, _)` shape — starts with "chaos-"
                    // (6) + 8B seq + ":" (1) + key + padding. We do NOT
                    // assert `r.value == want` here because between
                    // sampling `want` and the GET response landing, the
                    // writer can run *two* updates, leaving `expected[key]`
                    // at a third value — comparing the live response to
                    // either snapshot is racy. The authoritative
                    // correctness contract is the post-workload final
                    // verify, which runs AFTER writes stop + settle.
                    let prefix_ok = r.value.len() >= 6 + 8 + 1 + key.len()
                        && &r.value[..6] == b"chaos-"
                        && r.value[14] == b':'
                        && &r.value[15..15 + key.len()] == key.as_slice();
                    if !prefix_ok {
                        panic!(
                            "reader[{name}] CORRUPT shape key={:?} bytes={} (not a chaos-value)",
                            String::from_utf8_lossy(&key),
                            r.value.len()
                        );
                    }
                    // Drop the `want` shadow so it's clear we don't use
                    // it for the live check; keep it bound to make
                    // intent legible.
                    let _ = want;
                    reads_ok.fetch_add(1, Ordering::Relaxed);
                }
                _ => {
                    reads_miss.fetch_add(1, Ordering::Relaxed);
                }
            },
            Err(_) => {
                reads_miss.fetch_add(1, Ordering::Relaxed);
                compio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    eprintln!("reader[{name}] stopped");
}

// ── Nemesis ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum Action {
    Split,
    Merge,
    EcConvert,
    FenceUnfence,
    Flush,
    Compact,
    Gc,
    ForceGc,
    KillEn,
    KillThenFence,
    NetworkPartition,
    LatencySpike,
}

const ALL_ACTIONS: &[Action] = &[
    Action::Split,
    Action::Merge,
    Action::EcConvert,
    Action::FenceUnfence,
    Action::Flush,
    Action::Compact,
    Action::Gc,
    Action::ForceGc,
    Action::KillEn,
    Action::KillThenFence,
    Action::NetworkPartition,
    Action::LatencySpike,
];

struct NemesisCtx {
    mgr: Rc<RpcClient>,
    router: Rc<PsRouter>,
    topo: Rc<Topology>,
    ens: Rc<RefCell<Vec<EnProcess>>>,
    en_binary: PathBuf,
    manager_addr: SocketAddr,
    /// toxiproxy admin CLI; nemesis uses it to toggle proxies + inject
    /// latency. Stateless wrapper around `toxiproxy-cli` shell-outs.
    toxi: ToxiproxyCli,
    /// Set of node_ids currently fenced (so we know which to clear).
    fenced: RefCell<Vec<u64>>,
    /// Node_ids that we SIGKILLed and haven't restarted yet.
    dead: RefCell<Vec<u64>>,
    /// Proxy names currently disabled (NetworkPartition action).
    partitioned: RefCell<Vec<String>>,
    nemesis_events: Arc<AtomicU64>,
    nemesis_errors: Arc<AtomicU64>,
    ec_k: u32,
    ec_m: u32,
}

impl NemesisCtx {
    /// Count nodes that are currently reachable from manager + PS:
    /// alive process AND not fenced AND not SIGKILL'd AND not toxiproxy-
    /// partitioned. Pre-fix this didn't subtract `partitioned`, so two
    /// concurrent failure injections (e.g. partition + fence) could
    /// drop the cluster below K+M quorum without the nemesis budget
    /// guard catching it — F227 then refuses commit_length, the writer
    /// retries land in a hard-to-recover state, and a rare key
    /// reverts to an older value. The guard is the test's only
    /// safeguard against pushing the cluster off the cliff, so it must
    /// reflect EVERY failure dimension we inject.
    fn healthy_count(&self) -> usize {
        let ens = self.ens.borrow();
        let fenced = self.fenced.borrow();
        let dead = self.dead.borrow();
        let partitioned = self.partitioned.borrow();
        ens.iter()
            .filter(|e| {
                e.is_alive()
                    && !fenced.contains(&e.node_id)
                    && !dead.contains(&e.node_id)
                    && !partitioned.contains(&e.proxy_name)
            })
            .count()
    }
}

async fn do_split(ctx: &NemesisCtx) -> Result<String, String> {
    let parts = ctx.topo.snapshot();
    let pid = parts.first().map(|p| p.2).ok_or("no partitions")?;
    let client = ctx.router.client_for(pid).await;
    let resp = client
        .call(
            partition_rpc::MSG_SPLIT_PART,
            partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: pid }),
        )
        .await
        .map_err(|e| format!("rpc: {e}"))?;
    let r: partition_rpc::SplitPartResp =
        partition_rpc::rkyv_decode(&resp).map_err(|e| format!("decode: {e}"))?;
    if r.code != partition_rpc::CODE_OK {
        return Err(format!("split refused: {}", r.message));
    }
    compio::time::sleep(Duration::from_millis(3000)).await;
    refresh_topology(&ctx.mgr, &ctx.topo).await;
    Ok(format!("split part {pid}"))
}

async fn do_merge(ctx: &NemesisCtx) -> Result<String, String> {
    let parts = ctx.topo.snapshot();
    if parts.len() < 2 {
        return Err("not enough partitions".into());
    }
    let survivor = parts[0].2;
    let victim = parts[1].2;
    let resp = ctx
        .mgr
        .call(
            MSG_MERGE_PARTITIONS,
            rkyv_encode(&MergePartitionsReq {
                survivor_part_id: survivor,
                victim_part_id: victim,
            }),
        )
        .await
        .map_err(|e| format!("rpc: {e}"))?;
    let r: MergePartitionsResp = rkyv_decode(&resp).map_err(|e| format!("decode: {e}"))?;
    if r.code != CODE_OK {
        return Err(format!("merge refused: {}", r.message));
    }
    compio::time::sleep(Duration::from_millis(3000)).await;
    refresh_topology(&ctx.mgr, &ctx.topo).await;
    Ok(format!("merge {survivor} <- {victim}"))
}

async fn do_ec_convert(ctx: &NemesisCtx) -> Result<String, String> {
    if ctx.ec_m == 0 {
        return Err("M=0 (pure replication); no EC convert".into());
    }
    let parts = ctx.topo.snapshot();
    let Some((_, _, pid)) = parts.first().cloned() else {
        return Err("no partitions".into());
    };
    let client = ctx.router.client_for(pid).await;
    let _ = client
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id: pid,
                op: partition_rpc::MAINTENANCE_FLUSH,
                extent_ids: vec![],
                gc_ratio: None,
                gc_max_size: None,
                gc_stream_debt: None,
                gc_empty_only: false,
            }),
        )
        .await;

    let regions = get_regions(&ctx.mgr).await;
    let region = regions
        .regions
        .iter()
        .find(|(_, r)| r.part_id == pid)
        .map(|(_, r)| r.clone())
        .ok_or("partition not in regions")?;
    let info_resp = ctx
        .mgr
        .call(
            MSG_STREAM_INFO,
            rkyv_encode(&StreamInfoReq {
                stream_ids: vec![region.log_stream],
            }),
        )
        .await
        .map_err(|e| format!("stream_info: {e}"))?;
    let info: StreamInfoResp = rkyv_decode(&info_resp).map_err(|e| format!("decode: {e}"))?;
    let stream = info
        .streams
        .first()
        .map(|(_, s)| s)
        .ok_or("no stream info")?;
    if stream.extent_ids.len() < 2 {
        return Err("no sealed extents".into());
    }
    let extent_id = stream.extent_ids[0];

    let force_resp = ctx
        .mgr
        .call(
            MSG_FORCE_EC_CONVERT,
            rkyv_encode(&ForceEcConvertReq { extent_id }),
        )
        .await
        .map_err(|e| format!("force_ec rpc: {e}"))?;
    let r: ForceEcConvertResp =
        rkyv_decode(&force_resp).map_err(|e| format!("decode force_ec: {e}"))?;
    if r.code != CODE_OK && r.code != CODE_PRECONDITION {
        return Err(format!("force_ec refused: {}", r.message));
    }
    Ok(format!("force_ec extent {extent_id}"))
}

async fn do_fence_unfence(ctx: &NemesisCtx) -> Result<String, String> {
    // Keep at least K+M-1 healthy so recovery has somewhere to dispatch.
    let min_healthy = (ctx.ec_k + ctx.ec_m).max(3) as usize;
    if ctx.healthy_count() <= min_healthy {
        return Err(format!(
            "healthy={} ≤ min={min_healthy}",
            ctx.healthy_count()
        ));
    }
    let candidate = {
        let ens = ctx.ens.borrow();
        let fenced = ctx.fenced.borrow();
        let dead = ctx.dead.borrow();
        ens.iter()
            .find(|e| e.is_alive() && !fenced.contains(&e.node_id) && !dead.contains(&e.node_id))
            .map(|e| e.node_id)
    };
    let victim = candidate.ok_or("no candidate")?;

    let resp = ctx
        .mgr
        .call(
            MSG_FENCE_NODE,
            rkyv_encode(&FenceNodeReq {
                node_id: victim,
                reason: "chaos nemesis".into(),
                set_by: "chaos".into(),
                force: true,
            }),
        )
        .await
        .map_err(|e| format!("fence rpc: {e}"))?;
    let r: CodeResp = rkyv_decode(&resp).map_err(|e| format!("decode: {e}"))?;
    if r.code != CODE_OK {
        return Err(format!("fence refused: {}", r.message));
    }
    ctx.fenced.borrow_mut().push(victim);

    compio::time::sleep(Duration::from_millis(2500)).await;

    let resp = ctx
        .mgr
        .call(
            MSG_CLEAR_NODE_OVERRIDE,
            rkyv_encode(&ClearNodeOverrideReq {
                node_id: victim,
                set_by: "chaos".into(),
            }),
        )
        .await
        .map_err(|e| format!("clear rpc: {e}"))?;
    let r: CodeResp = rkyv_decode(&resp).map_err(|e| format!("decode: {e}"))?;
    if r.code != CODE_OK {
        return Err(format!("clear refused: {}", r.message));
    }
    ctx.fenced.borrow_mut().retain(|id| *id != victim);
    Ok(format!("fence+unfence node {victim}"))
}

/// SIGKILL an EN subprocess, hold dead for a few seconds (so reads
/// observe replica-down failover), then restart the same process
/// against its existing data dir.
async fn do_kill_en(ctx: &NemesisCtx) -> Result<String, String> {
    let min_healthy = (ctx.ec_k + ctx.ec_m).max(3) as usize;
    if ctx.healthy_count() <= min_healthy {
        return Err(format!(
            "healthy={} ≤ min={min_healthy}",
            ctx.healthy_count()
        ));
    }

    // Pick a victim that's currently alive and not fenced.
    let victim_idx = {
        let ens = ctx.ens.borrow();
        let fenced = ctx.fenced.borrow();
        let dead = ctx.dead.borrow();
        ens.iter().position(|e| {
            e.is_alive() && !fenced.contains(&e.node_id) && !dead.contains(&e.node_id)
        })
    };
    let Some(idx) = victim_idx else {
        return Err("no kill candidate".into());
    };

    let victim_node_id = {
        let mut ens = ctx.ens.borrow_mut();
        ens[idx].kill();
        ens[idx].node_id
    };
    ctx.dead.borrow_mut().push(victim_node_id);
    eprintln!("nemesis: SIGKILL node {victim_node_id}");

    // Wait ~3 s: long enough for manager df probes to fail and Suspected
    // transition to land, short enough that the verifier isn't disrupted.
    compio::time::sleep(Duration::from_millis(3000)).await;

    // Restart against the same data dir; sentinel files persist, so the
    // EN re-registers with the same node_id.
    {
        let mut ens = ctx.ens.borrow_mut();
        ens[idx].restart(&ctx.en_binary, &ctx.manager_addr);
    }

    // Give it a moment to register before unblocking subsequent
    // operations.
    compio::time::sleep(Duration::from_millis(1500)).await;
    ctx.dead.borrow_mut().retain(|id| *id != victim_node_id);
    Ok(format!("kill+restart node {victim_node_id}"))
}

/// SIGKILL an EN, then fence the dead node (operator declares it
/// permanently down → recovery dispatches). Restart later so the
/// cluster ends with a healthy node back.
async fn do_kill_then_fence(ctx: &NemesisCtx) -> Result<String, String> {
    let min_healthy = (ctx.ec_k + ctx.ec_m).max(3) as usize + 1;
    if ctx.healthy_count() <= min_healthy {
        return Err(format!(
            "healthy={} ≤ min={min_healthy}",
            ctx.healthy_count()
        ));
    }

    let victim_idx = {
        let ens = ctx.ens.borrow();
        let fenced = ctx.fenced.borrow();
        let dead = ctx.dead.borrow();
        ens.iter().position(|e| {
            e.is_alive() && !fenced.contains(&e.node_id) && !dead.contains(&e.node_id)
        })
    };
    let Some(idx) = victim_idx else {
        return Err("no candidate".into());
    };

    let victim_node_id = {
        let mut ens = ctx.ens.borrow_mut();
        ens[idx].kill();
        ens[idx].node_id
    };
    ctx.dead.borrow_mut().push(victim_node_id);
    eprintln!("nemesis: SIGKILL + fence node {victim_node_id}");

    // Give the manager a sec to observe df failure.
    compio::time::sleep(Duration::from_millis(2000)).await;

    let resp = ctx
        .mgr
        .call(
            MSG_FENCE_NODE,
            rkyv_encode(&FenceNodeReq {
                node_id: victim_node_id,
                reason: "chaos: killed then fenced".into(),
                set_by: "chaos".into(),
                force: true,
            }),
        )
        .await
        .map_err(|e| format!("fence rpc: {e}"))?;
    let r: CodeResp = rkyv_decode(&resp).map_err(|e| format!("decode: {e}"))?;
    if r.code != CODE_OK {
        // Best effort: roll back the dead-tracking and restart.
        let mut ens = ctx.ens.borrow_mut();
        ens[idx].restart(&ctx.en_binary, &ctx.manager_addr);
        ctx.dead.borrow_mut().retain(|id| *id != victim_node_id);
        return Err(format!("fence refused: {}", r.message));
    }
    ctx.fenced.borrow_mut().push(victim_node_id);

    // Hold long enough for recovery to dispatch (every 2 s tick).
    compio::time::sleep(Duration::from_millis(5000)).await;

    // Unfence + restart so we don't deplete the cluster.
    let _ = ctx
        .mgr
        .call(
            MSG_CLEAR_NODE_OVERRIDE,
            rkyv_encode(&ClearNodeOverrideReq {
                node_id: victim_node_id,
                set_by: "chaos".into(),
            }),
        )
        .await;
    ctx.fenced.borrow_mut().retain(|id| *id != victim_node_id);

    {
        let mut ens = ctx.ens.borrow_mut();
        ens[idx].restart(&ctx.en_binary, &ctx.manager_addr);
    }
    compio::time::sleep(Duration::from_millis(1500)).await;
    ctx.dead.borrow_mut().retain(|id| *id != victim_node_id);
    Ok(format!("kill+fence+restart node {victim_node_id}"))
}

/// Disable an EN's toxiproxy proxy for ~3 s, then re-enable. Simulates
/// a network partition where the EN process is still alive (and
/// committing data, fsync'ing, etc.) but unreachable from manager + PS.
/// Distinct from `KillEn` (which actually stops the process).
async fn do_network_partition(ctx: &NemesisCtx) -> Result<String, String> {
    let min_healthy = (ctx.ec_k + ctx.ec_m).max(3) as usize;
    if ctx.healthy_count() <= min_healthy {
        return Err(format!(
            "healthy={} ≤ min={min_healthy}",
            ctx.healthy_count()
        ));
    }
    let (victim_proxy, victim_node_id) = {
        let ens = ctx.ens.borrow();
        let fenced = ctx.fenced.borrow();
        let dead = ctx.dead.borrow();
        let partitioned = ctx.partitioned.borrow();
        let pick = ens
            .iter()
            .find(|e| {
                e.is_alive()
                    && !fenced.contains(&e.node_id)
                    && !dead.contains(&e.node_id)
                    && !partitioned.contains(&e.proxy_name)
            })
            .map(|e| (e.proxy_name.clone(), e.node_id));
        match pick {
            Some(p) => p,
            None => return Err("no candidate".into()),
        }
    };
    ctx.toxi
        .set_enabled(&victim_proxy, false)
        .map_err(|e| format!("toxiproxy disable: {e}"))?;
    // A real partition cuts BOTH planes — the control proxy (df/health)
    // goes down with the data proxy. Best-effort: the ctl proxy exists
    // for every EN bootstrapped via bootstrap_en step 1b.
    let _ = ctx.toxi.set_enabled(&format!("{victim_proxy}-ctl"), false);
    ctx.partitioned.borrow_mut().push(victim_proxy.clone());
    eprintln!("nemesis: NetworkPartition {victim_proxy} (node {victim_node_id}) — disabled");

    compio::time::sleep(Duration::from_millis(3000)).await;

    ctx.toxi
        .set_enabled(&victim_proxy, true)
        .map_err(|e| format!("toxiproxy enable: {e}"))?;
    let _ = ctx.toxi.set_enabled(&format!("{victim_proxy}-ctl"), true);
    ctx.partitioned.borrow_mut().retain(|p| p != &victim_proxy);
    Ok(format!("network partition node {victim_node_id} (3s)"))
}

/// Inject 500 ms latency on an EN's proxy for ~4 s, then remove the
/// toxic. Exercises slow-replica behaviour: F227 commit_length still
/// requires this replica to ACK so writes pay the latency, surfacing
/// any timeout bug.
async fn do_latency_spike(ctx: &NemesisCtx) -> Result<String, String> {
    let (victim_proxy, victim_node_id) = {
        let ens = ctx.ens.borrow();
        let dead = ctx.dead.borrow();
        let partitioned = ctx.partitioned.borrow();
        let pick = ens
            .iter()
            .find(|e| {
                e.is_alive() && !dead.contains(&e.node_id) && !partitioned.contains(&e.proxy_name)
            })
            .map(|e| (e.proxy_name.clone(), e.node_id));
        match pick {
            Some(p) => p,
            None => return Err("no candidate".into()),
        }
    };
    let toxic_name = format!("chaos-lat-{victim_node_id}");
    ctx.toxi
        .add_toxic(
            &victim_proxy,
            "latency",
            &toxic_name,
            &[("latency", "500"), ("jitter", "100")],
        )
        .map_err(|e| format!("toxic add: {e}"))?;
    eprintln!("nemesis: LatencySpike {victim_proxy} (node {victim_node_id}) — +500ms±100");

    compio::time::sleep(Duration::from_millis(4000)).await;

    // Best-effort remove. If toxic was already cleared (shouldn't be
    // possible in this single-nemesis test, but defensive), ignore.
    let _ = ctx.toxi.remove_toxic(&victim_proxy, &toxic_name);
    Ok(format!("latency spike node {victim_node_id} (4s)"))
}

async fn do_maintenance(ctx: &NemesisCtx, op: u8, label: &str) -> Result<String, String> {
    let parts = ctx.topo.snapshot();
    for (_, _, pid) in &parts {
        let client = ctx.router.client_for(*pid).await;
        let _ = client
            .call(
                partition_rpc::MSG_MAINTENANCE,
                partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                    part_id: *pid,
                    op,
                    extent_ids: vec![],
                    gc_ratio: None,
                    gc_max_size: None,
                    gc_stream_debt: None,
                    gc_empty_only: false,
                }),
            )
            .await;
    }
    Ok(format!("{label} × {}", parts.len()))
}

/// FORCE GC on every partition's sealed log_stream extents — the maximal stress
/// on the PS replay-floor guard (the F-COMPACT/FLUSH-VPHEAD vp_head thread). Force
/// GC bypasses the discard-ratio gate and asks the PS to punch SPECIFIC sealed
/// extents; a wrong vp_head (compaction MAX / flush rotation-stamp / recovery
/// tail-seed) would let it punch an extent still inside the replay window → the
/// un-flushed WAL tail is lost (caught by verify_per_key / _range). The guard must
/// relocate live VPs out first and SKIP any extent at/after the replay floor —
/// force GC of a protected extent is a no-op, never a punch.
async fn do_force_gc(ctx: &NemesisCtx) -> Result<String, String> {
    let parts = ctx.topo.snapshot();
    let regions = get_regions(&ctx.mgr).await;
    let mut requested = 0usize;
    let mut hit_parts = 0usize;
    for (_, _, pid) in &parts {
        let Some(region) = regions
            .regions
            .iter()
            .find(|(_, r)| r.part_id == *pid)
            .map(|(_, r)| r.clone())
        else {
            continue;
        };
        let Ok(info_resp) = ctx
            .mgr
            .call(
                MSG_STREAM_INFO,
                rkyv_encode(&StreamInfoReq {
                    stream_ids: vec![region.log_stream],
                }),
            )
            .await
        else {
            continue;
        };
        let Ok(info) = rkyv_decode::<StreamInfoResp>(&info_resp) else {
            continue;
        };
        let Some((_, stream)) = info.streams.first() else {
            continue;
        };
        if stream.extent_ids.len() < 2 {
            continue; // only the open tail — nothing sealed to force-GC
        }
        // Every sealed (non-tail) extent. The PS's replay-floor guard decides
        // which are actually punchable; extents inside the replay window are
        // SKIPPED (protected), not lost.
        let sealed: Vec<u64> = stream.extent_ids[..stream.extent_ids.len() - 1].to_vec();
        let client = ctx.router.client_for(*pid).await;
        let _ = client
            .call(
                partition_rpc::MSG_MAINTENANCE,
                partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                    part_id: *pid,
                    op: partition_rpc::MAINTENANCE_FORCE_GC,
                    extent_ids: sealed.clone(),
                    gc_ratio: None,
                    gc_max_size: None,
                    gc_stream_debt: None,
                    gc_empty_only: false,
                }),
            )
            .await;
        requested += sealed.len();
        hit_parts += 1;
    }
    Ok(format!(
        "forcegc requested {requested} sealed extent(s) across {hit_parts} part(s)"
    ))
}

async fn nemesis_loop(
    ctx: Rc<NemesisCtx>,
    stop: Arc<AtomicBool>,
    interval_ms: u64,
    actions: Vec<Action>,
    mut lcg: Lcg,
) {
    // Shuffled round-robin over the FULL action set (not a per-interval random
    // pick): every action is ATTEMPTED at least once per cycle (cycle length =
    // actions.len()), in per-seed-deterministic shuffled order. This guarantees
    // a single run can't miss any of split / merge / ec / gc / kill / fence
    // (user rule: every chaos run exercises the full set), while preserving
    // random ordering across cycles. (Completion still depends on cluster state
    // — e.g. merge needs ≥2 adjacent partitions with no in-flight recovery — but
    // the path is always attempted.) Refill + reshuffle when the cycle drains.
    let mut pending: Vec<Action> = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        compio::time::sleep(Duration::from_millis(interval_ms)).await;
        if stop.load(Ordering::Relaxed) {
            break;
        }
        if pending.is_empty() {
            pending = actions.clone();
            // Fisher-Yates with the LCG keeps per-seed determinism.
            for i in (1..pending.len()).rev() {
                let j = (lcg.next() as usize) % (i + 1);
                pending.swap(i, j);
            }
        }
        let action = pending.pop().expect("pending refilled when empty");
        // Bound every nemesis action: split/merge/EC orchestration RPCs are
        // unbounded, so if a PS partition is wedged (e.g. stuck on an F227
        // all-replica op after a node kill+restart whose behind replica was
        // never recovered), the in-flight op never returns and the test
        // hangs forever at shutdown-join. 30s is generous (split retries up
        // to ~10s); a longer stall means a real wedge → log + keep looping
        // so the loop still exits on `stop`. Surfaces the wedge as a
        // bounded failure (verify not_found) instead of a CI hang.
        let dispatch = async {
            match action {
                Action::Split => do_split(&ctx).await,
                Action::Merge => do_merge(&ctx).await,
                Action::EcConvert => do_ec_convert(&ctx).await,
                Action::FenceUnfence => do_fence_unfence(&ctx).await,
                Action::Flush => {
                    do_maintenance(&ctx, partition_rpc::MAINTENANCE_FLUSH, "flush").await
                }
                Action::Compact => {
                    do_maintenance(&ctx, partition_rpc::MAINTENANCE_COMPACT, "compact").await
                }
                Action::Gc => do_maintenance(&ctx, partition_rpc::MAINTENANCE_AUTO_GC, "gc").await,
                Action::ForceGc => do_force_gc(&ctx).await,
                Action::KillEn => do_kill_en(&ctx).await,
                Action::KillThenFence => do_kill_then_fence(&ctx).await,
                Action::NetworkPartition => do_network_partition(&ctx).await,
                Action::LatencySpike => do_latency_spike(&ctx).await,
            }
        };
        let result = match compio::time::timeout(Duration::from_secs(30), dispatch).await {
            Ok(r) => r,
            Err(_) => Err(format!(
                "{action:?} TIMED OUT (30s) — PS/orchestration wedged?"
            )),
        };
        ctx.nemesis_events.fetch_add(1, Ordering::Relaxed);
        match result {
            Ok(msg) => eprintln!("nemesis: {action:?} OK — {msg}"),
            Err(msg) => {
                ctx.nemesis_errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("nemesis: {action:?} skipped — {msg}");
            }
        }
    }
    eprintln!("nemesis stopped");
}

// ── Checker ────────────────────────────────────────────────────────────

/// Decode the `seq` field that `make_value` embedded so verify
/// diagnostics can show "expected seq=N got seq=M" — far more useful
/// than length-only output.
fn extract_seq(v: &[u8]) -> Option<u64> {
    if v.len() < 14 || &v[..6] != b"chaos-" {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&v[6..14]);
    Some(u64::from_le_bytes(buf))
}

async fn verify_per_key(
    router: &PsRouter,
    topo: &Topology,
    expected: &HashMap<Vec<u8>, Vec<u8>>,
) -> (usize, Vec<String>, Vec<String>) {
    let mut mismatches: Vec<String> = Vec::new();
    let mut not_found: Vec<String> = Vec::new();
    // Partitions that have proven wedged (a GET timed out post-settle). Once
    // known wedged, remaining keys routing there are recorded not_found
    // immediately instead of paying 10×5s each — the wedge is the finding;
    // grinding every key just delays the FAILED report by minutes.
    let mut wedged_parts: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let total = expected.len();
    for (key, want) in expected {
        let part_id = topo.route(key);
        if wedged_parts.contains(&part_id) {
            not_found.push(String::from_utf8_lossy(key).into_owned());
            continue;
        }
        let mut got: Option<Vec<u8>> = None;
        for _attempt in 0..10 {
            // try_client_for never panics (vs `client_for` which does
            // after AUTUMN_TEST_ROUTER_RETRIES exhausted). If routing
            // still fails after that, log + skip — the verify path
            // will record the key as not_found.
            let client = match router.try_client_for(part_id).await {
                Ok(c) => c,
                Err(_) => {
                    // Routing failure (no registered addr) is itself a
                    // wedge signal: a partition that never (re)opened has no
                    // part_addr. `try_client_for` already retried internally,
                    // so mark the partition wedged and fast-fail remaining
                    // keys instead of paying 10×500ms PER key (which made
                    // verify grind for minutes when a partition stayed
                    // unbound — BUG #3 persistent-EADDRINUSE case).
                    eprintln!(
                        "verify: ROUTE FAILED key={} part_id={} attempt={_attempt} — no part_addr, marking partition wedged",
                        String::from_utf8_lossy(key),
                        part_id
                    );
                    wedged_parts.insert(part_id);
                    break;
                }
            };
            let payload = partition_rpc::rkyv_encode(&partition_rpc::GetReq {
                part_id,
                key: key.clone(),
                offset: 0,
                length: 0,
                region_epoch: 0,
            });
            // A chaos verify must never hang forever: a wedged PS that
            // accepts the connection but never replies would otherwise
            // block this `.await` indefinitely. Bound it and, on timeout,
            // log the offending part_id so the wedge is localizable.
            let call_res = match compio::time::timeout(
                Duration::from_secs(5),
                client.call(partition_rpc::MSG_GET, payload),
            )
            .await
            {
                Ok(r) => r,
                Err(_) => {
                    eprintln!(
                        "verify: GET TIMED OUT (5s) key={} part_id={} attempt={_attempt} — PS wedged, marking partition wedged",
                        String::from_utf8_lossy(key),
                        part_id
                    );
                    wedged_parts.insert(part_id);
                    break;
                }
            };
            match call_res {
                Ok(resp) => match partition_rpc::rkyv_decode::<partition_rpc::GetResp>(&resp) {
                    Ok(r) if r.code == partition_rpc::CODE_OK => {
                        got = Some(r.value);
                        break;
                    }
                    Ok(_) => {}
                    Err(_) => {}
                },
                Err(_) => {
                    compio::time::sleep(Duration::from_millis(300)).await;
                    continue;
                }
            }
            compio::time::sleep(Duration::from_millis(300)).await;
        }
        match got {
            Some(v) if v == *want => {}
            Some(v) => {
                let exp_seq = extract_seq(want).unwrap_or(0);
                let got_seq = extract_seq(&v).unwrap_or(0);
                mismatches.push(format!(
                    "{} (expected seq={} got seq={})",
                    String::from_utf8_lossy(key),
                    exp_seq,
                    got_seq
                ));
            }
            None => not_found.push(String::from_utf8_lossy(key).into_owned()),
        }
    }
    (total, mismatches, not_found)
}

/// Post-settle WRITE-LIVENESS / convergence check (coco arch gap #1: "does the
/// cluster CONVERGE post-settle — no stuck inflight?").
///
/// `verify_per_key` + `verify_per_partition_range` are both READ-ONLY. The
/// nastiest failure class in this stream+partition layer — a never-completing
/// Recovery on a tail extent that makes `stream_alloc_extent` refuse, wedging
/// flush + (eventually) writes — is INVISIBLE to reads: point-gets and ranges
/// keep serving the already-flushed data fine while every WRITE hangs. So we
/// must prove the cluster can still take writes after the chaos stops.
///
/// For each partition we fire a burst of fresh in-range PUTs carrying 8 KiB
/// values (above the 4 KiB VALUE_THROTTLE) so the burst applies real memtable
/// pressure → rotation → flush → `row_stream.append` → `alloc_extent` — i.e. it
/// exercises the exact path the recovery-stuck bug wedges, not just a memtable
/// insert. Each PUT is bounded by a 5 s timeout.
///
/// A partition FAILS liveness only on an UNAMBIGUOUS wedge — either a 5 s PUT
/// timeout (PS accepted the connection but never replied), or zero acks across
/// the whole retry window. A partition that acks even one write is alive; this
/// keeps false positives near zero (a mid-reopen blip surfaces as a transient
/// connection error and is retried, not failed). These writes run AFTER both
/// read verifies, so they never pollute the data-correctness checks.
async fn verify_write_liveness(router: &PsRouter, topo: &Topology) -> Vec<String> {
    const LIVENESS_WRITES: u64 = 50;
    let mut errors: Vec<String> = Vec::new();
    let parts = topo.snapshot();
    for (start, end, part_id) in parts {
        // Build an in-range, well-formed chaos key that routes to THIS
        // partition: walk the valid key space and take the first key whose
        // range contains it. (A partition created mid-split may own a sub-range
        // that no single literal key prefix covers, so we must search.)
        let probe_key: Option<Vec<u8>> = (0..CHAOS_KEY_COUNT)
            .flat_map(|kid| {
                [
                    format!("b{kid:06}").into_bytes(),
                    format!("q{kid:06}").into_bytes(),
                ]
            })
            .find(|k| {
                k.as_slice() >= start.as_slice()
                    && (end.is_empty() || k.as_slice() < end.as_slice())
            });
        let Some(probe_key) = probe_key else {
            // No representable key in this partition's range — nothing we can
            // legitimately write here; skip (not a wedge).
            continue;
        };

        let mut acked: u64 = 0;
        let mut timed_out = false;
        'burst: for i in 0..LIVENESS_WRITES {
            // Distinct large value per write so the burst can't be coalesced
            // into a no-op; `seq` carried at the front (verify-compatible
            // encoding) though we don't re-read these.
            let value = make_value(&probe_key, 1_000_000 + i);
            let payload = partition_rpc::rkyv_encode(&partition_rpc::PutReq {
                part_id,
                key: probe_key.clone(),
                value,
                expires_at: 0,
                region_epoch: 0,
            inode_hint: 0,
            lease_epoch: 0,
            });
            // Up to 3 attempts per write to ride out a transient reopen blip.
            for _attempt in 0..3 {
                let client = match router.try_client_for(part_id).await {
                    Ok(c) => c,
                    Err(_) => {
                        compio::time::sleep(Duration::from_millis(300)).await;
                        continue;
                    }
                };
                match compio::time::timeout(
                    Duration::from_secs(5),
                    client.call(partition_rpc::MSG_PUT, payload.clone()),
                )
                .await
                {
                    Ok(Ok(resp)) => {
                        match partition_rpc::rkyv_decode::<partition_rpc::PutResp>(&resp) {
                            Ok(r) if r.code == partition_rpc::CODE_OK => {
                                acked += 1;
                                continue 'burst;
                            }
                            // Rejected (epoch / range) — retry; topology should
                            // be stable post-settle, but tolerate a late blip.
                            _ => {
                                compio::time::sleep(Duration::from_millis(300)).await;
                            }
                        }
                    }
                    Ok(Err(_)) => {
                        compio::time::sleep(Duration::from_millis(300)).await;
                    }
                    Err(_) => {
                        // 5 s timeout = the PS took the connection but never
                        // replied = the wedge we are hunting. Record + stop.
                        eprintln!(
                            "liveness: PUT TIMED OUT (5s) part_id={part_id} write={i}/{LIVENESS_WRITES} — partition wedged for writes"
                        );
                        timed_out = true;
                        break 'burst;
                    }
                }
            }
        }

        if timed_out {
            errors.push(format!(
                "part {part_id}: WRITE WEDGE — PUT timed out (5s) after {acked}/{LIVENESS_WRITES} acked post-settle"
            ));
        } else if acked == 0 {
            errors.push(format!(
                "part {part_id}: WRITE WEDGE — 0/{LIVENESS_WRITES} writes acked post-settle (partition cannot take writes)"
            ));
        }
    }
    errors
}

/// Range invariant: for each partition, walk it with `MSG_RANGE` and
/// confirm every expected key in `[start, end)` is returned. Detects
/// silent loss for keys that `verify_per_key` would still find via
/// point lookup (e.g., the per-key path uses a different code branch
/// than range — both must agree).
async fn verify_per_partition_range(
    router: &PsRouter,
    topo: &Topology,
    expected: &HashMap<Vec<u8>, Vec<u8>>,
) -> Vec<String> {
    let mut errors = Vec::new();
    let parts = topo.snapshot();
    for (start, end, pid) in &parts {
        // Collect expected keys in [start, end).
        let mut want_in_part: Vec<Vec<u8>> = expected
            .keys()
            .filter(|k| {
                let after_start = k.as_slice() >= start.as_slice();
                let before_end = end.is_empty() || k.as_slice() < end.as_slice();
                after_start && before_end
            })
            .cloned()
            .collect();
        want_in_part.sort();

        // Scan partition via repeated MSG_RANGE pages.
        let mut got: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        // Last key seen (across pages) for the strictly-ascending / no-duplicate
        // check. Resets per partition.
        let mut prev_key: Option<Vec<u8>> = None;
        let mut cursor: Vec<u8> = start.clone();
        let page_limit: u32 = 256;
        for _ in 0..200 {
            let client = router.client_for(*pid).await;
            let req = partition_rpc::RangeReq {
                part_id: *pid,
                prefix: Vec::new(),
                start: cursor.clone(),
                limit: page_limit,
                region_epoch: 0,
            };
            // Bounded like the per-key GET: a wedged PS would otherwise hang
            // the range scan forever (this runs AFTER per-key verify, so a
            // wedged partition is already a recorded failure; don't also hang
            // here). Timeout → record error + stop paging this partition.
            let resp = match compio::time::timeout(
                Duration::from_secs(5),
                client.call(partition_rpc::MSG_RANGE, partition_rpc::rkyv_encode(&req)),
            )
            .await
            {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => {
                    errors.push(format!("range rpc on part {pid}: {e}"));
                    break;
                }
                Err(_) => {
                    errors.push(format!(
                        "range rpc on part {pid}: TIMED OUT (5s) — PS wedged"
                    ));
                    break;
                }
            };
            let r: partition_rpc::RangeResp = match partition_rpc::rkyv_decode(&resp) {
                Ok(r) => r,
                Err(e) => {
                    errors.push(format!("range decode on part {pid}: {e}"));
                    break;
                }
            };
            if r.code != partition_rpc::CODE_OK {
                errors.push(format!(
                    "range code={} on part {pid}: {}",
                    r.code, r.message
                ));
                break;
            }
            if r.entries.is_empty() {
                break;
            }
            let last = r.entries.last().unwrap().key.clone();
            for kv in r.entries {
                let k = kv.key;
                // (a) ORDER: range must return keys in strictly ascending order.
                // A non-increasing key = duplicate or out-of-order = a real
                // iterator/merge bug (coco arch gap #5/#6). `prev_key` is the
                // last key seen across ALL pages for this partition.
                if let Some(p) = &prev_key {
                    if k <= *p {
                        errors.push(format!(
                            "part {pid}: range NOT ascending — {:?} after {:?}",
                            String::from_utf8_lossy(&k),
                            String::from_utf8_lossy(p)
                        ));
                    }
                }
                // (b) NO PHANTOM (malformed / never-written key). Any key outside
                // the writers' `{b|q}{kid<COUNT}` space is corruption.
                if !is_valid_chaos_key(&k) {
                    errors.push(format!(
                        "part {pid}: range returned PHANTOM/malformed key {:?}",
                        String::from_utf8_lossy(&k)
                    ));
                }
                // (c) IN-RANGE: a key outside this partition's [start, end) is a
                // CoW split/merge sibling leak (a key that belongs to another
                // partition's range showing up here).
                let in_range = k.as_slice() >= start.as_slice()
                    && (end.is_empty() || k.as_slice() < end.as_slice());
                if !in_range {
                    errors.push(format!(
                        "part {pid}: range returned OUT-OF-RANGE key {:?} (range [{:?},{:?}))",
                        String::from_utf8_lossy(&k),
                        String::from_utf8_lossy(start),
                        String::from_utf8_lossy(end)
                    ));
                }
                prev_key = Some(k.clone());
                got.insert(k);
            }
            // Advance cursor STRICTLY PAST every MVCC version of `last`. The
            // internal key is `user_key ++ 0x00 ++ BE(u64::MAX - seq)`, so
            // `last`'s versions span `last\x00\x00…` (newest) … `last\x00\xff…`
            // (oldest). Appending 0x00 (the old `push(0)`) makes the PS seek
            // `key_with_ts(last\x00, MAX) = last\x00\x00…`, which lands BEFORE
            // `last`'s older versions → the page re-returns `last` forever
            // (the old HashSet-dedup hid this; the no-duplicate/order check
            // above surfaces it). Appending 0x01 makes the seek
            // `last\x01\x00…` > `last\x00\xff…`, clearing all of `last`'s
            // versions while staying below the next user key.
            let mut next = last;
            next.push(1);
            cursor = next;
            // Hit cur_end_key (partition end)?
            if !r.cur_end_key.is_empty() && cursor >= r.cur_end_key {
                break;
            }
        }

        // Compare.
        let missing: Vec<_> = want_in_part
            .iter()
            .filter(|k| !got.contains(*k))
            .cloned()
            .collect();
        if !missing.is_empty() {
            errors.push(format!(
                "part {pid}: range missing {} expected keys (first: {:?})",
                missing.len(),
                String::from_utf8_lossy(&missing[0])
            ));
        }
    }
    errors
}

// ── Main test ──────────────────────────────────────────────────────────

async fn create_stream_kp(mgr: &RpcClient, k: u32, m: u32) -> u64 {
    let resp = mgr
        .call(
            MSG_CREATE_STREAM,
            rkyv_encode(&CreateStreamReq {
                replicates: k,
                ec_data_shard: k,
                ec_parity_shard: m,
            }),
        )
        .await
        .expect("create stream");
    let created: CreateStreamResp = rkyv_decode(&resp).expect("decode CreateStreamResp");
    created
        .stream
        .unwrap_or_else(|| {
            panic!(
                "create_stream code={} msg={}",
                created.code, created.message
            )
        })
        .stream_id
}

/// Storage-accounting invariant checker (the extent-10 / orphan-leak class).
///
/// The existing chaos checkers (`verify_per_key` / `_range` / `_write_liveness`)
/// only validate USER DATA. None of them assert STORAGE accounting, so a
/// refcount leak / orphan extent / dangling stream membership reads-and-writes
/// perfectly while quietly leaking space or — worse — sitting at a state the
/// both-zero sweep must skip to avoid data loss. This reads the manager's etcd
/// state directly (the source of truth, no new RPC) and asserts the invariant
/// that is true BY CONSTRUCTION on every mutation path:
///
///   for every extent E:  E.refs == (number of streams whose extent_ids list E)
///
/// because split does `refs += 1` + adds E to the child's `extent_ids`, merge
/// does `refs -= 1` + removes it, create/alloc set `refs = 1` + add to one
/// stream, and punch_holes does `refs -= 1` + removes — refs and membership
/// always move together, in one fenced etcd txn. A POST-SETTLE violation is
/// therefore a real accounting bug:
///   - refs >  membership  → over-count → extent never freed (leak)
///   - refs <  membership  → under-count → premature physical delete risk
///   - refs >0, membership 0 → orphan (referenced but in no stream) = extent-10
///   - membership >0, no ExtentInfo → dangling (stream points at a gone extent)
/// Plus `vp_table_refs == 0` (the post-removal new-build invariant; a non-zero
/// value is a legacy leak the upgrade-safety guard intentionally won't reap).
///
/// CONVERGENCE LOOP: a background GC/split/merge firing during the settle
/// window touches `streams/<id>` and `extents/<id>` in one atomic txn, so any
/// single etcd snapshot is internally consistent — but to be robust against
/// any future multi-txn path (and the etcd read itself racing a commit), we
/// retry: a transient desync heals within a tick, a REAL leak is permanent.
/// Clean on ANY attempt ⇒ pass; dirty on ALL attempts ⇒ return the last set.
/// Returns `(errors, extents_checked, total_memberships)`. The two counts prove
/// the check is non-vacuous (it actually saw extents + stream memberships, not
/// an empty etcd snapshot) and are logged at the call site.
async fn verify_extent_accounting(etcd_endpoint: &str) -> (Vec<String>, usize, usize) {
    let mut last = (Vec::new(), 0usize, 0usize);
    for attempt in 0..6 {
        let snap = accounting_snapshot_errors(etcd_endpoint).await;
        if snap.0.is_empty() {
            // Non-vacuity guard (coco P2): the chaos test always has log/row/meta
            // streams + their extents, so a clean-but-EMPTY snapshot means we
            // read the wrong/empty etcd or persistence is broken — NOT "all
            // good". Treat it as a failure rather than a vacuous pass.
            if snap.1 == 0 || snap.2 == 0 {
                return (
                    vec![format!(
                        "accounting: vacuous snapshot (extents={}, memberships={}) — expected non-empty (log/row/meta streams exist)",
                        snap.1, snap.2
                    )],
                    snap.1,
                    snap.2,
                );
            }
            return snap;
        }
        last = snap;
        if attempt < 5 {
            compio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    last
}

/// Set of all extent_ids the manager currently tracks in etcd. Used to measure
/// PHYSICAL reclamation (deleted extents) across the final GC pass.
async fn read_extent_id_set(etcd_endpoint: &str) -> std::collections::HashSet<u64> {
    let mut set = std::collections::HashSet::new();
    let Ok(client) = autumn_etcd::EtcdClient::connect(etcd_endpoint).await else {
        return set;
    };
    let Ok(resp) = client.get_prefix("extents/").await else {
        return set;
    };
    for kv in &resp.kvs {
        if let Some(eid) = parse_id_after_prefix(&kv.key, "extents/") {
            set.insert(eid);
        }
    }
    set
}

/// POSITIVE reclamation check (user ask: "GC 后 extent 确定可以被删除"). After the
/// workload quiesces, a final flush → major-compact → FORCE-GC pass MUST
/// physically DELETE extents: the chaos run created dead data (overwritten
/// versions, out-of-range post-split keys, superseded SSTs), and a working GC +
/// compaction reclaims it — relocate live VPs off a sealed extent, advance the
/// replay floor, `punch_holes` → `refs == 0` → the manager deletes the
/// ExtentInfo + unlinks the files. This is the FLIP SIDE of the no-loss checks:
/// it catches the OPPOSITE regression — a wrong vp_head PINNING the replay floor
/// so force-GC protects every extent and nothing is ever deletable (the user's
/// original "compact-then-forceg won't reclaim" symptom that the vp_head thread
/// fixed). Callers re-run per-key + accounting AFTER, so the destructive punch
/// pass is itself proven loss-free + leak-free. Returns
/// (errors, total_reclaimed, gc_reclaimed).
async fn verify_gc_reclaim(
    mgr: &RpcClient,
    router: &PsRouter,
    topo: &Topology,
    etcd_endpoint: &str,
) -> (Vec<String>, usize, usize) {
    let mut errors = Vec::new();
    // Set when any partition's force-GC reports PROTECTED extents (a pinned
    // replay floor holding reclaimable data). Distinguishes a genuinely STUCK
    // floor (protected + reclaim=0 = the bug this check catches) from a
    // legitimately-empty partition (nothing protected + reclaim=0 = nothing to
    // reclaim, e.g. a merge-consolidated survivor with ≤1 SST / ≤1 sealed log
    // extent — common under the full nemesis set, false-positive pre-fix).
    let mut any_protected = false;
    let before = read_extent_id_set(etcd_endpoint).await;
    let parts: Vec<u64> = topo.snapshot().iter().map(|p| p.2).collect();

    let maint = |pid: u64, op: u8, extent_ids: Vec<u64>| partition_rpc::MaintenanceReq {
        part_id: pid,
        op,
        extent_ids,
        gc_ratio: None,
        gc_max_size: None,
        gc_stream_debt: None,
        gc_empty_only: false,
    };

    // Flush + major-compact everything, twice: advance the replay floor past all
    // fully-flushed data, drop out-of-range post-split keys + superseded SSTs, and
    // truncate their row_stream extents. Two rounds so a CoW-shared SST extent
    // (refs 2 post-split) reaches refs 0 once BOTH children have compacted.
    for _ in 0..2 {
        for &pid in &parts {
            let client = router.client_for(pid).await;
            for op in [
                partition_rpc::MAINTENANCE_FLUSH,
                partition_rpc::MAINTENANCE_COMPACT,
            ] {
                let _ = client
                    .call(
                        partition_rpc::MSG_MAINTENANCE,
                        partition_rpc::rkyv_encode(&maint(pid, op, vec![])),
                    )
                    .await;
            }
        }
        compio::time::sleep(Duration::from_secs(3)).await;
    }
    let mid = read_extent_id_set(etcd_endpoint).await;

    // Force-GC every partition's sealed log extents: relocate live VPs off them +
    // punch the ones before the replay floor (incl. post-split shared log extents
    // once both children have compacted). Replay-window extents are SKIPPED
    // (protected), never punched.
    let regions = get_regions(mgr).await;
    for &pid in &parts {
        let Some(region) = regions
            .regions
            .iter()
            .find(|(_, r)| r.part_id == pid)
            .map(|(_, r)| r.clone())
        else {
            continue;
        };
        let Ok(info_resp) = mgr
            .call(
                MSG_STREAM_INFO,
                rkyv_encode(&StreamInfoReq {
                    stream_ids: vec![region.log_stream],
                }),
            )
            .await
        else {
            continue;
        };
        let Ok(info) = rkyv_decode::<StreamInfoResp>(&info_resp) else {
            continue;
        };
        let Some((_, stream)) = info.streams.first() else {
            continue;
        };
        if stream.extent_ids.len() < 2 {
            continue;
        }
        let sealed: Vec<u64> = stream.extent_ids[..stream.extent_ids.len() - 1].to_vec();
        let client = router.client_for(pid).await;
        // Capture the force-GC advisory: F-GC-FLOOR-OBS #3 returns a NON-EMPTY
        // `MaintenanceResp.message` ONLY when a requested sealed extent resolves
        // AT/BEFORE the recovery replay floor and is therefore PROTECTED (a
        // pinned floor holding reclaimable data — the "stuck" case this check
        // exists to catch). An EMPTY message ⇒ nothing was protected.
        if let Ok(resp) = client
            .call(
                partition_rpc::MSG_MAINTENANCE,
                partition_rpc::rkyv_encode(&maint(pid, partition_rpc::MAINTENANCE_FORCE_GC, sealed)),
            )
            .await
        {
            if let Ok(m) = partition_rpc::rkyv_decode::<partition_rpc::MaintenanceResp>(&resp) {
                if !m.message.is_empty() {
                    any_protected = true;
                    eprintln!("gc-reclaim: part {pid} force-GC protected: {}", m.message);
                }
            }
        }
    }
    // The manager's extent_delete_loop is a 2 s sweep; relocation appends + the
    // refs→0 delete need a few ticks to land.
    compio::time::sleep(Duration::from_secs(12)).await;

    let after = read_extent_id_set(etcd_endpoint).await;
    let total_reclaimed = before.difference(&after).count();
    let gc_reclaimed = mid.difference(&after).count();

    if total_reclaimed == 0 {
        if any_protected {
            // A pinned replay floor is holding reclaimable extents even after
            // flush + MAJOR-compact — the real "compact-then-forceg won't
            // reclaim" stuck-floor bug (F-VPHEAD-CHAOS / F-GC-FLOOR-OBS).
            errors.push(format!(
                "GC-RECLAIM: the quiesce (flush + major-compact + force-GC) DELETED 0 \
                 extents WHILE force-GC reported PROTECTED extents — reclamation is STUCK \
                 (a pinned replay floor is holding reclaimable data). before={} mid={} after={}",
                before.len(),
                mid.len(),
                after.len()
            ));
        } else {
            // Nothing was protected and nothing reclaimed ⇒ the partitions had
            // nothing reclaimable (all data live in ≤1 SST / no sealed log
            // extent — legitimate after merge/EC consolidation). NOT a stuck
            // floor; do not fail the run.
            eprintln!(
                "gc-reclaim: 0 reclaimed but NOTHING protected — nothing was reclaimable \
                 (not a stuck floor). before={} mid={} after={}",
                before.len(),
                mid.len(),
                after.len()
            );
        }
    }
    (errors, total_reclaimed, gc_reclaimed)
}

/// Standard etcd prefix range-end (increment the last non-0xff byte). Mirrors
/// `autumn_etcd`'s private `prefix_range_end` so we can build a revision-pinned
/// `streams/` Range for the consistent-snapshot read.
fn prefix_range_end_local(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(&last) = end.last() {
        if last < 0xff {
            *end.last_mut().unwrap() = last + 1;
            return end;
        }
        end.pop();
    }
    vec![0] // all-0xff prefix → open-ended range
}

fn parse_id_after_prefix(key: &[u8], prefix: &str) -> Option<u64> {
    std::str::from_utf8(key)
        .ok()?
        .strip_prefix(prefix)?
        .parse()
        .ok()
}

/// One read-and-check pass over the manager's etcd `extents/` + `streams/`
/// prefixes. Returns the list of accounting-invariant violations.
async fn accounting_snapshot_errors(etcd_endpoint: &str) -> (Vec<String>, usize, usize) {
    let mut errors = Vec::new();
    let client = match autumn_etcd::EtcdClient::connect(etcd_endpoint).await {
        Ok(c) => c,
        Err(e) => {
            errors.push(format!("accounting: etcd connect failed: {e}"));
            return (errors, 0, 0);
        }
    };
    // CONSISTENT SNAPSHOT (coco P2): read extents/ at the latest revision,
    // capture that revision, then read streams/ PINNED at the same revision.
    // Two independent latest-revision Range reads could otherwise straddle a
    // commit and stitch a phantom "old extents + new streams" state, making the
    // checker false-positive on refs != membership.
    let ext_resp = match client.get_prefix("extents/").await {
        Ok(r) => r,
        Err(e) => {
            errors.push(format!("accounting: get_prefix(extents/) failed: {e}"));
            return (errors, 0, 0);
        }
    };
    let snapshot_rev = ext_resp.header.as_ref().map(|h| h.revision).unwrap_or(0);
    let stream_req = autumn_etcd::proto::RangeRequest {
        key: b"streams/".to_vec(),
        range_end: prefix_range_end_local(b"streams/"),
        revision: snapshot_rev,
        ..Default::default()
    };
    let stream_resp = match client.range(stream_req).await {
        Ok(r) => r,
        Err(e) => {
            errors.push(format!(
                "accounting: range(streams/ @rev {snapshot_rev}) failed: {e}"
            ));
            return (errors, 0, 0);
        }
    };

    // Decode into plain tuples, then run the PURE invariant check (unit-tested
    // below) so the comparison logic is provable without a live cluster.
    let mut stream_lists: Vec<Vec<u64>> = Vec::new();
    for kv in &stream_resp.kvs {
        match rkyv_decode::<MgrStreamInfo>(&kv.value) {
            Ok(info) => stream_lists.push(info.extent_ids.clone()),
            Err(e) => errors.push(format!(
                "accounting: decode {} failed: {e}",
                String::from_utf8_lossy(&kv.key)
            )),
        }
    }
    let mut extents: Vec<(u64, u64, u64)> = Vec::new();
    for kv in &ext_resp.kvs {
        let eid = match parse_id_after_prefix(&kv.key, "extents/") {
            Some(v) => v,
            None => {
                errors.push(format!(
                    "accounting: unparseable extent key {}",
                    String::from_utf8_lossy(&kv.key)
                ));
                continue;
            }
        };
        match rkyv_decode::<MgrExtentInfo>(&kv.value) {
            Ok(info) => extents.push((eid, info.refs, info.vp_table_refs)),
            Err(e) => errors.push(format!("accounting: decode extents/{eid} failed: {e}")),
        }
    }
    let total_memberships: usize = stream_lists.iter().map(|l| l.len()).sum();
    let n_extents = extents.len();
    errors.extend(check_accounting_invariants(&extents, &stream_lists));
    (errors, n_extents, total_memberships)
}

/// Pure storage-accounting invariant check (no I/O) — unit-tested below so the
/// comparison logic is provable without a live cluster. `extents` = (extent_id,
/// refs, vp_table_refs); `stream_extent_lists` = each stream's `extent_ids`.
fn check_accounting_invariants(
    extents: &[(u64, u64, u64)],
    stream_extent_lists: &[Vec<u64>],
) -> Vec<String> {
    let mut errors = Vec::new();
    let mut membership: HashMap<u64, u64> = HashMap::new();
    for list in stream_extent_lists {
        for eid in list {
            *membership.entry(*eid).or_insert(0) += 1;
        }
    }
    let mut existing: HashMap<u64, ()> = HashMap::new();
    for &(eid, refs, vp_table_refs) in extents {
        existing.insert(eid, ());
        let memb = membership.get(&eid).copied().unwrap_or(0);
        if refs != memb {
            errors.push(format!(
                "extent {eid}: refs={refs} != {memb} streams listing it ({})",
                if memb == 0 {
                    "ORPHAN-LEAK: referenced but in no stream"
                } else if refs < memb {
                    "UNDER-COUNT: premature-delete risk"
                } else {
                    "OVER-COUNT: never-freed leak"
                }
            ));
        }
        if vp_table_refs != 0 {
            errors.push(format!(
                "extent {eid}: vp_table_refs={vp_table_refs} (new-build invariant: must be 0)"
            ));
        }
    }
    for (eid, cnt) in &membership {
        if !existing.contains_key(eid) {
            errors.push(format!(
                "extent {eid}: listed by {cnt} stream(s) but has NO ExtentInfo (dangling membership)"
            ));
        }
    }
    errors
}

#[cfg(test)]
mod accounting_checker_tests {
    use super::check_accounting_invariants;

    #[test]
    fn clean_state_incl_cow_shared_and_pending_delete() {
        // extent 1: refs 1, in 1 stream. extent 10: refs 2, CoW-shared by 2
        // streams. extent 5: refs 0, in no stream (pending physical delete).
        let extents = [(1u64, 1u64, 0u64), (10, 2, 0), (5, 0, 0)];
        let streams = [vec![1u64, 10], vec![10]];
        assert!(
            check_accounting_invariants(&extents, &streams).is_empty(),
            "clean state (incl CoW refs=2 and refs=0 pending-delete) must pass"
        );
    }

    #[test]
    fn orphan_leak_refs_but_no_stream() {
        let errs = check_accounting_invariants(&[(7u64, 1u64, 0u64)], &[]);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("ORPHAN-LEAK"), "{}", errs[0]);
    }

    #[test]
    fn under_count_refs_below_membership() {
        // refs 1 but two streams list it → premature-delete risk.
        let errs = check_accounting_invariants(&[(3u64, 1u64, 0u64)], &[vec![3], vec![3]]);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("UNDER-COUNT"), "{}", errs[0]);
    }

    #[test]
    fn over_count_refs_above_membership() {
        let errs = check_accounting_invariants(&[(4u64, 2u64, 0u64)], &[vec![4]]);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("OVER-COUNT"), "{}", errs[0]);
    }

    #[test]
    fn vp_table_refs_nonzero_is_flagged() {
        let errs = check_accounting_invariants(&[(8u64, 1u64, 5u64)], &[vec![8]]);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("vp_table_refs=5"), "{}", errs[0]);
    }

    #[test]
    fn dangling_membership_stream_points_at_missing_extent() {
        // stream lists extent 9 but there is no ExtentInfo for it.
        let errs = check_accounting_invariants(&[], &[vec![9u64]]);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("dangling membership"), "{}", errs[0]);
    }
}

/// Terminal DECOMMISSION phase (gated by `AUTUMN_CHAOS_DECOMMISSION=1`).
///
/// After the nemesis loop has stopped and the cluster is restored to full
/// health, fully decommission ONE extent-node the HDFS way: fence it, wait for
/// the F-FENCE-DRAIN open-tail sweep + `fenced_only` recovery to relocate every
/// extent off it, then `MSG_REMOVE_NODE` — which refuses with
/// `CODE_PRECONDITION` (listing the still-referencing extents) until the node is
/// fully drained, and tombstones the address on success. This exercises the
/// permanent node-removal primitive end-to-end against a POPULATED cluster; the
/// existing per-key / range / accounting verify that follows then confirms NO
/// DATA LOSS with the node permanently gone.
///
/// Deliberately a ONE-SHOT terminal phase, NOT a per-cycle nemesis action:
/// removal is non-reversible (the address is tombstoned, `refs` relocate), while
/// every nemesis action must be reversible (kill→restart, fence→unfence,
/// partition→heal) so the cluster returns to K+M health each cycle. A permanent
/// node loss injected repeatedly would monotonically starve the cluster below
/// quorum. Drain also completes deterministically only once the faults stop.
///
/// Returns `Ok("skipped: …")` when the cluster is too small to lose a node,
/// `Ok("…")` on a clean decommission, and `Err` on a genuine failure (drain
/// wedge / unexpected code) — the caller panics on `Err`.
async fn run_terminal_decommission(
    mgr: &RpcClient,
    ens: &Rc<RefCell<Vec<EnProcess>>>,
    ec_k: u32,
    ec_m: u32,
) -> Result<String, String> {
    // Must end with ≥ K+M live ENs so the post-decommission read verify has
    // enough replicas (recovery rebuilds the victim's slots onto survivors).
    let alive: Vec<(usize, u64)> = ens
        .borrow()
        .iter()
        .enumerate()
        .filter(|(_, e)| e.is_alive())
        .map(|(i, e)| (i, e.node_id))
        .collect();
    let min_after = (ec_k + ec_m) as usize;
    if alive.len() <= min_after {
        return Ok(format!(
            "skipped: only {} alive ENs, need > K+M ({min_after}) to remove one",
            alive.len()
        ));
    }
    let (victim_idx, victim) = alive[0];
    eprintln!(
        "decommission: fencing node {victim} for removal ({} alive ENs)",
        alive.len()
    );

    // 1. Fence (force) — arms the F-FENCE-DRAIN open-tail drain + fenced_only
    //    recovery of the victim's sealed slots, and hard-excludes it from new
    //    placement.
    let resp = mgr
        .call(
            MSG_FENCE_NODE,
            rkyv_encode(&FenceNodeReq {
                node_id: victim,
                reason: "chaos decommission".into(),
                set_by: "chaos".into(),
                force: true,
            }),
        )
        .await
        .map_err(|e| format!("fence rpc: {e}"))?;
    let r: CodeResp = rkyv_decode(&resp).map_err(|e| format!("fence decode: {e}"))?;
    if r.code != CODE_OK {
        return Err(format!("fence node {victim} refused: {}", r.message));
    }

    // Optional: SIGKILL the fenced EN (`AUTUMN_CHAOS_DECOMMISSION_KILL=1`) — the
    // "stop the node you're retiring" flow. A dead replica forces recovery via the
    // disk-offline / probe-fail paths in addition to the fenced-dispatch path,
    // isolating whether a LIVE fenced node's healthy sealed replicas relocate.
    let kill_first = std::env::var("AUTUMN_CHAOS_DECOMMISSION_KILL")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if kill_first {
        ens.borrow_mut()[victim_idx].kill();
        eprintln!("decommission: SIGKILL fenced node {victim} (kill-then-drain)");
    }

    // 2. Poll MSG_REMOVE_NODE until the node is fully drained. Recovery
    //    dispatches every 2 s and F-FENCE-DRAIN rolls open tails each tick
    //    (30 s per-partition cooldown). A fenced node's healthy SEALED replicas
    //    are relocated by the fenced-slot recovery dispatch; if that first
    //    recovery attempt sticks (the EN copy stalls under churn), re-dispatch
    //    is frozen until the F-FENCE-DRAIN-2 stale-marker sweep releases the
    //    Recovery marker (recovery-specific threshold, default 120 s; the
    //    decommission chaos script sets it to 60 s for faster CI). 90 attempts
    //    × 3 s = 270 s ceiling covers a stick + one sweep-release + retry.
    //    Timing out past that means a genuine drain/recovery wedge → fail.
    const MAX_ATTEMPTS: u32 = 90;
    for attempt in 1..=MAX_ATTEMPTS {
        compio::time::sleep(Duration::from_secs(3)).await;
        let resp = mgr
            .call(
                MSG_REMOVE_NODE,
                rkyv_encode(&RemoveNodeReq {
                    node_id: victim,
                    set_by: "chaos".into(),
                }),
            )
            .await
            .map_err(|e| format!("remove rpc: {e}"))?;
        let r: RemoveNodeResp =
            rkyv_decode(&resp).map_err(|e| format!("remove decode: {e}"))?;
        match r.code {
            CODE_OK => {
                // Tombstoned — kill the process so it doesn't churn re-register
                // attempts against its now-tombstoned address for the rest of
                // the run.
                ens.borrow_mut()[victim_idx].kill();
                return Ok(format!(
                    "node {victim} decommissioned after {attempt} probe(s) (~{}s drain)",
                    attempt * 3
                ));
            }
            CODE_PRECONDITION => {
                // One-shot diagnostic: dump each blocking extent's full state so a
                // stall is root-causeable (open vs sealed, replicas/parity, EC).
                if attempt == 3 {
                    for eid in &r.blocking_extent_ids {
                        if let Ok(bytes) = mgr
                            .call(MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id: *eid }))
                            .await
                        {
                            if let Ok(ei) = rkyv_decode::<ExtentInfoResp>(&bytes) {
                                eprintln!("decommission: blocking extent {eid} → {:?}", ei.extent);
                            }
                        }
                    }
                }
                if attempt % 5 == 0 {
                    eprintln!(
                        "decommission: node {victim} still draining ({attempt}/{MAX_ATTEMPTS}): \
                         {} extent {:?} + {} marker {:?} refs remain",
                        r.blocking_extent_ids.len(),
                        r.blocking_extent_ids,
                        r.blocking_marker_extent_ids.len(),
                        r.blocking_marker_extent_ids,
                    );
                }
            }
            CODE_NOT_LEADER => {
                // Transient manager leader hiccup (election churn under chaos) —
                // the remove is idempotent, so just retry on the next probe
                // rather than failing the run.
                eprintln!("decommission: node {victim} remove got NOT_LEADER (attempt {attempt}) — retrying");
            }
            other => {
                return Err(format!(
                    "remove node {victim}: unexpected code {other}: {}",
                    r.message
                ));
            }
        }
    }
    Err(format!(
        "node {victim} did NOT drain within {}s — MSG_REMOVE_NODE stayed blocked \
         (drain / recovery wedge)",
        MAX_ATTEMPTS * 3
    ))
}

#[test]
#[ignore]
fn chaos_real_kill_split_merge_ec_fence_no_data_loss() {
    // Opt-in tracing: silent unless RUST_LOG is set (EnvFilter then applies
    // it). Lets a chaos run surface in-process PS / stream-layer internals
    // (e.g. RUST_LOG=warn,autumn_stream=info,autumn_partition_server=info) so
    // a wedged partition's parked operation is observable. `try_init` so a
    // second test in the same process doesn't panic on a double-install.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
    let cfg = ChaosConfig::from_env();
    eprintln!(
        "chaos: duration={}s nemesis_iv={}ms K={} M={} ENs={} seed={}",
        cfg.duration_secs, cfg.nemesis_interval_ms, cfg.ec_k, cfg.ec_m, cfg.num_ens, cfg.seed
    );

    let op_binary = binary_path("autumn-op");
    let en_binary = binary_path("autumn-extent-node");

    // -------- Real etcd (binary subprocess; kept alive by guard) --------
    let (_etcd_guard, etcd_endpoint) = compio::runtime::Runtime::new()
        .unwrap()
        .block_on(async { start_etcd().await });
    eprintln!("chaos: etcd at {etcd_endpoint}");

    // -------- Real toxiproxy (binary subprocess) --------
    let (_toxi_guard, toxi_admin) = compio::runtime::Runtime::new()
        .unwrap()
        .block_on(async { start_toxiproxy().await });
    eprintln!("chaos: toxiproxy admin at {toxi_admin}");
    let toxi = ToxiproxyCli::new(toxi_admin.clone());

    // -------- Manager (etcd-persistent, in-process for now) --------
    let mgr_addr = pick_addr();
    start_etcd_manager(mgr_addr, etcd_endpoint.clone());

    // Log dir for subprocesses.
    let log_dir = tempfile::tempdir().expect("log dir").keep();
    eprintln!("chaos: subprocess logs at {}", log_dir.display());

    // Owned tempdirs (separate from ProcessGuard's borrowed PathBuf).
    let mut en_dirs: Vec<tempfile::TempDir> = (0..cfg.num_ens)
        .map(|_| tempfile::tempdir().expect("en tempdir"))
        .collect();

    compio::runtime::Runtime::new().unwrap().block_on(async {
        // Wait until the manager has acquired the etcd leader lease — F149
        // bootstrap fence rejects writes from a non-leader, and the
        // election loop runs every 2 s. Without this, `autumn-op format`'s
        // `register_node` can race in before `try_become_leader` lands.
        let mgr_probe = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        let leader_ok = poll_until_async(
            Duration::from_secs(15),
            Duration::from_millis(300),
            || async {
                // status is a no-op-ish call; once the manager is leader,
                // register_node would succeed but status is safer.
                mgr_probe.call(MSG_STATUS, bytes::Bytes::new()).await.is_ok()
            },
        )
        .await;
        assert!(leader_ok, "manager never reachable");
        // Extra 1.5 s for leader-election + first replay to settle.
        compio::time::sleep(Duration::from_millis(1500)).await;

        // -------- Bootstrap EN subprocesses (toxiproxy proxy in front, then format, then EN) --------
        // Lifecycle: create toxiproxy proxy -> format (advertise=proxy_port)
        // -> spawn EN listening on real port -> wait for register heartbeat.
        // Manager + PS see ONLY the proxy address — nemesis can disable
        // the proxy to simulate a network partition without killing the EN.
        let mut ens: Vec<EnProcess> = Vec::new();
        for (i, dir) in en_dirs.iter_mut().enumerate() {
            // F270: ENs are killed + respawned on the SAME port mid-run; an
            // ephemeral-range port loses a race to outbound sockets while the
            // EN is down (respawn EADDRINUSE → fail-stop → permanently dead
            // EN → "no healthy node" alloc starvation = ALL of this run's
            // wedge modes). Fixed identities come from below the ephemeral
            // floor; +1000 (the control listener) is checked free too.
            let port = pick_stable_port_pair();
            // Control-proxy fix: the registered control_address is
            // advertise+1000, so the proxy port pair (p, p+1000) must BOTH
            // be free — the data proxy binds p, the control proxy binds
            // p+1000 (see bootstrap_en step 1b).
            let proxy_port = pick_proxy_port_pair();
            let proxy_name = format!("en-{i}");
            let guard = bootstrap_en(
                &op_binary,
                &en_binary,
                &mgr_addr,
                port,
                proxy_port,
                proxy_name.clone(),
                &toxi,
                dir.path().to_path_buf(),
                &log_dir,
            );
            eprintln!(
                "chaos: EN[{i}] real_port={port} proxy_port={proxy_port} node_id={} dir={}",
                guard.node_id,
                dir.path().display()
            );
            ens.push(guard);
        }
        // Wait for all ENs to land their first df with the manager so
        // they all transition Suspend → Online; otherwise create_stream
        // → select_nodes would only see the cold-leader fallback set.
        compio::time::sleep(Duration::from_secs(4)).await;

        let mgr: Rc<RpcClient> = RpcClient::connect(mgr_addr).await.expect("connect mgr");

        // -------- Create EC-policy streams + partition --------
        let log = create_stream_kp(&mgr, cfg.ec_k, cfg.ec_m).await;
        let row = create_stream_kp(&mgr, cfg.ec_k, cfg.ec_m).await;
        let meta = create_stream_kp(&mgr, cfg.ec_k, cfg.ec_m).await;
        let part_id = 9001u64;
        upsert_partition(&mgr, part_id, log, row, meta, b"a", b"z").await;

        // -------- Start PS (in-process) --------
        let ps_addr = pick_addr();
        start_partition_server(91, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(2500)).await;
        let router = Rc::new(PsRouter::new(mgr_addr, ps_addr));

        // -------- Workload state --------
        let topo = Rc::new(Topology::new());
        refresh_topology(&mgr, &topo).await;
        let expected: Rc<RefCell<HashMap<Vec<u8>, Vec<u8>>>> =
            Rc::new(RefCell::new(HashMap::new()));

        let stop = Arc::new(AtomicBool::new(false));
        let writes_acked = Arc::new(AtomicU64::new(0));
        let writes_failed = Arc::new(AtomicU64::new(0));
        let reads_ok = Arc::new(AtomicU64::new(0));
        let reads_miss = Arc::new(AtomicU64::new(0));
        let nemesis_events = Arc::new(AtomicU64::new(0));
        let nemesis_errors = Arc::new(AtomicU64::new(0));

        // -------- Spawn workload --------
        let w1 = compio::runtime::spawn({
            let router = router.clone();
            let topo = topo.clone();
            let expected = expected.clone();
            let stop = stop.clone();
            let writes_acked = writes_acked.clone();
            let writes_failed = writes_failed.clone();
            let lcg = Lcg::new(cfg.seed.wrapping_add(101));
            async move {
                writer_loop(
                    "w1", router, topo, expected, b'b', CHAOS_KEY_COUNT, stop, writes_acked, writes_failed, lcg,
                )
                .await;
            }
        });
        let w2 = compio::runtime::spawn({
            let router = router.clone();
            let topo = topo.clone();
            let expected = expected.clone();
            let stop = stop.clone();
            let writes_acked = writes_acked.clone();
            let writes_failed = writes_failed.clone();
            let lcg = Lcg::new(cfg.seed.wrapping_add(202));
            async move {
                writer_loop(
                    "w2", router, topo, expected, b'q', CHAOS_KEY_COUNT, stop, writes_acked, writes_failed, lcg,
                )
                .await;
            }
        });
        let r1 = compio::runtime::spawn({
            let router = router.clone();
            let topo = topo.clone();
            let expected = expected.clone();
            let stop = stop.clone();
            let reads_ok = reads_ok.clone();
            let reads_miss = reads_miss.clone();
            let lcg = Lcg::new(cfg.seed.wrapping_add(303));
            async move {
                reader_loop("r1", router, topo, expected, stop, reads_ok, reads_miss, lcg).await;
            }
        });
        let r2 = compio::runtime::spawn({
            let router = router.clone();
            let topo = topo.clone();
            let expected = expected.clone();
            let stop = stop.clone();
            let reads_ok = reads_ok.clone();
            let reads_miss = reads_miss.clone();
            let lcg = Lcg::new(cfg.seed.wrapping_add(404));
            async move {
                reader_loop("r2", router, topo, expected, stop, reads_ok, reads_miss, lcg).await;
            }
        });

        // Warm-up before nemesis.
        compio::time::sleep(Duration::from_secs(3)).await;

        // -------- Spawn nemesis --------
        let nemesis_ctx = Rc::new(NemesisCtx {
            mgr: mgr.clone(),
            router: router.clone(),
            topo: topo.clone(),
            ens: Rc::new(RefCell::new(ens)),
            en_binary: en_binary.clone(),
            manager_addr: mgr_addr,
            toxi: ToxiproxyCli::new(toxi_admin.clone()),
            fenced: RefCell::new(Vec::new()),
            dead: RefCell::new(Vec::new()),
            partitioned: RefCell::new(Vec::new()),
            nemesis_events: nemesis_events.clone(),
            nemesis_errors: nemesis_errors.clone(),
            ec_k: cfg.ec_k,
            ec_m: cfg.ec_m,
        });
        let n = compio::runtime::spawn({
            let ctx = nemesis_ctx.clone();
            let stop = stop.clone();
            let lcg = Lcg::new(cfg.seed.wrapping_add(909));
            let actions = cfg.actions.clone();
            async move {
                nemesis_loop(ctx, stop, cfg.nemesis_interval_ms, actions, lcg).await;
            }
        });

        // -------- Run --------
        let t0 = Instant::now();
        compio::time::sleep(Duration::from_secs(cfg.duration_secs)).await;
        eprintln!("chaos: stopping workload after {:?}", t0.elapsed());
        stop.store(true, Ordering::Relaxed);

        let _ = w1.await;
        let _ = w2.await;
        let _ = r1.await;
        let _ = r2.await;
        let _ = n.await;

        // Cleanup: unfence everyone and ensure ENs are alive for verify.
        let fenced_snapshot: Vec<u64> = nemesis_ctx.fenced.borrow().clone();
        for nid in fenced_snapshot {
            let _ = mgr
                .call(
                    MSG_CLEAR_NODE_OVERRIDE,
                    rkyv_encode(&ClearNodeOverrideReq {
                        node_id: nid,
                        set_by: "chaos-cleanup".into(),
                    }),
                )
                .await;
        }
        {
            let mut ens = nemesis_ctx.ens.borrow_mut();
            for e in ens.iter_mut() {
                if !e.is_alive() {
                    e.restart(&nemesis_ctx.en_binary, &nemesis_ctx.manager_addr);
                }
            }
        }
        // Re-enable any proxies left disabled by NetworkPartition.
        let partitioned_snapshot: Vec<String> = nemesis_ctx.partitioned.borrow().clone();
        for p in partitioned_snapshot {
            let _ = nemesis_ctx.toxi.set_enabled(&p, true);
        }

        eprintln!("chaos: settle 10 s before verify");
        compio::time::sleep(Duration::from_secs(10)).await;
        refresh_topology(&mgr, &topo).await;

        // -------- Terminal decommission (AUTUMN_CHAOS_DECOMMISSION=1) --------
        // Runs on the SETTLED cluster (all ENs back online, no in-flight chaos) —
        // the realistic operator scenario. Fully remove one node; the verify below
        // then proves no loss with it gone. Panic on genuine failure so a stuck
        // drain / lost data fails the run (a capacity skip is benign).
        if cfg.decommission {
            eprintln!("chaos: terminal node-decommission phase (post-settle)");
            match run_terminal_decommission(&mgr, &nemesis_ctx.ens, cfg.ec_k, cfg.ec_m).await {
                Ok(msg) => eprintln!("chaos: decommission — {msg}"),
                Err(msg) => panic!("chaos: DECOMMISSION FAILED — {msg}"),
            }
            // Let recovery's slot rebuilds settle + refresh routing before verify.
            compio::time::sleep(Duration::from_secs(5)).await;
            refresh_topology(&mgr, &topo).await;
        }

        // -------- Verify --------
        eprintln!(
            "chaos summary: writes acked={} failed={} | reads ok={} miss={} | nemesis events={} skipped={}",
            writes_acked.load(Ordering::Relaxed),
            writes_failed.load(Ordering::Relaxed),
            reads_ok.load(Ordering::Relaxed),
            reads_miss.load(Ordering::Relaxed),
            nemesis_events.load(Ordering::Relaxed),
            nemesis_errors.load(Ordering::Relaxed),
        );

        let expected_snapshot = expected.borrow().clone();
        eprintln!("chaos: verifying {} acked keys (per-key)", expected_snapshot.len());
        let (total, mismatches, not_found) =
            verify_per_key(&router, &topo, &expected_snapshot).await;
        eprintln!(
            "chaos: per-key verify: total={total} mismatches={} not_found={}",
            mismatches.len(),
            not_found.len()
        );

        eprintln!("chaos: verifying range() per partition");
        let range_errors = verify_per_partition_range(&router, &topo, &expected_snapshot).await;
        eprintln!("chaos: range verify: errors={}", range_errors.len());

        // Storage-accounting invariants (extent-10 / orphan-leak class): refs ==
        // membership, vp_table_refs == 0, no dangling membership. Read from the
        // manager's etcd state (source of truth); convergence-looped so a
        // background GC/split/merge mid-settle heals while a real leak persists.
        eprintln!("chaos: verifying extent accounting (refs/orphan/leak) via etcd");
        let (accounting_errors, acct_extents, acct_memberships) =
            verify_extent_accounting(&etcd_endpoint).await;
        eprintln!(
            "chaos: accounting verify: errors={} (checked {acct_extents} extents, {acct_memberships} memberships)",
            accounting_errors.len()
        );

        // POSITIVE reclamation (the flip side of no-loss): a final quiesce →
        // major-compact → FORCE-GC pass must physically DELETE extents, proving
        // GC/compaction actually reclaims (the replay floor advances) instead of
        // protecting everything forever. MUTATING, so it runs AFTER the read-only
        // verifiers above.
        eprintln!("chaos: verifying GC reclaim (quiesce → compact → force-GC → delete)");
        let (reclaim_errors, total_reclaimed, gc_reclaimed) =
            verify_gc_reclaim(&mgr, &router, &topo, &etcd_endpoint).await;
        eprintln!(
            "chaos: gc-reclaim: physically deleted {total_reclaimed} extent(s) \
             (force-GC step {gc_reclaimed}), errors={}",
            reclaim_errors.len()
        );

        // The reclaim pass punched extents (relocate-then-punch) — re-confirm it
        // lost NOTHING and left the accounting clean (a wrong vp_head would let
        // force-GC punch a live extent = loss / orphan). The pass ran ~30 s, in
        // which a background split/merge CONVERGENCE can move keys to a widened
        // survivor → the settle-time topo goes STALE and `verify_per_key`
        // mis-routes the read to the old owner (its retry hits the SAME wrong
        // partition, so it can't self-heal). Refresh routing + settle, then
        // re-verify; if anything is still off, refresh + retry ONCE more before
        // declaring a real loss — a mis-route heals on refresh, a real loss
        // persists across a fresh topo.
        eprintln!("chaos: post-reclaim re-verify (per-key + accounting)");
        refresh_topology(&mgr, &topo).await;
        compio::time::sleep(Duration::from_secs(2)).await;
        let (_pt, mut mismatches2, mut not_found2) =
            verify_per_key(&router, &topo, &expected_snapshot).await;
        if !mismatches2.is_empty() || !not_found2.is_empty() {
            eprintln!(
                "chaos: post-reclaim re-verify saw mismatches={} not_found={} — refreshing routing + retrying once (rule out a convergence mis-route)",
                mismatches2.len(),
                not_found2.len()
            );
            refresh_topology(&mgr, &topo).await;
            compio::time::sleep(Duration::from_secs(3)).await;
            let r = verify_per_key(&router, &topo, &expected_snapshot).await;
            mismatches2 = r.1;
            not_found2 = r.2;
        }
        let (accounting_errors2, _e2, _m2) = verify_extent_accounting(&etcd_endpoint).await;
        eprintln!(
            "chaos: post-reclaim: per-key mismatches={} not_found={} | accounting errors={}",
            mismatches2.len(),
            not_found2.len(),
            accounting_errors2.len()
        );

        // Post-settle write-liveness LAST: every partition must still take writes
        // (catches the recovery-stuck → alloc_extent wedge that read-only checks
        // can't see — point-gets keep working while writes hang). It OVERWRITES
        // real chaos keys with a probe seq, so it MUST run after every per-key
        // verify (incl. the post-reclaim one) or it poisons them. Running it here
        // also proves writes still land AFTER the force-GC reclaim pass.
        eprintln!("chaos: verifying write-liveness per partition (post-reclaim)");
        let liveness_errors = verify_write_liveness(&router, &topo).await;
        eprintln!("chaos: write-liveness verify: errors={}", liveness_errors.len());

        if !mismatches.is_empty()
            || !not_found.is_empty()
            || !range_errors.is_empty()
            || !liveness_errors.is_empty()
            || !accounting_errors.is_empty()
            || !reclaim_errors.is_empty()
            || !mismatches2.is_empty()
            || !not_found2.is_empty()
            || !accounting_errors2.is_empty()
        {
            panic!(
                "chaos verify FAILED — mismatches={} not_found={} range_errors={} liveness_errors={} accounting_errors={} reclaim_errors={} post_reclaim(mismatches={} not_found={} accounting={})\nmismatches: {}\nnot_found: {}\nrange_errors: {}\nliveness_errors: {}\naccounting_errors: {}\nreclaim_errors: {}\npost_reclaim_mismatches: {}\npost_reclaim_not_found: {}\npost_reclaim_accounting: {}",
                mismatches.len(),
                not_found.len(),
                range_errors.len(),
                liveness_errors.len(),
                accounting_errors.len(),
                reclaim_errors.len(),
                mismatches2.len(),
                not_found2.len(),
                accounting_errors2.len(),
                mismatches.iter().take(10).cloned().collect::<Vec<_>>().join(", "),
                not_found.iter().take(10).cloned().collect::<Vec<_>>().join(", "),
                range_errors.iter().take(10).cloned().collect::<Vec<_>>().join("; "),
                liveness_errors.iter().take(10).cloned().collect::<Vec<_>>().join("; "),
                accounting_errors.iter().take(10).cloned().collect::<Vec<_>>().join("; "),
                reclaim_errors.iter().take(10).cloned().collect::<Vec<_>>().join("; "),
                mismatches2.iter().take(10).cloned().collect::<Vec<_>>().join(", "),
                not_found2.iter().take(10).cloned().collect::<Vec<_>>().join(", "),
                accounting_errors2.iter().take(10).cloned().collect::<Vec<_>>().join("; "),
            );
        }
        eprintln!(
            "chaos: all invariants OK ({total} keys) — reclaimed {total_reclaimed} extent(s) post-quiesce"
        );
    });
}
