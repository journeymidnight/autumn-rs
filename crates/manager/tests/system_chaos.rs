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
//!     F207 inflight ledger, F211-D revision bumps, and F198 rich EC
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
    /// Names: split,merge,ec,fence,flush,compact,gc,kill,killfence,partition,latency
    /// Useful for bisecting which action triggers a failure.
    actions: Vec<Action>,
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
        Self {
            duration_secs: env_u64("AUTUMN_CHAOS_DURATION_SECS", 30),
            nemesis_interval_ms: env_u64("AUTUMN_CHAOS_NEMESIS_INTERVAL_MS", 3000),
            ec_k,
            ec_m,
            num_ens,
            seed,
            actions,
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
    fn pick<T: Copy>(&mut self, xs: &[T]) -> T {
        let i = (self.next() as usize) % xs.len();
        xs[i]
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

    let advertise = format!("127.0.0.1:{proxy_port}");
    let listen = format!(":{proxy_port}");

    // 2. autumn-op format <DIR> — talks to manager, allocates uuid,
    //    stamps sentinel files with the PROXY address. Synchronous.
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
            "--listen",
            &listen,
            "--advertise",
            &advertise,
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

fn make_value(key: &[u8], seq: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(b"chaos-");
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(b":");
    out.extend_from_slice(key);
    while out.len() < 256 {
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
        match client.call(partition_rpc::MSG_PUT, payload).await {
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
        match client.call(partition_rpc::MSG_GET, payload).await {
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
    ctx.partitioned.borrow_mut().push(victim_proxy.clone());
    eprintln!("nemesis: NetworkPartition {victim_proxy} (node {victim_node_id}) — disabled");

    compio::time::sleep(Duration::from_millis(3000)).await;

    ctx.toxi
        .set_enabled(&victim_proxy, true)
        .map_err(|e| format!("toxiproxy enable: {e}"))?;
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

async fn nemesis_loop(
    ctx: Rc<NemesisCtx>,
    stop: Arc<AtomicBool>,
    interval_ms: u64,
    actions: Vec<Action>,
    mut lcg: Lcg,
) {
    while !stop.load(Ordering::Relaxed) {
        compio::time::sleep(Duration::from_millis(interval_ms)).await;
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let action = lcg.pick(&actions);
        let result = match action {
            Action::Split => do_split(&ctx).await,
            Action::Merge => do_merge(&ctx).await,
            Action::EcConvert => do_ec_convert(&ctx).await,
            Action::FenceUnfence => do_fence_unfence(&ctx).await,
            Action::Flush => do_maintenance(&ctx, partition_rpc::MAINTENANCE_FLUSH, "flush").await,
            Action::Compact => {
                do_maintenance(&ctx, partition_rpc::MAINTENANCE_COMPACT, "compact").await
            }
            Action::Gc => do_maintenance(&ctx, partition_rpc::MAINTENANCE_AUTO_GC, "gc").await,
            Action::KillEn => do_kill_en(&ctx).await,
            Action::KillThenFence => do_kill_then_fence(&ctx).await,
            Action::NetworkPartition => do_network_partition(&ctx).await,
            Action::LatencySpike => do_latency_spike(&ctx).await,
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
    let total = expected.len();
    for (key, want) in expected {
        let mut got: Option<Vec<u8>> = None;
        for _attempt in 0..10 {
            // try_client_for never panics (vs `client_for` which does
            // after AUTUMN_TEST_ROUTER_RETRIES exhausted). If routing
            // still fails after that, log + skip — the verify path
            // will record the key as not_found.
            let part_id = topo.route(key);
            let client = match router.try_client_for(part_id).await {
                Ok(c) => c,
                Err(_) => {
                    compio::time::sleep(Duration::from_millis(500)).await;
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
            match client.call(partition_rpc::MSG_GET, payload).await {
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
            let resp = match client
                .call(partition_rpc::MSG_RANGE, partition_rpc::rkyv_encode(&req))
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    errors.push(format!("range rpc on part {pid}: {e}"));
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
                got.insert(kv.key);
            }
            // Advance cursor past `last`.
            let mut next = last;
            next.push(0);
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

#[test]
#[ignore]
fn chaos_real_kill_split_merge_ec_fence_no_data_loss() {
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
            let port = pick_addr().port();
            let proxy_port = pick_addr().port();
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
                    "w1", router, topo, expected, b'b', 200, stop, writes_acked, writes_failed, lcg,
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
                    "w2", router, topo, expected, b'q', 200, stop, writes_acked, writes_failed, lcg,
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

        if !mismatches.is_empty() || !not_found.is_empty() || !range_errors.is_empty() {
            panic!(
                "chaos verify FAILED — mismatches={} not_found={} range_errors={}\nmismatches: {}\nnot_found: {}\nrange_errors: {}",
                mismatches.len(),
                not_found.len(),
                range_errors.len(),
                mismatches.iter().take(10).cloned().collect::<Vec<_>>().join(", "),
                not_found.iter().take(10).cloned().collect::<Vec<_>>().join(", "),
                range_errors.iter().take(10).cloned().collect::<Vec<_>>().join("; "),
            );
        }
        eprintln!("chaos: all invariants OK ({total} keys)");
    });
}
