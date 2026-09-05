use crate::conn_pool::{parse_addr, shard_addr_for_extent};
use crate::extent_cksum;
use crate::extent_rpc::*;
use autumn_rpc::manager_rpc::{self, MgrExtentInfo};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Convert manager RPC ExtentInfo to local extent_rpc ExtentInfo.
/// `payload_location` rides beside `MgrExtentInfo` on the wire (it is not part
/// of the persisted struct), so the caller passes what the manager reported.
/// `ec_stage_nonce` value meaning "staging is CLOSED for this extent" — the
/// layout was flipped, so the shard file is live and no attempt may write it.
///
/// A real nonce is an etcd revision, so it is always below this and always
/// refused by `claim_ec_staging`.
const EC_STAGING_SEALED: u64 = u64::MAX;

/// What this node knows about EC staging for one extent.
///
/// The nonce orders two attempts against each other. The tick answers a
/// different question — *when* did this node last accept staging for it — and
/// that is what lets a reconcile verdict be judged for freshness: a verdict
/// asked for before the staging arrived cannot be describing it.
#[derive(Clone, Copy)]
struct EcStageMark {
    /// The claiming attempt's nonce, or `EC_STAGING_SEALED`.
    nonce: u64,
    /// The node's staging tick at the moment this mark was written.
    tick: u64,
}

fn mgr_to_local_extent(e: &MgrExtentInfo, payload_location: u8) -> ExtentInfo {
    ExtentInfo {
        extent_id: e.extent_id,
        replicates: e.replicates.clone(),
        parity: e.parity.clone(),
        eversion: e.eversion,
        refs: e.refs,
        sealed_length: e.sealed_length,
        sealed: e.sealed,
        avali: e.avali,
        replicate_disks: e.replicate_disks.clone(),
        parity_disks: e.parity_disks.clone(),
        ec_converted: e.ec_converted,
        payload_location,
    }
}
use anyhow::{Context, Result};
use autumn_rpc::{Frame, FrameDecoder, HandlerResult, StatusCode};
use bytes::Bytes;
use compio::fs::{File as CompioFile, OpenOptions};
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use compio::BufResult;
use dashmap::DashMap;
#[allow(unused_imports)]
use libc;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

// ─── Per-extent fallocate prealloc (opt-in) ──────────────────────────────────

/// Read `AUTUMN_EN_PREALLOC_BYTES`: when set to a positive integer N, every
/// freshly-created extent calls `fallocate(KEEP_SIZE, 0, N)` so the underlying
/// disk blocks are pre-reserved (saves ext4 extent-tree updates + per-grow
/// journal entries on subsequent appends). Default 0 = disabled, matching
/// pre-change behaviour. Cached via OnceLock to avoid env-read overhead in
/// the hot extent-creation path.
fn en_prealloc_bytes() -> u64 {
    static CELL: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *CELL.get_or_init(|| {
        std::env::var("AUTUMN_EN_PREALLOC_BYTES")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
    })
}

// ─── Per-node append metrics ─────────────────────────────────────────────────

pub(crate) struct ExtentAppendMetrics {
    started_at: Instant,
    req_count: u64,
    bytes: u64,
    total_ns: u64,
}

impl ExtentAppendMetrics {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            req_count: 0,
            bytes: 0,
            total_ns: 0,
        }
    }
    pub(crate) fn record(&mut self, reqs: u64, bytes: u64, elapsed_ns: u64) {
        self.req_count += reqs;
        self.bytes += bytes;
        self.total_ns += elapsed_ns;
        // Observability batch 1: process-global monotonic totals for the
        // /metrics endpoint. Three relaxed fetch_adds per append BATCH
        // (record is per-batch, not per-frame) — negligible on the hot path.
        EN_APPEND_TOTALS.requests.fetch_add(reqs, Ordering::Relaxed);
        EN_APPEND_TOTALS.bytes.fetch_add(bytes, Ordering::Relaxed);
        EN_APPEND_TOTALS.ns.fetch_add(elapsed_ns, Ordering::Relaxed);
        self.maybe_report();
    }
    fn maybe_report(&mut self) {
        if self.started_at.elapsed() >= Duration::from_secs(1) && self.req_count > 0 {
            let elapsed = self.started_at.elapsed();
            let batches = self.req_count.max(1);
            tracing::info!(
                req_count = self.req_count,
                mb_per_sec = self.bytes as f64 / elapsed.as_secs_f64() / 1_048_576.0,
                avg_write_ms = autumn_common::metrics::ns_to_ms(self.total_ns, batches),
                "extent append summary",
            );
            *self = Self::new();
        }
    }
}

thread_local! {
    pub(crate) static EXTENT_APPEND_METRICS: RefCell<ExtentAppendMetrics> =
        RefCell::new(ExtentAppendMetrics::new());
}

// ─── /metrics globals (observability batch 1) ───────────────────────────────
//
// The EN's authoritative state (`extents` DashMap, `disks` map) is
// shard-local behind `Rc` — unreadable from the metrics HTTP thread. Each
// shard therefore mirrors cheap gauges into an `Arc<EnShardGauges>` slot
// (registered at construction, refreshed from `handle_df` — the manager's
// ~2 s node_health_loop probe — so staleness is bounded by the df cadence)
// while monotonic append totals accumulate directly into process-global
// atomics from the per-batch `record` path. `render_en_metrics()` is safe
// to call from any thread.

pub struct EnAppendTotals {
    pub requests: std::sync::atomic::AtomicU64,
    pub bytes: std::sync::atomic::AtomicU64,
    pub ns: std::sync::atomic::AtomicU64,
}

pub(crate) static EN_APPEND_TOTALS: EnAppendTotals = EnAppendTotals {
    requests: std::sync::atomic::AtomicU64::new(0),
    bytes: std::sync::atomic::AtomicU64::new(0),
    ns: std::sync::atomic::AtomicU64::new(0),
};

pub struct EnShardGauges {
    shard_idx: u32,
    extents: std::sync::atomic::AtomicU64,
    /// (disk_id, DiskHealth as u64: 0=Online 1=Full 2=Faulted) — disk
    /// set is fixed at construction.
    disks: Vec<(u64, std::sync::atomic::AtomicU64)>,
}

/// One slot per LIVE ExtentNode instance. Weak entries (coco P2): a
/// dropped node (test teardown, failed init after registration) leaves a
/// dead Weak that the renderer prunes — no slot leak, no stale/duplicate
/// series, and the per-shard refresh task exits when its upgrades fail.
static EN_SHARD_GAUGES: std::sync::Mutex<Vec<std::sync::Weak<EnShardGauges>>> =
    std::sync::Mutex::new(Vec::new());

/// Render the EN's Prometheus text. Extents are summed across shards
/// (shards own disjoint extent sets); per-disk health takes the WORST
/// across shards (each shard holds its own `DiskFS` instance, and
/// `mark_disk_error_for_extent` flips only the observing shard's copy):
/// `autumn_en_disk_online` = 0 iff some shard sees Faulted,
/// `autumn_en_disk_full` = 1 iff some shard sees Full (and none Faulted).
pub fn render_en_metrics() -> String {
    use autumn_common::metrics_http::{push_metric, push_type};
    use std::sync::atomic::Ordering::Relaxed;
    let mut out = String::with_capacity(1024);
    push_type(&mut out, "autumn_en_append_batches_total", "counter");
    push_metric(
        &mut out,
        "autumn_en_append_batches_total",
        &[],
        EN_APPEND_TOTALS.requests.load(Relaxed) as f64,
    );
    push_type(&mut out, "autumn_en_append_bytes_total", "counter");
    push_metric(
        &mut out,
        "autumn_en_append_bytes_total",
        &[],
        EN_APPEND_TOTALS.bytes.load(Relaxed) as f64,
    );
    push_type(&mut out, "autumn_en_append_ns_total", "counter");
    push_metric(
        &mut out,
        "autumn_en_append_ns_total",
        &[],
        EN_APPEND_TOTALS.ns.load(Relaxed) as f64,
    );
    let slots: Vec<std::sync::Arc<EnShardGauges>> = {
        let mut guard = EN_SHARD_GAUGES.lock().unwrap();
        // Prune slots whose ExtentNode has dropped (coco P2).
        guard.retain(|w| w.strong_count() > 0);
        guard.iter().filter_map(|w| w.upgrade()).collect()
    };
    let mut extents_total: u64 = 0;
    // Worst health per disk across shards (0=Online 1=Full 2=Faulted).
    let mut disk_health: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
    push_type(&mut out, "autumn_en_shard_extents", "gauge");
    for s in slots.iter() {
        let e = s.extents.load(Relaxed);
        extents_total += e;
        push_metric(
            &mut out,
            "autumn_en_shard_extents",
            &[("shard", s.shard_idx.to_string())],
            e as f64,
        );
        for (disk_id, health) in &s.disks {
            let v = health.load(Relaxed);
            disk_health
                .entry(*disk_id)
                .and_modify(|cur| *cur = (*cur).max(v))
                .or_insert(v);
        }
    }
    push_type(&mut out, "autumn_en_extents", "gauge");
    push_metric(&mut out, "autumn_en_extents", &[], extents_total as f64);
    let mut disk_ids: Vec<u64> = disk_health.keys().copied().collect();
    disk_ids.sort_unstable();
    push_type(&mut out, "autumn_en_disk_online", "gauge");
    for disk_id in &disk_ids {
        push_metric(
            &mut out,
            "autumn_en_disk_online",
            &[("disk_id", disk_id.to_string())],
            u64::from(disk_health[disk_id] != 2) as f64,
        );
    }
    push_type(&mut out, "autumn_en_disk_full", "gauge");
    for disk_id in &disk_ids {
        push_metric(
            &mut out,
            "autumn_en_disk_full",
            &[("disk_id", disk_id.to_string())],
            u64::from(disk_health[disk_id] == 1) as f64,
        );
    }
    out
}

// ─── DiskFS ──────────────────────────────────────────────────────────────────

/// Represents one physical disk (data directory) on an extent node.
///
/// Files are stored in a hash-based layout:
/// `{base_dir}/{crc32c(extent_id_le)&0xFF:02x}/extent-{id}.dat`
/// This matches the 256 subdirs created by `autumn-op format`.
/// Hash subdirs are created on-demand when the first extent is written.
/// ENOSPC-1: per-disk health state machine. `Full` (capacity: ENOSPC /
/// EDQUOT) is RECOVERABLE — the per-shard 2 s sweep clears it once free
/// space returns above the hysteresis floor (GC / operator cleanup), so a
/// transiently-full disk no longer stays dead until process restart.
/// `Faulted` (any other I/O error = media/fs fault) keeps the historical
/// permanent-until-restart semantics. Proper enum, not a second bool —
/// the states are mutually exclusive and Faulted must never be
/// downgraded by a capacity probe.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub(crate) enum DiskHealth {
    Online = 0,
    Full = 1,
    Faulted = 2,
}

struct DiskFS {
    base_dir: PathBuf,
    disk_id: u64,
    /// SHARED across every DiskFS instance for the same physical
    /// directory in this process (coco P1: multi-shard builds one
    /// DiskFS per shard for the same dir — a shard-local health flag let
    /// shard B keep allocating onto a disk shard A had just marked Full).
    /// Keyed by canonical base_dir via `shared_disk_health`.
    health: std::sync::Arc<std::sync::atomic::AtomicU8>,
}

/// Process-global registry of per-directory health cells. Canonical path
/// keying means two shards (or a re-created ExtentNode) observing the
/// same physical dir share ONE state; distinct dirs (in-process tests,
/// real multi-disk) stay isolated. Entries are tiny and bounded by the
/// number of distinct data dirs ever opened in the process.
/// The completion queues every shard of this process pushes into.
///
/// A long-running EN task (recovery, EC conversion) reports finishing by
/// queueing here; the manager drains the queue on its next `df`. But the
/// manager makes exactly ONE `df` call per NODE — deliberately, because
/// `handle_df` takes the queue, so a second caller would drain-and-discard —
/// and it dials the registered control address, which is shard 0's.
///
/// Each shard is a separate `ExtentNode` with its own state, so a per-instance
/// queue meant every completion for an extent owned by shard 1..N was pushed
/// somewhere nothing ever read. The conversion could never commit (its marker
/// pinned the extent forever, blocking that extent's GC), and a rebuilt replica
/// was never applied (the slot stayed silently unrepaired). On a production EN,
/// where shard count is core count, that is (N-1)/N of the extents.
///
/// Sharing the queues across the process fixes it without adding a second `df`
/// caller: shard 0 drains what every shard produced. Same shape as
/// `shared_disk_health`, which shares disk state across shards for the same
/// structural reason. `Arc<Mutex<..>>` rather than `Rc<RefCell<..>>` because
/// shards are separate OS threads.
#[derive(Clone, Default)]
pub(crate) struct DoneQueues {
    recovery: std::sync::Arc<std::sync::Mutex<Vec<RecoveryTaskDone>>>,
    ec: std::sync::Arc<std::sync::Mutex<Vec<crate::extent_rpc::EcConvertDone>>>,
}

impl DoneQueues {
    fn push_recovery(&self, d: RecoveryTaskDone) {
        self.recovery.lock().expect("recovery_done").push(d);
    }
    fn push_ec(&self, d: crate::extent_rpc::EcConvertDone) {
        self.ec.lock().expect("ec_done").push(d);
    }
    fn take_recovery(&self) -> Vec<RecoveryTaskDone> {
        std::mem::take(&mut *self.recovery.lock().expect("recovery_done"))
    }
    fn take_ec(&self) -> Vec<crate::extent_rpc::EcConvertDone> {
        std::mem::take(&mut *self.ec.lock().expect("ec_done"))
    }
}

/// One set of queues per NODE, keyed by its data directories.
///
/// Not per process. In production those coincide — one EN process serves one
/// node — but the test suites run several logical ENs in a single process, and
/// a process-wide queue would let one node's `df` drain another's completions.
/// That is not merely untidy: the manager refuses an `ec_done` reported by a
/// node that is not the marker's coordinator, so cross-talk would make
/// conversions fail to commit in exactly the multi-EN tests that are supposed
/// to prove they do.
///
/// The data dirs are the right key because every shard of one node opens the
/// SAME `--data` dirs, and two different nodes never do — the same reasoning
/// `shared_disk_health` uses one field over.
fn shared_done_queues(disks: &[(PathBuf, Option<u64>)]) -> DoneQueues {
    static Q: std::sync::Mutex<Option<HashMap<String, DoneQueues>>> =
        std::sync::Mutex::new(None);
    let mut key_parts: Vec<String> = disks
        .iter()
        .map(|(dir, _)| {
            dir.canonicalize()
                .unwrap_or_else(|_| dir.clone())
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    key_parts.sort();
    let key = key_parts.join("|");
    Q.lock()
        .expect("done queues")
        .get_or_insert_with(HashMap::new)
        .entry(key)
        .or_default()
        .clone()
}

fn shared_disk_health(base_dir: &std::path::Path) -> std::sync::Arc<std::sync::atomic::AtomicU8> {
    static CELLS: std::sync::Mutex<
        Option<HashMap<PathBuf, std::sync::Arc<std::sync::atomic::AtomicU8>>>,
    > = std::sync::Mutex::new(None);
    let key = base_dir
        .canonicalize()
        .unwrap_or_else(|_| base_dir.to_path_buf());
    let mut guard = CELLS.lock().unwrap();
    guard
        .get_or_insert_with(HashMap::new)
        .entry(key)
        .or_insert_with(|| {
            std::sync::Arc::new(std::sync::atomic::AtomicU8::new(DiskHealth::Online as u8))
        })
        .clone()
}

impl DiskFS {
    /// Open a disk directory formatted by `autumn-op format`.
    /// Reads `disk_id` from `{base_dir}/disk_id`.
    async fn open(base_dir: PathBuf) -> Result<Self> {
        let disk_id_path = base_dir.join("disk_id");
        let data = compio::fs::read(&disk_id_path)
            .await
            .map_err(|e| anyhow::anyhow!("read disk_id in {}: {e}", base_dir.display()))?;
        let disk_id_str = String::from_utf8(data)
            .map_err(|e| anyhow::anyhow!("invalid utf8 disk_id in {}: {e}", base_dir.display()))?;
        let disk_id: u64 = disk_id_str
            .trim()
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid disk_id in {}", base_dir.display()))?;
        let health = shared_disk_health(&base_dir);
        Ok(Self {
            base_dir,
            disk_id,
            health,
        })
    }

    /// Create a disk entry with an explicit disk_id (no `disk_id` file required).
    fn with_disk_id(base_dir: PathBuf, disk_id: u64) -> Self {
        let health = shared_disk_health(&base_dir);
        Self {
            base_dir,
            disk_id,
            health,
        }
    }

    fn health(&self) -> DiskHealth {
        match self.health.load(Ordering::Relaxed) {
            1 => DiskHealth::Full,
            2 => DiskHealth::Faulted,
            _ => DiskHealth::Online,
        }
    }

    /// Historical "online" semantic = NOT faulted. A Full disk still
    /// serves reads and existing-extent operations; it only stops
    /// accepting NEW extents (`allocatable`). Reported as-is in df and
    /// metrics.
    fn online(&self) -> bool {
        self.health() != DiskHealth::Faulted
    }

    /// May this disk host a NEW extent? Online only — Full and Faulted
    /// are both excluded from `choose_disk`.
    fn allocatable(&self) -> bool {
        self.health() == DiskHealth::Online
    }

    fn set_faulted(&self) {
        self.health
            .store(DiskHealth::Faulted as u8, Ordering::Relaxed);
    }

    /// Capacity-full: only upgrades Online → Full. NEVER downgrades a
    /// Faulted disk (a media fault that also manifests ENOSPC-ish later
    /// must stay Faulted).
    fn set_full(&self) {
        let _ = self.health.compare_exchange(
            DiskHealth::Online as u8,
            DiskHealth::Full as u8,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }

    /// Self-heal: only Full → Online (the 2 s sweep calls this once free
    /// space is back above the hysteresis floor). Faulted is permanent
    /// until restart.
    fn try_clear_full(&self) -> bool {
        self.health
            .compare_exchange(
                DiskHealth::Full as u8,
                DiskHealth::Online as u8,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    /// Low byte of crc32c over extent_id little-endian bytes → hash subdir name.
    fn hash_byte(extent_id: u64) -> u8 {
        (crc32c::crc32c(&extent_id.to_le_bytes()) & 0xFF) as u8
    }

    /// Build the on-disk path for one of an extent's files in the hashed
    /// layout `{base_dir}/{hash:02x}/extent-{id}.{suffix}`
    /// (hash = `crc32c(id_le) & 0xFF`). Single source of truth for the
    /// layout shared with `autumn-op format`'s 256 subdirs +
    /// `remove_extent_files`.
    fn extent_file_path(&self, extent_id: u64, suffix: &str) -> PathBuf {
        self.base_dir
            .join(format!("{:02x}", Self::hash_byte(extent_id)))
            .join(format!("extent-{extent_id}.{suffix}"))
    }

    fn extent_path(&self, extent_id: u64) -> PathBuf {
        self.extent_file_path(extent_id, "dat")
    }

    fn meta_path(&self, extent_id: u64) -> PathBuf {
        self.extent_file_path(extent_id, "meta")
    }

    /// `extent-{id}.ck` — per-block content checksums for a SEALED extent.
    ///
    /// Absent is legal and means "no evidence": an extent sealed before this
    /// existed verifies as unknown, never as corrupt, which is what lets the
    /// sidecar arrive with no migration.
    fn ck_path(&self, extent_id: u64) -> PathBuf {
        self.extent_file_path(extent_id, "ck")
    }

        /// `extent-{id}.shard{i}` — this node's EC shard as an ADDITIVE file, so a
    /// conversion never has to modify or replace the `.dat` it is derived from.
    /// The index is in the NAME: a shard staged for one index can then never be
    /// served as another, whatever the caller believes.
    fn shard_path(&self, extent_id: u64, shard_index: u32) -> PathBuf {
        self.extent_file_path(extent_id, &format!("shard{shard_index}"))
    }

    /// #5 EC-COMMIT-ATOMIC: the commit-intent marker. Written durably BEFORE
    /// `commit_shard_local` renames `.ec.dat`→`.dat`, deleted after `save_meta`.
    /// Its presence on restart means the EC commit was interrupted between the
    /// rename and the meta write (the `.dat` may be the shard while `.meta` is
    /// still pre-EC); `load_extents` replays it to write the consistent `.meta`.
    /// Payload = `[new_eversion: u64 LE][sealed_length: u64 LE]`.
    /// `extent-{id}.ec.prepared` — records WHICH attempt produced the current
    /// `.ec.dat` staging (its `new_eversion`). Without it the coordinator's
    /// "prepare already done, skip to commit" check is size-only, and the
    /// staging of a DIFFERENT attempt (same extent + same K ⇒ same size) would
    /// satisfy it — committing stale, possibly wrong-shard-index staging over
    /// live replicas. Written durably at the END of a full prepare.
    fn ec_prepared_marker_path(&self, extent_id: u64) -> PathBuf {
        self.extent_file_path(extent_id, "ec.prepared")
    }

        /// unlink the `.dat`, `.meta`, and `.ec.dat` files
    /// for an extent. Idempotent — `NotFound` errors on any of the
    /// three are downgraded to `Ok(())` so retries from the manager
    /// are safe. Returns Err only on a real I/O failure (permission
    /// denied, etc.) so the caller can keep the entry in the
    /// pending-delete queue and retry.
    ///
    /// **`.ec.dat` staging files are now unlinked.** Previously
    /// `remove_extent_files` only touched `.dat` + `.meta`, leaving any
    /// `.ec.dat` from a crashed mid-conversion as a permanent orphan
    /// (orphan-reconcile only scanned `self.extents`, not the directory).
    /// With the mutating-op lock, a delete that races a convert is now
    /// refused — but a CRASH mid-convert can still leave a `.ec.dat`
    /// behind. Including it here ensures that when the manager
    /// eventually issues `MSG_DELETE_EXTENT` for the extent (refs→0),
    /// the staging file is also cleaned. The orphan reconcile loop
    /// (second leg) handles the case where the extent's
    /// `extent-{id}.dat` is already gone but `.ec.dat` survived.
    async fn remove_extent_files(&self, extent_id: u64) -> Result<()> {
        // Shard files are named per index, so the set to unlink is whatever is
        // actually on disk — a deleted extent that left a shard behind would be
        // invisible to every accounting path and reappear at the next restart.
        let mut paths = vec![
            self.extent_path(extent_id),
            self.meta_path(extent_id),
            self.ck_path(extent_id),
            self.ec_prepared_marker_path(extent_id),
        ];
        for idx in self.shard_indices_for(extent_id).await {
            paths.push(self.shard_path(extent_id, idx));
        }
        for path in paths {
            match compio::fs::remove_file(&path).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "remove {} (disk_id={}): {e}",
                        path.display(),
                        self.disk_id,
                    ));
                }
            }
        }
        Ok(())
    }

    /// Return (total_bytes, free_bytes) for this disk via statvfs.
    fn disk_stats(&self) -> (u64, u64) {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            if let Ok(c_path) = std::ffi::CString::new(self.base_dir.as_os_str().as_bytes()) {
                unsafe {
                    let mut stat: libc::statvfs = std::mem::zeroed();
                    if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
                        // statvfs field widths differ across libc targets
                        // (glibc x86_64 = c_ulong/u64; CI's ubuntu-latest
                        // libc bindings have f_blocks/f_bavail as u32).
                        // Cast both operands to u64 — portable AND prevents
                        // the silent u32*u32 overflow on any disk >4 TiB.
                        // `#[allow]` is required because on the host where
                        // the fields are already u64 the cast is a no-op.
                        #[allow(clippy::unnecessary_cast)]
                        let frsize = stat.f_frsize as u64;
                        #[allow(clippy::unnecessary_cast)]
                        let total = (stat.f_blocks as u64) * frsize;
                        #[allow(clippy::unnecessary_cast)]
                        let free = (stat.f_bavail as u64) * frsize;
                        return (total, free);
                    }
                }
            }
        }
        (1u64 << 40, 1u64 << 39)
    }

    /// Scan all extent data files across the 256 hash subdirs.
    /// Subdirs that don't exist yet are silently skipped.
    async fn scan_extents<F>(&self, mut callback: F) -> Result<()>
    where
        F: FnMut(u64, PathBuf),
    {
        for byte in 0u8..=255 {
            let subdir = self.base_dir.join(format!("{byte:02x}"));
            let dir = match std::fs::read_dir(&subdir) {
                Ok(d) => d,
                Err(_) => continue,
            };
            for entry in dir {
                let entry = entry?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if let Some(id) = Self::parse_extent_id(&name) {
                    callback(id, entry.path());
                }
            }
        }
        Ok(())
    }

    fn parse_extent_id(name: &str) -> Option<u64> {
        // Reject `.ec.dat` (the ec-staging file) — that prefix
        // also ends with ".dat" but parses as "42.ec" which fails
        // parse::<u64>. Be explicit about the rejection so future
        // maintainers don't accidentally match it here.
        if name.ends_with(".ec.dat") {
            return None;
        }
        if name.starts_with("extent-") && name.ends_with(".dat") {
            let id_str = &name["extent-".len()..name.len() - ".dat".len()];
            id_str.parse().ok()
        } else {
            None
        }
    }

    /// Parse `extent-{id}.shard{i}` into `(extent_id, shard_index)`. Any other
    /// shape — including `.dat`, `.ec.dat`, and the markers — is None.
    fn parse_shard_file(name: &str) -> Option<(u64, u32)> {
        let rest = name.strip_prefix("extent-")?;
        let (id_str, idx_str) = rest.split_once(".shard")?;
        Some((id_str.parse().ok()?, idx_str.parse().ok()?))
    }

    /// Scan all 256 hash subdirs for `extent-{id}.shard{i}` files.
    ///
    /// A shard file that nothing scans is a file that survives every cleanup
    /// and then vanishes from the system at the next restart — so discovery is
    /// part of the on-disk design, not an optimisation.
    async fn scan_shard_files<F>(&self, mut callback: F) -> Result<()>
    where
        F: FnMut(u64, u32),
    {
        for byte in 0u8..=255 {
            let subdir = self.base_dir.join(format!("{byte:02x}"));
            let dir = match std::fs::read_dir(&subdir) {
                Ok(d) => d,
                Err(_) => continue,
            };
            for entry in dir {
                let entry = entry?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if let Some((id, idx)) = Self::parse_shard_file(&name) {
                    callback(id, idx);
                }
            }
        }
        Ok(())
    }

    /// Every shard file this disk holds for `extent_id`. Used by delete (which
    /// must not leave shards behind) and by the footprint accounting.
    async fn shard_indices_for(&self, extent_id: u64) -> Vec<u32> {
        let mut out = Vec::new();
        let dir = self.extent_file_path(extent_id, "dat");
        let Some(parent) = dir.parent().map(|p| p.to_path_buf()) else {
            return out;
        };
        let Ok(rd) = std::fs::read_dir(&parent) else {
            return out;
        };
        for entry in rd.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some((id, idx)) = Self::parse_shard_file(&name) {
                if id == extent_id {
                    out.push(idx);
                }
            }
        }
        out.sort_unstable();
        out
    }

}

// ─── ExtentNodeConfig ─────────────────────────────────────────────────────────

/// Configuration for an ExtentNode.
///
/// All layouts use the hash-based file layout:
/// `{data_dir}/{hash_byte:02x}/extent-{id}.dat`.
/// Hash subdirs are created on-demand; no pre-formatting required.
///
/// - `new(data_dir, io_mode, disk_id)`: single disk with explicit disk_id (tests, simple deploys).
/// - `new_multi(data_dirs, io_mode)`: multiple disks; each dir must have a `disk_id` file
///   written by `autumn-op format`.
/// M1b: the EN's own live identity, threaded from the binary into
/// **shard 0's** `ExtentNode` (the only shard the manager dials for df — the
/// registered `control_address` is shard 0's control port) so `handle_df` can
/// ECHO it to the manager. The manager uses the echo to detect pod-IP reuse (a
/// different process answering at a stored address) and to WARN on stored-
/// location drift. Sibling shards (1+) carry the default (empty) registration;
/// their `handle_df` echoes empty, which the manager treats as "no echo" (a df
/// misrouted to a sibling simply skips the echo checks). Empty/absent when the
/// EN was launched without `--advertise`.
#[derive(Clone, Default)]
pub struct NodeRegistration {
    pub node_uuid: String,
    pub advertise_addr: String,
    pub shard_ports: Vec<u16>,
}

#[derive(Clone)]
pub struct ExtentNodeConfig {
    /// (dir, disk_id): None disk_id → read from `disk_id` file in dir.
    disks: Vec<(PathBuf, Option<u64>)>,
    pub manager_endpoint: Option<String>,
    /// this shard's index (0..shard_count). Only extents where
    /// `autumn_rpc::shard_for_extent(extent_id, shard_count) == shard_idx` are
    /// owned by this instance (the canonical hash; was a raw `extent_id %
    /// shard_count`).
    pub shard_idx: u32,
    /// total shard count in the extent-node process. 1 = legacy
    /// single-threaded mode; >1 enables per-shard filtering + routing.
    pub shard_count: u32,
    /// sibling shards' local listener addresses on this process
    /// (typically `127.0.0.1:<shard_ports[i]>`). Used by control-plane
    /// RPC handlers (alloc, re_avali, convert_to_ec, copy_extent,
    /// require_recovery) to forward a mismatched extent_id to the
    /// owning sibling shard via localhost loopback.
    pub sibling_addrs: Vec<String>,
    /// (was env `AUTUMN_EXTENT_EC_CONVERT_PARALLELISM`):
    /// cross-extent cap on concurrent `handle_convert_to_ec` heavy
    /// paths. Default 1 = fully serialise. Clamped to [1, 16].
    pub ec_convert_parallelism: usize,
    /// (was env `AUTUMN_EXTENT_RECOVERY_PARALLELISM`):
    /// cross-extent cap on concurrent `run_recovery_task` heavy paths.
    /// Default 2 (repair work — some concurrency speeds post-failure
    /// convergence). Clamped to [1, 16].
    pub recovery_parallelism: usize,
    /// (was env `AUTUMN_EXTENT_INFLIGHT_CAP`): per-conn
    /// FuturesUnordered cap for the connection-task SQ/CQ loop. Caps
    /// the per-client memory footprint at `cap × avg-frame`. Default
    /// 64 matches the historical env default.
    pub inflight_cap: usize,
    /// M1b: this EN's own identity to echo in `handle_df`.
    /// `None` = not self-registered (`--advertise` unset) → the manager skips
    /// the echo-based drift-heal / imposter checks.
    pub registration: Option<NodeRegistration>,
}

impl ExtentNodeConfig {
    /// Single-disk constructor. `disk_id` is used directly (no file needed).
    pub fn new(data_dir: PathBuf, disk_id: u64) -> Self {
        Self {
            disks: vec![(data_dir, Some(disk_id))],
            manager_endpoint: None,
            shard_idx: 0,
            shard_count: 1,
            sibling_addrs: Vec::new(),
            ec_convert_parallelism: 1,
            recovery_parallelism: 2,
            inflight_cap: 64,
            registration: None,
        }
    }

    /// Multi-disk constructor. Each directory must have a `disk_id` file
    /// written by `autumn-op format`.
    pub fn new_multi(data_dirs: Vec<PathBuf>) -> Self {
        Self {
            disks: data_dirs.into_iter().map(|d| (d, None)).collect(),
            manager_endpoint: None,
            shard_idx: 0,
            shard_count: 1,
            sibling_addrs: Vec::new(),
            ec_convert_parallelism: 1,
            recovery_parallelism: 2,
            inflight_cap: 64,
            registration: None,
        }
    }

    /// EC convert parallelism setter. Clamped to [1, 16].
    pub fn with_ec_convert_parallelism(mut self, n: usize) -> Self {
        self.ec_convert_parallelism = n.clamp(1, 16);
        self
    }

    /// recovery parallelism setter. Clamped to [1, 16].
    pub fn with_recovery_parallelism(mut self, n: usize) -> Self {
        self.recovery_parallelism = n.clamp(1, 16);
        self
    }

    /// per-conn inflight cap setter. Must be > 0; falls
    /// back to default 64 on 0.
    pub fn with_inflight_cap(mut self, n: usize) -> Self {
        self.inflight_cap = if n == 0 { 64 } else { n };
        self
    }

    /// M1b: set the EN's own identity to echo in `handle_df`.
    pub fn with_registration(
        mut self,
        node_uuid: impl Into<String>,
        advertise_addr: impl Into<String>,
        shard_ports: Vec<u16>,
    ) -> Self {
        self.registration = Some(NodeRegistration {
            node_uuid: node_uuid.into(),
            advertise_addr: advertise_addr.into(),
            shard_ports,
        });
        self
    }

    /// See the field docs: reporting is the default; this turns it into a
    /// refusal, which is how the fleet-wide precondition is established.
        pub fn with_manager_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.manager_endpoint = Some(endpoint.into());
        self
    }

    /// mark this config as a shard of a multi-shard extent-node.
    /// `shard_idx` must be < `shard_count`. `sibling_addrs[i]` is the
    /// local address of shard `i` (normally `127.0.0.1:<shard_ports[i]>`).
    pub fn with_shard(
        mut self,
        shard_idx: u32,
        shard_count: u32,
        sibling_addrs: Vec<String>,
    ) -> Self {
        assert!(shard_count >= 1, "shard_count must be >= 1");
        assert!(shard_idx < shard_count, "shard_idx must be < shard_count");
        if shard_count > 1 {
            assert_eq!(
                sibling_addrs.len(),
                shard_count as usize,
                "sibling_addrs must have exactly shard_count entries"
            );
        }
        self.shard_idx = shard_idx;
        self.shard_count = shard_count;
        self.sibling_addrs = sibling_addrs;
        self
    }
}

// ─── ExtentEntry ─────────────────────────────────────────────────────────────

/// Per-extent durability watermarks (formerly the fsync coalescer's state; the
/// coalescer task + waiter machinery were removed once the per-extent owner task
/// serialised appends and does the fsync inline). `pending_fsync` = high-water of
/// pwritten bytes; `last_synced` = high-water of durable (fsynced) bytes — read
/// by MSG_SYNCED_LENGTH / committed_length and gated by `fd_evictable`
/// (`sealed && pending_fsync <= last_synced && strong_count == 1`).
pub(crate) struct Coalescer {
    pub(crate) last_synced: AtomicU64,
    pub(crate) pending_fsync: AtomicU64,
}

impl Coalescer {
    fn new(initial_len: u64) -> Self {
        Self {
            last_synced: AtomicU64::new(initial_len),
            pending_fsync: AtomicU64::new(initial_len),
        }
    }
}

/// (1C): supervise a RESTARTABLE extent-node background loop —
/// catch_unwind, ERROR-log on panic/unexpected return, restart after 1 s. Use
/// ONLY for re-derive-each-tick loops with no moved resource (the orphan
/// reconcile sweep). Mirrors the manager / PS `spawn_supervised`. Previously
/// these were bare `spawn(..).detach()` → a panic killed the loop silently.
pub(crate) fn en_spawn_supervised<F, Fut>(name: &'static str, make: F)
where
    F: Fn() -> Fut + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    compio::runtime::spawn(async move {
        use futures::future::FutureExt;
        loop {
            let outcome = std::panic::AssertUnwindSafe(make()).catch_unwind().await;
            match outcome {
                Ok(()) => tracing::error!(
                    bg_loop = name,
                    "extent-node background loop returned unexpectedly; restarting in 1s"
                ),
                Err(_) => tracing::error!(
                    bg_loop = name,
                    "extent-node background loop PANICKED; restarting in 1s"
                ),
            }
            compio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .detach();
}

/// (1C): supervise a NON-restartable extent-node loop that owns a moved
/// resource (the per-extent fsync coalescer owns its wake-channel receiver) and
/// is durability-critical. NORMAL return is the expected lazy-exit path
/// (no-op). A PANIC means the fsync-coalescing path broke on possibly-
/// inconsistent state; restart-in-place is unsafe (the receiver is gone), so
/// **fail-stop the process** — the EN restarts and recovers extents from disk
/// (the data files are the journal; nothing committed is lost).
pub(crate) fn en_spawn_failstop<Fut>(name: String, fut: Fut)
where
    Fut: std::future::Future<Output = ()> + 'static,
{
    compio::runtime::spawn(async move {
        use futures::future::FutureExt;
        if std::panic::AssertUnwindSafe(fut)
            .catch_unwind()
            .await
            .is_err()
        {
            tracing::error!(
                bg_loop = %name,
                "extent-node background loop PANICKED on a moved-resource loop; \
                 fail-stopping the process (extents recover from disk on restart)"
            );
            std::process::exit(1);
        }
    })
    .detach();
}

pub(crate) struct ExtentEntry {
    /// Does `extent-{id}.dat` exist on disk?
    ///
    /// It is not implied by the entry existing any more: under the CoW
    /// conversion a node can hold ONLY `extent-{id}.shard{i}`, with its `.dat`
    /// already reclaimed. `holds_payload` reads this, so a read naming `.dat`
    /// on a shard-only holder is refused instead of resurrecting an empty file.
    has_dat: AtomicBool,
    /// `extent_rpc::PayloadLocation` as a byte — which file holds this node's
    /// payload, as last told by the manager and PERSISTED in `.meta`.
    /// `InShardFile` means the conversion committed here, which is what
    /// `load_extents` re-derives the EC staging seal from after a restart.
    payload_location: AtomicU8,
    /// The shard indices this node holds files for.
    ///
    /// The shard files this node holds, `index -> byte length`.
    ///
    /// Two at once is a LEGAL transient — a node can be a target of two
    /// attempts, or hold a parity slot and a data slot after a reassignment —
    /// so this is a map, and the extent's published layout (not this map)
    /// decides which one is authoritative. Lengths live here rather than in a
    /// separate counter so "which files exist" and "how many bytes they cost"
    /// cannot drift apart; `len` is the `.dat` length and says nothing about
    /// them.
    shard_files: RefCell<std::collections::BTreeMap<u32, u64>>,
    /// structural close of the type-level UB at the file-replacement
    /// path. This was previously `UnsafeCell<CompioFile>` and the replace
    /// path (`*entry.file.get() = new_file`) could dangle a concurrent
    /// reader's `&CompioFile` borrow if the EC-conversion lock missed
    /// any reader (theoretical UB even when in practice ruled out by
    /// the eversion check).
    ///
    /// Now `RefCell<Rc<CompioFile>>`. Reads clone the `Rc` while
    /// holding a brief `borrow()`; the I/O runs on the cloned `Rc` so
    /// the borrow is released before any `.await`. The replace path
    /// takes a `borrow_mut()` and `Rc::replace` — the OLD `Rc` is
    /// returned and dropped only when the last concurrent reader
    /// releases its clone, so the underlying file handle / fd cannot
    /// dangle. No `unsafe` anywhere in the file-access path.
    ///
    /// now `Option<Rc<CompioFile>>`. `None` = the fd has been
    /// EVICTED by the sealed-extent fd cache (`FdLru`) to bound open fds on a
    /// node with many extents. **Only SEALED, idle, UNREFERENCED extents are
    /// ever evicted** (`fd_evictable`: `sealed && pending_fsync<=last_synced &&
    /// strong_count==1`). The real invariant is NOT "the write path never sees
    /// `None`" (an OPEN extent CAN be sealed concurrently, then evicted) — it is:
    /// every path resolves the fd via `resident_file()` (sync, write/durability
    /// path) or `ExtentNode::extent_file` (async, read/sealed-op path), holds the
    /// returned `Rc` for its whole I/O (so the `strong_count==1` evict
    /// guard can't yank it mid-op), and treats `None` as "concurrently sealed" →
    /// a clean `CODE_PRECONDITION` reject, never a panic. Eviction dropping the
    /// cache's `Rc` is the SAME structural safety as `replace_file`: a
    /// concurrent holder's `Rc` clone keeps the fd alive until it finishes.
    pub(crate) file: RefCell<Option<Rc<CompioFile>>>,
    /// this extent's id — needed to re-open the `.dat` on a cache
    /// miss (`disk_for(disk_id).extent_path(extent_id)`). Immutable.
    pub(crate) extent_id: u64,
    pub(crate) len: AtomicU64,
    pub(crate) eversion: AtomicU64,
    pub(crate) sealed_length: AtomicU64,
    /// P0-C: authoritative "is this extent sealed" flag, mirroring
    /// `ExtentInfo.sealed` / `MgrExtentInfo.sealed`. This is the STATE
    /// ("is sealed"); `sealed_length` is the LENGTH ("how much / is empty").
    /// An authoritative EMPTY seal is `sealed = true, sealed_length = 0`
    /// (e.g. a CoW-shared empty tail frozen by split/merge so children
    /// alloc a fresh tail). Pre-P0-C the EN derived "is sealed" from
    /// `sealed_length > 0`, so a sealed-empty extent looked OPEN after a
    /// restart — a stale/ghost writer could then append to a
    /// manager-sealed / CoW-shared extent (CoW isolation break), and a
    /// later VP/SST referencing offset>0 surfaced as
    /// `stale_vp_offset_past_sealed_length sealed_length=0`. Persisted in
    /// the V2 `.meta` sidecar. Invariant: `sealed_length > 0 ⇒ sealed`.
    pub(crate) sealed: AtomicBool,
    pub(crate) avali: AtomicU32,
    /// In-memory fencing bar: appends with `owner_epoch < owner_epoch` are
    /// rejected (CODE_LOCKED_BY_OTHER). Raised SYNCHRONOUSLY (monotonic
    /// `fetch_max`) the instant a higher owner_epoch arrives, so a stale lower
    /// owner is locked out immediately — even while the new fence is still
    /// being persisted.
    pub(crate) owner_epoch: AtomicI64,
    /// P0-B: the owner_epoch value KNOWN TO BE DURABLE in the `.meta`
    /// sidecar. `owner_epoch` (the in-memory bar) may be ahead of this while
    /// a persist is in flight. An append at owner_epoch R may be ACKed only once
    /// `durable_owner_epoch >= R` — otherwise a crash after the ACK but
    /// before the persist would let a stale lower owner re-pass the on-disk
    /// fence on restart (split-brain). Kept ≤ `owner_epoch`; advanced only
    /// inside the per-extent meta-write critical section AFTER `.meta` fsync.
    pub(crate) durable_owner_epoch: AtomicI64,
    /// Which disk this extent lives on. Used to resolve file paths.
    pub(crate) disk_id: u64,
    /// Phase 1: per-extent fsync coalescer state.
    pub(crate) coalescer: Coalescer,
    /// [owner-model] this extent's per-extent owner mailbox. Mirrors the
    /// coalescer's proven queue+wake+exit-when-idle lifecycle (fable P2-5): the
    /// message `queue` AND the "task running" `wake_tx` live under ONE RefCell so
    /// enqueue/spawn and drain/exit are atomic on the single-threaded runtime
    /// (no lost-wake). The owner holds a strong `Rc<ExtentEntry>` while running
    /// but EXITS when the queue drains (dropping the Rc) so it never pins a
    /// sealed/idle extent past eviction. WIP: read by the owner wiring.
    #[allow(dead_code)]
    pub(crate) owner: RefCell<OwnerMailbox>,
    /// META-FAILCLOSED: set true at load time when the `.meta` sidecar is
    /// PRESENT but CORRUPT (CRC/magic/extent_id invalid — bit rot / torn
    /// write / power loss) while the `.dat` still exists. A corrupt `.meta`
    /// must NOT silently default the extent to `open, owner_epoch=0` (that
    /// would let a stale/lower-epoch writer bypass the fence and ghost-append
    /// — see `corrupt_meta_quarantines_extent_and_rejects_stale_append`).
    /// While set, append / read / commit_length are REFUSED (the extent is
    /// quarantined); the manager rebuilds authoritative state via
    /// recovery / re_avali, whose `.meta` write clears this flag. A genuinely
    /// ABSENT `.meta` (fresh extent, or crash between `.dat` create and first
    /// `.meta` write) is NOT quarantined — only present-but-corrupt is.
    pub(crate) corrupt_meta: AtomicBool,
    /// Cached `.ck` for this extent; see `CachedChecksums`.
    content_ck: RefCell<CachedChecksums>,
}

impl ExtentEntry {
    /// Does this node hold the payload file a request NAMED?
    ///
    /// The answer must never be "close enough": serving `.dat` to a request for
    /// a shard file (or the reverse) returns shard bytes as a whole value, the
    /// exact corruption the location field exists to rule out. A caller naming
    /// a file this node does not have is told so, and refreshes its layout.
    fn holds_payload(&self, p: PayloadRef) -> bool {
        match p.location {
            PayloadLocation::InDat => self.has_dat.load(Ordering::SeqCst),
            PayloadLocation::InShardFile => self.shard_files.borrow().contains_key(&p.shard_index),
        }
    }

    /// Record that `extent-{id}.shard{i}` exists here with `len` bytes.
    fn note_shard_file(&self, shard_index: u32, len: u64) {
        self.shard_files.borrow_mut().insert(shard_index, len);
    }

    /// Forget a shard file this node no longer holds. Call AFTER the unlink, so
    /// the entry never advertises a file that is gone.
    fn forget_shard_file(&self, shard_index: u32) {
        self.shard_files.borrow_mut().remove(&shard_index);
    }

    /// Unlink a shard file, then stop advertising it -- in that order, and only
    /// if the file is really gone.
    ///
    /// The two steps are one invariant, which is why they live in one place
    /// instead of being spelled out at each call site (they were, and the
    /// failed-rebuild path spelled out only the first half). Break it either
    /// way and the entry disagrees with the disk:
    ///
    /// - unlink without forgetting: `holds_payload` stays true and `df` keeps
    ///   counting bytes that are gone, so a read routed here clears the
    ///   ownership gate and then fails inside `payload_file` as `Internal`
    ///   rather than refusing cleanly as `PayloadNotHere`.
    /// - forget without unlinking: the bytes stay on disk uncounted. Where the
    ///   forgotten index is also the one the layout wants, it additionally
    ///   makes `holds_payload(want)` false, which gates the `.dat` reclaim.
    ///
    /// `NotFound` counts as gone. A failed unlink KEEPS the record and returns
    /// the error, so the bytes and the accounting stay in agreement; what
    /// eventually clears the file is caller-specific, so the callers say so,
    /// not this doc.
    ///
    /// NOT atomic: the unlink is awaited before the record is dropped, so a
    /// writer that recreates this same index during the await has its record
    /// removed here while its file exists. `note_shard_file` on the next
    /// stripe, or restart discovery, puts it back. The window is pre-existing
    /// -- the reconcile sweep has always had it -- and is called out only
    /// because the wording above could be read as claiming an atomic pair.
    async fn discard_shard_file(
        &self,
        path: &std::path::Path,
        shard_index: u32,
    ) -> std::io::Result<()> {
        match compio::fs::remove_file(path).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        self.forget_shard_file(shard_index);
        Ok(())
    }

    fn shard_index_list(&self) -> Vec<u32> {
        self.shard_files.borrow().keys().copied().collect()
    }

    /// Bytes this extent's shard files occupy on this node.
    ///
    /// `.dat` is `len`, counted separately: a node mid-conversion legitimately
    /// holds BOTH, so reporting one under-counts the footprint the allocation
    /// gate and cluster-df read. The converse matters just as much — `len` is
    /// the `.dat` length and is 0 when there is no `.dat`, or the two would
    /// double-count the same bytes.
    fn shard_bytes(&self) -> u64 {
        self.shard_files.borrow().values().sum()
    }

    /// Recorded length of one shard file, or `None` if this node holds no file
    /// at that index.
    fn shard_file_len(&self, shard_index: u32) -> Option<u64> {
        self.shard_files.borrow().get(&shard_index).copied()
    }

    /// replace the file handle. Safe by construction —
    /// `RefCell::borrow_mut` panics if any borrow is currently held,
    /// and concurrent readers have already cloned an `Rc<CompioFile>`
    /// off a brief `borrow()` so they hold no `RefCell` borrow during
    /// their I/O. The OLD `Rc` is returned and dropped only when the
    /// last concurrent reader releases its clone — the underlying fd
    /// cannot dangle.
    ///
    /// The per-extent EC-conversion lock still serialises concurrent
    /// `handle_convert_to_ec` dispatches at a higher level (so two
    /// converts don't race on the staging file), but is no longer
    /// load-bearing for memory safety of the replace itself.
    /// Record that `len` durable bytes were installed out of band — by a
    /// peer copy or a recovery rebuild, both of which fsync before installing.
    ///
    /// The coalescer's watermarks are otherwise advanced only by the append
    /// paths, which never run on a repaired replica. Leaving them behind makes
    /// the extent read as permanently un-synced to everything that asks —
    /// including the content checksum, which then refuses to describe exactly
    /// the copy that was just rebuilt. One definition, so a future repair path
    /// cannot forget half of it.
    pub(crate) fn note_durable_install(&self, len: u64) {
        self.len.store(len, Ordering::SeqCst);
        self.coalescer.last_synced.fetch_max(len, Ordering::SeqCst);
        self.coalescer.pending_fsync.fetch_max(len, Ordering::SeqCst);
    }

    pub(crate) fn replace_file(&self, new_file: CompioFile) {
        // replacing installs a fresh resident fd (EC-commit /
        // recovery writeback). Sets `Some` — the extent is pinned resident.
        *self.file.borrow_mut() = Some(Rc::new(new_file));
    }

    /// SYNC accessor — clone the resident fd if present, else
    /// `None` (the extent's fd was evicted; by construction it is sealed + idle).
    /// This REPLACED the old panic-on-`None` `file_rc()` (a coco/subagent
    /// finding: an accepted append or in-flight `truncate_to_commit` could hit a
    /// concurrent seal+evict window and PANIC at first poll). Callers on the
    /// write/durability path resolve once (pinning the `Rc`) and treat
    /// `None` as "extent was concurrently sealed" → the semantically-correct
    /// `CODE_PRECONDITION` reject, NOT a panic. Read / sealed-extent background
    /// ops use the async `ExtentNode::extent_file` (open-on-miss) instead.
    pub(crate) fn resident_file(&self) -> Option<Rc<CompioFile>> {
        self.file.borrow().clone()
    }

    /// drop the cached fd (eviction). Safe by the same
    /// reasoning as `replace_file`: a concurrent reader's `Rc` clone keeps the
    /// underlying fd alive until it finishes; only the cache's reference is
    /// released here. Callers MUST call `fd_evictable` first.
    pub(crate) fn evict_file(&self) {
        *self.file.borrow_mut() = None;
    }

    /// is this extent's fd safe to evict RIGHT NOW? Three
    /// conditions, all load-bearing:
    /// - `sealed` — only sealed extents are ever cached/evicted (open/active
    ///   extents are pinned; the write/coalescer path assumes them resident);
    /// - `pending_fsync <= last_synced` — no un-fsynced bytes the coalescer
    ///   still owes (it would need the fd);
    /// - `Rc::strong_count == 1` — the cache is the SOLE holder, i.e. NO
    ///   in-flight I/O holds a clone. This closes the seal-transition panic
    ///   window: an append/truncate that resolved its fd (holding a clone)
    ///   before a concurrent seal cannot have its fd yanked mid-op, and the
    ///   coalescer (whose waiter's writer held a clone through registration,
    ///   after which `pending > synced` takes over) is likewise never evicted
    ///   out from under a pending fsync.
    pub(crate) fn fd_evictable(&self) -> bool {
        if !self.sealed.load(Ordering::Relaxed) {
            return false;
        }
        if self.coalescer.pending_fsync.load(Ordering::Relaxed)
            > self.coalescer.last_synced.load(Ordering::Relaxed)
        {
            return false;
        }
        // strong_count of the CACHED Rc without cloning (a clone would inflate
        // it). `None` (already evicted) → not evictable (nothing to do).
        self.file
            .borrow()
            .as_ref()
            .is_some_and(|rc| Rc::strong_count(rc) == 1)
    }
}

/// max resident SEALED-extent fds cached per shard. Open/active
/// extents are pinned (NOT counted here), so the process fd ceiling is
/// `fd_cache_cap + Σ(open tails) + sockets`. Default 4096 — well under the
/// raised RLIMIT_NOFILE (65535) with room for open tails + TCP
/// conns. Set via `--fd-cache-cap` (binary); OnceLock first-call-wins, env-free
/// per the project's no-env-in-Rust rule (the shell maps env→flag).
static FD_CACHE_CAP_CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
fn fd_cache_cap() -> usize {
    FD_CACHE_CAP_CELL.get().copied().unwrap_or(4096)
}

/// Binary override for `fd_cache_cap` (first-call-wins, returns false if already
/// set). The extent-node binary calls this from CLI parsing before
/// `ExtentNode::new`. Mirrors `set_ec_encode_stripe_bytes`. Floored at 64: a
/// tiny cache would churn (and, historically, widened the now-closed
/// seal-transition eviction window).
pub fn set_fd_cache_cap(cap: usize) -> bool {
    FD_CACHE_CAP_CELL.set(cap.max(64)).is_ok()
}

/// a bounded LRU cache of open file descriptors for SEALED
/// extents on one shard. Open/active extents are NEVER tracked here (their fd
/// is pinned resident by `ExtentEntry.file = Some`); only sealed, idle,
/// unreferenced extents (`fd_evictable`) are cached + evicted. The write /
/// coalescer / durability path resolves its fd via `resident_file()` and holds
/// the `Rc` for its whole op, so the `strong_count==1` evict guard never yanks
/// an fd from under an in-flight write/fsync.
///
/// When the number of resident sealed fds exceeds `cap`, the least-recently-used
/// one's fd is dropped (`ExtentEntry::evict_file`) — re-opened lazily on the
/// next read via `ExtentNode::extent_file`. Eviction dropping the cache's `Rc`
/// is the same structural safety as `replace_file`: a concurrent reader
/// holds its own `Rc` clone across `.await`, so the fd stays alive until the
/// reader finishes.
///
/// Single-threaded per shard (compio thread-per-core) → `Cell`/`RefCell`, no
/// atomics/locks needed for the LRU bookkeeping.
pub(crate) struct FdLru {
    cap: usize,
    seq: std::cell::Cell<u64>,
    /// recency index: seq -> extent_id (BTreeMap → O(log n) LRU pop).
    by_seq: RefCell<std::collections::BTreeMap<u64, u64>>,
    /// extent_id -> its current seq (for O(log n) re-touch / forget).
    seq_of: RefCell<HashMap<u64, u64>>,
    /// clone of the shard's extent map, so eviction can reach a victim entry.
    extents: Rc<DashMap<u64, Rc<ExtentEntry>>>,
}

impl FdLru {
    fn new(cap: usize, extents: Rc<DashMap<u64, Rc<ExtentEntry>>>) -> Self {
        Self {
            cap: cap.max(1),
            seq: std::cell::Cell::new(0),
            by_seq: RefCell::new(std::collections::BTreeMap::new()),
            seq_of: RefCell::new(HashMap::new()),
            extents,
        }
    }

    /// Record a use of `extent_id`'s (resident, sealed) fd, then evict the
    /// least-recently-used sealed fds while over `cap`.
    fn touch(&self, extent_id: u64) {
        let s = self.seq.get();
        self.seq.set(s.wrapping_add(1));
        {
            let mut seq_of = self.seq_of.borrow_mut();
            let mut by_seq = self.by_seq.borrow_mut();
            if let Some(old) = seq_of.insert(extent_id, s) {
                by_seq.remove(&old);
            }
            by_seq.insert(s, extent_id);
        }
        self.evict_over_cap();
    }

    fn evict_over_cap(&self) {
        // Scan LRU→MRU, evicting the first `fd_evictable` victim each pass;
        // KEEP non-evictable victims tracked (coco P1: dropping them from
        // tracking left their fd resident but un-cap-accounted → an fd leak
        // past `cap`). `skip` counts victims deferred THIS convergence so a
        // pathological all-non-evictable set terminates (fds temporarily > cap,
        // but every one is genuinely in-use / pending — bounded by concurrent
        // ops). No borrow held across `entry` access (evict_file borrows the
        // entry's RefCell).
        let mut skip = 0usize;
        loop {
            let victim = {
                let seq_of = self.seq_of.borrow();
                if seq_of.len() <= self.cap {
                    return;
                }
                // nth(skip): the `skip`-th least-recently-used still-tracked id.
                self.by_seq
                    .borrow()
                    .iter()
                    .nth(skip)
                    .map(|(&s, &id)| (s, id))
            };
            let Some((s, vid)) = victim else { return }; // scanned all → all in-use
            let evictable = self
                .extents
                .get(&vid)
                .is_some_and(|entry| entry.fd_evictable());
            if evictable {
                self.seq_of.borrow_mut().remove(&vid);
                self.by_seq.borrow_mut().remove(&s);
                if let Some(entry) = self.extents.get(&vid) {
                    entry.evict_file();
                }
                // indices shifted; restart the LRU scan from the front.
                skip = 0;
            } else {
                // in-use (pending fsync / in-flight I/O clone / not sealed) —
                // leave it tracked + resident; try the next-oldest.
                skip += 1;
            }
        }
    }

    /// Stop tracking an extent (on delete). Idempotent.
    fn forget(&self, extent_id: u64) {
        if let Some(s) = self.seq_of.borrow_mut().remove(&extent_id) {
            self.by_seq.borrow_mut().remove(&s);
        }
    }

    #[cfg(test)]
    fn resident_count(&self) -> usize {
        self.seq_of.borrow().len()
    }
}

/// P0-C: parsed `.meta` sidecar contents. Replaces the prior
/// `(sealed_length, eversion, owner_epoch)` tuple so the `sealed` /
/// `avali` fields can't be silently dropped at a call site.
#[derive(Debug, PartialEq, Eq, Clone)]
struct LocalExtentMeta {
    sealed_length: u64,
    eversion: u64,
    owner_epoch: i64,
    sealed: bool,
    avali: u32,
    /// Which file holds this node's payload for the extent
    /// (`extent_rpc::PayloadLocation` as a byte).
    ///
    /// Durable because it is the only thing that survives a restart to say an
    /// EC conversion COMMITTED here. The staged shard file becomes the live
    /// one at the manager's layout flip — there is no rename — so after the
    /// flip no attempt may write it again. That refusal is driven by an
    /// in-memory seal, which a restart would otherwise drop, reopening the
    /// window for a superseded coordinator's late stripe to overwrite live
    /// data. V0/V1 records and every extent written before this field read as
    /// `InDat`, which is the documented default.
    payload_location: u8,
}

// ─── ConcurrencyController ──────────────────────────────────
//
// Renamed from `ExtentNodeGate` to mirror PS's
// `partition_server::ConcurrencyController`. Same purpose on both
// sides: per-process cap on the number of simultaneous memory-heavy
// background operations. RAM cap, not rate cap (rate cap is not yet
// implemented on EN — see `[[stream/CLAUDE.md]]` "Concurrency vs rate
// limiting" section).
//
// Two independent counters in one struct (mirrors PS holding compact +
// gc concurrency together):
//   - `ec_convert`  — caps `handle_convert_to_ec` heavy paths.
//     Default `ExtentNodeConfig.ec_convert_parallelism = 1`.
//   - `recovery`    — caps `run_recovery_task` heavy paths.
//     Default `ExtentNodeConfig.recovery_parallelism = 2`.
//
// Uses `Cell<usize>` (NOT `AtomicUsize`) because each extent-node
// shard runs on a single-threaded compio runtime — all acquires
// happen on one OS thread, so no cross-thread atomic is needed.
// PS's counterpart uses `AtomicUsize` because it's shared across
// partition threads. Polling backoff is 50 ms, negligible vs the
// seconds-to-minutes wallclock of EC convert / recovery.
//
// Why not the per-extent locks alone? `ec_conversion_locks`
// only serialises requests for the SAME extent_id; `recovery_inflight`
// only blocks duplicate requests for the SAME extent_id. Both
// allow unbounded cross-extent fanout: a single manager
// `recovery_dispatch_loop` tick that finds 8 different extents needing
// recovery on the same node spawns 8 detached `run_recovery_task`
// tasks, each holding ~`payload × 2` memory through the fetch + write
// phases. ConcurrencyController caps that to N concurrent across all
// extents.

pub struct ConcurrencyController {
    ec_convert_max: usize,
    recovery_max: usize,
    ec_convert_inflight: std::cell::Cell<usize>,
    recovery_inflight: std::cell::Cell<usize>,
}

impl ConcurrencyController {
    pub fn new(ec_convert_max: usize, recovery_max: usize) -> Rc<Self> {
        Rc::new(Self {
            ec_convert_max: ec_convert_max.max(1),
            recovery_max: recovery_max.max(1),
            ec_convert_inflight: std::cell::Cell::new(0),
            recovery_inflight: std::cell::Cell::new(0),
        })
    }

    /// Acquire an EC-convert permit. Polls with 50 ms backoff while at
    /// cap — negligible relative to EC convert wallclock (seconds-minutes).
    pub async fn acquire_ec_convert(self: &Rc<Self>) -> EcConvertPermit {
        loop {
            let cur = self.ec_convert_inflight.get();
            if cur < self.ec_convert_max {
                self.ec_convert_inflight.set(cur + 1);
                return EcConvertPermit { ctrl: self.clone() };
            }
            compio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Acquire a recovery permit. Same pattern as `acquire_ec_convert`.
    pub async fn acquire_recovery(self: &Rc<Self>) -> RecoveryPermit {
        loop {
            let cur = self.recovery_inflight.get();
            if cur < self.recovery_max {
                self.recovery_inflight.set(cur + 1);
                return RecoveryPermit { ctrl: self.clone() };
            }
            compio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Snapshot of current EC-convert inflight count. Test-only.
    #[cfg(test)]
    pub fn ec_convert_inflight(&self) -> usize {
        self.ec_convert_inflight.get()
    }

    /// Snapshot of current recovery inflight count. Test-only.
    #[cfg(test)]
    pub fn recovery_inflight_count(&self) -> usize {
        self.recovery_inflight.get()
    }
}

pub struct EcConvertPermit {
    ctrl: Rc<ConcurrencyController>,
}

impl Drop for EcConvertPermit {
    fn drop(&mut self) {
        let cur = self.ctrl.ec_convert_inflight.get();
        self.ctrl.ec_convert_inflight.set(cur.saturating_sub(1));
    }
}

pub struct RecoveryPermit {
    ctrl: Rc<ConcurrencyController>,
}

impl Drop for RecoveryPermit {
    fn drop(&mut self) {
        let cur = self.ctrl.recovery_inflight.get();
        self.ctrl.recovery_inflight.set(cur.saturating_sub(1));
    }
}

// The env-reading helpers `ec_convert_parallelism()` and
// `recovery_parallelism()` were removed — values now live on
// `ExtentNodeConfig.ec_convert_parallelism` / `.recovery_parallelism`,
// set by the extent-node binary's CLI parser. The clamp([1, 16]) moved
// to the `with_*` builder methods on ExtentNodeConfig.

// ─── ExtentNode ───────────────────────────────────────────────────────────────

pub struct ExtentNode {
    extents: Rc<DashMap<u64, Rc<ExtentEntry>>>,
    /// bounded cache of open fds for SEALED extents (open/active
    /// extents are pinned). Shares the `extents` map (for eviction). See `FdLru`.
    fd_lru: Rc<FdLru>,
    /// All disks attached to this node, keyed by disk_id.
    disks: Rc<HashMap<u64, Rc<DiskFS>>>,
    /// Observability batch 1: this shard's /metrics gauge slot (also
    /// registered in the process-global `EN_SHARD_GAUGES`). Refreshed
    /// from `handle_df` (manager-driven ~2 s cadence).
    metrics_gauges: std::sync::Arc<EnShardGauges>,
    manager_endpoint: Option<String>,
    /// ConnPool for manager RPC calls (nodes_info, extent_info, etc.)
    manager_pool: Rc<crate::ConnPool>,
    /// per-downstream-addr chain forwarder queues. The conn loop
    /// enqueues forwards UNBOUNDED (non-blocking — a blocking submit here
    /// stalled the whole handle_connection loop under 8M backlog, v1 bug);
    /// each addr's forwarder task drains sequentially, preserving
    /// per-extent forward order (global per-addr order ⊇ per-extent order),
    /// and hands the response receiver back through the job's oneshot so
    /// downstream RTTs still overlap.
    chain_fwd: Rc<RefCell<HashMap<String, futures::channel::mpsc::Sender<ChainFwdJob>>>>,
    /// Shared across every shard of this process — see `DoneQueues`.
    done: DoneQueues,
    recovery_inflight: Rc<DashMap<u64, crate::extent_rpc::RecoveryTask>>,
    /// Finished EC conversions awaiting pickup by the next `df` (mirrors
    /// `recovery_done` — the manager learns completion from the heartbeat,
    /// not from the dispatch RPC's return).
    /// EC conversions running in the background on this shard. The manager
    /// re-dispatches from its durable marker every ~5 s, so without this guard
    /// each tick would spawn another converter for the same extent.
    ec_convert_inflight: Rc<DashMap<u64, ()>>,
    /// Why this extent's last EC conversion attempt failed.
    ///
    /// The coordinator's failures used to exist only in its own log: the manager
    /// re-dispatches every few seconds, this node answers CODE_OK because
    /// CODE_OK means ACCEPTED, and the ledger marker therefore sat at attempts=0
    /// with no error while a conversion failed forever and the extent's GC
    /// stayed blocked behind it. Carrying the reason back on the next accept
    /// costs nothing on the wire — the response already has a message field.
    ec_last_error: Rc<DashMap<u64, String>>,
    /// Live progress for the extent-scoped ops this node EXECUTES, keyed by
    /// extent_id → `(kind, done, total)`.
    ///
    /// EC conversion and recovery run in the background for minutes to hours
    /// and used to report only their terminal outcome, so `ops status` showed
    /// a bare RUNNING for the whole time. `handle_df` samples this map, which
    /// is why it is a plain overwrite per update and not a queue: a lost
    /// sample costs nothing, the next `df` carries a fresher one.
    op_progress: Rc<DashMap<u64, (u8, u64, u64)>>,
    /// Highest conversion-attempt nonce this shard has staged a stripe for,
    /// per extent. Nonces are etcd revisions and therefore MONOTONIC, so a
    /// `WriteShard` carrying a LOWER one provably belongs to an attempt that
    /// has since been superseded — a coordinator whose marker was released but
    /// which is still streaming stripes into the same staging file its
    /// successor is now filling. Without the ordering there is no way to tell
    /// which of two writers is the live one.
    ///
    /// In-memory only: it guards a race between two live coordinators, which
    /// is bounded by this process's lifetime. A restart forgets it, and the
    /// stripe-0 truncate plus the `owner_epoch` fence remain the defence there.
    ec_stage_nonce: Rc<DashMap<u64, EcStageMark>>,
    /// How many staging claims this node has accepted, over all extents.
    ///
    /// A reconcile snapshots it BEFORE it asks the manager what to hold, and
    /// compares the answer against it: a mark stamped after the snapshot
    /// belongs to an attempt the manager had not yet been asked about. Counted
    /// node-wide rather than per-extent because the snapshot has to cover
    /// extents that carried no mark at all when it was taken.
    ec_stage_tick: Rc<Cell<u64>>,
    /// WAL for small must_sync writes. None if WAL is disabled.
    /// Wrapped in Rc<RefCell<>> for interior mutability on single-threaded compio.
    /// shard_idx / shard_count for per-shard extent ownership.
    /// Default is (0, 1) = legacy single-thread mode.
    shard_idx: u32,
    shard_count: u32,
    /// local sibling shard addresses for cross-shard control RPC
    /// forwarding. `sibling_addrs[i]` is the address of shard `i` on this
    /// host. Empty in single-thread mode.
    sibling_addrs: Rc<Vec<String>>,
    /// per-extent serialisation lock for `handle_convert_to_ec`. The
    /// manager-side `ec_conversion_inflight` set is purely in-memory and is
    /// lost on leader failover; a deposed leader's in-flight EC conversion
    /// is invisible to the new leader, whose 5 s `ec_conversion_dispatch_loop`
    /// can fire a SECOND `EXT_MSG_CONVERT_TO_EC` before the first completes.
    /// The idempotency guard fires post-hoc (eversion bump is the last
    /// step of the 2PC), so during the deposed leader's mid-`spawn_blocking`
    /// `ec_encode` + `write_shard_local` window the guard does not yet
    /// trigger and two encodes race on the same `.ec.dat` staging file —
    /// producing corrupted shards or sub-shard-of-sub-shard payloads
    /// (the same corruption shape). This lock serialises both dispatches
    /// on the coordinator: the second one waits, then re-runs the idempotency
    /// guard under the lock and exits as a no-op once the first finishes.
    /// Pattern mirrors `client.rs::stream_init_locks`.
    ///
    /// **Extended to a general "mutating-op lock"**. In addition
    /// to EC convert, `handle_re_avali` now acquires this lock (its
    /// write path — fetch_full_extent_from_sources + truncate + pwrite —
    /// races with both convert and delete the same way). `handle_delete_extent`
    /// `try_lock`s this and refuses with CODE_PRECONDITION if held,
    /// closing the convert↔delete and re_avali↔delete races that the
    /// `recovery_inflight` check alone didn't cover. The lock entry lives
    /// for the lifetime of the node — bounded by the number of distinct
    /// extents that ever ran a mutating op on this shard. Use
    /// `get_or_create_extent_op_lock` to look up / create.
    ec_conversion_locks: Rc<RefCell<HashMap<u64, Rc<futures::lock::Mutex<()>>>>>,
    /// P0-B: per-extent `.meta`-write critical section. EVERY `.meta` writer
    /// (save_meta from seal / EC commit / recovery / re_avali, and the
    /// owner_epoch fence persist) acquires this so writers serialise and
    /// each reads the LIVE atomics just before writing — closing the
    /// last-writer-wins clobber where a fence persist with a stale snapshot
    /// would overwrite a concurrent seal's `.meta`. DISTINCT from
    /// `ec_conversion_locks` (the op-lock): EC commit / re_avali hold the
    /// op-lock and call `save_meta`, so reusing it would self-deadlock.
    meta_locks: Rc<RefCell<HashMap<u64, Rc<futures::lock::Mutex<()>>>>>,
    /// per-shard `ConcurrencyController` hosting both
    /// the EC-convert and recovery concurrency caps. Renamed from the
    /// two separate `ExtentNodeGate` fields (`ec_convert_gate` +
    /// `recovery_gate`) to mirror PS's `ConcurrencyController` shape —
    /// one struct, two counters. RAM cap, not rate cap.
    concurrency_ctrl: Rc<ConcurrencyController>,
    /// (was env `AUTUMN_EXTENT_INFLIGHT_CAP`): per-conn
    /// FuturesUnordered cap. Read once at construction from
    /// `ExtentNodeConfig.inflight_cap`; immutable after.
    inflight_cap: usize,
    /// M1b: this EN's own identity, echoed in `handle_df`
    /// (default/empty when `--advertise` was not passed).
    registration: Rc<NodeRegistration>,
}

impl Clone for ExtentNode {
    fn clone(&self) -> Self {
        Self {
            extents: self.extents.clone(),
            fd_lru: self.fd_lru.clone(),
            disks: self.disks.clone(),
            metrics_gauges: self.metrics_gauges.clone(),
            manager_endpoint: self.manager_endpoint.clone(),
            manager_pool: self.manager_pool.clone(),
            chain_fwd: self.chain_fwd.clone(),
            done: self.done.clone(),
            recovery_inflight: self.recovery_inflight.clone(),
            ec_convert_inflight: self.ec_convert_inflight.clone(),
            ec_last_error: self.ec_last_error.clone(),
            op_progress: self.op_progress.clone(),
            ec_stage_nonce: self.ec_stage_nonce.clone(),
            ec_stage_tick: self.ec_stage_tick.clone(),
            shard_idx: self.shard_idx,
            shard_count: self.shard_count,
            sibling_addrs: self.sibling_addrs.clone(),
            ec_conversion_locks: self.ec_conversion_locks.clone(),
            meta_locks: self.meta_locks.clone(),
            concurrency_ctrl: self.concurrency_ctrl.clone(),
            inflight_cap: self.inflight_cap,
            registration: self.registration.clone(),
        }
    }
}

/// Helper: one-shot RPC call (connect → send → recv → close).
async fn rpc_oneshot(addr: std::net::SocketAddr, msg_type: u8, payload: Bytes) -> Result<Bytes> {
    let conn = autumn_transport::current_or_init().connect(addr).await?;
    if let Some(s) = conn.as_tcp() {
        s.set_nodelay(true)?;
    }
    let (mut reader, mut writer) = conn.into_split();

    let req_id = 1u32;
    let frame = Frame::request(req_id, msg_type, payload);
    let BufResult(result, _) = writer.write_all(frame.encode()).await;
    result?;

    let mut decoder = FrameDecoder::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let BufResult(result, buf_back) = reader.read(buf).await;
        buf = buf_back;
        let n = result?;
        if n == 0 {
            return Err(anyhow::anyhow!("connection closed before response"));
        }
        decoder.feed(&buf[..n]);
        if let Some(resp) = decoder.try_decode().map_err(|e| anyhow::anyhow!("{e}"))? {
            if resp.is_error() {
                let (code, msg) = autumn_rpc::RpcError::decode_status(&resp.payload);
                return Err(anyhow::anyhow!("rpc error ({:?}): {}", code, msg));
            }
            return Ok(resp.payload);
        }
    }
}

/// Send an EC 2PC participant control RPC (`WriteShard` / `CommitEcShard`) to a
/// target node, extract its response code via `decode_code`, and map a transport
/// error or a non-`CODE_OK` reply into a uniform `Internal` error. `label`
/// describes the op (e.g. `"WriteShard to <addr> shard <i> @ <off>"`) so both
/// participant-RPC sites in `handle_convert_to_ec` emit identical messages from
/// one place.
async fn ec_2pc_participant_rpc(
    sock: std::net::SocketAddr,
    msg_type: u8,
    payload: Bytes,
    label: &str,
    decode_code: impl FnOnce(Bytes) -> std::result::Result<u8, (StatusCode, String)>,
) -> std::result::Result<(), (StatusCode, String)> {
    match rpc_oneshot(sock, msg_type, payload).await {
        Ok(resp_bytes) => {
            let code = decode_code(resp_bytes)?;
            if code != CODE_OK {
                return Err((
                    StatusCode::Internal,
                    // The NUMBER as well as the name — an unnamed code would
                    // otherwise render as one generic word and hide which
                    // guard refused.
                    format!("{label}: code={code} ({})", code_description(code)),
                ));
            }
            Ok(())
        }
        Err(e) => Err((StatusCode::Internal, format!("{label}: {e}"))),
    }
}

/// Build a generic `CodeResp { code, message }` reply — the extent node's most
/// common response shape (re_avali / delete / recovery / convert status returns).
/// Centralises the `Ok(rkyv_encode(&CodeResp { .. }))` boilerplate so each
/// handler guard / success arm is a single readable line. Mirrors the manager's
/// `code_resp` helper.
fn code_resp(code: u8, message: String) -> HandlerResult {
    Ok(rkyv_encode(&CodeResp { code, message }))
}

/// What a dispatched recovery should do about an extent this node ALREADY holds
/// a copy of. "Cannot tell" must never be collapsed into "incomplete", because
/// the action for incomplete is destructive; and "incomplete" itself splits by
/// how much of the local copy may be reset, which is not the same question as
/// how incomplete it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalCopyVerdict {
    /// Verified complete against the manager's authoritative view — adopt it and
    /// re-report done (the completion report was lost, not the data).
    Complete,
    /// The authoritative view was obtained and the local copy falls short of it.
    /// Safe to discard: the manager only dispatches recovery to a node it does
    /// NOT count as a member, so an incomplete copy there is referenced by no
    /// VP, no SST, and no checkpoint.
    Incomplete,
    /// An EC'd extent whose local SHARD is missing, short, or stale.
    ///
    /// Distinct from `Incomplete` because the REMEDY differs, not the
    /// diagnosis. `Incomplete`'s caller resets the whole local copy — drops the
    /// entry, then `remove_extent_files` unlinks every file for the extent.
    /// This verdict resets nothing and just dispatches.
    ///
    /// The reason is that the reset buys nothing here, not that it would be
    /// unsafe: the rebuild opens its destination with `truncate(true)`, so the
    /// only thing the reset could remove is a file the rebuild is about to
    /// overwrite anyway. Skipping it keeps the fall-through free of any
    /// destructive step, which is worth having on a path whose whole purpose is
    /// to stop a wedge — a reset that goes wrong turns a stuck extent into a
    /// lost one.
    ///
    /// (Two hazards that WOULD make the reset unsafe cannot occur at this
    /// moment, and the reasoning should not be borrowed from the replication
    /// path where they can. A conversion racing the rebuild is impossible
    /// because the manager drains the marker for an already-converted extent
    /// rather than converting it again (`recovery.rs`, the `ex.ec_converted ||
    /// sealed_length == 0` guard); and this node cannot hold a valid shard at
    /// another index because recovery candidates are filtered to NON-members
    /// (`dispatch_recovery_task`).)
    ///
    /// The stray 0-byte `.dat` that `ensure_extent` leaves is reclaimed by the
    /// reconcile sweep once the shard is in hand.
    IncompleteEcShard,
    /// The manager was unreachable, or the extent's shape (still open /
    /// quarantined) makes the comparison meaningless. Refuse and retry later —
    /// never destroy a copy whose completeness is unknown.
    Unknown,
}

/// Build an `AppendResp` rejection frame: a guard rejected the append, so no
/// bytes were written and `offset`/`end` are 0. Every append-protocol guard in
/// `handle_append` (quarantine / eversion / seal / owner-epoch fence / commit)
/// returns one of these — centralising the `offset:0, end:0` boilerplate keeps
/// each guard a single readable line. `code` is the rejection reason
/// (`CODE_PRECONDITION` / `CODE_LOCKED_BY_OTHER`).
fn append_reject(code: u8) -> HandlerResult {
    Ok(AppendResp {
        code,
        offset: 0,
        end: 0,
    }
    .encode())
}

/// Positional write (pwrite) at reserved offset — safe for concurrent
/// non-overlapping offsets (each caller uses fetch_add to reserve).
///
/// takes `Rc<CompioFile>` by value. The caller cloned the `Rc`
/// off `entry.file_rc()` before invoking us, so the `RefCell` borrow
/// is already released and the underlying fd is kept alive by THIS
/// future's captured `Rc` for the duration of the `.await`. If
/// another task calls `entry.replace_file(new_file)` while this
/// pwrite is in flight, the old fd survives until our `Rc` drops.
///
/// compio's `impl AsyncWriteAt for &File` (compio_fs/file.rs:250)
/// uses `SharedFd` interior mutability, so `&*rc` (giving
/// `&CompioFile`) suffices for the `write_all_at` syscall.
async fn file_pwrite(
    file: Rc<CompioFile>,
    offset: u64,
    data: impl compio::buf::IoBuf,
) -> Result<()> {
    let mut f: &CompioFile = &file;
    let BufResult(result, _) = f.write_all_at(data, offset).await;
    result.map_err(|e| anyhow::anyhow!(e))
}

/// Positional read (pread). See `file_pwrite` for the
/// `Rc<CompioFile>` rationale.
async fn file_pread(file: Rc<CompioFile>, offset: u64, len: usize) -> Result<Vec<u8>> {
    let f: &CompioFile = &file;
    let buf = vec![0u8; len];
    let BufResult(result, buf) = f.read_exact_at(buf, offset).await;
    result.map_err(|e| anyhow::anyhow!(e))?;
    Ok(buf)
}

/// Per-call chunk size for local-disk pread/pwrite. macOS caps a single
/// pread/pwrite at INT_MAX (~2 GiB) and Linux at 0x7ffff000 — without
/// chunking, sealed extents > 2 GiB EINVAL on the very first syscall.
/// Mirrors `read_chunk_bytes` in `client.rs` for the StreamClient
/// RPC path; this constant covers the local-file path on the extent node.
const FILE_IO_CHUNK_BYTES: usize = 256 * 1024 * 1024;

static EC_ENCODE_STRIPE_BYTES_CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

/// Set the EC-convert encode/transfer stripe size in bytes
/// (`autumn-extent-node --ec-stripe-bytes N`). First-call-wins (OnceLock) — the
/// binary applies it at startup, before any EC convert runs. Clamped to
/// `[1 MiB, 1 GiB]`: below 1 MiB the per-stripe RPC + `sync_data` overhead
/// dominates (more, smaller WriteShards); above 1 GiB the peak RAM
/// `(K+M) × stripe` balloons and a single stripe approaches the frame
/// `payload_len: u32` ceiling. Returns false if already initialised. Precedence:
/// this flag > `AUTUMN_EXTENT_EC_STRIPE_BYTES` env (test override) > 64 MiB default.
pub fn set_ec_encode_stripe_bytes(n: usize) -> bool {
    EC_ENCODE_STRIPE_BYTES_CELL
        .set(n.clamp(1024 * 1024, 1024 * 1024 * 1024))
        .is_ok()
}

/// EC convert encode/transfer stripe size. The chunked EC convert holds at most
/// `(K+M)` stripes resident at once (the K data sub-ranges read off the source
/// extent + the M parity sub-ranges computed from them), so peak RAM is
/// `(K+M) × stripe` — independent of extent size (was ~2× the whole extent for
/// the pre-chunking whole-extent encode). 64 MiB default keeps the peak ~256 MiB
/// at K+M=4 while bounding the per-shard `sync_data` count; it is also well under
/// the frame `payload_len: u32` ceiling so each stripe's `WriteShard` is a single
/// in-frame RPC even for >4 GiB shards. Tunable via `--ec-stripe-bytes`
/// (`set_ec_encode_stripe_bytes`); `AUTUMN_EXTENT_EC_STRIPE_BYTES` is a test
/// override to exercise multi-stripe without writing multi-GiB extents.
fn ec_encode_stripe_bytes() -> usize {
    *EC_ENCODE_STRIPE_BYTES_CELL.get_or_init(|| {
        std::env::var("AUTUMN_EXTENT_EC_STRIPE_BYTES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .map(|n| n.clamp(1024 * 1024, 1024 * 1024 * 1024))
            .unwrap_or(64 * 1024 * 1024)
    })
}

/// Chunked pread for full-extent reads (recovery / EC convert / etc.).
/// Single-shot reads <= FILE_IO_CHUNK_BYTES bypass the loop.
async fn file_pread_chunked(file: Rc<CompioFile>, offset: u64, len: usize) -> Result<Vec<u8>> {
    if len <= FILE_IO_CHUNK_BYTES {
        return file_pread(file, offset, len).await;
    }
    let mut buf = Vec::with_capacity(len);
    let mut cur = offset;
    let stop = offset + len as u64;
    while cur < stop {
        let want = ((stop - cur) as usize).min(FILE_IO_CHUNK_BYTES);
        let part = file_pread(file.clone(), cur, want).await?;
        let got = part.len() as u64;
        buf.extend_from_slice(&part);
        if got == 0 {
            break;
        }
        cur += got;
    }
    Ok(buf)
}

/// Chunked pwrite for full-extent writes (recovery payload restore, EC shard
/// staging, etc.). Takes `Bytes` so callers that already hold a `Bytes` (e.g.,
/// EC shard from `Vec<u8>` via zero-copy `Bytes::from`) avoid an event-loop
/// memcpy. Chunks are split via `Bytes::split_to` (O(1) Arc reslice) and
/// passed straight to `file_pwrite` which accepts `impl IoBuf`; this removed
/// the per-chunk `chunk.to_vec()` round-trip that previously forced
/// `O(extent)` event-loop memcpy on every full-extent write.
async fn file_pwrite_chunked(file: Rc<CompioFile>, offset: u64, data: Bytes) -> Result<()> {
    if data.len() <= FILE_IO_CHUNK_BYTES {
        return file_pwrite(file, offset, data).await;
    }
    let mut bytes = data;
    let mut cur = offset;
    while !bytes.is_empty() {
        let take = FILE_IO_CHUNK_BYTES.min(bytes.len());
        let chunk = bytes.split_to(take);
        let chunk_len = chunk.len() as u64;
        file_pwrite(file.clone(), cur, chunk).await?;
        cur += chunk_len;
    }
    Ok(())
}

// ───── R4 step 4.2 — inline SQ/CQ pipeline helpers ──────────────────────────

/// Outcome of the persistent read future used by `handle_connection`.
///
/// The future OWNS both the `OwnedReadHalf` and the read buffer across
/// iterations — when it completes, these are returned here and the caller
/// rebuilds a fresh future via `spawn_read` with the same reader and buf.
/// Never dropping the read future mid-flight is critical: dropping it would
/// cancel the pending io_uring SQE, which compio handles correctly but
/// introduces SQE-resubmit oscillation that regressed perf in earlier
/// attempts.
enum ReadBurst {
    /// A full read arrived. `n` bytes at `buf[..n]` are valid payload.
    Data {
        buf: Vec<u8>,
        n: usize,
        reader: autumn_transport::ReadHalf,
    },
    /// read() returned 0 (peer closed).
    Eof {
        #[allow(dead_code)]
        reader: autumn_transport::ReadHalf,
        #[allow(dead_code)]
        buf: Vec<u8>,
    },
    /// read() errored.
    Err {
        e: std::io::Error,
        #[allow(dead_code)]
        reader: autumn_transport::ReadHalf,
        #[allow(dead_code)]
        buf: Vec<u8>,
    },
}

/// Build a `'static`-lifetime `LocalBoxFuture<ReadBurst>` that reads once
/// into `buf` and returns ownership of both `reader` and `buf`.
fn spawn_read(
    mut reader: autumn_transport::ReadHalf,
    buf: Vec<u8>,
) -> futures::future::LocalBoxFuture<'static, ReadBurst> {
    use compio::io::AsyncRead;
    use futures::FutureExt;
    async move {
        let BufResult(result, buf_back) = reader.read(buf).await;
        match result {
            Ok(0) => ReadBurst::Eof {
                reader,
                buf: buf_back,
            },
            Ok(n) => ReadBurst::Data {
                buf: buf_back,
                n,
                reader,
            },
            Err(e) => ReadBurst::Err {
                e,
                reader,
                buf: buf_back,
            },
        }
    }
    .boxed_local()
}

// The env-reading helper `extent_inflight_cap()` was removed —
// value now lives on `ExtentNodeConfig.inflight_cap`, set by the
// extent-node binary's CLI parser. Default 64 matches the client-side
// pipelining depth where extent_bench peaks.

/// Decode all complete frames from `decoder`, group consecutive same-extent
/// APPEND/READ frames, and push one I/O future per group onto `inflight`.
/// Control RPCs are dispatched inline (as an `async move` future) and also
/// pushed onto `inflight`.
///
/// Back-pressure: if `inflight.len()` reaches `cap` mid-push, we await one
/// completion before pushing more. Completions drained during back-pressure
/// go into `tx_bufs` and are flushed by the caller after this returns.
/// one queued chain forward — `parts` is the full MSG_APPEND_CHAIN
/// request (prefix + AppendReq header + payload, all Bytes refs); the
/// forwarder sends `Ok(receiver)` (or the submit error) back through
/// `rx_back` so the chain future can await the downstream ack itself.
/// downstream failure classification — semantic codes pass through
/// to the writer (fencing/alloc reactions), transport faults stay generic.
enum ChainFail {
    Code(u8),
    Msg(String),
}

struct ChainFwdJob {
    parts: Vec<Bytes>,
    rx_back: futures::channel::oneshot::Sender<Result<futures::channel::oneshot::Receiver<Frame>>>,
}

impl ExtentNode {
    /// enqueue a chain forward to `addr` — non-blocking, in caller
    /// order. Lazily spawns the per-addr forwarder task on this shard's
    /// runtime (lives for the process; one per peer shard addr, ~dozens).
    fn chain_forward_enqueue(
        &self,
        addr: &str,
        parts: Vec<Bytes>,
    ) -> futures::channel::oneshot::Receiver<Result<futures::channel::oneshot::Receiver<Frame>>>
    {
        let (rx_back_tx, rx_back) = futures::channel::oneshot::channel();
        let job = ChainFwdJob {
            parts,
            rx_back: rx_back_tx,
        };
        let mut map = self.chain_fwd.borrow_mut();
        let tx = map.entry(addr.to_string()).or_insert_with(|| {
            // coco P2: BOUNDED — each job pins a large payload Bytes;
            // a slow/dead downstream must backpressure (fail fast) instead
            // of accumulating unbounded memory. 32 jobs ≈ 256MB of 8M refs.
            let (tx, mut rx) = futures::channel::mpsc::channel::<ChainFwdJob>(32);
            let pool = self.manager_pool.clone();
            let addr = addr.to_string();
            compio::runtime::spawn(async move {
                use futures::StreamExt;
                while let Some(job) = rx.next().await {
                    let res = pool.send_vectored(&addr, MSG_APPEND_CHAIN, job.parts).await;
                    let _ = job.rx_back.send(res);
                }
            })
            .detach();
            tx
        });
        if let Err(e) = tx.try_send(job) {
            // Queue full / forwarder gone: surface as an immediate chain
            // failure (client retries; no conn-loop stall, no unbounded pin).
            let job = e.into_inner();
            let _ = job
                .rx_back
                .send(Err(anyhow::anyhow!("chain forward queue saturated")));
        }
        rx_back
    }
}

/// bound for awaiting the downstream hop's ack in a chained append.
/// Generous — covers a tail hop's pwrite + coalesced fsync under load; the
/// writer-side append timeout (scaled by chain depth) is the outer bound.
const CHAIN_FORWARD_TIMEOUT: Duration = Duration::from_secs(30);

async fn process_frames_backpressured(
    node: &ExtentNode,
    decoder: &mut FrameDecoder,
    inflight: &mut futures::stream::FuturesUnordered<
        std::pin::Pin<Box<dyn std::future::Future<Output = Vec<Bytes>>>>,
    >,
    tx_bufs: &mut Vec<Bytes>,
    cap: usize,
) -> Result<()> {
    use futures::stream::StreamExt as _;
    // Pull all complete frames out of the decoder.
    let mut frames: Vec<Frame> = Vec::new();
    loop {
        match decoder.try_decode().map_err(|e| anyhow::anyhow!(e))? {
            Some(frame) if frame.req_id != 0 => frames.push(frame),
            Some(_) => continue, // req_id=0: fire-and-forget, no response needed
            None => break,
        }
    }

    // Back-pressure: before each push, if we're at/above cap, await one
    // completion and accumulate its bytes into tx_bufs so the caller's
    // final write_vectored_all includes them.
    macro_rules! backpressure {
        () => {
            while inflight.len() >= cap {
                if let Some(done) = inflight.next().await {
                    tx_bufs.extend(done);
                } else {
                    break;
                }
            }
        };
    }

    let mut i = 0;
    while i < frames.len() {
        let msg_type = frames[i].msg_type;

        if msg_type == MSG_APPEND {
            // Group consecutive same-extent APPEND frames.
            let first_req = match AppendReq::decode(frames[i].payload.clone()) {
                Ok(r) => r,
                Err(e) => {
                    let req_id = frames[i].req_id;
                    let bytes = err_bytes(req_id, MSG_APPEND, StatusCode::InvalidArgument, e);
                    inflight.push(Box::pin(async move { vec![bytes] }));
                    i += 1;
                    continue;
                }
            };
            let anchor_extent = first_req.extent_id;
            let mut slots: Vec<AppendSlot> = Vec::with_capacity(8);
            slots.push(AppendSlot {
                req: first_req,
                req_id: frames[i].req_id,
            });
            i += 1;
            while i < frames.len() && frames[i].msg_type == MSG_APPEND {
                match AppendReq::decode(frames[i].payload.clone()) {
                    Ok(r) if r.extent_id == anchor_extent => {
                        slots.push(AppendSlot {
                            req: r,
                            req_id: frames[i].req_id,
                        });
                        i += 1;
                    }
                    Ok(_) => break,
                    Err(e) => {
                        let req_id = frames[i].req_id;
                        let bytes = err_bytes(req_id, MSG_APPEND, StatusCode::InvalidArgument, e);
                        inflight.push(Box::pin(async move { vec![bytes] }));
                        i += 1;
                    }
                }
            }

            // Resolve extent; on error, synthesise one error frame per slot.
            let extent = match node.get_extent(anchor_extent).await {
                Ok(e) => e,
                Err((code, msg)) => {
                    let p = autumn_rpc::RpcError::encode_status(code, &msg);
                    let bytes_list: Vec<Bytes> = slots
                        .iter()
                        .map(|s| Frame::error(s.req_id, MSG_APPEND, p.clone()).encode())
                        .collect();
                    inflight.push(Box::pin(async move { bytes_list }));
                    continue;
                }
            };

            // Back-pressure BEFORE advancing ACL state (extent.len
            // reservation) so a pushed batch never stalls waiting to drain.
            backpressure!();

            // [owner-model] route the append burst through this extent's owner
            // task (the sole serial writer). Each slot becomes an Append message;
            // the per-slot response frames come back on oneshots and are gathered
            // (in slot order) into the same Vec<Bytes> the FU expected before.
            let items: Vec<(u32, futures::channel::oneshot::Receiver<Bytes>)> = slots
                .into_iter()
                .map(|slot| {
                    let req_id = slot.req_id;
                    let (tx, rx) = futures::channel::oneshot::channel::<Bytes>();
                    send_to_owner(
                        &node,
                        &extent,
                        ExtentMsg::Append {
                            req: slot.req,
                            req_id,
                            resp: tx,
                        },
                    );
                    (req_id, rx)
                })
                .collect();
            inflight.push(Box::pin(async move {
                let mut out = Vec::with_capacity(items.len());
                for (req_id, rx) in items {
                    match rx.await {
                        Ok(frame) => out.push(frame),
                        Err(_) => out.push(err_bytes(
                            req_id,
                            MSG_APPEND,
                            StatusCode::Internal,
                            "owner dropped append response",
                        )),
                    }
                }
                out
            }));
        } else if msg_type == MSG_APPEND_CHAIN {
            // chained append. One frame, one future (no same-extent
            // grouping: chained payloads are >= 64 KiB, pwritev coalescing
            // buys nothing). ORDERING INVARIANT: the downstream forward is
            // SUBMITTED here, synchronously, in frame-arrival order — this
            // socket's arrival order is the writer's lease order, and the
            // downstream RpcClient's single writer_task preserves submit
            // order, so every hop sees per-extent appends in lease order
            // (same argument as the client's star fanout).
            let req_id = frames[i].req_id;
            i += 1;
            let (chain, append_bytes) = match decode_chain_prefix(frames[i - 1].payload.clone()) {
                Ok(v) => v,
                Err(e) => {
                    let bytes = err_bytes(req_id, MSG_APPEND_CHAIN, StatusCode::InvalidArgument, e);
                    inflight.push(Box::pin(async move { vec![bytes] }));
                    continue;
                }
            };
            let req = match AppendReq::decode(append_bytes.clone()) {
                Ok(r) => r,
                Err(e) => {
                    let bytes = err_bytes(req_id, MSG_APPEND_CHAIN, StatusCode::InvalidArgument, e);
                    inflight.push(Box::pin(async move { vec![bytes] }));
                    continue;
                }
            };
            let extent = match node.get_extent(req.extent_id).await {
                Ok(e) => e,
                Err((code, msg)) => {
                    let bytes = err_bytes(req_id, MSG_APPEND_CHAIN, code, &msg);
                    inflight.push(Box::pin(async move { vec![bytes] }));
                    continue;
                }
            };
            backpressure!();

            // Forward submit FIRST (synchronous, ordering) — then build the
            // local append (which reserves extent.len synchronously too).
            let fwd_rx = if chain.is_empty() {
                None
            } else {
                let prefix = encode_chain_prefix(&chain[1..]);
                // Non-blocking ordered enqueue — see chain_forward_enqueue.
                Some(node.chain_forward_enqueue(&chain[0], vec![prefix, append_bytes.clone()]))
            };
            // [owner-model] the LOCAL append goes through the owner (serial
            // writer); the downstream forward stays here on the conn task and is
            // joined below, exactly as before.
            let (local_tx, local_rx) = futures::channel::oneshot::channel::<Bytes>();
            send_to_owner(
                &node,
                &extent,
                ExtentMsg::Append {
                    req,
                    req_id,
                    resp: local_tx,
                },
            );
            inflight.push(Box::pin(async move {
                let local_bytes = vec![match local_rx.await {
                    Ok(f) => f,
                    Err(_) => err_bytes(
                        req_id,
                        MSG_APPEND,
                        StatusCode::Internal,
                        "owner dropped append response",
                    ),
                }];
                let fwd_ok: Result<(), ChainFail> = match fwd_rx {
                    None => Ok(()),
                    Some(rx_back) => match rx_back.await {
                        Err(_) => Err(ChainFail::Msg("chain forwarder gone".to_string())),
                        Ok(Err(e)) => Err(ChainFail::Msg(format!("chain forward submit: {e}"))),
                        Ok(Ok(rx)) => {
                            // Bound the downstream wait — a wedged hop must not
                            // pin this future forever (client times out anyway).
                            match compio::time::timeout(CHAIN_FORWARD_TIMEOUT, rx).await {
                                Err(_) => Err(ChainFail::Msg("chain forward timeout".to_string())),
                                Ok(Err(_)) => {
                                    Err(ChainFail::Msg("chain forward conn closed".to_string()))
                                }
                                Ok(Ok(frame)) => {
                                    if frame.is_error() {
                                        Err(ChainFail::Msg("chain downstream error".to_string()))
                                    } else {
                                        match AppendResp::decode(frame.payload.clone()) {
                                            Ok(r) if r.code == CODE_OK => Ok(()),
                                            // coco P1: PRESERVE the downstream
                                            // code (LockedByOther must reach the
                                            // writer for self-eviction; NotFound
                                            // drives alloc-new-extent) — surfaced
                                            // below as a normal AppendResp with the
                                            // downstream code, not a generic error.
                                            Ok(r) => Err(ChainFail::Code(r.code)),
                                            Err(e) => Err(ChainFail::Msg(format!(
                                                "chain downstream decode: {e}"
                                            ))),
                                        }
                                    }
                                }
                            }
                        }
                    },
                };
                match fwd_ok {
                    // Both local + downstream OK → the local response frame
                    // (success or its own error) is the chain's answer.
                    Ok(()) => local_bytes,
                    // Downstream returned a SEMANTIC code: pass it through as
                    // a normal AppendResp so apply_completion's
                    // LockedByOther / NotFound arms fire (coco P1).
                    Err(ChainFail::Code(code)) => {
                        let resp = AppendResp {
                            code,
                            offset: 0,
                            end: 0,
                        };
                        vec![Frame::response(req_id, MSG_APPEND_CHAIN, resp.encode()).encode()]
                    }
                    Err(ChainFail::Msg(msg)) => {
                        vec![err_bytes(
                            req_id,
                            MSG_APPEND_CHAIN,
                            StatusCode::Unavailable,
                            &format!("chain append failed downstream: {msg}"),
                        )]
                    }
                }
            }));
        } else if msg_type == MSG_READ_BYTES {
            let first_req = match ReadBytesReq::decode(frames[i].payload.clone()) {
                Ok(r) => r,
                Err(e) => {
                    let req_id = frames[i].req_id;
                    let bytes = err_bytes(req_id, MSG_READ_BYTES, StatusCode::InvalidArgument, e);
                    inflight.push(Box::pin(async move { vec![bytes] }));
                    i += 1;
                    continue;
                }
            };
            let anchor_extent = first_req.extent_id;
            // Group by the PAYLOAD FILE, not just the extent. One batch
            // resolves ONE fd and serves every slot from it, so two requests
            // naming different files must never share a batch — that would
            // answer one of them out of the other's file.
            let anchor_payload = first_req.payload_ref();
            let mut slots: Vec<ReadSlot> = Vec::with_capacity(8);
            slots.push(ReadSlot {
                req: first_req,
                req_id: frames[i].req_id,
            });
            i += 1;
            while i < frames.len() && frames[i].msg_type == MSG_READ_BYTES {
                match ReadBytesReq::decode(frames[i].payload.clone()) {
                    Ok(r) if r.extent_id == anchor_extent && r.payload_ref() == anchor_payload => {
                        slots.push(ReadSlot {
                            req: r,
                            req_id: frames[i].req_id,
                        });
                        i += 1;
                    }
                    Ok(_) => break,
                    Err(e) => {
                        let req_id = frames[i].req_id;
                        let bytes =
                            err_bytes(req_id, MSG_READ_BYTES, StatusCode::InvalidArgument, e);
                        inflight.push(Box::pin(async move { vec![bytes] }));
                        i += 1;
                    }
                }
            }

            let extent = match node.get_extent(anchor_extent).await {
                Ok(e) => e,
                Err((code, msg)) => {
                    let p = autumn_rpc::RpcError::encode_status(code, &msg);
                    let bytes_list: Vec<Bytes> = slots
                        .iter()
                        .map(|s| Frame::error(s.req_id, MSG_READ_BYTES, p.clone()).encode())
                        .collect();
                    inflight.push(Box::pin(async move { bytes_list }));
                    continue;
                }
            };
            backpressure!();
            // Every slot in this batch names the SAME file (the grouping rule
            // above), so one refusal answers all of them.
            if !extent.holds_payload(anchor_payload) {
                let bytes_list: Vec<Bytes> = slots
                    .iter()
                    .map(|s| read_refusal_resp(s.req_id, false, ReadRefusal::PayloadNotHere))
                    .collect();
                inflight.push(Box::pin(async move { bytes_list }));
                continue;
            }
            // resolve (re-open if evicted) the fd here, where we
            // have the node; pin it into the read future.
            let file_rc = match node.payload_file(&extent, anchor_payload).await {
                Ok(f) => f,
                Err(msg) => {
                    let p = autumn_rpc::RpcError::encode_status(StatusCode::Internal, &msg);
                    let bytes_list: Vec<Bytes> = slots
                        .iter()
                        .map(|s| Frame::error(s.req_id, MSG_READ_BYTES, p.clone()).encode())
                        .collect();
                    inflight.push(Box::pin(async move { bytes_list }));
                    continue;
                }
            };
            // Resolve the content checksums beside the fd — same reason, same
            // place: every slot in this batch names the same file.
            let content_ck = node.cached_content_checksums(anchor_extent, &extent).await;
            inflight.push(build_read_future(extent, content_ck, file_rc, slots, false));
        } else if msg_type == MSG_READ_BYTES_BULK {
            // zero-copy read grouping — mirrors MSG_READ_BYTES but every
            // response (ok + error) is bulk-shaped (`bulk_read_head` + value Bytes)
            // so the PS's call_into_pooled always parses a bulk_ctrl.
            let first_req = match ReadBytesReq::decode(frames[i].payload.clone()) {
                Ok(r) => r,
                Err(_) => {
                    let bytes =
                        bulk_read_head(frames[i].req_id, CODE_ERROR, "malformed ReadBytesReq", 0);
                    inflight.push(Box::pin(async move { vec![bytes] }));
                    i += 1;
                    continue;
                }
            };
            let anchor_extent = first_req.extent_id;
            // Same file-identity grouping as the non-bulk path above.
            let anchor_payload = first_req.payload_ref();
            let mut slots: Vec<ReadSlot> = Vec::with_capacity(8);
            slots.push(ReadSlot {
                req: first_req,
                req_id: frames[i].req_id,
            });
            i += 1;
            while i < frames.len() && frames[i].msg_type == MSG_READ_BYTES_BULK {
                match ReadBytesReq::decode(frames[i].payload.clone()) {
                    Ok(r) if r.extent_id == anchor_extent && r.payload_ref() == anchor_payload => {
                        slots.push(ReadSlot {
                            req: r,
                            req_id: frames[i].req_id,
                        });
                        i += 1;
                    }
                    Ok(_) => break,
                    Err(_) => {
                        let bytes = bulk_read_head(
                            frames[i].req_id,
                            CODE_ERROR,
                            "malformed ReadBytesReq",
                            0,
                        );
                        inflight.push(Box::pin(async move { vec![bytes] }));
                        i += 1;
                    }
                }
            }
            let extent = match node.get_extent(anchor_extent).await {
                Ok(e) => e,
                Err((_code, _msg)) => {
                    let bytes_list: Vec<Bytes> = slots
                        .iter()
                        .map(|s| bulk_read_head(s.req_id, CODE_ERROR, "extent unavailable", 0))
                        .collect();
                    inflight.push(Box::pin(async move { bytes_list }));
                    continue;
                }
            };
            backpressure!();
            // Same file-identity rule as the non-bulk path: one refusal covers
            // the batch, because the batch is one file.
            if !extent.holds_payload(anchor_payload) {
                let bytes_list: Vec<Bytes> = slots
                    .iter()
                    .map(|s| read_refusal_resp(s.req_id, true, ReadRefusal::PayloadNotHere))
                    .collect();
                inflight.push(Box::pin(async move { bytes_list }));
                continue;
            }
            let file_rc = match node.payload_file(&extent, anchor_payload).await {
                Ok(f) => f,
                Err(_msg) => {
                    let bytes_list: Vec<Bytes> = slots
                        .iter()
                        .map(|s| bulk_read_head(s.req_id, CODE_ERROR, "extent unavailable", 0))
                        .collect();
                    inflight.push(Box::pin(async move { bytes_list }));
                    continue;
                }
            };
            // Resolve the content checksums beside the fd — same reason, same
            // place: every slot in this batch names the same file.
            let content_ck = node.cached_content_checksums(anchor_extent, &extent).await;
            inflight.push(build_read_future(extent, content_ck, file_rc, slots, true));
        } else {
            // Control RPC — no hot-path grouping. Build a future that
            // dispatches and encodes one response frame.
            backpressure!();
            let req_id = frames[i].req_id;
            let payload = frames[i].payload.clone();
            let node_clone = node.clone();
            inflight.push(Box::pin(async move {
                let resp_frame = match node_clone.dispatch(msg_type, payload).await {
                    Ok(p) => Frame::response(req_id, msg_type, p),
                    Err((code, message)) => {
                        let p = autumn_rpc::RpcError::encode_status(code, &message);
                        Frame::error(req_id, msg_type, p)
                    }
                };
                vec![resp_frame.encode()]
            }));
            i += 1;
        }
    }
    Ok(())
}

/// One append request slot routed through `handle_connection`.
struct AppendSlot {
    req: AppendReq,
    req_id: u32,
}

struct ReadSlot {
    req: ReadBytesReq,
    req_id: u32,
}

// ==================================================================
// [owner-model] per-extent owner task — interface types (WIP, step 1).
//
// One owner task per RESIDENT extent owns the fd + ALL mutations. Every mutation
// is an `ExtentMsg`, processed ONE AT A TIME, so mutations are STRUCTURALLY
// serialized. This lets a later step delete: the op-lock (get_or_create_extent_op_lock),
// the meta_write_lock, the synchronous offset reservation, the fsync coalescer,
// and the per-await seal/fence/fd-evict re-checks (their whole job is catching
// concurrent mutators, which structural serialization makes impossible).
//
// SHAPE (as built): the owner drains its mailbox in bursts, splits each drain
// into homogeneous runs (one owner_epoch + contiguous commits — see
// `owner_loop`), and runs each through `append_burst_frames` (the renamed
// `build_append_future`). That body kept its two-phase form (synchronous prologue
// reserves `extent.len`, the returned future does pwrite + inline `sync_data`);
// under the sole owner the reservation is a harmless vestige (one writer never
// races itself), left to flatten in a later cleanup. Durability is one
// `sync_data` per burst (`pending_fsync` before, `last_synced` after) — the fsync
// coalescer + its loop are DELETED. Reads stay lock-free (clone
// `ExtentEntry.file`); the owner clones the fd per burst and drops it on
// exit → still gates `fd_evictable`.
// ==================================================================

/// A message to a per-extent owner task. The `resp` oneshot carries the handler
/// result back to the ps-conn task, which batches the encoded frame into its
/// existing vectored write (response batching stays on the conn).
pub(crate) enum ExtentMsg {
    /// Append under the commit==extent.len ordering + owner_epoch fence.
    Append {
        req: AppendReq,
        req_id: u32,
        /// The owner sends back the ENCODED response frame (Frame::response or
        /// Frame::error), matching the old build_append_future output, so the
        /// ps-conn task batches it into its existing vectored write unchanged.
        resp: futures::channel::oneshot::Sender<Bytes>,
    },
    // Later steps add: Truncate, Seal, FencePersist (the interleaving set that
    // must move together with Append), plus the SHORT commit messages of the
    // staged long ops (ReAvaliCommit / ConvertCommit / EcShardCommit / Delete /
    // CopyMetaApply). SyncedLen / CommitLength are NOT messages — they stay
    // lock-free atomic reads of `last_synced` (fable P1-3).
}

/// Owner mailbox state (under `ExtentEntry.owner`'s RefCell). `queue` is the
/// pending messages; `running` is true iff an owner task is live. The owner
/// never PARKS (it processes a burst synchronously, and messages that arrive
/// after it drains simply respawn it), so unlike the coalescer it needs no wake
/// channel — a plain `running` flag suffices. Lost-wake-free by the same
/// single-thread argument: enqueue (`send_to_owner`) and the owner's exit
/// re-check both mutate this RefCell with NO await inside the borrow, so a
/// message never lands in a queue the exiting owner won't drain.
#[derive(Default)]
pub(crate) struct OwnerMailbox {
    queue: Vec<ExtentMsg>,
    running: bool,
}

/// Encode a single error-response frame (`Frame::error` + `encode_status`).
fn err_bytes(req_id: u32, msg_type: u8, code: StatusCode, msg: &str) -> Bytes {
    Frame::error(
        req_id,
        msg_type,
        autumn_rpc::RpcError::encode_status(code, msg),
    )
    .encode()
}

/// Build a `CODE_EVERSION_MISMATCH` read response for one slot — the bulk head
/// or a full `ReadBytesResp` frame, matching the connection's `bulk` mode.
/// Typed CODE (not a frame-level error) so the client's
/// `read_bytes_from_extent` retry self-heals (invalidate cache + refetch)
/// instead of seeing a generic transport error.
fn read_refusal_resp(req_id: u32, bulk: bool, why: ReadRefusal) -> Bytes {
    if bulk {
        bulk_read_head(req_id, why.code(), why.message(), 0)
    } else {
        Frame::response(
            req_id,
            MSG_READ_BYTES,
            ReadBytesResp {
                code: why.code(),
                end: 0,
                payload: Bytes::new(),
            }
            .encode(),
        )
        .encode()
    }
}

/// A read's serving plan, computed from the extent's local state — the ONE
/// place holding the eversion gate + logical-length semantics for reads.
/// Shared by the batched hot path (`build_read_future`) and the single-op
/// `handle_read_bytes` so the two can never drift (the eversion-gate and P0-C
/// fixes each previously had to land twice). Pure computation, no awaits.
///
/// Returns `None` when `req.eversion` is stale → the caller answers
/// CODE_EVERSION_MISMATCH. `req.eversion < ev` is enforced
/// UNCONDITIONALLY (no `> 0` skip) — a stale-cached eversion=0, populated
/// while the extent was open, must be rejected after split/EC bump it, and it
/// must be a typed RESPONSE (not a frame error) so the client's retry loop
/// invalidates its cache and re-routes through the EC path.
///
/// Returns `Some((end, read_offset, read_size))` otherwise. P0-C: a
/// sealed-EMPTY extent (sealed=true, sealed_length=0) has logical length 0 —
/// never serve residual/ghost `.dat` bytes past its (0) seal point (also
/// stops recovery's `length=0` read from copying ghost bytes to a fresh
/// replica). Normal sealed / EC extents keep `extent.len`: a replicated
/// sealed extent has len==sealed_length, and EC shard reads carry explicit
/// per-shard lengths, so clamping to the logical `sealed_length` there would
/// be wrong.
fn read_plan(extent: &ExtentEntry, req: &ReadBytesReq) -> Result<(u64, u64, u64), ReadRefusal> {
    // The request NAMES a payload file; serving a different one would hand back
    // shard bytes as a whole value (or the reverse), which is the corruption
    // the location field exists to prevent. Refuse with its own code so the
    // client refreshes the layout instead of retrying the same wrong file.
    if !extent.holds_payload(req.payload_ref()) {
        return Err(ReadRefusal::PayloadNotHere);
    }
    let ev = extent.eversion.load(Ordering::SeqCst);
    if req.eversion < ev {
        return Err(ReadRefusal::EversionStale);
    }
    // The length bound belongs to the FILE being read. A shard is
    // `sealed_length / K` bytes while `.dat` — which may still be sitting
    // beside it, awaiting cleanup — holds the whole extent, so taking the
    // extent-level length here would let a to-end shard read ask for several
    // times the shard's size.
    let total_len = match req.payload_ref().location {
        PayloadLocation::InShardFile => extent
            .shard_files
            .borrow()
            .get(&req.shard_index)
            .copied()
            .unwrap_or(0),
        PayloadLocation::InDat => {
            if extent.sealed.load(Ordering::SeqCst)
                && extent.sealed_length.load(Ordering::SeqCst) == 0
            {
                0
            } else {
                extent.len.load(Ordering::SeqCst)
            }
        }
    };
    let read_offset = req.offset;
    let read_size = if req.length == 0 {
        total_len.saturating_sub(read_offset)
    } else {
        req.length.min(total_len.saturating_sub(read_offset))
    };
    Ok((total_len, read_offset, read_size))
}

/// Why a read cannot be served from this node as requested. Both are typed
/// RESPONSE codes rather than frame errors, so the client's retry can tell them
/// apart and self-heal: refetch the extent's metadata and try again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadRefusal {
    /// The caller's cached `eversion` is behind this node's.
    EversionStale,
    /// This node does not hold the payload file the request named.
    PayloadNotHere,
}

impl ReadRefusal {
    fn code(self) -> u8 {
        match self {
            Self::EversionStale => CODE_EVERSION_MISMATCH,
            Self::PayloadNotHere => CODE_PAYLOAD_NOT_HERE,
        }
    }

    fn message(self) -> &'static str {
        match self {
            Self::EversionStale => "eversion mismatch",
            Self::PayloadNotHere => "payload file not held here",
        }
    }
}

/// The authoritative committed length reported to a length probe — shared by
/// `handle_commit_length` and `handle_probe_extent` (their length-source
/// semantics must stay identical so the `info` CLI display matches what a
/// real owner sees).
///
/// P0-C (coco review #3 issue 3): a sealed extent's authoritative length is
/// `sealed_length` — INCLUDING 0 for a sealed-EMPTY extent. Decide "is
/// sealed" via the explicit flag OR a positive length, not `sealed_length >
/// 0` (which fell through to `last_synced`, and after a restart
/// `Coalescer::new(len)` seeds last_synced from the file size, so
/// residual/ghost `.dat` bytes would be reported as a non-zero commit
/// boundary for a sealed-empty extent). Open extents report the fsync
/// high-water.
fn committed_length_value(entry: &ExtentEntry) -> u64 {
    let sealed_len = entry.sealed_length.load(Ordering::SeqCst);
    if entry.sealed.load(Ordering::SeqCst) || sealed_len > 0 {
        sealed_len
    } else {
        entry.coalescer.last_synced.load(Ordering::SeqCst)
    }
}

/// Reject an entire same-extent append batch with `code` (no bytes written →
/// offset/end = 0): encode the `AppendResp` once and fan it out to every slot's
/// `req_id`. Every fail-closed guard in `build_append_future` (quarantine /
/// eversion / seal / owner-epoch fence / commit-reconcile) returns one of these
/// — the batched analog of `append_reject`. Pure response construction; touches
/// none of the seal/fence/commit logic.
fn batch_append_reject(
    slots: Vec<AppendSlot>,
    code: u8,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<Bytes>>>> {
    let resp_payload = AppendResp {
        code,
        offset: 0,
        end: 0,
    }
    .encode();
    let out: Vec<Bytes> = slots
        .into_iter()
        .map(|s| Frame::response(s.req_id, MSG_APPEND, resp_payload.clone()).encode())
        .collect();
    Box::pin(async move { out })
}

/// `write_vectored_all_at`, segmented at `IOV_MAX`.
///
/// A kernel rejects a `writev`/`pwritev` by iovec COUNT (EINVAL on Linux,
/// EMSGSIZE on macOS) no matter how few bytes it carries, and the count here
/// scales with the number of APPENDS coalesced for one extent, not their size —
/// one iovec per request. The same limit already killed the RPC writer once at
/// 1075 iovecs for 57 KB, which is why `autumn_rpc::client::write_vectored_chunked`
/// exists; this is the file-offset twin of it.
///
/// Getting it wrong here is worse than a failed write: a count rejection arrives
/// as an ERROR, not a short write, and the caller answers an error from this path
/// by marking the DISK bad — so an iovec overflow would be diagnosed as failing
/// hardware and pull the node into recovery.
///
/// Each chunk keeps the `_all` semantics the ENOSPC fix depends on (loop until
/// every byte lands or a real error surfaces), and the offset advances by exactly
/// the bytes the previous chunks carried, so the concatenation on disk is
/// identical to what one giant pwritev would have written.
async fn write_vectored_all_at_chunked(
    // `mut` on the BINDING, not the file: the trait method takes `&mut self` and
    // the impl is on `&File`, so what has to be mutable is the reference.
    mut f: &CompioFile,
    bufs: Vec<Bytes>,
    at: u64,
) -> std::io::Result<()> {
    const IOV_MAX: usize = 1024;
    if bufs.len() <= IOV_MAX {
        let BufResult(r, _) = f.write_vectored_all_at(bufs, at).await;
        return r;
    }
    let mut rest = bufs;
    let mut off = at;
    while !rest.is_empty() {
        let tail = rest.split_off(rest.len().min(IOV_MAX));
        let n: u64 = rest.iter().map(|b| b.len() as u64).sum();
        let BufResult(r, _) = f.write_vectored_all_at(rest, off).await;
        r?;
        off += n;
        rest = tail;
    }
    Ok(())
}

/// Build the async future that performs ACL + pwritev for a same-extent
/// APPEND batch. ACL early rejections resolve the future as an immediate
/// pre-encoded Vec<Bytes> with no I/O.
///
/// The returned future is polled inside `handle_connection`'s
/// FuturesUnordered — multiple appends to DIFFERENT extents run concurrently;
/// appends to the SAME extent are all pushed to FU in order, and since the
/// ACL synchronously reserves `extent.len`, overlapping same-extent futures
/// compute non-overlapping `file_start`s.
///
/// NOTE: reserves `extent.len` synchronously BEFORE returning the I/O future
/// so a subsequent submit to the same extent sees the advanced len. The
/// returned future then calls `write_vectored_at` with pwritev at the
/// reserved offset.
// ==================================================================
// [owner-model] per-extent owner task (step 2: append path).
// ==================================================================

/// Enqueue `msg` to `extent`'s owner mailbox, lazily spawning the owner task if
/// none is running. Mirrors `register_sync_waiter`'s spawn coordination. The
/// `borrow_mut` block has NO await, so on the single-threaded runtime the
/// enqueue+spawn decision is atomic vs `owner_loop`'s exit re-check — a message
/// never lands in a queue an exiting owner will not drain.
fn send_to_owner(node: &ExtentNode, extent: &std::rc::Rc<ExtentEntry>, msg: ExtentMsg) {
    let spawn = {
        let mut mb = extent.owner.borrow_mut();
        mb.queue.push(msg);
        if mb.running {
            false
        } else {
            mb.running = true;
            true
        }
    };
    if spawn {
        let node2 = node.clone();
        let ext2 = std::rc::Rc::clone(extent);
        en_spawn_failstop("en_owner".to_string(), owner_loop(node2, ext2));
    }
}

/// Per-extent owner task. Drains the mailbox in bursts and processes each burst
/// as ONE group commit via `append_burst_frames` (the relocated
/// `build_append_future` body). Exits when the queue drains — dropping its
/// `Rc<ExtentEntry>` so a sealed/idle extent is never pinned past eviction;
/// `send_to_owner` respawns on the next message. Lost-wake-free exit: see
/// `OwnerMailbox`.
async fn owner_loop(node: ExtentNode, extent: std::rc::Rc<ExtentEntry>) {
    loop {
        let burst: Vec<ExtentMsg> = {
            let mut mb = extent.owner.borrow_mut();
            std::mem::take(&mut mb.queue)
        };
        if burst.is_empty() {
            // Exit re-check under ONE borrow (no await): if the queue is still
            // empty, clear `running` and exit; else a message arrived between the
            // take above and here — loop and drain it.
            let exit = {
                let mut mb = extent.owner.borrow_mut();
                if mb.queue.is_empty() {
                    mb.running = false;
                    true
                } else {
                    false
                }
            };
            if exit {
                return;
            }
            continue;
        }
        // Split the drained burst into HOMOGENEOUS runs, then group-commit each.
        //
        // The mailbox is a NEW cross-connection aggregation point: appends from a
        // fenced zombie writer (owner_epoch E) and the post-takeover owner (E+1),
        // or a retry replayed on a fresh connection, can land in ONE drain. But
        // `append_burst_frames` validates owner_epoch + commit from its FIRST slot
        // only — a rule that was sound when a "batch" was, by construction,
        // consecutive frames from ONE connection (one writer → one epoch,
        // contiguous commits). Feeding heterogeneous slots to one burst would let
        // a non-first slot bypass the fence (a zombie's E-epoch write ACKed inside
        // an E+1 burst) or wrongly reject a whole burst on a leading stale slot.
        // So start a fresh run whenever the owner_epoch changes OR commit
        // contiguity breaks (commit != prev.commit + prev.payload.len()); each
        // run's first slot then gets the FULL prologue check against the live
        // extent.len / owner_epoch. Single-writer pipelining is single-epoch +
        // contiguous ⇒ ONE run ⇒ zero hot-path cost. This splitter is now the home
        // of the "first slot governs the batch" invariant the prologue relies on.
        let mut runs: Vec<(
            Vec<AppendSlot>,
            Vec<futures::channel::oneshot::Sender<Bytes>>,
        )> = Vec::new();
        let mut prev_epoch_end: Option<(i64, u64)> = None; // (owner_epoch, commit + payload.len())
        for msg in burst {
            match msg {
                ExtentMsg::Append { req, req_id, resp } => {
                    let contiguous = matches!(
                        prev_epoch_end,
                        Some((pe, pend)) if pe == req.owner_epoch && pend == req.commit
                    );
                    if !contiguous {
                        runs.push((Vec::new(), Vec::new()));
                    }
                    prev_epoch_end = Some((req.owner_epoch, req.commit + req.payload.len() as u64));
                    let run = runs.last_mut().expect("a run was just pushed");
                    run.0.push(AppendSlot { req, req_id });
                    run.1.push(resp);
                }
            }
        }
        // Each run is ONE group commit (prologue reserves extent.len
        // synchronously, then the returned future does pwrite+fsync). Runs run
        // SEQUENTIALLY so a later run's fence/commit prologue observes the prior
        // run's durable extent.len / owner_epoch.
        for (slots, resps) in runs {
            let frames = append_burst_frames(node.clone(), std::rc::Rc::clone(&extent), slots)
                .await
                .await;
            // Per-slot frames in slot order == resps order.
            for (frame, resp) in frames.into_iter().zip(resps.into_iter()) {
                let _ = resp.send(frame);
            }
        }
    }
}

async fn append_burst_frames(
    node: ExtentNode,
    extent: std::rc::Rc<ExtentEntry>,
    slots: Vec<AppendSlot>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<Bytes>>>> {
    if slots.is_empty() {
        return Box::pin(async move { Vec::new() });
    }

    // 0. META-FAILCLOSED: a quarantined extent (`.meta` was present-but-corrupt
    // at load — see load_extents) must refuse ALL appends. Defaulting it to
    // open/owner_epoch=0 would let a stale lower-epoch writer bypass the fence
    // and ghost-append. Refuse until manager recovery rebuilds it.
    if extent.corrupt_meta.load(Ordering::SeqCst) {
        return batch_append_reject(slots, CODE_PRECONDITION);
    }

    // 1. Eversion refresh: if ANY req.eversion > local, refresh from manager.
    let local_eversion = extent.eversion.load(Ordering::SeqCst);
    let needs_refresh = slots.iter().any(|s| s.req.eversion > local_eversion);
    if needs_refresh {
        let extent_id = slots[0].req.extent_id;
        match node.extent_info_from_manager(extent_id).await {
            Ok(Some(ex)) => {
                // durable seal — fsync the data file when the
                // refresh promotes 0 → sealed_length so the on-disk
                // prefix matches the manager's view.
                // P0-A: if the seal can't be made durable, do NOT proceed with
                // the append — surface Unavailable so the client retries (the
                // disk is now offline → recovery re-replicates this extent).
                if let Err(e) = node
                    .apply_extent_meta_durable(extent_id, &extent, &ex)
                    .await
                {
                    let msg = format!("seal not durable for extent {extent_id}: {e}");
                    let out: Vec<Bytes> = slots
                        .into_iter()
                        .map(|s| err_bytes(s.req_id, MSG_APPEND, StatusCode::Unavailable, &msg))
                        .collect();
                    return Box::pin(async move { out });
                }
            }
            Ok(None) | Err(_) => {
                let msg = format!(
                    "cannot verify extent {} version: manager unreachable",
                    extent_id
                );
                let out: Vec<Bytes> = slots
                    .into_iter()
                    .map(|s| err_bytes(s.req_id, MSG_APPEND, StatusCode::Unavailable, &msg))
                    .collect();
                return Box::pin(async move { out });
            }
        }
    }

    // 2. Sealed / eversion check using CURRENT local atomics.
    let local_eversion = extent.eversion.load(Ordering::SeqCst);
    // P0-C: the explicit `sealed` flag is the authoritative signal — it catches
    // a sealed-EMPTY extent (sealed=true, sealed_length=0, avali possibly 0)
    // that the length/avali derivation would have treated as open. The
    // length/avali clauses stay as defence-in-depth.
    let sealed = extent.sealed.load(Ordering::SeqCst)
        || extent.sealed_length.load(Ordering::SeqCst) > 0
        || extent.avali.load(Ordering::SeqCst) > 0;
    if sealed || slots.iter().any(|s| local_eversion > s.req.eversion) {
        return batch_append_reject(slots, CODE_PRECONDITION);
    }

    // 3. OwnerEpoch fencing: the first slot's owner_epoch governs the batch. This
    // is SOUND ONLY because `owner_loop`'s burst-splitter guarantees every slot
    // here shares one owner_epoch and contiguous commits (a heterogeneous burst
    // is split into homogeneous runs before reaching this prologue) — do not feed
    // this fn a batch spanning multiple writers/epochs.
    let first = &slots[0].req;
    let owner_epoch = extent.owner_epoch.load(Ordering::SeqCst);
    if first.owner_epoch < owner_epoch {
        return batch_append_reject(slots, CODE_LOCKED_BY_OTHER);
    }
    // 3b. P0-B durable fence. Two coupled guarantees:
    //   (i) raise the in-memory bar SYNCHRONOUSLY (monotonic fetch_max) so a
    //       stale lower owner is rejected immediately — even while the new
    //       fence is still being persisted (closes the window where the old
    //       owner could slip a write through during the persist await);
    //   (ii) the fence must be DURABLE on disk (durable_owner_epoch >= R)
    //        BEFORE we ACK any data under it — else a crash after the ACK but
    //        before the persist lets the stale lower owner re-pass the on-disk
    //        fence on restart (split-brain / acked-data overwrite).
    // `ensure_fence_durable` fast-paths to one atomic load when already durable
    // (the steady state), and only locks+persists when a higher owner_epoch first
    // arrives. Fail-closed: a persist failure rejects this append (the writer
    // re-fences); we never ACK a write whose fence isn't durable.
    let fence_extent_id = first.extent_id;
    let first_revision = first.owner_epoch;
    if first_revision > owner_epoch {
        extent
            .owner_epoch
            .fetch_max(first_revision, Ordering::SeqCst);
    }
    if let Err(e) = node
        .ensure_fence_durable(fence_extent_id, &extent, first_revision)
        .await
    {
        node.mark_disk_error_for_extent(fence_extent_id, &e.to_string());
        tracing::error!(
            extent_id = fence_extent_id,
            error = %e,
            "P0-B: durable fence persist failed — rejecting append (fail-closed)"
        );
        return batch_append_reject(slots, CODE_PRECONDITION);
    }
    // P0-B: re-check fencing AFTER the (possibly awaiting) durable step. The
    // `ensure_fence_durable` await is a new yield point; during it a concurrent
    // task may have (a) taken over with a HIGHER owner_epoch, or (b) SEALED
    // this extent (seal/EC/re_avali bumps eversion + sets sealed). Owner
    // takeover → LockedByOther; a fresh seal → CODE_PRECONDITION (mirrors the
    // post-truncate seal recheck) so we never ghost-write past a seal
    // landed during our await. (owner_epoch and sealed are CHECKED
    // SEPARATELY — they are independent concerns: fencing vs seal state.)
    if first_revision < extent.owner_epoch.load(Ordering::SeqCst) {
        return batch_append_reject(slots, CODE_LOCKED_BY_OTHER);
    }
    if extent.sealed.load(Ordering::SeqCst)
        || extent.sealed_length.load(Ordering::SeqCst) > 0
        || extent.avali.load(Ordering::SeqCst) > 0
    {
        return batch_append_reject(slots, CODE_PRECONDITION);
    }

    // 4. Commit reconciliation.
    let mut file_start = extent.len.load(Ordering::SeqCst);
    if file_start < first.commit {
        return batch_append_reject(slots, CODE_PRECONDITION);
    }
    if file_start > first.commit {
        // before truncating, confirm with manager that this
        // extent is NOT sealed. A stale writer's low `header.commit` would
        // otherwise silently shrink a sealed extent.
        let extent_id = slots[0].req.extent_id;
        if let Ok(Some(mgr_info)) = node.extent_info_from_manager(extent_id).await {
            // P0-C: use the explicit `sealed` flag, not `sealed_length > 0`, so a
            // sealed-EMPTY extent (sealed=true, sealed_length=0 — a CoW-shared
            // tail) also refuses the stale writer's truncate+append instead of
            // shrinking + ghost-writing a manager-sealed extent.
            if mgr_info.sealed || mgr_info.sealed_length > 0 {
                // durable seal — fsync the data file as part
                // of accepting the manager's seal point.
                // P0-A: a seal-persist failure here marks the disk offline (for
                // recovery) inside apply_extent_meta_durable; the stale append
                // is still correctly rejected as CODE_PRECONDITION below, so we
                // log + proceed to the rejection rather than change the response.
                if let Err(e) = node
                    .apply_extent_meta_durable(extent_id, &extent, &mgr_info)
                    .await
                {
                    tracing::error!(extent_id, error = %e, "P0-A: seal not durable during commit-reconcile reject (disk offline)");
                }
                return batch_append_reject(slots, CODE_PRECONDITION);
            }
        }
        if let Err(e) = node.truncate_to_commit_ref(&extent, first.commit).await {
            let out: Vec<Bytes> = slots
                .into_iter()
                .map(|s| err_bytes(s.req_id, MSG_APPEND, StatusCode::Internal, &e))
                .collect();
            return Box::pin(async move { out });
        }
        // re-check seal state after the truncate await.
        // apply_extent_meta_durable from a concurrent handle_re_avali or
        // handle_convert_to_ec may have landed a fresh seal during the
        // truncate's I/O. Without this re-check our pwritev would write
        // bytes past the new sealed_length — a data-corruption path
        // surfacing as "logStream value short" or out-of-bounds slice
        // panics on EC reads after the sealed extent is re-read.
        if extent.sealed.load(Ordering::SeqCst)
            || extent.sealed_length.load(Ordering::SeqCst) > 0
            || extent.avali.load(Ordering::SeqCst) > 0
        {
            return batch_append_reject(slots, CODE_PRECONDITION);
        }
        // Also re-check the owner_epoch fence after the truncate await: a
        // concurrent MSG_FENCE_EXTENT (eager takeover fence) may have raised the
        // floor during the truncate's manager RTT + fsync. Without this a stale
        // lower-epoch (zombie) writer that passed the earlier prologue fence check
        // would still ACK here under the old epoch — the exact takeover
        // lost-update the fence op exists to close. Mirrors the P0-B re-check.
        if first_revision < extent.owner_epoch.load(Ordering::SeqCst) {
            return batch_append_reject(slots, CODE_LOCKED_BY_OTHER);
        }
        file_start = extent.len.load(Ordering::SeqCst);
    }

    // 5. Compute per-request offsets + collect payload Bytes for pwritev.
    let n = slots.len();
    let mut offsets: Vec<u64> = Vec::with_capacity(n);
    let mut bufs: Vec<Bytes> = Vec::with_capacity(n);
    let mut req_ids: Vec<u32> = Vec::with_capacity(n);
    let mut cursor = file_start;
    let mut total_payload: usize = 0;
    for slot in &slots {
        offsets.push(cursor);
        cursor += slot.req.payload.len() as u64;
        total_payload += slot.req.payload.len();
        bufs.push(slot.req.payload.clone());
        req_ids.push(slot.req_id);
    }
    let total_end = cursor;
    let extent_id = slots[0].req.extent_id;

    // (coco/subagent P1 — the seal-transition panic): resolve + PIN
    // the fd HERE, in the synchronous prologue, while the seal/fence re-checks
    // above have just established the extent is OPEN — NOT lazily at the returned
    // future's first poll. Between accept and poll the conn task awaits other
    // frames' prologues, so a concurrent seal + LRU-evict could set `file = None`
    // and the old poll-time access would PANIC. Resolving now and MOVING the `Rc`
    // into the future pins the fd for the whole write, and holding the
    // clone makes the extent non-evictable (`fd_evictable` checks
    // `strong_count == 1`) through the pending_fsync store + waiter register. A
    // `None` here means the extent was concurrently sealed → reject the batch
    // with `CODE_PRECONDITION` (the seal re-checks give the same
    // answer), never a panic.
    let file_rc = match extent.resident_file() {
        Some(f) => f,
        None => {
            let p = autumn_rpc::RpcError::encode_status(
                StatusCode::FailedPrecondition,
                "extent sealed (fd evicted) — retry on a fresh tail",
            );
            let bytes_list: Vec<Bytes> = req_ids
                .iter()
                .map(|&id| Frame::error(id, MSG_APPEND, p.clone()).encode())
                .collect();
            return Box::pin(async move { bytes_list });
        }
    };

    // 7. Reserve `extent.len` BEFORE returning the I/O future so overlapping
    //    same-extent futures compute non-overlapping file_starts.
    extent.len.store(total_end, Ordering::SeqCst);
    drop(slots); // release original AppendReq payload handles (already cloned into bufs)

    // 8. Return the I/O future. Must be 'static and own everything.
    let extent_for_io = extent;
    Box::pin(async move {
        let write_t0 = Instant::now();
        // The fd `Rc` (resolved+pinned in the prologue above) is moved into this
        // future; the write runs on it. It survives any concurrent
        // `replace_file` / LRU-evict until our I/O completes.
        let f: &CompioFile = &file_rc;
        // ENOSPC-1 CORRUPTION FIX: `write_vectored_all_at`, NOT the raw
        // `write_vectored_at`. POSIX pwritev on a nearly-full disk writes
        // what fits and returns the SHORT count — only a zero-fit write
        // errors. The raw call's Ok(n < total) was treated as success, so
        // a partial append was fsynced + ACKED, and the unwritten tail of
        // the reserved range read back as zeros (sparse hole): silent
        // corruption of an acked write (caught live by
        // scripts/enospc_chaos.sh — 1 MB values with 3.5 KB intact then
        // zeros). The `_all` form loops until every byte is written or a
        // real error (ENOSPC once nothing fits) surfaces — errors here
        // reject the batch, never ack.
        let wr = write_vectored_all_at_chunked(f, bufs, file_start).await;
        if let Err(e) = wr {
            let msg = e.to_string();
            node.mark_disk_error_for_extent(extent_id, &msg);
            return req_ids
                .into_iter()
                .map(|id| err_bytes(id, MSG_APPEND, StatusCode::Internal, &msg))
                .collect();
        }

        // every append is durable: fsync INLINE. The per-extent owner task
        // serialises appends, so this ONE `sync_data` covers exactly this
        // burst's bytes — the old cross-burst coalescer (register_sync_waiter +
        // coalescer_loop) is unnecessary under the owner and has been removed.
        // pending_fsync advances BEFORE the fsync and last_synced (the
        // durability high-water read by MSG_SYNCED_LENGTH / committed_length and
        // gated by fd_evictable) AFTER, so an evict-check during the fsync window
        // sees pending > last_synced and won't evict. On error: mark the disk +
        // reject the whole burst, never advance last_synced.
        extent_for_io
            .coalescer
            .pending_fsync
            .store(total_end, Ordering::SeqCst);
        if let Err(e) = f.sync_data().await {
            let msg = e.to_string();
            node.mark_disk_error_for_extent(extent_id, &msg);
            return req_ids
                .into_iter()
                .map(|id| err_bytes(id, MSG_APPEND, StatusCode::Internal, &msg))
                .collect();
        }
        extent_for_io
            .coalescer
            .last_synced
            .store(total_end, Ordering::SeqCst);

        let write_elapsed_ns = write_t0.elapsed().as_nanos() as u64;
        EXTENT_APPEND_METRICS.with(|m| {
            m.borrow_mut()
                .record(n as u64, total_payload as u64, write_elapsed_ns);
        });

        // P0-B: the owner_epoch fence is now persisted durably in the
        // prologue (under the per-extent op lock) BEFORE this write future runs,
        // so there is no post-write save_meta here. The data write above is
        // durable via the coalescer; the fence was durable before we got here.

        use bytes::BufMut;
        req_ids
            .into_iter()
            .enumerate()
            .map(|(k, req_id)| {
                let end = if k + 1 < n { offsets[k + 1] } else { total_end };
                let offset = offsets[k];
                // One allocation per response: the AppendResp payload
                // (`[code:1][offset:8 LE][end:8 LE]`, 17 bytes) is written
                // directly into the frame buffer instead of through an
                // intermediate `AppendResp::encode()` Bytes.
                Frame::encode_response_with(req_id, MSG_APPEND, 17, |b| {
                    b.put_u8(CODE_OK);
                    b.put_u64_le(offset);
                    b.put_u64_le(end);
                })
            })
            .collect()
    })
}

/// Build the async future that services a same-extent READ batch. Reads
/// are processed sequentially inside ONE future — each pread is ~1µs and
/// the responses are written back together.
/// build a MSG_READ_BYTES_BULK response head — v28 value-separable
/// frame head `[header][ctrl_len][code+message][crc]` (crc covers header+ctrl;
/// see autumn-rpc frame.rs). The value (if any) is pushed as a SEPARATE
/// `Bytes` right after, so it aliases the pread buffer — no copy, and the raw
/// value is never crc-scanned (transport + storage integrity).
fn bulk_read_head(req_id: u32, code: u8, msg: &str, value_len: usize) -> Bytes {
    autumn_rpc::frame::encode_bulk_response_head(req_id, MSG_READ_BYTES_BULK, code, msg, value_len)
}

/// A sealed extent's `.ck`, resolved at most once per extent.
///
/// Reads are the hot path and the sidecar is immutable for the life of the
/// seal, so loading it per read would add an open+read+close to every one. The
/// three states are distinct on purpose: "not looked at yet" must not be
/// confused with "looked at, and there is nothing" — the second is the common
/// steady state for extents sealed before this existed, and re-probing the
/// filesystem for them on every read is exactly the cost this avoids.
#[derive(Clone)]
enum CachedChecksums {
    NotLoaded,
    Absent,
    Present(Rc<extent_cksum::ExtentChecksums>),
}

/// Check a read's bytes against the extent's content checksums.
///
/// `Some(reason)` means the bytes must NOT be served. Serving them with a
/// success code is the one outcome a caller cannot defend against: it has no
/// way to tell them from correct ones. Refusing routes around the damage
/// through the failover the client already performs for a failed read.
///
/// Only `InDat` payloads are described — a shard file's content is not covered
/// by this sidecar — and only blocks the read fully covers are examined.
fn verify_read_content(
    content_ck: &Option<Rc<extent_cksum::ExtentChecksums>>,
    req: &ReadBytesReq,
    read_offset: u64,
    data: &[u8],
) -> Option<String> {
    let ck = content_ck.as_ref()?;
    if req.payload_ref().location != PayloadLocation::InDat {
        return None;
    }
    if !read_covers_a_full_block(ck, read_offset, data.len() as u64) {
        return None;
    }
    match ck.verify_read(read_offset, data) {
        Ok(_) => None,
        Err(bad) => {
            tracing::error!(
                extent_id = req.extent_id,
                block = bad.block,
                block_offset = bad.offset,
                expected = bad.expected,
                found = bad.found,
                "CONTENT CHECKSUM MISMATCH on a sealed extent — these bytes differ from \
                 what was hashed at seal; refusing to serve them"
            );
            Some(format!(
                "extent {} block {} fails its content checksum",
                req.extent_id, bad.block
            ))
        }
    }
}

/// Does `[offset, offset+len)` fully contain at least one checksummed block?
///
/// Checked BEFORE the sidecar is consulted so a sub-block read — the hot 4 KiB
/// case — pays nothing. Verifying one of those would mean reading and hashing
/// its whole 1 MiB block, a 256x amplification the scrub exists to avoid.
fn read_covers_a_full_block(
    ck: &extent_cksum::ExtentChecksums,
    offset: u64,
    len: u64,
) -> bool {
    let end = offset.saturating_add(len);
    let first = offset.div_ceil(ck.block_bytes);
    let (b_start, b_end) = extent_cksum::block_range(first as usize, ck.block_bytes, ck.sealed_length);
    b_start >= offset && b_end <= end && b_end > b_start
}

fn build_read_future(
    extent: std::rc::Rc<ExtentEntry>,
    // Resolved once per batch at the call site, for the same reason the fd is:
    // this is a free fn with no node handle. `None` = this extent has no
    // content checksums, which is the steady state for anything sealed before
    // they existed.
    content_ck: Option<Rc<extent_cksum::ExtentChecksums>>,
    // the fd is resolved ONCE at the call site (which has the
    // `ExtentNode` + can re-open an evicted sealed extent via `extent_file`) and
    // passed in; this boxed future is a free fn with no node handle, and holding
    // the `Rc` pins the fd for the whole read so a concurrent read's
    // eviction can't yank it mid-scan.
    file_rc: std::rc::Rc<CompioFile>,
    slots: Vec<ReadSlot>,
    bulk: bool,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<Bytes>>>> {
    Box::pin(async move {
        use compio::io::AsyncReadAtExt;

        let mut out: Vec<Bytes> = Vec::with_capacity(slots.len());

        // META-FAILCLOSED (coco P1): the batched read hot path must honour the
        // quarantine too — `handle_read_bytes` alone misses this (production
        // reads go through here). A corrupt-`.meta` extent's length/eversion
        // are untrusted; refuse every slot with CODE_EVERSION_MISMATCH so the
        // client fails over to a healthy replica.
        if extent.corrupt_meta.load(Ordering::SeqCst) {
            for slot in slots {
                out.push(read_refusal_resp(
                    slot.req_id,
                    bulk,
                    ReadRefusal::EversionStale,
                ));
            }
            return out;
        }

        for slot in slots {
            let req = slot.req;
            // Eversion gate + length semantics live in `read_plan` (shared
            // with handle_read_bytes — see its doc for the eversion gate / P0-C).
            let (end, read_offset, read_size) = match read_plan(&extent, &req) {
                Ok(plan) => plan,
                Err(why) => {
                    out.push(read_refusal_resp(slot.req_id, bulk, why));
                    continue;
                }
            };

            // use the caller-resolved, pinned fd for every slot
            // (was `extent.file_rc()` per slot — now the extent may be an
            // evicted sealed one, resolved once at the call site).
            let f: &CompioFile = &file_rc;
            if bulk {
                // BULK-EXACT invariant: a bulk read is always an EXACT-length value
                // read (the VP's `(offset, length)` — callers never pass
                // length=0/to-end). `read_plan` CLAMPS `read_size` to the local
                // bytes (correct for the non-bulk scanner path, whose callers
                // handle short reads), but for bulk a clamp means THIS REPLICA
                // cannot serve the requested range — answering CODE_OK with a
                // silently SHORT payload made every bulk consumer responsible for
                // its own length check (the copy path had one, the bulk proxy
                // path did not → a truncated value could reach a client).
                // Reject here — the producer — with CODE_PRECONDITION; clients
                // treat any non-OK as failover (next replica / copy path).
                // Under all-replica-ACK a committed VP is on every replica, so
                // this fires only for a genuinely stale/over-range request or a
                // diverged replica — never on the healthy path.
                if req.length > 0 && read_size < req.length {
                    tracing::warn!(
                        extent_id = req.extent_id,
                        offset = req.offset,
                        want = req.length,
                        have = read_size,
                        local_len = end,
                        "bulk read cannot serve full range; rejecting (no short CODE_OK)"
                    );
                    out.push(bulk_read_head(
                        slot.req_id,
                        CODE_PRECONDITION,
                        "bulk read cannot serve full range",
                        0,
                    ));
                    continue;
                }
                // pread straight into a registered, pooled, zeroed-once
                // slab — no per-op `vec![0u8; read_size]` alloc, no per-op 8 MiB
                // memset (the pread overwrites it anyway), and the UCX send finds
                // the `ucp_mem_map` registration via the rcache (stable slab
                // address). The value `Bytes` aliases the slab
                // (`Bytes::from_owner`) and returns it to the pool when the
                // response write completes. 2 Bytes on the wire:
                // [header+bulk_ctrl], [value] — no ReadBytesResp/Frame encode copy.
                let pb = autumn_transport::regpool_acquire(read_size as usize);
                let BufResult(result, pb) = f.read_exact_at(pb, read_offset).await;
                match result {
                    Ok(_) => {
                        let value = Bytes::from_owner(pb);
                        if let Some(why) =
                            verify_read_content(&content_ck, &req, read_offset, &value)
                        {
                            out.push(bulk_read_head(slot.req_id, CODE_ERROR, &why, 0));
                        } else {
                            out.push(bulk_read_head(slot.req_id, CODE_OK, "", value.len()));
                            out.push(value);
                        }
                    }
                    Err(_e) => {
                        out.push(bulk_read_head(slot.req_id, CODE_ERROR, "pread failed", 0));
                    }
                }
            } else {
                let buf = vec![0u8; read_size as usize];
                let BufResult(result, buf) = f.read_exact_at(buf, read_offset).await;
                match result {
                    Ok(_) => {
                        // Build the framed ReadBytesResp in ONE allocation. The
                        // old `ReadBytesResp{..}.encode()` then `Frame::encode()`
                        // pair copied the value payload twice (once into the resp
                        // buffer, once into the frame); this copies it once. The
                        // payload layout `[code:1][end:8 LE][value]` matches
                        // `ReadBytesResp::encode`.
                        //
                        // A fully zero-copy 3-segment form (head + value-alias +
                        // CRC trailer, like the bulk path) was measured and REJECTED
                        // for this small-read path: at 4 KiB the saved memcpy is
                        // cheaper than the cost it adds — tripling the iovec count
                        // in `write_vectored_all` plus a per-read trailer alloc —
                        // and regressed batched reads 2-5% (extent_bench d=16/64).
                        // Zero-copy only pays off once the value memcpy dominates
                        // (>= 64 KiB), which is exactly the UCX `bulk` branch above.
                        if let Some(why) =
                            verify_read_content(&content_ck, &req, read_offset, &buf)
                        {
                            out.push(err_bytes(
                                slot.req_id,
                                MSG_READ_BYTES,
                                StatusCode::Internal,
                                &why,
                            ));
                            continue;
                        }
                        use bytes::BufMut;
                        out.push(Frame::encode_response_with(
                            slot.req_id,
                            MSG_READ_BYTES,
                            9 + buf.len(),
                            |b| {
                                b.put_u8(CODE_OK);
                                b.put_u64_le(end);
                                b.extend_from_slice(&buf);
                            },
                        ));
                    }
                    Err(e) => {
                        out.push(err_bytes(
                            slot.req_id,
                            MSG_READ_BYTES,
                            StatusCode::Internal,
                            &e.to_string(),
                        ));
                    }
                }
            }
        }
        out
    })
}

impl ExtentNode {
    /// extent .meta sidecar layout versioning.
    ///
    /// V0 (legacy, no CRC): 40 bytes.
    ///   [magic[8]=b"EXTMETA\0"][extent_id[8]][sealed_length[8]][eversion[8]][owner_epoch[8]]
    ///
    /// V1 (with CRC): 44 bytes, CRC32C trailer over the first 40 bytes.
    ///   [magic[8]=b"EXTMETA\x01"][extent_id[8]][sealed_length[8]][eversion[8]][owner_epoch[8]][crc32c[4]]
    ///
    /// Before the CRC trailer, a flipped bit anywhere in the 40-byte payload (bit rot, undetected
    /// disk error, partial overwrite during a torn write) silently changed the
    /// extent's seal state at restart — recovery would load `sealed_length=0`
    /// for an actually-sealed extent, accept new appends past the old seal
    /// boundary, and corrupt every replica's view of the extent's tail bytes.
    /// V1 wraps a CRC32C trailer; on read, mismatch returns None (treated as
    /// "no meta file" → defaults applied + warning logged), so a corrupted
    /// meta cannot silently drive the extent into an inconsistent state.
    ///
    /// V2 (post-P0-C): 52 bytes. Adds an explicit `sealed` flag + the runtime
    /// `avali` mask so a sealed-EMPTY extent (`sealed = true, sealed_length =
    /// 0`) survives a restart. Pre-V2 the EN derived "is sealed" from
    /// `sealed_length > 0`, so a manager-sealed / CoW-shared empty tail looked
    /// OPEN after a restart and could accept ghost writes (CoW isolation break;
    /// later surfaces as `stale_vp_offset_past_sealed_length sealed_length=0`).
    ///   [magic[8]=b"EXTMETA\x02"][extent_id[8]][sealed_length[8]][eversion[8]]
    ///   [owner_epoch[8]][sealed[1]][pad[3]][avali[4]][crc32c[4]]
    /// CRC32C is computed over the first 48 bytes (everything but the trailer).
    ///
    /// **Migration:** save_meta always writes V2. parse_meta dispatches on
    /// `magic[7]`:
    ///   - 0x00 (V0): legacy 40-byte read, no CRC. `sealed`/`avali` DERIVED
    ///                from `sealed_length > 0`. Next save_meta upgrades to V2.
    ///   - 0x01 (V1): 44-byte read with CRC. `sealed`/`avali` likewise derived.
    ///   - 0x02 (V2): 52-byte read with CRC; `sealed`/`avali` read from disk.
    ///   - other: None (treated as missing/corrupt meta).
    /// Downgrade (V2-file read by a pre-V2 binary): magic mismatch → None →
    /// extent loads as unsealed. Acceptable since rollback is operator-driven
    /// and rare, and the manager re-applies the seal on next contact.
    const META_MAGIC_V0: &'static [u8; 8] = b"EXTMETA\0";
    const META_MAGIC_V1: &'static [u8; 8] = b"EXTMETA\x01";
    const META_MAGIC_V2: &'static [u8; 8] = b"EXTMETA\x02";
    const META_SIZE_V0: usize = 40;
    const META_SIZE_V1: usize = 44;
    const META_SIZE_V2: usize = 52;
    /// Backwards-compat alias for any external code reading the constant.
    /// Equal to the size save_meta currently writes (V2).
    #[allow(dead_code)]
    const META_SIZE: usize = Self::META_SIZE_V2;

    pub async fn new(config: ExtentNodeConfig) -> Result<Self> {
        // Keyed by the data dirs, which every shard of this node shares — see
        // `shared_done_queues`. Captured before `config.disks` is consumed.
        let done = shared_done_queues(&config.disks);
        // Build DiskFS instances for all configured disks.
        let mut disk_map: HashMap<u64, Rc<DiskFS>> = HashMap::new();
        for (dir, maybe_disk_id) in config.disks {
            compio::fs::create_dir_all(&dir).await?;
            let disk = if let Some(disk_id) = maybe_disk_id {
                DiskFS::with_disk_id(dir, disk_id)
            } else {
                DiskFS::open(dir).await?
            };
            let disk_id = disk.disk_id;
            disk_map.insert(disk_id, Rc::new(disk));
        }

        // Observability batch 1: register this instance's gauge slot —
        // the registry holds a Weak, so a dropped/failed-init node is
        // pruned by the renderer (coco P2/P3). Disk set is fixed for the
        // node's lifetime; initial state = all online (matches DiskFS
        // construction).
        let metrics_gauges = {
            let mut disk_slots: Vec<(u64, std::sync::atomic::AtomicU64)> = disk_map
                .keys()
                // DiskHealth::Online as u64 == 0 (coco P3: the old `1`
                // meant online under the bool scheme; under the health
                // scheme it reads as Full until the first sweep).
                .map(|id| (*id, std::sync::atomic::AtomicU64::new(0)))
                .collect();
            disk_slots.sort_unstable_by_key(|(id, _)| *id);
            let g = std::sync::Arc::new(EnShardGauges {
                shard_idx: config.shard_idx,
                extents: std::sync::atomic::AtomicU64::new(0),
                disks: disk_slots,
            });
            EN_SHARD_GAUGES
                .lock()
                .unwrap()
                .push(std::sync::Arc::downgrade(&g));
            g
        };

        // build `extents` first so the fd cache can share it (for
        // eviction reach-through). Both are `Rc`, no cycle (FdLru holds the
        // DashMap, not the node).
        let extents: Rc<DashMap<u64, Rc<ExtentEntry>>> = Rc::new(DashMap::new());
        // coco P1: `fd_cache_cap` is PER SHARD, but `RLIMIT_NOFILE` is
        // PROCESS-wide. Each shard is a separate `ExtentNode`, so N shards would
        // hold N × cap sealed fds; clamp the per-shard cap so the process total
        // stays comfortably under the 65535 RLIMIT_NOFILE limit (reserve headroom
        // for open tails + TCP sockets). Floor 64 (matches `set_fd_cache_cap`).
        let shards = config.shard_count.max(1) as usize;
        let per_shard_cap = fd_cache_cap().min((60_000 / shards).max(64));
        let fd_lru = Rc::new(FdLru::new(per_shard_cap, extents.clone()));
        let node = Self {
            extents,
            fd_lru,
            disks: Rc::new(disk_map),
            metrics_gauges,
            manager_endpoint: config.manager_endpoint,
            manager_pool: Rc::new(crate::ConnPool::new()),
            chain_fwd: Rc::new(RefCell::new(HashMap::new())),
            done,
            recovery_inflight: Rc::new(DashMap::new()),
            ec_convert_inflight: Rc::new(DashMap::new()),
            ec_last_error: Rc::new(DashMap::new()),
            op_progress: Rc::new(DashMap::new()),
            ec_stage_nonce: Rc::new(DashMap::new()),
            ec_stage_tick: Rc::new(Cell::new(0)),
            shard_idx: config.shard_idx,
            shard_count: config.shard_count,
            sibling_addrs: Rc::new(config.sibling_addrs),
            ec_conversion_locks: Rc::new(RefCell::new(HashMap::new())),
            meta_locks: Rc::new(RefCell::new(HashMap::new())),
            // parallelism comes from `ExtentNodeConfig`. CLI flag
            // → builder → here. No env read.
            concurrency_ctrl: ConcurrencyController::new(
                config.ec_convert_parallelism,
                config.recovery_parallelism,
            ),
            inflight_cap: config.inflight_cap.max(1),
            registration: Rc::new(config.registration.unwrap_or_default()),
        };

        // Load existing extents from all disks.
        node.load_extents().await?;

        // reconcile loaded extents against the manager. Any
        // `extent_id` the manager no longer knows about (refs went to
        // 0 while this node was offline, or the manager's in-memory
        // pending-delete queue was lost across a restart, or an EC
        // conversion left a replica behind) is unlinked.
        //
        // spawn as a long-lived background task. After an initial
        // exp-backoff retry that races past manager leader election,
        // it enters a steady-state periodic sweep so the node self-
        // heals on any extent that becomes garbage at runtime —
        // covering MSG_DELETE_EXTENT retry budget exhaustion, EC
        // conversion leftovers, and any other future case where an
        // extent's manager refs hit 0 while the node was momentarily
        // unreachable.
        node.spawn_reconcile_orphans_loop();

        // Per-shard 2 s sweep on THIS shard's runtime, two jobs:
        // (1) OBS-1 gauge refresh — the manager's df probe only reaches
        //     the registered control_address (one shard), so a df-driven
        //     refresh left every other shard's slot permanently stale;
        //     each shard must refresh its own.
        // (2) ENOSPC-1 Full self-heal — a disk marked Full (capacity)
        //     returns Online once free space is back above the
        //     hysteresis floor (5% of total), so GC / operator cleanup
        //     restores allocatability WITHOUT a process restart. Faulted
        //     is never touched here.
        // The task holds only WEAK refs (coco P2): when the node's last
        // clone drops, the upgrades fail, the loop exits, and nothing
        // pins the extent/disk state alive.
        {
            use std::sync::atomic::Ordering::Relaxed;
            let extents = Rc::downgrade(&node.extents);
            let disks = Rc::downgrade(&node.disks);
            let gauges = std::sync::Arc::downgrade(&node.metrics_gauges);
            compio::runtime::spawn(async move {
                loop {
                    let (Some(extents), Some(disks), Some(gauges)) =
                        (extents.upgrade(), disks.upgrade(), gauges.upgrade())
                    else {
                        return; // node dropped — exit, slot gets pruned
                    };
                    gauges.extents.store(extents.len() as u64, Relaxed);
                    for (disk_id, health_slot) in &gauges.disks {
                        if let Some(disk) = disks.get(disk_id) {
                            if disk.health() == DiskHealth::Full {
                                let (total, free) = disk.disk_stats();
                                if total > 0 && free >= total / 20 && disk.try_clear_full() {
                                    tracing::info!(
                                        disk_id,
                                        free,
                                        total,
                                        "disk no longer full — allocation re-enabled"
                                    );
                                }
                            }
                            health_slot.store(disk.health() as u64, Relaxed);
                        }
                    }
                    drop((extents, disks, gauges));
                    compio::time::sleep(Duration::from_secs(2)).await;
                }
            })
            .detach();
        }

        Ok(node)
    }

    /// long-lived periodic orphan reconcile.
    ///
    /// Runs immediately on spawn, then every `SWEEP_INTERVAL`. Errors
    /// (manager not leader during cold boot, transient network blip,
    /// etcd hiccup) are logged at WARN and the loop continues — the
    /// next sweep retries. No separate "startup retry" phase: a cold-
    /// boot race is just a failed first iteration, recovered on the
    /// next tick. Worst-case orphan-cleanup latency on cold boot is
    /// one sweep interval.
    ///
    /// This is the safety net for any case where the manager-push
    /// `MSG_DELETE_EXTENT` path doesn't unlink the local file:
    ///   • `MSG_DELETE_EXTENT` retry budget (60 sweeps × 2 s ≈ 2 min
    ///     on the manager side) exhausted while the node was
    ///     unreachable.
    ///   • Manager restart losing its in-memory
    ///     `pending_extent_deletes` queue between leader hand-offs.
    ///   • Future EC conversion: a replica-shaped extent that gets
    ///     converted to EC leaves the original `.dat` files behind on
    ///     the data nodes; `convert_to_ec` updates manager metadata
    ///     and the periodic reconcile reaps the leftovers without a
    ///     separate cleanup RPC.
    ///   • Any other future code path that drops an extent's refs to
    ///     0 in the manager but doesn't successfully unlink locally.
    ///
    /// Each sweep ships every locally-loaded `extent_id` to the
    /// manager. There's no cheaper "send only suspects" filter the
    /// node can apply — it can't know which ids are garbage without
    /// asking. That's why the cadence is generous (5 min, not 1 min):
    /// for a backstop role, freshness doesn't matter much; an orphan
    /// already escaped the primary push path, a few extra minutes on
    /// disk is harmless. If a node ever scales to 10k+ extents and
    /// the per-sweep payload becomes a concern, switch to chunked
    /// rotation (bounded id batches per sweep, rotating through the
    /// full set over multiple sweeps) — the helper signature is
    /// already shaped for that.
    fn spawn_reconcile_orphans_loop(&self) {
        if self.manager_endpoint.is_none() {
            // Test setups without a manager: nothing to reconcile.
            return;
        }
        let node = self.clone();
        en_spawn_supervised("en_reconcile_orphans", move || {
            let node = node.clone();
            async move {
                const SWEEP_INTERVAL: Duration = Duration::from_secs(300);
                loop {
                    if let Err(e) = node.reconcile_orphans_with_manager().await {
                        tracing::warn!(
                            error = %e,
                            "reconcile failed (will retry next sweep)",
                        );
                    }
                    compio::time::sleep(SWEEP_INTERVAL).await;
                }
            }
        });
    }

    /// best-effort startup orphan reconcile.
    /// If `manager_endpoint` is configured, ship every loaded
    /// `extent_id` to the manager; receive back the subset that's no
    /// longer registered and unlink the corresponding `.dat`/`.meta`.
    /// Skips silently when there's no manager (test setups). Per-disk
    /// errors are logged but don't propagate — partial cleanup is fine,
    /// the retry loop will catch the next iteration.
    async fn reconcile_orphans_with_manager(&self) -> Result<()> {
        let mgr = match &self.manager_endpoint {
            Some(ep) => crate::conn_pool::normalize_endpoint(ep),
            None => return Ok(()),
        };
        // Sampled BEFORE the request goes out, so any staging that lands while
        // the manager is answering is stamped above it and the answer is known
        // not to be about that attempt.
        let staging_tick_at_ask = self.ec_stage_tick.get();
        let extent_ids: Vec<u64> = self
            .extents
            .iter()
            .map(|e| *e.key())
            .filter(|id| self.owns_extent(*id))
            .collect();

        if extent_ids.is_empty() {
            return Ok(());
        }
        let req = manager_rpc::rkyv_encode(&manager_rpc::ReconcileExtentsReq {
            // The EN does not track its own node_id (the manager assigns it at
            // register time); its UUID is the identity it does hold, and the
            // manager resolves the two. Sending an unidentified request is not
            // an option: every verdict in the answer is "what should THIS node
            // hold", so the manager answers nothing without knowing who asked.
            node_id: 0,
            node_uuid: self.registration.node_uuid.clone(),
            shard_idx: self.shard_idx,
            extent_ids: extent_ids.clone(),
        });
        // 10 s — read-only manager call (returns subset of submitted
        // extent ids that are no longer in s.extents). Bounded so a
        // hanging manager doesn't trap the periodic 5-min sweep.
        let resp_data = self
            .manager_pool
            .call_timeout(
                &mgr,
                manager_rpc::MSG_RECONCILE_EXTENTS,
                req,
                Duration::from_secs(10),
            )
            .await
            .map_err(|e| anyhow::anyhow!("reconcile_extents rpc: {e}"))?;
        let resp: manager_rpc::ReconcileExtentsResp = manager_rpc::rkyv_decode(&resp_data)
            .map_err(|e| anyhow::anyhow!("decode reconcile resp: {e}"))?;
        if resp.code != manager_rpc::CODE_OK {
            return Err(anyhow::anyhow!(
                "reconcile_extents non-OK: {}",
                resp.message,
            ));
        }
        self.apply_placements(&resp.placements, staging_tick_at_ask)
            .await;
        if resp.garbage.is_empty() {
            return Ok(());
        }
        tracing::info!(
            local = extent_ids.len(),
            garbage = resp.garbage.len(),
            "startup reconcile: unlinking orphans",
        );
        for eid in &resp.garbage {
            // NEVER unlink an extent this node is actively building or
            // converting. The manager's garbage list now includes extents this
            // node is not a MEMBER of — and a recovery target is by definition
            // not yet a member, so without this guard the sweep would delete a
            // recovery out from under itself. `handle_delete_extent` has always
            // refused for the same reason; this path used to bypass it because
            // the old list only ever named extents the manager had forgotten
            // (which can never be a recovery target).
            if self.recovery_inflight.contains_key(eid)
                || self.ec_convert_inflight.contains_key(eid)
            {
                tracing::debug!(
                    extent_id = eid,
                    "reconcile: skipping unlink — an op is in flight on this extent"
                );
                continue;
            }
            // Drop in-memory entry and unlink files. Look up the disk
            // via the entry; if the entry is gone (concurrent delete),
            // fall back to scanning every disk.
            let entry = self.extents.remove(eid).map(|(_, v)| v);
            self.fd_lru.forget(*eid);
            self.ec_stage_nonce.remove(eid);
            if let Some(entry) = entry {
                if let Some(disk) = self.disks.get(&entry.disk_id) {
                    if let Err(e) = disk.remove_extent_files(*eid).await {
                        tracing::warn!(extent_id = eid, error = %e, "reconcile unlink failed");
                    }
                    continue;
                }
            }
            for disk in self.disks.values() {
                if let Err(e) = disk.remove_extent_files(*eid).await {
                    tracing::warn!(extent_id = eid, error = %e, "reconcile unlink failed");
                }
            }
        }
        Ok(())
    }

    /// Drop the payload files the manager says this node should NOT be
    /// holding for extents it IS a member of — the "keep the extent, drop the
    /// `.dat`" half of the reconcile, plus the shards of an abandoned attempt.
    ///
    /// The rule is one sentence: an extent has exactly ONE payload file here,
    /// the one the placement names; everything else is residue. Both post-flip
    /// cleanup and rollback cleanup fall out of it, with no second mechanism
    /// and no intent marker — a crash mid-cleanup is resolved by startup
    /// discovery re-deriving what is on disk.
    ///
    /// Deleting a payload file is DESTRUCTIVE, so it is gated three ways:
    ///
    /// 1. **The keeper must already be here.** `.dat` is dropped only once this
    ///    node actually holds the shard file the layout names. Otherwise a
    ///    placement arriving before the shard is staged — or naming a shard
    ///    this node never received — would delete the only copy it has.
    /// 2. **No in-flight op on the extent**, matching the garbage path and
    ///    `handle_delete_extent`: a recovery is mid-write into one of these
    ///    files.
    /// 3. **Only the manager decides.** The location comes from the placement;
    ///    the node never infers from what it happens to hold. A node with a
    ///    complete shard beside a complete `.dat` cannot tell which one the
    ///    cluster is pointed at — only the layout knows.
    /// 4. **The verdict must be newer than the staging it condemns.**
    ///    `staging_tick_at_ask` is the node's staging tick sampled before the
    ///    question was asked; a mark stamped after it belongs to an attempt the
    ///    manager had not been asked about, so its answer cannot be about it.
    async fn apply_placements(
        &self,
        placements: &[manager_rpc::ExtentPlacement],
        staging_tick_at_ask: u64,
    ) {
        for p in placements {
            let want = PayloadRef::for_extent(p.payload_location, p.shard_index);
            let Some(entry) = self.extents.get(&p.extent_id).map(|e| Rc::clone(e.value())) else {
                continue;
            };
            if self.recovery_inflight.contains_key(&p.extent_id)
                || self.ec_convert_inflight.contains_key(&p.extent_id)
            {
                continue;
            }
            // A placement is computed by the manager at ANSWER time and applied
            // HERE, later. A conversion can start in that window, and a
            // PARTICIPANT staging a shard sets no local marker — the guard
            // above only sees conversions THIS node coordinates. So the two
            // checks that matter are below.
            //
            // Serialize against the stripe writer, which holds this same lock
            // per stripe. Without it the delete lands BETWEEN two stripes.
            let op_lock = self.get_or_create_extent_op_lock(p.extent_id);
            let _op_guard = op_lock.lock().await;
            // The dangerous shape is a verdict that CONTRADICTS live staging:
            // it says the payload is still in `.dat` (so every shard here is
            // residue) while an attempt has staged shards on this node. Acting
            // on such a verdict deletes a shard the coordinator may still be
            // writing — so the question is whether the verdict is old enough to
            // predate the staging, and that is what the tick answers.
            //
            // A verdict of `InShardFile` cannot be stale in that way — the
            // manager only publishes it by flipping the layout, which happens
            // after every target confirmed. So that case proceeds, and clearing
            // the stage marker there is what lets cleanup run at all.
            // SEALED is not staging — it is the opposite — so it must not make
            // this look like a live attempt and block cleanup forever.
            let staged = self
                .ec_stage_nonce
                .get(&p.extent_id)
                .map(|v| *v)
                .filter(|m| m.nonce != EC_STAGING_SEALED);
            match want.location {
                PayloadLocation::InDat
                    if staged.is_some_and(|m| m.tick > staging_tick_at_ask) =>
                {
                    // The staging arrived after the question went out, so this
                    // answer is not about it. Leaving a shard costs space;
                    // deleting a live one costs the shard. Wait for a verdict
                    // asked for after the attempt was already visible.
                    tracing::debug!(
                        extent_id = p.extent_id,
                        "reconcile: skipping cleanup — this verdict says .dat while an \
                         attempt has staged shards here, so it predates the attempt"
                    );
                    continue;
                }
                PayloadLocation::InShardFile => {
                    // The flip happened — and it is the ONLY commit point, with
                    // no rename: the file every attempt was staging into is now
                    // the live shard. Clearing the floor here (what this used to
                    // do) left a coordinator whose attempt was superseded before
                    // the flip unordered against anything, so its late stripe was
                    // accepted and written straight over live data. Seal instead:
                    // staging for this extent is closed for good.
                    //
                    self.seal_ec_staging(p.extent_id);
                    // And make the seal survive a restart. The seal itself is
                    // in-memory; `.meta`'s payload_location is what
                    // `discover_shard_files` re-derives it from on the next
                    // boot. Written only on the transition — a reconcile round
                    // repeats this verdict every few minutes and re-fsyncing
                    // `.meta` each time would be pure I/O.
                    let already = entry.payload_location.load(Ordering::SeqCst)
                        == PayloadLocation::InShardFile.as_byte();
                    // The flip also carries a fact staging deliberately never
                    // wrote: the extent IS sealed (only a sealed extent can be
                    // EC-converted), at the manager's sealed_length and
                    // post-flip eversion. Staging writes no `.meta` — an
                    // abandoned CoW attempt must cost only deleted files — so
                    // the flip is the FIRST point a participant may persist the
                    // seal, and it is also effectively the last: the `.dat`
                    // this same sweep reclaims below is what the seal healer
                    // (`apply_extent_meta_durable` via append-refresh /
                    // re_avali) used to need, so a holder that misses it here
                    // kept `sealed=0 / eversion=1` under a live shard file
                    // forever and reloaded as an OPEN extent on every boot.
                    // `needs_seal_heal` also fires when the location byte was
                    // persisted by a pre-fix binary (or a crash landed between
                    // the two), healing existing clusters on their next sweep;
                    // once sealed it is false and the steady state does no I/O.
                    let needs_seal_heal = !entry.sealed.load(Ordering::SeqCst)
                        && entry.sealed_length.load(Ordering::SeqCst) == 0;
                    if !already {
                        entry
                            .payload_location
                            .store(PayloadLocation::InShardFile.as_byte(), Ordering::SeqCst);
                    }
                    if !already || needs_seal_heal {
                        // Never write `.meta` for a quarantined extent:
                        // `save_meta` would silently clear the quarantine and
                        // bypass the fail-closed contract.
                        if entry.corrupt_meta.load(Ordering::SeqCst) {
                            tracing::warn!(
                                extent_id = p.extent_id,
                                "layout committed to a shard file but `.meta` is quarantined — \
                                 the staging seal holds in memory only until recovery rebuilds it"
                            );
                        } else {
                            match self.extent_info_from_manager(p.extent_id).await {
                                Ok(Some(info)) if info.sealed || info.sealed_length > 0 => {
                                    // One save_meta (inside the durable applier)
                                    // carries the seal AND the payload location —
                                    // `write_meta_locked` reads the live atomics.
                                    if let Err(e) = self
                                        .apply_extent_meta_durable(p.extent_id, &entry, &info)
                                        .await
                                    {
                                        // The applier already fail-closed (disk
                                        // marked); the in-memory seal + staging
                                        // seal hold meanwhile.
                                        tracing::warn!(
                                            extent_id = p.extent_id,
                                            error = %e,
                                            "reconcile: could not persist the committed layout's \
                                             seal; the staging seal is in memory meanwhile"
                                        );
                                    }
                                }
                                res => {
                                    // Manager unreachable, or no authoritative
                                    // sealed view right now. The staging seal's
                                    // restart-survival must not wait on the
                                    // manager, so persist the payload location
                                    // alone; the seal fields retry via
                                    // `needs_seal_heal` on a later sweep.
                                    if let Err(e) = res {
                                        tracing::warn!(
                                            extent_id = p.extent_id,
                                            error = %e,
                                            "reconcile: seal fetch failed at layout commit \
                                             (seal persist retried next sweep)"
                                        );
                                    }
                                    if let Err(e) = self.save_meta(p.extent_id, &entry).await {
                                        tracing::warn!(
                                            extent_id = p.extent_id,
                                            error = %e,
                                            "could not persist the committed payload location; \
                                             the staging seal is in memory meanwhile"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                PayloadLocation::InDat => {
                    // `.dat` is the payload AND the staging here (if any) is
                    // older than the question. Two facts narrow what the
                    // verdict can be describing: the manager gives no placement
                    // at all while an op is in flight, so answering means no
                    // MARKER was held when it answered; and the tick says
                    // nothing staged here since. The shards below are reclaimed
                    // like any other file the layout does not name.
                    //
                    // That is NOT "no attempt is writing". The abandon this
                    // reclaim exists for fires on the dispatch tick where the
                    // coordinator has just started a fresh attempt, so stripes
                    // can still be streaming when the marker goes. Deleting
                    // under one is survivable because of two things outside this
                    // sweep, and both must keep holding: a later stripe whose
                    // staging file has vanished is REFUSED rather than recreated
                    // at its offset (`write_shard_stripe_local`), so no holey
                    // shard can be built; and an attempt whose marker was
                    // abandoned could never commit anyway — its completion
                    // report finds no marker and is ignored — so the bytes were
                    // already dead. Pinned by
                    // `a_reclaim_that_lands_mid_attempt_is_refused_by_the_next_stripe`.
                }
            }
            if !entry.holds_payload(want) {
                // The file we are told to keep is not here yet. Never delete
                // the other one on the strength of an instruction we cannot
                // yet satisfy.
                continue;
            }
            let Ok(disk) = self.disk_for(entry.disk_id) else {
                continue;
            };

            // Residual shard files: every index except the kept one.
            let stale_shards: Vec<u32> = entry
                .shard_index_list()
                .into_iter()
                .filter(|i| want.location != PayloadLocation::InShardFile || *i != want.shard_index)
                .collect();
            for idx in stale_shards {
                let path = disk.shard_path(p.extent_id, idx);
                if let Err(ue) = entry.discard_shard_file(&path, idx).await {
                    tracing::warn!(
                        extent_id = p.extent_id,
                        shard_index = idx,
                        error = %ue,
                        "reconcile: could not unlink a stale shard file (retried next sweep)"
                    );
                    continue;
                }
                tracing::info!(
                    extent_id = p.extent_id,
                    shard_index = idx,
                    "reconcile: dropped a shard file this node should not hold"
                );
            }

            // A redundant `.dat`, once the shard that replaced it is in hand.
            if want.location == PayloadLocation::InShardFile && entry.has_dat.load(Ordering::SeqCst)
            {
                // Order matters: stop serving `.dat` BEFORE unlinking it, so no
                // read can resolve an fd to a file that is about to vanish.
                entry.has_dat.store(false, Ordering::SeqCst);
                entry.len.store(0, Ordering::SeqCst);
                *entry.file.borrow_mut() = None;
                self.fd_lru.forget(p.extent_id);
                if let Err(e) = compio::fs::remove_file(&disk.extent_path(p.extent_id)).await {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        // Left for the next sweep; the entry already stopped
                        // serving it, which is the part that must not be wrong.
                        tracing::warn!(
                            extent_id = p.extent_id,
                            error = %e,
                            "reconcile: could not unlink the redundant .dat (retried next sweep)"
                        );
                        continue;
                    }
                }
                tracing::info!(
                    extent_id = p.extent_id,
                    shard_index = want.shard_index,
                    "reconcile: reclaimed the pre-conversion .dat; this node now serves its shard"
                );
            }
        }
    }

    /// does this shard own `extent_id`?
    #[inline]
    pub(crate) fn owns_extent(&self, extent_id: u64) -> bool {
        // canonical hashed map (was `extent_id % shard_count`,
        // which aliased bootstrap's contiguous ids onto shard 0). MUST match the
        // manager / StreamClient `shard_addr_for_extent`.
        self.shard_count <= 1
            || autumn_rpc::shard_for_extent(extent_id, self.shard_count) == self.shard_idx
    }

    /// look up / create the per-extent mutating-op lock. Held
    /// by `handle_convert_to_ec` and `handle_re_avali` for their full
    /// duration; `try_lock`'d by `handle_delete_extent` to refuse
    /// concurrent unlinks. Created lazily; lives for the node's
    /// lifetime. See the field docstring on `ec_conversion_locks` for
    /// the full semantic.
    fn get_or_create_extent_op_lock(&self, extent_id: u64) -> Rc<futures::lock::Mutex<()>> {
        let mut locks = self.ec_conversion_locks.borrow_mut();
        locks
            .entry(extent_id)
            .or_insert_with(|| Rc::new(futures::lock::Mutex::new(())))
            .clone()
    }

    /// return the local sibling address that owns `extent_id`
    /// (this host's shard for the target extent). None in single-thread mode.
    #[inline]
    fn sibling_for_extent(&self, extent_id: u64) -> Option<&str> {
        if self.shard_count <= 1 {
            return None;
        }
        let owner = autumn_rpc::shard_for_extent(extent_id, self.shard_count) as usize;
        self.sibling_addrs.get(owner).map(|s| s.as_str())
    }

    /// forward a control-plane RPC to a sibling shard on the same
    /// host. Used when an RPC arrives at a non-owner shard. Uses the
    /// manager_pool as a general-purpose ConnPool (the sibling address is
    /// a localhost loopback; per-shard reuse amortises the TCP cost).
    async fn forward_rpc_to_sibling(
        &self,
        sibling_addr: &str,
        msg_type: u8,
        payload: Bytes,
    ) -> HandlerResult {
        // 60 s — sibling shard forwarding can carry CONVERT_TO_EC,
        // COPY_EXTENT, RECOVERY which do real work. The bound is
        // generous but finite so the calling RPC handler doesn't
        // wedge if the sibling shard is itself paged out / hung.
        // The caller's own request will time out and the next retry
        // can re-route.
        self.manager_pool
            .call_timeout(sibling_addr, msg_type, payload, Duration::from_secs(60))
            .await
            .map_err(|e| {
                (
                    StatusCode::Unavailable,
                    format!("forward to shard {sibling_addr}: {e}"),
                )
            })
    }

    /// Return the first ALLOCATABLE disk (Online — not Full, not
    /// Faulted), or None. New extents must never land on a full disk;
    /// existing extents on a full disk keep serving reads + (failing)
    /// appends until space frees.
    fn choose_disk(&self) -> Option<Rc<DiskFS>> {
        self.disks.values().find(|d| d.allocatable()).cloned()
    }

    /// resolve an extent's file handle, re-opening it on a cache
    /// miss (a SEALED extent whose fd was evicted). Open/active extents hit the
    /// resident fast path (their fd is pinned, never evicted). Reading a sealed
    /// extent LRU-`touch`es it so the cache evicts the least-recently-read one
    /// once over `cap`. The returned `Rc` pins the fd for the caller's I/O
    /// across `.await`: even if a later read evicts this extent, the
    /// caller's clone keeps the fd alive until it finishes.
    ///
    /// Callers: every read / sealed-extent background op (EC-convert, re_avali,
    /// copy, recovery, meta-apply fsync). The write / append / coalescer /
    /// truncate durability path does NOT call this — it uses `file_rc()`
    /// synchronously (open extents are always resident).
    /// Open the payload file a request NAMED. `.dat` goes through the fd cache
    /// (`extent_file`); a shard file is opened per use — it is read-only after
    /// staging, and keeping it out of the cache means the cache keeps its
    /// one-fd-per-extent accounting rather than silently over-committing the
    /// process fd budget.
    async fn payload_file(
        &self,
        entry: &Rc<ExtentEntry>,
        payload: PayloadRef,
    ) -> Result<Rc<CompioFile>, String> {
        match payload.location {
            PayloadLocation::InDat => self.extent_file(entry).await,
            PayloadLocation::InShardFile => {
                let disk = self.disk_for(entry.disk_id)?;
                let path = disk.shard_path(entry.extent_id, payload.shard_index);
                let file = OpenOptions::new()
                    .read(true)
                    .open(&path)
                    .await
                    .map_err(|e| {
                        format!(
                            "open shard {} of extent {}: {e}",
                            payload.shard_index, entry.extent_id
                        )
                    })?;
                Ok(Rc::new(file))
            }
        }
    }

    async fn extent_file(&self, entry: &Rc<ExtentEntry>) -> Result<Rc<CompioFile>, String> {
        let sealed = entry.sealed.load(Ordering::Relaxed);
        if let Some(f) = entry.resident_file() {
            if sealed {
                self.fd_lru.touch(entry.extent_id);
            }
            return Ok(f);
        }
        // Miss — only ever a sealed, evicted extent. Re-open read+write (no
        // `create`: a missing file means the extent was deleted, which must
        // surface as an error, not silently resurrect an empty extent).
        let disk = self.disk_for(entry.disk_id)?;
        let path = disk.extent_path(entry.extent_id);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|e| format!("reopen sealed extent {}: {e}", entry.extent_id))?;
        let rc = Rc::new(file);
        // Publish the fd. A concurrent reader that also missed may have opened
        // its own handle — last writer wins the cache slot; both hold valid
        // `Rc`s over the SAME inode, so no correctness issue (the loser's fd
        // closes when its clone drops). No borrow is held across the `.await`.
        *entry.file.borrow_mut() = Some(rc.clone());
        if sealed {
            self.fd_lru.touch(entry.extent_id);
        }
        Ok(rc)
    }

    /// Resolve DiskFS for an extent by its disk_id. Returns error string if disk is unknown.
    fn disk_for(&self, disk_id: u64) -> Result<Rc<DiskFS>, String> {
        self.disks
            .get(&disk_id)
            .cloned()
            .ok_or_else(|| format!("unknown disk_id {disk_id}"))
    }

    /// ENOSPC-1: does this error message describe a CAPACITY condition
    /// (disk full / quota) rather than a media/fs fault? Matched on the
    /// std `io::Error` Display forms ("No space left on device (os error
    /// 28)", "Disk quota exceeded (os error 122)") because several call
    /// sites only have the stringified error (fsync coalescer waiters,
    /// anyhow chains) — the os-error suffix survives every wrapping in
    /// this codebase.
    pub(crate) fn is_disk_full_error(msg: &str) -> bool {
        msg.contains("os error 28")
            || msg.contains("No space left")
            || msg.contains("os error 122")
            || msg.contains("Disk quota")
    }

    /// Mark the disk hosting an extent after a write/persist error,
    /// CLASSIFIED (ENOSPC-1): capacity errors (ENOSPC/EDQUOT) set `Full`
    /// — recoverable, the 2 s sweep clears it when space frees — while
    /// anything else sets `Faulted` (permanent until restart, the
    /// historical "offline" semantics). Either way the disk stops
    /// hosting NEW extents immediately and the failing op itself has
    /// already been rejected by the caller (fail-closed, note 23/25).
    pub(crate) fn mark_disk_error_for_extent(&self, extent_id: u64, err_msg: &str) {
        if let Some(entry) = self.extents.get(&extent_id) {
            let disk_id = entry.disk_id;
            if let Some(disk) = self.disks.get(&disk_id) {
                if Self::is_disk_full_error(err_msg) {
                    if disk.health() == DiskHealth::Online {
                        tracing::warn!(
                            extent_id,
                            disk_id,
                            "disk FULL (capacity) — new-extent allocation suspended; \
                             self-heals when free space returns"
                        );
                        disk.set_full();
                    }
                } else if disk.online() {
                    tracing::error!(
                        extent_id,
                        disk_id,
                        err_msg,
                        "marking disk faulted due to I/O error"
                    );
                    disk.set_faulted();
                }
            }
        }
    }

    /// P0-B: per-extent `.meta`-write critical section. Every `.meta` writer
    /// acquires this so writes serialise AND each reads the live atomics just
    /// before writing — see the `meta_locks` field doc.
    fn meta_write_lock(&self, extent_id: u64) -> Rc<futures::lock::Mutex<()>> {
        self.meta_locks
            .borrow_mut()
            .entry(extent_id)
            .or_insert_with(|| Rc::new(futures::lock::Mutex::new(())))
            .clone()
    }

    pub(crate) async fn save_meta(
        &self,
        extent_id: u64,
        entry: &ExtentEntry,
    ) -> Result<(), String> {
        let lock = self.meta_write_lock(extent_id);
        let _g = lock.lock().await;
        self.write_meta_locked(extent_id, entry).await
    }

    /// Persist the V2 `.meta` sidecar from the entry's LIVE atomics, then
    /// advance `durable_owner_epoch` to the persisted `owner_epoch`.
    ///
    /// **The caller MUST hold this extent's `meta_write_lock`.** Reading the
    /// atomics + writing the file as ONE critical section is load-bearing: it
    /// stops a stale-snapshot writer (e.g. an owner_epoch fence persist)
    /// from clobbering a concurrent seal's `.meta`, and serialises the temp
    /// rename. The write is atomic (temp + fsync + rename) so a crash leaves
    /// EITHER the old valid record OR the new one — never a torn `.meta` that
    /// `parse_meta` would discard back to `owner_epoch = 0` (fail-open).
    async fn write_meta_locked(&self, extent_id: u64, entry: &ExtentEntry) -> Result<(), String> {
        let sealed_length = entry.sealed_length.load(Ordering::SeqCst);
        let eversion = entry.eversion.load(Ordering::SeqCst);
        let owner_epoch = entry.owner_epoch.load(Ordering::SeqCst);
        // P0-C: persist the explicit sealed flag + runtime avali mask. Enforce
        // `sealed_length > 0 ⇒ sealed` at write time.
        let sealed = entry.sealed.load(Ordering::SeqCst) || sealed_length > 0;
        let avali = entry.avali.load(Ordering::SeqCst);
        let payload_location = entry.payload_location.load(Ordering::SeqCst);

        // P0-C: always write V2 (52 bytes with CRC32C trailer over [0..48]).
        let mut buf = [0u8; Self::META_SIZE_V2];
        buf[0..8].copy_from_slice(Self::META_MAGIC_V2);
        buf[8..16].copy_from_slice(&extent_id.to_le_bytes());
        buf[16..24].copy_from_slice(&sealed_length.to_le_bytes());
        buf[24..32].copy_from_slice(&eversion.to_le_bytes());
        buf[32..40].copy_from_slice(&owner_epoch.to_le_bytes());
        buf[40] = u8::from(sealed);
        buf[41] = payload_location;
        // buf[42..44] reserved padding (left zero).
        buf[44..48].copy_from_slice(&avali.to_le_bytes());
        let crc = crc32c::crc32c(&buf[0..Self::META_SIZE_V2 - 4]);
        buf[48..52].copy_from_slice(&crc.to_le_bytes());

        // the .meta must be durable, not just in the page cache.
        // `apply_extent_meta_durable` fsyncs .dat FIRST so a crash can't leave a
        // persisted sealed_length longer than the durable .dat. P0-B: write
        // atomically (temp + fsync + rename) — a fixed `.tmp` name is safe
        // because the caller holds this extent's `meta_write_lock`.
        let disk = self.disk_for(entry.disk_id)?;
        let path = disk.meta_path(extent_id);
        let mut tmp = path.clone().into_os_string();
        tmp.push(".tmp");
        let tmp_path = std::path::PathBuf::from(tmp);
        let mut f = compio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)
            .await
            .map_err(|e| format!("open meta tmp for extent {extent_id}: {e}"))?;
        let BufResult(result, _) = f.write_all_at(buf.to_vec(), 0).await;
        result.map_err(|e| format!("write meta tmp for extent {extent_id}: {e}"))?;
        f.sync_data()
            .await
            .map_err(|e| format!("sync meta tmp for extent {extent_id}: {e}"))?;
        drop(f);
        compio::fs::rename(&tmp_path, &path)
            .await
            .map_err(|e| format!("rename meta for extent {extent_id}: {e}"))?;

        // P0-B: fsync the PARENT DIRECTORY so the rename (the directory-entry
        // update that swaps tmp → .meta) is itself durable. fsync of the tmp
        // file only makes its CONTENT durable; without the dir fsync a host
        // crash could lose the rename and leave the OLD `.meta` on disk — the
        // ACKed fence would then silently regress on restart, re-opening the
        // exact split-brain window this guarantee closes.
        if let Some(dir) = path.parent() {
            let d = compio::fs::File::open(dir)
                .await
                .map_err(|e| format!("open meta dir for extent {extent_id}: {e}"))?;
            d.sync_all()
                .await
                .map_err(|e| format!("fsync meta dir for extent {extent_id}: {e}"))?;
        }

        // The on-disk fence is now durable at `owner_epoch`; advance the
        // durable high-water so appends gated on it (P0-B) can proceed.
        entry
            .durable_owner_epoch
            .fetch_max(owner_epoch, Ordering::SeqCst);
        // META-FAILCLOSED: a fresh, valid `.meta` is now on disk — clear any
        // quarantine. Recovery / re_avali / a manager-driven re-seal that
        // reaches here has rebuilt authoritative state, so the extent may
        // serve again. (Steady-state writers never set the flag, so this is
        // a cheap no-op store on the hot path.)
        entry.corrupt_meta.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// P0-B: ensure the owner_epoch fence is DURABLE at `>= required` before
    /// the caller ACKs an append at that owner_epoch. The caller has already
    /// raised the in-memory bar (`owner_epoch.fetch_max(required)`) so a
    /// stale lower owner is rejected immediately; this makes the bar durable.
    /// Fast path = one atomic load (already durable). Else acquire the
    /// meta-write lock, re-check (a concurrent writer may have persisted it
    /// while we waited), and persist the live state. Fail-closed: a persist
    /// failure returns Err so the caller rejects the append (never ACK a write
    /// whose fence isn't durable).
    async fn ensure_fence_durable(
        &self,
        extent_id: u64,
        entry: &ExtentEntry,
        required: i64,
    ) -> Result<(), String> {
        if entry.durable_owner_epoch.load(Ordering::SeqCst) >= required {
            return Ok(());
        }
        let lock = self.meta_write_lock(extent_id);
        let _g = lock.lock().await;
        if entry.durable_owner_epoch.load(Ordering::SeqCst) >= required {
            return Ok(());
        }
        self.write_meta_locked(extent_id, entry).await
    }

    fn parse_meta(buf: &[u8], extent_id: u64) -> Option<LocalExtentMeta> {
        if buf.len() < Self::META_SIZE_V0 {
            return None;
        }
        // Dispatch on magic[7] for V0/V1/V2 layout.
        let v2 = &buf[0..8] == Self::META_MAGIC_V2;
        let v1 = &buf[0..8] == Self::META_MAGIC_V1;
        let v0 = &buf[0..8] == Self::META_MAGIC_V0;
        if !v0 && !v1 && !v2 {
            return None;
        }
        if v1 && buf.len() < Self::META_SIZE_V1 {
            return None;
        }
        if v2 && buf.len() < Self::META_SIZE_V2 {
            return None;
        }
        let eid = u64::from_le_bytes(buf[8..16].try_into().ok()?);
        if eid != extent_id {
            return None;
        }
        if v2 {
            let stored_crc = u32::from_le_bytes(buf[48..52].try_into().ok()?);
            let computed_crc = crc32c::crc32c(&buf[0..Self::META_SIZE_V2 - 4]);
            if stored_crc != computed_crc {
                tracing::warn!(
                    extent_id,
                    stored_crc,
                    computed_crc,
                    "P0-C: V2 meta sidecar CRC mismatch — bit rot or torn write; treating as missing"
                );
                return None;
            }
        } else if v1 {
            let stored_crc = u32::from_le_bytes(buf[40..44].try_into().ok()?);
            let computed_crc = crc32c::crc32c(&buf[0..Self::META_SIZE_V0]);
            if stored_crc != computed_crc {
                tracing::warn!(
                    extent_id,
                    stored_crc,
                    computed_crc,
                    "meta sidecar CRC mismatch — bit rot or torn write; treating as missing"
                );
                return None;
            }
        } else {
            // V0 legacy: no checksum. Warn once per load so operators see the upgrade signal.
            tracing::warn!(
                extent_id,
                "legacy V0 meta sidecar (no CRC) — will upgrade to V2 on next save_meta"
            );
        }
        let sealed_length = u64::from_le_bytes(buf[16..24].try_into().ok()?);
        let eversion = u64::from_le_bytes(buf[24..32].try_into().ok()?);
        let owner_epoch = i64::from_le_bytes(buf[32..40].try_into().ok()?);
        // P0-C: V2 carries the explicit sealed flag + avali; V0/V1 derive both
        // from `sealed_length > 0` (the pre-P0-C behaviour, so an old open
        // extent stays open and an old sealed extent stays sealed). The
        // invariant `sealed_length > 0 ⇒ sealed` is enforced on the V2 path
        // too (a corrupt-but-CRC-valid record claiming long-but-unsealed is
        // upgraded to sealed, fail-closed).
        let (sealed, avali, payload_location) = if v2 {
            let sealed = buf[40] != 0 || sealed_length > 0;
            let avali = u32::from_le_bytes(buf[44..48].try_into().ok()?);
            // buf[41] was reserved padding, so it is 0 — i.e. InDat — in every
            // record written before the field existed. Same layout, same size,
            // same CRC coverage: no migration, no version bump.
            (sealed, avali, buf[41])
        } else {
            (
                sealed_length > 0,
                if sealed_length > 0 { 1 } else { 0 },
                autumn_rpc::extent_rpc::PAYLOAD_LOCATION_IN_DAT,
            )
        };
        Some(LocalExtentMeta {
            sealed_length,
            eversion,
            owner_epoch,
            sealed,
            avali,
            payload_location,
        })
    }

    pub async fn load_extents(&self) -> Result<()> {
        for disk in self.disks.values() {
            let disk = Rc::clone(disk);
            let extents = Rc::clone(&self.extents);

            // Collect extent IDs from this disk (scan_extents needs &mut callback).
            let mut found: Vec<u64> = Vec::new();
            disk.scan_extents(|id, _path| {
                found.push(id);
            })
            .await?;

            for extent_id in found {
                // only load extents this shard owns. Under normal
                // operation the other shards will never have touched this
                // extent file (disk hash-byte vs. extent_id-modulo are
                // independent, so all extents with the same id collide on
                // the same file and shard). A mis-owned extent here would
                // indicate a prior run with a different shard_count.
                if !self.owns_extent(extent_id) {
                    tracing::debug!(
                        extent_id,
                        shard_idx = self.shard_idx,
                        shard_count = self.shard_count,
                        "skip load: extent does not belong to this shard"
                    );
                    continue;
                }
                let path = disk.extent_path(extent_id);
                let file = match OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(&path)
                    .await
                {
                    Ok(f) => f,
                    Err(e) => {
                        tracing::warn!(
                            "load_extents: cannot open extent {extent_id} on disk {}: {e}",
                            disk.disk_id
                        );
                        continue;
                    }
                };
                let len = file.metadata().await.map(|m| m.len()).unwrap_or(0);

                // META-FAILCLOSED: distinguish ABSENT `.meta` (fresh extent /
                // pre-first-write crash → default open is fine) from PRESENT
                // BUT CORRUPT `.meta` (CRC/magic/eid invalid → parse_meta None
                // → must QUARANTINE, never fail-open to owner_epoch=0).
                const DEFAULT_META: LocalExtentMeta = LocalExtentMeta {
                    sealed_length: 0,
                    eversion: 1,
                    owner_epoch: 0,
                    sealed: false,
                    avali: 0,
                    payload_location: autumn_rpc::extent_rpc::PAYLOAD_LOCATION_IN_DAT,
                };
                let (meta, corrupt_meta) = match compio::fs::read(disk.meta_path(extent_id)).await {
                    Ok(buf) => match Self::parse_meta(&buf, extent_id) {
                        Some(m) => (m, false),
                        None => {
                            // `.meta` exists but is unparseable: the `.dat` is
                            // present (we just opened it), so this is real
                            // corruption, not a fresh extent. Quarantine.
                            tracing::error!(
                                extent_id,
                                "META-FAILCLOSED: `.meta` present but corrupt — \
                                 quarantining extent (append/read/commit_length refused) \
                                 until manager recovery rebuilds it"
                            );
                            (DEFAULT_META, true)
                        }
                    },
                    // Only a genuine NotFound is a safe default-open (fresh
                    // extent, or crash between `.dat` create and first `.meta`
                    // write). ANY other read error (EIO / EACCES / etc.) leaves
                    // the `.meta` state UNKNOWN while the `.dat` exists —
                    // defaulting to open/owner_epoch=0 would re-open the same
                    // fail-open fence-bypass window, so quarantine (coco P1).
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => (DEFAULT_META, false),
                    Err(e) => {
                        tracing::error!(
                            extent_id,
                            error = %e,
                            "META-FAILCLOSED: `.meta` unreadable (non-NotFound IO error) — \
                             quarantining extent until manager recovery rebuilds it"
                        );
                        (DEFAULT_META, true)
                    }
                };
                let sealed_length = meta.sealed_length;
                let eversion = meta.eversion;

                extents.insert(
                    extent_id,
                    Rc::new(ExtentEntry {
                        has_dat: AtomicBool::new(true),
                        payload_location: AtomicU8::new(meta.payload_location),
                        shard_files: RefCell::new(Default::default()),
                        // at startup, do NOT keep SEALED extents'
                        // fds resident — that was the O(all-extents) open-fd
                        // storm. `file`/`len` were read above; drop the fd for
                        // sealed (first read re-opens via `extent_file`), keep it
                        // for OPEN/active extents (pinned). Startup fd peak is now
                        // ~one-at-a-time + open tails, not the whole extent set.
                        file: RefCell::new(if meta.sealed {
                            None
                        } else {
                            Some(Rc::new(file))
                        }),
                        extent_id,
                        len: AtomicU64::new(len),
                        eversion: AtomicU64::new(meta.eversion),
                        sealed_length: AtomicU64::new(meta.sealed_length),
                        // P0-C: restore the explicit sealed flag (V2) or the
                        // length-derived value (V0/V1). A sealed-empty extent
                        // (sealed=true, sealed_length=0) now correctly stays
                        // sealed across the restart and rejects ghost writes.
                        sealed: AtomicBool::new(meta.sealed),
                        avali: AtomicU32::new(meta.avali),
                        owner_epoch: AtomicI64::new(meta.owner_epoch),
                        // P0-B: the persisted fence IS durable on load (it came
                        // from the `.meta` we just parsed), so the in-memory bar
                        // and the durable high-water start equal.
                        durable_owner_epoch: AtomicI64::new(meta.owner_epoch),
                        disk_id: disk.disk_id,
                        coalescer: Coalescer::new(len),
                        owner: RefCell::new(OwnerMailbox::default()),
                        corrupt_meta: AtomicBool::new(corrupt_meta),
                        content_ck: RefCell::new(CachedChecksums::NotLoaded),
                        }),
                );
                tracing::info!(
                    "loaded extent {extent_id} from disk {}: len={len}, sealed_length={sealed_length}, eversion={eversion}",
                    disk.disk_id
                );
            }
        }

        self.discover_shard_files().await;
        Ok(())
    }

    /// Second startup pass: find every `extent-{id}.shard{i}` and attach it to
    /// its extent.
    ///
    /// Two shapes exist. A node that still has its `.dat` (mid-conversion, or
    /// awaiting cleanup) already has an entry from the `.dat` scan and just
    /// records which shard files it also holds. A node whose `.dat` was already
    /// reclaimed has NO entry from that scan — without this pass its shard
    /// would be unreachable, unaccounted, and undeletable, and the extent would
    /// look absent to the manager, which is how a rebuilt copy becomes a
    /// blocking orphan.
    ///
    /// A shard-only entry deliberately carries NO fd: `.dat` does not exist and
    /// must not be created (`extent_file` opens without `create` for exactly
    /// this reason). Its `len` is the shard's length, which is NOT the extent's
    /// `sealed_length` — the `.meta` keeps the extent-level truth.
    async fn discover_shard_files(&self) {
        for disk in self.disks.values() {
            let mut found: Vec<(u64, u32)> = Vec::new();
            if let Err(e) = disk.scan_shard_files(|id, idx| found.push((id, idx))).await {
                tracing::warn!(
                    disk_id = disk.disk_id,
                    error = %e,
                    "shard-file scan failed; shards on this disk stay unattached until the next restart"
                );
                continue;
            }
            for (extent_id, shard_index) in found {
                if !self.owns_extent(extent_id) {
                    continue;
                }
                let shard_len = compio::fs::metadata(&disk.shard_path(extent_id, shard_index))
                    .await
                    .map(|m| m.len())
                    .unwrap_or(0);
                if let Some(entry) = self.extents.get(&extent_id) {
                    entry.note_shard_file(shard_index, shard_len);
                    continue;
                }
                // Shard-only holder: build the entry from `.meta` alone.
                let meta = match compio::fs::read(disk.meta_path(extent_id)).await {
                    Ok(buf) => Self::parse_meta(&buf, extent_id),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                    Err(e) => {
                        tracing::error!(
                            extent_id,
                            error = %e,
                            "META-FAILCLOSED: shard present but `.meta` unreadable — quarantining"
                        );
                        None
                    }
                };
                // No parseable `.meta` beside a real payload file is the
                // META-FAILCLOSED case: quarantine rather than fail open to
                // eversion 1 / owner_epoch 0, which would let a stale writer
                // through the fence.
                let corrupt_meta = meta.is_none();
                let meta = meta.unwrap_or(LocalExtentMeta {
                    sealed_length: 0,
                    eversion: 1,
                    owner_epoch: 0,
                    sealed: false,
                    avali: 0,
                    payload_location: autumn_rpc::extent_rpc::PAYLOAD_LOCATION_IN_DAT,
                });
                let entry = Rc::new(ExtentEntry {
                    has_dat: AtomicBool::new(false),
                    payload_location: AtomicU8::new(meta.payload_location),
                    shard_files: RefCell::new(Default::default()),
                    file: RefCell::new(None),
                    extent_id,
                    // `len` is the `.dat` length, and there is no `.dat` here.
                    // The shard's bytes are accounted in `shard_files`; putting
                    // them in both would double-count this extent in every
                    // footprint `df` reports.
                    len: AtomicU64::new(0),
                    eversion: AtomicU64::new(meta.eversion),
                    sealed_length: AtomicU64::new(meta.sealed_length),
                    sealed: AtomicBool::new(meta.sealed),
                    avali: AtomicU32::new(meta.avali),
                    owner_epoch: AtomicI64::new(meta.owner_epoch),
                    durable_owner_epoch: AtomicI64::new(meta.owner_epoch),
                    disk_id: disk.disk_id,
                    coalescer: Coalescer::new(0),
                    owner: RefCell::new(OwnerMailbox::default()),
                    corrupt_meta: AtomicBool::new(corrupt_meta),
                    content_ck: RefCell::new(CachedChecksums::NotLoaded),
                });
                entry.note_shard_file(shard_index, shard_len);
                self.extents.insert(extent_id, entry);
                tracing::info!(
                    extent_id,
                    shard_index,
                    disk_id = disk.disk_id,
                    shard_len,
                    corrupt_meta,
                    "loaded shard-only extent (no .dat on this node)"
                );
            }
        }

        // Re-derive the EC staging seal from what `.meta` says. `InShardFile`
        // means the manager flipped this extent's layout, so the file every
        // attempt was staging into is now the LIVE shard and no attempt may
        // write it again. The seal itself is in-memory; without this pass a
        // restart would drop it and reopen the window for a superseded
        // coordinator's late stripe to overwrite live data.
        let sealed: Vec<u64> = self
            .extents
            .iter()
            .filter(|e| {
                e.value().payload_location.load(Ordering::SeqCst)
                    == autumn_rpc::extent_rpc::PAYLOAD_LOCATION_IN_SHARD_FILE
            })
            .map(|e| *e.key())
            .collect();
        for extent_id in &sealed {
            self.seal_ec_staging(*extent_id);
        }
        if !sealed.is_empty() {
            tracing::info!(
                count = sealed.len(),
                "EC staging sealed on load for extents whose layout is committed to a shard file"
            );
        }
    }

    /// Start the RPC server on a single-threaded compio runtime.
    /// Accepts connections (TCP or UCX, per `autumn_transport::current()`)
    /// and handles them cooperatively. TCP-only socket tuning gated on
    /// `Conn::as_tcp()` so UCX paths skip the TCP setsockopt calls.
    pub async fn serve(&self, addr: SocketAddr) -> Result<()> {
        self.accept_loop(addr, "data").await
    }

    /// serve BOTH the data-plane and a separate control-plane
    /// listener on the same `ExtentNode` instance. The control listener
    /// reuses `handle_connection` (same SQ/CQ machinery) but only
    /// receives small-payload control RPCs (`MSG_DF`, future
    /// `MSG_REPORT_DISK_FAILURE`, future heartbeat) so its `tx_bufs`
    /// flush and `FuturesUnordered` cap stay minimal in practice.
    ///
    /// Both listeners are bound synchronously up front before either accept
    /// loop starts. A bind failure on EITHER listener (e.g. `EADDRINUSE`
    /// because an operator misconfig left port+1000 occupied) is propagated
    /// as `Err` and the binary exits non-zero via the caller's `?`. The
    /// control listener is no longer best-effort: a half-bound EN
    /// (data online, control silently dead) used to flip `online=true`
    /// at the manager while every control RPC (ALLOC / RECOVERY / DELETE /
    /// RE_AVALI) blackholed — fail-stop is safer than a degraded node.
    pub async fn serve_with_control(
        &self,
        data_addr: SocketAddr,
        control_addr: SocketAddr,
    ) -> Result<()> {
        // separate control listener. Under UCX a second ucp_listener on
        // the same RoCE device fails to bind ("Device is busy" / "Address
        // already in use"), so we serve control RPCs on the data listener
        // instead — `handle_connection` dispatches by msg_type, so DF and the
        // other small control RPCs are handled identically there. The manager
        // is told an empty control_address (see autumn-op format) and routes
        // DF to the data address. TCP keeps the separate listener for
        // head-of-line isolation between bulk data and control RPCs.
        if autumn_transport::current_or_init().kind() == autumn_transport::TransportKind::Ucx {
            tracing::info!(
                data_addr = %data_addr,
                "UCX: control RPCs share the data listener (no separate control listener)"
            );
            return self.accept_loop(data_addr, "data").await;
        }
        // Bind BOTH listeners up front. Either bind failing is fatal:
        // the caller's `?` propagates the io error and the process exits.
        let transport = autumn_transport::current_or_init();
        let data_listener = transport
            .bind(data_addr)
            .await
            .with_context(|| format!("bind data listener {data_addr}"))?;
        let control_listener = transport
            .bind(control_addr)
            .await
            .with_context(|| format!("bind control listener {control_addr}"))?;
        tracing::info!(addr = %control_addr, "extent node CONTROL listener");
        let ctl_node = self.clone();
        compio::runtime::spawn(async move {
            if let Err(e) = ctl_node
                .accept_loop_on(control_listener, control_addr, "control")
                .await
            {
                tracing::warn!(
                    addr = %control_addr,
                    error = %e,
                    "control listener exited"
                );
            }
        })
        .detach();
        self.accept_loop_on(data_listener, data_addr, "data").await
    }

    /// Shared accept loop used by both `serve` and `serve_with_control`.
    /// `role` is a free-form label ("data" / "control") that goes into
    /// the listening log line for operator triage.
    async fn accept_loop(&self, addr: SocketAddr, role: &'static str) -> Result<()> {
        let transport = autumn_transport::current_or_init();
        let listener = transport.bind(addr).await?;
        self.accept_loop_on(listener, addr, role).await
    }

    /// Run the accept loop on an already-bound listener. Used by
    /// `serve_with_control` to surface bind errors synchronously before
    /// either listener starts servicing connections.
    async fn accept_loop_on(
        &self,
        mut listener: autumn_transport::Listener,
        addr: SocketAddr,
        role: &'static str,
    ) -> Result<()> {
        let transport = autumn_transport::current_or_init();
        tracing::info!(addr = %addr, role, kind = ?transport.kind(), "extent node listening");
        loop {
            // accept errors are connection-scoped — log + backoff +
            // continue. Previously the `?` here killed the whole EN shard on
            // a single failed handshake (on UCX, accept flushes the new ep,
            // so a peer dying mid-handshake surfaced as an accept Err).
            // Same fix shape as the manager serve loop + the PS
            // per-partition accept task. Known residual (coco P1,
            // accepted): the half-created UCX ep from a failed accept
            // stays allocated until worker destroy — no working close
            // path under MODE_NONE (see transport endpoint.rs "EP
            // lifetime"); one ep per failed handshake, bounded, strictly
            // better than process death.
            let (conn, peer) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(addr = %addr, role, error = %e, "accept failed; continuing");
                    compio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
            };
            if let Some(s) = conn.as_tcp() {
                if let Err(e) = s.set_nodelay(true) {
                    tracing::warn!(peer = %peer, error = %e, "set_nodelay failed");
                }
                // Do NOT setsockopt SO_RCVBUF/SO_SNDBUF here. An explicit
                // SO_RCVBUF on an ACCEPTED socket cannot raise the TCP window
                // (tp->window_clamp is already fixed at SYN time, ~64 KiB) —
                // it only sets SOCK_RCVBUF_LOCK, which disables receive-window
                // autotuning (DRS). Measured on a real 2-host cluster
                // (2026-07-19): the old `set_tcp_buffer_sizes(s, 512 KiB)`
                // froze the advertised window at 43008 B for the connection's
                // lifetime (`ss -ti`: snd_wnd const, rwnd_limited ≈ 100 %),
                // capping every PS→EN append conn at ~9 MB/s and a single
                // durable 8 MiB RF3 put at ~500-900 ms/append (37 MB/s file
                // write on a 200 GbE link). With autotuning (same as the PS
                // listener, which never locked buffers) the window grows to
                // multi-MB and the same conn moves hundreds of MB/s.
            }
            let node = self.clone();
            compio::runtime::spawn(async move {
                tracing::debug!(peer = %peer, role, "new rpc connection");
                if let Err(e) = Self::handle_connection(conn, node).await {
                    tracing::debug!(peer = %peer, role, error = %e, "rpc connection ended");
                }
            })
            .detach();
        }
    }

    /// Handle one TCP connection (R4 step 4.2 v3 — **true SQ/CQ**).
    ///
    /// **One compio task per TCP connection, inline `FuturesUnordered`,
    ///  concurrent submission and completion via `select` race.**
    ///
    /// The v2 design (commit b1a92f7) used a *burst-structured* loop:
    /// `reader.read().await` → push futures → drain ALL futures → flush →
    /// loop. This kept the microbench at parity but violated SQ/CQ semantics:
    /// fast ops in a burst were gated on the slowest op's completion, and no
    /// pipelining crossed burst boundaries (TCP burst N+1 couldn't start
    /// until burst N's drain finished).
    ///
    /// v3 restores true SQ/CQ:
    ///   - A **persistent read future** lives in `read_fut: Option<LocalBoxFuture>`
    ///     and owns the `OwnedReadHalf<TcpStream>` + read buffer until it
    ///     resolves. It is NEVER dropped mid-flight (that would corrupt
    ///     io_uring SQE state); on a completion-wins race it is put back
    ///     into the `Option` for the next iteration.
    ///   - A **single FuturesUnordered** holds in-flight batch I/O futures.
    ///   - **Each iteration** (in order):
    ///       1. Opportunistically drain any already-ready completions with
    ///          `inflight.next().now_or_never()` — costs nothing if none
    ///          are ready, and streams out responses immediately as they
    ///          finish rather than waiting for a burst boundary.
    ///       2. Flush accumulated `tx_bufs` with ONE `write_vectored_all`
    ///          syscall (amortises writev across multiple ready completions),
    ///          segmented at IOV_MAX — `tx_bufs` grows with request COUNT.
    ///       3. Decide what to wait on:
    ///           - `!has_inflight`  → await the read future alone.
    ///           - `at_cap`         → await completion alone (back-pressure:
    ///             we MUST NOT have more than `cap` futures in FU).
    ///           - Otherwise        → `select(read_fut, inflight.next())`.
    ///             On Left (read wins): consume result, decode, push, then
    ///             rebuild the read future. On Right (completion wins):
    ///             put the read future back, extend tx_bufs, loop.
    ///
    /// ## Why a completion doesn't starve the reader (and vice versa)
    ///
    /// `futures::future::select` polls both futures each call. If the read
    /// is always the slower of the two, the completion side naturally gets
    /// progress. If the completion is slower, the read side does. On the
    /// "many fast completions, no new reads" case the loop cycles through
    /// step 1 drain + step 3 select-Right repeatedly with the read future
    /// sitting pending.
    ///
    /// ## Buffer reuse
    ///
    /// The read buffer is moved INTO the read future and back OUT of it via
    /// `ReadBurst`. No per-iteration allocation — the same 512 KiB Vec is
    /// recycled.
    pub async fn handle_connection(conn: autumn_transport::Conn, node: ExtentNode) -> Result<()> {
        use futures::future::{select, Either, LocalBoxFuture};
        use futures::stream::{FuturesUnordered, StreamExt};
        use futures::FutureExt;

        const READ_BUF_SIZE: usize = 512 * 1024;

        let (reader, mut writer) = conn.into_split();
        let mut decoder = FrameDecoder::new();

        // per-conn inflight cap from `ExtentNodeConfig.inflight_cap`,
        // set once at node construction. No env read.
        let cap = node.inflight_cap;
        let mut inflight: FuturesUnordered<
            std::pin::Pin<Box<dyn std::future::Future<Output = Vec<Bytes>>>>,
        > = FuturesUnordered::new();

        // Response bytes from completions — flushed opportunistically each
        // iteration. A completion arriving mid-burst is written out as soon
        // as we swing past the top of the loop, not held until a burst
        // boundary.
        let mut tx_bufs: Vec<Bytes> = Vec::with_capacity(128);

        // Persistent read future: owns the reader + buf across iterations.
        // Rebuilt after it completes (ReadBurst returns reader + buf).
        let buf = vec![0u8; READ_BUF_SIZE];
        let mut read_fut: Option<LocalBoxFuture<'static, ReadBurst>> =
            Some(spawn_read(reader, buf));

        loop {
            // (1) Opportunistic drain of any already-ready completions.
            //     `now_or_never` never awaits — if the next item is Pending
            //     it returns None and we move on.
            while let Some(Some(done)) = inflight.next().now_or_never() {
                tx_bufs.extend(done);
            }

            // (2) Flush accumulated responses with ONE vectored write.
            // Segmented at IOV_MAX: `tx_bufs` holds one response per append in
            // the batch and accumulates across completions, so its length tracks
            // request COUNT — the axis the kernel rejects on.
            if !tx_bufs.is_empty() {
                let bufs = std::mem::take(&mut tx_bufs);
                autumn_rpc::client::write_vectored_chunked(&mut writer, bufs).await?;
            }

            // (3) Decide what to wait on.
            let n_inflight = inflight.len();
            let at_cap = n_inflight >= cap;

            if n_inflight == 0 {
                // Nothing in flight — just await the read.
                let rfut = read_fut
                    .take()
                    .expect("read_fut invariant: always Some when no Left branch pending");
                match rfut.await {
                    ReadBurst::Eof { .. } => return Ok(()),
                    ReadBurst::Err { e, .. } => return Err(e.into()),
                    ReadBurst::Data { buf, n, reader } => {
                        decoder.feed(&buf[..n]);
                        process_frames_backpressured(
                            &node,
                            &mut decoder,
                            &mut inflight,
                            &mut tx_bufs,
                            cap,
                        )
                        .await?;
                        read_fut = Some(spawn_read(reader, buf));
                    }
                }
                continue;
            }

            if at_cap {
                // Back-pressure: only await a completion. The read future
                // stays pinned in `read_fut` untouched.
                if let Some(done) = inflight.next().await {
                    tx_bufs.extend(done);
                }
                continue;
            }

            // (3c) Race read vs completion. Both futures are polled each
            //      call to `select`; whichever resolves first wins. We
            //      BORROW `inflight.next()` as the right-hand side — it's
            //      a single-use wrapper, so we create a fresh one each
            //      iteration. FU's internal completion state is preserved
            //      regardless of whether the wrapper is dropped or awaited.
            //
            //      The hot microbench workload (sustained request-response
            //      pipelining at depth=64 through one extent) produces a
            //      single inflight future at a time; the client doesn't
            //      send more until it drains responses. We detect this
            //      single-inflight case and skip the select overhead —
            //      the read future stays pinned, and we just await the
            //      completion. This preserves SQ/CQ semantics in multi-
            //      extent scenarios (n_inflight > 1) while regaining the
            //      per-op overhead of the old single-task hot path.
            if n_inflight == 1 {
                if let Some(done) = inflight.next().await {
                    tx_bufs.extend(done);
                }
                continue;
            }

            let rfut = read_fut.take().unwrap();
            let cfut = inflight.next();
            match select(rfut, Box::pin(cfut)).await {
                Either::Left((read_result, _cfut_dropped)) => {
                    match read_result {
                        ReadBurst::Eof { .. } => {
                            // Drain and flush remaining inflight before exiting.
                            while let Some(done) = inflight.next().await {
                                tx_bufs.extend(done);
                            }
                            if !tx_bufs.is_empty() {
                                let bufs = std::mem::take(&mut tx_bufs);
                                // Best-effort on the way out, but still segmented:
                                // the peer is owed as much of the drain as fits.
                                let _ =
                                    autumn_rpc::client::write_vectored_chunked(&mut writer, bufs)
                                        .await;
                            }
                            return Ok(());
                        }
                        ReadBurst::Err { e, .. } => return Err(e.into()),
                        ReadBurst::Data { buf, n, reader } => {
                            decoder.feed(&buf[..n]);
                            process_frames_backpressured(
                                &node,
                                &mut decoder,
                                &mut inflight,
                                &mut tx_bufs,
                                cap,
                            )
                            .await?;
                            read_fut = Some(spawn_read(reader, buf));
                        }
                    }
                }
                Either::Right((maybe_done, rfut_back)) => {
                    // Completion won; preserve the read future for next iter.
                    read_fut = Some(rfut_back);
                    if let Some(done) = maybe_done {
                        tx_bufs.extend(done);
                    }
                    // Loop top will drain more + flush opportunistically.
                }
            }
        }
    }

    async fn dispatch(&self, msg_type: u8, payload: Bytes) -> HandlerResult {
        match msg_type {
            MSG_APPEND => self.handle_append(payload).await,
            MSG_READ_BYTES => self.handle_read_bytes(payload).await,
            MSG_COMMIT_LENGTH => self.handle_commit_length(payload).await,
            MSG_ALLOC_EXTENT => self.handle_alloc_extent(payload).await,
            MSG_DF => self.handle_df(payload).await,
            MSG_REQUIRE_RECOVERY => self.handle_require_recovery(payload).await,
            MSG_RE_AVALI => self.handle_re_avali(payload).await,
            MSG_COPY_EXTENT => self.handle_copy_extent(payload).await,
            MSG_CONVERT_TO_EC => self.handle_convert_to_ec(payload).await,
            MSG_WRITE_SHARD => self.handle_write_shard(payload).await,
            MSG_DELETE_EXTENT => self.handle_delete_extent(payload).await,
            MSG_SYNCED_LENGTH => self.handle_synced_length(payload).await,
            MSG_PROBE_EXTENT => self.handle_probe_extent(payload).await,
            MSG_FENCE_EXTENT => self.handle_fence_extent(payload).await,
            _ => Err((
                StatusCode::InvalidArgument,
                format!("unknown msg_type {msg_type}"),
            )),
        }
    }

    /// wrong-shard rejection: hot-path RPCs (append/read/
    /// commit_length/probe/synced_length) must hit the owning shard. A
    /// wrong-shard request signals a client routing bug — surface it as
    /// FailedPrecondition so the client logs it instead of silently
    /// succeeding on the wrong shard.
    fn wrong_shard_err(&self, extent_id: u64) -> (StatusCode, String) {
        (
            StatusCode::FailedPrecondition,
            format!(
                "extent {} belongs to shard {} not shard {} (shard_count={})",
                extent_id,
                autumn_rpc::shard_for_extent(extent_id, self.shard_count),
                self.shard_idx,
                self.shard_count,
            ),
        )
    }

    async fn get_extent(&self, extent_id: u64) -> Result<Rc<ExtentEntry>, (StatusCode, String)> {
        if !self.owns_extent(extent_id) {
            return Err(self.wrong_shard_err(extent_id));
        }
        self.extents
            .get(&extent_id)
            .map(|v| Rc::clone(v.value()))
            .ok_or_else(|| {
                (
                    StatusCode::NotFound,
                    format!("extent {} not found", extent_id),
                )
            })
    }

    async fn ensure_extent(&self, extent_id: u64) -> Result<Rc<ExtentEntry>, String> {
        // a non-owning shard should never `ensure_extent`. This is
        // an invariant violation — log loudly and reject.
        if !self.owns_extent(extent_id) {
            return Err(format!(
                "ensure_extent on wrong shard: extent {} → shard {}, this is shard {} (count={})",
                extent_id,
                autumn_rpc::shard_for_extent(extent_id, self.shard_count),
                self.shard_idx,
                self.shard_count,
            ));
        }
        if let Some(v) = self.extents.get(&extent_id) {
            return Ok(Rc::clone(v.value()));
        }

        let disk = self
            .choose_disk()
            .ok_or_else(|| "no online disk available".to_string())?;
        let path = disk.extent_path(extent_id);
        if let Some(parent) = path.parent() {
            compio::fs::create_dir_all(parent)
                .await
                .map_err(|e| e.to_string())?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|e| e.to_string())?;

        // Optional ext4 block preallocation via `fallocate(FALLOC_FL_KEEP_SIZE)`.
        // Triggered ONLY when `AUTUMN_EN_PREALLOC_BYTES` is set to a positive
        // size (default 0 = disabled). Preallocates the extent file's disk
        // blocks up front so subsequent appends don't pay the ext4 extent-tree
        // update + block-allocation journal cost on each grow. `KEEP_SIZE`
        // preserves the inode size at 0 so `file.metadata().len()`-based
        // `len` recovery (load_extents) keeps working.
        //
        // SAFE for write-through cache + nobarrier (the same conditions under
        // which fdatasync is a no-op). On crash mid-append the unwritten
        // preallocated blocks may contain stale (zero / garbage) data, but
        // autumn's commit protocol (min-replica truncate-on-mismatch) and
        // the in-memory `len` watermark fence reads against any byte past
        // the acked commit length. So preallocated-but-unwritten blocks are
        // inert.
        // `fallocate(FALLOC_FL_KEEP_SIZE)` is a Linux/ext4-only optimization.
        // On non-Linux targets (macOS dev builds) it does not exist, so the
        // prealloc step is a no-op there — the extent still serves writes,
        // just without the up-front block reservation.
        let prealloc = if cfg!(target_os = "linux") {
            en_prealloc_bytes()
        } else {
            0
        };
        if prealloc > 0 {
            use std::os::fd::AsRawFd;
            let fd = file.as_raw_fd();
            let len_arg = prealloc as i64;
            let join = compio::runtime::spawn_blocking(move || -> std::io::Result<()> {
                // SAFETY: fd is owned by `file`, kept alive by this scope.
                #[cfg(target_os = "linux")]
                let rc = unsafe { libc::fallocate(fd, libc::FALLOC_FL_KEEP_SIZE, 0, len_arg) };
                // Non-Linux: unreachable because `prealloc` is forced to 0
                // above, but keep the branch compiling.
                #[cfg(not(target_os = "linux"))]
                let rc = {
                    let _ = (fd, len_arg);
                    0
                };
                if rc == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            })
            .await;
            match join {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    // ENOSPC / EOPNOTSUPP / etc. — log but don't fail extent open.
                    // The extent still serves writes; just no prealloc benefit.
                    tracing::warn!(
                        extent_id,
                        error = %e,
                        "fallocate(KEEP_SIZE) failed; continuing without prealloc"
                    );
                }
                Err(_panic) => {
                    tracing::warn!(
                        extent_id,
                        "fallocate spawn_blocking panicked; continuing without prealloc"
                    );
                }
            }
        }

        let len = file
            .metadata()
            .await
            .map(|m| m.len())
            .map_err(|e| e.to_string())?;

        let disk_id = disk.disk_id;
        self.extents.insert(
            extent_id,
            Rc::new(ExtentEntry {
                has_dat: AtomicBool::new(true),
                // A freshly created extent holds its payload in `.dat`; a
                // conversion only ever moves it via the manager's layout flip.
                payload_location: AtomicU8::new(autumn_rpc::extent_rpc::PAYLOAD_LOCATION_IN_DAT),
                shard_files: RefCell::new(Default::default()),
                // freshly-allocated OPEN extent — pinned resident.
                file: RefCell::new(Some(Rc::new(file))),
                extent_id,
                len: AtomicU64::new(len),
                eversion: AtomicU64::new(1),
                sealed_length: AtomicU64::new(0),
                // P0-C: a freshly-created/allocated extent is open.
                sealed: AtomicBool::new(false),
                avali: AtomicU32::new(0),
                owner_epoch: AtomicI64::new(0),
                durable_owner_epoch: AtomicI64::new(0),
                disk_id,
                coalescer: Coalescer::new(len),
                owner: RefCell::new(OwnerMailbox::default()),
                corrupt_meta: AtomicBool::new(false),
                content_ck: RefCell::new(CachedChecksums::NotLoaded),
            }),
        );
        self.extents
            .get(&extent_id)
            .map(|v| Rc::clone(v.value()))
            .ok_or_else(|| format!("extent {} not found after insert", extent_id))
    }

    /// Apply extent metadata from manager. Returns true if sealed_length changed from 0 to nonzero.
    fn apply_extent_meta(extent: &ExtentEntry, ex: &ExtentInfo) -> bool {
        let old_sealed = extent.sealed.load(Ordering::SeqCst);
        let old_len = extent.sealed_length.load(Ordering::SeqCst);
        extent.eversion.store(ex.eversion, Ordering::SeqCst);
        // P0-C: `sealed_length > 0 ⇒ sealed` (a manager that sends a length
        // implies the seal even if the bool lagged on an old wire path).
        extent
            .sealed
            .store(ex.sealed || ex.sealed_length > 0, Ordering::SeqCst);
        extent
            .sealed_length
            .store(ex.sealed_length, Ordering::SeqCst);
        extent.avali.store(ex.avali, Ordering::SeqCst);
        // BUG2 trace (opt-in, target `bug2_trace`): a manager seal that lands
        // BELOW this replica's local file length orphans the acked bytes in
        // `[sealed_length, local_len)` — the exact shape of
        // `stale_vp_offset_past_sealed_length` (a VP at offset O > sealed_length
        // is now unreadable). Silent in production (RUST_LOG unset); enabled in
        // the chaos repro via `RUST_LOG=…,bug2_trace=info`.
        let local_len = extent.len.load(Ordering::SeqCst);
        if (ex.sealed || ex.sealed_length > 0) && (ex.sealed_length) < local_len {
            tracing::warn!(
                target: "bug2_trace",
                extent_id = ex.extent_id,
                new_sealed_length = ex.sealed_length,
                local_len,
                eversion = ex.eversion,
                "BUG2 UNDER-SEAL: manager seal below local file length — orphans bytes [sealed_length, local_len)"
            );
        }
        // P0-C: the seal must be made durable when EITHER the extent newly
        // became sealed (incl. the sealed-EMPTY case `sealed_length` stays 0)
        // OR its sealed_length newly grew from 0 (the original grow trigger —
        // covers a sealed-empty tail that later receives a length). Both must
        // hit disk so a restart doesn't forget the seal.
        let became_sealed = !old_sealed && (ex.sealed || ex.sealed_length > 0);
        let len_grew = old_len == 0 && ex.sealed_length > 0;
        became_sealed || len_grew
    }

    /// apply extent metadata from manager AND make the seal
    /// durable on disk. When `apply_extent_meta` reports a 0→nonzero
    /// `sealed_length` transition we (1) persist the meta sidecar so a
    /// restart doesn't forget the seal, and (2) `file.sync_all()` the
    /// data file so any page-cache-only bytes up to `sealed_length`
    /// hit disk before the manager (or anyone else) can rely on the
    /// sealed prefix being durable.
    ///
    /// Without (2), an extent that was open + receiving `must_sync=
    /// false` writes can have `extent.len` advanced in memory past
    /// what's actually on disk; the seal then captures the in-memory
    /// length but the disk holds less. A subsequent extent-node
    /// restart, OOM-driven page eviction, or host reboot drops the
    /// unsynced bytes — the file shrinks below `sealed_length` and
    /// any VP referencing the lost region surfaces as
    /// `ec_read_full_and_slice: offset N past decoded payload len M`
    /// after EC conversion.
    ///
    /// Idempotent: repeat calls with no transition are cheap (one
    /// atomic load, no I/O). Returns the same `sealed_changed` flag
    /// as the underlying `apply_extent_meta` for callers that branch
    /// on it.
    async fn apply_extent_meta_durable(
        &self,
        extent_id: u64,
        extent: &Rc<ExtentEntry>,
        ex: &ExtentInfo,
    ) -> Result<bool, String> {
        let sealed_changed = Self::apply_extent_meta(extent, ex);
        // P0-A (coco issue 2): persist whenever the resulting state is SEALED —
        // NOT only on the 0→sealed transition (`sealed_changed`). apply_extent_meta
        // mutates the in-memory seal up-front; if a prior call's fsync/save_meta
        // failed, memory is already sealed so the retry's `sealed_changed` is
        // false and the old gate would SKIP the durable step forever (leaving
        // re_avali/copy to report CODE_OK for a never-durably-sealed replica).
        // Gating on the live `sealed` flag makes the durable step idempotent +
        // retry-safe; the seal is monotonic so this is concurrency-safe (a
        // concurrent call just re-persists the same state). Open extents
        // (sealed=false) still skip it. `sealed_changed` is still returned for
        // callers that branch on the transition.
        if extent.sealed.load(Ordering::SeqCst) {
            // fsync .dat FIRST (data durable), THEN write+fsync .meta
            // (sealed_length / eversion durable). Previously the order was
            // reversed: .meta written first then .dat fsync'd. If the
            // process crashed in that window, the OS page cache could have
            // already flushed .meta (44 bytes, well under one sector) while
            // .dat's must_sync=false bytes were still in page cache and
            // lost. On restart `parse_meta` returned the NEW sealed_length
            // while the `.dat` file size was SHORTER; subsequent reads
            // past the durable `extent.len` returned EOF or zero-padded
            // bytes — silent corruption that masqueraded as a successful
            // seal. The corrected order: even if the crash strikes
            // between the two steps, the worst observable state is "old
            // .meta + new .dat" which restart treats as still-unsealed
            // (manager re-applies the seal on next contact). Save_meta
            // itself was also made durable (open + write + fsync,
            // not bare `compio::fs::write`).
            // P0-A (coco final): do NOT persist a seal the local data does not
            // yet back. For a NON-EC extent whose `.dat` is shorter than the
            // manager's `sealed_length` — the re_avali / recovery "short
            // replica being repaired" case (handle_re_avali calls this BEFORE
            // its peer-copy) — persisting `.meta` with the longer sealed_length
            // would, on a crash before the peer-copy completes, leave exactly
            // the "short `.dat` + sealed `.meta`" corruption this fix targets.
            // Skip the durable step (memory stays sealed → appends rejected);
            // the seal is persisted later, once the peer-copy fills the data and
            // re-runs save_meta with `local_len >= sealed_length`. EC is
            // EXCLUDED: an EC shard `.dat` is legitimately `sealed_length / K`,
            // so its length never covers the logical `sealed_length`. Not an
            // error — the caller's repair flow continues.
            if !ex.ec_converted && extent.len.load(Ordering::SeqCst) < ex.sealed_length {
                tracing::debug!(
                    extent_id,
                    local_len = extent.len.load(Ordering::SeqCst),
                    sealed_length = ex.sealed_length,
                    "P0-A: skip seal-meta persist — local .dat does not yet cover sealed_length (short replica, will persist after repair)",
                );
                return Ok(sealed_changed);
            }
            // P0-A: FAIL-CLOSED on .dat fsync failure. Pre-P0-A this only
            // WARNed and then still wrote the sealed `.meta` — persisting a
            // sealed_length the `.dat` does not durably back. After a crash the
            // short `.dat` + the sealed `.meta` make `parse_meta` report a
            // sealed prefix that EOFs / zero-pads on read (silent corruption /
            // recovery failure). Instead: do NOT save_meta, and mark the disk
            // offline so the manager re-replicates this replica from a healthy
            // peer. The in-memory seal stays set (this process keeps rejecting
            // appends); nothing false is persisted, and on restart the OLD
            // (unsealed) `.meta` is the safe state — the manager re-applies the
            // seal on next contact (the ordering invariant).
            if ex.ec_converted && !extent.has_dat.load(Ordering::SeqCst) {
                // Shard-only holder (EC-converted, `.dat` reclaimed or never
                // held): there is no `.dat` whose durability the sealed `.meta`
                // depends on — the payload is the SHARD file, and both of its
                // writers (conversion staging and recovery rebuild) sync_data +
                // parent-dir-fsync before acking. `extent_file` deliberately
                // never creates `.dat` for such an entry, so resolving it here
                // would fail the whole call and leave the seal impossible to
                // ever persist on this node. Skip straight to the sidecar
                // persist below.
            } else {
                // resolve (re-open if the sealed extent was fd-evicted)
                // before the durability fsync. Fail-closed on a reopen error.
                let seal_f = self.extent_file(extent).await?;
                if let Err(e) = seal_f.sync_data().await {
                    tracing::error!(
                        extent_id,
                        sealed_length = ex.sealed_length,
                        error = %e,
                        "P0-A: .dat fsync failed before seal meta — disk OFFLINE, NOT persisting sealed meta",
                    );
                    self.mark_disk_error_for_extent(extent_id, &e.to_string());
                    // P0-A (coco): PROPAGATE the failure (was `-> bool`, swallowed)
                    // so callers (handle_re_avali / append meta-refresh / copy)
                    // map it to an error instead of reporting CODE_OK for a replica
                    // whose seal is not durable + whose disk is now offline.
                    return Err(format!(
                        ".dat fsync failed before seal meta for extent {extent_id}: {e}"
                    ));
                }
            }
            // Describe the sealed content while it is durable and immutable.
            // BEFORE `.meta`, so a crash between the two leaves an extent that
            // reloads unsealed and is re-sealed on the manager's next contact —
            // the reverse order would leave a sealed extent whose sidecar never
            // arrived until something happened to call this again.
            //
            // A failure here is a WARNING, not an error. The seal is the
            // load-bearing operation and integrity metadata is an addition to
            // it; failing the seal because a sidecar could not be written would
            // turn a hardening feature into an availability risk. This method
            // is idempotent and re-runs on every manager contact, so a missing
            // sidecar is retried rather than lost.
            if !ex.ec_converted {
                if let Err(e) = self
                    .write_extent_checksums(extent_id, extent, ex.sealed_length)
                    .await
                {
                    tracing::warn!(
                        extent_id,
                        sealed_length = ex.sealed_length,
                        error = %e,
                        "could not write the content checksum sidecar; this extent verifies \
                         as unknown until a later seal-apply retries it"
                    );
                }
            }
            // P0-A: a save_meta failure must likewise not be swallowed — the
            // seal is not durable, so flag the disk for recovery + propagate.
            if let Err(e) = self.save_meta(extent_id, extent).await {
                tracing::error!(
                    extent_id,
                    sealed_length = ex.sealed_length,
                    error = %e,
                    "P0-A: save_meta of sealed extent failed — disk OFFLINE (seal not durable)",
                );
                self.mark_disk_error_for_extent(extent_id, &e);
                return Err(format!(
                    "save_meta of sealed extent {extent_id} failed: {e}"
                ));
            }
        }
        Ok(sealed_changed)
    }

    /// Hash a sealed extent's `.dat` and persist the per-block checksums.
    ///
    /// Skipped when a sidecar already describes this exact `sealed_length`.
    /// That check is what keeps the cost at once-per-seal: the caller re-runs on
    /// every manager contact (append-refresh, re_avali, reconcile), and
    /// re-hashing a multi-GiB extent each time would make routine control
    /// traffic proportional to stored bytes.
    ///
    /// Blocks are read one at a time so peak memory is one block regardless of
    /// extent size, and each read is an await, so a long hash yields to the
    /// runtime instead of stalling the shard.
    async fn write_extent_checksums(
        &self,
        extent_id: u64,
        entry: &Rc<ExtentEntry>,
        sealed_length: u64,
    ) -> Result<(), String> {
        let disk = self.disk_for(entry.disk_id)?;
        let path = disk.ck_path(extent_id);
        if let Ok(existing) = compio::fs::read(&path).await {
            if let Some(ck) = extent_cksum::ExtentChecksums::decode(&existing, extent_id) {
                if ck.sealed_length == sealed_length {
                    return Ok(());
                }
            }
        }

        // Hash only bytes that are DURABLE, not merely reserved. The append
        // prologue advances `entry.len` before its pwritev is even submitted,
        // so `len >= sealed_length` (what the seal guard above tests) does not
        // mean the disk holds them; `last_synced` is the coalescer's fsync
        // high-water. (`fd_evictable` compares it against `pending_fsync`, not
        // against `sealed_length`, so it is a different question — every writer
        // that installs durable bytes must advance this watermark or the extent
        // reads as permanently un-synced here.) Hashing
        // over an in-flight write would record a checksum of bytes that never
        // existed, and skip-if-exists would keep it forever — turning every
        // whole-block read of a HEALTHY replica into a refusal. That is worse
        // than the rot this guards against, so it is the one case to fail
        // closed on.
        let durable = entry.coalescer.last_synced.load(Ordering::SeqCst);
        if durable < sealed_length {
            return Err(format!(
                "extent {extent_id}: only {durable} of {sealed_length} bytes are durable; \
                 not hashing content that is still in flight"
            ));
        }
        let block_bytes = extent_cksum::CK_BLOCK_BYTES;
        let n = extent_cksum::block_count_for(sealed_length, block_bytes);
        if n == 0 {
            // A sealed-empty extent describes no content; do not reopen an
            // evicted fd to hash nothing.
            let ck = extent_cksum::ExtentChecksums {
                sealed_length,
                block_bytes,
                blocks: Vec::new(),
            };
            self.persist_checksums(extent_id, &path, &ck).await?;
            *entry.content_ck.borrow_mut() = CachedChecksums::Present(Rc::new(ck));
            return Ok(());
        }
        let file = self.extent_file(entry).await?;
        let mut blocks = Vec::with_capacity(n);
        for i in 0..n {
            let (start, end) = extent_cksum::block_range(i, block_bytes, sealed_length);
            let buf = file_pread(Rc::clone(&file), start, (end - start) as usize)
                .await
                .map_err(|e| format!("read block {i} of extent {extent_id}: {e}"))?;
            blocks.push(crc32c::crc32c(&buf));
        }
        let ck = extent_cksum::ExtentChecksums {
            sealed_length,
            block_bytes,
            blocks,
        };
        self.persist_checksums(extent_id, &path, &ck).await?;
        // Refresh the read-side cache. The extent is marked sealed in memory
        // BEFORE any of this I/O runs, so a read batch arriving in that window
        // asks for the sidecar, does not find it, and caches `Absent` — which
        // nothing else ever resets. That window is not a corner: a log extent
        // seals at roll while readers are on its tail. Without this the very
        // extents that just got a checksum are the ones that never use it.
        *entry.content_ck.borrow_mut() = CachedChecksums::Present(Rc::new(ck));
        Ok(())
    }

    /// Write a sidecar durably: tmp → fsync → rename → parent-dir fsync, the
    /// same discipline `.meta` uses. A torn sidecar fails its own trailer CRC
    /// and reads as absent, so this is about not leaving one behind rather than
    /// about correctness.
    async fn persist_checksums(
        &self,
        extent_id: u64,
        path: &std::path::Path,
        ck: &extent_cksum::ExtentChecksums,
    ) -> Result<(), String> {
        let buf = ck.encode(extent_id);
        let mut tmp = path.to_path_buf().into_os_string();
        tmp.push(".tmp");
        let tmp_path = std::path::PathBuf::from(tmp);
        let mut f = compio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)
            .await
            .map_err(|e| format!("open ck tmp for extent {extent_id}: {e}"))?;
        let BufResult(result, _) = f.write_all_at(buf, 0).await;
        result.map_err(|e| format!("write ck tmp for extent {extent_id}: {e}"))?;
        f.sync_data()
            .await
            .map_err(|e| format!("sync ck tmp for extent {extent_id}: {e}"))?;
        drop(f);
        compio::fs::rename(&tmp_path, path)
            .await
            .map_err(|e| format!("rename ck for extent {extent_id}: {e}"))?;
        if let Some(dir) = path.parent() {
            let d = compio::fs::File::open(dir)
                .await
                .map_err(|e| format!("open ck dir for extent {extent_id}: {e}"))?;
            d.sync_all()
                .await
                .map_err(|e| format!("fsync ck dir for extent {extent_id}: {e}"))?;
        }
        tracing::debug!(
            extent_id,
            sealed_length = ck.sealed_length,
            blocks = ck.blocks.len(),
            "wrote the content checksum sidecar for a sealed extent"
        );
        Ok(())
    }

    /// The cached `.ck` for a sealed extent, loading it at most once.
    ///
    /// Only sealed extents have one, and the seal is immutable, so a single
    /// load serves every later read. `Absent` is cached as deliberately as
    /// `Present`: extents sealed before this existed are the common case and
    /// must not cost a filesystem probe per read.
    async fn cached_content_checksums(
        &self,
        extent_id: u64,
        entry: &Rc<ExtentEntry>,
    ) -> Option<Rc<extent_cksum::ExtentChecksums>> {
        if !entry.sealed.load(Ordering::SeqCst) {
            return None;
        }
        match &*entry.content_ck.borrow() {
            CachedChecksums::Present(ck) => return Some(Rc::clone(ck)),
            CachedChecksums::Absent => return None,
            CachedChecksums::NotLoaded => {}
        }
        let sealed_length = entry.sealed_length.load(Ordering::SeqCst);
        let loaded = self
            .load_extent_checksums(extent_id, entry, sealed_length)
            .await
            .map(Rc::new);
        *entry.content_ck.borrow_mut() = match &loaded {
            Some(ck) => CachedChecksums::Present(Rc::clone(ck)),
            None => CachedChecksums::Absent,
        };
        loaded
    }

    /// The sidecar for `extent_id`, or `None` when there is no evidence.
    ///
    /// Absent, unreadable, mismatched, or describing a different length all
    /// collapse to `None`. A sidecar that disagrees with the extent's seal
    /// describes different content — a crash between the two writes — and is
    /// not evidence about these bytes.
    async fn load_extent_checksums(
        &self,
        extent_id: u64,
        entry: &Rc<ExtentEntry>,
        sealed_length: u64,
    ) -> Option<extent_cksum::ExtentChecksums> {
        let disk = self.disk_for(entry.disk_id).ok()?;
        let raw = compio::fs::read(disk.ck_path(extent_id)).await.ok()?;
        let ck = extent_cksum::ExtentChecksums::decode(&raw, extent_id)?;
        if ck.sealed_length != sealed_length {
            tracing::warn!(
                extent_id,
                sidecar_length = ck.sealed_length,
                sealed_length,
                "content checksum sidecar describes a different length; ignoring it"
            );
            return None;
        }
        Some(ck)
    }

    async fn truncate_to_commit(extent: &Rc<ExtentEntry>, commit: u64) -> Result<(), String> {
        // (coco/subagent P1): called from the append prologue AFTER
        // a manager seal-confirm RPC await — a concurrent seal+evict in that
        // window would panic the old `file_rc()`. `None` = the extent was
        // concurrently sealed → reject (the caller's seal re-check handles it);
        // holding `f` pins the fd (via `fd_evictable` strong_count) for the
        // set_len + fsync.
        let Some(f) = extent.resident_file() else {
            return Err("extent sealed (fd evicted) during commit-reconcile".to_string());
        };
        // This is the ONLY path that makes an extent shorter, and it ran
        // silently. Beyond-commit bytes are un-acked by definition, so dropping
        // them is correct — but if a writer ever supplies a commit BELOW what
        // was already acked, this is where acked data disappears, and nothing
        // recorded that it happened. A checkpointed vp_head landing past a
        // later seal (`stale_vp_offset_past_sealed_length`, which wedges the
        // partition on reopen) can only be explained by `len` going backwards,
        // so this line is the evidence that diagnosis needs.
        let prev_len = extent.len.load(Ordering::SeqCst);
        if commit < prev_len {
            tracing::warn!(
                extent_id = extent.extent_id,
                prev_len,
                commit,
                dropped = prev_len - commit,
                "truncating extent to the writer's commit — beyond-commit bytes dropped"
            );
        }
        f.set_len(commit).await.map_err(|e| e.to_string())?;
        // fsync the truncate. Without this, the kernel may report the
        // smaller size in stat() before the inode metadata is durable; if the
        // node crashes after `set_len` but before any subsequent must_sync
        // append flushes the file's metadata, post-restart the file size
        // could be observed at the pre-truncate length. The min-replica
        // commit protocol depends on per-replica `extent.len` matching what
        // the file actually holds — a stale longer length lets the next
        // commit_length probe report wrong consensus, after which an append
        // truncates the OTHER replicas back to that wrong value, diverging
        // them at the same offset. fdatasync (sync_data) is sufficient: the
        // file size IS the data we need durable, and subsequent appends
        // will sync content separately.
        f.sync_data().await.map_err(|e| e.to_string())?;
        extent.len.store(commit, Ordering::SeqCst);
        // Bug fix: align the coalescer's view with the actual file
        // length post-truncate. `last_synced` is what `MSG_COMMIT_LENGTH`
        // and `MSG_PROBE_EXTENT` return; if we leave it at the
        // pre-truncate value, commit_length reports a length that no
        // longer exists on disk. `pending_fsync` follows the same
        // shrink — the subsequent pwrite (if any) will store its own
        // larger end value via the regular coalescer path.
        extent.coalescer.last_synced.store(commit, Ordering::SeqCst);
        extent
            .coalescer
            .pending_fsync
            .store(commit, Ordering::SeqCst);
        Ok(())
    }

    /// Crate-visible wrapper for `truncate_to_commit` used from extent_worker.rs.
    pub(crate) async fn truncate_to_commit_ref(
        &self,
        extent: &Rc<ExtentEntry>,
        commit: u64,
    ) -> Result<(), String> {
        Self::truncate_to_commit(extent, commit).await
    }

    /// The RESPONSE size is the bound that matters here: the frame encoder
    /// casts the payload length to u32 without a guard, so a single reply
    /// carrying >= 4 GiB wraps its header and the reader fails the CRC. Every
    /// caller must bound what it asks for in ONE request — `length == 0`
    /// (read-to-end) is only safe on a payload known to be small.
    ///
    /// One `MSG_READ_BYTES` round-trip. `length == 0` means read-to-end
    /// (legacy single-shot path); otherwise reads exactly `[offset,
    /// offset+length)`.
    async fn read_bytes_chunk(
        sock: std::net::SocketAddr,
        addr: &str,
        extent_id: u64,
        eversion: u64,
        offset: u64,
        length: u64,
        payload: PayloadRef,
    ) -> Result<Vec<u8>, String> {
        let req = ReadBytesReq::new(extent_id, eversion, offset, length, payload);
        // BOUND this recovery source read. `rpc_oneshot`
        // (connect + write + read) is otherwise UNBOUNDED — under chaos churn a
        // source EN slowed / half-open by a network-partition or latency toxic
        // (or mid-kill) can park this await indefinitely, pinning a recovery
        // permit + the manager's Recovery marker for the whole stall. A
        // timed-out read fails this source so `stream_one_source` falls
        // through to the next healthy source (or the recovery fails cleanly +
        // retries). 30 s covers a legit 256 MiB chunk transfer even on a
        // slow-but-progressing link while capping a genuinely wedged peer.
        // Mirrors the "bound every await reachable from a loop"
        // invariant + the append-fanout / chunked-read timeouts.
        // `extent_info_from_manager` (right below) already bounds its manager
        // call the same way. (NOTE: this bound was ADDED while chasing the
        // decommission drain wedge, but was NOT that bug's cause — the wedge
        // was the lost recovery COMPLETION, see `try_adopt_completed_recovery`
        // + the system_chaos control-port proxy fix. Kept as a correct
        // liveness bound in its own right.)
        let resp_bytes = match compio::time::timeout(
            std::time::Duration::from_secs(30),
            rpc_oneshot(sock, MSG_READ_BYTES, req.encode()),
        )
        .await
        {
            Ok(r) => r.map_err(|e| format!("read_bytes from {addr}: {e}"))?,
            Err(_) => {
                return Err(format!(
                    "read_bytes from {addr}: timed out after 30s (source slow/unreachable)"
                ))
            }
        };
        let resp = ReadBytesResp::decode(resp_bytes).map_err(|e| format!("decode: {e}"))?;
        if resp.code != CODE_OK {
            return Err(format!(
                "read_bytes error from {addr}: code={}",
                code_description(resp.code)
            ));
        }
        Ok(resp.payload.to_vec())
    }

    // (`fetch_full_extent_from_sources` — the whole-extent buffering peer-copy —
    // was removed once the EC-convert path switched to the streaming
    // `stream_extent_from_sources` below; recovery / re_avali already used the
    // streaming form. The buffering helper that used to sit beside it is
    // gone — the EC rebuild was its last caller and now reads per stripe.
    // `[offset, size)` range copy used by `handle_copy_extent`.)

    /// Stage C: stream the full sealed extent from a healthy peer straight
    /// into `dest`, chunk-by-chunk (read one `FILE_IO_CHUNK_BYTES` chunk →
    /// `pwrite` it → drop it), so peak RAM is ONE chunk regardless of extent
    /// size — unlike `fetch_full_extent_from_sources`, which materialized the
    /// whole extent in a single `Vec<u8>` before the writeback. `dest` is
    /// truncated to 0 before each source attempt, so a mid-stream source failure
    /// or short read abandons that source and the next one restarts cleanly from
    /// offset 0 (set_len(0) discards the partial write — no corruption). Returns
    /// the bytes written; succeeds only when a source delivered the full
    /// `sealed_length` (or `sealed_length == 0` read-to-end). Does NOT fsync —
    /// the caller syncs once after a successful return.
    async fn stream_extent_from_sources(
        &self,
        extent: &ExtentInfo,
        exclude_node_ids: &[u64],
        dest: &Rc<ExtentEntry>,
    ) -> Result<u64, String> {
        let nodes = self
            .nodes_map_from_manager()
            .await
            .map_err(|e| format!("nodes_map: {e}"))?;
        // `dest` may be a SEALED extent (re_avali repair) whose fd
        // was evicted — resolve (re-open on miss) ONCE and hold it for the whole
        // rebuild. The held `Rc` pins the fd across every source attempt.
        let dest_f = self.extent_file(dest).await?;
        let total = extent.sealed_length;
        // SEED13: per-source failure-reason trace + over-promised-seal
        // reconciliation. Pre-this, every source's error/short was swallowed
        // (`Err(_e) => continue`) so a stuck recovery surfaced only as the
        // opaque "no source replica available" summary, with no way to tell
        // unreachable-source from short-read. Logged at warn (recovery is rare
        // + the happy path returns on the first full Ok).
        //
        // `best` = the longest copy any REACHABLE source delivered. `err_count`
        // = sources that errored mid-stream. `unverified` = non-excluded
        // sources we could NOT even attempt (absent from nodes_map / unparseable
        // addr / dest reset failed). BOTH must be zero before we reconcile down
        // (coco P1): an unattempted source might still hold the full
        // `sealed_length`, so reconciling to a short consensus while any source
        // is unverified risks dropping data that exists out of reach.
        // This helper's contract is the FULL extent, which a per-node shard
        // file cannot satisfy — an EC'd extent is repaired by
        // `run_ec_recovery_payload`, which reads shards by name. Refuse loudly
        // rather than hand back a shard sized like a short read.
        if PayloadLocation::from_byte(extent.payload_location) != PayloadLocation::InDat {
            return Err(format!(
                "extent {}: payload is not in .dat; full-extent copy does not apply",
                extent.extent_id
            ));
        }
        let mut attempted = 0usize;
        let mut err_count = 0usize;
        let mut unverified = 0usize;
        let mut best: Option<(std::net::SocketAddr, String, u64)> = None;
        for node_id in extent.replicates.iter().chain(extent.parity.iter()) {
            if exclude_node_ids.contains(node_id) {
                continue;
            }
            let Some((base, shard_ports)) = nodes.get(node_id) else {
                unverified += 1;
                tracing::warn!(
                    extent_id = extent.extent_id,
                    node_id,
                    "recovery source skipped: node_id absent from nodes_map (stale map?)"
                );
                continue;
            };
            // The OWNING shard, not the base port: a peer refuses a hot-path
            // read addressed to the wrong shard, and with shard_count=4 only
            // the extents that hash to shard 0 would have worked.
            let routed = shard_addr_for_extent(base, shard_ports, extent.extent_id);
            let addr = &routed;
            let Ok(sock) = parse_addr(addr) else {
                unverified += 1;
                tracing::warn!(
                    extent_id = extent.extent_id,
                    node_id,
                    addr = %addr,
                    "recovery source skipped: unparseable addr"
                );
                continue;
            };
            // Reset before each attempt — a previous source's partial stream
            // must not bleed into this one.
            if dest_f.set_len(0).await.is_err() {
                unverified += 1;
                tracing::warn!(
                    extent_id = extent.extent_id,
                    node_id,
                    "recovery source skipped: dest set_len(0) failed"
                );
                continue;
            }
            attempted += 1;
            // Report bytes copied as they land, so a long rebuild shows a
            // ratio instead of a bare RUNNING. Reset per source: a failed
            // source truncates the destination and the next one restarts at 0.
            let on_progress = |done: u64| {
                self.note_op_progress(
                    extent.extent_id,
                    autumn_rpc::manager_rpc::OP_KIND_RECOVERY,
                    done,
                    total,
                );
            };
            match Self::stream_one_source(
                sock,
                addr,
                extent.extent_id,
                extent.eversion,
                total,
                &dest_f,
                PayloadRef::in_dat(),
                &on_progress,
            )
            .await
            {
                Ok(written) if total == 0 || written >= total => return Ok(written),
                Ok(short) => {
                    // source had < sealed_length — remember the longest, try next
                    if best.as_ref().is_none_or(|(_, _, w)| short > *w) {
                        best = Some((sock, addr.clone(), short));
                    }
                    tracing::warn!(
                        extent_id = extent.extent_id,
                        node_id,
                        addr = %addr,
                        got = short,
                        want = total,
                        eversion = extent.eversion,
                        "recovery source SHORT: replica has fewer bytes than sealed_length"
                    );
                    continue;
                }
                Err(e) => {
                    // source failed mid-stream — next restarts from 0
                    err_count += 1;
                    tracing::warn!(
                        extent_id = extent.extent_id,
                        node_id,
                        addr = %addr,
                        want = total,
                        eversion = extent.eversion,
                        err = %e,
                        "recovery source FAILED"
                    );
                    continue;
                }
            }
        }

        // SEED13 over-promised-seal reconciliation. No source held the full
        // `sealed_length`. If EVERY source we could reach responded
        // (`err_count == 0`) yet all are short, the manager's `sealed_length`
        // is an over-promise — the lenient failover-seal (the `end == 0`
        // path in `handle_stream_alloc_extent`) sealed at `min` over the
        // reachable members at seal time, promoting a speculative/un-acked
        // tail byte that NO replica durably retained (it rolled back on the
        // next min-commit truncation). Retrying forever for bytes that exist
        // nowhere wedges the manager's refuse-at-start guards for
        // alloc_extent / punch_holes against this extent, freezing the
        // partition's write / flush / range paths.
        //
        // Reconcile to the replica consensus: copy the longest available copy
        // and succeed. SAFE under all-replica-ACK — the acked prefix is on
        // EVERY replica, so the best reachable copy is >= the acked length;
        // only phantom (un-acked) tail bytes are dropped, which the lenient seal already
        // treats as acceptable (see manager note 28 / `feedback`-seal-lenient).
        // The guard `err_count == 0 && unverified == 0` ensures we NEVER
        // reconcile down while any source was unreachable OR unattempted — such
        // a source might still hold the full data, so we Err and let the
        // manager re-dispatch until every source is reachable + confirmed short
        // (coco P1). run_recovery_task still applies `sealed_length` via
        // `fetch_max`, so the recovered replica reports
        // `synced_length = max(0, sealed_length)` and the flush barrier clears.
        if err_count == 0 && unverified == 0 {
            if let Some((sock, addr, best_len)) = best {
                // Re-stream the longest copy cleanly — a trailing shorter
                // attempt above may have left `dest` at a different length.
                let _ = dest_f.set_len(0).await;
                // coco P0: do NOT swallow a re-stream failure as success. If the
                // best source fails or short-reads on the re-stream, the recovered
                // file would be incomplete yet marked recovered — propagate the
                // error / refuse so the manager re-dispatches instead.
                let got = Self::stream_one_source(
                    sock,
                    &addr,
                    extent.extent_id,
                    extent.eversion,
                    best_len,
                    &dest_f,
                    PayloadRef::in_dat(),
                    // Progress is reported by the recovery path only.
                    &|_| {},
                )
                .await?;
                if got < best_len {
                    return Err(format!(
                        "recovery reconcile re-stream short for extent {}: got {got} < best {best_len}",
                        extent.extent_id
                    ));
                }
                tracing::warn!(
                    extent_id = extent.extent_id,
                    sealed_length = total,
                    reconciled_to = got,
                    eversion = extent.eversion,
                    attempted,
                    "recovery: sealed_length over-promised (phantom seal); reconciled to replica consensus"
                );
                return Ok(got);
            }
        }

        tracing::warn!(
            extent_id = extent.extent_id,
            sealed_length = total,
            eversion = extent.eversion,
            replicates = ?extent.replicates,
            parity = ?extent.parity,
            exclude = ?exclude_node_ids,
            attempted,
            err_count,
            unverified,
            "recovery: no source replica available for streaming copy"
        );
        Err("no source replica available for streaming copy".to_string())
    }

    /// One source's contribution to `stream_extent_from_sources`: read
    /// `[0, total)` from `addr` in `FILE_IO_CHUNK_BYTES` chunks, writing each
    /// chunk to `dest_file` at its offset before reading the next, so only one
    /// chunk is resident at a time. Returns bytes written. `total == 0` =
    /// read-to-end single shot (unsealed; recovery normally runs on sealed
    /// extents, so this is the rare path).
    async fn stream_one_source(
        sock: std::net::SocketAddr,
        addr: &str,
        extent_id: u64,
        eversion: u64,
        total: u64,
        dest_file: &Rc<CompioFile>,
        payload: PayloadRef,
        // Called with the bytes transferred so far, once per chunk. Recovering
        // a 16 GiB extent runs for minutes; without this the ledger shows a
        // bare RUNNING for the whole time.
        on_progress: &dyn Fn(u64),
    ) -> Result<u64, String> {
        if total == 0 {
            let got =
                Self::read_bytes_chunk(sock, addr, extent_id, eversion, 0, 0, payload).await?;
            let n = got.len() as u64;
            if !got.is_empty() {
                file_pwrite(dest_file.clone(), 0, Bytes::from(got))
                    .await
                    .map_err(|e| e.to_string())?;
            }
            return Ok(n);
        }
        let chunk = FILE_IO_CHUNK_BYTES as u64;
        let mut offset: u64 = 0;
        while offset < total {
            let want = chunk.min(total - offset);
            let got =
                Self::read_bytes_chunk(sock, addr, extent_id, eversion, offset, want, payload)
                    .await?;
            if got.is_empty() {
                break;
            }
            let got_len = got.len() as u64;
            // Write this chunk, then it drops at the next loop iteration — peak
            // resident = one chunk.
            file_pwrite(dest_file.clone(), offset, Bytes::from(got))
                .await
                .map_err(|e| e.to_string())?;
            offset += got_len;
            on_progress(offset);
            if got_len < want {
                break; // short read — source has no more data
            }
        }
        Ok(offset)
    }

    pub(crate) async fn extent_info_from_manager(
        &self,
        extent_id: u64,
    ) -> Result<Option<ExtentInfo>, String> {
        let mgr = match &self.manager_endpoint {
            Some(ep) => crate::conn_pool::normalize_endpoint(ep),
            None => return Ok(None),
        };
        let req = manager_rpc::rkyv_encode(&manager_rpc::ExtentInfoReq { extent_id });
        // 5 s — read-only manager call. Hot in EC convert
        // (handle_convert_to_ec syncs sealed_length / eversion from
        // manager) and the recovery verify-after-fetch path.
        let resp_data = self
            .manager_pool
            .call_timeout(
                &mgr,
                autumn_rpc::manager_rpc::MSG_EXTENT_INFO,
                req,
                Duration::from_secs(5),
            )
            .await
            .map_err(|e| format!("extent_info rpc: {e}"))?;
        let resp: manager_rpc::ExtentInfoResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| format!("decode: {e}"))?;
        if resp.code != manager_rpc::CODE_OK {
            return Ok(None);
        }
        let loc = resp.payload_location;
        Ok(resp.extent.map(|e| mgr_to_local_extent(&e, loc)))
    }

    /// `node_id -> (base address, per-shard listener ports)`.
    ///
    /// The ports are NOT decoration. An EN runs `shard_count` listeners and a
    /// hot-path RPC must reach the shard that OWNS the extent
    /// (`autumn_rpc::shard_for_extent`, a hashed map — not `id % count`, which
    /// aliased bootstrap's contiguous ids onto shard 0). A read sent to the
    /// base address always lands on shard 0 and is refused with
    /// "extent N belongs to shard M not shard 0".
    async fn nodes_map_from_manager(
        &self,
    ) -> Result<HashMap<u64, (String, Vec<u16>)>, String> {
        let mgr = match &self.manager_endpoint {
            Some(ep) => crate::conn_pool::normalize_endpoint(ep),
            None => return Err("no manager endpoint configured".to_string()),
        };
        // 5 s — read-only manager call.
        let resp_data = self
            .manager_pool
            .call_timeout(
                &mgr,
                autumn_rpc::manager_rpc::MSG_NODES_INFO,
                Bytes::new(),
                Duration::from_secs(5),
            )
            .await
            .map_err(|e| format!("nodes_info rpc: {e}"))?;
        let resp: manager_rpc::NodesInfoResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| format!("decode: {e}"))?;
        if resp.code != manager_rpc::CODE_OK {
            return Err(format!("nodes_info failed: {}", resp.message));
        }
        Ok(resp
            .nodes
            .into_iter()
            .map(|(id, n)| (id, (n.address, n.shard_ports)))
            .collect())
    }

    async fn resolve_recovery_extent(
        &self,
        task: &crate::extent_rpc::RecoveryTask,
    ) -> Result<ExtentInfo, String> {
        self.extent_info_from_manager(task.extent_id)
            .await?
            .ok_or_else(|| format!("extent {} not found on manager", task.extent_id))
    }

    async fn run_recovery_task(
        &self,
        task: crate::extent_rpc::RecoveryTask,
    ) -> Result<RecoveryTaskDone, String> {
        let extent_info = self.resolve_recovery_extent(&task).await?;

        // refuse-at-start — if the local extent already has a fresher
        // eversion than the manager's snapshot, the recovery snapshot is stale.
        // Skip the expensive peer-copy and let the manager redispatch (the
        // caller's retry loop will re-resolve extent_info from manager on the
        // next attempt).
        if let Some(local) = self.extents.get(&task.extent_id) {
            let live_ev = local.eversion.load(Ordering::SeqCst);
            if live_ev > extent_info.eversion {
                return Err(format!(
                    "extent {} local eversion {} > recovery snapshot {}; skipping stale recovery",
                    task.extent_id, live_ev, extent_info.eversion
                ));
            }
        }

        // gate cross-extent recovery concurrency. Acquired AFTER
        // the cheap stale-snapshot check so a stale recovery
        // doesn't consume a permit. Held until the end of the function
        // via RAII (`_rec_permit`); released when the function returns
        // or unwinds. Each `run_recovery_task` peer-fetches ~payload
        // bytes then writes ~payload bytes (`payload × 2` transient
        // working set); a single `recovery_dispatch_loop` tick can
        // detect 6+ down extents and detached-spawn 6 concurrent
        // recoveries on the same survivor node, multiplying the peak
        // by N. Default parallelism=2 — repair work runs concurrently
        // for faster post-failure convergence. Env tunable via
        // `AUTUMN_EXTENT_RECOVERY_PARALLELISM` (clamped [1, 16]).
        let _rec_permit = self.concurrency_ctrl.acquire_recovery().await;

        // EC vs replication dispatch keys on `ec_converted` (set by the
        // manager's `apply_ec_conversion_done` after a sealed extent has
        // actually been RS-encoded). Pre-EC extents — including the open
        // extents the manager pre-allocates with parity slots in
        // `stream_alloc_extent` — are still full-replicated on every K+M
        // node, so they must take the replication path even though
        // `extent_info.parity` is non-empty.
        let extent = self.ensure_extent(task.extent_id).await?;

        // resolve (re-open if evicted) + pin the recovery dest fd
        // once for the writeback + sync below.
        let rf = self.extent_file(&extent).await?;
        let mut wrote_shard_file = false;
        let payload_len = if !extent_info.ec_converted && extent_info.sealed_length == 0 {
            // a sealed-EMPTY extent (`sealed_length == 0` — e.g.
            // an open tail that the fence drain rolled empty, then recovery
            // rebuilds its fenced slot) has NO bytes to copy: `ensure_extent`
            // already created the empty local file. SKIP the source read
            // entirely — a sealed-empty extent needs a 0-byte file marked
            // sealed, not a `length==0` read-to-end against a peer (a pure
            // waste that also exposes this recovery to source-side stalls).
            // Sets the sealed flag below via the same `sealed_length == 0` →
            // sealed path. This is the common shape in a fenced-node drain:
            // rolls the victim's open tails empty, then the
            // fenced-slot dispatch rebuilds them.
            0
        } else if !extent_info.ec_converted {
            // Replication recovery: stream the full extent from a
            // healthy peer chunk-by-chunk straight into the file (peak = one
            // FILE_IO_CHUNK_BYTES chunk), instead of materializing the whole
            // extent in a Vec then writing it back. stream_* truncates to 0 and
            // writes each chunk; succeeds only on a full sealed_length transfer.
            self.stream_extent_from_sources(&extent_info, &[task.node_id, task.replace_id], &extent)
                .await?
        } else {
            // EC recovery: reconstruct the missing shard stripe by stripe,
            // writing each stripe as it is produced. The destination file is
            // opened FIRST — which is why the shard index is computed here
            // rather than inside the rebuild: the layout decides which file
            // gets the bytes, and the rebuild needs somewhere to put them.
            //
            // The rebuilt shard goes back into the file the layout NAMES. On an
            // extent converted under the CoW scheme that is
            // `extent-{id}.shard{i}` — writing it into `.dat` would leave this
            // node serving shard bytes to anyone still asking for the whole
            // value, and leave the shard the layout points at missing. A
            // legacy converted extent (shard renamed over `.dat`) keeps its
            // old shape.
            let shard_index = Self::ec_shard_index(&extent_info, task.replace_id)?;
            if PayloadLocation::from_byte(extent_info.payload_location)
                == PayloadLocation::InShardFile
            {
                let disk = self.disk_for(extent.disk_id)?;
                let path = disk.shard_path(task.extent_id, shard_index as u32);
                if let Some(parent) = path.parent() {
                    compio::fs::create_dir_all(parent)
                        .await
                        .map_err(|e| format!("mkdir for rebuilt shard {}: {e}", task.extent_id))?;
                }
                let f = Rc::new(
                    OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(&path)
                        .await
                        .map_err(|e| format!("create rebuilt shard {}: {e}", task.extent_id))?,
                );
                // A failure mid-rebuild used to be free: the old code produced
                // the whole payload before touching the destination. Streaming
                // writes as it goes, so an error now leaves a PARTIAL shard
                // file. A retry truncates it and a reader rejects it by exact
                // length, so it cannot serve wrong bytes — but after a restart
                // `discover_shard_files` registers it at its partial length,
                // which makes `holds_payload` and the `df` accounting lie until
                // the next attempt truncates it (NOT until a reconcile sweep --
                // that loop skips the layout's own `want.shard_index`, which is
                // this one). Remove it on the way out.
                let len = match self
                    .stream_ec_recovery_payload(&task, &extent_info, shard_index, &f)
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        drop(f);
                        // Discarding it means BOTH halves. A `shard_files`
                        // record that predates this rebuild -- from restart
                        // discovery, or from a concurrent
                        // `write_shard_stripe_local` sharing this entry --
                        // would otherwise outlive the bytes just removed.
                        if let Err(ue) = extent.discard_shard_file(&path, shard_index as u32).await
                        {
                            // NOT the reconcile sweep: its stale-shard loop
                            // filters out `want.shard_index`, and for a rebuild
                            // that index IS the wanted one (both sides derive it
                            // as this node's position in `replicates ++ parity`).
                            // What actually clears a partial shard is the next
                            // attempt's `truncate(true)` open, or the manager
                            // reassigning the slot so placement GC takes it.
                            tracing::warn!(
                                extent_id = task.extent_id,
                                shard_index,
                                error = %ue,
                                "failed rebuild: could not unlink the partial shard \
                                 (the entry advertises it until the next attempt \
                                 truncates it)"
                            );
                        }
                        // The ENTRY is deliberately NOT dropped here, though
                        // leaving it is what wedges this (node, extent) pair:
                        // `require_recovery` refuses whenever an entry exists
                        // that it cannot classify, and for an ec_converted
                        // extent `try_adopt_completed_recovery` answers Unknown
                        // every time. Dropping it is still the wrong cure —
                        // `handle_write_shard` shares this entry, so an EC
                        // conversion that assigned this node as parity mid-
                        // rebuild (a case the manager documents) would lose the
                        // shard it just recorded, and `ec_stage_nonce` is the
                        // guard that refuses a superseded coordinator's write.
                        // Unwedging needs the EC arm of
                        // `try_adopt_completed_recovery` — see the ledger.
                        return Err(e);
                    }
                };
                f.sync_data().await.map_err(|e| e.to_string())?;
                self.fsync_staging_dir(task.extent_id, &path).await.map_err(|(_, m)| m)?;
                extent.note_shard_file(shard_index as u32, len);
                wrote_shard_file = true;
                len
            } else {
                rf.set_len(0).await.map_err(|e| e.to_string())?;
                self.stream_ec_recovery_payload(&task, &extent_info, shard_index, &rf)
                    .await?
            }
        };
        // The shard file was already synced by name; `.dat` is not this
        // extent's payload in that case and must not be truncated or synced.
        if !wrote_shard_file {
            rf.sync_data().await.map_err(|e| e.to_string())?;
        }

        // verify-after-sync — a concurrent apply_extent_meta_durable
        // (triggered by handle_re_avali or another append's seal-confirm branch)
        // may have bumped eversion/sealed_length/avali during the long peer-copy
        // await above. If the local eversion has advanced past the snapshot,
        // writing back the stale snapshot values would silently roll back the
        // fresher atomics. Return Err so the manager redispatches recovery
        // against the fresh seal point.
        let live_eversion = extent.eversion.load(Ordering::SeqCst);
        if live_eversion > extent_info.eversion {
            return Err(format!(
                "recovery for extent {} superseded by concurrent seal: local eversion {} > snapshot {}",
                task.extent_id, live_eversion, extent_info.eversion
            ));
        }

        extent.note_durable_install(payload_len);
        // Use fetch_max instead of store for eversion/sealed_length/avali so
        // that any concurrent atomic update that landed between the check and
        // these stores cannot be rolled back. Monotonic progress is guaranteed
        // even in the race window after the eversion check above.
        let _ = extent
            .eversion
            .fetch_max(extent_info.eversion, Ordering::SeqCst);
        let _ = extent
            .sealed_length
            .fetch_max(extent_info.sealed_length, Ordering::SeqCst);
        let _ = extent.avali.fetch_max(extent_info.avali, Ordering::SeqCst);
        // P0-C (coco review #3 issue 2): also sync the explicit `sealed` flag
        // MONOTONICALLY (true wins). A recovered sealed-EMPTY extent
        // (sealed=true, sealed_length=0) would otherwise keep the fresh
        // ExtentEntry's sealed=false, and the save_meta below — which writes
        // `entry.sealed || sealed_length>0` — would persist it as OPEN, letting
        // a restart accept ghost writes to a manager-sealed / CoW-shared tail.
        if extent_info.sealed || extent_info.sealed_length > 0 {
            extent.sealed.store(true, Ordering::SeqCst);
        }

        // P0-D (coco durability batch): the `.meta` persist is PART of the
        // recovered replica — eversion / sealed_length / sealed / avali are
        // what a restart trusts. Swallowing a persist failure reported the
        // recovery as DONE while the on-disk sidecar still carried the
        // pre-recovery state: after a crash the replica re-announces the old
        // eversion (manager believes the slot recovered at the new one) and a
        // manager-sealed extent can read back as OPEN. Fail-closed instead:
        // the recovery task FAILS (the dispatch loop retries it) and the disk
        // is marked offline, the established response to a sidecar-persist
        // I/O error (see `ensure_fence_durable`, note 23).
        if let Err(e) = self.save_meta(task.extent_id, &extent).await {
            // Mark the disk offline FIRST (the lookup needs the entry), then
            // REMOVE the partial entry (coco P1): leaving it in `extents`
            // would (a) make local retries reuse the now-offline disk via
            // `ensure_extent`'s existing-entry fast path — a later lucky
            // persist would then report a "recovered" replica on an offline
            // disk — and (b) block a future manager re-dispatch with
            // "extent already exists". The orphaned .dat is reaped by the
            // startup/periodic reconcile, the established path
            // for abandoned recovery artifacts.
            self.mark_disk_error_for_extent(task.extent_id, &e.to_string());
            self.extents.remove(&task.extent_id);
            self.fd_lru.forget(task.extent_id);
            self.ec_stage_nonce.remove(&task.extent_id);
            return Err(format!(
                "recovery of extent {} completed but .meta persist failed (fail-closed): {e}",
                task.extent_id
            ));
        }

        Ok(RecoveryTaskDone {
            task,
            ready_disk_id: extent.disk_id,
        })
    }

/// How many bytes one EC shard read should ask for.
///
/// `0` means "read to end" — one request for the whole shard. That is what this path used to
/// pass, on the stated assumption that a shard is "well under the chunking
/// threshold". It is not: a shard is `ceil(sealed_length / K)`, so a full
/// 17 GiB extent at K=4 gives ~4 GiB against a `FILE_IO_CHUNK_BYTES` of
/// 256 MiB.
///
/// On the live cluster every peer read FAILED — deterministically, in under a
/// second, on four extents, with the shards present, the layout correct and
/// every peer reachable. Sub-second rules OUT the 30 s per-request timeout,
/// which is what the size first suggested. The likely mechanism is the frame
/// encoder's unguarded `payload_len as u32`: that shard is 4,294,996,716
/// bytes = `u32::MAX + 29,421`, so the length wraps to ~29 KB, the peer ships
/// a header that disagrees with the body, and the reader fails the CRC at
/// once. Both mechanisms are cured the same way, and neither can be confirmed
/// from the error this path used to produce.
///
/// Returning the exact length puts the read on the chunking loop, where every
/// request is <= `FILE_IO_CHUNK_BYTES` — far from the u32 edge and inside the
/// timeout budget. It MUST equal `erasure::shard_size` — the length the
/// encoder actually wrote, padding included — or the reader and writer
/// disagree about shard geometry.
pub(crate) fn ec_shard_read_len(sealed_length: u64, data_shards: usize) -> u64 {
    if sealed_length == 0 || data_shards == 0 {
        // Unreachable in production — convert refuses an unsealed extent and
        // the manager only ever sets `ec_converted` with K >= 1 — so this is
        // the caller's cue to fail loudly, not a shape to read to end.
        return 0;
    }
    crate::erasure::shard_size(sealed_length as usize, data_shards) as u64
}

    /// Which shard slot this recovery is rebuilding.
    ///
    /// Split out of the reconstruct so the caller can pick the destination
    /// FILE before any bytes are read — the streaming rebuild writes each
    /// stripe as it is produced, so the file has to exist first, and which
    /// file it is depends on this index.
    fn ec_shard_index(extent_info: &ExtentInfo, replace_id: u64) -> Result<usize, String> {
        extent_info
            .replicates
            .iter()
            .chain(extent_info.parity.iter())
            .position(|&id| id == replace_id)
            .ok_or_else(|| format!("replace_id {replace_id} not found in the extent's node list"))
    }

    /// Per-shard bytes reconstructed per round.
    ///
    /// Peak memory is `(K + 1) * this` — K stripes read from peers plus the
    /// one rebuilt — instead of `(K + 1) * shard_size`, which on a full 17 GiB
    /// extent at K=4 is ~20 GiB against an EN pod that requests 1 GiB. RS over
    /// GF(256) is byte-wise per offset, so a stripe reconstructs from the SAME
    /// byte range of its peers with no dependence on the rest of the shard —
    /// `ec_encode_stripe_matches_whole` is the proof of that for the encode
    /// direction, and `ec_reconstruct_shard` is length-agnostic.
    ///
    /// 64 MiB matches the stripe the EC CONVERSION already writes with
    /// (`stripe_bytes` in its 2PC prepare), so the read path now moves in the
    /// same units as the write path.
    /// Overridable so a test can drive several stripes without a >128 MiB
    /// payload — the encode stripe has the same shape for the same reason.
    fn ec_recovery_stripe_bytes() -> u64 {
        static CELL: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
        *CELL.get_or_init(|| {
            std::env::var("AUTUMN_EXTENT_EC_RECOVERY_STRIPE_BYTES")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(64 * 1024 * 1024)
        })
    }

    /// The `(offset, span)` sequence a streaming rebuild walks.
    ///
    /// Pulled out of the loop so the arithmetic is testable without a peer: it
    /// must cover `[0, want)` exactly once, contiguously, with the last stripe
    /// carrying the misaligned tail. An off-by-one here does not fail loudly —
    /// a stalled offset rewrites stripe 0 forever and a skipped one leaves a
    /// hole — and the bytes still LOOK like a shard.
    fn ec_stripe_plan(want: u64, stripe: u64) -> Vec<(u64, u64)> {
        let mut out = Vec::new();
        if want == 0 || stripe == 0 {
            return out;
        }
        let mut offset = 0u64;
        while offset < want {
            let span = stripe.min(want - offset);
            out.push((offset, span));
            offset += span;
        }
        out
    }

    /// Rebuild an EC shard by streaming: for each stripe, read that byte range
    /// from `data_shards` healthy peers, reconstruct the missing range, write
    /// it, drop it.
    ///
    /// It used to collect every peer's WHOLE shard into memory, reconstruct
    /// once and hand the result back for the caller to write. Peak was
    /// `(K + 1) * shard_size` — ~20 GiB on a full 17 GiB extent at K=4, times
    /// `--recovery-parallelism`, on an EN pod that requests 1 GiB. Streaming
    /// makes it `(K + 1) * EC_RECOVERY_STRIPE_BYTES`.
    ///
    /// This is sound because RS over GF(256) is byte-wise per offset: a
    /// stripe's reconstruction depends only on the SAME byte range of its
    /// peers. `ec_encode_stripe_matches_whole` pins that for the encode
    /// direction and `ec_reconstruct_shard` is length-agnostic — it takes
    /// whatever equal-length slices it is given.
    ///
    /// Peer selection is per stripe, not once up front, so a peer that dies
    /// mid-rebuild is routed around exactly as it would have been on the first
    /// stripe, rather than failing the whole shard.
    async fn stream_ec_recovery_payload(
        &self,
        task: &crate::extent_rpc::RecoveryTask,
        extent_info: &ExtentInfo,
        shard_index: usize,
        dst: &Rc<compio::fs::File>,
    ) -> Result<u64, String> {
        let data_shards = extent_info.replicates.len();
        let parity_shards = extent_info.parity.len();
        let n = data_shards + parity_shards;

        let all_node_ids: Vec<u64> = extent_info
            .replicates
            .iter()
            .chain(extent_info.parity.iter())
            .copied()
            .collect();

        // Exact shard length, and the reader's expectation of every peer. `0`
        // cannot happen for a converted extent (convert refuses an unsealed
        // one, and the manager sets `ec_converted` only with K >= 1), so it
        // means the manager's record and this extent disagree.
        let want = Self::ec_shard_read_len(extent_info.sealed_length, data_shards);
        if want == 0 {
            return Err(format!(
                "EC recovery: extent {} is ec_converted with sealed_length={} and K={} \
                 — manager state inconsistent",
                task.extent_id, extent_info.sealed_length, data_shards
            ));
        }

        let nodes = self.nodes_map_from_manager().await?;

        for (offset, span) in Self::ec_stripe_plan(want, Self::ec_recovery_stripe_bytes()) {
            let mut shards: Vec<Option<Vec<u8>>> = vec![None; n];
            let mut collected = 0usize;
            // Per-peer failure reasons for THIS stripe. A bare `k/K` cannot say
            // whether a peer was unreachable, rejected the eversion, or
            // answered short, and those want opposite fixes.
            let mut why: Vec<String> = Vec::new();

            for (i, &node_id) in all_node_ids.iter().enumerate() {
                if i == shard_index || node_id == task.node_id {
                    continue;
                }
                let Some((base, shard_ports)) = nodes.get(&node_id) else {
                    why.push(format!("shard {i} (node {node_id}): not in the manager's node map"));
                    continue;
                };
                // Route to the shard that OWNS the extent. Addressing the base
                // port sends every read to shard 0, which the peer refuses with
                // "belongs to shard M not shard 0" -- the whole rebuild then
                // reports 0/K with every peer healthy and the shards on disk.
                let routed = shard_addr_for_extent(base, shard_ports, task.extent_id);
                let addr = &routed;
                let sock = match parse_addr(addr) {
                    Ok(v) => v,
                    Err(e) => {
                        why.push(format!("shard {i} (node {node_id} at {addr}): {e}"));
                        continue;
                    }
                };
                match Self::read_bytes_chunk(
                    sock,
                    addr,
                    task.extent_id,
                    extent_info.eversion,
                    offset,
                    span,
                    PayloadRef::for_extent(extent_info.payload_location, i as u32),
                )
                .await
                {
                    // EXACT length. A short read is not loud on this path: the
                    // server answers CODE_OK short, and K stripes short by the
                    // SAME amount reconstruct without complaint — the RS
                    // decoder only rejects shards of DIFFERING length — so a
                    // truncated stripe would be written back as authoritative.
                    Ok(b) if b.len() as u64 == span => {
                        shards[i] = Some(b);
                        collected += 1;
                        if collected >= data_shards {
                            break;
                        }
                    }
                    Ok(b) => why.push(format!(
                        "shard {i} (node {node_id} at {addr}): got {} of {span} bytes",
                        b.len()
                    )),
                    Err(e) => why.push(format!("shard {i} (node {node_id} at {addr}): {e}")),
                }
            }

            if collected < data_shards {
                return Err(format!(
                    "EC recovery: only {collected}/{data_shards} shards available for extent {} \
                     at [{offset}, {}) of {want}: {}",
                    task.extent_id,
                    offset + span,
                    if why.is_empty() { "no peers attempted".to_string() } else { why.join("; ") }
                ));
            }

            let rebuilt = compio::runtime::spawn_blocking(move || {
                crate::erasure::ec_reconstruct_shard(shards, data_shards, parity_shards, shard_index)
            })
            .await
            .map_err(|_| "EC reconstruct task panicked".to_string())?
            .map_err(|e| format!("EC reconstruct failed: {e}"))?;

            if rebuilt.len() as u64 != span {
                return Err(format!(
                    "EC recovery: reconstructed {} bytes for a {span}-byte stripe of extent {}",
                    rebuilt.len(),
                    task.extent_id
                ));
            }
            file_pwrite_chunked(dst.clone(), offset, Bytes::from(rebuilt))
                .await
                .map_err(|e| e.to_string())?;
        }

        Ok(want)
    }

    /// 2PC Phase 1 (prepare): write a single EC shard to a staging file
    /// (`extent-{id}.ec.dat`), preserving the original `.dat` intact.
    /// Called by both the coordinator (for its own shard) and the
    /// WriteShard RPC handler (for remote shards).
    ///
    /// The original data file is untouched — reads continue to serve
    /// the full replica until Phase 2 (`commit_shard_local`) renames
    /// the staging file over it. If the process crashes after prepare
    /// but before commit, the staging file is cleaned up on startup
    /// and the original data remains intact for a retry.
    /// Write one shard STRIPE into the staging `.ec.dat` at `shard_offset`
    /// (chunked EC convert). The shard is streamed as a sequence of stripes so
    /// no single WriteShard RPC exceeds the frame `payload_len: u32` ceiling —
    /// load-bearing once a shard can exceed 4 GiB. `shard_offset = 0` with the
    /// whole shard as `stripe_data` is the degenerate single-stripe form.
    ///
    /// Crash-safety: each stripe is `pwrite`'d at its offset and `sync_data`'d
    /// before the caller's ACK, so the durable prefix grows monotonically as
    /// the coordinator streams stripes sequentially (await-ack per stripe). The
    /// staging is renamed over `.dat` only by `commit_shard_local`, which the
    /// coordinator sends ONLY after every stripe acked (coordinator writes its
    /// OWN shard last, so coord-staging-full ⇒ all participants durably staged).
    /// `pwrite`-at-offset is idempotent, so a retry that re-streams from 0
    /// rewrites the same bytes at the same offsets. No truncate: stripes from
    /// different offsets coexist; the file grows to `shard_size` at the last.
    /// Publish how far an extent-scoped op has got, for the next `df` to carry.
    /// Overwrite, not append: only the newest sample is worth sending.
    fn note_op_progress(&self, extent_id: u64, kind: u8, done: u64, total: u64) {
        self.op_progress.insert(extent_id, (kind, done, total));
    }

    /// Stop reporting progress for an op that ended, whichever way it ended.
    /// A stale sample would leave `ops status` showing a repair frozen at 60%
    /// long after it finished or was abandoned.
    fn clear_op_progress(&self, extent_id: u64) {
        self.op_progress.remove(&extent_id);
    }

    /// Claim this node's EC staging for `extent_id` on behalf of `attempt_nonce`,
    /// or refuse if a NEWER attempt already claimed it.
    ///
    /// Two things depend on the claim being recorded, which is why BOTH staging
    /// paths call this and not just the RPC one:
    ///  - attempt ordering, so a released-but-alive coordinator cannot
    ///    interleave its stripes with its successor's into the same file (the
    ///    `owner_epoch` fence only fires when the ex-coordinator was FENCED,
    ///    not when its marker was merely released);
    ///  - the reconcile guard, which skips a cleanup verdict that says `.dat`
    ///    while an attempt has staged shards here. Unclaimed staging is
    ///    invisible to it, so a stale verdict deletes a shard being written.
    ///
    /// Nonce 0 = a pre-nonce peer: left unordered rather than blocked.
    /// Callers must hold this extent's op lock so compare-and-record is atomic
    /// against a concurrent stripe.
    fn claim_ec_staging(&self, extent_id: u64, attempt_nonce: u64) -> bool {
        let seen = self.ec_stage_nonce.get(&extent_id).map(|v| v.nonce);
        // SEALED wins over everything, including the nonce-0 pass-through: once
        // the layout is flipped the staged file IS the live shard, so there is
        // no such thing as a legitimate stripe for it any more.
        if seen == Some(EC_STAGING_SEALED) {
            return false;
        }
        if attempt_nonce == 0 {
            // A pre-nonce peer is left UNORDERED — but it is still staging, and
            // the reconcile guard reads the mark's TICK, not its nonce. Stamp
            // one so this staging is visible as recent, carrying whatever floor
            // a nonced attempt already established so the pass-through cannot
            // lower it. Without the stamp a nonce-0 attempt that starts after a
            // verdict was asked for looks like it never staged at all.
            let floor = seen.unwrap_or(0);
            self.ec_stage_nonce
                .insert(extent_id, self.stamp_stage_mark(floor));
            return true;
        }
        if let Some(seen) = seen {
            if attempt_nonce < seen {
                return false;
            }
        }
        self.ec_stage_nonce.insert(extent_id, self.stamp_stage_mark(attempt_nonce));
        true
    }

    /// Stamp a staging mark with the next tick.
    ///
    /// Every write to `ec_stage_nonce` goes through here, because a mark that
    /// kept an older tick would read as "staged long ago" and could let a
    /// reconcile verdict delete a shard an attempt is writing right now.
    fn stamp_stage_mark(&self, nonce: u64) -> EcStageMark {
        let tick = self.ec_stage_tick.get() + 1;
        self.ec_stage_tick.set(tick);
        EcStageMark { nonce, tick }
    }

    /// Mark this extent's staging CLOSED: the manager flipped the layout, so
    /// the file every attempt was staging into is now live data.
    fn seal_ec_staging(&self, extent_id: u64) {
        self.ec_stage_nonce
            .insert(extent_id, self.stamp_stage_mark(EC_STAGING_SEALED));
    }

    async fn write_shard_stripe_local(
        &self,
        extent_id: u64,
        shard_index: usize,
        shard_offset: u64,
        sealed_length: u64,
        _new_eversion: u64,
        stripe_data: Bytes,
    ) -> Result<(), (StatusCode, String)> {
        let entry = self
            .ensure_extent(extent_id)
            .await
            .map_err(|e| (StatusCode::Internal, e))?;

        let disk = self
            .disk_for(entry.disk_id)
            .map_err(|e| (StatusCode::Internal, e))?;
        // The shard is an ADDITIVE file named by its index — `.dat` is never
        // touched, so nothing has to be undone if this attempt is abandoned,
        // and a shard staged for one index can never be served as another.
        let staging_path = disk.shard_path(extent_id, shard_index as u32);
        let stripe_len = stripe_data.len();

        // coco P1 bounds guard: a malformed / stale WriteShard with a huge
        // `shard_offset` must not create an oversized sparse `.ec.dat` that a
        // later commit would publish as `.dat` (finish_ec_commit sets
        // entry.len/sealed_length from the file size). Every legitimate stripe
        // ends at most at `shard_size = ceil(sealed_length/K) <= sealed_length`
        // (for any K >= 1), so `shard_offset + stripe_len <= sealed_length` is a
        // K-free upper bound that never rejects a valid stripe but caps the
        // staging file at `sealed_length` — keeping finish_ec_commit's
        // `sealed_length.max(shard_len)` from being polluted. (A tight
        // `<= ceil(sealed_length/K)` bound would need `data_shards` on the wire;
        // the loose bound is enough to stop the egregious sparse-file case.)
        let stripe_end = shard_offset.checked_add(stripe_len as u64).ok_or_else(|| {
            (
                StatusCode::InvalidArgument,
                format!(
                    "write_shard {extent_id}: shard_offset {shard_offset} + len {stripe_len} \
                         overflows u64"
                ),
            )
        })?;
        if stripe_end > sealed_length {
            return Err((
                StatusCode::InvalidArgument,
                format!(
                    "write_shard {extent_id}: stripe end {stripe_end} exceeds sealed_length \
                     {sealed_length} (malformed shard_offset {shard_offset})"
                ),
            ));
        }

        if let Some(parent) = staging_path.parent() {
            compio::fs::create_dir_all(parent).await.map_err(|e| {
                (
                    StatusCode::Internal,
                    format!("mkdir for staging {extent_id}: {e}"),
                )
            })?;
        }

        // Stripe 0 TRUNCATES; later stripes must NOT (earlier stripes of THIS
        // attempt have to survive the open).
        //
        // The staging file carries no attempt identity, so without this a
        // previous attempt's `.ec.dat` survives into the next one: a reissue
        // with a different K leaves attempt #1's tail bytes past attempt #2's
        // shard end, and `finish_ec_commit` derives the published length from
        // the FILE SIZE — so the commit would publish a `.dat` longer than the
        // real shard (a to-end shard read then returns an over-long shard and
        // EC reconstruct fails). Truncating at the first stripe makes each
        // attempt's staging exactly its own bytes. Same-K re-prepare was only
        // ever safe because RS encode is deterministic; do not rely on that.
        // Stripe 0 creates (and truncates); a LATER stripe must find the file
        // already there. If it does not, this attempt's staging was removed out
        // from under it — recreating it here would silently leave zero holes
        // where the earlier stripes were, and the flip would publish that as
        // this node's shard. Fail the stripe; the coordinator restarts the
        // attempt from stripe 0.
        if shard_offset > 0 && compio::fs::metadata(&staging_path).await.is_err() {
            let msg = format!(
                "write_shard {extent_id}/{shard_index}: staging file vanished before \
                 stripe @{shard_offset} — this attempt's staging was clobbered; refusing to \
                 recreate it with holes"
            );
            tracing::error!("{msg}");
            return Err((StatusCode::FailedPrecondition, msg));
        }
        let staging_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(shard_offset == 0)
            .open(&staging_path)
            .await
            .map_err(|e| {
                let msg = format!("create staging {extent_id}: {e}");
                self.mark_disk_error_for_extent(extent_id, &msg);
                (StatusCode::Internal, msg)
            })?;

        // staging file is local to this function — the path is unique per
        // `extent_id` and EC convert on this extent is serialised by the
        // per-extent op-lock, so a freshly-created `Rc` suffices.
        let staging_rc = Rc::new(staging_file);
        // ENOSPC-1: EC staging writes mark the disk like every other write path.
        file_pwrite_chunked(staging_rc.clone(), shard_offset, stripe_data)
            .await
            .map_err(|e| {
                let msg = format!("write staging {extent_id}/{shard_index}@{shard_offset}: {e}");
                self.mark_disk_error_for_extent(extent_id, &msg);
                (StatusCode::Internal, msg)
            })?;
        staging_rc.sync_data().await.map_err(|e| {
            let msg = format!("sync staging {extent_id}: {e}");
            self.mark_disk_error_for_extent(extent_id, &msg);
            (StatusCode::Internal, msg)
        })?;
        // EC-PREPARE-DURABLE: `sync_data` makes the stripe CONTENT durable; the
        // parent-dir fsync makes the staging dirent durable (idempotent across
        // stripes — only the first stripe actually creates the file).
        self.fsync_staging_dir(extent_id, &staging_path).await?;

        // Publish the file to this node's own view: `holds_payload` must say
        // yes before the layout flip can send a reader here, and `df` must
        // count these bytes (the node now holds `.dat` AND a shard).
        let known = entry
            .shard_files
            .borrow()
            .get(&(shard_index as u32))
            .copied()
            .unwrap_or(0);
        entry.note_shard_file(shard_index as u32, known.max(stripe_end));

        tracing::debug!(
            extent_id,
            shard_index,
            shard_offset,
            stripe_len,
            sealed_length,
            "EC prepare: shard stripe written to its shard file"
        );
        Ok(())
    }

    /// EC-PREPARE-DURABLE: fsync the parent directory of an EC staging file so
    /// its directory entry is durable. POSIX does not persist a new file's
    /// NAME on a host crash from a content `sync_data` alone — only an fsync of
    /// the PARENT directory does. The 2PC commit doc promises `.ec.dat`
    /// "persists as a durable prepare record" across a crash-before-rename;
    /// without this a power loss could drop the dirent → commit retry finds the
    /// staging missing → the participant is stuck. Mirrors the `.meta`
    /// tmp→rename→parent-dir-fsync pattern in `write_meta_locked` (P0-B). Every
    /// prepare path that returns Ok calls this so the guarantee is uniform.
    async fn fsync_staging_dir(
        &self,
        extent_id: u64,
        staging_path: &std::path::Path,
    ) -> Result<(), (StatusCode, String)> {
        if let Some(dir) = staging_path.parent() {
            compio::fs::File::open(dir)
                .await
                .map_err(|e| {
                    let msg = format!("open staging dir {extent_id}: {e}");
                    self.mark_disk_error_for_extent(extent_id, &msg);
                    (StatusCode::Internal, msg)
                })?
                .sync_all()
                .await
                .map_err(|e| {
                    let msg = format!("fsync staging dir {extent_id}: {e}");
                    self.mark_disk_error_for_extent(extent_id, &msg);
                    (StatusCode::Internal, msg)
                })?;
        }
        Ok(())
    }

    /// 2PC Phase 2 (commit): atomically rename the staging file
    /// (`extent-{id}.ec.dat`) over the original data file (`.dat`),
    /// reopen the file handle, bump eversion, and persist metadata.
    ///
    /// After this call, the node serves shard data on reads. POSIX
    /// guarantees the rename is atomic — either the old or new file
    /// is visible, never a partial state. If the process crashes
    /// before rename, `.ec.dat` persists as a durable prepare record
    /// and the original `.dat` is intact; the manager's retry will
    /// re-send CommitEcShard to complete the conversion.
        /// #5: complete an EC commit on `entry` — rename `.ec.dat`→`.dat` if the
    /// staging file is still present (else `.dat` is already the shard from a
    /// pre-crash rename), reopen, set the post-EC atomics, and persist `.meta`.
    /// Shared by `commit_shard_local` (normal path) and the `load_extents`
    /// marker replay (crash recovery), so both produce the identical state.
        /// #5: write the EC commit-intent marker durably (tmp→sync→rename→dir-fsync).
    /// Payload = `[new_eversion: u64 LE][sealed_length: u64 LE]`.
    /// Record that a FULL prepare for `new_eversion` completed (coordinator
    /// stages itself LAST, so this also asserts every participant is staged).
    /// Payload = `[new_eversion: u64 LE][attempt_nonce: u64 LE]` — the nonce is
    /// what makes the record attempt-scoped, since `new_eversion` repeats
    /// across a released-and-reissued attempt.
    /// Best-effort by contract: on failure the caller just loses the skip
    /// optimisation and re-prepares, which is always safe.
    async fn write_ec_prepared_marker(
        &self,
        disk: &DiskFS,
        extent_id: u64,
        new_eversion: u64,
        attempt_nonce: u64,
    ) -> Result<(), String> {
        let path = disk.ec_prepared_marker_path(extent_id);
        if let Some(parent) = path.parent() {
            compio::fs::create_dir_all(parent)
                .await
                .map_err(|e| format!("mkdir ec prepared marker {extent_id}: {e}"))?;
        }
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .await
            .map_err(|e| format!("create ec prepared marker {extent_id}: {e}"))?;
        let f = Rc::new(f);
        let mut payload = [0u8; 16];
        payload[0..8].copy_from_slice(&new_eversion.to_le_bytes());
        payload[8..16].copy_from_slice(&attempt_nonce.to_le_bytes());
        file_pwrite_chunked(f.clone(), 0, Bytes::copy_from_slice(&payload))
            .await
            .map_err(|e| format!("write ec prepared marker {extent_id}: {e}"))?;
        f.sync_data()
            .await
            .map_err(|e| format!("sync ec prepared marker {extent_id}: {e}"))?;
        if let Some(dir) = path.parent() {
            compio::fs::File::open(dir)
                .await
                .map_err(|e| format!("open prepared-marker dir {extent_id}: {e}"))?
                .sync_all()
                .await
                .map_err(|e| format!("fsync prepared-marker dir {extent_id}: {e}"))?;
        }
        Ok(())
    }

    /// The `(new_eversion, attempt_nonce)` recorded by the last completed
    /// prepare, if any. Absent / short / unreadable ⇒ `None` ⇒ the caller
    /// re-prepares (safe: prepare is deterministic and truncates its staging at
    /// stripe 0). A pre-nonce 8-byte marker left by an older binary reads as
    /// short here, so an upgraded node re-prepares once rather than trusting a
    /// record whose attempt it cannot identify.
    async fn read_ec_prepared_marker(&self, disk: &DiskFS, extent_id: u64) -> Option<(u64, u64)> {
        let path = disk.ec_prepared_marker_path(extent_id);
        let f = compio::fs::File::open(&path).await.ok()?;
        let buf = vec![0u8; 16];
        let res = f.read_exact_at(buf, 0).await;
        let bytes = res.1;
        if res.0.is_err() || bytes.len() < 16 {
            return None;
        }
        Some((
            u64::from_le_bytes(bytes[0..8].try_into().ok()?),
            u64::from_le_bytes(bytes[8..16].try_into().ok()?),
        ))
    }

        /// #5: read the EC commit-intent marker, distinguishing the three states the
    /// recovery decision needs (coco P2 #3 — mirror the `.meta` NotFound-vs-
    /// corrupt fail-closed policy; a present-but-unreadable marker must NOT be
    /// silently treated as "no marker").
        /// #5: delete the EC commit-intent marker (best-effort + dir-fsync). A
    /// leftover marker only causes a redundant, idempotent replay next restart.
        // ─── RPC Handlers ────────────────────────────────────────────────────────

    async fn handle_append(&self, payload: Bytes) -> HandlerResult {
        let req =
            AppendReq::decode(payload).map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        let extent = self.get_extent(req.extent_id).await?;

        // META-FAILCLOSED: refuse on a quarantined extent (corrupt `.meta` at
        // load). See build_append_future step 0 + load_extents.
        if extent.corrupt_meta.load(Ordering::SeqCst) {
            return append_reject(CODE_PRECONDITION);
        }

        // Only fetch from manager when local eversion is behind what the client expects.
        // In the common case (eversions match) we trust local atomics -- no RPC needed.
        let local_eversion = extent.eversion.load(Ordering::SeqCst);
        if req.eversion > local_eversion {
            // TODO: manager RPC for eversion refresh not yet implemented
            match self.extent_info_from_manager(req.extent_id).await {
                Ok(Some(ex)) => {
                    // fsync on 0→sealed transition so the
                    // sealed prefix is durable on this node before we
                    // surface the seal upstream.
                    // P0-A: propagate a seal-persist failure (disk now offline)
                    // instead of proceeding with the append.
                    self.apply_extent_meta_durable(req.extent_id, &extent, &ex)
                        .await
                        .map_err(|e| {
                            (
                                StatusCode::Unavailable,
                                format!("seal not durable for extent {}: {e}", req.extent_id),
                            )
                        })?;
                }
                Ok(None) => {
                    // Manager unreachable but we know local state is stale -- reject.
                    return Err((
                        StatusCode::Unavailable,
                        format!(
                            "cannot verify extent {} version: manager unreachable",
                            req.extent_id
                        ),
                    ));
                }
                Err(_) => {
                    return Err((
                        StatusCode::Unavailable,
                        format!(
                            "cannot verify extent {} version: manager unreachable",
                            req.extent_id
                        ),
                    ));
                }
            }
        }

        // Validate eversion and sealed state from local atomics.
        let local_eversion = extent.eversion.load(Ordering::SeqCst);
        if local_eversion > req.eversion {
            return append_reject(CODE_PRECONDITION);
        }
        if extent.sealed.load(Ordering::SeqCst)
            || extent.sealed_length.load(Ordering::SeqCst) > 0
            || extent.avali.load(Ordering::SeqCst) > 0
        {
            return append_reject(CODE_PRECONDITION);
        }

        let owner_epoch = extent.owner_epoch.load(Ordering::SeqCst);
        if req.owner_epoch < owner_epoch {
            return append_reject(CODE_LOCKED_BY_OTHER);
        }
        // P0-B durable fence (same as build_append_future): raise the in-memory
        // bar synchronously, then require the fence to be DURABLE before we ACK.
        // Fail-closed on persist error. See build_append_future / ensure_fence_durable.
        if req.owner_epoch > owner_epoch {
            extent
                .owner_epoch
                .fetch_max(req.owner_epoch, Ordering::SeqCst);
        }
        if let Err(e) = self
            .ensure_fence_durable(req.extent_id, &extent, req.owner_epoch)
            .await
        {
            self.mark_disk_error_for_extent(req.extent_id, &e.to_string());
            tracing::error!(
                extent_id = req.extent_id,
                error = %e,
                "P0-B: durable fence persist failed — rejecting append (fail-closed)"
            );
            return append_reject(CODE_PRECONDITION);
        }
        // P0-B: re-check fencing after the (possibly awaiting) durable step —
        // a higher owner_epoch may have taken over (LockedByOther), or a concurrent
        // seal/EC may have SEALED the extent during the await (CODE_PRECONDITION,
        // mirrors the post-truncate recheck). owner_epoch and sealed are checked SEPARATELY.
        if req.owner_epoch < extent.owner_epoch.load(Ordering::SeqCst) {
            return append_reject(CODE_LOCKED_BY_OTHER);
        }
        if extent.sealed.load(Ordering::SeqCst)
            || extent.sealed_length.load(Ordering::SeqCst) > 0
            || extent.avali.load(Ordering::SeqCst) > 0
        {
            return append_reject(CODE_PRECONDITION);
        }

        let mut start = extent.len.load(Ordering::SeqCst);
        if start < req.commit {
            return append_reject(CODE_PRECONDITION);
        }
        if start > req.commit {
            // confirm with the manager that this extent is NOT
            // sealed before truncating. Otherwise a stale-PS append with
            // a low `header.commit` would silently shrink an extent the
            // manager has already sealed (the seal isn't pushed to
            // extent_nodes — there's no etcd watch — so a stale client's
            // eversion check passes and we'd fall through to truncate).
            // The lost bytes between `req.commit` and the prior file
            // length are unrecoverable: the on-disk shards from the
            // subsequent EC pass would encode `req.commit` bytes while
            // the manager still believes `sealed_length` was the larger
            // value, miscomputing every cross-shard read boundary
            // afterwards (surfaced as `invalid meta_len=...` /
            // `logStream value short`). The manager round-trip on this
            // path is acceptable because commit-reconciliation
            // truncation is rare in normal operation (only fires when
            // this replica got ahead of the consensus min).
            if let Ok(Some(mgr_info)) = self.extent_info_from_manager(req.extent_id).await {
                // P0-C: explicit `sealed` flag (catches sealed-empty), not
                // `sealed_length > 0`.
                if mgr_info.sealed || mgr_info.sealed_length > 0 {
                    // fsync as part of accepting the seal —
                    // see apply_extent_meta_durable for why.
                    // P0-A: a persist failure marks the disk offline (recovery)
                    // inside; the stale append is still rejected as
                    // CODE_PRECONDITION, so log + proceed to the rejection.
                    if let Err(e) = self
                        .apply_extent_meta_durable(req.extent_id, &extent, &mgr_info)
                        .await
                    {
                        tracing::error!(extent_id = req.extent_id, error = %e, "P0-A: seal not durable during commit-reconcile reject (disk offline)");
                    }
                    return append_reject(CODE_PRECONDITION);
                }
            }
            Self::truncate_to_commit(&extent, req.commit)
                .await
                .map_err(|e| (StatusCode::Internal, e))?;
            // re-check seal state after the truncate await (symmetric
            // to the recheck in build_append_future). A concurrent
            // apply_extent_meta_durable (from handle_re_avali or another
            // handle_append's pre-truncate seal-confirm branch) may have landed
            // a fresh seal DURING the truncate I/O. Without this re-check the
            // subsequent file_pwrite would write bytes past sealed_length —
            // corrupting subsequent reads as "logStream value short" or
            // out-of-bounds slice panics on EC reads.
            if extent.sealed.load(Ordering::SeqCst)
                || extent.sealed_length.load(Ordering::SeqCst) > 0
                || extent.avali.load(Ordering::SeqCst) > 0
            {
                return append_reject(CODE_PRECONDITION);
            }
            // Re-check the owner_epoch fence after the truncate await too: a
            // concurrent eager takeover MSG_FENCE_EXTENT may have raised the floor
            // during the manager RTT + truncate fsync. See the symmetric re-check
            // in build_append_future.
            if req.owner_epoch < extent.owner_epoch.load(Ordering::SeqCst) {
                return append_reject(CODE_LOCKED_BY_OTHER);
            }
            start = extent.len.load(Ordering::SeqCst);
        }

        let data_payload = req.payload;

        // resolve + pin the fd once (reject if concurrently
        // sealed+evicted); `af` is held through the pwrite AND the
        // `register_sync_waiter` below, so `fd_evictable`'s `strong_count == 1`
        // keeps the extent non-evictable across the whole durable-append window.
        let Some(af) = extent.resident_file() else {
            return Err((
                StatusCode::FailedPrecondition,
                "extent sealed (fd evicted) — retry on a fresh tail".to_string(),
            ));
        };
        if let Err(e) = file_pwrite(af.clone(), start, data_payload.clone()).await {
            let msg = e.to_string();
            self.mark_disk_error_for_extent(req.extent_id, &msg);
            return Err((StatusCode::Internal, msg));
        }
        let start_offset = start;
        let end = start + data_payload.len() as u64;
        // every append is durable: fsync INLINE (the coalescer was removed — see
        // append_burst_frames). `af` is the fd pinned for the pwrite above.
        // pending_fsync BEFORE the fsync, last_synced AFTER (fd_evictable gate).
        extent.coalescer.pending_fsync.store(end, Ordering::SeqCst);
        if let Err(e) = af.sync_data().await {
            let msg = e.to_string();
            self.mark_disk_error_for_extent(req.extent_id, &msg);
            return Err((StatusCode::Internal, msg));
        }
        extent.coalescer.last_synced.store(end, Ordering::SeqCst);

        // Final owner_epoch fence re-check before the write becomes visible/ACKed.
        // This path advances `extent.len` only here (after pwrite + fsync), so an
        // eager takeover MSG_FENCE_EXTENT that raised the floor DURING that window
        // is otherwise invisible until the ACK. Reject so a stale (zombie) writer
        // never gets an ACK; the bytes already on disk are past the un-advanced
        // committed end and are reconciled by the committed-end carry check on the
        // new owner's replay (never a silent lost update).
        if req.owner_epoch < extent.owner_epoch.load(Ordering::SeqCst) {
            return append_reject(CODE_LOCKED_BY_OTHER);
        }

        extent.len.store(end, Ordering::SeqCst);

        // P0-B: the owner_epoch fence was persisted durably in the prologue
        // (under the per-extent op lock) before this write — no post-write
        // save_meta. Data is durable via the coalescer above.

        Ok(AppendResp {
            code: CODE_OK,
            offset: start_offset,
            end,
        }
        .encode())
    }

    async fn handle_read_bytes(&self, payload: Bytes) -> HandlerResult {
        let req = ReadBytesReq::decode(payload)
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        let extent = self.get_extent(req.extent_id).await?;

        // META-FAILCLOSED: a quarantined extent (corrupt `.meta` at load) must
        // NOT serve reads — its length/seal/eversion are untrusted, so bytes
        // could be stale/garbage. Surface EVERSION_MISMATCH so the client
        // fails over to a healthy replica; recovery rebuilds this one.
        if extent.corrupt_meta.load(Ordering::SeqCst) {
            return Ok(ReadBytesResp {
                code: CODE_EVERSION_MISMATCH,
                end: 0,
                payload: Bytes::new(),
            }
            .encode());
        }

        // Eversion gate + length semantics live in `read_plan` (shared with
        // the batched build_read_future — see its doc for the eversion gate / P0-C and
        // why the mismatch is a typed RESPONSE, not an Err status).
        let (end, read_offset, read_size) = match read_plan(&extent, &req) {
            Ok(plan) => plan,
            Err(why) => {
                return Ok(ReadBytesResp {
                    code: why.code(),
                    end: 0,
                    payload: Bytes::new(),
                }
                .encode())
            }
        };

        // Chunk pread to dodge the per-syscall INT_MAX cap on macOS /
        // 0x7ffff000 on Linux. Recovery sends
        // length=0 to slurp full sealed extents in one RPC, so the
        // per-syscall size on the server side can exceed 2 GiB.
        // re-open on miss for an evicted sealed extent; a shard file is opened
        // by name. `read_plan` above already refused a file this node lacks.
        let rf = self
            .payload_file(&extent, req.payload_ref())
            .await
            .map_err(|e| (StatusCode::Internal, e))?;
        let data = file_pread_chunked(rf, read_offset, read_size as usize)
            .await
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;

        // Same policy body as the batched path, deliberately. This handler is
        // unreachable over the wire (`process_frames_backpressured` intercepts
        // both read types before `dispatch`), and it is kept only as the twin
        // that `read_plan` is shared with — so it must not grow a second,
        // drifting answer to the same question.
        if let Some(why) = verify_read_content(
            &self.cached_content_checksums(req.extent_id, &extent).await,
            &req,
            read_offset,
            &data,
        ) {
            return Err((StatusCode::Internal, why));
        }

        Ok(ReadBytesResp {
            code: CODE_OK,
            end,
            payload: Bytes::from(data),
        }
        .encode())
    }

    /// Process a batch of MSG_READ_BYTES frames sequentially, return one Frame per input.
    ///
    /// Sequential preads are faster than N spawned tasks for page-cache hits:
    /// each pread completes in ~1µs, and responses are written back together,
    /// saving per-request TCP write overhead.
    async fn handle_commit_length(&self, payload: Bytes) -> HandlerResult {
        let req = CommitLengthReq::decode(payload)
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        // commit_length is a hot-path RPC; reject wrong-shard.
        if !self.owns_extent(req.extent_id) {
            return Err(self.wrong_shard_err(req.extent_id));
        }

        let entry = self.extents.get(&req.extent_id).ok_or_else(|| {
            (
                StatusCode::NotFound,
                format!("extent {} not found", req.extent_id),
            )
        })?;

        // META-FAILCLOSED: a quarantined extent (corrupt `.meta` at load) has
        // an untrusted length/fence — never feed it into the manager's seal
        // (compute_commit_seal min over reachable members) or let it claim a
        // commit position. Refuse so the manager excludes this replica and
        // recovery rebuilds it.
        if entry.corrupt_meta.load(Ordering::SeqCst) {
            return Ok(CommitLengthResp {
                code: CODE_LOCKED_BY_OTHER,
                length: 0,
            }
            .encode());
        }

        // Tier 2 (post-2026-05-17): `req.owner_epoch <= 0` is a
        // protocol error, not a sentinel. The earlier "owner_epoch == 0
        // bypasses the fence" escape hatch tangled three call sites
        // (seal probe, recovery liveness, autumn-client info) onto one
        // RPC and forced ad-hoc fence skipping; closing that escape hatch
        // broke the seal+recovery paths; the Tier 2 redesign splits
        // probe-without-fence onto `MSG_PROBE_EXTENT` and tightens THIS
        // RPC into a clean fence-enforcing primitive. Callers that
        // legitimately don't have an owner (manager recovery liveness,
        // `autumn-client info` display) now use `handle_probe_extent`.
        //
        // Fence semantics on the surviving (owner_epoch > 0) path — CHECK ONLY,
        // NEVER handover (the three-concepts rule, 2026-05-29):
        //   owner_epoch < owner_epoch → CODE_LOCKED_BY_OTHER (stale owner)
        //   owner_epoch >= owner_epoch → no-op, return length
        //
        // commit_length is a length PROBE; write-ownership is established
        // EXCLUSIVELY by the APPEND path (`handle_append*` bumps owner_epoch
        // when a higher-owner_epoch owner writes). A probe must NOT steal the
        // write-fence. The old "owner_epoch > owner_epoch → bump + persist
        // .meta handover" was the Layer-C poison bug: the manager's
        // control-plane probes (the `admin-merge:<v>:<s>` owner-lock in
        // `handle_merge_partitions`, the seal in `handle_stream_alloc_extent`)
        // carry a high global owner-owner_epoch counter that does NOT represent a
        // new PS write-owner. Bumping owner_epoch on such a probe fenced out
        // the LIVE PS (which holds its acquire-time owner_epoch and never re-reads
        // the climbing counter) → CODE_LOCKED_BY_OTHER on its next append →
        // partition self-poison. New-owner takeover is unaffected: the new
        // owner's first APPEND advances the fence and fences the old owner
        // (see `system_locked_by_other` — sc2 fences sc1 via append, not probe).
        if req.owner_epoch <= 0 {
            return Err((
                StatusCode::InvalidArgument,
                format!(
                    "commit_length requires owner_epoch > 0 (got {}); use \
                     MSG_PROBE_EXTENT for fence-free probes",
                    req.owner_epoch
                ),
            ));
        }
        let owner_epoch = entry.owner_epoch.load(Ordering::SeqCst);
        if req.owner_epoch < owner_epoch {
            return Ok(CommitLengthResp {
                code: CODE_LOCKED_BY_OTHER,
                length: 0,
            }
            .encode());
        }
        // for sealed extents, return the LOGICAL sealed length
        // (the original payload length, agreed with the manager). For
        // open extents, return the durable
        // high-water (`coalescer.last_synced`), NOT `entry.len`.
        //
        // This previously returned `entry.len`, which is set to
        // `total_end` BEFORE the pwrite + fsync future is even returned
        // (see `build_append_future` step 7). A concurrent peer (e.g.
        // EC convert peer-copy gap fill, or manager seal) querying
        // commit_length during the pwrite-to-fsync window would read
        // the reservation and treat it as committed. Manager would
        // then seal at a non-durable value; on this replica's crash
        // before fsync, the file shrinks back below sealed_length →
        // permanent inconsistency in etcd.
        //
        // The per-extent coalescer maintains `last_synced` =
        // post-fsync durable high-water. Returning it gives the strict
        // "what's actually on disk" guarantee that seal needs.
        // Trade-off: bytes between `last_synced` and `entry.len` (in
        // flight pwrites) are temporarily invisible to commit_length;
        // they reappear on the next coalescer tick (1-5 ms later).
        // For the original post-EC-conversion shard-size concern,
        // `last_synced` is also bounded above by
        // `sealed_length` for sealed extents (set in
        // `apply_extent_meta_durable`), so the EC-shard-size confusion
        // doesn't recur.
        let length = committed_length_value(&entry);
        Ok(CommitLengthResp {
            code: CODE_OK,
            length,
        }
        .encode())
    }

    /// Tier 2: manager-only fence-free length+existence probe.
    ///
    /// Two call sites only:
    ///   - `manager/src/recovery.rs::recovery_dispatch_loop` — uses
    ///     `code == CODE_OK` to decide whether to fire
    ///     `dispatch_recovery_task`; ignores `length`.
    ///   - `autumn-client info` open-extent live-length display —
    ///     uses `length` to render commit_length on streams where no
    ///     PS-owner context is available (the `info` CLI doesn't hold
    ///     an owner lock).
    ///
    /// Differs from `handle_commit_length` in exactly two ways:
    ///   (a) takes no owner_epoch — request is 8 bytes, not 16.
    ///   (b) does NOT touch the owner-lock fence — never returns
    ///       LOCKED_BY_OTHER, never mutates `owner_epoch`, never
    ///       writes `.meta`.
    /// Length-source semantics are identical to commit_length so the
    /// `info` CLI display matches what a real owner would see.
    async fn handle_probe_extent(&self, payload: Bytes) -> HandlerResult {
        let req = ProbeExtentReq::decode(payload)
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        if !self.owns_extent(req.extent_id) {
            return Err(self.wrong_shard_err(req.extent_id));
        }

        let entry = match self.extents.get(&req.extent_id) {
            Some(e) => e,
            None => {
                return Ok(ProbeExtentResp {
                    code: CODE_NOT_FOUND,
                    length: 0,
                }
                .encode())
            }
        };

        let length = committed_length_value(&entry);
        Ok(ProbeExtentResp {
            code: CODE_OK,
            length,
        }
        .encode())
    }

    /// EAGER owner_epoch fence — raise the per-extent write fence floor to
    /// `req.owner_epoch` WITHOUT appending. Mirrors the APPEND fence prologue
    /// (`handle_append` step 3/3b + `build_append_future`) minus the write:
    /// synchronously raise the in-memory bar (`fetch_max`, monotonic), then
    /// make the fence DURABLE (`ensure_fence_durable`) before ACKing.
    /// Fail-closed: a persist failure marks the disk offline and returns
    /// `CODE_PRECONDITION` so the caller does NOT trust an undurable fence.
    ///
    /// Used on partition TAKEOVER (`StreamClient::fence_tail` from
    /// `open_partition`) to close the idle-takeover window: without it the EN
    /// floor stays at the OLD owner's epoch until the new owner's first append,
    /// letting a paused-then-resumed zombie writer's `E_old` append pass the
    /// fence and be silently ACKed (a lost update). After this fence, that same
    /// append is rejected at `handle_append` (`req.owner_epoch < stored`).
    ///
    /// Deliberately does NOT reject a SEALED extent: raising the floor on a
    /// sealed tail is a harmless no-op (the sealed check already rejects the
    /// zombie), and the caller resolves "the current tail" which may have
    /// sealed under it — fencing it anyway is safe (seal-lenient).
    async fn handle_fence_extent(&self, payload: Bytes) -> HandlerResult {
        let req = FenceExtentReq::decode(payload)
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        // hot-path-shaped RPC; reject wrong-shard (the StreamClient routes by
        // `shard_addr_for_extent`, so a mismatch is a routing bug).
        if !self.owns_extent(req.extent_id) {
            return Err(self.wrong_shard_err(req.extent_id));
        }

        // A fence carries a REAL acquired owner epoch (the manager owner-lock
        // `mod_revision`, always > 0). `<= 0` is protocol misuse — never a
        // sentinel (same stance as `handle_commit_length`).
        if req.owner_epoch <= 0 {
            return Err((
                StatusCode::InvalidArgument,
                format!(
                    "fence_extent requires owner_epoch > 0 (got {})",
                    req.owner_epoch
                ),
            ));
        }

        // Clone the `Rc` OUT of the DashMap and drop the shard `Ref` before any
        // `.await` — `ensure_fence_durable` awaits, and on the single-threaded
        // compio runtime holding a DashMap read guard across an await would
        // deadlock a concurrent writer (recovery / delete / alloc mutating
        // `self.extents` on the same shard). Mirrors `get_extent` / `handle_append`.
        let entry = self
            .extents
            .get(&req.extent_id)
            .map(|v| Rc::clone(v.value()))
            .ok_or_else(|| {
                (
                    StatusCode::NotFound,
                    format!("extent {} not found", req.extent_id),
                )
            })?;

        // META-FAILCLOSED: a quarantined extent (corrupt `.meta` at load) has an
        // untrusted fence — persisting a fence over it could clobber the
        // authoritative-but-unreadable sidecar. Refuse; recovery rebuilds it.
        if entry.corrupt_meta.load(Ordering::SeqCst) {
            return Ok(FenceExtentResp {
                code: CODE_PRECONDITION,
                message: format!("extent {} meta quarantined", req.extent_id),
            }
            .encode());
        }

        // Fence prologue, mirroring the append path (step 3/3b):
        //   req.owner_epoch < stored → stale caller, reject LockedByOther;
        //   req.owner_epoch > stored → raise the in-memory bar synchronously;
        //   then persist durably (fail-closed) before ACKing.
        let stored = entry.owner_epoch.load(Ordering::SeqCst);
        if req.owner_epoch < stored {
            return Ok(FenceExtentResp {
                code: CODE_LOCKED_BY_OTHER,
                message: format!(
                    "extent {} held at higher owner_epoch {} (fence carried {})",
                    req.extent_id, stored, req.owner_epoch
                ),
            }
            .encode());
        }
        if req.owner_epoch > stored {
            entry
                .owner_epoch
                .fetch_max(req.owner_epoch, Ordering::SeqCst);
        }
        if let Err(e) = self
            .ensure_fence_durable(req.extent_id, &entry, req.owner_epoch)
            .await
        {
            self.mark_disk_error_for_extent(req.extent_id, &e.to_string());
            tracing::error!(
                extent_id = req.extent_id,
                error = %e,
                "handle_fence_extent: durable fence persist failed — refusing (fail-closed)"
            );
            return Ok(FenceExtentResp {
                code: CODE_PRECONDITION,
                message: format!("durable fence persist failed: {e}"),
            }
            .encode());
        }
        // Re-check after the (possibly awaiting) durable step: a concurrent
        // higher-owner fence/append may have taken over. owner_epoch is a
        // monotonic fetch_max, so `stored >= req` now means our floor is at
        // least as high — that is exactly what the fence guarantees. If a
        // STRICTLY higher owner landed, report LockedByOther (the caller is
        // already superseded), else OK.
        let after = entry.owner_epoch.load(Ordering::SeqCst);
        if req.owner_epoch < after {
            return Ok(FenceExtentResp {
                code: CODE_LOCKED_BY_OTHER,
                message: format!(
                    "extent {} taken over at owner_epoch {} during fence persist",
                    req.extent_id, after
                ),
            }
            .encode());
        }
        Ok(FenceExtentResp {
            code: CODE_OK,
            message: String::new(),
        }
        .encode())
    }

    /// Phase 2: report the per-extent fsync coalescer's
    /// `last_synced_offset`. Used by `flush_one_imm` (via
    /// `StreamClient::await_log_synced_to`) to ensure all log_stream bytes
    /// referenced by a to-be-flushed memtable's ValuePointers are durable
    /// on this replica before the SST upload.
    ///
    /// Notes:
    /// - This is a node-local view; the client takes the quorum-min across
    ///   3 replicas (mirror of the commit_length quorum).
    /// - For sealed extents, all bytes up to `sealed_length` were forced
    ///   durable by `apply_extent_meta_durable` at seal time, so we
    ///   bound-up to `max(last_synced, sealed_length)` here. Otherwise a
    ///   reader of a sealed extent could observe `last_synced=0` purely
    ///   because no append-driven sync has run since this node loaded the
    ///   extent — even though the bytes are demonstrably on disk.
    async fn handle_synced_length(&self, payload: Bytes) -> HandlerResult {
        let req = SyncedLengthReq::decode(payload)
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        // hot-path RPC; reject wrong-shard.
        if !self.owns_extent(req.extent_id) {
            return Err(self.wrong_shard_err(req.extent_id));
        }

        let entry = self.extents.get(&req.extent_id).ok_or_else(|| {
            (
                StatusCode::NotFound,
                format!("extent {} not found", req.extent_id),
            )
        })?;

        let synced = entry.coalescer.last_synced.load(Ordering::SeqCst);
        let sealed = entry.sealed_length.load(Ordering::SeqCst);
        // P0-C (coco review #3 issue 3): a SEALED extent's durable length is its
        // authoritative `sealed_length` (incl. 0 for sealed-empty) — never the
        // residual file-derived `last_synced`. For an open extent keep the
        // max(synced, sealed) behaviour.
        let length = if entry.sealed.load(Ordering::SeqCst) {
            sealed
        } else {
            synced.max(sealed)
        };
        Ok(SyncedLengthResp {
            code: CODE_OK,
            length,
        }
        .encode())
    }

    async fn handle_alloc_extent(&self, payload: Bytes) -> HandlerResult {
        let req: AllocExtentReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // forward to owner shard if we don't own this extent.
        if !self.owns_extent(req.extent_id) {
            if let Some(sibling) = self.sibling_for_extent(req.extent_id) {
                return self
                    .forward_rpc_to_sibling(sibling, MSG_ALLOC_EXTENT, payload)
                    .await;
            }
        }

        let disk = self.choose_disk().ok_or_else(|| {
            (
                StatusCode::Unavailable,
                "no online disk available".to_string(),
            )
        })?;
        let disk_id = disk.disk_id;

        let path = disk.extent_path(req.extent_id);
        if let Some(parent) = path.parent() {
            compio::fs::create_dir_all(parent)
                .await
                .map_err(|e| (StatusCode::Internal, e.to_string()))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;
        let len = file
            .metadata()
            .await
            .map(|m| m.len())
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;

        self.extents.insert(
            req.extent_id,
            Rc::new(ExtentEntry {
                has_dat: AtomicBool::new(true),
                // A freshly created extent holds its payload in `.dat`; a
                // conversion only ever moves it via the manager's layout flip.
                payload_location: AtomicU8::new(autumn_rpc::extent_rpc::PAYLOAD_LOCATION_IN_DAT),
                shard_files: RefCell::new(Default::default()),
                // freshly-created local extent (copy/recovery) —
                // pinned resident; it's actively being written.
                file: RefCell::new(Some(Rc::new(file))),
                extent_id: req.extent_id,
                len: AtomicU64::new(len),
                eversion: AtomicU64::new(1),
                sealed_length: AtomicU64::new(0),
                // P0-C: a freshly-created/allocated extent is open.
                sealed: AtomicBool::new(false),
                avali: AtomicU32::new(0),
                owner_epoch: AtomicI64::new(0),
                durable_owner_epoch: AtomicI64::new(0),
                disk_id,
                coalescer: Coalescer::new(len),
                owner: RefCell::new(OwnerMailbox::default()),
                corrupt_meta: AtomicBool::new(false),
                content_ck: RefCell::new(CachedChecksums::NotLoaded),
            }),
        );

        let entry = self.get_extent(req.extent_id).await?;
        // ENOSPC-1 (coco P2): a failed initial `.meta` persist marks the
        // disk (Full on ENOSPC, Faulted otherwise) AND removes the
        // just-inserted entry — leaving it would let local lookups see an
        // extent with no durable sidecar and block a manager re-dispatch
        // with "extent already exists" (same family as the P0-D recovery
        // path; the orphan .dat is reaped by the reconcile sweep).
        if let Err(e) = self.save_meta(req.extent_id, &entry).await {
            self.mark_disk_error_for_extent(req.extent_id, &e);
            self.extents.remove(&req.extent_id);
            self.fd_lru.forget(req.extent_id);
            self.ec_stage_nonce.remove(&req.extent_id);
            return Err((StatusCode::Internal, e));
        }

        Ok(rkyv_encode(&AllocExtentResp {
            code: CODE_OK,
            disk_id,
            message: String::new(),
        }))
    }

    async fn handle_df(&self, payload: Bytes) -> HandlerResult {
        let req: DfReq = rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // Cluster-df: per-disk live extent footprint = Σ ExtentEntry.len
        // (the EN is the data owner; this is the REAL on-disk autumn byte
        // count — replicas count their full copy, EC shards count shard
        // size, open tails count live appended bytes — no amplification
        // formula needed). O(local extents), µs. The manager sums these
        // across all nodes into `physical_used`.
        let mut extent_bytes_by_disk: std::collections::HashMap<u64, u64> =
            std::collections::HashMap::new();
        for e in self.extents.iter() {
            let entry = e.value();
            *extent_bytes_by_disk.entry(entry.disk_id).or_insert(0) += entry
                .len
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_add(entry.shard_bytes());
        }

        let mut disk_status: Vec<(u64, DiskStatus)> = Vec::new();
        if req.disk_ids.is_empty() {
            // Report all known disks.
            for disk in self.disks.values() {
                let (total, free) = disk.disk_stats();
                disk_status.push((
                    disk.disk_id,
                    DiskStatus {
                        total,
                        free,
                        online: disk.online(),
                        extent_bytes: extent_bytes_by_disk
                            .get(&disk.disk_id)
                            .copied()
                            .unwrap_or(0),
                    },
                ));
            }
        } else {
            for disk_id in &req.disk_ids {
                if let Some(disk) = self.disks.get(disk_id) {
                    let (total, free) = disk.disk_stats();
                    disk_status.push((
                        *disk_id,
                        DiskStatus {
                            total,
                            free,
                            online: disk.online(),
                            extent_bytes: extent_bytes_by_disk.get(disk_id).copied().unwrap_or(0),
                        },
                    ));
                }
            }
        }

        let done_tasks = {
            let mut done = self.done.take_recovery();
            if req.tasks.is_empty() {
                done
            } else {
                let wanted = req
                    .tasks
                    .iter()
                    .map(|t| (t.extent_id, t.replace_id, t.node_id))
                    .collect::<std::collections::HashSet<_>>();
                let mut matched = Vec::new();
                let mut remaining = Vec::new();
                for status in done.drain(..) {
                    let key = (
                        status.task.extent_id,
                        status.task.replace_id,
                        status.task.node_id,
                    );
                    if wanted.contains(&key) {
                        matched.push(status);
                    } else {
                        remaining.push(status);
                    }
                }
                // Put back what this caller did not ask for.
                for status in remaining {
                    self.done.push_recovery(status);
                }
                matched
            }
        };

        // Drain completed EC conversions (at-most-once, same contract as
        // `done_tasks`): a report lost because the manager failed to apply it
        // converges via re-dispatch → the EN's idempotent-skip adopt path.
        let ec_done = self.done.take_ec();

        Ok(rkyv_encode(&DfResp {
            done_tasks,
            ec_done,
            disk_status,
            // M1b: echo our own identity so the manager can
            // self-heal stored-location drift + detect pod-IP reuse. Empty when
            // `--advertise` was not passed (test / pre-M1 deployments).
            node_uuid: self.registration.node_uuid.clone(),
            advertise_addr: self.registration.advertise_addr.clone(),
            shard_ports: self.registration.shard_ports.clone(),
            op_progress: self
                .op_progress
                .iter()
                .map(|e| {
                    let (kind, done, total) = *e.value();
                    crate::extent_rpc::ExtentOpProgress {
                        extent_id: *e.key(),
                        kind,
                        done,
                        total,
                    }
                })
                .collect(),
        }))
    }

    /// Judge this node's local EC shard against the manager's view.
    ///
    /// Before this existed, every `ec_converted` extent answered `Unknown`, and
    /// `Unknown` means "refuse". The manager treats its recovery marker as a
    /// standing instruction and re-sends it every tick, so the refusal and the
    /// re-dispatch fed each other forever: the pair wedged, the marker held a
    /// rate-limiter slot, and extents that genuinely needed rebuilding queued
    /// behind it. A failed rebuild leaves exactly the state that triggers it —
    /// the 0-byte `.dat` stub `ensure_extent` creates — so ONE failure was
    /// enough to wedge a (node, extent) pair permanently.
    ///
    /// The authoritative shard length is `ec_shard_read_len`, the same
    /// `erasure::shard_size` the encoder wrote with, so "did the last attempt
    /// finish" is decidable here and needs no new state. Pure and `&self`-free
    /// so it can be tested without a manager.
    fn classify_ec_shard(
        info: &ExtentInfo,
        entry: &ExtentEntry,
        replace_id: u64,
    ) -> LocalCopyVerdict {
        let Ok(shard_index) = Self::ec_shard_index(info, replace_id) else {
            // The slot to rebuild is not in the extent's node list: the task
            // and the snapshot disagree about membership, so there is nothing
            // to compare against. Refuse rather than guess.
            return LocalCopyVerdict::Unknown;
        };
        let want = Self::ec_shard_read_len(info.sealed_length, info.replicates.len());
        if want == 0 {
            // `ec_converted` with sealed_length 0 or K 0 — the manager's record
            // and this extent disagree. The rebuild refuses this shape too, so
            // claiming "incomplete" here would only dispatch a certain failure.
            return LocalCopyVerdict::Unknown;
        }
        if entry.eversion.load(Ordering::SeqCst) != info.eversion {
            // A shard from a different generation is not this shard, whatever
            // its length. Rebuilding is right for a stale local copy; for a
            // LOCAL copy that is somehow newer, `run_recovery_task`'s
            // refuse-at-start check rejects the stale snapshot cleanly and the
            // manager re-resolves. Neither outcome is a wedge.
            return LocalCopyVerdict::IncompleteEcShard;
        }
        // WHICH FILE holds the shard follows the manager's layout, exactly as
        // the rebuild decides it. A legacy converted extent renamed its shard
        // over `.dat` (`ec_converted = true, InDat`) and has no `shard_files`
        // entry at all, so reading only the map would answer "incomplete" for
        // every such extent, forever. That is not a wedge — the rebuild would
        // be dispatched and would succeed — but it makes a lost completion
        // report cost an outage: the legacy path opens with `set_len(0)`, so
        // re-running a rebuild that had already finished truncates a shard that
        // readers are currently being served.
        let have = if PayloadLocation::from_byte(info.payload_location)
            == PayloadLocation::InShardFile
        {
            entry.shard_file_len(shard_index as u32)
        } else if entry.has_dat.load(Ordering::SeqCst) {
            Some(entry.len.load(Ordering::SeqCst))
        } else {
            None
        };
        match have {
            // Exact length only. A shard is fixed-size by construction, so
            // `>=` would adopt an over-long file, and the reader — which
            // demands this exact length from every peer — would then reject
            // what we just reported as complete.
            Some(len) if len == want => LocalCopyVerdict::Complete,
            _ => LocalCopyVerdict::IncompleteEcShard,
        }
    }

    /// re-dispatch adopt for a COMPLETED-but-unreported
    /// recovery. `handle_df` hands `recovery_done` to the manager via
    /// `std::mem::take` BEFORE knowing the response was delivered (an
    /// at-most-once handoff): if the df response is lost in transit, the
    /// completion is gone forever — the manager's Recovery marker ages out
    /// via the stale sweep and the slot is re-dispatched. Before this fix
    /// the "extent already exists" refusal then PERMANENTLY poisoned every
    /// candidate that had already completed once; after every candidate was
    /// poisoned the fenced slot could never be rebuilt and `MSG_REMOVE_NODE`
    /// blocked forever (the live-decommission drain wedge — reproduced by
    /// `system_chaos` + `AUTUMN_CHAOS_DECOMMISSION=1`, where each 60 s
    /// sweep-release re-dispatched extent 16 to the next candidate until
    /// nodes 3/5/9 all held complete-but-unadoptable local copies).
    ///
    /// Adopt is deliberately NARROW. For a REPLICATED extent the bytes of a
    /// sealed extent are immutable, so a local copy matching the manager's
    /// authoritative snapshot (same eversion, sealed, full sealed_length, not
    /// quarantined) IS the completed recovery result; its `.meta` was made
    /// durable by the original run (P0-D fail-closed). An EC'd extent is
    /// judged by `classify_ec_shard` instead — its payload is a shard, so
    /// `len` says nothing about it — and that is also where the EC'd extent's
    /// "incomplete" answer comes from. Everything still unjudgeable (manager
    /// unreachable / open / quarantined) stays `Unknown`, which means refuse.
    async fn try_adopt_completed_recovery(
        &self,
        task: &crate::extent_rpc::RecoveryTask,
        entry: &Rc<ExtentEntry>,
    ) -> LocalCopyVerdict {
        let info = match self.extent_info_from_manager(task.extent_id).await {
            Ok(Some(info)) => info,
            // Manager unreachable / extent unknown — we cannot judge the local
            // copy at all. NOT "incomplete": resetting on a failed lookup would
            // destroy a complete replica whenever the manager blips.
            _ => return LocalCopyVerdict::Unknown,
        };
        if entry.corrupt_meta.load(Ordering::SeqCst) {
            // META-FAILCLOSED quarantine: never report a quarantined copy as a
            // healthy recovered replica, and never silently overwrite it — the
            // manager's repair path owns quarantined extents. Checked BEFORE
            // the shape split so a quarantined EC'd extent is refused too.
            //
            // ⚠️ This refusal has NO self-heal, and it is the one shape that can
            // still wedge the way `classify_ec_shard` exists to prevent. The
            // flag is cleared only by `save_meta`, which the placement path
            // refuses to run for a quarantined entry — so for a shard-only
            // holder that lost its `.meta`, recovery (the documented healer)
            // is exactly what stays refused, every tick. Rare: it needs the
            // `.dat` already reclaimed AND `.meta` then lost. Left as-is
            // deliberately rather than widened here, because letting recovery
            // overwrite a quarantined extent is a decision about the
            // fail-closed contract, not a detail of this comparison.
            return LocalCopyVerdict::Unknown;
        }
        if info.ec_converted {
            // An EC'd extent's local payload is a SHARD, so `len` (the `.dat`
            // length) says nothing about it. Judged on its own terms below.
            let verdict = Self::classify_ec_shard(&info, entry, task.replace_id);
            if verdict == LocalCopyVerdict::Complete {
                tracing::info!(
                    extent_id = task.extent_id,
                    replace_id = task.replace_id,
                    eversion = info.eversion,
                    sealed_length = info.sealed_length,
                    "require_recovery: local EC shard already complete — adopting \
                     (lost-completion re-dispatch)"
                );
                self.done.push_recovery(RecoveryTaskDone {
                    task: task.clone(),
                    ready_disk_id: entry.disk_id,
                });
            }
            return verdict;
        }
        if !info.sealed {
            // An open extent has no authoritative length. Refuse, never reset.
            return LocalCopyVerdict::Unknown;
        }
        let local_ev = entry.eversion.load(Ordering::SeqCst);
        let local_len = entry.len.load(Ordering::SeqCst);
        let local_sealed =
            entry.sealed.load(Ordering::SeqCst) || entry.sealed_length.load(Ordering::SeqCst) > 0;
        if !(local_sealed && local_ev == info.eversion && local_len >= info.sealed_length) {
            // We HAVE the authoritative view and the local copy falls short of
            // it — provably incomplete.
            return LocalCopyVerdict::Incomplete;
        }
        tracing::info!(
            extent_id = task.extent_id,
            replace_id = task.replace_id,
            eversion = local_ev,
            len = local_len,
            sealed_length = info.sealed_length,
            "require_recovery: local copy already complete — adopting (lost-completion re-dispatch)"
        );
        self.done.push_recovery(RecoveryTaskDone {
            task: task.clone(),
            ready_disk_id: entry.disk_id,
        });
        LocalCopyVerdict::Complete
    }

    async fn handle_require_recovery(&self, payload: Bytes) -> HandlerResult {
        let req: RequireRecoveryReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // forward to owner shard.
        if !self.owns_extent(req.task.extent_id) {
            if let Some(sibling) = self.sibling_for_extent(req.task.extent_id) {
                return self
                    .forward_rpc_to_sibling(sibling, MSG_REQUIRE_RECOVERY, payload)
                    .await;
            }
        }

        let task = req.task;

        if self.manager_endpoint.is_none() {
            return code_resp(
                CODE_PRECONDITION,
                "manager endpoint is not configured".to_string(),
            );
        }

        if self.recovery_inflight.contains_key(&task.extent_id) {
            // IDEMPOTENT ACCEPT, not a rejection. The manager treats its marker
            // as a standing instruction and re-sends it every tick, so "I am
            // already doing exactly this" is the request being satisfied — the
            // same contract `handle_convert_to_ec` uses. Answering
            // CODE_PRECONDITION here would make the manager's re-dispatch drain
            // the marker of a HEALTHY in-flight recovery and go hunting for
            // another candidate.
            return code_resp(
                CODE_OK,
                format!("extent {} recovery already running", task.extent_id),
            );
        }

        if let Some(entry) = self
            .extents
            .get(&task.extent_id)
            .map(|v| Rc::clone(v.value()))
        {
            // A local copy already exists. Recovery must be IDEMPOTENT here: the
            // manager treats its marker as a standing instruction and re-sends
            // it, so a permanent refusal is a permanent wedge.
            match self.try_adopt_completed_recovery(&task, &entry).await {
                // Completed-but-unreported prior recovery (the df response
                // carrying its RecoveryTaskDone was lost): re-report done.
                LocalCopyVerdict::Complete => return code_resp(CODE_OK, String::new()),
                // Provably incomplete — the residue of an attempt that died
                // mid-copy (`run_recovery_task` persists `.meta` LAST, so a
                // crash leaves a partial `.dat` that reloads as an open extent).
                // Refusing here poisons this (node, extent) pair forever: the
                // orphan reconcile won't reap it either, because the extent
                // itself is still very much alive. Drop the stub and rebuild.
                //
                // Safe because the manager only dispatches recovery to a node it
                // does NOT count as a replica, so this copy is referenced by
                // nothing; and the Complete arm above already took every copy
                // that IS good. The rebuild re-creates the entry from scratch.
                LocalCopyVerdict::Incomplete => {
                    tracing::warn!(
                        extent_id = task.extent_id,
                        replace_id = task.replace_id,
                        local_len = entry.len.load(Ordering::SeqCst),
                        "require_recovery: discarding an incomplete local copy \
                         (crashed prior attempt) and rebuilding"
                    );
                    self.extents.remove(&task.extent_id);
                    self.fd_lru.forget(task.extent_id);
                    self.ec_stage_nonce.remove(&task.extent_id);
                    if let Ok(disk) = self.disk_for(entry.disk_id) {
                        if let Err(e) = disk.remove_extent_files(task.extent_id).await {
                            // Not fatal: `run_recovery_task` truncates the
                            // destination before refilling. Log and proceed.
                            tracing::warn!(
                                extent_id = task.extent_id,
                                "require_recovery: could not unlink the incomplete copy: {e}"
                            );
                        }
                    }
                    // fall through to dispatch a fresh rebuild
                }
                // An EC'd extent whose shard is missing, short, or stale.
                // Dispatch a rebuild WITHOUT resetting anything: `ensure_extent`
                // is idempotent and the rebuild truncates its own destination,
                // so the reset has nothing left to do — and not doing it keeps
                // this path, whose whole job is to clear a wedge, free of any
                // destructive step. See `IncompleteEcShard` for why the
                // replication path's reasons for resetting do not transfer.
                LocalCopyVerdict::IncompleteEcShard => {
                    tracing::warn!(
                        extent_id = task.extent_id,
                        replace_id = task.replace_id,
                        "require_recovery: local EC shard is missing, short or stale \
                         — rebuilding over the existing entry"
                    );
                    // fall through to dispatch a fresh rebuild
                }
                // Cannot tell (manager unreachable / open / quarantined).
                // Keep refusing — destroying a copy of unknown completeness is
                // strictly worse than making the manager retry.
                LocalCopyVerdict::Unknown => {
                    return code_resp(
                        CODE_PRECONDITION,
                        format!("extent {} already exists", task.extent_id),
                    );
                }
            }
        }

        self.recovery_inflight.insert(task.extent_id, task.clone());
        let node = self.clone();
        compio::runtime::spawn(async move {
            let extent_id = task.extent_id;
            const MAX_RECOVERY_RETRIES: u32 = 10;
            for attempt in 1..=MAX_RECOVERY_RETRIES {
                match node.run_recovery_task(task.clone()).await {
                    Ok(done) => {
                        node.recovery_inflight.remove(&extent_id);
                        node.clear_op_progress(extent_id);
                        node.done.push_recovery(done);
                        return;
                    }
                    Err(e) => {
                        if attempt >= MAX_RECOVERY_RETRIES {
                            tracing::error!(
                                extent_id,
                                attempt,
                                error = %e,
                                "recovery task failed after max retries, giving up",
                            );
                            break;
                        }
                        tracing::warn!(
                            extent_id,
                            attempt,
                            error = %e,
                            "recovery task failed, retrying in 10s",
                        );
                        compio::time::sleep(std::time::Duration::from_secs(10)).await;
                    }
                }
            }
            node.recovery_inflight.remove(&extent_id);
        })
        .detach();

        code_resp(CODE_OK, String::new())
    }

    /// unlink the physical extent files after the manager has
    /// confirmed `refs == 0`. Idempotent: deleting an already-missing
    /// extent returns `CODE_OK` so the manager's retry loop is safe.
    ///
    /// Sequencing: remove the in-memory `ExtentEntry` *first* so any
    /// subsequent append fails fast with NotFound. Any pwritev that has
    /// already taken the file handle is allowed to complete to disk
    /// (the kernel preserves the open inode after `unlink`); the
    /// inode is reaped when the last fd closes. The data is meaningless
    /// at this point because the extent's manager-side refs are 0.
    async fn handle_delete_extent(&self, payload: Bytes) -> HandlerResult {
        let req: DeleteExtentReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // IS THIS DELETE FOR US? Extent ids are unique only within a cluster,
        // and a manager's persisted delete retries outlive the cluster: torn
        // down A retrying against an address now owned by B's node would unlink
        // B's live extent with the same id. Refuse before touching anything.
        // Either side leaving the uuid empty means "unspecified" (a legacy
        // persisted entry, a node started without `--advertise`), which skips
        // the check exactly like `classify_df_echo` does for `df`.
        {
            let reg = &self.registration;
            if !req.node_uuid.is_empty()
                && !reg.node_uuid.is_empty()
                && req.node_uuid != reg.node_uuid
            {
                tracing::warn!(
                    extent_id = req.extent_id,
                    for_node = %req.node_uuid,
                    this_node = %reg.node_uuid,
                    "delete_extent addressed to a DIFFERENT node — refusing \
                     (a stale retry from another cluster reusing this address?)"
                );
                return Ok(rkyv_encode(&CodeResp {
                    code: CODE_LOCKED_BY_OTHER,
                    message: format!(
                        "delete_extent is for node {}, this node is {}",
                        req.node_uuid, reg.node_uuid
                    ),
                }));
            }
        }

        // forward to owner shard so each shard only ever
        // touches the extents whose ids hash to it.
        if !self.owns_extent(req.extent_id) {
            if let Some(sibling) = self.sibling_for_extent(req.extent_id) {
                return self
                    .forward_rpc_to_sibling(sibling, MSG_DELETE_EXTENT, payload)
                    .await;
            }
        }

        // if recovery is in flight for this extent, refuse the delete.
        // run_recovery_task's ensure_extent auto-creates on NotFound; if we
        // unlink now, recovery either writes to the unlinked inode (data
        // evaporates when fd closes) or resurrects the extent on-disk as an
        // orphan with no manager record. The manager's extent_delete_loop
        // retries up to 60× (~2 min); orphan-reconcile is the backstop
        // if that budget exhausts before recovery completes.
        if self.recovery_inflight.contains_key(&req.extent_id) {
            return code_resp(
                CODE_PRECONDITION,
                format!(
                    "extent {} recovery in flight; delete deferred",
                    req.extent_id
                ),
            );
        }

        // try-acquire the per-extent mutating-op lock. If held
        // by an in-flight `handle_convert_to_ec` or `handle_re_avali`,
        // refuse the delete with CODE_PRECONDITION. Previously the
        // check only covered the recovery↔delete pair; convert
        // and re_avali could race with delete (data-loss paths
        // documented in feature_list):
        //   - convert↔delete: delete unlinks `.dat`+`.meta` mid-encode;
        //     convert's later `rename(.ec.dat, .dat)` resurrects an
        //     orphan with no manager record + stale `.meta`.
        //   - re_avali↔delete: delete unlinks `.dat`; re_avali's
        //     `file_pwrite_chunked` writes to the unlinked inode
        //     (POSIX preserves open fds); bytes evaporate on fd drop.
        // Lock held across unlink + entry removal so a concurrent
        // op blocks on the lock and observes NotFound after we release.
        // Manager's extent_delete_loop has 60 × 2 s retry budget;
        // covers the lock's typical hold (~seconds for convert, slightly
        // more for re_avali on big extents).
        let op_lock = self.get_or_create_extent_op_lock(req.extent_id);
        let _op_guard = match op_lock.try_lock() {
            Some(g) => g,
            None => {
                return code_resp(
                    CODE_PRECONDITION,
                    format!(
                        "extent {} has in-flight mutating op (convert/re_avali); delete deferred",
                        req.extent_id
                    ),
                );
            }
        };

        // Pull the entry out of the map so any later append on this id
        // fails with NotFound rather than racing the unlink.
        let entry = self.extents.remove(&req.extent_id).map(|(_, v)| v);
        self.fd_lru.forget(req.extent_id);
        self.ec_stage_nonce.remove(&req.extent_id);

        // Locate the file. Prefer the in-memory entry's disk_id (exact
        // match for the file that was actually created); fall back to
        // every disk for the orphan-reconcile case where the entry is
        // already gone (e.g. files left over from a prior boot).
        let mut last_err: Option<anyhow::Error> = None;
        let mut targeted = false;
        if let Some(entry) = entry {
            if let Some(disk) = self.disks.get(&entry.disk_id) {
                targeted = true;
                if let Err(e) = disk.remove_extent_files(req.extent_id).await {
                    last_err = Some(e);
                }
            }
        }
        if !targeted {
            for disk in self.disks.values() {
                if let Err(e) = disk.remove_extent_files(req.extent_id).await {
                    last_err = Some(e);
                }
            }
        }

        match last_err {
            None => {
                tracing::info!(
                    extent_id = req.extent_id,
                    shard_idx = self.shard_idx,
                    "delete_extent: unlinked .dat + .meta",
                );
                code_resp(CODE_OK, String::new())
            }
            Some(e) => code_resp(CODE_ERROR, e.to_string()),
        }
    }

    async fn handle_re_avali(&self, payload: Bytes) -> HandlerResult {
        let req: ReAvaliReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // forward to owner shard.
        if !self.owns_extent(req.extent_id) {
            if let Some(sibling) = self.sibling_for_extent(req.extent_id) {
                return self
                    .forward_rpc_to_sibling(sibling, MSG_RE_AVALI, payload)
                    .await;
            }
        }

        // acquire the per-extent mutating-op lock for the
        // entire re_avali. Held against concurrent
        // `handle_convert_to_ec` (would corrupt the staging path) and
        // `handle_delete_extent` (would unlink the inode while
        // `file_pwrite_chunked` is writing to it). Released on
        // function exit. Early-return paths (EC short-circuit,
        // already-up-to-date) hold the lock only briefly. The actual
        // long-running path (fetch_full_extent_from_sources + write)
        // serialises with convert / delete via this lock.
        let op_lock = self.get_or_create_extent_op_lock(req.extent_id);
        let _op_guard = op_lock.lock().await;

        let extent = match self.get_extent(req.extent_id).await {
            Ok(v) => v,
            Err(_) => {
                return code_resp(
                    CODE_NOT_FOUND,
                    format!("extent {} not found", req.extent_id),
                );
            }
        };

        // TODO: manager RPC for extent_info not yet implemented
        let extent_info = match self.extent_info_from_manager(req.extent_id).await {
            Ok(Some(ex)) => ex,
            Ok(None) => {
                return code_resp(
                    CODE_NOT_FOUND,
                    format!("extent {} not found in manager", req.extent_id),
                );
            }
            Err(e) => {
                return code_resp(CODE_ERROR, e);
            }
        };
        // fsync on 0→sealed transition.
        // P0-A (coco): if the seal can't be made durable, return CODE_ERROR
        // (disk is now offline) — do NOT fall through to the CODE_OK
        // "already up to date" path, which would let the manager treat this
        // non-durable replica as healthy.
        if let Err(e) = self
            .apply_extent_meta_durable(req.extent_id, &extent, &extent_info)
            .await
        {
            return code_resp(
                CODE_ERROR,
                format!(
                    "re_avali: seal not durable for extent {}: {e}",
                    req.extent_id
                ),
            );
        }

        if req.eversion < extent_info.eversion {
            return code_resp(
                CODE_PRECONDITION,
                format!(
                    "eversion too low: got {}, expect >= {}",
                    req.eversion, extent_info.eversion
                ),
            );
        }

        // RE_AVALI is a replicated-extent repair primitive. For an
        // EC'd extent the local shard size is `sealed_length / K`, so
        // the `local_len >= sealed_length` check below would always fall
        // through to `fetch_full_extent_from_sources` — which allocates a
        // `sealed_length`-sized Vec<u8> per peer and (on success) would
        // overwrite the local shard with raw bytes, corrupting EC.
        // Missing-shard repair on an EC'd extent must route through
        // EXT_MSG_REQUIRE_RECOVERY → run_ec_recovery_payload. Returning
        // CODE_OK here also lets the manager's recovery_dispatch_loop
        // self-heal historically buggy `avali` values via mark_extent_available
        // on the next 2 s tick.
        if extent_info.ec_converted {
            return code_resp(CODE_OK, String::new());
        }

        let local_len = extent.len.load(Ordering::SeqCst);
        if local_len >= extent_info.sealed_length {
            return code_resp(CODE_OK, String::new());
        }

        // gate cross-extent re_avali concurrency through the
        // shared recovery permit pool. Previously only `run_recovery_task`
        // acquired it; the replicated re_avali path
        // (`fetch_full_extent_from_sources` + `file_pwrite_chunked`) had
        // the same `payload × 2` transient working set as recovery but
        // no cap, so a leader's recovery dispatch fan-out to several
        // surviving nodes could push peak RAM proportional to
        // `concurrent_re_avali × sealed_length` per node. Acquired AFTER
        // the EC short-circuit and the already-up-to-date check so cheap
        // requests don't consume a permit. Held until function exit via
        // RAII. Permit pool shared with `run_recovery_task`; both are
        // logically "bulk repair work" and benefit from a unified cap
        // (env `AUTUMN_EXTENT_RECOVERY_PARALLELISM`, default 2).
        let _rec_permit = self.concurrency_ctrl.acquire_recovery().await;

        // TEMP-THEN-PUBLISH. The destination here is an EXISTING copy, so it
        // has something to lose: `stream_extent_from_sources` truncates the
        // destination to 0 before each source attempt, and if no source can
        // deliver, this replica is left holding LESS than it started with —
        // reproduced as 4096 bytes → 0 in
        // `crates/manager/tests/re_avali_no_destroy.rs`.
        //
        // Those bytes matter. `avali == 0` is what aims repair at this replica,
        // but it does not mean "lagging": a member that was merely UNREACHABLE
        // when the extent was sealed has its bit left unset (manager
        // CLAUDE.md, seal-over-reachable) while possibly holding the LONGEST
        // copy in the cluster — and every recovery elsewhere picks its sources
        // from the member list without consulting `avali`, so this file is
        // exactly what another node would rebuild from.
        //
        // `peer_copy_full_extent_to_dat` streams into a temp and atomic-renames
        // only once a FULL `sealed_length` copy has landed, so a repair that
        // cannot succeed is a no-op. It also deliberately has no reconcile-down
        // for an over-promised seal: adopting a SHORTER peer copy over a longer
        // local one is precisely the trade this path must not make. Recovery
        // keeps `stream_extent_from_sources` — its destination is a fresh or
        // provably-incomplete replica, which has nothing to lose.
        if let Err((_, msg)) = self
            .peer_copy_full_extent_to_dat(req.extent_id, &extent, &extent_info, extent_info.sealed_length)
            .await
        {
            return code_resp(CODE_ERROR, msg);
        }

        // P0-A (coco): the post-repair seal `.meta` must be durable before we
        // report success — the data is now filled + fsync'd, but if this
        // save_meta fails and we returned CODE_OK, the manager's
        // `mark_extent_available` (recovery.rs) would mark this replica healthy
        // while its sealed `.meta` is non-durable. Fail-closed: mark the disk
        // offline + return CODE_ERROR so the manager re-dispatches recovery.
        if let Err(e) = self.save_meta(req.extent_id, &extent).await {
            tracing::error!(
                extent_id = req.extent_id,
                error = %e,
                "P0-A: re_avali post-repair save_meta failed — disk OFFLINE, returning CODE_ERROR",
            );
            self.mark_disk_error_for_extent(req.extent_id, &e);
            return code_resp(
                CODE_ERROR,
                format!(
                    "re_avali: seal meta not durable for extent {}: {e}",
                    req.extent_id
                ),
            );
        }

        code_resp(CODE_OK, String::new())
    }

    async fn handle_copy_extent(&self, payload: Bytes) -> HandlerResult {
        let req = CopyExtentReq::decode(payload.clone())
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        // forward to owner shard.
        if !self.owns_extent(req.extent_id) {
            if let Some(sibling) = self.sibling_for_extent(req.extent_id) {
                return self
                    .forward_rpc_to_sibling(sibling, MSG_COPY_EXTENT, payload)
                    .await;
            }
        }

        let extent = self.get_extent(req.extent_id).await?;
        let mut logical_len = extent.len.load(Ordering::SeqCst);

        // TODO: manager RPC for extent_info not yet implemented
        match self.extent_info_from_manager(req.extent_id).await {
            Ok(Some(ex)) => {
                // fsync on 0→sealed transition.
                // P0-A: propagate a seal-persist failure (disk now offline)
                // rather than serving a copy from a non-durably-sealed replica.
                self.apply_extent_meta_durable(req.extent_id, &extent, &ex)
                    .await
                    .map_err(|e| {
                        (
                            StatusCode::Internal,
                            format!(
                                "copy_extent: seal not durable for extent {}: {e}",
                                req.extent_id
                            ),
                        )
                    })?;
                if req.eversion < ex.eversion {
                    return Err((
                        StatusCode::FailedPrecondition,
                        format!(
                            "eversion too low: got {}, expect >= {}",
                            req.eversion, ex.eversion
                        ),
                    ));
                }
                // P0-C: clamp to the manager's authoritative length whenever the
                // extent is SEALED (incl. sealed-empty → clamp to 0), not only
                // when sealed_length > 0 — otherwise a sealed-empty extent would
                // return stale local-residue bytes past its (0) seal point.
                if ex.sealed {
                    logical_len = logical_len.min(ex.sealed_length);
                }
            }
            Ok(None) => {
                let ev = extent.eversion.load(Ordering::SeqCst);
                // drop the `req.eversion > 0` clause to match
                // the eversion-gate invariant. The check previously skipped on
                // `req.eversion == 0`, which the eversion-gate closure for
                // `handle_read_bytes` had already identified as a
                // silent-skip loophole — `entry.eversion` defaults to 1
                // on alloc, so any `req.eversion == 0` is by construction
                // stale (or never-cached). `handle_copy_extent` is used
                // by `run_recovery_task` + `handle_re_avali`; both fetch
                // ExtentInfo from the manager before dispatching so eversion
                // is normally fresh, but a defense-in-depth check here
                // closes a future-bug class where uninitialised eversion
                // bypasses the EC-shape mismatch detection and copies
                // shard bytes as if they were full payload.
                if req.eversion < ev {
                    return Err((
                        StatusCode::FailedPrecondition,
                        format!("eversion too low: got {}, expect >= {}", req.eversion, ev),
                    ));
                }
            }
            Err(_) => {
                let ev = extent.eversion.load(Ordering::SeqCst);
                // same tightening as the Ok(None) branch above.
                if req.eversion < ev {
                    return Err((
                        StatusCode::FailedPrecondition,
                        format!("eversion too low: got {}, expect >= {}", req.eversion, ev),
                    ));
                }
            }
        }

        // refuse copy on unsealed extents. Production callers
        // (run_recovery_task, handle_re_avali) only target sealed extents
        // by design — the manager dispatches recovery/re-avali after seal.
        // Without this guard, a stray caller hitting an unsealed extent
        // could race a concurrent in-flight handle_append's
        // truncate_to_commit await window and observe a mix of pre- and
        // post-truncate bytes via file_pread_chunked below. On a sealed
        // extent the append protocol step 3 rejects concurrent appends, so
        // the race only exists for unsealed extents. Belt-and-braces.
        // P0-C: "is sealed" is the explicit flag OR a positive length (the same
        // disjunction used by the append-reject path), NOT `sealed_length == 0`.
        // This ACCEPTS a legitimate sealed-EMPTY extent (sealed=true,
        // sealed_length=0 → copies 0 bytes, logical_len clamped to 0 above)
        // while still refusing a genuinely-open extent.
        let is_sealed =
            extent.sealed.load(Ordering::SeqCst) || extent.sealed_length.load(Ordering::SeqCst) > 0;
        if !is_sealed {
            return Err((
                StatusCode::FailedPrecondition,
                format!(
                    "copy_extent on unsealed extent {} refused (not sealed)",
                    req.extent_id
                ),
            ));
        }

        // P0-C (coco re-review #2): clamp to the local seal point even on the
        // manager-unavailable fallback (Ok(None)/Err above only ran the eversion
        // check, leaving logical_len at the local file length). A sealed-EMPTY
        // extent (sealed=true, sealed_length=0) with any residual/ghost bytes in
        // its `.dat` must copy 0 bytes — never data past the seal point. The
        // Ok(Some(ex)) branch already clamped to the manager's authoritative
        // length; this guarantees the "sealed-empty → copies 0 bytes" invariant
        // holds regardless of manager reachability. Safe for sealed-with-length
        // too (local_len == sealed_length there, so the min is a no-op; a
        // not-yet-fully-recovered replica keeps returning what it has).
        if extent.sealed.load(Ordering::SeqCst) {
            logical_len = logical_len.min(extent.sealed_length.load(Ordering::SeqCst));
        }

        let offset = req.offset.min(logical_len);
        let size = if req.size == 0 {
            logical_len.saturating_sub(offset)
        } else {
            req.size.min(logical_len.saturating_sub(offset))
        };

        // copy serves a range of a (possibly sealed) extent —
        // re-open on miss.
        let cf = self
            .extent_file(&extent)
            .await
            .map_err(|e| (StatusCode::Internal, e))?;
        let data = file_pread_chunked(cf, offset, size as usize)
            .await
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;

        Ok(CopyExtentResp {
            code: CODE_OK,
            payload: Bytes::from(data),
        }
        .encode())
    }

    /// EC-convert peer-copy: bring the coordinator's local `.dat` up to
    /// `sealed_length` by streaming a full copy from a healthy peer, WITHOUT
    /// destroying the live local replica on a short/failed copy.
    ///
    /// Unlike `stream_extent_from_sources` (used by recovery, where the dest is
    /// a node being rebuilt so truncate-then-stream is fine), the EC-convert
    /// coordinator IS a live replica — we must not `set_len(0)` the live `.dat`
    /// before a complete copy is secured (coco P1). So we stream into a TEMP
    /// file (peak = one chunk, via `stream_one_source`) and atomic-rename it
    /// over `.dat` ONLY after a full `sealed_length` copy lands + fsyncs. On
    /// "no source held the full length" the live `.dat` is left untouched and we
    /// fail (the manager retries; a permanent over-seal needs operator/recovery
    /// reconciliation — EC cannot encode a partial extent). No reconcile-
    /// down here: a short consensus means EC convert must NOT proceed.
    async fn peer_copy_full_extent_to_dat(
        &self,
        extent_id: u64,
        entry: &Rc<ExtentEntry>,
        mgr_info: &ExtentInfo,
        sealed_length: u64,
    ) -> Result<(), (StatusCode, String)> {
        let disk = self
            .disk_for(entry.disk_id)
            .map_err(|e| (StatusCode::Internal, e))?;
        let dat_path = disk.extent_path(extent_id);
        let tmp_path = {
            let mut s = dat_path.clone().into_os_string();
            s.push(".peercopy.tmp");
            PathBuf::from(s)
        };
        let nodes = self
            .nodes_map_from_manager()
            .await
            .map_err(|e| (StatusCode::Unavailable, format!("nodes_map: {e}")))?;

        let mut got_full = false;
        {
            if let Some(parent) = tmp_path.parent() {
                compio::fs::create_dir_all(parent).await.map_err(|e| {
                    (
                        StatusCode::Internal,
                        format!("mkdir peercopy {extent_id}: {e}"),
                    )
                })?;
            }
            let tmp_file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)
                .await
                .map_err(|e| {
                    (
                        StatusCode::Internal,
                        format!("create peercopy tmp {extent_id}: {e}"),
                    )
                })?;
            let tmp_rc = Rc::new(tmp_file);
            for node_id in mgr_info.replicates.iter().chain(mgr_info.parity.iter()) {
                let Some((base, shard_ports)) = nodes.get(node_id) else {
                    continue;
                };
                let routed = shard_addr_for_extent(base, shard_ports, extent_id);
                let addr = &routed;
                let Ok(sock) = parse_addr(addr) else {
                    continue;
                };
                // Reset the TEMP (never the live .dat) before each attempt.
                if tmp_rc.set_len(0).await.is_err() {
                    continue;
                }
                match Self::stream_one_source(
                    sock,
                    addr,
                    extent_id,
                    mgr_info.eversion,
                    sealed_length,
                    &tmp_rc,
                    // A full replica: this path publishes a complete `.dat`,
                    // so a shard file is not a source it can use.
                    PayloadRef::in_dat(),
                    // Progress is reported by the recovery path only.
                    &|_| {},
                )
                .await
                {
                    Ok(w) if w >= sealed_length => {
                        tmp_rc.sync_data().await.map_err(|e| {
                            (
                                StatusCode::Internal,
                                format!("sync peercopy {extent_id}: {e}"),
                            )
                        })?;
                        got_full = true;
                        break;
                    }
                    Ok(short) => {
                        tracing::warn!(
                            extent_id,
                            node_id,
                            got = short,
                            want = sealed_length,
                            "EC peer-copy source SHORT"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(extent_id, node_id, err = %e, "EC peer-copy source FAILED");
                    }
                }
            }
        } // tmp_rc dropped (fd closed) before the rename

        if !got_full {
            let _ = compio::fs::remove_file(&tmp_path).await;
            return Err((
                StatusCode::FailedPrecondition,
                format!(
                    "peer-copy for extent {extent_id}: no source held the full sealed_length \
                     {sealed_length} — over-sealed / unrecoverable; live replica left intact"
                ),
            ));
        }

        // Atomic-replace: rename temp → .dat, fsync dir, reopen the handle.
        compio::fs::rename(&tmp_path, &dat_path)
            .await
            .map_err(|e| {
                (
                    StatusCode::Internal,
                    format!("rename peercopy {extent_id}: {e}"),
                )
            })?;
        if let Some(dir) = dat_path.parent() {
            compio::fs::File::open(dir)
                .await
                .map_err(|e| {
                    (
                        StatusCode::Internal,
                        format!("open dat dir {extent_id}: {e}"),
                    )
                })?
                .sync_all()
                .await
                .map_err(|e| {
                    (
                        StatusCode::Internal,
                        format!("fsync dat dir {extent_id}: {e}"),
                    )
                })?;
        }
        let new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&dat_path)
            .await
            .map_err(|e| (StatusCode::Internal, format!("reopen {extent_id}: {e}")))?;
        let len = new_file
            .metadata()
            .await
            .map(|m| m.len())
            .map_err(|e| (StatusCode::Internal, format!("metadata {extent_id}: {e}")))?;
        entry.replace_file(new_file);
        entry.note_durable_install(len);
        Ok(())
    }

    /// ACCEPT an EC conversion and run it in the BACKGROUND (same shape as
    /// `handle_require_recovery`): validate cheaply, guard against a duplicate
    /// converter, spawn, and ACK immediately. Completion is reported to the
    /// manager on the next `df` (`DfResp.ec_done`) — NOT by this RPC's return.
    ///
    /// Why not run it inline: an EC encode of a multi-GiB extent + K+M fan-out
    /// can exceed the manager's RPC timeout, and a timeout is indistinguishable
    /// from a dead coordinator — which is exactly why a stuck marker could never
    /// be auto-released. Decoupling dispatch from completion removes that
    /// ambiguity (the manager acts on a reported FACT), and stops one slow
    /// extent from stalling the manager's dispatch loop.
    async fn handle_convert_to_ec(&self, payload: Bytes) -> HandlerResult {
        let req: ConvertToEcReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // forward to owner shard.
        if !self.owns_extent(req.extent_id) {
            if let Some(sibling) = self.sibling_for_extent(req.extent_id) {
                return self
                    .forward_rpc_to_sibling(sibling, MSG_CONVERT_TO_EC, payload)
                    .await;
            }
        }

        let data_shards = req.data_shards as usize;
        let parity_shards = req.parity_shards as usize;

        // Argument validation stays INLINE so a malformed request fails loudly
        // on the RPC instead of dying in a detached task.
        if data_shards == 0 || parity_shards == 0 {
            return Err((
                StatusCode::InvalidArgument,
                "data_shards and parity_shards must be > 0".to_string(),
            ));
        }
        if req.target_addrs.len() != data_shards + parity_shards {
            return Err((
                StatusCode::InvalidArgument,
                format!(
                    "target_addrs len {} != data_shards+parity_shards {}",
                    req.target_addrs.len(),
                    data_shards + parity_shards
                ),
            ));
        }

        // The manager re-dispatches from its durable marker every ~5 s; without
        // this guard every tick would spawn another converter for the same
        // extent (they would serialise on the per-extent lock, but pile up).
        // Both answers below are CODE_OK — "accepted" and "already running" are
        // equally not-an-error — so the MESSAGE is the only place the manager can
        // learn that the previous attempt died, and which of the two states this
        // one is.
        let prior_failure = self
            .ec_last_error
            .get(&req.extent_id)
            .map(|e| e.value().clone());
        if self.ec_convert_inflight.contains_key(&req.extent_id) {
            return code_resp(
                CODE_OK,
                match &prior_failure {
                    Some(why) => format!("ec convert already running (last attempt failed: {why})"),
                    None => "ec convert already running".to_string(),
                },
            );
        }
        self.ec_convert_inflight.insert(req.extent_id, ());

        let node = self.clone();
        compio::runtime::spawn(async move {
            let extent_id = req.extent_id;
            let new_eversion = req.eversion;
            let attempt_nonce = req.attempt_nonce;
            match node.run_convert_to_ec_task(req).await {
                Ok(()) => {
                    // Report the completion for the next `df` pickup. This is
                    // the ONLY signal the manager applies the layout on. The
                    // nonce says WHICH attempt finished — without it a report
                    // that outlived its own attempt is indistinguishable from
                    // the live one's.
                    node.done.push_ec(crate::extent_rpc::EcConvertDone {
                        extent_id,
                        new_eversion,
                        attempt_nonce,
                    });
                    node.ec_last_error.remove(&extent_id);
                    tracing::info!(
                        extent_id,
                        new_eversion,
                        "EC convert done; queued for df report"
                    );
                }
                Err((_, msg)) => {
                    // No report ⇒ the manager's marker stays ⇒ it re-dispatches
                    // on its next tick. Nothing to roll back here: the 2PC's own
                    // staging/commit markers own crash-safety.
                    node.ec_last_error.insert(extent_id, msg.clone());
                    tracing::error!(
                        extent_id,
                        "EC convert failed (will be re-dispatched): {msg}"
                    );
                }
            }
            node.ec_convert_inflight.remove(&extent_id);
            node.clear_op_progress(extent_id);
        })
        .detach();

        code_resp(
            CODE_OK,
            match prior_failure {
                Some(why) => format!("ec convert accepted (previous attempt failed: {why})"),
                None => "ec convert accepted".to_string(),
            },
        )
    }

    /// The actual EC conversion (prepare + 2PC commit). Runs detached; returns
    /// `Ok(())` when the extent is durably EC-converted at `req.eversion` —
    /// INCLUDING the idempotent-skip path, which is the "adopt" case that lets a
    /// lost completion report converge on re-dispatch.
    async fn run_convert_to_ec_task(
        &self,
        req: ConvertToEcReq,
    ) -> std::result::Result<(), (StatusCode, String)> {
        let extent_id = req.extent_id;
        let data_shards = req.data_shards as usize;
        let parity_shards = req.parity_shards as usize;
        let new_eversion = req.eversion;

        // serialise concurrent EC conversion dispatches on this
        // extent. The manager-side `ec_conversion_inflight` set is purely
        // in-memory and is lost on leader failover; without this lock,
        // a deposed leader's mid-conversion + new leader's redispatch
        // could both pass the idempotency guard (because eversion has not yet
        // bumped) and race on `.ec.dat` writes. The lock entry is created
        // lazily and lives for the lifetime of the node — bounded by the
        // number of extents ever EC-converted on this shard, which is
        // the same bound as the existing `extents` DashMap (~negligible).
        // now uses the shared `extent_op_lock` helper (same
        // map, broadened semantic). handle_re_avali and the
        // delete try-lock route through the same lock.
        // Stage markers: without them there is nothing to say whether an attempt
        // never got the extent op lock or never got past the first read, and the
        // difference is a deadlock versus a slow peer.
        //
        // They are what ruled out the deadlock theory for
        // BUG-EC-CONVERT-STALL-HEALTHY-COORD. The stall's signature used to be a
        // marker pinned with attempts=0 and no error, because the failure only
        // reached this node's log; it now shows rising attempts and the reason,
        // which the manager reads off the next dispatch's response message.
        tracing::debug!(extent_id, new_eversion, "ec convert: waiting for the extent op lock");
        let convert_lock = self.get_or_create_extent_op_lock(extent_id);
        let _convert_guard = convert_lock.lock().await;
        tracing::debug!(extent_id, "ec convert: op lock held; reading the extent");

        let entry = self.get_extent(extent_id).await?;
        let mut sealed_length = entry.sealed_length.load(Ordering::SeqCst);

        // Idempotency guard: if the coordinator's eversion is already
        // at the post-EC value, a prior 2PC completed successfully
        // (commit_shard_local is the last step, so eversion bump means
        // all phases finished). Return OK so the manager's
        // apply_ec_conversion_done converges. This re-check now
        // runs UNDER the per-extent lock, so a serialized second
        // dispatch reliably observes the post-bump state.
        let local_eversion = entry.eversion.load(Ordering::SeqCst);
        if local_eversion >= req.eversion
            && sealed_length > 0
            && entry.avali.load(Ordering::SeqCst) > 0
        {
            tracing::info!(
                extent_id,
                local_eversion,
                req_eversion = req.eversion,
                sealed_length,
                "convert_to_ec idempotent skip: extent already EC-converted"
            );
            // P0-D (coco P1): the in-memory atomics satisfying this check do
            // NOT prove the sidecar is durable — a prior attempt may have
            // published them and then FAILED its `.meta` persist (the
            // fail-closed paths below error out but cannot roll the atomics
            // back). Returning OK here would let the manager commit the
            // conversion against a stale on-disk sidecar. ENSURE durability:
            // save_meta is idempotent; fail-closed if it still can't persist.
            if let Err(e) = self.save_meta(extent_id, &entry).await {
                self.mark_disk_error_for_extent(extent_id, &e);
                return Err((
                    StatusCode::Unavailable,
                    format!(
                        "extent {extent_id}: idempotent-skip .meta ensure failed (fail-closed): {e}"
                    ),
                ));
            }
            // ADOPT: already EC-converted at this eversion. Report it as DONE so
            // a completion lost before the manager's `df` pickup converges on
            // the next re-dispatch (mirrors recovery's "local copy already
            // complete — adopting" path).
            return Ok(());
        }

        // gate cross-extent EC convert concurrency. Acquired AFTER
        // the idempotent-skip check above so an already-converted
        // extent (e.g. a deposed-leader redispatch) returns OK without
        // consuming a permit. Held until the end of the function via
        // RAII (`_ec_permit`); released when the function returns or
        // unwinds. The per-extent lock above remains the
        // correctness gate against same-extent concurrent dispatches;
        // this is the new memory-safety gate against cross-extent fan
        // out. Default parallelism=1 — fully serialise. Env tunable
        // via `AUTUMN_EXTENT_EC_CONVERT_PARALLELISM` (clamped [1, 16]).
        let _ec_permit = self.concurrency_ctrl.acquire_ec_convert().await;

        // ── Has THIS attempt already staged every shard? ──
        //
        // The coordinator stages its own shard LAST, so its shard file being
        // complete means every participant's is too. Skipping then costs
        // nothing and re-reports the completion the manager may have lost.
        let coordinator_prepared = {
            let disk = self
                .disk_for(entry.disk_id)
                .map_err(|e| (StatusCode::Internal, e))?;
            // The coordinator is `target_addrs[0]`, hence shard 0.
            let staging = disk.shard_path(extent_id, 0);
            // The size check ALONE is not attempt-scoped: the same extent at the
            // same K always yields the same shard size, so a PREVIOUS attempt's
            // staging satisfies it — and reporting "done" off another attempt's
            // bytes would flip the layout onto shards this attempt never wrote.
            //
            // `new_eversion` does NOT scope this to an attempt: it is
            // `live + 1`, and an abandoned attempt never bumped the extent, so
            // a reissued attempt is handed the same value. The nonce is the
            // attempt's own identity, so require the marker to name THIS
            // attempt; anything else (including a pre-nonce marker written by
            // an older binary) re-prepares, which is always safe.
            let marker_matches = self.read_ec_prepared_marker(&disk, extent_id).await
                == Some((new_eversion, req.attempt_nonce));
            if !marker_matches {
                false
            } else if let Ok(meta) = compio::fs::metadata(&staging).await {
                // Validate shard size matches expectation. If sealed_length
                // is not yet known locally, we can't validate — fall through
                // to the full path which syncs from manager first.
                if sealed_length > 0 {
                    let expected_shard_size =
                        crate::erasure::shard_size(sealed_length as usize, data_shards);
                    meta.len() == expected_shard_size as u64
                } else {
                    false
                }
            } else {
                false
            }
        };

        if coordinator_prepared {
            tracing::info!(
                extent_id,
                "EC 2PC: coordinator staging file found, skipping to commit phase"
            );
        } else {
            // ── Full prepare path: read, encode, distribute ──

            // sync sealed_length / eversion from manager.
            let mgr_info_opt = self
                .extent_info_from_manager(extent_id)
                .await
                .ok()
                .flatten();
            if let Some(mgr_info) = mgr_info_opt.as_ref() {
                // P0-C: include the explicit `sealed` flag so a sealed-EMPTY
                // extent also has its seal persisted here (consistency with the
                // commit-reconcile + recovery paths).
                if mgr_info.sealed || mgr_info.sealed_length > 0 {
                    entry
                        .sealed_length
                        .store(mgr_info.sealed_length, Ordering::SeqCst);
                    // P0-C: sealed (incl. sealed-empty) ⇒ set the flag.
                    entry.sealed.store(true, Ordering::SeqCst);
                    entry.eversion.store(mgr_info.eversion, Ordering::SeqCst);
                    entry.avali.store(mgr_info.avali, Ordering::SeqCst);
                    // P0-D: the seal we just applied gates the EC encode below
                    // — proceeding with a NON-DURABLE seal lets a crash
                    // mid-convert restart this extent as OPEN while shards may
                    // already be distributed. Fail-closed: refuse the convert
                    // (the manager's dispatch loop retries; the per-extent
                    // lock + idempotency make the redo safe) and mark
                    // the disk offline (sidecar-persist I/O error).
                    if let Err(e) = self.save_meta(extent_id, &entry).await {
                        self.mark_disk_error_for_extent(extent_id, &e);
                        return Err((
                            StatusCode::Unavailable,
                            format!(
                                "extent {extent_id}: seal .meta persist failed before EC convert (fail-closed): {e}"
                            ),
                        ));
                    }
                    sealed_length = mgr_info.sealed_length;
                    tracing::info!(
                        extent_id,
                        sealed_length,
                        "applied seal from manager for EC convert"
                    );
                }
            }

            if sealed_length == 0 {
                return Err((
                    StatusCode::FailedPrecondition,
                    format!("extent {extent_id} is not sealed — cannot EC convert"),
                ));
            }

            // Peer-copy gap if local file is short.
            let local_len = entry.len.load(Ordering::SeqCst);
            // A short local copy is fetched from a peer before encoding.
            //
            // There used to be a branch here that treated `local_len ==
            // ceil(sealed_length/K)` as "this node crashed between
            // rename(.ec.dat → .dat) and save_meta", fixed up the meta, and
            // SKIPPED the entire prepare — no encode, no WriteShard to anyone —
            // then reported the conversion done. Under copy-on-write no rename
            // ever happens, so its premise is unreachable, but its TRIGGER was
            // not: a lagging coordinator (sealed-over-reachable legitimately
            // seals above a down node's length, which is why the peer-copy
            // below exists) whose length happened to equal the shard size would
            // report done having staged nothing. The nonce and reporter checks
            // authenticate the ATTEMPT, not the WORK, so the flip committed
            // onto a layout no node held a shard for: every read
            // CODE_PAYLOAD_NOT_HERE, 0 of K shards for reconstruct, and no way
            // back.
            if local_len < sealed_length {
                let mgr_info = mgr_info_opt.ok_or_else(|| {
                    (
                        StatusCode::Unavailable,
                        format!(
                            "extent {extent_id} local_len={local_len} < sealed_length={sealed_length} \
                             and manager unreachable — cannot peer-copy"
                        ),
                    )
                })?;
                // Stream the missing extent from a peer chunk-by-chunk into a
                // TEMP file and atomic-replace `.dat` only after a full copy is
                // secured — peak = one chunk (load-bearing at 16+ GiB) AND the
                // live local replica is never destroyed on a short/failed copy
                // (coco P1). Replaces the old whole-`Vec`
                // `fetch_full_extent_from_sources` materialization.
                self.peer_copy_full_extent_to_dat(extent_id, &entry, &mgr_info, sealed_length)
                    .await?;
                tracing::info!(
                    extent_id,
                    local_len,
                    sealed_length,
                    "peer-copied missing tail before EC convert (streamed to temp, atomic-replace)"
                );
            }

            {
                // ── Phase 1 (prepare): CHUNKED RS-encode + streamed fanout ──
                //
                // RS over GF(256) is byte-wise per offset, so each shard is
                // built+distributed one stripe at a time. Peak RAM = (K+M) ×
                // stripe (was ~2× the whole extent for the old read-all +
                // ec_encode + whole-shard WriteShard) — AND each stripe's
                // WriteShard payload stays under the frame `payload_len: u32`
                // ceiling, so EC convert works for >4 GiB shards (16+ GiB
                // extents) where a whole-shard WriteShard would overflow.
                let per_shard = crate::erasure::shard_size(sealed_length as usize, data_shards);
                let stripe_bytes = ec_encode_stripe_bytes();
                // EC converts a SEALED source extent — resolve
                // (re-open on miss) + pin its fd once for the whole stripe scan.
                let ecf = self
                    .extent_file(&entry)
                    .await
                    .map_err(|e| (StatusCode::Internal, e))?;
                // Claim staging for THIS attempt on the coordinator's OWN node,
                // exactly as every remote target does through write_shard. The
                // coordinator writes its shard 0 locally, bypassing that RPC, so
                // without this its node holds staged shards it has no record of:
                // a superseded attempt's local stripes go unordered against its
                // successor's, and reconcile's "a verdict saying .dat while an
                // attempt stages here predates the attempt" guard reads false
                // here and deletes the shard being written.
                if !self.claim_ec_staging(extent_id, req.attempt_nonce) {
                    return Err((
                        StatusCode::FailedPrecondition,
                        format!(
                            "extent {extent_id}: a newer EC attempt already stages on this node"
                        ),
                    ));
                }
                let mut s = 0usize;
                while s < per_shard {
                    let stripe_len = (per_shard - s).min(stripe_bytes);
                    // Read the K data-shard sub-ranges at shard-offset `s` from
                    // the local `.dat`. Data shard i covers
                    // `[i*per_shard, (i+1)*per_shard)` of the original payload;
                    // bytes past `sealed_length` are zero-padding (the original
                    // is only `sealed_length` bytes), so a short read is filled
                    // with zeros — identical to `ec_encode`'s zero-fill.
                    let mut data_bufs: Vec<Vec<u8>> = Vec::with_capacity(data_shards);
                    for i in 0..data_shards {
                        let start = i * per_shard + s;
                        let avail = (sealed_length as usize)
                            .saturating_sub(start)
                            .min(stripe_len);
                        let mut buf = vec![0u8; stripe_len];
                        if avail > 0 {
                            let read = file_pread_chunked(ecf.clone(), start as u64, avail)
                                .await
                                .map_err(|e| {
                                (
                                    StatusCode::Internal,
                                    format!("read extent {extent_id} @ {start}: {e}"),
                                )
                            })?;
                            let n = read.len().min(stripe_len);
                            buf[..n].copy_from_slice(&read[..n]);
                        }
                        data_bufs.push(buf);
                    }

                    // offload RS to a blocking thread. Move `data_bufs` in
                    // and hand it back alongside the parity so the fanout below
                    // doesn't re-clone the data stripes.
                    let pshards = parity_shards;
                    let (data_bufs, parity): (Vec<Vec<u8>>, Vec<Vec<u8>>) =
                        compio::runtime::spawn_blocking(
                            move || -> std::result::Result<_, String> {
                                let refs: Vec<&[u8]> =
                                    data_bufs.iter().map(|v| v.as_slice()).collect();
                                let parity = crate::erasure::ec_encode_stripe(&refs, pshards)
                                    .map_err(|e| e.to_string())?;
                                Ok((data_bufs, parity))
                            },
                        )
                        .await
                        .map_err(|_| {
                            (
                                StatusCode::Internal,
                                "ec_encode_stripe task panicked".to_string(),
                            )
                        })?
                        .map_err(|e| {
                            (
                                StatusCode::Internal,
                                format!("ec_encode_stripe failed: {e}"),
                            )
                        })?;

                    // Fan the stripe out: REMOTE shards (data 1..K, parity
                    // K..K+M) first, coordinator's own shard 0 LAST so that
                    // coord-staging-full ⇒ every participant durably staged
                    // every stripe (the `coordinator_prepared` skip + 2PC commit
                    // ordering depend on this). owner_epoch fence as before.
                    let shard_off = s as u64;
                    for i in 1..(data_shards + parity_shards) {
                        let payload: Bytes = if i < data_shards {
                            Bytes::from(data_bufs[i].clone())
                        } else {
                            Bytes::from(parity[i - data_shards].clone())
                        };
                        let target_addr = &req.target_addrs[i];
                        let ws_req = WriteShardReq {
                            extent_id,
                            shard_index: i as u32,
                            sealed_length,
                            eversion: new_eversion,
                            owner_epoch: req.owner_epoch,
                            shard_offset: shard_off,
                            attempt_nonce: req.attempt_nonce,
                            payload,
                        };
                        let sock = parse_addr(target_addr).map_err(|e| {
                            (
                                StatusCode::Internal,
                                format!("parse addr {target_addr}: {e}"),
                            )
                        })?;
                        let label = format!("WriteShard to {target_addr} shard {i} @ {shard_off}");
                        ec_2pc_participant_rpc(
                            sock,
                            MSG_WRITE_SHARD,
                            ws_req.encode(),
                            &label,
                            |b| {
                                WriteShardResp::decode(b).map(|r| r.code).map_err(|e| {
                                    (
                                        StatusCode::Internal,
                                        format!("decode write_shard resp: {e}"),
                                    )
                                })
                            },
                        )
                        .await?;
                    }

                    // Coordinator's own shard 0 stripe, written LAST.
                    self.write_shard_stripe_local(
                        extent_id,
                        0,
                        shard_off,
                        sealed_length,
                        new_eversion,
                        Bytes::from(data_bufs[0].clone()),
                    )
                    .await?;

                    s += stripe_len;
                    // One sample per stripe (64 MiB by default), never per
                    // byte. `per_shard` is this node's whole shard, so the
                    // ratio is this conversion's real completion.
                    self.note_op_progress(
                        extent_id,
                        autumn_rpc::manager_rpc::OP_KIND_EC_CONVERT,
                        s as u64,
                        per_shard as u64,
                    );
                }

                // Prepare finished for ALL nodes (the coordinator stages itself
                // last). Stamp WHICH attempt produced this staging so a later
                // re-dispatch can tell "my completed prepare" from "some other
                // attempt's leftovers" and skip only in the former case.
                // Best-effort: a failure here only costs a re-prepare.
                if let Ok(disk) = self.disk_for(entry.disk_id) {
                    if let Err(e) = self
                        .write_ec_prepared_marker(&disk, extent_id, new_eversion, req.attempt_nonce)
                        .await
                    {
                        tracing::warn!(
                            extent_id,
                            "ec prepared-marker write failed (will re-prepare on retry): {e}"
                        );
                    }
                }
                tracing::info!(
                    extent_id,
                    per_shard,
                    stripe_bytes,
                    "EC 2PC phase 1 (prepare) complete on all nodes (chunked)"
                );
            } // !recovered
        }

        // ── No commit phase. ──
        //
        // Nothing was published, so nothing has to be flipped node-by-node: the
        // shards are additive files and every `.dat` is untouched. The single
        // commit point is the manager's layout flip, driven by the completion
        // report below.
        //
        // This is what removes the middle state the old scheme could not
        // classify. A per-node rename left the cluster with "some renamed, some
        // not", and a crashed coordinator gave nobody the authority to decide
        // roll-forward or roll-back — which is what made a stuck marker
        // un-releasable. Abandoning an attempt now costs a delete of files no
        // reader is pointed at.
        //
        // The receiving side (`handle_commit_ec_shard` / `commit_shard_local` /
        // the `ec.commit` marker replay) is RETAINED as repair code for nodes
        // upgraded mid-rename; it is simply never driven from here.
        tracing::info!(
            extent_id,
            new_eversion,
            attempt_nonce = req.attempt_nonce,
            "EC shards staged on every target; awaiting the manager's layout flip"
        );

        Ok(())
    }

    async fn handle_write_shard(&self, payload: Bytes) -> HandlerResult {
        let req = WriteShardReq::decode(payload.clone())
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        // forward to owner shard.
        if !self.owns_extent(req.extent_id) {
            if let Some(sibling) = self.sibling_for_extent(req.extent_id) {
                return self
                    .forward_rpc_to_sibling(sibling, MSG_WRITE_SHARD, payload)
                    .await;
            }
        }

        // owner-lock owner_epoch fence. `owner_epoch == 0` keeps the
        // legacy no-fence behaviour; non-zero is rejected when the
        // local owner_epoch has moved ahead (e.g., a fence on the
        // coord node bumped owner-lock revisions on every extent the
        // coord touched, so a revived ghost coord's WriteShard with the
        // old owner_epoch is refused).
        // Serialise against a concurrent commit/convert on the SAME extent
        // (a takeover-driven commit racing a resumed coordinator's local one
        // would otherwise both pass their staging checks and one rename would
        // fault a healthy disk). Same lock the convert path takes.
        let op_lock = self.get_or_create_extent_op_lock(req.extent_id);
        let _op_guard = op_lock.lock().await;

        // ATTEMPT ORDERING: refuse a stripe from an attempt older than the one
        // already staging here. The `owner_epoch` fence above only fires when
        // the ex-coordinator was FENCED; a coordinator whose marker was merely
        // released (the routine case — its node went offline, or the assignment
        // was re-derived) keeps its epoch and would otherwise interleave its
        // stripes with its successor's into the same staging file. Checked
        // under the op lock so the compare-and-record is atomic against a
        // concurrent stripe. Nonce 0 = a pre-nonce peer: left unordered rather
        // than blocking it.
        if !self.claim_ec_staging(req.extent_id, req.attempt_nonce) {
            tracing::warn!(
                extent_id = req.extent_id,
                shard_index = req.shard_index,
                stripe_nonce = req.attempt_nonce,
                "write_shard from a SUPERSEDED conversion attempt — refusing"
            );
            return Ok(WriteShardResp {
                code: CODE_LOCKED_BY_OTHER,
            }
            .encode());
        }

        if let Ok(entry) = self.ensure_extent(req.extent_id).await {
            // Re-read UNDER the lock: a fence may have landed while we waited,
            // and the staging write below must not proceed on a stale epoch.
            // (`owner_epoch == 0` keeps the legacy no-fence behaviour.)
            if req.owner_epoch > 0 {
                let last = entry.owner_epoch.load(Ordering::SeqCst);
                if req.owner_epoch < last {
                    // Loud, like the attempt-nonce refusal beside it. A silent
                    // return leaves the coordinator's log as the only trace, and
                    // it can only report a code — so a conversion stuck behind
                    // this fence looks, from every log in the cluster, like a
                    // generic transient error being retried.
                    tracing::warn!(
                        extent_id = req.extent_id,
                        shard_index = req.shard_index,
                        req_owner_epoch = req.owner_epoch,
                        local_owner_epoch = last,
                        "write_shard fenced: the caller's owner_epoch is below this \
                         extent's floor — refusing"
                    );
                    return Ok(WriteShardResp {
                        code: CODE_LOCKED_BY_OTHER,
                    }
                    .encode());
                }
            }
            // META-FAILCLOSED: never stage onto a quarantined extent — a later
            // commit would `save_meta` and silently CLEAR the quarantine,
            // bypassing the fail-closed contract. Unconditional: this must hold
            // for legacy epoch-0 callers too.
            if entry.corrupt_meta.load(Ordering::SeqCst) {
                return Err((
                    StatusCode::FailedPrecondition,
                    format!("extent {}: quarantined (.meta corrupt)", req.extent_id),
                ));
            }
        }

        self.write_shard_stripe_local(
            req.extent_id,
            req.shard_index as usize,
            req.shard_offset,
            req.sealed_length,
            req.eversion,
            req.payload,
        )
        .await?;

        Ok(WriteShardResp { code: CODE_OK }.encode())
    }

        /// Queue an EC completion as the owning shard would, for tests that need to
    /// prove the node's `df` shard drains it.
    pub fn test_push_ec_done(&self, extent_id: u64, new_eversion: u64) {
        self.done.push_ec(crate::extent_rpc::EcConvertDone {
            extent_id,
            new_eversion,
            attempt_nonce: 0,
        });
    }

    /// Drain queued EC completions as `handle_df` does, as `(extent_id, eversion)`.
    pub fn test_take_ec_done(&self) -> Vec<(u64, u64)> {
        self.done
            .take_ec()
            .into_iter()
            .map(|d| (d.extent_id, d.new_eversion))
            .collect()
    }

    /// Seal an extent locally, for integration tests that need one in the
    /// post-seal shape (sealed, at a chosen length and eversion).
    ///
    /// In production only a manager-driven path seals an EN's extent
    /// (`apply_extent_meta_durable`, reached via re_avali / the append
    /// seal-confirm branch / recovery writeback). Tests used to get here by
    /// driving the EC commit phase, which no longer exists — conversion stages
    /// an additive shard file and the manager's layout flip is the only commit
    /// point — so they set the state directly instead of through a mechanism
    /// that is not what they are testing.
    pub async fn test_seal_local(
        &self,
        extent_id: u64,
        sealed_length: u64,
        eversion: u64,
    ) -> std::result::Result<(), String> {
        let entry = self.get_extent(extent_id).await.map_err(|(_, m)| m)?;
        entry.sealed_length.store(sealed_length, Ordering::SeqCst);
        entry.sealed.store(true, Ordering::SeqCst);
        entry.eversion.store(eversion, Ordering::SeqCst);
        entry.avali.store(1, Ordering::SeqCst);
        self.save_meta(extent_id, &entry).await
    }

    /// Seal through the REAL durable applier, for tests that need the side
    /// effects a seal actually has (the `.meta` persist AND the content
    /// checksum sidecar) rather than `test_seal_local`'s direct state poke.
    pub async fn test_seal_durable(
        &self,
        extent_id: u64,
        sealed_length: u64,
        eversion: u64,
    ) -> std::result::Result<(), String> {
        let entry = self.get_extent(extent_id).await.map_err(|(_, m)| m)?;
        entry
            .coalescer
            .last_synced
            .fetch_max(sealed_length, Ordering::SeqCst);
        let ex = ExtentInfo {
            extent_id,
            sealed: true,
            sealed_length,
            eversion,
            avali: 1,
            ..Default::default()
        };
        self.apply_extent_meta_durable(extent_id, &entry, &ex)
            .await
            .map(|_| ())
    }

    /// Run ONE reconcile round against the manager, for integration tests that
    /// need the production path — including where the staging tick is sampled —
    /// rather than the applier alone. The real trigger is a 5-minute sweep.
    /// A failure is logged the way the sweep logs it and swallowed here; the
    /// caller asserts on what happened to the files.
    pub async fn test_reconcile_once(&self) {
        if let Err(e) = self.reconcile_orphans_with_manager().await {
            tracing::warn!(error = %e, "test_reconcile_once: reconcile failed");
        }
    }

    /// The node's staging tick — what a reconcile samples before it asks the
    /// manager for a verdict. Integration tests take it at the point their
    /// simulated question goes out, so a verdict can be made to predate a
    /// staging (or not) on purpose.
    pub fn test_staging_tick(&self) -> u64 {
        self.ec_stage_tick.get()
    }

    /// Run the placement-cleanup pass directly, for integration tests that
    /// exercise the destructive half without standing up a manager to answer a
    /// reconcile. `(extent_id, payload_location, shard_index)` per placement,
    /// with the staging tick the question was asked at.
    pub async fn test_apply_placements(
        &self,
        placements: &[(u64, u8, u32)],
        staging_tick_at_ask: u64,
    ) {
        let ps: Vec<manager_rpc::ExtentPlacement> = placements
            .iter()
            .map(|(extent_id, payload_location, shard_index)| manager_rpc::ExtentPlacement {
                extent_id: *extent_id,
                payload_location: *payload_location,
                shard_index: *shard_index,
            })
            .collect();
        self.apply_placements(&ps, staging_tick_at_ask).await;
    }

    /// Expose the recovery_inflight map for integration tests. The Rc clone
    /// shares the same underlying DashMap, so inserts made after `serve` is
    /// spawned are visible to the running connection handler on the same
    /// compio thread. Only intended for test use.
    pub fn clone_recovery_inflight(
        &self,
    ) -> std::rc::Rc<dashmap::DashMap<u64, crate::extent_rpc::RecoveryTask>> {
        self.recovery_inflight.clone()
    }
}

// ─── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod enospc_disk_health_tests {
    use super::*;

    /// NB: health cells are shared per-path process-wide (multi-shard
    /// coupling, coco P1) — each test must use a distinct path.
    fn disk(path: &str) -> DiskFS {
        DiskFS::with_disk_id(PathBuf::from(path), 7)
    }

    /// ENOSPC/EDQUOT classify as capacity in every wrapping this codebase
    /// produces (typed io::Error Display, fsync-coalescer strings, anyhow
    /// chains); everything else is a fault.
    #[test]
    fn classification_matches_capacity_errors_only() {
        let enospc = std::io::Error::from_raw_os_error(28).to_string();
        let edquot = std::io::Error::from_raw_os_error(122).to_string();
        let eio = std::io::Error::from_raw_os_error(5).to_string();
        assert!(ExtentNode::is_disk_full_error(&enospc), "{enospc}");
        assert!(ExtentNode::is_disk_full_error(&edquot), "{edquot}");
        assert!(ExtentNode::is_disk_full_error(&format!(
            "write staging 42/1: {enospc}"
        )));
        assert!(!ExtentNode::is_disk_full_error(&eio), "{eio}");
        assert!(!ExtentNode::is_disk_full_error("fsync coalescer canceled"));
        assert!(!ExtentNode::is_disk_full_error("unclassified I/O failure"));
    }

    /// Full is recoverable and gates allocation; Faulted is terminal.
    #[test]
    fn health_state_machine_transitions() {
        let d = disk("/tmp/enospc-test-state-machine");
        assert_eq!(d.health(), DiskHealth::Online);
        assert!(d.online() && d.allocatable());

        // Online -> Full: still "online" (serves reads) but not allocatable.
        d.set_full();
        assert_eq!(d.health(), DiskHealth::Full);
        assert!(d.online());
        assert!(!d.allocatable());

        // Full -> Online via the sweep's clear.
        assert!(d.try_clear_full());
        assert_eq!(d.health(), DiskHealth::Online);
        assert!(d.allocatable());

        // Faulted is terminal: set_full must not downgrade it, and
        // try_clear_full must not resurrect it.
        d.set_faulted();
        assert_eq!(d.health(), DiskHealth::Faulted);
        assert!(!d.online() && !d.allocatable());
        d.set_full();
        assert_eq!(
            d.health(),
            DiskHealth::Faulted,
            "set_full downgraded Faulted"
        );
        assert!(!d.try_clear_full());
        assert_eq!(
            d.health(),
            DiskHealth::Faulted,
            "clear_full resurrected Faulted"
        );
    }

    /// coco P1 (multi-shard): two DiskFS instances for the SAME dir share
    /// one health cell — shard A marking Full must be visible to shard B.
    #[test]
    fn health_is_shared_per_directory_across_instances() {
        let a = disk("/tmp/enospc-test-shared");
        let b = DiskFS::with_disk_id(PathBuf::from("/tmp/enospc-test-shared"), 7);
        let other = disk("/tmp/enospc-test-shared-other");
        a.set_full();
        assert_eq!(b.health(), DiskHealth::Full, "sibling shard view diverged");
        assert!(!b.allocatable());
        assert_eq!(other.health(), DiskHealth::Online, "distinct dir coupled");
        assert!(b.try_clear_full());
        assert_eq!(a.health(), DiskHealth::Online);
    }
}

#[cfg(test)]
mod sealed_append_guard_tests {
    use super::*;

    /// handle_append returns CODE_PRECONDITION when sealed_length > 0.
    ///
    /// The post-truncate seal recheck in handle_append is inserted
    /// after `Self::truncate_to_commit` completes. That recheck fires in the
    /// async window between the truncate await and the subsequent file_pwrite —
    /// a concurrent `apply_extent_meta_durable` may have landed a fresh seal
    /// during the truncate I/O. In a single-threaded compio test we cannot
    /// inject that concurrency, so this test exercises the nearest-equivalent
    /// path: the _early_ seal check at step 3 of handle_append, which guards
    /// the same CODE_PRECONDITION response. It confirms:
    ///
    ///   (a) handle_append correctly returns CODE_PRECONDITION when
    ///       sealed_length > 0 (whatever the code path that fires it),
    ///   (b) the call does NOT panic or produce CODE_OK.
    ///
    /// The post-truncate recheck (at line ~2434) is validated
    /// by code inspection: it is structurally identical to the recheck in
    /// build_append_future (lines 882-898) and fires on the same atomics.
    #[compio::test]
    async fn handle_append_rejects_sealed_extent_with_low_commit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = ExtentNodeConfig::new(dir.path().to_path_buf(), 1);
        let node = ExtentNode::new(config).await.expect("ExtentNode::new");

        // Allocate extent 9001 via the handler.
        let alloc_payload = rkyv_encode(&AllocExtentReq { extent_id: 9001 });
        let alloc_result = node.handle_alloc_extent(alloc_payload).await;
        assert!(alloc_result.is_ok(), "alloc_extent should succeed");

        // Write 100 bytes at eversion=1, owner_epoch=0, commit=0 (no truncation).
        let write_req = AppendReq {
            extent_id: 9001,
            eversion: 1,
            commit: 0,
            owner_epoch: 0,
            payload: Bytes::from(vec![0u8; 100]),
        };
        let write_result = node.handle_append(write_req.encode()).await;
        assert!(write_result.is_ok(), "first append should succeed");
        let write_resp = AppendResp::decode(write_result.unwrap()).expect("decode AppendResp");
        assert_eq!(write_resp.code, CODE_OK, "first append code == CODE_OK");
        assert_eq!(write_resp.end, 100, "extent len == 100 after first append");

        // Simulate a concurrent seal arriving: set sealed_length = 100, avali = 1.
        // In production this is done by apply_extent_meta_durable triggered from
        // handle_re_avali or another handle_append's pre-truncate manager check.
        {
            let entry = node.extents.get(&9001).expect("extent 9001 in map");
            entry.sealed_length.store(100, Ordering::SeqCst);
            entry.avali.store(1, Ordering::SeqCst);
        }

        // Now attempt an append with commit=50 (< current len=100): truncation
        // branch is entered. The early sealed check (step 3 of handle_append)
        // fires before truncation starts and returns CODE_PRECONDITION, which is
        // the same CODE_PRECONDITION the post-truncate recheck would
        // return if the seal had arrived DURING the truncate await instead.
        let stale_req = AppendReq {
            extent_id: 9001,
            eversion: 1,
            commit: 50,
            owner_epoch: 0,
            payload: Bytes::from(b"x".to_vec()),
        };
        let stale_result = node.handle_append(stale_req.encode()).await;
        assert!(
            stale_result.is_ok(),
            "handle_append should not error on sealed extent"
        );
        let stale_resp = AppendResp::decode(stale_result.unwrap()).expect("decode AppendResp");
        assert_eq!(
            stale_resp.code, CODE_PRECONDITION,
            "handle_append on sealed extent must return CODE_PRECONDITION"
        );
    }

    /// P0-C: a sealed-EMPTY extent (sealed=true, sealed_length=0 — e.g. a
    /// CoW-shared empty tail frozen by split/merge) must REJECT appends both
    /// immediately AND after an extent-node restart. Pre-P0-C the `.meta`
    /// sidecar only stored `sealed_length`, so on reload the extent looked open
    /// (sealed_length=0 → avali=0) and a stale/ghost writer could append to it
    /// — later surfacing as `stale_vp_offset_past_sealed_length sealed_length=0`
    /// when a child partition's SST/VP referenced offset>0.
    #[compio::test]
    async fn p0c_sealed_empty_survives_restart_and_rejects_append() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();
        let ghost = || AppendReq {
            extent_id: 7001,
            eversion: 2,
            commit: 0,
            owner_epoch: 0,
            payload: Bytes::from(b"ghost".to_vec()),
        };

        // ---- node #1: alloc an open extent, then apply a sealed-EMPTY seal ----
        {
            let config = ExtentNodeConfig::new(path.clone(), 1);
            let node = ExtentNode::new(config).await.expect("node1");
            node.handle_alloc_extent(rkyv_encode(&AllocExtentReq { extent_id: 7001 }))
                .await
                .expect("alloc");

            let ex = ExtentInfo {
                extent_id: 7001,
                sealed: true,
                sealed_length: 0,
                avali: 1,
                eversion: 2,
                ..Default::default()
            };
            let entry = node.extents.get(&7001).expect("entry").clone();
            let changed = node
                .apply_extent_meta_durable(7001, &entry, &ex)
                .await
                .expect("apply_extent_meta_durable should succeed");
            assert!(changed, "sealed-empty must register as a durable seal");

            let resp =
                AppendResp::decode(node.handle_append(ghost().encode()).await.unwrap()).unwrap();
            assert_eq!(
                resp.code, CODE_PRECONDITION,
                "sealed-empty must reject appends pre-restart"
            );
        }

        // ---- node #2: reload the SAME dir; the seal must persist ----
        {
            let config = ExtentNodeConfig::new(path.clone(), 1);
            let node = ExtentNode::new(config).await.expect("node2 reload");
            let entry = node.extents.get(&7001).expect("extent 7001 must reload");
            assert!(
                entry.sealed.load(Ordering::SeqCst),
                "P0-C: sealed flag must survive the restart"
            );
            assert_eq!(
                entry.sealed_length.load(Ordering::SeqCst),
                0,
                "still sealed-empty after reload"
            );
            drop(entry);

            let resp =
                AppendResp::decode(node.handle_append(ghost().encode()).await.unwrap()).unwrap();
            assert_eq!(
                resp.code, CODE_PRECONDITION,
                "P0-C: sealed-empty must reject a ghost append AFTER restart"
            );
        }
    }

    /// A shard-only holder (EC-converted extent whose pre-conversion `.dat`
    /// the reconcile cleanup reclaimed) must still be able to persist a
    /// manager seal. `apply_extent_meta_durable` used to resolve the `.dat`
    /// fd unconditionally for its pre-persist fsync; with no `.dat` that open
    /// fails, so every heal path (append eversion-refresh, re_avali, the
    /// layout-commit seal persist) errored forever and the holder's `.meta`
    /// kept `sealed=0 / eversion=1` under a live shard file — reloading the
    /// extent as OPEN on every boot.
    #[compio::test]
    async fn ec_shard_only_holder_seal_persists_without_dat() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();

        {
            let config = ExtentNodeConfig::new(path.clone(), 1);
            let node = ExtentNode::new(config).await.expect("node");

            // Stage a shard the way a conversion participant does, then mimic
            // the post-flip reconcile's `.dat` reclaim so only the shard file
            // remains.
            node.write_shard_stripe_local(4242, 1, 0, 8, 2, Bytes::from(vec![7u8; 4]))
                .await
                .expect("stage shard stripe");
            let entry = node.extents.get(&4242).expect("entry").clone();
            entry.has_dat.store(false, Ordering::SeqCst);
            entry.len.store(0, Ordering::SeqCst);
            *entry.file.borrow_mut() = None;
            let disk = node.disk_for(entry.disk_id).expect("disk");
            compio::fs::remove_file(&disk.extent_path(4242))
                .await
                .expect("reclaim .dat");
            entry
                .payload_location
                .store(PAYLOAD_LOCATION_IN_SHARD_FILE, Ordering::SeqCst);

            let ex = ExtentInfo {
                extent_id: 4242,
                sealed: true,
                sealed_length: 8,
                eversion: 3,
                avali: 0b11,
                ec_converted: true,
                ..Default::default()
            };
            node.apply_extent_meta_durable(4242, &entry, &ex)
                .await
                .expect("a shard-only holder must persist the seal without a .dat");
        }

        // Restart: the persisted seal must reload with the shard-only entry.
        {
            let config = ExtentNodeConfig::new(path, 1);
            let node = ExtentNode::new(config).await.expect("node reload");
            let entry = node.extents.get(&4242).expect("shard-only extent must reload");
            assert!(
                entry.sealed.load(Ordering::SeqCst),
                "seal flag must survive the restart"
            );
            assert_eq!(entry.sealed_length.load(Ordering::SeqCst), 8);
            assert_eq!(entry.eversion.load(Ordering::SeqCst), 3);
            assert!(
                !entry.has_dat.load(Ordering::SeqCst),
                "still a shard-only holder after reload"
            );
        }
    }

    /// Sealing writes a sidecar that describes the sealed content, and a
    /// later bit flip in the `.dat` is caught by it.
    ///
    /// The `.meta` CRC cannot do this: it covers its own 48 metadata bytes, so
    /// it still validates perfectly while the value region rots underneath it.
    #[compio::test]
    async fn sealing_records_content_checksums_that_catch_a_later_flip() {
        let dir = tempfile::tempdir().expect("tmp");
        let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1))
            .await
            .expect("node");
        let eid = 5150u64;
        let content: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();

        let entry = node.ensure_extent(eid).await.expect("entry");
        let f = node.extent_file(&entry).await.expect("file");
        file_pwrite_chunked(f, 0, Bytes::from(content.clone()))
            .await
            .expect("write");
        entry.len.store(content.len() as u64, Ordering::SeqCst);
        entry.has_dat.store(true, Ordering::SeqCst);
        // The write above IS on disk, so the coalescer's fsync high-water must
        // say so — the hash refuses to describe bytes it cannot prove durable.
        entry
            .coalescer
            .last_synced
            .store(content.len() as u64, Ordering::SeqCst);

        let ex = ExtentInfo {
            extent_id: eid,
            sealed: true,
            sealed_length: content.len() as u64,
            eversion: 2,
            avali: 0b1,
            ..Default::default()
        };
        node.apply_extent_meta_durable(eid, &entry, &ex)
            .await
            .expect("seal");

        let ck = node
            .load_extent_checksums(eid, &entry, content.len() as u64)
            .await
            .expect("sealing must leave a sidecar describing the sealed content");
        assert_eq!(ck.verify_read(0, &content), Ok(1), "clean content verifies");

        // The same read, one byte different, is caught — and the block index
        // points at where it happened rather than just saying "somewhere".
        let mut rotted = content.clone();
        rotted[12_345] ^= 0x01;
        let bad = ck
            .verify_read(0, &rotted)
            .expect_err("a flipped byte must not verify");
        assert_eq!(bad.block, 0);
        assert_eq!(bad.offset, 0);
    }

    /// A read whose blocks fail their checksum must NOT be answered with the
    /// bytes. Serving them with `CODE_OK` is the one outcome a caller cannot
    /// defend against — it has no way to tell those bytes from correct ones.
    ///
    /// Failing instead is what routes around the damage: it is the same error
    /// shape a pread failure already returns, so the client's existing replica
    /// rotation carries the read to a healthy copy.
    #[compio::test]
    async fn a_sealed_read_that_fails_its_checksum_is_refused_not_served() {
        let dir = tempfile::tempdir().expect("tmp");
        let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1))
            .await
            .expect("node");
        let eid = 5152u64;
        let content: Vec<u8> = (0..30_000u32).map(|i| (i % 241) as u8).collect();
        let entry = node.ensure_extent(eid).await.expect("entry");
        let f = node.extent_file(&entry).await.expect("file");
        file_pwrite_chunked(f, 0, Bytes::from(content.clone()))
            .await
            .expect("write");
        entry.len.store(content.len() as u64, Ordering::SeqCst);
        entry.has_dat.store(true, Ordering::SeqCst);
        entry
            .coalescer
            .last_synced
            .store(content.len() as u64, Ordering::SeqCst);
        let ex = ExtentInfo {
            extent_id: eid,
            sealed: true,
            sealed_length: content.len() as u64,
            eversion: 2,
            avali: 0b1,
            ..Default::default()
        };
        node.apply_extent_meta_durable(eid, &entry, &ex).await.expect("seal");

        let whole = |ev: u64| {
            ReadBytesReq::new(eid, ev, 0, content.len() as u64, PayloadRef::in_dat()).encode()
        };
        // Clean: the read is served and the bytes are right.
        let resp = node.handle_read_bytes(whole(2)).await.expect("clean read");
        let decoded = ReadBytesResp::decode(resp).expect("decode");
        assert_eq!(decoded.code, CODE_OK);
        assert_eq!(decoded.payload.as_ref(), &content[..]);

        // Now the disk rots underneath a sealed, immutable extent.
        let disk = node.disk_for(entry.disk_id).expect("disk");
        let mut rotted = content.clone();
        rotted[17_000] ^= 0x01;
        std::fs::write(disk.extent_path(eid), &rotted).expect("rot");
        node.fd_lru.forget(eid);
        *entry.file.borrow_mut() = None;

        let err = node
            .handle_read_bytes(whole(2))
            .await
            .expect_err("a read failing its content checksum must not return bytes");
        assert!(
            err.1.contains("content checksum"),
            "refused for the wrong reason: {}",
            err.1
        );
    }

    /// Content that is not yet DURABLE must not be hashed.
    ///
    /// The append prologue advances `entry.len` before its write is even
    /// submitted, so "len covers sealed_length" does not mean the disk does.
    /// Hashing over an in-flight write would record a checksum of bytes that
    /// may never land, and skip-if-exists would keep it forever — every
    /// whole-block read of a HEALTHY replica would then be refused.
    ///
    /// The state below is a PARTIALLY synced extent, which is what an in-flight
    /// append actually looks like. An earlier version of this test used
    /// `last_synced == 0` with fully durable bytes and called that
    /// "not durable" — but that is byte-for-byte the state of a healthy
    /// RECOVERED replica, so it asserted that repaired copies must never be
    /// checksummed. `note_durable_install` is what keeps those two apart.
    #[compio::test]
    async fn content_that_is_not_durable_yet_is_not_hashed() {
        let dir = tempfile::tempdir().expect("tmp");
        let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1))
            .await
            .expect("node");
        let eid = 5153u64;
        let content = vec![3u8; 4096];
        let entry = node.ensure_extent(eid).await.expect("entry");
        let f = node.extent_file(&entry).await.expect("file");
        file_pwrite_chunked(f, 0, Bytes::from(content.clone()))
            .await
            .expect("write");
        entry.len.store(content.len() as u64, Ordering::SeqCst);
        entry.has_dat.store(true, Ordering::SeqCst);
        // Half the extent is acknowledged durable; the rest is still in flight.
        entry.coalescer.last_synced.store(2048, Ordering::SeqCst);

        let ex = ExtentInfo {
            extent_id: eid,
            sealed: true,
            sealed_length: content.len() as u64,
            eversion: 2,
            avali: 0b1,
            ..Default::default()
        };
        // The seal itself still succeeds — integrity metadata must never be
        // able to fail a seal.
        node.apply_extent_meta_durable(eid, &entry, &ex).await.expect("seal");
        assert!(
            node.load_extent_checksums(eid, &entry, content.len() as u64)
                .await
                .is_none(),
            "hashed content the coalescer had not reported durable"
        );

        // Once the tail lands, a later apply records it.
        entry
            .coalescer
            .last_synced
            .store(content.len() as u64, Ordering::SeqCst);
        node.apply_extent_meta_durable(eid, &entry, &ex).await.expect("re-seal");
        assert!(
            node.load_extent_checksums(eid, &entry, content.len() as u64)
                .await
                .is_some(),
            "durable content must eventually be described"
        );
    }

    /// A replica whose bytes arrived by REPAIR must be describable without a
    /// restart.
    ///
    /// Peer copy and recovery rebuild fsync and then install the file; neither
    /// runs the append path, so neither advances the coalescer watermarks that
    /// the checksum gate reads. Left alone, the extent looks permanently
    /// un-synced and the rebuilt copy — the one most in need of a checksum —
    /// never gets one until the process restarts and the watermark re-seeds
    /// from the file size.
    #[compio::test]
    async fn a_repaired_replica_is_describable_without_a_restart() {
        let dir = tempfile::tempdir().expect("tmp");
        let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1))
            .await
            .expect("node");
        let eid = 5154u64;
        let content = vec![0x5Au8; 8192];
        let entry = node.ensure_extent(eid).await.expect("entry");
        let f = node.extent_file(&entry).await.expect("file");
        file_pwrite_chunked(f, 0, Bytes::from(content.clone()))
            .await
            .expect("write");
        entry.has_dat.store(true, Ordering::SeqCst);
        // Exactly what peer_copy / run_recovery_task do after their fsync.
        entry.note_durable_install(content.len() as u64);

        let ex = ExtentInfo {
            extent_id: eid,
            sealed: true,
            sealed_length: content.len() as u64,
            eversion: 3,
            avali: 0b1,
            ..Default::default()
        };
        node.apply_extent_meta_durable(eid, &entry, &ex).await.expect("seal");
        assert!(
            node.load_extent_checksums(eid, &entry, content.len() as u64)
                .await
                .is_some(),
            "a repaired replica was denied a content checksum"
        );
    }

    /// Sealing must refresh the read-side cache, not just the file.
    ///
    /// The extent is marked sealed in memory BEFORE the sidecar is written, so
    /// a read arriving in that window finds no `.ck` and caches "absent" — and
    /// nothing else ever resets that. The window is not a corner: a log extent
    /// seals at roll while readers are on its tail. Without the refresh the
    /// extents that just got a checksum are the ones that never use it.
    #[compio::test]
    async fn sealing_refreshes_a_read_cache_that_already_saw_no_sidecar() {
        let dir = tempfile::tempdir().expect("tmp");
        let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1))
            .await
            .expect("node");
        let eid = 5155u64;
        let content = vec![0x11u8; 4096];
        let entry = node.ensure_extent(eid).await.expect("entry");
        let f = node.extent_file(&entry).await.expect("file");
        file_pwrite_chunked(f, 0, Bytes::from(content.clone()))
            .await
            .expect("write");
        entry.has_dat.store(true, Ordering::SeqCst);
        entry.note_durable_install(content.len() as u64);

        // A reader gets here first: sealed in memory, sidecar not written yet.
        entry.sealed.store(true, Ordering::SeqCst);
        entry
            .sealed_length
            .store(content.len() as u64, Ordering::SeqCst);
        assert!(
            node.cached_content_checksums(eid, &entry).await.is_none(),
            "precondition: no sidecar exists yet"
        );

        let ex = ExtentInfo {
            extent_id: eid,
            sealed: true,
            sealed_length: content.len() as u64,
            eversion: 2,
            avali: 0b1,
            ..Default::default()
        };
        node.apply_extent_meta_durable(eid, &entry, &ex).await.expect("seal");
        assert!(
            node.cached_content_checksums(eid, &entry).await.is_some(),
            "the read cache still says this extent has no checksums, so every \
             later read of it goes unverified for the life of the process"
        );
    }

    /// A repeat seal-apply must NOT re-hash — and the reason is correctness,    /// A repeat seal-apply must NOT re-hash — and the reason is correctness,
    /// not just cost.
    ///
    /// The applier runs on every manager contact (append-refresh, re_avali,
    /// reconcile). If it re-hashed each time, then any rot that appeared after
    /// the seal would be blessed into a fresh checksum on the next contact, and
    /// the corrupt bytes would verify perfectly forever after. The checksum has
    /// to keep describing the content as it was when the extent sealed, or it
    /// describes nothing worth knowing.
    #[compio::test]
    async fn a_repeat_seal_apply_does_not_re_bless_rotted_content() {
        let dir = tempfile::tempdir().expect("tmp");
        let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1))
            .await
            .expect("node");
        let eid = 5151u64;
        let content = vec![9u8; 1000];
        let entry = node.ensure_extent(eid).await.expect("entry");
        let f = node.extent_file(&entry).await.expect("file");
        file_pwrite_chunked(f, 0, Bytes::from(content.clone()))
            .await
            .expect("write");
        entry.len.store(content.len() as u64, Ordering::SeqCst);
        entry.has_dat.store(true, Ordering::SeqCst);
        entry
            .coalescer
            .last_synced
            .store(content.len() as u64, Ordering::SeqCst);

        let ex = ExtentInfo {
            extent_id: eid,
            sealed: true,
            sealed_length: content.len() as u64,
            eversion: 2,
            avali: 0b1,
            ..Default::default()
        };
        node.apply_extent_meta_durable(eid, &entry, &ex).await.expect("seal");

        // Rot appears after the seal.
        let mut rotted = content.clone();
        rotted[500] ^= 0x01;
        let disk = node.disk_for(entry.disk_id).expect("disk");
        std::fs::write(disk.extent_path(eid), &rotted).expect("rot the .dat");

        // The manager contacts this node again, as it routinely does.
        node.apply_extent_meta_durable(eid, &entry, &ex).await.expect("re-seal");

        let ck = node
            .load_extent_checksums(eid, &entry, content.len() as u64)
            .await
            .expect("sidecar");
        assert!(
            ck.verify_read(0, &rotted).is_err(),
            "the re-seal re-hashed the rotted bytes and blessed them; the checksum \
             must keep describing the content as it was at seal"
        );
        assert_eq!(ck.verify_read(0, &content), Ok(1), "and still matches the real content");
    }

    /// META-FAILCLOSED (coco prod-audit #1): a corrupt `.meta` (CRC mismatch
    /// from bit rot / torn write / power loss, with the `.dat` still present)
    /// must NOT silently re-open the extent as a fresh `open, owner_epoch=0`
    /// state on restart — that would let a stale/lower-epoch writer bypass the
    /// owner_epoch fence and ghost-append to a fenced extent (split-brain).
    /// The corrupt extent must be QUARANTINED: appends rejected until the
    /// manager rebuilds authoritative state via recovery/re_avali.
    #[compio::test]
    async fn corrupt_meta_quarantines_extent_and_rejects_stale_append() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();

        // ---- node #1: open extent 7300, append at owner_epoch=10 (raises +
        // persists the fence), confirm a stale owner_epoch=5 append is fenced.
        {
            let config = ExtentNodeConfig::new(path.clone(), 1);
            let node = ExtentNode::new(config).await.expect("node1");
            node.handle_alloc_extent(rkyv_encode(&AllocExtentReq { extent_id: 7300 }))
                .await
                .expect("alloc");

            let ok = AppendResp::decode(
                node.handle_append(
                    AppendReq {
                        extent_id: 7300,
                        eversion: 1,
                        commit: 0,
                        owner_epoch: 10,
                        payload: Bytes::from(vec![1u8; 64]),
                    }
                    .encode(),
                )
                .await
                .unwrap(),
            )
            .unwrap();
            assert_eq!(ok.code, CODE_OK, "owner_epoch=10 append should land");

            let stale = AppendResp::decode(
                node.handle_append(
                    AppendReq {
                        extent_id: 7300,
                        eversion: 1,
                        commit: 0,
                        owner_epoch: 5,
                        payload: Bytes::from(vec![2u8; 8]),
                    }
                    .encode(),
                )
                .await
                .unwrap(),
            )
            .unwrap();
            assert_eq!(
                stale.code, CODE_LOCKED_BY_OTHER,
                "pre-corruption: owner_epoch=5 < persisted 10 must be fenced"
            );
        }

        // ---- corrupt the `.meta` on disk: flip a CRC-covered byte (the
        // eversion field, 24..32) so magic + extent_id stay valid but the V2
        // CRC fails — exactly the bit-rot/torn-write shape parse_meta detects.
        fn find_meta(root: &std::path::Path, name: &str) -> Option<std::path::PathBuf> {
            for e in std::fs::read_dir(root).ok()?.flatten() {
                let p = e.path();
                if p.is_dir() {
                    if let Some(f) = find_meta(&p, name) {
                        return Some(f);
                    }
                } else if p.file_name().and_then(|s| s.to_str()) == Some(name) {
                    return Some(p);
                }
            }
            None
        }
        let meta_path =
            find_meta(&path, "extent-7300.meta").expect("extent-7300.meta must exist on disk");
        let mut bytes = std::fs::read(&meta_path).expect("read meta");
        assert!(bytes.len() >= 32, "V2 meta should be >= 52 bytes");
        bytes[24] ^= 0xFF; // corrupt eversion field → CRC mismatch
        std::fs::write(&meta_path, &bytes).expect("write corrupted meta");

        // ---- node #2: reload the SAME dir. The corrupt `.meta` must NOT
        // resurrect the extent as open/epoch-0; a stale owner_epoch=5 append
        // must still be refused.
        {
            let config = ExtentNodeConfig::new(path.clone(), 1);
            let node = ExtentNode::new(config).await.expect("node2 reload");

            let stale = AppendResp::decode(
                node.handle_append(
                    AppendReq {
                        extent_id: 7300,
                        eversion: 1,
                        commit: 0,
                        owner_epoch: 5,
                        payload: Bytes::from(vec![3u8; 8]),
                    }
                    .encode(),
                )
                .await
                .unwrap(),
            )
            .unwrap();
            assert_ne!(
                stale.code, CODE_OK,
                "corrupt .meta must QUARANTINE the extent — a stale owner_epoch=5 \
                 append must be refused, not silently accepted on a fail-open reset"
            );
        }
    }

    /// P0-C (coco review #3): a sealed-EMPTY extent that has residual/ghost
    /// `.dat` bytes must report logical length 0 via commit_length AND return 0
    /// bytes on a `length=0` read — never the residual length / bytes past its
    /// (0) seal point. (Guards the commit-protocol boundary + the recovery
    /// `length=0` copy from propagating ghost bytes.)
    #[compio::test]
    async fn p0c_sealed_empty_reports_zero_length_and_reads_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = ExtentNodeConfig::new(dir.path().to_path_buf(), 1);
        let node = ExtentNode::new(config).await.expect("node");
        node.handle_alloc_extent(rkyv_encode(&AllocExtentReq { extent_id: 7100 }))
            .await
            .expect("alloc");

        // Write 100 residual bytes, then forcibly mark the extent sealed-EMPTY
        // (sealed=true, sealed_length=0) — simulating a manager sealed-empty
        // seal landing on a replica that holds leftover/ghost bytes.
        let w = AppendReq {
            extent_id: 7100,
            eversion: 1,
            commit: 0,
            owner_epoch: 0,
            payload: Bytes::from(vec![7u8; 100]),
        };
        let wr = AppendResp::decode(node.handle_append(w.encode()).await.unwrap()).unwrap();
        assert_eq!(wr.code, CODE_OK);
        assert_eq!(wr.end, 100);
        {
            let e = node.extents.get(&7100).expect("entry");
            e.sealed.store(true, Ordering::SeqCst);
            e.sealed_length.store(0, Ordering::SeqCst);
        }

        // commit_length must report 0, NOT the residual 100.
        let cl = CommitLengthResp::decode(
            node.handle_commit_length(
                CommitLengthReq {
                    extent_id: 7100,
                    owner_epoch: 1,
                }
                .encode(),
            )
            .await
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            cl.length, 0,
            "sealed-empty commit_length must be 0, not the residual file length"
        );

        // A length=0 (read-to-end) read must return 0 bytes.
        let rd = ReadBytesResp::decode(
            node.handle_read_bytes(ReadBytesReq::new(7100, 1, 0, 0, PayloadRef::in_dat()).encode())
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            rd.payload.len(),
            0,
            "sealed-empty read-to-end must return 0 bytes, not residual ghost bytes"
        );
    }
}

#[cfg(test)]
mod recovery_eversion_guard_tests {
    use super::*;

    /// run_recovery_task refuses when the local extent's eversion
    /// already exceeds the manager's recovery snapshot.
    ///
    /// The full `run_recovery_task` path requires a live manager (for
    /// `resolve_recovery_extent`) and live peers (for peer-copy). We cannot
    /// inject those in a unit test, so this test exercises the logical
    /// precondition checked by the refuse-at-start guard directly:
    ///
    ///   Given: a local ExtentEntry with eversion = 10
    ///   Given: a recovery snapshot ExtentInfo with eversion = 5
    ///   Assertion: local.eversion.load(SeqCst) > extent_info.eversion   → true
    ///
    /// This is exactly the boolean the refuse-at-start guard evaluates before
    /// issuing the expensive peer-copy. The test also confirms:
    ///
    ///   (a) fetch_max on eversion/sealed_length correctly refuses to roll back
    ///       a higher value to a lower one (verifies the writeback monotonicity
    ///       guarantee that replaces the old unconditional store).
    ///   (b) fetch_max on avali (AtomicU32) behaves identically.
    ///
    /// Pattern matches the post-truncate recheck test: the post-fetch verify
    /// cannot be injected in a single-threaded compio test either, so both tests
    /// validate the observable guard semantics rather than the concurrent injection.
    #[compio::test]
    async fn recovery_refuses_when_local_eversion_advanced() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = ExtentNodeConfig::new(dir.path().to_path_buf(), 1);
        let node = ExtentNode::new(config).await.expect("ExtentNode::new");

        // Allocate extent 7001 so it lives in self.extents with a known state.
        let alloc_payload = rkyv_encode(&AllocExtentReq { extent_id: 7001 });
        let alloc_result = node.handle_alloc_extent(alloc_payload).await;
        assert!(alloc_result.is_ok(), "alloc_extent should succeed");

        // Simulate a concurrent apply_extent_meta_durable that advanced the
        // local eversion to 10 (e.g., from a seal-confirm branch or re_avali).
        {
            let entry = node.extents.get(&7001).expect("extent 7001 in map");
            entry.eversion.store(10, Ordering::SeqCst);
            entry.sealed_length.store(512, Ordering::SeqCst);
            entry.avali.store(1, Ordering::SeqCst);
        }

        // Build a stale recovery snapshot (eversion=5, sealed_length=256).
        // This is the ExtentInfo the refuse-at-start guard compares against.
        let stale_eversion: u64 = 5;
        let stale_sealed_length: u64 = 256;

        // Verify the refuse-at-start condition: local eversion > snapshot eversion.
        {
            let entry = node.extents.get(&7001).expect("extent 7001 in map");
            let live_ev = entry.eversion.load(Ordering::SeqCst);
            assert!(
                live_ev > stale_eversion,
                "refuse-at-start guard should fire: live_ev={} > stale_eversion={}",
                live_ev,
                stale_eversion
            );
        }

        // Verify fetch_max monotonicity: applying the stale snapshot must NOT
        // roll back the fresher local eversion/sealed_length/avali.
        {
            let entry = node.extents.get(&7001).expect("extent 7001 in map");

            // fetch_max(stale_eversion=5) on a field holding 10 must return 10
            // (the old value) and leave the field at 10.
            let prev_ev = entry.eversion.fetch_max(stale_eversion, Ordering::SeqCst);
            assert_eq!(prev_ev, 10, "fetch_max must return old value 10");
            assert_eq!(
                entry.eversion.load(Ordering::SeqCst),
                10,
                "eversion must not roll back from 10 to 5"
            );

            // fetch_max(stale_sealed_length=256) on a field holding 512 must
            // leave the field at 512.
            let prev_sl = entry
                .sealed_length
                .fetch_max(stale_sealed_length, Ordering::SeqCst);
            assert_eq!(prev_sl, 512, "fetch_max must return old sealed_length 512");
            assert_eq!(
                entry.sealed_length.load(Ordering::SeqCst),
                512,
                "sealed_length must not roll back from 512 to 256"
            );

            // fetch_max(0) on avali (AtomicU32) holding 1 must leave it at 1.
            let prev_avali = entry.avali.fetch_max(0u32, Ordering::SeqCst);
            assert_eq!(prev_avali, 1, "fetch_max must return old avali 1");
            assert_eq!(
                entry.avali.load(Ordering::SeqCst),
                1,
                "avali must not roll back from 1 to 0"
            );
        }
    }
}

#[cfg(test)]
mod copy_extent_tests {
    use super::*;

    /// handle_copy_extent refuses with CODE_PRECONDITION on
    /// unsealed extents.
    ///
    /// Production callers (run_recovery_task, handle_re_avali) only target
    /// sealed extents per design — the manager dispatches both only after
    /// seal. Without this guard, a stray caller hitting an unsealed extent
    /// could race a concurrent in-flight handle_append's truncate_to_commit
    /// await window and observe a mix of pre- and post-truncate bytes via
    /// file_pread_chunked. On a sealed extent the append protocol step 3
    /// rejects concurrent appends, so the race only exists for unsealed
    /// extents. The guard converts that theoretical race into a clean
    /// CODE_PRECONDITION error.
    ///
    /// `extent_info_from_manager` returns `Ok(None)` in unit tests (no
    /// manager configured) so the manager-fetch branch falls into `Ok(None)`,
    /// no apply_extent_meta_durable runs, and `entry.sealed_length` stays
    /// at its alloc-time value of 0. The post-fetch check fires.
    #[compio::test]
    async fn copy_extent_unsealed_refused_with_precondition() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = ExtentNodeConfig::new(dir.path().to_path_buf(), 1);
        let node = ExtentNode::new(config).await.expect("ExtentNode::new");

        // Allocate extent 8001 — sealed_length=0, avali=0 (unsealed).
        let alloc_payload = rkyv_encode(&AllocExtentReq { extent_id: 8001 });
        let alloc_result = node.handle_alloc_extent(alloc_payload).await;
        assert!(alloc_result.is_ok(), "alloc_extent should succeed");

        // Write some bytes so extent.len > 0 but extent stays unsealed.
        let write_req = AppendReq {
            extent_id: 8001,
            eversion: 1,
            commit: 0,
            owner_epoch: 0,
            payload: Bytes::from(vec![0u8; 256]),
        };
        let write_result = node.handle_append(write_req.encode()).await;
        assert!(write_result.is_ok(), "first append should succeed");
        let write_resp = AppendResp::decode(write_result.unwrap()).expect("decode");
        assert_eq!(write_resp.code, CODE_OK, "append on unsealed extent OK");

        // Confirm sealed_length is still 0.
        {
            let entry = node.extents.get(&8001).expect("extent 8001 in map");
            assert_eq!(
                entry.sealed_length.load(Ordering::SeqCst),
                0,
                "extent must remain unsealed for this test"
            );
        }

        // copy_extent on unsealed extent must refuse.
        let copy_req = CopyExtentReq {
            extent_id: 8001,
            offset: 0,
            size: 0,
            eversion: 1,
        };
        let copy_result = node.handle_copy_extent(copy_req.encode()).await;
        assert!(
            copy_result.is_err(),
            "copy_extent on unsealed extent must Err"
        );
        let (code, msg) = copy_result.unwrap_err();
        assert_eq!(
            code,
            StatusCode::FailedPrecondition,
            "expected FailedPrecondition, got {:?}: {}",
            code,
            msg
        );
        assert!(
            msg.contains("unsealed") || msg.contains("sealed_length"),
            "error message should reference unsealed/sealed_length: {}",
            msg
        );
    }

    /// handle_copy_extent succeeds on a sealed extent.
    ///
    /// Sanity check that the guard does not regress the production path.
    /// Seal the extent locally (sealed_length > 0) and assert copy_extent
    /// returns CODE_OK with the expected payload bytes.
    #[compio::test]
    async fn copy_extent_sealed_succeeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = ExtentNodeConfig::new(dir.path().to_path_buf(), 1);
        let node = ExtentNode::new(config).await.expect("ExtentNode::new");

        let alloc_payload = rkyv_encode(&AllocExtentReq { extent_id: 8002 });
        node.handle_alloc_extent(alloc_payload).await.unwrap();

        let payload_bytes = vec![0xAB_u8; 128];
        let write_req = AppendReq {
            extent_id: 8002,
            eversion: 1,
            commit: 0,
            owner_epoch: 0,
            payload: Bytes::from(payload_bytes.clone()),
        };
        let write_result = node.handle_append(write_req.encode()).await;
        assert!(write_result.is_ok());
        let write_resp = AppendResp::decode(write_result.unwrap()).unwrap();
        assert_eq!(write_resp.code, CODE_OK);

        // Simulate a seal landing locally.
        {
            let entry = node.extents.get(&8002).expect("extent 8002 in map");
            entry.sealed_length.store(128, Ordering::SeqCst);
            entry.avali.store(1, Ordering::SeqCst);
        }

        let copy_req = CopyExtentReq {
            extent_id: 8002,
            offset: 0,
            size: 128,
            eversion: 1,
        };
        let copy_result = node.handle_copy_extent(copy_req.encode()).await;
        assert!(
            copy_result.is_ok(),
            "copy_extent on sealed extent must succeed: {:?}",
            copy_result.as_ref().err()
        );
        let resp = CopyExtentResp::decode(copy_result.unwrap()).expect("decode");
        assert_eq!(resp.code, CODE_OK);
        assert_eq!(resp.payload.len(), 128);
        assert_eq!(&resp.payload[..], &payload_bytes[..]);
    }
}

#[cfg(test)]
mod ec_lock_tests {
    use super::*;

    /// per-extent EC conversion lock serialises concurrent dispatches.
    ///
    /// Validates the lock plumbing: requesting the same extent's lock twice
    /// returns the SAME `Rc<Mutex>`, so the second await blocks until the
    /// first guard drops. Different extent IDs get independent locks.
    ///
    /// Full end-to-end concurrent `handle_convert_to_ec` would require peer
    /// extent nodes for the prepare-fanout phase; this targeted test covers
    /// the lock semantics in isolation.
    #[compio::test]
    async fn ec_lock_serialises_same_extent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = ExtentNodeConfig::new(dir.path().to_path_buf(), 1);
        let node = ExtentNode::new(config).await.expect("ExtentNode::new");

        // Acquire the lock for extent 9001 the same way handle_convert_to_ec
        // does — lazy-create + clone.
        let lock_a = {
            let mut locks = node.ec_conversion_locks.borrow_mut();
            locks
                .entry(9001)
                .or_insert_with(|| Rc::new(futures::lock::Mutex::new(())))
                .clone()
        };
        let lock_b = {
            let mut locks = node.ec_conversion_locks.borrow_mut();
            locks
                .entry(9001)
                .or_insert_with(|| Rc::new(futures::lock::Mutex::new(())))
                .clone()
        };
        // Same extent → same Rc<Mutex>.
        assert!(Rc::ptr_eq(&lock_a, &lock_b), "same extent must share lock");

        // Hold the first guard; the second `try_lock` must fail.
        let guard_a = lock_a.lock().await;
        assert!(
            lock_b.try_lock().is_none(),
            "second try_lock must fail while first guard is held"
        );
        drop(guard_a);
        // After drop, the second try_lock succeeds.
        assert!(
            lock_b.try_lock().is_some(),
            "second try_lock must succeed after first guard drops"
        );

        // Different extent IDs get independent locks.
        let lock_other = {
            let mut locks = node.ec_conversion_locks.borrow_mut();
            locks
                .entry(9002)
                .or_insert_with(|| Rc::new(futures::lock::Mutex::new(())))
                .clone()
        };
        assert!(
            !Rc::ptr_eq(&lock_a, &lock_other),
            "different extents must not share lock"
        );
    }
}

#[cfg(test)]
mod meta_crc_tests {
    use super::*;

    /// `payload_location` rides in what used to be reserved padding — same
    /// size, same magic, same CRC coverage — so every record written before
    /// the field has a zero there, which IS `InDat`, the documented default.
    /// That is what makes this a same-layout change with no migration.
    #[test]
    fn v2_payload_location_round_trips_and_old_records_read_as_in_dat() {
        let extent_id = 0xdead_beef_cafe_0043u64;
        let build_v2 = |loc_byte: u8| {
            let mut buf = [0u8; ExtentNode::META_SIZE_V2];
            buf[0..8].copy_from_slice(ExtentNode::META_MAGIC_V2);
            buf[8..16].copy_from_slice(&extent_id.to_le_bytes());
            buf[16..24].copy_from_slice(&4096u64.to_le_bytes()); // sealed_length
            buf[24..32].copy_from_slice(&7u64.to_le_bytes()); // eversion
            buf[32..40].copy_from_slice(&42i64.to_le_bytes()); // owner_epoch
            buf[40] = 1; // sealed
            buf[41] = loc_byte;
            buf[44..48].copy_from_slice(&3u32.to_le_bytes()); // avali
            let crc = crc32c::crc32c(&buf[0..ExtentNode::META_SIZE_V2 - 4]);
            buf[48..52].copy_from_slice(&crc.to_le_bytes());
            buf
        };

        let parsed = ExtentNode::parse_meta(
            &build_v2(autumn_rpc::extent_rpc::PAYLOAD_LOCATION_IN_SHARD_FILE),
            extent_id,
        )
        .expect("V2 parse");
        assert_eq!(
            parsed.payload_location,
            autumn_rpc::extent_rpc::PAYLOAD_LOCATION_IN_SHARD_FILE
        );

        // A pre-field record: the byte is whatever the old writer left, i.e. 0.
        let parsed = ExtentNode::parse_meta(&build_v2(0), extent_id).expect("V2 parse");
        assert_eq!(
            parsed.payload_location,
            autumn_rpc::extent_rpc::PAYLOAD_LOCATION_IN_DAT,
            "a record from before the field must read as InDat, never as committed"
        );

        // The field is inside the CRC'd region, so flipping it is detected.
        let mut tampered = build_v2(autumn_rpc::extent_rpc::PAYLOAD_LOCATION_IN_DAT);
        tampered[41] = autumn_rpc::extent_rpc::PAYLOAD_LOCATION_IN_SHARD_FILE;
        assert!(
            ExtentNode::parse_meta(&tampered, extent_id).is_none(),
            "payload_location is CRC-protected like every other field"
        );
    }

    /// round-trip through V1 meta save/parse with CRC validation.
    #[test]
    fn v1_round_trip() {
        // Build a valid V1 buffer manually to test parse_meta in isolation
        // (save_meta requires a full ExtentNode + DiskFS setup).
        let extent_id = 0xdead_beef_cafe_0042u64;
        let mut buf = [0u8; ExtentNode::META_SIZE_V1];
        buf[0..8].copy_from_slice(ExtentNode::META_MAGIC_V1);
        buf[8..16].copy_from_slice(&extent_id.to_le_bytes());
        buf[16..24].copy_from_slice(&12345u64.to_le_bytes()); // sealed_length
        buf[24..32].copy_from_slice(&7u64.to_le_bytes()); // eversion
        buf[32..40].copy_from_slice(&42i64.to_le_bytes()); // owner_epoch
        let crc = crc32c::crc32c(&buf[0..ExtentNode::META_SIZE_V0]);
        buf[40..44].copy_from_slice(&crc.to_le_bytes());

        let parsed = ExtentNode::parse_meta(&buf, extent_id).expect("V1 parse");
        assert_eq!(parsed.sealed_length, 12345);
        assert_eq!(parsed.eversion, 7);
        assert_eq!(parsed.owner_epoch, 42);
        // P0-C: V1 has no sealed flag → derived from sealed_length > 0.
        assert!(parsed.sealed);
        assert_eq!(parsed.avali, 1);
    }

    /// V0 legacy 40-byte buffer must parse (back-compat).
    #[test]
    fn v0_legacy_compat() {
        let extent_id = 0x1234_5678u64;
        let mut buf = [0u8; ExtentNode::META_SIZE_V0];
        buf[0..8].copy_from_slice(ExtentNode::META_MAGIC_V0);
        buf[8..16].copy_from_slice(&extent_id.to_le_bytes());
        buf[16..24].copy_from_slice(&999u64.to_le_bytes());
        buf[24..32].copy_from_slice(&3u64.to_le_bytes());
        buf[32..40].copy_from_slice(&100i64.to_le_bytes());

        let parsed = ExtentNode::parse_meta(&buf, extent_id).expect("V0 parse");
        assert_eq!(parsed.sealed_length, 999);
        assert_eq!(parsed.eversion, 3);
        assert_eq!(parsed.owner_epoch, 100);
        // P0-C: V0 has no sealed flag → derived from sealed_length > 0.
        assert!(parsed.sealed);
        assert_eq!(parsed.avali, 1);
    }

    /// a V1 buffer with a flipped payload byte must be rejected (CRC mismatch).
    #[test]
    fn v1_bit_rot_in_payload_rejected() {
        let extent_id = 100u64;
        let mut buf = [0u8; ExtentNode::META_SIZE_V1];
        buf[0..8].copy_from_slice(ExtentNode::META_MAGIC_V1);
        buf[8..16].copy_from_slice(&extent_id.to_le_bytes());
        buf[16..24].copy_from_slice(&500u64.to_le_bytes());
        buf[24..32].copy_from_slice(&1u64.to_le_bytes());
        buf[32..40].copy_from_slice(&0i64.to_le_bytes());
        let crc = crc32c::crc32c(&buf[0..ExtentNode::META_SIZE_V0]);
        buf[40..44].copy_from_slice(&crc.to_le_bytes());

        // Flip a bit in sealed_length.
        buf[16] ^= 0x01;

        assert!(
            ExtentNode::parse_meta(&buf, extent_id).is_none(),
            "V1 bit rot in payload must trip CRC mismatch and return None"
        );
    }

    /// a V1 buffer with a flipped CRC trailer byte must be rejected.
    #[test]
    fn v1_bit_rot_in_crc_trailer_rejected() {
        let extent_id = 200u64;
        let mut buf = [0u8; ExtentNode::META_SIZE_V1];
        buf[0..8].copy_from_slice(ExtentNode::META_MAGIC_V1);
        buf[8..16].copy_from_slice(&extent_id.to_le_bytes());
        buf[16..24].copy_from_slice(&777u64.to_le_bytes());
        buf[24..32].copy_from_slice(&5u64.to_le_bytes());
        buf[32..40].copy_from_slice(&99i64.to_le_bytes());
        let crc = crc32c::crc32c(&buf[0..ExtentNode::META_SIZE_V0]);
        buf[40..44].copy_from_slice(&crc.to_le_bytes());

        // Flip a bit in the CRC.
        buf[40] ^= 0xff;

        assert!(
            ExtentNode::parse_meta(&buf, extent_id).is_none(),
            "V1 bit rot in CRC trailer must be rejected"
        );
    }

    /// extent_id mismatch on V1 meta returns None (existing behaviour preserved).
    #[test]
    fn v1_extent_id_mismatch_rejected() {
        let mut buf = [0u8; ExtentNode::META_SIZE_V1];
        buf[0..8].copy_from_slice(ExtentNode::META_MAGIC_V1);
        buf[8..16].copy_from_slice(&500u64.to_le_bytes()); // file says 500
        buf[16..24].copy_from_slice(&1u64.to_le_bytes());
        buf[24..32].copy_from_slice(&1u64.to_le_bytes());
        buf[32..40].copy_from_slice(&0i64.to_le_bytes());
        let crc = crc32c::crc32c(&buf[0..ExtentNode::META_SIZE_V0]);
        buf[40..44].copy_from_slice(&crc.to_le_bytes());

        assert!(
            ExtentNode::parse_meta(&buf, 999).is_none(),
            "extent_id mismatch must return None"
        );
    }

    /// unknown magic byte (not V0 or V1) returns None.
    #[test]
    fn unknown_magic_rejected() {
        let mut buf = [0u8; ExtentNode::META_SIZE_V1];
        buf[0..8].copy_from_slice(b"NOT_META");
        assert!(ExtentNode::parse_meta(&buf, 1).is_none());
    }

    // ─── P0-C: V2 sidecar (explicit sealed + avali) ──────────────────────────

    fn build_v2(extent_id: u64, sealed_length: u64, sealed: bool, avali: u32) -> Vec<u8> {
        let mut buf = vec![0u8; ExtentNode::META_SIZE_V2];
        buf[0..8].copy_from_slice(ExtentNode::META_MAGIC_V2);
        buf[8..16].copy_from_slice(&extent_id.to_le_bytes());
        buf[16..24].copy_from_slice(&sealed_length.to_le_bytes());
        buf[24..32].copy_from_slice(&3u64.to_le_bytes()); // eversion
        buf[32..40].copy_from_slice(&0i64.to_le_bytes()); // owner_epoch
        buf[40] = u8::from(sealed);
        buf[44..48].copy_from_slice(&avali.to_le_bytes());
        let crc = crc32c::crc32c(&buf[0..ExtentNode::META_SIZE_V2 - 4]);
        buf[48..52].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// P0-C: the load-bearing case — a sealed-EMPTY extent
    /// (sealed=true, sealed_length=0) round-trips with sealed=true so a restart
    /// does NOT treat it as open.
    #[test]
    fn p0c_v2_sealed_empty_round_trip() {
        let eid = 0xfeed_0001u64;
        let buf = build_v2(eid, 0, true, 7);
        let parsed = ExtentNode::parse_meta(&buf, eid).expect("V2 parse");
        assert_eq!(parsed.sealed_length, 0);
        assert!(parsed.sealed, "sealed-empty must round-trip as sealed");
        assert_eq!(parsed.avali, 7);
    }

    /// P0-C: a genuinely open extent (sealed=false, len=0) stays open.
    #[test]
    fn p0c_v2_open_extent_round_trip() {
        let eid = 0xfeed_0002u64;
        let buf = build_v2(eid, 0, false, 0);
        let parsed = ExtentNode::parse_meta(&buf, eid).expect("V2 parse");
        assert!(!parsed.sealed);
        assert_eq!(parsed.sealed_length, 0);
        assert_eq!(parsed.avali, 0);
    }

    /// P0-C: a sealed-with-length extent round-trips and stays sealed.
    #[test]
    fn p0c_v2_sealed_with_length_round_trip() {
        let eid = 0xfeed_0003u64;
        let buf = build_v2(eid, 4096, true, 1);
        let parsed = ExtentNode::parse_meta(&buf, eid).expect("V2 parse");
        assert!(parsed.sealed);
        assert_eq!(parsed.sealed_length, 4096);
    }

    /// P0-C: invariant `sealed_length > 0 ⇒ sealed` is enforced even if the
    /// on-disk flag byte is (corruptly) 0 — fail-closed to sealed.
    #[test]
    fn p0c_v2_length_implies_sealed_even_if_flag_zero() {
        let eid = 0xfeed_0004u64;
        // sealed flag byte = false but sealed_length > 0.
        let buf = build_v2(eid, 1234, false, 0);
        let parsed = ExtentNode::parse_meta(&buf, eid).expect("V2 parse");
        assert!(
            parsed.sealed,
            "sealed_length > 0 must force sealed=true (fail-closed)"
        );
    }

    /// P0-C: V2 CRC now covers the sealed/avali bytes — flipping the sealed
    /// flag without recomputing CRC must be rejected.
    #[test]
    fn p0c_v2_bit_rot_in_sealed_flag_rejected() {
        let eid = 0xfeed_0005u64;
        let mut buf = build_v2(eid, 0, true, 1);
        buf[40] ^= 0x01; // flip sealed flag, leave CRC stale
        assert!(
            ExtentNode::parse_meta(&buf, eid).is_none(),
            "bit rot in the sealed flag must trip the V2 CRC"
        );
    }

    /// P0-C: a V1 buffer with sealed_length=0 derives sealed=false (an old open
    /// extent stays open after the upgrade).
    #[test]
    fn p0c_v1_zero_length_derives_unsealed() {
        let eid = 0xfeed_0006u64;
        let mut buf = [0u8; ExtentNode::META_SIZE_V1];
        buf[0..8].copy_from_slice(ExtentNode::META_MAGIC_V1);
        buf[8..16].copy_from_slice(&eid.to_le_bytes());
        // sealed_length=0, eversion=1, owner_epoch=0
        buf[24..32].copy_from_slice(&1u64.to_le_bytes());
        let crc = crc32c::crc32c(&buf[0..ExtentNode::META_SIZE_V0]);
        buf[40..44].copy_from_slice(&crc.to_le_bytes());
        let parsed = ExtentNode::parse_meta(&buf, eid).expect("V1 parse");
        assert!(!parsed.sealed);
        assert_eq!(parsed.avali, 0);
    }
}

#[cfg(test)]
mod copy_extent_eversion_tests {
    use super::*;

    /// handle_copy_extent (the Ok(None) branch — no manager configured)
    /// must reject `req.eversion = 0` when local eversion has advanced past 0.
    /// The check previously skipped on req.eversion == 0 due to the legacy
    /// `req.eversion > 0 &&` clause that the eversion-gate fix had removed in
    /// handle_read_bytes / build_read_future but missed here.
    ///
    /// Production callers (run_recovery_task, handle_re_avali) fetch
    /// ExtentInfo from the manager before dispatching, so eversion is
    /// normally fresh. This test exercises the defense-in-depth check that
    /// catches a future-bug class where uninitialised eversion bypasses
    /// the EC-shape mismatch detection.
    #[compio::test]
    async fn copy_extent_rejects_zero_eversion_on_advanced_extent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = ExtentNodeConfig::new(dir.path().to_path_buf(), 1);
        let node = ExtentNode::new(config).await.expect("ExtentNode::new");

        // Allocate extent 9001 then seal it (so handle_copy_extent's
        // unsealed-refusal doesn't fire first — we want to reach
        // the eversion check).
        let alloc_payload = rkyv_encode(&AllocExtentReq { extent_id: 9001 });
        node.handle_alloc_extent(alloc_payload)
            .await
            .expect("alloc");
        // Append some bytes so the extent has content.
        let payload = vec![0xa5u8; 64];
        let write_req = AppendReq {
            extent_id: 9001,
            eversion: 1,
            commit: 0,
            owner_epoch: 0,
            payload: Bytes::from(payload),
        };
        node.handle_append(write_req.encode())
            .await
            .expect("append");
        // Manually seal in-memory (no manager configured in this test).
        {
            let entry = node.extents.get(&9001).expect("exists");
            entry.sealed_length.store(64, Ordering::SeqCst);
            entry.avali.store(1, Ordering::SeqCst);
            entry.eversion.store(7, Ordering::SeqCst); // bumped to 7
        }

        // Copy with eversion=0 (the previously-silently-accepted value).
        let copy_req = CopyExtentReq {
            extent_id: 9001,
            offset: 0,
            size: 0,
            eversion: 0,
        };
        let r = node.handle_copy_extent(copy_req.encode()).await;
        assert!(
            r.is_err(),
            "copy_extent with eversion=0 must Err when local eversion=7"
        );
        let (code, msg) = r.unwrap_err();
        assert_eq!(code, StatusCode::FailedPrecondition);
        assert!(
            msg.contains("eversion too low"),
            "expected eversion-too-low error, got: {}",
            msg
        );
    }
}

#[cfg(test)]
mod concurrency_gate_tests {
    //! (renamed to ConcurrencyController): cross-extent
    //! concurrency cap for EC convert and recovery. These tests target
    //! `ConcurrencyController` directly — full end-to-end coverage of
    //! `handle_convert_to_ec` / `run_recovery_task` with concurrent
    //! dispatches would require multi-node peer fixtures.
    use super::*;
    use std::time::Duration;

    #[compio::test]
    async fn ec_convert_parallelism_one_serialises_acquires() {
        // recovery cap=8 so it can't interfere; ec_convert cap=1 is the
        // tested dimension.
        let ctrl = ConcurrencyController::new(1, 8);
        let p1 = ctrl.acquire_ec_convert().await;
        assert_eq!(ctrl.ec_convert_inflight(), 1);

        let race = futures::future::select(
            Box::pin(ctrl.acquire_ec_convert()),
            Box::pin(compio::time::sleep(Duration::from_millis(200))),
        )
        .await;
        assert!(
            matches!(race, futures::future::Either::Right(_)),
            "second ec-convert acquire must block while first permit is held"
        );
        assert_eq!(ctrl.ec_convert_inflight(), 1);
        drop(p1);
        assert_eq!(ctrl.ec_convert_inflight(), 0);
        let p2 = ctrl.acquire_ec_convert().await;
        assert_eq!(ctrl.ec_convert_inflight(), 1);
        drop(p2);
        assert_eq!(ctrl.ec_convert_inflight(), 0);
    }

    #[compio::test]
    async fn recovery_parallelism_two_allows_two_then_blocks_third() {
        let ctrl = ConcurrencyController::new(8, 2);
        let _p1 = ctrl.acquire_recovery().await;
        let _p2 = ctrl.acquire_recovery().await;
        assert_eq!(ctrl.recovery_inflight_count(), 2);

        let race = futures::future::select(
            Box::pin(ctrl.acquire_recovery()),
            Box::pin(compio::time::sleep(Duration::from_millis(200))),
        )
        .await;
        assert!(
            matches!(race, futures::future::Either::Right(_)),
            "third recovery acquire must block while two permits are held"
        );
        assert_eq!(ctrl.recovery_inflight_count(), 2);
    }

    /// D-r7: the two counters are independent. Saturating
    /// ec_convert MUST NOT block recovery and vice versa.
    #[compio::test]
    async fn ec_convert_and_recovery_counters_are_independent() {
        let ctrl = ConcurrencyController::new(1, 1);
        let _ec = ctrl.acquire_ec_convert().await; // ec saturated
                                                   // recovery should still get a permit.
        let race = futures::future::select(
            Box::pin(ctrl.acquire_recovery()),
            Box::pin(compio::time::sleep(Duration::from_millis(200))),
        )
        .await;
        assert!(
            matches!(race, futures::future::Either::Left(_)),
            "recovery must not be blocked by ec_convert saturation"
        );
    }

    #[compio::test]
    async fn drop_wakes_blocked_acquire() {
        let ctrl = ConcurrencyController::new(1, 1);
        let p1 = ctrl.acquire_ec_convert().await;
        let ctrl_clone = ctrl.clone();
        let acquired = Rc::new(std::cell::Cell::new(false));
        let acquired_clone = acquired.clone();
        let task = compio::runtime::spawn(async move {
            let _p = ctrl_clone.acquire_ec_convert().await;
            acquired_clone.set(true);
            compio::time::sleep(Duration::from_millis(20)).await;
        });
        for _ in 0..3 {
            compio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!acquired.get(), "must still be blocked");
        drop(p1);
        compio::time::sleep(Duration::from_millis(150)).await;
        assert!(acquired.get(), "drop must unblock queued acquire");
        let _ = task.await;
    }

    /// Constructor clamps both caps to at least 1.
    #[test]
    fn zero_parallelism_clamps_to_one() {
        let ctrl = ConcurrencyController::new(0, 0);
        assert_eq!(ctrl.ec_convert_max, 1, "ec_convert: 0 must clamp to 1");
        assert_eq!(ctrl.recovery_max, 1, "recovery: 0 must clamp to 1");
    }

    /// clamp test against the builder methods (replaces the
    /// removed env-parser smoke test). Process-global env mutation
    /// removed — no more hostility to parallel test runs.
    #[test]
    fn config_builder_clamps_parallelism() {
        let cfg = ExtentNodeConfig::new(PathBuf::from("/tmp/x"), 1);
        assert_eq!(cfg.ec_convert_parallelism, 1, "default ec=1");
        assert_eq!(cfg.recovery_parallelism, 2, "default recovery=2");
        assert_eq!(cfg.inflight_cap, 64, "default inflight=64");

        let cfg = ExtentNodeConfig::new(PathBuf::from("/tmp/x"), 1)
            .with_ec_convert_parallelism(9999)
            .with_recovery_parallelism(0)
            .with_inflight_cap(0);
        assert_eq!(cfg.ec_convert_parallelism, 16, "9999 clamps to 16");
        assert_eq!(cfg.recovery_parallelism, 1, "0 clamps to 1");
        assert_eq!(cfg.inflight_cap, 64, "0 falls back to default 64");

        let cfg = ExtentNodeConfig::new(PathBuf::from("/tmp/x"), 1)
            .with_ec_convert_parallelism(4)
            .with_recovery_parallelism(8)
            .with_inflight_cap(128);
        assert_eq!(cfg.ec_convert_parallelism, 4);
        assert_eq!(cfg.recovery_parallelism, 8);
        assert_eq!(cfg.inflight_cap, 128);
    }
}

/// shard wire-fence on `WriteShardReq`.
/// Round-trip the encoded bytes through `decode` and assert the
/// `owner_epoch` field survives so future callers cannot accidentally
/// drop it. The handler-level fence behaviour is covered by the
/// integration tests in the manager crate's node-lifecycle suite
/// (`crates/manager/tests/`).
#[cfg(test)]
mod wire_fence_tests {
    use crate::extent_rpc::WriteShardReq;
    use bytes::Bytes;

    #[test]
    fn write_shard_req_roundtrip_carries_revision() {
        let original = WriteShardReq {
            extent_id: 42,
            shard_index: 3,
            sealed_length: 12345,
            eversion: 7,
            owner_epoch: 99,
            shard_offset: 7_000_000_000, // > u32::MAX — exercises the u64 offset
            attempt_nonce: 6_000_000_001,
            payload: Bytes::from_static(b"shard-bytes"),
        };
        let encoded = original.encode();
        let decoded = WriteShardReq::decode(encoded).unwrap();
        assert_eq!(decoded.extent_id, 42);
        assert_eq!(decoded.shard_index, 3);
        assert_eq!(decoded.sealed_length, 12345);
        assert_eq!(decoded.eversion, 7);
        assert_eq!(decoded.owner_epoch, 99);
        assert_eq!(decoded.shard_offset, 7_000_000_000);
        assert_eq!(decoded.attempt_nonce, 6_000_000_001);
        assert_eq!(decoded.payload.as_ref(), b"shard-bytes");
    }

    #[test]
    fn write_shard_req_revision_zero_is_no_fence_marker() {
        let original = WriteShardReq {
            extent_id: 1,
            shard_index: 0,
            sealed_length: 0,
            eversion: 1,
            owner_epoch: 0,
            shard_offset: 0,
            attempt_nonce: 0,
            payload: Bytes::new(),
        };
        let decoded = WriteShardReq::decode(original.encode()).unwrap();
        assert_eq!(decoded.owner_epoch, 0, "zero owner_epoch marker preserved");
    }

}

#[cfg(test)]
mod ec3_fence_handover_tests {
    //! #3: proves `commit_length` is CHECK-ONLY (never bumps owner_epoch), so
    //! the manager's `push_fence_handover_to_targets` (which sent a higher
    //! owner_epoch via commit_length expecting a "handover" bump) was DEAD —
    //! the bump never happened, the ghost ex-coordinator was never fenced out.
    //! Guards the three-concepts rule against anyone re-adding handover here.
    use super::*;
    use std::sync::atomic::Ordering::SeqCst;

    #[compio::test]
    async fn commit_length_is_check_only_never_handover() {
        let dir = tempfile::tempdir().unwrap();
        let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1))
            .await
            .unwrap();
        let eid = 7001u64;
        let entry = node.ensure_extent(eid).await.unwrap();
        entry.owner_epoch.store(5, SeqCst);
        entry.durable_owner_epoch.store(5, SeqCst);

        // commit_length with a HIGHER owner_epoch — exactly the shape the dead
        // fence-handover push sent. Expectation: no-op (returns length), and
        // owner_epoch is UNCHANGED (no handover — that's why the push was dead).
        let resp = node
            .handle_commit_length(
                CommitLengthReq {
                    extent_id: eid,
                    owner_epoch: 10,
                }
                .encode(),
            )
            .await
            .unwrap();
        let decoded = CommitLengthResp::decode(resp).unwrap();
        assert_eq!(
            decoded.code, CODE_OK,
            "higher owner_epoch → no-op OK, not LOCKED"
        );
        assert_eq!(
            entry.owner_epoch.load(SeqCst),
            5,
            "commit_length MUST NOT bump owner_epoch — write-fence is append-only"
        );

        // And a LOWER owner_epoch is still rejected (the check half is live).
        let resp = node
            .handle_commit_length(
                CommitLengthReq {
                    extent_id: eid,
                    owner_epoch: 3,
                }
                .encode(),
            )
            .await
            .unwrap();
        assert_eq!(
            CommitLengthResp::decode(resp).unwrap().code,
            CODE_LOCKED_BY_OTHER,
            "stale (lower) owner_epoch is rejected"
        );
    }
}

/// regression tests: the sealed-extent fd cache evicts the
/// least-recently-used sealed fd, re-opens on access, and never touches
/// pending-fsync (durability-critical) or open/active extents.
#[cfg(test)]
mod fd_lru_tests {
    use super::*;

    async fn alloc_sealed(node: &ExtentNode, eid: u64) -> Rc<ExtentEntry> {
        node.handle_alloc_extent(rkyv_encode(&AllocExtentReq { extent_id: eid }))
            .await
            .expect("alloc");
        let e = node.extents.get(&eid).expect("entry").clone();
        // Model a sealed, fully-synced extent (alloc seeds pending==synced==0).
        e.sealed.store(true, std::sync::atomic::Ordering::SeqCst);
        e
    }

    #[compio::test]
    async fn evicts_lru_sealed_extent_and_reopens_on_access() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1))
            .await
            .expect("node");
        for eid in [7401u64, 7402, 7403] {
            alloc_sealed(&node, eid).await;
        }
        let lru = FdLru::new(2, node.extents.clone());
        lru.touch(7401);
        lru.touch(7402);
        assert_eq!(lru.resident_count(), 2);
        // 7403 pushes over cap → 7401 (LRU) evicted.
        lru.touch(7403);
        assert_eq!(lru.resident_count(), 2);
        assert!(
            node.extents.get(&7401).unwrap().resident_file().is_none(),
            "LRU (7401) fd evicted"
        );
        assert!(node.extents.get(&7402).unwrap().resident_file().is_some());
        assert!(node.extents.get(&7403).unwrap().resident_file().is_some());

        // Reading the evicted extent re-opens it via `extent_file`.
        let e1 = node.extents.get(&7401).unwrap().clone();
        let f = node.extent_file(&e1).await.expect("reopen");
        assert!(
            node.extents.get(&7401).unwrap().resident_file().is_some(),
            "re-opened on access"
        );
        drop(f);
    }

    #[compio::test]
    async fn touch_refreshes_recency_so_hot_extent_survives() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1))
            .await
            .expect("node");
        for eid in [7501u64, 7502, 7503] {
            alloc_sealed(&node, eid).await;
        }
        let lru = FdLru::new(2, node.extents.clone());
        lru.touch(7501);
        lru.touch(7502);
        lru.touch(7501); // 7501 is now most-recent; 7502 is LRU
        lru.touch(7503); // evicts 7502, keeps 7501
        assert!(
            node.extents.get(&7502).unwrap().resident_file().is_none(),
            "7502 (now LRU) evicted"
        );
        assert!(
            node.extents.get(&7501).unwrap().resident_file().is_some(),
            "re-touched 7501 survives"
        );
    }

    #[compio::test]
    async fn never_evicts_extent_with_pending_fsync() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1))
            .await
            .expect("node");
        let e1 = alloc_sealed(&node, 7601).await;
        alloc_sealed(&node, 7602).await;
        // 7601 has un-synced pending bytes (durability-critical — the coalescer
        // still needs its resident fd — must NOT be fd-evicted).
        e1.coalescer
            .pending_fsync
            .store(4096, std::sync::atomic::Ordering::SeqCst);
        let lru = FdLru::new(1, node.extents.clone());
        lru.touch(7601);
        lru.touch(7602); // over cap → tries to evict 7601 but it has pending fsync
        assert!(
            node.extents.get(&7601).unwrap().resident_file().is_some(),
            "pending-fsync extent must NOT be fd-evicted"
        );
    }

    #[compio::test]
    async fn never_evicts_while_an_inflight_op_holds_the_fd() {
        // The seal-transition panic guard: an extent whose fd `Rc` is held by an
        // in-flight write/read (strong_count > 1) is NOT evictable, even when
        // sealed + drained + the LRU victim. This is what lets the write path
        // resolve-and-hold its fd and treat only a pre-resolution `None` as a
        // reject (never a mid-op yank → panic).
        let dir = tempfile::tempdir().expect("tempdir");
        let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1))
            .await
            .expect("node");
        let e1 = alloc_sealed(&node, 7801).await;
        alloc_sealed(&node, 7802).await;
        // Simulate an in-flight op on 7801 holding its fd clone.
        let _held = e1.resident_file().expect("resident");
        let lru = FdLru::new(1, node.extents.clone());
        lru.touch(7801);
        lru.touch(7802); // over cap → 7801 is LRU but NON-evictable (held) → skip → evict 7802
        assert!(
            node.extents.get(&7801).unwrap().resident_file().is_some(),
            "held fd (strong_count>1) must NOT be evicted"
        );
        assert!(
            node.extents.get(&7802).unwrap().resident_file().is_none(),
            "the unreferenced 7802 was evicted instead"
        );
        drop(_held);
    }

    #[compio::test]
    async fn forget_drops_tracking() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1))
            .await
            .expect("node");
        alloc_sealed(&node, 7701).await;
        let lru = FdLru::new(4, node.extents.clone());
        lru.touch(7701);
        assert_eq!(lru.resident_count(), 1);
        lru.forget(7701);
        assert_eq!(lru.resident_count(), 0);
    }
}

#[cfg(test)]
mod fd_lru_chaos_tests {
    //! chaos/stress — closes the deferred acceptance item ("a live EN
    //! with >cap extents serving without EMFILE / torn read on eviction"). The
    //! other `fd_lru_tests` drive `FdLru` in isolation with a hand-set cap; this
    //! drives the PRODUCTION path end to end: write N (>> cap) sealed extents to
    //! disk, RESTART (`ExtentNode::new` → `load_extents` marks them sealed + drops
    //! their fds), then hammer them with concurrent `extent_file` reads. Two
    //! invariants under churn:
    //!   - resident fds stay BOUNDED near cap (never O(all extents)) — the EMFILE
    //!     bound the LRU exists to enforce;
    //!   - every read is byte-exact — eviction never tears a concurrent read
    //!     (reopen-on-miss returns the right inode; the strong_count guard keeps
    //!     an in-flight fd alive).
    //!
    //! NON-VACUOUS by construction + reproduce-first-checked: the extents are
    //! sealed on disk, so the reloaded node LRU-manages all N and with cap 64 <<
    //! N=150 it MUST evict; a run with reopen-on-miss neutered fails here (reads
    //! genuinely hit evicted extents). An earlier wire-level draft was vacuous —
    //! rolled extents are never sealed on the EN (a plain read doesn't apply the
    //! manager's seal), so the sealed-only LRU never touched them.
    use super::*;

    /// Deterministic, per-index-distinct 1500-byte payload (regenerable for a
    /// byte-check). 1500 B is a small VP-class value.
    fn payload(i: usize) -> Vec<u8> {
        let mut v = vec![0u8; 1500];
        for (j, b) in v.iter_mut().enumerate() {
            *b = (i
                .wrapping_mul(2_654_435_761)
                .wrapping_add(j.wrapping_mul(40_503))
                & 0xff) as u8;
        }
        v
    }

    #[compio::test]
    async fn over_cap_reload_concurrent_churn_bounded_fds_no_torn_read() {
        // Global fd cache cap = 64 (the setter's minimum). This test is the ONLY
        // set_fd_cache_cap caller in the crate, so it wins the OnceLock; the other
        // fd_lru_tests build FdLru with explicit caps and are unaffected. If the
        // cap failed to take (stayed 4096), the mid-churn bound below fails loudly
        // (no eviction → residency grows to N), so a wrong cap can't false-pass.
        set_fd_cache_cap(64);
        const N: usize = 150; // >> cap ⇒ the reloaded node MUST evict to serve
        const CAP: usize = 64;
        const BASE: u64 = 9_000;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();

        // ── Phase 1: lay down N sealed extents on disk (alloc → append 1500 B →
        //    durable sealed .meta). This is the on-disk state a restarted EN loads.
        {
            let node = ExtentNode::new(ExtentNodeConfig::new(path.clone(), 1))
                .await
                .expect("node1");
            for i in 0..N {
                let eid = BASE + i as u64;
                node.handle_alloc_extent(rkyv_encode(&AllocExtentReq { extent_id: eid }))
                    .await
                    .expect("alloc");
                let ar = AppendResp::decode(
                    node.handle_append(
                        AppendReq {
                            extent_id: eid,
                            eversion: 1,
                            commit: 0,
                            owner_epoch: 0,
                            payload: Bytes::from(payload(i)),
                        }
                        .encode(),
                    )
                    .await
                    .unwrap(),
                )
                .unwrap();
                assert_eq!(ar.code, CODE_OK, "append {eid} must succeed");
                // Seal + persist .meta so a reload sees it sealed (LRU-managed).
                let entry = node.extents.get(&eid).expect("entry").clone();
                node.apply_extent_meta_durable(
                    eid,
                    &entry,
                    &ExtentInfo {
                        extent_id: eid,
                        sealed: true,
                        sealed_length: 1500,
                        avali: 1,
                        eversion: 2,
                        ..Default::default()
                    },
                )
                .await
                .expect("seal");
            }
        }

        // ── Phase 2: RESTART — reload the same dir. load_extents reads N sealed
        //    .meta, marks each sealed, and DROPS its fd (startup fd peak is
        //    ~one-at-a-time, NOT O(all extents) — the fd-cache startup win).
        let node = Rc::new(
            ExtentNode::new(ExtentNodeConfig::new(path.clone(), 1))
                .await
                .expect("node2 reload"),
        );
        assert_eq!(node.extents.len(), N, "all {N} extents must reload");
        assert!(
            node.fd_lru.resident_count() < N,
            "post-load resident fds {} must be << N={N} (load_extents drops sealed fds)",
            node.fd_lru.resident_count()
        );

        // ── Phase 3: CHAOS — 8 concurrent readers churn random extents through
        //    extent_file (evict/reopen races on the single shard runtime, a read
        //    holding its Rc across the pread await while a sibling evicts). Each
        //    read byte-checks; resident fds stay bounded near cap throughout.
        let eids: Rc<Vec<u64>> = Rc::new((0..N).map(|i| BASE + i as u64).collect());
        let mut tasks = Vec::new();
        for t in 0..8usize {
            let node = node.clone();
            let eids = eids.clone();
            tasks.push(compio::runtime::spawn(async move {
                for k in 0..300usize {
                    let i = (t.wrapping_mul(41).wrapping_add(k.wrapping_mul(31))) % eids.len();
                    let eid = eids[i];
                    let entry = node.extents.get(&eid).expect("entry").clone();
                    let f = node
                        .extent_file(&entry)
                        .await
                        .expect("extent_file (reopen-on-miss)");
                    let got = file_pread(f, 0, 1500).await.expect("pread");
                    assert_eq!(got, payload(i), "TORN/LOST read of extent {eid} (idx {i})");
                    // fd bound holds under concurrent churn: cap + in-flight slack
                    // (≤8 held reads + reopen transients), NEVER growing toward
                    // O(all)=N. A no-eviction regression makes this blow past.
                    let resident = node.fd_lru.resident_count();
                    assert!(
                        resident <= CAP + 32,
                        "resident fds {resident} exceeded cap {CAP}+slack mid-churn (fd leak / no eviction?)"
                    );
                }
            }));
        }
        // Propagate task panics — compio's spawn wraps the future in catch_unwind,
        // so a swallowed Result would make a torn read / fd leak a silent
        // false-positive.
        for t in tasks {
            t.await
                .expect("a reader task panicked (torn read or fd-bound violation)");
        }

        // Residency stayed bounded far below the N=150 extents that were read —
        // eviction genuinely happened (not a vacuous all-resident run).
        assert!(
            node.fd_lru.resident_count() <= CAP + 32,
            "post-churn resident fds {} must be bounded (<= cap+slack), not O(all)",
            node.fd_lru.resident_count()
        );

        // Final byte-exact sweep of ALL extents — forces a reopen of every one the
        // churn evicted, confirming no extent was lost/corrupted by the LRU.
        for (i, &eid) in eids.iter().enumerate() {
            let entry = node.extents.get(&eid).expect("entry").clone();
            let f = node.extent_file(&entry).await.expect("extent_file");
            let got = file_pread(f, 0, 1500).await.expect("pread");
            assert_eq!(got, payload(i), "post-churn extent {eid} wrong");
        }
    }
}

#[cfg(test)]
mod owner_burst_splitter_tests {
    use super::*;

    fn append_msg(
        extent_id: u64,
        eversion: u64,
        commit: u64,
        owner_epoch: i64,
        payload: &[u8],
        req_id: u32,
    ) -> (ExtentMsg, futures::channel::oneshot::Receiver<Bytes>) {
        let (tx, rx) = futures::channel::oneshot::channel::<Bytes>();
        let msg = ExtentMsg::Append {
            req: AppendReq {
                extent_id,
                eversion,
                commit,
                owner_epoch,
                payload: Bytes::copy_from_slice(payload),
            },
            req_id,
            resp: tx,
        };
        (msg, rx)
    }

    /// Decode the `AppendResp.code` out of the owner's encoded response frame.
    fn resp_code(frame_bytes: Bytes) -> u8 {
        let mut dec = FrameDecoder::new();
        dec.feed(&frame_bytes);
        let frame = dec
            .try_decode()
            .expect("decode")
            .expect("one complete frame");
        AppendResp::decode(frame.payload).expect("append resp").code
    }

    /// A single drained mailbox burst can merge appends from a fenced zombie
    /// writer (owner_epoch E) and the post-takeover owner (E+1) — the owner
    /// mailbox is a cross-connection aggregation point the per-connection batch
    /// never was. `owner_loop`'s burst-splitter MUST validate each writer's
    /// epoch independently: the zombie is always rejected (LockedByOther), the
    /// rightful owner always ACKs — in BOTH drain orders. Pre-splitter, the
    /// "first slot governs the batch" prologue made `[new, zombie]` ACK the
    /// zombie past the fence (acked-data loss) and `[zombie, new]` reject the
    /// rightful owner.
    async fn run_mixed_epoch_burst(new_first: bool) -> (u8 /* new */, u8 /* zombie */) {
        let dir = tempfile::tempdir().unwrap();
        let config = ExtentNodeConfig::new(dir.path().to_path_buf(), 1);
        let node = ExtentNode::new(config).await.expect("node");
        let eid = 4242u64;
        node.handle_alloc_extent(rkyv_encode(&AllocExtentReq { extent_id: eid }))
            .await
            .expect("alloc");
        let extent = node.extents.get(&eid).expect("entry").clone();
        // Model a completed takeover: the new owner already fenced the extent to
        // epoch 2 (in-memory bar raised). A paused-then-resumed old owner (epoch
        // 1) still holds a connection and its in-flight append lands in the SAME
        // mailbox drain as the new owner's.
        extent
            .owner_epoch
            .store(2, std::sync::atomic::Ordering::SeqCst);

        // Both writers append at commit=0 (fresh extent). Distinct epochs ⇒ the
        // splitter puts each in its own run.
        let (m_new, rx_new) = append_msg(eid, 1, 0, 2, b"NEWOWNER", 1);
        let (m_old, rx_old) = append_msg(eid, 1, 0, 1, b"ZOMBIE", 2);

        {
            let mut mb = extent.owner.borrow_mut();
            if new_first {
                mb.queue.push(m_new);
                mb.queue.push(m_old);
            } else {
                mb.queue.push(m_old);
                mb.queue.push(m_new);
            }
            mb.running = true;
        }
        // Drive the owner to drain + process the burst, then exit (queue empty).
        owner_loop(node.clone(), extent.clone()).await;

        let new_code = resp_code(rx_new.await.expect("new resp"));
        let old_code = resp_code(rx_old.await.expect("zombie resp"));
        (new_code, old_code)
    }

    #[compio::test]
    async fn zombie_epoch_rejected_when_drained_after_new_owner() {
        let (new_code, zombie_code) = run_mixed_epoch_burst(true).await;
        assert_eq!(new_code, CODE_OK, "rightful owner's append must ACK");
        assert_eq!(
            zombie_code, CODE_LOCKED_BY_OTHER,
            "zombie (epoch 1) must be fenced even riding the new owner's drain — \
             pre-splitter this ACKed past the fence (acked-data loss)"
        );
    }

    #[compio::test]
    async fn zombie_epoch_rejected_when_drained_before_new_owner() {
        let (new_code, zombie_code) = run_mixed_epoch_burst(false).await;
        assert_eq!(
            new_code, CODE_OK,
            "rightful owner's append must ACK even behind a leading stale slot — \
             pre-splitter the whole burst was rejected on the first slot"
        );
        assert_eq!(zombie_code, CODE_LOCKED_BY_OTHER, "zombie must be fenced");
    }
}

#[cfg(test)]
mod recovery_idempotence_tests {
    //! A recovery that dies mid-copy leaves a PARTIAL `.dat` with no `.meta`
    //! (`run_recovery_task` persists `.meta` last). On restart `load_extents`
    //! reads `.dat` + absent `.meta` as a normal OPEN extent, so the extent id is
    //! present in `self.extents` — and `handle_require_recovery` refuses every
    //! future dispatch to that node with "already exists". Nothing reaps the
    //! residue either: the orphan reconcile only removes extents the manager has
    //! FORGOTTEN, and this extent is still very much alive. The (node, extent)
    //! pair is poisoned until an operator deletes the file by hand.
    //!
    //! The marker side self-heals (the stale sweep releases it), which is what
    //! makes this easy to miss — the cluster keeps trying, and keeps being told
    //! "already exists" by the one node that holds a useless stub.
    //!
    //! These tests drive the REAL post-crash on-disk shape (partial `.dat`, no
    //! `.meta`, discovered by `load_extents`), not a hand-built in-memory entry.
    use super::*;

    /// Lay down the residue a crashed mid-copy recovery leaves, then reload the
    /// node from that directory the way a restart would.
    async fn node_with_partial_extent(path: &std::path::Path, eid: u64) -> ExtentNode {
        {
            let node = ExtentNode::new(ExtentNodeConfig::new(path.to_path_buf(), 1))
                .await
                .expect("node1");
            node.handle_alloc_extent(rkyv_encode(&AllocExtentReq { extent_id: eid }))
                .await
                .expect("alloc");
            // Some bytes, but nothing that makes this a COMPLETE copy of the
            // (sealed, larger) extent the manager knows about.
            let entry = node.get_extent(eid).await.expect("entry");
            let disk = node.disk_for(entry.disk_id).expect("disk");
            let f = Rc::new(
                OpenOptions::new()
                    .write(true)
                    .open(disk.extent_file_path(eid, "dat"))
                    .await
                    .expect("open dat"),
            );
            file_pwrite_chunked(f.clone(), 0, Bytes::from_static(b"half a copy"))
                .await
                .expect("write partial");
            f.sync_data().await.expect("sync");
            // The crash lands BEFORE the `.meta` persist — remove it so the
            // reload sees exactly what a killed recovery leaves behind.
            let _ = compio::fs::remove_file(disk.extent_file_path(eid, "meta")).await;
        }
        // Restart: `.dat` present, `.meta` absent ⇒ loaded as an open extent.
        ExtentNode::new(
            ExtentNodeConfig::new(path.to_path_buf(), 1)
                // Unreachable on purpose: `try_adopt_completed_recovery` must
                // fail to reach the manager, which is the case that must NOT be
                // mistaken for "the copy is incomplete".
                .with_manager_endpoint("127.0.0.1:1"),
        )
        .await
        .expect("node2")
    }

    /// The staging claim is what orders two EC attempts on one node, and both
    /// staging paths (the write_shard RPC and the coordinator's own local
    /// shard) go through it. A newer attempt takes over; an older one is
    /// refused; nonce 0 (a pre-nonce peer) is left unordered rather than
    /// blocked.
    #[compio::test]
    async fn ec_staging_claim_orders_attempts() {
        {
            let dir = tempfile::tempdir().expect("tmp");
            let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1))
                .await
                .expect("node");
            let eid = 42u64;

            assert!(node.claim_ec_staging(eid, 100), "first claim wins");
            assert!(node.claim_ec_staging(eid, 100), "same attempt may keep staging");
            assert!(node.claim_ec_staging(eid, 101), "a newer attempt takes over");
            assert!(
                !node.claim_ec_staging(eid, 100),
                "a superseded attempt must be refused once a newer one claimed"
            );
            assert!(
                node.claim_ec_staging(eid, 0),
                "nonce 0 is a pre-nonce peer: unordered, not blocked"
            );
            assert!(
                !node.claim_ec_staging(eid, 100),
                "the nonce-0 pass-through must not lower the floor"
            );
            // Unordered, but not INVISIBLE: the reconcile guard reads the tick,
            // so a pre-nonce peer's staging must still stamp one or a verdict
            // asked for before it would delete the shard it is writing.
            let before = node.ec_stage_tick.get();
            assert!(node.claim_ec_staging(eid, 0), "nonce 0 still stages");
            assert!(
                node.ec_stage_tick.get() > before,
                "a nonce-0 staging left no tick — it is invisible to the freshness guard"
            );

            // Once the layout is flipped the file is live: nothing may write it
            // again — not a newer attempt, and not a pre-nonce peer either.
            node.seal_ec_staging(eid);
            assert!(
                !node.claim_ec_staging(eid, 999),
                "a sealed extent refuses even a NEWER attempt"
            );
            assert!(
                !node.claim_ec_staging(eid, 0),
                "a sealed extent refuses a pre-nonce peer too — the nonce-0 \
                 pass-through must not be a way around the seal"
            );
        }
    }

    /// REPRODUCTION (EC P1-3). After the manager flips an extent's layout to
    /// InShardFile the conversion is committed and the staged file BECOMES the
    /// live shard — there is no rename, the flip is the only commit point. Yet
    /// `apply_placements` REMOVES this node's attempt-nonce floor on that
    /// verdict, so a coordinator whose attempt was superseded before the flip
    /// is no longer ordered against anything: its late stripe is accepted and
    /// written straight over live data.
    ///
    /// Deterministic, no concurrency needed — it is a state-machine bug, not a
    /// race.
    #[compio::test]
    async fn superseded_write_shard_after_the_flip_overwrites_the_live_shard() {
        let dir = tempfile::tempdir().expect("tmp");
        let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1))
            .await
            .expect("node");
        let eid = 4242u64;
        let live = Bytes::from_static(b"WINNING-ATTEMPT-SHARD-BYTES");

        let ws = |nonce: u64, payload: Bytes| {
            WriteShardReq {
                extent_id: eid,
                shard_index: 0,
                sealed_length: 4096,
                eversion: 2,
                owner_epoch: 0,
                shard_offset: 0,
                attempt_nonce: nonce,
                payload,
            }
            .encode()
        };

        // The winning attempt (nonce 100) stages shard 0.
        let resp = node.handle_write_shard(ws(100, live.clone())).await.expect("stage");
        assert_eq!(
            WriteShardResp::decode(resp.clone()).expect("decode").code,
            CODE_OK,
            "the winning attempt must be able to stage"
        );

        // The manager flips the layout: this file is now the live shard.
        node.apply_placements(
            &[manager_rpc::ExtentPlacement {
                extent_id: eid,
                payload_location: PayloadLocation::InShardFile.as_byte(),
                shard_index: 0,
            }],
            node.ec_stage_tick.get(),
        )
        .await;

        // A coordinator superseded BEFORE the flip retries its stripe.
        let resp = node
            .handle_write_shard(ws(99, Bytes::from_static(b"ZOMBIE-OVERWRITE-BYTES!!!!!")))
            .await
            .expect("late write");
        // THE HARM first: whatever the response code says, the committed shard
        // file must still hold the winning attempt's bytes.
        let entry = node.ensure_extent(eid).await.expect("entry");
        let disk = node.disk_for(entry.disk_id).expect("disk");
        let on_disk = std::fs::read(disk.shard_path(eid, 0)).expect("read shard");
        assert_eq!(
            on_disk,
            live.to_vec(),
            "the committed shard file was overwritten by a superseded attempt"
        );
        // And the mechanism: it should have been refused outright.
        assert_eq!(
            WriteShardResp::decode(resp.clone()).expect("decode").code,
            CODE_LOCKED_BY_OTHER,
            "a stripe from a superseded attempt must be refused once the layout is committed"
        );
    }

    /// The one RPC that destroys data must check WHO it is for. A manager's
    /// persisted delete retries outlive their cluster, and extent ids restart
    /// from small integers in the next one — so a retry landing on a reused
    /// address must not unlink the live extent that happens to share the id.
    #[compio::test]
    async fn delete_extent_refuses_a_request_addressed_to_another_node() {
        let dir = tempfile::tempdir().expect("tmp");
        let node = ExtentNode::new(
            ExtentNodeConfig::new(dir.path().to_path_buf(), 1)
                .with_registration("uuid-this-node", "127.0.0.1:9101", vec![]),
        )
        .await
        .expect("node");
        let eid = 7u64;
        let entry = node.ensure_extent(eid).await.expect("create extent");
        let path = node.disk_for(entry.disk_id).expect("disk").extent_path(eid);
        assert!(path.exists(), "precondition: the extent file exists");

        let del = |uuid: &str| {
            rkyv_encode(&DeleteExtentReq {
                extent_id: eid,
                node_uuid: uuid.to_string(),
            })
        };

        let resp = node
            .handle_delete_extent(del("uuid-some-other-node"))
            .await
            .expect("call");
        let (code, msg) = decode_code(&resp);
        assert_eq!(
            code, CODE_LOCKED_BY_OTHER,
            "a delete for a different node must be refused: {msg}"
        );
        assert!(
            path.exists(),
            "the extent file must survive a delete addressed to another node"
        );

        // The rightful target still works — and so does an unspecified uuid,
        // which is how a legacy persisted retry entry arrives.
        let resp = node.handle_delete_extent(del("uuid-this-node")).await.expect("call");
        assert_eq!(decode_code(&resp).0, CODE_OK, "the addressed node must proceed");
        assert!(!path.exists(), "the extent file must be gone");
    }

    /// The staging seal must survive a RESTART of the target node.
    ///
    /// The seal itself is in-memory, so before `.meta` carried the payload
    /// location a reboot dropped it: until the next reconcile round re-sealed
    /// (up to 5 minutes) a superseded coordinator's late stripe was accepted
    /// again and overwrote the committed shard. This is the same scenario as
    /// `superseded_write_shard_after_the_flip_overwrites_the_live_shard`, with
    /// a restart inserted between the flip and the zombie's retry.
    #[compio::test]
    async fn the_ec_staging_seal_survives_a_restart() {
        let dir = tempfile::tempdir().expect("tmp");
        let eid = 4243u64;
        let live = Bytes::from_static(b"WINNING-ATTEMPT-SHARD-BYTES");
        let ws = |nonce: u64, payload: Bytes| {
            WriteShardReq {
                extent_id: eid,
                shard_index: 0,
                sealed_length: 4096,
                eversion: 2,
                owner_epoch: 0,
                shard_offset: 0,
                attempt_nonce: nonce,
                payload,
            }
            .encode()
        };

        let shard_path = {
            let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1))
                .await
                .expect("node");
            let resp = node.handle_write_shard(ws(100, live.clone())).await.expect("stage");
            assert_eq!(
                WriteShardResp::decode(resp).expect("decode").code,
                CODE_OK,
                "the winning attempt must be able to stage"
            );
            node.apply_placements(
                &[manager_rpc::ExtentPlacement {
                    extent_id: eid,
                    payload_location: PayloadLocation::InShardFile.as_byte(),
                    shard_index: 0,
                }],
                node.ec_stage_tick.get(),
            )
            .await;
            let entry = node.ensure_extent(eid).await.expect("entry");
            node.disk_for(entry.disk_id).expect("disk").shard_path(eid, 0)
        };

        // Restart: a fresh node over the same data dir, with no memory of the
        // flip beyond what `.meta` records.
        let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1))
            .await
            .expect("restarted node");

        let resp = node
            .handle_write_shard(ws(99, Bytes::from_static(b"ZOMBIE-OVERWRITE-BYTES!!!!!")))
            .await
            .expect("late write");
        let on_disk = std::fs::read(&shard_path).expect("read shard");
        assert_eq!(
            on_disk,
            live.to_vec(),
            "after a restart the committed shard must still be intact"
        );
        assert_eq!(
            WriteShardResp::decode(resp).expect("decode").code,
            CODE_LOCKED_BY_OTHER,
            "the seal must be re-derived from `.meta` on load, not lost with the process"
        );
    }

    /// EC conversion and recovery run for minutes to hours on the node and used
    /// to surface only a terminal outcome. Progress now rides the same `df`
    /// those outcomes do — and stops riding it the moment the op ends, so
    /// `ops status` never shows a repair frozen at a stale percentage.
    #[compio::test]
    async fn df_carries_live_op_progress_and_drops_it_when_the_op_ends() {
        let dir = tempfile::tempdir().expect("tmp");
        let node = ExtentNode::new(ExtentNodeConfig::new(dir.path().to_path_buf(), 1))
            .await
            .expect("node");
        let df = || async {
            let resp = node
                .handle_df(rkyv_encode(&DfReq {
                    tasks: vec![],
                    disk_ids: vec![],
                }))
                .await
                .expect("df");
            let r: DfResp = rkyv_decode(&resp).expect("decode DfResp");
            r.op_progress
        };

        assert!(df().await.is_empty(), "nothing in flight, nothing reported");

        node.note_op_progress(77, autumn_rpc::manager_rpc::OP_KIND_RECOVERY, 3, 8);
        let got = df().await;
        assert_eq!(got.len(), 1);
        assert_eq!(
            (got[0].extent_id, got[0].kind, got[0].done, got[0].total),
            (77, autumn_rpc::manager_rpc::OP_KIND_RECOVERY, 3, 8)
        );

        // A newer sample replaces the older one — this is a sample, not a queue.
        node.note_op_progress(77, autumn_rpc::manager_rpc::OP_KIND_RECOVERY, 6, 8);
        let got = df().await;
        assert_eq!(got.len(), 1, "one entry per extent, overwritten");
        assert_eq!(got[0].done, 6);

        node.clear_op_progress(77);
        assert!(
            df().await.is_empty(),
            "a finished op must stop reporting; a stale 75% is worse than none"
        );
    }

    fn decode_code(resp: &Bytes) -> (u8, String) {
        let r: CodeResp = rkyv_decode(resp).expect("decode CodeResp");
        (r.code, r.message)
    }

    fn require_recovery_req(eid: u64) -> Bytes {
        rkyv_encode(&RequireRecoveryReq {
            task: crate::extent_rpc::RecoveryTask {
                extent_id: eid,
                replace_id: 7,
                node_id: 1,
                start_time: 0,
            },
        })
    }

    /// The CONSERVATIVE half of the triage, and the reason a dispatch cannot
    /// simply reset whenever the adopt check says no: with the manager
    /// UNREACHABLE we cannot tell a complete copy from an incomplete one, and
    /// destroying a complete replica is far worse than making the manager retry.
    /// "Unknown" must keep refusing.
    ///
    /// The other two verdicts need the manager's authoritative view, so they are
    /// covered where a real one exists —
    /// `manager/tests/system_extent_recovery.rs`:
    /// `incomplete_local_copy_is_discarded_and_rebuilt_not_refused_forever`
    /// (Incomplete) and `lost_recovery_completion_redispatch_adopts_local_copy`
    /// (Complete).
    #[compio::test]
    async fn unknown_completeness_still_refuses_rather_than_resetting() {
        const EID: u64 = 77_002;
        let dir = tempfile::tempdir().expect("tempdir");
        let node = node_with_partial_extent(dir.path(), EID).await;
        // Make the local copy look COMPLETE-ish locally (sealed with bytes), so
        // the only thing standing between "adopt" and "reset" is the manager's
        // authoritative view — which is unreachable here.
        {
            let entry = node.get_extent(EID).await.expect("entry");
            entry.sealed.store(true, Ordering::SeqCst);
            entry.sealed_length.store(11, Ordering::SeqCst);
        }
        let resp = node
            .handle_require_recovery(require_recovery_req(EID))
            .await
            .expect("handler");
        let (code, _msg) = decode_code(&resp);
        assert_eq!(
            code, CODE_PRECONDITION,
            "with the manager unreachable the completeness of the local copy is UNKNOWN — \
             it must be refused, never reset"
        );
    }
}

#[cfg(test)]
mod ec_shard_read_len_tests {
    use super::{ExtentNode, FILE_IO_CHUNK_BYTES};

    /// The bug this fixes. A `0` here means read-to-end — one request for the
    /// whole shard, against a 30 s
    /// timeout whose own comment sizes it for a 256 MiB chunk. On the live
    /// cluster that was a 4 GiB shard: every peer timed out, the rebuild
    /// reported `0/4 shards available`, and EC recovery could not repair a
    /// large extent at all.
    #[test]
    fn a_large_shard_is_read_in_chunks_not_in_one_request() {
        // A full extent: 17 GiB sealed, 4+1.
        let sealed = 17_179_986_864u64;
        let len = ExtentNode::ec_shard_read_len(sealed, 4);
        assert_ne!(
            len, 0,
            "0 means read-to-end, which is the single unbounded request that timed out"
        );
        assert!(
            len > FILE_IO_CHUNK_BYTES as u64,
            "the premise: this shard is {len} bytes, past the {FILE_IO_CHUNK_BYTES}-byte \
             chunk size, so it MUST go through the chunking loop"
        );
        let requests = len.div_ceil(FILE_IO_CHUNK_BYTES as u64);
        assert!(
            requests >= 16,
            "expected the read to be split into many bounded requests, got {requests}"
        );
    }

    /// Anchored on the ENCODER, not on the function under test. Asserting
    /// `ec_shard_read_len == shard_size` was tautological: the fn is one line
    /// calling `shard_size`, so any change breaking both together still
    /// passed. This runs a real encode and pins the read length to the length
    /// of the bytes that come out — padded last data shard and parity
    /// included. If the two ever drift, K shards short by the same amount
    /// reconstruct without complaint and a truncated shard is written back as
    /// authoritative.
    #[test]
    fn the_read_length_is_exactly_what_the_encoder_emits() {
        for k in [2usize, 3, 4] {
            for len in [1usize, k - 1, k + 1, 1000, (64 << 20) + 3] {
                let payload = vec![0xA5u8; len];
                let shards = crate::erasure::ec_encode(&payload, k, 1)
                    .unwrap_or_else(|e| panic!("encode k={k} len={len}: {e}"));
                let want = ExtentNode::ec_shard_read_len(len as u64, k);
                for (i, sh) in shards.iter().enumerate() {
                    assert_eq!(
                        sh.len() as u64,
                        want,
                        "k={k} len={len} shard {i}: encoder wrote {} bytes, reader asks for {want}",
                        sh.len()
                    );
                }
            }
        }
    }

    /// `0` is the "manager state is inconsistent" signal, not a read shape:
    /// convert refuses an unsealed extent and `ec_converted` is only set with
    /// K >= 1, so neither input is reachable. The caller turns it into a loud
    /// error rather than an unbounded read with nothing to check the answer against.
    #[test]
    fn an_impossible_geometry_yields_the_zero_sentinel() {
        assert_eq!(ExtentNode::ec_shard_read_len(0, 4), 0);
        assert_eq!(ExtentNode::ec_shard_read_len(1000, 0), 0);
    }
}

#[cfg(test)]
mod ec_stripe_plan_tests {
    use super::ExtentNode;

    /// The plan must tile `[0, want)` exactly once, contiguously. Neither way
    /// of getting this wrong fails loudly at runtime: a stalled offset
    /// rewrites stripe 0 for the whole shard, a skipped one leaves a hole, and
    /// the file still ends up the right LENGTH either way — the RS decoder
    /// never sees it, and the reader's exact-length check passes.
    #[test]
    fn the_plan_tiles_the_shard_exactly_once() {
        for &(want, stripe) in &[
            (1u64, 4096u64),
            (4096, 4096),
            (4097, 4096),
            (5 * 4096 + 37, 4096),
            (4_294_996_716, 64 << 20), // the live 17 GiB extent at K=4
            (100, 1),
        ] {
            let plan = ExtentNode::ec_stripe_plan(want, stripe);
            assert!(!plan.is_empty(), "want={want} stripe={stripe}");
            let mut cursor = 0u64;
            for (off, span) in &plan {
                assert_eq!(*off, cursor, "want={want} stripe={stripe}: gap or overlap");
                assert!(*span > 0 && *span <= stripe, "want={want}: bad span {span}");
                cursor += span;
            }
            assert_eq!(cursor, want, "want={want} stripe={stripe}: plan does not reach the end");
            assert_eq!(
                plan.len() as u64,
                want.div_ceil(stripe),
                "want={want} stripe={stripe}: wrong stripe count"
            );
        }
    }

    /// The tail is the misaligned one, and it is the only short stripe.
    #[test]
    fn only_the_last_stripe_is_short() {
        let plan = ExtentNode::ec_stripe_plan(5 * 4096 + 37, 4096);
        assert_eq!(plan.len(), 6);
        for (_, span) in &plan[..5] {
            assert_eq!(*span, 4096);
        }
        assert_eq!(plan[5].1, 37);
    }

    /// A degenerate geometry yields no work rather than an infinite loop.
    #[test]
    fn nothing_to_do_is_an_empty_plan() {
        assert!(ExtentNode::ec_stripe_plan(0, 4096).is_empty());
        assert!(ExtentNode::ec_stripe_plan(4096, 0).is_empty());
    }
}

#[cfg(test)]
mod shard_routing_tests {
    /// The extent->shard map must stay the HASH, not `extent_id % k`.
    ///
    /// This is the ONLY thing worth asserting here that
    /// `tests/shards.rs::client_routes_by_extent_hash` does not already cover:
    /// that the two maps genuinely disagree. The modulo form is the trap —
    /// bootstrap hands out contiguous extent ids, so on a fresh cluster it
    /// aliases most of them onto shard 0 and looks correct, then misroutes on a
    /// real one. Nothing here tests the CALL SITES, which is where the bug was:
    /// the arithmetic was always right, `nodes_map_from_manager` just dropped
    /// `shard_ports` on the floor.
    #[test]
    fn the_extent_to_shard_map_is_the_hash_not_the_modulo() {
        let mut differ = 0;
        for id in 0u64..256 {
            if autumn_rpc::shard_for_extent(id, 4) as u64 != id % 4 {
                differ += 1;
            }
        }
        assert!(
            differ > 100,
            "hash and modulo should disagree on most ids; got {differ} of 256 \
             — has the map been swapped back to modulo?"
        );
    }
}

#[cfg(test)]
mod discard_shard_file_tests {
    use super::*;

    /// Build an entry that advertises exactly one shard file, with no fd.
    ///
    /// `file: None` is a legal resident state (the sealed-extent fd cache
    /// evicts it), which is what lets this test skip opening a real
    /// `CompioFile` — nothing on the discard path touches the fd.
    fn entry_advertising(extent_id: u64, shard_index: u32, len: u64) -> ExtentEntry {
        ExtentEntry {
            has_dat: AtomicBool::new(false),
            payload_location: AtomicU8::new(
                autumn_rpc::extent_rpc::PAYLOAD_LOCATION_IN_SHARD_FILE,
            ),
            shard_files: RefCell::new([(shard_index, len)].into_iter().collect()),
            file: RefCell::new(None),
            extent_id,
            len: AtomicU64::new(0),
            eversion: AtomicU64::new(1),
            sealed_length: AtomicU64::new(len),
            sealed: AtomicBool::new(true),
            avali: AtomicU32::new(0),
            owner_epoch: AtomicI64::new(0),
            durable_owner_epoch: AtomicI64::new(0),
            disk_id: 0,
            coalescer: Coalescer::new(0),
            owner: RefCell::new(OwnerMailbox::default()),
            corrupt_meta: AtomicBool::new(false),
            content_ck: RefCell::new(CachedChecksums::NotLoaded),
        }
    }

    /// A discarded shard file must stop being advertised.
    ///
    /// ABLATION: drop the `forget_shard_file` call from `discard_shard_file`
    /// and this goes red on the `holds_payload` assertion. That is the exact
    /// state the failed-rebuild path used to leave behind — it unlinked the
    /// partial shard and returned without the second half — so a read routed
    /// to this node cleared the ownership gate and then failed inside
    /// `payload_file` as `Internal` instead of refusing as `PayloadNotHere`.
    #[compio::test]
    async fn discard_stops_advertising_the_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("extent-7.shard3");
        std::fs::write(&path, b"partial").expect("seed the shard file");

        let entry = entry_advertising(7, 3, 7);
        let want = PayloadRef {
            location: PayloadLocation::InShardFile,
            shard_index: 3,
        };
        assert!(entry.holds_payload(want), "precondition: entry advertises it");

        entry
            .discard_shard_file(&path, 3)
            .await
            .expect("unlink of an existing file must succeed");

        assert!(!path.exists(), "the file must actually be unlinked");
        assert_eq!(entry.shard_bytes(), 0, "`df` must stop counting the bytes");
        assert!(
            !entry.holds_payload(want),
            "entry still advertises a shard file that is gone — `df` over-counts \
             and a routed read fails as Internal instead of PayloadNotHere"
        );
    }

    /// An already-absent file is "gone" too: the record must still go.
    ///
    /// A retried discard, or a crash between the unlink and the entry update,
    /// lands here. Treating `NotFound` as failure would strand the record
    /// forever, since no later unlink can ever succeed.
    #[compio::test]
    async fn discard_treats_not_found_as_gone() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("extent-7.shard3");

        let entry = entry_advertising(7, 3, 7);
        entry
            .discard_shard_file(&path, 3)
            .await
            .expect("a file that is already absent counts as gone");
        assert!(!entry.holds_payload(PayloadRef {
            location: PayloadLocation::InShardFile,
            shard_index: 3,
        }));
    }

    /// The other half of the invariant: a FAILED unlink must keep the record.
    ///
    /// Forgetting bytes that are still on disk is the mirror-image bug — `df`
    /// under-counts them and, for the `InShardFile` case, the later `.dat`
    /// reclaim is blocked. A directory standing in for the file makes
    /// `remove_file` fail without depending on permissions (which root ignores,
    /// and CI often runs as root).
    #[compio::test]
    async fn failed_unlink_keeps_the_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("extent-7.shard3");
        std::fs::create_dir(&path).expect("stand a directory in for the file");

        let entry = entry_advertising(7, 3, 7);
        assert!(
            entry.discard_shard_file(&path, 3).await.is_err(),
            "remove_file on a directory must fail"
        );
        assert!(
            entry.holds_payload(PayloadRef {
                location: PayloadLocation::InShardFile,
                shard_index: 3,
            }),
            "bytes are still on disk, so the entry must keep advertising them"
        );
    }
}

#[cfg(test)]
mod classify_ec_shard_tests {
    use super::*;

    /// K=4 data + 1 parity, sealed at `sealed_length`. `replace_id` 40 is the
    /// parity slot (index 4); 20 is data slot index 1.
    fn ec_info(sealed_length: u64, eversion: u64) -> ExtentInfo {
        ExtentInfo {
            extent_id: 7,
            replicates: vec![10, 20, 30, 40],
            parity: vec![50],
            eversion,
            sealed_length,
            sealed: true,
            ec_converted: true,
            // The CoW layout: the shard lives in `extent-N.shard{i}`. NOT the
            // default — `payload_location` defaults to 0, which is `InDat`, the
            // legacy renamed-over-`.dat` shape covered by its own test below.
            payload_location: autumn_rpc::extent_rpc::PAYLOAD_LOCATION_IN_SHARD_FILE,
            ..Default::default()
        }
    }

    /// An entry holding `shard_files[index] = len`, at `eversion`.
    fn entry_with(eversion: u64, shard: Option<(u32, u64)>) -> ExtentEntry {
        ExtentEntry {
            has_dat: AtomicBool::new(true),
            payload_location: AtomicU8::new(
                autumn_rpc::extent_rpc::PAYLOAD_LOCATION_IN_SHARD_FILE,
            ),
            shard_files: RefCell::new(shard.into_iter().collect()),
            file: RefCell::new(None),
            extent_id: 7,
            len: AtomicU64::new(0),
            eversion: AtomicU64::new(eversion),
            sealed_length: AtomicU64::new(0),
            sealed: AtomicBool::new(true),
            avali: AtomicU32::new(0),
            owner_epoch: AtomicI64::new(0),
            durable_owner_epoch: AtomicI64::new(0),
            disk_id: 0,
            coalescer: Coalescer::new(0),
            owner: RefCell::new(OwnerMailbox::default()),
            corrupt_meta: AtomicBool::new(false),
            content_ck: RefCell::new(CachedChecksums::NotLoaded),
        }
    }

    /// THE WEDGE. A failed rebuild leaves the 0-byte `.dat` stub that
    /// `ensure_extent` created and no shard file at all. That state used to
    /// answer `Unknown`, which means "refuse" — and because the manager
    /// re-sends its recovery marker every tick, the refusal and the re-dispatch
    /// fed each other forever. One failure wedged the pair permanently.
    ///
    /// ABLATION: make `classify_ec_shard` return `Unknown` for a missing shard
    /// and this goes red — which is exactly the pre-fix behaviour.
    #[test]
    fn a_missing_shard_is_rebuildable_not_a_permanent_refusal() {
        let info = ec_info(4096, 9);
        let entry = entry_with(9, None);
        assert_eq!(
            ExtentNode::classify_ec_shard(&info, &entry, 20),
            LocalCopyVerdict::IncompleteEcShard,
            "a node holding no shard must be told to rebuild, never refused"
        );
    }

    /// A rebuild that died mid-stream leaves a SHORT shard. Same remedy.
    #[test]
    fn a_short_shard_is_rebuildable() {
        let info = ec_info(4096, 9);
        // K=4 over 4096 bytes => 1024 per shard. 600 is a dead attempt.
        let entry = entry_with(9, Some((1, 600)));
        assert_eq!(
            ExtentNode::classify_ec_shard(&info, &entry, 20),
            LocalCopyVerdict::IncompleteEcShard
        );
    }

    /// The completed-but-unreported case: exact length, matching generation.
    /// This is the one that must NOT rebuild — re-reporting done is the whole
    /// point of adopting.
    #[test]
    fn an_exact_length_shard_is_complete() {
        let info = ec_info(4096, 9);
        let want = ExtentNode::ec_shard_read_len(4096, 4);
        assert_eq!(want, 1024, "K=4 over 4096 bytes");
        let entry = entry_with(9, Some((1, want)));
        assert_eq!(
            ExtentNode::classify_ec_shard(&info, &entry, 20),
            LocalCopyVerdict::Complete
        );
    }

    /// Over-long is NOT complete. The reader demands this exact length from
    /// every peer, so adopting a longer file would report as done something
    /// every subsequent read rejects.
    #[test]
    fn an_over_long_shard_is_not_complete() {
        let info = ec_info(4096, 9);
        let entry = entry_with(9, Some((1, 1025)));
        assert_eq!(
            ExtentNode::classify_ec_shard(&info, &entry, 20),
            LocalCopyVerdict::IncompleteEcShard
        );
    }

    /// Right length, wrong generation: a shard from a previous life of this
    /// extent. Length alone must not adopt it.
    #[test]
    fn a_stale_generation_is_never_adopted() {
        let info = ec_info(4096, 9);
        let entry = entry_with(8, Some((1, 1024)));
        assert_eq!(
            ExtentNode::classify_ec_shard(&info, &entry, 20),
            LocalCopyVerdict::IncompleteEcShard,
            "matching length at the wrong eversion is a different shard"
        );
    }

    /// The parity slot is indexed AFTER the data slots, and the shard it holds
    /// is the same size. Getting this wrong would compare against the wrong
    /// file and adopt or rebuild the wrong slot.
    #[test]
    fn the_parity_slot_indexes_after_the_data_slots() {
        let info = ec_info(4096, 9);
        let entry = entry_with(9, Some((4, 1024)));
        assert_eq!(
            ExtentNode::classify_ec_shard(&info, &entry, 50),
            LocalCopyVerdict::Complete,
            "node 50 is parity => index 4"
        );
        // The same file does NOT satisfy a data slot's task.
        assert_eq!(
            ExtentNode::classify_ec_shard(&info, &entry, 20),
            LocalCopyVerdict::IncompleteEcShard
        );
    }

    /// Refuse, do not guess, when the task names a node the snapshot does not
    /// list — and when the manager's own record is self-inconsistent. Both
    /// dispatch a rebuild that is certain to fail, so `Unknown` (retry later)
    /// is the honest answer.
    #[test]
    fn unjudgeable_shapes_stay_unknown() {
        let info = ec_info(4096, 9);
        let entry = entry_with(9, None);
        assert_eq!(
            ExtentNode::classify_ec_shard(&info, &entry, 999),
            LocalCopyVerdict::Unknown,
            "node 999 is not a member of this extent"
        );

        let bad = ec_info(0, 9);
        assert_eq!(
            ExtentNode::classify_ec_shard(&bad, &entry, 20),
            LocalCopyVerdict::Unknown,
            "ec_converted with sealed_length 0 is manager-state inconsistency"
        );
    }

    /// The LEGACY layout: a pre-CoW conversion renamed the shard over `.dat`,
    /// so `ec_converted = true` with `payload_location = InDat` and an empty
    /// `shard_files`. Reading only the map would call every such extent
    /// incomplete forever — and because that path rebuilds with `set_len(0)`,
    /// a late completion echo would truncate a shard readers are being served.
    #[test]
    fn a_legacy_in_dat_shard_is_judged_on_the_dat_length() {
        let mut info = ec_info(4096, 9);
        info.payload_location = autumn_rpc::extent_rpc::PAYLOAD_LOCATION_IN_DAT;

        // Complete: the `.dat` IS the shard, at exactly shard_size.
        let done = entry_with(9, None);
        done.len.store(1024, Ordering::SeqCst);
        assert_eq!(
            ExtentNode::classify_ec_shard(&info, &done, 20),
            LocalCopyVerdict::Complete
        );

        // Short `.dat` — a dead attempt.
        let partial = entry_with(9, None);
        partial.len.store(600, Ordering::SeqCst);
        assert_eq!(
            ExtentNode::classify_ec_shard(&info, &partial, 20),
            LocalCopyVerdict::IncompleteEcShard
        );

        // No `.dat` at all cannot be complete, whatever `len` claims.
        let gone = entry_with(9, None);
        gone.len.store(1024, Ordering::SeqCst);
        gone.has_dat.store(false, Ordering::SeqCst);
        assert_eq!(
            ExtentNode::classify_ec_shard(&info, &gone, 20),
            LocalCopyVerdict::IncompleteEcShard
        );

        // A shard FILE does not satisfy a layout that says InDat.
        let wrong_shape = entry_with(9, Some((1, 1024)));
        wrong_shape.has_dat.store(false, Ordering::SeqCst);
        assert_eq!(
            ExtentNode::classify_ec_shard(&info, &wrong_shape, 20),
            LocalCopyVerdict::IncompleteEcShard
        );
    }

    /// A local copy NEWER than the manager's snapshot is not adoptable either.
    /// It means the snapshot is stale; `run_recovery_task`'s refuse-at-start
    /// then rejects it and the manager re-resolves. Rebuilding is the honest
    /// answer here — `Complete` would report a generation the manager did not
    /// ask for.
    #[test]
    fn a_newer_local_generation_is_not_adopted() {
        let info = ec_info(4096, 9);
        let entry = entry_with(10, Some((1, 1024)));
        assert_eq!(
            ExtentNode::classify_ec_shard(&info, &entry, 20),
            LocalCopyVerdict::IncompleteEcShard
        );
    }
}
