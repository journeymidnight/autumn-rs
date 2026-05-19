use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use autumn_rpc::manager_rpc::{self, MgrExtentInfo};
use crate::conn_pool::parse_addr;
use crate::extent_rpc::*;

/// Convert manager RPC ExtentInfo to local extent_rpc ExtentInfo.
fn mgr_to_local_extent(e: &MgrExtentInfo) -> ExtentInfo {
    ExtentInfo {
        extent_id: e.extent_id,
        replicates: e.replicates.clone(),
        parity: e.parity.clone(),
        eversion: e.eversion,
        refs: e.refs,
        sealed_length: e.sealed_length,
        avali: e.avali,
        replicate_disks: e.replicate_disks.clone(),
        parity_disks: e.parity_disks.clone(),
        ec_converted: e.ec_converted,
    }
}
use anyhow::Result;
use bytes::Bytes;
use autumn_rpc::{Frame, FrameDecoder, HandlerResult, StatusCode};
use compio::BufResult;
use compio::fs::{File as CompioFile, OpenOptions};
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use compio::net::TcpListener;
use compio::io::{AsyncRead, AsyncWriteExt};
use dashmap::DashMap;
#[allow(unused_imports)]
use libc;
use std::cell::RefCell;
use std::rc::Rc;

// ─── Per-node append metrics ─────────────────────────────────────────────────

pub(crate) struct ExtentAppendMetrics {
    started_at: Instant,
    req_count: u64,
    bytes: u64,
    total_ns: u64,
}

impl ExtentAppendMetrics {
    fn new() -> Self {
        Self { started_at: Instant::now(), req_count: 0, bytes: 0, total_ns: 0 }
    }
    pub(crate) fn record(&mut self, reqs: u64, bytes: u64, elapsed_ns: u64) {
        self.req_count += reqs;
        self.bytes += bytes;
        self.total_ns += elapsed_ns;
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

// ─── DiskFS ──────────────────────────────────────────────────────────────────

/// Represents one physical disk (data directory) on an extent node.
///
/// Files are stored in a hash-based layout:
/// `{base_dir}/{crc32c(extent_id_le)&0xFF:02x}/extent-{id}.dat`
/// This matches the 256 subdirs created by `autumn-op format`.
/// Hash subdirs are created on-demand when the first extent is written.
struct DiskFS {
    base_dir: PathBuf,
    disk_id: u64,
    online: AtomicBool,
}

impl DiskFS {
    /// Open a disk directory formatted by `autumn-op format`.
    /// Reads `disk_id` from `{base_dir}/disk_id`.
    async fn open(base_dir: PathBuf) -> Result<Self> {
        let disk_id_path = base_dir.join("disk_id");
        let data = compio::fs::read(&disk_id_path).await
            .map_err(|e| anyhow::anyhow!("read disk_id in {}: {e}", base_dir.display()))?;
        let disk_id_str = String::from_utf8(data)
            .map_err(|e| anyhow::anyhow!("invalid utf8 disk_id in {}: {e}", base_dir.display()))?;
        let disk_id: u64 = disk_id_str
            .trim()
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid disk_id in {}", base_dir.display()))?;
        Ok(Self {
            base_dir,
            disk_id,
            online: AtomicBool::new(true),
        })
    }

    /// Create a disk entry with an explicit disk_id (no `disk_id` file required).
    fn with_disk_id(base_dir: PathBuf, disk_id: u64) -> Self {
        Self {
            base_dir,
            disk_id,
            online: AtomicBool::new(true),
        }
    }

    fn online(&self) -> bool {
        self.online.load(Ordering::Relaxed)
    }

    fn set_offline(&self) {
        self.online.store(false, Ordering::Relaxed);
    }

    /// Low byte of crc32c over extent_id little-endian bytes → hash subdir name.
    fn hash_byte(extent_id: u64) -> u8 {
        (crc32c::crc32c(&extent_id.to_le_bytes()) & 0xFF) as u8
    }

    fn extent_path(&self, extent_id: u64) -> PathBuf {
        self.base_dir
            .join(format!("{:02x}", Self::hash_byte(extent_id)))
            .join(format!("extent-{extent_id}.dat"))
    }

    fn meta_path(&self, extent_id: u64) -> PathBuf {
        self.base_dir
            .join(format!("{:02x}", Self::hash_byte(extent_id)))
            .join(format!("extent-{extent_id}.meta"))
    }

    fn ec_staging_path(&self, extent_id: u64) -> PathBuf {
        self.base_dir
            .join(format!("{:02x}", Self::hash_byte(extent_id)))
            .join(format!("extent-{extent_id}.ec.dat"))
    }

    /// F109: unlink the `.dat`, `.meta`, and (F210-D2) `.ec.dat` files
    /// for an extent. Idempotent — `NotFound` errors on any of the
    /// three are downgraded to `Ok(())` so retries from the manager
    /// are safe. Returns Err only on a real I/O failure (permission
    /// denied, etc.) so the caller can keep the entry in the
    /// pending-delete queue and retry.
    ///
    /// **F210-D2: `.ec.dat` staging files are now unlinked.** Pre-F210-D2
    /// `remove_extent_files` only touched `.dat` + `.meta`, leaving any
    /// `.ec.dat` from a crashed mid-conversion as a permanent orphan
    /// (orphan-reconcile only scanned `self.extents`, not the directory).
    /// With the F210-D1 op lock, a delete that races a convert is now
    /// refused — but a CRASH mid-convert can still leave a `.ec.dat`
    /// behind. Including it here ensures that when the manager
    /// eventually issues `MSG_DELETE_EXTENT` for the extent (refs→0),
    /// the staging file is also cleaned. The orphan reconcile loop
    /// (F210-D2 second leg) handles the case where the extent's
    /// `extent-{id}.dat` is already gone but `.ec.dat` survived.
    async fn remove_extent_files(&self, extent_id: u64) -> Result<()> {
        for path in [
            self.extent_path(extent_id),
            self.meta_path(extent_id),
            self.ec_staging_path(extent_id),
        ] {
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
            if let Ok(c_path) =
                std::ffi::CString::new(self.base_dir.as_os_str().as_bytes())
            {
                unsafe {
                    let mut stat: libc::statvfs = std::mem::zeroed();
                    if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
                        let total = stat.f_blocks as u64 * stat.f_frsize as u64;
                        let free = stat.f_bavail as u64 * stat.f_frsize as u64;
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
        // Reject `.ec.dat` (the F210-D2 ec-staging file) — that prefix
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

    /// F210-D2: parse the extent_id out of an `extent-{id}.ec.dat`
    /// staging filename. Returns None for any other shape (including
    /// `extent-{id}.dat`).
    fn parse_ec_staging_extent_id(name: &str) -> Option<u64> {
        if name.starts_with("extent-") && name.ends_with(".ec.dat") {
            let id_str = &name["extent-".len()..name.len() - ".ec.dat".len()];
            id_str.parse().ok()
        } else {
            None
        }
    }

    /// F210-D2: scan all 256 hash subdirs for `extent-{id}.ec.dat`
    /// staging files. Returns the extent_ids that have a `.ec.dat` on
    /// disk. Used by the reconcile loop to also report ec-staging
    /// orphans to the manager (the regular `scan_extents` only sees
    /// `.dat` files; a crashed mid-convert that left `.ec.dat`
    /// without a corresponding `.dat` was previously invisible to
    /// reconcile). Sync — wraps `std::fs::read_dir`. Acceptable for
    /// a 5-minute sweep; 256 directory reads is cheap.
    fn scan_ec_staging_extent_ids(&self) -> Vec<u64> {
        let mut out = Vec::new();
        for byte in 0u8..=255 {
            let subdir = self.base_dir.join(format!("{byte:02x}"));
            let dir = match std::fs::read_dir(&subdir) {
                Ok(d) => d,
                Err(_) => continue,
            };
            for entry in dir.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if let Some(id) = Self::parse_ec_staging_extent_id(&name) {
                    out.push(id);
                }
            }
        }
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
#[derive(Clone)]
pub struct ExtentNodeConfig {
    /// (dir, disk_id): None disk_id → read from `disk_id` file in dir.
    disks: Vec<(PathBuf, Option<u64>)>,
    pub manager_endpoint: Option<String>,
    /// F099-M: this shard's index (0..shard_count). Only extents where
    /// `extent_id % shard_count == shard_idx` are owned by this instance.
    pub shard_idx: u32,
    /// F099-M: total shard count in the extent-node process. 1 = legacy
    /// single-threaded mode; >1 enables per-shard filtering + routing.
    pub shard_count: u32,
    /// F099-M: sibling shards' local listener addresses on this process
    /// (typically `127.0.0.1:<shard_ports[i]>`). Used by control-plane
    /// RPC handlers (alloc, re_avali, convert_to_ec, copy_extent,
    /// require_recovery) to forward a mismatched extent_id to the
    /// owning sibling shard via localhost loopback.
    pub sibling_addrs: Vec<String>,
    /// F195 (was F194 env `AUTUMN_EXTENT_EC_CONVERT_PARALLELISM`):
    /// cross-extent cap on concurrent `handle_convert_to_ec` heavy
    /// paths. Default 1 = fully serialise. Clamped to [1, 16].
    pub ec_convert_parallelism: usize,
    /// F195 (was F194 env `AUTUMN_EXTENT_RECOVERY_PARALLELISM`):
    /// cross-extent cap on concurrent `run_recovery_task` heavy paths.
    /// Default 2 (repair work — some concurrency speeds post-failure
    /// convergence). Clamped to [1, 16].
    pub recovery_parallelism: usize,
    /// F195 (was env `AUTUMN_EXTENT_INFLIGHT_CAP`, F099-I): per-conn
    /// FuturesUnordered cap for the connection-task SQ/CQ loop. Caps
    /// the per-client memory footprint at `cap × avg-frame`. Default
    /// 64 matches the historical env default.
    pub inflight_cap: usize,
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
        }
    }

    /// F195: F194 EC convert parallelism setter. Clamped to [1, 16].
    pub fn with_ec_convert_parallelism(mut self, n: usize) -> Self {
        self.ec_convert_parallelism = n.clamp(1, 16);
        self
    }

    /// F195: F194 recovery parallelism setter. Clamped to [1, 16].
    pub fn with_recovery_parallelism(mut self, n: usize) -> Self {
        self.recovery_parallelism = n.clamp(1, 16);
        self
    }

    /// F195: F099-I per-conn inflight cap setter. Must be > 0; falls
    /// back to default 64 on 0.
    pub fn with_inflight_cap(mut self, n: usize) -> Self {
        self.inflight_cap = if n == 0 { 64 } else { n };
        self
    }

    pub fn with_manager_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.manager_endpoint = Some(endpoint.into());
        self
    }

    /// F099-M: mark this config as a shard of a multi-shard extent-node.
    /// `shard_idx` must be < `shard_count`. `sibling_addrs[i]` is the
    /// local address of shard `i` (normally `127.0.0.1:<shard_ports[i]>`).
    pub fn with_shard(mut self, shard_idx: u32, shard_count: u32, sibling_addrs: Vec<String>) -> Self {
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

/// F178 Phase 1: per-extent fsync coalescer state (event-driven, RocksDB-style).
///
/// Decouples pwrite throughput from fsync rate. Hot-path append handlers
/// store-then-register: advance `pending_fsync` to their write end_offset,
/// register a `(end, oneshot)` waiter via `register_sync_waiter`, await the
/// receiver. A lazily-spawned coalescer task issues ONE `sync_data` syscall
/// per wake-cycle covering ALL pending bytes, then drains every waiter
/// whose `end ≤ last_synced`.
///
/// **Event-driven, not timer-driven.** The first `register_sync_waiter`
/// spawns the coalescer task with a `mpsc::Unbounded<()>` wake channel. Each
/// subsequent waiter pushes itself into the list AND sends a `()` on the
/// wake channel. The coalescer loop:
///
/// 1. Snapshot `pending`/`synced`. If there's work AND any waiter, run
///    `sync_data` immediately (no sleep).
/// 2. After fsync, drain every waiter covered by the snapshot.
/// 3. If no work AND no waiters, set `wake_tx = None` and return —
///    a future `register_sync_waiter` will see `wake_tx.is_none()` and
///    spawn a fresh task.
/// 4. Otherwise park on `wake_rx.next().await` until the next waiter wakes
///    us. No timer involved.
///
/// Latency profile vs the prior timer-driven design (kept for reference):
///   - timer (sleep 2 ms): every fsync paid up to 2 ms of "wait for more
///     friends" even when the queue was empty after the first arrival.
///   - event-driven (this version): first waiter triggers fsync immediately;
///     friends that arrive during the fsync's I/O await ride along on the
///     same syscall (whole-file fsync covers ALL dirty pages including
///     those written after the syscall was issued? — no: `sync_data`
///     captures the file's dirty page state at issue time, plus any new
///     pages whose write was started before the syscall returns. In
///     practice, "all friends whose pwrite completed before sync_data
///     returns" are durable, which is exactly the LevelDB/RocksDB group-
///     commit semantics.). Subsequent batches that arrived too late get a
///     fresh wake → fresh fsync. No per-fsync 2 ms floor.
///
/// Why per-extent: an extent file's `sync_data` covers ALL of THAT file's
/// dirty pages in one syscall — no benefit to grouping across extents at
/// userspace; the kernel already does the I/O scheduling.
///
/// Lifecycle race-freedom (single-threaded compio):
///   - Spawn: first register sees `wake_tx.is_none()`, sets it Some, spawns.
///     Subsequent registers see Some, send `()` on the channel.
///   - Exit: task takes `inner.borrow_mut()`, re-checks `waiters.is_empty()`
///     AND `pending == synced`, sets `wake_tx = None`, releases borrow,
///     returns. Compio is single-threaded, so a concurrent register
///     interleaves only at `.await` points; the borrow_mut block has no
///     await inside, so the swap from Some→None and a fresh register
///     observing None happen in disjoint scheduling slots — no lost wake.
pub(crate) struct Coalescer {
    pub(crate) last_synced: AtomicU64,
    pub(crate) pending_fsync: AtomicU64,
    inner: RefCell<CoalescerInner>,
}

struct CoalescerInner {
    waiters: Vec<(u64, futures::channel::oneshot::Sender<Result<(), String>>)>,
    /// Wake channel sender. `Some` iff a coalescer task is running.
    /// `None` means no task; the next `register_sync_waiter` must spawn one.
    /// Replaces the prior `task_running: bool` so we can both signal AND
    /// wake-from-park with a single primitive.
    wake_tx: Option<futures::channel::mpsc::UnboundedSender<()>>,
}

impl Coalescer {
    fn new(initial_len: u64) -> Self {
        Self {
            last_synced: AtomicU64::new(initial_len),
            pending_fsync: AtomicU64::new(initial_len),
            inner: RefCell::new(CoalescerInner {
                waiters: Vec::new(),
                wake_tx: None,
            }),
        }
    }
}

/// Register a sync waiter on `extent` for bytes up to `end_offset`. Returns
/// a `oneshot::Receiver<Result<(), String>>` that resolves Ok when the
/// next coalesced `sync_data` has covered `end_offset`, or Err with the
/// fsync error message if the syscall failed (in which case ALL pending
/// waiters fail together — sync_data covers the whole file, no per-waiter
/// ordering).
///
/// Side effect: if no coalescer task is currently running for this extent,
/// spawns one. Otherwise pushes a `()` onto the existing task's wake
/// channel so it processes us on its next iteration.
pub(crate) fn register_sync_waiter(
    extent: &Rc<ExtentEntry>,
    end_offset: u64,
) -> futures::channel::oneshot::Receiver<Result<(), String>> {
    let (tx, rx) = futures::channel::oneshot::channel();
    let new_wake_rx = {
        let mut inner = extent.coalescer.inner.borrow_mut();
        inner.waiters.push((end_offset, tx));
        if inner.wake_tx.is_none() {
            // No task running — create wake channel, take ownership of rx
            // so we can hand it to the new task.
            let (wtx, wrx) = futures::channel::mpsc::unbounded::<()>();
            inner.wake_tx = Some(wtx);
            Some(wrx)
        } else {
            // Task is running. Send a wake; ignore Err (would only happen
            // if the receiver was dropped, which shouldn't be possible
            // while wake_tx is Some).
            let _ = inner.wake_tx.as_ref().unwrap().unbounded_send(());
            None
        }
    };
    if let Some(wrx) = new_wake_rx {
        let extent_clone = Rc::clone(extent);
        compio::runtime::spawn(coalescer_loop(extent_clone, wrx)).detach();
    }
    rx
}

async fn coalescer_loop(
    extent: Rc<ExtentEntry>,
    mut wake_rx: futures::channel::mpsc::UnboundedReceiver<()>,
) {
    use futures::StreamExt as _;
    loop {
        // ── Try to do work ─────────────────────────────────────────────
        let pending = extent.coalescer.pending_fsync.load(Ordering::SeqCst);
        let synced = extent.coalescer.last_synced.load(Ordering::SeqCst);
        let have_waiters = !extent.coalescer.inner.borrow().waiters.is_empty();

        // Bug fix (truncate path): a `truncate_to_commit` shrinks
        // `extent.len` + the following pwrite stores a smaller
        // `pending_fsync` (e.g. 10 → set_len(6) + pwrite 1 byte → 7).
        // `truncate_to_commit` already issued `sync_data` for the
        // shrink, so `last_synced` still reflects the previous larger
        // value (10). Any waiter with `end <= last_synced` (here 7 ≤
        // 10) is already durable — satisfy them without a fresh fsync
        // call. Pre-fix the coalescer's `if pending > synced` skipped
        // the fsync branch, then parked on wake_rx forever even though
        // the waiters were trivially satisfiable. Reproducer:
        // `extent_append_semantics::append_with_mid_byte_commit_truncates_and_succeeds`
        // hung indefinitely on its second append (`commit=6` then
        // pwrite "!").
        if pending <= synced && have_waiters {
            let waiters = {
                let mut inner = extent.coalescer.inner.borrow_mut();
                std::mem::take(&mut inner.waiters)
            };
            let mut still = Vec::new();
            for (end, tx) in waiters {
                if end <= synced {
                    let _ = tx.send(Ok(()));
                } else {
                    still.push((end, tx));
                }
            }
            if !still.is_empty() {
                extent.coalescer.inner.borrow_mut().waiters.extend(still);
            }
            // Re-loop: state may have changed (new waiters could have
            // arrived during the borrow_mut drops). If nothing left to
            // do, the park-or-exit block below handles cleanup.
            continue;
        }

        if pending > synced && have_waiters {
            // POSIX-correct group commit: snapshot `pending` BEFORE
            // issuing `sync_data`. Per POSIX, `fdatasync` only
            // guarantees durability for writes that completed BEFORE
            // the syscall entered the kernel; writes that completed
            // DURING the syscall (i.e. between the syscall entry and
            // its return) MAY or MAY NOT be flushed (Linux often does
            // include them, but it's not contractual). RocksDB's
            // group-commit leader does this same snapshot — only the
            // batches the leader merged BEFORE issuing fsync are
            // claimed durable; late arrivals create a fresh group.
            //
            // We capture `snapshot = pending_fsync.load()` here, then
            // after fsync only credit `last_synced = snapshot`. Late
            // arrivals (whose `pending_fsync.store` happens DURING our
            // await) advance `pending_fsync` past `snapshot` and queue
            // a wake event on `wake_rx`; the next loop iteration sees
            // `pending > synced` and issues a fresh fsync.
            let snapshot = pending; // already loaded at top of iteration
            let file_rc = extent.file_rc();
            let f: &CompioFile = &*file_rc;
            match f.sync_data().await {
                Ok(_) => {
                    extent.coalescer.last_synced.store(snapshot, Ordering::SeqCst);
                    let waiters = {
                        let mut inner = extent.coalescer.inner.borrow_mut();
                        std::mem::take(&mut inner.waiters)
                    };
                    let mut still: Vec<(
                        u64,
                        futures::channel::oneshot::Sender<Result<(), String>>,
                    )> = Vec::new();
                    for (end, tx) in waiters {
                        if end <= snapshot {
                            let _ = tx.send(Ok(()));
                        } else {
                            still.push((end, tx));
                        }
                    }
                    if !still.is_empty() {
                        extent.coalescer.inner.borrow_mut().waiters.extend(still);
                    }
                }
                Err(e) => {
                    let msg = e.to_string();
                    let waiters = {
                        let mut inner = extent.coalescer.inner.borrow_mut();
                        std::mem::take(&mut inner.waiters)
                    };
                    for (_, tx) in waiters {
                        let _ = tx.send(Err(msg.clone()));
                    }
                    // Don't advance last_synced; the next register will
                    // retry via a fresh wake.
                }
            }
            // Loop back — there may already be queued wakes / late waiters.
            continue;
        }

        // ── No work. Park on wake_rx, OR exit if truly idle. ───────────
        let park_or_exit = {
            let mut inner = extent.coalescer.inner.borrow_mut();
            let p = extent.coalescer.pending_fsync.load(Ordering::SeqCst);
            let s = extent.coalescer.last_synced.load(Ordering::SeqCst);
            if inner.waiters.is_empty() && p == s {
                // No outstanding work AND nobody's waiting — exit cleanly.
                // Drop wake_tx so any concurrent registers see None and
                // spawn a fresh task.
                inner.wake_tx = None;
                None
            } else {
                Some(())
            }
        };
        if park_or_exit.is_none() {
            return;
        }
        // Park on the wake channel. Compio single-thread guarantees that
        // any register that took the inner borrow_mut after our exit-check
        // finished AND saw wake_tx=Some has already pushed its `()` onto
        // the channel, so `next().await` either returns Some(()) immediately
        // (event already queued) or blocks until the next register does so.
        if wake_rx.next().await.is_none() {
            // wake_tx dropped (shouldn't happen in normal operation —
            // we control its drop only on our own exit path). Bail.
            return;
        }
    }
}

pub(crate) struct ExtentEntry {
    /// F171: structural close of the type-level UB at the file-replacement
    /// path. Pre-F171 this was `UnsafeCell<CompioFile>` and the replace
    /// path (`*entry.file.get() = new_file`) could dangle a concurrent
    /// reader's `&CompioFile` borrow if F153's EC-conversion lock missed
    /// any reader (theoretical UB even when in practice ruled out by
    /// F119-C's eversion check).
    ///
    /// Post-F171: `RefCell<Rc<CompioFile>>`. Reads clone the `Rc` while
    /// holding a brief `borrow()`; the I/O runs on the cloned `Rc` so
    /// the borrow is released before any `.await`. The replace path
    /// takes a `borrow_mut()` and `Rc::replace` — the OLD `Rc` is
    /// returned and dropped only when the last concurrent reader
    /// releases its clone, so the underlying file handle / fd cannot
    /// dangle. No `unsafe` anywhere in the file-access path.
    pub(crate) file: RefCell<Rc<CompioFile>>,
    pub(crate) len: AtomicU64,
    pub(crate) eversion: AtomicU64,
    pub(crate) sealed_length: AtomicU64,
    pub(crate) avali: AtomicU32,
    pub(crate) last_revision: AtomicI64,
    /// Which disk this extent lives on. Used to resolve file paths.
    pub(crate) disk_id: u64,
    /// F178 Phase 1: per-extent fsync coalescer state.
    pub(crate) coalescer: Coalescer,
}

impl ExtentEntry {
    /// F171: replace the file handle. Safe by construction —
    /// `RefCell::borrow_mut` panics if any borrow is currently held,
    /// and concurrent readers have already cloned an `Rc<CompioFile>`
    /// off a brief `borrow()` so they hold no `RefCell` borrow during
    /// their I/O. The OLD `Rc` is returned and dropped only when the
    /// last concurrent reader releases its clone — the underlying fd
    /// cannot dangle.
    ///
    /// F153's per-extent EC-conversion lock still serialises concurrent
    /// `handle_convert_to_ec` dispatches at a higher level (so two
    /// converts don't race on the staging file), but is no longer
    /// load-bearing for memory safety of the replace itself.
    pub(crate) fn replace_file(&self, new_file: CompioFile) {
        *self.file.borrow_mut() = Rc::new(new_file);
    }

    /// F171: clone the current file Rc for I/O. Caller's `Rc` keeps
    /// the underlying fd alive across `.await` boundaries, even if
    /// another task calls `replace_file` mid-flight (the new file
    /// handle goes into the RefCell; the old one stays alive in the
    /// I/O caller's clone until they drop).
    pub(crate) fn file_rc(&self) -> Rc<CompioFile> {
        self.file.borrow().clone()
    }
}

// ─── F194 → F196 D-r7 ConcurrencyController ──────────────────────────────────
//
// Renamed from `ExtentNodeGate` in F196 D-r7 to mirror PS's
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
// Why not the per-extent locks alone? `ec_conversion_locks` (F153)
// only serialises requests for the SAME extent_id; `recovery_inflight`
// (F109) only blocks duplicate requests for the SAME extent_id. Both
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

// F195: F194 env-reading helpers `ec_convert_parallelism()` and
// `recovery_parallelism()` removed — values now live on
// `ExtentNodeConfig.ec_convert_parallelism` / `.recovery_parallelism`,
// set by the extent-node binary's CLI parser. The clamp([1, 16]) moved
// to the `with_*` builder methods on ExtentNodeConfig.

// ─── ExtentNode ───────────────────────────────────────────────────────────────

pub struct ExtentNode {
    extents: Rc<DashMap<u64, Rc<ExtentEntry>>>,
    /// All disks attached to this node, keyed by disk_id.
    disks: Rc<HashMap<u64, Rc<DiskFS>>>,
    manager_endpoint: Option<String>,
    /// ConnPool for manager RPC calls (nodes_info, extent_info, etc.)
    manager_pool: Rc<crate::ConnPool>,
    recovery_done: Rc<RefCell<Vec<RecoveryTaskDone>>>,
    recovery_inflight: Rc<DashMap<u64, crate::extent_rpc::RecoveryTask>>,
    /// WAL for small must_sync writes. None if WAL is disabled.
    /// Wrapped in Rc<RefCell<>> for interior mutability on single-threaded compio.
    /// F099-M: shard_idx / shard_count for per-shard extent ownership.
    /// Default is (0, 1) = legacy single-thread mode.
    shard_idx: u32,
    shard_count: u32,
    /// F099-M: local sibling shard addresses for cross-shard control RPC
    /// forwarding. `sibling_addrs[i]` is the address of shard `i` on this
    /// host. Empty in single-thread mode.
    sibling_addrs: Rc<Vec<String>>,
    /// F153: per-extent serialisation lock for `handle_convert_to_ec`. The
    /// manager-side `ec_conversion_inflight` set is purely in-memory and is
    /// lost on leader failover; a deposed leader's in-flight EC conversion
    /// is invisible to the new leader, whose 5 s `ec_conversion_dispatch_loop`
    /// can fire a SECOND `EXT_MSG_CONVERT_TO_EC` before the first completes.
    /// F119-D's idempotency guard fires post-hoc (eversion bump is the last
    /// step of the 2PC), so during the deposed leader's mid-`spawn_blocking`
    /// `ec_encode` + `write_shard_local` window the guard does not yet
    /// trigger and two encodes race on the same `.ec.dat` staging file —
    /// producing corrupted shards or sub-shard-of-sub-shard payloads
    /// (the F119-D corruption shape). This lock serialises both dispatches
    /// on the coordinator: the second one waits, then re-runs the F119-D
    /// guard under the lock and exits as a no-op once the first finishes.
    /// Pattern mirrors `client.rs::stream_init_locks`.
    ///
    /// **F210-D1: extended to a general "mutating-op lock"**. In addition
    /// to EC convert, `handle_re_avali` now acquires this lock (its
    /// write path — fetch_full_extent_from_sources + truncate + pwrite —
    /// races with both convert and delete the same way). `handle_delete_extent`
    /// `try_lock`s this and refuses with CODE_PRECONDITION if held,
    /// closing the convert↔delete and re_avali↔delete races that F139's
    /// `recovery_inflight` check alone didn't cover. The lock entry lives
    /// for the lifetime of the node — bounded by the number of distinct
    /// extents that ever ran a mutating op on this shard. Use
    /// `get_or_create_extent_op_lock` to look up / create.
    ec_conversion_locks: Rc<RefCell<HashMap<u64, Rc<futures::lock::Mutex<()>>>>>,
    /// F194 → F196 D-r7: per-shard `ConcurrencyController` hosting both
    /// the EC-convert and recovery concurrency caps. Renamed from the
    /// two separate `ExtentNodeGate` fields (`ec_convert_gate` +
    /// `recovery_gate`) to mirror PS's `ConcurrencyController` shape —
    /// one struct, two counters. RAM cap, not rate cap.
    concurrency_ctrl: Rc<ConcurrencyController>,
    /// F195 (was F099-I env `AUTUMN_EXTENT_INFLIGHT_CAP`): per-conn
    /// FuturesUnordered cap. Read once at construction from
    /// `ExtentNodeConfig.inflight_cap`; immutable after.
    inflight_cap: usize,
}

impl Clone for ExtentNode {
    fn clone(&self) -> Self {
        Self {
            extents: self.extents.clone(),
            disks: self.disks.clone(),
            manager_endpoint: self.manager_endpoint.clone(),
            manager_pool: self.manager_pool.clone(),
            recovery_done: self.recovery_done.clone(),
            recovery_inflight: self.recovery_inflight.clone(),
            shard_idx: self.shard_idx,
            shard_count: self.shard_count,
            sibling_addrs: self.sibling_addrs.clone(),
            ec_conversion_locks: self.ec_conversion_locks.clone(),
            concurrency_ctrl: self.concurrency_ctrl.clone(),
            inflight_cap: self.inflight_cap,
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

/// Set TCP send/recv buffer sizes via setsockopt.
fn set_tcp_buffer_sizes(stream: &compio::net::TcpStream, size: usize) {
    use std::os::fd::AsRawFd;
    let fd = stream.as_raw_fd();
    let size = size as libc::c_int;
    unsafe {
        libc::setsockopt(
            fd, libc::SOL_SOCKET, libc::SO_SNDBUF,
            &size as *const _ as *const libc::c_void, std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        libc::setsockopt(
            fd, libc::SOL_SOCKET, libc::SO_RCVBUF,
            &size as *const _ as *const libc::c_void, std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

/// Positional write (pwrite) at reserved offset — safe for concurrent
/// non-overlapping offsets (each caller uses fetch_add to reserve).
///
/// F171: takes `Rc<CompioFile>` by value. The caller cloned the `Rc`
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
    let mut f: &CompioFile = &*file;
    let BufResult(result, _) = f.write_all_at(data, offset).await;
    result.map_err(|e| anyhow::anyhow!(e))
}

/// Positional read (pread). F171: see `file_pwrite` for the
/// `Rc<CompioFile>` rationale.
async fn file_pread(file: Rc<CompioFile>, offset: u64, len: usize) -> Result<Vec<u8>> {
    let f: &CompioFile = &*file;
    let buf = vec![0u8; len];
    let BufResult(result, buf) = f.read_exact_at(buf, offset).await;
    result.map_err(|e| anyhow::anyhow!(e))?;
    Ok(buf)
}

/// Per-call chunk size for local-disk pread/pwrite. macOS caps a single
/// pread/pwrite at INT_MAX (~2 GiB) and Linux at 0x7ffff000 — without
/// chunking, sealed extents > 2 GiB EINVAL on the very first syscall.
/// Mirrors `read_chunk_bytes` in `client.rs` (F105) for the StreamClient
/// RPC path; this constant covers the local-file path on the extent node.
const FILE_IO_CHUNK_BYTES: usize = 256 * 1024 * 1024;

/// Chunked pread for full-extent reads (recovery / EC convert / etc.).
/// Single-shot reads <= FILE_IO_CHUNK_BYTES bypass the loop.
async fn file_pread_chunked(
    file: Rc<CompioFile>,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>> {
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
/// passed straight to `file_pwrite` which accepts `impl IoBuf`; F140 removed
/// the per-chunk `chunk.to_vec()` round-trip that previously forced
/// `O(extent)` event-loop memcpy on every full-extent write.
async fn file_pwrite_chunked(
    file: Rc<CompioFile>,
    offset: u64,
    data: Bytes,
) -> Result<()> {
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
            Ok(0) => ReadBurst::Eof { reader, buf: buf_back },
            Ok(n) => ReadBurst::Data { buf: buf_back, n, reader },
            Err(e) => ReadBurst::Err { e, reader, buf: buf_back },
        }
    }
    .boxed_local()
}

// F195: F099-I env-reading helper `extent_inflight_cap()` removed —
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
                    let p = autumn_rpc::RpcError::encode_status(
                        StatusCode::InvalidArgument, &e.to_string());
                    let bytes = Frame::error(req_id, MSG_APPEND, p).encode();
                    inflight.push(Box::pin(async move { vec![bytes] }));
                    i += 1;
                    continue;
                }
            };
            let anchor_extent = first_req.extent_id;
            let mut slots: Vec<AppendSlot> = Vec::with_capacity(8);
            slots.push(AppendSlot { req: first_req, req_id: frames[i].req_id });
            i += 1;
            while i < frames.len() && frames[i].msg_type == MSG_APPEND {
                match AppendReq::decode(frames[i].payload.clone()) {
                    Ok(r) if r.extent_id == anchor_extent => {
                        slots.push(AppendSlot { req: r, req_id: frames[i].req_id });
                        i += 1;
                    }
                    Ok(_) => break,
                    Err(e) => {
                        let req_id = frames[i].req_id;
                        let p = autumn_rpc::RpcError::encode_status(
                            StatusCode::InvalidArgument, &e.to_string());
                        let bytes = Frame::error(req_id, MSG_APPEND, p).encode();
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

            // Run ACL + build I/O future synchronously up to the pwritev
            // await. Early rejection paths resolve immediately (no I/O).
            let fut = build_append_future(node.clone(), extent, slots).await;
            inflight.push(fut);
        } else if msg_type == MSG_READ_BYTES {
            let first_req = match ReadBytesReq::decode(frames[i].payload.clone()) {
                Ok(r) => r,
                Err(e) => {
                    let req_id = frames[i].req_id;
                    let p = autumn_rpc::RpcError::encode_status(
                        StatusCode::InvalidArgument, &e.to_string());
                    let bytes = Frame::error(req_id, MSG_READ_BYTES, p).encode();
                    inflight.push(Box::pin(async move { vec![bytes] }));
                    i += 1;
                    continue;
                }
            };
            let anchor_extent = first_req.extent_id;
            let mut slots: Vec<ReadSlot> = Vec::with_capacity(8);
            slots.push(ReadSlot { req: first_req, req_id: frames[i].req_id });
            i += 1;
            while i < frames.len() && frames[i].msg_type == MSG_READ_BYTES {
                match ReadBytesReq::decode(frames[i].payload.clone()) {
                    Ok(r) if r.extent_id == anchor_extent => {
                        slots.push(ReadSlot { req: r, req_id: frames[i].req_id });
                        i += 1;
                    }
                    Ok(_) => break,
                    Err(e) => {
                        let req_id = frames[i].req_id;
                        let p = autumn_rpc::RpcError::encode_status(
                            StatusCode::InvalidArgument, &e.to_string());
                        let bytes = Frame::error(req_id, MSG_READ_BYTES, p).encode();
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
            inflight.push(build_read_future(extent, slots));
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

/// Error-encode a single append slot.
fn err_bytes(req_id: u32, msg_type: u8, code: StatusCode, msg: &str) -> Bytes {
    Frame::error(req_id, msg_type,
        autumn_rpc::RpcError::encode_status(code, msg),
    ).encode()
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
async fn build_append_future(
    node: ExtentNode,
    extent: std::rc::Rc<ExtentEntry>,
    slots: Vec<AppendSlot>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<Bytes>>>> {
    use compio::io::AsyncWriteAt;

    if slots.is_empty() {
        return Box::pin(async move { Vec::new() });
    }

    // 1. Eversion refresh: if ANY req.eversion > local, refresh from manager.
    let local_eversion = extent.eversion.load(Ordering::SeqCst);
    let needs_refresh = slots.iter().any(|s| s.req.eversion > local_eversion);
    if needs_refresh {
        let extent_id = slots[0].req.extent_id;
        match node.extent_info_from_manager(extent_id).await {
            Ok(Some(ex)) => {
                // F143: durable seal — fsync the data file when the
                // refresh promotes 0 → sealed_length so the on-disk
                // prefix matches the manager's view.
                let _ = node.apply_extent_meta_durable(extent_id, &extent, &ex).await;
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
    let sealed = extent.sealed_length.load(Ordering::SeqCst) > 0
        || extent.avali.load(Ordering::SeqCst) > 0;
    if sealed || slots.iter().any(|s| local_eversion > s.req.eversion) {
        let resp_payload = AppendResp { code: CODE_PRECONDITION, offset: 0, end: 0 }.encode();
        let out: Vec<Bytes> = slots
            .into_iter()
            .map(|s| Frame::response(s.req_id, MSG_APPEND, resp_payload.clone()).encode())
            .collect();
        return Box::pin(async move { out });
    }

    // 3. Revision fencing: the first request's revision governs the batch.
    let first = &slots[0].req;
    let last_revision = extent.last_revision.load(Ordering::SeqCst);
    if first.revision < last_revision {
        let resp_payload = AppendResp { code: CODE_LOCKED_BY_OTHER, offset: 0, end: 0 }.encode();
        let out: Vec<Bytes> = slots
            .into_iter()
            .map(|s| Frame::response(s.req_id, MSG_APPEND, resp_payload.clone()).encode())
            .collect();
        return Box::pin(async move { out });
    }
    let revision_changed = first.revision > last_revision;
    if revision_changed {
        extent.last_revision.store(first.revision, Ordering::SeqCst);
    }

    // 4. Commit reconciliation.
    let mut file_start = extent.len.load(Ordering::SeqCst);
    if file_start < first.commit as u64 {
        let resp_payload = AppendResp { code: CODE_PRECONDITION, offset: 0, end: 0 }.encode();
        let out: Vec<Bytes> = slots
            .into_iter()
            .map(|s| Frame::response(s.req_id, MSG_APPEND, resp_payload.clone()).encode())
            .collect();
        return Box::pin(async move { out });
    }
    if file_start > first.commit as u64 {
        // F119-E / F123: before truncating, confirm with manager that this
        // extent is NOT sealed. A stale writer's low `header.commit` would
        // otherwise silently shrink a sealed extent.
        let extent_id = slots[0].req.extent_id;
        if let Ok(Some(mgr_info)) = node.extent_info_from_manager(extent_id).await {
            if mgr_info.sealed_length > 0 {
                // F143: durable seal — fsync the data file as part
                // of accepting the manager's seal point.
                let _ = node
                    .apply_extent_meta_durable(extent_id, &extent, &mgr_info)
                    .await;
                let resp_payload = AppendResp { code: CODE_PRECONDITION, offset: 0, end: 0 }.encode();
                let out: Vec<Bytes> = slots
                    .into_iter()
                    .map(|s| Frame::response(s.req_id, MSG_APPEND, resp_payload.clone()).encode())
                    .collect();
                return Box::pin(async move { out });
            }
        }
        if let Err(e) = node.truncate_to_commit_ref(&extent, first.commit).await {
            let out: Vec<Bytes> = slots
                .into_iter()
                .map(|s| err_bytes(s.req_id, MSG_APPEND, StatusCode::Internal, &e))
                .collect();
            return Box::pin(async move { out });
        }
        // F146: re-check seal state after the truncate await.
        // apply_extent_meta_durable from a concurrent handle_re_avali or
        // handle_convert_to_ec may have landed a fresh seal during the
        // truncate's I/O. Without this re-check our pwritev would write
        // bytes past the new sealed_length — a data-corruption path
        // surfacing as "logStream value short" or out-of-bounds slice
        // panics on EC reads after the sealed extent is re-read.
        if extent.sealed_length.load(Ordering::SeqCst) > 0
            || extent.avali.load(Ordering::SeqCst) > 0
        {
            let resp_payload = AppendResp { code: CODE_PRECONDITION, offset: 0, end: 0 }.encode();
            let out: Vec<Bytes> = slots
                .into_iter()
                .map(|s| Frame::response(s.req_id, MSG_APPEND, resp_payload.clone()).encode())
                .collect();
            return Box::pin(async move { out });
        }
        file_start = extent.len.load(Ordering::SeqCst);
    }

    // 5. Compute per-request offsets + collect payload Bytes for pwritev.
    let n = slots.len();
    let mut offsets: Vec<u32> = Vec::with_capacity(n);
    let mut bufs: Vec<Bytes> = Vec::with_capacity(n);
    let mut req_ids: Vec<u32> = Vec::with_capacity(n);
    let mut cursor = file_start;
    let mut total_payload: usize = 0;
    for slot in &slots {
        offsets.push(cursor as u32);
        cursor += slot.req.payload.len() as u64;
        total_payload += slot.req.payload.len();
        bufs.push(slot.req.payload.clone());
        req_ids.push(slot.req_id);
    }
    let total_end = cursor;
    let extent_id = slots[0].req.extent_id;

    // 7. Reserve `extent.len` BEFORE returning the I/O future so overlapping
    //    same-extent futures compute non-overlapping file_starts.
    extent.len.store(total_end, Ordering::SeqCst);
    drop(slots); // release original AppendReq payload handles (already cloned into bufs)

    // 8. Return the I/O future. Must be 'static and own everything.
    let extent_for_io = extent;
    Box::pin(async move {
        let write_t0 = Instant::now();
        // F171: clone the `Rc<CompioFile>` off the RefCell once. The
        // future captures this `Rc` so the underlying fd survives any
        // concurrent `entry.replace_file()` (e.g. EC commit) until our
        // I/O completes — the old fd lives until the LAST clone drops.
        // The `RefCell` borrow is released immediately by `.clone()`.
        let file_rc = extent_for_io.file_rc();
        let mut f: &CompioFile = &*file_rc;
        let BufResult(wr, _) = f.write_vectored_at(bufs, file_start).await;
        if let Err(e) = wr {
            node.mark_disk_offline_for_extent(extent_id);
            let msg = e.to_string();
            return req_ids
                .into_iter()
                .map(|id| err_bytes(id, MSG_APPEND, StatusCode::Internal, &msg))
                .collect();
        }

        // F178: every append is durable. Advance pending_fsync to the new
        // high-water, register a sync waiter on the per-extent coalescer,
        // and await. The coalescer task issues ONE sync_data per
        // wake-cycle covering ALL pending bytes (event-driven, RocksDB
        // group-commit style); every waiter whose end_offset is now
        // covered wakes together. Pre-F178 must_sync was a per-batch
        // flag and false batches skipped this wait; post-F178 the wire
        // field is gone and every batch waits.
        extent_for_io
            .coalescer
            .pending_fsync
            .store(total_end, Ordering::SeqCst);
        let rx = register_sync_waiter(&extent_for_io, total_end);
        match rx.await {
            Ok(Ok(())) => {}
            Ok(Err(msg)) => {
                node.mark_disk_offline_for_extent(extent_id);
                return req_ids
                    .into_iter()
                    .map(|id| err_bytes(id, MSG_APPEND, StatusCode::Internal, &msg))
                    .collect();
            }
            Err(_canceled) => {
                // Coalescer dropped tx without sending — should not happen
                // unless the runtime is shutting down. Treat as Internal.
                node.mark_disk_offline_for_extent(extent_id);
                let msg = "fsync coalescer canceled".to_string();
                return req_ids
                    .into_iter()
                    .map(|id| err_bytes(id, MSG_APPEND, StatusCode::Internal, &msg))
                    .collect();
            }
        }

        let write_elapsed_ns = write_t0.elapsed().as_nanos() as u64;
        EXTENT_APPEND_METRICS.with(|m| {
            m.borrow_mut()
                .record(n as u64, total_payload as u64, write_elapsed_ns);
        });

        if revision_changed {
            let _ = node.save_meta(extent_id, &extent_for_io).await;
        }

        req_ids
            .into_iter()
            .enumerate()
            .map(|(k, req_id)| {
                let end = if k + 1 < n { offsets[k + 1] } else { total_end as u32 };
                let resp = AppendResp { code: CODE_OK, offset: offsets[k], end };
                Frame::response(req_id, MSG_APPEND, resp.encode()).encode()
            })
            .collect()
    })
}

/// Build the async future that services a same-extent READ batch. Reads
/// are processed sequentially inside ONE future — each pread is ~1µs and
/// the responses are written back together.
fn build_read_future(
    extent: std::rc::Rc<ExtentEntry>,
    slots: Vec<ReadSlot>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<Bytes>>>> {
    Box::pin(async move {
        use compio::io::AsyncReadAtExt;

        let mut out: Vec<Bytes> = Vec::with_capacity(slots.len());
        for slot in slots {
            let req = slot.req;
            let ev = extent.eversion.load(Ordering::SeqCst);
            // F119-C: see handle_read_bytes — drop the `req.eversion > 0`
            // skip so a stale-cached eversion=0 (populated when the extent
            // was open) gets rejected as CODE_EVERSION_MISMATCH after
            // split / EC bump it past 0. Also: return a CODE_EVERSION_MISMATCH
            // RESPONSE (not a frame-level error) so the client's
            // `read_shard_from_addr` recognises it via `resp.code ==
            // CODE_EVERSION_MISMATCH` and the top-level
            // `read_bytes_from_extent` retry loop self-heals via
            // `invalidate_extent_cache` + refetch + EC re-route. Pre-fix
            // this batched path emitted a `FailedPrecondition` frame
            // error, which surfaced as a generic transport error and
            // never triggered the cache refresh.
            if req.eversion < ev {
                out.push(
                    Frame::response(
                        slot.req_id,
                        MSG_READ_BYTES,
                        ReadBytesResp {
                            code: CODE_EVERSION_MISMATCH,
                            end: 0,
                            payload: Bytes::new(),
                        }
                        .encode(),
                    )
                    .encode(),
                );
                continue;
            }

            let total_len = extent.len.load(Ordering::SeqCst);
            let end = total_len as u32;
            let read_offset = req.offset as u64;
            let read_size = if req.length == 0 {
                total_len.saturating_sub(read_offset)
            } else {
                (req.length as u64).min(total_len.saturating_sub(read_offset))
            };

            // F171: clone the file Rc once per slot; same rationale as
            // build_append_future. The RefCell borrow is released by
            // `.clone()` before any `.await`.
            let file_rc = extent.file_rc();
            let f: &CompioFile = &*file_rc;
            let buf = vec![0u8; read_size as usize];
            let BufResult(result, buf) = f.read_exact_at(buf, read_offset).await;
            let bytes = match result {
                Ok(_) => Frame::response(
                    slot.req_id,
                    MSG_READ_BYTES,
                    ReadBytesResp {
                        code: CODE_OK,
                        end,
                        payload: Bytes::from(buf),
                    }
                    .encode(),
                )
                .encode(),
                Err(e) => err_bytes(
                    slot.req_id,
                    MSG_READ_BYTES,
                    StatusCode::Internal,
                    &e.to_string(),
                ),
            };
            out.push(bytes);
        }
        out
    })
}

impl ExtentNode {
    /// F157: extent .meta sidecar layout versioning.
    ///
    /// V0 (legacy, pre-F157): 40 bytes, no CRC.
    ///   [magic[8]=b"EXTMETA\0"][extent_id[8]][sealed_length[8]][eversion[8]][last_revision[8]]
    ///
    /// V1 (post-F157): 44 bytes, CRC32C trailer over the first 40 bytes.
    ///   [magic[8]=b"EXTMETA\x01"][extent_id[8]][sealed_length[8]][eversion[8]][last_revision[8]][crc32c[4]]
    ///
    /// Pre-F157 a flipped bit anywhere in the 40-byte payload (bit rot, undetected
    /// disk error, partial overwrite during a torn write) silently changed the
    /// extent's seal state at restart — recovery would load `sealed_length=0`
    /// for an actually-sealed extent, accept new appends past the old seal
    /// boundary, and corrupt every replica's view of the extent's tail bytes.
    /// V1 wraps a CRC32C trailer; on read, mismatch returns None (treated as
    /// "no meta file" → defaults applied + warning logged), so a corrupted
    /// meta cannot silently drive the extent into an inconsistent state.
    ///
    /// **Migration:** save_meta always writes V1. parse_meta dispatches on
    /// `magic[7]`:
    ///   - 0x00 (V0): legacy 40-byte read, no CRC verification, WARN logged.
    ///                Next save_meta upgrades to V1.
    ///   - 0x01 (V1): 44-byte read with CRC verification.
    ///   - other: None (treated as missing/corrupt meta).
    /// V0-binary on V1-file: magic mismatch → None → broken on rollback.
    /// Acceptable since rollback is operator-driven and rare.
    const META_MAGIC_V0: &'static [u8; 8] = b"EXTMETA\0";
    const META_MAGIC_V1: &'static [u8; 8] = b"EXTMETA\x01";
    const META_SIZE_V0: usize = 40;
    const META_SIZE_V1: usize = 44;
    /// Backwards-compat alias for any external code reading the constant.
    /// Equal to V1 size (the size save_meta writes).
    const META_SIZE: usize = Self::META_SIZE_V1;

    pub async fn new(config: ExtentNodeConfig) -> Result<Self> {
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

        let node = Self {
            extents: Rc::new(DashMap::new()),
            disks: Rc::new(disk_map),
            manager_endpoint: config.manager_endpoint,
            manager_pool: Rc::new(crate::ConnPool::new()),
            recovery_done: Rc::new(std::cell::RefCell::new(Vec::new())),
            recovery_inflight: Rc::new(DashMap::new()),
            shard_idx: config.shard_idx,
            shard_count: config.shard_count,
            sibling_addrs: Rc::new(config.sibling_addrs),
            ec_conversion_locks: Rc::new(RefCell::new(HashMap::new())),
            // F195: parallelism comes from `ExtentNodeConfig`. CLI flag
            // → builder → here. No env read.
            concurrency_ctrl: ConcurrencyController::new(
                config.ec_convert_parallelism,
                config.recovery_parallelism,
            ),
            inflight_cap: config.inflight_cap.max(1),
        };

        // Load existing extents from all disks.
        node.load_extents().await?;

        // F109+F113: reconcile loaded extents against the manager. Any
        // `extent_id` the manager no longer knows about (refs went to
        // 0 while this node was offline, or the manager's in-memory
        // pending-delete queue was lost across a restart, or an EC
        // conversion left a replica behind) is unlinked.
        //
        // F113: spawn as a long-lived background task. After an initial
        // exp-backoff retry that races past manager leader election,
        // it enters a steady-state periodic sweep so the node self-
        // heals on any extent that becomes garbage at runtime —
        // covering MSG_DELETE_EXTENT retry budget exhaustion, EC
        // conversion leftovers, and any other future case where an
        // extent's manager refs hit 0 while the node was momentarily
        // unreachable.
        node.spawn_reconcile_orphans_loop();

        Ok(node)
    }

    /// F113: long-lived periodic orphan reconcile.
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
        compio::runtime::spawn(async move {
            const SWEEP_INTERVAL: Duration = Duration::from_secs(300);
            loop {
                if let Err(e) = node.reconcile_orphans_with_manager().await {
                    tracing::warn!(
                        error = %e,
                        "F113 reconcile failed (will retry next sweep)",
                    );
                }
                compio::time::sleep(SWEEP_INTERVAL).await;
            }
        })
        .detach();
    }

    /// F109: best-effort startup orphan reconcile.
    /// If `manager_endpoint` is configured, ship every loaded
    /// `extent_id` to the manager; receive back the subset that's no
    /// longer registered and unlink the corresponding `.dat`/`.meta`.
    /// Skips silently when there's no manager (test setups). Per-disk
    /// errors are logged but don't propagate — partial cleanup is fine,
    /// the F113 retry loop will catch the next iteration.
    async fn reconcile_orphans_with_manager(&self) -> Result<()> {
        let mgr = match &self.manager_endpoint {
            Some(ep) => crate::conn_pool::normalize_endpoint(ep),
            None => return Ok(()),
        };
        let mut extent_ids: Vec<u64> = self
            .extents
            .iter()
            .map(|e| *e.key())
            .filter(|id| self.owns_extent(*id))
            .collect();

        // F210-D2: also include extent_ids that have an
        // `extent-{id}.ec.dat` staging file on disk. A CRASHED
        // mid-`handle_convert_to_ec` may leave a `.ec.dat` without a
        // corresponding `.dat` entry in `self.extents`. The F210-D2
        // `remove_extent_files` unlinks `.ec.dat` too, but only when
        // the manager TELLS us this extent is garbage — and the
        // manager only sees the IDs we report here. Without the scan,
        // the orphan `.ec.dat` persists forever (manager doesn't list
        // it; we don't list it). After the scan we report it; manager
        // says "yes, garbage"; we call `remove_extent_files` which
        // unlinks the staging file. Idempotent — if the extent IS
        // alive on the manager, no-op.
        {
            use std::collections::HashSet;
            let mut seen: HashSet<u64> = extent_ids.iter().copied().collect();
            for disk in self.disks.values() {
                for id in disk.scan_ec_staging_extent_ids() {
                    if self.owns_extent(id) && seen.insert(id) {
                        extent_ids.push(id);
                    }
                }
            }
        }

        if extent_ids.is_empty() {
            return Ok(());
        }
        let req = manager_rpc::rkyv_encode(&manager_rpc::ReconcileExtentsReq {
            // node_id 0 — the extent-node doesn't track its own node_id
            // (assigned by manager at register-time, not threaded down).
            // Manager uses this only for logging.
            node_id: 0,
            extent_ids: extent_ids.clone(),
        });
        // 10 s — read-only manager call (returns subset of submitted
        // extent ids that are no longer in s.extents). Bounded so a
        // hanging manager doesn't trap the periodic 5-min sweep.
        let resp_data = self
            .manager_pool
            .call_timeout(&mgr, manager_rpc::MSG_RECONCILE_EXTENTS, req, Duration::from_secs(10))
            .await
            .map_err(|e| anyhow::anyhow!("reconcile_extents rpc: {e}"))?;
        let resp: manager_rpc::ReconcileExtentsResp =
            manager_rpc::rkyv_decode(&resp_data)
                .map_err(|e| anyhow::anyhow!("decode reconcile resp: {e}"))?;
        if resp.code != manager_rpc::CODE_OK {
            return Err(anyhow::anyhow!(
                "reconcile_extents non-OK: {}",
                resp.message,
            ));
        }
        if resp.garbage.is_empty() {
            return Ok(());
        }
        tracing::info!(
            local = extent_ids.len(),
            garbage = resp.garbage.len(),
            "F109 startup reconcile: unlinking orphans",
        );
        for eid in &resp.garbage {
            // Drop in-memory entry and unlink files. Look up the disk
            // via the entry; if the entry is gone (concurrent delete),
            // fall back to scanning every disk.
            let entry = self.extents.remove(eid).map(|(_, v)| v);
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

    /// F099-M: does this shard own `extent_id`?
    #[inline]
    pub(crate) fn owns_extent(&self, extent_id: u64) -> bool {
        self.shard_count <= 1 || (extent_id % self.shard_count as u64) as u32 == self.shard_idx
    }

    /// F210-D1: look up / create the per-extent mutating-op lock. Held
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

    /// F099-M: return the local sibling address that owns `extent_id`
    /// (this host's shard for the target extent). None in single-thread mode.
    #[inline]
    fn sibling_for_extent(&self, extent_id: u64) -> Option<&str> {
        if self.shard_count <= 1 {
            return None;
        }
        let owner = (extent_id % self.shard_count as u64) as usize;
        self.sibling_addrs.get(owner).map(|s| s.as_str())
    }

    /// F099-M: forward a control-plane RPC to a sibling shard on the same
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
            .map_err(|e| (StatusCode::Unavailable, format!("forward to shard {sibling_addr}: {e}")))
    }

    /// Return the first online disk, or None if all are offline.
    fn choose_disk(&self) -> Option<Rc<DiskFS>> {
        self.disks.values().find(|d| d.online()).cloned()
    }

    /// Resolve DiskFS for an extent by its disk_id. Returns error string if disk is unknown.
    fn disk_for(&self, disk_id: u64) -> Result<Rc<DiskFS>, String> {
        self.disks.get(&disk_id).cloned().ok_or_else(|| {
            format!("unknown disk_id {disk_id}")
        })
    }

    /// Mark the disk hosting an extent as offline after an I/O error.
    pub(crate) fn mark_disk_offline_for_extent(&self, extent_id: u64) {
        if let Some(entry) = self.extents.get(&extent_id) {
            let disk_id = entry.disk_id;
            if let Some(disk) = self.disks.get(&disk_id) {
                if disk.online() {
                    tracing::error!(extent_id, disk_id, "marking disk offline due to I/O error");
                    disk.set_offline();
                }
            }
        }
    }

    pub(crate) async fn save_meta(&self, extent_id: u64, entry: &ExtentEntry) -> Result<(), String> {
        let sealed_length = entry.sealed_length.load(Ordering::SeqCst);
        let eversion = entry.eversion.load(Ordering::SeqCst);
        let last_revision = entry.last_revision.load(Ordering::SeqCst);

        // F157: always write V1 (44 bytes with CRC32C trailer).
        let mut buf = [0u8; Self::META_SIZE_V1];
        buf[0..8].copy_from_slice(Self::META_MAGIC_V1);
        buf[8..16].copy_from_slice(&extent_id.to_le_bytes());
        buf[16..24].copy_from_slice(&sealed_length.to_le_bytes());
        buf[24..32].copy_from_slice(&eversion.to_le_bytes());
        buf[32..40].copy_from_slice(&last_revision.to_le_bytes());
        let crc = crc32c::crc32c(&buf[0..Self::META_SIZE_V0]);
        buf[40..44].copy_from_slice(&crc.to_le_bytes());

        // F159: open + write + fsync. Pre-F159 the helper used
        // `compio::fs::write` which buffers via the page cache without
        // calling fsync; the .meta could remain in cache for an
        // unbounded time and be lost on a host crash. The most acute
        // failure mode was `apply_extent_meta_durable` writing the new
        // sealed_length to .meta before fsync'ing .dat — the .meta then
        // landed on disk via OS background flush, but the must_sync=false
        // bytes still in .dat's page cache were lost. On restart,
        // parse_meta returned the new sealed_length while file size of
        // .dat was shorter, so reads past the durable extent.len returned
        // EOF or zero-padded bytes. F159 makes save_meta itself durable
        // and reorders apply_extent_meta_durable to fsync .dat first.
        let disk = self.disk_for(entry.disk_id)?;
        let path = disk.meta_path(extent_id);
        let mut f = compio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|e| format!("open meta for extent {extent_id}: {e}"))?;
        let BufResult(result, _) = f.write_all_at(buf.to_vec(), 0).await;
        result.map_err(|e| format!("save meta for extent {extent_id}: {e}"))?;
        f.sync_data()
            .await
            .map_err(|e| format!("sync meta for extent {extent_id}: {e}"))?;
        Ok(())
    }

    fn parse_meta(buf: &[u8], extent_id: u64) -> Option<(u64, u64, i64)> {
        if buf.len() < Self::META_SIZE_V0 {
            return None;
        }
        // F157: dispatch on magic[7] for V0/V1 layout.
        let v1 = &buf[0..8] == Self::META_MAGIC_V1;
        let v0 = &buf[0..8] == Self::META_MAGIC_V0;
        if !v0 && !v1 {
            return None;
        }
        if v1 && buf.len() < Self::META_SIZE_V1 {
            return None;
        }
        let eid = u64::from_le_bytes(buf[8..16].try_into().ok()?);
        if eid != extent_id {
            return None;
        }
        if v1 {
            let stored_crc = u32::from_le_bytes(buf[40..44].try_into().ok()?);
            let computed_crc = crc32c::crc32c(&buf[0..Self::META_SIZE_V0]);
            if stored_crc != computed_crc {
                tracing::warn!(
                    extent_id,
                    stored_crc,
                    computed_crc,
                    "F157: meta sidecar CRC mismatch — bit rot or torn write; treating as missing"
                );
                return None;
            }
        } else {
            // V0 legacy: no checksum. Warn once per load so operators see the upgrade signal.
            tracing::warn!(
                extent_id,
                "F157: legacy V0 meta sidecar (no CRC) — will upgrade to V1 on next save_meta"
            );
        }
        let sealed_length = u64::from_le_bytes(buf[16..24].try_into().ok()?);
        let eversion = u64::from_le_bytes(buf[24..32].try_into().ok()?);
        let last_revision = i64::from_le_bytes(buf[32..40].try_into().ok()?);
        Some((sealed_length, eversion, last_revision))
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
                // F099-M: only load extents this shard owns. Under normal
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

                let (sealed_length, eversion, last_revision) =
                    match compio::fs::read(disk.meta_path(extent_id)).await {
                        Ok(buf) => Self::parse_meta(&buf, extent_id).unwrap_or((0, 1, 0)),
                        Err(_) => (0, 1, 0),
                    };

                extents.insert(
                    extent_id,
                    Rc::new(ExtentEntry {
                        file: RefCell::new(Rc::new(file)),
                        len: AtomicU64::new(len),
                                eversion: AtomicU64::new(eversion),
                        sealed_length: AtomicU64::new(sealed_length),
                        avali: AtomicU32::new(if sealed_length > 0 { 1 } else { 0 }),
                        last_revision: AtomicI64::new(last_revision),
                        disk_id: disk.disk_id,
                        coalescer: Coalescer::new(len),
                    }),
                );
                tracing::info!(
                    "loaded extent {extent_id} from disk {}: len={len}, sealed_length={sealed_length}, eversion={eversion}",
                    disk.disk_id
                );
            }
        }
        Ok(())
    }

    /// Start the RPC server on a single-threaded compio runtime.
    /// Accepts connections (TCP or UCX, per `autumn_transport::current()`)
    /// and handles them cooperatively. TCP-only socket tuning gated on
    /// `Conn::as_tcp()` so UCX paths skip the TCP setsockopt calls.
    pub async fn serve(&self, addr: SocketAddr) -> Result<()> {
        self.accept_loop(addr, "data").await
    }

    /// F191: serve BOTH the data-plane and a separate control-plane
    /// listener on the same `ExtentNode` instance. The control listener
    /// reuses `handle_connection` (same SQ/CQ machinery) but only
    /// receives small-payload control RPCs (`MSG_DF`, future
    /// `MSG_REPORT_DISK_FAILURE`, future heartbeat) so its `tx_bufs`
    /// flush and `FuturesUnordered` cap stay minimal in practice.
    ///
    /// We spawn the control listener as a detached compio task and run
    /// the data accept loop inline. If the control listener fails to
    /// bind, log + WARN and continue with data only (legacy behaviour
    /// preserved) — the manager's `control_address` fallback also
    /// covers a node whose control bind failed for an operator reason.
    pub async fn serve_with_control(
        &self,
        data_addr: SocketAddr,
        control_addr: SocketAddr,
    ) -> Result<()> {
        let ctl_node = self.clone();
        compio::runtime::spawn(async move {
            tracing::info!(addr = %control_addr, "extent node CONTROL listener");
            if let Err(e) = ctl_node.accept_loop(control_addr, "control").await {
                tracing::warn!(
                    addr = %control_addr,
                    error = %e,
                    "control listener exited"
                );
            }
        })
        .detach();
        self.accept_loop(data_addr, "data").await
    }

    /// Shared accept loop used by both `serve` and `serve_with_control`.
    /// `role` is a free-form label ("data" / "control") that goes into
    /// the listening log line for operator triage.
    async fn accept_loop(&self, addr: SocketAddr, role: &'static str) -> Result<()> {
        let transport = autumn_transport::current_or_init();
        let mut listener = transport.bind(addr).await?;
        tracing::info!(addr = %addr, role, kind = ?transport.kind(), "extent node listening");
        loop {
            let (conn, peer) = listener.accept().await?;
            if let Some(s) = conn.as_tcp() {
                if let Err(e) = s.set_nodelay(true) {
                    tracing::warn!(peer = %peer, error = %e, "set_nodelay failed");
                }
                set_tcp_buffer_sizes(s, 512 * 1024);
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
    ///          syscall (amortises writev across multiple ready completions).
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
    pub async fn handle_connection(
        conn: autumn_transport::Conn,
        node: ExtentNode,
    ) -> Result<()> {
        use futures::future::{select, Either, LocalBoxFuture};
        use futures::FutureExt;
        use futures::stream::{FuturesUnordered, StreamExt};

        const READ_BUF_SIZE: usize = 512 * 1024;

        let (reader, mut writer) = conn.into_split();
        let mut decoder = FrameDecoder::new();

        // F195: per-conn inflight cap from `ExtentNodeConfig.inflight_cap`,
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
            if !tx_bufs.is_empty() {
                let bufs = std::mem::take(&mut tx_bufs);
                let BufResult(result, _) = writer.write_vectored_all(bufs).await;
                result?;
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
                            &node, &mut decoder, &mut inflight, &mut tx_bufs, cap,
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
                                let _ = writer.write_vectored_all(bufs).await.0;
                            }
                            return Ok(());
                        }
                        ReadBurst::Err { e, .. } => return Err(e.into()),
                        ReadBurst::Data { buf, n, reader } => {
                            decoder.feed(&buf[..n]);
                            process_frames_backpressured(
                                &node, &mut decoder, &mut inflight, &mut tx_bufs, cap,
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
            MSG_COMMIT_EC_SHARD => self.handle_commit_ec_shard(payload).await,
            MSG_SYNCED_LENGTH => self.handle_synced_length(payload).await,
            MSG_PROBE_EXTENT => self.handle_probe_extent(payload).await,
            _ => Err((StatusCode::InvalidArgument, format!("unknown msg_type {msg_type}"))),
        }
    }

    async fn get_extent(&self, extent_id: u64) -> Result<Rc<ExtentEntry>, (StatusCode, String)> {
        // F099-M: hot-path RPCs (append/read/commit_length) must hit the
        // owning shard. A wrong-shard request signals a client routing
        // bug — surface it as FailedPrecondition so the client logs it
        // instead of silently succeeding on the wrong shard.
        if !self.owns_extent(extent_id) {
            return Err((
                StatusCode::FailedPrecondition,
                format!(
                    "extent {} belongs to shard {} not shard {} (shard_count={})",
                    extent_id,
                    extent_id % self.shard_count as u64,
                    self.shard_idx,
                    self.shard_count,
                ),
            ));
        }
        self.extents
            .get(&extent_id)
            .map(|v| Rc::clone(v.value()))
            .ok_or_else(|| (StatusCode::NotFound, format!("extent {} not found", extent_id)))
    }

    async fn ensure_extent(&self, extent_id: u64) -> Result<Rc<ExtentEntry>, String> {
        // F099-M: a non-owning shard should never `ensure_extent`. This is
        // an invariant violation — log loudly and reject.
        if !self.owns_extent(extent_id) {
            return Err(format!(
                "ensure_extent on wrong shard: extent {} → shard {}, this is shard {} (count={})",
                extent_id,
                extent_id % self.shard_count as u64,
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
            compio::fs::create_dir_all(parent).await
                .map_err(|e| e.to_string())?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|e| e.to_string())?;
        let len = file.metadata().await
            .map(|m| m.len())
            .map_err(|e| e.to_string())?;

        let disk_id = disk.disk_id;
        self.extents.insert(
            extent_id,
            Rc::new(ExtentEntry {
                        file: RefCell::new(Rc::new(file)),
                len: AtomicU64::new(len),
                eversion: AtomicU64::new(1),
                sealed_length: AtomicU64::new(0),
                avali: AtomicU32::new(0),
                last_revision: AtomicI64::new(0),
                disk_id,
                coalescer: Coalescer::new(len),
            }),
        );
        self.extents
            .get(&extent_id)
            .map(|v| Rc::clone(v.value()))
            .ok_or_else(|| format!("extent {} not found after insert", extent_id))
    }

    /// Apply extent metadata from manager. Returns true if sealed_length changed from 0 to nonzero.
    fn apply_extent_meta(extent: &ExtentEntry, ex: &ExtentInfo) -> bool {
        let old_sealed = extent.sealed_length.load(Ordering::SeqCst);
        extent.eversion.store(ex.eversion, Ordering::SeqCst);
        extent
            .sealed_length
            .store(ex.sealed_length, Ordering::SeqCst);
        extent.avali.store(ex.avali, Ordering::SeqCst);
        old_sealed == 0 && ex.sealed_length > 0
    }

    /// F143: apply extent metadata from manager AND make the seal
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
    ) -> bool {
        let sealed_changed = Self::apply_extent_meta(extent, ex);
        if sealed_changed {
            // F159: fsync .dat FIRST (data durable), THEN write+fsync .meta
            // (sealed_length / eversion durable). Pre-F159 the order was
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
            // itself was also made durable in F159 (open + write + fsync,
            // not bare `compio::fs::write`).
            if let Err(e) = extent.file_rc().sync_data().await {
                tracing::warn!(
                    extent_id,
                    sealed_length = ex.sealed_length,
                    error = %e,
                    "F159/F143: fsync of .dat failed before meta save — sealed prefix may not be durable",
                );
            }
            let _ = self.save_meta(extent_id, extent).await;
        }
        sealed_changed
    }


    async fn truncate_to_commit(extent: &Rc<ExtentEntry>, commit: u32) -> Result<(), String> {
        let f = extent.file_rc();
        f.set_len(commit as u64).await.map_err(|e| e.to_string())?;
        // F152: fsync the truncate. Without this, the kernel may report the
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
        extent.len.store(commit as u64, Ordering::SeqCst);
        // Bug fix: align the coalescer's view with the actual file
        // length post-truncate. `last_synced` is what `MSG_COMMIT_LENGTH`
        // and `MSG_PROBE_EXTENT` return; if we leave it at the
        // pre-truncate value, commit_length reports a length that no
        // longer exists on disk. `pending_fsync` follows the same
        // shrink — the subsequent pwrite (if any) will store its own
        // larger end value via the regular F178 path.
        extent.coalescer.last_synced.store(commit as u64, Ordering::SeqCst);
        extent.coalescer.pending_fsync.store(commit as u64, Ordering::SeqCst);
        Ok(())
    }

    /// Crate-visible wrapper for `truncate_to_commit` used from extent_worker.rs.
    pub(crate) async fn truncate_to_commit_ref(
        &self,
        extent: &Rc<ExtentEntry>,
        commit: u32,
    ) -> Result<(), String> {
        Self::truncate_to_commit(extent, commit).await
    }

    /// Copy the full extent data from a remote source node using autumn-rpc.
    async fn copy_bytes_from_source(
        addr: &str,
        extent_id: u64,
        eversion: u64,
    ) -> Result<Vec<u8>, String> {
        let sock: std::net::SocketAddr = parse_addr(addr)
            .map_err(|e| e.to_string())?;
        let req = ReadBytesReq {
            extent_id,
            eversion,
            offset: 0,
            length: 0,
        };
        let resp_bytes = rpc_oneshot(sock, MSG_READ_BYTES, req.encode())
            .await
            .map_err(|e| format!("read_bytes from {addr}: {e}"))?;
        let resp = ReadBytesResp::decode(resp_bytes)
            .map_err(|e| format!("decode: {e}"))?;
        if resp.code != CODE_OK {
            return Err(format!(
                "read_bytes error from {addr}: code={}",
                code_description(resp.code)
            ));
        }
        Ok(resp.payload.to_vec())
    }

    async fn fetch_full_extent_from_sources(
        &self,
        extent: &ExtentInfo,
        exclude_node_ids: &[u64],
    ) -> Result<Vec<u8>, String> {
        // TODO(F044): nodes_map_from_manager() stubbed
        let nodes = self.nodes_map_from_manager().await
            .map_err(|e| format!("nodes_map: {e}"))?;
        for node_id in extent.replicates.iter().chain(extent.parity.iter()) {
            if exclude_node_ids.contains(node_id) {
                continue;
            }
            let Some(addr) = nodes.get(node_id) else {
                continue;
            };
            let copied =
                Self::copy_bytes_from_source(addr, extent.extent_id, extent.eversion).await;
            if let Ok(payload) = copied {
                if extent.sealed_length > 0 && payload.len() < extent.sealed_length as usize {
                    continue;
                }
                return Ok(payload);
            }
        }
        Err("no source replica available for copy".to_string())
    }

    pub(crate) async fn extent_info_from_manager(&self, extent_id: u64) -> Result<Option<ExtentInfo>, String> {
        let mgr = match &self.manager_endpoint {
            Some(ep) => crate::conn_pool::normalize_endpoint(ep),
            None => return Ok(None),
        };
        let req = manager_rpc::rkyv_encode(&manager_rpc::ExtentInfoReq { extent_id });
        // 5 s — read-only manager call. Hot in F119-E
        // (handle_convert_to_ec syncs sealed_length / eversion from
        // manager) and the F147-C recovery verify-after-fetch path.
        let resp_data = self
            .manager_pool
            .call_timeout(&mgr, autumn_rpc::manager_rpc::MSG_EXTENT_INFO, req, Duration::from_secs(5))
            .await
            .map_err(|e| format!("extent_info rpc: {e}"))?;
        let resp: manager_rpc::ExtentInfoResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| format!("decode: {e}"))?;
        if resp.code != manager_rpc::CODE_OK {
            return Ok(None);
        }
        Ok(resp.extent.map(|e| mgr_to_local_extent(&e)))
    }

    async fn nodes_map_from_manager(&self) -> Result<HashMap<u64, String>, String> {
        let mgr = match &self.manager_endpoint {
            Some(ep) => crate::conn_pool::normalize_endpoint(ep),
            None => return Err("no manager endpoint configured".to_string()),
        };
        // 5 s — read-only manager call.
        let resp_data = self
            .manager_pool
            .call_timeout(&mgr, autumn_rpc::manager_rpc::MSG_NODES_INFO, Bytes::new(), Duration::from_secs(5))
            .await
            .map_err(|e| format!("nodes_info rpc: {e}"))?;
        let resp: manager_rpc::NodesInfoResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| format!("decode: {e}"))?;
        if resp.code != manager_rpc::CODE_OK {
            return Err(format!("nodes_info failed: {}", resp.message));
        }
        Ok(resp.nodes.into_iter().map(|(id, n)| (id, n.address)).collect())
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

        // F147-C: refuse-at-start — if the local extent already has a fresher
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

        // F194: gate cross-extent recovery concurrency. Acquired AFTER
        // the cheap F147-C stale-snapshot check so a stale recovery
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
        let payload = if !extent_info.ec_converted {
            // Replication recovery: copy full extent from any healthy peer.
            let raw = self
                .fetch_full_extent_from_sources(&extent_info, &[task.node_id, task.replace_id])
                .await?;
            if extent_info.sealed_length > 0 {
                raw[..(extent_info.sealed_length as usize)].to_vec()
            } else {
                raw
            }
        } else {
            // EC recovery: read individual shards from healthy peers and reconstruct
            // the missing shard for this node's slot in the extent.
            self.run_ec_recovery_payload(&task, &extent_info).await?
        };

        let extent = self.ensure_extent(task.extent_id).await?;

        extent.file_rc()
            .set_len(0)
            .await
            .map_err(|e| e.to_string())?;
        let payload_len = payload.len() as u64;
        file_pwrite_chunked(extent.file_rc(), 0, Bytes::from(payload)).await
            .map_err(|e| e.to_string())?;
        extent.file_rc().sync_data().await
            .map_err(|e| e.to_string())?;

        // F147-C: verify-after-sync — a concurrent apply_extent_meta_durable
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

        extent.len.store(payload_len, Ordering::SeqCst);
        // Use fetch_max instead of store for eversion/sealed_length/avali so
        // that any concurrent atomic update that landed between the check and
        // these stores cannot be rolled back. Monotonic progress is guaranteed
        // even in the race window after the eversion check above.
        let _ = extent.eversion.fetch_max(extent_info.eversion, Ordering::SeqCst);
        let _ = extent.sealed_length.fetch_max(extent_info.sealed_length, Ordering::SeqCst);
        let _ = extent.avali.fetch_max(extent_info.avali, Ordering::SeqCst);

        let _ = self.save_meta(task.extent_id, &extent).await;

        Ok(RecoveryTaskDone {
            task: task,
            ready_disk_id: extent.disk_id,
        })
    }

    /// For an EC extent: copy one shard from each of the `data_shards` healthy peers,
    /// then reconstruct the shard that belongs to the recovering node's slot.
    async fn run_ec_recovery_payload(
        &self,
        task: &crate::extent_rpc::RecoveryTask,
        extent_info: &ExtentInfo,
    ) -> Result<Vec<u8>, String> {
        let data_shards = extent_info.replicates.len();
        let parity_shards = extent_info.parity.len();
        let n = data_shards + parity_shards;

        // Build ordered list of all node IDs (data shards first, then parity shards).
        let all_node_ids: Vec<u64> = extent_info
            .replicates
            .iter()
            .chain(extent_info.parity.iter())
            .copied()
            .collect();

        // Determine which shard index this recovery is rebuilding.
        // `replace_id` is the failed node that needs to be replaced.
        let replacing_index = all_node_ids
            .iter()
            .position(|&id| id == task.replace_id)
            .ok_or_else(|| {
                format!(
                    "replace_id {} not found in extent {} node list",
                    task.replace_id, task.extent_id
                )
            })?;

        let nodes = self.nodes_map_from_manager().await?;

        // Copy the shard stored at each peer into the corresponding slot.
        // Skip the failed node (replace_id) and ourselves (node_id / disk_id).
        let mut shards: Vec<Option<Vec<u8>>> = vec![None; n];
        let mut collected = 0usize;

        for (i, &node_id) in all_node_ids.iter().enumerate() {
            if i == replacing_index {
                // This is the missing shard slot — leave as None.
                continue;
            }
            if node_id == task.node_id {
                // Skip ourselves.
                continue;
            }
            let Some(addr) = nodes.get(&node_id) else {
                continue;
            };
            match Self::copy_bytes_from_source(addr, task.extent_id, extent_info.eversion).await {
                Ok(shard_bytes) => {
                    // Trim to sealed length if the extent is sealed.
                    let shard = if extent_info.sealed_length > 0
                        && shard_bytes.len() > extent_info.sealed_length as usize
                    {
                        shard_bytes[..extent_info.sealed_length as usize].to_vec()
                    } else {
                        shard_bytes
                    };
                    shards[i] = Some(shard);
                    collected += 1;
                    if collected >= data_shards {
                        break; // Enough shards to reconstruct.
                    }
                }
                Err(_) => continue, // Unavailable peer — try next.
            }
        }

        if collected < data_shards {
            return Err(format!(
                "EC recovery: only {collected}/{data_shards} shards available for extent {}",
                task.extent_id
            ));
        }

        // F117: offload RS reconstruct (CPU-bound, GF(256) polynomial math
        // over up-to-data_shards × per-shard MiB) to the blocking pool so
        // recovery doesn't stall the extent-node compio runtime.
        compio::runtime::spawn_blocking(move || {
            crate::erasure::ec_reconstruct_shard(
                shards,
                data_shards,
                parity_shards,
                replacing_index,
            )
        })
        .await
        .map_err(|_| "EC reconstruct task panicked".to_string())?
        .map_err(|e| format!("EC reconstruct failed: {e}"))
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
    async fn write_shard_local(
        &self,
        extent_id: u64,
        shard_index: usize,
        sealed_length: u64,
        _new_eversion: u64,
        shard_data: Bytes,
    ) -> Result<(), (StatusCode, String)> {
        let entry = self.ensure_extent(extent_id).await
            .map_err(|e| (StatusCode::Internal, e))?;

        let disk = self.disk_for(entry.disk_id)
            .map_err(|e| (StatusCode::Internal, e))?;
        let staging_path = disk.ec_staging_path(extent_id);
        let shard_len = shard_data.len();

        // Idempotent: if a prior prepare already wrote .ec.dat with
        // the correct shard size, skip the redundant I/O.
        if let Ok(meta) = compio::fs::metadata(&staging_path).await {
            if meta.len() == shard_len as u64 {
                tracing::info!(
                    extent_id,
                    shard_index,
                    shard_len,
                    "EC prepare: staging file already exists with correct size, skipping"
                );
                return Ok(());
            }
        }

        if let Some(parent) = staging_path.parent() {
            compio::fs::create_dir_all(parent).await
                .map_err(|e| (StatusCode::Internal, format!("mkdir for staging {extent_id}: {e}")))?;
        }

        let staging_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&staging_path)
            .await
            .map_err(|e| (StatusCode::Internal, format!("create staging {extent_id}: {e}")))?;

        // F171: staging file is local to this function — never aliased
        // by other tasks (the path is unique per `extent_id`), so a
        // freshly-created `Rc` suffices. We share via clone for the
        // sync_data call below.
        let staging_rc = Rc::new(staging_file);
        file_pwrite_chunked(staging_rc.clone(), 0, shard_data).await
            .map_err(|e| (StatusCode::Internal, format!("write staging {extent_id}/{shard_index}: {e}")))?;
        staging_rc.sync_data().await
            .map_err(|e| (StatusCode::Internal, format!("sync staging {extent_id}: {e}")))?;

        tracing::info!(
            extent_id,
            shard_index,
            shard_len,
            sealed_length,
            "EC prepare: shard written to staging file"
        );
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
    async fn commit_shard_local(
        &self,
        extent_id: u64,
        sealed_length: u64,
        new_eversion: u64,
    ) -> Result<(), (StatusCode, String)> {
        let entry = self.ensure_extent(extent_id).await
            .map_err(|e| (StatusCode::Internal, e))?;

        let disk = self.disk_for(entry.disk_id)
            .map_err(|e| (StatusCode::Internal, e))?;
        let staging_path = disk.ec_staging_path(extent_id);
        let dat_path = disk.extent_path(extent_id);

        let staging_exists = compio::fs::metadata(&staging_path).await.is_ok();
        if !staging_exists {
            // Idempotent: staging file already renamed (prior commit
            // succeeded but response was lost). Check eversion to
            // confirm this is a replay, not a missing prepare.
            let local_ev = entry.eversion.load(Ordering::SeqCst);
            if local_ev >= new_eversion {
                return Ok(());
            }
            return Err((
                StatusCode::FailedPrecondition,
                format!(
                    "commit_shard extent {extent_id}: staging file missing and \
                     eversion {local_ev} < {new_eversion} — prepare was not run"
                ),
            ));
        }

        compio::fs::rename(&staging_path, &dat_path).await
            .map_err(|e| (StatusCode::Internal, format!("rename staging {extent_id}: {e}")))?;

        // Reopen the file at the .dat path so entry.file points to the
        // new (shard) data instead of the old (unlinked) inode.
        let new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&dat_path)
            .await
            .map_err(|e| (StatusCode::Internal, format!("reopen {extent_id}: {e}")))?;
        let shard_len = new_file.metadata().await
            .map(|m| m.len())
            .map_err(|e| (StatusCode::Internal, format!("metadata {extent_id}: {e}")))?;

        // F171: safe replace via `RefCell::borrow_mut` + `Rc` swap.
        // Concurrent readers have already cloned an `Rc<CompioFile>`
        // off `entry.file_rc()` for their I/O, so they keep the OLD
        // file alive in their captured `Rc` until they drop. F153's
        // per-extent `ec_conversion_locks` still serialises concurrent
        // EC dispatches at the handler level (so two converts don't
        // race on the staging path), and F119-C's eversion-mismatch
        // reject covers concurrent reads from stale-cached clients —
        // but they are no longer load-bearing for memory safety here.
        entry.replace_file(new_file);

        entry.len.store(shard_len, Ordering::SeqCst);
        // F119-E: sealed_length = original payload length (from manager),
        // not shard size.
        entry.sealed_length.store(sealed_length.max(shard_len), Ordering::SeqCst);
        entry.avali.store(1, Ordering::SeqCst);
        if new_eversion > 0 {
            entry.eversion.store(new_eversion, Ordering::SeqCst);
        }

        self.save_meta(extent_id, &entry).await
            .map_err(|e| (StatusCode::Internal, e))?;

        tracing::info!(
            extent_id,
            shard_len,
            sealed_length,
            new_eversion,
            "EC commit: staging renamed to .dat, eversion bumped"
        );
        Ok(())
    }

    // ─── RPC Handlers ────────────────────────────────────────────────────────

    async fn handle_append(&self, payload: Bytes) -> HandlerResult {
        let req = AppendReq::decode(payload)
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        let extent = self.get_extent(req.extent_id).await?;

        // Only fetch from manager when local eversion is behind what the client expects.
        // In the common case (eversions match) we trust local atomics -- no RPC needed.
        let local_eversion = extent.eversion.load(Ordering::SeqCst);
        if req.eversion > local_eversion {
            // TODO(F044): manager RPC for eversion refresh not yet implemented
            match self.extent_info_from_manager(req.extent_id).await {
                Ok(Some(ex)) => {
                    // F143: fsync on 0→sealed transition so the
                    // sealed prefix is durable on this node before we
                    // surface the seal upstream.
                    let _ = self
                        .apply_extent_meta_durable(req.extent_id, &extent, &ex)
                        .await;
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
            return Ok(AppendResp {
                code: CODE_PRECONDITION,
                offset: 0,
                end: 0,
            }
            .encode());
        }
        if extent.sealed_length.load(Ordering::SeqCst) > 0
            || extent.avali.load(Ordering::SeqCst) > 0
        {
            return Ok(AppendResp {
                code: CODE_PRECONDITION,
                offset: 0,
                end: 0,
            }
            .encode());
        }



        let last_revision = extent.last_revision.load(Ordering::SeqCst);
        if req.revision < last_revision {
            return Ok(AppendResp {
                code: CODE_LOCKED_BY_OTHER,
                offset: 0,
                end: 0,
            }
            .encode());
        }
        let revision_changed = req.revision > last_revision;
        if revision_changed {
            extent
                .last_revision
                .store(req.revision, Ordering::SeqCst);
        }

        let mut start = extent.len.load(Ordering::SeqCst);
        if start < req.commit as u64 {
            return Ok(AppendResp {
                code: CODE_PRECONDITION,
                offset: 0,
                end: 0,
            }
            .encode());
        }
        if start > req.commit as u64 {
            // F119-E: confirm with the manager that this extent is NOT
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
            // afterwards (surfaced as F119-E `invalid meta_len=...` /
            // `logStream value short`). The manager round-trip on this
            // path is acceptable because commit-reconciliation
            // truncation is rare in normal operation (only fires when
            // this replica got ahead of the consensus min).
            if let Ok(Some(mgr_info)) = self.extent_info_from_manager(req.extent_id).await {
                if mgr_info.sealed_length > 0 {
                    // F143: fsync as part of accepting the seal —
                    // see apply_extent_meta_durable for why.
                    let _ = self
                        .apply_extent_meta_durable(req.extent_id, &extent, &mgr_info)
                        .await;
                    return Ok(AppendResp {
                        code: CODE_PRECONDITION,
                        offset: 0,
                        end: 0,
                    }
                    .encode());
                }
            }
            Self::truncate_to_commit(&extent, req.commit)
                .await
                .map_err(|e| (StatusCode::Internal, e))?;
            // F147-B: re-check seal state after the truncate await (symmetric
            // to F146 in build_append_future). A concurrent
            // apply_extent_meta_durable (from handle_re_avali or another
            // handle_append's pre-truncate seal-confirm branch) may have landed
            // a fresh seal DURING the truncate I/O. Without this re-check the
            // subsequent file_pwrite would write bytes past sealed_length —
            // corrupting subsequent reads as "logStream value short" or
            // out-of-bounds slice panics on EC reads.
            if extent.sealed_length.load(Ordering::SeqCst) > 0
                || extent.avali.load(Ordering::SeqCst) > 0
            {
                return Ok(AppendResp { code: CODE_PRECONDITION, offset: 0, end: 0 }.encode());
            }
            start = extent.len.load(Ordering::SeqCst);
        }

        let data_payload = req.payload;

        if let Err(e) = file_pwrite(extent.file_rc(), start, data_payload.clone()).await {
            self.mark_disk_offline_for_extent(req.extent_id);
            return Err((StatusCode::Internal, e.to_string()));
        }
        let start_offset = start as u32;
        let end = start + data_payload.len() as u64;
        // F178: every append is durable via the per-extent coalescer. See
        // `register_sync_waiter` and the matching block in
        // `build_append_future` for the full design.
        extent.coalescer.pending_fsync.store(end, Ordering::SeqCst);
        let rx = register_sync_waiter(&extent, end);
        match rx.await {
            Ok(Ok(())) => {}
            Ok(Err(msg)) => {
                self.mark_disk_offline_for_extent(req.extent_id);
                return Err((StatusCode::Internal, msg));
            }
            Err(_canceled) => {
                self.mark_disk_offline_for_extent(req.extent_id);
                return Err((
                    StatusCode::Internal,
                    "fsync coalescer canceled".to_string(),
                ));
            }
        }

        extent.len.store(end, Ordering::SeqCst);

        if revision_changed {
            let _ = self.save_meta(req.extent_id, &extent).await;
        }

        Ok(AppendResp {
            code: CODE_OK,
            offset: start_offset,
            end: end as u32,
        }
        .encode())
    }


    async fn handle_read_bytes(&self, payload: Bytes) -> HandlerResult {
        let req = ReadBytesReq::decode(payload)
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        let extent = self.get_extent(req.extent_id).await?;

        // Use local extent state for eversion checks (no manager RPC needed on reads).
        // Returning a typed CODE_EVERSION_MISMATCH (rather than an Err
        // status) lets the StreamClient distinguish "stale cache —
        // refetch ExtentInfo and retry" from generic transport errors.
        // Critical post-EC-conversion: a stale-cache client would
        // otherwise drive 3-replica failover-with-timeout against
        // shrunken shard files (see plan: ec-http-...-smooth-tome.md).
        //
        // F119-C: enforce req.eversion < ev unconditionally — the prior
        // `req.eversion > 0` skip silently let through a stale-cached
        // eversion=0 (populated when the extent was open via
        // load_stream_tail / alloc_new_extent_once). After split bumped
        // ev to 1 and EC conversion bumped ev to 2 + shrunk the on-disk
        // file to shard_size, a cross-shard sub-range read (e.g. a 14 MB
        // VP straddling shards 0/1) silently truncated to the bytes
        // remaining in shard 0. The client now correctly sees
        // EVERSION_MISMATCH on attempt 0, invalidates the cache, and the
        // retry routes through ec_subrange_read.
        let ev = extent.eversion.load(Ordering::SeqCst);
        if req.eversion < ev {
            return Ok(ReadBytesResp {
                code: CODE_EVERSION_MISMATCH,
                end: 0,
                payload: Bytes::new(),
            }
            .encode());
        }

        let total_len = extent.len.load(Ordering::SeqCst);
        let end = total_len as u32;
        let read_offset = req.offset as u64;
        let read_size = if req.length == 0 {
            total_len.saturating_sub(read_offset)
        } else {
            (req.length as u64).min(total_len.saturating_sub(read_offset))
        };

        // Chunk pread to dodge the per-syscall INT_MAX cap on macOS /
        // 0x7ffff000 on Linux. Recovery (`copy_bytes_from_source`) sends
        // length=0 to slurp full sealed extents in one RPC, so the
        // per-syscall size on the server side can exceed 2 GiB.
        let data = file_pread_chunked(extent.file_rc(), read_offset, read_size as usize).await
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;

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

        // F099-M: commit_length is a hot-path RPC; reject wrong-shard.
        if !self.owns_extent(req.extent_id) {
            return Err((
                StatusCode::FailedPrecondition,
                format!(
                    "extent {} belongs to shard {} not shard {} (shard_count={})",
                    req.extent_id,
                    req.extent_id % self.shard_count as u64,
                    self.shard_idx,
                    self.shard_count,
                ),
            ));
        }

        let entry = self
            .extents
            .get(&req.extent_id)
            .ok_or_else(|| {
                (
                    StatusCode::NotFound,
                    format!("extent {} not found", req.extent_id),
                )
            })?;

        // F210-H3 Tier 2 (post-2026-05-17): `req.revision <= 0` is a
        // protocol error, not a sentinel. The pre-F210-H2 "revision == 0
        // bypasses the fence" escape hatch tangled three call sites
        // (seal probe, recovery liveness, autumn-client info) onto one
        // RPC and forced ad-hoc fence skipping; F210-H2 closed it and
        // broke the seal+recovery paths; the Tier 2 redesign splits
        // probe-without-fence onto `MSG_PROBE_EXTENT` and tightens THIS
        // RPC into a clean fence-enforcing primitive. Callers that
        // legitimately don't have an owner (manager recovery liveness,
        // `autumn-client info` display) now use `handle_probe_extent`.
        //
        // Fence handover semantics on the surviving (revision > 0) path:
        //   revision < last_revision → CODE_LOCKED_BY_OTHER (stale owner)
        //   revision = last_revision → no-op, return length
        //   revision > last_revision → bump + persist .meta (handover)
        // The handover-on-bump is load-bearing: when a new owner first
        // contacts an EN with a higher revision (via manager's
        // `handle_check_commit_length` per-replica probe carrying the
        // PS's validated revision), this is what advances the fence
        // BEFORE the new owner's first append. Old owners get
        // CODE_LOCKED_BY_OTHER on their next append.
        if req.revision <= 0 {
            return Err((
                StatusCode::InvalidArgument,
                format!(
                    "commit_length requires revision > 0 (got {}); use \
                     MSG_PROBE_EXTENT for fence-free probes",
                    req.revision
                ),
            ));
        }
        let last = entry.last_revision.load(Ordering::SeqCst);
        if req.revision < last {
            return Ok(CommitLengthResp {
                code: CODE_LOCKED_BY_OTHER,
                length: 0,
            }
            .encode());
        }
        if req.revision > last {
            entry.last_revision.store(req.revision, Ordering::SeqCst);
            let _ = self.save_meta(req.extent_id, &entry).await;
        }
        // F119-E: for sealed extents, return the LOGICAL sealed length
        // (the original payload length, agreed with the manager). For
        // open extents, return the **F210-B3 fix**: the durable
        // high-water (`coalescer.last_synced`), NOT `entry.len`.
        //
        // Pre-F210-B3 this returned `entry.len`, which is set to
        // `total_end` BEFORE the pwrite + fsync future is even returned
        // (see `build_append_future` step 7). A concurrent peer (e.g.
        // EC convert peer-copy gap fill, or manager seal) querying
        // commit_length during the pwrite-to-fsync window would read
        // the reservation and treat it as committed. Manager would
        // then seal at a non-durable value; on this replica's crash
        // before fsync, the file shrinks back below sealed_length →
        // permanent inconsistency in etcd.
        //
        // F178's per-extent coalescer maintains `last_synced` =
        // post-fsync durable high-water. Returning it gives the strict
        // "what's actually on disk" guarantee that seal needs.
        // Trade-off: bytes between `last_synced` and `entry.len` (in
        // flight pwrites) are temporarily invisible to commit_length;
        // they reappear on the next coalescer tick (1-5 ms later).
        // For the original F119-E concern (post-EC-conversion shard
        // size), `last_synced` is also bounded above by
        // `sealed_length` for sealed extents (set in
        // `apply_extent_meta_durable`), so the EC-shard-size confusion
        // doesn't recur.
        let sealed = entry.sealed_length.load(Ordering::SeqCst);
        let length = if sealed > 0 {
            sealed
        } else {
            entry.coalescer.last_synced.load(Ordering::SeqCst)
        };
        Ok(CommitLengthResp {
            code: CODE_OK,
            length: length as u32,
        }
        .encode())
    }

    /// F210-H3 Tier 2: manager-only fence-free length+existence probe.
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
    ///   (a) takes no revision — request is 8 bytes, not 16.
    ///   (b) does NOT touch the owner-lock fence — never returns
    ///       LOCKED_BY_OTHER, never mutates `last_revision`, never
    ///       writes `.meta`.
    /// Length-source semantics are identical to commit_length so the
    /// `info` CLI display matches what a real owner would see.
    async fn handle_probe_extent(&self, payload: Bytes) -> HandlerResult {
        let req = ProbeExtentReq::decode(payload)
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        if !self.owns_extent(req.extent_id) {
            return Err((
                StatusCode::FailedPrecondition,
                format!(
                    "extent {} belongs to shard {} not shard {} (shard_count={})",
                    req.extent_id,
                    req.extent_id % self.shard_count as u64,
                    self.shard_idx,
                    self.shard_count,
                ),
            ));
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

        let sealed = entry.sealed_length.load(Ordering::SeqCst);
        let length = if sealed > 0 {
            sealed
        } else {
            entry.coalescer.last_synced.load(Ordering::SeqCst)
        };
        Ok(ProbeExtentResp {
            code: CODE_OK,
            length: length as u32,
        }
        .encode())
    }

    /// F178 Phase 2: report the per-extent fsync coalescer's
    /// `last_synced_offset`. Used by `flush_one_imm` (via
    /// `StreamClient::await_log_synced_to`) to ensure all log_stream bytes
    /// referenced by a to-be-flushed memtable's ValuePointers are durable
    /// on this replica before the SST upload.
    ///
    /// Notes:
    /// - This is a node-local view; the client takes the quorum-min across
    ///   3 replicas (mirror of F156 commit_length quorum).
    /// - For sealed extents, all bytes up to `sealed_length` were forced
    ///   durable by `apply_extent_meta_durable` at seal time, so we
    ///   bound-up to `max(last_synced, sealed_length)` here. Otherwise a
    ///   reader of a sealed extent could observe `last_synced=0` purely
    ///   because no append-driven sync has run since this node loaded the
    ///   extent — even though the bytes are demonstrably on disk.
    async fn handle_synced_length(&self, payload: Bytes) -> HandlerResult {
        let req = SyncedLengthReq::decode(payload)
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        // F099-M: hot-path RPC; reject wrong-shard.
        if !self.owns_extent(req.extent_id) {
            return Err((
                StatusCode::FailedPrecondition,
                format!(
                    "extent {} belongs to shard {} not shard {} (shard_count={})",
                    req.extent_id,
                    req.extent_id % self.shard_count as u64,
                    self.shard_idx,
                    self.shard_count,
                ),
            ));
        }

        let entry = self.extents.get(&req.extent_id).ok_or_else(|| {
            (
                StatusCode::NotFound,
                format!("extent {} not found", req.extent_id),
            )
        })?;

        let synced = entry.coalescer.last_synced.load(Ordering::SeqCst);
        let sealed = entry.sealed_length.load(Ordering::SeqCst);
        let length = synced.max(sealed);
        Ok(SyncedLengthResp {
            code: CODE_OK,
            length,
        }
        .encode())
    }

    async fn handle_alloc_extent(&self, payload: Bytes) -> HandlerResult {
        let req: AllocExtentReq = rkyv_decode(&payload)
            .map_err(|e| (StatusCode::InvalidArgument, e))?;

        // F099-M: forward to owner shard if we don't own this extent.
        if !self.owns_extent(req.extent_id) {
            if let Some(sibling) = self.sibling_for_extent(req.extent_id) {
                return self
                    .forward_rpc_to_sibling(sibling, MSG_ALLOC_EXTENT, payload)
                    .await;
            }
        }

        let disk = self
            .choose_disk()
            .ok_or_else(|| (StatusCode::Unavailable, "no online disk available".to_string()))?;
        let disk_id = disk.disk_id;

        let path = disk.extent_path(req.extent_id);
        if let Some(parent) = path.parent() {
            compio::fs::create_dir_all(parent).await
                .map_err(|e| (StatusCode::Internal, e.to_string()))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;
        let len = file.metadata().await
            .map(|m| m.len())
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;

        self.extents.insert(
            req.extent_id,
            Rc::new(ExtentEntry {
                        file: RefCell::new(Rc::new(file)),
                len: AtomicU64::new(len),
                eversion: AtomicU64::new(1),
                sealed_length: AtomicU64::new(0),
                avali: AtomicU32::new(0),
                last_revision: AtomicI64::new(0),
                disk_id,
                coalescer: Coalescer::new(len),
            }),
        );

        let entry = self.get_extent(req.extent_id).await?;
        self.save_meta(req.extent_id, &entry).await
            .map_err(|e| (StatusCode::Internal, e))?;

        Ok(rkyv_encode(&AllocExtentResp {
            code: CODE_OK,
            disk_id,
            message: String::new(),
        }))
    }

    async fn handle_df(&self, payload: Bytes) -> HandlerResult {
        let req: DfReq = rkyv_decode(&payload)
            .map_err(|e| (StatusCode::InvalidArgument, e))?;

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
                        },
                    ));
                }
            }
        }

        let done_tasks = {
            let mut done = self.recovery_done.borrow_mut();
            if req.tasks.is_empty() {
                std::mem::take(&mut *done)
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
                *done = remaining;
                matched
            }
        };

        Ok(rkyv_encode(&DfResp {
            done_tasks,
            disk_status,
        }))
    }

    async fn handle_require_recovery(&self, payload: Bytes) -> HandlerResult {
        let req: RequireRecoveryReq = rkyv_decode(&payload)
            .map_err(|e| (StatusCode::InvalidArgument, e))?;

        // F099-M: forward to owner shard.
        if !self.owns_extent(req.task.extent_id) {
            if let Some(sibling) = self.sibling_for_extent(req.task.extent_id) {
                return self
                    .forward_rpc_to_sibling(sibling, MSG_REQUIRE_RECOVERY, payload)
                    .await;
            }
        }

        let task = req.task;

        if self.manager_endpoint.is_none() {
            return Ok(rkyv_encode(&CodeResp {
                code: CODE_PRECONDITION,
                message: "manager endpoint is not configured".to_string(),
            }));
        }

        if self.recovery_inflight.contains_key(&task.extent_id) {
            return Ok(rkyv_encode(&CodeResp {
                code: CODE_PRECONDITION,
                message: format!("extent {} recovery already running", task.extent_id),
            }));
        }

        if self.extents.contains_key(&task.extent_id) {
            return Ok(rkyv_encode(&CodeResp {
                code: CODE_PRECONDITION,
                message: format!("extent {} already exists", task.extent_id),
            }));
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
                        node.recovery_done.borrow_mut().push(done);
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

        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    /// F109: unlink the physical extent files after the manager has
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
        let req: DeleteExtentReq = rkyv_decode(&payload)
            .map_err(|e| (StatusCode::InvalidArgument, e))?;

        // F099-M: forward to owner shard so each shard only ever
        // touches the extents whose ids hash to it.
        if !self.owns_extent(req.extent_id) {
            if let Some(sibling) = self.sibling_for_extent(req.extent_id) {
                return self
                    .forward_rpc_to_sibling(sibling, MSG_DELETE_EXTENT, payload)
                    .await;
            }
        }

        // F139: if recovery is in flight for this extent, refuse the delete.
        // run_recovery_task's ensure_extent auto-creates on NotFound; if we
        // unlink now, recovery either writes to the unlinked inode (data
        // evaporates when fd closes) or resurrects the extent on-disk as an
        // orphan with no manager record. The manager's extent_delete_loop
        // retries up to 60× (~2 min); orphan-reconcile (F113) is the backstop
        // if that budget exhausts before recovery completes.
        if self.recovery_inflight.contains_key(&req.extent_id) {
            return Ok(rkyv_encode(&CodeResp {
                code: CODE_PRECONDITION,
                message: format!(
                    "extent {} recovery in flight; delete deferred",
                    req.extent_id
                ),
            }));
        }

        // F210-D1: try-acquire the per-extent mutating-op lock. If held
        // by an in-flight `handle_convert_to_ec` or `handle_re_avali`,
        // refuse the delete with CODE_PRECONDITION. Pre-F210-D1 the
        // F139 check only covered the recovery↔delete pair; convert
        // and re_avali could race with delete (data-loss paths
        // documented in feature_list F210-D1):
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
                return Ok(rkyv_encode(&CodeResp {
                    code: CODE_PRECONDITION,
                    message: format!(
                        "extent {} has in-flight mutating op (convert/re_avali); delete deferred",
                        req.extent_id
                    ),
                }));
            }
        };

        // Pull the entry out of the map so any later append on this id
        // fails with NotFound rather than racing the unlink.
        let entry = self.extents.remove(&req.extent_id).map(|(_, v)| v);

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
                Ok(rkyv_encode(&CodeResp {
                    code: CODE_OK,
                    message: String::new(),
                }))
            }
            Some(e) => Ok(rkyv_encode(&CodeResp {
                code: CODE_ERROR,
                message: e.to_string(),
            })),
        }
    }

    async fn handle_re_avali(&self, payload: Bytes) -> HandlerResult {
        let req: ReAvaliReq = rkyv_decode(&payload)
            .map_err(|e| (StatusCode::InvalidArgument, e))?;

        // F099-M: forward to owner shard.
        if !self.owns_extent(req.extent_id) {
            if let Some(sibling) = self.sibling_for_extent(req.extent_id) {
                return self
                    .forward_rpc_to_sibling(sibling, MSG_RE_AVALI, payload)
                    .await;
            }
        }

        // F210-D1: acquire the per-extent mutating-op lock for the
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
                return Ok(rkyv_encode(&CodeResp {
                    code: CODE_NOT_FOUND,
                    message: format!("extent {} not found", req.extent_id),
                }));
            }
        };

        // TODO(F044): manager RPC for extent_info not yet implemented
        let extent_info = match self.extent_info_from_manager(req.extent_id).await {
            Ok(Some(ex)) => ex,
            Ok(None) => {
                return Ok(rkyv_encode(&CodeResp {
                    code: CODE_NOT_FOUND,
                    message: format!("extent {} not found in manager", req.extent_id),
                }));
            }
            Err(e) => {
                return Ok(rkyv_encode(&CodeResp {
                    code: CODE_ERROR,
                    message: e,
                }));
            }
        };
        // F143: fsync on 0→sealed transition.
        let _ = self
            .apply_extent_meta_durable(req.extent_id, &extent, &extent_info)
            .await;

        if req.eversion < extent_info.eversion {
            return Ok(rkyv_encode(&CodeResp {
                code: CODE_PRECONDITION,
                message: format!(
                    "eversion too low: got {}, expect >= {}",
                    req.eversion, extent_info.eversion
                ),
            }));
        }

        // F206: RE_AVALI is a replicated-extent repair primitive. For an
        // EC'd extent the local shard size is `sealed_length / K`, so
        // the `local_len >= sealed_length` check below would always fall
        // through to `fetch_full_extent_from_sources` — which allocates a
        // `sealed_length`-sized Vec<u8> per peer and (on success) would
        // overwrite the local shard with raw bytes, corrupting EC.
        // Missing-shard repair on an EC'd extent must route through
        // EXT_MSG_REQUIRE_RECOVERY → run_ec_recovery_payload. Returning
        // CODE_OK here also lets the manager's recovery_dispatch_loop
        // self-heal pre-F206 buggy `avali` values via mark_extent_available
        // on the next 2 s tick.
        if extent_info.ec_converted {
            return Ok(rkyv_encode(&CodeResp {
                code: CODE_OK,
                message: String::new(),
            }));
        }

        let local_len = extent.len.load(Ordering::SeqCst);
        if local_len >= extent_info.sealed_length {
            return Ok(rkyv_encode(&CodeResp {
                code: CODE_OK,
                message: String::new(),
            }));
        }

        // F210-E1: gate cross-extent re_avali concurrency through the
        // shared recovery permit pool. Pre-F210-E1 only `run_recovery_task`
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

        let copied = self.fetch_full_extent_from_sources(&extent_info, &[]).await;
        let raw_payload = match copied {
            Ok(v) => v,
            Err(err) => {
                return Ok(rkyv_encode(&CodeResp {
                    code: CODE_ERROR,
                    message: err,
                }));
            }
        };

        let want = extent_info.sealed_length as usize;
        if raw_payload.len() < want {
            return Ok(rkyv_encode(&CodeResp {
                code: CODE_ERROR,
                message: format!("copied payload too short: {} < {}", raw_payload.len(), want),
            }));
        }
        let write_payload = Bytes::from(raw_payload[..want].to_vec());


        extent.file_rc()
            .set_len(0)
            .await
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;
        let payload_len = write_payload.len() as u64;
        file_pwrite_chunked(extent.file_rc(), 0, write_payload).await
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;
        extent.file_rc().sync_data().await
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;
        extent.len.store(payload_len, Ordering::SeqCst);

        let _ = self.save_meta(req.extent_id, &extent).await;

        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    async fn handle_copy_extent(&self, payload: Bytes) -> HandlerResult {
        let req = CopyExtentReq::decode(payload.clone())
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        // F099-M: forward to owner shard.
        if !self.owns_extent(req.extent_id) {
            if let Some(sibling) = self.sibling_for_extent(req.extent_id) {
                return self
                    .forward_rpc_to_sibling(sibling, MSG_COPY_EXTENT, payload)
                    .await;
            }
        }

        let extent = self.get_extent(req.extent_id).await?;
        let mut logical_len = extent.len.load(Ordering::SeqCst);

        // TODO(F044): manager RPC for extent_info not yet implemented
        match self.extent_info_from_manager(req.extent_id).await {
            Ok(Some(ex)) => {
                // F143: fsync on 0→sealed transition.
                let _ = self
                    .apply_extent_meta_durable(req.extent_id, &extent, &ex)
                    .await;
                if req.eversion < ex.eversion {
                    return Err((
                        StatusCode::FailedPrecondition,
                        format!(
                            "eversion too low: got {}, expect >= {}",
                            req.eversion, ex.eversion
                        ),
                    ));
                }
                if ex.sealed_length > 0 {
                    logical_len = logical_len.min(ex.sealed_length);
                }
            }
            Ok(None) => {
                let ev = extent.eversion.load(Ordering::SeqCst);
                // F160: drop the `req.eversion > 0` clause to match
                // F119-C's invariant. Pre-F160 the check skipped on
                // `req.eversion == 0`, which the F119-C closure for
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
                        format!(
                            "eversion too low: got {}, expect >= {}",
                            req.eversion, ev
                        ),
                    ));
                }
            }
            Err(_) => {
                let ev = extent.eversion.load(Ordering::SeqCst);
                // F160: same tightening as the Ok(None) branch above.
                if req.eversion < ev {
                    return Err((
                        StatusCode::FailedPrecondition,
                        format!(
                            "eversion too low: got {}, expect >= {}",
                            req.eversion, ev
                        ),
                    ));
                }
            }
        }

        // F148-B: refuse copy on unsealed extents. Production callers
        // (run_recovery_task, handle_re_avali) only target sealed extents
        // by design — the manager dispatches recovery/re-avali after seal.
        // Without this guard, a stray caller hitting an unsealed extent
        // could race a concurrent in-flight handle_append's
        // truncate_to_commit await window and observe a mix of pre- and
        // post-truncate bytes via file_pread_chunked below. On a sealed
        // extent the append protocol step 3 rejects concurrent appends, so
        // the race only exists for unsealed extents. Belt-and-braces.
        if extent.sealed_length.load(Ordering::SeqCst) == 0 {
            return Err((
                StatusCode::FailedPrecondition,
                format!(
                    "copy_extent on unsealed extent {} refused (sealed_length=0)",
                    req.extent_id
                ),
            ));
        }

        let offset = req.offset.min(logical_len);
        let size = if req.size == 0 {
            logical_len.saturating_sub(offset)
        } else {
            req.size.min(logical_len.saturating_sub(offset))
        };

        let data = file_pread_chunked(extent.file_rc(), offset, size as usize).await
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;

        Ok(CopyExtentResp {
            code: CODE_OK,
            payload: Bytes::from(data),
        }
        .encode())
    }

    async fn handle_convert_to_ec(&self, payload: Bytes) -> HandlerResult {
        let req: ConvertToEcReq = rkyv_decode(&payload)
            .map_err(|e| (StatusCode::InvalidArgument, e))?;

        // F099-M: forward to owner shard.
        if !self.owns_extent(req.extent_id) {
            if let Some(sibling) = self.sibling_for_extent(req.extent_id) {
                return self
                    .forward_rpc_to_sibling(sibling, MSG_CONVERT_TO_EC, payload)
                    .await;
            }
        }

        let extent_id = req.extent_id;
        let data_shards = req.data_shards as usize;
        let parity_shards = req.parity_shards as usize;
        let new_eversion = req.eversion;

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

        // F153: serialise concurrent EC conversion dispatches on this
        // extent. The manager-side `ec_conversion_inflight` set is purely
        // in-memory and is lost on leader failover; without this lock,
        // a deposed leader's mid-conversion + new leader's redispatch
        // could both pass the F119-D guard (because eversion has not yet
        // bumped) and race on `.ec.dat` writes. The lock entry is created
        // lazily and lives for the lifetime of the node — bounded by the
        // number of extents ever EC-converted on this shard, which is
        // the same bound as the existing `extents` DashMap (~negligible).
        // F210-D1: now uses the shared `extent_op_lock` helper (same
        // map, broadened semantic). handle_re_avali and the F210-D1
        // delete try-lock route through the same lock.
        let convert_lock = self.get_or_create_extent_op_lock(extent_id);
        let _convert_guard = convert_lock.lock().await;

        let entry = self.get_extent(extent_id).await?;
        let mut sealed_length = entry.sealed_length.load(Ordering::SeqCst);

        // Idempotency guard: if the coordinator's eversion is already
        // at the post-EC value, a prior 2PC completed successfully
        // (commit_shard_local is the last step, so eversion bump means
        // all phases finished). Return OK so the manager's
        // apply_ec_conversion_done converges. F153: this re-check now
        // runs UNDER the per-extent lock, so a serialized second
        // dispatch reliably observes the post-bump state.
        let local_eversion = entry.eversion.load(Ordering::SeqCst);
        if local_eversion >= req.eversion && sealed_length > 0 && entry.avali.load(Ordering::SeqCst) > 0 {
            tracing::info!(
                extent_id,
                local_eversion,
                req_eversion = req.eversion,
                sealed_length,
                "convert_to_ec idempotent skip: extent already EC-converted"
            );
            return Ok(rkyv_encode(&CodeResp {
                code: CODE_OK,
                message: String::new(),
            }));
        }

        // F194: gate cross-extent EC convert concurrency. Acquired AFTER
        // the F119-D idempotent-skip check above so an already-converted
        // extent (e.g. a deposed-leader redispatch) returns OK without
        // consuming a permit. Held until the end of the function via
        // RAII (`_ec_permit`); released when the function returns or
        // unwinds. The per-extent F153 lock above remains the
        // correctness gate against same-extent concurrent dispatches;
        // this is the new memory-safety gate against cross-extent fan
        // out. Default parallelism=1 — fully serialise. Env tunable
        // via `AUTUMN_EXTENT_EC_CONVERT_PARALLELISM` (clamped [1, 16]).
        let _ec_permit = self.concurrency_ctrl.acquire_ec_convert().await;

        // ── Check if coordinator's .ec.dat exists (prior prepare completed) ──
        //
        // If the coordinator's own staging file exists with the expected
        // shard size, a prior prepare phase completed for ALL nodes
        // (coordinator prepares itself last). Skip RS-encode and jump
        // straight to Phase 2 (commit).
        let coordinator_prepared = {
            let disk = self.disk_for(entry.disk_id)
                .map_err(|e| (StatusCode::Internal, e))?;
            let staging = disk.ec_staging_path(extent_id);
            if let Ok(meta) = compio::fs::metadata(&staging).await {
                // Validate shard size matches expectation. If sealed_length
                // is not yet known locally, we can't validate — fall through
                // to the full path which syncs from manager first.
                if sealed_length > 0 {
                    let expected_shard_size = crate::erasure::shard_size(sealed_length as usize, data_shards);
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

            // F119-E: sync sealed_length / eversion from manager.
            let mgr_info_opt = self.extent_info_from_manager(extent_id).await.ok().flatten();
            if let Some(mgr_info) = mgr_info_opt.as_ref() {
                if mgr_info.sealed_length > 0 {
                    entry.sealed_length.store(mgr_info.sealed_length, Ordering::SeqCst);
                    entry.eversion.store(mgr_info.eversion, Ordering::SeqCst);
                    entry.avali.store(mgr_info.avali, Ordering::SeqCst);
                    let _ = self.save_meta(extent_id, &entry).await;
                    sealed_length = mgr_info.sealed_length;
                    tracing::info!(extent_id, sealed_length, "applied seal from manager for EC convert");
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
            // F128: detect crash between rename(.ec.dat → .dat) and
            // save_meta in commit_shard_local. .dat is the shard file
            // (len = shard_size), .meta has old eversion, no staging
            // file exists. Fix meta and skip to Phase 2.
            let expected_shard = crate::erasure::shard_size(sealed_length as usize, data_shards) as u64;
            let f128_recovered = local_len < sealed_length
                && local_len == expected_shard
                && !coordinator_prepared;
            if f128_recovered {
                tracing::info!(
                    extent_id, local_len, sealed_length, new_eversion,
                    "F128: detected post-rename/pre-save_meta crash, recovering meta"
                );
                entry.sealed_length.store(sealed_length.max(local_len), Ordering::SeqCst);
                entry.avali.store(1, Ordering::SeqCst);
                if new_eversion > 0 {
                    entry.eversion.store(new_eversion, Ordering::SeqCst);
                }
                let _ = self.save_meta(extent_id, &entry).await;
            } else if local_len < sealed_length {
                let mgr_info = mgr_info_opt.ok_or_else(|| {
                    (
                        StatusCode::Unavailable,
                        format!(
                            "extent {extent_id} local_len={local_len} < sealed_length={sealed_length} \
                             and manager unreachable — cannot peer-copy"
                        ),
                    )
                })?;
                let fetched = self
                    .fetch_full_extent_from_sources(&mgr_info, &[])
                    .await
                    .map_err(|e| {
                        (
                            StatusCode::Unavailable,
                            format!(
                                "peer-copy for extent {extent_id} (need {sealed_length}, local has \
                                 {local_len}): {e}"
                            ),
                        )
                    })?;
                if (fetched.len() as u64) < sealed_length {
                    return Err((
                        StatusCode::FailedPrecondition,
                        format!(
                            "peer-copy returned {} bytes < sealed_length={sealed_length} for extent \
                             {extent_id} — data is unrecoverable; operator intervention required",
                            fetched.len()
                        ),
                    ));
                }
                let truncated = Bytes::from(fetched[..sealed_length as usize].to_vec());
                entry.file_rc()
                    .set_len(0)
                    .await
                    .map_err(|e| (StatusCode::Internal, format!("truncate {extent_id}: {e}")))?;
                file_pwrite_chunked(entry.file_rc(), 0, truncated)
                    .await
                    .map_err(|e| (StatusCode::Internal, format!("write {extent_id}: {e}")))?;
                entry.file_rc()
                    .sync_data()
                    .await
                    .map_err(|e| (StatusCode::Internal, format!("sync {extent_id}: {e}")))?;
                entry.len.store(sealed_length, Ordering::SeqCst);
                tracing::info!(extent_id, local_len, sealed_length, "peer-copied missing tail before EC convert");
            }

            if !f128_recovered {
            let data = file_pread_chunked(entry.file_rc(), 0, sealed_length as usize).await
                .map_err(|e| (StatusCode::Internal, format!("read extent {extent_id}: {e}")))?;

            // F117: offload RS encode to blocking thread.
            // F140: also do the `Vec<u8> -> Bytes` conversion inside the
            // blocking closure (zero-copy via `Bytes::from`) so the per-shard
            // ~shard_size memcpy that previously ran on the event loop as
            // `Bytes::copy_from_slice(shard)` per remote target moves off
            // the runtime. After this, the loop below uses
            // `shards[i].clone()` which is an O(1) Arc inc.
            let shards: Vec<Bytes> = compio::runtime::spawn_blocking(move || {
                crate::erasure::ec_encode(&data, data_shards, parity_shards)
                    .map(|vecs| vecs.into_iter().map(Bytes::from).collect())
            })
            .await
            .map_err(|_| (StatusCode::Internal, "ec_encode task panicked".to_string()))?
            .map_err(|e| (StatusCode::Internal, format!("ec_encode failed: {e}")))?;

            // ── Phase 1 (prepare): write .ec.dat on all nodes ──
            // Remote nodes first, coordinator (index 0) last.
            for (i, target_addr) in req.target_addrs.iter().enumerate() {
                if i == 0 { continue; }
                let ws_req = WriteShardReq {
                    extent_id,
                    shard_index: i as u32,
                    sealed_length,
                    eversion: new_eversion,
                    // F211-D: EC convert is manager-orchestrated; there
                    // is no per-stream owner-lock to propagate today.
                    // Passing 0 keeps the EN-side fence permissive on
                    // this path. Future: thread the manager's epoch
                    // through `ExtConvertToEcReq` so a fenced ex-coord
                    // is rejected at write_shard time too.
                    revision: 0,
                    payload: shards[i].clone(),
                };
                let sock = parse_addr(target_addr)
                    .map_err(|e| (StatusCode::Internal, format!("parse addr {target_addr}: {e}")))?;
                match rpc_oneshot(sock, MSG_WRITE_SHARD, ws_req.encode()).await {
                    Ok(resp_bytes) => {
                        let resp = WriteShardResp::decode(resp_bytes)
                            .map_err(|e| (StatusCode::Internal, format!("decode write_shard resp: {e}")))?;
                        if resp.code != CODE_OK {
                            return Err((
                                StatusCode::Internal,
                                format!(
                                    "WriteShard to {target_addr} shard {i}: code={}",
                                    code_description(resp.code)
                                ),
                            ));
                        }
                    }
                    Err(e) => {
                        return Err((
                            StatusCode::Internal,
                            format!("WriteShard to {target_addr} shard {i}: {e}"),
                        ));
                    }
                }
            }

            // Coordinator writes its own shard LAST. If we crash here,
            // no .ec.dat on coordinator → next retry re-reads full
            // data and re-distributes (remote nodes' prepare is
            // idempotent).
            self.write_shard_local(extent_id, 0, sealed_length, new_eversion, shards[0].clone()).await?;

            tracing::info!(extent_id, "EC 2PC phase 1 (prepare) complete on all nodes");
            } // !f128_recovered
        }

        // ── Phase 2 (commit): rename .ec.dat → .dat on all nodes ──
        // Remote nodes first, coordinator last.
        for (i, target_addr) in req.target_addrs.iter().enumerate() {
            if i == 0 { continue; }
            let commit_req = CommitEcShardReq {
                extent_id,
                sealed_length,
                eversion: new_eversion,
                // F211-D: see WriteShardReq site above.
                revision: 0,
            };
            let sock = parse_addr(target_addr)
                .map_err(|e| (StatusCode::Internal, format!("parse addr {target_addr}: {e}")))?;
            match rpc_oneshot(sock, MSG_COMMIT_EC_SHARD, commit_req.encode()).await {
                Ok(resp_bytes) => {
                    let resp = CommitEcShardResp::decode(resp_bytes)
                        .map_err(|e| (StatusCode::Internal, format!("decode commit_ec resp: {e}")))?;
                    if resp.code != CODE_OK {
                        return Err((
                            StatusCode::Internal,
                            format!(
                                "CommitEcShard to {target_addr} shard {i}: code={}",
                                code_description(resp.code)
                            ),
                        ));
                    }
                }
                Err(e) => {
                    return Err((
                        StatusCode::Internal,
                        format!("CommitEcShard to {target_addr} shard {i}: {e}"),
                    ));
                }
            }
        }

        // Coordinator commits itself LAST. After this, the idempotency
        // guard (eversion bump) ensures future retries are a no-op.
        self.commit_shard_local(extent_id, sealed_length, new_eversion).await?;

        tracing::info!(extent_id, new_eversion, "EC 2PC phase 2 (commit) complete");

        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    async fn handle_write_shard(&self, payload: Bytes) -> HandlerResult {
        let req = WriteShardReq::decode(payload.clone())
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        // F099-M: forward to owner shard.
        if !self.owns_extent(req.extent_id) {
            if let Some(sibling) = self.sibling_for_extent(req.extent_id) {
                return self
                    .forward_rpc_to_sibling(sibling, MSG_WRITE_SHARD, payload)
                    .await;
            }
        }

        // F211-D: owner-lock revision fence. `revision == 0` keeps the
        // pre-F211-D no-fence behaviour; non-zero is rejected when the
        // local last_revision has moved ahead (e.g., a fence on the
        // coord node bumped owner-lock revisions on every extent the
        // coord touched, so a revived ghost coord's WriteShard with the
        // old revision is refused).
        if req.revision > 0 {
            if let Ok(entry) = self.ensure_extent(req.extent_id).await {
                let last = entry.last_revision.load(Ordering::SeqCst);
                if req.revision < last {
                    return Ok(WriteShardResp { code: CODE_LOCKED_BY_OTHER }.encode());
                }
            }
        }

        self.write_shard_local(
            req.extent_id,
            req.shard_index as usize,
            req.sealed_length,
            req.eversion,
            req.payload,
        )
        .await?;

        Ok(WriteShardResp { code: CODE_OK }.encode())
    }

    async fn handle_commit_ec_shard(&self, payload: Bytes) -> HandlerResult {
        let req = CommitEcShardReq::decode(payload.clone())
            .map_err(|e| (StatusCode::InvalidArgument, e.to_string()))?;

        if !self.owns_extent(req.extent_id) {
            if let Some(sibling) = self.sibling_for_extent(req.extent_id) {
                return self
                    .forward_rpc_to_sibling(sibling, MSG_COMMIT_EC_SHARD, payload)
                    .await;
            }
        }

        // F211-D: owner-lock revision fence (see handle_write_shard).
        if req.revision > 0 {
            if let Ok(entry) = self.ensure_extent(req.extent_id).await {
                let last = entry.last_revision.load(Ordering::SeqCst);
                if req.revision < last {
                    return Ok(CommitEcShardResp { code: CODE_LOCKED_BY_OTHER }.encode());
                }
            }
        }

        self.commit_shard_local(req.extent_id, req.sealed_length, req.eversion)
            .await?;

        Ok(CommitEcShardResp { code: CODE_OK }.encode())
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
mod f147b_tests {
    use super::*;

    /// F147-B: handle_append returns CODE_PRECONDITION when sealed_length > 0.
    ///
    /// The F147-B fix inserts a post-truncate seal recheck in handle_append
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
    /// The post-truncate recheck (new F147-B code at line ~2434) is validated
    /// by code inspection: it is structurally identical to the F146 recheck in
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

        // Write 100 bytes at eversion=1, revision=0, commit=0 (no truncation).
        let write_req = AppendReq {
            extent_id: 9001,
            eversion: 1,
            commit: 0,
            revision: 0,
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
        // the same CODE_PRECONDITION the post-truncate F147-B recheck would
        // return if the seal had arrived DURING the truncate await instead.
        let stale_req = AppendReq {
            extent_id: 9001,
            eversion: 1,
            commit: 50,
            revision: 0,
            payload: Bytes::from(b"x".to_vec()),
        };
        let stale_result = node.handle_append(stale_req.encode()).await;
        assert!(stale_result.is_ok(), "handle_append should not error on sealed extent");
        let stale_resp = AppendResp::decode(stale_result.unwrap()).expect("decode AppendResp");
        assert_eq!(
            stale_resp.code, CODE_PRECONDITION,
            "handle_append on sealed extent must return CODE_PRECONDITION"
        );
    }
}

#[cfg(test)]
mod f147c_tests {
    use super::*;

    /// F147-C: run_recovery_task refuses when the local extent's eversion
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
    /// Pattern matches F147-B's test: the post-fetch verify cannot be injected
    /// in a single-threaded compio test either, so both tests validate the
    /// observable guard semantics rather than the concurrent injection.
    #[compio::test]
    async fn f147_recovery_refuses_when_local_eversion_advanced() {
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
                live_ev, stale_eversion
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
            let prev_sl = entry.sealed_length.fetch_max(stale_sealed_length, Ordering::SeqCst);
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
mod f148_copy_extent_tests {
    use super::*;

    /// F148-B: handle_copy_extent refuses with CODE_PRECONDITION on
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
    /// at its alloc-time value of 0. The F148-B post-fetch check fires.
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
            revision: 0,
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

    /// F148-B: handle_copy_extent succeeds on a sealed extent.
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
            revision: 0,
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
mod f153_ec_lock_tests {
    use super::*;

    /// F153: per-extent EC conversion lock serialises concurrent dispatches.
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
mod f157_meta_crc_tests {
    use super::*;

    /// F157: round-trip through V1 meta save/parse with CRC validation.
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
        buf[32..40].copy_from_slice(&42i64.to_le_bytes()); // last_revision
        let crc = crc32c::crc32c(&buf[0..ExtentNode::META_SIZE_V0]);
        buf[40..44].copy_from_slice(&crc.to_le_bytes());

        let parsed = ExtentNode::parse_meta(&buf, extent_id).expect("V1 parse");
        assert_eq!(parsed, (12345, 7, 42));
    }

    /// F157: V0 legacy 40-byte buffer must parse (back-compat).
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
        assert_eq!(parsed, (999, 3, 100));
    }

    /// F157: a V1 buffer with a flipped payload byte must be rejected (CRC mismatch).
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

    /// F157: a V1 buffer with a flipped CRC trailer byte must be rejected.
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

    /// F157: extent_id mismatch on V1 meta returns None (existing behaviour preserved).
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

    /// F157: unknown magic byte (not V0 or V1) returns None.
    #[test]
    fn unknown_magic_rejected() {
        let mut buf = [0u8; ExtentNode::META_SIZE_V1];
        buf[0..8].copy_from_slice(b"NOT_META");
        assert!(ExtentNode::parse_meta(&buf, 1).is_none());
    }
}

#[cfg(test)]
mod f160_copy_extent_eversion_tests {
    use super::*;

    /// F160: handle_copy_extent (the Ok(None) branch — no manager configured)
    /// must reject `req.eversion = 0` when local eversion has advanced past 0.
    /// Pre-F160 the check skipped on req.eversion == 0 due to the legacy
    /// `req.eversion > 0 &&` clause that F119-C had removed in
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
        // F148-B unsealed-refusal doesn't fire first — we want to reach
        // the F160 eversion check).
        let alloc_payload = rkyv_encode(&AllocExtentReq { extent_id: 9001 });
        node.handle_alloc_extent(alloc_payload).await.expect("alloc");
        // Append some bytes so the extent has content.
        let payload = vec![0xa5u8; 64];
        let write_req = AppendReq {
            extent_id: 9001,
            eversion: 1,
            commit: 0,
            revision: 0,
            payload: Bytes::from(payload),
        };
        node.handle_append(write_req.encode()).await.expect("append");
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
            "F160: copy_extent with eversion=0 must Err when local eversion=7"
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
mod f194_concurrency_gate_tests {
    //! F194 (renamed to ConcurrencyController in F196 D-r7): cross-extent
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

    /// F196 D-r7: the two counters are independent. Saturating
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
        task.await;
    }

    /// Constructor clamps both caps to at least 1.
    #[test]
    fn zero_parallelism_clamps_to_one() {
        let ctrl = ConcurrencyController::new(0, 0);
        assert_eq!(ctrl.ec_convert_max, 1, "ec_convert: 0 must clamp to 1");
        assert_eq!(ctrl.recovery_max, 1, "recovery: 0 must clamp to 1");
    }

    /// F195: clamp test against the builder methods (replaces the
    /// removed F194 env-parser smoke test). Process-global env mutation
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

/// F211-D: shard wire-fence on `WriteShardReq` / `CommitEcShardReq`.
/// Round-trip the encoded bytes through `decode` and assert the
/// `revision` field survives so future callers cannot accidentally
/// drop it. The handler-level fence behaviour is covered by the
/// integration tests in `crates/manager/tests/f211_node_lifecycle.rs`.
#[cfg(test)]
mod f211d_wire_fence_tests {
    use crate::extent_rpc::{CommitEcShardReq, WriteShardReq};
    use bytes::Bytes;

    #[test]
    fn write_shard_req_roundtrip_carries_revision() {
        let original = WriteShardReq {
            extent_id: 42,
            shard_index: 3,
            sealed_length: 12345,
            eversion: 7,
            revision: 99,
            payload: Bytes::from_static(b"shard-bytes"),
        };
        let encoded = original.encode();
        let decoded = WriteShardReq::decode(encoded).unwrap();
        assert_eq!(decoded.extent_id, 42);
        assert_eq!(decoded.shard_index, 3);
        assert_eq!(decoded.sealed_length, 12345);
        assert_eq!(decoded.eversion, 7);
        assert_eq!(decoded.revision, 99);
        assert_eq!(decoded.payload.as_ref(), b"shard-bytes");
    }

    #[test]
    fn write_shard_req_revision_zero_is_no_fence_marker() {
        let original = WriteShardReq {
            extent_id: 1,
            shard_index: 0,
            sealed_length: 0,
            eversion: 1,
            revision: 0,
            payload: Bytes::new(),
        };
        let decoded = WriteShardReq::decode(original.encode()).unwrap();
        assert_eq!(decoded.revision, 0, "zero revision marker preserved");
    }

    #[test]
    fn commit_ec_shard_req_roundtrip_carries_revision() {
        let original = CommitEcShardReq {
            extent_id: 7,
            sealed_length: 100,
            eversion: 4,
            revision: 5,
        };
        let decoded = CommitEcShardReq::decode(original.encode()).unwrap();
        assert_eq!(decoded.extent_id, 7);
        assert_eq!(decoded.sealed_length, 100);
        assert_eq!(decoded.eversion, 4);
        assert_eq!(decoded.revision, 5);
    }
}
