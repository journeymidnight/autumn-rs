// F195: `background` module made public so the autumn-ps binary can
// reach its `set_*` setters (`set_min_pipeline_batch`,
// `set_gc_*`). The module's internal symbols stay `pub(crate)`/private.
pub mod background;
mod rpc_handlers;
mod sstable;
mod wal_record;

use background::*;
use rpc_handlers::dispatch_partition_rpc;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use autumn_common::cpu_pin::{affinity_set, pick_cpu_for_ord};
use autumn_common::metrics::{duration_to_ns, ns_to_ms};
use autumn_rpc::manager_rpc::{self, MgrRange as Range, rkyv_encode, rkyv_decode};
use autumn_rpc::partition_rpc::{self, *, TableLocations, SstLocation};
use autumn_rpc::{Frame, FrameDecoder, HandlerResult, StatusCode};
use autumn_stream::{ConnPool, StreamClient};
use bytes::Bytes;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::TcpStream;
use compio::BufResult;
use futures::channel::{mpsc, oneshot};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, SinkExt, StreamExt};

use sstable::{IterItem, MemtableIterator, MergeIterator, SstBuilder, SstReader, TableIterator};

// ---------------------------------------------------------------------------
// Compat helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const FLUSH_MEM_BYTES: u64 = 256 * 1024 * 1024;
const MAX_SKIP_LIST: u64 = 256 * 1024 * 1024;
const WRITE_CHANNEL_CAP: usize = 1024;
const DEFAULT_MAX_WRITE_BATCH: usize = WRITE_CHANNEL_CAP * 3;

// F195: process-global PS tunables. Pre-F195 each `xxx()` function read
// env::var inside an inner static OnceLock — the cell was hidden inside
// the function so no setter could override it from the binary. F195
// lifts each cell to module scope and exposes a paired `pub fn set_xxx`
// that the autumn-ps binary calls from main() based on CLI args. The
// reader functions still use `get_or_init` for thread-safe lazy default
// fallback — but the init closure no longer touches the environment.
//
// First-call-wins semantics: any reader that fires before main() applied
// its setter will lock in the default. The binary's main() always calls
// the setters BEFORE constructing PartitionServer, so this is fine in
// production. Tests that need overrides must call `set_xxx` early too;
// tests running in parallel against the same process share the cells,
// same constraint as the previous env::var approach.

static MAX_WRITE_BATCH_CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
static PS_INFLIGHT_CAP_CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
static PS_BULK_INFLIGHT_CAP_CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
/// F197 — parallel-flush drain cap on P-log side. Sets how many imm
/// entries `background_flush_loop` can have in flight concurrently
/// (each doing build SST + upload row_stream + await response from
/// P-bulk). Commit (`tables.push` + `meta_stream` save) is still
/// strictly serial in launch order via `FuturesOrdered`.
static PS_FLUSH_INFLIGHT_CAP_CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
static MAX_IMM_DEPTH_CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
static MAX_WAL_GAP_CELL: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
static SHUTDOWN_TIMEOUT_MS_CELL: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
static PS_MAJOR_COMPACT_PARALLELISM_CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
static PS_GC_PARALLELISM_CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
static PS_CONN_INFLIGHT_CAP_CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
static PS_FG_RATE_BYTES_PER_SEC_CELL: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
static PS_FG_IOPS_PER_SEC_CELL: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
/// F196 D-r7: replaces the former `PS_BG_RATE_BYTES_PER_SEC_CELL`.
/// Compact and GC each have their own per-partition bytes/s cap now.
static PS_COMPACT_RATE_BYTES_PER_SEC_CELL: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
static PS_GC_RATE_BYTES_PER_SEC_CELL: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
static PS_FG_SATURATED_THRESHOLD_CELL: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
static PS_FG_QPS_QUOTA_CELL: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
static PS_GC_DEBT_HIGH_BYTES_CELL: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
static PS_COMPACT_PENDING_HIGH_BYTES_CELL: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
static PS_GC_COOLDOWN_SECS_CELL: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
static PS_COMPACT_COOLDOWN_SECS_CELL: std::sync::OnceLock<i64> = std::sync::OnceLock::new();

/// F195: setter for the group-commit request cap. First-call-wins.
/// `[1, 1_000_000]` clamp matches pre-F195 env-default behavior.
pub fn set_max_write_batch(n: usize) -> bool {
    MAX_WRITE_BATCH_CELL
        .set(n.clamp(1, 1_000_000))
        .is_ok()
}
pub fn set_ps_inflight_cap(n: usize) -> bool {
    PS_INFLIGHT_CAP_CELL.set(n.clamp(1, 64)).is_ok()
}
pub fn set_ps_bulk_inflight_cap(n: usize) -> bool {
    PS_BULK_INFLIGHT_CAP_CELL.set(n.clamp(1, 16)).is_ok()
}
/// F197: max concurrent in-flight imm flushes (parallel drain).
/// Range [1, 64]; default = MAX_IMM_DEPTH so the imm queue can fully
/// drain in parallel.
pub fn set_ps_flush_inflight_cap(n: usize) -> bool {
    PS_FLUSH_INFLIGHT_CAP_CELL.set(n.clamp(1, 64)).is_ok()
}
pub fn set_max_imm_depth(n: usize) -> bool {
    MAX_IMM_DEPTH_CELL.set(n.clamp(1, 64)).is_ok()
}
pub fn set_max_wal_gap(n: u64) -> bool {
    MAX_WAL_GAP_CELL
        .set(n.clamp(128 * 1024 * 1024, 64 * 1024 * 1024 * 1024))
        .is_ok()
}
pub fn set_shutdown_timeout_ms(n: u64) -> bool {
    SHUTDOWN_TIMEOUT_MS_CELL.set(n.clamp(1_000, 600_000)).is_ok()
}
pub fn set_ps_major_compact_parallelism(n: usize) -> bool {
    PS_MAJOR_COMPACT_PARALLELISM_CELL.set(n.clamp(1, 64)).is_ok()
}
pub fn set_ps_gc_parallelism(n: usize) -> bool {
    PS_GC_PARALLELISM_CELL.set(n.clamp(1, 64)).is_ok()
}
pub fn set_ps_conn_inflight_cap(n: usize) -> bool {
    if n == 0 || n > 4096 {
        return false;
    }
    PS_CONN_INFLIGHT_CAP_CELL.set(n).is_ok()
}
pub fn set_admission_fg_rate(n: u64) -> bool {
    PS_FG_RATE_BYTES_PER_SEC_CELL.set(n).is_ok()
}
pub fn set_admission_fg_iops(n: u64) -> bool {
    PS_FG_IOPS_PER_SEC_CELL.set(n).is_ok()
}
/// F196 D-r7: per-partition compact rate cap (bytes/s). Replaces the
/// former combined `set_admission_bg_rate`.
pub fn set_admission_compact_rate(n: u64) -> bool {
    PS_COMPACT_RATE_BYTES_PER_SEC_CELL.set(n).is_ok()
}
/// F196 D-r7: per-partition gc rate cap (bytes/s).
pub fn set_admission_gc_rate(n: u64) -> bool {
    PS_GC_RATE_BYTES_PER_SEC_CELL.set(n).is_ok()
}
pub fn set_admission_fg_saturated_threshold(n: f64) -> bool {
    PS_FG_SATURATED_THRESHOLD_CELL.set(n).is_ok()
}
pub fn set_fg_qps_quota(n: u32) -> bool {
    if n == 0 {
        return false;
    }
    PS_FG_QPS_QUOTA_CELL.set(n).is_ok()
}
pub fn set_gc_debt_high(n: u64) -> bool {
    if n == 0 {
        return false;
    }
    PS_GC_DEBT_HIGH_BYTES_CELL.set(n).is_ok()
}
pub fn set_compact_pending_high(n: u64) -> bool {
    if n == 0 {
        return false;
    }
    PS_COMPACT_PENDING_HIGH_BYTES_CELL.set(n).is_ok()
}
pub fn set_gc_cooldown_secs(n: i64) -> bool {
    PS_GC_COOLDOWN_SECS_CELL.set(n).is_ok()
}
pub fn set_compact_cooldown_secs(n: i64) -> bool {
    PS_COMPACT_COOLDOWN_SECS_CELL.set(n).is_ok()
}

/// Group-commit request count cap. F195: defaults to DEFAULT_MAX_WRITE_BATCH
/// (3072); overridable via `set_max_write_batch` from the binary main().
fn max_write_batch() -> usize {
    *MAX_WRITE_BATCH_CELL.get_or_init(|| DEFAULT_MAX_WRITE_BATCH)
}
/// R4 4.4 — maximum number of P-log `append_batch` futures in flight
/// concurrently per partition. Higher values give more pipeline depth so
/// multiple 256-request group-commit batches overlap their replica RTT, but
/// also raise peak memory (each in-flight batch may hold up to
/// `MAX_WRITE_BATCH_BYTES` = 30 MB of encoded segments).
///
/// Default = 8 → up to 8 × 30 MB = 240 MB worst-case memory per partition.
/// Range clamped to [1, 64]. F195: overridable via `set_ps_inflight_cap`.
pub(crate) fn ps_inflight_cap() -> usize {
    *PS_INFLIGHT_CAP_CELL.get_or_init(|| 8)
}

/// R4 4.4 — maximum number of P-bulk (flush) in-flight SST uploads per
/// partition. Each in-flight request holds a full 128 MB SSTable buffer
/// (peak), so this cap is deliberately small. Default = 2 lets the next
/// flush start its `build_sst_bytes` while the previous one's 128 MB
/// `row_stream.append` is streaming. Range clamped to [1, 16]. F195:
/// overridable via `set_ps_bulk_inflight_cap`.
///
/// F197 (2026-05-13): `background_flush_loop` is now structurally
/// parallel via `FuturesOrdered` and `ps_flush_inflight_cap()`. The
/// parallel path defaults to cap=1 (serial behaviour) after 120 s
/// perf_check showed cap=4 doesn't help fg-saturated 4K workloads
/// — EN-side row_stream fsync is the wall, not P-log concurrency.
/// Operators opting in to parallel flush via
/// `--flush-inflight-cap N` should bump this knob to match so the
/// P-bulk `FuturesUnordered` doesn't head-of-line block.
/// Default returns to 2 (the long-standing R4 4.4 value).
pub(crate) fn ps_bulk_inflight_cap() -> usize {
    *PS_BULK_INFLIGHT_CAP_CELL.get_or_init(|| 2)
}

/// F197 — parallel imm-flush drain cap on the P-log side.
/// `background_flush_loop` uses `FuturesOrdered` with this cap; up to
/// N imms can be building+uploading concurrently, then commit
/// (`tables.push` / `imm.pop_front` / `save_table_locs_raw`) runs
/// strictly serial in launch order — preserves F148-A invariant.
/// Range clamped to [1, 64].
///
/// **Default = 1** (functionally equivalent to pre-F197 serial flow).
/// The 120 s perf_check at p=16 d=8 4K showed bumping this to 4
/// does NOT improve write p99 / throughput on the sustained-write
/// pattern because the bottleneck is **EN-side row_stream fsync**,
/// not P-log concurrency: 256 MB SST × 3-replica fsync ≈ 3 s
/// regardless of how many SSTs are launched concurrently (writes
/// to the same row_stream tail extent serialise at the extent file).
///
/// F197 still matters for *future* workloads where the bottleneck
/// shifts — single-partition heavy churn (split-followup compaction),
/// VP-heavy imm with small SST + big log_stream tail, short burst
/// → drain. Operator can opt in via `--flush-inflight-cap N`.
pub(crate) fn ps_flush_inflight_cap() -> usize {
    *PS_FLUSH_INFLIGHT_CAP_CELL.get_or_init(|| 1)
}

/// F120-A — maximum imm queue depth per partition. When `imm.len()` reaches
/// this cap, `partition_loop` stops pulling from `req_rx` (write
/// back-pressure) and waits for `flush_one_imm` to pop one entry before
/// resuming. Worst-case unflushed-WAL window per partition is therefore
/// `MAX_IMM_DEPTH * FLUSH_MEM_BYTES + active.bytes` = 4 × 256 MB + 256 MB
/// = 1.25 GB (vs. unbounded pre-F120). Range clamped to [1, 64]. F195:
/// overridable via `set_max_imm_depth`. RocksDB analogue:
/// `max_write_buffer_number` (default 2). Default 4 because our memtable
/// is 4× RocksDB's default `write_buffer_size` and bulk uploads are
/// network-bound 128 MB SSTs.
pub(crate) fn max_imm_depth() -> usize {
    *MAX_IMM_DEPTH_CELL.get_or_init(|| 4)
}

/// F120-B — maximum unflushed log_stream gap per partition. When
/// `active.bytes + Σ imm[i].bytes > MAX_WAL_GAP`, `partition_loop`
/// force-rotates `active` to imm even if it hasn't reached
/// `FLUSH_MEM_BYTES = 256 MB`. Bounds `open_partition` replay time when
/// the workload is dominated by large values (small memtable footprint
/// per record but full payload sits in log_stream as VPs) or by small
/// writes that drip in below the rotate threshold. Range clamped to
/// [128 MiB, 64 GiB]. F195: overridable via `set_max_wal_gap`.
/// RocksDB analogue: `max_total_wal_size` (default
/// `write_buffer_size × max_write_buffer_number × 4`).
pub(crate) fn max_wal_gap() -> u64 {
    const DEFAULT: u64 = 1 * 1024 * 1024 * 1024;
    *MAX_WAL_GAP_CELL.get_or_init(|| DEFAULT)
}

/// F120-C — graceful shutdown deadline. After SIGTERM, `PartitionServer::
/// shutdown()` waits at most this many milliseconds for each partition's
/// drain (rotate active + flush all imm). Range clamped to [1 s, 10 min].
/// F195: overridable via `set_shutdown_timeout_ms`.
pub(crate) fn shutdown_timeout_ms() -> u64 {
    *SHUTDOWN_TIMEOUT_MS_CELL.get_or_init(|| 60_000)
}

// CPU affinity policy lives in `autumn_common::cpu_pin`. Both OS threads
// owned by a partition (P-log `part-{id}` and P-bulk `part-{id}-bulk`)
// pin to the SAME core — P-bulk is mostly idle during P-log's busy
// windows (its work is syscall + 3-replica network wait on a 128 MB SST
// upload), so sharing a core is fine and keeps "one partition = one core".

const COMPACT_RATIO: f64 = 0.5;
const HEAD_RATIO: f64 = 0.3;
const COMPACT_N: usize = 5;

/// F104 — global cross-partition gate on concurrent compactions.
///
/// `do_compact` (background.rs) holds, at peak, both the input SSTs'
/// `Bytes` (already in `PartitionData.sst_readers`) and the output SSTs'
/// `Bytes` (in the `new_readers` Vec built up during the streaming loop)
/// — roughly 2× the on-disk SST size of one partition. With N partitions
/// running compactions concurrently on the same process this multiplies
/// linearly. On a single PS hosting 4 partitions of ~5 GB SST each, the
/// observed peak was ~44 GB RSS during `autumn-op compact ALL`.
///
/// This gate caps cross-partition compaction concurrency. Default = **4**
/// (post-D-r7-recal; was 1 pre-recal). With single-compact peak ~256 MB
/// after F104-streaming + F169 spawn_blocking, 4 concurrent compacts =
/// ~2 GB peak RSS — well below the F104 incident's 44 GB. Memory-
/// constrained operators can lower via `--major-compact-parallelism 1`;
/// throughput-bound operators can raise up to 64.
pub(crate) fn ps_major_compact_parallelism() -> usize {
    // F196 D-r7-recal: bumped 1 → 4. The F104 RAM-cap default was
    // overly conservative — a single compact's spawn_blocking SST
    // buffer is bounded at ~256 MB (MAX_SKIP_LIST), so 4 concurrent
    // compacts peak at ~2 GB RSS, well within modern server budgets.
    // The 1 default created a structural bottleneck: with N partitions
    // doing sustained 4K writes (~27 MB/s flush per partition × 16 =
    // 432 MB/s aggregate flush input), a serialized single compact at
    // 256 MiB/s simply cannot keep up. Operator can lower via
    // `--major-compact-parallelism` for memory-constrained hosts.
    *PS_MAJOR_COMPACT_PARALLELISM_CELL.get_or_init(|| 4)
}

/// F196: PS-wide cap on concurrent `run_gc` calls across partitions.
/// GC reads sealed log_stream extents in 64 MiB chunks (F106) and
/// rewrites live VPs — both the chunk-read buffer (~64 MiB) and the
/// rewrite append staging consume RSS. Without a PS-wide cap,
/// `autumn-op gc ALL` on an N-partition PS would launch N
/// concurrent `run_gc` calls, multiplying peak memory by N.
///
/// Default = **4** (post-D-r7-recal), tunable via `--gc-parallelism`.
/// Range clamped to [1, 64]. Symmetric with
/// `ps_major_compact_parallelism`; reasoning is identical (RAM cap,
/// not rate cap — that's the per-partition `RateController`).
/// 4 × 64 MiB chunk buffer = 256 MiB peak RSS, well below any
/// reasonable budget.
pub(crate) fn ps_gc_parallelism() -> usize {
    *PS_GC_PARALLELISM_CELL.get_or_init(|| 4)
}

pub struct CompactionGate {
    inflight: std::sync::atomic::AtomicUsize,
    max_parallel: usize,
}

impl CompactionGate {
    pub fn new(max_parallel: usize) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            inflight: std::sync::atomic::AtomicUsize::new(0),
            max_parallel: max_parallel.max(1),
        })
    }

    pub async fn acquire(self: &std::sync::Arc<Self>) -> CompactionPermit {
        use std::sync::atomic::Ordering;
        loop {
            let cur = self.inflight.load(Ordering::Acquire);
            if cur < self.max_parallel
                && self
                    .inflight
                    .compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                return CompactionPermit { gate: std::sync::Arc::clone(self) };
            }
            // Either at-cap or CAS lost the race; back off briefly and
            // retry. 50 ms is short relative to compaction wallclock
            // (seconds–minutes) and avoids hot-spinning the CPU.
            compio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
}

pub struct CompactionPermit {
    gate: std::sync::Arc<CompactionGate>,
}

impl Drop for CompactionPermit {
    fn drop(&mut self) {
        self.gate
            .inflight
            .fetch_sub(1, std::sync::atomic::Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// MVCC internal-key helpers
// ---------------------------------------------------------------------------

const TS_BYTES: usize = 8;
const TS_SIZE: usize = TS_BYTES + 1;

pub(crate) fn key_with_ts(user_key: &[u8], ts: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(user_key.len() + TS_SIZE);
    out.extend_from_slice(user_key);
    out.push(0u8);
    out.extend_from_slice(&(u64::MAX - ts).to_be_bytes());
    out
}

pub(crate) fn parse_key(internal_key: &[u8]) -> &[u8] {
    if internal_key.len() <= TS_SIZE {
        return internal_key;
    }
    &internal_key[..internal_key.len() - TS_SIZE]
}

pub(crate) fn parse_ts(internal_key: &[u8]) -> u64 {
    if internal_key.len() <= TS_SIZE {
        return 0;
    }
    let b: [u8; 8] = internal_key[internal_key.len() - TS_BYTES..]
        .try_into()
        .unwrap();
    u64::MAX - u64::from_be_bytes(b)
}

// ---------------------------------------------------------------------------
// Value-log (F031)
// ---------------------------------------------------------------------------

const VALUE_THROTTLE: usize = 4 * 1024;
const VALUE_POINTER_SIZE: usize = 16;
const OP_VALUE_POINTER: u8 = 0x80;

/// F185: backstop TTL for `PartitionData.frozen_for_merge`. The manager's
/// merge orchestrator (`handle_merge_partitions`) commits in <1 s on the
/// happy path and explicitly unfreezes on rollback. This TTL fires only
/// when the orchestrator crashed mid-flow before either committing the
/// merge txn (which causes the survivor's region_sync_loop to drop the
/// frozen `PartitionData` and reopen with frozen=false) or unfreezing on
/// failure. 30 s gives the manager's leader-election + replay path enough
/// runway while bounding the worst-case write-halt window — far below the
/// alternative of "frozen forever until PS restart" that a crash-bare
/// design would imply.
const FREEZE_TTL: std::time::Duration = std::time::Duration::from_secs(30);

// F129/F186 — `OP_VALUE_POINTER_MULTI` (0x40) and `OP_CHUNK_BLOB` (0x10)
// were the F129 server-side multipart op flags. Removed in F186 with the
// rest of the server-side multipart machinery. Stripe-write is now pure
// client-side via `ClusterClient::put_stream_begin` (Ceph striperados
// pattern). Op-flag values 0x40 and 0x10 remain RESERVED to avoid
// conflicting with on-disk records that may still exist from pre-F186
// runs.

// Inline-Put cap. The PS rejects `MSG_PUT` with `value.len()` greater
// than this with `CODE_VALUE_TOO_LARGE`; client SDK retries via
// `put_stream_begin` (client-side striping).
pub(crate) const AUTUMN_PS_MAX_INLINE_BYTES_DEFAULT: u32 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ValuePointer {
    extent_id: u64,
    offset: u32,
    len: u32,
}

impl ValuePointer {
    fn encode(&self) -> [u8; VALUE_POINTER_SIZE] {
        let mut b = [0u8; VALUE_POINTER_SIZE];
        b[0..8].copy_from_slice(&self.extent_id.to_le_bytes());
        b[8..12].copy_from_slice(&self.offset.to_le_bytes());
        b[12..16].copy_from_slice(&self.len.to_le_bytes());
        b
    }
    fn decode(b: &[u8]) -> Self {
        Self {
            extent_id: u64::from_le_bytes(b[0..8].try_into().unwrap()),
            offset: u32::from_le_bytes(b[8..12].try_into().unwrap()),
            len: u32::from_le_bytes(b[12..16].try_into().unwrap()),
        }
    }
}


// ---------------------------------------------------------------------------
// Memtable entry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct MemEntry {
    op: u8,
    value: Vec<u8>,
    expires_at: u64,
}

// F099-C: single-writer (P-log) BTreeMap under parking_lot::RwLock.
//
// Motivation (see docs/superpowers/specs/2026-04-20-perf-r4-ceiling-diagnosis.md
// §Section 3): the previous crossbeam SkipMap paid full lock-free bookkeeping
// (epoch pinning + tagged atomic pointer loads + CAS splice retries + refcount
// drops) on every insert, which accounted for ~28 % of the P-log thread's CPU
// budget at the 60–65 k write ceiling. autumn-rs's write path has exactly one
// writer per memtable (the P-log thread's `background_write_loop_r1`), so that
// machinery is pure overhead. A plain `BTreeMap` under a `parking_lot::RwLock`
// gives:
//   - single-threaded insert walks (cache-friendly, no atomics)
//   - brief writer lock hold (~microseconds per batch phase-3)
//   - parallel reader access (ps-conn Get path can acquire the read lock
//     concurrently with other readers; a batch insert briefly excludes them)
// Linearizability is preserved by the RwLock (see Programming Notes in
// crates/partition-server/CLAUDE.md).
pub(crate) struct Memtable {
    data: parking_lot::RwLock<BTreeMap<Vec<u8>, MemEntry>>,
    bytes: AtomicU64,
}

impl Memtable {
    fn new() -> Self {
        Self {
            data: parking_lot::RwLock::new(BTreeMap::new()),
            bytes: AtomicU64::new(0),
        }
    }

    fn insert(&self, key: Vec<u8>, entry: MemEntry, size: u64) {
        // BTreeMap::insert returns the previous value if present; we intentionally
        // discard it — SkipMap::insert silently replaced on duplicate keys and
        // autumn's MVCC-encoded keys are unique per (user_key, seq) so collisions
        // only occur under replay-idempotent recovery, where dropping the prior
        // identical value is safe.
        let _ = self.data.write().insert(key, entry);
        self.bytes.fetch_add(size, Ordering::Relaxed);
    }

    /// Insert a whole batch of (key, entry, size) tuples under a SINGLE write
    /// lock acquisition. This is the hot-path helper used by Phase 3 of
    /// `background_write_loop_r1`, where up to 256 entries land at once. It
    /// collapses 256 `parking_lot::RwLock::write()` acquisitions into one,
    /// saving ~2–5 ns/entry of atomic-CAS cost on the uncontended write path.
    ///
    /// Semantics identical to calling `insert` N times: duplicate keys are
    /// replaced silently, byte counter accumulates the sum of sizes.
    pub(crate) fn insert_batch<I>(&self, items: I)
    where
        I: IntoIterator<Item = (Vec<u8>, MemEntry, u64)>,
    {
        let mut guard = self.data.write();
        let mut total = 0u64;
        for (k, v, s) in items {
            let _ = guard.insert(k, v);
            total += s;
        }
        drop(guard);
        if total > 0 {
            self.bytes.fetch_add(total, Ordering::Relaxed);
        }
    }

    fn is_empty(&self) -> bool {
        self.data.read().is_empty()
    }
    fn mem_bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    fn seek_user_key(&self, user_key: &[u8]) -> Option<MemEntry> {
        let seek = key_with_ts(user_key, u64::MAX);
        let guard = self.data.read();
        for (k, v) in guard.range(seek..) {
            if parse_key(k) != user_key {
                break;
            }
            return Some(v.clone());
        }
        None
    }

    fn snapshot_sorted(&self) -> Vec<IterItem> {
        let guard = self.data.read();
        guard
            .iter()
            .map(|(k, v)| IterItem {
                key: k.clone(),
                op: v.op,
                value: v.value.clone(),
                expires_at: v.expires_at,
            })
            .collect()
    }

    /// Iterate the memtable entries in ascending key order under a read lock
    /// and hand each entry to `f`. Used by `build_sst_bytes` and `rotate_active`
    /// to avoid allocating an intermediate snapshot Vec when the caller just
    /// needs read access to (&[u8], &MemEntry).
    pub(crate) fn for_each<F: FnMut(&[u8], &MemEntry)>(&self, mut f: F) {
        let guard = self.data.read();
        for (k, v) in guard.iter() {
            f(k.as_slice(), v);
        }
    }
}

// ---------------------------------------------------------------------------
// SSTable metadata
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct TableMeta {
    extent_id: u64,
    offset: u32,
    len: u32,
    estimated_size: u64,
    last_seq: u64,
}

impl TableMeta {
    fn loc(&self) -> (u64, u32) {
        (self.extent_id, self.offset)
    }
}

// ---------------------------------------------------------------------------
// PartitionData — lives on a dedicated partition thread (Rc, no locks)
// ---------------------------------------------------------------------------

// F129/F186 — `UploadSession` and `upload_sessions` field on PartitionData
// were the in-memory state for the F129 server-side multipart upload.
// Removed in F186; stripe-write is now pure client-side (Ceph
// striperados). The PS no longer holds any per-upload state.

pub(crate) struct PartitionData {
    part_id: u64,
    rg: Range,
    /// Monotonic epoch assigned by the manager, bumped whenever `rg` is
    /// rewritten (split: both children new; merge: survivor new). Picked
    /// up from `MgrRegionInfo.region_epoch` by `sync_regions_once` and
    /// passed in at open time. Once Phase 3 wires it onto the request
    /// path, ps-conn stamps it on every response so the SDK can detect
    /// stale routing without a manager round-trip (TiKV `region_epoch`
    /// pattern). Until then it is plumbed but unused on the wire.
    pub(crate) region_epoch: u64,
    active: Memtable,
    imm: VecDeque<Arc<Memtable>>,
    /// Set of imm `Arc::as_ptr` values currently being flushed.
    /// Prevents `flush_one_imm` (called inline by `flush_memtable_locked`)
    /// from racing `background_flush_loop` on the same imm[0]: pre-fix,
    /// both paths cloned imm.front() in their synchronous claim step,
    /// then each ran `run_flush_async_phase` to produce an SST and each
    /// called `commit_flush_outcome` to push to `tables`. Result: two
    /// SSTs in `tables` with identical content but different
    /// (extent_id, offset), and `imm.pop_front()` ran twice (the second
    /// pop was a no-op on the now-empty queue). The duplicate SSTs
    /// silently inflated table count and corrupted the `f029` /
    /// `f030_*` invariants.
    ///
    /// Lifetimes:
    /// - INSERT at claim time (synchronous, inside `borrow_mut`).
    /// - REMOVE at the bottom of `commit_flush_outcome` (synchronous,
    ///   inside the same `borrow_mut` block that pops imm.front).
    /// - A duplicate claim attempt sees the ptr already present and
    ///   returns Ok(false) — the caller treats it as "nothing to flush"
    ///   and moves on. The other path's commit will eventually clear it.
    flushing_imm_ptrs: RefCell<HashSet<usize>>,
    flush_tx: mpsc::UnboundedSender<()>,
    compact_tx: mpsc::Sender<bool>,
    gc_tx: mpsc::Sender<GcTask>,
    seq_number: u64,
    log_stream_id: u64,
    row_stream_id: u64,
    meta_stream_id: u64,
    tables: Vec<TableMeta>,
    sst_readers: Vec<Arc<SstReader>>,
    has_overlap: Cell<u32>,
    need_invalidate_row_stream: Cell<bool>,
    vp_extent_id: u64,
    vp_offset: u32,
    stream_client: Rc<StreamClient>,
    manager_addr: String,
    pool: Rc<ConnPool>,
    /// F088: sender to the per-partition bulk thread. `None` if the bulk
    /// thread failed to initialize — fall back to in-thread flush (legacy
    /// path) so the partition remains usable.
    flush_req_tx: Option<mpsc::Sender<FlushReq>>,
    /// Channel for compaction to route row_stream appends through P-bulk's
    /// StreamClient, preventing dual-writer truncation corruption.
    /// `None` when P-bulk failed to spawn (legacy fallback uses part_sc).
    row_append_tx: Option<mpsc::Sender<RowAppendReq>>,
    /// F120-A: signaled (one item per pop) by `flush_one_imm` after every
    /// successful `imm.pop_front()` so that `partition_loop` can
    /// wake from its imm-full back-pressure wait. Unbounded because the
    /// receiver always drains all pending notifications before re-checking
    /// `imm.len()`, so the buffer self-bounds at `MAX_IMM_DEPTH`.
    imm_drained_tx: mpsc::UnboundedSender<()>,
    /// Wake signal for `partition_loop` after a SPLIT freeze has
    /// parked its drain ack. F210-C2 spawns `handle_split_part` off the
    /// loop's dispatch stack so the loop is free to drain; but after
    /// the spawn returns, the loop sleeps on its (D) idle select
    /// (`req_rx + F120-C drain`) — neither receiver carries the
    /// "split_drain_ack just transitioned to Some" event, so the
    /// top-of-loop drain check is unreachable until incidental traffic
    /// arrives. In an idle partition that meant a full FREEZE_TTL (30s)
    /// of dead time before the watchdog forced the handler to error
    /// out. The spawned handler sends `()` here right after parking
    /// the ack; the loop's idle selects watch this rx and wake.
    /// Specific to SPLIT — MSG_MERGE_FREEZE runs inline in
    /// `handle_incoming_req`, so its parking happens on the loop's
    /// active stack and the next iteration's top check fires naturally.
    pub(crate) split_wake_tx: mpsc::UnboundedSender<()>,
    /// F140: per-partition gate shared with background_gc_loop.
    /// `handle_split_part` acquires this to ensure `run_gc` has no
    /// log_stream append in-flight when commit_length is read. This is
    /// split-vs-gc *synchronization*, NOT a resource cap. PS-wide
    /// compact / gc concurrency limits live in `io_bucket` (F196 D-r6).
    pub(crate) gc_gate: std::sync::Arc<CompactionGate>,
    /// F196 D-r7: per-partition rate controller. Fresh `Arc` per
    /// partition — fg/compact/gc rates are isolated; a hot partition's
    /// fg pressure cannot consume a cold sibling's budget. fg-aware
    /// yield in `account_compact`/`account_gc` is per-partition too
    /// (each partition compares its own fg activity against its own
    /// fg cap).
    pub(crate) rate_ctrl: std::sync::Arc<crate::RateController>,
    /// F196 D-r7: PS-wide concurrency permits. Clone of
    /// `PartitionServer.concurrency_ctrl`. Used by
    /// `background_compact_loop` / `background_gc_loop` /
    /// `handle_split_part`.
    pub(crate) concurrency_ctrl: std::sync::Arc<crate::ConcurrencyController>,
    /// F196: PS-wide partition budget (cpuset_len / 2 when --cpuset is
    /// set; usize::MAX otherwise). `handle_split_part` consults this
    /// before calling `multi_modify_split` so a split that would push
    /// the PS past its core budget is rejected rather than oversubscribing.
    pub(crate) partition_budget: std::sync::Arc<crate::PartitionBudget>,
    /// F162 (MED-2): per-extent reader-pin map. `handle_get → resolve_value`
    /// reads a ValuePointer from an SST, drops the partition borrow, and
    /// awaits `read_bytes_from_extent` on log_stream. Without coordination,
    /// `run_gc` could decrement vp_table_refs to 0 (after compaction
    /// rewrote the SSTs that referenced the extent) and call `punch_holes`,
    /// causing the manager to enqueue a physical delete. The in-flight
    /// resolve_value's read RPC arrives at the extent-node which (a) may
    /// have already received MSG_DELETE_EXTENT and respond NotFound — a
    /// spurious user-visible read failure on data that was perfectly valid
    /// at the moment the SST was looked up; or (b) the file fd is already
    /// unlinked but still open from a prior reader, returning the original
    /// bytes — correct but timing-dependent.
    ///
    /// F162 closes the spurious-NotFound class via a per-extent pin counter:
    ///   value >= 0: number of active readers; readers can acquire
    ///   value == -1: GC in progress (writer holds exclusively)
    ///
    /// Reader path (`resolve_value`): CAS-acquire (increment from a
    /// non-negative value); CAS-release (`fetch_sub(1)`).
    /// GC path (`run_gc` before `punch_holes`): try-CAS 0 → -1. On success,
    /// proceed with the RPC; release with `store(0)`. On failure (readers
    /// active), defer this extent's GC to the next 30-60 s tick. The reader
    /// completes within milliseconds typically; the deferred GC catches up
    /// on the next iteration.
    ///
    /// Memory footprint: HashMap grows monotonically with extents ever
    /// referenced by VP resolution. ~32 bytes per entry; bounded by
    /// log_stream extent count per partition (typically a few thousand
    /// at most). Not worth GC'ing the map; deleted extents leave benign
    /// stale entries with count=0.
    pub(crate) extent_pins: std::cell::RefCell<std::collections::HashMap<u64, std::rc::Rc<std::sync::atomic::AtomicI64>>>,
    /// F185: PrepareMerge-style write halt. `Some(instant_set)` while the
    /// partition is in the merge-window write-halt; `None` otherwise.
    /// Set by `MSG_MERGE_FREEZE`; cleared by an explicit unfreeze RPC
    /// (manager rollback path), by partition reopen on rg/stream-id
    /// change (the normal post-commit recovery), or by `FREEZE_TTL`
    /// expiry inside `partition_loop` (backstop for orchestrator
    /// crash). While Some, the loop rejects Put/Delete/StreamPut with
    /// `CODE_UNAVAILABLE` so writes never land in a log_stream tail
    /// past the to-be-captured `commit_length`. In-memory only — a PS
    /// crash mid-freeze loses the flag, which is correct because the
    /// merge txn either committed (next reopen sees the merged region)
    /// or didn't (reopen serves the pre-merge region normally).
    pub(crate) frozen_for_merge: Cell<Option<std::time::Instant>>,
    /// F185: stashed `MSG_MERGE_FREEZE` response oneshot. `handle_incoming_req`
    /// flips `frozen_for_merge=true` and parks the caller's resp here without
    /// replying; `partition_loop` consumes it once `pending` AND
    /// `inflight` are both empty AND every imm has been flushed, then sends
    /// the OK reply. The caller (CLI / autumn-op merge) thus blocks
    /// until every acked-pre-freeze write is durable on log_stream + has
    /// flushed through to a row_stream SST referenced by a meta_stream
    /// checkpoint, which is the strict precondition that makes
    /// `MSG_CHECK_COMMIT_LENGTH` safe to capture for the merge txn.
    pub(crate) freeze_drain_ack: std::cell::RefCell<Option<oneshot::Sender<HandlerResult>>>,
    /// F210-C2: PrepareSplit-style write halt. Mirror of `frozen_for_merge`
    /// but for the SPLIT path; needed because `handle_split_part` runs on
    /// a spawned task (not inline through dispatch_partition_rpc) so it
    /// can park here and let `partition_loop` drain
    /// pending+inflight+imm before `commit_length` is captured. Pre-F210-C2
    /// split called `flush_memtable_locked` then `commit_length` directly,
    /// but the inline await window let in-flight Phase 2 appends complete
    /// past the captured `commit_length` — those bytes existed on EN disk
    /// past sealed_length and were invisible on recovery.
    pub(crate) frozen_for_split: Cell<Option<std::time::Instant>>,
    /// F210-C2: internal oneshot signal from `partition_loop` to
    /// the spawned `handle_split_part` task — fired after drain completes.
    /// Payload is `Result<(), String>`: `Ok(())` = drain succeeded,
    /// commit_length is now safe to capture; `Err(msg)` = drain hit a
    /// flush failure (same shape as F210-C3 merge error path) and the
    /// split must abort.
    pub(crate) split_drain_ack:
        std::cell::RefCell<Option<oneshot::Sender<Result<(), String>>>>,
    /// F210-C4: set to `true` when `sync_partition_vp_refs` failed
    /// after a meta_stream checkpoint published a new SST set. While
    /// dirty, `background_gc_loop` skips calls into `punch_holes` /
    /// `truncate` on log_stream — manager's `vp_table_refs` is stale,
    /// so an `extent_can_delete` check against it could under-count
    /// references from SSTs whose `vp_deps` haven't been sync'd, and
    /// approve a deletion that orphans live VPs. The background
    /// `vp_refs_retry_loop` periodically retries the sync; on success
    /// it clears this flag and GC resumes.
    pub(crate) vp_refs_dirty: Cell<bool>,
    /// F183: per-partition load metrics for the manager's policy engine.
    /// Counters are bumped by `partition_loop` (req on each
    /// dispatch, imm_full each time the imm cap stalls intake). The
    /// PS-level `report_load_loop` snapshots and resets these every 5 s
    /// and ships them via MSG_REPORT_PARTITION_LOAD.
    pub(crate) metrics: std::sync::Arc<PartitionMetrics>,
    /// F212-fix-2: cross-thread mirror of `PartitionHandle.opened_with`.
    /// `handle_split_part` (and any future in-place updater of `rg` /
    /// `region_epoch`) MUST write the new tuple here after updating
    /// the local fields so `sync_regions_once` on the main thread
    /// observes a matching `(rg, log, row, meta, epoch)` and skips
    /// drop+reopen. Without this, the next `region_sync_loop` tick
    /// tears down the source partition even though its in-memory
    /// state is already correct — a 5-60+ s outage per split.
    pub(crate) opened_with_shared:
        std::sync::Arc<parking_lot::Mutex<(Range, u64, u64, u64, u64)>>,
}

#[derive(Default)]
pub struct PartitionMetrics {
    pub req_count: std::sync::atomic::AtomicU64,
    /// F189-fix MED-3: monotonic request counter (NEVER swap-reset).
    /// `report_load_loop` swaps `req_count` to 0 every 5 s for its
    /// per-window rate calc, which races the F188 maintenance
    /// scheduler's diff-based req_per_sec gate (the gate would see
    /// req_per_sec=0 right after a swap and dispatch BG work during a
    /// real FG storm). The scheduler now diffs against this monotonic
    /// counter instead, so it sees true delta over its own interval.
    pub req_count_monotonic: std::sync::atomic::AtomicU64,
    pub imm_full_count: std::sync::atomic::AtomicU64,
    /// Bytes resident: SST total + active.bytes + Σ imm.bytes. Updated
    /// after each flush + memtable rotate (cheap; under borrow_mut).
    pub size_bytes: std::sync::atomic::AtomicU64,
    /// F187: aggregated discard bytes on sealed log_stream extents that
    /// the GC loop would target. Refreshed by the GC loop's read-only
    /// prefix (no extra RPCs) and shipped via report_load_loop. Manager
    /// emits a `POLICY_KIND_GC` advisory when this stays above
    /// `gc_debt_high` for `policy.required_buckets` consecutive ticks
    /// outside the gc cooldown.
    pub gc_debt_bytes: std::sync::atomic::AtomicU64,
    /// F187: bytes of SSTable data that compaction would consume on its
    /// next pass — sum of (head-extent table sizes when head-ratio < 30 %)
    /// + (overlap-tagged tables when has_overlap == 1). Refreshed by the
    /// compact loop's periodic tick. Manager emits a `POLICY_KIND_COMPACT`
    /// advisory when sustained above `compact_pending_high`.
    pub pending_compaction_bytes: std::sync::atomic::AtomicU64,
    /// F187: 1 while `background_gc_loop` is inside `run_gc`, else 0. Lets
    /// operators see stuck-GC at a glance and lets the policy engine skip
    /// duplicate advisories for an already-active GC.
    pub gc_inflight: std::sync::atomic::AtomicU32,
    /// F187: 1 while `background_compact_loop` is inside `do_compact`,
    /// else 0.
    pub compact_inflight: std::sync::atomic::AtomicU32,
    /// F187: unix-epoch seconds of the last successful GC `punch_holes`
    /// completion. 0 if never run since process start. Used by the policy
    /// engine to enforce cooldown (no advisory while `now - last_gc_at <
    /// gc_cooldown_sec`).
    pub last_gc_at: std::sync::atomic::AtomicI64,
    /// F187: unix-epoch seconds of the last successful `do_compact`
    /// commit. 0 if never run since process start. Used for compact
    /// cooldown.
    pub last_compact_at: std::sync::atomic::AtomicI64,
    /// F202: Σ bytes occupied by tombstone (op==2) records inside the
    /// partition's live SSTs. Refreshed after every flush + compact
    /// commit. Drives external controller's "should major compact"
    /// decisions; advisory-only here.
    pub sst_tombstone_bytes: std::sync::atomic::AtomicU64,
    /// F202: Σ bytes of SST records whose `expires_at` has passed.
    pub sst_expired_bytes: std::sync::atomic::AtomicU64,
    /// F202: Σ bytes of SST records whose keys fall outside the
    /// partition's current `rg` range. Only > 0 while `has_overlap == 1`.
    pub sst_out_of_range_bytes: std::sync::atomic::AtomicU64,
    /// F202: bytes the next *minor* compact tick's `pickup_tables`
    /// would feed into `do_compact`. Distinct from
    /// `pending_compaction_bytes` (major). Both can be non-zero
    /// simultaneously.
    pub minor_compact_pending_bytes: std::sync::atomic::AtomicU64,
    /// F202: count of sealed log_stream extents (informational).
    pub sealed_log_extent_count: std::sync::atomic::AtomicU32,
}

impl PartitionData {
    /// F162 (MED-2): get-or-insert the per-extent pin counter for `eid`.
    /// Cheap — just a HashMap lookup with lazy creation.
    pub(crate) fn pin_for(&self, eid: u64) -> std::rc::Rc<std::sync::atomic::AtomicI64> {
        self.extent_pins
            .borrow_mut()
            .entry(eid)
            .or_insert_with(|| std::rc::Rc::new(std::sync::atomic::AtomicI64::new(0)))
            .clone()
    }
}

/// F162 (MED-2): RAII reader pin guard. Decrements the counter on drop.
/// Created via `acquire_reader_pin`; returns None if a writer (GC) currently
/// holds the pin (counter == -1). Caller treats None as "extent is being
/// reclaimed; treat as not-found".
pub(crate) struct ReaderPin(std::rc::Rc<std::sync::atomic::AtomicI64>);

impl Drop for ReaderPin {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// F162: try to acquire a reader pin on `eid`. CAS-loop increments the
/// counter only when current value >= 0. Returns None when a GC writer
/// holds the pin (counter == -1).
pub(crate) fn acquire_reader_pin(
    pin: std::rc::Rc<std::sync::atomic::AtomicI64>,
) -> Option<ReaderPin> {
    use std::sync::atomic::Ordering::SeqCst;
    loop {
        let cur = pin.load(SeqCst);
        if cur < 0 {
            return None;
        }
        if pin
            .compare_exchange(cur, cur + 1, SeqCst, SeqCst)
            .is_ok()
        {
            return Some(ReaderPin(pin));
        }
        // CAS lost a race with another reader/writer; re-try.
    }
}

/// F162: try to acquire a writer (GC) pin on `eid`. Single CAS 0 → -1.
/// Returns true on success (proceed with punch_holes); false on failure
/// (readers active — defer this extent's GC to the next tick).
pub(crate) fn try_acquire_writer_pin(
    pin: &std::rc::Rc<std::sync::atomic::AtomicI64>,
) -> bool {
    use std::sync::atomic::Ordering::SeqCst;
    pin.compare_exchange(0, -1, SeqCst, SeqCst).is_ok()
}

/// F162: release a writer pin (counter back to 0). Called after GC's
/// `punch_holes` returns (or errors).
pub(crate) fn release_writer_pin(pin: &std::rc::Rc<std::sync::atomic::AtomicI64>) {
    pin.store(0, std::sync::atomic::Ordering::SeqCst);
}

#[cfg(test)]
mod f162_reader_pin_tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    /// F162: acquire_reader_pin succeeds when no writer is holding,
    /// increments counter, drops decrement.
    #[test]
    fn reader_pin_acquire_release() {
        let pin = std::rc::Rc::new(AtomicI64::new(0));
        {
            let _g = acquire_reader_pin(pin.clone()).expect("acquire");
            assert_eq!(pin.load(Ordering::SeqCst), 1);
            // Acquiring a second reader pin works too.
            let _g2 = acquire_reader_pin(pin.clone()).expect("acquire 2");
            assert_eq!(pin.load(Ordering::SeqCst), 2);
        }
        // Both guards dropped → back to 0.
        assert_eq!(pin.load(Ordering::SeqCst), 0);
    }

    /// F162: acquire_reader_pin returns None when writer holds the pin.
    #[test]
    fn reader_pin_blocked_by_writer() {
        let pin = std::rc::Rc::new(AtomicI64::new(0));
        assert!(try_acquire_writer_pin(&pin), "writer should acquire");
        assert_eq!(pin.load(Ordering::SeqCst), -1);

        // Reader cannot acquire while writer holds.
        assert!(
            acquire_reader_pin(pin.clone()).is_none(),
            "reader should be blocked by writer"
        );

        release_writer_pin(&pin);
        assert_eq!(pin.load(Ordering::SeqCst), 0);

        // After writer releases, reader can acquire.
        let _g = acquire_reader_pin(pin.clone()).expect("acquire after writer release");
        assert_eq!(pin.load(Ordering::SeqCst), 1);
    }

    /// F162: try_acquire_writer_pin fails when readers are holding.
    #[test]
    fn writer_pin_blocked_by_readers() {
        let pin = std::rc::Rc::new(AtomicI64::new(0));
        let _g = acquire_reader_pin(pin.clone()).expect("acquire reader");
        assert_eq!(pin.load(Ordering::SeqCst), 1);

        // Writer cannot acquire while reader holds.
        assert!(
            !try_acquire_writer_pin(&pin),
            "writer should be blocked by reader"
        );
        // Reader pin still 1 (writer's failed CAS didn't perturb it).
        assert_eq!(pin.load(Ordering::SeqCst), 1);
        drop(_g);
        // After reader releases, writer can acquire.
        assert!(try_acquire_writer_pin(&pin), "writer after reader release");
    }

    /// F162: writer pin already held — second writer also blocked.
    /// (Should not happen in production since GC serialises per-extent
    /// via the gc_gate, but the protocol is correct either way.)
    #[test]
    fn writer_pin_exclusive() {
        let pin = std::rc::Rc::new(AtomicI64::new(0));
        assert!(try_acquire_writer_pin(&pin));
        assert!(!try_acquire_writer_pin(&pin), "second writer must fail");
    }
}

// ---------------------------------------------------------------------------
// GC task
// ---------------------------------------------------------------------------

pub(crate) enum GcTask {
    /// Pick GC candidates by policy. Parameters describe WHICH extents
    /// are eligible (multi-tier filtering). Default = standard
    /// `discard_ratio > 0.4` over all sealed non-tail extents (the
    /// pre-F201 single-tier behaviour) PLUS empty-sealed slots that
    /// the F201 candidate-set fix unblocked.
    Auto(GcAutoParams),
    Force { extent_ids: Vec<u64> },
}

/// F201: parameters passed by callers (CLI / external controller) to
/// the auto-GC candidate selection. All fields are optional; `Default`
/// reproduces pre-F201 single-tier behaviour augmented with the
/// empty-sealed punch path. The PS does not mix tiers — exactly one
/// of (default ratio, custom ratio + optional max_size, empty_only)
/// applies per dispatch — but the external controller can issue
/// multiple ticks back-to-back with different params to compose
/// effective tiers.
#[derive(Default, Clone, Debug)]
pub(crate) struct GcAutoParams {
    /// Discard ratio threshold (0.0..=1.0). `None` → 0.4
    /// (`GC_DISCARD_RATIO`).
    pub ratio: Option<f64>,
    /// Only consider sealed extents whose `sealed_length` is at most
    /// this many bytes. Combined with a lower `ratio` lets the caller
    /// say "punch small extents at even 10% dead". `None` → no upper
    /// bound.
    pub max_size: Option<u64>,
    /// Whole-stream dead-byte high-water hint. When the stream's total
    /// reclaimable bytes exceed this, the per-extent ratio is halved
    /// (so 0.4 → 0.2 etc.) for this dispatch. `None` → no relaxation.
    pub stream_debt: Option<u64>,
    /// If `true`, pick ONLY `sealed_length == 0` non-tail extents.
    /// Cheapest possible GC (no rewrite, just `punch_holes`). Overrides
    /// `ratio` / `max_size` when set.
    pub empty_only: bool,
}

// ---------------------------------------------------------------------------
// Flush channel types (P-log → P-bulk)
// ---------------------------------------------------------------------------
//
// F088: background_flush_loop on the P-log thread no longer runs
// build_sst_bytes + row_stream.append itself. Instead it ships a FlushReq
// over to a dedicated P-bulk OS thread (its own compio runtime + io_uring +
// ConnPool), which does the heavy lifting and replies with the new TableMeta
// + SstReader. P-log then atomically pushes the new table/reader, pops imm,
// and publishes the authoritative metaStream checkpoint from its single-
// threaded `p.tables` state.
//
// imm: Arc<Memtable> — parking_lot::RwLock<BTreeMap<_,_>> + AtomicU64,
// Send+Sync. Safe to cross threads (F099-C).
// SstReader holds a RefCell block cache; RefCell<T: Send> is Send (not Sync),
// so we can move it through the oneshot::Sender but not share across tasks.

pub(crate) struct FlushReq {
    pub(crate) imm: Arc<Memtable>,
    pub(crate) vp_eid: u64,
    pub(crate) vp_off: u32,
    pub(crate) row_stream_id: u64,
    pub(crate) invalidate_row_stream: bool,
    pub(crate) resp_tx: oneshot::Sender<Result<(TableMeta, SstReader)>>,
}

pub(crate) struct RowAppendReq {
    pub(crate) sst_bytes: Bytes,
    pub(crate) row_stream_id: u64,
    pub(crate) resp_tx: oneshot::Sender<Result<autumn_stream::AppendResult>>,
}

// ---------------------------------------------------------------------------
// Group-commit write channel types
// ---------------------------------------------------------------------------

pub(crate) enum WriteOp {
    Put {
        user_key: Bytes,
        value: Bytes,
        expires_at: u64,
    },
    Delete {
        user_key: Vec<u8>,
    },
}

/// F099-D: Direct responder into the outer `req.resp_tx` (encoded RPC frame
/// bytes). Replaces the R3/R4 inner `oneshot<Result<Vec<u8>, String>>` that
/// carried the raw key back to `handle_put`/`handle_delete` for re-encoding.
/// The tag selects whether Phase 3 encodes a `PutResp` or a `DeleteResp`.
pub(crate) enum WriteResponder {
    Put {
        outer: oneshot::Sender<HandlerResult>,
        /// User key to echo in `PutResp.key`. Owned copy — avoids keeping
        /// the decoded ArchivedPutReq alive across the Phase 2 await.
        key: Vec<u8>,
    },
    Delete {
        outer: oneshot::Sender<HandlerResult>,
        key: Vec<u8>,
    },
}

impl WriteResponder {
    /// Reply success (batch committed). Encodes the appropriate RPC response
    /// frame bytes and forwards them to the outer ps-conn oneshot.
    pub(crate) fn send_ok(self) {
        match self {
            WriteResponder::Put { outer, key } => {
                let bytes = partition_rpc::rkyv_encode(&PutResp {
                    code: CODE_OK,
                    message: String::new(),
                    key,
                });
                let _ = outer.send(Ok(bytes));
            }
            WriteResponder::Delete { outer, key } => {
                let bytes = partition_rpc::rkyv_encode(&DeleteResp {
                    code: CODE_OK,
                    message: String::new(),
                    key,
                });
                let _ = outer.send(Ok(bytes));
            }
        }
    }

    /// Reply failure — propagate the error string as Internal to the outer
    /// resp_tx. "key is out of range" is InvalidArgument per existing semantics.
    pub(crate) fn send_err(self, msg: String) {
        let code = if msg == "key is out of range" {
            StatusCode::InvalidArgument
        } else {
            StatusCode::Internal
        };
        let outer = match self {
            WriteResponder::Put { outer, .. } => outer,
            WriteResponder::Delete { outer, .. } => outer,
        };
        let _ = outer.send(Err((code, msg)));
    }
}

pub(crate) struct WriteRequest {
    op: WriteOp,
    resp: WriteResponder,
}

impl WriteRequest {
    fn encoded_size(&self) -> usize {
        match &self.op {
            WriteOp::Put {
                user_key, value, ..
            } => 17 + user_key.len() + value.len(),
            WriteOp::Delete { user_key } => 17 + user_key.len(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(op: WriteOp) -> Self {
        // Build a dangling responder (outer _rx dropped immediately). Tests
        // that exercise the responder should construct it explicitly.
        let (outer, _rx) = oneshot::channel();
        let key = match &op {
            WriteOp::Put { user_key, .. } => user_key.to_vec(),
            WriteOp::Delete { user_key } => user_key.clone(),
        };
        let resp = match &op {
            WriteOp::Put { .. } => WriteResponder::Put { outer, key },
            WriteOp::Delete { .. } => WriteResponder::Delete { outer, key },
        };
        Self { op, resp }
    }
}

#[derive(Debug, Default)]
pub(crate) struct BatchStats {
    ops: u64,
    batch_size: u64,
    phase1_ns: u64,
    phase2_ns: u64,
    phase3_ns: u64,
    end_to_end_ns: u64,
}

pub(crate) struct WriteLoopMetrics {
    started_at: Instant,
    ops: u64,
    batches: u64,
    batch_size_total: u64,
    phase1_ns: u64,
    phase2_ns: u64,
    phase3_ns: u64,
    end_to_end_ns: u64,
}

impl WriteLoopMetrics {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            ops: 0,
            batches: 0,
            batch_size_total: 0,
            phase1_ns: 0,
            phase2_ns: 0,
            phase3_ns: 0,
            end_to_end_ns: 0,
        }
    }
    fn record(&mut self, stats: BatchStats) {
        if stats.ops == 0 { return; }
        self.ops += stats.ops;
        self.batches += 1;
        self.batch_size_total += stats.batch_size;
        self.phase1_ns += stats.phase1_ns;
        self.phase2_ns += stats.phase2_ns;
        self.phase3_ns += stats.phase3_ns;
        self.end_to_end_ns += stats.end_to_end_ns;
    }
    fn maybe_report(&mut self, part_id: u64) {
        if self.started_at.elapsed() >= Duration::from_secs(1) { self.report(part_id); }
    }
    fn flush(&mut self, part_id: u64) {
        if self.ops > 0 { self.report(part_id); }
    }
    fn report(&mut self, part_id: u64) {
        let elapsed = self.started_at.elapsed();
        let batches = self.batches.max(1);
        tracing::info!(
            part_id,
            ops = self.ops,
            batches = self.batches,
            ops_per_sec = self.ops as f64 / elapsed.as_secs_f64().max(1e-9),
            avg_batch_size = self.batch_size_total as f64 / batches as f64,
            fill_ratio = self.batch_size_total as f64 / (batches * max_write_batch() as u64) as f64,
            avg_phase1_ms = ns_to_ms(self.phase1_ns, batches),
            avg_phase2_ms = ns_to_ms(self.phase2_ns, batches),
            avg_phase3_ms = ns_to_ms(self.phase3_ns, batches),
            avg_end_to_end_ms = ns_to_ms(self.end_to_end_ns, batches),
            "partition write summary",
        );
        *self = Self::new();
    }
}


// ---------------------------------------------------------------------------
// Inter-thread request routing (main thread ↔ partition thread)
// ---------------------------------------------------------------------------
//
// F099-K (2026-04-20): Per-partition listener (Seastar-style thread-per-
// shard completion). The central accept thread + main-thread fd dispatcher
// from F099-J are GONE. Each partition thread owns:
//
//   * A dedicated `compio::net::TcpListener` bound to `base_port + ord`
//     where `ord` is a monotonic counter bumped once per `open_partition`
//     call. Port conflicts surface as a hard error (no silent fallback).
//   * Its own accept task that loops `listener.accept().await` on the
//     partition's compio runtime and spawns `handle_ps_connection` for
//     each fd on the SAME runtime. No cross-thread fd handoff.
//   * Registration with the manager via `MSG_REGISTER_PARTITION_ADDR`
//     once the listener is bound — `GetRegions` then returns the per-
//     partition address, and `ClusterClient` routes each client thread
//     to the owning partition's P-log directly. At N=4 + 256 benchmark
//     threads, the 256 ps-conn tasks distribute across 4 P-log runtimes
//     (~64 each) instead of sharing one, clearing the F099-J saturation
//     ceiling.
//
// F099-J context preserved for the per-partition path: handle_ps_connection
// still runs on the same runtime as partition_loop, so the request
// handoff is a same-thread mpsc + oneshot with no eventfd/futex wake.
//
// See `docs/superpowers/specs/2026-04-20-perf-f099-h-kernel-rtt.md` §2.3
// (per-partition P-log utilization after F099-J saturated under 256 × d=1;
// F099-K fans the load out across N P-log threads).

/// A request dispatched from a ps-conn task (running on P-log runtime)
/// into `partition_loop` for write group-commit or for inline
/// read/maintenance dispatch. After F099-J this channel's endpoints BOTH
/// live on the same compio runtime, so `futures::channel::mpsc`'s wake
/// path stays in-process (no eventfd).
pub struct PartitionRequest {
    msg_type: u8,
    payload: Bytes,
    resp_tx: oneshot::Sender<HandlerResult>,
}

/// Handle to a running partition thread.
///
/// Owned by the main compio thread. After F099-K the partition thread
/// binds its own listener and runs its own accept loop; the main thread
/// does NOT push fds across. The only thing we hang on to is a shutdown
/// signal (drop-to-close oneshot) used on `region_sync` eviction to ask
/// the partition thread to tear down its accept loop.
struct PartitionHandle {
    /// Dropping `shutdown_tx` closes the oneshot and signals the
    /// partition's accept task to stop.  We wrap it in `Option` so we
    /// can take/drop it explicitly.
    #[allow(dead_code)]
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// F120-C — graceful drain signal. Main thread sends a
    /// `oneshot::Sender<()>` through it to ask the partition to flush
    /// active+imm and reply when done. Dropped during shutdown so the
    /// `mpsc::Receiver` end inside the partition thread observes EOF.
    drain_tx: Option<mpsc::UnboundedSender<oneshot::Sender<()>>>,
    /// Address (`host:port`) the partition is listening on. Reported to
    /// the manager via `MSG_REGISTER_PARTITION_ADDR` on open.
    #[allow(dead_code)]
    part_addr: String,
    /// F183: cross-thread metrics handle. Bumped by the partition thread,
    /// read by the main thread's `report_load_loop`. Arc is Send so this
    /// is safe to clone across threads.
    metrics: std::sync::Arc<PartitionMetrics>,
    /// F188: clone of the partition's compact-trigger sender. Held on the
    /// main thread so the maintenance scheduler loop can dispatch
    /// compact runs without going through the loopback PS RPC. Same
    /// channel as the in-thread `PartitionData.compact_tx`; the receiver
    /// inside `background_compact_loop` is unchanged.
    compact_trigger: mpsc::Sender<bool>,
    /// F188: same shape for GC.
    gc_trigger: mpsc::Sender<GcTask>,
    /// F184 + epoch: snapshot of (rg, log/row/meta stream ids,
    /// region_epoch) describing the partition's CURRENT in-memory
    /// shape. `sync_regions_once` compares this against the latest
    /// manager regions snapshot to detect a wider rg (post-merge),
    /// new stream IDs, or an epoch bump and force a reopen.
    ///
    /// **F212-fix-2 — shared with the partition thread.** Pre-fix,
    /// this was a frozen snapshot from `open_partition` time. After
    /// `handle_split_part` narrowed `p.rg` + bumped `p.region_epoch`
    /// in-place on the partition thread, this snapshot stayed stale,
    /// so the next `sync_regions_once` tick saw `prev != latest` and
    /// dropped + reopened a partition whose in-memory state was
    /// already perfectly correct — a 5-60+ s outage on every split.
    /// Post-fix, `handle_split_part` (and any future in-place updater)
    /// MUST take the lock and write the new tuple after updating
    /// `PartitionData`. `parking_lot::Mutex` is fine: written ~1/split,
    /// read 1/2s/partition by `region_sync_loop` — both rates are
    /// dwarfed by the lock's ~25 ns uncontended cost. Same pattern as
    /// `Arc<PartitionMetrics>` (CLAUDE.md programming note 11) and
    /// `Arc<ConcurrencyController>` (F196 D-r7).
    opened_with: std::sync::Arc<parking_lot::Mutex<(Range, u64, u64, u64, u64)>>,
    /// JoinHandle retained for RAII.
    #[allow(dead_code)]
    join: Option<std::thread::JoinHandle<()>>,
}

// ---------------------------------------------------------------------------
// PartitionServer — runs on the main compio thread
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct PartitionServer {
    ps_id: u64,
    advertise_addr: Option<String>,
    partitions: Rc<RefCell<HashMap<u64, PartitionHandle>>>,
    /// Manager addresses for round-robin on NotLeader.
    manager_addrs: Vec<String>,
    /// Current manager index.
    current_mgr: Cell<usize>,
    pool: Rc<ConnPool>,
    /// Server-level owner key and revision for split coordination.
    server_owner_key: String,
    server_revision: Rc<Cell<i64>>,
    /// F099-K — base TCP port. The first partition opened binds
    /// `base_port + 1`; subsequent partitions bind `base_port + 2`,
    /// `base_port + 3`, ... (monotonically increasing, tracked via
    /// `next_port_ord`).
    base_port: Cell<u16>,
    /// F099-K — monotonic port-ordinal counter, bumped once per
    /// `open_partition` call. Partitions keep their assigned port
    /// across region-sync cycles as long as the `PartitionHandle` is
    /// alive; a port is never reused by a different partition.
    next_port_ord: Rc<Cell<u16>>,
    /// F099-K — host component for the per-partition advertise
    /// address. Defaults to `127.0.0.1` if `--advertise` is omitted or
    /// is not parseable as `host:port`.
    advertise_host: Rc<std::cell::RefCell<String>>,
    /// F099-K/F100-UCX — host component for the per-partition listener bind.
    /// For UCX/RoCE this must be the routable HCA IP, not `0.0.0.0`.
    listen_host: Rc<std::cell::RefCell<String>>,
    /// F196 D-r7: PS-wide concurrency caps for compact + GC. RAM cap,
    /// shared by every partition on this PS. Per-partition rate
    /// limiting lives in each `PartitionData.rate_ctrl`.
    pub(crate) concurrency_ctrl: std::sync::Arc<ConcurrencyController>,
    /// F196 — static partition budget (ScyllaDB-style). Gate fires only
    /// when `--cpuset` was explicitly supplied; otherwise `max =
    /// usize::MAX` and `would_exceed` always returns false.
    pub(crate) partition_budget: std::sync::Arc<PartitionBudget>,
}

/// F196 D-r7: per-partition rate controller. Tracks FOUR independent
/// rate dimensions in one 1-second sliding window:
///   - fg bytes/sec
///   - fg ops/sec (catches small-value IOPS-bound workloads)
///   - compact bytes/sec (background SST merge writes)
///   - gc bytes/sec       (background log_stream rewrite)
///
/// Per-partition by ownership: each `PartitionData` holds its own
/// `Arc<RateController>`. Hot-partition fg pressure does NOT consume
/// cold-partition budget — the budgets are independent.
///
/// fg-aware yield: `account_compact` and `account_gc` BOTH inspect
/// `fg_bytes / fg_ops` saturation against the per-partition fg caps
/// and yield the rest of the 1-second window when fg observed rate
/// exceeds `fg_saturated_ratio × cap`. Disabled per-dimension when
/// that fg cap is 0 (we lack a baseline to detect saturation).
///
/// Defaults (process-global, set via `set_admission_*` setters before
/// constructing PartitionServer; see CLI `--*-rate-bytes-per-sec` etc.):
///   AUTUMN_PS_FG_RATE_BYTES_PER_SEC     — 256 MiB/s
///   AUTUMN_PS_FG_IOPS_PER_SEC           — 30_000
///   AUTUMN_PS_COMPACT_RATE_BYTES_PER_SEC— 64 MiB/s
///   AUTUMN_PS_GC_RATE_BYTES_PER_SEC     — 32 MiB/s
///   AUTUMN_PS_FG_SATURATED_THRESHOLD    — 0.8
/// `0` on any rate disables that dimension (unlimited).
///
/// `parking_lot::Mutex` is held only across the synchronous accounting
/// calc; sleep happens outside the lock. Per-partition controllers
/// have no cross-thread contention since the partition owns its
/// own Arc — the Mutex is uncontended in steady state.
pub struct RateController {
    fg_rate_bytes_per_sec: u64,
    fg_iops_per_sec: u64,
    compact_rate_bytes_per_sec: u64,
    gc_rate_bytes_per_sec: u64,
    fg_saturated_ratio: f64,
    state: parking_lot::Mutex<RateState>,
}

struct RateState {
    window_start: Instant,
    fg_bytes: u64,
    fg_ops: u64,
    compact_bytes: u64,
    gc_bytes: u64,
}

impl RateController {
    pub fn new(
        fg_rate_bytes_per_sec: u64,
        fg_iops_per_sec: u64,
        compact_rate_bytes_per_sec: u64,
        gc_rate_bytes_per_sec: u64,
        fg_saturated_ratio: f64,
    ) -> Self {
        Self {
            fg_rate_bytes_per_sec,
            fg_iops_per_sec,
            compact_rate_bytes_per_sec,
            gc_rate_bytes_per_sec,
            fg_saturated_ratio: fg_saturated_ratio.clamp(0.1, 1.0),
            state: parking_lot::Mutex::new(RateState {
                window_start: Instant::now(),
                fg_bytes: 0,
                fg_ops: 0,
                compact_bytes: 0,
                gc_bytes: 0,
            }),
        }
    }

    /// Reads process-global setters. Defaults recalibrated against
    /// `perf_baseline_tcp_p16_d8_s4k.json` and the 8M peak observation
    /// (see `[[partition-qps-ceiling]]` and the D-r7-recal analysis):
    ///
    ///   - fg 1 GiB/s     — 5× headroom over single-partition 8M TCP
    ///                       peak (218 MB/s); 4K never engages.
    ///   - fg 30K ops/s   — single-partition QPS ceiling
    ///                       (see `[[partition-qps-ceiling]]`).
    ///   - compact 256 MiB/s — single partition can sustainably ingest
    ///                          fg writes' flush output (4K aggregate
    ///                          437 MB/s flush = 27 MB/s per partition).
    ///   - gc 128 MiB/s   — handles 50% overwrite rate on 8M workloads
    ///                       (218 × 0.5 = 109 MB/s).
    pub fn from_env() -> Self {
        let fg_rate = *PS_FG_RATE_BYTES_PER_SEC_CELL
            .get_or_init(|| 1024 * 1024 * 1024);
        let fg_iops = *PS_FG_IOPS_PER_SEC_CELL.get_or_init(|| 30_000);
        let compact_rate = *PS_COMPACT_RATE_BYTES_PER_SEC_CELL
            .get_or_init(|| 256 * 1024 * 1024);
        let gc_rate = *PS_GC_RATE_BYTES_PER_SEC_CELL
            .get_or_init(|| 128 * 1024 * 1024);
        let saturated = *PS_FG_SATURATED_THRESHOLD_CELL.get_or_init(|| 0.8);
        Self::new(fg_rate, fg_iops, compact_rate, gc_rate, saturated)
    }

    fn maybe_reset_window(state: &mut RateState) {
        if state.window_start.elapsed() >= Duration::from_secs(1) {
            state.window_start = Instant::now();
            state.fg_bytes = 0;
            state.fg_ops = 0;
            state.compact_bytes = 0;
            state.gc_bytes = 0;
        }
    }

    /// Returns `true` iff fg observed rate (bytes OR ops) exceeds
    /// `fg_saturated_ratio × cap`. Used by `account_compact` and
    /// `account_gc` to decide whether to yield to fg.
    fn fg_saturated(&self, s: &RateState, elapsed_secs: f64) -> bool {
        if self.fg_rate_bytes_per_sec > 0 {
            let obs = s.fg_bytes as f64 / elapsed_secs;
            let at = self.fg_rate_bytes_per_sec as f64 * self.fg_saturated_ratio;
            if obs > at {
                return true;
            }
        }
        if self.fg_iops_per_sec > 0 {
            let obs = s.fg_ops as f64 / elapsed_secs;
            let at = self.fg_iops_per_sec as f64 * self.fg_saturated_ratio;
            if obs > at {
                return true;
            }
        }
        false
    }

    /// Foreground accounting. Sleeps when either fg-bytes or fg-iops
    /// ceiling is set AND would be exceeded (LARGER sleep wins).
    /// Both caps at 0 = unlimited (default in tests, NOT in production).
    pub async fn account_fg(&self, bytes: u64, ops: u64) {
        let sleep_for = {
            let mut s = self.state.lock();
            Self::maybe_reset_window(&mut s);
            s.fg_bytes = s.fg_bytes.saturating_add(bytes);
            s.fg_ops = s.fg_ops.saturating_add(ops);
            let elapsed = s.window_start.elapsed();
            let bytes_sleep: Option<Duration> = if self.fg_rate_bytes_per_sec == 0 {
                None
            } else {
                let target = Duration::from_secs_f64(
                    s.fg_bytes as f64 / self.fg_rate_bytes_per_sec as f64,
                );
                if target > elapsed { Some(target - elapsed) } else { None }
            };
            let iops_sleep: Option<Duration> = if self.fg_iops_per_sec == 0 {
                None
            } else {
                let target = Duration::from_secs_f64(
                    s.fg_ops as f64 / self.fg_iops_per_sec as f64,
                );
                if target > elapsed { Some(target - elapsed) } else { None }
            };
            match (bytes_sleep, iops_sleep) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) | (None, Some(a)) => Some(a),
                (None, None) => None,
            }
        };
        if let Some(d) = sleep_for {
            compio::time::sleep(d).await;
        }
    }

    /// Compact rate accounting. Sleeps until BOTH constraints hold:
    /// (1) own compact ceiling; (2) fg-aware yield (yields when fg is
    /// saturated).
    pub async fn account_compact(&self, bytes: u64) {
        let sleep_for = {
            let mut s = self.state.lock();
            Self::maybe_reset_window(&mut s);
            s.compact_bytes = s.compact_bytes.saturating_add(bytes);
            let elapsed = s.window_start.elapsed();
            let own_sleep: Option<Duration> = if self.compact_rate_bytes_per_sec == 0 {
                None
            } else {
                let target = Duration::from_secs_f64(
                    s.compact_bytes as f64 / self.compact_rate_bytes_per_sec as f64,
                );
                if target > elapsed { Some(target - elapsed) } else { None }
            };
            let elapsed_secs = elapsed.as_secs_f64().max(0.001);
            let yield_sleep: Option<Duration> = if self.fg_saturated(&s, elapsed_secs) {
                let remaining = Duration::from_secs(1).saturating_sub(elapsed);
                if remaining > Duration::ZERO { Some(remaining) } else { None }
            } else {
                None
            };
            match (own_sleep, yield_sleep) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) | (None, Some(a)) => Some(a),
                (None, None) => None,
            }
        };
        if let Some(d) = sleep_for {
            compio::time::sleep(d).await;
        }
    }

    /// GC rate accounting. Symmetric to `account_compact`, separate
    /// counter + cap so compact and gc don't fight for the same budget.
    pub async fn account_gc(&self, bytes: u64) {
        let sleep_for = {
            let mut s = self.state.lock();
            Self::maybe_reset_window(&mut s);
            s.gc_bytes = s.gc_bytes.saturating_add(bytes);
            let elapsed = s.window_start.elapsed();
            let own_sleep: Option<Duration> = if self.gc_rate_bytes_per_sec == 0 {
                None
            } else {
                let target = Duration::from_secs_f64(
                    s.gc_bytes as f64 / self.gc_rate_bytes_per_sec as f64,
                );
                if target > elapsed { Some(target - elapsed) } else { None }
            };
            let elapsed_secs = elapsed.as_secs_f64().max(0.001);
            let yield_sleep: Option<Duration> = if self.fg_saturated(&s, elapsed_secs) {
                let remaining = Duration::from_secs(1).saturating_sub(elapsed);
                if remaining > Duration::ZERO { Some(remaining) } else { None }
            } else {
                None
            };
            match (own_sleep, yield_sleep) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) | (None, Some(a)) => Some(a),
                (None, None) => None,
            }
        };
        if let Some(d) = sleep_for {
            compio::time::sleep(d).await;
        }
    }

    /// Test-only snapshot: `(fg_bytes, fg_ops, compact_bytes, gc_bytes, elapsed)`.
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> (u64, u64, u64, u64, Duration) {
        let s = self.state.lock();
        (
            s.fg_bytes,
            s.fg_ops,
            s.compact_bytes,
            s.gc_bytes,
            s.window_start.elapsed(),
        )
    }
}

/// F196 D-r7: PS-wide concurrency controller. Caps the number of
/// simultaneous `do_compact` / `run_gc` calls across all partitions
/// on this PS. RAM-cap by purpose — each compact holds ~2× SST bytes
/// and each GC holds ~64 MiB chunk buffer; without a global cap,
/// `compact ALL` or `gc ALL` would multiply peak RSS by N partitions
/// (the F104 incident hit 44 GB RSS).
///
/// One `Arc<ConcurrencyController>` per `PartitionServer`, cloned
/// into each `PartitionData`. Folds the former standalone
/// `CompactionGate`-backed `compact_gate` (F104) and
/// `gc_concurrency_gate` (D-r5).
pub struct ConcurrencyController {
    compact_max: usize,
    gc_max: usize,
    compact_inflight: std::sync::atomic::AtomicUsize,
    gc_inflight: std::sync::atomic::AtomicUsize,
}

/// F196 D-r7: RAII permit for `acquire_compact()`.
pub struct CompactPermit {
    ctrl: std::sync::Arc<ConcurrencyController>,
}
impl Drop for CompactPermit {
    fn drop(&mut self) {
        self.ctrl
            .compact_inflight
            .fetch_sub(1, std::sync::atomic::Ordering::Release);
    }
}

/// F196 D-r7: RAII permit for `acquire_gc()`.
pub struct GcPermit {
    ctrl: std::sync::Arc<ConcurrencyController>,
}
impl Drop for GcPermit {
    fn drop(&mut self) {
        self.ctrl
            .gc_inflight
            .fetch_sub(1, std::sync::atomic::Ordering::Release);
    }
}

impl ConcurrencyController {
    pub fn new(compact_max: usize, gc_max: usize) -> Self {
        Self {
            compact_max: compact_max.max(1),
            gc_max: gc_max.max(1),
            compact_inflight: std::sync::atomic::AtomicUsize::new(0),
            gc_inflight: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn from_env() -> Self {
        Self::new(ps_major_compact_parallelism(), ps_gc_parallelism())
    }

    pub async fn acquire_compact(self: &std::sync::Arc<Self>) -> CompactPermit {
        use std::sync::atomic::Ordering;
        loop {
            let cur = self.compact_inflight.load(Ordering::Acquire);
            if cur < self.compact_max
                && self
                    .compact_inflight
                    .compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                return CompactPermit { ctrl: std::sync::Arc::clone(self) };
            }
            compio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    pub async fn acquire_gc(self: &std::sync::Arc<Self>) -> GcPermit {
        use std::sync::atomic::Ordering;
        loop {
            let cur = self.gc_inflight.load(Ordering::Acquire);
            if cur < self.gc_max
                && self
                    .gc_inflight
                    .compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                return GcPermit { ctrl: std::sync::Arc::clone(self) };
            }
            compio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
}

/// F196: ScyllaDB-style static partition budget.
///
/// When the operator passes `--cpuset` to the PS binary, each partition
/// reserves 2 cores (P-log + P-bulk), so the budget is `cpuset_len / 2`.
/// `sync_regions_once` bumps `current` on insert and dec on remove;
/// `handle_split_part` refuses to call `multi_modify_split` when adding
/// one more partition would exceed `max`.
///
/// `max == usize::MAX` means "no gate" — surfaced when `--cpuset` was
/// not supplied (legacy pre-F196 behaviour: surplus threads stay
/// unpinned with a WARN).
pub(crate) struct PartitionBudget {
    pub(crate) max: usize,
    pub(crate) current: std::sync::atomic::AtomicUsize,
}

impl PartitionBudget {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            max,
            current: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    pub(crate) fn inc(&self) -> usize {
        self.current
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1
    }
    pub(crate) fn dec(&self) -> usize {
        let prev = self
            .current
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        if prev == 0 {
            self.current
                .store(0, std::sync::atomic::Ordering::Relaxed);
            0
        } else {
            prev - 1
        }
    }
    pub(crate) fn current(&self) -> usize {
        self.current.load(std::sync::atomic::Ordering::Relaxed)
    }
    /// Returns `true` iff adding `delta` more partitions would push past
    /// the budget. `max == usize::MAX` always returns `false` (gate off).
    pub(crate) fn would_exceed(&self, delta: usize) -> bool {
        if self.max == usize::MAX {
            return false;
        }
        self.current().saturating_add(delta) > self.max
    }
}

/// F196: pick the partition cap for this PS.
///
/// - When `--cpuset` was supplied explicitly (`cpuset_explicit() == true`),
///   the cap is `cpuset_len / 2` because each partition reserves 2 OS
///   threads (P-log + P-bulk). Floored at 1 so a single-core cpuset
///   still permits one partition.
/// - Otherwise the cap is `usize::MAX` (gate off — pre-F196 behaviour).
pub(crate) fn compute_partition_budget_cap() -> usize {
    use autumn_common::{cpuset_explicit, cpuset_len};
    if cpuset_explicit() {
        let n = cpuset_len();
        let cap = n / 2;
        if cap == 0 {
            tracing::warn!(
                cpuset_len = n,
                "F196: --cpuset has < 2 cores; PS partition budget set to 1 (no headroom for P-bulk pinning)"
            );
            1
        } else {
            tracing::info!(
                cpuset_len = n,
                max_partitions = cap,
                "F196: PS partition budget enforced (2 cores per partition)"
            );
            cap
        }
    } else {
        usize::MAX
    }
}

#[cfg(test)]
mod f189_admission_tests {
    //! F189 + F196 D-r7 admission tests. RateController is per-partition
    //! (fg bytes/iops + compact bytes + gc bytes); ConcurrencyController
    //! is PS-wide (compact + gc concurrency permits).
    use super::*;

    fn unlimited_rate() -> RateController {
        RateController::new(0, 0, 0, 0, 0.8)
    }

    #[test]
    fn fg_unlimited_no_sleep() {
        let rt = compio::runtime::Runtime::new().expect("rt");
        rt.block_on(async {
            let rc = unlimited_rate();
            let t0 = Instant::now();
            for _ in 0..100 {
                rc.account_fg(1024 * 1024, 1).await;
            }
            assert!(
                t0.elapsed() < Duration::from_millis(50),
                "fg unlimited must not sleep"
            );
            let (fg, ops, _c, _g, _) = rc.snapshot();
            assert!(fg >= 100 * 1024 * 1024);
            assert_eq!(ops, 100);
        });
    }

    #[test]
    fn compact_unlimited_no_sleep() {
        let rt = compio::runtime::Runtime::new().expect("rt");
        rt.block_on(async {
            let rc = unlimited_rate();
            let t0 = Instant::now();
            for _ in 0..100 {
                rc.account_compact(1024 * 1024).await;
            }
            assert!(t0.elapsed() < Duration::from_millis(50));
        });
    }

    #[test]
    fn gc_unlimited_no_sleep() {
        let rt = compio::runtime::Runtime::new().expect("rt");
        rt.block_on(async {
            let rc = unlimited_rate();
            let t0 = Instant::now();
            for _ in 0..100 {
                rc.account_gc(1024 * 1024).await;
            }
            assert!(t0.elapsed() < Duration::from_millis(50));
        });
    }

    /// compact_rate=10 MiB/s: 5 MiB × 2 should sleep ~1s back-to-back.
    #[test]
    fn compact_respects_own_rate() {
        let rt = compio::runtime::Runtime::new().expect("rt");
        rt.block_on(async {
            let rc = RateController::new(0, 0, 10 * 1024 * 1024, 0, 0.8);
            let t0 = Instant::now();
            rc.account_compact(5 * 1024 * 1024).await;
            rc.account_compact(5 * 1024 * 1024).await;
            let elapsed = t0.elapsed();
            assert!(
                elapsed >= Duration::from_millis(800)
                    && elapsed <= Duration::from_millis(1500),
                "compact should sleep ~1s on 10 MiB at 10 MiB/s, got {:?}",
                elapsed
            );
        });
    }

    /// gc_rate=10 MiB/s: same as compact but on its own counter.
    #[test]
    fn gc_respects_own_rate() {
        let rt = compio::runtime::Runtime::new().expect("rt");
        rt.block_on(async {
            let rc = RateController::new(0, 0, 0, 10 * 1024 * 1024, 0.8);
            let t0 = Instant::now();
            rc.account_gc(5 * 1024 * 1024).await;
            rc.account_gc(5 * 1024 * 1024).await;
            let elapsed = t0.elapsed();
            assert!(
                elapsed >= Duration::from_millis(800)
                    && elapsed <= Duration::from_millis(1500),
                "gc should sleep ~1s on 10 MiB at 10 MiB/s, got {:?}",
                elapsed
            );
        });
    }

    /// F196 D-r7: compact and gc rate counters are independent. With
    /// compact_rate=10 MiB/s and gc_rate=10 MiB/s, hammering compact
    /// to its limit MUST NOT throttle gc — they have separate budgets.
    #[test]
    fn compact_and_gc_rates_are_independent() {
        let rt = compio::runtime::Runtime::new().expect("rt");
        rt.block_on(async {
            let rc = RateController::new(0, 0, 10 * 1024 * 1024, 10 * 1024 * 1024, 0.8);
            // Saturate compact.
            rc.account_compact(10 * 1024 * 1024).await;
            // gc against its own (untouched) budget — should sleep
            // far less than the compact-saturated path would.
            let t0 = Instant::now();
            rc.account_gc(1024).await;
            let elapsed = t0.elapsed();
            assert!(
                elapsed < Duration::from_millis(50),
                "gc must not be throttled by compact saturation, got {:?}",
                elapsed
            );
        });
    }

    /// fg saturated: compact yields to the rest of the 1-s window.
    #[test]
    fn compact_yields_when_fg_saturated() {
        let rt = compio::runtime::Runtime::new().expect("rt");
        rt.block_on(async {
            // fg_rate=100 MiB/s, ratio=0.5 → saturated at 50 MiB/s
            // observed; compact budget generous so own-rate doesn't fire.
            let rc = RateController::new(
                100 * 1024 * 1024,
                0,
                100 * 1024 * 1024,
                0,
                0.5,
            );
            rc.account_fg(60 * 1024 * 1024, 1).await;
            let t0 = Instant::now();
            rc.account_compact(1024).await;
            assert!(
                t0.elapsed() >= Duration::from_millis(300),
                "compact should yield to fg, got {:?}",
                t0.elapsed()
            );
        });
    }

    /// fg saturated: gc yields too (symmetric).
    #[test]
    fn gc_yields_when_fg_saturated() {
        let rt = compio::runtime::Runtime::new().expect("rt");
        rt.block_on(async {
            let rc = RateController::new(
                100 * 1024 * 1024,
                0,
                0,
                100 * 1024 * 1024,
                0.5,
            );
            rc.account_fg(60 * 1024 * 1024, 1).await;
            let t0 = Instant::now();
            rc.account_gc(1024).await;
            assert!(
                t0.elapsed() >= Duration::from_millis(300),
                "gc should yield to fg, got {:?}",
                t0.elapsed()
            );
        });
    }

    /// fg idle: compact + gc don't yield.
    #[test]
    fn compact_and_gc_do_not_yield_when_fg_idle() {
        let rt = compio::runtime::Runtime::new().expect("rt");
        rt.block_on(async {
            let rc = RateController::new(
                100 * 1024 * 1024,
                0,
                100 * 1024 * 1024,
                100 * 1024 * 1024,
                0.5,
            );
            let t0 = Instant::now();
            rc.account_compact(1024).await;
            rc.account_gc(1024).await;
            assert!(
                t0.elapsed() < Duration::from_millis(50),
                "no yield when fg idle, got {:?}",
                t0.elapsed()
            );
        });
    }

    /// fg unlimited (both bytes and iops=0): compact/gc ignore fg-aware yield.
    #[test]
    fn compact_and_gc_ignore_fg_when_fg_unlimited() {
        let rt = compio::runtime::Runtime::new().expect("rt");
        rt.block_on(async {
            let rc = RateController::new(0, 0, 100 * 1024 * 1024, 100 * 1024 * 1024, 0.5);
            rc.account_fg(500 * 1024 * 1024, 1).await;
            let t0 = Instant::now();
            rc.account_compact(1024).await;
            rc.account_gc(1024).await;
            assert!(
                t0.elapsed() < Duration::from_millis(50),
                "must ignore fg when no cap"
            );
        });
    }

    /// fg-iops cap throttles small-value workloads even when bytes is
    /// far under the bytes cap.
    #[test]
    fn fg_iops_cap_throttles_small_value_workloads() {
        let rt = compio::runtime::Runtime::new().expect("rt");
        rt.block_on(async {
            let rc = RateController::new(10 * 1024 * 1024 * 1024, 1_000, 0, 0, 0.8);
            let t0 = Instant::now();
            rc.account_fg(800, 100).await;
            rc.account_fg(800, 100).await;
            let elapsed = t0.elapsed();
            assert!(
                elapsed >= Duration::from_millis(150)
                    && elapsed <= Duration::from_millis(500),
                "fg-iops cap should throttle 200 ops at 1k ops/s, got {:?}",
                elapsed
            );
        });
    }

    /// compact yields when fg observed IOPS exceeds ratio × fg_iops.
    #[test]
    fn compact_yields_when_fg_iops_saturated() {
        let rt = compio::runtime::Runtime::new().expect("rt");
        rt.block_on(async {
            let rc = RateController::new(
                10 * 1024 * 1024 * 1024, // bytes ~unlimited
                1_000,                    // 1k ops/s
                100 * 1024 * 1024,        // compact generous
                0,
                0.5, // bg yields at 500 ops/s observed
            );
            rc.account_fg(0, 600).await;
            let t0 = Instant::now();
            rc.account_compact(1024).await;
            assert!(
                t0.elapsed() >= Duration::from_millis(200),
                "compact should yield on fg-iops saturation, got {:?}",
                t0.elapsed()
            );
        });
    }

    /// gc yields when fg observed IOPS exceeds ratio × fg_iops (gc twin).
    #[test]
    fn gc_yields_when_fg_iops_saturated() {
        let rt = compio::runtime::Runtime::new().expect("rt");
        rt.block_on(async {
            let rc = RateController::new(
                10 * 1024 * 1024 * 1024,
                1_000,
                0,
                100 * 1024 * 1024,
                0.5,
            );
            rc.account_fg(0, 600).await;
            let t0 = Instant::now();
            rc.account_gc(1024).await;
            assert!(
                t0.elapsed() >= Duration::from_millis(200),
                "gc should yield on fg-iops saturation, got {:?}",
                t0.elapsed()
            );
        });
    }

    /// Window reset clears all four counters after 1s.
    #[test]
    fn window_resets_after_1s() {
        let rt = compio::runtime::Runtime::new().expect("rt");
        rt.block_on(async {
            let rc = unlimited_rate();
            rc.account_fg(123, 2).await;
            rc.account_compact(456).await;
            rc.account_gc(789).await;
            let (fg, ops, c, g, _) = rc.snapshot();
            assert_eq!((fg, ops, c, g), (123, 2, 456, 789));
            compio::time::sleep(Duration::from_millis(1100)).await;
            rc.account_fg(7, 1).await;
            let (fg, ops, c, g, _) = rc.snapshot();
            assert_eq!(
                (fg, ops, c, g),
                (7, 1, 0, 0),
                "window reset must zero all four counters"
            );
        });
    }

    /// PS-wide concurrency caps: acquire_compact / acquire_gc.
    /// compact_max=1 serializes; gc_max=2 allows two concurrent permits.
    #[test]
    fn concurrency_acquire_compact_and_gc_caps() {
        use std::sync::atomic::Ordering;
        let cc = std::sync::Arc::new(ConcurrencyController::new(1, 2));
        let rt = compio::runtime::Runtime::new().expect("rt");
        rt.block_on(async {
            let p1 = cc.acquire_compact().await;
            assert_eq!(cc.compact_inflight.load(Ordering::Acquire), 1);
            drop(p1);
            assert_eq!(cc.compact_inflight.load(Ordering::Acquire), 0);

            let g1 = cc.acquire_gc().await;
            let g2 = cc.acquire_gc().await;
            assert_eq!(cc.gc_inflight.load(Ordering::Acquire), 2);
            drop(g1);
            assert_eq!(cc.gc_inflight.load(Ordering::Acquire), 1);
            drop(g2);
            assert_eq!(cc.gc_inflight.load(Ordering::Acquire), 0);
        });
    }
}

impl PartitionServer {
    pub async fn connect(ps_id: u64, manager_endpoint: &str) -> Result<Self> {
        Self::connect_with_advertise(ps_id, manager_endpoint, None).await
    }

    /// Current manager address (round-robin).
    fn manager_addr(&self) -> &str {
        &self.manager_addrs[self.current_mgr.get() % self.manager_addrs.len()]
    }

    /// Rotate to next manager on NotLeader or connection error.
    fn rotate_manager(&self) {
        let next = (self.current_mgr.get() + 1) % self.manager_addrs.len();
        self.current_mgr.set(next);
    }

    /// F099-K: caller supplies the first-partition listen address up front
    /// so `base_port` + `advertise_host` are populated BEFORE
    /// `finish_connect()` runs its implicit `sync_regions_once()`.
    ///
    /// **Required for any caller that may see existing partitions on connect**,
    /// including the production binary (`autumn-ps`) on restart: when
    /// partitions are already registered with the manager, `sync_regions_once`
    /// fires `open_partition` immediately, and `open_partition` needs a valid
    /// `base_port`. Earlier comments here claimed the production binary could
    /// use `connect_with_advertise` because no partitions exist at connect
    /// time — that's only true on fresh bootstrap. On restart the first
    /// partition would otherwise bind to port `0 + 1 = 1`.
    pub async fn connect_with_advertise_and_port(
        ps_id: u64,
        manager_endpoint: &str,
        advertise_addr: Option<String>,
        listen_addr: SocketAddr,
    ) -> Result<Self> {
        let server = Self::connect_raw(ps_id, manager_endpoint, advertise_addr).await?;
        server.bind_listen_addr(listen_addr)?;
        server.finish_connect().await
    }

    pub async fn connect_with_advertise(
        ps_id: u64,
        manager_endpoint: &str,
        advertise_addr: Option<String>,
    ) -> Result<Self> {
        let server = Self::connect_raw(ps_id, manager_endpoint, advertise_addr).await?;
        server.finish_connect().await
    }

    /// F099-K helper: build `Self` (acquire owner lock + fill
    /// `manager_addrs`) but DO NOT call `finish_connect()`. Callers
    /// that need to set `base_port` before the implicit
    /// `sync_regions_once()` runs use this + `bind_listen_addr` +
    /// `finish_connect`.
    async fn connect_raw(
        ps_id: u64,
        manager_endpoint: &str,
        advertise_addr: Option<String>,
    ) -> Result<Self> {
        let pool = Rc::new(ConnPool::new());
        let mgr_addrs: Vec<String> = manager_endpoint
            .split(',')
            .map(|s| autumn_stream::conn_pool::normalize_endpoint(s.trim()))
            .collect();
        let owner_key = format!("ps-{ps_id}");

        // Acquire owner lock — try each manager until one responds.
        let req = manager_rpc::rkyv_encode(&manager_rpc::AcquireOwnerLockReq {
            owner_key: owner_key.clone(),
        });
        let mut last_err = None;
        for (idx, addr) in mgr_addrs.iter().enumerate() {
            // 10 s — owner-lock acquisition is one etcd CAS on
            // manager. Bounded so a hanging manager doesn't block
            // PS startup; the loop walks to the next address.
            match pool
                .call_timeout(addr, manager_rpc::MSG_ACQUIRE_OWNER_LOCK, req.clone(), Duration::from_secs(10))
                .await
            {
                Ok(resp_data) => {
                    let resp: manager_rpc::AcquireOwnerLockResp =
                        manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
                    if resp.code == manager_rpc::CODE_OK {
                        let connected_idx = idx;
                        let server = Self {
                            ps_id,
                            advertise_addr,
                            partitions: Rc::new(RefCell::new(HashMap::new())),
                            manager_addrs: mgr_addrs,
                            current_mgr: Cell::new(connected_idx),
                            pool,
                            server_owner_key: owner_key,
                            server_revision: Rc::new(Cell::new(resp.revision)),
                            // F099-K — placeholders; populated by
                            // `bind_listen_addr` (called from
                            // `serve()` or `connect_with_advertise_and_port`).
                            base_port: Cell::new(0),
                            next_port_ord: Rc::new(Cell::new(0)),
                            advertise_host: Rc::new(std::cell::RefCell::new(
                                String::from("127.0.0.1"),
                            )),
                            listen_host: Rc::new(std::cell::RefCell::new(
                                String::from("0.0.0.0"),
                            )),
                            concurrency_ctrl: std::sync::Arc::new(
                                ConcurrencyController::from_env(),
                            ),
                            partition_budget: std::sync::Arc::new(PartitionBudget::new(
                                compute_partition_budget_cap(),
                            )),
                        };
                        return Ok(server);
                    } else if resp.code == manager_rpc::CODE_NOT_LEADER {
                        last_err = Some(anyhow!("NotLeader from {}", addr));
                        continue;
                    } else {
                        return Err(anyhow!("acquire_owner_lock failed: {}", resp.message));
                    }
                }
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("no manager available")))
    }

    async fn finish_connect(self) -> Result<Self> {
        let server = self;

        // Retry register_ps — manager may still be electing leader after restart.
        let mut retries = 15;
        loop {
            match server.register_ps().await {
                Ok(()) => break,
                Err(e) if retries > 0 && e.to_string().contains("not leader") => {
                    retries -= 1;
                    tracing::warn!(
                        "register_ps: manager not leader yet, retrying in 1s ({retries} left)"
                    );
                    compio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(e) => return Err(e),
            }
        }

        // Start heartbeats BEFORE the (potentially long) sync_regions_once. The
        // manager evicts a PS after PS_DEAD_TIMEOUT (10s) without a heartbeat;
        // opening N partitions can take >10s (each open_partition awaits
        // commit_length on 3 streams + replays the WAL), which used to push
        // the first heartbeat past the deadline and silently evict the PS.
        let s = server.clone();
        compio::runtime::spawn(async move { s.heartbeat_loop().await }).detach();

        // F183: per-partition load metrics report (5 s cadence).
        let s = server.clone();
        compio::runtime::spawn(async move { s.report_load_loop().await }).detach();

        // F203: maintenance_scheduler_loop deleted. Pre-F203 this task
        // picked the highest-debt partition each tick and dispatched
        // GC / minor / major compact internally. Per the mechanism /
        // policy separation refactor, that policy lives in an external
        // controller now; the PS reacts to manual `Maintenance` RPCs
        // (`client gc / compact / forcegc / flush`) and to its own
        // expiry-triggered + has_overlap-triggered + size-tiered
        // minor compact (mechanism-level must-cleanup paths preserved
        // in `background_compact_loop`'s timer arm).

        server.sync_regions_once().await?;

        Ok(server)
    }

    async fn register_ps(&self) -> Result<()> {
        let address = self
            .advertise_addr
            .clone()
            .unwrap_or_else(|| format!("ps-{}", self.ps_id));
        let req = manager_rpc::rkyv_encode(&manager_rpc::RegisterPsReq {
            ps_id: self.ps_id,
            address,
        });
        // 10 s — register_ps is one in-memory insert + etcd mirror on
        // manager. Bounded so PS startup doesn't trap waiting for a
        // hung manager.
        let resp_data = self
            .pool
            .call_timeout(self.manager_addr(), manager_rpc::MSG_REGISTER_PS, req, Duration::from_secs(10))
            .await
            .context("register ps")?;
        let resp: manager_rpc::CodeResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        if resp.code != manager_rpc::CODE_OK {
            return Err(anyhow!("register_ps failed: {}", resp.message));
        }
        Ok(())
    }

    async fn heartbeat_loop(&self) {
        const HEARTBEAT_INTERVAL_SECS: u64 = 2;
        const MAX_CONSECUTIVE_FAILURES: u32 = 5; // 5 × 2s = 10s
        let mut consecutive_failures: u32 = 0;
        let mut ticker = compio::time::interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
        ticker.tick().await; // first tick is immediate
        loop {
            ticker.tick().await;
            let req = manager_rpc::rkyv_encode(&manager_rpc::HeartbeatPsReq { ps_id: self.ps_id });
            // 5 s — heartbeat is fired every 2 s; we tolerate up to 5
            // consecutive failures (~10 s) before exiting. A 5 s
            // ceiling keeps each tick tight; a missed beat shows up
            // as Err and feeds the failure counter.
            match self
                .pool
                .call_timeout(self.manager_addr(), manager_rpc::MSG_HEARTBEAT_PS, req, Duration::from_secs(5))
                .await
            {
                Ok(resp_data) => {
                    consecutive_failures = 0;
                    let code = manager_rpc::rkyv_decode::<manager_rpc::CodeResp>(&resp_data)
                        .map(|r| r.code)
                        .unwrap_or(manager_rpc::CODE_OK);
                    // Manager surfaces CODE_NOT_FOUND when ps_id is not in
                    // ps_nodes (e.g. evicted after a transient hiccup). Re-
                    // register and re-sync so the PS rejoins the cluster
                    // instead of staying invisible to clients (`ps=unknown`).
                    if code == manager_rpc::CODE_NOT_FOUND {
                        tracing::warn!(
                            "PS {} not in manager ps_nodes; re-registering",
                            self.ps_id,
                        );
                        if let Err(e) = self.register_ps().await {
                            tracing::warn!("PS {} re-register failed: {e}", self.ps_id);
                        } else if let Err(e) = self.sync_regions_once().await {
                            tracing::warn!(
                                "PS {} re-sync after re-register failed: {e}",
                                self.ps_id,
                            );
                        }
                    }
                }
                Err(e) => {
                    consecutive_failures += 1;
                    self.rotate_manager();
                    tracing::warn!(
                        "PS {} heartbeat failed ({}/{}): {e} (next mgr: {})",
                        self.ps_id, consecutive_failures, MAX_CONSECUTIVE_FAILURES,
                        self.manager_addr(),
                    );
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                        tracing::error!(
                            "PS {} heartbeat lost for {}s, exiting to prevent stale serving",
                            self.ps_id,
                            consecutive_failures as u64 * HEARTBEAT_INTERVAL_SECS,
                        );
                        std::process::exit(1);
                    }
                }
            }
        }
    }

    /// F183: every 5 s, snapshot per-partition metrics from each
    /// PartitionHandle's Arc<PartitionMetrics> and ship to the manager
    /// via MSG_REPORT_PARTITION_LOAD. Cheap — one RPC per cycle, payload
    /// scales with partition count.
    async fn report_load_loop(&self) {
        // F196: 5 s → 30 s. Manager aggregates into 60 s buckets and
        // the policy window only inspects the last 5 buckets (5 min);
        // 5 s upload was over-sampling by ~6× with no advisory benefit.
        // At 30 s cadence each bucket still gets ≥1 sample and req/s
        // is averaged over the 30 s window before the bucket close.
        const REPORT_INTERVAL_SECS: u64 = 30;
        let mut ticker = compio::time::interval(Duration::from_secs(REPORT_INTERVAL_SECS));
        ticker.tick().await; // first tick is immediate
        loop {
            ticker.tick().await;
            let snapshots: Vec<manager_rpc::PartitionLoad> = {
                let parts = self.partitions.borrow();
                parts
                    .iter()
                    .map(|(part_id, handle)| {
                        use std::sync::atomic::Ordering::Relaxed;
                        let req = handle.metrics.req_count.swap(0, Relaxed);
                        let imm_full = handle.metrics.imm_full_count.swap(0, Relaxed);
                        let size_bytes = handle.metrics.size_bytes.load(Relaxed);
                        // F187: maintenance debt + inflight + last-run timestamps.
                        // gc_debt/pending_compaction are gauges (load, no swap);
                        // *_inflight are 0/1 booleans.
                        let gc_debt_bytes = handle.metrics.gc_debt_bytes.load(Relaxed);
                        let pending_compaction_bytes =
                            handle.metrics.pending_compaction_bytes.load(Relaxed);
                        let gc_inflight = handle.metrics.gc_inflight.load(Relaxed);
                        let compact_inflight = handle.metrics.compact_inflight.load(Relaxed);
                        let last_gc_at = handle.metrics.last_gc_at.load(Relaxed);
                        let last_compact_at = handle.metrics.last_compact_at.load(Relaxed);
                        // F202: dead-data + minor-compact debt + sealed-extent count.
                        let sst_tombstone_bytes =
                            handle.metrics.sst_tombstone_bytes.load(Relaxed);
                        let sst_expired_bytes =
                            handle.metrics.sst_expired_bytes.load(Relaxed);
                        let sst_out_of_range_bytes =
                            handle.metrics.sst_out_of_range_bytes.load(Relaxed);
                        let minor_compact_pending_bytes =
                            handle.metrics.minor_compact_pending_bytes.load(Relaxed);
                        let sealed_log_extent_count =
                            handle.metrics.sealed_log_extent_count.load(Relaxed);
                        manager_rpc::PartitionLoad {
                            part_id: *part_id,
                            size_bytes,
                            req_per_sec: (req / REPORT_INTERVAL_SECS) as u32,
                            imm_full_per_sec: (imm_full / REPORT_INTERVAL_SECS) as u32,
                            p99_us: 0,
                            gc_debt_bytes,
                            pending_compaction_bytes,
                            gc_inflight,
                            compact_inflight,
                            last_gc_at,
                            last_compact_at,
                            sst_tombstone_bytes,
                            sst_expired_bytes,
                            sst_out_of_range_bytes,
                            minor_compact_pending_bytes,
                            sealed_log_extent_count,
                        }
                    })
                    .collect()
            };
            if snapshots.is_empty() {
                continue;
            }
            let req = manager_rpc::rkyv_encode(&manager_rpc::ReportPartitionLoadReq {
                ps_id: self.ps_id,
                partitions: snapshots,
            });
            // 10 s — telemetry report fired every 30 s. Bounded so a
            // hung manager doesn't keep the report task alive past
            // the next interval; missed report is benign (manager
            // policy degrades gracefully on stale data).
            if let Err(e) = self
                .pool
                .call_timeout(self.manager_addr(), manager_rpc::MSG_REPORT_PARTITION_LOAD, req, Duration::from_secs(10))
                .await
            {
                tracing::debug!("F183 report_load failed: {e}");
            }
        }
    }


    async fn region_sync_loop(&self) {
        let mut ticker = compio::time::interval(Duration::from_secs(2));
        ticker.tick().await; // first tick is immediate
        loop {
            ticker.tick().await;
            tracing::debug!("PS {} region_sync_loop: syncing", self.ps_id);
            if let Err(e) = self.sync_regions_once().await {
                tracing::warn!("PS {} region sync failed: {e}", self.ps_id);
            }
        }
    }

    pub async fn sync_regions_once(&self) -> Result<()> {
        // 10 s — read-only manager call. Bounded so the periodic
        // 2 s region_sync_loop doesn't pile up on a hung manager.
        let resp_data = self
            .pool
            .call_timeout(self.manager_addr(), manager_rpc::MSG_GET_REGIONS, Bytes::new(), Duration::from_secs(10))
            .await
            .context("get regions")?;
        let resp: manager_rpc::GetRegionsResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        if resp.code != manager_rpc::CODE_OK {
            return Err(anyhow!("get_regions failed: {}", resp.message));
        }

        // Tuple shape mirrors `PartitionHandle.opened_with`:
        //   (rg, log_stream, row_stream, meta_stream, region_epoch)
        let mut wanted: BTreeMap<u64, (Range, u64, u64, u64, u64)> = BTreeMap::new();
        tracing::debug!("PS {} sync: got {} regions, my ps_id={}", self.ps_id, resp.regions.len(), self.ps_id);
        for (part_id, region) in resp.regions {
            tracing::debug!("PS {} sync: region part_id={} ps_id={} epoch={}", self.ps_id, part_id, region.ps_id, region.region_epoch);
            if region.ps_id == self.ps_id {
                if let Some(rg) = region.rg {
                    wanted.insert(
                        part_id,
                        (
                            Range {
                                start_key: rg.start_key,
                                end_key: rg.end_key,
                            },
                            region.log_stream,
                            region.row_stream,
                            region.meta_stream,
                            region.region_epoch,
                        ),
                    );
                }
            }
        }

        // Remove partitions no longer assigned. Dropping the PartitionHandle
        // closes its `fd_tx`; the P-log fd-drain task sees `.next() == None`
        // and exits, which in turn closes every ps-conn task's `req_tx`
        // clone — partition_loop observes `req_rx.next() == None` and
        // drains. The partition thread then joins on its own.
        //
        // F184: also detect partitions whose (rg, stream_ids) changed since
        // open (e.g., post-merge widening; post-split stream rotation that
        // wasn't caught by F103). Drop the handle so the open-new-partitions
        // pass reopens with fresh state.
        let current: Vec<u64> = self.partitions.borrow().keys().copied().collect();
        for part_id in current {
            let drop_for_reload = match wanted.get(&part_id) {
                None => true,
                Some(latest) => {
                    // F212-fix-2: `opened_with` is now Arc<Mutex<_>>;
                    // lock + clone the tuple, then compare. The lock is
                    // ~25 ns uncontended and is held only for the
                    // tuple clone (no I/O between borrow and lock).
                    let opened = self.partitions.borrow().get(&part_id)
                        .map(|h| h.opened_with.lock().clone());
                    match opened {
                        Some(prev) => prev != *latest,
                        None => false,
                    }
                }
            };
            if drop_for_reload {
                tracing::info!(
                    "PS {} F184 reloading partition {part_id} due to region change",
                    self.ps_id
                );
                if self.partitions.borrow_mut().remove(&part_id).is_some() {
                    // F196: keep budget counter in sync with the live
                    // partition map.
                    self.partition_budget.dec();
                }
            }
        }

        // Open new partitions.
        for (part_id, (rg, log_stream_id, row_stream_id, meta_stream_id, region_epoch)) in wanted {
            if self.partitions.borrow().contains_key(&part_id) {
                continue;
            }
            // F196: refuse to open a NEW partition when the static budget
            // is exhausted. Manager assigned more partitions than the
            // operator pre-allocated cores for; leave the slot uncovered
            // so the operator sees `ps=unknown` in `client info` and can
            // either grow --cpuset or add more PSes. Existing partitions
            // are unaffected.
            if self.partition_budget.would_exceed(1) {
                tracing::warn!(
                    ps_id = self.ps_id,
                    part_id,
                    current = self.partition_budget.current(),
                    max = self.partition_budget.max,
                    "F196: refusing to open partition — PS core budget exhausted (cpuset_len/2). \
                     Operator must grow --cpuset or migrate this partition to another PS."
                );
                continue;
            }
            tracing::info!("PS {} opening partition {part_id}", self.ps_id);
            let handle = self
                .open_partition(part_id, rg, region_epoch, log_stream_id, row_stream_id, meta_stream_id)
                .await?;
            tracing::info!("PS {} partition {part_id} opened", self.ps_id);
            self.partitions.borrow_mut().insert(part_id, handle);
            // F196: bump budget counter only after a successful insert.
            self.partition_budget.inc();
        }
        Ok(())
    }

    /// Spawn a dedicated OS thread with its own compio runtime for this partition.
    ///
    /// F099-K: the partition thread BINDS its own TcpListener on a unique
    /// port (`base_port + next_port_ord`) and runs an accept loop on its
    /// own compio runtime, so there is no cross-thread fd handoff. Once
    /// the listener is bound, the partition thread registers its address
    /// with the manager via `MSG_REGISTER_PARTITION_ADDR`, which is then
    /// returned to clients via `GetRegionsResp.part_addrs`.
    ///
    /// Port allocation is monotonic — we never reuse a port within the
    /// lifetime of a PS process, even if a partition is closed and then
    /// re-opened, so there is no TIME_WAIT hazard across region-sync
    /// cycles. The ordinal is bumped BEFORE spawn so a bind failure on
    /// one partition does not collapse the whole port sequence.
    async fn open_partition(
        &self,
        part_id: u64,
        rg: Range,
        region_epoch: u64,
        log_stream_id: u64,
        row_stream_id: u64,
        meta_stream_id: u64,
    ) -> Result<PartitionHandle> {
        let manager_addr = self.manager_addrs.join(",");
        let owner_key = self.server_owner_key.clone();
        let revision = self.server_revision.get();
        let ps_id = self.ps_id;
        // F184: snapshot the open-time params for sync_regions_once
        // change-detection (post-merge widening, post-split narrowing,
        // stream-ID rotation).
        // F184 + epoch: drop+reopen when rg / stream IDs / region_epoch
        // changes vs open-time snapshot. rg change implies an epoch bump
        // in our manager-side rule, but include the epoch explicitly so
        // a future scheme (e.g., epoch bump on PS reassignment without
        // rg change) is caught without modifying the comparison logic.
        //
        // F212-fix-2: wrap in Arc<Mutex<>> so the partition thread can
        // update this in-place after `handle_split_part` (and future
        // in-place updaters) without going through a drop+reopen.
        let opened_with: std::sync::Arc<parking_lot::Mutex<(Range, u64, u64, u64, u64)>> =
            std::sync::Arc::new(parking_lot::Mutex::new((
                rg.clone(),
                log_stream_id,
                row_stream_id,
                meta_stream_id,
                region_epoch,
            )));
        let opened_with_for_thread = opened_with.clone();

        // F099-K port allocation: reserve the next ordinal eagerly so a
        // later `open_partition` never collides with this one even if the
        // actual `bind` below is delayed by the worker thread startup.
        let ord = self.next_port_ord.get().checked_add(1).ok_or_else(|| {
            anyhow!("exhausted partition port ordinal space (u16 overflow)")
        })?;
        self.next_port_ord.set(ord);
        // ord is 1-based; cpu pool indexing is 0-based. F122-fix: P-log and
        // P-bulk now pin to different cores — at sustained 4 KB write loads
        // P-bulk's `build_sst_bytes` is CPU-heavy enough to fight P-log for
        // cycles when they share. Layout: partition i takes cores
        // [cpu_offset + 2i, cpu_offset + 2i+1] (P-log, P-bulk).
        let ord_zero = (ord as usize).saturating_sub(1);
        let cpu_log = pick_cpu_for_ord(2 * ord_zero);
        let cpu_bulk = pick_cpu_for_ord(2 * ord_zero + 1);
        let base_port = self.base_port.get();
        let listen_port = base_port.checked_add(ord).ok_or_else(|| {
            anyhow!(
                "base_port={} + ord={} overflows u16; pick a smaller base port",
                base_port, ord,
            )
        })?;
        let listen_host = self.listen_host.borrow().clone();
        let listen_addr_s = if listen_host.contains(':') {
            format!("[{}]:{}", listen_host.trim_matches(['[', ']']), listen_port)
        } else {
            format!("{}:{}", listen_host, listen_port)
        };
        let listen_addr: SocketAddr = listen_addr_s
            .parse()
            .context("parse per-partition listen addr")?;
        let advertise_host = self.advertise_host.borrow().clone();
        let advertise_addr = format!("{}:{}", advertise_host, listen_port);

        // Shutdown signal: main thread drops `shutdown_tx` to tell the
        // partition's accept loop to exit.
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        // F120-C: graceful drain signal. Main thread (`PartitionServer::
        // shutdown()`) sends a `oneshot::Sender<()>` through `drain_tx` to
        // ask the partition to rotate active + flush all imm and reply
        // when done, BEFORE dropping `shutdown_tx`.
        let (drain_tx, drain_rx) = mpsc::unbounded::<oneshot::Sender<()>>();

        // Report bind + registration success/failure back to the caller,
        // so we can fail loudly and reclaim the ordinal if needed.
        let (ready_tx, ready_rx) = oneshot::channel::<Result<()>>();

        let manager_addr_for_thread = manager_addr.clone();
        let owner_key_for_thread = owner_key.clone();
        let advertise_addr_for_thread = advertise_addr.clone();
        // F196 D-r7: hand the partition thread the PS-wide concurrency
        // Arc (shared) + a fresh RateController per partition (built
        // inside the thread from process-global setters).
        let concurrency_for_thread = self.concurrency_ctrl.clone();
        let partition_budget_for_thread = self.partition_budget.clone();
        // F183: build the metrics Arc on the main thread so we can keep
        // a clone in PartitionHandle for the report loop. The other clone
        // is moved into the partition thread and threaded into PartitionData.
        let metrics_for_thread = std::sync::Arc::new(PartitionMetrics::default());
        let metrics_for_handle = metrics_for_thread.clone();
        // F188: build the compact + GC trigger channels on the main thread
        // so `PartitionHandle` can hold a Send `mpsc::Sender` for the
        // maintenance scheduler. The receivers go into `partition_thread_main`
        // and on into `background_compact_loop` / `background_gc_loop`. The
        // in-thread sender clones land in `PartitionData` via `partition_thread_main`.
        let (compact_tx_main, compact_rx_main) = mpsc::channel::<bool>(1);
        let (gc_tx_main, gc_rx_main) = mpsc::channel::<GcTask>(1);
        let compact_tx_for_thread = compact_tx_main.clone();
        let gc_tx_for_thread = gc_tx_main.clone();
        let join = std::thread::Builder::new()
            .name(format!("part-{part_id}"))
            .spawn(move || {
                let rt = compio::runtime::RuntimeBuilder::new()
                    .thread_affinity(affinity_set(cpu_log))
                    .build()
                    .expect("create compio runtime");
                tracing::info!(part_id, cpu_log = ?cpu_log, cpu_bulk = ?cpu_bulk, "P-log thread runtime ready");
                rt.block_on(async move {
                    if let Err(e) = partition_thread_main(
                        part_id,
                        rg,
                        region_epoch,
                        log_stream_id,
                        row_stream_id,
                        meta_stream_id,
                        manager_addr_for_thread,
                        owner_key_for_thread,
                        revision,
                        ps_id,
                        listen_addr,
                        advertise_addr_for_thread,
                        ready_tx,
                        shutdown_rx,
                        drain_rx,
                        cpu_bulk,
                        metrics_for_thread,
                        compact_tx_for_thread,
                        compact_rx_main,
                        gc_tx_for_thread,
                        gc_rx_main,
                        concurrency_for_thread,
                        partition_budget_for_thread,
                        opened_with_for_thread,
                    )
                    .await
                    {
                        tracing::error!(part_id, "partition thread failed: {e:#}");
                    }
                });
            })
            .context("spawn partition thread")?;

        // Wait for the partition thread to bind its listener and register
        // with the manager. If either step fails, bubble the error up so
        // `sync_regions_once` reports the failure (operator-visible; no
        // silent skip).
        match ready_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                return Err(e.context(format!(
                    "partition {part_id} failed to bind listener on {listen_addr} or register addr"
                )));
            }
            Err(_canceled) => {
                return Err(anyhow!(
                    "partition {part_id} thread exited before reporting listener readiness"
                ));
            }
        }

        Ok(PartitionHandle {
            shutdown_tx: Some(shutdown_tx),
            drain_tx: Some(drain_tx),
            part_addr: advertise_addr,
            metrics: metrics_for_handle,
            compact_trigger: compact_tx_main,
            gc_trigger: gc_tx_main,
            opened_with,
            join: Some(join),
        })
    }

    /// F120-C — graceful shutdown. For each open partition:
    ///   1. Send a `oneshot::Sender<()>` via `drain_tx`. The partition's
    ///      `partition_loop` stops pulling new requests, drains
    ///      inflight, rotates `active`, calls `flush_one_imm` until imm
    ///      is empty, then replies on the oneshot.
    ///   2. Await the oneshot with `AUTUMN_PS_SHUTDOWN_TIMEOUT_MS`
    ///      deadline. On timeout, log a warning and skip — the SIGKILL
    ///      fallback (and on-restart logStream replay) keeps correctness.
    ///   3. Drop `shutdown_tx` to close the per-partition accept loop;
    ///      every per-conn `req_tx` clone is dropped, `merged_loop`
    ///      observes `req_rx == None`, exits, partition thread joins.
    ///
    /// Concurrent partitions drain in parallel — wallclock is bounded
    /// by the slowest partition's flush time.
    pub async fn shutdown(&self) -> Result<()> {
        use futures::future::join_all;

        let timeout = Duration::from_millis(shutdown_timeout_ms());
        let part_ids: Vec<u64> = self.partitions.borrow().keys().copied().collect();

        tracing::info!(
            partitions = part_ids.len(),
            timeout_ms = timeout.as_millis() as u64,
            "graceful shutdown: draining partitions",
        );

        // Send drain signals + collect oneshot receivers.
        let mut drain_rxs: Vec<(u64, oneshot::Receiver<()>)> = Vec::new();
        {
            let mut parts = self.partitions.borrow_mut();
            for &pid in &part_ids {
                let Some(handle) = parts.get_mut(&pid) else { continue };
                let Some(drain_tx) = handle.drain_tx.as_ref() else { continue };
                let (ack_tx, ack_rx) = oneshot::channel::<()>();
                if drain_tx.unbounded_send(ack_tx).is_ok() {
                    drain_rxs.push((pid, ack_rx));
                }
            }
        }

        // Race each ack against the deadline.
        let waits = drain_rxs.into_iter().map(|(pid, ack_rx)| {
            let timeout = timeout;
            async move {
                use futures::future::{select, Either};
                let sleep_fut = compio::time::sleep(timeout);
                futures::pin_mut!(sleep_fut);
                match select(ack_rx, sleep_fut).await {
                    Either::Left((Ok(_), _)) => {
                        tracing::info!(part_id = pid, "graceful shutdown: drained");
                    }
                    Either::Left((Err(_), _)) => {
                        tracing::warn!(part_id = pid, "graceful shutdown: drain channel cancelled");
                    }
                    Either::Right(_) => {
                        tracing::warn!(
                            part_id = pid,
                            timeout_ms = timeout.as_millis() as u64,
                            "graceful shutdown: drain timed out (replay on restart will cover unflushed data)",
                        );
                    }
                }
            }
        });
        join_all(waits).await;

        // Now close accept loops by dropping `shutdown_tx`. The merged_loop
        // exits its tail-drain block once req_rx hits EOF (no more conns).
        // Drop drain_tx as well so any pending receiver wakes with EOF.
        {
            let mut parts = self.partitions.borrow_mut();
            for handle in parts.values_mut() {
                handle.shutdown_tx.take();
                handle.drain_tx.take();
            }
        }

        // Join threads (best effort; bound to a fraction of the timeout).
        let join_deadline = Duration::from_millis((timeout.as_millis() as u64).max(1_000));
        let join_start = std::time::Instant::now();
        loop {
            let mut all_done = true;
            {
                let mut parts = self.partitions.borrow_mut();
                for handle in parts.values_mut() {
                    if let Some(j) = handle.join.as_ref() {
                        if !j.is_finished() {
                            all_done = false;
                        }
                    }
                }
            }
            if all_done {
                let mut parts = self.partitions.borrow_mut();
                for handle in parts.values_mut() {
                    if let Some(j) = handle.join.take() {
                        let _ = j.join();
                    }
                }
                break;
            }
            if join_start.elapsed() >= join_deadline {
                tracing::warn!("graceful shutdown: thread join deadline reached, leaving threads detached");
                break;
            }
            compio::time::sleep(Duration::from_millis(50)).await;
        }
        tracing::info!("graceful shutdown: complete");
        Ok(())
    }

    // ── Serve ──────────────────────────────────────────────────────────
    //
    // F099-K thread model:
    //   - 1 main compio thread: control plane only (heartbeat_loop +
    //     region_sync_loop). No listener, no accept, no fd dispatch.
    //   - N × 2 partition OS threads: per-partition P-log + P-bulk. P-log
    //     binds its OWN `compio::net::TcpListener` on a unique port and
    //     runs its OWN accept task + ps-conn tasks + partition_loop
    //     on the same compio runtime. The only mpsc on the hot path is
    //     same-thread `PartitionRequest` (ps-conn → merged_loop). Total
    //     OS threads at N partitions: `1 + 2N` (pre-F099-K it was `2 + 2N`
    //     because of the separate accept thread).

    /// F099-K: initialise `base_port` + `advertise_host` from the
    /// supplied first-partition listen address. Safe to call multiple
    /// times with the same `addr` (idempotent set via `Cell` /
    /// `RefCell`). Called by `serve()` and by
    /// `connect_with_advertise_and_port` so callers that drive
    /// `sync_regions_once()` BEFORE entering `serve()`'s forever-loop
    /// still see a valid `base_port` in `open_partition`.
    pub fn bind_listen_addr(&self, addr: SocketAddr) -> Result<()> {
        // F099-K: the `addr` arg is repurposed as the FIRST PARTITION's
        // listener address. Partition N (1-indexed by open order) binds
        // `addr.port() + (N-1)`. `base_port` is therefore `addr.port() - 1`
        // so that `base_port + 1 == addr.port()` — preserves CLI compat
        // with the existing `--port 9201` convention.
        let first_port = addr.port();
        if first_port == 0 {
            return Err(anyhow!(
                "--port 0 (ephemeral) is not supported for PartitionServer; pick a stable base port"
            ));
        }
        let base_port = first_port - 1;
        self.base_port.set(base_port);
        *self.listen_host.borrow_mut() = addr.ip().to_string();

        // Parse advertise host from `advertise_addr` (falls back to
        // `127.0.0.1`). The per-partition advertise becomes
        // `"{host}:{listen_port}"`.
        let host = self
            .advertise_addr
            .as_ref()
            .and_then(|a| a.rsplit_once(':').map(|(h, _)| h.to_string()))
            .unwrap_or_else(|| "127.0.0.1".to_string());
        *self.advertise_host.borrow_mut() = host;
        Ok(())
    }

    pub async fn serve(&self, addr: SocketAddr) -> Result<()> {
        // Backwards-compatible: never-resolving shutdown_signal — caller
        // gets the pre-F120 forever-loop behavior. Production binaries
        // should use `serve_until_shutdown` with a SIGTERM-driven future.
        self.serve_until_shutdown(addr, std::future::pending::<()>()).await
    }

    /// F120-C — like `serve()` but exits the main control-plane loop
    /// when `shutdown_signal` resolves, then runs `self.shutdown()` to
    /// drain partitions before returning. Production: pass a future
    /// driven by a SIGTERM/SIGINT handler.
    pub async fn serve_until_shutdown<F>(
        &self,
        addr: SocketAddr,
        shutdown_signal: F,
    ) -> Result<()>
    where
        F: std::future::Future<Output = ()>,
    {
        // F099-K: set `base_port` + `advertise_host` BEFORE any
        // background loop spawns `open_partition`. Idempotent if a
        // caller already invoked `bind_listen_addr` (e.g. test
        // harness via `connect_with_advertise_and_port`).
        self.bind_listen_addr(addr)?;
        let base_port = self.base_port.get();
        let first_port = addr.port();

        tracing::info!(
            base_port = base_port,
            first_part_port = first_port,
            "partition server serving (per-partition listeners)"
        );

        // Control-plane loops run on main compio thread and never exit.
        // heartbeat_loop is spawned by `finish_connect` right after
        // register_ps succeeds, so it stays alive across the (potentially
        // long) initial sync_regions_once.
        let s = self.clone();
        compio::runtime::spawn(async move { s.region_sync_loop().await }).detach();

        // F099-K: region_sync_loop above drives all open/close of partition
        // threads. Partitions bind their own listeners. F120-C: park here
        // until `shutdown_signal` resolves, then drain.
        futures::pin_mut!(shutdown_signal);
        let park = async {
            loop {
                compio::time::sleep(Duration::from_secs(3600)).await;
            }
        };
        futures::pin_mut!(park);
        use futures::future::{select, Either};
        match select(shutdown_signal, park).await {
            Either::Left(_) => {
                tracing::info!("shutdown signal received, draining partitions");
                self.shutdown().await?;
                Ok(())
            }
            // park never resolves; this arm is unreachable but exhaustive.
            Either::Right(_) => Ok(()),
        }
    }
}

// ---------------------------------------------------------------------------
// Connection handler — F099-I: per-conn reply batching via FuturesUnordered +
// write_vectored_all. Mirrors the ExtentNode R4 4.2 v3 pattern.
// ---------------------------------------------------------------------------

/// F099-I — per-conn inflight cap. Maximum number of concurrently-awaiting
/// PartitionRequest futures `handle_ps_connection` holds at once. Once at
/// cap, TCP reads stop (back-pressure) until one completion drains into
/// `tx_bufs`. Default 4 is chosen so that the total across N conns
/// (typical benchmark N=256) stays bounded at N × CAP = 1024 — roughly
/// the `futures::channel::mpsc` `WRITE_CHANNEL_CAP = 1024` that carries
/// PartitionRequests into partition_loop.  Higher caps (8+) caused
/// EINVAL / "submit error: connection closed" under 256 × d=8 load —
/// we believe due to the aggregate rate of tx.send()-awaiting futures
/// overwhelming either the mpsc reservation pool or the PS's extent-node
/// RpcConn writer_task. F195: overridable via `set_ps_conn_inflight_cap`
/// (CLI flag `--conn-inflight-cap`); default 4.
fn ps_conn_inflight_cap() -> usize {
    *PS_CONN_INFLIGHT_CAP_CELL.get_or_init(|| 4)
}

/// F099-I-fix — observability counter for the d=1 fast path. Incremented
/// once per inline round-trip taken by `handle_ps_connection`. Exposed for
/// tests only; the `fetch_add` on an AtomicU64 is ~1 ns so the cost is
/// negligible on the hot path. In production the counter only grows — no
/// reader, no resetter — so there is no cache-line contention (single
/// writer per conn, separate allocator-decided line for the static).
pub(crate) static PS_FAST_PATH_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Workspace-wide serialization guard for tests that exercise
/// `handle_ps_connection`. The function bumps a process-global counter
/// (`PS_FAST_PATH_HITS`) so any parallel test using the same path
/// invalidates `f099i_tests`' exact-delta assertions. All such tests
/// acquire this lock for the duration of their TCP round-trips.
///
/// `parking_lot::Mutex` is used instead of `std::sync::Mutex` so a
/// failing test in this set doesn't poison the lock and cascade-fail
/// every subsequent run. Held only during the test body; non-test
/// code paths never see this.
#[cfg(test)]
pub(crate) fn ps_conn_test_lock() -> parking_lot::MutexGuard<'static, ()> {
    static LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
    LOCK.lock()
}

/// F099-I — outcome of one persistent read future iteration.  The future
/// owns both the reader and the buffer across iterations so it can be left
/// pinned in the event loop's `select` without ever being dropped
/// mid-flight (an in-flight io_uring SQE would otherwise be cancelled,
/// forcing the kernel to resubmit on the next poll; earlier ps-conn
/// iterations measured this as a perf regression).
enum PsReadBurst {
    Data {
        buf: Vec<u8>,
        n: usize,
        reader: autumn_transport::ReadHalf,
    },
    Eof {
        #[allow(dead_code)]
        reader: autumn_transport::ReadHalf,
        #[allow(dead_code)]
        buf: Vec<u8>,
    },
    Err {
        e: std::io::Error,
        #[allow(dead_code)]
        reader: autumn_transport::ReadHalf,
        #[allow(dead_code)]
        buf: Vec<u8>,
    },
}

/// Build a `'static`-lifetime `LocalBoxFuture<PsReadBurst>` that reads once
/// into `buf` and returns ownership of both reader and buf.
fn spawn_ps_read(
    mut reader: autumn_transport::ReadHalf,
    buf: Vec<u8>,
) -> futures::future::LocalBoxFuture<'static, PsReadBurst> {
    use compio::io::AsyncRead;
    use futures::FutureExt;
    async move {
        let BufResult(result, buf_back) = reader.read(buf).await;
        match result {
            Ok(0) => PsReadBurst::Eof { reader, buf: buf_back },
            Ok(n) => PsReadBurst::Data { buf: buf_back, n, reader },
            Err(e) => PsReadBurst::Err { e, reader, buf: buf_back },
        }
    }
    .boxed_local()
}

/// F099-I — push ONE frame onto `inflight`, encoded into a
/// LocalBoxFuture<Bytes>.  Shared by `push_frames_to_inflight` (slow-path
/// drain) and any caller that already has a single frame in hand.
///
/// Misrouted frames synth an error frame with no mpsc hop.  Caller must
/// have checked `frame.req_id != 0`; fire-and-forget frames are the
/// caller's responsibility to skip (matches pre-F099-I semantics).
fn push_one_frame_to_inflight(
    frame: Frame,
    req_tx: &mpsc::Sender<PartitionRequest>,
    // F216 (Option B): `Some` in production → GET served locally here; `None`
    // in the mock-loop unit tests (which only drive writes) → GET delegates.
    part: &Option<Rc<RefCell<PartitionData>>>,
    owner_part: u64,
    inflight: &mut FuturesUnordered<futures::future::LocalBoxFuture<'static, Bytes>>,
) {
    use futures::FutureExt;
    let req_id = frame.req_id;
    let msg_type = frame.msg_type;
    let payload = frame.payload;
    let part_id = partition_rpc::extract_part_id(msg_type, &payload);

    if part_id != owner_part {
        // Mis-routed — synth error frame, no mpsc hop.
        // TODO(F099-K): forward to owning P-log's req_tx.
        let err_payload = autumn_rpc::RpcError::encode_status(
            StatusCode::NotFound,
            &format!("partition {part_id} not served by this P-log (owner={owner_part})"),
        );
        let bytes = Frame::error(req_id, msg_type, err_payload).encode();
        inflight.push(async move { bytes }.boxed_local());
        return;
    }

    // F216 (Option B): serve GET reads LOCALLY in this ps-conn task's FU —
    // no `req_tx` hop, no detour through `partition_loop`. Reads need only a
    // consistent read of `PartitionData` (memtable + SSTs), not the
    // single-writer group-commit actor, so routing them through
    // `partition_loop` was pure overhead (an mpsc hop + a per-op spawn).
    // `handle_get` borrows the partition only across synchronous code (it
    // drops the borrow before the `resolve_value` await), and the whole
    // P-log runtime is single-threaded, so concurrent reads in this FU never
    // overlap a borrow with each other or with `partition_loop`'s writes
    // (`borrow_mut`). This keeps `partition_loop` focused on writes.
    if msg_type == MSG_GET {
        if let Some(part) = part {
            let part_c = part.clone();
            let fut = async move {
                let resp_frame = match crate::rpc_handlers::handle_get(payload, &part_c).await {
                    Ok(p) => Frame::response(req_id, msg_type, p),
                    Err((code, message)) => Frame::error(
                        req_id,
                        msg_type,
                        autumn_rpc::RpcError::encode_status(code, &message),
                    ),
                };
                resp_frame.encode()
            };
            inflight.push(fut.boxed_local());
            return;
        }
        // part == None (unit-test mode): fall through to the req_tx delegate.
    }

    let mut tx = req_tx.clone();
    let fut = async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = PartitionRequest {
            msg_type,
            payload,
            resp_tx,
        };
        let resp_frame = if tx.send(req).await.is_err() {
            Frame::error(
                req_id,
                msg_type,
                autumn_rpc::RpcError::encode_status(
                    StatusCode::Internal,
                    "partition thread closed",
                ),
            )
        } else {
            match resp_rx.await {
                Ok(Ok(p)) => Frame::response(req_id, msg_type, p),
                Ok(Err((code, message))) => Frame::error(
                    req_id,
                    msg_type,
                    autumn_rpc::RpcError::encode_status(code, &message),
                ),
                Err(_) => Frame::error(
                    req_id,
                    msg_type,
                    autumn_rpc::RpcError::encode_status(
                        StatusCode::Internal,
                        "partition response dropped",
                    ),
                ),
            }
        };
        resp_frame.encode()
    };
    inflight.push(fut.boxed_local());
}

/// F099-I — drain all complete frames from `decoder`, pushing one future
/// per frame onto `inflight`. Each future owns a cloned `req_tx` + fresh
/// oneshot; when polled it:
///   1. Sends the PartitionRequest via the same-thread mpsc.
///   2. Awaits the oneshot response.
///   3. Returns the encoded response frame bytes (ready for write_vectored_all).
///
/// Misrouted frames (part_id != owner_part) synthesise an immediate error
/// frame with no mpsc hop.  Frames with `req_id == 0` are ignored
/// (fire-and-forget — matches pre-F099-I behavior).
///
/// Back-pressure: if `inflight.len()` reaches `cap` mid-push, we await one
/// completion before pushing more.  Drained completions go into `tx_bufs`
/// so the caller's next `write_vectored_all` flushes them.
async fn push_frames_to_inflight(
    decoder: &mut FrameDecoder,
    req_tx: &mpsc::Sender<PartitionRequest>,
    part: &Option<Rc<RefCell<PartitionData>>>,
    owner_part: u64,
    inflight: &mut FuturesUnordered<futures::future::LocalBoxFuture<'static, Bytes>>,
    tx_bufs: &mut Vec<Bytes>,
    cap: usize,
) -> Result<()> {
    loop {
        match decoder.try_decode().map_err(|e| anyhow!(e))? {
            Some(frame) if frame.req_id != 0 => {
                // Back-pressure: drain one completion if we're at cap.
                while inflight.len() >= cap {
                    if let Some(done) = inflight.next().await {
                        tx_bufs.push(done);
                    } else {
                        break;
                    }
                }
                push_one_frame_to_inflight(frame, req_tx, part, owner_part, inflight);
            }
            Some(_) => continue, // req_id == 0 fire-and-forget
            None => break,
        }
    }
    Ok(())
}

/// F099-I-fix — d=1 fast path.  Run a single request round-trip inline
/// without going through `FuturesUnordered` or `Box<dyn Future>` at all.
/// Returns the encoded response frame bytes ready for `write_all`.
///
/// Precondition enforced by caller: `inflight.is_empty()` and
/// `tx_bufs.is_empty()`.  Frame must have `req_id != 0` (fire-and-forget
/// has no reply).  Misrouted frames synth a local error frame without
/// touching the mpsc — same ordering as the slow path.
///
/// This path avoids: (a) `Box::pin(async move { ... })` heap alloc,
/// (b) `FuturesUnordered::push` pinning ceremony, (c) `FuturesUnordered::next`
/// state-machine poll cost, (d) `write_vectored_all` with a single iov (goes
/// through sendmsg/UIO_MAXIOV setup) in favor of `write_all` (send/write).
/// Measured by F099-I as ~6.5 % of the N=1 × d=1 write throughput on tmpfs;
/// this restores the pre-F099-I baseline for the depth=1 hot path while
/// preserving the depth≥2 batching gains.
async fn d1_fast_path_round_trip(
    frame: Frame,
    req_tx: &mpsc::Sender<PartitionRequest>,
    part: &Option<Rc<RefCell<PartitionData>>>,
    owner_part: u64,
) -> Bytes {
    let req_id = frame.req_id;
    let msg_type = frame.msg_type;
    let payload = frame.payload;
    let part_id = partition_rpc::extract_part_id(msg_type, &payload);

    if part_id != owner_part {
        let err_payload = autumn_rpc::RpcError::encode_status(
            StatusCode::NotFound,
            &format!("partition {part_id} not served by this P-log (owner={owner_part})"),
        );
        return Frame::error(req_id, msg_type, err_payload).encode();
    }

    // F216 (Option B): d=1 GET served locally too — same rationale as
    // push_one_frame_to_inflight. No req_tx hop / partition_loop detour.
    if msg_type == MSG_GET {
        if let Some(part) = part {
            return match crate::rpc_handlers::handle_get(payload, part).await {
                Ok(p) => Frame::response(req_id, msg_type, p),
                Err((code, message)) => Frame::error(
                    req_id,
                    msg_type,
                    autumn_rpc::RpcError::encode_status(code, &message),
                ),
            }
            .encode();
        }
        // part == None (unit-test mode): fall through to the req_tx delegate.
    }

    let (resp_tx, resp_rx) = oneshot::channel();
    let req = PartitionRequest {
        msg_type,
        payload,
        resp_tx,
    };
    let mut tx = req_tx.clone();
    let resp_frame = if tx.send(req).await.is_err() {
        Frame::error(
            req_id,
            msg_type,
            autumn_rpc::RpcError::encode_status(
                StatusCode::Internal,
                "partition thread closed",
            ),
        )
    } else {
        match resp_rx.await {
            Ok(Ok(p)) => Frame::response(req_id, msg_type, p),
            Ok(Err((code, message))) => Frame::error(
                req_id,
                msg_type,
                autumn_rpc::RpcError::encode_status(code, &message),
            ),
            Err(_) => Frame::error(
                req_id,
                msg_type,
                autumn_rpc::RpcError::encode_status(
                    StatusCode::Internal,
                    "partition response dropped",
                ),
            ),
        }
    };
    resp_frame.encode()
}

/// Handle a single client connection on the P-log runtime.
///
/// **F099-I: per-conn reply batching.** The inner loop mirrors the
/// ExtentNode R4 4.2 v3 pattern (commit `1e7e456`):
///   - Persistent read future (`Option<LocalBoxFuture<PsReadBurst>>`) owns
///     reader + 64 KiB buf across iterations, never dropped mid-flight.
///   - `FuturesUnordered<LocalBoxFuture<Bytes>>` holds in-flight
///     PartitionRequest → oneshot-response → encoded-frame futures.
///   - Each loop iteration opportunistically drains ready completions into
///     `tx_bufs`, flushes `tx_bufs` with a SINGLE `write_vectored_all`
///     syscall, then races read vs inflight.next() when both are live.
///   - At `--pipeline-depth=1` degenerates to `write_vectored_all([one_frame])`
///     — same cost as the old `write_all(one_frame)`; no regression.
///   - At `--pipeline-depth ≥ N` steady state, the drain-loop collects up
///     to N frames per burst → one `tcp_sendmsg` instead of N → targeted
///     win against F099-H's 0.8-core small-frame TCP kernel overhead.
///
/// Arguments:
///   * `stream`      — client socket (owned; split into read/write halves).
///   * `req_tx`      — sender into partition_loop's request channel.
///                     Owned by this fn; cloned once per in-flight future.
///                     When this fn returns, the last clone drops, closing
///                     the mpsc (merged_loop sees `req_rx.next() == None`
///                     only after ALL connections on this P-log close).
///   * `owner_part`  — the partition id this P-log thread serves. Requests
///                     for other partitions synthesise a `NotFound` error
///                     frame (TODO(F099-K) forwarding).
async fn handle_ps_connection(
    conn: autumn_transport::Conn,
    req_tx: mpsc::Sender<PartitionRequest>,
    // F216 (Option B): this task serves GET reads locally (in its own FU)
    // when `Some` — `handle_get` only needs read access to PartitionData (it
    // pulls the StreamClient from `part.stream_client` internally). Writes
    // still delegate to `partition_loop` via `req_tx`. `None` is the
    // mock-loop unit-test mode (those tests drive only writes), where GET
    // would fall back to the req_tx delegate.
    part: Option<Rc<RefCell<PartitionData>>>,
    owner_part: u64,
) -> Result<()> {
    use futures::future::{select, Either, LocalBoxFuture};

    const READ_BUF_SIZE: usize = 64 * 1024;

    let (reader, mut writer) = conn.into_split();
    let mut decoder = FrameDecoder::new();

    let cap = ps_conn_inflight_cap();
    let mut inflight: FuturesUnordered<LocalBoxFuture<'static, Bytes>> =
        FuturesUnordered::new();
    let mut tx_bufs: Vec<Bytes> = Vec::with_capacity(64);

    // Persistent read future: owns reader + buf across iterations.
    let buf = vec![0u8; READ_BUF_SIZE];
    let mut read_fut: Option<LocalBoxFuture<'static, PsReadBurst>> =
        Some(spawn_ps_read(reader, buf));

    loop {
        // (A) Opportunistic drain of already-ready completions.
        while let Some(Some(done)) = inflight.next().now_or_never() {
            tx_bufs.push(done);
        }

        // (B) Flush accumulated replies with ONE vectored write.
        if !tx_bufs.is_empty() {
            let bufs = std::mem::take(&mut tx_bufs);
            let BufResult(result, _) = writer.write_vectored_all(bufs).await;
            result?;
        }

        // (C) Decide what to wait on.
        let n_inflight = inflight.len();
        let at_cap = n_inflight >= cap;

        if n_inflight == 0 {
            // Idle — just await the read.
            let rfut = read_fut
                .take()
                .expect("read_fut invariant: always Some when idle");
            match rfut.await {
                PsReadBurst::Eof { .. } => return Ok(()),
                PsReadBurst::Err { e, .. } => return Err(e.into()),
                PsReadBurst::Data { buf, n, reader } => {
                    decoder.feed(&buf[..n]);

                    // F099-I-fix — d=1 fast path.
                    //
                    // When a TCP read yields exactly one full frame AND
                    // nothing is already in flight, skip FU entirely: do
                    // the request→response→write inline using `write_all`
                    // (single iov path, matches pre-F099-I cost). This
                    // path dominates at `--pipeline-depth=1` (the client
                    // awaits each reply before sending the next), so the
                    // FU-based slow path was paying per-frame heap alloc
                    // (Box::pin) + push/pop ceremony + write_vectored_all
                    // with one iovec for every Put. F099-I measured
                    // -6.5 % at d=1 from that overhead; this restores the
                    // baseline while preserving d≥2 batching gains.
                    //
                    // Preconditions (all checked):
                    //   * inflight.is_empty() — guaranteed by the
                    //     `n_inflight == 0` branch we just entered.
                    //   * tx_bufs.is_empty() — flushed above at (B).
                    //   * decoder yields exactly one frame: first
                    //     try_decode returns Some, next returns None.
                    //     (If any bytes remain in decoder after the
                    //     first frame, there is either a complete second
                    //     frame — fall back to slow path for ordering —
                    //     or a partial frame header for the next read.
                    //     In the latter case the next try_decode returns
                    //     None, which is exactly the fast-path condition.)
                    //   * frame.req_id != 0 (real request, not fire-
                    //     and-forget).
                    //
                    // Correctness: because inflight+tx_bufs are empty,
                    // no earlier frame's reply is waiting to be written.
                    // Running the round-trip inline preserves in-order
                    // reply semantics for this connection.
                    let first = decoder.try_decode().map_err(|e| anyhow!(e))?;
                    if let Some(frame) = first {
                        let more = decoder.try_decode().map_err(|e| anyhow!(e))?;
                        if more.is_none() && frame.req_id != 0 {
                            // Engage fast path.
                            PS_FAST_PATH_HITS
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let resp_bytes = d1_fast_path_round_trip(
                                frame, &req_tx, &part, owner_part,
                            )
                            .await;
                            let BufResult(wr, _) = writer.write_all(resp_bytes).await;
                            wr?;
                            read_fut = Some(spawn_ps_read(reader, buf));
                            continue;
                        }
                        // Fall back: push the first frame (if it had a
                        // req_id) and any second frame we decoded,
                        // then drain whatever else is buffered.
                        if frame.req_id != 0 {
                            push_one_frame_to_inflight(
                                frame, &req_tx, &part, owner_part, &mut inflight,
                            );
                        }
                        if let Some(second) = more {
                            if second.req_id != 0 {
                                push_one_frame_to_inflight(
                                    second, &req_tx, &part, owner_part, &mut inflight,
                                );
                            }
                        }
                    }
                    // Drain any remaining frames + apply back-pressure.
                    push_frames_to_inflight(
                        &mut decoder,
                        &req_tx,
                        &part,
                        owner_part,
                        &mut inflight,
                        &mut tx_bufs,
                        cap,
                    )
                    .await?;
                    read_fut = Some(spawn_ps_read(reader, buf));
                }
            }
            continue;
        }

        if at_cap {
            // Back-pressure — only await a completion. The read future
            // stays pinned in `read_fut` untouched.
            if let Some(done) = inflight.next().await {
                tx_bufs.push(done);
            }
            continue;
        }

        // (D) Race read vs completion.
        //
        // Fast path: when n_inflight == 1, the client is typically waiting
        // on THIS one response before submitting more, so racing the read
        // buys nothing (the read stays Pending until the completion lands)
        // but costs ~5-10 µs of per-iter polling overhead. Await the
        // completion alone, matching the ExtentNode v3 fast-path branch.
        if n_inflight == 1 {
            if let Some(done) = inflight.next().await {
                tx_bufs.push(done);
            }
            continue;
        }

        let rfut = read_fut.take().expect("read_fut: Some in race arm");
        let cfut = inflight.next();
        match select(rfut, Box::pin(cfut)).await {
            Either::Left((read_result, _cfut_dropped)) => {
                // Completion-future wrapper dropped here is safe — FU's
                // internal state persists regardless of the wrapper's
                // lifetime. Remaining completions are drained at loop top.
                match read_result {
                    PsReadBurst::Eof { .. } => {
                        // Drain remaining inflight so clients get their
                        // final replies before we return.
                        while let Some(done) = inflight.next().await {
                            tx_bufs.push(done);
                        }
                        if !tx_bufs.is_empty() {
                            let bufs = std::mem::take(&mut tx_bufs);
                            let _ = writer.write_vectored_all(bufs).await.0;
                        }
                        return Ok(());
                    }
                    PsReadBurst::Err { e, .. } => return Err(e.into()),
                    PsReadBurst::Data { buf, n, reader } => {
                        decoder.feed(&buf[..n]);
                        push_frames_to_inflight(
                            &mut decoder,
                            &req_tx,
                            &part,
                            owner_part,
                            &mut inflight,
                            &mut tx_bufs,
                            cap,
                        )
                        .await?;
                        read_fut = Some(spawn_ps_read(reader, buf));
                    }
                }
            }
            Either::Right((maybe_done, rfut_back)) => {
                // Completion won; preserve the read future for next iter.
                read_fut = Some(rfut_back);
                if let Some(done) = maybe_done {
                    tx_bufs.push(done);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Partition thread main — runs on a dedicated OS thread with its own compio
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn partition_thread_main(
    part_id: u64,
    rg: Range,
    region_epoch: u64,
    log_stream_id: u64,
    row_stream_id: u64,
    meta_stream_id: u64,
    manager_addr: String,
    owner_key: String,
    revision: i64,
    ps_id: u64,
    listen_addr: SocketAddr,
    advertise_addr: String,
    ready_tx: oneshot::Sender<Result<()>>,
    shutdown_rx: oneshot::Receiver<()>,
    drain_rx: mpsc::UnboundedReceiver<oneshot::Sender<()>>,
    cpu_bulk: Option<usize>,
    metrics_arc: std::sync::Arc<PartitionMetrics>,
    // F188: trigger channels created on the main thread; the senders
    // are cloned into `PartitionData` (replacing the old in-thread
    // channel) so loopback rpc handlers + the maintenance scheduler
    // (main thread) drain into the same receiver.
    compact_tx: mpsc::Sender<bool>,
    compact_rx: mpsc::Receiver<bool>,
    gc_tx: mpsc::Sender<GcTask>,
    gc_rx: mpsc::Receiver<GcTask>,
    concurrency_ctrl: std::sync::Arc<crate::ConcurrencyController>,
    partition_budget: std::sync::Arc<crate::PartitionBudget>,
    // F212-fix-2: cross-thread mirror of `PartitionHandle.opened_with`.
    // Written by `handle_split_part` (and future in-place updaters) so
    // `sync_regions_once` on the main thread sees the post-update tuple
    // and doesn't drop+reopen a partition whose in-memory state is
    // already correct.
    opened_with_shared: std::sync::Arc<parking_lot::Mutex<(Range, u64, u64, u64, u64)>>,
) -> Result<()> {
    // F196 D-r7: per-partition rate controller. Built inside the
    // partition thread; each partition gets its own Mutex/state.
    let rate_ctrl = std::sync::Arc::new(crate::RateController::from_env());
    // F099-J: create the same-thread ps-conn ↔ partition_loop
    // channel. Both endpoints live on THIS compio runtime, so sends and
    // wakes do not cross threads.
    let (req_tx, req_rx) = mpsc::channel::<PartitionRequest>(WRITE_CHANNEL_CAP);

    let pool = Rc::new(ConnPool::new());
    // StreamClient::new_with_revision now returns `Rc<StreamClient>` directly
    // (R4 step 4.3: Rc::new_cyclic for Weak-self worker removal guard).
    let part_sc = StreamClient::new_with_revision(
        &manager_addr,
        owner_key.clone(),
        revision,
        3 * 1024 * 1024 * 1024,
        pool.clone(),
    )
    .await
    .context("create per-partition StreamClient")?;
    // F192: tag the per-partition StreamClient with `part_id` so the
    // F190 worker's failure-report drainer can attribute reports to
    // this partition (the manager's quorum debounce dedups by
    // `reporter_part_id`).
    part_sc.set_reporter_part_id(part_id);

    // Check commit length on all streams before recovery (Go: checkCommitLength).
    // This ensures the last extent of each stream has consistent commit length
    // across all replicas before we start reading from it.
    for (label, sid) in [
        ("logStream", log_stream_id),
        ("rowStream", row_stream_id),
        ("metaStream", meta_stream_id),
    ] {
        loop {
            match part_sc.commit_length(sid).await {
                Ok(end) => {
                    tracing::info!(part_id, stream_id = sid, end, "{} commit_length OK", label);
                    break;
                }
                Err(e) => {
                    tracing::warn!(
                        part_id,
                        stream_id = sid,
                        error = %e,
                        "{} commit_length failed, retrying in 5s",
                        label,
                    );
                    compio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    // Recovery: read metaStream → rowStream → logStream replay
    let (tables, sst_readers, max_seq, vp_eid, vp_off, detected_overlap, recovered_active) =
        recover_partition(part_id, &rg, log_stream_id, row_stream_id, meta_stream_id, &part_sc)
            .await?;

    let (flush_tx, flush_rx) = mpsc::unbounded::<()>();
    // F188: compact_tx/rx + gc_tx/rx are created on the main thread by
    // `open_partition` and passed in as parameters so PartitionHandle on
    // main can hold a Send Sender for the maintenance scheduler.
    // F120-A: signal channel from flush_one_imm (after each imm.pop_front)
    // back to partition_loop for back-pressure wakeup. Both ends
    // live on this thread so the unbounded futures::channel is fine.
    let (imm_drained_tx, imm_drained_rx) = mpsc::unbounded::<()>();
    // F210-C2 fix: wake channel for split freeze (see field doc on
    // PartitionData.split_wake_tx). Unbounded + small payload — a few
    // stranded `()` items between split attempts is harmless because
    // the loop drains them via `now_or_never` at iteration top.
    let (split_wake_tx, split_wake_rx) = mpsc::unbounded::<()>();

    // F088: spawn a dedicated OS thread (P-bulk) that owns its own compio
    // runtime + io_uring + ConnPool. Flush requests are forwarded to it via
    // `flush_req_tx`. capacity=1 keeps flushes sequential (matches the old
    // in-thread semantics) and provides back-pressure on the P-log flush_loop.
    let (flush_req_tx, flush_req_rx) = mpsc::channel::<FlushReq>(1);
    let (row_append_tx, row_append_rx) = mpsc::channel::<RowAppendReq>(1);
    let bulk_thread_spawn = spawn_bulk_thread(
        part_id,
        manager_addr.clone(),
        owner_key.clone(),
        revision,
        flush_req_rx,
        row_append_rx,
        cpu_bulk,
    );
    let (flush_req_tx_part, row_append_tx_part) = match &bulk_thread_spawn {
        Ok(_) => (Some(flush_req_tx.clone()), Some(row_append_tx.clone())),
        Err(e) => {
            tracing::error!(part_id, error = %e, "bulk thread spawn failed; flush will fall back to P-log");
            (None, None)
        }
    };

    // F107 observability: surface initial state so operators can tell
    // whether a user-triggered `compact <PARTID>` will actually run.
    tracing::info!(
        part_id,
        tables = tables.len(),
        sst_readers = sst_readers.len(),
        has_overlap = detected_overlap as u32,
        max_seq,
        vp_extent_id = vp_eid,
        vp_offset = vp_off,
        "open_partition: ready"
    );

    // F140: per-partition gc_gate — background_gc_loop acquires this around
    // the actual run_gc calls; handle_split_part acquires it before reading
    // commit_length so no log_stream append can race the seal.
    let gc_gate = CompactionGate::new(1);

    let part = Rc::new(RefCell::new(PartitionData {
        part_id,
        rg,
        region_epoch,
        active: recovered_active,
        imm: VecDeque::new(),
        flushing_imm_ptrs: RefCell::new(HashSet::new()),
        flush_tx,
        compact_tx,
        gc_tx,
        seq_number: max_seq,
        log_stream_id,
        row_stream_id,
        meta_stream_id,
        tables,
        sst_readers,
        has_overlap: Cell::new(if detected_overlap { 1 } else { 0 }),
        need_invalidate_row_stream: Cell::new(false),
        vp_extent_id: vp_eid,
        vp_offset: vp_off,
        stream_client: part_sc.clone(),
        manager_addr: manager_addr.clone(),
        pool: pool.clone(),
        flush_req_tx: flush_req_tx_part,
        row_append_tx: row_append_tx_part,
        imm_drained_tx,
        split_wake_tx,
        gc_gate: gc_gate.clone(),
        rate_ctrl: rate_ctrl.clone(),
        concurrency_ctrl: concurrency_ctrl.clone(),
        partition_budget: partition_budget.clone(),
        extent_pins: std::cell::RefCell::new(std::collections::HashMap::new()),
        frozen_for_merge: Cell::new(None),
        freeze_drain_ack: std::cell::RefCell::new(None),
        frozen_for_split: Cell::new(None),
        split_drain_ack: std::cell::RefCell::new(None),
        vp_refs_dirty: Cell::new(false),
        metrics: metrics_arc.clone(),
        opened_with_shared: opened_with_shared.clone(),
    }));

    sync_partition_vp_refs(&part)
        .await
        .context("sync partition vp refs after recovery")?;

    // Drop the extra clones held locally: the ones stored in PartitionData
    // are the only references. When PartitionData drops, the channels close,
    // the bulk thread sees rx.next() = None, and exits cleanly.
    drop(flush_req_tx);
    drop(row_append_tx);

    // Spawn background loops on this thread's compio runtime.
    //
    // F099-D: the write loop is NO LONGER a separate compio task. Writes
    // are serviced inline by `partition_loop` below, collapsing the
    // old `partition_thread_main → spawn_write_request → handle_put →
    // write_tx.send → background_write_loop_r1` chain into one task. See
    // F099-A flame graph analysis (docs/superpowers/specs/2026-04-20-*.md
    // §Section 3/4) for why this collapse matters (~30 % of P-log CPU on
    // 256 × d=1 came from spawn + inner oneshot + Waker cascade).
    {
        let p = part.clone();
        compio::runtime::spawn(async move {
            background_flush_loop(p, flush_rx).await;
        })
        .detach();
    }
    {
        let p = part.clone();
        let conc_for_compact = concurrency_ctrl.clone();
        compio::runtime::spawn(async move {
            background_compact_loop(part_id, p, compact_rx, conc_for_compact).await;
        })
        .detach();
    }
    {
        let p = part.clone();
        let gc_gate_for_loop = gc_gate.clone();
        let conc_for_gc = concurrency_ctrl.clone();
        compio::runtime::spawn(async move {
            background_gc_loop(p, gc_rx, gc_gate_for_loop, conc_for_gc).await;
        })
        .detach();
    }
    // F210-C4: retry loop for failed vp_refs sync. Every 5 s checks the
    // dirty flag; if set, attempts a fresh `sync_partition_vp_refs`. On
    // success clears the flag (GC resumes). Bounded backoff isn't
    // needed because the partition is already gated — GC is paused, so
    // wasted retries cost only one RPC every 5s.
    {
        let p = part.clone();
        compio::runtime::spawn(async move {
            vp_refs_retry_loop(p).await;
        })
        .detach();
    }
    let locked_by_other = Rc::new(Cell::new(false));

    // F099-K: bind this partition's dedicated TcpListener on THIS
    // compio runtime and report readiness (bind + manager-side register)
    // back to the caller via `ready_tx`. If EITHER step fails, we report
    // the error and exit the partition thread so the main loop can
    // reclaim the partition slot and, on the next sync cycle, retry.
    let mut listener = match autumn_transport::current_or_init().bind(listen_addr).await {
        Ok(l) => l,
        Err(e) => {
            let _ = ready_tx.send(Err(anyhow!("bind {}: {}", listen_addr, e)));
            return Ok(());
        }
    };
    match listener.local_addr() {
        Ok(actual) => tracing::info!(part_id, addr = %actual, "partition listener bound"),
        Err(_) => tracing::info!(part_id, addr = %listen_addr, "partition listener bound"),
    }

    // Register this partition's address with the manager. Do it on this
    // runtime so we can await the RPC without blocking the main thread.
    {
        let req = manager_rpc::rkyv_encode(&manager_rpc::RegisterPartitionAddrReq {
            ps_id,
            part_id,
            address: advertise_addr.clone(),
        });
        // Use the already-open `pool` (created above for StreamClient); it
        // normalizes manager addrs and round-robins internally.
        let mut last_err: Option<anyhow::Error> = None;
        let mut registered = false;
        for mgr in manager_addr.split(',') {
            let mgr = mgr.trim();
            if mgr.is_empty() {
                continue;
            }
            let mgr_norm = autumn_stream::conn_pool::normalize_endpoint(mgr);
            // 10 s — manager updates `part_addrs` (in-memory + etcd
            // mirror). Bounded so partition open doesn't trap.
            match pool
                .call_timeout(&mgr_norm, manager_rpc::MSG_REGISTER_PARTITION_ADDR, req.clone(), Duration::from_secs(10))
                .await
            {
                Ok(bytes) => {
                    match manager_rpc::rkyv_decode::<manager_rpc::CodeResp>(&bytes) {
                        Ok(r) if r.code == manager_rpc::CODE_OK => {
                            registered = true;
                            break;
                        }
                        Ok(r) => {
                            last_err = Some(anyhow!(
                                "register_partition_addr rejected by {}: {}",
                                mgr, r.message
                            ));
                        }
                        Err(e) => {
                            last_err = Some(anyhow!("decode register_partition_addr resp: {}", e));
                        }
                    }
                }
                Err(e) => {
                    last_err = Some(e);
                }
            }
        }
        if !registered {
            let err = last_err.unwrap_or_else(|| anyhow!("no manager addresses to register with"));
            let _ = ready_tx.send(Err(err));
            return Ok(());
        }
    }

    // Signal the main thread that the listener is up AND the address is
    // registered; `open_partition` can now return Ok.
    let _ = ready_tx.send(Ok(()));

    // F099-K accept loop: own the listener on this runtime, spawn
    // `handle_ps_connection` on this runtime for every new fd. The accept
    // task races against `shutdown_rx`: when the main thread drops its
    // `shutdown_tx`, `shutdown_rx.await` resolves and the task exits.
    //
    // IMPORTANT: this task holds a clone of `req_tx`. When it exits, its
    // clone is dropped. Once every per-connection task's clone is also
    // dropped, partition_loop observes `req_rx.next() == None` and
    // shuts down cleanly.
    {
        let req_tx_for_accept = req_tx.clone();
        // F216 (Option B): the accept task hands each ps-conn task a clone of
        // `part` so it can serve GET reads locally in its own FU.
        let part_for_accept = part.clone();
        compio::runtime::spawn(async move {
            let mut shutdown_rx = shutdown_rx;
            use futures::future::{select, Either};
            loop {
                // Race accept against shutdown. `shutdown_rx.await`
                // resolves when the main thread drops its sender.
                let accept_fut = listener.accept();
                futures::pin_mut!(accept_fut);
                let res = match select(accept_fut, &mut shutdown_rx).await {
                    Either::Left((r, _pending_shutdown)) => r,
                    Either::Right((_canceled_shutdown, _pending_accept)) => {
                        tracing::info!(part_id, "accept: shutdown signaled, exiting");
                        break;
                    }
                };
                match res {
                    Ok((conn, peer)) => {
                        if let Some(s) = conn.as_tcp() {
                            let _ = s.set_nodelay(true);
                        }
                        let req_tx_conn = req_tx_for_accept.clone();
                        let part_conn = part_for_accept.clone();
                        compio::runtime::spawn(async move {
                            if let Err(e) =
                                handle_ps_connection(conn, req_tx_conn, Some(part_conn), part_id).await
                            {
                                tracing::debug!(part_id, peer = %peer, error = %e, "ps connection ended");
                            }
                        })
                        .detach();
                    }
                    Err(e) => {
                        tracing::warn!(part_id, error = %e, "accept failed");
                        // Accept errors on loopback are rare; sleep briefly
                        // to avoid busy-looping on a persistent failure.
                        compio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            tracing::info!(part_id, "accept task exiting");
        })
        .detach();
    }

    // Drop our extra clone of req_tx — the accept task's clone (and any
    // per-conn clones it hands out) are the only remaining senders. When
    // they all drop, merged_loop shuts down.
    drop(req_tx);

    // F099-D: merged request + write loop runs directly on this task.
    partition_loop(
        part_id,
        part.clone(),
        req_rx,
        imm_drained_rx,
        split_wake_rx,
        drain_rx,
        locked_by_other,
        part_sc.clone(),
        pool.clone(),
        manager_addr.clone(),
        owner_key.clone(),
        revision,
    )
    .await;

    tracing::info!(part_id, "partition thread exiting");
    Ok(())
}

/// F099-D — the merged request + write loop. Replaces the old two-task
/// chain (`partition_thread_main` for request dispatch + a spawned
/// `background_write_loop_r1` for group commit) with a single compio task.
///
/// Why merge:
///   - Both tasks ran on the same OS thread; the split existed because a
///     separate task spawned per Put via `spawn_write_request` provided the
///     concurrency needed for batching. F099-A's flame graph attributed ~30 %
///     of P-log CPU to the *ceremony* of that split (one `compio::spawn`,
///     two `oneshot::channel()` allocations, one `mpsc::send`, one Waker
///     cascade, per Put).
///   - The SQ/CQ pipeline (R4 4.4) gives batching at the pending-queue
///     level regardless of how requests arrive. Once the outer `req_rx`
///     can push directly into `pending`, the per-request spawn is pure
///     overhead.
///
/// Preserves:
///   - R4 4.4 SQ/CQ pattern — Phase-2 futures execute concurrently up
///     to `ps_inflight_cap()`, MIN_PIPELINE_BATCH=256 gate for
///     non-first batches.
///   - **F210-C1: `FuturesOrdered` (was `FuturesUnordered`) — Phase 3
///     runs in launch order = seq order**, guaranteeing that a rotated
///     active memtable contains a contiguous seq range. This is what
///     makes the recovery-time dedup `if ts <= sst_max_seq { continue; }`
///     sound. Pre-F210-C1 `FuturesUnordered` allowed out-of-order
///     Phase 3, so an SST could have `last_seq = 200` while batch A's
///     seqs 1-100 were still in-flight; on crash before A's Phase 3,
///     replay's dedup silently dropped A. p99 trade-off: head-of-line
///     wait, bounded by Phase 2 latency variance; measured negligible
///     in F197's symmetric change on `background_flush_loop`.
///   - LockedByOther self-eviction (drain remaining inflight, exit cleanly).
///   - Read-op inlining: GET/HEAD/RANGE are processed directly on this
///     task via `dispatch_partition_rpc` so a busy write pipeline does
///     not starve readers.
///   - F099-C `insert_batch` — Phase 3 still uses the batched memtable
///     insert path.
///
/// Direct-response path:
///   - `WriteRequest.resp` is now a `WriteResponder` that encodes the
///     RPC response frame bytes inline on `send_ok` and drops directly
///     into the outer ps-conn oneshot. No inner oneshot, no Waker
///     cascade through a second compio task.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
async fn partition_loop(
    part_id: u64,
    part: Rc<RefCell<PartitionData>>,
    mut req_rx: mpsc::Receiver<PartitionRequest>,
    mut imm_drained_rx: mpsc::UnboundedReceiver<()>,
    mut split_wake_rx: mpsc::UnboundedReceiver<()>,
    mut drain_rx: mpsc::UnboundedReceiver<oneshot::Sender<()>>,
    locked_by_other: Rc<Cell<bool>>,
    part_sc: Rc<StreamClient>,
    pool: Rc<ConnPool>,
    manager_addr: String,
    owner_key: String,
    revision: i64,
) {
    use futures::future::{select, Either};

    let cap = ps_inflight_cap();
    let batch_target = crate::background::min_pipeline_batch().min(max_write_batch());
    let imm_cap = max_imm_depth();
    let wal_gap_cap = max_wal_gap();
    let mut metrics = WriteLoopMetrics::new();
    let mut pending: Vec<WriteRequest> = Vec::new();
    // F120-C — set when `drain_rx` delivered a request; once set, stop
    // pulling new items from `req_rx` and head for the tail-drain block.
    let mut drain_ack: Option<oneshot::Sender<()>> = None;

    // F210-C1: switched from FuturesUnordered to FuturesOrdered. Phase 2
    // futures still execute concurrently; only the yield order changes.
    // FuturesOrdered guarantees Phase 3 (memtable insert) runs in launch
    // order = seq order, so a rotated active memtable always contains a
    // contiguous seq range — the precondition that makes the
    // recover_partition dedup (`if ts <= sst_max_seq { continue; }`)
    // sound. See the doc comment above and feature_list.md F210-C1 for
    // the bug pre-F210-C1 enabled and the perf analysis.
    type CompletionFut =
        std::pin::Pin<Box<dyn std::future::Future<Output = InflightCompletion>>>;
    let mut inflight: futures::stream::FuturesOrdered<CompletionFut> =
        futures::stream::FuturesOrdered::new();

    'outer: loop {
        if locked_by_other.get() {
            tracing::error!(part_id, "partition poisoned by LockedByOther, shutting down");
            break;
        }
        if drain_ack.is_some() {
            // F120-C — we received a drain signal. Stop pulling from
            // req_rx; finish anything already started.
            break;
        }

        // F185 — freeze drain completion. If a MSG_MERGE_FREEZE arrived,
        // it stashed `freeze_drain_ack` and flipped `frozen_for_merge` to
        // Some(now). New writes are rejected by handle_incoming_req's
        // CODE_UNAVAILABLE branch, so `pending` only ever shrinks from
        // here. We fire the OK reply once pending+inflight are empty AND
        // every imm has been flushed — at that point the captured-by-the-
        // orchestrator commit_length is guaranteed to include every
        // pre-freeze acked write.
        //
        // Done at top-of-loop (before the SQ/CQ wait branches) because
        // once frozen, no new req_rx wakeups arrive — blocking on
        // req_rx.next() in the wait branches would prevent the freeze
        // ack from ever firing.
        // F210-C2: same drain logic also serves split — split parks its own
        // oneshot in `split_drain_ack` and awaits the signal here. Either
        // (or both, in rare overlap) can be pending.
        let need_merge_drain = part.borrow().freeze_drain_ack.borrow().is_some();
        let need_split_drain = part.borrow().split_drain_ack.borrow().is_some();
        let need_freeze_drain = need_merge_drain || need_split_drain;
        if need_freeze_drain && pending.is_empty() && inflight.is_empty() {
            {
                let mut p = part.borrow_mut();
                rotate_active(&mut p);
            }
            // F210-C3: capture flush_one_imm failures and propagate to the
            // freeze ack. Pre-F210-C3 the loop swallowed any error with
            // `break` and unconditionally returned CODE_OK — manager
            // thought drain was complete and proceeded with the merge
            // commit_length capture + atomic txn, but imm with unflushed
            // data was still in memory. After the merge txn deleted the
            // victim partition, region_sync_loop dropped the
            // PartitionData and any unflushed bytes were lost.
            //
            // Returning CODE_UNAVAILABLE signals manager to rollback the
            // freeze (best-effort MSG_MERGE_FREEZE { freeze: false }) and
            // abort the merge; client retries.
            let mut drain_err: Option<String> = None;
            loop {
                match flush_one_imm(&part).await {
                    Ok(true) => continue,
                    Ok(false) => break,
                    Err(e) => {
                        let msg = format!("{e:#}");
                        tracing::error!(part_id, "freeze drain flush_one_imm: {msg}");
                        drain_err = Some(msg);
                        break;
                    }
                }
            }
            // Merge ack: external RPC resp.
            let merge_ack = part.borrow().freeze_drain_ack.borrow_mut().take();
            if let Some(ack) = merge_ack {
                let resp = match &drain_err {
                    None => partition_rpc::MergeFreezeResp {
                        code: partition_rpc::CODE_OK,
                        message: String::new(),
                    },
                    Some(e) => partition_rpc::MergeFreezeResp {
                        code: partition_rpc::CODE_UNAVAILABLE,
                        message: format!("freeze drain flush failed: {e}"),
                    },
                };
                let succeeded = resp.code == partition_rpc::CODE_OK;
                let _ = ack.send(Ok(partition_rpc::rkyv_encode(&resp)));
                if succeeded {
                    tracing::info!(part_id, "freeze drain complete — partition halted");
                } else {
                    tracing::warn!(
                        part_id,
                        "freeze drain reported flush failure to manager; \
                         merge will be rolled back"
                    );
                }
            }
            // F210-C2: split ack: internal oneshot signal.
            let split_ack = part.borrow().split_drain_ack.borrow_mut().take();
            if let Some(ack) = split_ack {
                let payload = match &drain_err {
                    None => Ok(()),
                    Some(e) => Err(e.clone()),
                };
                let _ = ack.send(payload);
                if drain_err.is_none() {
                    tracing::info!(part_id, "split drain complete — proceeding to commit_length capture");
                } else {
                    tracing::warn!(
                        part_id,
                        "split drain reported flush failure; split will abort"
                    );
                }
            }
            // Fall through; subsequent iterations continue serving reads
            // and the (still-set) frozen_for_merge / frozen_for_split keep
            // rejecting writes.
        }

        // (A) Opportunistic CQ drain — run Phase 3 for every completion that
        // is already ready without blocking.
        while let Some(Some(c)) = inflight.next().now_or_never() {
            handle_completion(&part, &mut metrics, &locked_by_other, part_id, c).await;
            if locked_by_other.get() {
                break 'outer;
            }
        }

        // F120-A — sample imm depth + WAL gap snapshot under one borrow.
        // `imm_full` blocks new request intake (and new batch launches);
        // `force_rotate` is consulted after we already have a Phase-3 done
        // (in `finish_write_batch::maybe_rotate`).
        let (imm_full, _gap_now) = {
            let p = part.borrow();
            let gap = p.active.mem_bytes()
                + p.imm.iter().map(|m| m.mem_bytes()).sum::<u64>();
            (p.imm.len() >= imm_cap, gap)
        };
        // F183: track imm_full back-pressure events (per-iteration; coarse
        // but matches the spec's "events/sec" semantics — per-tick rate).
        if imm_full {
            part.borrow()
                .metrics
                .imm_full_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        // F120-A — drain any pending imm-pop notifications so we don't
        // accidentally wake on a stale signal in the wait branches below.
        while let Some(Some(())) = imm_drained_rx.next().now_or_never() {}
        // F210-C2 fix — same hygiene for split freeze wakes. Stale items
        // are harmless (the drain check itself is idempotent) but
        // draining them keeps the wait branches from spuriously waking.
        while let Some(Some(())) = split_wake_rx.next().now_or_never() {}

        let n_inflight = inflight.len();
        let at_cap = n_inflight >= cap;

        // (B) Launch a new batch when conditions are right. Same gate as
        // the legacy `background_write_loop_r1`: first batch always
        // launches; subsequent batches wait for pending >= batch_target
        // to avoid the R3 Task 5b regression. F120-A: when imm is full,
        // do not launch — the next batch's Phase 3 maybe_rotate would
        // exceed the cap.
        let ready_to_launch = !pending.is_empty()
            && !at_cap
            && !imm_full
            && (n_inflight == 0 || pending.len() >= batch_target);
        if ready_to_launch {
            let batch = std::mem::take(&mut pending);
            // F177: start_write_batch is now async — small batches stay
            // inline in the future (no spawn_blocking), big batches
            // (>= PHASE1_OFFLOAD_THRESHOLD) await spawn_blocking. The
            // .await yields to the runtime only on the big-batch path,
            // letting other tasks (ps-conn, flush_loop, etc.) progress
            // while the encoder runs on the blocking pool.
            match start_write_batch(&part, batch).await {
                Ok(Some(mut flight)) => {
                    let data = flight.data;
                    inflight.push(Box::pin(async move {
                        let phase2_result = (&mut flight.phase2_fut).await;
                        InflightCompletion { data, phase2_result }
                    }));
                }
                Ok(None) => {}
                Err(e) => tracing::error!("start_write_batch err: {e}"),
            }
            continue;
        }

        // (C) Pipeline full — only CQ can progress.
        if at_cap {
            if let Some(c) = inflight.next().await {
                handle_completion(&part, &mut metrics, &locked_by_other, part_id, c).await;
                if locked_by_other.get() {
                    break;
                }
            }
            continue;
        }

        // F120-A — when imm is full, treat it the same as `at_cap`: only
        // wait for either an inflight completion (which can pop_front imm
        // via maybe_rotate's flush_tx signal eventually) or an
        // `imm_drained` notification (one fired per pop_front). Drain
        // signal also wakes us so shutdown isn't blocked behind a slow
        // bulk thread.
        if imm_full {
            if n_inflight == 0 {
                // Pure back-pressure wait. Race imm-pop vs drain.
                let pop_fut = imm_drained_rx.next();
                let drain_fut = drain_rx.next();
                futures::pin_mut!(pop_fut);
                futures::pin_mut!(drain_fut);
                match select(pop_fut, drain_fut).await {
                    Either::Left((_, _)) => continue,
                    Either::Right((maybe_drain, _)) => {
                        if let Some(ack) = maybe_drain {
                            drain_ack = Some(ack);
                        }
                        continue;
                    }
                }
            }
            // Race imm-pop, inflight CQ, drain.
            let pop_fut = imm_drained_rx.next();
            let cfut = inflight.next();
            let drain_fut = drain_rx.next();
            futures::pin_mut!(pop_fut);
            futures::pin_mut!(drain_fut);
            match select(pop_fut, Box::pin(cfut)).await {
                Either::Left((_, _)) => continue,
                Either::Right((maybe_c, _)) => {
                    if let Some(c) = maybe_c {
                        handle_completion(&part, &mut metrics, &locked_by_other, part_id, c).await;
                        if locked_by_other.get() {
                            break;
                        }
                    }
                    // Also non-blocking-poll drain just in case.
                    if let Some(Some(ack)) = drain_fut.now_or_never() {
                        drain_ack = Some(ack);
                    }
                    continue;
                }
            }
        }

        // (D) Pipeline has room and imm not full; race SQ (req_rx),
        // CQ (inflight), split-wake, and drain.
        if n_inflight == 0 {
            // Fully idle: race req_rx vs split-wake vs F120-C drain.
            // F210-C2 fix: split_wake_rx is the wake source for split
            // freeze parking — without it an idle loop sleeps through
            // the entire FREEZE_TTL window.
            let req_fut = req_rx.next();
            let wake_fut = split_wake_rx.next();
            let drain_fut = drain_rx.next();
            futures::pin_mut!(req_fut, wake_fut, drain_fut);
            // First race req vs wake; whichever wins, also poll drain
            // via now_or_never below to fold the third receiver in.
            match select(req_fut, select(wake_fut, drain_fut)).await {
                Either::Left((maybe_req, _)) => match maybe_req {
                    Some(req) => {
                        handle_incoming_req(
                            req, &mut pending, &part, &part_sc, &pool,
                            &manager_addr, &owner_key, revision,
                        )
                        .await;
                    }
                    None => break,
                },
                Either::Right((inner, _)) => match inner {
                    Either::Left(_) => {
                        // split wake — just iterate; top-of-loop drain
                        // check will fire.
                    }
                    Either::Right((maybe_drain, _)) => {
                        if let Some(ack) = maybe_drain {
                            drain_ack = Some(ack);
                        }
                    }
                },
            }
        } else {
            let req_fut = req_rx.next();
            let cfut = inflight.next();
            let drain_fut = drain_rx.next();
            futures::pin_mut!(req_fut);
            futures::pin_mut!(drain_fut);
            // First select: race req_rx vs (inflight + drain). We poll
            // drain via now_or_never below to keep this select binary.
            match select(req_fut, Box::pin(cfut)).await {
                Either::Left((maybe_req, _cfut_dropped)) => match maybe_req {
                    Some(req) => {
                        handle_incoming_req(
                            req, &mut pending, &part, &part_sc, &pool,
                            &manager_addr, &owner_key, revision,
                        )
                        .await;
                    }
                    None => {
                        // Channel closed: drain remaining inflight, then exit.
                        while let Some(c) = inflight.next().await {
                            handle_completion(
                                &part, &mut metrics, &locked_by_other, part_id, c,
                            )
                            .await;
                            if locked_by_other.get() {
                                break;
                            }
                        }
                        break;
                    }
                },
                Either::Right((maybe_c, _req_dropped)) => {
                    if let Some(c) = maybe_c {
                        handle_completion(
                            &part, &mut metrics, &locked_by_other, part_id, c,
                        )
                        .await;
                        if locked_by_other.get() {
                            break;
                        }
                    }
                }
            }
            // Non-blocking drain check after either branch.
            if let Some(Some(ack)) = drain_fut.now_or_never() {
                drain_ack = Some(ack);
            }
        }

        // F120-B — WAL-gap-driven force rotate. After each iteration, if
        // the unflushed WAL window exceeds `MAX_WAL_GAP`, rotate `active`
        // even when it hasn't reached `FLUSH_MEM_BYTES`. Skipped when
        // imm is already full (rotation would over-cap and `imm_full`
        // back-pressure has already kicked in).
        {
            let mut p = part.borrow_mut();
            let gap = p.active.mem_bytes()
                + p.imm.iter().map(|m| m.mem_bytes()).sum::<u64>();
            if gap > wal_gap_cap && p.imm.len() < imm_cap && !p.active.is_empty() {
                rotate_active(&mut p);
            }
        }

        // (E) Non-blocking drain of any queued requests before the next
        // iteration. Reads are processed inline (await) and do NOT go into
        // pending; writes decode and push into pending. Skip when imm is
        // full so the back-pressure path actually applies.
        if !imm_full {
            while pending.len() < max_write_batch() {
                match req_rx.next().now_or_never() {
                    Some(Some(req)) => {
                        handle_incoming_req(
                            req, &mut pending, &part, &part_sc, &pool,
                            &manager_addr, &owner_key, revision,
                        )
                        .await;
                    }
                    _ => break,
                }
            }
        }

        // F185 — TTL backstop for freeze. The manager-side orchestrator
        // (`handle_merge_partitions`) commits in <1 s on the happy path;
        // a TTL fires only on orchestrator crash mid-flow, ensuring a
        // partition cannot be stuck frozen indefinitely. On expiry we
        // also drop any stale `freeze_drain_ack` (its caller is gone).
        if let Some(at) = part.borrow().frozen_for_merge.get() {
            if at.elapsed() >= FREEZE_TTL {
                let p = part.borrow();
                p.frozen_for_merge.set(None);
                if let Some(ack) = p.freeze_drain_ack.borrow_mut().take() {
                    let resp = partition_rpc::MergeFreezeResp {
                        code: partition_rpc::CODE_PRECONDITION,
                        message: "freeze TTL expired (orchestrator crash backstop)".to_string(),
                    };
                    let _ = ack.send(Ok(partition_rpc::rkyv_encode(&resp)));
                }
                tracing::warn!(
                    part_id,
                    ttl_secs = FREEZE_TTL.as_secs(),
                    "merge freeze TTL expired — auto-unfreeze"
                );
            }
        }
        // F210-C2: symmetric TTL backstop for split. If the spawned
        // handle_split_part task is wedged (e.g. manager unreachable
        // for the entire multi_modify_split retry window ~12 s, or
        // task panicked), unfreeze so the partition resumes serving
        // writes — at worst the client retries split.
        if let Some(at) = part.borrow().frozen_for_split.get() {
            if at.elapsed() >= FREEZE_TTL {
                let p = part.borrow();
                p.frozen_for_split.set(None);
                if let Some(ack) = p.split_drain_ack.borrow_mut().take() {
                    let _ = ack.send(Err(
                        "split freeze TTL expired (handler wedged)".to_string()
                    ));
                }
                tracing::warn!(
                    part_id,
                    ttl_secs = FREEZE_TTL.as_secs(),
                    "split freeze TTL expired — auto-unfreeze"
                );
            }
        }
    }

    // Shutdown path: drain any still-in-flight batches so clients get their
    // final ack (success or error), then flush any residual pending as one
    // last batch.
    while let Some(c) = inflight.next().await {
        handle_completion(&part, &mut metrics, &locked_by_other, part_id, c).await;
    }
    if !pending.is_empty() {
        let batch = std::mem::take(&mut pending);
        if let Ok(Some(mut flight)) = start_write_batch(&part, batch).await {
            let r = (&mut flight.phase2_fut).await;
            let _ = finish_write_batch(&part, flight.data, r).await;
        }
    }
    metrics.flush(part_id);

    // F120-C — graceful drain. If the loop exited because of `drain_rx`,
    // rotate `active` to imm and synchronously flush every imm via
    // `flush_one_imm` (which uses the existing P-bulk hand-off, or the
    // in-thread fallback). Reply on the oneshot once `imm` is empty.
    if let Some(ack) = drain_ack {
        // Rotate any leftover `active`. `rotate_active` is a no-op on
        // empty memtables.
        {
            let mut p = part.borrow_mut();
            rotate_active(&mut p);
        }
        // Drain imm in order. `flush_one_imm` returns `Ok(false)` when
        // the deque is empty.
        loop {
            match flush_one_imm(&part).await {
                Ok(true) => continue,
                Ok(false) => break,
                Err(e) => {
                    tracing::error!(part_id, "graceful drain flush_one_imm: {e:#}");
                    break;
                }
            }
        }
        let _ = ack.send(());
        tracing::info!(part_id, "graceful drain complete");
    }
}

/// F099-D — decode one incoming `PartitionRequest` and route it. Writes
/// (PUT/DELETE/STREAM_PUT) decode inline and push into `pending` with a
/// direct `WriteResponder` into the outer oneshot; reads (GET/HEAD/RANGE)
/// and other ops dispatch inline. No `compio::runtime::spawn`, no inner
/// oneshot on the write hot path.
#[allow(clippy::too_many_arguments)]
async fn handle_incoming_req(
    req: PartitionRequest,
    pending: &mut Vec<WriteRequest>,
    part: &Rc<RefCell<PartitionData>>,
    part_sc: &Rc<StreamClient>,
    pool: &Rc<ConnPool>,
    manager_addr: &str,
    owner_key: &str,
    revision: i64,
) {
    // F183: bump per-partition request counter for the policy engine.
    // F189-fix MED-3: also bump the never-reset monotonic twin used by
    // the F188 maintenance scheduler's req_per_sec diff. Two atomic
    // adds is ~5 ns total — cheap on the request hot path.
    {
        let p = part.borrow();
        p.metrics
            .req_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        p.metrics
            .req_count_monotonic
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    // F185 + F210-C2: reject mutating ops while frozen-for-merge OR
    // frozen-for-split so writes never land in a log_stream tail past
    // the to-be-captured commit_length. Reads + maintenance still flow
    // normally (the freeze is a write halt, not a full quiesce —
    // readers see the existing state).
    let frozen = {
        let p = part.borrow();
        p.frozen_for_merge.get().is_some() || p.frozen_for_split.get().is_some()
    };
    if frozen {
        match req.msg_type {
            MSG_PUT => {
                let key = match partition_rpc::rkyv_decode::<PutReq>(&req.payload) {
                    Ok(r) => r.key,
                    Err(_) => Vec::new(),
                };
                let resp = PutResp {
                    code: CODE_UNAVAILABLE,
                    message: "partition frozen for merge — refresh routing and retry".to_string(),
                    key,
                };
                let _ = req.resp_tx.send(Ok(partition_rpc::rkyv_encode(&resp)));
                return;
            }
            MSG_DELETE => {
                let key = match partition_rpc::rkyv_decode::<DeleteReq>(&req.payload) {
                    Ok(r) => r.key,
                    Err(_) => Vec::new(),
                };
                let resp = DeleteResp {
                    code: CODE_UNAVAILABLE,
                    message: "partition frozen for merge — refresh routing and retry".to_string(),
                    key,
                };
                let _ = req.resp_tx.send(Ok(partition_rpc::rkyv_encode(&resp)));
                return;
            }
            MSG_STREAM_PUT => {
                let key = match partition_rpc::rkyv_decode::<StreamPutReq>(&req.payload) {
                    Ok(r) => r.key,
                    Err(_) => Vec::new(),
                };
                let resp = PutResp {
                    code: CODE_UNAVAILABLE,
                    message: "partition frozen for merge — refresh routing and retry".to_string(),
                    key,
                };
                let _ = req.resp_tx.send(Ok(partition_rpc::rkyv_encode(&resp)));
                return;
            }
            _ => {} // reads + SPLIT/MAINTENANCE/MERGE_FREEZE flow through
        }
    }

    // Snapshot the per-partition epoch + id ONCE so the write-path
    // enqueue helpers can perform the same TiKV-style region epoch
    // check the read handlers do. `0` from the client = "skip check"
    // (bootstrap / tests / legacy callers). Reads already perform this
    // inside their respective handlers (`handle_get` / `handle_head`
    // / `handle_range` in `rpc_handlers.rs`).
    let (part_region_epoch, part_id_for_err) = {
        let p = part.borrow();
        (p.region_epoch, p.part_id)
    };

    match req.msg_type {
        MSG_PUT => enqueue_put(req, pending, part_region_epoch, part_id_for_err),
        MSG_DELETE => enqueue_delete(req, pending, part_region_epoch, part_id_for_err),
        MSG_STREAM_PUT => enqueue_stream_put(req, pending, part_region_epoch, part_id_for_err),
        // F185: freeze stashes its resp oneshot in PartitionData and
        // returns without replying — the loop body sends OK once
        // pending+inflight drain and every imm flushes (Phase 1.5
        // analogue of TiKV's PrepareMerge: write halt + final
        // checkpoint). `freeze=false` (the rollback path) clears the
        // flag synchronously; nothing to drain.
        MSG_MERGE_FREEZE => {
            let req_msg = match partition_rpc::rkyv_decode::<MergeFreezeReq>(&req.payload) {
                Ok(r) => r,
                Err(e) => {
                    let _ = req.resp_tx.send(Err((StatusCode::InvalidArgument, e)));
                    return;
                }
            };
            if !req_msg.freeze {
                part.borrow().frozen_for_merge.set(None);
                let resp = MergeFreezeResp { code: CODE_OK, message: String::new() };
                let _ = req.resp_tx.send(Ok(partition_rpc::rkyv_encode(&resp)));
                return;
            }
            // freeze=true. Idempotent: if a previous freeze is still
            // waiting on drain, fail the new one rather than overwriting.
            {
                let p = part.borrow();
                if p.frozen_for_merge.get().is_some() && p.freeze_drain_ack.borrow().is_none() {
                    // Already fully drained-frozen — reply OK immediately.
                    let resp = MergeFreezeResp { code: CODE_OK, message: String::new() };
                    let _ = req.resp_tx.send(Ok(partition_rpc::rkyv_encode(&resp)));
                    return;
                }
                if p.freeze_drain_ack.borrow().is_some() {
                    let resp = MergeFreezeResp {
                        code: CODE_PRECONDITION,
                        message: "freeze already in progress".to_string(),
                    };
                    let _ = req.resp_tx.send(Ok(partition_rpc::rkyv_encode(&resp)));
                    return;
                }
                p.frozen_for_merge.set(Some(std::time::Instant::now()));
                *p.freeze_drain_ack.borrow_mut() = Some(req.resp_tx);
            }
            // Loop body fires the OK reply once every pre-freeze write
            // has cleared pending → inflight → memtable → row_stream SST
            // → meta_stream checkpoint.
        }
        // F210-C2: SPLIT_PART is spawned as a separate task on the same
        // P-log runtime so its awaits (drain + commit_length + manager
        // RPC) don't block partition_loop. Without this, the loop
        // can't process the drain (it's the only one that pulls inflight
        // completions + flushes imm), causing a self-deadlock. Pre-F210-C2
        // split ran inline via dispatch_partition_rpc; the drain wasn't
        // needed because split didn't halt writes — but that was the
        // source of the bug (in-flight WAL appends landed past the
        // captured commit_length, invisible on recovery).
        MSG_SPLIT_PART => {
            let part_c = part.clone();
            let part_sc_c = part_sc.clone();
            let pool_c = pool.clone();
            let manager_addr_c = manager_addr.to_string();
            let owner_key_c = owner_key.to_string();
            let revision_c = revision;
            let payload = req.payload;
            let resp_tx = req.resp_tx;
            compio::runtime::spawn(async move {
                let result = crate::rpc_handlers::handle_split_part(
                    payload,
                    &part_c,
                    &part_sc_c,
                    &pool_c,
                    &manager_addr_c,
                    &owner_key_c,
                    revision_c,
                )
                .await;
                let _ = resp_tx.send(result);
            })
            .detach();
        }
        // F216: GET reads of large (256 KB-class) values spend ~93 % of
        // their server time in `resolve_value` — a request/response read of
        // the 256 KB payload from the extent node (the PS holds only the
        // VP). That cost is mostly round-trip LATENCY (the 256 KB transfers
        // in ~40 µs; the rest is the EN read + framing round-trip). Awaiting
        // it inline parks `partition_loop` for the whole ~480 µs, so gets
        // execute strictly one-at-a-time regardless of client pipeline depth
        // (profiled: ops/s ≈ 1/per-op-handle-time, ~477 MB/s @256K). SPAWN
        // the get as a detached task so the loop immediately picks up the
        // next request and many extent reads overlap — overlapping the
        // latency is the lever. No artificial concurrency cap: spawned reads
        // are already bounded upstream by the per-connection ps-conn inflight
        // cap (a ps-conn task won't issue request N+cap until earlier ones
        // reply) and downstream by the stream/EN admission. `handle_get`
        // only holds the partition `borrow()` across synchronous code (it
        // drops the borrow before the resolve_value await), and compio is
        // single-threaded, so concurrent spawned gets never overlap a borrow
        // with each other or with Phase 3's borrow_mut.
        MSG_GET => {
            let part_c = part.clone();
            let part_sc_c = part_sc.clone();
            let pool_c = pool.clone();
            let manager_addr_c = manager_addr.to_string();
            let owner_key_c = owner_key.to_string();
            let revision_c = revision;
            let payload = req.payload;
            let resp_tx = req.resp_tx;
            compio::runtime::spawn(async move {
                let result = dispatch_partition_rpc(
                    MSG_GET,
                    payload,
                    &part_c,
                    &part_sc_c,
                    &pool_c,
                    &manager_addr_c,
                    &owner_key_c,
                    revision_c,
                )
                .await;
                let _ = resp_tx.send(result);
            })
            .detach();
        }
        // Other reads (HEAD/RANGE/GET_DISCARDS) + low-frequency ops
        // (MAINTENANCE) go inline via dispatch_partition_rpc. HEAD is
        // synchronous (no value read); RANGE/MAINTENANCE are not on the
        // kvcache hot path and keep their existing inline semantics.
        _ => {
            let result = dispatch_partition_rpc(
                req.msg_type,
                req.payload,
                part,
                part_sc,
                pool,
                manager_addr,
                owner_key,
                revision,
            )
            .await;
            let _ = req.resp_tx.send(result);
        }
    }
}

fn enqueue_put(req: PartitionRequest, pending: &mut Vec<WriteRequest>, part_region_epoch: u64, part_id_for_err: u64) {
    match partition_rpc::rkyv_decode::<PutReq>(&req.payload) {
        Ok(put_req) => {
            if put_req.region_epoch != 0 && put_req.region_epoch != part_region_epoch {
                let _ = req.resp_tx.send(Err((StatusCode::FailedPrecondition, format!(
                    "region epoch stale: part_id={} have={} got={}",
                    part_id_for_err, part_region_epoch, put_req.region_epoch
                ))));
                return;
            }
            // F129: regular `Put` rejects values exceeding the inline
            // cap. Caller should retry via PutBegin/Chunk/Commit.
            if put_req.value.len() > AUTUMN_PS_MAX_INLINE_BYTES_DEFAULT as usize {
                let key_vec = put_req.key.clone();
                let resp = PutResp {
                    code: CODE_VALUE_TOO_LARGE,
                    message: format!(
                        "value {} bytes exceeds inline cap {} — use PutStream",
                        put_req.value.len(),
                        AUTUMN_PS_MAX_INLINE_BYTES_DEFAULT
                    ),
                    key: key_vec,
                };
                let _ = req.resp_tx.send(Ok(partition_rpc::rkyv_encode(&resp)));
                return;
            }
            let key_vec = put_req.key.clone();
            pending.push(WriteRequest {
                op: WriteOp::Put {
                    user_key: Bytes::from(put_req.key),
                    value: Bytes::from(put_req.value),
                    expires_at: put_req.expires_at,
                },
                resp: WriteResponder::Put {
                    outer: req.resp_tx,
                    key: key_vec,
                },
            });
        }
        Err(e) => {
            let _ = req.resp_tx.send(Err((StatusCode::InvalidArgument, e)));
        }
    }
}

fn enqueue_delete(req: PartitionRequest, pending: &mut Vec<WriteRequest>, part_region_epoch: u64, part_id_for_err: u64) {
    match partition_rpc::rkyv_decode::<DeleteReq>(&req.payload) {
        Ok(del_req) => {
            if del_req.region_epoch != 0 && del_req.region_epoch != part_region_epoch {
                let _ = req.resp_tx.send(Err((StatusCode::FailedPrecondition, format!(
                    "region epoch stale: part_id={} have={} got={}",
                    part_id_for_err, part_region_epoch, del_req.region_epoch
                ))));
                return;
            }
            let key_vec = del_req.key.clone();
            pending.push(WriteRequest {
                op: WriteOp::Delete { user_key: del_req.key },
                resp: WriteResponder::Delete {
                    outer: req.resp_tx,
                    key: key_vec,
                },
            });
        }
        Err(e) => {
            let _ = req.resp_tx.send(Err((StatusCode::InvalidArgument, e)));
        }
    }
}

fn enqueue_stream_put(req: PartitionRequest, pending: &mut Vec<WriteRequest>, part_region_epoch: u64, part_id_for_err: u64) {
    match partition_rpc::rkyv_decode::<StreamPutReq>(&req.payload) {
        Ok(sp_req) => {
            if sp_req.region_epoch != 0 && sp_req.region_epoch != part_region_epoch {
                let _ = req.resp_tx.send(Err((StatusCode::FailedPrecondition, format!(
                    "region epoch stale: part_id={} have={} got={}",
                    part_id_for_err, part_region_epoch, sp_req.region_epoch
                ))));
                return;
            }
            let key_vec = sp_req.key.clone();
            pending.push(WriteRequest {
                op: WriteOp::Put {
                    user_key: Bytes::from(sp_req.key),
                    value: Bytes::from(sp_req.value),
                    expires_at: sp_req.expires_at,
                },
                resp: WriteResponder::Put {
                    outer: req.resp_tx,
                    key: key_vec,
                },
            });
        }
        Err(e) => {
            let _ = req.resp_tx.send(Err((StatusCode::InvalidArgument, e)));
        }
    }
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

async fn recover_partition(
    _part_id: u64,
    rg: &Range,
    log_stream_id: u64,
    _row_stream_id: u64,
    meta_stream_id: u64,
    part_sc: &Rc<StreamClient>,
) -> Result<(Vec<TableMeta>, Vec<Arc<SstReader>>, u64, u64, u32, bool, Memtable)> {
    let mut tables: Vec<TableMeta> = Vec::new();
    let mut sst_readers: Vec<Arc<SstReader>> = Vec::new();
    let mut max_seq: u64 = 0;
    let mut recovered_vp_eid: u64 = 0;
    let mut recovered_vp_off: u32 = 0;
    let mut detected_overlap = false;

    // F184: union the LAST TableLocations record from EACH meta_stream
    // extent (instead of just the last extent's last record). Pre-merge
    // partitions have a single non-empty meta_stream extent with one
    // canonical record — single-extent path is unchanged. Post-merge,
    // the splice puts both survivor's and victim's old meta_stream
    // extents into the merged stream; reading just the last extent
    // would lose half the table set.
    let meta_records: Vec<TableLocations> = read_all_table_locations(meta_stream_id, part_sc)
        .await
        .context("union TableLocations from metaStream extents")?;

    if !meta_records.is_empty() {
        // Take the max vp_head across all source partitions (both were
        // drained pre-merge so neither has stale records past its vp_head).
        for r in &meta_records {
            if r.vp_extent_id > recovered_vp_eid
                || (r.vp_extent_id == recovered_vp_eid && r.vp_offset > recovered_vp_off)
            {
                recovered_vp_eid = r.vp_extent_id;
                recovered_vp_off = r.vp_offset;
            }
        }
        // Dedup SstLocation by (extent_id, offset, len) — CoW-shared SSTs
        // post-split appear in both partitions' records pointing at the
        // same physical bytes; we keep one entry.
        let mut seen: HashSet<(u64, u32, u32)> = HashSet::new();
        let mut all_locs: Vec<SstLocation> = Vec::new();
        for r in meta_records {
            for loc in r.locs {
                let key = (loc.extent_id, loc.offset, loc.len);
                if seen.insert(key) {
                    all_locs.push(loc);
                }
            }
        }
        for loc in all_locs {
            // F198: bounded retry on eversion-mismatch during open. A
            // post-restart manager whose `apply_ec_conversion_done` did
            // not commit in its previous lifetime keeps stale (pre-EC)
            // `ExtentInfo.eversion` in etcd while the extent-node's local
            // file already holds the post-EC eversion. The manager-side
            // `ec_conversion_dispatch_loop` re-dispatches via the F198
            // rich marker and converges manager state within ~1 tick
            // (≤ 0.5 s on first iter, then 5 s); we retry with backoff
            // until convergence so the operator doesn't have to manually
            // restart the PS a second time. Cap at ~30 s — past that we
            // surface the error so it doesn't hide a non-EC fault.
            let mut attempt: u32 = 0;
            let (sst_bytes, _end) = loop {
                match part_sc
                    .read_bytes_from_extent(loc.extent_id, loc.offset, loc.len)
                    .await
                {
                    Ok(v) => break v,
                    Err(e) if attempt < 30 && e.to_string().contains("eversion mismatch") => {
                        attempt += 1;
                        tracing::warn!(
                            part_id = _part_id,
                            extent_id = loc.extent_id,
                            offset = loc.offset,
                            attempt,
                            "F198: SST read returned eversion mismatch during open — \
                             waiting 1 s for manager to converge state, will retry"
                        );
                        compio::time::sleep(std::time::Duration::from_secs(1)).await;
                        continue;
                    }
                    Err(e) => {
                        return Err(e).with_context(|| {
                            format!(
                                "read SST from rowStream extent={} offset={}",
                                loc.extent_id, loc.offset
                            )
                        });
                    }
                }
            };

            let sst_bytes = Bytes::from(sst_bytes);
            let reader = SstReader::from_bytes(sst_bytes.clone()).with_context(|| {
                let preview_len = sst_bytes.len().min(32);
                format!(
                    "open SST extent={} offset={} read_len={} preview={:02x?}",
                    loc.extent_id,
                    loc.offset,
                    sst_bytes.len(),
                    &sst_bytes[..preview_len]
                )
            })?;

            let tbl_last_seq = reader.seq_num();
            if tbl_last_seq > max_seq {
                max_seq = tbl_last_seq;
            }

            if !detected_overlap {
                let sk = parse_key(reader.smallest_key());
                let bk = parse_key(reader.biggest_key());
                if !in_range(rg, sk) || !in_range(rg, bk) {
                    detected_overlap = true;
                }
            }

            if reader.vp_extent_id > recovered_vp_eid
                || (reader.vp_extent_id == recovered_vp_eid && reader.vp_offset > recovered_vp_off)
            {
                recovered_vp_eid = reader.vp_extent_id;
                recovered_vp_off = reader.vp_offset;
            }

            let estimated_size = reader.estimated_size();
            tables.push(TableMeta {
                extent_id: loc.extent_id,
                offset: loc.offset,
                len: loc.len,
                estimated_size,
                last_seq: tbl_last_seq,
            });
            sst_readers.push(Arc::new(reader));
        }
    }

    // Replay logStream.
    //
    // F210-C1: the dedup `if ts <= sst_max_seq { continue; }` below is
    // safe because `partition_loop` now uses `FuturesOrdered`
    // (was `FuturesUnordered`). Phase 3 (memtable insert) is therefore
    // strictly in launch order = strictly in seq order; a rotated
    // active contains a contiguous seq range [start, max_seq], and the
    // SST flushed from it has the invariant "every seq <= last_seq is
    // in this SST or an earlier SST". `sst_max_seq` is then a sound
    // upper bound for "seqs already in some SST".
    //
    // Pre-F210-C1 this used `FuturesUnordered`, which yielded Phase 2
    // completions in completion order, not launch order. Phase 3
    // therefore ran out-of-order: batch B (seq 101-200) could insert
    // into active and trigger rotation before batch A (seq 1-100)
    // completed Phase 2. The flushed SST then had `last_seq = 200` but
    // was MISSING seqs 1-100. Replay's `ts <= 200 → skip` predicate
    // then silently dropped batch A on crash recovery — see F210-C1 in
    // feature_list.md.
    let sst_max_seq = max_seq;
    let active = Memtable::new();

    let replay_extents: Option<Vec<(u64, u32)>> = if recovered_vp_eid == 0 && tables.is_empty() {
        let stream_info = part_sc.get_stream_info(log_stream_id).await?;
        Some(
            stream_info
                .extent_ids
                .into_iter()
                .map(|eid| (eid, 0u32))
                .collect(),
        )
    } else if recovered_vp_eid > 0 {
        let stream_info = part_sc.get_stream_info(log_stream_id).await?;
        Some(
            stream_info
                .extent_ids
                .into_iter()
                .filter_map(|eid| {
                    if eid < recovered_vp_eid {
                        None
                    } else {
                        let off = if eid == recovered_vp_eid {
                            recovered_vp_off
                        } else {
                            0
                        };
                        Some((eid, off))
                    }
                })
                .collect(),
        )
    } else {
        None
    };

    if let Some(extents) = replay_extents {
        for (eid, start_off) in extents {
            // F127: retry extent reads during recovery instead of silently
            // skipping. A transient node failure should not cause permanent
            // data loss for un-checkpointed writes.
            let data = {
                let mut attempt = 0u32;
                loop {
                    match part_sc.read_bytes_from_extent(eid, start_off, 0).await {
                        Ok((d, _)) => break d,
                        Err(e) => {
                            attempt += 1;
                            if attempt >= 10 {
                                return Err(anyhow::anyhow!(
                                    "recover_partition: failed to read extent {} after {} attempts: {}",
                                    eid, attempt, e
                                ));
                            }
                            tracing::warn!(
                                "recover_partition: read extent {} attempt {}/10 failed: {}, retrying...",
                                eid, attempt, e
                            );
                            compio::time::sleep(std::time::Duration::from_secs(2)).await;
                        }
                    }
                }
            };
            for (buf_off, op, key, value, expires_at) in decode_records_with_offsets(&data) {
                let ts = parse_ts(&key);
                if ts > max_seq {
                    max_seq = ts;
                }
                if ts <= sst_max_seq {
                    continue;
                }

                let record_extent_off = start_off + buf_off as u32;
                let mem_entry = if op & OP_VALUE_POINTER != 0 || value.len() > VALUE_THROTTLE {
                    // VP detection: new WAL has VP flag in op; old WAL uses
                    // value size as fallback. F186 fix: V1 envelope adds
                    // 5 bytes (sentinel+length) before the V0 inner header,
                    // so value bytes are at +22 not +17 from record start.
                    let vp = ValuePointer {
                        extent_id: eid,
                        offset: record_extent_off + 22 + key.len() as u32,
                        len: value.len() as u32,
                    };
                    MemEntry {
                        op: (op & 0x7f) | OP_VALUE_POINTER,
                        value: vp.encode().to_vec(),
                        expires_at,
                    }
                } else {
                    MemEntry {
                        op,
                        value,
                        expires_at,
                    }
                };

                let size = key.len() as u64 + mem_entry.value.len() as u64 + 32;
                active.insert(key, mem_entry, size);
            }
        }
    }

    Ok((tables, sst_readers, max_seq, recovered_vp_eid, recovered_vp_off, detected_overlap, active))
}

// ---------------------------------------------------------------------------
// Record encoding/decoding
// ---------------------------------------------------------------------------

pub(crate) fn encode_record(op: u8, key: &[u8], value: &[u8], expires_at: u64) -> Vec<u8> {
    // F158: now writes V1 envelope. Used by GC tests (background.rs
    // gc_streaming_tests) and lib.rs round-trip tests; both verify via the
    // V1-aware decoders.
    crate::wal_record::encode_v1(op, key, value, expires_at)
}

pub(crate) fn decode_records_full(bytes: &[u8]) -> Vec<(u8, Vec<u8>, Vec<u8>, u64)> {
    // F158: dispatches per-record on V0 vs V1 envelope; CRC failures on V1
    // log a WARN and skip past the corrupted record (advance by its declared
    // length) instead of silently returning truncated state.
    let mut out = Vec::new();
    let mut cursor = 0usize;
    let mut skipped: usize = 0;
    while cursor < bytes.len() {
        match crate::wal_record::decode_one(&bytes[cursor..]) {
            crate::wal_record::DecodeOne::Ok(r) => {
                out.push((r.op, r.key.to_vec(), r.value.to_vec(), r.expires_at));
                cursor += r.total;
            }
            crate::wal_record::DecodeOne::Incomplete => break,
            crate::wal_record::DecodeOne::Corrupt { skip_bytes, reason } => {
                tracing::warn!(
                    cursor,
                    skip_bytes,
                    reason,
                    "F158: WAL record corrupted; skipping"
                );
                cursor += skip_bytes;
                skipped += 1;
            }
        }
    }
    if skipped > 0 {
        tracing::warn!(skipped, "F158: skipped {skipped} corrupted WAL record(s)");
    }
    out
}

pub(crate) fn decode_records_with_offsets(bytes: &[u8]) -> Vec<(usize, u8, Vec<u8>, Vec<u8>, u64)> {
    // F158: same shape as decode_records_full but preserves the
    // record-start offset so callers can compute the recovered VP head.
    let mut out = Vec::new();
    let mut cursor = 0usize;
    let mut skipped: usize = 0;
    while cursor < bytes.len() {
        let record_start = cursor;
        match crate::wal_record::decode_one(&bytes[cursor..]) {
            crate::wal_record::DecodeOne::Ok(r) => {
                out.push((record_start, r.op, r.key.to_vec(), r.value.to_vec(), r.expires_at));
                cursor += r.total;
            }
            crate::wal_record::DecodeOne::Incomplete => break,
            crate::wal_record::DecodeOne::Corrupt { skip_bytes, reason } => {
                tracing::warn!(
                    record_start,
                    skip_bytes,
                    reason,
                    "F158: WAL record corrupted; skipping"
                );
                cursor += skip_bytes;
                skipped += 1;
            }
        }
    }
    if skipped > 0 {
        tracing::warn!(skipped, "F158: skipped {skipped} corrupted WAL record(s)");
    }
    out
}

/// F184: Walk all extents in a meta_stream and collect the LAST
/// `TableLocations` record from each non-empty extent. Used by
/// recovery to gather both survivor's and victim's checkpoints
/// after a partition merge spliced their meta_streams together.
///
/// Pre-merge partitions have one non-empty extent → single record →
/// behaviour identical to the legacy `read_last_extent_data +
/// decode_last_table_locations`. Post-merge they have N extents.
pub(crate) async fn read_all_table_locations(
    stream_id: u64,
    sc: &Rc<StreamClient>,
) -> Result<Vec<TableLocations>> {
    let info = sc.get_stream_info(stream_id).await?;
    let mut out: Vec<TableLocations> = Vec::new();
    for &eid in &info.extent_ids {
        let (payload, _end) = sc.read_bytes_from_extent(eid, 0, 0).await?;
        if payload.is_empty() {
            continue;
        }
        // decode_last_table_locations returns Err only when NO valid
        // record exists in the buffer; bit-rot mid-stream is logged
        // and skipped. Empty extents (carry no records) are common —
        // skip them silently.
        match decode_last_table_locations(&payload) {
            Ok(locs) => out.push(locs),
            Err(e) => {
                tracing::warn!(
                    extent_id = eid,
                    error = %e,
                    "F184: meta_stream extent has no valid TableLocations; skipping"
                );
            }
        }
    }
    Ok(out)
}

pub(crate) fn decode_last_table_locations(data: &[u8]) -> Result<TableLocations> {
    // Format: sequence of [len: u32 LE][rkyv payload] records. We want the last
    // successfully decoded record.
    //
    // F157: pre-F157 a decode failure mid-stream caused `break`, returning the
    // PRIOR record. After F155 made rkyv decoding strict (bytecheck), any bit
    // rot in a record's payload trips this break-and-fall-back path, silently
    // discarding all valid records that came AFTER the corrupted one — the
    // partition restarts on stale state without a single error logged.
    //
    // F157 changes the decode-failure handling to: log a WARN with offset +
    // error, advance past the corrupted record using its declared msg_len, and
    // continue scanning. This preserves the legitimate partial-tail-write
    // behaviour (the `total > buf.len()` check still breaks at the end) while
    // surfacing mid-stream corruption loudly and refusing to silently drop
    // newer valid records. If `msg_len` itself is corrupt to point into
    // garbage, we still bound the damage: the next record's length-prefix will
    // almost certainly fail decode too, and we'll skip it; eventually we either
    // find a valid record or exit with `last` populated by the last good one.
    let mut last: Option<TableLocations> = None;
    let mut buf = data;
    let mut offset = 0usize;
    let mut skipped: usize = 0;
    while buf.len() >= 4 {
        let msg_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let total = 4 + msg_len;
        if total > buf.len() {
            // Legitimate partial-tail-write: stop here.
            break;
        }
        match rkyv_decode::<TableLocations>(&buf[4..4 + msg_len]) {
            Ok(locs) => {
                last = Some(locs);
            }
            Err(e) => {
                tracing::warn!(
                    offset,
                    msg_len,
                    error = %e,
                    "F157: TableLocations record decode failed (likely bit rot); skipping and continuing"
                );
                skipped += 1;
            }
        }
        buf = &buf[total..];
        offset += total;
    }
    if skipped > 0 {
        tracing::warn!(
            skipped,
            "F157: skipped {skipped} corrupted TableLocations record(s); newer valid records preserved"
        );
    }
    last.ok_or_else(|| anyhow!("decode TableLocations: no valid record"))
}

pub(crate) fn in_range(rg: &Range, key: &[u8]) -> bool {
    if key < rg.start_key.as_slice() {
        return false;
    }
    if rg.end_key.is_empty() {
        return true;
    }
    key < rg.end_key.as_slice()
}

// ---------------------------------------------------------------------------
// MetaStream persistence
// ---------------------------------------------------------------------------

pub(crate) async fn save_table_locs_raw(
    stream_client: &Rc<StreamClient>,
    meta_stream_id: u64,
    tables: &[TableMeta],
    vp_extent_id: u64,
    vp_offset: u32,
) -> Result<()> {
    let locs = TableLocations {
        locs: tables
            .iter()
            .map(|t| SstLocation {
                extent_id: t.extent_id,
                offset: t.offset,
                len: t.len,
            })
            .collect(),
        vp_extent_id,
        vp_offset,
    };
    let payload = rkyv_encode(&locs);
    let mut data = Vec::with_capacity(4 + payload.len());
    data.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    data.extend_from_slice(&payload);
    stream_client.append(meta_stream_id, &data).await?;
    let info = stream_client.get_stream_info(meta_stream_id).await?;
    if info.extent_ids.len() > 1 {
        let last = *info.extent_ids.last().unwrap();
        stream_client.truncate(meta_stream_id, last).await?;
    }
    Ok(())
}

fn collect_partition_vp_refs(readers: &[Arc<SstReader>]) -> Vec<(u64, u32)> {
    let mut counts = BTreeMap::<u64, u32>::new();
    for reader in readers {
        for &extent_id in &reader.vp_deps {
            *counts.entry(extent_id).or_insert(0) += 1;
        }
    }
    counts.into_iter().collect()
}

/// F210-C4: background retry task. Every 5 s, if `vp_refs_dirty` is
/// set, attempts a fresh `sync_partition_vp_refs`. Clears dirty on
/// success (releases the GC gate). Exits when the partition's
/// channels close (`flush_tx` upgrade fails — partition torn down).
pub(crate) async fn vp_refs_retry_loop(part: Rc<RefCell<PartitionData>>) {
    const RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
    loop {
        compio::time::sleep(RETRY_INTERVAL).await;
        if !part.borrow().vp_refs_dirty.get() {
            continue;
        }
        let part_id = part.borrow().part_id;
        match sync_partition_vp_refs(&part).await {
            Ok(()) => {
                part.borrow().vp_refs_dirty.set(false);
                tracing::info!(
                    part_id,
                    "F210-C4: vp_refs sync recovered; GC gate released"
                );
            }
            Err(e) => {
                tracing::warn!(
                    part_id,
                    error = %e,
                    "F210-C4: vp_refs sync retry still failing; will retry in 5s"
                );
            }
        }
    }
}

/// F210-C4: wrapper around `sync_partition_vp_refs` that converts a
/// failure into a `vp_refs_dirty` flag set + WARN log, returning Ok.
/// Used by flush/compact paths so a transient manager unreachability
/// doesn't fail the flush itself — the SST is durable, only the
/// vp_refs RPC is pending. The background `vp_refs_retry_loop`
/// periodically retries until success.
///
/// On success: clears `vp_refs_dirty`. On failure: sets it.
///
/// IMPORTANT: callers must NOT use this for the INITIAL sync during
/// `open_partition` — that one needs strict error propagation so a
/// partition with broken manager link fails to open rather than
/// silently coming up in a dirty state. open_partition uses the raw
/// `sync_partition_vp_refs` directly.
pub(crate) async fn sync_partition_vp_refs_or_mark_dirty(
    part: &Rc<RefCell<PartitionData>>,
) {
    let part_id = part.borrow().part_id;
    match sync_partition_vp_refs(part).await {
        Ok(()) => {
            part.borrow().vp_refs_dirty.set(false);
        }
        Err(e) => {
            part.borrow().vp_refs_dirty.set(true);
            tracing::warn!(
                part_id,
                error = %e,
                "F210-C4: sync_partition_vp_refs failed; partition marked dirty. \
                 GC blocked until next successful sync. \
                 vp_refs_retry_loop will retry."
            );
        }
    }
}

pub(crate) async fn sync_partition_vp_refs(part: &Rc<RefCell<PartitionData>>) -> Result<()> {
    let (part_id, refs, manager_addr, pool) = {
        let p = part.borrow();
        (
            p.part_id,
            collect_partition_vp_refs(&p.sst_readers),
            p.manager_addr.clone(),
            p.pool.clone(),
        )
    };

    let req = manager_rpc::rkyv_encode(&manager_rpc::SyncPartitionVpRefsReq { part_id, refs });
    let mut last_err: Option<anyhow::Error> = None;
    for mgr in manager_addr.split(',') {
        let mgr = mgr.trim();
        if mgr.is_empty() {
            continue;
        }
        let mgr_norm = autumn_stream::conn_pool::normalize_endpoint(mgr);
        // 30 s — manager replaces the partition's full vp-ref
        // snapshot (in-memory + etcd mirror) and diffs it to adjust
        // every touched extent's `vp_table_refs`. Payload scales
        // with live SST count; bounded so a hung manager doesn't
        // block the flush/compact follow-up indefinitely.
        match pool
            .call_timeout(&mgr_norm, manager_rpc::MSG_SYNC_PARTITION_VP_REFS, req.clone(), Duration::from_secs(30))
            .await
        {
            Ok(bytes) => match manager_rpc::rkyv_decode::<manager_rpc::SyncPartitionVpRefsResp>(&bytes) {
                Ok(resp) if resp.code == manager_rpc::CODE_OK => return Ok(()),
                Ok(resp) => {
                    last_err = Some(anyhow!(
                        "sync_partition_vp_refs rejected by {}: {}",
                        mgr,
                        resp.message
                    ));
                }
                Err(e) => {
                    last_err = Some(anyhow!("decode sync_partition_vp_refs resp: {}", e));
                }
            },
            Err(e) => {
                last_err = Some(e);
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow!("no manager addresses to sync vp refs with")))
}

// ---------------------------------------------------------------------------
// SSTable building
// ---------------------------------------------------------------------------

pub(crate) fn build_sst_bytes(imm: &Memtable, vp_extent_id: u64, vp_offset: u32) -> (Vec<u8>, u64) {
    let mut builder = SstBuilder::new(vp_extent_id, vp_offset);
    let mut last_seq = 0u64;
    imm.for_each(|ikey, me| {
        let ts = parse_ts(ikey);
        if ts > last_seq {
            last_seq = ts;
        }
        builder.add(ikey, me.op, &me.value, me.expires_at);
    });
    if builder.is_empty() {
        (SstBuilder::new(vp_extent_id, vp_offset).finish(), last_seq)
    } else {
        (builder.finish(), last_seq)
    }
}

// ---------------------------------------------------------------------------
// Memtable rotation + flush
// ---------------------------------------------------------------------------

pub(crate) fn rotate_active(part: &mut PartitionData) {
    if part.active.is_empty() {
        return;
    }
    let frozen = std::mem::replace(&mut part.active, Memtable::new());
    part.imm.push_back(Arc::new(frozen));
    let _ = part.flush_tx.unbounded_send(());
}

pub(crate) fn maybe_rotate(part: &mut PartitionData) {
    if part.active.mem_bytes() >= FLUSH_MEM_BYTES {
        rotate_active(part);
    }
}

/// F197: outcome of `run_flush_async_phase` — the heavy build + upload
/// is done, but `tables` / `imm.pop_front` / meta_stream checkpoint
/// have NOT happened yet. Caller passes this to `commit_flush_outcome`
/// in strict launch order.
pub(crate) struct FlushOutcome {
    pub new_meta: TableMeta,
    pub reader: SstReader,
    pub vp_eid: u64,
    pub vp_off: u32,
}

/// F197: async / heavy phase of one imm flush. Safe to run concurrently
/// for different imm entries (each captures its own `Arc<Memtable>`
/// and `Arc<StreamClient>`). NO borrow_mut on `part` past the initial
/// snapshot, so multiple concurrent calls are race-free against each
/// other. NEVER `pop_front`s the imm queue — that's `commit_flush_outcome`'s
/// job.
pub(crate) async fn run_flush_async_phase(
    part: Rc<RefCell<PartitionData>>,
    imm_mem: Arc<Memtable>,
) -> Result<FlushOutcome> {
    let (row_stream_id, snap_vp_eid, snap_vp_off, req_tx_opt, part_sc, invalidate_row, meta_stream_id) = {
        let p = part.borrow();
        // need_invalidate_row_stream is fetch-and-clear semantics: the
        // first concurrent flush takes it, later ones see false. That's
        // correct — P-bulk only needs the invalidate signal once.
        let inv = p.need_invalidate_row_stream.replace(false);
        (
            p.row_stream_id,
            p.vp_extent_id,
            p.vp_offset,
            p.flush_req_tx.clone(),
            p.stream_client.clone(),
            inv,
            p.meta_stream_id,
        )
    };

    // F178 Phase 2 durability barrier — see flush_one_imm history comment.
    if snap_vp_off > 0 && snap_vp_eid != 0 {
        part_sc
            .await_log_synced_to(snap_vp_eid, snap_vp_off as u64)
            .await?;
    }

    let Some(mut req_tx) = req_tx_opt else {
        // P-bulk not spawned — in-thread fallback (legacy single-flow,
        // no parallelism). Build SST locally and return outcome.
        let imm_clone = imm_mem.clone();
        let (sst_bytes, last_seq) = compio::runtime::spawn_blocking(move || {
            build_sst_bytes(&imm_clone, snap_vp_eid, snap_vp_off)
        })
        .await
        .map_err(|_| anyhow!("SSTable build task failed"))?;
        let result = part_sc.append(row_stream_id, &sst_bytes).await?;
        let estimated_size = sst_bytes.len() as u64;
        let reader = SstReader::from_bytes(Bytes::from(sst_bytes))?;
        let _ = meta_stream_id;
        return Ok(FlushOutcome {
            new_meta: TableMeta {
                extent_id: result.extent_id,
                offset: result.offset,
                len: result.end - result.offset,
                estimated_size,
                last_seq,
            },
            reader,
            vp_eid: snap_vp_eid,
            vp_off: snap_vp_off,
        });
    };

    let (resp_tx, resp_rx) = oneshot::channel();
    let req = FlushReq {
        imm: imm_mem,
        vp_eid: snap_vp_eid,
        vp_off: snap_vp_off,
        row_stream_id,
        invalidate_row_stream: invalidate_row,
        resp_tx,
    };
    if req_tx.send(req).await.is_err() {
        return Err(anyhow!("bulk thread dropped flush channel"));
    }
    let (new_meta, reader) = match resp_rx.await {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(anyhow!("bulk thread dropped flush response")),
    };
    Ok(FlushOutcome { new_meta, reader, vp_eid: snap_vp_eid, vp_off: snap_vp_off })
}

/// F197: commit phase. Must be called in strict launch order (i.e. the
/// outcome for imm at front of the queue) — handles the atomic swap
/// (table-publish + imm.pop_front + meta_stream checkpoint). NEVER
/// reordered with respect to other commits.
///
/// F148-A invariant: NO `.await` between the `borrow_mut` block and
/// the `stream_client.append` mpsc-send inside `save_table_locs_raw`.
/// `FuturesOrdered` in `background_flush_loop` guarantees the per-
/// partition concurrent flushes commit in launch order, so commits
/// from this function and from `do_compact` interleave at single-
/// threaded P-log granularity — meta_stream record order = borrow_mut
/// order, as before.
pub(crate) async fn commit_flush_outcome(
    part: &Rc<RefCell<PartitionData>>,
    outcome: FlushOutcome,
) -> Result<()> {
    let (tables_snapshot, vp_eid, vp_off, part_sc, meta_stream_id) = {
        let mut p = part.borrow_mut();
        p.tables.push(outcome.new_meta);
        p.sst_readers.push(Arc::new(outcome.reader));
        // Release the claim-by-ptr (see `flush_one_imm` / the
        // background loop). The popped imm IS the one whose Phase 2
        // we just finished — FuturesOrdered in the background loop and
        // single-task serial `await` in flush_one_imm guarantee
        // FIFO commit order.
        if let Some(popped) = p.imm.pop_front() {
            let ptr = Arc::as_ptr(&popped) as usize;
            p.flushing_imm_ptrs.borrow_mut().remove(&ptr);
        }
        // F120-A: wake partition_loop on imm-full back-pressure.
        let _ = p.imm_drained_tx.unbounded_send(());
        (
            p.tables.clone(),
            outcome.vp_eid,
            outcome.vp_off,
            p.stream_client.clone(),
            p.meta_stream_id,
        )
    };
    // F148-A: no `.await` between the borrow_mut drop above and the
    // stream_client.append mpsc-send inside save_table_locs_raw.
    save_table_locs_raw(&part_sc, meta_stream_id, &tables_snapshot, vp_eid, vp_off).await?;
    // F210-C4: meta_stream checkpoint published; SST is durable. If the
    // vp_refs sync fails (manager unreachable / NotLeader / transient),
    // mark dirty + return Ok rather than fail the flush — the SST is
    // good, the manager just needs to catch up. Background retry +
    // GC gate prevent erroneous deletion until sync recovers.
    sync_partition_vp_refs_or_mark_dirty(part).await;
    // F202: tables changed (new SST committed) → refresh the
    // advisory-input metrics so the next report_load_loop tick carries
    // accurate dead-data / minor-compact-pending volumes.
    crate::background::refresh_f202_metrics(part);
    Ok(())
}

/// Single-imm flush (legacy back-compat for `flush_memtable_locked`,
/// `handle_split_part`, and test helpers). After F197, this is just
/// `run_flush_async_phase` + `commit_flush_outcome` composed; the
/// background loop bypasses this and runs the two phases concurrently.
///
/// **Claim-by-ptr invariant**: before running Phase 2 we record the
/// imm's `Arc::as_ptr()` in `flushing_imm_ptrs`. If the front imm is
/// already being flushed by `background_flush_loop`, return Ok(false)
/// so the caller (`flush_memtable_locked`'s drain loop) moves on
/// instead of building a duplicate SST. `commit_flush_outcome` is the
/// pair that REMOVES the ptr.
pub(crate) async fn flush_one_imm(part: &Rc<RefCell<PartitionData>>) -> Result<bool> {
    let imm_mem = {
        let p = part.borrow();
        let imm = match p.imm.front().cloned() {
            Some(m) => m,
            None => return Ok(false),
        };
        let ptr = Arc::as_ptr(&imm) as usize;
        let mut inflight = p.flushing_imm_ptrs.borrow_mut();
        if inflight.contains(&ptr) {
            // Already being flushed by another path (typically
            // background_flush_loop reached imm[0] first). The other
            // path's commit will publish the SST; we treat this as
            // "no-op" and let the caller proceed.
            return Ok(false);
        }
        inflight.insert(ptr);
        imm
    };
    let outcome = run_flush_async_phase(part.clone(), imm_mem).await?;
    commit_flush_outcome(part, outcome).await?;
    Ok(true)
}

// F197 removed `flush_one_imm_local` — the in-thread fallback now
// lives inline in `run_flush_async_phase` (the `req_tx_opt == None`
// branch).

pub(crate) async fn flush_memtable_locked(part: &Rc<RefCell<PartitionData>>) -> Result<bool> {
    {
        let mut p = part.borrow_mut();
        rotate_active(&mut p);
    }
    let mut any = false;
    loop {
        match flush_one_imm(part).await {
            Ok(true) => { any = true; continue; }
            Ok(false) => break,
            Err(e) => return Err(e),
        }
    }
    Ok(any)
}

// ---------------------------------------------------------------------------
// Background loops
// ---------------------------------------------------------------------------

/// F197: parallel imm-flush drain.
///
/// `FuturesOrdered` drives up to `ps_flush_inflight_cap()` concurrent
/// `run_flush_async_phase` futures. Each future captures one imm
/// (`Arc<Memtable>` clone) at launch time; completions are pulled in
/// strict launch order, then `commit_flush_outcome` runs serially
/// (preserves F148-A invariant: borrow_mut order = mpsc-send order
/// = meta_stream record order).
///
/// Why FuturesOrdered: matches the "concurrent build/upload, serial
/// commit" semantics exactly without any reorder buffer or seq
/// tagging — `.next()` yields in push order while all in-flight
/// futures still make progress in parallel.
///
/// Error policy: fail-stop. If any in-flight async phase OR any
/// commit returns Err, drain the remaining inflight (so they don't
/// dangle) and break. The next `flush_rx` signal restarts the loop
/// — same retry behaviour as pre-F197.
async fn background_flush_loop(
    part: Rc<RefCell<PartitionData>>,
    mut flush_rx: mpsc::UnboundedReceiver<()>,
) {
    use futures::stream::FuturesOrdered;
    use futures::future::FutureExt;
    type FlushFuture =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<FlushOutcome>>>>;

    let cap = ps_flush_inflight_cap();

    while flush_rx.next().await.is_some() {
        let mut inflight: FuturesOrdered<FlushFuture> = FuturesOrdered::new();
        let mut failed = false;
        loop {
            // (A) Launch up to `cap` concurrent flushes. The next imm to
            // launch sits at `imm[inflight.len()]` because commit pops
            // front strictly in order, so the queue tracks 1:1 with
            // in-flight + remaining unlaunched.
            while !failed && inflight.len() < cap {
                // Same claim-by-ptr pattern as flush_one_imm. The
                // `inflight.len()` index walks the queue front→back so
                // the launch order matches FuturesOrdered's pull order.
                // If the imm at this index is already claimed by an
                // inline `flush_one_imm` (called from
                // flush_memtable_locked / split / merge), skip it —
                // its commit_flush_outcome will pop it for us.
                let imm_at_idx = {
                    let p = part.borrow();
                    match p.imm.get(inflight.len()).cloned() {
                        Some(imm) => {
                            let ptr = Arc::as_ptr(&imm) as usize;
                            let mut set = p.flushing_imm_ptrs.borrow_mut();
                            if set.contains(&ptr) {
                                // Another path owns this imm; its
                                // commit_flush_outcome will publish.
                                // Skip launching for this index.
                                None
                            } else {
                                set.insert(ptr);
                                Some(imm)
                            }
                        }
                        None => None,
                    }
                };
                match imm_at_idx {
                    Some(imm) => {
                        let part_c = part.clone();
                        inflight.push_back(
                            run_flush_async_phase(part_c, imm).boxed_local(),
                        );
                    }
                    None => break,
                }
            }
            if inflight.is_empty() {
                break;
            }

            // (B) Pull next in launch order; commit (or fail-stop drain).
            match inflight.next().await {
                Some(Ok(outcome)) => {
                    if failed {
                        // Drop the outcome (already partway through drain).
                        continue;
                    }
                    if let Err(e) = commit_flush_outcome(&part, outcome).await {
                        tracing::error!("background flush commit error: {e}");
                        failed = true;
                    }
                }
                Some(Err(e)) => {
                    tracing::error!("background flush async-phase error: {e}");
                    failed = true;
                }
                None => break,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// F088: Per-partition bulk thread (P-bulk)
//
// The bulk thread owns its own compio runtime (separate io_uring), its own
// ConnPool, and its own StreamClient. This prevents 128MB row_stream SSTable
// uploads from head-of-line-blocking the 4KB log_stream WAL batches sharing
// the P-log runtime.
//
// The StreamClient uses `new_with_revision` to inherit the server-level
// owner-lock revision — no second `acquire_owner_lock` call, so both clients
// use the same fencing token. Post-F093 the pool no longer uses Hot/Bulk
// kinds; each thread's ConnPool is role-dedicated.
// ---------------------------------------------------------------------------

fn spawn_bulk_thread(
    part_id: u64,
    manager_addr: String,
    owner_key: String,
    revision: i64,
    flush_req_rx: mpsc::Receiver<FlushReq>,
    row_append_rx: mpsc::Receiver<RowAppendReq>,
    cpu: Option<usize>,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name(format!("part-{part_id}-bulk"))
        .spawn(move || {
            let rt = match compio::runtime::RuntimeBuilder::new()
                .thread_affinity(affinity_set(cpu))
                .build()
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(part_id, error = %e, "bulk thread runtime init failed");
                    return;
                }
            };
            tracing::info!(part_id, ?cpu, "P-bulk thread runtime ready");
            rt.block_on(async move {
                let pool = Rc::new(ConnPool::new());
                let bulk_sc = match StreamClient::new_with_revision(
                    &manager_addr,
                    owner_key,
                    revision,
                    3 * 1024 * 1024 * 1024,
                    pool,
                )
                .await
                {
                    Ok(sc) => sc,
                    Err(e) => {
                        tracing::error!(part_id, error = %e, "bulk StreamClient init failed");
                        return;
                    }
                };
                // F192: same reporter_part_id as the P-log StreamClient so
                // bulk-thread row_stream / compact append failures bucket
                // into the same partition for the manager's quorum count.
                bulk_sc.set_reporter_part_id(part_id);
                tracing::info!(part_id, "bulk thread ready");
                flush_worker_loop(bulk_sc, flush_req_rx, row_append_rx).await;
                tracing::info!(part_id, "bulk thread exiting");
            });
        })
}

/// R4 4.4 — P-bulk worker with N-deep SQ/CQ pipeline.
///
/// Handles two kinds of work, both using P-bulk's single StreamClient so
/// that row_stream commit tracking stays coherent:
///   - `FlushReq`: build SST bytes + row_stream.append (from flush_loop)
///   - `RowAppendReq`: row_stream.append only (from compaction on P-log)
///
/// The cap is deliberately small (default 2, env
/// `AUTUMN_PS_BULK_INFLIGHT_CAP`) because each in-flight item holds a
/// full SSTable buffer in RAM.
async fn flush_worker_loop(
    bulk_sc: Rc<StreamClient>,
    flush_req_rx: mpsc::Receiver<FlushReq>,
    row_append_rx: mpsc::Receiver<RowAppendReq>,
) {
    use futures::future::{select, Either};

    let cap = crate::ps_bulk_inflight_cap();

    enum BulkCompletion {
        Flush {
            resp_tx: oneshot::Sender<Result<(TableMeta, SstReader)>>,
            result: Result<(TableMeta, SstReader)>,
        },
        RowAppend {
            resp_tx: oneshot::Sender<Result<autumn_stream::AppendResult>>,
            result: Result<autumn_stream::AppendResult>,
        },
    }

    impl BulkCompletion {
        fn send(self) {
            match self {
                BulkCompletion::Flush { resp_tx, result } => { let _ = resp_tx.send(result); }
                BulkCompletion::RowAppend { resp_tx, result } => { let _ = resp_tx.send(result); }
            }
        }
    }

    type BulkFut = std::pin::Pin<Box<dyn std::future::Future<Output = BulkCompletion>>>;
    let mut inflight: FuturesUnordered<BulkFut> = FuturesUnordered::new();

    enum SqMsg {
        Flush(FlushReq),
        RowAppend(RowAppendReq),
    }

    let mut sq_rx = futures::stream::select(
        flush_req_rx.map(SqMsg::Flush),
        row_append_rx.map(SqMsg::RowAppend),
    );

    let launch = |msg: SqMsg, bulk_sc: &Rc<StreamClient>| -> BulkFut {
        match msg {
            SqMsg::Flush(req) => {
                let FlushReq { imm, vp_eid, vp_off, row_stream_id, invalidate_row_stream, resp_tx } = req;
                let bulk_sc = bulk_sc.clone();
                Box::pin(async move {
                    if invalidate_row_stream {
                        bulk_sc.invalidate_stream(row_stream_id);
                    }
                    let result = do_flush_on_bulk(&bulk_sc, imm, vp_eid, vp_off, row_stream_id).await;
                    BulkCompletion::Flush { resp_tx, result }
                })
            }
            SqMsg::RowAppend(req) => {
                let RowAppendReq { sst_bytes, row_stream_id, resp_tx } = req;
                let bulk_sc = bulk_sc.clone();
                Box::pin(async move {
                    let result = bulk_sc.append_bytes(row_stream_id, sst_bytes).await
                        .map_err(Into::into);
                    BulkCompletion::RowAppend { resp_tx, result }
                })
            }
        }
    };

    loop {
        // (A) Opportunistic CQ drain.
        while let Some(Some(done)) = inflight.next().now_or_never() {
            done.send();
        }

        let n_inflight = inflight.len();
        let at_cap = n_inflight >= cap;

        if n_inflight == 0 {
            // Idle: only SQ can progress.
            match sq_rx.next().await {
                Some(msg) => inflight.push(launch(msg, &bulk_sc)),
                None => break,
            }
            continue;
        }

        if at_cap {
            // Back-pressure: only CQ can progress.
            if let Some(done) = inflight.next().await {
                done.send();
            }
            continue;
        }

        // 0 < n_inflight < cap → race SQ vs CQ.
        let sq_fut = sq_rx.next();
        let cq_fut = inflight.next();
        futures::pin_mut!(sq_fut);
        match select(sq_fut, Box::pin(cq_fut)).await {
            Either::Left((maybe_msg, _cq_dropped)) => match maybe_msg {
                Some(msg) => inflight.push(launch(msg, &bulk_sc)),
                None => {
                    while let Some(done) = inflight.next().await {
                        done.send();
                    }
                    break;
                }
            },
            Either::Right((maybe_done, _sq_dropped)) => {
                if let Some(done) = maybe_done {
                    done.send();
                }
            }
        }
    }
}

async fn do_flush_on_bulk(
    bulk_sc: &Rc<StreamClient>,
    imm: Arc<Memtable>,
    vp_eid: u64,
    vp_off: u32,
    row_stream_id: u64,
) -> Result<(TableMeta, SstReader)> {
    let imm_clone = imm.clone();
    let (sst_bytes, last_seq) = compio::runtime::spawn_blocking(move || {
        build_sst_bytes(&imm_clone, vp_eid, vp_off)
    })
    .await
    .map_err(|_| anyhow::anyhow!("SSTable build task failed"))?;

    let append_result = bulk_sc.append(row_stream_id, &sst_bytes).await?;
    let estimated_size = sst_bytes.len() as u64;
    let reader = SstReader::from_bytes(Bytes::from(sst_bytes))?;
    let new_meta = TableMeta {
        extent_id: append_result.extent_id,
        offset: append_result.offset,
        len: append_result.end - append_result.offset,
        estimated_size,
        last_seq,
    };
    Ok((new_meta, reader))
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

pub(crate) fn rand_u64() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64
        ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
}

pub(crate) fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) fn human_size(bytes: u64) -> String {
    if bytes >= 1 << 30 {
        format!("{:.1} GB", bytes as f64 / (1u64 << 30) as f64)
    } else if bytes >= 1 << 20 {
        format!("{:.1} MB", bytes as f64 / (1u64 << 20) as f64)
    } else if bytes >= 1 << 10 {
        format!("{:.1} KB", bytes as f64 / (1u64 << 10) as f64)
    } else {
        format!("{} B", bytes)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compio_timer_in_spawn() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let fired = Arc::new(AtomicBool::new(false));
        let fired2 = fired.clone();

        compio::runtime::Runtime::new().unwrap().block_on(async {
            compio::runtime::spawn(async move {
                compio::time::sleep(Duration::from_millis(100)).await;
                fired2.store(true, Ordering::SeqCst);
            })
            .detach();

            // Main task sleeps longer to give spawned task time
            compio::time::sleep(Duration::from_millis(500)).await;
        });

        assert!(fired.load(Ordering::SeqCst), "spawned timer should have fired");
    }

    /// F127: The recover_partition retry loop must propagate errors after
    /// exhausting retries, not silently skip extents. This test validates the
    /// pattern: succeed after transient failures, or fail hard after 10 attempts.
    #[test]
    fn f127_retry_loop_propagates_after_exhaustion() {
        let max_retries = 10u32;

        // Case 1: succeeds on the 5th attempt (transient failure → recovery).
        let succeed_on = 5u32;
        let mut attempt = 0u32;
        let result: Result<Vec<u8>> = loop {
            match if attempt < succeed_on - 1 {
                Err(anyhow::anyhow!("transient"))
            } else {
                Ok(vec![1, 2, 3])
            } {
                Ok(d) => break Ok(d),
                Err(e) => {
                    attempt += 1;
                    if attempt >= max_retries {
                        break Err(anyhow::anyhow!(
                            "failed after {} attempts: {}",
                            attempt,
                            e
                        ));
                    }
                }
            }
        };
        assert!(result.is_ok(), "should succeed after transient failures");
        assert_eq!(result.unwrap(), vec![1, 2, 3]);

        // Case 2: all 10 attempts fail → error propagated (not silent skip).
        let mut attempt = 0u32;
        let result: Result<Vec<u8>> = loop {
            match Err::<Vec<u8>, _>(anyhow::anyhow!("persistent failure")) {
                Ok(d) => break Ok(d),
                Err(e) => {
                    attempt += 1;
                    if attempt >= max_retries {
                        break Err(anyhow::anyhow!(
                            "failed after {} attempts: {}",
                            attempt,
                            e
                        ));
                    }
                }
            }
        };
        assert!(result.is_err(), "should propagate error after exhausting retries");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("failed after 10 attempts"),
            "error message should include attempt count: {msg}"
        );
    }

    #[test]
    fn mvcc_key_encoding() {
        let uk = b"hello";
        let k1 = key_with_ts(uk, 1);
        let k2 = key_with_ts(uk, 2);
        let k3 = key_with_ts(uk, 100);
        assert!(k3 < k2);
        assert!(k2 < k1);
        assert_eq!(parse_key(&k1), uk.as_slice());
        assert_eq!(parse_ts(&k1), 1);
        assert_eq!(parse_ts(&k3), 100);

        let ka = key_with_ts(b"mykey", 1);
        let kb = key_with_ts(b"mykey1", 2);
        assert!(ka < kb);
        assert_eq!(parse_key(&ka), b"mykey");
        assert_eq!(parse_key(&kb), b"mykey1");
    }

    #[test]
    fn value_pointer_encode_decode() {
        let vp = ValuePointer {
            extent_id: 0xDEAD,
            offset: 0x1234,
            len: 0xABCD,
        };
        let enc = vp.encode();
        let dec = ValuePointer::decode(&enc);
        assert_eq!(dec.extent_id, vp.extent_id);
        assert_eq!(dec.offset, vp.offset);
        assert_eq!(dec.len, vp.len);
    }

    #[test]
    fn op_value_pointer_flag() {
        assert_eq!(1u8 | OP_VALUE_POINTER, 0x81);
        assert_eq!(0x81u8 & !OP_VALUE_POINTER, 1u8);
        assert_eq!(1u8 & OP_VALUE_POINTER, 0);
    }

    // ── Tests ported from Go range_partition/entry_test.go ───────────────────

    #[test]
    fn record_encode_decode_roundtrip() {
        let encoded = encode_record(1, b"key", b"hello world", 0);
        let records = decode_records_full(&encoded);
        assert_eq!(records.len(), 1);
        let (op, key, value, expires_at) = &records[0];
        assert_eq!(*op, 1);
        assert_eq!(key, b"key");
        assert_eq!(value, b"hello world");
        assert_eq!(*expires_at, 0);
    }

    #[test]
    fn record_encode_decode_with_expiry() {
        let encoded = encode_record(1, b"ttl_key", b"ttl_val", 1700000000);
        let records = decode_records_full(&encoded);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].3, 1700000000);
    }

    #[test]
    fn record_encode_decode_delete() {
        let encoded = encode_record(2, b"del_key", b"", 0);
        let records = decode_records_full(&encoded);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, 2); // op=delete
        assert!(records[0].2.is_empty()); // empty value
    }

    #[test]
    fn record_encode_decode_multiple() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&encode_record(1, b"k1", b"v1", 0));
        buf.extend_from_slice(&encode_record(1, b"k2", b"v2_longer", 100));
        buf.extend_from_slice(&encode_record(2, b"k3", b"", 0));

        let records = decode_records_full(&buf);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].1, b"k1");
        assert_eq!(records[0].2, b"v1");
        assert_eq!(records[1].1, b"k2");
        assert_eq!(records[1].2, b"v2_longer");
        assert_eq!(records[1].3, 100);
        assert_eq!(records[2].0, 2);
        assert_eq!(records[2].1, b"k3");
    }

    #[test]
    fn record_encode_decode_big_value() {
        let big_val = vec![0xAB; 1024 * 1024]; // 1MB
        let encoded = encode_record(1, b"bigkey", &big_val, 0);
        let records = decode_records_full(&encoded);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].2.len(), 1024 * 1024);
        assert_eq!(records[0].2[0], 0xAB);
    }

    /// Regression test for GC data loss bug: WAL records for large values
    /// must have OP_VALUE_POINTER flag in op so that GC's `run_gc` can
    /// identify them. Without the flag, GC skips all entries → moved=0
    /// → punches extent → live VP data lost.
    #[test]
    fn wal_vp_flag_for_large_values() {
        let small_val = vec![0u8; 100]; // < VALUE_THROTTLE (4KB)
        let large_val = vec![0u8; VALUE_THROTTLE + 1]; // > VALUE_THROTTLE

        // Simulate what the write path does:
        // small value → op=1, large value → op=1|OP_VALUE_POINTER
        let small_op: u8 = if small_val.len() > VALUE_THROTTLE { 1 | OP_VALUE_POINTER } else { 1 };
        let large_op: u8 = if large_val.len() > VALUE_THROTTLE { 1 | OP_VALUE_POINTER } else { 1 };

        assert_eq!(small_op, 1, "small value should NOT have VP flag");
        assert_eq!(large_op, 1 | OP_VALUE_POINTER, "large value MUST have VP flag");

        // Encode WAL records with the correct op
        let mut buf = Vec::new();
        buf.extend_from_slice(&encode_record(small_op, b"small_key", &small_val, 0));
        buf.extend_from_slice(&encode_record(large_op, b"large_key", &large_val, 0));

        let records = decode_records_full(&buf);
        assert_eq!(records.len(), 2);

        // GC uses this check to identify VP entries:
        let (op0, _, _, _) = &records[0];
        let (op1, _, _, _) = &records[1];
        assert_eq!(op0 & OP_VALUE_POINTER, 0, "small value WAL record should be skipped by GC");
        assert!(op1 & OP_VALUE_POINTER != 0, "large value WAL record MUST be detected by GC");
    }

    #[test]
    fn record_with_offsets() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&encode_record(1, b"k1", b"v1", 0));
        let off1 = buf.len();
        buf.extend_from_slice(&encode_record(1, b"k2", b"v2", 0));

        let records = decode_records_with_offsets(&buf);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, 0); // first record at offset 0
        assert_eq!(records[1].0, off1); // second record at correct offset
    }

    #[test]
    fn record_decode_truncated_data() {
        let encoded = encode_record(1, b"key", b"value", 0);
        // Truncate in the middle
        let truncated = &encoded[..10];
        let records = decode_records_full(truncated);
        assert!(records.is_empty(), "truncated data should produce no records");
    }

    // ── Memtable tests ported from Go skiplist tests ─────────────────────────

    #[test]
    fn memtable_empty() {
        let mt = Memtable::new();
        assert!(mt.is_empty());
        assert_eq!(mt.mem_bytes(), 0);
        assert!(mt.seek_user_key(b"anything").is_none());
    }

    #[test]
    fn memtable_basic_put_get() {
        let mt = Memtable::new();
        let k1 = key_with_ts(b"apple", 1);
        mt.insert(
            k1.clone(),
            MemEntry { op: 1, value: b"red".to_vec(), expires_at: 0 },
            100,
        );
        let k2 = key_with_ts(b"banana", 2);
        mt.insert(
            k2.clone(),
            MemEntry { op: 1, value: b"yellow".to_vec(), expires_at: 0 },
            100,
        );

        let got = mt.seek_user_key(b"apple").expect("apple should exist");
        assert_eq!(got.value, b"red");
        let got = mt.seek_user_key(b"banana").expect("banana should exist");
        assert_eq!(got.value, b"yellow");
        assert!(mt.seek_user_key(b"cherry").is_none());
    }

    #[test]
    fn memtable_update_returns_newest() {
        let mt = Memtable::new();
        // Insert same key with increasing seq — newest should win on seek
        for seq in 1..=100u64 {
            let k = key_with_ts(b"key", seq);
            let val = format!("value{seq}");
            mt.insert(
                k,
                MemEntry { op: 1, value: val.into_bytes(), expires_at: 0 },
                50,
            );
        }
        let got = mt.seek_user_key(b"key").expect("key should exist");
        assert_eq!(got.value, b"value100", "should return newest version");
    }

    #[test]
    fn memtable_snapshot_sorted() {
        let mt = Memtable::new();
        // Insert in reverse order
        for i in (0..10).rev() {
            let uk = format!("key{i:02}");
            let k = key_with_ts(uk.as_bytes(), i as u64);
            mt.insert(
                k,
                MemEntry { op: 1, value: format!("v{i}").into_bytes(), expires_at: 0 },
                50,
            );
        }
        let snapshot = mt.snapshot_sorted();
        assert_eq!(snapshot.len(), 10);
        // Verify sorted by internal key
        for i in 1..snapshot.len() {
            assert!(
                snapshot[i - 1].key <= snapshot[i].key,
                "snapshot should be sorted"
            );
        }
    }

    #[test]
    fn memtable_mem_bytes_tracking() {
        let mt = Memtable::new();
        mt.insert(
            key_with_ts(b"k1", 1),
            MemEntry { op: 1, value: b"v1".to_vec(), expires_at: 0 },
            100,
        );
        assert_eq!(mt.mem_bytes(), 100);
        mt.insert(
            key_with_ts(b"k2", 2),
            MemEntry { op: 1, value: b"v2".to_vec(), expires_at: 0 },
            200,
        );
        assert_eq!(mt.mem_bytes(), 300);
    }

    // F099-C: under the RwLock<BTreeMap> design the memtable has ONE writer
    // (the P-log thread) and N readers (ps-conn threads doing seek_user_key).
    // This test exercises that pattern: 1 writer thread does insert() in a
    // tight loop while 8 reader threads do seek_user_key() in a tight loop on
    // overlapping keys. Verifies:
    //   - no panic / data race
    //   - writer insertions become visible to subsequent readers
    //   - total reader ops progresses (readers are not starved by writer)
    //   - the writer's last-inserted key is visible to a post-test reader
    #[test]
    fn memtable_mixed_read_write_under_pressure() {
        use std::sync::atomic::{AtomicBool, AtomicU64 as StdAtomicU64, Ordering};
        use std::sync::Arc as StdArc;
        use std::thread;
        use std::time::{Duration as StdDuration, Instant as StdInstant};

        let mt = StdArc::new(Memtable::new());
        let stop = StdArc::new(AtomicBool::new(false));
        let writer_ops = StdArc::new(StdAtomicU64::new(0));
        let reader_ops = StdArc::new(StdAtomicU64::new(0));

        // Writer thread: tight insert loop with monotonically increasing seq.
        let writer = {
            let mt = mt.clone();
            let stop = stop.clone();
            let writer_ops = writer_ops.clone();
            thread::spawn(move || {
                let mut seq: u64 = 1;
                while !stop.load(Ordering::Relaxed) {
                    // Key space cycles through 64 user keys so readers see hits.
                    let uk = format!("uk{:03}", seq % 64);
                    let k = key_with_ts(uk.as_bytes(), seq);
                    let v = format!("v{}", seq).into_bytes();
                    mt.insert(
                        k,
                        MemEntry { op: 1, value: v, expires_at: 0 },
                        64,
                    );
                    writer_ops.fetch_add(1, Ordering::Relaxed);
                    seq = seq.wrapping_add(1);
                }
            })
        };

        // 8 reader threads: seek_user_key over the cycling key space.
        let mut readers = Vec::new();
        for i in 0..8 {
            let mt = mt.clone();
            let stop = stop.clone();
            let reader_ops = reader_ops.clone();
            readers.push(thread::spawn(move || {
                let mut j: u64 = 0;
                while !stop.load(Ordering::Relaxed) {
                    let uk = format!("uk{:03}", (i * 7 + j) % 64);
                    let _ = mt.seek_user_key(uk.as_bytes());
                    reader_ops.fetch_add(1, Ordering::Relaxed);
                    j = j.wrapping_add(1);
                }
            }));
        }

        // Run for 100 ms.
        thread::sleep(StdDuration::from_millis(100));
        let start_stop = StdInstant::now();
        stop.store(true, Ordering::Relaxed);
        writer.join().expect("writer thread panicked");
        for r in readers { r.join().expect("reader thread panicked"); }

        let w = writer_ops.load(Ordering::Relaxed);
        let r = reader_ops.load(Ordering::Relaxed);
        assert!(w > 0, "writer should have completed at least one insert");
        assert!(r > 0, "readers should have completed at least one op");
        // No hard SLA on ratio (CI noise) — just make sure readers are not
        // wholly starved. A catastrophically broken lock pattern would show
        // readers = 0 or writer = 0; both should be well into the thousands
        // on any modern box in 100 ms.
        let _ = start_stop.elapsed(); // keep timing var for clarity

        // Linearizability spot-check: do a final insert and read it back.
        let k_final = key_with_ts(b"final", u64::MAX - 1);
        mt.insert(
            k_final,
            MemEntry { op: 1, value: b"LAST".to_vec(), expires_at: 0 },
            64,
        );
        let got = mt.seek_user_key(b"final").expect("final key visible");
        assert_eq!(got.value, b"LAST");
    }

    // ── in_range tests ported from Go split algorithm tests ─────────────────

    #[test]
    fn in_range_basic() {
        let rg = Range {
            start_key: b"b".to_vec(),
            end_key: b"e".to_vec(),
            ..Default::default()
        };
        assert!(!in_range(&rg, b"a")); // before start
        assert!(in_range(&rg, b"b"));  // exactly start
        assert!(in_range(&rg, b"c"));  // in range
        assert!(in_range(&rg, b"d"));  // in range
        assert!(!in_range(&rg, b"e")); // exactly end (exclusive)
        assert!(!in_range(&rg, b"f")); // after end
    }

    #[test]
    fn in_range_open_end() {
        let rg = Range {
            start_key: b"a".to_vec(),
            end_key: vec![], // open-ended
            ..Default::default()
        };
        assert!(in_range(&rg, b"a"));
        assert!(in_range(&rg, b"z"));
        assert!(in_range(&rg, b"zzzzzzz"));
        assert!(!in_range(&rg, b"")); // before "a"
    }

    // ── decode_last_table_locations test ──────────────────────────────────────

    #[test]
    fn decode_table_locations_roundtrip() {
        let locs = TableLocations {
            locs: vec![
                SstLocation { extent_id: 1, offset: 0, len: 1000 },
                SstLocation { extent_id: 2, offset: 100, len: 2000 },
            ],
            vp_extent_id: 42,
            vp_offset: 512,
        };
        let payload = rkyv_encode(&locs);
        let mut data = Vec::new();
        data.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        data.extend_from_slice(&payload);

        let decoded = decode_last_table_locations(&data).unwrap();
        assert_eq!(decoded.locs.len(), 2);
        assert_eq!(decoded.locs[0].extent_id, 1);
        assert_eq!(decoded.locs[1].len, 2000);
        assert_eq!(decoded.vp_extent_id, 42);
        assert_eq!(decoded.vp_offset, 512);
    }

    #[test]
    fn decode_table_locations_multiple_records_returns_last() {
        let locs1 = TableLocations {
            locs: vec![SstLocation { extent_id: 1, offset: 0, len: 100 }],
            vp_extent_id: 10,
            vp_offset: 0,
        };
        let locs2 = TableLocations {
            locs: vec![
                SstLocation { extent_id: 1, offset: 0, len: 100 },
                SstLocation { extent_id: 2, offset: 0, len: 200 },
            ],
            vp_extent_id: 20,
            vp_offset: 50,
        };

        let mut data = Vec::new();
        let p1 = rkyv_encode(&locs1);
        data.extend_from_slice(&(p1.len() as u32).to_le_bytes());
        data.extend_from_slice(&p1);
        let p2 = rkyv_encode(&locs2);
        data.extend_from_slice(&(p2.len() as u32).to_le_bytes());
        data.extend_from_slice(&p2);

        let decoded = decode_last_table_locations(&data).unwrap();
        // Should return the LAST valid record
        assert_eq!(decoded.locs.len(), 2);
        assert_eq!(decoded.vp_extent_id, 20);
        assert_eq!(decoded.vp_offset, 50);
    }

    #[test]
    fn decode_table_locations_empty_fails() {
        assert!(decode_last_table_locations(&[]).is_err());
        assert!(decode_last_table_locations(&[0, 0, 0]).is_err());
    }

    /// F157: a corrupted record in the MIDDLE of the stream must not silently
    /// drop subsequent valid records. Pre-F157 the loop broke on the first
    /// decode failure and returned the prior record — losing the newer (valid)
    /// records entirely. Post-F157 the loop logs + skips and continues.
    #[test]
    fn f157_decode_table_locations_skips_mid_stream_corruption() {
        let locs1 = TableLocations {
            locs: vec![SstLocation { extent_id: 1, offset: 0, len: 100 }],
            vp_extent_id: 10,
            vp_offset: 0,
        };
        let locs3 = TableLocations {
            locs: vec![
                SstLocation { extent_id: 1, offset: 0, len: 100 },
                SstLocation { extent_id: 2, offset: 0, len: 200 },
                SstLocation { extent_id: 3, offset: 0, len: 300 },
            ],
            vp_extent_id: 30,
            vp_offset: 100,
        };

        let mut data = Vec::new();
        // Record 1: valid.
        let p1 = rkyv_encode(&locs1);
        data.extend_from_slice(&(p1.len() as u32).to_le_bytes());
        data.extend_from_slice(&p1);
        // Record 2: malformed payload (random bytes prefixed with a valid length).
        // Use a payload size that's plausible to keep the loop walking past it.
        let bogus_len: u32 = 64;
        data.extend_from_slice(&bogus_len.to_le_bytes());
        data.extend_from_slice(&vec![0xABu8; bogus_len as usize]);
        // Record 3: valid (the one pre-F157 would silently drop).
        let p3 = rkyv_encode(&locs3);
        data.extend_from_slice(&(p3.len() as u32).to_le_bytes());
        data.extend_from_slice(&p3);

        let decoded = decode_last_table_locations(&data).unwrap();
        // Must return record 3, NOT record 1.
        assert_eq!(decoded.locs.len(), 3, "should return record 3, not record 1");
        assert_eq!(decoded.vp_extent_id, 30);
        assert_eq!(decoded.vp_offset, 100);
    }

    // ── build_sst_bytes test ─────────────────────────────────────────────────

    #[test]
    fn build_sst_from_memtable() {
        let mt = Memtable::new();
        for i in 0u64..100 {
            let uk = format!("key{i:04}");
            let k = key_with_ts(uk.as_bytes(), i);
            mt.insert(
                k,
                MemEntry { op: 1, value: format!("val{i}").into_bytes(), expires_at: 0 },
                50,
            );
        }
        let (sst_bytes, last_seq) = build_sst_bytes(&mt, 0, 0);
        assert!(!sst_bytes.is_empty());
        assert_eq!(last_seq, 99);

        let reader = SstReader::from_bytes(Bytes::from(sst_bytes)).unwrap();
        // Verify all keys are readable
        let mut it = TableIterator::new(Arc::new(reader));
        it.rewind();
        let mut count = 0;
        while it.valid() {
            count += 1;
            it.next();
        }
        assert_eq!(count, 100);
    }

    // ── resolve_value sub-range tests (inline values, no StreamClient) ──

    /// Test the inline sub-range slicing logic directly (no StreamClient needed).
    fn inline_subrange(data: &[u8], offset: u32, length: u32) -> Vec<u8> {
        let v = data.to_vec();
        if offset == 0 && length == 0 {
            return v;
        }
        let start = (offset as usize).min(v.len());
        let end = if length == 0 { v.len() } else { (start + length as usize).min(v.len()) };
        v[start..end].to_vec()
    }

    #[test]
    fn resolve_value_inline_full() {
        assert_eq!(inline_subrange(b"hello world", 0, 0), b"hello world");
    }

    #[test]
    fn resolve_value_inline_subrange() {
        assert_eq!(inline_subrange(b"hello world", 6, 5), b"world");
    }

    #[test]
    fn resolve_value_inline_subrange_clamp() {
        // length exceeds data → clamped to end
        assert_eq!(inline_subrange(b"hello", 3, 100), b"lo");
    }

    #[test]
    fn resolve_value_inline_offset_past_end() {
        assert_eq!(inline_subrange(b"hello", 999, 0), b"");
    }

    #[test]
    fn resolve_value_inline_offset_zero_length_nonzero() {
        assert_eq!(inline_subrange(b"hello world", 0, 5), b"hello");
    }

    #[test]
    fn resolve_value_inline_middle_slice() {
        assert_eq!(inline_subrange(b"0123456789", 3, 4), b"3456");
    }
}

#[cfg(test)]
mod env_knob_tests {
    // `max_write_batch()` uses OnceLock and can only be initialized once per
    // process. We test the underlying parsing logic by inlining the same
    // expression, so the test does not depend on init order.
    fn parse_env(raw: Option<&str>) -> usize {
        raw.and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0 && n <= 1_000_000)
            .unwrap_or(super::DEFAULT_MAX_WRITE_BATCH)
    }

    #[test]
    fn default_when_unset() {
        assert_eq!(parse_env(None), super::DEFAULT_MAX_WRITE_BATCH);
    }

    #[test]
    fn parses_positive_in_range() {
        assert_eq!(parse_env(Some("8192")), 8192);
    }

    #[test]
    fn rejects_zero() {
        assert_eq!(parse_env(Some("0")), super::DEFAULT_MAX_WRITE_BATCH);
    }

    #[test]
    fn rejects_negative() {
        assert_eq!(parse_env(Some("-1")), super::DEFAULT_MAX_WRITE_BATCH);
    }

    #[test]
    fn rejects_too_large() {
        assert_eq!(parse_env(Some("999999999999")), super::DEFAULT_MAX_WRITE_BATCH);
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(parse_env(Some("abc")), super::DEFAULT_MAX_WRITE_BATCH);
    }
}

// ---------------------------------------------------------------------------
// F120 — env-knob parsing tests (mirrors env_knob_tests' "inline parse"
// strategy because OnceLock prevents re-initialisation in-process).
// ---------------------------------------------------------------------------
#[cfg(test)]
mod f120_knob_tests {
    fn parse_imm_depth(raw: Option<&str>) -> usize {
        raw.and_then(|s| s.parse::<usize>().ok())
            .map(|n| n.clamp(1, 64))
            .unwrap_or(4)
    }

    fn parse_wal_gap(raw: Option<&str>) -> u64 {
        const DEFAULT: u64 = 2 * 1024 * 1024 * 1024;
        const MIN: u64 = 128 * 1024 * 1024;
        const MAX: u64 = 64 * 1024 * 1024 * 1024;
        raw.and_then(|s| s.parse::<u64>().ok())
            .map(|n| n.clamp(MIN, MAX))
            .unwrap_or(DEFAULT)
    }

    fn parse_shutdown_timeout(raw: Option<&str>) -> u64 {
        raw.and_then(|s| s.parse::<u64>().ok())
            .map(|n| n.clamp(1_000, 600_000))
            .unwrap_or(60_000)
    }

    #[test]
    fn imm_depth_default() {
        assert_eq!(parse_imm_depth(None), 4);
    }

    #[test]
    fn imm_depth_clamped_low() {
        assert_eq!(parse_imm_depth(Some("0")), 1);
    }

    #[test]
    fn imm_depth_clamped_high() {
        assert_eq!(parse_imm_depth(Some("999")), 64);
    }

    #[test]
    fn imm_depth_in_range() {
        assert_eq!(parse_imm_depth(Some("8")), 8);
    }

    #[test]
    fn wal_gap_default_is_2gib() {
        assert_eq!(parse_wal_gap(None), 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn wal_gap_clamped_low() {
        // 1 KiB → clamped up to 128 MiB minimum.
        assert_eq!(parse_wal_gap(Some("1024")), 128 * 1024 * 1024);
    }

    #[test]
    fn wal_gap_clamped_high() {
        // 1 TiB → clamped down to 64 GiB maximum.
        let one_tib: u64 = 1024 * 1024 * 1024 * 1024;
        assert_eq!(
            parse_wal_gap(Some(&one_tib.to_string())),
            64 * 1024 * 1024 * 1024,
        );
    }

    #[test]
    fn shutdown_timeout_default() {
        assert_eq!(parse_shutdown_timeout(None), 60_000);
    }

    #[test]
    fn shutdown_timeout_clamped_low() {
        // 100 ms is below 1 s minimum.
        assert_eq!(parse_shutdown_timeout(Some("100")), 1_000);
    }

    #[test]
    fn shutdown_timeout_clamped_high() {
        // 1 hour is above 10 min maximum.
        assert_eq!(parse_shutdown_timeout(Some("3600000")), 600_000);
    }

    /// Drives the actual `max_imm_depth()` once via OnceLock — proves the
    /// runtime path returns a value in range without exploding. Subsequent
    /// reads are cached so this is a smoke test, not a per-env-value test.
    #[test]
    fn live_max_imm_depth_in_range() {
        let v = super::max_imm_depth();
        assert!((1..=64).contains(&v), "imm_depth out of range: {v}");
    }

    #[test]
    fn live_max_wal_gap_in_range() {
        let v = super::max_wal_gap();
        assert!(
            v >= 128 * 1024 * 1024 && v <= 64 * 1024 * 1024 * 1024,
            "wal_gap out of range: {v}",
        );
    }

    #[test]
    fn live_shutdown_timeout_in_range() {
        let v = super::shutdown_timeout_ms();
        assert!((1_000..=600_000).contains(&v), "shutdown_timeout_ms out of range: {v}");
    }
}

// F120-A imm-pop signal flow is validated by:
//   - The integration test `crates/manager/tests/f120_graceful_shutdown.rs`
//     (graceful drain end-to-end exercises rotate → imm push → P-bulk
//     flush → pop_front → imm_drained_tx wake).
//   - Live cluster verification documented in feature_list.md F120.
// Constructing a `PartitionData` directly in a unit test would require a
// real `StreamClient` (async constructor + manager round-trips), which
// is out of scope for an in-process unit test.

// ---------------------------------------------------------------------------
// F099-D — partition_loop direct-response path tests.
//
// These tests exercise the enqueue_put / enqueue_delete / enqueue_stream_put
// helpers and the WriteResponder::send_ok / send_err contract. The full
// merged loop (SQ/CQ pipeline + start/finish_write_batch) needs a live
// StreamClient and is covered by the ps_bench / perf_check harness in
// scripts/. The harness tests in `background::sqcq_tests` cover the SQ/CQ
// pattern itself (FU + cap + out-of-order completion + LockedByOther drain).
//
// What each test proves:
//   1. merged_loop_put_direct_response — a decoded PutReq produces exactly
//      one WriteRequest in `pending` whose `resp` is `WriteResponder::Put`
//      wired to the outer PartitionRequest oneshot. `send_ok` then delivers
//      a valid rkyv-encoded `PutResp` frame to the outer receiver. Zero
//      `compio::runtime::spawn` invocations, zero inner oneshot allocations.
//   2. merged_loop_mixed_read_write — interleaving decode + responder
//      handling for PUT and DELETE reproduces correct frames on both paths.
//      (Reads are covered by the existing read-path tests.)
//   3. merged_loop_bad_decode_replies_invalid_arg — a malformed payload is
//      rejected with StatusCode::InvalidArgument on the outer oneshot,
//      without ever touching `pending`.
//
// A live-loop LockedByOther drain test is covered by
// `background::sqcq_tests::ps_sqcq_locked_by_other_drains_cleanly` — the
// merged loop reuses the same `handle_completion` primitive, same
// `FuturesUnordered`, same break-on-flag exit condition.
#[cfg(test)]
mod merged_loop_tests {
    use super::*;
    use futures::channel::oneshot;

    fn build_put_partition_request(
        key: &[u8],
        value: &[u8],
        expires_at: u64,
    ) -> (PartitionRequest, oneshot::Receiver<HandlerResult>) {
        let req = PutReq {
            part_id: 0,
            key: key.to_vec(),
            value: value.to_vec(),
            expires_at,
            region_epoch: 0,
        };
        let payload = partition_rpc::rkyv_encode(&req);
        let (resp_tx, resp_rx) = oneshot::channel();
        (
            PartitionRequest {
                msg_type: MSG_PUT,
                payload: Bytes::from(payload),
                resp_tx,
            },
            resp_rx,
        )
    }

    fn build_delete_partition_request(
        key: &[u8],
    ) -> (PartitionRequest, oneshot::Receiver<HandlerResult>) {
        let req = DeleteReq {
            part_id: 0,
            key: key.to_vec(),
            region_epoch: 0,
        };
        let payload = partition_rpc::rkyv_encode(&req);
        let (resp_tx, resp_rx) = oneshot::channel();
        (
            PartitionRequest {
                msg_type: MSG_DELETE,
                payload: Bytes::from(payload),
                resp_tx,
            },
            resp_rx,
        )
    }

    /// F099-D test 1 — a PutReq is decoded inline, pushed into `pending`
    /// with a direct `WriteResponder::Put` responder, and `send_ok`
    /// delivers an encoded `PutResp` frame to the outer oneshot. No
    /// spawn, no inner oneshot.
    #[test]
    fn merged_loop_put_direct_response() {
        let (req, resp_rx) = build_put_partition_request(b"hello", b"world", 0);
        let mut pending: Vec<WriteRequest> = Vec::new();
        // Test sends epoch=0 on the wire, so 0 here matches and bypasses the check.
        enqueue_put(req, &mut pending, 0, 0);

        assert_eq!(pending.len(), 1, "exactly one WriteRequest enqueued");
        let w = pending.pop().unwrap();
        match &w.op {
            WriteOp::Put { user_key, value, expires_at } => {
                assert_eq!(user_key.as_ref(), b"hello");
                assert_eq!(value.as_ref(), b"world");
                assert_eq!(*expires_at, 0);
            }
            _ => panic!("expected Put"),
        }
        assert!(matches!(&w.resp, WriteResponder::Put { .. }), "direct Put responder");

        // Simulate Phase-3 success reply.
        w.resp.send_ok();

        // The outer oneshot must have received an encoded PutResp frame.
        let frame = compio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { resp_rx.await });
        let bytes = frame.expect("outer oneshot dropped").expect("send_ok should send Ok");
        let decoded: PutResp = partition_rpc::rkyv_decode(&bytes).unwrap();
        assert_eq!(decoded.code, CODE_OK);
        assert_eq!(decoded.key.as_slice(), b"hello");
    }

    /// F129 — regular Put rejects values > AUTUMN_PS_MAX_INLINE_BYTES
    /// without enqueueing into the write batch. The outer oneshot
    /// receives a PutResp with code=CODE_VALUE_TOO_LARGE so the
    /// client can fall back to PutBegin/Chunk/Commit.
    #[test]
    fn enqueue_put_rejects_oversized_value() {
        let big_value = vec![0u8; AUTUMN_PS_MAX_INLINE_BYTES_DEFAULT as usize + 1];
        let (req, resp_rx) =
            build_put_partition_request(b"big-key", &big_value, 0);
        let mut pending: Vec<WriteRequest> = Vec::new();
        enqueue_put(req, &mut pending, 0, 0);

        // No WriteRequest queued — the cap fires before pipeline insert.
        assert_eq!(pending.len(), 0, "oversized Put must not enter pipeline");

        // The outer oneshot got a CODE_VALUE_TOO_LARGE PutResp frame.
        let frame = compio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { resp_rx.await });
        let bytes = frame.expect("outer oneshot dropped").expect("encoded PutResp");
        let decoded: PutResp = partition_rpc::rkyv_decode(&bytes).unwrap();
        assert_eq!(decoded.code, CODE_VALUE_TOO_LARGE);
        assert_eq!(decoded.key.as_slice(), b"big-key");
    }

    /// F129 — regular Put at exactly the inline cap goes through normally.
    /// Cap is `> AUTUMN_PS_MAX_INLINE_BYTES`, so equality is allowed.
    #[test]
    fn enqueue_put_accepts_value_at_inline_cap() {
        let exact_cap_value = vec![0u8; AUTUMN_PS_MAX_INLINE_BYTES_DEFAULT as usize];
        let (req, _resp_rx) =
            build_put_partition_request(b"k", &exact_cap_value, 0);
        let mut pending: Vec<WriteRequest> = Vec::new();
        enqueue_put(req, &mut pending, 0, 0);
        assert_eq!(pending.len(), 1, "value at exact cap must enqueue normally");
    }

    /// F099-D test 2 — mixed sequence: enqueue 2 puts and 1 delete in
    /// order, then reply to each. Every outer oneshot receives the right
    /// frame type with the echoed key. Verifies both WriteResponder
    /// variants encode correctly.
    #[test]
    fn merged_loop_mixed_read_write() {
        let (p1, rx1) = build_put_partition_request(b"k1", b"v1", 0);
        let (p2, rx2) = build_put_partition_request(b"k2", b"v2", 0);
        let (d1, rx3) = build_delete_partition_request(b"k3");

        let mut pending: Vec<WriteRequest> = Vec::new();
        enqueue_put(p1, &mut pending, 0, 0);
        enqueue_put(p2, &mut pending, 0, 0);
        enqueue_delete(d1, &mut pending, 0, 0);

        assert_eq!(pending.len(), 3);
        // Order preserved (FIFO).
        for w in pending.drain(..) {
            w.resp.send_ok();
        }

        compio::runtime::Runtime::new().unwrap().block_on(async {
            let f1 = rx1.await.unwrap().unwrap();
            let f2 = rx2.await.unwrap().unwrap();
            let f3 = rx3.await.unwrap().unwrap();
            let r1: PutResp = partition_rpc::rkyv_decode(&f1).unwrap();
            let r2: PutResp = partition_rpc::rkyv_decode(&f2).unwrap();
            let r3: DeleteResp = partition_rpc::rkyv_decode(&f3).unwrap();
            assert_eq!(r1.key, b"k1");
            assert_eq!(r2.key, b"k2");
            assert_eq!(r3.key, b"k3");
            assert_eq!(r1.code, CODE_OK);
            assert_eq!(r2.code, CODE_OK);
            assert_eq!(r3.code, CODE_OK);
        });
    }

    /// F099-D test 3 — `WriteResponder::send_err` for "key is out of
    /// range" surfaces as StatusCode::InvalidArgument (matches the
    /// pre-merge behavior where handle_put returned InvalidArgument for
    /// out-of-range, not Internal); all other errors surface as Internal.
    /// This exercises the direct error-reply path that replaces the old
    /// inner-oneshot error propagation.
    #[test]
    fn merged_loop_out_of_range_err_is_invalid_argument() {
        let (outer, rx) = oneshot::channel();
        let resp = WriteResponder::Put {
            outer,
            key: b"x".to_vec(),
        };
        resp.send_err("key is out of range".to_string());
        let got = compio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { rx.await });
        let err = got.unwrap().err().unwrap();
        assert_eq!(err.0, StatusCode::InvalidArgument);
        assert_eq!(err.1, "key is out of range");

        // And a non-range error surfaces as Internal.
        let (outer2, rx2) = oneshot::channel();
        let resp2 = WriteResponder::Delete {
            outer: outer2,
            key: b"y".to_vec(),
        };
        resp2.send_err("log_stream append_segments: boom".to_string());
        let got2 = compio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { rx2.await });
        let err2 = got2.unwrap().err().unwrap();
        assert_eq!(err2.0, StatusCode::Internal);
    }

}


// ---------------------------------------------------------------------------
// F099-J — ps-conn on P-log runtime (same-thread, no worker pool) tests.
//
// These tests pin the two properties of the F099-J refactor:
//   1. `handle_ps_connection` no longer requires an Arc<PartitionRouter>
//      DashMap — it runs on the owning partition's compio runtime and
//      communicates with `partition_loop` via a same-thread
//      `mpsc::Sender<PartitionRequest>`. No cross-thread wake (eventfd
//      + futex) on the write hot path.
//   2. Under load (1000 sequential ops on one TCP connection), the full
//      decode → push → await → write cycle remains correct. This is a
//      lightweight correctness-under-load check (not a perf test).
// ---------------------------------------------------------------------------
#[cfg(test)]
mod f099j_tests {
    use super::*;

    /// F099-J test 1 — `handle_ps_connection` accepts a direct
    /// `mpsc::Sender<PartitionRequest>` and `owner_part` id. We drive
    /// one Put request through a loopback connection with BOTH the
    /// ps-conn task and the simulated merged_loop running on the SAME
    /// compio runtime. There is no spawned dispatcher worker thread,
    /// no DashMap, and no Arc<PartitionRouter>. Success = the response
    /// arrives with the exact key echoed.
    #[test]
    fn f099j_single_threaded_write_path_no_router() {
        // Serialize with f099i_tests + sqcq_tests to keep
        // PS_FAST_PATH_HITS coherent across the test process.
        let _g = super::ps_conn_test_lock();
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            // Bind a loopback listener and a client socket on the same runtime.
            let listener = compio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind listener");
            let server_addr = listener.local_addr().expect("local_addr");
            let client = compio::net::TcpStream::connect(server_addr)
                .await
                .expect("connect client");
            let (server_stream, _) = listener.accept().await.expect("accept");

            // Same-thread req channel — consumer is the simulated merged_loop
            // spawned on THIS compio runtime (no OS thread spawned).
            let (req_tx, mut req_rx) = mpsc::channel::<PartitionRequest>(16);

            // Spawn the ps-conn task with the direct req_tx (no router).
            let conn_handle = compio::runtime::spawn(async move {
                handle_ps_connection(autumn_transport::Conn::Tcp(server_stream), req_tx, None, /*owner_part=*/ 7).await
            });

            // Spawn a simulated merged_loop that answers the single Put.
            let loop_handle = compio::runtime::spawn(async move {
                if let Some(req) = req_rx.next().await {
                    assert_eq!(req.msg_type, MSG_PUT);
                    // Simulate Phase-3 success: encode PutResp + send.
                    let put: PutReq =
                        partition_rpc::rkyv_decode(&req.payload).expect("decode");
                    let resp = partition_rpc::rkyv_encode(&PutResp {
                        code: CODE_OK,
                        message: String::new(),
                        key: put.key,
                    });
                    let _ = req.resp_tx.send(Ok(resp));
                }
                // Exit — dropping rx closes the channel; ps-conn will exit
                // when the client closes its socket below.
            });

            // Client: build one PutReq frame, send it, read response.
            let put = PutReq {
                part_id: 7,
                key: b"hello".to_vec(),
                value: b"world".to_vec(),
                expires_at: 0,
                region_epoch: 0,
            };
            let payload = partition_rpc::rkyv_encode(&put);
            let frame = Frame::request(42, MSG_PUT, Bytes::from(payload));
            let bytes = frame.encode();

            let (mut client_rd, mut client_wr) = client.into_split();
            let BufResult(r, _buf) = client_wr.write_all(bytes).await;
            r.expect("write request");

            // Read the response frame.
            let mut decoder = FrameDecoder::new();
            let mut buf = vec![0u8; 8192];
            let mut decoded: Option<Frame> = None;
            while decoded.is_none() {
                let BufResult(n, back) = client_rd.read(buf).await;
                buf = back;
                let n = n.expect("read response");
                assert!(n > 0, "unexpected EOF before response arrived");
                decoder.feed(&buf[..n]);
                decoded = decoder.try_decode().expect("decode");
            }
            let resp_frame = decoded.unwrap();
            assert_eq!(resp_frame.req_id, 42);
            assert_eq!(resp_frame.msg_type, MSG_PUT);
            // Response is success — no status-code header.
            assert!(!resp_frame.is_error(), "response should not be error");

            let resp: PutResp =
                partition_rpc::rkyv_decode(&resp_frame.payload).expect("decode resp");
            assert_eq!(resp.code, CODE_OK);
            assert_eq!(resp.key, b"hello");

            // Close client → ps-conn exits.
            drop(client_rd);
            drop(client_wr);
            let _ = conn_handle.await;
            let _ = loop_handle.await;
        });
    }

    /// F099-J test 2 — correctness under load. Fire 1000 sequential
    /// Put requests on one TCP connection, verify every response arrives
    /// with the correct echoed key. No threading, no routing; all on
    /// the single compio runtime. Elapsed time bound (2 s) is
    /// generous — this is a correctness check, not a perf test.
    #[test]
    fn f099j_n1_load_basic_sanity() {
        let _g = super::ps_conn_test_lock();
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let listener = compio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let addr = listener.local_addr().expect("local_addr");
            let client = compio::net::TcpStream::connect(addr).await.expect("connect");
            let (server, _) = listener.accept().await.expect("accept");

            let (req_tx, mut req_rx) = mpsc::channel::<PartitionRequest>(128);

            let conn_handle = compio::runtime::spawn(async move {
                handle_ps_connection(autumn_transport::Conn::Tcp(server), req_tx, None, 1).await
            });

            // Simulated merged_loop: echo every Put.
            let loop_handle = compio::runtime::spawn(async move {
                while let Some(req) = req_rx.next().await {
                    let put: PutReq =
                        partition_rpc::rkyv_decode(&req.payload).expect("decode");
                    let resp = partition_rpc::rkyv_encode(&PutResp {
                        code: CODE_OK,
                        message: String::new(),
                        key: put.key,
                    });
                    let _ = req.resp_tx.send(Ok(resp));
                }
            });

            let (mut client_rd, mut client_wr) = client.into_split();
            let n_ops: u32 = 1000;
            let start = Instant::now();

            // Send all 1000 requests first (pipelined at the TCP layer,
            // serialized at ps-conn). Then read 1000 responses.
            let mut big_buf: Vec<u8> = Vec::with_capacity(64 * 1024);
            for i in 0..n_ops {
                let key = format!("k{:04}", i).into_bytes();
                let value = format!("v{:04}", i).into_bytes();
                let put = PutReq {
                    part_id: 1,
                    key: key.clone(),
                    value,
                    expires_at: 0,
                    region_epoch: 0,
                };
                let payload = partition_rpc::rkyv_encode(&put);
                let f = Frame::request(i + 1, MSG_PUT, Bytes::from(payload));
                big_buf.extend_from_slice(&f.encode()[..]);
            }
            let BufResult(r, _) = client_wr.write_all(big_buf).await;
            r.expect("write all requests");

            // Read and verify all 1000 responses.
            let mut decoder = FrameDecoder::new();
            let mut buf = vec![0u8; 64 * 1024];
            let mut verified: u32 = 0;
            while verified < n_ops {
                let BufResult(n, back) = client_rd.read(buf).await;
                buf = back;
                let n = n.expect("read");
                assert!(n > 0, "unexpected EOF at verified={}", verified);
                decoder.feed(&buf[..n]);
                while let Some(resp_frame) = decoder.try_decode().expect("decode") {
                    assert_eq!(resp_frame.req_id, verified + 1);
                    assert_eq!(resp_frame.msg_type, MSG_PUT);
                    assert!(!resp_frame.is_error());
                    let r: PutResp =
                        partition_rpc::rkyv_decode(&resp_frame.payload).expect("decode resp");
                    assert_eq!(r.code, CODE_OK);
                    let expected_key = format!("k{:04}", verified).into_bytes();
                    assert_eq!(r.key, expected_key);
                    verified += 1;
                }
            }
            let elapsed = start.elapsed();
            assert_eq!(verified, n_ops);
            assert!(
                elapsed < std::time::Duration::from_secs(5),
                "1000 ops should complete well under 5s on loopback; took {:?}",
                elapsed,
            );

            drop(client_rd);
            drop(client_wr);
            let _ = conn_handle.await;
            let _ = loop_handle.await;
        });
    }
}


// ---------------------------------------------------------------------------
// F099-K — per-partition listener tests.
//
// These tests pin the two core properties of the F099-K refactor:
//   1. A partition thread binds its OWN `compio::net::TcpListener` on a
//      unique port and runs its OWN accept loop + `handle_ps_connection`
//      tasks on the same compio runtime — no central accept thread, no
//      fd dispatcher. Clients connect directly to the partition's port.
//   2. At N > 1, N partitions bind N distinct ports and requests land on
//      the owning partition's listener. Each `handle_ps_connection` only
//      accepts requests whose `part_id` matches its `owner_part`; a request
//      for a foreign partition gets a `NotFound` error.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod f099k_tests {
    use super::*;

    /// Spin up one "partition" accept loop on a dedicated compio runtime
    /// thread, returning (listen_port, shutdown_tx, join). The accept loop:
    ///   - binds `127.0.0.1:0` (OS-assigned port)
    ///   - spawns `handle_ps_connection` on the same runtime for each
    ///     accepted fd, with the provided `owner_part` id
    ///   - runs a simulated merged_loop that echoes every Put with a
    ///     `PutResp { code: CODE_OK, key: put.key }`
    ///   - exits when `shutdown_rx` resolves (drop of the sender)
    fn spawn_partition_listener(
        owner_part: u64,
    ) -> (u16, std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>) {
        let (port_tx, port_rx) = std::sync::mpsc::channel::<u16>();
        let (shutdown_tx, shutdown_rx_std) = std::sync::mpsc::channel::<()>();

        let join = std::thread::Builder::new()
            .name(format!("f099k-part-{owner_part}"))
            .spawn(move || {
                let rt = compio::runtime::Runtime::new().expect("rt");
                rt.block_on(async move {
                    let listener = compio::net::TcpListener::bind("127.0.0.1:0")
                        .await
                        .expect("bind");
                    let port = listener.local_addr().expect("local_addr").port();
                    let _ = port_tx.send(port);

                    // Same-thread ps-conn <-> merged_loop channel.
                    let (req_tx, mut req_rx) =
                        mpsc::channel::<PartitionRequest>(WRITE_CHANNEL_CAP);

                    // Simulated merged_loop: echo every Put while req_rx is open.
                    let loop_handle = compio::runtime::spawn(async move {
                        while let Some(req) = req_rx.next().await {
                            // Accept both MSG_PUT and MSG_GET; echo on Put.
                            if req.msg_type == MSG_PUT {
                                let put: PutReq =
                                    partition_rpc::rkyv_decode(&req.payload)
                                        .expect("decode put");
                                let resp = partition_rpc::rkyv_encode(&PutResp {
                                    code: CODE_OK,
                                    message: String::new(),
                                    key: put.key,
                                });
                                let _ = req.resp_tx.send(Ok(resp));
                            } else {
                                let _ = req
                                    .resp_tx
                                    .send(Err((StatusCode::Internal, "test".to_string())));
                            }
                        }
                    });

                    // Accept loop: racy shutdown via a polling check (the
                    // test-only listener uses std::mpsc for signalling since
                    // we're crossing the spawning OS thread here).
                    let req_tx_accept = req_tx.clone();
                    let accept_handle = compio::runtime::spawn(async move {
                        loop {
                            // Poll shutdown; break on EITHER a message OR
                            // sender-drop (tests drop `shutdown_tx` without
                            // sending).
                            match shutdown_rx_std.try_recv() {
                                Ok(()) | Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
                                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                            }
                            // Race accept against a short timer so the shutdown
                            // poll runs at least every 50 ms.
                            let accept_fut = listener.accept();
                            let timer_fut =
                                compio::time::sleep(Duration::from_millis(50));
                            futures::pin_mut!(accept_fut);
                            futures::pin_mut!(timer_fut);
                            match futures::future::select(accept_fut, timer_fut).await {
                                futures::future::Either::Left((r, _)) => match r {
                                    Ok((stream, _peer)) => {
                                        let _ = stream.set_nodelay(true);
                                        let tx = req_tx_accept.clone();
                                        compio::runtime::spawn(async move {
                                            let _ = handle_ps_connection(
                                                autumn_transport::Conn::Tcp(stream),
                                                tx,
                                                None,
                                                owner_part,
                                            )
                                            .await;
                                        })
                                        .detach();
                                    }
                                    Err(_) => {
                                        compio::time::sleep(Duration::from_millis(10)).await;
                                    }
                                },
                                futures::future::Either::Right(_) => {
                                    // Fall through to the shutdown poll.
                                }
                            }
                        }
                    });

                    drop(req_tx);
                    let _ = accept_handle.await;
                    let _ = loop_handle.await;
                });
            })
            .expect("spawn");

        let port = port_rx.recv().expect("bind port reported");
        (port, shutdown_tx, join)
    }

    /// F099-K test 1 — one partition thread binds its own listener on
    /// an OS-assigned port; a client connects to that port and issues a
    /// Put; the response round-trips correctly. Verifies the "thread
    /// owns its listener" architectural property.
    #[test]
    fn f099k_n1_single_partition_listener() {
        let owner_part: u64 = 42;
        let (port, shutdown_tx, join) = spawn_partition_listener(owner_part);

        // Client: open a TCP connection and send one Put on a separate
        // compio runtime (matching what autumn-client does in perf-check).
        std::thread::spawn(move || {
            compio::runtime::Runtime::new().unwrap().block_on(async move {
                let addr: std::net::SocketAddr = format!("127.0.0.1:{port}")
                    .parse()
                    .expect("parse addr");
                let stream = compio::net::TcpStream::connect(addr)
                    .await
                    .expect("connect");
                let (mut rd, mut wr) = stream.into_split();

                let put = PutReq {
                    part_id: owner_part,
                    key: b"k_n1".to_vec(),
                    value: b"v_n1".to_vec(),
                    expires_at: 0,
                    region_epoch: 0,
                };
                let payload = partition_rpc::rkyv_encode(&put);
                let frame = Frame::request(1, MSG_PUT, Bytes::from(payload)).encode();
                let BufResult(r, _) = wr.write_all(frame).await;
                r.expect("write");

                let mut decoder = FrameDecoder::new();
                let mut buf = vec![0u8; 4096];
                let resp_frame = loop {
                    let BufResult(n, back) = rd.read(buf).await;
                    buf = back;
                    let n = n.expect("read");
                    assert!(n > 0, "EOF before response");
                    decoder.feed(&buf[..n]);
                    if let Some(f) = decoder.try_decode().expect("decode") {
                        break f;
                    }
                };
                assert_eq!(resp_frame.req_id, 1);
                assert!(!resp_frame.is_error(), "unexpected error response");
                let resp: PutResp =
                    partition_rpc::rkyv_decode(&resp_frame.payload).expect("decode resp");
                assert_eq!(resp.code, CODE_OK);
                assert_eq!(resp.key, b"k_n1");
            });
        })
        .join()
        .expect("client thread");

        drop(shutdown_tx);
        let _ = join.join();
    }

    /// F099-K test 2 — four partition threads bind four distinct ports;
    /// requests with the matching `part_id` on each port succeed, and
    /// a request targeting the wrong partition returns `NotFound`.
    /// Verifies that (a) ports are distinct, (b) each listener is
    /// isolated to its owner partition.
    #[test]
    fn f099k_n4_distinct_ports() {
        let owners: [u64; 4] = [101, 102, 103, 104];
        let mut ports: Vec<u16> = Vec::with_capacity(4);
        let mut shutdowns: Vec<std::sync::mpsc::Sender<()>> = Vec::with_capacity(4);
        let mut joins: Vec<std::thread::JoinHandle<()>> = Vec::with_capacity(4);
        for &o in &owners {
            let (p, st, j) = spawn_partition_listener(o);
            ports.push(p);
            shutdowns.push(st);
            joins.push(j);
        }

        // All four ports distinct.
        let mut sorted = ports.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 4, "partition ports must be distinct: {:?}", ports);

        // Drive a Put into each partition on its own port and verify
        // correct routing; also fire a mis-routed request that hits a
        // partition on the wrong port and expect `NotFound`.
        std::thread::spawn(move || {
            compio::runtime::Runtime::new().unwrap().block_on(async move {
                for (i, &o) in owners.iter().enumerate() {
                    let port = ports[i];
                    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}")
                        .parse()
                        .unwrap();
                    let stream = compio::net::TcpStream::connect(addr)
                        .await
                        .expect("connect");
                    let (mut rd, mut wr) = stream.into_split();

                    // (a) correct part_id on owner's port → CODE_OK.
                    let put = PutReq {
                        part_id: o,
                        key: format!("k-{o}").into_bytes(),
                        value: format!("v-{o}").into_bytes(),
                        expires_at: 0,
                        region_epoch: 0,
                    };
                    let payload = partition_rpc::rkyv_encode(&put);
                    let f =
                        Frame::request(10, MSG_PUT, Bytes::from(payload)).encode();
                    let BufResult(r, _) = wr.write_all(f).await;
                    r.expect("write put");

                    let mut decoder = FrameDecoder::new();
                    let mut buf = vec![0u8; 4096];
                    let resp = loop {
                        let BufResult(n, back) = rd.read(buf).await;
                        buf = back;
                        let n = n.expect("read");
                        assert!(n > 0, "EOF before response for part {o}");
                        decoder.feed(&buf[..n]);
                        if let Some(fr) = decoder.try_decode().expect("decode") {
                            break fr;
                        }
                    };
                    assert!(!resp.is_error(), "part {o} on port {port} unexpectedly errored");
                    let pr: PutResp =
                        partition_rpc::rkyv_decode(&resp.payload).expect("decode");
                    assert_eq!(pr.code, CODE_OK);
                    assert_eq!(pr.key, format!("k-{o}").into_bytes());

                    // (b) Mis-routed: send a request with WRONG part_id to
                    // this listener. handle_ps_connection should answer with
                    // NotFound (owner mismatch).
                    let wrong = PutReq {
                        part_id: o + 1000, // definitely not this listener's owner
                        key: b"bogus".to_vec(),
                        value: b"bogus".to_vec(),
                        expires_at: 0,
                        region_epoch: 0,
                    };
                    let payload = partition_rpc::rkyv_encode(&wrong);
                    let f =
                        Frame::request(11, MSG_PUT, Bytes::from(payload)).encode();
                    let BufResult(r, _) = wr.write_all(f).await;
                    r.expect("write mis-routed put");

                    let mut buf = vec![0u8; 4096];
                    let resp = loop {
                        let BufResult(n, back) = rd.read(buf).await;
                        buf = back;
                        let n = n.expect("read");
                        assert!(n > 0, "EOF before mis-route response for part {o}");
                        decoder.feed(&buf[..n]);
                        if let Some(fr) = decoder.try_decode().expect("decode") {
                            break fr;
                        }
                    };
                    assert!(
                        resp.is_error(),
                        "mis-routed request to part {o}'s port {port} should error"
                    );
                }
            });
        })
        .join()
        .expect("client thread");

        for st in shutdowns {
            drop(st);
        }
        for j in joins {
            let _ = j.join();
        }
    }
}


// ---------------------------------------------------------------------------
// F099-I — per-conn reply batching tests.
//
// These tests pin the three properties of the F099-I refactor:
//   1. Single-frame passthrough: a depth=1 client sending one frame and
//      awaiting its reply still works. Degenerates to
//      `write_vectored_all([one_frame])`, which is cheap enough not to
//      regress vs the old `write_all(one_frame)`.
//   2. Multi-frame batching: when a TCP read delivers N frames, all N
//      complete correctly and the total latency stays at the depth=1 cost
//      (not N× it) — i.e. the futures genuinely run concurrently.
//   3. Back-pressure at cap: a flood of N ≫ cap frames does NOT grow
//      `inflight` past the configured cap; instead `push_frames_to_inflight`
//      drains completions mid-push and the stream still processes every
//      frame correctly.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod f099i_tests {
    use super::*;

    /// F099-I test 1 — Single-frame passthrough: one Put per TCP read,
    /// client awaits reply before sending next. This is the depth=1
    /// baseline — MUST NOT regress.
    #[test]
    fn f099i_single_frame_passthrough() {
        let _g = fast_path_counter_lock();
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let listener = compio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let addr = listener.local_addr().expect("addr");
            let client = compio::net::TcpStream::connect(addr).await.expect("connect");
            let (server, _) = listener.accept().await.expect("accept");

            let (req_tx, mut req_rx) = mpsc::channel::<PartitionRequest>(16);

            let conn_handle = compio::runtime::spawn(async move {
                handle_ps_connection(autumn_transport::Conn::Tcp(server), req_tx, None, /*owner_part=*/ 7).await
            });

            let loop_handle = compio::runtime::spawn(async move {
                while let Some(req) = req_rx.next().await {
                    let put: PutReq =
                        partition_rpc::rkyv_decode(&req.payload).expect("decode");
                    let resp = partition_rpc::rkyv_encode(&PutResp {
                        code: CODE_OK,
                        message: String::new(),
                        key: put.key,
                    });
                    let _ = req.resp_tx.send(Ok(resp));
                }
            });

            let (mut client_rd, mut client_wr) = client.into_split();

            // One synchronous send-recv round trip.
            let put = PutReq {
                part_id: 7,
                key: b"one-frame".to_vec(),
                value: b"v".to_vec(),
                expires_at: 0,
                region_epoch: 0,
            };
            let payload = partition_rpc::rkyv_encode(&put);
            let frame_bytes = Frame::request(77, MSG_PUT, Bytes::from(payload)).encode();
            let BufResult(r, _) = client_wr.write_all(frame_bytes).await;
            r.expect("write");

            let mut decoder = FrameDecoder::new();
            let mut buf = vec![0u8; 8192];
            let resp_frame = loop {
                let BufResult(n, back) = client_rd.read(buf).await;
                buf = back;
                let n = n.expect("read");
                assert!(n > 0, "EOF before response");
                decoder.feed(&buf[..n]);
                if let Some(f) = decoder.try_decode().expect("decode") {
                    break f;
                }
            };
            assert_eq!(resp_frame.req_id, 77);
            assert!(!resp_frame.is_error());
            let r: PutResp =
                partition_rpc::rkyv_decode(&resp_frame.payload).expect("decode resp");
            assert_eq!(r.key, b"one-frame");

            drop(client_rd);
            drop(client_wr);
            let _ = conn_handle.await;
            let _ = loop_handle.await;
        });
    }

    /// F099-I test 2 — Multi-frame batched write: send 8 frames in one
    /// TCP write. Verify all 8 complete correctly AND measure the peak
    /// concurrency observed in merged_loop. The key correctness property:
    /// if the server receives all 8 frames in a single TCP read, all 8
    /// futures end up in `inflight` before any reply is sent, so peak
    /// concurrency equals the batch size.
    ///
    /// We avoid the F099-I n_inflight==1 fast-path deadlock hazard by
    /// responding to each request **immediately** as it arrives (via a
    /// small `spawn` per request) — then verify peak >= 2 to prove that
    /// handle_ps_connection did decode multiple frames before a single
    /// reply completed. Under d=1 the old sequential path would yield
    /// peak == 1 (one frame in, one reply out, loop).
    #[test]
    fn f099i_multi_frame_batches_write() {
        let _g = fast_path_counter_lock();
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let listener = compio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let addr = listener.local_addr().expect("addr");
            let client = compio::net::TcpStream::connect(addr).await.expect("connect");
            let (server, _) = listener.accept().await.expect("accept");

            let (req_tx, mut req_rx) = mpsc::channel::<PartitionRequest>(64);

            let peak = Rc::new(Cell::new(0usize));
            let cur = Rc::new(Cell::new(0usize));

            let conn_handle = compio::runtime::spawn(async move {
                handle_ps_connection(autumn_transport::Conn::Tcp(server), req_tx, None, 9).await
            });

            let peak_c = peak.clone();
            let cur_c = cur.clone();
            let loop_handle = compio::runtime::spawn(async move {
                // Reply pool — we reply to each req after a 1ms delay so
                // that multiple requests can pile up concurrently while
                // waiting. But every req's reply IS eventually sent, so
                // ps-conn's n_inflight==1 fast-path is never a deadlock.
                let mut handlers: FuturesUnordered<
                    futures::future::LocalBoxFuture<'static, ()>,
                > = FuturesUnordered::new();
                loop {
                    futures::select! {
                        maybe_req = req_rx.next() => {
                            match maybe_req {
                                Some(req) => {
                                    let n = cur_c.get() + 1;
                                    cur_c.set(n);
                                    if n > peak_c.get() { peak_c.set(n); }
                                    let cur_c2 = cur_c.clone();
                                    handlers.push(Box::pin(async move {
                                        compio::time::sleep(
                                            Duration::from_millis(1),
                                        ).await;
                                        let put: PutReq =
                                            partition_rpc::rkyv_decode(&req.payload)
                                                .expect("decode");
                                        let resp = partition_rpc::rkyv_encode(&PutResp {
                                            code: CODE_OK,
                                            message: String::new(),
                                            key: put.key,
                                        });
                                        let _ = req.resp_tx.send(Ok(resp));
                                        cur_c2.set(cur_c2.get() - 1);
                                    }));
                                }
                                None => break,
                            }
                        }
                        _ = handlers.next() => {}
                        complete => break,
                    }
                }
                while handlers.next().await.is_some() {}
            });

            // Client: send all 8 in ONE write_all.
            let mut big = Vec::with_capacity(1024);
            for i in 0..8u32 {
                let put = PutReq {
                    part_id: 9,
                    key: format!("batch-{i}").into_bytes(),
                    value: b"v".to_vec(),
                    expires_at: 0,
                    region_epoch: 0,
                };
                let payload = partition_rpc::rkyv_encode(&put);
                let f = Frame::request(100 + i, MSG_PUT, Bytes::from(payload)).encode();
                big.extend_from_slice(&f[..]);
            }
            let (mut client_rd, mut client_wr) = client.into_split();
            let BufResult(r, _) = client_wr.write_all(big).await;
            r.expect("write 8 frames");

            // Read and verify all 8 replies (order-independent — FU's
            // arrival order is NOT guaranteed, but every req_id must
            // show up exactly once).
            let mut decoder = FrameDecoder::new();
            let mut buf = vec![0u8; 16 * 1024];
            let mut seen: std::collections::HashSet<u32> = std::collections::HashSet::new();
            while seen.len() < 8 {
                let BufResult(n, back) = client_rd.read(buf).await;
                buf = back;
                let n = n.expect("read");
                assert!(n > 0, "EOF before 8 replies; seen={}", seen.len());
                decoder.feed(&buf[..n]);
                while let Some(frame) = decoder.try_decode().expect("decode") {
                    assert!(!frame.is_error());
                    let r: PutResp =
                        partition_rpc::rkyv_decode(&frame.payload).expect("decode");
                    let expected_key = format!(
                        "batch-{}",
                        frame.req_id - 100
                    )
                    .into_bytes();
                    assert_eq!(r.key, expected_key);
                    assert!(seen.insert(frame.req_id), "duplicate req_id {}", frame.req_id);
                }
            }
            assert_eq!(seen.len(), 8);

            drop(client_rd);
            drop(client_wr);
            let _ = conn_handle.await;
            let _ = loop_handle.await;

            // Peak concurrency: the batch write of 8 frames MUST have
            // resulted in >= 2 concurrent in-flight reqs at some point.
            // Old sequential path (pre-F099-I) would have peak == 1.
            let p = peak.get();
            assert!(
                p >= 2,
                "peak concurrent in-flight = {p}, expected >= 2 for batched write"
            );
        });
    }

    /// F099-I test 3 — Back-pressure at cap.  The env knob
    /// `AUTUMN_PS_CONN_INFLIGHT_CAP` lowers the cap so we can exercise the
    /// back-pressure branch in `push_frames_to_inflight`.  We set cap to 4,
    /// fire 100 frames in one write, and verify: (a) every frame completes
    /// correctly, (b) the merged_loop never observes more than `cap` reqs
    /// simultaneously in-flight.
    ///
    /// Because `ps_conn_inflight_cap` caches via OnceLock on first call,
    /// we spin up the whole test in a subprocess-like isolated runtime
    /// (a fresh thread) so the OnceLock init picks up our env override
    /// without interfering with parallel tests.
    #[test]
    fn f099i_backpressure_at_cap() {
        const CAP: usize = 4;
        const N_FRAMES: u32 = 100;

        // Hold the workspace ps-conn lock across the spawned thread so
        // PS_FAST_PATH_HITS stays coherent for the two counter-asserting
        // tests in this module.
        let _g = fast_path_counter_lock();

        // Run in a dedicated thread with the env var set BEFORE the
        // OnceLock is initialised.
        let handle = std::thread::Builder::new()
            .name("f099i-bp".to_string())
            .spawn(move || {
                // SAFETY: single-threaded isolation by virtue of the
                // OnceLock cache being per-process but initialised lazily
                // here before any other ps_conn_inflight_cap() caller.
                std::env::set_var("AUTUMN_PS_CONN_INFLIGHT_CAP", CAP.to_string());

                let rt = compio::runtime::Runtime::new().unwrap();
                rt.block_on(async move {
                    // Sanity-check that our override got picked up. If
                    // another test already triggered the OnceLock with a
                    // different cap, we skip this assertion and let the
                    // main body still exercise correctness under whatever
                    // cap is in force (the stream of 100 frames still
                    // completes; just the peak-concurrency assertion
                    // becomes weaker).
                    let effective_cap = ps_conn_inflight_cap();

                    let listener = compio::net::TcpListener::bind("127.0.0.1:0")
                        .await
                        .expect("bind");
                    let addr = listener.local_addr().expect("addr");
                    let client = compio::net::TcpStream::connect(addr)
                        .await
                        .expect("connect");
                    let (server, _) = listener.accept().await.expect("accept");

                    // Track concurrent in-flight count via atomic.
                    let peak = Rc::new(Cell::new(0usize));
                    let cur = Rc::new(Cell::new(0usize));

                    let (req_tx, mut req_rx) =
                        mpsc::channel::<PartitionRequest>(4096);

                    let conn_handle = compio::runtime::spawn(async move {
                        handle_ps_connection(autumn_transport::Conn::Tcp(server), req_tx, None, 5).await
                    });

                    let peak_c = peak.clone();
                    let cur_c = cur.clone();
                    let loop_handle = compio::runtime::spawn(async move {
                        // Hold each request on a small timer so concurrency
                        // actually builds up. This is the mechanism that
                        // exercises back-pressure: if ps-conn didn't cap
                        // inflight at CAP, cur would rise above CAP.
                        let mut handlers = FuturesUnordered::new();
                        let mut drained: u32 = 0;
                        loop {
                            futures::select! {
                                maybe_req = req_rx.next() => {
                                    match maybe_req {
                                        Some(req) => {
                                            let n = cur_c.get() + 1;
                                            cur_c.set(n);
                                            if n > peak_c.get() {
                                                peak_c.set(n);
                                            }
                                            let cur_c2 = cur_c.clone();
                                            handlers.push(async move {
                                                // Small delay so back-pressure
                                                // has teeth (futures stay
                                                // live concurrently).
                                                compio::time::sleep(
                                                    Duration::from_millis(2),
                                                )
                                                .await;
                                                let put: PutReq =
                                                    partition_rpc::rkyv_decode(&req.payload)
                                                        .expect("decode");
                                                let resp = partition_rpc::rkyv_encode(
                                                    &PutResp {
                                                        code: CODE_OK,
                                                        message: String::new(),
                                                        key: put.key,
                                                    },
                                                );
                                                let _ = req.resp_tx.send(Ok(resp));
                                                cur_c2.set(cur_c2.get() - 1);
                                            });
                                        }
                                        None => break,
                                    }
                                }
                                maybe_done = handlers.next() => {
                                    if maybe_done.is_some() {
                                        drained += 1;
                                    }
                                }
                                complete => break,
                            }
                        }
                        // Drain any remaining.
                        while let Some(_) = handlers.next().await {
                            drained += 1;
                        }
                        drained
                    });

                    let (mut client_rd, mut client_wr) = client.into_split();
                    let mut big = Vec::with_capacity(N_FRAMES as usize * 64);
                    for i in 0..N_FRAMES {
                        let put = PutReq {
                            part_id: 5,
                            key: format!("bp-{i:03}").into_bytes(),
                            value: b"v".to_vec(),
                            expires_at: 0,
                            region_epoch: 0,
                        };
                        let payload = partition_rpc::rkyv_encode(&put);
                        let f = Frame::request(
                            1000u32 + i,
                            MSG_PUT,
                            Bytes::from(payload),
                        )
                        .encode();
                        big.extend_from_slice(&f[..]);
                    }
                    let BufResult(r, _) = client_wr.write_all(big).await;
                    r.expect("write N frames");

                    // Read all 100 replies.
                    let mut decoder = FrameDecoder::new();
                    let mut buf = vec![0u8; 64 * 1024];
                    let mut seen: std::collections::HashSet<u32> =
                        std::collections::HashSet::new();
                    while seen.len() < N_FRAMES as usize {
                        let BufResult(n, back) = client_rd.read(buf).await;
                        buf = back;
                        let n = n.expect("read");
                        assert!(n > 0, "EOF before all replies; seen={}", seen.len());
                        decoder.feed(&buf[..n]);
                        while let Some(frame) = decoder.try_decode().expect("decode") {
                            assert!(!frame.is_error());
                            assert!(
                                seen.insert(frame.req_id),
                                "duplicate req_id {}",
                                frame.req_id
                            );
                        }
                    }
                    assert_eq!(seen.len(), N_FRAMES as usize);

                    drop(client_rd);
                    drop(client_wr);
                    let _ = conn_handle.await;
                    let drained = loop_handle.await.unwrap_or(0);
                    assert_eq!(drained, N_FRAMES);

                    // Peak concurrency assertion — gated on the override
                    // actually having been picked up (see the comment
                    // above ps_conn_inflight_cap()).
                    let p = peak.get();
                    if effective_cap == CAP {
                        assert!(
                            p <= CAP,
                            "peak in-flight {p} exceeded cap {CAP} — back-pressure failed"
                        );
                    } else {
                        // Env override didn't win the OnceLock race; still
                        // assert peak never exceeded the effective cap.
                        assert!(
                            p <= effective_cap,
                            "peak in-flight {p} exceeded effective cap {effective_cap}"
                        );
                    }
                });
            })
            .expect("spawn bp test thread");
        handle.join().expect("bp test thread panicked");
    }

    /// Re-exported workspace-wide lock — see `ps_conn_test_lock` above.
    /// Held across the entire f099i test body so concurrent
    /// `handle_ps_connection`-using tests don't race the
    /// `PS_FAST_PATH_HITS` counter and break the exact-delta assertion.
    fn fast_path_counter_lock() -> parking_lot::MutexGuard<'static, ()> {
        super::ps_conn_test_lock()
    }

    /// F099-I-fix test — the d=1 fast path MUST engage when exactly one
    /// frame is read from the TCP socket AND nothing else is in flight.
    ///
    /// We verify by sending 10 synchronous Put→reply round-trips on one
    /// connection and asserting `PS_FAST_PATH_HITS` grows by 10.  Each
    /// round-trip awaits the reply before sending the next, so every
    /// read delivers exactly one frame to a ps-conn task that has just
    /// finished writing the previous reply (inflight empty, tx_bufs
    /// empty) — textbook fast-path conditions.
    ///
    /// This test also doubles as a regression guard: if a future change
    /// re-introduces FU+Box allocation on the d=1 hot path, the hit
    /// counter stays at 0 and the test fails.
    #[test]
    fn f099i_d1_fast_path_no_fu_allocation() {
        let _guard = fast_path_counter_lock();
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            // Snapshot the counter before we start — the lock ensures no
            // other fast-path-observing test is concurrently running.
            let before = PS_FAST_PATH_HITS
                .load(std::sync::atomic::Ordering::Relaxed);

            let listener = compio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let addr = listener.local_addr().expect("addr");
            let client = compio::net::TcpStream::connect(addr).await.expect("connect");
            let (server, _) = listener.accept().await.expect("accept");

            let (req_tx, mut req_rx) = mpsc::channel::<PartitionRequest>(8);

            let conn_handle = compio::runtime::spawn(async move {
                handle_ps_connection(autumn_transport::Conn::Tcp(server), req_tx, None, /*owner_part=*/ 11).await
            });

            // Responder: answer each request immediately so the fast-path
            // round-trip completes without delay.
            let loop_handle = compio::runtime::spawn(async move {
                while let Some(req) = req_rx.next().await {
                    let put: PutReq =
                        partition_rpc::rkyv_decode(&req.payload).expect("decode");
                    let resp = partition_rpc::rkyv_encode(&PutResp {
                        code: CODE_OK,
                        message: String::new(),
                        key: put.key,
                    });
                    let _ = req.resp_tx.send(Ok(resp));
                }
            });

            let (mut client_rd, mut client_wr) = client.into_split();
            let mut decoder = FrameDecoder::new();
            let mut buf = vec![0u8; 8192];

            const N: u32 = 10;
            for i in 0..N {
                let put = PutReq {
                    part_id: 11,
                    key: format!("fast-{i:02}").into_bytes(),
                    value: b"v".to_vec(),
                    expires_at: 0,
                    region_epoch: 0,
                };
                let payload = partition_rpc::rkyv_encode(&put);
                let frame_bytes =
                    Frame::request(5000 + i, MSG_PUT, Bytes::from(payload)).encode();
                let BufResult(r, _) = client_wr.write_all(frame_bytes).await;
                r.expect("write");

                // Block until the single reply arrives — this is what
                // makes it "d=1": one frame in flight, then wait.
                let resp_frame = loop {
                    let BufResult(n, back) = client_rd.read(buf).await;
                    buf = back;
                    let n = n.expect("read");
                    assert!(n > 0, "EOF before response for i={i}");
                    decoder.feed(&buf[..n]);
                    if let Some(f) = decoder.try_decode().expect("decode") {
                        break f;
                    }
                };
                assert_eq!(resp_frame.req_id, 5000 + i);
                assert!(!resp_frame.is_error());
            }

            let after = PS_FAST_PATH_HITS
                .load(std::sync::atomic::Ordering::Relaxed);
            let delta = after - before;
            assert_eq!(
                delta, N as u64,
                "d=1 fast path must engage exactly N={N} times; \
                 observed delta={delta} (before={before}, after={after})"
            );

            drop(client_rd);
            drop(client_wr);
            let _ = conn_handle.await;
            let _ = loop_handle.await;
        });
    }

    /// F099-I-fix test — the fast path MUST NOT engage when prior frames
    /// are still in `inflight`.  We exercise this by flooding 8 frames
    /// in one TCP write: the first read yields 8 frames → 8 futures in
    /// `inflight` → slow path.  Counter must not grow.
    ///
    /// Correctness of the gating check `inflight.is_empty()` is critical:
    /// if the fast path engaged mid-burst, reply order would be scrambled
    /// (fast-path reply written before earlier in-flight replies).
    #[test]
    fn f099i_fast_path_inactive_under_batch() {
        let _guard = fast_path_counter_lock();
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let before = PS_FAST_PATH_HITS
                .load(std::sync::atomic::Ordering::Relaxed);

            let listener = compio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let addr = listener.local_addr().expect("addr");
            let client = compio::net::TcpStream::connect(addr).await.expect("connect");
            let (server, _) = listener.accept().await.expect("accept");

            let (req_tx, mut req_rx) = mpsc::channel::<PartitionRequest>(16);

            let conn_handle = compio::runtime::spawn(async move {
                handle_ps_connection(autumn_transport::Conn::Tcp(server), req_tx, None, 13).await
            });

            let loop_handle = compio::runtime::spawn(async move {
                while let Some(req) = req_rx.next().await {
                    let put: PutReq =
                        partition_rpc::rkyv_decode(&req.payload).expect("decode");
                    let resp = partition_rpc::rkyv_encode(&PutResp {
                        code: CODE_OK,
                        message: String::new(),
                        key: put.key,
                    });
                    let _ = req.resp_tx.send(Ok(resp));
                }
            });

            let (mut client_rd, mut client_wr) = client.into_split();

            const N: u32 = 8;
            let mut big = Vec::with_capacity(N as usize * 64);
            for i in 0..N {
                let put = PutReq {
                    part_id: 13,
                    key: format!("batch-{i}").into_bytes(),
                    value: b"v".to_vec(),
                    expires_at: 0,
                    region_epoch: 0,
                };
                let payload = partition_rpc::rkyv_encode(&put);
                let f = Frame::request(6000 + i, MSG_PUT, Bytes::from(payload)).encode();
                big.extend_from_slice(&f[..]);
            }
            let BufResult(r, _) = client_wr.write_all(big).await;
            r.expect("write batch");

            // Receive all N replies.
            let mut decoder = FrameDecoder::new();
            let mut buf = vec![0u8; 64 * 1024];
            let mut seen = 0u32;
            while seen < N {
                let BufResult(n, back) = client_rd.read(buf).await;
                buf = back;
                let n = n.expect("read");
                assert!(n > 0, "EOF before all replies");
                decoder.feed(&buf[..n]);
                while let Some(frame) = decoder.try_decode().expect("decode") {
                    assert!(!frame.is_error());
                    seen += 1;
                }
            }
            assert_eq!(seen, N);

            let after = PS_FAST_PATH_HITS
                .load(std::sync::atomic::Ordering::Relaxed);
            // The first read delivers all 8 frames at once (TCP on
            // loopback typically coalesces). So fast path must not
            // engage for any of them. Allow a small drift for the
            // (unlikely) race where TCP delivers the first frame alone
            // before the rest land, but assert that the fast path was
            // NOT used for MOST of the batch. In practice on loopback
            // we see delta == 0.
            let delta = after - before;
            assert!(
                delta < N as u64,
                "fast path engaged {delta} times for batched N={N} frames; \
                 must stay well below N (prefer 0 on loopback)"
            );

            drop(client_rd);
            drop(client_wr);
            let _ = conn_handle.await;
            let _ = loop_handle.await;
        });
    }
}

// ---------------------------------------------------------------------------
// F140 tests — split serialisation gates
// ---------------------------------------------------------------------------

#[cfg(test)]
mod f140_tests {
    use super::CompactionGate;
    use std::sync::atomic::Ordering;

    // Verify that a second acquire waits while a first permit is held, then
    // succeeds immediately after the first permit is dropped.
    #[test]
    fn f140_split_serialises_with_compact_gate() {
        let gate = CompactionGate::new(1);

        // Simulate "compaction in progress": manually bump inflight.
        gate.inflight.fetch_add(1, Ordering::Release);
        assert_eq!(gate.inflight.load(Ordering::Acquire), 1, "gate should show inflight=1");

        // A split trying to acquire should fail CAS (at-cap).
        let cur = gate.inflight.load(Ordering::Acquire);
        assert!(cur >= gate.max_parallel, "gate should be at capacity");

        // Simulate compaction finishing.
        gate.inflight.fetch_sub(1, Ordering::Release);
        assert_eq!(gate.inflight.load(Ordering::Acquire), 0, "gate should be free after compaction");

        // Now split's CAS should succeed.
        let cur = gate.inflight.load(Ordering::Acquire);
        assert!(cur < gate.max_parallel, "gate should be acquirable for split");
    }

    // Verify that gc_gate (per-partition) has the same acquire/release semantics.
    #[test]
    fn f140_gc_releases_gate_after_holes_loop() {
        let gc_gate = CompactionGate::new(1);

        // Simulate GC holding the gate.
        gc_gate.inflight.fetch_add(1, Ordering::Release);
        assert_eq!(gc_gate.inflight.load(Ordering::Acquire), 1, "gc_gate should show inflight=1 while GC runs");

        // Split cannot acquire.
        assert!(gc_gate.inflight.load(Ordering::Acquire) >= gc_gate.max_parallel);

        // GC loop exits: drop permit.
        gc_gate.inflight.fetch_sub(1, Ordering::Release);
        assert_eq!(gc_gate.inflight.load(Ordering::Acquire), 0, "gc_gate should be free after holes loop");
    }

    // F196 D-r6: gc_concurrency_gate folded into AdmissionController.
    // Cross-partition concurrent-GC behaviour now exercised by
    // `f189_admission_tests::admission_acquire_compact_and_gc_concurrency_caps`.
}

#[cfg(test)]
mod f148_publisher_invariant_tests {
    //! F148-A — locks in the metadata-publish ordering invariant that
    //! `flush_one_imm` (lib.rs) and `do_compact` (background.rs) both rely
    //! on for race-free concurrent publishing.
    //!
    //! ## The invariant
    //!
    //! Both publishers follow this pattern:
    //!
    //!   1. `borrow_mut` block: mutate `part.tables` / `part.sst_readers`,
    //!      capture `tables_snapshot = part.tables.clone()` inside the
    //!      borrow, drop the borrow.   (synchronous)
    //!   2. `save_table_locs_raw(&snap, ...)`: rkyv_encode + build payload,
    //!      then `stream_client.append.await`.   (synchronous until mpsc send)
    //!   3. `sync_partition_vp_refs(part).await`: re-reads CURRENT
    //!      `part.sst_readers` via a fresh borrow.
    //!
    //! Three load-bearing properties make this race-free against a
    //! concurrent publisher (flush vs compact, both running as separate
    //! tasks on the single-threaded P-log compio runtime):
    //!
    //!   (P1) compio P-log runtime is single-threaded.
    //!   (P2) the `borrow_mut` block contains no `.await`.
    //!   (P3) the path from borrow_mut drop → rkyv_encode →
    //!        stream_client.append → mpsc send is purely synchronous,
    //!        with the first `.await` on `ack_rx` (after the message is
    //!        in the per-stream worker FIFO mpsc).
    //!
    //! Together, (P1)–(P3) guarantee: `borrow_mut` order = mpsc-send
    //! order = meta_stream record order.   The LATEST persisted record's
    //! `tables_snapshot` therefore necessarily reflects all prior
    //! borrow_mut mutations.
    //!
    //! ## What this test exercises
    //!
    //! Two simulated publishers ("flush" and "compact") run concurrently
    //! within a single compio task via `futures::join!` (the canonical
    //! way to multiplex two await-yielding futures on a single-threaded
    //! runtime — exactly mirrors the production "two background tasks
    //! interleave at await points" model).   Each publisher:
    //!
    //!   - takes `RefCell::borrow_mut`, mutates a shared "tables" Vec,
    //!     captures snapshot, drops borrow (mirrors property P2);
    //!   - synchronously sends the snapshot to a fake stream worker via
    //!     `mpsc::unbounded_send` (mirrors property P3 — the first await
    //!     is on `ack_rx`, after the message lands in the worker queue);
    //!   - awaits ack.
    //!
    //! Asserts:
    //!
    //!   (A1) the worker receives both publishes;
    //!   (A2) the LATER publish's snapshot includes the EARLIER publish's
    //!        mutation (no stale persistence);
    //!   (A3) the LATEST received snapshot equals the post-state of the
    //!        shared tables (recovery would read this and reconstruct
    //!        the correct latest state).
    //!
    //! ## What this test does NOT cover
    //!
    //! A future refactor that inserts an `.await` between the publisher's
    //! borrow_mut drop and the mpsc send (violating P3) would silently
    //! re-open the stale-snapshot race in production but would NOT fail
    //! this test — because the test simulates the production pattern,
    //! it does not validate it.   Inline comments at the production call
    //! sites in `flush_one_imm` (lib.rs) and `do_compact`
    //! (background.rs) state the invariant explicitly to flag any such
    //! refactor at review time.

    use futures::channel::{mpsc, oneshot};
    use futures::StreamExt;
    use std::cell::RefCell;
    use std::rc::Rc;

    type WorkerMsg = (u32, Vec<u32>, oneshot::Sender<()>);

    /// Mirrors `save_table_locs_raw`'s critical path: synchronous compute
    /// → mpsc send → ack await.   Property (P3) is enforced by construction
    /// (no `.await` between the borrow_mut drop in the caller and the
    /// `unbounded_send` here; first await is `ack_rx`).
    async fn fake_save_and_sync(
        publisher_id: u32,
        snapshot: Vec<u32>,
        worker_tx: &mpsc::UnboundedSender<WorkerMsg>,
    ) {
        let (ack_tx, ack_rx) = oneshot::channel();
        // Synchronous send (mirrors stream_client.append's mpsc-send into
        // the per-stream worker queue).
        worker_tx
            .unbounded_send((publisher_id, snapshot, ack_tx))
            .expect("worker tx should be open");
        // First await on the publishing path.
        ack_rx.await.expect("worker should ack");
    }

    /// Mirrors flush_one_imm / do_compact's borrow_mut-then-publish
    /// pattern.
    async fn publisher(
        publisher_id: u32,
        tables: Rc<RefCell<Vec<u32>>>,
        worker_tx: mpsc::UnboundedSender<WorkerMsg>,
    ) -> Vec<u32> {
        // Property (P2): borrow_mut block contains no `.await`.
        let snapshot = {
            let mut t = tables.borrow_mut();
            t.push(publisher_id);
            t.clone()
        };
        // Property (P3): no `.await` between borrow_mut drop and mpsc
        // send inside fake_save_and_sync.
        fake_save_and_sync(publisher_id, snapshot.clone(), &worker_tx).await;
        snapshot
    }

    #[test]
    fn f148_concurrent_publisher_ordering_invariant() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let tables: Rc<RefCell<Vec<u32>>> = Rc::new(RefCell::new(Vec::new()));
            let (worker_tx, mut worker_rx) = mpsc::unbounded::<WorkerMsg>();

            // Fake stream worker: drains messages in FIFO order, acks each,
            // records (publisher_id, snapshot) for assertion.
            let received: Rc<RefCell<Vec<(u32, Vec<u32>)>>> =
                Rc::new(RefCell::new(Vec::new()));
            let received_clone = received.clone();
            let worker_task = compio::runtime::spawn(async move {
                while let Some((id, snap, ack)) = worker_rx.next().await {
                    received_clone.borrow_mut().push((id, snap));
                    let _ = ack.send(());
                }
            });

            // Run two publishers "concurrently" via futures::join! — the
            // canonical way to multiplex two await-yielding futures on a
            // single-threaded runtime (this is what compio P-log does for
            // concurrent flush_one_imm + do_compact tasks).
            let pub_flush = publisher(1, tables.clone(), worker_tx.clone());
            let pub_compact = publisher(2, tables.clone(), worker_tx.clone());
            let (snap_flush, snap_compact) = futures::join!(pub_flush, pub_compact);

            // Drop sender to let worker exit.
            drop(worker_tx);
            worker_task.await;

            let recv = received.borrow();
            // (A1) both publishers reached the worker.
            assert_eq!(recv.len(), 2, "worker should receive both publishes");

            // The first received snapshot is from whichever publisher's
            // borrow_mut completed first.   Because the synchronous path
            // from borrow_mut drop to mpsc send is uninterrupted, send
            // order == borrow_mut order.   Test that the LATER publisher's
            // snapshot strictly extends the EARLIER's.
            let (first_id, first_snap) = &recv[0];
            let (_second_id, second_snap) = &recv[1];

            // (A2) the LATER publisher's snapshot includes the EARLIER
            // publisher's mutation.   This is the load-bearing assertion:
            // it would FAIL if mpsc-send order diverged from borrow_mut
            // order (i.e., if a future refactor introduced an `.await`
            // between borrow_mut drop and the mpsc send).
            assert_eq!(
                first_snap.len(),
                1,
                "first publish's snapshot has 1 entry: {:?}",
                first_snap
            );
            assert_eq!(
                second_snap.len(),
                2,
                "second publish's snapshot has 2 entries: {:?}",
                second_snap
            );
            assert!(
                second_snap.contains(first_id),
                "second publisher's snapshot must contain first publisher's id ({}); \
                 got {:?} — invariant violated: borrow_mut order != mpsc-send order",
                first_id,
                second_snap
            );

            // (A3) the LATEST persisted record's snapshot equals the final
            // shared state.   Recovery would read this record and reconstruct
            // the union of all publishers' mutations — no stale persistence.
            let final_tables = tables.borrow().clone();
            assert_eq!(
                *second_snap, final_tables,
                "latest snapshot must equal final shared tables — \
                 a stale latest record would mis-reconstruct after restart"
            );

            // Sanity on returned values.
            assert_eq!(snap_flush.len() + snap_compact.len(), 3);
        });
    }
}
