use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::extent_rpc::{
    AppendReq, AppendResp, CommitLengthReq, CommitLengthResp, ExtentInfo, ProbeExtentReq,
    ProbeExtentResp, ReadBytesReq, ReadBytesResp, StreamInfo, SyncedLengthReq, SyncedLengthResp,
    encode_chain_prefix, CODE_EVERSION_MISMATCH, CODE_LOCKED_BY_OTHER, CODE_NOT_FOUND, CODE_OK,
    MSG_APPEND, MSG_APPEND_CHAIN,
    MSG_COMMIT_LENGTH, MSG_PROBE_EXTENT, MSG_READ_BYTES, MSG_READ_BYTES_ZC, MSG_SYNCED_LENGTH,
};
use crate::ConnPool;
use anyhow::{anyhow, Result};
use autumn_common::metrics::{duration_to_ns, ns_to_ms, unix_time_ms};
use autumn_rpc::manager_rpc::{self, *};

/// Sentinel error attached to `anyhow::Error` when a `MSG_READ_BYTES`
/// reply carries `CODE_EVERSION_MISMATCH`. Top-level
/// `read_bytes_from_extent` downcasts to this type to drive the
/// cache-invalidate-and-retry path. Carries no fields — the only
/// remediation is to refetch `ExtentInfo` from the manager.
#[derive(Debug)]
struct EversionStale;

impl std::fmt::Display for EversionStale {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("eversion mismatch (stale extent_info_cache)")
    }
}

impl std::error::Error for EversionStale {}

fn is_eversion_stale(err: &anyhow::Error) -> bool {
    err.chain().any(|e| e.is::<EversionStale>())
}

/// F204: structured sentinel for "VP points past manager-recorded
/// `sealed_length`" — historical data corruption from the
/// pre-2026-04-27 `handle_stream_alloc_extent` race against EC
/// conversion (see `crates/stream/CLAUDE.md` programming note 6).
/// Manager writes to `sealed_length` are now monotonic-by-construction
/// (F138/F145/F146/F147/F149 + the 2026-04-27 `if tail.sealed_length > 0`
/// guard), so no NEW corruption can arise — but etcd values that were
/// shrunken before those fixes shipped persist, and the physical EC
/// shards were truncated during the bug window so the bytes past
/// `sealed_length` are NOT recoverable.
///
/// External operational tooling (a Python audit/repair script per
/// `feedback_ops_tools_in_python` memory) downcasts to this type via
/// `anyhow::Error::chain()` to identify "this key is permanently
/// gone; clean it up" vs transient errors. The Display string is a
/// **stable wire contract**: the prefix `stale_vp_offset_past_sealed_length:`
/// and the field order `extent= offset= length= sealed_length=` MUST
/// NOT change.
#[derive(Debug, Clone)]
pub struct StaleVpOffset {
    pub extent_id: u64,
    pub requested_offset: u64,
    pub requested_length: u64,
    pub sealed_length: u64,
}

impl std::fmt::Display for StaleVpOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "stale_vp_offset_past_sealed_length: extent={} offset={} length={} sealed_length={}",
            self.extent_id, self.requested_offset, self.requested_length, self.sealed_length,
        )
    }
}

impl std::error::Error for StaleVpOffset {}
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use futures::channel::{mpsc, oneshot};
use futures::future::join_all;

/// F259: one-shot direct EN read of a value byte range, for clients holding
/// a MSG_GET_REDIRECT descriptor. No StreamClient (no owner lock, no manager)
/// — the zero-copy wire read the PS itself uses (MSG_READ_BYTES_ZC +
/// call_into_pooled: UCX registered recv / TCP owned read, no FrameDecoder
/// accumulation). Returns a `Bytes` ALIASING the pool buffer (returns to the
/// pool on drop). Errors (eversion mismatch, extent gone, replica down)
/// surface for the caller's proxy fallback.
pub async fn read_extent_value_direct(
    pool: &crate::ConnPool,
    addr: &str,
    extent_id: u64,
    eversion: u64,
    offset: u64,
    length: u64,
) -> Result<bytes::Bytes> {
    let req = ReadBytesReq {
        extent_id,
        eversion,
        offset,
        length,
    };
    let (pb, code) = pool
        .call_into_pooled(addr, MSG_READ_BYTES_ZC, req.encode(), Duration::from_secs(3))
        .await?;
    if code != CODE_OK {
        return Err(anyhow!(
            "direct read from {addr} extent={extent_id}: code={}",
            crate::extent_rpc::code_description(code)
        ));
    }
    let mut value = bytes::Bytes::from_owner(pb);
    if value.len() > length as usize {
        value.truncate(length as usize);
    }
    // coco P1 (F259): a short payload under CODE_OK (sealed_length clamp,
    // stale VP, EN-side truncation) must be a FAILURE — the proxy path's
    // read_value_from_log enforces the same "got < need" check. Returning
    // short bytes as Ok would hand the caller silently corrupt data.
    if value.len() < length as usize {
        return Err(anyhow!(
            "direct read short from {addr} extent={extent_id}: need {length} got {}",
            value.len()
        ));
    }
    Ok(value)
}

// ── F258: replicated-read spreading + hedging ──────────────────────────

/// F258 (b): hedge delay (ms) for replicated sealed-extent reads. 0
/// (default) = hedging disabled; rotation (F258 (a)) still applies. Set
/// once at process start from a CLI flag (`autumn-ps --read-hedge-ms`,
/// translated from `AUTUMN_READ_HEDGE_MS` by cluster.sh) — no env reads
/// in library code per the F195 discipline.
static READ_HEDGE_MS_CELL: std::sync::OnceLock<u64> = std::sync::OnceLock::new();

pub fn set_read_hedge_ms(ms: u64) -> bool {
    READ_HEDGE_MS_CELL.set(ms).is_ok()
}

pub(crate) fn read_hedge_ms() -> u64 {
    *READ_HEDGE_MS_CELL.get_or_init(|| 0)
}

/// F260: minimum total append payload (bytes) for CHAINED replication —
/// the writer sends ONE copy to replica[0] which pipelines to the rest
/// (PS egress 3x -> 1x for large writes). 0 = chaining disabled (always
/// star fanout). Default 64 KiB (the zc_worthwhile threshold). Set once
/// at process start (`autumn-ps --append-chain-min-bytes`).
static APPEND_CHAIN_MIN_CELL: std::sync::OnceLock<u32> = std::sync::OnceLock::new();

pub fn set_append_chain_min_bytes(n: u32) -> bool {
    APPEND_CHAIN_MIN_CELL.set(n).is_ok()
}

pub(crate) fn append_chain_min_bytes() -> u32 {
    // Default OFF (0): chaining trades per-append latency (store-and-
    // forward hops stack) for writer-egress bandwidth — a win only where
    // the writer NIC is the bottleneck (cross-host). Opt in via
    // `autumn-ps --append-chain-min-bytes 65536`.
    *APPEND_CHAIN_MIN_CELL.get_or_init(|| 0)
}

/// F258 (a): deterministic start-replica rotation for sealed-extent reads.
/// SplitMix64 finalizer over `(extent_id, offset)` — no extra deps, cheap,
/// and well-mixed so consecutive chunk offsets of one large read stripe
/// across replicas instead of clustering.
pub(crate) fn rotated_replica_start(extent_id: u64, offset: u64, n: usize) -> usize {
    if n <= 1 {
        return 0;
    }
    let mut x = extent_id ^ ((offset as u64) << 32) ^ 0x9e37_79b9_7f4a_7c15;
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    (x % n as u64) as usize
}

/// WAL self-heal A1 (docs/wal_selfheal_design.md I2): replica slot indices
/// (into `replicates ++ parity`, = the `read_replicated_with_failover` addr
/// order) eligible to serve a READ.
///
/// For a SEALED, REPLICATED extent, slots whose `avali` bit is 0 are EXCLUDED —
/// a recovering / isolated-corrupt replica must not serve reads (clearing the
/// avali bit is how the self-heal isolation removes a bit-rotted replica from
/// the serving set). OPEN extents are NOT filtered: `avali = 0` is the normal
/// "not sealed yet" state there, and open-extent read consistency is the
/// commit-min protocol's job, not avali's. (EC extents never reach this helper —
/// they go through `ec_subrange_read`, where a missing shard is reconstructed,
/// not skipped; filtering their addr list would break shard↔addr alignment.)
///
/// Defensive: if filtering would leave ZERO eligible slots (a sealed extent
/// with every avali bit clear — shouldn't happen, since seal sets all_bits),
/// fall back to ALL slots so reads don't regress; the caller logs the fallback.
pub(crate) fn eligible_replica_slots(ex: &ExtentInfo) -> Vec<usize> {
    let n = ex.replicates.len() + ex.parity.len();
    if !ex.sealed {
        return (0..n).collect();
    }
    let elig: Vec<usize> = (0..n).filter(|&i| (ex.avali & (1u32 << i)) != 0).collect();
    if elig.is_empty() {
        (0..n).collect()
    } else {
        elig
    }
}

/// F276: `replicates ++ parity` node ids in slot order — the SAME order that
/// `replica_addrs_from_cache` resolves addresses in, so a read slot index maps
/// back to its `node_id` (needed to consult the Suspected snapshot).
pub(crate) fn replica_node_ids(ex: &ExtentInfo) -> Vec<u64> {
    ex.replicates.iter().chain(ex.parity.iter()).copied().collect()
}

/// F276: slot try-order for a REPLICATED read. Starts from the F258 rotated
/// start, keeps only `avali`-eligible slots, and moves slots whose node the
/// manager currently believes `Suspected` to the BACK — healthy replicas are
/// tried first so a flaky node never costs a per-read RPC timeout before
/// failover. The suspected slots are KEPT (appended, not dropped): a
/// suspected node is not dead, and every committed byte of a sealed extent is
/// on every replica, so they remain a correct last-resort. With an empty
/// `suspected` set (the common case) the result is byte-identical to the
/// pre-F276 rotated order, so the hot path is unchanged.
pub(crate) fn replicated_read_order(
    ex: &ExtentInfo,
    offset: u64,
    suspected: &HashSet<u64>,
) -> Vec<usize> {
    let n = ex.replicates.len() + ex.parity.len();
    if n == 0 {
        return Vec::new();
    }
    let start = if ex.sealed {
        rotated_replica_start(ex.extent_id, offset, n)
    } else {
        0
    };
    let eligible = eligible_replica_slots(ex);
    let filtered = eligible.len() < n;
    let node_ids = replica_node_ids(ex);
    let mut healthy: Vec<usize> = Vec::with_capacity(eligible.len());
    let mut flaky: Vec<usize> = Vec::new();
    for i in 0..n {
        let slot = (start + i) % n;
        if filtered && !eligible.contains(&slot) {
            continue; // isolated (avali=0) replica — never serve from it
        }
        let nid = node_ids.get(slot).copied().unwrap_or(0);
        if suspected.contains(&nid) {
            flaky.push(slot);
        } else {
            healthy.push(slot);
        }
    }
    healthy.extend(flaky);
    healthy
}

/// F276: eligible replica slots with manager-`Suspected` nodes DROPPED — but
/// only when at least one healthy eligible slot remains (else fall back to ALL
/// eligible, never strand). Used by the CLIENT-DIRECT descriptor path
/// (`extent_read_descriptor`), whose external consumer picks its OWN
/// hash-rotated start over the returned address list — so merely reordering
/// (as `replicated_read_order` does for the in-order paths) would NOT route
/// around a flaky node; the suspected address has to be excluded outright.
/// Soft hint with the same fallback guarantee: an empty Suspected snapshot
/// returns exactly `eligible_replica_slots`.
pub(crate) fn healthy_eligible_slots(ex: &ExtentInfo, suspected: &HashSet<u64>) -> Vec<usize> {
    let eligible = eligible_replica_slots(ex);
    if suspected.is_empty() {
        return eligible;
    }
    let node_ids = replica_node_ids(ex);
    let healthy: Vec<usize> = eligible
        .iter()
        .copied()
        .filter(|&s| !suspected.contains(&node_ids.get(s).copied().unwrap_or(0)))
        .collect();
    if healthy.is_empty() {
        eligible
    } else {
        healthy
    }
}

use futures::stream::FuturesUnordered;
use futures::{FutureExt, SinkExt, StreamExt};

#[derive(Debug, Clone)]
pub struct AppendResult {
    pub extent_id: u64,
    pub offset: u64,
    pub end: u64,
}

#[derive(Debug, Clone)]
struct StreamTail {
    extent: ExtentInfo,
    replica_addrs: Vec<String>,
    /// F190: parallel to `replica_addrs` — `replicates ++ parity` node ids
    /// in the same index order used by `replica_addrs_from_cache`. Required
    /// so `apply_completion` can resolve a failing replica index back to
    /// its `node_id` for the per-stream `bad_nodes` exclusion list.
    replica_node_ids: Vec<u64>,
}

/// Per-stream append state owned exclusively by the stream worker task.
///
/// R3 state machine (lease_cursor + pending_acks + in_flight + poisoned)
/// is preserved; R4 step 4.3 removes the external `Arc<Mutex<_>>` because
/// the single-owner worker task serialises all state mutations.
///
/// - `tail`: cached `StreamTail`. None = needs reload (first use or after
///           NotFound / alloc_new_extent).
/// - `commit`: highest acked `end` that forms a contiguous prefix. Matches
///             Go's `sc.end`. Starts at 0 for a fresh extent.
/// - `lease_cursor`: next offset to lease. Advances monotonically on lease;
///                   rewound only via `rewind_or_poison` on the most-recent
///                   lease fast path.
/// - `pending_acks`: acked-but-not-yet-prefix batches (offset → end).
/// - `in_flight`: count of leased-but-not-acked batches.
/// - `poisoned`: set on mid-sequence failure; forces the next caller to
///               reset the stream via alloc_new_extent.
/// A completed-on-all-replicas append waiting for the contiguous prefix
/// to reach it before the CALLER is acked (ENOSPC-1 P1). `ack` is None in
/// unit tests that only exercise the cursor arithmetic.
struct PendingAck {
    end: u64,
    ack: Option<(oneshot::Sender<Result<AppendResult>>, AppendResult)>,
}

struct StreamAppendState {
    tail: Option<StreamTail>,
    commit: u64,
    lease_cursor: u64,
    pending_acks: std::collections::BTreeMap<u64, PendingAck>,
    in_flight: u32,
    poisoned: bool,
    /// ENOSPC-1 P1: offset of the FIRST failed lease (u64::MAX = none).
    /// Completions at or above this can never join the contiguous prefix
    /// — the hole below them is permanent for this extent — so they are
    /// failed to the caller instead of acked. Reset on a genuine roll.
    failure_floor: u64,
    /// Set true by `SealCommit` after it drains + reports `commit`; while true,
    /// new `Append`s on the about-to-be-sealed tail are REJECTED (soft error →
    /// caller retries onto the fresh tail). Cleared by `ResetTail`
    /// (`reset_for_new_extent`). Closes the seal→ResetTail window where a
    /// concurrent append could write past the new `sealed_length` and have its
    /// acked data become unreadable (coco P1).
    sealing: bool,
    /// F190: per-stream "recently failed" node ids (`node_id → expires_at`).
    /// Shared with the public API via `Rc<RefCell<_>>` so
    /// `alloc_new_extent_once` can snapshot non-expired entries to pass via
    /// `StreamAllocExtentReq.exclude_node_ids`. Worker writes on
    /// `apply_completion` Err; public API reads + prunes on snapshot. TTL
    /// covers a full manager polling cycle without holding excludes long
    /// enough to block natural disk recovery (default 30 s, env-tunable).
    bad_nodes: Rc<RefCell<HashMap<u64, Instant>>>,
    /// F192: cloned sender for the per-StreamClient failure-report
    /// drainer. `try_send` from `apply_completion` Err; drops the event
    /// silently when full (best-effort).
    failure_report_tx: mpsc::Sender<FailureReport>,
    /// F195: snapshotted F190 TTL — clone-on-spawn from StreamClientConfig
    /// so the worker doesn't need to re-clone the config Rc per Err.
    bad_nodes_ttl: Duration,
}

// F195: F190 TTL helper `bad_nodes_ttl()` removed. Value now lives on
// `StreamClientConfig.bad_nodes_ttl` (defined below) — set once at
// `StreamClient` construction, snapshotted into `StreamAppendState`
// for the worker. Was previously env `AUTUMN_STREAM_BAD_NODES_TTL_SECS`,
// now CLI-flag-driven on the PS binary.
impl StreamAppendState {
    fn new(
        bad_nodes: Rc<RefCell<HashMap<u64, Instant>>>,
        failure_report_tx: mpsc::Sender<FailureReport>,
        bad_nodes_ttl: Duration,
    ) -> Self {
        Self {
            tail: None,
            commit: 0,
            lease_cursor: 0,
            pending_acks: std::collections::BTreeMap::new(),
            in_flight: 0,
            poisoned: false,
            failure_floor: u64::MAX,
            sealing: false,
            bad_nodes,
            failure_report_tx,
            bad_nodes_ttl,
        }
    }

    /// F190: mark a node as recently failed. Called from `apply_completion`
    /// Err paths with the `node_id` resolved from the cached `ExtentInfo`
    /// via `StreamTail.replica_node_ids[failing_index]`. Refresh-on-insert
    /// (overwrites the existing expires_at) so a chain of failures keeps
    /// the entry hot.
    fn mark_bad_node(&self, node_id: u64) {
        let expires_at = Instant::now() + self.bad_nodes_ttl;
        self.bad_nodes.borrow_mut().insert(node_id, expires_at);
    }

    /// F192: best-effort fire of a `MSG_REPORT_DISK_FAILURE` event into
    /// the per-StreamClient drainer. Drops on full channel — the manager
    /// quorum debounce tolerates loss (per-stream alloc route-around via
    /// F190 stays as the primary defense).
    fn try_report_failure(&mut self, node_id: u64, extent_id: u64) {
        let _ = self
            .failure_report_tx
            .try_send(FailureReport { node_id, extent_id });
    }

    fn reset_for_new_extent(&mut self) {
        self.commit = 0;
        self.lease_cursor = 0;
        self.pending_acks.clear();
        self.failure_floor = u64::MAX;
        self.in_flight = 0;
        self.poisoned = false;
        // ResetTail moves to a fresh tail → un-freeze: the seal→reset window
        // (during which appends were rejected) is over.
        self.sealing = false;
    }

    /// Apply a `ResetTail`. A DIFFERENT extent is a genuine roll to a fresh,
    /// empty tail → reset ALL append-progress state (commit=0 etc.). The SAME
    /// extent is a soft-error tail reload that did NOT change the tail (a
    /// transient replica failure) → PRESERVE every byte of append-progress
    /// state (commit / lease_cursor / pending_acks / in_flight / poisoned /
    /// sealing) and only refresh the cached tail metadata (replica addrs).
    ///
    /// BUG#2 (seed=8): zeroing `commit` on a same-extent reload let a later
    /// `seal_commit_watermark` report commit=0 and `alloc_new_extent(Some(0))`
    /// seal the live extent at sealed_length=0 — orphaning every acked VP/SST
    /// byte past 0 (a split child inheriting this CoW tail then can never open:
    /// `stale_vp_offset_past_sealed_length`). `commit` only ever advances on a
    /// full all-replica ACK, so it is ground truth for THIS extent. `poisoned`
    /// is preserved too: under concurrent same-stream appends a non-tail-lease
    /// failure marks a HOLE (`rewind_or_poison`); kept poisoned, the next
    /// Append is rejected so the caller escalates to seal-and-roll, sealing at
    /// the contiguous `commit` (hole + everything after correctly excluded).
    /// (coco review, 2026-05-30.)
    fn apply_reset_tail(&mut self, tail: StreamTail) {
        let same_extent =
            self.tail.as_ref().map(|t| t.extent.extent_id) == Some(tail.extent.extent_id);
        if !same_extent {
            self.reset_for_new_extent();
        }
        self.tail = Some(tail);
    }

    fn lease(&mut self, size: u64) -> (u64, u64) {
        let offset = self.lease_cursor;
        let end = offset + size;
        self.lease_cursor = end;
        self.in_flight += 1;
        (offset, end)
    }

    /// Record an all-replica-completed append and fire CALLER acks only
    /// for the ranges the contiguous prefix now covers (ENOSPC-1 P1).
    /// Pre-fix, the caller ack fired unconditionally on completion: with
    /// a lower lease failed (hole) and this range completed above it, the
    /// caller saw Ok while `commit` (the seal/SealCommit watermark —
    /// "hole + everything after correctly excluded", see
    /// `apply_reset_tail`) stayed below — the eventual seal CHOPPED an
    /// acked range. Now acked ⊆ contiguous-commit, always.
    fn ack(
        &mut self,
        offset: u64,
        end: u64,
        ack: Option<(oneshot::Sender<Result<AppendResult>>, AppendResult)>,
    ) {
        self.in_flight = self.in_flight.saturating_sub(1);
        if offset >= self.failure_floor {
            // A lower lease already failed — this range is beyond the
            // permanent hole and will be excluded by the roll's seal.
            // Fail the caller (it retries on the fresh tail; the bytes on
            // the replicas become benign un-acked duplicates).
            if let Some((tx, _)) = ack {
                let _ = tx.send(Err(anyhow!(
                    "append completed but a lower offset failed (hole below);                      not acked — retry on a fresh extent"
                )));
            }
            return;
        }
        self.pending_acks.insert(offset, PendingAck { end, ack });
        while let Some((&off, slot)) = self.pending_acks.iter().next() {
            if off == self.commit {
                self.commit = slot.end;
                let slot = self.pending_acks.remove(&off).expect("just observed");
                if let Some((tx, result)) = slot.ack {
                    let _ = tx.send(Ok(result));
                }
            } else {
                break;
            }
        }
    }

    fn rewind_or_poison(&mut self, offset: u64, size: u64) {
        self.in_flight = self.in_flight.saturating_sub(1);
        if offset + size == self.lease_cursor {
            self.lease_cursor = offset;
        } else {
            self.poisoned = true;
            // ENOSPC-1 P1: everything pending at/above the hole can never
            // join the contiguous prefix — fail those callers now instead
            // of acking ranges the roll's seal will discard.
            self.failure_floor = self.failure_floor.min(offset);
            let dead = self.pending_acks.split_off(&self.failure_floor);
            for (_, slot) in dead {
                if let Some((tx, _)) = slot.ack {
                    let _ = tx.send(Err(anyhow!(
                        "append completed but a lower offset failed (hole below);                          not acked — retry on a fresh extent"
                    )));
                }
            }
        }
    }
}

#[derive(Default)]
struct StreamAppendMetrics {
    ops: AtomicU64,
    retries: AtomicU64,
    lock_wait_ns: AtomicU64,
    extent_lookup_ns: AtomicU64,
    fanout_ns: AtomicU64,
    total_ns: AtomicU64,
    last_report_ms: AtomicU64,
}

impl StreamAppendMetrics {
    fn record(
        &self,
        owner_key: &str,
        lock_wait: Duration,
        extent_lookup: Duration,
        fanout: Duration,
        total: Duration,
        retries: u64,
    ) {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.retries.fetch_add(retries, Ordering::Relaxed);
        self.lock_wait_ns
            .fetch_add(duration_to_ns(lock_wait), Ordering::Relaxed);
        self.extent_lookup_ns
            .fetch_add(duration_to_ns(extent_lookup), Ordering::Relaxed);
        self.fanout_ns
            .fetch_add(duration_to_ns(fanout), Ordering::Relaxed);
        self.total_ns
            .fetch_add(duration_to_ns(total), Ordering::Relaxed);
        self.maybe_report(owner_key);
    }

    fn maybe_report(&self, owner_key: &str) {
        let now_ms = unix_time_ms();
        let last = self.last_report_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last) < 1000 {
            return;
        }
        if self
            .last_report_ms
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let ops = self.ops.swap(0, Ordering::Relaxed);
        if ops == 0 {
            return;
        }
        let retries = self.retries.swap(0, Ordering::Relaxed);
        let lock_wait_ns = self.lock_wait_ns.swap(0, Ordering::Relaxed);
        let extent_lookup_ns = self.extent_lookup_ns.swap(0, Ordering::Relaxed);
        let fanout_ns = self.fanout_ns.swap(0, Ordering::Relaxed);
        let total_ns = self.total_ns.swap(0, Ordering::Relaxed);
        tracing::info!(
            owner_key,
            ops,
            retries,
            avg_lock_wait_ms = ns_to_ms(lock_wait_ns, ops),
            avg_extent_lookup_ms = ns_to_ms(extent_lookup_ns, ops),
            avg_fanout_ms = ns_to_ms(fanout_ns, ops),
            avg_total_ms = ns_to_ms(total_ns, ops),
            "stream append summary",
        );
    }
}

// ── R4 4.3 — StreamClient per-stream SQ/CQ worker ────────────────────────
//
// Each stream_id gets ONE worker compio task (spawned lazily on first
// append*). The worker owns `StreamAppendState` + a `FuturesUnordered` of
// in-flight 3-replica joins. NO external Mutex: all state mutations happen
// inside the worker.
//
// Public API → worker via a bounded mpsc. Worker replies via per-op
// oneshot. The append retry loop lives in the public API (Option A of the
// spec) — the worker is a stateful single-op executor, not a retry engine.
//
// Tail invalidation on alloc_new_extent: the public API explicitly sends
// `ResetTail` to the worker before resubmission — no hidden staleness, no
// generation counter, no extra probe from the worker.
//
// Construction uses `Rc::new_cyclic` so StreamClient stores a `Weak<Self>`
// that the worker can use for the removal-guard on exit — without forming
// an Rc cycle that would prevent shutdown.

/// Bounded capacity of the per-stream submit mpsc. Saturated callers park
/// on `send().await` — natural upstream back-pressure.
const STREAM_SUBMIT_CAP: usize = 256;

/// BUG#1 (seed=15): `ensure_tail_initialised` retries the open-tail all-replica
/// `current_commit` probe this many times (with `OPEN_TAIL_COMMIT_BACKOFF_MS`
/// between) before giving up and sealing+rolling to a fresh tail. Small —
/// enough to ride out a brief blip, but the seal-and-roll escape is itself safe
/// (lenient seal-over-reachable ≥ acked), so a few attempts suffice.
const OPEN_TAIL_COMMIT_RETRIES: u32 = 3;
const OPEN_TAIL_COMMIT_BACKOFF_MS: u64 = 300;

// F195: env-reading helpers `stream_inflight_cap()`,
// `append_fanout_timeout()`, `read_chunk_bytes()` removed. Values live
// on `StreamClientConfig` (defined below) — set once at construction,
// CLI-flag-driven on the binary. Tests that need overrides build a
// custom `StreamClientConfig` instead of setting process-global env
// vars (hostile to parallel test runs).

/// F195: Stream client tunables. Default values match the pre-F195
/// env-default behaviour:
///   - `bad_nodes_ttl`: 30 s (F190)
///   - `inflight_cap`: 32 per-stream FU cap
///   - `append_fanout_timeout`: 5 s per-replica append deadline (F121)
///   - `read_chunk_bytes`: 256 MiB per replicated read chunk (F105)
///   - `synced_poll`: 2 ms F178 flush-barrier poll interval
///   - `synced_timeout`: 30 s F178 flush-barrier overall timeout
#[derive(Clone, Debug)]
pub struct StreamClientConfig {
    pub bad_nodes_ttl: Duration,
    pub inflight_cap: usize,
    pub append_fanout_timeout: Duration,
    pub read_chunk_bytes: u64,
    pub synced_poll: Duration,
    pub synced_timeout: Duration,
}

impl Default for StreamClientConfig {
    fn default() -> Self {
        Self {
            bad_nodes_ttl: Duration::from_secs(30),
            inflight_cap: 32,
            append_fanout_timeout: Duration::from_secs(5),
            read_chunk_bytes: 256 * 1024 * 1024,
            synced_poll: Duration::from_millis(2),
            synced_timeout: Duration::from_secs(30),
        }
    }
}

impl StreamClientConfig {
    /// F195: F190 TTL clamp `[1, 600]` seconds.
    pub fn with_bad_nodes_ttl(mut self, ttl: Duration) -> Self {
        let secs = ttl.as_secs().clamp(1, 600);
        self.bad_nodes_ttl = Duration::from_secs(secs);
        self
    }
    /// F195: F121 fanout clamp `[200 ms, 60 s]`.
    pub fn with_append_fanout_timeout(mut self, t: Duration) -> Self {
        let ms = t.as_millis().clamp(200, 60_000) as u64;
        self.append_fanout_timeout = Duration::from_millis(ms);
        self
    }
    /// F195: per-stream FU cap. 0 → default 32.
    pub fn with_inflight_cap(mut self, cap: usize) -> Self {
        self.inflight_cap = if cap == 0 { 32 } else { cap };
        self
    }
    /// F195: F105 chunk size. 0 → default 256 MiB.
    pub fn with_read_chunk_bytes(mut self, bytes: u64) -> Self {
        self.read_chunk_bytes = if bytes == 0 { 256 * 1024 * 1024 } else { bytes };
        self
    }
    /// F195: F178 flush-barrier poll interval `[1, 50] ms`.
    pub fn with_synced_poll(mut self, p: Duration) -> Self {
        let ms = p.as_millis().clamp(1, 50) as u64;
        self.synced_poll = Duration::from_millis(ms);
        self
    }
    /// F195: F178 flush-barrier overall timeout `≥ 100 ms`.
    pub fn with_synced_timeout(mut self, t: Duration) -> Self {
        let ms = (t.as_millis() as u64).max(100);
        self.synced_timeout = Duration::from_millis(ms);
        self
    }
}

/// Pure slicing logic shared by `ec_read_full_and_slice`. Returns the
/// `[offset, offset+length)` sub-slice of an EC-decoded payload (where
/// `length == 0` means "to end"), or an `Err` describing the
/// out-of-bounds condition. Lifted out of the async method so it can
/// be unit-tested without standing up a manager + extent-node fixture.
/// F170: takes `full_payload` by value so the full-read path
/// (offset=0, length=0) can return the decoded Vec directly with
/// zero copy. Pre-F170 this took `&[u8]` and unconditionally
/// `.to_vec()`'d, which on a 256 MiB EC-decoded payload spent
/// 50-100 ms memcpy'ing INLINE on the caller's compio runtime —
/// a thread-per-core violation. Sub-range reads still allocate
/// `read_len` bytes (typically small), but the full-extent EC
/// read path (the dominant case during recovery / VP fetches)
/// is now zero-copy after `spawn_blocking(ec_decode)` returns.
/// `extent_id` + `sealed_length` are pass-through context for the
/// F204 `StaleVpOffset` sentinel — they're not used by the
/// in-range slice path. We could derive `sealed_length` from
/// `full_payload.len()` (they agree in the happy path), but the
/// caller already has the manager-reported value and we want the
/// sentinel to surface what the manager THINKS the sealed length
/// is, not just what the decoded payload happens to be.
fn ec_slice_decoded(
    full_payload: Vec<u8>,
    offset: u64,
    length: u64,
    extent_id: u64,
    sealed_length: u64,
) -> Result<Vec<u8>> {
    if offset == 0 && length == 0 {
        return Ok(full_payload);
    }
    let start = offset as usize;
    if start > full_payload.len() {
        // F204: structured sentinel so the PS read path can map this
        // to `StatusCode::FailedPrecondition` and surface a stable
        // diagnostic to Python operational tooling.
        return Err(anyhow::Error::new(StaleVpOffset {
            extent_id,
            requested_offset: offset,
            requested_length: length,
            sealed_length,
        }));
    }
    let read_len = if length == 0 {
        full_payload.len() - start
    } else {
        length as usize
    };
    let slice_end = (start + read_len).min(full_payload.len());
    Ok(full_payload[start..slice_end].to_vec())
}

/// Message from public API to per-stream worker.
enum StreamSubmitMsg {
    /// Append payload segments; worker leases offsets, fans out to 3
    /// replicas, and acks on completion.
    ///
    /// F178: no `must_sync` field. Every append is durable via the
    /// extent-node's per-extent fsync coalescer. Pre-F178 this carried
    /// a `must_sync: bool` that the extent-node honoured to skip the
    /// fsync wait; that wire field was removed when --nosync was
    /// dropped.
    Append {
        payload_parts: Vec<Bytes>,
        owner_epoch: i64,
        ack_tx: oneshot::Sender<Result<AppendResult>>,
    },
    /// Replace the cached tail (used after alloc_new_extent, on a fresh
    /// stream's first init, or a soft-error tail reload). When `tail` is a
    /// DIFFERENT extent than the one the worker is on, this resets all
    /// append-progress state (lease_cursor/commit/pending/in_flight/poisoned/
    /// sealing) for the new extent. When it is the SAME extent (a transient-
    /// failure reload that did not change the tail), it only refreshes the
    /// cached tail metadata (replica addrs) and PRESERVES the worker's
    /// append-progress state — see the handler for why (BUG#2 + hole/poison
    /// safety).
    ResetTail { tail: StreamTail },
    /// Seed the lease_cursor/commit to a non-zero starting value.  Sent by
    /// the public API's tail-init path when the manager-tracked extent
    /// already has data (`current_commit > 0`); without this the next
    /// append would try to overwrite pre-existing bytes.
    SeedCursor { cursor: u64 },
    /// Failover seal handshake: the worker DRAINS every in-flight append
    /// (awaits all completions, applying each so `state.commit` reaches its
    /// final contiguous all-replica-acked prefix), then replies with that
    /// `state.commit`. The public API uses the returned watermark as the
    /// authoritative seal length for `alloc_new_extent`. This is the ONLY
    /// safe source — a public-API-tracked value always lags the worker and
    /// races concurrent out-of-order appends + rolls. The drain is bounded by
    /// each append's `append_fanout_timeout`, so it cannot hang.
    ///
    /// BUG2-IDEMPOTENT-ROLL: reply is `(commit, tail_extent_id)` — the worker
    /// is the only place that authoritatively knows WHICH extent the drained
    /// `commit` belongs to (its cached tail). The caller threads
    /// `tail_extent_id` into `alloc_new_extent` as `seal_extent_id` so a retried
    /// roll seals that exact extent (idempotent), never the freshly-rolled one.
    SealCommit {
        resp: oneshot::Sender<(u64, u64)>,
    },
    /// Explicit shutdown.  Dropping the last Sender also exits the worker
    /// via channel close — this variant is kept for symmetry / tests.
    #[allow(dead_code)]
    Shutdown,
}

/// Result of a single in-flight append — produced by the future the worker
/// pushes into its FuturesUnordered.
struct InflightResult {
    offset: u64,
    end: u64,
    extent_id: u64,
    /// Raw oneshot frames from each replica. `Err` slots are RPC/connection
    /// failures; `Ok(f)` slots include protocol-level error frames.
    frames: Vec<Result<autumn_rpc::Frame>>,
    /// F190: parallel to `frames` — `node_id` of each replica in the same
    /// index order. Lets `apply_completion` resolve a failing replica back
    /// to its `node_id` for the per-stream `bad_nodes` exclusion list.
    replica_node_ids: Vec<u64>,
    ack_tx: oneshot::Sender<Result<AppendResult>>,
}

/// Pinned boxed type for the FuturesUnordered payload.
type InflightFut = std::pin::Pin<Box<dyn std::future::Future<Output = InflightResult>>>;

/// Guard that, on drop, removes the per-stream worker's sender from the
/// outer `stream_workers` map. Dropped inside the worker on exit so the
/// next call to `append_*` spawns a fresh worker instead of finding a
/// stale Sender whose receiver is gone.
struct WorkerRemovalGuard {
    sc: Weak<StreamClient>,
    stream_id: u64,
}

impl Drop for WorkerRemovalGuard {
    fn drop(&mut self) {
        if let Some(sc) = self.sc.upgrade() {
            sc.stream_workers.borrow_mut().remove(&self.stream_id);
        }
    }
}

/// F192: drainer task for per-StreamClient failure reports. Holds a
/// `Weak<StreamClient>` so it exits naturally when the StreamClient is
/// dropped (mpsc Receiver close also exits the loop, but Weak coverage
/// makes shutdown deterministic when senders survive in a worker mid-
/// drop). Each report is sent fire-and-forget against the current
/// manager address; failures are logged at trace and otherwise
/// ignored — F190's per-stream alloc route-around remains the primary
/// defense.
async fn failure_report_drain_loop(sc: Weak<StreamClient>, mut rx: mpsc::Receiver<FailureReport>) {
    use futures::StreamExt;
    while let Some(report) = rx.next().await {
        let Some(sc) = sc.upgrade() else {
            return;
        };
        let reporter = sc.reporter_part_id.get();
        if reporter == 0 {
            // No reporter id configured — skip rather than poll the
            // manager with sentinel zeros.
            continue;
        }
        let ts_ms = autumn_common::metrics::unix_time_ms() as i64;
        let req = manager_rpc::rkyv_encode(&manager_rpc::ReportDiskFailureReq {
            node_id: report.node_id,
            extent_id: report.extent_id,
            error_kind: manager_rpc::REPORT_DISK_FAILURE_KIND_GENERIC,
            reporter_part_id: reporter,
            ts_ms,
        });
        let addr = sc.manager_addr().to_string();
        // Fire-and-forget: best-effort. The handler does not return
        // a meaningful response payload (CODE_OK CodeResp), and a
        // dropped report is benign — F190's per-stream alloc route-
        // around handles the per-call need.
        // 5 s — fire-and-forget telemetry; bounded so a slow manager
        // doesn't keep this background task alive past a coarse SLO.
        if let Err(e) = sc
            .pool
            .call_timeout(
                &addr,
                manager_rpc::MSG_REPORT_DISK_FAILURE,
                req,
                Duration::from_secs(5),
            )
            .await
        {
            tracing::trace!(
                manager = %addr,
                node_id = report.node_id,
                extent_id = report.extent_id,
                error = %e,
                "f192 report_disk_failure send failed"
            );
        }
    }
}

/// SealCommit drain: await EVERY in-flight append and apply its completion so
/// `state.commit` reaches its final contiguous all-replica-acked prefix before
/// the worker reports the seal watermark. Bounded by each append's
/// `append_fanout_timeout` (every `InflightFut` resolves within it — success,
/// error, or timeout), so it cannot hang even if a replica is dead. After this
/// returns, `inflight` is empty and `state.commit` is the exact length to seal
/// the current tail at: every all-replica-acked byte is included, every
/// un-acked (failed/timed-out) byte is excluded (those callers retry onto the
/// fresh tail). No phantom, no truncation.
async fn drain_inflight_for_seal(
    state: &mut StreamAppendState,
    inflight: &mut FuturesUnordered<InflightFut>,
) {
    while let Some(result) = inflight.next().await {
        apply_completion(state, result);
    }
}

/// Apply one `StreamSubmitMsg` work item inside `stream_worker_loop`. Returns
/// `ControlFlow::Break(())` for `Shutdown` (the caller drains inflight + exits),
/// `Continue(())` otherwise. Extracted because the four work arms (ResetTail /
/// SeedCursor / SealCommit / Append) were byte-identical between the idle branch
/// and the `select` Left branch; the SQ/CQ arbitration loop is unchanged.
async fn apply_stream_submit_msg(
    state: &mut StreamAppendState,
    pool: &Rc<ConnPool>,
    inflight: &mut FuturesUnordered<InflightFut>,
    msg: StreamSubmitMsg,
    stream_id: u64,
    append_timeout: Duration,
) -> std::ops::ControlFlow<()> {
    match msg {
        StreamSubmitMsg::Shutdown => return std::ops::ControlFlow::Break(()),
        StreamSubmitMsg::ResetTail { tail } => {
            // Same-extent reload preserves the worker's append-progress state;
            // different-extent roll resets it. See
            // `StreamAppendState::apply_reset_tail` (BUG#2, seed=8).
            state.apply_reset_tail(tail);
        }
        StreamSubmitMsg::SeedCursor { cursor } => {
            state.commit = cursor;
            state.lease_cursor = cursor;
        }
        StreamSubmitMsg::SealCommit { resp } => {
            drain_inflight_for_seal(state, inflight).await;
            // BUG2 trace (opt-in, target `bug2_trace`): the worker's post-drain
            // `state.commit` IS the authoritative seal length it reports. A
            // `reported_commit=0` for a `tail_extent` that physically holds acked
            // data means THIS worker did not own the writes (reset/fresh worker
            // after invalidate_stream / ResetTail) — the dual-writer under-seal.
            tracing::info!(
                target: "bug2_trace",
                stream_id,
                reported_commit = state.commit,
                tail_extent = ?state.tail.as_ref().map(|t| t.extent.extent_id),
                in_flight = state.in_flight,
                "BUG2 SealCommit reply"
            );
            // BUG2-IDEMPOTENT-ROLL: report the tail extent id alongside the
            // commit so the caller can pin the seal to THIS extent.
            let tail_eid = state
                .tail
                .as_ref()
                .map(|t| t.extent.extent_id)
                .unwrap_or(0);
            let _ = resp.send((state.commit, tail_eid));
            // Freeze the tail until ResetTail so no append lands past the
            // just-reported seal point (coco P1).
            state.sealing = true;
        }
        StreamSubmitMsg::Append {
            payload_parts,
            owner_epoch,
            ack_tx,
        } => {
            launch_append(
                state,
                pool,
                inflight,
                payload_parts,
                owner_epoch,
                ack_tx,
                append_timeout,
            )
            .await;
        }
    }
    std::ops::ControlFlow::Continue(())
}

async fn stream_worker_loop(
    stream_id: u64,
    mut submit_rx: mpsc::Receiver<StreamSubmitMsg>,
    pool: Rc<ConnPool>,
    bad_nodes: Rc<RefCell<HashMap<u64, Instant>>>,
    failure_report_tx: mpsc::Sender<FailureReport>,
    // F195: tunables snapshot — clone-of-Rc<StreamClientConfig> from
    // the StreamClient, captured at spawn time. No env reads.
    config: Rc<StreamClientConfig>,
    removal_guard: WorkerRemovalGuard,
) {
    use futures::future::{select, Either};

    let cap = config.inflight_cap;
    let append_timeout = config.append_fanout_timeout;
    let mut state = StreamAppendState::new(bad_nodes, failure_report_tx, config.bad_nodes_ttl);
    let mut inflight: FuturesUnordered<InflightFut> = FuturesUnordered::new();

    loop {
        // (A) Opportunistically drain any already-ready completions.
        while let Some(Some(result)) = inflight.next().now_or_never() {
            apply_completion(&mut state, result);
        }

        let n_inflight = inflight.len();
        let at_cap = n_inflight >= cap;

        if n_inflight == 0 {
            // Idle: only SQ can progress. Nothing inflight to drain on exit.
            let should_break = match submit_rx.next().await {
                None => true,
                Some(msg) => apply_stream_submit_msg(
                    &mut state,
                    &pool,
                    &mut inflight,
                    msg,
                    stream_id,
                    append_timeout,
                )
                .await
                .is_break(),
            };
            if should_break {
                break;
            }
            continue;
        }

        if at_cap {
            // Back-pressure: only CQ can progress. Callers parked on
            // submit_tx.send() will wake once we pop a completion.
            if let Some(result) = inflight.next().await {
                apply_completion(&mut state, result);
            }
            continue;
        }

        // 0 < n_inflight < cap: race SQ vs CQ via futures::future::select.
        // Neither future preserves state across iterations (SQ is a channel
        // poll, CQ is FU::next which internally preserves the FU state
        // regardless of the temporary wrapper), so rebuilding them per
        // iteration is safe.
        let submit_fut = submit_rx.next();
        let cfut = inflight.next();
        futures::pin_mut!(submit_fut);
        match select(submit_fut, Box::pin(cfut)).await {
            Either::Left((maybe_msg, _cfut_dropped)) => {
                let should_break = match maybe_msg {
                    None => true,
                    Some(msg) => apply_stream_submit_msg(
                        &mut state,
                        &pool,
                        &mut inflight,
                        msg,
                        stream_id,
                        append_timeout,
                    )
                    .await
                    .is_break(),
                };
                if should_break {
                    // Drain remaining inflight before exit so callers get a
                    // final ack (success or connection-closed err).
                    while let Some(result) = inflight.next().await {
                        apply_completion(&mut state, result);
                    }
                    break;
                }
            }
            Either::Right((maybe_result, _submit_fut_dropped)) => {
                if let Some(result) = maybe_result {
                    apply_completion(&mut state, result);
                }
            }
        }
    }

    // Explicit keep-alive so the compiler doesn't move/drop the guard early.
    drop(removal_guard);
    tracing::debug!(stream_id, "stream worker exiting");
}

fn apply_completion(state: &mut StreamAppendState, result: InflightResult) {
    let InflightResult {
        offset,
        end,
        extent_id,
        frames,
        replica_node_ids,
        ack_tx,
    } = result;

    let size = end - offset;

    let mut success_first: Option<AppendResp> = None;
    let mut saw_not_found = false;
    let mut saw_locked_by_other = false;
    let mut err_msg: Option<String> = None;
    // F190: index of the first replica that produced a hard error
    // (rpc/decode/non-OK code/NotFound). LockedByOther is intentionally
    // NOT recorded — it's a control-plane fence event, not a node-health
    // signal. Resolved to a `node_id` after the loop via `replica_node_ids`.
    let mut bad_replica_idx: Option<usize> = None;

    for (i, frame_res) in frames.into_iter().enumerate() {
        match frame_res {
            Err(e) => {
                err_msg = Some(format!("replica {i} rpc error: {e}"));
                bad_replica_idx = Some(i);
                break;
            }
            Ok(frame) => {
                // F260 chaos root-cause (coco arch P1): NEVER decode an
                // ERROR frame's payload as AppendResp — error payloads are
                // [status_code][message], and StatusCode::Unavailable (5)
                // collides with CODE_LOCKED_BY_OTHER (5), so a generic
                // chain/transport error frame masqueraded as a fencing
                // event and POISONED the partition (the observed PS wedge
                // when a mid-chain EN died). Route error frames through
                // the soft-error path instead.
                if frame.is_error() {
                    err_msg = Some(format!(
                        "replica {i} error frame: {}",
                        String::from_utf8_lossy(&frame.payload[1.min(frame.payload.len())..])
                    ));
                    bad_replica_idx = Some(i);
                    break;
                }
                let payload = frame.payload;
                let resp = match AppendResp::decode(payload) {
                    Ok(r) => r,
                    Err(e) => {
                        err_msg = Some(format!("replica {i} decode AppendResp: {e}"));
                        bad_replica_idx = Some(i);
                        break;
                    }
                };
                if resp.code == CODE_NOT_FOUND {
                    saw_not_found = true;
                    bad_replica_idx = Some(i);
                    break;
                }
                if resp.code == CODE_LOCKED_BY_OTHER {
                    saw_locked_by_other = true;
                    break;
                }
                if resp.code != CODE_OK {
                    err_msg = Some(format!(
                        "replica {i} append failed: code={}",
                        crate::extent_rpc::code_description(resp.code)
                    ));
                    bad_replica_idx = Some(i);
                    break;
                }
                match &success_first {
                    None => success_first = Some(resp),
                    Some(first) => {
                        if resp.offset != first.offset || resp.end != first.end {
                            err_msg =
                                Some(format!("replica {i} offset mismatch on extent {extent_id}"));
                            bad_replica_idx = Some(i);
                            break;
                        }
                    }
                }
            }
        }
    }

    if saw_locked_by_other {
        state.rewind_or_poison(offset, size);
        let _ = ack_tx.send(Err(anyhow!(
            "LockedByOther: a newer owner holds extent {extent_id}"
        )));
        return;
    }

    if saw_not_found {
        // F190: the writer's tail cache is stale (the extent has been
        // moved/sealed/reclaimed on this replica). The replica itself
        // may still be healthy, but allocating around it for one TTL
        // window is harmless and routes around persistent inconsistency.
        if let Some(idx) = bad_replica_idx {
            if let Some(&nid) = replica_node_ids.get(idx) {
                state.mark_bad_node(nid);
                // F192: NotFound is stale-cache, not a node-health
                // signal — do NOT report it. The manager's quorum
                // would otherwise misclassify it as a real failure.
            }
        }
        state.rewind_or_poison(offset, size);
        let _ = ack_tx.send(Err(anyhow!(
            "extent {extent_id} not found on replica (needs alloc_new_extent)"
        )));
        return;
    }

    if let Some(err) = err_msg {
        // F190: real per-replica failure (rpc/decode/non-OK code) — mark
        // the node so the next alloc skips it.
        // F192: same failure pushes a manager-side report so the
        // global view catches up to the per-stream truth without
        // waiting for the next `node_health_loop` df tick (F222: 2 s;
        // was the 10 s `disk_status_update_loop`).
        if let Some(idx) = bad_replica_idx {
            if let Some(&nid) = replica_node_ids.get(idx) {
                state.mark_bad_node(nid);
                state.try_report_failure(nid, extent_id);
            }
        }
        state.rewind_or_poison(offset, size);
        let _ = ack_tx.send(Err(anyhow!(err)));
        return;
    }

    let appended = success_first.expect("success path implies Some");
    // ENOSPC-1 P1: the caller ack is DEFERRED until the contiguous prefix
    // covers this range — `ack` fires it (or fails it past a hole).
    state.ack(
        offset,
        end,
        Some((
            ack_tx,
            AppendResult {
                extent_id,
                offset: appended.offset,
                end: appended.end,
            },
        )),
    );
}

async fn launch_append(
    state: &mut StreamAppendState,
    pool: &Rc<ConnPool>,
    inflight: &mut FuturesUnordered<InflightFut>,
    payload_parts: Vec<Bytes>,
    owner_epoch: i64,
    ack_tx: oneshot::Sender<Result<AppendResult>>,
    // F195: F121 per-replica deadline. Passed by the worker loop from
    // its `config.append_fanout_timeout` snapshot.
    append_timeout: Duration,
) {
    let tail = match &state.tail {
        Some(t) => t.clone(),
        None => {
            let _ = ack_tx.send(Err(anyhow!(
                "stream worker: no tail set (public API must send ResetTail before first Append)"
            )));
            return;
        }
    };

    if state.poisoned {
        let _ = ack_tx.send(Err(anyhow!(
            "stream poisoned by prior failure; caller should alloc a new extent"
        )));
        return;
    }

    if state.sealing {
        // A failover SealCommit has captured this tail's final commit and the
        // manager is sealing it; appending more here would write past the new
        // sealed_length. Reject as a soft error so the caller retries — by then
        // ResetTail has switched to the fresh tail (sealing cleared).
        let _ = ack_tx.send(Err(anyhow!(
            "stream tail sealing for failover; retry on fresh tail"
        )));
        return;
    }

    let size: u64 = payload_parts.iter().map(|p| p.len() as u64).sum();
    let (offset, end) = state.lease(size);
    let header_commit = offset; // Option A: lease-time cursor.

    let extent_id = tail.extent.extent_id;
    // F190: node_ids parallel to replica_addrs, captured here so the
    // future moves a Vec<u64> rather than borrowing tail across await.
    let replica_node_ids: Vec<u64> = tail.replica_node_ids.clone();
    let hdr = AppendReq::encode_header(extent_id, tail.extent.eversion, header_commit, owner_epoch);

    // F260 — chained replication for large appends: ONE wire copy to
    // replica[0], which forwards down the chain (extent_node.rs
    // MSG_APPEND_CHAIN). The single ack means EVERY hop wrote (the tail
    // acks first, aggregated hop by hop) — all-replica-ACK semantics
    // unchanged, so `state.commit` stays ground truth. Submit happens
    // HERE, synchronously, so per-extent submit order = lease order on
    // the head replica's socket (the head preserves it down the chain).
    // Any hop failure surfaces as one error frame -> apply_completion's
    // existing soft-error / seal-and-roll path. Timeout scales by chain
    // depth (the ack traverses every hop).
    let chain_min = append_chain_min_bytes();
    if chain_min > 0 && size >= chain_min as u64 && tail.replica_addrs.len() >= 2 {
        let head_addr = tail.replica_addrs[0].clone();
        let chain: Vec<String> = tail.replica_addrs[1..].to_vec();
        let mut parts = Vec::with_capacity(2 + payload_parts.len());
        parts.push(encode_chain_prefix(&chain));
        parts.push(hdr);
        for seg in &payload_parts {
            parts.push(seg.clone());
        }
        let rx_res = pool.send_vectored(&head_addr, MSG_APPEND_CHAIN, parts).await;
        // Chained acks traverse every hop with store-and-forward latency —
        // budget generously (validated: deep 8M queues stack hop latencies).
        let chain_timeout = append_timeout * (tail.replica_addrs.len() as u32) * 3;
        let fut = async move {
            let res = match rx_res {
                Err(e) => Err(anyhow!("{} chain submit error: {}", head_addr, e)),
                Ok(rx) => {
                    let timer = compio::time::sleep(chain_timeout);
                    futures::pin_mut!(rx, timer);
                    match futures::future::select(rx, timer).await {
                        futures::future::Either::Left((Ok(frame), _)) => Ok(frame),
                        futures::future::Either::Left((Err(_), _)) => {
                            Err(anyhow!("{} connection closed", head_addr))
                        }
                        futures::future::Either::Right(_) => Err(anyhow!(
                            "{} chain append timeout after {:?}",
                            head_addr,
                            chain_timeout
                        )),
                    }
                }
            };
            InflightResult {
                offset,
                end,
                extent_id,
                frames: vec![res],
                replica_node_ids,
                ack_tx,
            }
        };
        inflight.push(Box::pin(fut));
        return;
    }

    // Fire send_vectored to each replica IN PARALLEL (F099-B). Each
    // RpcClient's writer_task is single-writer (R4 step 4.1), so per-
    // replica TCP byte order is still determined by the order this
    // worker's submits land on each replica's submit_tx — and since
    // this worker is single-task, submits into a given submit_tx
    // happen in lease order. Firing the 3 submits concurrently only
    // lets each replica's submit channel progress without waiting on
    // the others (they are independent); it does NOT interleave bytes
    // on any one replica's socket, so the commit-truncation invariant
    // (header.commit = offset must equal that replica's file_len on
    // arrival) is preserved.
    //
    // Preserve the "all 3 slots filled with Result" shape so
    // apply_completion's error handling (first-err-wins) is unchanged
    // from the pre-parallel version: use join_all, NOT try_join_all.
    let send_futs = tail.replica_addrs.iter().map(|addr| {
        let addr = addr.clone();
        let mut parts = Vec::with_capacity(1 + payload_parts.len());
        parts.push(hdr.clone());
        for seg in &payload_parts {
            parts.push(seg.clone());
        }
        let pool = pool.clone();
        async move {
            let rx_res = pool.send_vectored(&addr, MSG_APPEND, parts).await;
            (addr, rx_res)
        }
    });
    let receivers: Vec<(String, Result<oneshot::Receiver<autumn_rpc::Frame>>)> =
        join_all(send_futs).await;

    // F121: bound each replica's recv at `append_fanout_timeout`. A
    // half-open socket whose SubmitMsg landed in the writer_task before
    // RpcClient.closed flipped will otherwise hang join_all forever
    // (the response can never arrive — peer is dead). Translating
    // Elapsed into a regular `replica N rpc error: ... timeout` makes
    // `apply_completion` classify it as a soft error, which the
    // public-API retry loop already escalates to alloc_new_extent.
    let timeout = append_timeout;
    let fut = async move {
        let wait_futs = receivers.into_iter().map(|(addr, rx_res)| async move {
            match rx_res {
                Err(e) => Err(anyhow!("{} submit error: {}", addr, e)),
                Ok(rx) => {
                    let recv_fut = rx;
                    let timer = compio::time::sleep(timeout);
                    futures::pin_mut!(recv_fut, timer);
                    match futures::future::select(recv_fut, timer).await {
                        futures::future::Either::Left((Ok(frame), _)) => Ok(frame),
                        futures::future::Either::Left((Err(_), _)) => {
                            Err(anyhow!("{} connection closed", addr))
                        }
                        futures::future::Either::Right(_) => {
                            Err(anyhow!("{} append timeout after {:?}", addr, timeout))
                        }
                    }
                }
            }
        });
        let frames = join_all(wait_futs).await;
        InflightResult {
            offset,
            end,
            extent_id,
            frames,
            replica_node_ids,
            ack_tx,
        }
    };
    inflight.push(Box::pin(fut));
}

/// A lock-free StreamClient where operations on different stream_ids
/// never block each other.  No external Mutex is required.
///
/// Construction returns `Rc<Self>` (via `Rc::new_cyclic`) so the internal
/// per-stream workers can hold a `Weak<Self>` for the removal-guard
/// without forming an Rc cycle.  Callers that previously wrote
/// `let sc = StreamClient::connect(...)` get `Rc<StreamClient>`; method
/// calls `sc.append(...)` still work via `Deref`.
pub struct StreamClient {
    /// Weak self-reference — used by per-stream workers to clean up on
    /// exit. Set exactly once by `Rc::new_cyclic`.
    self_weak: Weak<StreamClient>,
    /// Manager addresses for round-robin failover on NotLeader.
    manager_addrs: Vec<String>,
    /// Current manager index (round-robin on NotLeader).
    current_mgr: Cell<usize>,
    owner_key: String,
    owner_epoch: i64,
    max_extent_size: u64,
    /// Shared connection pool — one RpcClient per remote address, with
    /// heartbeat health checks for extent nodes.
    pool: Rc<ConnPool>,
    /// Node-id → (address, shard_ports) map (refreshed on miss). F099-M:
    /// `shard_ports` is used to route hot-path RPCs by `extent_id % K`.
    /// Empty `shard_ports` means legacy single-thread extent-node.
    nodes_cache: DashMap<u64, (String, Vec<u16>)>,
    /// Cached ExtentInfo for read path.
    extent_info_cache: DashMap<u64, ExtentInfo>,
    /// R4 4.3: per-stream single-owner worker sender.  Spawned lazily on
    /// first append* to a given stream_id.  Replaces the R3 Mutex-guarded
    /// `stream_states` DashMap — all per-stream state now lives inside
    /// the worker task.
    stream_workers: RefCell<HashMap<u64, mpsc::Sender<StreamSubmitMsg>>>,
    /// Serialises the tail-load + ResetTail for concurrent first-callers
    /// to the same stream (per-stream init lock).  After the first init,
    /// subsequent callers observe `*guard == true` and skip.
    stream_init_locks: RefCell<HashMap<u64, Rc<futures::lock::Mutex<bool>>>>,
    /// F190: per-stream "recently failed" node ids (`node_id → expires_at`).
    /// Shared between the per-stream worker (writes on `apply_completion`
    /// Err) and the public-API `alloc_new_extent_once` (reads + prunes
    /// before each alloc). Persists across worker respawn — a node that
    /// just failed should still be excluded if a transient worker exit +
    /// respawn happens within the TTL window. Pruned lazily on snapshot;
    /// no background sweeper.
    stream_bad_nodes: RefCell<HashMap<u64, Rc<RefCell<HashMap<u64, Instant>>>>>,
    /// F192: fire-and-forget reporter for `MSG_REPORT_DISK_FAILURE`.
    /// Worker (`apply_completion` Err path) pushes here via `try_send`
    /// (drop on full — pure best-effort). The drainer task (spawned in
    /// `construct`) holds the `Receiver`, reads each report, builds a
    /// `ReportDiskFailureReq` with the current `reporter_part_id`, and
    /// sends to the manager. The channel is bounded so a misbehaving
    /// peer can't OOM us; F190's per-stream alloc route-around remains
    /// the primary defense, so dropped reports don't hurt correctness.
    failure_report_tx: mpsc::Sender<FailureReport>,
    /// F192: identifier the manager dedups by inside its quorum
    /// debounce window. Each `PartitionData` sets this to its own
    /// `part_id` after `StreamClient::new_with_owner_epoch`. Default 0
    /// means "no reporter id configured" — drainer skips sending to
    /// avoid polluting the manager's quorum count with a sentinel.
    reporter_part_id: Cell<u64>,
    /// F195: stream tunables. Defaults match pre-F195 env defaults.
    /// Cloned into per-stream workers at spawn time (`Rc` keeps the
    /// clone cheap).
    config: Rc<StreamClientConfig>,
    append_metrics: StreamAppendMetrics,
    /// F276: lazily-refreshed snapshot of manager-`Suspected` node ids. The
    /// READ path consults it to route around a flaky node proactively (no
    /// per-read RPC timeout) and to reconstruct EC shards from parity instead
    /// of reading a suspected shard. Soft hint only — see `SuspectedCache`.
    suspected: Rc<RefCell<SuspectedCache>>,
}

/// F192: payload of one failure-observation event passed from a
/// per-stream worker to the per-StreamClient drainer task.
#[derive(Clone, Copy, Debug)]
struct FailureReport {
    node_id: u64,
    extent_id: u64,
}

/// F276: client-side snapshot of the manager's `Suspected` node set, consumed
/// by the READ path. Refreshed lazily + NON-BLOCKING off the read path
/// (`maybe_refresh_suspected`): the read never waits on a manager RTT, it just
/// uses the latest snapshot (stale by at most `SUSPECTED_REFRESH_TTL`). This is
/// a pure soft hint — correctness NEVER depends on it (suspected replicas are
/// deprioritized, not excluded; EC reads reconstruct from parity). So a stale
/// or empty snapshot only costs latency on a genuinely-flaky node, never data.
#[derive(Default)]
struct SuspectedCache {
    nodes: HashSet<u64>,
    last_refresh: Option<Instant>,
    refreshing: bool,
}

/// F276: how stale the Suspected snapshot may get before the read path kicks
/// off a background refresh. 2 s matches the manager's df probe cadence; the
/// `Suspected` soft-timeout is ~10 s, so a 2 s-stale view is plenty fresh.
const SUSPECTED_REFRESH_TTL: Duration = Duration::from_secs(2);

impl StreamClient {
    /// Current manager address (round-robin index).
    fn manager_addr(&self) -> &str {
        &self.manager_addrs[self.current_mgr.get() % self.manager_addrs.len()]
    }

    /// Rotate to the next manager address (round-robin).
    fn rotate_manager(&self) {
        let next = (self.current_mgr.get() + 1) % self.manager_addrs.len();
        self.current_mgr.set(next);
    }

    /// F267: one manager RPC with rotate-on-transport-failure. Every
    /// StreamClient manager call MUST route through this (or through
    /// `retry_manager_call`, which rotates itself) — a raw
    /// `pool.call_timeout(self.manager_addr(), ..)` pins every
    /// CALLER-side retry loop (partition open's 5 s commit_length loop,
    /// GC cooldown retries, read-path cache refreshes) to a dead manager
    /// forever. Observed (manager-HA chaos H2): with the old leader
    /// killed and a healthy standby leading, a PS opening a migrated
    /// partition hammered the dead address every 5 s for 20+ minutes.
    async fn manager_call(
        &self,
        msg_type: u8,
        req: Bytes,
        timeout: Duration,
    ) -> Result<Bytes> {
        match self
            .pool
            .call_timeout(self.manager_addr(), msg_type, req, timeout)
            .await
        {
            Ok(b) => Ok(b),
            Err(e) => {
                self.rotate_manager();
                Err(e)
            }
        }
    }

    /// F267 companion: rotate away from a manager that ANSWERED but is
    /// no longer the leader (alive-but-deposed after an HA failover) —
    /// the symmetric wedge to the dead-address case above.
    fn note_manager_code(&self, code: u8) {
        if code == CODE_NOT_LEADER {
            self.rotate_manager();
        }
    }

    /// Shared tail of the manager-RPC wrappers: note the response `code`
    /// (rotating to the next manager on NOT_LEADER) and bail with
    /// `"<ctx> failed: <message>"` when it isn't OK.
    fn check_manager_resp(&self, code: u8, message: &str, ctx: &str) -> Result<()> {
        self.note_manager_code(code);
        if code != CODE_OK {
            return Err(anyhow!("{ctx} failed: {message}"));
        }
        Ok(())
    }

    async fn retry_manager_call<F, Fut, T>(&self, label: &str, max_retries: u32, f: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut attempt = 0u32;
        loop {
            let addr = self.manager_addr().to_string();
            match f().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    attempt += 1;
                    if attempt > max_retries {
                        return Err(
                            e.context(format!("{label} failed after {max_retries} retries"))
                        );
                    }
                    self.rotate_manager();
                    tracing::warn!(
                        attempt,
                        max_retries,
                        manager = %addr,
                        error = %e,
                        "{} failed, retrying in 500ms (next: {})",
                        label,
                        self.manager_addr(),
                    );
                    compio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    /// Connect with the default `StreamClientConfig`. Equivalent to
    /// `connect_with_config(..., StreamClientConfig::default())`.
    pub async fn connect(
        manager_endpoint: &str,
        owner_key: String,
        max_extent_size: u64,
        pool: Rc<ConnPool>,
    ) -> Result<Rc<Self>> {
        Self::connect_with_config(
            manager_endpoint,
            owner_key,
            max_extent_size,
            pool,
            StreamClientConfig::default(),
        )
        .await
    }

    /// F195: connect with explicit tunables.
    pub async fn connect_with_config(
        manager_endpoint: &str,
        owner_key: String,
        max_extent_size: u64,
        pool: Rc<ConnPool>,
        config: StreamClientConfig,
    ) -> Result<Rc<Self>> {
        let mgr_addrs: Vec<String> = manager_endpoint
            .split(',')
            .map(|s| crate::conn_pool::normalize_endpoint(s.trim()))
            .collect();
        let req = manager_rpc::rkyv_encode(&AcquireOwnerLockReq {
            owner_key: owner_key.clone(),
        });
        let mut last_err = None;
        let mut connected_idx = 0usize;
        let mut owner_epoch = 0i64;
        let mut ok = false;
        for (idx, addr) in mgr_addrs.iter().enumerate() {
            // 10 s — owner-lock acquisition is one etcd CAS on the
            // manager side. Bounded so a deposed/hanging manager
            // doesn't trap a fresh PS startup; the loop walks to the
            // next manager address on timeout.
            match pool
                .call_timeout(
                    addr,
                    MSG_ACQUIRE_OWNER_LOCK,
                    req.clone(),
                    Duration::from_secs(10),
                )
                .await
            {
                Ok(resp_data) => {
                    let resp: AcquireOwnerLockResp =
                        manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
                    if resp.code == CODE_OK {
                        connected_idx = idx;
                        owner_epoch = resp.owner_epoch;
                        ok = true;
                        break;
                    } else if resp.code == CODE_NOT_LEADER {
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
        if !ok {
            return Err(last_err.unwrap_or_else(|| anyhow!("no manager available")));
        }
        Ok(Self::construct(
            mgr_addrs,
            connected_idx,
            owner_key,
            owner_epoch,
            max_extent_size,
            pool,
            config,
        ))
    }

    /// Create a StreamClient that reuses an existing owner-lock owner_epoch
    /// without calling `acquire_owner_lock` again. Accepts comma-separated
    /// manager endpoints. Uses `StreamClientConfig::default()`.
    pub async fn new_with_owner_epoch(
        manager_endpoint: &str,
        owner_key: String,
        owner_epoch: i64,
        max_extent_size: u64,
        pool: Rc<ConnPool>,
    ) -> Result<Rc<Self>> {
        Self::new_with_owner_epoch_and_config(
            manager_endpoint,
            owner_key,
            owner_epoch,
            max_extent_size,
            pool,
            StreamClientConfig::default(),
        )
        .await
    }

    /// F195: as `new_with_owner_epoch` but with explicit tunables.
    pub async fn new_with_owner_epoch_and_config(
        manager_endpoint: &str,
        owner_key: String,
        owner_epoch: i64,
        max_extent_size: u64,
        pool: Rc<ConnPool>,
        config: StreamClientConfig,
    ) -> Result<Rc<Self>> {
        let mgr_addrs: Vec<String> = manager_endpoint
            .split(',')
            .map(|s| crate::conn_pool::normalize_endpoint(s.trim()))
            .collect();
        Ok(Self::construct(
            mgr_addrs,
            0,
            owner_key,
            owner_epoch,
            max_extent_size,
            pool,
            config,
        ))
    }

    /// Private ctor: `Rc::new_cyclic` captures a weak self-ref for the
    /// per-stream workers' removal guard and for the F192 failure-report
    /// drainer task. F195: `config` carries the (pre-F195 env-default-
    /// equivalent) tunables; pass `StreamClientConfig::default()` to
    /// keep historical behavior.
    fn construct(
        manager_addrs: Vec<String>,
        current_mgr: usize,
        owner_key: String,
        owner_epoch: i64,
        max_extent_size: u64,
        pool: Rc<ConnPool>,
        config: StreamClientConfig,
    ) -> Rc<Self> {
        let config = Rc::new(config);
        // F192: bounded channel — drop reports on overflow rather than
        // OOMing the writer. F190's per-stream alloc route-around is
        // the primary defense; reports are pure advisory.
        let (failure_report_tx, failure_report_rx) = mpsc::channel::<FailureReport>(1024);
        let rc = Rc::new_cyclic(|weak| Self {
            self_weak: weak.clone(),
            manager_addrs,
            current_mgr: Cell::new(current_mgr),
            owner_key,
            owner_epoch,
            max_extent_size,
            pool,
            nodes_cache: DashMap::new(),
            extent_info_cache: DashMap::new(),
            stream_workers: RefCell::new(HashMap::new()),
            stream_init_locks: RefCell::new(HashMap::new()),
            stream_bad_nodes: RefCell::new(HashMap::new()),
            failure_report_tx,
            reporter_part_id: Cell::new(0),
            config,
            append_metrics: StreamAppendMetrics::default(),
            suspected: Rc::new(RefCell::new(SuspectedCache::default())),
        });
        // F192: spawn the drainer task on the current compio runtime.
        // The Weak<Self> exits the loop when StreamClient is dropped.
        let weak = Rc::downgrade(&rc);
        compio::runtime::spawn(failure_report_drain_loop(weak, failure_report_rx)).detach();
        rc
    }

    /// F192: set the partition id that the manager-side quorum debounce
    /// dedups by. Each `PartitionData` calls this once after
    /// `new_with_owner_epoch`. Leaving it at 0 disables the F192 send path
    /// (the drainer skips events with reporter=0) — safe for tests and
    /// for the rare server-level `StreamClient` that doesn't belong to
    /// a partition.
    pub fn set_reporter_part_id(&self, part_id: u64) {
        self.reporter_part_id.set(part_id);
    }

    pub fn owner_epoch(&self) -> i64 {
        self.owner_epoch
    }
    pub fn owner_key(&self) -> &str {
        &self.owner_key
    }

    // ── internal helpers ─────────────────────────────────────────────────────

    /// Returns `true` if the heartbeat monitor has seen a recent echo from the
    /// extent node at `addr` (within the last 8 s).
    pub fn is_extent_healthy(&self, addr: &str) -> bool {
        self.pool.is_healthy(addr)
    }

    async fn refresh_nodes_map(&self) -> Result<()> {
        // 5 s — read-only manager call, all in-memory state.
        let resp_data = self
            .pool
            .call_timeout(
                self.manager_addr(),
                MSG_NODES_INFO,
                Bytes::new(),
                Duration::from_secs(5),
            )
            .await?;
        let resp: NodesInfoResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        self.check_manager_resp(resp.code, &resp.message, "nodes_info")?;
        for (id, node) in resp.nodes {
            self.nodes_cache
                .insert(id, (node.address, node.shard_ports));
        }
        Ok(())
    }

    /// F276: cheap, synchronous "does the manager currently believe this node
    /// Suspected?" read off the cached snapshot. Same-thread `RefCell` borrow,
    /// no await, no alloc.
    fn is_node_suspected(&self, node_id: u64) -> bool {
        self.suspected.borrow().nodes.contains(&node_id)
    }

    /// F276: kick a NON-BLOCKING refresh of the Suspected snapshot if it's
    /// older than `SUSPECTED_REFRESH_TTL` and none is already in flight. The
    /// caller (read path) does NOT await this — it spawns a detached task that
    /// polls `MSG_LIST_NODE_STATES` and swaps the snapshot in. The current read
    /// proceeds on the existing (slightly stale) snapshot. Idle StreamClients
    /// never poll: the refresh only fires while reads are actually happening.
    ///
    /// CONSEQUENCE (by design): the avoidance is STEADY-STATE / self-healing,
    /// not a per-read guarantee. The FIRST read after a node flips to Suspected
    /// (e.g. on a previously-idle client whose snapshot is still empty) only
    /// *kicks* this refresh — it uses the old snapshot and can still pay one
    /// timeout if it lands on the flaky node. Every read after the ~2 s refresh
    /// routes around it. We accept this over a synchronous first-refresh (which
    /// would put a manager RTT on the read's critical path) or idle background
    /// polling (the per-partition manager traffic we deliberately avoid). It
    /// never regresses the pre-F276 reactive failover.
    fn maybe_refresh_suspected(&self) {
        {
            let c = self.suspected.borrow();
            if c.refreshing {
                return;
            }
            if let Some(t) = c.last_refresh {
                if t.elapsed() < SUSPECTED_REFRESH_TTL {
                    return;
                }
            }
        }
        let Some(sc) = self.self_weak.upgrade() else {
            return;
        };
        sc.suspected.borrow_mut().refreshing = true;
        compio::runtime::spawn(async move {
            let res = sc.fetch_suspected_nodes().await;
            let mut c = sc.suspected.borrow_mut();
            c.refreshing = false;
            c.last_refresh = Some(Instant::now());
            // On error keep the previous snapshot — a failed poll must never
            // widen the avoidance set (that could strand reads); stale is fine.
            if let Ok(set) = res {
                c.nodes = set;
            }
        })
        .detach();
    }

    /// F276: one `MSG_LIST_NODE_STATES` poll → the set of node ids the manager
    /// currently marks `Suspected`. Rotates managers on transport / NotLeader
    /// failure like every other StreamClient manager call.
    async fn fetch_suspected_nodes(&self) -> Result<HashSet<u64>> {
        // 5 s — read-only manager call over in-memory state (matches
        // refresh_nodes_map). The handler ignores the request payload.
        let resp_data = self
            .manager_call(MSG_LIST_NODE_STATES, Bytes::new(), Duration::from_secs(5))
            .await?;
        let resp: ListNodeStatesResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        self.check_manager_resp(resp.code, &resp.message, "list_node_states")?;
        Ok(resp
            .nodes
            .iter()
            .filter(|n| n.auto_state == NODE_AUTO_STATE_SUSPECTED)
            .map(|n| n.node_id)
            .collect())
    }

    fn replica_addrs_from_cache(&self, ex: &ExtentInfo) -> Result<Vec<String>> {
        let mut addrs = Vec::with_capacity(ex.replicates.len() + ex.parity.len());
        for node_id in ex.replicates.iter().chain(ex.parity.iter()) {
            let entry = self
                .nodes_cache
                .get(node_id)
                .ok_or_else(|| anyhow!("node {} missing", node_id))?;
            let (addr, shard_ports) = entry.value();
            // F099-M: route this extent to the owning shard port.
            addrs.push(crate::conn_pool::shard_addr_for_extent(
                addr,
                shard_ports,
                ex.extent_id,
            ));
        }
        if addrs.is_empty() {
            return Err(anyhow!("extent {} has no replicas", ex.extent_id));
        }
        Ok(addrs)
    }

    async fn replica_addrs_for_extent(&self, ex: &ExtentInfo) -> Result<Vec<String>> {
        if self.nodes_cache.is_empty() {
            self.refresh_nodes_map().await?;
        }
        if let Ok(addrs) = self.replica_addrs_from_cache(ex) {
            return Ok(addrs);
        }
        self.refresh_nodes_map().await?;
        self.replica_addrs_from_cache(ex)
    }

    /// Per-stream init lock.  Used to serialise tail-load + ResetTail when
    /// multiple public-API callers race to first-initialise the same stream.
    fn stream_init_lock(&self, stream_id: u64) -> Rc<futures::lock::Mutex<bool>> {
        if let Some(l) = self.stream_init_locks.borrow().get(&stream_id) {
            return l.clone();
        }
        let l = Rc::new(futures::lock::Mutex::new(false));
        self.stream_init_locks
            .borrow_mut()
            .insert(stream_id, l.clone());
        l
    }

    /// Get or spawn the per-stream worker, returning a cloned Sender.
    fn stream_worker_sender(&self, stream_id: u64) -> mpsc::Sender<StreamSubmitMsg> {
        if let Some(tx) = self.stream_workers.borrow().get(&stream_id) {
            return tx.clone();
        }
        let (tx, rx) = mpsc::channel::<StreamSubmitMsg>(STREAM_SUBMIT_CAP);
        self.stream_workers
            .borrow_mut()
            .insert(stream_id, tx.clone());
        let pool = self.pool.clone();
        let bad_nodes = self.stream_bad_nodes_handle(stream_id);
        // F192: clone the failure-report sender into the worker so
        // `apply_completion` Err can fire reports without an extra
        // hop through the StreamClient.
        let failure_report_tx = self.failure_report_tx.clone();
        // F195: hand the worker its tunables. `Rc<StreamClientConfig>`
        // clone is O(1) ref-count bump.
        let config = self.config.clone();
        let guard = WorkerRemovalGuard {
            sc: self.self_weak.clone(),
            stream_id,
        };
        compio::runtime::spawn(async move {
            stream_worker_loop(
                stream_id,
                rx,
                pool,
                bad_nodes,
                failure_report_tx,
                config,
                guard,
            )
            .await;
        })
        .detach();
        tx
    }

    /// F190: get or create the per-stream `bad_nodes` Rc handle.
    /// Persisted across worker respawn so a node that just failed stays
    /// excluded for one TTL window even if the worker briefly exits.
    fn stream_bad_nodes_handle(&self, stream_id: u64) -> Rc<RefCell<HashMap<u64, Instant>>> {
        if let Some(rc) = self.stream_bad_nodes.borrow().get(&stream_id) {
            return rc.clone();
        }
        let rc = Rc::new(RefCell::new(HashMap::new()));
        self.stream_bad_nodes
            .borrow_mut()
            .insert(stream_id, rc.clone());
        rc
    }

    /// F190: snapshot of currently-active (non-expired) bad-node ids for
    /// `stream_id`. Lazily prunes expired entries during the read so the
    /// map can never grow without bound. Returns empty Vec if no entry
    /// exists (which is also the legacy / cold-start behaviour).
    fn snapshot_bad_nodes(&self, stream_id: u64) -> Vec<u64> {
        let map = self.stream_bad_nodes.borrow();
        let Some(rc) = map.get(&stream_id) else {
            return Vec::new();
        };
        let now = Instant::now();
        let mut entries = rc.borrow_mut();
        entries.retain(|_, expires_at| *expires_at > now);
        entries.keys().copied().collect()
    }

    /// Discard the cached worker and init-lock for `stream_id` so the next
    /// append spawns a fresh worker that re-loads the tail from the manager.
    /// Used after partition split: the manager sealed the old tail, but the
    /// existing worker still targets it.  Without invalidation the worker
    /// keeps appending beyond `sealed_length`, and recovery (which reads up
    /// to `sealed_length`) silently loses that data.
    pub fn invalidate_stream(&self, stream_id: u64) {
        self.stream_workers.borrow_mut().remove(&stream_id);
        self.stream_init_locks.borrow_mut().remove(&stream_id);
    }

    async fn load_stream_tail(&self, stream_id: u64) -> Result<StreamTail> {
        let req = manager_rpc::rkyv_encode(&StreamInfoReq {
            stream_ids: vec![stream_id],
        });
        // 5 s — read-only manager call, in-memory state.
        let resp_data = self
            .manager_call(MSG_STREAM_INFO, req, Duration::from_secs(5))
            .await?;
        let resp: StreamInfoResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        self.check_manager_resp(resp.code, &resp.message, "stream_info")?;
        let (_, mgr_stream) = resp
            .streams
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("stream {} not found", stream_id))?;
        let tail_eid = *mgr_stream
            .extent_ids
            .last()
            .ok_or_else(|| anyhow!("stream {} has no extents", stream_id))?;

        let mgr_extent = resp
            .extents
            .into_iter()
            .find(|(id, _)| *id == tail_eid)
            .map(|(_, e)| e)
            .ok_or_else(|| anyhow!("tail extent {} not in response", tail_eid))?;

        let extent = Self::mgr_to_extent_info(&mgr_extent);
        self.extent_info_cache
            .insert(extent.extent_id, extent.clone());

        self.refresh_nodes_map().await?;
        let addrs = self.replica_addrs_from_cache(&extent)?;
        let node_ids = Self::replica_node_ids_for(&extent);

        Ok(StreamTail {
            extent,
            replica_addrs: addrs,
            replica_node_ids: node_ids,
        })
    }

    /// F190: chained `replicates ++ parity` node ids — same index order as
    /// `replica_addrs_from_cache`. Used to populate `StreamTail.replica_node_ids`.
    fn replica_node_ids_for(ex: &ExtentInfo) -> Vec<u64> {
        ex.replicates
            .iter()
            .chain(ex.parity.iter())
            .copied()
            .collect()
    }

    async fn check_commit(&self, stream_id: u64) -> Result<(StreamInfo, ExtentInfo, u64)> {
        let req = manager_rpc::rkyv_encode(&CheckCommitLengthReq {
            stream_id,
            owner_key: self.owner_key.clone(),
            owner_epoch: self.owner_epoch,
        });
        // 15 s — manager fans out commit_length probes to every
        // replica of the tail extent before responding; each replica
        // call is itself bounded but the aggregate can take a few s.
        let resp_data = self
            .manager_call(MSG_CHECK_COMMIT_LENGTH, req, Duration::from_secs(15))
            .await?;
        let resp: CheckCommitLengthResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        self.check_manager_resp(resp.code, &resp.message, "check_commit_length")?;
        let stream = resp
            .stream_info
            .map(|s| Self::mgr_to_stream_info(&s))
            .ok_or_else(|| anyhow!("check_commit: missing stream_info"))?;
        let extent = resp
            .last_ex_info
            .map(|e| Self::mgr_to_extent_info(&e))
            .ok_or_else(|| anyhow!("check_commit: missing last_ex_info"))?;
        Ok((stream, extent, resp.end))
    }

    async fn alloc_new_extent_once(
        &self,
        stream_id: u64,
        seal_commit: Option<u64>,
        seal_extent_id: u64,
    ) -> Result<(StreamInfo, ExtentInfo)> {
        // F190: snapshot the per-stream `bad_nodes` set (lazily prunes
        // expired entries). The manager filters its candidate pool by
        // this set and only blocks allocation if doing so would empty
        // the pool — see `select_nodes` + `handle_stream_alloc_extent`.
        let exclude_node_ids = self.snapshot_bad_nodes(stream_id);
        let req = manager_rpc::rkyv_encode(&StreamAllocExtentReq {
            stream_id,
            owner_key: self.owner_key.clone(),
            owner_epoch: self.owner_epoch,
            seal_commit,
            exclude_node_ids,
            // BUG2-IDEMPOTENT-ROLL: pin the seal to the extent the commit was
            // captured for (0 = no specific target / probe). Reused verbatim
            // across retries, so a retried roll seals THAT extent (idempotent),
            // never the freshly-rolled new tail.
            seal_extent_id,
        });
        // 30 s — manager seals current tail (3-replica commit_length
        // probe + etcd mirror) and allocs a fresh extent on each new
        // replica node (alloc_extent_on_node bounded at 10 s each).
        let resp_data = self
            .pool
            .call_timeout(
                self.manager_addr(),
                MSG_STREAM_ALLOC_EXTENT,
                req,
                Duration::from_secs(30),
            )
            .await?;
        let resp: StreamAllocExtentResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        if resp.code != CODE_OK {
            return Err(anyhow!("stream_alloc_extent failed: {}", resp.message));
        }
        let stream = resp
            .stream_info
            .map(|s| Self::mgr_to_stream_info(&s))
            .ok_or_else(|| anyhow!("alloc_new_extent: missing stream_info"))?;
        let extent = resp
            .last_ex_info
            .map(|e| Self::mgr_to_extent_info(&e))
            .ok_or_else(|| anyhow!("alloc_new_extent: missing last_ex_info"))?;
        self.extent_info_cache
            .insert(extent.extent_id, extent.clone());
        // GC-RECLAIM: refresh the OLD tail's cache entry to its post-seal state.
        // `alloc_new_extent` caches only the NEW tail; pre-this the OLD tail
        // lingered as its pre-seal OPEN snapshot (`sealed=false, sealed_length=0`)
        // and GC's authoritative-sealed gate (background.rs
        // `authoritative_sealed_length`) SKIPS anything not-sealed — so the rolled
        // extent was never reclaimed until its cache happened to refresh (a read /
        // EC-invalidate / restart): the coco P1 GC-reclamation leak.
        //
        // We INVALIDATE the old tail's cache rather than SYNTHESIZE its sealed
        // length (coco P1, data-loss): we must NOT write `sealed_length = seal_
        // commit` locally, because the manager does NOT guarantee it sealed this
        // extent at exactly `seal_commit` — its `already_sealed` branch IGNORES
        // `req.seal_commit` and preserves the EXISTING `sealed_length L` (a prior
        // probe/split/merge seal; `L ≥ acked ≥ seal_commit`). Caching a too-small
        // `seal_commit < L` would make GC relocate only the first `seal_commit`
        // bytes yet punch the whole extent → lose committed `[seal_commit, L)`.
        // Invalidating forces GC's next `get_extent_info` to fetch the
        // AUTHORITATIVE sealed length from the manager. The extra cache-miss RPC
        // is safe now: the seed=603 wedge a GC-side refetch would once have
        // exposed is itself fixed (BUG2-IDEMPOTENT-ROLL), so the timing shift no
        // longer wedges anything; and it is one-shot (re-cached on the fetch).
        if seal_commit.is_some() && seal_extent_id != 0 {
            self.extent_info_cache.remove(&seal_extent_id);
        }
        Ok((stream, extent))
    }

    async fn alloc_new_extent(
        &self,
        stream_id: u64,
        seal_commit: Option<u64>,
        seal_extent_id: u64,
    ) -> Result<(StreamInfo, ExtentInfo)> {
        // BUG2-IDEMPOTENT-ROLL: `seal_extent_id` is captured ONCE (with
        // `seal_commit`) and reused across every retry inside
        // `retry_manager_call`, so a retried roll seals the original tail
        // (idempotent), never the freshly-rolled new tail.
        self.retry_manager_call("alloc_new_extent", 20, || {
            self.alloc_new_extent_once(stream_id, seal_commit, seal_extent_id)
        })
        .await
    }

    /// WAL self-heal A4: seal-and-roll the current OPEN tail of `stream_id` via
    /// the F227 manager probe (seal-over-reachable) and alloc a fresh tail.
    ///
    /// Used by recovery when replay finds the OPEN tail corrupt: an open extent
    /// has no `avali` to clear, so it can't be isolated in place. Sealing freezes
    /// it at the committed length (the acked prefix is on every committed member
    /// under all-replica-ACK, so `min`-over-reachable ≥ acked — no acked data
    /// lost; bytes beyond are un-acked speculation, correctly dropped) and turns
    /// it into a SEALED extent. This method ONLY performs the manager-side
    /// seal-and-roll; the caller decides what to do next. The recovery caller
    /// (`self_heal_replay_chunk`) invalidates the extent cache, re-fetches the
    /// now-sealed ExtentInfo, and runs the sealed cross-read on the same window in
    /// the same pass — isolating the bad replica via A5 without depending on a
    /// retried open.
    ///
    /// Does NOT touch the per-stream worker (recovery runs before it spawns) —
    /// it is a pure manager RPC + cache update. Fenced by `self.owner_epoch`
    /// (the recovering PS holds the partition owner lock). The new fresh tail is
    /// picked up by `ensure_tail_initialised` when the worker later spawns (the
    /// old tail now reports sealed → alloc fresh, the standard path).
    pub async fn seal_and_roll_tail(&self, stream_id: u64) -> Result<()> {
        // None seal_commit (probe) → seal_extent_id=0 (no specific target).
        self.alloc_new_extent(stream_id, None, 0).await?;
        Ok(())
    }

    /// SealCommit handshake — ask the per-stream worker for its TRUE
    /// all-replica-acked commit on the current tail, captured at a QUIESCED
    /// point (the worker drains every in-flight append first). This is the
    /// ONLY safe source for the failover seal length: a value tracked in the
    /// public API always lags the worker's `state.commit` and races concurrent
    /// out-of-order appends + tail rolls (→ phantom seal or acked-data
    /// truncation). The drain is bounded by each append's
    /// `append_fanout_timeout`, so it cannot hang. Returns the commit (may be
    /// 0 for a tail where nothing was ever all-acked); the caller passes it to
    /// `alloc_new_extent(.., Some(commit))` so the manager seals
    /// at EXACTLY this value without probing.
    /// Returns `(commit, tail_extent_id)` — `tail_extent_id` is the extent the
    /// drained `commit` belongs to (BUG2-IDEMPOTENT-ROLL). Pass it on to
    /// `alloc_new_extent` as `seal_extent_id` so a retried roll seals THAT
    /// extent (idempotent), not the freshly-rolled tail.
    async fn seal_commit_watermark(
        &self,
        stream_id: u64,
        tx: &mpsc::Sender<StreamSubmitMsg>,
    ) -> Result<(u64, u64)> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let mut tx_clone = tx.clone();
        tx_clone
            .send(StreamSubmitMsg::SealCommit { resp: resp_tx })
            .await
            .map_err(|_| anyhow!("stream {stream_id} worker gone before SealCommit"))?;
        resp_rx
            .await
            .map_err(|_| anyhow!("stream {stream_id} worker dropped SealCommit resp"))
    }

    /// Core append implementation.  Thin wrapper that wraps a single Bytes
    /// payload into the segments vec expected by the worker path.
    async fn append_payload(&self, stream_id: u64, payload: Bytes) -> Result<AppendResult> {
        self.append_payload_segments(stream_id, vec![payload]).await
    }

    /// R4 4.3 public-API retry loop.  The worker is a stateful single-op
    /// executor: it leases offsets, fires 3 replicas, and acks/rewinds on
    /// completion.  Retries (NotFound, soft errors, extent-full) live here.
    ///
    /// Steps per loop iteration:
    ///   1. Send Append msg to worker (parks on bounded channel under overload).
    ///   2. Await ack_rx.
    ///   3a. Ok  → if result.end >= max_extent_size, alloc + ResetTail;
    ///             return the AppendResult.
    ///   3b. Err "not found on replica" → alloc + ResetTail; retry.
    ///   3c. Err "LockedByOther" → return immediately (PS should self-evict).
    ///   3d. Err soft (retry ≤ 2) → sleep 100ms, reload tail, ResetTail; retry.
    ///   3e. Err hard → alloc + ResetTail; retry.
    ///
    /// Invariant: ResetTail is sent AFTER the previous ack lands, so the
    /// worker's in_flight is 0 at reset — no old-extent leases stranded on
    /// the new extent.
    async fn append_payload_segments(
        &self,
        stream_id: u64,
        segments: Vec<Bytes>,
    ) -> Result<AppendResult> {
        let payload_len_u32: u32 = segments.iter().map(|s| s.len() as u32).sum();
        let payload_len: usize = payload_len_u32 as usize;
        let append_started_at = Instant::now();

        let tx = self.stream_worker_sender(stream_id);

        // First-use tail init (serialised per stream).
        self.ensure_tail_initialised(stream_id, &tx).await?;

        let mut retry = 0usize;
        let mut alloc_count = 0u32;
        const MAX_ALLOC_PER_APPEND: u32 = 3;
        let mut fanout_elapsed = Duration::default();
        let lock_wait_total = Duration::default();
        let extent_lookup_elapsed = Duration::default();

        loop {
            let (ack_tx, ack_rx) = oneshot::channel();
            let msg = StreamSubmitMsg::Append {
                payload_parts: segments.clone(),
                owner_epoch: self.owner_epoch,
                ack_tx,
            };

            let fanout_started_at = Instant::now();
            {
                let mut tx_clone = tx.clone();
                tx_clone
                    .send(msg)
                    .await
                    .map_err(|_| anyhow!("stream {stream_id} worker gone"))?;
            }
            let ack = ack_rx
                .await
                .map_err(|_| anyhow!("stream {stream_id} worker dropped ack"))?;
            fanout_elapsed += fanout_started_at.elapsed();

            match ack {
                Ok(result) => {
                    let total_elapsed = append_started_at.elapsed();
                    tracing::debug!(
                        stream_id,
                        payload_len,
                        fanout_ms = fanout_elapsed.as_secs_f64() * 1000.0,
                        total_ms = total_elapsed.as_secs_f64() * 1000.0,
                        retry,
                        "append_payload"
                    );
                    self.append_metrics.record(
                        &self.owner_key,
                        lock_wait_total,
                        extent_lookup_elapsed,
                        fanout_elapsed,
                        total_elapsed,
                        retry as u64,
                    );

                    if result.end >= self.max_extent_size {
                        alloc_count += 1;
                        if alloc_count <= MAX_ALLOC_PER_APPEND {
                            // Preemptive roll on a SUCCESSFUL cap-hitting append:
                            // `result.end` is this append's acked end and the
                            // clean seal boundary (later-leased appends are
                            // beyond it → re-driven onto the new tail). end > 0
                            // ⇒ the manager trusts it without probing.
                            if let Ok((_, new_ext)) = self
                                .alloc_new_extent(stream_id, Some(result.end), result.extent_id)
                                .await
                            {
                                if let Ok(replica_addrs) =
                                    self.replica_addrs_for_extent(&new_ext).await
                                {
                                    let replica_node_ids = Self::replica_node_ids_for(&new_ext);
                                    let new_tail = StreamTail {
                                        extent: new_ext,
                                        replica_addrs,
                                        replica_node_ids,
                                    };
                                    let mut tx_clone = tx.clone();
                                    let _ = tx_clone
                                        .send(StreamSubmitMsg::ResetTail { tail: new_tail })
                                        .await;
                                }
                            }
                        }
                    }

                    return Ok(AppendResult {
                        extent_id: result.extent_id,
                        offset: result.offset,
                        end: result.end,
                    });
                }
                Err(e) => {
                    let msg = e.to_string();
                    let is_not_found = msg.contains("not found on replica");
                    let is_locked = msg.contains("LockedByOther");

                    if is_locked {
                        return Err(e);
                    }

                    retry += 1;
                    if is_not_found {
                        alloc_count += 1;
                        if alloc_count > MAX_ALLOC_PER_APPEND {
                            return Err(anyhow!(
                                "too many extent allocations ({alloc_count}) for single append, giving up"
                            ));
                        }
                        // SealCommit handshake: seal the failed tail at the
                        // worker's TRUE drained commit (no probe → no phantom).
                        // BUG2-IDEMPOTENT-ROLL: pin the seal to the tail the
                        // commit was captured for, so a retried roll is idempotent.
                        let (seal_commit, seal_eid) =
                            self.seal_commit_watermark(stream_id, &tx).await?;
                        let (_, new_ext) = self
                            .alloc_new_extent(stream_id, Some(seal_commit), seal_eid)
                            .await?;
                        let replica_addrs = self.replica_addrs_for_extent(&new_ext).await?;
                        let replica_node_ids = Self::replica_node_ids_for(&new_ext);
                        let new_tail = StreamTail {
                            extent: new_ext,
                            replica_addrs,
                            replica_node_ids,
                        };
                        let mut tx_clone = tx.clone();
                        tx_clone
                            .send(StreamSubmitMsg::ResetTail { tail: new_tail })
                            .await
                            .map_err(|_| anyhow!("worker gone mid-retry"))?;
                        continue;
                    }

                    if retry <= 2 {
                        tracing::warn!(stream_id, retry, error = %e, "append soft-error, retrying");
                        compio::time::sleep(Duration::from_millis(100)).await;
                        let fresh = self.load_stream_tail(stream_id).await?;
                        if fresh.extent.sealed {
                            alloc_count += 1;
                            if alloc_count > MAX_ALLOC_PER_APPEND {
                                return Err(anyhow!(
                                    "too many extent allocations ({alloc_count}) for single append, giving up"
                                ));
                            }
                            // BUG2-IDEMPOTENT-ROLL: pin seal to the captured tail.
                            let (seal_commit, seal_eid) =
                                self.seal_commit_watermark(stream_id, &tx).await?;
                            let (_, new_ext) = self
                                .alloc_new_extent(stream_id, Some(seal_commit), seal_eid)
                                .await?;
                            let replica_addrs = self.replica_addrs_for_extent(&new_ext).await?;
                            let replica_node_ids = Self::replica_node_ids_for(&new_ext);
                            let new_tail = StreamTail {
                                extent: new_ext,
                                replica_addrs,
                                replica_node_ids,
                            };
                            let mut tx_clone = tx.clone();
                            tx_clone
                                .send(StreamSubmitMsg::ResetTail { tail: new_tail })
                                .await
                                .map_err(|_| anyhow!("worker gone mid-retry"))?;
                        } else {
                            // Open tail reload. Hand the freshly-loaded tail to
                            // the worker; the worker's `ResetTail` handler
                            // decides whether to ZERO `state.commit`:
                            //   - SAME extent (the common soft-error case — a
                            //     transient replica failure that did NOT change
                            //     the tail) → PRESERVE the worker's commit (the
                            //     authoritative all-replica-acked prefix), only
                            //     refresh replica addrs. BUG#2 (seed=8): zeroing
                            //     it here let a later `seal_commit_watermark`
                            //     report commit=0 and seal the live tail at
                            //     sealed_length=0, orphaning acked VP/SST data
                            //     (split child then can't open:
                            //     stale_vp_offset_past_sealed_length).
                            //   - DIFFERENT extent (a genuine roll to a fresh,
                            //     empty open extent) → reset to 0 (correct).
                            // Putting the same-vs-different decision in the
                            // worker is load-bearing: only the worker knows its
                            // current cached tail extent_id (the public API does
                            // not), so it is the one place that can tell a
                            // transient failure on the same tail apart from a
                            // real tail change (coco review, 2026-05-30).
                            let mut tx_clone = tx.clone();
                            tx_clone
                                .send(StreamSubmitMsg::ResetTail { tail: fresh })
                                .await
                                .map_err(|_| anyhow!("worker gone mid-retry"))?;
                        }
                        continue;
                    }

                    alloc_count += 1;
                    if alloc_count > MAX_ALLOC_PER_APPEND {
                        return Err(anyhow!(
                            "too many extent allocations ({alloc_count}) for single append, giving up: {e}"
                        ));
                    }
                    // BUG2-IDEMPOTENT-ROLL: pin seal to the captured tail.
                    let (seal_commit, seal_eid) =
                        self.seal_commit_watermark(stream_id, &tx).await?;
                    let (_, new_ext) = self
                        .alloc_new_extent(stream_id, Some(seal_commit), seal_eid)
                        .await
                        .map_err(|alloc_err| {
                            alloc_err
                                .context(format!("alloc_new_extent failed after append error: {e}"))
                        })?;
                    let replica_addrs = self.replica_addrs_for_extent(&new_ext).await?;
                    let replica_node_ids = Self::replica_node_ids_for(&new_ext);
                    let new_tail = StreamTail {
                        extent: new_ext,
                        replica_addrs,
                        replica_node_ids,
                    };
                    let mut tx_clone = tx.clone();
                    tx_clone
                        .send(StreamSubmitMsg::ResetTail { tail: new_tail })
                        .await
                        .map_err(|_| anyhow!("worker gone mid-retry"))?;
                    retry = 0;
                    continue;
                }
            }
        }
    }

    /// First-use tail initialisation for a stream.  Serialised by a per-
    /// stream mutex so concurrent first-callers don't each RPC the
    /// manager.  The first caller loads the tail + queries commit_length
    /// and sends `ResetTail` + `SeedCursor` to the worker; subsequent
    /// callers observe `*guard == true` and skip.
    async fn ensure_tail_initialised(
        &self,
        stream_id: u64,
        tx: &mpsc::Sender<StreamSubmitMsg>,
    ) -> Result<()> {
        let lock = self.stream_init_lock(stream_id);
        let mut guard = lock.lock().await;
        if *guard {
            return Ok(());
        }
        let tail = self.load_stream_tail(stream_id).await?;
        let (tail, commit_val) = if tail.extent.sealed {
            // First-use / new-owner init: the inherited tail is already sealed,
            // so this is NOT a failover seal — we just roll past it. The
            // manager's `already_sealed` short-circuit preserves the existing
            // seal; `seal_commit = None` (no worker commit to claim → probe).
            let (_, new_ext) = self.alloc_new_extent(stream_id, None, 0).await?;
            let replica_addrs = self.replica_addrs_for_extent(&new_ext).await?;
            let replica_node_ids = Self::replica_node_ids_for(&new_ext);
            (
                StreamTail {
                    extent: new_ext,
                    replica_addrs,
                    replica_node_ids,
                },
                0,
            )
        } else {
            // Open tail. Determine its committed length to seed the worker
            // cursor. F227: a failure here must NEVER seed 0 — pre-F227
            // `unwrap_or(0)` turned "can't determine the committed length (a
            // replica down)" into cursor=0, and the next append's
            // `header.commit=0` truncated EVERY replica to 0, destroying all
            // acked data. So we retry the all-replica probe a few times first.
            //
            // BUG#1 (seed=15) — open-tail write-wedge: but a PERSISTENT failure
            // (a replica on the OPEN active tail permanently unreachable/short)
            // would otherwise wedge flush + writes FOREVER. Recovery only
            // reconfigures SEALED extents, so the open tail's bad replica never
            // heals, and `current_commit(&tail).await?` just propagated the
            // error on every retry → the flush's first-use init looped forever.
            // Escape: SEAL-AND-ROLL to a fresh tail on healthy nodes via
            // `alloc_new_extent(stream_id, None)`. This is SAFE under
            // all-replica-ACK: the manager's None path runs the LENIENT
            // seal-over-reachable probe (`compute_commit_seal`), sealing the old
            // tail at `min` over the reachable COMMITTED members, which is ≥ the
            // acked length (every acked record is on every replica) — no acked
            // data is dropped; only un-acked speculation past the seal is. MUST
            // pass `None` (let the manager probe), NEVER `Some(0)` (that asserts
            // commit=0 — the exact silent truncation the propagate above guards
            // against; we have no trusted worker commit here).
            let mut commit_result = self.current_commit(&tail).await;
            for _ in 0..OPEN_TAIL_COMMIT_RETRIES {
                if commit_result.is_ok() {
                    break;
                }
                compio::time::sleep(Duration::from_millis(OPEN_TAIL_COMMIT_BACKOFF_MS)).await;
                commit_result = self.current_commit(&tail).await;
            }
            match commit_result {
                Ok(commit_val) => (tail, commit_val),
                Err(e) => {
                    tracing::warn!(
                        stream_id,
                        extent_id = tail.extent.extent_id,
                        error = %e,
                        "BUG#1: open-tail current_commit persistently failed — sealing + rolling to a fresh tail (seal-over-reachable)"
                    );
                    let (_, new_ext) = self.alloc_new_extent(stream_id, None, 0).await?;
                    let replica_addrs = self.replica_addrs_for_extent(&new_ext).await?;
                    let replica_node_ids = Self::replica_node_ids_for(&new_ext);
                    (
                        StreamTail {
                            extent: new_ext,
                            replica_addrs,
                            replica_node_ids,
                        },
                        0,
                    )
                }
            }
        };
        let mut tx_clone = tx.clone();
        tx_clone
            .send(StreamSubmitMsg::ResetTail { tail })
            .await
            .map_err(|_| anyhow!("worker gone before init"))?;
        if commit_val > 0 {
            tx_clone
                .send(StreamSubmitMsg::SeedCursor { cursor: commit_val })
                .await
                .map_err(|_| anyhow!("worker gone before init"))?;
        }
        *guard = true;
        Ok(())
    }

    /// Query commit length from all replicas (min). Called on first append
    /// to an existing extent (commit==0) to avoid truncating pre-existing data.
    ///
    /// F227: NO quorum. This is a WAS stream layer — the append path is
    /// all-replica-ACK (`apply_completion` acks only when every replica
    /// wrote the record), so the committed length must be derived from ALL
    /// replicas. A subset `min` (the pre-F227 majority-quorum) can sit
    /// BELOW the acked length when it includes a short / catching-up
    /// replica (→ next append's `header.commit` truncates acked data on
    /// the up-to-date replicas → silent loss), or ABOVE it when it
    /// excludes a member (→ keeps un-acked data). We require every replica
    /// to respond and take the min over all of them; on any miss we return
    /// `Err` so the caller refuses to seed a cursor (NEVER seed a subset
    /// min — see `ensure_tail_initialised`). A permanently-dead replica is
    /// reconfigured out of the set by the manager seal + operator
    /// fence/recovery lifecycle, after which all remaining members respond.
    async fn current_commit(&self, tail: &StreamTail) -> Result<u64> {
        let mut min_len: Option<u64> = None;
        let mut success: usize = 0;
        let mut locked: usize = 0;
        let total = tail.replica_addrs.len();
        let owner_epoch = self.owner_epoch;
        for addr in &tail.replica_addrs {
            let req = CommitLengthReq {
                extent_id: tail.extent.extent_id,
                owner_epoch,
            };
            // 5 s — commit_length is a tiny in-memory probe on EN.
            // Per-replica timeout: a paged-out replica counts as a
            // miss for the quorum tally rather than wedging the
            // whole call.
            let result = self
                .pool
                .call_timeout(
                    addr,
                    MSG_COMMIT_LENGTH,
                    req.encode(),
                    Duration::from_secs(5),
                )
                .await;
            let Ok(resp_bytes) = result else {
                continue;
            };
            let Ok(resp) = CommitLengthResp::decode(resp_bytes) else {
                continue;
            };
            if resp.code != CODE_OK {
                // F270: a CODE_LOCKED_BY_OTHER rejection means OUR epoch is
                // stale (admin fence-bump / new owner), NOT that the replica
                // is unreachable. Pre-F270 this was folded into the generic
                // "only N/M responded" error, so the caller could never tell
                // a fenced probe from a dead replica — and a fence-bumped
                // writer retried the same stale epoch forever (the seed=13/15
                // open-tail write wedge's compounding layer).
                if resp.code == CODE_LOCKED_BY_OTHER {
                    locked += 1;
                }
                continue;
            }
            success += 1;
            min_len = Some(min_len.map_or(resp.length, |cur| cur.min(resp.length)));
        }
        // F227: require ALL replicas to respond — no quorum (see fn doc).
        if success < total {
            if locked > 0 {
                // The "LockedByOther" substring is load-bearing: the PS's
                // `is_locked_by_other` classifies on it and poisons the
                // partition, whose region_sync reopen re-acquires a FRESH
                // per-partition epoch (F267) — the self-heal for fence bumps.
                return Err(anyhow!(
                    "commit_length on extent {}: only {}/{} replicas responded \
                     ({} rejected LockedByOther — stale owner epoch)",
                    tail.extent.extent_id,
                    success,
                    total,
                    locked
                ));
            }
            return Err(anyhow!(
                "commit_length on extent {}: only {}/{} replicas responded (need all)",
                tail.extent.extent_id,
                success,
                total
            ));
        }
        min_len.ok_or_else(|| anyhow!("no available replica for commit_length"))
    }

    // ── public append API ────────────────────────────────────────────────────

    pub async fn append_batch_repeated(
        &self,
        stream_id: u64,
        block: &[u8],
        count: usize,
    ) -> Result<AppendResult> {
        if count == 0 {
            return Err(anyhow!("append_batch_repeated requires count > 0"));
        }
        let total = block
            .len()
            .checked_mul(count)
            .ok_or_else(|| anyhow!("append payload too large"))?;
        let mut payload = BytesMut::with_capacity(total);
        for _ in 0..count {
            payload.extend_from_slice(block);
        }
        self.append_payload(stream_id, payload.freeze()).await
    }

    pub async fn append_batch(&self, stream_id: u64, blocks: &[&[u8]]) -> Result<AppendResult> {
        if blocks.is_empty() {
            return Err(anyhow!("append_batch requires at least one block"));
        }
        let total = blocks.iter().try_fold(0usize, |acc, b| {
            acc.checked_add(b.len())
                .ok_or_else(|| anyhow!("append payload too large"))
        })?;
        let mut payload = BytesMut::with_capacity(total);
        for b in blocks {
            payload.extend_from_slice(b);
        }
        self.append_payload(stream_id, payload.freeze()).await
    }

    /// Append a pre-built Bytes payload directly (avoids an extra copy).
    pub async fn append_bytes(&self, stream_id: u64, payload: Bytes) -> Result<AppendResult> {
        self.append_payload(stream_id, payload).await
    }

    /// Append multiple Bytes segments without copying them into a single buffer.
    pub async fn append_segments(
        &self,
        stream_id: u64,
        segments: Vec<Bytes>,
    ) -> Result<AppendResult> {
        self.append_payload_segments(stream_id, segments).await
    }

    pub async fn append(&self, stream_id: u64, payload: &[u8]) -> Result<AppendResult> {
        self.append_payload(stream_id, Bytes::copy_from_slice(payload))
            .await
    }

    pub async fn commit_length(&self, stream_id: u64) -> Result<u64> {
        let (_stream, _extent, end) = self.check_commit(stream_id).await?;
        Ok(end)
    }

    /// F178 Phase 2: query a single replica for `MSG_SYNCED_LENGTH(extent_id)`.
    /// Returns `Ok(Some(synced))` on a success response, `Ok(None)` if the
    /// extent is unknown to that node (CODE_NOT_FOUND or any other non-OK
    /// code), and `Err` only on transport / decode failure.
    async fn synced_length_on_replica(&self, addr: &str, extent_id: u64) -> Result<Option<u64>> {
        let req = SyncedLengthReq { extent_id };
        // 5 s — atomic load of `entry.coalescer.last_synced` on EN.
        // Quorum-aware caller (`await_log_synced_to`) tolerates per-
        // replica failure, so the bound is generous.
        let resp_bytes = self
            .pool
            .call_timeout(
                addr,
                MSG_SYNCED_LENGTH,
                req.encode(),
                Duration::from_secs(5),
            )
            .await?;
        let resp = SyncedLengthResp::decode(resp_bytes)
            .map_err(|e| anyhow!("synced_length decode: {e}"))?;
        if resp.code == CODE_OK {
            Ok(Some(resp.length))
        } else {
            Ok(None)
        }
    }

    /// F178 Phase 2: wait until the per-extent fsync coalescer on **all**
    /// of `extent_id`'s replicas has flushed bytes covering `min_offset`.
    ///
    /// F227: NO quorum. The append path is all-replica-ACK, so a VP at
    /// `min_offset` is durable on every replica the moment its append
    /// acked; the flush barrier must therefore require ALL replicas to
    /// have synced past `min_offset` before the SST that names the VP is
    /// checkpointed — a fsync-quorum (the pre-F227 `⌊N/2⌋+1`) could
    /// publish an SST whose VP bytes are durable on only a subset, so a
    /// later min-commit truncation on the un-synced replica could orphan
    /// the VP. On a healthy cluster this is satisfied immediately (all-ACK
    /// already made it durable everywhere). Sealed extents report
    /// `max(last_synced, sealed_length)`, so this trivially succeeds
    /// against sealed sources.
    ///
    /// Polls every `AUTUMN_STREAM_SYNCED_POLL_MS` (default 2 ms — matches
    /// the coalescer cadence), bounded by
    /// `AUTUMN_STREAM_SYNCED_TIMEOUT_MS` (default 30 s). Returns
    /// `Err` if the wait times out (expected only on a stuck disk / dead
    /// majority).
    ///
    /// `min_offset == 0` is a no-op fast path; the caller can pass
    /// `imm.max_vp_offset` and we trivially return Ok if the imm carried
    /// no large values.
    pub async fn await_extent_synced_to(&self, extent_id: u64, min_offset: u64) -> Result<()> {
        if min_offset == 0 {
            return Ok(());
        }
        // F195: F178 flush-barrier knobs come from StreamClientConfig.
        // No env reads.
        let poll_ms: u64 = self.config.synced_poll.as_millis() as u64;
        let timeout_ms: u64 = self.config.synced_timeout.as_millis() as u64;

        let ex = self.fetch_extent_info(extent_id).await?;
        let addrs = self.replica_addrs_for_extent(&ex).await?;
        let total = addrs.len();
        if total == 0 {
            return Err(anyhow!(
                "await_extent_synced_to: no replica addrs for extent {extent_id}"
            ));
        }
        // F227: require ALL replicas synced — no quorum (see fn doc).
        let required = total;

        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            let mut covered: usize = 0;
            for addr in &addrs {
                match self.synced_length_on_replica(addr, extent_id).await {
                    Ok(Some(synced)) if synced >= min_offset => covered += 1,
                    _ => {}
                }
            }
            if covered >= required {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(anyhow!(
                    "await_extent_synced_to: timeout waiting for extent {extent_id} to sync \
                     past offset {min_offset} (all-replica {covered}/{required})"
                ));
            }
            compio::time::sleep(Duration::from_millis(poll_ms)).await;
        }
    }

    /// F178 Phase 2: helper for flush durability — convenience wrapper
    /// that delegates to `await_extent_synced_to` for a single extent.
    /// Renamed from the original plan's `await_log_synced_to(stream_id, _)`
    /// because `(extent_id, offset)` is the unit the partition layer
    /// already tracks: each `flush_one_imm` snapshot carries
    /// `(vp_extent_id, vp_offset)` for the latest log_stream extent the
    /// imm wrote to. The stream id is implicit in the extent id.
    pub async fn await_log_synced_to(&self, extent_id: u64, offset: u64) -> Result<()> {
        self.await_extent_synced_to(extent_id, offset).await
    }

    // F150 Phase B retired the public `sync_stream_tail` API. The F142 fsync
    // barrier is now folded into `start_write_batch`'s rotation-trigger
    // must_sync=true promotion in autumn-partition-server.

    pub async fn punch_holes(&self, stream_id: u64, extent_ids: Vec<u64>) -> Result<StreamInfo> {
        let req = manager_rpc::rkyv_encode(&PunchHolesReq {
            stream_id,
            owner_key: self.owner_key.clone(),
            owner_epoch: self.owner_epoch,
            extent_ids,
        });
        // 30 s — manager updates extent refs + may schedule
        // pending_extent_deletes; etcd mirror inside.
        let resp_data = self
            .manager_call(MSG_STREAM_PUNCH_HOLES, req, Duration::from_secs(30))
            .await?;
        let resp: PunchHolesResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        self.check_manager_resp(resp.code, &resp.message, "punch_holes")?;
        resp.stream
            .map(|s| Self::mgr_to_stream_info(&s))
            .ok_or_else(|| anyhow!("punch_holes: missing stream"))
    }

    pub async fn truncate(&self, stream_id: u64, extent_id: u64) -> Result<StreamInfo> {
        let req = manager_rpc::rkyv_encode(&TruncateReq {
            stream_id,
            owner_key: self.owner_key.clone(),
            owner_epoch: self.owner_epoch,
            extent_id,
        });
        // 30 s — same shape as punch_holes; ref updates + etcd mirror.
        let resp_data = self
            .manager_call(MSG_TRUNCATE, req, Duration::from_secs(30))
            .await?;
        let resp: TruncateResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        self.check_manager_resp(resp.code, &resp.message, "truncate")?;
        resp.updated_stream_info
            .map(|s| Self::mgr_to_stream_info(&s))
            .ok_or_else(|| anyhow!("truncate: missing stream"))
    }

    pub async fn get_stream_info(&self, stream_id: u64) -> Result<StreamInfo> {
        let req = manager_rpc::rkyv_encode(&StreamInfoReq {
            stream_ids: vec![stream_id],
        });
        // 5 s — read-only manager call.
        let resp_data = self
            .manager_call(MSG_STREAM_INFO, req, Duration::from_secs(5))
            .await?;
        let resp: StreamInfoResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        self.check_manager_resp(resp.code, &resp.message, "stream_info")?;
        resp.streams
            .into_iter()
            .next()
            .map(|(_, s)| Self::mgr_to_stream_info(&s))
            .ok_or_else(|| anyhow!("stream {} not found", stream_id))
    }

    /// Return the ExtentInfo for a given extent (includes sealed_length). Cached.
    pub async fn get_extent_info(&self, extent_id: u64) -> Result<ExtentInfo> {
        self.fetch_extent_info(extent_id).await
    }

    async fn fetch_extent_info(&self, extent_id: u64) -> Result<ExtentInfo> {
        if let Some(ex) = self.extent_info_cache.get(&extent_id) {
            return Ok(ex.clone());
        }
        let req = manager_rpc::rkyv_encode(&ExtentInfoReq { extent_id });
        // 5 s — read-only manager call. Hot in the EC stale-cache
        // refetch path; bounded so that path doesn't wedge.
        let resp_data = self
            .manager_call(MSG_EXTENT_INFO, req, Duration::from_secs(5))
            .await?;
        let resp: ExtentInfoResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        self.check_manager_resp(resp.code, &resp.message, "extent_info")?;
        let ex = resp
            .extent
            .map(|e| Self::mgr_to_extent_info(&e))
            .ok_or_else(|| anyhow!("extent {} not found", extent_id))?;
        self.extent_info_cache.insert(extent_id, ex.clone());
        Ok(ex)
    }

    /// Evict cached ExtentInfo so next read fetches fresh metadata.
    /// Needed after EC conversion changes the extent's topology.
    pub fn invalidate_extent_cache(&self, extent_id: u64) {
        self.extent_info_cache.remove(&extent_id);
    }

    /// Read-error policy shared by the replicated failover + hedge paths.
    /// An `EversionStale` error means the cached `ExtentInfo` is a stale
    /// GENERATION — the caller must FAIL FAST (returns `true`) so the
    /// top-level 2-attempt retry refetches it; the cache is left intact
    /// because every replica reports the same mismatch. Any OTHER error
    /// means this REPLICA failed; the cached replica layout may be wrong,
    /// so evict the entry and let the caller try the next replica (returns
    /// `false`).
    fn read_err_fail_fast(&self, e: &anyhow::Error, extent_id: u64) -> bool {
        if is_eversion_stale(e) {
            return true;
        }
        self.extent_info_cache.remove(&extent_id);
        false
    }

    /// Read bytes from a specific extent.
    /// Pass `length=0` to read from offset to the end of the extent.
    ///
    /// For replicated extents, reads larger than `read_chunk_bytes()`
    /// (default 256 MiB, env `AUTUMN_STREAM_READ_CHUNK_BYTES`) are split
    /// transparently into multiple chunks. This is required on macOS
    /// (pread INT_MAX) and matches Linux's per-syscall ceiling
    /// (`0x7ffff000`); without chunking, GC and recovery fail with
    /// EINVAL on extents > 2 GiB. EC reads keep their existing per-shard
    /// path — shards are at most `sealed_length / data_shards` and have
    /// their own size logic.
    ///
    /// Wrapped in a 2-attempt loop so that a stale `extent_info_cache`
    /// (e.g. after the manager flips an extent from replica to EC via
    /// `ec_conversion_dispatch_loop`) self-heals on the first miss:
    /// the inner read returns `EversionStale`, we evict the cache, and
    /// the second attempt fetches the fresh `ExtentInfo` and routes
    /// down the correct (EC) path. Without this, the first GET pays
    /// up to `3 × call_timeout` (~9 s) per stale replica before any
    /// future call sees the new layout.
    pub async fn read_bytes_from_extent(
        &self,
        extent_id: u64,
        offset: u64,
        length: u64,
    ) -> Result<(Vec<u8>, u64)> {
        for attempt in 0..2 {
            let ex = self.fetch_extent_info(extent_id).await?;
            match self.read_with_layout(extent_id, offset, length, &ex).await {
                Ok(r) => return Ok(r),
                Err(e) if attempt == 0 && is_eversion_stale(&e) => {
                    self.invalidate_extent_cache(extent_id);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("read_bytes_from_extent: 2-attempt loop must terminate")
    }

    /// F261: replay-oriented chunked read — like `read_bytes_from_extent`,
    /// but the requested window is CLAMPED to the extent's COMMITTED end
    /// before the read is issued: `sealed_length` for sealed extents, the
    /// min-replica commit probe for the open tail. An EN's data file can
    /// legitimately hold speculative bytes PAST the committed end (a replica
    /// that was ahead at seal time is never truncated back — commit-protocol
    /// truncation only happens on the next append, which never comes once
    /// sealed; open-tail replicas diverge the same way until the next
    /// append's `header.commit`). A plain explicit-length read is clamped
    /// only by the SERVING replica's local file length, so a chunked scanner
    /// stopping on `got < want` walks straight through the committed end and
    /// ingests un-committed — possibly replica-divergent, possibly
    /// about-to-be-truncated — bytes as if they were WAL content.
    ///
    /// Returns `(data, end)`; `data` is empty once `offset` reaches the
    /// committed end (clean end-of-scan). A SEALED extent with
    /// `offset > sealed_length` still surfaces the `StaleVpOffset` sentinel:
    /// a checkpoint pointing past the seal is corruption and must fail the
    /// partition open loudly, never be masked as a clean end-of-replay.
    pub async fn read_committed_bytes_from_extent(
        &self,
        extent_id: u64,
        offset: u64,
        length: u64,
    ) -> Result<(Vec<u8>, u64)> {
        for attempt in 0..2 {
            let ex = self.fetch_extent_info(extent_id).await?;
            let committed_end: u64 = if ex.sealed {
                if offset as u64 > ex.sealed_length {
                    return Err(anyhow::Error::new(StaleVpOffset {
                        extent_id,
                        requested_offset: offset,
                        requested_length: length,
                        sealed_length: ex.sealed_length,
                    }));
                }
                ex.sealed_length
            } else {
                // Open tail: one min-replica probe per call. Replay-only
                // cadence (one probe per 64 MiB chunk) — not a hot path.
                self.commit_length_for_extent(&ex).await?
            };
            let want = length.min(committed_end.saturating_sub(offset));
            if want == 0 {
                return Ok((Vec::new(), committed_end));
            }
            match self.read_with_layout(extent_id, offset, want, &ex).await {
                // MERGE-EC-REPLAY: return OUR authoritative `committed_end`
                // (the extent's sealed_length for a sealed extent), NOT
                // read_with_layout's second element. For an EC-converted extent
                // `ec_subrange_read` returns a SHARD-relative end
                // (≈ sealed_length / K); propagating it as the extent's
                // committed_end made the PS WAL replay — which uses this value
                // as its stop bound — read only ONE shard's worth of bytes and
                // trip WAL-FAILSTOP at the shard boundary, permanently wedging
                // any partition whose VP-head replay window reaches an EC log
                // extent (e.g. a merge survivor replaying the victim's spliced
                // extents, which the victim itself never replayed). The read
                // already clamped `want` to `committed_end - offset`, so the
                // returned bytes are correct; only the reported end was wrong.
                Ok((bytes, _read_end)) => return Ok((bytes, committed_end)),
                Err(e) if attempt == 0 && is_eversion_stale(&e) => {
                    self.invalidate_extent_cache(extent_id);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("read_committed_bytes_from_extent: 2-attempt loop must terminate")
    }

    /// WAL self-heal A2 (docs/wal_selfheal_design.md): read the committed
    /// `[offset, offset+length)` range of a REPLICATED extent from ONE specific
    /// replica (`replica_idx` into `replicates ++ parity`), with NO failover —
    /// so the replay self-heal (A3) can read the SAME committed range from each
    /// replica in turn and CRC-check it, isolating the bit-rotted one.
    ///
    /// The committed clamp is identical to `read_committed_bytes_from_extent`
    /// (the open-time `check commit length` already aligned the committed
    /// length across replicas, so all replicas agree on the range — only the
    /// CONTENT can differ). Returns `(bytes, committed_end, node_id)`.
    ///
    /// EC-converted extents return `Err` (the replicated self-heal does not
    /// apply — EC shard corruption is reconstructed via `ec_subrange_read`, a
    /// separate path). `replica_idx` out of range → `Err`.
    pub async fn read_committed_from_replica(
        &self,
        extent_id: u64,
        replica_idx: usize,
        offset: u64,
        length: u64,
    ) -> Result<(Vec<u8>, u64, u64)> {
        let ex = self.fetch_extent_info(extent_id).await?;
        if ex.ec_converted {
            return Err(anyhow!(
                "extent {extent_id} is EC-converted; per-replica read not applicable"
            ));
        }
        let node_ids: Vec<u64> = ex
            .replicates
            .iter()
            .chain(ex.parity.iter())
            .copied()
            .collect();
        if replica_idx >= node_ids.len() {
            return Err(anyhow!(
                "replica_idx {replica_idx} out of range (extent {extent_id} has {} replicas)",
                node_ids.len()
            ));
        }
        let committed_end: u64 = if ex.sealed {
            if offset as u64 > ex.sealed_length {
                return Err(anyhow::Error::new(StaleVpOffset {
                    extent_id,
                    requested_offset: offset,
                    requested_length: length,
                    sealed_length: ex.sealed_length,
                }));
            }
            ex.sealed_length
        } else {
            self.commit_length_for_extent(&ex).await?
        };
        let node_id = node_ids[replica_idx];
        let want = length.min(committed_end.saturating_sub(offset));
        if want == 0 {
            return Ok((Vec::new(), committed_end, node_id));
        }
        let addrs = self.replica_addrs_for_extent(&ex).await?;
        let addr = &addrs[replica_idx];
        let (bytes, _end) = self
            .read_shard_from_addr(addr, extent_id, ex.eversion, offset, want)
            .await?;
        Ok((bytes, committed_end, node_id))
    }

    /// WAL self-heal A2: replica count of a REPLICATED extent (= the valid
    /// `replica_idx` range for `read_committed_from_replica`). EC extent → 0.
    pub async fn replicated_replica_count(&self, extent_id: u64) -> Result<usize> {
        let ex = self.fetch_extent_info(extent_id).await?;
        if ex.ec_converted {
            return Ok(0);
        }
        Ok(ex.replicates.len() + ex.parity.len())
    }

    /// WAL self-heal A5: report bit-rotted replica(s) of a SEALED log_stream
    /// extent to the manager so it ISOLATES them (clears each corrupt slot's
    /// `avali` bit + bumps eversion, etcd-first) — the A1 read-path filter then
    /// stops serving from those slots, and the eversion bump forces every PS to
    /// refetch the cleared ExtentInfo on its next read. Fenced: carries the
    /// partition `owner_epoch` (self) + the extent `eversion` the PS saw; the
    /// manager CAS-validates both. Returns Err on any non-OK code so the caller
    /// can keep the partition fail-loud (isolation MUST succeed before serving,
    /// design invariant I1). After a successful report the caller should
    /// `invalidate_extent_cache(extent_id)` so its own subsequent reads refetch
    /// the cleared-avali / bumped-eversion ExtentInfo.
    pub async fn report_corrupt_replica(
        &self,
        partition_id: u64,
        log_stream_id: u64,
        extent_id: u64,
        eversion: u64,
        corrupt_node_ids: Vec<u64>,
    ) -> Result<()> {
        let req = manager_rpc::rkyv_encode(&ReportCorruptReplicaReq {
            partition_id,
            owner_epoch: self.owner_epoch,
            log_stream_id,
            extent_id,
            eversion,
            corrupt_node_ids,
        });
        let resp_data = self
            .manager_call(MSG_REPORT_CORRUPT_REPLICA, req, Duration::from_secs(10))
            .await?;
        let resp: ReportCorruptReplicaResp =
            manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        self.note_manager_code(resp.code);
        if resp.code != CODE_OK {
            return Err(anyhow!("report_corrupt_replica refused: {}", resp.message));
        }
        Ok(())
    }

    /// F216-E (UCX) / F219 (TCP) recv-side copy-elimination fast path: recv the
    /// value straight into a read_loop-owned `PooledBuf` (MSG_READ_BYTES_ZC +
    /// call_into_pooled). UCX → registered RDMA recv (zero-copy); TCP → compio
    /// owned read (one kernel copy, no app-level copy). Returns `Some((pb, len))`
    /// on a clean OK; returns `Ok(None)` for ANYTHING the simple replicated path
    /// can't handle (EC extent, length==0/unknown, multi-chunk, eversion-stale,
    /// non-OK code, all replicas failed) so the caller falls back to the copy path
    /// (`read_bytes_from_extent`, which owns eversion-retry / EC / chunking /
    /// failover). Cancel-safe: call_into_pooled keeps the buffer with the
    /// read_loop, so a timeout reclaims it (no leak).
    pub async fn read_value_into_pooled(
        &self,
        extent_id: u64,
        offset: u64,
        length: u64,
    ) -> Result<Option<(autumn_rpc::PooledBuf, usize)>> {
        // F219: both transports use this fast path now. UCX recvs the value into
        // a *registered* buffer (RDMA, no off-wire copy); TCP recvs it into a
        // pooled buffer via a compio owned read in the rpc read_loop (no
        // FrameDecoder accumulation copy — only the unavoidable kernel copy).
        // The EN `MSG_READ_BYTES_ZC` response is value-separable + pooled on both
        // transports, so the EN send side also drops its per-op alloc/zeroing +
        // encode copy (subsumes F216-F).
        if length == 0 {
            return Ok(None);
        }
        // F276: this ZC value fast path is a READ path too — refresh the
        // Suspected snapshot (TTL-gated, non-blocking) so it routes around a
        // flaky node like the copy path, not just the avali-isolated ones
        // (coco P2: without this the hot GET still hit slot 0 first and paid
        // the 3 s timeout on a Suspected EN — the VP-read-paths-fragmented trap).
        self.maybe_refresh_suspected();
        let ex = self.fetch_extent_info(extent_id).await?;
        // EC / chunked / stale-VP-offset → let the copy path handle it.
        if ex.ec_converted
            || length > self.config.read_chunk_bytes
            || (ex.sealed && offset as u64 > ex.sealed_length)
        {
            return Ok(None);
        }
        let addrs = self.replica_addrs_for_extent(&ex).await?;
        // WAL self-heal A1 + F276: serve only avali-eligible slots, healthy
        // (non-Suspected) replicas FIRST, rotated start — exactly the copy
        // path's `replicated_read_order`. avali isolation: a bit-rotted-but-
        // isolated replica returns CODE_OK with corrupt bytes (no per-VP CRC on
        // this path), so it must never be read (I2 invariant; e2e 2026-06-14).
        // Suspected-to-back: a flaky node is tried last so the common case
        // doesn't pay its timeout. With an empty snapshot this is the prior
        // eligible-slot order (plus F258 rotation, matching the copy path).
        let order = {
            let c = self.suspected.borrow();
            replicated_read_order(&ex, offset, &c.nodes)
        };
        for &slot in &order {
            let addr = &addrs[slot];
            let req = ReadBytesReq {
                extent_id,
                eversion: ex.eversion,
                offset,
                length,
            }
            .encode();
            match self
                .pool
                .call_into_pooled(addr, MSG_READ_BYTES_ZC, req, Duration::from_secs(3))
                .await
            {
                Ok((pb, code)) if code == CODE_OK => return Ok(Some((pb, length as usize))),
                Ok((_pb, _code)) => {
                    // eversion mismatch / EN-side error → bail to the copy path
                    // (it refetches ExtentInfo + re-routes EC). pb drops → pool.
                    self.extent_info_cache.remove(&extent_id);
                    return Ok(None);
                }
                Err(_e) => {
                    // transport/timeout error → evict + try next replica.
                    self.extent_info_cache.remove(&extent_id);
                }
            }
        }
        Ok(None)
    }

    /// Branch on the (cached) extent layout: EC sub-range read for
    /// EC extents, replicated read with chunked failover for replica
    /// extents. The `read_bytes_from_extent` retry loop handles
    /// stale-cache (`EversionStale`) propagation.
    ///
    /// EC dispatch keys on `ec_converted` (set only by the manager's
    /// `apply_ec_conversion_done` after RS-encoding a sealed extent),
    /// NOT on `parity.is_empty()`. The manager pre-fills `parity` for
    /// every extent allocated via `stream_alloc_extent` on an EC
    /// stream, but those extents stay full-replicated on every K+M
    /// node until EC conversion runs (only triggered after seal).
    /// Routing an open / pre-conversion extent through `ec_subrange_read`
    /// would compute `shard_size` from `sealed_length=0` and panic with
    /// `range start index … out of range for slice of length …` on the
    /// per-shard slice — and the underlying data isn't EC-shaped yet
    /// anyway. `read_replicated_with_failover` already iterates the
    /// full `replicates ++ parity` address list via
    /// `replica_addrs_for_extent`, so it correctly hits all K+M nodes
    /// holding the replicated payload.
    async fn read_with_layout(
        &self,
        extent_id: u64,
        offset: u64,
        length: u64,
        ex: &ExtentInfo,
    ) -> Result<(Vec<u8>, u64)> {
        // F276: refresh the Suspected snapshot in the background (TTL-gated,
        // non-blocking) so the replica-routing + EC-reconstruct decisions
        // below see a ~2 s-fresh view of which nodes are flaky.
        self.maybe_refresh_suspected();
        // F210-H1: mirror the F204 `StaleVpOffset` sentinel for the
        // replicated path. Pre-F210-H1 only `ec_slice_decoded` produced
        // it; a VP read on a sealed replicated extent whose offset was
        // past `sealed_length` was silently short-circuited by the
        // server (`handle_read_bytes` returns `code=OK end=total_len
        // payload=[]` when `read_offset > total_len`). Operations
        // grepping the wire contract `stale_vp_offset_past_sealed_length:`
        // never saw the replicated case. Detecting upfront here covers
        // BOTH layouts and skips the wasted server round-trip / EC
        // decode on a known-stale VP. Matches `ec_slice_decoded`'s
        // `if start > full_payload.len()` semantics — only fires when
        // the extent has a recorded `sealed_length`, since pre-seal
        // there's no authoritative bound to check against.
        if ex.sealed && offset as u64 > ex.sealed_length {
            return Err(anyhow::Error::new(StaleVpOffset {
                extent_id,
                requested_offset: offset,
                requested_length: length,
                sealed_length: ex.sealed_length,
            }));
        }
        if ex.ec_converted {
            return self.ec_subrange_read(extent_id, offset, length, ex).await;
        }

        // Resolve effective length so we know when to stop chunking.
        // length=0 ("to end") needs an explicit size: sealed_length for
        // sealed extents, commit_length min-replica for open extents.
        let resolved = if length == 0 {
            // A SEALED extent has an authoritative bound = sealed_length (even
            // 0 — a sealed-empty extent reads to-end as empty, no probe). Only
            // an OPEN extent needs the min-replica commit_length probe.
            let total_end = if ex.sealed {
                ex.sealed_length
            } else {
                self.commit_length_for_extent(ex).await?
            };
            total_end.saturating_sub(offset)
        } else {
            length
        };

        let chunk = self.config.read_chunk_bytes;
        if resolved <= chunk {
            return self.read_replicated_with_failover(ex, offset, length).await;
        }

        let mut data: Vec<u8> = Vec::with_capacity(resolved as usize);
        let stop = offset
            .checked_add(resolved)
            .ok_or_else(|| anyhow!("read_bytes_from_extent: offset+length overflows u32"))?;
        let mut cur = offset;
        let mut last_end: u64 = 0;
        while cur < stop {
            let want = (stop - cur).min(chunk);
            let (piece, end) = self.read_replicated_with_failover(ex, cur, want).await?;
            if piece.is_empty() {
                break;
            }
            let piece_len = piece.len() as u64;
            data.extend_from_slice(&piece);
            cur = cur.saturating_add(piece_len);
            last_end = end;
            if piece_len < want {
                // server-side has less data than requested (open extent
                // grew slower than expected); stop early
                break;
            }
        }
        Ok((data, last_end))
    }

    /// F259: descriptor for a CLIENT-side direct extent read — the cached
    /// `(eversion, replica addresses)` for an extent, fetched/refreshed via
    /// the manager on cache miss. The PS embeds these in a MSG_GET_REDIRECT
    /// response so the client can read value bytes straight from an EN
    /// without a manager round-trip of its own.
    pub async fn extent_read_descriptor(&self, extent_id: u64) -> Result<(u64, Vec<String>)> {
        // F276: the external client picks its OWN hash-rotated start over the
        // returned addrs (crates/client/src/lib.rs), so reordering can't route
        // around a flaky node — a Suspected address must be EXCLUDED outright
        // (coco P2). Refresh the snapshot here too (this is a read path).
        self.maybe_refresh_suspected();
        let ex = self.fetch_extent_info(extent_id).await?;
        // coco P1 (F259): an EC-converted extent holds RS shards, not the
        // full payload — a single-EN raw read would hand the client shard
        // bytes as if they were the value (data corruption). EC reads must
        // go through ec_subrange_read; refuse the descriptor so the PS
        // falls back to the proxy path.
        if ex.ec_converted {
            return Err(anyhow!(
                "extent {extent_id} is EC-converted; direct read not supported"
            ));
        }
        let addrs = self.replica_addrs_for_extent(&ex).await?;
        // WAL self-heal A1 + F276: hand the client only avali-ELIGIBLE replicas
        // (an isolated bit-rotted slot must not be a client-direct read target
        // — same I2 invariant as the ZC + copy paths, no per-VP CRC here) AND
        // drop manager-Suspected replicas (the client hash-rotates, so excluding
        // is the only effective route-around). `healthy_eligible_slots` keeps
        // ALL eligible when every one is Suspected — never strand the read.
        let slots = {
            let c = self.suspected.borrow();
            healthy_eligible_slots(&ex, &c.nodes)
        };
        let filtered: Vec<String> = slots.into_iter().map(|s| addrs[s].clone()).collect();
        Ok((ex.eversion, filtered))
    }

    /// Replicated-mode read with per-replica failover. Used both for the
    /// single-shot small path and as the per-chunk worker for the chunked
    /// large-extent path.
    ///
    /// On `EversionStale` we **fail fast** — every replica in `ex` is
    /// guaranteed to report the same mismatch (manager bumps eversion
    /// once for the whole extent during EC conversion), so iterating
    /// the remaining addresses just burns 3s timeouts each. Top-level
    /// `read_bytes_from_extent` catches this and refetches.
    ///
    /// F258 (a) — rotated start replica. Pre-F258 every read walked
    /// `addrs` from index 0, so ALL replicated read IO landed on
    /// replica[0] while the other two replicas' disks + NICs idled.
    /// For SEALED extents (immutable; all-replica-ACK means every
    /// committed byte is on every replica) the start index is rotated
    /// by `(extent_id, offset)` hash — consecutive chunks of the
    /// chunked large-read path naturally stripe across replicas.
    /// Open-tail reads keep the legacy replica[0]-first order.
    ///
    /// F258 (b) — optional hedged read (off by default;
    /// `set_read_hedge_ms`). When enabled and the rotated-first replica
    /// hasn't answered within the hedge window, the SECOND replica is
    /// raced concurrently and the first Ok wins — classic "Tail at
    /// Scale" tail-latency repair. Both-fail falls back to the
    /// remaining replicas in rotated order (same failover semantics).
    async fn read_replicated_with_failover(
        &self,
        ex: &ExtentInfo,
        offset: u64,
        length: u64,
    ) -> Result<(Vec<u8>, u64)> {
        let addrs = self.replica_addrs_for_extent(ex).await?;
        let n = addrs.len();
        let mut last_err = anyhow!("no replicas for extent {}", ex.extent_id);

        // A1 (self-heal I2): avali-isolated slots are dropped from the read set
        // on a sealed replicated extent. F276: among the eligible slots, the
        // ones whose node the manager believes Suspected are moved to the BACK
        // so a flaky replica never costs a per-read RPC timeout before failover
        // — soft hint, they remain a last-resort fallback (suspected != dead,
        // and every committed byte is on every replica). `replicated_read_order`
        // folds all three: rotated start, avali eligibility, suspected-to-back.
        // With an empty Suspected snapshot the order is the pre-F276 rotated
        // walk, so the hot path is unchanged.
        let eligible = eligible_replica_slots(ex);
        let filtered = eligible.len() < n;
        if filtered {
            tracing::warn!(
                extent_id = ex.extent_id,
                avali = ex.avali,
                eligible = ?eligible,
                "read: serving from avali-eligible replicas only (some slot isolated)"
            );
        }
        let order = {
            let c = self.suspected.borrow();
            replicated_read_order(ex, offset, &c.nodes)
        };
        if order.is_empty() {
            return Err(last_err);
        }

        let mut from = 0usize;
        // Hedge the two LEADING (healthy-first) slots. Disabled when a slot is
        // avali-isolated (filtered), same as pre-F276.
        if !filtered && read_hedge_ms() > 0 && ex.sealed && order.len() > 1 {
            match self
                .read_hedged_pair(&addrs, order[0], order[1], ex, offset, length)
                .await
            {
                Ok(r) => return Ok(r),
                Err(e) => {
                    if self.read_err_fail_fast(&e, ex.extent_id) {
                        return Err(e);
                    }
                    last_err = e;
                }
            }
            from = 2; // hedge already consumed order[0] and order[1]
        }

        for &slot in &order[from..] {
            let addr = &addrs[slot];
            match self
                .read_shard_from_addr(addr, ex.extent_id, ex.eversion, offset, length)
                .await
            {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if self.read_err_fail_fast(&e, ex.extent_id) {
                        return Err(e);
                    }
                    last_err = e;
                }
            }
        }
        Err(last_err)
    }

    /// F258 (b): hedged read over the first two rotated replicas. Fires the
    /// read to `addrs[start]`; if no answer within `read_hedge_ms()`, races
    /// a second read to `addrs[start+1]` and returns whichever succeeds
    /// first.
    ///
    /// Both reads run as DETACHED tasks that always run to completion (coco
    /// P1 on the first draft): dropping an in-flight `call_timeout` future
    /// would strand its req_id in `RpcClient.pending` — there is no
    /// drop-guard removal, and the bare drop also skips the timeout-evict
    /// path — so a connected-but-unresponsive EN would accumulate pending
    /// entries forever. Detached tasks let the LOSER finish on its own
    /// (bounded by its rpc timeout, which evicts on expiry); results come
    /// back over oneshots, and a dropped oneshot receiver is harmless.
    /// Eversion-stale fails fast on every arm (same rationale as failover).
    async fn read_hedged_pair(
        &self,
        addrs: &[String],
        s0: usize,
        s1: usize,
        ex: &ExtentInfo,
        offset: u64,
        length: u64,
    ) -> Result<(Vec<u8>, u64)> {
        use futures::future::{select, Either};
        let n = addrs.len();
        // F276: `s0`/`s1` are the two LEADING slots of the suspected-aware read
        // order (healthy-first), not necessarily `start`/`start+1`.
        let a0 = addrs[s0 % n].clone();
        let a1 = addrs[s1 % n].clone();
        let Some(sc) = self.self_weak.upgrade() else {
            // Client is shutting down — plain single read, no hedge.
            return self
                .read_shard_from_addr(&a0, ex.extent_id, ex.eversion, offset, length)
                .await;
        };
        let (eid, ev) = (ex.extent_id, ex.eversion);
        let spawn_read = |sc: Rc<StreamClient>, addr: String| {
            let (tx, rx) = futures::channel::oneshot::channel();
            compio::runtime::spawn(async move {
                let _ = tx.send(
                    sc.read_shard_from_addr(&addr, eid, ev, offset, length)
                        .await,
                );
            })
            .detach();
            rx
        };

        let rx0 = spawn_read(sc.clone(), a0);
        futures::pin_mut!(rx0);
        let timer = compio::time::sleep(Duration::from_millis(read_hedge_ms()));
        futures::pin_mut!(timer);

        let rx0 = match select(rx0, timer).await {
            Either::Left((res, _timer)) => {
                return match flatten_hedge(res) {
                    Ok(r) => Ok(r),
                    Err(e) => {
                        if self.read_err_fail_fast(&e, ex.extent_id) {
                            return Err(e);
                        }
                        // First failed before the hedge window: sequential
                        // second read (no point hedging a known failure).
                        flatten_hedge(spawn_read(sc, a1).await)
                    }
                };
            }
            Either::Right(((), rx0)) => rx0,
        };

        // Hedge window elapsed: race first vs second, first Ok wins. The
        // loser keeps running detached and cleans up via its own timeout.
        let rx1 = spawn_read(sc, a1);
        futures::pin_mut!(rx1);
        match select(rx0, rx1).await {
            Either::Left((res0, rx1_pending)) => match flatten_hedge(res0) {
                Ok(r) => Ok(r),
                Err(e) => {
                    if self.read_err_fail_fast(&e, ex.extent_id) {
                        return Err(e);
                    }
                    flatten_hedge(rx1_pending.await)
                }
            },
            Either::Right((res1, rx0_pending)) => match flatten_hedge(res1) {
                Ok(r) => Ok(r),
                Err(e) => {
                    if self.read_err_fail_fast(&e, ex.extent_id) {
                        return Err(e);
                    }
                    flatten_hedge(rx0_pending.await)
                }
            },
        }
    }

    /// Query commit_length on each replica, return the minimum (the
    /// safe contiguous-prefix end). For open extents only — sealed
    /// extents should read `ExtentInfo.sealed_length` directly.
    ///
    /// F156: requires majority quorum to respond — see `current_commit`
    /// for the rationale. Without a quorum check, the protocol could
    /// commit at a position only the lone surviving responder held,
    /// permanently losing data if that responder later died before
    /// re-replicating to the unreachable peers.
    ///
    /// **Fence-free**: uses `MSG_PROBE_EXTENT` rather than
    /// `MSG_COMMIT_LENGTH`. Pre-fix this called `MSG_COMMIT_LENGTH`
    /// with the StreamClient's owner owner_epoch, which causes the EN to
    /// run fence handover (bumps `owner_epoch` if our owner_epoch is
    /// higher). Two harmful side-effects in production:
    ///   1. A reader StreamClient created with a NEW owner_key
    ///      (higher owner_epoch) silently fences the original writer's
    ///      next append → CODE_LOCKED_BY_OTHER. Reproducible via the
    ///      f029 integration test: PS appends meta_stream 3× with
    ///      owner_epoch=1; test creates external StreamClient
    ///      (owner_epoch=2) for read-only `read_last_extent_data`; that
    ///      call falls through to this helper which bumps EN's
    ///      owner_epoch to 2; PS's 4th append (compact's checkpoint)
    ///      fails with LockedByOther.
    ///   2. The same shape can hit production whenever an external
    ///      reader (e.g. autumn-stream-cli read, or any consumer that
    ///      opens its own StreamClient against the same stream) does
    ///      a `read_bytes_from_extent(_, _, 0)` against an active
    ///      writer's stream.
    /// `MSG_PROBE_EXTENT` returns the same `(code, length)` shape but
    /// skips the fence interaction entirely — exactly what a read path
    /// needs.
    async fn commit_length_for_extent(&self, ex: &ExtentInfo) -> Result<u64> {
        let addrs = self.replica_addrs_for_extent(ex).await?;
        let mut min_len: Option<u64> = None;
        let mut success: usize = 0;
        let total = addrs.len();
        for addr in &addrs {
            let req = ProbeExtentReq {
                extent_id: ex.extent_id,
            };
            // 5 s — same per-replica budget as the historical
            // commit_length path. Per-replica miss is tolerated by
            // the quorum tally below.
            let Ok(resp_bytes) = self
                .pool
                .call_timeout(addr, MSG_PROBE_EXTENT, req.encode(), Duration::from_secs(5))
                .await
            else {
                continue;
            };
            let Ok(resp) = ProbeExtentResp::decode(resp_bytes) else {
                continue;
            };
            if resp.code != CODE_OK {
                continue;
            }
            success += 1;
            min_len = Some(min_len.map_or(resp.length, |cur| cur.min(resp.length)));
        }
        let quorum = total / 2 + 1;
        if success < quorum {
            return Err(anyhow!(
                "insufficient quorum for probe_extent on extent {}: got {} of {} (need {})",
                ex.extent_id,
                success,
                total,
                quorum
            ));
        }
        min_len.ok_or_else(|| {
            anyhow!(
                "no replica responded to probe_extent for extent {}",
                ex.extent_id
            )
        })
    }

    /// Read raw shard bytes from a single replica address via autumn-rpc.
    ///
    /// `eversion` is the caller's view of the extent's eversion (from
    /// the cached `ExtentInfo`). Server-side `handle_read_bytes` /
    /// `build_read_future` reject stale (lower) eversions with
    /// `CODE_EVERSION_MISMATCH`, surfaced here as `EversionStale` so
    /// the top-level `read_bytes_from_extent` can refetch and retry.
    ///
    /// F119-C: there is no longer a "pass 0 to skip" sentinel — the
    /// extent_node defaults `entry.eversion` to 1 on alloc (matches
    /// the manager's `MgrExtentInfo { eversion: 1 }` default), so any
    /// `eversion=0` is by definition stale (or never-cached). Always
    /// pass the cached `ex.eversion`; the only legitimate `0` is from
    /// raw bench/test code talking to a hand-rolled `entry.eversion=0`
    /// fixture.
    async fn read_shard_from_addr(
        &self,
        addr: &str,
        extent_id: u64,
        eversion: u64,
        offset: u64,
        length: u64,
    ) -> Result<(Vec<u8>, u64)> {
        // u64-offset widening: a single MSG_READ_BYTES response is framed with
        // `payload_len: u32` (frame.rs), so a per-RPC read MUST stay under
        // 4 GiB. With max_extent_size raised to 16 GiB an EC shard is
        // ~sealed_length/K (≈ 5.33 GiB at 16 GiB / K=3), so a full-shard read
        // here would overflow the frame length field. EC reads route straight
        // to ec_subrange_read → here WITHOUT the replicated path's chunking
        // (read_with_layout only chunks the replicated branch), so this is the
        // single EC choke point that must bound the per-RPC size. Chunk at
        // `read_chunk_bytes` (default 256 MiB), exactly like the replicated
        // path; short read stops early (a shard is shorter than requested).
        let chunk = self.config.read_chunk_bytes;
        if length <= chunk {
            return self
                .read_shard_chunk_from_addr(addr, extent_id, eversion, offset, length)
                .await;
        }
        let mut data: Vec<u8> = Vec::with_capacity(length as usize);
        let stop = offset
            .checked_add(length)
            .ok_or_else(|| anyhow!("read_shard_from_addr: offset+length overflows u64"))?;
        let mut cur = offset;
        let mut last_end: u64 = 0;
        while cur < stop {
            let want = (stop - cur).min(chunk);
            let (piece, end) = self
                .read_shard_chunk_from_addr(addr, extent_id, eversion, cur, want)
                .await?;
            if piece.is_empty() {
                break;
            }
            let piece_len = piece.len() as u64;
            data.extend_from_slice(&piece);
            cur = cur.saturating_add(piece_len);
            last_end = end;
            if piece_len < want {
                break;
            }
        }
        Ok((data, last_end))
    }

    /// Single-RPC shard read (one `MSG_READ_BYTES`). Callers must keep
    /// `length <= read_chunk_bytes` so the `payload_len: u32` frame field
    /// never overflows — `read_shard_from_addr` enforces that by chunking.
    async fn read_shard_chunk_from_addr(
        &self,
        addr: &str,
        extent_id: u64,
        eversion: u64,
        offset: u64,
        length: u64,
    ) -> Result<(Vec<u8>, u64)> {
        let req = ReadBytesReq {
            extent_id,
            eversion,
            offset,
            length,
        };
        let resp_bytes = self
            .pool
            .call_timeout(addr, MSG_READ_BYTES, req.encode(), Duration::from_secs(3))
            .await?;
        let resp = ReadBytesResp::decode(resp_bytes)
            .map_err(|e| anyhow!("decode ReadBytesResp from {addr}: {e}"))?;
        if resp.code == CODE_EVERSION_MISMATCH {
            return Err(anyhow::Error::new(EversionStale)
                .context(format!("read_bytes from {addr} extent={extent_id}")));
        }
        if resp.code != CODE_OK {
            return Err(anyhow!(
                "read_bytes error from {addr}: code={}",
                crate::extent_rpc::code_description(resp.code)
            ));
        }
        Ok((resp.payload.to_vec(), resp.end))
    }

    /// EC sub-range read.
    ///
    /// Generalised N-shard parallel scatter: for a `[offset, offset+length)`
    /// sub-range that spans `start_shard..=end_shard` of the data shards,
    /// fire one parallel `read_shard_from_addr` per touched shard and
    /// concatenate the results in shard order. Each shard's slice is:
    ///
    /// - **first shard** (`start_shard`): `[start % shard_size, shard_size)`
    /// - **last shard** (`end_shard`): `[0, end - end_shard*shard_size)`
    /// - **middle shards** (when `end_shard - start_shard >= 2`):
    ///   the entire shard `[0, shard_size)`
    /// - **single-shard read** (`start_shard == end_shard`): just
    ///   `[start % shard_size, start % shard_size + length)`
    ///
    /// On any per-shard error (non-eversion), short read, or a request
    /// that lands past the data shards (e.g. a stale VP whose offset
    /// straddles into the parity region), we fall back to
    /// `ec_read_full_and_slice` — slow (full extent decode + RS) but
    /// always correct.
    ///
    /// **Bug history**: pre-fix the two-shard branch only checked
    /// `end_shard < data_shards` and entered for any 3+ shard span
    /// (e.g. K=3 EC extent + 64 MiB GC chunk). It read shard 0 +
    /// shard 2 only — silently skipping shard 1's bytes. The server
    /// returned a SHORT-OK for shard 2 (length exceeded that shard's
    /// actual file size), the join produced a contiguous buffer of
    /// the right total length but with the middle ~15 MiB replaced
    /// by shard 2's prefix, and the GC log-record decoder panicked
    /// with `trailing bytes did not form a complete record`. The
    /// generalised scatter below handles spans of any width safely.
    async fn ec_subrange_read(
        &self,
        extent_id: u64,
        offset: u64,
        length: u64,
        ex: &ExtentInfo,
    ) -> Result<(Vec<u8>, u64)> {
        let data_shards = ex.replicates.len();
        if data_shards == 0 {
            return Err(anyhow!("EC extent {extent_id} has no data shards"));
        }

        let sealed_length = ex.sealed_length;
        let shard_size = crate::erasure::shard_size(sealed_length as usize, data_shards) as u64;
        // Saturating sub guards against a stale / corrupt VP whose
        // offset has drifted past sealed_length: instead of unsigned-
        // wrapping `read_len`, we fall through to ec_read_full_and_slice
        // which returns an explicit Err.
        let read_len = if length == 0 {
            sealed_length.saturating_sub(offset as u64)
        } else {
            length as u64
        };

        if read_len == 0 {
            return Ok((Vec::new(), 0));
        }

        let start = offset as u64;
        let end = start + read_len;

        let start_shard = (start / shard_size) as usize;
        let end_shard = ((end - 1) / shard_size) as usize;

        // If the read lands past the data-shard region (start_shard or
        // end_shard >= data_shards) we cannot service it from data
        // shards alone — surface via the bounds-checked full-decode
        // path which returns Err with extent_id + sealed_length context.
        if start_shard >= data_shards || end_shard >= data_shards {
            return self
                .ec_read_full_and_slice(extent_id, offset, length, ex)
                .await;
        }

        let addrs = self.replica_addrs_for_extent(ex).await?;

        // Build the per-shard (offset, length) plan.
        let span = end_shard - start_shard + 1;
        let mut shard_plan: Vec<(usize, u64, u64)> = Vec::with_capacity(span);
        for shard_idx in start_shard..=end_shard {
            let (sh_off, sh_len) = if start_shard == end_shard {
                (start % shard_size, read_len)
            } else if shard_idx == start_shard {
                let off = start % shard_size;
                let len = shard_size - start % shard_size;
                (off, len)
            } else if shard_idx == end_shard {
                let len = end - shard_idx as u64 * shard_size;
                (0u64, len)
            } else {
                (0u64, shard_size)
            };
            shard_plan.push((shard_idx, sh_off, sh_len));
        }

        // F276: a data shard whose node the manager believes Suspected is
        // reconstructed from parity IMMEDIATELY — its direct shard read is NOT
        // issued, so the `join_all` below never blocks on a flaky node's full
        // RPC timeout before reconstruction starts (the user's "EC → 直接重新
        // 计算, 不用等请求超时"). Soft hint: if reconstruction can't gather K
        // healthy shards it still launches to every peer
        // (`ec_reconstruct_shard_subrange` races first-K-wins), so a mistaken
        // Suspected mark only costs a little extra parity traffic, never
        // correctness. With an empty snapshot every shard is read directly =
        // pre-F276 behavior.
        let node_ids = replica_node_ids(ex);
        let mut plan_results: Vec<Option<Vec<u8>>> = vec![None; shard_plan.len()];
        let mut needs_reconstruct: Vec<usize> = Vec::new();
        let mut to_read: Vec<usize> = Vec::with_capacity(shard_plan.len());
        for (i, &(shard_idx, _, _)) in shard_plan.iter().enumerate() {
            let nid = node_ids.get(shard_idx).copied().unwrap_or(0);
            if self.is_node_suspected(nid) {
                needs_reconstruct.push(i);
            } else {
                to_read.push(i);
            }
        }

        // Parallel scatter over the non-suspected shards only.
        // `read_shard_from_addr` borrows `&self`, which is fine because all
        // futures share the same self borrow that outlives this `await`.
        let read_futs: Vec<_> = to_read
            .iter()
            .map(|&i| {
                let (shard_idx, sh_off, sh_len) = shard_plan[i];
                let addr = &addrs[shard_idx];
                self.read_shard_from_addr(addr, extent_id, ex.eversion, sh_off, sh_len)
            })
            .collect();
        let results = futures::future::join_all(read_futs).await;

        // F200: failed (or F276 suspected-skipped) entries get reconstructed
        // via sub-range RS in the pass below — DO NOT fall back to
        // `ec_read_full_and_slice` (which would read 4 × full-shard payloads to
        // decode the whole extent just to slice out the requested sub-range).
        // The amplification triggered macOS-side df-probe timeouts under heavy
        // GC fanout, flapping disks offline (see F200 entry in feature_list.md).
        let mut last_end: u64 = 0;
        for (k, r) in results.into_iter().enumerate() {
            let i = to_read[k];
            match r {
                Ok((bytes, end_val)) => {
                    let want = shard_plan[i].2 as usize;
                    if bytes.len() != want {
                        // short read — treat same as failure; reconstruct.
                        needs_reconstruct.push(i);
                        continue;
                    }
                    plan_results[i] = Some(bytes);
                    last_end = end_val;
                }
                Err(e) => {
                    if is_eversion_stale(&e) {
                        return Err(anyhow::Error::new(EversionStale));
                    }
                    needs_reconstruct.push(i);
                }
            }
        }

        if !needs_reconstruct.is_empty() {
            // Sub-range RS reconstruction. For each failed plan entry,
            // read the same [sh_off, sh_len] window from K healthy
            // shards (the other data shards + parity) and call
            // `ec_reconstruct_shard` on K-aligned sub-shards. Bytes-on-
            // the-wire ≈ K × sh_len per failure, vs 4 × full-shard
            // for the old fall-back path.
            for &plan_idx in &needs_reconstruct {
                let (failing_shard_idx, sh_off, sh_len) = shard_plan[plan_idx];
                let recon = self
                    .ec_reconstruct_shard_subrange(
                        extent_id,
                        ex,
                        &addrs,
                        failing_shard_idx,
                        sh_off,
                        sh_len,
                    )
                    .await?;
                plan_results[plan_idx] = Some(recon);
            }
            // No `last_end` from reconstruction (the sub-range reads
            // didn't surface server-side `end` watermarks). Fall back
            // to the sealed_length as the safe overall end signal —
            // matches what `read_replicated_with_failover` would
            // return for a sealed extent.
            if last_end == 0 {
                last_end = ex.sealed_length;
            }
        }

        let mut data: Vec<u8> = Vec::with_capacity(read_len as usize);
        for (i, slot) in plan_results.into_iter().enumerate() {
            let bytes = slot.ok_or_else(|| {
                anyhow!(
                    "ec_subrange_read: plan entry {} unfilled after reconstruction (shard_idx={}, sh_off={}, sh_len={})",
                    i,
                    shard_plan[i].0,
                    shard_plan[i].1,
                    shard_plan[i].2,
                )
            })?;
            data.extend_from_slice(&bytes);
        }
        Ok((data, last_end))
    }

    /// F200: reconstruct one data shard's sub-range `[sh_off, sh_off + sh_len)`
    /// from K healthy peers (the other data shards + a parity shard).
    /// Replaces the old `ec_read_full_and_slice` fall-back for the case where
    /// a single per-shard sub-range read fails inside `ec_subrange_read`.
    ///
    /// Why sub-range reconstruction works: RS encodes byte-by-byte across
    /// the K data shards (galois_8). Decoding any single missing shard at
    /// row position `i` needs `data_shards` (K) byte values at the SAME row
    /// position from healthy peers. So a sub-range `[sh_off, sh_off + sh_len)`
    /// on the missing shard can be reconstructed from `[sh_off, sh_off + sh_len)`
    /// on K healthy shards — no full-extent decode needed.
    ///
    /// Bytes-on-the-wire: `K × sh_len` (e.g., 3 × 64 MiB = 192 MiB for the
    /// user's GC chunk read of a 3+1 extent), vs the pre-F200 fall-back
    /// of `(K + M) × shard_size(sealed_length, K)` (e.g., 4 × 933 MiB ≈
    /// 3.7 GiB for the same chunk on a 2.8 GiB extent). 20× reduction.
    ///
    /// Concurrency: launches reads to ALL `(K + M) - 1` non-missing
    /// peers in parallel and stops as soon as `K` succeed. Any single
    /// stale-eversion response short-circuits the whole call so the
    /// top-level `read_bytes_from_extent` 2-attempt loop can refetch
    /// `ExtentInfo` and retry against the fresh EC layout.
    async fn ec_reconstruct_shard_subrange(
        &self,
        extent_id: u64,
        ex: &ExtentInfo,
        addrs: &[String],
        missing_shard_idx: usize,
        sh_off: u64,
        sh_len: u64,
    ) -> Result<Vec<u8>> {
        let data_shards = ex.replicates.len();
        let parity_shards = ex.parity.len();
        let n = data_shards + parity_shards;

        if sh_len == 0 {
            return Ok(Vec::new());
        }
        if missing_shard_idx >= data_shards {
            return Err(anyhow!(
                "ec_reconstruct_shard_subrange: missing_shard_idx {missing_shard_idx} is in parity region (data_shards={data_shards}); refusing"
            ));
        }

        let (tx, mut rx) = futures::channel::mpsc::channel::<(usize, Result<Vec<u8>>)>(n);
        let cached_eversion = ex.eversion;
        for (i, addr) in addrs.iter().enumerate() {
            if i == missing_shard_idx {
                continue;
            }
            let mut tx_clone = tx.clone();
            let addr_clone = addr.clone();
            let pool = self.pool.clone();
            compio::runtime::spawn(async move {
                let req = ReadBytesReq {
                    extent_id,
                    eversion: cached_eversion,
                    offset: sh_off,
                    length: sh_len,
                };
                // 5 s — same shape as `ec_read_full`. Sub-range size
                // is `sh_len` (≤ chunk_size, typically 64 MiB), so
                // 5 s is generous even on a stressed loopback.
                let result: Result<Vec<u8>> = match pool
                    .call_timeout(
                        &addr_clone,
                        MSG_READ_BYTES,
                        req.encode(),
                        Duration::from_secs(5),
                    )
                    .await
                {
                    Ok(resp_bytes) => match ReadBytesResp::decode(resp_bytes) {
                        Ok(resp) if resp.code == CODE_OK => {
                            if resp.payload.len() as u64 != sh_len {
                                Err(anyhow!(
                                    "ec_reconstruct: short read from {addr_clone}: got {} want {sh_len}",
                                    resp.payload.len(),
                                ))
                            } else {
                                Ok(resp.payload.to_vec())
                            }
                        }
                        Ok(resp) if resp.code == CODE_EVERSION_MISMATCH => {
                            Err(anyhow::Error::new(EversionStale)
                                .context(format!("ec_reconstruct shard {i} from {addr_clone}")))
                        }
                        Ok(resp) => Err(anyhow!(
                            "ec_reconstruct read_bytes from {}: code={}",
                            addr_clone,
                            crate::extent_rpc::code_description(resp.code)
                        )),
                        Err(e) => Err(anyhow!("decode ReadBytesResp from {addr_clone}: {e}")),
                    },
                    Err(e) => Err(anyhow!(e)),
                };
                let _ = futures::SinkExt::send(&mut tx_clone, (i, result)).await;
            })
            .detach();
        }
        drop(tx);

        let mut shards: Vec<Option<Vec<u8>>> = vec![None; n];
        let mut success: usize = 0;
        let mut last_err =
            anyhow!("ec_reconstruct_shard_subrange: no shard responses for extent {extent_id}");
        while let Some((idx, result)) = futures::StreamExt::next(&mut rx).await {
            match result {
                Ok(bytes) => {
                    shards[idx] = Some(bytes);
                    success += 1;
                    if success >= data_shards {
                        break;
                    }
                }
                Err(e) => {
                    if is_eversion_stale(&e) {
                        return Err(e);
                    }
                    last_err = e;
                }
            }
        }

        if success < data_shards {
            return Err(last_err.context(format!(
                "ec_reconstruct_shard_subrange: only {success}/{data_shards} shards available for sub-range reconstruct (missing={missing_shard_idx}, sh_off={sh_off}, sh_len={sh_len})"
            )));
        }

        // RS reconstruction is CPU-bound; offload to blocking pool.
        let result = compio::runtime::spawn_blocking(move || {
            crate::erasure::ec_reconstruct_shard(
                shards,
                data_shards,
                parity_shards,
                missing_shard_idx,
            )
        })
        .await
        .map_err(|_| anyhow!("ec_reconstruct_shard task panicked"))??;

        Ok(result)
    }

    /// Decode the entire EC extent and slice `[offset, offset+length)`.
    ///
    /// Returns `Err` (instead of panicking) when `offset` is past the
    /// decoded payload length. The caller is responsible for surfacing
    /// the resulting short/missing read upstream.
    ///
    /// **Bug history**: pre-fix this used `full_payload[start..slice_end]`
    /// directly, which panics with `range start index N out of range
    /// for slice of length L` when `start > L`. That happened in
    /// production when log_stream extents were (incorrectly) EC-converted
    /// and a VP referenced an offset past the manager's recorded
    /// `sealed_length` — the panic took down the entire PS process.
    /// The error is now structured so `read_value_from_log` reports a
    /// "logStream value short" / out-of-bounds error to the caller and
    /// the partition keeps serving other requests.
    async fn ec_read_full_and_slice(
        &self,
        extent_id: u64,
        offset: u64,
        length: u64,
        ex: &ExtentInfo,
    ) -> Result<(Vec<u8>, u64)> {
        let (full_payload, end) = self.ec_read_full(extent_id, ex).await?;
        // F204: pass extent_id + sealed_length so `ec_slice_decoded`
        // can build a structured `StaleVpOffset` sentinel on
        // out-of-bounds. Pre-F204 we wrapped a stringy
        // `anyhow!("ec_read_full_and_slice: ...")` here — that erased
        // the downcast surface the PS layer relies on. Now the
        // sentinel surfaces unmodified; the PS `map_storage_error`
        // helper recognises it and returns `FailedPrecondition`
        // instead of `Internal`.
        let bytes = ec_slice_decoded(full_payload, offset, length, extent_id, ex.sealed_length)?;
        Ok((bytes, end))
    }

    async fn ec_read_full(&self, extent_id: u64, ex: &ExtentInfo) -> Result<(Vec<u8>, u64)> {
        let data_shards = ex.replicates.len();
        let parity_shards = ex.parity.len();
        let n = data_shards + parity_shards;

        let addrs = self.replica_addrs_for_extent(ex).await?;
        debug_assert_eq!(addrs.len(), n);

        let (tx, mut rx) = futures::channel::mpsc::channel::<(usize, Result<(Vec<u8>, u64)>)>(n);

        let cached_eversion = ex.eversion;
        for (i, addr) in addrs.into_iter().enumerate() {
            let mut tx = tx.clone();
            let pool = self.pool.clone();
            let delay = if i >= data_shards {
                Duration::from_millis(20)
            } else {
                Duration::ZERO
            };
            compio::runtime::spawn(async move {
                if !delay.is_zero() {
                    compio::time::sleep(delay).await;
                }
                let req = ReadBytesReq {
                    extent_id,
                    eversion: cached_eversion,
                    offset: 0,
                    length: 0,
                };
                // 5 s per shard — without this, a paged-out / dead EN
                // can keep its spawned task alive forever, and since
                // the outer `while rx.next().await` only exits when
                // either `success >= data_shards` OR all senders
                // drop, two slow shards in a K=3 EC layout would
                // wedge `ec_read_full` indefinitely. Observed in
                // production as 162 s VP-resolve latency on a single
                // Get when one EN was paged out by macOS.
                let result: Result<(Vec<u8>, u64)> = match pool
                    .call_timeout(&addr, MSG_READ_BYTES, req.encode(), Duration::from_secs(5))
                    .await
                {
                    Ok(resp_bytes) => match ReadBytesResp::decode(resp_bytes) {
                        Ok(resp) if resp.code == CODE_OK => Ok((resp.payload.to_vec(), resp.end)),
                        Ok(resp) if resp.code == CODE_EVERSION_MISMATCH => {
                            Err(anyhow::Error::new(EversionStale)
                                .context(format!("ec_read_full shard {i} from {addr}")))
                        }
                        Ok(resp) => Err(anyhow!(
                            "read_bytes from {}: code={}",
                            addr,
                            crate::extent_rpc::code_description(resp.code)
                        )),
                        Err(e) => Err(anyhow!("decode ReadBytesResp from {addr}: {e}")),
                    },
                    Err(e) => Err(anyhow!(e)),
                };
                let _ = futures::SinkExt::send(&mut tx, (i, result)).await;
            })
            .detach();
        }
        drop(tx);

        let mut shard_data: Vec<Option<Vec<u8>>> = vec![None; n];
        let mut end_val: Option<u64> = None;
        let mut success = 0usize;
        let mut last_err = anyhow!("no shard responses for extent {}", extent_id);

        while let Some((idx, result)) = futures::StreamExt::next(&mut rx).await {
            match result {
                Ok((data, end)) => {
                    shard_data[idx] = Some(data);
                    end_val = Some(end);
                    success += 1;
                    if success >= data_shards {
                        break;
                    }
                }
                Err(e) => {
                    // Any single shard reporting stale eversion means
                    // the cached EC layout is wrong; bail out so the
                    // top-level retry can refetch ExtentInfo.
                    if is_eversion_stale(&e) {
                        return Err(e);
                    }
                    last_err = e;
                }
            }
        }

        if success < data_shards {
            return Err(last_err.context(format!(
                "only {success}/{data_shards} shards available for EC decode"
            )));
        }

        // F117: RS decode of a full extent (up to 128 MiB) is CPU-bound;
        // run it on the blocking pool so the caller's compio thread (P-log
        // / P-bulk / extent-node read fanout) stays responsive while the
        // GF(256) math runs. `sealed_length` is the authoritative payload
        // length (no in-shard trailer); decode truncates the data shards to it.
        let original_size = ex.sealed_length as usize;
        let decoded = compio::runtime::spawn_blocking(move || {
            crate::erasure::ec_decode(shard_data, data_shards, parity_shards, original_size)
        })
        .await
        .map_err(|_| anyhow!("ec_decode task panicked"))??;
        Ok((decoded, end_val.unwrap()))
    }

    /// Read all bytes from the last non-empty extent of the given stream.
    /// Returns None if the stream has no data.
    pub async fn read_last_extent_data(&self, stream_id: u64) -> Result<Option<Vec<u8>>> {
        let info = self.get_stream_info(stream_id).await?;
        for &eid in info.extent_ids.iter().rev() {
            let (payload, _end) = self.read_bytes_from_extent(eid, 0, 0).await?;
            if !payload.is_empty() {
                return Ok(Some(payload));
            }
        }
        Ok(None)
    }

    pub async fn multi_modify_split(
        &self,
        mid_key: Vec<u8>,
        part_id: u64,
        sealed_lengths: [u64; 3],
        timeout: Duration,
    ) -> Result<()> {
        let req = manager_rpc::rkyv_encode(&MultiModifySplitReq {
            part_id,
            owner_key: self.owner_key.clone(),
            owner_epoch: self.owner_epoch,
            mid_key,
            log_stream_sealed_length: sealed_lengths[0],
            row_stream_sealed_length: sealed_lengths[1],
            meta_stream_sealed_length: sealed_lengths[2],
        });
        // Per-call timeout is caller-chosen (#6): the PS split path bounds it
        // SHORT so the whole freeze critical section stays under FREEZE_TTL —
        // a split that committed AFTER the freeze lapsed would seal the
        // log_stream at a stale commit_length and lose the writes that resumed
        // post-unfreeze. The manager runs the split's atomic etcd txn (alloc 4
        // ids + duplicate 3 streams + create partition + update regions +
        // last_op_at sidecar).
        let resp_data = self
            .manager_call(MSG_MULTI_MODIFY_SPLIT, req, timeout)
            .await?;
        let resp: CodeResp = manager_rpc::rkyv_decode(&resp_data).map_err(|e| anyhow!("{e}"))?;
        self.check_manager_resp(resp.code, &resp.message, "multi_modify_split")?;
        Ok(())
    }

    // ── Mgr→local type conversion helpers ───────────────────────────────

    fn mgr_to_stream_info(s: &MgrStreamInfo) -> StreamInfo {
        StreamInfo {
            stream_id: s.stream_id,
            extent_ids: s.extent_ids.clone(),
            ec_data_shard: s.ec_data_shard,
            ec_parity_shard: s.ec_parity_shard,
        }
    }

    fn mgr_to_extent_info(e: &MgrExtentInfo) -> ExtentInfo {
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
        }
    }
}

#[cfg(test)]
mod pipeline_tests {
    use super::*;

    #[test]
    fn lease_no_collision() {
        let mut state = StreamAppendState::new(
            Rc::new(RefCell::new(HashMap::new())),
            mpsc::channel::<FailureReport>(1).0,
            Duration::from_secs(30),
        );
        let (o0, e0) = state.lease(100);
        let (o1, e1) = state.lease(200);
        let (o2, e2) = state.lease(50);
        assert_eq!(o0, 0);
        assert_eq!(e0, 100);
        assert_eq!(o1, 100);
        assert_eq!(e1, 300);
        assert_eq!(o2, 300);
        assert_eq!(e2, 350);
        assert_eq!(state.in_flight, 3);
        assert_eq!(state.lease_cursor, 350);
    }

    #[test]
    fn ack_advances_commit_on_prefix() {
        let mut state = StreamAppendState::new(
            Rc::new(RefCell::new(HashMap::new())),
            mpsc::channel::<FailureReport>(1).0,
            Duration::from_secs(30),
        );
        let (o0, e0) = state.lease(100); // 0..100
        let (o1, e1) = state.lease(100); // 100..200
        let (o2, e2) = state.lease(100); // 200..300

        state.ack(o1, e1, None);
        assert_eq!(state.commit, 0);
        assert_eq!(state.in_flight, 2);
        assert!(state.pending_acks.contains_key(&100));

        state.ack(o2, e2, None);
        assert_eq!(state.commit, 0);
        assert_eq!(state.in_flight, 1);

        state.ack(o0, e0, None);
        assert_eq!(state.commit, 300);
        assert_eq!(state.in_flight, 0);
        assert!(state.pending_acks.is_empty());
    }

    fn test_state() -> StreamAppendState {
        StreamAppendState::new(
            Rc::new(RefCell::new(HashMap::new())),
            mpsc::channel::<FailureReport>(1).0,
            Duration::from_secs(30),
        )
    }

    fn ack_payload(
        offset: u64,
        end: u64,
    ) -> (
        oneshot::Receiver<Result<AppendResult>>,
        Option<(oneshot::Sender<Result<AppendResult>>, AppendResult)>,
    ) {
        let (tx, rx) = oneshot::channel();
        (
            rx,
            Some((
                tx,
                AppendResult {
                    extent_id: 1,
                    offset,
                    end,
                },
            )),
        )
    }

    /// ENOSPC-1 P1: caller acks fire only when the contiguous prefix
    /// covers the range — an out-of-order completion is HELD, not acked.
    #[test]
    fn caller_ack_deferred_until_contiguous() {
        let mut state = test_state();
        let (o0, e0) = state.lease(100); // 0..100
        let (o1, e1) = state.lease(100); // 100..200

        let (mut rx1, p1) = ack_payload(o1, e1);
        state.ack(o1, e1, p1);
        assert_eq!(state.commit, 0);
        assert!(
            rx1.try_recv().expect("sender alive").is_none(),
            "ack for 100..200 fired before 0..100 completed"
        );

        let (mut rx0, p0) = ack_payload(o0, e0);
        state.ack(o0, e0, p0);
        assert_eq!(state.commit, 200);
        assert!(rx0.try_recv().unwrap().unwrap().is_ok());
        assert!(rx1.try_recv().unwrap().unwrap().is_ok());
    }

    /// ENOSPC-1 P1 (the seal-chop bug): a completion ABOVE a failed lease
    /// must be FAILED to the caller — pre-fix it acked Ok while `commit`
    /// (the seal watermark) stayed below it, so the roll's seal discarded
    /// an acked range. Covers both orders: completion-then-failure
    /// (pending drained) and failure-then-completion (floor check).
    #[test]
    fn completion_above_failed_lease_is_failed_not_acked() {
        // Order 1: B completes first, then A fails -> B's pending ack
        // must drain as Err.
        let mut state = test_state();
        let (a_off, a_end) = state.lease(100); // 0..100   (A)
        let (b_off, b_end) = state.lease(100); // 100..200 (B)
        let _ = state.lease(50); // 200..250 keeps A's rewind path off
        let (mut rx_b, p_b) = ack_payload(b_off, b_end);
        state.ack(b_off, b_end, p_b);
        assert!(rx_b.try_recv().unwrap().is_none(), "B acked early");
        state.rewind_or_poison(a_off, a_end - a_off); // A fails (hole)
        assert!(state.poisoned);
        let b = rx_b.try_recv().unwrap().expect("B must be resolved");
        assert!(b.is_err(), "B acked Ok above a hole (would be seal-chopped)");
        assert_eq!(state.commit, 0, "commit must not cover the hole");

        // Order 2: A fails first, then B completes -> floor check fails B.
        let mut state = test_state();
        let (a_off, a_end) = state.lease(100);
        let (b_off, b_end) = state.lease(100);
        let _ = state.lease(50);
        state.rewind_or_poison(a_off, a_end - a_off);
        let (mut rx_b, p_b) = ack_payload(b_off, b_end);
        state.ack(b_off, b_end, p_b);
        let b = rx_b.try_recv().unwrap().expect("B must be resolved");
        assert!(b.is_err(), "late completion above the hole acked Ok");

        // Ranges BELOW the hole still ack normally.
        let mut state = test_state();
        let (a_off, a_end) = state.lease(100); // 0..100
        let (b_off, b_end) = state.lease(100); // 100..200 - will fail
        let (c_off, c_end) = state.lease(100); // 200..300
        state.rewind_or_poison(b_off, b_end - b_off); // poison via gap
        let (mut rx_c, p_c) = ack_payload(c_off, c_end);
        state.ack(c_off, c_end, p_c);
        assert!(rx_c.try_recv().unwrap().unwrap().is_err(), "C above hole");
        let (mut rx_a, p_a) = ack_payload(a_off, a_end);
        state.ack(a_off, a_end, p_a);
        assert!(
            rx_a.try_recv().unwrap().unwrap().is_ok(),
            "A below the hole must still ack"
        );
        assert_eq!(state.commit, 100, "commit advances up to the hole only");
    }

    #[test]
    fn rewind_on_error_most_recent() {
        let mut state = StreamAppendState::new(
            Rc::new(RefCell::new(HashMap::new())),
            mpsc::channel::<FailureReport>(1).0,
            Duration::from_secs(30),
        );
        let (o0, _e0) = state.lease(100);
        let (o1, _e1) = state.lease(200);
        assert_eq!(state.lease_cursor, 300);
        assert_eq!(state.in_flight, 2);

        state.rewind_or_poison(o1, 200);
        assert_eq!(state.lease_cursor, 100);
        assert_eq!(state.in_flight, 1);
        assert!(!state.poisoned);

        let (o2, e2) = state.lease(50);
        assert_eq!(o2, 100);
        assert_eq!(e2, 150);

        let _ = o0;
    }

    #[test]
    fn poison_on_error_mid_sequence() {
        let mut state = StreamAppendState::new(
            Rc::new(RefCell::new(HashMap::new())),
            mpsc::channel::<FailureReport>(1).0,
            Duration::from_secs(30),
        );
        let (o0, _) = state.lease(100);
        let (_, _) = state.lease(200);
        assert_eq!(state.in_flight, 2);

        state.rewind_or_poison(o0, 100);
        assert!(state.poisoned);
        assert_eq!(state.lease_cursor, 300);
        assert_eq!(state.in_flight, 1);
    }

    /// BUG#2 (seed=8) regression: a soft-error tail reload onto the SAME open
    /// extent must preserve the worker's append-progress state; only a roll to
    /// a DIFFERENT extent resets it. Zeroing `commit` on a same-extent reload
    /// let a later `seal_commit_watermark` report commit=0 and seal the live
    /// tail at sealed_length=0, orphaning acked VP/SST data (split child then
    /// un-openable: stale_vp_offset_past_sealed_length).
    #[test]
    fn reset_tail_same_extent_preserves_commit_and_poison() {
        let mut state = StreamAppendState::new(
            Rc::new(RefCell::new(HashMap::new())),
            mpsc::channel::<FailureReport>(1).0,
            Duration::from_secs(30),
        );
        let tail = |eid: u64| StreamTail {
            extent: ExtentInfo {
                extent_id: eid,
                ..Default::default()
            },
            replica_addrs: vec!["a".to_string()],
            replica_node_ids: vec![1],
        };

        // Worker on extent 18 with acked data + a marked hole + sealing freeze.
        state.apply_reset_tail(tail(18));
        state.commit = 8_657_884;
        state.lease_cursor = 8_657_884;
        state.poisoned = true;
        state.sealing = true;

        // Same-extent reload (transient replica failure) → preserve everything.
        state.apply_reset_tail(tail(18));
        assert_eq!(
            state.commit, 8_657_884,
            "same-extent reload must preserve commit (the all-replica-acked prefix)"
        );
        assert_eq!(state.lease_cursor, 8_657_884);
        assert!(
            state.poisoned,
            "same-extent reload must preserve the hole marker"
        );
        assert!(
            state.sealing,
            "same-extent reload must preserve the sealing freeze"
        );
        assert_eq!(state.tail.as_ref().unwrap().extent.extent_id, 18);

        // Genuine roll to a DIFFERENT (fresh, empty) extent → full reset.
        state.apply_reset_tail(tail(30));
        assert_eq!(state.commit, 0, "new extent must reset commit to 0");
        assert_eq!(state.lease_cursor, 0);
        assert!(!state.poisoned);
        assert!(!state.sealing);
        assert_eq!(state.tail.as_ref().unwrap().extent.extent_id, 30);
    }
}

#[cfg(test)]
mod f190_bad_nodes_tests {
    //! F190: per-stream bad_nodes exclusion.
    //!
    //! Covers (a) insert + immediate snapshot returns the node id,
    //! (b) refresh-on-insert preserves only the latest expires_at, and
    //! (c) a snapshot called after the TTL has elapsed prunes the
    //! entry (verified by overriding `AUTUMN_STREAM_BAD_NODES_TTL_SECS`
    //! to 1 s and sleeping). The slow case is gated behind a
    //! `#[ignore]` so the default unit run stays sub-second.
    use super::*;

    #[test]
    fn mark_then_inspect_returns_node() {
        let bad = Rc::new(RefCell::new(HashMap::new()));
        let state = StreamAppendState::new(
            bad.clone(),
            mpsc::channel::<FailureReport>(1).0,
            Duration::from_secs(30),
        );
        state.mark_bad_node(7);
        let entries = bad.borrow();
        assert!(entries.contains_key(&7));
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn mark_refreshes_existing_entry() {
        let bad = Rc::new(RefCell::new(HashMap::new()));
        let state = StreamAppendState::new(
            bad.clone(),
            mpsc::channel::<FailureReport>(1).0,
            Duration::from_secs(30),
        );
        state.mark_bad_node(11);
        let first = *bad.borrow().get(&11).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        state.mark_bad_node(11);
        let second = *bad.borrow().get(&11).unwrap();
        assert!(second > first);
        assert_eq!(bad.borrow().len(), 1);
    }

    #[test]
    fn snapshot_prunes_expired_entries_in_place() {
        let map: HashMap<u64, Instant> = [
            (1u64, Instant::now() + Duration::from_secs(60)),
            (2u64, Instant::now() - Duration::from_millis(1)),
        ]
        .into_iter()
        .collect();
        let rc = Rc::new(RefCell::new(map));
        // Inline the snapshot semantics from `StreamClient::snapshot_bad_nodes`
        // — pruning is in-place on borrow_mut so the map can never grow.
        let now = Instant::now();
        let mut entries = rc.borrow_mut();
        entries.retain(|_, expires_at| *expires_at > now);
        let mut snap: Vec<u64> = entries.keys().copied().collect();
        snap.sort();
        assert_eq!(snap, vec![1u64]);
    }
}

#[cfg(test)]
mod selfheal_avali_filter_tests {
    //! WAL self-heal A1: `eligible_replica_slots` isolates avali=0 slots on a
    //! SEALED replicated extent (the read-path isolation that makes "clear the
    //! avali bit" actually remove a bit-rotted replica from the serving set),
    //! while leaving OPEN extents and the all-clear defensive case unfiltered.
    use super::eligible_replica_slots;
    use crate::extent_rpc::ExtentInfo;

    fn ext(sealed: bool, n_repl: usize, avali: u32) -> ExtentInfo {
        ExtentInfo {
            extent_id: 7,
            replicates: (0..n_repl as u64).collect(),
            parity: Vec::new(),
            sealed,
            avali,
            ..Default::default()
        }
    }

    #[test]
    fn sealed_excludes_avali_zero_slot() {
        // 3 replicas, slot 0's avali bit cleared (isolated corrupt) → only 1,2.
        let e = ext(true, 3, 0b110);
        assert_eq!(eligible_replica_slots(&e), vec![1, 2]);
        // slot 1 cleared → 0,2.
        assert_eq!(eligible_replica_slots(&ext(true, 3, 0b101)), vec![0, 2]);
        // all healthy → all.
        assert_eq!(eligible_replica_slots(&ext(true, 3, 0b111)), vec![0, 1, 2]);
    }

    #[test]
    fn open_extent_is_never_filtered() {
        // avali=0 on an OPEN extent is the normal "not sealed" state, not
        // "all replicas bad" — must read all of them.
        assert_eq!(eligible_replica_slots(&ext(false, 3, 0)), vec![0, 1, 2]);
    }

    #[test]
    fn sealed_all_clear_falls_back_to_all() {
        // Defensive: a sealed extent with EVERY avali bit clear shouldn't
        // happen (seal sets all_bits); never refuse all reads — fall back.
        assert_eq!(eligible_replica_slots(&ext(true, 3, 0)), vec![0, 1, 2]);
    }
}

#[cfg(test)]
mod f276_suspected_read_tests {
    //! F276: the READ path routes around manager-`Suspected` nodes. Replicated
    //! reads deprioritize suspected slots to the BACK (healthy-first, never
    //! dropped); EC reads reconstruct a suspected data shard from parity
    //! instead of issuing a doomed direct read. These cover the pure decision
    //! fn `replicated_read_order` + the `replicas ++ parity` slot→node mapping.
    use super::{
        healthy_eligible_slots, replica_node_ids, replicated_read_order, rotated_replica_start,
    };
    use crate::extent_rpc::ExtentInfo;
    use std::collections::HashSet;

    fn ext(replicates: Vec<u64>, parity: Vec<u64>, sealed: bool, avali: u32) -> ExtentInfo {
        ExtentInfo {
            extent_id: 42,
            replicates,
            parity,
            sealed,
            avali,
            ..Default::default()
        }
    }

    #[test]
    fn replica_node_ids_is_replicates_then_parity() {
        let ex = ext(vec![10, 20, 30], vec![40], false, 0);
        assert_eq!(replica_node_ids(&ex), vec![10, 20, 30, 40]);
    }

    #[test]
    fn empty_suspected_preserves_rotated_order() {
        // Common case: an empty Suspected snapshot must reproduce the pre-F276
        // rotated order exactly (open extent → start 0 → 0,1,2).
        let ex = ext(vec![10, 20, 30], vec![], false, 0);
        let empty = HashSet::new();
        assert_eq!(replicated_read_order(&ex, 0, &empty), vec![0, 1, 2]);
    }

    #[test]
    fn suspected_slot_moves_to_back_preserving_order() {
        // node 20 (slot 1) suspected → tried last: [0, 2, 1].
        let ex = ext(vec![10, 20, 30], vec![], false, 0);
        let s: HashSet<u64> = [20].into_iter().collect();
        assert_eq!(replicated_read_order(&ex, 0, &s), vec![0, 2, 1]);
    }

    #[test]
    fn all_suspected_still_returns_every_slot() {
        // Soft hint: never strand a read. Every node suspected → the full set
        // is still returned (all in the flaky tail), so a stale/over-broad
        // snapshot degrades latency, never availability.
        let ex = ext(vec![10, 20, 30], vec![], false, 0);
        let s: HashSet<u64> = [10, 20, 30].into_iter().collect();
        let order = replicated_read_order(&ex, 0, &s);
        assert_eq!(order.len(), 3);
        let set: HashSet<usize> = order.into_iter().collect();
        assert_eq!(set, [0, 1, 2].into_iter().collect::<HashSet<usize>>());
    }

    #[test]
    fn avali_isolated_slot_dropped_then_suspected_ordered() {
        // Sealed extent, slot 1 avali bit clear → excluded entirely (self-heal
        // I2), independent of Suspected. Remaining eligible slots {0,2}.
        let ex = ext(vec![10, 20, 30], vec![], true, 0b101);
        let empty = HashSet::new();
        let order = replicated_read_order(&ex, 0, &empty);
        let set: HashSet<usize> = order.iter().copied().collect();
        assert_eq!(set, [0, 2].into_iter().collect::<HashSet<usize>>());
        assert!(!order.contains(&1), "avali-isolated slot must never be read");
    }

    #[test]
    fn healthy_eligible_drops_suspected_with_fallback() {
        // Client-direct path: a Suspected node is EXCLUDED (the client rotates,
        // so ordering wouldn't route around it).
        let ex = ext(vec![10, 20, 30], vec![], false, 0);
        let s: HashSet<u64> = [20].into_iter().collect();
        assert_eq!(healthy_eligible_slots(&ex, &s), vec![0, 2]);
        // Empty snapshot → exactly eligible_replica_slots (no behavior change).
        assert_eq!(
            healthy_eligible_slots(&ex, &HashSet::new()),
            vec![0, 1, 2]
        );
        // ALL eligible Suspected → fall back to all (never strand the read).
        let all: HashSet<u64> = [10, 20, 30].into_iter().collect();
        assert_eq!(healthy_eligible_slots(&ex, &all), vec![0, 1, 2]);
    }

    #[test]
    fn healthy_eligible_respects_avali_isolation() {
        // Sealed, slot 1 avali bit clear → excluded by avali; node 10 (slot 0)
        // Suspected → excluded by F276; only slot 2 left.
        let ex = ext(vec![10, 20, 30], vec![], true, 0b101);
        let s: HashSet<u64> = [10].into_iter().collect();
        assert_eq!(healthy_eligible_slots(&ex, &s), vec![2]);
    }

    #[test]
    fn suspected_rotated_first_slot_tried_last_on_sealed() {
        // Sealed extent, all avali set: whichever slot the F258 rotation would
        // start on, if its node is Suspected it must end up LAST.
        let ex = ext(vec![10, 20, 30], vec![40], true, 0b1111);
        let start = rotated_replica_start(ex.extent_id, 0, 4);
        let node_ids = replica_node_ids(&ex);
        let s: HashSet<u64> = [node_ids[start]].into_iter().collect();
        let order = replicated_read_order(&ex, 0, &s);
        assert_eq!(order.len(), 4);
        assert_eq!(
            *order.last().unwrap(),
            start,
            "suspected rotated-first slot must be tried last"
        );
    }
}

#[cfg(test)]
mod f190_wire_compat_tests {
    //! F190: backwards-compat smoke for `StreamAllocExtentReq`. An empty
    //! `exclude_node_ids` field must round-trip through rkyv as a
    //! zero-length Vec — equivalent semantics to the pre-F190 wire (no
    //! filter applied on the manager side via the fall-back-on-empty
    //! branch in `select_nodes`).
    use autumn_rpc::manager_rpc::{rkyv_decode, rkyv_encode, StreamAllocExtentReq};

    #[test]
    fn empty_exclude_round_trips() {
        let req = StreamAllocExtentReq {
            stream_id: 42,
            owner_key: "ps/0/partition/3".to_string(),
            owner_epoch: 7,
            seal_commit: None,
            exclude_node_ids: Vec::new(),
                seal_extent_id: 0,
        };
        let bytes = rkyv_encode(&req);
        let back: StreamAllocExtentReq = rkyv_decode(&bytes).expect("decode");
        assert_eq!(back.stream_id, 42);
        assert_eq!(back.owner_key, "ps/0/partition/3");
        assert_eq!(back.owner_epoch, 7);
        assert_eq!(back.seal_commit, None);
        assert!(back.exclude_node_ids.is_empty());
    }

    #[test]
    fn populated_exclude_round_trips() {
        let req = StreamAllocExtentReq {
            stream_id: 1,
            owner_key: String::new(),
            owner_epoch: 0,
            seal_commit: Some(1024),
            exclude_node_ids: vec![3, 5, 9101],
                seal_extent_id: 0,
        };
        let bytes = rkyv_encode(&req);
        let back: StreamAllocExtentReq = rkyv_decode(&bytes).expect("decode");
        assert_eq!(back.seal_commit, Some(1024));
        assert_eq!(back.exclude_node_ids, vec![3, 5, 9101]);
    }
}

#[cfg(test)]
mod ec_slice_tests {
    //! Regression tests for the slice-bounds bug that crashed the
    //! partition server when a VP referenced an offset past the
    //! EC-decoded payload length. Pre-fix the slicing was
    //! `full_payload[start..slice_end]` with `start > full_payload.len()`,
    //! which panics with `range start index N out of range for slice of
    //! length L` and unwound the entire partition thread.
    use super::{ec_slice_decoded, StaleVpOffset};

    /// Test fixture: arbitrary extent_id + sealed_length context that
    /// ec_slice_decoded threads through into the F204 sentinel on
    /// out-of-bounds. The happy-path tests pass arbitrary values
    /// because the data flow doesn't use them when offset is in range.
    const TEST_EXTENT: u64 = 42;

    #[test]
    fn slice_in_range_returns_subslice() {
        let payload: Vec<u8> = (0u8..=199).collect();
        let expected = payload[50..80].to_vec();
        let out = ec_slice_decoded(payload, 50, 30, TEST_EXTENT, 200).expect("in-range slice");
        assert_eq!(out.len(), 30);
        assert_eq!(out, expected);
    }

    #[test]
    fn slice_zero_length_means_to_end() {
        let payload: Vec<u8> = (0u8..=199).collect();
        let expected = payload[50..].to_vec();
        let out = ec_slice_decoded(payload, 50, 0, TEST_EXTENT, 200).expect("to-end slice");
        assert_eq!(out, expected);
    }

    #[test]
    fn slice_offset_past_end_returns_err_not_panic() {
        // Reproduces the production crash: VP offset 49541652 on a
        // payload of length 45479123. Pre-fix this slicing path
        // panicked; post-fix it must surface a clean Err so the
        // caller can convert it into a "value short" RPC response and
        // the partition keeps serving other requests.
        let payload = vec![0u8; 45_479_123];
        let err = ec_slice_decoded(payload, 49_541_652, 14_456_954, 7, 45_479_123)
            .expect_err("offset past end must be rejected");
        // F204: must be the structured sentinel, not just a stringy error.
        let stale = err
            .chain()
            .find_map(|c| c.downcast_ref::<StaleVpOffset>())
            .expect("error chain must contain StaleVpOffset");
        assert_eq!(stale.extent_id, 7);
        assert_eq!(stale.requested_offset, 49_541_652);
        assert_eq!(stale.requested_length, 14_456_954);
        assert_eq!(stale.sealed_length, 45_479_123);
        // Display contract for Python regex consumers (memory:
        // feedback-ops-tools-in-python).
        let msg = stale.to_string();
        assert!(
            msg.starts_with("stale_vp_offset_past_sealed_length:"),
            "unexpected prefix: {msg}"
        );
        assert!(msg.contains("extent=7"));
        assert!(msg.contains("offset=49541652"));
        assert!(msg.contains("length=14456954"));
        assert!(msg.contains("sealed_length=45479123"));
    }

    #[test]
    fn slice_offset_at_end_returns_empty() {
        // offset == len is the boundary: no bytes to read, but not an
        // error. Required for callers that pass `offset = sealed_length`
        // and `length = 0` to mean "nothing left".
        let payload = vec![0u8; 100];
        let out = ec_slice_decoded(payload, 100, 0, TEST_EXTENT, 100).expect("offset==len is OK");
        assert!(out.is_empty());
    }

    #[test]
    fn slice_length_overshoots_clamps_to_end() {
        let payload: Vec<u8> = (0u8..=99).collect();
        let expected = payload[80..].to_vec();
        // Asking for 999 bytes from offset 80 should return 20.
        let out = ec_slice_decoded(payload, 80, 999, TEST_EXTENT, 100).expect("clamped slice");
        assert_eq!(out.len(), 20);
        assert_eq!(out, expected);
    }

    #[test]
    fn slice_full_read_is_zero_copy() {
        // F170 invariant: offset=0,length=0 returns the input Vec
        // by ownership transfer (no allocation). This test verifies
        // that the returned Vec's capacity matches the input — if a
        // memcpy slipped in, the new Vec would have shrunken capacity.
        let mut payload = Vec::with_capacity(1024);
        payload.extend_from_slice(&(0u8..=199).collect::<Vec<u8>>());
        let in_ptr = payload.as_ptr();
        let in_cap = payload.capacity();
        let out = ec_slice_decoded(payload, 0, 0, TEST_EXTENT, 200).expect("full read");
        assert_eq!(out.as_ptr(), in_ptr, "full-read must NOT memcpy");
        assert_eq!(out.capacity(), in_cap, "capacity preserved → no realloc");
    }
}

#[cfg(test)]
mod f258_rotation_tests {
    use super::rotated_replica_start;

    #[test]
    fn deterministic_and_in_range() {
        for eid in [1u64, 42, 7_000_000] {
            for off in [0u64, 4096, 64 << 20] {
                for n in [1usize, 2, 3, 5] {
                    let a = rotated_replica_start(eid, off, n);
                    let b = rotated_replica_start(eid, off, n);
                    assert_eq!(a, b, "deterministic");
                    assert!(a < n, "in range: {a} < {n}");
                }
            }
        }
    }

    #[test]
    fn n1_always_zero() {
        assert_eq!(rotated_replica_start(99, 12345, 1), 0);
        assert_eq!(rotated_replica_start(99, 12345, 0), 0);
    }

    #[test]
    fn spreads_across_replicas() {
        // Consecutive 64 MiB chunk offsets of one large extent must not all
        // land on the same start replica, and across many extents the
        // distribution must cover every replica.
        let n = 3usize;
        let mut seen = [0usize; 3];
        for eid in 0..32u64 {
            for chunk in 0..8u64 {
                seen[rotated_replica_start(eid, chunk * (64 << 20), n)] += 1;
            }
        }
        let total: usize = seen.iter().sum();
        assert_eq!(total, 32 * 8);
        for (i, &c) in seen.iter().enumerate() {
            assert!(
                c > total / 6,
                "replica {i} underused: {c}/{total} (want roughly even spread)"
            );
        }
    }
}

/// F258: unwrap a hedged-read oneshot result. `Canceled` means the spawned
/// read task was dropped before sending (runtime teardown) — surfaced as a
/// plain error so the failover loop continues.
fn flatten_hedge(
    res: std::result::Result<Result<(Vec<u8>, u64)>, futures::channel::oneshot::Canceled>,
) -> Result<(Vec<u8>, u64)> {
    match res {
        Ok(r) => r,
        Err(_) => Err(anyhow!("hedged read task canceled before completion")),
    }
}

/// MERGE-EC-REPLAY regression (commit c56a20c, 2026-06-16).
///
/// `read_committed_bytes_from_extent` must report the extent's OWN
/// authoritative `committed_end` (`sealed_length` for a sealed extent),
/// NOT the second element of `read_with_layout`'s return tuple. For an
/// EC-converted extent `ec_subrange_read` surfaces a SHARD-relative end
/// (≈ `sealed_length / K`); the pre-fix `Ok(r) => return Ok(r)` propagated
/// it verbatim, so the PS WAL replay — which uses this value as its stop
/// bound — read only ONE shard's worth of bytes and tripped WAL-FAILSTOP
/// at the shard boundary, permanently wedging recovery of any partition
/// whose VP-head replay window reached an EC log extent.
///
/// This exercises the real read path end-to-end against in-process extent
/// nodes holding RS shards: it asserts the reported end equals the logical
/// `sealed_length` (not the per-shard length) AND that the bytes decode to
/// the original payload (the bug was the END only — bytes were already
/// clamped correctly).
#[cfg(test)]
mod merge_ec_replay_tests {
    use super::*;
    use crate::extent_rpc::{
        rkyv_decode, AllocExtentReq, AllocExtentResp, CommitEcShardReq, CommitEcShardResp,
        WriteShardReq, WriteShardResp, MSG_ALLOC_EXTENT, MSG_COMMIT_EC_SHARD, MSG_WRITE_SHARD,
    };

    fn pick_addr() -> std::net::SocketAddr {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let a = l.local_addr().expect("local_addr");
        drop(l);
        a
    }

    async fn start_node(dir: &std::path::Path, addr: std::net::SocketAddr) {
        let config = crate::ExtentNodeConfig::new(dir.to_path_buf(), 1);
        let node = crate::ExtentNode::new(config)
            .await
            .expect("create ExtentNode");
        compio::runtime::spawn(async move {
            let _ = node.serve(addr).await;
        })
        .detach();
        compio::time::sleep(Duration::from_millis(120)).await;
    }

    #[compio::test]
    async fn ec_committed_end_is_sealed_length_not_shard_length() {
        const K: usize = 3; // data shards
        const M: usize = 1; // parity shards
        const N: usize = K + M;
        // Logical payload length. `shard_size` = ceil(L/K), strictly smaller
        // than the logical length — that gap is exactly what the bug confused
        // for the end. (6144/3 = 2048 exactly.)
        const L: usize = 6144;
        const EVERSION: u64 = 5; // post-EC target eversion
        let extent_id: u64 = 70_001;
        let node_ids: [u64; N] = [1, 2, 3, 4];

        // Bring up N in-process extent nodes.
        let dirs: Vec<_> = (0..N)
            .map(|_| tempfile::tempdir().expect("tempdir"))
            .collect();
        let addrs: Vec<std::net::SocketAddr> = (0..N).map(|_| pick_addr()).collect();
        for i in 0..N {
            start_node(dirs[i].path(), addrs[i]).await;
        }

        // RS-encode a deterministic payload into K data + M parity shards.
        let payload: Vec<u8> = (0..L).map(|i| (i % 251) as u8).collect();
        let shards = crate::erasure::ec_encode(&payload, K, M).expect("ec_encode");
        assert_eq!(shards.len(), N);
        let shard_size = crate::erasure::shard_size(L, K);
        assert!(
            shard_size < L,
            "shard ({shard_size}) must be strictly smaller than logical ({L})"
        );

        // Distribute one EC shard per node: alloc → write_shard (prepare) →
        // commit_ec_shard (commit). Mirrors the manager's EC-convert 2PC.
        let pool = Rc::new(ConnPool::new());
        for i in 0..N {
            let addr = addrs[i].to_string();

            let alloc = pool
                .call(
                    &addr,
                    MSG_ALLOC_EXTENT,
                    crate::extent_rpc::rkyv_encode(&AllocExtentReq { extent_id }),
                )
                .await
                .expect("alloc_extent RPC");
            assert_eq!(
                rkyv_decode::<AllocExtentResp>(&alloc).expect("decode").code,
                CODE_OK
            );

            let ws = WriteShardReq {
                extent_id,
                shard_index: i as u32,
                sealed_length: L as u64,
                eversion: EVERSION,
                owner_epoch: 0,
                shard_offset: 0,
                payload: Bytes::from(shards[i].clone()),
            };
            let ws_resp = pool
                .call(&addr, MSG_WRITE_SHARD, ws.encode())
                .await
                .expect("write_shard RPC");
            assert_eq!(WriteShardResp::decode(ws_resp).expect("decode").code, CODE_OK);

            let cs = CommitEcShardReq {
                extent_id,
                sealed_length: L as u64,
                eversion: EVERSION,
                owner_epoch: 0,
            };
            let cs_resp = pool
                .call(&addr, MSG_COMMIT_EC_SHARD, cs.encode())
                .await
                .expect("commit_ec_shard RPC");
            assert_eq!(
                CommitEcShardResp::decode(cs_resp).expect("decode").code,
                CODE_OK
            );
        }

        // Build a StreamClient WITHOUT touching a manager: `construct` skips
        // `acquire_owner_lock`, and we pre-seed both caches so the read path
        // never issues a manager RPC.
        let sc = StreamClient::construct(
            vec!["127.0.0.1:1".to_string()], // never contacted
            0,
            "merge-ec-replay-test".to_string(),
            0,
            1 << 30,
            pool.clone(),
            StreamClientConfig::default(),
        );
        for i in 0..N {
            sc.nodes_cache
                .insert(node_ids[i], (addrs[i].to_string(), Vec::<u16>::new()));
        }
        sc.extent_info_cache.insert(
            extent_id,
            ExtentInfo {
                extent_id,
                replicates: node_ids[..K].to_vec(),
                parity: node_ids[K..].to_vec(),
                eversion: EVERSION,
                refs: 1,
                sealed_length: L as u64,
                sealed: true,
                avali: (1u32 << N) - 1, // all slots available
                replicate_disks: vec![],
                parity_disks: vec![],
                ec_converted: true,
            },
        );

        // The function under test: a full-extent committed read.
        let (bytes, committed_end) = sc
            .read_committed_bytes_from_extent(extent_id, 0, L as u64)
            .await
            .expect("read_committed_bytes_from_extent");

        // THE REGRESSION GUARD: the reported committed end must be the
        // LOGICAL sealed_length, not a shard-relative end. Pre-fix this was
        // `last_end` from `ec_subrange_read` (≈ shard_size = L/K = 2048).
        assert_eq!(
            committed_end, L as u64,
            "committed_end must equal sealed_length ({L}), not the shard-relative end (~{shard_size}); \
             a shard-relative end here is the MERGE-EC-REPLAY WAL-FAILSTOP bug"
        );
        // And the bytes themselves decode to the original payload — the bug
        // was the reported END only; `want` was already clamped correctly.
        assert_eq!(bytes.len(), L, "full logical payload must be returned");
        assert_eq!(bytes, payload, "decoded EC bytes must match original");
    }
}
