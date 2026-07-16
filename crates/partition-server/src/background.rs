//! Background loops: compaction, GC, write, and their helper functions.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use autumn_stream::StreamClient;
use bytes::Bytes;
use futures::channel::mpsc;
use futures::StreamExt;

use crate::sstable::{
    AsyncMergeIterator, AsyncTableIterator, FetchMode, IterItem, SstBuilder, SstReader,
};

/// F262: bulk-read window for sequential SST sweeps (compaction merge
/// inputs, split key-scan). One `read_bytes_from_extent` per window,
/// bypassing the BlockCache (scan-resistant). 8 MiB ≈ 128 blocks per RPC —
/// large enough to amortize the round trip, small enough that N concurrent
/// merge inputs hold N × 8 MiB instead of Σ SST bytes (the Stage-1
/// materialization this replaces).
pub(crate) const SCAN_READ_WINDOW_BYTES: u32 = 8 * 1024 * 1024;
use crate::*;

// F256: the R4 4.4 MIN_PIPELINE_BATCH launch gate (and its
// `--min-pipeline-batch` knob) is GONE. The gate required `n_inflight == 0
// || pending >= 256` before launching a batch; whenever per-partition
// concurrency was below 256 (the common case at N>1 partitions) pending
// could never reach 256, so the pipeline degraded to lock-step — effective
// depth=1 regardless of `ps_inflight_cap()`. partition_loop now launches
// whatever `pending` holds as soon as a pipeline slot is free (natural
// batching): batch size adapts to arrival-rate × in-flight latency, and a
// naturally-full burst still lands as ONE batch because the (E) drain pulls
// the whole req channel into `pending` before the launch check runs.

/// F210-E2: per-partition SST count threshold above which the compact
/// loop's timer arm auto-triggers a minor compaction. Set high enough
/// to leave steady-state operation (post-flush + post-minor-compact)
/// untouched, but below the FPR cliff where per-Get miss-path block
/// reads dominate. At 1% per-SST bloom FPR, N=32 ≈ 28% cumulative
/// false-positive on a miss vs. 63% at N=100 — keeping reads cheap
/// on workloads where external policy hasn't kept up. Not tunable
/// because it's a mechanism-level defensive bound, not a policy knob.
const MAX_SST_BEFORE_AUTO_COMPACT: usize = 32;
// F195: process-global setter cells for the background.rs knobs.
// Pre-F195 each was an inner static OnceLock+env read; now lifted to
// module scope with paired pub setters that the autumn-ps binary calls
// from main() based on CLI args. (F256 removed MIN_PIPELINE_BATCH_CELL.)
pub(crate) static GC_READ_CHUNK_BYTES_CELL: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
pub(crate) static GC_BATCH_RECORDS_CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
pub(crate) static GC_BATCH_BYTES_CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
pub(crate) static GC_RATE_BYTES_PER_SEC_CELL: std::sync::OnceLock<u64> = std::sync::OnceLock::new();

pub fn set_gc_read_chunk_bytes(n: u32) -> bool {
    if n == 0 {
        return false;
    }
    GC_READ_CHUNK_BYTES_CELL.set(n).is_ok()
}
pub fn set_gc_batch_records(n: usize) -> bool {
    if !(1..=4096).contains(&n) {
        return false;
    }
    GC_BATCH_RECORDS_CELL.set(n).is_ok()
}
pub fn set_gc_batch_bytes(n: usize) -> bool {
    if !(64 * 1024..=256 * 1024 * 1024).contains(&n) {
        return false;
    }
    GC_BATCH_BYTES_CELL.set(n).is_ok()
}
pub fn set_gc_rate_bytes_per_sec(n: u64) -> bool {
    GC_RATE_BYTES_PER_SEC_CELL.set(n).is_ok()
}

pub(crate) struct CompactStats {
    pub input_tables: usize,
    pub output_tables: usize,
    pub entries_kept: usize,
    pub entries_discarded: usize,
    pub output_bytes: u64,
}

/// 5-7 s jittered timeout used by the compact + GC background loops to
/// periodically refresh their maintenance metrics.
fn random_delay() -> Duration {
    Duration::from_millis(5_000 + rand_u64() % 2_000)
}

/// WAL replay floor for GC — returns `(floor_pos, pos_by_eid)`.
///
/// `floor_pos` is the minimum stream position (FIRST-occurrence index into
/// `log_extent_ids`) over the live SSTs' vp_head extent ids — i.e. exactly
/// `recover_partition`'s replay start (`chosen_pos`). GC must never punch a
/// non-empty extent at/after it, or a crash before the next checkpoint loses
/// the WAL recovery replays from there (the F1 data-loss bug).
///
/// Details that are load-bearing:
/// - **FIRST occurrence**, not last: a CoW-shared extent repeats in the spliced
///   `log_extent_ids` after split/merge, and `recover_partition` keys off the
///   first occurrence (`first_pos_by_eid`). A last-occurrence floor would land
///   LATER than recovery's start and wrongly free an extent recovery replays.
/// - **vp_heads that no longer resolve** (extent already gone from the stream)
///   are skipped — recovery skips them too.
/// - **floor 0** when none resolve (nothing flushed yet, or all vp_head extents
///   gone): protect every non-empty extent, matching recovery's replay-from-0
///   path and its `chosen_pos==MAX` no-replay fallback.
pub(crate) fn gc_replay_floor(
    log_extent_ids: &[u64],
    sst_vp_extent_ids: impl IntoIterator<Item = u64>,
) -> (usize, HashMap<u64, usize>) {
    let mut pos_by_eid: HashMap<u64, usize> = HashMap::new();
    for (pos, &eid) in log_extent_ids.iter().enumerate() {
        pos_by_eid.entry(eid).or_insert(pos);
    }
    let floor = sst_vp_extent_ids
        .into_iter()
        .filter_map(|eid| pos_by_eid.get(&eid).copied())
        .min()
        .unwrap_or(0);
    (floor, pos_by_eid)
}

/// F-RECOVERY-UNBOUNDED BUG2 — raise the GC replay floor from the
/// over-conservative MIN-over-SST-vps (`gc_replay_floor`) up to the position of
/// the newest DURABLY-ACKed flush checkpoint vp (`durable_vp_eid`), when it
/// resolves in the current stream. Returns the (possibly raised) floor.
///
/// Safe because every log record STRICTLY BELOW a durable checkpoint's vp is in
/// that checkpoint's persisted SST set (or compaction-dead) — see the
/// `PartitionData.durable_ckpt_vp` field doc. The vp extent itself sits AT the
/// returned floor and the strictly-before punch rule (`gc_extent_punchable`)
/// keeps it live, so recovery's Step-1 checkpoint resolution always succeeds.
/// `durable_vp_eid == 0` (no flush committed yet this incarnation) or an
/// unresolvable eid leaves the floor at the conservative MIN.
///
/// INVARIANT: `durable_vp_eid` MUST come from the ack-gated `durable_ckpt_vp`
/// cell — NEVER from in-memory state (`vp_*`, or a MAX over live readers), which
/// can run ahead of the durable checkpoint and punch a log region whose naming
/// checkpoint never landed → silent loss.
pub(crate) fn gc_floor_raise_to_durable_ckpt(
    min_floor: usize,
    pos_by_eid: &HashMap<u64, usize>,
    durable_vp_eid: u64,
) -> usize {
    if durable_vp_eid == 0 {
        return min_floor;
    }
    match pos_by_eid.get(&durable_vp_eid) {
        Some(&p) => min_floor.max(p),
        None => min_floor,
    }
}

/// Whether a sealed extent is safe for GC to punch (the F1 guard): an empty
/// extent (`sealed_length == 0`, no committed data) always is; a non-empty one
/// only if it sits STRICTLY BEFORE the replay floor, so the WAL replay window
/// at/after the floor — including the vp_head extent itself — is never truncated.
pub(crate) fn gc_extent_punchable(
    eid: u64,
    sealed_length: u64,
    pos_by_eid: &HashMap<u64, usize>,
    replay_floor_pos: usize,
) -> bool {
    sealed_length == 0 || pos_by_eid.get(&eid).is_none_or(|&pos| pos < replay_floor_pos)
}

/// The vp_head a compaction stamps on its output SSTs IS recovery's replay-start
/// for the merged data. Two forces pull it opposite ways:
/// - it MUST NOT sit AHEAD of any acked-but-un-flushed write (those live in the
///   active memtable at log offsets below the live cursor; a vp_head past them
///   drops them out of the replay window → silent loss — regression
///   `system_compact_unflushed_vp_head`), yet
/// - it SHOULD advance as far as the merged data allows, so GC can reclaim the
///   fully-flushed log region behind it (else post-split shared extents are
///   pinned forever — `system_gc_multiversion_same_extent`).
///
/// The value that satisfies both is the MAX over the INPUT SSTs' OWN vp_heads,
/// taken by STREAM POSITION (first-occurrence index into `log_extent_ids`,
/// matching recovery's `chosen_pos` / `first_pos_by_eid`; extent_id order is
/// non-monotonic after a CoW split so a raw `max(extent_id)` is wrong). The
/// merged SST contains every input's data up to the newest input's `last_seq`,
/// so recovery only needs the log AFTER the newest input's content — which is
/// exactly the newest input's vp_head (the MAX). This advances the floor while
/// staying ≤ the live cursor, so it is STRICTLY SAFER than the pre-fix stamp of
/// `p.vp_*` (the live cursor, which sat past the un-flushed tail — the loss the
/// regression reproduces).
///
/// Residual (separate, deferred follow-up): an SST whose flush RACED writes has a
/// vp_head slightly AHEAD of its own content (the flush snapshots the live cursor
/// at claim time, not the imm's rotation boundary), so MAX can over-advance in
/// that narrow case. The clean fix is to record each imm's true content boundary
/// at rotation; until then MAX is never worse than the pre-fix live-cursor stamp
/// (MAX(inputs) ≤ live cursor always) and the oldest live SST masks the flush-side
/// gap outside of a major compaction.
///
/// Fallback when no input vp_head resolves in the current log (all zero, or their
/// extents already gone): replay from the FIRST log extent at offset 0 — the
/// maximally-conservative safe anchor (recovery replays everything and dedups).
/// NOT `(0,0)`: with a non-empty output table set that leaves recovery's
/// `chosen_pos == usize::MAX` on the no-replay branch = loss (see
/// `recover_partition`'s `replay_extents` selection).
pub(crate) fn compaction_output_vp_head(
    input_vp_heads: impl IntoIterator<Item = (u64, u64)>,
    log_extent_ids: &[u64],
) -> (u64, u64) {
    let mut pos_by_eid: HashMap<u64, usize> = HashMap::new();
    for (pos, &eid) in log_extent_ids.iter().enumerate() {
        pos_by_eid.entry(eid).or_insert(pos);
    }
    // (position, extent_id, offset) with the LARGEST position, tie-broken by
    // largest offset — the newest input's content boundary.
    let mut best: Option<(usize, u64, u64)> = None;
    for (eid, off) in input_vp_heads {
        if eid == 0 {
            continue;
        }
        let Some(&pos) = pos_by_eid.get(&eid) else {
            continue;
        };
        let take = match best {
            None => true,
            Some((bp, _, boff)) => pos > bp || (pos == bp && off > boff),
        };
        if take {
            best = Some((pos, eid, off));
        }
    }
    match best {
        Some((_, eid, off)) => (eid, off),
        None => match log_extent_ids.first() {
            Some(&eid) => (eid, 0),
            None => (0, 0),
        },
    }
}

pub(crate) async fn background_maintenance_loop(
    part_id: u64,
    part: Rc<RefCell<PartitionData>>,
    mut compact_rx: mpsc::Receiver<bool>,
    mut gc_rx: mpsc::Receiver<GcTask>,
    maintenance_gate: std::sync::Arc<crate::CompactionGate>,
    concurrency_ctrl: std::sync::Arc<crate::ConcurrencyController>,
) {
    // Compaction + GC folded onto ONE task (was background_compact_loop +
    // background_gc_loop). Single task => they are STRUCTURALLY serialized, so
    // GC never reads `sst_readers` while a compaction is mid-publish: the
    // recovery replay floor it computes always matches the durable checkpoint
    // recovery would load, with NO GC-vs-compaction gate. Both sections still
    // acquire the single per-partition `maintenance_gate` vs SPLIT (which runs
    // on partition_loop, a different task, and must see no compact_row_append
    // nor log_stream GC append in flight while it seals). See lock notes.
    const MAX_GC_ONCE: usize = 3;
    // Empty sealed extents (sealed_length == 0) are FREE to reclaim — `run_gc`
    // skips the read/relocate loop and only calls `punch_holes`. They sort LAST
    // in the discard-desc candidate order (0 reclaimable bytes), so sharing the
    // MAX_GC_ONCE budget with big rewrite candidates let them starve forever
    // under split/merge churn (which mints empty sealed tails in bulk). Give
    // them their OWN, more generous per-tick budget so they drain promptly.
    const MAX_GC_EMPTY_ONCE: usize = 32;
    const GC_DISCARD_RATIO: f64 = 0.4;
    const GC_FAILURE_COOLDOWN: Duration = Duration::from_secs(300);
    const GC_FAILURE_COOLDOWN_SOFT: Duration = Duration::from_secs(30);
    let mut gc_failure_cooldown: std::collections::HashMap<u64, (Instant, Duration)> =
        std::collections::HashMap::new();

    // Independent deadline timers per kind (deadline, NOT per-iteration
    // duration) so a busy compaction stream does not keep resetting the
    // gc_debt refresh timer and vice-versa. Both timeout arms refresh metrics
    // only (F188/F203 demoted them off the dispatch path).
    let mut next_compact_at = Instant::now() + random_delay();
    let mut next_gc_at = Instant::now() + random_delay();
    // F-OVERVIEW-OPENTAIL: next open-tail size probe (fires immediately on
    // first iteration, then every SIZE_REFRESH_INTERVAL).
    let mut next_size_refresh_at = Instant::now();
    const SIZE_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

    loop {
        use std::future::Future;
        use std::pin::Pin;
        use std::task::Poll;

        enum Sel {
            CompactRecv(Option<bool>),
            GcRecv(Option<GcTask>),
            CompactTimeout,
            GcTimeout,
        }

        let now = Instant::now();

        // F-OVERVIEW-OPENTAIL: throttled, NON-BLOCKING open-tail size probe.
        // Cluster-overview `live_size` = manager sealed-length sum + this. A
        // `commit_length` on each of the 3 stream tails is up to 15 s
        // worst-case (all-replica probe), so it runs DETACHED — it must never
        // stall GC/compaction on this shared maintenance task. Guarded by an
        // in-flight CAS so slow probes don't pile up. The value is kept at the
        // prior reading on ANY probe error (a partial 3-tail sum would
        // misreport, e.g. a briefly-unreachable replica dropping to a tiny
        // number). Read by report_load_loop → shipped to the manager.
        if now >= next_size_refresh_at {
            next_size_refresh_at = now + SIZE_REFRESH_INTERVAL;
            let (sc, log_id, row_id, meta_id, metrics) = {
                let p = part.borrow();
                // F-PS-SIZE-BYTES-DEAD: revive the size gauge here (local +
                // cheap, no RPC — unlike open_tail_bytes below). LSM-resident
                // size (SST + memtable) drives the Prometheus
                // `autumn_ps_partition_size_bytes` gauge and the manager's
                // size-based split/merge policy, both of which read a
                // constant 0 before this.
                p.metrics
                    .size_bytes
                    .store(p.lsm_resident_bytes(), std::sync::atomic::Ordering::Relaxed);
                (
                    p.stream_client.clone(),
                    p.log_stream_id,
                    p.row_stream_id,
                    p.meta_stream_id,
                    p.metrics.clone(),
                )
            };
            use std::sync::atomic::Ordering::Relaxed;
            if metrics
                .open_tail_probe_inflight
                .compare_exchange(false, true, Relaxed, Relaxed)
                .is_ok()
            {
                compio::runtime::spawn(async move {
                    let mut total = 0u64;
                    let mut ok = true;
                    for sid in [log_id, row_id, meta_id] {
                        // `open_tail_committed_len` returns 0 when the tail is
                        // SEALED — its length is already in the manager's
                        // sealed_length sum, so counting it here would
                        // double-count (a CoW split child / just-sealed tail).
                        match sc.open_tail_committed_len(sid).await {
                            Ok(len) => total = total.saturating_add(len),
                            Err(_) => {
                                ok = false;
                                break;
                            }
                        }
                    }
                    if ok {
                        metrics.open_tail_bytes.store(total, Relaxed);
                    }
                    metrics.open_tail_probe_inflight.store(false, Relaxed);
                })
                .detach();
            }
        }

        let compact_sleep = next_compact_at.saturating_duration_since(now);
        let gc_sleep = next_gc_at.saturating_duration_since(now);
        let sel = {
            let mut crecv = std::pin::pin!(compact_rx.next());
            let mut grecv = std::pin::pin!(gc_rx.next());
            let mut csleep = std::pin::pin!(compio::time::sleep(compact_sleep));
            let mut gsleep = std::pin::pin!(compio::time::sleep(gc_sleep));
            std::future::poll_fn(|cx| {
                if let Poll::Ready(v) = Pin::new(&mut crecv).poll(cx) {
                    return Poll::Ready(Sel::CompactRecv(v));
                }
                if let Poll::Ready(v) = Pin::new(&mut grecv).poll(cx) {
                    return Poll::Ready(Sel::GcRecv(v));
                }
                if let Poll::Ready(()) = Pin::new(&mut csleep).poll(cx) {
                    return Poll::Ready(Sel::CompactTimeout);
                }
                if let Poll::Ready(()) = Pin::new(&mut gsleep).poll(cx) {
                    return Poll::Ready(Sel::GcTimeout);
                }
                Poll::Pending
            })
            .await
        };

        match sel {
            // Either channel closing = partition shutdown -> exit the task.
            Sel::CompactRecv(None) | Sel::GcRecv(None) => break,
            Sel::CompactRecv(Some(first)) => {
                next_compact_at = Instant::now() + random_delay();
                // F189-fix HIGH-1: futures::channel::mpsc capacity is
                // `buffer + num_senders`, so cap=1 with 2 senders
                // (PartitionData clone + PartitionHandle clone) admits
                // up to 3 backlogged dispatches per partition — the
                // F188 scheduler comment about "silently no-op via
                // Full" was wrong. Drain everything that's already in
                // the channel and collapse: any `true` (major) wins
                // over `false` (minor). One pass is enough because
                // both senders are bounded; `now_or_never()` ensures
                // we never block here.
                use futures::stream::StreamExt;
                let mut major = first;
                while let Some(Some(more)) = compact_rx.next().now_or_never() {
                    if more {
                        major = true;
                    }
                }
                // 2026-06-02 fix — refuse to start a new compact while a
                // split / merge freeze is in flight. `try_complete_freeze_drain`
                // waits for `compact_inflight == 0` before acking the freeze,
                // and `do_compact` writes SSTs to row_stream after the
                // wait-window would have started. Without this gate the
                // sequence is: freeze set → drain waits on existing compact
                // (correct) → existing compact finishes, inflight=0 → drain
                // acks → split captures commit_length → a freshly-dispatched
                // compact starts a row_stream append → seal captures stale
                // length, compact's later TableLocations record points past
                // it (`stale_vp_offset_past_sealed_length` / `invalid
                // meta_len` on next PS open). The skip is silent; the
                // compact dispatcher (maintenance scheduler / manual
                // `autumn-op compact`) retries after the freeze clears.
                {
                    let p = part.borrow();
                    if p.frozen_for_split.get().is_some() || p.frozen_for_merge.get().is_some() {
                        // Defer; the dispatcher will retry once the freeze
                        // clears (region_sync_loop drops + reopens the
                        // partition on split-survivor / merge-victim, or
                        // partition_loop clears the flag on rollback).
                        continue;
                    }
                }
                let tbls = part.borrow().tables.clone();
                let metrics = part.borrow().metrics.clone();
                // F189-fix MED-4: latch compact_inflight=1 at dequeue,
                // not after gate.acquire(). The scheduler reads
                // compact_inflight to gate duplicate dispatches; the
                // gate.acquire() can block for seconds when other
                // partitions hold AUTUMN_PS_MAJOR_COMPACT_PARALLELISM.
                metrics
                    .compact_inflight
                    .store(1, std::sync::atomic::Ordering::Relaxed);
                // F189-fix HIGH-2: stamp last_compact_at on EVERY recv
                // arm exit (skip + ok + err), so the scheduler's
                // cooldown gate engages even on no-op / failed ticks.
                // See the matching gc_loop fix for full rationale.
                let stamp_last_compact = || {
                    metrics.last_compact_at.store(
                        crate::now_secs() as i64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                };
                let clear_compact_inflight = || {
                    metrics
                        .compact_inflight
                        .store(0, std::sync::atomic::Ordering::Relaxed);
                };
                if tbls.len() < 2 && part.borrow().has_overlap.get() == 0 {
                    tracing::info!(
                        "compact part {}: skipped (major={}) — tables={}, has_overlap=0",
                        part_id,
                        major,
                        tbls.len()
                    );
                    metrics.pending_compaction_bytes.store(
                        compute_pending_compaction_bytes(&part),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    refresh_f202_metrics(&part);
                    stamp_last_compact();
                    clear_compact_inflight();
                    continue;
                }

                let (compact_tbls, truncate_id) = if major {
                    (tbls.clone(), tbls.last().map(|t| t.extent_id).unwrap_or(0))
                } else {
                    pickup_tables(&tbls, 2 * MAX_SKIP_LIST)
                };
                // Skip when the size-tiered selector couldn't pick a mergeable
                // pair (minor only) or when there's literally nothing to
                // compact. Major mode falls through with a single table so
                // overlap cleanup (drop out-of-range keys → clear
                // `has_overlap` at line 229) can unblock split. The earlier
                // guard at line 183 already filters out "1 SST + no overlap",
                // so reaching here in major mode with `compact_tbls.len() < 2`
                // implies `has_overlap == 1` and `do_compact` will rewrite
                // the SST without the out-of-range keys.
                let skip_compact = if major {
                    compact_tbls.is_empty()
                } else {
                    compact_tbls.len() < 2
                };
                if skip_compact {
                    metrics.pending_compaction_bytes.store(
                        compute_pending_compaction_bytes(&part),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    refresh_f202_metrics(&part);
                    stamp_last_compact();
                    clear_compact_inflight();
                    continue;
                }

                // Per-partition maintenance_gate FIRST — serializes vs
                // `handle_split_part` on this partition (split holds this gate
                // from before commit_length through multi_modify_split so no
                // `compact_row_append` from us can race the seal).
                let _local_gate = maintenance_gate.acquire().await;
                // F104: PS-wide concurrency permit — limits cross-partition
                // peak RAM (each do_compact holds ~2x SST bytes).
                let _permit = concurrency_ctrl.acquire_compact().await;
                // compact_inflight already latched at top of recv arm.
                let result = do_compact(&part, compact_tbls, major).await;
                match result {
                    Ok(s) => {
                        tracing::info!(
                            "compact part {}: {}, input={} tables, output={} tables, kept={}, discarded={}, output={}",
                            part_id,
                            if major { "major" } else { "minor" },
                            s.input_tables, s.output_tables, s.entries_kept, s.entries_discarded,
                            crate::human_size(s.output_bytes)
                        );
                        if major {
                            part.borrow().has_overlap.set(0);
                        }
                        if truncate_id != 0 {
                            let (row_stream_id, part_sc) = {
                                let p = part.borrow();
                                (p.row_stream_id, p.stream_client.clone())
                            };
                            if let Err(e) = part_sc.truncate(row_stream_id, truncate_id).await {
                                tracing::warn!("compaction truncate: {e}");
                            }
                        }
                    }
                    Err(e) => tracing::error!("compaction: {e}"),
                }
                // F189-fix-r2 HIGH: stamp + refresh pending bytes BEFORE
                // clearing compact_inflight. Round-2 audit caught that
                // the previous order let the scheduler observe
                // inflight=false + last_compact_at=stale-0 +
                // pending_compaction_bytes=stale-high in the gap, then
                // dispatch a redundant compact for the partition that
                // had JUST finished compacting. Stamp-then-clear closes
                // that window.
                metrics.pending_compaction_bytes.store(
                    compute_pending_compaction_bytes(&part),
                    std::sync::atomic::Ordering::Relaxed,
                );
                refresh_f202_metrics(&part);
                stamp_last_compact();
                clear_compact_inflight();
            }
            Sel::CompactTimeout => {
                next_compact_at = Instant::now() + random_delay();

                // F187: refresh pending_compaction_bytes every periodic
                // tick — independent of whether we end up compacting.
                // F202: same cadence refreshes the dead-data + minor-
                // compact-debt gauges (`sst_expired_bytes`,
                // `sst_out_of_range_bytes`, `minor_compact_pending_bytes`).
                let metrics = part.borrow().metrics.clone();
                metrics.pending_compaction_bytes.store(
                    compute_pending_compaction_bytes(&part),
                    std::sync::atomic::Ordering::Relaxed,
                );
                refresh_f202_metrics(&part);

                // F188: timeout branch is now metric-refresh-only.
                // Actual compactions only fire on `compact_rx` triggers
                // (scheduler dispatches + manual `client compact`).
                // Expiry-major compaction is the one exception — TTL
                // keys ARE a wall-clock event the scheduler doesn't see,
                // so the loop continues to pick it up via the periodic
                // tick rather than waiting for an external trigger.
                let _expiry_continue_below = ();

                // Check if any SSTable has expired keys — trigger major compaction
                let has_expired = {
                    let p = part.borrow();
                    let now = crate::now_secs();
                    p.sst_readers
                        .iter()
                        .any(|r| r.min_expires_at > 0 && r.min_expires_at <= now)
                };
                if has_expired {
                    let tbls = part.borrow().tables.clone();
                    if !tbls.is_empty() {
                        let last_extent = tbls.last().map(|t| t.extent_id).unwrap_or(0);
                        // Per-partition maintenance_gate (see main arm above).
                        let _local_gate = maintenance_gate.acquire().await;
                        let _permit = concurrency_ctrl.acquire_compact().await;
                        metrics
                            .compact_inflight
                            .store(1, std::sync::atomic::Ordering::Relaxed);
                        let result = do_compact(&part, tbls, true).await;
                        match result {
                            Ok(s) => {
                                tracing::info!(
                                    "compact part {}: expiry major, input={} tables, output={} tables, kept={}, discarded={}, output={}",
                                    part_id, s.input_tables, s.output_tables, s.entries_kept, s.entries_discarded,
                                    crate::human_size(s.output_bytes)
                                );
                                if last_extent != 0 {
                                    let (row_stream_id, part_sc) = {
                                        let p = part.borrow();
                                        (p.row_stream_id, p.stream_client.clone())
                                    };
                                    if let Err(e) =
                                        part_sc.truncate(row_stream_id, last_extent).await
                                    {
                                        tracing::warn!("expiry major compaction truncate: {e}");
                                    }
                                }
                            }
                            Err(e) => tracing::error!("expiry major compaction: {e}"),
                        }
                        // F189-fix-r2 HIGH: stamp + refresh BEFORE clearing
                        // inflight; same race as the Recv arm fix above.
                        metrics.pending_compaction_bytes.store(
                            compute_pending_compaction_bytes(&part),
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        metrics.last_compact_at.store(
                            crate::now_secs() as i64,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        metrics
                            .compact_inflight
                            .store(0, std::sync::atomic::Ordering::Relaxed);
                        continue;
                    }
                }

                // F188: minor-compact-on-timer removed. Scheduler now
                // dispatches compactions via `compact_rx` based on
                // `pending_compaction_bytes` (which we just refreshed
                // above). The Recv branch handles BOTH manual triggers
                // and scheduler dispatches via the same channel.

                // F210-E2: defensive auto-compact when SST count grows
                // past `MAX_SST_BEFORE_AUTO_COMPACT`. Per-SST bloom is
                // tuned to 1% FPR but reads consult EVERY reader for a
                // miss (`p.sst_readers.iter().rev()` in `handle_get`),
                // so the cumulative chance that AT LEAST ONE bloom
                // false-positives is `1 - 0.99^N`: 39% at N=50, 63% at
                // N=100, 87% at N=200. Each false-positive costs one
                // block read + decode on the miss path. F203 deleted
                // the PS-side maintenance scheduler in favour of
                // external policy, but FPR runaway is a mechanism-
                // level concern (no operator can be expected to
                // monitor `tables.len()` per partition) so we keep a
                // cheap auto-trigger here. Uses the existing
                // `pickup_tables` size-tiered selector — drains the
                // smallest cohort each tick, converging to a stable
                // <`MAX_SST_BEFORE_AUTO_COMPACT` count over a few
                // ticks without monopolising the compact permit. The
                // recv arm handles externally-dispatched majors
                // unchanged; the threshold is intentionally larger
                // than typical steady-state so this only fires when
                // external policy is absent or has fallen behind.
                let sst_count = part.borrow().sst_readers.len();
                if sst_count > MAX_SST_BEFORE_AUTO_COMPACT
                    && metrics
                        .compact_inflight
                        .load(std::sync::atomic::Ordering::Relaxed)
                        == 0
                {
                    let tbls = part.borrow().tables.clone();
                    let (compact_tbls, truncate_id) = pickup_tables(&tbls, 2 * MAX_SKIP_LIST);
                    if compact_tbls.len() >= 2 {
                        // Per-partition maintenance_gate (see main arm above).
                        let _local_gate = maintenance_gate.acquire().await;
                        let _permit = concurrency_ctrl.acquire_compact().await;
                        metrics
                            .compact_inflight
                            .store(1, std::sync::atomic::Ordering::Relaxed);
                        let result = do_compact(&part, compact_tbls, false).await;
                        match result {
                            Ok(s) => {
                                tracing::info!(
                                    "compact part {}: auto-trim (sst_count was {}), input={} tables, output={} tables, kept={}, discarded={}, output={}",
                                    part_id, sst_count,
                                    s.input_tables, s.output_tables, s.entries_kept, s.entries_discarded,
                                    crate::human_size(s.output_bytes)
                                );
                                if truncate_id != 0 {
                                    let (row_stream_id, part_sc) = {
                                        let p = part.borrow();
                                        (p.row_stream_id, p.stream_client.clone())
                                    };
                                    if let Err(e) =
                                        part_sc.truncate(row_stream_id, truncate_id).await
                                    {
                                        tracing::warn!("auto-trim truncate: {e}");
                                    }
                                }
                            }
                            Err(e) => tracing::error!("auto-trim compaction: {e}"),
                        }
                        metrics.pending_compaction_bytes.store(
                            compute_pending_compaction_bytes(&part),
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        metrics.last_compact_at.store(
                            crate::now_secs() as i64,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        metrics
                            .compact_inflight
                            .store(0, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
            Sel::GcRecv(Some(first)) => {
                next_gc_at = Instant::now() + random_delay();
                let gc_task: GcTask = {
                    // F189-fix HIGH-1: drain backlogged sends (cap=1 +
                    // 2 senders ⇒ up to 3 messages can accumulate). Any
                    // queued Force unions its extents into the chosen
                    // task; multiple Autos collapse to a single Auto.
                    use futures::stream::StreamExt;
                    // F189-fix-r2 LOW: when an Auto is queued behind a
                    // Force (or vice versa), keep BOTH semantics by
                    // promoting to Force with the operator's explicit
                    // extents — and let the auto-discard scan still run
                    // by tagging the Force with a `..Default::default()`-
                    // style flag we don't have. Compromise: under Force,
                    // also union the auto-eligible extents we'd pick from
                    // the discards map. Cheap because we'll iterate
                    // sst_readers anyway. For now, the simpler middle
                    // ground is to flip the merged result to `Auto` when
                    // EITHER input was Auto so the threshold-based scan
                    // covers both extent sets — Force's explicit list is
                    // handled by promoting matched-Force-extents into
                    // Auto-pick at run time. The cleanest implementation
                    // would carry both lists; we accept one Auto extra
                    // tick rather than touching the GcTask enum shape.
                    //
                    // Net behavior: Force + Auto in the drain → run
                    // Force this tick; the dropped Auto is dispatched
                    // again on the next scheduler tick (5 s later) since
                    // the cooldown stamp from this run sets last_gc_at
                    // and the scheduler re-evaluates urgency next tick.
                    // Acceptable.
                    let mut chosen = first;
                    while let Some(Some(more)) = gc_rx.next().now_or_never() {
                        chosen = match (chosen, more) {
                            (
                                GcTask::Force { mut extent_ids },
                                GcTask::Force {
                                    extent_ids: more_eids,
                                },
                            ) => {
                                for e in more_eids {
                                    if !extent_ids.contains(&e) {
                                        extent_ids.push(e);
                                    }
                                }
                                GcTask::Force { extent_ids }
                            }
                            (GcTask::Force { extent_ids }, GcTask::Auto(_)) => {
                                GcTask::Force { extent_ids }
                            }
                            (GcTask::Auto(_), GcTask::Force { extent_ids }) => {
                                GcTask::Force { extent_ids }
                            }
                            // F201: when two Auto ticks coalesce, keep the
                            // most-recent params (the operator's latest
                            // intent supersedes anything queued behind it).
                            (GcTask::Auto(_), GcTask::Auto(p2)) => GcTask::Auto(p2),
                        };
                    }
                    chosen
                };
                // F189-fix MED-4: latch gc_inflight=1 at the very top of the
                // loop iteration, not after maintenance_gate. The scheduler reads
                // gc_inflight to gate duplicate dispatches; without the early
                // latch, a slow get_stream_info / get_extent_info (manager
                // RPC) leaves the flag at 0 for seconds and the scheduler
                // queues redundant Auto tasks behind us. The cleanup at every
                // exit path (continue + loop end) clears it back to 0.
                let metrics = part.borrow().metrics.clone();
                metrics
                    .gc_inflight
                    .store(1, std::sync::atomic::Ordering::Relaxed);
                let clear_inflight = |m: &PartitionMetrics| {
                    m.gc_inflight.store(0, std::sync::atomic::Ordering::Relaxed);
                };

                let (log_stream_id, readers_snapshot, part_sc) = {
                    let p = part.borrow();
                    (
                        p.log_stream_id,
                        p.sst_readers.clone(),
                        p.stream_client.clone(),
                    )
                };

                // F189-fix-r2 MEDIUM: stamp last_gc_at on EVERY early-continue
                // path so the scheduler's cooldown gate engages. Round-2 audit
                // caught that get_stream_info-failure and extent_ids<2 paths
                // skipped the stamp, letting the scheduler re-dispatch every
                // 5 s during transient manager/extent-node hiccups OR for
                // partitions that legitimately have <2 log_stream extents
                // (single-extent → no GC possible). Stamp BEFORE clearing
                // inflight so the scheduler's tuple-read sees both updates.
                let stamp_last_gc = || {
                    metrics.last_gc_at.store(
                        crate::now_secs() as i64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                };

                let stream_info = match part_sc.get_stream_info(log_stream_id).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!("GC get_stream_info: {e}");
                        stamp_last_gc();
                        clear_inflight(&metrics);
                        continue;
                    }
                };
                let extent_ids = stream_info.extent_ids;
                // F-DF-WALDEBT: refresh the open-tail dead-byte gauge from the
                // (already-persisted) SST discard maps BEFORE the <2-extent gate.
                // A log-heavy / all-open-tail partition — the exact case this
                // metric exists for — commonly has a SINGLE log extent (the open
                // tail), which continues below and would NEVER refresh if we
                // waited for the sealed-extent GC path. Computed once here and
                // reused for gc_debt below (get_discards is snapshot-deterministic).
                let mut tick_discards = get_discards(&readers_snapshot);
                metrics.open_tail_dead_bytes.store(
                    open_tail_dead_bytes(&tick_discards, &extent_ids),
                    std::sync::atomic::Ordering::Relaxed,
                );
                if extent_ids.len() < 2 {
                    // No sealed extents (only the open tail, or none) ⇒ sealed-only
                    // gc_debt is definitionally 0. Store it (don't leave the last
                    // ≥2-extent value stale): the manager now sums gc_debt into
                    // `logical_wal_debt` for df, and a partition GC-reclaimed down
                    // to one extent would otherwise over-report debt forever (and
                    // keep the urgency scheduler re-dispatching a no-op GC).
                    metrics
                        .gc_debt_bytes
                        .store(0, std::sync::atomic::Ordering::Relaxed);
                    stamp_last_gc();
                    clear_inflight(&metrics);
                    continue;
                }

                let sealed_extents = &extent_ids[..extent_ids.len() - 1];

                // WAL replay-floor guard (F1): GC must NEVER punch a log extent
                // at/after the floor — recovery replays the log_stream from there
                // forward, so punching it drops records recovery needs (un-flushed
                // small values not yet in any SST, or the vp_head extent itself →
                // chosen_pos==MAX → no replay at all). Only extents strictly BEFORE
                // the floor are fully flushed and safe to reclaim. Running on the
                // SAME task as compaction (the only op that REMOVES SSTs and could
                // raise the floor) means `readers_snapshot` matches the durable
                // checkpoint recovery loads, so this floor == recovery's chosen_pos.
                // (Flush only ADDS forward-vp_head SSTs, which never lower the min.)
                // See `gc_replay_floor` for the first-occurrence / floor-0 details.
                let (mut replay_floor_pos, pos_by_eid) =
                    gc_replay_floor(&extent_ids, readers_snapshot.iter().map(|r| r.vp_extent_id));

                // F-RECOVERY-UNBOUNDED BUG2: raise the floor to the newest
                // DURABLY-ACKed flush checkpoint vp. The MIN-over-SST-vps floor
                // above is over-conservative — it drags back to the OLDEST live
                // SST's vp_head, pinning GC (and recovery's replay window) far
                // behind the region that is actually still needed. Every log
                // record strictly below the durable checkpoint vp is in that
                // checkpoint's persisted SST set (or compaction-dead), so it is
                // safe to reclaim. Safety rests on THREE properties:
                //   (i)  ack-gated — `durable_ckpt_vp` is set only AFTER
                //        save_table_locs_raw acks, so a crash-time recovery is
                //        guaranteed to load a checkpoint whose vp/SST set covers
                //        everything below it (never an in-memory value that ran
                //        ahead of the durable checkpoint — that would punch a log
                //        region whose naming checkpoint never landed → loss);
                //   (ii) the vp extent sits AT the raised floor and the
                //        strictly-before punch rule (`gc_extent_punchable`)
                //        protects it, so recovery Step 1 always resolves it;
                //   (iii) SSTs whose vp extents we punch become unresolvable, and
                //        recovery's per-SST pass simply skips them (lib.rs Step 2,
                //        `if let Some(reader_pos) = first_pos_by_eid.get(...)`), so
                //        `chosen_pos` naturally lands at the first surviving
                //        position — no recovery-code change needed.
                // NEVER substitute an in-memory value (p.vp_*, MAX over
                // readers_snapshot vp_heads): see the `durable_ckpt_vp` field doc.
                let (dv_eid, _dv_off) = part.borrow().durable_ckpt_vp.get();
                replay_floor_pos =
                    gc_floor_raise_to_durable_ckpt(replay_floor_pos, &pos_by_eid, dv_eid);

                // F187: refresh gc_debt_bytes from the sealed-only discards.
                // `tick_discards` was computed once before the gate (and already
                // yielded open_tail_dead_bytes); filter it to sealed extents in
                // place so gc_debt = Σ reclaimable bytes on still-live SEALED
                // log_stream extents — what an operator calls "GC debt". The open
                // tail's dead bytes were counted above (F-DF-WALDEBT), so the two
                // gauges stay disjoint (no double-count).
                valid_discard(&mut tick_discards, sealed_extents);
                let gc_debt: u64 = tick_discards.values().map(|v| (*v).max(0) as u64).sum();
                metrics
                    .gc_debt_bytes
                    .store(gc_debt, std::sync::atomic::Ordering::Relaxed);

                let is_force = matches!(gc_task, GcTask::Force { .. });
                // (extent_id, authoritative sealed_length) — validated ONCE at selection
                // so the execution loop never re-reads (possibly stale) state before the
                // destructive punch. See `authoritative_sealed_length`.
                let mut holes: Vec<(u64, u64)> = match gc_task {
                    GcTask::Force { ref extent_ids } => {
                        let idx: HashSet<u64> = sealed_extents.iter().copied().collect();
                        let mut hs: Vec<(u64, u64)> = Vec::new();
                        let mut skipped = 0usize;
                        // coco P3: count the MAX_GC_ONCE quota by VALIDATED holes, not by
                        // input candidates — i.e. apply `take` AFTER `authoritative_
                        // sealed_length`, not before. Otherwise a transient `None`
                        // (manager RPC hiccup / not-yet-observable seal) on the first
                        // few requested extents consumes the quota and starves the later
                        // valid ones, and Force GC is a one-shot consume (no "later
                        // tick" for the skipped tail).
                        for e in extent_ids.iter().copied().filter(|e| idx.contains(e)) {
                            if hs.len() >= MAX_GC_ONCE {
                                break;
                            }
                            // Even an operator Force GC must not punch on stale/open
                            // state: re-validate authoritatively. A skip (None) means the
                            // seal isn't authoritatively observable yet; the operator can
                            // re-issue once it is.
                            if let Some(sealed_length) =
                                authoritative_sealed_length(&part_sc, e).await
                            {
                                hs.push((e, sealed_length));
                            } else {
                                skipped += 1;
                            }
                        }
                        if skipped > 0 {
                            tracing::info!(
                                skipped,
                                "Force GC: skipped extents not authoritatively sealed yet"
                            );
                        }
                        hs
                    }
                    GcTask::Auto(ref params) => {
                        let discards = tick_discards;

                        // F201: candidate set is ALL sealed (non-tail) extents,
                        // not just those with non-zero discard. Empty sealed
                        // extents (sealed_length == 0, allocated but never
                        // received data before stream_alloc_extent sealed them
                        // via commit_length capture) never appear in any SST's
                        // `discards` map, so the pre-F201 code path (which
                        // built `candidates` from `discards.keys()`) never even
                        // considered them — they stayed pinned in `extent_ids`
                        // forever. We now iterate every sealed extent, sorted
                        // by reclaimable bytes desc (zero-discard extents land
                        // last but still reachable).
                        let mut candidates: Vec<u64> = sealed_extents.to_vec();
                        candidates.sort_by(|a, b| {
                            let da = discards.get(a).copied().unwrap_or(0);
                            let db = discards.get(b).copied().unwrap_or(0);
                            db.cmp(&da)
                        });

                        // F201: resolve effective filter parameters. If the
                        // caller asked for `empty_only`, short-circuit other
                        // filters. Else apply `ratio` (default 0.4) optionally
                        // halved when stream-level dead bytes cross
                        // `stream_debt`, plus `max_size` upper bound.
                        let stream_dead: u64 = discards.values().map(|v| (*v).max(0) as u64).sum();
                        let stream_debt_hit =
                            params.stream_debt.is_some_and(|hw| stream_dead >= hw);
                        let effective_ratio = if params.empty_only {
                            f64::INFINITY // ratio gate unreachable
                        } else {
                            let r = params.ratio.unwrap_or(GC_DISCARD_RATIO);
                            if stream_debt_hit {
                                r * 0.5
                            } else {
                                r
                            }
                        };

                        let mut holes = Vec::new();
                        // Two INDEPENDENT budgets: expensive rewrite candidates
                        // (bounded by MAX_GC_ONCE — each holds ~chunk of RAM + moves
                        // live VPs) vs free empty-sealed reclaims (bounded by the
                        // larger MAX_GC_EMPTY_ONCE — just punch_holes). Empties sort
                        // LAST here (0 reclaimable bytes), so we must NOT stop scanning
                        // when the rewrite budget fills — else split/merge-minted empty
                        // sealed extents at the tail of the list starve forever.
                        let mut nonempty_selected = 0usize;
                        let mut empty_selected = 0usize;
                        for eid in candidates {
                            if nonempty_selected >= MAX_GC_ONCE
                                && empty_selected >= MAX_GC_EMPTY_ONCE
                            {
                                break;
                            }
                            // Authoritative (never stale) sealed state — see
                            // `authoritative_sealed_length`. `None` ⇒ unsealed/open or
                            // fetch failed ⇒ NEVER GC (open extents look like
                            // `sealed_length==0` but are not empty).
                            let sealed_length =
                                match authoritative_sealed_length(&part_sc, eid).await {
                                    Some(l) => l,
                                    None => continue,
                                };
                            if sealed_length == 0 {
                                // F201: a CONFIRMED sealed-empty extent — no committed
                                // data to rewrite, just punch. `run_gc` with
                                // sealed_length=0 skips the read loop and goes straight
                                // to flush_gc_batch (no-op) + punch_holes. Uses the
                                // SEPARATE empty budget so it never starves behind big
                                // rewrite candidates (the split/merge-churn gap).
                                if empty_selected < MAX_GC_EMPTY_ONCE {
                                    holes.push((eid, sealed_length));
                                    empty_selected += 1;
                                }
                                continue;
                            }
                            // Non-empty rewrite candidate: bounded by MAX_GC_ONCE, but
                            // `continue` (not `break`) so we keep scanning for empties.
                            if nonempty_selected >= MAX_GC_ONCE {
                                continue;
                            }
                            if params.empty_only {
                                continue;
                            }
                            if let Some(mx) = params.max_size {
                                if sealed_length > mx {
                                    continue;
                                }
                            }
                            let discard_bytes = discards.get(&eid).copied().unwrap_or(0);
                            if discard_bytes <= 0 {
                                continue;
                            }
                            let ratio = discard_bytes as f64 / sealed_length as f64;
                            if ratio > effective_ratio {
                                holes.push((eid, sealed_length));
                                nonempty_selected += 1;
                            }
                        }
                        holes
                    }
                };

                // WAL replay-floor guard (applies to BOTH Auto and Force — even an
                // operator Force must not punch the replay window; compact first to
                // advance the floor). Never punch a NON-EMPTY extent at/after the
                // replay floor (see floor computation above). Empty (sealed_length==0)
                // extents carry no committed data and stay eligible.
                {
                    let mut protected_eids: Vec<u64> = Vec::new();
                    holes.retain(|(eid, sealed_length)| {
                        let keep =
                            gc_extent_punchable(*eid, *sealed_length, &pos_by_eid, replay_floor_pos);
                        if !keep {
                            protected_eids.push(*eid);
                        }
                        keep
                    });
                    if !protected_eids.is_empty() {
                        // The extent recovery would replay FROM (floor position).
                        let floor_extent = extent_ids.get(replay_floor_pos).copied().unwrap_or(0);
                        // Which live SST's vp_head anchors that floor (the SST
                        // pinning the window). First match is enough for a hint.
                        let pinned_by_sst_vp_extent = readers_snapshot
                            .iter()
                            .map(|r| r.vp_extent_id)
                            .find(|eid| pos_by_eid.get(eid).copied() == Some(replay_floor_pos))
                            .unwrap_or(0);
                        tracing::info!(
                            part_id,
                            protected = ?protected_eids,
                            floor_extent,
                            floor_pos = replay_floor_pos,
                            pinned_by_sst_vp_extent,
                            "GC: protected extent(s) at/before the recovery replay floor — \
                             recovery replays log_stream from floor_extent forward, so punching \
                             them could lose un-flushed writes. This is EXPECTED, not a bug. To \
                             advance the floor: flush + MAJOR-compact so every live SST's vp_head \
                             moves past floor_extent, then retry (see `autumn-op info --part`)."
                        );
                    }
                }

                // F199: filter against the per-extent failure cooldown. Force
                // tasks bypass the cooldown (operator override), Auto tasks
                // respect it. Stale entries (older than the cooldown window)
                // are evicted lazily to keep the map bounded.
                if !is_force {
                    let now = Instant::now();
                    // Evict stale entries (past their own cooldown window).
                    gc_failure_cooldown.retain(|_, (t, dur)| now.duration_since(*t) < *dur);
                    let initial_len = holes.len();
                    holes.retain(|(eid, _)| {
                        gc_failure_cooldown
                            .get(eid)
                            .is_none_or(|(t, dur)| now.duration_since(*t) >= *dur)
                    });
                    if holes.len() < initial_len {
                        tracing::info!(
                            skipped = initial_len - holes.len(),
                            remaining = holes.len(),
                            "F199+F201: GC skipping recently-failed extents (cooldown active)"
                        );
                    }
                }

                if holes.is_empty() {
                    // F189-fix HIGH-2 + r2: same stamp-then-clear rationale as
                    // the early-exit paths above. Cooldown engages even when
                    // there's nothing to punch.
                    stamp_last_gc();
                    clear_inflight(&metrics);
                    continue;
                }

                // F196 D-r6: PS-wide GC concurrency cap (via the unified
                // AdmissionController). Acquired AFTER the per-partition
                // maintenance_gate so multiple partitions on the same PS don't all
                // enter run_gc together — each holds ~64 MiB chunk buffer +
                // rewrite staging. Default 1 (full serialization), tunable
                // via `--gc-parallelism`.
                // PS-wide gc concurrency cap FIRST (cross-partition RAM), THEN
                // the per-partition maintenance_gate. Permit-before-gate is
                // deliberate (coco): a GC queued on the global gc permit must NOT
                // hold this partition's maintenance_gate while it waits, or it
                // needlessly blocks a same-partition split. Safe despite the
                // asymmetry with compaction/split (which are gate-first): GC uses
                // `acquire_gc`, split/compaction use `acquire_compact`, so GC
                // never shares a PS-wide permit with them — the wait-for graph
                // acquire_gc → maintenance_gate → acquire_compact stays acyclic.
                // (Compaction MUST stay gate-first to match split's
                // maintenance_gate → acquire_compact, else AC↔MG would cycle; GC
                // has no such constraint, so permit-first is both safe and
                // avoids the split-blocking.)
                let _gc_conc_permit = concurrency_ctrl.acquire_gc().await;
                // maintenance_gate held around the holes-processing loop below
                // (the read-only candidate selection above ran without it) so
                // handle_split_part sees no log_stream GC append in-flight.
                let _gc_permit = maintenance_gate.acquire().await;
                tracing::info!("GC: starting, extents={:?}", holes);
                // F189-fix MED-4: gc_inflight already latched at top of loop;
                // hold through the punch and clear at the bottom.
                // `sealed_length` was validated AUTHORITATIVELY at selection
                // (`authoritative_sealed_length`) and carried here — do NOT re-read it
                // from the (possibly stale) extent_info cache, or the check/use split
                // re-opens the seed=583 stale-cache punch on a sealed extent.
                for (eid, sealed_length) in holes {
                    match run_gc(&part, eid, sealed_length).await {
                        Ok(()) => {
                            // F199: success → clear any prior failure stamp so
                            // a transient EC fall-back hiccup doesn't suppress
                            // the next legitimate GC need.
                            gc_failure_cooldown.remove(&eid);
                        }
                        Err(e) => {
                            let dur = classify_gc_failure_cooldown(
                                &e,
                                GC_FAILURE_COOLDOWN_SOFT,
                                GC_FAILURE_COOLDOWN,
                            );
                            tracing::error!(
                                extent_id = eid,
                                cooldown_secs = dur.as_secs(),
                                "GC run_gc extent: {e}"
                            );
                            gc_failure_cooldown.insert(eid, (Instant::now(), dur));
                        }
                    }
                }
                // F189-fix HIGH-2 + r2: stamp BEFORE clear so the scheduler
                // doesn't see (inflight=0, last_gc_at=stale) and re-dispatch.
                stamp_last_gc();
                clear_inflight(&metrics);
                drop(_gc_permit);
            }
            Sel::GcTimeout => {
                next_gc_at = Instant::now() + random_delay();
                // F203: refresh `gc_debt_bytes` metric without
                // dispatching. `report_load_loop` and any external
                // policy controller queries see fresh debt without
                // the loop deciding to act on it.
                let metrics = part.borrow().metrics.clone();
                let (log_stream_id, readers_snapshot, part_sc) = {
                    let p = part.borrow();
                    (
                        p.log_stream_id,
                        p.sst_readers.clone(),
                        p.stream_client.clone(),
                    )
                };
                if let Ok(stream_info) = part_sc.get_stream_info(log_stream_id).await {
                    let extent_ids = stream_info.extent_ids;
                    let mut discards = get_discards(&readers_snapshot);
                    // F-DF-WALDEBT: refresh open-tail dead bytes on EVERY periodic
                    // tick, regardless of extent count — this is the idle-refresh
                    // path (no GC dispatched), and an all-open-tail partition with a
                    // single log extent would otherwise never update it. `discards`
                    // is reused for gc_debt below (one get_discards per tick).
                    metrics.open_tail_dead_bytes.store(
                        open_tail_dead_bytes(&discards, &extent_ids),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    // Always store gc_debt (0 when there is no sealed prefix) so a
                    // partition reclaimed down to one extent doesn't leave a stale
                    // sealed-debt inflating the manager's `logical_wal_debt`.
                    let gc_debt: u64 = if extent_ids.len() >= 2 {
                        let sealed = &extent_ids[..extent_ids.len() - 1];
                        valid_discard(&mut discards, sealed);
                        discards.values().map(|v| (*v).max(0) as u64).sum()
                    } else {
                        0
                    };
                    metrics
                        .gc_debt_bytes
                        .store(gc_debt, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }
}

// F099-D: `background_write_loop` and its R1/LF dispatch helpers are gone —
// the write loop is now inlined into `partition_loop` on the main
// P-log task. The primitives below (`start_write_batch`, `finish_write_batch`,
// `handle_completion`, `InflightCompletion`, `InFlightBatch`, `BatchData`)
// remain as building blocks used by that merged loop.

/// Carrier payload pushed through the FuturesUnordered completion queue.
/// `data` is the Phase-1 validated batch; `phase2_result` is the return
/// value of the P-log `append_batch` / `append_segments` call.
pub(crate) struct InflightCompletion {
    pub(crate) data: BatchData,
    pub(crate) phase2_result: Result<autumn_stream::AppendResult>,
}

/// Consume one completion: run Phase 3 (memtable insert + client reply),
/// update metrics, and surface `LockedByOther` via the shared flag so the
/// main loop can terminate the partition.
pub(crate) async fn handle_completion(
    part: &Rc<RefCell<PartitionData>>,
    metrics: &mut WriteLoopMetrics,
    locked_by_other: &Rc<Cell<bool>>,
    part_id: u64,
    c: InflightCompletion,
) {
    match finish_write_batch(part, c.data, c.phase2_result).await {
        Ok(stats) => {
            // LAT-1: every op in a group-committed batch experienced the
            // batch's end-to-end latency — observe `ops` at that value.
            // Reuses the already-measured BatchStats (no new timing).
            part.borrow()
                .metrics
                .write_lat
                .observe_n(stats.end_to_end_ns, stats.ops);
            metrics.record(stats)
        }
        Err(e) => {
            if is_locked_by_other(&e) {
                tracing::error!(part_id, "LockedByOther detected, poisoning partition");
                locked_by_other.set(true);
            } else {
                tracing::error!("write batch error: {e}");
            }
        }
    }
    metrics.maybe_report(part_id);
}

// ---------------------------------------------------------------------------
// Write batch processing — split into start (Phase1) + finish (Phase3)
// with Phase2 (append I/O) as an in-flight future for double-buffering.
// ---------------------------------------------------------------------------

pub(crate) struct ValidatedEntry {
    internal_key: Vec<u8>,
    user_key: Bytes,
    op: u8,
    value: Bytes,
    expires_at: u64,
    /// F099-D: direct responder. On Phase 3 success we call `send_ok` which
    /// encodes the `PutResp` / `DeleteResp` frame bytes and forwards to the
    /// outer ps-conn oneshot — no inner oneshot hop.
    resp: crate::WriteResponder,
}

pub(crate) type Phase2Fut =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<autumn_stream::AppendResult>>>>;

/// In-flight batch data (without the future).
pub(crate) struct BatchData {
    picked_at: Instant,
    phase1_ns: u64,
    phase2_started_at: Instant,
    valid: Vec<ValidatedEntry>,
    record_sizes: Vec<u32>,
}

/// In-flight batch: Phase1 done, Phase2 future running.
pub(crate) struct InFlightBatch {
    pub(crate) data: BatchData,
    pub(crate) phase2_fut: Phase2Fut,
}

/// F177: payload-byte threshold for offloading Phase 1's WAL encoding
/// (CRC32C compute + segment build) to `spawn_blocking`. Below this
/// threshold the encode runs inline on the P-log compio runtime —
/// spawn_blocking's ~10-20 µs join overhead would dominate small
/// batches. Above this, the inline CPU cost (CRC32C ~12 GB/s = ~83 µs
/// per MB) dominates and offloading wins. 4 MiB → ~350 µs CRC, 17×
/// the spawn overhead.
const PHASE1_OFFLOAD_THRESHOLD: u64 = 4 * 1024 * 1024;

/// Phase 1: validate + encode + launch Phase2 future.
///
/// **F177 — async + conditional spawn_blocking on big batches.**
/// Pre-F177 this was sync and ran the full encode loop (CRC32C +
/// `Bytes::copy_from_slice` of every value) inline under the P-log
/// `borrow_mut`. For 8 MiB-value workloads a 256-record batch ran
/// ~950 ms inline CPU on the compio runtime, blocking all other tasks
/// (ps-conn, flush_loop, compact_loop, gc_loop) for the duration —
/// observed as 744 ms p99 on the F176 perf bench. F177 splits Phase 1
/// into two sub-phases:
///   - **Phase 1a (sync, under `borrow_mut`):** validate range, assign
///     seq numbers, build `ValidatedEntry` list. Bounded CPU; no per-
///     record CRC.
///   - **Phase 1b (CRC + segment build):** runs **inline** when total
///     batch payload < `PHASE1_OFFLOAD_THRESHOLD` (= 4 MiB), otherwise
///     `spawn_blocking` so the CPU work moves to the blocking pool and
///     the P-log runtime stays responsive for ps-conn / heartbeat /
///     other tasks.
pub(crate) async fn start_write_batch(
    part: &Rc<RefCell<PartitionData>>,
    batch: Vec<WriteRequest>,
) -> Result<Option<InFlightBatch>> {
    let picked_at = Instant::now();
    let phase1_started_at = Instant::now();

    // Phase 1a: validate + assign seq + collect entries.
    let (mut valid, log_stream_id, part_sc, admission) = {
        let mut p = part.borrow_mut();

        let mut valid: Vec<ValidatedEntry> = Vec::with_capacity(batch.len());
        for req in batch {
            let (user_key, op, value, expires_at) = match req.op {
                WriteOp::Put {
                    user_key,
                    value,
                    expires_at,
                } => (user_key, 1u8, value, expires_at),
                WriteOp::Delete { user_key } => (Bytes::from(user_key), 2u8, Bytes::new(), 0u64),
                // BUG-LEASE-2 Phase 2: fence-floor bump record. key = raw
                // ino bytes (NOT a user key — skips in_range below), value
                // = epoch. Never inserted into the memtable (Phase 3 skips
                // OP_FENCE_BUMP); replay max-merges it into fence_floors.
                WriteOp::FenceBump { ino, epoch } => (
                    Bytes::copy_from_slice(&ino.to_be_bytes()),
                    crate::OP_FENCE_BUMP,
                    Bytes::copy_from_slice(&epoch.to_le_bytes()),
                    0u64,
                ),
            };
            if op != crate::OP_FENCE_BUMP && !in_range(&p.rg, &user_key) {
                req.resp.send_err("key is out of range".to_string());
                continue;
            }
            p.seq_number += 1;
            let seq = p.seq_number;
            let internal_key = key_with_ts(&user_key, seq);
            valid.push(ValidatedEntry {
                internal_key,
                user_key,
                op,
                value,
                expires_at,
                resp: req.resp,
            });
        }

        if valid.is_empty() {
            return Ok(None);
        }

        let log_stream_id = p.log_stream_id;
        let part_sc = p.stream_client.clone();
        let admission = p.rate_ctrl.clone();
        (valid, log_stream_id, part_sc, admission)
    };
    // borrow_mut released here — safe to await below.

    // Phase 1b: CRC32C + segment build. Decide inline vs spawn_blocking
    // by total batch payload. Each ValidatedEntry contributes
    // `value.len()` bytes to the CRC compute (the dominant cost); keys
    // and headers are tens of bytes per record, negligible.
    let total_value_bytes: u64 = valid.iter().map(|e| e.value.len() as u64).sum();
    // Write-throughput accounting: accumulate value bytes once per batch (P-log
    // thread, no contention). report_load_loop swaps this for write_bytes_per_sec.
    part.borrow()
        .metrics
        .write_bytes
        .fetch_add(total_value_bytes, std::sync::atomic::Ordering::Relaxed);

    let (segments, record_sizes) = if total_value_bytes >= PHASE1_OFFLOAD_THRESHOLD {
        // F177: big-batch path — move encode inputs into spawn_blocking.
        // We MUST keep `valid` alive on the main runtime (its `resp` /
        // `WriteResponder` holds Rc<...> oneshot senders that are !Send).
        // So we stage `(op, internal_key, value: Bytes, expires_at)`
        // tuples into a Send Vec, and clone `value` (Bytes::clone =
        // Arc::clone, ~free for both small + large values).
        let inputs: Vec<(u8, Vec<u8>, Bytes, u64)> = valid
            .iter()
            .map(|e| {
                let wal_op = if e.value.len() > VALUE_THROTTLE {
                    e.op | OP_VALUE_POINTER
                } else {
                    e.op
                };
                (
                    wal_op,
                    e.internal_key.clone(),
                    e.value.clone(),
                    e.expires_at,
                )
            })
            .collect();
        let result = compio::runtime::spawn_blocking(move || {
            let mut segments: Vec<Bytes> = Vec::with_capacity(inputs.len() * 3);
            let mut record_sizes: Vec<u32> = Vec::with_capacity(inputs.len());
            for (wal_op, internal_key, value, expires_at) in inputs {
                let value_empty = value.is_empty();
                let (hdr_seg, val_seg, crc_seg) =
                    crate::wal_record::encode_v1_segments(wal_op, &internal_key, value, expires_at);
                let total = hdr_seg.len() + val_seg.len() + crc_seg.len();
                segments.push(hdr_seg);
                if !value_empty {
                    segments.push(val_seg);
                }
                segments.push(crc_seg);
                record_sizes.push(total as u32);
            }
            (segments, record_sizes)
        })
        .await
        .map_err(|_| anyhow!("F177 spawn_blocking encode panicked"))?;
        result
    } else {
        // Small-batch fast path: encode inline. Spawn overhead would
        // dominate sub-4 MiB batches. Move each value's Bytes into the
        // segments.
        let mut segments: Vec<Bytes> = Vec::with_capacity(valid.len() * 3);
        let mut record_sizes: Vec<u32> = Vec::with_capacity(valid.len());
        for e in valid.iter_mut() {
            let wal_op = if e.value.len() > VALUE_THROTTLE {
                e.op | OP_VALUE_POINTER
            } else {
                e.op
            };
            // Clone Bytes (Arc::clone, ~free) so the original stays in
            // ValidatedEntry for Phase 3 memtable insert (small values
            // go inline; large VP-path values aren't needed but the
            // clone cost is irrelevant since we're below 4 MiB total).
            let value_for_encode = e.value.clone();
            let value_empty = value_for_encode.is_empty();
            let (hdr_seg, val_seg, crc_seg) = crate::wal_record::encode_v1_segments(
                wal_op,
                &e.internal_key,
                value_for_encode,
                e.expires_at,
            );
            let total = hdr_seg.len() + val_seg.len() + crc_seg.len();
            segments.push(hdr_seg);
            if !value_empty {
                segments.push(val_seg);
            }
            segments.push(crc_seg);
            record_sizes.push(total as u32);
        }
        (segments, record_sizes)
    };

    // F178: every append is durable. The F150 Phase B rotation-trigger
    // barrier is gone (Phase 2), and the AppendReq.must_sync wire field
    // is gone (Phase 3 follow-up). Durability is enforced at TWO points:
    //   1. extent-node coalescer (Phase 1, event-driven group commit) —
    //      every pwrite's bytes become durable in one fsync coalesced
    //      with concurrent friends; the handler always awaits the
    //      coalescer's wake.
    //   2. flush-time `await_log_synced_to` in `flush_one_imm` — quorum
    //      of replicas must have synced past `vp_offset` BEFORE the SST
    //      upload, so every byte the imm's ValuePointers reference is
    //      durable on a quorum before the SST that names them is
    //      checkpointed.
    //
    // Net: every Put pays exactly one fsync syscall (~1 ms tmpfs / 5-15
    // ms NVMe). Flush adds ≈ 0 ms on the happy path (coalescer fires
    // when first waiter arrives; flush builds SST in parallel).
    let phase1_ns = duration_to_ns(phase1_started_at.elapsed());

    // F189 + F196: foreground admission. Per-batch single Mutex acquire +
    // (bytes, ops) accounting. bytes catches large-value workloads;
    // ops catches small-value IOPS-bound workloads (4 KiB Puts saturate
    // P-log long before bytes hit the cap). EITHER cap reached → fg
    // sleeps. With both caps at 0 (unlimited) returns immediately —
    // hot path stays cheap.
    admission
        .account_fg(total_value_bytes, valid.len() as u64)
        .await;

    // Launch Phase 2 as a future (not awaited yet).
    let phase2_started_at = Instant::now();
    let phase2_fut =
        Box::pin(async move { part_sc.append_segments(log_stream_id, segments).await });

    Ok(Some(InFlightBatch {
        data: BatchData {
            picked_at,
            phase1_ns,
            phase2_started_at,
            valid,
            record_sizes,
        },
        phase2_fut,
    }))
}

/// F270 fence classifier — the trigger for poison-and-reopen self-heal.
/// Matches the "LockedByOther" marker emitted by BOTH fence layers: the
/// EN's native `CODE_LOCKED_BY_OTHER` rejection (append / commit_length)
/// and, since BUG-MGR-RETRY-CLASS, the stream client's typed
/// `ManagerError` Display for a manager-side `ensure_owner_epoch`
/// rejection (stale owner_epoch on `alloc_new_extent` etc.).
///
/// `{e:#}` (anyhow alternate Display) prints the WHOLE context chain —
/// plain `{e}` shows only the outermost context, so a `.context(...)`
/// wrap added along the append path (e.g. client.rs "alloc_new_extent
/// failed after append error: …") would HIDE the marker and silently
/// downgrade a fence to a generic write error (no poison → no
/// fresh-epoch reopen).
pub(crate) fn is_locked_by_other(e: &anyhow::Error) -> bool {
    format!("{e:#}").contains("LockedByOther")
}

/// Phase 3: given Phase2 result, insert into memtable, reply to callers.
pub(crate) async fn finish_write_batch(
    part: &Rc<RefCell<PartitionData>>,
    bd: BatchData,
    phase2_result: Result<autumn_stream::AppendResult>,
) -> Result<BatchStats> {
    let phase2_elapsed = bd.phase2_started_at.elapsed();

    let result = match phase2_result {
        Ok(result) => result,
        Err(e) => {
            let msg = format!("log_stream append_segments: {e}");
            for entry in bd.valid {
                entry.resp.send_err(msg.clone());
            }
            return Err(anyhow!(msg));
        }
    };

    // Phase 3: insert into memtable + update VP head.
    //
    // F099-C: batch all N (up to 256) memtable inserts under ONE RwLock write
    // guard acquisition via `insert_batch`. Prior to F099-C this loop called
    // `p.active.insert` N times; under the new RwLock<BTreeMap> backing that
    // would mean N write-lock acquire/release cycles per batch. Collapsing
    // into one saves N-1 atomic-CAS pairs per batch (256 → 1 on the hot
    // --threads 256 workload) while preserving the single-writer semantics.
    //
    // F099-D: the per-entry responder is a direct `WriteResponder` into the
    // outer ps-conn oneshot, carrying the encoded `PutResp` / `DeleteResp`
    // frame bytes. No inner oneshot; `handle_put` is gone.
    let phase3_started_at = Instant::now();
    let mut responders: Vec<crate::WriteResponder> = Vec::new();
    let batch_ops = bd.record_sizes.len() as u64;
    let record_sizes = bd.record_sizes;
    // F-RECOVERY-UNBOUNDED BUG1: total log_stream bytes this batch appended
    // (value included) — feeds the memtable's log_bytes counter so the F120-B
    // WAL-gap force-rotate bounds the true replay window. Computed BEFORE
    // `record_sizes` is moved into the insert closure below.
    let batch_log_bytes: u64 = record_sizes.iter().map(|&s| s as u64).sum();
    let base_offset = result.offset;
    let extent_id_for_vp = result.extent_id;
    {
        let mut p = part.borrow_mut();

        // Materialise the inserts as an iterator that also side-effects
        // `responders`. The iterator is fully consumed inside insert_batch,
        // so the side effects all happen under the (single) write lock.
        let valid = bd.valid;
        let mut cumulative: u64 = 0;
        let mut idx: usize = 0;
        let responders_ref = &mut responders;
        let iter = valid.into_iter().filter_map(move |entry| {
            let record_offset = base_offset + cumulative;
            cumulative += record_sizes[idx] as u64;
            idx += 1;

            // BUG-LEASE-2 Phase 2: fence-bump records are WAL-only — the
            // floor was already raised in fence_floors at enqueue time;
            // nothing enters the memtable. (Offset accounting above must
            // still advance: record_sizes is aligned over ALL entries.)
            if entry.op == crate::OP_FENCE_BUMP {
                responders_ref.push(entry.resp);
                return None;
            }

            let mem_entry = if entry.value.len() > VALUE_THROTTLE {
                // V1 record layout (post-F165 default-on):
                //   [V1_SENTINEL:1][payload_len:4][op:1][key_len:4]
                //   [val_len:4][expires_at:8][key bytes][value bytes][crc:4]
                //
                // Value starts at record_offset + 1 + 4 + 17 + key.len()
                //                                        ^^^^^^ V0 inner header
                //                                  ^^ V1 envelope (sentinel+length)
                //                                = 22 + key.len()
                //
                // Pre-fix this used `+ 17 +` which was the V0-layout calc;
                // the V1 envelope's 5-byte sentinel+length prefix makes the
                // VP offset point 5 bytes EARLIER than the value bytes,
                // returning the last 5 bytes of internal_key (inverted-seq)
                // followed by (val_len - 5) bytes of value. Latent since
                // F165 flipped V1 default-on; surfaced by F186's putstream
                // tests because they were the first to verify > VALUE_THROTTLE
                // value content end-to-end with V1 records.
                let vp = ValuePointer {
                    extent_id: extent_id_for_vp,
                    offset: record_offset + 22 + entry.internal_key.len() as u64,
                    len: entry.value.len() as u64,
                };
                MemEntry {
                    op: entry.op | OP_VALUE_POINTER,
                    value: vp.encode().to_vec(),
                    expires_at: entry.expires_at,
                }
            } else {
                MemEntry {
                    op: entry.op,
                    value: entry.value.to_vec(),
                    expires_at: entry.expires_at,
                }
            };

            let write_size = (entry.user_key.len() + mem_entry.value.len() + 32) as u64;
            responders_ref.push(entry.resp);
            Some((entry.internal_key, mem_entry, write_size))
        });

        p.active.insert_batch(iter);
        // F-RECOVERY-UNBOUNDED BUG1: track the un-flushed LOG window (value
        // included) for the F120-B force-rotate; `mem_bytes` would only see the
        // ~24-byte VP for large values and never trip the 2 GiB gap.
        p.active.add_log_bytes(batch_log_bytes);

        p.vp_extent_id = result.extent_id;
        p.vp_offset = result.end;

        maybe_rotate(&mut p);
    }
    let phase3_elapsed = phase3_started_at.elapsed();

    // Send replies AFTER releasing the partition borrow so a poorly-timed
    // executor wake on the waking ps-conn can't re-enter PartitionData.
    for resp in responders {
        resp.send_ok();
    }

    Ok(BatchStats {
        ops: batch_ops,
        batch_size: batch_ops,
        phase1_ns: bd.phase1_ns,
        phase2_ns: duration_to_ns(phase2_elapsed),
        phase3_ns: duration_to_ns(phase3_elapsed),
        end_to_end_ns: duration_to_ns(bd.picked_at.elapsed()),
    })
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

/// F187: snapshot how many SSTable bytes the next compact tick would
/// consume. `has_overlap == 1` means major compaction is mandated and
/// will rewrite every table — so the answer is total SST bytes.
/// Otherwise it's whatever `pickup_tables` would pick, which is the same
/// thing the periodic compact tick will actually do. Cheap (no I/O, no
/// borrow_mut on Memtable).
pub(crate) fn compute_pending_compaction_bytes(part: &Rc<RefCell<PartitionData>>) -> u64 {
    let p = part.borrow();
    let tbls = p.tables.clone();
    let overlap = p.has_overlap.get();
    drop(p);
    if overlap == 1 {
        return tbls.iter().map(|t| t.estimated_size).sum();
    }
    let (compact_tbls, _) = pickup_tables(&tbls, 2 * MAX_SKIP_LIST);
    compact_tbls.iter().map(|t| t.estimated_size).sum()
}

/// F202: refresh the per-partition dead-data + minor-compact-debt
/// gauges. Called at the same points as `compute_pending_compaction_bytes`
/// (every compact tick) AND after flush completes (tables change).
///
/// Approximations:
/// - `sst_expired_bytes`: Σ `estimated_size` for tables whose paired
///   `SstReader.min_expires_at` is non-zero and `<= now`. Conservative
///   upper bound (counts whole SST, not just expired entries; tightening
///   needs an on-disk aggregate change deferred to a future stage).
/// - `sst_out_of_range_bytes`: Σ `estimated_size` of all tables when
///   `has_overlap == 1` (post-split CoW-shared SSTs); 0 otherwise. Same
///   conservative shape as `pending_compaction_bytes`'s overlap branch.
/// - `minor_compact_pending_bytes`: Σ `estimated_size` of
///   `pickup_tables` output when `has_overlap == 0`. 0 when overlap is
///   set (a major would run instead, accounted for in
///   `pending_compaction_bytes`).
/// - `sst_tombstone_bytes`: left at 0. Computing it without an SST
///   on-disk aggregate (which would require a format bump) means
///   scanning every block — expensive on the hot refresh path. The
///   advisory layer treats 0 as "no signal" for this dimension.
/// - `sealed_log_extent_count`: left at 0. The PS doesn't keep a
///   cached log-stream extent count without an RPC; future stages
///   can plumb this from the GC loop's `get_stream_info` call.
pub(crate) fn refresh_f202_metrics(part: &Rc<RefCell<PartitionData>>) {
    use std::sync::atomic::Ordering::Relaxed;
    let now = crate::now_secs();

    let (tbls, readers, overlap, metrics) = {
        let p = part.borrow();
        (
            p.tables.clone(),
            p.sst_readers.clone(),
            p.has_overlap.get(),
            p.metrics.clone(),
        )
    };

    // `tables` and `sst_readers` are aligned by index by construction
    // (see partition-server/CLAUDE.md programming note 7). zip is safe.
    let sst_expired: u64 = tbls
        .iter()
        .zip(readers.iter())
        .filter(|(_, r)| r.min_expires_at > 0 && r.min_expires_at <= now)
        .map(|(t, _)| t.estimated_size)
        .sum();
    metrics.sst_expired_bytes.store(sst_expired, Relaxed);

    let sst_oor: u64 = if overlap == 1 {
        tbls.iter().map(|t| t.estimated_size).sum()
    } else {
        0
    };
    metrics.sst_out_of_range_bytes.store(sst_oor, Relaxed);

    let minor_pending: u64 = if overlap == 0 {
        let (picked, _) = pickup_tables(&tbls, 2 * MAX_SKIP_LIST);
        picked.iter().map(|t| t.estimated_size).sum()
    } else {
        0
    };
    metrics
        .minor_compact_pending_bytes
        .store(minor_pending, Relaxed);

    // sst_tombstone_bytes + sealed_log_extent_count: deferred. The
    // advisory layer treats 0 in these dimensions as "no signal".
}

pub(crate) fn pickup_tables(tables: &[TableMeta], max_capacity: u64) -> (Vec<TableMeta>, u64) {
    if tables.len() < 2 {
        return (vec![], 0);
    }

    let total_size: u64 = tables.iter().map(|t| t.estimated_size).sum();
    let head_extent = tables[0].extent_id;
    let head_size: u64 = tables
        .iter()
        .filter(|t| t.extent_id == head_extent)
        .map(|t| t.estimated_size)
        .sum();
    let head_threshold = (HEAD_RATIO * total_size as f64).round() as u64;

    if head_size < head_threshold {
        let chosen: Vec<TableMeta> = tables
            .iter()
            .filter(|t| t.extent_id == head_extent)
            .take(COMPACT_N)
            .cloned()
            .collect();
        let truncate_id = tables
            .iter()
            .find(|t| t.extent_id != head_extent)
            .map(|t| t.extent_id)
            .unwrap_or(0);

        let mut tbls_sorted = tables.to_vec();
        tbls_sorted.sort_by_key(|t| t.last_seq);
        let mut chosen_sorted = chosen.clone();
        chosen_sorted.sort_by_key(|t| t.last_seq);
        if chosen_sorted.is_empty() {
            return (vec![], 0);
        }

        let start_seq = chosen_sorted[0].last_seq;
        let start_idx = tbls_sorted.partition_point(|t| t.last_seq < start_seq);
        let mut compact_tbls: Vec<TableMeta> = Vec::new();
        let mut ci = 0usize;
        let mut ti = start_idx;
        while ti < tbls_sorted.len() && ci < chosen_sorted.len() && compact_tbls.len() < COMPACT_N {
            if tbls_sorted[ti].last_seq <= chosen_sorted[ci].last_seq {
                compact_tbls.push(tbls_sorted[ti].clone());
                if tbls_sorted[ti].last_seq == chosen_sorted[ci].last_seq {
                    ci += 1;
                }
                ti += 1;
            } else {
                break;
            }
        }
        if ci == chosen_sorted.len() && compact_tbls.len() >= 2 {
            return (compact_tbls, truncate_id);
        }
        if compact_tbls.len() >= 2 {
            return (compact_tbls, 0);
        }
        return (vec![], 0);
    }

    // Size-tiered rule
    let mut tbls_sorted = tables.to_vec();
    tbls_sorted.sort_by_key(|t| t.last_seq);
    let throttle = (COMPACT_RATIO * MAX_SKIP_LIST as f64).round() as u64;
    let mut compact_tbls: Vec<TableMeta> = Vec::new();
    let mut i = 0usize;
    while i < tbls_sorted.len() {
        while i < tbls_sorted.len()
            && tbls_sorted[i].estimated_size < throttle
            && compact_tbls.len() < COMPACT_N
        {
            if i > 0
                && compact_tbls.is_empty()
                && tbls_sorted[i].estimated_size + tbls_sorted[i - 1].estimated_size < max_capacity
            {
                compact_tbls.push(tbls_sorted[i - 1].clone());
            }
            compact_tbls.push(tbls_sorted[i].clone());
            i += 1;
        }
        if !compact_tbls.is_empty() {
            if compact_tbls.len() == 1 {
                if i < tbls_sorted.len()
                    && compact_tbls[0].estimated_size + tbls_sorted[i].estimated_size < max_capacity
                {
                    compact_tbls.push(tbls_sorted[i].clone());
                } else {
                    compact_tbls.clear();
                    i += 1;
                    continue;
                }
            }
            break;
        }
        i += 1;
    }
    if compact_tbls.len() >= 2 {
        return (compact_tbls, 0);
    }
    (vec![], 0)
}

// F104 — streaming `do_compact`. The pre-F104 implementation built a
// `chunks: Vec<(Vec<IterItem>, u64)>` accumulator that materialized EVERY
// kept entry as a cloned `IterItem { key: Vec<u8>, value: Vec<u8>, ... }`
// (~150 B/entry for VP-path workloads). At 38 M entries per 5 GB partition
// this Vec alone was ~6 GB; with 4 concurrent partitions that's ~24 GB of
// pure accumulator overhead, which combined with input + output SST byte
// buffers explained the user-reported 44 GB single-PS RSS during
// `compact ALL`.
//
// The streaming version below builds each ≈512 MB chunk inline within the
// merge loop: when `current_builder` exceeds `max_chunk`, finalize and
// append immediately, then start a fresh builder. Peak intermediate state
// is one in-progress `SstBuilder` (≈current_chunk bytes) instead of the
// full output materialized as IterItem clones. The Go reference
// (Go autumn `range_partition/compaction.go`
// `doCompact`, L257-329) uses the same pattern; the Rust port had
// regressed to a Vec accumulator.
//
// Crash semantics are unchanged: `save_table_locs_raw` at the end remains
// the single atomic commit point. Any chunks appended to row_stream
// before that commit are orphan bytes if we crash, recoverable via the
// pre-existing meta_stream-authoritative recovery path.
/// F135 — route a single row_stream append through P-bulk's StreamClient.
///
/// **Why this matters:** flush is owned by P-bulk, which holds its own
/// `StreamClient` with its own per-stream commit-tracking state. If
/// compaction independently appends to row_stream via P-log's `part_sc`,
/// the two clients each carry their own commit watermark. When one client's
/// stale `commit` field hits the ExtentNode replicas, the server truncates
/// data written by the other client (commit-protocol step 5). Result: SST
/// bytes from one writer are silently destroyed mid-flight, surfacing later
/// as `invalid meta_len` on PS restart (witnessed 2026-05-03).
///
/// The fix is to funnel ALL row_stream appends through P-bulk's single
/// StreamClient. Flush does so via `FlushReq`; compaction does so via
/// `RowAppendReq`. P-log → P-bulk hand-off is a oneshot per request, so
/// callers see the same `AppendResult` shape as a direct append.
///
/// Pre-this we also kept an in-thread fallback when P-bulk failed to spawn
/// (`row_append_tx == None`, append via P-log's `part_sc`). That kept the
/// single-writer property by accident — flush had a matching fallback, so
/// in the spawn-failed case both writers happened to be `part_sc`. The
/// fallback is gone: `open_partition` returns Err on P-bulk spawn failure,
/// so this function's contract is now type-level — `row_append_tx` is
/// always live.
///
/// **F255** — `RowAppendReq` carries no invalidate flag; the invalidate is
/// performed as a synchronous P-log → P-bulk BARRIER by `handle_split_part`
/// before the manager seals the row_stream tail (see `RowInvalidateBarrierReq`
/// in lib.rs). By the time `do_compact` runs, any prior split's barrier
/// has already drained P-bulk's inflight FU to zero and invalidated
/// `bulk_sc`'s stale per-stream worker cache, so `compact_row_append`'s
/// append is guaranteed to land on a fresh, post-seal worker. No per-chunk
/// flag-checking — the structural barrier replaces the lazy fetch-and-
/// clear approach (the lazy form had a window where a FlushReq with
/// `invalidate=false` could race a RowAppendReq with `invalidate=true`
/// inside P-bulk's cap=2 FuturesUnordered; coco /arch found this 2026-06-02).
async fn compact_row_append(
    row_append_tx: &futures::channel::mpsc::Sender<crate::RowAppendReq>,
    row_stream_id: u64,
    sst_bytes: Bytes,
) -> Result<autumn_stream::AppendResult> {
    let (resp_tx, resp_rx) = futures::channel::oneshot::channel();
    let req = crate::RowAppendReq {
        sst_bytes,
        row_stream_id,
        resp_tx,
    };
    row_append_tx
        .clone()
        .send(req)
        .await
        .map_err(|_| anyhow::anyhow!("P-bulk row_append channel closed"))?;
    resp_rx
        .await
        .map_err(|_| anyhow::anyhow!("P-bulk row_append response dropped"))?
}

/// Finalize one compaction-output chunk: build the SST bytes off the compio
/// runtime, account the per-partition compact rate, append to row_stream
/// through P-bulk's single writer, parse the paged SstReader (off-runtime
/// too), and push `(TableMeta, reader)` into `new_readers`. Returns the
/// chunk's byte size. Shared by `do_compact`'s in-loop and final chunk
/// emits — the only difference is that the final caller attaches
/// `set_discards` to the builder before calling.
async fn emit_compact_chunk(
    builder: SstBuilder,
    row_append_tx: &futures::channel::mpsc::Sender<crate::RowAppendReq>,
    row_stream_id: u64,
    rate_ctrl: &crate::RateController,
    chunk_last_seq: u64,
    new_readers: &mut Vec<(TableMeta, Arc<SstReader>)>,
) -> Result<u64> {
    // Build SST bytes off the compio runtime (spawn_blocking): ~256 MiB
    // memcpy at max chunk + bloom finalize + meta encode + CRC32C — the same
    // offload flush_one_imm does via build_sst_bytes (note 17).
    let sst_bytes = compio::runtime::spawn_blocking(move || Bytes::from(builder.finish()))
        .await
        .map_err(|_| anyhow!("compact builder finish join failed"))?;
    let chunk_bytes = sst_bytes.len() as u64;
    // Sleep BEFORE the append so the counter reflects "intent to write".
    rate_ctrl.account_compact(chunk_bytes).await;
    // Route through P-bulk's StreamClient to preserve the single-writer
    // invariant on row_stream.
    let result = compact_row_append(row_append_tx, row_stream_id, sst_bytes.clone()).await?;
    let reader = compio::runtime::spawn_blocking(move || SstReader::from_bytes(sst_bytes))
        .await
        .map_err(|_| anyhow!("compact SstReader join failed"))??
        .into_paged(result.extent_id, result.offset, result.end - result.offset);
    new_readers.push((
        TableMeta {
            extent_id: result.extent_id,
            offset: result.offset,
            len: result.end - result.offset,
            estimated_size: chunk_bytes,
            last_seq: chunk_last_seq,
        },
        Arc::new(reader),
    ));
    Ok(chunk_bytes)
}

// clippy false-positive: every `part.borrow_mut()` here is `drop(p)`-ed before
// the following `.await` (the F148-A publish invariant requires exactly this —
// no await between the borrow_mut drop and the meta-stream mpsc send). The lint
// flags the borrow because awaits exist later in the fn; it doesn't track the drop.
#[allow(clippy::await_holding_refcell_ref)]
pub(crate) async fn do_compact(
    part: &Rc<RefCell<PartitionData>>,
    tbls: Vec<TableMeta>,
    major: bool,
) -> Result<CompactStats> {
    if tbls.is_empty() {
        return Ok(CompactStats {
            input_tables: 0,
            output_tables: 0,
            entries_kept: 0,
            entries_discarded: 0,
            output_bytes: 0,
        });
    }

    let input_tables = tbls.len();
    let compact_keys: HashSet<(u64, u64)> = tbls.iter().map(|t| t.loc()).collect();

    let (readers, row_stream_id, meta_stream_id, rg, part_sc, row_append_tx, rate_ctrl) = {
        let p = part.borrow();
        let mut rds: Vec<Arc<SstReader>> = Vec::new();
        for t in &tbls {
            if let Some(idx) = p.tables.iter().position(|x| x.loc() == t.loc()) {
                rds.push(p.sst_readers[idx].clone());
            }
        }
        (
            rds,
            p.row_stream_id,
            p.meta_stream_id,
            p.rg.clone(),
            p.stream_client.clone(),
            p.row_append_tx.clone(),
            p.rate_ctrl.clone(),
        )
    };

    if readers.is_empty() {
        return Ok(CompactStats {
            input_tables,
            output_tables: 0,
            entries_kept: 0,
            entries_discarded: 0,
            output_bytes: 0,
        });
    }

    // F262: async window iteration directly over the (paged) inputs — one
    // 8 MiB bulk read per window, cache-bypassing (scan-resistant). Replaces
    // Stage-1's materialized_for_iteration whole-SST transient residency;
    // peak read-side memory = inputs × one window instead of Σ input bytes.
    let mut readers_with_meta: Vec<(Arc<SstReader>, u64)> = readers
        .iter()
        .zip(tbls.iter())
        .map(|(r, t)| (r.clone(), t.last_seq))
        .collect();
    readers_with_meta.sort_by_key(|r| std::cmp::Reverse(r.1));

    let iters: Vec<AsyncTableIterator> = readers_with_meta
        .iter()
        .map(|(r, _)| {
            AsyncTableIterator::new(
                r.clone(),
                part_sc.clone(),
                FetchMode::Window(SCAN_READ_WINDOW_BYTES),
            )
        })
        .collect();
    let mut merge = AsyncMergeIterator::new(iters);
    merge.rewind().await?;

    let mut discards = get_discards(&readers);

    // log_extent_ids is needed by `valid_discard` to filter out discards
    // that point at extents already truncated from log_stream. Fetch it
    // once up front (cheap — one StreamInfo RPC).
    let log_stream_id = part.borrow().log_stream_id;
    // `log_extent_ids` is load-bearing for the output vp_head below, so a fetch
    // failure MUST NOT be swallowed into an empty list (coco P1): an empty list
    // makes `compaction_output_vp_head` fall back to `(0, 0)`, and recovery then
    // takes the no-replay branch (`chosen_pos == usize::MAX` with a non-empty
    // table set) → the acked-but-un-flushed WAL tail is lost. Abort the
    // compaction instead — this is BEFORE any row_stream append, so nothing is
    // half-published; the maintenance loop retries next tick.
    let log_extent_ids = part_sc.get_stream_info(log_stream_id).await?.extent_ids;

    // The output SSTs' vp_head (recovery replay-start) = MAX over the INPUT
    // SSTs' vp_heads by stream position (the newest input's content boundary) —
    // NOT the live write cursor `p.vp_*`. The live cursor sits past the
    // acked-but-un-flushed tail → those writes fall outside the replay window
    // and are lost on crash. MAX advances the floor for GC while staying behind
    // the un-flushed tail. See `compaction_output_vp_head` + regressions
    // `system_compact_unflushed_vp_head` / `system_gc_multiversion_same_extent`.
    let (compact_vp_eid, compact_vp_off) = compaction_output_vp_head(
        readers.iter().map(|r| (r.vp_extent_id, r.vp_offset)),
        &log_extent_ids,
    );

    let now = now_secs();
    let max_chunk = 2 * MAX_SKIP_LIST as usize;

    // F168: yield to other tasks on this compio runtime every
    // COMPACT_YIELD_EVERY entries. Pre-F168 the merge loop ran up to
    // `max_chunk = 512 MiB` of entries inline (~16M entries, ~1-2s of
    // CPU) before the chunk-emit `await` released the event loop —
    // long enough to stall client puts/gets to the same partition
    // for the entire duration. Heartbeat lives on PS-main (different
    // runtime) so it wasn't lost, but the thread-per-core principle
    // was violated. The yield cost is negligible (one poll
    // round-trip per 1000 entries, which is <1 µs amortised against
    // ~100ns of per-entry encode work).
    const COMPACT_YIELD_EVERY: usize = 1000;
    let mut entries_since_yield: usize = 0;

    // Streaming output state. `current_builder` accumulates the in-progress
    // chunk; when its byte budget is exceeded we finalize, append to
    // row_stream, push (TableMeta, Arc<SstReader>) into new_readers, and
    // start a fresh builder.
    let mut current_builder = SstBuilder::new(compact_vp_eid, compact_vp_off);
    let mut current_size: usize = 0;
    let mut chunk_last_seq: u64 = 0;
    let mut prev_user_key: Option<Vec<u8>> = None;
    let mut entries_kept = 0usize;
    let mut entries_discarded = 0usize;
    let mut output_bytes = 0u64;
    let mut new_readers: Vec<(TableMeta, Arc<SstReader>)> = Vec::new();

    while merge.valid() {
        // Snapshot the current item's needed fields. We can't hold the
        // `&IterItem` borrow across `merge.next()` (mutable borrow), and
        // an intermediate chunk emit is async, so copy out.
        let (raw_key, raw_op, raw_value, raw_expires) = {
            let item = match merge.item() {
                Some(i) => i,
                None => break,
            };
            (
                item.key.clone(),
                item.op,
                item.value.clone(),
                item.expires_at,
            )
        };
        // F262: a block-read failure aborts the compaction (Err) — the old
        // sync iterator silently went invalid on error, which would have
        // TRUNCATED the merge output once reads became network-backed.
        merge.next().await?;
        let raw_ts = parse_ts(&raw_key);

        let user_key = parse_key(&raw_key).to_vec();
        if prev_user_key.as_deref() == Some(&user_key) {
            bump_discards_for_dropped_entry(&mut discards, raw_op, &raw_value);
            entries_discarded += 1;
            continue;
        }
        prev_user_key = Some(user_key);

        if !in_range(&rg, prev_user_key.as_ref().unwrap()) {
            bump_discards_for_dropped_entry(&mut discards, raw_op, &raw_value);
            entries_discarded += 1;
            continue;
        }

        if major {
            if raw_op == 2 {
                bump_discards_for_dropped_entry(&mut discards, raw_op, &raw_value);
                entries_discarded += 1;
                continue;
            }
            if raw_expires > 0 && raw_expires <= now {
                bump_discards_for_dropped_entry(&mut discards, raw_op, &raw_value);
                entries_discarded += 1;
                continue;
            }
        }

        if raw_ts > chunk_last_seq {
            chunk_last_seq = raw_ts;
        }

        let entry_size = raw_key.len() + raw_value.len() + 20;
        if current_size + entry_size > max_chunk && !current_builder.is_empty() {
            // Finalize this chunk inline. Intermediate chunks carry NO
            // discards; only the final chunk after the loop attaches the
            // aggregated discard map (matches pre-F104 behaviour where
            // `last_chunk_idx` was the only chunk to call set_discards).
            let builder = std::mem::replace(
                &mut current_builder,
                SstBuilder::new(compact_vp_eid, compact_vp_off),
            );
            output_bytes += emit_compact_chunk(
                builder,
                &row_append_tx,
                row_stream_id,
                &rate_ctrl,
                chunk_last_seq,
                &mut new_readers,
            )
            .await?;
            current_size = 0;
            chunk_last_seq = raw_ts;
        }

        current_builder.add(&raw_key, raw_op, &raw_value, raw_expires);
        current_size += entry_size;
        entries_kept += 1;

        // F168: cooperative yield to keep the compio runtime responsive
        // for other tasks (partition_loop, ps-conn, etc.).
        entries_since_yield += 1;
        if entries_since_yield >= COMPACT_YIELD_EVERY {
            yield_to_runtime().await;
            entries_since_yield = 0;
        }
    }

    valid_discard(&mut discards, &log_extent_ids);

    // Final chunk: attach aggregated discards before finalize. If the
    // builder is empty (loop yielded zero kept entries OR the last
    // entry's chunk-emit consumed the previous in-progress builder and
    // the loop exited before pushing a new entry — only possible if the
    // merge iterator went invalid right after an emit), then there's no
    // last chunk to attach to; pin discards to the most recently emitted
    // reader's TableMeta instead. This case is rare but keeps GC's
    // discard-driven extent reclamation correct.
    if !current_builder.is_empty() {
        let mut builder = std::mem::replace(
            &mut current_builder,
            SstBuilder::new(compact_vp_eid, compact_vp_off),
        );
        builder.set_discards(discards.clone());
        output_bytes += emit_compact_chunk(
            builder,
            &row_append_tx,
            row_stream_id,
            &rate_ctrl,
            chunk_last_seq,
            &mut new_readers,
        )
        .await?;
    } else if !new_readers.is_empty() {
        // Rare boundary case: the loop's last item exactly tipped the chunk
        // size budget, so the in-loop emit consumed the builder and the loop
        // exited with an empty trailing builder. With no final chunk to carry
        // the aggregated discards, defer them to the next major compaction
        // that touches one of these output SSTs — the same outcome as a no-op
        // set_discards on an empty builder. If this ever becomes a GC blocker,
        // emit a tiny discards-only SST here instead.
        tracing::debug!(
            "compact: last chunk emit consumed builder before loop exit; \
             discards (extents={}) deferred to next compaction",
            discards.len()
        );
    }

    let output_tables = new_readers.len();

    if new_readers.is_empty() {
        // No new SSTs emitted (input had no kept entries). Just remove
        // old tables and persist meta.
        let mut p = part.borrow_mut();
        remove_compacted_tables(&mut p, &compact_keys);
        let tables_snapshot = p.tables.clone();
        let floors_snapshot = crate::snapshot_fence_floors(&p);
        drop(p);
        // F148-A invariant — DO NOT introduce an `.await` between the
        // borrow_mut drop above and the mpsc send inside
        // `save_table_locs_raw` below. See the matching comment in
        // `flush_one_imm` (lib.rs). The invariant guarantees that
        // concurrent flush + compact publishers (running as separate
        // tasks on the single-threaded P-log compio runtime) cannot
        // produce a stale-snapshot meta_stream checkpoint: borrow_mut
        // order = mpsc-send order = meta_stream record order, so the
        // latest record always reflects the latest in-memory state.
        save_table_locs_raw(
            &part_sc,
            meta_stream_id,
            &tables_snapshot,
            compact_vp_eid,
            compact_vp_off,
            log_extent_ids.len() as u32,
            floors_snapshot,
        )
        .await?;
        return Ok(CompactStats {
            input_tables,
            output_tables: 0,
            entries_kept: 0,
            entries_discarded,
            output_bytes: 0,
        });
    }

    // Drop local input-reader Arc clones BEFORE the swap. The partition's
    // own `sst_readers` Vec still holds them via separate Arcs, so this
    // doesn't free memory yet — but after the swap removes them from the
    // partition's Vec, the Arc count drops to zero and the input SST
    // bytes are released. Without this drop, `readers` would keep the
    // Arc count at >=1 and the memory would be retained until function
    // return.
    drop(merge);
    drop(readers);
    drop(readers_with_meta);

    let (tables_snapshot, floors_snapshot) = {
        let mut p = part.borrow_mut();
        // F252: locate the position of the OLDEST input table BEFORE
        // removing the compaction inputs. The compaction output
        // logically replaces those inputs in age order (its newest
        // contained seq is bounded by the input set's last_seq), so it
        // must be inserted at the SAME position — NOT appended at the
        // newest end. Appending breaks the
        // `sst_readers.iter().rev() = newest first` lookup contract
        // when a flush completed during the compaction await window:
        // that newer SST sits at a lower index than the compaction
        // output, and a Get for a key updated by that newer flush
        // walks the compaction output first (stale value) and never
        // reaches the newer flush. Surfaced as the chaos test's
        // fence+flush data-loss bug and the in-process f250 reproducer.
        let insert_at = p
            .tables
            .iter()
            .position(|tm| compact_keys.contains(&tm.loc()))
            .unwrap_or(p.tables.len());
        remove_compacted_tables(&mut p, &compact_keys);
        for (offset, (tbl_meta, reader)) in new_readers.into_iter().enumerate() {
            let idx = insert_at + offset;
            p.sst_readers.insert(idx, reader);
            p.tables.insert(idx, tbl_meta);
        }
        (p.tables.clone(), crate::snapshot_fence_floors(&p))
    };

    // F148-A invariant — see flush_one_imm in lib.rs for the full
    // statement. No `.await` may be introduced between the borrow_mut
    // drop and the mpsc send inside `save_table_locs_raw`.
    save_table_locs_raw(
        &part_sc,
        meta_stream_id,
        &tables_snapshot,
        compact_vp_eid,
        compact_vp_off,
        log_extent_ids.len() as u32,
        floors_snapshot,
    )
    .await?;
    Ok(CompactStats {
        input_tables,
        output_tables,
        entries_kept,
        entries_discarded,
        output_bytes,
    })
}

pub(crate) fn remove_compacted_tables(
    part: &mut PartitionData,
    compact_keys: &HashSet<(u64, u64)>,
) {
    let mut i = 0;
    while i < part.tables.len() {
        if compact_keys.contains(&part.tables[i].loc()) {
            part.tables.remove(i);
            part.sst_readers.remove(i);
        } else {
            i += 1;
        }
    }
}

/// F201: classify a `run_gc` failure into a cooldown duration. Scans
/// the anyhow chain for sentinel substrings used by recoverable
/// cooperative races: `"precondition failed"` (from
/// `AppError::Precondition` Display — manager rejects punch_holes
/// while `ec_conversion_inflight` per F138/F145) and `"eversion
/// mismatch"` (from autumn-stream's private `EversionStale` sentinel
/// — stale `extent_info_cache` after an EC bump). Soft cooldown lets
/// these recover in ~30 s; hard cooldown applies to anything else
/// (timeouts, irrecoverable EC shard shortage, etc.).
pub(crate) fn classify_gc_failure_cooldown(
    e: &anyhow::Error,
    soft: Duration,
    hard: Duration,
) -> Duration {
    let recoverable = e.chain().any(|cause| {
        let msg = cause.to_string();
        msg.contains("precondition failed") || msg.contains("eversion mismatch")
    });
    if recoverable {
        soft
    } else {
        hard
    }
}

pub(crate) fn get_discards(readers: &[Arc<SstReader>]) -> HashMap<u64, i64> {
    let mut out: HashMap<u64, i64> = HashMap::new();
    for r in readers {
        for (&eid, &sz) in &r.discards {
            *out.entry(eid).or_insert(0) += sz;
        }
    }
    out
}

pub(crate) fn valid_discard(discards: &mut HashMap<u64, i64>, extent_ids: &[u64]) {
    let idx: HashSet<u64> = extent_ids.iter().copied().collect();
    discards.retain(|eid, _| idx.contains(eid));
}

/// F-DF-WALDEBT: dead bytes on the OPEN (last) log extent, read from the
/// aggregated discard map. This is precisely the entry `gc_debt` EXCLUDES —
/// `valid_discard(sealed_extents)` filters to `extent_ids[..len-1]`, dropping
/// the tail — so `gc_debt_bytes` (sealed) and this (open) are DISJOINT and sum
/// to the partition's full reclaimable WAL debt. Rides the already-persisted
/// SST discard maps, so it needs no bespoke counter and survives restart
/// exactly like `gc_debt`. Returns 0 when the tail has no discard entry (all
/// live) or there is no extent.
pub(crate) fn open_tail_dead_bytes(discards: &HashMap<u64, i64>, extent_ids: &[u64]) -> u64 {
    extent_ids
        .last()
        .and_then(|eid| discards.get(eid))
        .map(|v| (*v).max(0) as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// GC
// ---------------------------------------------------------------------------

/// F106 chunk size for `run_gc` streaming reads. Bounds peak GC RAM
/// (one chunk + partial-record carry).
///
/// F141 lowered the default from 64 MiB → 8 MiB after observing that a
/// single 64 MiB EC-subrange-read against a 1 GiB sealed log_stream
/// extent could stall extent-node 1's compio runtime for ~15 s,
/// causing foreground put fanout against partitions sharing that node
/// to time out at the StreamClient's 5 s ceiling. Smaller chunks keep
/// the extent-node's read I/O slot returning often enough that
/// foreground appends don't hit the timeout. F195: overridable via
/// `set_gc_read_chunk_bytes` (CLI flag `--gc-read-chunk-bytes`).
fn gc_read_chunk_bytes() -> u32 {
    *GC_READ_CHUNK_BYTES_CELL.get_or_init(|| 8 * 1024 * 1024)
}

/// F141: max records per GC append batch. Defaults to 256 to match
/// `MAX_WRITE_BATCH` on the foreground put path. F195: overridable
/// via `set_gc_batch_records`.
fn gc_batch_records() -> usize {
    *GC_BATCH_RECORDS_CELL.get_or_init(|| 256)
}

/// F141: max bytes per GC append batch. Defaults to 4 MiB so a single
/// `append_segments` payload is bounded regardless of how large the
/// individual VP values are. Hit the records cap first on small VPs;
/// hit the bytes cap first on large VPs. F195: overridable via
/// `set_gc_batch_bytes`.
fn gc_batch_bytes() -> usize {
    *GC_BATCH_BYTES_CELL.get_or_init(|| 4 * 1024 * 1024)
}

/// F141: GC log_stream rewrite throttle in bytes/sec. 0 = unlimited.
/// Default 64 MiB/s — bounded headroom relative to typical foreground
/// put traffic so GC doesn't starve client writes on the shared
/// log_stream worker / extent-node fanout. F195: overridable via
/// `set_gc_rate_bytes_per_sec`.
fn gc_rate_bytes_per_sec() -> u64 {
    *GC_RATE_BYTES_PER_SEC_CELL.get_or_init(|| 64 * 1024 * 1024)
}

/// Per-record metadata kept alongside the encoded segments for memtable
/// post-processing once `append_segments` returns the batch's offset.
struct GcRecord {
    user_key: Vec<u8>,
    internal_key: Vec<u8>,
    value_len: u32,
    expires_at: u64,
    /// Total bytes the record occupies in log_stream: 17 + key_len + value_len.
    record_size: u32,
}

/// F141: accumulates a batch of GC rewrites. Records are encoded into
/// `segments` (alternating header+key / value Bytes per record). The
/// per-record metadata in `pending` is consumed when the batch flushes
/// into the memtable, walking the tail offset returned by
/// `append_segments`.
struct GcWriteBatch {
    segments: Vec<Bytes>,
    pending: Vec<GcRecord>,
    bytes: u64,
    record_cap: usize,
    byte_cap: u64,
}

impl GcWriteBatch {
    fn new() -> Self {
        Self {
            segments: Vec::new(),
            pending: Vec::new(),
            bytes: 0,
            record_cap: gc_batch_records(),
            byte_cap: gc_batch_bytes() as u64,
        }
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    fn is_full(&self) -> bool {
        self.pending.len() >= self.record_cap || self.bytes >= self.byte_cap
    }
}

/// F141: simple wall-clock rate limiter. After each batch flush we add
/// the batch's bytes to a 1-second sliding window; if cumulative bytes
/// exceed what the budget allows for the elapsed time, sleep enough to
/// catch up. Window resets every second to bound drift.
struct GcRateLimiter {
    bytes_per_sec: u64,
    window_start: Instant,
    bytes_in_window: u64,
}

impl GcRateLimiter {
    fn new() -> Self {
        Self {
            bytes_per_sec: gc_rate_bytes_per_sec(),
            window_start: Instant::now(),
            bytes_in_window: 0,
        }
    }

    fn account(&mut self, bytes: u64) -> Option<Duration> {
        if self.bytes_per_sec == 0 {
            return None;
        }
        self.bytes_in_window = self.bytes_in_window.saturating_add(bytes);
        let elapsed = self.window_start.elapsed();
        if elapsed >= Duration::from_secs(1) {
            // Reset window — we earned a full second's worth of budget.
            self.window_start = Instant::now();
            self.bytes_in_window = 0;
            return None;
        }
        let target_secs = self.bytes_in_window as f64 / self.bytes_per_sec as f64;
        let target = Duration::from_secs_f64(target_secs);
        if target > elapsed {
            Some(target - elapsed)
        } else {
            None
        }
    }
}

/// F141 / F168: cooperative single-step yield. Lets other tasks on
/// this compio runtime (partition_loop, ps-conn,
/// background_flush_loop, etc.) make forward progress between
/// long stretches of inline CPU work that would otherwise starve
/// the event loop.
///
/// Pattern: `poll_fn` returns `Pending` once after waking itself, so
/// the runtime polls all OTHER ready tasks before returning to this
/// one. Lighter-weight than `compio::time::sleep(Duration::ZERO)`
/// which routes through the timer wheel.
///
/// F168 promoted from `gc_yield_now` (was GC-only) to a crate-private
/// helper and now also called from `do_compact`'s merge loop every
/// `COMPACT_YIELD_EVERY` entries (1000) — pre-F168 the inline merge
/// loop ran up to `2 * MAX_SKIP_LIST = 512 MiB` of entries with NO
/// `.await`, blocking the P-log compio runtime for ~1-2 SECONDS on
/// large compactions. Client puts/gets to the same partition stalled
/// for that duration. Heartbeat (PS-main runtime) was unaffected —
/// it lives on a different thread — but the thread-per-core
/// principle ("compio runtime should NEVER block due to CPU busy")
/// was violated. This periodic yield enforces the principle while
/// keeping the per-yield cost tiny (one poll round-trip).
async fn yield_to_runtime() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await
}

/// Backwards-compat alias for the historical name.
async fn gc_yield_now() {
    yield_to_runtime().await
}

/// Flush a non-empty `GcWriteBatch`: send all queued records as ONE
/// `append_segments`, then walk the returned tail offset to insert the
/// new VPs into the memtable in a single `insert_batch`. The flush is
/// the only place GC awaits the network, so it's also the place where
/// the rate limiter and yield run.
async fn flush_gc_batch(
    part: &Rc<RefCell<PartitionData>>,
    log_stream_id: u64,
    part_sc: &Rc<StreamClient>,
    batch: &mut GcWriteBatch,
    rate_limiter: &mut GcRateLimiter,
    rate_ctrl: &std::sync::Arc<crate::RateController>,
    moved: &mut usize,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let segments = std::mem::take(&mut batch.segments);
    let pending = std::mem::take(&mut batch.pending);
    let batch_bytes = std::mem::replace(&mut batch.bytes, 0);
    let n = pending.len();

    let result = part_sc.append_segments(log_stream_id, segments).await?;

    let mut cur_offset = result.offset;
    let mut insert_items: Vec<(Vec<u8>, MemEntry, u64)> = Vec::with_capacity(n);
    for r in pending {
        // F186 fix: V1 envelope adds 5 bytes (sentinel+length) before the
        // V0 inner header, so value bytes start at +22 not +17. See
        // `finish_write_batch` for the full layout discussion.
        let new_vp = ValuePointer {
            extent_id: result.extent_id,
            offset: cur_offset + 22 + r.internal_key.len() as u64,
            len: r.value_len as u64,
        };
        let mem_entry = MemEntry {
            op: 1 | OP_VALUE_POINTER,
            value: new_vp.encode().to_vec(),
            expires_at: r.expires_at,
        };
        let write_size = (r.user_key.len() + r.value_len as usize + 32) as u64;
        insert_items.push((r.internal_key, mem_entry, write_size));
        cur_offset = cur_offset.saturating_add(r.record_size as u64);
    }

    {
        let mut p = part.borrow_mut();
        p.vp_extent_id = result.extent_id;
        p.vp_offset = result.end;
        p.active.insert_batch(insert_items);
        // F-RECOVERY-UNBOUNDED BUG1: the GC multi-frag rewrite re-appends live
        // values to log_stream and seeds them into the active memtable, so they
        // join the un-flushed LOG window that recovery would replay. Track the
        // appended bytes for an accurate F120-B force-rotate gap.
        p.active.add_log_bytes(batch_bytes);
    }
    *moved += n;

    // Cooperative yield: even if the rate limiter has no budget to
    // burn, give partition_loop / ps-conn a turn before the
    // next batch.
    gc_yield_now().await;

    if let Some(sleep_dur) = rate_limiter.account(batch_bytes) {
        compio::time::sleep(sleep_dur).await;
    }
    // F196 D-r7: per-partition gc rate cap (replaces F188's bg cap).
    rate_ctrl.account_gc(batch_bytes).await;

    Ok(())
}

/// F130 — when compaction drops an entry (dedup, range filter,
/// tombstone, expired), bump the per-extent discard counter for every
/// log_stream byte the dropped entry was holding live.
///
/// Single-VP path (existing): one VP → one fragment → one extent.
/// Multi-frag path (F130): one mfvp → N fragments → potentially N
/// extents. Per F130's full-rewrite invariant, when a multi-frag mfvp
/// is shadowed by a newer entry (whether a fresh foreground Put or a
/// GC rewrite), every fragment of the shadowed mfvp is truly dead —
/// the newer entry has its own fresh fragment list. So we can blindly
/// bump discards for every frag.
fn bump_discards_for_dropped_entry(discards: &mut HashMap<u64, i64>, op: u8, raw_value: &[u8]) {
    if op & OP_VALUE_POINTER != 0 && raw_value.len() >= VALUE_POINTER_SIZE {
        let vp = ValuePointer::decode(raw_value);
        *discards.entry(vp.extent_id).or_insert(0) += vp.len as i64;
    }
    // F129/F186 — multi-frag VP discard handling deleted with the rest
    // of the server-side multipart machinery. Stripe-write chunks are
    // now normal Puts under reserved-namespace keys, so each chunk's
    // single-VP discard already covers its bytes via the branch above.
}

/// Seal state gate for a GC punch decision: returns the `sealed_length` ONLY for
/// an authoritatively-sealed extent, else `None` (skip — never GC it).
///
/// A GC punch is DESTRUCTIVE + irreversible, so it must NEVER fire on stale or
/// open state. The trap: `StreamClient::alloc_new_extent` caches the NEW tail
/// after a seal-and-roll but does NOT evict the OLD one, so a now-sealed extent
/// can linger in `extent_info_cache` as its pre-seal OPEN snapshot
/// (`sealed=false, sealed_length=0`). The pre-F-fix F201 fast-punch trusted that
/// stale `sealed_length==0` and punched a sealed extent full of live
/// ValuePointers as if empty → silent big-value loss (chaos seed=583: extent
/// sealed at 7.8 MB read back as stale `sealed_length=0`, punched, GET of the
/// VP'd key returned NotFound).
///
/// The gate: `sealed` is IMMUTABLE once set, so a cached `sealed=true` is always
/// trustworthy; only a `sealed=false` snapshot can be stale. We therefore SKIP
/// any candidate that reads NOT sealed (`None`) — conservatively refusing to GC
/// anything not authoritatively known sealed. An OPEN extent always reports
/// `sealed_length==0` but is NOT empty (its committed length is `last_synced`,
/// invisible here; it is the live tail or holds uncommitted data), so skipping is
/// exactly right. A stale `sealed=false` snapshot of a genuinely-sealed extent is
/// likewise skipped — data-safe (never punched → no loss); it is reclaimed a tick
/// later once its cache refreshes (a read / EC-invalidate / restart).
///
/// We deliberately do NOT `invalidate_extent_cache` + refetch here to "freshen" a
/// `sealed=false` read: an extra GC→manager RPC per stale candidate shifts P-log
/// timing and (chaos seed=603) exposed a SEPARATE pre-existing split-child-open
/// wedge. The conservative skip is data-safe with ZERO added RPCs (one
/// `get_extent_info` per candidate, same as the pre-fix baseline). Trade-off: a
/// stale-cached-as-open sealed extent isn't reclaimed until its cache refreshes —
/// a GC-promptness gap, never a data-loss or correctness gap.
///
/// Returns `Some(sealed_length)` for a confirmed-sealed extent (incl. `0` for a
/// genuinely sealed-empty one — safe to fast-punch). EVERY path that feeds
/// `run_gc` / `punch_holes` (Auto candidate selection AND Force GC) goes through
/// this, carrying the validated `(eid, sealed_length)` to execution — so there is
/// no check/use split where a re-read could resurrect the stale value.
async fn authoritative_sealed_length(part_sc: &StreamClient, eid: u64) -> Option<u64> {
    let info = match part_sc.get_extent_info(eid).await {
        Ok(i) => i,
        Err(e) => {
            tracing::warn!("GC extent_info {eid}: {e}");
            return None;
        }
    };
    // Trust the immutable `sealed` flag; skip anything not authoritatively sealed.
    // No invalidate+refetch (see doc: avoids the seed=603 timing regression).
    if !info.sealed {
        return None;
    }
    Some(info.sealed_length)
}

pub(crate) async fn run_gc(
    part: &Rc<RefCell<PartitionData>>,
    extent_id: u64,
    sealed_length: u64,
) -> Result<()> {
    let (log_stream_id, rg, part_sc, rate_ctrl) = {
        let p = part.borrow();
        (
            p.log_stream_id,
            p.rg.clone(),
            p.stream_client.clone(),
            p.rate_ctrl.clone(),
        )
    };

    // F106 streaming: read the sealed extent in `gc_read_chunk_bytes()`
    // slices, decoding complete records as they arrive. The partial
    // record at the chunk tail (if any) is carried into the next chunk.
    // Pre-F106 this slurped the whole extent into one Vec, which (a)
    // peaked at ~3 GB RAM on extent 10 of the user's 4-partition
    // workload, and (b) tripped macOS pread INT_MAX (also addressed by
    // F105 read_bytes_from_extent chunking — F106 keeps memory bounded
    // even when F105 is forced to materialise the full read).
    //
    // F141 batching: rewrites accumulate into `batch` and flush via
    // `append_segments` when the batch hits its record/byte caps.
    // F178 made every append durable via the per-extent coalescer
    // (group commit), so the per-record fsync storm of pre-F141 is
    // structurally impossible — multiple in-flight batch appends share
    // one coalesced fsync per wake-cycle.
    // F130 multi-frag VP rewrite pre-pass deleted with F186 — stripe-
    // write chunks are now plain Puts under reserved-namespace keys, so
    // each chunk has its OWN single-VP entry in memtable that the
    // existing process_gc_chunk loop below correctly rewrites or skips.
    // No special multi-frag handling needed.

    let chunk_bytes = gc_read_chunk_bytes();
    let mut moved = 0usize;
    let mut cur: u64 = 0;
    let mut carry: Vec<u8> = Vec::new();
    let mut batch = GcWriteBatch::new();
    let mut rate_limiter = GcRateLimiter::new();

    while cur < sealed_length {
        let want = (sealed_length - cur).min(chunk_bytes as u64);
        let (chunk, _end) = part_sc.read_bytes_from_extent(extent_id, cur, want).await?;
        if chunk.is_empty() {
            break;
        }
        let chunk_len = chunk.len() as u64;
        // `buf` = carry (the unconsumed record-tail preceding this chunk) ++
        // chunk, so buf[0] sits at absolute extent offset (read offset -
        // carry.len()). `cur` is still the read offset here (incremented below).
        let buf_base_offset = cur - carry.len() as u64;
        cur = cur.saturating_add(chunk_len);

        let buf: Vec<u8> = if carry.is_empty() {
            chunk
        } else {
            let mut b = std::mem::take(&mut carry);
            b.extend_from_slice(&chunk);
            b
        };

        let consumed = process_gc_chunk(
            part,
            log_stream_id,
            extent_id,
            &rg,
            &part_sc,
            buf_base_offset,
            &buf,
            &mut moved,
            &mut batch,
            &mut rate_limiter,
            &rate_ctrl,
        )
        .await?;
        if consumed < buf.len() {
            carry = buf[consumed..].to_vec();
        }

        // F141 read-side throttle: a 64 MiB chunk read against an
        // EC-converted, replicated source extent (e.g. extent 20 with
        // sealed_length ≈ 1 GiB) is not free — it competes with
        // foreground put fanout on the same extent-nodes. After each
        // chunk yield + bill the bytes against the same rate limiter
        // we use for writes, so GC's total log-layer footprint stays
        // bounded regardless of which side dominates.
        gc_yield_now().await;
        if let Some(sleep_dur) = rate_limiter.account(chunk_len) {
            compio::time::sleep(sleep_dur).await;
        }
        // F196 D-r7: per-partition gc rate cap on the read side (so
        // GC's read pressure is throttled too, not just the write side).
        rate_ctrl.account_gc(chunk_len).await;
    }

    if !carry.is_empty() {
        // A sealed extent's record stream should be byte-aligned; a
        // non-empty carry at the end means we either truncated mid-
        // record or saw corruption. Don't punch in that case — log loud.
        return Err(anyhow!(
            "run_gc extent {extent_id}: {} trailing bytes did not form a complete record; refusing to punch",
            carry.len()
        ));
    }

    // Final flush: F178 makes every append durable via the per-extent
    // coalescer, so by the time `flush_gc_batch` returns, the moved
    // values are durable on a quorum of replicas. punch_holes is safe
    // to fire after this.
    flush_gc_batch(
        part,
        log_stream_id,
        &part_sc,
        &mut batch,
        &mut rate_limiter,
        &rate_ctrl,
        &mut moved,
    )
    .await?;

    // F162 (MED-2): try-acquire writer pin on this extent BEFORE punch_holes.
    // If a `handle_get → resolve_value` reader is currently in-flight on this
    // extent (rare race window — they typically complete in milliseconds),
    // defer this extent's GC to the next 30-60 s tick rather than letting
    // the reader return spurious NotFound bytes when the manager processes
    // the punch_holes RPC and enqueues a physical delete.
    let pin = part.borrow().pin_for(extent_id);
    if !crate::try_acquire_writer_pin(&pin) {
        tracing::info!(
            extent_id,
            "F162: GC deferred — reader pin held; will retry on next gc tick"
        );
        return Ok(());
    }
    let punch_result = part_sc.punch_holes(log_stream_id, vec![extent_id]).await;
    crate::release_writer_pin(&pin);
    punch_result?;
    tracing::info!("GC: punched extent {extent_id}, moved {moved} entries");
    Ok(())
}

/// Process every complete record in `buf`, staging VP rewrites into
/// `batch` and flushing mid-chunk when the batch fills. Returns how
/// many bytes were consumed (always at a record boundary). The
/// remaining `buf.len() - consumed` bytes are an incomplete record
/// that must be carried into the next chunk.
async fn process_gc_chunk(
    part: &Rc<RefCell<PartitionData>>,
    log_stream_id: u64,
    extent_id: u64,
    rg: &Range,
    part_sc: &Rc<StreamClient>,
    // Absolute offset in `extent_id` where `buf[0]` sits (carry-tail of the
    // previous chunk ++ this chunk). Used to compute each scanned record's
    // absolute value offset for full VP-identity liveness matching.
    buf_base_offset: u64,
    buf: &[u8],
    moved: &mut usize,
    batch: &mut GcWriteBatch,
    rate_limiter: &mut GcRateLimiter,
    rate_ctrl: &std::sync::Arc<crate::RateController>,
) -> Result<usize> {
    // F158: decode via shared codec — handles both V0 (legacy on-disk) and V1
    // (post-F158 with CRC). On a V1 CRC failure we log + skip + continue,
    // matching the recover_partition decoder semantics.
    let mut cursor = 0usize;
    while cursor < buf.len() {
        let record_start = cursor;
        let (op, key_owned, value_owned, expires_at, val_off) =
            match crate::wal_record::decode_one(&buf[cursor..]) {
                crate::wal_record::DecodeOne::Ok(r) => {
                    let op = r.op;
                    let key = r.key.to_vec();
                    let value = r.value.to_vec();
                    let expires_at = r.expires_at;
                    let val_off = r.val_off;
                    cursor += r.total;
                    (op, key, value, expires_at, val_off)
                }
                crate::wal_record::DecodeOne::Incomplete => {
                    // Caller carries this partial record into the next chunk.
                    break;
                }
                crate::wal_record::DecodeOne::Corrupt { skip_bytes, reason } => {
                    // WAL-FAILSTOP (coco prod-audit P0 #2): a corrupt record
                    // means we can't trust which value pointers are live in
                    // this extent — continuing the GC scan past it could
                    // punch_holes / reclaim data still referenced by the
                    // corrupt (unparseable) record. Abort this GC pass loud
                    // instead of skipping; the classify-cooldown backs it off
                    // and recovery handles the corrupt replica.
                    return Err(anyhow::anyhow!(
                        "WAL-FAILSTOP: GC hit corrupt log_stream record on extent \
                         {extent_id} at offset {record_start} ({reason}, len={skip_bytes}) — \
                         aborting GC (refusing to punch holes past unparseable records)"
                    ));
                }
            };
        let key = key_owned.as_slice();
        let value = value_owned.as_slice();
        let val_len = value.len();
        let _ = expires_at; // used downstream by GcRecord builder

        if op & OP_VALUE_POINTER == 0 {
            continue;
        }
        let user_key = parse_key(key).to_vec();
        if !in_range(rg, &user_key) {
            continue;
        }

        // coco P0 #3 (F261): the paged-SST liveness lookup AWAITS, so the
        // sst_readers snapshot it ran against can go STALE mid-lookup — a
        // concurrent Put for this key can land in active AND be flushed all
        // the way into a NEW SST within the await window, where neither the
        // snapshot lookup nor an active/imm re-check sees it. Acting on the
        // stale verdict would rewrite the OLD value at a fresh (higher) seq
        // — resurrecting it over the new write. The dual hazard makes
        // "just skip on any change" wrong too: if the old value WAS still
        // live, skipping its rewrite and then punching the extent destroys
        // it. So: VALIDATE the snapshot after the await (P-log is single-
        // threaded — interleaving happens only at awaits, and there is no
        // await between this validation, the verdict use, and the seq
        // allocation below). If sst_readers changed, REDO the lookup
        // against a fresh snapshot; on repeated churn, abort this extent's
        // round (no punch — safe, retried next tick).
        let mut lookup_attempts = 0u32;
        let current: Option<(u8, Bytes, u64)> = loop {
            lookup_attempts += 1;
            if lookup_attempts > 4 {
                return Err(anyhow::anyhow!(
                    "gc liveness lookup: sst_readers changed on {} consecutive \
                     attempts (heavy flush/compact churn); aborting extent round",
                    lookup_attempts - 1
                ));
            }
            let p = part.borrow();
            let mem = p
                .active
                .seek_user_key(&user_key)
                .or_else(|| p.imm.iter().rev().find_map(|m| m.seek_user_key(&user_key)))
                .map(|e| (e.op, Bytes::from(e.value), e.expires_at));
            if mem.is_some() {
                break mem;
            }
            // F261: paged SST lookup awaits — snapshot + drop borrow.
            let readers: Vec<Arc<SstReader>> = p.sst_readers.to_vec();
            let sc = p.stream_client.clone();
            drop(p);
            let cache = crate::global_block_cache().clone();
            let mut found = None;
            for r in readers.iter().rev() {
                // coco P0: a read ERROR must abort this extent's GC
                // round (no punch) — folding it into "miss" deleted
                // still-referenced VP data.
                match lookup_in_sst_via(r, &user_key, &sc, &cache).await {
                    Ok(Some(e)) => {
                        found = Some(e);
                        break;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        return Err(e.context("gc liveness lookup (paged sst)"));
                    }
                }
            }
            // Post-await validation under one borrow:
            // (a) did the key appear in active/imm during the await (a
            //     concurrent Put)? → the SST verdict is superseded; loop —
            //     the next iteration takes the mem branch and judges
            //     against the NEW version.
            // (b) did sst_readers change (flush push / compact swap)? →
            //     the snapshot lookup may have missed a newer flushed
            //     version; loop with a fresh snapshot.
            // There is NO await between this validation, the verdict use,
            // and the seq allocation below (single-threaded P-log), so a
            // verdict that passes here cannot go stale before the rewrite
            // is staged.
            let p = part.borrow();
            let appeared_in_mem = p.active.seek_user_key(&user_key).is_some()
                || p.imm
                    .iter()
                    .rev()
                    .any(|m| m.seek_user_key(&user_key).is_some());
            let snapshot_stale = crate::sst_readers_changed(&p.sst_readers, &readers);
            drop(p);
            if appeared_in_mem || snapshot_stale {
                continue;
            }
            break found;
        };

        let Some((cur_op, cur_val, _)) = current else {
            continue;
        };
        if cur_op & OP_VALUE_POINTER == 0 || cur_val.len() < VALUE_POINTER_SIZE {
            continue;
        }
        // Full VP-identity liveness. The scanned record is the LIVE version of
        // its key ONLY if the current live VP points at THIS record's exact
        // bytes — extent_id AND absolute offset AND len. Comparing extent_id
        // alone (the pre-fix bug) misclassifies a SUPERSEDED older version of
        // the same key that happens to live in the same extent as "live"; GC
        // then relocates the OLD value with a fresh (higher) seq, reviving it
        // over the newer version and dropping the newer one on the next punch
        // (coco /arch P0; regression: tests/system_gc_multiversion_same_extent).
        let scanned_value_offset = buf_base_offset + record_start as u64 + val_off as u64;
        let vp = ValuePointer::decode(&cur_val);
        if vp.extent_id != extent_id
            || vp.offset != scanned_value_offset
            || vp.len != val_len as u64
        {
            continue;
        }

        // Stage the WAL record into the batch under a brief borrow_mut
        // (seq assignment + internal_key encode). No await happens
        // inside the borrow.
        // Stale-verdict protection lives in the validation loop ABOVE (the
        // post-await active/imm + sst_readers re-check) — NOT here. A naive
        // "skip if key present in active/imm" check at this point would be
        // WRONG: the mem branch finds the live VP in the memtable (the
        // normal recently-written case), and skipping its rewrite before
        // the punch would destroy live data.
        let internal_key = {
            let mut p = part.borrow_mut();
            p.seq_number += 1;
            let seq = p.seq_number;
            key_with_ts(&user_key, seq)
        };

        // F158: GC re-write also emits V1 envelope (sentinel + length +
        // payload + crc). Match the original GC behaviour of writing op=1
        // (no VP flag); recovery's `value.len() > VALUE_THROTTLE` fallback
        // at lib.rs:2891 still detects this as a VP entry on replay, since
        // GC only ever rewrites entries that were tagged VP in the source.
        // F177: caller-side memcpy unavoidable here — `value` is borrowed
        // from the chunk read buffer; can't move it. GC is bounded by
        // `gc_batch_bytes() = 4 MiB` per batch and runs cooperatively
        // (yield between chunks), so the copy is acceptable on this cold
        // path.
        let value_bytes = Bytes::copy_from_slice(value);
        let (hdr_seg, val_seg, crc_seg) =
            crate::wal_record::encode_v1_segments(1u8, &internal_key, value_bytes, expires_at);
        let record_size = (hdr_seg.len() + val_seg.len() + crc_seg.len()) as u32;
        batch.segments.push(hdr_seg);
        if !value.is_empty() {
            batch.segments.push(val_seg);
        }
        batch.segments.push(crc_seg);
        batch.bytes = batch.bytes.saturating_add(record_size as u64);
        batch.pending.push(GcRecord {
            user_key,
            internal_key,
            value_len: val_len as u32,
            expires_at,
            record_size,
        });

        if batch.is_full() {
            flush_gc_batch(
                part,
                log_stream_id,
                part_sc,
                batch,
                rate_limiter,
                rate_ctrl,
                moved,
            )
            .await?;
        }
    }
    Ok(cursor)
}

// ---------------------------------------------------------------------------
// Lookup helpers
// ---------------------------------------------------------------------------

pub(crate) fn lookup_in_memtable(mem: &Memtable, user_key: &[u8]) -> Option<(u8, Bytes, u64)> {
    mem.seek_user_key(user_key)
        .map(|e| (e.op, Bytes::from(e.value), e.expires_at))
}

/// F250 diag: return the newest seq for `user_key` in this SST (or 0).
/// Re-walks `lookup_in_sst`'s logic but returns the parsed seq instead
/// of the value. Used only by `MSG_DIAG_TRACE_KEY`.
///
/// `use_bloom`: when true, mirror the real GET path (return 0 if the
/// bloom says no). When false, skip the bloom and scan anyway — so a
/// bloom FALSE NEGATIVE (key present but bloom says absent) shows up as
/// a divergence between the two.
pub(crate) fn lookup_in_sst_seq_opt(reader: &SstReader, user_key: &[u8], use_bloom: bool) -> u64 {
    if use_bloom && !reader.bloom_may_contain(user_key) {
        return 0;
    }
    let target = key_with_ts(user_key, u64::MAX);
    let block_idx = reader.find_block_for_key(&target);
    let Ok(block) = reader.read_block(block_idx) else {
        return 0;
    };
    let n = block.num_entries();
    if n == 0 {
        return 0;
    }
    let mut lo = 0usize;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let Ok(key) = block.get_key(mid) else {
            return 0;
        };
        if key.as_slice() < target.as_slice() {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo < n {
        let Ok(key) = block.get_key(lo) else {
            return 0;
        };
        if parse_key(key.as_slice()) == user_key {
            return parse_ts(key.as_slice());
        }
    }
    0
}

/// F250 diag: FULL linear scan of every block/entry for `user_key`,
/// ignoring bloom AND `find_block_for_key`. Returns the newest (max)
/// seq found, or 0. If this finds the key where `lookup_in_sst_seq`
/// (binary-search-in-one-block) returns 0, the bug is in block
/// SELECTION / lookup, not data presence.
pub(crate) fn lookup_in_sst_seq_fullscan(reader: &SstReader, user_key: &[u8]) -> u64 {
    let mut best = 0u64;
    for bi in 0..reader.block_count() {
        let Ok(block) = reader.read_block(bi) else {
            continue;
        };
        for ei in 0..block.num_entries() {
            let Ok(key) = block.get_key(ei) else {
                continue;
            };
            if parse_key(key.as_slice()) == user_key {
                let ts = parse_ts(key.as_slice());
                if ts > best {
                    best = ts;
                }
            }
        }
    }
    best
}

/// F261: async point lookup that works for paged AND resident readers —
/// exact mirror of `lookup_in_sst` (incl. the F250 next-block hop), with
/// block reads going through `read_block_via` (bounded global cache; miss
/// fetched from row_stream). MUST be called with NO RefCell borrow held
/// (note 15) — callers snapshot Arc<SstReader>s first.
/// Errors are PROPAGATED, never folded into "miss" (coco P0): a paged
/// block read is a fallible network read — treating a transient failure
/// as "key absent" let GC punch extents whose VPs were still live, and
/// let GET return a false NotFound. `Ok(None)` strictly means "definitely
/// not in this SST".
pub(crate) async fn lookup_in_sst_via(
    reader: &SstReader,
    user_key: &[u8],
    sc: &Rc<StreamClient>,
    cache: &crate::sstable::BlockCache,
) -> Result<Option<(u8, Bytes, u64)>> {
    if !reader.bloom_may_contain(user_key) {
        return Ok(None);
    }
    let target = key_with_ts(user_key, u64::MAX);
    let block_idx = reader.find_block_for_key(&target);
    let block = reader.read_block_via(block_idx, sc, cache).await?;
    let n = block.num_entries();
    if n == 0 {
        return Ok(None);
    }
    let mut lo = 0usize;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let key = block
            .get_key(mid)
            .map_err(|e| anyhow!("sst block entry decode: {e}"))?;
        if key.as_slice() < target.as_slice() {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    // F250 next-block hop — see lookup_in_sst for the full rationale.
    let (blk, pos) = if lo < n {
        (block, lo)
    } else if block_idx + 1 < reader.block_count() {
        let nb = reader.read_block_via(block_idx + 1, sc, cache).await?;
        if nb.num_entries() == 0 {
            return Ok(None);
        }
        (nb, 0usize)
    } else {
        return Ok(None);
    };
    let (key, op, value, expires_at) = blk
        .get_entry(pos)
        .map_err(|e| anyhow!("sst block entry decode: {e}"))?;
    if parse_key(&key) == user_key {
        return Ok(Some((op, value, expires_at)));
    }
    Ok(None)
}

/// TEST-ONLY reference implementation of the SST point lookup (incl. the F250
/// next-block hop) — the production paths are its specialised siblings
/// `lookup_in_sst_seq_opt` / `lookup_in_sst_via`, which mirror this logic.
#[cfg(test)]
pub(crate) fn lookup_in_sst(reader: &SstReader, user_key: &[u8]) -> Option<(u8, Bytes, u64)> {
    if !reader.bloom_may_contain(user_key) {
        return None;
    }
    let target = key_with_ts(user_key, u64::MAX);
    let block_idx = reader.find_block_for_key(&target);
    let block = reader.read_block(block_idx).ok()?;
    let n = block.num_entries();
    if n == 0 {
        return None;
    }

    // Binary search: find first entry whose key >= target.
    let mut lo = 0usize;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let key = block.get_key(mid).ok()?;
        if key.as_slice() < target.as_slice() {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    // The first entry whose key >= target is the newest version for
    // user_key. It is at `lo` in THIS block when `lo < n`. But when
    // `lo == n` (every key in `block_idx` sorts < target), the answer is
    // the FIRST entry of the NEXT block: `find_block_for_key` returns the
    // last block whose base_key <= target, so `base_key[block_idx+1]` is
    // the smallest key > target. Without this next-block hop a user_key
    // whose newest entry happens to be the base (first entry) of a block
    // is missed entirely — the binary search runs off the end of the
    // preceding block and we return None, the read then falls through to
    // an older SST and returns a STALE value. This was the F250
    // fence+flush "data loss" — actually a point-lookup block-boundary
    // bug, invisible until SSTs grew past one block per table.
    let (blk, pos) = if lo < n {
        (block, lo)
    } else if block_idx + 1 < reader.block_count() {
        let nb = reader.read_block(block_idx + 1).ok()?;
        if nb.num_entries() == 0 {
            return None;
        }
        (nb, 0usize)
    } else {
        return None;
    };
    let (key, op, value, expires_at) = blk.get_entry(pos).ok()?;
    if parse_key(&key) == user_key {
        return Some((op, value, expires_at));
    }
    None
}

pub(crate) fn collect_mem_items(part: &PartitionData) -> Vec<IterItem> {
    let mut items = part.active.snapshot_sorted();
    for imm in part.imm.iter().rev() {
        items.extend(imm.snapshot_sorted());
    }
    items
}

/// F262: async window scan over the (paged) readers — SST side only, no
/// materialization. Caller snapshots `readers` + `sc` under a brief borrow,
/// DROPS it, then awaits (note 15). The split path holds `maintenance_gate`,
/// so the reader set is stable across the scan. Returns the newest
/// (first-occurrence, newest-first reader order) version per user key.
///
/// coco P2 (F262): the MEMTABLE sample is deliberately NOT taken here —
/// this scan can run for a while on large SST sets while normal writes
/// keep landing in active. The caller samples the memtable AFTER this
/// returns (`finalize_unique_user_keys`), so writes that arrived during
/// the scan still participate in split's `< 2 keys` check and midpoint
/// selection (pre-F262 parity: the old sync scan sampled mem after the
/// materialization await).
pub(crate) async fn sst_user_key_versions(
    readers: &[Arc<SstReader>],
    sc: &Rc<StreamClient>,
) -> Result<BTreeMap<Vec<u8>, (u8, u64)>> {
    let mut seen: BTreeMap<Vec<u8>, (u8, u64)> = BTreeMap::new();
    for reader in readers.iter().rev() {
        let mut it = AsyncTableIterator::new(
            reader.clone(),
            sc.clone(),
            FetchMode::Window(SCAN_READ_WINDOW_BYTES),
        );
        it.rewind().await?;
        while it.valid() {
            let item = it.item().unwrap();
            let uk = parse_key(&item.key).to_vec();
            seen.entry(uk).or_insert((item.op, item.expires_at));
            it.next().await?;
        }
    }
    Ok(seen)
}

/// Merge a FRESH memtable sample over the SST scan result (memtable holds
/// strictly newer versions, so it wins per key), then drop tombstones and
/// expired entries. Sync — call under/right after a brief borrow.
pub(crate) fn finalize_unique_user_keys(
    mem_items: &[IterItem],
    sst_seen: BTreeMap<Vec<u8>, (u8, u64)>,
) -> Vec<Vec<u8>> {
    let now = now_secs();
    let mut seen: BTreeMap<Vec<u8>, (u8, u64)> = BTreeMap::new();
    // mem_items are sorted by internal key (newest version of each user
    // key first) — or_insert keeps the newest, mirroring the old combined
    // scan's first-occurrence-wins discipline.
    for item in mem_items {
        let uk = parse_key(&item.key).to_vec();
        seen.entry(uk).or_insert((item.op, item.expires_at));
    }
    for (uk, v) in sst_seen {
        seen.entry(uk).or_insert(v);
    }
    seen.into_iter()
        .filter_map(|(uk, (op, expires_at))| {
            if op == 2 {
                return None;
            }
            if expires_at > 0 && expires_at <= now {
                return None;
            }
            Some(uk)
        })
        .collect()
}

/// Resolve a value, optionally reading only a sub-range.
/// `offset` and `length` are byte offsets within the value (0/0 = full read).
pub(crate) async fn resolve_value(
    op: u8,
    raw_value: Bytes,
    stream_client: &Rc<StreamClient>,
    offset: u64,
    length: u64,
) -> Result<Bytes> {
    if op & OP_VALUE_POINTER != 0 {
        if raw_value.len() < VALUE_POINTER_SIZE {
            return Err(anyhow!("ValuePointer too short"));
        }
        let vp = ValuePointer::decode(&raw_value[..VALUE_POINTER_SIZE]);
        read_value_from_log(&vp, stream_client, offset, length).await
    } else {
        // Inline value already in a memtable Bytes — slice it zero-copy.
        let n = raw_value.len();
        if offset == 0 && length == 0 {
            Ok(raw_value)
        } else {
            let start = (offset as usize).min(n);
            let end = if length == 0 {
                n
            } else {
                (start + length as usize).min(n)
            };
            Ok(raw_value.slice(start..end))
        }
    }
}

/// Read value bytes from logStream. VP.offset points to value start.
/// `offset`/`length` = 0/0 means read the entire value. Returns a `Bytes`:
/// R4 — on the UCX zero-copy fast path the value `Bytes` ALIASES the registered
/// RegPool buffer (`Bytes::from_owner(pb)`, no copy), so handle_get_zc can send
/// it as its own iovec (fully copy-free EN->PS->client); on the copy-path
/// fallback the Vec is moved into a Bytes.
pub(crate) async fn read_value_from_log(
    vp: &ValuePointer,
    stream_client: &Rc<StreamClient>,
    offset: u64,
    length: u64,
) -> Result<Bytes> {
    let (read_off, read_len) = if offset == 0 && length == 0 {
        (vp.offset, vp.len)
    } else {
        let off = offset.min(vp.len);
        let len = if length == 0 {
            vp.len - off
        } else {
            length.min(vp.len - off)
        };
        (vp.offset + off, len)
    };
    // Sub-range fully past the value end (offset >= vp.len) clamps to a
    // zero-length read — return empty EXPLICITLY. Pre-fix this fell
    // through to `read_value_into_pooled(.., 0)`, whose pooled fast path
    // handed back the recycled RegPool buffer's STALE contents as the
    // value (`Bytes::from_owner(pb)` with a dirty buffer) — fuse reads of
    // a shortened/sparse extent window returned VARYING GARBAGE instead
    // of zeros (caught by fuse_chaos T2: shrink→grow read non-zero,
    // non-source bytes that changed between reads).
    if read_len == 0 {
        return Ok(Bytes::new());
    }
    // F216-E R3/R4: zero-copy fast path — recv the value straight into a
    // registered RegPool buffer over UCX (MSG_READ_BYTES_ZC) and hand it onward
    // as a Bytes ALIASING that buffer (from_owner; pb returns to the pool when
    // the Bytes drops). No off-wire copy, no pb->Vec. Any non-OK / EC / chunked
    // / non-UCX case returns None -> the proven read_bytes_from_extent copy path.
    if let Ok(Some((pb, _n))) = stream_client
        .read_value_into_pooled(vp.extent_id, read_off, read_len)
        .await
    {
        return Ok(Bytes::from_owner(pb));
    }
    let (data, _) = stream_client
        .read_bytes_from_extent(vp.extent_id, read_off, read_len)
        .await?;
    if (data.len() as u64) < read_len {
        return Err(anyhow!(
            "logStream value short: need {} bytes, got {}, extent={}, offset={}",
            read_len,
            data.len(),
            vp.extent_id,
            read_off
        ));
    }
    Ok(Bytes::from(data))
}

// ---------------------------------------------------------------------------
// RPC dispatch (runs on partition thread)
// ---------------------------------------------------------------------------

// ===========================================================================
// Tests — R4 4.4 SQ/CQ pipeline semantics
// ===========================================================================
//
// These tests validate the *pattern* used by background_write_loop_r1 and
// flush_worker_loop: a FuturesUnordered-driven N-deep pipeline that drains
// concurrently with the submit side. They do NOT require a live StreamClient
// (which would need a full manager + extent nodes) — instead they drive
// futures whose completion timing and result are explicitly controlled.
//
// What each test proves:
//   1. ps_sqcq_handles_concurrent_in_flight — with cap=N, N slow futures
//      launched sequentially complete in max(latency), not sum(latencies).
//      This is the whole point of the refactor.
//   2. ps_sqcq_memtable_rotation_works_out_of_order — completions arrive in
//      a different order than launch; aggregating "memtable-insert effects"
//      in completion order yields the correct final state.
//   3. ps_sqcq_backpressure_at_cap — when inflight reaches cap, SQ blocks
//      until a CQ completion frees a slot; the observed in-flight count
//      never exceeds cap.
//   4. ps_sqcq_locked_by_other_drains_cleanly — on a simulated
//      LockedByOther error the loop drains its remaining inflight before
//      exiting (no leaked Phase-3 replies).
#[cfg(test)]
mod sqcq_tests {
    use super::*;
    use futures::channel::mpsc;
    use futures::future::{select, Either};
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Mini SQ/CQ driver that mirrors background_write_loop_r1's control
    /// flow but operates on (id, delay, result) triples. The caller injects
    /// work via `submit_tx`; each work item completes after `delay` and
    /// returns `(id, result)`. The loop stops when the channel closes.
    ///
    /// Returned: `(completion_order, max_seen_inflight)`.
    async fn run_harness(
        cap: usize,
        mut submit_rx: mpsc::Receiver<(u32, Duration, std::result::Result<u64, String>)>,
        collected: Rc<std::cell::RefCell<Vec<(u32, std::result::Result<u64, String>)>>>,
        max_inflight: Rc<Cell<usize>>,
        locked_by_other: Rc<Cell<bool>>,
    ) {
        type Fut = std::pin::Pin<
            Box<dyn std::future::Future<Output = (u32, std::result::Result<u64, String>)>>,
        >;
        let mut inflight: FuturesUnordered<Fut> = FuturesUnordered::new();

        let record_inflight = |n: usize| {
            if n > max_inflight.get() {
                max_inflight.set(n);
            }
        };

        loop {
            // (A) Opportunistic CQ drain.
            while let Some(Some(c)) = inflight.next().now_or_never() {
                if matches!(&c.1, Err(e) if e.contains("LockedByOther")) {
                    locked_by_other.set(true);
                }
                collected.borrow_mut().push(c);
                if locked_by_other.get() {
                    return;
                }
            }

            let n_inflight = inflight.len();
            let at_cap = n_inflight >= cap;

            if at_cap {
                if let Some(c) = inflight.next().await {
                    if matches!(&c.1, Err(e) if e.contains("LockedByOther")) {
                        locked_by_other.set(true);
                    }
                    collected.borrow_mut().push(c);
                    if locked_by_other.get() {
                        return;
                    }
                }
                continue;
            }

            if n_inflight == 0 {
                match submit_rx.next().await {
                    Some((id, delay, result)) => {
                        inflight.push(Box::pin(async move {
                            compio::time::sleep(delay).await;
                            (id, result)
                        }));
                        record_inflight(inflight.len());
                    }
                    None => break,
                }
                continue;
            }

            let sq_fut = submit_rx.next();
            let cq_fut = inflight.next();
            futures::pin_mut!(sq_fut);
            match select(sq_fut, Box::pin(cq_fut)).await {
                Either::Left((maybe, _)) => match maybe {
                    Some((id, delay, result)) => {
                        inflight.push(Box::pin(async move {
                            compio::time::sleep(delay).await;
                            (id, result)
                        }));
                        record_inflight(inflight.len());
                    }
                    None => {
                        while let Some(c) = inflight.next().await {
                            if matches!(&c.1, Err(e) if e.contains("LockedByOther")) {
                                locked_by_other.set(true);
                            }
                            collected.borrow_mut().push(c);
                        }
                        break;
                    }
                },
                Either::Right((maybe_c, _)) => {
                    if let Some(c) = maybe_c {
                        if matches!(&c.1, Err(e) if e.contains("LockedByOther")) {
                            locked_by_other.set(true);
                        }
                        collected.borrow_mut().push(c);
                        if locked_by_other.get() {
                            return;
                        }
                    }
                }
            }
        }

        // Drain remaining inflight after channel close (shutdown path).
        while let Some(c) = inflight.next().await {
            if matches!(&c.1, Err(e) if e.contains("LockedByOther")) {
                locked_by_other.set(true);
            }
            collected.borrow_mut().push(c);
        }
    }

    /// Test 1 — with cap=4 and 4 concurrent 100ms futures, the run
    /// completes in ~100ms, not ~400ms. This demonstrates the pipeline
    /// genuinely parallelises the Phase 2 RTT (the whole point of 4.4).
    #[test]
    fn ps_sqcq_handles_concurrent_in_flight() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut tx, rx) = mpsc::channel(16);
            let collected = Rc::new(std::cell::RefCell::new(Vec::new()));
            let max_inflight = Rc::new(Cell::new(0usize));
            let locked = Rc::new(Cell::new(false));

            let c = collected.clone();
            let m = max_inflight.clone();
            let l = locked.clone();
            let handle = compio::runtime::spawn(async move {
                run_harness(4, rx, c, m, l).await;
            });

            for i in 0..4u32 {
                tx.send((i, Duration::from_millis(100), Ok(i as u64)))
                    .await
                    .unwrap();
            }
            drop(tx);

            let start = Instant::now();
            let _ = handle.await;
            let elapsed = start.elapsed();

            assert_eq!(collected.borrow().len(), 4);
            assert!(
                elapsed < Duration::from_millis(300),
                "4 × 100ms futures should run concurrently; took {:?}",
                elapsed
            );
            assert!(
                max_inflight.get() >= 2,
                "expected multiple futures to be in flight concurrently; got max={}",
                max_inflight.get()
            );
        });
    }

    /// Test 2 — completions may arrive in a different order than launches.
    /// Submit 5 items with mixed latencies; verify every completion is
    /// recorded exactly once even though the order differs from submit
    /// order. The "memtable rotation" analogue is: applying inserts in
    /// completion order is safe because each has a distinct seq number
    /// (here, id) and the final aggregate set is order-independent.
    #[test]
    fn ps_sqcq_memtable_rotation_works_out_of_order() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut tx, rx) = mpsc::channel(16);
            let collected = Rc::new(std::cell::RefCell::new(Vec::new()));
            let max_inflight = Rc::new(Cell::new(0usize));
            let locked = Rc::new(Cell::new(false));

            let c = collected.clone();
            let m = max_inflight.clone();
            let l = locked.clone();
            let handle = compio::runtime::spawn(async move {
                run_harness(8, rx, c, m, l).await;
            });

            // Submit with descending delays so completions arrive in reverse
            // order of submission.
            let delays = [250u64, 200, 150, 100, 50];
            for (i, d) in delays.iter().enumerate() {
                tx.send((i as u32, Duration::from_millis(*d), Ok(i as u64)))
                    .await
                    .unwrap();
            }
            drop(tx);

            let _ = handle.await;

            let got = collected.borrow();
            assert_eq!(got.len(), 5);

            // Completion order should be the reverse of launch order.
            let order: Vec<u32> = got.iter().map(|(id, _)| *id).collect();
            assert_eq!(
                order,
                vec![4, 3, 2, 1, 0],
                "CQ order should reflect latency, not launch order"
            );

            // Regardless of order, the aggregate set of "id × result" equals
            // everything we submitted (memtable-insert analogue: final set
            // contains all entries even though inserted out of order).
            let mut ids: Vec<u32> = got.iter().map(|(id, _)| *id).collect();
            ids.sort();
            assert_eq!(ids, vec![0, 1, 2, 3, 4]);
        });
    }

    /// Test 3 — cap=2, submit 10 items. Observed in-flight count must
    /// never exceed 2 during the test. This validates back-pressure.
    #[test]
    fn ps_sqcq_backpressure_at_cap() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut tx, rx) = mpsc::channel(32);
            let collected = Rc::new(std::cell::RefCell::new(Vec::new()));
            let max_inflight = Rc::new(Cell::new(0usize));
            let locked = Rc::new(Cell::new(false));

            let c = collected.clone();
            let m = max_inflight.clone();
            let l = locked.clone();
            let handle = compio::runtime::spawn(async move {
                run_harness(2, rx, c, m, l).await;
            });

            for i in 0..10u32 {
                tx.send((i, Duration::from_millis(30), Ok(i as u64)))
                    .await
                    .unwrap();
            }
            drop(tx);

            let _ = handle.await;

            assert_eq!(collected.borrow().len(), 10);
            assert!(
                max_inflight.get() <= 2,
                "inflight count exceeded cap=2: {}",
                max_inflight.get()
            );
            assert!(
                max_inflight.get() >= 2,
                "expected cap to be saturated at least once; got max={}",
                max_inflight.get()
            );
        });
    }

    /// Test 4 — inject a LockedByOther on item 2 (of 4 submitted). The
    /// loop must surface the flag and return early after the LBO
    /// completion is processed, but any items already in flight when the
    /// LBO arrives must still complete and be recorded (no leaked Phase-3
    /// replies — clean drain semantics).
    #[test]
    fn ps_sqcq_locked_by_other_drains_cleanly() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut tx, rx) = mpsc::channel(16);
            let collected = Rc::new(std::cell::RefCell::new(Vec::new()));
            let max_inflight = Rc::new(Cell::new(0usize));
            let locked = Rc::new(Cell::new(false));

            let c = collected.clone();
            let m = max_inflight.clone();
            let l = locked.clone();
            let handle = compio::runtime::spawn(async move {
                run_harness(4, rx, c, m, l).await;
            });

            // Item 2 returns LockedByOther. Items 0, 1, 3 are Ok.
            tx.send((0, Duration::from_millis(50), Ok(0)))
                .await
                .unwrap();
            tx.send((1, Duration::from_millis(50), Ok(1)))
                .await
                .unwrap();
            tx.send((2, Duration::from_millis(20), Err("LockedByOther".into())))
                .await
                .unwrap();
            tx.send((3, Duration::from_millis(50), Ok(3)))
                .await
                .unwrap();
            // Channel deliberately left open; loop exits on locked_by_other flag.

            let _ = handle.await;

            // After the flag is set, the loop returns. We can't assert an
            // exact count but we MUST have observed the LBO entry and the
            // loop must NOT have silently dropped any already-completed
            // entries. Check the invariant: all collected entries have the
            // right (id, result) pairing, and LBO is present, and the flag
            // is set.
            assert!(locked.get(), "locked_by_other flag must be set");
            let got = collected.borrow();
            assert!(
                got.iter().any(|(id, r)| *id == 2 && r.is_err()),
                "LBO item must be in collected; got {:?}",
                got.iter().map(|(id, _)| *id).collect::<Vec<_>>()
            );
            // No entry is duplicated or corrupted.
            let mut seen_ids: Vec<u32> = got.iter().map(|(id, _)| *id).collect();
            let before = seen_ids.len();
            seen_ids.sort();
            seen_ids.dedup();
            assert_eq!(before, seen_ids.len(), "duplicate ids in completions");
        });
    }

    /// Env + constant sanity checks — cheap, detects accidental regressions.
    #[test]
    fn ps_inflight_cap_default_and_bounds() {
        // Default (no env override) = 8. The env is read once via OnceLock;
        // we only assert default or that the cached value is valid.
        let v = crate::ps_inflight_cap();
        assert!((1..=64).contains(&v), "ps_inflight_cap out of range: {}", v);
    }

    #[test]
    fn ps_bulk_inflight_cap_default_and_bounds() {
        let v = crate::ps_bulk_inflight_cap();
        assert!(
            (1..=16).contains(&v),
            "ps_bulk_inflight_cap out of range: {}",
            v
        );
    }

    /// Silence unused-warning for AtomicUsize import if other tests change.
    #[allow(dead_code)]
    fn _touch_atomic_usize() {
        let _ = AtomicUsize::new(0).fetch_add(1, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// F106: streaming run_gc record-boundary carry tests
// ---------------------------------------------------------------------------
//
// These tests verify the boundary contract that run_gc/process_gc_chunk
// rely on: when a chunk ends in the middle of a record, the decoder
// must stop at the start of that incomplete record (so the caller can
// carry it into the next chunk). They exercise the same record header
// arithmetic as process_gc_chunk's inner loop.

#[cfg(test)]
mod gc_streaming_tests {
    use crate::{decode_records_full, encode_record};

    fn rec(op: u8, key: &[u8], value: &[u8]) -> Vec<u8> {
        encode_record(op, key, value, 0)
    }

    /// Full buffer: every record is decoded.
    #[test]
    fn decode_full_buf_yields_all_records() {
        let r1 = rec(0x80, b"k1", b"v1-payload");
        let r2 = rec(0x80, b"k2", b"v2-other");
        let r3 = rec(0x02, b"k3", b""); // tombstone, no VP
        let mut buf = Vec::new();
        buf.extend_from_slice(&r1);
        buf.extend_from_slice(&r2);
        buf.extend_from_slice(&r3);
        let recs = decode_records_full(&buf);
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[0].1, b"k1");
        assert_eq!(recs[1].1, b"k2");
        assert_eq!(recs[2].1, b"k3");
    }

    /// Truncate the buffer 5 bytes into the 3rd record's payload —
    /// decode must yield only the first 2 complete records and stop at
    /// the start of record 3 (so a streaming caller can carry r3 into
    /// the next chunk).
    #[test]
    fn decode_stops_at_partial_record_boundary() {
        let r1 = rec(0x80, b"key-1", b"value-1");
        let r2 = rec(0x80, b"key-2", b"value-2");
        let r3 = rec(0x80, b"key-3", b"value-3-longer");
        let r1_r2_len = r1.len() + r2.len();

        let mut buf = Vec::new();
        buf.extend_from_slice(&r1);
        buf.extend_from_slice(&r2);
        buf.extend_from_slice(&r3);
        // truncate 5 bytes into r3 — header is intact, payload incomplete
        let truncated = &buf[..r1_r2_len + 22]; // 17B header + a few key bytes

        let recs = decode_records_full(truncated);
        assert_eq!(
            recs.len(),
            2,
            "incomplete record at tail must not be returned; consumed prefix is r1+r2"
        );
        assert_eq!(recs[0].1, b"key-1");
        assert_eq!(recs[1].1, b"key-2");
    }

    /// Truncate INSIDE the 17-byte header — same contract: yield only
    /// the prior complete records.
    #[test]
    fn decode_stops_when_header_itself_is_partial() {
        let r1 = rec(0x80, b"k", b"v");
        let r2 = rec(0x80, b"k2", b"v2");
        let mut buf = Vec::new();
        buf.extend_from_slice(&r1);
        buf.extend_from_slice(&r2);
        // Truncate so r2's header is incomplete (only 8 of 17 header bytes present).
        let truncated = &buf[..r1.len() + 8];
        let recs = decode_records_full(truncated);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].1, b"k");
    }

    /// Record-by-record concatenation: feeding the buffer in two halves
    /// (with the split mid-record) plus carry-forward must reconstruct
    /// the same record set as a single full decode. This is the exact
    /// pattern process_gc_chunk uses.
    #[test]
    fn carry_forward_round_trips_to_full_decode() {
        let recs_in: Vec<Vec<u8>> = (0..7)
            .map(|i| {
                let key = format!("user-key-{:02}", i);
                let val: Vec<u8> = (0..(13 + i * 7) as usize).map(|x| x as u8).collect();
                rec(0x80, key.as_bytes(), &val)
            })
            .collect();
        let mut full: Vec<u8> = Vec::new();
        for r in &recs_in {
            full.extend_from_slice(r);
        }
        let expected = decode_records_full(&full);

        // Split the full buffer at every byte position; feed half-1,
        // carry tail, append half-2, decode again. The combined output
        // must match `expected` for every split point.
        for split in 1..full.len() {
            let left = &full[..split];
            let right = &full[split..];

            let recs_left = decode_records_full(left);
            // Determine consumed prefix length by re-encoding what we just decoded.
            // F158: post-V1 the envelope adds 9 bytes per record (sentinel + length + crc).
            let consumed: usize = recs_left
                .iter()
                .map(|(_op, k, v, _)| {
                    crate::wal_record::V1_ENVELOPE_OVERHEAD
                        + crate::wal_record::PAYLOAD_HEADER
                        + k.len()
                        + v.len()
                })
                .sum();
            let mut carry = left[consumed..].to_vec();
            carry.extend_from_slice(right);
            let recs_after_carry = decode_records_full(&carry);

            let mut combined = recs_left.clone();
            combined.extend(recs_after_carry);
            assert_eq!(
                combined.len(),
                expected.len(),
                "split={split}: combined record count mismatch"
            );
            for (got, want) in combined.iter().zip(expected.iter()) {
                assert_eq!(got.0, want.0, "split={split}: op mismatch");
                assert_eq!(got.1, want.1, "split={split}: key mismatch");
                assert_eq!(got.2, want.2, "split={split}: value mismatch");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// F201 — GC failure-cooldown classification tests
// ---------------------------------------------------------------------------
//
// Pure-fn coverage for `classify_gc_failure_cooldown`. The full GC loop
// is harder to unit-test (requires PartitionData + StreamClient
// fixtures); this covers the classifier itself end-to-end via anyhow
// chains representative of the production error shapes.

#[cfg(test)]
mod f201_classify_cooldown_tests {
    use super::classify_gc_failure_cooldown;
    use anyhow::anyhow;
    use std::time::Duration;

    const SOFT: Duration = Duration::from_secs(30);
    const HARD: Duration = Duration::from_secs(300);

    /// Manager rejects punch_holes with CODE_PRECONDITION while a
    /// concurrent EC conversion holds `ec_conversion_inflight`. The
    /// stream-client surfaces this as `anyhow!("punch_holes failed:
    /// precondition failed: ...")`. Soft cooldown so we retry once
    /// EC completes (typically within seconds).
    #[test]
    fn precondition_in_chain_uses_soft_cooldown() {
        let inner = anyhow!("precondition failed: ec_conversion_inflight contains 42");
        let outer = inner.context("punch_holes failed");
        assert_eq!(classify_gc_failure_cooldown(&outer, SOFT, HARD), SOFT);
    }

    /// Stream client returns `eversion mismatch (stale extent_info_cache)`
    /// after the 2-attempt retry loop exhausts and the cache is still
    /// stale — typically a momentary EC dispatch finishing mid-GC read.
    /// Soft cooldown.
    #[test]
    fn eversion_mismatch_uses_soft_cooldown() {
        let inner = anyhow!("eversion mismatch (stale extent_info_cache)");
        let wrapped = inner.context("run_gc extent 42 process_gc_chunk read failed");
        assert_eq!(classify_gc_failure_cooldown(&wrapped, SOFT, HARD), SOFT);
    }

    /// Network timeout / disk failure / decode error — anything not on
    /// the sentinel substring list gets the hard 300 s cooldown so we
    /// don't burn IO retrying every 5-7 s against a broken EC layout.
    #[test]
    fn unrecognised_failure_uses_hard_cooldown() {
        let err = anyhow!("connection closed mid-read");
        assert_eq!(classify_gc_failure_cooldown(&err, SOFT, HARD), HARD);
    }

    /// Empty top-level message but recognisable substring deeper in
    /// the chain — still soft. Defends the classifier against future
    /// callsite refactors that wrap the original error in additional
    /// context layers.
    #[test]
    fn sentinel_deep_in_chain_still_recognised() {
        let bottom = anyhow!("precondition failed: locked by ec");
        let middle = bottom.context("manager rejected request");
        let top = middle.context("punch_holes failed");
        assert_eq!(classify_gc_failure_cooldown(&top, SOFT, HARD), SOFT);
    }
}

#[cfg(test)]
mod lookup_block_boundary_tests {
    use super::*;
    use crate::key_with_ts;
    use crate::sstable::builder::SstBuilder;
    use crate::sstable::reader::SstReader;

    /// Regression for the F250 fence+flush "data loss" — actually a
    /// point-lookup block-boundary bug. When a user_key's newest entry is
    /// the FIRST entry (base_key) of an SST block, `find_block_for_key`
    /// (last block with base <= target) selected the PRECEDING block (whose
    /// base <= target = `user_key++0x00++BE(0)`), and the single-block
    /// binary search ran off the end → `lookup_in_sst` returned None →
    /// the read fell through to an older SST and returned a STALE value.
    /// `lookup_in_sst` must hop to the next block's first entry in that
    /// case. Invisible until SSTs grew past one block per table (>64 KiB),
    /// which is why the 64-key reproducer passed but the 1024-key one failed.
    #[test]
    fn lookup_in_sst_finds_keys_at_block_boundaries() {
        // ~2 KiB values × 300 keys ≈ 600 KiB → many 64 KiB blocks, so
        // several keys necessarily become block base_keys.
        let val = vec![b'x'; 2048];
        let n: u32 = 300;
        let mut b = SstBuilder::new(0, 0);
        for i in 0..n {
            let uk = format!("k{i:06}").into_bytes();
            // distinct seq per key so the stored ts is checkable
            let seq = (i as u64) + 1;
            b.add(&key_with_ts(&uk, seq), 1, &val, 0);
        }
        let reader = SstReader::from_bytes(bytes::Bytes::from(b.finish())).expect("reader");

        // The bug only exists with >1 block; assert we actually exercise it.
        assert!(
            reader.block_count() > 1,
            "test must span multiple blocks (got {})",
            reader.block_count()
        );

        // EVERY key — including the ones sitting at block boundaries — must
        // be found via the bloom-gated point lookup the GET path uses.
        for i in 0..n {
            let uk = format!("k{i:06}").into_bytes();
            let got = lookup_in_sst(&reader, &uk);
            assert!(
                got.is_some(),
                "lookup_in_sst missed k{i:06} (block-boundary regression)"
            );
            let (op, value, _) = got.unwrap();
            assert_eq!(op, 1);
            assert_eq!(value.len(), val.len(), "wrong value for k{i:06}");
        }

        // A key that does not exist must still return None.
        assert!(lookup_in_sst(&reader, b"zzzzzz").is_none());
    }
}

#[cfg(test)]
mod wal_debt_tests {
    //! F-DF-WALDEBT: the open-tail dead-byte extraction must read exactly the
    //! LAST log extent's discard and stay DISJOINT from `gc_debt` (which
    //! `valid_discard` restricts to the sealed prefix). If these ever
    //! double-count or the open tail leaks into gc_debt, df's debt figure is
    //! wrong.
    use super::{open_tail_dead_bytes, valid_discard};
    use std::collections::HashMap;

    #[test]
    fn open_tail_dead_is_last_extent_discard() {
        // extents [10, 20, 30]; 30 is the open tail.
        let discards = HashMap::from([(10u64, 100i64), (20, 200), (30, 500)]);
        let extent_ids = [10u64, 20, 30];
        assert_eq!(open_tail_dead_bytes(&discards, &extent_ids), 500);
    }

    #[test]
    fn open_tail_dead_and_gc_debt_are_disjoint_no_double_count() {
        let all = HashMap::from([(10u64, 100i64), (20, 200), (30, 500)]);
        let extent_ids = [10u64, 20, 30];
        let open = open_tail_dead_bytes(&all, &extent_ids);
        // gc_debt = sealed prefix only (drops the open tail 30).
        let mut sealed = all.clone();
        valid_discard(&mut sealed, &extent_ids[..extent_ids.len() - 1]);
        let gc_debt: u64 = sealed.values().map(|v| (*v).max(0) as u64).sum();
        assert_eq!(open, 500);
        assert_eq!(gc_debt, 300); // 100 + 200, NOT the tail's 500
        assert_eq!(open + gc_debt, 800); // full WAL debt, each byte once
    }

    #[test]
    fn open_tail_dead_zero_when_tail_all_live_or_absent() {
        // Tail 30 has no discard entry (all live) → 0.
        let discards = HashMap::from([(10u64, 100i64)]);
        assert_eq!(open_tail_dead_bytes(&discards, &[10u64, 20, 30]), 0);
        // No extents at all → 0.
        assert_eq!(open_tail_dead_bytes(&discards, &[]), 0);
        // Negative (over-counted) discard clamps to 0, never underflows u64.
        let neg = HashMap::from([(30u64, -50i64)]);
        assert_eq!(open_tail_dead_bytes(&neg, &[10u64, 30]), 0);
    }
}

#[cfg(test)]
mod gc_replay_floor_tests {
    //! F1 regression: GC must never punch a log_stream extent that crash
    //! recovery still replays. These pin the floor computation (== recovery's
    //! `chosen_pos`) and the punch guard.
    use super::{gc_extent_punchable, gc_floor_raise_to_durable_ckpt, gc_replay_floor};

    #[test]
    fn floor_is_min_first_occurrence_position() {
        let log = [10u64, 11, 12, 13];
        let (floor, pos) = gc_replay_floor(&log, [11u64]);
        assert_eq!(floor, 1);
        assert_eq!(pos.get(&10), Some(&0));
        assert_eq!(pos.get(&13), Some(&3));
    }

    #[test]
    fn floor_takes_min_over_multiple_vp_heads() {
        let log = [10u64, 11, 12, 13];
        // earliest position among {13, 11, 12} is 11's (pos 1).
        assert_eq!(gc_replay_floor(&log, [13u64, 11, 12]).0, 1);
    }

    #[test]
    fn floor_uses_first_occurrence_for_cow_shared_extent() {
        // coco-P1 regression: post split/merge the spliced log_stream repeats a
        // CoW-shared extent (11). recover_partition replays from the FIRST
        // occurrence (pos 1). A last-occurrence floor would be 3, wrongly
        // freeing extent 12 (pos 2) that recovery still replays.
        let log = [10u64, 11, 12, 11, 13];
        assert_eq!(
            gc_replay_floor(&log, [11u64]).0,
            1,
            "must be first occurrence of 11, not 3"
        );
    }

    #[test]
    fn floor_zero_protects_all_when_nothing_resolves() {
        let log = [10u64, 11, 12];
        // No SSTs at all (nothing flushed) → recovery replays from 0.
        assert_eq!(gc_replay_floor(&log, std::iter::empty()).0, 0);
        // vp_head points at an extent already gone from the stream.
        assert_eq!(gc_replay_floor(&log, [99u64]).0, 0);
    }

    #[test]
    fn floor_skips_unresolvable_vp_heads_like_recovery() {
        let log = [10u64, 11, 12, 13];
        // 99 is gone; 12 resolves at pos 2 → min over resolvable = 2.
        assert_eq!(gc_replay_floor(&log, [99u64, 12]).0, 2);
    }

    #[test]
    fn guard_punches_only_below_floor_protects_replay_window() {
        // The F1 scenario: floor sits at the vp_head extent 11 (pos 1). Without
        // the guard GC would punch 11 (the vp_head extent) and 12 (after it) —
        // both have high discard — and a crash before the next checkpoint would
        // then lose the WAL recovery replays from 11 forward. With the guard
        // only the fully-flushed extent 10 and the empty extent 13 are punched.
        let log = [10u64, 11, 12, 13];
        let (floor, pos) = gc_replay_floor(&log, [11u64]);
        assert_eq!(floor, 1);
        let candidates = [
            (10u64, 1_000u64), // before floor, non-empty → punchable
            (11, 1_000),       // AT floor (vp_head extent) → PROTECTED
            (12, 1_000),       // after floor → PROTECTED
            (13, 0),           // empty → punchable regardless
        ];
        let punchable: Vec<u64> = candidates
            .iter()
            .filter(|(eid, sl)| gc_extent_punchable(*eid, *sl, &pos, floor))
            .map(|(eid, _)| *eid)
            .collect();
        assert_eq!(punchable, vec![10, 13]);
    }

    // F-RECOVERY-UNBOUNDED BUG2 — the durable-checkpoint floor raise.

    #[test]
    fn durable_ckpt_raise_advances_floor_and_frees_covered_prefix() {
        // Live SST vp_heads span extents 11 (pos 1) .. 14 (pos 4); the
        // over-conservative MIN floor is 1. The newest DURABLY-ACKed flush
        // checkpoint's vp is at extent 13 (pos 3): every record below pos 3 is
        // in that checkpoint's persisted SST set → safe to reclaim. The raise
        // lifts the floor to 3, so 11 and 12 become punchable while 13 (the vp
        // extent, AT the floor) and 14 stay protected.
        let log = [10u64, 11, 12, 13, 14];
        let (min_floor, pos) = gc_replay_floor(&log, [11u64, 12, 13, 14]);
        assert_eq!(min_floor, 1, "MIN over SST vp_heads");
        let raised = gc_floor_raise_to_durable_ckpt(min_floor, &pos, /*durable vp eid*/ 13);
        assert_eq!(raised, 3);
        let candidates = [
            (10u64, 1_000u64), // before raised floor → punchable
            (11, 1_000),       // now below floor 3 → punchable (was protected at MIN)
            (12, 1_000),       // now below floor 3 → punchable
            (13, 1_000),       // AT floor (durable vp extent) → PROTECTED
            (14, 1_000),       // after floor → PROTECTED
        ];
        let punchable: Vec<u64> = candidates
            .iter()
            .filter(|(eid, sl)| gc_extent_punchable(*eid, *sl, &pos, raised))
            .map(|(eid, _)| *eid)
            .collect();
        assert_eq!(punchable, vec![10, 11, 12]);
    }

    #[test]
    fn durable_ckpt_raise_is_noop_before_first_flush_or_when_unresolvable() {
        let log = [10u64, 11, 12, 13];
        let (min_floor, pos) = gc_replay_floor(&log, [11u64]);
        assert_eq!(min_floor, 1);
        // (0,0) = no flush committed this incarnation → stay at the MIN floor.
        assert_eq!(gc_floor_raise_to_durable_ckpt(min_floor, &pos, 0), 1);
        // Durable vp extent already gone from the stream (e.g. post-merge GC) →
        // conservative: keep the MIN floor, never raise past what resolves.
        assert_eq!(gc_floor_raise_to_durable_ckpt(min_floor, &pos, 99), 1);
    }

    #[test]
    fn durable_ckpt_raise_never_lowers_the_min_floor() {
        // A durable vp that resolves EARLIER than the MIN floor (a stale/older
        // checkpoint vp) must never pull the floor backward — the raise is a
        // max, so the more-conservative MIN wins.
        let log = [10u64, 11, 12, 13];
        let (min_floor, pos) = gc_replay_floor(&log, [12u64]); // MIN floor = 2
        assert_eq!(min_floor, 2);
        // durable vp resolves at pos 1 (< 2) → floor stays 2.
        assert_eq!(gc_floor_raise_to_durable_ckpt(min_floor, &pos, 11), 2);
    }
}

#[cfg(test)]
mod compaction_vp_head_tests {
    //! Regression: a compaction's output vp_head (recovery replay-start) is the
    //! MAX over the INPUT SSTs' vp_heads by stream position — the newest input's
    //! content boundary. It advances the GC floor while staying BEHIND the
    //! acked-but-un-flushed tail (which the pre-fix live-cursor stamp overran →
    //! `system_compact_unflushed_vp_head`).
    use super::compaction_output_vp_head;

    #[test]
    fn takes_max_first_occurrence_position() {
        let log = [10u64, 11, 12, 13];
        // positions: 10→0, 11→1. max over {(10,500),(11,400)} is 11 (pos 1).
        assert_eq!(
            compaction_output_vp_head([(10u64, 500u64), (11, 400)], &log),
            (11, 400)
        );
    }

    #[test]
    fn tie_breaks_by_larger_offset() {
        let log = [10u64, 11];
        // same extent 10 (pos 0): the larger offset is the newer boundary.
        assert_eq!(
            compaction_output_vp_head([(10u64, 100u64), (10, 900), (10, 400)], &log),
            (10, 900)
        );
    }

    #[test]
    fn skips_zero_and_unresolvable_vp_heads() {
        let log = [10u64, 11, 12];
        // eid 0 = no vp_head; eid 99 = gone from the stream. Only 11 resolves.
        assert_eq!(
            compaction_output_vp_head([(0u64, 5u64), (99, 5), (11, 700)], &log),
            (11, 700)
        );
    }

    #[test]
    fn falls_back_to_first_extent_when_none_resolve() {
        let log = [10u64, 11, 12];
        // No input resolves → replay from the first log extent, offset 0
        // (conservative full replay — NEVER (0,0), the recovery no-replay branch).
        assert_eq!(
            compaction_output_vp_head([(0u64, 5u64), (99, 5)], &log),
            (10, 0)
        );
        assert_eq!(compaction_output_vp_head(std::iter::empty(), &log), (10, 0));
        // Truly empty log (no data): (0,0) is fine — nothing to replay.
        assert_eq!(compaction_output_vp_head([(11u64, 5u64)], &[]), (0, 0));
    }

    #[test]
    fn cow_shared_extent_uses_first_occurrence_position() {
        // Post split/merge the spliced log_stream repeats a CoW extent (11).
        // Position keys off the FIRST occurrence (pos 1), matching recovery.
        let log = [10u64, 11, 12, 11, 13];
        // inputs at extent 11 (first-occ pos 1) and extent 12 (pos 2): max = 12.
        assert_eq!(
            compaction_output_vp_head([(11u64, 400u64), (12, 100)], &log),
            (12, 100)
        );
    }
}

/// BUG-MGR-RETRY-CLASS: the F270 fence classifier must see the WHOLE anyhow
/// chain — a `.context(...)` wrap added along the append path (e.g. stream
/// client.rs "alloc_new_extent failed after append error: …") hides the
/// "LockedByOther" marker from plain `{e}` Display, which only prints the
/// outermost context. Pre-fix that silently downgraded a fence to a generic
/// write error: no poison, no fresh-epoch reopen.
#[cfg(test)]
mod fence_classifier_tests {
    use super::is_locked_by_other;

    #[test]
    fn locked_by_other_survives_context_wrap() {
        // EN-layer shape (commit_length probe, F270).
        let en = anyhow::anyhow!(
            "commit_length on extent 42: only 2/3 replicas responded \
             (1 rejected LockedByOther — stale owner epoch)"
        );
        assert!(is_locked_by_other(&en));

        // Manager-layer shape (typed ManagerError Display) wrapped by the
        // append path's context — the regression this test pins.
        let mgr = anyhow::anyhow!(
            "stream_alloc_extent fenced (LockedByOther): precondition failed: \
             owner_key=partition/17 owner_epoch mismatch, expected 14396, got 14364"
        )
        .context("alloc_new_extent failed after append error: replica timeout");
        assert!(
            format!("{mgr}") == "alloc_new_extent failed after append error: replica timeout",
            "precondition of this test: plain Display hides the marker"
        );
        assert!(
            is_locked_by_other(&mgr),
            "chain-wide match must survive the context wrap"
        );

        // Generic errors must not classify.
        let plain = anyhow::anyhow!("replica 1 rpc error: connection reset");
        assert!(!is_locked_by_other(&plain));
    }
}
