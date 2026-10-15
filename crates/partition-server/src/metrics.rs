//! Per-partition counters and gauges, and the PS-wide Prometheus text
//! `/metrics` serves from them (`PsMetrics`).

use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use autumn_common::metrics_http::{push_metric, push_type};
use autumn_rpc::manager_rpc;

use crate::plog_load::PlogLoad;
use crate::sstable::BlockCache;

/// What asked for a minor compaction; its log line and metric label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MinorCause {
    Flush,
    Tick,
    /// Too many mergeable tables: the smallest window, ratio ignored.
    Blocking,
    Reclaim,
}

impl MinorCause {
    pub const ALL: [MinorCause; 4] = [Self::Flush, Self::Tick, Self::Blocking, Self::Reclaim];

    pub fn name(self) -> &'static str {
        match self {
            Self::Flush => "flush",
            Self::Tick => "tick",
            Self::Blocking => "blocking",
            Self::Reclaim => "reclaim",
        }
    }
}

/// LAT-1: fixed-bucket latency histogram (Prometheus-shape). Recording
/// is one bucket fetch_add + sum/count adds; PUT observations reuse the
/// ALREADY-MEASURED per-batch end-to-end ns from `WriteLoopMetrics`
/// (zero new hot-path timing), GET adds one `Instant` pair per request
/// (same cost class as the `req_count` add).
#[derive(Default)]
pub struct LatHist {
    pub buckets: [std::sync::atomic::AtomicU64; LAT_BUCKET_BOUNDS_NS.len()],
    pub sum_ns: std::sync::atomic::AtomicU64,
    pub count: std::sync::atomic::AtomicU64,
}

/// Upper bounds (ns) of the finite buckets. Stored per-bucket counts are
/// non-cumulative for cheap recording; render makes them cumulative, with
/// `+Inf` = count. Bounds: 0.5/1/2/5/10/20/50/100/250 ms.
pub const LAT_BUCKET_BOUNDS_NS: [u64; 9] = [
    500_000,
    1_000_000,
    2_000_000,
    5_000_000,
    10_000_000,
    20_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
];


impl LatHist {
    /// Record `n` observations of `ns` each (a group-committed batch's
    /// ops all share the batch's end-to-end latency).
    pub fn observe_n(&self, ns: u64, n: u64) {
        if n == 0 {
            return;
        }
        let idx = LAT_BUCKET_BOUNDS_NS.partition_point(|&b| ns > b);
        if idx < self.buckets.len() {
            self.buckets[idx].fetch_add(n, Relaxed);
        }
        // idx == len → +Inf bucket, implicit via count.
        self.sum_ns.fetch_add(ns.saturating_mul(n), Relaxed);
        self.count.fetch_add(n, Relaxed);
    }
}

#[derive(Default)]
pub struct PartitionMetrics {
    /// Set when this partition's handle goes (close, move, reopen); its
    /// gauges stop counting toward the PS's, its counters keep counting.
    pub closed: std::sync::atomic::AtomicBool,
    /// LAT-1: PUT (group-commit end-to-end) / GET (inline serve) latency.
    pub write_lat: LatHist,
    pub get_lat: LatHist,
    pub req_count: std::sync::atomic::AtomicU64,
    /// fix MED-3: monotonic request counter (NEVER swap-reset).
    /// `report_load_loop` swaps `req_count` to 0 every 5 s for its
    /// per-window rate calc, which races the maintenance
    /// scheduler's diff-based req_per_sec gate (the gate would see
    /// req_per_sec=0 right after a swap and dispatch BG work during a
    /// real FG storm). The scheduler now diffs against this monotonic
    /// counter instead, so it sees true delta over its own interval.
    pub req_count_monotonic: std::sync::atomic::AtomicU64,
    /// Value bytes written (Put values, accumulated per batch in
    /// `start_write_batch`). Monotonic: `report_load_loop` diffs it for
    /// `write_bytes_per_sec`, `/metrics` exports it as a counter.
    pub write_bytes: std::sync::atomic::AtomicU64,
    /// Value bytes READ (GET-served value lengths). Monotonic, like
    /// `write_bytes`.
    pub read_bytes: std::sync::atomic::AtomicU64,
    pub imm_full_count: std::sync::atomic::AtomicU64,
    /// LSM-resident bytes: Σ SST `tables[].len` + active memtable + Σ imm
    /// memtables (`PartitionData::lsm_resident_bytes`). Refreshed on the
    /// maintenance loop's throttled tick (2026-07-05 — was previously DEAD
    /// with no writer, so this gauge + the manager's
    /// size-based split/merge policy read a constant 0). This is the LOGICAL
    /// data size, NOT the total extent footprint — the cluster-overview
    /// size uses `open_tail_bytes` + the manager's sealed sum instead.
    /// Feeds the PS-wide `autumn_ps_size_bytes`
    /// Prometheus gauge and `policy.rs` split_size_*/merge_size_low.
    pub size_bytes: std::sync::atomic::AtomicU64,
    /// Σ committed bytes on this partition's three
    /// stream OPEN-tail extents (log + row + meta). Refreshed by the
    /// maintenance loop's throttled, non-blocking size probe (a detached
    /// `commit_length` on each of the 3 tails; kept at the prior value on
    /// any probe error) and shipped via `report_load_loop`. The manager
    /// adds it to its authoritative sealed-length sum for the cluster
    /// overview so an all-open-tail partition doesn't render 0 B. 0 until
    /// the first refresh completes.
    pub open_tail_bytes: std::sync::atomic::AtomicU64,
    /// guards against piling up concurrent open-tail
    /// size probes (each probe is a detached task; CAS false→true to launch,
    /// reset to false on completion).
    pub open_tail_probe_inflight: std::sync::atomic::AtomicBool,
    /// aggregated discard bytes on sealed log_stream extents that
    /// the GC loop would target. Refreshed by the GC loop's read-only
    /// prefix (no extra RPCs) and shipped via report_load_loop. Manager
    /// emits a `POLICY_KIND_GC` advisory when this stays above
    /// `gc_debt_high` for `policy.required_buckets` consecutive ticks
    /// outside the gc cooldown.
    pub gc_debt_bytes: std::sync::atomic::AtomicU64,
    /// dead (overwritten/deleted) large-value bytes on the OPEN
    /// log_stream tail extent — the discard-map entry `gc_debt_bytes` EXCLUDES
    /// because `valid_discard` filters to sealed extents (GC can't punch an
    /// unsealed tail). Refreshed on the SAME GC tick as `gc_debt_bytes`, from
    /// the already-persisted SST discard maps (no extra RPC, no write-path
    /// cost). Survives restart because it rides the meta_stream checkpoint,
    /// exactly like `gc_debt_bytes` — it is DERIVED each tick, never a bespoke
    /// counter to persist. Dead bytes at/after the replay floor, open tail
    /// included (`split_at_replay_floor`): GC cannot take them yet, so they
    /// are not `gc_debt_bytes`. Shipped via `report_load_loop`; 0 until the
    /// first GC tick.
    pub open_tail_dead_bytes: std::sync::atomic::AtomicU64,
    /// bytes of SSTable data that compaction would consume on its
    /// next pass — sum of (head-extent table sizes when head-ratio < 30 %)
    /// + (overlap-tagged tables when has_overlap == 1). Refreshed by the
    /// compact loop's periodic tick. Manager emits a `POLICY_KIND_COMPACT`
    /// advisory when sustained above `compact_pending_high`.
    pub pending_compaction_bytes: std::sync::atomic::AtomicU64,
    /// 1 while `background_gc_loop` is inside `run_gc`, else 0. Lets
    /// operators see stuck-GC at a glance and lets the policy engine skip
    /// duplicate advisories for an already-active GC.
    pub gc_inflight: std::sync::atomic::AtomicU32,
    /// 1 while `background_compact_loop` is inside `do_compact`,
    /// else 0.
    pub compact_inflight: std::sync::atomic::AtomicU32,
    /// unix-epoch seconds of the last successful GC `punch_holes`
    /// completion. 0 if never run since process start. Used by the policy
    /// engine to enforce cooldown (no advisory while `now - last_gc_at <
    /// gc_cooldown_sec`).
    pub last_gc_at: std::sync::atomic::AtomicI64,
    /// unix-epoch seconds of the last successful `do_compact`
    /// commit. 0 if never run since process start. Used for compact
    /// cooldown.
    pub last_compact_at: std::sync::atomic::AtomicI64,
    /// Σ bytes occupied by tombstone (op==2) records inside the
    /// partition's live SSTs. Refreshed after every flush + compact
    /// commit. Drives external controller's "should major compact"
    /// decisions; advisory-only here.
    pub sst_tombstone_bytes: std::sync::atomic::AtomicU64,
    /// Σ bytes of SST records whose `expires_at` has passed.
    pub sst_expired_bytes: std::sync::atomic::AtomicU64,
    /// Σ bytes of SST records whose keys fall outside the
    /// partition's current `rg` range. Only > 0 while `has_overlap == 1`.
    pub sst_out_of_range_bytes: std::sync::atomic::AtomicU64,
    /// Cross-thread mirror of `PartitionData.has_overlap` (1 = this
    /// partition's SSTs still carry keys outside its own range).
    ///
    /// `report_load_loop` runs on the MAIN thread and can only reach a
    /// partition through this `Arc`; `PartitionData` lives on the partition's
    /// own thread. Writers go through `PartitionData::set_has_overlap` so the
    /// mirror cannot drift from the Cell it mirrors. The manager needs it
    /// because `handle_split_part` refuses while it is 1.
    pub has_overlap: std::sync::atomic::AtomicU32,
    /// bytes of the minor window `select_minor` picks now. Distinct from
    /// `pending_compaction_bytes` (major). Both can be non-zero
    /// simultaneously.
    pub minor_compact_pending_bytes: std::sync::atomic::AtomicU64,
    /// count of sealed log_stream extents (informational).
    pub sealed_log_extent_count: std::sync::atomic::AtomicU32,
    /// Deletes this partition took that no MAJOR compaction has seen yet.
    ///
    /// A delete frees nothing on its own: the value it kills is reclaimable
    /// only once a compaction drops the superseded entry and records the
    /// discard that GC reads. On an idle partition nothing else gets it there —
    /// the tombstones sit in the memtable (it rotates on size or on the WAL
    /// gap, and a few thousand deletes reach neither), and the size-driven
    /// compaction advisories never fire for an LSM of a few KB, however many
    /// GiB of large values the deletes killed. The manager's policy reads this
    /// to advise the one major compaction that settles such a partition.
    ///
    /// Bumped per delete as it enters the memtable; seeded at open from the
    /// replayed memtable plus the tombstones the loaded SSTs record
    /// (`num_deletions`), so deletes flushed before a restart, split or merge
    /// are still counted. A successful major compaction subtracts what it
    /// covered (the count taken as it flushed), so deletes that arrive during
    /// the compaction stay counted.
    pub unsettled_deletes: std::sync::atomic::AtomicU64,
    /// Terminal outcomes of manager-submitted maintenance ops (compact/gc/
    /// forcegc carrying a non-zero op_id) — a bounded ring copied onto the load
    /// heartbeat so the manager's op-ledger learns the state + error string that
    /// would otherwise die in a PS `tracing::error!`.
    pub maintenance_outcomes: parking_lot::Mutex<VecDeque<manager_rpc::MaintenanceOutcome>>,
    /// Tables in the SST list, memtable bytes (active + queued immutables) and
    /// queued immutables. Set by `PartitionData::publish_lsm_gauges` wherever
    /// one of them changes.
    pub sst_tables: AtomicU64,
    pub memtable_bytes: AtomicU64,
    pub imm_memtables: AtomicU64,
    /// Committed flushes and the SST bytes they wrote.
    pub flushes: AtomicU64,
    pub flush_bytes: AtomicU64,
    /// Completed minor compactions, indexed like `MinorCause::ALL`.
    pub minor_compactions: [AtomicU64; MinorCause::ALL.len()],
    pub major_compactions: AtomicU64,
    /// SST bytes compactions wrote: `[minor, major]`.
    pub compaction_bytes: [AtomicU64; 2],
    /// SSTs a point lookup (GET / HEAD) consulted, by outcome: excluded by key
    /// range or bloom filter, read and found, read and absent (a bloom false
    /// positive). Lookups answered by a memtable consult none.
    pub sst_skipped: AtomicU64,
    pub sst_hits: AtomicU64,
    pub sst_false_positives: AtomicU64,
    /// Large values a GET resolved through this PS from the log stream, and
    /// their bytes. Reads redirected to the EN are not counted.
    pub vp_reads: AtomicU64,
    pub vp_read_bytes: AtomicU64,
    /// Latest progress sample for the maintenance op running right now, or
    /// None when idle. A single slot, not a ring: progress is a sample that is
    /// worthless once superseded, and the merged GC/compaction loop runs one op
    /// at a time by construction.
    pub maintenance_progress: parking_lot::Mutex<Option<manager_rpc::MaintenanceProgress>>,
    /// The running split's phase. Its own slot: a split waits at phase 1 on
    /// the maintenance gate while a compaction or GC reports and clears the
    /// slot above, and a submitted split that goes unreported is judged not
    /// running by the manager.
    pub split_phase: parking_lot::Mutex<Option<manager_rpc::MaintenanceProgress>>,
}

/// Max terminal maintenance outcomes buffered for the heartbeat. Small — each
/// retransmits until newer ones push it out (the manager reconciles by op_id,
/// idempotently), so a couple of heartbeats' worth is plenty.
const MAINT_OUTCOME_RING_CAP: usize = 8;

impl PartitionMetrics {
    pub fn record_flush(&self, sst_bytes: u64) {
        self.flushes.fetch_add(1, Relaxed);
        self.flush_bytes.fetch_add(sst_bytes, Relaxed);
    }

    pub fn record_compaction(&self, major: bool, output_bytes: u64) {
        if major {
            self.major_compactions.fetch_add(1, Relaxed);
        }
        self.compaction_bytes[major as usize].fetch_add(output_bytes, Relaxed);
    }

    pub fn record_minor_compaction(&self, cause: MinorCause) {
        let i = MinorCause::ALL.iter().position(|c| *c == cause).expect("listed cause");
        self.minor_compactions[i].fetch_add(1, Relaxed);
    }

    /// One point lookup's SST outcomes, added once per lookup.
    pub fn record_sst_lookup(&self, skipped: u64, hit: bool, false_positives: u64) {
        if skipped > 0 {
            self.sst_skipped.fetch_add(skipped, Relaxed);
        }
        if hit {
            self.sst_hits.fetch_add(1, Relaxed);
        }
        if false_positives > 0 {
            self.sst_false_positives.fetch_add(false_positives, Relaxed);
        }
    }

    /// Record a terminal maintenance outcome for the next heartbeat (bounded).
    pub fn push_maintenance_outcome(&self, o: manager_rpc::MaintenanceOutcome) {
        let mut r = self.maintenance_outcomes.lock();
        r.push_back(o);
        while r.len() > MAINT_OUTCOME_RING_CAP {
            r.pop_front();
        }
    }
    /// Publish how far the currently-running maintenance op has got. Called
    /// from the op's own loop, so the cost must stay trivial — one short lock,
    /// once per GC chunk / compaction table, never per record.
    pub fn set_maintenance_progress(&self, op_id: u64, kind: u8, done: u64, total: u64) {
        if op_id == 0 {
            return; // PS-local scheduler run: nothing on the manager to update.
        }
        *self.maintenance_progress.lock() = Some(manager_rpc::MaintenanceProgress {
            op_id,
            kind,
            done,
            total,
        });
    }

    /// Publish which PHASE the split has reached.
    ///
    /// Unlike `set_maintenance_progress`, `op_id: 0` is published too: an
    /// untracked split (presplit, test helpers) carries none, and the manager
    /// matches it on (kind, part_id). A submitted split's id also tells the
    /// manager the split is still running after it stopped waiting for the
    /// reply.
    ///
    /// Phases, not bytes. Split's steps cost wildly different amounts — the
    /// flush dominates — so a byte counter sits still through the expensive one
    /// and reads as a hang, which is the exact confusion this is here to end.
    /// A phase index says "still flushing", and that is the true answer.
    pub fn set_maintenance_phase(&self, op_id: u64, kind: u8, phase: u64, phases: u64) {
        *self.split_phase.lock() = Some(manager_rpc::MaintenanceProgress {
            op_id,
            kind,
            done: phase,
            total: phases,
        });
    }

    /// Drop the progress slot — the op finished (its terminal state travels in
    /// the outcome ring). Leaving a stale sample would show a finished op as
    /// forever mid-flight.
    pub fn clear_maintenance_progress(&self) {
        *self.maintenance_progress.lock() = None;
    }

    pub fn clear_split_phase(&self) {
        *self.split_phase.lock() = None;
    }

    /// Snapshot the live progress samples for the heartbeat.
    pub fn snapshot_maintenance_progress(&self) -> Vec<manager_rpc::MaintenanceProgress> {
        let mut v: Vec<_> = self.maintenance_progress.lock().iter().cloned().collect();
        v.extend(self.split_phase.lock().iter().cloned());
        v
    }

    /// Snapshot (copy, not drain) the ring for the heartbeat — idempotent
    /// retransmit covers a dropped report.
    pub fn snapshot_maintenance_outcomes(&self) -> Vec<manager_rpc::MaintenanceOutcome> {
        self.maintenance_outcomes.lock().iter().cloned().collect()
    }
}

/// What `/metrics` serves for this PS, read on the metrics thread.
///
/// Counters are kept per partition so the hot paths of different partitions
/// never share a cache line; the PS-wide value is their sum. A partition no one
/// holds any more can count nothing else, so its counts move into `retired`:
/// the totals never step back when a partition closes, moves or reopens.
/// Gauges sum the partitions still open on this PS.
pub struct PsMetrics {
    block_cache: Arc<BlockCache>,
    plog_load: Arc<PlogLoad>,
    parts: parking_lot::Mutex<Parts>,
}

#[derive(Default)]
struct Parts {
    live: Vec<Arc<PartitionMetrics>>,
    retired: Counts,
}

#[derive(Clone, Default)]
struct HistCounts {
    buckets: [u64; LAT_BUCKET_BOUNDS_NS.len()],
    sum_ns: u64,
    count: u64,
}

impl HistCounts {
    fn add(&mut self, h: &LatHist) {
        for (b, n) in self.buckets.iter_mut().zip(&h.buckets) {
            *b += n.load(Relaxed);
        }
        self.sum_ns += h.sum_ns.load(Relaxed);
        self.count += h.count.load(Relaxed);
    }
}

#[derive(Clone, Default)]
struct Counts {
    requests: u64,
    write_bytes: u64,
    read_bytes: u64,
    flushes: u64,
    flush_bytes: u64,
    minor: [u64; MinorCause::ALL.len()],
    majors: u64,
    /// `[minor, major]`.
    compaction_bytes: [u64; 2],
    /// `[skipped, hit, false_positive]`.
    sst_checks: [u64; 3],
    vp_reads: u64,
    vp_read_bytes: u64,
    write_lat: HistCounts,
    get_lat: HistCounts,
}

impl Counts {
    fn add(&mut self, m: &PartitionMetrics) {
        self.requests += m.req_count_monotonic.load(Relaxed);
        self.write_bytes += m.write_bytes.load(Relaxed);
        self.read_bytes += m.read_bytes.load(Relaxed);
        self.flushes += m.flushes.load(Relaxed);
        self.flush_bytes += m.flush_bytes.load(Relaxed);
        for (t, n) in self.minor.iter_mut().zip(&m.minor_compactions) {
            *t += n.load(Relaxed);
        }
        self.majors += m.major_compactions.load(Relaxed);
        for (t, n) in self.compaction_bytes.iter_mut().zip(&m.compaction_bytes) {
            *t += n.load(Relaxed);
        }
        for (t, n) in self.sst_checks.iter_mut().zip([&m.sst_skipped, &m.sst_hits, &m.sst_false_positives]) {
            *t += n.load(Relaxed);
        }
        self.vp_reads += m.vp_reads.load(Relaxed);
        self.vp_read_bytes += m.vp_read_bytes.load(Relaxed);
        self.write_lat.add(&m.write_lat);
        self.get_lat.add(&m.get_lat);
    }
}

#[derive(Default)]
struct Gauges {
    partitions: u64,
    size_bytes: u64,
    sst_tables: u64,
    sst_tables_max: u64,
    memtable_bytes: u64,
    imm_memtables: u64,
}

impl PsMetrics {
    pub(crate) fn new(block_cache: Arc<BlockCache>, plog_load: Arc<PlogLoad>) -> Self {
        Self {
            block_cache,
            plog_load,
            parts: parking_lot::Mutex::new(Parts::default()),
        }
    }

    /// Count a newly opened partition from now on.
    pub fn register(&self, m: Arc<PartitionMetrics>) {
        self.parts.lock().live.push(m);
    }

    fn snapshot(&self) -> (Counts, Gauges) {
        let mut parts = self.parts.lock();
        let Parts { live, retired } = &mut *parts;
        // Only this list holds it: no one can add to it any more.
        live.retain(|m| {
            let held = Arc::strong_count(m) > 1;
            if !held {
                retired.add(m);
            }
            held
        });
        let mut c = retired.clone();
        let mut g = Gauges::default();
        for m in live.iter() {
            c.add(m);
            if !m.closed.load(Relaxed) {
                let tables = m.sst_tables.load(Relaxed);
                g.partitions += 1;
                g.size_bytes += m.size_bytes.load(Relaxed);
                g.sst_tables += tables;
                g.sst_tables_max = g.sst_tables_max.max(tables);
                g.memtable_bytes += m.memtable_bytes.load(Relaxed);
                g.imm_memtables += m.imm_memtables.load(Relaxed);
            }
        }
        (c, g)
    }

    /// Prometheus text: PS-wide series only, no partition label.
    pub fn render(&self) -> String {
        let (c, g) = self.snapshot();
        let (cache_bytes, _, hits, misses) = self.block_cache.stats();
        let mut out = String::with_capacity(8192);
        let plain: [(&str, &str, f64); 18] = [
            ("autumn_ps_partitions", "gauge", g.partitions as f64),
            ("autumn_ps_size_bytes", "gauge", g.size_bytes as f64),
            ("autumn_ps_sst_tables", "gauge", g.sst_tables as f64),
            ("autumn_ps_partition_sst_tables_max", "gauge", g.sst_tables_max as f64),
            ("autumn_ps_memtable_bytes", "gauge", g.memtable_bytes as f64),
            ("autumn_ps_imm_memtables", "gauge", g.imm_memtables as f64),
            ("autumn_ps_block_cache_hits_total", "counter", hits as f64),
            ("autumn_ps_block_cache_misses_total", "counter", misses as f64),
            ("autumn_ps_block_cache_bytes", "gauge", cache_bytes as f64),
            ("autumn_ps_block_cache_capacity_bytes", "gauge", self.block_cache.cap_bytes() as f64),
            ("autumn_ps_requests_total", "counter", c.requests as f64),
            ("autumn_ps_write_bytes_total", "counter", c.write_bytes as f64),
            ("autumn_ps_read_bytes_total", "counter", c.read_bytes as f64),
            ("autumn_ps_flushes_total", "counter", c.flushes as f64),
            ("autumn_ps_flush_bytes_total", "counter", c.flush_bytes as f64),
            ("autumn_ps_major_compactions_total", "counter", c.majors as f64),
            ("autumn_ps_vp_reads_total", "counter", c.vp_reads as f64),
            ("autumn_ps_vp_read_bytes_total", "counter", c.vp_read_bytes as f64),
        ];
        for (name, kind, v) in plain {
            push_type(&mut out, name, kind);
            push_metric(&mut out, name, &[], v);
        }

        let minor: Vec<(&str, u64)> = MinorCause::ALL.iter().map(|c| c.name()).zip(c.minor).collect();
        let labeled: [(&str, &str, Vec<(&str, u64)>); 3] = [
            ("autumn_ps_minor_compactions_total", "trigger", minor),
            (
                "autumn_ps_compaction_write_bytes_total",
                "kind",
                vec![("minor", c.compaction_bytes[0]), ("major", c.compaction_bytes[1])],
            ),
            (
                "autumn_ps_get_sst_checks_total",
                "result",
                vec![("skipped", c.sst_checks[0]), ("hit", c.sst_checks[1]), ("false_positive", c.sst_checks[2])],
            ),
        ];
        for (name, label, samples) in labeled {
            push_type(&mut out, name, "counter");
            for (value, n) in samples {
                push_metric(&mut out, name, &[(label, value.to_string())], n as f64);
            }
        }

        // Latency histograms: cumulative `le` buckets + _sum + _count.
        for (name, h) in [("autumn_ps_write_duration_seconds", &c.write_lat), ("autumn_ps_get_duration_seconds", &c.get_lat)] {
            push_type(&mut out, name, "histogram");
            let bucket = format!("{name}_bucket");
            let mut cum: u64 = 0;
            for (n, &bound) in h.buckets.iter().zip(&LAT_BUCKET_BOUNDS_NS) {
                cum += n;
                push_metric(&mut out, &bucket, &[("le", format!("{}", bound as f64 / 1e9))], cum as f64);
            }
            push_metric(&mut out, &bucket, &[("le", "+Inf".to_string())], h.count as f64);
            push_metric(&mut out, &format!("{name}_sum"), &[], h.sum_ns as f64 / 1e9);
            push_metric(&mut out, &format!("{name}_count"), &[], h.count as f64);
        }
        self.plog_load.render(&mut out);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(text: &str, series: &str) -> f64 {
        text.lines()
            .find_map(|l| l.strip_prefix(series)?.strip_prefix(' ')?.parse().ok())
            .unwrap_or_else(|| panic!("no sample {series}"))
    }

    #[test]
    fn render_sums_partitions_with_no_partition_label() {
        let ps = PsMetrics::new(Arc::new(BlockCache::new(64 << 20)), Arc::new(PlogLoad::new()));
        let a = Arc::new(PartitionMetrics::default());
        let b = Arc::new(PartitionMetrics::default());
        ps.register(a.clone());
        ps.register(b.clone());
        a.sst_tables.store(7, Relaxed);
        b.sst_tables.store(3, Relaxed);
        a.record_flush(4096);
        b.record_flush(100);
        a.record_compaction(false, 100);
        a.record_compaction(true, 900);
        b.record_minor_compaction(MinorCause::Blocking);
        a.record_sst_lookup(5, true, 1);
        b.get_lat.observe_n(700_000, 2);
        let text = ps.render();

        assert!(!text.contains("part="), "{text}");
        assert_eq!(sample(&text, "autumn_ps_partitions"), 2.0);
        assert_eq!(sample(&text, "autumn_ps_sst_tables"), 10.0);
        assert_eq!(sample(&text, "autumn_ps_partition_sst_tables_max"), 7.0);
        assert_eq!(sample(&text, "autumn_ps_flush_bytes_total"), 4196.0);
        assert_eq!(sample(&text, "autumn_ps_block_cache_capacity_bytes"), (64 << 20) as f64);
        assert_eq!(sample(&text, r#"autumn_ps_compaction_write_bytes_total{kind="major"}"#), 900.0);
        assert_eq!(sample(&text, r#"autumn_ps_minor_compactions_total{trigger="blocking"}"#), 1.0);
        assert_eq!(sample(&text, r#"autumn_ps_get_sst_checks_total{result="false_positive"}"#), 1.0);
        assert_eq!(sample(&text, r#"autumn_ps_get_duration_seconds_bucket{le="0.001"}"#), 2.0);
        assert_eq!(sample(&text, "autumn_ps_get_duration_seconds_count"), 2.0);

        // Every metric's samples form one run after its # TYPE line.
        let mut seen = std::collections::HashSet::new();
        let mut current = String::new();
        for l in text.lines() {
            if let Some(rest) = l.strip_prefix("# TYPE ") {
                current = rest.split(' ').next().unwrap().to_string();
                assert!(seen.insert(current.clone()), "{current} typed twice");
            } else {
                let name = l.split(['{', ' ']).next().unwrap();
                assert!(name.starts_with(&current), "{name} outside its group ({current})");
            }
        }
    }

    #[test]
    fn counters_stay_put_when_a_partition_closes_and_reopens() {
        let ps = PsMetrics::new(Arc::new(BlockCache::new(1 << 20)), Arc::new(PlogLoad::new()));
        let old = Arc::new(PartitionMetrics::default());
        ps.register(old.clone());
        old.record_flush(10);
        old.sst_tables.store(4, Relaxed);
        assert_eq!(sample(&ps.render(), "autumn_ps_flushes_total"), 1.0);

        // Closed but still held (its thread winding down): counted, not open.
        old.closed.store(true, Relaxed);
        let reopened = Arc::new(PartitionMetrics::default());
        ps.register(reopened.clone());
        reopened.record_flush(10);
        let text = ps.render();
        assert_eq!(sample(&text, "autumn_ps_flushes_total"), 2.0);
        assert_eq!(sample(&text, "autumn_ps_partitions"), 1.0);
        assert_eq!(sample(&text, "autumn_ps_sst_tables"), 0.0);

        // Released: its counts move to retired, nothing steps back.
        drop(old);
        assert_eq!(sample(&ps.render(), "autumn_ps_flushes_total"), 2.0);
        assert_eq!(ps.parts.lock().live.len(), 1);
        reopened.record_flush(10);
        assert_eq!(sample(&ps.render(), "autumn_ps_flushes_total"), 3.0);
    }
}
