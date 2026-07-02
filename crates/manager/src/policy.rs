//! F183 policy engine: per-partition load metrics window + split/merge
//! candidate computation. Stage 1 ships advisory only; auto-trigger is
//! gated behind feature flags in Stage 2/3.

use std::collections::{HashMap, HashSet, VecDeque};

use autumn_common::MetadataState;
use autumn_rpc::manager_rpc::{
    PartitionLoad, PolicyCandidate, POLICY_KIND_EC, POLICY_KIND_GC, POLICY_KIND_HOT_COLD,
    POLICY_KIND_MAJOR_COMPACT, POLICY_KIND_MERGE, POLICY_KIND_MINOR_COMPACT, POLICY_KIND_SPLIT,
};

const GIB: u64 = 1024 * 1024 * 1024;

pub const SPLIT_SIZE_HARD: u64 = 50 * GIB;
pub const SPLIT_SIZE_MIN: u64 = GIB;
/// F196: recalibrated 50K → 15K. autumn-rs's measured single-partition
/// QPS ceiling on the perf_check workload is ~30K (one P-log thread,
/// one io_uring). 15K = 50% of ceiling — "this partition is using
/// half its single-thread budget, time to split." Pre-F196 the value
/// was 50K which is unreachable on this engine; condition ② of the
/// SPLIT predicate (`req_per_sec > SPLIT_QPS_HIGH && size > SPLIT_
/// SIZE_MIN`) was effectively dead code.
pub const SPLIT_QPS_HIGH: u32 = 15_000;
pub const SPLIT_IMMFULL_HIGH: u32 = 10;
pub const SPLIT_COOLDOWN_SEC: i64 = 3600;

pub const MERGE_SIZE_LOW: u64 = GIB;
/// F196: recalibrated 5K → 1.5K to preserve the 10× hysteresis ratio
/// against the new `SPLIT_QPS_HIGH = 15K`. 1.5K = 5% of single-partition
/// ceiling — "two adjacent cold partitions barely make a dent, merge
/// them to free a core slot."
pub const MERGE_QPS_LOW: u32 = 1_500;
pub const MERGE_COOLDOWN_SEC: i64 = 6 * 3600;

/// F187: GC debt advisory threshold. Default 1 GiB sustained — large
/// enough to filter normal write churn, small enough that operators
/// notice before disk pressure. Tunable via `PolicyConfig`.
pub const GC_DEBT_HIGH: u64 = GIB;
/// F187: major-compaction debt advisory threshold. Default 4 GiB — higher
/// than GC because compact's pending bytes naturally accumulate to
/// MAX_SKIP_LIST × N tables before the periodic loop fires.
pub const COMPACT_PENDING_HIGH: u64 = 4 * GIB;
/// F187: GC advisory cooldown. Once an advisory fires for a partition,
/// suppress re-emission for 5 min so operators can react without
/// duplicate-noise. Distinct from any auto-trigger cooldown (Stage 2/3
/// territory).
pub const GC_COOLDOWN_SEC: i64 = 300;
/// F187: compaction advisory cooldown.
pub const COMPACT_COOLDOWN_SEC: i64 = 300;

/// F202: minor-compaction advisory threshold. 512 MiB — minor compact
/// is much cheaper than major (size-tiered pickup of < 32 MiB tables
/// only), so the bar to surface as advisory is correspondingly lower.
pub const MINOR_COMPACT_PENDING_HIGH: u64 = 512 * 1024 * 1024;
/// F202: minor-compact advisory cooldown.
pub const MINOR_COMPACT_COOLDOWN_SEC: i64 = 120;
/// F202: minimum sealed-extent size to even consider EC advice. Below
/// this, the EC encode + K+M shard write + manager state churn
/// outweigh the (replication - K/(K+M)) × `sealed_length` saved.
/// Aligned with the deferred PS-side `EC_MIN_EXTENT_BYTES` const in
/// the plan; 64 MiB matches WAS lazy-EC heuristics.
pub const EC_MIN_EXTENT_BYTES: u64 = 64 * 1024 * 1024;

/// F210-F3: drop a partition's metrics window when its latest bucket
/// is older than this. Without it, a deleted / merged-away partition's
/// final `PartitionLoad` snapshot kept driving advisories indefinitely
/// (the manager never explicitly clears `metrics[part_id]` on partition
/// drop). 5 min covers a full `POLICY_REQUIRED_BUCKETS × bucket_sec`
/// window plus slack — anything older has no valid contribution to any
/// `required_buckets`-spanning advisory. Set in seconds (i64 to match
/// the rest of the policy clock).
pub const STALE_METRICS_AGE_SEC: i64 = 300;

/// F210-E3: per-tick cap on EC-conversion advisory candidates. Without
/// it, a backlog of N sealed-not-converted extents (real-cluster
/// observation: 10k+ after a controller pause) emits N candidates per
/// tick into both the manager's `advisory_cache` and every
/// `MSG_GET_POLICY_CANDIDATES` response payload, with no upper bound.
/// 256 was picked as ~1 MiB of rkyv'd advisory payload at the
/// observed ~4 KiB per `PolicyCandidate` — keeps the response under
/// etcd's default `max-request-bytes` ceiling and the controller's
/// per-tick decision space tractable. Sort-by-sealed-length-desc
/// preserves payoff-first ordering (the controller naturally drains
/// the largest extents first, where 3 → K/(K+M) savings dominate);
/// the same extents stay in the backlog across ticks until the
/// controller dispatches `MSG_FORCE_EC_CONVERT`, so truncation does
/// not "lose" them.
pub const MAX_EC_ADVISORY_CANDIDATES: usize = 256;

pub const POLICY_BUCKET_SEC: i64 = 60;
/// F196: trimmed 30 → 10 buckets. Only `REQUIRED_BUCKETS = 5` are ever
/// consulted; the extra 25 buckets were dead memory. 10 keeps 2× safety
/// margin for a future advisory that needs slightly more history.
pub const POLICY_WINDOW_BUCKETS: usize = 10;
pub const POLICY_REQUIRED_BUCKETS: usize = 5;
pub const POLICY_TICK_INTERVAL_SEC: i64 = 60;

/// F196 Stage D: hot/cold imbalance advisory thresholds. Per PS, the
/// ratio `max(req_per_sec) / max(1, min(req_per_sec))` over
/// `required_buckets` consecutive sustained windows must exceed this
/// to trigger a single advisory candidate listing the hot + cold partitions.
pub const HOT_COLD_RATIO: u32 = 10;
/// The hottest partition must additionally exceed this floor so a
/// uniformly-cold cluster (e.g. idle dev) doesn't spam the operator.
/// F196: pinned to **10_000** (≈ 1/3 of the measured ~30K single-partition
/// QPS ceiling). Decoupled from `SPLIT_QPS_HIGH` — operators tuning
/// the split threshold no longer auto-shift the hot/cold floor.
pub const HOT_COLD_MIN_HOT_QPS: u32 = 10_000;
/// F196 Stage D (size dimension): same 10× ratio, but on
/// `size_bytes`. Catches large-value / low-QPS workloads that QPS
/// imbalance can't see.
pub const HOT_COLD_SIZE_RATIO: u64 = 10;
/// Size-dimension floor: the largest partition must exceed this to
/// be worth flagging. Default = half of `SPLIT_SIZE_HARD` (25 GiB),
/// i.e. already at half of the "must split" size — meaningfully big.
pub const HOT_COLD_MIN_HOT_SIZE_BYTES: u64 = SPLIT_SIZE_HARD / 2;
/// Per-PS advisory cooldown — suppress re-emission for 5 min so the
/// operator log isn't flooded while imbalance persists.
pub const HOT_COLD_COOLDOWN_SEC: i64 = 300;
/// F210-F4: band tightener for hot/cold classification. A partition
/// only qualifies as "hot" when its window MIN is still within
/// 1/HOT_COLD_BAND_DIVISOR of the PS's hottest reading — i.e. its
/// LOWEST bucket is still elevated. Symmetric on the cold side.
/// Pre-F210-F4 the classifier picked partitions on equality with the
/// PS-wide max/min, so a partition that oscillated wildly (10k qps in
/// bucket 1, 100 qps in bucket 5) registered as the PS's MAX in
/// bucket 1 AND its MIN in bucket 5 — emitted into BOTH the hot and
/// cold lists of the same advisory. D=2 ≈ "stable within a factor of
/// 2 across the window"; a workload that swings 10× is genuinely
/// neither sustained-hot nor sustained-cold and the advisory should
/// stay silent rather than recommending an action against it.
pub const HOT_COLD_BAND_DIVISOR: u32 = 2;

/// F184: runtime-configurable policy thresholds. Production uses the
/// `*_DEFAULT` constants above; tests can lower `required_buckets` and
/// `tick_interval_sec` to exercise the full policy_tick_loop fast.
#[derive(Clone, Debug)]
pub struct PolicyConfig {
    pub split_size_hard: u64,
    pub split_size_min: u64,
    pub split_qps_high: u32,
    pub split_immfull_high: u32,
    pub split_cooldown_sec: i64,
    pub merge_size_low: u64,
    pub merge_qps_low: u32,
    pub merge_cooldown_sec: i64,
    pub bucket_sec: i64,
    pub window_buckets: usize,
    pub required_buckets: usize,
    pub tick_interval_sec: i64,
    /// F187: gc advisory threshold (bytes, sustained over
    /// `required_buckets`).
    pub gc_debt_high: u64,
    /// F187: compaction advisory threshold (bytes, sustained over
    /// `required_buckets`).
    pub compact_pending_high: u64,
    /// F187: gc advisory cooldown (seconds since `last_gc_at`).
    pub gc_cooldown_sec: i64,
    /// F187: compact advisory cooldown (seconds since `last_compact_at`).
    pub compact_cooldown_sec: i64,
    /// F202: minor-compact advisory threshold (bytes, sustained over
    /// `required_buckets`).
    pub minor_compact_pending_high: u64,
    /// F202: minor-compact advisory cooldown (seconds since
    /// `last_compact_at` — same field; minor and major share it
    /// because they share the PS-side `do_compact` infrastructure).
    pub minor_compact_cooldown_sec: i64,
    /// F202: minimum sealed-extent size below which EC advisory is
    /// suppressed (encode overhead would outweigh the savings).
    pub ec_min_extent_bytes: u64,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            split_size_hard: SPLIT_SIZE_HARD,
            split_size_min: SPLIT_SIZE_MIN,
            split_qps_high: SPLIT_QPS_HIGH,
            split_immfull_high: SPLIT_IMMFULL_HIGH,
            split_cooldown_sec: SPLIT_COOLDOWN_SEC,
            merge_size_low: MERGE_SIZE_LOW,
            merge_qps_low: MERGE_QPS_LOW,
            merge_cooldown_sec: MERGE_COOLDOWN_SEC,
            bucket_sec: POLICY_BUCKET_SEC,
            window_buckets: POLICY_WINDOW_BUCKETS,
            required_buckets: POLICY_REQUIRED_BUCKETS,
            tick_interval_sec: POLICY_TICK_INTERVAL_SEC,
            gc_debt_high: GC_DEBT_HIGH,
            compact_pending_high: COMPACT_PENDING_HIGH,
            gc_cooldown_sec: GC_COOLDOWN_SEC,
            compact_cooldown_sec: COMPACT_COOLDOWN_SEC,
            minor_compact_pending_high: MINOR_COMPACT_PENDING_HIGH,
            minor_compact_cooldown_sec: MINOR_COMPACT_COOLDOWN_SEC,
            ec_min_extent_bytes: EC_MIN_EXTENT_BYTES,
        }
    }
}

#[derive(Default)]
pub struct PartitionMetricsWindow {
    pub buckets: VecDeque<(i64, PartitionLoad)>,
}

impl PartitionMetricsWindow {
    pub fn push(&mut self, ts: i64, load: PartitionLoad) {
        self.push_with_cap_and_bucket(ts, load, POLICY_WINDOW_BUCKETS, POLICY_BUCKET_SEC);
    }
    /// F210-F2: snap `ts` to a `bucket_sec` boundary. If the previous
    /// entry shares that snapped bucket, REPLACE its load (last-wins
    /// within a bucket). Pre-F210-F2 every PS `report_load_loop` tick
    /// (5 s cadence) produced a fresh bucket, so `required_buckets=5`
    /// represented just 25 s of history, not the 5 minutes the
    /// `bucket_sec=60` × `required_buckets=5` arithmetic implied — every
    /// advisory's "sustained over 5 min" guarantee was off by 12×.
    /// Bucketing here makes the windowed advisories' time-span match
    /// their documented semantics regardless of report cadence.
    pub fn push_with_cap_and_bucket(
        &mut self,
        ts: i64,
        load: PartitionLoad,
        cap: usize,
        bucket_sec: i64,
    ) {
        let bucket_sec = bucket_sec.max(1);
        let snapped = (ts / bucket_sec) * bucket_sec;
        if let Some(back) = self.buckets.back_mut() {
            if back.0 == snapped {
                back.1 = load;
                return;
            }
        }
        self.buckets.push_back((snapped, load));
        while self.buckets.len() > cap {
            self.buckets.pop_front();
        }
    }

    /// The most-recent `n` buckets (newest-first), or `None` when the window
    /// holds fewer than `n` (insufficient history → the caller skips the
    /// advisory). Dedups the `buckets.iter().rev().take(n)` + length-guard
    /// preamble shared by every `compute_*` advisory.
    fn recent(&self, n: usize) -> Option<Vec<&(i64, PartitionLoad)>> {
        let bs: Vec<_> = self.buckets.iter().rev().take(n).collect();
        (bs.len() >= n).then_some(bs)
    }
}

#[derive(Default)]
pub struct PolicyEngine {
    pub metrics: HashMap<u64, PartitionMetricsWindow>,
    /// (kind, primary, secondary) -> ts of last advisory log; used to
    /// avoid spamming the same candidate every tick.
    pub last_advisory_at: HashMap<(u8, u64, u64), i64>,
    pub advisory_cache: Vec<PolicyCandidate>,
    pub advisory_cache_at: i64,
    /// F184: runtime-configurable thresholds. Default production values;
    /// tests can override via `set_config`.
    pub config: PolicyConfig,
    /// F196 Stage D: per-PS hot/cold advisory cooldown. Keyed by
    /// `ps_id`; value is the unix-epoch second of the last WARN.
    pub last_hot_cold_at: HashMap<u64, i64>,
}

impl PolicyEngine {
    pub fn set_config(&mut self, config: PolicyConfig) {
        self.config = config;
    }

    /// F210-F3: drop metrics windows that no longer correspond to a
    /// known partition (post-split / merge / PS-evict) OR whose latest
    /// bucket is stale beyond `STALE_METRICS_AGE_SEC`. Pre-F210-F3 the
    /// `policy_tick_loop` would happily emit advisories — including
    /// MERGE candidates pointing at a victim that no longer exists —
    /// off these zombie windows. Also clears the per-PS hot/cold
    /// cooldown for evicted PSes so the entry doesn't leak forever.
    /// Called once at the top of `policy_tick_loop` before any
    /// `compute_*_advisory` runs.
    pub fn prune_stale_metrics(&mut self, state: &MetadataState, now: i64) {
        let known_parts: HashSet<u64> = state.partitions.keys().copied().collect();
        let known_pses: HashSet<u64> = state.regions.values().map(|r| r.ps_id).collect();
        self.metrics.retain(|part_id, window| {
            if !known_parts.contains(part_id) {
                return false;
            }
            match window.buckets.back() {
                Some((ts, _)) => now - *ts <= STALE_METRICS_AGE_SEC,
                None => false,
            }
        });
        self.last_hot_cold_at
            .retain(|ps_id, _| known_pses.contains(ps_id));
        // last_advisory_at entries naturally age out as policy_tick_loop
        // overwrites the cache; not worth a targeted prune.
    }
}

pub struct ComputeArgs<'a> {
    pub state: &'a MetadataState,
    pub last_op_at: &'a HashMap<u64, i64>,
    /// part_id -> ps_id from regions; used to mark merge candidates
    /// `same_ps = false` when adjacent partitions live on different PSes.
    pub region_owners: &'a HashMap<u64, u64>,
    pub now: i64,
}

impl PolicyEngine {
    pub fn compute_candidates(&mut self, args: ComputeArgs<'_>) -> Vec<PolicyCandidate> {
        let mut out = self.split_candidates(&args);
        out.extend(self.merge_candidates(&args));
        self.advisory_cache = out.clone();
        self.advisory_cache_at = args.now;
        out
    }

    /// F183 SPLIT pass — per-partition sliding-window trigger.
    ///
    /// For each metrics-tracked partition, requires ALL of the last
    /// `required_buckets` to show a split trigger (size-hard, sustained
    /// QPS-over-min-size, or imm-full-high) and the partition to be
    /// outside its `split_cooldown_sec`. Emits one `POLICY_KIND_SPLIT`
    /// candidate per qualifying partition.
    fn split_candidates(&self, args: &ComputeArgs<'_>) -> Vec<PolicyCandidate> {
        let mut out = Vec::new();
        let cfg = self.config.clone();

        for (&part_id, window) in self.metrics.iter() {
            let Some(bs) = window.recent(cfg.required_buckets) else {
                continue;
            };

            let last_op = args.last_op_at.get(&part_id).copied().unwrap_or(0);
            if args.now - last_op < cfg.split_cooldown_sec {
                continue;
            }

            // ALL of the last required_buckets must show a trigger.
            let all_match = bs.iter().all(|(_, l)| {
                l.size_bytes > cfg.split_size_hard
                    || (l.req_per_sec > cfg.split_qps_high && l.size_bytes > cfg.split_size_min)
                    || l.imm_full_per_sec > cfg.split_immfull_high
            });
            if !all_match {
                continue;
            }

            let recent = &bs[0].1;
            let reason = if recent.size_bytes > cfg.split_size_hard {
                format!(
                    "size_bytes>{} ({} GiB)",
                    cfg.split_size_hard,
                    recent.size_bytes / GIB
                )
            } else if recent.imm_full_per_sec > cfg.split_immfull_high {
                format!("imm_full_per_sec>{} sustained", cfg.split_immfull_high)
            } else {
                format!(
                    "req_per_sec>{} sustained AND size_bytes>{}",
                    cfg.split_qps_high, cfg.split_size_min
                )
            };
            out.push(PolicyCandidate {
                kind: POLICY_KIND_SPLIT,
                primary_part_id: part_id,
                secondary_part_id: 0,
                reason,
                size_bytes: recent.size_bytes,
                req_per_sec: recent.req_per_sec,
                imm_full_per_sec: recent.imm_full_per_sec,
                same_ps: true, // not meaningful for split
                last_op_at: last_op,
            });
        }
        out
    }

    /// F183 MERGE pass — adjacent-pair sliding-window trigger.
    ///
    /// Walks partitions sorted by `start_key`; for each adjacent pair
    /// where `left.end_key == right.start_key`, requires ALL of the last
    /// `required_buckets` in BOTH windows to qualify as cold (size-low,
    /// summed-QPS-low, zero imm-full) and the pair to be outside
    /// `merge_cooldown_sec`. Emits one `POLICY_KIND_MERGE` candidate
    /// (survivor = left) per qualifying pair; `same_ps` reflects whether
    /// the pair currently lives on the same PS (cross-PS merges are
    /// infeasible and flagged in `reason`).
    fn merge_candidates(&self, args: &ComputeArgs<'_>) -> Vec<PolicyCandidate> {
        let mut out = Vec::new();
        let cfg = self.config.clone();

        // Walk partitions sorted by start_key; for each adjacent pair where
        // left.end_key == right.start_key, check both windows.
        let mut sorted_parts: Vec<(u64, &autumn_rpc::manager_rpc::MgrPartitionMeta)> = args
            .state
            .partitions
            .iter()
            .filter_map(|(id, p)| p.rg.as_ref().map(|_| (*id, p)))
            .collect();
        sorted_parts.sort_by(|(_, a), (_, b)| {
            a.rg.as_ref()
                .unwrap()
                .start_key
                .cmp(&b.rg.as_ref().unwrap().start_key)
        });

        for win in sorted_parts.windows(2) {
            let (left_id, left_meta) = win[0];
            let (right_id, right_meta) = win[1];
            if left_meta.rg.as_ref().unwrap().end_key != right_meta.rg.as_ref().unwrap().start_key {
                continue;
            }
            let lw = match self.metrics.get(&left_id) {
                Some(w) => w,
                None => continue,
            };
            let rw = match self.metrics.get(&right_id) {
                Some(w) => w,
                None => continue,
            };
            let (Some(lbs), Some(rbs)) = (
                lw.recent(cfg.required_buckets),
                rw.recent(cfg.required_buckets),
            ) else {
                continue;
            };

            let last_op_l = args.last_op_at.get(&left_id).copied().unwrap_or(0);
            let last_op_r = args.last_op_at.get(&right_id).copied().unwrap_or(0);
            let max_last_op = last_op_l.max(last_op_r);
            if args.now - max_last_op < cfg.merge_cooldown_sec {
                continue;
            }

            let all_qualify = lbs.iter().zip(rbs.iter()).all(|((_, lb), (_, rb))| {
                lb.size_bytes < cfg.merge_size_low
                    && rb.size_bytes < cfg.merge_size_low
                    && (lb.req_per_sec + rb.req_per_sec) < cfg.merge_qps_low
                    && lb.imm_full_per_sec == 0
                    && rb.imm_full_per_sec == 0
            });
            if !all_qualify {
                continue;
            }

            let same_ps = match (
                args.region_owners.get(&left_id),
                args.region_owners.get(&right_id),
            ) {
                (Some(a), Some(b)) => a == b,
                _ => false,
            };
            let recent_l = &lbs[0].1;
            let recent_r = &rbs[0].1;
            out.push(PolicyCandidate {
                kind: POLICY_KIND_MERGE,
                primary_part_id: left_id, // survivor candidate = left
                secondary_part_id: right_id,
                reason: format!(
                    "size_sum<{} qps_sum<{} sustained{}",
                    cfg.merge_size_low,
                    cfg.merge_qps_low,
                    if !same_ps {
                        " (cross-PS, infeasible)"
                    } else {
                        ""
                    }
                ),
                size_bytes: recent_l.size_bytes + recent_r.size_bytes,
                req_per_sec: recent_l.req_per_sec + recent_r.req_per_sec,
                imm_full_per_sec: 0,
                same_ps,
                last_op_at: max_last_op,
            });
        }

        out
    }

    /// F187: maintenance (GC + compact) advisory pass. Mirrors the F183
    /// split/merge structure: require all of the most recent
    /// `required_buckets` to exceed the threshold, gate by per-kind
    /// cooldown driven from the partition's own `last_gc_at` /
    /// `last_compact_at` (PS-reported, not manager-tracked — the PS is
    /// the authority on when its loops actually ran). Skips partitions
    /// where the corresponding loop is currently inflight (no point
    /// telling an operator to GC a partition that's already GCing).
    ///
    /// The output uses `PolicyCandidate` with `kind = POLICY_KIND_GC`
    /// or `POLICY_KIND_COMPACT`. `secondary_part_id = 0`,
    /// `same_ps = true` (not meaningful for maintenance), `last_op_at`
    /// carries the kind-specific last-run timestamp so the operator
    /// dashboard can render "since X minutes ago".
    pub fn compute_maintenance_advisory(&mut self, now: i64) -> Vec<PolicyCandidate> {
        let mut out = Vec::new();
        let cfg = self.config.clone();

        for (&part_id, window) in self.metrics.iter() {
            let Some(bs) = window.recent(cfg.required_buckets) else {
                continue;
            };
            // Three independent maintenance checks per partition, each
            // emitting at most one candidate. Kept as separate named
            // sub-checks (NOT one parametrised helper) because the
            // metric / threshold / reason text differ per kind.
            out.extend(Self::gc_advisory(part_id, &bs, &cfg, now));
            out.extend(Self::major_compact_advisory(part_id, &bs, &cfg, now));
            out.extend(Self::minor_compact_advisory(part_id, &bs, &cfg, now));
        }

        out
    }

    /// GC maintenance sub-check (F187). Emits a `POLICY_KIND_GC`
    /// candidate when no GC is inflight on the partition, it is outside
    /// `gc_cooldown_sec`, and ALL recent buckets sustain
    /// `gc_debt_bytes > gc_debt_high`.
    fn gc_advisory(
        part_id: u64,
        bs: &[&(i64, PartitionLoad)],
        cfg: &PolicyConfig,
        now: i64,
    ) -> Option<PolicyCandidate> {
        let recent = &bs[0].1;
        // Skip when an inflight GC is already chewing on this
        // partition; let that complete before re-advising.
        if recent.gc_inflight == 0
            && (recent.last_gc_at == 0 || now - recent.last_gc_at >= cfg.gc_cooldown_sec)
            && bs.iter().all(|(_, l)| l.gc_debt_bytes > cfg.gc_debt_high)
        {
            Some(PolicyCandidate {
                kind: POLICY_KIND_GC,
                primary_part_id: part_id,
                secondary_part_id: 0,
                reason: format!(
                    "gc_debt_bytes>{} ({} MiB) sustained {}m",
                    cfg.gc_debt_high,
                    recent.gc_debt_bytes / (1024 * 1024),
                    cfg.required_buckets * cfg.bucket_sec as usize / 60,
                ),
                size_bytes: recent.gc_debt_bytes,
                req_per_sec: recent.req_per_sec,
                imm_full_per_sec: recent.imm_full_per_sec,
                same_ps: true,
                last_op_at: recent.last_gc_at,
            })
        } else {
            None
        }
    }

    /// Major-compaction maintenance sub-check (F187). Emits a
    /// `POLICY_KIND_MAJOR_COMPACT` candidate when no compaction is
    /// inflight, the partition is outside `compact_cooldown_sec`, and
    /// ALL recent buckets sustain
    /// `pending_compaction_bytes > compact_pending_high`.
    fn major_compact_advisory(
        part_id: u64,
        bs: &[&(i64, PartitionLoad)],
        cfg: &PolicyConfig,
        now: i64,
    ) -> Option<PolicyCandidate> {
        let recent = &bs[0].1;
        if recent.compact_inflight == 0
            && (recent.last_compact_at == 0
                || now - recent.last_compact_at >= cfg.compact_cooldown_sec)
            && bs
                .iter()
                .all(|(_, l)| l.pending_compaction_bytes > cfg.compact_pending_high)
        {
            Some(PolicyCandidate {
                kind: POLICY_KIND_MAJOR_COMPACT,
                primary_part_id: part_id,
                secondary_part_id: 0,
                reason: format!(
                    "pending_compaction_bytes>{} ({} MiB) sustained {}m",
                    cfg.compact_pending_high,
                    recent.pending_compaction_bytes / (1024 * 1024),
                    cfg.required_buckets * cfg.bucket_sec as usize / 60,
                ),
                size_bytes: recent.pending_compaction_bytes,
                req_per_sec: recent.req_per_sec,
                imm_full_per_sec: recent.imm_full_per_sec,
                same_ps: true,
                last_op_at: recent.last_compact_at,
            })
        } else {
            None
        }
    }

    /// Minor-compaction maintenance sub-check (F202). Independent from
    /// the major path: minor compact addresses size-tiered write-amp
    /// hygiene, not dead-data cleanup. Common-sense filter: only emit
    /// when the PS-side `minor_compact_pending_bytes` is non-zero (i.e.
    /// `pickup_tables` actually had something to do) so we don't spam
    /// advisories for partitions with no real work. Emits a
    /// `POLICY_KIND_MINOR_COMPACT` candidate when no compaction is
    /// inflight, the partition is outside `minor_compact_cooldown_sec`,
    /// and ALL recent buckets sustain
    /// `minor_compact_pending_bytes > minor_compact_pending_high`.
    fn minor_compact_advisory(
        part_id: u64,
        bs: &[&(i64, PartitionLoad)],
        cfg: &PolicyConfig,
        now: i64,
    ) -> Option<PolicyCandidate> {
        let recent = &bs[0].1;
        if recent.compact_inflight == 0
            && recent.minor_compact_pending_bytes > 0
            && (recent.last_compact_at == 0
                || now - recent.last_compact_at >= cfg.minor_compact_cooldown_sec)
            && bs
                .iter()
                .all(|(_, l)| l.minor_compact_pending_bytes > cfg.minor_compact_pending_high)
        {
            Some(PolicyCandidate {
                kind: POLICY_KIND_MINOR_COMPACT,
                primary_part_id: part_id,
                secondary_part_id: 0,
                reason: format!(
                    "minor_compact_pending_bytes>{} ({} MiB) sustained {}m",
                    cfg.minor_compact_pending_high,
                    recent.minor_compact_pending_bytes / (1024 * 1024),
                    cfg.required_buckets * cfg.bucket_sec as usize / 60,
                ),
                size_bytes: recent.minor_compact_pending_bytes,
                req_per_sec: recent.req_per_sec,
                imm_full_per_sec: recent.imm_full_per_sec,
                same_ps: true,
                last_op_at: recent.last_compact_at,
            })
        } else {
            None
        }
    }

    /// F202: EC-conversion advisory. Scans the manager state for
    /// sealed-but-not-converted extents that exceed
    /// `cfg.ec_min_extent_bytes`. Emits one `POLICY_KIND_EC`
    /// candidate per such extent. Stage 2 is advisory-only.
    ///
    /// **Note**: this advisory's input is `MetadataState`, not the
    /// per-partition `metrics` window — EC is per-extent, not
    /// per-partition. `secondary_part_id` carries the extent_id;
    /// `primary_part_id = 0` (no partition association). `size_bytes`
    /// carries `sealed_length` so external controllers can sort by
    /// payoff.
    ///
    /// Common-sense filter: small sealed extents (< 64 MiB by
    /// default) are NOT surfaced. Below that threshold, EC overhead
    /// (network fanout + ec_encode CPU + metadata churn) outweighs
    /// the 3 → K/(K+M) replication savings; we'd be advising a
    /// negative-EV operation. Operators who still want to convert
    /// such extents can use `client set-stream-ec --stream <ID>`
    /// which bypasses the advisory layer entirely.
    pub fn compute_ec_advisory(&self, state: &MetadataState, now: i64) -> Vec<PolicyCandidate> {
        let mut out = Vec::new();
        let cfg = &self.config;
        for stream in state.streams.values() {
            if stream.ec_data_shard == 0 || stream.ec_parity_shard == 0 {
                // Replication-only stream — no EC policy attached, so
                // there's nothing to convert.
                continue;
            }
            for &eid in &stream.extent_ids {
                let ext = match state.extents.get(&eid) {
                    Some(e) => e,
                    None => continue,
                };
                if ext.ec_converted {
                    continue;
                }
                if ext.sealed_length == 0 {
                    // Open or sealed-at-zero — encode has nothing to
                    // do. Open-tail won't be EC'd until sealed; F201
                    // GC empty-extent picks reclaims sealed-at-zero.
                    continue;
                }
                if ext.sealed_length < cfg.ec_min_extent_bytes {
                    continue;
                }
                out.push(PolicyCandidate {
                    kind: POLICY_KIND_EC,
                    primary_part_id: 0,
                    secondary_part_id: eid,
                    reason: format!(
                        "stream {} ec={}+{} sealed_length={} ({} MiB) not-converted",
                        stream.stream_id,
                        stream.ec_data_shard,
                        stream.ec_parity_shard,
                        ext.sealed_length,
                        ext.sealed_length / (1024 * 1024),
                    ),
                    size_bytes: ext.sealed_length,
                    req_per_sec: 0,
                    imm_full_per_sec: 0,
                    same_ps: true,
                    last_op_at: now, // EC has no per-extent cooldown stamp; this is freshness only
                });
            }
        }
        // F210-E3: cap the per-tick advisory at the top
        // `MAX_EC_ADVISORY_CANDIDATES` extents sorted by `sealed_length`
        // desc. Pre-F210-E3 the function emitted one candidate per
        // sealed-not-converted extent unconditionally; on a cluster
        // that ran without EC for a while (or any time the external
        // controller is slow to drain candidates), the backlog of
        // 10k+ sealed extents inflated both the manager's
        // `advisory_cache` and every `MSG_GET_POLICY_CANDIDATES` RPC
        // payload — the largest payload observed on a real cluster
        // was 1.4 MiB on a single tick, enough to push the etcd
        // sidecar response past its 1 MiB default `max-request-bytes`.
        // Sorting by `sealed_length` desc + truncating preserves
        // payoff-first ordering (the controller works through the
        // biggest extents first, where the 3 → K/(K+M) savings
        // dominate) and survives across ticks because the same
        // extents stay sealed-not-converted until the controller
        // dispatches `MSG_FORCE_EC_CONVERT`.
        if out.len() > MAX_EC_ADVISORY_CANDIDATES {
            out.sort_unstable_by_key(|c| std::cmp::Reverse(c.size_bytes));
            out.truncate(MAX_EC_ADVISORY_CANDIDATES);
        }
        out
    }

    /// F196 Stage D — hot/cold imbalance advisory.
    ///
    /// Groups partitions by their owning PS via `region_owners` and, for
    /// every PS that hosts at least two metrics-tracked partitions,
    /// computes per-partition sustained min/max over the last
    /// `required_buckets` for TWO dimensions:
    ///
    /// 1. **QPS**: `req_per_sec`. Triggers when the PS's `max(part.max_req)
    ///    / max(1, min(part.min_req)) >= HOT_COLD_RATIO (10)` AND the
    ///    hottest's `max_req > HOT_COLD_MIN_HOT_QPS (10_000)`.
    /// 2. **Size**: `size_bytes`. Same 10× ratio with floor
    ///    `HOT_COLD_MIN_HOT_SIZE_BYTES (SPLIT_SIZE_HARD/2 = 25 GiB)`.
    ///    Catches large-value / low-QPS workloads that the QPS check
    ///    can't see.
    ///
    /// Either dimension is enough to fire. One `PolicyCandidate` per PS
    /// is emitted (kind = `POLICY_KIND_HOT_COLD`), with the hottest +
    /// coldest part_ids in `primary_part_id` / `secondary_part_id` and
    /// the full hot/cold lists + ratios in `reason`. Shared per-PS
    /// cooldown via `last_hot_cold_at`: at most one candidate per PS per
    /// `HOT_COLD_COOLDOWN_SEC` (5 min).
    ///
    /// The returned candidates are appended to the manager's
    /// `advisory_cache` by `policy_tick_loop`, so `client info`
    /// (via `MSG_GET_POLICY_CANDIDATES`) renders them next to SPLIT /
    /// MERGE / GC / COMPACT advisories. A matching `WARN` line is also
    /// emitted to `manager.log` for at-a-glance operator awareness.
    /// F210-F4 hot/cold band classification for ONE dimension (qps or size).
    /// Hot = own window MAX equals the PS's hottest AND own MIN is still
    /// within the band of it (sustained-high across its buckets); symmetric
    /// on cold. Returns sorted `(hot, cold)` part-id lists; both empty when
    /// the dimension didn't fire. `parts` tuples are
    /// `(part_id, min_req, max_req, min_size, max_size)` — the accessors
    /// select which pair this dimension reads.
    #[allow(clippy::too_many_arguments)]
    fn classify_hot_cold_band<T: Copy + PartialOrd>(
        parts: &[(u64, u32, u32, u64, u64)],
        fire: bool,
        min_of: impl Fn(&(u64, u32, u32, u64, u64)) -> T,
        max_of: impl Fn(&(u64, u32, u32, u64, u64)) -> T,
        hottest: T,
        coldest: T,
        hot_min: T,
        cold_max: T,
    ) -> (Vec<u64>, Vec<u64>) {
        if !fire {
            return (Vec::new(), Vec::new());
        }
        let mut hot: Vec<u64> = parts
            .iter()
            .filter(|t| min_of(t) >= hot_min && max_of(t) == hottest)
            .map(|t| t.0)
            .collect();
        let mut cold: Vec<u64> = parts
            .iter()
            .filter(|t| max_of(t) <= cold_max && min_of(t) == coldest)
            .map(|t| t.0)
            .collect();
        hot.sort_unstable();
        cold.sort_unstable();
        (hot, cold)
    }

    pub fn compute_hot_cold_advisory(
        &mut self,
        region_owners: &HashMap<u64, u64>,
        now: i64,
    ) -> Vec<PolicyCandidate> {
        let mut out: Vec<PolicyCandidate> = Vec::new();
        let cfg = self.config.clone();
        // ps_id -> Vec<(part_id, min_req, max_req, min_size, max_size)>
        let mut by_ps: HashMap<u64, Vec<(u64, u32, u32, u64, u64)>> = HashMap::new();
        for (&part_id, window) in self.metrics.iter() {
            let Some(bs) = window.recent(cfg.required_buckets) else {
                continue;
            };
            let ps_id = match region_owners.get(&part_id) {
                Some(p) => *p,
                None => continue,
            };
            let min_req = bs.iter().map(|(_, l)| l.req_per_sec).min().unwrap_or(0);
            let max_req = bs.iter().map(|(_, l)| l.req_per_sec).max().unwrap_or(0);
            let min_size = bs.iter().map(|(_, l)| l.size_bytes).min().unwrap_or(0);
            let max_size = bs.iter().map(|(_, l)| l.size_bytes).max().unwrap_or(0);
            by_ps
                .entry(ps_id)
                .or_default()
                .push((part_id, min_req, max_req, min_size, max_size));
        }
        for (ps_id, parts) in by_ps {
            if parts.len() < 2 {
                continue;
            }
            if let Some(&last) = self.last_hot_cold_at.get(&ps_id) {
                if now - last < HOT_COLD_COOLDOWN_SEC {
                    continue;
                }
            }
            // ── QPS dimension ──────────────────────────────────────
            let qps_hottest = parts.iter().map(|t| t.2).max().unwrap_or(0);
            let qps_coldest = parts.iter().map(|t| t.1).min().unwrap_or(0);
            let qps_fire = qps_hottest >= HOT_COLD_MIN_HOT_QPS
                && qps_hottest / qps_coldest.max(1) >= HOT_COLD_RATIO;
            // ── Size dimension ─────────────────────────────────────
            let size_hottest = parts.iter().map(|t| t.4).max().unwrap_or(0);
            let size_coldest = parts.iter().map(|t| t.3).min().unwrap_or(0);
            let size_fire = size_hottest >= HOT_COLD_MIN_HOT_SIZE_BYTES
                && size_hottest / size_coldest.max(1) >= HOT_COLD_SIZE_RATIO;
            if !qps_fire && !size_fire {
                continue;
            }
            // ── Hot/cold lists per triggering dimension ────────────
            //
            // Hot = every partition whose own max equals the PS's
            // `hottest` (i.e. is "the busiest" — at least one partition
            // always qualifies). Cold = every partition whose own min
            // equals the PS's `coldest`. Ties (rare) put multiple
            // partitions in the same list.
            //
            // The earlier median-based classification didn't work for
            // 2-partition PSes (median == max → hot list empty), so
            // this min/max-match approach is what we use.
            // F210-F4: partition qualifies as "hot" only when its own
            // window MIN is still within 1/HOT_COLD_BAND_DIVISOR of the
            // PS's hottest reading (sustained-high across its buckets);
            // symmetric on cold. Excludes rotating hotspots that would
            // otherwise appear on BOTH lists in the same advisory.
            let qps_band = HOT_COLD_BAND_DIVISOR.max(1);
            let (qps_hot, qps_cold) = Self::classify_hot_cold_band(
                &parts,
                qps_fire,
                |t| t.1,
                |t| t.2,
                qps_hottest,
                qps_coldest,
                qps_hottest / qps_band,
                qps_coldest.saturating_mul(qps_band),
            );
            let size_band = HOT_COLD_BAND_DIVISOR.max(1) as u64;
            let (size_hot, size_cold) = Self::classify_hot_cold_band(
                &parts,
                size_fire,
                |t| t.3,
                |t| t.4,
                size_hottest,
                size_coldest,
                size_hottest / size_band,
                size_coldest.saturating_mul(size_band),
            );
            // F210-F4: if the band filter dropped every candidate on a
            // triggering dimension, suppress the advisory for that
            // dimension. Without this we'd still log a fire WARN but
            // emit empty hot/cold lists.
            let qps_fire = qps_fire && !qps_hot.is_empty() && !qps_cold.is_empty();
            let size_fire = size_fire && !size_hot.is_empty() && !size_cold.is_empty();
            if !qps_fire && !size_fire {
                continue;
            }
            let qps_ratio = qps_hottest / qps_coldest.max(1);
            let size_ratio = size_hottest / size_coldest.max(1);
            tracing::warn!(
                ps_id,
                qps_fire,
                qps_ratio,
                qps_hottest,
                qps_coldest,
                qps_hot_parts = ?qps_hot,
                qps_cold_parts = ?qps_cold,
                size_fire,
                size_ratio,
                size_hottest_mb = size_hottest / (1024 * 1024),
                size_coldest_mb = size_coldest / (1024 * 1024),
                size_hot_parts = ?size_hot,
                size_cold_parts = ?size_cold,
                "F196 hot/cold imbalance on PS — consider auto-split for the hot partitions, or merge the cold pair to free a core slot"
            );
            // Hottest part = first qps_hot if QPS triggered else first
            // size_hot; coldest = symmetric. Fall back to 0 when a
            // dimension didn't trigger or its hot/cold list is empty.
            let primary = qps_hot
                .first()
                .copied()
                .or_else(|| size_hot.first().copied())
                .unwrap_or(0);
            let secondary = qps_cold
                .first()
                .copied()
                .or_else(|| size_cold.first().copied())
                .unwrap_or(0);
            let mut reason_parts: Vec<String> = Vec::new();
            reason_parts.push(format!("ps_id={ps_id}"));
            if qps_fire {
                reason_parts.push(format!(
                    "qps_ratio={qps_ratio} hot={qps_hot:?} cold={qps_cold:?}"
                ));
            }
            if size_fire {
                reason_parts.push(format!(
                    "size_ratio={size_ratio} hot={size_hot:?} cold={size_cold:?}"
                ));
            }
            out.push(PolicyCandidate {
                kind: POLICY_KIND_HOT_COLD,
                primary_part_id: primary,
                secondary_part_id: secondary,
                reason: reason_parts.join(" "),
                size_bytes: size_hottest,
                req_per_sec: qps_hottest,
                imm_full_per_sec: 0,
                same_ps: true,
                last_op_at: now,
            });
            self.last_hot_cold_at.insert(ps_id, now);
        }
        out
    }
}
