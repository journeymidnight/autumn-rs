//! F183 policy engine: per-partition load metrics window + split/merge
//! candidate computation. Stage 1 ships advisory only; auto-trigger is
//! gated behind feature flags in Stage 2/3.
//!
//! See `docs/superpowers/specs/2026-05-09-partition-merge-and-split-merge-policy-design.md`.

use std::collections::{HashMap, VecDeque};

use autumn_common::MetadataState;
use autumn_rpc::manager_rpc::{
    PartitionLoad, PolicyCandidate, POLICY_KIND_COMPACT, POLICY_KIND_GC, POLICY_KIND_MERGE,
    POLICY_KIND_SPLIT,
};

const GIB: u64 = 1024 * 1024 * 1024;

pub const SPLIT_SIZE_HARD: u64 = 50 * GIB;
pub const SPLIT_SIZE_MIN: u64 = GIB;
pub const SPLIT_QPS_HIGH: u32 = 50_000;
pub const SPLIT_IMMFULL_HIGH: u32 = 10;
pub const SPLIT_COOLDOWN_SEC: i64 = 3600;

pub const MERGE_SIZE_LOW: u64 = GIB;
pub const MERGE_QPS_LOW: u32 = 5_000;
pub const MERGE_COOLDOWN_SEC: i64 = 6 * 3600;

/// F187: GC debt advisory threshold. Default 1 GiB sustained — large
/// enough to filter normal write churn, small enough that operators
/// notice before disk pressure. Tunable via `PolicyConfig`.
pub const GC_DEBT_HIGH: u64 = GIB;
/// F187: compaction debt advisory threshold. Default 4 GiB — higher
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

pub const POLICY_BUCKET_SEC: i64 = 60;
pub const POLICY_WINDOW_BUCKETS: usize = 30;
pub const POLICY_REQUIRED_BUCKETS: usize = 5;
pub const POLICY_TICK_INTERVAL_SEC: i64 = 60;

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
        }
    }
}

#[derive(Default)]
pub struct PartitionMetricsWindow {
    pub buckets: VecDeque<(i64, PartitionLoad)>,
}

impl PartitionMetricsWindow {
    pub fn push(&mut self, ts: i64, load: PartitionLoad) {
        self.push_with_cap(ts, load, POLICY_WINDOW_BUCKETS);
    }
    pub fn push_with_cap(&mut self, ts: i64, load: PartitionLoad, cap: usize) {
        self.buckets.push_back((ts, load));
        while self.buckets.len() > cap {
            self.buckets.pop_front();
        }
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
}

impl PolicyEngine {
    pub fn set_config(&mut self, config: PolicyConfig) {
        self.config = config;
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
        let mut out = Vec::new();
        let cfg = self.config.clone();

        // ── SPLIT pass ──────────────────────────────────────────────────────
        for (&part_id, window) in self.metrics.iter() {
            let bs: Vec<&(i64, PartitionLoad)> = window
                .buckets
                .iter()
                .rev()
                .take(cfg.required_buckets)
                .collect();
            if bs.len() < cfg.required_buckets {
                continue;
            }

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

        // ── MERGE pass ──────────────────────────────────────────────────────
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
            if left_meta.rg.as_ref().unwrap().end_key
                != right_meta.rg.as_ref().unwrap().start_key
            {
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
            let lbs: Vec<&(i64, PartitionLoad)> =
                lw.buckets.iter().rev().take(cfg.required_buckets).collect();
            let rbs: Vec<&(i64, PartitionLoad)> =
                rw.buckets.iter().rev().take(cfg.required_buckets).collect();
            if lbs.len() < cfg.required_buckets || rbs.len() < cfg.required_buckets {
                continue;
            }

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

        self.advisory_cache = out.clone();
        self.advisory_cache_at = args.now;
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
    pub fn compute_maintenance_advisory(
        &mut self,
        now: i64,
    ) -> Vec<PolicyCandidate> {
        let mut out = Vec::new();
        let cfg = self.config.clone();

        for (&part_id, window) in self.metrics.iter() {
            let bs: Vec<&(i64, PartitionLoad)> = window
                .buckets
                .iter()
                .rev()
                .take(cfg.required_buckets)
                .collect();
            if bs.len() < cfg.required_buckets {
                continue;
            }
            let recent = &bs[0].1;

            // ── GC advisory ────────────────────────────────────────────
            // Skip when an inflight GC is already chewing on this
            // partition; let that complete before re-advising.
            if recent.gc_inflight == 0
                && (recent.last_gc_at == 0
                    || now - recent.last_gc_at >= cfg.gc_cooldown_sec)
                && bs.iter().all(|(_, l)| l.gc_debt_bytes > cfg.gc_debt_high)
            {
                out.push(PolicyCandidate {
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
                });
            }

            // ── Compact advisory ──────────────────────────────────────
            if recent.compact_inflight == 0
                && (recent.last_compact_at == 0
                    || now - recent.last_compact_at >= cfg.compact_cooldown_sec)
                && bs.iter().all(|(_, l)| {
                    l.pending_compaction_bytes > cfg.compact_pending_high
                })
            {
                out.push(PolicyCandidate {
                    kind: POLICY_KIND_COMPACT,
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
                });
            }
        }

        out
    }
}
