//! F183 policy engine: per-partition load metrics window + split/merge
//! candidate computation. Stage 1 ships advisory only; auto-trigger is
//! gated behind feature flags in Stage 2/3.
//!
//! See `docs/superpowers/specs/2026-05-09-partition-merge-and-split-merge-policy-design.md`.

use std::collections::{HashMap, VecDeque};

use autumn_common::MetadataState;
use autumn_rpc::manager_rpc::{
    PartitionLoad, PolicyCandidate, POLICY_KIND_MERGE, POLICY_KIND_SPLIT,
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

pub const POLICY_BUCKET_SEC: i64 = 60;
pub const POLICY_WINDOW_BUCKETS: usize = 30;
pub const POLICY_REQUIRED_BUCKETS: usize = 5;
pub const POLICY_TICK_INTERVAL_SEC: i64 = 60;

#[derive(Default)]
pub struct PartitionMetricsWindow {
    pub buckets: VecDeque<(i64, PartitionLoad)>,
}

impl PartitionMetricsWindow {
    pub fn push(&mut self, ts: i64, load: PartitionLoad) {
        self.buckets.push_back((ts, load));
        while self.buckets.len() > POLICY_WINDOW_BUCKETS {
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

        // ── SPLIT pass ──────────────────────────────────────────────────────
        for (&part_id, window) in self.metrics.iter() {
            let bs: Vec<&(i64, PartitionLoad)> = window
                .buckets
                .iter()
                .rev()
                .take(POLICY_REQUIRED_BUCKETS)
                .collect();
            if bs.len() < POLICY_REQUIRED_BUCKETS {
                continue;
            }

            let last_op = args.last_op_at.get(&part_id).copied().unwrap_or(0);
            if args.now - last_op < SPLIT_COOLDOWN_SEC {
                continue;
            }

            // ALL of the last POLICY_REQUIRED_BUCKETS must show a trigger.
            let all_match = bs.iter().all(|(_, l)| {
                l.size_bytes > SPLIT_SIZE_HARD
                    || (l.req_per_sec > SPLIT_QPS_HIGH && l.size_bytes > SPLIT_SIZE_MIN)
                    || l.imm_full_per_sec > SPLIT_IMMFULL_HIGH
            });
            if !all_match {
                continue;
            }

            let recent = &bs[0].1;
            let reason = if recent.size_bytes > SPLIT_SIZE_HARD {
                format!(
                    "size_bytes>{} ({} GiB)",
                    SPLIT_SIZE_HARD,
                    recent.size_bytes / GIB
                )
            } else if recent.imm_full_per_sec > SPLIT_IMMFULL_HIGH {
                format!("imm_full_per_sec>{} sustained", SPLIT_IMMFULL_HIGH)
            } else {
                format!(
                    "req_per_sec>{} sustained AND size_bytes>{}",
                    SPLIT_QPS_HIGH, SPLIT_SIZE_MIN
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
                lw.buckets.iter().rev().take(POLICY_REQUIRED_BUCKETS).collect();
            let rbs: Vec<&(i64, PartitionLoad)> =
                rw.buckets.iter().rev().take(POLICY_REQUIRED_BUCKETS).collect();
            if lbs.len() < POLICY_REQUIRED_BUCKETS || rbs.len() < POLICY_REQUIRED_BUCKETS {
                continue;
            }

            let last_op_l = args.last_op_at.get(&left_id).copied().unwrap_or(0);
            let last_op_r = args.last_op_at.get(&right_id).copied().unwrap_or(0);
            let max_last_op = last_op_l.max(last_op_r);
            if args.now - max_last_op < MERGE_COOLDOWN_SEC {
                continue;
            }

            let all_qualify = lbs.iter().zip(rbs.iter()).all(|((_, lb), (_, rb))| {
                lb.size_bytes < MERGE_SIZE_LOW
                    && rb.size_bytes < MERGE_SIZE_LOW
                    && (lb.req_per_sec + rb.req_per_sec) < MERGE_QPS_LOW
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
                    MERGE_SIZE_LOW,
                    MERGE_QPS_LOW,
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
}
