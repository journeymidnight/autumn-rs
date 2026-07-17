//! F183 policy engine unit tests.

use std::collections::HashMap;

use autumn_common::MetadataState;
use autumn_rpc::manager_rpc::{
    MgrExtentInfo, MgrPartitionMeta, MgrRange, MgrRegionInfo, MgrStreamInfo, PartitionLoad,
    POLICY_KIND_EC, POLICY_KIND_GC, POLICY_KIND_MAJOR_COMPACT, POLICY_KIND_MERGE,
    POLICY_KIND_MINOR_COMPACT, POLICY_KIND_REBALANCE, POLICY_KIND_SPLIT,
};

use crate::policy::{
    ComputeArgs, PolicyEngine, COMPACT_COOLDOWN_SEC, COMPACT_PENDING_HIGH, EC_MIN_EXTENT_BYTES,
    GC_COOLDOWN_SEC, GC_DEBT_HIGH, MERGE_COOLDOWN_SEC, MERGE_QPS_LOW, MERGE_SIZE_LOW,
    MINOR_COMPACT_COOLDOWN_SEC, MINOR_COMPACT_PENDING_HIGH, POLICY_BUCKET_SEC,
    POLICY_REQUIRED_BUCKETS, SPLIT_COOLDOWN_SEC, SPLIT_IMMFULL_HIGH, SPLIT_QPS_HIGH,
    SPLIT_SIZE_HARD,
};

/// F202 compatibility: the old `POLICY_KIND_COMPACT` constant maps to
/// `POLICY_KIND_MAJOR_COMPACT` (same wire value 3). Existing tests use
/// the major-compact path; re-export under both names to keep them
/// compiling without churn.
#[allow(dead_code)]
const POLICY_KIND_COMPACT: u8 = POLICY_KIND_MAJOR_COMPACT;

const GIB: u64 = 1024 * 1024 * 1024;

fn fill_window(eng: &mut PolicyEngine, part_id: u64, n: usize, load: PartitionLoad, base_ts: i64) {
    for i in 0..n {
        eng.metrics
            .entry(part_id)
            .or_default()
            .push(base_ts + i as i64 * POLICY_BUCKET_SEC, load.clone());
    }
}

fn mk_part(state: &mut MetadataState, id: u64, start: &[u8], end: &[u8]) {
    state.partitions.insert(
        id,
        MgrPartitionMeta {
            part_id: id,
            log_stream: 0,
            row_stream: 0,
            meta_stream: 0,
            rg: Some(MgrRange {
                start_key: start.to_vec(),
                end_key: end.to_vec(),
            }),
        },
    );
}

#[test]
fn split_size_hard_triggers() {
    let state = MetadataState::default();
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    fill_window(
        &mut eng,
        7,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 7,
            size_bytes: SPLIT_SIZE_HARD + GIB,
            req_per_sec: 100,
            imm_full_per_sec: 0,
            p99_us: 0,
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let out = eng.compute_candidates(ComputeArgs {
        state: &state,
        last_op_at: &HashMap::new(),
        region_owners: &HashMap::new(),
        now,
    });
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].kind, POLICY_KIND_SPLIT);
    assert_eq!(out[0].primary_part_id, 7);
}

#[test]
fn split_qps_high_below_size_min_no_trigger() {
    let state = MetadataState::default();
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    fill_window(
        &mut eng,
        7,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 7,
            size_bytes: 100 * 1024 * 1024,
            req_per_sec: SPLIT_QPS_HIGH + 1000,
            imm_full_per_sec: 0,
            p99_us: 0,
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let out = eng.compute_candidates(ComputeArgs {
        state: &state,
        last_op_at: &HashMap::new(),
        region_owners: &HashMap::new(),
        now,
    });
    assert!(out.is_empty());
}

#[test]
fn split_immfull_above_threshold_triggers() {
    let state = MetadataState::default();
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    fill_window(
        &mut eng,
        7,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 7,
            size_bytes: 100 * 1024 * 1024,
            req_per_sec: 100,
            imm_full_per_sec: SPLIT_IMMFULL_HIGH + 1,
            p99_us: 0,
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let out = eng.compute_candidates(ComputeArgs {
        state: &state,
        last_op_at: &HashMap::new(),
        region_owners: &HashMap::new(),
        now,
    });
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].kind, POLICY_KIND_SPLIT);
}

#[test]
fn split_cooldown_blocks() {
    let state = MetadataState::default();
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    fill_window(
        &mut eng,
        7,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 7,
            size_bytes: SPLIT_SIZE_HARD + GIB,
            req_per_sec: 0,
            imm_full_per_sec: 0,
            p99_us: 0,
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let mut last_op = HashMap::new();
    last_op.insert(7u64, now - 60);
    let _ = SPLIT_COOLDOWN_SEC; // documented dependency
    let out = eng.compute_candidates(ComputeArgs {
        state: &state,
        last_op_at: &last_op,
        region_owners: &HashMap::new(),
        now,
    });
    assert!(out.is_empty());
}

#[test]
fn split_partial_window_no_trigger() {
    let state = MetadataState::default();
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    fill_window(
        &mut eng,
        7,
        POLICY_REQUIRED_BUCKETS - 1,
        PartitionLoad {
            part_id: 7,
            size_bytes: SPLIT_SIZE_HARD + GIB,
            req_per_sec: 0,
            imm_full_per_sec: 0,
            p99_us: 0,
            ..Default::default()
        },
        now - 4 * POLICY_BUCKET_SEC,
    );
    let out = eng.compute_candidates(ComputeArgs {
        state: &state,
        last_op_at: &HashMap::new(),
        region_owners: &HashMap::new(),
        now,
    });
    assert!(out.is_empty());
}

#[test]
fn merge_adjacent_pair_qualifying_same_ps() {
    let mut state = MetadataState::default();
    mk_part(&mut state, 1, b"a", b"m");
    mk_part(&mut state, 2, b"m", b"z");
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    let small = PartitionLoad {
        part_id: 0,
        size_bytes: 200 * 1024 * 1024,
        req_per_sec: 100,
        imm_full_per_sec: 0,
        p99_us: 0,
        ..Default::default()
    };
    fill_window(
        &mut eng,
        1,
        POLICY_REQUIRED_BUCKETS,
        small.clone(),
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    fill_window(
        &mut eng,
        2,
        POLICY_REQUIRED_BUCKETS,
        small,
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let mut owners = HashMap::new();
    owners.insert(1u64, 99u64);
    owners.insert(2u64, 99u64);
    let out = eng.compute_candidates(ComputeArgs {
        state: &state,
        last_op_at: &HashMap::new(),
        region_owners: &owners,
        now,
    });
    let merge_cands: Vec<_> = out.iter().filter(|c| c.kind == POLICY_KIND_MERGE).collect();
    assert_eq!(merge_cands.len(), 1);
    assert_eq!(merge_cands[0].primary_part_id, 1);
    assert_eq!(merge_cands[0].secondary_part_id, 2);
    assert!(merge_cands[0].same_ps);
}

#[test]
fn merge_cross_ps_marks_infeasible() {
    let mut state = MetadataState::default();
    mk_part(&mut state, 1, b"a", b"m");
    mk_part(&mut state, 2, b"m", b"z");
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    let small = PartitionLoad {
        part_id: 0,
        size_bytes: 200 * 1024 * 1024,
        req_per_sec: 100,
        imm_full_per_sec: 0,
        p99_us: 0,
        ..Default::default()
    };
    fill_window(
        &mut eng,
        1,
        POLICY_REQUIRED_BUCKETS,
        small.clone(),
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    fill_window(
        &mut eng,
        2,
        POLICY_REQUIRED_BUCKETS,
        small,
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let mut owners = HashMap::new();
    owners.insert(1u64, 11u64);
    owners.insert(2u64, 22u64);
    let out = eng.compute_candidates(ComputeArgs {
        state: &state,
        last_op_at: &HashMap::new(),
        region_owners: &owners,
        now,
    });
    let merge_cands: Vec<_> = out.iter().filter(|c| c.kind == POLICY_KIND_MERGE).collect();
    assert_eq!(merge_cands.len(), 1);
    assert!(!merge_cands[0].same_ps);
}

#[test]
fn merge_non_adjacent_no_trigger() {
    let mut state = MetadataState::default();
    mk_part(&mut state, 1, b"a", b"f");
    mk_part(&mut state, 2, b"m", b"z"); // gap [f..m)
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    let small = PartitionLoad {
        part_id: 0,
        size_bytes: 200 * 1024 * 1024,
        req_per_sec: 100,
        imm_full_per_sec: 0,
        p99_us: 0,
        ..Default::default()
    };
    fill_window(
        &mut eng,
        1,
        POLICY_REQUIRED_BUCKETS,
        small.clone(),
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    fill_window(
        &mut eng,
        2,
        POLICY_REQUIRED_BUCKETS,
        small,
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let owners = HashMap::new();
    let out = eng.compute_candidates(ComputeArgs {
        state: &state,
        last_op_at: &HashMap::new(),
        region_owners: &owners,
        now,
    });
    assert!(out.iter().all(|c| c.kind != POLICY_KIND_MERGE));
}

#[test]
fn merge_size_above_low_no_trigger() {
    let mut state = MetadataState::default();
    mk_part(&mut state, 1, b"a", b"m");
    mk_part(&mut state, 2, b"m", b"z");
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    let big = PartitionLoad {
        part_id: 0,
        size_bytes: 2 * MERGE_SIZE_LOW, // exceeds threshold
        req_per_sec: 100,
        imm_full_per_sec: 0,
        p99_us: 0,
        ..Default::default()
    };
    fill_window(
        &mut eng,
        1,
        POLICY_REQUIRED_BUCKETS,
        big.clone(),
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    fill_window(
        &mut eng,
        2,
        POLICY_REQUIRED_BUCKETS,
        big,
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let mut owners = HashMap::new();
    owners.insert(1u64, 99u64);
    owners.insert(2u64, 99u64);
    let out = eng.compute_candidates(ComputeArgs {
        state: &state,
        last_op_at: &HashMap::new(),
        region_owners: &owners,
        now,
    });
    assert!(out.iter().all(|c| c.kind != POLICY_KIND_MERGE));
}

#[test]
fn merge_qps_above_low_no_trigger() {
    let mut state = MetadataState::default();
    mk_part(&mut state, 1, b"a", b"m");
    mk_part(&mut state, 2, b"m", b"z");
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    let hot = PartitionLoad {
        part_id: 0,
        size_bytes: 100 * 1024 * 1024,
        req_per_sec: MERGE_QPS_LOW, // sum >= MERGE_QPS_LOW
        imm_full_per_sec: 0,
        p99_us: 0,
        ..Default::default()
    };
    fill_window(
        &mut eng,
        1,
        POLICY_REQUIRED_BUCKETS,
        hot.clone(),
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    fill_window(
        &mut eng,
        2,
        POLICY_REQUIRED_BUCKETS,
        hot,
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let mut owners = HashMap::new();
    owners.insert(1u64, 99u64);
    owners.insert(2u64, 99u64);
    let out = eng.compute_candidates(ComputeArgs {
        state: &state,
        last_op_at: &HashMap::new(),
        region_owners: &owners,
        now,
    });
    assert!(out.iter().all(|c| c.kind != POLICY_KIND_MERGE));
}

#[test]
fn merge_cooldown_blocks() {
    let mut state = MetadataState::default();
    mk_part(&mut state, 1, b"a", b"m");
    mk_part(&mut state, 2, b"m", b"z");
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    let small = PartitionLoad {
        part_id: 0,
        size_bytes: 200 * 1024 * 1024,
        req_per_sec: 100,
        imm_full_per_sec: 0,
        p99_us: 0,
        ..Default::default()
    };
    fill_window(
        &mut eng,
        1,
        POLICY_REQUIRED_BUCKETS,
        small.clone(),
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    fill_window(
        &mut eng,
        2,
        POLICY_REQUIRED_BUCKETS,
        small,
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let mut owners = HashMap::new();
    owners.insert(1u64, 99u64);
    owners.insert(2u64, 99u64);
    let mut last_op = HashMap::new();
    last_op.insert(1u64, now - 60); // freshly merged/split
    let _ = MERGE_COOLDOWN_SEC;
    let out = eng.compute_candidates(ComputeArgs {
        state: &state,
        last_op_at: &last_op,
        region_owners: &owners,
        now,
    });
    assert!(out.iter().all(|c| c.kind != POLICY_KIND_MERGE));
}

// ---------------------------------------------------------------------------
// F187 maintenance advisory tests
// ---------------------------------------------------------------------------

#[test]
fn gc_advisory_fires_on_sustained_debt() {
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    fill_window(
        &mut eng,
        7,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 7,
            gc_debt_bytes: GC_DEBT_HIGH + 1024,
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let out = eng.compute_maintenance_advisory(now);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].kind, POLICY_KIND_GC);
    assert_eq!(out[0].primary_part_id, 7);
    assert_eq!(out[0].secondary_part_id, 0);
    assert_eq!(out[0].size_bytes, GC_DEBT_HIGH + 1024);
}

#[test]
fn gc_advisory_skipped_when_inflight() {
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    fill_window(
        &mut eng,
        7,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 7,
            gc_debt_bytes: GC_DEBT_HIGH + 1024,
            gc_inflight: 1,
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let out = eng.compute_maintenance_advisory(now);
    assert!(out.iter().all(|c| c.kind != POLICY_KIND_GC));
}

#[test]
fn gc_advisory_respects_cooldown() {
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    // Last GC ran 10 s ago — within the 5-min cooldown.
    fill_window(
        &mut eng,
        7,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 7,
            gc_debt_bytes: GC_DEBT_HIGH + 1024,
            last_gc_at: now - 10,
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let out = eng.compute_maintenance_advisory(now);
    assert!(out.iter().all(|c| c.kind != POLICY_KIND_GC));
    let _ = GC_COOLDOWN_SEC;
}

#[test]
fn gc_advisory_no_trigger_below_threshold() {
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    fill_window(
        &mut eng,
        7,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 7,
            gc_debt_bytes: GC_DEBT_HIGH / 2,
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let out = eng.compute_maintenance_advisory(now);
    assert!(out.iter().all(|c| c.kind != POLICY_KIND_GC));
}

#[test]
fn compact_advisory_fires_on_sustained_pending() {
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    fill_window(
        &mut eng,
        9,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 9,
            pending_compaction_bytes: COMPACT_PENDING_HIGH + 1024,
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let out = eng.compute_maintenance_advisory(now);
    let cs: Vec<_> = out
        .iter()
        .filter(|c| c.kind == POLICY_KIND_COMPACT)
        .collect();
    assert_eq!(cs.len(), 1);
    assert_eq!(cs[0].primary_part_id, 9);
    assert_eq!(cs[0].size_bytes, COMPACT_PENDING_HIGH + 1024);
}

#[test]
fn compact_advisory_respects_cooldown_and_inflight() {
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    fill_window(
        &mut eng,
        9,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 9,
            pending_compaction_bytes: COMPACT_PENDING_HIGH + 1024,
            last_compact_at: now - 30,
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let out = eng.compute_maintenance_advisory(now);
    assert!(out.iter().all(|c| c.kind != POLICY_KIND_COMPACT));
    let _ = COMPACT_COOLDOWN_SEC;

    let mut eng2 = PolicyEngine::default();
    fill_window(
        &mut eng2,
        9,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 9,
            pending_compaction_bytes: COMPACT_PENDING_HIGH + 1024,
            compact_inflight: 1,
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let out2 = eng2.compute_maintenance_advisory(now);
    assert!(out2.iter().all(|c| c.kind != POLICY_KIND_COMPACT));
}

#[test]
fn maintenance_advisory_partial_window_no_trigger() {
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    // Only 2 of POLICY_REQUIRED_BUCKETS at high debt; rest is at zero.
    eng.metrics.entry(7).or_default().push(
        now - 2 * POLICY_BUCKET_SEC,
        PartitionLoad {
            part_id: 7,
            gc_debt_bytes: GC_DEBT_HIGH + 1024,
            ..Default::default()
        },
    );
    eng.metrics.entry(7).or_default().push(
        now - POLICY_BUCKET_SEC,
        PartitionLoad {
            part_id: 7,
            gc_debt_bytes: 0,
            ..Default::default()
        },
    );
    let out = eng.compute_maintenance_advisory(now);
    assert!(out.is_empty());
}

// ── F196 Stage D: hot/cold advisory ─────────────────────────────────────

#[test]
fn hot_cold_advisory_fires_on_10x_imbalance_same_ps() {
    use crate::policy::{HOT_COLD_MIN_HOT_QPS, HOT_COLD_RATIO};
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    // Two partitions on PS=42: one hot (req_per_sec = HOT_COLD_MIN_HOT_QPS * 2),
    // one cold (1 qps). Ratio > HOT_COLD_RATIO.
    fill_window(
        &mut eng,
        100,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 100,
            req_per_sec: HOT_COLD_MIN_HOT_QPS.saturating_mul(2),
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    fill_window(
        &mut eng,
        101,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 101,
            req_per_sec: 1,
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let owners: HashMap<u64, u64> = vec![(100u64, 42u64), (101, 42)].into_iter().collect();
    // First call should record a cooldown entry.
    eng.compute_hot_cold_advisory(&owners, &HashMap::new(), now);
    assert!(
        eng.last_hot_cold_at.contains_key(&42),
        "hot/cold advisory expected to fire on >{}x imbalance",
        HOT_COLD_RATIO,
    );
}

#[test]
fn hot_cold_advisory_skips_when_hottest_below_floor() {
    use crate::policy::HOT_COLD_MIN_HOT_QPS;
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    fill_window(
        &mut eng,
        100,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 100,
            req_per_sec: HOT_COLD_MIN_HOT_QPS / 2, // below the floor
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    fill_window(
        &mut eng,
        101,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 101,
            req_per_sec: 1,
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let owners: HashMap<u64, u64> = vec![(100u64, 42u64), (101, 42)].into_iter().collect();
    eng.compute_hot_cold_advisory(&owners, &HashMap::new(), now);
    assert!(
        !eng.last_hot_cold_at.contains_key(&42),
        "advisory must suppress when hottest is below the QPS floor"
    );
}

#[test]
fn hot_cold_advisory_cooldown_dedupes() {
    use crate::policy::{HOT_COLD_COOLDOWN_SEC, HOT_COLD_MIN_HOT_QPS};
    let mut eng = PolicyEngine::default();
    let t0 = 1_700_000_000;
    fill_window(
        &mut eng,
        100,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 100,
            req_per_sec: HOT_COLD_MIN_HOT_QPS.saturating_mul(2),
            ..Default::default()
        },
        t0 - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    fill_window(
        &mut eng,
        101,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 101,
            req_per_sec: 1,
            ..Default::default()
        },
        t0 - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let owners: HashMap<u64, u64> = vec![(100u64, 42u64), (101, 42)].into_iter().collect();
    eng.compute_hot_cold_advisory(&owners, &HashMap::new(), t0);
    let first = eng.last_hot_cold_at.get(&42).copied().unwrap_or(0);
    // Tick again well within cooldown — last_hot_cold_at must NOT update.
    eng.compute_hot_cold_advisory(&owners, &HashMap::new(), t0 + HOT_COLD_COOLDOWN_SEC / 2);
    let second = eng.last_hot_cold_at.get(&42).copied().unwrap_or(0);
    assert_eq!(first, second, "advisory inside cooldown must not refire");
    // Past cooldown: refires.
    eng.compute_hot_cold_advisory(&owners, &HashMap::new(), t0 + HOT_COLD_COOLDOWN_SEC + 1);
    let third = eng.last_hot_cold_at.get(&42).copied().unwrap_or(0);
    assert!(third > first, "advisory past cooldown must refire");
}

#[test]
fn hot_cold_advisory_fires_on_size_imbalance() {
    use crate::policy::{HOT_COLD_MIN_HOT_SIZE_BYTES, HOT_COLD_SIZE_RATIO};
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    // Hot partition: 2× the floor (so e.g. 50 GiB if floor is 25 GiB).
    // Cold partition: 1 byte. QPS is 0 on both — only size triggers.
    fill_window(
        &mut eng,
        200,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 200,
            size_bytes: HOT_COLD_MIN_HOT_SIZE_BYTES.saturating_mul(2),
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    fill_window(
        &mut eng,
        201,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 201,
            size_bytes: 1,
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let owners: HashMap<u64, u64> = vec![(200u64, 99u64), (201, 99)].into_iter().collect();
    eng.compute_hot_cold_advisory(&owners, &HashMap::new(), now);
    assert!(
        eng.last_hot_cold_at.contains_key(&99),
        "size-only imbalance >{}x should also trigger the advisory",
        HOT_COLD_SIZE_RATIO,
    );
}

#[test]
fn hot_cold_advisory_size_below_floor_does_not_fire() {
    use crate::policy::HOT_COLD_MIN_HOT_SIZE_BYTES;
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    // Both partitions are below the size floor; ratio is huge but
    // it's a "small partitions, who cares" cluster.
    fill_window(
        &mut eng,
        200,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 200,
            size_bytes: HOT_COLD_MIN_HOT_SIZE_BYTES / 4, // below floor
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    fill_window(
        &mut eng,
        201,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 201,
            size_bytes: 1,
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let owners: HashMap<u64, u64> = vec![(200u64, 99u64), (201, 99)].into_iter().collect();
    eng.compute_hot_cold_advisory(&owners, &HashMap::new(), now);
    assert!(
        !eng.last_hot_cold_at.contains_key(&99),
        "size advisory must suppress when hottest size is below the floor"
    );
}

#[test]
fn hot_cold_advisory_emits_policy_candidate_for_client_info() {
    use crate::policy::HOT_COLD_MIN_HOT_QPS;
    use autumn_rpc::manager_rpc::POLICY_KIND_HOT_COLD;
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    fill_window(
        &mut eng,
        300,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 300,
            req_per_sec: HOT_COLD_MIN_HOT_QPS.saturating_mul(2),
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    fill_window(
        &mut eng,
        301,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 301,
            req_per_sec: 1,
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let owners: HashMap<u64, u64> = vec![(300u64, 77u64), (301, 77)].into_iter().collect();
    let cands = eng.compute_hot_cold_advisory(&owners, &HashMap::new(), now);
    assert_eq!(
        cands.len(),
        1,
        "expected one HOT_COLD candidate, got {cands:?}"
    );
    let c = &cands[0];
    assert_eq!(c.kind, POLICY_KIND_HOT_COLD);
    assert_eq!(c.primary_part_id, 300, "primary = hottest");
    assert_eq!(c.secondary_part_id, 301, "secondary = coldest");
    assert!(
        c.reason.contains("ps_id=77"),
        "reason missing ps_id: {}",
        c.reason
    );
    assert!(
        c.reason.contains("qps_ratio="),
        "reason missing qps_ratio: {}",
        c.reason
    );
    assert!(c.same_ps, "HOT_COLD candidates are by-construction same_ps");
}

// ===========================================================================
// F202 — minor compact + EC advisory tests
// ===========================================================================

fn mk_stream(state: &mut MetadataState, sid: u64, ec: (u32, u32), extent_ids: &[u64]) {
    state.streams.insert(
        sid,
        MgrStreamInfo {
            stream_id: sid,
            extent_ids: extent_ids.to_vec(),
            ec_data_shard: ec.0,
            ec_parity_shard: ec.1,
            replicates: 3,
        },
    );
}

fn mk_extent(state: &mut MetadataState, eid: u64, sealed_length: u64, ec_converted: bool) {
    state.extents.insert(
        eid,
        MgrExtentInfo {
            extent_id: eid,
            replicates: vec![1, 3, 5],
            parity: vec![],
            eversion: 1,
            refs: 1,
            vp_table_refs: 0,
            sealed_length,
            sealed: sealed_length > 0,
            avali: if sealed_length > 0 { 1 } else { 0 },
            replicate_disks: vec![2, 4, 6],
            parity_disks: vec![],
            ec_converted,
        },
    );
}

/// F202: minor compact advisory fires when sustained
/// `minor_compact_pending_bytes` exceeds threshold across the window.
#[test]
fn minor_compact_fires_when_sustained_above_threshold() {
    let mut eng = PolicyEngine::default();
    let now = 10_000;
    fill_window(
        &mut eng,
        500,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 500,
            minor_compact_pending_bytes: MINOR_COMPACT_PENDING_HIGH * 2,
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let out = eng.compute_maintenance_advisory(now);
    assert_eq!(
        out.len(),
        1,
        "expected one MINOR_COMPACT candidate: {out:?}"
    );
    assert_eq!(out[0].kind, POLICY_KIND_MINOR_COMPACT);
    assert_eq!(out[0].primary_part_id, 500);
    assert!(
        out[0].size_bytes >= MINOR_COMPACT_PENDING_HIGH,
        "size_bytes carries the recent pending volume"
    );
}

/// F202: minor compact advisory is SUPPRESSED when the latest bucket has
/// `minor_compact_pending_bytes == 0` (i.e. `pickup_tables` had nothing
/// to do — common-sense filter "don't suggest minor compact when there's
/// no minor compact work").
#[test]
fn minor_compact_suppressed_when_latest_bucket_empty() {
    let mut eng = PolicyEngine::default();
    let now = 10_000;
    // Fill the window with a HIGH-ish historic value then a 0 most-recent.
    let base = now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC;
    fill_window(
        &mut eng,
        501,
        POLICY_REQUIRED_BUCKETS - 1,
        PartitionLoad {
            part_id: 501,
            minor_compact_pending_bytes: MINOR_COMPACT_PENDING_HIGH * 2,
            ..Default::default()
        },
        base,
    );
    // Latest bucket = 0 → filter trips at "recent.minor_compact_pending_bytes > 0".
    eng.metrics.entry(501).or_default().push(
        base + POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
        PartitionLoad {
            part_id: 501,
            minor_compact_pending_bytes: 0,
            ..Default::default()
        },
    );
    let out = eng.compute_maintenance_advisory(now);
    assert!(
        out.iter().all(|c| c.kind != POLICY_KIND_MINOR_COMPACT),
        "minor advisory must not fire when latest pending = 0: {out:?}"
    );
}

/// F202: minor compact respects its own cooldown — distinct from major.
#[test]
fn minor_compact_respects_cooldown() {
    let mut eng = PolicyEngine::default();
    let now = 10_000;
    fill_window(
        &mut eng,
        502,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 502,
            minor_compact_pending_bytes: MINOR_COMPACT_PENDING_HIGH * 2,
            // Just-completed compact → cooldown active.
            last_compact_at: now - MINOR_COMPACT_COOLDOWN_SEC + 10,
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let out = eng.compute_maintenance_advisory(now);
    assert!(
        out.iter().all(|c| c.kind != POLICY_KIND_MINOR_COMPACT),
        "in cooldown — must not fire: {out:?}"
    );
}

/// F202: EC advisory fires for sealed-unconverted extents ≥ threshold.
#[test]
fn ec_advisory_fires_for_large_sealed_unconverted_extent() {
    let mut state = MetadataState::default();
    mk_stream(&mut state, 100, (3, 1), &[1001]);
    mk_extent(&mut state, 1001, EC_MIN_EXTENT_BYTES * 2, false);
    let eng = PolicyEngine::default();
    let out = eng.compute_ec_advisory(&state, 999);
    assert_eq!(out.len(), 1, "expected one EC candidate: {out:?}");
    let c = &out[0];
    assert_eq!(c.kind, POLICY_KIND_EC);
    assert_eq!(c.primary_part_id, 0, "EC is per-extent, not per-partition");
    assert_eq!(c.secondary_part_id, 1001, "secondary carries extent_id");
    assert_eq!(c.size_bytes, EC_MIN_EXTENT_BYTES * 2);
}

/// F202: EC advisory common-sense filter — extents below
/// `ec_min_extent_bytes` are NOT surfaced (encode overhead > savings).
#[test]
fn ec_advisory_suppresses_small_extents() {
    let mut state = MetadataState::default();
    mk_stream(&mut state, 101, (3, 1), &[1002, 1003]);
    // Below threshold — should not be advised.
    mk_extent(&mut state, 1002, EC_MIN_EXTENT_BYTES / 2, false);
    // Above threshold — should be advised.
    mk_extent(&mut state, 1003, EC_MIN_EXTENT_BYTES + 1, false);
    let eng = PolicyEngine::default();
    let out = eng.compute_ec_advisory(&state, 0);
    assert_eq!(out.len(), 1, "small extent filtered out: {out:?}");
    assert_eq!(out[0].secondary_part_id, 1003);
}

/// F202: EC advisory skips already-converted extents.
#[test]
fn ec_advisory_skips_converted() {
    let mut state = MetadataState::default();
    mk_stream(&mut state, 102, (3, 1), &[1004]);
    mk_extent(&mut state, 1004, EC_MIN_EXTENT_BYTES * 4, true);
    let eng = PolicyEngine::default();
    let out = eng.compute_ec_advisory(&state, 0);
    assert!(
        out.is_empty(),
        "ec_converted=true should be skipped: {out:?}"
    );
}

/// F202: EC advisory skips replication-only streams (no EC policy
/// attached → nothing to convert toward).
#[test]
fn ec_advisory_skips_non_ec_streams() {
    let mut state = MetadataState::default();
    // ec=(0,0) → replication-only.
    mk_stream(&mut state, 103, (0, 0), &[1005]);
    mk_extent(&mut state, 1005, EC_MIN_EXTENT_BYTES * 4, false);
    let eng = PolicyEngine::default();
    let out = eng.compute_ec_advisory(&state, 0);
    assert!(
        out.is_empty(),
        "non-EC stream should not be advised: {out:?}"
    );
}

/// F202: EC advisory skips sealed_length=0 extents (open OR
/// sealed-at-zero — both are GC empty-extent territory, not EC).
#[test]
fn ec_advisory_skips_empty_extents() {
    let mut state = MetadataState::default();
    mk_stream(&mut state, 104, (3, 1), &[1006]);
    mk_extent(&mut state, 1006, 0, false);
    let eng = PolicyEngine::default();
    let out = eng.compute_ec_advisory(&state, 0);
    assert!(out.is_empty(), "sealed_length=0 should be skipped: {out:?}");
}

// ── F-REGION-REBALANCE Phase B: compute_rebalance_advisory ───────────────────

fn rebal_state(ps_ids: &[u64], assignments: &[(u64, u64)]) -> MetadataState {
    let mut state = MetadataState::default();
    for &id in ps_ids {
        state.ps_nodes.insert(id, format!("ps{id}:9001"));
    }
    for &(part_id, ps_id) in assignments {
        state.regions.insert(
            part_id,
            MgrRegionInfo {
                rg: Some(MgrRange { start_key: vec![], end_key: vec![] }),
                part_id,
                ps_id,
                log_stream: part_id,
                row_stream: part_id + 1000,
                meta_stream: part_id + 2000,
                region_epoch: 1,
            },
        );
    }
    state
}

#[test]
fn rebalance_advisory_fires_on_concentration() {
    // 32 partitions all on ps 3, ps 1/2 idle — gap 32 >> threshold.
    let assignments: Vec<(u64, u64)> = (100..132).map(|p| (p, 3)).collect();
    let state = rebal_state(&[1, 2, 3], &assignments);
    let mut eng = PolicyEngine::default();
    let out = eng.compute_rebalance_advisory(&state, 1000);
    assert_eq!(out.len(), 1, "expected one cluster-level candidate");
    assert_eq!(out[0].kind, POLICY_KIND_REBALANCE);
    assert_eq!(out[0].primary_part_id, 0); // cluster-scoped
    assert_eq!(out[0].secondary_part_id, 0);
}

#[test]
fn rebalance_advisory_silent_when_balanced() {
    // 11/11/10 — gap 1 <= threshold(2), no advisory.
    let mut assignments = Vec::new();
    for (i, p) in (100..132).enumerate() {
        assignments.push((p, [1u64, 2, 3][i % 3]));
    }
    let state = rebal_state(&[1, 2, 3], &assignments);
    let mut eng = PolicyEngine::default();
    assert!(eng.compute_rebalance_advisory(&state, 1000).is_empty());
}

#[test]
fn rebalance_advisory_respects_cooldown() {
    let assignments: Vec<(u64, u64)> = (100..132).map(|p| (p, 3)).collect();
    let state = rebal_state(&[1, 2, 3], &assignments);
    let mut eng = PolicyEngine::default();
    // First fires and stamps last_rebalance_at = 1000.
    assert_eq!(eng.compute_rebalance_advisory(&state, 1000).len(), 1);
    // Within the cooldown window → suppressed.
    assert!(eng.compute_rebalance_advisory(&state, 1000 + 10).is_empty());
    // After the cooldown → fires again.
    let later = 1000 + eng.config.rebalance_cooldown_sec + 1;
    assert_eq!(eng.compute_rebalance_advisory(&state, later).len(), 1);
}

#[test]
fn rebalance_advisory_disabled_when_threshold_zero() {
    let assignments: Vec<(u64, u64)> = (100..132).map(|p| (p, 3)).collect();
    let state = rebal_state(&[1, 2, 3], &assignments);
    let mut eng = PolicyEngine::default();
    let mut cfg = eng.config.clone();
    cfg.rebalance_gap_threshold = 0;
    eng.set_config(cfg);
    assert!(eng.compute_rebalance_advisory(&state, 1000).is_empty());
}

// ===========================================================================
// F-POLICY-SIZE-EST-LIVE — est_live口径 tests (design doc §3.3 D3)
// ===========================================================================

use crate::policy::{effective_size_bytes, est_live_bytes, partition_sealed_sums};

fn mk_part_streams(
    state: &mut MetadataState,
    id: u64,
    start: &[u8],
    end: &[u8],
    log: u64,
    row: u64,
    meta: u64,
) {
    state.partitions.insert(
        id,
        MgrPartitionMeta {
            part_id: id,
            log_stream: log,
            row_stream: row,
            meta_stream: meta,
            rg: Some(MgrRange {
                start_key: start.to_vec(),
                end_key: end.to_vec(),
            }),
        },
    );
}

/// Pure-fn arithmetic: additions, per-component subtraction, and the
/// saturating clamp (stale debts > sealed + open_tail must yield 0, never
/// wrap), plus the `max` fallback to the LSM-resident metric.
#[test]
fn est_live_pure_fn_arithmetic_and_saturation() {
    let l = PartitionLoad {
        open_tail_bytes: 2 * GIB,
        gc_debt_bytes: GIB,
        open_tail_dead_bytes: GIB / 2,
        size_bytes: 700 * 1024 * 1024,
        ..Default::default()
    };
    // 10 + 2 − 1 − 0.5 = 10.5 GiB
    assert_eq!(est_live_bytes(10 * GIB, &l), 10 * GIB + GIB / 2);
    // effective = max(lsm, est_live)
    assert_eq!(effective_size_bytes(10 * GIB, &l), 10 * GIB + GIB / 2);

    // Degenerate: debts exceed sealed + open_tail → clamp to 0, and the
    // effective size falls back to the LSM-resident metric.
    let stale = PartitionLoad {
        open_tail_bytes: 0,
        gc_debt_bytes: 100 * GIB,
        open_tail_dead_bytes: u64::MAX,
        size_bytes: 200 * 1024 * 1024,
        ..Default::default()
    };
    assert_eq!(est_live_bytes(GIB, &stale), 0);
    assert_eq!(effective_size_bytes(GIB, &stale), 200 * 1024 * 1024);

    // Component missing (all-zero PS gauges — e.g. probe never ran):
    // est_live degrades to the sealed sum alone.
    let zeroed = PartitionLoad::default();
    assert_eq!(est_live_bytes(7 * GIB, &zeroed), 7 * GIB);

    // Overflow side: sealed + open_tail saturates instead of wrapping.
    let huge = PartitionLoad {
        open_tail_bytes: u64::MAX,
        ..Default::default()
    };
    assert_eq!(est_live_bytes(u64::MAX, &huge), u64::MAX);
}

/// The sealed sum dedups extents shared across the SAME partition's three
/// streams (each extent counted once), skips extent ids with no
/// `MgrExtentInfo`, and yields 0 for a partition with no stream state.
#[test]
fn partition_sealed_sums_dedups_and_degrades() {
    let mut state = MetadataState::default();
    mk_part_streams(&mut state, 7, b"a", b"m", 100, 101, 102);
    mk_extent(&mut state, 1, 4 * GIB, false);
    mk_extent(&mut state, 2, 2 * GIB, false);
    // Extent 1 appears in BOTH log and row streams (CoW/splice shape) —
    // counted once. Extent 999 has no MgrExtentInfo — skipped.
    mk_stream(&mut state, 100, (0, 0), &[1, 2]);
    mk_stream(&mut state, 101, (0, 0), &[1, 999]);
    mk_stream(&mut state, 102, (0, 0), &[]);
    // Partition 8 exists but references unknown streams → sum 0.
    mk_part_streams(&mut state, 8, b"m", b"z", 200, 201, 202);

    let sums = partition_sealed_sums(&state);
    assert_eq!(sums.get(&7).copied(), Some(6 * GIB));
    assert_eq!(sums.get(&8).copied(), Some(0));
}

/// The headline fix: a VP-heavy partition (LSM-resident 741 MB, ~55 GiB of
/// sealed stream bytes) reaches SPLIT_SIZE_HARD under the new口径 — the old
/// `size_bytes`-only predicate could never fire here.
#[test]
fn est_live_vp_load_split_fires_despite_small_lsm() {
    let mut state = MetadataState::default();
    mk_part_streams(&mut state, 17, b"", b"m", 100, 101, 102);
    mk_extent(&mut state, 1, 30 * GIB, false);
    mk_extent(&mut state, 2, 25 * GIB, false);
    mk_stream(&mut state, 100, (0, 0), &[1]);
    mk_stream(&mut state, 101, (0, 0), &[2]);
    mk_stream(&mut state, 102, (0, 0), &[]);

    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    fill_window(
        &mut eng,
        17,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 17,
            size_bytes: 741 * 1024 * 1024, // LSM-resident: pointers only
            req_per_sec: 100,              // low QPS — size is the only trigger
            ..Default::default()
        },
        now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC,
    );
    let out = eng.compute_candidates(ComputeArgs {
        state: &state,
        last_op_at: &HashMap::new(),
        region_owners: &HashMap::new(),
        now,
    });
    assert_eq!(out.len(), 1, "expected exactly the split candidate: {out:?}");
    assert_eq!(out[0].kind, POLICY_KIND_SPLIT);
    assert_eq!(out[0].primary_part_id, 17);
    // The SIZE column (candidate.size_bytes) carries the new口径.
    assert_eq!(out[0].size_bytes, 55 * GIB);
    assert!(
        out[0].reason.contains("est_live"),
        "reason should expose est_live for DryRun calibration: {}",
        out[0].reason
    );
}

/// gc_debt / open_tail_dead are subtracted: dead bytes must not push a
/// partition over SPLIT_SIZE_HARD. Part 7 (52 GiB gross − 3 GiB debt =
/// 49 GiB) stays silent; part 8 (same gross, no debt) fires.
#[test]
fn est_live_debt_subtraction_suppresses_split() {
    let mut state = MetadataState::default();
    for (pid, log_sid, eid) in [(7u64, 100u64, 1u64), (8, 200, 2)] {
        let (start, end) = if pid == 7 {
            (b"a".as_ref(), b"m".as_ref())
        } else {
            (b"m".as_ref(), b"z".as_ref())
        };
        mk_part_streams(&mut state, pid, start, end, log_sid, log_sid + 1, log_sid + 2);
        mk_extent(&mut state, eid, 49 * GIB, false);
        mk_stream(&mut state, log_sid, (0, 0), &[eid]);
        mk_stream(&mut state, log_sid + 1, (0, 0), &[]);
        mk_stream(&mut state, log_sid + 2, (0, 0), &[]);
    }

    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    let base = now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC;
    fill_window(
        &mut eng,
        7,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 7,
            size_bytes: 500 * 1024 * 1024,
            open_tail_bytes: 3 * GIB,     // gross 52 GiB > hard...
            gc_debt_bytes: 2 * GIB,       // ...but 3 GiB of it is dead
            open_tail_dead_bytes: GIB,    // → est_live 49 GiB < 50 GiB
            req_per_sec: 100,
            ..Default::default()
        },
        base,
    );
    fill_window(
        &mut eng,
        8,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 8,
            size_bytes: 500 * 1024 * 1024,
            open_tail_bytes: 3 * GIB, // gross 52 GiB, no debt → fires
            req_per_sec: 100,
            ..Default::default()
        },
        base,
    );
    let out = eng.compute_candidates(ComputeArgs {
        state: &state,
        last_op_at: &HashMap::new(),
        region_owners: &HashMap::new(),
        now,
    });
    assert_eq!(out.len(), 1, "only the debt-free partition splits: {out:?}");
    assert_eq!(out[0].primary_part_id, 8);
    assert_eq!(out[0].size_bytes, 52 * GIB);
}

/// The direction fix from the live incident: a partition whose LSM-resident
/// size is tiny but which carries tens of GiB of VP bytes must NOT surface
/// as a merge candidate.
#[test]
fn est_live_vp_partition_not_a_merge_candidate() {
    let mut state = MetadataState::default();
    mk_part_streams(&mut state, 1, b"a", b"m", 100, 101, 102);
    mk_part_streams(&mut state, 2, b"m", b"z", 200, 201, 202); // adjacent
    mk_extent(&mut state, 1, 45 * GIB, false); // part 1 carries 45 GiB
    mk_stream(&mut state, 100, (0, 0), &[1]);
    mk_stream(&mut state, 101, (0, 0), &[]);
    mk_stream(&mut state, 102, (0, 0), &[]);

    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    let base = now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC;
    let cold = PartitionLoad {
        size_bytes: 200 * 1024 * 1024, // both look tiny to the old口径
        req_per_sec: 1,
        ..Default::default()
    };
    fill_window(&mut eng, 1, POLICY_REQUIRED_BUCKETS, cold.clone(), base);
    fill_window(&mut eng, 2, POLICY_REQUIRED_BUCKETS, cold, base);
    let owners: HashMap<u64, u64> = vec![(1u64, 9u64), (2, 9)].into_iter().collect();
    let out = eng.compute_candidates(ComputeArgs {
        state: &state,
        last_op_at: &HashMap::new(),
        region_owners: &owners,
        now,
    });
    assert!(
        out.is_empty(),
        "a 45 GiB-carrying partition must not be a merge candidate: {out:?}"
    );
}

/// Degradation: stale debts exceeding the sealed sum clamp est_live to 0 —
/// the predicate then falls back to the old LSM-resident metric via the
/// max, so a genuinely-cold pair still merges (no panic, no wraparound,
/// no false veto).
#[test]
fn est_live_underflow_clamps_and_cold_pair_still_merges() {
    let mut state = MetadataState::default();
    mk_part_streams(&mut state, 1, b"a", b"m", 100, 101, 102);
    mk_part_streams(&mut state, 2, b"m", b"z", 200, 201, 202);
    mk_extent(&mut state, 1, 100 * 1024 * 1024, false);
    mk_stream(&mut state, 100, (0, 0), &[1]);

    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    let base = now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC;
    let cold = PartitionLoad {
        size_bytes: 200 * 1024 * 1024,
        gc_debt_bytes: 5 * GIB, // stale over-report ≫ sealed sum
        req_per_sec: 1,
        ..Default::default()
    };
    fill_window(&mut eng, 1, POLICY_REQUIRED_BUCKETS, cold.clone(), base);
    fill_window(&mut eng, 2, POLICY_REQUIRED_BUCKETS, cold, base);
    let owners: HashMap<u64, u64> = vec![(1u64, 9u64), (2, 9)].into_iter().collect();
    let out = eng.compute_candidates(ComputeArgs {
        state: &state,
        last_op_at: &HashMap::new(),
        region_owners: &owners,
        now,
    });
    assert_eq!(out.len(), 1, "cold pair should still merge: {out:?}");
    assert_eq!(out[0].kind, POLICY_KIND_MERGE);
}

/// The hot/cold size dimension consumes the same口径: a VP-heavy partition
/// (tiny LSM-resident, 60 GiB sealed) registers as size-hot.
#[test]
fn hot_cold_size_dimension_sees_est_live() {
    use autumn_rpc::manager_rpc::POLICY_KIND_HOT_COLD;
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    let base = now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC;
    fill_window(
        &mut eng,
        300,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 300,
            size_bytes: 1024 * 1024, // 1 MiB LSM-resident
            ..Default::default()
        },
        base,
    );
    fill_window(
        &mut eng,
        301,
        POLICY_REQUIRED_BUCKETS,
        PartitionLoad {
            part_id: 301,
            size_bytes: 1,
            ..Default::default()
        },
        base,
    );
    let owners: HashMap<u64, u64> = vec![(300u64, 55u64), (301, 55)].into_iter().collect();
    let sealed: HashMap<u64, u64> = vec![(300u64, 60 * GIB)].into_iter().collect();
    let cands = eng.compute_hot_cold_advisory(&owners, &sealed, now);
    assert_eq!(cands.len(), 1, "size-hot via est_live expected: {cands:?}");
    assert_eq!(cands[0].kind, POLICY_KIND_HOT_COLD);
    assert_eq!(cands[0].primary_part_id, 300);
    assert_eq!(cands[0].size_bytes, 60 * GIB, "SIZE column carries the new口径");
}
