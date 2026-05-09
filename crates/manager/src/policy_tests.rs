//! F183 policy engine unit tests.

use std::collections::HashMap;

use autumn_common::MetadataState;
use autumn_rpc::manager_rpc::{
    MgrPartitionMeta, MgrRange, PartitionLoad, POLICY_KIND_MERGE, POLICY_KIND_SPLIT,
};

use crate::policy::{
    ComputeArgs, PolicyEngine, MERGE_COOLDOWN_SEC, MERGE_QPS_LOW, MERGE_SIZE_LOW,
    POLICY_BUCKET_SEC, POLICY_REQUIRED_BUCKETS, SPLIT_COOLDOWN_SEC, SPLIT_IMMFULL_HIGH,
    SPLIT_QPS_HIGH, SPLIT_SIZE_HARD,
};

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
    let merge_cands: Vec<_> = out
        .iter()
        .filter(|c| c.kind == POLICY_KIND_MERGE)
        .collect();
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
    let merge_cands: Vec<_> = out
        .iter()
        .filter(|c| c.kind == POLICY_KIND_MERGE)
        .collect();
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
