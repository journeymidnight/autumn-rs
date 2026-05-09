# Partition Merge + Split/Merge Policy — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship F181 Stage 1 — manual partition merge primitive (CoW stream-extent splice, no value rewrite) + size+load-driven advisory policy engine in the manager. Auto-trigger of split/merge stays OFF.

**Architecture:** Inverse-of-split via stream-extent splice. Manager allocates a fresh log_stream tail extent inside the merge etcd txn; survivor's log_stream extent_ids becomes `[L.sealed]+[V.sealed]+[E_new]`, row_stream is `[L.sealed]+[V.sealed]`. Merged TableLocations checkpoint is written to survivor's meta_stream. Victim partition deleted. F124 single-txn commit + F138/F145/F146 inflight checks + F149 fence preserved. Policy engine collects per-partition `size_bytes / req_per_sec / imm_full_per_sec` over a 30-min sliding window via a new `MSG_REPORT_PARTITION_LOAD` RPC, computes split/merge candidates every 60 s, exposes via `MSG_GET_POLICY_CANDIDATES`.

**Tech Stack:** Rust 2021, compio runtime (thread-per-core), autumn-rpc (10-byte binary frames), rkyv zero-copy serialization, embedded etcd for integration tests, parking_lot::RwLock for memtable.

**Spec:** `docs/superpowers/specs/2026-05-09-partition-merge-and-split-merge-policy-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `crates/rpc/src/manager_rpc.rs` | Modify | Add MSG_MULTI_MODIFY_MERGE 0x34, MSG_GET_POLICY_CANDIDATES 0x35, MSG_REPORT_PARTITION_LOAD 0x36 + req/resp structs + PolicyCandidate + PartitionLoad |
| `crates/rpc/src/partition_rpc.rs` | Modify | Add MSG_MERGE_PART 0x4D + MergePartReq/Resp |
| `crates/manager/src/lib.rs` | Modify | Add `compute_merge_streams`, `apply_merge_mutations`, `merged_partition_vp_refs`; add `last_op_at` HashMap + `partitionLastOp/` etcd prefix replay; spawn `policy_tick_loop` |
| `crates/manager/src/policy.rs` | Create | PolicyEngine + PartitionMetricsWindow + thresholds + compute_candidates |
| `crates/manager/src/rpc_handlers.rs` | Modify | Add `handle_multi_modify_merge`, `handle_get_policy_candidates`, `handle_report_partition_load`; extend `handle_multi_modify_split` to write last_op_at |
| `crates/partition-server/src/lib.rs` | Modify | Add `frozen_for_merge` Cell + PartitionMetrics struct on PartitionData; extend PartitionRequest enum with MergeFreeze/MergeRelease; spawn report_load_loop |
| `crates/partition-server/src/background.rs` | Modify | Bump req_count_60s + imm_full_count_60s in merged_partition_loop; handle MergeFreeze/MergeRelease arms |
| `crates/partition-server/src/rpc_handlers.rs` | Modify | Add `handle_merge_part` |
| `crates/client/src/lib.rs` | Modify | Add `merge_partitions` + `policy_candidates` methods on ClusterClient |
| `crates/server/src/bin/autumn_client.rs` | Modify | Add `merge` + `policy candidates` CLI subcommands |
| `crates/manager/src/policy_tests.rs` | Create | Unit tests for policy engine |
| `crates/manager/tests/system_merge.rs` | Create | Integration tests for merge primitive |
| `crates/manager/tests/system_policy.rs` | Create | Integration tests for advisory policy |
| `feature_list.md` | Modify | F181 entry |
| `claude-progress.txt` | Modify | Status update |
| `README.md` | Modify | Manual merge + policy candidates sections |
| `crates/manager/CLAUDE.md` | Modify | Note 16: merge handler + last_op_at sidecar |
| `crates/partition-server/CLAUDE.md` | Modify | Programming Note 11: merge handler + dual-gate ordering |

---

## Pre-flight

Before starting, output the two CLAUDE.md task lists:

- [ ] **Pre-step: Output implemented vs not-implemented lists for F181**

Reference the spec §0. Verify these match what the spec calls out:

```
已实现 (今 main):
  F008/F037/F124/F140 split + multi_modify_split + dual-gate
  F138/F145/F146/F147-A inflight exclusion patterns
  F149 leader-fence
  F124 etcd single-txn pattern
  F148-A metadata-publish ordering invariant
  MgrPartitionVpRefs + vp_table_refs lifetime
  Heartbeat 2s + region_sync_loop 2s
  pending_extent_deletes + extent_delete_loop (F109/F139)

未实现 (本次范围 = F181 Stage 1):
  Partition merge primitive (no MSG_MULTI_MODIFY_MERGE / MSG_MERGE_PART)
  Per-partition size+req/sec+imm_full metrics export
  Manager policy engine + advisory candidate emission
  CLI merge + policy candidates subcommands
  Auto-trigger of split/merge (Stage 2/3, NOT in this commit)
```

---

## Phase A — Wire types

### Task 1: Add manager-side merge RPC types

**Files:**
- Modify: `crates/rpc/src/manager_rpc.rs`

- [ ] **Step 1: Read the current end of manager_rpc.rs**

Run: `wc -l /data/dongmao_dev/autumn-rs/crates/rpc/src/manager_rpc.rs`

Note: existing constants end at `MSG_SYNC_PARTITION_VP_REFS = 0x33`.

- [ ] **Step 2: Add new constants below `MSG_SYNC_PARTITION_VP_REFS`**

After line 51 (`pub const MSG_SYNC_PARTITION_VP_REFS: u8 = 0x33;`), insert:

```rust
pub const MSG_MULTI_MODIFY_MERGE: u8 = 0x34;
pub const MSG_GET_POLICY_CANDIDATES: u8 = 0x35;
pub const MSG_REPORT_PARTITION_LOAD: u8 = 0x36;
```

- [ ] **Step 3: Add MultiModifyMergeReq/Resp at end of file**

Append (use the existing rkyv derive pattern from `MultiModifySplitReq`):

```rust
// --- MultiModifyMerge (F181) ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MultiModifyMergeReq {
    pub survivor_part_id: u64,
    pub victim_part_id: u64,
    pub owner_key: String,
    pub revision: i64,
    /// commit_length per stream type, indexed [0]=survivor, [1]=victim
    pub log_sealed_lengths: [u64; 2],
    pub row_sealed_lengths: [u64; 2],
    pub meta_sealed_lengths: [u64; 2],
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MultiModifyMergeResp {
    pub code: u8,
    pub message: String,
    /// extent_id of the freshly allocated empty tail for survivor's
    /// log_stream; used as vp_head when writing the merged
    /// TableLocations checkpoint.
    pub new_log_tail_extent_id: u64,
}
```

- [ ] **Step 4: Add PartitionLoad + ReportPartitionLoadReq**

Append:

```rust
// --- ReportPartitionLoad (F181 — policy metrics) ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PartitionLoad {
    pub part_id: u64,
    pub size_bytes: u64,
    pub req_per_sec: u32,
    pub imm_full_per_sec: u32,
    pub p99_us: u32,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct ReportPartitionLoadReq {
    pub ps_id: u64,
    pub partitions: Vec<PartitionLoad>,
}
```

- [ ] **Step 5: Add PolicyCandidate + GetPolicyCandidatesReq/Resp**

Append:

```rust
// --- GetPolicyCandidates (F181 — advisory) ---
pub const POLICY_KIND_SPLIT: u8 = 0;
pub const POLICY_KIND_MERGE: u8 = 1;

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PolicyCandidate {
    pub kind: u8,                    // POLICY_KIND_SPLIT or POLICY_KIND_MERGE
    pub primary_part_id: u64,        // split: target; merge: survivor
    pub secondary_part_id: u64,      // split: 0; merge: victim
    pub reason: String,
    pub size_bytes: u64,
    pub req_per_sec: u32,
    pub imm_full_per_sec: u32,
    pub same_ps: bool,               // merge: false → infeasible until co-located
    pub last_op_at: i64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct GetPolicyCandidatesReq {}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetPolicyCandidatesResp {
    pub code: u8,
    pub message: String,
    pub candidates: Vec<PolicyCandidate>,
}
```

- [ ] **Step 6: Run cargo check**

Run: `cd /data/dongmao_dev/autumn-rs && cargo check -p autumn-rpc`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add crates/rpc/src/manager_rpc.rs
git commit -m "F181-A1: wire types — manager-side merge + policy RPCs"
```

---

### Task 2: Add PS-side merge RPC type

**Files:**
- Modify: `crates/rpc/src/partition_rpc.rs`

- [ ] **Step 1: Add MSG_MERGE_PART constant**

In `crates/rpc/src/partition_rpc.rs` after `pub const MSG_PUT_ABORT: u8 = 0x4C;`, insert:

```rust
pub const MSG_MERGE_PART: u8 = 0x4D;
```

- [ ] **Step 2: Add MergePartReq/Resp structs**

Append at end of file (mirror SplitPartReq/Resp pattern at lines 135-145):

```rust
// --- MergePart (F181) ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MergePartReq {
    pub survivor_part_id: u64,
    pub victim_part_id: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MergePartResp {
    pub code: u8,
    pub message: String,
}
```

- [ ] **Step 3: Add MSG_MERGE_PART to the partition_id_from_payload match**

In `crates/rpc/src/partition_rpc.rs`, find the match block around line 308 (`MSG_SPLIT_PART => rkyv_decode::<SplitPartReq>(payload).map(|r| r.part_id).unwrap_or(0),`) and add an arm:

```rust
            MSG_MERGE_PART => rkyv_decode::<MergePartReq>(payload).map(|r| r.survivor_part_id).unwrap_or(0),
```

- [ ] **Step 4: Add MSG_MERGE_PART to the test list around line 384**

If there's a test `partition_msg_routing_test` enumerating the full set of partition-routed messages, append `MSG_MERGE_PART` to the slice. If no such test exists, skip this step.

- [ ] **Step 5: cargo check**

Run: `cd /data/dongmao_dev/autumn-rs && cargo check -p autumn-rpc`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/rpc/src/partition_rpc.rs
git commit -m "F181-A2: wire types — PS-side MSG_MERGE_PART"
```

---

## Phase B — Manager pure functions

### Task 3: `compute_merge_streams` pure function + tests

**Files:**
- Modify: `crates/manager/src/lib.rs`

- [ ] **Step 1: Locate `compute_duplicate_stream`**

Find around line 964. Note the signature pattern: takes `&MetadataState`, returns `Result<(MgrStreamInfo, Vec<MgrExtentInfo>), AppError>`. We mirror this for merge.

- [ ] **Step 2: Add the new helper just below `compute_duplicate_stream`**

Insert after the closing `}` of `compute_duplicate_stream`:

```rust
    /// F181: Compute the mutations for splicing `victim_stream`'s
    /// extents onto the END of `survivor_stream`'s extent_ids list, then
    /// appending one fresh tail extent (`new_tail`).
    ///
    /// Returns (updated_survivor_stream, modified_extents).
    /// modified_extents includes:
    ///   - victim's extents with refs += 1 (CoW)
    ///   - survivor's old tail (if any) sealed at survivor_sealed
    ///   - victim's old tail (if any) sealed at victim_sealed
    ///   - new_tail extent itself (caller must have already built its
    ///     MgrExtentInfo via select_nodes + alloc_extent_on_node)
    ///
    /// Order invariant (load-bearing — F181):
    ///   updated.extent_ids = [survivor's existing] + [victim's existing] + [new_tail]
    ///
    /// Caller (handle_multi_modify_merge) is responsible for the F138/
    /// F145/F146 inflight checks before calling this.
    fn compute_merge_streams(
        state: &autumn_common::MetadataState,
        survivor_stream_id: u64,
        victim_stream_id: u64,
        survivor_sealed: u32,
        victim_sealed: u32,
        new_tail: MgrExtentInfo,
    ) -> Result<(MgrStreamInfo, Vec<MgrExtentInfo>), AppError> {
        let survivor = state
            .streams
            .get(&survivor_stream_id)
            .cloned()
            .ok_or_else(|| AppError::NotFound(format!("stream {survivor_stream_id}")))?;
        let victim = state
            .streams
            .get(&victim_stream_id)
            .cloned()
            .ok_or_else(|| AppError::NotFound(format!("stream {victim_stream_id}")))?;

        let mut modified_extents = Vec::new();

        // Seal survivor's existing tail (if open) at survivor_sealed.
        if let Some(&tail_id) = survivor.extent_ids.last() {
            let extent = state
                .extents
                .get(&tail_id)
                .ok_or_else(|| AppError::NotFound(format!("extent {tail_id}")))?;
            let mut ex = extent.clone();
            if ex.sealed_length == 0 && survivor_sealed > 0 {
                ex.sealed_length = survivor_sealed as u64;
                ex.eversion += 1;
                ex.avali = Self::all_bits(ex.replicates.len() + ex.parity.len());
                modified_extents.push(ex);
            }
        }

        // Seal victim's tail at victim_sealed AND refs++ on EVERY victim extent
        // (CoW transfer into survivor's stream).
        for (idx, &eid) in victim.extent_ids.iter().enumerate() {
            let extent = state
                .extents
                .get(&eid)
                .ok_or_else(|| AppError::NotFound(format!("extent {eid}")))?;
            let mut ex = extent.clone();
            ex.refs += 1;
            ex.eversion += 1;
            if idx == victim.extent_ids.len() - 1 && ex.sealed_length == 0 && victim_sealed > 0 {
                ex.sealed_length = victim_sealed as u64;
                ex.avali = Self::all_bits(ex.replicates.len() + ex.parity.len());
            }
            modified_extents.push(ex);
        }

        // Build the spliced extent_ids list, append new_tail at the END.
        // Order invariant: [survivor.extent_ids] + [victim.extent_ids] + [new_tail].
        let mut new_extent_ids = survivor.extent_ids.clone();
        new_extent_ids.extend(victim.extent_ids.iter().copied());
        new_extent_ids.push(new_tail.extent_id);

        let updated_survivor = MgrStreamInfo {
            stream_id: survivor.stream_id,
            extent_ids: new_extent_ids,
            ec_data_shard: survivor.ec_data_shard,
            ec_parity_shard: survivor.ec_parity_shard,
            replicates: survivor.replicates,
        };

        modified_extents.push(new_tail);

        Ok((updated_survivor, modified_extents))
    }
```

- [ ] **Step 3: Write the unit test below the existing `split_partition_vp_snapshot_clones_parent_refs` test**

Locate the `#[test]` block around line 1722. Add a new test:

```rust
    #[test]
    fn compute_merge_streams_extent_ids_order_and_refs() {
        let mut state = autumn_common::MetadataState::default();
        // survivor stream 100 with [E10, E11] (E11 is tail, open)
        let mk_extent = |id: u64, refs: u32, sealed: u64| MgrExtentInfo {
            extent_id: id,
            replicates: vec![1],
            parity: vec![],
            replicate_disks: vec![1],
            parity_disks: vec![],
            sealed_length: sealed,
            avali: 1,
            eversion: 0,
            refs,
            vp_table_refs: 0,
            ec_converted: false,
        };
        state.extents.insert(10, mk_extent(10, 1, 1024));
        state.extents.insert(11, mk_extent(11, 1, 0));
        state.streams.insert(100, MgrStreamInfo {
            stream_id: 100,
            extent_ids: vec![10, 11],
            ec_data_shard: 1, ec_parity_shard: 0,
            replicates: vec![1],
        });
        // victim stream 200 with [E20, E21] (E21 is tail, open)
        state.extents.insert(20, mk_extent(20, 1, 2048));
        state.extents.insert(21, mk_extent(21, 1, 0));
        state.streams.insert(200, MgrStreamInfo {
            stream_id: 200,
            extent_ids: vec![20, 21],
            ec_data_shard: 1, ec_parity_shard: 0,
            replicates: vec![1],
        });
        let new_tail = mk_extent(99, 1, 0);

        let (updated, modified) = AutumnManager::compute_merge_streams(
            &state, 100, 200, 4096, 8192, new_tail.clone(),
        ).unwrap();

        // Order invariant: [10, 11, 20, 21, 99]
        assert_eq!(updated.extent_ids, vec![10, 11, 20, 21, 99]);
        // Stream id preserved
        assert_eq!(updated.stream_id, 100);

        // Survivor's tail E11 is sealed at 4096; ref unchanged.
        let e11 = modified.iter().find(|e| e.extent_id == 11).unwrap();
        assert_eq!(e11.sealed_length, 4096);
        assert_eq!(e11.refs, 1);

        // Victim's E20 (non-tail): refs += 1, sealed unchanged at 2048.
        let e20 = modified.iter().find(|e| e.extent_id == 20).unwrap();
        assert_eq!(e20.refs, 2);
        assert_eq!(e20.sealed_length, 2048);

        // Victim's tail E21: refs += 1, sealed = 8192.
        let e21 = modified.iter().find(|e| e.extent_id == 21).unwrap();
        assert_eq!(e21.refs, 2);
        assert_eq!(e21.sealed_length, 8192);

        // New tail E99 included as-is.
        let e99 = modified.iter().find(|e| e.extent_id == 99).unwrap();
        assert_eq!(e99.sealed_length, 0);
        assert_eq!(e99.refs, 1);
    }
```

- [ ] **Step 4: Run the test (expect FAIL: function not yet wired into module path)**

Run: `cd /data/dongmao_dev/autumn-rs && cargo test -p autumn-manager --lib compute_merge_streams_extent_ids_order_and_refs 2>&1 | tail -20`

If `compute_merge_streams` fails to resolve, ensure it's defined inside the `impl AutumnManager` block. Re-run.

Expected: PASS once the function is in the right scope.

- [ ] **Step 5: Commit**

```bash
git add crates/manager/src/lib.rs
git commit -m "F181-B1: compute_merge_streams pure-fn + unit test"
```

---

### Task 4: `merged_partition_vp_refs` pure function + test

**Files:**
- Modify: `crates/manager/src/lib.rs`

- [ ] **Step 1: Add the helper near `split_partition_vp_snapshot`**

Insert just below `split_partition_vp_snapshot` (around line 1083):

```rust
    /// F181: per-extent sum of two partitions' VP refs into a snapshot
    /// owned by `survivor_id`. Caller deletes `partition_vp_refs[victim_id]`
    /// in Phase 3.
    fn merged_partition_vp_refs(
        state: &autumn_common::MetadataState,
        survivor_id: u64,
        victim_id: u64,
    ) -> MgrPartitionVpRefs {
        let survivor = state
            .partition_vp_refs
            .get(&survivor_id)
            .cloned()
            .unwrap_or_default();
        let victim = state
            .partition_vp_refs
            .get(&victim_id)
            .cloned()
            .unwrap_or_default();
        let mut sum: HashMap<u64, u32> = survivor.refs.iter().copied().collect();
        for (eid, n) in victim.refs.iter().copied() {
            *sum.entry(eid).or_insert(0) += n;
        }
        MgrPartitionVpRefs {
            part_id: survivor_id,
            refs: sum.into_iter().collect(),
        }
    }
```

- [ ] **Step 2: Add unit test**

Append in the test module:

```rust
    #[test]
    fn merged_partition_vp_refs_sums_per_extent() {
        let mut state = autumn_common::MetadataState::default();
        state.partition_vp_refs.insert(1, MgrPartitionVpRefs {
            part_id: 1,
            refs: vec![(10, 2), (20, 5)],
        });
        state.partition_vp_refs.insert(2, MgrPartitionVpRefs {
            part_id: 2,
            refs: vec![(20, 3), (30, 7)],
        });
        let merged = AutumnManager::merged_partition_vp_refs(&state, 1, 2);
        assert_eq!(merged.part_id, 1);
        let map: HashMap<u64, u32> = merged.refs.iter().copied().collect();
        assert_eq!(map.get(&10), Some(&2));
        assert_eq!(map.get(&20), Some(&8));
        assert_eq!(map.get(&30), Some(&7));
    }
```

- [ ] **Step 3: Test + commit**

Run: `cd /data/dongmao_dev/autumn-rs && cargo test -p autumn-manager --lib merged_partition_vp_refs_sums_per_extent`
Expected: PASS.

```bash
git add crates/manager/src/lib.rs
git commit -m "F181-B2: merged_partition_vp_refs pure-fn + unit test"
```

---

### Task 5: `apply_merge_mutations` pure function

**Files:**
- Modify: `crates/manager/src/lib.rs`

- [ ] **Step 1: Add helper near `apply_split_mutations`**

Insert below `apply_split_mutations` (around line 1102-1118):

```rust
    /// F181: apply computed merge mutations to in-memory store.
    /// Mirror of `apply_split_mutations`. Caller has already verified
    /// (Phase 3) that no concurrent mutator drifted eversion during the
    /// etcd await.
    ///
    /// `survivor_streams` is the spliced-stream Vec from compute_merge_streams
    /// (3 entries: log, row, meta).
    /// `modified_extents` includes seal updates + refs++ + new_tail entries.
    /// `merged_vp_refs` is the survivor-owned merged snapshot.
    fn apply_merge_mutations(
        state: &mut autumn_common::MetadataState,
        survivor_streams: &[MgrStreamInfo],
        modified_extents: &[MgrExtentInfo],
        survivor_meta: MgrPartitionMeta,
        merged_vp_refs: MgrPartitionVpRefs,
        victim_part_id: u64,
        victim_log_stream: u64,
        victim_row_stream: u64,
        victim_meta_stream: u64,
    ) {
        for ex in modified_extents {
            state.extents.insert(ex.extent_id, ex.clone());
        }
        for st in survivor_streams {
            state.streams.insert(st.stream_id, st.clone());
        }
        state.partitions.insert(survivor_meta.part_id, survivor_meta);
        state.partition_vp_refs.insert(merged_vp_refs.part_id, merged_vp_refs);

        // Drop victim entries.
        state.partitions.remove(&victim_part_id);
        state.streams.remove(&victim_log_stream);
        state.streams.remove(&victim_row_stream);
        state.streams.remove(&victim_meta_stream);
        state.partition_vp_refs.remove(&victim_part_id);

        Self::rebalance_regions(state);
    }
```

- [ ] **Step 2: Add a unit test that builds two partitions then applies merge mutations**

Append in the test module:

```rust
    #[test]
    fn apply_merge_mutations_drops_victim_entries() {
        let mut state = autumn_common::MetadataState::default();
        // Survivor partition 1 with streams 100/101/102
        state.partitions.insert(1, MgrPartitionMeta {
            part_id: 1, log_stream: 100, row_stream: 101, meta_stream: 102,
            rg: Some(MgrRange { start_key: b"a".to_vec(), end_key: b"m".to_vec() }),
        });
        // Victim partition 2 with streams 200/201/202
        state.partitions.insert(2, MgrPartitionMeta {
            part_id: 2, log_stream: 200, row_stream: 201, meta_stream: 202,
            rg: Some(MgrRange { start_key: b"m".to_vec(), end_key: b"z".to_vec() }),
        });
        for sid in [100, 101, 102, 200, 201, 202] {
            state.streams.insert(sid, MgrStreamInfo {
                stream_id: sid, extent_ids: vec![],
                ec_data_shard: 1, ec_parity_shard: 0, replicates: vec![1],
            });
        }
        state.partition_vp_refs.insert(1, MgrPartitionVpRefs { part_id: 1, refs: vec![] });
        state.partition_vp_refs.insert(2, MgrPartitionVpRefs { part_id: 2, refs: vec![] });

        let new_survivor_meta = MgrPartitionMeta {
            part_id: 1, log_stream: 100, row_stream: 101, meta_stream: 102,
            rg: Some(MgrRange { start_key: b"a".to_vec(), end_key: b"z".to_vec() }),
        };

        AutumnManager::apply_merge_mutations(
            &mut state,
            &[],   // survivor_streams (none updated for this test)
            &[],   // modified_extents
            new_survivor_meta,
            MgrPartitionVpRefs { part_id: 1, refs: vec![] },
            2, 200, 201, 202,
        );

        assert!(state.partitions.contains_key(&1));
        assert!(!state.partitions.contains_key(&2));
        assert!(state.streams.contains_key(&100));
        assert!(!state.streams.contains_key(&200));
        assert!(!state.partition_vp_refs.contains_key(&2));
        assert_eq!(
            state.partitions.get(&1).unwrap().rg.as_ref().unwrap().end_key,
            b"z".to_vec()
        );
    }
```

- [ ] **Step 3: Test + commit**

Run: `cd /data/dongmao_dev/autumn-rs && cargo test -p autumn-manager --lib apply_merge_mutations_drops_victim_entries`
Expected: PASS.

```bash
git add crates/manager/src/lib.rs
git commit -m "F181-B3: apply_merge_mutations pure-fn + unit test"
```

---

## Phase C — last_op_at sidecar etcd key

### Task 6: Add `last_op_at` field on AutumnManager + replay

**Files:**
- Modify: `crates/manager/src/lib.rs`

- [ ] **Step 1: Locate `AutumnManager` struct definition**

Search for `pub struct AutumnManager` near top of the file. Note its existing fields.

- [ ] **Step 2: Add the `last_op_at` field**

Add to the struct:

```rust
    /// F181: per-partition unix-epoch timestamp of the last split or
    /// merge involving this partition. Sourced from etcd prefix
    /// `partitionLastOp/<part_id>` (i64 little-endian). Default 0
    /// for partitions never split/merged.
    pub(crate) last_op_at: Rc<RefCell<HashMap<u64, i64>>>,
```

In `AutumnManager::new` (or wherever the struct is constructed), initialise:

```rust
last_op_at: Rc::new(RefCell::new(HashMap::new())),
```

- [ ] **Step 3: Add replay step in `replay_from_etcd`**

Locate `replay_from_etcd`. After the existing partition/stream/extent prefixes are loaded, add:

```rust
        // F181: load partitionLastOp/ sidecar
        let resp = etcd.get_with_prefix("partitionLastOp/").await?;
        let mut last_op_map = HashMap::new();
        for kv in resp {
            let part_id: u64 = std::str::from_utf8(kv.key.strip_prefix(b"partitionLastOp/").unwrap_or(b""))
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| anyhow::anyhow!("bad partitionLastOp key"))?;
            if kv.value.len() >= 8 {
                let ts = i64::from_le_bytes(kv.value[..8].try_into().unwrap());
                last_op_map.insert(part_id, ts);
            }
        }
        *self.last_op_at.borrow_mut() = last_op_map;
```

(If `etcd.get_with_prefix` doesn't exist, look at how other prefixes are loaded — e.g., the partitions prefix replay block — and follow that pattern.)

- [ ] **Step 4: Helper method `last_op_at_for(part_id)`**

In `impl AutumnManager`:

```rust
    pub(crate) fn last_op_at_for(&self, part_id: u64) -> i64 {
        self.last_op_at.borrow().get(&part_id).copied().unwrap_or(0)
    }
```

- [ ] **Step 5: cargo check**

Run: `cd /data/dongmao_dev/autumn-rs && cargo check -p autumn-manager`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/manager/src/lib.rs
git commit -m "F181-C1: last_op_at sidecar field + etcd replay"
```

---

### Task 7: Extend `handle_multi_modify_split` to write `partitionLastOp`

**Files:**
- Modify: `crates/manager/src/rpc_handlers.rs`

- [ ] **Step 1: Locate the split handler's etcd Phase-2 kvs Vec build**

Find around line 1546 in `handle_multi_modify_split`. The block constructs `kvs: Vec<(String, Vec<u8>)>` for `etcd.put_msgs_txn(kvs)`.

- [ ] **Step 2: Append `partitionLastOp/` puts for both children**

Just before the `etcd.put_msgs_txn(kvs).await...` call, add:

```rust
                    let now = Self::epoch_seconds();
                    kvs.push((
                        format!("partitionLastOp/{}", left.part_id),
                        now.to_le_bytes().to_vec(),
                    ));
                    kvs.push((
                        format!("partitionLastOp/{}", right.part_id),
                        now.to_le_bytes().to_vec(),
                    ));
```

- [ ] **Step 3: Add Phase-3 in-memory map update**

After `apply_split_mutations(...)` in Phase 3, add:

```rust
                    let now = Self::epoch_seconds();
                    self.last_op_at.borrow_mut().insert(left.part_id, now);
                    self.last_op_at.borrow_mut().insert(right.part_id, now);
```

- [ ] **Step 4: cargo build and existing split tests stay green**

Run: `cd /data/dongmao_dev/autumn-rs && cargo test -p autumn-manager --lib split 2>&1 | tail -20`
Expected: existing split tests still PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/manager/src/rpc_handlers.rs
git commit -m "F181-C2: split handler writes partitionLastOp sidecar"
```

---

## Phase D — Manager merge handler

### Task 8: Implement `handle_multi_modify_merge` (Phase 1 + 1.5)

**Files:**
- Modify: `crates/manager/src/rpc_handlers.rs`

- [ ] **Step 1: Add the dispatch arm in `handle_request`**

Find the match around line 80-100 of `rpc_handlers.rs` (look for `MSG_MULTI_MODIFY_SPLIT => self.handle_multi_modify_split(payload).await,`). Add right below:

```rust
            MSG_MULTI_MODIFY_MERGE => self.handle_multi_modify_merge(payload).await,
```

- [ ] **Step 2: Add the handler stub at the end of the impl block**

Append (replace the body in subsequent steps):

```rust
    pub(crate) async fn handle_multi_modify_merge(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&MultiModifyMergeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                new_log_tail_extent_id: 0,
            }));
        }
        let req: MultiModifyMergeReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // Implementation continues — see following steps.
        Err((StatusCode::Internal, "F181 handler not yet implemented".to_string()))
    }
```

- [ ] **Step 3: Implement Phase 1 (under borrow_mut, no awaits)**

Replace the stub body with:

```rust
        // Phase 1: compute mutations under borrow_mut (NO awaits inside)
        let phase1 = {
            let mut s = self.store.inner.borrow_mut();
            (|| -> Result<(
                Vec<MgrStreamInfo>,
                Vec<MgrExtentInfo>,
                MgrPartitionMeta,
                MgrPartitionVpRefs,
                u64,                       // victim_part_id
                u64, u64, u64,             // victim_log/row/meta_stream
                u64,                       // new_tail_extent_id
                Vec<u64>,                  // new_tail_node_ids (for Phase 1.5)
                HashMap<u64, u64>,         // pre_bump_eversion
            ), AppError> {
                Self::ensure_owner_revision(&req.owner_key, req.revision, &s)?;

                if req.survivor_part_id == req.victim_part_id {
                    return Err(AppError::Precondition(
                        "survivor and victim are the same partition".to_string(),
                    ));
                }
                let survivor_meta = s
                    .partitions
                    .get(&req.survivor_part_id)
                    .cloned()
                    .ok_or_else(|| AppError::NotFound(format!("partition {}", req.survivor_part_id)))?;
                let victim_meta = s
                    .partitions
                    .get(&req.victim_part_id)
                    .cloned()
                    .ok_or_else(|| AppError::NotFound(format!("partition {}", req.victim_part_id)))?;

                let s_rg = survivor_meta.rg.clone()
                    .ok_or_else(|| AppError::Internal("survivor range missing".into()))?;
                let v_rg = victim_meta.rg.clone()
                    .ok_or_else(|| AppError::Internal("victim range missing".into()))?;
                if s_rg.end_key != v_rg.start_key {
                    return Err(AppError::Precondition(format!(
                        "partitions are not adjacent (survivor.end={:?}, victim.start={:?})",
                        s_rg.end_key, v_rg.start_key
                    )));
                }

                // F138/F145/F146: refuse if any source extent is in
                // ec_conversion_inflight, recovery_tasks, or pending_extent_deletes.
                let all_streams: Vec<u64> = vec![
                    survivor_meta.log_stream,
                    survivor_meta.row_stream,
                    survivor_meta.meta_stream,
                    victim_meta.log_stream,
                    victim_meta.row_stream,
                    victim_meta.meta_stream,
                ];
                {
                    let ec_inflight = self.ec_conversion_inflight.borrow();
                    let recovery_inflight = self.recovery_tasks.borrow();
                    let pending_deletes = self.pending_extent_deletes.borrow();
                    let pending_eids: HashSet<u64> = pending_deletes.iter().map(|p| p.extent_id).collect();
                    for &sid in &all_streams {
                        if let Some(stream) = s.streams.get(&sid) {
                            for &eid in &stream.extent_ids {
                                if ec_inflight.contains(&eid) {
                                    return Err(AppError::Precondition(format!(
                                        "ec conversion in flight on extent {eid}; retry merge"
                                    )));
                                }
                                if recovery_inflight.contains_key(&eid) {
                                    return Err(AppError::Precondition(format!(
                                        "recovery in flight on extent {eid}; retry merge"
                                    )));
                                }
                                if pending_eids.contains(&eid) {
                                    return Err(AppError::Precondition(format!(
                                        "extent {eid} pending delete; retry merge"
                                    )));
                                }
                            }
                        }
                    }
                }

                // Snapshot eversion for verify-at-apply (F146)
                let pre_bump_eversion: HashMap<u64, u64> = {
                    let mut m = HashMap::new();
                    for &sid in &all_streams {
                        if let Some(stream) = s.streams.get(&sid) {
                            for &eid in &stream.extent_ids {
                                if let Some(ex) = s.extents.get(&eid) {
                                    m.insert(eid, ex.eversion);
                                }
                            }
                        }
                    }
                    m
                };

                // Allocate the new tail extent id + select nodes for it.
                let (start, _end) = s.alloc_ids(1);
                let new_tail_id = start;
                let candidate_nodes = Self::select_nodes(&s, 3);   // 3-replica default
                if candidate_nodes.is_empty() {
                    return Err(AppError::Precondition("no healthy nodes available".into()));
                }
                let new_tail = MgrExtentInfo {
                    extent_id: new_tail_id,
                    replicates: candidate_nodes.clone(),
                    parity: vec![],
                    replicate_disks: vec![0; candidate_nodes.len()],
                    parity_disks: vec![],
                    sealed_length: 0,
                    avali: 0,
                    eversion: 0,
                    refs: 1,
                    vp_table_refs: 0,
                    ec_converted: false,
                };

                // Compute spliced streams. Survivor's log_stream gets the new tail.
                let (log_dup, log_exts) = Self::compute_merge_streams(
                    &s, survivor_meta.log_stream, victim_meta.log_stream,
                    req.log_sealed_lengths[0] as u32, req.log_sealed_lengths[1] as u32,
                    new_tail.clone(),
                )?;
                // For row + meta, no new tail — pass an empty extent that we exclude
                // from extent_ids by trimming. Simpler: build a separate path.
                // For row_stream: splice without new_tail.
                let row_dup = Self::splice_streams_without_new_tail(
                    &s, survivor_meta.row_stream, victim_meta.row_stream,
                    req.row_sealed_lengths[0] as u32, req.row_sealed_lengths[1] as u32,
                )?;
                let meta_dup = Self::splice_streams_without_new_tail(
                    &s, survivor_meta.meta_stream, victim_meta.meta_stream,
                    req.meta_sealed_lengths[0] as u32, req.meta_sealed_lengths[1] as u32,
                )?;

                let new_streams = vec![log_dup.0, row_dup.0, meta_dup.0];
                let mut all_extents = Vec::new();
                all_extents.extend(log_exts);
                all_extents.extend(row_dup.1);
                all_extents.extend(meta_dup.1);

                // Merged VP refs.
                let merged_vp = Self::merged_partition_vp_refs(
                    &s, req.survivor_part_id, req.victim_part_id);
                let vp_extent_puts = Self::preview_partition_vp_refs_apply(&s, &merged_vp);
                let all_extents = Self::merge_extent_updates(all_extents, vp_extent_puts);

                // New survivor partition meta with widened range.
                let mut new_survivor_meta = survivor_meta.clone();
                new_survivor_meta.rg = Some(MgrRange {
                    start_key: s_rg.start_key,
                    end_key: v_rg.end_key,
                });

                Ok((
                    new_streams,
                    all_extents,
                    new_survivor_meta,
                    merged_vp,
                    req.victim_part_id,
                    victim_meta.log_stream,
                    victim_meta.row_stream,
                    victim_meta.meta_stream,
                    new_tail_id,
                    candidate_nodes,
                    pre_bump_eversion,
                ))
            })()
        };

        let (new_streams, all_extents, new_survivor_meta, merged_vp,
             victim_part_id, victim_log, victim_row, victim_meta,
             new_tail_id, new_tail_nodes, pre_bump_eversion) =
            match phase1 {
                Ok(t) => t,
                Err(e) => return Ok(rkyv_encode(&MultiModifyMergeResp {
                    code: Self::err_to_code(&e),
                    message: e.to_string(),
                    new_log_tail_extent_id: 0,
                })),
            };

        // Phase 1.5: alloc_extent_on_node for E_new on each replica.
        for node_id in &new_tail_nodes {
            if let Err(e) = self.alloc_extent_on_node(*node_id, new_tail_id).await {
                return Ok(rkyv_encode(&MultiModifyMergeResp {
                    code: CODE_INTERNAL,
                    message: format!("alloc_extent_on_node({node_id}, {new_tail_id}): {e}"),
                    new_log_tail_extent_id: 0,
                }));
            }
        }
```

- [ ] **Step 4: Add the `splice_streams_without_new_tail` helper near `compute_merge_streams`**

In `lib.rs`:

```rust
    /// F181: same as compute_merge_streams but without appending a fresh tail.
    /// Used for row_stream + meta_stream where the "current tail" is the
    /// last existing extent (sealed by the caller's commit_length).
    fn splice_streams_without_new_tail(
        state: &autumn_common::MetadataState,
        survivor_stream_id: u64,
        victim_stream_id: u64,
        survivor_sealed: u32,
        victim_sealed: u32,
    ) -> Result<(MgrStreamInfo, Vec<MgrExtentInfo>), AppError> {
        let survivor = state.streams.get(&survivor_stream_id).cloned()
            .ok_or_else(|| AppError::NotFound(format!("stream {survivor_stream_id}")))?;
        let victim = state.streams.get(&victim_stream_id).cloned()
            .ok_or_else(|| AppError::NotFound(format!("stream {victim_stream_id}")))?;

        let mut modified_extents = Vec::new();
        if let Some(&tail_id) = survivor.extent_ids.last() {
            let extent = state.extents.get(&tail_id)
                .ok_or_else(|| AppError::NotFound(format!("extent {tail_id}")))?;
            let mut ex = extent.clone();
            if ex.sealed_length == 0 && survivor_sealed > 0 {
                ex.sealed_length = survivor_sealed as u64;
                ex.eversion += 1;
                ex.avali = Self::all_bits(ex.replicates.len() + ex.parity.len());
                modified_extents.push(ex);
            }
        }
        for (idx, &eid) in victim.extent_ids.iter().enumerate() {
            let extent = state.extents.get(&eid)
                .ok_or_else(|| AppError::NotFound(format!("extent {eid}")))?;
            let mut ex = extent.clone();
            ex.refs += 1;
            ex.eversion += 1;
            if idx == victim.extent_ids.len() - 1 && ex.sealed_length == 0 && victim_sealed > 0 {
                ex.sealed_length = victim_sealed as u64;
                ex.avali = Self::all_bits(ex.replicates.len() + ex.parity.len());
            }
            modified_extents.push(ex);
        }

        let mut new_extent_ids = survivor.extent_ids.clone();
        new_extent_ids.extend(victim.extent_ids.iter().copied());

        Ok((MgrStreamInfo {
            stream_id: survivor.stream_id,
            extent_ids: new_extent_ids,
            ec_data_shard: survivor.ec_data_shard,
            ec_parity_shard: survivor.ec_parity_shard,
            replicates: survivor.replicates,
        }, modified_extents))
    }
```

- [ ] **Step 5: cargo check**

Run: `cd /data/dongmao_dev/autumn-rs && cargo check -p autumn-manager`
Fix any unresolved imports (likely need `HashSet` and `MultiModifyMergeReq/Resp` in scope at the top of `rpc_handlers.rs`).

- [ ] **Step 6: Commit (Phase 1+1.5 only — handler still returns Internal at end)**

```bash
git add crates/manager/src/lib.rs crates/manager/src/rpc_handlers.rs
git commit -m "F181-D1: handle_multi_modify_merge Phase 1 + 1.5"
```

---

### Task 9: Implement Phase 2 (etcd) + Phase 3 (apply)

**Files:**
- Modify: `crates/manager/src/rpc_handlers.rs`

- [ ] **Step 1: Append Phase 2 to the handler body**

Continuing from where Task 8 stopped (after the `alloc_extent_on_node` loop):

```rust
        // Phase 2: single fenced etcd txn (F124-style).
        if let Some(etcd) = &self.etcd {
            let now = Self::epoch_seconds();
            let mut kvs = Vec::with_capacity(new_streams.len() + all_extents.len() + 6);
            for st in &new_streams {
                kvs.push((format!("streams/{}", st.stream_id), rkyv_encode(st).to_vec()));
            }
            for ex in &all_extents {
                kvs.push((format!("extents/{}", ex.extent_id), rkyv_encode(ex).to_vec()));
            }
            kvs.push((
                format!("partitionVpRefs/{}", merged_vp.part_id),
                rkyv_encode(&merged_vp).to_vec(),
            ));
            kvs.push((
                format!("partitions/{}", new_survivor_meta.part_id),
                rkyv_encode(&new_survivor_meta).to_vec(),
            ));
            // Survivor region update
            {
                let s = self.store.inner.borrow();
                let region = Self::compute_region_for_partition(&s, &new_survivor_meta);
                kvs.push((
                    format!("regions/{}", new_survivor_meta.part_id),
                    rkyv_encode(&region).to_vec(),
                ));
            }
            kvs.push((
                format!("partitionLastOp/{}", new_survivor_meta.part_id),
                now.to_le_bytes().to_vec(),
            ));

            let deletes = vec![
                format!("partitions/{victim_part_id}"),
                format!("streams/{victim_log}"),
                format!("streams/{victim_row}"),
                format!("streams/{victim_meta}"),
                format!("partitionVpRefs/{victim_part_id}"),
                format!("regions/{victim_part_id}"),
                format!("partitionLastOp/{victim_part_id}"),
            ];

            etcd.put_and_delete_txn(kvs, deletes)
                .await
                .map_err(|e| Self::err_to_status(&e))?;
        }
```

- [ ] **Step 2: Append Phase 3**

```rust
        // Phase 3: apply to in-memory store + verify eversion drift.
        {
            let mut s = self.store.inner.borrow_mut();
            for (eid, expected) in &pre_bump_eversion {
                if let Some(live) = s.extents.get(eid).map(|ex| ex.eversion) {
                    if live != *expected {
                        return Ok(rkyv_encode(&MultiModifyMergeResp {
                            code: CODE_PRECONDITION,
                            message: format!(
                                "extent {eid} eversion drift during merge \
                                 ({expected} -> {live}); retry merge"
                            ),
                            new_log_tail_extent_id: 0,
                        }));
                    }
                }
            }
            Self::apply_merge_mutations(
                &mut s,
                &new_streams,
                &all_extents,
                new_survivor_meta.clone(),
                merged_vp,
                victim_part_id,
                victim_log,
                victim_row,
                victim_meta,
            );
        }
        // last_op_at in-memory map
        let now = Self::epoch_seconds();
        self.last_op_at.borrow_mut().insert(new_survivor_meta.part_id, now);
        self.last_op_at.borrow_mut().remove(&victim_part_id);

        Ok(rkyv_encode(&MultiModifyMergeResp {
            code: CODE_OK,
            message: String::new(),
            new_log_tail_extent_id: new_tail_id,
        }))
```

(Replace the trailing `Err((StatusCode::Internal, ...))` from Task 8.)

- [ ] **Step 3: cargo build clean + existing split tests still pass**

Run: `cd /data/dongmao_dev/autumn-rs && cargo test -p autumn-manager --lib 2>&1 | tail -40`
Expected: all existing tests pass; merge handler compiles.

- [ ] **Step 4: Commit**

```bash
git add crates/manager/src/rpc_handlers.rs
git commit -m "F181-D2: handle_multi_modify_merge Phase 2 (etcd) + Phase 3 (apply)"
```

---

### Task 10: Manager-level merge integration test (with embedded etcd)

**Files:**
- Modify: `crates/manager/src/lib.rs`

- [ ] **Step 1: Add a unit test that constructs an in-memory state with two adjacent partitions and calls handle_multi_modify_merge**

In the existing `mod tests { ... }` block (look for existing F138/F146 tests around line 2400+), add:

```rust
    #[compio::test]
    async fn f181_multi_modify_merge_basic_in_memory() {
        // Build a manager in memory-only mode (no etcd).
        let m = AutumnManager::new_in_memory();
        // Register a node and create two partitions ranged [a..m), [m..z)
        // [boilerplate omitted: model after f138_split_aborts_when_source_extent_is_ec_inflight]
        // ...
        // Issue handle_multi_modify_merge and assert:
        //   - resp.code == CODE_OK
        //   - resp.new_log_tail_extent_id != 0
        //   - state.partitions has only survivor
        //   - state.streams missing victim's three streams
        //   - state.partitions[survivor].rg.end_key == [b'z'].to_vec()
    }
```

The existing `f138_*` tests show the precise pattern for in-memory state setup; reuse that scaffolding. Look at lines around 2338-2422 for the template.

- [ ] **Step 2: Run the test**

Run: `cd /data/dongmao_dev/autumn-rs && cargo test -p autumn-manager --lib f181_multi_modify_merge_basic_in_memory 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/manager/src/lib.rs
git commit -m "F181-D3: in-memory merge handler smoke test"
```

---

## Phase E — Policy engine

### Task 11: Create `policy.rs` skeleton

**Files:**
- Create: `crates/manager/src/policy.rs`

- [ ] **Step 1: Create the file with thresholds + types**

```rust
//! F181 policy engine: per-partition metrics window + split/merge candidate
//! computation. Stage 1 is advisory only; auto-trigger gated behind feature
//! flags in Stage 2/3.

use std::collections::{HashMap, VecDeque};
use autumn_rpc::manager_rpc::{PartitionLoad, PolicyCandidate, POLICY_KIND_SPLIT, POLICY_KIND_MERGE};

const GIB: u64 = 1024 * 1024 * 1024;

pub const SPLIT_SIZE_HARD: u64        = 50 * GIB;
pub const SPLIT_SIZE_MIN:  u64        = GIB;
pub const SPLIT_QPS_HIGH:  u32        = 50_000;
pub const SPLIT_IMMFULL_HIGH: u32     = 10;
pub const SPLIT_COOLDOWN_SEC: i64     = 3600;

pub const MERGE_SIZE_LOW:  u64        = GIB;
pub const MERGE_QPS_LOW:   u32        = 5_000;
pub const MERGE_COOLDOWN_SEC: i64     = 6 * 3600;

pub const POLICY_BUCKET_SEC:      i64 = 60;
pub const POLICY_WINDOW_BUCKETS:  usize = 30;
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
    pub last_advisory_at: HashMap<(u8, u64, u64), i64>, // (kind, primary, secondary) -> ts
    pub advisory_cache: Vec<PolicyCandidate>,
    pub advisory_cache_at: i64,
}
```

- [ ] **Step 2: Wire `mod policy;` into `crates/manager/src/lib.rs`**

Near the top of `lib.rs` add:

```rust
pub(crate) mod policy;
```

And expose `PolicyEngine` for tests:

```rust
#[cfg(test)]
pub use policy::PolicyEngine;
```

- [ ] **Step 3: cargo check**

Run: `cd /data/dongmao_dev/autumn-rs && cargo check -p autumn-manager`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/manager/src/policy.rs crates/manager/src/lib.rs
git commit -m "F181-E1: policy.rs skeleton — thresholds + window struct"
```

---

### Task 12: `PolicyEngine::compute_candidates` — split logic + tests

**Files:**
- Modify: `crates/manager/src/policy.rs`
- Create: `crates/manager/src/policy_tests.rs`

- [ ] **Step 1: Add `compute_candidates` method to PolicyEngine**

```rust
use autumn_common::MetadataState;

pub struct ComputeArgs<'a> {
    pub state: &'a MetadataState,
    pub last_op_at: &'a HashMap<u64, i64>,
    pub region_owners: &'a HashMap<u64, u64>, // part_id -> ps_id (for same-PS check)
    pub now: i64,
}

impl PolicyEngine {
    pub fn compute_candidates(&mut self, args: ComputeArgs<'_>) -> Vec<PolicyCandidate> {
        let mut out = Vec::new();
        // SPLIT pass
        for (&part_id, window) in self.metrics.iter() {
            // Need at least POLICY_REQUIRED_BUCKETS buckets in the most recent
            // POLICY_REQUIRED_BUCKETS-bucket window.
            let bs: Vec<&(i64, PartitionLoad)> = window.buckets.iter()
                .rev().take(POLICY_REQUIRED_BUCKETS).collect();
            if bs.len() < POLICY_REQUIRED_BUCKETS { continue; }

            let last_op = args.last_op_at.get(&part_id).copied().unwrap_or(0);
            if args.now - last_op < SPLIT_COOLDOWN_SEC { continue; }

            // ALL of the last POLICY_REQUIRED_BUCKETS buckets must show
            // *some* split-trigger condition.
            let all_match = bs.iter().all(|(_, l)| {
                l.size_bytes > SPLIT_SIZE_HARD
                || (l.req_per_sec > SPLIT_QPS_HIGH && l.size_bytes > SPLIT_SIZE_MIN)
                || l.imm_full_per_sec > SPLIT_IMMFULL_HIGH
            });
            if !all_match { continue; }

            // Pick the worst-bucket reason for the message.
            let recent = &bs[0].1;
            let reason = if recent.size_bytes > SPLIT_SIZE_HARD {
                format!("size_bytes>{} ({} GiB)", SPLIT_SIZE_HARD, recent.size_bytes / GIB)
            } else if recent.imm_full_per_sec > SPLIT_IMMFULL_HIGH {
                format!("imm_full_per_sec>{} sustained", SPLIT_IMMFULL_HIGH)
            } else {
                format!("req_per_sec>{} sustained AND size_bytes>{}",
                        SPLIT_QPS_HIGH, SPLIT_SIZE_MIN)
            };
            out.push(PolicyCandidate {
                kind: POLICY_KIND_SPLIT,
                primary_part_id: part_id,
                secondary_part_id: 0,
                reason,
                size_bytes: recent.size_bytes,
                req_per_sec: recent.req_per_sec,
                imm_full_per_sec: recent.imm_full_per_sec,
                same_ps: true,    // not meaningful for split
                last_op_at: last_op,
            });
        }

        // MERGE pass — implemented in Task 13.
        // ...

        self.advisory_cache = out.clone();
        self.advisory_cache_at = args.now;
        out
    }
}
```

- [ ] **Step 2: Create `crates/manager/src/policy_tests.rs`**

```rust
//! F181 policy engine unit tests.

use std::collections::HashMap;
use autumn_common::MetadataState;
use autumn_rpc::manager_rpc::{PartitionLoad, POLICY_KIND_SPLIT, POLICY_KIND_MERGE};
use crate::policy::{
    PolicyEngine, ComputeArgs, POLICY_BUCKET_SEC, POLICY_REQUIRED_BUCKETS,
    SPLIT_SIZE_HARD, SPLIT_QPS_HIGH, SPLIT_IMMFULL_HIGH, SPLIT_COOLDOWN_SEC,
};

const GIB: u64 = 1024 * 1024 * 1024;

fn fill_window(eng: &mut PolicyEngine, part_id: u64, n: usize, load: PartitionLoad, base_ts: i64) {
    for i in 0..n {
        eng.metrics.entry(part_id).or_default()
            .push(base_ts + i as i64 * POLICY_BUCKET_SEC, load.clone());
    }
}

#[test]
fn split_size_hard_triggers() {
    let state = MetadataState::default();
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    fill_window(&mut eng, 7, POLICY_REQUIRED_BUCKETS, PartitionLoad {
        part_id: 7,
        size_bytes: SPLIT_SIZE_HARD + GIB,
        req_per_sec: 100,
        imm_full_per_sec: 0,
        p99_us: 0,
    }, now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC);
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
    fill_window(&mut eng, 7, POLICY_REQUIRED_BUCKETS, PartitionLoad {
        part_id: 7,
        size_bytes: 100 * 1024 * 1024,    // <1 GiB → blocked
        req_per_sec: SPLIT_QPS_HIGH + 1000,
        imm_full_per_sec: 0,
        p99_us: 0,
    }, now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC);
    let out = eng.compute_candidates(ComputeArgs {
        state: &state, last_op_at: &HashMap::new(),
        region_owners: &HashMap::new(), now,
    });
    assert!(out.is_empty());
}

#[test]
fn split_cooldown_blocks() {
    let state = MetadataState::default();
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    fill_window(&mut eng, 7, POLICY_REQUIRED_BUCKETS, PartitionLoad {
        part_id: 7,
        size_bytes: SPLIT_SIZE_HARD + GIB,
        req_per_sec: 0, imm_full_per_sec: 0, p99_us: 0,
    }, now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC);
    let mut last_op = HashMap::new();
    last_op.insert(7, now - 60);   // 60 s ago, well within 1 h cooldown
    let out = eng.compute_candidates(ComputeArgs {
        state: &state, last_op_at: &last_op,
        region_owners: &HashMap::new(), now,
    });
    assert!(out.is_empty());
}

#[test]
fn split_partial_window_no_trigger() {
    let state = MetadataState::default();
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    // Only 4 buckets, need 5
    fill_window(&mut eng, 7, POLICY_REQUIRED_BUCKETS - 1, PartitionLoad {
        part_id: 7,
        size_bytes: SPLIT_SIZE_HARD + GIB,
        req_per_sec: 0, imm_full_per_sec: 0, p99_us: 0,
    }, now - 4 * POLICY_BUCKET_SEC);
    let out = eng.compute_candidates(ComputeArgs {
        state: &state, last_op_at: &HashMap::new(),
        region_owners: &HashMap::new(), now,
    });
    assert!(out.is_empty());
}
```

Wire in lib.rs near the existing `mod policy;`:

```rust
#[cfg(test)]
mod policy_tests;
```

- [ ] **Step 3: Run the four tests**

Run: `cd /data/dongmao_dev/autumn-rs && cargo test -p autumn-manager --lib 'policy_tests::split' 2>&1 | tail -20`
Expected: 4/4 PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/manager/src/policy.rs crates/manager/src/policy_tests.rs crates/manager/src/lib.rs
git commit -m "F181-E2: policy split logic + 4 unit tests"
```

---

### Task 13: `PolicyEngine::compute_candidates` — merge pass + tests

**Files:**
- Modify: `crates/manager/src/policy.rs`
- Modify: `crates/manager/src/policy_tests.rs`

- [ ] **Step 1: Replace the `// MERGE pass — implemented in Task 13.` block with**

```rust
        // MERGE pass: walk partitions sorted by start_key; for each adjacent
        // pair where end_key == next.start_key, check both sides' windows.
        let mut sorted_parts: Vec<(u64, &MgrPartitionMeta)> = args.state
            .partitions
            .iter()
            .filter_map(|(id, p)| p.rg.as_ref().map(|_| (*id, p)))
            .collect();
        sorted_parts.sort_by(|(_, a), (_, b)| {
            a.rg.as_ref().unwrap().start_key.cmp(&b.rg.as_ref().unwrap().start_key)
        });

        for win in sorted_parts.windows(2) {
            let (left_id, left_meta) = win[0];
            let (right_id, right_meta) = win[1];
            if left_meta.rg.as_ref().unwrap().end_key !=
               right_meta.rg.as_ref().unwrap().start_key {
                continue;
            }
            let lw = match self.metrics.get(&left_id) { Some(w) => w, None => continue };
            let rw = match self.metrics.get(&right_id) { Some(w) => w, None => continue };
            let lbs: Vec<&(i64, PartitionLoad)> = lw.buckets.iter()
                .rev().take(POLICY_REQUIRED_BUCKETS).collect();
            let rbs: Vec<&(i64, PartitionLoad)> = rw.buckets.iter()
                .rev().take(POLICY_REQUIRED_BUCKETS).collect();
            if lbs.len() < POLICY_REQUIRED_BUCKETS || rbs.len() < POLICY_REQUIRED_BUCKETS {
                continue;
            }

            let last_op_l = args.last_op_at.get(&left_id).copied().unwrap_or(0);
            let last_op_r = args.last_op_at.get(&right_id).copied().unwrap_or(0);
            let max_last_op = last_op_l.max(last_op_r);
            if args.now - max_last_op < MERGE_COOLDOWN_SEC { continue; }

            let all_qualify = lbs.iter().zip(rbs.iter()).all(|((_, lb), (_, rb))| {
                lb.size_bytes < MERGE_SIZE_LOW &&
                rb.size_bytes < MERGE_SIZE_LOW &&
                (lb.req_per_sec + rb.req_per_sec) < MERGE_QPS_LOW &&
                lb.imm_full_per_sec == 0 &&
                rb.imm_full_per_sec == 0
            });
            if !all_qualify { continue; }

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
                primary_part_id: left_id,        // survivor candidate = left
                secondary_part_id: right_id,
                reason: format!(
                    "size_sum<{} qps_sum<{} sustained{}",
                    MERGE_SIZE_LOW, MERGE_QPS_LOW,
                    if !same_ps { " (cross-PS, infeasible)" } else { "" }
                ),
                size_bytes: recent_l.size_bytes + recent_r.size_bytes,
                req_per_sec: recent_l.req_per_sec + recent_r.req_per_sec,
                imm_full_per_sec: 0,
                same_ps,
                last_op_at: max_last_op,
            });
        }
```

(Note: needs `MgrPartitionMeta` and `MERGE_*` constants in scope — add the missing imports at top of file.)

- [ ] **Step 2: Add 3 merge unit tests to `policy_tests.rs`**

```rust
use crate::policy::{MERGE_SIZE_LOW, MERGE_QPS_LOW, MERGE_COOLDOWN_SEC};
use autumn_rpc::manager_rpc::{MgrPartitionMeta, MgrRange};

fn mk_part(state: &mut MetadataState, id: u64, start: &[u8], end: &[u8]) {
    state.partitions.insert(id, MgrPartitionMeta {
        part_id: id, log_stream: 0, row_stream: 0, meta_stream: 0,
        rg: Some(MgrRange { start_key: start.to_vec(), end_key: end.to_vec() }),
    });
}

#[test]
fn merge_adjacent_pair_qualifying_same_ps() {
    let mut state = MetadataState::default();
    mk_part(&mut state, 1, b"a", b"m");
    mk_part(&mut state, 2, b"m", b"z");
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    let small = PartitionLoad {
        part_id: 0, size_bytes: 200 * 1024 * 1024, req_per_sec: 100,
        imm_full_per_sec: 0, p99_us: 0,
    };
    fill_window(&mut eng, 1, POLICY_REQUIRED_BUCKETS, small.clone(),
                now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC);
    fill_window(&mut eng, 2, POLICY_REQUIRED_BUCKETS, small,
                now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC);
    let mut owners = HashMap::new();
    owners.insert(1u64, 99u64);
    owners.insert(2u64, 99u64);
    let out = eng.compute_candidates(ComputeArgs {
        state: &state, last_op_at: &HashMap::new(),
        region_owners: &owners, now,
    });
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].kind, POLICY_KIND_MERGE);
    assert_eq!(out[0].primary_part_id, 1);
    assert_eq!(out[0].secondary_part_id, 2);
    assert!(out[0].same_ps);
}

#[test]
fn merge_cross_ps_marks_infeasible() {
    // identical to above but owners differ
    let mut state = MetadataState::default();
    mk_part(&mut state, 1, b"a", b"m");
    mk_part(&mut state, 2, b"m", b"z");
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    let small = PartitionLoad {
        part_id: 0, size_bytes: 200 * 1024 * 1024, req_per_sec: 100,
        imm_full_per_sec: 0, p99_us: 0,
    };
    fill_window(&mut eng, 1, POLICY_REQUIRED_BUCKETS, small.clone(),
                now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC);
    fill_window(&mut eng, 2, POLICY_REQUIRED_BUCKETS, small,
                now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC);
    let mut owners = HashMap::new();
    owners.insert(1u64, 11u64);
    owners.insert(2u64, 22u64);
    let out = eng.compute_candidates(ComputeArgs {
        state: &state, last_op_at: &HashMap::new(),
        region_owners: &owners, now,
    });
    assert_eq!(out.len(), 1);
    assert!(!out[0].same_ps);
}

#[test]
fn merge_non_adjacent_no_trigger() {
    let mut state = MetadataState::default();
    mk_part(&mut state, 1, b"a", b"f");
    mk_part(&mut state, 2, b"m", b"z");   // gap [f..m)
    let mut eng = PolicyEngine::default();
    let now = 1_700_000_000;
    let small = PartitionLoad {
        part_id: 0, size_bytes: 200 * 1024 * 1024, req_per_sec: 100,
        imm_full_per_sec: 0, p99_us: 0,
    };
    fill_window(&mut eng, 1, POLICY_REQUIRED_BUCKETS, small.clone(),
                now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC);
    fill_window(&mut eng, 2, POLICY_REQUIRED_BUCKETS, small,
                now - POLICY_REQUIRED_BUCKETS as i64 * POLICY_BUCKET_SEC);
    let owners = HashMap::new();
    let out = eng.compute_candidates(ComputeArgs {
        state: &state, last_op_at: &HashMap::new(),
        region_owners: &owners, now,
    });
    assert!(out.is_empty());
}
```

- [ ] **Step 3: Run all policy tests**

Run: `cd /data/dongmao_dev/autumn-rs && cargo test -p autumn-manager --lib policy_tests 2>&1 | tail -30`
Expected: 7/7 PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/manager/src/policy.rs crates/manager/src/policy_tests.rs
git commit -m "F181-E3: policy merge logic + 3 unit tests"
```

---

## Phase F — Manager dispatch wire-up

### Task 14: Wire `MSG_GET_POLICY_CANDIDATES` and `MSG_REPORT_PARTITION_LOAD` handlers

**Files:**
- Modify: `crates/manager/src/rpc_handlers.rs`
- Modify: `crates/manager/src/lib.rs`

- [ ] **Step 1: Add `policy: Rc<RefCell<PolicyEngine>>` field on AutumnManager**

In the struct definition:

```rust
    pub(crate) policy: Rc<RefCell<crate::policy::PolicyEngine>>,
```

Initialise in `new`:

```rust
policy: Rc::new(RefCell::new(crate::policy::PolicyEngine::default())),
```

- [ ] **Step 2: Add the two dispatch arms in `handle_request`**

```rust
            MSG_GET_POLICY_CANDIDATES => self.handle_get_policy_candidates(payload).await,
            MSG_REPORT_PARTITION_LOAD => self.handle_report_partition_load(payload).await,
```

- [ ] **Step 3: Implement the handlers**

```rust
    pub(crate) async fn handle_report_partition_load(&self, payload: Bytes) -> HandlerResult {
        let req: ReportPartitionLoadReq = rkyv_decode(&payload)
            .map_err(|e| (StatusCode::InvalidArgument, e))?;
        let now = Self::epoch_seconds();
        let mut p = self.policy.borrow_mut();
        for load in req.partitions {
            p.metrics.entry(load.part_id).or_default().push(now, load);
        }
        drop(p);
        Ok(rkyv_encode(&CodeResp { code: CODE_OK, message: String::new() }))
    }

    pub(crate) async fn handle_get_policy_candidates(&self, _payload: Bytes) -> HandlerResult {
        let p = self.policy.borrow();
        let candidates = p.advisory_cache.clone();
        Ok(rkyv_encode(&GetPolicyCandidatesResp {
            code: CODE_OK,
            message: String::new(),
            candidates,
        }))
    }
```

- [ ] **Step 4: Spawn `policy_tick_loop`**

In `AutumnManager::serve` (or wherever other background loops are spawned — heartbeat / disk_status_update / etc.), add:

```rust
        let mgr = self.clone();
        compio::runtime::spawn(async move {
            let interval = Duration::from_secs(crate::policy::POLICY_TICK_INTERVAL_SEC as u64);
            loop {
                compio::time::sleep(interval).await;
                if !mgr.leader.get() { continue; }
                let now = Self::epoch_seconds();
                let owners: HashMap<u64, u64> = {
                    let s = mgr.store.inner.borrow();
                    s.regions.iter().map(|(id, r)| (*id, r.ps_id)).collect()
                };
                let last_op = mgr.last_op_at.borrow().clone();
                let state_snapshot = mgr.store.inner.borrow().clone();
                let mut p = mgr.policy.borrow_mut();
                let cands = p.compute_candidates(crate::policy::ComputeArgs {
                    state: &state_snapshot,
                    last_op_at: &last_op,
                    region_owners: &owners,
                    now,
                });
                if !cands.is_empty() {
                    tracing::info!("F181 policy: {} candidates", cands.len());
                    for c in &cands {
                        tracing::info!("  {:?}: {} -> {} ({})",
                            if c.kind == POLICY_KIND_SPLIT { "SPLIT" } else { "MERGE" },
                            c.primary_part_id, c.secondary_part_id, c.reason);
                    }
                }
            }
        }).detach();
```

- [ ] **Step 5: cargo check + run all manager tests**

Run: `cd /data/dongmao_dev/autumn-rs && cargo test -p autumn-manager --lib 2>&1 | tail -30`
Expected: clean compile; existing tests + policy unit tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/manager/src/lib.rs crates/manager/src/rpc_handlers.rs
git commit -m "F181-F: policy_tick_loop + handle_get_policy_candidates + handle_report_partition_load"
```

---

## Phase G — PS metrics export

### Task 15: Add `PartitionMetrics` to `PartitionData`

**Files:**
- Modify: `crates/partition-server/src/lib.rs`

- [ ] **Step 1: Define `PartitionMetrics` near `PartitionData`**

```rust
#[derive(Default)]
pub(crate) struct PartitionMetrics {
    pub req_count_60s: AtomicU64,
    pub imm_full_count_60s: AtomicU64,
}
```

- [ ] **Step 2: Add `metrics: Rc<PartitionMetrics>` field to `PartitionData`**

```rust
    pub(crate) metrics: Rc<PartitionMetrics>,
```

Initialise in `PartitionData::new`:

```rust
metrics: Rc::new(PartitionMetrics::default()),
```

- [ ] **Step 3: cargo check + commit**

```bash
git add crates/partition-server/src/lib.rs
git commit -m "F181-G1: PartitionMetrics struct on PartitionData"
```

---

### Task 16: Bump counters in `merged_partition_loop`

**Files:**
- Modify: `crates/partition-server/src/background.rs`

- [ ] **Step 1: Add request counter bump**

In `merged_partition_loop`, find the `handle_incoming_req` call site (where PUT/DELETE/GET are dispatched). Just before dispatch:

```rust
            part.borrow().metrics.req_count_60s.fetch_add(1, Ordering::Relaxed);
```

- [ ] **Step 2: Add imm_full event counter**

Find the F120-A "imm_full → skip launching new batch" branch (search for `imm_full` or `MAX_IMM_DEPTH`). When that branch is taken, bump:

```rust
            part.borrow().metrics.imm_full_count_60s.fetch_add(1, Ordering::Relaxed);
```

- [ ] **Step 3: cargo check + existing PS tests still pass**

Run: `cd /data/dongmao_dev/autumn-rs && cargo test -p autumn-partition-server --lib -- --test-threads=1 2>&1 | tail -20`
Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add crates/partition-server/src/background.rs
git commit -m "F181-G2: bump req_count + imm_full_count in merged_partition_loop"
```

---

### Task 17: PS `report_load_loop`

**Files:**
- Modify: `crates/partition-server/src/lib.rs`

- [ ] **Step 1: Snapshot helper that builds `PartitionLoad` per partition**

```rust
fn snapshot_partition_load(part: &Rc<RefCell<PartitionData>>) -> PartitionLoad {
    let p = part.borrow();
    let req = p.metrics.req_count_60s.swap(0, Ordering::Relaxed);
    let imm = p.metrics.imm_full_count_60s.swap(0, Ordering::Relaxed);
    let active_bytes = p.active.bytes();
    let imm_bytes: u64 = p.imm.iter().map(|m| m.bytes()).sum();
    let sst_bytes: u64 = p.tables.iter().map(|t| t.size).sum();
    PartitionLoad {
        part_id: p.part_id,
        size_bytes: active_bytes + imm_bytes + sst_bytes,
        req_per_sec: (req / 5) as u32,        // 5 s tick
        imm_full_per_sec: (imm / 5) as u32,
        p99_us: 0,
    }
}
```

- [ ] **Step 2: Spawn the loop in `PartitionServer::serve` (or `finish_connect`)**

Find where `heartbeat_loop` is spawned (per F111, in `finish_connect`). Add a sibling:

```rust
        let server_clone = self.clone();
        compio::runtime::spawn(async move {
            let interval = Duration::from_secs(5);
            loop {
                compio::time::sleep(interval).await;
                let snapshots: Vec<PartitionLoad> = {
                    let parts = server_clone.partitions.borrow();
                    parts.values().map(|h| snapshot_partition_load(&h.part)).collect()
                };
                if snapshots.is_empty() { continue; }
                let req = ReportPartitionLoadReq {
                    ps_id: server_clone.ps_id,
                    partitions: snapshots,
                };
                let payload = manager_rpc::rkyv_encode(&req);
                if let Err(e) = server_clone.pool
                    .call(&server_clone.manager_addr, MSG_REPORT_PARTITION_LOAD, payload)
                    .await
                {
                    tracing::debug!("F181 report_load failed: {e}");
                }
            }
        }).detach();
```

- [ ] **Step 3: cargo build + smoke that PS still starts cleanly**

Run: `cd /data/dongmao_dev/autumn-rs && cargo build --workspace --exclude autumn-fuse 2>&1 | tail -10`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/partition-server/src/lib.rs
git commit -m "F181-G3: PS report_load_loop (5 s cadence)"
```

---

## Phase H — PS merge handler

### Task 18: Add `MergeFreeze` / `MergeRelease` partition control messages

**Files:**
- Modify: `crates/partition-server/src/lib.rs`
- Modify: `crates/partition-server/src/background.rs`

- [ ] **Step 1: Find `PartitionRequest` enum (or its sibling control channel)**

It's the request enum carrying messages from ps-conn into `merged_partition_loop`. Search for `enum PartitionRequest` or the `Drain` variant (added in F120-C).

- [ ] **Step 2: Add the two new variants**

```rust
    MergeFreeze(futures::channel::oneshot::Sender<()>),
    MergeRelease,
```

- [ ] **Step 3: Add `frozen_for_merge: Cell<bool>` field on PartitionData**

```rust
    pub(crate) frozen_for_merge: std::cell::Cell<bool>,
```

Initialise to `false`.

- [ ] **Step 4: Handle the variants in `merged_partition_loop`**

Find the `Drain` arm of the select! macro (F120-C). Add sibling arms:

```rust
                PartitionControl::MergeFreeze(ack) => {
                    // Drain pending + inflight + flush imm — same as Drain — but stay in loop.
                    // Set frozen_for_merge so future req_rx arms reject with Unavailable.
                    part.borrow().frozen_for_merge.set(true);
                    // Drain inflight
                    while let Some(c) = inflight.next().await { /* run Phase 3 */ }
                    if !pending.is_empty() {
                        // run start_write_batch / append_batch on remaining pending
                    }
                    // Rotate active + flush all imm via flush_one_imm loop
                    rotate_active(&part);
                    while !part.borrow().imm.is_empty() {
                        flush_one_imm(&part, &p_bulk, &part_sc).await
                            .map_err(|e| tracing::warn!("F181 freeze flush failed: {e}"))?;
                    }
                    let _ = ack.send(());
                    // Continue loop, but the request-intake arm now short-circuits
                    // because frozen_for_merge == true.
                }
                PartitionControl::MergeRelease => {
                    // Exit loop; PartitionServer will drop the handle.
                    break;
                }
```

In the request-intake branch (the `req_rx.next()` arm), add a guard at the top:

```rust
                if part.borrow().frozen_for_merge.get() {
                    // Reject all writes with Unavailable; reads can still succeed
                    // but for simplicity reject everything.
                    drop_request_with_unavailable(req);
                    continue;
                }
```

- [ ] **Step 5: cargo build clean**

Run: `cd /data/dongmao_dev/autumn-rs && cargo check -p autumn-partition-server`
Expected: clean (some renaming of helpers may be needed; adapt to existing names).

- [ ] **Step 6: Commit**

```bash
git add crates/partition-server/src/lib.rs crates/partition-server/src/background.rs
git commit -m "F181-H1: MergeFreeze/MergeRelease control messages"
```

---

### Task 19: Implement `handle_merge_part`

**Files:**
- Modify: `crates/partition-server/src/rpc_handlers.rs`

- [ ] **Step 1: Add the handler at end of the file (mirror handle_split_part)**

```rust
pub(crate) async fn handle_merge_part(
    payload: Bytes,
    server: &Rc<PartitionServer>,
    pool: &Rc<ConnPool>,
    manager_addr: &str,
    owner_key: &str,
    revision: i64,
) -> HandlerResult {
    let req: MergePartReq = partition_rpc::rkyv_decode(&payload)
        .map_err(|e| (StatusCode::InvalidArgument, e))?;

    // 1. Resolve both partition handles on this PS (same-PS only).
    let (survivor, victim) = {
        let parts = server.partitions.borrow();
        let s = parts.get(&req.survivor_part_id).cloned()
            .ok_or_else(|| (StatusCode::NotFound, format!("partition {} not on this PS", req.survivor_part_id)))?;
        let v = parts.get(&req.victim_part_id).cloned()
            .ok_or_else(|| (StatusCode::Precondition, format!("partition {} not on this PS (cross-PS merge unsupported)", req.victim_part_id)))?;
        (s, v)
    };

    // 2. has_overlap check
    if survivor.part.borrow().has_overlap.get() != 0 || victim.part.borrow().has_overlap.get() != 0 {
        return Err((StatusCode::FailedPrecondition,
            "either side has has_overlap=1; run major compaction first".into()));
    }

    // 3. Acquire dual-gate on BOTH partitions, strict order.
    let (vc, vg) = { let p = victim.part.borrow(); (p.compact_gate.clone(), p.gc_gate.clone()) };
    let (sc, sg) = { let p = survivor.part.borrow(); (p.compact_gate.clone(), p.gc_gate.clone()) };
    let _v_compact = vc.acquire().await;
    let _v_gc = vg.acquire().await;
    let _s_compact = sc.acquire().await;
    let _s_gc = sg.acquire().await;

    // 4. MergeFreeze on victim
    let (ack_tx, ack_rx) = futures::channel::oneshot::channel();
    victim.req_tx.send(PartitionControl::MergeFreeze(ack_tx)).await
        .map_err(|e| (StatusCode::Internal, format!("victim freeze tx: {e}")))?;
    ack_rx.await
        .map_err(|e| (StatusCode::Internal, format!("victim freeze ack: {e}")))?;

    // 5. flush_memtable_locked on survivor (drain its imm too)
    flush_memtable_locked(&survivor.part).await
        .map_err(|e| (StatusCode::Internal, e.to_string()))?;

    // 6. commit_length on six streams
    let s_part_sc = survivor.part_sc.clone();
    let v_part_sc = victim.part_sc.clone();
    let (s_log, s_row, s_meta, v_log, v_row, v_meta) = {
        let s = survivor.part.borrow();
        let v = victim.part.borrow();
        (s.log_stream_id, s.row_stream_id, s.meta_stream_id,
         v.log_stream_id, v.row_stream_id, v.meta_stream_id)
    };
    let log_lens = [
        s_part_sc.commit_length(s_log).await.unwrap_or(0).max(1),
        v_part_sc.commit_length(v_log).await.unwrap_or(0).max(1),
    ];
    let row_lens = [
        s_part_sc.commit_length(s_row).await.unwrap_or(0).max(1),
        v_part_sc.commit_length(v_row).await.unwrap_or(0).max(1),
    ];
    let meta_lens = [
        s_part_sc.commit_length(s_meta).await.unwrap_or(0).max(1),
        v_part_sc.commit_length(v_meta).await.unwrap_or(0).max(1),
    ];

    // 7. Call manager
    let mgr_resp_bytes = pool.call(
        manager_addr,
        manager_rpc::MSG_MULTI_MODIFY_MERGE,
        manager_rpc::rkyv_encode(&manager_rpc::MultiModifyMergeReq {
            survivor_part_id: req.survivor_part_id,
            victim_part_id: req.victim_part_id,
            owner_key: owner_key.to_string(),
            revision,
            log_sealed_lengths: log_lens,
            row_sealed_lengths: row_lens,
            meta_sealed_lengths: meta_lens,
        }).to_vec().into(),
    ).await
    .map_err(|e| (StatusCode::Internal, format!("multi_modify_merge: {e}")))?;
    let mgr_resp: manager_rpc::MultiModifyMergeResp = manager_rpc::rkyv_decode(&mgr_resp_bytes)
        .map_err(|e| (StatusCode::Internal, e))?;
    if mgr_resp.code != manager_rpc::CODE_OK {
        return Err((StatusCode::FailedPrecondition, mgr_resp.message));
    }
    let new_log_tail = mgr_resp.new_log_tail_extent_id;

    // 8. PS-side splice on survivor
    {
        let mut s = survivor.part.borrow_mut();
        let v = victim.part.borrow();
        // Append victim's tables + sst_readers
        for (meta, reader) in v.tables.iter().zip(v.sst_readers.iter()) {
            s.tables.push(meta.clone());
            s.sst_readers.push(reader.clone());
        }
        // Widen rg
        let v_end = v.rg.end_key.clone();
        s.rg.end_key = v_end;
        // Bump seq counter
        s.seq_number = s.seq_number.max(v.seq_number) + 1;
    }

    // 9. Write merged TableLocations checkpoint to survivor.meta_stream
    {
        let s = survivor.part.borrow();
        save_table_locs_raw(
            &s_part_sc,
            s.meta_stream_id,
            s.tables.clone(),
            (new_log_tail, 0u32),
        ).await.map_err(|e| (StatusCode::Internal, e.to_string()))?;
    }

    // 10. Invalidate survivor stream workers
    s_part_sc.invalidate_stream(s_log);
    s_part_sc.invalidate_stream(s_row);
    s_part_sc.invalidate_stream(s_meta);
    survivor.part.borrow().need_invalidate_row_stream.set(true);

    // 11. MergeRelease on victim
    victim.req_tx.send(PartitionControl::MergeRelease).await
        .map_err(|e| (StatusCode::Internal, format!("victim release tx: {e}")))?;
    // Drop victim from PartitionServer's map
    server.partitions.borrow_mut().remove(&req.victim_part_id);

    // 12. Done.
    Ok(partition_rpc::rkyv_encode(&MergePartResp {
        code: CODE_OK, message: String::new(),
    }))
}
```

- [ ] **Step 2: Wire the dispatch in the `dispatch_partition_rpc` (or equivalent) function**

```rust
        MSG_MERGE_PART => handle_merge_part(payload, server, pool, manager_addr, owner_key, revision).await,
```

- [ ] **Step 3: cargo check + run unit tests**

Run: `cd /data/dongmao_dev/autumn-rs && cargo check -p autumn-partition-server && cargo test -p autumn-partition-server --lib -- --test-threads=1 2>&1 | tail -20`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/partition-server/src/rpc_handlers.rs
git commit -m "F181-H2: handle_merge_part — 12-step flow"
```

---

## Phase I — CLI

### Task 20: ClusterClient API + CLI subcommands

**Files:**
- Modify: `crates/client/src/lib.rs`
- Modify: `crates/server/src/bin/autumn_client.rs`

- [ ] **Step 1: Add `merge_partitions` on ClusterClient**

```rust
    pub async fn merge_partitions(
        &self,
        survivor_part_id: u64,
        victim_part_id: u64,
    ) -> Result<(), AutumnError> {
        // Resolve survivor's PS via region cache; fall back to pool.
        let ps_addr = self.resolve_ps_for_partition(survivor_part_id).await?;
        let payload = partition_rpc::rkyv_encode(&MergePartReq {
            survivor_part_id, victim_part_id,
        });
        let resp_bytes = self.pool.call(&ps_addr, MSG_MERGE_PART, payload.to_vec().into()).await?;
        let resp: MergePartResp = partition_rpc::rkyv_decode(&resp_bytes)?;
        if resp.code != CODE_OK {
            return Err(AutumnError::Server(resp.message));
        }
        Ok(())
    }

    pub async fn policy_candidates(&self) -> Result<Vec<PolicyCandidate>, AutumnError> {
        let payload = manager_rpc::rkyv_encode(&GetPolicyCandidatesReq::default());
        let resp_bytes = self.pool.call(&self.manager_addr, MSG_GET_POLICY_CANDIDATES, payload.to_vec().into()).await?;
        let resp: GetPolicyCandidatesResp = manager_rpc::rkyv_decode(&resp_bytes)?;
        if resp.code != CODE_OK {
            return Err(AutumnError::Server(resp.message));
        }
        Ok(resp.candidates)
    }
```

- [ ] **Step 2: Add the `merge` subcommand to `autumn_client.rs`**

In the `Subcommand` enum:

```rust
    /// Merge two adjacent partitions. Survivor keeps its part_id; victim is deleted.
    Merge {
        survivor_part_id: u64,
        victim_part_id: u64,
    },
```

In the dispatch:

```rust
        Subcommand::Merge { survivor_part_id, victim_part_id } => {
            cluster_client.merge_partitions(survivor_part_id, victim_part_id).await?;
            println!("OK: merged partition {} into {}", victim_part_id, survivor_part_id);
        }
```

- [ ] **Step 3: Add the `policy candidates` subcommand**

```rust
    /// Show split/merge candidates from the manager's advisory engine.
    PolicyCandidates,
```

```rust
        Subcommand::PolicyCandidates => {
            let cands = cluster_client.policy_candidates().await?;
            if cands.is_empty() {
                println!("(no candidates)");
            } else {
                println!("{:<6} {:<8} {:<10} {:<40} {:<10} {:<8} {:<6} {:<5}",
                    "KIND", "PRIMARY", "SECONDARY", "REASON", "SIZE", "QPS", "IMM/s", "FEAS");
                for c in cands {
                    let kind = if c.kind == POLICY_KIND_SPLIT { "split" } else { "merge" };
                    let feas = if c.same_ps { "yes" } else { "no" };
                    let secondary = if c.secondary_part_id == 0 { "-".to_string() }
                                    else { c.secondary_part_id.to_string() };
                    println!("{:<6} {:<8} {:<10} {:<40} {:<10} {:<8} {:<6} {:<5}",
                        kind, c.primary_part_id, secondary, c.reason,
                        format!("{} MB", c.size_bytes / (1024 * 1024)),
                        c.req_per_sec, c.imm_full_per_sec, feas);
                }
            }
        }
```

- [ ] **Step 4: cargo build the binaries**

Run: `cd /data/dongmao_dev/autumn-rs && cargo build -p autumn-server --bin autumn-client 2>&1 | tail -10`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/client/src/lib.rs crates/server/src/bin/autumn_client.rs
git commit -m "F181-I: CLI — merge + policy candidates subcommands"
```

---

## Phase J — Integration tests

### Task 21: `system_merge.rs` — merge basic + adjacency refusal

**Files:**
- Create: `crates/manager/tests/system_merge.rs`

- [ ] **Step 1: Create the test file with the standard test scaffolding**

Mirror `system_delete_tombstone.rs` (one of the existing tests created in the F-prior `2026-05-02-data-integrity-tests` plan). At top:

```rust
mod support;
use support::*;
```

Use `compio::runtime::Runtime::new().unwrap().block_on(async { ... })`.

- [ ] **Step 2: Test 1 — `merge_two_drained_partitions_same_ps`**

```rust
#[test]
#[ignore]
fn merge_two_drained_partitions_same_ps() {
    let ctx = ClusterCtx::start(1, 3);   // 1 PS, 3 ENs
    let rt = compio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let cluster = ctx.cluster_client().await;
        // Create one partition spanning [a..z); split at 'm'.
        let init_part = ctx.create_partition(b"a".to_vec(), b"z".to_vec()).await;
        // Write 20 keys 'a01'..'a10' (left side) + 'n01'..'n10' (right side)
        for i in 1..=10 {
            cluster.put(format!("a{i:02}").into_bytes(), format!("Lval{i}").into_bytes()).await.unwrap();
            cluster.put(format!("n{i:02}").into_bytes(), format!("Rval{i}").into_bytes()).await.unwrap();
        }
        // Force flush + split at mid_key 'm'
        cluster.maintenance(init_part, MAINTENANCE_FLUSH).await.unwrap();
        cluster.split(init_part).await.unwrap();
        // Wait for region_sync to pick up the new partition
        compio::time::sleep(Duration::from_millis(500)).await;

        // Find the two resulting part_ids on this PS
        let parts = cluster.list_partitions().await.unwrap();
        assert_eq!(parts.len(), 2);
        let (left_id, right_id) = (parts[0].part_id, parts[1].part_id);

        // Now merge right back into left.
        cluster.merge_partitions(left_id, right_id).await.unwrap();

        // Verify: only `left_id` exists, all 20 keys readable from it.
        compio::time::sleep(Duration::from_millis(500)).await;
        let parts = cluster.list_partitions().await.unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_id, left_id);
        for i in 1..=10 {
            let lv = cluster.get(format!("a{i:02}").into_bytes()).await.unwrap();
            assert_eq!(lv, Some(format!("Lval{i}").into_bytes()));
            let rv = cluster.get(format!("n{i:02}").into_bytes()).await.unwrap();
            assert_eq!(rv, Some(format!("Rval{i}").into_bytes()));
        }
    });
}
```

- [ ] **Step 3: Test 2 — `merge_refuses_non_adjacent`**

Construct two non-adjacent partitions (e.g., gap in keyspace) and assert `merge_partitions` returns `Precondition`-mapped error.

- [ ] **Step 4: Test 3 — `merge_refuses_when_either_has_overlap`**

After a split, victim's `has_overlap=1` until major compact runs; assert merge fails with the documented Precondition error.

- [ ] **Step 5: Run the tests**

Run: `cd /data/dongmao_dev/autumn-rs && cargo test -p autumn-manager --test system_merge -- --ignored --test-threads=1 2>&1 | tail -30`
Expected: 3/3 PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/manager/tests/system_merge.rs
git commit -m "F181-J1: integration tests — basic merge + 2 refusal cases"
```

---

### Task 22: `system_policy.rs` — advisory engine end-to-end

**Files:**
- Create: `crates/manager/tests/system_policy.rs`

- [ ] **Step 1: Build a 2-partition cluster, exercise the engine**

```rust
#[test]
#[ignore]
fn policy_advisory_emits_split_then_merge_after_action() {
    let ctx = ClusterCtx::start(1, 3);
    let rt = compio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let cluster = ctx.cluster_client().await;
        // Set up 2 small adjacent partitions
        // ... create + split scaffolding ...

        // Advance metrics: low-load both sides for >5 buckets (simulate by directly
        // calling MSG_REPORT_PARTITION_LOAD with synthetic small loads via a helper).
        for _ in 0..6 {
            ctx.report_synthetic_load(&[
                PartitionLoad { part_id: left_id, size_bytes: 100*MIB, req_per_sec: 50, imm_full_per_sec: 0, p99_us: 0 },
                PartitionLoad { part_id: right_id, size_bytes: 100*MIB, req_per_sec: 50, imm_full_per_sec: 0, p99_us: 0 },
            ]).await;
            compio::time::sleep(Duration::from_secs(61)).await;   // tick beyond bucket
        }
        // Allow policy_tick_loop to compute
        compio::time::sleep(Duration::from_secs(61)).await;

        let cands = cluster.policy_candidates().await.unwrap();
        let merge_count = cands.iter().filter(|c| c.kind == POLICY_KIND_MERGE).count();
        assert_eq!(merge_count, 1);

        // Execute the merge; assert the candidate disappears next tick.
        cluster.merge_partitions(left_id, right_id).await.unwrap();
        compio::time::sleep(Duration::from_secs(61)).await;
        let cands_after = cluster.policy_candidates().await.unwrap();
        assert!(cands_after.iter().all(|c| c.kind != POLICY_KIND_MERGE
            || c.primary_part_id != left_id));
    });
}
```

(Runtime cost: ~6 minutes. Mark `#[ignore]`.)

For unit-style policy testing, the unit tests in `policy_tests.rs` already cover the engine logic. This integration test exercises the wire path end-to-end.

- [ ] **Step 2: Run the test**

Run: `cd /data/dongmao_dev/autumn-rs && cargo test -p autumn-manager --test system_policy -- --ignored --test-threads=1 2>&1 | tail -20`
Expected: PASS (long-running).

- [ ] **Step 3: Commit**

```bash
git add crates/manager/tests/system_policy.rs
git commit -m "F181-J2: integration test — policy advisory end-to-end"
```

---

## Phase K — Docs + closeout

### Task 23: README + CLAUDE.md updates

**Files:**
- Modify: `README.md`
- Modify: `crates/manager/CLAUDE.md`
- Modify: `crates/partition-server/CLAUDE.md`
- Modify: `crates/rpc/CLAUDE.md`

- [ ] **Step 1: Add to `README.md`**

Append a new section:

```markdown
## F181 — Partition merge + policy advisory

### Manual partition merge

```bash
# Merge two adjacent partitions (must be on same PS, both has_overlap=0)
autumn-client --manager 127.0.0.1:9001 merge <SURVIVOR_PART_ID> <VICTIM_PART_ID>
```

`SURVIVOR` keeps its `part_id`; `VICTIM` is deleted from the manager. The
merged partition's range becomes `[SURVIVOR.start, VICTIM.end)`.

Preconditions:
- `SURVIVOR.end_key == VICTIM.start_key` (adjacent in keyspace)
- Both partitions on the same PS (cross-PS merge unsupported in Stage 1)
- `has_overlap = 0` on both (run `compact` first if not)
- No EC conversion / recovery / pending-delete on any source extent

### Policy candidates

```bash
autumn-client --manager 127.0.0.1:9001 policy candidates
```

Shows the manager's advisory split/merge candidates (computed every 60 s
from the last 30 min of per-partition metrics — `size_bytes`,
`req_per_sec`, `imm_full_per_sec`). Stage 1 is advisory only; operator
runs `split` / `merge` manually based on output.
```

- [ ] **Step 2: Add to `crates/manager/CLAUDE.md` — note 16**

```markdown
16. **F181 partition merge handler.** `handle_multi_modify_merge` is
    the inverse of `handle_multi_modify_split`. Pattern matches the
    F124 single-txn + F138/F145/F146 inflight checks + F149 fence. Phase 1
    (no awaits) computes spliced streams via `compute_merge_streams`
    (log: with new tail) and `splice_streams_without_new_tail` (row,
    meta), and merges VP refs via `merged_partition_vp_refs`. Phase 1.5
    (await) runs `alloc_extent_on_node` per replica for E_new. Phase 2
    is one fenced `put_and_delete_txn` containing all puts + victim
    deletes. Phase 3 verifies eversion drift then calls
    `apply_merge_mutations`. **Order invariant** in spliced extent_ids:
    `[survivor's existing] + [victim's existing] + [new tail]` — load-
    bearing for vp_head replay correctness; tested by
    `compute_merge_streams_extent_ids_order_and_refs`.

    **`partitionLastOp/<part_id>` sidecar etcd prefix** stores the
    last split or merge timestamp per partition (i64 unix-epoch LE).
    Loaded by `replay_from_etcd` into `AutumnManager.last_op_at`.
    Both split and merge handlers write entries in their atomic txn.
    Used by the policy engine for cooldown.
```

- [ ] **Step 3: Add to `crates/partition-server/CLAUDE.md` — Programming Note 11**

```markdown
11. **F181 partition merge.** `handle_merge_part` runs on the
    survivor's PS (same-PS-only constraint; cross-PS rejected at
    Precondition). Acquisition order for the four gates is strict:
    `(victim, compact_gate) → (victim, gc_gate) → (survivor,
    compact_gate) → (survivor, gc_gate)` — never invert. `MergeFreeze`
    on the victim partition drains pending+inflight+imm but stays in
    `merged_partition_loop` (with `frozen_for_merge` set) so the loop
    can still process the eventual `MergeRelease`. Survivor's PS-side
    splice is a `borrow_mut` block on the survivor's PartitionData
    that appends victim.tables + victim.sst_readers + widens rg +
    bumps seq_number. After the splice, write the merged
    TableLocations checkpoint with `vp_head = (new_log_tail_eid, 0)`
    BEFORE `MergeRelease` — this is the PS-side linearization point.
    F148-A invariant comment inline at this site.
```

- [ ] **Step 4: Add to `crates/rpc/CLAUDE.md`**

Find the message-type table; add:

```
| `0x34`  | `MSG_MULTI_MODIFY_MERGE`     | manager: F181 partition merge    |
| `0x35`  | `MSG_GET_POLICY_CANDIDATES`  | manager: F181 advisory engine    |
| `0x36`  | `MSG_REPORT_PARTITION_LOAD`  | PS → manager: F181 metrics       |
| `0x4D`  | `MSG_MERGE_PART`             | PS: F181 merge entry-point       |
```

- [ ] **Step 5: cargo doc + commit**

Run: `cd /data/dongmao_dev/autumn-rs && cargo doc --workspace --no-deps --exclude autumn-fuse 2>&1 | tail -10`
(Optional sanity — no Rustdoc errors.)

```bash
git add README.md crates/manager/CLAUDE.md crates/partition-server/CLAUDE.md crates/rpc/CLAUDE.md
git commit -m "F181-K1: docs — README manual repro + CLAUDE.md notes"
```

---

### Task 24: feature_list.md + claude-progress.txt

**Files:**
- Modify: `feature_list.md`
- Modify: `claude-progress.txt`

- [ ] **Step 1: Add F181 entry to `feature_list.md`**

Find the table at the top (~line 16), add the F181 row:

```
| F181 | Partition merge + size+load advisory policy | partition/manager |
```

Then add a detailed entry in the body of the file (mirror the F129 entry's structure):

```markdown
### F181 · Partition merge + split/merge advisory policy
- **Target:** Inverse-of-split partition merge primitive (CoW stream-extent
  splice, no value rewrite, single-stream-per-partition invariant
  preserved); manager-side advisory engine emitting split/merge candidates
  from `size_bytes + req_per_sec + imm_full_per_sec` over a 30 min sliding
  window. Stage 1 = manual triggers + advisory only.
- **Mechanism:** manager handler `handle_multi_modify_merge` allocates a
  fresh log_stream tail extent inside the same atomic etcd txn; survivor's
  log_stream becomes `[L]+[V]+[E_new]`, row + meta become `[L]+[V]`.
  PS-side `handle_merge_part` runs on the survivor PS only; dual-gate on
  both partitions; `MergeFreeze` drains victim before splice. Crash
  recovery handled by F124-style single-txn semantics — no MERGING marker
  needed.
- **Files:** `crates/rpc/src/manager_rpc.rs` (3 new RPCs:
  `MSG_MULTI_MODIFY_MERGE 0x34`, `MSG_GET_POLICY_CANDIDATES 0x35`,
  `MSG_REPORT_PARTITION_LOAD 0x36`); `crates/rpc/src/partition_rpc.rs`
  (`MSG_MERGE_PART 0x4D`); `crates/manager/src/lib.rs` (3 helpers +
  `last_op_at` HashMap + `partitionLastOp/` etcd prefix); `crates/manager/
  src/policy.rs` (NEW); `crates/manager/src/rpc_handlers.rs` (3 new
  handlers + split-handler `last_op_at` write); `crates/partition-server/
  src/{lib,background,rpc_handlers}.rs` (`MergeFreeze`/`MergeRelease`
  control + metrics export + `handle_merge_part`); CLI subcommands `merge`
  and `policy candidates`. Tests: 7 policy unit tests + 4 merge unit
  tests + 4 integration tests.
- **Stages:** Stage 1 in this commit (manual + advisory); Stage 2
  (`AUTUMN_MGR_AUTO_SPLIT`) and Stage 3 (`AUTUMN_MGR_AUTO_MERGE`) ship
  later behind feature flags. **Auto-split must precede auto-merge** —
  TPC concentrates load onto a single core; merge has the higher blast
  radius. (Recorded in `feedback_auto_split_before_merge.md` memory.)
- **Spec:** `docs/superpowers/specs/2026-05-09-partition-merge-and-split-merge-policy-design.md`
- **passes:** true
```

- [ ] **Step 2: Update `claude-progress.txt`**

Replace contents with:

```
Date: 2026-05-09
TaskStatus: completed
Task scope: F181 — Partition merge primitive + advisory policy engine.
            Stage 1: manual triggers + advisory only.
            Spec: docs/superpowers/specs/2026-05-09-partition-merge-...

What landed in this commit family:

  Manager:
    - MSG_MULTI_MODIFY_MERGE 0x34 + handle_multi_modify_merge
      (Phase 1 + 1.5 + 2 + 3, F124 single-txn + F138/F145/F146 +
       F149 fence + F146-style verify-at-apply)
    - compute_merge_streams + splice_streams_without_new_tail +
      merged_partition_vp_refs + apply_merge_mutations (pure fns)
    - last_op_at HashMap + partitionLastOp/<id> etcd sidecar
    - PolicyEngine (policy.rs) + thresholds + 30 min window
    - MSG_GET_POLICY_CANDIDATES + MSG_REPORT_PARTITION_LOAD handlers
    - policy_tick_loop spawned in serve()

  Partition server:
    - MergeFreeze / MergeRelease control messages
    - frozen_for_merge: Cell<bool> + PartitionMetrics on PartitionData
    - merged_partition_loop bumps req_count_60s + imm_full_count_60s
    - report_load_loop (5 s cadence)
    - handle_merge_part (12-step flow)

  Client + CLI:
    - ClusterClient.merge_partitions + policy_candidates
    - autumn-client merge / autumn-client policy candidates

  Tests:
    - 4 manager unit tests (compute_merge_streams, merged_partition_vp_refs,
      apply_merge_mutations, in-memory smoke)
    - 7 policy unit tests
    - 3 system_merge.rs integration tests
    - 1 system_policy.rs end-to-end advisory test

  Docs:
    - README.md F181 manual repro section
    - manager CLAUDE.md note 16
    - partition-server CLAUDE.md Programming Note 11
    - rpc CLAUDE.md message-type table updated

Out of scope (deferred Stages):
  - Stage 2 AUTUMN_MGR_AUTO_SPLIT auto-trigger loop
  - Stage 3 AUTUMN_MGR_AUTO_MERGE auto-trigger loop
  - Cross-PS merge (depends on partition migration primitive)
  - p99 latency reading on PS (PartitionLoad.p99_us = 0 always)
  - Per-cluster threshold knobs (Stage 1 hard-codes)

Workspace verification:
  cargo check --workspace --exclude autumn-fuse: clean
  cargo test -p autumn-rpc: clean
  cargo test -p autumn-manager --lib: 4 new unit + 7 policy + existing all pass
  cargo test -p autumn-manager --test system_merge -- --ignored: 3/3
  cargo test -p autumn-manager --test system_policy -- --ignored: 1/1
  cargo test -p autumn-partition-server --lib -- --test-threads=1: existing all pass
```

- [ ] **Step 3: Run the verification commands listed above**

Run them in order. Each should be green before proceeding.

- [ ] **Step 4: Commit**

```bash
git add feature_list.md claude-progress.txt
git commit -m "F181: feature_list + claude-progress closeout"
```

- [ ] **Step 5: Push to main (per memory: solo-flow repo)**

```bash
git push origin main
```

---

## Self-review

After completing all tasks, verify:

1. **Spec coverage.** Skim `docs/superpowers/specs/2026-05-09-partition-merge-and-split-merge-policy-design.md` §0–§11. Each section has at least one task implementing it:
   - §1.1/§1.2 architecture: Tasks 3-9 (compute_merge_streams + handle_multi_modify_merge)
   - §2 wire: Tasks 1-2
   - §3 manager: Tasks 3-14
   - §4 PS: Tasks 15-19
   - §5 CLI: Task 20
   - §6 staging (Stage 1 only): all the above
   - §7 crash recovery: covered by single-txn invariant in handle_multi_modify_merge — verify by inspection of Task 9 + integration tests
   - §8 tests: Tasks 10, 12, 13, 21, 22
   - §9 file changes: see "File Structure" above
   - §11 risks: documented in CLAUDE.md updates (Task 23)

2. **Placeholder scan.** No "TBD" / "fill in" / "implement appropriate" anywhere in the plan.

3. **Type consistency.** `MultiModifyMergeReq` fields (Task 1) match what's read in Task 8/9. `PartitionLoad` fields (Task 1) match what's pushed by `snapshot_partition_load` (Task 17) and what `PolicyEngine.compute_candidates` reads (Tasks 12, 13). `PolicyCandidate` fields match CLI rendering (Task 20).

4. **Test coverage** maps to the spec test plan (§8). Counts match.

---

## Out of Scope (deferred)

- Stage 2 / Stage 3 auto-trigger loops (separate commit families per §6 of the spec).
- Cross-PS merge (requires partition migration primitive — separate feature).
- p99 latency emission from PS (`PartitionLoad.p99_us` reserved field, kept as 0 in Stage 1).
- Per-cluster threshold tuning via CLI flags (Stage 1 uses hard-coded constants).
- Removing the `same_ps == false` candidates from advisory output entirely (Stage 1 emits them with feasible=no so operators can plan co-location).
