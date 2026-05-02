# Data Integrity Integration Test Suite — Design Spec

**Date:** 2026-05-02
**Scope:** Comprehensive integration tests for autumn-rs covering crash recovery, split, restart, delete, range scan, VP lifetime, and concurrent writes.

---

## 1. Infrastructure Changes

### 1.1 New Helpers in `support/mod.rs`

Add 6 RPC helpers matching existing `ps_put`/`psr_put` pattern:

| Helper | RPC | Returns |
|--------|-----|---------|
| `ps_delete(ps, part_id, key)` | `MSG_DELETE` | `DeleteResp` |
| `psr_delete(router, part_id, key)` | `MSG_DELETE` | `DeleteResp` |
| `ps_head(ps, part_id, key)` | `MSG_HEAD` | `HeadResp` |
| `psr_head(router, part_id, key)` | `MSG_HEAD` | `HeadResp` |
| `ps_range(ps, part_id, prefix, start, limit)` | `MSG_RANGE` | `RangeResp` |
| `psr_range(router, part_id, prefix, start, limit)` | `MSG_RANGE` | `RangeResp` |

Add 2 bulk utility functions:

| Helper | Purpose |
|--------|---------|
| `write_sequential_keys(ps, part_id, prefix, count)` | Write `{prefix}-{i:03}` keys with deterministic values (`val-{prefix}-{i:03}`), return key list |
| `verify_sequential_keys(ps, part_id, keys)` | Verify all keys exist with correct values via `ps_get`, return count verified |
| `verify_sequential_keys_routed(router, part_id, keys)` | Same as above but via `psr_get` for post-split partitions |

### 1.2 No Production Code Changes

All tests exercise existing RPCs and failure modes. No changes to `autumn-partition-server`, `autumn-stream`, or `autumn-manager`.

---

## 2. Test Gating

Tests are split into two tiers using `#[ignore]`:

- **Focused tests (no flag):** 10 tests, ~2 min total. Single-invariant, <15s each. Run with `cargo test -p autumn-manager`.
- **Compound tests (`#[ignore]`):** 8 tests, ~3 min total. Multi-step chains, 15-25s each. Run with `cargo test -p autumn-manager -- --ignored`.

---

## 3. Test Files

### 3.1 `system_delete_tombstone.rs`

**Test 1: `delete_survives_crash_and_recovery`** (focused, ~10s)
- Write 10 keys -> flush -> delete 5 -> crash PS (drop)
- New PS opens, replays logStream
- Verify: 5 deleted keys not-found via `ps_head`, 5 remaining keys correct values
- **Invariant:** tombstones in logStream WAL are replayed correctly on recovery

**Test 2: `delete_compaction_removes_tombstones`** (focused, ~12s)
- Write 20 keys -> flush -> delete 10 -> flush (tombstones in SSTable)
- Major compaction -> wait -> crash PS -> new PS opens
- Verify: deleted keys gone, surviving keys intact
- Range scan returns exactly 10 entries
- **Invariant:** compaction physically removes tombstoned entries; range scan is consistent

**Test 3: `delete_before_split_correct_in_both_children`** (`#[ignore]`, ~15s)
- Write 20 keys across `[a, z)` -> flush -> delete 10 (mix of left/right) -> flush
- Split -> verify via router: deleted keys not-found, surviving keys correct in each child
- Compact both children -> re-verify
- **Invariant:** tombstones correctly partitioned by split and honored by both children

### 3.2 `system_range_split.rs`

**Test 1: `range_scan_returns_correct_results_after_split`** (focused, ~12s)
- Write 30 keys -> flush -> split
- Range scan left child (all keys) -> range scan right child (all keys)
- Union = full 30 keys, no duplicates, no gaps
- **Invariant:** split correctly partitions range scan results

**Test 2: `range_scan_with_limit_and_pagination_after_split`** (focused, ~12s)
- Write 30 keys -> flush -> split
- Paginated range scan on right child: `limit=5`, continue with `start=last_key+\x00`
- Collected entries = full right-child key set
- **Invariant:** pagination works correctly within a post-split child

**Test 3: `range_scan_consistent_after_split_compact_split`** (`#[ignore]`, ~20s)
- Write 50 keys -> flush -> split -> compact both
- Split left child again -> compact -> 3 partitions total
- Range scan all 3 partitions, collect all entries
- Exactly 50 unique keys, correct values, sorted within each
- **Invariant:** multiple sequential splits preserve range scan completeness

### 3.3 `system_crash_mid_flush.rs`

**Test 1: `crash_during_flush_unflushed_data_recoverable`** (focused, ~10s)
- Write 50 keys (enough for rotation) -> immediately drop PS
- New PS opens, replays logStream
- All 50 keys readable
- **Invariant:** logStream is authoritative WAL; interrupted flush doesn't lose data

**Test 2: `crash_mid_flush_no_orphan_corruption`** (`#[ignore]`, ~15s)
- Write 30 keys -> flush (completes) -> write 30 more -> trigger flush via rotation
- Sleep 100ms (flush begins) -> drop PS
- New PS: first 30 present (checkpointed), second 30 present (logStream replay)
- Compact -> verify again (no orphan SSTable corruption)
- **Invariant:** meta_stream checkpoint is atomic commit point; orphan row_stream appends are invisible and harmless

### 3.4 `system_crash_mid_compact.rs`

**Test 1: `crash_during_compaction_no_data_loss`** (focused, ~15s)
- Write 20 keys -> flush -> write 20 more -> flush (2 SSTables)
- Trigger compaction -> sleep 100ms -> drop PS
- New PS recovers -> all 40 keys readable
- **Invariant:** compaction's atomic commit is `save_table_locs_raw` to meta_stream; crash before that preserves pre-compaction state

**Test 2: `crash_during_compaction_then_successful_compact`** (`#[ignore]`, ~18s)
- Write 30 keys -> flush -> delete 15 -> flush -> write 15 new -> flush (3 SSTables)
- Trigger compaction -> sleep 100ms -> drop PS
- New PS recovers -> verify data
- Trigger compaction again -> must succeed cleanly
- After second compaction: 30 keys total (15 original survivors + 15 new), 15 deleted gone
- **Invariant:** crashed compaction leaves partition in a state where next compaction succeeds

### 3.5 `system_multi_split_chain.rs`

**Test 1: `split_compact_split_preserves_all_data`** (focused, ~20s)
- Write 60 keys -> flush -> split -> compact both children
- Split left child again -> compact all 3 partitions
- Verify all 60 keys readable from correct partitions
- **Invariant:** two levels of split + compaction lose no data

**Test 2: `split_chain_with_writes_between_splits`** (`#[ignore]`, ~25s)
- Write 20 keys -> flush -> split
- Write 10 new to left, 10 new to right -> flush both -> compact both
- Split right child again -> write 5 to newest -> flush -> compact all 3
- Crash PS -> new PS opens
- Verify all 45 keys correct across 3 partitions
- **Invariant:** interleaved writes between splits preserved through crash recovery

### 3.6 `system_vp_after_split_gc.rs`

**Test 1: `vp_resolvable_after_split_and_one_child_gc`** (focused, ~18s)
- Write 10 large-value keys (>4KB, VP path) -> flush
- Split -> compact right child -> GC right child
- Verify left child resolves all large values; right child also correct
- **Invariant:** `vp_table_refs` protects shared log extents; GC on one child can't break sibling's VP resolution

**Test 2: `vp_freed_after_both_children_compact_and_gc`** (`#[ignore]`, ~22s)
- Write 10 large-value keys -> flush -> note log extent IDs
- Split -> compact both -> write small key to each child -> flush both
- GC both -> check shared log extent refs via `get_extent_info`
- If exists: refs decreased. If deleted: ideal outcome.
- Both children still serve all data
- **Invariant:** after both children compact + GC, shared log extents are eligible for deletion

### 3.7 `system_bulk_verification.rs`

**Test 1: `bulk_write_crash_restart_all_data_intact`** (focused, ~15s)
- Write 200 keys: 100 small (<4KB), 100 large (>4KB VP)
- Flush -> crash PS -> new PS recovers
- All 200 keys correct via `ps_get`
- Range scan returns exactly 200 entries, sorted
- **Invariant:** recovery at scale handles both inline and VP entries

**Test 2: `bulk_mixed_ops_split_restart_verify`** (`#[ignore]`, ~25s)
- Write 100 keys (mixed) -> flush -> delete 20 -> write 30 new -> flush
- Split -> compact both -> write 20 more to each child -> flush both
- Crash PS -> new PS opens
- Verify: 80 original survivors, 20 deleted not-found, 30 post-delete keys, 40 post-split keys
- Range scan union = exactly 150 entries
- **Invariant:** full lifecycle (write, delete, flush, split, compact, write-to-children, crash, recover) preserves integrity at scale

### 3.8 `system_concurrent_write_crash.rs`

**Test 1: `concurrent_writers_crash_no_data_loss`** (focused, ~12s)
- 5 async tasks, each writing 20 keys with distinct prefixes, `must_sync=true`
- All 100 writes complete -> do NOT flush -> crash PS
- New PS replays from logStream
- Every key that got a successful `PutResp` is readable
- **Invariant:** concurrent acknowledged writes are durable via logStream replay

**Test 2: `concurrent_writers_during_split`** (`#[ignore]`, ~18s)
- Write 50 keys -> flush
- Start split in one task; concurrently write 20 more from another task
- Wait for split completion
- All 50 pre-split keys in correct children
- Each acknowledged concurrent key readable from exactly one child
- Compact both -> re-verify
- **Invariant:** writes concurrent with split are not lost; each lands in exactly one child

---

## 4. Summary

| File | Focused | Compound (`#[ignore]`) | Total |
|------|---------|------------------------|-------|
| `system_delete_tombstone.rs` | 2 | 1 | 3 |
| `system_range_split.rs` | 2 | 1 | 3 |
| `system_crash_mid_flush.rs` | 1 | 1 | 2 |
| `system_crash_mid_compact.rs` | 1 | 1 | 2 |
| `system_multi_split_chain.rs` | 1 | 1 | 2 |
| `system_vp_after_split_gc.rs` | 1 | 1 | 2 |
| `system_bulk_verification.rs` | 1 | 1 | 2 |
| `system_concurrent_write_crash.rs` | 1 | 1 | 2 |
| **Total** | **10** | **8** | **18** |

**Running:**
```bash
# Focused tests only (~2 min)
cargo test -p autumn-manager --test 'system_delete*' --test 'system_range*' --test 'system_crash*' --test 'system_multi*' --test 'system_vp*' --test 'system_bulk*' --test 'system_concurrent*'

# Compound tests only (~3 min)
cargo test -p autumn-manager -- --ignored

# All tests (~5 min)
cargo test -p autumn-manager -- --include-ignored
```
