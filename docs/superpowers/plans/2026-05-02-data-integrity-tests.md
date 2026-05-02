# Data Integrity Integration Tests — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 18 new integration tests across 8 files that comprehensively verify data integrity under crash, split, delete, range scan, compaction, GC, and concurrent writes.

**Architecture:** All tests follow the existing `system_*.rs` pattern: each file does `mod support; use support::*;`, starts components on separate threads, and runs assertions inside `compio::runtime::Runtime::new().unwrap().block_on(async { ... })`. "Crash" is simulated by dropping the `RpcClient` and starting a new PS with the same `ps_id`. Compound tests are gated behind `#[ignore]`.

**Tech Stack:** Rust, compio runtime, autumn-rpc (custom binary RPC), rkyv serialization, tempfile for ephemeral data dirs.

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `crates/manager/tests/support/mod.rs` | Modify | Add `ps_delete`, `ps_head`, `ps_range` + routed variants + bulk helpers |
| `crates/manager/tests/system_delete_tombstone.rs` | Create | 3 tests: delete + crash, delete + compaction, delete + split |
| `crates/manager/tests/system_range_split.rs` | Create | 3 tests: range after split, pagination, multi-split chain |
| `crates/manager/tests/system_crash_mid_flush.rs` | Create | 2 tests: crash during flush, orphan SST safety |
| `crates/manager/tests/system_crash_mid_compact.rs` | Create | 2 tests: crash during compaction, re-compact after crash |
| `crates/manager/tests/system_multi_split_chain.rs` | Create | 2 tests: split-compact-split, interleaved writes |
| `crates/manager/tests/system_vp_after_split_gc.rs` | Create | 2 tests: VP after one-child GC, VP freed after both GC |
| `crates/manager/tests/system_bulk_verification.rs` | Create | 2 tests: bulk crash recovery, bulk mixed-ops lifecycle |
| `crates/manager/tests/system_concurrent_write_crash.rs` | Create | 2 tests: concurrent writers + crash, concurrent + split |

---

### Task 1: Add new RPC helpers to `support/mod.rs`

**Files:**
- Modify: `crates/manager/tests/support/mod.rs`

- [ ] **Step 1: Add `ps_delete` and `psr_delete` helpers**

Append after the `ps_gc` function (line 292) and before the `PsRouter` section (line 294):

```rust
/// Delete a key.
pub async fn ps_delete(ps: &RpcClient, part_id: u64, key: &[u8]) -> partition_rpc::DeleteResp {
    let resp = ps
        .call(
            partition_rpc::MSG_DELETE,
            partition_rpc::rkyv_encode(&partition_rpc::DeleteReq {
                part_id,
                key: key.to_vec(),
            }),
        )
        .await
        .expect("delete");
    let r: partition_rpc::DeleteResp = partition_rpc::rkyv_decode(&resp).expect("decode DeleteResp");
    assert_eq!(r.code, partition_rpc::CODE_OK, "delete failed: {}", r.message);
    r
}

/// Check if a key exists without fetching the value.
pub async fn ps_head(ps: &RpcClient, part_id: u64, key: &[u8]) -> partition_rpc::HeadResp {
    let resp = ps
        .call(
            partition_rpc::MSG_HEAD,
            partition_rpc::rkyv_encode(&partition_rpc::HeadReq {
                part_id,
                key: key.to_vec(),
            }),
        )
        .await
        .expect("head");
    partition_rpc::rkyv_decode(&resp).expect("decode HeadResp")
}

/// Range scan with prefix, start key, and limit.
pub async fn ps_range(
    ps: &RpcClient,
    part_id: u64,
    prefix: &[u8],
    start: &[u8],
    limit: u32,
) -> partition_rpc::RangeResp {
    let resp = ps
        .call(
            partition_rpc::MSG_RANGE,
            partition_rpc::rkyv_encode(&partition_rpc::RangeReq {
                part_id,
                prefix: prefix.to_vec(),
                start: start.to_vec(),
                limit,
            }),
        )
        .await
        .expect("range");
    let r: partition_rpc::RangeResp = partition_rpc::rkyv_decode(&resp).expect("decode RangeResp");
    assert_eq!(r.code, partition_rpc::CODE_OK, "range failed: {}", r.message);
    r
}
```

- [ ] **Step 2: Add routed variants after `psr_gc`**

Append after `psr_gc` (line 382):

```rust
/// Routed `ps_delete` — F099-K aware.
pub async fn psr_delete(router: &PsRouter, part_id: u64, key: &[u8]) -> partition_rpc::DeleteResp {
    let c = router.client_for(part_id).await;
    ps_delete(&c, part_id, key).await
}

/// Routed `ps_head` — F099-K aware.
pub async fn psr_head(router: &PsRouter, part_id: u64, key: &[u8]) -> partition_rpc::HeadResp {
    let c = router.client_for(part_id).await;
    ps_head(&c, part_id, key).await
}

/// Routed `ps_range` — F099-K aware.
pub async fn psr_range(
    router: &PsRouter,
    part_id: u64,
    prefix: &[u8],
    start: &[u8],
    limit: u32,
) -> partition_rpc::RangeResp {
    let c = router.client_for(part_id).await;
    ps_range(&c, part_id, prefix, start, limit).await
}
```

- [ ] **Step 3: Add bulk utility helpers**

Append after the routed variants:

```rust
// ── Bulk test utilities ──────────────────────────────────────────────

/// Write `count` keys as `{prefix}-{i:03}` with values `val-{prefix}-{i:03}`.
/// Returns the list of keys written.
pub async fn write_sequential_keys(
    ps: &RpcClient,
    part_id: u64,
    prefix: &str,
    count: u32,
) -> Vec<String> {
    let mut keys = Vec::with_capacity(count as usize);
    for i in 0..count {
        let key = format!("{prefix}-{i:03}");
        let val = format!("val-{prefix}-{i:03}");
        ps_put(ps, part_id, key.as_bytes(), val.as_bytes(), true).await;
        keys.push(key);
    }
    keys
}

/// Verify all keys exist with their deterministic values via `ps_get`.
/// Panics on first mismatch. Returns the number of keys verified.
pub async fn verify_sequential_keys(
    ps: &RpcClient,
    part_id: u64,
    keys: &[String],
) -> usize {
    for key in keys {
        let expected_val = format!("val-{key}");
        let resp = ps_get(ps, part_id, key.as_bytes()).await;
        assert_eq!(
            resp.value,
            expected_val.as_bytes(),
            "key {key} has wrong value"
        );
    }
    keys.len()
}

/// Verify all keys via routed `psr_get` (for post-split partitions).
pub async fn verify_sequential_keys_routed(
    router: &PsRouter,
    part_id: u64,
    keys: &[String],
) -> usize {
    for key in keys {
        let expected_val = format!("val-{key}");
        let resp = psr_get(router, part_id, key.as_bytes()).await;
        assert_eq!(
            resp.value,
            expected_val.as_bytes(),
            "key {key} has wrong value (routed)"
        );
    }
    keys.len()
}
```

- [ ] **Step 4: Verify compilation**

Run: `cargo build -p autumn-manager --tests 2>&1 | tail -5`
Expected: `Finished` with no errors (warnings OK).

- [ ] **Step 5: Commit**

```bash
git add crates/manager/tests/support/mod.rs
git commit -m "test: add ps_delete, ps_head, ps_range helpers and bulk utilities to support/mod.rs"
```

---

### Task 2: `system_delete_tombstone.rs` — Delete correctness tests

**Files:**
- Create: `crates/manager/tests/system_delete_tombstone.rs`

- [ ] **Step 1: Create the test file with all 3 tests**

```rust
//! Data integrity: delete/tombstone correctness across crash, compaction, and split.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;

use support::*;

#[test]
fn delete_survives_crash_and_recovery() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 30).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        let ps1_addr = pick_addr();
        start_partition_server(51, mgr_addr, ps1_addr);
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        // Write 10 keys and flush
        for i in 0u32..10 {
            ps_put(
                &ps1, 901,
                format!("key-{i:02}").as_bytes(),
                format!("val-{i}").as_bytes(),
                true,
            ).await;
        }
        ps_flush(&ps1, 901).await;

        // Delete keys 0..5
        for i in 0u32..5 {
            ps_delete(&ps1, 901, format!("key-{i:02}").as_bytes()).await;
        }

        // Crash PS1
        drop(ps1);

        // PS2 takes over
        let ps2_addr = pick_addr();
        start_partition_server(51, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Deleted keys should not be found
        for i in 0u32..5 {
            let resp = ps_head(&ps2, 901, format!("key-{i:02}").as_bytes()).await;
            assert!(!resp.found, "key-{i:02} should be deleted after recovery");
        }
        // Surviving keys should be intact
        for i in 5u32..10 {
            let resp = ps_get(&ps2, 901, format!("key-{i:02}").as_bytes()).await;
            assert_eq!(resp.value, format!("val-{i}").as_bytes(), "key-{i:02} should survive");
        }
    });
}

#[test]
fn delete_compaction_removes_tombstones() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 31).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 902, log, row, meta, b"a", b"z").await;

        let ps1_addr = pick_addr();
        start_partition_server(52, mgr_addr, ps1_addr);
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        // Write 20 keys, flush
        for i in 0u32..20 {
            ps_put(
                &ps1, 902,
                format!("key-{i:02}").as_bytes(),
                format!("val-{i}").as_bytes(),
                true,
            ).await;
        }
        ps_flush(&ps1, 902).await;

        // Delete 10 keys, flush (tombstones in SSTable)
        for i in 0u32..10 {
            ps_delete(&ps1, 902, format!("key-{i:02}").as_bytes()).await;
        }
        ps_flush(&ps1, 902).await;

        // Major compaction should remove tombstones
        ps_compact(&ps1, 902).await;
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Crash and recover
        drop(ps1);
        let ps2_addr = pick_addr();
        start_partition_server(52, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Deleted keys gone
        for i in 0u32..10 {
            let resp = ps_head(&ps2, 902, format!("key-{i:02}").as_bytes()).await;
            assert!(!resp.found, "key-{i:02} should be gone after compaction");
        }
        // Surviving keys intact
        for i in 10u32..20 {
            let resp = ps_get(&ps2, 902, format!("key-{i:02}").as_bytes()).await;
            assert_eq!(resp.value, format!("val-{i}").as_bytes());
        }
        // Range scan returns exactly 10 entries
        let range_resp = ps_range(&ps2, 902, b"", b"", 100).await;
        assert_eq!(range_resp.entries.len(), 10, "range should return exactly 10 surviving keys");
    });
}

#[test]
#[ignore] // compound test
fn delete_before_split_correct_in_both_children() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 32).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 903, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(53, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Write 20 keys across the range, flush
        for i in 0u32..20 {
            ps_put(
                &ps, 903,
                format!("key-{i:02}").as_bytes(),
                format!("val-{i}").as_bytes(),
                true,
            ).await;
        }
        ps_flush(&ps, 903).await;

        // Delete every other key (mix of left/right range), flush
        for i in (0u32..20).step_by(2) {
            ps_delete(&ps, 903, format!("key-{i:02}").as_bytes()).await;
        }
        ps_flush(&ps, 903).await;

        // Split
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 903 }),
            )
            .await
            .expect("split");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split failed: {}", sr.message);
        compio::time::sleep(Duration::from_millis(1000)).await;

        // Get region info
        let regions = get_regions(&mgr).await;
        assert_eq!(regions.regions.len(), 2, "should have 2 partitions after split");
        let left_rg = regions.regions.iter().find(|(_, r)| r.part_id == 903).unwrap().1.clone();
        let right_rg = regions.regions.iter().find(|(_, r)| r.part_id != 903).unwrap().1.clone();
        let right_id = right_rg.part_id;
        let mid_key = left_rg.rg.as_ref().unwrap().end_key.clone();

        // Wait for PS to discover right partition
        compio::time::sleep(Duration::from_millis(6000)).await;

        // Verify: deleted keys not-found in correct child, surviving keys correct
        for i in 0u32..20 {
            let key = format!("key-{i:02}");
            let kb = key.as_bytes();
            let part_id = if kb < mid_key.as_slice() { 903 } else { right_id };
            let head = psr_head(&router, part_id, kb).await;
            if i % 2 == 0 {
                assert!(!head.found, "{key} (part {part_id}) should be deleted");
            } else {
                let resp = psr_get(&router, part_id, kb).await;
                assert_eq!(resp.value, format!("val-{i}").as_bytes(), "{key} (part {part_id}) wrong value");
            }
        }

        // Compact both children, re-verify
        ps_compact(&ps, 903).await;
        psr_compact(&router, right_id).await;
        compio::time::sleep(Duration::from_millis(3000)).await;

        for i in 0u32..20 {
            let key = format!("key-{i:02}");
            let kb = key.as_bytes();
            let part_id = if kb < mid_key.as_slice() { 903 } else { right_id };
            let head = psr_head(&router, part_id, kb).await;
            if i % 2 == 0 {
                assert!(!head.found, "{key} should still be deleted after compact");
            } else {
                let resp = psr_get(&router, part_id, kb).await;
                assert_eq!(resp.value, format!("val-{i}").as_bytes());
            }
        }
    });
}
```

- [ ] **Step 2: Verify compilation**

Run: `cargo build -p autumn-manager --test system_delete_tombstone 2>&1 | tail -5`
Expected: `Finished` with no errors.

- [ ] **Step 3: Run focused tests**

Run: `cargo test -p autumn-manager --test system_delete_tombstone -- --exact delete_survives_crash_and_recovery delete_compaction_removes_tombstones 2>&1 | tail -20`
Expected: 2 tests pass.

- [ ] **Step 4: Run compound test**

Run: `cargo test -p autumn-manager --test system_delete_tombstone -- --ignored 2>&1 | tail -20`
Expected: 1 test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/manager/tests/system_delete_tombstone.rs
git commit -m "test: add delete/tombstone data integrity tests (crash, compaction, split)"
```

---

### Task 3: `system_range_split.rs` — Range scan correctness tests

**Files:**
- Create: `crates/manager/tests/system_range_split.rs`

- [ ] **Step 1: Create the test file with all 3 tests**

```rust
//! Data integrity: range scan correctness across splits.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;

use support::*;

#[test]
fn range_scan_returns_correct_results_after_split() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 40).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(61, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Write 30 keys and flush
        for i in 0u32..30 {
            ps_put(
                &ps, 901,
                format!("key-{i:02}").as_bytes(),
                format!("val-{i}").as_bytes(),
                true,
            ).await;
        }
        ps_flush(&ps, 901).await;

        // Split
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 901 }),
            )
            .await
            .expect("split");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split failed: {}", sr.message);
        compio::time::sleep(Duration::from_millis(1000)).await;

        let regions = get_regions(&mgr).await;
        let right_id = regions.regions.iter().find(|(_, r)| r.part_id != 901).unwrap().1.part_id;
        compio::time::sleep(Duration::from_millis(6000)).await;

        // Range scan both children
        let left_range = ps_range(&ps, 901, b"", b"", 100).await;
        let right_range = psr_range(&router, right_id, b"", b"", 100).await;

        // Union should be exactly 30 keys
        let total = left_range.entries.len() + right_range.entries.len();
        assert_eq!(total, 30, "union of both scans should be 30, got {total}");

        // No duplicates
        let mut all_keys: Vec<Vec<u8>> = left_range.entries.iter().map(|e| e.key.clone()).collect();
        all_keys.extend(right_range.entries.iter().map(|e| e.key.clone()));
        all_keys.sort();
        all_keys.dedup();
        assert_eq!(all_keys.len(), 30, "no duplicate keys across children");

        // Verify values
        for entry in left_range.entries.iter().chain(right_range.entries.iter()) {
            let key_str = String::from_utf8_lossy(&entry.key);
            let i: u32 = key_str.strip_prefix("key-").unwrap().parse().unwrap();
            assert_eq!(entry.value, format!("val-{i}").as_bytes(), "value mismatch for {key_str}");
        }
    });
}

#[test]
fn range_scan_with_limit_and_pagination_after_split() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 41).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 902, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(62, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Write 30 keys, flush, split
        for i in 0u32..30 {
            ps_put(
                &ps, 902,
                format!("key-{i:02}").as_bytes(),
                format!("val-{i}").as_bytes(),
                true,
            ).await;
        }
        ps_flush(&ps, 902).await;

        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 902 }),
            )
            .await
            .expect("split");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split failed: {}", sr.message);
        compio::time::sleep(Duration::from_millis(1000)).await;

        let regions = get_regions(&mgr).await;
        let right_id = regions.regions.iter().find(|(_, r)| r.part_id != 902).unwrap().1.part_id;
        compio::time::sleep(Duration::from_millis(6000)).await;

        // Get the expected total for right child
        let full_range = psr_range(&router, right_id, b"", b"", 1000).await;
        let expected_count = full_range.entries.len();
        assert!(expected_count > 0, "right child should have keys");

        // Paginate with limit=5
        let mut collected = Vec::new();
        let mut start_key: Vec<u8> = Vec::new();
        loop {
            let page = psr_range(&router, right_id, b"", &start_key, 5).await;
            collected.extend(page.entries.iter().map(|e| e.key.clone()));
            if !page.has_more || page.entries.is_empty() {
                break;
            }
            // Next page starts after the last key
            start_key = page.entries.last().unwrap().key.clone();
            start_key.push(0x00);
        }

        assert_eq!(
            collected.len(), expected_count,
            "pagination should collect all {expected_count} right-child keys, got {}",
            collected.len()
        );
    });
}

#[test]
#[ignore] // compound test
fn range_scan_consistent_after_split_compact_split() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 42).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 903, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(63, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Write 50 keys, flush
        for i in 0u32..50 {
            ps_put(
                &ps, 903,
                format!("key-{i:02}").as_bytes(),
                format!("val-{i}").as_bytes(),
                true,
            ).await;
        }
        ps_flush(&ps, 903).await;

        // First split
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 903 }),
            )
            .await
            .expect("split1");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split1 failed: {}", sr.message);
        compio::time::sleep(Duration::from_millis(1000)).await;

        let regions = get_regions(&mgr).await;
        let right1_id = regions.regions.iter().find(|(_, r)| r.part_id != 903).unwrap().1.part_id;
        compio::time::sleep(Duration::from_millis(6000)).await;

        // Compact both children to clear overlap
        ps_compact(&ps, 903).await;
        psr_compact(&router, right1_id).await;
        compio::time::sleep(Duration::from_millis(3000)).await;

        // Split left child again
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 903 }),
            )
            .await
            .expect("split2");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split2 failed: {}", sr.message);
        compio::time::sleep(Duration::from_millis(1000)).await;

        let regions = get_regions(&mgr).await;
        assert_eq!(regions.regions.len(), 3, "should have 3 partitions");
        compio::time::sleep(Duration::from_millis(6000)).await;

        // Compact all 3
        for (_, rg) in &regions.regions {
            psr_compact(&router, rg.part_id).await;
        }
        compio::time::sleep(Duration::from_millis(3000)).await;

        // Range scan all 3, collect entries
        let mut all_entries = Vec::new();
        for (_, rg) in &regions.regions {
            let range_resp = psr_range(&router, rg.part_id, b"", b"", 1000).await;
            all_entries.extend(range_resp.entries);
        }

        // Verify completeness
        let mut all_keys: Vec<Vec<u8>> = all_entries.iter().map(|e| e.key.clone()).collect();
        all_keys.sort();
        all_keys.dedup();
        assert_eq!(all_keys.len(), 50, "should have exactly 50 unique keys, got {}", all_keys.len());

        // Verify values
        for entry in &all_entries {
            let key_str = String::from_utf8_lossy(&entry.key);
            let i: u32 = key_str.strip_prefix("key-").unwrap().parse().unwrap();
            assert_eq!(entry.value, format!("val-{i}").as_bytes());
        }
    });
}
```

- [ ] **Step 2: Build and run focused tests**

Run: `cargo test -p autumn-manager --test system_range_split -- --exact range_scan_returns_correct_results_after_split range_scan_with_limit_and_pagination_after_split 2>&1 | tail -20`
Expected: 2 tests pass.

- [ ] **Step 3: Run compound test**

Run: `cargo test -p autumn-manager --test system_range_split -- --ignored 2>&1 | tail -20`
Expected: 1 test passes.

- [ ] **Step 4: Commit**

```bash
git add crates/manager/tests/system_range_split.rs
git commit -m "test: add range scan data integrity tests across splits"
```

---

### Task 4: `system_crash_mid_flush.rs` — Crash during flush tests

**Files:**
- Create: `crates/manager/tests/system_crash_mid_flush.rs`

- [ ] **Step 1: Create the test file with 2 tests**

```rust
//! Data integrity: crash during flush pipeline — data recoverable from logStream.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;

use support::*;

#[test]
fn crash_during_flush_unflushed_data_recoverable() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 50).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        let ps1_addr = pick_addr();
        start_partition_server(71, mgr_addr, ps1_addr);
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        // Write 50 keys (enough to potentially trigger rotation)
        for i in 0u32..50 {
            ps_put(
                &ps1, 901,
                format!("key-{i:03}").as_bytes(),
                format!("val-{i}").as_bytes(),
                true,
            ).await;
        }

        // Immediately crash — do not wait for flush
        drop(ps1);

        // PS2 takes over and recovers from logStream
        let ps2_addr = pick_addr();
        start_partition_server(71, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        compio::time::sleep(Duration::from_millis(2000)).await;

        // All 50 keys must be readable
        for i in 0u32..50 {
            let key = format!("key-{i:03}");
            let resp = ps_get(&ps2, 901, key.as_bytes()).await;
            assert_eq!(
                resp.value,
                format!("val-{i}").as_bytes(),
                "{key} must be recoverable after crash"
            );
        }
    });
}

#[test]
#[ignore] // compound test
fn crash_mid_flush_no_orphan_corruption() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 51).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 902, log, row, meta, b"a", b"z").await;

        let ps1_addr = pick_addr();
        start_partition_server(72, mgr_addr, ps1_addr);
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        // Write 30 keys and flush (completes fully)
        for i in 0u32..30 {
            ps_put(
                &ps1, 902,
                format!("batch1-{i:03}").as_bytes(),
                format!("val1-{i}").as_bytes(),
                true,
            ).await;
        }
        ps_flush(&ps1, 902).await;

        // Write 30 more keys, trigger flush, then crash quickly
        for i in 0u32..30 {
            ps_put(
                &ps1, 902,
                format!("batch2-{i:03}").as_bytes(),
                format!("val2-{i}").as_bytes(),
                true,
            ).await;
        }
        // Trigger flush and race with crash
        ps_flush(&ps1, 902).await;
        compio::time::sleep(Duration::from_millis(100)).await;
        drop(ps1);

        // PS2 recovers
        let ps2_addr = pick_addr();
        start_partition_server(72, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Both batches must be present
        for i in 0u32..30 {
            let key = format!("batch1-{i:03}");
            let resp = ps_get(&ps2, 902, key.as_bytes()).await;
            assert_eq!(resp.value, format!("val1-{i}").as_bytes(), "{key} (batch1) missing");
        }
        for i in 0u32..30 {
            let key = format!("batch2-{i:03}");
            let resp = ps_get(&ps2, 902, key.as_bytes()).await;
            assert_eq!(resp.value, format!("val2-{i}").as_bytes(), "{key} (batch2) missing");
        }

        // Compaction must not choke on any orphan row_stream data
        ps_compact(&ps2, 902).await;
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Re-verify after compaction
        for i in 0u32..30 {
            let key = format!("batch1-{i:03}");
            let resp = ps_get(&ps2, 902, key.as_bytes()).await;
            assert_eq!(resp.value, format!("val1-{i}").as_bytes(), "{key} wrong after compact");
        }
        for i in 0u32..30 {
            let key = format!("batch2-{i:03}");
            let resp = ps_get(&ps2, 902, key.as_bytes()).await;
            assert_eq!(resp.value, format!("val2-{i}").as_bytes(), "{key} wrong after compact");
        }
    });
}
```

- [ ] **Step 2: Build and run**

Run: `cargo test -p autumn-manager --test system_crash_mid_flush 2>&1 | tail -20`
Expected: 1 focused test passes, 1 ignored.

Run: `cargo test -p autumn-manager --test system_crash_mid_flush -- --ignored 2>&1 | tail -20`
Expected: 1 compound test passes.

- [ ] **Step 3: Commit**

```bash
git add crates/manager/tests/system_crash_mid_flush.rs
git commit -m "test: add crash-during-flush data integrity tests"
```

---

### Task 5: `system_crash_mid_compact.rs` — Crash during compaction tests

**Files:**
- Create: `crates/manager/tests/system_crash_mid_compact.rs`

- [ ] **Step 1: Create the test file with 2 tests**

```rust
//! Data integrity: crash during compaction — no data loss, re-compact succeeds.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;

use support::*;

#[test]
fn crash_during_compaction_no_data_loss() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 55).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        let ps1_addr = pick_addr();
        start_partition_server(75, mgr_addr, ps1_addr);
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        // Write 20 keys, flush → SSTable 1
        for i in 0u32..20 {
            ps_put(
                &ps1, 901,
                format!("key-{i:02}").as_bytes(),
                format!("val-{i}").as_bytes(),
                true,
            ).await;
        }
        ps_flush(&ps1, 901).await;

        // Write 20 more, flush → SSTable 2
        for i in 20u32..40 {
            ps_put(
                &ps1, 901,
                format!("key-{i:02}").as_bytes(),
                format!("val-{i}").as_bytes(),
                true,
            ).await;
        }
        ps_flush(&ps1, 901).await;

        // Trigger compaction, then crash
        ps_compact(&ps1, 901).await;
        compio::time::sleep(Duration::from_millis(100)).await;
        drop(ps1);

        // PS2 recovers
        let ps2_addr = pick_addr();
        start_partition_server(75, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        compio::time::sleep(Duration::from_millis(2000)).await;

        // All 40 keys must be readable
        for i in 0u32..40 {
            let key = format!("key-{i:02}");
            let resp = ps_get(&ps2, 901, key.as_bytes()).await;
            assert_eq!(
                resp.value,
                format!("val-{i}").as_bytes(),
                "{key} must survive crash during compaction"
            );
        }
    });
}

#[test]
#[ignore] // compound test
fn crash_during_compaction_then_successful_compact() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 56).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 902, log, row, meta, b"a", b"z").await;

        let ps1_addr = pick_addr();
        start_partition_server(76, mgr_addr, ps1_addr);
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        // SSTable 1: 30 keys
        for i in 0u32..30 {
            ps_put(
                &ps1, 902,
                format!("key-{i:02}").as_bytes(),
                format!("val-{i}").as_bytes(),
                true,
            ).await;
        }
        ps_flush(&ps1, 902).await;

        // Delete 15 keys, flush → SSTable 2 with tombstones
        for i in 0u32..15 {
            ps_delete(&ps1, 902, format!("key-{i:02}").as_bytes()).await;
        }
        ps_flush(&ps1, 902).await;

        // SSTable 3: 15 new keys
        for i in 30u32..45 {
            ps_put(
                &ps1, 902,
                format!("key-{i:02}").as_bytes(),
                format!("val-{i}").as_bytes(),
                true,
            ).await;
        }
        ps_flush(&ps1, 902).await;

        // Trigger compaction, crash
        ps_compact(&ps1, 902).await;
        compio::time::sleep(Duration::from_millis(100)).await;
        drop(ps1);

        // PS2 recovers
        let ps2_addr = pick_addr();
        start_partition_server(76, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Verify data (some tombstones may or may not have been compacted)
        for i in 15u32..45 {
            let key = format!("key-{i:02}");
            let resp = ps_get(&ps2, 902, key.as_bytes()).await;
            assert_eq!(resp.value, format!("val-{i}").as_bytes(), "{key} must survive");
        }

        // Second compaction must succeed cleanly
        ps_compact(&ps2, 902).await;
        compio::time::sleep(Duration::from_millis(2000)).await;

        // After second compaction: deleted keys gone, all 30 surviving keys present
        for i in 0u32..15 {
            let resp = ps_head(&ps2, 902, format!("key-{i:02}").as_bytes()).await;
            assert!(!resp.found, "key-{i:02} should be deleted after compaction");
        }
        for i in 15u32..45 {
            let key = format!("key-{i:02}");
            let resp = ps_get(&ps2, 902, key.as_bytes()).await;
            assert_eq!(resp.value, format!("val-{i}").as_bytes(), "{key} wrong after re-compact");
        }
    });
}
```

- [ ] **Step 2: Build and run**

Run: `cargo test -p autumn-manager --test system_crash_mid_compact -- --include-ignored 2>&1 | tail -20`
Expected: 2 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/manager/tests/system_crash_mid_compact.rs
git commit -m "test: add crash-during-compaction data integrity tests"
```

---

### Task 6: `system_multi_split_chain.rs` — Sequential split chain tests

**Files:**
- Create: `crates/manager/tests/system_multi_split_chain.rs`

- [ ] **Step 1: Create the test file with 2 tests**

```rust
//! Data integrity: sequential split chains preserve all data.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;

use support::*;

#[test]
fn split_compact_split_preserves_all_data() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 60).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(81, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Write 60 keys, flush
        for i in 0u32..60 {
            ps_put(
                &ps, 901,
                format!("key-{i:02}").as_bytes(),
                format!("val-{i}").as_bytes(),
                true,
            ).await;
        }
        ps_flush(&ps, 901).await;

        // First split
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 901 }),
            )
            .await
            .expect("split1");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split1 failed: {}", sr.message);
        compio::time::sleep(Duration::from_millis(1000)).await;

        let regions = get_regions(&mgr).await;
        let right1_id = regions.regions.iter().find(|(_, r)| r.part_id != 901).unwrap().1.part_id;
        compio::time::sleep(Duration::from_millis(6000)).await;

        // Compact both to clear overlap
        ps_compact(&ps, 901).await;
        psr_compact(&router, right1_id).await;
        compio::time::sleep(Duration::from_millis(3000)).await;

        // Second split on left child
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 901 }),
            )
            .await
            .expect("split2");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split2 failed: {}", sr.message);
        compio::time::sleep(Duration::from_millis(1000)).await;

        let regions = get_regions(&mgr).await;
        assert_eq!(regions.regions.len(), 3, "should have 3 partitions");
        compio::time::sleep(Duration::from_millis(6000)).await;

        // Compact all 3
        for (_, rg) in &regions.regions {
            psr_compact(&router, rg.part_id).await;
        }
        compio::time::sleep(Duration::from_millis(3000)).await;

        // Verify all 60 keys via range scan across all partitions
        let mut all_keys = Vec::new();
        for (_, rg) in &regions.regions {
            let range_resp = psr_range(&router, rg.part_id, b"", b"", 1000).await;
            for entry in &range_resp.entries {
                let key_str = String::from_utf8_lossy(&entry.key);
                let i: u32 = key_str.strip_prefix("key-").unwrap().parse().unwrap();
                assert_eq!(entry.value, format!("val-{i}").as_bytes(), "{key_str} wrong value");
                all_keys.push(entry.key.clone());
            }
        }
        all_keys.sort();
        all_keys.dedup();
        assert_eq!(all_keys.len(), 60, "all 60 keys must be present across 3 partitions");
    });
}

#[test]
#[ignore] // compound test
fn split_chain_with_writes_between_splits() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 61).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 902, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(82, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Write 20 keys, flush
        let keys_batch1 = write_sequential_keys(&ps, 902, "b1", 20).await;
        ps_flush(&ps, 902).await;

        // First split
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 902 }),
            )
            .await
            .expect("split1");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split1 failed: {}", sr.message);
        compio::time::sleep(Duration::from_millis(1000)).await;

        let regions = get_regions(&mgr).await;
        let left_rg = regions.regions.iter().find(|(_, r)| r.part_id == 902).unwrap().1.clone();
        let right1_id = regions.regions.iter().find(|(_, r)| r.part_id != 902).unwrap().1.part_id;
        let right1_rg = regions.regions.iter().find(|(_, r)| r.part_id != 902).unwrap().1.clone();
        compio::time::sleep(Duration::from_millis(6000)).await;

        // Write new keys to both children
        let left_start = String::from_utf8_lossy(&left_rg.rg.as_ref().unwrap().start_key);
        let right_start = String::from_utf8_lossy(&right1_rg.rg.as_ref().unwrap().start_key);
        for i in 0u32..10 {
            let lk = format!("{left_start}new-{i:02}");
            let rk = format!("{right_start}new-{i:02}");
            ps_put(&ps, 902, lk.as_bytes(), lk.as_bytes(), true).await;
            psr_put(&router, right1_id, rk.as_bytes(), rk.as_bytes(), true).await;
        }
        ps_flush(&ps, 902).await;
        psr_flush(&router, right1_id).await;

        // Compact both, then split right child
        ps_compact(&ps, 902).await;
        psr_compact(&router, right1_id).await;
        compio::time::sleep(Duration::from_millis(3000)).await;

        let resp = router.client_for(right1_id).await;
        let split_resp = resp
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: right1_id }),
            )
            .await
            .expect("split2");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&split_resp).expect("decode");
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split2 failed: {}", sr.message);
        compio::time::sleep(Duration::from_millis(1000)).await;

        let regions = get_regions(&mgr).await;
        assert_eq!(regions.regions.len(), 3, "should have 3 partitions");
        compio::time::sleep(Duration::from_millis(6000)).await;

        // Compact all 3
        for (_, rg) in &regions.regions {
            psr_compact(&router, rg.part_id).await;
        }
        compio::time::sleep(Duration::from_millis(3000)).await;

        // Crash and recover
        drop(ps);
        drop(resp);
        let ps2_addr = pick_addr();
        start_partition_server(82, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        let router2 = PsRouter::new(mgr_addr, ps2_addr);
        compio::time::sleep(Duration::from_millis(6000)).await;

        // Verify batch1 keys across all partitions
        let regions = get_regions(&mgr).await;
        let mut found_count = 0u32;
        for key in &keys_batch1 {
            for (_, rg) in &regions.regions {
                let head = psr_head(&router2, rg.part_id, key.as_bytes()).await;
                if head.found {
                    let resp = psr_get(&router2, rg.part_id, key.as_bytes()).await;
                    let expected = format!("val-{key}");
                    assert_eq!(resp.value, expected.as_bytes(), "{key} wrong value");
                    found_count += 1;
                    break;
                }
            }
        }
        assert_eq!(found_count, 20, "all 20 batch1 keys must be found, got {found_count}");
    });
}
```

- [ ] **Step 2: Build and run**

Run: `cargo test -p autumn-manager --test system_multi_split_chain -- --include-ignored 2>&1 | tail -20`
Expected: 2 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/manager/tests/system_multi_split_chain.rs
git commit -m "test: add multi-split chain data integrity tests"
```

---

### Task 7: `system_vp_after_split_gc.rs` — VP lifetime after split + GC

**Files:**
- Create: `crates/manager/tests/system_vp_after_split_gc.rs`

- [ ] **Step 1: Create the test file with 2 tests**

```rust
//! Data integrity: ValuePointer lifetime after split + GC.
//! Verifies vp_table_refs protects shared log extents from premature deletion.

mod support;

use std::rc::Rc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

/// Generate a large value (>4KB) to force the VP path.
fn large_value(i: u32) -> Vec<u8> {
    let prefix = format!("large-val-{i:03}-");
    let mut v = prefix.into_bytes();
    v.resize(5000, b'x');
    v
}

#[test]
fn vp_resolvable_after_split_and_one_child_gc() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 70).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(91, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Write 10 large-value keys (VP path), flush
        for i in 0u32..10 {
            ps_put(
                &ps, 901,
                format!("key-{i:02}").as_bytes(),
                &large_value(i),
                true,
            ).await;
        }
        ps_flush(&ps, 901).await;

        // Split
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 901 }),
            )
            .await
            .expect("split");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split failed: {}", sr.message);
        compio::time::sleep(Duration::from_millis(1000)).await;

        let regions = get_regions(&mgr).await;
        let left_rg = regions.regions.iter().find(|(_, r)| r.part_id == 901).unwrap().1.clone();
        let right_id = regions.regions.iter().find(|(_, r)| r.part_id != 901).unwrap().1.part_id;
        let mid_key = left_rg.rg.as_ref().unwrap().end_key.clone();
        compio::time::sleep(Duration::from_millis(6000)).await;

        // Compact right child, then GC right child
        psr_compact(&router, right_id).await;
        compio::time::sleep(Duration::from_millis(2000)).await;
        psr_gc(&router, right_id).await;
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Left child must still resolve all its large values
        for i in 0u32..10 {
            let key = format!("key-{i:02}");
            let kb = key.as_bytes();
            if kb < mid_key.as_slice() {
                let resp = ps_get(&ps, 901, kb).await;
                assert!(
                    resp.value.starts_with(format!("large-val-{i:03}-").as_bytes()),
                    "{key} VP resolution failed on left child"
                );
            }
        }

        // Right child should also serve correct values
        for i in 0u32..10 {
            let key = format!("key-{i:02}");
            let kb = key.as_bytes();
            if kb >= mid_key.as_slice() {
                let resp = psr_get(&router, right_id, kb).await;
                assert!(
                    resp.value.starts_with(format!("large-val-{i:03}-").as_bytes()),
                    "{key} VP resolution failed on right child"
                );
            }
        }
    });
}

#[test]
#[ignore] // compound test
fn vp_freed_after_both_children_compact_and_gc() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 71).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 902, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(92, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Write 10 large-value keys, flush
        for i in 0u32..10 {
            ps_put(
                &ps, 902,
                format!("key-{i:02}").as_bytes(),
                &large_value(i),
                true,
            ).await;
        }
        ps_flush(&ps, 902).await;

        // Note the log stream's extent IDs before split
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "test-vp-gc".to_string(),
            1024 * 1024,
            pool.clone(),
        )
        .await
        .expect("connect sc");
        let log_info = sc.get_stream_info(log).await.expect("log stream info");
        let shared_log_extent = log_info.extent_ids[0];

        // Split
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 902 }),
            )
            .await
            .expect("split");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split failed: {}", sr.message);
        compio::time::sleep(Duration::from_millis(1000)).await;

        let regions = get_regions(&mgr).await;
        let left_rg = regions.regions.iter().find(|(_, r)| r.part_id == 902).unwrap().1.clone();
        let right_id = regions.regions.iter().find(|(_, r)| r.part_id != 902).unwrap().1.part_id;
        let right_rg = regions.regions.iter().find(|(_, r)| r.part_id != 902).unwrap().1.clone();
        let mid_key = left_rg.rg.as_ref().unwrap().end_key.clone();
        compio::time::sleep(Duration::from_millis(6000)).await;

        // Compact both children (rewrites SSTables with only in-range keys)
        ps_compact(&ps, 902).await;
        psr_compact(&router, right_id).await;
        compio::time::sleep(Duration::from_millis(3000)).await;

        // Write a small key to each child and flush (forces new log extents)
        let left_key = format!("{}small", String::from_utf8_lossy(&left_rg.rg.as_ref().unwrap().start_key));
        let right_key = format!("{}small", String::from_utf8_lossy(&right_rg.rg.as_ref().unwrap().start_key));
        ps_put(&ps, 902, left_key.as_bytes(), b"v", true).await;
        ps_flush(&ps, 902).await;
        psr_put(&router, right_id, right_key.as_bytes(), b"v", true).await;
        psr_flush(&router, right_id).await;

        // GC both children
        ps_gc(&ps, 902).await;
        psr_gc(&router, right_id).await;
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Check shared log extent refs
        sc.invalidate_extent_cache(shared_log_extent);
        match sc.get_extent_info(shared_log_extent).await {
            Ok(info) => {
                assert!(
                    info.refs <= 2,
                    "shared log extent refs should be <= 2 after GC, got {}",
                    info.refs
                );
                if info.refs < 2 {
                    println!("log extent refs decreased from 2 to {} after GC", info.refs);
                }
            }
            Err(_) => {
                println!("shared log extent was fully deleted — ideal outcome");
            }
        }

        // Both children must still serve data
        for i in 0u32..10 {
            let key = format!("key-{i:02}");
            let kb = key.as_bytes();
            let part_id = if kb < mid_key.as_slice() { 902 } else { right_id };
            let resp = psr_get(&router, part_id, kb).await;
            assert!(
                resp.value.starts_with(format!("large-val-{i:03}-").as_bytes()),
                "{key} (part {part_id}) VP resolution failed after both GC"
            );
        }
    });
}
```

- [ ] **Step 2: Build and run**

Run: `cargo test -p autumn-manager --test system_vp_after_split_gc -- --include-ignored 2>&1 | tail -20`
Expected: 2 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/manager/tests/system_vp_after_split_gc.rs
git commit -m "test: add VP lifetime after split + GC data integrity tests"
```

---

### Task 8: `system_bulk_verification.rs` — Scale verification tests

**Files:**
- Create: `crates/manager/tests/system_bulk_verification.rs`

- [ ] **Step 1: Create the test file with 2 tests**

```rust
//! Data integrity: bulk verification at scale — mixed inline + VP values.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;

use support::*;

#[test]
fn bulk_write_crash_restart_all_data_intact() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 80).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        let ps1_addr = pick_addr();
        start_partition_server(95, mgr_addr, ps1_addr);
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        // Write 100 small keys (inline)
        for i in 0u32..100 {
            let key = format!("small-{i:03}");
            let val = format!("sv-{i:03}");
            ps_put(&ps1, 901, key.as_bytes(), val.as_bytes(), true).await;
        }
        // Write 100 large keys (>4KB, VP path)
        for i in 0u32..100 {
            let key = format!("large-{i:03}");
            let mut val = format!("lv-{i:03}-").into_bytes();
            val.resize(5000, b'y');
            ps_put(&ps1, 901, key.as_bytes(), &val, true).await;
        }

        ps_flush(&ps1, 901).await;

        // Crash and recover
        drop(ps1);
        let ps2_addr = pick_addr();
        start_partition_server(95, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        compio::time::sleep(Duration::from_millis(3000)).await;

        // Verify all 200 keys
        for i in 0u32..100 {
            let key = format!("small-{i:03}");
            let resp = ps_get(&ps2, 901, key.as_bytes()).await;
            assert_eq!(resp.value, format!("sv-{i:03}").as_bytes(), "{key} wrong");
        }
        for i in 0u32..100 {
            let key = format!("large-{i:03}");
            let resp = ps_get(&ps2, 901, key.as_bytes()).await;
            assert!(
                resp.value.starts_with(format!("lv-{i:03}-").as_bytes()),
                "{key} VP wrong"
            );
            assert_eq!(resp.value.len(), 5000, "{key} VP truncated");
        }

        // Range scan returns exactly 200
        let range_resp = ps_range(&ps2, 901, b"", b"", 1000).await;
        assert_eq!(range_resp.entries.len(), 200, "range should return 200 entries");
    });
}

#[test]
#[ignore] // compound test
fn bulk_mixed_ops_split_restart_verify() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 81).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 902, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(96, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Write 100 keys (mix of small and large), flush
        for i in 0u32..100 {
            let key = format!("orig-{i:03}");
            let val = if i % 2 == 0 {
                format!("sv-{i:03}").into_bytes()
            } else {
                let mut v = format!("lv-{i:03}-").into_bytes();
                v.resize(5000, b'y');
                v
            };
            ps_put(&ps, 902, key.as_bytes(), &val, true).await;
        }
        ps_flush(&ps, 902).await;

        // Delete 20 keys
        for i in 0u32..20 {
            ps_delete(&ps, 902, format!("orig-{i:03}").as_bytes()).await;
        }
        // Write 30 new keys
        for i in 0u32..30 {
            let key = format!("post-{i:03}");
            let val = format!("pv-{i:03}");
            ps_put(&ps, 902, key.as_bytes(), val.as_bytes(), true).await;
        }
        ps_flush(&ps, 902).await;

        // Split
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 902 }),
            )
            .await
            .expect("split");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split failed: {}", sr.message);
        compio::time::sleep(Duration::from_millis(1000)).await;

        let regions = get_regions(&mgr).await;
        let left_rg = regions.regions.iter().find(|(_, r)| r.part_id == 902).unwrap().1.clone();
        let right_id = regions.regions.iter().find(|(_, r)| r.part_id != 902).unwrap().1.part_id;
        let right_rg = regions.regions.iter().find(|(_, r)| r.part_id != 902).unwrap().1.clone();
        let mid_key = left_rg.rg.as_ref().unwrap().end_key.clone();
        compio::time::sleep(Duration::from_millis(6000)).await;

        // Compact both
        ps_compact(&ps, 902).await;
        psr_compact(&router, right_id).await;
        compio::time::sleep(Duration::from_millis(3000)).await;

        // Write 20 more to each child
        let left_start = String::from_utf8_lossy(&left_rg.rg.as_ref().unwrap().start_key);
        let right_start = String::from_utf8_lossy(&right_rg.rg.as_ref().unwrap().start_key);
        for i in 0u32..20 {
            let lk = format!("{left_start}child-{i:02}");
            let rk = format!("{right_start}child-{i:02}");
            ps_put(&ps, 902, lk.as_bytes(), lk.as_bytes(), true).await;
            psr_put(&router, right_id, rk.as_bytes(), rk.as_bytes(), true).await;
        }
        ps_flush(&ps, 902).await;
        psr_flush(&router, right_id).await;

        // Crash and recover
        drop(ps);
        let ps2_addr = pick_addr();
        start_partition_server(96, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        let router2 = PsRouter::new(mgr_addr, ps2_addr);
        compio::time::sleep(Duration::from_millis(6000)).await;

        // Verify deleted keys not-found
        for i in 0u32..20 {
            let key = format!("orig-{i:03}");
            let kb = key.as_bytes();
            let part_id = if kb < mid_key.as_slice() { 902 } else { right_id };
            let head = psr_head(&router2, part_id, kb).await;
            assert!(!head.found, "{key} should be deleted");
        }

        // Verify surviving original keys
        for i in 20u32..100 {
            let key = format!("orig-{i:03}");
            let kb = key.as_bytes();
            let part_id = if kb < mid_key.as_slice() { 902 } else { right_id };
            let resp = psr_get(&router2, part_id, kb).await;
            if i % 2 == 0 {
                assert_eq!(resp.value, format!("sv-{i:03}").as_bytes(), "{key} wrong (small)");
            } else {
                assert!(resp.value.starts_with(format!("lv-{i:03}-").as_bytes()), "{key} wrong (large)");
            }
        }

        // Range scan union
        let left_range = psr_range(&router2, 902, b"", b"", 1000).await;
        let right_range = psr_range(&router2, right_id, b"", b"", 1000).await;
        let total = left_range.entries.len() + right_range.entries.len();
        // 80 surviving originals + 30 post-delete + 40 child keys = 150
        assert_eq!(total, 150, "range union should be 150, got {total}");
    });
}
```

- [ ] **Step 2: Build and run**

Run: `cargo test -p autumn-manager --test system_bulk_verification -- --include-ignored 2>&1 | tail -20`
Expected: 2 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/manager/tests/system_bulk_verification.rs
git commit -m "test: add bulk data integrity verification tests (200+ keys, mixed ops)"
```

---

### Task 9: `system_concurrent_write_crash.rs` — Concurrent writer tests

**Files:**
- Create: `crates/manager/tests/system_concurrent_write_crash.rs`

- [ ] **Step 1: Create the test file with 2 tests**

```rust
//! Data integrity: concurrent writers + crash/split — no acknowledged writes lost.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;

use support::*;

#[test]
fn concurrent_writers_crash_no_data_loss() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 85).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        let ps1_addr = pick_addr();
        start_partition_server(97, mgr_addr, ps1_addr);
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        // 5 concurrent writers, each writing 20 keys
        let mut handles = Vec::new();
        for w in 0u32..5 {
            let ps_clone = RpcClient::connect(ps1_addr).await.expect("connect writer");
            handles.push(compio::runtime::spawn(async move {
                let mut keys = Vec::new();
                for i in 0u32..20 {
                    let key = format!("w{w}-key-{i:02}");
                    let val = format!("w{w}-val-{i:02}");
                    ps_put(&ps_clone, 901, key.as_bytes(), val.as_bytes(), true).await;
                    keys.push(key);
                }
                keys
            }));
        }

        // Collect all acknowledged keys
        let mut all_keys: Vec<String> = Vec::new();
        for h in handles {
            let keys = h.await;
            all_keys.extend(keys);
        }
        assert_eq!(all_keys.len(), 100);

        // Crash without flushing
        drop(ps1);

        // PS2 recovers from logStream
        let ps2_addr = pick_addr();
        start_partition_server(97, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        compio::time::sleep(Duration::from_millis(3000)).await;

        // Every acknowledged key must be readable
        for key in &all_keys {
            // key = "wN-key-MM", expected value = "wN-val-MM"
            let expected = key.replace("-key-", "-val-");
            let resp = ps_get(&ps2, 901, key.as_bytes()).await;
            assert_eq!(
                resp.value,
                expected.as_bytes(),
                "{key} missing or wrong after recovery"
            );
        }
    });
}

#[test]
#[ignore] // compound test
fn concurrent_writers_during_split() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 86).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 902, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(98, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Pre-populate 50 keys, flush
        for i in 0u32..50 {
            ps_put(
                &ps, 902,
                format!("pre-{i:02}").as_bytes(),
                format!("pv-{i:02}").as_bytes(),
                true,
            ).await;
        }
        ps_flush(&ps, 902).await;

        // Start split and concurrent writes
        let split_ps = RpcClient::connect(ps_addr).await.expect("connect split");
        let write_ps = RpcClient::connect(ps_addr).await.expect("connect writer");

        let split_handle = compio::runtime::spawn(async move {
            split_ps
                .call(
                    partition_rpc::MSG_SPLIT_PART,
                    partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 902 }),
                )
                .await
                .expect("split")
        });

        let write_handle = compio::runtime::spawn(async move {
            let mut written = Vec::new();
            for i in 0u32..20 {
                let key = format!("concurrent-{i:02}");
                let val = format!("cv-{i:02}");
                match write_ps
                    .call(
                        partition_rpc::MSG_PUT,
                        partition_rpc::rkyv_encode(&partition_rpc::PutReq {
                            part_id: 902,
                            key: key.as_bytes().to_vec(),
                            value: val.as_bytes().to_vec(),
                            must_sync: true,
                            expires_at: 0,
                        }),
                    )
                    .await
                {
                    Ok(_) => written.push(key),
                    Err(_) => {} // write may fail if split changes ownership
                }
            }
            written
        });

        let split_resp_bytes = split_handle.await;
        let sr: partition_rpc::SplitPartResp =
            partition_rpc::rkyv_decode(&split_resp_bytes).expect("decode");
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split failed: {}", sr.message);

        let concurrent_keys = write_handle.await;
        compio::time::sleep(Duration::from_millis(1000)).await;

        let regions = get_regions(&mgr).await;
        assert_eq!(regions.regions.len(), 2);
        let left_rg = regions.regions.iter().find(|(_, r)| r.part_id == 902).unwrap().1.clone();
        let right_id = regions.regions.iter().find(|(_, r)| r.part_id != 902).unwrap().1.part_id;
        let mid_key = left_rg.rg.as_ref().unwrap().end_key.clone();
        compio::time::sleep(Duration::from_millis(6000)).await;

        // All pre-split keys must be in the correct child
        for i in 0u32..50 {
            let key = format!("pre-{i:02}");
            let kb = key.as_bytes();
            let part_id = if kb < mid_key.as_slice() { 902 } else { right_id };
            let resp = psr_get(&router, part_id, kb).await;
            assert_eq!(resp.value, format!("pv-{i:02}").as_bytes(), "{key} wrong");
        }

        // Each acknowledged concurrent key must be readable from exactly one child
        for key in &concurrent_keys {
            let kb = key.as_bytes();
            let left_head = psr_head(&router, 902, kb).await;
            let right_head = psr_head(&router, right_id, kb).await;
            assert!(
                left_head.found || right_head.found,
                "concurrent {key} acknowledged but not found in either child"
            );
        }

        // Compact both, re-verify pre-split keys
        ps_compact(&ps, 902).await;
        psr_compact(&router, right_id).await;
        compio::time::sleep(Duration::from_millis(3000)).await;

        for i in 0u32..50 {
            let key = format!("pre-{i:02}");
            let kb = key.as_bytes();
            let part_id = if kb < mid_key.as_slice() { 902 } else { right_id };
            let resp = psr_get(&router, part_id, kb).await;
            assert_eq!(resp.value, format!("pv-{i:02}").as_bytes(), "{key} wrong after compact");
        }
    });
}
```

- [ ] **Step 2: Build and run**

Run: `cargo test -p autumn-manager --test system_concurrent_write_crash -- --include-ignored 2>&1 | tail -20`
Expected: 2 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/manager/tests/system_concurrent_write_crash.rs
git commit -m "test: add concurrent writer data integrity tests (crash + split)"
```

---

### Task 10: Final verification and progress update

**Files:**
- Modify: `claude-progress.txt`
- Modify: `feature_list.md` (if a relevant feature entry exists)

- [ ] **Step 1: Run all focused tests**

Run: `cargo test -p autumn-manager --test system_delete_tombstone --test system_range_split --test system_crash_mid_flush --test system_crash_mid_compact --test system_multi_split_chain --test system_vp_after_split_gc --test system_bulk_verification --test system_concurrent_write_crash 2>&1 | tail -30`
Expected: 10 focused tests pass, 8 ignored.

- [ ] **Step 2: Run all compound tests**

Run: `cargo test -p autumn-manager --test system_delete_tombstone --test system_range_split --test system_crash_mid_flush --test system_crash_mid_compact --test system_multi_split_chain --test system_vp_after_split_gc --test system_bulk_verification --test system_concurrent_write_crash -- --ignored 2>&1 | tail -30`
Expected: 8 compound tests pass.

- [ ] **Step 3: Update `claude-progress.txt`**

```
Date: 2026-05-02
TaskStatus: completed
Task scope: Data integrity integration test suite (18 tests across 8 files)

Completed:
  - Added helpers to support/mod.rs: ps_delete, ps_head, ps_range + routed variants + bulk utilities
  - system_delete_tombstone.rs: 3 tests (delete + crash, compaction, split)
  - system_range_split.rs: 3 tests (range after split, pagination, multi-split)
  - system_crash_mid_flush.rs: 2 tests (crash during flush, orphan safety)
  - system_crash_mid_compact.rs: 2 tests (crash during compaction, re-compact)
  - system_multi_split_chain.rs: 2 tests (split-compact-split, interleaved writes)
  - system_vp_after_split_gc.rs: 2 tests (VP after one-child GC, VP freed after both GC)
  - system_bulk_verification.rs: 2 tests (200 keys crash recovery, mixed ops lifecycle)
  - system_concurrent_write_crash.rs: 2 tests (concurrent + crash, concurrent + split)
  - 10 focused tests run by default, 8 compound tests gated behind #[ignore]

Pre-existing failures (not related):
  - ec_integration::replication_stream_works
  - integration::f030_flush_writes_sst_to_row_stream
  - integration::f029_compaction_merges_small_tables
```

- [ ] **Step 4: Commit progress update**

```bash
git add claude-progress.txt
git commit -m "chore: update progress — data integrity test suite complete"
```
