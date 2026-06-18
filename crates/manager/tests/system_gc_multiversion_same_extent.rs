//! Reproduce-first: GC liveness-identity bug (coco /arch P0, 2026-06-18).
//!
//! `run_gc → process_gc_chunk` decides a scanned WAL record is "live" by
//! comparing ONLY `ValuePointer.extent_id` against the key's current live VP
//! (`background.rs` ~2650), NOT `offset/len`. When the SAME key has two large
//! (VP) versions in the SAME sealed log extent — old `A` at `off_a`, new `B`
//! at `off_b` — GC scans `A` first, sees the live VP (`B`) is in this extent,
//! and relocates `A`'s OLD value with a fresh (higher) seq. If `A`'s
//! relocation is flushed into the memtable BEFORE `B` is scanned, the live
//! version of the key becomes the relocated OLD value; scanning `B` then sees
//! a live VP pointing at the new extent and SKIPS `B`. After `punch_holes`,
//! the new value is gone and the old value is revived → silent lost-update.
//!
//! This test forces the flush-before-scan condition deterministically with
//! `set_gc_batch_records(1)` (each relocated record flushes immediately), so a
//! small (5 KiB, still VP) value reproduces it without needing ≥4 MiB values.
//!
//! Seal of the shared log extent is via `split` (the proven harness pattern —
//! log_stream is never truncate-from-head and only rolls at the 16 GiB extent
//! size); `compact` populates the discard map so AUTO GC selects the extent.
//!
//! Pre-fix: this test FAILS (the key reads back the OLD value).
//! Post-fix (full VP identity match): the key reads back the NEW value.

mod support;

use std::time::Duration;

use std::rc::Rc;

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

fn val(tag: u8) -> Vec<u8> {
    // >4 KiB ⇒ stored as a ValuePointer in the log_stream (VP path).
    vec![tag; 5000]
}

#[test]
fn gc_same_key_two_large_versions_same_extent_keeps_newest() {
    // Force GC to flush every relocated record immediately so a superseded
    // older version (scanned + relocated first) becomes the live version
    // before the newer version is scanned — the exact P0 race. Must run
    // before the PS performs any GC (the OnceLock is set-once).
    assert!(
        autumn_partition_server::background::set_gc_batch_records(1),
        "set_gc_batch_records must win the OnceLock before any GC"
    );

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
        register_two_nodes(&mgr, n1_addr, n2_addr, 88).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(81, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Three distinct keys (≥2 needed for split). "kmm" is the victim: two
        // large versions written back-to-back land in the SAME active log
        // extent (no seal between them). v1 = b'A'*, v2 = b'B'*.
        let v1 = val(b'A');
        let v2 = val(b'B');
        ps_put(&ps, 901, b"kaa", &val(b'P')).await;
        ps_put(&ps, 901, b"kmm", &v1).await; // record A (old) in extent X
        ps_put(&ps, 901, b"kmm", &v2).await; // record B (new) in extent X
        ps_put(&ps, 901, b"kzz", &val(b'Q')).await;
        ps_flush(&ps, 901).await; // live VP for "kmm" = v2 lands in an SST

        // Sanity: newest value is readable before split.
        let pre = ps_get(&ps, 901, b"kmm").await;
        assert_eq!(pre.value, v2, "pre-split: kmm must read v2");

        // Capture the shared log extent X (holds both kmm versions) so we can
        // later PROVE GC actually scanned + punched it (coco P2: guards against
        // a false-negative where AUTO_GC silently never selects X).
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(&mgr_addr.to_string(), "test-gc-mv".to_string(), 1 << 20, pool)
            .await
            .expect("connect sc");
        let x = sc.get_stream_info(log).await.expect("log info").extent_ids[0];

        // Split seals the shared log extent X (CoW). user_keys = [kaa, kmm,
        // kzz] ⇒ mid_key = "kmm" ⇒ "kmm" lands in the RIGHT child.
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
        let left_rg = regions
            .regions
            .iter()
            .find(|(_, r)| r.part_id == 901)
            .expect("left")
            .1
            .clone();
        let right_id = regions
            .regions
            .iter()
            .find(|(_, r)| r.part_id != 901)
            .expect("right")
            .1
            .part_id;
        let mid_key = left_rg.rg.as_ref().unwrap().end_key.clone();
        // Which partition owns "kmm"?
        let owner = if b"kmm".as_slice() < mid_key.as_slice() {
            901
        } else {
            right_id
        };
        // Wait for the right child to open.
        compio::time::sleep(Duration::from_millis(6000)).await;

        // Roll a new log tail on the owner: post-split its log_stream is just
        // [X] (X = the sealed shared extent), so X is the LAST extent and
        // `run_gc`'s `sealed_extents = extent_ids[..len-1]` excludes it. A small
        // in-range write allocates a fresh tail → X becomes a non-tail sealed
        // GC candidate. (Mirrors the vp_freed_after_both_children test.)
        let roll_key: &[u8] = if owner == 901 { b"ab_roll" } else { b"kp_roll" };
        if owner == 901 {
            ps_put(&ps, owner, roll_key, b"s").await;
            ps_flush(&ps, owner).await;
        } else {
            psr_put(&router, owner, roll_key, b"s").await;
            psr_flush(&router, owner).await;
        }
        compio::time::sleep(Duration::from_millis(500)).await;

        // Compact the owner: drops the superseded "kmm"=v1 (and out-of-range
        // keys), populating the discard map for extent X so AUTO GC selects it.
        if owner == 901 {
            ps_compact(&ps, owner).await;
        } else {
            psr_compact(&router, owner).await;
        }
        compio::time::sleep(Duration::from_millis(2000)).await;

        // GC the owner → run_gc scans X. With the bug, it relocates the OLD
        // "kmm"=v1 (flushed immediately via record_cap=1), then skips the new
        // "kmm"=v2, then punches X.
        if owner == 901 {
            ps_gc(&ps, owner).await;
        } else {
            psr_gc(&router, owner).await;
        }
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Prove GC actually scanned + punched X (otherwise this test would be a
        // false-negative — the original v2 would survive trivially). X had refs=2
        // post-split (CoW: both children's log_streams); the owner's run_gc must
        // punch it out of its membership → refs<2 (or fully deleted).
        let mut punched = false;
        for _ in 0..12 {
            sc.invalidate_extent_cache(x);
            match sc.get_extent_info(x).await {
                Ok(info) if info.refs < 2 => {
                    punched = true;
                    break;
                }
                Err(_) => {
                    punched = true;
                    break;
                }
                _ => compio::time::sleep(Duration::from_millis(500)).await,
            }
        }
        assert!(
            punched,
            "GC never punched extent X (refs stayed >=2) — the test did not \
             exercise run_gc on the shared extent; reproduction is invalid"
        );

        // The newest value MUST survive GC. Pre-fix this reads v1 (revived old).
        let got = if owner == 901 {
            ps_get(&ps, owner, b"kmm").await
        } else {
            psr_get(&router, owner, b"kmm").await
        };
        assert_eq!(
            got.value, v2,
            "GC revived the OLD value of kmm and lost the NEW one \
             (got {:?}..., expected v2 b'B'*)",
            &got.value.get(..4)
        );
    });
}
