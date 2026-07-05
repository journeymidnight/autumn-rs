//! Reproduce-first probe for the "split-sealed tail stays cached-as-open →
//! force-GC conservatively skips it" GC-promptness gap.
//!
//! Claim under test: the split SOURCE keeps its StreamClient (no reopen, F212),
//! and `handle_split_part` only `invalidate_stream`s (drops the worker) — it does
//! NOT `invalidate_extent_cache` the manager-sealed tail. So the sealed tail could
//! read `sealed=false` from a stale cache and `authoritative_sealed_length` would
//! skip it → force-GC never reclaims it.
//!
//! But the sealed tail is EXCLUDED from `sealed_extents` (it's the last extent)
//! until a fresh tail is allocated — and that allocation goes through
//! `ensure_tail_initialised` → `load_stream_tail`, which INSERTS the tail into
//! `extent_info_cache` with its fresh sealed state. So by the time the sealed
//! tail is a force-GC candidate, its cache is already refreshed. This test proves
//! that end-to-end: after split + a source write + major-compact, force-GC of the
//! source's sealed log extents MUST reclaim the shared tail (refs 2 → 1). If it
//! does, the "stale-open cache" state is unreachable and no seal-time evict is
//! needed.

mod support;

use std::rc::Rc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

fn large_value(i: u32) -> Vec<u8> {
    // > 4 KiB VALUE_THROTTLE → ValuePointer path (value lives in log_stream).
    let mut v = format!("large-val-{i:03}-").into_bytes();
    v.resize(5000, b'x');
    v
}

async fn force_gc(ps: &RpcClient, part_id: u64, extent_ids: Vec<u64>) {
    let resp = ps
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id,
                op: partition_rpc::MAINTENANCE_FORCE_GC,
                extent_ids,
                gc_ratio: None,
                gc_max_size: None,
                gc_stream_debt: None,
                gc_empty_only: false,
            }),
        )
        .await
        .expect("force gc");
    let r: partition_rpc::MaintenanceResp =
        partition_rpc::rkyv_decode(&resp).expect("decode");
    assert_eq!(r.code, partition_rpc::CODE_OK, "force gc failed: {}", r.message);
}

#[test]
fn split_source_forcegc_reclaims_shared_tail_no_stale_open_cache() {
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
        upsert_partition(&mgr, 909, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(93, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // 24 large-value keys spanning the range so the split divides them and
        // the source ends up with out-of-range keys that major-compact drops.
        for (i, c) in (b'a'..=b'x').enumerate() {
            let key = format!("{}0", c as char);
            ps_put(&ps, 909, key.as_bytes(), &large_value(i as u32)).await;
        }
        ps_flush(&ps, 909).await;

        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(&mgr_addr.to_string(), "probe".to_string(), 1 << 20, pool)
            .await
            .expect("connect sc");
        let log_info = sc.get_stream_info(log).await.expect("log info");
        let shared_log_extent = log_info.extent_ids[0];
        // Precondition: single log extent, refs 1 pre-split.
        let pre = sc.get_extent_info(shared_log_extent).await.expect("pre info");
        assert_eq!(pre.refs, 1, "shared extent refs should be 1 pre-split");

        // Split — manager seals `shared_log_extent`, refs → 2 (both children).
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 909 }),
            )
            .await
            .expect("split");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split failed: {}", sr.message);
        compio::time::sleep(Duration::from_millis(6000)).await;

        // Write a small IN-RANGE key to the SOURCE. This allocates a fresh tail
        // (the sealed shared tail becomes a non-tail GC candidate) AND — the crux
        // — routes through `load_stream_tail`, refreshing the sealed tail's cache.
        let left = get_regions(&mgr)
            .await
            .regions
            .iter()
            .find(|(_, r)| r.part_id == 909)
            .and_then(|(_, r)| r.rg.clone())
            .expect("left rg");
        let small_key = format!("{}0small", String::from_utf8_lossy(&left.start_key));
        ps_put(&ps, 909, small_key.as_bytes(), b"v").await;
        ps_flush(&ps, 909).await;

        // Major-compact the source: drops the out-of-range keys, so their VPs in
        // the shared tail become dead, and advances the replay floor past it.
        ps_compact(&ps, 909).await;
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Force-GC the source's sealed log extents (incl. the shared tail).
        let src_log = sc.get_stream_info(log).await.expect("src log info");
        assert!(
            src_log.extent_ids.len() >= 2,
            "source should have a fresh tail beyond the sealed shared extent, got {:?}",
            src_log.extent_ids
        );
        let sealed: Vec<u64> = src_log.extent_ids[..src_log.extent_ids.len() - 1].to_vec();
        assert!(sealed.contains(&shared_log_extent), "shared extent must be sealed-non-tail now");
        force_gc(&ps, 909, sealed).await;
        compio::time::sleep(Duration::from_millis(2000)).await;

        // If the cache were stale-open, `authoritative_sealed_length` would skip
        // the shared tail and refs would stay 2. It must be reclaimed (relocate
        // live VPs + punch out of the SOURCE's membership) → refs drops to 1.
        sc.invalidate_extent_cache(shared_log_extent);
        match sc.get_extent_info(shared_log_extent).await {
            Ok(info) => assert!(
                info.refs < 2,
                "force-GC did NOT reclaim the split-sealed shared tail (refs still {}) — \
                 stale-open cache gap IS reachable; a seal-time invalidate_extent_cache is needed",
                info.refs
            ),
            Err(_) => { /* fully deleted (refs→0) — also reclaimed */ }
        }

        // Source must still resolve its in-range large values (relocate was safe).
        for c in b'a'..=b'f' {
            let key = format!("{}0", c as char);
            let resp = ps_get(&ps, 909, key.as_bytes()).await;
            assert!(!resp.value.is_empty(), "{} lost after force-GC relocate", c as char);
        }
    });
}
