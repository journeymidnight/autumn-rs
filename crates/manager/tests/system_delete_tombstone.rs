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
