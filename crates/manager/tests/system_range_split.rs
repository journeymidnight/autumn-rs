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

        // Verify values via point get (range scan returns keys only)
        for entry in left_range.entries.iter().chain(right_range.entries.iter()) {
            let key_str = String::from_utf8_lossy(&entry.key);
            let i: u32 = key_str.strip_prefix("key-").unwrap().parse().unwrap();
            let part_id = if left_range.entries.iter().any(|e| e.key == entry.key) { 901 } else { right_id };
            let resp = psr_get(&router, part_id, &entry.key).await;
            assert_eq!(resp.value, format!("val-{i}").as_bytes(), "value mismatch for {key_str}");
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
            // Next page starts after the last key (0x01 > 0x00 separator in MVCC encoding)
            start_key = page.entries.last().unwrap().key.clone();
            start_key.push(0x01);
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

        // Verify values via point get (range scan returns keys only)
        for entry in &all_entries {
            let key_str = String::from_utf8_lossy(&entry.key);
            let i: u32 = key_str.strip_prefix("key-").unwrap().parse().unwrap();
            let part_id = regions.regions.iter()
                .find(|(_, r)| {
                    let rg = r.rg.as_ref().unwrap();
                    entry.key.as_slice() >= rg.start_key.as_slice()
                        && (rg.end_key.is_empty() || entry.key.as_slice() < rg.end_key.as_slice())
                })
                .unwrap().1.part_id;
            let resp = psr_get(&router, part_id, &entry.key).await;
            assert_eq!(resp.value, format!("val-{i}").as_bytes(), "value mismatch for {key_str}");
        }
    });
}
