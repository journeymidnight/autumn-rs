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
                let resp = psr_get(&router, rg.part_id, &entry.key).await;
                assert_eq!(resp.value, format!("val-{i}").as_bytes(), "{key_str} wrong value");
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

        let right_client = router.client_for(right1_id).await;
        let split_resp = right_client
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
        drop(right_client);
        let ps2_addr = pick_addr();
        start_partition_server(82, mgr_addr, ps2_addr);
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
