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
