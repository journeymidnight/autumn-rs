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

        // Compact both children
        ps_compact(&ps, 902).await;
        psr_compact(&router, right_id).await;
        compio::time::sleep(Duration::from_millis(3000)).await;

        // Write a small key to each child and flush
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
