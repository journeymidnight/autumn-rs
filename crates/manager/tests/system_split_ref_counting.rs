//! Test: split extent ref counting — the sealed extents both children share
//! carry refs 2 after the split, and both children keep serving.

mod support;

use std::rc::Rc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

#[test]
fn split_children_share_the_sealed_extents_with_two_refs() {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 99).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(71, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        // after split, the right child binds a different port.
        // Route per-partition via manager's `part_addrs`.
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Write data across the range and flush
        for i in 0u8..10 {
            ps_put(
                &ps,
                901,
                format!("key-{i:02}").as_bytes(),
                format!("val-{i}").as_bytes(),
            )
            .await;
        }
        ps_flush(&ps, 901).await;

        // Check the row_stream's extent before split
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "test-ref-count".to_string(),
            1024 * 1024,
            pool.clone(),
        )
        .await
        .expect("connect sc");

        // The tail holding the SST: the partition's open sealed the first
        // extent empty.
        let row_info = sc.get_stream_info(row).await.expect("row stream info");
        let shared_extent_id = *row_info.extent_ids.last().expect("row tail");

        // Check extent refs before split
        let ext_info = sc
            .get_extent_info(shared_extent_id)
            .await
            .expect("extent info");
        assert_eq!(ext_info.refs, 1, "extent should have refs=1 before split");

        // Split
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 901, at_key: None, op_id: 0 }),
            )
            .await
            .expect("split");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(
            sr.code,
            partition_rpc::CODE_OK,
            "split failed: {}",
            sr.message
        );

        // Wait for split to propagate
        compio::time::sleep(Duration::from_millis(1000)).await;

        // Check extent refs after split — should be 2 (shared between left and right)
        // Need to invalidate cache to see the updated extent
        sc.invalidate_extent_cache(shared_extent_id);
        let ext_info = sc
            .get_extent_info(shared_extent_id)
            .await
            .expect("extent info after split");
        assert_eq!(
            ext_info.refs, 2,
            "shared extent should have refs=2 after split (left + right)"
        );
        assert!(
            ext_info.sealed && ext_info.sealed_length > 0,
            "the split must seal the shared extent holding the SST: sealed={} len={}",
            ext_info.sealed,
            ext_info.sealed_length
        );

        // Get the right partition id
        let regions = get_regions(&mgr).await;
        let right_id = regions
            .regions
            .iter()
            .find(|(_, r)| r.part_id != 901)
            .expect("right")
            .1
            .part_id;

        // Wait for PS to discover the right partition
        compio::time::sleep(Duration::from_millis(6000)).await;

        // Compact both children (this creates new SSTables with only in-range keys)
        ps_compact(&ps, 901).await;
        psr_compact(&router, right_id).await;
        compio::time::sleep(Duration::from_millis(3000)).await;

        // After compaction, flush again to ensure new SSTables are on new extents
        // Write a small key to each child and flush
        // Use keys that are definitely within each child's range
        let left_rg = regions
            .regions
            .iter()
            .find(|(_, r)| r.part_id == 901)
            .unwrap()
            .1
            .clone();
        let right_rg = regions
            .regions
            .iter()
            .find(|(_, r)| r.part_id == right_id)
            .unwrap()
            .1
            .clone();
        let left_key = format!(
            "{}new",
            String::from_utf8_lossy(&left_rg.rg.as_ref().unwrap().start_key)
        );
        let right_key = format!(
            "{}new",
            String::from_utf8_lossy(&right_rg.rg.as_ref().unwrap().start_key)
        );

        ps_put(&ps, 901, left_key.as_bytes(), b"v").await;
        ps_flush(&ps, 901).await;
        psr_put(&router, right_id, right_key.as_bytes(), b"v").await;
        psr_flush(&router, right_id).await;

        // Both children write past the split and still serve reads.
        let resp = ps_get(&ps, 901, left_key.as_bytes()).await;
        assert_eq!(resp.value, b"v", "left child should still serve reads");
        let resp = psr_get(&router, right_id, right_key.as_bytes()).await;
        assert_eq!(resp.value, b"v", "right child should still serve reads");
    });
}
