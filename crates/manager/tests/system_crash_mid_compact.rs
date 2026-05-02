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

        // Write 20 keys, flush -> SSTable 1
        for i in 0u32..20 {
            ps_put(
                &ps1, 901,
                format!("key-{i:02}").as_bytes(),
                format!("val-{i}").as_bytes(),
                true,
            ).await;
        }
        ps_flush(&ps1, 901).await;

        // Write 20 more, flush -> SSTable 2
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

        // Delete 15 keys, flush -> SSTable 2 with tombstones
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

        // Verify surviving data
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
