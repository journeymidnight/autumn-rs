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
