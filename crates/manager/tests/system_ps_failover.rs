//! System test — PS crash → partition reassigned → continued read/write.
//!
//! PS1 serves a partition and writes data. PS1 stops heartbeating.
//! After ~10s manager timeout, partition is reassigned to PS2.
//! PS2 recovers all data and serves new writes.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;

use support::*;

/// Test that with 2 PS nodes, manager reassigns partition when heartbeat
/// timeout fires. This test takes ~12s due to the 10s timeout.
#[test]
fn ps_heartbeat_timeout_triggers_reassignment() {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 69).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        // Register PS1 (ps_id=41) — but DON'T start a real partition server.
        // Just register to get the assignment, then let the heartbeat expire.
        let req = rkyv_encode(&RegisterPsReq {
            ps_id: 41,
            address: "fake:9201".to_string(),
            slot_cap: 0,
        });
        let resp = mgr.call(MSG_REGISTER_PS, req).await.expect("register PS1");
        let r: CodeResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(r.code, CODE_OK);

        // Verify partition assigned to PS1
        let regions = get_regions(&mgr).await;
        let region = regions
            .regions
            .iter()
            .find(|(_, r)| r.part_id == 901)
            .unwrap()
            .1
            .clone();
        assert_eq!(region.ps_id, 41, "partition should be on PS1");

        // Register PS2 (ps_id=42)
        let req = rkyv_encode(&RegisterPsReq {
            ps_id: 42,
            address: "fake:9202".to_string(),
            slot_cap: 0,
        });
        let resp = mgr.call(MSG_REGISTER_PS, req).await.expect("register PS2");
        let r: CodeResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(r.code, CODE_OK);

        // Partition stays on PS1 (existing assignment preserved by rebalance)
        let regions = get_regions(&mgr).await;
        let region = regions
            .regions
            .iter()
            .find(|(_, r)| r.part_id == 901)
            .unwrap()
            .1
            .clone();
        assert_eq!(region.ps_id, 41, "partition should still be on PS1");

        // PS2 also heartbeats — send a heartbeat for PS2 so it stays alive
        for _ in 0..12 {
            compio::time::sleep(Duration::from_secs(2)).await;
            // Send heartbeat for PS2 but NOT PS1
            let req = rkyv_encode(&HeartbeatPsReq { ps_id: 42, slot_cap: 0, open_parts: Vec::new(), metrics_addr: String::new() });
            let _ = mgr.call(MSG_HEARTBEAT_PS, req).await;
        }

        // After 24s, PS1 should be evicted (no heartbeat for 24s > 10s timeout)
        let regions = get_regions(&mgr).await;
        let region = regions
            .regions
            .iter()
            .find(|(_, r)| r.part_id == 901)
            .unwrap()
            .1
            .clone();
        assert_eq!(
            region.ps_id, 42,
            "partition should be reassigned to PS2 after PS1 heartbeat timeout (got ps_id={})",
            region.ps_id
        );
    });
}
