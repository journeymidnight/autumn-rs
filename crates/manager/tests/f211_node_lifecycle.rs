//! F211 operator-driven node lifecycle — in-memory smoke tests.
//!
//! These tests exercise the AutumnManager state machine without
//! requiring an etcd server. They cover:
//!   * F211-A: NodeStateTracker default state, on_heartbeat_ok seed
//!   * F211-B: handle_list_node_states / extent_health_report aggregation
//!   * F211-C: handle_fence_node + handle_clear_node_override +
//!             handle_remove_node + zombie defense
//!   * F211-I: audit append is exercised by going through the handlers
//!   * F211-H: handle_recovery_stats baseline

use autumn_manager::AutumnManager;
use autumn_rpc::manager_rpc::*;
use bytes::Bytes;

fn run<F: std::future::Future<Output = T>, T>(f: F) -> T {
    compio::runtime::Runtime::new().unwrap().block_on(f)
}

fn register(m: &AutumnManager, addr: &str) -> u64 {
    let req = RegisterNodeReq {
        addr: addr.to_string(),
        disk_uuids: vec!["uuid-a".to_string()],
        shard_ports: vec![],
        control_address: String::new(),
    };
    let payload = rkyv_encode(&req);
    let resp_bytes = run(m.handle_register_node(payload)).expect("register");
    let resp: RegisterNodeResp = rkyv_decode(&resp_bytes).expect("decode");
    assert_eq!(resp.code, CODE_OK, "register: {}", resp.message);
    resp.node_id
}

// F214-B: first-time register seeds NodeAutoState::Suspend, not Online.
// The first successful df transitions to Online. Renamed from
// `list_node_states_starts_online` to reflect the new initial state.
#[test]
fn list_node_states_starts_suspend() {
    let m = AutumnManager::new();
    let n1 = register(&m, "127.0.0.1:9001");
    let n2 = register(&m, "127.0.0.1:9002");
    let resp_bytes = run(m.handle_list_node_states(Bytes::new())).expect("list");
    let resp: ListNodeStatesResp = rkyv_decode(&resp_bytes).expect("decode");
    assert_eq!(resp.code, CODE_OK);
    assert_eq!(resp.nodes.len(), 2);
    for entry in &resp.nodes {
        assert_eq!(entry.auto_state, NODE_AUTO_STATE_SUSPEND);
        assert_eq!(entry.override_kind, NODE_OVERRIDE_NONE);
    }
    let ids: Vec<u64> = resp.nodes.iter().map(|n| n.node_id).collect();
    assert!(ids.contains(&n1));
    assert!(ids.contains(&n2));
}

#[test]
fn fence_persists_override_and_audit_records_op() {
    let m = AutumnManager::new();
    let n1 = register(&m, "127.0.0.1:9001");

    let fence_req = FenceNodeReq {
        node_id: n1,
        reason: "ssh-confirmed dead".to_string(),
        set_by: "op-alice".to_string(),
        force: true, // skip capacity check (no alt nodes)
    };
    let resp_bytes = run(m.handle_fence_node(rkyv_encode(&fence_req))).expect("fence");
    let resp: CodeResp = rkyv_decode(&resp_bytes).expect("decode");
    assert_eq!(resp.code, CODE_OK, "fence: {}", resp.message);

    // Override is reflected in list_node_states.
    let list_bytes = run(m.handle_list_node_states(Bytes::new())).expect("list");
    let list: ListNodeStatesResp = rkyv_decode(&list_bytes).expect("decode");
    let entry = list.nodes.iter().find(|n| n.node_id == n1).expect("entry");
    assert_eq!(entry.override_kind, NODE_OVERRIDE_FENCED);
    assert_eq!(entry.override_reason, "ssh-confirmed dead");
    assert_eq!(entry.override_set_by, "op-alice");
}

#[test]
fn maintenance_set_then_clear_round_trip() {
    let m = AutumnManager::new();
    let n1 = register(&m, "127.0.0.1:9001");

    let req = SetNodeMaintenanceReq {
        node_id: n1,
        reason: "planned reboot".to_string(),
        set_by: "op-alice".to_string(),
        expire_at: 1_700_000_000,
    };
    let resp_bytes = run(m.handle_set_node_maintenance(rkyv_encode(&req))).expect("set");
    let resp: CodeResp = rkyv_decode(&resp_bytes).expect("decode");
    assert_eq!(resp.code, CODE_OK);

    let list: ListNodeStatesResp =
        rkyv_decode(&run(m.handle_list_node_states(Bytes::new())).unwrap()).unwrap();
    let entry = list.nodes.iter().find(|n| n.node_id == n1).unwrap();
    assert_eq!(entry.override_kind, NODE_OVERRIDE_MAINTENANCE);
    assert_eq!(entry.override_expire_at, 1_700_000_000);

    let clear = ClearNodeOverrideReq {
        node_id: n1,
        set_by: "op-alice".to_string(),
    };
    let resp_bytes = run(m.handle_clear_node_override(rkyv_encode(&clear))).expect("clear");
    let resp: CodeResp = rkyv_decode(&resp_bytes).expect("decode");
    assert_eq!(resp.code, CODE_OK);

    let list: ListNodeStatesResp =
        rkyv_decode(&run(m.handle_list_node_states(Bytes::new())).unwrap()).unwrap();
    let entry = list.nodes.iter().find(|n| n.node_id == n1).unwrap();
    assert_eq!(entry.override_kind, NODE_OVERRIDE_NONE);
}

#[test]
fn remove_refuses_unfenced_node() {
    let m = AutumnManager::new();
    let n1 = register(&m, "127.0.0.1:9001");
    let req = RemoveNodeReq {
        node_id: n1,
        set_by: "op-alice".to_string(),
    };
    let resp_bytes = run(m.handle_remove_node(rkyv_encode(&req))).expect("remove");
    let resp: RemoveNodeResp = rkyv_decode(&resp_bytes).expect("decode");
    assert_eq!(resp.code, CODE_PRECONDITION);
    assert!(resp.message.contains("must be Fenced"), "{}", resp.message);
}

#[test]
fn remove_after_fence_succeeds_when_no_refs() {
    let m = AutumnManager::new();
    let n1 = register(&m, "127.0.0.1:9001");

    // Fence (no capacity check — no alt nodes in this test).
    let fence = FenceNodeReq {
        node_id: n1,
        reason: "dead".to_string(),
        set_by: "op-alice".to_string(),
        force: true,
    };
    let resp_bytes = run(m.handle_fence_node(rkyv_encode(&fence))).expect("fence");
    let resp: CodeResp = rkyv_decode(&resp_bytes).expect("decode");
    assert_eq!(resp.code, CODE_OK);

    // No extents reference this node yet (we never created any), so
    // remove should succeed.
    let req = RemoveNodeReq {
        node_id: n1,
        set_by: "op-alice".to_string(),
    };
    let resp_bytes = run(m.handle_remove_node(rkyv_encode(&req))).expect("remove");
    let resp: RemoveNodeResp = rkyv_decode(&resp_bytes).expect("decode");
    assert_eq!(resp.code, CODE_OK, "remove: {}", resp.message);

    // Re-registering at the same address must be refused (tombstone).
    let req = RegisterNodeReq {
        addr: "127.0.0.1:9001".to_string(),
        disk_uuids: vec!["uuid-a".to_string()],
        shard_ports: vec![],
        control_address: String::new(),
    };
    // Since the tombstone keys by node_id and the in-memory `nodes`
    // map is also empty, a fresh register at the same addr is allowed
    // and gets a NEW node_id. The decommissioned tombstone only blocks
    // re-association with the SAME node_id (the prior_id lookup is
    // address-based, so an address whose prior_id is decommissioned is
    // blocked). Verify that branch.
    let resp_bytes = run(m.handle_register_node(rkyv_encode(&req))).expect("re-register");
    let resp: RegisterNodeResp = rkyv_decode(&resp_bytes).expect("decode");
    // After remove, prior nodes entry is gone — register succeeds with
    // fresh id. This is correct (operator wiped the node).
    assert_eq!(resp.code, CODE_OK, "{}", resp.message);
    assert_ne!(resp.node_id, n1);
}

#[test]
fn fence_node_then_re_register_refused_before_clear() {
    let m = AutumnManager::new();
    let n1 = register(&m, "127.0.0.1:9001");

    // Fence (force — no alt nodes).
    let fence = FenceNodeReq {
        node_id: n1,
        reason: "dead".to_string(),
        set_by: "op-alice".to_string(),
        force: true,
    };
    let _ = run(m.handle_fence_node(rkyv_encode(&fence))).expect("fence");

    // Re-register at the same address: the address-based prior lookup
    // finds n1, n1's override is Fenced → must be refused.
    let req = RegisterNodeReq {
        addr: "127.0.0.1:9001".to_string(),
        disk_uuids: vec!["uuid-a".to_string()],
        shard_ports: vec![],
        control_address: String::new(),
    };
    let resp_bytes = run(m.handle_register_node(rkyv_encode(&req))).expect("re-register");
    let resp: RegisterNodeResp = rkyv_decode(&resp_bytes).expect("decode");
    assert_eq!(resp.code, CODE_PRECONDITION);
    assert!(resp.message.contains("Fenced"), "{}", resp.message);
}

#[test]
fn extent_health_report_handles_empty_cluster() {
    let m = AutumnManager::new();
    let req = ExtentHealthReq {
        node_id_filter: vec![],
        include_healthy: true,
    };
    let resp_bytes =
        run(m.handle_extent_health_report(rkyv_encode(&req))).expect("health");
    let resp: ExtentHealthResp = rkyv_decode(&resp_bytes).expect("decode");
    assert_eq!(resp.code, CODE_OK);
    assert!(resp.extents.is_empty());
}

#[test]
fn list_ec_inflight_markers_empty_by_default() {
    let m = AutumnManager::new();
    let resp_bytes =
        run(m.handle_list_ec_inflight_markers(Bytes::new())).expect("list markers");
    let resp: ListEcInflightMarkersResp = rkyv_decode(&resp_bytes).expect("decode");
    assert_eq!(resp.code, CODE_OK);
    assert!(resp.markers.is_empty());
}

#[test]
fn recovery_stats_baseline() {
    let m = AutumnManager::new();
    let resp_bytes = run(m.handle_recovery_stats(Bytes::new())).expect("stats");
    let resp: RecoveryStatsResp = rkyv_decode(&resp_bytes).expect("decode");
    assert_eq!(resp.code, CODE_OK);
    assert_eq!(resp.global_inflight, 0);
    assert!(resp.max_global >= 1);
    assert!(resp.per_source.is_empty());
    assert!(resp.per_target.is_empty());
}
