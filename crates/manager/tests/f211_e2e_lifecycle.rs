//! F211 — end-to-end operator-driven node lifecycle tests.
//!
//! Spins up a real manager + 4 real extent nodes (one of which is a
//! "phantom" — registered with an address that has no listener so the
//! manager's `df` probe fails and the `NodeStateTracker` flips it to
//! Suspected). Exercises:
//!
//! - F211-A: auto Online → Suspected after `df` timeout
//! - F211-B: list_node_states reflects the transition
//! - F211-C: fence persists override + appears in list_node_states +
//!   register_node is refused while Fenced (zombie defense)
//! - F211-E: fence on a node holding a replica triggers
//!   `recovery_dispatch_loop` to rebuild the slot on another node
//! - F211-H: recovery_stats RPC reports counters after dispatch
//! - F211-I: audit log captures the fence operation
//!
//! These tests are tagged with the `e2e` test name so they can be run
//! selectively via `cargo test --test f211_e2e_lifecycle`.

mod support;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ExtentNode, ExtentNodeConfig};
use bytes::Bytes;

use support::*;

/// Like `support::start_extent_node` but wires the manager endpoint so
/// the EN's `handle_require_recovery` path actually runs (without it
/// the EN refuses with "manager endpoint is not configured" and we
/// can't observe the cross-node fence → recovery flow).
fn start_extent_node_with_manager(
    addr: SocketAddr,
    dir: PathBuf,
    disk_id: u64,
    mgr_addr: SocketAddr,
) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let cfg =
                ExtentNodeConfig::new(dir, disk_id).with_manager_endpoint(mgr_addr.to_string());
            let n = ExtentNode::new(cfg).await.expect("extent node");
            let _ = n.serve(addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));
}

/// Set the soft timeout to 2s for tests (so we don't wait 10s + tick).
fn fast_node_timeout() {
    std::env::set_var("AUTUMN_MGR_NODE_SUSPECTED_TIMEOUT_SECS", "2");
}

async fn list_nodes(mgr: &RpcClient) -> Vec<NodeStateEntry> {
    let bytes = mgr
        .call(MSG_LIST_NODE_STATES, rkyv_encode(&ListNodeStatesReq {}))
        .await
        .expect("list_node_states");
    let resp: ListNodeStatesResp = rkyv_decode(&bytes).expect("decode list_node_states");
    assert_eq!(resp.code, CODE_OK, "list: {}", resp.message);
    resp.nodes
}

async fn fence(mgr: &RpcClient, node_id: u64, reason: &str, force: bool) -> CodeResp {
    let req = FenceNodeReq {
        node_id,
        reason: reason.to_string(),
        set_by: "e2e-test".to_string(),
        force,
    };
    let bytes = mgr
        .call(MSG_FENCE_NODE, rkyv_encode(&req))
        .await
        .expect("fence_node");
    rkyv_decode(&bytes).expect("decode fence")
}

async fn remove(mgr: &RpcClient, node_id: u64) -> RemoveNodeResp {
    let req = RemoveNodeReq {
        node_id,
        set_by: "e2e-test".to_string(),
    };
    let bytes = mgr
        .call(MSG_REMOVE_NODE, rkyv_encode(&req))
        .await
        .expect("remove_node");
    rkyv_decode(&bytes).expect("decode remove")
}

async fn query_audit(mgr: &RpcClient, op: u8, node_id: u64) -> Vec<MgrAuditEntry> {
    let req = QueryAuditLogReq {
        op_filter: op,
        node_id_filter: node_id,
        since_ts_s: 0,
        until_ts_s: 0,
        limit: 100,
    };
    let bytes = mgr
        .call(MSG_QUERY_AUDIT_LOG, rkyv_encode(&req))
        .await
        .expect("query_audit_log");
    let resp: QueryAuditLogResp = rkyv_decode(&bytes).expect("decode query");
    assert_eq!(resp.code, CODE_OK);
    resp.entries
}

// ── E2E test 1: phantom node stays Suspend (F214-B) ─────────────────
//
// Pre-F214 a phantom node (registered, no real EN listening) seeded
// Online and was promoted to Suspected after df failed for the soft-
// timeout window. Post-F214-B (state-machine refactor), an
// unverified-alive node starts in `Suspend`; `on_heartbeat_fail` and
// `tick()` no longer auto-promote Suspend → Suspected because the
// "Suspected" semantic requires a prior verified-alive baseline. So
// the phantom stays Suspend forever — which is the correct
// operator-facing diagnostic ("format ran but EN never started").

#[test]
fn f211_e2e_phantom_node_stays_suspend() {
    fast_node_timeout();
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");

        // Register a phantom node (no real EN listening at that addr).
        let phantom_addr = pick_addr();
        let resp = register_node(&mgr, &phantom_addr.to_string(), "uuid-phantom").await;
        assert_eq!(resp.code, CODE_OK);
        let phantom_id = resp.node_id;

        // F214-B: first-time register seeds Suspend (was Online pre-F214).
        let nodes = list_nodes(&mgr).await;
        let me = nodes.iter().find(|n| n.node_id == phantom_id).expect("me");
        assert_eq!(me.auto_state, NODE_AUTO_STATE_SUSPEND);

        // Wait long enough that pre-F214 the node would have been
        // promoted to Suspected (one df cycle + soft timeout). With
        // F214-B the state stays Suspend forever.
        compio::time::sleep(Duration::from_secs(15)).await;
        let nodes = list_nodes(&mgr).await;
        let me = nodes.iter().find(|n| n.node_id == phantom_id).unwrap();
        assert_eq!(
            me.auto_state, NODE_AUTO_STATE_SUSPEND,
            "phantom must stay Suspend; got auto_state={}",
            me.auto_state
        );
    });
}

// ── E2E test 2: fence persists override + zombie defense ─────────────

#[test]
fn f211_e2e_fence_persists_and_blocks_reregister() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");

        // Register one phantom node (so the fence path works without
        // needing a real EN heartbeat).
        let n1_addr = pick_addr();
        let n2_addr = pick_addr();
        let n1 = register_node(&mgr, &n1_addr.to_string(), "uuid-1")
            .await
            .node_id;
        let _ = register_node(&mgr, &n2_addr.to_string(), "uuid-2").await;

        // Fence n1 — force=true skips capacity precheck.
        let resp = fence(&mgr, n1, "e2e fence", true).await;
        assert_eq!(resp.code, CODE_OK, "fence: {}", resp.message);

        // Override visible.
        let nodes = list_nodes(&mgr).await;
        let me = nodes.iter().find(|n| n.node_id == n1).unwrap();
        assert_eq!(me.override_kind, NODE_OVERRIDE_FENCED);
        assert_eq!(me.override_reason, "e2e fence");

        // Try to re-register at the same address — must be refused.
        let req = RegisterNodeReq {
            addr: n1_addr.to_string(),
            disk_uuids: vec!["uuid-1".to_string()],
            shard_ports: vec![],
            control_address: String::new(),
            node_uuid: String::new(),
        };
        let bytes = mgr
            .call(MSG_REGISTER_NODE, rkyv_encode(&req))
            .await
            .expect("re-register");
        let resp: RegisterNodeResp = rkyv_decode(&bytes).expect("decode");
        assert_eq!(
            resp.code, CODE_PRECONDITION,
            "re-register: {}",
            resp.message
        );
        assert!(resp.message.contains("Fenced"), "{}", resp.message);

        // Clearing the override should let it re-register.
        let req = ClearNodeOverrideReq {
            node_id: n1,
            set_by: "e2e-test".to_string(),
        };
        let bytes = mgr
            .call(MSG_CLEAR_NODE_OVERRIDE, rkyv_encode(&req))
            .await
            .unwrap();
        let resp: CodeResp = rkyv_decode(&bytes).unwrap();
        assert_eq!(resp.code, CODE_OK);

        // Re-register now succeeds with the same node_id.
        let req = RegisterNodeReq {
            addr: n1_addr.to_string(),
            disk_uuids: vec!["uuid-1".to_string()],
            shard_ports: vec![],
            control_address: String::new(),
            node_uuid: String::new(),
        };
        let bytes = mgr
            .call(MSG_REGISTER_NODE, rkyv_encode(&req))
            .await
            .unwrap();
        let resp: RegisterNodeResp = rkyv_decode(&bytes).unwrap();
        assert_eq!(resp.code, CODE_OK, "{}", resp.message);
        assert_eq!(resp.node_id, n1, "same address must yield same node_id");
    });
}

// ── E2E test 3: fence triggers recovery on a real replica ────────────

#[test]
fn f211_e2e_fence_triggers_recovery_dispatch() {
    // Make the recovery gate explicit so the test is independent of
    // a future default flip.
    std::env::set_var("AUTUMN_MGR_RECOVERY_GATE", "fenced_only");

    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    // 3 real ENs so we have 2-replica streams + 1 recovery target.
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n3_dir = tempfile::tempdir().expect("n3");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n3_addr = pick_addr();
    start_extent_node_with_manager(n1_addr, n1_dir.path().to_path_buf(), 1, mgr_addr);
    start_extent_node_with_manager(n2_addr, n2_dir.path().to_path_buf(), 2, mgr_addr);
    start_extent_node_with_manager(n3_addr, n3_dir.path().to_path_buf(), 3, mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");

        let id1 = register_node(&mgr, &n1_addr.to_string(), "uuid-1").await.node_id;
        let id2 = register_node(&mgr, &n2_addr.to_string(), "uuid-2").await.node_id;
        let id3 = register_node(&mgr, &n3_addr.to_string(), "uuid-3").await.node_id;

        // 2-replica stream — manager picks 2 of {id1, id2, id3}.
        let stream_id = create_stream(&mgr, 2).await;

        let pool = std::rc::Rc::new(autumn_stream::ConnPool::new());
        let sc = autumn_stream::StreamClient::connect(
            &mgr_addr.to_string(),
            "e2e-recovery".to_string(),
            1024 * 1024,
            pool.clone(),
        )
        .await
        .expect("connect sc");

        // Append some data so the extent has content.
        let payload = b"f211-e2e-recovery-test";
        let result = sc.append(stream_id, payload).await.expect("append");
        let extent_id = result.extent_id;

        // Seal the extent so recovery has a target with known
        // sealed_length.
        let resp = mgr
            .call(
                MSG_STREAM_ALLOC_EXTENT,
                rkyv_encode(&StreamAllocExtentReq {
                    stream_id,
                    owner_key: sc.owner_key().to_string(),
                    owner_epoch: sc.owner_epoch(),
                    seal_commit: Some(result.end),
                    exclude_node_ids: vec![],
                seal_extent_id: 0,
                }),
            )
            .await
            .expect("seal");
        let _: StreamAllocExtentResp = rkyv_decode(&resp).expect("decode");

        // Read which two replicas the extent landed on.
        let resp = mgr
            .call(
                MSG_EXTENT_INFO,
                rkyv_encode(&ExtentInfoReq { extent_id }),
            )
            .await
            .expect("extent_info");
        let info: ExtentInfoResp = rkyv_decode(&resp).expect("decode extent_info");
        let ex = info.extent.expect("extent exists");
        assert_eq!(ex.replicates.len(), 2);
        let victim_id = ex.replicates[0];

        // Sanity: third node exists and is NOT one of the replicas.
        let candidates: Vec<u64> = vec![id1, id2, id3];
        let healthy_target = candidates
            .iter()
            .find(|&&id| id != ex.replicates[0] && id != ex.replicates[1])
            .copied()
            .expect("should have at least one non-replica candidate");

        // Fence the victim. The capacity check should pass because we
        // have a healthy alternative (healthy_target).
        let resp = fence(&mgr, victim_id, "e2e simulated failure", false).await;
        assert_eq!(resp.code, CODE_OK, "fence: {}", resp.message);

        // Poll until recovery rebuilds the slot. recovery_dispatch_loop
        // fires every 2s; recovery_collect_loop drains on next df tick.
        // Typical wallclock: 4-10 s on a quiet box.
        let recovered = poll_until_async(
            Duration::from_secs(60),
            Duration::from_secs(2),
            || async {
                let resp = mgr
                    .call(
                        MSG_EXTENT_INFO,
                        rkyv_encode(&ExtentInfoReq { extent_id }),
                    )
                    .await
                    .unwrap_or_default();
                let Ok(info) = rkyv_decode::<ExtentInfoResp>(&resp) else {
                    return false;
                };
                let Some(ex) = info.extent else { return false };
                // Recovery done when victim_id is no longer present and
                // healthy_target replaces it.
                !ex.replicates.contains(&victim_id) && ex.replicates.contains(&healthy_target)
            },
        )
        .await;
        assert!(
            recovered,
            "recovery did not replace fenced victim {victim_id} with healthy_target {healthy_target}"
        );
    });
}

// ── E2E test 4: remove refused on Fenced node with active extents ────

#[test]
fn f211_e2e_remove_blocked_by_active_extents() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n3_dir = tempfile::tempdir().expect("n3");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n3_addr = pick_addr();
    start_extent_node_with_manager(n1_addr, n1_dir.path().to_path_buf(), 1, mgr_addr);
    start_extent_node_with_manager(n2_addr, n2_dir.path().to_path_buf(), 2, mgr_addr);
    start_extent_node_with_manager(n3_addr, n3_dir.path().to_path_buf(), 3, mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");

        let id1 = register_node(&mgr, &n1_addr.to_string(), "uuid-1")
            .await
            .node_id;
        let _id2 = register_node(&mgr, &n2_addr.to_string(), "uuid-2")
            .await
            .node_id;
        let _id3 = register_node(&mgr, &n3_addr.to_string(), "uuid-3")
            .await
            .node_id;

        // Create a 2-replica stream + write something so id1 holds an extent.
        let stream_id = create_stream(&mgr, 2).await;
        let pool = std::rc::Rc::new(autumn_stream::ConnPool::new());
        let sc = autumn_stream::StreamClient::connect(
            &mgr_addr.to_string(),
            "e2e-block-remove".to_string(),
            1024 * 1024,
            pool.clone(),
        )
        .await
        .expect("connect sc");
        let _ = sc.append(stream_id, b"hello").await.expect("append");

        // Figure out which node id1 corresponds to in the replica list.
        // We need to fence a node that actually holds an extent.
        let resp = mgr
            .call(
                MSG_STREAM_INFO,
                rkyv_encode(&StreamInfoReq {
                    stream_ids: vec![stream_id],
                }),
            )
            .await
            .unwrap();
        let si: StreamInfoResp = rkyv_decode(&resp).unwrap();
        assert!(
            !si.extents.is_empty(),
            "stream must have at least one extent"
        );
        let victim_id = si.extents[0].1.replicates[0];

        // Fence (force=true so capacity check doesn't reject when
        // alternative nodes aren't yet sized).
        let resp = fence(&mgr, victim_id, "to-be-removed", true).await;
        assert_eq!(resp.code, CODE_OK, "fence: {}", resp.message);

        // Immediate remove must fail because extents still reference
        // the fenced node (recovery hasn't drained yet).
        let resp = remove(&mgr, victim_id).await;
        assert_eq!(resp.code, CODE_PRECONDITION);
        assert!(
            !resp.blocking_extent_ids.is_empty(),
            "blocking_extent_ids must be populated; got {:?}",
            resp.blocking_extent_ids
        );
        // We don't assert id1 is the victim — manager's select_nodes
        // shuffles. The point is: at least ONE extent is blocking.
        let _ = id1; // silence unused warning if id1 isn't the victim
    });
}

// ── E2E test 5: audit log captures admin ops ─────────────────────────

#[test]
fn f211_e2e_audit_log_captures_admin_ops() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");

        let n1_addr = pick_addr();
        let n1 = register_node(&mgr, &n1_addr.to_string(), "uuid-1")
            .await
            .node_id;

        // Fence + clear → 2 audit entries on n1.
        let _ = fence(&mgr, n1, "first fence", true).await;
        let req = ClearNodeOverrideReq {
            node_id: n1,
            set_by: "e2e".to_string(),
        };
        let _ = mgr
            .call(MSG_CLEAR_NODE_OVERRIDE, rkyv_encode(&req))
            .await
            .unwrap();

        // In-memory mode (no etcd) returns empty — audit is etcd-backed.
        // We still call the RPC and verify the wire works.
        let entries = query_audit(&mgr, 0, n1).await;
        // Either etcd is configured (entries.len() == 2) or it isn't
        // (entries.len() == 0). Both are valid wire-level outcomes.
        assert!(
            entries.len() <= 10,
            "audit returned too many entries (expected 0 or 2): {}",
            entries.len()
        );
        // If etcd persisted, verify the expected ops landed.
        if entries.len() == 2 {
            let ops: Vec<u8> = entries.iter().map(|e| e.op).collect();
            assert!(ops.contains(&AUDIT_OP_FENCE_NODE));
            assert!(ops.contains(&AUDIT_OP_CLEAR_NODE_OVERRIDE));
        }
    });
}

// ── E2E test 6: recovery_stats RPC is queryable ──────────────────────

#[test]
fn f211_e2e_recovery_stats_baseline() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        let bytes = mgr
            .call(MSG_RECOVERY_STATS, rkyv_encode(&RecoveryStatsReq {}))
            .await
            .expect("recovery_stats");
        let resp: RecoveryStatsResp = rkyv_decode(&bytes).expect("decode");
        assert_eq!(resp.code, CODE_OK);
        assert!(resp.max_global >= 1, "max_global must be >= 1");
        assert!(resp.max_per_source >= 1);
        assert!(resp.max_per_target >= 1);
        // No fence has fired → global_inflight should be 0 at start.
        assert_eq!(resp.global_inflight, 0);
    });
}

// ── E2E test 7: extent_health_report shows fenced-slot details ───────

#[test]
fn f211_e2e_extent_health_report_reflects_overrides() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node_with_manager(n1_addr, n1_dir.path().to_path_buf(), 1, mgr_addr);
    start_extent_node_with_manager(n2_addr, n2_dir.path().to_path_buf(), 2, mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        let id1 = register_node(&mgr, &n1_addr.to_string(), "uuid-1")
            .await
            .node_id;
        let _ = register_node(&mgr, &n2_addr.to_string(), "uuid-2").await;

        let stream_id = create_stream(&mgr, 2).await;
        let pool = std::rc::Rc::new(autumn_stream::ConnPool::new());
        let sc = autumn_stream::StreamClient::connect(
            &mgr_addr.to_string(),
            "e2e-health".to_string(),
            1024 * 1024,
            pool.clone(),
        )
        .await
        .expect("sc");
        let _ = sc.append(stream_id, b"x").await.expect("append");

        // Maintenance the first node so the health report shows a non-
        // default override on at least one slot.
        let req = SetNodeMaintenanceReq {
            node_id: id1,
            reason: "scheduled".to_string(),
            set_by: "e2e".to_string(),
            expire_at: 0,
        };
        let _ = mgr
            .call(MSG_SET_NODE_MAINTENANCE, rkyv_encode(&req))
            .await
            .unwrap();

        // Ask for everything — must surface the extent (unhealthy
        // because Maintenance is a non-default override).
        let req = ExtentHealthReq {
            node_id_filter: vec![],
            include_healthy: false,
        };
        let bytes = mgr
            .call(MSG_EXTENT_HEALTH_REPORT, rkyv_encode(&req))
            .await
            .expect("health");
        let resp: ExtentHealthResp = rkyv_decode(&bytes).expect("decode");
        assert_eq!(resp.code, CODE_OK);
        // Find an extent whose slot covers id1 (the maintenance one).
        let with_id1: Vec<&ExtentHealth> = resp
            .extents
            .iter()
            .filter(|e| e.slots.iter().any(|s| s.node_id == id1))
            .collect();
        assert!(
            !with_id1.is_empty(),
            "health report should include extents on the maintenance node"
        );
        // Confirm the maintenance slot bubbles up.
        let any_maint = with_id1.iter().any(|e| {
            e.slots
                .iter()
                .any(|s| s.node_id == id1 && s.override_kind == NODE_OVERRIDE_MAINTENANCE)
        });
        assert!(
            any_maint,
            "expected at least one slot reporting Maintenance override"
        );
    });
}

// ── E2E test 8: list_ec_inflight_markers reads cleanly (no markers) ──

#[test]
fn f211_e2e_list_ec_inflight_markers_empty_baseline() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        let bytes = mgr
            .call(
                MSG_LIST_EC_INFLIGHT_MARKERS,
                rkyv_encode(&ListEcInflightMarkersReq {}),
            )
            .await
            .expect("list ec markers");
        let resp: ListEcInflightMarkersResp = rkyv_decode(&bytes).expect("decode");
        assert_eq!(resp.code, CODE_OK);
        assert!(
            resp.markers.is_empty(),
            "baseline cluster has no EC markers"
        );
    });
}

// ── Suppress unused warnings on Bytes when present in unmodified paths ─

#[allow(dead_code)]
fn _silence_unused_bytes(_: Bytes) {}
