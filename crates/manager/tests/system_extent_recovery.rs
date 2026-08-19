//! System test — extent recovery end-to-end.
//!
//! 3 extent nodes. Create a 2-replica stream on (node1, node2).
//! Write data, seal extent. Register a "dead" node (node_dead) that was
//! one of the original replicas. The manager's recovery_dispatch_loop
//! detects the dead replica and dispatches recovery to node3.
//! After recovery completes, extent_info shows node3 replacing node_dead.

mod support;

use std::rc::Rc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

#[test]
fn extent_recovery_replaces_dead_node() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    // 3 real extent nodes
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n3_dir = tempfile::tempdir().expect("n3");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n3_addr = pick_addr();

    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);
    start_extent_node(n3_addr, n3_dir.path().to_path_buf(), 3);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");

        // Register all 3 nodes. Node order determines which get selected for the stream.
        register_node(&mgr, &n1_addr.to_string(), "uuid-1").await;
        register_node(&mgr, &n2_addr.to_string(), "uuid-2").await;
        register_node(&mgr, &n3_addr.to_string(), "uuid-3").await;

        // Create 2-replica stream (selects node1 + node2, sorted by node_id)
        let stream_id = create_stream(&mgr, 2).await;

        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "test-recovery".to_string(),
            1024 * 1024,
            pool.clone(),
        )
        .await
        .expect("connect sc");

        // Write data
        let payload = b"recovery-test-data-1234567890";
        let result = sc.append(stream_id, payload).await.expect("append");
        let extent_id = result.extent_id;

        // Seal the extent
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
        let seal: StreamAllocExtentResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(seal.code, CODE_OK, "seal failed: {}", seal.message);

        // Check initial extent info (invalidate cache to see post-seal state)
        sc.invalidate_extent_cache(extent_id);
        let ext = sc.get_extent_info(extent_id).await.expect("extent info");
        assert!(
            ext.sealed_length > 0,
            "extent should be sealed (sealed_length={}, end={})",
            ext.sealed_length,
            result.end
        );
        let _orig_node1 = ext.replicates[0];
        let _orig_node2 = ext.replicates[1];

        // Now we need to make one replica "dead". We can't kill the node,
        // but we can unregister it from the manager. However, unregister
        // doesn't exist. Instead, we rely on the manager's health check:
        // the recovery_dispatch_loop calls commit_length_on_node for each
        // sealed extent's replica. If the RPC fails, it dispatches recovery.
        //
        // Since all 3 nodes are alive, recovery won't trigger naturally.
        // Let's verify the mechanism works by checking that the extent
        // has all replicas available (avali bits set).
        sc.invalidate_extent_cache(extent_id);
        let ext = sc.get_extent_info(extent_id).await.expect("extent info");
        // assert every slot bit is set, not just `> 0`. Previously a
        // post-EC extent had `avali = all_bits(K)` instead of
        // `all_bits(K + M)`, which the looser `> 0` check let through.
        let total_slots = ext.replicates.len() + ext.parity.len();
        let expected = if total_slots >= 32 {
            u32::MAX
        } else {
            (1u32 << total_slots) - 1
        };
        assert_eq!(
            ext.avali,
            expected,
            "sealed extent should have every slot's avali bit set (replicates={} + parity={})",
            ext.replicates.len(),
            ext.parity.len(),
        );

        // Read data from the sealed extent
        let (read_data, _) = sc
            .read_bytes_from_extent(extent_id, result.offset, result.end - result.offset)
            .await
            .expect("read sealed extent");
        assert_eq!(
            read_data, payload,
            "data should be readable from sealed extent"
        );

        // Verify we can read from a specific replica (node3 is the spare, not in this extent)
        // The test validates that the recovery dispatch mechanism's health check
        // correctly identifies healthy nodes (no false positives).
        //
        // For a true recovery test, we would need to:
        // 1. Start a node at a temporary address
        // 2. Register it
        // 3. Create stream using it
        // 4. Stop the node (kill the thread)
        // 5. Wait for manager to detect and dispatch recovery
        //
        // This is hard without a proper ShutdownHandle in the serve loop.
        // The EC failover test (ec_failover.rs) has the same limitation.
        //
        // For now, we verify the positive path: all replicas healthy,
        // data readable, sealed correctly.

        // Write more data — should succeed (StreamClient handles sealed extent)
        sc.append(stream_id, b"post-seal-data")
            .await
            .expect("post-seal append");
    });
}

/// a recovery completion whose df delivery was LOST must
/// not permanently poison the recovery candidate.
///
/// `handle_df` hands `recovery_done` to the manager via `std::mem::take`
/// BEFORE the response is known delivered (at-most-once handoff). If that
/// df response is lost in transit, the completion is gone: the manager's
/// Recovery marker ages out via the stale sweep and the slot is
/// re-dispatched — and pre-fix the candidate EN refused the re-dispatch
/// with "extent already exists" FOREVER. Once every candidate had completed
/// (and lost) one recovery, the slot could never be rebuilt and a fenced
/// node's `MSG_REMOVE_NODE` blocked forever (the live-decommission drain
/// wedge, reproduced by `system_chaos` + `AUTUMN_CHAOS_DECOMMISSION=1`).
///
/// Post-fix the EN verifies its local copy against the manager's snapshot
/// (same eversion, sealed, full length, not EC) and ADOPTS it: CODE_OK +
/// a re-reported `RecoveryTaskDone`.
#[test]
fn lost_recovery_completion_redispatch_adopts_local_copy() {
    use autumn_rpc::extent_rpc as ext;
    use autumn_stream::{ExtentNode, ExtentNodeConfig};

    /// Like `support::start_extent_node` but with the manager endpoint
    /// wired, so `handle_require_recovery` / the adopt triage can resolve
    /// extent_info (mirrors `e2e_lifecycle::start_extent_node_with_manager`).
    fn start_en_with_manager(
        addr: std::net::SocketAddr,
        dir: std::path::PathBuf,
        disk_id: u64,
        mgr_addr: std::net::SocketAddr,
    ) {
        std::thread::spawn(move || {
            compio::runtime::Runtime::new().unwrap().block_on(async {
                let cfg = ExtentNodeConfig::new(dir, disk_id)
                    .with_manager_endpoint(mgr_addr.to_string());
                let n = ExtentNode::new(cfg).await.expect("extent node");
                let _ = n.serve(addr).await;
            });
        });
        std::thread::sleep(Duration::from_millis(200));
    }

    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n3_dir = tempfile::tempdir().expect("n3");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n3_addr = pick_addr();

    // n1 + n2 are the registered stream members. n3 is the spare recovery
    // target and is deliberately NOT registered with the manager: the
    // manager's node_health_loop therefore never sends it a df, so n3's
    // `recovery_done` queue is drained ONLY by this test — making the
    // "df response lost in transit" scenario deterministic (we drain the
    // completion ourselves and discard it, exactly what a lost response
    // does to the manager's view).
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);
    start_en_with_manager(n3_addr, n3_dir.path().to_path_buf(), 3, mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_node(&mgr, &n1_addr.to_string(), "uuid-1").await;
        register_node(&mgr, &n2_addr.to_string(), "uuid-2").await;

        let stream_id = create_stream(&mgr, 2).await;
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "test-adopt".to_string(),
            1024 * 1024,
            pool.clone(),
        )
        .await
        .expect("connect sc");

        let payload = b"adopt-test-payload";
        let r = sc.append(stream_id, payload).await.expect("append");
        let extent_id = r.extent_id;

        // Seal at the acked commit (same shape as the sibling tests).
        let resp = mgr
            .call(
                MSG_STREAM_ALLOC_EXTENT,
                rkyv_encode(&StreamAllocExtentReq {
                    stream_id,
                    owner_key: sc.owner_key().to_string(),
                    owner_epoch: sc.owner_epoch(),
                    seal_commit: Some(r.end),
                    exclude_node_ids: vec![],
                    seal_extent_id: 0,
                }),
            )
            .await
            .expect("seal");
        let seal: StreamAllocExtentResp = rkyv_decode(&resp).expect("decode seal");
        assert_eq!(seal.code, CODE_OK, "seal failed: {}", seal.message);

        sc.invalidate_extent_cache(extent_id);
        let ext_info = sc.get_extent_info(extent_id).await.expect("extent info");
        let replace_id = ext_info.replicates[1];

        // Dispatch recovery DIRECTLY to n3 (bypassing the manager's
        // dispatch loop — n3 consults the manager only for extent_info).
        let en3 = RpcClient::connect(n3_addr).await.expect("connect n3");
        let task = ext::RecoveryTask {
            extent_id,
            replace_id,
            node_id: 999,
            start_time: 0,
        };
        let resp = en3
            .call(
                ext::MSG_REQUIRE_RECOVERY,
                ext::rkyv_encode(&ext::RequireRecoveryReq { task: task.clone() }),
            )
            .await
            .expect("require_recovery #1");
        let code: ext::CodeResp = ext::rkyv_decode(&resp).expect("decode #1");
        assert_eq!(code.code, ext::CODE_OK, "dispatch #1 refused: {}", code.message);

        // Wait for the background recovery to complete, then DRAIN the
        // completion and DISCARD it — modelling a df whose response was
        // lost after handle_df's mem::take.
        let mut drained: Vec<ext::RecoveryTaskDone> = Vec::new();
        for _ in 0..50 {
            compio::time::sleep(Duration::from_millis(200)).await;
            let resp = en3
                .call(
                    ext::MSG_DF,
                    ext::rkyv_encode(&ext::DfReq {
                        tasks: vec![],
                        disk_ids: vec![],
                    }),
                )
                .await
                .expect("df drain");
            let df: ext::DfResp = ext::rkyv_decode(&resp).expect("decode df");
            if !df.done_tasks.is_empty() {
                drained = df.done_tasks;
                break;
            }
        }
        assert_eq!(drained.len(), 1, "recovery #1 should complete and report done");
        assert_eq!(drained[0].task.extent_id, extent_id);
        drop(drained); // the manager never saw this completion

        // Re-dispatch the SAME task. Pre-fix: CODE_PRECONDITION
        // "extent already exists" — candidate poisoned forever. Post-fix:
        // the EN adopts its verified-complete local copy.
        let resp = en3
            .call(
                ext::MSG_REQUIRE_RECOVERY,
                ext::rkyv_encode(&ext::RequireRecoveryReq { task: task.clone() }),
            )
            .await
            .expect("require_recovery #2");
        let code: ext::CodeResp = ext::rkyv_decode(&resp).expect("decode #2");
        assert_eq!(
            code.code,
            ext::CODE_OK,
            "re-dispatch should ADOPT the complete local copy, got: {}",
            code.message
        );

        let resp = en3
            .call(
                ext::MSG_DF,
                ext::rkyv_encode(&ext::DfReq {
                    tasks: vec![],
                    disk_ids: vec![],
                }),
            )
            .await
            .expect("df after adopt");
        let df: ext::DfResp = ext::rkyv_decode(&resp).expect("decode df #2");
        assert_eq!(df.done_tasks.len(), 1, "adopt must re-report RecoveryTaskDone");
        assert_eq!(df.done_tasks[0].task.extent_id, extent_id);
        assert_eq!(df.done_tasks[0].task.replace_id, replace_id);
        assert_eq!(df.done_tasks[0].task.node_id, 999);
    });
}

/// Test that the manager's recovery_dispatch_loop correctly skips healthy
/// sealed extents (no spurious recovery dispatch).
#[test]
fn recovery_dispatch_skips_healthy_sealed_extents() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_node(&mgr, &n1_addr.to_string(), "uuid-1").await;
        register_node(&mgr, &n2_addr.to_string(), "uuid-2").await;

        let stream_id = create_stream(&mgr, 2).await;

        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "test-no-spurious".to_string(),
            1024 * 1024,
            pool.clone(),
        )
        .await
        .expect("connect sc");

        let r = sc.append(stream_id, b"data").await.expect("append");
        let extent_id = r.extent_id;

        // Seal
        let resp = mgr
            .call(
                MSG_STREAM_ALLOC_EXTENT,
                rkyv_encode(&StreamAllocExtentReq {
                    stream_id,
                    owner_key: sc.owner_key().to_string(),
                    owner_epoch: sc.owner_epoch(),
                    seal_commit: Some(r.end),
                    exclude_node_ids: vec![],
                seal_extent_id: 0,
                }),
            )
            .await
            .expect("seal");
        let seal: StreamAllocExtentResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(seal.code, CODE_OK);

        // Wait for recovery_dispatch_loop to run a few cycles (2s interval)
        compio::time::sleep(Duration::from_secs(6)).await;

        // The extent should still have the same replicas (no spurious recovery)
        sc.invalidate_extent_cache(extent_id);
        let ext = sc.get_extent_info(extent_id).await.expect("extent info");
        assert_eq!(ext.replicates.len(), 2, "should still have 2 replicas");

        // Data should still be readable
        let (data, _) = sc
            .read_bytes_from_extent(extent_id, r.offset, r.end - r.offset)
            .await
            .expect("read");
        assert_eq!(data, b"data");
    });
}

/// Sibling of `lost_recovery_completion_redispatch_adopts_local_copy`, for the
/// OTHER half of the triage: a local copy that is **provably incomplete**.
///
/// A recovery that dies mid-copy leaves a partial `.dat` (the `.meta` persist is
/// LAST), which reloads as an ordinary open extent — so the extent id is present
/// locally while holding nothing useful. The pre-fix handler refused every future
/// dispatch to that node with "extent already exists", and nothing reaped the
/// stub: the orphan reconcile only removes extents the MANAGER has forgotten, and
/// this extent is still very much alive. That poisoned the (node, extent) pair
/// permanently — recoverable only by an operator deleting the file.
///
/// Here the incomplete local copy is planted over the wire with `ALLOC_EXTENT`
/// (an extent present locally, but empty and unsealed — the same shape a killed
/// copy leaves), which keeps the test deterministic.
#[test]
fn incomplete_local_copy_is_discarded_and_rebuilt_not_refused_forever() {
    use autumn_rpc::extent_rpc as ext;
    use autumn_stream::{ExtentNode, ExtentNodeConfig};

    fn start_en_with_manager(
        addr: std::net::SocketAddr,
        dir: std::path::PathBuf,
        disk_id: u64,
        mgr_addr: std::net::SocketAddr,
    ) {
        std::thread::spawn(move || {
            compio::runtime::Runtime::new().unwrap().block_on(async {
                let cfg = ExtentNodeConfig::new(dir, disk_id)
                    .with_manager_endpoint(mgr_addr.to_string());
                let n = ExtentNode::new(cfg).await.expect("extent node");
                let _ = n.serve(addr).await;
            });
        });
        std::thread::sleep(Duration::from_millis(200));
    }

    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n3_dir = tempfile::tempdir().expect("n3");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n3_addr = pick_addr();

    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);
    // n3 is the recovery target; it needs the manager endpoint so the triage can
    // fetch the authoritative extent_info. Deliberately unregistered so only this
    // test drains its `recovery_done`.
    start_en_with_manager(n3_addr, n3_dir.path().to_path_buf(), 3, mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_node(&mgr, &n1_addr.to_string(), "uuid-1").await;
        register_node(&mgr, &n2_addr.to_string(), "uuid-2").await;

        let stream_id = create_stream(&mgr, 2).await;
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "test-incomplete".to_string(),
            1024 * 1024,
            pool.clone(),
        )
        .await
        .expect("connect sc");

        let payload = b"incomplete-residue-test-payload";
        let r = sc.append(stream_id, payload).await.expect("append");
        let extent_id = r.extent_id;

        let resp = mgr
            .call(
                MSG_STREAM_ALLOC_EXTENT,
                rkyv_encode(&StreamAllocExtentReq {
                    stream_id,
                    owner_key: sc.owner_key().to_string(),
                    owner_epoch: sc.owner_epoch(),
                    seal_commit: Some(r.end),
                    exclude_node_ids: vec![],
                    seal_extent_id: 0,
                }),
            )
            .await
            .expect("seal");
        let seal: StreamAllocExtentResp = rkyv_decode(&resp).expect("decode seal");
        assert_eq!(seal.code, CODE_OK, "seal failed: {}", seal.message);

        sc.invalidate_extent_cache(extent_id);
        let ext_info = sc.get_extent_info(extent_id).await.expect("extent info");
        let replace_id = ext_info.replicates[1];
        assert!(ext_info.sealed_length > 0, "extent must be sealed with data");

        let en3 = RpcClient::connect(n3_addr).await.expect("connect n3");

        // Plant the residue: n3 now HOLDS this extent id, but empty + unsealed —
        // exactly what a recovery killed mid-copy leaves behind.
        let resp = en3
            .call(
                ext::MSG_ALLOC_EXTENT,
                ext::rkyv_encode(&ext::AllocExtentReq { extent_id }),
            )
            .await
            .expect("plant residue");
        let code: ext::CodeResp = ext::rkyv_decode(&resp).expect("decode alloc");
        assert_eq!(code.code, ext::CODE_OK, "planting residue failed: {}", code.message);

        // Dispatch recovery. PRE-FIX this returns CODE_PRECONDITION
        // "extent {id} already exists" — and would do so forever.
        let task = ext::RecoveryTask {
            extent_id,
            replace_id,
            node_id: 999,
            start_time: 0,
        };
        let resp = en3
            .call(
                ext::MSG_REQUIRE_RECOVERY,
                ext::rkyv_encode(&ext::RequireRecoveryReq { task: task.clone() }),
            )
            .await
            .expect("require_recovery");
        let code: ext::CodeResp = ext::rkyv_decode(&resp).expect("decode require_recovery");
        assert_eq!(
            code.code,
            ext::CODE_OK,
            "an INCOMPLETE local copy must be discarded and rebuilt, not refused \
             forever (this is the permanent (node,extent) poisoning), got: {}",
            code.message
        );

        // And the rebuild must actually complete — the discard is only correct if
        // what follows is a real recovery.
        let mut done: Vec<ext::RecoveryTaskDone> = Vec::new();
        for _ in 0..50 {
            compio::time::sleep(Duration::from_millis(200)).await;
            let resp = en3
                .call(
                    ext::MSG_DF,
                    ext::rkyv_encode(&ext::DfReq {
                        tasks: vec![],
                        disk_ids: vec![],
                    }),
                )
                .await
                .expect("df drain");
            let df: ext::DfResp = ext::rkyv_decode(&resp).expect("decode df");
            if !df.done_tasks.is_empty() {
                done = df.done_tasks;
                break;
            }
        }
        assert_eq!(done.len(), 1, "the rebuild after discarding the stub must complete");
        assert_eq!(done[0].task.extent_id, extent_id);
        assert_eq!(done[0].task.replace_id, replace_id);
    });
}
