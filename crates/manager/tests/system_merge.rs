#![allow(
    dead_code,
    unused_must_use,
    clippy::redundant_pattern_matching,
    clippy::if_same_then_else
)] // integration-test file
//! System tests for partition merge primitive.
//!
//! Cluster topology: manager + 2 extent-nodes + 1 PS. Tests drive the
//! manager's orchestrated merge (`MSG_MERGE_PARTITIONS`: freeze both,
//! capture commit lengths, commit the merge txn) end to end.
//!
//! Coverage:
//!  - happy path: split → merge round-trip; all keys readable from survivor
//!  - refusal: non-adjacent partitions → Precondition
//!  - refusal: self-merge → Precondition

mod support;

use std::rc::Rc;
use std::time::Duration;

use autumn_partition_server::replay_read_bytes;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

/// Merge `victim` into `survivor` through the orchestrated
/// `MSG_MERGE_PARTITIONS`.
async fn merge_partitions(mgr: &RpcClient, survivor: u64, victim: u64) -> CodeResp {
    let bytes = mgr
        .call(
            MSG_MERGE_PARTITIONS,
            rkyv_encode(&MergePartitionsReq {
                survivor_part_id: survivor,
                victim_part_id: victim,
                force: false,
            }),
        )
        .await
        .unwrap();
    let resp: MergePartitionsResp = rkyv_decode(&bytes).unwrap();
    CodeResp {
        code: resp.code,
        message: resp.message,
    }
}

/// Refusal: non-adjacent partitions cannot merge.
#[test]
fn merge_refuses_non_adjacent_partitions() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 81).await;
        // Two non-adjacent partitions: [a..f) and [m..z) — gap in [f..m).
        let (l1, r1, m1) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 2001, l1, r1, m1, b"a", b"f").await;
        let (l2, r2, m2) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 2002, l2, r2, m2, b"m", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(81, mgr_addr, ps_addr);
        let _ps = RpcClient::connect_as(ps_addr, autumn_rpc::version_hello::Role::Admin, None).await.unwrap();
        compio::time::sleep(Duration::from_millis(800)).await;

        let resp = merge_partitions(&mgr, 2001, 2002).await;
        assert_ne!(resp.code, CODE_OK, "non-adjacent merge must be rejected");
        assert!(
            resp.message.contains("not adjacent"),
            "error must explain non-adjacency: {}",
            resp.message
        );
    });
}

/// Refusal: self-merge is rejected immediately.
#[test]
fn merge_refuses_self_merge() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 82).await;
        let (l, r, m) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 3001, l, r, m, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(82, mgr_addr, ps_addr);
        let _ps = RpcClient::connect_as(ps_addr, autumn_rpc::version_hello::Role::Admin, None).await.unwrap();
        compio::time::sleep(Duration::from_millis(500)).await;

        let resp = merge_partitions(&mgr, 3001, 3001).await;
        assert_ne!(resp.code, CODE_OK, "self-merge must be rejected");
        assert!(
            resp.message.contains("same partition"),
            "error must identify self-merge: {}",
            resp.message
        );
    });
}

/// Merge with large values that go through ValuePointer. After merge,
/// the survivor's SSTs (including those imported from victim) must
/// still resolve their VPs against the spliced log_stream's extents.
#[test]
fn merge_preserves_value_pointer_resolution() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 90).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 4001, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(90, mgr_addr, ps_addr);
        let _ps = RpcClient::connect_as(ps_addr, autumn_rpc::version_hello::Role::Admin, None).await.unwrap();
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Write 6 large values (8 KiB each) → above the 4 KiB VP threshold,
        // so each becomes a ValuePointer in the SST. 3 keys < 'm' (left
        // half), 3 keys >= 'm' (right half).
        let big_val: Vec<u8> = (0..8 * 1024).map(|i| (i % 251) as u8).collect();
        for i in 0u8..3 {
            psr_put(&router, 4001, format!("a-{:02}", i).as_bytes(), &big_val).await;
            psr_put(&router, 4001, format!("n-{:02}", i).as_bytes(), &big_val).await;
        }
        psr_flush(&router, 4001).await;
        psr_compact(&router, 4001).await;
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Sanity: read big_val pre-split — must succeed cleanly.
        for i in 0u8..3 {
            let r = psr_get(&router, 4001, format!("a-{:02}", i).as_bytes()).await;
            assert_eq!(
                r.value,
                big_val,
                "PRE-SPLIT: big_val a-{:02} corrupted (len={}, expected {})",
                i,
                r.value.len(),
                big_val.len()
            );
        }

        // Split.
        let split_resp_bytes = router
            .client_for(4001)
            .await
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 4001, at_key: None, op_id: 0 }),
            )
            .await
            .expect("split");
        let sr: partition_rpc::SplitPartResp =
            partition_rpc::rkyv_decode(&split_resp_bytes).unwrap();
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split: {}", sr.message);

        let _ = poll_until_async(
            Duration::from_secs(10),
            Duration::from_millis(200),
            || async {
                let r = get_regions(&mgr).await;
                r.regions.len() == 2 && r.part_addrs.len() == 2
            },
        )
        .await;

        let regions = get_regions(&mgr).await;
        let mut survivor_id = 0u64;
        let mut victim_id = 0u64;
        for (pid, r) in &regions.regions {
            if let Some(rg) = &r.rg {
                if rg.start_key == b"a".to_vec() {
                    survivor_id = *pid;
                } else {
                    victim_id = *pid;
                }
            }
        }
        assert!(survivor_id != 0 && victim_id != 0);

        psr_compact(&router, survivor_id).await;
        psr_compact(&router, victim_id).await;
        compio::time::sleep(Duration::from_millis(3000)).await;

        let resp = merge_partitions(&mgr, survivor_id, victim_id).await;
        assert_eq!(resp.code, CODE_OK, "merge: {}", resp.message);

        compio::time::sleep(Duration::from_millis(2500)).await;

        // Verify: every large value resolves correctly post-merge. The
        // VPs in re-imported SSTs reference log_stream extents that were
        // spliced into survivor's log_stream by the manager merge.
        for i in 0u8..3 {
            let r = psr_get(&router, survivor_id, format!("a-{:02}", i).as_bytes()).await;
            assert_eq!(
                r.value.len(),
                big_val.len(),
                "left big-val a-{:02} len mismatch",
                i
            );
            assert_eq!(r.value, big_val, "left big-val a-{:02} content mismatch", i);
            let r = psr_get(&router, survivor_id, format!("n-{:02}", i).as_bytes()).await;
            assert_eq!(
                r.value.len(),
                big_val.len(),
                "right big-val n-{:02} len mismatch",
                i
            );
            assert_eq!(
                r.value, big_val,
                "right big-val n-{:02} content mismatch",
                i
            );
        }
    });
}

/// Merge then split-again chain: confirms cooldown and post-merge state
/// supports a follow-up split. Cooldown thresholds are 1h split / 6h
/// merge — but `last_op_at` is a hint to the policy engine, NOT a hard
/// gate on manual triggers. Manual split should always work.
#[test]
fn merge_then_split_again_round_trip() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 91).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 5001, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(91, mgr_addr, ps_addr);
        let _ps = RpcClient::connect_as(ps_addr, autumn_rpc::version_hello::Role::Admin, None).await.unwrap();
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Pre-populate.
        for i in 0u8..6 {
            psr_put(&router, 5001, format!("k-{:02}", i).as_bytes(), b"v").await;
            psr_put(&router, 5001, format!("p-{:02}", i).as_bytes(), b"v").await;
        }
        psr_flush(&router, 5001).await;
        psr_compact(&router, 5001).await;
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Split #1.
        router
            .client_for(5001)
            .await
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 5001, at_key: None, op_id: 0 }),
            )
            .await
            .expect("split #1");
        let _ = poll_until_async(
            Duration::from_secs(10),
            Duration::from_millis(200),
            || async {
                let r = get_regions(&mgr).await;
                r.regions.len() == 2 && r.part_addrs.len() == 2
            },
        )
        .await;

        let regions = get_regions(&mgr).await;
        let mut s1 = 0u64;
        let mut v1 = 0u64;
        for (pid, r) in &regions.regions {
            if let Some(rg) = &r.rg {
                if rg.start_key == b"a".to_vec() {
                    s1 = *pid;
                } else {
                    v1 = *pid;
                }
            }
        }

        psr_compact(&router, s1).await;
        psr_compact(&router, v1).await;
        compio::time::sleep(Duration::from_millis(3000)).await;

        // Merge.
        let mr = merge_partitions(&mgr, s1, v1).await;
        assert_eq!(mr.code, CODE_OK, "merge: {}", mr.message);
        compio::time::sleep(Duration::from_millis(2500)).await;
        assert_eq!(get_regions(&mgr).await.regions.len(), 1);

        // Compact survivor to clear has_overlap (post-merge SSTs span
        // both old halves; compaction unifies into the wider range).
        psr_compact(&router, s1).await;
        compio::time::sleep(Duration::from_millis(3000)).await;

        // Split #2 on the merged partition. This is the harder case
        // because survivor's seq_number must be > max of both old
        // partitions' seqs (asserted by the merge logic) AND
        // unique_user_keys must reflect the unioned table set.
        let r2 = router
            .client_for(s1)
            .await
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: s1, at_key: None, op_id: 0 }),
            )
            .await
            .expect("split #2 call");
        let sr2: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&r2).unwrap();
        assert_eq!(
            sr2.code,
            partition_rpc::CODE_OK,
            "split #2 must succeed after merge+compact: {}",
            sr2.message
        );
        compio::time::sleep(Duration::from_millis(800)).await;
        assert_eq!(get_regions(&mgr).await.regions.len(), 2);
    });
}

/// Roll a stream tail from an independent client. The caller waits for all
/// writes before each roll; this is only used to create a real multi-extent
/// source shape for recovery coverage.
async fn roll_stream_tail(mgr_addr: std::net::SocketAddr, stream_id: u64) {
    let sc = StreamClient::connect(
        &mgr_addr.to_string(),
        "auto-merge-multi-extent".to_string(),
        128 * 1024 * 1024,
        Rc::new(ConnPool::new()),
    )
    .await
    .expect("stream client");
    let before = sc
        .get_stream_info(stream_id)
        .await
        .expect("stream info before roll")
        .extent_ids;
    sc.seal_and_roll_tail(stream_id)
        .await
        .expect("roll stream tail");
    let after = sc
        .get_stream_info(stream_id)
        .await
        .expect("stream info after roll")
        .extent_ids;
    assert_eq!(after.len(), before.len() + 1);
}

/// Register an in-process AutumnManager so the test can call
/// `force_auto_merge` directly. The sources are split-born and each WAL has
/// multiple extents. This pins two invariants together:
///
/// - the mechanism helper delegates to the normal freeze/certificate handler;
/// - freeze publication canonicalizes split-inherited meta prefixes before
///   merge, allowing the first reopen to skip every certified source extent.
#[test]
fn auto_dispatch_merge_orchestrates_full_flow() {
    use autumn_manager::AutumnManager;

    let mgr_addr = pick_addr();
    // AutumnManager is Rc-based (!Send), so the manager handle must live
    // entirely on one thread. We spawn `serve` on the test's compio
    // runtime and keep a clone of the manager handle in the test's task.

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let manager = AutumnManager::new();
        // Clone for the spawned serve task (Rc-clone, shares state).
        let mgr_for_serve = manager.clone();
        compio::runtime::spawn(async move {
            let _ = mgr_for_serve.serve(mgr_addr).await;
        })
        .detach();
        compio::time::sleep(Duration::from_millis(200)).await;
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 95).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 6001, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(95, mgr_addr, ps_addr);
        // Give the PS a moment to register + open partition.
        compio::time::sleep(Duration::from_millis(2000)).await;
        let _ps = RpcClient::connect_as(ps_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        for i in 0..128usize {
            psr_put(
                &router,
                6001,
                format!("b-{i:05}").as_bytes(),
                &vec![b'l'; 1024],
            )
            .await;
            psr_put(
                &router,
                6001,
                format!("n-{i:05}").as_bytes(),
                &vec![b'r'; 1024],
            )
            .await;
        }
        psr_flush(&router, 6001).await;
        psr_compact(&router, 6001).await;
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Split into two children.
        router
            .client_for(6001)
            .await
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq {
                    part_id: 6001,
                    at_key: Some(b"m".to_vec()),
                    op_id: 0,
                }),
            )
            .await
            .expect("split");
        let _ = poll_until_async(
            Duration::from_secs(10),
            Duration::from_millis(200),
            || async {
                let r = get_regions(&mgr).await;
                r.regions.len() == 2 && r.part_addrs.len() == 2
            },
        )
        .await;

        let regions = get_regions(&mgr).await;
        let mut s = 0u64;
        let mut v = 0u64;
        let mut s_log = 0u64;
        let mut v_log = 0u64;
        for (pid, r) in &regions.regions {
            if let Some(rg) = &r.rg {
                if rg.start_key == b"a".to_vec() {
                    s = *pid;
                    s_log = r.log_stream;
                } else {
                    v = *pid;
                    v_log = r.log_stream;
                }
            }
        }
        assert!(s != 0 && v != 0 && s_log != 0 && v_log != 0);

        // Create two complete post-split WAL extents per child. Each put is
        // acknowledged before the external roll and no maintenance/GC is
        // dispatched during these loops.
        for round in 0..2usize {
            for i in 0..128usize {
                psr_put(
                    &router,
                    s,
                    format!("c-{round}-{i:05}").as_bytes(),
                    &vec![b's'; 1024],
                )
                .await;
                psr_put(
                    &router,
                    v,
                    format!("p-{round}-{i:05}").as_bytes(),
                    &vec![b'v'; 1024],
                )
                .await;
            }
            roll_stream_tail(mgr_addr, s_log).await;
            roll_stream_tail(mgr_addr, v_log).await;
        }

        // Split children carry overlapping CoW tables and merge refuses that
        // shape. The required major compactions also exercise the realistic
        // split-born lifecycle before freeze canonicalizes each final source
        // checkpoint and attaches its exact WAL extent list.
        psr_compact(&router, s).await;
        psr_compact(&router, v).await;
        compio::time::sleep(Duration::from_millis(3000)).await;

        // ── KEY DIFFERENCE: drive the merge via force_auto_merge ──
        // This must enter the same freeze/certificate path as the public RPC.
        let before_replay = replay_read_bytes(s);
        manager
            .force_auto_merge(s, v)
            .await
            .expect("force_auto_merge must succeed");

        assert!(
            poll_until_async(Duration::from_secs(20), Duration::from_millis(200), || async {
                match router.try_client_for(s).await {
                    Ok(c) => ps_get(&c, s, b"p-1-00000").await.code == partition_rpc::CODE_OK,
                    Err(_) => false,
                }
            })
            .await,
            "merged split-born survivor never reopened"
        );
        assert_eq!(
            get_regions(&mgr).await.regions.len(),
            1,
            "merge must complete"
        );
        let replayed = replay_read_bytes(s) - before_replay;
        assert!(
            replayed < 64 * 1024,
            "split-born auto-merge replayed {replayed} checkpoint-covered WAL bytes"
        );
    });
}

/// auto-split smoke: invoke `force_auto_split` and verify the
/// manager dispatches MSG_SPLIT_PART to the owning PS via conn_pool.
#[test]
fn auto_dispatch_split_dispatches_msg_split_part() {
    use autumn_manager::AutumnManager;

    let mgr_addr = pick_addr();

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let manager = AutumnManager::new();
        let mgr_for_serve = manager.clone();
        compio::runtime::spawn(async move {
            let _ = mgr_for_serve.serve(mgr_addr).await;
        })
        .detach();
        compio::time::sleep(Duration::from_millis(200)).await;

        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 96).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 7001, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(96, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(2000)).await;
        let _ps = RpcClient::connect_as(ps_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("connect ps");
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Populate enough keys for unique_user_keys to find a clean mid.
        for i in 0u8..10 {
            psr_put(&router, 7001, format!("key-{:02}", i).as_bytes(), b"v").await;
        }
        psr_flush(&router, 7001).await;
        psr_compact(&router, 7001).await;
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Auto-dispatch the split via force_auto_split. Manager looks up
        // owning PS from regions/part_addrs, sends MSG_SPLIT_PART via
        // its conn_pool, returns Ok on PS handler success.
        manager
            .force_auto_split(7001)
            .await
            .expect("force_auto_split must succeed");

        // Wait for region propagation.
        let _ = poll_until_async(
            Duration::from_secs(10),
            Duration::from_millis(200),
            || async { get_regions(&mgr).await.regions.len() == 2 },
        )
        .await;
        let regions = get_regions(&mgr).await;
        assert_eq!(
            regions.regions.len(),
            2,
            "auto-split must produce 2 regions"
        );
    });
}

/// stress: split → put → merge → put → split with batches of writes
/// interleaved between topology ops. Verifies all written keys remain
/// readable across the full lifecycle.
///
/// (Concurrent-writer version was attempted but compio's single-threaded
/// runtime + per-call PsRouter reconnects produced unbounded CPU usage
/// without yielding the test thread back to the foreground topology
/// driver. Sequential interleaved writes still cover the meaningful
/// invariants: split/merge metadata mutations, log_stream extent
/// splice, region_sync reload, multi-step seq_number monotonicity.)
#[test]
fn split_merge_split_with_interleaved_writes() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 99).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 12001, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(99, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(2000)).await;
        let _ps = RpcClient::connect_as(ps_addr, autumn_rpc::version_hello::Role::Admin, None).await.unwrap();
        let router = PsRouter::new(mgr_addr, ps_addr);

        // ── Phase 0: pre-seed ─────────────────────────────────────────
        let mut all_keys: Vec<Vec<u8>> = Vec::new();
        for i in 0u8..20 {
            let key = format!("k{:03}", i).into_bytes();
            psr_put(&router, 12001, &key, b"v").await;
            all_keys.push(key);
        }
        psr_flush(&router, 12001).await;
        psr_compact(&router, 12001).await;
        compio::time::sleep(Duration::from_millis(2500)).await;

        // ── Phase 1: SPLIT #1 ─────────────────────────────────────────
        let r = router
            .client_for(12001)
            .await
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 12001, at_key: None, op_id: 0 }),
            )
            .await
            .expect("split #1");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&r).unwrap();
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split #1: {}", sr.message);

        let _ = poll_until_async(
            Duration::from_secs(10),
            Duration::from_millis(200),
            || async {
                let r = get_regions(&mgr).await;
                r.regions.len() == 2 && r.part_addrs.len() == 2
            },
        )
        .await;
        let regions = get_regions(&mgr).await;
        let mut s1 = 0u64;
        let mut v1 = 0u64;
        for (pid, r) in &regions.regions {
            if let Some(rg) = &r.rg {
                if rg.start_key == b"a".to_vec() {
                    s1 = *pid;
                } else {
                    v1 = *pid;
                }
            }
        }

        // ── Phase 2: writes against 2-partition topology ──────────────
        for i in 0u8..10 {
            let lkey = format!("a-mid-{:02}", i).into_bytes();
            let rkey = format!("n-mid-{:02}", i).into_bytes();
            psr_put(&router, s1, &lkey, b"v").await;
            psr_put(&router, v1, &rkey, b"v").await;
            all_keys.push(lkey);
            all_keys.push(rkey);
        }
        psr_flush(&router, s1).await;
        psr_flush(&router, v1).await;
        psr_compact(&router, s1).await;
        psr_compact(&router, v1).await;
        compio::time::sleep(Duration::from_millis(3000)).await;

        // ── Phase 3: MERGE ────────────────────────────────────────────
        let resp = merge_partitions(&mgr, s1, v1).await;
        assert_eq!(resp.code, CODE_OK, "merge: {}", resp.message);
        compio::time::sleep(Duration::from_millis(3000)).await;
        assert_eq!(get_regions(&mgr).await.regions.len(), 1, "after merge");

        // ── Phase 4: writes against merged topology ───────────────────
        for i in 0u8..10 {
            let key = format!("post-merge-{:02}", i).into_bytes();
            psr_put(&router, s1, &key, b"v").await;
            all_keys.push(key);
        }
        psr_flush(&router, s1).await;
        psr_compact(&router, s1).await;
        compio::time::sleep(Duration::from_millis(3000)).await;

        // ── Phase 5: SPLIT #2 on merged partition ─────────────────────
        let r = router
            .client_for(s1)
            .await
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: s1, at_key: None, op_id: 0 }),
            )
            .await
            .expect("split #2");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&r).unwrap();
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split #2: {}", sr.message);

        let _ = poll_until_async(
            Duration::from_secs(10),
            Duration::from_millis(200),
            || async { get_regions(&mgr).await.regions.len() == 2 },
        )
        .await;
        compio::time::sleep(Duration::from_millis(800)).await;

        // ── Phase 6: writes against post-split-#2 topology ────────────
        for i in 0u8..6 {
            let key = format!("post-split2-{:02}", i).into_bytes();
            // Resolve to current part_id.
            let mgr_c = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.unwrap();
            let pid = resolve_part_id_for_key(&mgr_c, &key)
                .await
                .expect("post-split2 key must route to a partition");
            psr_put(&router, pid, &key, b"v").await;
            all_keys.push(key);
        }

        // ── Verify ALL keys readable from current topology ────────────
        let mgr_v = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.unwrap();
        let mut missing: Vec<Vec<u8>> = Vec::new();
        for key in &all_keys {
            let pid = match resolve_part_id_for_key(&mgr_v, key).await {
                Some(p) => p,
                None => {
                    missing.push(key.clone());
                    continue;
                }
            };
            let r = psr_get(&router, pid, key).await;
            let want = if key.starts_with(b"k") {
                b"v".to_vec()
            } else {
                b"v".to_vec()
            };
            if r.code != partition_rpc::CODE_OK || r.value != want {
                missing.push(key.clone());
            }
        }
        assert!(
            missing.is_empty(),
            "{} keys missing/wrong after full lifecycle: {:?}",
            missing.len(),
            missing
                .iter()
                .take(5)
                .map(|k| String::from_utf8_lossy(k).to_string())
                .collect::<Vec<_>>()
        );
    });
}

/// A merge must refuse a CoW child that still carries the parent's tables.
///
/// After a split each child's tables still hold keys OUTSIDE its range, and
/// only its sibling knows how stale they are. The merged partition unions both
/// sources' tables and a point read takes the first hit newest-table-first, so
/// an un-separated child re-exposes pre-split values over the sibling's history —
/// and a key the sibling deleted after the split, whose tombstone the sibling's
/// major compaction already dropped, comes back for good. Whichever side is left
/// un-compacted, the merge must be refused until it is, and then read correctly.
fn merge_refuses_unseparated_cow_child(compact_survivor_first: bool) {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 83).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        const PART: u64 = 1201;
        upsert_partition(&mgr, PART, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(83, mgr_addr, ps_addr);
        let ps = RpcClient::connect_as(ps_addr, autumn_rpc::version_hello::Role::Admin, None).await.unwrap();
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Survivor range [a, m): D is rewritten, J deleted after the split.
        // Victim range [m, z): K is rewritten, R deleted after the split.
        const D: &[u8] = b"d-rewritten";
        const J: &[u8] = b"j-deleted";
        const K: &[u8] = b"q-rewritten";
        const R: &[u8] = b"r-deleted";
        for k in [D, J, K, R] {
            ps_put(&ps, PART, k, b"OLD").await;
        }
        ps_flush(&ps, PART).await;

        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq {
                    part_id: PART,
                    at_key: Some(b"m".to_vec()),
                    op_id: 0,
                }),
            )
            .await
            .expect("split call");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&resp).unwrap();
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split: {}", sr.message);
        assert!(
            poll_until_async(Duration::from_secs(10), Duration::from_millis(200), || async {
                let r = get_regions(&mgr).await;
                r.regions.len() == 2 && r.part_addrs.len() == 2
            })
            .await,
            "right child never registered"
        );
        let survivor = PART;
        let victim = get_regions(&mgr)
            .await
            .regions
            .iter()
            .map(|(pid, _)| *pid)
            .find(|pid| *pid != survivor)
            .unwrap();

        let delete = |part: u64, key: &'static [u8]| {
            let router = &router;
            async move {
                let c = router.client_for(part).await;
                let r = ps_delete(&c, part, key).await;
                assert_eq!(r.code, partition_rpc::CODE_OK, "delete: {}", r.message);
            }
        };
        // Each delete precedes a put. With the delete as a side's LAST record,
        // the deleted key read NOT_FOUND after an ungated merge — the reopen
        // replays that tombstone — which hides the resurrection this checks for.
        delete(survivor, J).await;
        psr_put(&router, survivor, D, b"NEW").await;
        delete(victim, R).await;
        psr_put(&router, victim, K, b"NEW").await;
        psr_flush(&router, survivor).await;
        psr_flush(&router, victim).await;

        let pool = std::rc::Rc::new(autumn_stream::ConnPool::new());
        let sc = autumn_stream::StreamClient::connect(
            &mgr_addr.to_string(),
            "test-merge-separation".to_string(),
            1 << 20,
            pool,
        )
        .await
        .expect("connect sc");
        // A major compaction of these few keys leaves exactly one table, and it
        // drops the out-of-range keys along with the child's own tombstone.
        let compact_and_wait = |part: u64| {
            let (mgr, router, sc) = (&mgr, &router, &sc);
            async move {
                psr_compact(router, part).await;
                let meta_stream = get_regions(mgr)
                    .await
                    .regions
                    .iter()
                    .find(|(pid, _)| *pid == part)
                    .map(|(_, r)| r.meta_stream)
                    .unwrap();
                assert!(
                    poll_until_async(Duration::from_secs(15), Duration::from_millis(250), || async {
                        matches!(
                            sc.read_last_extent_data(meta_stream).await,
                            Ok(Some(raw)) if decode_last_table_locations(raw.as_slice()).locs.len() == 1
                        )
                    })
                    .await,
                    "major compaction of partition {part} never settled to one table"
                );
            }
        };
        let merge = || async {
            let bytes = mgr
                .call(
                    MSG_MERGE_PARTITIONS,
                    rkyv_encode(&MergePartitionsReq {
                        survivor_part_id: survivor,
                        victim_part_id: victim,
                        force: false,
                    }),
                )
                .await
                .unwrap();
            rkyv_decode::<MergePartitionsResp>(&bytes).unwrap()
        };

        let (first, second) = if compact_survivor_first {
            (survivor, victim)
        } else {
            (victim, survivor)
        };
        compact_and_wait(first).await;
        let refused = merge().await;
        assert!(
            refused.code != CODE_OK && refused.message.contains("overlapping keys"),
            "merge with partition {second} still un-separated must be refused on has_overlap, \
             got code {} {:?}",
            refused.code,
            refused.message
        );
        assert_eq!(get_regions(&mgr).await.regions.len(), 2, "a refused merge changed the topology");
        // The side that was frozen before the other refused must be thawed:
        // a partition left frozen would only show on its next write.
        for (part, key) in [(survivor, &b"b-after-refusal"[..]), (victim, &b"s-after-refusal"[..])] {
            assert!(
                try_psr_put(&router, part, key, b"x").await.is_ok(),
                "partition {part} still refuses writes after the refused merge"
            );
        }

        compact_and_wait(second).await;
        let merged = merge().await;
        assert_eq!(merged.code, CODE_OK, "merge of separated children: {}", merged.message);
        assert!(
            poll_until_async(Duration::from_secs(10), Duration::from_millis(200), || async {
                get_regions(&mgr).await.regions.len() == 1
            })
            .await,
            "merge never settled to one partition"
        );

        // Until the survivor reopens over the widened range, its frozen
        // pre-merge instance still answers reads for [a, m) — from its own
        // tables, which would pass this test for the wrong reason. A victim-range
        // key reading OK is what proves the reopen.
        assert!(
            poll_until_async(Duration::from_secs(15), Duration::from_millis(250), || async {
                psr_get(&router, survivor, K).await.code == partition_rpc::CODE_OK
            })
            .await,
            "the survivor never reopened over the merged range"
        );
        let check = |when: &'static str| {
            let router = &router;
            async move {
            for (key, want) in [(D, Some(&b"NEW"[..])), (K, Some(&b"NEW"[..])), (J, None), (R, None)] {
                let got = psr_get(router, survivor, key).await;
                match want {
                    Some(v) => assert!(
                        got.code == partition_rpc::CODE_OK && got.value == v,
                        "{when}: {} reads code {} {:?}, want NEW",
                        String::from_utf8_lossy(key),
                        got.code,
                        String::from_utf8_lossy(&got.value)
                    ),
                    None => assert_eq!(
                        got.code,
                        partition_rpc::CODE_NOT_FOUND,
                        "{when}: deleted {} came back as {:?}",
                        String::from_utf8_lossy(key),
                        String::from_utf8_lossy(&got.value)
                    ),
                }
            }
            }
        };
        check("after merge").await;
        // A seq-ordered merge of every table: what a point read cannot show.
        compact_and_wait(survivor).await;
        check("after merge + major compaction").await;
    });
}

#[test]
fn merge_refuses_victim_still_carrying_parent_tables() {
    merge_refuses_unseparated_cow_child(true);
}

#[test]
fn merge_refuses_survivor_still_carrying_parent_tables() {
    merge_refuses_unseparated_cow_child(false);
}

/// Routes a key to its current partition via GetRegions. Returns None
/// if no partition's range covers the key (transient during merge).
async fn resolve_part_id_for_key(mgr: &RpcClient, key: &[u8]) -> Option<u64> {
    let regions = get_regions(mgr).await;
    for (pid, r) in regions.regions {
        if let Some(rg) = r.rg {
            let in_range = key >= rg.start_key.as_slice()
                && (rg.end_key.is_empty() || key < rg.end_key.as_slice());
            if in_range {
                return Some(pid);
            }
        }
    }
    None
}

/// Try a Put; map all error variants to Err(()) so the writer can
/// distinguish "no progress, retry" from a permanent-fault assertion.
async fn try_psr_put(router: &PsRouter, part_id: u64, key: &[u8], value: &[u8]) -> Result<(), ()> {
    let c = router.client_for(part_id).await;
    let resp = c
        .call(
            partition_rpc::MSG_PUT,
            partition_rpc::rkyv_encode(&partition_rpc::PutReq {
                part_id,
                key: key.to_vec(),
                value: value.to_vec(),
                expires_at: 0,
                region_epoch: 0,
            inode_hint: 0,
            lease_epoch: 0,
            }),
        )
        .await;
    let resp_bytes = match resp {
        Ok(b) => b,
        Err(_) => return Err(()),
    };
    let r: partition_rpc::PutResp = match partition_rpc::rkyv_decode(&resp_bytes) {
        Ok(r) => r,
        Err(_) => return Err(()),
    };
    if r.code != partition_rpc::CODE_OK {
        Err(())
    } else {
        Ok(())
    }
}

/// Drive a SPLIT through transient retries. The CLI's `split` is a
/// single RPC; if the partition is mid-flush or compacting, the
/// PartitionData mutex will serialise. Wrap with a short retry loop.
async fn poll_split_succeeds(router: &std::rc::Rc<PsRouter>, part_id: u64) -> bool {
    for _ in 0..15 {
        let c = router.client_for(part_id).await;
        let resp = c
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id, at_key: None, op_id: 0 }),
            )
            .await;
        if let Ok(bytes) = resp {
            if let Ok(r) = partition_rpc::rkyv_decode::<partition_rpc::SplitPartResp>(&bytes) {
                if r.code == partition_rpc::CODE_OK {
                    return true;
                }
            }
        }
        compio::time::sleep(Duration::from_millis(500)).await;
    }
    false
}
