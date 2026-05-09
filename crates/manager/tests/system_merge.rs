//! F183 / F184 — System tests for partition merge primitive.
//!
//! Cluster topology: manager + 2 extent-nodes + 1 PS. Tests exercise
//! the manager's `MSG_MULTI_MODIFY_MERGE` handler end-to-end via the
//! Stage 1 CLI orchestration flow (FLUSH both → admin owner-lock →
//! commit_length → merge).
//!
//! Coverage:
//!  - happy path: split → merge round-trip; all keys readable from survivor
//!  - refusal: non-adjacent partitions → Precondition
//!  - refusal: self-merge → Precondition

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;
use bytes::Bytes;

use support::*;

/// Helper: drive the Stage 1 merge orchestration directly against the
/// manager (bypasses ClusterClient because the test scaffolding wires
/// per-partition addresses manually).
async fn merge_partitions(
    mgr: &RpcClient,
    router: &PsRouter,
    survivor: u64,
    victim: u64,
) -> CodeResp {
    // FLUSH both partitions via the per-partition router (F099-K).
    psr_flush(router, survivor).await;
    psr_flush(router, victim).await;

    // Acquire admin owner-lock.
    let owner_key = format!("test-merge:{survivor}:{victim}");
    let lock_payload = rkyv_encode(&AcquireOwnerLockReq {
        owner_key: owner_key.clone(),
    });
    let lock_bytes = mgr.call(MSG_ACQUIRE_OWNER_LOCK, lock_payload).await.unwrap();
    let lock_resp: AcquireOwnerLockResp = rkyv_decode(&lock_bytes).unwrap();
    assert_eq!(lock_resp.code, CODE_OK, "acquire_owner_lock: {}", lock_resp.message);
    let revision = lock_resp.revision;

    // Resolve stream IDs via GetRegions.
    let regions = get_regions(mgr).await;
    let mut s_log = 0;
    let mut s_row = 0;
    let mut s_meta = 0;
    let mut v_log = 0;
    let mut v_row = 0;
    let mut v_meta = 0;
    for (pid, r) in &regions.regions {
        if *pid == survivor {
            s_log = r.log_stream;
            s_row = r.row_stream;
            s_meta = r.meta_stream;
        }
        if *pid == victim {
            v_log = r.log_stream;
            v_row = r.row_stream;
            v_meta = r.meta_stream;
        }
    }

    // commit_length per stream.
    let cl = |sid: u64| {
        let owner = owner_key.clone();
        async move {
            let req = rkyv_encode(&CheckCommitLengthReq {
                stream_id: sid,
                owner_key: owner,
                revision,
            });
            let bytes = mgr.call(MSG_CHECK_COMMIT_LENGTH, req).await.unwrap();
            let resp: CheckCommitLengthResp = rkyv_decode(&bytes).unwrap();
            assert_eq!(resp.code, CODE_OK, "commit_length stream={sid}: {}", resp.message);
            resp.end as u64
        }
    };
    let log_lens = [cl(s_log).await.max(1), cl(v_log).await.max(1)];
    let row_lens = [cl(s_row).await.max(1), cl(v_row).await.max(1)];
    let meta_lens = [cl(s_meta).await.max(1), cl(v_meta).await.max(1)];

    // Call merge.
    let req = rkyv_encode(&MultiModifyMergeReq {
        survivor_part_id: survivor,
        victim_part_id: victim,
        owner_key,
        revision,
        log_sealed_lengths: log_lens,
        row_sealed_lengths: row_lens,
        meta_sealed_lengths: meta_lens,
    });
    let resp_bytes = mgr.call(MSG_MULTI_MODIFY_MERGE, req).await.unwrap();
    let resp: MultiModifyMergeResp = rkyv_decode(&resp_bytes).unwrap();
    CodeResp { code: resp.code, message: resp.message }
}

/// Happy path: split a partition, then merge children back. All keys
/// must remain readable from the survivor; victim region is gone.
#[test]
#[ignore] // long-running: cluster startup + flush + split + merge
fn merge_split_round_trip_keys_intact() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 80).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 1001, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(80, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.unwrap();
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Write 10 keys spread across [a..z): "key-00".."key-09" + "merge-00".."merge-09"
        for i in 0u8..10 {
            ps_put(&ps, 1001, format!("key-{:02}", i).as_bytes(), format!("LV{i}").as_bytes()).await;
            ps_put(&ps, 1001, format!("merge-{:02}", i).as_bytes(), format!("RV{i}").as_bytes()).await;
        }
        ps_flush(&ps, 1001).await;

        // Run major compaction so split picks a clean mid_key (avoid has_overlap).
        ps_compact(&ps, 1001).await;
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Split.
        let split_resp_bytes = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq { part_id: 1001 }),
            )
            .await
            .expect("split call");
        let sr: partition_rpc::SplitPartResp =
            partition_rpc::rkyv_decode(&split_resp_bytes).unwrap();
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split: {}", sr.message);

        // Wait for region propagation; the manager allocates a new part_id
        // (next id from alloc_ids) for the right child. Poll until BOTH
        // partition addresses are registered (F099-K per-partition listener).
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
        assert_eq!(regions.regions.len(), 2, "expected 2 partitions after split");
        assert_eq!(regions.part_addrs.len(), 2, "expected 2 part_addrs after split");
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
        assert!(survivor_id != 0 && victim_id != 0 && survivor_id != victim_id);

        // Major compact survivor's left child to clear its has_overlap (it
        // inherited the wider range's SSTs via CoW). Same for the right
        // child. Without this, merge would refuse with the has_overlap gate.
        // Use the per-partition router because the right child has its own
        // listener port (F099-K).
        psr_compact(&router, survivor_id).await;
        psr_compact(&router, victim_id).await;
        compio::time::sleep(Duration::from_millis(3000)).await;

        // Merge.
        let resp = merge_partitions(&mgr, &router, survivor_id, victim_id).await;
        assert_eq!(resp.code, CODE_OK, "merge: {}", resp.message);

        // Wait for region_sync to pick up the merged state.
        compio::time::sleep(Duration::from_millis(2500)).await;

        let regions = get_regions(&mgr).await;
        assert_eq!(regions.regions.len(), 1, "expected 1 partition after merge");
        assert_eq!(regions.regions[0].0, survivor_id, "survivor must keep its part_id");

        // All 20 keys must still be readable from the survivor partition.
        for i in 0u8..10 {
            let r = psr_get(&router, survivor_id, format!("key-{:02}", i).as_bytes()).await;
            assert_eq!(
                r.value,
                format!("LV{i}").as_bytes().to_vec(),
                "left-side key key-{:02} lost after merge",
                i
            );
            let r = psr_get(&router, survivor_id, format!("merge-{:02}", i).as_bytes()).await;
            assert_eq!(
                r.value,
                format!("RV{i}").as_bytes().to_vec(),
                "right-side key merge-{:02} lost after merge",
                i
            );
        }
    });
}

/// Refusal: non-adjacent partitions cannot merge.
#[test]
#[ignore] // requires cluster startup
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
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 81).await;
        // Two non-adjacent partitions: [a..f) and [m..z) — gap in [f..m).
        let (l1, r1, m1) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 2001, l1, r1, m1, b"a", b"f").await;
        let (l2, r2, m2) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 2002, l2, r2, m2, b"m", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(81, mgr_addr, ps_addr);
        let _ps = RpcClient::connect(ps_addr).await.unwrap();
        let router = PsRouter::new(mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(800)).await;

        let resp = merge_partitions(&mgr, &router, 2001, 2002).await;
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
#[ignore] // requires cluster startup
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
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 82).await;
        let (l, r, m) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 3001, l, r, m, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(82, mgr_addr, ps_addr);
        let _ps = RpcClient::connect(ps_addr).await.unwrap();
        let router = PsRouter::new(mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(500)).await;

        let resp = merge_partitions(&mgr, &router, 3001, 3001).await;
        assert_ne!(resp.code, CODE_OK, "self-merge must be rejected");
        assert!(
            resp.message.contains("same partition"),
            "error must identify self-merge: {}",
            resp.message
        );
    });
}

#[allow(dead_code)]
fn _suppress_unused() {
    let _ = Bytes::new();
}
