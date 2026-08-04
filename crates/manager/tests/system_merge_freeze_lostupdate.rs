#![allow(
    dead_code,
    unused_must_use,
    clippy::redundant_pattern_matching,
    clippy::if_same_then_else
)] // integration-test file
//! G5 reproduction: "merge coordinator pause across FREEZE_TTL → lost update".
//!
//! Hypothesis (verified against the code — see the module notes below):
//! `handle_merge_partitions` (manager) freezes the survivor+victim PSes,
//! captures `commit_length` on the 6 streams, then commits the Phase-2 merge
//! txn that seals the victim's tail at the CAPTURED lengths. The PS-side freeze
//! has a wall-clock backstop `FREEZE_TTL` (30 s) that auto-unfreezes and RESUMES
//! writes if the manager is slow (`check_freeze_ttls`). The manager-side merge
//! commit has NO equivalent guard — no elapsed-time / freeze-deadline / owner-
//! epoch / commit-length re-validation between the capture and the seal (contrast
//! the SPLIT path, which is gated by `split_freeze_deadline` on the PS BECAUSE the
//! split handler runs on the same node that owns the freeze state). So if the
//! coordinator is delayed > FREEZE_TTL between the capture and the txn:
//!   1. the victim PS auto-unfreezes,
//!   2. accepts NEW appends to the SAME open log tail (appends don't touch etcd,
//!      and the EN `commit_length` probe is deliberately check-only — it does NOT
//!      bump the EN write-fence — so the merge's high admin-lock epoch never
//!      fences out the live PS), they get ACKed above the captured length,
//!   3. the coordinator wakes and seals the victim's tail at the STALE captured
//!      length → the spliced survivor log excludes the post-unfreeze acked writes
//!      → torn merge / lost update.
//!
//! ── Injection method ─────────────────────────────────────────────────────────
//! This harness does NOT SIGSTOP a subprocess (the in-process test cluster runs
//! the manager/EN/PS as threads, each on its own compio runtime — a thread can't
//! be selectively SIGSTOP'd) and does NOT add a test-only pause hook to the
//! manager source. Instead it REPLAYS the exact orchestration steps that
//! `handle_merge_partitions` performs internally — `MSG_ACQUIRE_OWNER_LOCK` →
//! `MSG_MERGE_FREEZE{true}` (victim, then survivor) → `MSG_CHECK_COMMIT_LENGTH`
//! ×6 → `MSG_MULTI_MODIFY_MERGE` — with the coordinator PAUSE injected in the test
//! BETWEEN the capture and the txn. Because the real coordinator issues those same
//! RPCs, in that same order, with zero elapsed-time guard between the last capture
//! and `handle_multi_modify_merge`, this replay is behaviourally identical to a
//! manager paused/SIGSTOP'd at that site for > FREEZE_TTL. It is deterministic (we
//! control the exact timeline) and touches no shared harness file.
//!
//! Topology: manager + 2 ENs + 1 PS; two ADJACENT partitions created directly via
//! `upsert_partition` (no CoW split → no `has_overlap`): survivor [a,m), victim
//! [m,z). A writer hammers victim-range keys through the FREEZE_TTL window.
//!
//! ACKed-vs-uncertain rule: only writes that returned a clean `CODE_OK` from the
//! PS (after unfreeze) are recorded as `acked`; those MUST be readable on the
//! survivor after the merge. A missing ACKed key = the reproduced lost update.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;

use support::*;

const SURVIVOR: u64 = 21001; // [a, m)
const VICTIM: u64 = 21002; // [m, z)
const PS_ID: u64 = 110;

/// Raw PUT that returns the PS's `PutResp.code` (so the caller can distinguish a
/// clean ACK (`CODE_OK`) from the frozen rejection (`CODE_UNAVAILABLE`) or any
/// transient error). Unlike the shared `ps_put` helper, this NEVER treats a
/// non-OK code as success.
async fn put_code(ps: &RpcClient, part_id: u64, key: &[u8], value: &[u8]) -> Result<u8, String> {
    let payload = partition_rpc::rkyv_encode(&partition_rpc::PutReq {
        part_id,
        key: key.to_vec(),
        value: value.to_vec(),
        expires_at: 0,
        region_epoch: 0, // test: skip epoch check
        inode_hint: 0,
        lease_epoch: 0,
    });
    match ps.call(partition_rpc::MSG_PUT, payload).await {
        Ok(resp) => {
            let r: partition_rpc::PutResp =
                partition_rpc::rkyv_decode(&resp).map_err(|e| format!("decode PutResp: {e}"))?;
            Ok(r.code)
        }
        Err(e) => Err(format!("{e:?}")),
    }
}

/// Send `MSG_MERGE_FREEZE` to a partition's own PS listener. Returns
/// `(code, message)`.
async fn merge_freeze(ps: &RpcClient, part_id: u64, freeze: bool) -> (u8, String) {
    let payload = partition_rpc::rkyv_encode(&partition_rpc::MergeFreezeReq { part_id, freeze });
    let resp = ps
        .call(partition_rpc::MSG_MERGE_FREEZE, payload)
        .await
        .expect("merge_freeze rpc");
    let r: partition_rpc::MergeFreezeResp =
        partition_rpc::rkyv_decode(&resp).expect("decode MergeFreezeResp");
    (r.code, r.message)
}

/// Capture `commit_length` on one stream via the manager, using the merge
/// owner-lock — exactly what `handle_merge_partitions::read_commit_len` does
/// (note: NO `.max(1)`; pass the real length through).
async fn commit_length(mgr: &RpcClient, stream_id: u64, owner_key: &str, owner_epoch: i64) -> u64 {
    let req = rkyv_encode(&CheckCommitLengthReq {
        stream_id,
        owner_key: owner_key.to_string(),
        owner_epoch,
    });
    let bytes = mgr.call(MSG_CHECK_COMMIT_LENGTH, req).await.unwrap();
    let resp: CheckCommitLengthResp = rkyv_decode(&bytes).unwrap();
    assert_eq!(
        resp.code, CODE_OK,
        "commit_length stream={stream_id}: {}",
        resp.message
    );
    resp.end
}

/// Reproduce the merge-freeze lost-update. This test asserts the CORRECTNESS
/// property (every post-unfreeze ACKed write survives the merge). If the anomaly
/// reproduces, the final assertion FAILS with a concrete history excerpt.
#[test]
#[ignore] // long-running: cluster startup + 30 s FREEZE_TTL window
fn merge_freeze_ttl_lost_update() {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 110).await;

        // Two ADJACENT partitions created directly (no CoW split → no has_overlap):
        //   survivor [a, m)   victim [m, z)
        let (s_log, s_row, s_meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, SURVIVOR, s_log, s_row, s_meta, b"a", b"m").await;
        let (v_log, v_row, v_meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, VICTIM, v_log, v_row, v_meta, b"m", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(PS_ID, mgr_addr, ps_addr);
        let router = PsRouter::new(mgr_addr, ps_addr);

        // Wait until BOTH partitions have their own listener registered.
        let opened = poll_until_async(
            Duration::from_secs(15),
            Duration::from_millis(200),
            || async {
                let r = get_regions(&mgr).await;
                r.regions.len() == 2 && r.part_addrs.len() == 2
            },
        )
        .await;
        assert!(opened, "both partitions must open before the merge");

        // ── pre-freeze baseline on the victim ────────────────────────────────
        // These are flushed → they live in victim's row_stream SST + are covered
        // by the captured log length, so they MUST survive the merge. They are
        // the control that proves the merge itself works and the loss is specific
        // to the post-unfreeze window.
        for i in 0u8..5 {
            psr_put(
                &router,
                VICTIM,
                format!("m-pre-{:02}", i).as_bytes(),
                b"preval",
            )
            .await;
        }
        psr_flush(&router, VICTIM).await;

        // ══ The FIX under test: freeze-budget guard in handle_merge_partitions ══
        // Arm the failpoint so the coordinator STALLS past the 20 s merge freeze
        // deadline between the commit_length capture and the txn — exactly what a
        // SIGSTOP'd / etcd-stalled manager looks like. Before the fix the merge
        // committed anyway and sealed the victim tail at a STALE captured length,
        // dropping the writes that resumed after the PS auto-unfroze (FREEZE_TTL).
        // With the guard it MUST abort instead.
        const DEADLINE_S: u64 = 15; // MERGE_FREEZE_COMMIT_DEADLINE
        autumn_manager::MERGE_TEST_PAUSE_MS
            .store((DEADLINE_S + 3) * 1000, std::sync::atomic::Ordering::Relaxed);

        let merge_bytes = mgr
            .call(
                MSG_MERGE_PARTITIONS,
                rkyv_encode(&MergePartitionsReq {
                    survivor_part_id: SURVIVOR,
                    victim_part_id: VICTIM,
                    force: false,
                }),
            )
            .await
            .unwrap();
        autumn_manager::MERGE_TEST_PAUSE_MS.store(0, std::sync::atomic::Ordering::Relaxed);
        let merge_resp: MergePartitionsResp = rkyv_decode(&merge_bytes).unwrap();
        eprintln!("[guarded merge] code={} msg={}", merge_resp.code, merge_resp.message);

        // The guard must ABORT (non-OK) with the freeze-budget reason. A commit
        // here = regression: the stale-length-seal lost-update window reopened.
        assert_ne!(
            merge_resp.code, CODE_OK,
            "freeze-budget guard MUST abort a coordinator that stalled past the \
             deadline; it committed instead (lost-update window reopened): {}",
            merge_resp.message
        );
        assert!(
            merge_resp.message.contains("freeze budget exceeded"),
            "expected a freeze-budget abort, got code={} msg={}",
            merge_resp.code, merge_resp.message
        );

        // Aborted merge → both partitions remain, victim data intact (the guard
        // prevented the torn merge outright; rollback unfroze both PSes).
        let still_two = poll_until_async(
            Duration::from_secs(10),
            Duration::from_millis(250),
            || async { get_regions(&mgr).await.regions.len() == 2 },
        )
        .await;
        assert!(still_two, "aborted merge must leave both partitions in place");
        compio::time::sleep(Duration::from_millis(1500)).await;
        for i in 0u8..5 {
            let k = format!("m-pre-{:02}", i);
            let r = psr_get(&router, VICTIM, k.as_bytes()).await;
            assert!(
                r.code == partition_rpc::CODE_OK && r.value == b"preval".to_vec(),
                "victim baseline {k} lost after the aborted merge (code={})",
                r.code
            );
        }

        // ══ Control: a NORMAL (un-stalled) merge still succeeds and carries the
        // victim's data into the survivor — the guard doesn't break healthy merges.
        psr_put(&router, VICTIM, b"m-live-1", b"liveval").await;
        psr_flush(&router, VICTIM).await;
        let merge2 = mgr
            .call(
                MSG_MERGE_PARTITIONS,
                rkyv_encode(&MergePartitionsReq {
                    survivor_part_id: SURVIVOR,
                    victim_part_id: VICTIM,
                    force: false,
                }),
            )
            .await
            .unwrap();
        let m2: MergePartitionsResp = rkyv_decode(&merge2).unwrap();
        assert_eq!(m2.code, CODE_OK, "normal merge must succeed: {}", m2.message);
        let merged = poll_until_async(
            Duration::from_secs(15),
            Duration::from_millis(250),
            || async { get_regions(&mgr).await.regions.len() == 1 },
        )
        .await;
        assert!(merged, "normal merge must reduce to a single region");
        compio::time::sleep(Duration::from_millis(3000)).await;
        for k in ["m-pre-00", "m-pre-04", "m-live-1"] {
            let r = psr_get(&router, SURVIVOR, k.as_bytes()).await;
            assert_eq!(
                r.code, partition_rpc::CODE_OK,
                "victim key {k} missing on survivor after the successful merge"
            );
        }
        eprintln!("[control] normal merge OK, victim data preserved on survivor");
    });
}
