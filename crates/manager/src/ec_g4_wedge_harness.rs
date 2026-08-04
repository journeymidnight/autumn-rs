//! G4 / BUG-EC-APPLY-FAIL loop-level reproduce harness (reproduce-first).
//!
//! Scenario G4 ("EC apply-fail wedge"): a sealed extent's `ConvertToEc` RPC
//! succeeds on the coordinator EN, but the manager's `apply_ec_conversion_done`
//! etcd write hits a TRANSIENT error WITHOUT losing leadership. Pre-fix
//! (`d5c0220^`) the dispatch loop did:
//!
//! ```ignore
//! if rpc_ok { let _ = self.apply_ec_conversion_done(...).await; }  // (a) swallow
//! if rpc_ok { self.commit_extent_inflight_release(extent_id); }    // (b) unconditional
//! ```
//!
//! so the in-memory `ConvertToEc` marker was dropped even though the apply
//! failed. Because `ec_conversion_dispatch_loop` is **drain-only** (candidates
//! come only from the in-memory inflight ledger, never a fresh stream re-scan —
//! (c)), the extent was then never re-dispatched: it stayed manager-pre-EC /
//! EN-post-EC and every read wedged on `EVERSION_MISMATCH` until a manager
//! failover replayed the etcd marker.
//!
//! The fix (`finalize_ec_dispatch_after_convert`) releases the marker ONLY on
//! apply success and KEEPS it on failure, so the retained marker stays a live
//! re-dispatch candidate and the drain-only loop self-heals on the next tick —
//! no failover needed.
//!
//! This harness drives the loop's two internal phases directly
//! (`collect_ec_dispatch_candidates` + `finalize_ec_dispatch_after_convert`) in
//! memory mode. The transient apply failure is injected with a one-shot
//! `_test_arm_ec_apply_fail_once` failpoint that returns `Internal` BEFORE any
//! etcd/leadership interaction — a faithful model of "apply's etcd txn blipped
//! while this manager stayed leader". Everything runs on ONE manager instance:
//! there is NO failover / `replay_from_etcd` anywhere in these tests, so a green
//! result proves the extent self-heals via re-dispatch alone.

use crate::extent_inflight::ExtentOpKind;
use crate::AutumnManager;
use autumn_rpc::manager_rpc::{MgrExtentInfo, MgrStreamInfo};

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    compio::runtime::Runtime::new().unwrap().block_on(f)
}

/// A sealed, fully-replicated, pre-conversion extent (K=3 replicas, no parity).
fn pre_ec_extent(extent_id: u64) -> MgrExtentInfo {
    MgrExtentInfo {
        extent_id,
        replicates: vec![1, 3, 5],
        parity: vec![],
        eversion: 3,
        refs: 1,
        vp_table_refs: 0,
        sealed_length: 4096,
        sealed: true,
        avali: 0x7,
        replicate_disks: vec![10, 30, 50],
        parity_disks: vec![],
        ec_converted: false,
    }
}

/// A K=2 + M=1 EC stream that owns `extent_id` (what makes the extent an EC
/// conversion candidate once a marker is enrolled).
fn ec_stream(stream_id: u64, extent_id: u64) -> MgrStreamInfo {
    MgrStreamInfo {
        stream_id,
        extent_ids: vec![extent_id],
        ec_data_shard: 2,
        ec_parity_shard: 1,
        replicates: 3,
    }
}

fn is_candidate(m: &AutumnManager, extent_id: u64) -> bool {
    let (cands, _) = m.collect_ec_dispatch_candidates();
    cands.iter().any(|c| c.ex.extent_id == extent_id)
}

/// REPRODUCE + HEAL: a transient apply failure must NOT wedge the extent — the
/// retained marker keeps it a drain-only re-dispatch candidate, and the next
/// tick's re-dispatch converts it, all WITHOUT a manager failover.
///
/// Pre-fix this test would fail at the first `is_candidate` assertion after the
/// injected failure (the marker was dropped → empty candidate set → the loop
/// never re-dispatches → permanent `EVERSION_MISMATCH` wedge).
#[test]
fn g4_transient_apply_fail_selfheals_via_redispatch_without_failover() {
    block_on(async {
        let m = AutumnManager::new();
        let eid: u64 = 8001;

        {
            let mut s = m.store.inner.borrow_mut();
            s.streams.insert(9001, ec_stream(9001, eid));
            s.extents.insert(eid, pre_ec_extent(eid));
        }
        // The ConvertToEc marker `handle_force_ec_convert` would have persisted.
        m._test_mark_ec_inflight(eid);

        // Pre-convert: the drain-only loop sees the extent as a candidate.
        assert!(
            is_candidate(&m, eid),
            "precondition: enrolled + sealed + unconverted extent is a candidate"
        );

        // ── Dispatch tick 1: convert RPC succeeds, apply hits a transient blip ──
        super::_test_arm_ec_apply_fail_once();
        m.finalize_ec_dispatch_after_convert(eid, vec![1, 3, 5], vec![70], 2, 4)
            .await;

        // Fix property #1 — the marker survives a leadership-retained apply fail.
        assert_eq!(
            m.extent_inflight_op(eid),
            Some(ExtentOpKind::ConvertToEc),
            "apply-fail MUST retain the marker (pre-fix dropped it → wedge)"
        );
        // Apply never landed: the extent is still manager-pre-EC.
        assert!(
            !m.store.inner.borrow().extents.get(&eid).unwrap().ec_converted,
            "failed apply must leave the extent unconverted"
        );

        // Fix property #2 — the retained marker keeps the extent a live
        // re-dispatch candidate. This is the crux: the drain-only loop WILL
        // retry on its next tick with NO failover. Pre-fix, the candidate set
        // is empty here and only `replay_from_etcd` (failover) could recover it.
        assert!(
            is_candidate(&m, eid),
            "retained marker MUST keep the extent a re-dispatch candidate \
             (self-heal without failover)"
        );

        // ── Dispatch tick 2 (~5 s later in prod): re-dispatch, apply succeeds ──
        m.finalize_ec_dispatch_after_convert(eid, vec![1, 3, 5], vec![70], 2, 4)
            .await;

        // Healed on the SAME manager — no restart, no replay_from_etcd.
        assert_eq!(
            m.extent_inflight_op(eid),
            None,
            "successful re-dispatch releases the marker"
        );
        let ex = m.store.inner.borrow().extents.get(&eid).cloned().unwrap();
        assert!(ex.ec_converted, "extent is EC-converted after re-dispatch");
        assert_eq!(ex.eversion, 4, "eversion bumped to the in-band post-EC value");
        assert!(
            !is_candidate(&m, eid),
            "a converted extent drops out of the candidate set"
        );
    });
}

/// EVIDENCE for claim (c): the dispatch loop is DRAIN-ONLY. A sealed,
/// unconverted extent on an EC stream with NO ledger marker is NEVER a
/// candidate — the loop does not re-scan `streams` to synthesize work. This is
/// precisely why a marker dropped by the pre-fix bug wedges permanently: with
/// the marker gone there is no re-scan to rediscover the conversion, so only a
/// failover (`replay_from_etcd` rebuilding the marker from etcd) can heal it.
#[test]
fn g4_dispatch_is_drain_only_no_marker_no_candidate() {
    block_on(async {
        let m = AutumnManager::new();
        let eid: u64 = 8002;
        {
            let mut s = m.store.inner.borrow_mut();
            s.streams.insert(9002, ec_stream(9002, eid));
            s.extents.insert(eid, pre_ec_extent(eid));
        }
        // Deliberately enrol NO marker.
        assert!(
            !is_candidate(&m, eid),
            "drain-only: no ledger marker ⇒ no candidate (loop never re-scans streams)"
        );
    });
}
