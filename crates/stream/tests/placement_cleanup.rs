//! Cleanup after the layout flip: the manager says which payload file a node
//! should hold, and the node drops the others.
//!
//! Every test here is about a DELETE, so the ones that matter most are the
//! refusals. A wrong cleanup does not degrade — it destroys the only copy a
//! node has.

mod test_helpers;

use autumn_stream::extent_rpc::{PAYLOAD_LOCATION_IN_DAT, PAYLOAD_LOCATION_IN_SHARD_FILE};
use autumn_stream::{ExtentNode, ExtentNodeConfig};

/// The hashed subdir an extent's files live in. Located by any surviving file
/// for that extent, so it keeps working after a payload file is reclaimed —
/// these paths are asserted on for ABSENCE as much as presence.
fn extent_dir(root: &std::path::Path, extent_id: u64) -> std::path::PathBuf {
    for byte in 0u8..=255 {
        let sub = root.join(format!("{byte:02x}"));
        let Ok(rd) = std::fs::read_dir(&sub) else {
            continue;
        };
        for e in rd.flatten() {
            if e.file_name()
                .to_string_lossy()
                .starts_with(&format!("extent-{extent_id}."))
            {
                return sub;
            }
        }
    }
    panic!("no files at all for extent {extent_id}");
}

fn dat(root: &std::path::Path, extent_id: u64) -> std::path::PathBuf {
    extent_dir(root, extent_id).join(format!("extent-{extent_id}.dat"))
}

fn shard(root: &std::path::Path, extent_id: u64, idx: u32) -> std::path::PathBuf {
    extent_dir(root, extent_id).join(format!("extent-{extent_id}.shard{idx}"))
}

/// Build a node over `dir` (loading whatever is on disk) without serving it.
async fn node_over(dir: &std::path::Path) -> ExtentNode {
    ExtentNode::new(ExtentNodeConfig::new(dir.to_path_buf(), 1))
        .await
        .expect("create ExtentNode")
}

/// Lay down an extent with a `.dat` and, optionally, shard files, by driving a
/// served node over the real RPC path — then let it go, so the cleanup runs
/// against what is actually on disk.
async fn seed(dir: &std::path::Path, extent_id: u64, dat_len: usize, shards: &[(u32, usize)]) {
    let addr = test_helpers::pick_addr();
    test_helpers::start_node(dir, addr).await;
    let conn = test_helpers::TestConn::new(addr);
    assert_eq!(
        conn.alloc_extent(extent_id).await.code,
        autumn_stream::extent_rpc::CODE_OK
    );
    assert_eq!(
        conn.append(extent_id, 1, 0, 0, vec![0x5Au8; dat_len]).await.code,
        autumn_stream::extent_rpc::CODE_OK
    );
    for (idx, len) in shards {
        std::fs::write(shard(dir, extent_id, *idx), vec![0xC7u8; *len]).expect("plant shard");
    }
}

/// The post-flip case: the layout names this node's shard, so the `.dat` it was
/// derived from is redundant and gets reclaimed. This is the whole point — until
/// it works, every converted extent occupies both forms forever.
#[compio::test]
async fn the_pre_conversion_dat_is_reclaimed_once_the_shard_is_held() {
    let d = tempfile::tempdir().expect("tempdir");
    let eid = 9200;
    seed(d.path(), eid, 3000, &[(1, 1000)]).await;

    let node = node_over(d.path()).await;
    node.test_apply_placements(&[(eid, PAYLOAD_LOCATION_IN_SHARD_FILE, 1)], node.test_staging_tick())
        .await;

    assert!(!dat(d.path(), eid).exists(), "the redundant .dat was not reclaimed");
    assert!(shard(d.path(), eid, 1).exists(), "the kept shard must survive");
}

/// The refusal that matters most: if the shard the layout names is NOT here,
/// the `.dat` is the only copy this node has. A placement that arrives before
/// staging finishes — or names a shard this node never received — must not
/// destroy it.
#[compio::test]
async fn the_dat_survives_when_the_named_shard_is_absent() {
    let d = tempfile::tempdir().expect("tempdir");
    let eid = 9201;
    seed(d.path(), eid, 3000, &[]).await;

    let node = node_over(d.path()).await;
    node.test_apply_placements(&[(eid, PAYLOAD_LOCATION_IN_SHARD_FILE, 2)], node.test_staging_tick())
        .await;

    assert!(
        dat(d.path(), eid).exists(),
        "the only copy this node holds must never be deleted on an instruction it cannot satisfy"
    );
}

/// Rollback residue: the attempt was abandoned, so the layout still says
/// `InDat`. The staged shard is garbage and the `.dat` is authoritative — the
/// same rule, applied the other way round, with no second mechanism.
#[compio::test]
async fn an_abandoned_attempts_shard_is_dropped_and_the_dat_kept() {
    let d = tempfile::tempdir().expect("tempdir");
    let eid = 9202;
    seed(d.path(), eid, 3000, &[(0, 1000)]).await;

    let node = node_over(d.path()).await;
    node.test_apply_placements(&[(eid, PAYLOAD_LOCATION_IN_DAT, 0)], node.test_staging_tick())
        .await;

    assert!(!shard(d.path(), eid, 0).exists(), "rollback residue was not cleaned");
    assert!(dat(d.path(), eid).exists(), "the authoritative .dat must survive");
}

/// A node can legitimately hold shards at two indices (two attempts, or a
/// parity slot beside a data slot after a reassignment). Only the one the
/// layout names survives.
#[compio::test]
async fn only_the_named_shard_index_survives() {
    let d = tempfile::tempdir().expect("tempdir");
    let eid = 9203;
    seed(d.path(), eid, 3000, &[(0, 1000), (3, 1000)]).await;

    let node = node_over(d.path()).await;
    node.test_apply_placements(&[(eid, PAYLOAD_LOCATION_IN_SHARD_FILE, 3)], node.test_staging_tick())
        .await;

    assert!(shard(d.path(), eid, 3).exists(), "the named shard must survive");
    assert!(!shard(d.path(), eid, 0).exists(), "the other attempt's shard is residue");
    assert!(!dat(d.path(), eid).exists());
}

/// Cleanup is idempotent and re-convergent: running it again after it has
/// already converged changes nothing. This is what lets a crash mid-cleanup be
/// resolved by re-running rather than by an intent marker.
#[compio::test]
async fn cleanup_is_idempotent() {
    let d = tempfile::tempdir().expect("tempdir");
    let eid = 9204;
    seed(d.path(), eid, 3000, &[(1, 1000)]).await;

    let node = node_over(d.path()).await;
    for _ in 0..3 {
        node.test_apply_placements(&[(eid, PAYLOAD_LOCATION_IN_SHARD_FILE, 1)], node.test_staging_tick())
            .await;
    }
    assert!(shard(d.path(), eid, 1).exists());
    assert!(!dat(d.path(), eid).exists());

    // And it survives a restart, which is the path a mid-cleanup crash takes.
    drop(node);
    let node = node_over(d.path()).await;
    node.test_apply_placements(&[(eid, PAYLOAD_LOCATION_IN_SHARD_FILE, 1)], node.test_staging_tick())
        .await;
    assert!(shard(d.path(), eid, 1).exists(), "the shard must survive a restart + re-run");
}

/// After cleanup the node serves its shard and reports `.dat` gone — it must
/// not resurrect an empty `.dat` for a reader that still asks for one.
#[compio::test]
async fn after_cleanup_the_node_serves_the_shard_and_not_a_resurrected_dat() {
    let d = tempfile::tempdir().expect("tempdir");
    let eid = 9205;
    let shard_bytes = vec![0xC7u8; 1000];
    seed(d.path(), eid, 3000, &[(2, 1000)]).await;

    let node = node_over(d.path()).await;
    node.test_apply_placements(&[(eid, PAYLOAD_LOCATION_IN_SHARD_FILE, 2)], node.test_staging_tick())
        .await;
    drop(node);

    let addr = test_helpers::pick_addr();
    test_helpers::start_node(d.path(), addr).await;
    let conn = test_helpers::TestConn::new(addr);

    let sh = conn
        .read_bytes_from(
            eid,
            1,
            0,
            1000,
            autumn_stream::extent_rpc::PayloadRef::shard(2),
        )
        .await;
    assert_eq!(sh.code, autumn_stream::extent_rpc::CODE_OK);
    assert_eq!(&sh.payload[..], &shard_bytes[..]);

    let d2 = conn
        .read_bytes_from(eid, 1, 0, 1000, autumn_stream::extent_rpc::PayloadRef::in_dat())
        .await;
    assert_eq!(
        d2.code,
        autumn_stream::extent_rpc::CODE_PAYLOAD_NOT_HERE,
        "a reclaimed .dat must be reported missing, never re-created"
    );
    assert!(!dat(d.path(), eid).exists());
}

/// A placement is computed by the manager at ANSWER time and applied by the
/// node LATER. A conversion can start in that window — and a PARTICIPANT
/// staging a shard has no local marker (`ec_convert_inflight` is set only on
/// the coordinator), so nothing on this node knows the staging exists.
///
/// Applying the stale `InDat` verdict then deletes the shard mid-flight. The
/// coordinator's next stripe recreates the file and pwrites at its offset,
/// leaving ZERO HOLES where the earlier stripes were — and the flip publishes
/// it, because the completion report is from the same attempt and passes every
/// nonce/reporter check. Reads return zeros with CODE_OK.
///
/// What makes the verdict stale is not "this node has staged at some point" —
/// it is that the staging landed AFTER the question went out. So the tick is
/// sampled first, exactly where the reconcile samples it.
#[compio::test]
async fn a_stale_placement_must_not_delete_a_shard_being_staged() {
    let d = tempfile::tempdir().expect("tempdir");
    let eid = 9210;
    let addr = test_helpers::pick_addr();
    let node = test_helpers::start_node(d.path(), addr).await;
    let conn = test_helpers::TestConn::new(addr);

    assert_eq!(
        conn.alloc_extent(eid).await.code,
        autumn_stream::extent_rpc::CODE_OK
    );
    assert_eq!(
        conn.append(eid, 1, 0, 0, vec![0x5Au8; 3000]).await.code,
        autumn_stream::extent_rpc::CODE_OK
    );

    // The reconcile question goes out here, before any staging exists.
    let asked_at = node.test_staging_tick();

    // A conversion begins: this node is a PARTICIPANT and stages stripe 0 of
    // shard 1. It sets no local inflight marker — only the coordinator does.
    let stripe = vec![0xC7u8; 500];
    assert_eq!(
        conn.write_shard_with_nonce(eid, 1, 1500, 5, 4242, stripe.clone())
            .await
            .code,
        autumn_stream::extent_rpc::CODE_OK
    );
    assert!(shard(d.path(), eid, 1).exists(), "staging did not land");

    // The answer to that older question now arrives.
    node.test_apply_placements(&[(eid, PAYLOAD_LOCATION_IN_DAT, 0)], asked_at)
        .await;

    assert!(
        shard(d.path(), eid, 1).exists(),
        "cleanup deleted a shard that an in-flight attempt is still staging; \
         the coordinator's next stripe would recreate it with zero holes and \
         the flip would publish that as this node's shard"
    );
}

/// An abandoned attempt's staging must actually be reclaimed.
///
/// This is the case neither existing mechanism reaches: `remove_extent_files`
/// waits for the whole extent to be deleted, and the orphan reconcile's second
/// leg only fires once the `.dat` is already gone. After a conversion is given
/// up the extent is still alive in replicated form — `.dat` present, staged
/// `extent-{id}.shard{N}` present — so the space was held until some later
/// attempt happened to reuse the file.
///
/// The verdict that reclaims it is an ordinary `InDat` placement. Two facts
/// narrow what it can be describing: the manager withholds a placement entirely
/// while an op is in flight on the extent, so answering at all means no MARKER
/// was held; and the staging tick says nothing staged here since the question
/// went out. That is not the same as "no attempt is writing" — see
/// `a_reclaim_that_lands_mid_attempt_is_refused_by_the_next_stripe` for the
/// window where one is, and what makes it harmless.
#[compio::test]
async fn an_abandoned_attempts_real_staging_is_reclaimed() {
    let d = tempfile::tempdir().expect("tempdir");
    let eid = 9212;
    let addr = test_helpers::pick_addr();
    let node = test_helpers::start_node(d.path(), addr).await;
    let conn = test_helpers::TestConn::new(addr);

    assert_eq!(
        conn.alloc_extent(eid).await.code,
        autumn_stream::extent_rpc::CODE_OK
    );
    assert_eq!(
        conn.append(eid, 1, 0, 0, vec![0x5Au8; 3000]).await.code,
        autumn_stream::extent_rpc::CODE_OK
    );
    // A conversion stages this node's shard through the real participant path,
    // so the node carries a genuine stage mark — not a planted file.
    assert_eq!(
        conn.write_shard_with_nonce(eid, 1, 1500, 5, 4242, vec![0xC7u8; 1500])
            .await
            .code,
        autumn_stream::extent_rpc::CODE_OK
    );
    assert!(shard(d.path(), eid, 1).exists(), "staging did not land");

    // The attempt is then abandoned: the manager releases the marker without
    // flipping the layout, so the next reconcile — asked AFTER the staging —
    // still says the payload is in `.dat`.
    let asked_at = node.test_staging_tick();
    assert!(
        asked_at >= 1,
        "the staging left no mark, so this would also pass with the guard \
         removed entirely"
    );
    node.test_apply_placements(&[(eid, PAYLOAD_LOCATION_IN_DAT, 0)], asked_at)
        .await;

    assert!(
        !shard(d.path(), eid, 1).exists(),
        "the abandoned attempt's staging was not reclaimed — it is named by no \
         layout and no other sweep will ever collect it"
    );
    assert!(dat(d.path(), eid).exists(), "the authoritative .dat must survive");
    // And the extent still reads: reclaiming staging must not disturb the
    // payload the cluster is actually pointed at.
    let r = conn.read_bytes(eid, 1, 0, 3000).await;
    assert_eq!(
        r.code,
        autumn_stream::extent_rpc::CODE_OK,
        "the extent is no longer readable after reclaiming staging"
    );
    assert_eq!(
        r.payload.as_ref(),
        &vec![0x5Au8; 3000][..],
        "the .dat bytes changed"
    );
}

/// The reclaim CAN land on an attempt that is still writing — and what keeps
/// that survivable lives one layer down, so it must not be allowed to rot.
///
/// The abandon this reclaim was built for fires on the dispatch tick where the
/// coordinator has JUST started a fresh attempt (the failure tally only counts a
/// dispatch that started a new one). So the marker is released while stripes are
/// streaming, placements for the extent resume immediately, and a participant
/// whose reconcile question went out after that attempt's first stripe gets an
/// `InDat` verdict it is entitled to act on. It deletes staging that is being
/// written. That is not a hole in the guard; it is the guard working on the
/// information it has.
///
/// Nothing is corrupted, for two reasons outside this sweep. The one this test
/// pins: a later stripe whose staging file has vanished is REFUSED rather than
/// recreated at its offset, which would leave zero holes where the earlier
/// stripes were and let the flip publish them. (The other: an attempt whose
/// marker was abandoned can never commit anyway — its completion report finds no
/// marker and is ignored — so the bytes deleted here were already dead.)
#[compio::test]
async fn a_reclaim_that_lands_mid_attempt_is_refused_by_the_next_stripe() {
    let d = tempfile::tempdir().expect("tempdir");
    let eid = 9213;
    let addr = test_helpers::pick_addr();
    let node = test_helpers::start_node(d.path(), addr).await;
    let conn = test_helpers::TestConn::new(addr);

    assert_eq!(
        conn.alloc_extent(eid).await.code,
        autumn_stream::extent_rpc::CODE_OK
    );
    assert_eq!(
        conn.append(eid, 1, 0, 0, vec![0x5Au8; 3000]).await.code,
        autumn_stream::extent_rpc::CODE_OK
    );
    // Stripe 0 of a live attempt.
    assert_eq!(
        conn.write_shard_stripe(eid, 1, 1500, 5, 4242, 0, vec![0xC7u8; 500])
            .await
            .expect("stripe 0")
            .code,
        autumn_stream::extent_rpc::CODE_OK
    );

    // The marker is abandoned mid-attempt, so the next verdict says `.dat` and
    // is newer than the staging. The reclaim fires — correctly.
    let asked_at = node.test_staging_tick();
    node.test_apply_placements(&[(eid, PAYLOAD_LOCATION_IN_DAT, 0)], asked_at)
        .await;
    assert!(
        !shard(d.path(), eid, 1).exists(),
        "precondition: this test is about what happens AFTER the reclaim deletes it"
    );

    // The coordinator, which knows nothing of any of this, sends stripe 1.
    let Err(err) = conn
        .write_shard_stripe(eid, 1, 1500, 5, 4242, 500, vec![0xC7u8; 500])
        .await
    else {
        panic!(
            "a stripe whose staging vanished was ACCEPTED — it recreated the file at \
             its own offset, leaving zero holes where stripe 0 was, and the flip \
             would publish that as this node's shard"
        );
    };
    assert!(
        err.contains("staging file vanished"),
        "refused for the wrong reason: {err}"
    );
    assert!(
        !shard(d.path(), eid, 1).exists(),
        "the refusal still left a holey staging file behind"
    );
}

/// The other side of the guard above: after the flip, cleanup must still run on
/// a node that REALLY staged (not one with a planted file). The stage marker is
/// per-extent and never expires on its own, so a guard that keyed only on "has
/// this node ever staged?" would skip cleanup forever and the `.dat` would never
/// be reclaimed — the whole point of the step.
#[compio::test]
async fn cleanup_runs_after_a_real_staging_once_the_flip_is_published() {
    let d = tempfile::tempdir().expect("tempdir");
    let eid = 9211;
    let addr = test_helpers::pick_addr();
    let node = test_helpers::start_node(d.path(), addr).await;
    let conn = test_helpers::TestConn::new(addr);

    assert_eq!(
        conn.alloc_extent(eid).await.code,
        autumn_stream::extent_rpc::CODE_OK
    );
    assert_eq!(
        conn.append(eid, 1, 0, 0, vec![0x5Au8; 3000]).await.code,
        autumn_stream::extent_rpc::CODE_OK
    );
    // Stage the whole shard through the real path, so the node carries a live
    // stage marker for this extent.
    assert_eq!(
        conn.write_shard_with_nonce(eid, 2, 1500, 5, 4242, vec![0xC7u8; 1500])
            .await
            .code,
        autumn_stream::extent_rpc::CODE_OK
    );

    // The manager flips the layout and the next reconcile names this node's
    // shard. Cleanup must reclaim the now-redundant `.dat`.
    node.test_apply_placements(&[(eid, PAYLOAD_LOCATION_IN_SHARD_FILE, 2)], node.test_staging_tick())
        .await;

    assert!(shard(d.path(), eid, 2).exists(), "the named shard must survive");
    assert!(
        !dat(d.path(), eid).exists(),
        "the pre-conversion .dat was not reclaimed — cleanup never ran"
    );
}
