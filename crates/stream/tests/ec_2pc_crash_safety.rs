//! EC 2PC crash-safety tests.
//!
//! Verifies that the two-phase commit protocol for EC conversion
//! preserves data integrity under all intermediate states:
//!
//! - Phase 1 (prepare / WriteShard): shard data goes to `.ec.dat`,
//!   original `.dat` stays intact.
//! - Phase 2 (commit / CommitEcShard): atomic rename `.ec.dat` → `.dat`,
//!   eversion bumped.
//! - Idempotency: both prepare and commit can be safely retried.
//! - Crash between phases: original data is still readable.

mod test_helpers;

use autumn_stream::extent_rpc::{CODE_EVERSION_MISMATCH, CODE_OK};
use test_helpers::{pick_addr, start_node, TestConn};

/// After WriteShard (Phase 1 prepare), original data must still be
/// readable — the shard goes to `.ec.dat` staging, not `.dat`.
#[compio::test]
async fn prepare_preserves_original_data() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 9001;
    assert_eq!(conn.alloc_extent(extent_id).await.code, CODE_OK);

    let original = vec![0x42u8; 2048];
    let append = conn.append(extent_id, 1, 0, 0, false, original.clone()).await;
    assert_eq!(append.code, CODE_OK);

    // Phase 1: write shard to staging. eversion=5 is the post-EC target.
    let shard_payload = vec![0xABu8; 1024];
    let ws = conn
        .write_shard(extent_id, 0, 2048, 5, shard_payload.clone())
        .await;
    assert_eq!(ws.code, CODE_OK, "write_shard (prepare) should succeed");

    // Original data is still readable at the current eversion (1).
    let read = conn.read_bytes(extent_id, 1, 0, 2048).await;
    assert_eq!(read.code, CODE_OK, "original data must still be readable after prepare");
    assert_eq!(read.payload.len(), 2048);
    assert_eq!(&read.payload[..], &original[..]);
}

/// After CommitEcShard (Phase 2 commit), the shard data replaces the
/// original and the eversion is bumped.
#[compio::test]
async fn commit_switches_to_shard_data() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 9002;
    assert_eq!(conn.alloc_extent(extent_id).await.code, CODE_OK);

    let original = vec![0x42u8; 2048];
    conn.append(extent_id, 1, 0, 0, false, original).await;

    let shard_payload = vec![0xCDu8; 1024];
    let ws = conn
        .write_shard(extent_id, 0, 2048, 5, shard_payload.clone())
        .await;
    assert_eq!(ws.code, CODE_OK);

    // Phase 2: commit — renames .ec.dat → .dat, bumps eversion to 5.
    let cs = conn.commit_ec_shard(extent_id, 2048, 5).await;
    assert_eq!(cs.code, CODE_OK, "commit_ec_shard should succeed");

    // Old eversion (1) is now stale.
    let stale = conn.read_bytes(extent_id, 1, 0, 1024).await;
    assert_eq!(
        stale.code, CODE_EVERSION_MISMATCH,
        "old eversion must be rejected after commit"
    );

    // New eversion (5) returns shard data.
    let ok = conn.read_bytes(extent_id, 5, 0, 1024).await;
    assert_eq!(ok.code, CODE_OK);
    assert_eq!(ok.payload.len(), 1024);
    assert_eq!(&ok.payload[..], &shard_payload[..]);
}

/// WriteShard is idempotent: calling it twice with the same shard size
/// succeeds (the second call is a no-op).
#[compio::test]
async fn idempotent_prepare() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 9003;
    assert_eq!(conn.alloc_extent(extent_id).await.code, CODE_OK);

    let shard = vec![0xEEu8; 512];
    let ws1 = conn.write_shard(extent_id, 0, 512, 3, shard.clone()).await;
    assert_eq!(ws1.code, CODE_OK);

    // Second call with same shard size — should succeed (idempotent skip).
    let ws2 = conn.write_shard(extent_id, 0, 512, 3, shard).await;
    assert_eq!(ws2.code, CODE_OK, "idempotent prepare must succeed");
}

/// CommitEcShard is idempotent: calling it after the staging file is
/// already renamed (eversion already at target) succeeds.
#[compio::test]
async fn idempotent_commit() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 9004;
    assert_eq!(conn.alloc_extent(extent_id).await.code, CODE_OK);

    let shard = vec![0xBBu8; 256];
    let ws = conn.write_shard(extent_id, 0, 256, 4, shard).await;
    assert_eq!(ws.code, CODE_OK);

    let cs1 = conn.commit_ec_shard(extent_id, 256, 4).await;
    assert_eq!(cs1.code, CODE_OK);

    // Second commit — staging file gone but eversion matches → idempotent OK.
    let cs2 = conn.commit_ec_shard(extent_id, 256, 4).await;
    assert_eq!(cs2.code, CODE_OK, "idempotent commit must succeed");
}

/// Simulates a crash between Phase 1 and Phase 2: the staging file
/// (.ec.dat) exists alongside the original (.dat). Original data must
/// still be readable. Completing Phase 2 afterwards must succeed.
#[compio::test]
async fn crash_between_prepare_and_commit_preserves_data() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 9005;
    assert_eq!(conn.alloc_extent(extent_id).await.code, CODE_OK);

    let original = vec![0x11u8; 4096];
    let append = conn.append(extent_id, 1, 0, 0, false, original.clone()).await;
    assert_eq!(append.code, CODE_OK);

    // Prepare: shard goes to .ec.dat
    let shard = vec![0x22u8; 2048];
    let ws = conn.write_shard(extent_id, 0, 4096, 6, shard.clone()).await;
    assert_eq!(ws.code, CODE_OK);

    // Simulate "crash" state: both .ec.dat and .dat exist.
    // Original data is still intact and readable.
    let read = conn.read_bytes(extent_id, 1, 0, 4096).await;
    assert_eq!(read.code, CODE_OK, "original data must survive a prepare-only crash");
    assert_eq!(&read.payload[..], &original[..]);

    // "Recovery" completes Phase 2 via retry.
    let cs = conn.commit_ec_shard(extent_id, 4096, 6).await;
    assert_eq!(cs.code, CODE_OK, "commit after simulated crash must succeed");

    // After recovery-commit, shard data is live.
    let read2 = conn.read_bytes(extent_id, 6, 0, 2048).await;
    assert_eq!(read2.code, CODE_OK);
    assert_eq!(&read2.payload[..], &shard[..]);
}
