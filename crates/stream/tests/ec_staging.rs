//! EC conversion staging.
//!
//! Conversion publishes nothing node-by-node: `WriteShard` stages into
//! `extent-{id}.shard{i}`, an additive file, and the manager's layout flip is
//! the single commit point. There is no commit phase and no rename, so the
//! properties that matter are that staging never disturbs `.dat`, that it is
//! idempotent, and that it is attempt-scoped.

mod test_helpers;

use autumn_stream::extent_rpc::{CODE_LOCKED_BY_OTHER, CODE_OK};
use test_helpers::{pick_addr, start_node, TestConn};

/// Staging must not disturb the replica it is derived from: the shard goes to
/// its own file, and `.dat` still serves the whole value. This is what makes an
/// abandoned attempt free — there is nothing to undo.
#[compio::test]
async fn prepare_preserves_original_data() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 9001;
    assert_eq!(conn.alloc_extent(extent_id).await.code, CODE_OK);

    let original = vec![0x42u8; 2048];
    let append = conn.append(extent_id, 1, 0, 0, original.clone()).await;
    assert_eq!(append.code, CODE_OK);

    // Phase 1: write shard to staging. eversion=5 is the post-EC target.
    let shard_payload = vec![0xABu8; 1024];
    let ws = conn
        .write_shard(extent_id, 0, 2048, 5, shard_payload.clone())
        .await;
    assert_eq!(ws.code, CODE_OK, "write_shard (prepare) should succeed");

    // Original data is still readable at the current eversion (1).
    let read = conn.read_bytes(extent_id, 1, 0, 2048).await;
    assert_eq!(
        read.code, CODE_OK,
        "original data must still be readable after prepare"
    );
    assert_eq!(read.payload.len(), 2048);
    assert_eq!(&read.payload[..], &original[..]);
}

/// Staging is idempotent: re-running it is how a re-dispatched attempt makes
/// progress, so a second identical `WriteShard` must succeed.
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

/// A coordinator whose marker was released keeps streaming stripes into the
/// same staging file its successor is now filling, and the `owner_epoch` fence
/// does not stop it: that fence only rises when the ex-coordinator was FENCED,
/// while a routine release (its node went offline, or the assignment was
/// re-derived) leaves its epoch untouched. Attempt nonces are etcd revisions,
/// so the superseded writer is the one carrying the LOWER nonce.
#[compio::test]
async fn a_superseded_attempts_stripe_is_refused() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 9007;
    assert_eq!(conn.alloc_extent(extent_id).await.code, CODE_OK);

    let old_attempt = 500u64;
    let new_attempt = 517u64;
    let shard = vec![0x11u8; 512];

    // The live attempt stages first.
    let ok = conn
        .write_shard_with_nonce(extent_id, 0, 512, 3, new_attempt, shard.clone())
        .await;
    assert_eq!(ok.code, CODE_OK);

    // The predecessor, still running, tries to write into the same file.
    let stale = conn
        .write_shard_with_nonce(extent_id, 0, 512, 3, old_attempt, vec![0x22u8; 512])
        .await;
    assert_eq!(
        stale.code, CODE_LOCKED_BY_OTHER,
        "a stripe from a superseded attempt must not land in the live attempt's staging"
    );

    // The live attempt keeps going, unaffected.
    let still_ok = conn
        .write_shard_with_nonce(extent_id, 0, 512, 3, new_attempt, shard)
        .await;
    assert_eq!(still_ok.code, CODE_OK);
}

