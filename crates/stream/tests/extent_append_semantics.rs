mod test_helpers;

use std::time::Duration;

use autumn_stream::extent_rpc::{CODE_OK, CODE_LOCKED_BY_OTHER, CODE_PRECONDITION};
use test_helpers::{pick_addr, start_node, TestConn};

#[compio::test]
async fn append_rejects_stale_revision() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let alloc = conn.alloc_extent(1001).await;
    assert_eq!(alloc.code, CODE_OK);

    let first = conn
        .append(1001, 1, 0, 20, true, b"abc".to_vec())
        .await;
    assert_eq!(first.code, CODE_OK);

    let stale = conn
        .append(1001, 1, 3, 10, true, b"x".to_vec())
        .await;
    assert_eq!(stale.code, CODE_LOCKED_BY_OTHER, "stale revision should be rejected");
}

#[compio::test]
async fn append_with_mid_byte_commit_truncates_and_succeeds() {
    // F038: block_sizes removed; truncate is byte-granular, no alignment check.
    // commit=6 truncates the file to 6 bytes, then appends the new payload.
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let alloc = conn.alloc_extent(1002).await;
    assert_eq!(alloc.code, CODE_OK);

    let first = conn
        .append(1002, 1, 0, 30, true, b"helloworld".to_vec())
        .await;
    assert_eq!(first.code, CODE_OK);
    assert_eq!(first.end, 10);

    // commit=6 truncates to 6 bytes (byte-granular), then appends "!" → end=7
    let partial = conn
        .append(1002, 1, 6, 30, true, b"!".to_vec())
        .await;
    assert_eq!(
        partial.code, CODE_OK,
        "mid-byte commit should succeed"
    );
    assert_eq!(
        partial.end, 7,
        "truncated to 6 then appended 1 byte → end=7"
    );

    let cl = conn.commit_length(1002, 30).await;
    assert_eq!(cl.code, CODE_OK);
    assert_eq!(cl.length, 7);
}

/// F123: batch append path must reject with PRECONDITION when the extent is
/// sealed, even when the append carries a commit value lower than file_start.
/// This exercises the sealed check in `build_append_future` step 2 (local
/// atomics) and ensures the batch hot-path doesn't silently truncate a
/// sealed extent.
#[compio::test]
async fn f123_batch_append_rejects_sealed_extent_with_low_commit() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let eid: u64 = 2001;
    let alloc = conn.alloc_extent(eid).await;
    assert_eq!(alloc.code, CODE_OK);

    // Write 10 bytes so extent.len = 10.
    let w1 = conn.append(eid, 1, 0, 30, true, b"0123456789".to_vec()).await;
    assert_eq!(w1.code, CODE_OK);
    assert_eq!(w1.end, 10);

    // Seal the extent by writing a shard and committing it.
    // write_shard creates .ec.dat; commit_ec_shard renames .ec.dat→.dat,
    // bumps eversion and sets sealed_length + avali.
    let shard_data = b"shard_data".to_vec();
    let ws = conn.write_shard(eid, 0, 10, 2, shard_data).await;
    assert_eq!(ws.code, CODE_OK);
    let cs = conn.commit_ec_shard(eid, 10, 2).await;
    assert_eq!(cs.code, CODE_OK);

    // Attempt an append with commit=5 (lower than file_start).
    // The batch path's sealed check (step 2) should reject immediately.
    let stale = conn.append(eid, 2, 5, 30, true, b"x".to_vec()).await;
    assert_eq!(
        stale.code, CODE_PRECONDITION,
        "batch append on a sealed extent must return PRECONDITION"
    );
}
