mod test_helpers;

use autumn_stream::extent_rpc::{CODE_LOCKED_BY_OTHER, CODE_OK, CODE_PRECONDITION};
use test_helpers::{pick_addr, start_node, TestConn};

#[compio::test]
async fn append_rejects_stale_revision() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let alloc = conn.alloc_extent(1001).await;
    assert_eq!(alloc.code, CODE_OK);

    let first = conn.append(1001, 1, 0, 20, b"abc".to_vec()).await;
    assert_eq!(first.code, CODE_OK);

    let stale = conn.append(1001, 1, 3, 10, b"x".to_vec()).await;
    assert_eq!(
        stale.code, CODE_LOCKED_BY_OTHER,
        "stale owner_epoch should be rejected"
    );
}

#[compio::test]
async fn append_with_mid_byte_commit_truncates_and_succeeds() {
    // block_sizes removed; truncate is byte-granular, no alignment check.
    // commit=6 truncates the file to 6 bytes, then appends the new payload.
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let alloc = conn.alloc_extent(1002).await;
    assert_eq!(alloc.code, CODE_OK);

    let first = conn.append(1002, 1, 0, 30, b"helloworld".to_vec()).await;
    assert_eq!(first.code, CODE_OK);
    assert_eq!(first.end, 10);

    // commit=6 truncates to 6 bytes (byte-granular), then appends "!" → end=7
    let partial = conn.append(1002, 1, 6, 30, b"!".to_vec()).await;
    assert_eq!(partial.code, CODE_OK, "mid-byte commit should succeed");
    assert_eq!(
        partial.end, 7,
        "truncated to 6 then appended 1 byte → end=7"
    );

    let cl = conn.commit_length(1002, 30).await;
    assert_eq!(cl.code, CODE_OK);
    assert_eq!(cl.length, 7);
}

/// batch append path must reject with PRECONDITION when the extent is
/// sealed, even when the append carries a commit value lower than file_start.
/// This exercises the sealed check in `build_append_future` step 2 (local
/// atomics) and ensures the batch hot-path doesn't silently truncate a
/// sealed extent.
#[compio::test]
async fn batch_append_rejects_sealed_extent_with_low_commit() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    let node = start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let eid: u64 = 2001;
    let alloc = conn.alloc_extent(eid).await;
    assert_eq!(alloc.code, CODE_OK);

    // Write 10 bytes so extent.len = 10.
    let w1 = conn.append(eid, 1, 0, 30, b"0123456789".to_vec()).await;
    assert_eq!(w1.code, CODE_OK);
    assert_eq!(w1.end, 10);

    // Seal the extent (length 10, eversion 2) so the append below hits the
    // sealed guard rather than a length check.
    node.test_seal_local(eid, 10, 2).await.expect("seal");

    // Attempt an append with commit=5 (lower than file_start).
    // The batch path's sealed check (step 2) should reject immediately.
    let stale = conn.append(eid, 2, 5, 30, b"x".to_vec()).await;
    assert_eq!(
        stale.code, CODE_PRECONDITION,
        "batch append on a sealed extent must return PRECONDITION"
    );
}

// ─────────────────────────────────────────────────────────────────────────
// BULK-EXACT invariant (open-tail-rotate root fix, coco P1): a bulk read
// (MSG_READ_BYTES_BULK) is always an exact-length VP value read — the EN must
// NEVER answer CODE_OK with a silently SHORT payload. `read_plan` clamps to
// the local bytes (correct for the non-bulk scanner path), but for bulk an
// unservable range is a REJECTION (CODE_PRECONDITION), so no bulk consumer
// needs its own defensive length check and a rotated-to replica that is
// somehow short can never hand a truncated value to a client.
// ─────────────────────────────────────────────────────────────────────────

#[compio::test]
async fn bulk_read_rejects_short_range_instead_of_short_ok() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let eid = 4001u64;
    assert_eq!(conn.alloc_extent(eid).await.code, CODE_OK);
    let w = conn.append(eid, 1, 0, 20, b"0123456789".to_vec()).await;
    assert_eq!(w.code, CODE_OK);
    assert_eq!(w.end, 10);

    // Exact in-range bulk read → OK + the full requested bytes.
    let (code, payload) = conn.read_bytes_bulk(eid, 1, 2, 5).await;
    assert_eq!(code, CODE_OK);
    assert_eq!(&payload, b"23456", "in-range bulk read returns exact bytes");

    // Over-range bulk read (want 20, extent holds 10) → REJECTED, not OK+short.
    let (code, payload) = conn.read_bytes_bulk(eid, 1, 0, 20).await;
    assert_eq!(
        code,
        CODE_PRECONDITION,
        "over-range bulk read must be rejected — CODE_OK+short payload would let \
         a truncated value reach a client (payload len {})",
        payload.len()
    );

    // Offset past the end entirely → same rejection.
    let (code, _p) = conn.read_bytes_bulk(eid, 1, 15, 4).await;
    assert_eq!(
        code, CODE_PRECONDITION,
        "past-end bulk read must be rejected"
    );
}
