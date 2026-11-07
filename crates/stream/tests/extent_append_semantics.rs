mod test_helpers;

use autumn_stream::extent_rpc::{CODE_ERROR, CODE_LOCKED_BY_OTHER, CODE_OK, CODE_PRECONDITION, READ_EXACT, READ_TO_END, READ_UP_TO};
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

/// An append whose commit is past this replica's end is refused: written at
/// the replica's end instead, it would be acked at an offset the writer never
/// leased, and the replicas would hold it at different places.
#[compio::test]
async fn an_append_past_the_replicas_end_is_refused() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);
    assert_eq!(conn.alloc_extent(1003).await.code, CODE_OK);
    assert_eq!(conn.append(1003, 1, 0, 30, b"0123456789".to_vec()).await.code, CODE_OK);

    let ahead = conn.append(1003, 1, 20, 30, b"x".to_vec()).await;
    assert_eq!(ahead.code, CODE_PRECONDITION, "commit 20 on a 10-byte replica");
    assert_eq!(conn.commit_length(1003, 30).await.length, 10, "nothing written");
}

/// batch append path must reject with PRECONDITION when the extent is
/// sealed, even when the append carries a commit value lower than file_start.
/// This exercises the sealed check in `append_burst_frames` step 2 (local
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
// EXACT invariant (open-tail-rotate root fix): an exact read
// (`MSG_READ`, `READ_EXACT`) is a VP value read — the EN must NEVER answer
// CODE_OK with a silently SHORT payload. `read_plan` clamps to the local bytes
// (correct for up-to reads), but for an exact read an unservable range is a
// REJECTION (CODE_PRECONDITION), so no consumer needs its own defensive length
// check and a rotated-to replica that is somehow short can never hand a
// truncated value to a client.
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
    let (code, payload) = conn.read_exact(eid, 1, 2, 5).await;
    assert_eq!(code, CODE_OK);
    assert_eq!(&payload, b"23456", "in-range bulk read returns exact bytes");

    // Over-range bulk read (want 20, extent holds 10) → REJECTED, not OK+short.
    let (code, payload) = conn.read_exact(eid, 1, 0, 20).await;
    assert_eq!(
        code,
        CODE_PRECONDITION,
        "over-range bulk read must be rejected — CODE_OK+short payload would let \
         a truncated value reach a client (payload len {})",
        payload.len()
    );

    // Offset past the end entirely → same rejection.
    let (code, _p) = conn.read_exact(eid, 1, 15, 4).await;
    assert_eq!(
        code, CODE_PRECONDITION,
        "past-end bulk read must be rejected"
    );
}

/// `MSG_READ`'s modes over a replica holding 10 bytes: an exact read serves
/// exactly its range or is refused (never a short OK); a read up to `length`
/// serves what the replica holds; a read to the end serves everything from
/// the offset. Every OK reply names the replica's end.
#[compio::test]
async fn read_serves_each_mode_as_it_says() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);
    let eid = 4002u64;
    assert_eq!(conn.alloc_extent(eid).await.code, CODE_OK);
    assert_eq!(conn.append(eid, 1, 0, 20, b"0123456789".to_vec()).await.code, CODE_OK);

    assert_eq!(conn.read(eid, 1, 2, 5, READ_EXACT).await, (CODE_OK, 10, b"23456".to_vec()));
    let (code, _, got) = conn.read(eid, 1, 0, 20, READ_EXACT).await;
    assert_eq!((code, got.len()), (CODE_PRECONDITION, 0), "an exact read past the end is refused");
    assert_eq!(conn.read(eid, 1, 6, 20, READ_UP_TO).await, (CODE_OK, 10, b"6789".to_vec()));
    assert_eq!(conn.read(eid, 1, 15, 4, READ_UP_TO).await, (CODE_OK, 10, Vec::new()), "past the end: nothing");
    assert_eq!(conn.read(eid, 1, 3, 2, READ_TO_END).await, (CODE_OK, 10, b"3456789".to_vec()), "length ignored");
    // Only a to-end read may leave the length open, and only known modes are read.
    assert_eq!(conn.read(eid, 1, 2, 0, READ_EXACT).await.0, CODE_ERROR, "an exact read of nothing");
    assert_eq!(conn.read(eid, 1, 2, 0, READ_UP_TO).await.0, CODE_ERROR);
    assert_eq!(conn.read(eid, 1, 2, 4, 9).await.0, CODE_ERROR, "an unknown mode");
}

/// An append's payload travels outside the frame CRC on both transports; the
/// node checks it against the crc32c in ctrl and refuses a mismatch, writing
/// nothing. Over TCP, where the payload used to sit inside ctrl.
#[compio::test]
async fn an_append_whose_payload_misses_its_crc_is_refused() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);
    let eid = 4003u64;
    assert_eq!(conn.alloc_extent(eid).await.code, CODE_OK);

    let client = autumn_rpc::client::RpcClient::connect(addr).await.expect("connect");
    let body = bytes::Bytes::from(vec![7u8; 4096]);
    let wrong = crc32c::crc32c(&body) ^ 1;
    let ctrl = vec![autumn_stream::extent_rpc::rkyv_encode(&autumn_stream::extent_rpc::AppendHeader {
        extent_id: eid,
        eversion: 1,
        commit: 0,
        owner_epoch: 1,
        payload_crc: wrong,
    })];
    let err = client
        .call_vectored_bulk(autumn_stream::extent_rpc::MSG_APPEND, ctrl, body.clone())
        .await
        .expect_err("a payload that misses its crc was accepted");
    assert!(err.to_string().contains("crc"), "{err}");

    // Nothing was written: the good payload goes in at offset 0.
    let appended = conn.append(eid, 1, 0, 1, body.to_vec()).await;
    assert_eq!((appended.code, appended.offset), (CODE_OK, 0));
}

/// An EC stripe is the `WRITE_SHARD` frame's value, outside the frame CRC: the
/// node checks it against the crc32c in the header and refuses a mismatch,
/// staging nothing.
#[compio::test]
async fn a_stripe_that_misses_its_crc_is_refused() {
    use autumn_stream::extent_rpc::{rkyv_encode, WriteShardHeader, MSG_WRITE_SHARD};
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);
    let eid = 4004u64;
    assert_eq!(conn.alloc_extent(eid).await.code, CODE_OK);
    assert_eq!(conn.append(eid, 1, 0, 1, vec![3u8; 8192]).await.code, CODE_OK);

    let client = autumn_rpc::client::RpcClient::connect(addr).await.expect("connect");
    let stripe = bytes::Bytes::from(vec![9u8; 4096]);
    let header = |payload_crc| WriteShardHeader {
        extent_id: eid,
        shard_index: 1,
        sealed_length: 8192,
        shard_len: 4096,
        eversion: 2,
        owner_epoch: 0,
        shard_offset: 0,
        attempt_nonce: 7,
        payload_crc,
    };
    let err = client
        .call_vectored_bulk(MSG_WRITE_SHARD, vec![rkyv_encode(&header(crc32c::crc32c(&stripe) ^ 1))], stripe.clone())
        .await
        .expect_err("a stripe that misses its crc was staged");
    assert!(err.to_string().contains("crc"), "{err}");
    let staged = std::fs::read_dir(node_dir.path())
        .unwrap()
        .flatten()
        .filter(|d| d.path().is_dir())
        .flat_map(|d| std::fs::read_dir(d.path()).unwrap().flatten())
        .any(|f| f.file_name().to_string_lossy() == format!("extent-{eid}.shard1"));
    assert!(!staged, "nothing staged");
    // The same stripe with its crc is staged.
    let ok = client
        .call_vectored_bulk(MSG_WRITE_SHARD, vec![rkyv_encode(&header(crc32c::crc32c(&stripe)))], stripe)
        .await
        .expect("the stripe with its crc");
    assert_eq!(autumn_stream::extent_rpc::WriteShardResp::decode(ok).unwrap().code, CODE_OK);
}
