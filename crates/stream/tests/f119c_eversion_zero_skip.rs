//! F119-C regression: a stale-cached `eversion=0` must NOT silently bypass
//! the server-side eversion check on a sealed/EC-converted extent.
//!
//! Pre-fix: `handle_read_bytes` and the batched `build_read_future` both
//! gated their reject path on `req.eversion > 0 && req.eversion < ev`.
//! After split + EC conversion bumped `entry.eversion` from 0 to N>0
//! and shrunk the on-disk file to `shard_size`, a partition server
//! holding a stale `ExtentInfo` from when the extent was open
//! (eversion=0, ec_converted=false, populated by
//! `load_stream_tail` / `alloc_new_extent_once`) would route through
//! `read_replicated_with_failover`, send `eversion=0`, and silently
//! receive `min(req.length, shard_size - offset)` bytes. For a value
//! straddling shards this surfaced upstream as
//! `logStream value short: need N bytes, got M`.
//!
//! Post-fix: the server enforces `req.eversion < ev` unconditionally.
//! The client's existing 2-attempt retry loop in
//! `read_bytes_from_extent` then evicts the cache, refetches
//! `ExtentInfo`, and re-routes through `ec_subrange_read`.

mod test_helpers;

use autumn_stream::extent_rpc::{CODE_EVERSION_MISMATCH, CODE_OK};
use test_helpers::{pick_addr, start_node, TestConn};

/// Single-frame read path (`handle_read_bytes` via `dispatch`).
/// Reproduces the user's "logStream value short" error shape: requested
/// length crosses past the shard's truncated `entry.len`, so without
/// the eversion check the server would return a short OK response.
#[compio::test]
async fn read_bytes_rejects_eversion_zero_after_post_seal_bump() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 7777;
    let alloc = conn.alloc_extent(extent_id).await;
    assert_eq!(alloc.code, CODE_OK);

    // Pretend we're the EC coordinator's 2PC: WriteShard (prepare) writes
    // the shard to .ec.dat, then CommitEcShard (commit) renames it to
    // .dat and bumps eversion from 0 to 5, shrinking the on-disk file
    // to the shard payload (mimicking post-EC state where a value at
    // offset > shard_len no longer fits).
    let shard_payload = vec![0xABu8; 1024];
    let ws = conn
        .write_shard(extent_id, 0, 1024, 5, shard_payload)
        .await;
    assert_eq!(ws.code, CODE_OK, "write_shard should succeed");
    let cs = conn.commit_ec_shard(extent_id, 1024, 5).await;
    assert_eq!(cs.code, CODE_OK, "commit_ec_shard should succeed");

    // A client with stale ExtentInfo (eversion=0 cached when the extent
    // was open) attempts to read past the shard boundary. Pre-fix the
    // server returned CODE_OK with a short payload, since
    // `req.eversion=0` skipped the check. Post-fix it must return
    // CODE_EVERSION_MISMATCH so the client refreshes its cache.
    let resp = conn
        .read_bytes(extent_id, /* eversion */ 0, /* offset */ 1000, /* length */ 200)
        .await;
    assert_eq!(
        resp.code, CODE_EVERSION_MISMATCH,
        "stale eversion=0 against ev>0 must reject (was silently truncated pre-fix)"
    );
    assert!(resp.payload.is_empty(), "mismatch response carries no payload");

    // Sanity: the matching eversion (or higher) still succeeds.
    let ok = conn.read_bytes(extent_id, 5, 0, 1024).await;
    assert_eq!(ok.code, CODE_OK);
    assert_eq!(ok.payload.len(), 1024);
}

/// Batched read path (`build_read_future` in `handle_connection`'s
/// MSG_READ_BYTES grouping). Pre-fix this returned a frame-level
/// `FailedPrecondition` error which never reached the client's
/// `is_eversion_stale` retry detection — so the cache never refreshed
/// even for callers that did pass a non-zero (stale) eversion.
#[compio::test]
async fn batched_read_returns_eversion_mismatch_response() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 7778;
    assert_eq!(conn.alloc_extent(extent_id).await.code, CODE_OK);

    // 2PC: prepare (write .ec.dat) + commit (rename + bump eversion to 7).
    let ws = conn
        .write_shard(extent_id, 0, 1024, 7, vec![0xCDu8; 1024])
        .await;
    assert_eq!(ws.code, CODE_OK);
    let cs = conn.commit_ec_shard(extent_id, 1024, 7).await;
    assert_eq!(cs.code, CODE_OK);

    // Stale non-zero eversion — pre-fix this hit the batched path's
    // FailedPrecondition error frame, bypassing the client's
    // `resp.code == CODE_EVERSION_MISMATCH` retry trigger.
    let resp = conn.read_bytes(extent_id, /* stale */ 3, 0, 1024).await;
    assert_eq!(
        resp.code, CODE_EVERSION_MISMATCH,
        "batched path must surface mismatch as a typed response, not a frame error"
    );

    // Stale zero — same story, plus the extra coverage that the
    // `req.eversion > 0` skip is gone.
    let resp = conn.read_bytes(extent_id, 0, 0, 1024).await;
    assert_eq!(resp.code, CODE_EVERSION_MISMATCH);
}
