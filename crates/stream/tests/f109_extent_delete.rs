//! F109 — extent-node DelExtent handler.
//!
//! Single-node integration coverage:
//! 1. Existing extent: handler returns CODE_OK and unlinks both
//!    `extent-{id}.dat` and `extent-{id}.meta`.
//! 2. Missing extent: handler is idempotent — returns CODE_OK with no
//!    error so the manager's retry loop is safe.
//! 3. Delete-then-append: a fresh append on the deleted extent_id sees
//!    no leftover state (re-creates the file).

mod test_helpers;

use std::time::Duration;

use autumn_stream::extent_rpc::CODE_OK;
use test_helpers::{pick_addr, start_node, TestConn};

#[compio::test]
async fn delete_existing_extent_unlinks_files() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let addr = pick_addr();
    start_node(tmp.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 42;
    let alloc = conn.alloc_extent(extent_id).await;
    assert_eq!(alloc.code, CODE_OK, "alloc failed: {}", alloc.message);

    // Write a payload so the .dat file has bytes; meta is created at
    // alloc time. After delete both files must be gone.
    let payload = vec![0xab; 4096];
    let resp = conn.append(extent_id, 1, 0, 10, false, payload).await;
    assert_eq!(resp.code, CODE_OK, "append failed");

    // Sanity: files exist before delete.
    let hash_byte = (crc32c::crc32c(&extent_id.to_le_bytes()) & 0xFF) as u8;
    let hash_dir = tmp.path().join(format!("{:02x}", hash_byte));
    let dat = hash_dir.join(format!("extent-{extent_id}.dat"));
    let meta = hash_dir.join(format!("extent-{extent_id}.meta"));
    assert!(dat.exists(), "dat must exist before delete: {}", dat.display());
    assert!(meta.exists(), "meta must exist before delete: {}", meta.display());

    let resp = conn.delete_extent(extent_id).await;
    assert_eq!(resp.code, CODE_OK, "delete failed: {}", resp.message);

    // Allow the io completion to settle (compio fs ops are
    // completion-based; the kernel may take a moment to surface the
    // unlink to the directory entry on some platforms).
    compio::time::sleep(Duration::from_millis(50)).await;
    assert!(!dat.exists(), "dat must be unlinked after delete");
    assert!(!meta.exists(), "meta must be unlinked after delete");
}

#[compio::test]
async fn delete_missing_extent_is_idempotent() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let addr = pick_addr();
    start_node(tmp.path(), addr).await;
    let conn = TestConn::new(addr);

    // Delete an extent_id the node has never seen; idempotent contract
    // says CODE_OK with empty message so manager retries are safe.
    let resp = conn.delete_extent(99_999).await;
    assert_eq!(
        resp.code, CODE_OK,
        "delete on missing extent should be CODE_OK: {}",
        resp.message
    );

    // Second delete on the same id: still CODE_OK.
    let resp = conn.delete_extent(99_999).await;
    assert_eq!(resp.code, CODE_OK, "repeated delete should be CODE_OK");
}

#[compio::test]
async fn delete_then_realloc_starts_fresh() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let addr = pick_addr();
    start_node(tmp.path(), addr).await;
    let conn = TestConn::new(addr);

    let extent_id: u64 = 7;
    let alloc = conn.alloc_extent(extent_id).await;
    assert_eq!(alloc.code, CODE_OK);
    let resp = conn.append(extent_id, 1, 0, 10, false, vec![0x11; 1024]).await;
    assert_eq!(resp.code, CODE_OK);

    let resp = conn.delete_extent(extent_id).await;
    assert_eq!(resp.code, CODE_OK);

    compio::time::sleep(Duration::from_millis(50)).await;

    // Re-alloc the same id: should succeed (file gone, fresh entry).
    let alloc = conn.alloc_extent(extent_id).await;
    assert_eq!(alloc.code, CODE_OK, "re-alloc after delete failed: {}", alloc.message);

    // Fresh append at offset 0 — we use `revision=20 >= 10` to satisfy
    // the fencing check (the .meta sidecar was unlinked, so
    // last_revision is reset to 0; any positive revision is fine).
    let resp = conn.append(extent_id, 1, 0, 20, false, vec![0x22; 512]).await;
    assert_eq!(resp.code, CODE_OK, "append after re-alloc failed");
    assert_eq!(resp.offset, 0, "fresh extent must start at offset 0");
    assert_eq!(resp.end, 512);
}
