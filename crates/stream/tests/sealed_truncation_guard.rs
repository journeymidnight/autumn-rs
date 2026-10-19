mod test_helpers;

use autumn_stream::extent_rpc::{CODE_OK, CODE_PRECONDITION};
use test_helpers::{pick_addr, start_node, TestConn};

/// An append with eversion < local eversion must be rejected
/// through the batch path (step 2 sealed/eversion check).
#[compio::test]
async fn batch_path_rejects_stale_eversion() {
    let node_dir = tempfile::tempdir().expect("node tempdir");
    let addr = pick_addr();
    start_node(node_dir.path(), addr).await;
    let conn = TestConn::new(addr);

    let alloc = conn.alloc_extent(5002).await;
    assert_eq!(alloc.code, CODE_OK);

    // Write at eversion=1.
    let w1 = conn.append(5002, 1, 0, 1, b"data".to_vec()).await;
    assert_eq!(w1.code, CODE_OK);

    // Send eversion=2 to trigger eversion refresh (step 1 of build_append_future).
    // Without manager, the refresh fails → we get an error/unavailable response.
    // But eversion check at step 2 should handle: local_eversion=1, req=2 →
    // needs_refresh → manager unavailable → returns unavailable error.
    // After that, sending eversion=0 should get PRECONDITION (local=1 > req=0).
    let stale = conn.append(5002, 0, 4, 1, b"x".to_vec()).await;
    assert_eq!(
        stale.code, CODE_PRECONDITION,
        "eversion 0 < local 1 should be rejected"
    );
}
