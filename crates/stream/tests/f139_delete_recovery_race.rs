//! F139 — delete_extent must be refused while recovery is in flight for the
//! same extent_id.
//!
//! Covers the extent-node side of the delete-vs-recovery race:
//! - `handle_delete_extent` returns `CODE_PRECONDITION` when
//!   `recovery_inflight.contains_key(&extent_id)`.
//! - Once recovery clears, the same delete succeeds with `CODE_OK`.

mod test_helpers;

use std::time::Duration;

use autumn_stream::extent_rpc::{RecoveryTask, CODE_OK, CODE_PRECONDITION};
use autumn_stream::{ExtentNode, ExtentNodeConfig};
use test_helpers::{pick_addr, TestConn};

#[compio::test]
async fn f139_delete_extent_refused_during_recovery() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let addr = pick_addr();

    let config = ExtentNodeConfig::new(tmp.path().to_path_buf(), 1);
    let node = ExtentNode::new(config).await.expect("create ExtentNode");

    // Clone the Rc<DashMap> before moving the node into the serve task.
    // Both handles share the same underlying map (same compio thread, Rc safe).
    let ri = node.clone_recovery_inflight();

    compio::runtime::spawn(async move {
        let _ = node.serve(addr).await;
    })
    .detach();
    compio::time::sleep(Duration::from_millis(120)).await;

    let conn = TestConn::new(addr);
    let extent_id: u64 = 1001;

    // Alloc the extent so the node has an in-memory entry to guard.
    let alloc = conn.alloc_extent(extent_id).await;
    assert_eq!(alloc.code, CODE_OK, "alloc failed: {}", alloc.message);

    // Inject a recovery_inflight entry (mirrors what handle_require_recovery
    // does when a real recovery task is dispatched from the manager).
    ri.insert(
        extent_id,
        RecoveryTask {
            extent_id,
            replace_id: 1,
            node_id: 9,
            start_time: 0,
        },
    );

    // Delete must be refused while recovery is in flight.
    let resp = conn.delete_extent(extent_id).await;
    assert_eq!(
        resp.code, CODE_PRECONDITION,
        "F139: delete must return CODE_PRECONDITION when recovery in flight; \
         got code={} msg={}",
        resp.code, resp.message
    );

    // After recovery completes (entry cleared), the same delete must succeed.
    ri.remove(&extent_id);
    let resp = conn.delete_extent(extent_id).await;
    assert_eq!(
        resp.code, CODE_OK,
        "F139: delete must succeed after recovery_inflight cleared: {}",
        resp.message
    );
}
