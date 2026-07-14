//! BUG-FLUSH-TIMEOUT-LEAK FIX #2 — an abandoned (rolled-away, zero-acked)
//! tail extent must be reclaimable, not a permanent leak.
//!
//! Live-cluster shape being reproduced: a bulk append fails (there: 5 s
//! timeout on a 256 MiB SST; here: the tail replica dies), the writer's
//! retry path runs the SealCommit handshake → worker reports commit=0
//! (nothing on this tail was ever all-replica-acked) →
//! `alloc_new_extent(stream, Some(0), tail)` seals the abandoned tail at
//! sealed_length=0 and rolls to a fresh extent. Pre-fix that extent stayed
//! FOREVER: still a member of the stream (refs=1), invisible to accounting
//! (`sealed_length=0` contributes nothing to logical size), skipped by
//! log-GC's authoritative-sealed gate on other streams, and unreachable by
//! row-stream `truncate` (which only advances past extents holding live
//! SSTs — an abandoned extent holds none). On the live cluster this leaked
//! 10.4 TB (≈40k × 255 MB extents full of real-but-un-acked bytes) for
//! 222 GB of logical data.
//!
//! The test kills the EN hosting a fresh empty tail, appends (forcing the
//! error → SealCommit(0) → roll path), and asserts the abandoned extent is
//! REMOVED from the stream (punched → refs=0 → physical delete queued),
//! while the previous extent — sealed with acked data — is preserved.

mod support;

use std::rc::Rc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_stream::{ConnPool, StreamClient};
use support::*;

/// Return true iff `dir` (hashed `{base}/{hh}/` layout) holds extent-{id}.dat.
fn has_dat(dir: &std::path::Path, extent_id: u64) -> bool {
    let name = format!("extent-{extent_id}.dat");
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if !p.is_dir() {
            continue;
        }
        if let Ok(files) = std::fs::read_dir(&p) {
            for f in files.flatten() {
                if f.file_name().to_str() == Some(name.as_str()) {
                    return true;
                }
            }
        }
    }
    false
}

#[test]
fn abandoned_empty_rolled_tail_is_reclaimed() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let (n1_flag, n1_handle) =
        start_extent_node_stoppable(n1_addr, n1_dir.path().to_path_buf(), 8300);
    let (n2_flag, n2_handle) =
        start_extent_node_stoppable(n2_addr, n2_dir.path().to_path_buf(), 8301);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_node(&mgr, &n1_addr.to_string(), "uuid-leak-1").await;
        register_node(&mgr, &n2_addr.to_string(), "uuid-leak-2").await;
        // RF=1 so killing ONE node makes the tail unreachable deterministically.
        let stream_id = create_stream(&mgr, 1).await;
        drop(mgr);

        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "bug-flush-timeout-leak/owner".to_string(),
            1024, // tiny max_extent_size → first append triggers a preemptive roll
            pool,
        )
        .await
        .expect("StreamClient::connect");

        // Append 1: succeeds, end (1500) ≥ max_extent_size → preemptive roll:
        // E1 sealed at 1500 (Some(end>0) — holds acked data, must be KEPT),
        // fresh EMPTY tail E2 allocated; worker now on E2 with commit=0.
        let payload = vec![0xcd_u8; 1500];
        let r1 = sc.append(stream_id, &payload).await.expect("first append");
        let e1 = r1.extent_id;

        let info = sc.get_stream_info(stream_id).await.expect("stream_info");
        assert_eq!(
            info.extent_ids.len(),
            2,
            "expected [E1, fresh tail E2], got {:?}",
            info.extent_ids
        );
        let e2 = *info.extent_ids.last().expect("tail");
        assert_ne!(e1, e2, "preemptive roll should have allocated a fresh tail");

        // Kill the EN hosting the fresh tail E2 (alloc_extent created its
        // empty .dat at roll time, so the file tells us where it lives).
        let e2_on_n1 = has_dat(n1_dir.path(), e2);
        let e2_on_n2 = has_dat(n2_dir.path(), e2);
        assert!(
            e2_on_n1 ^ e2_on_n2,
            "RF=1 extent {e2} must live on exactly one node (n1={e2_on_n1}, n2={e2_on_n2})"
        );
        let mut n1_handle = Some(n1_handle);
        let mut n2_handle = Some(n2_handle);
        // Join deterministically; give in-flight sockets a beat to close.
        if e2_on_n1 {
            n1_flag.shutdown();
            n1_handle.take().unwrap().join().expect("join n1");
        } else {
            n2_flag.shutdown();
            n2_handle.take().unwrap().join().expect("join n2");
        }
        compio::time::sleep(Duration::from_millis(300)).await;

        // Append 2: the dead tail forces error → soft retries → hard path →
        // SealCommit handshake reports (commit=0, E2) →
        // alloc_new_extent(stream, Some(0), E2): manager seals E2 at
        // sealed_length=0 and rolls a fresh tail E3 on the surviving node.
        let r2 = sc
            .append(stream_id, &payload)
            .await
            .expect("append after tail-node death must roll to a fresh tail and succeed");
        assert_ne!(r2.extent_id, e2, "retry must land on a fresh tail, not the dead one");

        // THE BUG (red pre-fix): E2 — sealed EMPTY (not one byte was ever
        // all-replica-acked on it, so no VP / SST / checkpoint can reference
        // it) — stayed a member of the stream forever: not the tail, holds
        // no live tables (row-stream truncate never covers it), sealed_length
        // 0 (invisible to all accounting), refs=1 (never physically deleted).
        // FIX #2: the roll-away path must reclaim it (punch → refs=0).
        let info2 = sc.get_stream_info(stream_id).await.expect("stream_info 2");
        assert!(
            !info2.extent_ids.contains(&e2),
            "abandoned empty tail {e2} leaked: still a stream member ({:?}) — \
             sealed_length=0, not the tail, referenced by nothing, reclaimed by nothing",
            info2.extent_ids
        );
        // E1 was sealed at 1500 with acked data — it must NEVER be touched
        // by the abandoned-tail reclaim (only zero-acked seals qualify).
        assert!(
            info2.extent_ids.contains(&e1),
            "extent {e1} holds acked data and must be preserved ({:?})",
            info2.extent_ids
        );
        // And the extent record itself must be gone (refs → 0 → deleted).
        assert!(
            sc.get_extent_info(e2).await.is_err(),
            "extent {e2} record should be deleted once punched to refs=0"
        );

        // Cleanup: stop the surviving node.
        n1_flag.shutdown();
        n2_flag.shutdown();
        if let Some(h) = n1_handle.take() {
            h.join().expect("join n1");
        }
        if let Some(h) = n2_handle.take() {
            h.join().expect("join n2");
        }
    });
}
