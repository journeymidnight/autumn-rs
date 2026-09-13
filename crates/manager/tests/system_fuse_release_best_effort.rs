//! RELEASE must not consume the inode's sticky `flush_error`, even when it is
//! the NON-revoked release that answers the kernel with EIO.
//!
//! This is the half the first fix got wrong. `flush_error` is errseq_t-shaped —
//! one report per failure — so only a caller that can actually TELL the
//! application may consume it. The first cut keyed that on
//! `ReleaseAction::propagate_flush_err` (= NOT revoked) on the reasoning that a
//! release which returns EIO has reported. It has not: fuser's release contract
//! says "the filesystem may reply with an error, but error values are not
//! returned to close() or munmap() which triggered the release". The kernel
//! drops the reply, so a non-revoked release tells nobody — and consuming there
//! leaves the application's next fsync with nothing pending, free to persist a
//! size covering bytes that never landed and answer SUCCESS over the hole.
//!
//! Nor may it lean on "FLUSH always runs first". FUSE_FLUSH is the caller that
//! CAN deliver the error (per fuser, one reason flush exists is "if the
//! filesystem wants to return write errors"), but the same doc warns filesystems
//! "shouldn't assume that flush will always be called after some writes, or that
//! it will be called at all".
//!
//! Driven through `dispatch::handle_request` with a real `FsRequest::Release`,
//! not through `flush_inode` directly, because the classification under test is
//! the dispatcher's. No lease is held, which is exactly the non-revoked shape:
//! `compute_release_action` answers `propagate_flush_err: true` there.
//!
//! PRE-FIX: red at the survival assertion (Release consumed the record).
//! POST-FIX: green.

mod support;

use std::time::Duration;

use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;

use autumn_fuse::bridge::{self, FsRequest};
use autumn_fuse::schema::WRITE_BUF_CAP;
use autumn_fuse::state::FsState;
use autumn_fuse::{dispatch, meta, write};

use support::*;

#[test]
#[ignore]
fn release_must_not_consume_the_sticky_flush_error() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    let ps_addr = pick_addr();
    let mut ps = {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mgr = RpcClient::connect(mgr_addr).await.unwrap();
            register_two_nodes(&mgr, n1_addr, n2_addr, 141).await;
            let (log, row, meta_s) = create_three_streams(&mgr).await;
            upsert_partition(&mgr, 14101, log, row, meta_s, b"", b"\xff\xff\xff\xff").await;
        });
        start_partition_server_killable(141, mgr_addr, ps_addr)
    };

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster = ClusterClient::connect_raw(&mgr_addr.to_string())
            .await
            .expect("admin client");
        cluster.set_rpc_timeout(Duration::from_secs(30));

        let mut state = FsState::new(&mgr_addr.to_string())
            .await
            .expect("FsState::new");
        dispatch::init_root(&mut state).await.expect("init_root");
        state.pipelined_writes = true;

        let ino = 900u64;
        let m = meta::new_file_meta(0o644, 0, 0);
        meta::put_inode(&mut state, ino, &m).await.expect("put_inode");

        // Warm the extent cache with one SUCCESSFUL flush, or the next flush
        // fails while PLANNING (the map scan needs the PS) and none is spawned.
        let warm = vec![0xCDu8; WRITE_BUF_CAP];
        write::write(&mut state, ino, 0, &warm).await.expect("warm write");
        write::flush_inode(&mut state, ino, write::FlushReport::ToApplication)
            .await
            .expect("warm flush");

        ps.kill();

        let doomed = vec![0xEFu8; WRITE_BUF_CAP];
        let _ = write::write(&mut state, ino, WRITE_BUF_CAP as i64, &doomed).await;
        let drained = write::drain_pending(&mut state, ino).await;
        assert!(
            drained.is_err(),
            "precondition: with the PS dead the spawned flush must fail"
        );
        assert!(
            state.inodes.get(&ino).and_then(|is| is.flush_error.as_ref()).is_some(),
            "precondition: the failure must have been recorded on the inode"
        );

        // THE DRIVE: a real RELEASE with the kernel's flush bit set and NO lease
        // held — `compute_release_action` gives `must_flush: true` and
        // `propagate_flush_err: true`, the arm the first fix classified as
        // "reports, so may consume".
        // Stand in for the Open this test does not drive. ONE, not two: the
        // decrement then reaches 0, so the eviction guard below is actually
        // evaluated instead of short-circuiting on `open_count == 0`. And it is
        // still non-vacuous in the other direction — a skipped decrement leaves
        // 1, which is not 0.
        state.inodes.get_mut(&ino).expect("cached").open_count = 1;

        let (tx, rx) = bridge::reply_channel::<()>();
        let keep_going = dispatch::handle_request(
            &mut state,
            FsRequest::Release {
                ino,
                flush: true,
                reply: tx,
            },
            None,
        )
        .await;
        assert!(keep_going, "only Destroy ends the loop");

        // It DOES answer EIO — BestEffort means "may not consume", not "swallow".
        let replied = rx.recv().expect("release replied");
        assert!(
            replied.is_err(),
            "a non-revoked release still answers the kernel with the error"
        );

        // THE ASSERTION: and the record is still standing, because that reply
        // never reaches the caller of close().
        assert!(
            state.inodes.get(&ino).and_then(|is| is.flush_error.as_ref()).is_some(),
            "RELEASE consumed the sticky record — but fuser drops a release \
             error, so nobody was told, and the application's next fsync would \
             answer success over the hole"
        );

        // Teardown must still have run. Reporting the error is NOT a licence to
        // return from the middle of the Release arm: that would skip the
        // `open_count` decrement, the eviction check, the `held_leases`
        // refcount and `lease::release`, and fuser sends exactly one release
        // per open, so nothing would ever come back to finish them.
        //
        // COVERAGE BOUNDARY, so nobody reads more into this than it proves:
        // this test holds NO lease, so `compute_release_action` takes its
        // `None` arm, where `must_drop_entry` is always false and
        // `lease::release` is never reached. Only the `open_count` half of the
        // teardown is pinned here. The refcount/lease half — the one that
        // wedges a writer for the life of the mount — has no test.
        assert_eq!(
            state.inodes.get(&ino).map(|is| is.open_count),
            Some(0),
            "Release returned the error without decrementing open_count — the \
             teardown below the flush was skipped, and no second release comes"
        );

        // And the inode must still be cached for the record to mean anything.
        // With `open_count` now 0 and `lookup_count` 0, `!dirty` is the ONLY
        // conjunct left standing between the record and eviction — the failed
        // flush left the inode dirty, which is what keeps it.
        assert!(
            state.inodes.get(&ino).map(|is| is.dirty).unwrap_or(false),
            "the inode went CLEAN on release, so only `!dirty` stood between \
             the record and eviction — and it no longer does (eviction itself \
             is ruled out: the assert above already found the inode present)"
        );
    });
}
