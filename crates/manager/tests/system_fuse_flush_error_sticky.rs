//! BUG-FUSE-FLUSH-ERROR-EATEN-BY-LOGGERS — a caller that only LOGS a flush
//! failure must not consume the inode's sticky record.
//!
//! `flush_error` is errseq_t-shaped: one report per failure. That only works if
//! the report reaches someone who acts on it. Three callers take a failure and
//! only `tracing::warn!` it — `periodic_sync` (every 30 s), `Destroy`, and a
//! revoked release. Pre-fix they consumed the record, so the application's next
//! fsync found nothing pending, persisted a size covering the missing bytes,
//! and answered SUCCESS over a hole — exactly what the record exists to stop.
//!
//! WHAT THIS ASSERTS, and why not the literal acceptance text. Asserting "the
//! app's fsync still errors" does NOT discriminate here: with the PS dead that
//! fsync errors pre-fix too, just for a different reason (its own put fails).
//! Both sides would be red and the test would prove nothing. So it asserts the
//! property the fix actually changes — whether the sticky record SURVIVES a
//! logging-only caller — plus the symmetric half, that a reporting caller does
//! consume it.
//!
//! PRE-FIX: red at the survival assertion.  POST-FIX: green.

mod support;

use std::time::Duration;

use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;

use autumn_fuse::schema::WRITE_BUF_CAP;
use autumn_fuse::state::FsState;
use autumn_fuse::{dispatch, meta, read, write};

use support::*;

#[test]
#[ignore]
fn a_logging_only_caller_must_not_eat_the_sticky_flush_error() {
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
            register_two_nodes(&mgr, n1_addr, n2_addr, 139).await;
            let (log, row, meta_s) = create_three_streams(&mgr).await;
            upsert_partition(&mgr, 13901, log, row, meta_s, b"", b"\xff\xff\xff\xff").await;
        });
        start_partition_server_killable(139, mgr_addr, ps_addr)
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
        // The sticky record only arises on the mount's pipelined path, which is
        // the only front-end with the dispatcher drain that makes it safe.
        state.pipelined_writes = true;

        let ino = 800u64;
        let m = meta::new_file_meta(0o644, 0, 0);
        meta::put_inode(&mut state, ino, &m).await.expect("put_inode");

        // Warm the extent cache with one SUCCESSFUL flush. Without this the
        // next flush fails while PLANNING (the extent-map scan needs the PS),
        // so no flush is ever spawned and no record is ever set.
        let warm = vec![0xCDu8; WRITE_BUF_CAP];
        write::write(&mut state, ino, 0, &warm).await.expect("warm write");
        write::flush_inode(&mut state, ino, write::FlushReport::ToApplication)
            .await
            .expect("warm flush");

        // From here every extent put fails.
        ps.kill();

        // A contiguous append fills the buffer, plans from the WARM cache, and
        // spawns a flush whose puts cannot land.
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

        // periodic_sync / Destroy / revoked-release shape: it only logs.
        let _ = write::flush_inode(&mut state, ino, write::FlushReport::BestEffort).await;

        // THE ASSERTION: the record must survive a caller that only logs.
        assert!(
            state.inodes.get(&ino).and_then(|is| is.flush_error.as_ref()).is_some(),
            "a logging-only flush consumed the sticky record — the application's \
             next fsync would find nothing pending and answer success over a hole"
        );

        // The symmetric half: a caller that DOES report consumes it, so one
        // failure is reported exactly once rather than for ever.
        let _ = write::flush_inode(&mut state, ino, write::FlushReport::ToApplication).await;
        assert!(
            state.inodes.get(&ino).and_then(|is| is.flush_error.as_ref()).is_none(),
            "a reporting flush must consume the record (errseq_t: one report per failure)"
        );
    });
}

/// The third door onto the same hole: the READ-AFTER-WRITE BARRIER.
///
/// `read::prepare` flushes the write buffer when it overlaps the read range.
/// That flush hands its caller an error, so classifying it `ToApplication` — as
/// the first cut did — looks defensible. It is not, and Linux says why: errseq
/// retires a writeback error only at fsync/close/msync, never at a read. An
/// application reads back what it just wrote, gets EIO, retries the read (which
/// now succeeds), and closes; if the barrier consumed the record, that close's
/// fsync finds nothing pending and publishes a size covering bytes that never
/// landed — SUCCESS over a hole, the exact outcome the record exists to stop.
///
/// The read still fails either way. Only the record's survival differs, which
/// is what this pins.
///
/// PRE-FIX (barrier classified `ToApplication`): red at the survival assertion.
#[test]
#[ignore]
fn the_read_after_write_barrier_must_not_eat_the_sticky_flush_error() {
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
            register_two_nodes(&mgr, n1_addr, n2_addr, 143).await;
            let (log, row, meta_s) = create_three_streams(&mgr).await;
            upsert_partition(&mgr, 14301, log, row, meta_s, b"", b"\xff\xff\xff\xff").await;
        });
        start_partition_server_killable(143, mgr_addr, ps_addr)
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

        let ino = 1000u64;
        let m = meta::new_file_meta(0o644, 0, 0);
        meta::put_inode(&mut state, ino, &m).await.expect("put_inode");

        let warm = vec![0xCDu8; WRITE_BUF_CAP];
        write::write(&mut state, ino, 0, &warm).await.expect("warm write");
        write::flush_inode(&mut state, ino, write::FlushReport::ToApplication)
            .await
            .expect("warm flush");

        ps.kill();

        let doomed = vec![0xEFu8; WRITE_BUF_CAP];
        let _ = write::write(&mut state, ino, WRITE_BUF_CAP as i64, &doomed).await;
        let drained = write::drain_pending(&mut state, ino).await;
        assert!(drained.is_err(), "precondition: the spawned flush must fail");
        assert!(
            state.inodes.get(&ino).and_then(|is| is.flush_error.as_ref()).is_some(),
            "precondition: the failure must have been recorded on the inode"
        );

        // Now make the barrier reachable: a SMALL contiguous append leaves a
        // dirty buffer that does not fill (so nothing is spawned) and does not
        // start a gap (so no `clean_beyond_eof`, which would need the dead PS).
        // Take the offset from the inode's own in-memory size so it is
        // contiguous by construction rather than by my arithmetic.
        let tail_off = state.inodes.get(&ino).expect("cached").meta.size;
        let tail = vec![0x5Au8; 4096];
        write::write(&mut state, ino, tail_off as i64, &tail)
            .await
            .expect("small contiguous append should just buffer");
        assert!(
            state
                .inodes
                .get(&ino)
                .and_then(|is| is.write_buf.as_ref())
                .map(|wb| wb.len > 0)
                .unwrap_or(false),
            "precondition: the append must have left a DIRTY buffer for the \
             barrier to trip over"
        );

        // THE TRIGGER: read the range the dirty buffer covers.
        let got = read::read(&mut state, ino, tail_off as i64, 4096).await;
        assert!(
            got.is_err(),
            "precondition: the barrier's flush fails, so the read fails — if it \
             succeeded, this test is not exercising the barrier at all"
        );

        // THE ASSERTION: a read is not where a writeback error retires.
        assert!(
            state.inodes.get(&ino).and_then(|is| is.flush_error.as_ref()).is_some(),
            "the read-after-write barrier consumed the sticky record — the \
             application's next fsync would answer success over the hole"
        );
    });
}

/// The FOURTH and FIFTH doors: the write path's own GAP FLUSH, and `truncate`.
///
/// `write_inner` flushes when the new offset is not contiguous with the buffer
/// (`write.rs`'s `needs_flush`), and `truncate` flushes before it rewrites the
/// size. Both flushes' `?` surfaces as EIO to the caller — which makes
/// `ToApplication` look defensible, and both were classified that way until a
/// second review. It is the same mistake as the read barrier: neither a
/// buffered `write()` nor `ftruncate` is one of Linux's writeback-error
/// retirement points (fsync/close/msync are). The chain is identical, and the
/// retry succeeds because `flush_inode` returns from the record check BEFORE it
/// extracts the buffer or touches the size:
/// seek+write → EIO → app retries the write → succeeds → `close()`'s fsync
/// finds nothing pending → publishes a size covering bytes that never landed.
///
/// PRE-FIX (either site classified `ToApplication`): red at that site's assert.
#[test]
#[ignore]
fn the_write_path_flushes_must_not_eat_the_sticky_flush_error() {
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
            register_two_nodes(&mgr, n1_addr, n2_addr, 145).await;
            let (log, row, meta_s) = create_three_streams(&mgr).await;
            upsert_partition(&mgr, 14501, log, row, meta_s, b"", b"\xff\xff\xff\xff").await;
        });
        start_partition_server_killable(145, mgr_addr, ps_addr)
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

        let ino = 1100u64;
        let m = meta::new_file_meta(0o644, 0, 0);
        meta::put_inode(&mut state, ino, &m).await.expect("put_inode");

        let warm = vec![0xCDu8; WRITE_BUF_CAP];
        write::write(&mut state, ino, 0, &warm).await.expect("warm write");
        write::flush_inode(&mut state, ino, write::FlushReport::ToApplication)
            .await
            .expect("warm flush");

        ps.kill();

        let doomed = vec![0xEFu8; WRITE_BUF_CAP];
        let _ = write::write(&mut state, ino, WRITE_BUF_CAP as i64, &doomed).await;
        let drained = write::drain_pending(&mut state, ino).await;
        assert!(drained.is_err(), "precondition: the spawned flush must fail");
        assert!(
            state.inodes.get(&ino).and_then(|is| is.flush_error.as_ref()).is_some(),
            "precondition: the failure must have been recorded on the inode"
        );

        // Leave a dirty buffer for the gap to be measured against.
        let tail_off = state.inodes.get(&ino).expect("cached").meta.size;
        let tail = vec![0x5Au8; 4096];
        write::write(&mut state, ino, tail_off as i64, &tail)
            .await
            .expect("small contiguous append should just buffer");

        // THE TRIGGER: a BACKWARD seek — non-contiguous with the buffer, and
        // deliberately BELOW EOF so this does not also drag in
        // `clean_beyond_eof`, which would need the dead PS and muddy the cause.
        let stomp = vec![0x77u8; 128];
        let gap = write::write(&mut state, ino, 0, &stomp).await;
        assert!(
            gap.is_err(),
            "precondition: the gap flush fails, so the write fails — if it \
             succeeded this test is not exercising the gap flush at all"
        );

        // THE ASSERTION: a buffered write() is not where a writeback error
        // retires either.
        assert!(
            state.inodes.get(&ino).and_then(|is| is.flush_error.as_ref()).is_some(),
            "the gap flush consumed the sticky record — the application would \
             retry the write, succeed, and have close() answer success over the \
             hole"
        );

        // ── FIFTH door: truncate ─────────────────────────────────────────
        // `truncate` flushes before rewriting the size, and returns from the
        // record check before it gets there.
        //
        // VACUITY, CHECKED: `truncate` calls `ensure_inode_cached` first, and
        // had that failed against the dead PS this assertion would pass without
        // `flush_inode` ever being reached — proving nothing. The ablation
        // settled it: flipping THIS site back to `ToApplication` turns THIS
        // assertion red, at its own line, distinct from the gap assertion
        // above. So truncate does reach the record check, and the assertion is
        // load-bearing rather than decorative.
        let trunc = write::truncate(&mut state, ino, 4096).await;
        assert!(
            trunc.is_err(),
            "precondition: truncate's flush fails, so truncate fails"
        );
        assert!(
            state.inodes.get(&ino).and_then(|is| is.flush_error.as_ref()).is_some(),
            "truncate consumed the sticky record — ftruncate is not a point at \
             which a writeback error retires either"
        );
    });
}
