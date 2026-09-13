//! BUG-FUSE-EOF-READ-CLOBBERS-DIRTY-META — a read at EOF must not overwrite
//! in-memory meta that holds UNPUBLISHED writes.
//!
//! `read::prepare` confirms EOF against KV before reporting it (that check is
//! correct and stays: this clamp is the only path that can answer a FUSE read
//! with fewer bytes than asked, so a stale-SMALL cache would silently truncate
//! a live file). The defect was the DIRECTION: `get_inode_uncached` adopted the
//! KV copy on any difference, and a cache LARGER than KV is not staleness — it
//! is the normal state of a file being written through the mount, where
//! extents land long before the size covering them is published.
//!
//! Adopting the smaller size there does not merely date the answer. The next
//! write sees `cur_size < offset`, runs `clean_beyond_eof` and DELETES every
//! extent already landed; with no further write, `flush_inode` publishes the
//! shrunken size and the file closes at 0.
//!
//! Driven directly through `FsState` — no kernel mount needed, because the
//! clobber happens in the daemon's own cache, below the FUSE boundary.
//!
//! PRE-FIX: red (final size 0 / read comes back empty).  POST-FIX: green.

mod support;

use std::time::Duration;

use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;

use autumn_fuse::schema::{self, WRITE_BUF_CAP};
use autumn_fuse::state::FsState;
use autumn_fuse::{dispatch, key, meta, read, write};

use support::*;

async fn boot_cluster(
    mgr_addr: std::net::SocketAddr,
    n1_addr: std::net::SocketAddr,
    n2_addr: std::net::SocketAddr,
    base: u16,
    part_id: u64,
) -> ClusterClient {
    let mgr = RpcClient::connect(mgr_addr).await.unwrap();
    register_two_nodes(&mgr, n1_addr, n2_addr, base).await;
    let (log, row, meta) = create_three_streams(&mgr).await;
    upsert_partition(&mgr, part_id, log, row, meta, b"", b"\xff\xff\xff\xff").await;
    let ps_addr = pick_addr();
    start_partition_server(base as u64, mgr_addr, ps_addr);
    compio::time::sleep(Duration::from_millis(1500)).await;
    let _ = RpcClient::connect(ps_addr).await.unwrap();
    let cluster = ClusterClient::connect_raw(&mgr_addr.to_string())
        .await
        .expect("ClusterClient::connect");
    cluster.set_rpc_timeout(Duration::from_secs(30));
    cluster
}

#[test]
#[ignore]
fn eof_read_must_not_clobber_unpublished_size() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot_cluster(mgr_addr, n1_addr, n2_addr, 137, 13701).await;

        let mut state = FsState::new(&mgr_addr.to_string())
            .await
            .expect("FsState::new");
        dispatch::init_root(&mut state).await.expect("init_root");

        let ino = 700u64;
        let m = meta::new_file_meta(0o644, 0, 0);
        meta::put_inode(&mut state, ino, &m).await.expect("put_inode");

        // Exactly one full buffer: the batch FLUSHES (extents land) while the
        // size covering them stays unpublished, because nothing fsyncs. This
        // is the ordinary mid-write state, not a contrived one.
        let total = WRITE_BUF_CAP;
        let data = vec![0xABu8; total];
        let n = write::write(&mut state, ino, 0, &data).await.expect("write");
        assert_eq!(n as usize, total, "write returned full length");

        // Extents are on disk...
        let prefix = key::extent_prefix(ino);
        let landed = state
            .kv_range_keys(&prefix, &prefix, 4096)
            .await
            .expect("range");
        assert!(!landed.is_empty(), "precondition: a batch must have landed");

        // ...while KV's inode still says 0. The cache is legitimately LARGER.
        //
        // Read the inode key STRAIGHT from KV rather than through
        // `get_inode_uncached`: that function is the one under test, and
        // pre-fix it does not merely report — it ADOPTS, which is the very
        // clobber the assertions below are about. Establishing the
        // precondition with it would fire the trigger early and the test would
        // be measuring its own setup.
        let raw = state
            .kv_get(&key::inode_key(ino))
            .await
            .expect("read the inode key straight from KV");
        let kv_meta = schema::decode_inode_meta(&raw).expect("decode inode meta");
        assert_eq!(
            kv_meta.size, 0,
            "precondition: the size must still be unpublished"
        );

        // THE TRIGGER: a read at EOF. Pre-fix this adopted KV's 0 and wiped the
        // cached 64 MiB — `tail -f` and any reader after an exact-multiple
        // write does precisely this.
        let at_eof = read::read(&mut state, ino, total as i64, 4096)
            .await
            .expect("EOF read");
        assert!(at_eof.is_empty(), "a read AT eof is legitimately empty");

        // The cache must be intact: the file is still 64 MiB and still readable.
        let after = read::read(&mut state, ino, 0, total as u32)
            .await
            .expect("read after the EOF probe");
        assert_eq!(
            after.len(),
            total,
            "the EOF probe clobbered the unpublished size: the file's own bytes \
             are no longer readable"
        );
        assert!(after == data, "content mismatch after the EOF probe");

        // And closing must publish the REAL size, not a shrunken one.
        write::flush_inode(&mut state, ino, write::FlushReport::ToApplication)
            .await
            .expect("flush");
        // Straight from KV again, for the same reason as the precondition: what
        // is asserted here is what got PERSISTED, and reading it through the
        // function under test would make the answer depend on that function.
        let raw_after = state
            .kv_get(&key::inode_key(ino))
            .await
            .expect("read the published inode key straight from KV");
        let published = schema::decode_inode_meta(&raw_after).expect("decode published meta");
        assert_eq!(
            published.size, total as u64,
            "the file closed at the wrong size — the EOF probe won"
        );

        // ── the OTHER direction must STILL work ──────────────────────────
        // Narrowing `!=` to `>` must not break what this EOF check is FOR: a
        // cache that is stale-SMALL still has to be corrected from KV, or
        // `read` clamps a live file to a premature EOF and a sequential
        // reader silently stops. That is the hole `01e0ad7` closed, and the
        // second half of this bug's acceptance.
        //
        // Unlike the assertions above, this one is GREEN BOTH BEFORE AND AFTER
        // the fix — it is a regression guard on the behaviour being preserved,
        // not a reproduction. Shrinking the cached size below the published one
        // is the shape a second mount produces.
        state.inodes.get_mut(&ino).expect("cached").meta.size = 1024;
        let refreshed = read::read(&mut state, ino, 1024, 4096)
            .await
            .expect("read at a stale-small EOF");
        assert_eq!(
            refreshed.len(),
            4096,
            "a stale-SMALL cache must still be corrected from KV — narrowing the \
             direction must not cost the truncation guard this check exists for"
        );
        assert!(
            refreshed.iter().all(|&b| b == 0xAB),
            "stale-small refresh served the wrong bytes"
        );
    });
}
