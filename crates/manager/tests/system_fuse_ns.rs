//! F-KEY-NS SD-3 — fuse volume-scoping regression tests (the P0 blind spot).
//!
//! The SD-3 core wires the `{volume}/` prefix into `FsState`'s `kv_*` helpers,
//! but the extent DATA path bypasses them for performance (read →
//! `get_many_into`, write append → `put_many_fenced`). The original SD-3 commit
//! missed prefixing those, so extents landed at `fs/{tenant}/[0x03]…` (no volume)
//! while metadata used `fs/{tenant}/{volume}/…`. Every existing fuse test
//! write+read within ONE warm `FsState`, so the mismatch was invisible. These
//! two tests are the exact blind spot — they REMOUNT (fresh cache → cold
//! range-scan) and use TWO volumes:
//!
//! 1. `cold_remount_reads_back_written_extents` — write a multi-extent file,
//!    drop the `FsState` (unmount), open a NEW one (cold `scan_extents`), read
//!    it back byte-exact. Fails if the append path's extents aren't at the same
//!    volume-scoped keys the cold scan/read use.
//! 2. `two_volumes_isolate_same_inode` — two volumes both write ino 100 with
//!    DIFFERENT content; each must read back its OWN bytes. Fails if extents
//!    collide in a shared (unprefixed) keyspace.
//!
//! Driven directly through `FsState` (the kernel FUSE mount layer is unchanged).
//! `#[ignore]` — needs a live cluster (manager + 2 EN + PS), same as
//! `system_fuse_read.rs`.

mod support;

use std::time::Duration;

use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;

use autumn_fuse::state::FsState;
use autumn_fuse::{dispatch, meta, read, write};

use support::*;

/// Deterministic, seed-varied byte pattern so two volumes' payloads differ.
fn pattern(len: usize, seed: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut i: u64 = seed;
    while out.len() + 8 <= len {
        out.extend_from_slice(&i.to_le_bytes());
        i = i.wrapping_add(1);
    }
    while out.len() < len {
        out.push(seed as u8);
    }
    out
}

/// Stand up a 2-EN + PS cluster with one partition covering the whole keyspace.
/// Returns an admin client kept alive for the test's duration.
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
fn cold_remount_reads_back_written_extents() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot_cluster(mgr_addr, n1_addr, n2_addr, 138, 13801).await;
        let mgr = mgr_addr.to_string();

        // 10 MiB → 2 extents, written through the buffered append path
        // (flush_appends → put_many_fenced — the batch write that bypassed the
        // kv_* choke point pre-fix).
        let total = 10 * 1024 * 1024usize;
        let data = pattern(total, 0xA5);
        let ino = 100u64;

        // ── mount #1: write + flush, then DROP (unmount) ──
        {
            let mut state = FsState::new(&mgr, "default", "default")
                .await
                .expect("mount1");
            dispatch::init_root(&mut state).await.expect("init_root");
            meta::put_inode(&mut state, ino, &meta::new_file_meta(0o644, 0, 0))
                .await
                .expect("put_inode");
            let n = write::write(&mut state, ino, 0, &data).await.expect("write");
            assert_eq!(n as usize, total, "write full length");
            write::flush_inode(&mut state, ino).await.expect("flush");
            // state dropped here → all in-memory extent caches gone.
        }

        // ── mount #2: fresh FsState (cold), read back through scan_extents ──
        let mut state2 = FsState::new(&mgr, "default", "default")
            .await
            .expect("mount2");
        // init_root is idempotent — root already exists, this is a no-op.
        dispatch::init_root(&mut state2).await.expect("init_root2");
        let back = read::read(&mut state2, ino, 0, total as u32)
            .await
            .expect("cold read");
        assert_eq!(back.len(), total, "cold read length");
        assert!(
            back == data,
            "cold remount read did NOT match written bytes — extents are not at \
             the volume-scoped keys the cold scan/read use (SD-3 P0 regression)"
        );
    });
}

#[test]
#[ignore]
fn two_volumes_isolate_same_inode() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot_cluster(mgr_addr, n1_addr, n2_addr, 139, 13901).await;
        let mgr = mgr_addr.to_string();

        // Two volumes of the SAME tenant; both write the SAME inode number with
        // DIFFERENT content. Isolation must come from the `{volume}/` key prefix.
        let mut vol_a = FsState::new(&mgr, "default", "vola").await.expect("vola");
        let mut vol_b = FsState::new(&mgr, "default", "volb").await.expect("volb");
        dispatch::init_root(&mut vol_a).await.expect("init vola");
        dispatch::init_root(&mut vol_b).await.expect("init volb");

        let len = 2 * 1024 * 1024usize; // non-inline → extent path
        let data_a = pattern(len, 0x11);
        let data_b = pattern(len, 0x22);
        assert!(data_a != data_b, "sanity: patterns differ");
        let ino = 100u64;

        for (state, data, who) in [
            (&mut vol_a, &data_a, "vola"),
            (&mut vol_b, &data_b, "volb"),
        ] {
            meta::put_inode(state, ino, &meta::new_file_meta(0o644, 0, 0))
                .await
                .unwrap_or_else(|e| panic!("put_inode {who}: {e}"));
            write::write(state, ino, 0, data)
                .await
                .unwrap_or_else(|e| panic!("write {who}: {e}"));
            write::flush_inode(state, ino)
                .await
                .unwrap_or_else(|e| panic!("flush {who}: {e}"));
        }

        // Each volume must read back ITS OWN bytes — a shared (unprefixed)
        // extent keyspace would make the second writer clobber the first.
        let back_a = read::read(&mut vol_a, ino, 0, len as u32)
            .await
            .expect("read vola");
        let back_b = read::read(&mut vol_b, ino, 0, len as u32)
            .await
            .expect("read volb");
        assert!(back_a == data_a, "vol A ino 100 leaked/collided with vol B");
        assert!(back_b == data_b, "vol B ino 100 leaked/collided with vol A");
    });
}
