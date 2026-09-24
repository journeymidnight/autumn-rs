//! fuse batch-data-path key-consistency regression test.
//!
//! The extent DATA path bypasses `FsState`'s `kv_*` helpers for performance
//! (read → `get_many_into`, write append → `put_many_fenced`), calling the
//! scoped client directly. Both paths and the `kv_*` metadata paths must land
//! at the SAME `fs/{tenant}/[type]…` keys, or a cold remount's range-scan/read
//! wouldn't find the extents the append path wrote. Every OTHER fuse test
//! write+read within ONE warm `FsState`, so a key mismatch there is invisible;
//! this test is the exact blind spot:
//!
//! `cold_remount_reads_back_written_extents` — write a multi-extent file, drop
//! the `FsState` (unmount), open a NEW one (cold `scan_extents`), read it back
//! byte-exact. Fails if the append path's extents aren't at the same keys the
//! cold scan/read use.
//!
//! (2026-07-19: the former `two_tenants_isolate_same_inode`
//! + `stale_volume_data_refuses_mount` cases were REMOVED — fuse no longer has a
//! tenant segment (`fs/…` is one global tree; multi-tree isolation is by distinct
//! namespaces, §8.9), so per-tenant isolation / per-tenant stale-volume refusal are
//! no longer meaningful.)
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
            let mut state = FsState::new(&mgr)
                .await
                .expect("mount1");
            dispatch::init_root(&mut state).await.expect("init_root");
            meta::put_inode(&mut state, ino, &meta::new_file_meta(0o644, 0, 0))
                .await
                .expect("put_inode");
            let n = write::write(&mut state, ino, 0, &data).await.expect("write");
            assert_eq!(n as usize, total, "write full length");
            write::flush_inode(&mut state, ino, write::FlushReport::ToApplication)
                .await
                .expect("flush");
            // state dropped here → all in-memory extent caches gone.
        }

        // ── mount #2: fresh FsState (cold), read back through scan_extents ──
        let mut state2 = FsState::new(&mgr)
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

/// `readdir` must return every entry of a directory larger than one dirent
/// range page (4096), and a kernel-style caller that consumes only part of
/// each reply and resumes from its last offset must see each name exactly
/// once. Two shapes are checked: an unbounded read from offset 0 (what the
/// PyO3 binding does) and the mount's bounded batches resumed at arbitrary
/// offsets, including offsets that land inside and exactly on a page edge.
#[test]
#[ignore]
fn readdir_pages_past_one_range_page() {
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
        let mut state = FsState::new(&mgr_addr.to_string()).await.expect("mount");
        dispatch::init_root(&mut state).await.expect("init_root");
        let (dir, _) = autumn_fuse::dir::mkdir(&mut state, 1, std::ffi::OsStr::new("big"), 0o755)
            .await
            .expect("mkdir");

        // Names where one is a prefix of the next exercise the `name ++ 0x00`
        // successor at page edges.
        let mut names: Vec<String> = (0..4300).map(|i| format!("f{i:05}")).collect();
        names.extend(["p", "p0", "p00", "p000"].map(String::from));
        names.sort();
        let dv = autumn_fuse::schema::encode_dirent(&autumn_fuse::schema::DirentValue {
            child_inode: 7,
            file_type: autumn_fuse::schema::DT_REG,
        });
        let keys: Vec<Vec<u8>> = names
            .iter()
            .map(|n| autumn_fuse::key::dirent_key(dir, n.as_bytes()))
            .collect();
        let items: Vec<(&[u8], bytes::Bytes, u64)> = keys
            .iter()
            .map(|k| (k.as_slice(), bytes::Bytes::from(dv.clone()), 0u64))
            .collect();
        for r in state.client.put_many(&items).await {
            r.expect("put dirent");
        }

        let all = autumn_fuse::dir::readdir(&mut state, dir, 0).await.expect("readdir");
        let got: Vec<String> = all.iter().skip(2).map(|e| e.name.to_string_lossy().into_owned()).collect();
        assert_eq!(got, names, "unbounded readdir lists every entry once, in order");
        let offsets: Vec<i64> = all.iter().map(|e| e.offset).collect();
        assert_eq!(offsets, (1..=names.len() as i64 + 2).collect::<Vec<_>>());

        for (batch, take) in [(256usize, 100usize), (4096, 4096), (97, 97), (5000, 4095)] {
            let (mut seen, mut offset) = (Vec::new(), 0i64);
            loop {
                let page = autumn_fuse::dir::readdir_bounded(&mut state, dir, offset, batch)
                    .await
                    .expect("readdir_bounded");
                assert!(page.len() <= batch);
                if page.is_empty() {
                    break;
                }
                // The kernel keeps what fits its buffer and asks again from
                // the last offset it kept.
                for e in page.iter().take(take) {
                    seen.push(e.name.to_string_lossy().into_owned());
                    offset = e.offset;
                }
            }
            assert_eq!(&seen[2..], names.as_slice(), "batch={batch} take={take}");
        }
    });
}
