//! Segmented files through the shared FS core against a live in-process
//! cluster: writes, overwrites, truncates and gaps only splice the map; reads
//! match a byte model cold and warm; a large map pages; reclaim removes
//! exactly the data objects the current map no longer names; and a missing
//! data extent fails the read instead of reading as zeros.

mod support;

use std::time::Duration;

use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;

use autumn_fs::schema::{SegmentMap, MAX_EXTENT};
use autumn_fs::state::FsState;
use autumn_fs::{key, meta, read, segment, write};

use support::*;

fn pattern(len: usize, seed: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(len + 8);
    let mut i = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    while out.len() < len {
        out.extend_from_slice(&i.to_le_bytes());
        i = i.wrapping_add(1);
    }
    out.truncate(len);
    out
}

async fn boot_cluster(mgr_addr: std::net::SocketAddr, n1: std::net::SocketAddr, n2: std::net::SocketAddr, base: u16, part_id: u64) -> ClusterClient {
    let mgr = RpcClient::connect(mgr_addr).await.unwrap();
    register_two_nodes(&mgr, n1, n2, base).await;
    let (log, row, meta) = create_three_streams(&mgr).await;
    upsert_partition(&mgr, part_id, log, row, meta, b"", b"\xff\xff\xff\xff").await;
    let ps_addr = pick_addr();
    start_partition_server(base as u64, mgr_addr, ps_addr);
    compio::time::sleep(Duration::from_millis(1500)).await;
    let _ = RpcClient::connect(ps_addr).await.unwrap();
    let c = ClusterClient::connect_raw(&mgr_addr.to_string()).await.expect("connect");
    c.set_rpc_timeout(Duration::from_secs(30));
    c
}

/// Take the WRITE lease a FUSE open would, and record it as held.
async fn hold_write(st: &mut FsState, ino: u64) -> u64 {
    use autumn_client::lease::{self, AcquireResult};
    let epoch = match lease::acquire(&st.client, &st.client_id, ino, autumn_rpc::manager_rpc::LEASE_MODE_WRITE)
        .await
        .unwrap()
    {
        AcquireResult::Granted(i) => i.version,
        other => panic!("{other:?}"),
    };
    st.held_leases.borrow_mut().insert(
        ino,
        autumn_fs::state::FuseLease {
            writer_refs: 1,
            reader_refs: 0,
            mode: autumn_rpc::manager_rpc::LEASE_MODE_WRITE,
            lease_epoch: epoch,
            revoked: false,
        },
    );
    epoch
}

/// Drop the lease `hold_write` took, as the last close would.
async fn drop_write(st: &mut FsState, ino: u64) {
    st.held_leases.borrow_mut().remove(&ino);
    autumn_client::lease::release(&st.client, &st.client_id, ino).await.unwrap();
}

async fn write_at(st: &mut FsState, ino: u64, off: usize, data: &[u8], model: &mut Vec<u8>) {
    write::write(st, ino, off as i64, data).await.expect("write");
    write::flush_inode(st, ino, write::FlushReport::ToApplication).await.expect("flush");
    if model.len() < off + data.len() {
        model.resize(off + data.len(), 0);
    }
    model[off..off + data.len()].copy_from_slice(data);
}

async fn read_all(st: &mut FsState, ino: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::new();
    while out.len() < len {
        let want = (len - out.len()).min(3 * MAX_EXTENT + 12345);
        let got = read::read(st, ino, out.len() as i64, want as u32).await.expect("read");
        assert!(!got.is_empty(), "short read at {}", out.len());
        out.extend_from_slice(&got);
    }
    out
}

#[test]
#[ignore]
fn segmented_files_splice_read_page_and_reclaim() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1 = pick_addr();
    let n2 = pick_addr();
    start_extent_node(n1, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot_cluster(mgr_addr, n1, n2, 141, 14101).await;
        let mgr = mgr_addr.to_string();
        let mut st = FsState::new(&mgr).await.expect("mount");
        meta::ensure_root(&mut st).await.expect("init_root");

        let ino = 500u64;
        let mut m = meta::new_file_meta(0o644, 0, 0);
        m.segments = Some(SegmentMap::default());
        meta::put_inode(&mut st, ino, &m).await.expect("create");
        let mut model = Vec::new();
        let mib = 1 << 20;

        // Without a WRITE lease a segmented file is not changed at all.
        write::write(&mut st, ino, 0, b"x").await.expect("buffered");
        let err = write::flush_inode(&mut st, ino, write::FlushReport::ToApplication).await;
        assert!(err.unwrap_err().to_string().contains("WRITE lease"));
        st.inodes.remove(&ino);
        hold_write(&mut st, ino).await;

        // 20 MiB, then a 5 MiB overwrite in the middle, both through the
        // buffered write path.
        write_at(&mut st, ino, 0, &pattern(20 * mib, 1), &mut model).await;
        let first_objects = segment::referenced_objects(
            &meta::get_inode(&mut st, ino).await.unwrap().segments.unwrap().inline,
        );
        write_at(&mut st, ino, 3 * mib + 17, &pattern(5 * mib, 2), &mut model).await;
        assert_eq!(read_all(&mut st, ino, model.len()).await, model, "warm read after overwrite");

        // Truncate, extend by truncate (a hole, never the cut bytes), and a
        // write past a gap.
        write::truncate(&mut st, ino, 10 * mib as u64).await.expect("shrink");
        model.truncate(10 * mib);
        write::truncate(&mut st, ino, 12 * mib as u64).await.expect("grow");
        model.resize(12 * mib, 0);
        write_at(&mut st, ino, 15 * mib, &pattern(mib, 3), &mut model).await;
        assert_eq!(read_all(&mut st, ino, model.len()).await, model, "after truncate/extend/gap");
        let gen_before = meta::get_inode(&mut st, ino).await.unwrap().generation;

        // Cold: a fresh session reads the same bytes, through the PS and
        // straight from the extent nodes.
        {
            let mut cold = FsState::new(&mgr).await.expect("mount2");
            assert_eq!(read_all(&mut cold, ino, model.len()).await, model, "cold read");
            cold.direct_read = true;
            assert_eq!(read_all(&mut cold, ino, model.len()).await, model, "cold direct read");
        }

        // A map past INLINE_SEGMENTS pages, and still reads right.
        for i in 0..100usize {
            write_at(&mut st, ino, 17 * mib + i * 8192, &pattern(4096, 100 + i as u64), &mut model).await;
        }
        let m = meta::get_inode(&mut st, ino).await.unwrap();
        let map = m.segments.clone().unwrap();
        assert!(map.map_id != 0 && map.inline.is_empty(), "paged: {} segments", map.count);
        assert!(m.generation > gen_before, "every content change raises the generation");
        assert_eq!(read_all(&mut st, ino, model.len()).await, model, "paged map read");
        {
            let mut cold = FsState::new(&mgr).await.expect("mount3");
            assert_eq!(read_all(&mut cold, ino, model.len()).await, model, "cold paged read");
        }

        // Overwrite everything: every earlier object and page is garbage.
        let len = model.len();
        write_at(&mut st, ino, 0, &pattern(len, 9), &mut model).await;
        let m = meta::get_inode(&mut st, ino).await.unwrap();
        let first = key::data_extent_key(
            *first_objects.iter().next().unwrap(),
            0,
            st.stripe_geom.as_ref().unwrap().lanes,
            MAX_EXTENT as u32,
        );
        assert!(st.kv_get_opt(&first).await.unwrap().is_some(), "garbage waits while the writer holds the file");
        assert!(st.kv_get_opt(&key::segment_garbage_key(ino)).await.unwrap().is_some(), "marked");
        // Another client holding the file keeps the sweep off it too.
        let other = autumn_client::lease::DaemonClientId::new("reader");
        drop_write(&mut st, ino).await;
        autumn_client::lease::acquire(&st.client, &other, ino, autumn_rpc::manager_rpc::LEASE_MODE_READ).await.unwrap();
        assert_eq!(segment::sweep_garbage(&mut st).await.unwrap(), 0);
        autumn_client::lease::release(&st.client, &other, ino).await.unwrap();
        // Last holder gone: the sweep reclaims against the map in KV.
        assert_eq!(segment::sweep_garbage(&mut st).await.unwrap(), 1);
        assert_eq!(st.kv_get_opt(&key::segment_garbage_key(ino)).await.unwrap(), None, "unmarked");
        hold_write(&mut st, ino).await;
        for d in &first_objects {
            let k = key::data_extent_key(*d, 0, st.stripe_geom.as_ref().unwrap().lanes, MAX_EXTENT as u32);
            assert_eq!(st.kv_get_opt(&k).await.unwrap(), None, "object {d} reclaimed");
        }
        assert_eq!(read_all(&mut st, ino, model.len()).await, model, "reclaim kept the live map");
        // A second pass finds nothing more.
        assert_eq!(segment::reclaim(&mut st, ino, m.segments.as_ref(), autumn_client::WriteLease::ANON).await.unwrap(), 0);

        // Lost data is an error, not zeros.
        let s0 = m.segments.as_ref().unwrap().inline[0].clone();
        st.kv_delete(&key::data_extent_key(s0.data_ino, 0, s0.lanes, s0.unit)).await.unwrap();
        match read::read(&mut st, ino, 0, 4096).await {
            Err(e) => assert!(e.to_string().contains("segmented read"), "{e}"),
            Ok(b) => panic!("missing data extent must fail, read {} bytes", b.len()),
        }
    });
}

/// Unlinking a file another client still holds leaves its data alone: the
/// tombstone waits, the sweep defers, and once the holder lets go the sweep
/// reclaims every data object, the inode and the tombstone.
#[test]
#[ignore]
fn reclaim_waits_for_other_holders() {
    use autumn_client::lease::{self, AcquireResult, DaemonClientId};
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1 = pick_addr();
    let n2 = pick_addr();
    start_extent_node(n1, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot_cluster(mgr_addr, n1, n2, 142, 14201).await;
        let mut st = FsState::new(&mgr_addr.to_string()).await.expect("mount");
        meta::ensure_root(&mut st).await.expect("init_root");
        let name = std::ffi::OsStr::new("f");
        let (ino, _) = autumn_fs::dir::create(&mut st, 1, name, 0o644).await.expect("create");
        let mut m = meta::get_inode(&mut st, ino).await.unwrap();
        m.segments = Some(SegmentMap::default());
        meta::put_inode(&mut st, ino, &m).await.unwrap();
        let mut model = Vec::new();
        hold_write(&mut st, ino).await;
        write_at(&mut st, ino, 0, &pattern(3 << 20, 7), &mut model).await;
        let s0 = meta::get_inode(&mut st, ino).await.unwrap().segments.unwrap().inline[0].clone();

        // Unlinked while this session holds it: nothing goes until the last
        // close, and then everything does.
        autumn_fs::dir::link(&mut st, ino, 1, std::ffi::OsStr::new("g")).await.expect("second name");
        autumn_fs::dir::unlink(&mut st, 1, std::ffi::OsStr::new("g")).await.expect("drop a name");
        let data_key0 = key::data_extent_key(s0.data_ino, 0, s0.lanes, s0.unit);
        assert!(st.kv_get_opt(&data_key0).await.unwrap().is_some());
        drop_write(&mut st, ino).await;
        let data_key = key::data_extent_key(s0.data_ino, 0, s0.lanes, s0.unit);
        let tomb = key::unlink_tombstone_key(ino);

        let reader = DaemonClientId::new("other-mount");
        assert!(matches!(
            lease::acquire(&st.client, &reader, ino, autumn_rpc::manager_rpc::LEASE_MODE_READ).await.unwrap(),
            AcquireResult::Granted(_)
        ));
        autumn_fs::dir::unlink(&mut st, 1, name).await.expect("unlink");
        assert!(st.kv_get_opt(&data_key).await.unwrap().is_some(), "data kept while held");
        assert!(st.kv_get_opt(&tomb).await.unwrap().is_some(), "tombstone waits");
        assert_eq!(autumn_fs::extent::sweep_unlink_tombstones(&mut st).await.unwrap(), 0);
        assert!(st.kv_get_opt(&data_key).await.unwrap().is_some(), "sweep defers too");

        lease::release(&st.client, &reader, ino).await.unwrap();
        assert_eq!(autumn_fs::extent::sweep_unlink_tombstones(&mut st).await.unwrap(), 1);
        assert_eq!(st.kv_get_opt(&data_key).await.unwrap(), None, "data reclaimed");
        assert_eq!(st.kv_get_opt(&key::inode_key(ino)).await.unwrap(), None, "inode gone");
        assert_eq!(st.kv_get_opt(&tomb).await.unwrap(), None, "tombstone gone");
        assert_eq!(st.kv_get_opt(&key::segc_key(ino, s0.data_ino)).await.unwrap(), None, "record gone");
    });
}

/// A file unlinked while THIS session holds it keeps its data until the
/// session lets go (the FUSE last close), then loses all of it.
#[test]
#[ignore]
fn unlinked_while_open_here_is_reclaimed_at_last_close() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1 = pick_addr();
    let n2 = pick_addr();
    start_extent_node(n1, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot_cluster(mgr_addr, n1, n2, 143, 14301).await;
        let mut st = FsState::new(&mgr_addr.to_string()).await.expect("mount");
        meta::ensure_root(&mut st).await.expect("init_root");
        let name = std::ffi::OsStr::new("open-f");
        let (ino, _) = autumn_fs::dir::create(&mut st, 1, name, 0o644).await.expect("create");
        let mut m = meta::get_inode(&mut st, ino).await.unwrap();
        m.segments = Some(SegmentMap::default());
        meta::put_inode(&mut st, ino, &m).await.unwrap();
        hold_write(&mut st, ino).await;
        let mut model = Vec::new();
        write_at(&mut st, ino, 0, &pattern(1 << 20, 3), &mut model).await;
        let s0 = meta::get_inode(&mut st, ino).await.unwrap().segments.unwrap().inline[0].clone();
        let data_key = key::data_extent_key(s0.data_ino, 0, s0.lanes, s0.unit);

        autumn_fs::dir::unlink(&mut st, 1, name).await.expect("unlink");
        assert!(st.unlinked_open.contains(&ino));
        assert!(st.kv_get_opt(&data_key).await.unwrap().is_some(), "open here: data kept");
        assert_eq!(autumn_fs::extent::sweep_unlink_tombstones(&mut st).await.unwrap(), 0, "the sweep leaves it too");
        assert_eq!(read_all(&mut st, ino, model.len()).await, model, "still readable through the open handle");

        // The last close.
        drop_write(&mut st, ino).await;
        assert!(st.unlinked_open.remove(&ino));
        assert!(autumn_fs::extent::reclaim_unreachable(&mut st, ino).await.unwrap());
        assert_eq!(st.kv_get_opt(&data_key).await.unwrap(), None);
        assert_eq!(st.kv_get_opt(&key::inode_key(ino)).await.unwrap(), None);
    });
}

/// `open(O_TRUNC)` and path `truncate(2)` reach the core before any open
/// holds a lease: the truncate takes its own WRITE lease for the change, and
/// is EBUSY only while another client holds the file. Every change is marked
/// for the sweep before anything is written, so an object that is never
/// published is still found.
#[test]
#[ignore]
fn a_path_truncate_takes_its_own_lease() {
    use autumn_client::lease::{self, AcquireResult, DaemonClientId};
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1 = pick_addr();
    let n2 = pick_addr();
    start_extent_node(n1, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot_cluster(mgr_addr, n1, n2, 144, 14401).await;
        let mut st = FsState::new(&mgr_addr.to_string()).await.expect("mount");
        meta::ensure_root(&mut st).await.expect("init_root");
        let (ino, _) = autumn_fs::dir::create(&mut st, 1, std::ffi::OsStr::new("t"), 0o644).await.expect("create");
        let mut m = meta::get_inode(&mut st, ino).await.unwrap();
        m.segments = Some(SegmentMap::default());
        meta::put_inode(&mut st, ino, &m).await.unwrap();
        let mut model = Vec::new();
        hold_write(&mut st, ino).await;
        write_at(&mut st, ino, 0, &pattern(2 << 20, 5), &mut model).await;
        // The first change of an empty map drops nothing, and is marked all
        // the same.
        assert!(st.kv_get_opt(&key::segment_garbage_key(ino)).await.unwrap().is_some(), "marked before publish");
        drop_write(&mut st, ino).await;
        segment::sweep_garbage(&mut st).await.unwrap();
        assert_eq!(st.kv_get_opt(&key::segment_garbage_key(ino)).await.unwrap(), None);

        // Held elsewhere: EBUSY, nothing changed.
        let other = DaemonClientId::new("other-writer");
        assert!(matches!(
            lease::acquire(&st.client, &other, ino, autumn_rpc::manager_rpc::LEASE_MODE_WRITE).await.unwrap(),
            AcquireResult::Granted(_)
        ));
        let err = write::truncate(&mut st, ino, 1 << 20).await.unwrap_err();
        assert!(err.to_string().contains("EBUSY"), "{err}");
        assert!(!st.held_leases.borrow().contains_key(&ino));
        lease::release(&st.client, &other, ino).await.unwrap();

        // Unheld: the truncate takes and returns its own lease, and reclaims
        // what it cut off.
        let before = meta::get_inode(&mut st, ino).await.unwrap().segments.unwrap().inline[0].clone();
        write::truncate(&mut st, ino, 0).await.expect("truncate without an open");
        model.clear();
        assert!(!st.held_leases.borrow().contains_key(&ino), "transient lease returned");
        assert!(matches!(
            lease::acquire(&st.client, &other, ino, autumn_rpc::manager_rpc::LEASE_MODE_WRITE).await.unwrap(),
            AcquireResult::Granted(_)
        ), "and released at the manager");
        lease::release(&st.client, &other, ino).await.unwrap();
        let m = meta::get_inode(&mut st, ino).await.unwrap();
        assert_eq!(m.size, 0);
        let k = key::data_extent_key(before.data_ino, 0, before.lanes, before.unit);
        assert_eq!(st.kv_get_opt(&k).await.unwrap(), None, "cut-off object reclaimed");
        let _ = model;
    });
}

/// A path truncate by a session whose cached copy of the file predates
/// another session's rewrite clips the CURRENT map, not the cached one — a
/// clip of the stale map, once published, would have the reclaim that
/// follows delete the other session's objects.
#[test]
#[ignore]
fn a_path_truncate_reads_the_map_under_its_lease() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1 = pick_addr();
    let n2 = pick_addr();
    start_extent_node(n1, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let _admin = boot_cluster(mgr_addr, n1, n2, 145, 14501).await;
        let mgr = mgr_addr.to_string();
        let mut a = FsState::new(&mgr).await.expect("mount a");
        meta::ensure_root(&mut a).await.expect("init_root");
        let (ino, _) = autumn_fs::dir::create(&mut a, 1, std::ffi::OsStr::new("s"), 0o644).await.expect("create");
        let mut m = meta::get_inode(&mut a, ino).await.unwrap();
        m.segments = Some(SegmentMap::default());
        meta::put_inode(&mut a, ino, &m).await.unwrap();
        let mut old = Vec::new();
        hold_write(&mut a, ino).await;
        write_at(&mut a, ino, 0, &pattern(3 << 20, 1), &mut old).await;
        drop_write(&mut a, ino).await;
        assert!(a.inodes.contains_key(&ino), "a keeps its (soon stale) copy cached");

        // Another session rewrites the whole file.
        let mut b = FsState::new(&mgr).await.expect("mount b");
        let mut new = Vec::new();
        hold_write(&mut b, ino).await;
        write_at(&mut b, ino, 0, &pattern(3 << 20, 2), &mut new).await;
        drop_write(&mut b, ino).await;
        segment::sweep_garbage(&mut b).await.unwrap();
        let b_objects = segment::referenced_objects(&meta::get_inode(&mut b, ino).await.unwrap().segments.unwrap().inline);

        write::truncate(&mut a, ino, 1 << 20).await.expect("path truncate");
        new.truncate(1 << 20);
        let mut fresh = FsState::new(&mgr).await.expect("mount c");
        assert_eq!(read_all(&mut fresh, ino, new.len()).await, new, "the current content, clipped");
        for d in b_objects {
            let k = key::data_extent_key(d, 0, a.stripe_geom.as_ref().unwrap().lanes, MAX_EXTENT as u32);
            assert!(a.kv_get_opt(&k).await.unwrap().is_some(), "the other session's object {d} survives");
        }
    });
}
