//! Whether the mount's Open keeps the kernel page cache (`FOPEN_KEEP_CACHE`).
//!
//! The mount answers every open through the page cache and keeps cached
//! pages across opens. That is only safe while nothing changed the file
//! behind the mount's back: a mount with no lease hears of no writes, so its
//! Open compares the content generation read fresh from the PS with the one
//! recorded when the pages were last vouched for.
//!
//! Two `FsState`s stand for two mounts, driven through
//! `dispatch::handle_request` — no kernel involved; the flags returned are
//! what the kernel would be told.

mod support;

use std::time::Duration;

use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;

use autumn_fs::schema::{self, DirentValue, DT_REG, ROOT_INO};
use autumn_fs::state::FsState;
use autumn_fs::{key, meta};
use autumn_fuse::dispatch::FOPEN_KEEP_CACHE;
use autumn_fuse::{bridge, dispatch};

use support::*;

const O_RDONLY: i32 = 0;
const O_RDWR: i32 = 2;
const FOPEN_DIRECT_IO: u32 = 1;

async fn boot_cluster(
    mgr_addr: std::net::SocketAddr,
    n1_addr: std::net::SocketAddr,
    n2_addr: std::net::SocketAddr,
    base: u16,
    part_id: u64,
) {
    let mgr = RpcClient::connect(mgr_addr).await.unwrap();
    register_two_nodes(&mgr, n1_addr, n2_addr, base).await;
    let (log, row, meta) = create_three_streams(&mgr).await;
    upsert_partition(&mgr, part_id, log, row, meta, b"", b"\xff\xff\xff\xff").await;
    let ps_addr = pick_addr();
    start_partition_server(base as u64, mgr_addr, ps_addr);
    compio::time::sleep(Duration::from_millis(1500)).await;
    let _ = RpcClient::connect(ps_addr).await.unwrap();
    create_namespaces(mgr_addr, &["fs"]).await;
    let cluster = ClusterClient::connect_raw(&mgr_addr.to_string())
        .await
        .expect("ClusterClient::connect");
    drop(cluster);
}

async fn mount(mgr_addr: std::net::SocketAddr) -> FsState {
    let mut state = FsState::new(&mgr_addr.to_string()).await.expect("FsState::new");
    dispatch::init_root(&mut state).await.expect("init_root");
    state.pipelined_writes = true;
    state
}

async fn seed_file(state: &mut FsState, name: &[u8], ino: u64) {
    let m = meta::new_file_meta(0o644, 0, 0);
    meta::put_new_inode(state, ino, &m).await.expect("put_inode");
    let dk = key::dirent_key(ROOT_INO, name);
    let dv = schema::encode_dirent(&DirentValue { child_inode: ino, file_type: DT_REG });
    state.kv_put(&dk, &dv).await.expect("put dirent");
}

/// The FOPEN_* flags of a successful Open.
async fn open(state: &mut FsState, ino: u64, flags: i32) -> u32 {
    let (tx, rx) = bridge::reply_channel::<(u64, u32)>();
    dispatch::handle_request(state, bridge::FsRequest::Open { ino, flags, reply: tx }, None).await;
    let (fh, open_flags) = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("open reply")
        .expect("open");
    assert_eq!(fh, ino);
    assert_eq!(open_flags & FOPEN_DIRECT_IO, 0, "no open may bypass the page cache");
    open_flags
}

async fn getattr_size(state: &mut FsState, ino: u64) -> u64 {
    let (tx, rx) = bridge::reply_channel::<autumn_fuse::fuser::FileAttr>();
    dispatch::handle_request(state, bridge::FsRequest::GetAttr { ino, reply: tx }, None).await;
    rx.recv_timeout(Duration::from_secs(10)).expect("getattr reply").expect("getattr").size
}

async fn release(state: &mut FsState, ino: u64, flags: i32) {
    let (tx, rx) = bridge::reply_channel::<()>();
    let req = bridge::FsRequest::Release { ino, flags, flush: true, reply: tx };
    dispatch::handle_request(state, req, None).await;
    rx.recv_timeout(Duration::from_secs(10)).expect("release reply").expect("release");
}

async fn write(state: &mut FsState, ino: u64, offset: i64, data: Vec<u8>) {
    let (tx, rx) = bridge::reply_channel::<u32>();
    let n = data.len() as u32;
    let req = bridge::FsRequest::Write { ino, offset, data, reply: tx };
    dispatch::handle_request(state, req, None).await;
    let wrote = rx.recv_timeout(Duration::from_secs(10)).expect("write reply").expect("write");
    assert_eq!(wrote, n);
}

/// One mount rewrites the whole file in place, same size, and closes.
async fn rewrite(state: &mut FsState, ino: u64, byte: u8) {
    open(state, ino, O_RDWR).await;
    write(state, ino, 0, vec![byte; 4096]).await;
    release(state, ino, O_RDWR).await;
}

fn keeps(flags: u32) -> bool {
    flags & FOPEN_KEEP_CACHE != 0
}

#[test]
fn page_cache_survives_reopen_until_another_mount_rewrites_the_file() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        boot_cluster(mgr_addr, n1_addr, n2_addr, 187, 18701).await;
        let mut reader = mount(mgr_addr).await;
        let mut writer = mount(mgr_addr).await;
        let ino = 1870u64;
        seed_file(&mut writer, b"weights.bin", ino).await;
        rewrite(&mut writer, ino, 0xAA).await;

        // Nothing is known about pages cached before the first open.
        assert!(!keeps(open(&mut reader, ino, O_RDONLY).await), "first open must drop the cache");
        release(&mut reader, ino, O_RDONLY).await;
        assert!(keeps(open(&mut reader, ino, O_RDONLY).await), "unchanged file: keep the cache");
        release(&mut reader, ino, O_RDONLY).await;

        // Same size, so only the generation tells the rewrite apart. The
        // reader held no lease across it and heard nothing.
        rewrite(&mut writer, ino, 0xBB).await;
        assert!(
            !keeps(open(&mut reader, ino, O_RDONLY).await),
            "a rewrite by another mount while this one held no lease must drop the cache"
        );
        release(&mut reader, ino, O_RDONLY).await;
        assert!(keeps(open(&mut reader, ino, O_RDONLY).await), "the new generation is vouched for now");
        release(&mut reader, ino, O_RDONLY).await;
    });
}

#[test]
fn an_open_under_a_held_lease_keeps_the_cache_unless_an_invalidation_failed() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        boot_cluster(mgr_addr, n1_addr, n2_addr, 188, 18801).await;
        let mut state = mount(mgr_addr).await;
        let ino = 1880u64;
        seed_file(&mut state, b"held.bin", ino).await;

        open(&mut state, ino, O_RDONLY).await; // stays open: the lease is held
        assert!(keeps(open(&mut state, ino, O_RDONLY).await), "held lease: keep the cache");
        release(&mut state, ino, O_RDONLY).await;

        // A kernel invalidation that failed leaves pages the lease cannot vouch for.
        state.notify_inval_failed.borrow_mut().insert(ino);
        assert!(
            !keeps(open(&mut state, ino, O_RDONLY).await),
            "a failed invalidation must drop the cache even under a held lease"
        );
    });
}

#[test]
fn forgetting_the_inode_forgets_what_its_pages_held() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        boot_cluster(mgr_addr, n1_addr, n2_addr, 189, 18901).await;
        let mut state = mount(mgr_addr).await;
        let ino = 1890u64;
        seed_file(&mut state, b"forgotten.bin", ino).await;

        open(&mut state, ino, O_RDONLY).await;
        release(&mut state, ino, O_RDONLY).await;
        assert!(state.page_cache_generation.contains_key(&ino));

        state.lookup_count.insert(ino, 1);
        dispatch::handle_request(&mut state, bridge::FsRequest::Forget { ino, nlookup: 1 }, None)
            .await;
        assert!(
            !state.page_cache_generation.contains_key(&ino),
            "a forgotten inode's record must go with its pages"
        );
    });
}

/// With reads served from the page cache, the kernel learns a file's size only
/// from GETATTR: it never sends a READ past it. So after another mount appends,
/// the size this mount reports must be the new one — even though it still holds
/// an fd and has the inode cached.
#[test]
fn getattr_reports_an_append_made_by_another_mount_while_an_fd_is_open() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        boot_cluster(mgr_addr, n1_addr, n2_addr, 190, 19001).await;
        let mut reader = mount(mgr_addr).await;
        dispatch::spawn_lease_background_tasks(&reader, None);
        let mut writer = mount(mgr_addr).await;
        let ino = 1900u64;
        seed_file(&mut writer, b"tail.bin", ino).await;
        rewrite(&mut writer, ino, 0xAA).await;

        open(&mut reader, ino, O_RDONLY).await; // stays open: the lease is held
        assert_eq!(getattr_size(&mut reader, ino).await, 4096);

        open(&mut writer, ino, O_RDWR).await;
        write(&mut writer, ino, 4096, vec![0xBB; 4096]).await;
        release(&mut writer, ino, O_RDWR).await;

        // The WriterClosed push reaches the reader's poll loop.
        let mut tries = 0;
        while !reader.meta_invalidated.borrow().contains(&ino) {
            tries += 1;
            assert!(tries < 100, "the writer's close never reached the reader");
            compio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            getattr_size(&mut reader, ino).await,
            8192,
            "the reader must report the appended size"
        );
    });
}

/// A mount holding no lease on a file hears of no writes to it, so what it
/// cached while it had the file open must not answer GETATTR once it closed
/// it. A `cat` of a file another mount was still writing left its size behind,
/// and `stat` reported it until the kernel forgot the inode.
#[test]
fn getattr_reports_a_write_another_mount_made_after_this_one_closed_the_file() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        boot_cluster(mgr_addr, n1_addr, n2_addr, 194, 19401).await;
        let mut reader = mount(mgr_addr).await;
        dispatch::spawn_lease_background_tasks(&reader, None);
        let mut writer = mount(mgr_addr).await;
        let ino = 1940u64;
        seed_file(&mut writer, b"late.bin", ino).await;

        // The reader's kernel looks the name up (and keeps the inode) and
        // reads the file while it is still empty.
        let (tx, rx) = bridge::reply_channel::<(autumn_fuse::fuser::FileAttr, u64)>();
        let req = bridge::FsRequest::Lookup { parent: ROOT_INO, name: "late.bin".into(), reply: tx };
        dispatch::handle_request(&mut reader, req, None).await;
        let (attr, found) = rx.recv_timeout(Duration::from_secs(10)).expect("lookup reply").expect("lookup");
        assert_eq!((found, attr.size), (ino, 0));
        open(&mut reader, ino, O_RDONLY).await;
        release(&mut reader, ino, O_RDONLY).await;

        rewrite(&mut writer, ino, 0xAA).await;
        assert!(reader.inodes.contains_key(&ino), "non-vacuity: the close left the inode cached");
        assert_eq!(
            getattr_size(&mut reader, ino).await,
            4096,
            "GETATTR answered the size cached while the file was open"
        );
    });
}

/// `open(O_TRUNC)` arrives as a SETATTR before the OPEN, with no lease held
/// yet: the truncate must start from the file's size, not from what an
/// earlier open cached. From a cached 0, `echo x > file` was a no-op truncate
/// and the write landed over the other mount's bytes.
#[test]
fn a_truncate_without_a_lease_starts_from_the_current_size() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        boot_cluster(mgr_addr, n1_addr, n2_addr, 195, 19501).await;
        let mut a = mount(mgr_addr).await;
        dispatch::spawn_lease_background_tasks(&a, None);
        let mut b = mount(mgr_addr).await;
        let ino = 1950u64;
        seed_file(&mut b, b"trunc.bin", ino).await;

        let (tx, rx) = bridge::reply_channel::<(autumn_fuse::fuser::FileAttr, u64)>();
        let req = bridge::FsRequest::Lookup { parent: ROOT_INO, name: "trunc.bin".into(), reply: tx };
        dispatch::handle_request(&mut a, req, None).await;
        rx.recv_timeout(Duration::from_secs(10)).expect("lookup reply").expect("lookup");
        open(&mut a, ino, O_RDWR).await;
        release(&mut a, ino, O_RDWR).await;
        rewrite(&mut b, ino, 0xAA).await;
        let written = meta::fetch_inode(&mut b, ino).await.expect("fetch").generation;

        // `echo x > trunc.bin` on A: SETATTR(size 0), OPEN, WRITE, RELEASE.
        let (tx, rx) = bridge::reply_channel::<autumn_fuse::fuser::FileAttr>();
        let req = bridge::FsRequest::SetAttr {
            ino,
            mode: None,
            uid: None,
            gid: None,
            size: Some(0),
            atime: None,
            mtime: None,
            reply: tx,
        };
        dispatch::handle_request(&mut a, req, None).await;
        rx.recv_timeout(Duration::from_secs(10)).expect("setattr reply").expect("setattr");
        let m = meta::fetch_inode(&mut b, ino).await.expect("fetch");
        assert_eq!(m.size, 0);
        assert!(m.generation > written, "the truncate was a no-op: the SETATTR put back the meta A cached");
        open(&mut a, ino, O_RDWR).await;
        write(&mut a, ino, 0, b"x\n".to_vec()).await;
        release(&mut a, ino, O_RDWR).await;

        let m = meta::fetch_inode(&mut b, ino).await.expect("fetch");
        assert_eq!(m.size, 2, "the truncate started from the cached size: B's bytes survive past A's write");
    });
}

/// A SETATTR that changes no size (`chmod`, `touch`) puts the whole meta it
/// read back, so with no lease it must read the current one: from what an
/// earlier open cached, `touch` set the file back to that size.
#[test]
fn a_chmod_without_a_lease_keeps_another_mount_s_write() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        boot_cluster(mgr_addr, n1_addr, n2_addr, 197, 19701).await;
        let mut a = mount(mgr_addr).await;
        let mut b = mount(mgr_addr).await;
        let ino = 1970u64;
        seed_file(&mut b, b"chmod.bin", ino).await;
        let (tx, rx) = bridge::reply_channel::<(autumn_fuse::fuser::FileAttr, u64)>();
        let req = bridge::FsRequest::Lookup { parent: ROOT_INO, name: "chmod.bin".into(), reply: tx };
        dispatch::handle_request(&mut a, req, None).await;
        rx.recv_timeout(Duration::from_secs(10)).expect("lookup reply").expect("lookup");
        open(&mut a, ino, O_RDONLY).await;
        release(&mut a, ino, O_RDONLY).await;
        assert!(a.inodes.contains_key(&ino), "non-vacuity: the close left the inode cached");
        rewrite(&mut b, ino, 0xAA).await;
        let written = meta::fetch_inode(&mut b, ino).await.expect("fetch");

        let (tx, rx) = bridge::reply_channel::<autumn_fuse::fuser::FileAttr>();
        let req = bridge::FsRequest::SetAttr {
            ino,
            mode: Some(0o600),
            uid: None,
            gid: None,
            size: None,
            atime: None,
            mtime: None,
            reply: tx,
        };
        dispatch::handle_request(&mut a, req, None).await;
        rx.recv_timeout(Duration::from_secs(10)).expect("setattr reply").expect("setattr");

        let m = meta::fetch_inode(&mut b, ino).await.expect("fetch");
        assert_eq!(m.mode & 0o7777, 0o600);
        assert_eq!(
            (m.size, m.generation),
            (written.size, written.generation),
            "the chmod put back the meta A cached before B's write"
        );
    });
}

/// An fd left without a lease (a lost poll drops READ slots) re-reads the
/// meta on every read, and keeps its extent map only while the content is
/// unchanged. A write into a hole changes neither the size nor any extent the
/// map already lists, so a map kept past it reads the new bytes as zeros.
#[test]
fn a_read_without_a_lease_sees_a_write_into_a_hole() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        boot_cluster(mgr_addr, n1_addr, n2_addr, 196, 19601).await;
        let mut a = mount(mgr_addr).await;
        let mut b = mount(mgr_addr).await;
        let ino = 1960u64;
        seed_file(&mut b, b"sparse.bin", ino).await;
        const MIB: i64 = 1 << 20;
        open(&mut b, ino, O_RDWR).await;
        write(&mut b, ino, 0, vec![0xAA; MIB as usize]).await;
        write(&mut b, ino, 16 * MIB - 4096, vec![0xAA; 4096]).await;
        release(&mut b, ino, O_RDWR).await;

        open(&mut a, ino, O_RDONLY).await;
        let hole = autumn_fs::read::read(&mut a, ino, 8 * MIB, 4096).await.expect("read hole");
        assert!(hole.iter().all(|&x| x == 0), "precondition: a hole reads as zeros");
        assert!(
            a.inodes.get(&ino).and_then(|is| is.extents.as_ref()).is_some(),
            "non-vacuity: the read cached the extent map"
        );
        a.held_leases.borrow_mut().remove(&ino);

        open(&mut b, ino, O_RDWR).await;
        write(&mut b, ino, 8 * MIB, vec![0xBB; 4096]).await;
        release(&mut b, ino, O_RDWR).await;
        assert_eq!(meta::fetch_inode(&mut b, ino).await.expect("fetch").size, 16 * MIB as u64);

        let got = autumn_fs::read::read(&mut a, ino, 8 * MIB, 4096).await.expect("read");
        assert!(got.iter().all(|&x| x == 0xBB), "the read used the extent map from before the write");
    });
}

/// Bytes this mount wrote are in its page cache under its own write lease:
/// the next open must keep them rather than read them all back.
#[test]
fn a_file_this_mount_wrote_stays_cached_on_the_next_open() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        boot_cluster(mgr_addr, n1_addr, n2_addr, 191, 19101).await;
        let mut state = mount(mgr_addr).await;
        let ino = 1910u64;
        seed_file(&mut state, b"written.bin", ino).await;
        rewrite(&mut state, ino, 0xAA).await;
        assert!(
            keeps(open(&mut state, ino, O_RDONLY).await),
            "a file this mount just wrote must keep its cache"
        );
    });
}

/// While this mount holds the write lease nobody else can change the file, so
/// a mark (a `WillRevokeIn` to us) is no reason to trust KV over the cache — whose size may be ahead of KV's (a
/// flush clears `dirty` before its put lands). Adopting the smaller KV size is
/// the data loss `meta::get_inode_uncached` describes.
#[test]
fn a_mark_never_shrinks_the_size_of_a_file_this_mount_is_writing() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        boot_cluster(mgr_addr, n1_addr, n2_addr, 192, 19201).await;
        let mut state = mount(mgr_addr).await;
        let ino = 1920u64;
        seed_file(&mut state, b"growing.bin", ino).await;
        rewrite(&mut state, ino, 0xAA).await;
        let published = meta::fetch_inode(&mut state, ino).await.expect("fetch");
        assert_eq!(published.size, 4096);

        open(&mut state, ino, O_RDWR).await; // holds the write lease
        write(&mut state, ino, 4096, vec![0xBB; 4096]).await;
        // The shape a failed final put leaves: clean, size 8192, KV still 4096.
        let is = state.inodes.get_mut(&ino).expect("cached");
        is.dirty = false;
        state.dirty_inodes.remove(&ino);
        state.meta_invalidated.borrow_mut().insert(ino);

        assert_eq!(
            getattr_size(&mut state, ino).await,
            8192,
            "a mark must not replace the writer's own size with KV's older one"
        );
    });
}
