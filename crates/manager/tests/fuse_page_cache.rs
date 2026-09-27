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
    meta::put_inode(state, ino, &m).await.expect("put_inode");
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
#[ignore]
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
#[ignore]
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
#[ignore]
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
#[ignore]
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

/// Bytes this mount wrote are in its page cache under its own write lease:
/// the next open must keep them rather than read them all back.
#[test]
#[ignore]
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
#[ignore]
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
