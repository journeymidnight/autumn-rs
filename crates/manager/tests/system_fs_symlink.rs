//! Symbolic links in the shared fs core (`autumn_fs::dir`) against a live
//! in-process cluster: creating and reading a link, resolving paths through
//! links, and removing or replacing a link without touching its target.

mod support;

use std::ffi::OsStr;
use std::time::Duration;

use autumn_fs::schema::{DT_DIR, DT_LNK, DT_REG};
use autumn_fs::state::FsState;
use autumn_fs::{dir, key, meta, write};
use autumn_rpc::client::RpcClient;

use support::*;

async fn boot(base: u16, part: u64) -> (String, (tempfile::TempDir, tempfile::TempDir)) {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let (n1, n2) = (pick_addr(), pick_addr());
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    start_extent_node(n1, dirs.0.path().to_path_buf(), 1);
    start_extent_node(n2, dirs.1.path().to_path_buf(), 2);
    let mgr = RpcClient::connect(mgr_addr).await.unwrap();
    register_two_nodes(&mgr, n1, n2, base).await;
    let (log, row, meta_stream) = create_three_streams(&mgr).await;
    upsert_partition(&mgr, part, log, row, meta_stream, b"", b"\xff\xff\xff\xff").await;
    start_partition_server(base as u64, mgr_addr, pick_addr());
    compio::time::sleep(Duration::from_millis(1500)).await;
    create_namespaces(mgr_addr, &["fs"]).await;
    (mgr_addr.to_string(), dirs)
}

async fn mount(mgr: &str) -> FsState {
    let mut st = FsState::new(mgr).await.expect("mount");
    meta::ensure_root(&mut st).await.expect("root");
    st
}

async fn file(st: &mut FsState, parent: u64, name: &str, body: &[u8]) -> u64 {
    let (ino, _) = dir::create(st, parent, OsStr::new(name), 0o644).await.unwrap();
    write::write(st, ino, 0, body).await.unwrap();
    write::flush_inode(st, ino, write::FlushReport::ToApplication).await.unwrap();
    ino
}

async fn follow(st: &mut FsState, path: &str) -> Option<(u64, u8)> {
    dir::resolve_follow(st, path, true).await.unwrap()
}

/// The HuggingFace cache's shape: `snapshots/<rev>/<file>` is a relative
/// link into `blobs/`. The link reads back as written, resolves to the blob
/// through `..`, and `lstat`-style resolution stops at it.
#[test]
fn a_relative_link_resolves_through_dot_dot() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, _dirs) = boot(262, 26201).await;
        let mut st = mount(&mgr).await;
        let (model, _) = dir::mkdir(&mut st, 1, OsStr::new("model"), 0o755).await.unwrap();
        let (blobs, _) = dir::mkdir(&mut st, model, OsStr::new("blobs"), 0o755).await.unwrap();
        let (snaps, _) = dir::mkdir(&mut st, model, OsStr::new("snapshots"), 0o755).await.unwrap();
        let (rev, _) = dir::mkdir(&mut st, snaps, OsStr::new("r1"), 0o755).await.unwrap();
        let blob = file(&mut st, blobs, "abc123", b"weights").await;

        let target = b"../../blobs/abc123";
        let (link, m) = dir::symlink(&mut st, rev, OsStr::new("w.bin"), target).await.unwrap();
        assert_eq!(m.mode & meta::S_IFMT, meta::S_IFLNK);
        assert_eq!(m.size, target.len() as u64);
        assert_eq!(dir::readlink(&mut st, link).await.unwrap(), target);
        let listed = dir::readdir(&mut st, rev, 0).await.unwrap();
        assert!(listed.iter().any(|e| e.name == "w.bin" && e.kind == DT_LNK && e.ino == link));

        assert_eq!(follow(&mut st, "/model/snapshots/r1/w.bin").await, Some((blob, DT_REG)));
        assert_eq!(
            dir::resolve_follow(&mut st, "/model/snapshots/r1/w.bin", false).await.unwrap(),
            Some((link, DT_LNK))
        );
        // A later session reads the target from the KV, not from a cache.
        let mut other = mount(&mgr).await;
        assert_eq!(follow(&mut other, "/model/snapshots/r1/w.bin").await, Some((blob, DT_REG)));

        // A link to a directory is walked through, and a link may sit in
        // the middle of a path.
        dir::symlink(&mut st, 1, OsStr::new("latest"), b"model/snapshots/r1").await.unwrap();
        assert_eq!(follow(&mut st, "/latest/w.bin").await, Some((blob, DT_REG)));
        assert_eq!(follow(&mut st, "/latest").await, Some((rev, DT_DIR)));
        // A last link written with a trailing `/` or `/.` is followed even
        // by `lstat`, as POSIX does.
        assert_eq!(dir::resolve_follow(&mut st, "/latest/", false).await.unwrap(), Some((rev, DT_DIR)));
        assert_eq!(dir::resolve_follow(&mut st, "/latest/.", false).await.unwrap(), Some((rev, DT_DIR)));
        // `..` of the root is the root.
        assert_eq!(follow(&mut st, "/../model/blobs/abc123").await, Some((blob, DT_REG)));
    });
}

/// An absolute target starts from this tree's root; a dangling link, a walk
/// through a file and a loop resolve to nothing, the loop as ELOOP.
#[test]
fn absolute_dangling_and_looping_links() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, _dirs) = boot(263, 26301).await;
        let mut st = mount(&mgr).await;
        let (d, _) = dir::mkdir(&mut st, 1, OsStr::new("d"), 0o755).await.unwrap();
        let f = file(&mut st, d, "f", b"x").await;

        dir::symlink(&mut st, d, OsStr::new("abs"), b"/d/f").await.unwrap();
        assert_eq!(follow(&mut st, "/d/abs").await, Some((f, DT_REG)));
        dir::symlink(&mut st, d, OsStr::new("gone"), b"nothing-here").await.unwrap();
        assert_eq!(follow(&mut st, "/d/gone").await, None);
        assert_eq!(follow(&mut st, "/d/f/below").await, None, "a file is not walked through");

        dir::symlink(&mut st, d, OsStr::new("a"), b"b").await.unwrap();
        dir::symlink(&mut st, d, OsStr::new("b"), b"a").await.unwrap();
        let err = dir::resolve_follow(&mut st, "/d/a", true).await.unwrap_err();
        assert!(err.to_string().contains("ELOOP"), "{err}");

        // A link whose inode is gone (removed after its name was read) leads
        // nowhere rather than failing the resolution.
        let (gone_ino, _) = dir::symlink(&mut st, d, OsStr::new("vanished"), b"f").await.unwrap();
        st.kv_delete(&key::inode_key(gone_ino)).await.unwrap();
        st.inodes.remove(&gone_ino);
        assert_eq!(follow(&mut st, "/d/vanished").await, None);

        // Not a link: EINVAL, as readlink(2). An empty target is refused.
        let err = dir::readlink(&mut st, f).await.unwrap_err();
        assert!(err.to_string().contains("EINVAL"), "{err}");
        assert!(dir::symlink(&mut st, d, OsStr::new("empty"), b"").await.is_err());
        let err = dir::symlink(&mut st, d, OsStr::new("f"), b"x").await.unwrap_err();
        assert!(err.to_string().contains("EEXIST"), "{err}");
    });
}

/// Removing a link removes the link only; renaming a file over a link
/// replaces the link, whose inode goes away while the old target stays.
#[test]
fn removing_or_replacing_a_link_leaves_its_target() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, _dirs) = boot(264, 26401).await;
        let mut st = mount(&mgr).await;
        let target = file(&mut st, 1, "target", b"keep me").await;

        let (l1, _) = dir::symlink(&mut st, 1, OsStr::new("l1"), b"target").await.unwrap();
        dir::unlink(&mut st, 1, OsStr::new("l1")).await.unwrap();
        assert!(dir::lookup_opt(&mut st, 1, OsStr::new("l1")).await.unwrap().is_none());
        assert!(st.kv_get_opt(&key::inode_key(l1)).await.unwrap().is_none(), "the link's inode was left");
        assert_eq!(meta::fetch_inode(&mut st, target).await.unwrap().nlink, 1);

        let (l2, _) = dir::symlink(&mut st, 1, OsStr::new("l2"), b"target").await.unwrap();
        let other = file(&mut st, 1, "other", b"new").await;
        dir::rename(&mut st, 1, OsStr::new("other"), 1, OsStr::new("l2")).await.unwrap();
        assert_eq!(follow(&mut st, "/l2").await, Some((other, DT_REG)));
        assert!(st.kv_get_opt(&key::inode_key(l2)).await.unwrap().is_none(), "the replaced link's inode was left");
        assert_eq!(follow(&mut st, "/target").await, Some((target, DT_REG)));
    });
}
