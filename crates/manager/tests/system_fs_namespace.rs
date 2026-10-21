//! Namespace operations of the shared fs core (`autumn_fs::dir`) against a
//! live in-process cluster: two sessions creating one name, and rename onto
//! names it must not replace.

mod support;

use std::ffi::OsStr;
use std::time::Duration;

use autumn_fs::state::FsState;
use autumn_fs::{dir, key, meta};
use autumn_rpc::client::RpcClient;

use support::*;

/// The manager's address, and the extent nodes' data dirs (removed on drop).
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
    (mgr_addr.to_string(), dirs)
}

async fn mount(mgr: &str) -> FsState {
    let mut st = FsState::new(mgr).await.expect("mount");
    meta::ensure_root(&mut st).await.expect("root");
    st
}

fn errno(r: &anyhow::Result<impl std::fmt::Debug>) -> String {
    match r {
        Ok(v) => format!("Ok({v:?})"),
        Err(e) => e.to_string(),
    }
}

async fn named(st: &mut FsState, parent: u64, name: &str) -> Option<u64> {
    dir::lookup_opt(st, parent, OsStr::new(name)).await.unwrap().map(|(ino, _)| ino)
}

async fn links(st: &mut FsState, ino: u64) -> u32 {
    meta::fetch_inode(st, ino).await.unwrap().nlink
}

/// Two sessions create one name at once: one inode wins the name, the
/// other create fails EEXIST and leaves no inode behind; the same for mkdir,
/// whose parent counts exactly one new subdirectory.
#[test]
fn two_sessions_creating_one_name_publish_one_inode() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, _dirs) = boot(233, 23301).await;
        let (mut a, mut b) = (mount(&mgr).await, mount(&mgr).await);

        let (one, two) = futures::join!(
            dir::create(&mut a, 1, OsStr::new("raced"), 0o644),
            dir::create(&mut b, 1, OsStr::new("raced"), 0o644)
        );
        let (won, lost) = match (&one, &two) {
            (Ok((w, _)), Err(_)) => (*w, two.as_ref().err().unwrap()),
            (Err(_), Ok((w, _))) => (*w, one.as_ref().err().unwrap()),
            _ => panic!("expected one create to win: a={} b={}", errno(&one), errno(&two)),
        };
        assert!(lost.to_string().contains("EEXIST"), "{lost}");
        assert_eq!(named(&mut a, 1, "raced").await, Some(won));
        // The losing session's last allocation is the inode it wrote.
        let loser = if one.is_ok() { b.next_inode } else { a.next_inode } - 1;
        assert!(a.kv_get_opt(&key::inode_key(loser)).await.unwrap().is_none(), "the losing inode {loser} was left behind");

        let root_links = links(&mut a, 1).await;
        let (one, two) = futures::join!(
            dir::mkdir(&mut a, 1, OsStr::new("raced-dir"), 0o755),
            dir::mkdir(&mut b, 1, OsStr::new("raced-dir"), 0o755)
        );
        assert!(one.is_ok() != two.is_ok(), "expected one mkdir to win: a={} b={}", errno(&one), errno(&two));
        assert_eq!(links(&mut a, 1).await, root_links + 1);
    });
}

/// Two sessions make different subdirectories of one parent at once: the
/// parent counts both.
#[test]
fn concurrent_mkdirs_in_one_parent_both_count() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, _dirs) = boot(234, 23401).await;
        let (mut a, mut b) = (mount(&mgr).await, mount(&mgr).await);
        let before = links(&mut a, 1).await;
        for i in 0..4 {
            let (x, y) = (format!("a{i}"), format!("b{i}"));
            let (one, two) = futures::join!(
                dir::mkdir(&mut a, 1, OsStr::new(&x), 0o755),
                dir::mkdir(&mut b, 1, OsStr::new(&y), 0o755)
            );
            one.unwrap();
            two.unwrap();
        }
        assert_eq!(links(&mut a, 1).await, before + 8, "a concurrent mkdir's link was lost");
    });
}

/// One session makes subdirectories while another unlinks files in the same
/// parent: the unlinks' mtime updates do not overwrite the link counts.
#[test]
fn unlinks_beside_mkdirs_keep_the_parent_s_links() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, _dirs) = boot(237, 23701).await;
        let (mut a, mut b) = (mount(&mgr).await, mount(&mgr).await);
        const ROUNDS: u64 = 60;
        for i in 0..ROUNDS {
            dir::create(&mut b, 1, OsStr::new(&format!("f{i}")), 0o644).await.unwrap();
        }
        let before = links(&mut a, 1).await;
        // The mkdir starts later each round, so some round lands its link
        // inside the unlink's parent update.
        for i in 0..ROUNDS {
            let (d, f) = (format!("d{i}"), format!("f{i}"));
            let (one, two) = futures::join!(
                async {
                    compio::time::sleep(Duration::from_micros(100 * i)).await;
                    dir::mkdir(&mut a, 1, OsStr::new(&d), 0o755).await
                },
                dir::unlink(&mut b, 1, OsStr::new(&f))
            );
            one.unwrap();
            two.unwrap();
        }
        assert_eq!(links(&mut a, 1).await, before + ROUNDS as u32, "an unlink overwrote a mkdir's link");
    });
}

/// rename onto a non-empty directory, a file onto a directory and a
/// directory onto a file are refused with nothing changed; a directory
/// replaces an empty one and the link counts follow; a name onto its own
/// inode is a no-op.
#[test]
fn rename_refuses_what_it_must_not_replace() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, _dirs) = boot(235, 23501).await;
        let mut a = mount(&mgr).await;
        let (src, _) = dir::mkdir(&mut a, 1, OsStr::new("srcdir"), 0o755).await.unwrap();
        let (dst, _) = dir::mkdir(&mut a, 1, OsStr::new("dstdir"), 0o755).await.unwrap();
        let (precious, _) = dir::create(&mut a, dst, OsStr::new("precious"), 0o644).await.unwrap();
        let (file, _) = dir::create(&mut a, 1, OsStr::new("file"), 0o644).await.unwrap();

        let r = dir::rename(&mut a, 1, OsStr::new("srcdir"), 1, OsStr::new("dstdir")).await;
        assert!(errno(&r).contains("ENOTEMPTY"), "{}", errno(&r));
        let r = dir::rename(&mut a, 1, OsStr::new("file"), 1, OsStr::new("dstdir")).await;
        assert!(errno(&r).contains("EISDIR"), "{}", errno(&r));
        let r = dir::rename(&mut a, 1, OsStr::new("srcdir"), 1, OsStr::new("file")).await;
        assert!(errno(&r).contains("ENOTDIR"), "{}", errno(&r));
        assert_eq!(named(&mut a, 1, "srcdir").await, Some(src));
        assert_eq!(named(&mut a, 1, "dstdir").await, Some(dst));
        assert_eq!(named(&mut a, dst, "precious").await, Some(precious));
        assert_eq!(named(&mut a, 1, "file").await, Some(file));

        dir::rename(&mut a, 1, OsStr::new("file"), 1, OsStr::new("file")).await.unwrap();
        assert_eq!(named(&mut a, 1, "file").await, Some(file));

        // Same parent: srcdir replaces an empty directory.
        let (empty, _) = dir::mkdir(&mut a, 1, OsStr::new("empty"), 0o755).await.unwrap();
        let root = links(&mut a, 1).await;
        dir::rename(&mut a, 1, OsStr::new("srcdir"), 1, OsStr::new("empty")).await.unwrap();
        assert_eq!(named(&mut a, 1, "empty").await, Some(src));
        assert_eq!(named(&mut a, 1, "srcdir").await, None);
        assert!(a.kv_get_opt(&key::inode_key(empty)).await.unwrap().is_none(), "the replaced directory's inode stayed");
        assert_eq!(links(&mut a, 1).await, root - 1);

        // Across parents: dstdir/inner replaces the empty other/hole.
        let (inner, _) = dir::mkdir(&mut a, dst, OsStr::new("inner"), 0o755).await.unwrap();
        let (other, _) = dir::mkdir(&mut a, 1, OsStr::new("other"), 0o755).await.unwrap();
        dir::mkdir(&mut a, other, OsStr::new("hole"), 0o755).await.unwrap();
        let hole = named(&mut a, other, "hole").await.unwrap();
        let (dst_links, other_links) = (links(&mut a, dst).await, links(&mut a, other).await);
        dir::rename(&mut a, dst, OsStr::new("inner"), other, OsStr::new("hole")).await.unwrap();
        assert!(a.kv_get_opt(&key::inode_key(hole)).await.unwrap().is_none(), "the replaced directory's inode stayed");
        assert_eq!(named(&mut a, other, "hole").await, Some(inner));
        assert_eq!(links(&mut a, dst).await, dst_links - 1);
        assert_eq!(links(&mut a, other).await, other_links);
    });
}

/// Two sessions rename different files onto one free name at once. One
/// lands first; the other then replaces it as a rename over a file does,
/// so the file it replaced is removed rather than left with no name.
#[test]
fn concurrent_renames_onto_one_name_replace_in_turn() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, _dirs) = boot(236, 23601).await;
        let (mut a, mut b) = (mount(&mgr).await, mount(&mgr).await);
        let (x, _) = dir::create(&mut a, 1, OsStr::new("x"), 0o644).await.unwrap();
        let (y, _) = dir::create(&mut b, 1, OsStr::new("y"), 0o644).await.unwrap();
        let (one, two) = futures::join!(
            dir::rename(&mut a, 1, OsStr::new("x"), 1, OsStr::new("t")),
            dir::rename(&mut b, 1, OsStr::new("y"), 1, OsStr::new("t"))
        );
        one.unwrap();
        two.unwrap();
        let won = named(&mut a, 1, "t").await.expect("t");
        let lost = if won == x { y } else { x };
        assert!(
            a.kv_get_opt(&key::inode_key(lost)).await.unwrap().is_none(),
            "the file {lost} the other rename replaced was left with no name"
        );
    });
}

/// Creates move a directory's mtime once per wall-clock second: the first in
/// a second writes it, the next in the same second does not, one in a later
/// second does.
#[test]
fn creates_move_a_directory_s_mtime_once_a_second() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, _dirs) = boot(238, 23801).await;
        let mut a = mount(&mgr).await;
        let (d, made) = dir::mkdir(&mut a, 1, OsStr::new("d"), 0o755).await.unwrap();
        // Start just after a second boundary, so two quick creates share it.
        compio::time::sleep(Duration::from_millis(1100)).await;
        let into_second = || {
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().subsec_millis()
        };
        while into_second() > 100 {
            compio::time::sleep(Duration::from_millis(10)).await;
        }
        dir::create(&mut a, d, OsStr::new("f1"), 0o644).await.unwrap();
        let after_first = raw(&mut a, d).await;
        let first = autumn_fs::schema::decode_inode_meta(&after_first).unwrap();
        assert!(first.mtime_secs > made.mtime_secs, "the first create of a later second left the mtime");
        dir::create(&mut a, d, OsStr::new("f2"), 0o644).await.unwrap();
        assert_eq!(raw(&mut a, d).await, after_first, "a create in the same second rewrote the directory");
        compio::time::sleep(Duration::from_millis(1100)).await;
        dir::create(&mut a, d, OsStr::new("f3"), 0o644).await.unwrap();
        let m = meta::fetch_inode(&mut a, d).await.unwrap();
        assert!(m.mtime_secs > first.mtime_secs, "a create a second later left the mtime");
        assert_eq!(m.ctime_secs, m.mtime_secs);
    });
}

async fn raw(st: &mut FsState, ino: u64) -> Vec<u8> {
    st.kv_get_opt(&key::inode_key(ino)).await.unwrap().unwrap()
}
