//! A file's link count against concurrent sessions: one session drops a name
//! while another adds one. Whatever the order, a name that exists reads the
//! file's bytes.

mod support;

use std::ffi::OsStr;
use std::time::Duration;

use autumn_fs::publish::{Condition, NewFile};
use autumn_fs::state::FsState;
use autumn_fs::{dir, meta, read};
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

/// Publishes `name` in the root holding `bytes`; its inode.
async fn put(st: &mut FsState, name: &str, bytes: &[u8]) -> u64 {
    let mut f = NewFile::begin(st, 1, name.as_bytes()).await.unwrap();
    f.write(st, bytes).await.unwrap();
    f.finish(st).await.unwrap();
    let ino = f.ino;
    f.publish(st, Condition::None).await.unwrap();
    ino
}

/// The bytes `name` reads: `Ok(None)` if it does not exist, `Err` if it
/// names an inode or data that is gone.
async fn read_name(st: &mut FsState, name: &str) -> anyhow::Result<Option<Vec<u8>>> {
    let Some((ino, m)) = dir::lookup_opt(st, 1, OsStr::new(name)).await? else {
        return Ok(None);
    };
    let mut out = Vec::new();
    while (out.len() as u64) < m.size {
        let got = read::read(st, ino, out.len() as i64, 1 << 20).await?;
        anyhow::ensure!(!got.is_empty(), "short read at {}", out.len());
        out.extend_from_slice(&got);
    }
    Ok(Some(out))
}

/// A racing link's outcome: refused because the file's last name was gone
/// (ENOENT), or a second name that reads `body`; any other result goes to
/// `lost`. Returns 1 when the link succeeded.
async fn check_link(
    st: &mut FsState,
    round: u64,
    linked: anyhow::Result<autumn_fs::schema::InodeMeta>,
    name: &str,
    body: &[u8],
    lost: &mut Vec<(u64, String)>,
) -> u32 {
    match linked {
        Ok(_) => {
            match read_name(st, name).await {
                Ok(Some(bytes)) if bytes == body => {}
                other => lost.push((round, format!("{:?}", other.map(|b| b.map(|b| b.len()))))),
            }
            1
        }
        Err(e) => {
            let msg = e.to_string();
            if !(msg.contains("ENOENT") || msg.contains("not found")) {
                lost.push((round, format!("link refused for another reason: {msg}")));
            }
            0
        }
    }
}

/// One session unlinks a file's only name while another links it under a
/// second name. Either the link lands first and the second name reads the
/// bytes, or the unlink does and the link fails; a second name over removed
/// data is the lost update. The link starts a little later each round, so
/// some round lands it inside the unlink's read-modify-write of the count.
#[test]
fn an_unlink_racing_a_link_keeps_the_new_name_s_bytes() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, _dirs) = boot(245, 24501).await;
        let (mut a, mut b) = (mount(&mgr).await, mount(&mgr).await);
        let body = vec![0x5au8; 64 << 10];
        let (mut lost, mut linked_rounds) = (Vec::new(), 0);
        for round in 0..40u64 {
            let (n1, n2) = (format!("n1-{round}"), format!("n2-{round}"));
            let ino = put(&mut a, &n1, &body).await;
            let (unlinked, linked) = futures::join!(dir::unlink(&mut a, 1, OsStr::new(&n1)), async {
                compio::time::sleep(Duration::from_micros(150 * round)).await;
                dir::link(&mut b, ino, 1, OsStr::new(&n2)).await
            });
            unlinked.unwrap();
            linked_rounds += check_link(&mut b, round, linked, &n2, &body, &mut lost).await;
        }
        assert!(lost.is_empty(), "second names that lost their bytes: {lost:?}");
        assert!(linked_rounds > 0, "no round's link succeeded: the race was never run");
    });
}

/// A session writing a file flushes its cached metadata after another
/// session linked the file: the flush keeps the new link, so dropping the
/// first name leaves the second reading the bytes.
#[test]
fn a_writer_s_flush_keeps_a_link_made_meanwhile() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, _dirs) = boot(246, 24601).await;
        let (mut a, mut b) = (mount(&mgr).await, mount(&mgr).await);
        let (ino, _) = dir::create(&mut a, 1, OsStr::new("f"), 0o644).await.unwrap();
        let body = vec![0x3cu8; 256 << 10];
        autumn_fs::write::write(&mut a, ino, 0, &body).await.unwrap();
        dir::link(&mut b, ino, 1, OsStr::new("g")).await.unwrap();
        autumn_fs::write::flush_inode(&mut a, ino, autumn_fs::write::FlushReport::ToApplication)
            .await
            .unwrap();
        assert_eq!(meta::fetch_inode(&mut b, ino).await.unwrap().nlink, 2, "the flush put the old link count back");
        dir::unlink(&mut b, 1, OsStr::new("f")).await.unwrap();
        assert_eq!(read_name(&mut b, "g").await.unwrap(), Some(body));
    });
}

/// A whole-inode write from a copy read before another session's mkdir in
/// the directory (what a chmod does) keeps the directory's link count.
#[test]
fn a_whole_inode_write_keeps_a_link_count_changed_since_its_read() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, _dirs) = boot(247, 24701).await;
        let (mut a, mut b) = (mount(&mgr).await, mount(&mgr).await);
        let (d, _) = dir::mkdir(&mut a, 1, OsStr::new("d"), 0o755).await.unwrap();
        let mut copy = meta::fetch_inode(&mut a, d).await.unwrap();
        dir::mkdir(&mut b, d, OsStr::new("sub"), 0o755).await.unwrap();
        copy.mode = (copy.mode & !0o7777) | 0o700;
        meta::put_inode(&mut a, d, &copy).await.unwrap();
        let m = meta::fetch_inode(&mut b, d).await.unwrap();
        assert_eq!(m.nlink, 3, "the write put the old link count back");
        assert_eq!(m.mode & 0o7777, 0o700);
    });
}

/// A publish replaces a file's only name (retiring the old inode) while
/// another session links the old inode under a second name: the second name
/// either reads the old bytes or is refused, never names reclaimed data.
#[test]
fn a_replace_racing_a_link_of_the_old_file_keeps_its_bytes() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, _dirs) = boot(248, 24801).await;
        let (mut a, mut b) = (mount(&mgr).await, mount(&mgr).await);
        let (old_body, new_body) = (vec![0x11u8; 64 << 10], vec![0x22u8; 1 << 10]);
        let (mut lost, mut linked_rounds) = (Vec::new(), 0);
        for round in 0..40u64 {
            let (name, alias) = (format!("o-{round}"), format!("alias-{round}"));
            let old = put(&mut a, &name, &old_body).await;
            let (_, linked) = futures::join!(put(&mut a, &name, &new_body), async {
                compio::time::sleep(Duration::from_micros(150 * round)).await;
                dir::link(&mut b, old, 1, OsStr::new(&alias)).await
            });
            linked_rounds += check_link(&mut b, round, linked, &alias, &old_body, &mut lost).await;
        }
        assert!(lost.is_empty(), "second names that lost their bytes: {lost:?}");
        assert!(linked_rounds > 0, "no round's link succeeded: the race was never run");
    });
}
