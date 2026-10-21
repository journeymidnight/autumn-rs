//! A link count change whose reply is lost after it landed, the inode then
//! rewritten by another session before the change is settled: the change
//! must not be applied a second time.

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

/// Session A unlinks one of two names; its count-down lands but the reply
/// is lost (failpoint), and session B rewrites the inode (a chmod) before A
/// looks again. A must not count down a second time: the other name keeps
/// the file.
#[test]
fn a_lost_reply_is_not_counted_twice() {
    use std::sync::atomic::Ordering::Relaxed;
    use autumn_fs::meta::CHANGE_LINKS_TEST_LOST_REPLY as HOLD;
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, _dirs) = boot(249, 24901).await;
        let (mut a, mut b) = (mount(&mgr).await, mount(&mgr).await);
        let body = vec![0x77u8; 64 << 10];
        let ino = put(&mut a, "n1", &body).await;
        dir::link(&mut a, ino, 1, OsStr::new("n2")).await.unwrap();
        HOLD.store(1, Relaxed);
        let (unlinked, ()) = futures::join!(dir::unlink(&mut a, 1, OsStr::new("n1")), async {
            let start = std::time::Instant::now();
            while HOLD.load(Relaxed) != 2 {
                assert!(start.elapsed() < Duration::from_secs(10), "the count-down never reached the hold");
                compio::time::sleep(Duration::from_millis(5)).await;
            }
            let mut m = meta::fetch_inode(&mut b, ino).await.unwrap();
            m.mode = (m.mode & !0o7777) | 0o600;
            meta::put_inode(&mut b, ino, &m).await.unwrap();
            HOLD.store(0, Relaxed);
        });
        let err = unlinked.expect_err("an undecidable outcome was settled by counting down again");
        assert!(err.to_string().contains("outcome unknown"), "{err:#}");
        assert_eq!(meta::fetch_inode(&mut b, ino).await.unwrap().nlink, 1);
        assert_eq!(read_name(&mut b, "n2").await.unwrap(), Some(body));
    });
}
