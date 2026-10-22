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

/// The failpoints are process-wide.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Session A unlinks one of two names; its count-down lands but the reply
/// is lost (failpoint), and session B rewrites the inode (a chmod) before A
/// looks again. A must not count down a second time: the other name keeps
/// the file.
#[test]
fn a_lost_reply_is_not_counted_twice() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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

/// Session A unlinks a file's only name; the count reaches 0 but the reply
/// is lost, and session B rewrites the inode before A looks again. A cannot
/// tell its own landing apart, but a count at 0 has no name left: the file
/// is reclaimed, not leaked.
#[test]
fn a_lost_reply_at_zero_still_reclaims() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    use std::sync::atomic::Ordering::Relaxed;
    use autumn_fs::meta::CHANGE_LINKS_TEST_LOST_REPLY as HOLD;
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (mgr, _dirs) = boot(250, 25001).await;
        let (mut a, mut b) = (mount(&mgr).await, mount(&mgr).await);
        let ino = put(&mut a, "only", &vec![0x44u8; 64 << 10]).await;
        HOLD.store(1, Relaxed);
        let (unlinked, ()) = futures::join!(dir::unlink(&mut a, 1, OsStr::new("only")), async {
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
        unlinked.unwrap();
        assert!(
            b.kv_get_opt(&autumn_fs::key::inode_key(ino)).await.unwrap().is_none(),
            "a nameless inode at count 0 was left behind"
        );
    });
}

/// Renames "from" to "to" with the failpoint `fault` set on the publish of
/// the new name; the file ends with exactly the name "to".
async fn rename_under_fault(base: u16, fault: u8) {
    use std::sync::atomic::Ordering::Relaxed;
    use autumn_fs::dir::SETTLE_TEST_FAULT;
    let (mgr, _dirs) = boot(base, base as u64 * 100 + 1).await;
    let mut a = mount(&mgr).await;
    let body = vec![0x55u8; 4 << 10];
    let ino = put(&mut a, "from", &body).await;
    SETTLE_TEST_FAULT.store(fault, Relaxed);
    dir::rename(&mut a, 1, OsStr::new("from"), 1, OsStr::new("to")).await.unwrap();
    assert_eq!(SETTLE_TEST_FAULT.load(Relaxed), 0, "the failpoint never fired");
    if fault == autumn_fs::dir::SETTLE_TEST_LANDS_LATE {
        assert!(
            autumn_fs::dir::SETTLE_TEST_LATE_LANDED.swap(false, Relaxed),
            "the late copy never landed"
        );
    }
    assert_eq!(dir::lookup_opt(&mut a, 1, OsStr::new("to")).await.unwrap().map(|(i, _)| i), Some(ino));
    assert!(
        dir::lookup_opt(&mut a, 1, OsStr::new("from")).await.unwrap().is_none(),
        "the old name is back beside the new one"
    );
    assert_eq!(meta::fetch_inode(&mut a, ino).await.unwrap().nlink, 1);
    assert_eq!(read_name(&mut a, "to").await.unwrap(), Some(body));
}

/// A rename whose publish of the new name comes back with an unknown
/// outcome before it reached the server: the name still reads what was
/// expected, so the rename sends it again and completes; the file is not
/// left without a name.
#[test]
fn a_rename_publish_of_unknown_outcome_is_settled() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    compio::runtime::Runtime::new()
        .unwrap()
        .block_on(rename_under_fault(251, autumn_fs::dir::SETTLE_TEST_UNSENT));
}

/// The unknown publish lands late, after the re-read and before the copy
/// sent again, which is then refused by it: the refusal is settled by
/// reading, so the rename completes instead of putting the old name back
/// beside the new one (two names on a count of one).
#[test]
fn a_late_landing_publish_is_not_taken_for_a_refusal() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    compio::runtime::Runtime::new()
        .unwrap()
        .block_on(rename_under_fault(252, autumn_fs::dir::SETTLE_TEST_LANDS_LATE));
}
