//! The block crcs of a `.dat` are recorded as its appends become durable
//! (`extent-{id}.ck.open`), so its first scrub checks against them instead of
//! trusting what it reads.

mod test_helpers;

use autumn_stream::extent_rpc::{
    PayloadLocation, ScrubTask, CODE_OK, SCRUB_OUTCOME_DESCRIBED, SCRUB_OUTCOME_ROT,
};
use std::path::{Path, PathBuf};
use test_helpers::{pick_addr, start_node, TestConn};

const MIB: usize = 1 << 20;

fn find(dir: &Path, name: &str) -> Option<PathBuf> {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.file_name().is_some_and(|f| f == name) {
                return Some(p);
            }
        }
    }
    None
}

/// `.ck.open`'s slots, read by this test's own parser: a 16-byte header
/// (magic, extent id), then `[crc][!crc32c(extent, block, crc)]` per block.
/// The node creates the file and then writes its header, so a file shorter
/// than the header has nothing written yet (as the node's own reader takes it).
fn written_crcs(dir: &Path, eid: u64) -> Vec<Option<u32>> {
    let Some(path) = find(dir, &format!("extent-{eid}.ck.open")) else {
        return Vec::new();
    };
    let buf = std::fs::read(path).unwrap();
    if buf.len() < 16 {
        return Vec::new();
    }
    assert_eq!(&buf[..8], b"EXTCKO\0\x01");
    assert_eq!(u64::from_le_bytes(buf[8..16].try_into().unwrap()), eid);
    buf[16..]
        .chunks_exact(8)
        .enumerate()
        .map(|(i, s)| {
            let crc = u32::from_le_bytes(s[..4].try_into().unwrap());
            let mut b = eid.to_le_bytes().to_vec();
            b.extend_from_slice(&(i as u64).to_le_bytes());
            b.extend_from_slice(&crc.to_le_bytes());
            (u32::from_le_bytes(s[4..].try_into().unwrap()) == !crc32c::crc32c(&b)).then_some(crc)
        })
        .collect()
}

/// Caught in full parallel runs: the file read between its creation and its
/// header's write.
#[test]
fn a_file_without_its_header_yet_has_nothing_written() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("extent-7.ck.open"), b"").unwrap();
    assert!(written_crcs(dir.path(), 7).is_empty());
}

/// The slots are written after the append's reply: wait until they are.
async fn settled(dir: &Path, eid: u64, blocks: usize) -> Vec<Option<u32>> {
    for _ in 0..100 {
        let got = written_crcs(dir, eid);
        if got.len() >= blocks && got[..blocks].iter().all(Option::is_some) {
            return got;
        }
        compio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    written_crcs(dir, eid)
}

fn bytes(seed: usize, n: usize) -> Vec<u8> {
    (0..n).map(|i| ((i * 31 + seed) % 251) as u8).collect()
}

/// Append `content` in pieces of these sizes (the last takes the rest).
async fn append_all(conn: &TestConn, eid: u64, at: usize, content: &[u8], sizes: &[usize]) {
    let mut off = 0;
    for (k, &n) in sizes.iter().enumerate() {
        let end = if k + 1 == sizes.len() { content.len() } else { (off + n).min(content.len()) };
        let r = conn.append(eid, 1, (at + off) as u64, 30, content[off..end].to_vec()).await;
        assert_eq!(r.code, CODE_OK, "append at {}", at + off);
        off = end;
    }
}

async fn first_scrub(node: &autumn_stream::ExtentNode, eid: u64, len: usize) -> (u8, String) {
    node.test_seal_local(eid, len as u64, 2).await.expect("seal");
    let task = ScrubTask {
        extent_id: eid,
        payload_location: PayloadLocation::InDat.as_byte(),
        shard_index: 0,
        length: len as u64,
        eversion: 2,
        op_id: 0,
    };
    let done = node.test_scrub(vec![task]).await.remove(0);
    (done.outcome, done.message)
}

/// Appends of every size leave each whole block's crc behind, and the first
/// scrub checks the blocks against them, then replaces them with `.ck`.
#[compio::test]
async fn written_blocks_are_recorded_and_checked_by_the_first_scrub() {
    let dir = tempfile::tempdir().unwrap();
    let addr = pick_addr();
    let node = start_node(dir.path(), addr).await;
    let conn = TestConn::new(addr);
    assert_eq!(conn.alloc_extent(7001).await.code, CODE_OK);
    let content = bytes(1, 2 * MIB + 3000);
    append_all(&conn, 7001, 0, &content, &[4096, 4096, 700_000, 1_500_000, 4096, 0]).await;

    let want: Vec<u32> = content.chunks(MIB).take(2).map(crc32c::crc32c).collect();
    assert_eq!(settled(dir.path(), 7001, 2).await, vec![Some(want[0]), Some(want[1])], "before any scrub");

    let (outcome, message) = first_scrub(&node, 7001, content.len()).await;
    assert_eq!(outcome, SCRUB_OUTCOME_DESCRIBED, "{message}");
    assert!(message.contains("2 against the crcs they were written with"), "{message}");
    assert!(find(dir.path(), "extent-7001.ck").is_some());
    assert!(find(dir.path(), "extent-7001.ck.open").is_none(), "replaced by the description");
}

/// A block that rots on disk before the first scrub is found by it: the
/// baseline is what was written, not what is read.
#[compio::test]
async fn rot_before_the_first_scrub_is_found() {
    let dir = tempfile::tempdir().unwrap();
    let addr = pick_addr();
    let node = start_node(dir.path(), addr).await;
    let conn = TestConn::new(addr);
    assert_eq!(conn.alloc_extent(7002).await.code, CODE_OK);
    let content = bytes(2, 2 * MIB + 10);
    append_all(&conn, 7002, 0, &content, &[MIB / 2, 0]).await;
    settled(dir.path(), 7002, 2).await;

    let dat = find(dir.path(), "extent-7002.dat").unwrap();
    let mut raw = std::fs::read(&dat).unwrap();
    raw[MIB + MIB / 2] ^= 0x40;
    std::fs::write(&dat, raw).unwrap();

    let (outcome, message) = first_scrub(&node, 7002, content.len()).await;
    assert_eq!(outcome, SCRUB_OUTCOME_ROT, "{message}");
    assert!(message.contains("block 1") && message.contains("written with"), "{message}");
}

/// A truncate mid-block cuts that block's crc: the block is written again
/// with other bytes and, starting mid-block, is not recorded again — a crc
/// left from before would condemn it.
#[compio::test]
async fn a_truncated_block_keeps_no_stale_crc() {
    let dir = tempfile::tempdir().unwrap();
    let addr = pick_addr();
    let node = start_node(dir.path(), addr).await;
    let conn = TestConn::new(addr);
    assert_eq!(conn.alloc_extent(7003).await.code, CODE_OK);
    let first = bytes(3, 2 * MIB + 100);
    append_all(&conn, 7003, 0, &first, &[0]).await;
    let before = settled(dir.path(), 7003, 2).await;
    assert!(before[0].is_some() && before[1].is_some());

    // The writer's commit is mid block 1: the node truncates there.
    let cut = MIB + 50;
    let second = bytes(4, MIB + 150);
    append_all(&conn, 7003, cut, &second, &[0]).await;
    let len = cut + second.len();
    let after = written_crcs(dir.path(), 7003);
    assert_eq!(after.first().copied().flatten(), before[0], "blocks before the cut keep theirs");
    assert_eq!(after.get(1).copied().flatten(), None, "the cut block's crc is gone");

    let (outcome, message) = first_scrub(&node, 7003, len).await;
    assert_eq!(outcome, SCRUB_OUTCOME_DESCRIBED, "{message}");
}

/// A burst whose fsync failed records nothing: the `.dat` is a link to
/// `/dev/null`, where every write succeeds and `fdatasync` fails.
#[compio::test]
async fn a_burst_whose_fsync_failed_records_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let eid = 7004u64;
    let hashed = dir.path().join(format!("{:02x}", crc32c::crc32c(&eid.to_le_bytes()) & 0xFF));
    std::fs::create_dir_all(&hashed).unwrap();
    std::os::unix::fs::symlink("/dev/null", hashed.join(format!("extent-{eid}.dat"))).unwrap();
    let addr = pick_addr();
    let _node = start_node(dir.path(), addr).await;
    let conn = TestConn::new(addr);
    assert_eq!(conn.alloc_extent(eid).await.code, CODE_OK);
    let r = conn.try_append(eid, 1, 0, 30, bytes(5, 2 * MIB)).await;
    assert!(r.is_err(), "the burst's fsync failed");
    compio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(written_crcs(dir.path(), eid).iter().all(Option::is_none), "nothing recorded");
}
