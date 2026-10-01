//! A sealed log extent holding only small values must become reclaimable once
//! they are flushed.
//!
//! A value up to `VALUE_THROTTLE` (4 KiB) is inline: the SST carries the value
//! itself, and its WAL record in the log stream is needed only until the
//! memtable is flushed. Compaction recorded discards only for dropped
//! ValuePointers, and an inline entry does not say where its WAL record is, so
//! nothing ever counted those bytes as dead. On a real cluster, partitions that
//! had held benchmark data (4 KiB values, all deleted and compacted away) kept
//! ~200 GB of sealed log extents with a discard of zero; GC answered "no
//! eligible extents to reclaim" every time. The flush now records the dead
//! WAL bytes of each extent in the SST's discard map.
//!
//! The extent also holds one value above the threshold, still live: GC must
//! relocate it before punching, and it must survive a reopen.
//!
//! The same must hold for records that reach the memtable by WAL replay after
//! a crash rather than by a write (the second test kills the PS before any
//! flush).

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{self, StreamInfoReq, StreamInfoResp, MSG_STREAM_INFO};
use autumn_rpc::partition_rpc;

use support::*;

const PART: u64 = 941;
const PART_REPLAYED: u64 = 942;
const SMALL_KEYS: u32 = 200;
/// Inline: at most `VALUE_THROTTLE`.
const SMALL_LEN: usize = 1024;
/// Above `VALUE_THROTTLE`: stored behind a ValuePointer, live in the log.
const BIG_LEN: usize = 8 * 1024;

async fn log_extents(mgr: &RpcClient, stream_id: u64) -> Vec<u64> {
    let resp = mgr
        .call(
            MSG_STREAM_INFO,
            manager_rpc::rkyv_encode(&StreamInfoReq {
                stream_ids: vec![stream_id],
            }),
        )
        .await
        .expect("stream_info rpc");
    let resp: StreamInfoResp = manager_rpc::rkyv_decode(&resp).expect("decode StreamInfoResp");
    assert_eq!(resp.code, manager_rpc::CODE_OK, "stream_info: {}", resp.message);
    resp.streams
        .into_iter()
        .find(|(id, _)| *id == stream_id)
        .expect("stream in response")
        .1
        .extent_ids
}

async fn roll_tails(ps: &RpcClient, part_id: u64, entries: Vec<(u64, u64)>) -> u32 {
    let resp = ps
        .call(
            partition_rpc::MSG_ROLL_TAILS,
            partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq { part_id, entries }),
        )
        .await
        .expect("roll_tails rpc");
    let resp: partition_rpc::RollTailsResp =
        partition_rpc::rkyv_decode(&resp).expect("decode RollTailsResp");
    assert_eq!(resp.code, partition_rpc::CODE_OK, "roll_tails: {}", resp.message);
    resp.rolled
}

async fn discards_on(ps: &RpcClient, part_id: u64, extent_id: u64) -> i64 {
    let resp = ps
        .call(
            partition_rpc::MSG_GET_DISCARDS,
            partition_rpc::rkyv_encode(&partition_rpc::GetDiscardsReq { part_id }),
        )
        .await
        .expect("get discards");
    let r: partition_rpc::GetDiscardsResp =
        partition_rpc::rkyv_decode(&resp).expect("decode GetDiscardsResp");
    assert_eq!(r.code, partition_rpc::CODE_OK, "get discards: {}", r.message);
    r.discards
        .iter()
        .filter(|(eid, _)| *eid == extent_id)
        .map(|(_, b)| *b)
        .sum()
}

fn small_value(i: u32) -> Vec<u8> {
    let mut v = format!("small{i:04}").into_bytes();
    v.resize(SMALL_LEN, b's');
    v
}

async fn assert_all_readable(ps: &RpcClient, part_id: u64, big: &[u8]) {
    for i in 0..SMALL_KEYS {
        let r = ps_get(ps, part_id, format!("k{i:04}").as_bytes()).await;
        assert_eq!(r.code, partition_rpc::CODE_OK, "k{i:04}: {}", r.message);
        assert_eq!(r.value, small_value(i), "k{i:04} value");
    }
    let r = ps_get(ps, part_id, b"big").await;
    assert_eq!(r.code, partition_rpc::CODE_OK, "big: {}", r.message);
    assert_eq!(r.value, big, "big value");
}

/// Seal E0 and write once into the new tail, so a flush puts the durable
/// checkpoint past E0 and E0 is strictly before the replay floor.
async fn seal_e0(mgr: &RpcClient, ps: &RpcClient, part_id: u64, log: u64, e0: u64) {
    assert_eq!(roll_tails(ps, part_id, vec![(log, e0)]).await, 1, "roll the log tail");
    let ext = log_extents(mgr, log).await;
    assert_eq!(ext.len(), 2, "E0 sealed plus a fresh tail: {ext:?}");
    ps_put(ps, part_id, b"post-roll", b"x").await;
}

/// Flush, then require the flush to have counted E0's inline WAL as dead and
/// auto GC (default ratio) to reclaim E0.
async fn flush_and_gc(mgr: &RpcClient, ps: &RpcClient, part_id: u64, log: u64, e0: u64) {
    ps_flush(ps, part_id).await;

    let dead = discards_on(ps, part_id, e0).await;
    let small_wal = (SMALL_KEYS as usize * SMALL_LEN) as i64;
    assert!(
        dead >= small_wal,
        "the flush must count E0's inline WAL records as dead: {dead} < {small_wal}"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut ext = log_extents(mgr, log).await;
    while ext.contains(&e0) && std::time::Instant::now() < deadline {
        ps_gc(ps, part_id).await;
        compio::time::sleep(Duration::from_millis(500)).await;
        ext = log_extents(mgr, log).await;
    }
    assert!(!ext.contains(&e0), "auto GC must reclaim E0: log extents {ext:?}");
}

#[test]
fn gc_reclaims_a_log_extent_of_flushed_inline_values() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 73).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, PART, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        let (ps_stop, ps_join) = start_partition_server_stoppable(73, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // E0: the small values' WAL records plus one live ValuePointer value.
        let big = vec![0xb1_u8; BIG_LEN];
        ps_put(&ps, PART, b"big", &big).await;
        for i in 0..SMALL_KEYS {
            ps_put(&ps, PART, format!("k{i:04}").as_bytes(), &small_value(i)).await;
        }
        let e0 = *log_extents(&mgr, log).await.last().expect("log tail");

        seal_e0(&mgr, &ps, PART, log, e0).await;
        flush_and_gc(&mgr, &ps, PART, log, e0).await;

        assert_all_readable(&ps, PART, &big).await;

        // The relocated value and the inline ones must survive a reopen.
        drop(ps);
        ps_stop.shutdown();
        ps_join.join().expect("join PS");
        let ps2_addr = pick_addr();
        start_partition_server(73, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while ps_get(&ps2, PART, b"post-roll").await.code != partition_rpc::CODE_OK {
            assert!(std::time::Instant::now() < deadline, "reopened partition never served");
            compio::time::sleep(Duration::from_millis(200)).await;
        }
        assert_all_readable(&ps2, PART, &big).await;
    });
}

/// Build first: `cargo build -p autumn-server --bins` (the PS runs as a child
/// process so it can be killed without the drain's flush).
#[test]
fn gc_reclaims_inline_values_that_came_back_by_wal_replay() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 74).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, PART_REPLAYED, log, row, meta, b"a", b"z").await;

        let ps1_addr = pick_addr();
        let mut ps1 = start_partition_server_killable(74, mgr_addr, ps1_addr);
        let ps = RpcClient::connect(ps1_addr).await.expect("connect ps1");
        let big = vec![0xb2_u8; BIG_LEN];
        ps_put(&ps, PART_REPLAYED, b"big", &big).await;
        for i in 0..SMALL_KEYS {
            ps_put(&ps, PART_REPLAYED, format!("k{i:04}").as_bytes(), &small_value(i)).await;
        }
        let e0 = *log_extents(&mgr, log).await.last().expect("log tail");
        // E0 is sealed before the crash: a PS that opens with E0 as its open
        // tail caches it unsealed and GC would not see the seal until another
        // restart (a separate matter, not what this test is about).
        seal_e0(&mgr, &ps, PART_REPLAYED, log, e0).await;
        // No flush: everything above is only in the WAL.
        drop(ps);
        ps1.kill();

        let ps2_addr = pick_addr();
        let _ps2 = start_partition_server_killable(74, mgr_addr, ps2_addr);
        let ps = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        assert_all_readable(&ps, PART_REPLAYED, &big).await;

        flush_and_gc(&mgr, &ps, PART_REPLAYED, log, e0).await;
        assert_all_readable(&ps, PART_REPLAYED, &big).await;
    });
}
