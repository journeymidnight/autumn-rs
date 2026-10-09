//! `gc_debt_bytes` counts only what GC may take: dead bytes in sealed log
//! extents strictly before the replay floor. The floor extent and everything
//! after it are replayed on reopen, so the punch guard refuses them; counted
//! as debt they made the advisory dispatch a GC that answered "no eligible
//! extents to reclaim", every cooldown. Their dead bytes are reported in
//! `open_tail_dead_bytes` instead, so the two still sum to every dead byte.
//!
//! The partition: values written and mostly deleted in extent E, compacted
//! (E's discard recorded, the cursor in E), one more write in E that no flush
//! covers, then E rolls. E is sealed with records past the cursor: protected.
//! Once a flush and a compaction move the cursor past E, its bytes are debt.
//!
//! E is the log's first extent, so the floor sits at position 0 here: the
//! first phase cannot tell "the floor extent is protected" from "floor 0
//! protects everything", the second (floor moved past E) can.
//! `background::wal_debt_tests` covers a floor in the middle of the log.
//!
//! Ablation: splitting at the tail instead of the floor (the old gauges)
//! reports E's garbage as `gc_debt_bytes`.

mod support;

use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::{manager_rpc, partition_rpc};
use autumn_stream::{ConnPool, StreamClient};

use support::*;

const PART: u64 = 1351;
const BIG_LEN: usize = 64 * 1024;
const BIG_KEYS: u8 = 32;
const DELETED: u8 = 24;

async fn stream_client(mgr_addr: std::net::SocketAddr) -> Rc<StreamClient> {
    StreamClient::connect(
        &mgr_addr.to_string(),
        "gc-debt-replay-floor".to_string(),
        128 * 1024 * 1024,
        Rc::new(ConnPool::new()),
    )
    .await
    .expect("stream client")
}

async fn extents(sc: &StreamClient, stream: u64) -> Vec<u64> {
    sc.get_stream_info(stream)
        .await
        .expect("stream info")
        .extent_ids
}

async fn last_cursor(sc: &StreamClient, meta: u64) -> (u64, u64) {
    let eid = *extents(sc, meta).await.last().expect("meta extent");
    let (payload, _) = sc
        .read_bytes_from_extent(eid, 0, 0)
        .await
        .expect("read meta");
    let locs = decode_last_table_locations(&payload);
    (locs.vp_extent_id, locs.vp_offset)
}

async fn discards(ps: &RpcClient) -> Vec<(u64, i64)> {
    let resp = ps
        .call(
            partition_rpc::MSG_GET_DISCARDS,
            partition_rpc::rkyv_encode(&partition_rpc::GetDiscardsReq { part_id: PART }),
        )
        .await
        .expect("get discards");
    let r: partition_rpc::GetDiscardsResp = partition_rpc::rkyv_decode(&resp).expect("decode");
    assert_eq!(
        r.code,
        partition_rpc::CODE_OK,
        "get discards: {}",
        r.message
    );
    r.discards
}

/// (gc_debt_bytes, open_tail_dead_bytes) as the manager last received them.
async fn reported(mgr: &RpcClient) -> Option<(u64, u64)> {
    let resp = mgr
        .call(
            manager_rpc::MSG_GET_PARTITION_DETAIL,
            manager_rpc::rkyv_encode(&manager_rpc::GetPartitionDetailReq { part_id: PART }),
        )
        .await
        .ok()?;
    let r: manager_rpc::GetPartitionDetailResp = manager_rpc::rkyv_decode(&resp).ok()?;
    (r.load.part_id == PART).then_some((r.load.gc_debt_bytes, r.load.open_tail_dead_bytes))
}

/// Waits for a report satisfying `want`; returns it.
async fn wait_reported(mgr: &RpcClient, what: &str, want: impl Fn(u64, u64) -> bool) -> (u64, u64) {
    let started = Instant::now();
    let mut last = None;
    while started.elapsed() < Duration::from_secs(45) {
        if let Some((debt, protected)) = reported(mgr).await {
            if want(debt, protected) {
                return (debt, protected);
            }
            last = Some((debt, protected));
        }
        compio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("{what}: last report (gc_debt, open_tail_dead) = {last:?}");
}

async fn roll_log(ps: &RpcClient, log: u64, tail: u64) {
    let resp = ps
        .call(
            partition_rpc::MSG_ROLL_TAILS,
            partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq {
                part_id: PART,
                entries: vec![(log, tail)],
            }),
        )
        .await
        .expect("roll_tails rpc");
    let resp: partition_rpc::RollTailsResp = partition_rpc::rkyv_decode(&resp).expect("decode");
    assert_eq!(resp.rolled, 1, "roll_tails: {}", resp.message);
}

#[test]
fn garbage_in_the_replay_window_is_not_gc_debt() {
    let mgr_addr = pick_addr();
    let en_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);
    let (log, meta) = compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .expect("mgr");
        let _ = register_node(&mgr, &en_addr.to_string(), "uuid-gc-debt-floor").await;
        let (log, row, meta) = (
            create_stream(&mgr, 1).await,
            create_stream(&mgr, 1).await,
            create_stream(&mgr, 1).await,
        );
        upsert_partition(&mgr, PART, log, row, meta, b"", b"\xff").await;
        (log, meta)
    });

    let ps_addr = pick_addr();
    let (stop, join) = start_partition_server_stoppable(1, mgr_addr, ps_addr);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .expect("mgr");
        let ps = RpcClient::connect(ps_addr).await.expect("ps");
        let sc = stream_client(mgr_addr).await;
        for i in 0..BIG_KEYS {
            ps_put(
                &ps,
                PART,
                format!("big-{i:02}").as_bytes(),
                &vec![i; BIG_LEN],
            )
            .await;
        }
        for i in 0..DELETED {
            let r = ps_delete(&ps, PART, format!("big-{i:02}").as_bytes()).await;
            assert_eq!(r.code, partition_rpc::CODE_OK);
        }
        let e = *extents(&sc, log).await.last().unwrap();
        let dead_on_e = |d: Vec<(u64, i64)>| -> u64 {
            d.iter()
                .filter(|(eid, _)| *eid == e)
                .map(|(_, b)| (*b).max(0) as u64)
                .sum()
        };
        let killed = DELETED as u64 * BIG_LEN as u64;
        ps_compact(&ps, PART).await;
        let started = Instant::now();
        while dead_on_e(discards(&ps).await) < killed {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "the compaction never recorded E's garbage"
            );
            compio::time::sleep(Duration::from_millis(100)).await;
        }
        // A write past the cursor, in E, that no flush covers.
        ps_put(&ps, PART, b"unflushed", b"v").await;
        roll_log(&ps, log, e).await;
        assert_eq!(last_cursor(&sc, meta).await.0, e, "the cursor sits in E");

        let dead_e = dead_on_e(discards(&ps).await);

        // Before the roll E was the open tail, which reads the same way under
        // the old gauges; a report from then must not pass this. Hold the
        // answer across more than one GC tick (5-7 s) plus one report (5 s).
        wait_reported(&mgr, "E protected, its garbage not debt", |d, p| {
            d == 0 && p >= dead_e
        })
        .await;
        let held = Instant::now();
        while held.elapsed() < Duration::from_secs(15) {
            let r = reported(&mgr).await;
            assert!(
                r.is_some_and(|(d, p)| d == 0 && p >= dead_e),
                "E's garbage reported as debt: (gc_debt, open_tail_dead) = {r:?}"
            );
            compio::time::sleep(Duration::from_millis(500)).await;
        }

        // Move the cursor past E: its garbage becomes debt.
        ps_flush(&ps, PART).await;
        let before = last_cursor(&sc, meta).await;
        ps_compact(&ps, PART).await;
        let started = Instant::now();
        while last_cursor(&sc, meta).await.0 == before.0 {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "the cursor never left E"
            );
            compio::time::sleep(Duration::from_millis(100)).await;
        }
        wait_reported(&mgr, "past the floor, E's garbage is debt", |d, _| {
            d >= killed
        })
        .await;
    });
    stop.shutdown();
    join.join().unwrap();
}
