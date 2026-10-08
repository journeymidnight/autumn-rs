//! A major compaction re-counts the dead bytes of every sealed log extent
//! behind its inputs' cursor: the extent's sealed length minus the values it
//! keeps alive there.
//!
//! The discard map is a running sum (each write's dead WAL bytes, each
//! dropped value), so a byte no write ever counted stays uncounted — WAL
//! written before flushes recorded their dead bytes is all of it, and auto GC
//! never takes such an extent. The test writes with that tally switched off
//! (test failpoint), and the major compaction must then put the exact dead
//! bytes on the extent so auto GC at the default ratio reclaims it.
//!
//! Ablation: skipping the re-count leaves the extent's discard at 0.
//! Own test binary: the failpoint is process-global.

mod support;

use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

const PART: u64 = 1331;
const SMALL_KEYS: u32 = 300;
/// Inline: at most `VALUE_THROTTLE`.
const SMALL_LEN: usize = 1024;
/// Above `VALUE_THROTTLE`: stored behind a ValuePointer in the log.
const BIG_LEN: usize = 8 * 1024;
const BIG_KEYS: u8 = 4;

async fn stream_client(mgr_addr: std::net::SocketAddr) -> Rc<StreamClient> {
    StreamClient::connect(
        &mgr_addr.to_string(),
        "compact-recount-discard".to_string(),
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

async fn discards_on(ps: &RpcClient, extent_id: u64) -> i64 {
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
        .iter()
        .filter(|(e, _)| *e == extent_id)
        .map(|(_, b)| *b)
        .sum()
}

fn small(i: u32) -> Vec<u8> {
    let mut v = format!("s{i:04}").into_bytes();
    v.resize(SMALL_LEN, b's');
    v
}

#[test]
fn a_major_compaction_recounts_uncounted_wal_as_dead() {
    let mgr_addr = pick_addr();
    let en_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);
    let log = compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .expect("mgr");
        let _ = register_node(&mgr, &en_addr.to_string(), "uuid-recount-discard").await;
        let (log, row, meta) = (
            create_stream(&mgr, 1).await,
            create_stream(&mgr, 1).await,
            create_stream(&mgr, 1).await,
        );
        upsert_partition(&mgr, PART, log, row, meta, b"", b"\xff").await;
        log
    });

    let ps_addr = pick_addr();
    let (stop, join) = start_partition_server_stoppable(1, mgr_addr, ps_addr);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let ps = RpcClient::connect(ps_addr).await.expect("ps");
        let sc = stream_client(mgr_addr).await;

        autumn_partition_server::set_wal_dead_off(true);
        for i in 0..BIG_KEYS {
            ps_put(&ps, PART, format!("big-{i}").as_bytes(), &vec![i; BIG_LEN]).await;
        }
        for i in 0..SMALL_KEYS {
            ps_put(&ps, PART, format!("small-{i:04}").as_bytes(), &small(i)).await;
        }
        ps_flush(&ps, PART).await;
        autumn_partition_server::set_wal_dead_off(false);

        let e = *extents(&sc, log).await.last().unwrap();
        assert_eq!(
            discards_on(&ps, e).await,
            0,
            "the failpoint must leave the WAL uncounted"
        );

        let resp = ps
            .call(
                partition_rpc::MSG_ROLL_TAILS,
                partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq {
                    part_id: PART,
                    entries: vec![(log, e)],
                }),
            )
            .await
            .expect("roll_tails rpc");
        let resp: partition_rpc::RollTailsResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(resp.rolled, 1, "roll_tails: {}", resp.message);
        let sealed_length = sc
            .get_extent_info(e)
            .await
            .expect("extent info")
            .sealed_length;
        assert!(sealed_length > 0);

        ps_compact(&ps, PART).await;
        let live = BIG_KEYS as i64 * BIG_LEN as i64;
        let started = Instant::now();
        loop {
            let dead = discards_on(&ps, e).await;
            if dead != 0 {
                assert_eq!(
                    dead,
                    sealed_length as i64 - live,
                    "dead = sealed length - live values"
                );
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "the major compaction left extent {e} with no dead bytes"
            );
            compio::time::sleep(Duration::from_millis(100)).await;
        }

        let started = Instant::now();
        while extents(&sc, log).await.contains(&e) {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "auto GC never reclaimed {e}"
            );
            ps_gc(&ps, PART).await;
            compio::time::sleep(Duration::from_millis(300)).await;
        }
        for i in 0..BIG_KEYS {
            let r = ps_get(&ps, PART, format!("big-{i}").as_bytes()).await;
            assert_eq!(r.code, partition_rpc::CODE_OK, "big-{i}: {}", r.message);
            assert!(r.value == vec![i; BIG_LEN], "big-{i} value");
        }
        for i in (0..SMALL_KEYS).step_by(37) {
            let r = ps_get(&ps, PART, format!("small-{i:04}").as_bytes()).await;
            assert_eq!(
                r.code,
                partition_rpc::CODE_OK,
                "small-{i:04}: {}",
                r.message
            );
            assert_eq!(r.value, small(i));
        }
    });
    stop.shutdown();
    join.join().unwrap();
}
