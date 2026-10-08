//! A compaction moves a checkpoint cursor that sits at the very end of a
//! sealed log extent onto the next extent, so GC can reclaim it.
//!
//! The cursor is set by the last flush. With no write after it, nothing moves
//! it: once the log rolls, the extent it names is sealed, ends exactly at the
//! cursor and holds nothing to replay — yet GC keeps the extent the floor
//! names, so its garbage stays forever. Replaying from `(next, 0)` is the same
//! replay, and a compaction publishes it.
//!
//! The PS runs as a child process (`support::ChildPs`) and is SIGKILLed after
//! GC relocated the live values, so the reopen has to replay them from the new
//! cursor.
//!
//! Ablation: `advance_sealed_anchor` returning its input leaves the cursor on
//! the sealed extent and the test fails waiting for it to move.

mod support;

use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

const PART: u64 = 1311;
const PS_ID: u64 = 83;
/// Above `VALUE_THROTTLE`: stored behind a ValuePointer in the log.
const BIG_LEN: usize = 64 * 1024;
const BIG_KEYS: u8 = 32;
const DELETED: u8 = 24;
const SMALL_KEYS: u32 = 20;

#[test]
fn child_ps() {
    child_ps_main();
}

fn big(tag: u8) -> Vec<u8> {
    vec![tag; BIG_LEN]
}

async fn stream_client(mgr_addr: SocketAddr) -> Rc<StreamClient> {
    StreamClient::connect(
        &mgr_addr.to_string(),
        "compact-advance-anchor".to_string(),
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
    let resp: partition_rpc::RollTailsResp =
        partition_rpc::rkyv_decode(&resp).expect("decode RollTailsResp");
    assert_eq!(
        resp.code,
        partition_rpc::CODE_OK,
        "roll_tails: {}",
        resp.message
    );
    assert_eq!(resp.rolled, 1, "the log tail must roll");
}

async fn force_gc(ps: &RpcClient, extent_ids: Vec<u64>) {
    let resp = ps
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id: PART,
                op: partition_rpc::MAINTENANCE_FORCE_GC,
                extent_ids,
                gc_ratio: None,
                gc_max_size: None,
                gc_stream_debt: None,
                gc_dead_bytes_high: None,
                gc_empty_only: false,
                gc_policy_is_standing: false,
                op_id: 0,
            }),
        )
        .await
        .expect("forcegc");
    let r: partition_rpc::MaintenanceResp = partition_rpc::rkyv_decode(&resp).expect("decode");
    assert_eq!(r.code, partition_rpc::CODE_OK, "forcegc: {}", r.message);
}

async fn check_state(ps: &RpcClient, when: &str) {
    for i in 0..BIG_KEYS {
        let r = ps_get(ps, PART, format!("big-{i:02}").as_bytes()).await;
        if i < DELETED {
            assert_eq!(
                r.code,
                partition_rpc::CODE_NOT_FOUND,
                "big-{i:02} came back {when}"
            );
        } else {
            assert_eq!(
                r.code,
                partition_rpc::CODE_OK,
                "big-{i:02} lost {when}: {}",
                r.message
            );
            assert!(r.value == big(i), "big-{i:02} has the wrong value {when}");
        }
    }
    for i in 0..SMALL_KEYS {
        let r = ps_get(ps, PART, format!("small-{i:03}").as_bytes()).await;
        assert_eq!(r.code, partition_rpc::CODE_OK, "small-{i:03} lost {when}");
        assert_eq!(r.value, format!("v-{i}").into_bytes());
    }
    let r = ps_get(ps, PART, b"after-gc").await;
    assert_eq!(
        r.code,
        partition_rpc::CODE_OK,
        "the write after GC was lost {when}"
    );
    assert!(r.value == big(0xAA));
}

#[test]
fn a_cursor_at_a_sealed_end_moves_on_and_the_extent_is_reclaimed() {
    let mgr_addr = pick_addr();
    let en_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);
    let (log, meta) = compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .expect("mgr");
        let _ = register_node(&mgr, &en_addr.to_string(), "uuid-advance-anchor").await;
        let (log, row, meta) = (
            create_stream(&mgr, 1).await,
            create_stream(&mgr, 1).await,
            create_stream(&mgr, 1).await,
        );
        upsert_partition(&mgr, PART, log, row, meta, b"", b"\xff").await;
        (log, meta)
    });

    let ps_addr = pick_addr();
    let mut child = ChildPs::spawn(PS_ID, mgr_addr, ps_addr, ChildFailpoints::default());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let ps = RpcClient::connect(ps_addr).await.expect("ps");
        let sc = stream_client(mgr_addr).await;
        for i in 0..BIG_KEYS {
            ps_put(&ps, PART, format!("big-{i:02}").as_bytes(), &big(i)).await;
        }
        for i in 0..SMALL_KEYS {
            ps_put(
                &ps,
                PART,
                format!("small-{i:03}").as_bytes(),
                format!("v-{i}").as_bytes(),
            )
            .await;
        }
        for i in 0..DELETED {
            let r = ps_delete(&ps, PART, format!("big-{i:02}").as_bytes()).await;
            assert_eq!(r.code, partition_rpc::CODE_OK, "delete big-{i:02}");
        }
        ps_flush(&ps, PART).await;

        let sealed = *extents(&sc, log).await.last().unwrap();
        let flushed = last_cursor(&sc, meta).await;
        assert_eq!(flushed.0, sealed, "the flush names the tail it wrote");
        roll_log(&ps, log, sealed).await;
        let next = *extents(&sc, log).await.last().unwrap();
        assert_ne!(next, sealed);

        ps_compact(&ps, PART).await;
        let started = Instant::now();
        let cursor = loop {
            let c = last_cursor(&sc, meta).await;
            if c != flushed {
                break c;
            }
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "the compaction left the cursor at the end of sealed extent {sealed}"
            );
            compio::time::sleep(Duration::from_millis(100)).await;
        };
        assert_eq!(cursor, (next, 0));

        force_gc(&ps, vec![sealed]).await;
        let started = Instant::now();
        while extents(&sc, log).await.contains(&sealed) {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "GC never reclaimed extent {sealed}"
            );
            compio::time::sleep(Duration::from_millis(200)).await;
        }
        ps_put(&ps, PART, b"after-gc", &big(0xAA)).await;
        check_state(&ps, "before the crash").await;
    });

    // The live values GC relocated sit in the memtable and in the log past
    // the cursor: only a replay from it brings them back.
    child.kill();
    let ps_addr = pick_addr();
    let _child = ChildPs::spawn(PS_ID, mgr_addr, ps_addr, ChildFailpoints::default());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let sc = stream_client(mgr_addr).await;
        let started = Instant::now();
        let ps = loop {
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "partition did not serve after the crash"
            );
            if let Ok(ps) = RpcClient::connect(ps_addr).await {
                if ps_get(&ps, PART, b"small-000").await.code == partition_rpc::CODE_OK {
                    break ps;
                }
            }
            compio::time::sleep(Duration::from_millis(200)).await;
        };
        check_state(&ps, "after the crash").await;
        let cursor = last_cursor(&sc, meta).await;
        assert!(
            extents(&sc, log).await.contains(&cursor.0),
            "the checkpoint names extent {}, which is not in the log",
            cursor.0
        );
    });
}
