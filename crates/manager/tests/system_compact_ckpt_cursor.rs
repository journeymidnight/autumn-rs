//! A compaction fetches the log's extent list when it starts. A flush that
//! rolls the log while it runs publishes a cursor in an extent that list does
//! not name. The compaction's checkpoint must not fall behind that cursor: GC
//! raises its floor to the newest flush cursor, so a record behind it names an
//! extent GC may delete, and the next open has no cursor to replay from.
//!
//! Own test binary: the compaction hold is process-global.

mod support;

use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

const PART: u64 = 1301;

async fn stream_client(mgr_addr: std::net::SocketAddr) -> Rc<StreamClient> {
    StreamClient::connect(
        &mgr_addr.to_string(),
        "compact-ckpt-cursor".to_string(),
        128 * 1024 * 1024,
        Rc::new(ConnPool::new()),
    )
    .await
    .expect("stream client")
}

async fn extents(sc: &StreamClient, stream: u64) -> Vec<u64> {
    sc.get_stream_info(stream).await.expect("stream info").extent_ids
}

async fn last_checkpoint(sc: &StreamClient, meta: u64) -> ((u64, u64), usize) {
    let eid = *extents(sc, meta).await.last().expect("meta extent");
    let (payload, _) = sc.read_bytes_from_extent(eid, 0, 0).await.expect("read meta");
    let locs = decode_last_table_locations(&payload);
    ((locs.vp_extent_id, locs.vp_offset), locs.locs.len())
}

async fn put_range(ps: &RpcClient, tag: &str, n: usize) {
    for i in 0..n {
        ps_put(ps, PART, format!("{tag}-{i:05}").as_bytes(), &[b'x'; 512]).await;
    }
}

async fn roll_tails(ps: &RpcClient, entries: Vec<(u64, u64)>) {
    let resp = ps
        .call(
            partition_rpc::MSG_ROLL_TAILS,
            partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq {
                part_id: PART,
                entries,
            }),
        )
        .await
        .expect("roll_tails rpc");
    let resp: partition_rpc::RollTailsResp =
        partition_rpc::rkyv_decode(&resp).expect("decode RollTailsResp");
    assert_eq!(resp.code, partition_rpc::CODE_OK, "roll_tails: {}", resp.message);
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

#[test]
fn a_flush_rolling_the_log_during_a_compaction_keeps_its_cursor() {
    autumn_partition_server::background::set_compaction_hold(false);
    let mgr_addr = pick_addr();
    let en_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);
    let (log, meta) = compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .expect("mgr");
        let _ = register_node(&mgr, &en_addr.to_string(), "uuid-compact-cursor").await;
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
        let ps = RpcClient::connect(ps_addr).await.expect("ps");
        let sc = stream_client(mgr_addr).await;
        put_range(&ps, "a", 400).await;
        ps_flush(&ps, PART).await;
        put_range(&ps, "b", 400).await;
        ps_flush(&ps, PART).await;

        // The compaction starts, fetches the log list, and parks.
        let held = autumn_partition_server::background::compaction_held_count();
        autumn_partition_server::background::set_compaction_hold(true);
        ps_compact(&ps, PART).await;
        assert!(
            poll_until(Duration::from_secs(20), Duration::from_millis(5), || {
                autumn_partition_server::background::compaction_held_count() > held
            })
            .await,
            "the compaction never parked"
        );

        // A flush whose cursor sits in an extent created after that fetch.
        let tail = *extents(&sc, log).await.last().unwrap();
        roll_tails(&ps, vec![(log, tail)]).await;
        put_range(&ps, "c", 400).await;
        ps_flush(&ps, PART).await;
        let (flushed_cursor, tables_before) = last_checkpoint(&sc, meta).await;
        assert_eq!(flushed_cursor.0, *extents(&sc, log).await.last().unwrap());

        autumn_partition_server::background::set_compaction_hold(false);
        let started = Instant::now();
        let compacted_cursor = loop {
            let (cursor, tables) = last_checkpoint(&sc, meta).await;
            if tables < tables_before {
                break cursor;
            }
            assert!(started.elapsed() < Duration::from_secs(60), "compaction never published");
            compio::time::sleep(Duration::from_millis(100)).await;
        };
        assert_eq!(
            compacted_cursor, flushed_cursor,
            "the compaction published a cursor behind the newest flush"
        );

        let log_ids = extents(&sc, log).await;
        let behind = log_ids[..log_ids.len() - 1].to_vec();
        force_gc(&ps, behind.clone()).await;
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(20) {
            let now = extents(&sc, log).await;
            let (cursor, _) = last_checkpoint(&sc, meta).await;
            assert!(
                now.contains(&cursor.0),
                "GC deleted extent {} that the newest checkpoint names (log now {now:?})",
                cursor.0
            );
            if behind.iter().all(|e| !now.contains(e)) {
                break;
            }
            compio::time::sleep(Duration::from_millis(200)).await;
        }
    });
    stop.shutdown();
    join.join().unwrap();
}
