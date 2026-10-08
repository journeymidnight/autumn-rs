//! A minor compaction moves a cursor at the end of a sealed log extent too,
//! against the log as it is when it publishes: here the log rolls while the
//! compaction runs, after it fetched its extent list.
//!
//! The minor compaction is the PS's own auto-trim (more than 32 SSTs). Own
//! test binary: the compaction hold is process-global.

mod support;

use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_partition_server::background::{compaction_held_count, set_compaction_hold};
use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

const PART: u64 = 1321;
/// One past the auto-trim threshold (`MAX_SST_BEFORE_AUTO_COMPACT`).
const FLUSHES: usize = 33;

async fn stream_client(mgr_addr: std::net::SocketAddr) -> Rc<StreamClient> {
    StreamClient::connect(
        &mgr_addr.to_string(),
        "compact-advance-anchor-minor".to_string(),
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

async fn last_checkpoint(sc: &StreamClient, meta: u64) -> ((u64, u64), usize) {
    let eid = *extents(sc, meta).await.last().expect("meta extent");
    let (payload, _) = sc
        .read_bytes_from_extent(eid, 0, 0)
        .await
        .expect("read meta");
    let locs = decode_last_table_locations(&payload);
    ((locs.vp_extent_id, locs.vp_offset), locs.locs.len())
}

#[test]
fn a_minor_compaction_moves_a_cursor_at_a_sealed_end() {
    set_compaction_hold(false);
    let mgr_addr = pick_addr();
    let en_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);
    let (log, meta) = compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .expect("mgr");
        let _ = register_node(&mgr, &en_addr.to_string(), "uuid-advance-anchor-minor").await;
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
        let held = compaction_held_count();
        set_compaction_hold(true);
        for f in 0..FLUSHES {
            for i in 0..10 {
                ps_put(&ps, PART, format!("k-{f:02}-{i}").as_bytes(), &[b'v'; 256]).await;
            }
            ps_flush(&ps, PART).await;
        }
        assert!(
            poll_until(Duration::from_secs(60), Duration::from_millis(20), || {
                compaction_held_count() > held
            })
            .await,
            "the auto-trim never started"
        );
        let (flushed, tables_before) = last_checkpoint(&sc, meta).await;
        let sealed = *extents(&sc, log).await.last().unwrap();
        assert_eq!(flushed.0, sealed);

        let resp = ps
            .call(
                partition_rpc::MSG_ROLL_TAILS,
                partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq {
                    part_id: PART,
                    entries: vec![(log, sealed)],
                }),
            )
            .await
            .expect("roll_tails rpc");
        let resp: partition_rpc::RollTailsResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(resp.rolled, 1, "roll_tails: {}", resp.message);
        let next = *extents(&sc, log).await.last().unwrap();

        set_compaction_hold(false);
        let started = Instant::now();
        let cursor = loop {
            let (cursor, tables) = last_checkpoint(&sc, meta).await;
            if tables < tables_before {
                break cursor;
            }
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "the auto-trim never published"
            );
            compio::time::sleep(Duration::from_millis(100)).await;
        };
        assert_eq!(
            cursor,
            (next, 0),
            "the minor compaction left the cursor on sealed {sealed}"
        );
    });
    stop.shutdown();
    join.join().unwrap();
}
