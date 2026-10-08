//! A checkpoint cursor naming a reclaimed extent is rebuilt by the next
//! compaction instead of republished.
//!
//! A cursor at `(T, 0)` (a compaction moved it onto the empty tail, or a
//! freeze drained an empty tail) stops resolving once T rolls empty and the
//! sealed-empty sweep or GC reclaims it. Republished as is, it kept GC at the
//! MIN-over-SST floor until the next flush; the compaction now rebuilds the
//! cursor from the tables it lists, which resolves.
//!
//! Ablation: `durable_cursor_swept` returning false leaves `(T, 0)` published.

mod support;

use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

const PART: u64 = 1341;
const BIG_LEN: usize = 16 * 1024;

async fn stream_client(mgr_addr: std::net::SocketAddr) -> Rc<StreamClient> {
    StreamClient::connect(
        &mgr_addr.to_string(),
        "compact-swept-anchor".to_string(),
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

/// Major-compact and wait for a checkpoint other than `before`.
async fn compact_until_changed(
    ps: &RpcClient,
    sc: &StreamClient,
    meta: u64,
    before: (u64, u64),
) -> (u64, u64) {
    ps_compact(ps, PART).await;
    let started = Instant::now();
    loop {
        let (c, _) = last_checkpoint(sc, meta).await;
        if c != before {
            return c;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the compaction republished cursor {before:?}"
        );
        compio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[test]
fn a_cursor_naming_a_reclaimed_extent_is_rebuilt() {
    let mgr_addr = pick_addr();
    let en_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);
    let (log, meta) = compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .expect("mgr");
        let _ = register_node(&mgr, &en_addr.to_string(), "uuid-swept-anchor").await;
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
        for i in 0..16u8 {
            ps_put(&ps, PART, format!("k-{i:02}").as_bytes(), &vec![i; BIG_LEN]).await;
        }
        ps_flush(&ps, PART).await;
        let e = *extents(&sc, log).await.last().unwrap();
        roll_log(&ps, log, e).await;
        let t = *extents(&sc, log).await.last().unwrap();
        let (flushed, _) = last_checkpoint(&sc, meta).await;
        assert_eq!(compact_until_changed(&ps, &sc, meta, flushed).await, (t, 0));

        // T rolls empty and is reclaimed: (T, 0) no longer resolves.
        roll_log(&ps, log, t).await;
        let resp = ps
            .call(
                partition_rpc::MSG_MAINTENANCE,
                partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                    part_id: PART,
                    op: partition_rpc::MAINTENANCE_FORCE_GC,
                    extent_ids: vec![t],
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
        let started = Instant::now();
        while extents(&sc, log).await.contains(&t) {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "GC never reclaimed empty {t}"
            );
            compio::time::sleep(Duration::from_millis(200)).await;
        }

        let rebuilt = compact_until_changed(&ps, &sc, meta, (t, 0)).await;
        let log_now = extents(&sc, log).await;
        assert!(
            log_now.contains(&rebuilt.0),
            "the rebuilt cursor {rebuilt:?} does not resolve in {log_now:?}"
        );
        // E ends at the tables' newest boundary and is followed only by the tail.
        assert_eq!(rebuilt, (*log_now.last().unwrap(), 0));
        for i in 0..16u8 {
            let r = ps_get(&ps, PART, format!("k-{i:02}").as_bytes()).await;
            assert_eq!(r.code, partition_rpc::CODE_OK, "k-{i:02}: {}", r.message);
            assert!(r.value == vec![i; BIG_LEN]);
        }
    });
    stop.shutdown();
    join.join().unwrap();
}
