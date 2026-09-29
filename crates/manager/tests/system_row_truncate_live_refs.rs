//! A compaction drops only the row-stream extents no live table references.
//!
//! The truncate point used to come from the table list's order, which is not
//! row-stream order; a cut past a still-referenced extent had the manager delete
//! SSTs the checkpoint lists, and the partition could not reopen. The point is
//! now the first extent, in stream order, that a live table references
//! (`row_truncate_point`, unit-tested with the order that broke). This drives
//! the whole path on a real multi-extent row stream: after every compaction the
//! checkpoint's SSTs must all sit in extents the stream still has, the dead
//! prefix must actually go, and the partition must reopen with every key.

mod support;

use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_partition_server::PartitionServer;
use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc::{self, TableLocations, CODE_OK};
use autumn_stream::{ConnPool, StreamClient};
use support::*;

const PART: u64 = 1201;

fn spawn_ps(mgr: SocketAddr, addr: SocketAddr, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let ps = PartitionServer::connect_with_advertise_and_port(
                1,
                &mgr.to_string(),
                Some(addr.to_string()),
                addr,
            )
            .await
            .expect("connect partition server");
            ps.sync_regions_once().await.expect("sync regions");
            let stop_fut = async move {
                while !stop.load(Ordering::Acquire) {
                    compio::time::sleep(Duration::from_millis(50)).await;
                }
            };
            ps.serve_until_shutdown(addr, stop_fut).await.expect("serve");
        });
    })
}

fn stop_ps(stop: &AtomicBool, join: std::thread::JoinHandle<()>) {
    stop.store(true, Ordering::Release);
    let t0 = Instant::now();
    while !join.is_finished() {
        assert!(t0.elapsed() < Duration::from_secs(30), "PS did not stop");
        std::thread::sleep(Duration::from_millis(50));
    }
    join.join().expect("PS thread panicked");
}

async fn stream_client(mgr: SocketAddr) -> Rc<StreamClient> {
    StreamClient::connect(
        &mgr.to_string(),
        "row-truncate-test".to_string(),
        128 * 1024 * 1024,
        Rc::new(ConnPool::new()),
    )
    .await
    .expect("stream client")
}

async fn row_extents(sc: &StreamClient, row: u64) -> Vec<u64> {
    sc.get_stream_info(row).await.expect("row stream info").extent_ids
}

async fn checkpoint(sc: &StreamClient, meta: u64) -> TableLocations {
    let raw = sc
        .read_last_extent_data(meta)
        .await
        .expect("read meta stream")
        .expect("a checkpoint");
    decode_last_table_locations(&raw)
}

async fn roll_row(ps: &RpcClient, sc: &StreamClient, row: u64) {
    let tail = *row_extents(sc, row).await.last().unwrap();
    let resp = ps
        .call(
            partition_rpc::MSG_ROLL_TAILS,
            partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq {
                part_id: PART,
                entries: vec![(row, tail)],
            }),
        )
        .await
        .expect("roll_tails rpc");
    let r: partition_rpc::RollTailsResp = partition_rpc::rkyv_decode(&resp).expect("decode");
    assert_eq!(r.code, CODE_OK, "roll_tails: {}", r.message);
}

async fn put_flush(ps: &RpcClient, i: u32) {
    ps_put(ps, PART, format!("k{i:03}").as_bytes(), format!("v{i}").as_bytes()).await;
    ps_flush(ps, PART).await;
}

/// Every SST the checkpoint lists must sit in an extent the row stream has.
async fn assert_checkpoint_covered(sc: &StreamClient, row: u64, meta: u64, when: &str) {
    let stream = row_extents(sc, row).await;
    let ck = checkpoint(sc, meta).await;
    let missing: Vec<u64> = ck
        .locs
        .iter()
        .map(|l| l.extent_id)
        .filter(|e| !stream.contains(e))
        .collect();
    assert!(
        missing.is_empty(),
        "{when}: checkpoint lists SSTs in extents {missing:?} the row stream {stream:?} no longer has"
    );
}

#[test]
fn compaction_drops_only_unreferenced_row_extents() {
    let mgr_addr = pick_addr();
    let n1 = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(n1, dir.path().to_path_buf(), 1);
    let (row, meta) = compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("mgr");
        let _ = register_node(&mgr, &n1.to_string(), "uuid-row-truncate").await;
        let log = create_stream(&mgr, 1).await;
        let row = create_stream(&mgr, 1).await;
        let meta = create_stream(&mgr, 1).await;
        upsert_partition(&mgr, PART, log, row, meta, b"", b"\xff").await;
        (row, meta)
    });

    let ps_addr = pick_addr();
    let stop = Arc::new(AtomicBool::new(false));
    let join = spawn_ps(mgr_addr, ps_addr, stop.clone());
    std::thread::sleep(Duration::from_millis(800));

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let ps = RpcClient::connect(ps_addr).await.expect("ps");
        let sc = stream_client(mgr_addr).await;
        // Empty major is a no-op rewrite, but must retry prefix truncation.
        roll_row(&ps, &sc, row).await;
        roll_row(&ps, &sc, row).await;
        let empty_tail = *row_extents(&sc, row).await.last().unwrap();
        ps_compact(&ps, PART).await;
        let t0 = Instant::now();
        while row_extents(&sc, row).await != vec![empty_tail] {
            assert!(
                t0.elapsed() < Duration::from_secs(15),
                "empty major did not truncate"
            );
            compio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(checkpoint(&sc, meta).await.locs.is_empty());
        // R1: two SSTs. R2: thirty. R3: one — the 33rd, past the PS's own
        // auto-trim trigger, whose head rule takes R1's two tables.
        let mut i = 0u32;
        for _ in 0..2 {
            put_flush(&ps, i).await;
            i += 1;
        }
        let r1 = row_extents(&sc, row).await[0];
        roll_row(&ps, &sc, row).await;
        for _ in 0..30 {
            put_flush(&ps, i).await;
            i += 1;
        }
        roll_row(&ps, &sc, row).await;
        put_flush(&ps, i).await;
        assert_eq!(checkpoint(&sc, meta).await.locs.len(), 33);

        let t0 = Instant::now();
        loop {
            if checkpoint(&sc, meta).await.locs.len() < 33 {
                break;
            }
            assert!(t0.elapsed() < Duration::from_secs(60), "auto-trim never ran");
            compio::time::sleep(Duration::from_millis(500)).await;
        }
        compio::time::sleep(Duration::from_millis(500)).await;
        assert_checkpoint_covered(&sc, row, meta, "after the auto-trim").await;
        let stream = row_extents(&sc, row).await;
        assert!(!stream.contains(&r1), "R1 is fully compacted away and should be dropped: {stream:?}");

        // A major compaction rewrites everything into the tail: only it stays.
        ps_compact(&ps, PART).await;
        let t0 = Instant::now();
        loop {
            if row_extents(&sc, row).await.len() == 1 {
                break;
            }
            assert!(t0.elapsed() < Duration::from_secs(60), "major compaction never truncated");
            compio::time::sleep(Duration::from_millis(500)).await;
        }
        assert_checkpoint_covered(&sc, row, meta, "after the major compaction").await;
    });
    stop_ps(&stop, join);

    // Reopen from the checkpoint and read every key back.
    let ps_addr = pick_addr();
    let stop = Arc::new(AtomicBool::new(false));
    let join = spawn_ps(mgr_addr, ps_addr, stop.clone());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let t0 = Instant::now();
        let ps = loop {
            assert!(
                t0.elapsed() < Duration::from_secs(60),
                "partition did not reopen"
            );
            if let Ok(ps) = RpcClient::connect(ps_addr).await {
                if ps_get(&ps, PART, b"k000").await.code == CODE_OK {
                    break ps;
                }
            }
            compio::time::sleep(Duration::from_millis(200)).await;
        };
        // After reopening, one SST and no in-memory unsettled-delete hint:
        // a major must still move that SST out of its old tail. Do it twice
        // to catch the former single-table no-op as well as a missing roll.
        let sc = stream_client(mgr_addr).await;
        for _ in 0..2 {
            let before = row_extents(&sc, row).await;
            assert_eq!(before.len(), 1);
            assert_eq!(checkpoint(&sc, meta).await.locs.len(), 1);
            ps_compact(&ps, PART).await;
            let t0 = Instant::now();
            loop {
                let after = row_extents(&sc, row).await;
                if after.len() == 1 && after != before {
                    break;
                }
                assert!(
                    t0.elapsed() < Duration::from_secs(15),
                    "single-SST major did not replace its old extent: {before:?} -> {after:?}"
                );
                compio::time::sleep(Duration::from_millis(100)).await;
            }
            assert_checkpoint_covered(&sc, row, meta, "single SST after reopen").await;
        }
        for i in 0..33u32 {
            let r = ps_get(&ps, PART, format!("k{i:03}").as_bytes()).await;
            assert_eq!(r.code, CODE_OK, "k{i:03} lost");
            assert_eq!(r.value, format!("v{i}").into_bytes());
        }
    });
    stop_ps(&stop, join);

    let ps_addr = pick_addr();
    let stop = Arc::new(AtomicBool::new(false));
    let join = spawn_ps(mgr_addr, ps_addr, stop.clone());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let t0 = Instant::now();
        let ps = loop {
            assert!(
                t0.elapsed() < Duration::from_secs(60),
                "partition did not reopen"
            );
            if let Ok(ps) = RpcClient::connect(ps_addr).await {
                if ps_get(&ps, PART, b"k000").await.code == CODE_OK {
                    break ps;
                }
            }
            compio::time::sleep(Duration::from_millis(200)).await;
        };
        for i in 0..33u32 {
            let r = ps_get(&ps, PART, format!("k{i:03}").as_bytes()).await;
            assert_eq!(r.code, CODE_OK, "k{i:03} lost after single-SST rewrite");
            assert_eq!(r.value, format!("v{i}").into_bytes());
        }
    });
    stop_ps(&stop, join);
}
