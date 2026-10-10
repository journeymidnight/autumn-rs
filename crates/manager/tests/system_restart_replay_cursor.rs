//! A graceful restart replays only the WAL past the last checkpoint.
//!
//! The drain flushes every memtable and publishes a checkpoint whose replay
//! cursor is the log tail, so the next open has nothing to replay. Two things
//! used to pull the replay start back anyway, and each made an open walk every
//! byte of WAL written since some old flush:
//!
//! - recovery lowered the start to the OLDEST cursor stamped on any loaded SST,
//!   so one early SST that no compaction had touched anchored the replay at its
//!   own flush (`graceful_restart_replays_only_past_the_checkpoint`);
//! - a compaction published its checkpoint with the newest INPUT's cursor, so a
//!   minor compaction of older tables that landed after a newer flush moved the
//!   checkpoint backwards (`compaction_never_moves_the_checkpoint_back`).

mod support;

use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_partition_server::background::set_minor_compaction_paused;
use autumn_partition_server::compact_policy::{set_minor_policy, MinorPolicy};
use autumn_partition_server::{replay_read_bytes, PartitionServer};
use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc::{TableLocations, CODE_OK};
use autumn_stream::{ConnPool, StreamClient};
use support::*;

/// WAL written after the early flush: 3000 x 1 KiB values. Far below the
/// 256 MiB rotate threshold, so only the graceful drain flushes it.
const BULK_KEYS: usize = 3000;
const BULK_VALUE: usize = 1024;
/// What a replay that starts at the tail may still read (a few records past a
/// cursor, framing). Replaying the bulk region reads ~3 MiB.
const TAIL_REPLAY_BOUND: u64 = 64 * 1024;

struct Cluster {
    mgr_addr: SocketAddr,
    _dir: tempfile::TempDir,
    meta_stream: u64,
}

fn start_cluster(part_id: u64) -> Cluster {
    let mgr_addr = pick_addr();
    let n1_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(n1_addr, dir.path().to_path_buf(), 1);
    let meta_stream = compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("mgr");
        let _ = register_node(&mgr, &n1_addr.to_string(), &format!("uuid-replay-{part_id}")).await;
        let log = create_stream(&mgr, 1).await;
        let row = create_stream(&mgr, 1).await;
        let meta = create_stream(&mgr, 1).await;
        upsert_partition(&mgr, part_id, log, row, meta, b"", b"\xff").await;
        meta
    });
    Cluster {
        mgr_addr,
        _dir: dir,
        meta_stream,
    }
}

fn spawn_ps(
    mgr_addr: SocketAddr,
    ps_addr: SocketAddr,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let ps = PartitionServer::connect_with_advertise_and_port(
                1,
                &mgr_addr.to_string(),
                Some(ps_addr.to_string()),
                ps_addr,
            )
            .await
            .expect("connect partition server");
            ps.sync_regions_once().await.expect("sync regions");
            let stop_fut = async move {
                while !stop.load(Ordering::Acquire) {
                    compio::time::sleep(Duration::from_millis(50)).await;
                }
            };
            ps.serve_until_shutdown(ps_addr, stop_fut)
                .await
                .expect("serve_until_shutdown");
        });
    })
}

fn stop_ps(stop: &AtomicBool, join: std::thread::JoinHandle<()>) {
    stop.store(true, Ordering::Release);
    let started = Instant::now();
    while !join.is_finished() {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "PS did not finish its graceful shutdown"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    join.join().expect("PS thread panicked");
}

fn bulk_key(i: usize) -> String {
    format!("bulk-{i:05}")
}

async fn put_bulk(ps: &RpcClient, part_id: u64) {
    for i in 0..BULK_KEYS {
        ps_put(ps, part_id, bulk_key(i).as_bytes(), &vec![b'b'; BULK_VALUE]).await;
    }
}

/// Every minor window faces the ratio test, so the bulk table stays out of one.
/// Process-wide and first-set-wins, so each test sets it before any partition
/// opens.
fn minor_policy() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        set_minor_policy(MinorPolicy { min_size: 1, ..Default::default() }).expect("policy")
    });
}

/// Restart the PS on a fresh address, wait until the partition serves, and
/// return how many WAL bytes its recovery replayed. Also checks every bulk key.
fn restart_and_measure_replay(c: &Cluster, part_id: u64) -> u64 {
    let before = replay_read_bytes(part_id);
    let ps_addr = pick_addr();
    let stop = Arc::new(AtomicBool::new(false));
    let join = spawn_ps(c.mgr_addr, ps_addr, stop.clone());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let started = Instant::now();
        let ps = loop {
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "partition {part_id} did not serve after restart"
            );
            if let Ok(ps) = RpcClient::connect(ps_addr).await {
                if ps_get(&ps, part_id, bulk_key(0).as_bytes()).await.code == CODE_OK {
                    break ps;
                }
            }
            compio::time::sleep(Duration::from_millis(200)).await;
        };
        for i in 0..BULK_KEYS {
            let r = ps_get(&ps, part_id, bulk_key(i).as_bytes()).await;
            assert_eq!(r.code, CODE_OK, "{} lost across the restart", bulk_key(i));
            assert_eq!(r.value.len(), BULK_VALUE);
        }
    });
    let replayed = replay_read_bytes(part_id) - before;
    stop_ps(&stop, join);
    replayed
}

async fn last_checkpoint(mgr_addr: SocketAddr, meta_stream: u64) -> TableLocations {
    let sc = StreamClient::connect(
        &mgr_addr.to_string(),
        "replay-cursor-test".to_string(),
        128 * 1024 * 1024,
        Rc::new(ConnPool::new()),
    )
    .await
    .expect("stream client");
    let raw = sc
        .read_last_extent_data(meta_stream)
        .await
        .expect("read meta stream")
        .expect("meta stream has a checkpoint");
    decode_last_table_locations(&raw)
}

#[test]
fn graceful_restart_replays_only_past_the_checkpoint() {
    minor_policy();
    let part_id = 1101;
    let c = start_cluster(part_id);
    let ps_addr = pick_addr();
    let stop = Arc::new(AtomicBool::new(false));
    let join = spawn_ps(c.mgr_addr, ps_addr, stop.clone());
    std::thread::sleep(Duration::from_millis(800));

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let ps = RpcClient::connect(ps_addr).await.expect("ps");
        // The early SST: its cursor sits before all of the bulk WAL.
        for i in 0..10 {
            ps_put(&ps, part_id, format!("early-{i}").as_bytes(), b"e").await;
        }
        ps_flush(&ps, part_id).await;
        put_bulk(&ps, part_id).await;
    });
    // The drain flushes the bulk into a second SST; its checkpoint names the tail.
    stop_ps(&stop, join);

    let replayed = restart_and_measure_replay(&c, part_id);
    assert!(
        replayed < TAIL_REPLAY_BOUND,
        "a restart after a clean drain replayed {replayed} WAL bytes; the checkpoint \
         names the tail, so the early SST's older cursor must not pull the start back"
    );
}

#[test]
fn compaction_never_moves_the_checkpoint_back() {
    minor_policy();
    let part_id = 1102;
    let c = start_cluster(part_id);
    let ps_addr = pick_addr();
    let stop = Arc::new(AtomicBool::new(false));
    let join = spawn_ps(c.mgr_addr, ps_addr, stop.clone());
    std::thread::sleep(Duration::from_millis(800));

    let (flushed, trimmed) = compio::runtime::Runtime::new().unwrap().block_on(async {
        let ps = RpcClient::connect(ps_addr).await.expect("ps");
        // Three small SSTs, then the newest holding the bulk. Its flush
        // checkpoint names the tail; the minor compaction then takes the
        // three OLDER small tables only (the bulk one fails the ratio).
        set_minor_compaction_paused(true);
        for i in 0..3 {
            ps_put(&ps, part_id, format!("small-{i:02}").as_bytes(), b"s").await;
            ps_flush(&ps, part_id).await;
        }
        put_bulk(&ps, part_id).await;
        ps_flush(&ps, part_id).await;
        let flushed = last_checkpoint(c.mgr_addr, c.meta_stream).await;
        assert_eq!(flushed.locs.len(), 4, "expected 4 SSTs before the minor compaction");
        set_minor_compaction_paused(false);

        let started = Instant::now();
        let trimmed = loop {
            let ck = last_checkpoint(c.mgr_addr, c.meta_stream).await;
            if ck.locs.len() < 4 {
                break ck;
            }
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "the minor compaction never ran"
            );
            compio::time::sleep(Duration::from_millis(500)).await;
        };
        (flushed, trimmed)
    });
    assert_eq!(
        (trimmed.vp_extent_id, trimmed.vp_offset),
        (flushed.vp_extent_id, flushed.vp_offset),
        "the compaction's checkpoint moved the replay cursor back from the newest flush's"
    );

    // Nothing is left to flush, so the drain publishes no checkpoint and the
    // compaction's is the one recovery reads.
    stop_ps(&stop, join);
    let replayed = restart_and_measure_replay(&c, part_id);
    assert!(
        replayed < TAIL_REPLAY_BOUND,
        "a restart after a compaction replayed {replayed} WAL bytes"
    );
}
