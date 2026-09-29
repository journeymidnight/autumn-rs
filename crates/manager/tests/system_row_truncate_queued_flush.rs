//! A compaction truncates the row stream while a flush is queued, and the
//! queued flush's data survives.
//!
//! A queued imm's SST may already sit in an extent its table does not name yet,
//! so the truncate keeps each queued imm's rotation-time floor
//! (`row_keep_set`, unit-tested with the extent the SST could land in). The
//! earlier guard skipped the truncate whenever any imm was queued, which under
//! sustained writes is nearly always, so compacted-away SSTs were never
//! dropped. Here the background flush is paused so an imm stays queued, an
//! expiry major compaction (which does not flush first) runs, and the row
//! stream must still shrink; then the flush is released and a reopen must read
//! every key, the queued ones included.

mod support;

use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_partition_server::PartitionServer;
use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc::{self, CODE_OK};
use autumn_stream::{ConnPool, StreamClient};
use support::*;

const PART: u64 = 1301;
/// Seconds until the TTL key expires and the expiry major compaction fires.
const TTL_SECS: u64 = 15;

/// Clears the process-global flush pause on drop, so a panic mid-test cannot
/// leave the background flush loop parked.
struct PauseGuard;
impl Drop for PauseGuard {
    fn drop(&mut self) {
        autumn_partition_server::set_flush_test_pause(false);
    }
}

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

async fn row_extents(sc: &StreamClient, row: u64) -> Vec<u64> {
    sc.get_stream_info(row).await.expect("row stream info").extent_ids
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

fn queued_value(i: u32) -> Vec<u8> {
    // 512 B inline values: twenty of them cross the 8 KiB rotation threshold.
    let mut v = format!("q{i:03}").into_bytes();
    v.resize(512, b'.');
    v
}

#[test]
fn truncation_proceeds_while_a_flush_is_queued() {
    assert!(
        autumn_partition_server::set_flush_mem_bytes(8 * 1024),
        "flush_mem_bytes was already set in this test binary"
    );
    let _guard = PauseGuard;

    let mgr_addr = pick_addr();
    let n1 = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(n1, dir.path().to_path_buf(), 1);
    let row = compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("mgr");
        let _ = register_node(&mgr, &n1.to_string(), "uuid-row-truncate-queued").await;
        let log = create_stream(&mgr, 1).await;
        let row = create_stream(&mgr, 1).await;
        let meta = create_stream(&mgr, 1).await;
        upsert_partition(&mgr, PART, log, row, meta, b"", b"\xff").await;
        row
    });

    let ps_addr = pick_addr();
    let stop = Arc::new(AtomicBool::new(false));
    let join = spawn_ps(mgr_addr, ps_addr, stop.clone());
    std::thread::sleep(Duration::from_millis(800));

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let ps = RpcClient::connect(ps_addr).await.expect("ps");
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "row-truncate-queued-test".to_string(),
            128 * 1024 * 1024,
            Rc::new(ConnPool::new()),
        )
        .await
        .expect("stream client");

        // Three row extents of SSTs; the first holds a key that expires, which
        // is what makes the PS run a major compaction on its own.
        ps_put_ttl(&ps, PART, b"ttl", b"gone", TTL_SECS).await;
        ps_put(&ps, PART, b"a0", b"va0").await;
        ps_flush(&ps, PART).await;
        roll_row(&ps, &sc, row).await;
        ps_put(&ps, PART, b"b0", b"vb0").await;
        ps_flush(&ps, PART).await;
        roll_row(&ps, &sc, row).await;
        ps_put(&ps, PART, b"c0", b"vc0").await;
        ps_flush(&ps, PART).await;
        let before = row_extents(&sc, row).await;
        assert_eq!(before.len(), 3, "precondition: three row extents {before:?}");

        // Freeze an imm and keep its flush from starting.
        autumn_partition_server::set_flush_test_pause(true);
        for i in 0..20u32 {
            ps_put(&ps, PART, format!("q{i:03}").as_bytes(), &queued_value(i)).await;
        }

        // The expiry major compaction rewrites every table into a new tail and
        // truncates, with the imm still queued.
        let t0 = Instant::now();
        let after = loop {
            let ids = row_extents(&sc, row).await;
            if ids.len() < before.len() {
                break ids;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(TTL_SECS + 45),
                "the row stream was never truncated while a flush was queued: {ids:?}"
            );
            compio::time::sleep(Duration::from_millis(500)).await;
        };
        assert_eq!(
            after.len(),
            2,
            "queued floor plus the major's new tail: {after:?}"
        );
        assert_eq!(
            after[0],
            *before.last().unwrap(),
            "queued flush still pins its floor"
        );
        assert!(
            !before.contains(after.last().unwrap()),
            "major output uses a new tail"
        );

        // Release the queued flush and wait for it to commit.
        let commits = autumn_partition_server::flush_commit_count();
        autumn_partition_server::set_flush_test_pause(false);
        let t0 = Instant::now();
        while autumn_partition_server::flush_commit_count() == commits {
            assert!(t0.elapsed() < Duration::from_secs(30), "the queued flush never committed");
            compio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    stop_ps(&stop, join);

    let ps_addr = pick_addr();
    let stop = Arc::new(AtomicBool::new(false));
    let join = spawn_ps(mgr_addr, ps_addr, stop.clone());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let t0 = Instant::now();
        let ps = loop {
            assert!(t0.elapsed() < Duration::from_secs(60), "partition did not reopen");
            if let Ok(ps) = RpcClient::connect(ps_addr).await {
                if ps_get(&ps, PART, b"a0").await.code == CODE_OK {
                    break ps;
                }
            }
            compio::time::sleep(Duration::from_millis(200)).await;
        };
        for (k, v) in [("a0", "va0"), ("b0", "vb0"), ("c0", "vc0")] {
            let r = ps_get(&ps, PART, k.as_bytes()).await;
            assert_eq!(r.code, CODE_OK, "{k} lost");
            assert_eq!(r.value, v.as_bytes());
        }
        for i in 0..20u32 {
            let r = ps_get(&ps, PART, format!("q{i:03}").as_bytes()).await;
            assert_eq!(r.code, CODE_OK, "queued key q{i:03} lost");
            assert_eq!(r.value, queued_value(i));
        }
    });
    stop_ps(&stop, join);
}
