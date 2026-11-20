//! A merge source whose meta stream still holds an older checkpoint beside
//! its freeze record must not merge: recovery would load the wrong records.
//!
//! A merged open takes the run of freeze records after the newest normal one
//! (one per source). The merge splices `[survivor meta][victim meta]`; if the
//! victim's freeze record could not cut the victim's older extents (the
//! manager defers a cut while an op is in flight on them), the splice reads
//! `F_survivor, N_victim_old, F_victim`, and recovery took only the victim's
//! record: every flushed survivor key was gone. So a freeze record whose cut
//! is deferred fails the freeze, and the merge is retried.

mod support;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_partition_server::PartitionServer;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, MergePartitionsReq, MergePartitionsResp, MSG_MERGE_PARTITIONS,
};
use autumn_rpc::partition_rpc::{self, CODE_OK};
use support::*;

const SURVIVOR: u64 = 1451;
const VICTIM: u64 = 1452;
const KEYS: usize = 40;

fn spawn_ps(mgr_addr: SocketAddr, ps_addr: SocketAddr, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
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
            ps.serve_until_shutdown(ps_addr, stop_fut).await.expect("serve_until_shutdown");
        });
    })
}

fn survivor_key(i: usize) -> String {
    format!("a-{i:03}")
}

fn victim_key(i: usize) -> String {
    format!("n-{i:03}")
}

async fn extents(mgr: &RpcClient, stream: u64) -> Vec<u64> {
    use autumn_rpc::manager_rpc::{self, StreamInfoReq, StreamInfoResp, MSG_STREAM_INFO};
    let resp = mgr
        .call(MSG_STREAM_INFO, manager_rpc::rkyv_encode(&StreamInfoReq { stream_ids: vec![stream] }))
        .await
        .expect("stream_info rpc");
    let resp: StreamInfoResp = manager_rpc::rkyv_decode(&resp).expect("decode StreamInfoResp");
    assert_eq!(resp.code, manager_rpc::CODE_OK, "stream_info: {}", resp.message);
    resp.streams.into_iter().find(|(id, _)| *id == stream).expect("stream").1.extent_ids
}

async fn merge(mgr: &RpcClient) -> MergePartitionsResp {
    let bytes = mgr
        .call(
            MSG_MERGE_PARTITIONS,
            rkyv_encode(&MergePartitionsReq { survivor_part_id: SURVIVOR, victim_part_id: VICTIM, force: false }),
        )
        .await
        .expect("merge call");
    rkyv_decode(&bytes).expect("merge resp")
}

#[test]
fn a_merge_source_with_an_older_record_left_does_not_lose_the_other_source() {
    let mgr_addr = pick_addr();
    let en_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);
    let victim_meta = compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("mgr");
        let _ = register_node(&mgr, &en_addr.to_string(), "uuid-merge-freeze-cut").await;
        let mut s = Vec::new();
        for _ in 0..6 {
            s.push(create_stream(&mgr, 1).await);
        }
        upsert_partition(&mgr, SURVIVOR, s[0], s[1], s[2], b"", b"m").await;
        upsert_partition(&mgr, VICTIM, s[3], s[4], s[5], b"m", b"\xff").await;
        s[5]
    });

    let ps_addr = pick_addr();
    let stop = Arc::new(AtomicBool::new(false));
    let join = spawn_ps(mgr_addr, ps_addr, stop.clone());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("mgr");
        let router = PsRouter::new(mgr_addr, ps_addr);
        for i in 0..KEYS {
            psr_put(&router, SURVIVOR, survivor_key(i).as_bytes(), b"s").await;
            psr_put(&router, VICTIM, victim_key(i).as_bytes(), b"v0").await;
        }
        psr_flush(&router, SURVIVOR).await;
        psr_flush(&router, VICTIM).await;

        // The victim's next checkpoint goes to a fresh meta extent and its
        // cut of the old one is deferred, for as long as `deferring` holds.
        let meta_old = *extents(&mgr, victim_meta).await.last().unwrap();
        let vc = router.client_for(VICTIM).await;
        let resp = vc
            .call(
                partition_rpc::MSG_ROLL_TAILS,
                partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq {
                    part_id: VICTIM,
                    entries: vec![(victim_meta, meta_old)],
                }),
            )
            .await
            .expect("roll_tails rpc");
        let resp: partition_rpc::RollTailsResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!((resp.code, resp.rolled), (CODE_OK, 1), "roll_tails: {}", resp.message);
        let deferring = std::rc::Rc::new(std::cell::Cell::new(true));
        let defer_count = std::rc::Rc::new(std::cell::Cell::new(0u32));
        {
            let (deferring, defer_count) = (deferring.clone(), defer_count.clone());
            compio::runtime::spawn(async move {
                autumn_manager::TRUNCATE_TEST_DEFER_STREAM.store(victim_meta, Ordering::Relaxed);
                while deferring.get() {
                    if autumn_manager::TRUNCATE_TEST_DEFER_STREAM.load(Ordering::Relaxed) == 0 {
                        defer_count.set(defer_count.get() + 1);
                        autumn_manager::TRUNCATE_TEST_DEFER_STREAM.store(victim_meta, Ordering::Relaxed);
                    }
                    compio::time::sleep(Duration::from_millis(1)).await;
                }
                autumn_manager::TRUNCATE_TEST_DEFER_STREAM.store(0, Ordering::Relaxed);
            })
            .detach();
        }
        for i in 0..KEYS {
            psr_put(&router, VICTIM, victim_key(i).as_bytes(), b"v1").await;
        }
        psr_flush(&router, VICTIM).await;
        let t0 = Instant::now();
        while defer_count.get() == 0 {
            assert!(t0.elapsed() < Duration::from_secs(20), "the victim's flush never cut its meta stream");
            compio::time::sleep(Duration::from_millis(20)).await;
        }
        let meta_now = extents(&mgr, victim_meta).await;
        assert_eq!(meta_now.len(), 2, "non-vacuity: the victim keeps its older record: {meta_now:?}");

        // Merge while the cut is deferred, then again once it is not.
        let first = merge(&mgr).await;
        deferring.set(false);
        if first.code != CODE_OK {
            let t0 = Instant::now();
            loop {
                let r = merge(&mgr).await;
                if r.code == CODE_OK {
                    break;
                }
                assert!(t0.elapsed() < Duration::from_secs(60), "merge never succeeded: {}", r.message);
                compio::time::sleep(Duration::from_millis(500)).await;
            }
        }

        let t0 = Instant::now();
        loop {
            if let Ok(c) = router.try_client_for(SURVIVOR).await {
                if try_ps_get(&c, SURVIVOR, victim_key(0).as_bytes()).await.is_ok_and(|r| r.code == CODE_OK) {
                    break;
                }
            }
            assert!(t0.elapsed() < Duration::from_secs(60), "the survivor never served the merged range");
            compio::time::sleep(Duration::from_millis(200)).await;
        }
        let mut lost = Vec::new();
        for i in 0..KEYS {
            let r = psr_get(&router, SURVIVOR, survivor_key(i).as_bytes()).await;
            if r.code != CODE_OK || r.value != b"s" {
                lost.push(survivor_key(i));
            }
            let r = psr_get(&router, SURVIVOR, victim_key(i).as_bytes()).await;
            if r.code != CODE_OK || r.value != b"v1" {
                lost.push(victim_key(i));
            }
        }
        assert!(
            lost.is_empty(),
            "{} keys lost or stale after the merge (first merge: code {} {}): {:?}",
            lost.len(),
            first.code,
            first.message,
            &lost[..lost.len().min(5)]
        );
    });
    stop.store(true, Ordering::Release);
    join.join().expect("PS thread panicked");
}
