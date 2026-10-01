//! After a merge the survivor skips every freeze-certified source extent,
//! replays only the post-merge WAL tail, publishes one checkpoint, and a
//! restart replays nothing.
//!
//! The merge splices both partitions' meta streams into the survivor's, so it
//! briefly holds one checkpoint record per source. Recovery replays from the
//! earliest cursor among them. A cursor offset alone cannot skip complete
//! checkpoint-covered extents before a later source's cursor, so merge freeze
//! certifies each source's exact extent list. The survivor's open then publishes
//! one record for both; without that, the records stayed until the survivor's
//! next flush, and a drain with an empty memtable does not flush, so every
//! restart replayed the victim's WAL again.

mod support;

use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_partition_server::{replay_read_bytes, PartitionServer};
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, MergePartitionsReq, MergePartitionsResp, MSG_MERGE_PARTITIONS,
};
use autumn_rpc::partition_rpc::CODE_OK;
use autumn_stream::{ConnPool, StreamClient};
use support::*;

const SURVIVOR: u64 = 1201;
const VICTIM: u64 = 1202;
const SURVIVOR_KEYS: usize = 1000;
/// The victim's WAL: 3000 x 1 KiB values, flushed by the merge's drain.
const VICTIM_KEYS: usize = 3000;
const VALUE: usize = 1024;
/// What a replay that starts at the tail may still read. Replaying the
/// victim's two sealed prefix extents reads ~2 MiB.
const TAIL_REPLAY_BOUND: u64 = 64 * 1024;

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

fn survivor_key(i: usize) -> String {
    format!("a-{i:05}")
}

fn victim_key(i: usize) -> String {
    format!("n-{i:05}")
}

/// Every checkpoint record recovery would read: the last one in each extent
/// of the meta stream.
async fn checkpoint_records(mgr_addr: SocketAddr, meta_stream: u64) -> usize {
    let sc = StreamClient::connect(
        &mgr_addr.to_string(),
        "merge-checkpoint-test".to_string(),
        128 * 1024 * 1024,
        Rc::new(ConnPool::new()),
    )
    .await
    .expect("stream client");
    let info = sc.get_stream_info(meta_stream).await.expect("meta stream info");
    let mut records = 0;
    for &eid in &info.extent_ids {
        let (payload, _) = sc
            .read_bytes_from_extent(eid, 0, 0)
            .await
            .expect("read meta extent");
        if payload.len() >= 4 {
            records += 1;
        }
    }
    records
}

async fn stream_extent_ids(mgr_addr: SocketAddr, stream_id: u64) -> Vec<u64> {
    let sc = StreamClient::connect(
        &mgr_addr.to_string(),
        "merge-checkpoint-test".to_string(),
        128 * 1024 * 1024,
        Rc::new(ConnPool::new()),
    )
    .await
    .expect("stream client");
    sc.get_stream_info(stream_id)
        .await
        .expect("stream info")
        .extent_ids
}

/// Force a real WAL extent boundary without weakening the production 1-GiB
/// minimum accepted by `set_max_extent_size_bytes`. Every preceding put is
/// awaited and this test has one sequential writer; no P-log append is in
/// flight when the independent stream client rolls the tail. Background GC is
/// never dispatched in this test.
async fn roll_log_tail(mgr_addr: SocketAddr, log_stream_id: u64) {
    let sc = StreamClient::connect(
        &mgr_addr.to_string(),
        "merge-checkpoint-roll".to_string(),
        128 * 1024 * 1024,
        Rc::new(ConnPool::new()),
    )
    .await
    .expect("stream client");
    let before = sc
        .get_stream_info(log_stream_id)
        .await
        .expect("stream info before roll")
        .extent_ids;
    sc.seal_and_roll_tail(log_stream_id)
        .await
        .expect("roll log tail");
    let after = stream_extent_ids(mgr_addr, log_stream_id).await;
    assert_eq!(after.len(), before.len() + 1, "roll must add one extent");
    assert_eq!(&after[..before.len()], before.as_slice());
}

/// Wait until the survivor serves `key`, which the merge moved into its range.
async fn wait_serving(router: &PsRouter, key: &[u8]) {
    let started = Instant::now();
    loop {
        if let Ok(c) = router.try_client_for(SURVIVOR).await {
            if ps_get(&c, SURVIVOR, key).await.code == CODE_OK {
                return;
            }
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "the survivor never served the merged range"
        );
        compio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[test]
fn a_merge_survivor_holds_one_checkpoint_and_restarts_without_replay() {
    // Force both WALs across multiple extents. The old cursor-offset-only
    // recovery passed the single-extent shape but re-read every complete
    // victim prefix extent on the first merge reopen.
    let mgr_addr = pick_addr();
    let en_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);
    let (survivor_log, victim_log, survivor_meta) =
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::protocol_hello::Role::Admin, None).await.expect("mgr");
            let _ = register_node(&mgr, &en_addr.to_string(), "uuid-merge-ckpt").await;
            let (s_log, s_row, s_meta) = (
                create_stream(&mgr, 1).await,
                create_stream(&mgr, 1).await,
                create_stream(&mgr, 1).await,
            );
            let (v_log, v_row, v_meta) = (
                create_stream(&mgr, 1).await,
                create_stream(&mgr, 1).await,
                create_stream(&mgr, 1).await,
            );
            upsert_partition(&mgr, SURVIVOR, s_log, s_row, s_meta, b"", b"m").await;
            upsert_partition(&mgr, VICTIM, v_log, v_row, v_meta, b"m", b"\xff").await;
            (s_log, v_log, s_meta)
        });

    let ps_addr = pick_addr();
    let stop = Arc::new(AtomicBool::new(false));
    let join = spawn_ps(mgr_addr, ps_addr, stop.clone());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let router = PsRouter::new(mgr_addr, ps_addr);
        for i in 0..SURVIVOR_KEYS {
            psr_put(&router, SURVIVOR, survivor_key(i).as_bytes(), &vec![b's'; VALUE]).await;
            if i == 299 || i == 599 {
                roll_log_tail(mgr_addr, survivor_log).await;
            }
        }
        psr_flush(&router, SURVIVOR).await;
        for i in 0..VICTIM_KEYS {
            psr_put(&router, VICTIM, victim_key(i).as_bytes(), &vec![b'v'; VALUE]).await;
            if i == 999 || i == 1999 {
                roll_log_tail(mgr_addr, victim_log).await;
            }
        }

        assert!(
            stream_extent_ids(mgr_addr, survivor_log).await.len() >= 3,
            "survivor must have multiple WAL extents before merge"
        );
        assert!(
            stream_extent_ids(mgr_addr, victim_log).await.len() >= 3,
            "victim must have multiple WAL extents before merge"
        );

        let before_merge_replay = replay_read_bytes(SURVIVOR);
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::protocol_hello::Role::Admin, None).await.expect("mgr");
        let bytes = mgr
            .call(
                MSG_MERGE_PARTITIONS,
                rkyv_encode(&MergePartitionsReq {
                    survivor_part_id: SURVIVOR,
                    victim_part_id: VICTIM,
                    force: false,
                }),
            )
            .await
            .expect("merge call");
        let resp: MergePartitionsResp = rkyv_decode(&bytes).expect("merge resp");
        assert_eq!(resp.code, CODE_OK, "merge: {}", resp.message);

        wait_serving(&router, victim_key(0).as_bytes()).await;
        let merge_replayed = replay_read_bytes(SURVIVOR) - before_merge_replay;
        assert!(
            merge_replayed < TAIL_REPLAY_BOUND,
            "the merge reopen replayed {merge_replayed} WAL bytes already covered by source checkpoints"
        );
        assert_eq!(
            checkpoint_records(mgr_addr, survivor_meta).await,
            1,
            "the survivor is open, so its checkpoints must be merged into one"
        );
    });
    stop_ps(&stop, join);

    // The drain has nothing to flush. What recovery reads is whatever the
    // survivor's open published.
    let before = replay_read_bytes(SURVIVOR);
    let ps_addr = pick_addr();
    let stop = Arc::new(AtomicBool::new(false));
    let join = spawn_ps(mgr_addr, ps_addr, stop.clone());
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let router = PsRouter::new(mgr_addr, ps_addr);
        wait_serving(&router, victim_key(0).as_bytes()).await;
        for i in 0..SURVIVOR_KEYS {
            let r = psr_get(&router, SURVIVOR, survivor_key(i).as_bytes()).await;
            assert_eq!(r.code, CODE_OK, "{} lost", survivor_key(i));
            assert_eq!(r.value.len(), VALUE);
        }
        for i in 0..VICTIM_KEYS {
            let r = psr_get(&router, SURVIVOR, victim_key(i).as_bytes()).await;
            assert_eq!(r.code, CODE_OK, "{} lost", victim_key(i));
            assert_eq!(r.value.len(), VALUE);
        }
    });
    let replayed = replay_read_bytes(SURVIVOR) - before;
    stop_ps(&stop, join);
    assert!(
        replayed < TAIL_REPLAY_BOUND,
        "a restart after the merge replayed {replayed} WAL bytes"
    );
}
