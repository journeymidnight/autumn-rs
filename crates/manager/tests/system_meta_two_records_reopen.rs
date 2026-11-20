//! A partition reopened with two checkpoint records of its own in its meta
//! stream must load the newer one, not the union of both.
//!
//! A checkpoint truncates the meta extents before its own; the manager can
//! defer that cut (an op in flight on the old extent), and a crash can land
//! between the append and the cut. The meta stream then holds the old record
//! in one extent and the new one in the next. A major compaction's record
//! lists only its outputs, and the row-stream cut after it drops the extents
//! of the tables it replaced. Recovery took every record's tables, as for a
//! merge's sources, so the reopen asked for tables whose extents are gone.
//!
//! The deferral is the manager's test failpoint `TRUNCATE_TEST_DEFER_STREAM`,
//! re-armed for as long as the test runs: an op in flight on the old extent
//! keeps the manager refusing every cut, not just one.

mod support;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;

use support::*;

const PART: u64 = 1441;
const PS_ID: u64 = 91;
const KEYS: u32 = 30;

fn value(round: u32, i: u32) -> Vec<u8> {
    format!("r{round}-v{i}").into_bytes()
}

async fn extents(mgr: &RpcClient, stream: u64) -> Vec<u64> {
    use autumn_rpc::manager_rpc::{self, StreamInfoReq, StreamInfoResp, MSG_STREAM_INFO};
    let resp = mgr
        .call(
            MSG_STREAM_INFO,
            manager_rpc::rkyv_encode(&StreamInfoReq {
                stream_ids: vec![stream],
            }),
        )
        .await
        .expect("stream_info rpc");
    let resp: StreamInfoResp = manager_rpc::rkyv_decode(&resp).expect("decode StreamInfoResp");
    assert_eq!(resp.code, manager_rpc::CODE_OK, "stream_info: {}", resp.message);
    resp.streams
        .into_iter()
        .find(|(id, _)| *id == stream)
        .expect("stream in response")
        .1
        .extent_ids
}

async fn roll(ps: &RpcClient, stream: u64, tail: u64) {
    let resp = ps
        .call(
            partition_rpc::MSG_ROLL_TAILS,
            partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq {
                part_id: PART,
                entries: vec![(stream, tail)],
            }),
        )
        .await
        .expect("roll_tails rpc");
    let resp: partition_rpc::RollTailsResp =
        partition_rpc::rkyv_decode(&resp).expect("decode RollTailsResp");
    assert_eq!(resp.code, partition_rpc::CODE_OK, "roll_tails: {}", resp.message);
    assert_eq!(resp.rolled, 1, "stream {stream} tail {tail} must roll");
}

#[test]
fn a_reopen_with_two_own_records_loads_the_newer_one() {
    let mgr_addr = pick_addr();
    let en_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .expect("mgr");
        let _ = register_node(&mgr, &en_addr.to_string(), "uuid-meta-two-records").await;
        let (log, row, meta) = (
            create_stream(&mgr, 1).await,
            create_stream(&mgr, 1).await,
            create_stream(&mgr, 1).await,
        );
        upsert_partition(&mgr, PART, log, row, meta, b"", b"\xff").await;

        let ps_addr = pick_addr();
        start_partition_server(PS_ID, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("ps");

        // Three flushed tables; each round overwrites every key.
        for round in 0..3 {
            for i in 0..KEYS {
                ps_put(&ps, PART, format!("k{i:03}").as_bytes(), &value(round, i)).await;
            }
            ps_flush(&ps, PART).await;
        }
        let row_before = extents(&mgr, row).await;

        // The next checkpoint goes to a fresh meta extent, and its cut of the
        // old one is deferred: the old record stays beside the new.
        let meta_old = *extents(&mgr, meta).await.last().unwrap();
        roll(&ps, meta, meta_old).await;
        let deferring = std::rc::Rc::new(std::cell::Cell::new(true));
        let defer_count = std::rc::Rc::new(std::cell::Cell::new(0u32));
        {
            let (deferring, defer_count) = (deferring.clone(), defer_count.clone());
            compio::runtime::spawn(async move {
                autumn_manager::TRUNCATE_TEST_DEFER_STREAM.store(meta, Ordering::Relaxed);
                while deferring.get() {
                    if autumn_manager::TRUNCATE_TEST_DEFER_STREAM.load(Ordering::Relaxed) == 0 {
                        defer_count.set(defer_count.get() + 1);
                        autumn_manager::TRUNCATE_TEST_DEFER_STREAM.store(meta, Ordering::Relaxed);
                    }
                    compio::time::sleep(Duration::from_millis(1)).await;
                }
                autumn_manager::TRUNCATE_TEST_DEFER_STREAM.store(0, Ordering::Relaxed);
            })
            .detach();
        }
        ps_compact(&ps, PART).await;
        let t0 = Instant::now();
        while defer_count.get() == 0 {
            assert!(t0.elapsed() < Duration::from_secs(20), "the compaction never cut the meta stream");
            compio::time::sleep(Duration::from_millis(50)).await;
        }
        // The major rolled the row tail and cut the extents of the tables it
        // replaced.
        let t0 = Instant::now();
        loop {
            let now = extents(&mgr, row).await;
            if !now.contains(&row_before[0]) {
                break;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(20),
                "the replaced tables' row extent was never cut: {now:?} (before {row_before:?})"
            );
            compio::time::sleep(Duration::from_millis(100)).await;
        }
        let meta_now = extents(&mgr, meta).await;
        assert_eq!(meta_now.len(), 2, "both records must be left: {meta_now:?}");
        assert_eq!(meta_now[0], meta_old, "the old record's extent must be first");
        // The old record names tables in the cut row extent; the new one does not.
        let sc = autumn_stream::StreamClient::connect(
            &mgr_addr.to_string(),
            "meta-two-records-probe".to_string(),
            128 * 1024 * 1024,
            std::rc::Rc::new(autumn_stream::ConnPool::new()),
        )
        .await
        .expect("stream client");
        let record = |eid: u64| {
            let sc = sc.clone();
            async move {
                let (payload, _) = sc.read_bytes_from_extent(eid, 0, 0).await.expect("read meta");
                decode_last_table_locations(&payload)
            }
        };
        let (old_rec, new_rec) = (record(meta_now[0]).await, record(meta_now[1]).await);
        let row_now = extents(&mgr, row).await;
        assert!(
            old_rec.locs.iter().any(|l| !row_now.contains(&l.extent_id)),
            "non-vacuity: the old record must name a table whose row extent was cut"
        );
        assert!(
            new_rec.locs.iter().all(|l| row_now.contains(&l.extent_id)),
            "the new record names only live row extents"
        );

        // Crash: a same-id PS takes the partition over and reopens it.
        let ps2_addr = pick_addr();
        spawn_partition_server(PS_ID, mgr_addr, ps2_addr);
        let ps2_up = poll_until(Duration::from_secs(30), Duration::from_millis(100), || {
            std::net::TcpStream::connect(ps2_addr).is_ok()
        })
        .await;
        assert!(
            ps2_up,
            "the takeover PS never came up: its open of the partition does not finish \
             (meta stream {meta_now:?}, row before {row_before:?})"
        );
        let ps2 = RpcClient::connect(ps2_addr).await.expect("ps2");
        let opened = poll_until_async(Duration::from_secs(30), Duration::from_millis(100), || {
            let ps2 = &ps2;
            async move {
                matches!(try_ps_get(ps2, PART, b"k000").await,
                    Ok(r) if r.code == partition_rpc::CODE_OK)
            }
        })
        .await;
        assert!(
            opened,
            "the partition never reopened (meta stream {meta_now:?}, row before {row_before:?})"
        );
        for i in 0..KEYS {
            let r = ps_get(&ps2, PART, format!("k{i:03}").as_bytes()).await;
            assert_eq!(r.code, partition_rpc::CODE_OK, "k{i:03}: {}", r.message);
            assert_eq!(r.value, value(2, i), "k{i:03} has an older value");
        }
        deferring.set(false);
    });
}
