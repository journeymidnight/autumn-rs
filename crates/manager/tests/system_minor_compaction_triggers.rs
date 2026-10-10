//! The partition compacts its own tables: every flush asks for a minor
//! compaction when the exploring policy finds a window, and a row-stream head
//! extent pinned by one old table is rewritten so the stream can be truncated.
//! No manager policy, no `autumn-op compact`.

mod support;

use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_partition_server::background::minor_compaction_runs;
use autumn_partition_server::compact_policy::{set_minor_policy, MinorPolicy};
use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc::{self, CODE_OK};
use autumn_stream::{ConnPool, StreamClient};
use support::*;

/// Test data is far below the 128 MiB default `min_size`, under which every
/// window skips the ratio test; at 1 byte the ratio decides, as it does for
/// production-sized tables. Process-wide, first set wins.
fn minor_policy() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        set_minor_policy(MinorPolicy { min_size: 1, ..Default::default() }).expect("policy")
    });
}

struct Cluster {
    mgr_addr: SocketAddr,
    _dir: tempfile::TempDir,
    ps: Rc<RpcClient>,
    log: u64,
    row: u64,
    meta: u64,
}

async fn start_cluster(part: u64, uuid: &str) -> Cluster {
    let mgr_addr = pick_addr();
    let en_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);
    let mgr = RpcClient::connect(mgr_addr).await.expect("mgr");
    register_node(&mgr, &en_addr.to_string(), uuid).await;
    let (log, row, meta) = (
        create_stream(&mgr, 1).await,
        create_stream(&mgr, 1).await,
        create_stream(&mgr, 1).await,
    );
    upsert_partition(&mgr, part, log, row, meta, b"", b"\xff").await;
    let ps_addr = pick_addr();
    start_partition_server(part, mgr_addr, ps_addr);
    let ps = RpcClient::connect(ps_addr).await.expect("ps");
    Cluster { mgr_addr, _dir: dir, ps, log, row, meta }
}

async fn stream_client(mgr: SocketAddr) -> Rc<StreamClient> {
    StreamClient::connect(
        &mgr.to_string(),
        "minor-trigger-test".to_string(),
        128 * 1024 * 1024,
        Rc::new(ConnPool::new()),
    )
    .await
    .expect("stream client")
}

async fn listed_ssts(sc: &StreamClient, meta: u64) -> usize {
    match sc.read_last_extent_data(meta).await.expect("read meta stream") {
        Some(raw) => decode_last_table_locations(&raw).locs.len(),
        None => 0,
    }
}

async fn row_extents(sc: &StreamClient, row: u64) -> Vec<u64> {
    sc.get_stream_info(row).await.expect("row stream info").extent_ids
}

/// A burst of flushes, faster than the 5-7 s maintenance tick: the table count
/// stays below the blocking count only because each flush asks for a minor
/// compaction.
#[test]
fn flushes_keep_the_table_count_bounded() {
    minor_policy();
    const PART: u64 = 1501;
    const FLUSHES: usize = 60;
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let c = start_cluster(PART, "uuid-minor-flush").await;
        let sc = stream_client(c.mgr_addr).await;
        let mut most = 0;
        for i in 0..FLUSHES {
            ps_put(&c.ps, PART, format!("k{i:03}").as_bytes(), b"v").await;
            ps_flush(&c.ps, PART).await;
            most = most.max(listed_ssts(&sc, c.meta).await);
        }
        assert!(
            most < MinorPolicy::default().blocking_files,
            "{most} tables listed during {FLUSHES} flushes"
        );
        for i in 0..FLUSHES {
            let r = ps_get(&c.ps, PART, format!("k{i:03}").as_bytes()).await;
            assert_eq!(r.code, CODE_OK, "k{i:03} lost");
        }
    });
}

/// One big old table A sits in the row stream's first extent. Small flushes
/// overwrite the same keys over and over; the minor compactions merge them
/// (A fails the ratio against them and is never in a window) and the dead
/// copies pile up in A's extent. Once that extent is sealed, A is under 30%
/// of it: the reclaim rewrites A to the tail and the truncate drops it. The
/// flushes go on meanwhile: each queues a minor, and the tick (which runs the
/// probe) must still come round.
#[test]
fn a_mostly_dead_head_extent_is_reclaimed() {
    minor_policy();
    const PART: u64 = 1502;
    let value = vec![b'a'; 1024];
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let c = start_cluster(PART, "uuid-minor-reclaim").await;
        let sc = stream_client(c.mgr_addr).await;
        // A: 1000 x 1 KiB inline values, about 1 MiB.
        for i in 0..1000 {
            ps_put(&c.ps, PART, format!("a{i:04}").as_bytes(), &value).await;
        }
        ps_flush(&c.ps, PART).await;
        // 400 flushes of the same keys (four 3 KiB inline values): ~4.8 MiB
        // written before the merges rewrite it, ~12 KiB live.
        let pad = vec![b'p'; 3 * 1024];
        let small_flush = |round: u32| {
            let (ps, pad) = (c.ps.clone(), pad.clone());
            async move {
                for k in 0..10 {
                    ps_put(&ps, PART, format!("s{k}").as_bytes(), format!("{round}").as_bytes())
                        .await;
                }
                for p in 0..4 {
                    ps_put(&ps, PART, format!("pad{p}").as_bytes(), &pad).await;
                }
                ps_flush(&ps, PART).await;
            }
        };
        let mut round = 0u32;
        while round < 400 {
            small_flush(round).await;
            round += 1;
        }
        let head = row_extents(&sc, c.row).await[0];
        let resp = c
            .ps
            .call(
                partition_rpc::MSG_ROLL_TAILS,
                partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq {
                    part_id: PART,
                    entries: vec![(c.row, head)],
                }),
            )
            .await
            .expect("roll_tails rpc");
        let r: partition_rpc::RollTailsResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(r.rolled, 1, "roll_tails: {}", r.message);

        // The probe runs at most once a minute; keep flushing meanwhile.
        let before = minor_compaction_runs().2;
        let deadline = Instant::now() + Duration::from_secs(200);
        while row_extents(&sc, c.row).await.contains(&head) {
            assert!(
                Instant::now() < deadline,
                "head extent {head} never reclaimed (reclaims run: {})",
                minor_compaction_runs().2 - before
            );
            small_flush(round).await;
            round += 1;
            compio::time::sleep(Duration::from_millis(300)).await;
        }
        assert!(minor_compaction_runs().2 > before, "dropped without a reclaim");
        for i in (0..1000).step_by(97) {
            let r = ps_get(&c.ps, PART, format!("a{i:04}").as_bytes()).await;
            assert_eq!(r.code, CODE_OK, "a{i:04} lost");
            assert_eq!(r.value, value);
        }
        for k in 0..10 {
            let r = ps_get(&c.ps, PART, format!("s{k}").as_bytes()).await;
            assert_eq!(r.value, format!("{}", round - 1).into_bytes(), "s{k}");
        }
    });
}

/// Every flush queues a minor, so under steady flushes the compaction channel
/// is nearly always ready. A GC dispatched meanwhile must still run.
#[test]
fn a_dispatched_gc_runs_under_steady_flushes() {
    minor_policy();
    const PART: u64 = 1503;
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let c = start_cluster(PART, "uuid-minor-gc").await;
        let sc = stream_client(c.mgr_addr).await;
        let stop = Rc::new(std::cell::Cell::new(false));
        let mut writers = Vec::new();
        for w in 0..2u32 {
            let (ps, stop) = (c.ps.clone(), stop.clone());
            writers.push(compio::runtime::spawn(async move {
                let mut i = 0u32;
                while !stop.get() {
                    ps_put(&ps, PART, format!("w{w}-{}", i % 50).as_bytes(), b"v").await;
                    ps_flush(&ps, PART).await;
                    i += 1;
                }
            }));
        }
        compio::time::sleep(Duration::from_secs(3)).await;
        // Seal the first log extent; the flushes go on into the new tail and
        // move the durable cursor past it, so GC may punch it.
        let e0 = sc.get_stream_info(c.log).await.expect("log info").extent_ids[0];
        let resp = c
            .ps
            .call(
                partition_rpc::MSG_ROLL_TAILS,
                partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq {
                    part_id: PART,
                    entries: vec![(c.log, e0)],
                }),
            )
            .await
            .expect("roll_tails rpc");
        let r: partition_rpc::RollTailsResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(r.rolled, 1, "roll_tails: {}", r.message);
        compio::time::sleep(Duration::from_secs(2)).await;
        let resp = c
            .ps
            .call(
                partition_rpc::MSG_MAINTENANCE,
                partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                    part_id: PART,
                    op: partition_rpc::MAINTENANCE_FORCE_GC,
                    extent_ids: vec![e0],
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
            .expect("forcegc rpc");
        let r: partition_rpc::MaintenanceResp = partition_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(r.code, CODE_OK, "forcegc: {}", r.message);
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut punched = false;
        while Instant::now() < deadline {
            if !sc.get_stream_info(c.log).await.expect("log info").extent_ids.contains(&e0) {
                punched = true;
                break;
            }
            compio::time::sleep(Duration::from_millis(200)).await;
        }
        stop.set(true);
        for w in writers {
            w.await.expect("writer task");
        }
        assert!(punched, "the dispatched GC of {e0} never ran while flushes went on");
    });
}
