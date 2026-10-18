//! Experiment: a partition whose minor compactions wait for the PS-wide
//! compaction permit (held by another partition's long major) under steady
//! flushes. Does its row-stream head-extent reclaim starve, or only wait for
//! the major?
//!
//! B gets a mostly-dead head extent (as in
//! `system_minor_compaction_triggers::a_mostly_dead_head_extent_is_reclaimed`);
//! A's major parks on the test hold with `major_compact_parallelism = 1`, so
//! every compaction on the PS waits behind it. B keeps flushing throughout.
//!
//! Measured (2026-10-18, three runs): during a 60 s hold B flushed ~186
//! times and ran no minor and no reclaim; after the release the head was
//! reclaimed 53-57 s later, after ~175 minors, against 0.3-3 s with no hold.
//! The probe runs only on a tick that finds no minor window, and that stays
//! shut until the minors work off the tables the hold let pile up.

mod support;

use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_partition_server::background::{
    compaction_held_count, minor_compaction_runs, set_compaction_hold,
    set_minor_compaction_paused,
};
use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc::{self, CODE_OK};
use autumn_stream::{ConnPool, StreamClient};
use support::*;

const A: u64 = 1601;
const B: u64 = 1602;

async fn row_extents(sc: &StreamClient, row: u64) -> Vec<u64> {
    sc.get_stream_info(row).await.expect("row stream info").extent_ids
}

#[test]
fn a_reclaim_waits_for_the_major_holding_the_permit_then_runs() {
    experiment(Duration::from_secs(60));
}

/// Control: the same run with the major released at once.
#[test]
fn control_without_a_held_permit() {
    experiment(Duration::ZERO);
}

/// The compaction hold, the params and the run counters are process-wide.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn experiment(hold: Duration) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let ps_params = autumn_partition_server::params();
    ps_params.compact_min_size_bytes.set(1);
    ps_params.major_compact_parallelism.set(1);
    ps_params.reclaim_check_interval_secs.set(5);
    set_compaction_hold(false);

    let mgr_addr = pick_addr();
    let en_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);
    let ps_addr = pick_addr();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("mgr");
        register_node(&mgr, &en_addr.to_string(), "uuid-minor-behind-major").await;
        let one = |mgr| async move { create_stream(mgr, 1).await };
        let (a_log, a_row, a_meta) = (one(&mgr).await, one(&mgr).await, one(&mgr).await);
        upsert_partition(&mgr, A, a_log, a_row, a_meta, b"", b"m").await;
        let (b_log, b_row, b_meta) = (one(&mgr).await, one(&mgr).await, one(&mgr).await);
        upsert_partition(&mgr, B, b_log, b_row, b_meta, b"m", b"\xff").await;
        start_partition_server(1, mgr_addr, ps_addr);
        let router = PsRouter::new(mgr_addr, ps_addr);
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "minor-behind-major-test".to_string(),
            128 * 1024 * 1024,
            Rc::new(ConnPool::new()),
        )
        .await
        .expect("stream client");
        run(&router, &sc, b_row, hold).await;
    });
}

async fn run(router: &PsRouter, sc: &StreamClient, b_row: u64, hold: Duration) {
    // B: one old ~1 MiB table pinned in the row head extent.
    let value = vec![b'a'; 1024];
    for i in 0..1000 {
        psr_put(router, B, format!("x{i:04}").as_bytes(), &value).await;
    }
    psr_flush(router, B).await;
    let head = *row_extents(sc, b_row).await.last().expect("row tail");
    let pad = vec![b'p'; 3 * 1024];
    let mut round = 0u32;
    let small_flush = |round: u32| {
        let pad = pad.clone();
        async move {
            for k in 0..10 {
                psr_put(router, B, format!("y{k}").as_bytes(), format!("{round}").as_bytes()).await;
            }
            for p in 0..4 {
                psr_put(router, B, format!("z{p}").as_bytes(), &pad).await;
            }
            psr_flush(router, B).await;
        }
    };
    while round < 400 {
        small_flush(round).await;
        round += 1;
    }
    // A: some tables for a major to chew on; it parks holding the only permit.
    for t in 0..3 {
        for i in 0..300 {
            psr_put(router, A, format!("a{t}-{i:04}").as_bytes(), &value).await;
        }
        psr_flush(router, A).await;
    }
    // Only A's major may be the compaction that parks: no new minor is
    // requested, and the ones in flight finish first.
    set_minor_compaction_paused(true);
    let mut last = minor_compaction_runs();
    loop {
        compio::time::sleep(Duration::from_secs(2)).await;
        let now = minor_compaction_runs();
        if now == last {
            break;
        }
        last = now;
    }
    let held = compaction_held_count();
    set_compaction_hold(true);
    psr_compact(router, A).await;
    let t0 = Instant::now();
    while compaction_held_count() == held {
        assert!(t0.elapsed() < Duration::from_secs(30), "A's major never parked");
        compio::time::sleep(Duration::from_millis(10)).await;
    }
    set_minor_compaction_paused(false);

    // Seal B's head only now, so its reclaim cannot start before A holds the
    // permit.
    let b_ps = router.client_for(B).await;
    let resp = b_ps
        .call(
            partition_rpc::MSG_ROLL_TAILS,
            partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq {
                part_id: B,
                entries: vec![(b_row, head)],
            }),
        )
        .await
        .expect("roll_tails rpc");
    let r: partition_rpc::RollTailsResp = partition_rpc::rkyv_decode(&resp).expect("decode");
    assert_eq!(r.rolled, 1, "roll_tails: {}", r.message);
    assert!(row_extents(sc, b_row).await.contains(&head));

    // While A holds the permit, B keeps flushing.
    let (minors0, reclaims0) = {
        let r = minor_compaction_runs();
        (r.0 + r.1, r.2)
    };
    let hold_start = Instant::now();
    let mut gone_during_hold = None;
    let mut flushes = 0u32;
    while hold_start.elapsed() < hold {
        small_flush(round).await;
        round += 1;
        flushes += 1;
        if gone_during_hold.is_none() && !row_extents(sc, b_row).await.contains(&head) {
            gone_during_hold = Some(hold_start.elapsed());
        }
        compio::time::sleep(Duration::from_millis(300)).await;
    }
    let (minors1, tick1, reclaims1) = {
        let r = minor_compaction_runs();
        (r.0 + r.1, r.1, r.2)
    };
    eprintln!(
        "EXPERIMENT hold {:?}: B flushes={flushes} minors run={} reclaims run={} head gone at {:?}",
        hold,
        minors1 - minors0,
        reclaims1 - reclaims0,
        gone_during_hold
    );

    // The reclaim is a minor compaction too: it waits for the permit.
    assert_eq!(gone_during_hold, None, "B's head was reclaimed while A held the only permit");

    // Release A; how long until B's head extent is reclaimed?
    set_compaction_hold(false);
    let release = Instant::now();
    let deadline = release + Duration::from_secs(300);
    while row_extents(sc, b_row).await.contains(&head) {
        assert!(
            Instant::now() < deadline,
            "head extent {head} never reclaimed after the major ended (reclaims run: {})",
            minor_compaction_runs().2 - reclaims1
        );
        small_flush(round).await;
        round += 1;
        compio::time::sleep(Duration::from_millis(300)).await;
    }
    let r = minor_compaction_runs();
    eprintln!(
        "EXPERIMENT after release: head reclaimed in {:?}, minors run={} (tick {}), reclaims run={}",
        release.elapsed(),
        r.0 + r.1 - minors1,
        r.1 - tick1,
        r.2 - reclaims1
    );
    for i in (0..1000).step_by(97) {
        let r = psr_get(router, B, format!("x{i:04}").as_bytes()).await;
        assert_eq!(r.code, CODE_OK, "x{i:04} lost");
        assert_eq!(r.value, value);
    }
}
