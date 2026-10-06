//! Can a merged open's replay skip an acknowledged write?
//!
//! A merged survivor skips WAL records by the UNION of both sources' max SST
//! seq, although the sources' seq counters were independent: a survivor whose
//! SSTs end at seq 100 next to a victim at 2000 would lose an unflushed survivor
//! record at seq 101. That needs a source record outside its SSTs when the
//! merge commits. The orchestrated merge's freeze drain prevents it: writes
//! stop, the memtable is flushed and a checkpoint is published at the committed
//! log end, and a failure in any of that refuses the merge.
//!
//! `failed_drain_flush_case`: the survivor's drain flush fails its checkpoint.
//! The merge must be refused, a retry must succeed, and every acknowledged
//! write must survive the merge and a SIGKILL. Ablations: the drain replies OK
//! despite the error -> the survivor's unflushed writes are lost; the failed
//! side stays frozen -> an immediate retry is told "already drained" and
//! merges without flushing (writes lost), and a put after the refusal waits
//! out FREEZE_TTL.
//!
//! `a_merged_open_with_no_resolvable_cursor_loses_nothing`: both sources drain
//! on an empty log tail, so both cursors are `(T, 0)`; the PS dies before the
//! merged open publishes its own checkpoint; the sealed-empty sweep reclaims
//! both T. The next open resolves no cursor and replays the whole log with the
//! union skip, the shape above (survivor seqs 101..200, victim up to 2500). Ablation: the
//! drain does not flush -> those survivor writes are skipped and lost, while the
//! victim's unflushed writes (above the union) survive.
//!
//! The PS is a child process (`support::ChildPs`) so failpoints can be armed in
//! it and it can be SIGKILLed.

mod support;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, MergePartitionsReq, MergePartitionsResp, MSG_MERGE_PARTITIONS,
};
use autumn_rpc::partition_rpc;
use autumn_stream::{ConnPool, StreamClient};
use support::*;

const SURVIVOR: u64 = 1301;
const VICTIM: u64 = 1302;
const PS_ID: u64 = 131;
/// Victim seqs reach 2500 (2000 flushed + 500 not), survivor seqs stay <= 200.
const VICTIM_KEYS: usize = 2000;
const VICTIM_OVERWRITES: usize = 500;
const SURVIVOR_KEYS: usize = 100;

/// The re-executed child PS (`support::ChildPs`).
#[test]
fn child_ps() {
    child_ps_main();
}

/// Every acknowledged write: key -> value a read must return.
type Expected = BTreeMap<String, Vec<u8>>;

fn start_manager_sweeping(mgr_addr: SocketAddr, every: Duration) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new();
            manager.set_sealed_empty_sweep_interval(every);
            let _ = manager.serve(mgr_addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));
}

struct Cluster {
    mgr_addr: SocketAddr,
    survivor_log: u64,
    victim_log: u64,
    survivor_meta: u64,
    _dir: tempfile::TempDir,
}

fn cluster(sweep_every: Option<Duration>) -> Cluster {
    let mgr_addr = pick_addr();
    match sweep_every {
        Some(every) => start_manager_sweeping(mgr_addr, every),
        None => start_manager(mgr_addr),
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let en_addr = pick_addr();
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("mgr");
        register_node(&mgr, &en_addr.to_string(), "uuid-merge-reach").await;
        let mut ids = Vec::new();
        for _ in 0..6 {
            ids.push(create_stream(&mgr, 1).await);
        }
        upsert_partition(&mgr, SURVIVOR, ids[0], ids[1], ids[2], b"", b"m").await;
        upsert_partition(&mgr, VICTIM, ids[3], ids[4], ids[5], b"m", b"\xff").await;
        Cluster {
            mgr_addr,
            survivor_log: ids[0],
            victim_log: ids[3],
            survivor_meta: ids[2],
            _dir: dir,
        }
    })
}

async fn put(router: &PsRouter, part: u64, want: &mut Expected, key: String, value: &[u8]) {
    let c = router.client_for(part).await;
    let resp = c
        .call(
            partition_rpc::MSG_PUT,
            partition_rpc::rkyv_encode(&partition_rpc::PutReq {
                part_id: part,
                key: key.as_bytes().to_vec(),
                value: value.to_vec(),
                expires_at: 0,
                region_epoch: 0,
                inode_hint: 0,
                lease_epoch: 0,
            }),
        )
        .await
        .expect("put rpc");
    let r: partition_rpc::PutResp = partition_rpc::rkyv_decode(&resp).expect("decode PutResp");
    assert_eq!(r.code, partition_rpc::CODE_OK, "put {key}: {}", r.message);
    want.insert(key, value.to_vec());
}

/// Victim: 2000 flushed keys, then 500 unflushed overwrites (seqs 2001..2500).
/// Survivor: 100 flushed keys, then 50 overwrites and 50 new keys, unflushed
/// (seqs 101..200 — all at or below the victim's flushed max).
async fn write_sources(router: &PsRouter, want: &mut Expected) {
    for i in 0..VICTIM_KEYS {
        put(router, VICTIM, want, format!("n-{i:05}"), b"v1").await;
    }
    psr_flush(router, VICTIM).await;
    for i in 0..VICTIM_OVERWRITES {
        put(router, VICTIM, want, format!("n-{i:05}"), b"v2").await;
    }
    for i in 0..SURVIVOR_KEYS {
        put(router, SURVIVOR, want, format!("a-{i:05}"), b"s1").await;
    }
    psr_flush(router, SURVIVOR).await;
    for i in 0..SURVIVOR_KEYS / 2 {
        put(router, SURVIVOR, want, format!("a-{i:05}"), b"s2").await;
    }
    for i in SURVIVOR_KEYS..SURVIVOR_KEYS * 3 / 2 {
        put(router, SURVIVOR, want, format!("a-{i:05}"), b"s1").await;
    }
}

async fn merge(mgr_addr: SocketAddr) -> MergePartitionsResp {
    let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
        .await
        .expect("admin mgr");
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
    rkyv_decode(&bytes).expect("merge resp")
}

/// Every acknowledged write reads back, and the range holds exactly those keys.
async fn assert_state(router: &PsRouter, want: &Expected, when: &str) {
    let c = router.client_for(SURVIVOR).await;
    let mut wrong = Vec::new();
    for (key, value) in want {
        let r = ps_get(&c, SURVIVOR, key.as_bytes()).await;
        if r.code != partition_rpc::CODE_OK || r.value != *value {
            wrong.push(format!(
                "{key}: want {:?}, got code {} {:?}",
                String::from_utf8_lossy(value),
                r.code,
                String::from_utf8_lossy(&r.value)
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "{when}: {} of {} acknowledged writes wrong, first: {:?}",
        wrong.len(),
        want.len(),
        &wrong[..wrong.len().min(5)]
    );
    let r = ps_range(&c, SURVIVOR, b"", b"", 10_000).await;
    let got: Vec<String> = r
        .entries
        .iter()
        .map(|e| String::from_utf8_lossy(&e.key).into_owned())
        .collect();
    let keys: Vec<String> = want.keys().cloned().collect();
    assert_eq!(got, keys, "{when}: range returned a different key set");
}

async fn wait_serving(router: &PsRouter, key: &str) {
    let started = Instant::now();
    loop {
        if let Ok(c) = router.try_client_for(SURVIVOR).await {
            if ps_get(&c, SURVIVOR, key.as_bytes()).await.code == partition_rpc::CODE_OK {
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

async fn stream_client(mgr_addr: SocketAddr) -> Rc<StreamClient> {
    StreamClient::connect(
        &mgr_addr.to_string(),
        "merge-reachability-probe".to_string(),
        128 * 1024 * 1024,
        Rc::new(ConnPool::new()),
    )
    .await
    .expect("stream client")
}

/// Seal the log tail and start an empty one; returns the new tail. No append
/// is in flight: every put above was awaited and the writer is idle.
async fn roll_log_tail(mgr_addr: SocketAddr, log: u64) -> u64 {
    let sc = stream_client(mgr_addr).await;
    sc.seal_and_roll_tail(log).await.expect("roll log tail");
    *sc.get_stream_info(log)
        .await
        .expect("log info")
        .extent_ids
        .last()
        .expect("log tail")
}

/// The survivor's drain flush fails its checkpoint (the imm stays queued).
/// `write_after_refusal`: put to the survivor at once, before retrying;
/// otherwise retry the merge at once, before anything re-flushes the imm.
fn failed_drain_flush_case(write_after_refusal: bool) {
    let c = cluster(None);
    let ps_addr = pick_addr();
    // Flushes: 1 victim, 2 survivor, 3 the victim's drain, 4 the survivor's.
    let mut child = ChildPs::spawn(
        PS_ID,
        c.mgr_addr,
        ps_addr,
        ChildFailpoints {
            flush_checkpoint_nth: 4,
            ..Default::default()
        },
    );
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let router = PsRouter::new(c.mgr_addr, ps_addr);
        let mut want = Expected::new();
        write_sources(&router, &mut want).await;

        let first = merge(c.mgr_addr).await;
        let mut merged = first.code == partition_rpc::CODE_OK;
        if !merged && write_after_refusal {
            put(&router, SURVIVOR, &mut want, "a-99999".to_string(), b"after-refusal").await;
        }
        let deadline = Instant::now() + Duration::from_secs(60);
        while !merged {
            assert!(Instant::now() < deadline, "the retried merge never succeeded");
            let r = merge(c.mgr_addr).await;
            merged = r.code == partition_rpc::CODE_OK;
            if !merged {
                compio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        wait_serving(&router, "n-00000").await;
        assert_state(&router, &want, "after the merge").await;
        child.kill();

        let ps2_addr = pick_addr();
        let _child2 = ChildPs::spawn(PS_ID, c.mgr_addr, ps2_addr, ChildFailpoints::default());
        let router = PsRouter::new(c.mgr_addr, ps2_addr);
        wait_serving(&router, "n-00000").await;
        assert_state(&router, &want, "after SIGKILL and reopen").await;

        // Last, so an ablation that lets the merge through fails on the data.
        assert_ne!(first.code, partition_rpc::CODE_OK, "the first merge must be refused");
        assert!(
            first.message.contains(&format!("partition {SURVIVOR}"))
                && first.message.contains("freeze drain flush failed"),
            "the refusal must come from the survivor's drain: {}",
            first.message
        );
    });
}

/// A side whose drain failed must not stay frozen: a retry would find it
/// "already drained" and merge without flushing the queued imm.
#[test]
fn a_merge_retried_after_a_failed_drain_flush_loses_nothing() {
    failed_drain_flush_case(false);
}

/// ...and it takes writes at once instead of waiting out FREEZE_TTL.
#[test]
fn a_side_refused_by_its_failed_drain_takes_writes_at_once() {
    failed_drain_flush_case(true);
}

#[test]
fn a_merged_open_with_no_resolvable_cursor_loses_nothing() {
    let c = cluster(Some(Duration::from_secs(1)));
    let ps_addr = pick_addr();
    let mut child = ChildPs::spawn(
        PS_ID,
        c.mgr_addr,
        ps_addr,
        ChildFailpoints {
            merged_checkpoint: true,
            ..Default::default()
        },
    );
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let router = PsRouter::new(c.mgr_addr, ps_addr);
        let mut want = Expected::new();
        write_sources(&router, &mut want).await;
        // Each drain then publishes its cursor at `(empty tail, 0)`.
        let survivor_tail = roll_log_tail(c.mgr_addr, c.survivor_log).await;
        let victim_tail = roll_log_tail(c.mgr_addr, c.victim_log).await;

        let r = merge(c.mgr_addr).await;
        assert_eq!(r.code, partition_rpc::CODE_OK, "merge: {}", r.message);

        // The merge sealed both empty tails behind its new one; the sweep
        // reclaims them while every merged open fails before publishing.
        let sc = stream_client(c.mgr_addr).await;
        let deadline = Instant::now() + Duration::from_secs(60);
        let log = loop {
            let log = sc
                .get_stream_info(c.survivor_log)
                .await
                .expect("merged log info")
                .extent_ids;
            if !log.contains(&survivor_tail) && !log.contains(&victim_tail) {
                break log;
            }
            assert!(
                Instant::now() < deadline,
                "the sweep never reclaimed the drained tails {survivor_tail}, {victim_tail}: {log:?}"
            );
            compio::time::sleep(Duration::from_millis(500)).await;
        };
        let meta = sc
            .get_stream_info(c.survivor_meta)
            .await
            .expect("meta info")
            .extent_ids;
        let mut cursors = Vec::new();
        for eid in meta {
            let (payload, _) = sc.read_bytes_from_extent(eid, 0, 0).await.expect("read meta");
            if payload.len() >= 4 {
                let t = decode_last_table_locations(&payload);
                cursors.push((t.vp_extent_id, t.vp_offset));
            }
        }
        assert_eq!(
            cursors,
            vec![(survivor_tail, 0), (victim_tail, 0)],
            "the meta stream must still hold the two drain checkpoints"
        );
        assert!(
            cursors.iter().all(|(eid, _)| !log.contains(eid)),
            "no cursor may resolve: {cursors:?} vs log {log:?}"
        );
        child.kill();

        let ps2_addr = pick_addr();
        let _child2 = ChildPs::spawn(PS_ID, c.mgr_addr, ps2_addr, ChildFailpoints::default());
        let router = PsRouter::new(c.mgr_addr, ps2_addr);
        wait_serving(&router, "n-00000").await;
        assert_state(&router, &want, "after a whole-log merged replay").await;
    });
}
