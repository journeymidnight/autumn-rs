//! A split child's reopen must not replay its sibling's half of the parent's
//! WAL into its own memtable. Replay skips a record iff its seq is at or below
//! the loaded tables' max seq, and does not filter by range.
//!
//! The parent writes both halves into E1 and the right half again into E2 (its
//! highest seqs), then splits on an empty tail T. The left child L
//! major-compacts (dropping the right half), GC takes E2 out of L's log, T is
//! swept, and the PS is SIGKILLed before L flushes: L's reopen resolves no
//! cursor or stamp and replays its whole log, E1 included. The right child
//! deletes and overwrites a key; then the two merge.
//!
//! What holds it: a compaction output covers its inputs' max seq
//! (`cover_seq`). Ablation (outputs carry only the seqs they keep): L takes
//! the right half of E1 back in and, after the merge, the deleted key reads its
//! pre-split value.

mod support;

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

const PARENT: u64 = 1401;
const PS_ID: u64 = 141;
const KEYS: usize = 50;

#[test]
fn child_ps() {
    child_ps_main();
}

fn start_manager_sweeping(mgr_addr: SocketAddr, every: Duration) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new();
            manager
                .params()
                .sealed_empty_sweep_interval_secs
                .set(every.as_secs());
            let _ = manager.serve(mgr_addr).await;
        });
    });
    support::wait_accepting(mgr_addr, "start_manager_sweeping");
}

async fn stream_client(mgr_addr: SocketAddr) -> Rc<StreamClient> {
    StreamClient::connect(
        &mgr_addr.to_string(),
        "split-replay-oor-probe".to_string(),
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

/// The newest checkpoint: (cursor, the tables it lists).
async fn last_checkpoint(sc: &StreamClient, meta: u64) -> ((u64, u64), Vec<(u64, u64)>) {
    let eids = extents(sc, meta).await;
    assert_eq!(eids.len(), 1, "one meta extent expected: {eids:?}");
    let (payload, _) = sc
        .read_bytes_from_extent(eids[0], 0, 0)
        .await
        .expect("read meta");
    let t = decode_last_table_locations(&payload);
    (
        (t.vp_extent_id, t.vp_offset),
        t.locs.iter().map(|l| (l.extent_id, l.offset)).collect(),
    )
}

async fn put(router: &PsRouter, part: u64, key: &str, value: &[u8]) {
    let c = router.client_for(part).await;
    ps_put(&c, part, key.as_bytes(), value).await;
}

async fn get(router: &PsRouter, part: u64, key: &str) -> (u8, String) {
    let c = router.client_for(part).await;
    let r = ps_get(&c, part, key.as_bytes()).await;
    (r.code, String::from_utf8_lossy(&r.value).into_owned())
}

async fn range_keys(router: &PsRouter, part: u64) -> Vec<String> {
    let c = router.client_for(part).await;
    ps_range(&c, part, b"", b"", 10_000)
        .await
        .entries
        .iter()
        .map(|e| String::from_utf8_lossy(&e.key).into_owned())
        .collect()
}

async fn split_at(router: &PsRouter, part: u64, at: &[u8]) {
    let c = router.client_for(part).await;
    let resp = c
        .call(
            partition_rpc::MSG_SPLIT_PART,
            partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq {
                part_id: part,
                at_key: Some(at.to_vec()),
                op_id: 0,
            }),
        )
        .await
        .expect("split");
    let r: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&resp).expect("decode");
    assert_eq!(r.code, partition_rpc::CODE_OK, "split: {}", r.message);
}

async fn merge(mgr_addr: SocketAddr, survivor: u64, victim: u64) -> MergePartitionsResp {
    let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
        .await
        .expect("admin mgr");
    let bytes = mgr
        .call(
            MSG_MERGE_PARTITIONS,
            rkyv_encode(&MergePartitionsReq {
                survivor_part_id: survivor,
                victim_part_id: victim,
                force: true,
            }),
        )
        .await
        .expect("merge call");
    rkyv_decode(&bytes).expect("merge resp")
}

/// Major-compact and wait until a new checkpoint is published.
async fn compact_and_wait(router: &PsRouter, sc: &StreamClient, part: u64, meta: u64) {
    let before = last_checkpoint(sc, meta).await;
    psr_compact(router, part).await;
    let started = Instant::now();
    loop {
        let now = last_checkpoint(sc, meta).await;
        if now != before {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "compaction of {part} never published: {before:?}"
        );
        compio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_serving(router: &PsRouter, part: u64, key: &str) {
    let started = Instant::now();
    loop {
        if let Ok(c) = router.try_client_for(part).await {
            if try_ps_get(&c, part, key.as_bytes()).await.is_ok_and(|r| {
                r.code == partition_rpc::CODE_OK || r.code == partition_rpc::CODE_NOT_FOUND
            }) {
                return;
            }
        }
        assert!(started.elapsed() < Duration::from_secs(60), "partition {part} never served");
        compio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The right child: (part id, meta stream), and how many partitions there are.
async fn right_child(mgr_addr: SocketAddr) -> (u64, u64, usize) {
    let mgr = RpcClient::connect(mgr_addr).await.expect("mgr");
    let regions = get_regions(&mgr).await;
    let n = regions.regions.len();
    let (_, r) = regions.regions.iter().find(|(_, r)| r.part_id != PARENT).expect("right child");
    (r.part_id, r.meta_stream, n)
}

async fn force_gc(router: &PsRouter, part: u64, extent_ids: Vec<u64>) {
    let c = router.client_for(part).await;
    let resp = c
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id: part,
                op: partition_rpc::MAINTENANCE_FORCE_GC,
                extent_ids,
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
}

/// Seal and roll `part`'s log tail through its own writer (a roll from another
/// client leaves the writer appending past the seal).
async fn roll_log(router: &PsRouter, sc: &StreamClient, part: u64, log: u64) -> u64 {
    let tail = *extents(sc, log).await.last().unwrap();
    let c = router.client_for(part).await;
    let resp = c
        .call(
            partition_rpc::MSG_ROLL_TAILS,
            partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq {
                part_id: part,
                entries: vec![(log, tail)],
            }),
        )
        .await
        .expect("roll_tails rpc");
    let r: partition_rpc::RollTailsResp = partition_rpc::rkyv_decode(&resp).expect("decode");
    assert_eq!(r.code, partition_rpc::CODE_OK, "roll_tails: {}", r.message);
    assert_eq!(r.rolled, 1, "the log tail must roll");
    let now = *extents(sc, log).await.last().unwrap();
    assert_ne!(now, tail);
    now
}

async fn wait_gone(sc: &StreamClient, stream: u64, eid: u64, what: &str) {
    let started = Instant::now();
    while extents(sc, stream).await.contains(&eid) {
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "{what}: {eid} never left stream {stream}"
        );
        compio::time::sleep(Duration::from_millis(300)).await;
    }
}

#[test]
fn a_split_child_whole_log_replay_skips_its_siblings_records() {
    let mgr_addr = pick_addr();
    start_manager_sweeping(mgr_addr, Duration::from_secs(1));
    let dir = tempfile::tempdir().expect("tempdir");
    let en_addr = pick_addr();
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);
    let (log, meta) = compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("mgr");
        register_node(&mgr, &en_addr.to_string(), "uuid-split-replay-oor").await;
        let log = create_stream(&mgr, 1).await;
        let row = create_stream(&mgr, 1).await;
        let meta = create_stream(&mgr, 1).await;
        upsert_partition(&mgr, PARENT, log, row, meta, b"", b"\xff").await;
        (log, meta)
    });

    let ps_addr = pick_addr();
    let mut child = ChildPs::spawn(
        PS_ID,
        mgr_addr,
        ps_addr,
        ChildFailpoints {
            minor_paused: true,
            ..Default::default()
        },
    );
    let right = compio::runtime::Runtime::new().unwrap().block_on(async {
        let router = PsRouter::new(mgr_addr, ps_addr);
        let sc = stream_client(mgr_addr).await;
        // E1: both halves. E2: the right half again, the parent's highest seqs.
        for i in 0..KEYS {
            put(&router, PARENT, &format!("a-{i:05}"), b"p1").await;
        }
        for i in 0..KEYS {
            put(&router, PARENT, &format!("n-{i:05}"), b"p1").await;
        }
        roll_log(&router, &sc, PARENT, log).await;
        let after_e1 = extents(&sc, log).await;
        let e1 = after_e1[after_e1.len() - 2];
        for i in 0..KEYS {
            put(&router, PARENT, &format!("n-{i:05}"), b"p2").await;
        }
        psr_flush(&router, PARENT).await;
        eprintln!("PROBE parent checkpoint after flush: {:?}", last_checkpoint(&sc, meta).await);
        // Drain on an empty tail T: the children's cursor is (T, 0).
        roll_log(&router, &sc, PARENT, log).await;
        let plog = extents(&sc, log).await;
        let t = *plog.last().unwrap();
        let e2 = plog[plog.len() - 2];
        eprintln!("PROBE parent log: after E1 roll {after_e1:?}, now {plog:?}; E2={e2} T={t}");

        split_at(&router, PARENT, b"m").await;
        let (right, right_meta, n) = right_child(mgr_addr).await;
        assert_eq!(n, 2);
        wait_serving(&router, right, "n-00000").await;
        let ck = last_checkpoint(&sc, meta).await;
        eprintln!("PROBE L after split: log {:?} checkpoint {ck:?}", extents(&sc, log).await);
        assert_eq!(ck.0, (t, 0), "L's cursor must be the empty tail");

        // L drops every right-half key (the parent's highest seqs).
        compact_and_wait(&router, &sc, PARENT, meta).await;
        let ck = last_checkpoint(&sc, meta).await;
        eprintln!(
            "PROBE L after major compaction: log {:?} checkpoint {ck:?}",
            extents(&sc, log).await
        );
        assert_eq!(ck.0, (t, 0), "L's compaction keeps the cursor (T, 0)");

        // Take E2 (where every table's stamp points) out of L's log, so
        // neither the cursor nor a stamp resolves once T goes too.
        force_gc(&router, PARENT, vec![e2]).await;
        wait_gone(&sc, log, e2, "GC of E2 from L").await;

        // R: delete one key, overwrite another, flush, major-compact.
        {
            let c = router.client_for(right).await;
            let r = ps_delete(&c, right, b"n-00001").await;
            assert_eq!(r.code, partition_rpc::CODE_OK, "delete: {}", r.message);
        }
        put(&router, right, "n-00002", b"r2").await;
        psr_flush(&router, right).await;
        compact_and_wait(&router, &sc, right, right_meta).await;

        // L takes a write (rolls the sealed T away), never flushed.
        put(&router, PARENT, "a-live", b"l1").await;
        wait_gone(&sc, log, t, "sweep of T from L").await;
        let ck = last_checkpoint(&sc, meta).await;
        let l_log = extents(&sc, log).await;
        eprintln!("PROBE L log before the kill: {l_log:?} checkpoint {ck:?}");
        assert_eq!(ck.0, (t, 0));
        // The reopen's whole-log walk passes over the parent's records.
        assert!(l_log.contains(&e1), "E1 must still be in L's log: {l_log:?}");
        right
    });
    child.kill();

    let ps2_addr = pick_addr();
    let _child2 = ChildPs::spawn(
        PS_ID,
        mgr_addr,
        ps2_addr,
        ChildFailpoints {
            minor_paused: true,
            ..Default::default()
        },
    );
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let router = PsRouter::new(mgr_addr, ps2_addr);
        wait_serving(&router, PARENT, "a-live").await;
        wait_serving(&router, right, "n-00000").await;
        let l_keys = range_keys(&router, PARENT).await;
        eprintln!(
            "PROBE L range after reopen ({} keys): first {:?} last {:?}",
            l_keys.len(),
            l_keys.first(),
            l_keys.last()
        );
        let oor = l_keys.iter().filter(|k| k.as_str() >= "m").count();

        let m = merge(mgr_addr, PARENT, right).await;
        eprintln!("PROBE merge: code {} {}", m.code, m.message);
        assert_eq!(m.code, partition_rpc::CODE_OK, "merge: {}", m.message);
        wait_serving(&router, PARENT, "n-00000").await;

        let deleted = get(&router, PARENT, "n-00001").await;
        let updated = get(&router, PARENT, "n-00002").await;
        let other = get(&router, PARENT, "n-00003").await;
        let left = get(&router, PARENT, "a-00003").await;
        let live = get(&router, PARENT, "a-live").await;
        let all = range_keys(&router, PARENT).await;
        eprintln!(
            "PROBE after merge: n-00001 {deleted:?} n-00002 {updated:?} n-00003 {other:?} \
             a-00003 {left:?} a-live {live:?} keys {}",
            all.len()
        );
        let mut wrong = Vec::new();
        if oor != 0 {
            wrong.push(format!("L lists {oor} right-half keys after its reopen"));
        }
        if deleted.0 != partition_rpc::CODE_NOT_FOUND {
            wrong.push(format!("n-00001 (deleted by R) reads {deleted:?}"));
        }
        if updated != (partition_rpc::CODE_OK, "r2".to_string()) {
            wrong.push(format!("n-00002 (overwritten by R) reads {updated:?}"));
        }
        if other != (partition_rpc::CODE_OK, "p2".to_string()) {
            wrong.push(format!("n-00003 reads {other:?}"));
        }
        if left != (partition_rpc::CODE_OK, "p1".to_string()) {
            wrong.push(format!("a-00003 reads {left:?}"));
        }
        if live != (partition_rpc::CODE_OK, "l1".to_string()) {
            wrong.push(format!("a-live reads {live:?}"));
        }
        // a-* + a-live + n-* without n-00001.
        if all.len() != 2 * KEYS {
            wrong.push(format!("range holds {} keys, want {}", all.len(), 2 * KEYS));
        }
        assert!(wrong.is_empty(), "{wrong:?}");
    });
    drop(dir);
}
