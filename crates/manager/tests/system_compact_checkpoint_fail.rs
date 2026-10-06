//! A checkpoint is published BEFORE the in-memory table list changes, so a
//! failed checkpoint append leaves memory exactly as durable as it was, and a
//! crash at any point afterwards recovers every acknowledged value and delete.
//!
//! `crash_after_failed_compaction_checkpoint_loses_nothing`: two flushed SSTs
//! hold big values (ValuePointers into log extent E0), deletes of some of
//! them, and small inline values; E0 is sealed and a third SST puts the
//! durable cursor past it. A major compaction writes its output and its
//! checkpoint fails (test failpoint): the partition must keep serving the
//! three inputs, the output left as orphan bytes in the row stream. The test
//! then overwrites and deletes keys, force-GCs E0 (relocating the live values
//! and punching it), writes again, and SIGKILLs the PS; the reopened
//! partition must serve the exact acknowledged state. A later major
//! compaction then publishes and truncates past both the inputs and the
//! orphaned output, and a second reopen must serve the same state.
//!
//! `unlisted_flush_case`: a flush whose checkpoint fails, then a compaction
//! whose checkpoint fails (and each alone as a control). When either changed
//! memory before its append, the flush's SST sat in the table list with no
//! durable checkpoint naming it, the failed compaction's output carried that
//! flush's vp_head, GC's floor rose past the durable cursor, and a crash lost
//! the inline writes and deletes in between.
//!
//! The PS runs as a child process (`support::ChildPs`, this test binary
//! re-executed) so the failpoints can be armed in it and it can be SIGKILLed:
//! a graceful stop would flush and publish a checkpoint on the way out.
//!
//! Ablation: changing the table list before the append (flush and
//! compaction) turns both the compaction case (it serves the outputs) and the
//! flush-then-compaction case (20 puts lost, 5 deletes back) red.

mod support;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{self, StreamInfoReq, StreamInfoResp, MSG_STREAM_INFO};
use autumn_rpc::partition_rpc;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

const PART: u64 = 951;
const PS_ID: u64 = 79;
/// Above `VALUE_THROTTLE` (4 KiB): stored behind a ValuePointer in the log.
const BIG_LEN: usize = 8 * 1024;
const SMALL_KEYS: u32 = 40;
fn big(tag: u8) -> Vec<u8> {
    vec![tag; BIG_LEN]
}

/// The re-executed child PS (`support::ChildPs`).
#[test]
fn child_ps() {
    child_ps_main();
}

async fn stream_extents(mgr: &RpcClient, stream_id: u64) -> Vec<u64> {
    let resp = mgr
        .call(
            MSG_STREAM_INFO,
            manager_rpc::rkyv_encode(&StreamInfoReq {
                stream_ids: vec![stream_id],
            }),
        )
        .await
        .expect("stream_info rpc");
    let resp: StreamInfoResp = manager_rpc::rkyv_decode(&resp).expect("decode StreamInfoResp");
    assert_eq!(resp.code, manager_rpc::CODE_OK, "stream_info: {}", resp.message);
    resp.streams
        .into_iter()
        .find(|(id, _)| *id == stream_id)
        .expect("stream in response")
        .1
        .extent_ids
}

async fn roll_tails(ps: &RpcClient, entries: Vec<(u64, u64)>) -> u32 {
    let resp = ps
        .call(
            partition_rpc::MSG_ROLL_TAILS,
            partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq {
                part_id: PART,
                entries,
            }),
        )
        .await
        .expect("roll_tails rpc");
    let resp: partition_rpc::RollTailsResp =
        partition_rpc::rkyv_decode(&resp).expect("decode RollTailsResp");
    assert_eq!(resp.code, partition_rpc::CODE_OK, "roll_tails: {}", resp.message);
    resp.rolled
}

async fn force_gc(ps: &RpcClient, extent_ids: Vec<u64>) {
    let resp = ps
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id: PART,
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
    let r: partition_rpc::MaintenanceResp =
        partition_rpc::rkyv_decode(&resp).expect("decode MaintenanceResp");
    assert_eq!(r.code, partition_rpc::CODE_OK, "forcegc: {}", r.message);
}

/// The partition's IN-MEMORY live SSTs, as their vp_heads.
async fn live_sst_count(ps: &RpcClient) -> usize {
    let resp = ps
        .call(
            partition_rpc::MSG_DIAG_PARTITION_VP,
            partition_rpc::rkyv_encode(&partition_rpc::DiagPartitionVpReq { part_id: PART }),
        )
        .await
        .expect("diag partition vp");
    let r: partition_rpc::DiagPartitionVpResp =
        partition_rpc::rkyv_decode(&resp).expect("decode DiagPartitionVpResp");
    assert_eq!(r.code, partition_rpc::CODE_OK, "diag partition vp: {}", r.message);
    r.sst_vp_heads.len()
}

/// Wait for a major compaction to start writing: it rolls the row tail first
/// (`before` is the tail it rolls off; a successful one may then truncate it
/// away). GC dispatched after this runs after the compaction (one maintenance
/// task).
async fn wait_row_roll(mgr: &RpcClient, row: u64, before: u64) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while stream_extents(mgr, row).await.last() == Some(&before) {
        assert!(std::time::Instant::now() < deadline, "the compaction never rolled the row tail");
        compio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The row extents of the SSTs the DURABLE checkpoint lists.
async fn checkpoint_sst_extents(sc: &StreamClient, meta: u64) -> Vec<u64> {
    let info = sc.get_stream_info(meta).await.expect("meta stream info");
    let eid = *info.extent_ids.last().expect("meta extent");
    let (payload, _) = sc.read_bytes_from_extent(eid, 0, 0).await.expect("read meta");
    decode_last_table_locations(&payload)
        .locs
        .iter()
        .map(|l| l.extent_id)
        .collect()
}

/// Every acknowledged write, as the value a read must return (`None` = deleted).
type Expected = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

async fn put(ps: &RpcClient, want: &mut Expected, key: &str, value: Vec<u8>) {
    ps_put(ps, PART, key.as_bytes(), &value).await;
    want.insert(key.as_bytes().to_vec(), Some(value));
}

async fn delete(ps: &RpcClient, want: &mut Expected, key: &str) {
    let r = ps_delete(ps, PART, key.as_bytes()).await;
    assert_eq!(r.code, partition_rpc::CODE_OK, "delete {key}: {}", r.message);
    want.insert(key.as_bytes().to_vec(), None);
}

async fn assert_state(ps: &RpcClient, want: &Expected, when: &str) {
    for (key, value) in want {
        let k = String::from_utf8_lossy(key);
        let r = ps_get(ps, PART, key).await;
        match value {
            Some(v) => {
                assert_eq!(r.code, partition_rpc::CODE_OK, "{when}: get {k}: {}", r.message);
                assert!(r.value == *v, "{when}: {k} has the wrong value");
            }
            None => assert_eq!(
                r.code,
                partition_rpc::CODE_NOT_FOUND,
                "{when}: deleted {k} came back ({} bytes)",
                r.value.len()
            ),
        }
    }
    let r = ps_range(ps, PART, b"", b"", 10_000).await;
    let got: Vec<Vec<u8>> = r.entries.iter().map(|e| e.key.clone()).collect();
    let live: Vec<Vec<u8>> = want
        .iter()
        .filter(|(_, v)| v.is_some())
        .map(|(k, _)| k.clone())
        .collect();
    assert_eq!(got, live, "{when}: range returned a different key set");
}

/// Connect to a reopened PS and wait until it serves `PART`. On timeout, say
/// whether the durable checkpoint names row extents the row stream lost.
async fn reopened(
    addr: SocketAddr,
    mgr: &RpcClient,
    sc: &StreamClient,
    row: u64,
    meta: u64,
) -> Rc<RpcClient> {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(ps) = RpcClient::connect(addr).await {
            if ps_get(&ps, PART, b"x0").await.code == partition_rpc::CODE_OK {
                return ps;
            }
        }
        if std::time::Instant::now() >= deadline {
            let durable = checkpoint_sst_extents(sc, meta).await;
            let members = stream_extents(mgr, row).await;
            panic!(
                "reopened partition never served; durable checkpoint lists SSTs in \
                 row extents {durable:?}, row stream holds {members:?}"
            );
        }
        compio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[test]
fn crash_after_failed_compaction_checkpoint_loses_nothing() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 951).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, PART, log, row, meta, b"a", b"z").await;
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "compact-ckpt-fail-probe".to_string(),
            1 << 20,
            Rc::new(ConnPool::new()),
        )
        .await
        .expect("stream client");

        let ps1_addr = pick_addr();
        let mut child = ChildPs::spawn(
            PS_ID,
            mgr_addr,
            ps1_addr,
            ChildFailpoints {
                compaction_checkpoint: true,
                ..Default::default()
            },
        );
        let ps = RpcClient::connect(ps1_addr).await.expect("connect ps1");
        let mut want = Expected::new();

        // SST 1 and 2, both with their WAL in E0. "c" stays live, "o" is
        // overwritten inside the window, "d" is deleted by SST 2 (the major
        // compaction drops both its put and its delete), "s" is inline.
        for i in 0..4u8 {
            put(&ps, &mut want, &format!("c{i}"), big(0xc0 + i)).await;
            put(&ps, &mut want, &format!("d{i}"), big(0xd0 + i)).await;
            put(&ps, &mut want, &format!("o{i}"), big(0xa0 + i)).await;
        }
        for i in 0..SMALL_KEYS {
            put(&ps, &mut want, &format!("s{i:02}"), format!("small-{i}").into_bytes()).await;
        }
        ps_flush(&ps, PART).await;
        for i in 0..4u8 {
            delete(&ps, &mut want, &format!("d{i}")).await;
        }
        ps_flush(&ps, PART).await;

        // Seal E0; SST 3 puts the durable cursor in the new tail, so E0 is
        // strictly below GC's replay floor.
        let e0 = *stream_extents(&mgr, log).await.last().expect("log tail");
        assert_eq!(roll_tails(&ps, vec![(log, e0)]).await, 1, "roll the log tail");
        put(&ps, &mut want, "x0", b"after-roll".to_vec()).await;
        ps_flush(&ps, PART).await;
        let inputs_row = checkpoint_sst_extents(&sc, meta).await;
        assert_eq!(inputs_row.len(), 3, "three durable SSTs before the compaction");
        assert_eq!(live_sst_count(&ps).await, 3);

        // The major compaction writes its output; its checkpoint fails.
        let row_before_compaction = *stream_extents(&mgr, row).await.last().expect("row tail");
        ps_compact(&ps, PART).await;
        wait_row_roll(&mgr, row, row_before_compaction).await;

        // Inside the window: writes, GC of E0, writes.
        for i in 0..4u8 {
            put(&ps, &mut want, &format!("o{i}"), big(0xb0 + i)).await;
            put(&ps, &mut want, &format!("n{i}"), big(0xe0 + i)).await;
        }
        for i in 0..10u32 {
            delete(&ps, &mut want, &format!("s{i:02}")).await;
        }
        force_gc(&ps, vec![e0]).await;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while stream_extents(&mgr, log).await.contains(&e0) {
            assert!(std::time::Instant::now() < deadline, "GC never punched E0");
            compio::time::sleep(Duration::from_millis(200)).await;
        }
        put(&ps, &mut want, "c0", big(0x11)).await;
        delete(&ps, &mut want, "c1").await;
        put(&ps, &mut want, "d0", big(0x22)).await;
        assert_state(&ps, &want, "before the crash").await;

        // The compaction finished before GC ran (one task): it failed, and the
        // partition still serves what the durable checkpoint lists.
        assert_eq!(checkpoint_sst_extents(&sc, meta).await, inputs_row);
        assert_eq!(live_sst_count(&ps).await, 3, "a failed checkpoint must not swap");
        drop(ps);
        child.kill();

        let ps2_addr = pick_addr();
        let (ps2_stop, ps2_join) = start_partition_server_stoppable(PS_ID, mgr_addr, ps2_addr);
        let ps = reopened(ps2_addr, &mgr, &sc, row, meta).await;
        assert_state(&ps, &want, "after the crash").await;

        // A major compaction publishes, then truncates the row stream past the
        // inputs' extent and the orphaned output's.
        let row_before = stream_extents(&mgr, row).await;
        ps_compact(&ps, PART).await;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let durable = checkpoint_sst_extents(&sc, meta).await;
            let members = stream_extents(&mgr, row).await;
            if durable.iter().all(|e| !inputs_row.contains(e))
                && row_before.iter().all(|e| !members.contains(e))
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "compaction never published and truncated: checkpoint on {durable:?}, \
                 row stream {members:?} (was {row_before:?})"
            );
            compio::time::sleep(Duration::from_millis(200)).await;
        }
        assert_state(&ps, &want, "after the later compaction").await;
        drop(ps);
        ps2_stop.shutdown();
        ps2_join.join().expect("join ps2");

        let ps3_addr = pick_addr();
        start_partition_server(PS_ID, mgr_addr, ps3_addr);
        let ps = reopened(ps3_addr, &mgr, &sc, row, meta).await;
        assert_state(&ps, &want, "after the second reopen").await;
    });
}

/// Raw flush whose outcome the caller judges (`ps_flush` asserts success).
async fn flush_raw(ps: &RpcClient) -> bool {
    let resp = ps
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id: PART,
                op: partition_rpc::MAINTENANCE_FLUSH,
                extent_ids: vec![],
                gc_ratio: None,
                gc_max_size: None,
                gc_stream_debt: None,
                gc_dead_bytes_high: None,
                gc_empty_only: false,
                gc_policy_is_standing: false,
                op_id: 0,
            }),
        )
        .await;
    match resp {
        Ok(b) => {
            partition_rpc::rkyv_decode::<partition_rpc::MaintenanceResp>(&b)
                .map(|r| r.code == partition_rpc::CODE_OK)
                .unwrap_or(false)
        }
        Err(_) => false,
    }
}

/// A flush whose checkpoint fails, then a major compaction whose checkpoint
/// fails, then GC of the log extent holding the inline puts and deletes
/// written between the durable cursor C and that flush's vp_head H, then a
/// crash. Had either changed memory first, GC's floor would sit at H while
/// recovery replays from C, and those writes (no ValuePointer, so nothing to
/// relocate) would be gone.
///
/// `flush_fails` / `compaction_fails` pick which checkpoint fails; each
/// control (one failure only) must keep everything too.
fn unlisted_flush_case(flush_fails: bool, compaction_fails: bool) {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 951).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, PART, log, row, meta, b"a", b"z").await;
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "unlisted-flush-probe".to_string(),
            1 << 20,
            Rc::new(ConnPool::new()),
        )
        .await
        .expect("stream client");

        let ps1_addr = pick_addr();
        let mut child = ChildPs::spawn(
            PS_ID,
            mgr_addr,
            ps1_addr,
            ChildFailpoints {
                // The first flush publishes C; the second is the unlisted one.
                flush_checkpoint_nth: if flush_fails { 2 } else { 0 },
                compaction_checkpoint: compaction_fails,
                ..Default::default()
            },
        );
        let ps = RpcClient::connect(ps1_addr).await.expect("connect ps1");
        let mut want = Expected::new();

        // SST 1: its checkpoint is durable, cursor C in E0.
        for i in 0..20u32 {
            put(&ps, &mut want, &format!("b{i:02}"), format!("base-{i}").into_bytes()).await;
        }
        assert!(flush_raw(&ps).await, "the first flush must publish");

        // [C, H): inline puts and deletes, still in E0. Seal E0 and write into
        // E1, so the second flush's vp_head H is in E1.
        for i in 0..20u32 {
            put(&ps, &mut want, &format!("k{i:02}"), format!("unlisted-{i}").into_bytes()).await;
        }
        for i in 0..5u32 {
            delete(&ps, &mut want, &format!("b{i:02}")).await;
        }
        let e0 = *stream_extents(&mgr, log).await.last().expect("log tail");
        assert_eq!(roll_tails(&ps, vec![(log, e0)]).await, 1, "roll the log tail");
        put(&ps, &mut want, "x0", b"after-roll".to_vec()).await;

        // SST 2; the failed checkpoint is reported to the caller.
        assert_eq!(flush_raw(&ps).await, !flush_fails, "second flush outcome");

        // The major compaction. Its pre-flush re-flushes an imm whose checkpoint
        // failed (it stayed queued), so SST 2 is durable before the compaction's
        // own checkpoint fails: the unlisted-SST state never forms.
        let row_before_compaction = *stream_extents(&mgr, row).await.last().expect("row tail");
        ps_compact(&ps, PART).await;
        wait_row_roll(&mgr, row, row_before_compaction).await;

        // GC E0, then one more write, then crash.
        force_gc(&ps, vec![e0]).await;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while stream_extents(&mgr, log).await.contains(&e0) {
            assert!(std::time::Instant::now() < deadline, "GC never punched E0");
            compio::time::sleep(Duration::from_millis(200)).await;
        }
        put(&ps, &mut want, "x1", b"after-gc".to_vec()).await;
        assert_state(&ps, &want, "before the crash").await;
        drop(ps);
        child.kill();

        let ps2_addr = pick_addr();
        start_partition_server(PS_ID, mgr_addr, ps2_addr);
        let ps = reopened(ps2_addr, &mgr, &sc, row, meta).await;
        assert_state(&ps, &want, "after the crash").await;
    });
}

#[test]
fn failed_flush_then_failed_compaction_checkpoint_loses_nothing() {
    unlisted_flush_case(true, true);
}

#[test]
fn failed_flush_checkpoint_alone_loses_nothing() {
    unlisted_flush_case(true, false);
}

#[test]
fn failed_compaction_checkpoint_after_a_published_flush_loses_nothing() {
    unlisted_flush_case(false, true);
}
