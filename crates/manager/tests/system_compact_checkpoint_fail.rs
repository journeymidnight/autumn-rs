//! A compaction whose checkpoint append fails leaves the in-memory table list
//! on its outputs while the durable checkpoint still names its inputs. The
//! partition keeps serving; GC, foreground writes and later truncates then act
//! on the in-memory list. A crash anywhere in that window must recover every
//! acknowledged value and delete from the durable checkpoint plus the log.
//!
//! What keeps that true:
//! - The failed compaction does not truncate the row stream, and every later
//!   truncate follows a checkpoint that succeeded with the current list. The
//!   inputs' row extent therefore stays while the durable checkpoint names it.
//! - GC's replay floor cannot pass the durable cursor: an output's vp_head is
//!   the newest of its inputs', which the durable checkpoint already covers
//!   when it lists every input, and the raise to `durable_ckpt_vp` is gated on
//!   a checkpoint ack. (A flush whose own checkpoint failed leaves an SST no
//!   durable checkpoint lists; compacting it under a second failed checkpoint
//!   is outside this test.)
//! - GC relocates what is live in memory, which a compaction never changes, at
//!   fresh seqs past the durable cursor, so replay brings those values back
//!   over the durable SSTs' stale pointers into the punched extent.
//!
//! The scenario: two flushed SSTs hold big values (ValuePointers into log
//! extent E0), deletes of some of them, and small inline values; E0 is sealed
//! and a third SST puts the durable cursor past it. A major compaction swaps
//! in one output and its checkpoint fails (test failpoint). Inside the window
//! the test overwrites and deletes keys, force-GCs E0 (relocating the live
//! values and punching it), writes again, and SIGKILLs the PS while the
//! durable checkpoint still names the three inputs. The reopened partition
//! must serve the exact acknowledged state. A later major compaction then
//! publishes and truncates past both the inputs and the orphaned output, and a
//! second reopen must serve the same state.
//!
//! The PS runs as a child process (this test binary re-executed into
//! `child_ps`) so the failpoint can be armed in it and it can be SIGKILLed: a
//! graceful stop would flush and publish a checkpoint on the way out, closing
//! the window before the "crash".
//!
//! Ablation: letting the failed compaction go on to truncate the row stream
//! makes the reopen load SSTs from a dropped extent, and this test fails. GC
//! here punches only E0, well below the floor, so the test does not
//! discriminate the floor itself (the `system_gc_*` tests do).
//!
//! `child_ps` passes as a no-op in an ordinary run. A child outlives a parent
//! killed from outside the test (an outer timeout); kill it by hand.

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
/// Environment of a re-executed `child_ps`: `<ps_id> <manager> <ps addr>`.
const CHILD_ENV: &str = "AUTUMN_TEST_COMPACT_CKPT_FAIL_CHILD";

fn big(tag: u8) -> Vec<u8> {
    vec![tag; BIG_LEN]
}

/// The PS of the test, when this binary is re-executed with `CHILD_ENV` set:
/// it arms the failpoint, serves until SIGKILLed, and never returns. Without
/// `CHILD_ENV` it does nothing.
#[test]
fn child_ps() {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return;
    };
    let mut it = spec.split(' ');
    let ps_id: u64 = it.next().unwrap().parse().unwrap();
    let mgr = it.next().unwrap().to_string();
    let ps_addr: SocketAddr = it.next().unwrap().parse().unwrap();
    // The cluster secret every server here proves (installed by the helper).
    cluster_secret_file();
    autumn_partition_server::background::fail_next_compaction_checkpoint();
    compio::runtime::Runtime::new().unwrap().block_on(async move {
        let ps = autumn_partition_server::PartitionServer::connect_with_advertise_and_port(
            ps_id,
            &mgr,
            Some(ps_addr.to_string()),
            ps_addr,
        )
        .await
        .expect("connect partition server");
        ps.sync_regions_once().await.expect("sync regions");
        let _ = ps.serve(ps_addr).await;
    });
}

struct ChildPs(std::process::Child);

impl ChildPs {
    fn spawn(ps_id: u64, mgr_addr: SocketAddr, ps_addr: SocketAddr) -> Self {
        let child = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args(["--exact", "child_ps", "--nocapture", "--test-threads=1"])
            .env(CHILD_ENV, format!("{ps_id} {mgr_addr} {ps_addr}"))
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("spawn child PS");
        let mut ps = ChildPs(child);
        for _ in 0..600 {
            if std::net::TcpStream::connect_timeout(&ps_addr, Duration::from_millis(200)).is_ok() {
                return ps;
            }
            if let Some(status) = ps.0.try_wait().expect("poll child PS") {
                panic!("child PS exited before accepting: {status} (its stderr is above)");
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("child PS never accepted on {ps_addr}");
    }

    /// SIGKILL and reap: what is durable is what was durable before it.
    fn kill(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for ChildPs {
    fn drop(&mut self) {
        self.kill();
    }
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
        let mut child = ChildPs::spawn(PS_ID, mgr_addr, ps1_addr);
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

        // The major compaction swaps one output in; its checkpoint fails.
        ps_compact(&ps, PART).await;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while live_sst_count(&ps).await != 1 {
            assert!(std::time::Instant::now() < deadline, "the compaction never swapped");
            compio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            checkpoint_sst_extents(&sc, meta).await,
            inputs_row,
            "the failpoint must leave the durable checkpoint on the inputs"
        );

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

        // The window is still open at the crash.
        assert_eq!(live_sst_count(&ps).await, 1);
        assert_eq!(checkpoint_sst_extents(&sc, meta).await, inputs_row);
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
