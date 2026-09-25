//! A major compaction must reach deletes that are still in the memtable.
//!
//! On a real cluster, 270 deleted 64 MiB values on an idle partition stayed
//! unreclaimable indefinitely: the memtable rotates only on size or on the WAL
//! gap and 270 deletes reach neither, so the tombstones never left it — and a
//! major compaction merges SSTs only. It ran, discarded nothing, and GC never
//! learned that 16 GiB had died. The PS now flushes the memtable before a major
//! compaction.
//!
//! When every entry is dropped the compaction has no output table to carry its
//! discard map; it writes one with no entries rather than lose the map (which
//! also holds the input tables' earlier discards). That table must survive a
//! reopen without reading as keys out of range.
//!
//! The partition also reports how many deletes no major compaction has covered
//! (`PartitionLoad.unsettled_deletes`) — the manager's policy advises the
//! compaction from it — and a successful major compaction clears it.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::{manager_rpc, partition_rpc};

use support::*;

const PART: u64 = 931;
const PART_ONE_TABLE: u64 = 932;
/// Over `VALUE_THROTTLE` (4 KiB), so each value lives in the log stream behind
/// a ValuePointer and a delete makes exactly this many bytes reclaimable.
const VALUE_LEN: usize = 64 * 1024;
const KEYS: u32 = 8;

async fn discarded_bytes(ps: &RpcClient, part_id: u64) -> i64 {
    let resp = ps
        .call(
            partition_rpc::MSG_GET_DISCARDS,
            partition_rpc::rkyv_encode(&partition_rpc::GetDiscardsReq { part_id }),
        )
        .await
        .expect("get discards");
    let r: partition_rpc::GetDiscardsResp =
        partition_rpc::rkyv_decode(&resp).expect("decode GetDiscardsResp");
    assert_eq!(r.code, partition_rpc::CODE_OK, "get discards: {}", r.message);
    r.discards.iter().map(|(_, b)| *b).sum()
}

async fn reported_load(mgr: &RpcClient) -> Option<manager_rpc::PartitionLoad> {
    let resp = mgr
        .call(
            manager_rpc::MSG_GET_PARTITION_DETAIL,
            manager_rpc::rkyv_encode(&manager_rpc::GetPartitionDetailReq { part_id: PART }),
        )
        .await
        .ok()?;
    let r: manager_rpc::GetPartitionDetailResp = manager_rpc::rkyv_decode(&resp).ok()?;
    (r.load.part_id == PART).then_some(r.load)
}

/// Waits for the reported count to become `want`. Each call's `want` differs
/// from what the previous one saw, so matching it proves a fresh report.
async fn wait_reported(mgr: &RpcClient, want: u64) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut last = None;
    while std::time::Instant::now() < deadline {
        last = reported_load(mgr).await.map(|l| l.unsettled_deletes);
        if last == Some(want) {
            return;
        }
        compio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("unsettled_deletes: wanted {want}, last report {last:?}");
}

#[test]
fn a_major_compaction_settles_deletes_still_in_the_memtable() {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 71).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, PART, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        let (ps_stop, ps_join) = start_partition_server_stoppable(71, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        let value = vec![0x5a_u8; VALUE_LEN];
        for i in 0..KEYS {
            ps_put(&ps, PART, format!("big-{i:02}").as_bytes(), &value).await;
        }
        // The values' pointers go into an SST; the deletes below do not.
        ps_flush(&ps, PART).await;
        for i in 0..KEYS {
            ps_delete(&ps, PART, format!("big-{i:02}").as_bytes()).await;
        }
        assert_eq!(discarded_bytes(&ps, PART).await, 0, "nothing compacted yet");
        wait_reported(&mgr, KEYS as u64).await;

        // One SST, tombstones only in the memtable: without the flush this
        // compaction has nothing to merge and records no discard at all.
        ps_compact(&ps, PART).await;
        let want = KEYS as i64 * VALUE_LEN as i64;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let mut got = discarded_bytes(&ps, PART).await;
        while got != want && std::time::Instant::now() < deadline {
            compio::time::sleep(Duration::from_millis(200)).await;
            got = discarded_bytes(&ps, PART).await;
        }
        assert_eq!(
            got, want,
            "the major compaction must discard every deleted value"
        );
        wait_reported(&mgr, 0).await;

        for i in 0..KEYS {
            let resp = ps_head(&ps, PART, format!("big-{i:02}").as_bytes()).await;
            assert!(!resp.found, "big-{i:02} must stay deleted");
        }

        // Every entry was dropped, so the discards ride an SST with no
        // entries. After a reopen they must still be there, and that table
        // must not read as keys outside the range (which refuses splits).
        drop(ps);
        ps_stop.shutdown();
        ps_join.join().expect("join PS");
        let ps2_addr = pick_addr();
        start_partition_server(71, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let mut got = None;
        while std::time::Instant::now() < deadline {
            if let Ok(resp) = ps2
                .call(
                    partition_rpc::MSG_GET_DISCARDS,
                    partition_rpc::rkyv_encode(&partition_rpc::GetDiscardsReq { part_id: PART }),
                )
                .await
            {
                let r: partition_rpc::GetDiscardsResp =
                    partition_rpc::rkyv_decode(&resp).expect("decode GetDiscardsResp");
                if r.code == partition_rpc::CODE_OK {
                    got = Some(r.discards.iter().map(|(_, b)| *b).sum::<i64>());
                    break;
                }
            }
            compio::time::sleep(Duration::from_millis(200)).await;
        }
        assert_eq!(got, Some(want), "the discards must survive a reopen");
        // The old PS is gone, so a report older than one interval (5 s) past
        // the reopen can only come from the new one.
        compio::time::sleep(Duration::from_secs(6)).await;
        let load = reported_load(&mgr).await.expect("load report");
        assert_eq!(load.has_overlap, 0, "an SST without keys has none out of range");
    });
}

/// A value and the delete that kills it can both sit in the one memtable the
/// compaction flushes, leaving a single table. A major compaction skips a
/// single table (nothing to merge) — unless it is settling deletes, because
/// only a major compaction drops that pair.
#[test]
fn a_single_table_holding_a_value_and_its_delete_is_still_compacted() {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 72).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, PART_ONE_TABLE, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(72, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        let value = vec![0x5a_u8; VALUE_LEN];
        for i in 0..KEYS {
            let key = format!("big-{i:02}");
            ps_put(&ps, PART_ONE_TABLE, key.as_bytes(), &value).await;
            ps_delete(&ps, PART_ONE_TABLE, key.as_bytes()).await;
        }
        ps_compact(&ps, PART_ONE_TABLE).await;

        let want = KEYS as i64 * VALUE_LEN as i64;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let mut got = discarded_bytes(&ps, PART_ONE_TABLE).await;
        while got != want && std::time::Instant::now() < deadline {
            compio::time::sleep(Duration::from_millis(200)).await;
            got = discarded_bytes(&ps, PART_ONE_TABLE).await;
        }
        assert_eq!(got, want, "the single flushed table must still be compacted");
    });
}
