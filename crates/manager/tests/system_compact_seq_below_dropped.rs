//! A major compaction must not let an old, dropped delete come back and hide a
//! newer acknowledged write when recovery walks already-flushed log.
//!
//! Recovery skips log records at or below the loaded SSTs' max seq, on the
//! premise that every flushed record is at or below it. A compaction output's
//! seq is the newest entry it KEPT, so a major compaction that drops a key's
//! puts and its newest tombstone leaves an SST whose seq is below those records,
//! and the next open seeds the seq counter from it: a new put of the same key
//! gets a seq below the old delete. While the checkpoint cursor resolves,
//! replay starts after both and nothing shows. When neither the cursor nor any
//! SST's stamped vp_head resolves, replay walks the whole log, the old delete
//! passes the lowered threshold and shadows the new put.
//!
//! The unresolvable cursor is built as in `system_empty_vp_cursor`: a restart
//! seeds the cursor to an empty rolled tail E, flushes stamp it, and E is
//! reclaimed once it is a sealed-empty non-tail. Here the delete is replayed
//! and flushed through that seeded cursor, so the compaction output's stamp is
//! E too. With `keep = false` the compaction keeps nothing at all, the case
//! that used to leave no table and so seeded the counter from zero.
//! `compact = false` is the control: the same steps without the major
//! compaction.

mod support;

use std::rc::Rc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{self, StreamInfoReq, StreamInfoResp, MSG_STREAM_INFO};
use autumn_rpc::partition_rpc;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

async fn stream_members(mgr: &RpcClient, stream_id: u64) -> Vec<u64> {
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
    let (_, info) = resp
        .streams
        .into_iter()
        .find(|(id, _)| *id == stream_id)
        .expect("stream in response");
    info.extent_ids
}

async fn stream_tail(mgr: &RpcClient, stream_id: u64) -> u64 {
    *stream_members(mgr, stream_id)
        .await
        .last()
        .expect("stream has extents")
}

/// Row extents of the SSTs the partition's last checkpoint lists.
async fn checkpoint_sst_extents(mgr_addr: std::net::SocketAddr, meta_stream: u64) -> Vec<u64> {
    let sc = StreamClient::connect(
        &mgr_addr.to_string(),
        "compact-seq-below-dropped-reader".to_string(),
        128 * 1024 * 1024,
        Rc::new(ConnPool::new()),
    )
    .await
    .expect("stream client");
    let info = sc.get_stream_info(meta_stream).await.expect("meta stream info");
    let eid = *info.extent_ids.last().expect("meta extent");
    let (payload, _) = sc.read_bytes_from_extent(eid, 0, 0).await.expect("read meta");
    decode_last_table_locations(&payload)
        .locs
        .iter()
        .map(|l| l.extent_id)
        .collect()
}

async fn roll_tail(ps: &RpcClient, part_id: u64, stream_id: u64, expected_tail: u64) -> u32 {
    let resp = ps
        .call(
            partition_rpc::MSG_ROLL_TAILS,
            partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq {
                part_id,
                entries: vec![(stream_id, expected_tail)],
            }),
        )
        .await
        .expect("roll_tails rpc");
    let resp: partition_rpc::RollTailsResp =
        partition_rpc::rkyv_decode(&resp).expect("decode RollTailsResp");
    assert_eq!(resp.code, partition_rpc::CODE_OK, "roll_tails: {}", resp.message);
    resp.rolled
}

async fn punch(mgr_addr: std::net::SocketAddr, mgr: &RpcClient, stream_id: u64, extent_id: u64) {
    let owner = StreamClient::connect(
        &mgr_addr.to_string(),
        "owner/compact-seq-below-dropped/0".to_string(),
        256 * 1024 * 1024,
        Rc::new(ConnPool::new()),
    )
    .await
    .expect("stream client for the punch");
    let resp = mgr
        .call(
            manager_rpc::MSG_STREAM_PUNCH_HOLES,
            manager_rpc::rkyv_encode(&manager_rpc::PunchHolesReq {
                stream_id,
                owner_key: owner.owner_key().to_string(),
                owner_epoch: owner.owner_epoch(),
                extent_ids: vec![extent_id],
            }),
        )
        .await
        .expect("punch_holes rpc");
    let resp: manager_rpc::PunchHolesResp =
        manager_rpc::rkyv_decode(&resp).expect("decode PunchHolesResp");
    assert_eq!(resp.code, manager_rpc::CODE_OK, "punch: {}", resp.message);
}

fn run(compact: bool, keep: bool, ps_id: u64, part_id: u64, disk_seed: u16) {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, disk_seed).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, part_id, log, row, meta, b"a", b"z").await;

        // (1) Optionally one key that survives every compaction, then `k` put
        // and deleted ten times: the last delete carries the highest seq
        // written so far. Acked, not flushed.
        let ps1_addr = pick_addr();
        start_partition_server(ps_id, mgr_addr, ps1_addr);
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");
        if keep {
            ps_put(&ps1, part_id, b"keep", b"keep-v").await;
        }
        for i in 0u32..10 {
            ps_put(&ps1, part_id, b"k", format!("old-{i}").as_bytes()).await;
            let d = ps_delete(&ps1, part_id, b"k").await;
            assert_eq!(d.code, partition_rpc::CODE_OK, "delete k: {}", d.message);
        }

        // (2) Roll the log tail to an empty E, then crash (drop the client; the
        // server self-evicts without flushing). The next open replays (1) and
        // seeds the write cursor to (E, 0).
        let l1 = stream_tail(&mgr, log).await;
        assert_eq!(roll_tail(&ps1, part_id, log, l1).await, 1, "log tail must roll");
        let empty_e = stream_tail(&mgr, log).await;
        assert_ne!(empty_e, l1, "the roll must produce a new tail");
        drop(ps1);

        let ps2_addr = pick_addr();
        start_partition_server(ps_id, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        compio::time::sleep(Duration::from_millis(2000)).await;

        // (3) Flush: the SST holding the delete is stamped (E, 0). The major
        // compaction drops every version of `k`, so the newest entry it keeps
        // is `keep` (or there is none), and its stamp is still (E, 0).
        ps_flush(&ps2, part_id).await;
        compio::time::sleep(Duration::from_millis(500)).await;
        if compact {
            // The compaction runs after the RPC returns. A major rolls the row
            // tail and writes its output there, so it has published once the
            // checkpoint lists only SSTs in a new row tail. Without this wait a
            // slow compaction would leave the flush's checkpoint in place and
            // the test would pass as the control does.
            let row_before = stream_tail(&mgr, row).await;
            ps_compact(&ps2, part_id).await;
            let mut published = false;
            for _ in 0..120 {
                let tail = stream_tail(&mgr, row).await;
                let ssts = checkpoint_sst_extents(mgr_addr, meta).await;
                if tail != row_before && !ssts.is_empty() && ssts.iter().all(|e| *e == tail) {
                    published = true;
                    break;
                }
                compio::time::sleep(Duration::from_millis(250)).await;
            }
            assert!(published, "precondition: the major compaction must publish its output");
        }
        assert!(
            ps_get(&ps2, part_id, b"k").await.code != partition_rpc::CODE_OK,
            "k was deleted"
        );

        // (4) Roll again so E is a sealed-empty non-tail, then crash.
        let tail_now = stream_tail(&mgr, log).await;
        if tail_now == empty_e {
            assert_eq!(roll_tail(&ps2, part_id, log, empty_e).await, 1, "second roll");
        }
        drop(ps2);

        // (5) The cursor (E, 0) still resolves: replay is empty and the seq
        // counter starts from the SSTs' max. Put `k` again, acked.
        let ps3_addr = pick_addr();
        start_partition_server(ps_id, mgr_addr, ps3_addr);
        let ps3 = RpcClient::connect(ps3_addr).await.expect("connect ps3");
        compio::time::sleep(Duration::from_millis(2000)).await;
        ps_put(&ps3, part_id, b"k", b"new").await;
        assert_eq!(ps_get(&ps3, part_id, b"k").await.value, b"new");
        drop(ps3);
        compio::time::sleep(Duration::from_millis(500)).await;

        // (6) Reclaim E while nothing owns the stream; no checkpoint cursor or
        // SST stamp resolves any more.
        punch(mgr_addr, &mgr, log, empty_e).await;
        let members = stream_members(&mgr, log).await;
        assert!(
            !members.contains(&empty_e) && members.contains(&l1),
            "precondition: E {empty_e} reclaimed, the extent holding the deletes {l1} kept \
             (members: {members:?})"
        );

        // (7) Recover by walking the whole log.
        let ps4_addr = pick_addr();
        start_partition_server(ps_id, mgr_addr, ps4_addr);
        let ps4 = RpcClient::connect(ps4_addr).await.expect("connect ps4");
        compio::time::sleep(Duration::from_millis(3000)).await;

        if keep {
            assert_eq!(ps_get(&ps4, part_id, b"keep").await.value, b"keep-v");
        }
        let got = ps_get(&ps4, part_id, b"k").await;
        assert!(
            got.code == partition_rpc::CODE_OK && got.value == b"new",
            "k = \"new\" was acked after the deletes; a whole-log replay must not let \
             an old delete shadow it (code {}, value {:?})",
            got.code,
            String::from_utf8_lossy(&got.value)
        );
    });
}

#[test]
fn whole_log_replay_after_a_major_compaction_keeps_a_newer_put() {
    run(true, true, 93, 931, 73);
}

#[test]
fn whole_log_replay_after_a_major_compaction_that_kept_nothing_keeps_a_newer_put() {
    run(true, false, 95, 951, 75);
}

#[test]
fn whole_log_replay_without_a_compaction_keeps_a_newer_put() {
    run(false, true, 94, 941, 74);
}
