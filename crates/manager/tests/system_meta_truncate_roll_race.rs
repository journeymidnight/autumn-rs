//! A meta-tail roll landing between a checkpoint's append and the meta
//! truncate after it must not take the checkpoint with it.
//!
//! `save_table_locs_raw` appends the checkpoint, then reads the meta stream
//! and cuts the extents before the one the checkpoint landed in. A
//! `MSG_ROLL_TAILS` of the meta stream (the fence-drain roll, which takes no
//! publish lock) in between seals that extent and opens an empty one. A cut
//! before the stream's LAST extent dropped the checkpoint with it.
//!
//! The partition kept serving from memory, and its GC floor is the durable
//! checkpoint it believes in, so GC punched the log extent whose records were
//! only in the flushed SSTs. The reopen found no checkpoint, opened with no
//! tables and replayed only the log that was left: the flushed keys were gone.
//!
//! The held checkpoint lands in the second of two meta extents, so the cut
//! must still drop the older checkpoint before it.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;

use support::*;

const PART: u64 = 1421;
const PS_ID: u64 = 87;
const OLD_KEYS: u32 = 40;
const NEW_KEYS: u32 = 4;
/// Above the old keys' total; four new values (inline, under
/// `VALUE_THROTTLE`) fill the memtable past it.
const FLUSH_BYTES: u64 = 8 * 1024;

fn new_value(i: u32) -> Vec<u8> {
    vec![b'a' + i as u8; 3000]
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
    let r: partition_rpc::MaintenanceResp = partition_rpc::rkyv_decode(&resp).expect("decode");
    assert_eq!(r.code, partition_rpc::CODE_OK, "forcegc: {}", r.message);
}

#[test]
fn a_meta_roll_between_checkpoint_and_truncate_keeps_the_checkpoint() {
    let mgr_addr = pick_addr();
    let en_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        autumn_partition_server::set_ckpt_truncate_pause(false);
        autumn_partition_server::params().flush_mem_bytes.set(FLUSH_BYTES);
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .expect("mgr");
        let _ = register_node(&mgr, &en_addr.to_string(), "uuid-meta-roll-race").await;
        let (log, row, meta) = (
            create_stream(&mgr, 1).await,
            create_stream(&mgr, 1).await,
            create_stream(&mgr, 1).await,
        );
        upsert_partition(&mgr, PART, log, row, meta, b"", b"\xff").await;

        let ps_addr = pick_addr();
        start_partition_server(PS_ID, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("ps");

        // Old keys: inline values in the first log extent, flushed.
        for i in 0..OLD_KEYS {
            ps_put(&ps, PART, format!("old-{i:03}").as_bytes(), format!("v{i}").as_bytes()).await;
        }
        ps_flush(&ps, PART).await;
        let log0 = *extents(&mgr, log).await.last().unwrap();
        roll(&ps, log, log0).await;

        // A second meta extent: the held checkpoint lands there.
        let meta_old = *extents(&mgr, meta).await.last().unwrap();
        roll(&ps, meta, meta_old).await;

        // Second flush, held right after its checkpoint append. It is the
        // background flush a full memtable starts (the flush RPC runs inline
        // on the partition loop and would hold the roll request back).
        let meta_tail = *extents(&mgr, meta).await.last().unwrap();
        let parked0 = autumn_partition_server::ckpt_truncate_parked_count();
        autumn_partition_server::set_ckpt_truncate_pause(true);
        for i in 0..NEW_KEYS {
            ps_put(&ps, PART, format!("new-{i:03}").as_bytes(), &new_value(i)).await;
        }
        let parked = poll_until(Duration::from_secs(20), Duration::from_millis(5), || {
            autumn_partition_server::ckpt_truncate_parked_count() > parked0
        })
        .await;
        assert!(parked, "the flush never reached the post-append sync point");
        assert_eq!(
            *extents(&mgr, meta).await.last().unwrap(),
            meta_tail,
            "the checkpoint went to the meta tail read before the flush"
        );

        // The fence-drain roll of the meta tail lands in the window.
        roll(&ps, meta, meta_tail).await;
        let commits0 = autumn_partition_server::flush_commit_count();
        autumn_partition_server::set_ckpt_truncate_pause(false);
        let committed = poll_until(Duration::from_secs(20), Duration::from_millis(5), || {
            autumn_partition_server::flush_commit_count() > commits0
        })
        .await;
        assert!(committed, "the held flush never committed");

        let meta_now = extents(&mgr, meta).await;
        eprintln!(
            "meta stream after the flush: {meta_now:?} (checkpoint was in {meta_tail}, \
             the older one in {meta_old})"
        );
        assert_eq!(
            meta_now.len(),
            2,
            "the cut keeps the checkpoint's extent and the rolled tail: {meta_now:?}"
        );
        assert_eq!(meta_now[0], meta_tail, "the checkpoint's extent must survive");

        // GC the first log extent: its records are in the flushed SSTs.
        assert!(extents(&mgr, log).await.contains(&log0));
        force_gc(&ps, vec![log0]).await;
        let gone = poll_until_async(Duration::from_secs(20), Duration::from_millis(50), || {
            let mgr = &mgr;
            async move { !extents(mgr, log).await.contains(&log0) }
        })
        .await;
        assert!(gone, "GC must reclaim the first log extent (non-vacuity)");

        // Crash: a same-id PS takes the partition over and reopens it.
        let ps2_addr = pick_addr();
        start_partition_server(PS_ID, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("ps2");
        // new-000 must exist: once it reads back, the partition is open on
        // the new PS.
        let opened = poll_until_async(Duration::from_secs(30), Duration::from_millis(100), || {
            let ps2 = &ps2;
            async move {
                matches!(try_ps_get(ps2, PART, b"new-000").await,
                    Ok(r) if r.code == partition_rpc::CODE_OK && r.value == new_value(0))
            }
        })
        .await;
        assert!(opened, "the partition never reopened on the new PS with new-000");
        let mut lost = Vec::new();
        for i in 0..OLD_KEYS {
            let k = format!("old-{i:03}");
            let r = ps_get(&ps2, PART, k.as_bytes()).await;
            if r.code != partition_rpc::CODE_OK || r.value != format!("v{i}").into_bytes() {
                lost.push(k);
            }
        }
        for i in 0..NEW_KEYS {
            let r = ps_get(&ps2, PART, format!("new-{i:03}").as_bytes()).await;
            assert!(r.code == partition_rpc::CODE_OK && r.value == new_value(i), "new-{i:03} lost");
        }
        assert!(
            lost.is_empty(),
            "{} of {OLD_KEYS} flushed keys lost after the reopen (meta stream {meta_now:?}): {lost:?}",
            lost.len()
        );
    });
}
