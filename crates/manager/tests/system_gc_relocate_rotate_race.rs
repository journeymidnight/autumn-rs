//! A memtable rotation that lands between a GC relocation append and the
//! memtable insert of the values it moved must not leave those values out of
//! both the flushed table and the replayed log.
//!
//! GC used to append the live values of the extent it collects to the log
//! itself and insert their new ValuePointers into the memtable after the
//! append returned. Foreground writes leased after that append could be
//! acked, inserted and rotated first (the append's return can wait on a
//! preemptive extent roll); the frozen memtable's cursor was then past the
//! relocation records while it did not hold them. Its flush published that
//! cursor, GC punched the source extent, and a reopen replayed only from the
//! cursor: the moved values were in no SST and not replayed (8/8 lost).
//!
//! Relocations now go through the write pipeline, so they reach the memtable
//! in log order with every other write. The sync point
//! `set_gc_relocated_pause` holds GC after its relocations are written, while
//! a foreground write, a rotation and a flush run; the pre-fix code held the
//! same place between its append and its insert.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;

use support::*;

const PART: u64 = 1431;
const PS_ID: u64 = 89;
const BIG_KEYS: u8 = 8;
/// Above `VALUE_THROTTLE`: stored behind a ValuePointer in the log.
const BIG_LEN: usize = 64 * 1024;

fn big(i: u8) -> Vec<u8> {
    vec![i.wrapping_add(1); BIG_LEN]
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
fn a_rotation_between_gc_append_and_insert_keeps_the_moved_values() {
    let mgr_addr = pick_addr();
    let en_addr = pick_addr();
    start_manager(mgr_addr);
    let dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(en_addr, dir.path().to_path_buf(), 1);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        autumn_partition_server::set_gc_relocated_pause(false);
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .expect("mgr");
        let _ = register_node(&mgr, &en_addr.to_string(), "uuid-gc-rotate-race").await;
        let (log, row, meta) = (
            create_stream(&mgr, 1).await,
            create_stream(&mgr, 1).await,
            create_stream(&mgr, 1).await,
        );
        upsert_partition(&mgr, PART, log, row, meta, b"", b"\xff").await;

        let ps_addr = pick_addr();
        start_partition_server(PS_ID, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("ps");

        // Large values in the first log extent, flushed: the SST holds
        // ValuePointers into it.
        for i in 0..BIG_KEYS {
            ps_put(&ps, PART, format!("big-{i:02}").as_bytes(), &big(i)).await;
        }
        ps_flush(&ps, PART).await;
        let log0 = *extents(&mgr, log).await.last().unwrap();
        roll(&ps, log, log0).await;
        // A write and a flush past it, so the checkpoint cursor leaves log0
        // and GC may punch it.
        ps_put(&ps, PART, b"pad", b"pad").await;
        ps_flush(&ps, PART).await;

        // GC log0, held after its relocations are written.
        let parked0 = autumn_partition_server::gc_relocated_parked_count();
        autumn_partition_server::set_gc_relocated_pause(true);
        force_gc(&ps, vec![log0]).await;
        let parked = poll_until(Duration::from_secs(20), Duration::from_millis(5), || {
            autumn_partition_server::gc_relocated_parked_count() > parked0
        })
        .await;
        assert!(parked, "GC never reached the post-append sync point");

        // A foreground write after the relocation records, then a rotation
        // and flush: the published cursor is past them.
        ps_put(&ps, PART, b"after-gc", b"after").await;
        ps_flush(&ps, PART).await;

        autumn_partition_server::set_gc_relocated_pause(false);
        let punched = poll_until_async(Duration::from_secs(20), Duration::from_millis(50), || {
            let mgr = &mgr;
            async move { !extents(mgr, log).await.contains(&log0) }
        })
        .await;
        assert!(punched, "GC must punch log0 (non-vacuity)");
        for i in 0..BIG_KEYS {
            let r = ps_get(&ps, PART, format!("big-{i:02}").as_bytes()).await;
            assert!(
                r.code == partition_rpc::CODE_OK && r.value == big(i),
                "big-{i:02} unreadable before the crash: {}",
                r.message
            );
        }

        // Crash: a same-id PS takes the partition over and reopens it.
        let ps2_addr = pick_addr();
        start_partition_server(PS_ID, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("ps2");
        let opened = poll_until_async(Duration::from_secs(30), Duration::from_millis(100), || {
            let ps2 = &ps2;
            async move {
                matches!(try_ps_get(ps2, PART, b"after-gc").await,
                    Ok(r) if r.code == partition_rpc::CODE_OK)
            }
        })
        .await;
        assert!(opened, "the partition never reopened on the new PS");
        let mut lost = Vec::new();
        for i in 0..BIG_KEYS {
            let k = format!("big-{i:02}");
            match try_ps_get(&ps2, PART, k.as_bytes()).await {
                Ok(r) if r.code == partition_rpc::CODE_OK && r.value == big(i) => {}
                Ok(r) => lost.push(format!("{k}: code {} {}", r.code, r.message)),
                Err(e) => lost.push(format!("{k}: {e}")),
            }
        }
        assert!(
            lost.is_empty(),
            "{} of {BIG_KEYS} GC-moved values lost after the reopen: {lost:?}",
            lost.len()
        );
    });
}
