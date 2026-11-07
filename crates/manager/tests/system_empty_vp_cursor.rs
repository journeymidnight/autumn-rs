//! A checkpoint whose replay cursor names a ZERO-BYTE log extent must not cost
//! acknowledged writes when that extent is reclaimed.
//!
//! The safety argument behind reclaiming `sealed = true, sealed_length = 0`
//! extents is that they hold no acked byte, so nothing can reference them. That
//! is true of everything pointing at BYTES — no ValuePointer, no SST content, no
//! checkpoint content lives in an extent that was sealed at zero. It is NOT true
//! of the replay CURSOR, which the partition server deliberately points at such
//! an extent: `recover_partition` seeds the write cursor to the committed log
//! TAIL, and a freshly-rolled tail has zero committed bytes, so the seed is
//! `(E, 0)`. The next rotation stamps that into an SST and the meta checkpoint.
//!
//! Once E leaves the tail slot it is a sealed-empty non-tail extent, which
//! `gc_extent_punchable` deems punchable UNCONDITIONALLY — `sealed_length == 0 ||
//! pos < replay_floor_pos`, so the replay floor does not protect it. Recovery
//! then cannot resolve the cursor, skips it, and if no live SST's cursor resolves
//! while the table set is non-empty it takes the `chosen_pos == usize::MAX`
//! branch and replays NOTHING — which `background.rs` already describes, in its
//! own words, as losing the acked-but-un-flushed WAL tail.
//!
//! Every step below is a supported operation: the roll is the fence-drain
//! `MSG_ROLL_TAILS`, and the reclaim is `MSG_STREAM_PUNCH_HOLES` — byte for byte
//! the request `run_gc`'s empty-extent path issues, so what this pins is the
//! CONSEQUENCE of reclaiming the cursor's extent. That GC is the thing which
//! selects it is read from `gc_extent_punchable` and its unit tests, not
//! reproduced here; see the note at the punch.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{self, StreamInfoReq, StreamInfoResp, MSG_STREAM_INFO};
use autumn_rpc::partition_rpc;
use autumn_stream::{ConnPool, StreamClient};
use std::rc::Rc;

use support::*;

async fn stream_tail(mgr: &RpcClient, stream_id: u64) -> u64 {
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
    *info.extent_ids.last().expect("stream has extents")
}

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
    let (_, info) = resp
        .streams
        .into_iter()
        .find(|(id, _)| *id == stream_id)
        .expect("stream in response");
    info.extent_ids
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

#[test]
fn acked_writes_survive_reclaiming_the_empty_extent_a_checkpoint_points_at() {
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
        let part_id = 921u64;
        upsert_partition(&mgr, part_id, log, row, meta, b"a", b"z").await;

        let ps1_addr = pick_addr();
        start_partition_server(91, mgr_addr, ps1_addr);
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        // (1) Batch A, acked, NOT flushed — it lives only in the active memtable
        // and the log WAL.
        for i in 0u32..10 {
            ps_put(&ps1, part_id, format!("a-{i:02}").as_bytes(), format!("va-{i}").as_bytes()).await;
        }

        // (2) Roll the log tail. The extent holding A is sealed at its real
        // length; the new tail E has zero committed bytes.
        let tail_with_a = stream_tail(&mgr, log).await;
        assert_eq!(roll_tail(&ps1, part_id, log, tail_with_a).await, 1, "log tail must roll");
        let empty_e = stream_tail(&mgr, log).await;
        assert_ne!(empty_e, tail_with_a, "the roll must produce a NEW tail");

        // (3) Crash — by dropping the client, so the old server self-evicts on
        // LockedByOther WITHOUT flushing. This must not become a graceful
        // shutdown (`start_partition_server_stoppable`): that drains and flushes,
        // which would empty the un-flushed batches the whole test is about and
        // leave it passing with nothing to lose. Same reasoning as
        // `system_recovery_vp_seed`, and the reason both reuse the ps_id.
        //
        // Recovery replays A and seeds the write cursor to the committed tail —
        // which is E at offset 0, because E holds nothing.
        drop(ps1);
        let ps2_addr = pick_addr();
        start_partition_server(91, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        compio::time::sleep(Duration::from_millis(2000)).await;

        // (4) Flush the recovered active: it rotates and stamps the seeded
        // cursor onto the SST and the meta checkpoint.
        ps_flush(&ps2, part_id).await;
        compio::time::sleep(Duration::from_millis(500)).await;

        // (5) Roll again, so E leaves the tail slot as a SEALED-EMPTY non-tail —
        // the shape GC considers punchable regardless of the replay floor.
        let tail_now = stream_tail(&mgr, log).await;
        if tail_now == empty_e {
            assert_eq!(roll_tail(&ps2, part_id, log, empty_e).await, 1, "second roll");
        }

        // (6) Batch C: acked, NOT flushed. This is what the no-replay branch
        // silently drops.
        for i in 0u32..10 {
            ps_put(&ps2, part_id, format!("c-{i:02}").as_bytes(), format!("vc-{i}").as_bytes()).await;
        }

        // (7) Crash, then reclaim E while nothing owns the stream.
        //
        // The production reclaimer is the partition's own GC —
        // `gc_extent_punchable` is `sealed_length == 0 || pos < floor`, so it
        // considers E punchable regardless of the replay floor. Driving it from
        // this harness did not fire (the MAINTENANCE_AUTO_GC dispatch is accepted
        // and nothing is reclaimed, not even the non-empty extent below the
        // floor), so the reclaim here is an explicit punch instead. What that
        // costs in fidelity: this proves the CONSEQUENCE of reclaiming the
        // cursor's extent, not that GC is the thing that reclaims it. The latter
        // is read from `gc_extent_punchable` and its own unit tests, not
        // reproduced.
        drop(ps2);
        compio::time::sleep(Duration::from_millis(500)).await;
        {
            let pool = Rc::new(ConnPool::new());
            let owner = StreamClient::connect(
                &mgr_addr.to_string(),
                "owner/empty-vp-cursor/0".to_string(),
                256 * 1024 * 1024,
                pool,
            )
            .await
            .expect("stream client for the punch");
            let resp = mgr
                .call(
                    manager_rpc::MSG_STREAM_PUNCH_HOLES,
                    manager_rpc::rkyv_encode(&manager_rpc::PunchHolesReq {
                        stream_id: log,
                        owner_key: owner.owner_key().to_string(),
                        owner_epoch: owner.owner_epoch(),
                        extent_ids: vec![empty_e],
                    }),
                )
                .await
                .expect("punch_holes rpc");
            let resp: manager_rpc::PunchHolesResp =
                manager_rpc::rkyv_decode(&resp).expect("decode PunchHolesResp");
            assert_eq!(
                resp.code,
                manager_rpc::CODE_OK,
                "punching the sealed-empty extent: {}",
                resp.message
            );
        }
        let members = stream_members(&mgr, log).await;
        assert!(
            !members.contains(&empty_e),
            "precondition: GC must have reclaimed the sealed-empty extent {empty_e} \
             the checkpoint points at (members: {members:?})"
        );

        // (8) Recover with the cursor's extent gone.
        let ps3_addr = pick_addr();
        start_partition_server(91, mgr_addr, ps3_addr);
        let ps3 = RpcClient::connect(ps3_addr).await.expect("connect ps3");
        compio::time::sleep(Duration::from_millis(3000)).await;

        for i in 0u32..10 {
            let key = format!("a-{i:02}");
            let resp = ps_get(&ps3, part_id, key.as_bytes()).await;
            assert_eq!(
                resp.value,
                format!("va-{i}").as_bytes(),
                "{key} was flushed into an SST and must survive"
            );
        }
        for i in 0u32..10 {
            let key = format!("c-{i:02}");
            let resp = ps_get(&ps3, part_id, key.as_bytes()).await;
            assert_eq!(
                resp.value,
                format!("vc-{i}").as_bytes(),
                "{key} was ACKED and un-flushed; reclaiming the cursor's empty extent \
                 must not silently drop it"
            );
        }
    });
}
