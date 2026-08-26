//! Rolling a LIVE partition's stream tails (the fence-drain `MSG_ROLL_TAILS`
//! path) must not lose acknowledged writes.
//!
//! The bug this pins (the chaos split-child acked-write loss): `seal_and_roll_tail` used a bare manager
//! PROBE seal — no worker quiesce, no `ResetTail` — so after the manager
//! froze the tail's `sealed_length`, the partition's live stream worker (and
//! the extent-nodes, which learn of manager seals only lazily) kept appending
//! to the SAME extent and kept ACKing client writes. Every byte acked after
//! the seal sat above `sealed_length`:
//!   * a checkpoint/SST landing in (or referencing) that ghost region wedged
//!     the CoW split child permanently with
//!     `stale_vp_offset_past_sealed_length` (the LOUD shape);
//!   * otherwise recovery's committed-clamped replay stopped cleanly at the
//!     seal and the acked tail vanished SILENTLY — the child served stale
//!     values with no fail-loud marker anywhere (the SILENT shape).
//!
//! Two scenarios, matching the two observed chaos shapes:
//!   1. roll log+row tails, then write + flush + split → the child must OPEN
//!      (pre-fix: checkpoint references an SST past the row seal → wedge);
//!   2. roll all three tails (meta too), then write + flush + split → the
//!      child must serve the POST-ROLL acked values (pre-fix: the post-roll
//!      checkpoint is invisible past the meta seal and the WAL replay clamps
//!      at the log seal → silent stale reads).

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{self, StreamInfoReq, StreamInfoResp, MSG_STREAM_INFO};
use autumn_rpc::partition_rpc;

use support::*;

/// Current tail extent id of `stream_id`, from the manager's view.
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

/// Send MSG_ROLL_TAILS for the given (stream, expected_tail) entries and
/// return `rolled`.
async fn roll_tails(ps: &RpcClient, part_id: u64, entries: Vec<(u64, u64)>) -> u32 {
    let resp = ps
        .call(
            partition_rpc::MSG_ROLL_TAILS,
            partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq { part_id, entries }),
        )
        .await
        .expect("roll_tails rpc");
    let resp: partition_rpc::RollTailsResp =
        partition_rpc::rkyv_decode(&resp).expect("decode RollTailsResp");
    assert_eq!(
        resp.code,
        partition_rpc::CODE_OK,
        "roll_tails: {}",
        resp.message
    );
    resp.rolled
}

/// Post-roll ("ghost window" pre-fix) value for key `t{i:03}`. Every 4th is
/// 8 KiB — above the 4 KiB VALUE_THROTTLE, so it is stored as a ValuePointer
/// whose bytes must be FETCHED from the log extent on read (the small ones
/// read straight from the memtable/SST). Non-vacuity for the VP arm is
/// structural: 8192 > VALUE_THROTTLE guarantees the VP path.
fn ghost_value(i: u32) -> Vec<u8> {
    let mut v = format!("ghost{i:03}").into_bytes();
    if i % 4 == 0 {
        while v.len() < 8192 {
            v.push(b'G');
        }
    }
    v
}

/// Shared scenario driver. `roll_meta_too` selects between the loud and
/// silent pre-fix shapes; the assertions cover both: the split child must
/// open AND serve the post-roll acked values.
fn run_scenario(part_id: u64, ps_id: u64, roll_meta_too: bool) {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 40).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, part_id, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(ps_id, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // Phase 1: acked writes + one flush so all three stream workers are
        // LIVE (log via the WAL appends, row via the SST upload, meta via the
        // checkpoint) and every tail holds data.
        for i in 0u32..40 {
            ps_put(&ps, part_id, format!("d{i:03}").as_bytes(), b"base").await;
        }
        ps_flush(&ps, part_id).await;

        // Phase 2: roll the LIVE tails — exactly what the manager's
        // fence-drain sweep sends when the tails' replica set includes a
        // fenced node.
        let (log_tail, row_tail, meta_tail) = (
            stream_tail(&mgr, log).await,
            stream_tail(&mgr, row).await,
            stream_tail(&mgr, meta).await,
        );
        let mut entries = vec![(log, log_tail), (row, row_tail)];
        if roll_meta_too {
            entries.push((meta, meta_tail));
        }
        let want_rolled = entries.len() as u32;
        let rolled = roll_tails(&ps, part_id, entries).await;
        // Non-vacuity: the roll must actually have sealed + rolled the tails —
        // a skipped/failed roll would make every assertion below pass without
        // exercising the live-writer window.
        assert_eq!(rolled, want_rolled, "roll_tails must roll the live tails");
        let new_log_tail = stream_tail(&mgr, log).await;
        assert_ne!(
            new_log_tail, log_tail,
            "log stream must have a fresh tail after the roll"
        );

        // Phase 3: MORE acked writes after the roll — pre-fix these landed on
        // the just-sealed extents (the ghost window). Ghost keys sort ABOVE
        // the split point so they belong to the RIGHT child. Every 4th value
        // is LARGE (8 KiB > the 4 KiB VALUE_THROTTLE) so it takes the
        // ValuePointer path: the value bytes live in the log extent and a
        // read must fetch them — the memtable holds only the pointer. This
        // pins the third manifestation of the ghost window (the chaos
        // "big-values-only persistent read failure" shape): a VP pointing
        // past a stale seal is refused with stale_vp_offset on every read,
        // while an equally-ghosted small value still reads from the memtable.
        for i in 0u32..20 {
            ps_put(
                &ps,
                part_id,
                format!("t{i:03}").as_bytes(),
                &ghost_value(i),
            )
            .await;
        }
        // Flush so an SST + checkpoint reference the post-roll state (the loud
        // arm needs a checkpoint pointing into the ghost region; with the fix
        // everything lands on the fresh tails).
        ps_flush(&ps, part_id).await;

        // Phase 4: split strictly between the base keys (d…) and the ghost
        // keys (t…) — the ghost keys are the RIGHT child's range.
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq {
                    part_id,
                    at_key: Some(b"m".to_vec()),
                }),
            )
            .await
            .expect("split rpc");
        let sr: partition_rpc::SplitPartResp =
            partition_rpc::rkyv_decode(&resp).expect("decode SplitPartResp");
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split: {}", sr.message);

        // Phase 5: find the right child and verify it opens + serves every
        // post-roll acked value. Bounded poll: pre-fix the child NEVER opens
        // (loud arm) or serves stale/not-found (silent arm).
        let right_part = {
            let regions = get_regions(&mgr).await;
            regions
                .regions
                .iter()
                .find(|(_, r)| {
                    r.rg.as_ref().map(|g| g.start_key.as_slice()) == Some(b"m".as_slice())
                })
                .map(|(pid, _)| *pid)
                .expect("right child in regions")
        };
        let router = PsRouter::new(mgr_addr, ps_addr);
        let opened = poll_until_async(Duration::from_secs(20), Duration::from_millis(250), || {
            let router = &router;
            async move {
                match router.try_client_for(right_part).await {
                    Ok(c) => {
                        // Any decisive answer (OK / NOT_FOUND) proves the child
                        // OPENED; the loud pre-fix shape never opens at all.
                        // Whether the VALUES survived is asserted per-key below
                        // (the silent pre-fix shape opens but answers
                        // NOT_FOUND / stale).
                        let r = ps_get(&c, right_part, b"t000").await;
                        r.code == partition_rpc::CODE_OK
                            || r.code == partition_rpc::CODE_NOT_FOUND
                    }
                    Err(_) => false,
                }
            }
        })
        .await;
        assert!(
            opened,
            "right child (part {right_part}) never became readable — \
             pre-fix wedge shape (stale_vp_offset_past_sealed_length)"
        );
        for i in 0u32..20 {
            let key = format!("t{i:03}");
            let want = ghost_value(i);
            let got = psr_get(&router, right_part, key.as_bytes()).await;
            assert_eq!(
                got.code,
                partition_rpc::CODE_OK,
                "right child lost acked post-roll key {key} (len {})",
                want.len()
            );
            assert_eq!(
                got.value, want,
                "right child serves a STALE value for acked post-roll key {key}"
            );
        }
        // Left child sanity: base keys still served.
        let left_get = psr_get(&router, part_id, b"d000").await;
        assert_eq!(left_get.code, partition_rpc::CODE_OK, "left child lost d000");
    });
}

/// Loud shape: log+row tails rolled under the live writer; the post-roll flush's
/// SST/checkpoint must not strand the right child un-openable.
#[test]
fn roll_log_row_tails_then_split_child_opens_and_serves() {
    run_scenario(901, 61, false);
}

/// A roll ALREADY IN FLIGHT when a split's freeze begins (it passed the
/// freeze-defer check before the freeze was set) can land its seal+alloc
/// INSIDE the split's captured-commit window: the split captured the commit
/// length of tail T, the roll then seals T and appends a fresh empty tail T',
/// and `multi_modify_split` seals whatever extent is the CURRENT tail — T' —
/// at T's captured length. T' is then "sealed" longer than any replica holds,
/// and the CoW child's WAL replay can never read it → the child never opens.
///
/// Interleaving is forced with two test sync-points (`set_roll_tails_pause`
/// holds the roll post-checks/pre-seal; `set_split_commit_pause` holds the
/// split post-capture/pre-commit). The fix is the manager REFUSING a
/// `multi_modify_split` whose captured tail ids no longer match the current
/// tails (the PS aborts, the client retries with a fresh capture) — so the
/// assertion is: the split either commits with consistent tails or is
/// refused-and-retried, and the right child ALWAYS opens and serves.
#[test]
fn in_flight_roll_racing_split_commit_child_still_opens() {
    let part_id: u64 = 903;
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        // Clean hook state even if a prior aborted test left them armed.
        autumn_partition_server::set_split_commit_pause(false);
        autumn_partition_server::set_roll_tails_pause(false);

        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 40).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, part_id, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(63, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // Both halves of the eventual split get data; all acked pre-freeze.
        for i in 0u32..40 {
            ps_put(&ps, part_id, format!("d{i:03}").as_bytes(), b"base").await;
        }
        ps_flush(&ps, part_id).await;
        for i in 0u32..20 {
            ps_put(
                &ps,
                part_id,
                format!("t{i:03}").as_bytes(),
                format!("ghost{i:03}").as_bytes(),
            )
            .await;
        }

        let log_tail = stream_tail(&mgr, log).await;

        // 1. Park an in-flight roll of the log tail (checks done, seal held).
        let roll_parked0 = autumn_partition_server::roll_tails_parked_count();
        autumn_partition_server::set_roll_tails_pause(true);
        let roll_ps = RpcClient::connect(ps_addr).await.expect("connect ps (roll)");
        let roll_task = compio::runtime::spawn(async move {
            let resp = roll_ps
                .call(
                    partition_rpc::MSG_ROLL_TAILS,
                    partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq {
                        part_id,
                        entries: vec![(log, log_tail)],
                    }),
                )
                .await
                .expect("roll rpc");
            partition_rpc::rkyv_decode::<partition_rpc::RollTailsResp>(&resp)
                .expect("decode RollTailsResp")
        });
        let roll_parked = poll_until(Duration::from_secs(10), Duration::from_millis(5), || {
            autumn_partition_server::roll_tails_parked_count() > roll_parked0
        })
        .await;
        assert!(roll_parked, "roll never reached the pre-seal sync point");

        // 2. Park a split at the post-capture / pre-commit sync point.
        let split_parked0 = autumn_partition_server::split_commit_parked_count();
        autumn_partition_server::set_split_commit_pause(true);
        let split_ps = RpcClient::connect(ps_addr).await.expect("connect ps (split)");
        let split_task = compio::runtime::spawn(async move {
            split_ps
                .call(
                    partition_rpc::MSG_SPLIT_PART,
                    partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq {
                        part_id,
                        at_key: Some(b"m".to_vec()),
                    }),
                )
                .await
        });
        let split_parked = poll_until(Duration::from_secs(20), Duration::from_millis(5), || {
            autumn_partition_server::split_commit_parked_count() > split_parked0
        })
        .await;
        assert!(split_parked, "split never reached the pre-commit sync point");

        // 3. Release the roll INSIDE the split's captured-commit window; wait
        //    for its seal+alloc to land.
        autumn_partition_server::set_roll_tails_pause(false);
        let roll_resp = roll_task.await.expect("roll task panicked");
        assert_eq!(roll_resp.code, partition_rpc::CODE_OK, "{}", roll_resp.message);
        assert_eq!(
            roll_resp.rolled, 1,
            "non-vacuity: the roll must seal+roll the log tail during the split window"
        );
        let rolled_tail = stream_tail(&mgr, log).await;
        assert_ne!(
            rolled_tail, log_tail,
            "non-vacuity: the log tail must have moved inside the window"
        );

        // 4. Release the split. With the tail-id guard the manager refuses the
        //    stale-capture commit and the PS aborts; retry with fresh captures.
        autumn_partition_server::set_split_commit_pause(false);
        let first = split_task.await.expect("split task panicked");
        let first_committed = match first {
            Ok(bytes) => {
                let sr: partition_rpc::SplitPartResp =
                    partition_rpc::rkyv_decode(&bytes).expect("decode SplitPartResp");
                sr.code == partition_rpc::CODE_OK
            }
            Err(_) => false,
        };
        if !first_committed {
            let resp = ps
                .call(
                    partition_rpc::MSG_SPLIT_PART,
                    partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq {
                        part_id,
                        at_key: Some(b"m".to_vec()),
                    }),
                )
                .await
                .expect("split retry rpc");
            let sr: partition_rpc::SplitPartResp =
                partition_rpc::rkyv_decode(&resp).expect("decode SplitPartResp");
            assert_eq!(
                sr.code,
                partition_rpc::CODE_OK,
                "split retry after tail-moved refusal: {}",
                sr.message
            );
        }

        // 5. The right child must open and serve everything acked pre-split.
        let right_part = {
            let regions = get_regions(&mgr).await;
            regions
                .regions
                .iter()
                .find(|(_, r)| {
                    r.rg.as_ref().map(|g| g.start_key.as_slice()) == Some(b"m".as_slice())
                })
                .map(|(pid, _)| *pid)
                .expect("right child in regions")
        };
        let router = PsRouter::new(mgr_addr, ps_addr);
        let opened = poll_until_async(Duration::from_secs(20), Duration::from_millis(250), || {
            let router = &router;
            async move {
                match router.try_client_for(right_part).await {
                    Ok(c) => ps_get(&c, right_part, b"t000").await.code == partition_rpc::CODE_OK,
                    Err(_) => false,
                }
            }
        })
        .await;
        assert!(
            opened,
            "right child (part {right_part}) never became readable — the split committed \
             with a tail sealed at a stale captured length"
        );
        for i in 0u32..20 {
            let key = format!("t{i:03}");
            let want = format!("ghost{i:03}");
            let got = psr_get(&router, right_part, key.as_bytes()).await;
            assert_eq!(got.code, partition_rpc::CODE_OK, "right child lost {key}");
            assert_eq!(got.value, want.as_bytes(), "right child stale value for {key}");
        }
        let left_get = psr_get(&router, part_id, b"d000").await;
        assert_eq!(left_get.code, partition_rpc::CODE_OK, "left child lost d000");
    });
}

/// Silent shape: all three tails rolled (checkpoint lands past the meta seal
/// pre-fix); the right child must serve the post-roll acked values, not a
/// silent stale snapshot.
#[test]
fn roll_all_tails_then_split_child_serves_acked_writes() {
    run_scenario(902, 62, true);
}
