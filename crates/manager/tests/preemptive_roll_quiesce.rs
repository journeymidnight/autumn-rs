//! The cap-hitting PREEMPTIVE ROLL must quiesce the worker, or it seals the
//! old extent BELOW bytes it has already acked.
//!
//! The roll fires from a successful append whose `end` reached
//! `max_extent_size`. It used to seal at that `end` and move the tail without
//! draining, on the theory that later-leased appends are past the boundary and
//! get re-driven onto the new tail. That holds for ONE caller at a time, and
//! the partition server pipelines up to `ps_inflight_cap` Phase-2 appends on a
//! single stream — so the roller is one of several, and the others are
//! mid-flight.
//!
//! A sibling that COMPLETES before the `ResetTail` lands is acked `Ok` by the
//! contiguous-prefix rule (`ack` has no cap awareness) at an offset ABOVE the
//! seal the roll is taking. WAL replay clamps to `sealed_length`, so those
//! acked bytes are invisible on recovery: silent loss of acked data, which is
//! what stream CLAUDE.md notes 20/22/25a exist to prevent.
//!
//! This drives concurrent appends across a 1 KiB extent cap and asserts the
//! invariant directly: **every range this stream acked is inside its extent's
//! sealed length.** Ablation — restore `alloc_new_extent(Some(result.end),
//! result.extent_id)` in place of the `seal_commit_watermark` handshake — and
//! an acked range lands past the seal.

mod support;

use std::rc::Rc;

use autumn_rpc::client::RpcClient;
use autumn_stream::{ConnPool, StreamClient};
use futures::future::join_all;
use support::*;

#[test]
fn a_preemptive_roll_never_seals_below_an_acked_byte() {
    let (mgr_addr, _mgr_guard) = start_manager();
    let n1_addr = pick_addr();
    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let (_n1_flag, _n1_handle) =
        start_extent_node_stoppable(n1_addr, n1_dir.path().to_path_buf(), 8320);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_node(&mgr, &n1_addr.to_string(), "uuid-roll-quiesce-1").await;
        // RF=1 keeps the fanout deterministic; the race under test is between
        // the roller and its siblings on the writer side, not between replicas.
        let stream_id = create_stream(&mgr, 1).await;
        drop(mgr);

        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "preemptive-roll-quiesce/owner".to_string(),
            1024, // tiny cap → every batch of appends crosses a roll boundary
            pool,
        )
        .await
        .expect("StreamClient::connect");

        // Several rounds, each firing a burst CONCURRENTLY so that some
        // siblings complete while the cap-hitting one is rolling. Each payload
        // is well under the cap so a burst spans the boundary rather than one
        // append clearing it.
        let payload = vec![0xab_u8; 400];
        let mut acked: Vec<(u64, u64)> = Vec::new(); // (extent_id, end)
        for _ in 0..6 {
            let results = join_all((0..4).map(|_| sc.append(stream_id, &payload))).await;
            for r in results {
                // A roll can legitimately fail a sibling ("retry on the current
                // tail") — the public API retries internally, so an Err here is
                // a genuine give-up and worth surfacing, but it is not what this
                // test is about. Only ACKED ranges carry the invariant.
                if let Ok(r) = r {
                    acked.push((r.extent_id, r.end));
                }
            }
        }
        assert!(
            acked.len() >= 8,
            "expected the burst to ack repeatedly across roll boundaries, got {}",
            acked.len()
        );

        // Every acked range must be inside its extent's sealed length. An open
        // tail has no seal yet and is trivially fine.
        let info = sc.get_stream_info(stream_id).await.expect("stream_info");
        assert!(
            info.extent_ids.len() >= 3,
            "expected several rolls at a 1 KiB cap, got {:?}",
            info.extent_ids
        );
        for (extent_id, end) in acked {
            let ex = sc
                .get_extent_info(extent_id)
                .await
                .expect("extent_info for an extent we acked onto");
            if !ex.sealed {
                continue; // still the open tail
            }
            assert!(
                ex.sealed_length >= end,
                "extent {extent_id} was sealed at {} but this stream ACKED a write \
                 ending at {end}; WAL replay clamps to the seal, so those acked \
                 bytes are gone — the roll sealed below its own acked data",
                ex.sealed_length,
            );
        }
    });
}
