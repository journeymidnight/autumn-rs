//! Data integrity: a major compaction must NOT strand un-flushed writes past
//! the vp_head it stamps on its output SSTs.
//!
//! Bug (pre-fix): `do_compact` stamped the output SST's vp_head (recovery
//! replay-start) from the LIVE write cursor `p.vp_extent_id/vp_offset`, which
//! points AFTER the most recent write. Major compaction replaces every SST
//! (each of which carried an older, smaller vp_head anchoring the un-flushed
//! log tail) with outputs all stamped at the live cursor. Recovery reads the
//! latest checkpoint (`decode_last_table_locations`) + the live SST vp_heads;
//! after major compaction ALL of them equal the live cursor, so replay starts
//! PAST any write that was still in the active memtable at compaction time —
//! those acked-but-un-flushed writes are never replayed = silent data loss.
//!
//! Fix: compaction stamps vp_head = MAX over the INPUT SSTs' vp_heads (by
//! stream position) = the newest input's content boundary. That stays BEHIND
//! the un-flushed tail (so it is never stranded) while still advancing the GC
//! replay floor past the fully-merged log region.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;

use support::*;

#[test]
fn compaction_must_not_strand_unflushed_writes_past_vp_head() {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 57).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 903, log, row, meta, b"a", b"z").await;

        let ps1_addr = pick_addr();
        start_partition_server(77, mgr_addr, ps1_addr);
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        // Batch A: keys 00..20 -> flush -> SSTable 1 (vp_head anchors A's tail)
        for i in 0u32..20 {
            ps_put(
                &ps1,
                903,
                format!("key-{i:02}").as_bytes(),
                format!("val-{i}").as_bytes(),
            )
            .await;
        }
        ps_flush(&ps1, 903).await;

        // Batch B: keys 20..40 -> flush -> SSTable 2
        for i in 20u32..40 {
            ps_put(
                &ps1,
                903,
                format!("key-{i:02}").as_bytes(),
                format!("val-{i}").as_bytes(),
            )
            .await;
        }
        ps_flush(&ps1, 903).await;

        // Batch C: keys 40..60 -> NO flush. These are acked + durable in the
        // log_stream (WAL) but live only in the active memtable, NOT in any
        // SSTable. They sit at log offsets BELOW the live write cursor.
        for i in 40u32..60 {
            ps_put(
                &ps1,
                903,
                format!("key-{i:02}").as_bytes(),
                format!("val-{i}").as_bytes(),
            )
            .await;
        }

        // Major compaction merges SST1 + SST2. Pre-fix it stamps the output
        // vp_head at the live cursor (past batch C). Post-fix it stamps
        // max(SST1.vp, SST2.vp) = B's tail, which stays behind C.
        ps_compact(&ps1, 903).await;
        compio::time::sleep(Duration::from_millis(300)).await;

        // Crash (NON-graceful): drop the client and let a same-id PS take over
        // via owner-epoch fencing — the old server thread self-evicts on
        // LockedByOther WITHOUT flushing, so the batch-C memtable is abandoned
        // and recovery must rebuild it from the log replay window. This mirrors
        // `system_crash_mid_compact`. We deliberately do NOT use
        // `start_partition_server_stoppable`: its `serve_until_shutdown` path is
        // a GRACEFUL drain that flushes every imm (F120-C), which would persist
        // batch C into an SST and mask the very loss this test reproduces.
        drop(ps1);

        let ps2_addr = pick_addr();
        start_partition_server(77, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        compio::time::sleep(Duration::from_millis(2000)).await;

        // All 60 keys must survive. Batches A/B are in the merged SST; batch C
        // must be recovered by replaying the log from the (correctly stamped)
        // vp_head. Pre-fix, C is stranded past the vp_head and returns NotFound.
        for i in 0u32..60 {
            let key = format!("key-{i:02}");
            let resp = ps_get(&ps2, 903, key.as_bytes()).await;
            assert_eq!(
                resp.value,
                format!("val-{i}").as_bytes(),
                "{key} must survive compaction+crash (un-flushed writes must stay \
                 inside the replay window)"
            );
        }
    });
}
