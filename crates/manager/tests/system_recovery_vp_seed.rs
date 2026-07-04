//! Recovery seeds the write cursor `p.vp` to the committed log TAIL, not the
//! replay START — so an idle-restarted partition's recovered active memtable
//! rotates with a FORWARD vp_head (its true content boundary), which lets the GC
//! floor advance. This test guards the data-safety of that forward seed: after a
//! restart with un-flushed data, flushing the recovered active (which stamps the
//! seeded vp onto a new SST) followed by a SECOND restart must still recover
//! everything. A seed computed PAST the recovered data would strand it here.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;

use support::*;

#[test]
fn recovery_seeds_tail_and_flushing_recovered_active_loses_nothing() {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 61).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 907, log, row, meta, b"a", b"z").await;

        let ps1_addr = pick_addr();
        start_partition_server(81, mgr_addr, ps1_addr);
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        // Batches A + B: acked but NOT flushed — only in the active memtable +
        // log WAL. Crucially NO pre-crash flush, so crash-1 recovery has no
        // checkpoint/SST anchor: the recovered-active SST (flushed below) becomes
        // the SOLE replay anchor. A tail seed computed too-far-FORWARD would then
        // strand later data (a pre-crash SST would otherwise pin the replay-min
        // low and mask the overshoot — fable review P2).
        for i in 0u32..10 {
            ps_put(&ps1, 907, format!("a-{i:02}").as_bytes(), format!("va-{i}").as_bytes()).await;
        }
        for i in 0u32..10 {
            ps_put(&ps1, 907, format!("b-{i:02}").as_bytes(), format!("vb-{i}").as_bytes()).await;
        }

        // Crash 1 → same-id takeover (NON-graceful: drop the client; the old
        // server self-evicts on LockedByOther without flushing — mirrors
        // system_crash_mid_flush). Graceful shutdown would flush B, emptying the
        // recovered active memtable and skipping the exact path under test. ps2
        // recovers A+B by replaying the log from offset 0 (no checkpoint); the
        // recovered active now holds A+B, and recovery seeds p.vp to the committed
        // log tail.
        drop(ps1);
        let ps2_addr = pick_addr();
        start_partition_server(81, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Flush the RECOVERED active memtable: this rotates it and stamps the
        // SEEDED p.vp (the tail) as the new SST's vp_head — the exact path the
        // fix changes. Then write batch C (un-flushed again).
        ps_flush(&ps2, 907).await;
        for i in 0u32..10 {
            ps_put(&ps2, 907, format!("c-{i:02}").as_bytes(), format!("vc-{i}").as_bytes()).await;
        }

        // Crash 2 → recover again. If the seeded vp_head on B's SST had been
        // pushed past B (or C stranded past the replay window), this second
        // recovery would drop data.
        drop(ps2);
        let ps3_addr = pick_addr();
        start_partition_server(81, mgr_addr, ps3_addr);
        let ps3 = RpcClient::connect(ps3_addr).await.expect("connect ps3");
        compio::time::sleep(Duration::from_millis(2000)).await;

        for i in 0u32..10 {
            for (prefix, vp) in [("a", "va"), ("b", "vb"), ("c", "vc")] {
                let key = format!("{prefix}-{i:02}");
                let resp = ps_get(&ps3, 907, key.as_bytes()).await;
                assert_eq!(
                    resp.value,
                    format!("{vp}-{i}").as_bytes(),
                    "{key} must survive restart → flush-recovered-active → restart"
                );
            }
        }
    });
}
