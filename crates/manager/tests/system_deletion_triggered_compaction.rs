//! A partition whose SSTs are mostly tombstones compacts itself, and the
//! tombstones it holds are still counted after a restart.
//!
//! A range scan reads every tombstone and every value it shadows until a major
//! compaction drops them. On a real cluster a job deleted ~13 M keys while it
//! kept deleting, so the manager's settle advisory (which waits for the deletes
//! to stop) never fired, and after the job restarted its first range page took
//! 185 s. Each SST now records how many entries and tombstones it holds, and the
//! PS applies TiKV's rule itself: at least 10 000 tombstones and at least 30% of
//! all entries → a major compaction, no manager involved (auto-policy is off in
//! this test, as it is by default).
//!
//! The same counts make `unsettled_deletes` survive a reopen. It used to be a
//! memory-only counter, so deletes flushed before a restart were forgotten and
//! the settle advisory never saw them.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc;

use support::*;

/// Deletes enough to pass the rule (10 000 tombstones, 50% of the entries).
const HEAVY: u64 = 961;
const HEAVY_KEYS: u32 = 12_000;
/// Deletes well under the rule; only its count across the restart is checked.
const LIGHT: u64 = 962;
const LIGHT_KEYS: u32 = 300;

/// Evaluate the rule on every maintenance tick instead of every 5 minutes.
/// Both tests call this before starting anything, so no PS reads the default.
fn fast_checks() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        assert!(autumn_partition_server::background::set_deletion_compact_check_secs(1));
    });
}

async fn reported_load(mgr: &RpcClient, part_id: u64) -> Option<manager_rpc::PartitionLoad> {
    let resp = mgr
        .call(
            manager_rpc::MSG_GET_PARTITION_DETAIL,
            manager_rpc::rkyv_encode(&manager_rpc::GetPartitionDetailReq { part_id }),
        )
        .await
        .ok()?;
    let r: manager_rpc::GetPartitionDetailResp = manager_rpc::rkyv_decode(&resp).ok()?;
    (r.load.part_id == part_id).then_some(r.load)
}

async fn reported_unsettled(mgr: &RpcClient, part_id: u64) -> Option<u64> {
    reported_load(mgr, part_id).await.map(|l| l.unsettled_deletes)
}

async fn wait_unsettled(mgr: &RpcClient, part_id: u64, want: u64, what: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(40);
    let mut last = None;
    while std::time::Instant::now() < deadline {
        last = reported_unsettled(mgr, part_id).await;
        if last == Some(want) {
            return;
        }
        compio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("part {part_id}: {what}: wanted unsettled_deletes {want}, last report {last:?}");
}

#[test]
fn tombstone_heavy_ssts_trigger_a_major_compaction_and_survive_a_restart() {
    fast_checks();

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
        register_two_nodes(&mgr, n1_addr, n2_addr, 81).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, HEAVY, log, row, meta, b"a", b"m").await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, LIGHT, log, row, meta, b"m", b"z").await;

        let ps_addr = pick_addr();
        let (ps_stop, ps_join) = start_partition_server_stoppable(81, mgr_addr, ps_addr);
        let router = PsRouter::new(mgr_addr, ps_addr);
        let light = router.client_for(LIGHT).await;
        let heavy = router.client_for(HEAVY).await;

        // LIGHT: a few hundred deletes, flushed to an SST.
        for i in 0..LIGHT_KEYS {
            ps_put(&light, LIGHT, format!("n-{i:05}").as_bytes(), b"v").await;
        }
        ps_flush(&light, LIGHT).await;
        for i in 0..LIGHT_KEYS {
            ps_delete(&light, LIGHT, format!("n-{i:05}").as_bytes()).await;
        }
        ps_flush(&light, LIGHT).await;
        wait_unsettled(&mgr, LIGHT, LIGHT_KEYS as u64, "after the deletes").await;

        // HEAVY: every key written, flushed, then deleted. While the deletes
        // sit in the memtable no SST counts them, so nothing compacts yet.
        for i in 0..HEAVY_KEYS {
            ps_put(&heavy, HEAVY, format!("b-{i:05}").as_bytes(), b"value").await;
        }
        for i in 0..10u32 {
            ps_put(&heavy, HEAVY, format!("live-{i}").as_bytes(), b"kept").await;
        }
        ps_flush(&heavy, HEAVY).await;
        for i in 0..HEAVY_KEYS {
            ps_delete(&heavy, HEAVY, format!("b-{i:05}").as_bytes()).await;
        }
        wait_unsettled(&mgr, HEAVY, HEAVY_KEYS as u64, "deletes still in the memtable").await;

        // The flush puts 12 000 tombstones next to 12 010 puts. Nobody asks for
        // a compaction; the PS must run one on its own, which settles them.
        ps_flush(&heavy, HEAVY).await;
        wait_unsettled(&mgr, HEAVY, 0, "the deletion-triggered major compaction").await;

        let page = ps_range(&heavy, HEAVY, b"", b"", 100).await;
        let keys: Vec<String> = page
            .entries
            .iter()
            .map(|e| String::from_utf8_lossy(&e.key).into_owned())
            .collect();
        let want: Vec<String> = (0..10).map(|i| format!("live-{i}")).collect();
        assert_eq!(keys, want, "only the live keys are left");

        // Restart. A graceful stop flushes, so the reopened memtable is empty
        // and LIGHT's tombstones exist only in its SSTs.
        drop((light, heavy, router));
        ps_stop.shutdown();
        ps_join.join().expect("join PS");
        let ps2_addr = pick_addr();
        start_partition_server(81, mgr_addr, ps2_addr);
        let router2 = PsRouter::new(mgr_addr, ps2_addr);
        let light2 = router2.client_for(LIGHT).await;
        let r = ps_range(&light2, LIGHT, b"", b"", 1).await;
        assert!(r.entries.is_empty(), "LIGHT's keys stay deleted");
        // The old PS is gone; a report one interval (5 s) past the reopen can
        // only come from the new one.
        compio::time::sleep(Duration::from_secs(7)).await;
        assert_eq!(
            reported_unsettled(&mgr, LIGHT).await,
            Some(LIGHT_KEYS as u64),
            "the deletes flushed before the restart must still be counted"
        );
    });
}

/// Deletes can leave the SSTs without the settle counter knowing: the periodic
/// expiry major (and a minor compaction's shadowing) drops tombstones but does
/// not settle them. The dispatched major that follows finds one table with no
/// tombstone and must still settle, or the count stays up for good and the
/// manager's SETTLE advisory re-issues that compaction every cooldown.
#[test]
fn a_major_after_an_expiry_pass_settles_the_deletes_it_dropped() {
    fast_checks();
    const PART: u64 = 963;
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 82).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, PART, log, row, meta, b"a", b"z").await;
        let ps_addr = pick_addr();
        start_partition_server(82, mgr_addr, ps_addr);
        let ps = PsRouter::new(mgr_addr, ps_addr).client_for(PART).await;

        // One table: an expiring key, and a key with its delete.
        ps_put_ttl(&ps, PART, b"expiring", b"v", 2).await;
        ps_put(&ps, PART, b"doomed", b"v").await;
        ps_delete(&ps, PART, b"doomed").await;
        ps_flush(&ps, PART).await;
        wait_unsettled(&mgr, PART, 1, "the flushed delete").await;

        // The expiry major runs on its own once the TTL passes. It drops the
        // tombstone too, but does not settle it.
        let deadline = std::time::Instant::now() + Duration::from_secs(40);
        loop {
            let load = reported_load(&mgr, PART).await;
            if load.as_ref().is_some_and(|l| l.last_compact_at > 0) {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "the expiry major never ran");
            compio::time::sleep(Duration::from_millis(500)).await;
        }
        assert_eq!(reported_unsettled(&mgr, PART).await, Some(1));

        // What SETTLE dispatches: a major over one tombstone-free table.
        ps_compact(&ps, PART).await;
        wait_unsettled(&mgr, PART, 0, "the dispatched major").await;
    });
}
