//! BUG2 — end-to-end data-safety of the GC replay-floor
//! raise to the DURABLY-ACKed checkpoint vp (`gc_floor_raise_to_durable_ckpt`).
//!
//! Reproduce-first + discriminating: we force-GC the FIRST log extent E0, which
//! holds the OLDEST live SST's vp_head — i.e. E0 sits AT the over-conservative
//! MIN-over-SST-vps floor. The `gc_extent_punchable` strictly-before rule means
//! E0 is punchable ONLY when the floor is RAISED past it (BUG2). So:
//!   - pre-BUG2 (floor == MIN): E0 is protected → the reclaim assertion FAILS (red).
//!   - post-BUG2 (floor == durable_ckpt_vp, in a later extent): E0 is reclaimed (green).
//!
//! Data-safety (the user's concern — "一定确保不会丢失数据"): 4 "cold" keys live
//! ENTIRELY inside E0. GC must relocate-then-punch them (never drop a live value
//! whose naming checkpoint is durable). After a crash + same-id takeover the new
//! PS's `recover_partition` must find EVERY key — the cold ones (now relocated
//! out of the punched E0, resolved via replay of the relocate records) and the
//! latest hot ones — plus a complete range scan (no phantom / missing).
//!
//! HEAVY by necessity: exercising a real log-extent roll needs the PS extent-size
//! setter, which clamps to [1 GiB, 64 GiB]; there is no cheap sub-GiB roll for a
//! PS-owned StreamClient (only raw clients, e.g. f109, can). So this writes >1 GiB
//! of 8 MiB VP values and GC then scans the ~1 GiB E0 to relocate the cold keys.
//! Expect ~1-2 min. `#[ignore]` so it is opt-in, run via
//! `cargo test -p autumn-manager --test system_gc_floor_durable_ckpt -- --ignored`.

mod support;

use std::collections::BTreeSet;
use std::rc::Rc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

/// 8 MiB value → stored as a ValuePointer in the log_stream (VP path); big
/// enough that ~128 of them roll a 1 GiB extent.
fn big(tag: u8) -> Vec<u8> {
    vec![tag; 8 * 1024 * 1024]
}

async fn ps_forcegc(
    ps: &RpcClient,
    part_id: u64,
    extent_ids: Vec<u64>,
) -> partition_rpc::MaintenanceResp {
    let resp = ps
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id,
                op: partition_rpc::MAINTENANCE_FORCE_GC,
                extent_ids,
                gc_ratio: None,
                gc_max_size: None,
                gc_stream_debt: None,
                gc_empty_only: false,
                op_id: 0,
            }),
        )
        .await
        .expect("forcegc");
    partition_rpc::rkyv_decode(&resp).expect("decode MaintenanceResp")
}

async fn log_extent_ids(sc: &StreamClient, log: u64) -> Vec<u64> {
    sc.get_stream_info(log).await.expect("log info").extent_ids
}

#[test]
#[ignore = "heavy: writes >1 GiB to roll a log extent + GC scans ~1 GiB (~1-2 min)"]
fn gc_floor_raise_reclaims_min_extent_and_loses_nothing() {
    // Roll a fresh log extent at 1 GiB (the smallest the PS setter allows). Set
    // once, BEFORE any partition opens (OnceLock; this is the only test in the
    // binary so it wins uncontended).
    assert!(
        autumn_partition_server::set_max_extent_size_bytes(1024 * 1024 * 1024),
        "set_max_extent_size_bytes must win the OnceLock before any PS start"
    );

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

        // replication=1 halves the write I/O (this test is about the GC floor +
        // recovery, not replica durability; the seal floor is 1).
        let log = create_stream(&mgr, 1).await;
        let row = create_stream(&mgr, 1).await;
        let meta = create_stream(&mgr, 1).await;
        upsert_partition(&mgr, 940, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(71, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // StreamClient to inspect log_stream membership + reclaim of E0.
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(&mgr_addr.to_string(), "bug2-probe".to_string(), 1 << 20, pool)
            .await
            .expect("connect sc");

        // 1) COLD keys — written once, LIVE forever. Their flushed SST's vp_head
        //    anchors the MIN-over-SST-vps floor at E0. These are the live values
        //    inside E0 that GC must relocate-then-punch (the data-safety crux).
        let cold_vals: Vec<Vec<u8>> = (0..4u8).map(|i| big(0xC0 + i)).collect();
        for i in 0..4u8 {
            ps_put(&ps, 940, format!("c{i}").as_bytes(), &cold_vals[i as usize]).await;
        }
        ps_flush(&ps, 940).await;

        let e0 = log_extent_ids(&sc, log).await[0];

        // 2) HOT churn — overwrite 8 hot keys with 8 MiB values until the log
        //    ROLLS (E0 sealed + a fresh tail). Every hot version that lands in E0
        //    is later SUPERSEDED, so it is DEAD → GC punches it free; only the 4
        //    cold keys need relocating. Flush periodically to publish SSTs.
        let mut wrote = 0u32;
        loop {
            for _ in 0..24 {
                let h = wrote % 8;
                ps_put(&ps, 940, format!("h{h}").as_bytes(), &big(0x10 + (wrote % 180) as u8)).await;
                wrote += 1;
            }
            ps_flush(&ps, 940).await;
            if log_extent_ids(&sc, log).await.len() >= 2 {
                break;
            }
            assert!(
                wrote < 400,
                "log never rolled after {wrote} × 8 MiB writes — extent-size setter?"
            );
        }

        // 3) Final overwrite of ALL 8 hot keys → their LIVE versions land in the
        //    post-roll open tail (above E0). Two flushes so the durable checkpoint
        //    vp is firmly in a LATER extent than E0 ⇒ the raise lifts the floor
        //    strictly past E0.
        let hot_final: Vec<Vec<u8>> = (0..8u8).map(|i| big(0xE0 + i)).collect();
        for i in 0..8u8 {
            ps_put(&ps, 940, format!("h{i}").as_bytes(), &hot_final[i as usize]).await;
        }
        ps_flush(&ps, 940).await;
        ps_put(&ps, 940, b"h0", &hot_final[0]).await; // nudge the active into the tail
        ps_flush(&ps, 940).await;

        // E0 must still be the FIRST log extent and there must be a later one
        // (log_stream never truncates-from-head; it only appends fresh extents).
        let ext = log_extent_ids(&sc, log).await;
        assert!(ext.len() >= 2, "need >=2 log extents; got {ext:?}");
        assert_eq!(ext[0], e0, "E0 must still be extent_ids[0]");

        // Pre-GC sanity: everything reads correctly (nothing lost before GC).
        for i in 0..4u8 {
            let got = ps_get(&ps, 940, format!("c{i}").as_bytes()).await;
            assert_eq!(got.value, cold_vals[i as usize], "pre-GC cold c{i} wrong");
        }

        // 4) FORCE-GC E0. Under the BUG2 raise the floor is at the durable
        //    checkpoint vp (a later extent), so E0 (pos 0 < floor) is PUNCHABLE.
        //    Without the raise the floor == MIN sits AT E0 → E0 is protected →
        //    the reclaim assertion below FAILS (this is the reproduce-first red).
        let _ = ps_forcegc(&ps, 940, vec![e0]).await;

        // 5) PROVE the raise fired: E0 must be reclaimed. Single partition ⇒
        //    refs 1→0 ⇒ the extent is physically deleted. GC first relocates the
        //    4 cold values + scans the ~1 GiB extent, so allow a generous poll.
        let mut reclaimed = false;
        for _ in 0..60 {
            sc.invalidate_extent_cache(e0);
            match sc.get_extent_info(e0).await {
                Err(_) => {
                    reclaimed = true;
                    break;
                }
                Ok(info) if info.refs == 0 => {
                    reclaimed = true;
                    break;
                }
                _ => compio::time::sleep(Duration::from_millis(500)).await,
            }
        }
        assert!(
            reclaimed,
            "E0 was NOT reclaimed by force-GC — the BUG2 durable-ckpt floor-raise \
             did not fire (the baseline MIN floor protects E0 at its own position), \
             so this run did not exercise the fix"
        );
        // E0 is gone from the log membership too.
        assert!(
            !log_extent_ids(&sc, log).await.contains(&e0),
            "E0 still in log_stream membership after force-GC"
        );

        // 6) CRASH + same-id takeover → the new PS runs `recover_partition`,
        //    which must resolve the punched E0's SST vp_head as unresolvable,
        //    land chosen_pos at the first surviving extent, and replay the
        //    relocate records that carry the cold values forward.
        drop(ps);
        let ps2_addr = pick_addr();
        start_partition_server(71, mgr_addr, ps2_addr);
        // Recovery replays the surviving ~1 GiB extent E1 (chosen_pos lands
        // there once E0's SST vp_head is unresolvable), and the PS binds its
        // listener only AFTER `sync_regions_once` finishes that replay — so the
        // connect can be refused for tens of seconds. Retry until it's up.
        let mut ps2_opt = None;
        for _ in 0..180 {
            match RpcClient::connect(ps2_addr).await {
                Ok(c) => {
                    ps2_opt = Some(c);
                    break;
                }
                Err(_) => compio::time::sleep(Duration::from_millis(500)).await,
            }
        }
        let ps2 = ps2_opt.expect("ps2 never bound its listener (recovery too slow / stuck)");
        compio::time::sleep(Duration::from_millis(1000)).await;

        // 7) NO DATA LOSS across GC-punch(E0) + restart.
        for i in 0..4u8 {
            let got = ps_get(&ps2, 940, format!("c{i}").as_bytes()).await;
            assert_eq!(
                got.value,
                cold_vals[i as usize],
                "COLD key c{i} lost/wrong after GC-punch(E0) + restart — the \
                 relocate-then-punch under the raised floor dropped a live value"
            );
        }
        for i in 0..8u8 {
            let got = ps_get(&ps2, 940, format!("h{i}").as_bytes()).await;
            assert_eq!(got.value, hot_final[i as usize], "hot key h{i} wrong after restart");
        }

        // 8) Range scan is complete — exactly the 12 distinct live user keys.
        let r = ps_range(&ps2, 940, b"", b"", 1000).await;
        let keys: BTreeSet<Vec<u8>> = r.entries.iter().map(|e| e.key.clone()).collect();
        for i in 0..4u8 {
            assert!(keys.contains(format!("c{i}").as_bytes().as_ref() as &[u8]), "range missing c{i}");
        }
        for i in 0..8u8 {
            assert!(keys.contains(format!("h{i}").as_bytes().as_ref() as &[u8]), "range missing h{i}");
        }
        assert_eq!(keys.len(), 12, "range returned unexpected key set: {keys:?}");
    });
}
