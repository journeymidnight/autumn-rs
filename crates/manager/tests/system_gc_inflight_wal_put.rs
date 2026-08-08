//! Reproduce-first: GC value-relocation vs a Put still in the WAL-write queue.
//!
//! `run_gc → process_gc_chunk` decides a scanned large-value (VP) record is
//! "live" by looking up the key's current version in the **memtable + SST
//! snapshot** (the post-await active/imm re-check + full VP-identity match,
//! `background.rs`). But the PS write pipeline is 3-phase:
//!   Phase 1 `start_write_batch`  — seq_number += 1, encode WAL record
//!   Phase 2 `append_batch`       — WAL append (durable)
//!   Phase 3 `finish_write_batch` — memtable insert + client ACK
//! A Put sitting between Phase 1 and Phase 3 — seq ASSIGNED, bytes DURABLE, but
//! NOT yet in the memtable — is invisible to BOTH guards. Because Phase 1
//! already bumped `seq_number`, GC's own relocation seq (`seq_number += 1`) is
//! HIGHER than the in-flight Put's; GC then relocates the OLD value at that
//! higher seq, shadowing the newer in-flight Put → silent lost-update.
//!
//! Determinism uses two sync points on the single-threaded partition runtime:
//!   `set_gc_verdict_pause`   — holds GC right after its SST lookup, before the
//!                              verdict (so we can slip a Put in first);
//!   `set_write_phase3_pause` — holds the Put after Phase 2, before Phase 3
//!                              (seq assigned + durable, not yet in the memtable).
//! GC is triggered FIRST (while partition_loop is free); only then is Phase 3
//! held — otherwise the held Phase 3 would stall partition_loop and the
//! force-GC RPC (dispatched on it) would deadlock.
//!
//! Seal of the extent holding the old value is via `split` (log_stream only
//! rolls at 16 GiB or on split).
//!
//! Pre-fix: FAILS — `kmm` reads back the OLD value. Post-fix: the NEW value.

mod support;

use std::rc::Rc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

fn val(tag: u8) -> Vec<u8> {
    // >4 KiB ⇒ stored as a ValuePointer in the log_stream (the only thing GC
    // value-relocation touches).
    vec![tag; 5000]
}

async fn force_gc(ps: &RpcClient, part_id: u64, extent_ids: Vec<u64>) {
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
            }),
        )
        .await
        .expect("force gc");
    let r: partition_rpc::MaintenanceResp =
        partition_rpc::rkyv_decode(&resp).expect("decode MaintenanceResp");
    assert_eq!(r.code, partition_rpc::CODE_OK, "force gc failed: {}", r.message);
}

/// Poll `counter()` until it exceeds `base`, up to ~10 s. Panics on timeout.
async fn wait_counter(counter: impl Fn() -> u64, base: u64, what: &str) {
    for _ in 0..1000 {
        if counter() > base {
            return;
        }
        compio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

#[test]
fn gc_relocation_must_not_shadow_inflight_wal_put() {
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
        register_two_nodes(&mgr, n1_addr, n2_addr, 88).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(81, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        let v_old = val(b'A');
        let v_new = val(b'B');

        // Seed: kmm = v_old (a large VP value) into log extent X — the SOLE VP
        // record, so GC's verdict pause fires exactly once, on kmm. Flush ⇒
        // kmm's live VP (→ v_old in X) lands in an SST, active memtable empties.
        ps_put(&ps, 901, b"kmm", &v_old).await;
        ps_flush(&ps, 901).await;
        assert_eq!(ps_get(&ps, 901, b"kmm").await.value, v_old, "seed: kmm must read v_old");

        // Capture X (the log extent holding v_old) before sealing it.
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(&mgr_addr.to_string(), "test-gc-inflight".to_string(), 1 << 20, pool)
            .await
            .expect("connect sc");
        let x = sc.get_stream_info(log).await.expect("log info").extent_ids[0];

        // Split at "n": part 901 stays the LEFT child [a, n) and keeps kmm; the
        // split SEALS the shared log tail X (CoW) so it is a GC candidate and so
        // the next kmm Put rolls a FRESH tail (v_new never lands in X). Explicit
        // at_key skips the >=2-distinct-keys gate.
        let resp = ps
            .call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq {
                    part_id: 901,
                    at_key: Some(b"n".to_vec()),
                }),
            )
            .await
            .expect("split");
        let sr: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&resp).expect("decode split");
        assert_eq!(sr.code, partition_rpc::CODE_OK, "split failed: {}", sr.message);
        compio::time::sleep(Duration::from_millis(1500)).await;

        // Roll a fresh log tail (a small in-range write) AND flush it: the
        // post-split append to the sealed X allocates a new tail (evicting X
        // from the PS extent_info_cache so force-GC reads X as sealed), and the
        // flush advances `durable_ckpt_vp` PAST X so the replay-floor guard no
        // longer protects X from GC. Small value ⇒ not a VP ⇒ never a GC
        // verdict-pause target.
        ps_put(&ps, 901, b"azz", b"s").await;
        ps_flush(&ps, 901).await;
        compio::time::sleep(Duration::from_millis(500)).await;

        // ── The race window ──────────────────────────────────────────────
        // (1) Arm the GC verdict pause and trigger force-GC on X. partition_loop
        //     is FREE (no Phase-3 hold yet), so the MAINTENANCE RPC dispatches;
        //     GC scans kmm, does its SST lookup, and PARKS before the verdict.
        let gc_parked0 = autumn_partition_server::background::gc_verdict_parked_count();
        autumn_partition_server::background::set_gc_verdict_pause(true);
        force_gc(&ps, 901, vec![x]).await;
        wait_counter(
            autumn_partition_server::background::gc_verdict_parked_count,
            gc_parked0,
            "GC to park at the verdict",
        )
        .await;

        // (2) Hold the next Put after Phase 2 (seq assigned, WAL durable) but
        //     before Phase 3, then issue Put(kmm = v_new) on a detached task.
        //     Phase 1 bumps seq_number ABOVE v_old's seq.
        let parked0 = autumn_partition_server::write_phase3_parked_count();
        autumn_partition_server::set_write_phase3_pause(true);
        let ps_for_put = ps.clone();
        let v_new_for_put = v_new.clone();
        let put_task = compio::runtime::spawn(async move {
            ps_put(&ps_for_put, 901, b"kmm", &v_new_for_put).await;
        });
        wait_counter(
            autumn_partition_server::write_phase3_parked_count,
            parked0,
            "the target Put to reach the Phase-3 hold",
        )
        .await;

        // (3) Release GC. Its verdict now runs while kmm is seq-assigned but NOT
        //     in the memtable: (pre-fix) it judges v_old live and relocates it at
        //     a FRESH seq HIGHER than the held Put.
        autumn_partition_server::background::set_gc_verdict_pause(false);
        compio::time::sleep(Duration::from_millis(2500)).await;

        // (4) Release the Put → its Phase 3 inserts kmm = v_new at its lower seq.
        autumn_partition_server::set_write_phase3_pause(false);
        let _ = put_task.await;
        compio::time::sleep(Duration::from_millis(500)).await;

        // The in-flight Put MUST survive. Pre-fix: GC's relocated OLD value
        // (higher seq) shadows it → reads v_old.
        let got = ps_get(&ps, 901, b"kmm").await;
        assert_eq!(
            got.value,
            v_new,
            "GC relocated the OLD value of kmm at a higher seq and shadowed the \
             in-flight (WAL-queued) Put of the NEW value (got {:?}..., expected v_new b'B'*)",
            &got.value.get(..4)
        );
    });
}
