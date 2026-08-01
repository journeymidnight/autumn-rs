//! Data integrity: a flush must stamp its output SST's vp_head from the imm's
//! ROTATION-time content boundary — NOT the live write cursor at flush-claim.
//!
//! Bug (pre-fix): `run_flush_async_phase_inner` read `p.vp_extent_id/vp_offset`
//! (the live cursor) at CLAIM time, not the position captured when the imm was
//! frozen. Foreground writes that land between an imm's rotation and its
//! (lagging, background) flush-claim push the cursor forward, so the flushed
//! SST's vp_head ends up PAST those acked-but-un-flushed writes. On crash before
//! they flush, recovery starts replay past them → silent data loss. (This is
//! also the premise `compaction_output_vp_head`'s MAX relies on: if an input
//! SST's vp_head is already ahead of its content, MAX inherits it.)
//!
//! Exercised deterministically via a flush test sync-point: pause the background
//! flush, write a batch that rotates an imm, write a tail that advances the
//! cursor (stays un-flushed), release the pause so the flush claims the imm AFTER
//! the cursor moved, wait for the flush to DURABLY commit (checkpoint published,
//! polled via `flush_commit_count` — not a sleep), then crash + recover.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;

use support::*;

/// Clears the process-global flush pause on drop, so a panic mid-test cannot
/// leave the background flush loop wedged for any later test in this binary.
struct PauseGuard;
impl Drop for PauseGuard {
    fn drop(&mut self) {
        autumn_partition_server::set_flush_test_pause(false);
    }
}

fn val_a(i: u32) -> Vec<u8> {
    // 512 B (inline, < VALUE_THROTTLE) so it fills the memtable toward the
    // lowered rotation threshold; content is verifiable on read-back.
    let mut v = format!("va-{i:02}").into_bytes();
    v.resize(512, b'.');
    v
}
fn val_b(i: u32) -> Vec<u8> {
    format!("vb-{i:02}").into_bytes()
}

#[test]
fn flush_must_stamp_rotation_vp_not_claim_cursor() {
    // Rotate on small writes + hold the background flush so its claim races the
    // cursor. Process-global (own test binary); the setter is first-call-wins,
    // so assert it took. The guard clears the pause even on panic.
    assert!(
        autumn_partition_server::set_flush_mem_bytes(8 * 1024),
        "flush_mem_bytes must be settable (OnceLock first-call-wins)"
    );
    autumn_partition_server::set_flush_test_pause(true);
    let _pause_guard = PauseGuard;

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
        register_two_nodes(&mgr, n1_addr, n2_addr, 59).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 905, log, row, meta, b"a", b"z").await;

        let ps1_addr = pick_addr();
        start_partition_server(79, mgr_addr, ps1_addr);
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        // Rotating batch: 20 × 512 B crosses the 8 KiB rotation threshold. The
        // flush is PAUSED, so the frozen imm stays queued (unclaimed). NOTE: the
        // exact rotation point depends on memtable accounting (key + value + ~32
        // overhead) — only the PREFIX of these keys is guaranteed to land in the
        // frozen imm; the tail (a-15..a-19, per the reproduced failure) spills
        // into the new active memtable, so it is un-flushed like the b-* keys.
        for i in 0u32..20 {
            ps_put(&ps1, 905, format!("a-{i:02}").as_bytes(), &val_a(i)).await;
        }

        // Tail batch: small keys that stay in the active memtable (un-flushed)
        // but advance the write cursor PAST the imm's rotation point.
        for i in 0u32..5 {
            ps_put(&ps1, 905, format!("b-{i:02}").as_bytes(), &val_b(i)).await;
        }

        // Release the flush: it now claims the imm AFTER the cursor moved. Pre-fix
        // it stamps the cursor (past the tail); post-fix it stamps the imm's
        // rotation vp (behind the tail). Wait for the flush to DURABLY commit
        // (checkpoint published) so recovery is guaranteed to load it — polling
        // a real signal, never a fixed sleep.
        let base_commits = autumn_partition_server::flush_commit_count();
        autumn_partition_server::set_flush_test_pause(false);
        let mut committed = false;
        for _ in 0..200 {
            if autumn_partition_server::flush_commit_count() > base_commits {
                committed = true;
                break;
            }
            compio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(committed, "flush never committed within 10s");
        // Small settle so the checkpoint's stream append is observable on reopen.
        compio::time::sleep(Duration::from_millis(200)).await;

        // Crash (NON-graceful): drop the client; a same-id PS takes over via
        // owner-epoch fencing and the old server self-evicts on LockedByOther
        // WITHOUT a graceful flush — so the un-flushed tail is abandoned and must
        // be rebuilt from the log replay window. Mirrors `system_crash_mid_flush`
        // / `system_crash_mid_compact`. We do NOT use
        // `start_partition_server_stoppable`: its graceful drain flushes every
        // imm, which would persist the tail and mask the loss.
        drop(ps1);

        let ps2_addr = pick_addr();
        start_partition_server(79, mgr_addr, ps2_addr);
        let ps2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");
        compio::time::sleep(Duration::from_millis(2000)).await;

        // Everything must survive. Pre-fix the un-flushed tail sits PAST the
        // flushed SST's vp_head and returns NotFound.
        for i in 0u32..20 {
            let key = format!("a-{i:02}");
            let resp = ps_get(&ps2, 905, key.as_bytes()).await;
            assert_eq!(resp.value, val_a(i), "{key} (rotating batch) must survive");
        }
        for i in 0u32..5 {
            let key = format!("b-{i:02}");
            let resp = ps_get(&ps2, 905, key.as_bytes()).await;
            assert_eq!(
                resp.value,
                val_b(i),
                "{key} (un-flushed tail) must stay inside the replay window — \
                 flush must stamp the imm's ROTATION vp, not the claim cursor"
            );
        }
    });
}
