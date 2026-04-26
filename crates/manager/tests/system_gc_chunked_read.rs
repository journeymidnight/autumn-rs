//! F105/F106 system test: chunked `read_bytes_from_extent` keeps GC
//! working when a sealed log_stream extent exceeds the per-syscall
//! read ceiling (macOS pread INT_MAX, Linux 0x7ffff000).
//!
//! Pre-F105 the user observed `GC run_gc extent X: rpc status Internal:
//! Invalid argument (os error 22)` repeated for every 30s GC tick on a
//! 3 GiB log_stream extent — the partition server's run_gc slurped the
//! whole extent into one Vec via a single `read_bytes_from_extent(eid,
//! 0, sealed_length)` call, which the server-side `pread` rejected with
//! EINVAL once length crossed `INT_MAX` (~2 GiB).
//!
//! This test reproduces the failure mode at small scale by overriding
//! `AUTUMN_STREAM_READ_CHUNK_BYTES` to a tiny value (forcing the
//! chunked path on a regular workload) AND `AUTUMN_PS_GC_READ_CHUNK_BYTES`
//! (forcing run_gc's record-streaming carry across every chunk
//! boundary). With both knobs set, GC must still reclaim the dead VPs
//! and a normal Get must still return the live value.
//!
//! This binary is its own test target so the env-var overrides don't
//! leak into other tests' StreamClients running in the same process.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use support::*;

#[test]
fn f105_gc_works_on_large_extent_via_chunked_reads() {
    // Force StreamClient to chunk reads at 1 KiB and run_gc to carry
    // records across 512 B GC chunks. With ~8 KiB values, every read
    // and every record will straddle a chunk boundary — exercising the
    // F105 chunked read path AND the F106 carry-forward path on every
    // step. `set_var` is fine here because this is the only test in
    // this binary.
    //
    // SAFETY: std::env::set_var is unsafe in 1.84+ because env mutation
    // races with concurrent reads in other threads. This test binary
    // contains exactly one test and no other thread has spawned yet.
    unsafe {
        std::env::set_var("AUTUMN_STREAM_READ_CHUNK_BYTES", "1024");
        std::env::set_var("AUTUMN_PS_GC_READ_CHUNK_BYTES", "512");
    }

    let (mgr_addr, n1_addr, n2_addr, _n1_dir, _n2_dir) = setup_two_node_infra(7100);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 7100).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        let part_id = 901;
        upsert_partition(&mgr, part_id, log, row, meta, b"a", b"z").await;

        let ps_addr = pick_addr();
        start_partition_server(91, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // Round 1 — write 4 large keys (>4 KiB triggers VP path).
        let val_v1: Vec<u8> = vec![b'A'; 8 * 1024];
        for i in 0u8..4 {
            ps_put(
                &ps,
                part_id,
                format!("key-{}", i).as_bytes(),
                &val_v1,
                false,
            )
            .await;
            ps_flush(&ps, part_id).await;
        }

        // Round 2 — overwrite same keys (creates dead VPs in the old
        // log_stream extent).
        let val_v2: Vec<u8> = vec![b'B'; 8 * 1024];
        for i in 0u8..4 {
            ps_put(
                &ps,
                part_id,
                format!("key-{}", i).as_bytes(),
                &val_v2,
                false,
            )
            .await;
            ps_flush(&ps, part_id).await;
        }

        // Compaction populates discard map. GC re-writes live VPs and
        // punches the dead extent. Both paths now go through chunked
        // reads at 1 KiB / 512 B granularity.
        ps_compact(&ps, part_id).await;
        compio::time::sleep(Duration::from_millis(1500)).await;
        ps_gc(&ps, part_id).await;
        compio::time::sleep(Duration::from_millis(2000)).await;

        // All reads must still return v2. If chunked read or GC carry
        // misbehaves, a Get would either fail or return garbage.
        for i in 0u8..4 {
            let resp = ps_get(&ps, part_id, format!("key-{}", i).as_bytes()).await;
            assert_eq!(
                resp.value, val_v2,
                "key-{}: stale or corrupt value after chunked-read GC",
                i
            );
        }
    });
}
