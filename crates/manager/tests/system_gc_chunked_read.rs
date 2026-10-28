//! system test: chunked `read_bytes_from_extent` keeps GC
//! working when a sealed log_stream extent exceeds the per-syscall
//! read ceiling (macOS pread INT_MAX, Linux 0x7ffff000).
//!
//! Previously the user observed `GC run_gc extent X: rpc status Internal:
//! Invalid argument (os error 22)` repeated for every 30s GC tick on a
//! 3 GiB log_stream extent — the partition server's run_gc slurped the
//! whole extent into one Vec via a single `read_bytes_from_extent(eid,
//! 0, sealed_length)` call, which the server-side `pread` rejected with
//! EINVAL once length crossed `INT_MAX` (~2 GiB).
//!
//! This test reproduces the failure mode at small scale by overriding
//! `--stream-read-chunk-bytes` to a tiny value (forcing the
//! chunked path on a regular workload) AND `--gc-read-chunk-bytes`
//! (forcing run_gc's record-streaming carry across every chunk
//! boundary). With both knobs set, GC must still reclaim the dead VPs
//! and a normal Get must still return the live value.
//!
//! This binary is its own test target so the `params()` overrides don't
//! leak into other tests' StreamClients running in the same process.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{self, StreamInfoReq, StreamInfoResp, MSG_STREAM_INFO};
use autumn_rpc::partition_rpc;
use support::*;

async fn log_extents(mgr: &RpcClient, stream_id: u64) -> Vec<u64> {
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
    resp.streams
        .into_iter()
        .find(|(id, _)| *id == stream_id)
        .expect("stream in response")
        .1
        .extent_ids
}

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
    assert_eq!(resp.code, partition_rpc::CODE_OK, "roll_tails: {}", resp.message);
    resp.rolled
}

#[test]
fn gc_works_on_large_extent_via_chunked_reads() {
    // Force the PS's stream client to chunk reads at 1 KiB and run_gc to
    // carry records across 512 B GC chunks. With ~8 KiB values, every read
    // and every record will straddle a chunk boundary — exercising the
    // chunked read path AND the carry-forward path on every step. Set
    // before the PS opens the partition (its stream clients read them then).
    let ps_params = autumn_partition_server::params();
    ps_params.stream_read_chunk_bytes.set(1024);
    ps_params.gc_read_chunk_bytes.set(512);

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
        let mut first_tail = None;
        for i in 0u8..4 {
            ps_put(&ps, part_id, format!("key-{}", i).as_bytes(), &val_v1).await;
            if first_tail.is_none() {
                first_tail = log_extents(&mgr, log).await.last().copied();
            }
            ps_flush(&ps, part_id).await;
        }

        // Round 2 — overwrite same keys (creates dead VPs in E0).
        let val_v2: Vec<u8> = vec![b'B'; 8 * 1024];
        for i in 0u8..4 {
            ps_put(&ps, part_id, format!("key-{}", i).as_bytes(), &val_v2).await;
            ps_flush(&ps, part_id).await;
        }

        // Both rounds landed in E0, the log tail once the partition is open.
        let e0 = *log_extents(&mgr, log).await.last().expect("log tail");
        assert_eq!(first_tail, Some(e0), "both rounds must land in E0");

        // GC only takes sealed extents before the replay floor: seal E0, then
        // write and flush once so the checkpoint lies past it.
        assert_eq!(roll_tails(&ps, part_id, vec![(log, e0)]).await, 1, "roll the log tail");
        ps_put(&ps, part_id, b"post-roll", b"x").await;
        ps_flush(&ps, part_id).await;

        // Compaction populates the discard map; GC relocates the live VPs
        // through 1 KiB / 512 B chunked reads and drops E0.
        ps_compact(&ps, part_id).await;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let mut ext = log_extents(&mgr, log).await;
        while ext.contains(&e0) && std::time::Instant::now() < deadline {
            ps_gc(&ps, part_id).await;
            compio::time::sleep(Duration::from_millis(500)).await;
            ext = log_extents(&mgr, log).await;
        }
        assert!(!ext.contains(&e0), "GC must reclaim E0: log extents {ext:?}");

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
