//! F120-C — graceful shutdown end-to-end.
//!
//! Strategy:
//!   1. Bring up a 1-node cluster (manager + 1 extent node).
//!   2. Spawn a partition server on a thread we control via an
//!      `Arc<AtomicBool>` shutdown flag. The thread runs
//!      `ps.serve_until_shutdown(addr, fut)` where `fut` polls the flag.
//!   3. Write enough small entries to populate the active memtable
//!      WITHOUT triggering a 256 MB FLUSH_MEM_BYTES rotate (we want
//!      active to hold un-flushed data when shutdown fires).
//!   4. Set the shutdown flag. Assert the thread joins within
//!      `AUTUMN_PS_SHUTDOWN_TIMEOUT_MS` (set tight at 5 s for the test).
//!   5. After the thread exits cleanly, spawn a SECOND PS pointing at
//!      the same partitions. Verify that every previously-written key
//!      is readable. This proves the post-shutdown on-disk state
//!      survives — either via SST written during graceful drain
//!      (F120-C path) or via logStream replay (pre-F120 fallback).
//!      Either way the data is safe; the deadline assertion + a clean
//!      thread join differentiates regression vs. SIGKILL race.
//!
//! What it does NOT verify (by design — too brittle to assert
//! externally):
//!   - That the SST was written DURING shutdown (vs. via replay on
//!     restart). The right assertion would be "metaStream's vp_offset
//!     equals log_stream commit_length after restart" but the manager
//!     surface for those values requires extra RPC plumbing — covered
//!     by live cluster verification per feature_list.md F120 acceptance.
//!   - The `MAX_IMM_DEPTH` back-pressure path (F120-A) — that requires
//!     a slowed-down P-bulk fixture to actually fill imm.
//!   - The `MAX_WAL_GAP` force-rotate path (F120-B) — same.

mod support;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_partition_server::PartitionServer;
use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc::CODE_OK;
use support::*;

#[test]
fn f120_graceful_shutdown_drains_active_to_sst() {
    // Tighten shutdown deadline so the test fails fast on regression.
    // Read by `shutdown_timeout_ms()` once via OnceLock — must be set
    // before the PS thread spawns.
    std::env::set_var("AUTUMN_PS_SHUTDOWN_TIMEOUT_MS", "5000");

    let mgr_addr = pick_addr();
    let n1_addr = pick_addr();
    let ps_addr = pick_addr();

    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("tempdir");
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);

    // Bootstrap: register node, create three streams, upsert one
    // partition.
    let part_id: u64 = 100;
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("mgr");
        let _ = register_node(&mgr, &n1_addr.to_string(), "uuid-f120-1").await;
        // replicates=1 fits the single-node cluster.
        let log = create_stream(&mgr, 1).await;
        let row = create_stream(&mgr, 1).await;
        let meta = create_stream(&mgr, 1).await;
        upsert_partition(&mgr, part_id, log, row, meta, b"", b"\xff").await;
    });

    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let join = spawn_ps_with_shutdown(1, mgr_addr, ps_addr, shutdown_flag.clone());

    // Wait for PS to bind its per-partition listener.
    std::thread::sleep(Duration::from_millis(800));

    // Write 64 entries × 256 B ≈ 16 KB — well below FLUSH_MEM_BYTES.
    let n_keys: usize = 64;
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let ps = RpcClient::connect(ps_addr).await.expect("ps connect");
        for i in 0..n_keys {
            let key = format!("k{:04}", i);
            let value = vec![b'v'; 256];
            ps_put(&ps, part_id, key.as_bytes(), &value, true).await;
        }
    });

    // Trigger graceful shutdown.
    let shutdown_started = Instant::now();
    shutdown_flag.store(true, Ordering::Release);

    // Bound on test wallclock: should be well under shutdown_timeout_ms
    // (5 s). Allow some margin for compio/test scheduling.
    let join_deadline = Duration::from_secs(15);
    let join_start = Instant::now();
    while !join.is_finished() {
        assert!(
            join_start.elapsed() < join_deadline,
            "PS thread did not exit within {:?}",
            join_deadline,
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let join_result = join.join();
    let total_shutdown = shutdown_started.elapsed();
    assert!(
        join_result.is_ok(),
        "PS thread panicked during graceful shutdown: {:?}",
        join_result.err(),
    );
    assert!(
        total_shutdown < Duration::from_secs(10),
        "graceful shutdown took {:?} (deadline 10 s — likely regression)",
        total_shutdown,
    );
    println!("F120-C: graceful shutdown took {:?}", total_shutdown);

    // Spawn a second PS (different psid, same partition slot) and verify
    // the data is queryable. Manager rebalances the partition onto the
    // new PS once the first one's heartbeat times out (10 s default,
    // F069). We wait long enough.
    std::thread::sleep(Duration::from_secs(11));
    let ps2_addr = pick_addr();
    let shutdown_flag2 = Arc::new(AtomicBool::new(false));
    let join2 = spawn_ps_with_shutdown(2, mgr_addr, ps2_addr, shutdown_flag2.clone());
    std::thread::sleep(Duration::from_millis(2000));

    // Resolve the partition's current PS address via a region query.
    let recovered = compio::runtime::Runtime::new().unwrap().block_on(async {
        // Loop a few seconds until rebalance completes — region_sync_loop
        // ticks every 2 s. `part_addrs` is `Vec<(part_id, address)>`.
        let mut last_addr: Option<String> = None;
        for _ in 0..40 {
            let mgr = RpcClient::connect(mgr_addr).await.expect("mgr");
            let regions = get_regions(&mgr).await;
            for (pid, addr) in &regions.part_addrs {
                if *pid == part_id && !addr.is_empty() && addr != "unknown" {
                    last_addr = Some(addr.clone());
                    break;
                }
            }
            if last_addr.is_some() {
                break;
            }
            compio::time::sleep(Duration::from_millis(500)).await;
        }
        let addr = last_addr.expect("partition has no PS address after rebalance");
        let psr: SocketAddr = addr.parse().expect("parse part_addr");
        let ps = RpcClient::connect(psr).await.expect("ps2 connect");

        // Fetch each previously-written key.
        let mut hits = 0usize;
        for i in 0..n_keys {
            let key = format!("k{:04}", i);
            let resp = ps_get(&ps, part_id, key.as_bytes()).await;
            if resp.code == CODE_OK && resp.value.len() == 256 {
                hits += 1;
            }
        }
        hits
    });
    assert_eq!(
        recovered, n_keys,
        "expected all {n_keys} keys recovered post-shutdown; got {recovered}",
    );

    // Tear down the second PS as well.
    shutdown_flag2.store(true, Ordering::Release);
    let _ = join2.join();
}

/// Spawn an `autumn-ps` thread that runs `serve_until_shutdown` driven
/// by `shutdown_flag`. Returns the join handle so the test can verify
/// the thread exits cleanly after flag-set.
fn spawn_ps_with_shutdown(
    ps_id: u64,
    mgr_addr: SocketAddr,
    ps_addr: SocketAddr,
    shutdown_flag: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let ps = PartitionServer::connect_with_advertise_and_port(
                ps_id,
                &mgr_addr.to_string(),
                Some(ps_addr.to_string()),
                ps_addr,
            )
            .await
            .expect("connect partition server");
            ps.sync_regions_once().await.expect("sync regions");

            let flag = shutdown_flag.clone();
            let shutdown_fut = async move {
                loop {
                    if flag.load(Ordering::Acquire) {
                        return;
                    }
                    compio::time::sleep(Duration::from_millis(50)).await;
                }
            };
            ps.serve_until_shutdown(ps_addr, shutdown_fut)
                .await
                .expect("serve_until_shutdown");
        });
    })
}
