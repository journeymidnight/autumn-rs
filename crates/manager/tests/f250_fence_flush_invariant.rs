//! F250 — deterministic single-thread reproducer for the fence+flush data-loss bug.
//!
//! The chaos test (`system_chaos.rs`) reliably surfaces a mismatch under
//! `AUTUMN_CHAOS_ACTIONS=fence,flush` with seed=3 (q000112 expected=4497
//! got=3985). It is Heisenbug-y: adding `RUST_LOG=info` masks the race.
//! Subprocess ENs + toxiproxy make timing non-deterministic.
//!
//! This test reproduces the same race in-process with:
//!   - In-process manager (no etcd, no leader election).
//!   - In-process extent nodes (no subprocess).
//!   - In-process partition server.
//!   - Single tokio compio runtime per logical role.
//!   - Tight nemesis interval (50 ms) → many more racy events per second
//!     than the chaos test's 3000 ms → faster discovery.
//!
//! Invariant under test (the chaos test's per-key check, distilled):
//!   For every key K that the writer received a `CODE_OK` Put response
//!   on, a subsequent Get(K) MUST return the latest written value.
//!
//! A violation = the data-loss bug. The test asserts this directly
//! instead of comparing whole-stream snapshots — easier to localise
//! the failing write event in stderr.

mod support;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc;
use bytes::Bytes;

use support::*;

// ─── Tunables ─────────────────────────────────────────────────────────

const DURATION_S: u64 = 30;
const NEMESIS_INTERVAL_MS: u64 = 250;
const KEY_COUNT: u32 = 1024; // mimic chaos test 4×256 keys
const WRITER_COUNT: u32 = 4;
const VICTIM_NODE: u64 = 1; // EN[0] gets fenced repeatedly
const PART_ID: u64 = 9001;

// ─── Test value encoding ──────────────────────────────────────────────

/// Encode the writer's local seq into the value bytes so a mismatch
/// reveals BOTH the expected and observed seq.
fn make_value(key: &[u8], seq: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(b"f250-");
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(b":");
    out.extend_from_slice(key);
    while out.len() < 128 {
        out.extend_from_slice(b"x");
    }
    out
}

fn parse_value_seq(v: &[u8]) -> Option<u64> {
    if v.len() < 13 || &v[..5] != b"f250-" {
        return None;
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&v[5..13]);
    Some(u64::from_le_bytes(bytes))
}

// ─── Fence + Unfence helpers ──────────────────────────────────────────

async fn fence_node(mgr: &RpcClient, node_id: u64) -> Result<(), String> {
    let resp = mgr
        .call(
            MSG_FENCE_NODE,
            rkyv_encode(&FenceNodeReq {
                node_id,
                reason: "f250-test".into(),
                set_by: "f250".into(),
                force: true,
            }),
        )
        .await
        .map_err(|e| format!("fence rpc: {e}"))?;
    let r: CodeResp = rkyv_decode(&resp).map_err(|e| format!("decode: {e}"))?;
    if r.code != CODE_OK {
        return Err(format!("fence refused: {}", r.message));
    }
    Ok(())
}

async fn clear_override(mgr: &RpcClient, node_id: u64) -> Result<(), String> {
    let resp = mgr
        .call(
            MSG_CLEAR_NODE_OVERRIDE,
            rkyv_encode(&ClearNodeOverrideReq {
                node_id,
                set_by: "f250".into(),
            }),
        )
        .await
        .map_err(|e| format!("clear rpc: {e}"))?;
    let r: CodeResp = rkyv_decode(&resp).map_err(|e| format!("decode: {e}"))?;
    if r.code != CODE_OK {
        return Err(format!("clear refused: {}", r.message));
    }
    Ok(())
}

async fn flush_partition(ps: &RpcClient) -> Result<(), String> {
    let resp = ps
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id: PART_ID,
                op: partition_rpc::MAINTENANCE_FLUSH,
                extent_ids: vec![],
                gc_ratio: None,
                gc_max_size: None,
                gc_stream_debt: None,
                gc_empty_only: false,
            }),
        )
        .await
        .map_err(|e| format!("flush rpc: {e}"))?;
    let r: partition_rpc::MaintenanceResp =
        partition_rpc::rkyv_decode(&resp).map_err(|e| format!("decode: {e}"))?;
    if r.code != partition_rpc::CODE_OK {
        return Err(format!("flush refused: {}", r.message));
    }
    Ok(())
}

// ─── Writer loop ──────────────────────────────────────────────────────

async fn writer_loop(
    writer_id: u32,
    ps: Rc<RpcClient>,
    expected: Rc<RefCell<HashMap<Vec<u8>, (u64, Vec<u8>)>>>,
    stop: Arc<AtomicBool>,
    acked: Arc<AtomicU64>,
    failed: Arc<AtomicU64>,
) {
    let mut seq: u64 = 0;
    // Per-writer LCG seed so the 4 writers don't all hit the same keys.
    let mut lcg: u64 =
        0x9E3779B97F4A7C15u64.wrapping_add((writer_id as u64).wrapping_mul(0xBF58476D1CE4E5B9));
    // Each writer gets a disjoint key slice of KEY_COUNT/WRITER_COUNT keys
    // so writers don't fight over the same expected[] entry — mirrors
    // chaos test's per-prefix workload.
    let slice = KEY_COUNT / WRITER_COUNT;
    let key_start = writer_id * slice;
    while !stop.load(Ordering::Relaxed) {
        seq += 1;
        lcg = lcg
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let kid = key_start + ((lcg >> 32) as u32 % slice);
        let key = format!("k{:06}", kid).into_bytes();
        let value = make_value(&key, seq);
        let resp = ps
            .call(
                partition_rpc::MSG_PUT,
                partition_rpc::rkyv_encode(&partition_rpc::PutReq {
                    part_id: PART_ID,
                    key: key.clone(),
                    value: value.clone(),
                    expires_at: 0,
                    region_epoch: 0,
                inode_hint: 0,
                lease_epoch: 0,
                }),
            )
            .await;
        match resp {
            Ok(r) => match partition_rpc::rkyv_decode::<partition_rpc::PutResp>(&r) {
                Ok(p) if p.code == partition_rpc::CODE_OK => {
                    expected.borrow_mut().insert(key, (seq, value));
                    acked.fetch_add(1, Ordering::Relaxed);
                }
                _ => {
                    failed.fetch_add(1, Ordering::Relaxed);
                }
            },
            Err(_) => {
                failed.fetch_add(1, Ordering::Relaxed);
                compio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

// ─── Nemesis loop ─────────────────────────────────────────────────────

async fn nemesis_loop(
    mgr: Rc<RpcClient>,
    ps: Rc<RpcClient>,
    stop: Arc<AtomicBool>,
    flush_count: Arc<AtomicU64>,
    fence_count: Arc<AtomicU64>,
) {
    let mut tick: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        compio::time::sleep(Duration::from_millis(NEMESIS_INTERVAL_MS)).await;
        if stop.load(Ordering::Relaxed) {
            break;
        }
        tick += 1;
        if tick.is_multiple_of(2) {
            // Even ticks: flush.
            let _ = flush_partition(&ps).await;
            flush_count.fetch_add(1, Ordering::Relaxed);
        } else {
            // Odd ticks: fence + unfence.
            if fence_node(&mgr, VICTIM_NODE).await.is_ok() {
                compio::time::sleep(Duration::from_millis(20)).await;
                let _ = clear_override(&mgr, VICTIM_NODE).await;
                fence_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

// ─── The test ─────────────────────────────────────────────────────────

#[test]
#[ignore]
fn f250_fence_flush_invariant() {
    // ── Manager (in-process, memory-only) ──
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    std::thread::sleep(Duration::from_millis(200));

    // ── 4 ENs (in-process) ──
    let mut en_dirs = Vec::new();
    let mut en_addrs = Vec::new();
    for i in 0..4 {
        let dir = tempfile::tempdir().expect("en tempdir").keep();
        let addr = pick_addr();
        start_extent_node(addr, dir.clone(), (i + 1) as u64);
        en_dirs.push(dir);
        en_addrs.push(addr);
    }
    std::thread::sleep(Duration::from_millis(400));

    // ── Register ENs with the manager ──
    let outer_rt = compio::runtime::Runtime::new().unwrap();
    let (en_node_ids, _mgr, log_stream, row_stream, meta_stream, ps_addr) =
        outer_rt.block_on(async {
            let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
            // wait for leader
            for _ in 0..30 {
                if mgr.call(MSG_STATUS, Bytes::new()).await.is_ok() {
                    break;
                }
                compio::time::sleep(Duration::from_millis(100)).await;
            }

            let mut node_ids = Vec::new();
            for (i, addr) in en_addrs.iter().enumerate() {
                let r = register_node(&mgr, &addr.to_string(), &format!("disk-uuid-{i}")).await;
                assert_eq!(r.code, CODE_OK, "register_node EN[{i}]: {}", r.message);
                node_ids.push(r.node_id);
            }
            // wait for ENs to df + transition to Online
            compio::time::sleep(Duration::from_secs(3)).await;

            // 3 replicas; 4 ENs → one fence still leaves 3 healthy.
            let log = create_stream(&mgr, 3).await;
            let row = create_stream(&mgr, 3).await;
            let meta = create_stream(&mgr, 3).await;
            upsert_partition(&mgr, PART_ID, log, row, meta, b"\x00", b"\xff\xff\xff\xff").await;

            // PS
            let ps_addr = pick_addr();
            start_partition_server(91, mgr_addr, ps_addr);
            compio::time::sleep(Duration::from_secs(2)).await;
            (node_ids, mgr, log, row, meta, ps_addr)
        });
    let _ = (log_stream, row_stream, meta_stream); // streams reachable via manager

    eprintln!(
        "f250: setup complete; ENs={:?} victim_node={}",
        en_node_ids, VICTIM_NODE
    );

    // ── Stress phase ──
    let expected: Rc<RefCell<HashMap<Vec<u8>, (u64, Vec<u8>)>>> =
        Rc::new(RefCell::new(HashMap::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let flush_count = Arc::new(AtomicU64::new(0));
    let fence_count = Arc::new(AtomicU64::new(0));

    let (acked_c, failed_c, flush_c, fence_c) = (
        acked.clone(),
        failed.clone(),
        flush_count.clone(),
        fence_count.clone(),
    );
    let stop_c = stop.clone();

    let stress_rt = compio::runtime::Runtime::new().unwrap();
    let final_expected = stress_rt.block_on(async move {
        let ps: Rc<RpcClient> = RpcClient::connect(ps_addr).await.expect("connect ps");
        let mgr_c: Rc<RpcClient> = RpcClient::connect(mgr_addr).await.expect("connect mgr2");

        // Spawn WRITER_COUNT writers (mirrors chaos test).
        let mut writers = Vec::new();
        for wid in 0..WRITER_COUNT {
            writers.push(compio::runtime::spawn(writer_loop(
                wid,
                ps.clone(),
                expected.clone(),
                stop_c.clone(),
                acked_c.clone(),
                failed_c.clone(),
            )));
        }
        let n = compio::runtime::spawn(nemesis_loop(
            mgr_c.clone(),
            ps.clone(),
            stop_c.clone(),
            flush_c,
            fence_c,
        ));

        // Run for DURATION_S then stop.
        compio::time::sleep(Duration::from_secs(DURATION_S)).await;
        stop_c.store(true, Ordering::Relaxed);
        for w in writers {
            let _ = w.await;
        }
        let _ = n.await;

        // settle: let any in-flight flushes complete
        compio::time::sleep(Duration::from_secs(3)).await;

        expected.borrow().clone()
    });

    eprintln!(
        "f250: stress done — acked={} failed={} flushes={} fences={} expected_keys={}",
        acked.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed),
        flush_count.load(Ordering::Relaxed),
        fence_count.load(Ordering::Relaxed),
        final_expected.len(),
    );

    // ── Verify ──
    let verify_rt = compio::runtime::Runtime::new().unwrap();
    let mismatches = verify_rt.block_on(async {
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        let mut mismatches: Vec<(Vec<u8>, u64, Option<u64>)> = Vec::new();
        for (key, (exp_seq, exp_val)) in final_expected.iter() {
            let resp = ps
                .call(
                    partition_rpc::MSG_GET,
                    partition_rpc::rkyv_encode(&partition_rpc::GetReq {
                        part_id: PART_ID,
                        key: key.clone(),
                        offset: 0,
                        length: 0,
                        region_epoch: 0,
                    }),
                )
                .await;
            let got_seq = match resp {
                Ok(r) => match partition_rpc::rkyv_decode::<partition_rpc::GetResp>(&r) {
                    Ok(g) if g.code == partition_rpc::CODE_OK => parse_value_seq(&g.value),
                    _ => None,
                },
                Err(_) => None,
            };
            if got_seq != Some(*exp_seq) {
                let _ = exp_val;
                mismatches.push((key.clone(), *exp_seq, got_seq));
            }
        }
        mismatches
    });

    let n = mismatches.len();
    // F250 diagnostic: trace where each failing key's MVCC entries live
    // across memtable / imm / SSTs to localise the data-loss root cause.
    let trace_rt = compio::runtime::Runtime::new().unwrap();
    trace_rt.block_on(async {
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");
        for (key, exp, got) in mismatches.iter().take(20) {
            let resp = ps
                .call(
                    partition_rpc::MSG_DIAG_TRACE_KEY,
                    partition_rpc::rkyv_encode(&partition_rpc::DiagTraceKeyReq {
                        part_id: PART_ID,
                        user_key: key.clone(),
                    }),
                )
                .await;
            eprintln!(
                "  mismatch: {} expected_value_seq={} got_value_seq={:?}",
                String::from_utf8_lossy(key), exp, got
            );
            match resp {
                Ok(r) => match partition_rpc::rkyv_decode::<partition_rpc::DiagTraceKeyResp>(&r) {
                    Ok(t) => {
                        eprintln!(
                            "    PS state: memtable_ps_seq={} imm_ps_seqs={:?} sst_ps_seqs={:?} sst_last_seqs={:?}",
                            t.memtable_seq, t.imm_seqs, t.sst_seqs, t.sst_last_seqs
                        );
                        // Bloom false-negative detector: any SST where the
                        // no-bloom scan finds the key but the bloom-gated
                        // scan returned 0.
                        let fn_idx: Vec<(usize, u64)> = t
                            .sst_seqs
                            .iter()
                            .zip(t.sst_seqs_nobloom.iter())
                            .enumerate()
                            .filter(|(_, (b, nb))| **b == 0 && **nb != 0)
                            .map(|(i, (_, nb))| (i, *nb))
                            .collect();
                        if !fn_idx.is_empty() {
                            eprintln!(
                                "    *** BLOOM FALSE NEGATIVE at SST idx (idx,seq)={:?}  nobloom={:?}",
                                fn_idx, t.sst_seqs_nobloom
                            );
                        }
                        // Block-selection bug detector: fullscan finds the
                        // key but the single-block (find_block_for_key)
                        // scan returned 0.
                        let bs_idx: Vec<(usize, u64)> = t
                            .sst_seqs_nobloom
                            .iter()
                            .zip(t.sst_seqs_fullscan.iter())
                            .enumerate()
                            .filter(|(_, (nb, fs))| **nb == 0 && **fs != 0)
                            .map(|(i, (_, fs))| (i, *fs))
                            .collect();
                        if !bs_idx.is_empty() {
                            eprintln!(
                                "    *** BLOCK-SELECTION MISS at SST idx (idx,seq)={:?}  fullscan={:?}",
                                bs_idx, t.sst_seqs_fullscan
                            );
                        } else {
                            // Fullscan agrees with single-block scan → the
                            // key is genuinely ABSENT where 0. Report the
                            // global max seq found by fullscan for context.
                            let maxfs = t.sst_seqs_fullscan.iter().copied().max().unwrap_or(0);
                            eprintln!("    fullscan max seq across all SSTs = {maxfs}");
                        }
                    }
                    Err(e) => eprintln!("    PS state: decode err: {}", e),
                },
                Err(e) => eprintln!("    PS state: rpc err: {}", e),
            }
        }
    });
    assert_eq!(
        n, 0,
        "{} mismatches — fence+flush race lost ack'd writes",
        n
    );

    // cleanup
    drop(en_dirs);
}
