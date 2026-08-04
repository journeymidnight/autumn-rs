//! System test (reproduce-FIRST) — G8 anomaly:
//! "node rejoin with a WIPED data dir under the SAME identity → potential
//!  silent truncation of healthy replicas."
//!
//! Hypothesis under test (from stream CLAUDE.md "Commit protocol — all-replica,
//! NO quorum"): the append commit_length is MIN-over-replicas. If a *reachable*
//! replica that lost its data reported a SHORT/zero length that got folded into
//! that min, the next append's `header.commit` (or a manager seal) would
//! truncate acked data on the up-to-date replicas → silent loss.
//!
//! Scenario reproduced here on a single-host thread cluster, RF3:
//!   1. Write + ACK a batch of records: some land in a SEALED extent (E1),
//!      some in the OPEN tail (E2). Snapshot every acked (extent,offset,len).
//!   2. "SIGKILL" one EN (drop its compio runtime → listener + conns torn
//!      down = connection-refused to clients), `rm -rf` its `--data` dir
//!      contents (simulated disk replacement), restart it on the SAME
//!      address/port/disk_id/data-dir (now empty). It rejoins Online (df
//!      healthy) and now answers NotFound for every extent it used to hold.
//!   3. Drive a commit-length check + reads + a fresh-writer tail-init.
//!      Assert: every acked record is still byte-identical readable, and NO
//!      extent's commit_length was lowered by the wiped replica's emptiness.
//!      A violation (acked record gone, or commit_length == 0 / < acked) =
//!      reproduced silent truncation.
//!
//! Result: silent truncation does NOT reproduce — the empty replica is
//! EXCLUDED from every min (EN answers a frame-level NotFound, never a
//! 0-length; the client's `current_commit` counts it as a non-response and
//! refuses to seed a subset min; the manager's `compute_commit_seal` mins only
//! over responders). See the module-level assertions + the agent report.
//!
//! DISTINCT filename + only-local helpers on purpose: this test touches no
//! shared harness file beyond READING `support` (mirrors
//! `system_extent_recovery::start_en_with_manager`).

mod support;

use std::rc::Rc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ConnPool, ExtentNode, ExtentNodeConfig, StreamClient};

use support::*;

// ── local EN helpers (manager endpoint wired so recovery / orphan-reconcile
//    can resolve extent_info; stoppable so we can model a real node death) ──

/// Non-stoppable EN with the manager endpoint wired.
fn start_en_with_mgr(
    addr: std::net::SocketAddr,
    dir: std::path::PathBuf,
    disk_id: u64,
    mgr_addr: std::net::SocketAddr,
) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let cfg = ExtentNodeConfig::new(dir, disk_id).with_manager_endpoint(mgr_addr.to_string());
            let n = ExtentNode::new(cfg).await.expect("extent node");
            let _ = n.serve(addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));
}

/// Stoppable EN with the manager endpoint wired. `.shutdown()` + join drops
/// the compio runtime → the detached `serve` task, its listener, and every
/// live connection are cancelled; subsequent client connects get
/// ECONNREFUSED — a real node death as clients observe it.
fn start_en_with_mgr_stoppable(
    addr: std::net::SocketAddr,
    dir: std::path::PathBuf,
    disk_id: u64,
    mgr_addr: std::net::SocketAddr,
) -> (ShutdownFlag, std::thread::JoinHandle<()>) {
    let flag = ShutdownFlag::new();
    let flag_thread = flag.clone();
    let handle = std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let cfg = ExtentNodeConfig::new(dir, disk_id).with_manager_endpoint(mgr_addr.to_string());
            let n = ExtentNode::new(cfg).await.expect("extent node");
            compio::runtime::spawn(async move {
                if let Err(e) = n.serve(addr).await {
                    eprintln!("EN serve({addr}) exited: {e}");
                }
            })
            .detach();
            while !flag_thread.is_shutdown() {
                compio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    });
    std::thread::sleep(Duration::from_millis(200));
    (flag, handle)
}

/// Register an EN carrying a STABLE `node_uuid` — the node's real identity
/// (survives address/port changes, mirroring a k8s pod reschedule onto a fresh
/// IP). Re-registering the same `node_uuid` at a NEW address makes the manager
/// update that node's location IN PLACE, keeping the SAME node_id — exactly the
/// "same identity, new location" rejoin the G8 scenario needs. (Killing an EN
/// and rebinding its exact port is unreliable here: compio releases the
/// listener FD asynchronously on runtime drop, and the EN is fail-stop on bind
/// conflict — so identity is carried by `node_uuid`, not the port.)
async fn register_node_uuid(
    mgr: &RpcClient,
    addr: &str,
    disk_uuid: &str,
    node_uuid: &str,
) -> RegisterNodeResp {
    let resp = mgr
        .call(
            MSG_REGISTER_NODE,
            rkyv_encode(&RegisterNodeReq {
                addr: addr.to_string(),
                disk_uuids: vec![disk_uuid.to_string()],
                shard_ports: vec![],
                control_address: String::new(),
                node_uuid: node_uuid.to_string(),
            }),
        )
        .await
        .expect("register node");
    rkyv_decode::<RegisterNodeResp>(&resp).expect("decode RegisterNodeResp")
}

/// Remove every file/subdir under `dir` but KEEP the dir itself — a simulated
/// disk swap. The restarted EN then finds a clean empty data dir (test ENs run
/// fine on an unformatted empty dir — see `support::start_extent_node`).
fn wipe_data_dir(dir: &std::path::Path) {
    for entry in std::fs::read_dir(dir).expect("read_dir data dir") {
        let p = entry.expect("dir entry").path();
        if p.is_dir() {
            std::fs::remove_dir_all(&p).expect("rm -rf subdir");
        } else {
            std::fs::remove_file(&p).expect("rm file");
        }
    }
}

/// Poll `MSG_LIST_NODE_STATES` until the node at `address` reports
/// `NODE_AUTO_STATE_ONLINE` (df-verified alive), or timeout.
async fn wait_node_online(mgr: &RpcClient, address: &str, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    loop {
        let resp = mgr
            .call(MSG_LIST_NODE_STATES, rkyv_encode(&ListNodeStatesReq {}))
            .await;
        if let Ok(bytes) = resp {
            if let Ok(r) = rkyv_decode::<ListNodeStatesResp>(&bytes) {
                if r.nodes.iter().any(|n| {
                    n.address == address && n.auto_state == NODE_AUTO_STATE_ONLINE
                }) {
                    return true;
                }
            }
        }
        if start.elapsed() >= timeout {
            return false;
        }
        compio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Manager-side authoritative commit-length probe on a stream's current tail.
async fn check_commit_length(
    mgr: &RpcClient,
    stream_id: u64,
    owner_key: &str,
    owner_epoch: i64,
) -> CheckCommitLengthResp {
    let bytes = mgr
        .call(
            MSG_CHECK_COMMIT_LENGTH,
            rkyv_encode(&CheckCommitLengthReq {
                stream_id,
                owner_key: owner_key.to_string(),
                owner_epoch,
            }),
        )
        .await
        .expect("check_commit_length RPC");
    rkyv_decode(&bytes).expect("decode CheckCommitLengthResp")
}

/// One acked record: where it lives + its exact bytes, for a byte-identical
/// read-back assertion after the wipe.
#[derive(Clone)]
struct AckedRecord {
    extent_id: u64,
    offset: u64,
    bytes: Vec<u8>,
}

/// Append `payloads` to the current tail via `sc`, returning the acked
/// (extent,offset,bytes) for each and the max `end` observed.
async fn append_records(
    sc: &StreamClient,
    stream_id: u64,
    payloads: &[Vec<u8>],
) -> (Vec<AckedRecord>, u64) {
    let mut out = Vec::new();
    let mut max_end = 0u64;
    for p in payloads {
        let r = sc.append(stream_id, p).await.expect("append record");
        max_end = max_end.max(r.end);
        out.push(AckedRecord {
            extent_id: r.extent_id,
            offset: r.offset,
            bytes: p.clone(),
        });
    }
    (out, max_end)
}

/// SMALL `max_extent_size` used by the writers so a natural preemptive roll
/// (the client's own SealCommit-driven seal+roll) fires after a few records —
/// giving us a genuinely SEALED extent (E1) plus a live OPEN tail (E2), both
/// holding acked data. This is the well-trodden path (as opposed to a manual
/// manager-side seal, which does not move the writer's cached tail).
const SMALL_MAX_EXTENT: u64 = 250;

/// Fill one extent until the writer preemptively rolls (E1 sealed), then land
/// exactly two records on the fresh OPEN tail (E2). Returns
/// `(sealed_records_on_E1, open_tail_records_on_E2)`. With `SMALL_MAX_EXTENT`
/// and ~91-byte records the roll fires on the third record.
async fn build_sealed_plus_open_tail(
    sc: &StreamClient,
    stream_id: u64,
) -> (Vec<AckedRecord>, Vec<AckedRecord>) {
    let mut sealed = Vec::new();
    let mut open = Vec::new();
    let mut i = 0usize;

    let mk = |i: usize| AckedRecord {
        extent_id: 0,
        offset: 0,
        bytes: pad("REC", i),
    };

    // First record → establishes the initial (soon-to-be-sealed) extent.
    let first = mk(i);
    i += 1;
    let r = sc.append(stream_id, &first.bytes).await.expect("append r0");
    let first_ext = r.extent_id;
    sealed.push(AckedRecord {
        extent_id: r.extent_id,
        offset: r.offset,
        bytes: first.bytes,
    });

    // Keep appending until the extent id FLIPS — that record is the first on
    // the rolled-fresh OPEN tail. The crossing record (which triggered the
    // roll) stays in `sealed` (it was acked on E1 before the roll).
    loop {
        let rec = mk(i);
        i += 1;
        let r = sc.append(stream_id, &rec.bytes).await.expect("append fill");
        let ar = AckedRecord {
            extent_id: r.extent_id,
            offset: r.offset,
            bytes: rec.bytes,
        };
        if r.extent_id != first_ext {
            open.push(ar);
            break;
        }
        sealed.push(ar);
        assert!(i < 64, "roll never fired — SMALL_MAX_EXTENT too large?");
    }

    // One more record on the open tail (offset > 0), still well under the
    // roll threshold so the tail stays OPEN.
    let rec = mk(i);
    let r = sc.append(stream_id, &rec.bytes).await.expect("append tail#2");
    assert_eq!(
        r.extent_id, open[0].extent_id,
        "second open-tail record must not have rolled"
    );
    open.push(AckedRecord {
        extent_id: r.extent_id,
        offset: r.offset,
        bytes: rec.bytes,
    });

    (sealed, open)
}

/// Read each acked record straight from its extent and assert byte-identity.
/// Reads rotate + failover across replicas, so a wiped replica answering
/// NotFound is transparently routed around.
async fn assert_records_readable(sc: &StreamClient, recs: &[AckedRecord], ctx: &str) {
    for (i, rec) in recs.iter().enumerate() {
        sc.invalidate_extent_cache(rec.extent_id);
        let (data, _) = sc
            .read_bytes_from_extent(rec.extent_id, rec.offset, rec.bytes.len() as u64)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "{ctx}: acked record #{i} (extent={} off={}) UNREADABLE = SILENT LOSS: {e}",
                    rec.extent_id, rec.offset
                )
            });
        assert_eq!(
            data, rec.bytes,
            "{ctx}: acked record #{i} (extent={} off={}) bytes differ = SILENT CORRUPTION",
            rec.extent_id, rec.offset
        );
    }
}

fn pad(tag: &str, i: usize) -> Vec<u8> {
    // ~96-byte records so each append is a distinct, verifiable range.
    format!("{tag}-record-{i:04}-{}", "x".repeat(72)).into_bytes()
}

/// Manual seal of a stream's current tail at `commit`, rolling a fresh tail.
async fn seal_tail(
    mgr: &RpcClient,
    sc: &StreamClient,
    stream_id: u64,
    seal_extent_id: u64,
    commit: u64,
) {
    let bytes = mgr
        .call(
            MSG_STREAM_ALLOC_EXTENT,
            rkyv_encode(&StreamAllocExtentReq {
                stream_id,
                owner_key: sc.owner_key().to_string(),
                owner_epoch: sc.owner_epoch(),
                seal_commit: Some(commit),
                exclude_node_ids: vec![],
                seal_extent_id,
            }),
        )
        .await
        .expect("seal RPC");
    let r: StreamAllocExtentResp = rkyv_decode(&bytes).expect("decode seal");
    assert_eq!(r.code, CODE_OK, "seal failed: {}", r.message);
}

// ════════════════════════════════════════════════════════════════════════
// TEST 1 — the G8 reproduction: wiped rejoin under same identity, NOT fenced.
// ════════════════════════════════════════════════════════════════════════
#[test]
fn wiped_rejoin_under_same_identity_does_not_truncate_acked_data() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    // n1/n2 stay up; n3 is the victim. Identity is carried by node_uuid
    // "g8-n3" so the rejoin (a fresh EN on a NEW port + re-register with the
    // same uuid) keeps the SAME node_id — the "same identity, new location"
    // rejoin.
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n3_dir = tempfile::tempdir().expect("n3");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n3_addr = pick_addr();

    start_en_with_mgr(n1_addr, n1_dir.path().to_path_buf(), 1, mgr_addr);
    start_en_with_mgr(n2_addr, n2_dir.path().to_path_buf(), 2, mgr_addr);
    let (n3_flag, mut n3_handle) =
        start_en_with_mgr_stoppable(n3_addr, n3_dir.path().to_path_buf(), 3, mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        let _r1 = register_node(&mgr, &n1_addr.to_string(), "uuid-1").await;
        let _r2 = register_node(&mgr, &n2_addr.to_string(), "uuid-2").await;
        let r3 = register_node_uuid(&mgr, &n3_addr.to_string(), "uuid-3", "g8-n3").await;
        let n3_id = r3.node_id;

        // RF3 stream across all three nodes.
        let stream_id = create_stream(&mgr, 3).await;

        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "G8-writer-A".to_string(),
            SMALL_MAX_EXTENT, // natural preemptive roll → real sealed E1 + open E2
            pool.clone(),
        )
        .await
        .expect("connect sc");

        // ── Phase A: acked data in a SEALED extent (E1) + the OPEN tail (E2) ──
        let (sealed_recs, open_recs) = build_sealed_plus_open_tail(&sc, stream_id).await;
        let e1 = sealed_recs[0].extent_id;
        let e2 = open_recs[0].extent_id;
        let end2 = open_recs.last().unwrap().offset + open_recs.last().unwrap().bytes.len() as u64;
        assert_ne!(e2, e1, "open tail must be a distinct rolled-fresh extent");
        assert!(
            open_recs.iter().all(|r| r.extent_id == e2),
            "all open-tail records share E2"
        );
        assert!(end2 > 0);

        // Confirm n3 is a committed member of BOTH the sealed extent and the
        // open tail (the wipe must actually target held data).
        sc.invalidate_extent_cache(e1);
        sc.invalidate_extent_cache(e2);
        let e1_info = sc.get_extent_info(e1).await.expect("e1 info");
        let e2_info = sc.get_extent_info(e2).await.expect("e2 info");
        assert!(e1_info.sealed, "E1 should be sealed");
        assert!(!e2_info.sealed, "E2 should be the open tail");
        assert!(
            e1_info.replicates.contains(&n3_id) && e2_info.replicates.contains(&n3_id),
            "n3 (id={n3_id}) must hold E1+E2 before the wipe (e1={:?} e2={:?})",
            e1_info.replicates,
            e2_info.replicates
        );
        let e1_replicas_before = e1_info.replicates.clone();

        // Baseline: everything readable + commit_length == end2 pre-wipe.
        assert_records_readable(&sc, &sealed_recs, "pre-wipe sealed").await;
        assert_records_readable(&sc, &open_recs, "pre-wipe open-tail").await;
        let pre = check_commit_length(&mgr, stream_id, sc.owner_key(), sc.owner_epoch()).await;
        assert_eq!(pre.code, CODE_OK, "pre-wipe ccl: {}", pre.message);
        assert_eq!(pre.end, end2, "pre-wipe commit_length must equal the open-tail acked end");

        // ── Phase B: "SIGKILL" n3 (drop its runtime → conns refused), wipe
        //    its data dir, restart a FRESH empty EN and re-register it under
        //    the SAME node_uuid → same node_id, new location, holding NOTHING ──
        n3_flag.shutdown();
        n3_handle.join().expect("join dead n3");
        wipe_data_dir(n3_dir.path());
        let n3_addr_v2 = pick_addr();
        let (n3_flag2, n3_handle2) =
            start_en_with_mgr_stoppable(n3_addr_v2, n3_dir.path().to_path_buf(), 3, mgr_addr);
        n3_handle = n3_handle2; // keep the restarted handle alive for the test body
        // Re-register the SAME identity at the new location (uuid-match → the
        // manager updates n3's address in place, node_id unchanged).
        let r3b = register_node_uuid(&mgr, &n3_addr_v2.to_string(), "uuid-3", "g8-n3").await;
        assert_eq!(
            r3b.node_id, n3_id,
            "rejoin under the same node_uuid must keep the SAME node_id (identity preserved)"
        );

        // The rejoined node comes back Online under its old identity but now
        // holds NOTHING (answers NotFound for every extent it used to hold).
        assert!(
            wait_node_online(&mgr, &n3_addr_v2.to_string(), Duration::from_secs(20)).await,
            "wiped n3 must rejoin Online (df-healthy) under its old identity"
        );

        // ── Phase C: THE ANOMALY PROBE — did the empty replica lower any
        //             commit_length / truncate acked data? ──

        // (1) Manager authoritative commit-length on the OPEN tail E2 (still
        //     current). n3 answers NotFound → excluded from the min →
        //     min over {n1,n2} == end2. A folded-in 0 would surface here.
        let post = check_commit_length(&mgr, stream_id, sc.owner_key(), sc.owner_epoch()).await;
        assert_eq!(post.code, CODE_OK, "post-wipe ccl code: {}", post.message);
        assert_ne!(
            post.end, 0,
            "REPRODUCED SILENT TRUNCATION: commit_length collapsed to 0 after the empty replica rejoined"
        );
        assert_eq!(
            post.end, end2,
            "REPRODUCED SILENT TRUNCATION: commit_length lowered by the wiped replica ({} < acked {end2})",
            post.end
        );

        // (2) Every acked record still byte-identical readable (reads fail over
        //     past the wiped n3).
        assert_records_readable(&sc, &sealed_recs, "post-wipe sealed").await;
        assert_records_readable(&sc, &open_recs, "post-wipe open-tail").await;

        // ── Phase D: write-liveness — a FRESH writer re-initialises the tail
        //             (the `ensure_tail_initialised` / `current_commit` path
        //             the CLAUDE.md warns about), then keeps writing. ──
        let sc2 = StreamClient::connect(
            &mgr_addr.to_string(),
            "G8-writer-B".to_string(),
            16 * 1024 * 1024,
            pool.clone(),
        )
        .await
        .expect("connect sc2");

        let batch3: Vec<Vec<u8>> = (0..4).map(|i| pad("POSTREJOIN", i)).collect();
        // First append triggers ensure_tail_initialised → current_commit(E2)
        // (n3 NotFound → Err → BUG#1 seal-over-reachable-and-roll to a fresh
        // tail on healthy nodes), then the batch lands. No wedge expected.
        let (post_recs, end3) = append_records(&sc2, stream_id, &batch3).await;
        assert!(end3 > 0, "post-rejoin writes must make progress (no wedge)");
        assert_records_readable(&sc2, &post_recs, "post-rejoin new writes").await;

        // E2 must have been sealed at >= its acked length by the lenient
        // seal-over-reachable (NEVER truncated below acked, NEVER 0).
        sc2.invalidate_extent_cache(e2);
        let e2_after = sc2.get_extent_info(e2).await.expect("e2 info after");
        assert!(e2_after.sealed, "E2 sealed by the roll-away");
        assert!(
            e2_after.sealed_length >= end2,
            "REPRODUCED SILENT TRUNCATION: E2 sealed at {} < acked {end2}",
            e2_after.sealed_length
        );

        // And the sealed extent's acked data is STILL intact after all of it.
        assert_records_readable(&sc2, &sealed_recs, "final sealed").await;
        assert_records_readable(&sc2, &open_recs, "final open-tail (now sealed)").await;

        // ── Phase E: recovery observation under the DEFAULT gate ──
        // Under `fenced_only` (default), a wiped-but-rejoined-NOT-fenced node
        // is NOT auto-refilled: recovery only fires for Fenced/disk-offline/
        // avali-unset slots, and the manager still believes n3's avali bit is
        // set. So E1's membership still lists n3 even though n3 holds nothing.
        // Redundancy is silently degraded (effectively RF2) until an operator
        // fences n3 or the auto_disk gate is enabled — but NO acked data is at
        // risk (the emptiness is excluded from every min).
        compio::time::sleep(Duration::from_secs(5)).await; // > 2 recovery ticks
        sc2.invalidate_extent_cache(e1);
        let e1_after = sc2.get_extent_info(e1).await.expect("e1 info after");
        assert!(
            e1_after.replicates.contains(&n3_id),
            "under the default fenced_only gate, the not-fenced wiped node is NOT auto-reconfigured out (observation, not a bug): before={e1_replicas_before:?} after={:?}",
            e1_after.replicates
        );

        // clean shutdown of the restarted victim.
        n3_flag2.shutdown();
        let _ = n3_handle;
    });
}

// ════════════════════════════════════════════════════════════════════════
// TEST 2 — the affirmative recovery path: FENCING the wiped node makes the
// manager rebuild its lost SEALED extent onto a spare, and the recovered
// replica serves the acked bytes.
// ════════════════════════════════════════════════════════════════════════
#[test]
fn fencing_a_wiped_rejoined_node_triggers_recovery_refill() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    // 3 members (n1,n2,n3) built FIRST; the spare (n4) is registered only
    // AFTER the stream exists, so E1's replica set is deterministically
    // {n1,n2,n3} and n4 is a genuine recovery target.
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n3_dir = tempfile::tempdir().expect("n3");
    let n4_dir = tempfile::tempdir().expect("n4");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n3_addr = pick_addr();
    let n4_addr = pick_addr();

    start_en_with_mgr(n1_addr, n1_dir.path().to_path_buf(), 1, mgr_addr);
    start_en_with_mgr(n2_addr, n2_dir.path().to_path_buf(), 2, mgr_addr);
    let (n3_flag, n3_handle) =
        start_en_with_mgr_stoppable(n3_addr, n3_dir.path().to_path_buf(), 3, mgr_addr);
    start_en_with_mgr(n4_addr, n4_dir.path().to_path_buf(), 4, mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        let _r1 = register_node(&mgr, &n1_addr.to_string(), "uuid-1").await;
        let _r2 = register_node(&mgr, &n2_addr.to_string(), "uuid-2").await;
        let r3 = register_node_uuid(&mgr, &n3_addr.to_string(), "uuid-3", "g8-n3").await;
        let n3_id = r3.node_id;

        // Only 3 nodes registered → create_stream(3) deterministically selects
        // n1,n2,n3.
        let stream_id = create_stream(&mgr, 3).await;
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "G8-recovery-writer".to_string(),
            16 * 1024 * 1024,
            pool.clone(),
        )
        .await
        .expect("connect sc");

        // Acked data in a SEALED extent E1 (recovery only rebuilds sealed).
        let batch: Vec<Vec<u8>> = (0..4).map(|i| pad("REC", i)).collect();
        let (recs, end1) = append_records(&sc, stream_id, &batch).await;
        let e1 = recs[0].extent_id;
        seal_tail(&mgr, &sc, stream_id, e1, end1).await;

        sc.invalidate_extent_cache(e1);
        let e1_info = sc.get_extent_info(e1).await.expect("e1 info");
        assert!(
            e1_info.sealed && e1_info.replicates.contains(&n3_id),
            "E1 must be sealed and hold n3 (replicates={:?})",
            e1_info.replicates
        );

        // Register the spare n4 NOW (after the stream is placed) and wait for
        // it Online so recovery has a genuine target.
        let r4 = register_node(&mgr, &n4_addr.to_string(), "uuid-4").await;
        let n4_id = r4.node_id;
        assert!(
            !e1_info.replicates.contains(&n4_id),
            "n4 must be a genuine spare (not in E1)"
        );
        assert!(
            wait_node_online(&mgr, &n4_addr.to_string(), Duration::from_secs(20)).await,
            "spare n4 online"
        );

        // "SIGKILL" + wipe + rejoin n3 under the SAME node_uuid at a NEW
        // location (same node_id, holding nothing).
        n3_flag.shutdown();
        n3_handle.join().expect("join n3");
        wipe_data_dir(n3_dir.path());
        let n3_addr_v2 = pick_addr();
        let (n3_flag2, _n3_handle2) =
            start_en_with_mgr_stoppable(n3_addr_v2, n3_dir.path().to_path_buf(), 3, mgr_addr);
        let r3b = register_node_uuid(&mgr, &n3_addr_v2.to_string(), "uuid-3", "g8-n3").await;
        assert_eq!(r3b.node_id, n3_id, "rejoin keeps the same node_id");
        assert!(
            wait_node_online(&mgr, &n3_addr_v2.to_string(), Duration::from_secs(20)).await,
            "wiped n3 rejoins Online"
        );

        // FENCE n3 (operator action) → recovery dispatches immediately for
        // every slot it holds, regardless of probe outcome.
        let fbytes = mgr
            .call(
                MSG_FENCE_NODE,
                rkyv_encode(&FenceNodeReq {
                    node_id: n3_id,
                    reason: "G8 wiped-disk decommission".to_string(),
                    set_by: "test".to_string(),
                    force: true,
                }),
            )
            .await
            .expect("fence RPC");
        let fr: CodeResp = rkyv_decode(&fbytes).expect("decode fence CodeResp");
        assert_eq!(fr.code, CODE_OK, "fence n3 failed: {}", fr.message);

        // Recovery should reconfigure E1: swap the n3 slot onto the spare n4.
        let refilled = poll_until_async(Duration::from_secs(40), Duration::from_millis(500), || {
            let sc = &sc;
            async move {
                sc.invalidate_extent_cache(e1);
                match sc.get_extent_info(e1).await {
                    Ok(info) => info.replicates.contains(&n4_id) && !info.replicates.contains(&n3_id),
                    Err(_) => false,
                }
            }
        })
        .await;
        assert!(
            refilled,
            "fencing the wiped node must dispatch recovery that rebuilds E1 onto the spare n4"
        );

        // The recovered replica set serves the acked bytes byte-identically.
        assert_records_readable(&sc, &recs, "after recovery refill").await;

        n3_flag2.shutdown();
    });
}
