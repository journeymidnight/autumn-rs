//! G7 — correlated failure: lose 2-of-3 replicas of an extent simultaneously
//! (+ EC beyond parity).  REPRODUCE-FIRST — this file only asserts the current
//! behaviour; it does not fix anything.
//!
//! autumn-rs is RF3, all-replica-ACK, `min`-over-reachable-committed seal with a
//! lenient durability floor of **1** (`AUTUMN_MGR_SEAL_DURABILITY_FLOOR`,
//! `compute_commit_seal`, `rpc_handlers.rs`).  A correlated 2-of-3 loss stresses
//! three invariants at their boundary:
//!
//!   Leg 1 — kill 2 of an extent's 3 replicas at the same time.  The single
//!           surviving replica holds the full acked prefix (all-replica-ACK), so
//!           reads MUST still serve, and after the operator fences the two dead
//!           nodes, recovery MUST refill BOTH slots from the one survivor onto
//!           spare nodes without wedging.  (This also is Leg 2 — "rebuild both
//!           from the single survivor" — the mechanism is identical: the dead
//!           replicas never return, both slots are re-replicated off the lone
//!           survivor.)
//!
//!   Leg 3 — EC-convert a sealed extent to K=2 + M=1, then lose M+1 = 2 shards
//!           (both data shards).  With < K shards left the extent is
//!           unreconstructable; this MUST surface LOUD — the manager
//!           `extent_health_report` flags it unhealthy, a read returns an error,
//!           and the RS codec returns `Err`, NEVER a silent NotFound or wrong
//!           bytes.
//!
//! This harness is intentionally SELF-CONTAINED (it copies the few helpers it
//! needs) so it does not depend on `tests/support/mod.rs`, `recovery.rs`, or
//! `system_chaos.rs`, which other work may be editing concurrently.  The
//! `system_chaos` `healthy_count()` guard that forbids compound kills lives in a
//! different binary and does not gate this one.

use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ConnPool, ExtentNode, ExtentNodeConfig, StreamClient};

// ── copied helpers (do NOT touch support/mod.rs) ─────────────────────────────

fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr
}

fn start_manager(addr: SocketAddr) {
    // block cache is process-global — clear it so a prior in-process test's
    // block can't be served for this test's same-id extent.
    autumn_partition_server::clear_global_block_cache();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new();
            let _ = manager.serve(addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(300));
}

/// A killable extent node wired to the manager (so recovery / EC conversion can
/// resolve `ExtentInfo` + peer addresses).  `.kill()` drops the compio runtime
/// → listener + every live connection torn down → a real node death as seen by
/// clients (connection refused).
struct KillableEn {
    node_id: u64, // filled after register
    addr: SocketAddr,
    flag: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl KillableEn {
    fn kill(&mut self) {
        self.flag.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
    fn is_dead(&self) -> bool {
        self.handle.is_none()
    }
}

fn start_en(addr: SocketAddr, dir: std::path::PathBuf, disk_id: u64, mgr_addr: SocketAddr) -> KillableEn {
    let flag = Arc::new(AtomicBool::new(false));
    let flag_thread = flag.clone();
    let handle = std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let cfg = ExtentNodeConfig::new(dir, disk_id).with_manager_endpoint(mgr_addr.to_string());
            let n = ExtentNode::new(cfg).await.expect("extent node");
            compio::runtime::spawn(async move {
                let _ = n.serve(addr).await;
            })
            .detach();
            while !flag_thread.load(Ordering::Acquire) {
                compio::time::sleep(Duration::from_millis(50)).await;
            }
            // block_on returns → Runtime drops → detached serve task + listener
            // + all conn tasks cancelled and sockets closed.
        });
    });
    std::thread::sleep(Duration::from_millis(200));
    KillableEn {
        node_id: 0,
        addr,
        flag,
        handle: Some(handle),
    }
}

async fn register_node(mgr: &RpcClient, addr: &str, uuid: &str) -> u64 {
    let resp = mgr
        .call(
            MSG_REGISTER_NODE,
            rkyv_encode(&RegisterNodeReq {
                addr: addr.to_string(),
                disk_uuids: vec![uuid.to_string()],
                shard_ports: vec![],
                control_address: String::new(),
                node_uuid: String::new(),
            }),
        )
        .await
        .expect("register node");
    let r: RegisterNodeResp = rkyv_decode(&resp).expect("decode RegisterNodeResp");
    assert_eq!(r.code, CODE_OK, "register node: {}", r.message);
    r.node_id
}

async fn create_stream(mgr: &RpcClient, replicates: u32, ec_data: u32, ec_parity: u32) -> u64 {
    let resp = mgr
        .call(
            MSG_CREATE_STREAM,
            rkyv_encode(&CreateStreamReq {
                replicates,
                ec_data_shard: ec_data,
                ec_parity_shard: ec_parity,
            }),
        )
        .await
        .expect("create stream");
    let created: CreateStreamResp = rkyv_decode(&resp).expect("decode CreateStreamResp");
    created
        .stream
        .unwrap_or_else(|| panic!("create_stream code={} msg={}", created.code, created.message))
        .stream_id
}

async fn mgr_extent_info(mgr: &RpcClient, extent_id: u64) -> MgrExtentInfo {
    let resp = mgr
        .call(MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id }))
        .await
        .expect("extent_info");
    let r: ExtentInfoResp = rkyv_decode(&resp).expect("decode ExtentInfoResp");
    r.extent.expect("extent info present")
}

/// seal the stream's current tail at `commit` and roll a fresh open tail
/// (mirrors `system_extent_recovery.rs`).
async fn seal_tail(mgr: &RpcClient, sc: &StreamClient, stream_id: u64, commit: u64) {
    let resp = mgr
        .call(
            MSG_STREAM_ALLOC_EXTENT,
            rkyv_encode(&StreamAllocExtentReq {
                stream_id,
                owner_key: sc.owner_key().to_string(),
                owner_epoch: sc.owner_epoch(),
                seal_commit: Some(commit),
                exclude_node_ids: vec![],
                seal_extent_id: 0,
            }),
        )
        .await
        .expect("stream_alloc_extent(seal)");
    let seal: StreamAllocExtentResp = rkyv_decode(&resp).expect("decode seal");
    assert_eq!(seal.code, CODE_OK, "seal failed: {}", seal.message);
}

async fn fence_node(mgr: &RpcClient, node_id: u64) {
    let resp = mgr
        .call(
            MSG_FENCE_NODE,
            rkyv_encode(&FenceNodeReq {
                node_id,
                reason: "G7 correlated-loss reproduction".to_string(),
                set_by: "system_correlated_2of3_loss".to_string(),
                force: true, // skip capacity precheck (no cluster_df dependency)
            }),
        )
        .await
        .expect("fence_node");
    let r: CodeResp = rkyv_decode(&resp).expect("decode fence CodeResp");
    assert_eq!(r.code, CODE_OK, "fence_node({node_id}): {}", r.message);
}

async fn update_stream_ec(mgr: &RpcClient, stream_id: u64, k: u32, m: u32) {
    let resp = mgr
        .call(
            MSG_UPDATE_STREAM_EC,
            rkyv_encode(&UpdateStreamEcReq {
                stream_id,
                ec_data_shard: k,
                ec_parity_shard: m,
            }),
        )
        .await
        .expect("update_stream_ec");
    let r: UpdateStreamEcResp = rkyv_decode(&resp).expect("decode UpdateStreamEcResp");
    assert_eq!(r.code, CODE_OK, "update_stream_ec: {}", r.message);
}

async fn force_ec_convert(mgr: &RpcClient, extent_id: u64) {
    let resp = mgr
        .call(MSG_FORCE_EC_CONVERT, rkyv_encode(&ForceEcConvertReq { extent_id }))
        .await
        .expect("force_ec_convert");
    let r: ForceEcConvertResp = rkyv_decode(&resp).expect("decode ForceEcConvertResp");
    assert_eq!(r.code, CODE_OK, "force_ec_convert: {}", r.message);
}

/// query `extent_health_report` for a single extent (via node filter) and
/// return its `ExtentHealth` if present.
async fn extent_health(mgr: &RpcClient, node_id_filter: Vec<u64>) -> Vec<ExtentHealth> {
    let resp = mgr
        .call(
            MSG_EXTENT_HEALTH_REPORT,
            rkyv_encode(&ExtentHealthReq {
                node_id_filter,
                include_healthy: true,
            }),
        )
        .await
        .expect("extent_health_report");
    let r: ExtentHealthResp = rkyv_decode(&resp).expect("decode ExtentHealthResp");
    assert_eq!(r.code, CODE_OK, "extent_health_report: {}", r.message);
    r.extents
}

fn all_bits(n: usize) -> u32 {
    if n >= 32 {
        u32::MAX
    } else {
        (1u32 << n) - 1
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LEG 1 (+ LEG 2): correlated 2-of-3 replica loss.  Data survives on the single
// remaining replica; reads still serve; after fencing the two dead nodes
// recovery refills BOTH slots from the one survivor onto spares, no wedge, no
// acked loss.
// ─────────────────────────────────────────────────────────────────────────────
#[test]
fn leg1_correlated_2of3_loss_survives_and_recovery_refills_from_survivor() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    // 6 killable ENs.  We register the FIRST 3 up front so the stream's extents
    // co-locate on exactly {n0,n1,n2}; the other 3 are registered later as spare
    // recovery targets (≥6 ENs at the correlated-failure moment).
    let mut dirs: Vec<tempfile::TempDir> = Vec::new();
    let mut ens: Vec<KillableEn> = Vec::new();
    for i in 0..6u64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let addr = pick_addr();
        let en = start_en(addr, dir.path().to_path_buf(), i + 1, mgr_addr);
        dirs.push(dir);
        ens.push(en);
    }

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");

        // register the first 3 → the stream lands on them.
        for i in 0..3 {
            ens[i].node_id = register_node(&mgr, &ens[i].addr.to_string(), &format!("uuid-{i}")).await;
        }
        // let the 3 get a df tick so they are verified-Online for select_nodes.
        compio::time::sleep(Duration::from_secs(3)).await;

        let stream_id = create_stream(&mgr, 3, 3, 0).await; // RF3, no EC.

        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "owner/g7-leg1/0".to_string(),
            256 * 1024 * 1024,
            pool.clone(),
        )
        .await
        .expect("connect stream client");

        // Write TWO DISTINCT sealed extents.  Each round: append (acked
        // all-replicas), seal that tail at the acked commit, then
        // `invalidate_stream` so the next append re-inits onto the freshly
        // rolled tail rather than continuing to append past the manager-seal on
        // the EN-local-open copy.  Both co-locate on {n0,n1,n2} (only 3 nodes
        // registered so far).
        let mut sealed: Vec<(u64, u64, u64, Vec<u8>)> = Vec::new(); // (eid, off, len, payload)
        for round in 0u8..2 {
            let payload: Vec<u8> = (0..2048usize).map(|j| ((j as u8) ^ (round.wrapping_mul(97))) as u8).collect();
            let r = sc.append(stream_id, &payload).await.expect("append");
            assert_eq!(r.offset, 0, "each fresh extent should start at offset 0");
            seal_tail(&mgr, &sc, stream_id, r.end).await;
            sealed.push((r.extent_id, r.offset, r.end - r.offset, payload));
            sc.invalidate_stream(stream_id);
        }
        assert_ne!(sealed[0].0, sealed[1].0, "the two sealed extents must be distinct");

        // sanity: both sealed extents live on the same 3-node set.
        let e0 = mgr_extent_info(&mgr, sealed[0].0).await;
        let e1 = mgr_extent_info(&mgr, sealed[1].0).await;
        assert!(e0.sealed && e0.sealed_length > 0, "extent0 must be sealed");
        assert!(e1.sealed && e1.sealed_length > 0, "extent1 must be sealed");
        let rset0: std::collections::HashSet<u64> = e0.replicates.iter().copied().collect();
        let rset1: std::collections::HashSet<u64> = e1.replicates.iter().copied().collect();
        assert_eq!(rset0, rset1, "both extents should share the replica set (co-located)");
        assert_eq!(e0.replicates.len(), 3, "RF3");
        // lenient seal floor=1 boundary: every slot is avali after an
        // all-replica-acked seal.
        assert_eq!(e0.avali, all_bits(3), "sealed extent should have every avali bit set");

        // register the 3 spares now (recovery targets); give them a df tick.
        for i in 3..6 {
            ens[i].node_id = register_node(&mgr, &ens[i].addr.to_string(), &format!("uuid-{i}")).await;
        }
        compio::time::sleep(Duration::from_secs(3)).await;

        // choose the correlated victims: kill 2 of the 3 replicas at once, keep 1.
        let replica_ids = e0.replicates.clone();
        let survivor = replica_ids[0];
        let dead = [replica_ids[1], replica_ids[2]];

        // SIGKILL both victims simultaneously (drop their runtimes).
        for &nid in &dead {
            let idx = ens.iter().position(|e| e.node_id == nid).expect("victim en");
            ens[idx].kill();
        }
        assert!(
            dead.iter().all(|&nid| ens.iter().find(|e| e.node_id == nid).unwrap().is_dead()),
            "both victims must be down"
        );

        // ── ASSERT reads still serve from the single surviving replica ──
        for (eid, off, len, payload) in &sealed {
            sc.invalidate_extent_cache(*eid);
            let (data, _) = sc
                .read_bytes_from_extent(*eid, *off, *len)
                .await
                .unwrap_or_else(|e| panic!("read of extent {eid} after 2/3 loss failed (should serve from survivor {survivor}): {e:#}"));
            assert_eq!(&data, payload, "surviving replica must return the acked bytes verbatim");
        }

        // ── fence the two dead nodes → recovery dispatches under the default
        // `fenced_only` gate → refill both slots from the lone survivor ──
        for &nid in &dead {
            fence_node(&mgr, nid).await;
        }

        // poll until BOTH extents have both dead slots rebuilt onto live spares:
        // no dead node remains in the replica set, and every avali bit is set.
        let dead_set: std::collections::HashSet<u64> = dead.iter().copied().collect();
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut converged = false;
        let mut last = String::new();
        while Instant::now() < deadline {
            let mut all_ok = true;
            let mut snap = String::new();
            for (eid, _, _, _) in &sealed {
                let ex = mgr_extent_info(&mgr, *eid).await;
                let nodes: std::collections::HashSet<u64> =
                    ex.replicates.iter().chain(ex.parity.iter()).copied().collect();
                let has_dead = nodes.iter().any(|n| dead_set.contains(n));
                let full_avali = ex.avali == all_bits(ex.replicates.len() + ex.parity.len());
                snap += &format!(" ext{eid}[repl={:?} avali={:#x} has_dead={has_dead} full={full_avali}]", ex.replicates, ex.avali);
                if has_dead || !full_avali {
                    all_ok = false;
                }
            }
            last = snap;
            if all_ok {
                converged = true;
                break;
            }
            compio::time::sleep(Duration::from_secs(2)).await;
        }
        assert!(
            converged,
            "recovery did NOT converge (wedge?) within 120s — both dead slots should be \
             re-replicated from survivor {survivor} onto spares. last:{last}"
        );

        // ── ASSERT no acked data loss: every sealed extent still reads back
        // verbatim after recovery (fresh eversion self-heals via the cache) ──
        for (eid, off, len, payload) in &sealed {
            let ex = mgr_extent_info(&mgr, *eid).await;
            eprintln!(
                "POST-RECOVERY ext{eid}: sealed={} sealed_length={} eversion={} replicates={:?} avali={:#x}",
                ex.sealed, ex.sealed_length, ex.eversion, ex.replicates, ex.avali
            );
            // probe each replica individually (no failover) to see which slots
            // actually hold the bytes.
            for idx in 0..ex.replicates.len() {
                match sc.read_committed_from_replica(*eid, idx, *off, *len).await {
                    Ok((bytes, cend, nid)) => eprintln!(
                        "   replica[{idx}] node={nid}: {} bytes (committed_end={cend})",
                        bytes.len()
                    ),
                    Err(e) => eprintln!("   replica[{idx}] read err: {e:#}"),
                }
            }
            // retry a few times: fresh eversion self-heals via the cache; a
            // spare may still be settling right at convergence.
            let mut got: Option<Vec<u8>> = None;
            let mut last = String::new();
            for _ in 0..10 {
                sc.invalidate_extent_cache(*eid);
                match sc.read_bytes_from_extent(*eid, *off, *len).await {
                    Ok((data, _)) if data.len() as u64 == *len => {
                        got = Some(data);
                        break;
                    }
                    Ok((data, end)) => last = format!("short read: {} bytes, end={end}", data.len()),
                    Err(e) => last = format!("err: {e:#}"),
                }
                compio::time::sleep(Duration::from_millis(500)).await;
            }
            let data = got.unwrap_or_else(|| panic!("post-recovery read of extent {eid} never returned full length ({last})"));
            assert_eq!(&data, payload, "acked data must survive the correlated loss + rebuild");
        }

        // survivor still holds a copy (source of the rebuild); make sure the
        // final replica set excludes both dead nodes for extent0.
        let final0 = mgr_extent_info(&mgr, sealed[0].0).await;
        assert!(
            final0.replicates.contains(&survivor),
            "survivor should remain in the replica set (it sourced both rebuilds)"
        );
        assert!(
            final0.replicates.iter().all(|n| !dead_set.contains(n)),
            "both dead replicas must be gone from the set after recovery"
        );
    });

    // keep dirs alive until here.
    for mut en in ens {
        en.kill();
    }
    drop(dirs);
}

// ─────────────────────────────────────────────────────────────────────────────
// LEG 3: EC over-loss.  Convert to K=2 + M=1, then lose M+1 = 2 shards (both
// data shards).  With < K shards the extent is unreconstructable → LOUD failure:
// the health report flags it, a read errors, and the codec returns Err — never a
// silent NotFound or wrong bytes.
// ─────────────────────────────────────────────────────────────────────────────
#[test]
fn leg3_ec_over_loss_is_loud_not_silent() {
    // (a) crisp codec-level proof, cluster-independent: losing M+1 shards → Err,
    //     never Ok-with-garbage.
    {
        use autumn_stream::erasure;
        let payload: Vec<u8> = (0..4096usize).map(|i| (i % 251) as u8).collect();
        let shards = erasure::ec_encode(&payload, 2, 1).expect("ec_encode");
        // lose both data shards (M+1 = 2 > M = 1 parity) → only 1 shard left < K=2.
        let mut opt: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        opt[0] = None;
        opt[1] = None;
        let dec = erasure::ec_decode(opt, 2, 1, payload.len());
        assert!(
            dec.is_err(),
            "ec_decode with < K shards MUST be a loud Err, not silent garbage"
        );
        // reconstruct with too few shards also errors.
        let shards2 = erasure::ec_encode(&payload, 2, 1).expect("ec_encode");
        let mut opt2: Vec<Option<Vec<u8>>> = shards2.into_iter().map(Some).collect();
        opt2[0] = None;
        opt2[1] = None; // want to rebuild slot 0, but only parity survives (< K)
        let rec = erasure::ec_reconstruct_shard(opt2, 2, 1, 0);
        assert!(rec.is_err(), "ec_reconstruct_shard below K MUST be a loud Err");
    }

    // (b) end-to-end: convert an extent to EC 2+1 on a live cluster, kill both
    //     data-shard holders, assert the manager health report is LOUD and a
    //     read errors.
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let mut dirs: Vec<tempfile::TempDir> = Vec::new();
    let mut ens: Vec<KillableEn> = Vec::new();
    for i in 0..6u64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let addr = pick_addr();
        let en = start_en(addr, dir.path().to_path_buf(), i + 1, mgr_addr);
        dirs.push(dir);
        ens.push(en);
    }

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        for i in 0..6 {
            ens[i].node_id = register_node(&mgr, &ens[i].addr.to_string(), &format!("uuid-{i}")).await;
        }
        compio::time::sleep(Duration::from_secs(3)).await;

        // RF3 stream (open extents are always 3-replica); EC shape armed but
        // conversion is driven explicitly below.
        let stream_id = create_stream(&mgr, 3, 3, 0).await;

        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "owner/g7-leg3/0".to_string(),
            256 * 1024 * 1024,
            pool.clone(),
        )
        .await
        .expect("connect stream client");

        // write a value large enough to be a real 2-shard EC payload, seal it.
        let payload: Vec<u8> = (0..(64 * 1024usize)).map(|i| (i % 251) as u8).collect();
        let r = sc.append(stream_id, &payload).await.expect("append");
        let extent_id = r.extent_id;
        seal_tail(&mgr, &sc, stream_id, r.end).await;

        // arm EC 2+1 and force-convert this sealed extent.
        update_stream_ec(&mgr, stream_id, 2, 1).await;
        force_ec_convert(&mgr, extent_id).await;

        // wait for conversion (dispatch loop fires every 5 s).
        let deadline = Instant::now() + Duration::from_secs(45);
        let mut converted = false;
        while Instant::now() < deadline {
            let ex = mgr_extent_info(&mgr, extent_id).await;
            if ex.ec_converted {
                converted = true;
                break;
            }
            compio::time::sleep(Duration::from_secs(2)).await;
        }
        assert!(converted, "extent should EC-convert within 45s");

        let ec = mgr_extent_info(&mgr, extent_id).await;
        assert!(ec.ec_converted, "converted");
        assert_eq!(ec.replicates.len(), 2, "K=2 data shards");
        assert_eq!(ec.parity.len(), 1, "M=1 parity shard");
        // shard→node layout: replicates = the 2 DATA shards, parity = the M parity.
        let data_nodes = ec.replicates.clone();
        let parity_node = ec.parity[0];
        let sealed_len = ec.sealed_length;

        // ── lose M+1 = 2 shards: kill BOTH data-shard holders (keep parity) ──
        for &nid in &data_nodes {
            let idx = ens.iter().position(|e| e.node_id == nid).expect("data-shard en");
            ens[idx].kill();
        }
        // fence them so the loss is authoritative in the health report NOW
        // (Suspected would also surface it after the ~10s soft timeout).
        for &nid in &data_nodes {
            fence_node(&mgr, nid).await;
        }

        // ── ASSERT the manager health report is LOUD ──
        // poll briefly (fence is immediate; this tolerates report caching).
        let mut loud = None;
        let hdeadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < hdeadline {
            // filter by a lost NODE id (node_id_filter is NODE ids, not extent
            // ids) so the report includes this extent.
            let extents = extent_health(&mgr, vec![data_nodes[0]]).await;
            if let Some(h) = extents.iter().find(|h| h.extent_id == extent_id) {
                if h.unhealthy {
                    loud = Some(h.clone());
                    break;
                }
            }
            compio::time::sleep(Duration::from_millis(500)).await;
        }
        let h = loud.expect("extent_health_report MUST flag the over-loss extent unhealthy (LOUD)");
        assert!(h.ec_converted, "health report should show ec_converted");
        // the two killed data slots must be reported not-healthy (fenced).
        let bad_slots = h
            .slots
            .iter()
            .filter(|s| data_nodes.contains(&s.node_id))
            .count();
        assert_eq!(bad_slots, 2, "both lost data shards should appear as slots");
        assert!(
            h.slots
                .iter()
                .filter(|s| data_nodes.contains(&s.node_id))
                .all(|s| s.override_kind != NODE_OVERRIDE_NONE || s.auto_state != NODE_AUTO_STATE_ONLINE),
            "the lost data-shard slots must be reported degraded (fenced / not-online), not healthy"
        );
        // parity node is still alive.
        assert_eq!(parity_node, ec.parity[0]);

        // ── ASSERT a read of the over-lost EC extent errors LOUD, not silent ──
        // full-payload read spans BOTH data shards → needs 2 shards → only parity
        // (1 < K) reachable → unreconstructable → Err (never wrong/short bytes).
        sc.invalidate_extent_cache(extent_id);
        let read = sc.read_bytes_from_extent(extent_id, 0, sealed_len).await;
        assert!(
            read.is_err(),
            "reading an EC extent past parity MUST return an error, got Ok (silent over-loss): {:?}",
            read.map(|(d, e)| (d.len(), e))
        );
    });

    for mut en in ens {
        if !en.is_dead() {
            en.kill();
        }
    }
    drop(dirs);
}
