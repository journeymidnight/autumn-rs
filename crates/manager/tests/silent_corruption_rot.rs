//! G12 reproduction (reproduce-FIRST, NO FIX) — "silent corruption of sealed
//! data + rot propagation through repair/EC".
//!
//! HYPOTHESIS (verified against code before this harness was written):
//!   Large values at rest have NO checksum. The RPC frame CRC covers only
//!   `header ++ ctrl_len ++ ctrl` and deliberately EXCLUDES the bulk value tail
//!   ("bulk value integrity is the transport's job" — `crates/rpc/src/frame.rs`,
//!   whose own `vectored_zc_request_round_trip` test flips a value byte and
//!   asserts the frame still decodes). The extent-node serves raw `pread` bytes
//!   (`extent_node.rs::file_pread` / `build_read_future`). The `.meta` sidecar
//!   carries a CRC32C, but it covers ONLY the 40 metadata bytes — never the
//!   `.dat` value region. There is NO scrubber anywhere under
//!   `crates/stream/src/`. The read-path start-replica rotation is a
//!   deterministic SplitMix64 over `(extent_id, offset)`
//!   (`client.rs::rotated_replica_start`), so a corrupt replica is CONSISTENTLY
//!   chosen for the affected reads. Recovery's verify-after-fetch checks
//!   fetched-length == `sealed_length` and that eversion did not advance — NOT
//!   content (`run_recovery_task` / `stream_extent_from_sources`), so recovery
//!   FAITHFULLY REPLICATES corruption. EC conversion's coordinator
//!   (`target_nodes[0] == ex.replicates[0]`) reads its local bytes and encodes
//!   parity with no verification (`handle_convert_to_ec`), making the corruption
//!   canonical.
//!
//! Net effect this harness demonstrates, as three independent runnable legs:
//!   (a) READ     — a single-replica bit-flip in a SEALED extent's value region
//!                  is served to clients with CODE_OK and NO detection; the
//!                  deterministic rotation routes a fraction of reads onto the
//!                  corrupt replica (byzantine-adjacent wrong bytes).
//!   (b) RECOVERY — rebuilding a lost replica from sources that include the
//!                  corrupt one copies the corruption verbatim; verify-after-
//!                  fetch (length/eversion) passes because neither changed.
//!   (c) EC       — converting the extent to erasure coding encodes the shards
//!                  (and parity) from the coordinator's corrupt bytes; the
//!                  cluster reports success and the reconstructed value is the
//!                  corrupt one — nothing detects it.
//!
//! This is a REPRODUCTION, not a fix. Today's EXPECTED outcome is that all
//! three legs succeed silently (no checksum, no scrub). Every assertion below
//! is written to FAIL if some layer we missed actually DOES detect the
//! corruption — in which case the harness would surface that instead.
//!
//! This file is intentionally self-contained (mirrors `update_stream_ec.rs`'s
//! standalone helpers) so it touches no shared harness module.

use std::io::{Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::extent_rpc;
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ConnPool, StreamClient};

// ── standalone helpers ────────────────────────────────────────────────────

fn pick_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let a = l.local_addr().expect("local_addr");
    drop(l);
    a
}

fn start_manager(addr: SocketAddr) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new();
            let _ = manager.serve(addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));
}

/// EN wired to the manager endpoint (recovery + EC convert both consult the
/// manager for `extent_info`).
fn start_extent_node(addr: SocketAddr, dir: PathBuf, disk_id: u64, mgr: &str) {
    use autumn_stream::{ExtentNode, ExtentNodeConfig};
    let mgr = mgr.to_string();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let cfg = ExtentNodeConfig::new(dir, disk_id).with_manager_endpoint(mgr);
            let n = ExtentNode::new(cfg).await.expect("extent node");
            let _ = n.serve(addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));
}

async fn register_node(mgr: &RpcClient, addr: &str, disk_uuid: &str) -> u64 {
    let resp = mgr
        .call(
            MSG_REGISTER_NODE,
            rkyv_encode(&RegisterNodeReq {
                addr: addr.to_string(),
                disk_uuids: vec![disk_uuid.to_string()],
                shard_ports: vec![],
                control_address: String::new(),
                node_uuid: String::new(),
            }),
        )
        .await
        .expect("register node");
    let r: RegisterNodeResp = rkyv_decode(&resp).expect("decode RegisterNodeResp");
    assert_eq!(r.code, CODE_OK, "register: {}", r.message);
    r.node_id
}

/// Create an RF-`replicates` pure-replication stream; return `stream_id`.
async fn create_stream(mgr: &RpcClient, replicates: u32) -> u64 {
    let resp = mgr
        .call(
            MSG_CREATE_STREAM,
            rkyv_encode(&CreateStreamReq {
                replicates,
                ec_data_shard: replicates,
                ec_parity_shard: 0,
            }),
        )
        .await
        .expect("create_stream");
    let r: CreateStreamResp = rkyv_decode(&resp).expect("decode CreateStreamResp");
    assert_eq!(r.code, CODE_OK, "create_stream: {}", r.message);
    r.stream.expect("stream info").stream_id
}

async fn get_extent_info(mgr: &RpcClient, extent_id: u64) -> MgrExtentInfo {
    let resp = mgr
        .call(MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id }))
        .await
        .expect("extent_info");
    let r: ExtentInfoResp = rkyv_decode(&resp).expect("decode ExtentInfoResp");
    r.extent.expect("extent info")
}

/// Seal the stream's current tail at `commit` via the authoritative failover
/// seal path (the same call the recovery/EC tests use).
async fn seal_extent(mgr: &RpcClient, sc: &StreamClient, stream_id: u64, commit: u64) {
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
        .expect("seal");
    let seal: StreamAllocExtentResp = rkyv_decode(&resp).expect("decode seal");
    assert_eq!(seal.code, CODE_OK, "seal failed: {}", seal.message);
}

/// Recursively locate `extent-{id}.dat` under a node's data dir (hashed layout
/// `{dir}/{hash:02x}/extent-{id}.dat`).
fn find_dat(dir: &Path, extent_id: u64) -> PathBuf {
    let name = format!("extent-{extent_id}.dat");
    fn rec(d: &Path, name: &str) -> Option<PathBuf> {
        for e in std::fs::read_dir(d).ok()?.flatten() {
            let p = e.path();
            if p.is_dir() {
                if let Some(f) = rec(&p, name) {
                    return Some(f);
                }
            } else if p.file_name().map(|n| n == name).unwrap_or(false) {
                return Some(p);
            }
        }
        None
    }
    rec(dir, &name).unwrap_or_else(|| panic!("{name} not found under {dir:?}"))
}

fn read_file(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|e| panic!("read {path:?}: {e}"))
}

/// In-place bit-flip of `[start, start+len)` in `.dat` (XOR 0xFF). No
/// truncation, so a concurrently-open EN fd keeps a valid file the whole time —
/// this is exactly a silent at-rest bit-rot of the value region.
fn flip_range(path: &Path, start: usize, len: usize) {
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap_or_else(|e| panic!("open {path:?}: {e}"));
    f.seek(SeekFrom::Start(start as u64)).unwrap();
    let mut buf = vec![0u8; len];
    f.read_exact(&mut buf).unwrap();
    for b in &mut buf {
        *b ^= 0xFF;
    }
    f.seek(SeekFrom::Start(start as u64)).unwrap();
    f.write_all(&buf).unwrap();
    f.sync_all().unwrap();
}

/// The corrupt version of `payload` with `[start,start+len)` XOR-flipped —
/// the in-memory oracle for what a corrupted replica now holds.
fn corrupt_of(payload: &[u8], start: usize, len: usize) -> Vec<u8> {
    let mut c = payload.to_vec();
    for b in &mut c[start..start + len] {
        *b ^= 0xFF;
    }
    c
}

/// Direct single-replica EN read of `[offset,len)` (raw `pread`, no PS proxy).
async fn direct_read(en: &RpcClient, extent_id: u64, eversion: u64, offset: u64, len: u64) -> (u8, Vec<u8>) {
    let req = extent_rpc::ReadBytesReq {
        extent_id,
        eversion,
        offset,
        length: len,
    };
    let resp = en
        .call(extent_rpc::MSG_READ_BYTES, req.encode())
        .await
        .expect("MSG_READ_BYTES");
    let r = extent_rpc::ReadBytesResp::decode(resp).expect("decode ReadBytesResp");
    (r.code, r.payload.to_vec())
}

// ═══════════════════════════════════════════════════════════════════════════
// LEG (a) — READ: silent corruption served to clients, no detection.
// ═══════════════════════════════════════════════════════════════════════════
#[test]
fn leg_a_read_serves_silently_corrupted_sealed_bytes() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let mgr_str = mgr_addr.to_string();

    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();
    let (a1, a2, a3) = (pick_addr(), pick_addr(), pick_addr());
    start_extent_node(a1, d1.path().to_path_buf(), 1, &mgr_str);
    start_extent_node(a2, d2.path().to_path_buf(), 2, &mgr_str);
    start_extent_node(a3, d3.path().to_path_buf(), 3, &mgr_str);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("mgr");
        register_node(&mgr, &a1.to_string(), "u1").await;
        register_node(&mgr, &a2.to_string(), "u2").await;
        register_node(&mgr, &a3.to_string(), "u3").await;
        let stream_id = create_stream(&mgr, 3).await;

        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(&mgr_str, "owner/g12-read/0".into(), 256 * 1024 * 1024, pool)
            .await
            .expect("stream client");

        // Write a distinctive 64 KiB value, then SEAL the extent.
        const N: usize = 64 * 1024;
        let payload: Vec<u8> = (0..N).map(|i| (i % 251) as u8).collect();
        let r = sc.append(stream_id, &payload).await.expect("append");
        let extent_id = r.extent_id;
        seal_extent(&mgr, &sc, stream_id, r.end).await;

        sc.invalidate_extent_cache(extent_id);
        let ext = get_extent_info(&mgr, extent_id).await;
        assert!(ext.sealed_length as usize == N, "sealed_length={}", ext.sealed_length);
        let ev = ext.eversion;

        // Baseline: all three replicas hold identical, clean value bytes.
        let (p1, p2, p3) = (
            find_dat(d1.path(), extent_id),
            find_dat(d2.path(), extent_id),
            find_dat(d3.path(), extent_id),
        );
        assert_eq!(read_file(&p1), payload, "replica1 baseline");
        assert_eq!(read_file(&p2), payload, "replica2 baseline");
        assert_eq!(read_file(&p3), payload, "replica3 baseline");

        // Snapshot replica-1's `.meta` — we will show it is UNCHANGED after we
        // corrupt the `.dat`, i.e. the only at-rest checksum (the 40-byte meta
        // CRC) still validates while the value region silently rots.
        let meta1 = p1.with_extension("meta");
        let meta_before = read_file(&meta1);

        // ── corrupt the ENTIRE value region on replica 1 only ──
        flip_range(&p1, 0, N);
        let corrupt = corrupt_of(&payload, 0, N);
        assert_eq!(read_file(&p1), corrupt, "replica1 now corrupt on disk");
        assert_eq!(read_file(&p2), payload, "replica2 still clean");
        assert_eq!(read_file(&p3), payload, "replica3 still clean");
        assert_eq!(
            read_file(&meta1),
            meta_before,
            "the .dat corruption did NOT touch .meta — its CRC still validates \
             the (unchanged) 40 metadata bytes; nothing at rest guards the value"
        );

        // ── sub-check A1: the EN serves the corrupt bytes raw, with CODE_OK ──
        let en1 = RpcClient::connect(a1).await.expect("en1");
        let en2 = RpcClient::connect(a2).await.expect("en2");
        let en3 = RpcClient::connect(a3).await.expect("en3");
        let (c1, v1) = direct_read(&en1, extent_id, ev, 0, N as u64).await;
        let (c2, v2) = direct_read(&en2, extent_id, ev, 0, N as u64).await;
        let (c3, v3) = direct_read(&en3, extent_id, ev, 0, N as u64).await;
        assert_eq!(c1, extent_rpc::CODE_OK, "corrupt replica STILL returns CODE_OK (no checksum)");
        assert_eq!(c2, extent_rpc::CODE_OK);
        assert_eq!(c3, extent_rpc::CODE_OK);
        assert_eq!(v1, corrupt, "EN #1 served the corrupt value verbatim");
        assert_eq!(v2, payload, "EN #2 served clean");
        assert_eq!(v3, payload, "EN #3 served clean");
        eprintln!(
            "[G12/leg-a] EN direct reads: replica1=CORRUPT(code={c1}) replica2=clean replica3=clean \
             — the extent-node pread serves raw bytes with NO at-rest checksum."
        );

        // ── sub-check A2: the deterministic (extent_id,offset) rotation routes
        //    a fraction of client reads onto the corrupt replica → wrong bytes.
        let mut corrupt_reads = 0usize;
        let mut total = 0usize;
        let win = 1024usize;
        let mut off = 0usize;
        while off + win <= N {
            let (got, _) = sc
                .read_bytes_from_extent(extent_id, off as u64, win as u64)
                .await
                .expect("client read");
            let expect_clean = &payload[off..off + win];
            if got != expect_clean {
                assert_eq!(
                    got,
                    &corrupt[off..off + win],
                    "a client read returned bytes that are neither clean NOR the \
                     known corruption — unexpected"
                );
                corrupt_reads += 1;
            }
            total += 1;
            off += win;
        }
        eprintln!(
            "[G12/leg-a] client rotation sweep: {corrupt_reads}/{total} sub-range reads returned \
             the CORRUPT bytes (deterministic rotation over (extent_id,offset) — no failover, \
             no error, no detection)."
        );
        assert!(
            corrupt_reads >= 1,
            "expected the deterministic rotation to route at least one client read onto the \
             corrupt replica; got 0/{total} — if this fails the read path may have gained a \
             content check we did not model"
        );
    });
}

// ═══════════════════════════════════════════════════════════════════════════
// LEG (b) — RECOVERY: rebuilds a replica FROM the corrupt one, laundering it.
// ═══════════════════════════════════════════════════════════════════════════
#[test]
fn leg_b_recovery_launders_corruption_no_content_check() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let mgr_str = mgr_addr.to_string();

    // 3 stream members + 1 spare recovery target.
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();
    let d4 = tempfile::tempdir().unwrap();
    let (a1, a2, a3, a4) = (pick_addr(), pick_addr(), pick_addr(), pick_addr());
    start_extent_node(a1, d1.path().to_path_buf(), 1, &mgr_str);
    start_extent_node(a2, d2.path().to_path_buf(), 2, &mgr_str);
    start_extent_node(a3, d3.path().to_path_buf(), 3, &mgr_str);
    start_extent_node(a4, d4.path().to_path_buf(), 4, &mgr_str);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("mgr");
        // Register the 3 members FIRST (lowest node ids) so the RF-3 stream
        // selects them; the spare (a4) is registered afterwards.
        let n1 = register_node(&mgr, &a1.to_string(), "u1").await;
        let n2 = register_node(&mgr, &a2.to_string(), "u2").await;
        let n3 = register_node(&mgr, &a3.to_string(), "u3").await;
        let stream_id = create_stream(&mgr, 3).await;
        let _n4 = register_node(&mgr, &a4.to_string(), "u4").await;

        // node_id -> (addr, dir)
        let node_dir = |nid: u64| -> &Path {
            if nid == n1 {
                d1.path()
            } else if nid == n2 {
                d2.path()
            } else if nid == n3 {
                d3.path()
            } else {
                panic!("unexpected node id {nid}")
            }
        };

        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(&mgr_str, "owner/g12-recovery/0".into(), 256 * 1024 * 1024, pool)
            .await
            .expect("stream client");

        const N: usize = 32 * 1024;
        let payload: Vec<u8> = (0..N).map(|i| (i % 241) as u8 ^ 0x5A).collect();
        let r = sc.append(stream_id, &payload).await.expect("append");
        let extent_id = r.extent_id;
        seal_extent(&mgr, &sc, stream_id, r.end).await;

        sc.invalidate_extent_cache(extent_id);
        let ext = get_extent_info(&mgr, extent_id).await;
        assert!(ext.sealed_length as usize == N);
        let reps = ext.replicates.clone(); // slot order [r0, r1, r2]
        assert_eq!(reps.len(), 3, "replicated stream must have 3 members");
        let ev_before = ext.eversion;

        // The corrupt SURVIVOR is r0. To force recovery to source from it, we
        // make it the ONLY replica that still holds the data: physically delete
        // r1's and r2's files (models "those replicas were lost" — the reason
        // recovery runs). r1 will be the replaced slot; r2 is simply gone.
        let corrupt_dir = node_dir(reps[0]);
        let corrupt_dat = find_dat(corrupt_dir, extent_id);
        // flip a distinctive 64-byte marker inside the value region.
        let (cstart, clen) = (777usize, 64usize);
        flip_range(&corrupt_dat, cstart, clen);
        let corrupt = corrupt_of(&payload, cstart, clen);
        assert_eq!(read_file(&corrupt_dat), corrupt, "r0 corrupted on disk");

        for &nid in &[reps[1], reps[2]] {
            let dat = find_dat(node_dir(nid), extent_id);
            let meta = dat.with_extension("meta");
            std::fs::remove_file(&dat).ok();
            std::fs::remove_file(&meta).ok();
        }

        // Dispatch recovery DIRECTLY to the spare (a4), replacing r1. The spare
        // resolves extent_info from the manager (replicas [r0,r1,r2]), excludes
        // r1, and among {r0(corrupt,intact), r2(deleted)} only r0 can serve —
        // so it streams the corrupt copy. (fenced_only recovery gate means the
        // manager does not auto-dispatch; this direct dispatch is the only one.)
        let en4 = RpcClient::connect(a4).await.expect("en4");
        let task = extent_rpc::RecoveryTask {
            extent_id,
            replace_id: reps[1],
            node_id: 999,
            start_time: 0,
        };
        let resp = en4
            .call(
                extent_rpc::MSG_REQUIRE_RECOVERY,
                extent_rpc::rkyv_encode(&extent_rpc::RequireRecoveryReq { task: task.clone() }),
            )
            .await
            .expect("require_recovery");
        let code: extent_rpc::CodeResp = extent_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(code.code, extent_rpc::CODE_OK, "recovery dispatch refused: {}", code.message);

        // Wait for background recovery to complete (drain the spare's df queue).
        let mut done = false;
        for _ in 0..75 {
            compio::time::sleep(Duration::from_millis(200)).await;
            let resp = en4
                .call(
                    extent_rpc::MSG_DF,
                    extent_rpc::rkyv_encode(&extent_rpc::DfReq {
                        tasks: vec![],
                        disk_ids: vec![],
                    }),
                )
                .await
                .expect("df");
            let df: extent_rpc::DfResp = extent_rpc::rkyv_decode(&resp).expect("decode df");
            if df.done_tasks.iter().any(|t| t.task.extent_id == extent_id) {
                done = true;
                break;
            }
        }
        assert!(done, "recovery did not report completion within 15s");

        // The rebuilt replica on the spare must byte-equal the CORRUPT source —
        // recovery copied the rot with no content verification. verify-after-
        // fetch passed only because length (== sealed_length) and eversion did
        // not change.
        let recovered = read_file(&find_dat(d4.path(), extent_id));
        assert_eq!(
            recovered, corrupt,
            "the recovered replica does NOT match the corrupt source — if this fails, recovery \
             gained a content check we did not model"
        );
        assert_ne!(recovered, payload, "recovered copy equals the ORIGINAL — corruption was healed?!");

        sc.invalidate_extent_cache(extent_id);
        let ext_after = get_extent_info(&mgr, extent_id).await;
        eprintln!(
            "[G12/leg-b] recovery rebuilt slot from the corrupt survivor: recovered {} bytes == \
             corrupt source (marker @[{cstart},{}) flipped), eversion {}→{} (verify-after-fetch \
             is length+eversion only, NEVER content).",
            recovered.len(),
            cstart + clen,
            ev_before,
            ext_after.eversion,
        );
    });
}

// ═══════════════════════════════════════════════════════════════════════════
// LEG (c) — EC: parity/shards encoded over corrupt bytes, made canonical.
// ═══════════════════════════════════════════════════════════════════════════
#[test]
fn leg_c_ec_convert_encodes_corrupt_bytes_undetected() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let mgr_str = mgr_addr.to_string();

    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();
    let (a1, a2, a3) = (pick_addr(), pick_addr(), pick_addr());
    start_extent_node(a1, d1.path().to_path_buf(), 1, &mgr_str);
    start_extent_node(a2, d2.path().to_path_buf(), 2, &mgr_str);
    start_extent_node(a3, d3.path().to_path_buf(), 3, &mgr_str);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("mgr");
        let n1 = register_node(&mgr, &a1.to_string(), "u1").await;
        let n2 = register_node(&mgr, &a2.to_string(), "u2").await;
        let n3 = register_node(&mgr, &a3.to_string(), "u3").await;
        let stream_id = create_stream(&mgr, 3).await;

        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(&mgr_str, "owner/g12-ec/0".into(), 256 * 1024 * 1024, pool)
            .await
            .expect("stream client");

        const N: usize = 16 * 1024;
        let payload: Vec<u8> = (0..N).map(|i| ((i * 7) % 253) as u8).collect();
        let r = sc.append(stream_id, &payload).await.expect("append");
        let extent_id = r.extent_id;
        seal_extent(&mgr, &sc, stream_id, r.end).await;

        sc.invalidate_extent_cache(extent_id);
        let ext = get_extent_info(&mgr, extent_id).await;
        assert!(ext.sealed_length as usize == N);
        let reps = ext.replicates.clone();
        assert_eq!(reps.len(), 3);

        // The EC coordinator is `target_nodes[0] == ex.replicates[0]`
        // (handle_force_ec_convert). It reads its LOCAL bytes, slices them into
        // K=2 data shards, and encodes M=1 parity — all derived from ITS copy.
        // Corrupt exactly that node so the whole EC image is corrupt-derived.
        let coord_dir = if reps[0] == n1 {
            d1.path()
        } else if reps[0] == n2 {
            d2.path()
        } else if reps[0] == n3 {
            d3.path()
        } else {
            panic!("coordinator node {} not found", reps[0])
        };
        let coord_dat = find_dat(coord_dir, extent_id);
        let (cstart, clen) = (321usize, 48usize);
        flip_range(&coord_dat, cstart, clen);
        let corrupt = corrupt_of(&payload, cstart, clen);

        // Turn the stream into an EC-2+1 policy stream, then force conversion of
        // this sealed extent. Nothing verifies the coordinator's bytes.
        let resp = mgr
            .call(
                MSG_UPDATE_STREAM_EC,
                rkyv_encode(&UpdateStreamEcReq {
                    stream_id,
                    ec_data_shard: 2,
                    ec_parity_shard: 1,
                }),
            )
            .await
            .expect("update_stream_ec");
        let u: UpdateStreamEcResp = rkyv_decode(&resp).expect("decode UpdateStreamEcResp");
        assert_eq!(u.code, CODE_OK, "update_stream_ec: {}", u.message);

        let resp = mgr
            .call(MSG_FORCE_EC_CONVERT, rkyv_encode(&ForceEcConvertReq { extent_id }))
            .await
            .expect("force_ec");
        let f: ForceEcConvertResp = rkyv_decode(&resp).expect("decode ForceEcConvertResp");
        assert_eq!(
            f.code, CODE_OK,
            "force_ec_convert refused an extent with a corrupt replica: {} \
             (if this is a NEW content check, the harness has surfaced a guard)",
            f.message
        );

        // Wait for the dispatch loop (5s cadence) to convert.
        let mut converted = false;
        for _ in 0..20 {
            compio::time::sleep(Duration::from_secs(2)).await;
            let e = get_extent_info(&mgr, extent_id).await;
            if e.ec_converted {
                converted = true;
                break;
            }
        }
        assert!(
            converted,
            "EC conversion of the corrupt-containing extent did not complete — NO layer errored \
             on the corruption, but conversion also did not finish within 40s"
        );
        eprintln!("[G12/leg-c] EC conversion COMPLETED (ec_converted=true) with NO detection of the corrupt replica.");

        // Read the value back through the EC decode path. Because the
        // coordinator's corrupt copy seeded every shard, the reconstructed
        // value IS the corrupt one — corruption is now canonical/erasure-coded.
        sc.invalidate_extent_cache(extent_id);
        let (got, _) = sc
            .read_bytes_from_extent(extent_id, 0, N as u64)
            .await
            .expect("EC read-back");
        assert_eq!(
            got, corrupt,
            "EC read-back is not the coordinator's corrupt bytes — if this fails, EC gained a \
             content/parity check we did not model"
        );
        assert_ne!(got, payload, "EC read-back equals ORIGINAL — corruption was somehow healed?!");
        eprintln!(
            "[G12/leg-c] EC read-back returned the CORRUPT value (marker @[{cstart},{}) flipped): \
             parity + data shards were encoded over corrupt bytes and nothing detected it.",
            cstart + clen
        );
    });
}
