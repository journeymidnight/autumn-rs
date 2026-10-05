//! At-rest rot of a sealed replica, end to end: once a replica's content has
//! been described, a bit flipped in it at rest must never reach a reader, be
//! copied by recovery, or be encoded into EC parity.
//!
//! This began as a reproduction that passed BECAUSE nothing noticed: the RPC
//! frame CRC excludes the bulk value, `.meta`'s CRC covers its own 48 bytes,
//! recovery's verify-after-fetch compares length and eversion (a flip moves
//! neither), EC conversion encoded whatever the coordinator read, and replica
//! choice is a deterministic hash of `(extent_id, offset)`, so the damaged copy
//! was picked consistently. Each leg is now the opposite assertion.
//!
//! Every leg waits for the replicas to DESCRIBE the sealed content before it
//! rots one. A replica that rots before any description exists gets the rot
//! recorded as truth by the first backfill — the trust-on-first-use window the
//! design accepts — so rotting first would test that window, not detection.
//!
//! The legs and what each catches:
//!   (a) READ     — a whole-block read of the rotted replica is refused by its
//!                  node, client reads get the right bytes from another
//!                  replica, and once the node's scrub has isolated the slot
//!                  even sub-block reads (which no checksum covers) are right.
//!   (b) RECOVERY — a rebuild whose only full-length source is the rotted
//!                  replica refuses it: the result is byte-exact or absent.
//!   (c) EC       — converting over a rotted coordinator never yields parity
//!                  encoded from the rot: the rot is found and isolated, and an
//!                  EC read-back, if the extent ever converts, is the original.
//!
//! Rot found with nobody reading is `scrub_isolates_rot.rs`; the same chain for
//! an EC shard file is `ec_shard_rot.rs`.

mod support;

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
    find_dat_opt(dir, extent_id)
        .unwrap_or_else(|| panic!("extent-{extent_id}.dat not found under {dir:?}"))
}

fn find_dat_opt(dir: &Path, extent_id: u64) -> Option<PathBuf> {
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
    rec(dir, &name)
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

/// Direct single-replica EN read of `[offset,len)` (no PS proxy). `Ok` only
/// when the node SERVED the bytes; a refusal — a typed error frame or a non-OK
/// code — is `Err` with its reason.
async fn direct_read(
    en: &RpcClient,
    extent_id: u64,
    eversion: u64,
    offset: u64,
    len: u64,
) -> Result<Vec<u8>, String> {
    let req = extent_rpc::ReadBytesReq::new(
        extent_id,
        eversion,
        offset,
        len,
        extent_rpc::PayloadRef::in_dat(),
    );
    let resp = en
        .call(extent_rpc::MSG_READ_BYTES, req.encode())
        .await
        .map_err(|e| e.to_string())?;
    let r = extent_rpc::ReadBytesResp::decode(resp).expect("decode ReadBytesResp");
    if r.code != extent_rpc::CODE_OK {
        return Err(format!("code {}", r.code));
    }
    Ok(r.payload.to_vec())
}

fn ck_exists(dir: &Path, extent_id: u64) -> bool {
    let dat = find_dat(dir, extent_id);
    dat.with_extension("ck").exists()
}

/// Wait until every listed node has described the sealed extent. Rot is only
/// catchable against a description taken BEFORE it: an extent that rots before
/// any sidecar exists gets the rot recorded as truth (trust-on-first-use).
async fn wait_described(dirs: &[&Path], extent_id: u64) {
    for _ in 0..60 {
        if dirs.iter().all(|d| ck_exists(d, extent_id)) {
            return;
        }
        compio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("not every replica described extent {extent_id} within 30 s");
}

fn slot_bit(ex: &MgrExtentInfo, node_id: u64) -> u32 {
    1u32 << ex
        .replicates
        .iter()
        .chain(ex.parity.iter())
        .position(|n| *n == node_id)
        .expect("node holds a slot")
}

// ═══════════════════════════════════════════════════════════════════════════
// LEG (a) — READ
// ═══════════════════════════════════════════════════════════════════════════
#[test]
fn leg_a_rotted_replica_is_never_served() {
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
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("mgr");
        let n1 = register_node(&mgr, &a1.to_string(), "u1").await;
        register_node(&mgr, &a2.to_string(), "u2").await;
        register_node(&mgr, &a3.to_string(), "u3").await;
        let stream_id = create_stream(&mgr, 3).await;

        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(&mgr_str, "owner/g12-read/0".into(), 256 * 1024 * 1024, pool)
            .await
            .expect("stream client");

        // Three whole 1 MiB blocks and a short tail.
        const MIB: usize = 1024 * 1024;
        const N: usize = 3 * MIB + 777;
        let payload: Vec<u8> = (0..N).map(|i| (i % 251) as u8).collect();
        let r = sc.append(stream_id, &payload).await.expect("append");
        let extent_id = r.extent_id;
        seal_extent(&mgr, &sc, stream_id, r.end).await;
        wait_described(&[d1.path(), d2.path(), d3.path()], extent_id).await;

        sc.invalidate_extent_cache(extent_id);
        let ext = get_extent_info(&mgr, extent_id).await;
        assert_eq!(ext.sealed_length as usize, N);
        let victim_bit = slot_bit(&ext, n1);

        // Rot 64 bytes inside block 1 of replica 1 only.
        let p1 = find_dat(d1.path(), extent_id);
        let (cstart, clen) = (MIB + MIB / 2, 64usize);
        flip_range(&p1, cstart, clen);
        assert_eq!(read_file(&p1), corrupt_of(&payload, cstart, clen), "replica 1 rotted on disk");

        // Its node refuses a read that covers the rotted block; a clean replica
        // serves the same range.
        let en1 = RpcClient::connect_as(a1, autumn_rpc::version_hello::Role::Admin, None).await.expect("en1");
        let en2 = RpcClient::connect_as(a2, autumn_rpc::version_hello::Role::Admin, None).await.expect("en2");
        let refused = direct_read(&en1, extent_id, ext.eversion, MIB as u64, MIB as u64).await;
        assert!(refused.is_err(), "the rotted replica served a block that fails its checksum");
        assert_eq!(
            direct_read(&en2, extent_id, ext.eversion, MIB as u64, MIB as u64).await,
            Ok(payload[MIB..2 * MIB].to_vec()),
            "a clean replica must serve the same block"
        );

        // Whole-block client reads: whichever replica the rotation starts on,
        // the answer is the original.
        for (off, len) in [(0, N), (0, MIB), (MIB, MIB), (2 * MIB, MIB), (MIB, 2 * MIB)] {
            let (got, _) = sc
                .read_bytes_from_extent(extent_id, off as u64, len as u64)
                .await
                .expect("client read");
            assert!(got == payload[off..off + len], "client read [{off}, +{len}) returned rot");
        }

        // Nobody has to read the rotted bytes for the slot to be isolated: the
        // node's own scrub finds them.
        let mut isolated = false;
        for _ in 0..60 {
            compio::time::sleep(Duration::from_millis(500)).await;
            if get_extent_info(&mgr, extent_id).await.avali & victim_bit == 0 {
                isolated = true;
                break;
            }
        }
        assert!(isolated, "the rotted replica was never isolated");

        // Isolated, it serves nothing — including the 4 KiB reads that no
        // checksum covers, which before isolation could still land on it.
        sc.invalidate_extent_cache(extent_id);
        let win = 4096usize;
        let mut off = MIB;
        while off < 2 * MIB {
            let (got, _) = sc
                .read_bytes_from_extent(extent_id, off as u64, win as u64)
                .await
                .expect("sub-block read");
            assert!(got == payload[off..off + win], "sub-block read at {off} returned rot");
            off += win;
        }
    });
}

// ═══════════════════════════════════════════════════════════════════════════
// LEG (b) — RECOVERY
// ═══════════════════════════════════════════════════════════════════════════
#[test]
fn leg_b_recovery_never_copies_a_rotted_source() {
    let mgr_addr = pick_addr();
    // Background loops off: the rebuild below is dispatched by hand, and the
    // scrub's own findings must not isolate the rotted source first — that
    // would keep recovery from reading it at all, and this leg is about what
    // recovery does when it does.
    let manager_control = support::start_recovery_manager(mgr_addr);
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

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("mgr");
        // Register the 3 members FIRST (lowest node ids) so the RF-3 stream
        // selects them; the spare (a4) is registered afterwards.
        let n1 = register_node(&mgr, &a1.to_string(), "u1").await;
        let n2 = register_node(&mgr, &a2.to_string(), "u2").await;
        let n3 = register_node(&mgr, &a3.to_string(), "u3").await;
        let stream_id = create_stream(&mgr, 3).await;
        let n4 = register_node(&mgr, &a4.to_string(), "u4").await;
        let nodes: NodesInfoResp =
            rkyv_decode(&mgr.call(MSG_NODES_INFO, bytes::Bytes::new()).await.unwrap()).unwrap();
        let disk = nodes
            .nodes
            .iter()
            .find(|(id, _)| *id == n4)
            .unwrap()
            .1
            .disks[0];
        start_extent_node(a4, d4.path().to_path_buf(), disk, &mgr_str);

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

        const N: usize = 2 * 1024 * 1024 + 333;
        let payload: Vec<u8> = (0..N).map(|i| (i % 241) as u8 ^ 0x5A).collect();
        let r = sc.append(stream_id, &payload).await.expect("append");
        let extent_id = r.extent_id;
        seal_extent(&mgr, &sc, stream_id, r.end).await;

        sc.invalidate_extent_cache(extent_id);
        let ext = get_extent_info(&mgr, extent_id).await;
        assert_eq!(ext.sealed_length as usize, N);
        let reps = ext.replicates.clone();
        assert_eq!(reps.len(), 3, "replicated stream must have 3 members");
        wait_described(&[node_dir(reps[0])], extent_id).await;

        // r0 rots and becomes the only replica still on disk: r1 is the slot
        // being replaced and r2 is gone too. This is the case where copying
        // the rot used to be recovery's only option.
        let corrupt_dat = find_dat(node_dir(reps[0]), extent_id);
        let (cstart, clen) = (1024 * 1024 + 777usize, 64usize);
        flip_range(&corrupt_dat, cstart, clen);
        let corrupt = corrupt_of(&payload, cstart, clen);
        assert_eq!(read_file(&corrupt_dat), corrupt, "r0 corrupted on disk");
        for &nid in &[reps[1], reps[2]] {
            let dat = find_dat(node_dir(nid), extent_id);
            std::fs::remove_file(dat.with_extension("meta")).ok();
            std::fs::remove_file(&dat).ok();
        }

        let en4 = RpcClient::connect_as(a4, autumn_rpc::version_hello::Role::Admin, None).await.expect("en4");
        let task = extent_rpc::RecoveryTask {
            extent_id,
            replace_id: reps[1],
            node_id: n4,
            start_time: 0,
        };
        let request = manager_control.instruction(task.clone()).await;
        let resp = en4
            .call(
                extent_rpc::MSG_REQUIRE_RECOVERY,
                extent_rpc::rkyv_encode(&request),
            )
            .await
            .expect("require_recovery");
        let code: extent_rpc::CodeResp = extent_rpc::rkyv_decode(&resp).expect("decode");
        assert_eq!(code.code, extent_rpc::CODE_OK, "recovery dispatch refused: {}", code.message);

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

        // Byte-exact or not at all. A copy that finished must be the original;
        // one that did not must not have left the rot behind as a replica.
        let rebuilt = find_dat_opt(d4.path(), extent_id).map(|p| read_file(&p));
        eprintln!(
            "[leg-b] recovery {}",
            if done { "completed" } else { "did not complete" }
        );
        if done {
            assert!(
                rebuilt.as_deref() == Some(payload.as_slice()),
                "recovery reported done with content that is not the original"
            );
        } else {
            assert!(
                rebuilt.as_deref() != Some(corrupt.as_slice()),
                "recovery copied the rotted source"
            );
        }
    });
}

// ═══════════════════════════════════════════════════════════════════════════
// LEG (c) — EC
// ═══════════════════════════════════════════════════════════════════════════
#[test]
fn leg_c_ec_never_encodes_a_rotted_coordinator() {
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
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("mgr");
        let n1 = register_node(&mgr, &a1.to_string(), "u1").await;
        let n2 = register_node(&mgr, &a2.to_string(), "u2").await;
        let n3 = register_node(&mgr, &a3.to_string(), "u3").await;
        let stream_id = create_stream(&mgr, 3).await;

        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(&mgr_str, "owner/g12-ec/0".into(), 256 * 1024 * 1024, pool)
            .await
            .expect("stream client");

        const N: usize = 2 * 1024 * 1024 + 100;
        let payload: Vec<u8> = (0..N).map(|i| ((i * 7) % 253) as u8).collect();
        let r = sc.append(stream_id, &payload).await.expect("append");
        let extent_id = r.extent_id;
        seal_extent(&mgr, &sc, stream_id, r.end).await;
        wait_described(&[d1.path(), d2.path(), d3.path()], extent_id).await;

        sc.invalidate_extent_cache(extent_id);
        let ext = get_extent_info(&mgr, extent_id).await;
        assert_eq!(ext.sealed_length as usize, N);

        // The coordinator (`replicates[0]`) encodes every shard from ITS copy,
        // so rotting it is what would make the rot canonical across the stripe.
        let coord = ext.replicates[0];
        let coord_dir = if coord == n1 {
            d1.path()
        } else if coord == n2 {
            d2.path()
        } else if coord == n3 {
            d3.path()
        } else {
            panic!("coordinator node {coord} not found")
        };
        let coord_bit = slot_bit(&ext, coord);
        let (cstart, clen) = (1024 * 1024 + 321usize, 48usize);
        flip_range(&find_dat(coord_dir, extent_id), cstart, clen);

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
        // Accepted or refused, both are fine here: the scrub may already have
        // isolated the coordinator, and a marked extent is not converted until
        // its slot is rebuilt.
        let _ = mgr
            .call(MSG_FORCE_EC_CONVERT, rkyv_encode(&ForceEcConvertReq { extent_id }))
            .await
            .expect("force_ec");

        // The rot is found (by the pre-encode check or by the scrub) and the
        // coordinator's slot isolated; with no spare node it stays dark, and a
        // marked extent is never converted. If it ever does convert, it must
        // read back as the original.
        let mut isolated = false;
        for _ in 0..40 {
            compio::time::sleep(Duration::from_secs(1)).await;
            let e = get_extent_info(&mgr, extent_id).await;
            if e.ec_converted {
                sc.invalidate_extent_cache(extent_id);
                let (got, _) = sc
                    .read_bytes_from_extent(extent_id, 0, N as u64)
                    .await
                    .expect("EC read-back");
                assert!(got == payload, "EC parity was encoded from the rotted coordinator");
            } else if e.avali & coord_bit == 0 {
                isolated = true;
            }
        }
        assert!(isolated, "the rotted coordinator was never isolated");
        assert!(
            !get_extent_info(&mgr, extent_id).await.ec_converted,
            "converted with a rotted coordinator and nowhere to rebuild it"
        );
    });
}
