//! §9 of the CoW design: `handle_re_avali` refills a replica by TRUNCATING it
//! to zero and streaming from peers. If the peers cannot deliver, the local
//! copy is gone — a payload file left in "neither state", which §1 forbids.
//!
//! The design asked whether the target can be a replica the manager still
//! counts. Reading the code, the answer is no: the sole dispatcher
//! (`recovery.rs`, `if (ex.avali & bit) == 0`) only ever targets a slot whose
//! `avali` bit is CLEAR. But that turns out to be the wrong question, because
//! **`avali == 0` does not mean "lagging"**:
//!
//!   > An unreachable committed member gets its `avali` bit left unset →
//!   > reconciled by recovery later; it does not block the seal.
//!   >   — `crates/manager/CLAUDE.md`, seal-over-reachable
//!
//! So a member that was merely UNREACHABLE when the extent was sealed carries
//! `avali = 0` while potentially holding the LONGEST copy in the cluster — and
//! `stream_extent_from_sources` selects its sources from all members WITHOUT
//! filtering on `avali`, so those bytes are exactly what another node's
//! recovery would rebuild from.
//!
//! This test drives that shape end to end: an extent sealed above what the
//! target holds, with the only other member unreachable. The refill cannot
//! succeed. The question under test is what the target is left holding.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::extent_rpc;
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ConnPool, StreamClient};

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

/// A node that can be KILLED: dropping its runtime tears down the listener and
/// every live connection, so later connects get ECONNREFUSED — a real peer
/// death, which is the situation repair runs in.
fn start_killable_extent_node(
    addr: SocketAddr,
    dir: PathBuf,
    disk_id: u64,
    mgr: &str,
) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
    use autumn_stream::{ExtentNode, ExtentNodeConfig};
    let mgr = mgr.to_string();
    let kill = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let k = kill.clone();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let cfg = ExtentNodeConfig::new(dir, disk_id).with_manager_endpoint(mgr);
            let n = ExtentNode::new(cfg).await.expect("extent node");
            compio::runtime::spawn(async move {
                let _ = n.serve(addr).await;
            })
            .detach();
            while !k.load(std::sync::atomic::Ordering::SeqCst) {
                compio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    });
    std::thread::sleep(Duration::from_millis(200));
    kill
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

/// Seal the stream's tail at EXACTLY `commit` (authoritative, no probe). This
/// is how a real seal-over-reachable ends up above what an unreachable member
/// holds — that member simply was not in the min.
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

/// A re_avali that cannot obtain a replacement must leave the local copy
/// exactly as it found it.
///
/// The bytes at stake are the cluster's best available copy of this extent: the
/// target is `avali = 0` (which is why repair was aimed at it), but that only
/// means "not counted at seal time", and every recovery elsewhere reads sources
/// from the member list without consulting `avali`. Destroying them before a
/// replacement is in hand converts a repairable replica into an empty one, and
/// on a cluster where the other members are down — the situation that HAS
/// repair running — there is nothing left to rebuild from.
#[test]
fn re_avali_that_cannot_refill_must_not_destroy_the_local_copy() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let mgr_str = mgr_addr.to_string();

    let a_addr = pick_addr();
    let a_dir = tempfile::tempdir().expect("tempdir a");
    start_extent_node(a_addr, a_dir.path().to_path_buf(), 1, &mgr_str);

    // The second member is alive while the extent is created and written, then
    // KILLED — the peer that is down while repair runs.
    let b_addr = pick_addr();
    let b_dir = tempfile::tempdir().expect("tempdir b");
    let kill_b = start_killable_extent_node(b_addr, b_dir.path().to_path_buf(), 2, &mgr_str);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_node(&mgr, &a_addr.to_string(), "disk-a").await;
        register_node(&mgr, &b_addr.to_string(), "disk-b").await;

        let stream_id = create_stream(&mgr, 2).await;
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(&mgr_str, "owner/re-avali/0".into(), 256 * 1024 * 1024, pool)
            .await
            .expect("stream client");

        // Write through the normal path so A holds a real, valid prefix.
        let payload = vec![0x7Eu8; 4096];
        let r = sc.append(stream_id, &payload).await.expect("append");
        let extent_id = r.extent_id;

        // Seal ABOVE what A holds. This is the shape a seal-over-reachable
        // produces when a member with more data was unreachable at seal time.
        let sealed_at = (payload.len() + 2048) as u64;
        seal_extent(&mgr, &sc, stream_id, sealed_at).await;
        sc.invalidate_extent_cache(extent_id);

        let ex = get_extent_info(&mgr, extent_id).await;
        assert_eq!(ex.sealed_length, sealed_at, "seal did not take");

        let dat = find_dat(a_dir.path(), extent_id);
        let before = std::fs::metadata(&dat).expect("stat before").len();
        assert!(
            before > 0 && before < sealed_at,
            "precondition: A must hold a short-but-real copy (got {before} of {sealed_at})"
        );

        // Kill B. A's only peer is now unreachable and A is itself short, so the
        // refill cannot possibly reach `sealed_length`.
        kill_b.store(true, std::sync::atomic::Ordering::SeqCst);
        compio::time::sleep(Duration::from_millis(400)).await;

        let a = RpcClient::connect(a_addr).await.expect("connect a");
        let resp = a
            .call(
                extent_rpc::MSG_RE_AVALI,
                extent_rpc::rkyv_encode(&extent_rpc::ReAvaliReq {
                    extent_id,
                    eversion: ex.eversion,
                }),
            )
            .await
            .expect("re_avali rpc");
        let r: extent_rpc::CodeResp = extent_rpc::rkyv_decode(&resp).expect("decode CodeResp");
        assert_ne!(
            r.code,
            extent_rpc::CODE_OK,
            "re_avali claimed success without reaching sealed_length"
        );

        let after = std::fs::metadata(&dat).expect("stat after").len();
        assert_eq!(
            after, before,
            "re_avali destroyed the local copy it could not replace \
             ({before} bytes -> {after}); a failed repair must be a no-op"
        );
    });
}

/// The other half: when a peer CAN deliver, re_avali must still actually
/// repair. A guard that only ever refuses is not a fix, and nothing else in
/// the suite covers this path.
#[test]
fn re_avali_still_repairs_when_a_peer_has_the_data() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let mgr_str = mgr_addr.to_string();

    let a_addr = pick_addr();
    let a_dir = tempfile::tempdir().expect("tempdir a");
    start_extent_node(a_addr, a_dir.path().to_path_buf(), 1, &mgr_str);

    let b_addr = pick_addr();
    let b_dir = tempfile::tempdir().expect("tempdir b");
    start_extent_node(b_addr, b_dir.path().to_path_buf(), 2, &mgr_str);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_node(&mgr, &a_addr.to_string(), "disk-a2").await;
        register_node(&mgr, &b_addr.to_string(), "disk-b2").await;

        let stream_id = create_stream(&mgr, 2).await;
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(&mgr_str, "owner/re-avali/1".into(), 256 * 1024 * 1024, pool)
            .await
            .expect("stream client");
        // Allocate the extent on both members without writing through the
        // all-replica path, so the two copies can legitimately diverge.
        let tail = sc.get_stream_info(stream_id).await.expect("stream info");
        let extent_id = *tail.extent_ids.last().expect("tail extent");

        // Write to B ONLY. A is left holding an empty copy — a member that
        // lags, which is exactly what re_avali exists to repair.
        let payload = vec![0x3Du8; 4096];
        let b = RpcClient::connect(b_addr).await.expect("connect b");
        let ap = b
            .call(
                extent_rpc::MSG_APPEND,
                extent_rpc::AppendReq {
                    extent_id,
                    eversion: 1,
                    commit: 0,
                    owner_epoch: 0,
                    payload: payload.clone().into(),
                }
                .encode(),
            )
            .await
            .expect("append to b");
        let ap = extent_rpc::AppendResp::decode(ap).expect("decode AppendResp");
        assert_eq!(ap.code, extent_rpc::CODE_OK, "direct append to B failed");

        seal_extent(&mgr, &sc, stream_id, payload.len() as u64).await;
        sc.invalidate_extent_cache(extent_id);
        let ex = get_extent_info(&mgr, extent_id).await;
        assert_eq!(ex.sealed_length, payload.len() as u64);

        let dat = find_dat(a_dir.path(), extent_id);
        assert_eq!(
            std::fs::metadata(&dat).expect("stat a").len(),
            0,
            "precondition: A must be the lagging copy"
        );

        let a = RpcClient::connect(a_addr).await.expect("connect a");
        let resp = a
            .call(
                extent_rpc::MSG_RE_AVALI,
                extent_rpc::rkyv_encode(&extent_rpc::ReAvaliReq {
                    extent_id,
                    eversion: ex.eversion,
                }),
            )
            .await
            .expect("re_avali rpc");
        let r: extent_rpc::CodeResp = extent_rpc::rkyv_decode(&resp).expect("decode CodeResp");
        assert_eq!(r.code, extent_rpc::CODE_OK, "re_avali failed: {}", r.message);

        let after = std::fs::read(&dat).expect("read a dat");
        assert_eq!(after.len(), payload.len(), "repair did not restore full length");
        assert_eq!(after, payload, "repair restored the wrong bytes");
    });
}
