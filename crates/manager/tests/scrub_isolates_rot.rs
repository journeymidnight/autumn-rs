//! The whole chain, end to end: a replica rots at rest, its own node's scrub
//! finds it with nobody reading, and the manager isolates that slot so recovery
//! rebuilds it.
//!
//! Each half of this is useless alone. Detection that cannot reach the manager
//! leaves a rotted copy in the read rotation forever; isolation with no detector
//! is what the cluster already had — a clear `avali` bit means "behind", which
//! `re_avali` heals with a length comparison that a full-length rotted replica
//! passes. This test is the seam.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ConnPool, ExtentNode, ExtentNodeConfig, StreamClient};

fn pick_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let a = l.local_addr().expect("addr");
    drop(l);
    a
}

fn start_manager(addr: SocketAddr) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let m = AutumnManager::new();
            let _ = m.serve(addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));
}
use autumn_manager::AutumnManager;

fn start_extent_node(addr: SocketAddr, dir: PathBuf, disk_id: u64, mgr: &str) {
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

async fn register_node(mgr: &RpcClient, addr: &str, disk: &str) -> u64 {
    let resp = mgr
        .call(
            MSG_REGISTER_NODE,
            rkyv_encode(&RegisterNodeReq {
                addr: addr.to_string(),
                disk_uuids: vec![disk.to_string()],
                shard_ports: vec![],
                control_address: String::new(),
                node_uuid: String::new(),
            }),
        )
        .await
        .expect("register");
    rkyv_decode::<RegisterNodeResp>(&resp).expect("decode").node_id
}

async fn extent_info(mgr: &RpcClient, extent_id: u64) -> MgrExtentInfo {
    let resp = mgr
        .call(MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id }))
        .await
        .expect("extent_info");
    rkyv_decode::<ExtentInfoResp>(&resp)
        .expect("decode")
        .extent
        .expect("extent")
}

fn find_dat(root: &Path, extent_id: u64) -> PathBuf {
    let name = format!("extent-{extent_id}.dat");
    for sub in std::fs::read_dir(root).expect("read root").flatten() {
        let p = sub.path();
        if !p.is_dir() {
            continue;
        }
        for f in std::fs::read_dir(&p).expect("read sub").flatten() {
            if f.file_name().to_string_lossy() == name {
                return f.path();
            }
        }
    }
    panic!("no .dat for extent {extent_id} under {}", root.display());
}

fn ck_exists(root: &Path, extent_id: u64) -> bool {
    let name = format!("extent-{extent_id}.ck");
    std::fs::read_dir(root)
        .expect("read root")
        .flatten()
        .filter(|s| s.path().is_dir())
        .any(|s| {
            std::fs::read_dir(s.path())
                .map(|d| {
                    d.flatten()
                        .any(|f| f.file_name().to_string_lossy() == name)
                })
                .unwrap_or(false)
        })
}

#[test]
fn a_rotted_replica_is_found_by_its_own_node_and_isolated_by_the_manager() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let mgr_str = mgr_addr.to_string();

    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs: Vec<SocketAddr> = (0..3).map(|_| pick_addr()).collect();
    for i in 0..3 {
        start_extent_node(
            addrs[i],
            dirs[i].path().to_path_buf(),
            (i + 1) as u64,
            &mgr_str,
        );
    }

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("mgr");
        let mut node_ids = Vec::new();
        for i in 0..3 {
            node_ids.push(register_node(&mgr, &addrs[i].to_string(), &format!("d{i}")).await);
        }

        let resp = mgr
            .call(
                MSG_CREATE_STREAM,
                rkyv_encode(&CreateStreamReq {
                    replicates: 3,
                    ec_data_shard: 3,
                    ec_parity_shard: 0,
                }),
            )
            .await
            .unwrap();
        let created: CreateStreamResp = rkyv_decode(&resp).unwrap();
        assert_eq!(created.code, CODE_OK, "create_stream: {}", created.message);
        let stream = created.stream.expect("stream");
        let stream_id = stream.stream_id;

        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(&mgr_str, "owner/scrub-iso/0".into(), 256 * 1024 * 1024, pool)
            .await
            .expect("stream client");
        let payload: Vec<u8> = (0..(64 * 1024u32)).map(|i| (i % 251) as u8).collect();
        let r = sc.append(stream_id, &payload).await.expect("append");
        let extent_id = r.extent_id;

        // Seal, then give the scrub time to describe the clean content. This is
        // the backfill half: with no seal event on an extent node, the sweep is
        // what gives a rolled tail its checksums.
        let resp = mgr
            .call(
                MSG_STREAM_ALLOC_EXTENT,
                rkyv_encode(&StreamAllocExtentReq {
                    stream_id,
                    owner_key: sc.owner_key().to_string(),
                    owner_epoch: sc.owner_epoch(),
                    seal_commit: Some(r.end),
                    exclude_node_ids: vec![],
                    seal_extent_id: 0,
                }),
            )
            .await
            .expect("seal+roll");
        let _: StreamAllocExtentResp = rkyv_decode(&resp).expect("decode");

        let victim = 0usize;
        let mut described = false;
        for _ in 0..40 {
            compio::time::sleep(Duration::from_millis(500)).await;
            if ck_exists(dirs[victim].path(), extent_id) {
                described = true;
                break;
            }
        }
        assert!(
            described,
            "the scrub never described this sealed extent, so there is nothing \
             for it to detect rot against"
        );

        let before = extent_info(&mgr, extent_id).await;
        assert!(
            before.avali.count_ones() >= 2,
            "precondition: more than one replica is available, or isolation would \
             correctly refuse"
        );
        // The slot belonging to the node whose disk we are about to rot. Asserting
        // on a COUNT would pass if the manager darkened somebody else's slot,
        // which is the failure most worth catching here.
        let victim_slot = before
            .replicates
            .iter()
            .chain(before.parity.iter())
            .position(|n| *n == node_ids[victim])
            .expect("the victim node holds a slot of this extent");
        let victim_bit = 1u32 << victim_slot;
        assert_ne!(before.avali & victim_bit, 0, "precondition: victim slot is available");

        // Rot one byte on replica 1 only. Nothing reads it.
        let path = find_dat(dirs[victim].path(), extent_id);
        let mut rotted = std::fs::read(&path).expect("read dat");
        rotted[1234] ^= 0x01;
        std::fs::write(&path, &rotted).expect("rot");

        // The node's own scrub must find it and the manager must act.
        // Assert on the VICTIM's bit, resolved from the manager's own replica
        // order: that order is not the order these nodes registered in, so
        // "slot 0" is not this directory's slot, and isolating the wrong bit
        // would otherwise look exactly like success.
        let mut isolated = false;
        for _ in 0..60 {
            compio::time::sleep(Duration::from_millis(500)).await;
            let ex = extent_info(&mgr, extent_id).await;
            if ex.avali & victim_bit == 0 {
                isolated = true;
                assert_eq!(
                    ex.avali.count_ones() + 1,
                    before.avali.count_ones(),
                    "exactly one slot should have gone dark"
                );
                break;
            }
        }
        assert!(
            isolated,
            "the rotted replica was never isolated — it stays in the read rotation, \
             and recovery has no reason to rebuild it"
        );
    });
}
