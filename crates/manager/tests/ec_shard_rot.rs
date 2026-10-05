//! An EC shard that rots at rest is caught, routed around, isolated and
//! rebuilt — the shard-file counterpart of `scrub_isolates_rot.rs`.
//!
//! A shard has no second home: the only other source of its bytes is a parity
//! reconstruct. So "route around it" means two different things here than for
//! a replica. A whole-block read of the rotted shard must be REFUSED by its
//! node (the client then reconstructs it from the others), and once the scrub
//! has isolated the slot the client must not read it at all — not even the
//! sub-block reads no checksum covers — nor use it as an input to anyone
//! else's reconstruct, where one wrong shard makes every byte it spans wrong.
//!
//! The shard's checksums are written as it is staged, from the bytes in hand,
//! so this rots a shard that was described the moment it existed: there is no
//! trust-on-first-use window to wait out.

mod support;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use autumn_rpc::client::RpcClient;
use autumn_rpc::extent_rpc;
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

async fn extent_info(mgr: &RpcClient, extent_id: u64) -> MgrExtentInfo {
    let resp = mgr
        .call(MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id }))
        .await
        .expect("extent_info");
    let r: ExtentInfoResp = rkyv_decode(&resp).expect("decode extent_info");
    r.extent.expect("extent present")
}

fn find_file(dir: &Path, name: &str) -> Option<PathBuf> {
    for e in std::fs::read_dir(dir).ok()?.flatten() {
        let p = e.path();
        if p.is_dir() {
            if let Some(f) = find_file(&p, name) {
                return Some(f);
            }
        } else if p.file_name().is_some_and(|n| n == name) {
            return Some(p);
        }
    }
    None
}

fn flip_byte(path: &Path, offset: usize) {
    use std::io::{Read, Seek, SeekFrom, Write};
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open shard");
    f.seek(SeekFrom::Start(offset as u64)).unwrap();
    let mut b = [0u8; 1];
    f.read_exact(&mut b).unwrap();
    b[0] ^= 0x5A;
    f.seek(SeekFrom::Start(offset as u64)).unwrap();
    f.write_all(&b).unwrap();
    f.sync_all().unwrap();
}

#[test]
fn a_rotted_shard_is_refused_reconstructed_isolated_and_rebuilt() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    // Three EC targets for 2+1. The spare the rebuild lands on (index 3)
    // joins only once the rotted slot is dark, so nothing can rebuild it
    // before the isolated window has been checked.
    let addrs: Vec<SocketAddr> = (0..4).map(|_| pick_addr()).collect();
    let dirs: Vec<tempfile::TempDir> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    for i in 0..3 {
        let disk = format_node(mgr_addr, addrs[i], &format!("uuid-ecrot-{i}"));
        start_extent_node_with_manager(addrs[i], dirs[i].path().to_path_buf(), disk, mgr_addr);
    }

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .expect("mgr");
        let mut node_dir: Vec<(u64, PathBuf)> = Vec::new();
        for i in 0..3 {
            let r = register_node(&mgr, &addrs[i].to_string(), &format!("uuid-ecrot-{i}")).await;
            node_dir.push((r.node_id, dirs[i].path().to_path_buf()));
        }
        let dir_of = |node_dir: &[(u64, PathBuf)], nid: u64| -> PathBuf {
            node_dir
                .iter()
                .find(|(n, _)| *n == nid)
                .map(|(_, d)| d.clone())
                .unwrap_or_else(|| panic!("node {nid} is not one of ours"))
        };

        let resp = mgr
            .call(
                MSG_CREATE_STREAM,
                rkyv_encode(&CreateStreamReq {
                    replicates: 2,
                    ec_data_shard: 2,
                    ec_parity_shard: 1,
                }),
            )
            .await
            .unwrap();
        let created: CreateStreamResp = rkyv_decode(&resp).unwrap();
        assert_eq!(created.code, CODE_OK, "create_stream: {}", created.message);
        let stream_id = created.stream.expect("stream").stream_id;

        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(&mgr_addr.to_string(), "owner/ec-rot/0".into(), 256 << 20, pool)
            .await
            .expect("stream client");

        // Each data shard is a little over 2 MiB: two whole 1 MiB blocks plus a
        // short tail, so a whole-block read and a sub-block read both exist.
        const N: usize = 4 * 1024 * 1024 + 1000;
        let payload: Vec<u8> = (0..N).map(|i| ((i * 13) % 251) as u8).collect();
        let r = sc.append(stream_id, &payload).await.expect("append");
        let extent_id = r.extent_id;
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
            .expect("seal");
        let seal: StreamAllocExtentResp = rkyv_decode(&resp).expect("decode seal");
        assert_eq!(seal.code, CODE_OK, "seal: {}", seal.message);

        let resp = mgr
            .call(MSG_FORCE_EC_CONVERT, rkyv_encode(&ForceEcConvertReq { extent_id }))
            .await
            .expect("force_ec");
        let f: ForceEcConvertResp = rkyv_decode(&resp).expect("decode force_ec");
        assert_eq!(f.code, CODE_OK, "force_ec_convert: {}", f.message);
        let mut layout = None;
        for _ in 0..30 {
            compio::time::sleep(Duration::from_secs(1)).await;
            let e = extent_info(&mgr, extent_id).await;
            if e.ec_converted {
                layout = Some(e);
                break;
            }
        }
        let layout = layout.expect("the extent never converted to EC");
        assert_eq!(layout.avali, 0b111, "every shard serves after the flip");

        // Data shard 0's holder: its shard covers payload [0, shard_len).
        let victim = layout.replicates[0];
        let victim_dir = dir_of(&node_dir, victim);
        let shard_name = format!("extent-{extent_id}.shard0");
        let shard_path = find_file(&victim_dir, &shard_name).expect("victim's shard file");
        let clean_shard = std::fs::read(&shard_path).expect("read shard");
        let ck_path = find_file(&victim_dir, &format!("{shard_name}.ck"));
        assert!(
            ck_path.is_some(),
            "the shard was not described while it was staged, so the rot below would be \
             recorded as truth by the first backfill instead of caught"
        );
        assert_eq!(&clean_shard[..64], &payload[..64], "shard 0 is the payload's first slice");

        sc.invalidate_extent_cache(extent_id);
        let (got, _) = sc
            .read_bytes_from_extent(extent_id, 0, N as u64)
            .await
            .expect("clean read");
        assert!(got == payload, "precondition: the converted extent reads back clean");

        // Rot one byte in block 1 of shard 0 (payload offset 1.5 MiB).
        let rot_at = 1024 * 1024 + 512 * 1024;
        flip_byte(&shard_path, rot_at);

        // (1) Its node refuses a whole-block read of it.
        let victim_addr = addrs[node_dir.iter().position(|(n, _)| *n == victim).unwrap()];
        let en = RpcClient::connect_as(victim_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .expect("victim EN");
        let req = extent_rpc::ReadBytesReq::new(
            extent_id,
            layout.eversion,
            1024 * 1024,
            1024 * 1024,
            extent_rpc::PayloadRef::for_extent(extent_rpc::PayloadLocation::InShardFile, 0),
        );
        match en.call(extent_rpc::MSG_READ_BYTES, req.encode()).await {
            Err(e) => assert!(
                e.to_string().contains("fails its content checksum"),
                "refused for some other reason: {e}"
            ),
            Ok(resp) => {
                let direct = extent_rpc::ReadBytesResp::decode(resp).expect("decode");
                assert_ne!(
                    direct.code,
                    extent_rpc::CODE_OK,
                    "the rotted shard's node served a block that fails its checksum"
                );
            }
        }

        // (2) A client reading through it gets the right bytes: the refused
        // shard is reconstructed from the other data shard and the parity.
        sc.invalidate_extent_cache(extent_id);
        let (got, _) = sc
            .read_bytes_from_extent(extent_id, 1024 * 1024, 1024 * 1024)
            .await
            .expect("read over the rotted block");
        assert!(
            got == payload[1024 * 1024..2 * 1024 * 1024],
            "the client was handed the rotted shard's bytes"
        );

        // (3) With nobody reading, the victim's own scrub finds it and the
        // manager isolates exactly that slot.
        let victim_bit = 1u32
            << layout
                .replicates
                .iter()
                .chain(layout.parity.iter())
                .position(|n| *n == victim)
                .unwrap();
        let mut isolated = false;
        for _ in 0..60 {
            compio::time::sleep(Duration::from_millis(500)).await;
            let e = extent_info(&mgr, extent_id).await;
            if e.avali & victim_bit == 0 {
                assert_eq!(
                    e.avali,
                    0b111 & !victim_bit,
                    "exactly the victim's slot should have gone dark"
                );
                isolated = true;
                break;
            }
        }
        assert!(isolated, "the rotted shard was never isolated");

        // While it is dark nothing may read it — not even the sub-block read no
        // checksum covers, which is exactly where the rotted byte is. There is
        // no node to rebuild onto yet, so the slot stays dark for this.
        sc.invalidate_extent_cache(extent_id);
        let (got, _) = sc
            .read_bytes_from_extent(extent_id, rot_at as u64, 64)
            .await
            .expect("sub-block read while isolated");
        assert!(
            got == payload[rot_at..rot_at + 64],
            "a sub-block read was served from the isolated shard"
        );
        let still = extent_info(&mgr, extent_id).await;
        assert!(
            still.replicates[0] == victim && still.avali & victim_bit == 0,
            "precondition: the slot was still isolated, not yet rebuilt, during that read"
        );

        // (4) A spare joins, and recovery rebuilds the shard on it.
        let r = register_node(&mgr, &addrs[3].to_string(), "uuid-ecrot-3").await;
        let disk = r
            .disk_uuids
            .iter()
            .find(|(u, _)| u == "uuid-ecrot-3")
            .map(|(_, d)| *d)
            .expect("spare disk id");
        start_extent_node_with_manager(addrs[3], dirs[3].path().to_path_buf(), disk, mgr_addr);
        node_dir.push((r.node_id, dirs[3].path().to_path_buf()));
        let mut rebuilt = None;
        for _ in 0..120 {
            compio::time::sleep(Duration::from_millis(500)).await;
            let e = extent_info(&mgr, extent_id).await;
            if e.replicates[0] != victim && e.avali == 0b111 {
                rebuilt = Some(e);
                break;
            }
        }
        let rebuilt = rebuilt.expect("the isolated shard was never rebuilt on the spare");
        let new_holder = rebuilt.replicates[0];
        assert_eq!(new_holder, r.node_id, "rebuilt somewhere other than the spare");

        let new_dir = dir_of(&node_dir, new_holder);
        let new_shard = find_file(&new_dir, &shard_name).expect("rebuilt shard file");
        assert!(
            std::fs::read(&new_shard).unwrap() == clean_shard,
            "the rebuilt shard is not the original shard byte for byte"
        );
        // Described by the rebuild itself (unit-tested in
        // `a_durable_shard_is_recorded_at_its_length`), or at the latest by
        // the scrub's backfill — either way it is checkable from here on.
        assert!(
            find_file(&new_dir, &format!("{shard_name}.ck")).is_some(),
            "the rebuilt shard has no description"
        );

        sc.invalidate_extent_cache(extent_id);
        let (got, _) = sc
            .read_bytes_from_extent(extent_id, 0, N as u64)
            .await
            .expect("read after rebuild");
        assert!(got == payload, "the extent does not read back clean after the rebuild");
    });
}
