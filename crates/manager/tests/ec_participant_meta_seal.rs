/// EC conversion participants must end up with a SEALED `.meta`, not only a
/// payload-location byte.
///
/// Staging deliberately writes no `.meta` (an abandoned CoW attempt must cost
/// only deleted files), so the manager's layout flip is the first point a
/// shard holder can durably learn the extent's seal. The reconcile placement
/// application (`apply_placements`) used to persist ONLY `payload_location`
/// there, and `save_meta` writes the live atomics — which on a participant
/// were never sealed. Every shard holder except the coordinator then kept
/// `sealed=0 / sealed_length=0 / eversion=1` under a live shard file FOREVER
/// (observed on a live cluster: extent sealed at 17 GiB, four of five holders
/// with all-zero seal fields), and a restart loaded the shard's extent as an
/// OPEN extent at eversion 1. This test drives a real 2+1 conversion and then
/// asserts every holder's on-disk `.meta` carries the manager's seal.
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::mpsc::{channel, Sender, TryRecvError};
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::extent_rpc::PAYLOAD_LOCATION_IN_SHARD_FILE;
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ConnPool, ExtentNode, ExtentNodeConfig, StreamClient};

fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr
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

enum NodeCmd {
    /// Run the reconcile placement application with these
    /// `(extent_id, payload_location, shard_index)` verdicts, then ack.
    ApplyPlacements(Vec<(u64, u8, u32)>, Sender<()>),
}

/// Start an extent node that also services test commands on its own runtime,
/// so the test can drive `apply_placements` directly (the production trigger
/// is the 5-minute orphan-reconcile sweep — too slow for a test).
fn start_extent_node_with_cmds(
    addr: SocketAddr,
    dir: PathBuf,
    disk_id: u64,
    mgr: &str,
) -> Sender<NodeCmd> {
    let mgr = mgr.to_string();
    let (tx, rx) = channel::<NodeCmd>();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async move {
            let config = ExtentNodeConfig::new(dir, disk_id).with_manager_endpoint(mgr);
            let node = ExtentNode::new(config).await.expect("extent node");
            let serve_node = node.clone();
            compio::runtime::spawn(async move {
                let _ = serve_node.serve(addr).await;
            })
            .detach();
            loop {
                match rx.try_recv() {
                    Ok(NodeCmd::ApplyPlacements(placements, done)) => {
                        node.test_apply_placements(&placements, node.test_staging_tick())
                            .await;
                        let _ = done.send(());
                    }
                    Err(TryRecvError::Empty) => {
                        compio::time::sleep(Duration::from_millis(20)).await;
                    }
                    Err(TryRecvError::Disconnected) => break,
                }
            }
        });
    });
    std::thread::sleep(Duration::from_millis(200));
    tx
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
        .expect("register node");
    rkyv_decode::<RegisterNodeResp>(&resp)
        .expect("decode")
        .node_id
}

async fn get_extent_info(mgr: &RpcClient, extent_id: u64) -> MgrExtentInfo {
    let resp = mgr
        .call(MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id }))
        .await
        .expect("extent_info");
    let info: ExtentInfoResp = rkyv_decode(&resp).expect("decode ExtentInfoResp");
    info.extent.expect("extent info")
}

/// Locate `extent-{id}.meta` under a node's hashed data layout.
fn find_meta(dir: &Path, extent_id: u64) -> Option<PathBuf> {
    let name = format!("extent-{extent_id}.meta");
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.file_name().is_some_and(|f| f == name.as_str()) {
                return Some(p);
            }
        }
    }
    None
}

/// Parse the V2 `.meta` fields this test asserts on:
/// bytes 16–23 `sealed_length`, 24–31 `eversion`, 40 `sealed`.
fn parse_meta_seal(path: &Path) -> (u64, u64, u8) {
    let buf = std::fs::read(path).expect("read .meta");
    assert!(buf.len() >= 48, "meta too short: {} bytes", buf.len());
    let sealed_length = u64::from_le_bytes(buf[16..24].try_into().unwrap());
    let eversion = u64::from_le_bytes(buf[24..32].try_into().unwrap());
    (sealed_length, eversion, buf[40])
}

#[test]
fn ec_flip_persists_seal_on_every_shard_holder() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();

    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let mgr_str = mgr_addr.to_string();

    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n3_addr = pick_addr();

    let dirs = [
        d1.path().to_path_buf(),
        d2.path().to_path_buf(),
        d3.path().to_path_buf(),
    ];
    // `Option` slots, so one node can be stopped (drop its Sender -> its command
    // loop hits Disconnected -> the runtime drops and the listener closes)
    // without shifting the indices the rest of the test routes by.
    let mut txs: Vec<Option<Sender<NodeCmd>>> = vec![
        Some(start_extent_node_with_cmds(n1_addr, dirs[0].clone(), 1, &mgr_str)),
        Some(start_extent_node_with_cmds(n2_addr, dirs[1].clone(), 2, &mgr_str)),
        Some(start_extent_node_with_cmds(n3_addr, dirs[2].clone(), 3, &mgr_str)),
    ];

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");

        let node_ids = [
            register_node(&mgr, &n1_addr.to_string(), "disk-pm-1").await,
            register_node(&mgr, &n2_addr.to_string(), "disk-pm-2").await,
            register_node(&mgr, &n3_addr.to_string(), "disk-pm-3").await,
        ];

        // 2-replica open extents, EC 2+1 on seal — 3 nodes host K+M exactly.
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
        let stream_id = created.stream.as_ref().unwrap().stream_id;
        let extent_id = created.stream.as_ref().unwrap().extent_ids[0];

        let pool = Rc::new(ConnPool::new());
        let client = StreamClient::connect(
            &mgr_str,
            "owner/ec-meta-seal/0".to_string(),
            256 * 1024 * 1024,
            pool,
        )
        .await
        .expect("stream client");

        let payload: Vec<u8> = (0..8192u16).map(|i| (i % 251) as u8).collect();
        let result = client.append(stream_id, &payload).await.expect("append");
        assert_eq!(result.extent_id, extent_id);

        // Seal the tail, then force the EC conversion.
        let seal_resp = mgr
            .call(
                MSG_STREAM_ALLOC_EXTENT,
                rkyv_encode(&StreamAllocExtentReq {
                    stream_id,
                    owner_key: client.owner_key().to_string(),
                    owner_epoch: client.owner_epoch(),
                    seal_commit: Some(result.end),
                    exclude_node_ids: vec![],
                    seal_extent_id: 0,
                }),
            )
            .await
            .unwrap();
        let seal_info: StreamAllocExtentResp = rkyv_decode(&seal_resp).unwrap();
        assert_eq!(seal_info.code, CODE_OK, "seal failed: {}", seal_info.message);

        let force_resp = mgr
            .call(
                MSG_FORCE_EC_CONVERT,
                rkyv_encode(&ForceEcConvertReq { extent_id }),
            )
            .await
            .expect("force-ec-convert");
        let force: ForceEcConvertResp = rkyv_decode(&force_resp).expect("decode");
        assert_eq!(force.code, CODE_OK, "force-ec-convert: {}", force.message);

        let mut ex = None;
        for _ in 0..15 {
            compio::time::sleep(Duration::from_secs(2)).await;
            let e = get_extent_info(&mgr, extent_id).await;
            if e.ec_converted {
                ex = Some(e);
                break;
            }
        }
        let ex = ex.expect("EC conversion did not happen within 30s");
        assert!(ex.sealed_length > 0, "converted extent must be sealed");

        // Let the coordinator's in-flight bookkeeping settle before the sweep.
        compio::time::sleep(Duration::from_secs(1)).await;

        // Drive each holder's reconcile placement application — the production
        // trigger is the periodic orphan-reconcile sweep answering with this
        // exact verdict. Shard index = position in replicates ++ parity.
        let shard_holders: Vec<u64> = ex
            .replicates
            .iter()
            .chain(ex.parity.iter())
            .copied()
            .collect();
        for (shard_index, holder) in shard_holders.iter().enumerate() {
            let ni = node_ids
                .iter()
                .position(|id| id == holder)
                .unwrap_or_else(|| panic!("holder node {holder} not in registered set"));
            let (ack_tx, ack_rx) = channel();
            txs[ni]
                .as_ref()
                .unwrap()
                .send(NodeCmd::ApplyPlacements(
                    vec![(extent_id, PAYLOAD_LOCATION_IN_SHARD_FILE, shard_index as u32)],
                    ack_tx,
                ))
                .expect("send ApplyPlacements");
            ack_rx
                .recv_timeout(Duration::from_secs(30))
                .expect("apply_placements did not finish");
        }

        // Every shard holder's on-disk `.meta` must now carry the seal the
        // manager published — not just the payload-location byte. The seal
        // flag is asserted for ALL holders first, so a red run reports the
        // headline defect (a participant with sealed=0) rather than the
        // coordinator's milder stale-eversion symptom.
        let metas: Vec<(u64, u64, u64, u8)> = shard_holders
            .iter()
            .map(|holder| {
                let ni = node_ids.iter().position(|id| id == holder).unwrap();
                let meta = find_meta(&dirs[ni], extent_id)
                    .unwrap_or_else(|| panic!("no .meta for extent {extent_id} on node {holder}"));
                let (sealed_length, eversion, sealed) = parse_meta_seal(&meta);
                (*holder, sealed_length, eversion, sealed)
            })
            .collect();
        eprintln!(
            "manager: sealed_length={} eversion={} replicates={:?} parity={:?}",
            ex.sealed_length, ex.eversion, ex.replicates, ex.parity
        );
        for (holder, sealed_length, eversion, sealed) in &metas {
            eprintln!(
                "holder {holder}: .meta sealed={sealed} sealed_length={sealed_length} eversion={eversion}"
            );
        }
        for (holder, _, _, sealed) in &metas {
            assert_eq!(
                *sealed, 1,
                "node {holder}: shard holder's .meta must be SEALED after the flip \
                 (got sealed=0 — the holder would reload as an OPEN extent)"
            );
        }
        for (holder, sealed_length, eversion, _) in &metas {
            assert_eq!(
                *sealed_length, ex.sealed_length,
                "node {holder}: .meta sealed_length must match the manager's"
            );
            assert_eq!(
                *eversion, ex.eversion,
                "node {holder}: .meta eversion must match the manager's post-flip eversion"
            );
        }

        // Read the bytes back BEFORE anything is taken down: the conversion must
        // not have disturbed them. This has to precede the restart below, which
        // moves a shard holder to a port the manager does not know — a gather
        // that still routes to the old address just times out, which would say
        // nothing about the conversion.
        let (got, _) = client
            .read_bytes_from_extent(extent_id, 0, payload.len() as u64)
            .await
            .expect("read back after conversion");
        assert_eq!(
            got, payload,
            "converted extent must still serve the original bytes"
        );

        // What the restarted holder believes about this extent.
        //
        // Restarting is the load-bearing part. Probing the node that is still
        // running proves nothing: it learned the seal in memory during the
        // conversion, so it answers from `entry.sealed` whatever the `.meta`
        // says. Only a process that had to reload from disk can tell the two
        // apart — the `.dat` was reclaimed at the flip, so this extent comes
        // back as a shard-only entry whose state is read entirely from `.meta`.
        //
        // Same directory, fresh port: the probe talks to the node directly, so
        // the manager need not learn about the new process, and a new port
        // avoids racing the old listener's teardown.
        let coordinator = ex.replicates[0];
        let victim = *shard_holders
            .iter()
            .find(|h| **h != coordinator)
            .expect("a participant distinct from the coordinator");
        let vi = node_ids.iter().position(|id| *id == victim).unwrap();
        drop(txs[vi].take());
        std::thread::sleep(Duration::from_millis(600));
        let restart_addr = pick_addr();
        let _restarted = start_extent_node_with_cmds(
            restart_addr,
            dirs[vi].clone(),
            (vi + 1) as u64,
            &mgr_str,
        );

        {
            let holder = &victim;
            let node = RpcClient::connect(restart_addr)
                .await
                .expect("connect restarted holder");

            // MSG_PROBE_EXTENT is the assertion that actually discriminates,
            // because it answers from local atomics only — no fence check, no
            // manager fetch, so nothing can heal the state out from under it.
            // A sealed extent reports its `sealed_length`; the pre-fix
            // participant reloads as OPEN and reports 0.
            let pr = node
                .call(
                    autumn_rpc::extent_rpc::MSG_PROBE_EXTENT,
                    autumn_rpc::extent_rpc::ProbeExtentReq { extent_id }.encode(),
                )
                .await
                .expect("probe rpc to restarted holder");
            let pr = autumn_rpc::extent_rpc::ProbeExtentResp::decode(pr)
                .expect("decode ProbeExtentResp");
            assert_eq!(
                pr.code,
                autumn_rpc::extent_rpc::CODE_OK,
                "holder {holder}: restarted node does not know the extent at all"
            );
            assert_eq!(
                pr.length, ex.sealed_length,
                "holder {holder}: a restarted shard holder must reload the extent as SEALED at the \
                 manager's length (0 means it came back OPEN — the defect)"
            );
            let ap = node
                .call(
                    autumn_rpc::extent_rpc::MSG_APPEND,
                    autumn_rpc::extent_rpc::AppendReq {
                        extent_id,
                        eversion: ex.eversion,
                        commit: 0,
                        owner_epoch: 0,
                        payload: vec![0xABu8; 64].into(),
                    }
                    .encode(),
                )
                .await
                .expect("append rpc to holder");
            let ap = autumn_rpc::extent_rpc::AppendResp::decode(ap).expect("decode AppendResp");
            // The ledger's other clause. Weaker than the probe above and worth
            // knowing why: with the caller's eversion AHEAD of the reloaded one,
            // as it is here, the append path refreshes from the manager before
            // reaching the seal gate, so this answers CODE_PRECONDITION with or
            // without the fix. It pins the contract, not the regression.
            assert_eq!(
                ap.code,
                autumn_rpc::extent_rpc::CODE_PRECONDITION,
                "holder {holder}: a RESTARTED shard holder must refuse an append"
            );
        }

    });
}
