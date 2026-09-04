//! A given-up EC conversion's staging is reclaimed by the ordinary reconcile.
//!
//! When a conversion fails or is abandoned the manager releases its marker
//! WITHOUT flipping the layout, so the extent stays replicated: `.dat` present,
//! and on every participant the shard file it already staged. That shard is
//! named by nothing. Neither existing sweep collects it — `remove_extent_files`
//! waits for the whole extent to be deleted, and the orphan reconcile's other
//! leg only fires once the `.dat` is already gone — and the node-side guard that
//! protects a LIVE attempt used to skip the extent outright, so the bytes were
//! held for the rest of that extent node's life.
//!
//! This drives the REAL reconcile round rather than the applier, because that is
//! the only way to see the whole chain hold: the node identifies itself, the
//! manager resolves the reporter and answers `InDat` for an extent it is a
//! member of, and the applier reaches the staged file. The unit tests supply a
//! placement by hand and cannot show any of that.
//!
//! What this test does NOT pin is the ORDERING that makes the delete safe — the
//! staging tick is sampled before the request goes out, so a verdict is acted on
//! only if no staging landed while the manager was answering. Moving that sample
//! later would still pass here, because here nothing stages concurrently. That
//! direction is pinned by `a_stale_placement_must_not_delete_a_shard_being_staged`
//! (autumn-stream, `placement_cleanup`), which hands the applier a sample taken
//! before the staging on purpose.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::mpsc::{channel, Sender, TryRecvError};
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::extent_rpc::{WriteShardReq, WriteShardResp, CODE_OK, MSG_WRITE_SHARD};
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
    /// Run one full reconcile round against the manager — the production
    /// trigger is a 5-minute sweep, too slow for a test — then ack.
    ReconcileOnce(Sender<()>),
}

/// Start an extent node that knows its own identity (so the manager can resolve
/// the reporter of a reconcile) and services test commands on its own runtime.
fn start_extent_node(
    addr: SocketAddr,
    dir: PathBuf,
    disk_id: u64,
    uuid: &str,
    mgr: &str,
) -> Sender<NodeCmd> {
    let mgr = mgr.to_string();
    let uuid = uuid.to_string();
    let (tx, rx) = channel::<NodeCmd>();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async move {
            let config = ExtentNodeConfig::new(dir, disk_id)
                .with_manager_endpoint(mgr)
                .with_registration(&uuid, addr.to_string(), vec![]);
            let node = ExtentNode::new(config).await.expect("extent node");
            let serve_node = node.clone();
            compio::runtime::spawn(async move {
                let _ = serve_node.serve(addr).await;
            })
            .detach();
            loop {
                match rx.try_recv() {
                    Ok(NodeCmd::ReconcileOnce(done)) => {
                        node.test_reconcile_once().await;
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

async fn register_node(mgr: &RpcClient, addr: &str, disk: &str, uuid: &str) -> u64 {
    let resp = mgr
        .call(
            MSG_REGISTER_NODE,
            rkyv_encode(&RegisterNodeReq {
                addr: addr.to_string(),
                disk_uuids: vec![disk.to_string()],
                shard_ports: vec![],
                control_address: String::new(),
                node_uuid: uuid.to_string(),
            }),
        )
        .await
        .expect("register node");
    rkyv_decode::<RegisterNodeResp>(&resp)
        .expect("decode")
        .node_id
}

/// Does any `extent-{id}.shard*` exist anywhere under this node's data dir?
fn staged_shard_present(dir: &std::path::Path, extent_id: u64) -> bool {
    any_file_starting_with(dir, &format!("extent-{extent_id}.shard"))
}

/// Does this node hold the extent's replicated payload?
fn dat_present(dir: &std::path::Path, extent_id: u64) -> bool {
    any_file_starting_with(dir, &format!("extent-{extent_id}.dat"))
}

fn any_file_starting_with(dir: &std::path::Path, prefix: &str) -> bool {
    let Ok(top) = std::fs::read_dir(dir) else {
        return false;
    };
    for sub in top.flatten() {
        let p = sub.path();
        if !p.is_dir() {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&p) else {
            continue;
        };
        for f in entries.flatten() {
            if f.file_name().to_string_lossy().starts_with(prefix) {
                return true;
            }
        }
    }
    false
}

#[test]
fn an_abandoned_conversions_staging_is_reclaimed_by_the_next_reconcile() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let mgr_str = mgr_addr.to_string();

    let addrs: Vec<SocketAddr> = (0..3).map(|_| pick_addr()).collect();
    let uuids = ["uuid-stg-1", "uuid-stg-2", "uuid-stg-3"];
    let txs: Vec<_> = (0..3)
        .map(|i| {
            start_extent_node(
                addrs[i],
                dirs[i].path().to_path_buf(),
                (i + 1) as u64,
                uuids[i],
                &mgr_str,
            )
        })
        .collect();

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        for i in 0..3 {
            register_node(
                &mgr,
                &addrs[i].to_string(),
                &format!("disk-stg-{i}"),
                uuids[i],
            )
            .await;
        }

        let resp = mgr
            .call(
                MSG_CREATE_STREAM,
                rkyv_encode(&CreateStreamReq {
                    replicates: 3,
                    ec_data_shard: 2,
                    ec_parity_shard: 1,
                }),
            )
            .await
            .unwrap();
        let created: CreateStreamResp = rkyv_decode(&resp).unwrap();
        let stream = created.stream.expect("stream");
        let stream_id = stream.stream_id;
        let extent_id = stream.extent_ids[0];

        let pool = Rc::new(ConnPool::new());
        let client = StreamClient::connect(
            &mgr_str,
            "owner/ec-staging-reclaim/0".to_string(),
            256 * 1024 * 1024,
            pool,
        )
        .await
        .expect("stream client");
        let payload: Vec<u8> = (0..32768u32).map(|i| (i % 251) as u8).collect();
        let appended = client.append(stream_id, &payload).await.expect("append");
        assert_eq!(appended.extent_id, extent_id);

        // Pick a holder by what is on disk rather than by node id: the id the
        // manager assigns is not the order these were started in, and the test
        // needs the ADDRESS anyway.
        let victim = (0..3)
            .find(|i| dat_present(dirs[*i].path(), extent_id))
            .expect("no node holds the extent's .dat");

        // A conversion stages this node's shard through the real participant
        // path — the same RPC a coordinator sends — and is then given up, which
        // on the manager means the marker is released with the layout still
        // pointing at `.dat`. Nothing about the file records that it was
        // abandoned; it just stops being anybody's.
        let node = RpcClient::connect(addrs[victim]).await.expect("connect en");
        let staged: Vec<u8> = vec![0xC7; 16384];
        let resp = node
            .call(
                MSG_WRITE_SHARD,
                WriteShardReq {
                    extent_id,
                    shard_index: 0,
                    sealed_length: payload.len() as u64,
                    eversion: 2,
                    owner_epoch: 0,
                    shard_offset: 0,
                    attempt_nonce: 4242,
                    payload: staged.clone().into(),
                }
                .encode(),
            )
            .await
            .expect("write_shard");
        assert_eq!(
            WriteShardResp::decode(resp).expect("decode").code,
            CODE_OK,
            "the participant must accept the staging"
        );
        assert!(
            staged_shard_present(dirs[victim].path(), extent_id),
            "precondition: the staging did not land"
        );

        // One ordinary reconcile round, asked AFTER the staging exists.
        let (done_tx, done_rx) = channel();
        txs[victim]
            .send(NodeCmd::ReconcileOnce(done_tx))
            .expect("send reconcile");
        done_rx.recv_timeout(Duration::from_secs(20)).expect("reconcile");

        assert!(
            !staged_shard_present(dirs[victim].path(), extent_id),
            "the abandoned attempt's staging survived a reconcile round — it is \
             named by no layout and nothing else will ever collect it"
        );
        // The `.dat` on THIS node must survive: it is what the verdict named.
        // Without this the test would also pass if the round had condemned the
        // whole extent as garbage — which deletes the shard too, for the wrong
        // reason, and the read below could still be served by a sibling.
        assert!(
            dat_present(dirs[victim].path(), extent_id),
            "the reconcile deleted the payload the layout points at"
        );

        // And the extent is untouched: the payload the cluster is actually
        // pointed at still reads back byte-exact, through the node that just
        // had a file deleted under it.
        let (got, _) = client
            .read_committed_bytes_from_extent(extent_id, 0, payload.len() as u64)
            .await
            .expect("read after reclaim");
        assert_eq!(got, payload, "reclaiming staging disturbed the .dat");
    });
}
