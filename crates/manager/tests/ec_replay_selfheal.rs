//! A WAL replay whose cached extent layout went stale under an EC conversion
//! must heal itself.
//!
//! `read_committed_bytes_from_extent` is the read a partition uses to replay its
//! log stream at open, 64 MiB at a time, and a log stream's sealed extents are
//! EC-encoded by default — so a replay meets EC-converted extents routinely, and
//! most often on exactly the partitions that have been down longest.
//!
//! It used to recover from one failure class, `EversionStale`, and that is the
//! one the node does not send here: `read_plan` tests `holds_payload` BEFORE
//! eversion, so a client holding a pre-conversion `payload_location` is answered
//! `PAYLOAD_NOT_HERE`.
//!
//! What that cost is worth stating exactly, because the halves differ. On the
//! REPLICATED path — the one this test drives — `read_err_fail_fast` already
//! evicted the layout cache for any non-eversion error, so `recover_partition`'s
//! own ten-attempt loop healed on its next try: one warn and one 2 s stall per
//! stale extent, not a wedge. On the EC path `ec_subrange_read` never calls
//! that, so nothing evicted anything and the failure repeated across all ten
//! attempts. Handling the classes in the loop heals both without leaving the
//! call, and this test pins the cheaper half because it is the one that can be
//! driven deterministically.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::mpsc::{channel, Sender, TryRecvError};
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::extent_rpc::PAYLOAD_LOCATION_IN_SHARD_FILE;
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ConnPool, ExtentNode, ExtentNodeConfig, StreamClient};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Counts the loop's own "refreshing extent layout" events.
///
/// The acceptance asks for a COUNTED retry, not just an eventual success: a heal
/// that fired on every call would satisfy "it worked" while still paying a
/// wasted round trip per replay chunk. Nothing in the client exposes a retry
/// counter, so count the event the loop emits — no production code bent for a
/// test, and the subscriber is scoped to this thread.
#[derive(Clone, Default)]
struct RetryCounter(std::sync::Arc<AtomicUsize>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for RetryCounter {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        struct Find(bool);
        impl tracing::field::Visit for Find {
            fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                if f.name() == "message" && format!("{v:?}").contains("refreshing extent layout") {
                    self.0 = true;
                }
            }
        }
        let mut find = Find(false);
        event.record(&mut find);
        if find.0 {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

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

/// Warm the client's layout cache, convert the extent underneath it, then replay.
#[test]
fn a_replay_read_heals_a_layout_that_went_stale_under_ec() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();

    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let mgr_str = mgr_addr.to_string();

    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n3_addr = pick_addr();
    let txs = [
        start_extent_node_with_cmds(n1_addr, d1.path().to_path_buf(), 1, &mgr_str),
        start_extent_node_with_cmds(n2_addr, d2.path().to_path_buf(), 2, &mgr_str),
        start_extent_node_with_cmds(n3_addr, d3.path().to_path_buf(), 3, &mgr_str),
    ];

    let retries = RetryCounter::default();
    let counted = retries.0.clone();
    use tracing_subscriber::layer::SubscriberExt;
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(retries));

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        let node_ids = [
            register_node(&mgr, &n1_addr.to_string(), "disk-rs-1").await,
            register_node(&mgr, &n2_addr.to_string(), "disk-rs-2").await,
            register_node(&mgr, &n3_addr.to_string(), "disk-rs-3").await,
        ];

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
            "owner/ec-replay-selfheal/0".to_string(),
            256 * 1024 * 1024,
            pool,
        )
        .await
        .expect("stream client");

        let payload: Vec<u8> = (0..32768u32).map(|i| (i % 251) as u8).collect();
        let result = client.append(stream_id, &payload).await.expect("append");
        assert_eq!(result.extent_id, extent_id);

        // Warm the layout cache while the extent is still replicated. This is
        // what a long-lived StreamClient has, and it is the whole setup: the
        // cached `payload_location` is about to stop being true.
        let (pre, _) = client
            .read_committed_bytes_from_extent(extent_id, 0, payload.len() as u64)
            .await
            .expect("pre-conversion replay read");
        assert_eq!(pre, payload, "pre-conversion read must be byte-exact");

        // Seal the tail, then convert it, without telling the client.
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

        let mut converted = None;
        for _ in 0..15 {
            compio::time::sleep(Duration::from_secs(2)).await;
            let e = get_extent_info(&mgr, extent_id).await;
            if e.ec_converted {
                converted = Some(e);
                break;
            }
        }
        let ex = converted.expect("EC conversion did not happen within 30s");
        compio::time::sleep(Duration::from_secs(1)).await;

        // Drive each holder's placement application. This is the step that makes
        // the client's cached layout actually WRONG rather than merely old: it
        // moves the payload into the shard file and reclaims the `.dat` the
        // cached layout still points at. Without it the stale read finds the
        // old file intact and succeeds, which proves nothing. In production the
        // periodic orphan-reconcile sweep is what issues this verdict.
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
                .unwrap_or_else(|| panic!("holder node {holder} not registered"));
            let (ack_tx, ack_rx) = channel();
            txs[ni]
                .send(NodeCmd::ApplyPlacements(
                    vec![(extent_id, PAYLOAD_LOCATION_IN_SHARD_FILE, shard_index as u32)],
                    ack_tx,
                ))
                .expect("send ApplyPlacements");
            ack_rx
                .recv_timeout(Duration::from_secs(30))
                .expect("apply_placements did not finish");
        }

        // The replay read, on the SAME client, whose cache now describes a
        // layout that no longer exists. Without the fix this is where it fails
        // and keeps failing.
        let (got, end) = client
            .read_committed_bytes_from_extent(extent_id, 0, payload.len() as u64)
            .await
            .expect("replay read must heal the stale layout, not surface the refusal");
        assert_eq!(got, payload, "replay read after conversion must be byte-exact");
        assert_eq!(
            end,
            payload.len() as u64,
            "committed end must be the extent's, not a shard's"
        );

        // EXACTLY one retry got us here — not "it eventually worked".
        assert_eq!(
            counted.load(Ordering::SeqCst),
            1,
            "the stale layout must cost exactly one refresh"
        );

        // And the refresh STUCK: the chunks a real replay reads next must cost
        // no further refreshes, which the count after this loop asserts.
        for chunk in 0..3u64 {
            let off = chunk * 8192;
            let (part, _) = client
                .read_committed_bytes_from_extent(extent_id, off, 8192)
                .await
                .unwrap_or_else(|e| panic!("replay chunk at {off} failed after the heal: {e:#}"));
            assert_eq!(
                part,
                &payload[off as usize..off as usize + 8192],
                "replay chunk at {off} came back wrong"
            );
        }
        assert_eq!(
            counted.load(Ordering::SeqCst),
            1,
            "later replay chunks must not each pay their own refresh"
        );
    });
}
