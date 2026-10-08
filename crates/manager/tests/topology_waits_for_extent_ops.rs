//! A split or merge does not hold writes frozen while an EC conversion or a
//! recovery runs on its partitions' extents: the manager waits for those
//! before anything freezes, and one that appears after a split's wait makes
//! the PS abort at once.

mod support;

use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::mpsc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    self, OpQueryReq, OpQueryResp, OpRecord, OpSubmitReq, OpSubmitResp, StreamInfoReq,
    StreamInfoResp, MSG_OP_QUERY, MSG_OP_SUBMIT, MSG_STREAM_INFO, OP_KIND_MERGE, OP_KIND_SPLIT,
    OP_STATE_FAILED, OP_STATE_RUNNING, OP_STATE_SUCCEEDED,
};
use autumn_rpc::version_hello::Role;

use support::*;

/// The split commit pause is process-global; scenarios must not overlap.
static SERIAL: Mutex<()> = Mutex::new(());

enum Marker {
    /// An EC marker on `extent` whose coordinator is `node`.
    Ec { extent: u64, node: u64 },
    Clear(u64),
}

/// A manager on its own thread, taking marker commands from the test thread.
fn start_manager_with_markers(addr: SocketAddr) -> mpsc::Sender<Marker> {
    let (tx, rx) = mpsc::channel::<Marker>();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async move {
            let m = AutumnManager::new();
            let mc = m.clone();
            compio::runtime::spawn(async move {
                loop {
                    while let Ok(cmd) = rx.try_recv() {
                        match cmd {
                            Marker::Ec { extent, node } => {
                                mc._test_mark_ec_inflight_by(extent, vec![node])
                            }
                            Marker::Clear(eid) => mc._test_clear_inflight(eid),
                        }
                    }
                    compio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .detach();
            let _ = m.serve(addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));
    tx
}

struct Cluster {
    mgr: Rc<RpcClient>,
    admin: Rc<RpcClient>,
    ps: PsRouter,
    markers: mpsc::Sender<Marker>,
    /// Per partition, in the order given: an extent of its log stream.
    log_extents: Vec<u64>,
    /// Registered, never started: it stays `Suspend`, so a marker naming it
    /// as coordinator is neither released nor dispatched.
    idle_node: u64,
    _dirs: [tempfile::TempDir; 2],
}

async fn log_tail(mgr: &RpcClient, stream_id: u64) -> u64 {
    let resp = mgr
        .call(
            MSG_STREAM_INFO,
            manager_rpc::rkyv_encode(&StreamInfoReq {
                stream_ids: vec![stream_id],
            }),
        )
        .await
        .unwrap();
    let resp: StreamInfoResp = manager_rpc::rkyv_decode(&resp).unwrap();
    *resp.streams[0].1.extent_ids.last().unwrap()
}

/// `parts`: `(part_id, start, end)`, all on one PS.
async fn cluster(parts: &[(u64, &[u8], &[u8])], ps_id: u64) -> Cluster {
    let mgr_addr = pick_addr();
    let markers = start_manager_with_markers(mgr_addr);
    let (n1, n2) = (pick_addr(), pick_addr());
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    start_extent_node(n1, d1.path().to_path_buf(), 1);
    start_extent_node(n2, d2.path().to_path_buf(), 2);
    let mgr = RpcClient::connect(mgr_addr).await.unwrap();
    register_two_nodes(&mgr, n1, n2, 40).await;
    let mut logs = Vec::new();
    for &(part_id, start, end) in parts {
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, part_id, log, row, meta, start, end).await;
        logs.push(log);
    }
    let ps_addr = pick_addr();
    start_partition_server(ps_id, mgr_addr, ps_addr);
    let ps = PsRouter::new(mgr_addr, ps_addr);
    for &(part_id, start, _) in parts {
        for i in 0u32..20 {
            let k = [start, format!("{i:03}").as_bytes()].concat();
            psr_put(&ps, part_id, &k, b"v").await;
        }
        psr_flush(&ps, part_id).await;
    }
    let admin = RpcClient::connect_as(mgr_addr, Role::Admin, None)
        .await
        .unwrap();
    let idle = register_node(&mgr, &pick_addr().to_string(), "idle-disk").await;
    assert_eq!(idle.code, manager_rpc::CODE_OK, "{}", idle.message);
    let mut log_extents = Vec::new();
    for log in logs {
        log_extents.push(log_tail(&mgr, log).await);
    }
    Cluster {
        mgr,
        admin,
        ps,
        markers,
        log_extents,
        idle_node: idle.node_id,
        _dirs: [d1, d2],
    }
}

async fn submit_split(admin: &RpcClient, part_id: u64, at: &[u8]) -> u64 {
    let resp = admin
        .call(
            MSG_OP_SUBMIT,
            manager_rpc::rkyv_encode(&OpSubmitReq {
                kind: OP_KIND_SPLIT,
                part_id,
                at_key: Some(at.to_vec()),
                requested_by: "test".to_string(),
                ..Default::default()
            }),
        )
        .await
        .expect("submit split");
    let r: OpSubmitResp = manager_rpc::rkyv_decode(&resp).unwrap();
    assert_eq!(r.code, manager_rpc::CODE_OK, "split refused: {}", r.message);
    r.op_id
}

async fn op(admin: &RpcClient, op_id: u64) -> OpRecord {
    let resp = admin
        .call(
            MSG_OP_QUERY,
            manager_rpc::rkyv_encode(&OpQueryReq {
                op_id,
                ..Default::default()
            }),
        )
        .await
        .expect("query op");
    let q: OpQueryResp = manager_rpc::rkyv_decode(&resp).unwrap();
    q.ops.into_iter().next().expect("one op")
}

async fn wait_terminal(admin: &RpcClient, op_id: u64, within: Duration) -> OpRecord {
    let start = Instant::now();
    loop {
        let o = op(admin, op_id).await;
        if o.state != OP_STATE_RUNNING && o.state != manager_rpc::OP_STATE_PENDING {
            return o;
        }
        assert!(
            start.elapsed() < within,
            "op {op_id} still active after {within:?}: {}",
            o.message
        );
        compio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn partition_count(mgr: &RpcClient) -> usize {
    get_regions(mgr).await.regions.len()
}

/// The conversion is running when the split is submitted: the op waits with
/// the reason and writes keep flowing; the split runs once it ends.
#[test]
fn a_split_waits_for_an_ec_conversion_with_writes_flowing() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let part_id = 971;
    compio::runtime::Runtime::new().unwrap().block_on(async {
        autumn_partition_server::set_split_commit_pause(false);
        let c = cluster(&[(part_id, b"a", b"z")], 81).await;
        c.markers
            .send(Marker::Ec {
                extent: c.log_extents[0],
                node: c.idle_node,
            })
            .unwrap();
        compio::time::sleep(Duration::from_millis(50)).await;

        let op_id = submit_split(&c.admin, part_id, b"m").await;
        let want = format!(
            "waiting, writes not frozen: ec conversion in flight on extent {}",
            c.log_extents[0]
        );
        let mut seen = String::new();
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let o = op(&c.admin, op_id).await;
            assert_eq!(o.state, OP_STATE_RUNNING, "{} / {}", o.error, o.message);
            seen = o.message;
            if seen == want {
                break;
            }
            compio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(seen, want);
        // Not frozen: a frozen partition refuses puts.
        for i in 0u32..10 {
            psr_put(&c.ps, part_id, format!("w{i:03}").as_bytes(), b"v").await;
        }
        // Past a few recovery ticks: the marker stands and the split waits.
        compio::time::sleep(Duration::from_secs(5)).await;
        let o = op(&c.admin, op_id).await;
        assert_eq!(o.state, OP_STATE_RUNNING, "{} / {}", o.error, o.message);
        assert_eq!(o.message, want);
        assert_eq!(partition_count(&c.mgr).await, 1);

        c.markers.send(Marker::Clear(c.log_extents[0])).unwrap();
        let o = wait_terminal(&c.admin, op_id, Duration::from_secs(20)).await;
        assert_eq!(o.state, OP_STATE_SUCCEEDED, "{} / {}", o.error, o.message);
        assert_eq!(partition_count(&c.mgr).await, 2);
    });
}

/// A conversion that begins after the wait, while the split is frozen and
/// about to commit: the PS aborts and unfreezes at once instead of retrying
/// the refused commit for the rest of its ~20 s freeze budget.
#[test]
fn an_ec_conversion_appearing_at_commit_aborts_the_freeze_at_once() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let part_id = 972;
    compio::runtime::Runtime::new().unwrap().block_on(async {
        autumn_partition_server::set_split_commit_pause(false);
        let c = cluster(&[(part_id, b"a", b"z")], 82).await;
        let parked0 = autumn_partition_server::split_commit_parked_count();
        autumn_partition_server::set_split_commit_pause(true);
        let op_id = submit_split(&c.admin, part_id, b"m").await;
        assert!(
            poll_until(Duration::from_secs(20), Duration::from_millis(5), || {
                autumn_partition_server::split_commit_parked_count() > parked0
            })
            .await,
            "split never reached the pre-commit sync point"
        );
        c.markers
            .send(Marker::Ec {
                extent: c.log_extents[0],
                node: c.idle_node,
            })
            .unwrap();
        compio::time::sleep(Duration::from_millis(50)).await;

        let released = Instant::now();
        autumn_partition_server::set_split_commit_pause(false);
        let o = wait_terminal(&c.admin, op_id, Duration::from_secs(30)).await;
        let took = released.elapsed();
        assert_eq!(o.state, OP_STATE_FAILED, "{} / {}", o.error, o.message);
        assert!(
            o.error
                .contains(&format!("ec conversion in flight on extent {}", c.log_extents[0])),
            "{}",
            o.error
        );
        assert!(took < Duration::from_secs(5), "frozen for {took:?} after release");
        psr_put(&c.ps, part_id, b"after", b"v").await;
        assert_eq!(partition_count(&c.mgr).await, 1);
    });
}

/// Merge: a conversion on the victim's extent holds the merge before either
/// side freezes (both keep taking writes); a direct merge RPC meanwhile is
/// refused at once; the merge runs once the conversion ends.
#[test]
fn a_merge_waits_for_an_ec_conversion_with_writes_flowing() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (left, right) = (973, 974);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        autumn_partition_server::set_split_commit_pause(false);
        let c = cluster(&[(left, b"a", b"m"), (right, b"m", b"z")], 83).await;
        let victim_extent = c.log_extents[1];
        c.markers
            .send(Marker::Ec {
                extent: victim_extent,
                node: c.idle_node,
            })
            .unwrap();
        compio::time::sleep(Duration::from_millis(50)).await;

        let resp = c
            .admin
            .call(
                MSG_OP_SUBMIT,
                manager_rpc::rkyv_encode(&OpSubmitReq {
                    kind: OP_KIND_MERGE,
                    part_id: left,
                    secondary_id: right,
                    requested_by: "test".to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("submit merge");
        let r: OpSubmitResp = manager_rpc::rkyv_decode(&resp).unwrap();
        assert_eq!(r.code, manager_rpc::CODE_OK, "{}", r.message);
        let op_id = r.op_id;

        let want = format!(
            "waiting, writes not frozen: ec conversion in flight on extent {victim_extent}"
        );
        assert!(
            poll_until_async(Duration::from_secs(5), Duration::from_millis(50), || async {
                op(&c.admin, op_id).await.message == want
            })
            .await,
            "merge never reported the wait: {}",
            op(&c.admin, op_id).await.message
        );
        // Neither side frozen: a merge-frozen partition refuses puts.
        compio::time::sleep(Duration::from_secs(3)).await;
        psr_put(&c.ps, left, b"b-during", b"v").await;
        psr_put(&c.ps, right, b"n-during", b"v").await;
        let o = op(&c.admin, op_id).await;
        assert_eq!(o.state, OP_STATE_RUNNING, "{} / {}", o.error, o.message);
        assert_eq!(partition_count(&c.mgr).await, 2);

        // Another merge of the pair, straight over RPC, is refused at once.
        let direct = c
            .admin
            .call(
                manager_rpc::MSG_MERGE_PARTITIONS,
                manager_rpc::rkyv_encode(&manager_rpc::MergePartitionsReq {
                    survivor_part_id: left,
                    victim_part_id: right,
                    force: false,
                }),
            )
            .await
            .unwrap();
        let direct: manager_rpc::MergePartitionsResp = manager_rpc::rkyv_decode(&direct).unwrap();
        assert_eq!(direct.code, manager_rpc::CODE_PRECONDITION, "{}", direct.message);
        assert!(direct.message.contains("already being split or merged"), "{}", direct.message);

        c.markers.send(Marker::Clear(victim_extent)).unwrap();
        let o = wait_terminal(&c.admin, op_id, Duration::from_secs(30)).await;
        assert_eq!(o.state, OP_STATE_SUCCEEDED, "{} / {}", o.error, o.message);
        assert_eq!(partition_count(&c.mgr).await, 1);
    });
}
