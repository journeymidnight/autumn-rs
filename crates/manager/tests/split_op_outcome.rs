//! A submitted split whose PS reply does not arrive in time is not a failed
//! split. The PS runs the split on its own task, so the manager giving up on
//! the reply cancels nothing: the split can still commit. The op must stay
//! RUNNING until a fact closes it — the commit itself, the PS's reported
//! outcome — and a retry submitted meanwhile must attach to it rather than
//! cut the partition again.

mod support;

use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    self, OpQueryReq, OpQueryResp, OpRecord, OpSubmitReq, OpSubmitResp, StreamInfoReq,
    StreamInfoResp, MSG_OP_QUERY, MSG_OP_SUBMIT, MSG_STREAM_INFO, OP_KIND_SPLIT,
    OP_STATE_FAILED, OP_STATE_RUNNING, OP_STATE_SUCCEEDED,
};
use autumn_rpc::partition_rpc;
use autumn_rpc::version_hello::Role;

use support::*;

/// The split commit pause is process-global; scenarios must not overlap.
static SERIAL: Mutex<()> = Mutex::new(());

const REPLY_TIMEOUT: Duration = Duration::from_secs(2);

fn start_manager_with_split_timeout(addr: SocketAddr) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let m = AutumnManager::new();
            m.set_split_reply_timeout(REPLY_TIMEOUT);
            let _ = m.serve(addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));
}

struct Cluster {
    mgr: Rc<RpcClient>,
    admin: Rc<RpcClient>,
    ps: Rc<RpcClient>,
    ps_addr: SocketAddr,
    log_stream: u64,
    _dirs: [tempfile::TempDir; 2],
}

async fn cluster(part_id: u64, ps_id: u64) -> Cluster {
    let mgr_addr = pick_addr();
    start_manager_with_split_timeout(mgr_addr);
    let (n1, n2) = (pick_addr(), pick_addr());
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    start_extent_node(n1, d1.path().to_path_buf(), 1);
    start_extent_node(n2, d2.path().to_path_buf(), 2);
    let mgr = RpcClient::connect(mgr_addr).await.unwrap();
    register_two_nodes(&mgr, n1, n2, 40).await;
    let (log, row, meta) = create_three_streams(&mgr).await;
    upsert_partition(&mgr, part_id, log, row, meta, b"a", b"z").await;
    let ps_addr = pick_addr();
    start_partition_server(ps_id, mgr_addr, ps_addr);
    let ps = RpcClient::connect(ps_addr).await.unwrap();
    for i in 0u32..20 {
        ps_put(&ps, part_id, format!("d{i:03}").as_bytes(), b"v").await;
        ps_put(&ps, part_id, format!("t{i:03}").as_bytes(), b"v").await;
    }
    ps_flush(&ps, part_id).await;
    let admin = RpcClient::connect_as(mgr_addr, Role::Admin, None)
        .await
        .unwrap();
    Cluster {
        mgr,
        admin,
        ps,
        ps_addr,
        log_stream: log,
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
        compio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn park_split(admin: &RpcClient, part_id: u64, at: &[u8]) -> u64 {
    let parked0 = autumn_partition_server::split_commit_parked_count();
    autumn_partition_server::set_split_commit_pause(true);
    let op_id = submit_split(admin, part_id, at).await;
    let parked = poll_until(Duration::from_secs(20), Duration::from_millis(5), || {
        autumn_partition_server::split_commit_parked_count() > parked0
    })
    .await;
    assert!(parked, "split never reached the pre-commit sync point");
    // Past the manager's reply timeout while the PS still holds the split.
    compio::time::sleep(REPLY_TIMEOUT + Duration::from_secs(1)).await;
    op_id
}

async fn partition_count(mgr: &RpcClient) -> usize {
    get_regions(mgr).await.regions.len()
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

/// The split commits after the manager stopped waiting for its reply: the op
/// ends SUCCEEDED, and a retry submitted while it was unknown attaches to it
/// instead of cutting the partition a second time.
#[test]
fn a_split_committing_after_its_reply_timeout_ends_succeeded() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let part_id = 961;
    compio::runtime::Runtime::new().unwrap().block_on(async {
        autumn_partition_server::set_split_commit_pause(false);
        let c = cluster(part_id, 71).await;
        let op_id = park_split(&c.admin, part_id, b"m").await;

        let o = op(&c.admin, op_id).await;
        assert_eq!(
            o.state, OP_STATE_RUNNING,
            "no reply is not a failure: {} / {}",
            o.error, o.message
        );
        // The operator retries at another point inside the left half.
        let retry = submit_split(&c.admin, part_id, b"f").await;
        assert_eq!(retry, op_id, "a retry must attach to the unknown split");

        autumn_partition_server::set_split_commit_pause(false);
        let o = wait_terminal(&c.admin, op_id, Duration::from_secs(20)).await;
        assert_eq!(o.state, OP_STATE_SUCCEEDED, "{} / {}", o.error, o.message);
        compio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(partition_count(&c.mgr).await, 2, "exactly one split");
    });
}

/// The split fails after the manager stopped waiting: the op ends FAILED with
/// the PS's own reason, carried by its load report.
#[test]
fn a_split_failing_after_its_reply_timeout_reports_the_ps_reason() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let part_id = 962;
    compio::runtime::Runtime::new().unwrap().block_on(async {
        autumn_partition_server::set_split_commit_pause(false);
        autumn_partition_server::set_roll_tails_pause(false);
        let c = cluster(part_id, 72).await;
        // A roll already in flight when the split freezes (a frozen partition
        // defers new ones): parked before its seal, released inside the
        // split's captured-commit window, so the commit is refused.
        let tail = log_tail(&c.mgr, c.log_stream).await;
        let roll_parked0 = autumn_partition_server::roll_tails_parked_count();
        autumn_partition_server::set_roll_tails_pause(true);
        let roller = RpcClient::connect(c.ps_addr).await.unwrap();
        let log_stream = c.log_stream;
        let roll = compio::runtime::spawn(async move {
            let resp = roller
                .call(
                    partition_rpc::MSG_ROLL_TAILS,
                    partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq {
                        part_id,
                        entries: vec![(log_stream, tail)],
                    }),
                )
                .await
                .unwrap();
            partition_rpc::rkyv_decode::<partition_rpc::RollTailsResp>(&resp).unwrap()
        });
        assert!(
            poll_until(Duration::from_secs(10), Duration::from_millis(5), || {
                autumn_partition_server::roll_tails_parked_count() > roll_parked0
            })
            .await,
            "roll never parked"
        );
        let op_id = park_split(&c.admin, part_id, b"m").await;
        autumn_partition_server::set_roll_tails_pause(false);
        assert_eq!(roll.await.unwrap().rolled, 1, "non-vacuity: the tail must move");

        autumn_partition_server::set_split_commit_pause(false);
        let o = wait_terminal(&c.admin, op_id, Duration::from_secs(20)).await;
        assert_eq!(o.state, OP_STATE_FAILED);
        assert!(
            o.error.contains("captured tail moved"),
            "the PS's reason, not the reply timeout: {}",
            o.error
        );
        assert_eq!(partition_count(&c.mgr).await, 1);
    });
}

/// A second split for a partition whose split is still pending is refused at
/// once, not queued behind the first to cut again after it.
#[test]
fn a_second_split_on_a_partition_with_one_pending_is_refused() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let part_id = 963;
    compio::runtime::Runtime::new().unwrap().block_on(async {
        autumn_partition_server::set_split_commit_pause(false);
        let c = cluster(part_id, 73).await;
        let parked0 = autumn_partition_server::split_commit_parked_count();
        autumn_partition_server::set_split_commit_pause(true);
        let first_ps = RpcClient::connect(c.ps_addr).await.unwrap();
        let first = compio::runtime::spawn(async move {
            first_ps
                .call(
                    partition_rpc::MSG_SPLIT_PART,
                    partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq {
                        part_id,
                        at_key: Some(b"m".to_vec()),
                        op_id: 0,
                    }),
                )
                .await
        });
        assert!(
            poll_until(Duration::from_secs(20), Duration::from_millis(5), || {
                autumn_partition_server::split_commit_parked_count() > parked0
            })
            .await,
            "first split never parked"
        );

        let second = compio::time::timeout(
            Duration::from_secs(3),
            c.ps.call(
                partition_rpc::MSG_SPLIT_PART,
                partition_rpc::rkyv_encode(&partition_rpc::SplitPartReq {
                    part_id,
                    at_key: Some(b"f".to_vec()),
                    op_id: 0,
                }),
            ),
        )
        .await;
        autumn_partition_server::set_split_commit_pause(false);
        let first: partition_rpc::SplitPartResp =
            partition_rpc::rkyv_decode(&first.await.unwrap().unwrap()).unwrap();
        assert_eq!(first.code, partition_rpc::CODE_OK, "{}", first.message);
        let second = second.expect("the second split must be answered at once, not queued");
        match second {
            Err(e) => assert!(e.to_string().contains("in progress"), "{e}"),
            Ok(b) => {
                let r: partition_rpc::SplitPartResp = partition_rpc::rkyv_decode(&b).unwrap();
                panic!("second split answered code {}: {}", r.code, r.message);
            }
        }
        compio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(partition_count(&c.mgr).await, 2, "exactly one split");
    });
}
