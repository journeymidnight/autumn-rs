//! A submitted compaction lives in its PS's memory. When the partition is
//! opened again (here: the PS is SIGKILLed mid-compaction and restarted), the
//! op will never run or report, so the manager must stop showing it RUNNING
//! and a resubmit must start a new compaction rather than attach to the dead
//! one.
//!
//! The PS runs as a re-executed child (`support::ChildPs`) so the compaction
//! can be held in place and the process SIGKILLed.

mod support;

use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    self, OpQueryReq, OpQueryResp, OpRecord, OpSubmitReq, OpSubmitResp, MSG_OP_QUERY,
    MSG_OP_SUBMIT, OP_KIND_COMPACT, OP_STATE_PENDING, OP_STATE_RUNNING, OP_STATE_SUCCEEDED,
    OP_STATE_UNKNOWN,
};
use autumn_rpc::version_hello::Role;

use support::*;

const PART: u64 = 971;
const PS_ID: u64 = 83;

/// The re-executed child PS (`support::ChildPs`).
#[test]
fn child_ps() {
    child_ps_main();
}

async fn submit_compact(admin: &RpcClient) -> OpSubmitResp {
    let resp = admin
        .call(
            MSG_OP_SUBMIT,
            manager_rpc::rkyv_encode(&OpSubmitReq {
                kind: OP_KIND_COMPACT,
                part_id: PART,
                requested_by: "test".to_string(),
                ..Default::default()
            }),
        )
        .await
        .expect("submit compact");
    let r: OpSubmitResp = manager_rpc::rkyv_decode(&resp).unwrap();
    assert_eq!(r.code, manager_rpc::CODE_OK, "compact refused: {}", r.message);
    r
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

async fn wait_for(
    admin: &RpcClient,
    op_id: u64,
    within: Duration,
    what: &str,
    done: impl Fn(&OpRecord) -> bool,
) -> OpRecord {
    let start = Instant::now();
    loop {
        let o = op(admin, op_id).await;
        if done(&o) {
            return o;
        }
        assert!(
            start.elapsed() < within,
            "op {op_id}: {what} not seen within {within:?} (state {}, {}/{}, {})",
            o.state,
            o.progress_done,
            o.progress_total,
            o.message
        );
        compio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[test]
fn a_compaction_lost_to_a_ps_restart_stops_running_and_can_be_resubmitted() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let (n1, n2) = (pick_addr(), pick_addr());
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    start_extent_node(n1, d1.path().to_path_buf(), 1);
    start_extent_node(n2, d2.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1, n2, 971).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, PART, log, row, meta, b"a", b"z").await;
        let admin = RpcClient::connect_as(mgr_addr, Role::Admin, None)
            .await
            .unwrap();

        let ps1_addr = pick_addr();
        let mut child = ChildPs::spawn(
            PS_ID,
            mgr_addr,
            ps1_addr,
            ChildFailpoints {
                compaction_hold: true,
                ..Default::default()
            },
        );
        let ps = RpcClient::connect(ps1_addr).await.expect("connect ps1");
        for round in 0..2 {
            for i in 0..200u32 {
                ps_put(&ps, PART, format!("k{round}{i:04}").as_bytes(), b"v").await;
            }
            ps_flush(&ps, PART).await;
        }

        let first = submit_compact(&admin).await.op_id;
        // The held compaction's sample reaches the manager: it really started.
        wait_for(&admin, first, Duration::from_secs(20), "a progress sample", |o| {
            o.state == OP_STATE_RUNNING && o.progress_total > 0
        })
        .await;
        drop(ps);
        child.kill();

        let ps2_addr = pick_addr();
        let (ps2_stop, ps2_join) = start_partition_server_stoppable(PS_ID, mgr_addr, ps2_addr);

        let o = wait_for(&admin, first, Duration::from_secs(20), "the op leaving RUNNING", |o| {
            o.state != OP_STATE_RUNNING && o.state != OP_STATE_PENDING
        })
        .await;
        assert_eq!(o.state, OP_STATE_UNKNOWN, "{} / {}", o.error, o.message);
        assert!(o.message.contains("reopened"), "{}", o.message);

        let again = submit_compact(&admin).await;
        assert_ne!(again.op_id, first, "a resubmit attached to the lost op: {}", again.message);
        let o = wait_for(&admin, again.op_id, Duration::from_secs(30), "the new op ending", |o| {
            o.state != OP_STATE_RUNNING && o.state != OP_STATE_PENDING
        })
        .await;
        assert_eq!(o.state, OP_STATE_SUCCEEDED, "{} / {}", o.error, o.message);

        ps2_stop.shutdown();
        ps2_join.join().unwrap();
    });
}
