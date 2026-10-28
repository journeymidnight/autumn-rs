//! Data integrity: a PS SIGKILLed in the middle of a major compaction loses
//! nothing, and the reopened partition compacts cleanly.
//!
//! The PS is a re-executed child (`support::ChildPs`) whose compactions park
//! at their first progress sample, before any output reaches the row stream.
//! The test waits until the manager has that sample, so the kill lands inside
//! the merge, and the kill reaps the process before the next PS opens the
//! partition. The reopened partition must still list the uncompacted input
//! tables: the compaction published nothing.

mod support;

use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    self, OpQueryReq, OpQueryResp, OpRecord, OpSubmitReq, OpSubmitResp, MSG_OP_QUERY,
    MSG_OP_SUBMIT, OP_KIND_COMPACT, OP_STATE_RUNNING, OP_STATE_SUCCEEDED,
};
use autumn_rpc::partition_rpc;
use autumn_rpc::version_hello::Role;

use support::*;

/// The re-executed child PS (`support::ChildPs`).
#[test]
fn child_ps() {
    child_ps_main();
}

async fn submit_compact(admin: &RpcClient, part: u64) -> u64 {
    let resp = admin
        .call(
            MSG_OP_SUBMIT,
            manager_rpc::rkyv_encode(&OpSubmitReq {
                kind: OP_KIND_COMPACT,
                part_id: part,
                requested_by: "test".to_string(),
                ..Default::default()
            }),
        )
        .await
        .expect("submit compact");
    let r: OpSubmitResp = manager_rpc::rkyv_decode(&resp).unwrap();
    assert_eq!(r.code, manager_rpc::CODE_OK, "compact refused: {}", r.message);
    r.op_id
}

async fn wait_op(admin: &RpcClient, op_id: u64, what: &str, done: impl Fn(&OpRecord) -> bool) {
    let start = Instant::now();
    loop {
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
        let o = q.ops.into_iter().next().expect("one op");
        if done(&o) {
            return;
        }
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "op {op_id}: {what} not seen (state {}, {}/{}, {})",
            o.state,
            o.progress_done,
            o.progress_total,
            o.message
        );
        compio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn live_sst_count(ps: &RpcClient, part: u64) -> usize {
    let resp = ps
        .call(
            partition_rpc::MSG_DIAG_PARTITION_VP,
            partition_rpc::rkyv_encode(&partition_rpc::DiagPartitionVpReq { part_id: part }),
        )
        .await
        .expect("diag partition vp");
    let r: partition_rpc::DiagPartitionVpResp =
        partition_rpc::rkyv_decode(&resp).expect("decode DiagPartitionVpResp");
    assert_eq!(r.code, partition_rpc::CODE_OK, "diag partition vp: {}", r.message);
    r.sst_vp_heads.len()
}

struct Cluster {
    mgr_addr: SocketAddr,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
}

async fn cluster(part: u64, tag: u16) -> Cluster {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let (n1, n2) = (pick_addr(), pick_addr());
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    start_extent_node(n1, d1.path().to_path_buf(), 1);
    start_extent_node(n2, d2.path().to_path_buf(), 2);
    let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
    register_two_nodes(&mgr, n1, n2, tag).await;
    let (log, row, meta) = create_three_streams(&mgr).await;
    upsert_partition(&mgr, part, log, row, meta, b"a", b"z").await;
    Cluster { mgr_addr, _dirs: (d1, d2) }
}

/// Run a major compaction on `child`'s partition, wait until the manager sees
/// it parked mid-merge, and SIGKILL the child. Returns the lost op.
async fn kill_mid_compaction(admin: &RpcClient, part: u64, ps: Rc<RpcClient>, mut child: ChildPs) -> u64 {
    let op = submit_compact(admin, part).await;
    wait_op(admin, op, "the compaction's progress sample", |o| {
        o.state == OP_STATE_RUNNING && o.progress_total > 0
    })
    .await;
    drop(ps);
    child.kill();
    op
}

/// Wait until the reopened partition serves `key`.
async fn reopened(ps_addr: SocketAddr, part: u64, key: &[u8]) -> Rc<RpcClient> {
    let ps = RpcClient::connect(ps_addr).await.expect("connect reopened PS");
    let start = Instant::now();
    while !try_ps_get(&ps, part, key).await.is_ok_and(|r| r.code == partition_rpc::CODE_OK) {
        assert!(start.elapsed() < Duration::from_secs(30), "the reopened partition never served");
        compio::time::sleep(Duration::from_millis(100)).await;
    }
    ps
}

fn key(i: u32) -> Vec<u8> {
    format!("key-{i:02}").into_bytes()
}

fn val(i: u32) -> Vec<u8> {
    format!("val-{i}").into_bytes()
}

#[test]
fn crash_during_compaction_no_data_loss() {
    const PART: u64 = 901;
    const PS_ID: u64 = 75;
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let c = cluster(PART, 55).await;
        let admin = RpcClient::connect_as(c.mgr_addr, Role::Admin, None).await.unwrap();
        let ps1_addr = pick_addr();
        let child = ChildPs::spawn(
            PS_ID,
            c.mgr_addr,
            ps1_addr,
            ChildFailpoints { compaction_hold: true, minor_paused: true, ..Default::default() },
        );
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        // Two SSTs of 20 keys each.
        for round in 0u32..2 {
            for i in round * 20..round * 20 + 20 {
                ps_put(&ps1, PART, &key(i), &val(i)).await;
            }
            ps_flush(&ps1, PART).await;
        }
        kill_mid_compaction(&admin, PART, ps1, child).await;

        // The reopened PS runs in this process: keep the table list as is.
        autumn_partition_server::background::set_minor_compaction_paused(true);
        let ps2_addr = pick_addr();
        let (stop, join) = start_partition_server_stoppable(PS_ID, c.mgr_addr, ps2_addr);
        let ps2 = reopened(ps2_addr, PART, &key(0)).await;
        assert_eq!(live_sst_count(&ps2, PART).await, 2, "the killed compaction published nothing");
        for i in 0u32..40 {
            let resp = ps_get(&ps2, PART, &key(i)).await;
            assert_eq!(resp.value, val(i), "key-{i:02} must survive a crash during compaction");
        }
        drop(ps2);
        stop.shutdown();
        join.join().expect("join ps2");
    });
}

#[test]
fn crash_during_compaction_then_successful_compact() {
    const PART: u64 = 902;
    const PS_ID: u64 = 76;
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let c = cluster(PART, 56).await;
        let admin = RpcClient::connect_as(c.mgr_addr, Role::Admin, None).await.unwrap();
        let ps1_addr = pick_addr();
        let child = ChildPs::spawn(
            PS_ID,
            c.mgr_addr,
            ps1_addr,
            ChildFailpoints { compaction_hold: true, minor_paused: true, ..Default::default() },
        );
        let ps1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        // SST 1: 30 keys; SST 2: tombstones for the first 15; SST 3: 15 new keys.
        for i in 0u32..30 {
            ps_put(&ps1, PART, &key(i), &val(i)).await;
        }
        ps_flush(&ps1, PART).await;
        for i in 0u32..15 {
            ps_delete(&ps1, PART, &key(i)).await;
        }
        ps_flush(&ps1, PART).await;
        for i in 30u32..45 {
            ps_put(&ps1, PART, &key(i), &val(i)).await;
        }
        ps_flush(&ps1, PART).await;
        let lost = kill_mid_compaction(&admin, PART, ps1, child).await;

        // The reopened PS runs in this process: keep the table list as is.
        autumn_partition_server::background::set_minor_compaction_paused(true);
        let ps2_addr = pick_addr();
        let (stop, join) = start_partition_server_stoppable(PS_ID, c.mgr_addr, ps2_addr);
        let ps2 = reopened(ps2_addr, PART, &key(15)).await;
        assert_eq!(live_sst_count(&ps2, PART).await, 3, "the killed compaction published nothing");
        let check = |when: &'static str| {
            let ps2 = &ps2;
            async move {
                for i in 0u32..15 {
                    let resp = ps_head(ps2, PART, &key(i)).await;
                    assert!(!resp.found, "key-{i:02} must stay deleted {when}");
                }
                for i in 15u32..45 {
                    let resp = ps_get(ps2, PART, &key(i)).await;
                    assert_eq!(resp.value, val(i), "key-{i:02} wrong {when}");
                }
            }
        };
        check("after the crash").await;

        // A submit attaches to an op still RUNNING; the lost one ends once
        // the manager sees the reopen.
        wait_op(&admin, lost, "the lost op ending", |o| o.state != OP_STATE_RUNNING).await;
        let op = submit_compact(&admin, PART).await;
        assert_ne!(op, lost, "the resubmit attached to the lost op");
        wait_op(&admin, op, "the second compaction succeeding", |o| {
            o.state == OP_STATE_SUCCEEDED
        })
        .await;
        assert_eq!(live_sst_count(&ps2, PART).await, 1, "the second compaction published");
        check("after the second compaction").await;
        drop(ps2);
        stop.shutdown();
        join.join().expect("join ps2");
    });
}
