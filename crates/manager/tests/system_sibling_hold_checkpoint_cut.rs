//! A checkpoint whose cut of an extent shared with a split sibling meets the
//! sibling's split/merge hold must stand, the cut deferred, like a cut refused
//! for an op in flight: not fail the flush.
//!
//! Split children share the parent's meta extents. A split or merge holds its
//! partitions (`topology_held`) from before its wait for extent ops until it
//! ends, and the manager refuses a truncate from another partition that would
//! remove an extent a held partition's streams list. That refusal did not
//! read as "retry later" to the PS, so a sibling's flush failed for as long as
//! the hold lasted (up to the 10 min topology wait).
//!
//! Shape: an empty partition split (its children share the parent's one meta
//! extent, and an empty child can merge at once), an EC marker on the right
//! neighbour's extent so the child's merge waits with both sides held, then a
//! flush of the other child.

mod support;

use std::net::SocketAddr;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    self, OpQueryReq, OpQueryResp, OpRecord, OpSubmitReq, OpSubmitResp, StreamInfoReq,
    StreamInfoResp, MSG_OP_QUERY, MSG_OP_SUBMIT, MSG_STREAM_INFO, OP_KIND_MERGE, OP_KIND_SPLIT,
    OP_STATE_RUNNING, OP_STATE_SUCCEEDED,
};
use autumn_rpc::partition_rpc;
use autumn_rpc::version_hello::Role;

use support::*;

const LEFT: u64 = 1491; // [a, m), split at f
const RIGHT: u64 = 1492; // [m, z)

enum Marker {
    Ec { extent: u64, node: u64 },
    Clear(u64),
}

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
                            Marker::Ec { extent, node } => mc._test_mark_ec_inflight_by(extent, vec![node]),
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
    support::wait_accepting(addr, "start_manager_with_markers");
    tx
}

async fn extents(mgr: &RpcClient, stream_id: u64) -> Vec<u64> {
    let resp = mgr
        .call(MSG_STREAM_INFO, manager_rpc::rkyv_encode(&StreamInfoReq { stream_ids: vec![stream_id] }))
        .await
        .unwrap();
    let resp: StreamInfoResp = manager_rpc::rkyv_decode(&resp).unwrap();
    resp.streams[0].1.extent_ids.clone()
}

async fn submit(admin: &RpcClient, req: OpSubmitReq) -> u64 {
    let resp = admin.call(MSG_OP_SUBMIT, manager_rpc::rkyv_encode(&req)).await.expect("submit");
    let r: OpSubmitResp = manager_rpc::rkyv_decode(&resp).unwrap();
    assert_eq!(r.code, manager_rpc::CODE_OK, "submit refused: {}", r.message);
    r.op_id
}

async fn op(admin: &RpcClient, op_id: u64) -> OpRecord {
    let resp = admin
        .call(MSG_OP_QUERY, manager_rpc::rkyv_encode(&OpQueryReq { op_id, ..Default::default() }))
        .await
        .expect("query op");
    let q: OpQueryResp = manager_rpc::rkyv_decode(&resp).unwrap();
    q.ops.into_iter().next().expect("one op")
}

async fn wait_terminal(admin: &RpcClient, op_id: u64) -> OpRecord {
    let start = Instant::now();
    loop {
        let o = op(admin, op_id).await;
        if o.state != OP_STATE_RUNNING && o.state != manager_rpc::OP_STATE_PENDING {
            return o;
        }
        assert!(start.elapsed() < Duration::from_secs(60), "op {op_id} still active: {}", o.message);
        compio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn try_flush(router: &PsRouter, part_id: u64) -> (u8, String) {
    let c = router.client_for(part_id).await;
    let resp = c
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id,
                op: partition_rpc::MAINTENANCE_FLUSH,
                extent_ids: vec![],
                gc_ratio: None,
                gc_max_size: None,
                gc_stream_debt: None,
                gc_dead_bytes_high: None,
                gc_empty_only: false,
                gc_policy_is_standing: false,
                op_id: 0,
            }),
        )
        .await;
    match resp {
        Ok(b) => {
            let r: partition_rpc::MaintenanceResp = partition_rpc::rkyv_decode(&b).expect("decode");
            (r.code, r.message)
        }
        Err(e) => (partition_rpc::CODE_ERROR, format!("{e}")),
    }
}

#[test]
fn a_sibling_s_hold_defers_the_checkpoint_cut_not_the_flush() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr_addr = pick_addr();
        let markers = start_manager_with_markers(mgr_addr);
        let (n1, n2) = (pick_addr(), pick_addr());
        let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        start_extent_node(n1, d1.path().to_path_buf(), 1);
        start_extent_node(n2, d2.path().to_path_buf(), 2);
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1, n2, 40).await;
        let (l_log, l_row, l_meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, LEFT, l_log, l_row, l_meta, b"a", b"m").await;
        let (r_log, r_row, r_meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, RIGHT, r_log, r_row, r_meta, b"m", b"z").await;
        let ps_addr = pick_addr();
        start_partition_server(95, mgr_addr, ps_addr);
        let router = PsRouter::new(mgr_addr, ps_addr);
        let admin = RpcClient::connect_as(mgr_addr, Role::Admin, None).await.unwrap();
        let idle = register_node(&mgr, &pick_addr().to_string(), "idle-disk").await;
        assert_eq!(idle.code, manager_rpc::CODE_OK, "{}", idle.message);

        // An empty LEFT split at f: both children keep only the parent's meta
        // extent, and neither has a table, so the right child may merge.
        let split = submit(
            &admin,
            OpSubmitReq { kind: OP_KIND_SPLIT, part_id: LEFT, at_key: Some(b"f".to_vec()), requested_by: "test".into(), ..Default::default() },
        )
        .await;
        let o = wait_terminal(&admin, split).await;
        assert_eq!(o.state, OP_STATE_SUCCEEDED, "split: {} / {}", o.error, o.message);
        let regions = get_regions(&mgr).await;
        let (child, child_meta) = regions
            .regions
            .iter()
            .find(|(_, r)| r.rg.as_ref().is_some_and(|g| g.start_key == b"f"))
            .map(|(id, r)| (*id, r.meta_stream))
            .expect("the right child");
        let shared: Vec<u64> = {
            let (a, b) = (extents(&mgr, l_meta).await, extents(&mgr, child_meta).await);
            a.into_iter().filter(|e| b.contains(e)).collect()
        };
        assert!(!shared.is_empty(), "non-vacuity: the children share a meta extent");

        // A merge resolves both PS addresses: the child must be open first.
        try_psr_get(&router, child, b"g").await.expect("the child serves");
        // The child's merge with RIGHT waits on an EC marker, both held.
        let r_extent = *extents(&mgr, r_log).await.last().unwrap();
        markers.send(Marker::Ec { extent: r_extent, node: idle.node_id }).unwrap();
        compio::time::sleep(Duration::from_millis(50)).await;
        let merge = submit(
            &admin,
            OpSubmitReq { kind: OP_KIND_MERGE, part_id: child, secondary_id: RIGHT, requested_by: "test".into(), ..Default::default() },
        )
        .await;
        let want = format!("waiting, writes not frozen: ec conversion in flight on extent {r_extent}");
        assert!(
            poll_until_async(Duration::from_secs(10), Duration::from_millis(50), || async {
                op(&admin, merge).await.message == want
            })
            .await,
            "the merge never waited: {}",
            op(&admin, merge).await.message
        );

        // LEFT's checkpoint cuts the shared extent out of its own stream.
        psr_put(&router, LEFT, b"b-key", b"v").await;
        let (code, message) = try_flush(&router, LEFT).await;
        assert!(
            code == partition_rpc::CODE_OK,
            "LEFT's flush failed while its sibling is held: code {code} {message}"
        );
        assert_eq!(psr_get(&router, LEFT, b"b-key").await.value, b"v");
        let l_after = extents(&mgr, l_meta).await;
        assert!(
            shared.iter().all(|e| l_after.contains(e)),
            "non-vacuity: the hold kept the cut from landing: {l_after:?} vs {shared:?}"
        );

        markers.send(Marker::Clear(r_extent)).unwrap();
        let o = wait_terminal(&admin, merge).await;
        assert_eq!(o.state, OP_STATE_SUCCEEDED, "merge: {} / {}", o.error, o.message);
        drop((d1, d2));
    });
}
