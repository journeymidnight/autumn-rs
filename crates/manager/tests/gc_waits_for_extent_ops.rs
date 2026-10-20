//! GC does not relocate an extent's live values while another op is in flight
//! on it: nothing preempts that op, so the punch would be refused and the
//! relocation thrown away. The PS asks the manager first and skips the extent.

mod support;

use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::mpsc;
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::{manager_rpc, partition_rpc};
use autumn_stream::{ConnPool, StreamClient};

use support::*;

enum Marker {
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
    support::wait_accepting(addr, "start_manager_with_markers");
    tx
}

fn large_value(i: u32, gen: u8) -> Vec<u8> {
    // > 4 KiB: the value lives in the log stream behind a ValuePointer.
    let mut v = format!("v{gen}-{i:03}-").into_bytes();
    v.resize(5000, b'x');
    v
}

async fn force_gc(ps: &RpcClient, part_id: u64, extent_ids: Vec<u64>) {
    let resp = ps
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id,
                op: partition_rpc::MAINTENANCE_FORCE_GC,
                extent_ids,
                gc_ratio: None,
                gc_max_size: None,
                gc_stream_debt: None,
                gc_dead_bytes_high: None,
                gc_empty_only: false,
                gc_policy_is_standing: false,
                op_id: 0,
            }),
        )
        .await
        .expect("force gc");
    let r: partition_rpc::MaintenanceResp = partition_rpc::rkyv_decode(&resp).expect("decode");
    assert_eq!(r.code, partition_rpc::CODE_OK, "force gc: {}", r.message);
}

#[test]
fn gc_skips_an_extent_with_an_op_in_flight() {
    let mgr_addr = pick_addr();
    let markers = start_manager_with_markers(mgr_addr);
    let (n1, n2) = (pick_addr(), pick_addr());
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    start_extent_node(n1, d1.path().to_path_buf(), 1);
    start_extent_node(n2, d2.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1, n2, 81).await;
        // Registered, never started: an EC marker naming it as coordinator is
        // neither cancelled (it is not abnormal, only unheard-of) nor dispatched.
        let idle = register_node(&mgr, &pick_addr().to_string(), "idle-disk").await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 801, log, row, meta, b"a", b"z").await;
        let ps_addr = pick_addr();
        start_partition_server(81, mgr_addr, ps_addr);
        let ps = RpcClient::connect(ps_addr).await.expect("connect ps");

        // Two generations of every value in the first log extent: the first is
        // garbage once overwritten, the second live.
        for gen in 0..2u8 {
            for i in 0..20u32 {
                ps_put(&ps, 801, format!("k{i:03}").as_bytes(), &large_value(i, gen)).await;
            }
        }
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "gc-precheck-reader".to_string(),
            1 << 20,
            Rc::new(ConnPool::new()),
        )
        .await
        .expect("stream client");
        let first = *sc.get_stream_info(log).await.unwrap().extent_ids.last().unwrap();
        let resp = ps
            .call(
                partition_rpc::MSG_ROLL_TAILS,
                partition_rpc::rkyv_encode(&partition_rpc::RollTailsReq {
                    part_id: 801,
                    entries: vec![(log, first)],
                }),
            )
            .await
            .expect("roll");
        let rolled: partition_rpc::RollTailsResp = partition_rpc::rkyv_decode(&resp).unwrap();
        assert_eq!(rolled.rolled, 1, "the first log extent must be sealed");
        ps_put(&ps, 801, b"tail-key", b"small").await;
        ps_flush(&ps, 801).await;
        ps_compact(&ps, 801).await;
        compio::time::sleep(Duration::from_millis(2000)).await;

        // An EC conversion is in flight on the extent: GC must not touch it.
        markers.send(Marker::Ec { extent: first, node: idle.node_id }).unwrap();
        compio::time::sleep(Duration::from_millis(100)).await;
        let before = sc.commit_length(log).await.expect("tail commit length");
        force_gc(&ps, 801, vec![first]).await;
        compio::time::sleep(Duration::from_millis(2000)).await;
        assert!(
            sc.get_stream_info(log).await.unwrap().extent_ids.contains(&first),
            "the extent stays while its op is in flight"
        );
        assert_eq!(
            sc.commit_length(log).await.expect("tail commit length"),
            before,
            "nothing was relocated off an extent GC could not punch"
        );
        // The same through the op ledger: the op says why nothing ran.
        let admin = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .expect("admin");
        let resp = admin
            .call(
                manager_rpc::MSG_OP_SUBMIT,
                manager_rpc::rkyv_encode(&manager_rpc::OpSubmitReq {
                    kind: manager_rpc::OP_KIND_FORCE_GC,
                    part_id: 801,
                    extent_ids: vec![first],
                    requested_by: "test".into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("submit");
        let sub: manager_rpc::OpSubmitResp = manager_rpc::rkyv_decode(&resp).unwrap();
        assert_eq!(sub.code, manager_rpc::CODE_OK, "{}", sub.message);
        let mut ended = None;
        for _ in 0..60 {
            compio::time::sleep(Duration::from_millis(250)).await;
            let resp = admin
                .call(
                    manager_rpc::MSG_OP_QUERY,
                    manager_rpc::rkyv_encode(&manager_rpc::OpQueryReq {
                        op_id: sub.op_id,
                        ..Default::default()
                    }),
                )
                .await
                .expect("query");
            let q: manager_rpc::OpQueryResp = manager_rpc::rkyv_decode(&resp).unwrap();
            if let Some(op) = q.ops.into_iter().find(|o| {
                o.state != manager_rpc::OP_STATE_RUNNING && o.state != manager_rpc::OP_STATE_PENDING
            }) {
                ended = Some(op);
                break;
            }
        }
        let op = ended.expect("the forcegc op ended");
        assert_eq!(op.state, manager_rpc::OP_STATE_FAILED, "{op:?}");
        assert!(
            op.error.contains("deferred") && op.error.contains(&first.to_string()),
            "{}",
            op.error
        );

        // The op ends: the same GC reclaims it (the skip's cooldown is a
        // soft one; FORCE_GC is not held back by it).
        markers.send(Marker::Clear(first)).unwrap();
        compio::time::sleep(Duration::from_millis(100)).await;
        force_gc(&ps, 801, vec![first]).await;
        let mut gone = false;
        for _ in 0..40 {
            compio::time::sleep(Duration::from_millis(250)).await;
            if !sc.get_stream_info(log).await.unwrap().extent_ids.contains(&first) {
                gone = true;
                break;
            }
        }
        assert!(gone, "with nothing in flight, GC reclaims the extent");
        for i in 0..20u32 {
            let r = ps_get(&ps, 801, format!("k{i:03}").as_bytes()).await;
            assert_eq!(r.value, large_value(i, 1), "k{i:03} survives the relocation");
        }
    });
}
