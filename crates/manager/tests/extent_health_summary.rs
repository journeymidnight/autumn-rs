//! The extent health summary an operator polls (`autumn-op health`, the
//! dashboard's alert rows) reads the leader's own view: a sealed extent whose
//! replica sits on a node that stopped answering is DEGRADED and named, and it
//! is clean again once the node is back.

mod support;

use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, ExtentHealthSummaryReq, ExtentHealthSummaryResp,
    StreamAllocExtentReq, StreamAllocExtentResp, CODE_OK,
    MSG_EXTENT_HEALTH_SUMMARY, MSG_STREAM_ALLOC_EXTENT, SLOT_STATE_UNREACHABLE,
};
use autumn_stream::{ConnPool, StreamClient};
use support::*;

async fn summary(mgr: &RpcClient) -> ExtentHealthSummaryResp {
    let resp = mgr
        .call(
            MSG_EXTENT_HEALTH_SUMMARY,
            rkyv_encode(&ExtentHealthSummaryReq { max_problems: 10 }),
        )
        .await
        .expect("health summary");
    let r: ExtentHealthSummaryResp = rkyv_decode(&resp).expect("decode summary");
    assert_eq!(r.code, CODE_OK, "{}", r.message);
    r
}

#[test]
fn a_replica_on_a_silent_node_is_degraded_until_the_node_returns() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().expect("tmpdir")).collect();
    let addrs: Vec<_> = (0..3).map(|_| pick_addr()).collect();
    let disks: Vec<u64> = (0..3)
        .map(|i| format_node(mgr_addr, addrs[i], &format!("uuid-health-{i}")))
        .collect();
    let mut nodes: Vec<_> = (0..3)
        .map(|i| Some(start_extent_node_stoppable(addrs[i], dirs[i].path().to_path_buf(), disks[i])))
        .collect();

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        let mut node_ids = Vec::new();
        for (i, addr) in addrs.iter().enumerate() {
            node_ids.push(
                register_node(&mgr, &addr.to_string(), &format!("uuid-health-{i}"))
                    .await
                    .node_id,
            );
        }
        let stream_id = create_stream(&mgr, 3).await;
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "extent-health/owner".to_string(),
            1 << 20,
            Rc::new(ConnPool::new()),
        )
        .await
        .expect("stream client");
        let appended = sc.append(stream_id, &[0x5a_u8; 4096]).await.expect("append");
        let extent = appended.extent_id;
        // Seal at the acked end: every member answered, every bit set.
        let resp = mgr
            .call(
                MSG_STREAM_ALLOC_EXTENT,
                rkyv_encode(&StreamAllocExtentReq {
                    stream_id,
                    owner_key: sc.owner_key().to_string(),
                    owner_epoch: sc.owner_epoch(),
                    seal_commit: Some(appended.end),
                    exclude_node_ids: vec![],
                    seal_extent_id: extent,
                }),
            )
            .await
            .expect("seal");
        let seal: StreamAllocExtentResp = rkyv_decode(&resp).expect("decode seal");
        assert_eq!(seal.code, CODE_OK, "seal failed: {}", seal.message);

        let before = summary(&mgr).await;
        assert_eq!((before.degraded, before.unavailable), (0, 0), "{before:?}");
        assert!(before.sealed_extents >= 1 && before.clean == before.sealed_extents);

        let ex = sc.get_extent_info(extent).await.expect("extent info");
        let victim = node_ids.iter().position(|n| *n == ex.replicates[1]).unwrap();
        let (flag, handle) = nodes[victim].take().unwrap();
        flag.shutdown();
        handle.join().expect("join extent node");

        // The node is judged after the soft timeout (10 s) of failed `df`s.
        let start = Instant::now();
        let degraded = loop {
            let r = summary(&mgr).await;
            if r.degraded > 0 {
                break r;
            }
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "30 s after a replica's node stopped, the summary still says {r:?}"
            );
            compio::time::sleep(Duration::from_millis(500)).await;
        };
        assert_eq!(degraded.unavailable, 0, "two copies still serve");
        assert!(degraded.degraded >= 1);
        assert!(degraded.slot_counts[SLOT_STATE_UNREACHABLE as usize] >= 1);
        let p = degraded
            .problems
            .iter()
            .find(|p| p.extent_id == extent)
            .expect("the extent is named among the problems");
        assert_eq!((p.serving, p.total, p.needed), (2, 3, 1));
        assert_eq!(p.slots.len(), 1);
        assert_eq!(p.slots[0].node_id, node_ids[victim]);
        assert_eq!(p.slots[0].state, SLOT_STATE_UNREACHABLE);

        nodes[victim] = Some(start_extent_node_stoppable(
            addrs[victim],
            dirs[victim].path().to_path_buf(),
            disks[victim],
        ));
        let start = Instant::now();
        loop {
            let r = summary(&mgr).await;
            if r.degraded == 0 && r.unavailable == 0 {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "30 s after the node returned, the summary still says {r:?}"
            );
            compio::time::sleep(Duration::from_millis(500)).await;
        }
    });
    drop(nodes);
}
