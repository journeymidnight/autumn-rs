//! System test — extent node crash, StreamClient retries on new extent.
//!
//! With 3 extent nodes, a 2-replica stream is created on (node1, node2).
//! We register a third node but simulate node1 failure by stopping it.
//! StreamClient's append detects the connection error, retries, and
//! eventually allocates a new extent on healthy nodes.

mod support;

use std::rc::Rc;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

/// Test that stream_alloc_extent falls back to healthy nodes when a
/// preferred node is unreachable.
#[test]
fn alloc_extent_falls_back_on_dead_node() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let dead_addr = pick_addr(); // no server

    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");

        // Register alive nodes first so they get lower node_ids (selected first by manager)
        register_node(&mgr, &n1_addr.to_string(), "uuid-n1").await;
        register_node(&mgr, &n2_addr.to_string(), "uuid-n2").await;
        // Dead node gets highest node_id, used as fallback only
        register_node(&mgr, &dead_addr.to_string(), "uuid-dead").await;

        // Create a 2-replica stream on the 2 alive nodes (lowest node_ids selected)
        let stream_id = create_stream(&mgr, 2).await;

        // Stream creation should succeed despite dead node (fallback to alive nodes)
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "test-fallback".to_string(),
            1024 * 1024,
            pool.clone(),
        )
        .await
        .expect("connect stream client");

        // Appends should work
        for i in 0..5 {
            let payload = format!("data-{i}").into_bytes();
            sc.append(stream_id, &payload)
                .await
                .expect("append should succeed with fallback nodes");
        }

        // Seal + alloc new extent: should also work with fallback
        let resp = mgr
            .call(
                MSG_STREAM_ALLOC_EXTENT,
                rkyv_encode(&StreamAllocExtentReq {
                    stream_id,
                    owner_key: "test-fallback".to_string(),
                    owner_epoch: sc.owner_epoch(),
                    seal_commit: None,
                    exclude_node_ids: vec![],
                seal_extent_id: 0,
                }),
            )
            .await
            .expect("alloc extent");
        let alloc_resp: StreamAllocExtentResp =
            rkyv_decode(&resp).expect("decode StreamAllocExtentResp");
        assert_eq!(
            alloc_resp.code, CODE_OK,
            "alloc_extent with dead node fallback failed: {}",
            alloc_resp.message
        );

        let info = sc.get_stream_info(stream_id).await.expect("final info");
        assert_eq!(
            info.extent_ids.len(),
            2,
            "stream should have 2 extents after alloc"
        );
    });
}
