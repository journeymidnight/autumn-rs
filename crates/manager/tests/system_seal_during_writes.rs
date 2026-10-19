//! System test — appends after a manager seal land on the new tail.
//!
//! The stream's tail is sealed through the manager (`stream_alloc_extent`),
//! and a client that loads the stream afterwards appends to the new extent.
//! The stale-tail retry of a client that kept appending is not exercised here.

mod support;

use std::rc::Rc;

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

#[test]
fn appends_after_a_manager_seal_land_on_the_new_tail() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 64).await;

        let stream_id = create_stream(&mgr, 2).await;

        // Create a StreamClient for writing
        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "test-seal-writer".to_string(),
            1024 * 1024,
            pool.clone(),
        )
        .await
        .expect("connect stream client");

        // Write some data to establish the stream tail
        for i in 0..5 {
            let payload = format!("pre-seal-{i:04}").into_bytes();
            sc.append(stream_id, &payload)
                .await
                .expect("pre-seal append");
        }

        // Verify stream has 1 extent
        let info = sc.get_stream_info(stream_id).await.expect("stream_info");
        assert_eq!(info.extent_ids.len(), 1, "should have 1 extent before seal");
        let original_extent = info.extent_ids[0];

        // Seal the current tail by calling stream_alloc_extent via manager RPC
        let resp = mgr
            .call(
                MSG_STREAM_ALLOC_EXTENT,
                rkyv_encode(&StreamAllocExtentReq {
                    stream_id,
                    owner_key: "test-seal-writer".to_string(),
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
            "alloc extent failed: {}",
            alloc_resp.message
        );

        // Manager now reports 2 extents
        let info = sc
            .get_stream_info(stream_id)
            .await
            .expect("stream_info after seal");
        assert_eq!(info.extent_ids.len(), 2, "should have 2 extents after seal");

        // Create a fresh StreamClient to verify writes go to the new extent
        let sc2 = StreamClient::connect(
            &mgr_addr.to_string(),
            "test-seal-writer-2".to_string(),
            1024 * 1024,
            pool.clone(),
        )
        .await
        .expect("connect stream client 2");

        // sc2 will load the stream tail fresh from manager → gets the new extent
        let result = sc2
            .append(stream_id, b"after-seal-data")
            .await
            .expect("append on fresh client");

        // The append should land on the new (unsealed) extent, not the old one
        assert_ne!(
            result.extent_id, original_extent,
            "fresh client's append should land on new extent, not the sealed one"
        );

        // Verify the old extent's data is still readable
        let (old_data, _) = sc2
            .read_bytes_from_extent(original_extent, 0, 4096)
            .await
            .expect("read old extent");
        assert!(!old_data.is_empty(), "old extent should have pre-seal data");

        // Verify the stream has expected structure
        let final_info = sc2
            .get_stream_info(stream_id)
            .await
            .expect("final stream_info");
        assert!(
            final_info.extent_ids.len() >= 2,
            "stream should have >= 2 extents"
        );
    });
}
