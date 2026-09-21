mod support;

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_partition_server::PartitionServer;
use autumn_rpc::client::RpcClient;
use autumn_rpc::extent_rpc::{
    AppendReq, AppendResp, FenceExtentResp, CODE_LOCKED_BY_OTHER, MSG_APPEND, MSG_FENCE_EXTENT,
};
use autumn_rpc::frame::{Frame, FrameDecoder};
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ConnPool, ExtentNode, ExtentNodeConfig, StreamClient};
use compio::io::{AsyncRead, AsyncWriteExt};
use support::*;

#[compio::test]
async fn takeover_refuses_to_serve_until_fence_succeeds() {
    assert!(autumn_common::set_cpuset(vec![]));
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
    compio::time::timeout(Duration::from_secs(20), check_takeover_fence())
        .await
        .expect("takeover regression timed out");
}

async fn check_takeover_fence() {
    let manager = AutumnManager::new();
    let manager_addr = pick_addr();
    let served = manager.clone();
    let manager_task = compio::runtime::spawn(async move {
        served.serve(manager_addr).await.unwrap();
    });
    let directory = tempfile::tempdir().unwrap();
    let node = ExtentNode::new(ExtentNodeConfig::new(directory.path().to_path_buf(), 1))
        .await
        .unwrap();
    let node_addr = pick_addr();
    let node_task = compio::runtime::spawn(async move {
        node.serve(node_addr).await.unwrap();
    });
    let listener = compio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    let refuse_fence = Rc::new(Cell::new(true));
    let rejected = Rc::new(Cell::new(0usize));
    let proxy_refuse = refuse_fence.clone();
    let proxy_rejected = rejected.clone();
    let proxy_task = compio::runtime::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let refuse = proxy_refuse.clone();
            let rejected = proxy_rejected.clone();
            compio::runtime::spawn(async move {
                let backend = RpcClient::connect(node_addr).await.unwrap();
                let mut decoder = FrameDecoder::new();
                loop {
                    let (result, bytes) = socket.read(vec![0; 65536]).await.into_parts();
                    let count = match result {
                        Ok(0) | Err(_) => break,
                        Ok(count) => count,
                    };
                    decoder.feed(&bytes[..count]);
                    while let Some(request) = decoder.try_decode().unwrap() {
                        let payload = if request.msg_type == MSG_FENCE_EXTENT && refuse.get() {
                            rejected.set(rejected.get() + 1);
                            FenceExtentResp {
                                code: CODE_PRECONDITION,
                                message: "injected fence refusal".to_string(),
                            }
                            .encode()
                        } else {
                            backend
                                .call(request.msg_type, request.payload)
                                .await
                                .unwrap()
                        };
                        let response = Frame::response(request.req_id, request.msg_type, payload);
                        let (result, _) = socket.write_all(response.encode()).await.into_parts();
                        if result.is_err() {
                            return;
                        }
                    }
                }
            })
            .detach();
        }
    });
    compio::time::sleep(Duration::from_millis(100)).await;
    let client = RpcClient::connect(manager_addr).await.unwrap();
    let registered = register_node(&client, &proxy_addr.to_string(), "fence-disk").await;
    assert_eq!(registered.code, CODE_OK);
    let log = create_stream(&client, 1).await;
    let row = create_stream(&client, 1).await;
    let meta = create_stream(&client, 1).await;
    upsert_partition(&client, 901, log, row, meta, b"mem/a", b"mem/z").await;
    let old_owner: AcquireOwnerLockResp = rkyv_decode(
        &client
            .call(
                MSG_ACQUIRE_OWNER_LOCK,
                rkyv_encode(&AcquireOwnerLockReq {
                    owner_key: "partition/901".to_string(),
                }),
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(old_owner.code, CODE_OK);
    let server_addr = pick_addr();
    let server = PartitionServer::connect_with_advertise_and_port(
        71,
        &manager_addr.to_string(),
        Some(server_addr.to_string()),
        server_addr,
    )
    .await
    .unwrap();
    assert!(rejected.get() >= 3, "must exercise all fence retries");
    assert!(get_regions(&client).await.part_addrs.is_empty());
    let old_epoch = old_owner.owner_epoch;

    refuse_fence.set(false);
    compio::time::sleep(Duration::from_millis(1100)).await;
    server.sync_regions_once().await.expect("retry takeover");
    assert!(get_regions(&client)
        .await
        .part_addrs
        .iter()
        .any(|(id, _)| *id == 901));
    let stream = StreamClient::connect(
        &manager_addr.to_string(),
        "fence-probe".to_string(),
        1 << 20,
        Rc::new(ConnPool::new()),
    )
    .await
    .unwrap();
    let tail_id = stream.get_stream_info(log).await.unwrap().extent_ids[0];
    let tail = stream.get_extent_info(tail_id).await.unwrap();
    let backend = RpcClient::connect(node_addr).await.unwrap();
    let stale = backend
        .call(
            MSG_APPEND,
            AppendReq {
                extent_id: tail.extent_id,
                eversion: tail.eversion,
                commit: 0,
                owner_epoch: old_epoch,
                payload: bytes::Bytes::from_static(b"stale owner write"),
            }
            .encode(),
        )
        .await
        .unwrap();
    assert_eq!(
        AppendResp::decode(stale).unwrap().code,
        CODE_LOCKED_BY_OTHER
    );
    server.shutdown().await.unwrap();
    drop((proxy_task, node_task, manager_task));
}
