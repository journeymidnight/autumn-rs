mod support;

/// Integration tests for MSG_UPDATE_STREAM_EC (FOPS-03).
use std::net::SocketAddr;

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;

fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr
}

fn start_manager(addr: SocketAddr) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new();
            let _ = manager.serve(addr).await;
        });
    });
    support::wait_accepting(addr, "start_manager");
}

fn start_extent_node(addr: SocketAddr, dir: std::path::PathBuf, disk_id: u64, mgr: &str) {
    use autumn_stream::{ExtentNode, ExtentNodeConfig};
    let mgr = mgr.to_string();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let config = ExtentNodeConfig::new(dir, disk_id).with_manager_endpoint(mgr);
            let n = ExtentNode::new(config).await.expect("extent node");
            let _ = n.serve(addr).await;
        });
    });
    support::wait_accepting(addr, "start_extent_node");
}

async fn register_node(mgr: &RpcClient, addr: &str, disk: &str) {
    let resp = mgr
        .call(
            MSG_REGISTER_NODE,
            rkyv_encode(&RegisterNodeReq {
                addr: addr.to_string(),
                disk_uuids: vec![disk.to_string()],
                shard_ports: vec![],
                control_address: String::new(),
                node_uuid: String::new(),
            }),
        )
        .await
        .expect("register node");
    let r: RegisterNodeResp = rkyv_decode(&resp).expect("decode");
    assert_eq!(r.code, CODE_OK, "register node: {}", r.message);
}

async fn create_stream_repl(mgr: &RpcClient, replicates: u32) -> (u64, u64) {
    let resp = mgr
        .call(
            MSG_CREATE_STREAM,
            rkyv_encode(&CreateStreamReq {
                replicates,
                ec_data_shard: replicates,
                ec_parity_shard: 0,
            }),
        )
        .await
        .expect("create_stream");
    let r: CreateStreamResp = rkyv_decode(&resp).expect("decode");
    assert_eq!(r.code, CODE_OK, "create_stream: {}", r.message);
    let s = r.stream.expect("stream info");
    (s.stream_id, s.extent_ids[0])
}

async fn call_update_stream_ec(
    mgr: &RpcClient,
    stream_id: u64,
    ec_data: u32,
    ec_parity: u32,
) -> UpdateStreamEcResp {
    let resp = mgr
        .call(
            MSG_UPDATE_STREAM_EC,
            rkyv_encode(&UpdateStreamEcReq {
                stream_id,
                ec_data_shard: ec_data,
                ec_parity_shard: ec_parity,
            }),
        )
        .await
        .expect("update_stream_ec");
    rkyv_decode(&resp).expect("decode UpdateStreamEcResp")
}

/// ec_data_shard < 2 is rejected with InvalidArgument.
#[test]
fn update_stream_ec_rejects_ec_data_below_two() {
    let mgr_addr = pick_addr();
    let n1_addr = pick_addr();
    let d1 = tempfile::tempdir().unwrap();
    start_manager(mgr_addr);
    start_extent_node(n1_addr, d1.path().to_path_buf(), 1, &mgr_addr.to_string());

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("connect");
        register_node(&mgr, &n1_addr.to_string(), "disk-1").await;
        let (stream_id, _) = create_stream_repl(&mgr, 1).await;

        // ec_data=1 is too small.
        let resp = call_update_stream_ec(&mgr, stream_id, 1, 1).await;
        assert_ne!(resp.code, CODE_OK, "should have been rejected");

        // ec_parity=0 is also invalid.
        let resp2 = call_update_stream_ec(&mgr, stream_id, 2, 0).await;
        assert_ne!(resp2.code, CODE_OK, "parity=0 should be rejected");
    });
}

/// Unknown stream_id returns a non-OK code.
#[test]
fn update_stream_ec_rejects_unknown_stream() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("connect");
        let resp = call_update_stream_ec(&mgr, 999999, 2, 1).await;
        assert_ne!(resp.code, CODE_OK, "unknown stream should fail");
    });
}
