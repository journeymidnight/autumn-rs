/// Integration tests for MSG_UPDATE_STREAM_EC (FOPS-03).
use std::net::SocketAddr;
use std::time::Duration;

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
    std::thread::sleep(Duration::from_millis(200));
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
    std::thread::sleep(Duration::from_millis(200));
}

async fn register_node(mgr: &RpcClient, addr: &str, disk: &str) {
    let resp = mgr
        .call(
            MSG_REGISTER_NODE,
            rkyv_encode(&RegisterNodeReq {
                addr: addr.to_string(),
                disk_uuids: vec![disk.to_string()],
                shard_ports: vec![],
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

async fn get_stream_info(mgr: &RpcClient, stream_id: u64) -> MgrStreamInfo {
    let resp = mgr
        .call(
            MSG_STREAM_INFO,
            rkyv_encode(&StreamInfoReq { stream_ids: vec![stream_id] }),
        )
        .await
        .expect("stream_info");
    let r: StreamInfoResp = rkyv_decode(&resp).expect("decode StreamInfoResp");
    r.streams.into_iter().next().expect("stream info").1
}

async fn get_extent_info(mgr: &RpcClient, extent_id: u64) -> MgrExtentInfo {
    let resp = mgr
        .call(
            MSG_EXTENT_INFO,
            rkyv_encode(&ExtentInfoReq { extent_id }),
        )
        .await
        .expect("extent_info");
    let r: ExtentInfoResp = rkyv_decode(&resp).expect("decode ExtentInfoResp");
    r.extent.expect("extent info")
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

/// Updating a pure-replication stream to EC 2+1 succeeds and the manager
/// reflects the new EC shape.
#[test]
fn update_stream_ec_sets_ec_fields() {
    let mgr_addr = pick_addr();
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n3_addr = pick_addr();
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();

    start_manager(mgr_addr);
    let mgr_str = mgr_addr.to_string();
    start_extent_node(n1_addr, d1.path().to_path_buf(), 1, &mgr_str);
    start_extent_node(n2_addr, d2.path().to_path_buf(), 2, &mgr_str);
    start_extent_node(n3_addr, d3.path().to_path_buf(), 3, &mgr_str);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_node(&mgr, &n1_addr.to_string(), "disk-1").await;
        register_node(&mgr, &n2_addr.to_string(), "disk-2").await;
        register_node(&mgr, &n3_addr.to_string(), "disk-3").await;

        let (stream_id, _extent_id) = create_stream_repl(&mgr, 3).await;

        // Confirm starting shape is 3+0 (3-replica stream).
        let info = get_stream_info(&mgr, stream_id).await;
        assert_eq!(info.ec_data_shard, 3);
        assert_eq!(info.ec_parity_shard, 0);

        // Update to 2+1.
        let resp = call_update_stream_ec(&mgr, stream_id, 2, 1).await;
        assert_eq!(resp.code, CODE_OK, "update_stream_ec: {}", resp.message);
        let updated = resp.stream.expect("updated stream");
        assert_eq!(updated.ec_data_shard, 2);
        assert_eq!(updated.ec_parity_shard, 1);

        // Manager in-memory state reflects the change.
        let info2 = get_stream_info(&mgr, stream_id).await;
        assert_eq!(info2.ec_data_shard, 2);
        assert_eq!(info2.ec_parity_shard, 1);
    });
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
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect");
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
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect");
        let resp = call_update_stream_ec(&mgr, 999999, 2, 1).await;
        assert_ne!(resp.code, CODE_OK, "unknown stream should fail");
    });
}

/// After updating stream EC to 2+1, the ec_conversion_dispatch_loop converts
/// a sealed extent within ~30 seconds. Requires 3 extent nodes.
#[test]
fn update_stream_ec_triggers_conversion() {
    let mgr_addr = pick_addr();
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n3_addr = pick_addr();
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let d3 = tempfile::tempdir().unwrap();

    start_manager(mgr_addr);
    let mgr_str = mgr_addr.to_string();
    start_extent_node(n1_addr, d1.path().to_path_buf(), 1, &mgr_str);
    start_extent_node(n2_addr, d2.path().to_path_buf(), 2, &mgr_str);
    start_extent_node(n3_addr, d3.path().to_path_buf(), 3, &mgr_str);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        use std::rc::Rc;
        use autumn_stream::{ConnPool, StreamClient};

        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_node(&mgr, &n1_addr.to_string(), "disk-1").await;
        register_node(&mgr, &n2_addr.to_string(), "disk-2").await;
        register_node(&mgr, &n3_addr.to_string(), "disk-3").await;

        // Create a 3-replica stream with no EC.
        let (stream_id, first_extent_id) = create_stream_repl(&mgr, 3).await;

        let pool = Rc::new(ConnPool::new());
        let sc = StreamClient::connect(
            &mgr_addr.to_string(),
            "owner/update-ec-conv/0".to_string(),
            256 * 1024 * 1024,
            Rc::clone(&pool),
        )
        .await
        .expect("stream client");

        let payload: Vec<u8> = vec![0xABu8; 4096];
        let result = sc.append(stream_id, &payload, false).await.expect("append");

        // Seal by calling MSG_STREAM_ALLOC_EXTENT (mirrors ec_failover.rs pattern).
        let seal_resp = mgr
            .call(
                MSG_STREAM_ALLOC_EXTENT,
                rkyv_encode(&StreamAllocExtentReq {
                    stream_id,
                    owner_key: sc.owner_key().to_string(),
                    revision: sc.revision(),
                    end: result.end,
                }),
            )
            .await
            .expect("stream_alloc_extent");
        let seal_info: StreamAllocExtentResp = rkyv_decode(&seal_resp).expect("decode seal");
        assert_eq!(seal_info.code, CODE_OK, "seal failed: {}", seal_info.message);

        // Confirm extent is sealed.
        let ext_info = get_extent_info(&mgr, first_extent_id).await;
        assert!(ext_info.sealed_length > 0, "extent should be sealed");

        // Now update the stream to EC 2+1.
        let resp = call_update_stream_ec(&mgr, stream_id, 2, 1).await;
        assert_eq!(resp.code, CODE_OK, "update_stream_ec: {}", resp.message);

        // Wait up to 30 s for the ec_conversion_dispatch_loop (fires every 5 s).
        let mut converted = false;
        for _ in 0..15 {
            compio::time::sleep(Duration::from_secs(2)).await;
            let ext = get_extent_info(&mgr, first_extent_id).await;
            if ext.ec_converted {
                converted = true;
                break;
            }
        }
        assert!(converted, "extent should have been EC-converted within 30 s");
    });
}
