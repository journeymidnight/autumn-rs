//! A marker is one attempt: when the EC coordinator reports that its attempt
//! failed on a participant, the manager cancels the marker at once — not after
//! a run of identical failures — and the op closes FAILED naming that node.

use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
use autumn_rpc::extent_rpc as ext;
use autumn_rpc::manager_rpc::*;
use autumn_stream::{ConnPool, ExtentNode, ExtentNodeConfig, StreamClient};

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

async fn call<T: rkyv::Archive>(mgr: &RpcClient, msg: u8, payload: bytes::Bytes) -> T
where
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<
            rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>,
        > + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
{
    let resp = mgr.call(msg, payload).await.expect("manager call");
    rkyv_decode::<T>(&resp).expect("decode")
}

async fn wait_all_nodes_online(mgr: &RpcClient) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let r: ListNodeStatesResp =
            call(mgr, MSG_LIST_NODE_STATES, rkyv_encode(&ListNodeStatesReq {})).await;
        if !r.nodes.is_empty() && r.nodes.iter().all(|n| n.auto_state == NODE_AUTO_STATE_ONLINE) {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "nodes never came Online");
        compio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[test]
fn a_participant_failure_cancels_the_attempt_and_names_the_node() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let mgr_str = mgr_addr.to_string();
    let addrs: Vec<SocketAddr> = (0..3).map(|_| pick_addr()).collect();
    for (i, a) in addrs.iter().enumerate() {
        start_extent_node(*a, dirs[i].path().to_path_buf(), i as u64 + 1, &mgr_str);
    }

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .expect("connect mgr");
        let mut node_addr = std::collections::HashMap::new();
        for (i, a) in addrs.iter().enumerate() {
            let r: RegisterNodeResp = call(
                &mgr,
                MSG_REGISTER_NODE,
                rkyv_encode(&RegisterNodeReq {
                    addr: a.to_string(),
                    disk_uuids: vec![format!("disk-{i}")],
                    shard_ports: vec![],
                    control_address: String::new(),
                    node_uuid: String::new(),
                }),
            )
            .await;
            assert_eq!(r.code, CODE_OK, "register: {}", r.message);
            node_addr.insert(r.node_id, *a);
        }
        let created: CreateStreamResp = call(
            &mgr,
            MSG_CREATE_STREAM,
            rkyv_encode(&CreateStreamReq { replicates: 2, ec_data_shard: 2, ec_parity_shard: 1 }),
        )
        .await;
        let stream_id = created.stream.as_ref().unwrap().stream_id;
        let client = StreamClient::connect(
            &mgr_str,
            "owner/ec-attempt/0".to_string(),
            256 * 1024 * 1024,
            Rc::new(ConnPool::new()),
        )
        .await
        .expect("stream client");
        let written = client.append(stream_id, &vec![9u8; 8192]).await.expect("append");
        let extent_id = written.extent_id;
        let seal: StreamAllocExtentResp = call(
            &mgr,
            MSG_STREAM_ALLOC_EXTENT,
            rkyv_encode(&StreamAllocExtentReq {
                stream_id,
                owner_key: client.owner_key().to_string(),
                owner_epoch: client.owner_epoch(),
                seal_commit: Some(written.end),
                exclude_node_ids: vec![],
                seal_extent_id: 0,
            }),
        )
        .await;
        assert_eq!(seal.code, CODE_OK, "seal: {}", seal.message);
        let info: ExtentInfoResp =
            call(&mgr, MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id })).await;
        let participant = info.extent.expect("extent").replicates[1];

        // The second replica already stages this extent for a far newer
        // attempt, so it refuses the manager's attempt's shard.
        let staged = ConnPool::new()
            .call(
                &node_addr[&participant].to_string(),
                ext::MSG_WRITE_SHARD,
                ext::WriteShardReq {
                    extent_id,
                    shard_index: 1,
                    sealed_length: 8192,
                    eversion: 2,
                    owner_epoch: 0,
                    shard_offset: 0,
                    attempt_nonce: u64::MAX / 2,
                    payload: bytes::Bytes::from(vec![0u8; 16]),
                }
                .encode(),
            )
            .await
            .expect("stage");
        assert_eq!(ext::WriteShardResp::decode(staged).unwrap().code, ext::CODE_OK);

        wait_all_nodes_online(&mgr).await;
        let force: ForceEcConvertResp =
            call(&mgr, MSG_FORCE_EC_CONVERT, rkyv_encode(&ForceEcConvertReq { extent_id })).await;
        assert_eq!(force.code, CODE_OK, "force-ec-convert: {}", force.message);

        // Two dispatch ticks: one starts the attempt, the next hears it failed.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let op = loop {
            let q: OpQueryResp = call(
                &mgr,
                MSG_OP_QUERY,
                rkyv_encode(&OpQueryReq { kind_filter: OP_KIND_EC_CONVERT, ..Default::default() }),
            )
            .await;
            if let Some(op) = q.ops.into_iter().find(|o| {
                o.secondary_id == extent_id
                    && o.state != OP_STATE_RUNNING
                    && o.state != OP_STATE_PENDING
            }) {
                break op;
            }
            assert!(std::time::Instant::now() < deadline, "the attempt was never cancelled");
            compio::time::sleep(Duration::from_millis(250)).await;
        };
        assert_eq!(op.state, OP_STATE_FAILED, "{op:?}");
        assert!(
            op.error.contains(&format!("node {participant} failed the conversion")),
            "the op names the participant: {}",
            op.error
        );
        let info: ExtentInfoResp =
            call(&mgr, MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id })).await;
        assert!(!info.extent.expect("extent").ec_converted, "nothing was committed");
    });
}
