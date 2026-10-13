//! EC conversion onto health nodes: a replica on a node in maintenance is no
//! reason to refuse — a health node takes its shard, and the conversion leaves
//! the maintenance node out of the layout.

use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_rpc::client::RpcClient;
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
fn a_replica_in_maintenance_is_substituted_by_a_health_node() {
    let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let mgr_str = mgr_addr.to_string();
    let addrs: Vec<SocketAddr> = (0..4).map(|_| pick_addr()).collect();
    for (i, a) in addrs.iter().enumerate() {
        start_extent_node(*a, dirs[i].path().to_path_buf(), i as u64 + 1, &mgr_str);
    }

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .expect("connect mgr");
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
        }
        wait_all_nodes_online(&mgr).await;
        let created: CreateStreamResp = call(
            &mgr,
            MSG_CREATE_STREAM,
            rkyv_encode(&CreateStreamReq { replicates: 2, ec_data_shard: 2, ec_parity_shard: 1 }),
        )
        .await;
        let stream_id = created.stream.as_ref().unwrap().stream_id;
        let client = StreamClient::connect(
            &mgr_str,
            "owner/ec-substitute/0".to_string(),
            256 * 1024 * 1024,
            Rc::new(ConnPool::new()),
        )
        .await
        .expect("stream client");
        let payload: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
        let written = client.append(stream_id, &payload).await.expect("append");
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
        let away = info.extent.expect("extent").replicates[1];

        let set: CodeResp = call(
            &mgr,
            MSG_SET_NODE_MAINTENANCE,
            rkyv_encode(&SetNodeMaintenanceReq {
                node_id: away,
                reason: "test".into(),
                set_by: "test".into(),
                expire_at: 0,
            }),
        )
        .await;
        assert_eq!(set.code, CODE_OK, "maintenance: {}", set.message);
        let force: ForceEcConvertResp =
            call(&mgr, MSG_FORCE_EC_CONVERT, rkyv_encode(&ForceEcConvertReq { extent_id })).await;
        assert_eq!(force.code, CODE_OK, "force-ec-convert: {}", force.message);

        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let ex = loop {
            let info: ExtentInfoResp =
                call(&mgr, MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id })).await;
            let ex = info.extent.expect("extent");
            if ex.ec_converted {
                break ex;
            }
            assert!(std::time::Instant::now() < deadline, "never converted");
            compio::time::sleep(Duration::from_millis(250)).await;
        };
        let layout: Vec<u64> = ex.replicates.iter().chain(ex.parity.iter()).copied().collect();
        assert_eq!(layout.len(), 3, "{layout:?}");
        assert!(!layout.contains(&away), "the maintenance node is out of the layout: {layout:?}");

        client.invalidate_extent_cache(extent_id);
        let (read, _) = client
            .read_bytes_from_extent(extent_id, written.offset, written.end - written.offset)
            .await
            .expect("read after conversion");
        assert_eq!(read, payload, "the converted extent reads back");
    });
}
