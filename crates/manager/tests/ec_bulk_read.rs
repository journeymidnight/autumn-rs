//! A bulk read of an erasure-coded extent: a range inside one data shard is
//! read from that shard's node in one `MSG_READ_BYTES_BULK`
//! (`read_value_into_pooled`), at its offset inside the shard; a range across
//! two shards is left to the copy path (`Ok(None)`), which splits it; a shard
//! whose node cannot be reached is rebuilt from the other shards at once.

mod support;

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
    support::wait_accepting(addr, "start_manager");
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
    support::wait_accepting(addr, "start_extent_node");
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

struct Ec {
    mgr_str: String,
    mgr: Rc<RpcClient>,
    addrs: Vec<SocketAddr>,
    client: Rc<StreamClient>,
    extent_id: u64,
    payload: Vec<u8>,
}

/// Three nodes (registered under `ec-bulk-{i}`), an 8192-byte extent converted
/// to EC 2+1: shard 0 holds [0, 4096), shard 1 [4096, 8192).
async fn converted_extent(mgr_addr: SocketAddr, addrs: Vec<SocketAddr>) -> Ec {
    let mgr_str = mgr_addr.to_string();
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
                node_uuid: format!("ec-bulk-{i}"),
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
        "owner/ec-bulk-read/0".to_string(),
        256 * 1024 * 1024,
        Rc::new(ConnPool::new()),
    )
    .await
    .expect("stream client");
    let payload: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8 ^ (i / 251) as u8).collect();
    let written = client.append(stream_id, &payload).await.expect("append");
    assert_eq!(written.offset, 0, "the extent starts with this payload");
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
    let force: ForceEcConvertResp =
        call(&mgr, MSG_FORCE_EC_CONVERT, rkyv_encode(&ForceEcConvertReq { extent_id })).await;
    assert_eq!(force.code, CODE_OK, "force-ec-convert: {}", force.message);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let info: ExtentInfoResp =
            call(&mgr, MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id })).await;
        if info.extent.expect("extent").ec_converted {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "never converted");
        compio::time::sleep(Duration::from_millis(250)).await;
    }
    client.invalidate_extent_cache(extent_id);
    Ec { mgr_str, mgr, addrs, client, extent_id, payload }
}

fn start_cluster() -> (SocketAddr, Vec<SocketAddr>, Vec<tempfile::TempDir>) {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let addrs: Vec<SocketAddr> = (0..3).map(|_| pick_addr()).collect();
    for (i, a) in addrs.iter().enumerate() {
        start_extent_node(*a, dirs[i].path().to_path_buf(), i as u64 + 1, &mgr_addr.to_string());
    }
    (mgr_addr, addrs, dirs)
}

#[test]
fn a_range_inside_one_ec_shard_is_one_bulk_read() {
    let (mgr_addr, addrs, _dirs) = start_cluster();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let ec = converted_extent(mgr_addr, addrs).await;
        for (off, len) in [(100u64, 1000u64), (5000, 1000)] {
            let (buf, n) = ec
                .client
                .read_value_into_pooled(ec.extent_id, off, len)
                .await
                .expect("bulk read")
                .unwrap_or_else(|| panic!("[{off}, +{len}) lies in one shard: one bulk read"));
            assert_eq!(n as u64, len);
            assert_eq!(&buf.filled()[..n], &ec.payload[off as usize..(off + len) as usize], "[{off}, +{len}) read back");
        }
        let across = ec.client.read_value_into_pooled(ec.extent_id, 4000, 200).await.expect("bulk read");
        assert!(across.is_none(), "a range across two shards is the copy path's");
        let (read, _) = ec.client.read_bytes_from_extent(ec.extent_id, 4000, 200).await.expect("copy path");
        assert_eq!(read, &ec.payload[4000..4200], "and the copy path reads it");
    });
}

/// Shard 0's node moves to an address nobody listens on; a fresh client reads
/// a range of shard 0. The bulk read fails to connect and the range comes back
/// rebuilt from shard 1 and the parity, not as `Ok(None)` for the copy path to
/// try the same node again.
#[test]
fn a_shard_that_cannot_be_reached_is_rebuilt_at_once() {
    let (mgr_addr, addrs, _dirs) = start_cluster();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let ec = converted_extent(mgr_addr, addrs).await;
        let info: ExtentInfoResp =
            call(&ec.mgr, MSG_EXTENT_INFO, rkyv_encode(&ExtentInfoReq { extent_id: ec.extent_id })).await;
        let shard0_node = info.extent.expect("extent").replicates[0];
        let nodes: ListNodeStatesResp =
            call(&ec.mgr, MSG_LIST_NODE_STATES, rkyv_encode(&ListNodeStatesReq {})).await;
        let shard0_addr = nodes.nodes.iter().find(|n| n.node_id == shard0_node).expect("node").address.clone();
        let i = ec.addrs.iter().position(|a| a.to_string() == shard0_addr).expect("one of ours");
        let moved: RegisterNodeResp = call(
            &ec.mgr,
            MSG_REGISTER_NODE,
            rkyv_encode(&RegisterNodeReq {
                addr: pick_addr().to_string(),
                disk_uuids: vec![format!("disk-{i}")],
                shard_ports: vec![],
                control_address: String::new(),
                node_uuid: format!("ec-bulk-{i}"),
            }),
        )
        .await;
        assert_eq!(moved.code, CODE_OK, "re-register: {}", moved.message);
        assert_eq!(moved.node_id, shard0_node, "the same node, moved");

        let fresh = StreamClient::connect(
            &ec.mgr_str,
            "owner/ec-bulk-read/reader".to_string(),
            256 * 1024 * 1024,
            Rc::new(ConnPool::new()),
        )
        .await
        .expect("stream client");
        let (buf, n) = fresh
            .read_value_into_pooled(ec.extent_id, 100, 1000)
            .await
            .expect("bulk read")
            .expect("rebuilt from the other shards");
        assert_eq!(&buf.filled()[..n], &ec.payload[100..1100], "the rebuilt range");
    });
}
