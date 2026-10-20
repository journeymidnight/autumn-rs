mod support;

use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_partition_server::PartitionServer;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc::{self};
use autumn_stream::{ConnPool, ExtentNode, ExtentNodeConfig, StreamClient};

fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr
}

/// Start a manager on its own thread and return its address.
fn start_manager(mgr_addr: SocketAddr) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new();
            let _ = manager.serve(mgr_addr).await;
        });
    });
    support::wait_accepting(mgr_addr, "start_manager");
}

/// Start an extent node on its own thread and return its address.
fn start_extent_node(addr: SocketAddr, dir: std::path::PathBuf, disk_id: u64) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let n = ExtentNode::new(ExtentNodeConfig::new(dir, disk_id))
                .await
                .expect("extent node");
            let _ = n.serve(addr).await;
        });
    });
    support::wait_accepting(addr, "start_extent_node");
}

/// Register a node with the manager via RPC.
async fn register_node(mgr: &RpcClient, addr: &str, disk_uuid: &str) -> RegisterNodeResp {
    let resp = mgr
        .call(
            MSG_REGISTER_NODE,
            rkyv_encode(&RegisterNodeReq {
                addr: addr.to_string(),
                disk_uuids: vec![disk_uuid.to_string()],
                shard_ports: vec![],
                control_address: String::new(),
                node_uuid: String::new(),
            }),
        )
        .await
        .expect("register node");
    rkyv_decode::<RegisterNodeResp>(&resp).expect("decode RegisterNodeResp")
}

/// Create a stream via RPC and return its stream_id.
async fn create_stream(mgr: &RpcClient, replicates: u32) -> u64 {
    let mgr = RpcClient::connect_as(mgr.peer_addr(), autumn_rpc::version_hello::Role::Admin,
        Some(autumn_rpc::version_hello::Service::Manager)).await.unwrap();
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
        .expect("create stream");
    let created: CreateStreamResp = rkyv_decode(&resp).expect("decode CreateStreamResp");
    created.stream.expect("stream").stream_id
}

/// Helper: send a PutReq to a partition server via RpcClient.
async fn ps_put(ps: &RpcClient, part_id: u64, key: &[u8], value: &[u8]) {
    let resp = ps
        .call(
            partition_rpc::MSG_PUT,
            partition_rpc::rkyv_encode(&partition_rpc::PutReq {
                part_id,
                key: key.to_vec(),
                value: value.to_vec(),
                expires_at: 0,
                region_epoch: 0,
            inode_hint: 0,
            lease_epoch: 0,
            }),
        )
        .await
        .expect("put");
    let _: partition_rpc::PutResp = partition_rpc::rkyv_decode(&resp).expect("decode PutResp");
}

/// A point read's outcome: `CODE_*`, the PS's message, and the value bytes.
#[allow(dead_code)]
struct GetResult {
    code: u8,
    message: String,
    value: Vec<u8>,
}

/// Helper: read a key from a partition server through `MSG_GET_BULK`.
async fn ps_get(ps: &RpcClient, part_id: u64, key: &[u8]) -> GetResult {
    let resp = ps
        .call_into_pooled(
            partition_rpc::MSG_GET_BULK,
            partition_rpc::rkyv_encode(&partition_rpc::GetReq {
                part_id,
                key: key.to_vec(),
                offset: 0,
                length: 0,
                region_epoch: 0,
            }),
        )
        .await
        .expect("get");
    GetResult {
        code: resp.code,
        message: resp.message,
        value: resp.buf.filled().to_vec(),
    }
}

/// Helper: flush a partition via the Maintenance RPC.
async fn ps_flush(ps: &RpcClient, part_id: u64) {
    let resp = ps
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id,
                op: partition_rpc::MAINTENANCE_FLUSH,
                extent_ids: vec![],
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
        .expect("flush");
    let r: partition_rpc::MaintenanceResp =
        partition_rpc::rkyv_decode(&resp).expect("decode MaintenanceResp");
    assert_eq!(
        r.code,
        partition_rpc::CODE_OK,
        "flush failed: {}",
        r.message
    );
}

/// Helper: upsert a partition via the manager RPC.
async fn upsert_partition(
    mgr: &RpcClient,
    part_id: u64,
    log_stream: u64,
    row_stream: u64,
    meta_stream: u64,
    start_key: &[u8],
    end_key: &[u8],
) {
    let mgr = RpcClient::connect_as(mgr.peer_addr(), autumn_rpc::version_hello::Role::Admin,
        Some(autumn_rpc::version_hello::Service::Manager)).await.unwrap();
    let resp = mgr
        .call(
            MSG_UPSERT_PARTITION,
            rkyv_encode(&UpsertPartitionReq {
                meta: MgrPartitionMeta {
                    part_id,
                    log_stream,
                    row_stream,
                    meta_stream,
                    rg: Some(MgrRange {
                        start_key: start_key.to_vec(),
                        end_key: end_key.to_vec(),
                    }),
                },
            }),
        )
        .await
        .expect("upsert partition");
    // UpsertPartitionResp added a `part_id: u64` field after this test
    // was written; decoding as CodeResp would read those bytes as the
    // tail of `message` and fail UTF-8 validation.
    let r: UpsertPartitionResp = rkyv_decode(&resp).expect("decode UpsertPartitionResp");
    assert_eq!(r.code, CODE_OK, "upsert_partition failed: {}", r.message);
}

/// Start a partition server on its own thread.
///
/// use `connect_with_advertise_and_port` so `base_port` is
/// populated BEFORE the implicit `sync_regions_once()` inside
/// `finish_connect()` fires. See
/// `crates/manager/tests/support/mod.rs::start_partition_server` for
/// the full explanation.
fn start_partition_server(ps_id: u64, mgr_addr: SocketAddr, ps_addr: SocketAddr) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let advertise = ps_addr.to_string();
            let ps = PartitionServer::connect_with_advertise_and_port(
                ps_id,
                &mgr_addr.to_string(),
                Some(advertise),
                ps_addr,
            )
            .await
            .expect("connect partition server");
            ps.sync_regions_once().await.expect("sync regions");
            let _ = ps.serve(ps_addr).await;
        });
    });
    support::wait_accepting(ps_addr, "start_partition_server");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn partition_server_recovery_replays_table_and_wal() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n1_dir = tempfile::tempdir().expect("n1 tempdir");
    let n2_dir = tempfile::tempdir().expect("n2 tempdir");
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("connect mgr");

        register_node(&mgr, &n1_addr.to_string(), "disk-rp1").await;
        register_node(&mgr, &n2_addr.to_string(), "disk-rp2").await;

        let log_stream = create_stream(&mgr, 1).await;
        let row_stream = create_stream(&mgr, 1).await;
        let meta_stream = create_stream(&mgr, 1).await;

        upsert_partition(&mgr, 511, log_stream, row_stream, meta_stream, b"a", b"z").await;

        // First PS
        let ps1_addr = pick_addr();
        start_partition_server(22, mgr_addr, ps1_addr);

        let ps1 = RpcClient::connect_as(ps1_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("connect ps1");

        ps_put(&ps1, 511, b"a-flush", b"flushed-value").await;
        ps_flush(&ps1, 511).await;
        ps_put(&ps1, 511, b"a-wal-1", b"v1").await;
        ps_put(&ps1, 511, b"a-wal-2", b"v2").await;

        // Drop ps1 connection (server thread keeps running but we don't care)
        drop(ps1);
        std::thread::sleep(Duration::from_millis(200));

        // Second PS (recovery)
        let ps2_addr = pick_addr();
        start_partition_server(22, mgr_addr, ps2_addr);

        let ps2 = RpcClient::connect_as(ps2_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("connect ps2");

        let got_flush = ps_get(&ps2, 511, b"a-flush").await;
        assert_eq!(got_flush.value, b"flushed-value");

        let got_wal_1 = ps_get(&ps2, 511, b"a-wal-1").await;
        assert_eq!(got_wal_1.value, b"v1");

        let got_wal_2 = ps_get(&ps2, 511, b"a-wal-2").await;
        assert_eq!(got_wal_2.value, b"v2");
    });
}

#[test]
fn stream_append_commit_punchhole_truncate_flow() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n1_dir = tempfile::tempdir().expect("n1 tempdir");
    let n2_dir = tempfile::tempdir().expect("n2 tempdir");
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("connect mgr");

        register_node(&mgr, &n1_addr.to_string(), "disk-e").await;
        register_node(&mgr, &n2_addr.to_string(), "disk-f").await;

        let stream_id = create_stream(&mgr, 1).await;

        let pool = Rc::new(ConnPool::new());
        let client =
            StreamClient::connect(&mgr_addr.to_string(), "owner/e2e/1".to_string(), 8, pool)
                .await
                .expect("stream client");

        let first_batch = [b"hello".as_slice(), b"world!!!".as_slice()];
        let b1 = client
            .append_batch(stream_id, &first_batch)
            .await
            .expect("batch append 1");
        assert_eq!(b1.offset, 0);
        let _a3 = client.append(stream_id, b"z").await.expect("append 3");

        let committed = client
            .commit_length(stream_id)
            .await
            .expect("commit length");
        assert!(committed > 0);

        let resp = mgr
            .call(
                MSG_STREAM_INFO,
                rkyv_encode(&StreamInfoReq {
                    stream_ids: vec![stream_id],
                }),
            )
            .await
            .expect("stream_info");
        let info: StreamInfoResp = rkyv_decode(&resp).expect("decode");
        let stream = &info
            .streams
            .iter()
            .find(|(id, _)| *id == stream_id)
            .expect("stream exists")
            .1;
        assert!(stream.extent_ids.len() >= 2);
        let first = stream.extent_ids[0];
        let second = stream.extent_ids[1];

        let after_trunc = client.truncate(stream_id, second).await.expect("truncate");
        assert_eq!(after_trunc.extent_ids[0], second);

        let _ = client
            .punch_holes(stream_id, vec![first])
            .await
            .expect("punchhole");
    });
}

#[test]
fn stream_append_and_read_blocks_flow() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n1_dir = tempfile::tempdir().expect("n1 tempdir");
    let n2_dir = tempfile::tempdir().expect("n2 tempdir");
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("connect mgr");

        register_node(&mgr, &n1_addr.to_string(), "disk-r1").await;
        register_node(&mgr, &n2_addr.to_string(), "disk-r2").await;

        let stream_id = create_stream(&mgr, 1).await;

        let pool = Rc::new(ConnPool::new());
        let client = StreamClient::connect(
            &mgr_addr.to_string(),
            "owner/read/1".to_string(),
            512 * 1024 * 1024,
            pool,
        )
        .await
        .expect("stream client");

        let batch = [b"hello".as_slice(), b"world".as_slice()];
        let wr = client
            .append_batch(stream_id, &batch)
            .await
            .expect("append batch");
        assert_eq!(wr.offset, 0);
        let wr2 = client.append(stream_id, b"!").await.expect("append third");
        assert_eq!(wr2.offset, 10);

        // Read all bytes via StreamClient
        let resp = mgr
            .call(
                MSG_STREAM_INFO,
                rkyv_encode(&StreamInfoReq {
                    stream_ids: vec![stream_id],
                }),
            )
            .await
            .expect("stream_info");
        let info: StreamInfoResp = rkyv_decode(&resp).expect("decode");
        let stream = &info
            .streams
            .iter()
            .find(|(id, _)| *id == stream_id)
            .expect("stream exists")
            .1;
        let extent_id = *stream.extent_ids.last().expect("tail extent");

        // Read all bytes from the extent
        let (payload_buf, end) = client
            .read_bytes_from_extent(extent_id, 0, 0)
            .await
            .expect("read bytes");
        assert_eq!(end, 11); // "hello" + "world" + "!" = 11 bytes
        assert_eq!(payload_buf, b"helloworld!");

        // Read just the last byte via byte-range read
        let (last_payload, _) = client
            .read_bytes_from_extent(extent_id, 10, 1)
            .await
            .expect("read last byte");
        assert_eq!(last_payload, b"!");
    });
}

// ---------------------------------------------------------------------------
// three-stream model tests
// ---------------------------------------------------------------------------

/// Helper: spin up manager + 2 extent nodes.
fn setup_infra_f030(
    node_id_base: u64,
) -> (
    SocketAddr,
    SocketAddr,
    SocketAddr,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_sock = pick_addr();
    let n2_sock = pick_addr();
    let n1_dir = tempfile::tempdir().expect("n1 tempdir");
    let n2_dir = tempfile::tempdir().expect("n2 tempdir");

    start_extent_node(n1_sock, n1_dir.path().to_path_buf(), node_id_base);
    start_extent_node(n2_sock, n2_dir.path().to_path_buf(), node_id_base + 1);

    (mgr_addr, n1_sock, n2_sock, n1_dir, n2_dir)
}

/// Register nodes after connect.
async fn register_infra_nodes(
    mgr: &RpcClient,
    n1_addr: SocketAddr,
    n2_addr: SocketAddr,
    node_id_base: u64,
) {
    register_node(
        mgr,
        &n1_addr.to_string(),
        &format!("disk-f030-{}", node_id_base),
    )
    .await;
    register_node(
        mgr,
        &n2_addr.to_string(),
        &format!("disk-f030-{}", node_id_base + 1),
    )
    .await;
}

/// Create 3 streams (log, row, meta) and return their IDs.
async fn create_three_streams(mgr: &RpcClient) -> (u64, u64, u64) {
    let log_stream = create_stream(mgr, 1).await;
    let row_stream = create_stream(mgr, 1).await;
    let meta_stream = create_stream(mgr, 1).await;
    (log_stream, row_stream, meta_stream)
}

#[test]
fn recovery_from_meta_and_row_streams() {
    let (mgr_addr, n1_addr, n2_addr, _n1_dir, _n2_dir) = setup_infra_f030(103);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("connect mgr");
        register_infra_nodes(&mgr, n1_addr, n2_addr, 103).await;

        let (log_stream, row_stream, meta_stream) = create_three_streams(&mgr).await;

        upsert_partition(&mgr, 611, log_stream, row_stream, meta_stream, b"a", b"z").await;

        // First PS: write one flushed key + one WAL-only key. The flushed
        // value is above VALUE_THROTTLE (4 KiB), so the SST holds a pointer
        // into the log stream rather than the bytes.
        let ps1_addr = pick_addr();
        start_partition_server(42, mgr_addr, ps1_addr);

        let streamed: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
        let ps1 = RpcClient::connect_as(ps1_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("connect ps1");
        ps_put(&ps1, 611, b"a-streamed", &streamed).await;
        ps_flush(&ps1, 611).await;
        ps_put(&ps1, 611, b"a-wal-only", b"small").await;

        drop(ps1);
        std::thread::sleep(Duration::from_millis(200));

        // Second PS: recover
        let ps2_addr = pick_addr();
        start_partition_server(42, mgr_addr, ps2_addr);

        let ps2 = RpcClient::connect_as(ps2_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("connect ps2");

        let v1 = ps_get(&ps2, 611, b"a-streamed").await;
        assert!(v1.value == streamed, "stream-backed SST key survives restart");

        let v2 = ps_get(&ps2, 611, b"a-wal-only").await;
        assert_eq!(v2.value, b"small", "WAL-only key survives restart");
    });
}

// ---------------------------------------------------------------------------
// value log separation tests
// ---------------------------------------------------------------------------

#[test]
fn recovery_replays_log_stream() {
    let (mgr_addr, n1_addr, n2_addr, _n1_dir, _n2_dir) = setup_infra_f030(109);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("connect mgr");
        register_infra_nodes(&mgr, n1_addr, n2_addr, 109).await;

        let (log_stream, row_stream, meta_stream) = create_three_streams(&mgr).await;

        upsert_partition(&mgr, 711, log_stream, row_stream, meta_stream, b"a", b"z").await;

        // First PS
        let ps1_addr = pick_addr();
        start_partition_server(52, mgr_addr, ps1_addr);

        let ps1 = RpcClient::connect_as(ps1_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("connect ps1");

        let large_val: Vec<u8> = (0u8..=255).cycle().take(8 * 1024).collect();
        ps_put(&ps1, 711, b"b-large", &large_val).await;
        ps_flush(&ps1, 711).await;
        ps_put(&ps1, 711, b"b-wal-small", b"small-wal").await;

        drop(ps1);
        std::thread::sleep(Duration::from_millis(200));

        // Second PS: recover
        let ps2_addr = pick_addr();
        start_partition_server(52, mgr_addr, ps2_addr);

        let ps2 = RpcClient::connect_as(ps2_addr, autumn_rpc::version_hello::Role::Admin, None).await.expect("connect ps2");

        let got_large = ps_get(&ps2, 711, b"b-large").await;
        assert_eq!(
            got_large.value, large_val,
            "large value must survive restart via logStream"
        );

        let got_small = ps_get(&ps2, 711, b"b-wal-small").await;
        assert_eq!(
            got_small.value, b"small-wal",
            "small WAL key must survive restart"
        );
    });
}

// ---------------------------------------------------------------------------
// Partition split with overlap detection and major compaction
// ---------------------------------------------------------------------------
