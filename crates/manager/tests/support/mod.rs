//! Shared test infrastructure for autumn-manager integration tests.
//!
//! Provides helper functions for starting components, RPC helpers,
//! and a ShutdownHandle for controlled node lifecycle management.

use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_partition_server::PartitionServer;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc::{self, TableLocations};
use autumn_stream::{ConnPool, ExtentNode, ExtentNodeConfig, StreamClient};

// ── ShutdownHandle ────────────────────────────────────────────────────

/// A handle that allows controlled shutdown of a component running on
/// a separate thread. The component checks `is_shutdown()` periodically
/// and exits when true. Dropping the handle signals shutdown.
#[derive(Clone)]
pub struct ShutdownFlag(pub Arc<AtomicBool>);

impl ShutdownFlag {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    pub fn shutdown(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_shutdown(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

// ── Address allocation ────────────────────────────────────────────────

/// Pick a random available port on loopback.
pub fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr
}

// ── Component startup ─────────────────────────────────────────────────

/// Start a manager (no etcd) on its own thread.
pub fn start_manager(mgr_addr: SocketAddr) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = AutumnManager::new();
            let _ = manager.serve(mgr_addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));
}

/// Start an extent node on its own thread.
pub fn start_extent_node(addr: SocketAddr, dir: std::path::PathBuf, disk_id: u64) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let n = ExtentNode::new(ExtentNodeConfig::new(dir, disk_id))
                .await
                .expect("extent node");
            let _ = n.serve(addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));
}

/// Start a partition server on its own thread.
///
/// F099-K note: `PartitionServer::connect()` implicitly calls
/// `sync_regions_once()` during `finish_connect`. That sync opens any
/// partitions already assigned to this PS via `open_partition`, which
/// binds a dedicated per-partition listener on `base_port + ord`.
/// Pre-fix, `base_port` was only set inside `serve()`; tests that
/// `upsert_partition` BEFORE `connect` would therefore have
/// `finish_connect`'s implicit sync try to bind port `0 + ord`, failing
/// with EACCES / EADDRINUSE. We now use `connect_with_advertise_and_port`
/// which seeds `base_port` + `advertise_host` BEFORE `finish_connect`
/// runs its implicit sync; the subsequent explicit `sync_regions_once`
/// is a no-op for already-open partitions and `serve()` reinstates the
/// same base_port (idempotent). See
/// `crates/partition-server/src/lib.rs::bind_listen_addr`.
pub fn start_partition_server(ps_id: u64, mgr_addr: SocketAddr, ps_addr: SocketAddr) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            // Match production (`autumn-ps`): pass the advertise addr so
            // `bind_listen_addr` can derive the advertise host from it.
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
    std::thread::sleep(Duration::from_millis(300));
}

// ── Manager RPC helpers ───────────────────────────────────────────────

/// Register an extent node with the manager.
pub async fn register_node(mgr: &RpcClient, addr: &str, disk_uuid: &str) -> RegisterNodeResp {
    let resp = mgr
        .call(
            MSG_REGISTER_NODE,
            rkyv_encode(&RegisterNodeReq {
                addr: addr.to_string(),
                disk_uuids: vec![disk_uuid.to_string()],
                shard_ports: vec![],
            }),
        )
        .await
        .expect("register node");
    rkyv_decode::<RegisterNodeResp>(&resp).expect("decode RegisterNodeResp")
}

/// Create a replicated stream.
pub async fn create_stream(mgr: &RpcClient, replicates: u32) -> u64 {
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

/// Create three streams (log, row, meta) for a partition.
pub async fn create_three_streams(mgr: &RpcClient) -> (u64, u64, u64) {
    let log = create_stream(mgr, 2).await;
    let row = create_stream(mgr, 2).await;
    let meta = create_stream(mgr, 2).await;
    (log, row, meta)
}

/// Upsert a partition via manager RPC.
pub async fn upsert_partition(
    mgr: &RpcClient,
    part_id: u64,
    log_stream: u64,
    row_stream: u64,
    meta_stream: u64,
    start_key: &[u8],
    end_key: &[u8],
) {
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
    let r: CodeResp = rkyv_decode(&resp).expect("decode CodeResp");
    assert_eq!(r.code, CODE_OK, "upsert_partition failed: {}", r.message);
}

/// Get regions from manager.
pub async fn get_regions(mgr: &RpcClient) -> GetRegionsResp {
    let resp = mgr
        .call(MSG_GET_REGIONS, bytes::Bytes::new())
        .await
        .expect("get_regions");
    rkyv_decode::<GetRegionsResp>(&resp).expect("decode GetRegionsResp")
}

// ── Partition Server RPC helpers ──────────────────────────────────────

/// Put a key-value pair.
pub async fn ps_put(
    ps: &RpcClient,
    part_id: u64,
    key: &[u8],
    value: &[u8],
    must_sync: bool,
) {
    let resp = ps
        .call(
            partition_rpc::MSG_PUT,
            partition_rpc::rkyv_encode(&partition_rpc::PutReq {
                part_id,
                key: key.to_vec(),
                value: value.to_vec(),
                must_sync,
                expires_at: 0,
            }),
        )
        .await
        .expect("put");
    let _: partition_rpc::PutResp = partition_rpc::rkyv_decode(&resp).expect("decode PutResp");
}

/// Get a key's value.
pub async fn ps_get(ps: &RpcClient, part_id: u64, key: &[u8]) -> partition_rpc::GetResp {
    let resp = ps
        .call(
            partition_rpc::MSG_GET,
            partition_rpc::rkyv_encode(&partition_rpc::GetReq {
                part_id,
                key: key.to_vec(),
                offset: 0,
                length: 0,
            }),
        )
        .await
        .expect("get");
    partition_rpc::rkyv_decode(&resp).expect("decode GetResp")
}

/// Flush a partition's memtable.
pub async fn ps_flush(ps: &RpcClient, part_id: u64) {
    let resp = ps
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id,
                op: partition_rpc::MAINTENANCE_FLUSH,
                extent_ids: vec![],
            }),
        )
        .await
        .expect("flush");
    let r: partition_rpc::MaintenanceResp =
        partition_rpc::rkyv_decode(&resp).expect("decode MaintenanceResp");
    assert_eq!(r.code, partition_rpc::CODE_OK, "flush failed: {}", r.message);
}

/// Trigger major compaction.
pub async fn ps_compact(ps: &RpcClient, part_id: u64) {
    let resp = ps
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id,
                op: partition_rpc::MAINTENANCE_COMPACT,
                extent_ids: vec![],
            }),
        )
        .await
        .expect("compact");
    let r: partition_rpc::MaintenanceResp =
        partition_rpc::rkyv_decode(&resp).expect("decode MaintenanceResp");
    assert_eq!(r.code, partition_rpc::CODE_OK, "compact failed: {}", r.message);
}

/// Trigger GC.
pub async fn ps_gc(ps: &RpcClient, part_id: u64) {
    let resp = ps
        .call(
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::rkyv_encode(&partition_rpc::MaintenanceReq {
                part_id,
                op: partition_rpc::MAINTENANCE_AUTO_GC,
                extent_ids: vec![],
            }),
        )
        .await
        .expect("gc");
    let r: partition_rpc::MaintenanceResp =
        partition_rpc::rkyv_decode(&resp).expect("decode MaintenanceResp");
    assert_eq!(r.code, partition_rpc::CODE_OK, "gc failed: {}", r.message);
}

// ── F099-K per-partition router ───────────────────────────────────────
//
// After F099-K, each partition binds its own TCP listener at
// `base_port + ord`; cross-partition frames sent to the wrong port are
// rejected with `NotFound` ("partition X not served by this P-log
// (owner=Y)"). Tests that touch multiple partitions on one PS (e.g.
// after a split) therefore need to dial the per-partition address from
// `GetRegions.part_addrs`, not the first partition's port.
//
// `PsRouter` hides this routing: given a manager address and (on
// construction) the initial partition's RpcClient, it resolves
// subsequent `part_id`s by querying the manager and caching the
// resulting RpcClients. Existing helpers (`ps_put`, `ps_get`, ...) are
// still available for single-partition tests; split tests should use
// the `psr_*` variants below.

/// Cache of `RpcClient`s keyed by partition id. Resolves unknown
/// partitions by querying the manager for `part_addrs`.
pub struct PsRouter {
    mgr_addr: SocketAddr,
    /// Fallback address used when `part_addrs` does not yet list the
    /// requested partition — typically the owning PS's first partition
    /// port (pre-split it owns everything).
    fallback_addr: SocketAddr,
}

impl PsRouter {
    pub fn new(mgr_addr: SocketAddr, fallback_addr: SocketAddr) -> Self {
        Self { mgr_addr, fallback_addr }
    }

    /// Resolve `part_id` to a fresh RpcClient every call. We do NOT
    /// cache connections because a PS-side task drop (e.g. a partition
    /// being evicted and re-opened) would leave the cached client
    /// pointing at a dead FD. Test-scale cost of a reconnect per call
    /// is negligible.
    pub async fn client_for(&self, part_id: u64) -> Rc<autumn_rpc::client::RpcClient> {
        // Fetch latest part_addrs from manager.
        let mgr = RpcClient::connect(self.mgr_addr).await.expect("connect mgr");
        let regions = get_regions(&mgr).await;
        let mut addr: Option<SocketAddr> = None;
        for (pid, part_addr) in regions.part_addrs {
            if pid == part_id {
                if let Ok(sa) = part_addr.parse::<SocketAddr>() {
                    addr = Some(sa);
                }
                break;
            }
        }
        let target = addr.unwrap_or(self.fallback_addr);
        RpcClient::connect(target).await.expect("connect partition")
    }
}

/// Routed `ps_put` — F099-K aware: dials the partition's own listener.
pub async fn psr_put(
    router: &PsRouter,
    part_id: u64,
    key: &[u8],
    value: &[u8],
    must_sync: bool,
) {
    let c = router.client_for(part_id).await;
    ps_put(&c, part_id, key, value, must_sync).await;
}

/// Routed `ps_get` — F099-K aware.
pub async fn psr_get(router: &PsRouter, part_id: u64, key: &[u8]) -> partition_rpc::GetResp {
    let c = router.client_for(part_id).await;
    ps_get(&c, part_id, key).await
}

/// Routed `ps_flush` — F099-K aware.
pub async fn psr_flush(router: &PsRouter, part_id: u64) {
    let c = router.client_for(part_id).await;
    ps_flush(&c, part_id).await;
}

/// Routed `ps_compact` — F099-K aware.
pub async fn psr_compact(router: &PsRouter, part_id: u64) {
    let c = router.client_for(part_id).await;
    ps_compact(&c, part_id).await;
}

/// Routed `ps_gc` — F099-K aware.
pub async fn psr_gc(router: &PsRouter, part_id: u64) {
    let c = router.client_for(part_id).await;
    ps_gc(&c, part_id).await;
}

// ── Common setup patterns ─────────────────────────────────────────────

/// Standard 2-node infra: manager + 2 extent nodes.
/// Returns (mgr_addr, n1_addr, n2_addr, n1_dir, n2_dir).
pub fn setup_two_node_infra(
    base_port_hint: u16,
) -> (
    SocketAddr,
    SocketAddr,
    SocketAddr,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");

    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), base_port_hint as u64);
    start_extent_node(
        n2_addr,
        n2_dir.path().to_path_buf(),
        base_port_hint as u64 + 1,
    );

    (mgr_addr, n1_addr, n2_addr, n1_dir, n2_dir)
}

/// Register 2 extent nodes with the manager.
pub async fn register_two_nodes(
    mgr: &RpcClient,
    n1_addr: SocketAddr,
    n2_addr: SocketAddr,
    base_id: u16,
) {
    register_node(mgr, &n1_addr.to_string(), &format!("uuid-{}", base_id)).await;
    register_node(
        mgr,
        &n2_addr.to_string(),
        &format!("uuid-{}", base_id + 1),
    )
    .await;
}

/// Full partition setup: manager + 2 extent nodes + 3 streams + partition.
/// Returns (mgr_addr, ps_addr, n1_dir, n2_dir, part_id).
pub async fn setup_full_partition(
    mgr: &RpcClient,
    mgr_addr: SocketAddr,
    part_id: u64,
    ps_id: u64,
) -> SocketAddr {
    let (log, row, meta) = create_three_streams(mgr).await;
    upsert_partition(mgr, part_id, log, row, meta, b"a", b"z").await;
    let ps_addr = pick_addr();
    start_partition_server(ps_id, mgr_addr, ps_addr);
    ps_addr
}

// ── Polling helper ────────────────────────────────────────────────────

/// Poll a condition until it returns true or timeout expires.
/// Returns true if the condition was met, false on timeout.
pub async fn poll_until(
    timeout: Duration,
    interval: Duration,
    mut condition: impl FnMut() -> bool,
) -> bool {
    let start = std::time::Instant::now();
    loop {
        if condition() {
            return true;
        }
        if start.elapsed() >= timeout {
            return false;
        }
        compio::time::sleep(interval).await;
    }
}

/// Async version of poll_until for conditions that need async evaluation.
pub async fn poll_until_async<F, Fut>(timeout: Duration, interval: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    loop {
        if f().await {
            return true;
        }
        if start.elapsed() >= timeout {
            return false;
        }
        compio::time::sleep(interval).await;
    }
}

// ── TableLocations decoder ────────────────────────────────────────────

/// Decode the last TableLocations record from raw metaStream bytes.
pub fn decode_last_table_locations(data: &[u8]) -> TableLocations {
    let mut last: Option<TableLocations> = None;
    let mut buf = data;
    while buf.len() >= 4 {
        let msg_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let total = 4 + msg_len;
        if total > buf.len() {
            break;
        }
        match partition_rpc::rkyv_decode::<TableLocations>(&buf[4..4 + msg_len]) {
            Ok(locs) => {
                last = Some(locs);
                buf = &buf[total..];
            }
            Err(_) => break,
        }
    }
    last.expect("no valid TableLocations record")
}
