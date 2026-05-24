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
                control_address: String::new(),
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
    created
        .stream
        .unwrap_or_else(|| panic!("create_stream code={} msg={}", created.code, created.message))
        .stream_id
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
    let r: UpsertPartitionResp = rkyv_decode(&resp).expect("decode UpsertPartitionResp");
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
) {
    // Bounded retry on transient errors. The first write on a stream
    // triggers tail-init `current_commit`, which under F227 requires ALL
    // replicas to answer `commit_length`; a momentary single-replica
    // non-response (common on a loaded test cluster during a concurrent
    // split/merge storm, or right after PS startup) surfaces as a
    // retryable `Internal` error. The F227 contract is "the caller
    // retries"; the production SDK (maps Internal → retryable ServerError)
    // and the concurrent-writer task in `system_merge` both already do.
    // This helper used to `.expect()` on the first attempt, so a transient
    // miss panicked the seeding loop. Retry mirrors those callers; the
    // success path is unchanged.
    let payload = partition_rpc::rkyv_encode(&partition_rpc::PutReq {
        part_id,
        key: key.to_vec(),
        value: value.to_vec(),
        expires_at: 0,
        region_epoch: 0, // test helper: skip epoch check
    });
    let mut last_err = String::new();
    for _ in 0..30u32 {
        match ps.call(partition_rpc::MSG_PUT, payload.clone()).await {
            Ok(resp) => {
                let _: partition_rpc::PutResp =
                    partition_rpc::rkyv_decode(&resp).expect("decode PutResp");
                return;
            }
            Err(e) => {
                last_err = format!("{e:?}");
                compio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    panic!("put failed after 30 retries: {last_err}");
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
                region_epoch: 0, // test helper: skip epoch check
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
                gc_ratio: None,
                gc_max_size: None,
                gc_stream_debt: None,
                gc_empty_only: false,
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
                gc_ratio: None,
                gc_max_size: None,
                gc_stream_debt: None,
                gc_empty_only: false,
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
                gc_ratio: None,
                gc_max_size: None,
                gc_stream_debt: None,
                gc_empty_only: false,
            }),
        )
        .await
        .expect("gc");
    let r: partition_rpc::MaintenanceResp =
        partition_rpc::rkyv_decode(&resp).expect("decode MaintenanceResp");
    assert_eq!(r.code, partition_rpc::CODE_OK, "gc failed: {}", r.message);
}

/// Delete a key.
pub async fn ps_delete(ps: &RpcClient, part_id: u64, key: &[u8]) -> partition_rpc::DeleteResp {
    let resp = ps
        .call(
            partition_rpc::MSG_DELETE,
            partition_rpc::rkyv_encode(&partition_rpc::DeleteReq {
                part_id,
                key: key.to_vec(),
                region_epoch: 0,
            }),
        )
        .await
        .expect("delete");
    let r: partition_rpc::DeleteResp = partition_rpc::rkyv_decode(&resp).expect("decode DeleteResp");
    assert_eq!(r.code, partition_rpc::CODE_OK, "delete failed: {}", r.message);
    r
}

/// Check if a key exists without fetching the value.
///
/// Returns a synthetic `found = false` response when the PS rejects
/// with "key is out of range" — that error path is the expected
/// behaviour for cross-partition probes (e.g. post-split tests that
/// ask each child whether it holds a key). Other RPC errors still
/// panic via `.expect("head")`.
pub async fn ps_head(ps: &RpcClient, part_id: u64, key: &[u8]) -> partition_rpc::HeadResp {
    match ps
        .call(
            partition_rpc::MSG_HEAD,
            partition_rpc::rkyv_encode(&partition_rpc::HeadReq {
                part_id,
                key: key.to_vec(),
                region_epoch: 0,
            }),
        )
        .await
    {
        Ok(bytes) => partition_rpc::rkyv_decode(&bytes).expect("decode HeadResp"),
        Err(e) if e.to_string().contains("key is out of range") => {
            partition_rpc::HeadResp {
                code: partition_rpc::CODE_OK,
                message: String::new(),
                found: false,
                value_length: 0,
            }
        }
        Err(e) => panic!("head: {e}"),
    }
}

/// Range scan with prefix, start key, and limit.
pub async fn ps_range(
    ps: &RpcClient,
    part_id: u64,
    prefix: &[u8],
    start: &[u8],
    limit: u32,
) -> partition_rpc::RangeResp {
    let resp = ps
        .call(
            partition_rpc::MSG_RANGE,
            partition_rpc::rkyv_encode(&partition_rpc::RangeReq {
                part_id,
                prefix: prefix.to_vec(),
                start: start.to_vec(),
                limit,
                region_epoch: 0,
            }),
        )
        .await
        .expect("range");
    let r: partition_rpc::RangeResp = partition_rpc::rkyv_decode(&resp).expect("decode RangeResp");
    assert_eq!(r.code, partition_rpc::CODE_OK, "range failed: {}", r.message);
    r
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
        // Retry with region re-resolution: a partition can be mid-reopen
        // (split / merge widen its rg → region_sync drops + reopens it on a
        // fresh per-partition listener), during which its addr is briefly
        // unpublished or its listener not yet bound. The production SDK
        // handles this by refreshing regions + reconnecting
        // (`call_ps_for_part`, MAX_PS_REFRESHES); this test router mirrors
        // that instead of panicking on the first transient miss.
        let mut last_err = String::new();
        for _ in 0..50 {
            let mgr = match RpcClient::connect(self.mgr_addr).await {
                Ok(m) => m,
                Err(e) => {
                    last_err = format!("connect mgr: {e}");
                    compio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
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
            match addr {
                Some(target) => match RpcClient::connect(target).await {
                    Ok(c) => return c,
                    Err(e) => last_err = format!("connect part {part_id} @ {target}: {e}"),
                },
                None => last_err = format!("part {part_id} not in regions yet"),
            }
            compio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("connect partition {part_id} after retries: {last_err}");
    }
}

/// Routed `ps_put` — F099-K aware: dials the partition's own listener.
pub async fn psr_put(
    router: &PsRouter,
    part_id: u64,
    key: &[u8],
    value: &[u8],
) {
    let c = router.client_for(part_id).await;
    ps_put(&c, part_id, key, value).await;
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

/// Routed `ps_delete` — F099-K aware.
pub async fn psr_delete(router: &PsRouter, part_id: u64, key: &[u8]) -> partition_rpc::DeleteResp {
    let c = router.client_for(part_id).await;
    ps_delete(&c, part_id, key).await
}

/// Routed `ps_head` — F099-K aware.
pub async fn psr_head(router: &PsRouter, part_id: u64, key: &[u8]) -> partition_rpc::HeadResp {
    let c = router.client_for(part_id).await;
    ps_head(&c, part_id, key).await
}

/// Routed `ps_range` — F099-K aware.
pub async fn psr_range(
    router: &PsRouter,
    part_id: u64,
    prefix: &[u8],
    start: &[u8],
    limit: u32,
) -> partition_rpc::RangeResp {
    let c = router.client_for(part_id).await;
    ps_range(&c, part_id, prefix, start, limit).await
}

// ── Bulk test utilities ──────────────────────────────────────────────

/// Write `count` keys as `{prefix}-{i:03}` with values `val-{prefix}-{i:03}`.
pub async fn write_sequential_keys(
    ps: &RpcClient,
    part_id: u64,
    prefix: &str,
    count: u32,
) -> Vec<String> {
    let mut keys = Vec::with_capacity(count as usize);
    for i in 0..count {
        let key = format!("{prefix}-{i:03}");
        let val = format!("val-{prefix}-{i:03}");
        ps_put(ps, part_id, key.as_bytes(), val.as_bytes()).await;
        keys.push(key);
    }
    keys
}

/// Verify all keys exist with their deterministic values via `ps_get`.
pub async fn verify_sequential_keys(
    ps: &RpcClient,
    part_id: u64,
    keys: &[String],
) -> usize {
    for key in keys {
        let expected_val = format!("val-{key}");
        let resp = ps_get(ps, part_id, key.as_bytes()).await;
        assert_eq!(
            resp.value,
            expected_val.as_bytes(),
            "key {key} has wrong value"
        );
    }
    keys.len()
}

/// Verify all keys via routed `psr_get` (for post-split partitions).
pub async fn verify_sequential_keys_routed(
    router: &PsRouter,
    part_id: u64,
    keys: &[String],
) -> usize {
    for key in keys {
        let expected_val = format!("val-{key}");
        let resp = psr_get(router, part_id, key.as_bytes()).await;
        assert_eq!(
            resp.value,
            expected_val.as_bytes(),
            "key {key} has wrong value (routed)"
        );
    }
    keys.len()
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

// ── Embedded etcd ─────────────────────────────────────────────────────
//
// Spawns the real `etcd` binary in single-member dev mode against a
// tempdir. Replaces the pre-this Go helper (`tests/support/embedded_etcd/main.go`)
// which required a Go toolchain to compile `go.etcd.io/etcd/server/v3/embed`
// at test time. The binary approach has no build-time dependency and
// uses whatever etcd version is installed on the system (verified
// against 3.5.x).
//
// All etcd-dependent tests (`f149_leader_fence`, `f209_apply_done_atomicity`,
// `system_manager_failover`, `etcd_stream_integration`) call
// `start_etcd()` and drop the returned guard at test exit.

/// RAII guard that kills the spawned etcd child + cleans up its data
/// dir on drop. Tests bind this to an `_etcd_guard` local with a
/// leading underscore to signal "kept alive for the test body, not
/// referenced directly."
pub struct EtcdGuard {
    child: Option<std::process::Child>,
    _data_dir: tempfile::TempDir,
}

impl Drop for EtcdGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Block until the etcd endpoint responds to a `get` call or the
/// timeout elapses. Panics on timeout so tests fail loudly when etcd
/// isn't reachable (e.g. binary missing, port collision, peer URL
/// already in use).
pub async fn wait_for_etcd(endpoint: &str, timeout: Duration) {
    let start = std::time::Instant::now();
    loop {
        if let Ok(c) = autumn_etcd::EtcdClient::connect(endpoint).await {
            if c.get("health-check").await.is_ok() {
                return;
            }
        }
        assert!(
            start.elapsed() < timeout,
            "etcd did not become ready within {timeout:?}"
        );
        compio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Spawn a single-member etcd instance for tests. Picks two free ports
/// (client + peer) on loopback, writes data to a tempdir, and returns
/// `(guard, "http://127.0.0.1:<client_port>")`.
///
/// Looks for `etcd` on `$PATH`; override with `AUTUMN_TEST_ETCD_BIN` if
/// the binary lives somewhere unusual (CI image, dev box, etc.).
pub async fn start_etcd() -> (EtcdGuard, String) {
    let etcd_bin = std::env::var("AUTUMN_TEST_ETCD_BIN").unwrap_or_else(|_| "etcd".to_string());
    let client_addr = pick_addr();
    let peer_addr = pick_addr();
    let client_url = format!("http://{}", client_addr);
    let peer_url = format!("http://{}", peer_addr);

    let data_dir = tempfile::tempdir().expect("etcd tempdir");
    let data_path = data_dir.path().join("etcd-data");

    let mut cmd = std::process::Command::new(&etcd_bin);
    cmd.arg("--name").arg("n1")
        .arg("--data-dir").arg(&data_path)
        .arg("--listen-client-urls").arg(&client_url)
        .arg("--advertise-client-urls").arg(&client_url)
        .arg("--listen-peer-urls").arg(&peer_url)
        .arg("--initial-advertise-peer-urls").arg(&peer_url)
        .arg("--initial-cluster").arg(format!("n1={peer_url}"))
        .arg("--initial-cluster-state").arg("new")
        .arg("--log-level").arg("error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());

    let child = cmd.spawn().unwrap_or_else(|e| {
        panic!(
            "spawn etcd binary `{etcd_bin}`: {e} — install etcd or set AUTUMN_TEST_ETCD_BIN"
        )
    });
    wait_for_etcd(&client_url, Duration::from_secs(30)).await;

    (
        EtcdGuard {
            child: Some(child),
            _data_dir: data_dir,
        },
        client_url,
    )
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
