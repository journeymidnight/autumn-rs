use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc::{self, *};
use bytes::Bytes;

// ── Re-exports for SDK consumers ────────────────────────────────────────────

pub use autumn_rpc::partition_rpc::RangeEntry;

// ── Public helpers ──────────────────────────────────────────────────────────

pub fn parse_addr(addr: &str) -> Result<SocketAddr> {
    addr.parse()
        .with_context(|| format!("invalid address: {addr}"))
}

pub fn decode_err(e: String) -> anyhow::Error {
    anyhow!("rkyv decode: {e}")
}

// ── Error type ──────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum AutumnError {
    NotFound,
    InvalidArgument(String),
    PreconditionFailed(String),
    ServerError(String),
    RoutingError(String),
    ConnectionError(String),
}

impl std::fmt::Display for AutumnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AutumnError::NotFound => write!(f, "key not found"),
            AutumnError::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
            AutumnError::PreconditionFailed(msg) => write!(f, "precondition failed: {msg}"),
            AutumnError::ServerError(msg) => write!(f, "server error: {msg}"),
            AutumnError::RoutingError(msg) => write!(f, "routing error: {msg}"),
            AutumnError::ConnectionError(msg) => write!(f, "connection error: {msg}"),
        }
    }
}

impl std::error::Error for AutumnError {}

fn code_to_error(code: u8, message: String) -> AutumnError {
    match code {
        partition_rpc::CODE_NOT_FOUND => AutumnError::NotFound,
        partition_rpc::CODE_INVALID_ARGUMENT => AutumnError::InvalidArgument(message),
        partition_rpc::CODE_PRECONDITION => AutumnError::PreconditionFailed(message),
        _ => AutumnError::ServerError(message),
    }
}

// ── Range scan result ───────────────────────────────────────────────────────

pub struct RangeResult {
    pub entries: Vec<RangeEntry>,
    pub has_more: bool,
}

// ── Key metadata ────────────────────────────────────────────────────────────

pub struct KeyMeta {
    pub found: bool,
    pub value_length: u64,
}

// ── ClusterClient ───────────────────────────────────────────────────────────

/// Client for interacting with an autumn-rs cluster.
///
/// Supports multiple manager addresses with round-robin failover on
/// NotLeader or connection errors. PS connections auto-reconnect on failure.
///
/// **All hot-path methods take `&self`** so an `Rc<ClusterClient>` can be
/// shared across concurrent compio tasks (e.g., FUSE dispatcher spawning
/// per-request tasks). Internal mutability is provided via `RefCell` for
/// the routing caches and `Cell` for the manager round-robin index.
/// Borrows are deliberately scoped: every `borrow()` / `borrow_mut()` is
/// released before any `.await`.
pub struct ClusterClient {
    /// Manager addresses (comma-separated on construction).
    manager_addrs: Vec<String>,
    /// Current manager index (round-robin).
    current_mgr: Cell<usize>,
    /// Cached manager RPC connection. Recreated on error.
    mgr_conn: Rc<RefCell<Option<Rc<RpcClient>>>>,
    /// Cached PS RPC connections. Dropped on error, recreated on next use.
    ps_conns: RefCell<HashMap<String, Rc<RpcClient>>>,
    /// Routing cache. Populated on `connect`, refreshed on `refresh_regions`.
    /// `RefCell` so concurrent tasks holding `Rc<ClusterClient>` can do
    /// brief lookup-and-clone borrows without blocking each other.
    regions: RefCell<Vec<(u64, MgrRegionInfo)>>,
    ps_details: RefCell<HashMap<u64, MgrPsDetail>>,
    /// F099-K — per-partition listener addresses, indexed by `part_id`.
    /// When an entry is present, it supersedes `ps_details[ps_id].address`
    /// for routing decisions (thread-per-partition shard target).
    part_addrs: RefCell<HashMap<u64, String>>,
}

impl ClusterClient {
    /// Current manager address.
    fn manager_addr(&self) -> &str {
        &self.manager_addrs[self.current_mgr.get() % self.manager_addrs.len()]
    }

    /// Rotate to next manager.
    fn rotate_manager(&self) {
        if self.manager_addrs.len() > 1 {
            let next = (self.current_mgr.get() + 1) % self.manager_addrs.len();
            self.current_mgr.set(next);
            // Drop cached connection so next call reconnects to new manager
            *self.mgr_conn.borrow_mut() = None;
        }
    }

    /// Get or create a manager RPC connection. Auto-reconnects on failure.
    async fn mgr_client(&self) -> Result<Rc<RpcClient>> {
        {
            let guard = self.mgr_conn.borrow();
            if let Some(c) = guard.as_ref() {
                return Ok(c.clone());
            }
        }
        let addr = parse_addr(self.manager_addr())?;
        let client = RpcClient::connect(addr)
            .await
            .with_context(|| format!("connect manager {}", self.manager_addr()))?;
        *self.mgr_conn.borrow_mut() = Some(client.clone());
        Ok(client)
    }

    /// Call the current manager. On error, drop connection (auto-reconnect next time).
    pub async fn mgr_call(&self, msg_type: u8, payload: Bytes) -> Result<Bytes> {
        let client = self.mgr_client().await?;
        match client.call(msg_type, payload).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                // Drop connection so next call reconnects
                *self.mgr_conn.borrow_mut() = None;
                Err(anyhow!("{e}"))
            }
        }
    }

    /// Call manager with retry and round-robin on NotLeader/connection error.
    pub async fn mgr_call_retry(&self, msg_type: u8, payload: Bytes, max_retries: u32) -> Result<Bytes> {
        let mut attempt = 0u32;
        loop {
            match self.mgr_call(msg_type, payload.clone()).await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    attempt += 1;
                    if attempt > max_retries {
                        return Err(e.context(format!("failed after {max_retries} retries")));
                    }
                    self.rotate_manager();
                    compio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    pub fn mgr(&self) -> Result<Rc<RpcClient>> {
        self.mgr_conn
            .borrow()
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("manager not connected"))
    }

    /// Connect to the cluster. Accepts comma-separated manager addresses.
    pub async fn connect(manager: &str) -> Result<Self> {
        let manager_addrs: Vec<String> = manager
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let client = Self {
            manager_addrs,
            current_mgr: Cell::new(0),
            mgr_conn: Rc::new(RefCell::new(None)),
            ps_conns: RefCell::new(HashMap::new()),
            regions: RefCell::new(Vec::new()),
            ps_details: RefCell::new(HashMap::new()),
            part_addrs: RefCell::new(HashMap::new()),
        };

        // Try connecting to each manager until one responds
        let mut connected = false;
        for idx in 0..client.manager_addrs.len() {
            client.current_mgr.set(idx);
            match client.mgr_client().await {
                Ok(_) => {
                    connected = true;
                    break;
                }
                Err(_) => continue,
            }
        }
        if !connected {
            return Err(anyhow!("cannot connect to any manager: {}", manager));
        }

        client.refresh_regions().await?;
        Ok(client)
    }

    pub async fn refresh_regions(&self) -> Result<()> {
        let resp_bytes = self
            .mgr_call_retry(MSG_GET_REGIONS, Bytes::new(), 3)
            .await
            .context("get regions")?;
        let resp: GetRegionsResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
        let mut sorted: Vec<(u64, MgrRegionInfo)> = resp.regions.into_iter().collect();
        sorted.sort_by(|a, b| {
            a.1.rg
                .as_ref()
                .map(|r| r.start_key.as_slice())
                .unwrap_or(&[])
                .cmp(
                    b.1.rg
                        .as_ref()
                        .map(|r| r.start_key.as_slice())
                        .unwrap_or(&[]),
                )
        });
        for i in 0..sorted.len().saturating_sub(1) {
            let end_key = sorted[i]
                .1
                .rg
                .as_ref()
                .map(|r| r.end_key.as_slice())
                .unwrap_or(&[]);
            let next_start = sorted[i + 1]
                .1
                .rg
                .as_ref()
                .map(|r| r.start_key.as_slice())
                .unwrap_or(&[]);
            if end_key != next_start {
                eprintln!(
                    "WARNING: region gap: partition {} end_key != partition {} start_key",
                    sorted[i].1.part_id,
                    sorted[i + 1].1.part_id
                );
            }
        }
        // Brief swap — no .await held under any borrow.
        *self.regions.borrow_mut() = sorted;
        *self.ps_details.borrow_mut() = resp.ps_details.into_iter().collect();
        *self.part_addrs.borrow_mut() = resp.part_addrs.into_iter().collect();
        Ok(())
    }

    /// Get or create a PS RPC connection. Auto-reconnects on failure.
    pub async fn get_ps_client(&self, ps_addr: &str) -> Result<Rc<RpcClient>> {
        {
            let conns = self.ps_conns.borrow();
            if let Some(c) = conns.get(ps_addr) {
                return Ok(c.clone());
            }
        }
        let addr = parse_addr(ps_addr)?;
        let client = RpcClient::connect(addr)
            .await
            .with_context(|| format!("connect PS {ps_addr}"))?;
        self.ps_conns
            .borrow_mut()
            .insert(ps_addr.to_string(), client.clone());
        Ok(client)
    }

    /// Call a PS. On error, drop connection (auto-reconnect next time).
    pub async fn ps_call(
        &self,
        ps_addr: &str,
        msg_type: u8,
        payload: Bytes,
    ) -> Result<Bytes> {
        let client = self.get_ps_client(ps_addr).await?;
        match client.call(msg_type, payload).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                // Drop connection so next call reconnects
                self.ps_conns.borrow_mut().remove(ps_addr);
                Err(anyhow!("{e}"))
            }
        }
    }

    pub fn lookup_key(&self, key: &[u8]) -> Option<(u64, String)> {
        let regions = self.regions.borrow();
        if regions.is_empty() {
            return None;
        }
        let idx = regions.partition_point(|(_, region)| match region.rg.as_ref() {
            Some(rg) if !rg.end_key.is_empty() => rg.end_key.as_slice() <= key,
            _ => false,
        });
        if idx >= regions.len() {
            return None;
        }
        let (_, region) = &regions[idx];
        // F099-K: prefer per-partition listener if registered.
        let part_addrs = self.part_addrs.borrow();
        let ps_details = self.ps_details.borrow();
        let addr = match part_addrs.get(&region.part_id) {
            Some(a) => a.clone(),
            None => ps_details.get(&region.ps_id)?.address.clone(),
        };
        Some((region.part_id, addr))
    }

    pub async fn resolve_key(&self, key: &[u8]) -> Result<(u64, String)> {
        if let Some(result) = self.lookup_key(key) {
            return Ok(result);
        }
        self.refresh_regions().await?;
        self.lookup_key(key)
            .ok_or_else(|| anyhow!("key is out of range"))
    }

    pub async fn resolve_part_id(&self, part_id: u64) -> Result<String> {
        // F099-K: prefer the per-partition listener address when
        // registered; fall back to the PS-level base address otherwise.
        // Borrows are scoped — released before any `.await`.
        let lookup = |this: &Self| -> Option<String> {
            let regions = this.regions.borrow();
            let region = regions.iter().find(|(_, r)| r.part_id == part_id)?;
            let part_addrs = this.part_addrs.borrow();
            if let Some(a) = part_addrs.get(&region.1.part_id) {
                return Some(a.clone());
            }
            let ps_details = this.ps_details.borrow();
            ps_details.get(&region.1.ps_id).map(|d| d.address.clone())
        };
        if let Some(addr) = lookup(self) {
            return Ok(addr);
        }
        self.refresh_regions().await?;
        lookup(self).ok_or_else(|| anyhow!("partition {} not found", part_id))
    }

    pub async fn all_partitions(&self) -> Result<Vec<(u64, String)>> {
        if self.regions.borrow().is_empty() {
            self.refresh_regions().await?;
        }
        let regions = self.regions.borrow();
        let part_addrs = self.part_addrs.borrow();
        let ps_details = self.ps_details.borrow();
        let mut result: Vec<(u64, String)> = regions
            .iter()
            .map(|(_, region)| {
                // F099-K: prefer per-partition listener address when present.
                let addr = part_addrs
                    .get(&region.part_id)
                    .cloned()
                    .or_else(|| ps_details.get(&region.ps_id).map(|d| d.address.clone()))
                    .unwrap_or_default();
                (region.part_id, addr)
            })
            .collect();
        result.sort_by_key(|(pid, _)| *pid);
        Ok(result)
    }

    /// F099-N-c — like `all_partitions`, but also returns each partition's
    /// `(start_key, end_key)` range so bench tools can generate keys that
    /// actually land in each partition. Prior bench tools used a constant
    /// prefix like "pc_{tid}_{seq}" / "bench_{tid}_{seq}" which lexically
    /// always fell in ONE partition, making N>1 perf tests measure a single
    /// partition with (N-1) rejecting load.
    pub async fn all_partitions_with_range(
        &self,
    ) -> Result<Vec<(u64, String, Vec<u8>, Vec<u8>)>> {
        if self.regions.borrow().is_empty() {
            self.refresh_regions().await?;
        }
        let regions = self.regions.borrow();
        let part_addrs = self.part_addrs.borrow();
        let ps_details = self.ps_details.borrow();
        let mut result: Vec<(u64, String, Vec<u8>, Vec<u8>)> = regions
            .iter()
            .map(|(_, region)| {
                let addr = part_addrs
                    .get(&region.part_id)
                    .cloned()
                    .or_else(|| ps_details.get(&region.ps_id).map(|d| d.address.clone()))
                    .unwrap_or_default();
                let (start_key, end_key) = region
                    .rg
                    .as_ref()
                    .map(|r| (r.start_key.clone(), r.end_key.clone()))
                    .unwrap_or_default();
                (region.part_id, addr, start_key, end_key)
            })
            .collect();
        result.sort_by_key(|(pid, _, _, _)| *pid);
        Ok(result)
    }

    // ── Internal: PS call with routing retry ─────────────────────────────────

    /// Resolve key to (part_id, ps_addr), call PS, retry once on failure with refresh.
    async fn call_ps_for_key(
        &self,
        key: &[u8],
        msg_type: u8,
        build_payload: impl Fn(u64) -> Bytes,
    ) -> std::result::Result<Bytes, AutumnError> {
        for attempt in 0..2 {
            let (part_id, ps_addr) = self.resolve_key(key).await
                .map_err(|e| AutumnError::RoutingError(e.to_string()))?;
            match self.ps_call(&ps_addr, msg_type, build_payload(part_id)).await {
                Ok(b) => return Ok(b),
                Err(_) if attempt == 0 => { let _ = self.refresh_regions().await; }
                Err(e) => return Err(AutumnError::ConnectionError(e.to_string())),
            }
        }
        unreachable!()
    }

    /// Resolve part_id to ps_addr, call PS, retry once on failure with refresh.
    async fn call_ps_for_part(
        &self,
        part_id: u64,
        msg_type: u8,
        payload: Bytes,
    ) -> std::result::Result<Bytes, AutumnError> {
        for attempt in 0..2 {
            let ps_addr = self.resolve_part_id(part_id).await
                .map_err(|e| AutumnError::RoutingError(e.to_string()))?;
            match self.ps_call(&ps_addr, msg_type, payload.clone()).await {
                Ok(b) => return Ok(b),
                Err(_) if attempt == 0 => { let _ = self.refresh_regions().await; }
                Err(e) => return Err(AutumnError::ConnectionError(e.to_string())),
            }
        }
        unreachable!()
    }

    // ── High-level SDK API ──────────────────────────────────────────────────

    /// Put a key-value pair. Retries once on routing miss.
    ///
    /// F178: every Put is durable. Pre-F178 the API took a `must_sync: bool`
    /// flag; after F178 the field was removed from the wire and every
    /// append goes through the extent-node fsync coalescer (RocksDB-style
    /// group commit). Callers no longer have a "fast but unsafe" mode.
    pub async fn put(&self, key: &[u8], value: &[u8]) -> std::result::Result<(), AutumnError> {
        self.put_opts(key, value, 0).await
    }

    /// Put a key-value pair with TTL (seconds from now). 0 = no expiry.
    pub async fn put_with_ttl(
        &self,
        key: &[u8],
        value: &[u8],
        ttl_secs: u64,
    ) -> std::result::Result<(), AutumnError> {
        let expires_at = if ttl_secs > 0 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + ttl_secs
        } else {
            0
        };
        self.put_opts(key, value, expires_at).await
    }

    async fn put_opts(
        &self,
        key: &[u8],
        value: &[u8],
        expires_at: u64,
    ) -> std::result::Result<(), AutumnError> {
        let key = key.to_vec();
        let value = value.to_vec();
        let resp_bytes = self.call_ps_for_key(&key, MSG_PUT, |part_id| {
            rkyv_encode(&PutReq { part_id, key: key.clone(), value: value.clone(), expires_at })
        }).await?;
        let resp: PutResp = rkyv_decode(&resp_bytes).map_err(|e| AutumnError::ServerError(e))?;
        if resp.code != partition_rpc::CODE_OK {
            return Err(code_to_error(resp.code, resp.message));
        }
        Ok(())
    }

    /// Get a value by key. Returns None if not found.
    pub async fn get(&self, key: &[u8]) -> std::result::Result<Option<Vec<u8>>, AutumnError> {
        self.get_range(key, 0, 0).await
    }

    /// Get a sub-range of a value: bytes `[offset, offset+length)`.
    /// `length == 0` means "from offset to the end of the value" (matches the
    /// underlying `GetReq` semantics). Returns None if the key is not found.
    ///
    /// Routes through `call_ps_for_key` so the cached PS connection is dropped
    /// on RPC error and routing is refreshed on the second attempt — same
    /// resilience as `get`/`put`/`head` after a cluster restart.
    pub async fn get_range(
        &self,
        key: &[u8],
        offset: u32,
        length: u32,
    ) -> std::result::Result<Option<Vec<u8>>, AutumnError> {
        let key = key.to_vec();
        let resp_bytes = self.call_ps_for_key(&key, MSG_GET, |part_id| {
            rkyv_encode(&GetReq { part_id, key: key.clone(), offset, length })
        }).await?;
        let resp: GetResp = rkyv_decode(&resp_bytes).map_err(|e| AutumnError::ServerError(e))?;
        if resp.code == partition_rpc::CODE_NOT_FOUND {
            return Ok(None);
        }
        if resp.code != partition_rpc::CODE_OK {
            return Err(code_to_error(resp.code, resp.message));
        }
        Ok(Some(resp.value))
    }

    /// Delete a key. Returns Ok(()) even if key didn't exist.
    pub async fn delete(&self, key: &[u8]) -> std::result::Result<(), AutumnError> {
        let key = key.to_vec();
        let resp_bytes = self.call_ps_for_key(&key, MSG_DELETE, |part_id| {
            rkyv_encode(&DeleteReq { part_id, key: key.clone() })
        }).await?;
        let resp: DeleteResp = rkyv_decode(&resp_bytes).map_err(|e| AutumnError::ServerError(e))?;
        if resp.code != partition_rpc::CODE_OK && resp.code != partition_rpc::CODE_NOT_FOUND {
            return Err(code_to_error(resp.code, resp.message));
        }
        Ok(())
    }

    /// Get key metadata (existence and value length).
    pub async fn head(&self, key: &[u8]) -> std::result::Result<KeyMeta, AutumnError> {
        let key = key.to_vec();
        let resp_bytes = self.call_ps_for_key(&key, MSG_HEAD, |part_id| {
            rkyv_encode(&HeadReq { part_id, key: key.clone() })
        }).await?;
        let resp: HeadResp = rkyv_decode(&resp_bytes).map_err(|e| AutumnError::ServerError(e))?;
        Ok(KeyMeta { found: resp.found, value_length: resp.value_length })
    }

    /// Range scan with prefix filter. Scans across partitions like Go's Range().
    pub async fn range(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: u32,
    ) -> std::result::Result<RangeResult, AutumnError> {
        // Ensure regions are loaded
        if self.regions.borrow().is_empty() {
            self.refresh_regions().await
                .map_err(|e| AutumnError::RoutingError(e.to_string()))?;
        }

        let search_key = if start.is_empty() { prefix } else { start };

        // Snapshot the routing info into Vec<(part_id, ps_addr, start_key)>
        // upfront so we can drop the borrow before any await. F112: prefer
        // per-partition listener (F099-K) — same lookup pattern as
        // `lookup_key` / `resolve_part_id` / `all_partitions`.
        let snapshot: Vec<(u64, String, Vec<u8>)> = {
            let regions = self.regions.borrow();
            let part_addrs = self.part_addrs.borrow();
            let ps_details = self.ps_details.borrow();
            let start_idx = regions.partition_point(|(_, region)| match region.rg.as_ref() {
                Some(rg) if !rg.end_key.is_empty() => rg.end_key.as_slice() <= search_key,
                _ => false,
            });
            let mut out = Vec::new();
            for (_, region) in regions.iter().skip(start_idx) {
                let ps_addr = match part_addrs.get(&region.part_id) {
                    Some(a) => a.clone(),
                    None => match ps_details.get(&region.ps_id) {
                        Some(d) => d.address.clone(),
                        None => {
                            return Err(AutumnError::RoutingError(format!(
                                "no address for partition {} (ps_id {})",
                                region.part_id, region.ps_id
                            )));
                        }
                    },
                };
                let region_start_key = region
                    .rg
                    .as_ref()
                    .map(|r| r.start_key.clone())
                    .unwrap_or_default();
                out.push((region.part_id, ps_addr, region_start_key));
            }
            out
        };

        let mut remaining = limit;
        let mut all_entries = Vec::new();
        let mut has_more = false;

        for (idx, (part_id, ps_addr, region_start_key)) in snapshot.iter().enumerate() {
            if remaining == 0 {
                has_more = true;
                break;
            }
            // For partitions after the first, check if start_key still has the prefix
            if idx != 0 && !prefix.is_empty() {
                if !region_start_key.is_empty() && !region_start_key.starts_with(prefix) {
                    break;
                }
            }
            let part_id = *part_id;

            let resp_bytes = match self
                .ps_call(
                    &ps_addr,
                    MSG_RANGE,
                    rkyv_encode(&RangeReq {
                        part_id,
                        prefix: prefix.to_vec(),
                        start: start.to_vec(),
                        limit: remaining,
                    }),
                )
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    // F112: refresh regions then surface — silently
                    // skipping a partition truncates the result without
                    // the caller knowing.
                    let _ = self.refresh_regions().await;
                    return Err(AutumnError::ConnectionError(format!(
                        "range on partition {part_id}: {e}"
                    )));
                }
            };
            let resp: RangeResp = rkyv_decode(&resp_bytes)
                .map_err(|e| AutumnError::ServerError(e))?;
            if resp.code != partition_rpc::CODE_OK {
                return Err(code_to_error(resp.code, resp.message));
            }

            let count = resp.entries.len() as u32;
            all_entries.extend(resp.entries);
            remaining = remaining.saturating_sub(count);
            if resp.has_more {
                has_more = true;
            }
        }

        // Dedup by key — after split, overlapping SSTables may return
        // the same key from multiple partitions before compaction cleans up.
        // Keep the first occurrence (from the authoritative partition).
        {
            let mut seen = std::collections::HashSet::new();
            all_entries.retain(|e| seen.insert(e.key.clone()));
        }

        Ok(RangeResult {
            entries: all_entries,
            has_more,
        })
    }

    /// Stream put (for large values, single RPC).
    ///
    /// F178: see `put` doc — every write is durable, no `must_sync` flag.
    pub async fn stream_put(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> std::result::Result<(), AutumnError> {
        let key = key.to_vec();
        let value = value.to_vec();
        let resp_bytes = self.call_ps_for_key(&key, MSG_STREAM_PUT, |part_id| {
            rkyv_encode(&StreamPutReq { part_id, key: key.clone(), value: value.clone(), expires_at: 0 })
        }).await?;
        let resp: PutResp = rkyv_decode(&resp_bytes).map_err(|e| AutumnError::ServerError(e))?;
        if resp.code != partition_rpc::CODE_OK {
            return Err(code_to_error(resp.code, resp.message));
        }
        Ok(())
    }

    /// Trigger partition split.
    pub async fn split(&self, part_id: u64) -> std::result::Result<(), AutumnError> {
        self.call_ps_for_part(part_id, MSG_SPLIT_PART, rkyv_encode(&SplitPartReq { part_id })).await?;
        Ok(())
    }

    /// Trigger compaction on a partition.
    pub async fn compact(&self, part_id: u64) -> std::result::Result<(), AutumnError> {
        self.maintenance(part_id, MAINTENANCE_COMPACT, vec![]).await
    }

    /// Trigger automatic GC on a partition.
    pub async fn gc(&self, part_id: u64) -> std::result::Result<(), AutumnError> {
        self.maintenance(part_id, MAINTENANCE_AUTO_GC, vec![]).await
    }

    /// Force GC of specific extents on a partition.
    pub async fn force_gc(&self, part_id: u64, extent_ids: Vec<u64>) -> std::result::Result<(), AutumnError> {
        self.maintenance(part_id, MAINTENANCE_FORCE_GC, extent_ids).await
    }

    /// Trigger flush on a partition.
    pub async fn flush(&self, part_id: u64) -> std::result::Result<(), AutumnError> {
        self.maintenance(part_id, MAINTENANCE_FLUSH, vec![]).await
    }

    async fn maintenance(&self, part_id: u64, op: u8, extent_ids: Vec<u64>) -> std::result::Result<(), AutumnError> {
        let resp_bytes = self.call_ps_for_part(
            part_id, MSG_MAINTENANCE,
            rkyv_encode(&MaintenanceReq { part_id, op, extent_ids }),
        ).await?;
        let resp: MaintenanceResp = rkyv_decode(&resp_bytes).map_err(|e| AutumnError::ServerError(e))?;
        if resp.code != partition_rpc::CODE_OK {
            return Err(code_to_error(resp.code, resp.message));
        }
        Ok(())
    }
}
