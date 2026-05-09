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
    /// F129: value exceeds the inline `Put` cap. Caller should retry
    /// via `put_stream_begin` / `PutStreamHandle::send` / `commit`.
    ValueTooLarge { size: u64, cap: u64 },
    /// F129: the upload_id is unknown to the PS — TTL-evicted, never
    /// opened, already committed/aborted, or the PS restarted (resume
    /// across restart is F132, not yet implemented).
    UploadNotFound,
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
            AutumnError::ValueTooLarge { size, cap } => {
                if *cap > 0 {
                    write!(f, "value {size} bytes exceeds inline cap {cap} — use put_stream")
                } else {
                    write!(f, "value {size} bytes exceeds the partition server's inline cap — use put_stream")
                }
            }
            AutumnError::UploadNotFound => write!(f, "upload_id not found on partition server"),
        }
    }
}

impl std::error::Error for AutumnError {}

fn code_to_error(code: u8, message: String) -> AutumnError {
    match code {
        partition_rpc::CODE_NOT_FOUND => AutumnError::NotFound,
        partition_rpc::CODE_INVALID_ARGUMENT => AutumnError::InvalidArgument(message),
        partition_rpc::CODE_PRECONDITION => AutumnError::PreconditionFailed(message),
        partition_rpc::CODE_VALUE_TOO_LARGE => {
            // The cap is in the message; we don't try to parse it. Surface the
            // raw size if the caller doesn't already know.
            AutumnError::ValueTooLarge { size: 0, cap: 0 }
        }
        partition_rpc::CODE_UPLOAD_NOT_FOUND => AutumnError::UploadNotFound,
        _ => AutumnError::ServerError(message),
    }
}

/// F129 client-side hard cap. Matches the PS's `AUTUMN_PS_MAX_INLINE_BYTES_HARD`
/// (256 MiB). Pre-checked in `put_opts` to avoid sending a 256 MB+ request
/// over the wire only to get rejected. The PS may be configured with a
/// stricter (lower) cap; in that case the server still rejects, mapped to
/// `AutumnError::ValueTooLarge` with `cap = 0` (the size + cap parsed
/// from the message would require parsing, which we skip).
pub const CLIENT_PUT_HARD_CAP: u64 = 256 * 1024 * 1024;

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
        // F129: client-side pre-check against the hard cap. Avoids sending
        // a 256 MB+ payload over the wire only to be rejected post-decode.
        // The PS still authoritatively rejects > its configured (default
        // 64 MiB) cap; for anything in (64 MiB, 256 MiB] we let the server
        // decide (its default rejects, but it could be raised via env).
        if value.len() as u64 > CLIENT_PUT_HARD_CAP {
            return Err(AutumnError::ValueTooLarge {
                size: value.len() as u64,
                cap: CLIENT_PUT_HARD_CAP,
            });
        }
        let key = key.to_vec();
        let value = value.to_vec();
        let resp_bytes = self.call_ps_for_key(&key, MSG_PUT, |part_id| {
            rkyv_encode(&PutReq { part_id, key: key.clone(), value: value.clone(), expires_at })
        }).await?;
        let resp: PutResp = rkyv_decode(&resp_bytes).map_err(|e| AutumnError::ServerError(e))?;
        if resp.code == partition_rpc::CODE_VALUE_TOO_LARGE {
            // PS-reported cap (lower than CLIENT_PUT_HARD_CAP). Surface the
            // value's size so the caller can size the next put_stream
            // chunks appropriately.
            return Err(AutumnError::ValueTooLarge {
                size: value.len() as u64,
                cap: 0, // unknown server-side cap (would need to parse `resp.message`)
            });
        }
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

    // ── F129 multipart upload ──────────────────────────────────────────────

    /// Open a multipart upload session for `key`. Returns a handle that
    /// holds the cached PS connection and the running fragment list;
    /// repeated calls to `send` push chunks to log_stream, `commit`
    /// finalises the value, `abort` discards. Drop without commit/abort
    /// is logged at WARN — the PS-side TTL (default 30 min) will reclaim.
    pub async fn put_stream_begin(
        &self,
        key: &[u8],
        expires_at: u64,
    ) -> std::result::Result<PutStreamHandle, AutumnError> {
        let key_vec = key.to_vec();
        let (part_id, ps_addr) = self
            .resolve_key(&key_vec)
            .await
            .map_err(|e| AutumnError::RoutingError(e.to_string()))?;
        let ps = self
            .get_ps_client(&ps_addr)
            .await
            .map_err(|e| AutumnError::ConnectionError(e.to_string()))?;
        let req = PutBeginReq {
            part_id,
            key: key_vec.clone(),
            expires_at,
            total_bytes_hint: 0,
        };
        let payload = rkyv_encode(&req);
        let resp_bytes = ps
            .call(MSG_PUT_BEGIN, payload)
            .await
            .map_err(|e| AutumnError::ConnectionError(e.to_string()))?;
        let resp: PutBeginResp =
            rkyv_decode(&resp_bytes).map_err(|e| AutumnError::ServerError(e))?;
        if resp.code != partition_rpc::CODE_OK {
            return Err(code_to_error(resp.code, resp.message));
        }
        Ok(PutStreamHandle {
            upload_id: resp.upload_id,
            user_key: key_vec,
            part_id,
            ps,
            next_chunk_index: 0,
            bytes_sent: 0,
            state: PutStreamState::Open,
        })
    }

    /// Open a streaming reader over an existing key. The reader yields
    /// `chunk_size`-byte chunks via `next_chunk()`, walking the value
    /// without buffering the full payload in client memory. Useful for
    /// large multi-fragment values written via `put_stream_*`. Returns
    /// `Ok(None)` if the key doesn't exist.
    pub async fn get_stream(
        &self,
        key: &[u8],
        chunk_size: u32,
    ) -> std::result::Result<Option<GetStream>, AutumnError> {
        let key_vec = key.to_vec();
        let meta = self.head(&key_vec).await?;
        if !meta.found {
            return Ok(None);
        }
        let (part_id, ps_addr) = self
            .resolve_key(&key_vec)
            .await
            .map_err(|e| AutumnError::RoutingError(e.to_string()))?;
        let ps = self
            .get_ps_client(&ps_addr)
            .await
            .map_err(|e| AutumnError::ConnectionError(e.to_string()))?;
        Ok(Some(GetStream {
            user_key: key_vec,
            part_id,
            ps,
            total_bytes: meta.value_length,
            cursor: 0,
            chunk_size: chunk_size.max(1),
        }))
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

    /// F181: merge two adjacent partitions. Survivor keeps its part_id;
    /// victim is deleted from the manager.
    ///
    /// Stage 1 implementation orchestrates the merge from the client:
    ///   1. FLUSH on both partitions (drains imm into SSTs durable in row_stream).
    ///   2. Read commit_length on each of the six streams via stream_info.
    ///   3. Call manager's MSG_MULTI_MODIFY_MERGE with the sealed lengths.
    ///   4. On success, the survivor's PS picks up the wider rg + spliced
    ///      extent_ids on the next region_sync_loop tick (~2 s). Brief
    ///      unavailability during the reopen is the trade-off for not
    ///      requiring a PS-side splice handler in Stage 1.
    ///
    /// Caller is responsible for stopping writes to both partitions
    /// during the window between FLUSH and the manager call. Stage 2/3
    /// would add a proper PS-side handler with dual-gate + drain.
    pub async fn merge_partitions(
        &self,
        survivor_part_id: u64,
        victim_part_id: u64,
    ) -> std::result::Result<(), AutumnError> {
        self.flush(survivor_part_id).await?;
        self.flush(victim_part_id).await?;

        // Acquire an admin owner-lock for the merge sequence.
        let owner_key = format!("admin-merge:{survivor_part_id}:{victim_part_id}");
        let lock_resp_bytes = self
            .mgr_call(
                MSG_ACQUIRE_OWNER_LOCK,
                rkyv_encode(&AcquireOwnerLockReq { owner_key: owner_key.clone() }),
            )
            .await
            .map_err(|e| AutumnError::ServerError(e.to_string()))?;
        let lock_resp: AcquireOwnerLockResp =
            rkyv_decode(&lock_resp_bytes).map_err(AutumnError::ServerError)?;
        if lock_resp.code != autumn_rpc::manager_rpc::CODE_OK {
            return Err(AutumnError::ServerError(lock_resp.message));
        }
        let revision = lock_resp.revision;

        // Resolve stream IDs.
        let regions = {
            let resp_bytes = self
                .mgr_call(MSG_GET_REGIONS, Bytes::new())
                .await
                .map_err(|e| AutumnError::ServerError(e.to_string()))?;
            let resp: GetRegionsResp =
                rkyv_decode(&resp_bytes).map_err(AutumnError::ServerError)?;
            if resp.code != autumn_rpc::manager_rpc::CODE_OK {
                return Err(AutumnError::ServerError(resp.message));
            }
            resp.regions
        };
        let find_region = |pid: u64| -> std::result::Result<MgrRegionInfo, AutumnError> {
            regions
                .iter()
                .find(|(id, _)| *id == pid)
                .map(|(_, r)| r.clone())
                .ok_or(AutumnError::NotFound)
        };
        let s_region = find_region(survivor_part_id)?;
        let v_region = find_region(victim_part_id)?;

        // commit_length per stream.
        async fn commit_len_helper(
            client: &ClusterClient,
            stream_id: u64,
            owner_key: &str,
            revision: i64,
        ) -> std::result::Result<u64, AutumnError> {
            let req = rkyv_encode(&CheckCommitLengthReq {
                stream_id,
                owner_key: owner_key.to_string(),
                revision,
            });
            let resp_bytes = client
                .mgr_call(MSG_CHECK_COMMIT_LENGTH, req)
                .await
                .map_err(|e| AutumnError::ServerError(e.to_string()))?;
            let resp: CheckCommitLengthResp =
                rkyv_decode(&resp_bytes).map_err(AutumnError::ServerError)?;
            if resp.code != autumn_rpc::manager_rpc::CODE_OK {
                return Err(AutumnError::ServerError(resp.message));
            }
            Ok(resp.end as u64)
        }
        let log_lens = [
            commit_len_helper(self, s_region.log_stream, &owner_key, revision)
                .await?
                .max(1),
            commit_len_helper(self, v_region.log_stream, &owner_key, revision)
                .await?
                .max(1),
        ];
        let row_lens = [
            commit_len_helper(self, s_region.row_stream, &owner_key, revision)
                .await?
                .max(1),
            commit_len_helper(self, v_region.row_stream, &owner_key, revision)
                .await?
                .max(1),
        ];
        let meta_lens = [
            commit_len_helper(self, s_region.meta_stream, &owner_key, revision)
                .await?
                .max(1),
            commit_len_helper(self, v_region.meta_stream, &owner_key, revision)
                .await?
                .max(1),
        ];

        let req = rkyv_encode(&MultiModifyMergeReq {
            survivor_part_id,
            victim_part_id,
            owner_key,
            revision,
            log_sealed_lengths: log_lens,
            row_sealed_lengths: row_lens,
            meta_sealed_lengths: meta_lens,
        });
        let resp_bytes = self
            .mgr_call(MSG_MULTI_MODIFY_MERGE, req)
            .await
            .map_err(|e| AutumnError::ServerError(e.to_string()))?;
        let resp: MultiModifyMergeResp =
            rkyv_decode(&resp_bytes).map_err(AutumnError::ServerError)?;
        if resp.code != autumn_rpc::manager_rpc::CODE_OK {
            return Err(AutumnError::ServerError(resp.message));
        }
        Ok(())
    }

    /// F181: query the manager's policy-engine advisory cache.
    pub async fn policy_candidates(
        &self,
    ) -> std::result::Result<Vec<PolicyCandidate>, AutumnError> {
        let resp_bytes = self
            .mgr_call(
                MSG_GET_POLICY_CANDIDATES,
                rkyv_encode(&GetPolicyCandidatesReq::default()),
            )
            .await
            .map_err(|e| AutumnError::ServerError(e.to_string()))?;
        let resp: GetPolicyCandidatesResp =
            rkyv_decode(&resp_bytes).map_err(AutumnError::ServerError)?;
        if resp.code != autumn_rpc::manager_rpc::CODE_OK {
            return Err(AutumnError::ServerError(resp.message));
        }
        Ok(resp.candidates)
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

// ── F129 PutStream / GetStream handles ────────────────────────────────────

/// Lifecycle state of a `PutStreamHandle`. Open → (Committed | Aborted)
/// transitions are one-way; the `commit` / `abort` consumers move the
/// handle. A drop in the `Open` state logs a WARN; the PS-side TTL will
/// reclaim the session within `AUTUMN_PS_UPLOAD_TTL_SECS` (default 30 min).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PutStreamState {
    Open,
    Committed,
    Aborted,
}

/// In-progress multipart upload handle. Owns the cached `Rc<RpcClient>`
/// to the partition's PS so subsequent `send` calls don't re-resolve
/// (saves several RefCell + hashmap lookups per chunk vs going through
/// `call_ps_for_key`). The handle is `!Send` because it owns an `Rc`;
/// don't move it across compio runtimes.
pub struct PutStreamHandle {
    upload_id: u128,
    user_key: Vec<u8>,
    part_id: u64,
    ps: Rc<RpcClient>,
    next_chunk_index: u32,
    bytes_sent: u64,
    state: PutStreamState,
}

impl PutStreamHandle {
    /// Server-assigned u128 upload identifier. Useful for logs.
    pub fn upload_id(&self) -> u128 {
        self.upload_id
    }

    /// Total bytes successfully appended to log_stream so far.
    /// Updated only on successful `send`.
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent
    }

    /// Number of chunks successfully appended.
    pub fn chunks_sent(&self) -> u32 {
        self.next_chunk_index
    }

    /// Append one chunk. The PS appends `chunk` to log_stream as a
    /// single `OP_CHUNK_BLOB` WAL record and adds a fragment to the
    /// session. Returns the running total of bytes committed.
    ///
    /// Caller is responsible for choosing chunk sizes — typically
    /// 1–4 MiB. Larger chunks reduce per-chunk RPC overhead but raise
    /// peak memory on both client and PS; smaller chunks improve
    /// recoverability across transient network blips (less re-send on
    /// retry).
    pub async fn send(&mut self, chunk: &[u8]) -> std::result::Result<u64, AutumnError> {
        if self.state != PutStreamState::Open {
            return Err(AutumnError::InvalidArgument(format!(
                "PutStreamHandle is {:?}, not Open",
                self.state
            )));
        }
        let req = PutChunkReq {
            part_id: self.part_id,
            upload_id: self.upload_id,
            chunk_index: self.next_chunk_index,
            data: chunk.to_vec(),
        };
        let payload = rkyv_encode(&req);
        let resp_bytes = self
            .ps
            .call(MSG_PUT_CHUNK, payload)
            .await
            .map_err(|e| AutumnError::ConnectionError(e.to_string()))?;
        let resp: PutChunkResp =
            rkyv_decode(&resp_bytes).map_err(|e| AutumnError::ServerError(e))?;
        if resp.code != partition_rpc::CODE_OK {
            return Err(code_to_error(resp.code, resp.message));
        }
        self.next_chunk_index = self.next_chunk_index.saturating_add(1);
        self.bytes_sent = resp.bytes_committed;
        Ok(resp.bytes_committed)
    }

    /// Finalise the upload. The PS builds a multi-fragment ValuePointer
    /// from the session's fragment list, allocates a memtable seq, and
    /// writes one V1 WAL record (op = `OP_VALUE_POINTER_MULTI | 1`)
    /// before inserting into the active memtable. After this returns,
    /// the value is visible to subsequent `get` / `get_stream`.
    pub async fn commit(mut self) -> std::result::Result<(), AutumnError> {
        if self.state != PutStreamState::Open {
            return Err(AutumnError::InvalidArgument(format!(
                "PutStreamHandle is {:?}, cannot commit",
                self.state
            )));
        }
        let req = PutCommitReq {
            part_id: self.part_id,
            upload_id: self.upload_id,
            expected_total_bytes: self.bytes_sent,
        };
        let payload = rkyv_encode(&req);
        let resp_bytes = self
            .ps
            .call(MSG_PUT_COMMIT, payload)
            .await
            .map_err(|e| AutumnError::ConnectionError(e.to_string()))?;
        let resp: PutResp =
            rkyv_decode(&resp_bytes).map_err(|e| AutumnError::ServerError(e))?;
        if resp.code != partition_rpc::CODE_OK {
            self.state = PutStreamState::Aborted; // suppress Drop warn
            return Err(code_to_error(resp.code, resp.message));
        }
        self.state = PutStreamState::Committed;
        Ok(())
    }

    /// Discard the in-progress upload. Idempotent — calling abort on a
    /// handle whose session has already been TTL-reclaimed succeeds.
    /// The chunk bytes already in log_stream become OP_CHUNK_BLOB
    /// garbage; GC reclaims them when the host extent is punched.
    pub async fn abort(mut self) -> std::result::Result<(), AutumnError> {
        if self.state != PutStreamState::Open {
            return Ok(());
        }
        let req = PutAbortReq {
            part_id: self.part_id,
            upload_id: self.upload_id,
        };
        let payload = rkyv_encode(&req);
        // Best-effort: a transient connection error doesn't change the
        // outcome (TTL still reclaims).
        let _ = self.ps.call(MSG_PUT_ABORT, payload).await;
        self.state = PutStreamState::Aborted;
        Ok(())
    }
}

impl Drop for PutStreamHandle {
    fn drop(&mut self) {
        if self.state == PutStreamState::Open {
            tracing::warn!(
                upload_id = ?self.upload_id,
                key = ?String::from_utf8_lossy(&self.user_key),
                bytes_sent = self.bytes_sent,
                "PutStreamHandle dropped without commit/abort; \
                 PS-side TTL will reclaim within AUTUMN_PS_UPLOAD_TTL_SECS (~30 min)"
            );
        }
    }
}

/// Streaming reader over an existing value. Yields `chunk_size`-byte
/// chunks via `next_chunk()` until the value is exhausted. Each chunk
/// is one `MSG_GET` RPC under the hood, so multi-fragment values
/// (`OP_VALUE_POINTER_MULTI`) are reassembled by the PS's
/// `resolve_multi_frag` (sequential per-fragment reads); inline values
/// just get sliced.
pub struct GetStream {
    user_key: Vec<u8>,
    part_id: u64,
    ps: Rc<RpcClient>,
    total_bytes: u64,
    cursor: u64,
    chunk_size: u32,
}

impl GetStream {
    /// Total value size in bytes (set by the initial `head` call).
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Bytes yielded so far.
    pub fn position(&self) -> u64 {
        self.cursor
    }

    /// Bytes remaining to yield.
    pub fn remaining(&self) -> u64 {
        self.total_bytes.saturating_sub(self.cursor)
    }

    /// Pull the next chunk. Returns `Ok(None)` when the value is
    /// exhausted. The yielded chunk is at most `chunk_size` bytes; the
    /// final chunk may be shorter.
    pub async fn next_chunk(&mut self) -> std::result::Result<Option<Vec<u8>>, AutumnError> {
        if self.cursor >= self.total_bytes {
            return Ok(None);
        }
        let want = (self.total_bytes - self.cursor).min(self.chunk_size as u64) as u32;
        let req = GetReq {
            part_id: self.part_id,
            key: self.user_key.clone(),
            offset: self.cursor as u32,
            length: want,
        };
        let payload = rkyv_encode(&req);
        let resp_bytes = self
            .ps
            .call(MSG_GET, payload)
            .await
            .map_err(|e| AutumnError::ConnectionError(e.to_string()))?;
        let resp: GetResp =
            rkyv_decode(&resp_bytes).map_err(|e| AutumnError::ServerError(e))?;
        if resp.code == partition_rpc::CODE_NOT_FOUND {
            // Key was concurrently deleted mid-stream. Surface as Ok(None).
            return Ok(None);
        }
        if resp.code != partition_rpc::CODE_OK {
            return Err(code_to_error(resp.code, resp.message));
        }
        if resp.value.is_empty() {
            // Defensive: PS reported OK but no bytes. Shouldn't happen
            // with our resolve_value, but treat as EOF rather than spinning.
            return Ok(None);
        }
        self.cursor += resp.value.len() as u64;
        Ok(Some(resp.value))
    }
}
