//! Per-process connection pool for extent nodes (single-threaded compio).
//!
//! One `Rc<RpcClient>` per SocketAddr. Since autumn-rpc's `RpcClient`
//! handles concurrent `call`/`call_vectored` via an internal
//! `Mutex<WriteHalf>` (byte-level frame serialization) plus a background
//! reader that dispatches responses by `req_id`, multiple tasks can
//! drive appends against the same addr simultaneously — true TCP
//! multiplex, not one-caller-at-a-time.
//!
//! Historical note: before R3, this pool used a take/put `RefCell<Option<RpcConn>>`
//! pattern with an embedded sequential `RpcConn` struct. That prevented
//! multiplex even though autumn-rpc supports it at the wire level. R3
//! replaces the custom sequential conn with `Rc<RpcClient>` (shared).

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use autumn_rpc::client::RpcClient;
use bytes::Bytes;

/// (1A): bound the TCP connect so a blackholed peer (SYN dropped) can't
/// hang `get_client` — and therefore any PS/EN background loop that reaches it
/// (region_sync `open_partition` → `commit_length`, EN reconcile, recovery
/// fanout) — indefinitely. `call_timeout` only bounds the call AFTER connect;
/// the connect itself had no deadline. Mirrors the manager-side connect-timeout
/// fix (`AUTUMN_MGR_CONNECT_TIMEOUT_MS`); a fixed constant here (not a tuning
/// knob) keeps it env-free per the config rule. 5 s is generous for
/// datacenter / loopback (sub-second normal); on expiry the entry is not
/// cached, so the next call retries a fresh connect.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub struct ConnPool {
    clients: RefCell<HashMap<SocketAddr, Rc<RpcClient>>>,
}

/// A pipelined response receiver that PINS its connection.
///
/// The pipelined senders below hand back a receiver and keep nothing else, so
/// the pool map held the only `Rc<RpcClient>`. Dropping the last one now closes
/// the connection (autumn-rpc `RpcClient`'s task handles are its teardown), and
/// eviction is a thing OTHER tasks do: one worker's replica timeout evicts the
/// address another worker is mid-append on. Without this pin, that append's
/// frame would be abandoned mid-`writev` and its caller would see a closed
/// connection — an append that was about to succeed, failed by an unrelated
/// caller's error. Holding the `Rc` here scopes the connection to the work
/// outstanding on it rather than to the pool entry: an evicted connection goes
/// away once its last in-flight response has landed or timed out (every caller
/// bounds its own wait), and no new caller can reach it through the pool.
///
/// `#[must_use]` because the inner `oneshot::Receiver` carries it and a newtype
/// does not inherit it: a receiver nobody awaits is a request nobody reads.
#[must_use = "a response receiver does nothing unless you .await it"]
pub struct PinnedRecv {
    rx: futures::channel::oneshot::Receiver<autumn_rpc::Frame>,
    /// Kept alive for exactly as long as the response is outstanding.
    _conn: Rc<RpcClient>,
}

impl std::future::Future for PinnedRecv {
    type Output = Result<autumn_rpc::Frame, futures::channel::oneshot::Canceled>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.rx).poll(cx)
    }
}

impl ConnPool {
    pub fn new() -> Self {
        Self {
            clients: RefCell::new(HashMap::new()),
        }
    }

    /// Get or open an RpcClient for `addr`. Uses the existing pool entry
    /// if present and not closed; otherwise connects and stashes.
    /// Connection is shared across concurrent callers.
    ///
    /// a cached entry whose `read_loop`/`writer_task` has exited
    /// (i.e. the peer died) is poisoned — `client.is_closed()` returns
    /// true. Returning it would cause new submits to fail fast (good)
    /// but lock us out of any reconnect attempt. Evict and reconnect
    /// instead so peer recovery surfaces as a fresh successful client
    /// and a still-dead peer surfaces as `ECONNREFUSED` immediately.
    async fn get_client(&self, addr: SocketAddr) -> Result<Rc<RpcClient>> {
        if let Some(client) = self.clients.borrow().get(&addr).cloned() {
            if !client.is_closed() {
                return Ok(client);
            }
        }
        // Evict any closed entry under a fresh borrow so the upcoming
        // `connect.await` doesn't hold the RefCell across a yield.
        self.clients.borrow_mut().remove(&addr);
        // (1A): bound the connect (see CONNECT_TIMEOUT). On timeout the
        // entry stays uncached so the next call retries a fresh connect.
        let client = match compio::time::timeout(CONNECT_TIMEOUT, RpcClient::connect(addr)).await {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => return Err(anyhow!("connect {}: {}", addr, e)),
            Err(_) => {
                return Err(anyhow!(
                    "connect {} timed out after {:?}",
                    addr,
                    CONNECT_TIMEOUT
                ))
            }
        };
        self.clients.borrow_mut().insert(addr, client.clone());
        Ok(client)
    }

    /// Evict the pooled client for `addr` (next `get_client` reconnects).
    fn evict(&self, addr: SocketAddr) {
        self.clients.borrow_mut().remove(&addr);
    }

    /// Send an RPC and await the response. On error, evict the client so
    /// the next call reconnects (matches R2 behavior semantically).
    pub async fn call(&self, addr: &str, msg_type: u8, payload: Bytes) -> Result<Bytes> {
        let sock = parse_addr(addr)?;
        let client = self.get_client(sock).await?;
        match client.call(msg_type, payload).await {
            Ok(bytes) => Ok(bytes),
            Err(e) => {
                self.evict(sock);
                Err(anyhow!("{}", e))
            }
        }
    }

    /// Send an RPC with a timeout. Same error-eviction contract as `call`.
    pub async fn call_timeout(
        &self,
        addr: &str,
        msg_type: u8,
        payload: Bytes,
        timeout: Duration,
    ) -> Result<Bytes> {
        let sock = parse_addr(addr)?;
        let client = self.get_client(sock).await?;
        match client.call_timeout(msg_type, payload, timeout).await {
            Ok(bytes) => Ok(bytes),
            Err(e) => {
                self.evict(sock);
                Err(anyhow!("{}", e))
            }
        }
    }

    /// send an RPC and recv the value response into a read_loop-owned
    /// `PooledBuf` (cancel-safe — see RpcClient::call_into_pooled). Wraps the
    /// timeout here; on expiry the inner future drops and the read_loop reclaims
    /// the buffer (no leak). Returns a `BulkResp` (buffer + code + message).
    pub async fn call_into_pooled(
        &self,
        addr: &str,
        msg_type: u8,
        payload: Bytes,
        timeout: Duration,
    ) -> Result<autumn_rpc::client::BulkResp> {
        let sock = parse_addr(addr)?;
        let client = self.get_client(sock).await?;
        let fut = client.call_into_pooled(msg_type, payload);
        match compio::time::timeout(timeout, fut).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => {
                self.evict(sock);
                Err(anyhow!("{}", e))
            }
            Err(_) => {
                self.evict(sock);
                Err(anyhow!("call_into_pooled timed out after {:?}", timeout))
            }
        }
    }

    /// Send an RPC with payload already split into parts (zero-copy).
    pub async fn call_vectored(
        &self,
        addr: &str,
        msg_type: u8,
        payload_parts: Vec<Bytes>,
    ) -> Result<Bytes> {
        let sock = parse_addr(addr)?;
        let client = self.get_client(sock).await?;
        match client.call_vectored(msg_type, payload_parts).await {
            Ok(bytes) => Ok(bytes),
            Err(e) => {
                self.evict(sock);
                Err(anyhow!("{}", e))
            }
        }
    }

    /// Send a vectored request and return the oneshot receiver for the
    /// response, without awaiting. Enables R3 pipelining: StreamClient
    /// fires 3 `send_vectored` calls under a short mutex, then awaits
    /// all 3 receivers concurrently outside the mutex.
    pub async fn send_vectored(
        &self,
        addr: &str,
        msg_type: u8,
        payload_parts: Vec<Bytes>,
    ) -> Result<PinnedRecv> {
        let sock = parse_addr(addr)?;
        let client = self.get_client(sock).await?;
        match client.send_vectored(msg_type, payload_parts).await {
            Ok(rx) => Ok(PinnedRecv {
                rx,
                _conn: client.clone(),
            }),
            Err(e) => {
                // matches `call`/`call_timeout` semantics — evict
                // on submit-time error so the next call retries with a
                // fresh client. `client.is_closed()` is `true` whenever
                // the reader/writer task has exited, regardless of which
                // path triggered the error.
                if client.is_closed() {
                    self.evict(sock);
                }
                Err(anyhow!("{}", e))
            }
        }
    }

    pub fn is_healthy(&self, addr: &str) -> bool {
        let Ok(sock) = parse_addr(addr) else {
            return false;
        };
        self.clients.borrow().contains_key(&sock)
    }

    pub async fn send_prepared(
        &self,
        addr: &str,
        msg_type: u8,
        payload: &autumn_rpc::frame::PreparedPayload,
    ) -> Result<PinnedRecv> {
        let sock = parse_addr(addr)?;
        let client = self.get_client(sock).await?;
        match client.send_prepared(msg_type, payload).await {
            Ok(rx) => Ok(PinnedRecv {
                rx,
                _conn: client.clone(),
            }),
            Err(e) => {
                if client.is_closed() {
                    self.evict(sock);
                }
                Err(anyhow!("{e}"))
            }
        }
    }
}

impl Default for ConnPool {
    fn default() -> Self {
        Self::new()
    }
}

/// route `extent_id` to the correct shard port.
///
/// If `shard_ports` is empty, returns `address` unchanged (legacy mode).
/// Otherwise replaces the port in `address` with
/// `shard_ports[autumn_rpc::shard_for_extent(extent_id, K)]` (the canonical
/// hashed map; was `extent_id % K`) and returns the
/// resulting `host:port` string.
pub fn shard_addr_for_extent(address: &str, shard_ports: &[u16], extent_id: u64) -> String {
    if shard_ports.is_empty() {
        return address.to_string();
    }
    let k = shard_ports.len();
    // canonical hashed extent→shard map (was `extent_id % k`,
    // which aliased bootstrap's contiguous ids onto shard 0). MUST match the EN
    // `owns_extent` + manager `shard_addr_for_extent`.
    let port = shard_ports[autumn_rpc::shard_for_extent(extent_id, k as u32) as usize];
    // Replace port while preserving host. Address may be of the form
    // "host:port" or "[ipv6]:port". Split at the last ':' before the port.
    let addr_trimmed = address
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    if let Some(colon) = addr_trimmed.rfind(':') {
        format!("{}:{}", &addr_trimmed[..colon], port)
    } else {
        format!("{address}:{port}")
    }
}

/// Parse a "host:port" address into a SocketAddr.
pub fn parse_addr(addr: &str) -> Result<SocketAddr> {
    let stripped = addr
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    stripped
        .parse::<SocketAddr>()
        .map_err(|e| anyhow!("invalid address {:?}: {}", addr, e))
}

/// Normalize an address string by stripping any http:// prefix.
pub fn normalize_endpoint(addr: &str) -> String {
    addr.trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_string()
}
