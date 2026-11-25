//! autumn-etcd: compio-native etcd v3 client.
//!
//! Minimal etcd v3 gRPC client built on compio + hyper HTTP/2 (cleartext h2c,
//! or TLS), optionally authenticating as an etcd user.
//! Only implements the subset of APIs needed by autumn-manager:
//! - KV: Get (with prefix), Put, Txn (CAS, batch put/delete)
//! - Lease: Grant, KeepAlive, Revoke

pub mod proto;

mod transport;

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use prost::Message;

use proto::*;
use transport::{call_with_sender, is_stale_token, Dialer, GrpcChannel};
pub use transport::GrpcStatus;

/// One etcd endpoint: the address dialed, and the name its TLS certificate
/// is checked against (the host as the operator wrote it, before resolving).
#[derive(Clone, Debug)]
pub struct EtcdEndpoint {
    pub addr: String,
    pub server_name: String,
}

impl EtcdEndpoint {
    /// `"host:port"` (an optional `http://` / `https://` is dropped); the
    /// host is also the TLS server name.
    pub fn new(endpoint: &str) -> Self {
        let addr = normalize_endpoint(endpoint);
        let host = addr.rsplit_once(':').map_or(addr.as_str(), |(h, _)| h);
        let server_name = host.trim_matches(['[', ']']).to_string();
        Self { addr, server_name }
    }
}

/// The etcd user the client authenticates as.
#[derive(Clone)]
pub struct Credentials {
    pub user: String,
    pub password: String,
}

/// TLS to etcd: the CA that signs its certificates, and optionally this
/// client's certificate.
#[derive(Clone)]
pub struct TlsConfig(Arc<rustls::ClientConfig>);

impl TlsConfig {
    /// PEM files: `ca` holds one or more CA certificates; `client` is a
    /// certificate chain and its private key.
    pub fn from_pem_files(ca: &Path, client: Option<(&Path, &Path)>) -> Result<Self> {
        use rustls::pki_types::pem::PemObject;
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};

        let mut roots = rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_file_iter(ca)
            .with_context(|| format!("read CA file {}", ca.display()))?
        {
            let cert = cert.with_context(|| format!("parse CA file {}", ca.display()))?;
            roots
                .add(cert)
                .with_context(|| format!("load CA certificate from {}", ca.display()))?;
        }
        if roots.is_empty() {
            bail!("CA file {} holds no certificate", ca.display());
        }
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .context("TLS protocol versions")?
        .with_root_certificates(roots);
        let mut config = match client {
            None => builder.with_no_client_auth(),
            Some((cert, key)) => {
                let chain = CertificateDer::pem_file_iter(cert)
                    .with_context(|| format!("read certificate file {}", cert.display()))?
                    .collect::<Result<Vec<_>, _>>()
                    .with_context(|| format!("parse certificate file {}", cert.display()))?;
                if chain.is_empty() {
                    bail!("certificate file {} holds no certificate", cert.display());
                }
                let key = PrivateKeyDer::from_pem_file(key)
                    .with_context(|| format!("read private key file {}", key.display()))?;
                builder
                    .with_client_auth_cert(chain, key)
                    .context("client certificate and key")?
            }
        };
        // etcd serves gRPC over TLS only on HTTP/2, which it picks by ALPN.
        config.alpn_protocols = vec![b"h2".to_vec()];
        Ok(Self(Arc::new(config)))
    }
}

/// Everything needed to reach etcd.
#[derive(Clone)]
pub struct EtcdConfig {
    pub endpoints: Vec<EtcdEndpoint>,
    pub tls: Option<TlsConfig>,
    pub credentials: Option<Credentials>,
}

impl EtcdConfig {
    /// Cleartext, no authentication.
    pub fn plain(endpoints: &[String]) -> Self {
        Self {
            endpoints: endpoints.iter().map(|e| EtcdEndpoint::new(e)).collect(),
            tls: None,
            credentials: None,
        }
    }
}

/// A connection that moves to the next endpoint when the current one fails.
/// Shared shape of the client and of a lease keeper's own connection.
struct Conn {
    channel: RefCell<GrpcChannel>,
    endpoints: Vec<EtcdEndpoint>,
    /// Index of the currently connected endpoint.
    current_ep: Cell<usize>,
    dialer: Rc<Dialer>,
}

impl Conn {
    /// Connect (TCP, TLS, h2, authenticate) within `timeout`: a frozen etcd
    /// still completes the TCP handshake, then never answers.
    async fn open_at(
        endpoints: Vec<EtcdEndpoint>,
        idx: usize,
        dialer: Rc<Dialer>,
        timeout: Duration,
    ) -> Result<Self> {
        let ep = &endpoints[idx];
        let channel = within(timeout, GrpcChannel::connect(ep, &dialer))
            .await
            .ok_or_else(|| anyhow::anyhow!("connect to etcd {} timed out after {timeout:?}", ep.addr))??;
        Ok(Self {
            channel: RefCell::new(channel),
            endpoints,
            current_ep: Cell::new(idx),
            dialer,
        })
    }

    /// The first endpoint that connects, trying each once from `start`.
    async fn open_any(
        endpoints: Vec<EtcdEndpoint>,
        start: usize,
        dialer: Rc<Dialer>,
        timeout: Duration,
    ) -> Result<Self> {
        let n = endpoints.len();
        let mut errors = Vec::new();
        for i in 0..n {
            let idx = (start + i) % n;
            match Self::open_at(endpoints.clone(), idx, dialer.clone(), timeout).await {
                Ok(conn) => return Ok(conn),
                Err(e) => {
                    let addr = &endpoints[idx].addr;
                    tracing::warn!(endpoint = %addr, error = %e, "etcd connect failed, trying next");
                    errors.push(format!("{addr}: {e:#}"));
                }
            }
        }
        bail!("failed to connect to any etcd endpoint: {}", errors.join("; "))
    }

    /// `resend`: whether a request whose first attempt failed or timed out is
    /// sent again on the new connection. Reads and leases are; a txn is not.
    /// A refused token is the exception: etcd rejected the request before
    /// applying it, so it is sent again after authenticating, txn included.
    async fn call(
        &self,
        path: &str,
        body: Bytes,
        timeout: Duration,
        resend: bool,
    ) -> Result<Bytes> {
        // clone the call handle out of the RefCell, then drop the borrow
        // before awaiting. Holding `RefMut<GrpcChannel>` across `.await` would
        // panic the next concurrent caller on the same single-threaded runtime
        // (e.g. 4 partitions racing on `handle_stream_punch_holes`).
        //
        // (1A): bound the call with `compio::time::timeout`. Without this
        // a stuck server / half-open TCP hangs the caller forever (and with it
        // the manager's `node_health_loop`). A timeout falls through to the
        // reconnect + retry path, same as a connection error.
        let mut call = self.channel.borrow().call();
        let cause = match within(timeout, call_with_sender(&mut call, path, body.clone())).await {
            Some(Ok(resp)) => return Ok(resp),
            Some(Err(e)) if is_stale_token(&e) => {
                let Some(creds) = &self.dialer.credentials else {
                    return Err(e);
                };
                tracing::info!(path, error = %e, "etcd refused the token, authenticating again");
                // Boxed, like the reconnect: rare paths kept out of every call's future.
                return Box::pin(reauth_and_resend(call, creds, path, body, timeout, resend)).await;
            }
            Some(Err(e)) => {
                // Connection might be dead. Try reconnecting to another endpoint.
                tracing::warn!(path, error = %e, "etcd call failed, attempting reconnect");
                e.to_string()
            }
            None => {
                tracing::warn!(path, ?timeout, "etcd call timed out, attempting reconnect");
                format!("timed out after {timeout:?}")
            }
        };

        // Reconnect: try each endpoint once (round-robin from the next one),
        // each within `timeout`.
        Box::pin(self.reconnect(timeout)).await?;
        if !resend {
            return Err(UncertainOutcome { path: path.to_string(), cause }.into());
        }

        // Retry on the new connection (re-clone after the channel was swapped).
        let mut call = self.channel.borrow().call();
        match within(timeout, call_with_sender(&mut call, path, body)).await {
            Some(r) => r,
            None => Err(anyhow::anyhow!(
                "etcd call '{path}' timed out after retry ({timeout:?})"
            )),
        }
    }

    /// Each endpoint is bounded on its own: with TLS or auth, a connect to a
    /// frozen member hangs after TCP, and one bound over all of them never
    /// reached the members after it.
    async fn reconnect(&self, timeout: Duration) -> Result<()> {
        let n = self.endpoints.len();
        let start = self.current_ep.get();
        let mut errors = Vec::new();
        for i in 1..=n {
            let idx = (start + i) % n;
            let ep = &self.endpoints[idx];
            let bound = timeout.min(CONNECT_TIMEOUT);
            let connected = within(bound, GrpcChannel::connect(ep, &self.dialer))
                .await
                .unwrap_or_else(|| Err(anyhow::anyhow!("connect timed out after {bound:?}")));
            match connected {
                Ok(new_channel) => {
                    *self.channel.borrow_mut() = new_channel;
                    self.current_ep.set(idx);
                    tracing::info!(endpoint = %ep.addr, "etcd reconnected");
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(endpoint = %ep.addr, error = %e, "etcd reconnect failed");
                    errors.push(format!("{}: {e:#}", ep.addr));
                }
            }
        }
        bail!("failed to reconnect to any etcd endpoint: {}", errors.join("; "))
    }
}

/// Authenticate the call's connection again and send the request once more.
/// etcd refused the token before the request reached raft, so even a txn
/// may go again. Only a second refusal of the token is a definite failure
/// for a txn; anything else may have committed.
async fn reauth_and_resend(
    mut call: transport::Call,
    creds: &Credentials,
    path: &str,
    body: Bytes,
    timeout: Duration,
    resend: bool,
) -> Result<Bytes> {
    match within(timeout, transport::authenticate(&mut call, creds)).await {
        Some(r) => r.context("authenticate to etcd again")?,
        None => bail!("etcd authenticate timed out after {timeout:?}"),
    }
    let outcome = within(timeout, call_with_sender(&mut call, path, body)).await;
    resend_outcome(path, timeout, resend, outcome)
}

/// The result of the send after a re-authentication. For a txn
/// (`!resend`), only a token refused again proves it did not apply.
fn resend_outcome(
    path: &str,
    timeout: Duration,
    resend: bool,
    outcome: Option<Result<Bytes>>,
) -> Result<Bytes> {
    match outcome {
        Some(Ok(resp)) => Ok(resp),
        Some(Err(e)) if resend || is_stale_token(&e) => Err(e),
        Some(Err(e)) => Err(UncertainOutcome { path: path.to_string(), cause: e.to_string() }.into()),
        None if resend => bail!("etcd call '{path}' timed out after {timeout:?}"),
        None => Err(UncertainOutcome {
            path: path.to_string(),
            cause: format!("timed out after {timeout:?}"),
        }
        .into()),
    }
}

/// `fut` bounded by `timeout`; `None` = timed out. Every etcd call goes
/// through here: nothing awaits etcd without a deadline.
async fn within<F: Future>(timeout: Duration, fut: F) -> Option<F::Output> {
    compio::time::timeout(timeout, fut).await.ok()
}

/// Minimal etcd v3 client for compio runtime (single-threaded, Rc-based).
///
/// Supports etcd clusters: stores all endpoints and automatically reconnects
/// to the next endpoint when the current connection fails.
///
/// All methods take `&self` (the connection's channel is a `RefCell` and is
/// the only mutable state — swapped on reconnect; concurrent calls are safe
/// via the handle-clone-out-of-cell pattern). Callers therefore hold it as a
/// plain `Rc<EtcdClient>` (no outer `RefCell`), so a manager etcd RPC never
/// holds a borrow across `.await`.
pub struct EtcdClient {
    conn: Conn,
    /// Bound on every unary call and on the first connect. etcd over HTTP/2 has
    /// no request deadline of its own, so a half-open connection would hang
    /// the caller forever; a timeout turns it into the reconnect-and-retry path.
    request_timeout: Duration,
}

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Cap on one endpoint's connect (TCP, TLS, h2, authenticate) in a mid-call
/// reconnect: milliseconds on the LAN etcd sits on (a bcrypt check is
/// ~100 ms). Without it a round over n members cost n request timeouts. The
/// first connect is not capped: with auth it waits for an etcd leader.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

impl EtcdClient {
    /// Connect to an etcd endpoint (plaintext HTTP/2, h2c).
    ///
    /// `endpoint` is `"host:port"` or `"http://host:port"`.
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let conn = Conn::open_at(
            vec![EtcdEndpoint::new(endpoint)],
            0,
            Rc::new(Dialer { tls: None, credentials: None }),
            DEFAULT_REQUEST_TIMEOUT,
        )
        .await?;
        Ok(Self { conn, request_timeout: DEFAULT_REQUEST_TIMEOUT })
    }

    /// [`connect_with`](Self::connect_with) over cleartext, no authentication.
    pub async fn connect_many(endpoints: &[String]) -> Result<Self> {
        Self::connect_with(EtcdConfig::plain(endpoints), DEFAULT_REQUEST_TIMEOUT).await
    }

    /// Connect to the first endpoint that accepts (TLS handshake and
    /// authentication included). On connection failure during operation,
    /// automatically reconnects to the next endpoint in round-robin order.
    /// `request_timeout` bounds each endpoint's first connect and every call.
    pub async fn connect_with(config: EtcdConfig, request_timeout: Duration) -> Result<Self> {
        let dialer = Rc::new(Dialer {
            tls: config.tls.map(|t| compio::tls::TlsConnector::from(t.0)),
            credentials: config.credentials,
        });
        let conn = Conn::open_any(config.endpoints, 0, dialer, request_timeout).await?;
        Ok(Self { conn, request_timeout })
    }

    // ── KV: Get ──────────────────────────────────────────────────────────

    /// Get a single key from etcd.
    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<RangeResponse> {
        let req = RangeRequest {
            key: key.as_ref().to_vec(),
            ..Default::default()
        };
        self.range(req).await
    }

    /// Get all keys with a given prefix.
    pub async fn get_prefix(&self, prefix: impl AsRef<[u8]>) -> Result<RangeResponse> {
        let key = prefix.as_ref().to_vec();
        let range_end = prefix_range_end(&key);
        let req = RangeRequest {
            key,
            range_end,
            ..Default::default()
        };
        self.range(req).await
    }

    /// Execute a range request.
    pub async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        self.unary_call("/etcdserverpb.KV/Range", &req).await
    }

    // ── KV: Put ──────────────────────────────────────────────────────────

    /// Put a key-value pair.
    pub async fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<PutResponse> {
        let req = PutRequest {
            key: key.as_ref().to_vec(),
            value: value.as_ref().to_vec(),
            ..Default::default()
        };
        self.put_request(req).await
    }

    /// Put with a lease attached.
    pub async fn put_with_lease(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        lease: i64,
    ) -> Result<PutResponse> {
        let req = PutRequest {
            key: key.as_ref().to_vec(),
            value: value.as_ref().to_vec(),
            lease,
            ..Default::default()
        };
        self.put_request(req).await
    }

    /// Execute a put request.
    pub async fn put_request(&self, req: PutRequest) -> Result<PutResponse> {
        self.unary_call("/etcdserverpb.KV/Put", &req).await
    }

    // ── KV: Delete ───────────────────────────────────────────────────────

    /// Delete a single key.
    pub async fn delete(&self, key: impl AsRef<[u8]>) -> Result<DeleteRangeResponse> {
        let req = DeleteRangeRequest {
            key: key.as_ref().to_vec(),
            ..Default::default()
        };
        self.unary_call("/etcdserverpb.KV/DeleteRange", &req).await
    }

    // ── KV: Txn ──────────────────────────────────────────────────────────

    /// Execute a transaction.
    /// A txn is not re-sent after a timeout or a connection error: the first
    /// copy may have committed, and a compare-and-set sent twice refuses its
    /// own write. The caller gets [`UncertainOutcome`] and decides.
    pub async fn txn(&self, req: TxnRequest) -> Result<TxnResponse> {
        self.txn_ref(&req).await
    }

    /// `txn` for a request the caller keeps (to send again under a new
    /// compare without cloning its ops).
    pub async fn txn_ref(&self, req: &TxnRequest) -> Result<TxnResponse> {
        self.unary_call_with("/etcdserverpb.KV/Txn", req, false).await
    }

    // ── Lease ────────────────────────────────────────────────────────────

    /// Grant a new lease with the given TTL (seconds).
    pub async fn lease_grant(&self, ttl: i64) -> Result<LeaseGrantResponse> {
        let req = LeaseGrantRequest {
            ttl,
            id: 0, // auto-assign
        };
        self.unary_call("/etcdserverpb.Lease/LeaseGrant", &req)
            .await
    }

    /// A keeper that renews the lease on its **own** connection (the client's
    /// endpoint first, then the others). Opening it and every renewal step (send,
    /// reconnect, resend, authenticate) are each bounded by `attempt_timeout`,
    /// which should stay well under the lease TTL so a renewal can fail over
    /// to another member before the lease runs out.
    pub async fn lease_keep_alive(&self, lease_id: i64, attempt_timeout: Duration) -> Result<LeaseKeeper> {
        let conn = Conn::open_any(
            self.conn.endpoints.clone(),
            self.conn.current_ep.get(),
            self.conn.dialer.clone(),
            attempt_timeout,
        )
        .await?;
        Ok(LeaseKeeper { conn, lease_id, attempt_timeout })
    }

    /// Revoke a lease, releasing all keys attached to it.
    pub async fn lease_revoke(&self, lease_id: i64) -> Result<LeaseRevokeResponse> {
        let req = LeaseRevokeRequest { id: lease_id };
        self.unary_call("/etcdserverpb.Lease/LeaseRevoke", &req)
            .await
    }

    /// Return the currently connected endpoint address.
    pub fn current_endpoint(&self) -> String {
        self.conn.endpoints[self.conn.current_ep.get()].addr.clone()
    }

    // ── Internal ─────────────────────────────────────────────────────────

    /// A unary RPC this client has no method for (etcd administration in
    /// tests: users, roles, enabling auth). Sent again after a failure.
    pub async fn unary<Req: Message, Resp: Message + Default>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Resp> {
        self.unary_call(path, req).await
    }

    async fn unary_call<Req: Message, Resp: Message + Default>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Resp> {
        self.unary_call_with(path, req, true).await
    }

    async fn unary_call_with<Req: Message, Resp: Message + Default>(
        &self,
        path: &str,
        req: &Req,
        resend: bool,
    ) -> Result<Resp> {
        let resp = self
            .conn
            .call(path, grpc_encode(req), self.request_timeout, resend)
            .await?;
        grpc_decode::<Resp>(&resp)
    }
}

/// Lease keep-alive handle.
///
/// Sends periodic keep-alive requests to etcd for a specific lease.
/// Each `keep_alive()` call sends one request and reads one response.
/// On connection failure, it reconnects to the next endpoint transparently.
pub struct LeaseKeeper {
    conn: Conn,
    lease_id: i64,
    attempt_timeout: Duration,
}

impl LeaseKeeper {
    /// Send a keep-alive ping and return the response.
    /// Returns the remaining TTL. Returns Err if the lease has expired or etcd is unreachable.
    pub async fn keep_alive(&self) -> Result<LeaseKeepAliveResponse> {
        let req = LeaseKeepAliveRequest { id: self.lease_id };
        let resp = self
            .conn
            .call("/etcdserverpb.Lease/LeaseKeepAlive", grpc_encode(&req), self.attempt_timeout, true)
            .await?;
        grpc_decode::<LeaseKeepAliveResponse>(&resp)
    }

    /// Get the lease ID this keeper is managing.
    pub fn lease_id(&self) -> i64 {
        self.lease_id
    }
}

// ── Txn builder helpers ─────────────────────────────────────────────────────

/// Helper for building Compare operations.
pub struct Cmp;

impl Cmp {
    /// Compare a key's latest modification revision with an exact revision.
    /// This distinguishes delete-and-recreate from an unchanged value.
    pub fn mod_revision(key: impl AsRef<[u8]>, revision: i64) -> Compare {
        Compare {
            result: 0,
            target: 2,
            key: key.as_ref().to_vec(),
            target_union: Some(TargetUnion::ModRevision(revision)),
            ..Default::default()
        }
    }

    /// Compare create_revision of a key equals the given value (0 = key does not exist).
    pub fn create_revision(key: impl AsRef<[u8]>, rev: i64) -> Compare {
        Compare {
            result: 0, // EQUAL
            target: 1, // CREATE
            key: key.as_ref().to_vec(),
            target_union: Some(TargetUnion::CreateRevision(rev)),
            ..Default::default()
        }
    }

    /// Compare version of a key equals the given value (0 = key does not exist).
    pub fn version(key: impl AsRef<[u8]>, version: i64) -> Compare {
        Compare {
            result: 0, // EQUAL
            target: 0, // VERSION
            key: key.as_ref().to_vec(),
            target_union: Some(TargetUnion::Version(version)),
            ..Default::default()
        }
    }

    /// Compare value of a key equals the given byte slice. Used by autumn-rs
    /// for the leader-fence: every manager etcd write txn prepends a
    /// `Cmp::value(LEADER_KEY) == instance_id` so a deposed leader cannot
    /// stomp on the new leader's writes during the failover window.
    pub fn value(key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Compare {
        Compare {
            result: 0, // EQUAL
            target: 3, // VALUE
            key: key.as_ref().to_vec(),
            target_union: Some(TargetUnion::Value(value.as_ref().to_vec())),
            ..Default::default()
        }
    }
}

/// A request that may or may not have been applied: it failed or timed out
/// after it was handed to the connection, and was not sent again.
#[derive(Debug)]
pub struct UncertainOutcome {
    pub path: String,
    pub cause: String,
}

impl std::fmt::Display for UncertainOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "etcd call '{}' outcome unknown: {}", self.path, self.cause)
    }
}

impl std::error::Error for UncertainOutcome {}

/// Helper for building RequestOp operations.
pub struct Op;

impl Op {
    /// Create a Put operation.
    pub fn put(key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> RequestOp {
        RequestOp {
            request: Some(RequestOpInner::RequestPut(PutRequest {
                key: key.as_ref().to_vec(),
                value: value.as_ref().to_vec(),
                ..Default::default()
            })),
        }
    }

    /// Create a Put operation with a lease.
    pub fn put_with_lease(key: impl AsRef<[u8]>, value: impl AsRef<[u8]>, lease: i64) -> RequestOp {
        RequestOp {
            request: Some(RequestOpInner::RequestPut(PutRequest {
                key: key.as_ref().to_vec(),
                value: value.as_ref().to_vec(),
                lease,
                ..Default::default()
            })),
        }
    }

    /// Create a Delete operation.
    /// Read one key inside a txn (`ResponseOpInner::ResponseRange`).
    pub fn get(key: impl AsRef<[u8]>) -> RequestOp {
        RequestOp {
            request: Some(RequestOpInner::RequestRange(RangeRequest {
                key: key.as_ref().to_vec(),
                ..Default::default()
            })),
        }
    }

    pub fn delete(key: impl AsRef<[u8]>) -> RequestOp {
        RequestOp {
            request: Some(RequestOpInner::RequestDeleteRange(DeleteRangeRequest {
                key: key.as_ref().to_vec(),
                ..Default::default()
            })),
        }
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Compute the range_end for a prefix scan.
/// This is the smallest byte string that is not a prefix of `key`.
fn prefix_range_end(key: &[u8]) -> Vec<u8> {
    let mut end = key.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] < 0xFF {
            end[i] += 1;
            end.truncate(i + 1);
            return end;
        }
    }
    // key is all 0xFF — use \x00 as range_end (meaning no bound)
    vec![0]
}

fn normalize_endpoint(ep: &str) -> String {
    let ep = ep.trim();
    let ep = ep
        .strip_prefix("http://")
        .or_else(|| ep.strip_prefix("https://"))
        .unwrap_or(ep);
    ep.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix_range_end() {
        assert_eq!(prefix_range_end(b"abc"), b"abd");
        assert_eq!(prefix_range_end(b"ab\xff"), b"ac");
        assert_eq!(prefix_range_end(b"\xff\xff"), vec![0]);
        assert_eq!(prefix_range_end(b"nodes/"), b"nodes0"); // '/' + 1 = '0'
    }

    #[test]
    fn test_grpc_roundtrip() {
        let req = PutRequest {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
            ..Default::default()
        };
        let encoded = grpc_encode(&req);
        let decoded: PutRequest = grpc_decode(&encoded).unwrap();
        assert_eq!(decoded.key, b"hello");
        assert_eq!(decoded.value, b"world");
    }

    #[test]
    fn mod_revision_compare_targets_the_exact_lifetime() {
        let compare = Cmp::mod_revision(b"extent_inflight/7", 42);
        assert_eq!(compare.target, 2);
        assert!(matches!(
            compare.target_union,
            Some(TargetUnion::ModRevision(42))
        ));
    }

    /// After a re-authentication, a txn fails definitely only when the token
    /// is refused again (rejected before raft); any other status may follow a
    /// commit ("request timed out" is answered after the proposal).
    #[test]
    fn txn_after_reauth_is_uncertain_unless_the_token_is_refused_again() {
        let status = |code, message: &str| -> Option<Result<Bytes>> {
            Some(Err(GrpcStatus { path: "txn".into(), code, message: message.into() }.into()))
        };
        let uncertain = |r: Result<Bytes>| {
            r.err().expect("an error").downcast_ref::<UncertainOutcome>().is_some()
        };
        let txn = |outcome| resend_outcome("txn", Duration::from_secs(1), false, outcome);
        assert!(uncertain(txn(status(14, "etcdserver: request timed out"))));
        assert!(uncertain(txn(None)));
        assert!(!uncertain(txn(status(16, "etcdserver: invalid auth token"))));
        let get = resend_outcome("get", Duration::from_secs(1), true, status(14, "timed out"));
        assert!(!uncertain(get));
    }
}
