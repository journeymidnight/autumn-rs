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
    async fn open_at(endpoints: Vec<EtcdEndpoint>, idx: usize, dialer: Rc<Dialer>) -> Result<Self> {
        let channel = GrpcChannel::connect(&endpoints[idx], &dialer).await?;
        Ok(Self {
            channel: RefCell::new(channel),
            endpoints,
            current_ep: Cell::new(idx),
            dialer,
        })
    }

    /// `resend`: whether a request whose first attempt failed or timed out is
    /// sent again on the new connection. Reads and leases are; a txn is not.
    /// A refused token is the exception: etcd rejected the request before
    /// applying it, so it is sent again after authenticating, txn included.
    async fn call(
        &self,
        path: &str,
        body: Bytes,
        timeout: Option<Duration>,
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

        // Reconnect: try each endpoint once (round-robin from the next one).
        // (1A): also bounded — a hung connect must not wedge the loop.
        within(timeout, Box::pin(self.reconnect()))
            .await
            .ok_or_else(|| anyhow::anyhow!("etcd reconnect timed out after {timeout:?}"))??;
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

    async fn reconnect(&self) -> Result<()> {
        let n = self.endpoints.len();
        let start = self.current_ep.get();
        let mut errors = Vec::new();
        for i in 1..=n {
            let idx = (start + i) % n;
            let ep = &self.endpoints[idx];
            match GrpcChannel::connect(ep, &self.dialer).await {
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
    timeout: Option<Duration>,
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
    timeout: Option<Duration>,
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

/// `fut` bounded by `timeout` when there is one; `None` = timed out.
async fn within<F: Future>(timeout: Option<Duration>, fut: F) -> Option<F::Output> {
    match timeout {
        Some(t) => compio::time::timeout(t, fut).await.ok(),
        None => Some(fut.await),
    }
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
    /// Bound on every unary call (and on its reconnect). etcd over HTTP/2 has
    /// no request deadline of its own, so a half-open connection would hang
    /// the caller forever; a timeout turns it into the reconnect-and-retry path.
    request_timeout: Duration,
}

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

impl EtcdClient {
    /// Connect to an etcd endpoint (plaintext HTTP/2, h2c).
    ///
    /// `endpoint` is `"host:port"` or `"http://host:port"`.
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let conn = Conn::open_at(
            vec![EtcdEndpoint::new(endpoint)],
            0,
            Rc::new(Dialer { tls: None, credentials: None }),
        )
        .await?;
        Ok(Self { conn, request_timeout: DEFAULT_REQUEST_TIMEOUT })
    }

    /// [`connect_with`](Self::connect_with) over cleartext, no authentication.
    pub async fn connect_many(endpoints: &[String]) -> Result<Self> {
        Self::connect_with(EtcdConfig::plain(endpoints)).await
    }

    /// Connect to the first endpoint that accepts (TLS handshake and
    /// authentication included). On connection failure during operation,
    /// automatically reconnects to the next endpoint in round-robin order.
    pub async fn connect_with(config: EtcdConfig) -> Result<Self> {
        let dialer = Rc::new(Dialer {
            tls: config.tls.map(|t| compio::tls::TlsConnector::from(t.0)),
            credentials: config.credentials,
        });
        let mut errors = Vec::new();
        for i in 0..config.endpoints.len() {
            match Conn::open_at(config.endpoints.clone(), i, dialer.clone()).await {
                Ok(conn) => return Ok(Self { conn, request_timeout: DEFAULT_REQUEST_TIMEOUT }),
                Err(e) => {
                    let addr = &config.endpoints[i].addr;
                    tracing::warn!(endpoint = %addr, error = %e, "etcd connect failed, trying next");
                    errors.push(format!("{addr}: {e:#}"));
                }
            }
        }
        bail!("failed to connect to any etcd endpoint: {}", errors.join("; "))
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
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

    /// A keeper that renews the lease.
    /// Returns a `LeaseKeeper` with its **own** dedicated connection to avoid
    /// `RefCell` borrow conflicts with concurrent `EtcdClient` calls.
    pub async fn lease_keep_alive(&self, lease_id: i64) -> Result<LeaseKeeper> {
        // Open a dedicated connection for keepalive (same endpoint as current).
        let conn = Conn::open_at(
            self.conn.endpoints.clone(),
            self.conn.current_ep.get(),
            self.conn.dialer.clone(),
        )
        .await?;
        Ok(LeaseKeeper { conn, lease_id })
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
            .call(path, grpc_encode(req), Some(self.request_timeout), resend)
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
}

impl LeaseKeeper {
    /// Send a keep-alive ping and return the response.
    /// Returns the remaining TTL. Returns Err if the lease has expired or etcd is unreachable.
    pub async fn keep_alive(&self) -> Result<LeaseKeepAliveResponse> {
        let req = LeaseKeepAliveRequest { id: self.lease_id };
        let resp = self
            .conn
            .call("/etcdserverpb.Lease/LeaseKeepAlive", grpc_encode(&req), None, true)
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
        let txn = |outcome| resend_outcome("txn", None, false, outcome);
        assert!(uncertain(txn(status(14, "etcdserver: request timed out"))));
        assert!(uncertain(txn(None)));
        assert!(!uncertain(txn(status(16, "etcdserver: invalid auth token"))));
        assert!(!uncertain(resend_outcome("get", None, true, status(14, "timed out"))));
    }
}
