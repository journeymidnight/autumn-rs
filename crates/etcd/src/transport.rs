//! HTTP/2 transport for gRPC calls to etcd: cleartext (h2c), or TLS with ALPN
//! `h2`.
//!
//! Architecture:
//!   compio::net::TcpStream
//!     → [compio::tls::TlsStream] (when TLS is configured)
//!       → cyper_core::HyperStream (compio→hyper I/O adapter)
//!         → hyper::client::conn::http2 handshake
//!           → SendRequest for gRPC POST calls

use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;

use anyhow::{bail, Result};
use bytes::Bytes;
use compio::tls::TlsConnector;
use cyper_core::{CompioExecutor, HyperStream};
use http::{HeaderValue, Request, Version};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http2;

use crate::proto::{grpc_decode, grpc_encode, AuthenticateRequest, AuthenticateResponse};
use crate::{Credentials, EtcdEndpoint};

const AUTHENTICATE: &str = "/etcdserverpb.Auth/Authenticate";

/// Boxed: the h2 connection holds its stream by value in several layers, and
/// the TLS variant is over a KB.
type Stream = Box<HyperStream<compio::net::TcpStream>>;

/// How every connection of one client is made: TLS or not, and the user it
/// authenticates as.
pub(crate) struct Dialer {
    pub(crate) tls: Option<TlsConnector>,
    pub(crate) credentials: Option<Credentials>,
}

impl Dialer {
    fn scheme(&self) -> &'static str {
        if self.tls.is_some() {
            "https"
        } else {
            "http"
        }
    }

    /// TCP, then TLS when configured. The certificate is checked against the
    /// endpoint's name as written, the connection goes to its address.
    async fn dial(&self, ep: &EtcdEndpoint) -> Result<Stream> {
        let addr = &ep.addr;
        let sock_addr: SocketAddr = addr
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid address '{}': {}", addr, e))?;

        // Each step is named. A bare `?` here reports only the OS error —
        // "Bad file descriptor (os error 9)" with nothing to say whether the
        // socket, the connect or the h2 handshake produced it, which is not
        // enough to act on when the same binary works on one kernel and not
        // another.
        let tcp = compio::net::TcpStream::connect(sock_addr)
            .await
            .map_err(|e| anyhow::anyhow!("tcp connect to {addr}: {e}"))?;
        tcp.set_nodelay(true)
            .map_err(|e| anyhow::anyhow!("set_nodelay on {addr}: {e}"))?;
        match &self.tls {
            None => Ok(Box::new(HyperStream::new_plain(tcp))),
            Some(tls) => {
                let name = &ep.server_name;
                let stream = tls
                    .connect(name, tcp)
                    .await
                    .map_err(|e| anyhow::anyhow!("tls handshake with {addr} as {name:?}: {e}"))?;
                Ok(Box::new(HyperStream::new_tls(stream)))
            }
        }
    }
}

/// A single HTTP/2 connection to etcd for gRPC unary calls.
///
/// Holds an `http2::SendRequest` handle. The handle is `Clone` (cheaply —
/// internally just an mpsc sender + Arc), which is hyper's canonical idiom
/// for multiplexing concurrent requests over one HTTP/2 connection.
///
/// Callers should obtain a [`Call`] via [`GrpcChannel::call`] and invoke
/// [`call_with_sender`] **without** holding a borrow of the channel across
/// `.await`. This is essential for `EtcdClient`, which keeps the channel in a
/// `RefCell` for swap-on-reconnect: holding `RefMut` across an awaited gRPC
/// call would panic the second concurrent caller.
pub struct GrpcChannel {
    sender: http2::SendRequest<Full<Bytes>>,
    /// This connection's auth token. Each connection authenticates for itself:
    /// a simple token's expiry runs on each member separately, so a token
    /// still live where it was issued can already be gone on another.
    token: Rc<RefCell<Option<HeaderValue>>>,
    scheme: &'static str,
}

/// What one request needs from its connection, taken out of the channel's
/// `RefCell` before the await.
pub struct Call {
    sender: http2::SendRequest<Full<Bytes>>,
    token: Rc<RefCell<Option<HeaderValue>>>,
    scheme: &'static str,
}

impl GrpcChannel {
    /// Establish the HTTP/2 connection and, with credentials, authenticate.
    pub(crate) async fn connect(ep: &EtcdEndpoint, dialer: &Dialer) -> Result<Self> {
        let stream = dialer.dial(ep).await?;

        let (sender, conn) = http2::handshake::<
            CompioExecutor,
            Stream,
            Full<Bytes>,
        >(CompioExecutor, stream)
        .await
        .map_err(|e| anyhow::anyhow!("h2 handshake failed: {e}"))?;

        // Spawn the connection driver — it runs in the background, processing
        // HTTP/2 frames. When all senders are dropped, it shuts down.
        compio::runtime::spawn(async move {
            if let Err(e) = conn.await {
                tracing::debug!(error = %e, "etcd h2 connection ended");
            }
        })
        .detach();

        let mut call = Call {
            sender,
            token: Rc::new(RefCell::new(None)),
            scheme: dialer.scheme(),
        };
        if let Some(creds) = &dialer.credentials {
            authenticate(&mut call, creds)
                .await
                .map_err(|e| anyhow::anyhow!("authenticate to etcd {} as {:?}: {e}", ep.addr, creds.user))?;
        }
        tracing::debug!(addr = %ep.addr, tls = dialer.tls.is_some(), "etcd connection established");
        Ok(Self {
            sender: call.sender,
            token: call.token,
            scheme: call.scheme,
        })
    }

    /// Cheap clone of the sender handle and the token slot.
    ///
    /// Use this to drop the [`GrpcChannel`] borrow before `.await`-ing a call,
    /// so concurrent in-flight RPCs on the same client don't panic on
    /// `RefCell::borrow_mut`.
    pub fn call(&self) -> Call {
        Call {
            sender: self.sender.clone(),
            token: self.token.clone(),
            scheme: self.scheme,
        }
    }
}

/// Exchange the user's password for a token and store it in the call's
/// connection, where every later request on that connection reads it.
pub(crate) async fn authenticate(call: &mut Call, creds: &Credentials) -> Result<()> {
    let req = AuthenticateRequest {
        name: creds.user.clone(),
        password: creds.password.clone(),
    };
    let resp = send(&mut call.sender, call.scheme, None, AUTHENTICATE, grpc_encode(&req)).await?;
    let resp: AuthenticateResponse = grpc_decode(&resp)?;
    let token = HeaderValue::from_str(&resp.token)
        .map_err(|e| anyhow::anyhow!("etcd returned an unusable token: {e}"))?;
    *call.token.borrow_mut() = Some(token);
    Ok(())
}

/// A non-OK gRPC status etcd answered with.
#[derive(Debug)]
pub struct GrpcStatus {
    pub path: String,
    pub code: u32,
    pub message: String,
}

impl std::fmt::Display for GrpcStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "etcd gRPC {}: status={} message={}", self.path, self.code, self.message)
    }
}

impl std::error::Error for GrpcStatus {}

/// etcd refused the request's token: expired, or older than the last
/// user/role change. An invalid token is refused before the request reaches
/// raft, an old one at apply before any write, so even a txn may be sent
/// again after authenticating.
pub(crate) fn is_stale_token(e: &anyhow::Error) -> bool {
    const UNAUTHENTICATED: u32 = 16;
    const INVALID_ARGUMENT: u32 = 3;
    e.downcast_ref::<GrpcStatus>().is_some_and(|s| {
        s.code == UNAUTHENTICATED
            || (s.code == INVALID_ARGUMENT && s.message.contains("revision of auth store is old"))
    })
}

/// Send a unary gRPC call on the given call handle and return the response body bytes.
///
/// Free function (not a method on `GrpcChannel`) so callers can clone the
/// handle out of a `RefCell`, drop the borrow, then await — the canonical
/// fix for borrow-across-await on a shared `RefCell<GrpcChannel>`.
pub async fn call_with_sender(call: &mut Call, path: &str, body: Bytes) -> Result<Bytes> {
    let token = call.token.borrow().clone();
    send(&mut call.sender, call.scheme, token, path, body).await
}

async fn send(
    sender: &mut http2::SendRequest<Full<Bytes>>,
    scheme: &str,
    token: Option<HeaderValue>,
    path: &str,
    body: Bytes,
) -> Result<Bytes> {
    let mut req = Request::builder()
        .method("POST")
        .uri(format!("{scheme}://etcd{path}"))
        .version(Version::HTTP_2)
        .header("content-type", "application/grpc")
        .header("te", "trailers");
    if let Some(token) = token {
        req = req.header("token", token);
    }
    let req = req
        .body(Full::new(body))
        .map_err(|e| anyhow::anyhow!("build request: {e}"))?;

    let resp = sender
        .send_request(req)
        .await
        .map_err(|e| anyhow::anyhow!("send request to {path}: {e}"))?;

    let status = resp.status();
    if !status.is_success() {
        bail!("etcd gRPC {path}: HTTP {status}");
    }

    let (parts, body) = resp.into_parts();
    let collected = body
        .collect()
        .await
        .map_err(|e| anyhow::anyhow!("read response body for {path}: {e}"))?;
    check_grpc_status(path, &parts.headers)?;
    if let Some(trailers) = collected.trailers() {
        check_grpc_status(path, trailers)?;
    }
    Ok(collected.to_bytes())
}

/// The gRPC status sits in the headers of a "trailers-only" response, else
/// in the trailers.
fn check_grpc_status(path: &str, headers: &http::HeaderMap) -> Result<()> {
    if let Some(grpc_status) = headers.get("grpc-status") {
        let code = grpc_status.to_str().unwrap_or("?");
        if code != "0" {
            let message = headers
                .get("grpc-message")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("unknown");
            return Err(GrpcStatus {
                path: path.to_string(),
                code: code.parse().unwrap_or(u32::MAX),
                message: message.to_string(),
            }
            .into());
        }
    }
    Ok(())
}
