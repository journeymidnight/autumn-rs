//! RPC server: accepts TCP connections and dispatches requests to handlers.
//!
//! Uses compio's `Dispatcher` for thread-per-core connection handling.
//! A dedicated accept thread runs `std::net::TcpListener::accept()` in a loop
//! and sends accepted `std::net::TcpStream` (Send-safe) to compio worker
//! threads via `Dispatcher::dispatch`. Workers convert to `compio::net::TcpStream`
//! (which is !Send, thread-local).

use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

use bytes::Bytes;
use compio::dispatcher::Dispatcher;
use compio::net::TcpStream;
use compio::BufResult;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::runtime::spawn;
use futures::StreamExt;
use tracing;

use crate::error::{RpcError, StatusCode};
use crate::frame::{Frame, FrameDecoder};

/// The result type returned by RPC handlers.
pub type HandlerResult = Result<Bytes, (StatusCode, String)>;

/// Async handler function type.
/// Takes (msg_type, request_payload) and returns response payload or error.
pub type BoxHandler = Arc<
    dyn Fn(u8, Bytes) -> Pin<Box<dyn Future<Output = HandlerResult>>> + Send + Sync,
>;

/// RPC server configuration and state.
pub struct RpcServer {
    handler: BoxHandler,
    worker_threads: Option<NonZeroUsize>,
}

impl RpcServer {
    /// Create a new server with the given handler.
    ///
    /// The handler receives `(msg_type, payload)` for each request and returns
    /// either `Ok(response_payload)` or `Err((status_code, message))`.
    pub fn new<F, Fut>(handler: F) -> Self
    where
        F: Fn(u8, Bytes) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HandlerResult> + 'static,
    {
        Self {
            handler: Arc::new(move |msg_type, payload| {
                Box::pin(handler(msg_type, payload))
            }),
            worker_threads: None,
        }
    }

    /// Set the number of worker threads (defaults to CPU count).
    pub fn worker_threads(mut self, n: NonZeroUsize) -> Self {
        self.worker_threads = Some(n);
        self
    }

    /// Start serving on the given address. Runs until the process exits.
    ///
    /// Branches on the process-global transport (set by
    /// `autumn_transport::init`):
    ///
    /// - **TCP**: dedicated OS thread runs `std::net::TcpListener::accept`,
    ///   accepted streams are dispatched across compio worker threads via
    ///   `Dispatcher` (multi-core scaling, the original autumn-rpc design).
    ///
    /// - **UCX**: accept loop runs on the calling compio runtime (UCX EPs
    ///   are thread-bound to their worker; they cannot be Send-shipped to
    ///   another runtime). Each connection is `compio::runtime::spawn`'d
    ///   on the same runtime as a separate task. Single-thread server —
    ///   sufficient for path 2 functional correctness; multi-core UCX
    ///   server scaling needs per-worker listeners + manager-side
    ///   discovery, which is a separate ticket.
    pub async fn serve(self, addr: SocketAddr) -> Result<(), RpcError> {
        let transport = autumn_transport::current_or_init();
        match transport.kind() {
            autumn_transport::TransportKind::Tcp => self.serve_tcp(addr).await,
            autumn_transport::TransportKind::Ucx => self.serve_ucx(addr).await,
        }
    }

    /// UCX-mode serve: accept on the calling compio runtime, spawn each
    /// conn handler as a task on the same runtime. No Dispatcher.
    async fn serve_ucx(self, addr: SocketAddr) -> Result<(), RpcError> {
        let transport = autumn_transport::current_or_init();
        let mut listener = transport.bind(addr).await?;
        let local_addr = listener.local_addr()?;
        tracing::info!(addr = %local_addr, "rpc server (UCX) listening");

        let handler = self.handler;
        loop {
            let (conn, peer_addr) = listener.accept().await?;
            let handler = handler.clone();
            spawn(async move {
                tracing::debug!(peer = %peer_addr, "new ucx rpc connection");
                if let Err(e) = handle_conn_generic(conn, handler).await {
                    tracing::debug!(peer = %peer_addr, error = %e, "ucx rpc connection ended");
                }
            })
            .detach();
        }
    }

    async fn serve_tcp(self, addr: SocketAddr) -> Result<(), RpcError> {
        let std_listener = std::net::TcpListener::bind(addr)?;
        let local_addr = std_listener.local_addr()?;
        tracing::info!(addr = %local_addr, "rpc server listening");

        let dispatcher = {
            let mut builder = Dispatcher::builder();
            if let Some(n) = self.worker_threads {
                builder = builder.worker_threads(n);
            }
            builder
                .thread_names(|i| format!("autumn-rpc-worker-{i}"))
                .build()
                .map_err(RpcError::Io)?
        };

        let handler = self.handler;

        // Use futures channel to receive accepted connections from a blocking thread.
        let (tx, mut rx) = futures::channel::mpsc::channel::<(std::net::TcpStream, SocketAddr)>(256);

        // Spawn a blocking accept thread.
        std::thread::Builder::new()
            .name("autumn-rpc-accept".to_string())
            .spawn(move || {
                let mut tx = tx;
                loop {
                    match std_listener.accept() {
                        Ok((stream, peer_addr)) => {
                            if let Err(e) = stream.set_nonblocking(true) {
                                tracing::warn!(peer = %peer_addr, error = %e, "set_nonblocking failed");
                                continue;
                            }
                            // try_send: if channel full (256 pending), drop connection
                            if tx.try_send((stream, peer_addr)).is_err() {
                                tracing::warn!("accept channel full, dropping connection");
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "accept failed");
                        }
                    }
                }
            })
            .map_err(|e| RpcError::Io(std::io::Error::other(format!("spawn accept thread: {e}"))))?;

        // Dispatch accepted connections to compio workers.
        loop {
            let (std_stream, peer_addr) = match rx.next().await {
                Some(v) => v,
                None => return Ok(()), // accept thread exited
            };

            let handler = handler.clone();
            let dispatch_result = dispatcher.dispatch(move || async move {
                let stream = match TcpStream::from_std(std_stream) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(peer = %peer_addr, error = %e, "from_std failed");
                        return;
                    }
                };
                if let Err(e) = stream.set_nodelay(true) {
                    tracing::warn!(peer = %peer_addr, error = %e, "set_nodelay failed");
                }

                tracing::debug!(peer = %peer_addr, "new rpc connection");
                if let Err(e) = handle_connection(stream, handler).await {
                    tracing::debug!(peer = %peer_addr, error = %e, "rpc connection ended");
                }
            });

            if let Err(_) = dispatch_result {
                tracing::error!("all rpc worker threads panicked");
                return Err(RpcError::Io(std::io::Error::other(
                    "all rpc worker threads panicked",
                )));
            }
        }
    }
}

/// Handle a single connection over an `autumn_transport::Conn`. Identical
/// frame loop to the TCP-specific `handle_connection`; we just take the
/// trait-enum so it works for both Tcp and Ucx variants.
async fn handle_conn_generic(
    conn: autumn_transport::Conn,
    handler: BoxHandler,
) -> Result<(), RpcError> {
    let (mut reader, writer) = conn.into_split();
    let writer = Rc::new(futures::lock::Mutex::new(writer));
    let mut decoder = FrameDecoder::new();
    let mut buf = vec![0u8; 64 * 1024];

    loop {
        let BufResult(result, buf_back) = reader.read(buf).await;
        buf = buf_back;
        let n = result?;
        if n == 0 {
            return Ok(());
        }
        decoder.feed(&buf[..n]);
        loop {
            match decoder.try_decode()? {
                Some(frame) => {
                    if frame.req_id == 0 {
                        continue;
                    }
                    let handler = handler.clone();
                    let writer = writer.clone();
                    let req_id = frame.req_id;
                    let msg_type = frame.msg_type;
                    spawn(async move {
                        let resp_frame = match handler(msg_type, frame.payload).await {
                            Ok(payload) => Frame::response(req_id, msg_type, payload),
                            Err((code, message)) => {
                                let payload = RpcError::encode_status(code, &message);
                                Frame::error(req_id, msg_type, payload)
                            }
                        };
                        let data = resp_frame.encode();
                        let mut w = writer.lock().await;
                        let BufResult(result, _) = w.write_all(data).await;
                        if let Err(e) = result {
                            tracing::debug!(req_id, error = %e, "failed to write response");
                        }
                    })
                    .detach();
                }
                None => break,
            }
        }
    }
}

/// Handle a single TCP connection: read frames, dispatch to handler, write responses.
async fn handle_connection(
    stream: TcpStream,
    handler: BoxHandler,
) -> Result<(), RpcError> {
    let (mut reader, writer) = stream.into_split();
    // Must be async Mutex: multiple spawned handler tasks write responses
    // concurrently, and write_all().await crosses the await point.
    let writer = Rc::new(futures::lock::Mutex::new(writer));
    let mut decoder = FrameDecoder::new();
    let mut buf = vec![0u8; 64 * 1024];

    loop {
        let BufResult(result, buf_back) = reader.read(buf).await;
        buf = buf_back;
        let n = result?;
        if n == 0 {
            return Ok(());
        }

        decoder.feed(&buf[..n]);

        loop {
            match decoder.try_decode()? {
                Some(frame) => {
                    if frame.req_id == 0 {
                        continue;
                    }

                    let handler = handler.clone();
                    let writer = writer.clone();
                    let req_id = frame.req_id;
                    let msg_type = frame.msg_type;

                    spawn(async move {
                        let resp_frame = match handler(msg_type, frame.payload).await {
                            Ok(payload) => Frame::response(req_id, msg_type, payload),
                            Err((code, message)) => {
                                let payload = RpcError::encode_status(code, &message);
                                Frame::error(req_id, msg_type, payload)
                            }
                        };

                        let data = resp_frame.encode();
                        let mut w = writer.lock().await;
                        let BufResult(result, _) = w.write_all(data).await;
                        if let Err(e) = result {
                            tracing::debug!(req_id, error = %e, "failed to write response");
                        }
                    })
                    .detach();
                }
                None => break,
            }
        }
    }
}
