//! RPC client with request multiplexing over a single TCP connection.
//!
//! One `RpcClient` per remote address. Multiple concurrent requests are
//! multiplexed via `req_id` and, as of R4 (step 4.1), an **SQ/CQ pipeline**:
//!
//! - **SQ (submit queue)**: callers push encoded frames into a bounded mpsc;
//!   a single background `writer_task` owns the `WriteHalf` and drains the
//!   queue sequentially (no write-side mutex). Back-pressure is provided by
//!   the bounded channel.
//! - **CQ (completion queue)**: a background `read_loop` task owns the
//!   `ReadHalf`, decodes response frames and routes each to the matching
//!   `oneshot::Sender<Frame>` in the `pending` inflight map.
//!
//! Callers never block on the wire. They insert their oneshot sender into
//! `pending`, push a `SubmitMsg` into the submit channel (may await when the
//! channel is full), and then await their own receiver. This decouples
//! submission order from completion order: whichever response CQE arrives
//! first wakes its specific caller, independent of which caller submitted
//! first.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use compio::BufResult;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::runtime::spawn;
use futures::channel::{mpsc, oneshot};
use futures::{SinkExt, StreamExt};

use crate::error::RpcError;
use crate::frame::{Frame, FrameDecoder};

type WriteHalf = autumn_transport::WriteHalf;
type ReadHalf = autumn_transport::ReadHalf;

/// Capacity of the submit mpsc channel between callers and the writer task.
///
/// Bounded so that callers back-pressure naturally under overload — the
/// `submit_tx.send().await` will park until the writer_task drains one slot.
const SUBMIT_CHANNEL_CAP: usize = 1024;

/// Submission message pushed onto the writer_task's queue.
///
/// The caller has already (a) assigned `req_id`, (b) inserted its oneshot
/// sender into `pending`, and (c) encoded the frame bytes (or prepared the
/// vectored bufs). The writer_task simply writes to the socket.
enum SubmitMsg {
    /// A single-buffer frame (used by `call()`, `send_oneshot()`, etc.).
    Single { bytes: Bytes, req_id: u32 },
    /// A vectored frame `[header][part0][part1]...` (used by `call_vectored`,
    /// `send_vectored`). Zero-copy for the payload parts.
    Vectored { bufs: Vec<Bytes>, req_id: u32 },
}

impl SubmitMsg {
    fn req_id(&self) -> u32 {
        match self {
            SubmitMsg::Single { req_id, .. } => *req_id,
            SubmitMsg::Vectored { req_id, .. } => *req_id,
        }
    }
}

/// A multiplexed RPC client over a single TCP connection.
///
/// Write path: callers push `SubmitMsg` into `submit_tx`; the `writer_task`
/// drains and writes sequentially — no cross-caller mutex contention.
/// Read path: the `read_loop` task decodes response frames and dispatches
/// them via `pending`.
///
/// All fields are !Send (single-threaded, compio thread-per-core model).
/// `pending` uses `RefCell` with scoped borrows — never held across await.
pub struct RpcClient {
    /// SQ: submit channel to writer_task. `Sender::send` requires `&mut self`
    /// so callers `clone()` before sending (cheap, `Sender` is `Arc`-backed).
    submit_tx: RefCell<mpsc::Sender<SubmitMsg>>,
    /// CQ-side inflight map: `req_id -> oneshot::Sender<Frame>`.
    /// Borrowed only briefly (insert/remove/get), never across await.
    pending: Rc<RefCell<HashMap<u32, oneshot::Sender<Frame>>>>,
    /// Monotonic request id. Single-threaded, no await crossing.
    /// Value 0 is reserved for fire-and-forget (no response expected).
    next_id: Cell<u32>,
    peer_addr: SocketAddr,
}

impl RpcClient {
    /// Connect to a remote address through the process-global transport
    /// (`autumn_transport::current()`), then start the background reader +
    /// writer. Honours `AUTUMN_TRANSPORT={tcp,ucx,auto}` once Phase 4 wires
    /// the env switch.
    pub async fn connect(addr: SocketAddr) -> Result<Rc<Self>, RpcError> {
        let conn = autumn_transport::current_or_init().connect(addr).await?;
        // TCP_NODELAY only applies to the TCP variant; UCX manages framing
        // itself and exposes no equivalent knob.
        if let Some(s) = conn.as_tcp() {
            s.set_nodelay(true)?;
        }
        Self::from_conn(conn, addr)
    }

    /// Build an RpcClient from an already-connected `autumn_transport::Conn`.
    ///
    /// Spawns two background tasks on the current compio runtime:
    /// - `writer_task`: owns the write half, drains submit_rx, writes frames.
    /// - `read_loop`: owns the read half, decodes frames, dispatches via pending.
    ///
    /// Both tasks terminate on socket close / write error. When either exits,
    /// `pending` is cleared so callers' receivers see `RecvError` and surface
    /// `RpcError::ConnectionClosed`.
    pub fn from_conn(
        conn: autumn_transport::Conn,
        peer_addr: SocketAddr,
    ) -> Result<Rc<Self>, RpcError> {
        let (reader, writer) = conn.into_split();
        let pending: Rc<RefCell<HashMap<u32, oneshot::Sender<Frame>>>> =
            Rc::new(RefCell::new(HashMap::new()));

        let (submit_tx, submit_rx) = mpsc::channel::<SubmitMsg>(SUBMIT_CHANNEL_CAP);

        let client = Rc::new(Self {
            submit_tx: RefCell::new(submit_tx),
            pending: pending.clone(),
            next_id: Cell::new(1),
            peer_addr,
        });

        // SQ: writer_task drains submit_rx and writes to the socket.
        // On write error, it removes the offending req_id from `pending`
        // (caller's receiver will then see RecvError) and exits. Remaining
        // in-flight requests will be cleaned up when read_loop sees EOF
        // (socket close) and clears `pending`.
        let pending_for_writer = pending.clone();
        spawn(async move {
            writer_task(writer, submit_rx, pending_for_writer, peer_addr).await;
        })
        .detach();

        // CQ: read_loop decodes response frames and dispatches via pending.
        let pending_for_reader = pending;
        spawn(async move {
            if let Err(e) = read_loop(reader, pending_for_reader.clone(), peer_addr).await {
                tracing::warn!(addr = %peer_addr, error = %e, "rpc client reader exited");
            }
            // On disconnect, drop all pending senders so callers get RecvError.
            pending_for_reader.borrow_mut().clear();
        })
        .detach();

        Ok(client)
    }

    /// Send a request and wait for the response.
    pub async fn call(&self, msg_type: u8, payload: Bytes) -> Result<Bytes, RpcError> {
        let req_id = self.next_req_id();
        let frame = Frame::request(req_id, msg_type, payload);
        let rx = self.send_frame(frame).await?;

        let resp = rx.await.map_err(|_| RpcError::ConnectionClosed)?;
        if resp.is_error() {
            let (code, message) = RpcError::decode_status(&resp.payload);
            return Err(RpcError::status(code, message));
        }
        Ok(resp.payload)
    }

    /// Send a request frame and return the oneshot receiver for the response.
    ///
    /// On return, the frame has been queued for the writer_task (or is waiting
    /// for a slot when the submit channel is full — natural back-pressure).
    /// The caller awaits the receiver to get the response frame.
    pub async fn send_frame(
        &self,
        frame: Frame,
    ) -> Result<oneshot::Receiver<Frame>, RpcError> {
        let req_id = frame.req_id;
        let (tx, rx) = oneshot::channel();

        // Borrow scoped: insert → drop. Ordering: pending-insert BEFORE submit
        // so that when reader_task sees the response it can dispatch
        // immediately (no race window where response lands first).
        self.pending.borrow_mut().insert(req_id, tx);

        let bytes = frame.encode();
        if let Err(e) = self.submit(SubmitMsg::Single { bytes, req_id }).await {
            // submit failed (writer_task exited / channel closed) — remove
            // the pending entry so we don't leak it.
            self.pending.borrow_mut().remove(&req_id);
            return Err(e);
        }

        Ok(rx)
    }

    /// Send a request whose payload is already split into parts.
    /// Uses vectored write: [frame_header][part0][part1]... — zero payload copy.
    pub async fn call_vectored(
        &self,
        msg_type: u8,
        payload_parts: Vec<Bytes>,
    ) -> Result<Bytes, RpcError> {
        let rx = self.send_vectored(msg_type, payload_parts).await?;
        let resp = rx.await.map_err(|_| RpcError::ConnectionClosed)?;
        if resp.is_error() {
            let (code, message) = RpcError::decode_status(&resp.payload);
            return Err(RpcError::status(code, message));
        }
        Ok(resp.payload)
    }

    /// Send a request and wait for the response with a timeout.
    pub async fn call_timeout(
        &self,
        msg_type: u8,
        payload: Bytes,
        timeout: Duration,
    ) -> Result<Bytes, RpcError> {
        let call_fut = self.call(msg_type, payload);
        let timer_fut = compio::time::sleep(timeout);
        futures::pin_mut!(call_fut, timer_fut);
        match futures::future::select(call_fut, timer_fut).await {
            futures::future::Either::Left((result, _)) => result,
            futures::future::Either::Right(_) => Err(RpcError::Status {
                code: crate::error::StatusCode::Unavailable,
                message: format!("RPC timed out after {:?}", timeout),
            }),
        }
    }

    /// Send a vectored request and return the receiver for the response,
    /// without awaiting. Enables pipelined submit + parallel await patterns.
    ///
    /// The oneshot receiver is inserted into `pending` before the submit, so
    /// the background reader can dispatch the response as soon as it arrives
    /// (no lost-wakeup race).
    pub async fn send_vectored(
        &self,
        msg_type: u8,
        payload_parts: Vec<Bytes>,
    ) -> Result<oneshot::Receiver<Frame>, RpcError> {
        let req_id = self.next_req_id();
        let payload_len: usize = payload_parts.iter().map(|p| p.len()).sum();
        let hdr = Frame::encode_request_header(req_id, msg_type, payload_len as u32);

        let (tx, rx) = oneshot::channel();
        // Insert BEFORE submit — see comment in send_frame.
        self.pending.borrow_mut().insert(req_id, tx);

        let mut bufs: Vec<Bytes> = Vec::with_capacity(1 + payload_parts.len());
        bufs.push(Bytes::copy_from_slice(&hdr));
        bufs.extend(payload_parts);

        if let Err(e) = self.submit(SubmitMsg::Vectored { bufs, req_id }).await {
            self.pending.borrow_mut().remove(&req_id);
            return Err(e);
        }

        Ok(rx)
    }

    /// Send a vectored request with a timeout.
    pub async fn call_vectored_timeout(
        &self,
        msg_type: u8,
        payload_parts: Vec<Bytes>,
        timeout: Duration,
    ) -> Result<Bytes, RpcError> {
        let call_fut = self.call_vectored(msg_type, payload_parts);
        let timer_fut = compio::time::sleep(timeout);
        futures::pin_mut!(call_fut, timer_fut);
        match futures::future::select(call_fut, timer_fut).await {
            futures::future::Either::Left((result, _)) => result,
            futures::future::Either::Right(_) => Err(RpcError::Status {
                code: crate::error::StatusCode::Unavailable,
                message: format!("RPC timed out after {:?}", timeout),
            }),
        }
    }

    /// Send a fire-and-forget frame (no response expected).
    ///
    /// `req_id = 0` tells the remote side not to send a response frame.
    /// Returns Ok once the frame has been queued for the writer_task
    /// (under back-pressure from the bounded submit channel).
    pub async fn send_oneshot(&self, msg_type: u8, payload: Bytes) -> Result<(), RpcError> {
        let req_id = 0; // req_id 0 = no response expected
        let frame = Frame::request(req_id, msg_type, payload);
        let bytes = frame.encode();
        self.submit(SubmitMsg::Single { bytes, req_id }).await
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Number of in-flight requests awaiting response.
    pub fn pending_count(&self) -> usize {
        self.pending.borrow().len()
    }

    /// Assign the next request id. Request id 0 is reserved for fire-and-forget
    /// so we skip it on wraparound.
    fn next_req_id(&self) -> u32 {
        let mut id = self.next_id.get();
        if id == 0 {
            id = 1;
        }
        self.next_id.set(id.wrapping_add(1));
        id
    }

    /// Push a SubmitMsg onto the writer_task's queue.
    ///
    /// Critical: we clone the `Sender` instead of borrowing `submit_tx` across
    /// the `.await`. Borrowing a `RefCell` across await can panic if the same
    /// thread re-enters (e.g., another spawned task calls another RpcClient
    /// method on the same runtime). Cloning the Sender is cheap (`Arc`-backed).
    async fn submit(&self, msg: SubmitMsg) -> Result<(), RpcError> {
        // Scoped borrow: clone → drop guard immediately.
        let mut tx = self.submit_tx.borrow().clone();
        tx.send(msg).await.map_err(|_| RpcError::ConnectionClosed)
    }
}

/// SQ task: owns WriteHalf, drains the submit queue, writes to the socket.
///
/// Sequential writes preserve per-caller submit order on the wire. If a
/// write fails (TCP closed, peer reset, etc.) the task removes the failing
/// req_id from `pending` (so that caller's receiver surfaces an error) and
/// exits — subsequent receivers will fail once read_loop's EOF clears the
/// rest of `pending`.
async fn writer_task(
    mut writer: WriteHalf,
    mut submit_rx: mpsc::Receiver<SubmitMsg>,
    pending: Rc<RefCell<HashMap<u32, oneshot::Sender<Frame>>>>,
    peer_addr: SocketAddr,
) {
    while let Some(msg) = submit_rx.next().await {
        let req_id = msg.req_id();
        // F099-I-fix instrumentation: capture iov count + total bytes
        // before the syscall so the EINVAL path can attribute the error
        // to the exact shape of the Vectored message. Negligible cost
        // (2 integer ops per msg; the logging formatter only runs on the
        // rare error branch).
        let (iov_count, total_bytes) = match &msg {
            SubmitMsg::Single { bytes, .. } => (1usize, bytes.len()),
            SubmitMsg::Vectored { bufs, .. } => {
                let total: usize = bufs.iter().map(|b| b.len()).sum();
                (bufs.len(), total)
            }
        };
        let result = match msg {
            SubmitMsg::Single { bytes, .. } => {
                let BufResult(r, _) = writer.write_all(bytes).await;
                r
            }
            SubmitMsg::Vectored { bufs, .. } => {
                let BufResult(r, _) = writer.write_vectored_all(bufs).await;
                r
            }
        };

        if let Err(e) = result {
            // F099-I-fix (CAP-EINVAL): ALWAYS log the write error at
            // WARN so production runs surface the root-cause signature
            // (iov_count, total_bytes, errno.raw_os_error()) rather than
            // just a downstream "submit error: connection closed" cascade.
            // The original F099-I concern speculated about `IOV_MAX`
            // exhaustion; in practice the shape logged here lets us
            // confirm or reject that hypothesis from a single bench run.
            tracing::warn!(
                addr = %peer_addr,
                req_id,
                iov_count,
                total_bytes,
                errno = ?e.raw_os_error(),
                kind = ?e.kind(),
                error = %e,
                "rpc client writer exited on write error (F099-I-fix instrumentation)"
            );
            // Remove this request's pending entry so the caller surfaces
            // ConnectionClosed immediately (req_id 0 never had one).
            if req_id != 0 {
                pending.borrow_mut().remove(&req_id);
            }
            return;
        }
    }

    // submit_rx closed (all Senders dropped / RpcClient dropped). Exit cleanly.
    tracing::debug!(addr = %peer_addr, "rpc client writer_task exiting (channel closed)");
}

/// CQ task: owns ReadHalf, decodes response frames, dispatches via pending.
async fn read_loop(
    mut reader: ReadHalf,
    pending: Rc<RefCell<HashMap<u32, oneshot::Sender<Frame>>>>,
    addr: SocketAddr,
) -> Result<(), RpcError> {
    let mut decoder = FrameDecoder::new();
    let mut buf = vec![0u8; 64 * 1024];

    loop {
        let BufResult(result, buf_back) = reader.read(buf).await;
        buf = buf_back;
        let n = result?;
        if n == 0 {
            tracing::debug!(addr = %addr, "rpc connection closed by peer");
            return Ok(());
        }

        decoder.feed(&buf[..n]);

        // No await crossing: borrow_mut → remove → drop within each iteration
        loop {
            match decoder.try_decode()? {
                Some(frame) => {
                    if let Some(tx) = pending.borrow_mut().remove(&frame.req_id) {
                        let _ = tx.send(frame);
                    } else {
                        tracing::trace!(
                            req_id = frame.req_id,
                            msg_type = frame.msg_type,
                            "response for unknown req_id, dropped"
                        );
                    }
                }
                None => break,
            }
        }
    }
}
