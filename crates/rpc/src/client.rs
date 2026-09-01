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
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::runtime::spawn;
use compio::BufResult;
use futures::channel::{mpsc, oneshot};
use futures::{SinkExt, StreamExt};

use crate::error::RpcError;
use crate::frame::{Frame, FrameDecoder};

// ── zero-copy GET (recv-into-pooled) ─────────────────────────────────────────
//
// `call_into_pooled` recvs a value-response's raw value tail straight into a
// read_loop-owned RegPool `PooledBuf` (registered, for UCX) instead of a fresh
// `Vec`. Wire v28 uses ONE frame shape for everything:
//
//   [header 10][ctrl_len 4][ctrl…][crc32c 4][value…]
//
// For the bulk read response ctrl = `[code:1][message…]`; the crc covers
// header+ctrl (so a flipped code/req_id fails loud) while the value tail is
// raw — its integrity is the transport's (UCX NIC ICRC / TCP kernel segment
// checksum; the per-value crc was measured at ~20% of a core @ 8 MiB).
//
// The recv-into-CALLER-dest sibling (`call_into_dest`, raw `*mut u8` + optional
// explicit `RegisteredMem`) was REMOVED: its cancel-safety contract (dest must
// outlive the call, no per-call timeout ever) made it the one SDK RPC that
// could hang unboundedly, its `reg` was always `None` in production (implicit
// rcache registration), and the default-on EN-direct read path had already
// switched to pooled-recv-plus-memcpy deliberately. Callers needing the value
// at a specific address copy out of the returned `PooledBuf` (one memcpy).

/// Minimum value size at which the **TCP** `call_into_pooled` read path recvs
/// the value straight into a `PooledBuf` (compio owned read) instead of
/// letting the `FrameDecoder` accumulate it then copying into the pool buffer.
/// Below this the FrameDecoder path is cheaper (it batch-decodes multiple small
/// frames per socket read and a small value's whole payload is usually already
/// buffered, so recv-into-pooled would only add a pool acquire + an extra
/// syscall). Mirrors the UCX read threshold (`BULK_MIN_BYTES`); TCP
/// can't be true zero-copy (the kernel copy is unavoidable) but this removes the
/// extra app-level copy for large values.
pub const TCP_RECV_INTO_POOLED_MIN_BYTES: usize = 64 * 1024;

/// Outcome of a `call_into_pooled`: the raw value (possibly empty) in a
/// read_loop-owned pool buffer, plus the bulk status ctrl — application `code`
/// and a human-readable `message` (usually empty on success), both
/// CRC-protected on the wire.
pub struct BulkResp {
    pub buf: autumn_transport::PooledBuf,
    pub code: u8,
    pub message: String,
}

impl std::fmt::Debug for BulkResp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BulkResp")
            .field("code", &self.code)
            .field("message", &self.message)
            .field("value_len", &self.buf.len())
            .finish()
    }
}

/// CQ-side inflight entry. A normal request awaits a `Frame`; a bulk request
/// gets its value recv'd into a read_loop-owned `PooledBuf`.
enum Pending {
    Frame(oneshot::Sender<Frame>),
    /// `call_into_pooled`: the READ_LOOP owns the dest buffer — it acquires a
    /// `PooledBuf` at recv time, recvs the value into it, and hands the filled
    /// buffer back here. On caller-cancel the receiver is gone → the read_loop
    /// drops the `PooledBuf` → returns to pool (no leak; cancel-safe, since the
    /// cancellable caller never owns the in-flight buffer).
    IntoPooled(oneshot::Sender<Result<BulkResp, RpcError>>),
}

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
    pending: Rc<RefCell<HashMap<u32, Pending>>>,
    /// Monotonic request id. Single-threaded, no await crossing.
    /// Value 0 is reserved for fire-and-forget (no response expected).
    next_id: Cell<u32>,
    peer_addr: SocketAddr,
    /// Set true when either `read_loop` or `writer_task` exits. After
    /// that point new submits MUST short-circuit with `ConnectionClosed`
    /// — otherwise the caller's pending entry would never be dispatched
    /// (no read_loop alive to deliver the response). Without this flag,
    /// a stale pooled client whose peer has died blocks callers forever.
    closed: Rc<Cell<bool>>,
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
        let pending: Rc<RefCell<HashMap<u32, Pending>>> = Rc::new(RefCell::new(HashMap::new()));

        let (submit_tx, submit_rx) = mpsc::channel::<SubmitMsg>(SUBMIT_CHANNEL_CAP);
        let closed: Rc<Cell<bool>> = Rc::new(Cell::new(false));

        let client = Rc::new(Self {
            submit_tx: RefCell::new(submit_tx),
            pending: pending.clone(),
            next_id: Cell::new(1),
            peer_addr,
            closed: closed.clone(),
        });

        // SQ: writer_task drains submit_rx and writes to the socket.
        // On exit (write error or channel-close) we set `closed` BEFORE
        // clearing `pending` so any caller racing a fresh `send_*` checks
        // the flag and short-circuits with `ConnectionClosed`. Without
        // `closed`, a stale `Rc<RpcClient>` left in a pool would let new
        // submits insert pending entries that nobody dispatches — the
        // caller's `rx.await` then hangs forever (the original hang's root cause).
        let pending_for_writer = pending.clone();
        let closed_for_writer = closed.clone();
        spawn(async move {
            writer_task(writer, submit_rx, pending_for_writer.clone(), peer_addr).await;
            closed_for_writer.set(true);
            pending_for_writer.borrow_mut().clear();
        })
        .detach();

        // CQ: read_loop decodes response frames and dispatches via pending.
        let pending_for_reader = pending;
        let closed_for_reader = closed;
        spawn(async move {
            if let Err(e) = read_loop(reader, pending_for_reader.clone(), peer_addr).await {
                tracing::warn!(addr = %peer_addr, error = %e, "rpc client reader exited");
            }
            // set closed BEFORE clearing pending so subsequent
            // `send_*` short-circuits and never inserts a fresh pending
            // entry that has no read_loop alive to dispatch it.
            closed_for_reader.set(true);
            pending_for_reader.borrow_mut().clear();
        })
        .detach();

        Ok(client)
    }

    /// True when either `read_loop` or `writer_task` has exited.
    /// Pools should evict the entry; new `send_*` calls return
    /// `ConnectionClosed` without inserting into `pending`.
    pub fn is_closed(&self) -> bool {
        self.closed.get()
    }

    /// Send a request and wait for the response.
    pub async fn call(&self, msg_type: u8, payload: Bytes) -> Result<Bytes, RpcError> {
        let req_id = self.next_req_id();
        let frame = Frame::request(req_id, msg_type, payload);
        let rx = self.send_frame(frame).await?;
        Self::await_response(rx).await
    }

    /// Await a queued request's response frame and unwrap the error envelope.
    async fn await_response(rx: oneshot::Receiver<Frame>) -> Result<Bytes, RpcError> {
        let resp = rx.await.map_err(|_| RpcError::ConnectionClosed)?;
        if resp.is_error() {
            let (code, message) = RpcError::decode_status(&resp.payload);
            return Err(RpcError::status(code, message));
        }
        Ok(resp.payload)
    }

    /// cancel-safe zero-copy read: send a request whose value response
    /// the READ_LOOP recvs straight into a freshly-acquired `PooledBuf` (which
    /// the read_loop owns), then hands back here. The caller never owns the
    /// in-flight buffer — so a cancelled/timed-out caller can NOT leave the
    /// NIC writing a freed/recycled buffer, and the buffer is always reclaimed
    /// (handed back on success, dropped→pool on cancel — never leaked).
    /// Returns a [`BulkResp`] (filled `PooledBuf` + status code + message). On
    /// UCX the value RDMAs into the registered pool buffer (memh zero-copy);
    /// on TCP the value is copied off the wire into a (plain) pool buffer.
    pub async fn call_into_pooled(
        &self,
        msg_type: u8,
        payload: Bytes,
    ) -> Result<BulkResp, RpcError> {
        if self.closed.get() {
            return Err(RpcError::ConnectionClosed);
        }
        let req_id = self.next_req_id();
        let (meta_tx, meta_rx) = oneshot::channel();
        let bytes = Frame::request(req_id, msg_type, payload).encode();
        self.register_and_submit(req_id, Pending::IntoPooled(meta_tx), bytes)
            .await?;
        meta_rx.await.map_err(|_| RpcError::ConnectionClosed)?
    }

    /// Send a request frame and return the oneshot receiver for the response.
    ///
    /// On return, the frame has been queued for the writer_task (or is waiting
    /// for a slot when the submit channel is full — natural back-pressure).
    /// The caller awaits the receiver to get the response frame.
    pub async fn send_frame(&self, frame: Frame) -> Result<oneshot::Receiver<Frame>, RpcError> {
        // short-circuit if the reader/writer task has already
        // exited. The check + pending.insert below run in one sync block
        // (single-threaded compio, no awaits), so a concurrent close that
        // races us either flips `closed` first → we return here, or runs
        // after we insert → its `pending.clear()` cancels our `tx`.
        if self.closed.get() {
            return Err(RpcError::ConnectionClosed);
        }
        let req_id = frame.req_id;
        let (tx, rx) = oneshot::channel();
        let bytes = frame.encode();
        self.register_and_submit(req_id, Pending::Frame(tx), bytes)
            .await?;
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
        Self::await_response(rx).await
    }

    /// Race `fut` against a timer; a timeout surfaces as `Unavailable`.
    async fn with_timeout<T>(
        fut: impl std::future::Future<Output = Result<T, RpcError>>,
        timeout: Duration,
    ) -> Result<T, RpcError> {
        let timer_fut = compio::time::sleep(timeout);
        futures::pin_mut!(fut, timer_fut);
        match futures::future::select(fut, timer_fut).await {
            futures::future::Either::Left((result, _)) => result,
            futures::future::Either::Right(_) => Err(RpcError::Status {
                code: crate::error::StatusCode::Unavailable,
                message: format!("RPC timed out after {:?}", timeout),
            }),
        }
    }

    /// Send a request and wait for the response with a timeout.
    pub async fn call_timeout(
        &self,
        msg_type: u8,
        payload: Bytes,
        timeout: Duration,
    ) -> Result<Bytes, RpcError> {
        Self::with_timeout(self.call(msg_type, payload), timeout).await
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
        // see send_frame for the rationale.
        if self.closed.get() {
            return Err(RpcError::ConnectionClosed);
        }
        let req_id = self.next_req_id();
        let ctrl_len: usize = payload_parts.iter().map(|p| p.len()).sum();
        // Vectored ctrl-only frame: [head(hdr+ctrl_len)][parts…][crc]. The crc
        // covers head + parts (header always protected).
        let head = crate::frame::encode_vectored_head(req_id, msg_type, 0, ctrl_len, 0);

        let (tx, rx) = oneshot::channel();
        // Insert BEFORE submit — see register_and_submit for the rationale.
        self.pending.borrow_mut().insert(req_id, Pending::Frame(tx));

        // compute CRC32C over head + the multi-segment ctrl BEFORE
        // moving the parts into bufs (compute_ctrl_crc takes &[Bytes]).
        let crc = crate::frame::compute_ctrl_crc(&head, &payload_parts);
        let mut bufs: Vec<Bytes> = Vec::with_capacity(2 + payload_parts.len());
        bufs.push(Bytes::copy_from_slice(&head));
        bufs.extend(payload_parts);
        bufs.push(Bytes::copy_from_slice(&crc));

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
        Self::with_timeout(self.call_vectored(msg_type, payload_parts), timeout).await
    }

    /// Send a value-separable request (`MSG_PUT_BULK`'s shape): the ctrl parts
    /// (meta + key) are CRC-protected, the raw `value` rides after the crc as
    /// its own iovec and is NEVER crc-scanned by the sender — value integrity
    /// is the transport's + the storage layer's. Wire:
    /// `[head][ctrl parts…][crc][value]`.
    pub async fn call_vectored_bulk(
        &self,
        msg_type: u8,
        ctrl_parts: Vec<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, RpcError> {
        if self.closed.get() {
            return Err(RpcError::ConnectionClosed);
        }
        let req_id = self.next_req_id();
        let ctrl_len: usize = ctrl_parts.iter().map(|p| p.len()).sum();
        let head =
            crate::frame::encode_vectored_head(req_id, msg_type, 0, ctrl_len, value.len());

        let (tx, rx) = oneshot::channel();
        self.pending.borrow_mut().insert(req_id, Pending::Frame(tx));

        let crc = crate::frame::compute_ctrl_crc(&head, &ctrl_parts);
        let mut bufs: Vec<Bytes> = Vec::with_capacity(3 + ctrl_parts.len());
        bufs.push(Bytes::copy_from_slice(&head));
        bufs.extend(ctrl_parts);
        bufs.push(Bytes::copy_from_slice(&crc));
        bufs.push(value);

        if let Err(e) = self.submit(SubmitMsg::Vectored { bufs, req_id }).await {
            self.pending.borrow_mut().remove(&req_id);
            return Err(e);
        }

        Self::await_response(rx).await
    }

    /// Send a fire-and-forget frame (no response expected).
    ///
    /// `req_id = 0` tells the remote side not to send a response frame.
    /// Returns Ok once the frame has been queued for the writer_task
    /// (under back-pressure from the bounded submit channel).
    pub async fn send_oneshot(&self, msg_type: u8, payload: Bytes) -> Result<(), RpcError> {
        // short-circuit on a dead client; the submit channel may
        // still drain into a writer_task that has nowhere to read replies.
        if self.closed.get() {
            return Err(RpcError::ConnectionClosed);
        }
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

    /// Insert a pending entry, then queue the frame bytes for the writer_task,
    /// rolling the entry back if the submit fails.
    ///
    /// Ordering invariant: pending-insert happens BEFORE submit so the
    /// read_loop can dispatch a response the moment it arrives (no window
    /// where the response lands and finds no entry). Callers must check
    /// `self.closed` first, in the same sync block — see `send_frame`.
    async fn register_and_submit(
        &self,
        req_id: u32,
        entry: Pending,
        bytes: Bytes,
    ) -> Result<(), RpcError> {
        self.pending.borrow_mut().insert(req_id, entry);
        if let Err(e) = self.submit(SubmitMsg::Single { bytes, req_id }).await {
            // submit failed (writer_task exited / channel closed) — remove
            // the pending entry so we don't leak it.
            self.pending.borrow_mut().remove(&req_id);
            return Err(e);
        }
        Ok(())
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

/// Kernels cap a single `writev` at `IOV_MAX` iovecs (1024 on both Linux and
/// macOS) and reject anything longer — EINVAL on Linux, EMSGSIZE on macOS —
/// regardless of how few BYTES it carries. A 1075-iovec, 57 KB frame hit
/// exactly that and killed the writer task, surfacing to callers as an opaque
/// "connection closed" and to the PS as `batch_put op status=1`.
///
/// The count scales with the number of KEYS in a batch, not their size, so any
/// wide `put_many`/`delete_many` can reach it — which is why it only showed up
/// once ingest started issuing large batches concurrently.
///
/// Writing the chunks back to back on the same writer preserves wire order: the
/// writer task is the sole owner of the socket and awaits each chunk in turn,
/// so a frame is still contiguous from the peer's point of view.
const IOV_MAX: usize = 1024;

async fn write_vectored_chunked(writer: &mut WriteHalf, bufs: Vec<Bytes>) -> std::io::Result<()> {
    if bufs.len() <= IOV_MAX {
        let BufResult(r, _) = writer.write_vectored_all(bufs).await;
        return r;
    }
    let mut rest = bufs;
    while !rest.is_empty() {
        let tail = rest.split_off(rest.len().min(IOV_MAX));
        let BufResult(r, _) = writer.write_vectored_all(rest).await;
        r?;
        rest = tail;
    }
    Ok(())
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
    pending: Rc<RefCell<HashMap<u32, Pending>>>,
    peer_addr: SocketAddr,
) {
    while let Some(msg) = submit_rx.next().await {
        let req_id = msg.req_id();
        // fix instrumentation: capture iov count + total bytes
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
            SubmitMsg::Vectored { bufs, .. } => write_vectored_chunked(&mut writer, bufs).await,
        };

        if let Err(e) = result {
            // fix (CAP-EINVAL): ALWAYS log the write error at
            // WARN so production runs surface the root-cause signature
            // (iov_count, total_bytes, errno.raw_os_error()) rather than
            // just a downstream "submit error: connection closed" cascade.
            // The original concern speculated about `IOV_MAX`
            // exhaustion, and the instrumentation CONFIRMED it: a 1075-iovec
            // write of only 57 KB failed with errno 40. `write_vectored_chunked`
            // now caps the count, so an `iov_count` at or below `IOV_MAX` here
            // means a genuine socket error, not this shape.
            tracing::warn!(
                addr = %peer_addr,
                req_id,
                iov_count,
                total_bytes,
                errno = ?e.raw_os_error(),
                kind = ?e.kind(),
                error = %e,
                "rpc client writer exited on write error (instrumentation)"
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
    pending: Rc<RefCell<HashMap<u32, Pending>>>,
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

        // Peek the next frame header so a bulk value-response can be recv'd
        // straight into its destination instead of accumulating in the
        // FrameDecoder. (Inner `break`s mean "wait for more bytes" — they
        // exit this while back to the socket read above.)
        while let Some((req_id, _mt, flags, payload_len)) = decoder.peek_header() {
            let payload_len = payload_len as usize;

            // Does a `call_into_pooled` caller await this req_id? (Checked
            // without removing: the entry must stay pending if we `break` to
            // wait for more bytes.)
            let bulk_pending = matches!(
                pending.borrow().get(&req_id),
                Some(Pending::IntoPooled(_))
            );

            // bulk responses bypass FrameDecoder accumulation: always on UCX
            // (the value lands zero-copy in its dest); on TCP only when the
            // value is large enough to beat the batch-decoding normal path
            // (small values fall through to finish_into_pooled_from_frame
            // below). An ERROR frame (FLAG_ERROR — e.g. an authz
            // PermissionDenied) carries a status envelope, not a bulk ctrl: it
            // takes the normal decode path below, where the IntoPooled arm
            // decodes it into an `RpcError::Status`.
            //
            // The value boundary needs `ctrl_len` (v28: variable-length ctrl =
            // `[code][message]`); on TCP gate on the WHOLE payload first (a
            // ≥64 KiB payload of a small-ctrl frame is value-dominated), then
            // refine once ctrl_len is buffered.
            let bulk_fast_path = bulk_pending
                && (flags & crate::frame::FLAG_ERROR) == 0
                && (reader.is_ucx() || payload_len >= TCP_RECV_INTO_POOLED_MIN_BYTES);

            if bulk_fast_path {
                // Wait for the full prologue ([header][ctrl_len][ctrl][crc]),
                // verify the ctrl CRC (header included),
                // and parse the status ctrl. bulk ctrls are tiny (code+message),
                // so "prologue buffered" is ~always the very next read.
                let prologue = match decoder.peek_bulk_prologue() {
                    Ok(Some(p)) => p,
                    Ok(None) => break, // need more bytes
                    Err(e) => {
                        // Corrupt/malformed prologue: fail the caller + the
                        // connection (stream position is unrecoverable).
                        let msg = e.to_string();
                        if let Some(Pending::IntoPooled(tx)) =
                            pending.borrow_mut().remove(&req_id)
                        {
                            let _ = tx.send(Err(RpcError::status(
                                crate::error::StatusCode::Internal,
                                msg.clone(),
                            )));
                        }
                        return Err(e.into());
                    }
                };
                let (code, message) = {
                    let ctrl = decoder
                        .peek_ctrl(prologue.ctrl_len)
                        .expect("prologue verified => ctrl buffered");
                    match crate::frame::parse_bulk_ctrl(ctrl) {
                        Some((c, m)) => (c, String::from_utf8_lossy(m).into_owned()),
                        None => (0, String::new()), // empty ctrl = OK, no msg
                    }
                };
                let value_len = prologue.value_len;
                decoder.consume_bulk_prologue(prologue.ctrl_len);

                // Bind the removed entry BEFORE matching: a `match` on the
                // `borrow_mut()` temporary would hold the RefMut across the
                // awaits in the arms below and panic any concurrent `send_*`
                // on this thread (the same borrow-across-await class).
                let entry = pending.borrow_mut().remove(&req_id);
                match entry {
                    Some(Pending::IntoPooled(tx)) => {
                        // READ_LOOP acquires + owns the buffer: on caller-cancel
                        // the send below fails and `pb` drops → pool (no leak);
                        // the cancellable caller never owns the in-flight buffer.
                        let mut pb = autumn_transport::regpool_acquire(value_len);
                        if reader.is_ucx() {
                            let (dest, reg) = pb.dest_and_reg();
                            match recv_value_ucx(&mut reader, &mut decoder, dest, reg).await {
                                ValueRecv::Done => {}
                                ValueRecv::PeerClosed => {
                                    let _ = tx.send(Err(RpcError::ConnectionClosed));
                                    return Ok(());
                                }
                                ValueRecv::Failed(e) => {
                                    let _ = tx.send(Err(e.into()));
                                    return Err(RpcError::ConnectionClosed);
                                }
                            }
                        } else {
                            // TCP: drain the buffered prefix, then one owned
                            // read into the pool buffer (single copy).
                            let filled = {
                                let (dest, _reg) = pb.dest_and_reg();
                                decoder.drain_into(dest)
                            };
                            match reader.read_exact_into_pooled(pb, filled, value_len).await {
                                Ok(p) => pb = p,
                                Err(e) => {
                                    let _ = tx.send(Err(e.into()));
                                    return Err(RpcError::ConnectionClosed);
                                }
                            }
                        }
                        let _ = tx.send(Ok(BulkResp {
                            buf: pb,
                            code,
                            message,
                        }));
                    }
                    _ => unreachable!("bulk_pending checked above"),
                }
                continue;
            }

            // ── normal path: full frame decode ──
            match decoder.try_decode()? {
                Some(frame) => match pending.borrow_mut().remove(&frame.req_id) {
                    Some(Pending::Frame(tx)) => {
                        let _ = tx.send(frame);
                    }
                    Some(Pending::IntoPooled(tx)) => {
                        if frame.is_error() {
                            // Frame-level ERROR (FLAG_ERROR — e.g. an authz
                            // PermissionDenied from the PS authz_gate, or a
                            // mis-route NotFound): decode the status envelope
                            // instead of parsing it as a bulk ctrl. The
                            // fast-path gate above excludes FLAG_ERROR frames,
                            // so every error frame for a bulk caller lands here.
                            let (code, message) = RpcError::decode_status(&frame.payload);
                            let _ = tx.send(Err(RpcError::status(code, message)));
                        } else {
                            // Small-value TCP path: the decoder already split
                            // ctrl (status) and value; copy the value into a
                            // (plain) pool buffer and hand it back. The
                            // read_loop owns it until the send; on
                            // caller-cancel it drops → pool.
                            finish_into_pooled_from_frame(tx, &frame.payload, &frame.value);
                        }
                    }
                    None => {
                        tracing::trace!(
                            req_id = frame.req_id,
                            msg_type = frame.msg_type,
                            "response for unknown req_id, dropped"
                        );
                    }
                },
                None => break,
            }
        }
    }
}

/// How a bulk value recv ended.
enum ValueRecv {
    Done,
    /// Peer closed the connection mid-value.
    PeerClosed,
    /// Transport error mid-value.
    Failed(std::io::Error),
}

/// UCX bulk value recv: drain the value's already-buffered prefix out of the
/// decoder, then recv the remainder straight into `dest` (zero-copy via memh
/// when `reg` is Some). `dest.len()` is the exact value length.
async fn recv_value_ucx(
    reader: &mut ReadHalf,
    decoder: &mut FrameDecoder,
    dest: &mut [u8],
    reg: Option<&autumn_transport::RegisteredMem>,
) -> ValueRecv {
    let mut filled = decoder.drain_into(dest);
    while filled < dest.len() {
        match reader.recv_into(&mut dest[filled..], reg).await {
            Ok(0) => return ValueRecv::PeerClosed,
            Ok(k) => filled += k,
            Err(e) => return ValueRecv::Failed(e),
        }
    }
    ValueRecv::Done
}

/// Complete a `call_into_pooled` from a fully-decoded value frame (TCP /
/// non-UCX small-value path). The decoder already verified the ctrl CRC and
/// split `ctrl` (`[code][message]`) from the raw `value`. The read_loop
/// acquires a (plain, on TCP) pool buffer, copies the value in, and hands it
/// back; on caller-cancel `tx.send` fails and the buffer drops → pool.
fn finish_into_pooled_from_frame(
    tx: oneshot::Sender<Result<BulkResp, RpcError>>,
    ctrl: &[u8],
    value: &[u8],
) {
    let (code, message) = match crate::frame::parse_bulk_ctrl(ctrl) {
        Some((c, m)) => (c, String::from_utf8_lossy(m).into_owned()),
        None => (0, String::new()), // empty ctrl = OK, no message
    };
    let mut pb = autumn_transport::regpool_acquire(value.len());
    {
        let (dest, _reg) = pb.dest_and_reg();
        dest.copy_from_slice(value);
    }
    let _ = tx.send(Ok(BulkResp {
        buf: pb,
        code,
        message,
    }));
}

// ── connection-close tests ─────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Duration;

    /// when the peer closes its socket without responding, the
    /// client's `read_loop` exits, `closed` flips to true, and the very
    /// next `send_frame`/`send_vectored`/`send_oneshot`/`call` returns
    /// `ConnectionClosed` immediately instead of inserting a fresh
    /// pending entry that nobody will ever dispatch (the bug that caused
    /// the >120 s hang in the original repro).
    #[compio::test]
    async fn closed_flag_set_after_peer_disconnect() {
        // drive-by: was `autumn_transport::init()` (function removed in
        // a prior refactor; baseline build failure noted in claude-progress
        // at the time). The transport is now initialised on first use via
        // `current_or_init()`.
        let _ = autumn_transport::current_or_init();

        // Bind a server-side listener; accept one connection then drop
        // both halves so the client sees EOF.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let server_addr = listener.local_addr().expect("local_addr");
        let accept_thread = std::thread::spawn(move || {
            let (sock, _) = listener.accept().expect("accept");
            // Hold for ~50 ms so the client finishes connecting, then
            // drop — the FIN reaches the client and read_loop exits.
            std::thread::sleep(Duration::from_millis(50));
            drop(sock);
        });

        let client = RpcClient::connect(server_addr).await.expect("connect");
        accept_thread.join().expect("accept thread");

        // Wait up to 1 s for read_loop to observe EOF and flip closed.
        let mut waited = Duration::ZERO;
        while !client.is_closed() && waited < Duration::from_secs(1) {
            compio::time::sleep(Duration::from_millis(10)).await;
            waited += Duration::from_millis(10);
        }
        assert!(
            client.is_closed(),
            "RpcClient.closed should flip true within 1 s of peer FIN"
        );

        // Each public submit path must short-circuit, NOT hang.
        let r = client.send_frame(Frame::request(1, 1, Bytes::new())).await;
        assert!(matches!(r, Err(RpcError::ConnectionClosed)));

        let r = client
            .send_vectored(2, vec![Bytes::from_static(b"x")])
            .await;
        assert!(matches!(r, Err(RpcError::ConnectionClosed)));

        let r = client.send_oneshot(3, Bytes::new()).await;
        assert!(matches!(r, Err(RpcError::ConnectionClosed)));

        // `call` returns the same error.
        let r = client.call(4, Bytes::new()).await;
        assert!(matches!(r, Err(RpcError::ConnectionClosed)));
    }

    /// A frame-level ERROR response (FLAG_ERROR — the shape the PS authz_gate
    /// emits for a PermissionDenied, or the ps-conn mis-route NotFound) to a
    /// `call_into_pooled` caller must surface as the decoded
    /// `RpcError::Status`, NOT be misparsed as a bulk meta (whose first byte
    /// would read the StatusCode as a bogus application code and the message
    /// bytes as value payload).
    #[compio::test]
    async fn call_into_pooled_error_frame_surfaces_status() {
        use std::io::{Read, Write};
        let _ = autumn_transport::current_or_init(); // TCP

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let server_addr = listener.local_addr().expect("local_addr");

        let srv = std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().expect("accept");
            let mut hdr = [0u8; 10];
            sock.read_exact(&mut hdr).expect("read req hdr");
            let req_id = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
            let plen = u32::from_le_bytes(hdr[6..10].try_into().unwrap()) as usize;
            let mut req_payload = vec![0u8; plen];
            sock.read_exact(&mut req_payload).expect("read req payload");
            // Normal CRC'd error frame, exactly what authz_gate emits.
            let status = RpcError::encode_status(
                crate::error::StatusCode::PermissionDenied,
                "protected key requires a capability token",
            );
            let bytes = Frame::error(req_id, 9, status).encode();
            sock.write_all(&bytes).expect("write resp");
            std::thread::sleep(Duration::from_millis(50));
        });

        let client = RpcClient::connect(server_addr).await.expect("connect");
        let r = client.call_into_pooled(9, Bytes::from_static(b"req")).await;
        match r {
            Err(RpcError::Status { code, message }) => {
                assert_eq!(code, crate::error::StatusCode::PermissionDenied);
                assert!(
                    message.contains("capability token"),
                    "message preserved: {message}"
                );
            }
            Err(other) => panic!("expected PermissionDenied status, got {other:?}"),
            Ok(z) => panic!(
                "expected PermissionDenied status, got Ok(code={}, {} bytes)",
                z.code,
                z.buf.len()
            ),
        }
        srv.join().expect("server thread");
    }

    /// `call_into_pooled` recvs a value-separable response into a
    /// read_loop-owned `PooledBuf` (TCP small-value path = decode + copy),
    /// ctrl CRC verified by the decoder, `BulkResp` handed back. Proves the
    /// cancel-safe primitive's happy path before the UCX + EN/PS wiring.
    #[compio::test]
    async fn call_into_pooled_tcp_returns_filled_buffer() {
        use std::io::{Read, Write};
        let _ = autumn_transport::current_or_init(); // TCP

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let server_addr = listener.local_addr().expect("local_addr");
        let value: Vec<u8> = (0..4096u32).map(|i| ((i * 7) & 0xff) as u8).collect();
        let value_srv = value.clone();

        let srv = std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().expect("accept");
            let mut hdr = [0u8; 10];
            sock.read_exact(&mut hdr).expect("read req hdr");
            let req_id = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
            let plen = u32::from_le_bytes(hdr[6..10].try_into().unwrap()) as usize;
            let mut req_payload = vec![0u8; plen];
            sock.read_exact(&mut req_payload).expect("read req payload");
            // v28 value-separable response: [head(ctrl=[code])][raw value].
            let head =
                crate::frame::encode_bulk_response_head(req_id, 15, 0, "", value_srv.len());
            sock.write_all(&head).expect("write head");
            sock.write_all(&value_srv).expect("write value");
            std::thread::sleep(Duration::from_millis(50));
        });

        let client = RpcClient::connect(server_addr).await.expect("connect");
        let z = client
            .call_into_pooled(15, Bytes::from_static(b"req"))
            .await
            .expect("call_into_pooled");
        assert_eq!(z.code, 0);
        assert!(z.message.is_empty());
        assert_eq!(
            z.buf.filled(),
            &value[..],
            "value bytes landed in the pool buffer"
        );
        srv.join().expect("server thread");
    }

    /// `call_into_pooled` over TCP with a value ABOVE
    /// `TCP_RECV_INTO_POOLED_MIN_BYTES` engages the read_loop recv-into-pooled
    /// branch: prologue verified via `peek_bulk_prologue`, then the value recv'd
    /// straight into the PooledBuf via a compio owned read, skipping the
    /// FrameDecoder accumulation. A 256 KiB value forces multiple socket reads
    /// (64 KiB read_loop scratch), exercising drain-prefix + read-remainder.
    #[compio::test]
    async fn call_into_pooled_tcp_large_value_recv_into_pooled() {
        use std::io::{Read, Write};
        let _ = autumn_transport::current_or_init(); // TCP

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let server_addr = listener.local_addr().expect("local_addr");
        let n = 256 * 1024usize; // > 64 KiB threshold; spans many read_loop reads
        let value: Vec<u8> = (0..n).map(|i| ((i * 31 + 7) & 0xff) as u8).collect();
        let value_srv = value.clone();

        let srv = std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().expect("accept");
            let mut hdr = [0u8; 10];
            sock.read_exact(&mut hdr).expect("read req hdr");
            let req_id = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
            let plen = u32::from_le_bytes(hdr[6..10].try_into().unwrap()) as usize;
            let mut req_payload = vec![0u8; plen];
            sock.read_exact(&mut req_payload).expect("read req payload");
            let head =
                crate::frame::encode_bulk_response_head(req_id, 15, 0, "", value_srv.len());
            sock.write_all(&head).expect("write head");
            sock.write_all(&value_srv).expect("write value");
            std::thread::sleep(Duration::from_millis(50));
        });

        let client = RpcClient::connect(server_addr).await.expect("connect");
        let z = client
            .call_into_pooled(15, Bytes::from_static(b"req"))
            .await
            .expect("call_into_pooled");
        assert_eq!(z.code, 0);
        assert_eq!(z.buf.len(), n, "full value length");
        assert_eq!(
            z.buf.filled(),
            &value[..],
            "large value bytes landed in the pool buffer"
        );
        srv.join().expect("server thread");
    }

    /// v28 — a bulk ERROR response (non-zero code + human message in the CRC'd
    /// ctrl, empty value) surfaces both fields through `call_into_pooled`.
    #[compio::test]
    async fn call_into_pooled_zc_error_message_round_trips() {
        use std::io::{Read, Write};
        let _ = autumn_transport::current_or_init(); // TCP

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let server_addr = listener.local_addr().expect("local_addr");

        let srv = std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().expect("accept");
            let mut hdr = [0u8; 10];
            sock.read_exact(&mut hdr).expect("read req hdr");
            let req_id = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
            let plen = u32::from_le_bytes(hdr[6..10].try_into().unwrap()) as usize;
            let mut req_payload = vec![0u8; plen];
            sock.read_exact(&mut req_payload).expect("read req payload");
            let head = crate::frame::encode_bulk_response_head(
                req_id,
                15,
                6, // e.g. CODE_EVERSION_MISMATCH
                "eversion mismatch: have 3 want 5",
                0,
            );
            sock.write_all(&head).expect("write head");
            std::thread::sleep(Duration::from_millis(50));
        });

        let client = RpcClient::connect(server_addr).await.expect("connect");
        let z = client
            .call_into_pooled(15, Bytes::from_static(b"req"))
            .await
            .expect("call_into_pooled");
        assert_eq!(z.code, 6);
        assert_eq!(z.message, "eversion mismatch: have 3 want 5");
        assert!(z.buf.filled().is_empty());
        srv.join().expect("server thread");
    }
}
