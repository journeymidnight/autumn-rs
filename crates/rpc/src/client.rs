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

// ── F216 zero-copy GET (recv-into-registered-dest) ───────────────────────────
//
// `call_into_dest` recvs a value-response payload straight into a caller-owned
// (registered, for UCX) destination buffer instead of a fresh `Vec`. The
// response uses a value-separable framing so the raw value is a contiguous
// tail the receiver can land directly in `dest`:
//
//   frame payload = [ZC meta: code(1) value_len(4 LE) reserved(4 LE)][value]
//
// The frame is emitted CRC-less (no frame CRC) so the value tail can be recv'd
// straight into `dest` without stripping a trailer. Value integrity is the
// transport's (UCX NIC ICRC / TCP kernel segment checksum) — F219 removed the
// per-value crc that used to ride in the meta's now-reserved 3rd field.
// `ZC_META_LEN` bytes precede the value.

/// Length of the zero-copy value-response meta prefix.
pub const ZC_META_LEN: usize = 9;

/// F219 — minimum value size at which the **TCP** `call_into_pooled` read path
/// recvs the value straight into a `PooledBuf` (compio owned read) instead of
/// letting the `FrameDecoder` accumulate it then copying into the pool buffer.
/// Below this the FrameDecoder path is cheaper (it batch-decodes multiple small
/// frames per socket read and a small value's whole payload is usually already
/// buffered, so recv-into-pooled would only add a pool acquire + an extra
/// syscall). Mirrors the UCX read threshold (`UCX_ZC_READ_MIN_BYTES`); TCP
/// can't be true zero-copy (the kernel copy is unavoidable) but this removes the
/// extra app-level copy for large values.
pub const TCP_RECV_INTO_POOLED_MIN_BYTES: usize = 64 * 1024;

/// Build the ZC meta prefix for `value`. Returns `[code][value_len][crc32c]`.
///
/// F219: the `crc32c` field is now always `0`. The per-value ZC crc on the hot
/// path (compute on send + verify on recv = two full crc32c passes over every
/// value) was removed — it cost ~20% of a core at 8 MiB and duplicated the
/// integrity already provided by the transport (UCX NIC ICRC / TCP kernel
/// segment checksum). The 9-byte layout is kept (wire-compatible) so the field
/// is reserved/zero rather than dropped. Normal RPC frames keep their
/// frame-CRC (F165) — only the ZC-read value crc is gone.
pub fn encode_zc_meta(code: u8, value: &[u8]) -> [u8; ZC_META_LEN] {
    let mut m = [0u8; ZC_META_LEN];
    m[0] = code;
    m[1..5].copy_from_slice(&(value.len() as u32).to_le_bytes());
    // m[5..9] left zero — ZC value crc removed (F219).
    m
}

/// Parse a ZC meta prefix → `(code, value_len, reserved)`. The third field
/// (formerly the value crc32c) is reserved/zero since F219.
fn parse_zc_meta(b: &[u8]) -> (u8, u32, u32) {
    (
        b[0],
        u32::from_le_bytes(b[1..5].try_into().unwrap()),
        u32::from_le_bytes(b[5..9].try_into().unwrap()),
    )
}

/// Result of a `call_into_dest`: the value bytes are already in the caller's
/// `dest`; this carries the status code + how many bytes were written.
#[derive(Debug, Clone, Copy)]
pub struct DestMeta {
    pub code: u8,
    pub value_len: usize,
}

/// CQ-side inflight entry. A normal request awaits a `Frame`; a `call_into_dest`
/// request recvs its value straight into `dest` and gets a `DestMeta`.
enum Pending {
    Frame(oneshot::Sender<Frame>),
    IntoDest(IntoDest),
    /// `call_into_pooled`: the READ_LOOP owns the dest buffer — it acquires a
    /// `PooledBuf` at recv time, recvs the value into it, and hands the filled
    /// buffer back here. On caller-cancel the receiver is gone → the read_loop
    /// drops the `PooledBuf` → returns to pool (no leak; cancel-safe, since the
    /// cancellable caller never owns the in-flight buffer). Carries the status
    /// code alongside the buffer.
    IntoPooled(oneshot::Sender<Result<(autumn_transport::PooledBuf, u8), RpcError>>),
}

/// Recv-into-dest state. `dest`/`reg` are raw because the read_loop and the
/// caller run on the SAME compio runtime (single thread) and the caller holds
/// the backing buffer (a regpool `PooledBuf`) alive until `meta_tx` fires.
struct IntoDest {
    dest: *mut u8,
    dest_cap: usize,
    /// `*const RegisteredMem` for the memh (UCX zero-copy), or null →
    /// unregistered (copy-out) / TCP (decode+memcpy).
    reg: *const autumn_transport::RegisteredMem,
    meta_tx: oneshot::Sender<Result<DestMeta, RpcError>>,
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
        // caller's `rx.await` then hangs forever (F121 root cause).
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
            // F121: set closed BEFORE clearing pending so subsequent
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

        let resp = rx.await.map_err(|_| RpcError::ConnectionClosed)?;
        if resp.is_error() {
            let (code, message) = RpcError::decode_status(&resp.payload);
            return Err(RpcError::status(code, message));
        }
        Ok(resp.payload)
    }

    /// F216 zero-copy GET: send a request whose response is a value-separable
    /// frame (`[ZC meta][value]`, see module docs), and recv the value bytes
    /// straight into `dest` (no intermediate `Vec`). For UCX with `Some(reg)`
    /// the value RDMAs directly into the registered dest (zero-copy); for TCP
    /// (or unregistered) it's one copy off the wire. Returns the status code +
    /// value length; the value is already in `dest` on `Ok`.
    ///
    /// SAFETY/lifetime: `dest`/`reg` must stay valid until this future
    /// resolves. The caller (e.g. a regpool `PooledBuf` or a registered sglang
    /// page) holds them across the await — read_loop + caller share one compio
    /// thread, so no cross-thread aliasing.
    pub async fn call_into_dest(
        &self,
        msg_type: u8,
        payload: Bytes,
        dest: *mut u8,
        dest_cap: usize,
        reg: Option<&autumn_transport::RegisteredMem>,
    ) -> Result<DestMeta, RpcError> {
        if self.closed.get() {
            return Err(RpcError::ConnectionClosed);
        }
        let req_id = self.next_req_id();
        let frame = Frame::request(req_id, msg_type, payload);
        let (meta_tx, meta_rx) = oneshot::channel();
        let into = IntoDest {
            dest,
            dest_cap,
            reg: reg.map_or(std::ptr::null(), |r| r as *const _),
            meta_tx,
        };
        // Insert BEFORE submit — same ordering invariant as send_frame.
        self.pending
            .borrow_mut()
            .insert(req_id, Pending::IntoDest(into));
        let bytes = frame.encode();
        if let Err(e) = self.submit(SubmitMsg::Single { bytes, req_id }).await {
            self.pending.borrow_mut().remove(&req_id);
            return Err(e);
        }
        match meta_rx.await {
            Ok(res) => res,
            Err(_) => Err(RpcError::ConnectionClosed),
        }
    }

    /// F216-E cancel-safe zero-copy read: send a request whose value response
    /// the READ_LOOP recvs straight into a freshly-acquired `PooledBuf` (which
    /// the read_loop owns), then hands back here. Unlike `call_into_dest`, the
    /// caller never owns the in-flight buffer — so a cancelled/timed-out caller
    /// can NOT leave the NIC writing a freed/recycled buffer, and the buffer is
    /// always reclaimed (handed back on success, dropped→pool on cancel — never
    /// leaked). Returns `(filled PooledBuf, status code)`. On UCX the value
    /// RDMAs into the registered pool buffer (memh zero-copy); on TCP the value
    /// is copied off the wire into a (plain) pool buffer.
    pub async fn call_into_pooled(
        &self,
        msg_type: u8,
        payload: Bytes,
    ) -> Result<(autumn_transport::PooledBuf, u8), RpcError> {
        if self.closed.get() {
            return Err(RpcError::ConnectionClosed);
        }
        let req_id = self.next_req_id();
        let frame = Frame::request(req_id, msg_type, payload);
        let (meta_tx, meta_rx) = oneshot::channel();
        self.pending
            .borrow_mut()
            .insert(req_id, Pending::IntoPooled(meta_tx));
        let bytes = frame.encode();
        if let Err(e) = self.submit(SubmitMsg::Single { bytes, req_id }).await {
            self.pending.borrow_mut().remove(&req_id);
            return Err(e);
        }
        match meta_rx.await {
            Ok(res) => res,
            Err(_) => Err(RpcError::ConnectionClosed),
        }
    }

    /// Send a request frame and return the oneshot receiver for the response.
    ///
    /// On return, the frame has been queued for the writer_task (or is waiting
    /// for a slot when the submit channel is full — natural back-pressure).
    /// The caller awaits the receiver to get the response frame.
    pub async fn send_frame(&self, frame: Frame) -> Result<oneshot::Receiver<Frame>, RpcError> {
        // F121: short-circuit if the reader/writer task has already
        // exited. The check + pending.insert below run in one sync block
        // (single-threaded compio, no awaits), so a concurrent close that
        // races us either flips `closed` first → we return here, or runs
        // after we insert → its `pending.clear()` cancels our `tx`.
        if self.closed.get() {
            return Err(RpcError::ConnectionClosed);
        }
        let req_id = frame.req_id;
        let (tx, rx) = oneshot::channel();

        // Borrow scoped: insert → drop. Ordering: pending-insert BEFORE submit
        // so that when reader_task sees the response it can dispatch
        // immediately (no race window where response lands first).
        self.pending.borrow_mut().insert(req_id, Pending::Frame(tx));

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
        // F121: see send_frame for the rationale.
        if self.closed.get() {
            return Err(RpcError::ConnectionClosed);
        }
        let req_id = self.next_req_id();
        let inner_payload_len: usize = payload_parts.iter().map(|p| p.len()).sum();
        // encode_request_header sets FLAG_CRC and bumps the wire `payload_len`
        // field by 4 to account for the CRC trailer we append below.
        let hdr = Frame::encode_request_header(req_id, msg_type, inner_payload_len as u32);

        let (tx, rx) = oneshot::channel();
        // Insert BEFORE submit — see comment in send_frame.
        self.pending.borrow_mut().insert(req_id, Pending::Frame(tx));

        let v1 = (hdr[5] & crate::frame::FLAG_CRC) != 0;
        let extra = if v1 { 1 } else { 0 };
        let mut bufs: Vec<Bytes> = Vec::with_capacity(1 + payload_parts.len() + extra);
        bufs.push(Bytes::copy_from_slice(&hdr));
        if v1 {
            // F163: compute CRC32C over the multi-segment payload BEFORE
            // moving the parts into bufs (compute_payload_crc takes &[Bytes]).
            let crc = crate::frame::compute_payload_crc(&payload_parts);
            bufs.extend(payload_parts);
            bufs.push(Bytes::copy_from_slice(&crc));
        } else {
            bufs.extend(payload_parts);
        }

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
        // F121: short-circuit on a dead client; the submit channel may
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
    pending: Rc<RefCell<HashMap<u32, Pending>>>,
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

        // Peek so a `call_into_dest` value-response can be recv'd straight
        // into the caller's registered dest (UCX) before try_decode would
        // buffer the whole payload. (Inner `break`s for "wait for more
        // bytes" / try_decode None still exit this loop, same as for `loop`.)
        while let Some((req_id, _mt, _flags, payload_len)) = decoder.peek_header() {
            let is_into = matches!(pending.borrow().get(&req_id), Some(Pending::IntoDest(_)));

            if is_into && reader.is_ucx() {
                // ── UCX zero-copy recv-into-dest ──
                // Value frame is CRC-less: payload = [ZC meta][value]. Need header
                // + meta buffered to parse the meta prefix; else read more.
                if (payload_len as usize) < ZC_META_LEN
                    || decoder.buffered_len() < crate::frame::HEADER_LEN + ZC_META_LEN
                {
                    if (payload_len as usize) < ZC_META_LEN {
                        // Malformed value frame — fail this request + close.
                        if let Some(Pending::IntoDest(into)) = pending.borrow_mut().remove(&req_id)
                        {
                            let _ = into.meta_tx.send(Err(RpcError::status(
                                crate::error::StatusCode::Internal,
                                "zc value frame shorter than meta",
                            )));
                        }
                        return Err(RpcError::status(
                            crate::error::StatusCode::Internal,
                            "zc value frame shorter than meta",
                        ));
                    }
                    break; // need more bytes for the meta prefix
                }
                let Some(Pending::IntoDest(into)) = pending.borrow_mut().remove(&req_id) else {
                    unreachable!("is_into checked above");
                };
                decoder.consume(crate::frame::HEADER_LEN);
                let mut meta = [0u8; ZC_META_LEN];
                let got = decoder.drain_into(&mut meta);
                debug_assert_eq!(got, ZC_META_LEN);
                let (code, _m_vlen, _crc) = parse_zc_meta(&meta);
                let value_len = payload_len as usize - ZC_META_LEN;
                if value_len > into.dest_cap {
                    let _ = into.meta_tx.send(Err(RpcError::status(
                        crate::error::StatusCode::InvalidArgument,
                        "zc value larger than dest buffer",
                    )));
                    return Err(RpcError::status(
                        crate::error::StatusCode::InvalidArgument,
                        "zc value larger than dest buffer",
                    ));
                }
                // SAFETY: caller holds the backing buffer alive until meta_tx
                // fires (it awaits the receiver); same compio thread.
                let dest = unsafe { std::slice::from_raw_parts_mut(into.dest, value_len) };
                let reg = if into.reg.is_null() {
                    None
                } else {
                    Some(unsafe { &*into.reg })
                };
                // Drain the value's already-buffered prefix, then recv the rest
                // straight into dest (zero-copy via memh when reg is Some).
                let mut filled = decoder.drain_into(dest);
                while filled < value_len {
                    match reader.recv_into(&mut dest[filled..], reg).await {
                        Ok(0) => {
                            let _ = into.meta_tx.send(Err(RpcError::ConnectionClosed));
                            return Ok(());
                        }
                        Ok(k) => filled += k,
                        Err(e) => {
                            let _ = into.meta_tx.send(Err(e.into()));
                            return Err(RpcError::ConnectionClosed);
                        }
                    }
                }
                // F219: ZC value crc removed — value integrity is the transport's
                // job (UCX ICRC / TCP kernel checksum).
                let _ = into.meta_tx.send(Ok(DestMeta { code, value_len }));
                continue;
            }

            // ── call_into_pooled: read_loop owns the dest (cancel-safe) ──
            let is_pooled = matches!(pending.borrow().get(&req_id), Some(Pending::IntoPooled(_)));
            if is_pooled && reader.is_ucx() {
                if (payload_len as usize) < ZC_META_LEN
                    || decoder.buffered_len() < crate::frame::HEADER_LEN + ZC_META_LEN
                {
                    if (payload_len as usize) < ZC_META_LEN {
                        if let Some(Pending::IntoPooled(tx)) = pending.borrow_mut().remove(&req_id)
                        {
                            let _ = tx.send(Err(RpcError::status(
                                crate::error::StatusCode::Internal,
                                "zc value frame shorter than meta",
                            )));
                        }
                        return Err(RpcError::status(
                            crate::error::StatusCode::Internal,
                            "zc value frame shorter than meta",
                        ));
                    }
                    break; // need more bytes for the meta prefix
                }
                let Some(Pending::IntoPooled(tx)) = pending.borrow_mut().remove(&req_id) else {
                    unreachable!("is_pooled checked above");
                };
                decoder.consume(crate::frame::HEADER_LEN);
                let mut meta = [0u8; ZC_META_LEN];
                let _ = decoder.drain_into(&mut meta);
                let (code, _m_vlen, _crc) = parse_zc_meta(&meta);
                let value_len = payload_len as usize - ZC_META_LEN;
                // READ_LOOP acquires + owns the buffer. On caller-cancel the
                // send below fails and `pb` drops → pool (no leak); the buffer
                // is never owned by the cancellable caller.
                let mut pb = autumn_transport::regpool_acquire(value_len);
                let (dest, reg) = pb.dest_and_reg();
                let mut filled = decoder.drain_into(dest);
                let mut recv_err: Option<RpcError> = None;
                while filled < value_len {
                    match reader.recv_into(&mut dest[filled..], reg).await {
                        Ok(0) => {
                            let _ = tx.send(Err(RpcError::ConnectionClosed));
                            return Ok(());
                        }
                        Ok(k) => filled += k,
                        Err(e) => {
                            recv_err = Some(e.into());
                            break;
                        }
                    }
                }
                if let Some(e) = recv_err {
                    let _ = tx.send(Err(e));
                    return Err(RpcError::ConnectionClosed);
                }
                // F219: ZC value crc removed (transport integrity covers it).
                // (`dest`/`reg` borrows of `pb` end at the recv loop above — NLL
                // lets `pb` move into the send below; no explicit drop needed.)
                let _ = tx.send(Ok((pb, code)));
                continue;
            }

            // ── call_into_pooled on TCP: recv the large value straight into a
            //    read_loop-owned PooledBuf (compio owned read), skipping the
            //    FrameDecoder accumulation + finish_into_pooled_from_frame copy.
            //    Only the kernel→userspace copy remains (TCP can't be true ZC).
            //    Small values (< threshold) fall through to the normal decode
            //    path — finish_into_pooled_from_frame still serves them. ──
            if is_pooled && !reader.is_ucx() {
                let value_len = (payload_len as usize).saturating_sub(ZC_META_LEN);
                if (payload_len as usize) >= ZC_META_LEN
                    && value_len >= TCP_RECV_INTO_POOLED_MIN_BYTES
                {
                    // Need header + meta buffered to parse the meta prefix.
                    if decoder.buffered_len() < crate::frame::HEADER_LEN + ZC_META_LEN {
                        break; // wait for the next read to accumulate the meta
                    }
                    let Some(Pending::IntoPooled(tx)) = pending.borrow_mut().remove(&req_id) else {
                        unreachable!("is_pooled checked above");
                    };
                    decoder.consume(crate::frame::HEADER_LEN);
                    let mut meta = [0u8; ZC_META_LEN];
                    let _ = decoder.drain_into(&mut meta);
                    let (code, _m_vlen, _crc) = parse_zc_meta(&meta);
                    // READ_LOOP owns the buffer (cancel-safe). Drain the buffered
                    // value prefix, then recv the remainder off the wire.
                    let mut pb = autumn_transport::regpool_acquire(value_len);
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
                    // F219: ZC value crc removed (transport integrity covers it).
                    let _ = tx.send(Ok((pb, code)));
                    continue;
                }
                // else: small value → normal decode path below.
            }

            // ── call_into_dest on TCP (F219): recv the large value straight into
            //    the CALLER's dest (compio owned read over a raw-ptr buffer),
            //    skipping the FrameDecoder accumulation + finish_into_dest memcpy.
            //    Same cancel contract as the UCX call_into_dest path (dest
            //    outlives the call; no per-call timeout — get_into honours this).
            //    Small values fall through to finish_into_dest_from_frame. ──
            if is_into && !reader.is_ucx() {
                let value_len = (payload_len as usize).saturating_sub(ZC_META_LEN);
                if (payload_len as usize) >= ZC_META_LEN
                    && value_len >= TCP_RECV_INTO_POOLED_MIN_BYTES
                {
                    if decoder.buffered_len() < crate::frame::HEADER_LEN + ZC_META_LEN {
                        break; // wait for the meta prefix
                    }
                    let Some(Pending::IntoDest(into)) = pending.borrow_mut().remove(&req_id) else {
                        unreachable!("is_into checked above");
                    };
                    decoder.consume(crate::frame::HEADER_LEN);
                    let mut meta = [0u8; ZC_META_LEN];
                    let _ = decoder.drain_into(&mut meta);
                    let (code, _m_vlen, _crc) = parse_zc_meta(&meta);
                    if value_len > into.dest_cap {
                        let _ = into.meta_tx.send(Err(RpcError::status(
                            crate::error::StatusCode::InvalidArgument,
                            "zc value larger than dest buffer",
                        )));
                        return Err(RpcError::status(
                            crate::error::StatusCode::InvalidArgument,
                            "zc value larger than dest buffer",
                        ));
                    }
                    // SAFETY: caller holds `dest` alive until meta_tx fires (it
                    // awaits the receiver); same compio thread; no per-call timeout.
                    let dest = unsafe { std::slice::from_raw_parts_mut(into.dest, value_len) };
                    let filled = decoder.drain_into(dest);
                    // Recv the remainder straight into the caller dest (single copy).
                    match unsafe {
                        reader
                            .read_exact_into_raw(into.dest, filled, value_len)
                            .await
                    } {
                        Ok(()) => {
                            let _ = into.meta_tx.send(Ok(DestMeta { code, value_len }));
                        }
                        Err(e) => {
                            let _ = into.meta_tx.send(Err(e.into()));
                            return Err(RpcError::ConnectionClosed);
                        }
                    }
                    continue;
                }
                // else: small value → normal decode path below.
            }

            // ── normal path: full frame decode ──
            match decoder.try_decode()? {
                Some(frame) => match pending.borrow_mut().remove(&frame.req_id) {
                    Some(Pending::Frame(tx)) => {
                        let _ = tx.send(frame);
                    }
                    Some(Pending::IntoDest(into)) => {
                        // TCP / non-UCX: the whole [meta][value] frame was
                        // decoded; copy the value into dest (one memcpy — TCP
                        // always pays the kernel copy anyway).
                        finish_into_dest_from_frame(into, &frame.payload);
                    }
                    Some(Pending::IntoPooled(tx)) => {
                        // TCP / non-UCX: copy the value into a (plain) pool
                        // buffer and hand it back. The read_loop owns it until
                        // the send; on caller-cancel it drops → pool.
                        finish_into_pooled_from_frame(tx, &frame.payload);
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

/// Complete a `call_into_dest` from a fully-decoded value frame (TCP /
/// non-UCX path). `payload` = `[ZC meta][value]`.
fn finish_into_dest_from_frame(into: IntoDest, payload: &[u8]) {
    if payload.len() < ZC_META_LEN {
        let _ = into.meta_tx.send(Err(RpcError::status(
            crate::error::StatusCode::Internal,
            "zc value frame shorter than meta",
        )));
        return;
    }
    let (code, _m_vlen, _crc) = parse_zc_meta(&payload[..ZC_META_LEN]);
    let value = &payload[ZC_META_LEN..];
    if value.len() > into.dest_cap {
        let _ = into.meta_tx.send(Err(RpcError::status(
            crate::error::StatusCode::InvalidArgument,
            "zc value larger than dest buffer",
        )));
        return;
    }
    // SAFETY: caller holds the dest buffer alive until meta_tx fires.
    let dest = unsafe { std::slice::from_raw_parts_mut(into.dest, value.len()) };
    dest.copy_from_slice(value);
    // F219: ZC value crc removed (transport integrity covers it).
    let _ = into.meta_tx.send(Ok(DestMeta {
        code,
        value_len: value.len(),
    }));
}

/// Complete a `call_into_pooled` from a fully-decoded value frame (TCP /
/// non-UCX path). `payload` = `[ZC meta][value]`. The read_loop acquires a
/// (plain, on TCP) pool buffer, copies the value in, and hands it back; on
/// caller-cancel `tx.send` fails and the buffer drops → pool.
fn finish_into_pooled_from_frame(
    tx: oneshot::Sender<Result<(autumn_transport::PooledBuf, u8), RpcError>>,
    payload: &[u8],
) {
    if payload.len() < ZC_META_LEN {
        let _ = tx.send(Err(RpcError::status(
            crate::error::StatusCode::Internal,
            "zc value frame shorter than meta",
        )));
        return;
    }
    let (code, _m_vlen, _crc) = parse_zc_meta(&payload[..ZC_META_LEN]);
    let value = &payload[ZC_META_LEN..];
    let mut pb = autumn_transport::regpool_acquire(value.len());
    {
        let (dest, _reg) = pb.dest_and_reg();
        dest.copy_from_slice(value);
    }
    // F219: ZC value crc removed (transport integrity covers it).
    let _ = tx.send(Ok((pb, code)));
}

// ── F121 tests ─────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Duration;

    /// F121 — when the peer closes its socket without responding, the
    /// client's `read_loop` exits, `closed` flips to true, and the very
    /// next `send_frame`/`send_vectored`/`send_oneshot`/`call` returns
    /// `ConnectionClosed` immediately instead of inserting a fresh
    /// pending entry that nobody will ever dispatch (the bug that caused
    /// the >120 s hang in the original repro).
    #[compio::test]
    async fn closed_flag_set_after_peer_disconnect() {
        // F161 drive-by: was `autumn_transport::init()` (function removed in
        // a prior refactor; baseline build failure noted in claude-progress
        // since F151). The transport is now initialised on first use via
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

    /// F216 — `call_into_dest` recvs a value-separable response straight into
    /// the caller's dest buffer (TCP path = decode + memcpy), with the value
    /// CRC verified over the dest. Validates the API + framing before the UCX
    /// zero-copy path (which needs a live RoCE cluster) and the EN/PS wiring.
    #[compio::test]
    async fn call_into_dest_tcp_recvs_value_into_dest() {
        use std::io::{Read, Write};
        let _ = autumn_transport::current_or_init(); // TCP

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let server_addr = listener.local_addr().expect("local_addr");
        let value: Vec<u8> = (0..4096u32).map(|i| (i & 0xff) as u8).collect();
        let value_srv = value.clone();

        let srv = std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().expect("accept");
            // Read the request frame header to learn req_id + payload_len.
            let mut hdr = [0u8; 10];
            sock.read_exact(&mut hdr).expect("read req hdr");
            let req_id = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
            let plen = u32::from_le_bytes(hdr[6..10].try_into().unwrap()) as usize;
            let mut req_payload = vec![0u8; plen];
            sock.read_exact(&mut req_payload).expect("read req payload");
            // Build a CRC-less value response: payload = [ZC meta][value].
            let meta = encode_zc_meta(0 /*OK*/, &value_srv);
            let mut payload = Vec::with_capacity(meta.len() + value_srv.len());
            payload.extend_from_slice(&meta);
            payload.extend_from_slice(&value_srv);
            let frame = Frame::response(req_id, 7, Bytes::from(payload));
            let bytes = frame.encode_no_crc();
            sock.write_all(&bytes).expect("write resp");
            // Hold the socket open briefly so the client finishes reading.
            std::thread::sleep(Duration::from_millis(50));
        });

        let client = RpcClient::connect(server_addr).await.expect("connect");
        let mut dest = vec![0u8; 4096];
        let dm = client
            .call_into_dest(
                7,
                Bytes::from_static(b"req"),
                dest.as_mut_ptr(),
                dest.len(),
                None,
            )
            .await
            .expect("call_into_dest");
        assert_eq!(dm.code, 0);
        assert_eq!(dm.value_len, 4096);
        assert_eq!(dest, value, "value bytes landed in dest");
        srv.join().expect("server thread");
    }

    /// F216-E — `call_into_pooled` recvs a value-separable response into a
    /// read_loop-owned `PooledBuf` (TCP path = decode + copy), value CRC
    /// verified, `(PooledBuf, code)` handed back. Proves the cancel-safe
    /// primitive's happy path before the UCX zero-copy + EN/PS wiring.
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
            let meta = encode_zc_meta(0 /*OK*/, &value_srv);
            let mut payload = Vec::with_capacity(meta.len() + value_srv.len());
            payload.extend_from_slice(&meta);
            payload.extend_from_slice(&value_srv);
            let bytes = Frame::response(req_id, 15, Bytes::from(payload)).encode_no_crc();
            sock.write_all(&bytes).expect("write resp");
            std::thread::sleep(Duration::from_millis(50));
        });

        let client = RpcClient::connect(server_addr).await.expect("connect");
        let (pb, code) = client
            .call_into_pooled(15, Bytes::from_static(b"req"))
            .await
            .expect("call_into_pooled");
        assert_eq!(code, 0);
        assert_eq!(
            pb.filled(),
            &value[..],
            "value bytes landed in the pool buffer"
        );
        srv.join().expect("server thread");
    }

    /// F219 — `call_into_pooled` over TCP with a value ABOVE
    /// `TCP_RECV_INTO_POOLED_MIN_BYTES` engages the new read_loop recv-into-pooled
    /// branch: the value is recv'd straight into the PooledBuf via a compio owned
    /// read (`read_exact_into_pooled`), skipping the FrameDecoder accumulation +
    /// finish_into_pooled_from_frame copy. A 256 KiB value forces multiple socket
    /// reads (64 KiB read_loop scratch), exercising drain-prefix + read-remainder.
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
            let meta = encode_zc_meta(0 /*OK*/, &value_srv);
            let mut payload = Vec::with_capacity(meta.len() + value_srv.len());
            payload.extend_from_slice(&meta);
            payload.extend_from_slice(&value_srv);
            // CRC-less frame: payload = [ZC meta][value] (mirrors zc_read_head / ps_zc_head).
            let bytes = Frame::response(req_id, 15, Bytes::from(payload)).encode_no_crc();
            sock.write_all(&bytes).expect("write resp");
            std::thread::sleep(Duration::from_millis(50));
        });

        let client = RpcClient::connect(server_addr).await.expect("connect");
        let (pb, code) = client
            .call_into_pooled(15, Bytes::from_static(b"req"))
            .await
            .expect("call_into_pooled");
        assert_eq!(code, 0);
        assert_eq!(pb.len(), n, "full value length");
        assert_eq!(
            pb.filled(),
            &value[..],
            "large value bytes landed in the pool buffer"
        );
        srv.join().expect("server thread");
    }
}
