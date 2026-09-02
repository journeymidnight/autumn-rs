//! UCX endpoint = one `ucp_ep` to a peer. Implements `compio::io::AsyncRead`
//! and `AsyncWrite` over `ucp_stream_send_nbx` / `ucp_stream_recv_nbx`.
//!
//! ## Cancel safety
//!
//! `ucp_stream_send_nbx` requires the caller-supplied buffer to remain valid
//! until UCX fires the completion callback. compio's owned-buffer Future
//! model owns the buffer in the future itself, so a normal `await` keeps it
//! alive. **But** if the future is dropped mid-await (timeouts, `select!`,
//! task cancellation), Rust drops the buffer immediately — yet UCX still
//! holds a pointer.
//!
//! `InflightGuard` plugs that hole. When the future is dropped before the
//! callback has fired, the guard's `Drop` calls `ucp_request_cancel` and
//! then synchronously drives `ucp_worker_progress` until the callback runs
//! (which, in single-thread mode, the spawned progress task can't do for
//! us — we're on the same OS thread, holding it). UCX guarantees the
//! cancel callback fires within bounded progress iterations. Once it has
//! fired, UCX no longer holds the buffer pointer, so it's safe for Rust
//! to free the buffer.
//!
//! ## EP lifetime
//!
//! `*mut ucp_ep` lives inside `Rc<UcxEp>` so `UcxConn` and the two halves
//! produced by `into_split` can share ownership. When the last `Rc` drops,
//! `UcxEp::Drop` simply releases the Rust-side handle. We deliberately
//! do **not** call `ucp_ep_close_nbx`:
//!
//! * With the default `UCP_ERR_HANDLING_MODE_NONE` (set in `connect` /
//!   `accept`), `ucp_ep_close_nbx(ep, FORCE)` is rejected with
//!   `UCS_ERR_INVALID_PARAM (-5)` and the EP isn't actually closed — so
//!   the call is a noisy no-op.
//! * `ucp_ep_close_nbx(ep, /*FLUSH*/ NULL)` *is* accepted, but a
//!   loopback close (both peers in the same compio runtime) deadlocks:
//!   each side's blocking close waits for an ack the other side can't
//!   deliver until *its* close runs. Bounded waits time out, but only
//!   after 30 s+ of UCX-internal retries.
//! * Switching to `UCP_ERR_HANDLING_MODE_PEER` (which would let FORCE
//!   succeed) enables UCX's RC-layer dead-peer detection. Under high
//!   fan-out write load (16 client threads × 16 inflight against 8 PS
//!   workers) the rc_mlx5 RNR/timeout fires *mid-test* and tears down
//!   live EPs, which is much worse than a teardown warning.
//!
//! Skipping the close call means each EP stays allocated in its UCX
//! worker context until the worker is destroyed (`ucp_worker_destroy`,
//! invoked when the OS thread exits or the process exits). For our
//! deployment this is fine: connection pools (autumn-stream::conn_pool,
//! the manager's RpcConn map) reuse one EP per (peer-address, thread)
//! pair for the life of the process, so EPs do not accumulate. Long-
//! running servers reclaim them on normal shutdown via worker destroy.

use crate::ucx::ffi::*;
use crate::ucx::sockaddr;
use crate::ucx::worker::{ucs_err, with_thread_ctx};
use compio::buf::{IoBuf, IoBufMut, IoVectoredBuf};
use compio::BufResult;
use std::cell::Cell;
use std::io;
use std::mem::ManuallyDrop;
use std::net::SocketAddr;
use std::os::raw::c_void;
use std::ptr;
use std::rc::Rc;

/// In-line `ucp_worker_progress` spin count in the send/recv hot path before
/// falling back to the eventfd-driven `progress_loop` task.
///
/// **Why:** the eventfd path costs ~1 io_uring `POLL_ADD` round trip
/// (~0.3–0.5 μs per direction) on the critical RTT path. For RoCE-loopback
/// and short cross-node RC, completion typically lands within a handful of
/// `ucp_worker_progress` iterations (~10 ns each), so a brief inline spin
/// avoids that kernel transition entirely. Long-tail completions still fall
/// through to the eventfd wait, capping CPU burn at ~`SPIN_ITERS × 10ns` per
/// in-flight op.
///
/// 32 ≈ 0.2–0.5 μs at ~10–20 ns/iter — chosen empirically. Larger values
/// (tested 256) regressed RTT because (a) `ucp_worker_progress` with
/// multiple TLs registered is closer to ~100 ns/iter than 10 ns, and
/// (b) the receive-side completion in a ping-pong waits on the *peer's*
/// eventfd-driven progress wake-up — no amount of local spin shortens
/// that. Send-completion (local NIC ACK) lands within ~10–20 iters; the
/// spin should be just long enough to catch that and no longer.
const SPIN_ITERS: usize = 32;

// ---- Pointer-status decoder ----

/// UCS uses a fat-pointer convention: NULL = immediate OK, a real heap
/// pointer = request in progress, or a `(void*)(intptr_t)UCS_ERR_*` =
/// immediate error (mirrors `UCS_PTR_IS_ERR` from `ucs/type/status.h`).
fn classify_ptr(ptr: *mut c_void) -> PtrStatus {
    if ptr.is_null() {
        PtrStatus::Done
    } else {
        let s = ptr as isize;
        if (-100..0).contains(&s) {
            PtrStatus::Err(s as ucs_status_t::Type)
        } else {
            PtrStatus::Pending(ptr)
        }
    }
}

enum PtrStatus {
    Done,
    Err(ucs_status_t::Type),
    Pending(*mut c_void),
}

// ---- Owned EP (drops via FORCE close) ----

/// Single-owner wrapper around `*mut ucp_ep`. Cloned via `Rc` between
/// `UcxConn` / `UcxReadHalf` / `UcxWriteHalf` so the EP outlives whichever
/// half-handle is dropped last.
pub(crate) struct UcxEp {
    raw: *mut ucp_ep,
}

impl UcxEp {
    fn new(raw: *mut ucp_ep) -> Rc<Self> {
        Rc::new(Self { raw })
    }
    fn ptr(&self) -> *mut ucp_ep {
        self.raw
    }
}

// ---- Pooled completion slot ----
//
// Replaces the prior per-op `(oneshot::channel + Rc<Cell<bool>> +
// Box<SendUserData/RecvUserData>)` triple — that path did 3 heap allocs and
// 3 frees per send/recv (oneshot, Rc, Box), which dominated bench profiles
// for small msgs (each `ucp_stream_send_nbx` itself is sub-µs).
//
// `Slot` is a single-shot completion bundle: state flag + status + length +
// optional waker. All `Cell`-based — UCX is `THREAD_MODE_SINGLE`, callbacks
// fire synchronously inside `ucp_worker_progress` on the worker's owning
// thread, which is also the compio thread our future polls on, so there is
// no cross-thread access.
//
// Slots live in a thread-local freelist (capped to bound memory). Acquire
// pops or allocates; release resets and pushes (or drops if pool full).
struct Slot {
    /// 0 = pending, 1 = completed.
    state: Cell<u8>,
    /// `ucs_status_t::Type` written by the cb.
    status: Cell<ucs_status_t::Type>,
    /// Bytes received (recv path only; send path leaves it 0).
    length: Cell<usize>,
    /// Waker registered by the polling future when it suspends. The cb
    /// takes it (replace with None) and wakes after setting state.
    waker: Cell<Option<std::task::Waker>>,
}

impl Slot {
    fn new() -> Self {
        Self {
            state: Cell::new(0),
            status: Cell::new(0),
            length: Cell::new(0),
            waker: Cell::new(None),
        }
    }
    fn reset(&self) {
        self.state.set(0);
        self.status.set(0);
        self.length.set(0);
        self.waker.set(None);
    }
    fn is_done(&self) -> bool {
        self.state.get() == 1
    }
    /// Cb path: store status + flip state + wake any registered waker.
    /// Order matters: state flip BEFORE wake, so a re-poll after wake sees
    /// `is_done() == true`.
    fn complete_send(&self, status: ucs_status_t::Type) {
        self.status.set(status);
        self.state.set(1);
        if let Some(w) = self.waker.replace(None) {
            w.wake();
        }
    }
    fn complete_recv(&self, status: ucs_status_t::Type, length: usize) {
        self.status.set(status);
        self.length.set(length);
        self.state.set(1);
        if let Some(w) = self.waker.replace(None) {
            w.wake();
        }
    }
}

thread_local! {
    static SLOT_POOL: std::cell::RefCell<Vec<Box<Slot>>> =
        std::cell::RefCell::new(Vec::with_capacity(64));
}

/// Cap on pool size — beyond this, surplus slots are dropped on release.
/// 256 covers per-thread peak in-flight (autumn-rpc pool + stream_worker).
const SLOT_POOL_CAP: usize = 256;

fn slot_acquire() -> *mut Slot {
    SLOT_POOL.with(|p| {
        if let Some(slot) = p.borrow_mut().pop() {
            Box::into_raw(slot)
        } else {
            Box::into_raw(Box::new(Slot::new()))
        }
    })
}

/// SAFETY: caller must own the slot (it must have been returned by a prior
/// `slot_acquire` and not already released, and UCX must no longer hold the
/// pointer — i.e. the cb has fired).
unsafe fn slot_release(slot: *mut Slot) {
    SLOT_POOL.with(|p| {
        let mut p = p.borrow_mut();
        let b = Box::from_raw(slot);
        if p.len() < SLOT_POOL_CAP {
            b.reset();
            p.push(b);
        } else {
            drop(b);
        }
    });
}

/// Future that resolves once `slot.is_done()` becomes true.
struct WaitSlot {
    slot: *const Slot,
}

impl std::future::Future for WaitSlot {
    type Output = ();
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        // SAFETY: the slot pointer is owned by the parent function (held in
        // an `InflightSlot` in the same scope), so it's live for the whole
        // future.
        let slot = unsafe { &*self.slot };
        if slot.is_done() {
            return std::task::Poll::Ready(());
        }
        // Register waker BEFORE re-checking state to avoid the race where
        // the cb fires between our first check and the registration.
        slot.waker.set(Some(cx.waker().clone()));
        if slot.is_done() {
            slot.waker.set(None);
            return std::task::Poll::Ready(());
        }
        std::task::Poll::Pending
    }
}

// ---- Cancel-safe in-flight guard ----

/// Owns the in-flight `(request, slot)` pair. Drops the pair on:
///
///   * normal return — slot already completed by the cb; drop frees the
///     UCX request and returns the slot to the pool.
///   * early drop (timeout / cancel / select! loser) — slot not yet
///     completed; we issue `ucp_request_cancel` then synchronously drive
///     `ucp_worker_progress` until the cancel callback fires (UCX is
///     thread-local; the spawned progress task can't run while we hold
///     the OS thread). Once `is_done()` flips, UCX is done with the
///     buffer pointer and Rust can safely drop the buffer.
struct InflightSlot {
    request: *mut c_void,
    worker: *mut ucp_worker,
    slot: *mut Slot,
    cleaned_up: Cell<bool>,
}

impl Drop for InflightSlot {
    fn drop(&mut self) {
        if self.cleaned_up.replace(true) {
            return;
        }
        unsafe {
            if !(*self.slot).is_done() {
                ucp_request_cancel(self.worker, self.request);
                // Bounded in practice — UCX queues the cancel callback on the
                // next progress drain; empirically <1k iters on rc_mlx5.
                // Cap as a safety net; on overflow we leak the slot rather
                // than hang.
                let mut iters = 0;
                const SPIN_CAP: usize = 100_000;
                while !(*self.slot).is_done() && iters < SPIN_CAP {
                    ucp_worker_progress(self.worker);
                    iters += 1;
                }
                if !(*self.slot).is_done() {
                    tracing::warn!(
                        iters,
                        "ucp_request_cancel did not complete in {SPIN_CAP} progress iters; \
                         leaking slot + request (UCX still owns the buffer pointer)"
                    );
                    return;
                }
            }
            ucp_request_free(self.request);
            slot_release(self.slot);
        }
    }
}

// ---- Send / recv callbacks ----

unsafe extern "C" fn cb_send(_request: *mut c_void, status: ucs_status_t::Type, user: *mut c_void) {
    let slot = &*(user as *const Slot);
    slot.complete_send(status);
}

unsafe extern "C" fn cb_recv(
    _request: *mut c_void,
    status: ucs_status_t::Type,
    length: usize,
    user: *mut c_void,
) {
    let slot = &*(user as *const Slot);
    slot.complete_recv(status, length);
}

// ---- Connection ----

pub struct UcxConn {
    ep: Rc<UcxEp>,
    peer: SocketAddr,
}

impl UcxConn {
    pub(crate) async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let ep = with_thread_ctx(|ctx| -> io::Result<*mut ucp_ep> {
            let (sa, sa_len) = sockaddr::to_storage(&addr);
            let mut params: ucp_ep_params_t = unsafe { std::mem::zeroed() };
            params.field_mask = (ucp_ep_params_field::UCP_EP_PARAM_FIELD_FLAGS
                | ucp_ep_params_field::UCP_EP_PARAM_FIELD_SOCK_ADDR)
                as u64;
            params.flags = ucp_ep_params_flags_field::UCP_EP_PARAMS_FLAGS_CLIENT_SERVER as u32;
            params.sockaddr.addr = &sa as *const _ as *const _;
            params.sockaddr.addrlen = sa_len;
            let mut ep: *mut ucp_ep = ptr::null_mut();
            let st = unsafe { ucp_ep_create(ctx.worker, &params, &mut ep) };
            if st != ucs_status_t::UCS_OK {
                return Err(ucs_err(st, "ucp_ep_create"));
            }
            // `sa` is borrowed by `params.sockaddr.addr` via &sa; the borrow
            // keeps it live across the FFI call without needing black_box.
            Ok(ep)
        })?;
        let ep = UcxEp::new(ep);
        ucx_flush(ep.ptr()).await?;
        Ok(Self { ep, peer: addr })
    }

    /// Internal constructor used by `UcxListener::accept` after `ucp_ep_create`
    /// has already been driven from a `ucp_conn_request_h`.
    pub(crate) fn from_raw_ep(ep: *mut ucp_ep, peer: SocketAddr) -> Self {
        Self {
            ep: UcxEp::new(ep),
            peer,
        }
    }
}

impl UcxConn {
    pub(crate) fn peer_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.peer)
    }

    pub(crate) fn into_split(self) -> (UcxReadHalf, UcxWriteHalf) {
        let ep = self.ep;
        (
            UcxReadHalf {
                ep: ep.clone(),
                peer: self.peer,
            },
            UcxWriteHalf {
                ep,
                peer: self.peer,
            },
        )
    }
}

// ---- AsyncRead / AsyncWrite on the joined UcxConn ----

impl compio::io::AsyncRead for UcxConn {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
        ucx_recv(self.ep.ptr(), buf).await
    }
}

impl compio::io::AsyncWrite for UcxConn {
    async fn write<B: IoBuf>(&mut self, buf: B) -> BufResult<usize, B> {
        ucx_send(self.ep.ptr(), buf).await
    }
    async fn write_vectored<T: IoVectoredBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        ucx_send_vectored(self.ep.ptr(), buf).await
    }
    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// ---- Half types ----

pub struct UcxReadHalf {
    ep: Rc<UcxEp>,
    #[allow(dead_code)]
    peer: SocketAddr,
}

pub struct UcxWriteHalf {
    ep: Rc<UcxEp>,
    #[allow(dead_code)]
    peer: SocketAddr,
}

impl compio::io::AsyncRead for UcxReadHalf {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
        ucx_recv(self.ep.ptr(), buf).await
    }
}

impl UcxReadHalf {
    /// zero-copy recv: `ucp_stream_recv_nbx` into `buf` with the
    /// registered region's `memh` passed via `UCP_OP_ATTR_FIELD_MEMH`, so UCX
    /// can RDMA directly into it (zero-copy) instead of the default copy-out
    /// (bounce buffer → memcpy). Single recv — may return < buf.len(); the
    /// caller loops for read_exact semantics. `buf` must lie inside `reg`'s
    /// registered range.
    pub async fn recv_registered(
        &mut self,
        buf: &mut [u8],
        reg: &crate::RegisteredMem,
    ) -> io::Result<usize> {
        ucx_recv_raw(
            self.ep.ptr(),
            buf.as_mut_ptr() as *mut c_void,
            buf.len(),
            reg.memh(),
        )
        .await
    }

    /// Recv into a borrowed slice WITHOUT a memh (copy-out path). Used when the
    /// dest buffer is the regpool over-cap unregistered fallback. Single recv.
    pub async fn recv_unregistered(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        ucx_recv_raw(
            self.ep.ptr(),
            buf.as_mut_ptr() as *mut c_void,
            buf.len(),
            std::ptr::null_mut(),
        )
        .await
    }
}

impl compio::io::AsyncWrite for UcxWriteHalf {
    async fn write<B: IoBuf>(&mut self, buf: B) -> BufResult<usize, B> {
        ucx_send(self.ep.ptr(), buf).await
    }
    async fn write_vectored<T: IoVectoredBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        ucx_send_vectored(self.ep.ptr(), buf).await
    }
    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// ---- Internals ----

/// Await a `PtrStatus::Pending` nbx request: install the cancel-safe
/// `InflightSlot` guard, inline-spin the worker up to `spin_iters` (0 = no
/// spin) so fast completions skip the eventfd wakeup, then park on the slot.
/// Returns the callback's `(status, length)`; the guard is dropped before
/// returning so the slot is back in the pool when the caller chains ops.
async fn await_pending(
    req: *mut c_void,
    slot: *mut Slot,
    spin_iters: usize,
) -> (ucs_status_t::Type, usize) {
    let worker = with_thread_ctx(|c| c.worker);
    // Guard owns slot + request from here; its Drop frees both on normal
    // return AND on early-drop (cancel path).
    let guard = InflightSlot {
        request: req,
        worker,
        slot,
        cleaned_up: Cell::new(false),
    };
    for _ in 0..spin_iters {
        if unsafe { (*slot).is_done() } {
            break;
        }
        unsafe {
            ucp_worker_progress(worker);
        }
    }
    // Wait for completion (resolves immediately if the spin caught it).
    WaitSlot {
        slot: slot as *const Slot,
    }
    .await;
    let status = unsafe { (*slot).status.get() };
    let length = unsafe { (*slot).length.get() };
    drop(guard);
    (status, length)
}

async fn ucx_send<B: IoBuf>(ep: *mut ucp_ep, buf: B) -> BufResult<usize, B> {
    let buf = ManuallyDrop::new(buf);
    let len = buf.buf_len();
    if len == 0 {
        return BufResult(Ok(0), ManuallyDrop::into_inner(buf));
    }
    let ptr = buf.buf_ptr();

    let slot = slot_acquire();

    let mut params: ucp_request_param_t = unsafe { std::mem::zeroed() };
    params.op_attr_mask = (ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK
        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA) as u32;
    params.cb.send = Some(cb_send);
    params.user_data = slot as *mut c_void;

    let r = unsafe { ucp_stream_send_nbx(ep, ptr as *const c_void, len, &params) };
    match classify_ptr(r) {
        PtrStatus::Done => {
            unsafe { slot_release(slot) };
            BufResult(Ok(len), ManuallyDrop::into_inner(buf))
        }
        PtrStatus::Err(st) => {
            unsafe { slot_release(slot) };
            BufResult(
                Err(ucs_err(st, "ucp_stream_send_nbx")),
                ManuallyDrop::into_inner(buf),
            )
        }
        PtrStatus::Pending(req) => {
            let (status, _) = await_pending(req, slot, SPIN_ITERS).await;
            if status == ucs_status_t::UCS_OK {
                BufResult(Ok(len), ManuallyDrop::into_inner(buf))
            } else {
                BufResult(
                    Err(ucs_err(status, "ucp_stream_send cb")),
                    ManuallyDrop::into_inner(buf),
                )
            }
        }
    }
}

/// Vectored send — ONE `ucp_stream_send_nbx` for the whole buffer list, via
/// `UCP_DATATYPE_IOV`.
///
/// Without this the trait default applies, and it sends only the FIRST
/// non-empty buffer, so `write_vectored_all` walks the list one send at a
/// time: a 256-op batched write left here as 259 separate sends. TCP has had
/// the sendmsg-with-N-iovecs path all along; this is the UCX counterpart.
///
/// UCX reads the iov ARRAY itself — not just the payloads it points at — for
/// the duration of the operation, so the array follows the SAME rule the
/// payload buffers do: held in `ManuallyDrop`, freed only on a path that
/// reached completion. On `InflightSlot`'s cancel-overflow path the guard
/// returns with UCX still owning every pointer it was handed, and a plain local
/// would be freed there while UCX may still read it.
async fn ucx_send_vectored<T: IoVectoredBuf>(ep: *mut ucp_ep, buf: T) -> BufResult<usize, T> {
    let buf = ManuallyDrop::new(buf);
    let mut iov: Vec<ucp_dt_iov_t> = Vec::new();
    let mut total = 0usize;
    for s in buf.iter_slice() {
        if s.is_empty() {
            continue;
        }
        total += s.len();
        iov.push(ucp_dt_iov_t {
            buffer: s.as_ptr() as *mut c_void,
            length: s.len(),
        });
    }
    if total == 0 {
        return BufResult(Ok(0), ManuallyDrop::into_inner(buf));
    }
    let iov = ManuallyDrop::new(iov);

    let slot = slot_acquire();

    let mut params: ucp_request_param_t = unsafe { std::mem::zeroed() };
    params.op_attr_mask = (ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK
        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA
        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_DATATYPE) as u32;
    params.cb.send = Some(cb_send);
    params.user_data = slot as *mut c_void;
    params.datatype = ucp_dt_type::UCP_DATATYPE_IOV as ucp_datatype_t;

    // With UCP_DATATYPE_IOV the "buffer" is the iov array and the "count" is
    // the number of entries, not a byte length.
    let r = unsafe { ucp_stream_send_nbx(ep, iov.as_ptr() as *const c_void, iov.len(), &params) };
    // Every arm below is reached only once UCX is DONE with the operation
    // (`await_pending` returns only when the slot completes; a mid-await drop
    // unwinds through the guard and never gets here), so freeing the array is
    // safe here — and NOT freeing it on the drop path is the point.
    match classify_ptr(r) {
        PtrStatus::Done => {
            unsafe { slot_release(slot) };
            drop(ManuallyDrop::into_inner(iov));
            BufResult(Ok(total), ManuallyDrop::into_inner(buf))
        }
        PtrStatus::Err(st) => {
            unsafe { slot_release(slot) };
            drop(ManuallyDrop::into_inner(iov));
            BufResult(
                Err(ucs_err(st, "ucp_stream_send_nbx(iov)")),
                ManuallyDrop::into_inner(buf),
            )
        }
        PtrStatus::Pending(req) => {
            let (status, _) = await_pending(req, slot, SPIN_ITERS).await;
            drop(ManuallyDrop::into_inner(iov));
            if status == ucs_status_t::UCS_OK {
                BufResult(Ok(total), ManuallyDrop::into_inner(buf))
            } else {
                BufResult(
                    Err(ucs_err(status, "ucp_stream_send(iov) cb")),
                    ManuallyDrop::into_inner(buf),
                )
            }
        }
    }
}

pub(crate) async fn ucx_flush(ep: *mut ucp_ep) -> io::Result<()> {
    let slot = slot_acquire();

    let mut params: ucp_request_param_t = unsafe { std::mem::zeroed() };
    params.op_attr_mask = (ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK
        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA) as u32;
    params.cb.send = Some(cb_send);
    params.user_data = slot as *mut c_void;

    let r = unsafe { ucp_ep_flush_nbx(ep, &params) };
    match classify_ptr(r) {
        PtrStatus::Done => {
            unsafe { slot_release(slot) };
            Ok(())
        }
        PtrStatus::Err(st) => {
            unsafe { slot_release(slot) };
            Err(ucs_err(st, "ucp_ep_flush_nbx"))
        }
        PtrStatus::Pending(req) => {
            let (status, _) = await_pending(req, slot, 0).await;
            if status == ucs_status_t::UCS_OK {
                Ok(())
            } else {
                Err(ucs_err(status, "ucp_ep_flush cb"))
            }
        }
    }
}

/// Raw recv into a borrowed buffer (ptr+cap), optionally with a registered
/// `memh` for zero-copy receive. `memh.is_null()` → default copy-out path
/// (identical to `ucx_recv`). Returns bytes received. Cancel-safe: the
/// `InflightSlot` guard drains UCX before returning; the buffer is borrowed
/// by the caller (no ManuallyDrop leak on cancel).
async fn ucx_recv_raw(
    ep: *mut ucp_ep,
    ptr: *mut c_void,
    cap: usize,
    memh: ucp_mem_h,
) -> io::Result<usize> {
    if cap == 0 {
        return Ok(0);
    }
    let slot = slot_acquire();

    let mut params: ucp_request_param_t = unsafe { std::mem::zeroed() };
    let mut mask = ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK
        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA
        | ucp_op_attr_t::UCP_OP_ATTR_FLAG_NO_IMM_CMPL;
    if !memh.is_null() {
        // Zero-copy receive: tell UCX the dest is registered so it RDMAs
        // straight in (no bounce buffer / copy-out).
        mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_MEMH;
        params.memh = memh;
    }
    params.op_attr_mask = mask as u32;
    params.cb.recv_stream = Some(cb_recv);
    params.user_data = slot as *mut c_void;

    let mut got: usize = 0;
    let r = unsafe { ucp_stream_recv_nbx(ep, ptr, cap, &mut got, &params) };
    match classify_ptr(r) {
        PtrStatus::Done => {
            unsafe { slot_release(slot) };
            Ok(got)
        }
        PtrStatus::Err(st) => {
            unsafe { slot_release(slot) };
            Err(ucs_err(st, "ucp_stream_recv_nbx"))
        }
        PtrStatus::Pending(req) => {
            let (status, n) = await_pending(req, slot, SPIN_ITERS).await;
            if status == ucs_status_t::UCS_OK {
                Ok(n)
            } else {
                Err(ucs_err(status, "ucp_stream_recv cb"))
            }
        }
    }
}

/// Recv into an owned compio buffer (copy-out path, no memh) — a thin wrapper
/// over `ucx_recv_raw`. On cancel the `ManuallyDrop` deliberately leaks the
/// buffer: the guard's drain normally makes freeing safe, but its SPIN_CAP
/// bail-out can return while UCX still holds the pointer, so leaking is the
/// only always-safe choice for an owned buffer.
async fn ucx_recv<B: IoBufMut>(ep: *mut ucp_ep, buf: B) -> BufResult<usize, B> {
    let mut buf = ManuallyDrop::new(buf);
    let cap = buf.buf_capacity();
    if cap == 0 {
        return BufResult(Ok(0), ManuallyDrop::into_inner(buf));
    }
    let ptr = buf.as_uninit().as_mut_ptr() as *mut c_void;
    match ucx_recv_raw(ep, ptr, cap, ptr::null_mut()).await {
        Ok(n) => {
            unsafe { buf.set_len(n) };
            BufResult(Ok(n), ManuallyDrop::into_inner(buf))
        }
        Err(e) => BufResult(Err(e), ManuallyDrop::into_inner(buf)),
    }
}
