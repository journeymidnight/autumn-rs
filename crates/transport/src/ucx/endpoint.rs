//! UCX endpoint = one `ucp_ep` to a peer. Implements `compio::io::AsyncRead`
//! and `AsyncWrite` over `ucp_stream_send_nbx` / `ucp_stream_recv_nbx`.
//!
//! ## Cancel safety
//!
//! A stream send or recv hands UCX a buffer pointer that stays live until the
//! completion callback fires, and a stream operation cannot be cancelled:
//! `ucp_request_cancel` handles tag receives only (ucp_request.c), a posted
//! stream recv leaves the ep's queue only by completing or when the ep is
//! destroyed (`ucp_stream_ep_cleanup`). So when a future is dropped mid-await
//! (timeouts, `select!`, task teardown) the operation is still in flight, and
//! every buffer it uses is owned by the future that was just dropped.
//!
//! `Inflight` therefore owns the operation's buffers, and its `Drop` before
//! completion moves request, slot and buffers to this thread's orphan list
//! (`reap_orphans`, run by the progress loop) instead of freeing anything; a
//! pool slab goes back to its pool when UCX is done with it, so cancel costs
//! no registration. This is why every receive takes its buffer by value.
//!
//! An orphaned recv still takes the connection's next bytes, which no later
//! recv can get back, so the ep is marked and every later recv on it fails.
//!
//! ## EP lifetime and peer failure
//!
//! Every endpoint is created with `UCP_ERR_HANDLING_MODE_PEER`
//! (`peer_error_handling`). When the peer closes its endpoint or dies, UCX
//! completes every request on ours with an error (sends, stream recvs, a
//! rendezvous receive under way) and calls `on_ep_error`, which fails the frame
//! values still awaited from that peer (`am::ep_failed`); nothing waits on a
//! dead peer for longer than UCX takes to notice it.
//!
//! `*mut ucp_ep` lives inside `Rc<UcxEp>` so `UcxConn` and the two halves
//! produced by `into_split` share it. The last drop closes it with
//! `UCP_EP_CLOSE_FLAG_FORCE`: requests still on it (orphans, below) complete
//! as cancelled — posted stream recvs inside the close call, sends once UCX has
//! discarded the lanes they use — and the peer sees the close.
//! A flushing close would wait for the peer (two ends in one runtime would wait
//! for each other), and FORCE is refused without PEER mode. Once closed, its
//! address may be reused for a new endpoint, so nothing keyed by it outlives
//! the drop.

use crate::ucx::ffi::*;
use crate::ucx::sockaddr;
use crate::ucx::worker::{ucs_err, with_thread_ctx};
use crate::PooledBuf;
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
pub(crate) const SPIN_ITERS: usize = 32;

// ---- Pointer-status decoder ----

/// UCS uses a fat-pointer convention: NULL = immediate OK, a real heap
/// pointer = request in progress, or a `(void*)(intptr_t)UCS_ERR_*` =
/// immediate error (mirrors `UCS_PTR_IS_ERR` from `ucs/type/status.h`).
pub(crate) fn classify_ptr(ptr: *mut c_void) -> PtrStatus {
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

pub(crate) enum PtrStatus {
    Done,
    Err(ucs_status_t::Type),
    Pending(*mut c_void),
}

// ---- Owned EP ----

/// Single-owner wrapper around `*mut ucp_ep`. Cloned via `Rc` between
/// `UcxConn` / `UcxReadHalf` / `UcxWriteHalf` so the EP outlives whichever
/// half-handle is dropped last.
pub(crate) struct UcxEp {
    raw: *mut ucp_ep,
    /// A recv on this ep was dropped before it completed. It stays posted and
    /// takes the next bytes the peer sends, so no later recv can be trusted.
    recv_abandoned: Cell<bool>,
}

impl UcxEp {
    fn new(raw: *mut ucp_ep) -> Rc<Self> {
        Rc::new(Self {
            raw,
            recv_abandoned: Cell::new(false),
        })
    }
    pub(crate) fn ptr(&self) -> *mut ucp_ep {
        self.raw
    }
    #[inline]
    fn check_readable(&self) -> io::Result<()> {
        if self.recv_abandoned.get() {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "a receive on this UCX connection was dropped while in flight; it still \
                 takes the next bytes, so the connection cannot be read any further",
            ));
        }
        Ok(())
    }
}

impl Drop for UcxEp {
    /// Releases the frame values held for it, so their senders complete, and
    /// closes it (see "EP lifetime and peer failure").
    fn drop(&mut self) {
        crate::ucx::am::forget_ep(self.raw);
        let mut p: ucp_request_param_t = unsafe { std::mem::zeroed() };
        p.op_attr_mask = ucp_op_attr_t::UCP_OP_ATTR_FIELD_FLAGS as u32;
        p.flags = ucp_ep_close_flags_t::UCP_EP_CLOSE_FLAG_FORCE as u32;
        match classify_ptr(unsafe { ucp_ep_close_nbx(self.raw, &p) }) {
            PtrStatus::Done => {}
            // A FORCE close returns no request (UCX 1.20); freed if one does.
            PtrStatus::Pending(req) => unsafe { ucp_request_free(req) },
            PtrStatus::Err(st) => tracing::warn!(status = st, "ucp_ep_close_nbx failed"),
        }
        // The lanes are discarded from worker progress, which UCX schedules
        // without waking an idle worker: until then the peer is not told.
        let worker = crate::ucx::am::this_worker();
        if !worker.is_null() {
            unsafe { ucp_worker_signal(worker) };
        }
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
pub(crate) struct Slot {
    /// 0 = pending, 1 = completed.
    state: Cell<u8>,
    /// `ucs_status_t::Type` written by the cb.
    status: Cell<ucs_status_t::Type>,
    /// Bytes received (recv path only; send path leaves it 0).
    length: Cell<usize>,
    /// Waker registered by the polling future when it suspends. The cb
    /// takes it (replace with None) and wakes after setting state.
    waker: Cell<Option<std::task::Waker>>,
    /// The owning future was dropped; the completion counts for `reap_orphans`.
    orphaned: Cell<bool>,
}

impl Slot {
    fn new() -> Self {
        Self {
            state: Cell::new(0),
            status: Cell::new(0),
            length: Cell::new(0),
            waker: Cell::new(None),
            orphaned: Cell::new(false),
        }
    }
    fn reset(&self) {
        self.state.set(0);
        self.status.set(0);
        self.length.set(0);
        self.waker.set(None);
        self.orphaned.set(false);
    }
    fn is_done(&self) -> bool {
        self.state.get() == 1
    }
    /// Cb path: store status + flip state + wake any registered waker.
    /// Order matters: state flip BEFORE wake, so a re-poll after wake sees
    /// `is_done() == true`.
    fn complete_send(&self, status: ucs_status_t::Type) {
        self.status.set(status);
        self.complete();
    }
    fn complete_recv(&self, status: ucs_status_t::Type, length: usize) {
        self.status.set(status);
        self.length.set(length);
        self.complete();
    }
    fn complete(&self) {
        self.state.set(1);
        if self.orphaned.get() {
            ORPHANS_DONE.with(|c| c.set(c.get() + 1));
        } else if let Some(w) = self.waker.replace(None) {
            w.wake();
        }
    }
}

thread_local! {
    /// How this thread's stream recvs that found their data already queued
    /// completed: unpacked in place by `ucp_stream_recv_nbx` (the only path the
    /// transport accepts), or through UCX's request callback before the call
    /// returned (never: that path reports a stale status and leaks the request
    /// on release builds up to 1.20.1, see `ucx_recv_guarded`). Read by
    /// `ucx_recv_counts`.
    pub(crate) static RECV_IN_PLACE: Cell<u64> = const { Cell::new(0) };
    pub(crate) static RECV_VIA_CALLBACK: Cell<u64> = const { Cell::new(0) };
}

thread_local! {
    static SLOT_POOL: std::cell::RefCell<Vec<Box<Slot>>> =
        std::cell::RefCell::new(Vec::with_capacity(64));
}

/// Cap on pool size — beyond this, surplus slots are dropped on release.
/// 256 covers per-thread peak in-flight (autumn-rpc pool + stream_worker).
const SLOT_POOL_CAP: usize = 256;

pub(crate) fn slot_acquire() -> *mut Slot {
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
pub(crate) unsafe fn slot_release(slot: *mut Slot) {
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
        // an `Inflight` in the same scope), so it's live for the whole
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

// ---- In-flight operation and its orphans ----

/// An in-flight nbx operation: its UCX request, completion slot, and every
/// buffer UCX may still touch (`payload`). The buffers stay in the caller's
/// frame, in a `ManuallyDrop` the guard only points at, so the hot path never
/// moves them. `finish` runs after the slot completed and frees request and
/// slot. `Drop` runs only on a future dropped mid-await: the operation cannot
/// be cancelled, so the buffers are read out of the caller's frame and move
/// with request and slot to the thread's orphan list, freed once the callback
/// has fired.
struct Inflight<'a, P: 'static> {
    request: *mut c_void,
    slot: *mut Slot,
    /// A local of the awaiting async fn, declared before the guard and alive
    /// until after it; the future is pinned, so the address holds.
    payload: *mut ManuallyDrop<P>,
    /// Set for a recv: dropping it leaves the ep unreadable.
    recv_on: Option<&'a UcxEp>,
}

impl<P: 'static> Inflight<'_, P> {
    /// SAFETY: the slot must be completed (UCX has released every pointer).
    unsafe fn finish(self) {
        debug_assert!((*self.slot).is_done());
        ucp_request_free(self.request);
        slot_release(self.slot);
        std::mem::forget(self);
    }
}

impl<P: 'static> Drop for Inflight<'_, P> {
    fn drop(&mut self) {
        // SAFETY: the caller's `ManuallyDrop` is still live (it is dropped
        // after this guard) and never read again after a drop mid-await.
        let payload = ManuallyDrop::into_inner(unsafe { ptr::read(self.payload) });
        unsafe {
            if (*self.slot).is_done() {
                ucp_request_free(self.request);
                slot_release(self.slot);
                return;
            }
            (*self.slot).orphaned.set(true);
        }
        if let Some(ep) = self.recv_on {
            ep.recv_abandoned.set(true);
        }
        ORPHANED.with(|c| c.set(c.get() + 1));
        tracing::debug!(
            op = if self.recv_on.is_some() { "recv" } else { "send" },
            "ucx operation dropped before completion; its buffers wait on the orphan list"
        );
        ORPHANS.with(|o| {
            o.borrow_mut().list.push(Orphan {
                request: self.request,
                slot: self.slot,
                payload: Box::new(payload),
            })
        });
    }
}

struct Orphan {
    request: *mut c_void,
    slot: *mut Slot,
    payload: Box<dyn std::any::Any>,
}

/// Dropped in-flight operations of this thread, freed by `reap_orphans` once
/// UCX has completed them. The thread's worker is never destroyed, so an
/// orphan still pending at thread exit stays allocated, as the worker does.
struct Orphans {
    list: Vec<Orphan>,
}

impl Drop for Orphans {
    fn drop(&mut self) {
        for o in self.list.drain(..) {
            if unsafe { (*o.slot).is_done() } {
                drop(o.payload);
            } else {
                std::mem::forget(o.payload);
            }
        }
    }
}

thread_local! {
    static ORPHANS: std::cell::RefCell<Orphans> = const { std::cell::RefCell::new(Orphans { list: Vec::new() }) };
    /// Orphans created / completed / freed on this thread, and list scans.
    /// `ORPHANS_DONE == ORPHANS_REAPED` is the "nothing to reap" fast path,
    /// so an orphan that never completes (its peer never sends again) costs
    /// later operations nothing. Read by `ucx_orphan_counts`.
    pub(crate) static ORPHANED: Cell<u64> = const { Cell::new(0) };
    static ORPHANS_DONE: Cell<u64> = const { Cell::new(0) };
    pub(crate) static ORPHANS_REAPED: Cell<u64> = const { Cell::new(0) };
    pub(crate) static ORPHAN_SCANS: Cell<u64> = const { Cell::new(0) };
}

/// Free every orphan whose callback has fired. Runs from the progress loop's
/// drain and from a pool `acquire`, which wants the slab back (the `self`
/// transport and the inline spin complete receives inside another call, with
/// no event for the loop). Nothing completed since the last reap costs two
/// thread-local reads.
#[inline]
pub(crate) fn reap_orphans() {
    if ORPHANS_DONE.get() == ORPHANS_REAPED.get() {
        return;
    }
    let done: Vec<Orphan> = ORPHANS.with(|o| {
        let mut o = o.borrow_mut();
        ORPHAN_SCANS.with(|c| c.set(c.get() + 1));
        let mut done = Vec::new();
        let mut i = 0;
        while i < o.list.len() {
            if unsafe { (*o.list[i].slot).is_done() } {
                done.push(o.list.swap_remove(i));
            } else {
                i += 1;
            }
        }
        done
    });
    for orphan in done {
        unsafe {
            ucp_request_free(orphan.request);
            slot_release(orphan.slot);
        }
        drop(orphan.payload);
        ORPHANS_REAPED.with(|c| c.set(c.get() + 1));
    }
}

/// A buffer whose bytes live inside the value itself (`[u8; N]`, `ArrayVec`,
/// `SmallVec` inline) cannot be guarded: `Inflight::drop` moves the value out
/// of the dying frame while UCX keeps the pointer. Refused before posting.
#[inline]
pub(crate) fn inline_storage<P>(payload: &ManuallyDrop<P>, ptr: *const c_void) -> bool {
    let start = payload as *const _ as usize;
    (start..start + std::mem::size_of::<P>()).contains(&(ptr as usize))
}

pub(crate) fn inline_storage_err() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "buffer with inline storage cannot be handed to UCX (a dropped operation could not keep it alive)",
    )
}

// ---- Send / recv callbacks ----

pub(crate) unsafe extern "C" fn cb_send(_request: *mut c_void, status: ucs_status_t::Type, user: *mut c_void) {
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

/// UCX found the peer gone: a close from its side (`CONNECTION_RESET`), a dial
/// that never connected, or a failure. Debug only: every request on the
/// endpoint fails with the status, and its caller reports it. Runs inside
/// worker progress.
unsafe extern "C" fn on_ep_error(_arg: *mut c_void, ep: ucp_ep_h, status: ucs_status_t::Type) {
    tracing::debug!(ep = ?ep, error = %ucs_err(status, "ucx endpoint failed"), "ucx endpoint failed");
    crate::ucx::am::ep_failed(ep, status);
}

/// UCX reports a peer that closed or failed instead of leaving requests on its
/// endpoint pending (see "EP lifetime and peer failure").
pub(crate) fn peer_error_handling(params: &mut ucp_ep_params_t) {
    params.field_mask |= (ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE
        | ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLER) as u64;
    params.err_mode = ucp_err_handling_mode_t::UCP_ERR_HANDLING_MODE_PEER;
    params.err_handler = ucp_err_handler { cb: Some(on_ep_error), arg: ptr::null_mut() };
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
            peer_error_handling(&mut params);
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

    /// The connection `UcxListener::accept` created `ep` for; closed if its
    /// flush fails.
    pub(crate) async fn accepted(ep: *mut ucp_ep, peer: SocketAddr) -> io::Result<Self> {
        let ep = UcxEp::new(ep);
        ucx_flush(ep.ptr()).await?;
        Ok(Self { ep, peer })
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
        ucx_recv(&self.ep, buf).await
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
        ucx_recv(&self.ep, buf).await
    }
}

thread_local! {
    /// Tasks in `recv_data_with` waiting for stream data on this thread's worker.
    static STREAM_WAITERS: std::cell::RefCell<Vec<std::task::Waker>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Wake every `recv_data_with` waiter; called after each progress pass.
pub(crate) fn wake_stream_waiters() {
    let waiters = STREAM_WAITERS.with(|w| std::mem::take(&mut *w.borrow_mut()));
    for w in waiters {
        w.wake();
    }
}

impl UcxReadHalf {
    pub(crate) fn ep(&self) -> &Rc<UcxEp> {
        &self.ep
    }

    /// Wait until stream data is queued on this connection, then hand each
    /// queued chunk to `f`, in order, and release it. Returns the bytes
    /// handed over. Nothing is posted to UCX, so a dropped call leaves no
    /// operation behind: the data `f` copies goes where `f` puts it, and only
    /// while `f` runs.
    pub async fn recv_data_with(&mut self, mut f: impl FnMut(&[u8])) -> io::Result<usize> {
        self.ep.check_readable()?;
        let ep = self.ep.ptr();
        std::future::poll_fn(|cx| {
            let mut total = 0;
            loop {
                let mut len = 0usize;
                let p = unsafe { ucp_stream_recv_data_nb(ep, &mut len) } as *mut c_void;
                match classify_ptr(p) {
                    PtrStatus::Done => break,
                    PtrStatus::Err(st) => return std::task::Poll::Ready(Err(ucs_err(st, "ucp_stream_recv_data_nb"))),
                    PtrStatus::Pending(data) => {
                        // SAFETY: UCX owns `len` bytes at `data` until the release.
                        f(unsafe { std::slice::from_raw_parts(data as *const u8, len) });
                        unsafe { ucp_stream_data_release(ep, data) };
                        total += len;
                    }
                }
            }
            if total > 0 {
                return std::task::Poll::Ready(Ok(total));
            }
            STREAM_WAITERS.with(|w| w.borrow_mut().push(cx.waker().clone()));
            std::task::Poll::Pending
        })
        .await
    }

    /// `ucp_stream_recv_nbx` into `pb`'s writable region from `filled` on. A
    /// registered slab passes its `memh` via `UCP_OP_ATTR_FIELD_MEMH`; Stream
    /// receive still unpacks the arrived AM data into the slab (see
    /// `ucx/mod.rs`), and receiving into the final buffer is what saves the
    /// application copy. Single recv — may fill less than the region; the
    /// caller loops for read_exact semantics.
    pub async fn recv_pooled(&mut self, pb: PooledBuf, filled: usize) -> BufResult<usize, PooledBuf> {
        ucx_recv_pooled(&self.ep, pb, filled).await
    }
}

impl UcxWriteHalf {
    pub(crate) fn ep(&self) -> &Rc<UcxEp> {
        &self.ep
    }

    /// A sender of tagged AM values on this connection (`crate::am`).
    pub fn am_sender(&self) -> crate::am::Sender {
        crate::am::Sender { ep: self.ep.clone() }
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

/// Await a `PtrStatus::Pending` nbx request whose buffers sit in the caller's
/// `payload` (see `Inflight`): inline-spin the worker up to `spin_iters` (0 =
/// no spin) so fast completions skip the eventfd wakeup, then park on the
/// slot. Returns the callback's `(status, length)`; request and slot are freed
/// before returning so the slot is back in the pool when the caller chains
/// ops, and the caller may then take its payload back.
pub(crate) async fn await_pending<'a, P: 'static>(
    req: *mut c_void,
    slot: *mut Slot,
    spin_iters: usize,
    payload: *mut ManuallyDrop<P>,
    recv_on: Option<&'a UcxEp>,
) -> (ucs_status_t::Type, usize) {
    let worker = with_thread_ctx(|c| c.worker);
    let inflight = Inflight {
        request: req,
        slot,
        payload,
        recv_on,
    };
    for _ in 0..spin_iters {
        if unsafe { (*slot).is_done() } {
            break;
        }
        unsafe {
            ucp_worker_progress(worker);
        }
    }
    // The spin may have queued stream data that a `recv_data_with` waits for.
    wake_stream_waiters();
    // Wait for completion (resolves immediately if the spin caught it).
    WaitSlot {
        slot: slot as *const Slot,
    }
    .await;
    let status = unsafe { (*slot).status.get() };
    let length = unsafe { (*slot).length.get() };
    unsafe { inflight.finish() };
    (status, length)
}

async fn ucx_send<B: IoBuf>(ep: *mut ucp_ep, buf: B) -> BufResult<usize, B> {
    let r = ucx_send_staged(ep, || std::iter::once(buf.as_init())).await;
    BufResult(r, buf)
}

/// Vectored send — the whole buffer list goes out in ONE stream send.
///
/// Without this the trait default applies, and it sends only the FIRST
/// non-empty buffer, so `write_vectored_all` walks the list one send at a
/// time: a 256-op batched write left here as 259 separate sends. TCP has had
/// the sendmsg-with-N-iovecs path all along; this is the UCX counterpart.
async fn ucx_send_vectored<T: IoVectoredBuf>(ep: *mut ucp_ep, buf: T) -> BufResult<usize, T> {
    let r = ucx_send_staged(ep, || buf.iter_slice()).await;
    BufResult(r, buf)
}

/// A stream send leaves from a registered pool slab, the parts copied in,
/// with the slab's `memh`. Handed the caller's memory, UCX sends 478 bytes and
/// up zero-copy and registers every buffer through its cache, which misses on
/// memory that turns over (SST blocks read on a cache miss, heap pages
/// jemalloc purged): each such send paid `ibv_reg_mr` and later
/// `ibv_dereg_mr`, and the partition threads queued on the registration lock.
/// The copy is what eager bcopy would cost; values of `BULK_MIN_BYTES` and up
/// go beside the stream as active messages, so the stream carries small parts.
async fn ucx_send_staged<'a, I: Iterator<Item = &'a [u8]>>(ep: *mut ucp_ep, parts: impl Fn() -> I) -> io::Result<usize> {
    let total: usize = parts().map(<[u8]>::len).sum();
    if total == 0 {
        return Ok(0);
    }
    let mut slab = crate::regpool_acquire(total);
    {
        let (dest, _) = slab.dest_and_reg();
        let mut at = 0;
        for p in parts() {
            dest[at..at + p.len()].copy_from_slice(p);
            at += p.len();
        }
    }
    let ptr = slab.filled().as_ptr();
    let memh = slab.reg().map_or(ptr::null_mut(), |r| r.memh());
    let mut payload = ManuallyDrop::new(slab);

    let slot = slot_acquire();

    let mut params: ucp_request_param_t = unsafe { std::mem::zeroed() };
    params.op_attr_mask = (ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK
        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA) as u32;
    params.cb.send = Some(cb_send);
    params.user_data = slot as *mut c_void;
    if !memh.is_null() {
        params.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_MEMH as u32;
        params.memh = memh;
    }

    let r = unsafe { ucp_stream_send_nbx(ep, ptr as *const c_void, total, &params) };
    let outcome = match classify_ptr(r) {
        PtrStatus::Done => {
            unsafe { slot_release(slot) };
            Ok(total)
        }
        PtrStatus::Err(st) => {
            unsafe { slot_release(slot) };
            Err(ucs_err(st, "ucp_stream_send_nbx"))
        }
        PtrStatus::Pending(req) => {
            let (status, _) = await_pending(req, slot, SPIN_ITERS, &mut payload, None).await;
            if status == ucs_status_t::UCS_OK {
                Ok(total)
            } else {
                Err(ucs_err(status, "ucp_stream_send cb"))
            }
        }
    };
    drop(ManuallyDrop::into_inner(payload));
    outcome
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
            let mut none = ManuallyDrop::new(());
            let (status, _) = await_pending(req, slot, 0, &mut none, None).await;
            if status == ucs_status_t::UCS_OK {
                Ok(())
            } else {
                Err(ucs_err(status, "ucp_ep_flush cb"))
            }
        }
    }
}

/// One stream recv into `cap` bytes at `ptr`, a region of the caller's `buf`,
/// which the operation guards until it completes (see `Inflight`; the caller
/// takes the buffer back only after this returns). `memh` names the region's
/// registration or is null; UCX Stream unpacks into the buffer either way.
/// Returns the bytes received, which may be fewer than `cap`.
async fn ucx_recv_guarded<P: 'static>(
    ep: &UcxEp,
    buf: &mut ManuallyDrop<P>,
    ptr: *mut c_void,
    cap: usize,
    memh: ucp_mem_h,
) -> io::Result<usize> {
    ep.check_readable()?;
    if cap == 0 {
        return Ok(0);
    }
    if inline_storage(buf, ptr) {
        return Err(inline_storage_err());
    }
    let slot = slot_acquire();

    let mut params: ucp_request_param_t = unsafe { std::mem::zeroed() };
    // No NO_IMM_CMPL: data already queued at the ep is then unpacked in place
    // and the call returns NULL with `got` set, no request involved. With the
    // flag UCX builds a request, completes it inside the call and hands the
    // callback `req->status`, which release builds up to 1.20.1 never set for
    // a stream recv (stream_recv.c, assert-only): the PS saw UCS_INPROGRESS on
    // 8 MiB receives and dropped the connection. That request is also never
    // marked completed, so `ucp_request_free` leaked it.
    let mut mask = ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA;
    if !memh.is_null() {
        // The dest is registered; Stream still unpacks AM data into it.
        mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_MEMH;
        params.memh = memh;
    }
    params.op_attr_mask = mask as u32;
    params.cb.recv_stream = Some(cb_recv);
    params.user_data = slot as *mut c_void;

    let mut got: usize = 0;
    let r = unsafe { ucp_stream_recv_nbx(ep.ptr(), ptr, cap, &mut got, &params) };
    match classify_ptr(r) {
        PtrStatus::Done => {
            unsafe { slot_release(slot) };
            RECV_IN_PLACE.with(|c| c.set(c.get() + 1));
            Ok(got)
        }
        PtrStatus::Err(st) => {
            unsafe { slot_release(slot) };
            Err(ucs_err(st, "ucp_stream_recv_nbx"))
        }
        PtrStatus::Pending(req) => {
            if unsafe { (*slot).is_done() } {
                RECV_VIA_CALLBACK.with(|c| c.set(c.get() + 1));
            }
            let (status, n) = await_pending(req, slot, SPIN_ITERS, buf, Some(ep)).await;
            if status == ucs_status_t::UCS_OK {
                Ok(n)
            } else {
                Err(ucs_err(status, "ucp_stream_recv cb"))
            }
        }
    }
}

/// Recv into an owned compio buffer (copy-out path, no memh).
async fn ucx_recv<B: IoBufMut>(ep: &UcxEp, buf: B) -> BufResult<usize, B> {
    let mut buf = ManuallyDrop::new(buf);
    let cap = buf.buf_capacity();
    let ptr = buf.as_uninit().as_mut_ptr() as *mut c_void;
    let r = ucx_recv_guarded(ep, &mut buf, ptr, cap, ptr::null_mut()).await;
    if let Ok(n) = r {
        unsafe { buf.set_len(n) };
    }
    BufResult(r, ManuallyDrop::into_inner(buf))
}

/// Recv into `pb`'s writable region from `filled` on, naming the slab's
/// registration when it has one. Returns the bytes received (may be fewer than
/// the region) and the buffer; a buffer whose recv was dropped mid-await is
/// held by the orphan list and returns to its pool once UCX is done with it.
async fn ucx_recv_pooled(ep: &UcxEp, pb: PooledBuf, filled: usize) -> BufResult<usize, PooledBuf> {
    let mut pb = ManuallyDrop::new(pb);
    let (dest, reg) = pb.dest_and_reg();
    let memh = reg.map_or(ptr::null_mut(), |r| r.memh());
    let cap = dest.len().saturating_sub(filled);
    let ptr = if cap == 0 {
        ptr::null_mut()
    } else {
        dest[filled..].as_mut_ptr() as *mut c_void
    };
    let r = ucx_recv_guarded(ep, &mut pb, ptr, cap, memh).await;
    BufResult(r, ManuallyDrop::into_inner(pb))
}
