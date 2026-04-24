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
//! `UcxEp::Drop` issues `ucp_ep_close_nbx` with `UCP_EP_CLOSE_FLAG_FORCE`
//! (no flush — peer learns of disconnect via wire RST, equivalent to a
//! TCP socket close after kernel SO_LINGER=0). The Drop drives progress
//! synchronously until the close request completes.

use crate::ucx::ffi::*;
use crate::ucx::sockaddr;
use crate::ucx::worker::{ucs_err, with_thread_ctx};
use compio::buf::{IoBuf, IoBufMut};
use compio::BufResult;
use futures::channel::oneshot;
use std::cell::Cell;
use std::io;
use std::net::SocketAddr;
use std::os::raw::c_void;
use std::ptr;
use std::rc::Rc;

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

impl Drop for UcxEp {
    fn drop(&mut self) {
        if self.raw.is_null() {
            return;
        }
        // FORCE close: don't flush in-flight ops on our side, don't wait for
        // peer-side drain. Sends a wire RST equivalent — peer's pending
        // recvs see ENDPOINT_TIMEOUT/CONN_RESET, like TCP shutdown(LINGER=0).
        // Suitable for application-level disconnect; for graceful close use
        // an explicit flush-then-close API at the layer above.
        let mut params: ucp_request_param_t = unsafe { std::mem::zeroed() };
        params.op_attr_mask = ucp_op_attr_t::UCP_OP_ATTR_FIELD_FLAGS as u32;
        params.flags = ucp_ep_close_flags_t::UCP_EP_CLOSE_FLAG_FORCE;
        let r = unsafe { ucp_ep_close_nbx(self.raw, &params) };
        match classify_ptr(r) {
            PtrStatus::Done => {} // synchronous close, nothing to drain
            PtrStatus::Err(st) => {
                tracing::warn!(status = st, "ucp_ep_close_nbx returned err");
            }
            PtrStatus::Pending(_) => {
                // Drive progress synchronously until the close completes.
                // We're on the worker's owning OS thread (UCX_THREAD is
                // thread-local), so the spawned async progress task can't
                // run while we hold the thread — drive the loop ourselves.
                let worker = with_thread_ctx(|c| c.worker);
                unsafe {
                    while ucp_request_check_status(r) == ucs_status_t::UCS_INPROGRESS {
                        ucp_worker_progress(worker);
                    }
                    ucp_request_free(r);
                }
            }
        }
        self.raw = ptr::null_mut();
    }
}

// ---- Cancel-safe in-flight guard ----

/// Lives in the future stack-frame across the `rx.await` for an in-flight
/// `ucp_stream_send_nbx` / `ucp_stream_recv_nbx`. If the future drops
/// before the completion callback fires (= `completed.get() == false`),
/// we cancel the UCX request and drive progress synchronously until the
/// callback runs — at which point UCX no longer holds a pointer to the
/// caller's buffer and Rust can safely drop it.
struct InflightGuard {
    request: *mut c_void,
    worker: *mut ucp_worker,
    completed: Rc<Cell<bool>>,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if self.completed.get() {
            return; // happy path: callback already fired
        }
        unsafe {
            ucp_request_cancel(self.worker, self.request);
            // Drive progress until the callback fires with status =
            // UCS_ERR_CANCELED. This is bounded in practice (UCX handles
            // pending sends + recvs by flipping the request to canceled
            // state and queuing the callback for the next progress drain);
            // empirically completes in <1k iters on rc_mlx5. Capped at
            // 100k as a safety net — if UCX ever fails to honour cancel
            // within that, we leak rather than hang.
            let mut iters = 0;
            const SPIN_CAP: usize = 100_000;
            while !self.completed.get() && iters < SPIN_CAP {
                ucp_worker_progress(self.worker);
                iters += 1;
            }
            if !self.completed.get() {
                tracing::warn!(
                    iters,
                    "ucp_request_cancel did not complete in {SPIN_CAP} progress iters; \
                     leaking request + user_data Box (UCX still owns the buffer pointer)"
                );
            }
        }
    }
}

// ---- Send / recv callbacks ----

unsafe extern "C" fn cb_send(
    request: *mut c_void,
    status: ucs_status_t::Type,
    user: *mut c_void,
) {
    let ud = Box::from_raw(user as *mut SendUserData);
    // Order matters: set the completed flag BEFORE waking the rx so the
    // InflightGuard's Drop sees `true` if it runs after rx wakes.
    ud.completed.set(true);
    let _ = ud.tx.send(status);
    ucp_request_free(request);
}

unsafe extern "C" fn cb_recv(
    request: *mut c_void,
    status: ucs_status_t::Type,
    length: usize,
    user: *mut c_void,
) {
    let ud = Box::from_raw(user as *mut RecvUserData);
    ud.completed.set(true);
    let _ = ud.tx.send((status, length));
    ucp_request_free(request);
}

struct SendUserData {
    tx: oneshot::Sender<ucs_status_t::Type>,
    completed: Rc<Cell<bool>>,
}

struct RecvUserData {
    tx: oneshot::Sender<(ucs_status_t::Type, usize)>,
    completed: Rc<Cell<bool>>,
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
            params.flags =
                ucp_ep_params_flags_field::UCP_EP_PARAMS_FLAGS_CLIENT_SERVER as u32;
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
        Ok(Self {
            ep: UcxEp::new(ep),
            peer: addr,
        })
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

impl compio::io::AsyncWrite for UcxWriteHalf {
    async fn write<B: IoBuf>(&mut self, buf: B) -> BufResult<usize, B> {
        ucx_send(self.ep.ptr(), buf).await
    }
    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// ---- Internals ----

async fn ucx_send<B: IoBuf>(ep: *mut ucp_ep, buf: B) -> BufResult<usize, B> {
    let len = buf.buf_len();
    if len == 0 {
        return BufResult(Ok(0), buf);
    }
    let ptr = buf.buf_ptr();

    let (tx, rx) = oneshot::channel::<ucs_status_t::Type>();
    let completed = Rc::new(Cell::new(false));
    let user = Box::into_raw(Box::new(SendUserData {
        tx,
        completed: completed.clone(),
    })) as *mut c_void;

    let mut params: ucp_request_param_t = unsafe { std::mem::zeroed() };
    params.op_attr_mask = (ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK
                         | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA) as u32;
    params.cb.send = Some(cb_send);
    params.user_data = user;

    let r = unsafe { ucp_stream_send_nbx(ep, ptr as *const c_void, len, &params) };
    match classify_ptr(r) {
        PtrStatus::Done => {
            unsafe { drop(Box::from_raw(user as *mut SendUserData)) };
            BufResult(Ok(len), buf)
        }
        PtrStatus::Err(st) => {
            unsafe { drop(Box::from_raw(user as *mut SendUserData)) };
            BufResult(Err(ucs_err(st, "ucp_stream_send_nbx")), buf)
        }
        PtrStatus::Pending(req) => {
            let worker = with_thread_ctx(|c| c.worker);
            // _guard fires on early-drop; on normal return its `completed`
            // check skips the cancel.
            let _guard = InflightGuard {
                request: req,
                worker,
                completed,
            };
            match rx.await {
                Ok(st) if st == ucs_status_t::UCS_OK => BufResult(Ok(len), buf),
                Ok(st) => BufResult(Err(ucs_err(st, "ucp_stream_send cb")), buf),
                Err(_) => unreachable!(
                    "tx is owned by the SendUserData box; the callback always sends \
                     before freeing the box, so the receiver can never see Canceled"
                ),
            }
        }
    }
}

async fn ucx_recv<B: IoBufMut>(ep: *mut ucp_ep, mut buf: B) -> BufResult<usize, B> {
    let cap = buf.buf_capacity();
    if cap == 0 {
        return BufResult(Ok(0), buf);
    }
    let ptr = buf.as_uninit().as_mut_ptr() as *mut c_void;

    let (tx, rx) = oneshot::channel::<(ucs_status_t::Type, usize)>();
    let completed = Rc::new(Cell::new(false));
    let user = Box::into_raw(Box::new(RecvUserData {
        tx,
        completed: completed.clone(),
    })) as *mut c_void;

    let mut params: ucp_request_param_t = unsafe { std::mem::zeroed() };
    params.op_attr_mask = (ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK
                         | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA
                         | ucp_op_attr_t::UCP_OP_ATTR_FLAG_NO_IMM_CMPL) as u32;
    params.cb.recv_stream = Some(cb_recv);
    params.user_data = user;

    let mut got: usize = 0;
    let r = unsafe { ucp_stream_recv_nbx(ep, ptr, cap, &mut got, &params) };
    match classify_ptr(r) {
        PtrStatus::Done => {
            unsafe { drop(Box::from_raw(user as *mut RecvUserData)) };
            unsafe { buf.set_len(got) };
            BufResult(Ok(got), buf)
        }
        PtrStatus::Err(st) => {
            unsafe { drop(Box::from_raw(user as *mut RecvUserData)) };
            BufResult(Err(ucs_err(st, "ucp_stream_recv_nbx")), buf)
        }
        PtrStatus::Pending(req) => {
            let worker = with_thread_ctx(|c| c.worker);
            let _guard = InflightGuard {
                request: req,
                worker,
                completed,
            };
            match rx.await {
                Ok((st, n)) if st == ucs_status_t::UCS_OK => {
                    unsafe { buf.set_len(n) };
                    BufResult(Ok(n), buf)
                }
                Ok((st, _)) => BufResult(Err(ucs_err(st, "ucp_stream_recv cb")), buf),
                Err(_) => unreachable!(
                    "tx is owned by the RecvUserData box; the callback always sends \
                     before freeing the box, so the receiver can never see Canceled"
                ),
            }
        }
    }
}
