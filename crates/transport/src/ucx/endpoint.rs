//! UCX endpoint = one `ucp_ep` to a peer. Implements `compio::io::AsyncRead`
//! and `AsyncWrite` over `ucp_stream_send_nbx` / `ucp_stream_recv_nbx`.
//!
//! Buffer ownership: compio's owned-buffer Future model keeps the buffer
//! alive in the future; we await the per-op oneshot before returning, so
//! the buffer outlives the UCX request. **Cancel safety not yet handled** —
//! if the future is dropped pre-completion the buffer goes away while UCX
//! still holds the pointer; tracked in spec §10 as a P3+ follow-up.

use crate::ucx::ffi::*;
use crate::ucx::sockaddr;
use crate::ucx::worker::{progress_once, ucs_err, with_thread_ctx};
use compio::buf::{IoBuf, IoBufMut, SetLen};
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
/// immediate error. This mirrors the `UCS_PTR_IS_ERR` / `UCS_PTR_IS_PTR`
/// macros from `ucs/type/status.h`.
fn classify_ptr(ptr: *mut c_void) -> PtrStatus {
    if ptr.is_null() {
        PtrStatus::Done
    } else {
        let s = ptr as isize;
        // Error codes are in roughly -1..-100 range; any negative isize
        // pointer cast is an error code, any positive is a real handle.
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

/// `yield_now()` equivalent for compio: returns `Pending` once, then `Ready(())`.
/// Wakes itself before returning Pending so the executor immediately
/// re-schedules us — bounded busy loop, no io_uring round-trip.
pub(crate) struct YieldNow(pub(crate) bool);
impl std::future::Future for YieldNow {
    type Output = ();
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.0 {
            std::task::Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }
}

/// Await `rx` while interleaving `ucp_worker_progress` so UCX state
/// machines (wireup, send/recv, close) actually make progress in this
/// single-threaded compio runtime. Polls every 50 µs.
/// Backstop polling: the spawned progress task in `worker.rs` is the
/// primary driver, but waking it requires the runtime to schedule it.
/// We also drive progress inline before each await to shave a 50µs
/// scheduling round-trip off the critical path. The `now_or_never`
/// shortcut returns immediately if the receiver has the value already.
async fn await_with_progress<T>(rx: oneshot::Receiver<T>) -> Result<T, oneshot::Canceled> {
    progress_once();
    rx.await
}

// ---- Send / recv callbacks ----

unsafe extern "C" fn cb_send(
    request: *mut c_void,
    status: ucs_status_t::Type,
    user: *mut c_void,
) {
    let tx = Box::from_raw(user as *mut oneshot::Sender<ucs_status_t::Type>);
    let _ = tx.send(status);
    // Free the request handle; UCX recycles it back into its pool.
    ucp_request_free(request);
}

unsafe extern "C" fn cb_recv(
    request: *mut c_void,
    status: ucs_status_t::Type,
    length: usize,
    user: *mut c_void,
) {
    let tx = Box::from_raw(user as *mut oneshot::Sender<(ucs_status_t::Type, usize)>);
    let _ = tx.send((status, length));
    ucp_request_free(request);
}

// ---- Connection ----

pub struct UcxConn {
    ep: *mut ucp_ep,
    peer: SocketAddr,
}

impl UcxConn {
    pub(crate) async fn connect(addr: SocketAddr) -> io::Result<Self> {
        // Each call yields a fresh EP — matches TCP semantics where each
        // `connect` produces a distinct socket. Connection-pool reuse is
        // handled one layer up (autumn-rpc / autumn-stream ConnPool keyed
        // by SocketAddr); a per-thread EP cache would collapse N logical
        // connections to one shared byte stream and corrupt the framing.
        let ep = with_thread_ctx(|ctx| -> io::Result<*mut ucp_ep> {
            let (mut sa, sa_len) = sockaddr::to_storage(&addr);
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
            // Keep `sa` live across the FFI call (it's referenced by &sa).
            // UCX copies the sockaddr internally so post-call drop is fine.
            std::hint::black_box(&mut sa);
            Ok(ep)
        })?;

        Ok(Self { ep, peer: addr })
    }

    /// Internal constructor used by `UcxListener::accept` after `ucp_ep_create`
    /// has already been driven from a `ucp_conn_request_h`.
    pub(crate) fn from_raw_ep(ep: *mut ucp_ep, peer: SocketAddr) -> Self {
        Self { ep, peer }
    }
}

impl Drop for UcxConn {
    fn drop(&mut self) {
        // Phase 3 simplification: do NOT close the endpoint on drop. UCX's
        // ucp_ep_close_nbx synchronously cancels in-flight ops on the
        // peer's side of the same EP, which races with the peer's pending
        // recv (TCP buffers in the kernel; UCX RC does not). The clean
        // fix needs ucp_worker_flush + await before destroy, which we
        // can't do from a sync Drop.
        //
        // For now: leak the EP. The owning worker reclaims its memory on
        // ucp_worker_destroy at process exit. In production this is fine
        // because EPs are pooled by autumn-rpc / autumn-stream ConnPool
        // (one EP per peer addr per process); we never drop EPs in steady
        // state. Tracked in spec §10 as a P3+ follow-up.
        let _ = self.ep;
    }
}

impl UcxConn {
    pub(crate) fn peer_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.peer)
    }

    pub(crate) fn into_split(self) -> (UcxReadHalf, UcxWriteHalf) {
        let shared = Rc::new(Cell::new(self.ep));
        (
            UcxReadHalf {
                ep: shared.clone(),
                peer: self.peer,
            },
            UcxWriteHalf {
                ep: shared,
                peer: self.peer,
            },
        )
    }

    fn ep(&self) -> *mut ucp_ep {
        self.ep
    }
}

// ---- AsyncRead / AsyncWrite on the joined UcxConn ----

impl compio::io::AsyncRead for UcxConn {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
        ucx_recv(self.ep, buf, &self.peer).await
    }
}

impl compio::io::AsyncWrite for UcxConn {
    async fn write<B: IoBuf>(&mut self, buf: B) -> BufResult<usize, B> {
        ucx_send(self.ep, buf, &self.peer).await
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
    ep: Rc<Cell<*mut ucp_ep>>,
    peer: SocketAddr,
}

pub struct UcxWriteHalf {
    ep: Rc<Cell<*mut ucp_ep>>,
    peer: SocketAddr,
}

impl compio::io::AsyncRead for UcxReadHalf {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
        ucx_recv(self.ep.get(), buf, &self.peer).await
    }
}

impl compio::io::AsyncWrite for UcxWriteHalf {
    async fn write<B: IoBuf>(&mut self, buf: B) -> BufResult<usize, B> {
        ucx_send(self.ep.get(), buf, &self.peer).await
    }
    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// ---- Internals ----

async fn ucx_send<B: IoBuf>(
    ep: *mut ucp_ep,
    buf: B,
    _peer: &SocketAddr,
) -> BufResult<usize, B> {
    let len = buf.buf_len();
    if len == 0 {
        return BufResult(Ok(0), buf);
    }
    let ptr = buf.buf_ptr();

    let (tx, rx) = oneshot::channel::<ucs_status_t::Type>();
    let user = Box::into_raw(Box::new(tx)) as *mut c_void;

    let mut params: ucp_request_param_t = unsafe { std::mem::zeroed() };
    params.op_attr_mask = (ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK
                         | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA) as u32;
    params.cb.send = Some(cb_send);
    params.user_data = user;

    let r = unsafe { ucp_stream_send_nbx(ep, ptr as *const c_void, len, &params) };
    match classify_ptr(r) {
        PtrStatus::Done => {
            unsafe { drop(Box::from_raw(user as *mut oneshot::Sender<ucs_status_t::Type>)) };
            BufResult(Ok(len), buf)
        }
        PtrStatus::Err(st) => {
            unsafe { drop(Box::from_raw(user as *mut oneshot::Sender<ucs_status_t::Type>)) };
            BufResult(Err(ucs_err(st, "ucp_stream_send_nbx")), buf)
        }
        PtrStatus::Pending(_) => match await_with_progress(rx).await {
            Ok(st) if st == ucs_status_t::UCS_OK => BufResult(Ok(len), buf),
            Ok(st) => BufResult(Err(ucs_err(st, "ucp_stream_send cb")), buf),
            Err(_) => BufResult(
                Err(io::Error::other("ucx send callback dropped")),
                buf,
            ),
        },
    }
}

async fn ucx_recv<B: IoBufMut>(
    ep: *mut ucp_ep,
    mut buf: B,
    _peer: &SocketAddr,
) -> BufResult<usize, B> {
    let cap = buf.buf_capacity();
    if cap == 0 {
        return BufResult(Ok(0), buf);
    }
    let ptr = buf.as_uninit().as_mut_ptr() as *mut c_void;

    let (tx, rx) = oneshot::channel::<(ucs_status_t::Type, usize)>();
    let user = Box::into_raw(Box::new(tx)) as *mut c_void;

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
            unsafe {
                drop(Box::from_raw(
                    user as *mut oneshot::Sender<(ucs_status_t::Type, usize)>,
                ))
            };
            unsafe { buf.set_len(got) };
            BufResult(Ok(got), buf)
        }
        PtrStatus::Err(st) => {
            unsafe {
                drop(Box::from_raw(
                    user as *mut oneshot::Sender<(ucs_status_t::Type, usize)>,
                ))
            };
            BufResult(Err(ucs_err(st, "ucp_stream_recv_nbx")), buf)
        }
        PtrStatus::Pending(_) => match await_with_progress(rx).await {
            Ok((st, n)) if st == ucs_status_t::UCS_OK => {
                unsafe { buf.set_len(n) };
                BufResult(Ok(n), buf)
            }
            Ok((st, _)) => BufResult(Err(ucs_err(st, "ucp_stream_recv cb")), buf),
            Err(_) => BufResult(
                Err(io::Error::other("ucx recv callback dropped")),
                buf,
            ),
        },
    }
}
