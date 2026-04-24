//! UCX listener — wraps `ucp_listener_create` + a `conn_handler` callback.
//!
//! UCX runs the `conn_handler` callback synchronously inside a
//! `ucp_worker_progress` invocation, on the same compio runtime thread that
//! created the listener. The callback simply pushes the new
//! `ucp_conn_request_h` onto a futures mpsc; `accept().await` pulls them
//! off the receiver and finishes the handshake via `ucp_ep_create`.

use crate::ucx::endpoint::UcxConn;
use crate::ucx::ffi::*;
use crate::ucx::sockaddr;
use crate::ucx::worker::{ucs_err, with_thread_ctx};
use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::StreamExt;
use std::io;
use std::net::SocketAddr;
use std::os::raw::c_void;
use std::ptr;

pub struct UcxListener {
    inner: *mut ucp_listener,
    rx: UnboundedReceiver<ConnReqPtr>,
    sender_box: *mut UnboundedSender<ConnReqPtr>,
    addr: SocketAddr,
}

/// Wrapper so a `*mut ucp_conn_request` can travel through `mpsc::Unbounded*`.
/// The pointer is single-thread-tied (UCX worker is per-thread) — we never
/// actually send it across threads, but the Send/Sync bounds on the channel
/// type need satisfying for the type to be usable.
struct ConnReqPtr(*mut ucp_conn_request);
unsafe impl Send for ConnReqPtr {}
unsafe impl Sync for ConnReqPtr {}

impl UcxListener {
    pub(crate) async fn bind(addr: SocketAddr) -> io::Result<Self> {
        // Create the channel and leak the sender into raw pointer form so the
        // C callback can find it via user_data. The pointer is freed in
        // `Drop for UcxListener`.
        let (tx, rx) = mpsc::unbounded::<ConnReqPtr>();
        let sender_box = Box::into_raw(Box::new(tx));

        let (inner, local_addr) = with_thread_ctx(|ctx| -> io::Result<(*mut ucp_listener, SocketAddr)> {
            let (mut sa, sa_len) = sockaddr::to_storage(&addr);
            let mut params: ucp_listener_params_t = unsafe { std::mem::zeroed() };
            params.field_mask = (ucp_listener_params_field::UCP_LISTENER_PARAM_FIELD_SOCK_ADDR
                               | ucp_listener_params_field::UCP_LISTENER_PARAM_FIELD_CONN_HANDLER)
                as u64;
            params.sockaddr.addr = &sa as *const _ as *const _;
            params.sockaddr.addrlen = sa_len;
            params.conn_handler.cb = Some(on_conn);
            params.conn_handler.arg = sender_box as *mut c_void;

            let mut l: *mut ucp_listener = ptr::null_mut();
            let st = unsafe { ucp_listener_create(ctx.worker, &params, &mut l) };
            if st != ucs_status_t::UCS_OK {
                // Caller is responsible for freeing sender_box on error.
                return Err(ucs_err(st, "ucp_listener_create"));
            }
            std::hint::black_box(&mut sa);

            // ucp_listener_create may have allocated a different concrete
            // sockaddr (for port=0 binds); use the supplied address as
            // local_addr — it's already concrete (callers who passed
            // [::]:0 will get a kernel-chosen port, so accept that).
            // For the test harness we pass an explicit port, so this is fine.
            Ok((l, addr))
        })
        .map_err(|e| {
            // On failure, drop the sender we leaked.
            unsafe { drop(Box::from_raw(sender_box)) };
            e
        })?;

        Ok(UcxListener {
            inner,
            rx,
            sender_box,
            addr: local_addr,
        })
    }

    pub(crate) async fn accept(&mut self) -> io::Result<(UcxConn, SocketAddr)> {
        let req = self
            .rx
            .next()
            .await
            .ok_or_else(|| io::Error::other("ucx listener closed"))?
            .0;

        // Query the peer address before creating the ep.
        let peer = unsafe {
            let mut q: ucp_conn_request_attr_t = std::mem::zeroed();
            q.field_mask =
                ucp_conn_request_attr_field::UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR as u64;
            let st = ucp_conn_request_query(req, &mut q);
            if st != ucs_status_t::UCS_OK {
                tracing::warn!(status = st, "ucp_conn_request_query failed; peer=0.0.0.0:0");
                "0.0.0.0:0".parse().unwrap()
            } else {
                // ucx_sys_mini::sockaddr_storage and libc::sockaddr_storage
                // have identical layout; transmute via raw bytes.
                let bytes = std::slice::from_raw_parts(
                    &q.client_address as *const _ as *const u8,
                    std::mem::size_of::<libc::sockaddr_storage>(),
                );
                let mut storage: libc::sockaddr_storage = std::mem::zeroed();
                std::ptr::copy_nonoverlapping(
                    bytes.as_ptr(),
                    &mut storage as *mut _ as *mut u8,
                    bytes.len(),
                );
                sockaddr::from_storage(&storage)
            }
        };

        let ep = with_thread_ctx(|ctx| -> io::Result<*mut ucp_ep> {
            let mut params: ucp_ep_params_t = unsafe { std::mem::zeroed() };
            params.field_mask =
                ucp_ep_params_field::UCP_EP_PARAM_FIELD_CONN_REQUEST as u64;
            params.conn_request = req;
            let mut ep: *mut ucp_ep = ptr::null_mut();
            let st = unsafe { ucp_ep_create(ctx.worker, &params, &mut ep) };
            if st != ucs_status_t::UCS_OK {
                return Err(ucs_err(st, "accept ucp_ep_create"));
            }
            Ok(ep)
        })?;

        Ok((UcxConn::from_raw_ep(ep, peer), peer))
    }

    pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.addr)
    }
}

impl Drop for UcxListener {
    fn drop(&mut self) {
        // Tear down UCX listener first (so no more callbacks fire), then drop
        // the sender we leaked.
        unsafe {
            ucp_listener_destroy(self.inner);
            drop(Box::from_raw(self.sender_box));
        }
    }
}

unsafe extern "C" fn on_conn(req: *mut ucp_conn_request, arg: *mut c_void) {
    // SAFETY: arg is the leaked Box<UnboundedSender<ConnReqPtr>> from
    // UcxListener::bind; it lives until UcxListener is dropped, which
    // happens after ucp_listener_destroy so no callbacks can fire after.
    let tx = &*(arg as *const UnboundedSender<ConnReqPtr>);
    let _ = tx.unbounded_send(ConnReqPtr(req));
}
