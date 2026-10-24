//! Large values as active messages: from `BULK_MIN_BYTES` on, `send` asks
//! for rendezvous and UCX moves the value straight into the receiver's buffer
//! (the receiving CPU copies nothing), which a stream receive cannot do — UCP
//! Stream has no rendezvous. Below it the value goes eager and is copied.
//!
//! The sender tags each value (`am_send`); the receiver registers where the
//! value with that tag goes before it can arrive (`expect`). The worker's
//! handler (`on_value`) looks the tag up when the message arrives: a waiting
//! buffer gets the value (`ucp_am_recv_data_nbx` for a rendezvous message, a
//! copy for an eager one); a tag nobody waits for any more is dropped, which
//! also completes the sender. That is why this is AM and not tag matching: a
//! tagged rendezvous send nobody receives never completes.
//!
//! Tags are per receiving worker (`next_tag`): every value a thread expects
//! carries a tag no other value to that thread carries.

use crate::recv_dest::RecvDest;
use crate::ucx::endpoint::{
    await_pending, cb_send, classify_ptr, inline_storage, inline_storage_err, slot_acquire, slot_release, PtrStatus,
    UcxEp, SPIN_ITERS,
};
use crate::ucx::ffi::*;
use crate::ucx::worker::ucs_err;
use bytes::Bytes;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::mem::ManuallyDrop;
use std::os::raw::c_void;
use std::pin::Pin;
use std::ptr;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

/// The AM id of a tagged value. One handler per worker.
const VALUE_AM_ID: u32 = 1;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    /// Registered; the value has not arrived.
    Waiting,
    /// A rendezvous receive into the buffer is in flight.
    Receiving,
    Done(usize),
    /// The value arrived but could not be received (UCX status, or longer
    /// than the buffer: `TOO_LONG`).
    Failed(ucs_status_t::Type, usize),
}

const TOO_LONG: ucs_status_t::Type = ucs_status_t::UCS_ERR_MESSAGE_TRUNCATED;

struct Slot {
    tag: u64,
    dest: RefCell<Option<RecvDest>>,
    state: Cell<State>,
    waker: Cell<Option<Waker>>,
}

impl Slot {
    /// The value is in (or will never be): release the memory, then tell the waiter.
    fn finish(&self, state: State) {
        drop(self.dest.borrow_mut().take());
        self.state.set(state);
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }
}

thread_local! {
    static WORKER: Cell<*mut ucp_worker> = const { Cell::new(ptr::null_mut()) };
    static EXPECTED: RefCell<HashMap<u64, Rc<Slot>>> = RefCell::new(HashMap::new());
    static NEXT_TAG: Cell<u64> = const { Cell::new(1) };
    /// Values received on this thread: (eager, rendezvous).
    static RECEIVED: Cell<(u64, u64)> = const { Cell::new((0, 0)) };
}

/// This thread's values received (eager copies, rendezvous receives).
pub fn received_counts() -> (u64, u64) {
    RECEIVED.with(|c| c.get())
}

/// A tag no other value expected on this thread carries.
pub(crate) fn next_tag() -> u64 {
    NEXT_TAG.with(|t| {
        let tag = t.get();
        t.set(tag.wrapping_add(1).max(1));
        tag
    })
}

/// Install the value handler on a new worker (called once per worker thread).
pub(crate) unsafe fn register_handler(worker: *mut ucp_worker) {
    WORKER.with(|w| w.set(worker));
    let mut param: ucp_am_handler_param_t = std::mem::zeroed();
    param.field_mask = (ucp_am_handler_param_field::UCP_AM_HANDLER_PARAM_FIELD_ID
        | ucp_am_handler_param_field::UCP_AM_HANDLER_PARAM_FIELD_FLAGS
        | ucp_am_handler_param_field::UCP_AM_HANDLER_PARAM_FIELD_CB) as u64;
    param.id = VALUE_AM_ID;
    // An eager value reaches the handler whole, not fragment by fragment.
    param.flags = ucp_am_cb_flags::UCP_AM_FLAG_WHOLE_MSG as u32;
    param.cb = Some(on_value);
    let st = ucp_worker_set_am_recv_handler(worker, &param);
    assert_eq!(st, ucs_status_t::UCS_OK, "ucp_worker_set_am_recv_handler");
}

unsafe extern "C" fn on_value(
    _arg: *mut c_void,
    header: *const c_void,
    header_length: usize,
    data: *mut c_void,
    length: usize,
    param: *const ucp_am_recv_param_t,
) -> ucs_status_t::Type {
    if header_length != 8 {
        return ucs_status_t::UCS_OK;
    }
    let tag = u64::from_le_bytes(*(header as *const [u8; 8]));
    // Unknown tag: nobody waits (the caller gave up). Returning OK drops the
    // value, rendezvous included.
    let Some(slot) = EXPECTED.with(|e| e.borrow_mut().remove(&tag)) else {
        return ucs_status_t::UCS_OK;
    };
    let rndv = (*param).recv_attr & ucp_am_recv_attr_t::UCP_AM_RECV_ATTR_FLAG_RNDV as u64 != 0;
    let (dst, cap) = match slot.dest.borrow_mut().as_mut() {
        Some(d) => (d.region_mut().as_mut_ptr(), d.cap()),
        None => (ptr::null_mut(), 0),
    };
    if dst.is_null() || length > cap {
        slot.finish(State::Failed(TOO_LONG, length));
        return ucs_status_t::UCS_OK;
    }
    RECEIVED.with(|c| {
        let (e, r) = c.get();
        c.set(if rndv { (e, r + 1) } else { (e + 1, r) })
    });
    if !rndv {
        // Eager: the whole value is at `data` for the length of this call.
        ptr::copy_nonoverlapping(data as *const u8, dst, length);
        slot.finish(State::Done(length));
        return ucs_status_t::UCS_OK;
    }
    let mut p: ucp_request_param_t = std::mem::zeroed();
    p.op_attr_mask = (ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA) as u32;
    p.cb.recv_am = Some(on_received);
    // The in-flight receive holds the slot (and with it the buffer) until it completes.
    p.user_data = Rc::into_raw(slot.clone()) as *mut c_void;
    let worker = WORKER.with(|w| w.get());
    let r = ucp_am_recv_data_nbx(worker, data, dst as *mut c_void, length, &p);
    match classify_ptr(r) {
        PtrStatus::Done => {
            drop(Rc::from_raw(p.user_data as *const Slot));
            slot.finish(State::Done(length));
        }
        PtrStatus::Err(st) => {
            drop(Rc::from_raw(p.user_data as *const Slot));
            slot.finish(State::Failed(st, length));
        }
        PtrStatus::Pending(_) => slot.state.set(State::Receiving),
    }
    // UCX owns `data` after `ucp_am_recv_data_nbx`, whatever it returned.
    ucs_status_t::UCS_OK
}

unsafe extern "C" fn on_received(request: *mut c_void, status: ucs_status_t::Type, length: usize, user: *mut c_void) {
    let slot = Rc::from_raw(user as *const Slot);
    ucp_request_free(request);
    slot.finish(if status == ucs_status_t::UCS_OK { State::Done(length) } else { State::Failed(status, length) });
}

/// Where the value tagged `tag` goes. Register it before the value can be
/// sent; then await [`Expected::arrived`], and end with
/// [`Expected::finish`], after which nothing writes `dest`'s memory.
pub(crate) struct Expected(Rc<Slot>);

pub(crate) fn expect(tag: u64, dest: RecvDest) -> Expected {
    let slot = Rc::new(Slot {
        tag,
        dest: RefCell::new(Some(dest)),
        state: Cell::new(State::Waiting),
        waker: Cell::new(None),
    });
    EXPECTED.with(|e| e.borrow_mut().insert(tag, slot.clone()));
    Expected(slot)
}

pub(crate) enum Arrival {
    Received(usize),
    /// Longer than the buffer: not received.
    TooLong(usize),
}

impl Expected {
    /// The value is in the buffer, or failed to be.
    pub(crate) async fn arrived(&self) -> io::Result<Arrival> {
        WaitArrival(&self.0).await
    }

    /// Stop waiting. A value that has not arrived is dropped when it does; a
    /// rendezvous receive already under way runs to its end (it cannot be
    /// stopped), and this returns once it has. Either way the memory is
    /// released when this returns.
    pub(crate) async fn finish(self) {
        if self.0.state.get() == State::Receiving {
            // Its outcome no longer matters, only that the receive is over.
            drop(WaitArrival(&self.0).await);
        }
    }
}

impl Drop for Expected {
    /// Dropped while waiting: unregister, so a late value is dropped instead
    /// of written. (Dropped mid-receive, the receive keeps the memory until
    /// it ends; `finish` is what waits for that.)
    fn drop(&mut self) {
        if self.0.state.get() == State::Waiting {
            EXPECTED.with(|e| e.borrow_mut().remove(&self.0.tag));
            self.0.finish(State::Failed(ucs_status_t::UCS_ERR_CANCELED, 0));
        }
    }
}

struct WaitArrival<'a>(&'a Slot);

impl Future for WaitArrival<'_> {
    type Output = io::Result<Arrival>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.0.state.get() {
            State::Done(n) => Poll::Ready(Ok(Arrival::Received(n))),
            State::Failed(st, n) if st == TOO_LONG => Poll::Ready(Ok(Arrival::TooLong(n))),
            State::Failed(st, _) => Poll::Ready(Err(ucs_err(st, "am value receive"))),
            State::Waiting | State::Receiving => {
                self.0.waker.set(Some(cx.waker().clone()));
                Poll::Pending
            }
        }
    }
}

/// Send `value` as the AM tagged `tag`. Completes once UCX no longer needs
/// `value` (for a rendezvous: once the receiver fetched or dropped it).
pub(crate) async fn send(ep: &UcxEp, tag: u64, value: Bytes) -> io::Result<()> {
    let mut payload = ManuallyDrop::new(value);
    let (ptr_v, len) = (payload.as_ptr() as *const c_void, payload.len());
    if len > 0 && inline_storage(&payload, ptr_v) {
        drop(ManuallyDrop::into_inner(payload));
        return Err(inline_storage_err());
    }
    let header = tag.to_le_bytes();
    let slot = slot_acquire();
    let mut p: ucp_request_param_t = unsafe { std::mem::zeroed() };
    p.op_attr_mask = (ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK
        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA
        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_FLAGS) as u32;
    p.cb.send = Some(cb_send);
    p.user_data = slot as *mut c_void;
    // The header lives on this frame: UCX copies it at once. Eager or
    // rendezvous at the same size as every other bulk cut, not UCX's own.
    let protocol = if len >= crate::BULK_MIN_BYTES {
        ucp_send_am_flags::UCP_AM_SEND_FLAG_RNDV
    } else {
        ucp_send_am_flags::UCP_AM_SEND_FLAG_EAGER
    };
    p.flags = (ucp_send_am_flags::UCP_AM_SEND_FLAG_COPY_HEADER | protocol) as u32;
    let r = unsafe { ucp_am_send_nbx(ep.ptr(), VALUE_AM_ID, header.as_ptr() as *const c_void, 8, ptr_v, len, &p) };
    let out = match classify_ptr(r) {
        PtrStatus::Done => {
            unsafe { slot_release(slot) };
            Ok(())
        }
        PtrStatus::Err(st) => {
            unsafe { slot_release(slot) };
            Err(ucs_err(st, "ucp_am_send_nbx"))
        }
        PtrStatus::Pending(req) => {
            let (st, _) = await_pending(req, slot, SPIN_ITERS, &mut payload, None).await;
            if st == ucs_status_t::UCS_OK {
                Ok(())
            } else {
                Err(ucs_err(st, "ucp_am_send cb"))
            }
        }
    };
    drop(ManuallyDrop::into_inner(payload));
    out
}
