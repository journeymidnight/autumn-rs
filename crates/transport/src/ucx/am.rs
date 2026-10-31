//! Large values as active messages: from `BULK_MIN_BYTES` on, a send asks
//! for rendezvous and UCX moves the value straight into the receiver's buffer
//! (the receiving CPU copies nothing), which a stream receive cannot do — UCP
//! Stream has no rendezvous. Below it the value goes eager and is copied.
//!
//! Two ways to pair a value with the buffer it goes to:
//!
//! - **Asked for** (`expect` / `send`, AM id 1): the receiver registers where
//!   the value with a tag goes before asking for it (`MSG_GET_AM`), so the tag
//!   is the receiver's (`next_tag`). A tag nobody waits for any more is
//!   dropped, which also completes the sender. That is why this is AM and not
//!   tag matching: a tagged rendezvous send nobody receives never completes.
//! - **Frame values** (`post_frame_value` / `receive_value`, AM id 2): the
//!   value of an RPC frame sent beside it. The sender tags it (per connection)
//!   and the frame names the tag; the receiver learns of the value from the
//!   frame, which can arrive before or after the value's rendezvous request.
//!   One that arrives first is held (`EARLY`, the callback answers
//!   `UCS_INPROGRESS`) until the frame asks for it. Values are keyed by the
//!   receiving endpoint and the tag, so tags only need to be unique per
//!   connection.

use crate::recv_dest::RecvDest;
use crate::regpool::{PooledBuf, RegBytes};
use crate::ucx::endpoint::{
    await_pending, cb_send, classify_ptr, inline_storage, inline_storage_err, slot_acquire, slot_release, PtrStatus,
    UcxEp, SPIN_ITERS,
};
use crate::ucx::ffi::*;
use crate::ucx::worker::ucs_err;
use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io;
use std::mem::ManuallyDrop;
use std::os::raw::c_void;
use std::pin::Pin;
use std::ptr;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

/// The AM id of a value the receiver asked for. One handler per worker.
const VALUE_AM_ID: u32 = 1;
/// The AM id of a frame's value.
const FRAME_VALUE_AM_ID: u32 = 2;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    /// Registered; the value has not arrived.
    Waiting,
    /// A rendezvous receive into the buffer is in flight.
    Receiving,
    Done(usize),
    /// The value arrived but could not be received (UCX status, or a length
    /// the buffer does not take: `TOO_LONG`).
    Failed(ucs_status_t::Type, usize),
}

const TOO_LONG: ucs_status_t::Type = ucs_status_t::UCS_ERR_MESSAGE_TRUNCATED;

/// A frame value: the receiving endpoint and the sender's tag.
type FrameKey = (usize, u64);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Key {
    Asked(u64),
    Frame(FrameKey),
}

/// Where a value goes. UCX only ever receives into memory with a memh.
enum Target {
    /// Memory the caller lent, inside a region it registered; released by
    /// dropping it. The region is held until then, so a receive left running
    /// by a dropped call never outlives the registration.
    Lent { dest: RecvDest, _region: std::sync::Arc<crate::Region>, memh: ucp_mem_h },
    /// Memory the caller lent outside any registered region: received into
    /// `landing`, copied into `dest` once in.
    Landed { dest: RecvDest, landing: PooledBuf },
    /// `len` bytes at `ptr` inside a pool buffer `owner` keeps alive for as
    /// long as a receive may write them.
    Pooled { ptr: *mut u8, len: usize, memh: ucp_mem_h, _owner: Rc<dyn Any> },
}

impl Target {
    fn region(&mut self) -> (*mut u8, usize, ucp_mem_h) {
        match self {
            Target::Lent { dest, memh, .. } => (dest.region_mut().as_mut_ptr(), dest.cap(), *memh),
            Target::Landed { landing, .. } => {
                let (buf, reg) = landing.dest_and_reg();
                (buf.as_mut_ptr(), buf.len(), reg.map_or(ptr::null_mut(), |r| r.memh()))
            }
            Target::Pooled { ptr, len, memh, .. } => (*ptr, *len, *memh),
        }
    }

    /// The value (`n` bytes) is in: a landed one goes on to the caller's memory.
    fn arrived(&mut self, n: usize) {
        if let Target::Landed { dest, landing } = self {
            dest.region_mut()[..n].copy_from_slice(&landing.dest_mut()[..n]);
            LANDED_BYTES.with(|c| c.set(c.get() + n as u64));
            LANDED_TOTAL.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

struct Slot {
    key: Key,
    dest: RefCell<Option<Target>>,
    state: Cell<State>,
    waker: Cell<Option<Waker>>,
}

impl Slot {
    /// The value is in (or will never be): release the memory, then tell the waiter.
    fn finish(&self, state: State) {
        let target = self.dest.borrow_mut().take();
        if let (Some(mut t), State::Done(n)) = (target, state) {
            t.arrived(n);
        }
        self.state.set(state);
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }
}

/// A frame value that arrived before its frame asked for it: its rendezvous
/// request, held by answering `UCS_INPROGRESS`.
struct Early {
    desc: *mut c_void,
    len: usize,
}

thread_local! {
    static WORKER: Cell<*mut ucp_worker> = const { Cell::new(ptr::null_mut()) };
    static EXPECTED: RefCell<HashMap<Key, Rc<Slot>>> = RefCell::new(HashMap::new());
    static EARLY: RefCell<HashMap<FrameKey, Early>> = RefCell::new(HashMap::new());
    /// Frame values whose receiver stopped waiting before they arrived.
    static ABANDONED: RefCell<HashSet<FrameKey>> = RefCell::new(HashSet::new());
    /// Endpoints UCX reported failed, until they are dropped: no value comes
    /// on them any more.
    static FAILED: RefCell<HashMap<usize, ucs_status_t::Type>> = RefCell::new(HashMap::new());
    static NEXT_TAG: Cell<u64> = const { Cell::new(1) };
    /// Values received on this thread: (eager, rendezvous).
    static RECEIVED: Cell<(u64, u64)> = const { Cell::new((0, 0)) };
    /// Rendezvous receives posted on this thread: (with a memh, without).
    static RECEIVED_MEMH: Cell<(u64, u64)> = const { Cell::new((0, 0)) };
    /// Values sent from this thread: (with a memh, without).
    static SENT: Cell<(u64, u64)> = const { Cell::new((0, 0)) };
    /// Values this thread copied into a pool buffer to send them.
    static STAGED: Cell<u64> = const { Cell::new(0) };
    /// Bytes of asked-for values this thread received into a pool buffer and
    /// copied into the caller's memory (it had no registration).
    static LANDED_BYTES: Cell<u64> = const { Cell::new(0) };
}

/// This thread's asked-for value bytes copied out of a pool buffer.
pub fn landed_bytes() -> u64 {
    LANDED_BYTES.with(|c| c.get())
}

static LANDED_TOTAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// `landed_bytes` of the whole process.
pub fn landed_bytes_total() -> u64 {
    LANDED_TOTAL.load(std::sync::atomic::Ordering::Relaxed)
}

/// This thread's worker; null once the thread is being torn down.
pub(crate) fn this_worker() -> *mut ucp_worker {
    WORKER.try_with(|w| w.get()).unwrap_or(ptr::null_mut())
}

/// This thread's rendezvous receives posted with a memh, and without one.
pub fn received_memh_counts() -> (u64, u64) {
    RECEIVED_MEMH.with(|c| c.get())
}

/// This thread's values received (eager copies, rendezvous receives).
pub fn received_counts() -> (u64, u64) {
    RECEIVED.with(|c| c.get())
}

/// This thread's values sent (with their slab's memh, without one).
pub fn sent_counts() -> (u64, u64) {
    SENT.with(|c| c.get())
}

/// Bytes sent as active messages by the whole process: with a memh, without.
static SENT_BYTES: [std::sync::atomic::AtomicU64; 2] = [const { std::sync::atomic::AtomicU64::new(0) }; 2];

/// Bytes this process sent as active messages: (with a memh, without one —
/// memory UCX had to look up or register itself).
pub fn sent_bytes() -> (u64, u64) {
    use std::sync::atomic::Ordering::Relaxed;
    (SENT_BYTES[0].load(Relaxed), SENT_BYTES[1].load(Relaxed))
}

/// A tag no other value expected on this thread carries.
pub(crate) fn next_tag() -> u64 {
    NEXT_TAG.with(|t| {
        let tag = t.get();
        t.set(tag.wrapping_add(1).max(1));
        tag
    })
}

/// Install the value handlers on a new worker (called once per worker thread).
pub(crate) unsafe fn register_handler(worker: *mut ucp_worker) {
    WORKER.with(|w| w.set(worker));
    let handlers: [(u32, ucp_am_recv_callback_t); 2] =
        [(VALUE_AM_ID, Some(on_value)), (FRAME_VALUE_AM_ID, Some(on_frame_value))];
    for (id, cb) in handlers {
        let mut param: ucp_am_handler_param_t = std::mem::zeroed();
        param.field_mask = (ucp_am_handler_param_field::UCP_AM_HANDLER_PARAM_FIELD_ID
            | ucp_am_handler_param_field::UCP_AM_HANDLER_PARAM_FIELD_FLAGS
            | ucp_am_handler_param_field::UCP_AM_HANDLER_PARAM_FIELD_CB) as u64;
        param.id = id;
        // An eager value reaches the handler whole, not fragment by fragment.
        param.flags = ucp_am_cb_flags::UCP_AM_FLAG_WHOLE_MSG as u32;
        param.cb = cb;
        let st = ucp_worker_set_am_recv_handler(worker, &param);
        assert_eq!(st, ucs_status_t::UCS_OK, "ucp_worker_set_am_recv_handler");
    }
}

fn tag_of(header: *const c_void, header_length: usize) -> Option<u64> {
    (header_length == 8).then(|| u64::from_le_bytes(unsafe { *(header as *const [u8; 8]) }))
}

unsafe fn is_rndv(param: *const ucp_am_recv_param_t) -> bool {
    (*param).recv_attr & ucp_am_recv_attr_t::UCP_AM_RECV_ATTR_FLAG_RNDV as u64 != 0
}

unsafe extern "C" fn on_value(
    _arg: *mut c_void,
    header: *const c_void,
    header_length: usize,
    data: *mut c_void,
    length: usize,
    param: *const ucp_am_recv_param_t,
) -> ucs_status_t::Type {
    let Some(tag) = tag_of(header, header_length) else {
        return ucs_status_t::UCS_OK;
    };
    // Unknown tag: nobody waits (the caller gave up). Returning OK drops the
    // value, rendezvous included.
    let Some(slot) = EXPECTED.with(|e| e.borrow_mut().remove(&Key::Asked(tag))) else {
        return ucs_status_t::UCS_OK;
    };
    receive(slot, data, length, is_rndv(param), false, false);
    // UCX owns `data` after `ucp_am_recv_data_nbx` (on its rare internal
    // failures, out of memory, it keeps the descriptor and the sender waits).
    ucs_status_t::UCS_OK
}

unsafe extern "C" fn on_frame_value(
    _arg: *mut c_void,
    header: *const c_void,
    header_length: usize,
    data: *mut c_void,
    length: usize,
    param: *const ucp_am_recv_param_t,
) -> ucs_status_t::Type {
    let has_ep = (*param).recv_attr & ucp_am_recv_attr_t::UCP_AM_RECV_ATTR_FIELD_REPLY_EP as u64 != 0;
    let (Some(tag), true) = (tag_of(header, header_length), has_ep) else {
        tracing::error!(header_length, has_ep, "frame value without a tag or a reply endpoint, dropped");
        return ucs_status_t::UCS_OK;
    };
    let key = ((*param).reply_ep as usize, tag);
    let rndv = is_rndv(param);
    if let Some(slot) = EXPECTED.with(|e| e.borrow_mut().remove(&Key::Frame(key))) {
        receive(slot, data, length, rndv, true, false);
        return ucs_status_t::UCS_OK;
    }
    // Nobody will ask for it: dropping it completes the sender.
    if ABANDONED.with(|a| a.borrow_mut().remove(&key)) {
        return ucs_status_t::UCS_OK;
    }
    // Ahead of its frame: held. Only a rendezvous request is (frame values are
    // always sent so), which costs a descriptor, not the value; an eager one
    // or a second value under a held tag is dropped, as nothing of ours sends
    // either.
    if !rndv || EARLY.with(|e| e.borrow().contains_key(&key)) {
        tracing::warn!(rndv, length, "frame value not held: eager, or its tag is already held; dropped");
        return ucs_status_t::UCS_OK;
    }
    EARLY.with(|e| e.borrow_mut().insert(key, Early { desc: data, len: length }));
    ucs_status_t::UCS_INPROGRESS
}

/// Receive an arrived value (`data`: the eager bytes, or the rendezvous
/// descriptor) into `slot`'s buffer. A frame value must fill it exactly.
/// `held`: a rendezvous descriptor kept past its callback, which only an
/// explicit release lets go of (inside the callback, not receiving drops it).
unsafe fn receive(slot: Rc<Slot>, data: *mut c_void, length: usize, rndv: bool, exact: bool, held: bool) {
    let (dst, cap, memh) = match slot.dest.borrow_mut().as_mut() {
        Some(t) => t.region(),
        None => (ptr::null_mut(), 0, ptr::null_mut()),
    };
    let fits = if exact { length == cap } else { length <= cap };
    if dst.is_null() || !fits {
        if rndv && held {
            // Not received: let the sender complete.
            ucp_am_data_release(WORKER.with(|w| w.get()), data);
        }
        slot.finish(State::Failed(TOO_LONG, length));
        return;
    }
    RECEIVED.with(|c| {
        let (e, r) = c.get();
        c.set(if rndv { (e, r + 1) } else { (e + 1, r) })
    });
    if !rndv {
        // Eager: the whole value is at `data`.
        ptr::copy_nonoverlapping(data as *const u8, dst, length);
        slot.finish(State::Done(length));
        return;
    }
    let mut p: ucp_request_param_t = std::mem::zeroed();
    p.op_attr_mask = (ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA) as u32;
    if !memh.is_null() {
        p.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_MEMH as u32;
        p.memh = memh;
    }
    RECEIVED_MEMH.with(|c| {
        let (with, without) = c.get();
        c.set(if memh.is_null() { (with, without + 1) } else { (with + 1, without) })
    });
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
}

unsafe extern "C" fn on_received(request: *mut c_void, status: ucs_status_t::Type, length: usize, user: *mut c_void) {
    let slot = Rc::from_raw(user as *const Slot);
    ucp_request_free(request);
    slot.finish(if status == ucs_status_t::UCS_OK { State::Done(length) } else { State::Failed(status, length) });
}

/// UCX reported `ep` failed (its peer closed or died): the frame values still
/// awaited on it, and any asked for later, fail with `status`.
pub(crate) fn ep_failed(ep: *mut ucp_ep, status: ucs_status_t::Type) {
    let ep = ep as usize;
    FAILED.with(|f| f.borrow_mut().insert(ep, status));
    fail_awaited(ep, status);
}

/// Fail the frame values still awaited on `ep`.
fn fail_awaited(ep: usize, status: ucs_status_t::Type) {
    let waiting: Vec<Rc<Slot>> = EXPECTED
        .try_with(|e| {
            let mut e = e.borrow_mut();
            let keys: Vec<Key> = e.keys().filter(|k| matches!(k, Key::Frame((p, _)) if *p == ep)).copied().collect();
            keys.into_iter().filter_map(|k| e.remove(&k)).collect()
        })
        .unwrap_or_default();
    for slot in waiting {
        slot.finish(State::Failed(status, 0));
    }
}

/// The endpoint is about to be closed: fail the values still awaited on it, let
/// go of the ones held for it, and forget it, since its address may be reused
/// (an awaited value left registered would take the next endpoint's value of
/// the same tag).
pub(crate) fn forget_ep(ep: *mut ucp_ep) {
    let ep = ep as usize;
    // `try_with` throughout: an endpoint dropped during thread teardown has
    // nothing left to release (no one progresses the worker any more).
    if FAILED.try_with(|f| f.borrow_mut().remove(&ep)).is_err() {
        return;
    }
    fail_awaited(ep, ucs_status_t::UCS_ERR_CANCELED);
    let held: Vec<Early> = EARLY
        .try_with(|e| {
            let mut e = e.borrow_mut();
            let keys: Vec<FrameKey> = e.keys().filter(|k| k.0 == ep).copied().collect();
            keys.into_iter().filter_map(|k| e.remove(&k)).collect()
        })
        .unwrap_or_default();
    let worker = WORKER.try_with(|w| w.get()).unwrap_or(ptr::null_mut());
    if !worker.is_null() {
        for h in held {
            unsafe { ucp_am_data_release(worker, h.desc) };
        }
    }
    let _ = ABANDONED.try_with(|a| a.borrow_mut().retain(|k| k.0 != ep));
}

/// Give up a frame's value that was never started: a piece already held is
/// released, one still to come is dropped on arrival; either way its sender
/// completes.
pub(crate) fn abandon_value(ep: &UcxEp, segments: &[(usize, u64)]) {
    let ep = ep.ptr() as usize;
    let worker = WORKER.with(|w| w.get());
    for &(_, tag) in segments.iter().filter(|s| s.1 != 0) {
        let key = (ep, tag);
        match EARLY.with(|e| e.borrow_mut().remove(&key)) {
            Some(early) => unsafe { ucp_am_data_release(worker, early.desc) },
            None => {
                ABANDONED.with(|a| a.borrow_mut().insert(key));
            }
        }
    }
}

/// Where a value goes, from registration until [`Expected::finish`].
pub(crate) struct Expected(Rc<Slot>);

fn register(key: Key, dest: Target) -> Expected {
    let slot = Rc::new(Slot {
        key,
        dest: RefCell::new(Some(dest)),
        state: Cell::new(State::Waiting),
        waker: Cell::new(None),
    });
    EXPECTED.with(|e| e.borrow_mut().insert(key, slot.clone()));
    Expected(slot)
}

/// Where the value tagged `tag` goes. Register it before the value can be
/// sent; then await [`Expected::arrived`], and end with
/// [`Expected::finish`], after which nothing writes `dest`'s memory.
pub(crate) fn expect(tag: u64, dest: RecvDest, region: Option<&std::sync::Arc<crate::Region>>) -> io::Result<Expected> {
    let mut dest = dest;
    let lent = region.and_then(|r| r.memh_for(dest.region_mut().as_ptr(), dest.cap()).map(|m| (r.clone(), m)));
    let target = match lent {
        Some((region, memh)) => Target::Lent { dest, _region: region, memh },
        // No pool buffer that large: it would be heap memory UCX registers itself.
        None if dest.cap() > crate::regpool::MAX_CHUNK_BYTES => {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "an unregistered destination over 256 MiB cannot take an AM value",
            ))
        }
        None => {
            let landing = crate::regpool::acquire_charged_if_room(crate::regpool::Use::Reply, dest.cap());
            Target::Landed { dest, landing }
        }
    };
    Ok(register(Key::Asked(tag), target))
}

/// Where the frame value tagged `tag` on `ep` goes; starts receiving it at
/// once if it is already here.
fn expect_frame_value(ep: &UcxEp, tag: u64, dest: Target) -> Expected {
    let key = (ep.ptr() as usize, tag);
    let expected = register(Key::Frame(key), dest);
    if let Some(status) = FAILED.with(|f| f.borrow().get(&key.0).copied()) {
        EXPECTED.with(|e| e.borrow_mut().remove(&Key::Frame(key)));
        expected.0.finish(State::Failed(status, 0));
        return expected;
    }
    if let Some(early) = EARLY.with(|e| e.borrow_mut().remove(&key)) {
        let slot = EXPECTED.with(|e| e.borrow_mut().remove(&Key::Frame(key))).expect("just registered");
        unsafe { receive(slot, early.desc, early.len, true, true, true) };
    }
    expected
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
            EXPECTED.with(|e| e.borrow_mut().remove(&self.0.key));
            if let Key::Frame(key) = self.0.key {
                ABANDONED.with(|a| a.borrow_mut().insert(key));
            }
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

/// A frame's value being received into one pool buffer (`start_value`).
pub(crate) struct FrameValue {
    owner: Rc<PooledBuf>,
    pending: Vec<Expected>,
}

/// Start receiving a frame's value into `pb`, a pool buffer of the value's
/// whole length. `segments` are its pieces in order, `(len, tag)`: tag 0 is the
/// next piece of `inline` (bytes that came on the stream), any other tag a
/// value on `ep`. Every piece is registered here, so the values of several
/// frames started before any is waited for are received together. Dropped
/// before `wait` ends, a receive under way keeps the buffer until it ends.
pub(crate) fn start_value(
    ep: &UcxEp,
    segments: &[(usize, u64)],
    inline: &[u8],
    mut pb: PooledBuf,
) -> io::Result<FrameValue> {
    debug_assert_eq!(pb.cap(), segments.iter().map(|s| s.0).sum::<usize>());
    let (base, memh) = {
        let (dest, reg) = pb.dest_and_reg();
        (dest.as_mut_ptr(), reg.map_or(ptr::null_mut(), |r| r.memh()))
    };
    let mut at = 0;
    let mut inline_at = 0;
    for &(len, tag) in segments {
        if tag == 0 {
            let piece = inline
                .get(inline_at..inline_at + len)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "frame value: inline piece past the frame"))?;
            pb.dest_mut()[at..at + len].copy_from_slice(piece);
            inline_at += len;
        }
        at += len;
    }
    let owner = Rc::new(pb);
    let mut pending = Vec::new();
    let mut at = 0;
    for &(len, tag) in segments {
        if tag != 0 {
            let dest = Target::Pooled { ptr: unsafe { base.add(at) }, len, memh, _owner: owner.clone() };
            pending.push(expect_frame_value(ep, tag, dest));
        }
        at += len;
    }
    // Spin like every other operation does after posting. A receive started
    // here, outside a progress pass, is otherwise advanced by the worker's
    // event wakeup — which the `self` transport (both ends on one worker, as in
    // the tests) never raises, so there it would not move at all.
    if pending.iter().any(|e| e.0.state.get() == State::Receiving) {
        let worker = WORKER.with(|w| w.get());
        for _ in 0..SPIN_ITERS {
            if pending.iter().all(|e| e.0.state.get() != State::Receiving) {
                break;
            }
            unsafe { ucp_worker_progress(worker) };
        }
        // The spin may have queued stream data a `recv_data_with` waits for.
        crate::ucx::endpoint::wake_stream_waiters();
    }
    Ok(FrameValue { owner, pending })
}

impl FrameValue {
    /// The whole value, once every piece is in.
    pub(crate) async fn wait(self) -> io::Result<PooledBuf> {
        let FrameValue { owner, pending } = self;
        let mut failure = None;
        for e in &pending {
            match e.arrived().await {
                Ok(Arrival::Received(_)) => {}
                Ok(Arrival::TooLong(n)) => {
                    failure = Some(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("frame value: {n} bytes arrived for a piece of another length"),
                    ));
                    break;
                }
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            }
        }
        for e in pending {
            e.finish().await;
        }
        if let Some(e) = failure {
            return Err(e);
        }
        Rc::try_unwrap(owner).map_err(|_| io::Error::other("frame value: buffer still held by a receive"))
    }
}

/// Bytes copied into a pool buffer to be sent (`Staged::of`).
static STAGED_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Bytes this process copied into a pool buffer before sending them as an
/// active message, because they came without a memh.
pub fn staged_bytes() -> u64 {
    STAGED_BYTES.load(std::sync::atomic::Ordering::Relaxed)
}

/// This thread's values copied into a pool buffer to be sent.
pub fn staged_count() -> u64 {
    STAGED.with(|c| c.get())
}

/// A value UCX may send: bytes with the memh of the pool buffer they live in.
/// Only `of` builds one, copying a value that has no memh into a pool buffer,
/// so UCX is never handed memory it would register itself. A value larger than
/// the largest chunk (256 MiB) has no pool buffer to go into and is sent as it
/// is, until large transfers are sent in segments.
struct Staged(RegBytes);

impl Staged {
    fn of(value: RegBytes) -> Self {
        if value.is_registered() || value.is_empty() || value.len() > crate::regpool::MAX_CHUNK_BYTES {
            return Self(value);
        }
        let mut pb = crate::regpool::acquire_charged_if_room(crate::regpool::Use::Staging, value.len());
        pb.dest_mut().copy_from_slice(&value);
        STAGED_BYTES.fetch_add(value.len() as u64, std::sync::atomic::Ordering::Relaxed);
        STAGED.with(|c| c.set(c.get() + 1));
        Self(pb.freeze())
    }
}

/// Send `value` as the AM `am_id` tagged `tag`. Completes once UCX no longer
/// needs `value` (for a rendezvous: once the receiver fetched or dropped it).
async fn send_am(ep: &UcxEp, am_id: u32, tag: u64, value: Staged, extra_flags: u32) -> io::Result<()> {
    let Staged(value) = value;
    let memh = value.memh();
    let mut payload = ManuallyDrop::new(value.into_bytes());
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
    p.flags = (ucp_send_am_flags::UCP_AM_SEND_FLAG_COPY_HEADER | protocol) as u32 | extra_flags;
    // A slab's own registration: UCX need not look the memory up.
    let registered = match memh {
        Some(memh) => {
            p.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_MEMH as u32;
            p.memh = memh.raw();
            true
        }
        None => false,
    };
    SENT.with(|c| {
        let (with, without) = c.get();
        c.set(if registered { (with + 1, without) } else { (with, without + 1) })
    });
    SENT_BYTES[!registered as usize].fetch_add(len as u64, std::sync::atomic::Ordering::Relaxed);
    let r = unsafe { ucp_am_send_nbx(ep.ptr(), am_id, header.as_ptr() as *const c_void, 8, ptr_v, len, &p) };
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

/// Send `value` as the value the receiver asked for with `tag`.
pub(crate) async fn send(ep: &UcxEp, tag: u64, value: RegBytes) -> io::Result<()> {
    send_am(ep, VALUE_AM_ID, tag, Staged::of(value), 0).await
}

/// Send `value` as the frame value tagged `tag`. The receiver keys it by
/// the endpoint it arrived on, which the reply flag hands its handler.
pub(crate) async fn send_frame_value(ep: &UcxEp, tag: u64, value: RegBytes) -> io::Result<()> {
    send_am(ep, FRAME_VALUE_AM_ID, tag, Staged::of(value), ucp_send_am_flags::UCP_AM_SEND_FLAG_REPLY as u32).await
}
