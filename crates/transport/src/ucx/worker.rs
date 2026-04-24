//! Per-thread UCX worker bootstrap + progress task.
//!
//! UCX threading model (per spec §5.1):
//! - One process-global `ucp_context_h` (created lazily on first use).
//! - One `ucp_worker_h` per compio runtime thread, kept in thread-local
//!   storage and lazily created on first `connect`/`bind` from that thread.
//! - The worker's eventfd (`ucp_worker_get_efd`) is wrapped in
//!   `compio::fs::AsyncFd` and a long-lived progress task drains
//!   completions whenever the kernel signals the eventfd.

use crate::ucx::ffi::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsFd, BorrowedFd, RawFd};
use std::ptr;
use std::sync::OnceLock;

// ---- Process-global context ----

/// Wrapper so `*mut ucp_context` can live in a `OnceLock` and be sent across
/// threads. UCX contexts are thread-safe for the read-only operations we do
/// from worker threads (worker creation, etc.).
struct CtxPtr(*mut ucp_context);
unsafe impl Send for CtxPtr {}
unsafe impl Sync for CtxPtr {}

static CTX: OnceLock<CtxPtr> = OnceLock::new();

pub(crate) fn process_context() -> *mut ucp_context {
    CTX.get_or_init(|| {
        let mut params: ucp_params_t = unsafe { std::mem::zeroed() };
        params.field_mask = ucp_params_field::UCP_PARAM_FIELD_FEATURES as u64;
        // STREAM = ucp_stream_*_nbx; WAKEUP = enables ucp_worker_get_efd so
        // we can integrate progress with compio's io_uring poll loop.
        params.features = (ucp_feature::UCP_FEATURE_STREAM
                         | ucp_feature::UCP_FEATURE_WAKEUP) as u64;

        let mut cfg: *mut ucp_config_t = ptr::null_mut();
        let st = unsafe { ucp_config_read(ptr::null(), ptr::null(), &mut cfg) };
        assert_eq!(st, ucs_status_t::UCS_OK, "ucp_config_read");

        let mut ctx: ucp_context_h = ptr::null_mut();
        let st = unsafe { ucp_init_version(
            UCP_API_MAJOR,
            UCP_API_MINOR,
            &params,
            cfg,
            &mut ctx,
        ) };
        unsafe { ucp_config_release(cfg) };
        assert_eq!(st, ucs_status_t::UCS_OK, "ucp_init_version");

        tracing::info!("autumn-transport ucx: ucp_context_h initialized");
        CtxPtr(ctx)
    })
    .0
}

// ---- Per-thread context ----

thread_local! {
    pub(crate) static UCX_THREAD: RefCell<Option<UcxThreadCtx>> =
        const { RefCell::new(None) };
}

pub(crate) struct UcxThreadCtx {
    pub worker: *mut ucp_worker,
    pub _efd: RawFd,
    pub ep_cache: HashMap<SocketAddr, *mut ucp_ep>,
}

/// Run `f` against the calling thread's UcxThreadCtx, lazily initialising
/// the worker AND spawning a per-runtime progress task on first use within
/// each compio runtime. The progress task drives `ucp_worker_progress`
/// every 50µs and dies cleanly when its runtime drops.
///
/// For test runners that spin up multiple compio runtimes on the same OS
/// thread, the spawned task from runtime N dies when runtime N drops; we
/// detect that on the next call (during runtime N+1) via an Rc strong-count
/// sentinel and re-spawn.
pub(crate) fn with_thread_ctx<R>(f: impl FnOnce(&mut UcxThreadCtx) -> R) -> R {
    UCX_THREAD.with(|cell| {
        {
            let mut borrow = cell.borrow_mut();
            if borrow.is_none() {
                let ctx = process_context();
                let (worker, efd) = unsafe { create_worker(ctx) };
                *borrow = Some(UcxThreadCtx {
                    worker,
                    _efd: efd,
                    ep_cache: HashMap::new(),
                });
                tracing::debug!(thread = ?std::thread::current().id(),
                                worker = ?worker, efd,
                                "autumn-transport ucx: worker created");
            }
        }
        ensure_progress_task();
        let mut borrow = cell.borrow_mut();
        f(borrow.as_mut().expect("just initialised"))
    })
}

thread_local! {
    static PROGRESS_GUARD: RefCell<Option<std::rc::Rc<()>>> =
        const { RefCell::new(None) };
}

fn ensure_progress_task() {
    PROGRESS_GUARD.with(|g| {
        let need_spawn = match g.borrow().as_ref() {
            Some(rc) => std::rc::Rc::strong_count(rc) == 1,
            None => true,
        };
        if need_spawn {
            let rc = std::rc::Rc::new(());
            *g.borrow_mut() = Some(rc.clone());
            spawn_progress_task(rc);
        }
    });
}

fn spawn_progress_task(rc: std::rc::Rc<()>) {
    let worker = UCX_THREAD.with(|c| c.borrow().as_ref().map(|x| WorkerPtr(x.worker)));
    let Some(w) = worker else { return };
    compio::runtime::spawn(async move {
        let _keepalive = rc;
        loop {
            unsafe { while ucp_worker_progress(w.0) > 0 {} }
            compio::time::sleep(std::time::Duration::from_micros(50)).await;
        }
    })
    .detach();
}

/// Drive `ucp_worker_progress` once on the calling thread's worker.
/// Cheap: it's a thread-local lookup + a fast UCX call.
pub(crate) fn progress_once() {
    UCX_THREAD.with(|cell| {
        if let Some(c) = cell.borrow().as_ref() {
            unsafe {
                while ucp_worker_progress(c.worker) > 0 {}
            }
        }
    });
}

unsafe fn create_worker(ctx: *mut ucp_context) -> (*mut ucp_worker, RawFd) {
    let mut wp: ucp_worker_params_t = std::mem::zeroed();
    wp.field_mask = ucp_worker_params_field::UCP_WORKER_PARAM_FIELD_THREAD_MODE as u64;
    wp.thread_mode = ucs_thread_mode_t::UCS_THREAD_MODE_SINGLE;
    let mut w: *mut ucp_worker = ptr::null_mut();
    let st = ucp_worker_create(ctx, &wp, &mut w);
    assert_eq!(st, ucs_status_t::UCS_OK, "ucp_worker_create");
    let mut efd: i32 = -1;
    let st = ucp_worker_get_efd(w, &mut efd);
    assert_eq!(st, ucs_status_t::UCS_OK, "ucp_worker_get_efd");
    (w, efd as RawFd)
}

// ---- Progress task ----

/// Wrapper around a UCX-managed eventfd so it can implement `AsFd` for
/// `compio::fs::AsyncFd`. Does NOT own the fd — UCX closes it during
/// `ucp_worker_destroy`.
struct UcxEventFd(RawFd);
impl AsFd for UcxEventFd {
    fn as_fd(&self) -> BorrowedFd<'_> {
        // SAFETY: caller guarantees efd outlives the AsyncFd.
        unsafe { BorrowedFd::borrow_raw(self.0) }
    }
}

/// Send-safe handle to a `*mut ucp_worker`. Only used to ship the pointer
/// into the spawned progress task — the task itself runs on the same compio
/// runtime thread that created the worker (compio is thread-per-core, so a
/// `spawn` stays local), so the !Send-ness of the pointer is fine in practice.
#[derive(Copy, Clone)]
struct WorkerPtr(*mut ucp_worker);
unsafe impl Send for WorkerPtr {}


// ---- Helpers used by endpoint.rs / listener.rs ----

/// Cache lookup; returns the cached endpoint for `addr` if any.
pub(crate) fn cached_ep(addr: &SocketAddr) -> Option<*mut ucp_ep> {
    UCX_THREAD.with(|cell| cell.borrow().as_ref().and_then(|c| c.ep_cache.get(addr).copied()))
}

/// Remember a freshly-created endpoint in this thread's cache.
pub(crate) fn cache_ep(addr: SocketAddr, ep: *mut ucp_ep) {
    UCX_THREAD.with(|cell| {
        if let Some(c) = cell.borrow_mut().as_mut() {
            c.ep_cache.insert(addr, ep);
        }
    });
}

/// Drop a broken endpoint from the cache (called on connection-reset paths).
pub(crate) fn forget_ep(addr: &SocketAddr) {
    UCX_THREAD.with(|cell| {
        if let Some(c) = cell.borrow_mut().as_mut() {
            c.ep_cache.remove(addr);
        }
    });
}

// Convenience: format a UCX status code for error messages.
pub(crate) fn ucs_err(st: ucs_status_t::Type, ctx: &str) -> io::Error {
    let cstr = unsafe { std::ffi::CStr::from_ptr(ucs_status_string(st)) };
    io::Error::other(format!("{ctx}: {}", cstr.to_string_lossy()))
}
