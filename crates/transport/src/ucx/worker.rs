//! Per-thread UCX worker bootstrap + progress task.
//!
//! UCX threading model (per spec §5.1):
//! - One process-global `ucp_context_h` (created lazily on first use).
//! - One `ucp_worker_h` per compio runtime thread, kept in thread-local
//!   storage and lazily created on first `connect`/`bind` from that thread.
//! - A long-lived progress task per compio runtime drives
//!   `ucp_worker_progress` every 50 µs. The task is bound to the runtime
//!   that started it and dies cleanly when the runtime drops; we re-spawn
//!   on the next runtime via the `Rc` strong-count sentinel below.
//!
//! ### Why polling instead of `ucp_worker_arm` + eventfd?
//!
//! The eventfd-driven model (spec §5.1's preferred design) wraps
//! `ucp_worker_get_efd()` in `compio::fs::AsyncFd` and waits via
//! `arm → fd.read → progress`. In single-thread mode this hit a
//! chicken-and-egg in initial integration tests (UCX won't signal the
//! eventfd until progress runs; progress is gated on the read await).
//! Resolving it cleanly needs careful drain-before-arm sequencing —
//! tracked as a Phase 5 perf task. 50 µs polling adds bounded latency
//! and is correct.

use crate::ucx::ffi::*;
use std::cell::RefCell;
use std::io;
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
        params.features = ucp_feature::UCP_FEATURE_STREAM as u64;

        let mut cfg: *mut ucp_config_t = ptr::null_mut();
        let st = unsafe { ucp_config_read(ptr::null(), ptr::null(), &mut cfg) };
        assert_eq!(st, ucs_status_t::UCS_OK, "ucp_config_read");

        let mut ctx: ucp_context_h = ptr::null_mut();
        let st = unsafe {
            ucp_init_version(UCP_API_MAJOR, UCP_API_MINOR, &params, cfg, &mut ctx)
        };
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
}

/// Run `f` against the calling thread's UcxThreadCtx, lazily initialising
/// the worker AND ensuring a progress task is alive in the current
/// compio runtime.
pub(crate) fn with_thread_ctx<R>(f: impl FnOnce(&mut UcxThreadCtx) -> R) -> R {
    UCX_THREAD.with(|cell| {
        {
            let mut borrow = cell.borrow_mut();
            if borrow.is_none() {
                let ctx = process_context();
                let worker = unsafe { create_worker(ctx) };
                *borrow = Some(UcxThreadCtx { worker });
                tracing::debug!(
                    thread = ?std::thread::current().id(),
                    worker = ?worker,
                    "autumn-transport ucx: worker created"
                );
            }
        }
        ensure_progress_task();
        let mut borrow = cell.borrow_mut();
        f(borrow.as_mut().expect("just initialised"))
    })
}

unsafe fn create_worker(ctx: *mut ucp_context) -> *mut ucp_worker {
    let mut wp: ucp_worker_params_t = std::mem::zeroed();
    wp.field_mask = ucp_worker_params_field::UCP_WORKER_PARAM_FIELD_THREAD_MODE as u64;
    wp.thread_mode = ucs_thread_mode_t::UCS_THREAD_MODE_SINGLE;
    let mut w: *mut ucp_worker = ptr::null_mut();
    let st = ucp_worker_create(ctx, &wp, &mut w);
    assert_eq!(st, ucs_status_t::UCS_OK, "ucp_worker_create");
    w
}

// ---- Progress task ----

thread_local! {
    /// Per-runtime sentinel: when the compio runtime drops, its spawned
    /// progress task drops its clone of this `Rc`, decrementing the count.
    /// We compare strong_count to detect "the previous runtime ended,
    /// re-spawn in the current one".
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

/// Send-safe wrapper around `*mut ucp_worker`.
///
/// SAFETY: Compio's `runtime::spawn` keeps the spawned future on the calling
/// thread (thread-per-core model), so although `*mut ucp_worker` is logically
/// !Send, it never crosses threads in practice. The `unsafe impl Send` only
/// satisfies the closure's `: 'static + Send` bound; if compio ever changes
/// to a work-stealing scheduler this assertion would become unsound and
/// would need to be replaced (e.g., with a thread-local lookup inside the
/// spawned task).
#[derive(Copy, Clone)]
struct WorkerPtr(*mut ucp_worker);
unsafe impl Send for WorkerPtr {}

fn spawn_progress_task(rc: std::rc::Rc<()>) {
    let worker = UCX_THREAD.with(|c| c.borrow().as_ref().map(|x| WorkerPtr(x.worker)));
    let Some(w) = worker else { return };
    compio::runtime::spawn(async move {
        // Move `rc` into the future so its strong_count tracks task liveness.
        let _keepalive = rc;
        loop {
            unsafe { while ucp_worker_progress(w.0) > 0 {} }
            compio::time::sleep(std::time::Duration::from_micros(50)).await;
        }
    })
    .detach();
}

// ---- Helpers used by endpoint.rs / listener.rs ----

/// Format a UCX status code for error messages.
pub(crate) fn ucs_err(st: ucs_status_t::Type, ctx: &str) -> io::Error {
    let cstr = unsafe { std::ffi::CStr::from_ptr(ucs_status_string(st)) };
    io::Error::other(format!("{ctx}: {}", cstr.to_string_lossy()))
}
