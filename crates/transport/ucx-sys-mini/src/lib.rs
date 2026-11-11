#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(deref_nullptr)]
#![allow(clippy::missing_safety_doc)]
#![allow(clippy::too_many_arguments)]

//! FFI for UCP/UCX, loaded from `libucp.so.0` at run time.
//!
//! `bindings.rs` is bindgen's output over `<ucp/api/ucp.h>` (regenerate with
//! `--features regen`, see `build.rs`); its functions are fields of [`Ucp`],
//! filled by `dlopen`. Nothing links against UCX, so a binary built with the
//! `ucx` feature runs over TCP on hosts without it. The free
//! functions below forward to the process's one [`Ucp`], so callers keep the
//! C names; adding a function to the allowlist means adding its forwarder.
//!
//! Re-exports `libc` so consumers (`autumn-transport`) don't need a separate
//! dep for the `sockaddr` / `sockaddr_storage` types that UCX takes by pointer.

pub use libc;

use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::sync::OnceLock;

include!("bindings.rs");

/// The UCP library's soname: the system's UCX, with its transport modules.
pub const LIBRARY: &str = "libucp.so.0";

static UCP: OnceLock<Result<Ucp, String>> = OnceLock::new();

/// Loads [`LIBRARY`] once. The error names the library and why it is missing.
pub fn load() -> Result<&'static Ucp, &'static str> {
    UCP.get_or_init(|| {
        // RTLD_NOW: a libucp whose own UCX dependencies do not resolve (mixed
        // installs) fails here, not at its first call.
        use libloading::os::unix::{Library, RTLD_LOCAL, RTLD_NOW};
        let ucp = unsafe { Library::open(Some(LIBRARY), RTLD_NOW | RTLD_LOCAL).and_then(|lib| Ucp::from_library(lib)) };
        ucp.map_err(|e| format!("cannot load {LIBRARY} ({e}); install UCX or add its lib directory to LD_LIBRARY_PATH"))
    })
    .as_ref()
    .map_err(String::as_str)
}

fn ucp() -> &'static Ucp {
    match load() {
        Ok(ucp) => ucp,
        Err(e) => panic!("{e}"),
    }
}

pub unsafe fn ucs_status_string(status: ucs_status_t::Type) -> *const c_char {
    ucp().ucs_status_string(status)
}
pub unsafe fn ucp_config_read(
    env_prefix: *const c_char,
    filename: *const c_char,
    config_p: *mut *mut ucp_config_t,
) -> ucs_status_t::Type {
    ucp().ucp_config_read(env_prefix, filename, config_p)
}
pub unsafe fn ucp_config_release(config: *mut ucp_config_t) {
    ucp().ucp_config_release(config)
}
pub unsafe fn ucp_get_version(
    major_version: *mut c_uint,
    minor_version: *mut c_uint,
    release_number: *mut c_uint,
) {
    ucp().ucp_get_version(major_version, minor_version, release_number)
}
pub unsafe fn ucp_init_version(
    api_major_version: c_uint,
    api_minor_version: c_uint,
    params: *const ucp_params_t,
    config: *const ucp_config_t,
    context_p: *mut ucp_context_h,
) -> ucs_status_t::Type {
    ucp().ucp_init_version(
        api_major_version,
        api_minor_version,
        params,
        config,
        context_p,
    )
}
pub unsafe fn ucp_cleanup(context_p: ucp_context_h) {
    ucp().ucp_cleanup(context_p)
}
pub unsafe fn ucp_context_print_info(context: ucp_context_h, stream: *mut FILE) {
    ucp().ucp_context_print_info(context, stream)
}
pub unsafe fn ucp_worker_create(
    context: ucp_context_h,
    params: *const ucp_worker_params_t,
    worker_p: *mut ucp_worker_h,
) -> ucs_status_t::Type {
    ucp().ucp_worker_create(context, params, worker_p)
}
pub unsafe fn ucp_worker_destroy(worker: ucp_worker_h) {
    ucp().ucp_worker_destroy(worker)
}
pub unsafe fn ucp_worker_progress(worker: ucp_worker_h) -> c_uint {
    ucp().ucp_worker_progress(worker)
}
pub unsafe fn ucp_worker_get_efd(worker: ucp_worker_h, fd: *mut c_int) -> ucs_status_t::Type {
    ucp().ucp_worker_get_efd(worker, fd)
}
pub unsafe fn ucp_worker_arm(worker: ucp_worker_h) -> ucs_status_t::Type {
    ucp().ucp_worker_arm(worker)
}
pub unsafe fn ucp_worker_signal(worker: ucp_worker_h) -> ucs_status_t::Type {
    ucp().ucp_worker_signal(worker)
}
pub unsafe fn ucp_listener_create(
    worker: ucp_worker_h,
    params: *const ucp_listener_params_t,
    listener_p: *mut ucp_listener_h,
) -> ucs_status_t::Type {
    ucp().ucp_listener_create(worker, params, listener_p)
}
pub unsafe fn ucp_listener_destroy(listener: ucp_listener_h) {
    ucp().ucp_listener_destroy(listener)
}
pub unsafe fn ucp_listener_query(
    listener: ucp_listener_h,
    attr: *mut ucp_listener_attr_t,
) -> ucs_status_t::Type {
    ucp().ucp_listener_query(listener, attr)
}
pub unsafe fn ucp_conn_request_query(
    conn_request: ucp_conn_request_h,
    attr: *mut ucp_conn_request_attr_t,
) -> ucs_status_t::Type {
    ucp().ucp_conn_request_query(conn_request, attr)
}
pub unsafe fn ucp_ep_create(
    worker: ucp_worker_h,
    params: *const ucp_ep_params_t,
    ep_p: *mut ucp_ep_h,
) -> ucs_status_t::Type {
    ucp().ucp_ep_create(worker, params, ep_p)
}
pub unsafe fn ucp_ep_close_nbx(
    ep: ucp_ep_h,
    param: *const ucp_request_param_t,
) -> ucs_status_ptr_t {
    ucp().ucp_ep_close_nbx(ep, param)
}
pub unsafe fn ucp_ep_flush_nbx(
    ep: ucp_ep_h,
    param: *const ucp_request_param_t,
) -> ucs_status_ptr_t {
    ucp().ucp_ep_flush_nbx(ep, param)
}
pub unsafe fn ucp_mem_map(
    context: ucp_context_h,
    params: *const ucp_mem_map_params_t,
    memh_p: *mut ucp_mem_h,
) -> ucs_status_t::Type {
    ucp().ucp_mem_map(context, params, memh_p)
}
pub unsafe fn ucp_mem_unmap(context: ucp_context_h, memh: ucp_mem_h) -> ucs_status_t::Type {
    ucp().ucp_mem_unmap(context, memh)
}
pub unsafe fn ucp_worker_set_am_recv_handler(
    worker: ucp_worker_h,
    param: *const ucp_am_handler_param_t,
) -> ucs_status_t::Type {
    ucp().ucp_worker_set_am_recv_handler(worker, param)
}
pub unsafe fn ucp_am_send_nbx(
    ep: ucp_ep_h,
    id: c_uint,
    header: *const c_void,
    header_length: usize,
    buffer: *const c_void,
    count: usize,
    param: *const ucp_request_param_t,
) -> ucs_status_ptr_t {
    ucp().ucp_am_send_nbx(ep, id, header, header_length, buffer, count, param)
}
pub unsafe fn ucp_am_recv_data_nbx(
    worker: ucp_worker_h,
    data_desc: *mut c_void,
    buffer: *mut c_void,
    count: usize,
    param: *const ucp_request_param_t,
) -> ucs_status_ptr_t {
    ucp().ucp_am_recv_data_nbx(worker, data_desc, buffer, count, param)
}
pub unsafe fn ucp_am_data_release(worker: ucp_worker_h, data: *mut c_void) {
    ucp().ucp_am_data_release(worker, data)
}
pub unsafe fn ucp_stream_send_nbx(
    ep: ucp_ep_h,
    buffer: *const c_void,
    count: usize,
    param: *const ucp_request_param_t,
) -> ucs_status_ptr_t {
    ucp().ucp_stream_send_nbx(ep, buffer, count, param)
}
pub unsafe fn ucp_stream_recv_nbx(
    ep: ucp_ep_h,
    buffer: *mut c_void,
    count: usize,
    length: *mut usize,
    param: *const ucp_request_param_t,
) -> ucs_status_ptr_t {
    ucp().ucp_stream_recv_nbx(ep, buffer, count, length, param)
}
pub unsafe fn ucp_stream_recv_data_nb(ep: ucp_ep_h, length: *mut usize) -> ucs_status_ptr_t {
    ucp().ucp_stream_recv_data_nb(ep, length)
}
pub unsafe fn ucp_request_check_status(request: *mut c_void) -> ucs_status_t::Type {
    ucp().ucp_request_check_status(request)
}
pub unsafe fn ucp_request_cancel(worker: ucp_worker_h, request: *mut c_void) {
    ucp().ucp_request_cancel(worker, request)
}
pub unsafe fn ucp_stream_data_release(ep: ucp_ep_h, data: *mut c_void) {
    ucp().ucp_stream_data_release(ep, data)
}
pub unsafe fn ucp_request_free(request: *mut c_void) {
    ucp().ucp_request_free(request)
}
pub unsafe fn ucp_ep_query(ep: ucp_ep_h, attr: *mut ucp_ep_attr_t) -> ucs_status_t::Type {
    ucp().ucp_ep_query(ep, attr)
}
