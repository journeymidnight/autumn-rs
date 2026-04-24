//! Transport selection — Phase 1 skeleton; Phase 4 Task 20 fills `probe_ucx`.

use std::env;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Tcp,
    Ucx,
}

/// Decide which transport to bind based on `AUTUMN_TRANSPORT`:
///   - `tcp`   → always TCP
///   - `ucx`   → must succeed; panics if no RDMA capability
///   - `auto`  → probe RDMA, fall back to TCP if unavailable
///   - unset   → same as `auto`
///   - other   → panic with the bad value
pub fn decide() -> Decision {
    match env::var("AUTUMN_TRANSPORT").as_deref() {
        Ok("tcp") => Decision::Tcp,
        Ok("ucx") => probe_ucx().expect("AUTUMN_TRANSPORT=ucx but UCX unavailable"),
        Ok("auto") | Err(_) => probe_ucx().unwrap_or(Decision::Tcp),
        Ok(other) => panic!("invalid AUTUMN_TRANSPORT={other}"),
    }
}

/// RDMA-capable UCX transport names. If the probe finds at least one of
/// these in the worker's available transport set, UCX gives a real perf
/// win; otherwise we may as well stay on TCP.
#[cfg(feature = "ucx")]
const RDMA_TRANSPORTS: &[&str] =
    &["rc_mlx5", "rc_verbs", "dc_mlx5", "ud_mlx5", "ud_verbs"];

/// Probe UCX availability: returns `Some(Decision::Ucx)` only when an
/// RDMA-capable transport is present. We don't return `Ucx` for
/// pure-TCP UCX (no RDMA) because there's no reason to layer UCX over
/// TCP when we already have native TCP — we'd just add overhead.
///
/// Implementation: capture `ucp_context_print_info` output via Linux
/// `open_memstream`, then grep for any RDMA transport name. Cheap
/// (~ms) and self-contained — does NOT touch our process-global UCX
/// context (which is created lazily by the worker bootstrap), uses
/// its own short-lived context that gets cleaned up before return.
#[cfg(feature = "ucx")]
pub(crate) fn probe_ucx() -> Option<Decision> {
    use crate::ucx::ffi::*;
    use std::ffi::CString;
    use std::os::raw::c_char;
    use std::ptr;

    unsafe {
        let mut params: ucp_params_t = std::mem::zeroed();
        params.field_mask = ucp_params_field::UCP_PARAM_FIELD_FEATURES as u64;
        params.features = ucp_feature::UCP_FEATURE_STREAM as u64;

        let mut cfg: *mut ucp_config_t = ptr::null_mut();
        if ucp_config_read(ptr::null(), ptr::null(), &mut cfg) != ucs_status_t::UCS_OK {
            return None;
        }

        let mut ctx: ucp_context_h = ptr::null_mut();
        let st = ucp_init_version(UCP_API_MAJOR, UCP_API_MINOR, &params, cfg, &mut ctx);
        ucp_config_release(cfg);
        if st != ucs_status_t::UCS_OK {
            return None;
        }

        // Capture context info via open_memstream. ucp_context_print_info
        // writes a textual dump that includes "Transport: rc_mlx5" /
        // "Transport: tcp" lines we can grep.
        let mut buf: *mut c_char = ptr::null_mut();
        let mut size: libc::size_t = 0;
        let f = libc::open_memstream(&mut buf, &mut size);
        if f.is_null() {
            ucp_cleanup(ctx);
            return None;
        }
        ucp_context_print_info(ctx, f as *mut _);
        libc::fflush(f);
        libc::fclose(f);
        ucp_cleanup(ctx);

        if buf.is_null() {
            return None;
        }
        let info = CString::from_raw(buf).to_string_lossy().into_owned();

        let has_rdma = RDMA_TRANSPORTS.iter().any(|t| info.contains(t));
        if has_rdma {
            tracing::info!("autumn-transport: probe_ucx → RDMA available");
            Some(Decision::Ucx)
        } else {
            tracing::info!("autumn-transport: probe_ucx → no RDMA transport");
            None
        }
    }
}

#[cfg(not(feature = "ucx"))]
pub(crate) fn probe_ucx() -> Option<Decision> {
    None
}
