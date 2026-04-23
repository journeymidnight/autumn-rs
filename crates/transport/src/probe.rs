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

#[cfg(feature = "ucx")]
pub(crate) fn probe_ucx() -> Option<Decision> {
    // Phase 4 Task 20 fills this in (inspect ucp_context for RDMA transports).
    None
}

#[cfg(not(feature = "ucx"))]
pub(crate) fn probe_ucx() -> Option<Decision> {
    None
}
