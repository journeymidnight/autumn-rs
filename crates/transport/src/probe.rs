//! Transport selection — CLI-string → TransportKind.
//!
//! The `AUTUMN_TRANSPORT` env-var probe (`decide()`, `probe_ucx()`) is gone.
//! Every binary now takes `--transport tcp|ucx` explicitly.
//! UCX runtime transport selection (e.g. `UCX_TLS`) is left to the UCX
//! library itself; Rust does not read or set those variables.

use crate::TransportKind;

/// Parse the `--transport` flag value. Accepts `"tcp"` or `"ucx"` only.
/// Returns `Err(s)` for any other input so the caller can emit a clean error.
pub fn parse_transport_flag(s: &str) -> Result<TransportKind, &str> {
    match s {
        "tcp" => Ok(TransportKind::Tcp),
        "ucx" => Ok(TransportKind::Ucx),
        other => Err(other),
    }
}
