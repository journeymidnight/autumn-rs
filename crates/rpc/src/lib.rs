//! autumn-rpc: wire protocol framing for custom binary RPC.
//!
//! Provides a 10-byte-header wire protocol with request multiplexing IDs.
//!
//! # Wire Format
//!
//! ```text
//! [req_id: u32 LE][msg_type: u8][flags: u8][payload_len: u32 LE][payload]
//! ```

pub mod client;
pub mod error;
pub mod frame;
pub mod manager_rpc;
pub mod partition_rpc;
pub mod pool;

/// Re-exported so consumers of `RpcClient::call_into_dest(reg: Option<&RegisteredMem>)`
/// don't need a direct autumn-transport dependency. (Uninhabited stub on
/// non-ucx builds — `reg` is always `None` there.)
pub use autumn_transport::RegisteredMem;
/// Re-exported so `call_into_pooled` consumers (autumn-stream's StreamClient)
/// reference `autumn_rpc::PooledBuf` without a direct autumn-transport dep.
/// Transport-agnostic: registered on `ucx`, plain (copy-out) on TCP/no-ucx.
pub use autumn_transport::{regpool_acquire, PooledBuf};
pub use error::{Result, RpcError, StatusCode};
pub use frame::{Frame, FrameDecoder, HEADER_LEN};

/// Handler result type for RPC dispatch.
pub type HandlerResult = std::result::Result<bytes::Bytes, (StatusCode, String)>;

/// Msg type reserved for heartbeat ping/pong.
pub const MSG_TYPE_PING: u8 = 0xFF;

/// WIRE-1: build-time fingerprint of the wire-schema source files
/// (manager_rpc / partition_rpc / frame / extent_rpc). Same-commit
/// deploys share it; ANY wire-struct edit changes it. Exchanged via
/// `GetClusterIdResp.wire_fingerprint` and checked at every long-lived
/// process's startup — a mixed-version join refuses LOUDLY instead of
/// silently decoding garbage (rkyv has no cross-version compat; F275).
pub const WIRE_FINGERPRINT: &str = env!("AUTUMN_WIRE_FINGERPRINT");

/// WIRE-1: compare a peer-reported fingerprint against ours. Returns the
/// actionable refusal message on mismatch. Callers treat a TRANSPORT
/// failure fetching the fingerprint as best-effort-skip (the peer may be
/// briefly down; availability wins), but a SUCCESSFUL response with a
/// different fingerprint is a hard startup refusal.
pub fn wire_fingerprint_check(remote: &str) -> std::result::Result<(), String> {
    if remote == WIRE_FINGERPRINT {
        return Ok(());
    }
    Err(format!(
        "wire-schema fingerprint mismatch: local={WIRE_FINGERPRINT} manager={remote} — \
autumn-rs deploys are SAME-COMMIT (rkyv wire structs have no cross-version \
compatibility; a mixed deploy decodes garbage silently). Rebuild this binary/wheel \
from the same tree as the running cluster, or restart the whole cluster from one \
build (cluster.sh restart)."
    ))
}

#[cfg(test)]
mod wire_fingerprint_tests {
    #[test]
    fn fingerprint_is_nonempty_hex() {
        assert_eq!(super::WIRE_FINGERPRINT.len(), 16);
        assert!(super::WIRE_FINGERPRINT.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn check_accepts_self_rejects_other() {
        assert!(super::wire_fingerprint_check(super::WIRE_FINGERPRINT).is_ok());
        let err = super::wire_fingerprint_check("deadbeefdeadbeef").unwrap_err();
        assert!(err.contains("SAME-COMMIT"), "{err}");
        // An empty fingerprint (pre-WIRE-1 peer) must also refuse.
        assert!(super::wire_fingerprint_check("").is_err());
    }
}
