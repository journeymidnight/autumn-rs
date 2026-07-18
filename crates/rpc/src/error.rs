//! RPC error types and status codes.

use bytes::Bytes;

/// Status codes for RPC responses, kept minimal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StatusCode {
    Ok = 0,
    NotFound = 1,
    InvalidArgument = 2,
    FailedPrecondition = 3,
    Internal = 4,
    Unavailable = 5,
    AlreadyExists = 6,
    /// F-AUTHZ-1: the caller's connection is not authorized for the key range
    /// (no valid capability token, or the key falls outside its granted
    /// prefixes, or the token expired). Distinct from NotFound so a rogue
    /// client can't blind-retry-amplify a denied key as if it were absent.
    /// Appended (discriminant 7) so all prior codes stay wire-stable.
    PermissionDenied = 7,
    /// F-KEY-NS D7 Layer-A: a put-class write whose key falls in NO registered
    /// namespace prefix (the namespace does not exist). Distinct from NotFound
    /// (which is a read-miss on an existing keyspace) so the client can treat it
    /// as TERMINAL on the write path — refreshing routing can't create the
    /// namespace, same class as `PermissionDenied`. Appended (discriminant 8) so
    /// all prior codes stay wire-stable. NOTE: `error.rs` is intentionally NOT in
    /// the WIRE fingerprint (build.rs hashes only manager_rpc/partition_rpc/frame/
    /// extent_rpc/cap_token), so this addition keeps fp `76e8ba557f7fca2d`;
    /// same-commit stop-world deploy makes the new value safe.
    NamespaceUnknown = 8,
}

impl StatusCode {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Ok,
            1 => Self::NotFound,
            2 => Self::InvalidArgument,
            3 => Self::FailedPrecondition,
            4 => Self::Internal,
            5 => Self::Unavailable,
            6 => Self::AlreadyExists,
            7 => Self::PermissionDenied,
            8 => Self::NamespaceUnknown,
            _ => Self::Internal,
        }
    }
}

/// An RPC error returned to callers.
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("rpc status {code:?}: {message}")]
    Status { code: StatusCode, message: String },

    #[error("connection closed")]
    ConnectionClosed,

    #[error("request cancelled")]
    Cancelled,

    #[error("frame error: {0}")]
    Frame(#[from] crate::frame::FrameError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl RpcError {
    pub fn status(code: StatusCode, message: impl Into<String>) -> Self {
        Self::Status {
            code,
            message: message.into(),
        }
    }

    /// Encode an error into a wire payload: `[status_code: u8][message bytes]`
    pub fn encode_status(code: StatusCode, message: &str) -> Bytes {
        let mut buf = Vec::with_capacity(1 + message.len());
        buf.push(code as u8);
        buf.extend_from_slice(message.as_bytes());
        Bytes::from(buf)
    }

    /// Decode an error from a wire payload.
    pub fn decode_status(payload: &[u8]) -> (StatusCode, String) {
        if payload.is_empty() {
            return (StatusCode::Internal, String::new());
        }
        let code = StatusCode::from_u8(payload[0]);
        let message = String::from_utf8_lossy(&payload[1..]).into_owned();
        (code, message)
    }
}

pub type Result<T> = std::result::Result<T, RpcError>;
