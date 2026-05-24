//! Session-establishment handshake between client and daemon.
//!
//! # Lifecycle
//!
//! ```text
//!  client                                      daemon
//!  ──────                                      ──────
//!  connect to /run/autumn-fuse/ring.sock       (listening)
//!  send HelloRequest (proto_version, caps,
//!                     desired sq/cq/buf sizes)
//!                                              receive HelloRequest
//!                                              negotiate (server picks
//!                                                effective sizes,
//!                                                clamps to limits)
//!                                              allocate SHM file
//!                                              initialise RingHeader
//!                                              send HelloResponse +
//!                                                SCM_RIGHTS(shm_fd)
//!  receive HelloResponse + shm_fd
//!  mmap shm_fd
//!  ready to submit SQEs
//! ```
//!
//! This module owns ONLY the message wire format + version/capability
//! negotiation logic. The actual Unix-socket `sendmsg` / `recvmsg`
//! with `SCM_RIGHTS` ancillary data lives in F180-B3 (`socket.rs`),
//! and the daemon-side allocation + accept loop lives in F180-B4
//! (autumn-fuse `ring_server.rs`).

use bytes::{Buf, BufMut};

use crate::header::{
    DEFAULT_BUF_POOL_SIZE, DEFAULT_BUF_SLOT_SIZE, DEFAULT_CAPABILITIES, DEFAULT_CQ_ENTRIES,
    DEFAULT_SQ_ENTRIES, RING_VERSION,
};

/// Magic preamble on every handshake message — separate from the SHM
/// `RING_MAGIC` so a malformed mmap can't be confused for a wire
/// message and vice-versa.
pub const HANDSHAKE_MAGIC: u32 = u32::from_le_bytes(*b"AUSH");

/// HelloRequest wire size: 4 (magic) + 2 (msg_type) + 2 (proto_version)
/// + 4 (sq_entries) + 4 (cq_entries) + 8 (buf_pool_size) + 4
/// (buf_slot_size) + 4 (capabilities) = 32 bytes.
pub const HELLO_REQUEST_SIZE: usize = 32;

/// HelloResponse wire size: 4 (magic) + 2 (msg_type) + 2 (status)
/// + 4 (sq_entries) + 4 (cq_entries) + 8 (buf_pool_size) + 4
/// (buf_slot_size) + 4 (capabilities) + 8 (session_id) = 40 bytes.
/// (Plus `shm_fd` carried out-of-band via SCM_RIGHTS, not in the
/// payload.)
pub const HELLO_RESPONSE_SIZE: usize = 40;

/// Message type byte — opaque tag distinguishing request vs response
/// (and any future handshake messages).
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MsgType {
    HelloRequest = 1,
    HelloResponse = 2,
}

impl MsgType {
    pub fn from_u16(v: u16) -> Option<Self> {
        match v {
            1 => Some(Self::HelloRequest),
            2 => Some(Self::HelloResponse),
            _ => None,
        }
    }
}

/// Status codes returned in HelloResponse.
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HelloStatus {
    Ok = 0,
    UnsupportedVersion = 1,
    /// Client requested capabilities daemon doesn't implement.
    UnsupportedCapabilities = 2,
    /// Daemon out of resources (max session count, no SHM disk space).
    OutOfResources = 3,
    /// Client request was malformed beyond version/cap mismatch.
    BadRequest = 4,
}

impl HelloStatus {
    pub fn from_u16(v: u16) -> Option<Self> {
        match v {
            0 => Some(Self::Ok),
            1 => Some(Self::UnsupportedVersion),
            2 => Some(Self::UnsupportedCapabilities),
            3 => Some(Self::OutOfResources),
            4 => Some(Self::BadRequest),
            _ => None,
        }
    }
}

/// Client → daemon "I'd like to open a ring" message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HelloRequest {
    pub proto_version: u16,
    pub sq_entries: u32,
    pub cq_entries: u32,
    pub buf_pool_size: u64,
    pub buf_slot_size: u32,
    /// Capability flags the client supports. Daemon picks the
    /// intersection with its own and reports it in the response.
    pub capabilities: u32,
}

impl HelloRequest {
    /// Construct a sensible default request — protocol version =
    /// `RING_VERSION`, default ring sizes, default capabilities.
    pub fn defaults() -> Self {
        Self {
            proto_version: RING_VERSION,
            sq_entries: DEFAULT_SQ_ENTRIES,
            cq_entries: DEFAULT_CQ_ENTRIES,
            buf_pool_size: DEFAULT_BUF_POOL_SIZE,
            buf_slot_size: DEFAULT_BUF_SLOT_SIZE,
            capabilities: DEFAULT_CAPABILITIES,
        }
    }

    pub fn encode(&self, dst: &mut [u8; HELLO_REQUEST_SIZE]) {
        let mut buf = &mut dst[..];
        buf.put_u32_le(HANDSHAKE_MAGIC);
        buf.put_u16_le(MsgType::HelloRequest as u16);
        buf.put_u16_le(self.proto_version);
        buf.put_u32_le(self.sq_entries);
        buf.put_u32_le(self.cq_entries);
        buf.put_u64_le(self.buf_pool_size);
        buf.put_u32_le(self.buf_slot_size);
        buf.put_u32_le(self.capabilities);
    }

    pub fn decode(src: &[u8]) -> Result<Self, HandshakeDecodeError> {
        if src.len() < HELLO_REQUEST_SIZE {
            return Err(HandshakeDecodeError::TooShort {
                got: src.len(),
                expected: HELLO_REQUEST_SIZE,
            });
        }
        let mut buf = &src[..HELLO_REQUEST_SIZE];
        let magic = buf.get_u32_le();
        if magic != HANDSHAKE_MAGIC {
            return Err(HandshakeDecodeError::BadMagic { got: magic });
        }
        let msg_type = buf.get_u16_le();
        let mt =
            MsgType::from_u16(msg_type).ok_or(HandshakeDecodeError::UnknownMsgType(msg_type))?;
        if mt != MsgType::HelloRequest {
            return Err(HandshakeDecodeError::WrongMsgType {
                got: mt,
                expected: MsgType::HelloRequest,
            });
        }
        let proto_version = buf.get_u16_le();
        let sq_entries = buf.get_u32_le();
        let cq_entries = buf.get_u32_le();
        let buf_pool_size = buf.get_u64_le();
        let buf_slot_size = buf.get_u32_le();
        let capabilities = buf.get_u32_le();
        Ok(Self {
            proto_version,
            sq_entries,
            cq_entries,
            buf_pool_size,
            buf_slot_size,
            capabilities,
        })
    }
}

/// Daemon → client response. On success `status == Ok` and `shm_fd`
/// is passed via SCM_RIGHTS alongside the wire payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HelloResponse {
    pub status: HelloStatus,
    /// Effective sizes the daemon allocated — may be smaller than what
    /// the client requested if the daemon enforces caps.
    pub sq_entries: u32,
    pub cq_entries: u32,
    pub buf_pool_size: u64,
    pub buf_slot_size: u32,
    /// Capabilities the daemon will honor (intersection of client +
    /// daemon).
    pub capabilities: u32,
    /// Random session id; the daemon also writes it into the ring
    /// header so the client can sanity-check the SHM region matches
    /// the handshake.
    pub session_id: u64,
}

impl HelloResponse {
    /// Build a successful response from a (possibly-clamped) request.
    pub fn ok(req: &HelloRequest, session_id: u64) -> Self {
        Self {
            status: HelloStatus::Ok,
            sq_entries: req.sq_entries,
            cq_entries: req.cq_entries,
            buf_pool_size: req.buf_pool_size,
            buf_slot_size: req.buf_slot_size,
            capabilities: req.capabilities,
            session_id,
        }
    }

    /// Build a rejection response. Sizes/caps are zeroed because the
    /// client won't get a usable ring.
    pub fn reject(status: HelloStatus) -> Self {
        Self {
            status,
            sq_entries: 0,
            cq_entries: 0,
            buf_pool_size: 0,
            buf_slot_size: 0,
            capabilities: 0,
            session_id: 0,
        }
    }

    pub fn encode(&self, dst: &mut [u8; HELLO_RESPONSE_SIZE]) {
        let mut buf = &mut dst[..];
        buf.put_u32_le(HANDSHAKE_MAGIC);
        buf.put_u16_le(MsgType::HelloResponse as u16);
        buf.put_u16_le(self.status as u16);
        buf.put_u32_le(self.sq_entries);
        buf.put_u32_le(self.cq_entries);
        buf.put_u64_le(self.buf_pool_size);
        buf.put_u32_le(self.buf_slot_size);
        buf.put_u32_le(self.capabilities);
        buf.put_u64_le(self.session_id);
    }

    pub fn decode(src: &[u8]) -> Result<Self, HandshakeDecodeError> {
        if src.len() < HELLO_RESPONSE_SIZE {
            return Err(HandshakeDecodeError::TooShort {
                got: src.len(),
                expected: HELLO_RESPONSE_SIZE,
            });
        }
        let mut buf = &src[..HELLO_RESPONSE_SIZE];
        let magic = buf.get_u32_le();
        if magic != HANDSHAKE_MAGIC {
            return Err(HandshakeDecodeError::BadMagic { got: magic });
        }
        let msg_type = buf.get_u16_le();
        let mt =
            MsgType::from_u16(msg_type).ok_or(HandshakeDecodeError::UnknownMsgType(msg_type))?;
        if mt != MsgType::HelloResponse {
            return Err(HandshakeDecodeError::WrongMsgType {
                got: mt,
                expected: MsgType::HelloResponse,
            });
        }
        let status_raw = buf.get_u16_le();
        let status = HelloStatus::from_u16(status_raw)
            .ok_or(HandshakeDecodeError::UnknownStatus(status_raw))?;
        let sq_entries = buf.get_u32_le();
        let cq_entries = buf.get_u32_le();
        let buf_pool_size = buf.get_u64_le();
        let buf_slot_size = buf.get_u32_le();
        let capabilities = buf.get_u32_le();
        let session_id = buf.get_u64_le();
        Ok(Self {
            status,
            sq_entries,
            cq_entries,
            buf_pool_size,
            buf_slot_size,
            capabilities,
            session_id,
        })
    }
}

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum HandshakeDecodeError {
    #[error("buffer too short: need {expected} bytes, got {got}")]
    TooShort { got: usize, expected: usize },
    #[error("bad handshake magic: got {got:#x}")]
    BadMagic { got: u32 },
    #[error("unknown message type: {0}")]
    UnknownMsgType(u16),
    #[error("wrong message type: got {got:?}, expected {expected:?}")]
    WrongMsgType { got: MsgType, expected: MsgType },
    #[error("unknown status code: {0}")]
    UnknownStatus(u16),
}

/// Daemon-side: validate a HelloRequest and either return an `ok`
/// HelloResponse or a rejection. Intended to be called from the
/// daemon's accept loop after deserialising the request bytes.
///
/// Daemon-side limits are passed in by the caller so production /
/// tests / mocks can configure them differently.
pub struct DaemonLimits {
    pub max_sq_entries: u32,
    pub max_cq_entries: u32,
    pub max_buf_pool_size: u64,
    pub max_buf_slot_size: u32,
    pub supported_capabilities: u32,
}

impl DaemonLimits {
    pub fn defaults() -> Self {
        Self {
            // Allow up to 16k entries; handshake's u32 limits this anyway.
            max_sq_entries: 16 * 1024,
            max_cq_entries: 16 * 1024,
            // Up to 1 GiB of pinned buffer pool.
            max_buf_pool_size: 1024 * 1024 * 1024,
            // Up to 16 MiB per slot.
            max_buf_slot_size: 16 * 1024 * 1024,
            supported_capabilities: DEFAULT_CAPABILITIES,
        }
    }
}

/// Apply limits / version checks. Returns an `Ok` response with the
/// negotiated sizes (clamped to `limits`) and the assigned
/// `session_id` on success, or a rejection response with the
/// appropriate status on failure.
pub fn negotiate(req: &HelloRequest, limits: &DaemonLimits, session_id: u64) -> HelloResponse {
    if req.proto_version != RING_VERSION {
        return HelloResponse::reject(HelloStatus::UnsupportedVersion);
    }
    if req.sq_entries < 2
        || !req.sq_entries.is_power_of_two()
        || req.cq_entries < 2
        || !req.cq_entries.is_power_of_two()
        || req.buf_slot_size == 0
        || (req.buf_slot_size as u64) > req.buf_pool_size
    {
        return HelloResponse::reject(HelloStatus::BadRequest);
    }
    let needed_caps = req.capabilities;
    if (limits.supported_capabilities & needed_caps) != needed_caps {
        return HelloResponse::reject(HelloStatus::UnsupportedCapabilities);
    }
    // Clamp client-requested sizes to daemon limits. Power-of-2
    // requested sizes stay valid after `min` because both bounds are
    // power-of-2 (limits configured that way).
    let sq_entries = req.sq_entries.min(limits.max_sq_entries);
    let cq_entries = req.cq_entries.min(limits.max_cq_entries);
    let buf_pool_size = req.buf_pool_size.min(limits.max_buf_pool_size);
    let buf_slot_size = req
        .buf_slot_size
        .min(limits.max_buf_slot_size)
        .min(buf_pool_size as u32);
    HelloResponse {
        status: HelloStatus::Ok,
        sq_entries,
        cq_entries,
        buf_pool_size,
        buf_slot_size,
        capabilities: req.capabilities,
        session_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_request_round_trip() {
        let req = HelloRequest::defaults();
        let mut buf = [0u8; HELLO_REQUEST_SIZE];
        req.encode(&mut buf);
        let decoded = HelloRequest::decode(&buf).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn hello_response_round_trip_ok() {
        let req = HelloRequest::defaults();
        let resp = HelloResponse::ok(&req, 0xdead_beef_cafe_babe);
        let mut buf = [0u8; HELLO_RESPONSE_SIZE];
        resp.encode(&mut buf);
        let decoded = HelloResponse::decode(&buf).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn hello_response_round_trip_reject() {
        let resp = HelloResponse::reject(HelloStatus::UnsupportedVersion);
        let mut buf = [0u8; HELLO_RESPONSE_SIZE];
        resp.encode(&mut buf);
        let decoded = HelloResponse::decode(&buf).unwrap();
        assert_eq!(resp, decoded);
        assert_eq!(decoded.status, HelloStatus::UnsupportedVersion);
        assert_eq!(decoded.session_id, 0);
    }

    #[test]
    fn hello_request_decode_too_short() {
        let buf = [0u8; HELLO_REQUEST_SIZE - 1];
        let err = HelloRequest::decode(&buf).unwrap_err();
        assert!(matches!(err, HandshakeDecodeError::TooShort { .. }));
    }

    #[test]
    fn hello_request_decode_bad_magic() {
        let mut buf = [0u8; HELLO_REQUEST_SIZE];
        buf[0..4].copy_from_slice(&[0xff, 0xff, 0xff, 0xff]);
        let err = HelloRequest::decode(&buf).unwrap_err();
        assert!(matches!(err, HandshakeDecodeError::BadMagic { .. }));
    }

    #[test]
    fn decode_rejects_response_bytes_as_request() {
        let resp = HelloResponse::reject(HelloStatus::Ok);
        let mut buf = [0u8; HELLO_RESPONSE_SIZE];
        resp.encode(&mut buf);
        // Try to decode as request — should fail because msg_type is
        // HelloResponse, not HelloRequest.
        let err = HelloRequest::decode(&buf).unwrap_err();
        assert!(matches!(err, HandshakeDecodeError::WrongMsgType { .. }));
    }

    #[test]
    fn negotiate_accepts_default_request() {
        let req = HelloRequest::defaults();
        let resp = negotiate(&req, &DaemonLimits::defaults(), 1);
        assert_eq!(resp.status, HelloStatus::Ok);
        assert_eq!(resp.sq_entries, req.sq_entries);
        assert_eq!(resp.cq_entries, req.cq_entries);
        assert_eq!(resp.buf_pool_size, req.buf_pool_size);
        assert_eq!(resp.buf_slot_size, req.buf_slot_size);
        assert_eq!(resp.session_id, 1);
    }

    #[test]
    fn negotiate_rejects_wrong_version() {
        let mut req = HelloRequest::defaults();
        req.proto_version = 99;
        let resp = negotiate(&req, &DaemonLimits::defaults(), 1);
        assert_eq!(resp.status, HelloStatus::UnsupportedVersion);
    }

    #[test]
    fn negotiate_rejects_unsupported_caps() {
        let mut req = HelloRequest::defaults();
        req.capabilities |= 1 << 31; // bogus bit
        let resp = negotiate(&req, &DaemonLimits::defaults(), 1);
        assert_eq!(resp.status, HelloStatus::UnsupportedCapabilities);
    }

    #[test]
    fn negotiate_rejects_non_power_of_two_sq() {
        let mut req = HelloRequest::defaults();
        req.sq_entries = 1000;
        let resp = negotiate(&req, &DaemonLimits::defaults(), 1);
        assert_eq!(resp.status, HelloStatus::BadRequest);
    }

    #[test]
    fn negotiate_clamps_sq_to_limit() {
        let mut req = HelloRequest::defaults();
        req.sq_entries = 64 * 1024; // larger than default limit
        let mut limits = DaemonLimits::defaults();
        limits.max_sq_entries = 4096;
        let resp = negotiate(&req, &limits, 1);
        assert_eq!(resp.status, HelloStatus::Ok);
        assert_eq!(resp.sq_entries, 4096);
    }

    #[test]
    fn negotiate_clamps_buf_pool_size() {
        let mut req = HelloRequest::defaults();
        req.buf_pool_size = 1u64 << 40; // 1 TiB
        let resp = negotiate(&req, &DaemonLimits::defaults(), 1);
        assert_eq!(resp.status, HelloStatus::Ok);
        assert_eq!(
            resp.buf_pool_size,
            DaemonLimits::defaults().max_buf_pool_size
        );
    }

    #[test]
    fn negotiate_rejects_zero_buf_slot() {
        let mut req = HelloRequest::defaults();
        req.buf_slot_size = 0;
        let resp = negotiate(&req, &DaemonLimits::defaults(), 1);
        assert_eq!(resp.status, HelloStatus::BadRequest);
    }
}
