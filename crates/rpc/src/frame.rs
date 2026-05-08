//! Wire protocol framing for autumn-rpc.
//!
//! Frame format (10-byte header + payload):
//! ```text
//! ┌───────────┬──────────┬───────────┬──────────────┬─────────────────┐
//! │ req_id    │ msg_type │ flags     │ payload_len  │ payload         │
//! │ u32 LE    │ u8       │ u8        │ u32 LE       │ N bytes         │
//! └───────────┴──────────┴───────────┴──────────────┴─────────────────┘
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Frame header size in bytes.
pub const HEADER_LEN: usize = 10;

/// Maximum payload size: 4 GB - 1 (u32::MAX).
/// Individual services should enforce their own practical limits.
pub const MAX_PAYLOAD_LEN: u32 = u32::MAX;

// Flag bits
pub const FLAG_RESPONSE: u8 = 0x01;
pub const FLAG_ERROR: u8 = 0x02;
pub const FLAG_STREAM_END: u8 = 0x04;
/// F161 (DEFERRED — see feature_list.md): wire-format flag bit reserved for
/// the V1 frame format with a per-frame CRC32C trailer. The decoder
/// supports both V0 (no CRC) and V1 (with CRC verify), but the encoder
/// currently emits V0 only. F161's audit identified the closure as
/// valuable defense-in-depth against TCP CRC collisions / NIC offload
/// bugs / cosmic-ray corruption in hot-path binary frames; enabling V1
/// requires a careful migration that the F161 prototype attempt
/// surfaced is non-trivial (the cluster broke when V1 was enabled
/// unconditionally, root cause not yet isolated). Re-attempt should
/// add a feature flag, ship the decoder support first, then flip the
/// encoder under controlled rollout.
pub const FLAG_CRC: u8 = 0x08;

/// A single RPC frame on the wire.
#[derive(Debug, Clone)]
pub struct Frame {
    pub req_id: u32,
    pub msg_type: u8,
    pub flags: u8,
    pub payload: Bytes,
}

impl Frame {
    /// Create a new request frame.
    pub fn request(req_id: u32, msg_type: u8, payload: Bytes) -> Self {
        Self {
            req_id,
            msg_type,
            flags: 0,
            payload,
        }
    }

    /// Create a response frame.
    pub fn response(req_id: u32, msg_type: u8, payload: Bytes) -> Self {
        Self {
            req_id,
            msg_type,
            flags: FLAG_RESPONSE,
            payload,
        }
    }

    /// Create an error response frame.
    pub fn error(req_id: u32, msg_type: u8, payload: Bytes) -> Self {
        Self {
            req_id,
            msg_type,
            flags: FLAG_RESPONSE | FLAG_ERROR,
            payload,
        }
    }

    pub fn is_response(&self) -> bool {
        self.flags & FLAG_RESPONSE != 0
    }

    pub fn is_error(&self) -> bool {
        self.flags & FLAG_ERROR != 0
    }

    pub fn is_stream_end(&self) -> bool {
        self.flags & FLAG_STREAM_END != 0
    }

    /// Encode this frame into bytes (header + payload).
    ///
    /// F163: dispatches to V0 (legacy, no CRC) or V1 (per-frame CRC32C
    /// trailer, FLAG_CRC set) based on the `AUTUMN_RPC_FRAME_V1` env var.
    /// Default is V0 — the V1 encoder rollout is gated to opt-in until
    /// the F161 first-attempt cluster break is root-caused. Decoder
    /// always supports both formats so a V1-enabled binary still talks
    /// to V0 peers and vice versa (within the constraint that
    /// V0-binary-on-V1-data fails inner-protocol decode; restart-all-
    /// together is the deployment model).
    pub fn encode(&self) -> Bytes {
        if v1_encoder_enabled() {
            self.encode_v1()
        } else {
            self.encode_v0()
        }
    }

    /// V0 encoder: legacy `[req_id 4][msg_type 1][flags 1][payload_len 4][payload N]`.
    /// Exposed for direct test access independent of env-var state.
    pub fn encode_v0(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(HEADER_LEN + self.payload.len());
        buf.put_u32_le(self.req_id);
        buf.put_u8(self.msg_type);
        buf.put_u8(self.flags);
        buf.put_u32_le(self.payload.len() as u32);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    /// V1 encoder: `[req_id 4][msg_type 1][flags|FLAG_CRC 1][payload_len=N+4 4][payload N][crc32c 4]`.
    /// `payload_len` includes the 4-byte CRC trailer; CRC32C covers
    /// `inner_payload` bytes only (header is already protected by TCP
    /// CRC + the FLAG_CRC sentinel; corruption of the header would
    /// either fail bounds-check at decode or land at a wrong stream
    /// position which the next frame's flag-bit check will catch).
    pub fn encode_v1(&self) -> Bytes {
        let crc = crc32c::crc32c(&self.payload);
        let on_wire_len = self.payload.len() + 4;
        let mut buf = BytesMut::with_capacity(HEADER_LEN + on_wire_len);
        buf.put_u32_le(self.req_id);
        buf.put_u8(self.msg_type);
        buf.put_u8(self.flags | FLAG_CRC);
        buf.put_u32_le(on_wire_len as u32);
        buf.extend_from_slice(&self.payload);
        buf.put_u32_le(crc);
        buf.freeze()
    }

    /// Encode only the header into a fixed-size array (for vectored writes).
    /// F163: dispatches V0/V1 based on `AUTUMN_RPC_FRAME_V1` env var. When
    /// V1, caller must follow the header bytes with the payload AND a
    /// 4-byte CRC trailer (use `compute_payload_crc`). The `payload_len`
    /// in the header already accounts for the 4-byte trailer in V1.
    pub fn encode_header(&self) -> [u8; HEADER_LEN] {
        let v1 = v1_encoder_enabled();
        let mut hdr = [0u8; HEADER_LEN];
        hdr[0..4].copy_from_slice(&self.req_id.to_le_bytes());
        hdr[4] = self.msg_type;
        hdr[5] = if v1 { self.flags | FLAG_CRC } else { self.flags };
        let len = if v1 { (self.payload.len() + 4) as u32 } else { self.payload.len() as u32 };
        hdr[6..10].copy_from_slice(&len.to_le_bytes());
        hdr
    }

    /// Build a request frame header without the payload (for vectored writes).
    /// F163: dispatches V0/V1 based on env var. `inner_payload_len` is the
    /// caller's payload bytes only; under V1 the wire `payload_len` field
    /// is `inner_payload_len + 4` and the caller MUST append a 4-byte CRC
    /// trailer (see `compute_payload_crc`).
    pub fn encode_request_header(
        req_id: u32,
        msg_type: u8,
        inner_payload_len: u32,
    ) -> [u8; HEADER_LEN] {
        let v1 = v1_encoder_enabled();
        let mut hdr = [0u8; HEADER_LEN];
        hdr[0..4].copy_from_slice(&req_id.to_le_bytes());
        hdr[4] = msg_type;
        hdr[5] = if v1 { FLAG_CRC } else { 0 };
        let len = if v1 { inner_payload_len + 4 } else { inner_payload_len };
        hdr[6..10].copy_from_slice(&len.to_le_bytes());
        hdr
    }
}

/// F163 / F165: cached `AUTUMN_RPC_FRAME_V1` env-var lookup. Reads once on
/// first access and memoises (so toggling the env mid-process has no effect
/// — the encoder choice must be process-stable to avoid mid-stream wire-
/// format changes that the decoder couldn't follow).
///
/// F165 flipped the default from V0 to V1: F164 verified V1 works
/// end-to-end (root-caused the F161/F163 "cluster break" as stale release
/// binaries, not a real wire-format bug). With V1 default-on, the 7
/// verified hot-path corruption surfaces from F161's audit (header field
/// corruption silently passed to handle_append, payload bytes corrupted
/// in transit, length-field poisoning, eversion bypass, etc.) are now
/// closed in production deployments — not just for users who opt in.
///
/// Opt-out: `AUTUMN_RPC_FRAME_V1=0` (or `false` / `no` / `off`) reverts
/// to legacy V0 wire format. Use this only if (a) you observe the ~10%
/// CRC32C compute cost is not acceptable for your workload AND (b) you
/// understand the corruption surfaces it leaves open. The whole cluster
/// (manager + extent-nodes + PSes + clients) MUST agree on the wire
/// version: per-process flips would create decode mismatches.
fn v1_encoder_enabled() -> bool {
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("AUTUMN_RPC_FRAME_V1")
            .map(|v| !matches!(v.as_str(), "0" | "false" | "no" | "off"))
            .unwrap_or(true)
    })
}

/// F161 (DEFERRED): compute CRC32C over a multi-segment payload by rolling
/// `crc32c_append`. Returns the 4-byte little-endian trailer to append to
/// the vectored write. Kept exposed so a future V1-enabling encoder can
/// use it; currently unused on production paths.
#[allow(dead_code)]
pub fn compute_payload_crc(parts: &[Bytes]) -> [u8; 4] {
    let mut crc: u32 = 0;
    for p in parts {
        crc = crc32c::crc32c_append(crc, p);
    }
    crc.to_le_bytes()
}

/// Decode state machine for reading frames from a byte stream.
pub struct FrameDecoder {
    buf: BytesMut,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::with_capacity(64 * 1024),
        }
    }

    /// Feed new data into the decoder buffer.
    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Try to decode the next complete frame from the buffer.
    /// Returns `None` if not enough data is available yet.
    ///
    /// F161: if `flags & FLAG_CRC != 0`, the trailing 4 bytes of the
    /// announced payload are a CRC32C over the inner payload bytes. The
    /// CRC is verified before the payload is exposed; mismatch returns
    /// `FrameError::CrcMismatch` and the corrupted bytes are dropped.
    /// The exposed payload excludes the CRC trailer (so the inner
    /// protocol decoders see exactly what the encoder sent).
    pub fn try_decode(&mut self) -> Result<Option<Frame>, FrameError> {
        if self.buf.len() < HEADER_LEN {
            return Ok(None);
        }

        let payload_len =
            u32::from_le_bytes(self.buf[6..10].try_into().unwrap());

        if payload_len > MAX_PAYLOAD_LEN {
            return Err(FrameError::PayloadTooLarge(payload_len));
        }

        let total = HEADER_LEN + payload_len as usize;
        if self.buf.len() < total {
            // Reserve capacity for the rest of the frame to reduce reallocations.
            self.buf.reserve(total - self.buf.len());
            return Ok(None);
        }

        let req_id = u32::from_le_bytes(self.buf[0..4].try_into().unwrap());
        let msg_type = self.buf[4];
        let flags = self.buf[5];

        self.buf.advance(HEADER_LEN);
        let mut payload = self.buf.split_to(payload_len as usize).freeze();

        if flags & FLAG_CRC != 0 {
            // F161 (DEFERRED): V1 frame with CRC trailer. Decoder support
            // is in place for forward compat; encoders currently emit V0
            // (FLAG_CRC unset) so this branch is dormant in production.
            if payload.len() < 4 {
                return Err(FrameError::CrcMissing);
            }
            let crc_pos = payload.len() - 4;
            let stored = u32::from_le_bytes(payload[crc_pos..].try_into().unwrap());
            let computed = crc32c::crc32c(&payload[..crc_pos]);
            if stored != computed {
                return Err(FrameError::CrcMismatch { stored, computed });
            }
            payload = payload.slice(..crc_pos);
        }

        Ok(Some(Frame {
            req_id,
            msg_type,
            flags,
            payload,
        }))
    }
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("payload too large: {0} bytes (max {MAX_PAYLOAD_LEN})")]
    PayloadTooLarge(u32),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// F161: V1 frame's announced payload is shorter than the 4-byte CRC trailer.
    #[error("V1 frame payload too short for CRC trailer")]
    CrcMissing,
    /// F161: V1 frame CRC32C does not match the inner payload.
    #[error("V1 frame CRC mismatch: stored={stored:#010x} computed={computed:#010x}")]
    CrcMismatch { stored: u32, computed: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trip() {
        let frame = Frame::request(42, 7, Bytes::from_static(b"hello world"));
        let encoded = frame.encode();
        // F165: V1 is the default; wire format is HEADER_LEN + payload + 4-byte CRC.
        assert_eq!(encoded.len(), HEADER_LEN + 11 + 4);

        let mut decoder = FrameDecoder::new();
        decoder.feed(&encoded);
        let decoded = decoder.try_decode().unwrap().unwrap();

        assert_eq!(decoded.req_id, 42);
        assert_eq!(decoded.msg_type, 7);
        // F165: V1 default → FLAG_CRC bit set on the surfaced frame.
        assert_eq!(decoded.flags & FLAG_CRC, FLAG_CRC);
        assert_eq!(decoded.payload, Bytes::from_static(b"hello world"));
    }

    #[test]
    fn decode_partial_header() {
        let frame = Frame::request(1, 2, Bytes::from_static(b"x"));
        let encoded = frame.encode();

        let mut decoder = FrameDecoder::new();
        decoder.feed(&encoded[..5]); // partial header
        assert!(decoder.try_decode().unwrap().is_none());

        decoder.feed(&encoded[5..]); // rest
        let decoded = decoder.try_decode().unwrap().unwrap();
        assert_eq!(decoded.req_id, 1);
        assert_eq!(decoded.payload, Bytes::from_static(b"x"));
    }

    #[test]
    fn decode_partial_payload() {
        let payload = Bytes::from(vec![0xAB; 100]);
        let frame = Frame::request(10, 3, payload.clone());
        let encoded = frame.encode();

        let mut decoder = FrameDecoder::new();
        decoder.feed(&encoded[..HEADER_LEN + 50]); // header + half payload
        assert!(decoder.try_decode().unwrap().is_none());

        decoder.feed(&encoded[HEADER_LEN + 50..]); // rest of payload + CRC
        let decoded = decoder.try_decode().unwrap().unwrap();
        assert_eq!(decoded.payload, payload);
    }

    /// F163: encode_v1 + decoder round-trip. Verifies the explicit V1
    /// encoder produces bytes the decoder accepts and CRC-validates.
    #[test]
    fn f163_encode_v1_decode_round_trip() {
        let payload = Bytes::from(b"hello-v1".to_vec());
        let frame = Frame::request(42, 7, payload.clone());
        let encoded = frame.encode_v1();
        // V1 frame size = HEADER_LEN + payload + 4-byte CRC trailer.
        assert_eq!(encoded.len(), HEADER_LEN + payload.len() + 4);

        let mut decoder = FrameDecoder::new();
        decoder.feed(&encoded);
        let decoded = decoder.try_decode().unwrap().unwrap();

        assert_eq!(decoded.req_id, 42);
        assert_eq!(decoded.msg_type, 7);
        assert_eq!(decoded.flags & FLAG_CRC, FLAG_CRC);
        assert_eq!(decoded.payload, payload);
    }

    /// F163: encode_v0 + decoder round-trip. Verifies the explicit V0
    /// encoder still works (the legacy path remains the production
    /// default until the V1 rollout is verified safe).
    #[test]
    fn f163_encode_v0_decode_round_trip() {
        let payload = Bytes::from(b"hello-v0".to_vec());
        let frame = Frame::request(99, 3, payload.clone());
        let encoded = frame.encode_v0();
        // V0 frame size = HEADER_LEN + payload (no CRC).
        assert_eq!(encoded.len(), HEADER_LEN + payload.len());

        let mut decoder = FrameDecoder::new();
        decoder.feed(&encoded);
        let decoded = decoder.try_decode().unwrap().unwrap();

        assert_eq!(decoded.req_id, 99);
        assert_eq!(decoded.flags & FLAG_CRC, 0); // V0 → no CRC bit
        assert_eq!(decoded.payload, payload);
    }

    /// F161 (DEFERRED): hand-construct a V1 frame and verify the decoder
    /// CRC-validates it. Encoder currently emits V0; this test exercises
    /// the decoder support path that's in place for forward-compat.
    #[test]
    fn f161_decoder_v1_round_trip_via_compute_payload_crc() {
        let p1 = Bytes::from_static(b"hello");
        let p2 = Bytes::from(vec![0xab; 64]);
        let p3 = Bytes::from_static(b"world");
        let parts = vec![p1.clone(), p2.clone(), p3.clone()];
        let inner_len: usize = parts.iter().map(|p| p.len()).sum();

        // Hand-construct a V1 frame: header (FLAG_CRC set, payload_len = inner+4),
        // payload, CRC trailer.
        let mut wire = BytesMut::new();
        wire.put_u32_le(99); // req_id
        wire.put_u8(3); // msg_type
        wire.put_u8(FLAG_CRC); // V1 marker
        wire.put_u32_le((inner_len + 4) as u32);
        for p in &parts {
            wire.extend_from_slice(p);
        }
        let crc_bytes = compute_payload_crc(&parts);
        wire.extend_from_slice(&crc_bytes);

        let mut decoder = FrameDecoder::new();
        decoder.feed(&wire);
        let decoded = decoder.try_decode().unwrap().unwrap();
        assert_eq!(decoded.req_id, 99);
        assert_eq!(decoded.flags & FLAG_CRC, FLAG_CRC);
        let mut expected = Vec::new();
        expected.extend_from_slice(&p1);
        expected.extend_from_slice(&p2);
        expected.extend_from_slice(&p3);
        assert_eq!(&decoded.payload[..], &expected[..]);
    }

    /// F161 (DEFERRED): a V1 frame with a flipped payload byte trips CRC.
    #[test]
    fn f161_decoder_rejects_corrupted_v1_payload() {
        let payload = vec![0u8; 256];
        let crc = crc32c::crc32c(&payload);

        let mut wire = BytesMut::new();
        wire.put_u32_le(1); // req_id
        wire.put_u8(1); // msg_type
        wire.put_u8(FLAG_CRC);
        wire.put_u32_le((payload.len() + 4) as u32);
        wire.extend_from_slice(&payload);
        wire.put_u32_le(crc);

        // Flip a payload byte.
        wire[HEADER_LEN + 50] ^= 0x01;

        let mut decoder = FrameDecoder::new();
        decoder.feed(&wire);
        match decoder.try_decode() {
            Err(FrameError::CrcMismatch { .. }) => {}
            other => panic!("expected CrcMismatch, got {other:?}"),
        }
    }

    #[test]
    fn decode_multiple_frames() {
        let f1 = Frame::request(1, 1, Bytes::from_static(b"aaa"));
        let f2 = Frame::response(2, 2, Bytes::from_static(b"bbb"));

        let mut all = BytesMut::new();
        all.extend_from_slice(&f1.encode());
        all.extend_from_slice(&f2.encode());

        let mut decoder = FrameDecoder::new();
        decoder.feed(&all);

        let d1 = decoder.try_decode().unwrap().unwrap();
        assert_eq!(d1.req_id, 1);
        assert!(!d1.is_response());

        let d2 = decoder.try_decode().unwrap().unwrap();
        assert_eq!(d2.req_id, 2);
        assert!(d2.is_response());

        assert!(decoder.try_decode().unwrap().is_none());
    }

    // payload_too_large test removed: MAX_PAYLOAD_LEN == u32::MAX, so no u32
    // value can exceed it. The guard `payload_len > MAX_PAYLOAD_LEN` is dead code
    // for the current wire format.

    #[test]
    fn response_and_error_flags() {
        let f = Frame::error(99, 5, Bytes::from_static(b"oops"));
        assert!(f.is_response());
        assert!(f.is_error());
        assert!(!f.is_stream_end());
    }

    #[test]
    fn empty_payload() {
        let frame = Frame::request(0, 0, Bytes::new());
        let encoded = frame.encode();
        // F165: V1 is the default; even an empty payload carries a 4-byte CRC trailer.
        assert_eq!(encoded.len(), HEADER_LEN + 4);

        let mut decoder = FrameDecoder::new();
        decoder.feed(&encoded);
        let decoded = decoder.try_decode().unwrap().unwrap();
        assert_eq!(decoded.payload.len(), 0);
    }

    #[test]
    fn encode_header_matches_encode() {
        let frame = Frame::request(123, 45, Bytes::from_static(b"test"));
        let full = frame.encode();
        let hdr = frame.encode_header();
        assert_eq!(&full[..HEADER_LEN], &hdr);
    }
}
