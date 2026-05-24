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
/// Flag bit marking a frame that carries a per-frame CRC32C trailer over its
/// payload. Every frame produced by `Frame::encode` sets it — the CRC is the
/// standard frame protection (closes 7 audited hot-path corruption surfaces: a
/// flipped extent_id / eversion / commit / revision over a TCP link whose
/// 16-bit checksum + NIC offload bugs can pass such bit-flips through; see
/// feature_list.md F161 → F165). HW CRC32C (SSE4.2) keeps the cost negligible
/// for the small control frames it now covers.
///
/// The bit is NOT always set: the zero-copy value-response frame
/// (`encode_no_crc`, consumed by `call_into_dest` / `call_into_pooled`)
/// deliberately omits the trailer — recv-into-dest can't strip it, and value
/// integrity there is the transport's (UCX NIC ICRC / TCP kernel checksum, per
/// F219). The decoder dispatches on this bit to handle that one CRC-less shape.
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

    /// Encode this frame into bytes: `[req_id 4][msg_type 1][flags|FLAG_CRC 1]
    /// [payload_len = N+4  4][payload N][crc32c 4]`.
    ///
    /// Every frame carries a per-frame CRC32C trailer (FLAG_CRC set); the
    /// announced `payload_len` includes the 4 trailer bytes. CRC32C covers the
    /// payload only — a corrupted header either fails the decode bounds-check
    /// or lands at a wrong stream position that the next frame's flag-bit check
    /// catches. The sole CRC-less frame is the zero-copy value response, built
    /// by `encode_no_crc` (and hand-built in production) — see its doc.
    pub fn encode(&self) -> Bytes {
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

    /// CRC-less framing: `[req_id 4][msg_type 1][flags 1][payload_len 4][payload N]`.
    ///
    /// The ONLY frame shape without a CRC trailer, used exclusively for the
    /// zero-copy value response: `call_into_dest` / `call_into_pooled` recv the
    /// value straight into a caller dest and cannot strip a trailer, so these
    /// frames omit it (value integrity is the transport's — UCX NIC ICRC / TCP
    /// kernel checksum, per F219). Production builds the identical header by
    /// hand (`ps_zc_head` / `zc_read_head`); this method backs the ZC test
    /// fixtures. NOT an old wire version — there is one frame protocol.
    pub fn encode_no_crc(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(HEADER_LEN + self.payload.len());
        buf.put_u32_le(self.req_id);
        buf.put_u8(self.msg_type);
        buf.put_u8(self.flags);
        buf.put_u32_le(self.payload.len() as u32);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    /// Encode only the header into a fixed-size array (for vectored writes).
    /// Always sets FLAG_CRC — the caller MUST follow the header bytes with the
    /// payload AND a 4-byte CRC trailer (use `compute_payload_crc`). The
    /// header's `payload_len` already accounts for the trailer.
    pub fn encode_header(&self) -> [u8; HEADER_LEN] {
        let mut hdr = [0u8; HEADER_LEN];
        hdr[0..4].copy_from_slice(&self.req_id.to_le_bytes());
        hdr[4] = self.msg_type;
        hdr[5] = self.flags | FLAG_CRC;
        let len = (self.payload.len() + 4) as u32;
        hdr[6..10].copy_from_slice(&len.to_le_bytes());
        hdr
    }

    /// Build a request frame header without the payload (for vectored writes).
    /// Always sets FLAG_CRC. `inner_payload_len` is the caller's payload bytes
    /// only; the wire `payload_len` field is `inner_payload_len + 4` and the
    /// caller MUST append a 4-byte CRC trailer (see `compute_payload_crc`).
    pub fn encode_request_header(
        req_id: u32,
        msg_type: u8,
        inner_payload_len: u32,
    ) -> [u8; HEADER_LEN] {
        let mut hdr = [0u8; HEADER_LEN];
        hdr[0..4].copy_from_slice(&req_id.to_le_bytes());
        hdr[4] = msg_type;
        hdr[5] = FLAG_CRC;
        let len = inner_payload_len + 4;
        hdr[6..10].copy_from_slice(&len.to_le_bytes());
        hdr
    }
}

/// Compute CRC32C over a multi-segment payload by rolling `crc32c_append`.
/// Returns the 4-byte little-endian trailer to append to a vectored write —
/// `client::send_vectored` uses it to build the frame CRC trailer without
/// concatenating the payload segments.
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
    /// CRC frame (`flags & FLAG_CRC != 0`, the normal case): the trailing 4
    /// bytes of the announced payload are a CRC32C over the inner payload.
    /// The CRC is verified before the payload is exposed; mismatch returns
    /// `FrameError::CrcMismatch` and the corrupted bytes are dropped. The
    /// exposed payload excludes the trailer (so inner-protocol decoders see
    /// exactly what the encoder sent). CRC-less frames (FLAG_CRC unset) are
    /// the zero-copy value-response shape (`encode_no_crc`) and pass through
    /// unverified — integrity is the transport's there (F219).
    pub fn try_decode(&mut self) -> Result<Option<Frame>, FrameError> {
        if self.buf.len() < HEADER_LEN {
            return Ok(None);
        }

        let payload_len = u32::from_le_bytes(self.buf[6..10].try_into().unwrap());

        // Defensive bound, deliberately kept even though it is always false
        // today (`MAX_PAYLOAD_LEN == u32::MAX`, and `payload_len` is a u32). It
        // becomes load-bearing the moment `MAX_PAYLOAD_LEN` is lowered to a real
        // practical cap — at which point removing it would be a silent
        // regression. `#[allow]` rather than delete so that future-proofing
        // stays in place.
        #[allow(clippy::absurd_extreme_comparisons)]
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
            // CRC frame: verify + strip the CRC32C trailer. This is the normal
            // path; only the CRC-less ZC value-response (`encode_no_crc`) skips it.
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

    /// F216 zero-copy support — bytes currently buffered (not yet decoded).
    pub fn buffered_len(&self) -> usize {
        self.buf.len()
    }

    /// Peek the next frame's header without consuming it. Returns
    /// `(req_id, msg_type, flags, payload_len)` once `HEADER_LEN` bytes are
    /// buffered. Lets the read_loop decide whether to recv a value-response
    /// straight into a registered dest (`call_into_dest`) before `try_decode`
    /// would buffer the whole payload.
    pub fn peek_header(&self) -> Option<(u32, u8, u8, u32)> {
        if self.buf.len() < HEADER_LEN {
            return None;
        }
        Some((
            u32::from_le_bytes(self.buf[0..4].try_into().unwrap()),
            self.buf[4],
            self.buf[5],
            u32::from_le_bytes(self.buf[6..10].try_into().unwrap()),
        ))
    }

    /// Peek the first `n` payload bytes (the bytes after the `HEADER_LEN`
    /// header) without consuming, once they are buffered. Returns `None` until
    /// `HEADER_LEN + n` bytes are present. Lets a server read_loop read a
    /// value-separable request's meta/key prefix (e.g. `MSG_PUT_ZC`'s
    /// `[part_id][..][key_len][key]`) to locate the value boundary BEFORE
    /// deciding to recv the value straight into a registered dest.
    pub fn peek_payload(&self, n: usize) -> Option<&[u8]> {
        if self.buf.len() < HEADER_LEN + n {
            return None;
        }
        Some(&self.buf[HEADER_LEN..HEADER_LEN + n])
    }

    /// Advance past `n` already-buffered bytes (e.g. a peeked header).
    pub fn consume(&mut self, n: usize) {
        self.buf.advance(n);
    }

    /// Copy up to `dest.len()` buffered bytes into `dest`, advancing the
    /// decoder by the amount moved. Returns the count. Used to drain a
    /// value's already-buffered prefix into a recv-into-dest target before
    /// recv'ing the remainder straight off the wire.
    pub fn drain_into(&mut self, dest: &mut [u8]) -> usize {
        let n = dest.len().min(self.buf.len());
        dest[..n].copy_from_slice(&self.buf[..n]);
        self.buf.advance(n);
        n
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
    /// A CRC frame's announced payload is shorter than the 4-byte CRC trailer.
    #[error("frame payload too short for CRC trailer")]
    CrcMissing,
    /// A CRC frame's CRC32C does not match the inner payload.
    #[error("frame CRC mismatch: stored={stored:#010x} computed={computed:#010x}")]
    CrcMismatch { stored: u32, computed: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trip() {
        let frame = Frame::request(42, 7, Bytes::from_static(b"hello world"));
        let encoded = frame.encode();
        // Every frame carries a CRC: wire = HEADER_LEN + payload + 4-byte CRC.
        assert_eq!(encoded.len(), HEADER_LEN + 11 + 4);

        let mut decoder = FrameDecoder::new();
        decoder.feed(&encoded);
        let decoded = decoder.try_decode().unwrap().unwrap();

        assert_eq!(decoded.req_id, 42);
        assert_eq!(decoded.msg_type, 7);
        // FLAG_CRC set on every encoded frame.
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

    /// encode + decoder round-trip. Verifies the normal (CRC) encoder produces
    /// bytes the decoder accepts and CRC-validates.
    #[test]
    fn encode_crc_decode_round_trip() {
        let payload = Bytes::from(b"hello-crc".to_vec());
        let frame = Frame::request(42, 7, payload.clone());
        let encoded = frame.encode();
        // CRC frame size = HEADER_LEN + payload + 4-byte CRC trailer.
        assert_eq!(encoded.len(), HEADER_LEN + payload.len() + 4);

        let mut decoder = FrameDecoder::new();
        decoder.feed(&encoded);
        let decoded = decoder.try_decode().unwrap().unwrap();

        assert_eq!(decoded.req_id, 42);
        assert_eq!(decoded.msg_type, 7);
        assert_eq!(decoded.flags & FLAG_CRC, FLAG_CRC);
        assert_eq!(decoded.payload, payload);
    }

    /// encode_no_crc + decoder round-trip. `encode_no_crc` is the CRC-less
    /// encoder for the zero-copy value-response path; verify it round-trips
    /// through the decoder's no-CRC (FLAG_CRC-unset) branch.
    #[test]
    fn encode_no_crc_decode_round_trip() {
        let payload = Bytes::from(b"hello-no-crc".to_vec());
        let frame = Frame::request(99, 3, payload.clone());
        let encoded = frame.encode_no_crc();
        // CRC-less frame size = HEADER_LEN + payload (no trailer).
        assert_eq!(encoded.len(), HEADER_LEN + payload.len());

        let mut decoder = FrameDecoder::new();
        decoder.feed(&encoded);
        let decoded = decoder.try_decode().unwrap().unwrap();

        assert_eq!(decoded.req_id, 99);
        assert_eq!(decoded.flags & FLAG_CRC, 0); // CRC-less → no CRC bit
        assert_eq!(decoded.payload, payload);
    }

    /// Hand-construct a CRC frame and verify the decoder CRC-validates it.
    /// Exercises the decoder's CRC-verify path — the normal-frame format that
    /// every `Frame::encode` produces.
    #[test]
    fn decoder_crc_round_trip_via_compute_payload_crc() {
        let p1 = Bytes::from_static(b"hello");
        let p2 = Bytes::from(vec![0xab; 64]);
        let p3 = Bytes::from_static(b"world");
        let parts = vec![p1.clone(), p2.clone(), p3.clone()];
        let inner_len: usize = parts.iter().map(|p| p.len()).sum();

        // Hand-construct a CRC frame: header (FLAG_CRC set, payload_len = inner+4),
        // payload, CRC trailer.
        let mut wire = BytesMut::new();
        wire.put_u32_le(99); // req_id
        wire.put_u8(3); // msg_type
        wire.put_u8(FLAG_CRC); // CRC-frame marker
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

    /// A CRC frame with a flipped payload byte trips CRC verification.
    #[test]
    fn decoder_rejects_corrupted_payload() {
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
        // Every frame carries a 4-byte CRC trailer — even an empty payload.
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
