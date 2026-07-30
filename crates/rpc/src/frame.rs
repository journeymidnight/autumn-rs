//! Wire protocol framing for autumn-rpc.
//!
//! ONE frame shape (F-WIRE-CRC-UNIFY, wire v28) — no flag-dependent variants:
//!
//! ```text
//! ┌───────────┬──────────┬───────┬──────────────┐
//! │ req_id    │ msg_type │ flags │ payload_len  │   header, 10 B
//! │ u32 LE    │ u8       │ u8    │ u32 LE       │
//! └───────────┴──────────┴───────┴──────────────┘
//! payload = [ctrl_len: u32 LE][ctrl …][crc32c: u32 LE][value …]
//! ```
//!
//! The CRC32C covers `header ++ ctrl_len ++ ctrl` — control bytes are ALWAYS
//! protected (there is no CRC flag bit; protection is structural). The `value`
//! tail is raw: bulk value integrity is the transport's job (UCX NIC ICRC /
//! TCP kernel checksum) plus the storage-layer checksums (WAL record CRC, SST
//! block CRC). `value_len = payload_len − 4 − ctrl_len − 4` and may be 0.
//!
//! Per-msg_type ctrl/value split:
//! - normal rkyv / binary RPCs and error envelopes: ctrl = whole body, no value.
//! - `MSG_GET_BULK` / `MSG_READ_BYTES_BULK` responses: ctrl = `[code:1][message…]`,
//!   value = the raw value bytes (recv'd straight into a `PooledBuf`).
//! - `MSG_PUT_BULK` requests: ctrl = `[put_bulk meta][key]`, value = raw value —
//!   the sender never CRC-scans the value.
//! - `MSG_APPEND` is the ONE deliberate exception (durability path): its bulk
//!   payload rides INSIDE ctrl, so append bytes keep in-transit CRC protection.
//!
//! Header-inclusive CRC closes two pre-v28 holes: a flipped `req_id` delivering
//! a valid-CRC response to the wrong caller, and a flipped CRC flag bit
//! silently disabling verification (the bit no longer exists).

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Frame header size in bytes.
pub const HEADER_LEN: usize = 10;

/// Size of the `ctrl_len` field that starts every payload.
pub const CTRL_PREFIX_LEN: usize = 4;

/// Size of the CRC32C field between ctrl and value.
pub const CRC_LEN: usize = 4;

/// Fixed per-frame payload overhead: `ctrl_len` field + CRC.
pub const CTRL_OVERHEAD: usize = CTRL_PREFIX_LEN + CRC_LEN;

/// Maximum payload size: 4 GB - 1 (u32::MAX).
/// Individual services should enforce their own practical limits.
pub const MAX_PAYLOAD_LEN: u32 = u32::MAX;

// Flag bits. Bit 3 (0x08) is RESERVED — it was FLAG_CRC before wire v28, when
// CRC presence was optional; protection is now structural, so the bit is gone.
pub const FLAG_RESPONSE: u8 = 0x01;
pub const FLAG_ERROR: u8 = 0x02;
pub const FLAG_STREAM_END: u8 = 0x04;

/// A single RPC frame on the wire.
#[derive(Debug, Clone)]
pub struct Frame {
    pub req_id: u32,
    pub msg_type: u8,
    pub flags: u8,
    /// Control bytes — the CRC-protected part (rkyv body / error envelope /
    /// bulk meta+key). For non-value-separable frames this is the whole logical
    /// payload, so existing handlers keep reading `frame.payload` unchanged.
    pub payload: Bytes,
    /// Raw value tail — NOT covered by the frame CRC (transport + storage
    /// integrity). Empty for non-value-separable frames.
    pub value: Bytes,
}

impl Frame {
    /// Create a new request frame (ctrl-only, no raw value tail).
    pub fn request(req_id: u32, msg_type: u8, payload: Bytes) -> Self {
        Self {
            req_id,
            msg_type,
            flags: 0,
            payload,
            value: Bytes::new(),
        }
    }

    /// Create a response frame (ctrl-only, no raw value tail).
    pub fn response(req_id: u32, msg_type: u8, payload: Bytes) -> Self {
        Self {
            req_id,
            msg_type,
            flags: FLAG_RESPONSE,
            payload,
            value: Bytes::new(),
        }
    }

    /// Create an error response frame. `payload` is the status envelope
    /// (`RpcError::encode_status`), carried in ctrl (CRC-protected).
    pub fn error(req_id: u32, msg_type: u8, payload: Bytes) -> Self {
        Self {
            req_id,
            msg_type,
            flags: FLAG_RESPONSE | FLAG_ERROR,
            payload,
            value: Bytes::new(),
        }
    }

    /// Create a value-separable request frame: `payload` = ctrl (CRC'd),
    /// `value` = raw tail (transport integrity). `MSG_PUT_BULK`'s shape.
    pub fn request_zc(req_id: u32, msg_type: u8, ctrl: Bytes, value: Bytes) -> Self {
        Self {
            req_id,
            msg_type,
            flags: 0,
            payload: ctrl,
            value,
        }
    }

    /// Create a value-separable response frame: `payload` = ctrl (CRC'd,
    /// `[code:1][message…]` for the bulk read responses), `value` = raw tail.
    pub fn response_zc(req_id: u32, msg_type: u8, ctrl: Bytes, value: Bytes) -> Self {
        Self {
            req_id,
            msg_type,
            flags: FLAG_RESPONSE,
            payload: ctrl,
            value,
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

    /// Encode this frame into one buffer:
    /// `[header][ctrl_len][ctrl][crc32c][value]`, CRC over
    /// `header ++ ctrl_len ++ ctrl`.
    pub fn encode(&self) -> Bytes {
        let ctrl_len = self.payload.len();
        let wire_payload_len = CTRL_OVERHEAD + ctrl_len + self.value.len();
        let mut buf = BytesMut::with_capacity(HEADER_LEN + wire_payload_len);
        buf.put_u32_le(self.req_id);
        buf.put_u8(self.msg_type);
        buf.put_u8(self.flags);
        buf.put_u32_le(wire_payload_len as u32);
        buf.put_u32_le(ctrl_len as u32);
        buf.extend_from_slice(&self.payload);
        let crc = crc32c::crc32c(&buf[..HEADER_LEN + CTRL_PREFIX_LEN + ctrl_len]);
        buf.put_u32_le(crc);
        buf.extend_from_slice(&self.value);
        buf.freeze()
    }

    /// Build + encode a (ctrl-only) response frame in ONE allocation, writing
    /// the ctrl bytes straight into the frame buffer via `write_payload`
    /// (which must append exactly `payload_len` bytes), then appending the
    /// CRC32C. Byte-for-byte identical to
    /// `Frame::response(req_id, msg_type, payload).encode()` without the
    /// intermediate payload `Bytes` (saves one alloc + one payload memcpy on
    /// the read hot path).
    pub fn encode_response_with<F: FnOnce(&mut BytesMut)>(
        req_id: u32,
        msg_type: u8,
        payload_len: usize,
        write_payload: F,
    ) -> Bytes {
        let wire_payload_len = CTRL_OVERHEAD + payload_len;
        let mut buf = BytesMut::with_capacity(HEADER_LEN + wire_payload_len);
        buf.put_u32_le(req_id);
        buf.put_u8(msg_type);
        buf.put_u8(FLAG_RESPONSE);
        buf.put_u32_le(wire_payload_len as u32);
        buf.put_u32_le(payload_len as u32);
        let payload_start = buf.len();
        write_payload(&mut buf);
        // Release-enforced (not debug-only): a `write_payload` that appends a
        // different count than `payload_len` would emit a frame whose header
        // `payload_len`, actual ctrl, and CRC position disagree — a malformed
        // frame that desyncs the peer's `FrameDecoder`. Fail loud rather than
        // ship a silently-bad frame.
        assert_eq!(
            buf.len() - payload_start,
            payload_len,
            "encode_response_with: write_payload appended {} bytes, expected {payload_len}",
            buf.len() - payload_start,
        );
        let crc = crc32c::crc32c(&buf[..]);
        buf.put_u32_le(crc);
        buf.freeze()
    }
}

/// Header + `ctrl_len` prefix for a vectored write (14 bytes). The caller
/// follows these bytes with the ctrl parts, the 4-byte CRC trailer
/// (`compute_ctrl_crc` over this head + the same ctrl parts), then the raw
/// value parts (if any). `ctrl_len`/`value_len` are the parts' total sizes.
pub fn encode_vectored_head(
    req_id: u32,
    msg_type: u8,
    flags: u8,
    ctrl_len: usize,
    value_len: usize,
) -> [u8; HEADER_LEN + CTRL_PREFIX_LEN] {
    let mut h = [0u8; HEADER_LEN + CTRL_PREFIX_LEN];
    h[0..4].copy_from_slice(&req_id.to_le_bytes());
    h[4] = msg_type;
    h[5] = flags;
    let wire_payload_len = (CTRL_OVERHEAD + ctrl_len + value_len) as u32;
    h[6..10].copy_from_slice(&wire_payload_len.to_le_bytes());
    h[10..14].copy_from_slice(&(ctrl_len as u32).to_le_bytes());
    h
}

/// Compute the frame CRC32C for a vectored write: over the 14-byte
/// `encode_vectored_head` bytes ++ the ctrl parts (NEVER the value parts).
/// Returns the 4-byte little-endian CRC field.
pub fn compute_ctrl_crc(head: &[u8], ctrl_parts: &[Bytes]) -> [u8; 4] {
    let mut crc = crc32c::crc32c(head);
    for p in ctrl_parts {
        crc = crc32c::crc32c_append(crc, p);
    }
    crc.to_le_bytes()
}

/// Build the complete head of a value-separable RESPONSE as ONE buffer:
/// `[header][ctrl_len][code:1][message][crc]`. The caller emits the raw value
/// as the following iovec(s); `value_len` only feeds the header's
/// `payload_len`. This is the bulk read response (`MSG_GET_BULK` /
/// `MSG_READ_BYTES_BULK`): status code + human-readable message ride in the
/// CRC-protected ctrl, the value is a raw tail the receiver lands in a
/// `PooledBuf`.
pub fn encode_bulk_response_head(
    req_id: u32,
    msg_type: u8,
    code: u8,
    message: &str,
    value_len: usize,
) -> Bytes {
    let ctrl_len = 1 + message.len();
    let wire_payload_len = CTRL_OVERHEAD + ctrl_len + value_len;
    let mut buf = BytesMut::with_capacity(HEADER_LEN + CTRL_PREFIX_LEN + ctrl_len + CRC_LEN);
    buf.put_u32_le(req_id);
    buf.put_u8(msg_type);
    buf.put_u8(FLAG_RESPONSE);
    buf.put_u32_le(wire_payload_len as u32);
    buf.put_u32_le(ctrl_len as u32);
    buf.put_u8(code);
    buf.extend_from_slice(message.as_bytes());
    let crc = crc32c::crc32c(&buf[..]);
    buf.put_u32_le(crc);
    buf.freeze()
}

/// Parse a bulk read-response ctrl: `[code:1][message…]`. `None` on empty ctrl.
pub fn parse_bulk_ctrl(ctrl: &[u8]) -> Option<(u8, &[u8])> {
    ctrl.split_first().map(|(c, m)| (*c, m))
}

/// Verified prologue of the frame at the front of a `FrameDecoder` — the
/// bulk fast paths use it to learn the value boundary and validate the control
/// bytes BEFORE recv'ing the value straight into its destination.
#[derive(Debug, Clone, Copy)]
pub struct BulkPrologue {
    pub req_id: u32,
    pub msg_type: u8,
    pub flags: u8,
    pub ctrl_len: usize,
    pub value_len: usize,
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
    /// Verifies the frame CRC32C over `header ++ ctrl_len ++ ctrl` before
    /// exposing anything; the raw value tail passes through unverified
    /// (transport integrity). The returned `Frame` carries ctrl in `payload`
    /// and the raw tail in `value`.
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
        let payload_len = payload_len as usize;

        let total = HEADER_LEN + payload_len;
        if self.buf.len() < total {
            // Reserve capacity for the rest of the frame to reduce reallocations.
            self.buf.reserve(total - self.buf.len());
            return Ok(None);
        }

        if payload_len < CTRL_OVERHEAD {
            return Err(FrameError::Malformed("payload shorter than ctrl_len+crc"));
        }
        let ctrl_len =
            u32::from_le_bytes(self.buf[10..14].try_into().unwrap()) as usize;
        if CTRL_OVERHEAD + ctrl_len > payload_len {
            return Err(FrameError::Malformed("ctrl_len exceeds payload"));
        }

        let crc_off = HEADER_LEN + CTRL_PREFIX_LEN + ctrl_len;
        let computed = crc32c::crc32c(&self.buf[..crc_off]);
        let stored = u32::from_le_bytes(self.buf[crc_off..crc_off + 4].try_into().unwrap());
        if stored != computed {
            return Err(FrameError::CrcMismatch { stored, computed });
        }

        let req_id = u32::from_le_bytes(self.buf[0..4].try_into().unwrap());
        let msg_type = self.buf[4];
        let flags = self.buf[5];
        let value_len = payload_len - CTRL_OVERHEAD - ctrl_len;

        self.buf.advance(HEADER_LEN + CTRL_PREFIX_LEN);
        let payload = self.buf.split_to(ctrl_len).freeze();
        self.buf.advance(CRC_LEN);
        let value = self.buf.split_to(value_len).freeze();

        Ok(Some(Frame {
            req_id,
            msg_type,
            flags,
            payload,
            value,
        }))
    }

    /// Bytes currently buffered (not yet decoded).
    pub fn buffered_len(&self) -> usize {
        self.buf.len()
    }

    /// Peek the next frame's header without consuming it. Returns
    /// `(req_id, msg_type, flags, payload_len)` once `HEADER_LEN` bytes are
    /// buffered. Lets the bulk fast paths decide whether to recv a raw value
    /// tail straight into its destination before `try_decode` would buffer
    /// the whole payload.
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

    /// Peek the front frame's `ctrl_len` field (needs `HEADER_LEN + 4` bytes
    /// buffered).
    pub fn peek_ctrl_len(&self) -> Option<usize> {
        if self.buf.len() < HEADER_LEN + CTRL_PREFIX_LEN {
            return None;
        }
        Some(u32::from_le_bytes(self.buf[10..14].try_into().unwrap()) as usize)
    }

    /// Peek the first `n` ctrl bytes of the front frame without consuming,
    /// once they are buffered. Lets a server read loop inspect a
    /// value-separable request's meta/key prefix (e.g. `MSG_PUT_BULK`'s
    /// `[meta][key]`) to gate the recv-into fast path BEFORE committing.
    pub fn peek_ctrl(&self, n: usize) -> Option<&[u8]> {
        let start = HEADER_LEN + CTRL_PREFIX_LEN;
        if self.buf.len() < start + n {
            return None;
        }
        Some(&self.buf[start..start + n])
    }

    /// bulk fast path: once the front frame's FULL prologue
    /// (`[header][ctrl_len][ctrl][crc]`) is buffered, verify the CRC and
    /// return the parsed prologue WITHOUT consuming anything.
    /// `Ok(None)` = need more bytes. A CRC mismatch or malformed ctrl_len is
    /// a hard frame error (same as `try_decode` would raise).
    pub fn peek_bulk_prologue(&self) -> Result<Option<BulkPrologue>, FrameError> {
        let Some((req_id, msg_type, flags, payload_len)) = self.peek_header() else {
            return Ok(None);
        };
        let payload_len = payload_len as usize;
        if payload_len < CTRL_OVERHEAD {
            return Err(FrameError::Malformed("payload shorter than ctrl_len+crc"));
        }
        let Some(ctrl_len) = self.peek_ctrl_len() else {
            return Ok(None);
        };
        if CTRL_OVERHEAD + ctrl_len > payload_len {
            return Err(FrameError::Malformed("ctrl_len exceeds payload"));
        }
        let crc_off = HEADER_LEN + CTRL_PREFIX_LEN + ctrl_len;
        if self.buf.len() < crc_off + CRC_LEN {
            return Ok(None);
        }
        let computed = crc32c::crc32c(&self.buf[..crc_off]);
        let stored = u32::from_le_bytes(self.buf[crc_off..crc_off + 4].try_into().unwrap());
        if stored != computed {
            return Err(FrameError::CrcMismatch { stored, computed });
        }
        Ok(Some(BulkPrologue {
            req_id,
            msg_type,
            flags,
            ctrl_len,
            value_len: payload_len - CTRL_OVERHEAD - ctrl_len,
        }))
    }

    /// Consume a prologue previously verified by `peek_bulk_prologue`, leaving
    /// the frame's raw value bytes (or the next frame) at the front.
    pub fn consume_bulk_prologue(&mut self, ctrl_len: usize) {
        self.buf
            .advance(HEADER_LEN + CTRL_PREFIX_LEN + ctrl_len + CRC_LEN);
    }

    /// Advance past `n` already-buffered bytes.
    pub fn consume(&mut self, n: usize) {
        self.buf.advance(n);
    }

    /// Copy up to `dest.len()` buffered bytes into `dest`, advancing the
    /// decoder by the amount moved. Returns the count. Used to drain a
    /// value's already-buffered prefix into a recv-into target before
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
    /// The frame's structural fields are inconsistent (payload shorter than
    /// the mandatory ctrl_len+crc, or ctrl_len pointing past the payload).
    #[error("malformed frame: {0}")]
    Malformed(&'static str),
    /// The frame CRC32C over `header ++ ctrl_len ++ ctrl` does not match.
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
        // wire = header + ctrl_len + ctrl + crc (no value).
        assert_eq!(encoded.len(), HEADER_LEN + CTRL_OVERHEAD + 11);

        let mut decoder = FrameDecoder::new();
        decoder.feed(&encoded);
        let decoded = decoder.try_decode().unwrap().unwrap();

        assert_eq!(decoded.req_id, 42);
        assert_eq!(decoded.msg_type, 7);
        assert_eq!(decoded.payload, Bytes::from_static(b"hello world"));
        assert!(decoded.value.is_empty());
    }

    #[test]
    fn encode_response_with_matches_response_encode() {
        // Byte-for-byte identical to the two-step `response(...).encode()`,
        // for an empty payload, a tiny payload, and a large one.
        for payload in [
            Bytes::new(),
            Bytes::from_static(b"\x00abcdefgh"),
            Bytes::from(vec![0xCD; 4096]),
        ] {
            let want = Frame::response(7, 2, payload.clone()).encode();
            let got = Frame::encode_response_with(7, 2, payload.len(), |b| {
                b.extend_from_slice(&payload);
            });
            assert_eq!(got, want, "payload len {}", payload.len());

            // And it still decodes (CRC verified) back to the payload.
            let mut decoder = FrameDecoder::new();
            decoder.feed(&got);
            let decoded = decoder.try_decode().unwrap().unwrap();
            assert!(decoded.is_response());
            assert_eq!(decoded.payload, payload);
        }
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
        decoder.feed(&encoded[..HEADER_LEN + 50]); // header + partial payload
        assert!(decoder.try_decode().unwrap().is_none());

        decoder.feed(&encoded[HEADER_LEN + 50..]); // rest
        let decoded = decoder.try_decode().unwrap().unwrap();
        assert_eq!(decoded.payload, payload);
    }

    /// Value-separable round trip: ctrl carries `[code][message]`, value is a
    /// raw tail; decoder splits them and verifies the ctrl CRC.
    #[test]
    fn bulk_frame_round_trip_splits_ctrl_and_value() {
        let value = Bytes::from(vec![0x5A; 4096]);
        let head = encode_bulk_response_head(9, 0x50, 0, "", value.len());
        let mut wire = BytesMut::new();
        wire.extend_from_slice(&head);
        wire.extend_from_slice(&value);

        let mut decoder = FrameDecoder::new();
        decoder.feed(&wire);
        let decoded = decoder.try_decode().unwrap().unwrap();
        assert!(decoded.is_response());
        let (code, msg) = parse_bulk_ctrl(&decoded.payload).unwrap();
        assert_eq!(code, 0);
        assert!(msg.is_empty());
        assert_eq!(decoded.value, value);
    }

    /// bulk error responses carry a readable message in the CRC'd ctrl.
    #[test]
    fn bulk_head_carries_error_message() {
        let head = encode_bulk_response_head(3, 0x50, 6, "eversion mismatch", 0);
        let mut decoder = FrameDecoder::new();
        decoder.feed(&head);
        let decoded = decoder.try_decode().unwrap().unwrap();
        let (code, msg) = parse_bulk_ctrl(&decoded.payload).unwrap();
        assert_eq!(code, 6);
        assert_eq!(msg, b"eversion mismatch");
        assert!(decoded.value.is_empty());
    }

    /// The vectored head + compute_ctrl_crc produce bytes the decoder accepts,
    /// and the value parts are NOT covered by the CRC.
    #[test]
    fn vectored_zc_request_round_trip() {
        let meta = Bytes::from_static(b"meta-and-key-bytes");
        let value = Bytes::from(vec![0xEE; 1024]);
        let head = encode_vectored_head(77, 0x51, 0, meta.len(), value.len());
        let crc = compute_ctrl_crc(&head, std::slice::from_ref(&meta));

        let mut wire = BytesMut::new();
        wire.extend_from_slice(&head);
        wire.extend_from_slice(&meta);
        wire.extend_from_slice(&crc);
        wire.extend_from_slice(&value);

        let mut decoder = FrameDecoder::new();
        decoder.feed(&wire);
        let decoded = decoder.try_decode().unwrap().unwrap();
        assert_eq!(decoded.req_id, 77);
        assert_eq!(decoded.msg_type, 0x51);
        assert_eq!(decoded.payload, meta);
        assert_eq!(decoded.value, value);

        // Corrupt a VALUE byte → still decodes fine (value is raw; its
        // integrity is the transport's + the storage layer's).
        let mut wire2 = wire.clone();
        let vstart = wire2.len() - 1024;
        wire2[vstart + 10] ^= 0xFF;
        let mut d2 = FrameDecoder::new();
        d2.feed(&wire2);
        assert!(d2.try_decode().unwrap().is_some());
    }

    /// Any flipped bit in the HEADER trips CRC verification — the pre-v28
    /// holes (req_id delivering to the wrong caller with a valid payload CRC;
    /// a flipped CRC flag bit disabling verification) are structurally closed.
    #[test]
    fn decoder_rejects_corrupted_header_and_ctrl() {
        let frame = Frame::request(1, 1, Bytes::from(vec![0u8; 256]));
        let encoded = frame.encode();

        // Flip one bit in every header byte position + a ctrl byte; each must
        // fail loud (CrcMismatch), never decode silently.
        for pos in [0usize, 4, 5, 11, HEADER_LEN + CTRL_PREFIX_LEN + 50] {
            let mut wire = encoded.to_vec();
            wire[pos] ^= 0x01;
            let mut decoder = FrameDecoder::new();
            decoder.feed(&wire);
            match decoder.try_decode() {
                Err(_) => {}
                Ok(v) => panic!("corrupted byte {pos} decoded silently: {:?}", v.is_some()),
            }
        }
    }

    /// peek_bulk_prologue: verifies the ctrl CRC without consuming; a partial
    /// buffer reports "need more"; consume_bulk_prologue leaves the value.
    #[test]
    fn bulk_prologue_peek_verify_consume() {
        let value = Bytes::from(vec![0x11; 512]);
        let head = encode_bulk_response_head(5, 0x50, 0, "", value.len());

        let mut decoder = FrameDecoder::new();
        decoder.feed(&head[..8]);
        assert!(decoder.peek_bulk_prologue().unwrap().is_none(), "partial header");
        decoder.feed(&head[8..]);
        let p = decoder.peek_bulk_prologue().unwrap().expect("full prologue");
        assert_eq!(p.req_id, 5);
        assert_eq!(p.ctrl_len, 1);
        assert_eq!(p.value_len, 512);

        decoder.feed(&value[..100]); // value arrives in pieces
        decoder.consume_bulk_prologue(p.ctrl_len);
        assert_eq!(decoder.buffered_len(), 100, "value prefix left at front");
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
        // Every frame carries ctrl_len + CRC — even an empty ctrl.
        assert_eq!(encoded.len(), HEADER_LEN + CTRL_OVERHEAD);

        let mut decoder = FrameDecoder::new();
        decoder.feed(&encoded);
        let decoded = decoder.try_decode().unwrap().unwrap();
        assert_eq!(decoded.payload.len(), 0);
        assert_eq!(decoded.value.len(), 0);
    }

    /// encode_vectored_head + compute_ctrl_crc match Frame::encode byte-for-
    /// byte for a ctrl-only frame (the send_vectored path must be
    /// indistinguishable on the wire from the single-buffer path).
    #[test]
    fn vectored_head_matches_encode() {
        let p1 = Bytes::from_static(b"hello");
        let p2 = Bytes::from(vec![0xab; 64]);
        let parts = vec![p1.clone(), p2.clone()];
        let ctrl_len: usize = parts.iter().map(|p| p.len()).sum();

        let head = encode_vectored_head(123, 45, 0, ctrl_len, 0);
        let crc = compute_ctrl_crc(&head, &parts);
        let mut wire = BytesMut::new();
        wire.extend_from_slice(&head);
        for p in &parts {
            wire.extend_from_slice(p);
        }
        wire.extend_from_slice(&crc);

        let mut whole = Vec::new();
        whole.extend_from_slice(&p1);
        whole.extend_from_slice(&p2);
        let want = Frame::request(123, 45, Bytes::from(whole)).encode();
        assert_eq!(&wire[..], &want[..]);
    }
}
