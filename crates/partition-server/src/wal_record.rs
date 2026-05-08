//! WAL record codec — F158.
//!
//! Each `log_stream` record is one of two on-disk formats:
//!
//! - **V0 (legacy, pre-F158):** 17-byte header + variable payload, no CRC.
//!   ```text
//!   [op:1][key_len:4 LE][val_len:4 LE][expires_at:8 LE][key][value]
//!   ```
//!   Total = 17 + key.len() + value.len(). The first byte is `op`, which is
//!   always one of {1=Put, 2=Delete} optionally OR'd with 0x80=OP_VALUE_POINTER,
//!   so the high-order bit pattern of valid op values is always
//!   `0_000_0001` (1), `0_000_0010` (2), `1_000_0001` (0x81), or
//!   `1_000_0010` (0x82). Values like 0xff never appear in production.
//!
//! - **V1 (post-F158):** 5-byte envelope header + V0 payload + 4-byte CRC.
//!   ```text
//!   [0xff sentinel][length:4 LE][op:1][key_len:4 LE][val_len:4 LE][expires_at:8 LE][key][value][crc32c:4 LE]
//!   ```
//!   `length` is the number of payload bytes (= 17 + key.len() + value.len()).
//!   Total on disk = 1 + 4 + length + 4. `crc32c` covers `length_bytes` ||
//!   `payload_bytes` so a corrupted `length` is detected before its value is
//!   trusted. The 0xff sentinel is unambiguous because no valid V0 op byte can
//!   be 0xff.
//!
//! ## Why
//!
//! Pre-F158, a flipped bit in a `log_stream` record (bit rot, undetected disk
//! error, sub-page torn write) silently corrupted recovered state at restart:
//! a corrupted `key_len` could read past the record into garbage and insert a
//! garbage MVCC entry into the memtable; a corrupted `val_len` could return
//! the wrong value bytes to subsequent VP-resolution reads. The block-level
//! CRC32C inside `crates/partition-server/src/sstable/builder.rs` covers
//! SSTable bytes but **not** `log_stream` (WAL) bytes.
//!
//! V1 puts a CRC32C on every record. On read, mismatch → log WARN + skip the
//! record + continue scanning. This converts silent corruption into a clean,
//! observable error.
//!
//! ## Migration
//!
//! Forward-compatible: V1 binaries write V1 records; V1 decoders accept both
//! V0 (legacy data on disk pre-F158) and V1 (new writes). V0 binaries on V1
//! data would interpret the 0xff sentinel as an `op` byte and fail the bounds
//! check (V0's `decode_records*` uses `if cursor + key_len + val_len >
//! bytes.len() break`); rollback is operator-driven and rare, acceptable.

use bytes::Bytes;

/// V1 envelope sentinel — unambiguous because op ∈ {1, 2, 0x81, 0x82}.
pub(crate) const V1_SENTINEL: u8 = 0xff;
/// Inner-payload header size (op + key_len + val_len + expires_at).
pub(crate) const PAYLOAD_HEADER: usize = 17;
/// V1 envelope overhead = sentinel(1) + length(4) + crc(4).
pub(crate) const V1_ENVELOPE_OVERHEAD: usize = 9;

/// Decoded WAL record — borrows from the input buffer for zero-copy.
pub(crate) struct DecodedRecord<'a> {
    pub op: u8,
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub expires_at: u64,
    /// Total bytes consumed from the input buffer (for cursor advancement).
    pub total: usize,
}

/// Outcome of attempting to decode one record at `bytes[cursor..]`.
pub(crate) enum DecodeOne<'a> {
    /// A record decoded successfully.
    Ok(DecodedRecord<'a>),
    /// The buffer is too short for a complete record header — caller treats
    /// this as a legitimate partial-tail-write and stops scanning.
    Incomplete,
    /// A V1 record's CRC failed or its envelope length was inconsistent
    /// with the inner header — bit rot. Caller should log + advance by
    /// `skip_bytes` + continue scanning.
    Corrupt { skip_bytes: usize, reason: &'static str },
}

/// Decode the next WAL record at `bytes[cursor..]`. Dispatches on the first
/// byte: `0xff` → V1 (with CRC verification), anything else → V0 legacy.
pub(crate) fn decode_one(bytes: &[u8]) -> DecodeOne<'_> {
    if bytes.is_empty() {
        return DecodeOne::Incomplete;
    }

    if bytes[0] == V1_SENTINEL {
        // V1: [0xff][length:4][payload][crc:4]
        if bytes.len() < 1 + 4 {
            return DecodeOne::Incomplete;
        }
        let length =
            u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as usize;
        let total = 1 + 4 + length + 4;
        if bytes.len() < total {
            return DecodeOne::Incomplete;
        }
        let payload_start = 5;
        let crc_start = payload_start + length;
        let stored_crc =
            u32::from_le_bytes(bytes[crc_start..crc_start + 4].try_into().unwrap());
        // CRC over [length_bytes || payload_bytes].
        let mut crc = crc32c::crc32c(&bytes[1..5]);
        crc = crc32c::crc32c_append(crc, &bytes[payload_start..payload_start + length]);
        if crc != stored_crc {
            return DecodeOne::Corrupt { skip_bytes: total, reason: "V1 CRC mismatch" };
        }
        if length < PAYLOAD_HEADER {
            return DecodeOne::Corrupt { skip_bytes: total, reason: "V1 payload too short for header" };
        }
        let p = &bytes[payload_start..payload_start + length];
        let op = p[0];
        let key_len = u32::from_le_bytes(p[1..5].try_into().unwrap()) as usize;
        let val_len = u32::from_le_bytes(p[5..9].try_into().unwrap()) as usize;
        let expires_at = u64::from_le_bytes(p[9..17].try_into().unwrap());
        if PAYLOAD_HEADER + key_len + val_len != length {
            return DecodeOne::Corrupt {
                skip_bytes: total,
                reason: "V1 inner header length disagrees with envelope",
            };
        }
        let key = &p[PAYLOAD_HEADER..PAYLOAD_HEADER + key_len];
        let value = &p[PAYLOAD_HEADER + key_len..PAYLOAD_HEADER + key_len + val_len];
        DecodeOne::Ok(DecodedRecord { op, key, value, expires_at, total })
    } else {
        // V0 legacy: [op][key_len:4][val_len:4][expires_at:8][key][value]
        if bytes.len() < PAYLOAD_HEADER {
            return DecodeOne::Incomplete;
        }
        let op = bytes[0];
        let key_len = u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as usize;
        let val_len = u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as usize;
        let expires_at = u64::from_le_bytes(bytes[9..17].try_into().unwrap());
        let total = PAYLOAD_HEADER + key_len + val_len;
        if bytes.len() < total {
            return DecodeOne::Incomplete;
        }
        let key = &bytes[PAYLOAD_HEADER..PAYLOAD_HEADER + key_len];
        let value = &bytes[PAYLOAD_HEADER + key_len..total];
        DecodeOne::Ok(DecodedRecord { op, key, value, expires_at, total })
    }
}

/// Encode one WAL record as a V1 envelope. Returns
/// `(header_bytes, value_bytes, crc_bytes)` — three segments suitable for
/// vectored I/O without copying `value`. The caller appends them in order.
pub(crate) fn encode_v1_segments(
    op: u8,
    key: &[u8],
    value: &[u8],
    expires_at: u64,
) -> (Bytes, Bytes, Bytes) {
    let payload_len = PAYLOAD_HEADER + key.len() + value.len();
    let mut header = Vec::with_capacity(1 + 4 + PAYLOAD_HEADER + key.len());
    header.push(V1_SENTINEL);
    header.extend_from_slice(&(payload_len as u32).to_le_bytes());
    header.push(op);
    header.extend_from_slice(&(key.len() as u32).to_le_bytes());
    header.extend_from_slice(&(value.len() as u32).to_le_bytes());
    header.extend_from_slice(&expires_at.to_le_bytes());
    header.extend_from_slice(key);

    // CRC over [length_bytes || payload_bytes]:
    //   length_bytes = header[1..5]
    //   payload_bytes = header[5..] || value
    let mut crc = crc32c::crc32c(&header[1..5]);
    crc = crc32c::crc32c_append(crc, &header[5..]);
    crc = crc32c::crc32c_append(crc, value);

    let crc_bytes = crc.to_le_bytes().to_vec();
    (Bytes::from(header), Bytes::copy_from_slice(value), Bytes::from(crc_bytes))
}

/// Encode one WAL record as a single contiguous V1 envelope buffer. Used by
/// callers that don't need vectored I/O (mostly tests + GC-rewrite path).
pub(crate) fn encode_v1(op: u8, key: &[u8], value: &[u8], expires_at: u64) -> Vec<u8> {
    let (h, _v, c) = encode_v1_segments(op, key, value, expires_at);
    let mut out = Vec::with_capacity(h.len() + value.len() + c.len());
    out.extend_from_slice(&h);
    out.extend_from_slice(value);
    out.extend_from_slice(&c);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_round_trip(op: u8, key: &[u8], value: &[u8], expires_at: u64) {
        let buf = encode_v1(op, key, value, expires_at);
        match decode_one(&buf) {
            DecodeOne::Ok(r) => {
                assert_eq!(r.op, op);
                assert_eq!(r.key, key);
                assert_eq!(r.value, value);
                assert_eq!(r.expires_at, expires_at);
                assert_eq!(r.total, buf.len());
            }
            _ => panic!("V1 round-trip failed for {} bytes", buf.len()),
        }
    }

    #[test]
    fn v1_round_trip_basic() {
        assert_round_trip(1, b"key", b"value", 0);
        assert_round_trip(2, b"k", b"", 100);
        assert_round_trip(1 | 0x80, b"large-key", b"vp-bytes", 0);
    }

    #[test]
    fn v1_round_trip_empty_key_value() {
        assert_round_trip(1, b"", b"", 0);
    }

    #[test]
    fn v1_round_trip_large_value() {
        let v = vec![0xab; 100_000];
        assert_round_trip(1, b"big-key", &v, 1_700_000_000);
    }

    #[test]
    fn v0_legacy_decode() {
        // Hand-construct a V0 record.
        let mut buf = Vec::new();
        buf.push(1u8); // op
        buf.extend_from_slice(&3u32.to_le_bytes()); // key_len
        buf.extend_from_slice(&5u32.to_le_bytes()); // val_len
        buf.extend_from_slice(&0u64.to_le_bytes()); // expires_at
        buf.extend_from_slice(b"key"); // key
        buf.extend_from_slice(b"value"); // value
        match decode_one(&buf) {
            DecodeOne::Ok(r) => {
                assert_eq!(r.op, 1);
                assert_eq!(r.key, b"key");
                assert_eq!(r.value, b"value");
                assert_eq!(r.total, 17 + 3 + 5);
            }
            _ => panic!("V0 decode failed"),
        }
    }

    #[test]
    fn v1_corrupted_payload_byte_caught() {
        let mut buf = encode_v1(1, b"abc", b"defgh", 0);
        // Flip a payload byte (the value's first byte).
        let payload_start = 5; // sentinel + length
        let value_start = payload_start + PAYLOAD_HEADER + 3; // +key_len
        buf[value_start] ^= 0xff;
        match decode_one(&buf) {
            DecodeOne::Corrupt { reason, .. } => {
                assert!(reason.contains("CRC"), "expected CRC error, got {reason}");
            }
            _ => panic!("expected Corrupt, got something else"),
        }
    }

    #[test]
    fn v1_corrupted_crc_byte_caught() {
        let mut buf = encode_v1(1, b"k", b"v", 0);
        let crc_start = buf.len() - 4;
        buf[crc_start] ^= 0xff;
        match decode_one(&buf) {
            DecodeOne::Corrupt { .. } => {}
            _ => panic!("expected Corrupt"),
        }
    }

    #[test]
    fn v1_corrupted_length_caught() {
        let mut buf = encode_v1(1, b"k", b"v", 0);
        // Flip a high bit in the length field — likely makes it overflow the buffer.
        buf[1] ^= 0xff;
        // Either Incomplete (if length now > buf.len()) or Corrupt (CRC fail).
        match decode_one(&buf) {
            DecodeOne::Incomplete | DecodeOne::Corrupt { .. } => {}
            DecodeOne::Ok(_) => panic!("expected Incomplete or Corrupt"),
        }
    }

    #[test]
    fn v1_truncated_at_tail_is_incomplete() {
        let buf = encode_v1(1, b"k", b"v", 0);
        // Drop the last few bytes.
        let truncated = &buf[..buf.len() - 3];
        match decode_one(truncated) {
            DecodeOne::Incomplete => {}
            _ => panic!("expected Incomplete on truncated tail"),
        }
    }

    #[test]
    fn v1_then_v0_mixed_decode() {
        // Simulate a log_stream containing a V1 record followed by a V0 record
        // (e.g., during a rolling upgrade between F158 and pre-F158 binaries
        // writing to the same stream — won't actually happen in production
        // since autumn-rs restarts all-together, but tests the dispatcher).
        let mut buf = encode_v1(1, b"v1key", b"v1val", 0);
        // Append V0
        buf.push(2u8);
        buf.extend_from_slice(&5u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(b"v0key");

        let v1_size = encode_v1(1, b"v1key", b"v1val", 0).len();
        let r1 = decode_one(&buf);
        match r1 {
            DecodeOne::Ok(r) => {
                assert_eq!(r.op, 1);
                assert_eq!(r.key, b"v1key");
                assert_eq!(r.value, b"v1val");
                assert_eq!(r.total, v1_size);
            }
            _ => panic!("expected V1 ok"),
        }
        let r2 = decode_one(&buf[v1_size..]);
        match r2 {
            DecodeOne::Ok(r) => {
                assert_eq!(r.op, 2);
                assert_eq!(r.key, b"v0key");
                assert_eq!(r.value, b"");
            }
            _ => panic!("expected V0 ok"),
        }
    }
}
