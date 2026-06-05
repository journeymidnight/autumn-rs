//! Submission Queue Entry (SQE) — what a client writes into the ring.
//!
//! Layout is fixed at 40 bytes: 8 byte cache-line aligned, padded so
//! consecutive SQEs each start on an 8-byte boundary.

use bytes::{Buf, BufMut};

use crate::opcode::Opcode;

/// Wire size of one SQE in bytes. Stable: changing this breaks the
/// shared-memory ABI between daemon and client. Bumps `RING_VERSION`.
pub const SQE_SIZE: usize = 40;

/// F-ioring-lease-2: per-Opcode lease-mode discriminant carried in the
/// `Sqe.flags` byte. Mirrors `autumn_rpc::manager_rpc::LEASE_MODE_*`.
/// For `Opcode::Open` the daemon uses this to decide whether to
/// AcquireLease in READ or WRITE mode; for every other opcode the
/// byte stays reserved (must be 0).
pub const SQE_LEASE_MODE_UNSET: u8 = 0;
pub const SQE_LEASE_MODE_READ: u8 = 1;
pub const SQE_LEASE_MODE_WRITE: u8 = 2;

/// Submission queue entry. Pure data — no atomics, no SHM concerns.
/// Atomic ring-index management lives in the producer/consumer code
/// paths in F180-B / F180-C.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sqe {
    pub opcode: Opcode,
    /// Per-session ring file descriptor (NOT a kernel fd). Assigned by
    /// the daemon on `Open` and released on `Close`. Ignored by `Open`
    /// itself.
    pub ring_fd: u32,
    /// Byte offset within the file (read/write only; ignored by other
    /// opcodes).
    pub offset: u64,
    /// Number of bytes the daemon should read or write. For `Open`, the
    /// length of the path payload at `buf_offset`. Capped at the buffer
    /// pool slot size — see `RingHeader::buf_slot_size`.
    pub length: u32,
    /// Byte offset within the client-allocated buffer pool. Reads write
    /// data into this slot; writes/opens read data from it. The slot
    /// must be entirely within `[0, buf_pool_size)`.
    pub buf_offset: u64,
    /// Opaque application token returned verbatim in the matching CQE
    /// so the client can correlate completions to its in-flight table.
    pub user_data: u64,
    /// F-ioring-lease-2: lease mode requested at `Opcode::Open` time.
    /// `SQE_LEASE_MODE_UNSET` (= 0, sent by v1 clients) is interpreted
    /// by the daemon as `SQE_LEASE_MODE_WRITE` — the safe default that
    /// never silently downgrades a writer to a read-only session.
    /// For every non-Open opcode this MUST be `SQE_LEASE_MODE_UNSET`
    /// (decode rejects otherwise — see `SqeDecodeError::ReservedBitsSet`).
    pub lease_mode: u8,
}

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum SqeDecodeError {
    #[error("SQE buffer too short: need {SQE_SIZE} bytes, got {0}")]
    TooShort(usize),
    #[error("SQE has reserved bits set in flags byte: {0:#x}")]
    ReservedBitsSet(u8),
    #[error("SQE has unknown opcode: {0}")]
    UnknownOpcode(u8),
}

impl Sqe {
    /// Encode an SQE into exactly `SQE_SIZE` bytes. Layout (LE):
    ///
    /// ```text
    /// 0..1  : opcode (u8)
    /// 1..2  : flags  (u8) — reserved, must be 0 in F180-A
    /// 2..4  : pad    (2 × u8) — must be 0
    /// 4..8  : ring_fd (u32 LE)
    /// 8..12 : length (u32 LE)
    /// 12..16: pad    (u32) — must be 0
    /// 16..24: offset (u64 LE)
    /// 24..32: buf_offset (u64 LE)
    /// 32..40: user_data (u64 LE)
    /// ```
    pub fn encode(&self, dst: &mut [u8; SQE_SIZE]) {
        let mut buf = &mut dst[..];
        buf.put_u8(self.opcode.as_u8());
        // F-ioring-lease-2: flags byte = lease_mode for Open, 0 otherwise.
        // Encoder MUST zero out `lease_mode` for non-Open so a caller
        // that accidentally constructs a non-Open SQE with a non-zero
        // `lease_mode` doesn't produce a frame the decoder will reject
        // with `ReservedBitsSet` (the symmetric invariant on decode).
        let flags = match self.opcode {
            crate::opcode::Opcode::Open => self.lease_mode,
            _ => 0,
        };
        buf.put_u8(flags);
        buf.put_u8(0);
        buf.put_u8(0);
        buf.put_u32_le(self.ring_fd);
        buf.put_u32_le(self.length);
        buf.put_u32_le(0); // pad
        buf.put_u64_le(self.offset);
        buf.put_u64_le(self.buf_offset);
        buf.put_u64_le(self.user_data);
    }

    /// Decode an SQE from a slice. Slice must be at least `SQE_SIZE`
    /// bytes; trailing bytes are ignored.
    pub fn decode(src: &[u8]) -> Result<Self, SqeDecodeError> {
        if src.len() < SQE_SIZE {
            return Err(SqeDecodeError::TooShort(src.len()));
        }
        let mut buf = &src[..SQE_SIZE];
        let opcode_u8 = buf.get_u8();
        let flags = buf.get_u8();
        let _pad0 = buf.get_u8();
        let _pad1 = buf.get_u8();
        let ring_fd = buf.get_u32_le();
        let length = buf.get_u32_le();
        let _pad2 = buf.get_u32_le();
        let offset = buf.get_u64_le();
        let buf_offset = buf.get_u64_le();
        let user_data = buf.get_u64_le();
        let opcode = Opcode::from_u8(opcode_u8).ok_or(SqeDecodeError::UnknownOpcode(opcode_u8))?;
        // F-ioring-lease-2: for Open the flags byte carries the lease
        // mode (0/1/2 valid; 0 means "unset → daemon defaults to
        // WRITE"). For every other opcode it stays reserved.
        let lease_mode = match opcode {
            Opcode::Open => match flags {
                SQE_LEASE_MODE_UNSET | SQE_LEASE_MODE_READ | SQE_LEASE_MODE_WRITE => flags,
                _ => return Err(SqeDecodeError::ReservedBitsSet(flags)),
            },
            _ => {
                if flags != 0 {
                    return Err(SqeDecodeError::ReservedBitsSet(flags));
                }
                0
            }
        };
        Ok(Self {
            opcode,
            ring_fd,
            offset,
            length,
            buf_offset,
            user_data,
            lease_mode,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(opcode: Opcode) -> Sqe {
        Sqe {
            opcode,
            ring_fd: 0xdeadbeef,
            offset: 0x1234_5678_9abc_def0,
            length: 0x4000,
            buf_offset: 0x10_0000,
            user_data: 0xcafe_babe_dead_beef,
            lease_mode: SQE_LEASE_MODE_UNSET,
        }
    }

    #[test]
    fn round_trip_all_opcodes() {
        for op in [
            Opcode::Nop,
            Opcode::Open,
            Opcode::Read,
            Opcode::Write,
            Opcode::Close,
        ] {
            let s = sample(op);
            let mut buf = [0u8; SQE_SIZE];
            s.encode(&mut buf);
            let decoded = Sqe::decode(&buf).expect("decode");
            assert_eq!(s, decoded);
        }
    }

    #[test]
    fn decode_too_short() {
        let buf = [0u8; SQE_SIZE - 1];
        let err = Sqe::decode(&buf).unwrap_err();
        assert!(matches!(err, SqeDecodeError::TooShort(_)));
    }

    #[test]
    fn open_round_trips_read_and_write_lease_modes() {
        // F-ioring-lease-2: Open carries the lease mode in the flags byte.
        for mode in [SQE_LEASE_MODE_READ, SQE_LEASE_MODE_WRITE] {
            let mut s = sample(Opcode::Open);
            s.lease_mode = mode;
            let mut buf = [0u8; SQE_SIZE];
            s.encode(&mut buf);
            assert_eq!(buf[1], mode, "flags byte must carry lease mode");
            let decoded = Sqe::decode(&buf).expect("decode");
            assert_eq!(decoded.lease_mode, mode);
            assert_eq!(decoded, s);
        }
    }

    #[test]
    fn open_invalid_lease_mode_rejected() {
        // Flags byte for Open is constrained to {0,1,2}; any other
        // value is a wire-protocol bug.
        let mut buf = [0u8; SQE_SIZE];
        buf[0] = Opcode::Open.as_u8();
        buf[1] = 0x99;
        let err = Sqe::decode(&buf).unwrap_err();
        assert!(matches!(err, SqeDecodeError::ReservedBitsSet(0x99)));
    }

    #[test]
    fn encode_zeroes_lease_mode_for_non_open_opcodes() {
        // Regression for coco P2 #8: pre-fix `encode()` unconditionally
        // wrote `self.lease_mode` into the flags byte, so a caller
        // who accidentally set `lease_mode` on a Read/Write SQE
        // produced a frame the decoder rejects with `ReservedBitsSet`.
        // The encoder is now opcode-aware: non-Open ⇒ flags=0.
        for op in [Opcode::Nop, Opcode::Read, Opcode::Write, Opcode::Close] {
            let mut s = sample(op);
            s.lease_mode = SQE_LEASE_MODE_WRITE;
            let mut buf = [0u8; SQE_SIZE];
            s.encode(&mut buf);
            assert_eq!(buf[1], 0, "{op:?} encode must zero the flags byte");
            // Round-trips: decoded `lease_mode` is the encoder's
            // zero, NOT the caller's mis-set value.
            let decoded = Sqe::decode(&buf).expect("decode");
            assert_eq!(decoded.lease_mode, SQE_LEASE_MODE_UNSET);
        }
    }

    #[test]
    fn non_open_must_keep_flags_zero() {
        // For any non-Open opcode the flags byte must stay 0 — even
        // an otherwise-valid lease-mode discriminant is an error
        // because the daemon has no semantics for it on Read/Write/
        // Close/Nop.
        for op in [Opcode::Nop, Opcode::Read, Opcode::Write, Opcode::Close] {
            let mut buf = [0u8; SQE_SIZE];
            buf[0] = op.as_u8();
            buf[1] = SQE_LEASE_MODE_READ; // valid for Open, invalid here
            let err = Sqe::decode(&buf).unwrap_err();
            assert!(
                matches!(err, SqeDecodeError::ReservedBitsSet(SQE_LEASE_MODE_READ)),
                "opcode {op:?} should reject lease-mode flag",
            );
        }
    }

    #[test]
    fn decode_unknown_opcode_rejected() {
        let mut buf = [0u8; SQE_SIZE];
        buf[0] = 99; // not in the enum
        let err = Sqe::decode(&buf).unwrap_err();
        assert!(matches!(err, SqeDecodeError::UnknownOpcode(99)));
    }

    #[test]
    fn extra_trailing_bytes_ignored() {
        let s = sample(Opcode::Read);
        let mut buf = [0u8; SQE_SIZE + 16];
        s.encode((&mut buf[..SQE_SIZE]).try_into().unwrap());
        let decoded = Sqe::decode(&buf).expect("decode with trailing bytes");
        assert_eq!(s, decoded);
    }
}
