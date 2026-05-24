//! Ring header — fixed-layout metadata at the start of the SHM file.
//!
//! All fields are little-endian regardless of host endianness; the ring
//! is intended for same-host shared memory but pinning the wire format
//! makes future cross-arch (x86 ↔ ARM) coexistence trivial.
//!
//! ```text
//! Offset  Size  Field            Notes
//! 0       4     magic            "AUIR" (0x52_49_55_41 LE)
//! 4       2     version          RING_VERSION (current: 1)
//! 6       2     header_size      = HEADER_SIZE (= 64 in v1)
//! 8       4     sq_entries       SQ ring capacity (power of 2)
//! 12      4     cq_entries       CQ ring capacity (power of 2)
//! 16      8     buf_pool_offset  byte offset of buffer pool region
//! 24      8     buf_pool_size    total buffer pool bytes
//! 32      4     buf_slot_size    per-slot bytes (capped read length)
//! 36      4     capability_flags client/daemon negotiated features
//! 40      8     session_id       random per-session token (handshake)
//! 48     16     reserved         must be zero
//! ```
//!
//! Atomic SQ/CQ head/tail indices live AFTER the header — see the
//! `sq_head_offset()` / `sq_tail_offset()` / `cq_head_offset()` /
//! `cq_tail_offset()` accessors. Each is a u64 occupying its own
//! 64-byte cache line to avoid false sharing between producer (client)
//! and consumer (daemon).

use bytes::{Buf, BufMut};

/// Magic bytes at the start of the ring header — `"AUIR"` little-endian.
pub const RING_MAGIC: u32 = u32::from_le_bytes(*b"AUIR");

/// Wire format version. Bump on any layout change.
pub const RING_VERSION: u16 = 1;

/// Bytes occupied by the on-disk header struct itself.
pub const HEADER_SIZE: u16 = 64;

/// Cache-line size (assumed on x86-64 / ARM64). SQ/CQ atomic indices
/// each occupy a full line to avoid false sharing.
pub const CACHE_LINE: usize = 64;

/// Default SQ ring capacity. Power of 2 so head/tail wraparound is
/// `index & (sq_entries - 1)`.
pub const DEFAULT_SQ_ENTRIES: u32 = 1024;

/// Default CQ ring capacity. Same as SQ in the common case.
pub const DEFAULT_CQ_ENTRIES: u32 = 1024;

/// Default total buffer pool size (per ring): 64 MiB. Tuneable.
pub const DEFAULT_BUF_POOL_SIZE: u64 = 64 * 1024 * 1024;

/// Default per-slot size: 1 MiB. Matches the typical FUSE max_read
/// request size and lets a single SQE carry up to 1 MiB of data.
pub const DEFAULT_BUF_SLOT_SIZE: u32 = 1024 * 1024;

/// Capability flag: client and daemon support `Opcode::Read`. Always
/// set in v1.
pub const CAP_READ: u32 = 1 << 0;
/// Capability flag: client and daemon support `Opcode::Write`. Always
/// set in v1.
pub const CAP_WRITE: u32 = 1 << 1;
/// Capability flag: client and daemon support `Opcode::Open` / `Close`.
/// Always set in v1.
pub const CAP_OPEN_CLOSE: u32 = 1 << 2;
/// Capability flag: futex-based wait/wake on empty queue (vs pure
/// busy-poll). Optional; reserved for F180-B.
pub const CAP_FUTEX_WAIT: u32 = 1 << 3;

/// Default capabilities advertised by a v1 daemon.
pub const DEFAULT_CAPABILITIES: u32 = CAP_READ | CAP_WRITE | CAP_OPEN_CLOSE;

/// Pure-data ring header. Atomic indices are NOT modeled here — they
/// are independent u64 cells laid out after this header in the SHM
/// region; F180-B/C will wrap them in `AtomicU64` views.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RingHeader {
    pub magic: u32,
    pub version: u16,
    pub header_size: u16,
    pub sq_entries: u32,
    pub cq_entries: u32,
    pub buf_pool_offset: u64,
    pub buf_pool_size: u64,
    pub buf_slot_size: u32,
    pub capability_flags: u32,
    pub session_id: u64,
}

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum RingHeaderDecodeError {
    #[error("header buffer too short: need {expected} bytes, got {got}", expected = HEADER_SIZE)]
    TooShort { got: usize },
    #[error("bad magic: got {got:#x}, expected {expected:#x}")]
    BadMagic { got: u32, expected: u32 },
    #[error("unsupported version: got {got}, expected {expected}")]
    UnsupportedVersion { got: u16, expected: u16 },
    #[error("header_size mismatch: got {got}, expected {expected}")]
    BadHeaderSize { got: u16, expected: u16 },
    #[error("sq_entries must be a power of 2 ≥ 2, got {0}")]
    BadSqEntries(u32),
    #[error("cq_entries must be a power of 2 ≥ 2, got {0}")]
    BadCqEntries(u32),
    #[error("buf_slot_size must be > 0 and ≤ buf_pool_size, got slot={slot}, pool={pool}")]
    BadBufSlotSize { slot: u32, pool: u64 },
    #[error("required capability bits not set in flags: got {0:#x}")]
    MissingCapabilities(u32),
}

impl RingHeader {
    /// Build a default header for a new ring. `buf_pool_offset` is
    /// computed to place the pool immediately after the CQ array
    /// (which itself comes after the four atomic cells + SQ array).
    pub fn new(session_id: u64) -> Self {
        let mut h = Self {
            magic: RING_MAGIC,
            version: RING_VERSION,
            header_size: HEADER_SIZE,
            sq_entries: DEFAULT_SQ_ENTRIES,
            cq_entries: DEFAULT_CQ_ENTRIES,
            // Placeholder; recomputed below using the layout helpers.
            buf_pool_offset: 0,
            buf_pool_size: DEFAULT_BUF_POOL_SIZE,
            buf_slot_size: DEFAULT_BUF_SLOT_SIZE,
            capability_flags: DEFAULT_CAPABILITIES,
            session_id,
        };
        h.buf_pool_offset =
            h.cq_array_offset() + (h.cq_entries as u64) * (crate::cqe::CQE_SIZE as u64);
        h
    }

    /// Encode into exactly `HEADER_SIZE` bytes.
    pub fn encode(&self, dst: &mut [u8; HEADER_SIZE as usize]) {
        let mut buf = &mut dst[..];
        buf.put_u32_le(self.magic);
        buf.put_u16_le(self.version);
        buf.put_u16_le(self.header_size);
        buf.put_u32_le(self.sq_entries);
        buf.put_u32_le(self.cq_entries);
        buf.put_u64_le(self.buf_pool_offset);
        buf.put_u64_le(self.buf_pool_size);
        buf.put_u32_le(self.buf_slot_size);
        buf.put_u32_le(self.capability_flags);
        buf.put_u64_le(self.session_id);
        // Reserved 16 bytes — write zeros.
        for _ in 0..16 {
            buf.put_u8(0);
        }
    }

    /// Decode and validate. Required invariants enforced here:
    /// - magic matches `RING_MAGIC`,
    /// - version matches `RING_VERSION` (later revisions can negotiate
    ///   downgrade in handshake; pure decode is strict),
    /// - header_size matches our compile-time constant,
    /// - sq/cq entries are powers of 2 ≥ 2,
    /// - buf_slot_size ∈ (0, buf_pool_size].
    pub fn decode(src: &[u8]) -> Result<Self, RingHeaderDecodeError> {
        if src.len() < HEADER_SIZE as usize {
            return Err(RingHeaderDecodeError::TooShort { got: src.len() });
        }
        let mut buf = &src[..HEADER_SIZE as usize];
        let magic = buf.get_u32_le();
        if magic != RING_MAGIC {
            return Err(RingHeaderDecodeError::BadMagic {
                got: magic,
                expected: RING_MAGIC,
            });
        }
        let version = buf.get_u16_le();
        if version != RING_VERSION {
            return Err(RingHeaderDecodeError::UnsupportedVersion {
                got: version,
                expected: RING_VERSION,
            });
        }
        let header_size = buf.get_u16_le();
        if header_size != HEADER_SIZE {
            return Err(RingHeaderDecodeError::BadHeaderSize {
                got: header_size,
                expected: HEADER_SIZE,
            });
        }
        let sq_entries = buf.get_u32_le();
        if sq_entries < 2 || !sq_entries.is_power_of_two() {
            return Err(RingHeaderDecodeError::BadSqEntries(sq_entries));
        }
        let cq_entries = buf.get_u32_le();
        if cq_entries < 2 || !cq_entries.is_power_of_two() {
            return Err(RingHeaderDecodeError::BadCqEntries(cq_entries));
        }
        let buf_pool_offset = buf.get_u64_le();
        let buf_pool_size = buf.get_u64_le();
        let buf_slot_size = buf.get_u32_le();
        if buf_slot_size == 0 || (buf_slot_size as u64) > buf_pool_size {
            return Err(RingHeaderDecodeError::BadBufSlotSize {
                slot: buf_slot_size,
                pool: buf_pool_size,
            });
        }
        let capability_flags = buf.get_u32_le();
        if capability_flags & DEFAULT_CAPABILITIES != DEFAULT_CAPABILITIES {
            return Err(RingHeaderDecodeError::MissingCapabilities(capability_flags));
        }
        let session_id = buf.get_u64_le();
        // Reserved bytes — silently consume; future revisions may
        // re-define them but v1 doesn't validate.
        Ok(Self {
            magic,
            version,
            header_size,
            sq_entries,
            cq_entries,
            buf_pool_offset,
            buf_pool_size,
            buf_slot_size,
            capability_flags,
            session_id,
        })
    }

    /// Offset of the SQ tail atomic (producer-write, consumer-read).
    /// On its own cache line.
    pub fn sq_tail_offset(&self) -> u64 {
        HEADER_SIZE as u64
    }
    /// Offset of the SQ head atomic (consumer-write, producer-read).
    pub fn sq_head_offset(&self) -> u64 {
        self.sq_tail_offset() + CACHE_LINE as u64
    }
    /// Offset of the CQ tail atomic (consumer-write, producer-read).
    pub fn cq_tail_offset(&self) -> u64 {
        self.sq_head_offset() + CACHE_LINE as u64
    }
    /// Offset of the CQ head atomic (producer-write, consumer-read).
    pub fn cq_head_offset(&self) -> u64 {
        self.cq_tail_offset() + CACHE_LINE as u64
    }
    /// Offset of the SQ entries array (after the four atomic cells).
    pub fn sq_array_offset(&self) -> u64 {
        self.cq_head_offset() + CACHE_LINE as u64
    }
    /// Offset of the CQ entries array (after the SQ entries).
    pub fn cq_array_offset(&self) -> u64 {
        self.sq_array_offset() + (self.sq_entries as u64) * (crate::sqe::SQE_SIZE as u64)
    }

    /// Total bytes a ring with this header occupies (header + atomics
    /// + SQ array + CQ array + buf pool).
    pub fn total_size(&self) -> u64 {
        self.cq_array_offset()
            + (self.cq_entries as u64) * (crate::cqe::CQE_SIZE as u64)
            + self.buf_pool_size
    }

    /// Sanity-check that `buf_pool_offset` we encoded matches the sum
    /// of the structural offsets — used by the daemon to verify a
    /// client-allocated SHM file is consistent.
    pub fn buf_pool_offset_consistent(&self) -> bool {
        self.buf_pool_offset
            == self.cq_array_offset() + (self.cq_entries as u64) * (crate::cqe::CQE_SIZE as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_default_header() {
        let h = RingHeader::new(0xdead_beef_cafe_babe);
        let mut buf = [0u8; HEADER_SIZE as usize];
        h.encode(&mut buf);
        let decoded = RingHeader::decode(&buf).expect("decode");
        // buf_pool_offset is computed by `new` based on the post-header
        // layout; round-trip must preserve it bit-for-bit.
        assert_eq!(h, decoded);
    }

    #[test]
    fn decode_too_short() {
        let buf = [0u8; HEADER_SIZE as usize - 1];
        let err = RingHeader::decode(&buf).unwrap_err();
        assert!(matches!(err, RingHeaderDecodeError::TooShort { .. }));
    }

    #[test]
    fn decode_bad_magic() {
        let mut buf = [0u8; HEADER_SIZE as usize];
        buf[0..4].copy_from_slice(&[0xff, 0xff, 0xff, 0xff]);
        let err = RingHeader::decode(&buf).unwrap_err();
        assert!(matches!(err, RingHeaderDecodeError::BadMagic { .. }));
    }

    #[test]
    fn decode_bad_version() {
        let mut h = RingHeader::new(1);
        let mut buf = [0u8; HEADER_SIZE as usize];
        h.version = 99;
        h.encode(&mut buf);
        let err = RingHeader::decode(&buf).unwrap_err();
        assert!(matches!(
            err,
            RingHeaderDecodeError::UnsupportedVersion { .. }
        ));
    }

    #[test]
    fn decode_non_power_of_two_sq_rejected() {
        let mut h = RingHeader::new(1);
        h.sq_entries = 1000; // not a power of 2
        let mut buf = [0u8; HEADER_SIZE as usize];
        h.encode(&mut buf);
        let err = RingHeader::decode(&buf).unwrap_err();
        assert!(matches!(err, RingHeaderDecodeError::BadSqEntries(1000)));
    }

    #[test]
    fn decode_buf_slot_zero_rejected() {
        let mut h = RingHeader::new(1);
        h.buf_slot_size = 0;
        let mut buf = [0u8; HEADER_SIZE as usize];
        h.encode(&mut buf);
        let err = RingHeader::decode(&buf).unwrap_err();
        assert!(matches!(err, RingHeaderDecodeError::BadBufSlotSize { .. }));
    }

    #[test]
    fn decode_buf_slot_larger_than_pool_rejected() {
        let mut h = RingHeader::new(1);
        h.buf_pool_size = 1024;
        h.buf_slot_size = 4096;
        let mut buf = [0u8; HEADER_SIZE as usize];
        h.encode(&mut buf);
        let err = RingHeader::decode(&buf).unwrap_err();
        assert!(matches!(err, RingHeaderDecodeError::BadBufSlotSize { .. }));
    }

    #[test]
    fn decode_missing_capabilities_rejected() {
        let mut h = RingHeader::new(1);
        h.capability_flags = 0; // no caps at all
        let mut buf = [0u8; HEADER_SIZE as usize];
        h.encode(&mut buf);
        let err = RingHeader::decode(&buf).unwrap_err();
        assert!(matches!(err, RingHeaderDecodeError::MissingCapabilities(0)));
    }

    #[test]
    fn layout_offsets_are_cache_aligned() {
        let h = RingHeader::new(1);
        // Each atomic cell sits on its own cache line.
        assert_eq!(h.sq_tail_offset() % CACHE_LINE as u64, 0);
        assert_eq!(h.sq_head_offset() % CACHE_LINE as u64, 0);
        assert_eq!(h.cq_tail_offset() % CACHE_LINE as u64, 0);
        assert_eq!(h.cq_head_offset() % CACHE_LINE as u64, 0);
        // Pairs are on different lines.
        assert_ne!(h.sq_tail_offset(), h.sq_head_offset());
    }

    #[test]
    fn buf_pool_offset_consistent_after_new() {
        let h = RingHeader::new(1);
        assert!(h.buf_pool_offset_consistent());
    }

    #[test]
    fn total_size_includes_all_regions() {
        let h = RingHeader::new(1);
        let expected = h.cq_array_offset()
            + (h.cq_entries as u64) * (crate::cqe::CQE_SIZE as u64)
            + h.buf_pool_size;
        assert_eq!(h.total_size(), expected);
        // Sanity: total > buf_pool_offset + buf_pool_size.
        assert!(h.total_size() >= h.buf_pool_offset + h.buf_pool_size);
    }
}
