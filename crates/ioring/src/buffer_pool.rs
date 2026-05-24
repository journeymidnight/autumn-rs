//! Buffer pool layout helpers — pure arithmetic over the ring header's
//! `buf_pool_offset` / `buf_pool_size` / `buf_slot_size` fields.
//!
//! The buffer pool is the SHM region where read-data and write-data
//! payloads live. SQEs reference a slot via `buf_offset`; the daemon
//! reads/writes the data at that absolute offset within the SHM file.
//!
//! This module does NOT touch SHM — it only validates and computes
//! offsets / slot indices. Allocation strategy (free-list vs
//! ring-cursor) is deferred to F180-C alongside the client-side
//! producer.

use crate::header::RingHeader;

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum BufferPoolError {
    #[error("buf_offset {offset} out of pool range [{lo}, {hi})")]
    OutOfRange { offset: u64, lo: u64, hi: u64 },
    #[error("buf_offset {offset} not slot-aligned (slot={slot})")]
    Misaligned { offset: u64, slot: u32 },
    #[error("read length {length} exceeds slot size {slot}")]
    LengthExceedsSlot { length: u32, slot: u32 },
    #[error("buf_offset+length {end} extends past pool end {pool_end}")]
    SliceCrossesPoolEnd { end: u64, pool_end: u64 },
}

/// Geometric view onto the buffer pool. Cheap to recompute from a
/// header so callers can validate SQEs without holding extra state.
#[derive(Debug, Clone, Copy)]
pub struct BufferPoolLayout {
    pub pool_offset: u64,
    pub pool_size: u64,
    pub slot_size: u32,
}

impl BufferPoolLayout {
    pub fn from_header(h: &RingHeader) -> Self {
        Self {
            pool_offset: h.buf_pool_offset,
            pool_size: h.buf_pool_size,
            slot_size: h.buf_slot_size,
        }
    }

    /// Number of slots the pool can hold.
    pub fn num_slots(&self) -> u64 {
        if self.slot_size == 0 {
            0
        } else {
            self.pool_size / self.slot_size as u64
        }
    }

    /// First byte after the pool (exclusive upper bound).
    pub fn pool_end(&self) -> u64 {
        self.pool_offset + self.pool_size
    }

    /// Validate a `(buf_offset, length)` pair from an SQE.
    /// Returns Ok if:
    /// - offset ∈ `[pool_offset, pool_end)`,
    /// - offset is `slot_size`-aligned relative to `pool_offset`,
    /// - length ≤ slot_size,
    /// - offset + length ≤ pool_end.
    pub fn validate_slice(&self, buf_offset: u64, length: u32) -> Result<(), BufferPoolError> {
        if buf_offset < self.pool_offset || buf_offset >= self.pool_end() {
            return Err(BufferPoolError::OutOfRange {
                offset: buf_offset,
                lo: self.pool_offset,
                hi: self.pool_end(),
            });
        }
        let rel = buf_offset - self.pool_offset;
        if rel % (self.slot_size as u64) != 0 {
            return Err(BufferPoolError::Misaligned {
                offset: buf_offset,
                slot: self.slot_size,
            });
        }
        if length > self.slot_size {
            return Err(BufferPoolError::LengthExceedsSlot {
                length,
                slot: self.slot_size,
            });
        }
        let end = buf_offset + length as u64;
        if end > self.pool_end() {
            return Err(BufferPoolError::SliceCrossesPoolEnd {
                end,
                pool_end: self.pool_end(),
            });
        }
        Ok(())
    }

    /// Convert a slot index to the absolute SHM byte offset for that
    /// slot's start.
    pub fn slot_offset(&self, slot_idx: u64) -> Option<u64> {
        if slot_idx >= self.num_slots() {
            return None;
        }
        Some(self.pool_offset + slot_idx * self.slot_size as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::RingHeader;

    fn layout() -> BufferPoolLayout {
        BufferPoolLayout::from_header(&RingHeader::new(0))
    }

    #[test]
    fn num_slots_default() {
        let l = layout();
        // 64 MiB / 1 MiB = 64 slots.
        assert_eq!(l.num_slots(), 64);
    }

    #[test]
    fn slot_offset_within_pool() {
        let l = layout();
        let off = l.slot_offset(0).unwrap();
        assert_eq!(off, l.pool_offset);
        let off_last = l.slot_offset(l.num_slots() - 1).unwrap();
        assert_eq!(
            off_last,
            l.pool_offset + (l.num_slots() - 1) * l.slot_size as u64
        );
    }

    #[test]
    fn slot_offset_out_of_range() {
        let l = layout();
        assert!(l.slot_offset(l.num_slots()).is_none());
    }

    #[test]
    fn validate_below_pool_rejected() {
        let l = layout();
        let err = l.validate_slice(l.pool_offset - 1, 100).unwrap_err();
        assert!(matches!(err, BufferPoolError::OutOfRange { .. }));
    }

    #[test]
    fn validate_above_pool_rejected() {
        let l = layout();
        let err = l.validate_slice(l.pool_end(), 1).unwrap_err();
        assert!(matches!(err, BufferPoolError::OutOfRange { .. }));
    }

    #[test]
    fn validate_misaligned_rejected() {
        let l = layout();
        let err = l.validate_slice(l.pool_offset + 1, 100).unwrap_err();
        assert!(matches!(err, BufferPoolError::Misaligned { .. }));
    }

    #[test]
    fn validate_length_exceeds_slot_rejected() {
        let l = layout();
        let err = l
            .validate_slice(l.pool_offset, l.slot_size + 1)
            .unwrap_err();
        assert!(matches!(err, BufferPoolError::LengthExceedsSlot { .. }));
    }

    #[test]
    fn validate_full_slot_ok() {
        let l = layout();
        l.validate_slice(l.pool_offset, l.slot_size).unwrap();
    }

    #[test]
    fn validate_zero_length_ok() {
        let l = layout();
        l.validate_slice(l.pool_offset, 0).unwrap();
    }
}
