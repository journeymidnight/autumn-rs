//! Completion Queue Entry — what the daemon writes back into the ring.
//!
//! 16 bytes, 8-byte aligned. Tighter than SQE because completions
//! carry less per-entry context (the ring_fd / offset / length are all
//! recoverable from the matching SQE via `user_data`).

use bytes::{Buf, BufMut};

/// Wire size of one CQE in bytes.
pub const CQE_SIZE: usize = 16;

/// Completion queue entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cqe {
    /// Echoed from the originating SQE so the client can correlate to
    /// its in-flight table.
    pub user_data: u64,
    /// Operation result. Non-negative = success (e.g. bytes
    /// read/written; ring_fd for `Open`). Negative = errno.
    pub result: i64,
}

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum CqeDecodeError {
    #[error("CQE buffer too short: need {CQE_SIZE} bytes, got {0}")]
    TooShort(usize),
}

impl Cqe {
    /// Encode a CQE into exactly `CQE_SIZE` bytes:
    ///
    /// ```text
    /// 0..8 : user_data (u64 LE)
    /// 8..16: result    (i64 LE)
    /// ```
    pub fn encode(&self, dst: &mut [u8; CQE_SIZE]) {
        let mut buf = &mut dst[..];
        buf.put_u64_le(self.user_data);
        buf.put_i64_le(self.result);
    }

    pub fn decode(src: &[u8]) -> Result<Self, CqeDecodeError> {
        if src.len() < CQE_SIZE {
            return Err(CqeDecodeError::TooShort(src.len()));
        }
        let mut buf = &src[..CQE_SIZE];
        let user_data = buf.get_u64_le();
        let result = buf.get_i64_le();
        Ok(Self { user_data, result })
    }

    /// Convenience: build a success CQE from a non-negative byte count.
    pub fn ok(user_data: u64, bytes: u64) -> Self {
        Self {
            user_data,
            result: bytes as i64,
        }
    }

    /// Convenience: build an error CQE from a positive errno (the
    /// stored value is `-errno`, matching POSIX convention used by
    /// io_uring).
    pub fn err(user_data: u64, errno: i32) -> Self {
        Self {
            user_data,
            result: -(errno as i64),
        }
    }

    /// True if this completion is an error (negative result).
    pub fn is_err(&self) -> bool {
        self.result < 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_success() {
        let c = Cqe {
            user_data: 0x1234_5678_9abc_def0,
            result: 0x4000,
        };
        let mut buf = [0u8; CQE_SIZE];
        c.encode(&mut buf);
        let decoded = Cqe::decode(&buf).unwrap();
        assert_eq!(c, decoded);
    }

    #[test]
    fn round_trip_error() {
        let c = Cqe::err(0xfeed_face, libc_einval());
        let mut buf = [0u8; CQE_SIZE];
        c.encode(&mut buf);
        let decoded = Cqe::decode(&buf).unwrap();
        assert_eq!(c, decoded);
        assert!(decoded.is_err());
    }

    #[test]
    fn ok_helper() {
        let c = Cqe::ok(42, 1024);
        assert_eq!(c.user_data, 42);
        assert_eq!(c.result, 1024);
        assert!(!c.is_err());
    }

    #[test]
    fn err_helper_stores_negative() {
        let c = Cqe::err(7, 13);
        assert_eq!(c.user_data, 7);
        assert_eq!(c.result, -13);
    }

    #[test]
    fn decode_too_short() {
        let buf = [0u8; CQE_SIZE - 1];
        let err = Cqe::decode(&buf).unwrap_err();
        assert!(matches!(err, CqeDecodeError::TooShort(_)));
    }

    fn libc_einval() -> i32 {
        22 // EINVAL on Linux
    }
}
