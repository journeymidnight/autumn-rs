//! SQE opcodes — the operations a client can submit through the ring.
//!
//! Stable u8 values: any change is a wire-protocol break and bumps
//! `RING_VERSION`. Keep additions APPEND-ONLY; never reuse a freed slot.

/// Operation codes recognised by the daemon's ring poller.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Opcode {
    /// No-op — useful for warmup / round-trip latency probes. Daemon
    /// completes immediately with `result = 0`.
    Nop = 0,

    /// Open a path. Request: `length` = path bytes length, payload at
    /// `buf_offset` is the path. Reply: `result` = ring_fd on success
    /// (positive), or negative errno. The `ring_fd` is a per-session
    /// integer assigned by the daemon — NOT a kernel fd.
    Open = 1,

    /// Read from a previously-opened ring_fd. Request: `ring_fd`,
    /// `offset`, `length`, `buf_offset` (slot in client's pinned
    /// buffer pool). Daemon writes `length` bytes into that slot and
    /// completes with `result = bytes_read` or negative errno.
    Read = 2,

    /// Write to a previously-opened ring_fd. Request: `ring_fd`,
    /// `offset`, `length`, `buf_offset` (data already in that slot).
    /// Daemon completes with `result = bytes_written` or negative
    /// errno.
    Write = 3,

    /// Close a ring_fd. Request: `ring_fd`. Reply: 0 on success or
    /// negative errno. Releases daemon-side ring_fd → inode mapping
    /// and any associated state (write buffer, etc.).
    Close = 4,
    // NEXT FREE: 5
}

impl Opcode {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Nop),
            1 => Some(Self::Open),
            2 => Some(Self::Read),
            3 => Some(Self::Write),
            4 => Some(Self::Close),
            _ => None,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opcode_round_trip() {
        for op in [
            Opcode::Nop,
            Opcode::Open,
            Opcode::Read,
            Opcode::Write,
            Opcode::Close,
        ] {
            let v = op.as_u8();
            assert_eq!(Opcode::from_u8(v), Some(op));
        }
    }

    #[test]
    fn unknown_opcode_returns_none() {
        // Any value not in the enum should return None — daemon must
        // reject SQEs with unknown opcode rather than silently no-op.
        for v in 5u8..=255u8 {
            assert!(Opcode::from_u8(v).is_none(), "value {v} unexpectedly known");
        }
    }
}
