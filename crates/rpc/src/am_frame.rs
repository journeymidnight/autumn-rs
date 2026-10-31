//! Frames whose large value parts travel as UCX active messages
//! (`FLAG_AM_VALUE`, see `frame.rs`). By rendezvous the receiver's NIC reads
//! each part straight into the receive buffer, where a stream receive costs
//! the receiving CPU one copy of every byte.
//!
//! Used on a UCX connection whose peer is at `WIRE_VERSION_WITH_AM_VALUES`
//! (`AmOut::for_connection`), for frames with a value part of at least
//! `BULK_MIN_BYTES`. Every receive loop of such a peer assembles the value
//! (`receive_value`) before the frame goes on, so frames keep their order.

use crate::error::{RpcError, StatusCode};
use crate::frame::{encode_am_frame, Frame};
use autumn_transport::{am, PoolError, PoolUse, PooledBuf, ReadHalf, RegBytes, WriteHalf};
use bytes::Bytes;
use std::cell::Cell;
use std::rc::Rc;

/// Sends one connection's frame values.
pub struct AmOut {
    sender: am::Sender,
    next_tag: Cell<u64>,
}

impl AmOut {
    /// `Some` on a UCX connection to a peer that reads frame values.
    pub fn for_connection(writer: &WriteHalf, peer_wire: u32) -> Option<Rc<Self>> {
        if peer_wire < crate::WIRE_VERSION_WITH_AM_VALUES {
            return None;
        }
        writer.am_sender().map(|sender| Rc::new(Self { sender, next_tag: Cell::new(1) }))
    }

    /// Whether a frame with these value parts goes with active messages.
    pub fn wants<V: AsRef<[u8]>>(value_parts: &[V]) -> bool {
        value_parts.iter().any(|p| p.as_ref().len() >= autumn_transport::BULK_MIN_BYTES)
    }

    /// The stream buffers of the frame, and the values to `post` once it is
    /// certain to be written.
    pub fn encode<V: Into<RegBytes>>(
        &self,
        req_id: u32,
        msg_type: u8,
        flags: u8,
        ctrl_parts: Vec<Bytes>,
        value_parts: Vec<V>,
    ) -> (Vec<Bytes>, Vec<(u64, RegBytes)>) {
        encode_am_frame(req_id, msg_type, flags, ctrl_parts, value_parts, || {
            let tag = self.next_tag.get();
            self.next_tag.set(tag + 1);
            tag
        })
    }

    pub fn post(&self, values: Vec<(u64, RegBytes)>) {
        self.sender.post_frame_values(values);
    }

    /// `encode` and `post` at once, for a reply that is written whatever
    /// happens (a server's): the frame's stream buffers.
    pub fn frame_now<V: Into<RegBytes>>(
        &self,
        req_id: u32,
        msg_type: u8,
        flags: u8,
        ctrl_parts: Vec<Bytes>,
        value_parts: Vec<V>,
    ) -> Vec<Bytes> {
        let (bufs, values) = self.encode(req_id, msg_type, flags, ctrl_parts, value_parts);
        self.post(values);
        bufs
    }

    /// A value-separable frame already encoded as `head` (`[header][ctrl_len]
    /// [ctrl][crc]`) and its value parts, as stream buffers: unchanged when no
    /// part is large enough, else re-encoded with the large parts posted as
    /// frame values.
    pub fn reframe(&self, head: Bytes, values: Vec<RegBytes>) -> Vec<Bytes> {
        use crate::frame::{CTRL_PREFIX_LEN, HEADER_LEN};
        if !Self::wants(&values) {
            let mut bufs = Vec::with_capacity(1 + values.len());
            bufs.push(head);
            bufs.extend(values.into_iter().map(RegBytes::into_bytes));
            return bufs;
        }
        let req_id = u32::from_le_bytes(head[0..4].try_into().unwrap());
        let ctrl_len = u32::from_le_bytes(head[HEADER_LEN..HEADER_LEN + CTRL_PREFIX_LEN].try_into().unwrap()) as usize;
        let ctrl_at = HEADER_LEN + CTRL_PREFIX_LEN;
        let ctrl = head.slice(ctrl_at..ctrl_at + ctrl_len);
        self.frame_now(req_id, head[4], head[5], vec![ctrl], values)
    }
}

/// A decoded frame whose value may still be arriving (`start`).
pub struct Incoming {
    frame: Frame,
    value: Value,
}

enum Value {
    None,
    Started(am::FrameValue),
    /// Too large for one pool buffer: one per piece. `keep`: the frame keeps
    /// them as parts (`Frame::value_parts`); otherwise they are joined into
    /// one heap buffer.
    Parts { started: am::FrameValueParts, keep: bool },
    /// No buffer: the value is let go when the frame is taken. Dropped without
    /// being taken (only on the way to ending the connection) its senders wait
    /// until the endpoint is forgotten.
    Refused { pieces: Vec<(usize, u64)> },
}

/// What `start` does when the use's budget has no room for a value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WhenFull {
    /// Refuse the frame (`Unavailable`): a read loop must not wait, it would
    /// hold up every other frame and the pings on its connection.
    Refuse,
    /// Take an unbudgeted pool buffer: for values whose in-flight count the
    /// sender already bounds (the PS's appends to an extent node).
    Unbudgeted,
}

/// Why a frame came without its value.
pub enum IncomingError {
    /// No pool buffer for the value: the value was let go (its senders
    /// complete) and the request should be answered, not the connection
    /// dropped.
    NoBuffer { req_id: u32, msg_type: u8 },
    Io(std::io::Error),
}

impl IncomingError {
    /// The `Unavailable` error frame standing for a frame refused for want of
    /// a buffer, `None` for errors that end the connection.
    pub fn refusal(&self) -> Option<Frame> {
        match self {
            IncomingError::NoBuffer { req_id, msg_type } => Some(Frame::error(
                *req_id,
                *msg_type,
                RpcError::encode_status(StatusCode::Unavailable, &PoolError::Full.to_string()),
            )),
            IncomingError::Io(_) => None,
        }
    }
}

impl std::fmt::Display for IncomingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IncomingError::NoBuffer { .. } => write!(f, "frame value: {}", PoolError::Full),
            IncomingError::Io(e) => write!(f, "frame value: {e}"),
        }
    }
}

impl From<std::io::Error> for IncomingError {
    fn from(e: std::io::Error) -> Self {
        IncomingError::Io(e)
    }
}

/// Bytes of a decoded frame's value that come as active messages.
pub fn am_value_len(frame: &Frame) -> usize {
    frame.am_pieces.as_ref().map_or(0, |p| p.iter().filter(|p| p.1 != 0).map(|p| p.0).sum())
}

/// Start receiving a decoded frame's value if part of it comes as active
/// messages, into a buffer counted against `u` (a request's value is
/// `Receive`, a reply's `Reply`); with no room, as `when_full` says. Start every
/// frame of a burst before waiting for the first, so their values are received
/// together; then take them in order. A value larger than a thread may hold of
/// the budget is received unbudgeted. One larger than a pool buffer
/// (`POOL_MAX_BUFFER_BYTES`: an SST appended in segments, a WAL batch of large
/// values) is received one pool buffer per piece, never into memory UCX would
/// register itself: an extent node's appends (`WhenFull::Unbudgeted`) keep the
/// parts, anything else gets them joined into one heap buffer (a copy).
pub fn start(reader: &ReadHalf, mut frame: Frame, u: PoolUse, when_full: WhenFull) -> std::io::Result<Incoming> {
    let value = match frame.am_pieces.take() {
        Some(pieces) if pieces.iter().map(|p| p.0).sum::<usize>() > autumn_transport::POOL_MAX_BUFFER_BYTES => {
            let started = am::start_frame_value_parts(reader, &pieces, &frame.value, u)?;
            Value::Parts { started, keep: when_full == WhenFull::Unbudgeted }
        }
        Some(pieces) => {
            let total = pieces.iter().map(|p| p.0).sum();
            let pb = match when_full {
                WhenFull::Unbudgeted => Some(autumn_transport::regpool_acquire_charged_if_room(u, total)),
                WhenFull::Refuse => match autumn_transport::regpool_try_acquire_for(u, total) {
                    Ok(pb) => Some(pb),
                    Err(PoolError::TooLarge) => Some(autumn_transport::regpool_acquire(total)),
                    Err(PoolError::Full) => None,
                },
            };
            match pb {
                Some(pb) => Value::Started(am::start_frame_value(reader, &pieces, &frame.value, pb)?),
                None => Value::Refused { pieces },
            }
        }
        None => Value::None,
    };
    Ok(Incoming { frame, value })
}

impl Incoming {
    /// The frame, and the whole value in a pool buffer if it came as active
    /// messages (then `frame.value` holds only the stream pieces). A refused
    /// value is let go here.
    pub async fn into_parts(self, reader: &ReadHalf) -> Result<(Frame, Option<PooledBuf>), IncomingError> {
        let Incoming { mut frame, value } = self;
        let started = match value {
            Value::None => return Ok((frame, None)),
            Value::Parts { started, keep } => {
                let parts = started.wait().await?;
                frame.value = RegBytes::default();
                if keep {
                    frame.value_parts = parts;
                } else {
                    let mut all = Vec::with_capacity(parts.iter().map(|p| p.len()).sum());
                    for p in &parts {
                        all.extend_from_slice(p);
                    }
                    frame.value = Bytes::from(all).into();
                }
                return Ok((frame, None));
            }
            Value::Started(v) => v,
            Value::Refused { pieces } => {
                am::abandon_frame_value(reader, &pieces);
                return Err(IncomingError::NoBuffer { req_id: frame.req_id, msg_type: frame.msg_type });
            }
        };
        let pb = started.wait().await?;
        log_first_receive(pb.len());
        Ok((frame, Some(pb)))
    }

    /// The frame with its whole value in `frame.value`.
    pub async fn into_frame(self, reader: &ReadHalf) -> Result<Frame, IncomingError> {
        let (mut frame, value) = self.into_parts(reader).await?;
        if let Some(pb) = value {
            frame.value = pb.freeze();
        }
        Ok(frame)
    }
}

/// Logged once per process, so a run shows the frame values are in use.
fn log_first_receive(len: usize) {
    static SEEN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if !SEEN.swap(true, std::sync::atomic::Ordering::Relaxed) {
        tracing::info!(value_len = len, "received a frame value by UCX rendezvous (first in this process)");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A frame with no buffer for its value is answered with an error frame
    /// for its request; other errors end the connection.
    #[test]
    fn a_refused_frame_is_answered_for_its_request() {
        let f = IncomingError::NoBuffer { req_id: 7, msg_type: 0x51 }.refusal().expect("answered");
        assert_eq!((f.req_id, f.msg_type), (7, 0x51));
        assert!(f.is_error());
        assert_eq!(RpcError::decode_status(&f.payload).0, StatusCode::Unavailable);
        assert!(IncomingError::Io(std::io::Error::other("gone")).refusal().is_none());
    }
}
