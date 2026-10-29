//! Frames whose large value parts travel as UCX active messages
//! (`FLAG_AM_VALUE`, see `frame.rs`). By rendezvous the receiver's NIC reads
//! each part straight into the receive buffer, where a stream receive costs
//! the receiving CPU one copy of every byte.
//!
//! Used on a UCX connection whose peer is at `WIRE_VERSION_WITH_AM_VALUES`
//! (`AmOut::for_connection`), for frames with a value part of at least
//! `BULK_MIN_BYTES`. Every receive loop of such a peer assembles the value
//! (`receive_value`) before the frame goes on, so frames keep their order.

use crate::frame::{encode_am_frame, Frame};
use autumn_transport::{am, PooledBuf, ReadHalf, RegBytes, WriteHalf};
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
    pub fn wants(value_parts: &[Bytes]) -> bool {
        value_parts.iter().any(|p| p.len() >= autumn_transport::BULK_MIN_BYTES)
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
        for (tag, v) in values {
            self.sender.post_frame_value(tag, v);
        }
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
    pub fn reframe(&self, head: Bytes, values: Vec<Bytes>) -> Vec<Bytes> {
        use crate::frame::{CTRL_PREFIX_LEN, HEADER_LEN};
        if !Self::wants(&values) {
            let mut bufs = Vec::with_capacity(1 + values.len());
            bufs.push(head);
            bufs.extend(values);
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
    value: Option<am::FrameValue>,
}

/// Bytes of a decoded frame's value that come as active messages.
pub fn am_value_len(frame: &Frame) -> usize {
    frame.am_pieces.as_ref().map_or(0, |p| p.iter().filter(|p| p.1 != 0).map(|p| p.0).sum())
}

/// Start receiving a decoded frame's value if part of it comes as active
/// messages. Start every frame of a burst before waiting for the first, so
/// their values are received together; then take them in order.
pub fn start(reader: &ReadHalf, mut frame: Frame) -> std::io::Result<Incoming> {
    let value = match frame.am_pieces.take() {
        Some(pieces) => Some(am::start_frame_value(reader, &pieces, &frame.value)?),
        None => None,
    };
    Ok(Incoming { frame, value })
}

impl Incoming {
    /// The frame, and the whole value in a pool buffer if it came as active
    /// messages (then `frame.value` holds only the stream pieces).
    pub async fn into_parts(self) -> std::io::Result<(Frame, Option<PooledBuf>)> {
        let value = match self.value {
            Some(v) => {
                let pb = v.wait().await?;
                log_first_receive(pb.len());
                Some(pb)
            }
            None => None,
        };
        Ok((self.frame, value))
    }

    /// The frame with its whole value in `frame.value`.
    pub async fn into_frame(self) -> std::io::Result<Frame> {
        let (mut frame, value) = self.into_parts().await?;
        if let Some(pb) = value {
            frame.value = Bytes::from_owner(pb);
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
