//! autumn-rpc: wire protocol framing for custom binary RPC.
//!
//! Provides a 10-byte-header wire protocol with request multiplexing IDs.
//!
//! # Wire Format
//!
//! ```text
//! [req_id: u32 LE][msg_type: u8][flags: u8][payload_len: u32 LE][payload]
//! ```

pub mod client;
pub mod error;
pub mod frame;
pub mod manager_rpc;
pub mod partition_rpc;
pub mod pool;

pub use error::{RpcError, Result, StatusCode};
pub use frame::{Frame, FrameDecoder, HEADER_LEN};
/// Re-exported so consumers of `RpcClient::call_into_dest(reg: Option<&RegisteredMem>)`
/// don't need a direct autumn-transport dependency. (Uninhabited stub on
/// non-ucx builds — `reg` is always `None` there.)
pub use autumn_transport::RegisteredMem;

/// Handler result type for RPC dispatch.
pub type HandlerResult = std::result::Result<bytes::Bytes, (StatusCode, String)>;

/// Msg type reserved for heartbeat ping/pong.
pub const MSG_TYPE_PING: u8 = 0xFF;
