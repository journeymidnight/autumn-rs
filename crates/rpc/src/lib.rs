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

/// Re-exported so consumers of `RpcClient::call_into_dest(reg: Option<&RegisteredMem>)`
/// don't need a direct autumn-transport dependency. (Uninhabited stub on
/// non-ucx builds — `reg` is always `None` there.)
pub use autumn_transport::RegisteredMem;
/// Re-exported so `call_into_pooled` consumers (autumn-stream's StreamClient)
/// reference `autumn_rpc::PooledBuf` without a direct autumn-transport dep.
/// Transport-agnostic: registered on `ucx`, plain (copy-out) on TCP/no-ucx.
pub use autumn_transport::{regpool_acquire, PooledBuf};
pub use error::{Result, RpcError, StatusCode};
pub use frame::{Frame, FrameDecoder, HEADER_LEN};

/// Handler result type for RPC dispatch.
pub type HandlerResult = std::result::Result<bytes::Bytes, (StatusCode, String)>;

/// Msg type reserved for heartbeat ping/pong.
pub const MSG_TYPE_PING: u8 = 0xFF;
