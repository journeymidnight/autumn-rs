//! UCX transport — uses `ucp_stream_*_nbx` over rc_mlx5 RoCEv2.
//!
//! UCP Stream has no rendezvous protocol: a send is eager AM (bcopy, or zcopy
//! from registered memory for large fragments), and a receive always unpacks
//! the arrived AM fragments into the posted buffer with a memcpy
//! (`ucp_stream_rdata_unpack` in UCX 1.16 `src/ucp/stream/stream_recv.c`). A
//! `memh` on the receive does not avoid that copy; only posting the FINAL
//! destination avoids a second, application-level one.

pub(crate) mod endpoint;
pub(crate) mod ffi;
pub(crate) mod listener;
pub(crate) mod sockaddr;
pub(crate) mod worker;
// regpool is now a top-level `mod regpool` in lib.rs (transport-agnostic).

pub use endpoint::UcxConn;
pub use listener::UcxListener;
pub use worker::{register_memory, RegisteredMem};

use crate::{AutumnTransport, Conn, Listener, TransportKind};
use async_trait::async_trait;
use std::io;
use std::net::SocketAddr;

#[derive(Clone)]
pub struct UcxTransport;

#[async_trait(?Send)]
impl AutumnTransport for UcxTransport {
    async fn connect(&self, addr: SocketAddr) -> io::Result<Conn> {
        let c = UcxConn::connect(addr).await?;
        Ok(Conn::Ucx(c))
    }

    async fn bind(&self, addr: SocketAddr) -> io::Result<Listener> {
        let l = UcxListener::bind(addr).await?;
        Ok(Listener::Ucx(l))
    }

    fn kind(&self) -> TransportKind {
        TransportKind::Ucx
    }
}
