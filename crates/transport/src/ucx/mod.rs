//! UCX transport — uses `ucp_stream_*_nbx` over rc_mlx5 RoCEv2 (verified
//! in spec §12 Q1: ≥478 B payloads pick `multi-frag stream zero-copy
//! copy-out` = rndv get zcopy).

pub(crate) mod ffi;
pub(crate) mod sockaddr;
pub(crate) mod worker;
pub(crate) mod endpoint;
pub(crate) mod listener;

pub use endpoint::UcxConn;
pub use listener::UcxListener;

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
