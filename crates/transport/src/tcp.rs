//! TCP transport — entry point only. The `Conn` / `Listener` / `ReadHalf` /
//! `WriteHalf` enums in `lib.rs` carry `compio::net` types directly, so this
//! module needs nothing beyond the `TcpTransport` zero-sized type.

use crate::{AutumnTransport, Conn, Listener, TransportKind};
use async_trait::async_trait;
use compio::net::{TcpListener as CompioTcpListener, TcpStream};
use std::io;
use std::net::SocketAddr;

#[derive(Clone)]
pub struct TcpTransport;

#[async_trait(?Send)]
impl AutumnTransport for TcpTransport {
    async fn connect(&self, addr: SocketAddr) -> io::Result<Conn> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Conn::Tcp(stream))
    }

    async fn bind(&self, addr: SocketAddr) -> io::Result<Listener> {
        let l = CompioTcpListener::bind(addr).await?;
        Ok(Listener::Tcp(l))
    }

    fn kind(&self) -> TransportKind {
        TransportKind::Tcp
    }
}
