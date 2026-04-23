//! TCP transport — wraps `compio::net`.
//!
//! Public surface lives in `lib.rs` via `enum Conn::Tcp(TcpConn)` etc.

use crate::{AutumnTransport, Conn, Listener, TransportKind};
use async_trait::async_trait;
use compio::buf::{IoBuf, IoBufMut};
use compio::io::{AsyncRead, AsyncWrite};
use compio::net::{
    OwnedReadHalf, OwnedWriteHalf, TcpListener as CompioTcpListener, TcpStream,
};
use compio::BufResult;
use std::io;
use std::net::SocketAddr;

#[derive(Clone)]
pub struct TcpTransport;

#[async_trait(?Send)]
impl AutumnTransport for TcpTransport {
    async fn connect(&self, addr: SocketAddr) -> io::Result<Conn> {
        let stream = TcpStream::connect(addr).await?;
        let peer = stream.peer_addr()?;
        Ok(Conn::Tcp(TcpConn { stream, peer }))
    }

    async fn bind(&self, addr: SocketAddr) -> io::Result<Listener> {
        let inner = CompioTcpListener::bind(addr).await?;
        let local = inner.local_addr()?;
        Ok(Listener::Tcp(TcpListener { inner, local }))
    }

    fn kind(&self) -> TransportKind {
        TransportKind::Tcp
    }
}

pub struct TcpConn {
    stream: TcpStream,
    peer: SocketAddr,
}

impl TcpConn {
    pub(crate) fn peer_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.peer)
    }

    pub(crate) fn into_split(self) -> (TcpReadHalf, TcpWriteHalf) {
        let (r, w) = self.stream.into_split();
        (TcpReadHalf(r), TcpWriteHalf(w))
    }

    pub(crate) fn as_inner(&self) -> &TcpStream {
        &self.stream
    }
}

impl compio::io::AsyncRead for TcpConn {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
        self.stream.read(buf).await
    }
}

impl compio::io::AsyncWrite for TcpConn {
    async fn write<B: IoBuf>(&mut self, buf: B) -> BufResult<usize, B> {
        self.stream.write(buf).await
    }
    async fn flush(&mut self) -> io::Result<()> {
        self.stream.flush().await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        self.stream.shutdown().await
    }
}

pub struct TcpListener {
    inner: CompioTcpListener,
    local: SocketAddr,
}

impl TcpListener {
    pub(crate) async fn accept(&mut self) -> io::Result<(TcpConn, SocketAddr)> {
        let (stream, peer) = self.inner.accept().await?;
        Ok((TcpConn { stream, peer }, peer))
    }

    pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local)
    }
}

pub struct TcpReadHalf(OwnedReadHalf<TcpStream>);
pub struct TcpWriteHalf(OwnedWriteHalf<TcpStream>);

impl compio::io::AsyncRead for TcpReadHalf {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
        self.0.read(buf).await
    }
}

impl compio::io::AsyncWrite for TcpWriteHalf {
    async fn write<B: IoBuf>(&mut self, buf: B) -> BufResult<usize, B> {
        self.0.write(buf).await
    }
    async fn flush(&mut self) -> io::Result<()> {
        self.0.flush().await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        self.0.shutdown().await
    }
}

// ---- Helper for users that want the listener concrete type ----

impl Listener {
    /// If this listener is the TCP variant, return its address. Convenience for
    /// tests/binaries that already know they're in TCP mode.
    pub fn tcp_inner(&self) -> Option<&CompioTcpListener> {
        match self {
            Listener::Tcp(l) => Some(&l.inner),
        }
    }
}
