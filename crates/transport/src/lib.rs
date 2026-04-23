//! autumn-transport — pluggable transport for autumn-rs (TCP today, UCX optional).
//!
//! See `docs/superpowers/specs/2026-04-23-ucx-transport-design.md` (F100-UCX).
//!
//! ## Why enum dispatch instead of trait objects
//!
//! Spec §3 originally drafted `Box<dyn AutumnConn>` for runtime polymorphism.
//! That doesn't compile against compio 0.18: `compio::io::AsyncRead::read` and
//! `AsyncWrite::write` are generic over the buffer type (`B: IoBufMut`,
//! `T: IoBuf`), and a trait with generic methods is not `dyn`-compatible.
//!
//! Instead we expose concrete `enum Conn { Tcp(...), Ucx(...) }` /
//! `enum Listener` / `enum ReadHalf` / `enum WriteHalf`. The `AutumnTransport`
//! trait stays `dyn`-safe (its methods are non-generic, returning the concrete
//! enums) so `&'static dyn AutumnTransport` from `init()` still gives runtime
//! transport selection. Hot-path I/O dispatches via enum match — strictly
//! cheaper than the vtable hop §12 Q2 originally analysed.

use std::io;
use std::net::SocketAddr;

mod probe;
mod tcp;

pub use probe::{decide, Decision};
pub use tcp::TcpTransport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    Tcp,
    Ucx,
}

#[async_trait::async_trait(?Send)]
pub trait AutumnTransport: Send + Sync + 'static {
    async fn connect(&self, addr: SocketAddr) -> io::Result<Conn>;
    async fn bind(&self, addr: SocketAddr) -> io::Result<Listener>;
    fn kind(&self) -> TransportKind;
}

/// Concrete connection enum — variants added per backend (`Tcp` always present,
/// `Ucx` under `feature = "ucx"`).
pub enum Conn {
    Tcp(tcp::TcpConn),
    // #[cfg(feature = "ucx")] Ucx(crate::ucx::UcxConn),  // Phase 3
}

pub enum Listener {
    Tcp(tcp::TcpListener),
    // #[cfg(feature = "ucx")] Ucx(crate::ucx::UcxListener),  // Phase 3
}

pub enum ReadHalf {
    Tcp(tcp::TcpReadHalf),
    // #[cfg(feature = "ucx")] Ucx(crate::ucx::UcxReadHalf),  // Phase 3
}

pub enum WriteHalf {
    Tcp(tcp::TcpWriteHalf),
    // #[cfg(feature = "ucx")] Ucx(crate::ucx::UcxWriteHalf),  // Phase 3
}

// ---- Conn API ----

impl Conn {
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Conn::Tcp(c) => c.peer_addr(),
        }
    }

    pub fn into_split(self) -> (ReadHalf, WriteHalf) {
        match self {
            Conn::Tcp(c) => {
                let (r, w) = c.into_split();
                (ReadHalf::Tcp(r), WriteHalf::Tcp(w))
            }
        }
    }

    /// If this connection is backed by a TCP socket, return it for socket-level
    /// tuning (`SO_RCVBUF`, `SO_SNDBUF`, `TCP_NODELAY`). Returns `None` for
    /// non-TCP transports.
    pub fn as_tcp(&self) -> Option<&compio::net::TcpStream> {
        match self {
            Conn::Tcp(c) => Some(c.as_inner()),
        }
    }
}

impl compio::io::AsyncRead for Conn {
    async fn read<B: compio::buf::IoBufMut>(
        &mut self,
        buf: B,
    ) -> compio::BufResult<usize, B> {
        match self {
            Conn::Tcp(c) => c.read(buf).await,
        }
    }
}

impl compio::io::AsyncWrite for Conn {
    async fn write<B: compio::buf::IoBuf>(
        &mut self,
        buf: B,
    ) -> compio::BufResult<usize, B> {
        match self {
            Conn::Tcp(c) => c.write(buf).await,
        }
    }
    async fn flush(&mut self) -> io::Result<()> {
        match self {
            Conn::Tcp(c) => c.flush().await,
        }
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        match self {
            Conn::Tcp(c) => c.shutdown().await,
        }
    }
}

// ---- Listener API ----

impl Listener {
    pub async fn accept(&mut self) -> io::Result<(Conn, SocketAddr)> {
        match self {
            Listener::Tcp(l) => {
                let (c, peer) = l.accept().await?;
                Ok((Conn::Tcp(c), peer))
            }
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Listener::Tcp(l) => l.local_addr(),
        }
    }
}

// ---- Half APIs ----

impl compio::io::AsyncRead for ReadHalf {
    async fn read<B: compio::buf::IoBufMut>(
        &mut self,
        buf: B,
    ) -> compio::BufResult<usize, B> {
        match self {
            ReadHalf::Tcp(r) => r.read(buf).await,
        }
    }
}

impl compio::io::AsyncWrite for WriteHalf {
    async fn write<B: compio::buf::IoBuf>(
        &mut self,
        buf: B,
    ) -> compio::BufResult<usize, B> {
        match self {
            WriteHalf::Tcp(w) => w.write(buf).await,
        }
    }
    async fn flush(&mut self) -> io::Result<()> {
        match self {
            WriteHalf::Tcp(w) => w.flush().await,
        }
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        match self {
            WriteHalf::Tcp(w) => w.shutdown().await,
        }
    }
}
