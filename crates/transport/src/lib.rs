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
//!
//! TCP variants carry `compio::net` types directly; UCX variants (Phase 3)
//! will carry the custom `UcxConn` / `UcxListener` types from `crate::ucx`.

use std::io;
use std::net::SocketAddr;
use std::sync::OnceLock;

mod probe;
mod tcp;
#[cfg(feature = "ucx")]
mod ucx;

pub use probe::{decide, Decision};
pub use tcp::TcpTransport;
#[cfg(feature = "ucx")]
pub use ucx::UcxTransport;

static GLOBAL: OnceLock<Box<dyn AutumnTransport>> = OnceLock::new();

/// Initialise the process-global transport. Idempotent; first call wins.
/// Phase 2 returns `TcpTransport` unconditionally; Phase 4 will honour
/// `AUTUMN_TRANSPORT` via `decide()`.
pub fn init() -> &'static dyn AutumnTransport {
    let _ = GLOBAL.set(Box::new(TcpTransport));
    let t = &**GLOBAL.get().expect("init");
    tracing::info!("autumn-transport: init kind={:?}", t.kind());
    t
}

/// Read the process-global transport. Panics if `init()` was never called.
///
/// Use this from binaries that call `init()` explicitly at startup so a
/// missing initialisation surfaces as a panic rather than silent fallback.
pub fn current() -> &'static dyn AutumnTransport {
    &**GLOBAL
        .get()
        .expect("autumn_transport::init() must be called once at startup")
}

/// Read the process-global transport, lazily initialising it if needed.
///
/// Use this from library code (`autumn-rpc`, `autumn-stream`, …) so that
/// callers (tests, third-party users) don't have to remember to call
/// `init()` first.
pub fn current_or_init() -> &'static dyn AutumnTransport {
    if let Some(t) = GLOBAL.get() {
        return &**t;
    }
    init()
}

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

pub enum Conn {
    Tcp(compio::net::TcpStream),
    #[cfg(feature = "ucx")]
    Ucx(crate::ucx::endpoint::UcxConn),
}

pub enum Listener {
    Tcp(compio::net::TcpListener),
    #[cfg(feature = "ucx")]
    Ucx(crate::ucx::listener::UcxListener),
}

pub enum ReadHalf {
    Tcp(compio::net::OwnedReadHalf<compio::net::TcpStream>),
    #[cfg(feature = "ucx")]
    Ucx(crate::ucx::endpoint::UcxReadHalf),
}

pub enum WriteHalf {
    Tcp(compio::net::OwnedWriteHalf<compio::net::TcpStream>),
    #[cfg(feature = "ucx")]
    Ucx(crate::ucx::endpoint::UcxWriteHalf),
}

// ---- Conn API ----

impl Conn {
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Conn::Tcp(s) => s.peer_addr(),
            #[cfg(feature = "ucx")]
            Conn::Ucx(c) => c.peer_addr(),
        }
    }

    pub fn into_split(self) -> (ReadHalf, WriteHalf) {
        match self {
            Conn::Tcp(s) => {
                let (r, w) = s.into_split();
                (ReadHalf::Tcp(r), WriteHalf::Tcp(w))
            }
            #[cfg(feature = "ucx")]
            Conn::Ucx(c) => {
                let (r, w) = c.into_split();
                (ReadHalf::Ucx(r), WriteHalf::Ucx(w))
            }
        }
    }

    /// `Some(_)` only for the `Tcp` variant — call sites that need TCP-only
    /// socket tuning (`SO_RCVBUF`, `TCP_NODELAY`) gate on this.
    pub fn as_tcp(&self) -> Option<&compio::net::TcpStream> {
        match self {
            Conn::Tcp(s) => Some(s),
            #[cfg(feature = "ucx")]
            Conn::Ucx(_) => None,
        }
    }
}

impl compio::io::AsyncRead for Conn {
    async fn read<B: compio::buf::IoBufMut>(
        &mut self,
        buf: B,
    ) -> compio::BufResult<usize, B> {
        match self {
            Conn::Tcp(s) => s.read(buf).await,
            #[cfg(feature = "ucx")]
            Conn::Ucx(c) => c.read(buf).await,
        }
    }
}

impl compio::io::AsyncWrite for Conn {
    async fn write<B: compio::buf::IoBuf>(
        &mut self,
        buf: B,
    ) -> compio::BufResult<usize, B> {
        match self {
            Conn::Tcp(s) => s.write(buf).await,
            #[cfg(feature = "ucx")]
            Conn::Ucx(c) => c.write(buf).await,
        }
    }
    async fn flush(&mut self) -> io::Result<()> {
        match self {
            Conn::Tcp(s) => s.flush().await,
            #[cfg(feature = "ucx")]
            Conn::Ucx(c) => c.flush().await,
        }
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        match self {
            Conn::Tcp(s) => s.shutdown().await,
            #[cfg(feature = "ucx")]
            Conn::Ucx(c) => c.shutdown().await,
        }
    }
}

// ---- Listener API ----

impl Listener {
    pub async fn accept(&mut self) -> io::Result<(Conn, SocketAddr)> {
        match self {
            Listener::Tcp(l) => {
                let (s, peer) = l.accept().await?;
                Ok((Conn::Tcp(s), peer))
            }
            #[cfg(feature = "ucx")]
            Listener::Ucx(l) => {
                let (c, peer) = l.accept().await?;
                Ok((Conn::Ucx(c), peer))
            }
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Listener::Tcp(l) => l.local_addr(),
            #[cfg(feature = "ucx")]
            Listener::Ucx(l) => l.local_addr(),
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
            #[cfg(feature = "ucx")]
            ReadHalf::Ucx(r) => r.read(buf).await,
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
            #[cfg(feature = "ucx")]
            WriteHalf::Ucx(w) => w.write(buf).await,
        }
    }
    async fn flush(&mut self) -> io::Result<()> {
        match self {
            WriteHalf::Tcp(w) => w.flush().await,
            #[cfg(feature = "ucx")]
            WriteHalf::Ucx(w) => w.flush().await,
        }
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        match self {
            WriteHalf::Tcp(w) => w.shutdown().await,
            #[cfg(feature = "ucx")]
            WriteHalf::Ucx(w) => w.shutdown().await,
        }
    }
}
