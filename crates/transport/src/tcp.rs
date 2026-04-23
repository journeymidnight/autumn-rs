//! TCP transport — wraps `compio::net`. Phase 1 Task 3 fills the body.
//!
//! Public surface lives in `lib.rs` via `enum Conn::Tcp(TcpConn)` etc.;
//! this module exports just the concrete leaf types.

#[derive(Clone)]
pub struct TcpTransport;

pub struct TcpConn;
pub struct TcpListener;
pub struct TcpReadHalf;
pub struct TcpWriteHalf;

impl TcpConn {
    pub(crate) fn peer_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        unimplemented!("Phase 1 Task 3")
    }
    pub(crate) fn into_split(self) -> (TcpReadHalf, TcpWriteHalf) {
        unimplemented!("Phase 1 Task 3")
    }
    pub(crate) fn as_inner(&self) -> &compio::net::TcpStream {
        unimplemented!("Phase 1 Task 3")
    }
}

impl compio::io::AsyncRead for TcpConn {
    async fn read<B: compio::buf::IoBufMut>(
        &mut self,
        _buf: B,
    ) -> compio::BufResult<usize, B> {
        unimplemented!("Phase 1 Task 3")
    }
}

impl compio::io::AsyncWrite for TcpConn {
    async fn write<B: compio::buf::IoBuf>(
        &mut self,
        _buf: B,
    ) -> compio::BufResult<usize, B> {
        unimplemented!("Phase 1 Task 3")
    }
    async fn flush(&mut self) -> std::io::Result<()> {
        unimplemented!("Phase 1 Task 3")
    }
    async fn shutdown(&mut self) -> std::io::Result<()> {
        unimplemented!("Phase 1 Task 3")
    }
}

impl TcpListener {
    pub(crate) async fn accept(
        &mut self,
    ) -> std::io::Result<(TcpConn, std::net::SocketAddr)> {
        unimplemented!("Phase 1 Task 3")
    }
    pub(crate) fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        unimplemented!("Phase 1 Task 3")
    }
}

impl compio::io::AsyncRead for TcpReadHalf {
    async fn read<B: compio::buf::IoBufMut>(
        &mut self,
        _buf: B,
    ) -> compio::BufResult<usize, B> {
        unimplemented!("Phase 1 Task 3")
    }
}

impl compio::io::AsyncWrite for TcpWriteHalf {
    async fn write<B: compio::buf::IoBuf>(
        &mut self,
        _buf: B,
    ) -> compio::BufResult<usize, B> {
        unimplemented!("Phase 1 Task 3")
    }
    async fn flush(&mut self) -> std::io::Result<()> {
        unimplemented!("Phase 1 Task 3")
    }
    async fn shutdown(&mut self) -> std::io::Result<()> {
        unimplemented!("Phase 1 Task 3")
    }
}
