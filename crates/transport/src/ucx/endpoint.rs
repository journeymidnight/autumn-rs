//! UCX endpoint = one `ucp_ep` to a peer. Skeleton; Task 16 fills in
//! `connect`, `read`, `write`, `into_split`.

use std::io;
use std::net::SocketAddr;

pub struct UcxConn {
    _peer: SocketAddr,
}

impl UcxConn {
    pub(crate) async fn connect(_addr: SocketAddr) -> io::Result<Self> {
        unimplemented!("Phase 3 Task 16: ucp_ep_create over rc_mlx5")
    }

    pub(crate) fn peer_addr(&self) -> io::Result<SocketAddr> {
        Ok(self._peer)
    }

    pub(crate) fn into_split(self) -> (UcxReadHalf, UcxWriteHalf) {
        unimplemented!("Phase 3 Task 16")
    }
}

impl compio::io::AsyncRead for UcxConn {
    async fn read<B: compio::buf::IoBufMut>(
        &mut self,
        _buf: B,
    ) -> compio::BufResult<usize, B> {
        unimplemented!("Phase 3 Task 16: ucp_stream_recv_nbx")
    }
}

impl compio::io::AsyncWrite for UcxConn {
    async fn write<B: compio::buf::IoBuf>(
        &mut self,
        _buf: B,
    ) -> compio::BufResult<usize, B> {
        unimplemented!("Phase 3 Task 16: ucp_stream_send_nbx")
    }
    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub struct UcxReadHalf {
    _peer: SocketAddr,
}
pub struct UcxWriteHalf {
    _peer: SocketAddr,
}

impl compio::io::AsyncRead for UcxReadHalf {
    async fn read<B: compio::buf::IoBufMut>(
        &mut self,
        _buf: B,
    ) -> compio::BufResult<usize, B> {
        unimplemented!("Phase 3 Task 16")
    }
}

impl compio::io::AsyncWrite for UcxWriteHalf {
    async fn write<B: compio::buf::IoBuf>(
        &mut self,
        _buf: B,
    ) -> compio::BufResult<usize, B> {
        unimplemented!("Phase 3 Task 16")
    }
    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}
