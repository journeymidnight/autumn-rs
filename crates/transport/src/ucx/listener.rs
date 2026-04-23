//! UCX listener — wraps `ucp_listener_create`. Skeleton; Task 17 fills in
//! `bind`, `accept`, `local_addr`.

use crate::ucx::endpoint::UcxConn;
use std::io;
use std::net::SocketAddr;

pub struct UcxListener {
    _addr: SocketAddr,
}

impl UcxListener {
    pub(crate) async fn bind(_addr: SocketAddr) -> io::Result<Self> {
        unimplemented!("Phase 3 Task 17: ucp_listener_create")
    }

    pub(crate) async fn accept(&mut self) -> io::Result<(UcxConn, SocketAddr)> {
        unimplemented!("Phase 3 Task 17: on_conn handler -> mpsc -> accept")
    }

    pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self._addr)
    }
}
