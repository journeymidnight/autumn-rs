//! Tiny sockaddr ↔ Rust `SocketAddr` helpers — UCX takes raw
//! `struct sockaddr*` for `ucp_ep_create` / `ucp_listener_create`.

use std::net::SocketAddr;

/// Build a `sockaddr_storage` for the given `SocketAddr`. Returns the storage
/// (you must own it for the duration of the UCX call) and the actual length
/// (`sizeof(sockaddr_in)` / `sizeof(sockaddr_in6)`).
pub(crate) fn to_storage(addr: &SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let sa = socket2::SockAddr::from(*addr);
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    unsafe {
        std::ptr::copy_nonoverlapping(
            sa.as_ptr() as *const u8,
            &mut storage as *mut _ as *mut u8,
            sa.len() as usize,
        );
    }
    (storage, sa.len())
}

/// Best-effort decode of a `sockaddr_storage` back to `SocketAddr`. Falls back
/// to `0.0.0.0:0` if the family is unknown — should not happen in practice.
pub(crate) fn from_storage(storage: &libc::sockaddr_storage) -> SocketAddr {
    unsafe {
        let sa = socket2::SockAddr::new(
            *storage,
            std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
        );
        sa.as_socket().unwrap_or_else(|| "0.0.0.0:0".parse().unwrap())
    }
}
