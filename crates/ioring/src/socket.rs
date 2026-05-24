//! Unix-socket transport for the autumn-fuse handshake (F180-B3).
//!
//! Wraps `sendmsg(2)` / `recvmsg(2)` with `SCM_RIGHTS` ancillary data
//! so the daemon can ship a memfd-backed SHM region's file descriptor
//! to the client alongside the `HelloResponse` bytes. The client
//! `mmap(2)`s that fd to share the ring memory.
//!
//! # Why direct libc
//!
//! The `std::os::unix::net::UnixStream` API doesn't expose ancillary
//! data; you have to drop down to `sendmsg`/`recvmsg` to ship file
//! descriptors. This module is the smallest possible safe wrapper —
//! one `unsafe` block per syscall — sufficient for the daemon
//! accept-handshake-detach handshake. It does NOT implement async I/O;
//! handshake is one-shot and synchronous on both ends.
//!
//! # Lifetime of the passed fd
//!
//! After `recv_response_with_fd` returns, the caller owns the new fd
//! and is responsible for `close(2)`. The daemon-side `send_response_with_fd`
//! does NOT close its end — the daemon keeps the fd around because
//! the SHM region needs to outlive the handshake (the daemon's ring
//! poller mmap's the same fd to read SQEs).

use std::io::{self, IoSlice, IoSliceMut};
use std::mem::{size_of, MaybeUninit};
use std::os::unix::io::{FromRawFd, OwnedFd, RawFd};

use crate::handshake::{HelloRequest, HelloResponse, HELLO_REQUEST_SIZE, HELLO_RESPONSE_SIZE};

/// Send a `HelloRequest` over `socket`. Bytes-only; client doesn't
/// pass any fd.
pub fn send_request(socket: RawFd, req: &HelloRequest) -> io::Result<()> {
    let mut buf = [0u8; HELLO_REQUEST_SIZE];
    req.encode(&mut buf);
    write_all(socket, &buf)
}

/// Receive a `HelloRequest` (daemon side, no fd attached).
pub fn recv_request(socket: RawFd) -> io::Result<HelloRequest> {
    let mut buf = [0u8; HELLO_REQUEST_SIZE];
    read_exact(socket, &mut buf)?;
    HelloRequest::decode(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Send a `HelloResponse` along with `shm_fd` via `SCM_RIGHTS`.
///
/// The daemon's caller retains `shm_fd` after this call (it's
/// duplicated in the kernel for transport). The daemon needs to keep
/// using `shm_fd` to mmap the same region for its own ring poller.
pub fn send_response_with_fd(socket: RawFd, resp: &HelloResponse, shm_fd: RawFd) -> io::Result<()> {
    let mut payload = [0u8; HELLO_RESPONSE_SIZE];
    resp.encode(&mut payload);

    let cmsg_space = unsafe { libc::CMSG_SPACE(size_of::<RawFd>() as u32) } as usize;
    let mut cmsg_buf = vec![0u8; cmsg_space];

    let iov = libc::iovec {
        iov_base: payload.as_ptr() as *mut libc::c_void,
        iov_len: payload.len(),
    };

    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &iov as *const _ as *mut _;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = cmsg_buf.len() as _;

    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if cmsg.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "CMSG_FIRSTHDR returned null (control buffer too small?)",
            ));
        }
        (*cmsg).cmsg_len = libc::CMSG_LEN(size_of::<RawFd>() as u32) as _;
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        std::ptr::copy_nonoverlapping(
            &shm_fd as *const RawFd,
            libc::CMSG_DATA(cmsg) as *mut RawFd,
            1,
        );

        // Loop in case of EINTR / partial send (handshake is small so
        // partial sendmsg is unlikely on a stream socket, but be safe).
        let n = libc::sendmsg(socket, &msg, libc::MSG_NOSIGNAL);
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        if (n as usize) < payload.len() {
            // Partial send on the data side — caller's contract is one
            // shot; surface as Other.
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!("partial sendmsg: {} of {}", n, payload.len()),
            ));
        }
    }
    Ok(())
}

/// Receive a `HelloResponse` and the attached `SHM` fd.
///
/// The returned `OwnedFd` is the new (kernel-duplicated) fd. Caller
/// owns it; drop closes the fd.
pub fn recv_response_with_fd(socket: RawFd) -> io::Result<(HelloResponse, OwnedFd)> {
    let mut payload = [0u8; HELLO_RESPONSE_SIZE];
    let cmsg_space = unsafe { libc::CMSG_SPACE(size_of::<RawFd>() as u32) } as usize;
    let mut cmsg_buf = vec![0u8; cmsg_space];

    let mut iov = libc::iovec {
        iov_base: payload.as_mut_ptr() as *mut libc::c_void,
        iov_len: payload.len(),
    };

    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov as *mut _;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = cmsg_buf.len() as _;

    let received_fd: RawFd;
    unsafe {
        let n = libc::recvmsg(socket, &mut msg, 0);
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        if (n as usize) < payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("partial recvmsg: {} of {}", n, payload.len()),
            ));
        }
        if msg.msg_flags & libc::MSG_CTRUNC != 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "MSG_CTRUNC set: control buffer too small for ancillary data",
            ));
        }

        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if cmsg.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "no SCM_RIGHTS attached to handshake response",
            ));
        }
        if (*cmsg).cmsg_level != libc::SOL_SOCKET || (*cmsg).cmsg_type != libc::SCM_RIGHTS {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "unexpected cmsg: level={} type={}",
                    (*cmsg).cmsg_level,
                    (*cmsg).cmsg_type
                ),
            ));
        }
        // We expect exactly one fd.
        let expected_len = libc::CMSG_LEN(size_of::<RawFd>() as u32) as libc::size_t;
        if (*cmsg).cmsg_len as libc::size_t != expected_len {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "unexpected cmsg payload size: got {} bytes, expected {}",
                    (*cmsg).cmsg_len,
                    expected_len
                ),
            ));
        }
        let mut fd_out: MaybeUninit<RawFd> = MaybeUninit::uninit();
        std::ptr::copy_nonoverlapping(
            libc::CMSG_DATA(cmsg) as *const RawFd,
            fd_out.as_mut_ptr(),
            1,
        );
        received_fd = fd_out.assume_init();
    }

    let resp = HelloResponse::decode(&payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    // SAFETY: `received_fd` came from the kernel as a freshly-duplicated
    // fd we have unique ownership of.
    let owned = unsafe { OwnedFd::from_raw_fd(received_fd) };
    Ok((resp, owned))
}

// ── helpers ────────────────────────────────────────────────────────────────

fn write_all(socket: RawFd, buf: &[u8]) -> io::Result<()> {
    let mut written = 0;
    while written < buf.len() {
        let iov = IoSlice::new(&buf[written..]);
        let n = unsafe {
            libc::send(
                socket,
                iov.as_ref().as_ptr() as *const libc::c_void,
                iov.as_ref().len(),
                libc::MSG_NOSIGNAL,
            )
        };
        if n < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "send returned 0 with bytes still to write",
            ));
        }
        written += n as usize;
    }
    Ok(())
}

fn read_exact(socket: RawFd, buf: &mut [u8]) -> io::Result<()> {
    let mut read = 0;
    while read < buf.len() {
        let mut iov = IoSliceMut::new(&mut buf[read..]);
        let n = unsafe {
            libc::recv(
                socket,
                iov.as_mut().as_mut_ptr() as *mut libc::c_void,
                iov.as_mut().len(),
                0,
            )
        };
        if n < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "peer closed before all expected bytes",
            ));
        }
        read += n as usize;
    }
    Ok(())
}

/// Convenience: create a connected pair of stream Unix sockets.
/// Wraps `socketpair(AF_UNIX, SOCK_STREAM, 0)`. Useful for tests and
/// for parent-child IPC. Returns (a, b); both ends are SOCK_STREAM and
/// equally usable.
pub fn socket_pair() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds: [RawFd; 2] = [-1, -1];
    let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: socketpair returned 0 → both fds are valid + unique.
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

/// Convenience: create an anonymous shared-memory file via `memfd_create`.
/// Used by the daemon to allocate the SHM region before passing the fd
/// over the handshake socket.
pub fn create_memfd(name: &str, size: u64) -> io::Result<OwnedFd> {
    let cname =
        std::ffi::CString::new(name).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let fd = unsafe { libc::memfd_create(cname.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // Set the size.
    let rc = unsafe { libc::ftruncate(fd, size as libc::off_t) };
    if rc != 0 {
        let err = io::Error::last_os_error();
        unsafe { libc::close(fd) };
        return Err(err);
    }
    // SAFETY: fd is valid (memfd_create + ftruncate both succeeded).
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handshake::{negotiate, DaemonLimits, HelloStatus};
    use std::os::unix::io::AsRawFd;

    #[test]
    fn socket_pair_works() {
        let (a, b) = socket_pair().expect("socketpair");
        // Send "hello" on a, receive on b.
        write_all(a.as_raw_fd(), b"hello").unwrap();
        let mut buf = [0u8; 5];
        read_exact(b.as_raw_fd(), &mut buf).unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn end_to_end_handshake_with_fd_passing() {
        // 1. Create socket pair (a = client, b = daemon).
        let (client_sock, daemon_sock) = socket_pair().unwrap();

        // 2. Daemon creates the SHM region.
        let shm_size = 1024 * 1024;
        let shm_fd = create_memfd("autumn-ioring-test", shm_size).unwrap();

        // Daemon writes a marker into the SHM that the client should
        // be able to see after fd passing + mmap.
        let daemon_mmap = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                shm_size as usize,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                shm_fd.as_raw_fd(),
                0,
            )
        };
        assert!(daemon_mmap != libc::MAP_FAILED);
        unsafe {
            *(daemon_mmap as *mut u32) = 0xdead_beef;
        }

        // 3. Client → server: HelloRequest.
        let req = HelloRequest::defaults();
        send_request(client_sock.as_raw_fd(), &req).unwrap();

        // 4. Server: read request, negotiate, send HelloResponse + SHM fd.
        let received_req = recv_request(daemon_sock.as_raw_fd()).unwrap();
        assert_eq!(received_req, req);
        let resp = negotiate(&received_req, &DaemonLimits::defaults(), 0xface_d00d);
        assert_eq!(resp.status, HelloStatus::Ok);
        send_response_with_fd(daemon_sock.as_raw_fd(), &resp, shm_fd.as_raw_fd()).unwrap();

        // 5. Client: read response + extract fd.
        let (received_resp, client_shm_fd) =
            recv_response_with_fd(client_sock.as_raw_fd()).unwrap();
        assert_eq!(received_resp.status, HelloStatus::Ok);
        assert_eq!(received_resp.session_id, 0xface_d00d);
        // The client's fd is a NEW kernel fd (not the same number as
        // the daemon's), but mapping it must reveal the daemon's
        // marker.
        assert_ne!(client_shm_fd.as_raw_fd(), shm_fd.as_raw_fd());

        // 6. Client mmap's the received fd and reads back the marker.
        let client_mmap = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                shm_size as usize,
                libc::PROT_READ,
                libc::MAP_SHARED,
                client_shm_fd.as_raw_fd(),
                0,
            )
        };
        assert!(client_mmap != libc::MAP_FAILED);
        let marker = unsafe { *(client_mmap as *const u32) };
        assert_eq!(marker, 0xdead_beef, "client should see daemon's marker");

        // 7. Reverse: client writes a marker, daemon reads it back.
        let client_mmap_rw = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                shm_size as usize,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                client_shm_fd.as_raw_fd(),
                0,
            )
        };
        assert!(client_mmap_rw != libc::MAP_FAILED);
        unsafe {
            *((client_mmap_rw as *mut u32).add(1)) = 0xcafe_babe;
        }
        let daemon_view = unsafe { *((daemon_mmap as *const u32).add(1)) };
        assert_eq!(
            daemon_view, 0xcafe_babe,
            "daemon should see client's marker via shared SHM"
        );

        // Cleanup: unmap.
        unsafe {
            libc::munmap(daemon_mmap, shm_size as usize);
            libc::munmap(client_mmap, shm_size as usize);
            libc::munmap(client_mmap_rw, shm_size as usize);
        }
        // OwnedFds drop → close.
    }

    #[test]
    fn negotiate_reject_passes_through() {
        let (client_sock, daemon_sock) = socket_pair().unwrap();
        // Build a request the daemon will reject (bogus version).
        let mut req = HelloRequest::defaults();
        req.proto_version = 99;
        send_request(client_sock.as_raw_fd(), &req).unwrap();

        // Daemon negotiates, decides to reject. Still sends a response
        // (without a useful fd). Use stdout fd as a placeholder (any
        // valid fd works for transport; client will see an
        // UnsupportedVersion status and discard the fd).
        let received = recv_request(daemon_sock.as_raw_fd()).unwrap();
        let resp = negotiate(&received, &DaemonLimits::defaults(), 0);
        assert_eq!(resp.status, HelloStatus::UnsupportedVersion);
        // For rejections we still have to send a fd because SCM_RIGHTS
        // requires one — use the daemon socket itself (will be a
        // duplicate). Real daemon code would either send a sentinel
        // memfd or use a separate "no-fd" reject path.
        send_response_with_fd(daemon_sock.as_raw_fd(), &resp, daemon_sock.as_raw_fd()).unwrap();

        let (received_resp, _placeholder_fd) =
            recv_response_with_fd(client_sock.as_raw_fd()).unwrap();
        assert_eq!(received_resp.status, HelloStatus::UnsupportedVersion);
    }

    #[test]
    fn write_and_read_partial_in_chunks() {
        // Verify write_all / read_exact loop correctly when peer reads
        // in smaller pieces.
        let (a, b) = socket_pair().unwrap();
        let payload = vec![0xau8; 4096];
        // Background writer.
        let writer = {
            let payload = payload.clone();
            let fd = a.as_raw_fd();
            std::thread::spawn(move || {
                write_all(fd, &payload).unwrap();
            })
        };
        let mut buf = vec![0u8; 4096];
        read_exact(b.as_raw_fd(), &mut buf).unwrap();
        writer.join().unwrap();
        assert_eq!(buf, payload);
    }

    #[test]
    fn create_memfd_returns_writable_fd() {
        let fd = create_memfd("autumn-ioring-test-memfd", 4096).unwrap();
        let map = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                4096,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        assert!(map != libc::MAP_FAILED);
        unsafe {
            *(map as *mut u64) = 0x1234_5678_9abc_def0;
            assert_eq!(*(map as *const u64), 0x1234_5678_9abc_def0);
            libc::munmap(map, 4096);
        }
    }
}
