//! `IoRingClient` — synchronous client API for the autumn-ioring SHM
//! channel.
//!
//! Lives on the application side. Talks to a running
//! `autumn-ioring-daemon` over a Unix socket for the handshake, then
//! shares an mmap-backed SHM region for the hot-path SQE/CQE
//! traffic — no syscalls per submit / per completion.
//!
//! # API shape (F180-C)
//!
//! ```ignore
//! let mut client = IoRingClient::connect("/run/autumn-ioring/ring.sock")?;
//! let path_slot = client.alloc_buf_slot().unwrap();
//! client.write_buf(path_slot, b"dataset/foo.bin");
//! client.submit(Sqe {
//!     opcode: Opcode::Open,
//!     ring_fd: 0,
//!     offset: 0,
//!     length: b"dataset/foo.bin".len() as u32,
//!     buf_offset: path_slot,
//!     user_data: 1,
//! })?;
//! let cqe = client.wait_completion();
//! assert!(cqe.result >= 0);
//! let ring_fd = cqe.result as u32;
//! ```
//!
//! # Threading
//!
//! The client is `!Send` because it owns mmap + file descriptors that
//! shouldn't be moved between threads casually. For multi-threaded
//! benchmarks, open one client per thread.
//!
//! # Buffer pool allocator
//!
//! v1 is a simple bump allocator with explicit `free_buf_slot`. Holes
//! are tracked in a free list. Sufficient for AI workloads where
//! batches reuse the same slots. Production may want a more robust
//! allocator (size classes, etc.) — out of F180-C scope.

use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd};
use std::path::Path;
use std::time::Duration;

use crate::cqe::Cqe;
use crate::handshake::{HelloRequest, HelloStatus};
use crate::header::{HEADER_SIZE, RingHeader};
use crate::mmap::{prot, MmapRegion};
use crate::ring::{CqConsumer, SqProducer};
use crate::socket;
use crate::sqe::Sqe;

/// Errors specific to the client side.
#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("daemon rejected handshake: {0:?}")]
    HandshakeRejected(HelloStatus),
    #[error("malformed daemon response: {0}")]
    BadResponse(#[from] crate::handshake::HandshakeDecodeError),
    #[error("malformed ring header: {0}")]
    BadHeader(#[from] crate::header::RingHeaderDecodeError),
}

/// Synchronous client to a running autumn-ioring-daemon.
pub struct IoRingClient {
    /// Holds the Unix socket open for the duration of the session.
    /// Daemon watches for EOF on this to clean up the session.
    _socket_fd: OwnedFd,
    /// Holds the SHM memfd open. Kernel-refcounts the underlying
    /// inode; daemon holds its own reference.
    _shm_fd: OwnedFd,
    /// Mapped SHM region.
    region: MmapRegion,
    /// Decoded ring header (cached; never changes after handshake).
    header: RingHeader,
    /// Buffer-pool allocator state. Free list of (offset, size) slots.
    free_slots: Vec<u64>,
}

impl IoRingClient {
    /// Connect to the daemon at `socket_path` and complete the
    /// handshake. On success the SHM region is mapped and ready for
    /// SQE submission.
    pub fn connect(socket_path: &Path) -> Result<Self, ClientError> {
        Self::connect_with(socket_path, HelloRequest::defaults())
    }

    /// Like `connect` but lets the caller customise the request
    /// (smaller ring, different buffer pool size, etc.).
    pub fn connect_with(
        socket_path: &Path,
        req: HelloRequest,
    ) -> Result<Self, ClientError> {
        // 1. Connect AF_UNIX socket via libc (avoids the std blocking
        //    socket layer; we want raw fd for sendmsg/recvmsg).
        let path_c = std::ffi::CString::new(socket_path.as_os_str().as_encoded_bytes())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let sock_fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        if sock_fd < 0 {
            return Err(ClientError::Io(io::Error::last_os_error()));
        }
        // SAFETY: socket() returned valid fd.
        let socket = unsafe { OwnedFd::from_raw_fd(sock_fd) };
        // Build sockaddr_un.
        let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
        let path_bytes = path_c.as_bytes_with_nul();
        if path_bytes.len() > addr.sun_path.len() {
            return Err(ClientError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "socket path too long",
            )));
        }
        for (i, &b) in path_bytes.iter().enumerate() {
            addr.sun_path[i] = b as libc::c_char;
        }
        let len = std::mem::size_of::<libc::sa_family_t>() + path_bytes.len() - 1;
        let rc = unsafe {
            libc::connect(
                socket.as_raw_fd(),
                &addr as *const _ as *const libc::sockaddr,
                len as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(ClientError::Io(io::Error::last_os_error()));
        }

        // 2. Send HelloRequest.
        socket::send_request(socket.as_raw_fd(), &req)?;

        // 3. Receive HelloResponse + memfd.
        let (resp, shm_fd) = socket::recv_response_with_fd(socket.as_raw_fd())?;
        if resp.status != HelloStatus::Ok {
            return Err(ClientError::HandshakeRejected(resp.status));
        }

        // 4. mmap the SHM region.
        // Compute its size from the daemon-confirmed sizes (which may
        // have been clamped from our request).
        let mut header_template = RingHeader::new(resp.session_id);
        header_template.sq_entries = resp.sq_entries;
        header_template.cq_entries = resp.cq_entries;
        header_template.buf_pool_size = resp.buf_pool_size;
        header_template.buf_slot_size = resp.buf_slot_size;
        header_template.buf_pool_offset = header_template.cq_array_offset()
            + (header_template.cq_entries as u64) * (crate::cqe::CQE_SIZE as u64);
        let shm_size = header_template.total_size() as usize;

        let region = MmapRegion::map(&shm_fd, shm_size, prot::READ_WRITE)?;

        // 5. Re-decode the header from SHM (daemon wrote it; we
        //    sanity-check vs our local computation).
        let header = RingHeader::decode(&region.as_slice()[..HEADER_SIZE as usize])?;
        if header.session_id != resp.session_id {
            return Err(ClientError::Io(io::Error::new(
                io::ErrorKind::Other,
                "session_id mismatch between handshake response and SHM header",
            )));
        }

        // 6. Build the buffer-pool free list. Slot allocations bump
        //    from the bottom; freed slots get pushed onto the LIFO.
        // Initially all slots are free; populate the free list lazily
        // (next_free_slot starts at 0, no Vec entries needed).
        let layout = crate::buffer_pool::BufferPoolLayout::from_header(&header);
        let initial_free: Vec<u64> = (0..layout.num_slots())
            .rev() // pop order = ascending → LIFO returns slot 0 first
            .map(|i| layout.slot_offset(i).unwrap())
            .collect();

        Ok(Self {
            _socket_fd: socket,
            _shm_fd: shm_fd,
            region,
            header,
            free_slots: initial_free,
        })
    }

    /// Decoded ring header. Stable for the lifetime of the client.
    pub fn header(&self) -> &RingHeader {
        &self.header
    }

    /// Number of buffer pool slots that have not yet been allocated.
    pub fn free_slot_count(&self) -> usize {
        self.free_slots.len()
    }

    /// Allocate a buffer-pool slot. Returns the SHM byte offset of
    /// the slot's start; pass this to `Sqe.buf_offset`.
    pub fn alloc_buf_slot(&mut self) -> Option<u64> {
        self.free_slots.pop()
    }

    /// Return a slot to the free list. Caller must guarantee no
    /// in-flight SQE references this slot when called.
    pub fn free_buf_slot(&mut self, offset: u64) {
        self.free_slots.push(offset);
    }

    /// Borrow a slot's bytes for read access.
    pub fn buf_at(&self, offset: u64, len: usize) -> &[u8] {
        &self.region.as_slice()[offset as usize..offset as usize + len]
    }

    /// Borrow a slot's bytes for write access — used when seeding
    /// `OPEN` path bytes or staging a `WRITE` payload.
    pub fn write_buf(&mut self, offset: u64, src: &[u8]) {
        let dst = &mut self.region.as_mut_slice()
            [offset as usize..offset as usize + src.len()];
        dst.copy_from_slice(src);
    }

    /// Try to submit one SQE. Returns `Err(sqe)` echoing it back if SQ is full.
    pub fn submit(&mut self, sqe: Sqe) -> Result<(), Sqe> {
        let mut prod = SqProducer::new(self.region.as_mut_slice(), self.header);
        prod.try_push(sqe)
    }

    /// Try to pop one CQE. Returns None if no completion is ready.
    pub fn try_completion(&self) -> Option<Cqe> {
        let cons = CqConsumer::new(self.region.as_slice(), self.header);
        cons.try_pop()
    }

    /// Block (with adaptive backoff) until a CQE is available.
    /// `idle_us` is the busy-poll backoff after the first miss.
    pub fn wait_completion(&self, idle_us: u64) -> Cqe {
        // Tight spin first, then sleep. Keeps low-latency reads fast
        // without burning a core when the daemon is idle.
        let cons = CqConsumer::new(self.region.as_slice(), self.header);
        for _ in 0..256 {
            if let Some(c) = cons.try_pop() {
                return c;
            }
            std::hint::spin_loop();
        }
        loop {
            if let Some(c) = cons.try_pop() {
                return c;
            }
            if idle_us > 0 {
                std::thread::sleep(Duration::from_micros(idle_us));
            } else {
                std::thread::yield_now();
            }
        }
    }

    /// Drain up to `max` completions into `dst`. Returns count drained.
    pub fn drain_completions(&self, dst: &mut Vec<Cqe>, max: usize) -> usize {
        let cons = CqConsumer::new(self.region.as_slice(), self.header);
        cons.try_pop_batch(dst, max)
    }
}
