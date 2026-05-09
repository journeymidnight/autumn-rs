//! Mmap wrapper used by both the daemon and the client to access a
//! shared SHM region backed by a memfd file descriptor.
//!
//! Lifetime: the `MmapRegion` owns the mmap mapping and `munmap`s on
//! drop. The backing file descriptor is held separately by the caller
//! (the daemon needs it after handshake to keep the inode alive for
//! the client; the client received it via SCM_RIGHTS).
//!
//! Both daemon and client mmap with `MAP_SHARED` so writes from one
//! side are visible to the other immediately. Atomic SQ/CQ index ops
//! provide ordering — `MAP_SHARED` guarantees byte-level coherence;
//! the atomic ops bound the visibility relative to surrounding loads
//! and stores.

use std::io;
use std::os::unix::io::{AsRawFd, RawFd};
use std::ptr;

/// PROT flags accepted by [`MmapRegion::map`].
pub mod prot {
    pub const READ: i32 = libc::PROT_READ;
    pub const WRITE: i32 = libc::PROT_WRITE;
    pub const READ_WRITE: i32 = libc::PROT_READ | libc::PROT_WRITE;
}

/// Owned mmap mapping. Drops by `munmap`-ing the region.
pub struct MmapRegion {
    ptr: *mut u8,
    len: usize,
}

// SAFETY: Send because the underlying `*mut u8` points into a shared
// memory region with no thread-local state. Sync because the only
// safe public APIs are `as_slice()` / `as_mut_slice()` which surface
// borrow-checked slices, and atomic operations through pointer math
// inside `ring` are explicitly thread-safe (AtomicU64 ops).
unsafe impl Send for MmapRegion {}
unsafe impl Sync for MmapRegion {}

impl MmapRegion {
    /// Map `len` bytes starting at offset 0 of `fd` with `prot`
    /// permissions and `MAP_SHARED`.
    pub fn map<F: AsRawFd>(fd: &F, len: usize, prot: i32) -> io::Result<Self> {
        Self::map_raw(fd.as_raw_fd(), len, prot)
    }

    fn map_raw(fd: RawFd, len: usize, prot: i32) -> io::Result<Self> {
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                prot,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            ptr: ptr as *mut u8,
            len,
        })
    }

    /// Total mapped size in bytes.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Borrow the mapping as a `&[u8]`.
    ///
    /// Multiple concurrent immutable borrows are fine. Atomic
    /// operations on `AtomicU64` views into this slice are also fine
    /// even from other threads — they bypass Rust's borrow checker
    /// via raw pointer ops.
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` is valid for `len` bytes from a successful
        // `mmap` call; `Self` owns the mapping for at least the
        // returned reference's lifetime.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    /// Borrow the mapping as a `&mut [u8]`.
    ///
    /// **Caution**: with `MAP_SHARED`, another process may write to
    /// the underlying memory while you hold this `&mut [u8]`. Rust's
    /// non-aliasing rule still applies WITHIN this process, but
    /// across-process modifications break the assumption that `&mut`
    /// has exclusive access. Callers MUST use atomic operations
    /// (e.g., AtomicU64 ops on the SQ/CQ index cells) for any data
    /// the peer also reads/writes.
    ///
    /// Producer-side ring writers and CQ writers in `ring.rs` follow
    /// this rule.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: see `as_slice`. `&mut self` enforces no other
        // simultaneous reference *within this process*.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for MmapRegion {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: `ptr` came from `mmap` with `len` bytes; we own
            // the mapping. Errors from munmap (typically only EINVAL
            // for misaligned/bad address) are ignored — there's
            // nothing useful a Drop impl can do.
            let _ = unsafe { libc::munmap(self.ptr as *mut libc::c_void, self.len) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::socket::create_memfd;

    #[test]
    fn map_then_drop() {
        let fd = create_memfd("autumn-mmap-test", 4096).unwrap();
        let region = MmapRegion::map(&fd, 4096, prot::READ_WRITE).unwrap();
        assert_eq!(region.len(), 4096);
        assert!(!region.is_empty());
        drop(region);
        // No assertion — just ensure no panic / segfault on Drop.
    }

    #[test]
    fn write_then_read_through_separate_mappings() {
        let fd = create_memfd("autumn-mmap-shared", 4096).unwrap();
        // Two separate mappings of the same memfd. Writes to one are
        // visible through the other (MAP_SHARED semantics).
        {
            let mut writer = MmapRegion::map(&fd, 4096, prot::READ_WRITE).unwrap();
            writer.as_mut_slice()[..8].copy_from_slice(&0xdead_beef_cafe_babe_u64.to_le_bytes());
        }
        let reader = MmapRegion::map(&fd, 4096, prot::READ).unwrap();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&reader.as_slice()[..8]);
        assert_eq!(u64::from_le_bytes(buf), 0xdead_beef_cafe_babe);
    }

    #[test]
    fn ring_layout_fits_inside_mmap() {
        // Sanity: a default-size RingHeader's total_size fits within
        // the size we'd ftruncate the memfd to.
        let header = crate::header::RingHeader::new(0);
        let total = header.total_size() as usize;
        let fd = create_memfd("autumn-mmap-ring-fit", total as u64).unwrap();
        let region = MmapRegion::map(&fd, total, prot::READ_WRITE).unwrap();
        assert_eq!(region.len(), total);
    }

    #[test]
    fn write_via_atomic_visible_to_separate_mapping() {
        // Pattern that the daemon + client will use: producer writes
        // an AtomicU64 in one mapping, consumer reads it through a
        // separate mapping of the same memfd.
        use std::sync::atomic::{AtomicU64, Ordering};
        let fd = create_memfd("autumn-mmap-atomic", 4096).unwrap();
        let mut writer = MmapRegion::map(&fd, 4096, prot::READ_WRITE).unwrap();
        let reader = MmapRegion::map(&fd, 4096, prot::READ).unwrap();

        // Materialize an AtomicU64 view at offset 0 of the writer
        // mapping, store, then read through the reader mapping.
        unsafe {
            let atom = &*(writer.as_mut_slice().as_ptr() as *const AtomicU64);
            atom.store(42, Ordering::Release);
        }
        let read_atom = unsafe { &*(reader.as_slice().as_ptr() as *const AtomicU64) };
        assert_eq!(read_atom.load(Ordering::Acquire), 42);
    }
}
