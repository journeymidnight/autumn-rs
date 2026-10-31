//! Caller memory sent without a copy: a region the caller registers once
//! (`Region::register`) and values lent out of it (`lend`).
//!
//! A lent value is read by the transport until every clone of it is gone, which
//! can be after the call that sent it gave up (a timeout). `Returned` resolves
//! only then, so a caller that waits for it never gets its memory back while a
//! send may still read it. A value inside a registered region carries the
//! region's memh (UCX sends it zero-copy); any other is copied into a pool
//! buffer at the UCX send, and returned as soon as it is copied.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::RegBytes;

/// Caller memory registered once for zero-copy UCX sends. On TCP, or with an
/// empty range, it only records the bounds.
pub struct Region {
    start: usize,
    len: usize,
    #[cfg(feature = "ucx")]
    reg: Option<crate::RegisteredMem>,
}

impl Region {
    /// Register `[ptr, ptr + len)` with the process's UCX context (nothing on
    /// TCP).
    ///
    /// # Safety
    /// The memory must stay mapped while the returned region or any value lent
    /// from it lives.
    pub unsafe fn register(ptr: *mut u8, len: usize) -> std::io::Result<Arc<Region>> {
        #[cfg(feature = "ucx")]
        let reg = match crate::runtime_transport_is_ucx() && len > 0 {
            true => Some(crate::register_memory(ptr.cast(), len)?),
            false => None,
        };
        Ok(Arc::new(Region {
            start: ptr as usize,
            len,
            #[cfg(feature = "ucx")]
            reg,
        }))
    }

    /// Whether `[ptr, ptr + len)` lies inside this region.
    pub fn contains(&self, ptr: *const u8, len: usize) -> bool {
        let p = ptr as usize;
        p >= self.start && p.checked_add(len).is_some_and(|end| end <= self.start + self.len)
    }

    /// The registration's memh when it holds all of `[ptr, ptr + len)`.
    #[cfg(feature = "ucx")]
    pub(crate) fn memh_for(&self, ptr: *const u8, len: usize) -> Option<crate::ucx::ffi::ucp_mem_h> {
        self.reg.as_ref().filter(|_| self.contains(ptr, len)).map(|r| r.memh())
    }

    /// Whether UCX registered it.
    pub fn is_registered(&self) -> bool {
        #[cfg(feature = "ucx")]
        return self.reg.is_some();
        #[cfg(not(feature = "ucx"))]
        false
    }
}

#[derive(Default)]
struct LendState {
    done: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

/// Resolves once every clone of a lent value has been dropped: from then on
/// nothing reads the memory.
pub struct Returned(Arc<LendState>);

impl Returned {
    pub fn is_returned(&self) -> bool {
        self.0.done.load(Ordering::Acquire)
    }
}

impl Future for Returned {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.is_returned() {
            return Poll::Ready(());
        }
        *self.0.waker.lock().unwrap() = Some(cx.waker().clone());
        // Dropped between the check and storing the waker: it found none to wake.
        match self.is_returned() {
            true => Poll::Ready(()),
            false => Poll::Pending,
        }
    }
}

struct Lent {
    ptr: *const u8,
    len: usize,
    region: Option<Arc<Region>>,
    state: Arc<LendState>,
}

// SAFETY: `lend`'s contract keeps the memory readable from any thread until
// the last clone is dropped.
unsafe impl Send for Lent {}

impl AsRef<[u8]> for Lent {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: as above.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for Lent {
    fn drop(&mut self) {
        // Before the memory is returned: the last value may end the
        // registration, which must not outlive the memory.
        drop(self.region.take());
        self.state.done.store(true, Ordering::Release);
        if let Some(w) = self.state.waker.lock().unwrap().take() {
            w.wake();
        }
    }
}

/// `len` bytes at `ptr` as a value to send, with `region`'s memh when the
/// region holds all of them, else none.
///
/// # Safety
/// `[ptr, ptr + len)` must stay valid and unmodified until the returned
/// `Returned` resolves.
pub unsafe fn lend(ptr: *const u8, len: usize, region: Option<&Arc<Region>>) -> (RegBytes, Returned) {
    let ptr = if len == 0 { std::ptr::NonNull::<u8>::dangling().as_ptr() as *const u8 } else { ptr };
    let region = region.filter(|r| r.contains(ptr, len)).cloned();
    #[cfg(feature = "ucx")]
    let memh = region.as_ref().and_then(|r| r.reg.as_ref()).and_then(crate::regpool::Memh::of);
    let state = Arc::new(LendState::default());
    let bytes = bytes::Bytes::from_owner(Lent { ptr, len, region, state: state.clone() });
    #[cfg(feature = "ucx")]
    let value = RegBytes::from_parts(bytes, memh);
    #[cfg(not(feature = "ucx"))]
    let value = RegBytes::from(bytes);
    (value, Returned(state))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returned_resolves_once_every_clone_is_dropped() {
        let mem = vec![7u8; 64];
        let (value, returned) = unsafe { lend(mem.as_ptr(), mem.len(), None) };
        let part = value.slice(8..16);
        drop(value);
        assert!(!returned.is_returned(), "a slice still reads the memory");
        assert_eq!(&part[..], &[7u8; 8]);
        drop(part);
        assert!(returned.is_returned());
    }

    #[test]
    fn a_region_holds_only_what_lies_inside_it() {
        let mut mem = vec![0u8; 64];
        let region = unsafe { Region::register(mem.as_mut_ptr(), mem.len()) }.unwrap();
        assert!(region.contains(mem.as_ptr(), 64));
        assert!(region.contains(unsafe { mem.as_ptr().add(60) }, 4));
        assert!(!region.contains(unsafe { mem.as_ptr().add(60) }, 5));
        assert!(!region.contains(mem.as_ptr().wrapping_sub(1), 2));
    }
}
