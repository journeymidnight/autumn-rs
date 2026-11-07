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

/// Lend each `(ptr, len)` page, hand the values to `send`, and return its
/// result only once every page is returned — after a `send` that gave up (a
/// timeout) too, so no send still in flight reads a page its caller has
/// reused.
///
/// # Safety
/// Each page must stay mapped and unmodified until this returns, and the
/// future must run to completion: dropped early, a send may still read a page.
pub async unsafe fn with_lent_pages<T, Fut>(
    pages: impl IntoIterator<Item = (*const u8, usize)>,
    region: Option<&Arc<Region>>,
    send: impl FnOnce(Vec<RegBytes>) -> Fut,
) -> T
where
    Fut: Future<Output = T>,
{
    let (values, returned): (Vec<RegBytes>, Vec<Returned>) =
        pages.into_iter().map(|(ptr, len)| lend(ptr, len, region)).unzip();
    let out = send(values).await;
    futures::future::join_all(returned).await;
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A send that outlives the call (it timed out, the transport still holds
    /// the value) keeps the call from returning: the page is the caller's
    /// again only when nothing reads it.
    #[compio::test]
    async fn the_pages_are_back_before_the_call_returns() {
        let page = vec![3u8; 4096];
        let still_sending = Arc::new(AtomicBool::new(true));
        let flag = still_sending.clone();
        let out: Result<(), &str> = unsafe {
            with_lent_pages([(page.as_ptr(), page.len())], None, |values| async move {
                // The transport keeps the value past the caller's deadline.
                compio::runtime::spawn(async move {
                    compio::time::sleep(std::time::Duration::from_millis(200)).await;
                    flag.store(false, Ordering::SeqCst);
                    drop(values);
                })
                .detach();
                Err("timed out")
            })
        }
        .await;
        assert_eq!(out, Err("timed out"));
        assert!(!still_sending.load(Ordering::SeqCst), "returned while a send still held the page");
    }

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
