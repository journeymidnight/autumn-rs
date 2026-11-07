//! Caller memory a receive writes into directly (no pool buffer, no copy out).
//!
//! A receive of a large value normally lands in a pool buffer the receiving
//! task owns and is then copied to where the caller wants it. `RecvDest` lets
//! it land at the caller's address instead. The hazard is the one that made
//! pooled receives the rule: the receive runs in a long-lived reader task, so
//! a caller that gives up (a timeout) must not get its memory back while the
//! kernel can still write into it. Ownership solves it: whoever holds the
//! `RecvDest` may write the memory, compio keeps an owned buffer until the
//! operation's completion lands even when the future is dropped, and the
//! caller's [`Released`] resolves only once the `RecvDest` is dropped.

use std::cell::Cell;
use std::future::Future;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

#[derive(Default)]
struct ReleaseState {
    done: Cell<bool>,
    waker: Cell<Option<Waker>>,
}

/// `cap` bytes at `ptr`, lent to one receive. Dropping it ends the loan.
pub struct RecvDest {
    ptr: *mut u8,
    cap: usize,
    filled: usize,
    state: Rc<ReleaseState>,
}

/// Resolves when the paired [`RecvDest`] has been dropped: from then on
/// nothing writes the memory.
pub struct Released(Rc<ReleaseState>);

impl RecvDest {
    /// # Safety
    /// `[ptr, ptr + cap)` must stay valid for writes, and must not be read or
    /// written by anyone else, until the returned [`Released`] resolves.
    pub unsafe fn new(ptr: *mut u8, cap: usize) -> (RecvDest, Released) {
        // Slices are built from `ptr` even when empty, and must not be null.
        let ptr = if cap == 0 { std::ptr::NonNull::dangling().as_ptr() } else { ptr };
        let state = Rc::new(ReleaseState::default());
        (RecvDest { ptr, cap, filled: 0, state: state.clone() }, Released(state))
    }

    pub fn cap(&self) -> usize {
        self.cap
    }

    /// The whole lent region.
    pub fn region_mut(&mut self) -> &mut [u8] {
        // SAFETY: `new`'s contract makes the region ours while `self` lives.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.cap) }
    }
}

impl Drop for RecvDest {
    fn drop(&mut self) {
        self.state.done.set(true);
        if let Some(w) = self.state.waker.take() {
            w.wake();
        }
    }
}

impl Released {
    pub fn is_released(&self) -> bool {
        self.0.done.get()
    }
}

impl Future for Released {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0.done.get() {
            return Poll::Ready(());
        }
        self.0.waker.set(Some(cx.waker().clone()));
        Poll::Pending
    }
}

impl compio::buf::IoBuf for RecvDest {
    fn as_init(&self) -> &[u8] {
        // SAFETY: as in `region_mut`; `filled <= cap`.
        unsafe { std::slice::from_raw_parts(self.ptr, self.filled) }
    }
}

impl compio::buf::SetLen for RecvDest {
    unsafe fn set_len(&mut self, len: usize) {
        self.filled = len.min(self.cap);
    }
}

impl compio::buf::IoBufMut for RecvDest {
    fn as_uninit(&mut self) -> &mut [MaybeUninit<u8>] {
        // SAFETY: as in `region_mut`; `u8` and `MaybeUninit<u8>` share layout.
        unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut MaybeUninit<u8>, self.cap) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn released_resolves_only_after_the_dest_is_dropped() {
        let mut mem = vec![0u8; 8];
        let (mut dest, released) = unsafe { RecvDest::new(mem.as_mut_ptr(), mem.len()) };
        dest.region_mut()[..3].copy_from_slice(b"abc");
        assert!(!released.is_released());
        drop(dest);
        assert!(released.is_released());
        assert_eq!(&mem[..3], b"abc");
    }
}
