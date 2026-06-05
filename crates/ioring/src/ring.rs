//! SQ/CQ ring traversal primitives — atomic head/tail over a borrowed
//! byte slice.
//!
//! # Layout & roles
//!
//! Per [`RingHeader`], a ring memory region (typically `mmap`-backed
//! shared memory) contains four 64-byte aligned atomic cells followed
//! by the SQ entries array, then the CQ entries array, then the buffer
//! pool:
//!
//! ```text
//! [ header (64 B) ]
//! [ sq_tail (64 B aligned u64 atomic) ]
//! [ sq_head (64 B aligned u64 atomic) ]
//! [ cq_tail (64 B aligned u64 atomic) ]
//! [ cq_head (64 B aligned u64 atomic) ]
//! [ sq_array (sq_entries × SQE_SIZE) ]
//! [ cq_array (cq_entries × CQE_SIZE) ]
//! [ buf_pool (buf_pool_size) ]
//! ```
//!
//! Roles in steady state:
//!
//! - **Client = SQ producer**: writes new SQEs at `sq_tail`, then
//!   atomically increments `sq_tail` (Release).
//! - **Daemon = SQ consumer**: reads SQEs at `sq_head` until catching
//!   up with `sq_tail`, atomically increments `sq_head` (Release) when
//!   the slot has been processed.
//! - **Daemon = CQ producer**: writes completion CQEs at `cq_tail`,
//!   atomically increments `cq_tail` (Release).
//! - **Client = CQ consumer**: reads CQEs at `cq_head`, atomically
//!   increments `cq_head` (Release) when consumed.
//!
//! Head/tail indices are **monotonically increasing u64s** — they are
//! NOT modulo'd against entry count. Slot index = `idx & (entries-1)`.
//! This avoids the empty/full ambiguity of mod-N indices and is the
//! same pattern used by Linux io_uring.
//!
//! # Atomic ordering
//!
//! Producer: `Acquire`-load the consumer's index (to compute
//! "is there room?"), `Release`-store its own incremented index after
//! writing the entry. The producer's Release pairs with the consumer's
//! Acquire.
//!
//! Consumer: `Acquire`-load the producer's index, `Release`-store its
//! own incremented index after reading the entry. Same pattern in
//! reverse.
//!
//! This crate operates on a generic byte slice (`&[u8]` for read,
//! `&mut [u8]` for write) so it can be unit-tested in-process with a
//! plain `Vec<u8>`. The mmap-backed shared-memory case is identical at
//! the byte level; F180-B/C wrap an mmap region in this same API.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::cqe::{Cqe, CQE_SIZE};
use crate::header::RingHeader;
use crate::sqe::{Sqe, SQE_SIZE};

/// Capacity of the SQ ring. Always a power of 2; slot index =
/// `monotonic_idx & (capacity - 1)`.
fn sq_capacity(h: &RingHeader) -> u64 {
    h.sq_entries as u64
}

fn cq_capacity(h: &RingHeader) -> u64 {
    h.cq_entries as u64
}

/// Build a `&AtomicU64` view over an 8-byte aligned offset within a
/// shared region.
///
/// Safety: caller guarantees `region` is at least `offset + 8` bytes
/// and `offset` is 8-byte aligned. The shared-memory region is
/// expected to outlive the returned reference; in practice this is
/// always true because the SHM region is owned for the lifetime of
/// the ring.
fn atomic_at(region: &[u8], offset: u64) -> &AtomicU64 {
    debug_assert!(offset as usize + 8 <= region.len(), "offset past region");
    debug_assert!(
        (offset as usize).is_multiple_of(std::mem::align_of::<AtomicU64>()),
        "offset not 8-byte aligned"
    );
    let ptr = unsafe { region.as_ptr().add(offset as usize) } as *const AtomicU64;
    // SAFETY: alignment + bounds checked above; AtomicU64 has the same
    // memory layout as `u64`, so a region of zeroed bytes is a valid
    // initial state. No mutation through this reference except through
    // Atomic methods (which use raw pointer ops).
    unsafe { &*ptr }
}

/// Slot offset for a monotonic index.
fn sq_slot_offset(h: &RingHeader, idx: u64) -> u64 {
    h.sq_array_offset() + (idx & (sq_capacity(h) - 1)) * SQE_SIZE as u64
}

fn cq_slot_offset(h: &RingHeader, idx: u64) -> u64 {
    h.cq_array_offset() + (idx & (cq_capacity(h) - 1)) * CQE_SIZE as u64
}

/// Producer side of the SQ — writes SQEs.
///
/// Typical caller: the autumn-ioring client (F180-C). Tests in this
/// module exercise it via a plain Vec<u8> region.
pub struct SqProducer<'a> {
    region: &'a mut [u8],
    header: RingHeader,
}

impl<'a> SqProducer<'a> {
    /// Wrap a mutable region with a parsed header.
    pub fn new(region: &'a mut [u8], header: RingHeader) -> Self {
        Self { region, header }
    }

    /// Number of SQE slots currently free for the producer to write.
    pub fn free_slots(&self) -> u64 {
        let head = atomic_at(self.region, self.header.sq_head_offset()).load(Ordering::Acquire);
        let tail = atomic_at(self.region, self.header.sq_tail_offset()).load(Ordering::Relaxed);
        sq_capacity(&self.header) - (tail.wrapping_sub(head))
    }

    /// True if the SQ is full (no room to push).
    pub fn is_full(&self) -> bool {
        self.free_slots() == 0
    }

    /// Try to enqueue one SQE. Returns `Ok(())` on success, `Err(sqe)`
    /// echoing back the SQE if the ring is full.
    pub fn try_push(&mut self, sqe: Sqe) -> Result<(), Sqe> {
        let head = atomic_at(self.region, self.header.sq_head_offset()).load(Ordering::Acquire);
        let tail = atomic_at(self.region, self.header.sq_tail_offset()).load(Ordering::Relaxed);
        if tail.wrapping_sub(head) >= sq_capacity(&self.header) {
            return Err(sqe);
        }
        let off = sq_slot_offset(&self.header, tail) as usize;
        // SAFETY: off + SQE_SIZE bounded by sq_array bounds (which
        // were validated when the header was decoded).
        let slot: &mut [u8; SQE_SIZE] = (&mut self.region[off..off + SQE_SIZE])
            .try_into()
            .expect("slot fits");
        sqe.encode(slot);
        atomic_at(self.region, self.header.sq_tail_offset()).store(tail + 1, Ordering::Release);
        Ok(())
    }
}

/// Consumer side of the SQ — reads SQEs.
///
/// Typical caller: the autumn-fuse daemon's ring poller (F180-B).
pub struct SqConsumer<'a> {
    region: &'a [u8],
    header: RingHeader,
}

impl<'a> SqConsumer<'a> {
    /// Wrap a read-only region. The daemon-side ring is technically
    /// shared-mutable but the consumer only needs to READ SQEs and
    /// atomically advance `sq_head`; both fit `&[u8]` because Atomic
    /// ops bypass the borrow rules.
    pub fn new(region: &'a [u8], header: RingHeader) -> Self {
        Self { region, header }
    }

    /// Number of SQEs available for the consumer to pop.
    pub fn pending(&self) -> u64 {
        let head = atomic_at(self.region, self.header.sq_head_offset()).load(Ordering::Relaxed);
        let tail = atomic_at(self.region, self.header.sq_tail_offset()).load(Ordering::Acquire);
        tail.wrapping_sub(head)
    }

    /// Pop one SQE. Returns `None` if no entries are pending.
    pub fn try_pop(&self) -> Option<Sqe> {
        let head = atomic_at(self.region, self.header.sq_head_offset()).load(Ordering::Relaxed);
        let tail = atomic_at(self.region, self.header.sq_tail_offset()).load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        let off = sq_slot_offset(&self.header, head) as usize;
        let sqe = Sqe::decode(&self.region[off..off + SQE_SIZE]).expect("valid SQE");
        atomic_at(self.region, self.header.sq_head_offset()).store(head + 1, Ordering::Release);
        Some(sqe)
    }

    /// Pop up to `max` SQEs into `dst`. Returns the number popped.
    /// Useful for batched daemon dispatch.
    pub fn try_pop_batch(&self, dst: &mut Vec<Sqe>, max: usize) -> usize {
        let head_init =
            atomic_at(self.region, self.header.sq_head_offset()).load(Ordering::Relaxed);
        let tail = atomic_at(self.region, self.header.sq_tail_offset()).load(Ordering::Acquire);
        let avail = tail.wrapping_sub(head_init).min(max as u64);
        let mut head = head_init;
        for _ in 0..avail {
            let off = sq_slot_offset(&self.header, head) as usize;
            let sqe = Sqe::decode(&self.region[off..off + SQE_SIZE]).expect("valid SQE");
            dst.push(sqe);
            head += 1;
        }
        if avail > 0 {
            atomic_at(self.region, self.header.sq_head_offset()).store(head, Ordering::Release);
        }
        avail as usize
    }
}

/// Producer side of the CQ — daemon writes completions.
pub struct CqProducer<'a> {
    region: &'a mut [u8],
    header: RingHeader,
}

impl<'a> CqProducer<'a> {
    pub fn new(region: &'a mut [u8], header: RingHeader) -> Self {
        Self { region, header }
    }

    pub fn free_slots(&self) -> u64 {
        let head = atomic_at(self.region, self.header.cq_head_offset()).load(Ordering::Acquire);
        let tail = atomic_at(self.region, self.header.cq_tail_offset()).load(Ordering::Relaxed);
        cq_capacity(&self.header) - (tail.wrapping_sub(head))
    }

    pub fn is_full(&self) -> bool {
        self.free_slots() == 0
    }

    /// Try to enqueue one CQE. Returns `Err(cqe)` if the ring is full;
    /// daemon should drop or back-pressure (CQ overflow handling is a
    /// design decision deferred to F180-B).
    pub fn try_push(&mut self, cqe: Cqe) -> Result<(), Cqe> {
        let head = atomic_at(self.region, self.header.cq_head_offset()).load(Ordering::Acquire);
        let tail = atomic_at(self.region, self.header.cq_tail_offset()).load(Ordering::Relaxed);
        if tail.wrapping_sub(head) >= cq_capacity(&self.header) {
            return Err(cqe);
        }
        let off = cq_slot_offset(&self.header, tail) as usize;
        let slot: &mut [u8; CQE_SIZE] = (&mut self.region[off..off + CQE_SIZE])
            .try_into()
            .expect("slot fits");
        cqe.encode(slot);
        atomic_at(self.region, self.header.cq_tail_offset()).store(tail + 1, Ordering::Release);
        Ok(())
    }
}

/// Consumer side of the CQ — client reads completions.
pub struct CqConsumer<'a> {
    region: &'a [u8],
    header: RingHeader,
}

impl<'a> CqConsumer<'a> {
    pub fn new(region: &'a [u8], header: RingHeader) -> Self {
        Self { region, header }
    }

    pub fn pending(&self) -> u64 {
        let head = atomic_at(self.region, self.header.cq_head_offset()).load(Ordering::Relaxed);
        let tail = atomic_at(self.region, self.header.cq_tail_offset()).load(Ordering::Acquire);
        tail.wrapping_sub(head)
    }

    pub fn try_pop(&self) -> Option<Cqe> {
        let head = atomic_at(self.region, self.header.cq_head_offset()).load(Ordering::Relaxed);
        let tail = atomic_at(self.region, self.header.cq_tail_offset()).load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        let off = cq_slot_offset(&self.header, head) as usize;
        let cqe = Cqe::decode(&self.region[off..off + CQE_SIZE]).expect("valid CQE");
        atomic_at(self.region, self.header.cq_head_offset()).store(head + 1, Ordering::Release);
        Some(cqe)
    }

    pub fn try_pop_batch(&self, dst: &mut Vec<Cqe>, max: usize) -> usize {
        let head_init =
            atomic_at(self.region, self.header.cq_head_offset()).load(Ordering::Relaxed);
        let tail = atomic_at(self.region, self.header.cq_tail_offset()).load(Ordering::Acquire);
        let avail = tail.wrapping_sub(head_init).min(max as u64);
        let mut head = head_init;
        for _ in 0..avail {
            let off = cq_slot_offset(&self.header, head) as usize;
            let cqe = Cqe::decode(&self.region[off..off + CQE_SIZE]).expect("valid CQE");
            dst.push(cqe);
            head += 1;
        }
        if avail > 0 {
            atomic_at(self.region, self.header.cq_head_offset()).store(head, Ordering::Release);
        }
        avail as usize
    }
}

/// Allocate a zeroed test region sized to fit a default ring. Public
/// for downstream test crates (F180-B/C will reuse this for round-
/// trip tests against a daemon stub).
pub fn allocate_test_region(header: &RingHeader) -> Vec<u8> {
    vec![0u8; header.total_size() as usize]
}

#[cfg(test)]
mod tests {
    use super::*;
    // Imported here (not at module top) because these consts are only used by
    // the tests; keeping them in the non-test import set tripped clippy's
    // unused-import lint.
    use crate::header::{CACHE_LINE, HEADER_SIZE};
    use crate::opcode::Opcode;

    fn small_header() -> RingHeader {
        // Use small-but-valid SQ/CQ entries so wrap-around tests run fast.
        let mut h = RingHeader::new(0xface);
        h.sq_entries = 4;
        h.cq_entries = 4;
        h.buf_pool_size = 64 * 1024;
        h.buf_slot_size = 4096;
        // Recompute buf_pool_offset for the smaller layout.
        h.buf_pool_offset = h.cq_array_offset() + (h.cq_entries as u64) * (CQE_SIZE as u64);
        h
    }

    fn dummy_sqe(user_data: u64) -> Sqe {
        Sqe {
            opcode: Opcode::Read,
            ring_fd: 1,
            offset: 0,
            length: 4096,
            buf_offset: 0,
            user_data,
            lease_mode: crate::sqe::SQE_LEASE_MODE_UNSET,
        }
    }

    #[test]
    fn empty_ring_pops_none() {
        let h = small_header();
        let region = allocate_test_region(&h);
        let cons = SqConsumer::new(&region, h);
        assert_eq!(cons.pending(), 0);
        assert!(cons.try_pop().is_none());
    }

    #[test]
    fn push_then_pop_one_sqe() {
        let h = small_header();
        let mut region = allocate_test_region(&h);
        {
            let mut prod = SqProducer::new(&mut region, h);
            prod.try_push(dummy_sqe(42)).unwrap();
            assert_eq!(prod.free_slots(), sq_capacity(&h) - 1);
        }
        let cons = SqConsumer::new(&region, h);
        assert_eq!(cons.pending(), 1);
        let popped = cons.try_pop().unwrap();
        assert_eq!(popped.user_data, 42);
        assert_eq!(cons.pending(), 0);
    }

    #[test]
    fn full_ring_rejects_push() {
        let h = small_header();
        let mut region = allocate_test_region(&h);
        let mut prod = SqProducer::new(&mut region, h);
        for i in 0..h.sq_entries {
            prod.try_push(dummy_sqe(i as u64)).unwrap();
        }
        assert!(prod.is_full());
        let err = prod.try_push(dummy_sqe(999));
        assert!(err.is_err());
        assert_eq!(err.unwrap_err().user_data, 999);
    }

    #[test]
    fn wrap_around_after_many_push_pop_cycles() {
        let h = small_header();
        let mut region = allocate_test_region(&h);
        let cap = sq_capacity(&h);
        // Push & pop > capacity many times — should never lose data
        // and slot offsets must wrap correctly.
        for i in 0..(cap * 8) {
            {
                let mut prod = SqProducer::new(&mut region, h);
                prod.try_push(dummy_sqe(i)).unwrap();
            }
            {
                let cons = SqConsumer::new(&region, h);
                assert_eq!(cons.try_pop().unwrap().user_data, i);
            }
        }
    }

    #[test]
    fn batch_pop_returns_in_order() {
        let h = small_header();
        let mut region = allocate_test_region(&h);
        {
            let mut prod = SqProducer::new(&mut region, h);
            for i in 0..3 {
                prod.try_push(dummy_sqe(i + 100)).unwrap();
            }
        }
        let cons = SqConsumer::new(&region, h);
        let mut out = Vec::new();
        let n = cons.try_pop_batch(&mut out, 10);
        assert_eq!(n, 3);
        assert_eq!(
            out.iter().map(|s| s.user_data).collect::<Vec<_>>(),
            vec![100, 101, 102]
        );
        assert_eq!(cons.pending(), 0);
    }

    #[test]
    fn batch_pop_respects_max() {
        let h = small_header();
        let mut region = allocate_test_region(&h);
        {
            let mut prod = SqProducer::new(&mut region, h);
            for i in 0..h.sq_entries {
                prod.try_push(dummy_sqe(i as u64)).unwrap();
            }
        }
        let cons = SqConsumer::new(&region, h);
        let mut out = Vec::new();
        let n = cons.try_pop_batch(&mut out, 2);
        assert_eq!(n, 2);
        assert_eq!(out.len(), 2);
        assert_eq!(cons.pending(), (h.sq_entries - 2) as u64);
    }

    #[test]
    fn cq_round_trip() {
        let h = small_header();
        let mut region = allocate_test_region(&h);
        {
            let mut prod = CqProducer::new(&mut region, h);
            prod.try_push(Cqe::ok(7, 1024)).unwrap();
            prod.try_push(Cqe::err(8, 2)).unwrap();
        }
        let cons = CqConsumer::new(&region, h);
        let c1 = cons.try_pop().unwrap();
        assert_eq!(c1.user_data, 7);
        assert_eq!(c1.result, 1024);
        let c2 = cons.try_pop().unwrap();
        assert_eq!(c2.user_data, 8);
        assert_eq!(c2.result, -2);
        assert!(cons.try_pop().is_none());
    }

    #[test]
    fn cq_full_rejects_push() {
        let h = small_header();
        let mut region = allocate_test_region(&h);
        let mut prod = CqProducer::new(&mut region, h);
        for i in 0..h.cq_entries {
            prod.try_push(Cqe::ok(i as u64, 1)).unwrap();
        }
        assert!(prod.is_full());
        assert!(prod.try_push(Cqe::ok(999, 1)).is_err());
    }

    #[test]
    fn interleaved_sq_and_cq() {
        // Simulate one "round trip": producer pushes SQE, consumer
        // pops it, dispatcher pushes CQE, original producer (acting
        // as CQE consumer) pops it. Common pattern that the daemon
        // and client will follow at the wire level.
        let h = small_header();
        let mut region = allocate_test_region(&h);

        // Client: push SQE.
        {
            let mut sqp = SqProducer::new(&mut region, h);
            sqp.try_push(dummy_sqe(1234)).unwrap();
        }
        // Daemon: pop SQE.
        let user_data = {
            let cons = SqConsumer::new(&region, h);
            cons.try_pop().unwrap().user_data
        };
        // Daemon: push CQE.
        {
            let mut cqp = CqProducer::new(&mut region, h);
            cqp.try_push(Cqe::ok(user_data, 4096)).unwrap();
        }
        // Client: pop CQE.
        let cons = CqConsumer::new(&region, h);
        let cqe = cons.try_pop().unwrap();
        assert_eq!(cqe.user_data, 1234);
        assert_eq!(cqe.result, 4096);
    }

    #[test]
    fn fill_then_drain_multiple_times() {
        let h = small_header();
        let mut region = allocate_test_region(&h);
        for round in 0..5u64 {
            for i in 0..h.sq_entries {
                let mut prod = SqProducer::new(&mut region, h);
                prod.try_push(dummy_sqe(round * 100 + i as u64)).unwrap();
            }
            let cons = SqConsumer::new(&region, h);
            for i in 0..h.sq_entries {
                let s = cons.try_pop().unwrap();
                assert_eq!(s.user_data, round * 100 + i as u64);
            }
        }
    }

    /// Sanity: confirm the layout fits within the allocated region.
    #[test]
    fn layout_within_region_bounds() {
        let h = small_header();
        let region_size = h.total_size() as usize;
        // Last byte of the buffer pool:
        let last = h.buf_pool_offset + h.buf_pool_size;
        assert_eq!(last as usize, region_size);
        // Header + atomic cells fit before sq_array.
        assert!(h.sq_array_offset() >= HEADER_SIZE as u64 + 4 * CACHE_LINE as u64);
    }
}
