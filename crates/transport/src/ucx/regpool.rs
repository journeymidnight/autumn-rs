//! Process-wide pool of reusable buffers, carved out of large chunks.
//!
//! A chunk is one anonymous mapping, registered with the NIC once when the
//! process transport is UCX (`ucp_mem_map`, never unmapped while the process
//! lives) and never registered on TCP, where the pool only saves the allocation
//! and the zeroing. Inside a chunk an `offset_allocator::Allocator` (TLSF-like,
//! O(1), metadata outside the memory) hands out 4 KiB-granular ranges; each
//! buffer keeps its chunk alive (`Arc`), so its memh stays valid for as long as
//! any `Bytes` made from it (`PooledBuf::freeze`).
//!
//! Each thread claims one chunk and allocates from it; the chunk's lock is then
//! uncontended except for frees from other threads, which lock it directly (no
//! return queue to drain). A thread whose chunk has no room claims another one
//! nobody holds, or creates one while the pool is under its process cap
//! (`--ucx-regpool-cap-bytes`); at the cap it allocates from any chunk with
//! room, claimed or not. A thread that exits gives its claim up. Chunks are
//! never freed: in-flight peaks recur, and a dropped chunk would have to be
//! registered again.
//!
//! When nothing fits at the cap the buffer is a plain heap allocation (counted
//! as `over_cap_total`); so is a request larger than the largest chunk
//! (`out_of_pool_total`).
//!
//! ## Lifetime / cancel-safety (load-bearing)
//! UCX writes into the buffer until the recv completes, and a stream recv
//! cannot be cancelled. So a recv takes the `PooledBuf` by value (`recv_pooled`)
//! and hands it back with the result; a recv dropped mid-await keeps the buffer
//! on the transport's orphan list until UCX is done, then drops it here
//! (`endpoint.rs`).

use crate::RegisteredMem;
use offset_allocator::{Allocation, Allocator};
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// Allocation granule: page-sized, so every buffer is O_DIRECT-aligned.
const UNIT: usize = 4096;
/// Size of a chunk made for requests up to its size.
const CHUNK_BYTES: usize = 64 << 20;
/// Largest chunk; a larger request is an out-of-pool heap buffer.
const MAX_CHUNK_BYTES: usize = 256 << 20;

static ACQUIRE_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Acquires served from a chunk that already existed.
static HIT_TOTAL: AtomicU64 = AtomicU64::new(0);
static REGISTER_FAILED_TOTAL: AtomicU64 = AtomicU64::new(0);
static OVER_CAP_TOTAL: AtomicU64 = AtomicU64::new(0);
static OUT_OF_POOL_TOTAL: AtomicU64 = AtomicU64::new(0);
static WARNED_MAP: AtomicBool = AtomicBool::new(false);
#[cfg(feature = "ucx")]
static WARNED_REGISTER: AtomicBool = AtomicBool::new(false);
static WARNED_CAP: AtomicBool = AtomicBool::new(false);

/// Counter snapshot for logs and the perf client (process-wide).
#[derive(Debug, Clone, Copy, Default)]
pub struct RegpoolStats {
    pub acquire_total: u64,
    pub hit_total: u64,
    pub register_failed_total: u64,
    pub over_cap_total: u64,
    pub out_of_pool_total: u64,
    /// Bytes of all chunks.
    pub pool_bytes: u64,
    /// Bytes of the chunks registered with the NIC.
    pub registered_bytes: u64,
    /// Bytes handed out and not yet returned (4 KiB granular).
    pub in_use_bytes: u64,
}

impl RegpoolStats {
    /// Share of acquires served without making a chunk (0 if none yet).
    pub fn hit_rate(&self) -> f64 {
        if self.acquire_total == 0 {
            0.0
        } else {
            self.hit_total as f64 / self.acquire_total as f64
        }
    }
}

pub fn snapshot() -> RegpoolStats {
    let mut s = RegpoolStats {
        acquire_total: ACQUIRE_TOTAL.load(Ordering::Relaxed),
        hit_total: HIT_TOTAL.load(Ordering::Relaxed),
        register_failed_total: REGISTER_FAILED_TOTAL.load(Ordering::Relaxed),
        over_cap_total: OVER_CAP_TOTAL.load(Ordering::Relaxed),
        out_of_pool_total: OUT_OF_POOL_TOTAL.load(Ordering::Relaxed),
        ..Default::default()
    };
    for chunk in &pool().lock().unwrap().chunks {
        s.pool_bytes += chunk.len as u64;
        if matches!(chunk.reg.get(), Some(Some(_))) {
            s.registered_bytes += chunk.len as u64;
        }
        s.in_use_bytes += (chunk.used_units.load(Ordering::Relaxed) * UNIT) as u64;
    }
    s
}

static CAP_CELL: OnceLock<usize> = OnceLock::new();

/// Set the process cap on chunk bytes (first call wins), clamped to
/// [one chunk, 64 GiB]. From `--ucx-regpool-cap-bytes`.
pub fn set_regpool_cap_bytes(n: usize) -> bool {
    CAP_CELL.set(n.clamp(CHUNK_BYTES, 64 << 30)).is_ok()
}

fn cap_bytes() -> usize {
    *CAP_CELL.get_or_init(|| 16 << 30)
}

struct Chunk {
    /// Before `mem`: unregistered before it is unmapped. Set by the first
    /// `reg()` call once the process transport is UCX (a chunk can predate
    /// `init_with`); `None` inside when registration failed.
    reg: OnceLock<Option<RegisteredMem>>,
    mem: Mapping,
    len: usize,
    alloc: Mutex<Allocator>,
    used_units: AtomicUsize,
    /// A thread allocates from it (`CLAIM`).
    claimed: AtomicBool,
}

impl Chunk {
    fn new(len: usize) -> Option<Self> {
        let mem = Mapping::new(len)?;
        let units = (len / UNIT) as u32;
        // A free region between every two allocations: at most 2n + 1 nodes.
        let nodes = (2 * units + 2).min(u32::MAX - 2);
        let chunk = Self {
            reg: OnceLock::new(),
            mem,
            len,
            alloc: Mutex::new(Allocator::with_max_allocs(units, nodes)),
            used_units: AtomicUsize::new(0),
            claimed: AtomicBool::new(false),
        };
        chunk.reg();
        Some(chunk)
    }

    /// The registration, made on first use under UCX.
    fn reg(&self) -> Option<&RegisteredMem> {
        if let Some(r) = self.reg.get() {
            return r.as_ref();
        }
        #[cfg(feature = "ucx")]
        if crate::runtime_transport_is_ucx() {
            return self
                .reg
                .get_or_init(|| match crate::register_memory(self.mem.ptr.cast(), self.len) {
                    Ok(r) => Some(r),
                    Err(e) => {
                        REGISTER_FAILED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        warn_once(
                            &WARNED_REGISTER,
                            format_args!("regpool: ucp_mem_map of a {}-byte chunk failed ({e}); its buffers are unregistered", self.len),
                        );
                        None
                    }
                })
                .as_ref();
        }
        None
    }

    fn allocate(&self, units: u32) -> Option<Allocation> {
        let a = self.alloc.lock().unwrap().allocate(units)?;
        self.used_units.fetch_add(units as usize, Ordering::Relaxed);
        Some(a)
    }

    fn free(&self, a: Allocation, units: u32) {
        self.alloc.lock().unwrap().free(a);
        self.used_units.fetch_sub(units as usize, Ordering::Relaxed);
    }
}

/// An anonymous private mapping; the kernel hands its pages out zeroed.
struct Mapping {
    ptr: *mut u8,
    len: usize,
}

// SAFETY: plain memory; who may touch which range is the allocator's business.
unsafe impl Send for Mapping {}
unsafe impl Sync for Mapping {}

impl Mapping {
    fn new(len: usize) -> Option<Self> {
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            warn_once(&WARNED_MAP, format_args!("regpool: mmap of a {len}-byte chunk failed: {}", std::io::Error::last_os_error()));
            return None;
        }
        // Fewer TLB and NIC translation entries; a hint, its failure is harmless.
        unsafe { libc::madvise(ptr, len, libc::MADV_HUGEPAGE) };
        Some(Self { ptr: ptr.cast(), len })
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        unsafe { libc::munmap(self.ptr.cast(), self.len) };
    }
}

fn warn_once(flag: &AtomicBool, msg: std::fmt::Arguments<'_>) {
    if !flag.swap(true, Ordering::Relaxed) {
        tracing::warn!("{msg}");
    }
}

struct Pool {
    chunks: Vec<Arc<Chunk>>,
    bytes: usize,
}

fn pool() -> &'static Mutex<Pool> {
    static POOL: OnceLock<Mutex<Pool>> = OnceLock::new();
    POOL.get_or_init(|| Mutex::new(Pool { chunks: Vec::new(), bytes: 0 }))
}

/// This thread's chunk; dropping it (switching chunks, thread exit) gives the
/// claim up.
struct Claim(Arc<Chunk>);

impl Drop for Claim {
    fn drop(&mut self) {
        self.0.claimed.store(false, Ordering::Relaxed);
    }
}

thread_local! {
    static CLAIM: RefCell<Option<Claim>> = const { RefCell::new(None) };
}

fn claim(chunk: &Arc<Chunk>) {
    chunk.claimed.store(true, Ordering::Relaxed);
    let new = Claim(chunk.clone());
    // During thread teardown there is nothing to claim for.
    if let Ok(old) = CLAIM.try_with(|c| c.borrow_mut().replace(new)) {
        drop(old);
    }
}

/// A buffer from the pool. Returns its range to its chunk on drop, on whatever
/// thread that happens. Hold it for the entire duration of any in-flight recv
/// that targets it (see module cancel-safety note).
pub struct PooledBuf {
    storage: Storage,
    /// Length the caller asked for: the writable area (`dest_mut`,
    /// `dest_and_reg`, `IoBufMut::as_uninit`).
    used: usize,
    /// Bytes filled by the most recent I/O; `used` until `SetLen::set_len`
    /// narrows it, so `filled()` / `AsRef` / `IoBuf::as_init` never expose
    /// stale pool bytes past a short recv.
    filled_len: usize,
}

enum Storage {
    Pooled { chunk: Arc<Chunk>, alloc: Allocation, units: u32, ptr: *mut u8 },
    /// Out of pool (larger than any chunk) or over the cap.
    Heap(Vec<u8>),
}

// SAFETY: the range `ptr` points at belongs to this buffer alone until its
// drop returns it to the chunk.
unsafe impl Send for PooledBuf {}
unsafe impl Sync for PooledBuf {}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        if let Storage::Pooled { chunk, alloc, units, .. } = &self.storage {
            chunk.free(*alloc, *units);
        }
    }
}

impl PooledBuf {
    fn base(&self) -> *const u8 {
        match &self.storage {
            Storage::Pooled { ptr, .. } => *ptr,
            Storage::Heap(v) => v.as_ptr(),
        }
    }

    fn base_mut(&mut self) -> *mut u8 {
        match &mut self.storage {
            Storage::Pooled { ptr, .. } => *ptr,
            Storage::Heap(v) => v.as_mut_ptr(),
        }
    }

    /// Mutable view of the requested region, for recv-into-dest.
    pub fn dest_mut(&mut self) -> &mut [u8] {
        let used = self.used;
        // SAFETY: `used` bytes at the base are this buffer's and initialized
        // (mapped zeroed, or written before).
        unsafe { std::slice::from_raw_parts_mut(self.base_mut(), used) }
    }

    /// The registration covering this buffer, or `None` (TCP, heap buffer,
    /// failed registration). Pass `reg().memh()` to the recv for zero-copy.
    pub fn reg(&self) -> Option<&RegisteredMem> {
        match &self.storage {
            Storage::Pooled { chunk, .. } => chunk.reg(),
            Storage::Heap(_) => None,
        }
    }

    /// The dest slice and its registration at once (`dest_mut()` and `reg()`
    /// cannot be held together).
    pub fn dest_and_reg(&mut self) -> (&mut [u8], Option<&RegisteredMem>) {
        let used = self.used;
        let (ptr, reg) = match &mut self.storage {
            Storage::Pooled { chunk, ptr, .. } => (*ptr, chunk.reg()),
            Storage::Heap(v) => (v.as_mut_ptr(), None),
        };
        // SAFETY: as in `dest_mut`; `reg` borrows the chunk, not the bytes.
        (unsafe { std::slice::from_raw_parts_mut(ptr, used) }, reg)
    }

    /// The filled region (for sending the value onward).
    pub fn filled(&self) -> &[u8] {
        // SAFETY: `filled_len <= used`, initialized as in `dest_mut`.
        unsafe { std::slice::from_raw_parts(self.base(), self.filled_len) }
    }

    /// Filled length; `used` until `SetLen::set_len` narrows it.
    pub fn len(&self) -> usize {
        self.filled_len
    }

    pub fn is_empty(&self) -> bool {
        self.filled_len == 0
    }

    /// Writable-area cap — the `used` value passed to `acquire`. Used by
    /// callers (`crate::lib::read_exact_into_pooled`'s `debug_assert`) to
    /// bound a target offset inside the buffer.
    pub fn cap(&self) -> usize {
        self.used
    }

    /// The filled bytes as a value that keeps this buffer's registration, so a
    /// UCX send passes the memh instead of looking the memory up.
    pub fn freeze(self) -> RegBytes {
        #[cfg(feature = "ucx")]
        let memh = self.reg().map(|r| Memh(r.memh()));
        RegBytes {
            bytes: bytes::Bytes::from_owner(self),
            #[cfg(feature = "ucx")]
            memh,
        }
    }
}

/// A chunk's registration handle. The `Bytes` beside it owns a buffer of the
/// chunk, which keeps the registration mapped for as long as the handle is held.
#[cfg(feature = "ucx")]
#[derive(Clone, Copy)]
pub(crate) struct Memh(pub(crate) crate::ucx::ffi::ucp_mem_h);

// SAFETY: a memh belongs to the process-global UCX context, not a thread.
#[cfg(feature = "ucx")]
unsafe impl Send for Memh {}
#[cfg(feature = "ucx")]
unsafe impl Sync for Memh {}

/// Bytes to send, with the registration they live in when they come from a
/// registered pool buffer ([`PooledBuf::freeze`]). Other bytes carry none, and
/// so does a buffer the pool did not register (TCP runtime, over-cap or
/// out-of-pool buffers): see `is_registered`.
#[derive(Clone)]
pub struct RegBytes {
    bytes: bytes::Bytes,
    #[cfg(feature = "ucx")]
    memh: Option<Memh>,
}

impl RegBytes {
    pub fn bytes(&self) -> &bytes::Bytes {
        &self.bytes
    }

    pub fn into_bytes(self) -> bytes::Bytes {
        self.bytes
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn is_registered(&self) -> bool {
        #[cfg(feature = "ucx")]
        return self.memh.is_some();
        #[cfg(not(feature = "ucx"))]
        false
    }

    #[cfg(feature = "ucx")]
    pub(crate) fn memh(&self) -> Option<Memh> {
        self.memh
    }
}

impl std::fmt::Debug for RegBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegBytes").field("len", &self.len()).field("registered", &self.is_registered()).finish()
    }
}

impl From<bytes::Bytes> for RegBytes {
    fn from(bytes: bytes::Bytes) -> Self {
        Self {
            bytes,
            #[cfg(feature = "ucx")]
            memh: None,
        }
    }
}

/// So a `PooledBuf` can back a `bytes::Bytes` via `Bytes::from_owner(pb)` — the
/// value aliases the buffer (no copy), which returns to the pool when the last
/// `Bytes` clone drops.
impl AsRef<[u8]> for PooledBuf {
    fn as_ref(&self) -> &[u8] {
        self.filled()
    }
}

// A first-class compio buffer, so the EN read path can `read_exact_at(pb, off)`
// straight into pooled memory — no per-op `vec![0u8; n]` (malloc + memset the
// pread overwrites anyway); `freeze` then sends it with the chunk's memh.
//
// compio's `read_exact_at` reads exactly `buf_capacity()` (= `as_uninit().len()`)
// bytes and tracks progress with its own counter, so exposing the requested
// `used` bytes as the capacity makes it read exactly the value length.
impl compio::buf::IoBuf for PooledBuf {
    fn as_init(&self) -> &[u8] {
        self.filled()
    }
}

impl compio::buf::SetLen for PooledBuf {
    /// Mark `len` bytes of the writable area as filled, clamped to `used`.
    unsafe fn set_len(&mut self, len: usize) {
        self.filled_len = len.min(self.used);
    }
}

impl compio::buf::IoBufMut for PooledBuf {
    fn as_uninit(&mut self) -> &mut [std::mem::MaybeUninit<u8>] {
        let used = self.used;
        // SAFETY: `u8` and `MaybeUninit<u8>` share layout; the range is this
        // buffer's (see `dest_mut`).
        unsafe { std::slice::from_raw_parts_mut(self.base_mut().cast(), used) }
    }
}

fn heap(need: usize) -> PooledBuf {
    PooledBuf { storage: Storage::Heap(vec![0u8; need]), used: need, filled_len: need }
}

fn pooled(chunk: &Arc<Chunk>, alloc: Allocation, units: u32, need: usize) -> PooledBuf {
    // SAFETY: the allocation lies inside the chunk's mapping.
    let ptr = unsafe { chunk.mem.ptr.add(alloc.offset as usize * UNIT) };
    PooledBuf {
        storage: Storage::Pooled { chunk: chunk.clone(), alloc, units, ptr },
        used: need,
        filled_len: need,
    }
}

/// A buffer of `need` writable bytes: from this thread's chunk when it has
/// room, else from another chunk (see module doc), else from the heap.
pub fn acquire(need: usize) -> PooledBuf {
    // A finished orphan recv (`endpoint.rs`) hands its buffer back here first.
    #[cfg(feature = "ucx")]
    crate::ucx::endpoint::reap_orphans();
    ACQUIRE_TOTAL.fetch_add(1, Ordering::Relaxed);
    if need > MAX_CHUNK_BYTES {
        OUT_OF_POOL_TOTAL.fetch_add(1, Ordering::Relaxed);
        return heap(need);
    }
    let units = need.div_ceil(UNIT).max(1) as u32;
    let current = CLAIM
        .try_with(|c| {
            let c = c.borrow();
            let chunk = &c.as_ref()?.0;
            chunk.allocate(units).map(|a| pooled(chunk, a, units, need))
        })
        .ok()
        .flatten();
    if let Some(pb) = current {
        HIT_TOTAL.fetch_add(1, Ordering::Relaxed);
        return pb;
    }
    acquire_slow(need, units)
}

fn acquire_slow(need: usize, units: u32) -> PooledBuf {
    let mut pool = pool().lock().unwrap();
    for chunk in &pool.chunks {
        if chunk.claimed.load(Ordering::Relaxed) {
            continue;
        }
        if let Some(a) = chunk.allocate(units) {
            claim(chunk);
            HIT_TOTAL.fetch_add(1, Ordering::Relaxed);
            return pooled(chunk, a, units, need);
        }
    }
    let size = if need <= CHUNK_BYTES { CHUNK_BYTES } else { need.next_power_of_two() };
    if pool.bytes + size <= cap_bytes() {
        // Reserve, then map and register without the lock: other threads'
        // slow paths must not wait for a registration.
        pool.bytes += size;
        drop(pool);
        let made = Chunk::new(size).map(Arc::new);
        pool = self::pool().lock().unwrap();
        match made {
            Some(chunk) => {
                pool.chunks.push(chunk.clone());
                let a = chunk.allocate(units).expect("a new chunk fits the request it was made for");
                claim(&chunk);
                return pooled(&chunk, a, units, need);
            }
            None => {
                pool.bytes -= size;
                // Not the cap: the mapping failed (warned in `Mapping::new`).
                for chunk in &pool.chunks {
                    if let Some(a) = chunk.allocate(units) {
                        HIT_TOTAL.fetch_add(1, Ordering::Relaxed);
                        return pooled(chunk, a, units, need);
                    }
                }
                return heap(need);
            }
        }
    }
    // At the cap: share any chunk with room.
    for chunk in &pool.chunks {
        if let Some(a) = chunk.allocate(units) {
            HIT_TOTAL.fetch_add(1, Ordering::Relaxed);
            return pooled(chunk, a, units, need);
        }
    }
    drop(pool);
    OVER_CAP_TOTAL.fetch_add(1, Ordering::Relaxed);
    warn_once(&WARNED_CAP, format_args!(
        "regpool: no chunk has room for {need} bytes at the {}-byte cap; falling back to unregistered heap buffers",
        cap_bytes()
    ));
    heap(need)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk_of(pb: &PooledBuf) -> Option<*const Chunk> {
        match &pb.storage {
            Storage::Pooled { chunk, .. } => Some(Arc::as_ptr(chunk)),
            Storage::Heap(_) => None,
        }
    }

    #[test]
    fn a_freed_range_is_reused_and_counts_as_a_hit() {
        std::thread::spawn(|| {
            let mut first = acquire(200_000);
            first.dest_mut().fill(0xa5);
            let addr = first.filled().as_ptr();
            drop(first);
            let before = snapshot().hit_total;
            let next = acquire(250_000);
            assert_eq!(next.filled().as_ptr(), addr, "the freed range comes back");
            assert!(snapshot().hit_total > before);
        })
        .join()
        .unwrap();
    }

    #[test]
    fn a_buffer_dropped_on_another_thread_returns_to_its_chunk() {
        std::thread::spawn(|| {
            let pb = acquire(1 << 20);
            let chunk = chunk_of(&pb).unwrap();
            let addr = pb.filled().as_ptr() as usize;
            std::thread::spawn(move || drop(pb)).join().unwrap();
            let again = acquire(1 << 20);
            assert_eq!(chunk_of(&again), Some(chunk));
            assert_eq!(again.filled().as_ptr() as usize, addr, "freed by the other thread, reused here");
        })
        .join()
        .unwrap();
    }

    #[test]
    fn a_thread_that_exits_lets_another_claim_its_chunk() {
        let chunk = std::thread::spawn(|| {
            let pb = acquire(4096);
            chunk_of(&pb).unwrap() as usize
        })
        .join()
        .unwrap();
        // Unclaimed now: some thread can take it over (other tests run beside
        // this one, so only check that it is not held by the exited thread).
        let pool = pool().lock().unwrap();
        let c = pool.chunks.iter().find(|c| Arc::as_ptr(c) as usize == chunk).unwrap();
        assert!(!c.claimed.load(Ordering::Relaxed) || Arc::strong_count(c) > 1);
    }

    #[test]
    fn a_request_larger_than_a_chunk_gets_its_own_and_one_larger_than_any_goes_to_the_heap() {
        std::thread::spawn(|| {
            let big = acquire(CHUNK_BYTES + 1);
            let c = chunk_of(&big).expect("pooled");
            assert_eq!(unsafe { (*c).len }, (CHUNK_BYTES + 1).next_power_of_two());
            let before = snapshot().out_of_pool_total;
            let huge = acquire(MAX_CHUNK_BYTES + 1);
            assert!(chunk_of(&huge).is_none());
            assert_eq!(huge.cap(), MAX_CHUNK_BYTES + 1);
            assert!(snapshot().out_of_pool_total > before);
        })
        .join()
        .unwrap();
    }

    #[test]
    fn buffers_are_page_aligned_and_do_not_overlap() {
        std::thread::spawn(|| {
            let bufs: Vec<_> = [1usize, 4096, 4097, 100_000, 1 << 20].iter().map(|&n| acquire(n)).collect();
            let mut ranges: Vec<_> = bufs.iter().map(|b| (b.filled().as_ptr() as usize, b.cap())).collect();
            assert!(ranges.iter().all(|(p, _)| p % UNIT == 0));
            ranges.sort();
            for w in ranges.windows(2) {
                assert!(w[0].0 + w[0].1 <= w[1].0, "{w:?}");
            }
        })
        .join()
        .unwrap();
    }

    /// Partial-recv narrowing: after `set_len(n)`, `filled()` / `AsRef` /
    /// `IoBuf::as_init()` must only expose `[..n]`, never the stale
    /// pool bytes between `[n..used]`.
    #[test]
    fn set_len_narrows_filled_view() {
        use compio::buf::{IoBuf, SetLen};
        let mut pb = acquire(8000);
        assert_eq!(pb.cap(), 8000, "cap = requested capacity");
        assert_eq!(pb.len(), 8000, "default filled_len = used (legacy contract)");
        unsafe { pb.set_len(3000) };
        assert_eq!(pb.len(), 3000, "set_len narrows filled_len");
        assert_eq!(pb.filled().len(), 3000, "filled() narrows");
        assert_eq!(pb.as_init().len(), 3000, "IoBuf::as_init narrows");
        assert_eq!(pb.cap(), 8000, "cap unchanged by set_len");
        assert_eq!(pb.dest_mut().len(), 8000, "dest_mut returns writable cap");
        unsafe { pb.set_len(usize::MAX) };
        assert_eq!(pb.len(), 8000, "set_len clamps to used");
    }

    // `read_exact_at` into a PooledBuf must read exactly `used` bytes, not the
    // 4 KiB-rounded range: a 5000-byte file would hit EOF otherwise.
    #[test]
    fn read_exact_at_into_pooled_reads_used_not_the_rounded_range() {
        use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
        let n = 5000usize;
        let pattern: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
        let path = std::env::temp_dir().join(format!("autumn_regpool_pread_{}.bin", std::process::id()));
        let rt = compio::runtime::Runtime::new().expect("compio rt");
        rt.block_on(async {
            {
                let mut f = compio::fs::File::create(&path).await.expect("create");
                f.write_all_at(pattern.clone(), 0).await.0.expect("write");
                f.sync_all().await.expect("sync");
            }
            let f = compio::fs::File::open(&path).await.expect("open");
            let pb = acquire(n);
            let compio::BufResult(res, pb) = f.read_exact_at(pb, 0).await;
            res.expect("read_exact_at must fill exactly `used` bytes");
            assert_eq!(pb.len(), n);
            assert_eq!(pb.filled(), &pattern[..]);
            let _ = compio::fs::remove_file(&path).await;
        });
    }
}
