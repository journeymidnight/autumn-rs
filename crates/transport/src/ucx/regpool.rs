//! F216 — thread-local pool of UCX-registered buffers for zero-copy receive.
//!
//! `ucp_mem_map` is expensive (tens of µs), so we register a buffer ONCE and
//! reuse it across ops. Each `PooledBuf` owns a stable-address `Vec<u8>` plus a
//! `RegisteredMem` over it; on drop the buffer returns to a thread-local
//! free-list keyed by power-of-two size class (so non-uniform value sizes don't
//! fragment unboundedly). Recv-into-`PooledBuf` then passes `reg().memh()` to
//! `ucp_stream_recv_nbx` (`UCP_OP_ATTR_FIELD_MEMH`) for a true zero-copy
//! receive (RDMA into the registered dest, no bounce-buffer copy-out).
//!
//! ## Why thread-local
//! The PS runs one partition per OS thread and the EN one shard per OS thread
//! (single-threaded compio runtimes), so a thread-local free-list needs no
//! locking. The `RegisteredMem` handle is bound to the process-global
//! `ucp_context` and is `Send+Sync`, so a buffer registered on one thread is
//! technically usable from another — but we never share `PooledBuf` across
//! threads; each thread pools its own.
//!
//! ## Lifetime / cancel-safety (load-bearing)
//! UCX may DMA into a registered buffer until the recv completes. The
//! `RegisteredMem` MUST outlive that DMA. The rule: **the future that owns the
//! in-flight recv holds the `PooledBuf` for the whole `await`; the buffer only
//! returns to the free-list when the `PooledBuf` drops, which the caller does
//! after the op resolves.** `endpoint.rs::InflightSlot::drop` already drains
//! UCX synchronously on cancel before the recv borrow ends, so dropping a
//! `PooledBuf` right after the recv call returns is safe even on cancel.
//!
//! ## memlock
//! Registration pins pages (counts against `RLIMIT_MEMLOCK`). The per-thread
//! pool is capped (`REGPOOL_CAP_BYTES`); an `acquire` that would exceed the cap
//! returns an UNREGISTERED buffer (correctness preserved — recv falls back to
//! copy-out — just not zero-copy), logged once.

// Transport-agnostic pool (declared as a top-level `mod regpool` in lib.rs, so
// it compiles with AND without `ucx`). On `ucx` a fresh slab is registered with
// the NIC (ibv_reg_mr via `register_memory`); without `ucx`, `RegisteredMem` is
// the uninhabited stub so `reg` is always `None` (recv falls back to copy-out —
// the path autumn-rpc takes on TCP anyway). This keeps the "only the transport
// leaf is cfg-gated, everyone above is uniform" pattern: autumn-rpc/stream use
// `PooledBuf` with no `cfg`.
use crate::RegisteredMem;
use std::cell::RefCell;

/// Smallest size class. Values below this still round up to it.
const MIN_CLASS: usize = 4096;
/// Power-of-two exponent for `MIN_CLASS` — used as the bucket-index offset
/// so `class.trailing_zeros() - MIN_CLASS_SHIFT` is the bucket array index.
const MIN_CLASS_SHIFT: u32 = MIN_CLASS.trailing_zeros(); // = 12

/// Largest size class. Caller-requested `need` above this is rejected (the
/// pool returns an unregistered, in-place `Vec` of exactly `need` bytes, NOT
/// a power-of-two slab — registering >256 MiB through the pool is almost
/// certainly a frame-length corruption or DoS attempt, and `next_power_of_two`
/// would panic on `need > usize::MAX/2` anyway). 256 MiB matches autumn's
/// largest single value (8 MiB extent) × 32 — generous headroom.
const MAX_CLASS: usize = 256 * 1024 * 1024;
const MAX_CLASS_SHIFT: u32 = MAX_CLASS.trailing_zeros(); // = 28

/// One bucket per power-of-two size class from `MIN_CLASS` (2^12 = 4 KiB) to
/// `MAX_CLASS` (2^28 = 256 MiB), inclusive. Indexed by
/// `class.trailing_zeros() - MIN_CLASS_SHIFT`. Sized as a `const` so the
/// fixed-array allocation lives on the stack (or in TLS — see `POOL`) with
/// no heap indirection per acquire/drop hot-path lookup.
const NUM_CLASSES: usize = (MAX_CLASS_SHIFT - MIN_CLASS_SHIFT + 1) as usize; // = 17

/// Map a power-of-two `class` byte-count to its bucket index.
/// Panics in debug if `class` isn't a valid in-pool size; the public
/// callers (`acquire` / `PooledBuf::Drop` after the `class > MAX_CLASS`
/// guard) always pass a valid class.
#[inline]
fn class_index(class: usize) -> usize {
    debug_assert!(class >= MIN_CLASS && class <= MAX_CLASS && class.is_power_of_two());
    (class.trailing_zeros() - MIN_CLASS_SHIFT) as usize
}

/// Per-thread cap on total registered (pinned) bytes held by the pool's
/// free-list. Operators must keep `cap × threads < RLIMIT_MEMLOCK`. Default
/// 512 MiB; overridable once via [`set_regpool_cap_bytes`] from a CLI flag
/// (no env reads in production code).
static REGPOOL_CAP_CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

/// Set the per-thread registered-buffer-pool cap (first-call-wins). Clamp
/// `[16 MiB, 64 GiB]`. Wire from `--ucx-regpool-cap-bytes`.
pub fn set_regpool_cap_bytes(n: usize) -> bool {
    REGPOOL_CAP_CELL
        .set(n.clamp(16 * 1024 * 1024, 64 * 1024 * 1024 * 1024))
        .is_ok()
}

fn cap_bytes() -> usize {
    *REGPOOL_CAP_CELL.get_or_init(|| 512 * 1024 * 1024)
}

struct Slab {
    /// `Some` = registered (zero-copy capable); `None` = over-cap fallback.
    ///
    /// MUST be declared BEFORE `buf` so it drops first: `RegisteredMem::drop`
    /// runs `ucp_mem_unmap` on the address range backed by `buf`; if `buf`
    /// dropped first, UCX would unmap freed memory (the C-side rcache + verbs
    /// driver may keep that address live for a brief window after unmap, so
    /// the use-after-free is occasionally observable as a driver error or
    /// SIGSEGV during process shutdown / over-cap eviction). Rust drops in
    /// declaration order.
    reg: Option<RegisteredMem>,
    /// Capacity == the buffer's size class; address is stable for the slab's
    /// life (allocated zeroed, never re-grown), so `reg` stays valid.
    buf: Vec<u8>,
}

struct PoolState {
    /// Free slabs grouped by power-of-two size class. Bucket index =
    /// `class.trailing_zeros() - MIN_CLASS_SHIFT`, so 4 KiB → 0, 8 KiB → 1,
    /// …, 256 MiB → 16. Replaces the prior `HashMap<usize, Vec<Slab>>` —
    /// the hot path was `buckets.get_mut(&class)` (hash + bucket walk) and
    /// `entry(class).or_default()` (hash + maybe alloc) on every Drop;
    /// `[Vec<Slab>; 17]` is a single index, and the Vecs amortise their
    /// own allocations.
    buckets: [Vec<Slab>; NUM_CLASSES],
    /// Registered bytes sitting on the free-list (subset of `registered_bytes`).
    pooled_bytes: usize,
    /// Total registered (pinned) bytes for this thread — free-list AND
    /// in-flight (currently owned by some `PooledBuf`). This is the value
    /// `cap_bytes()` actually constrains. The previous accounting only
    /// summed free-list, so a pipelined worker holding N `PooledBuf`s of
    /// 8 MiB in-flight could pin far more than cap before any pressure was
    /// observed. Now `acquire` of a fresh slab checks this counter BEFORE
    /// `register_memory`; eviction (`PooledBuf::Drop` over-cap branch)
    /// decrements it after `RegisteredMem` is released. Always 0 on the
    /// non-ucx build (the type-erased `RegisteredMem` is uninhabited there),
    /// so flagged `dead_code` for that build only.
    #[allow(dead_code)]
    registered_bytes: usize,
    /// Read only on the `#[cfg(feature = "ucx")]` warn-once path (ucp_mem_map
    /// failure); the field + its init are always compiled, so the default
    /// (non-ucx) build sees it as never-read.
    #[allow(dead_code)]
    warned_over_cap: bool,
}

impl PoolState {
    fn new() -> Self {
        Self {
            // [const { Vec::new() }; N] needs the inline-const stabilised in
            // 1.79 — autumn-rs's MSRV is well past that. `Vec::new()` doesn't
            // allocate, so this is one stack-array init, no heap.
            buckets: [const { Vec::new() }; NUM_CLASSES],
            pooled_bytes: 0,
            registered_bytes: 0,
            warned_over_cap: false,
        }
    }
}

thread_local! {
    static POOL: RefCell<PoolState> = RefCell::new(PoolState::new());
}

/// Round `need` up to its size class (power of two, min `MIN_CLASS`).
/// Returns `None` if `need` exceeds `MAX_CLASS` (caller decides whether to
/// allocate an unregistered out-of-pool buffer or reject the request).
fn size_class(need: usize) -> Option<usize> {
    if need > MAX_CLASS {
        return None;
    }
    // `checked_next_power_of_two` covers any future change to MAX_CLASS that
    // accidentally pushes near `usize::MAX/2`.
    need.max(MIN_CLASS).checked_next_power_of_two()
}

/// A registered buffer borrowed from the thread-local pool. Returns to the
/// free-list on drop (if under cap). Hold it for the entire duration of any
/// in-flight recv that targets it (see module cancel-safety note).
pub struct PooledBuf {
    /// `Option` so `Drop` can move the slab back into the pool.
    slab: Option<Slab>,
    /// size class of the slab (its `buf.capacity()`).
    class: usize,
    /// logical length the caller asked for (`<= class`). This is the
    /// WRITABLE area cap returned by `dest_mut` / `dest_and_reg` /
    /// `IoBufMut::as_uninit` — the "I want a `used`-byte recv target".
    used: usize,
    /// Bytes actually initialized / filled by the most recent I/O.
    /// Defaults to `used` on acquire (the legacy assumption was "the recv
    /// will fully fill the requested area" — every current dest_and_reg
    /// caller's recv loop does exactly that, and breaks early on error
    /// before exposing `pb`). The UCX recv path (`endpoint.rs:recv_into`)
    /// and compio's owned-read path both call `SetLen::set_len(got)` with
    /// the actual recv count; that now narrows `filled_len` so `filled()`
    /// / `AsRef<[u8]>` / `IoBuf::as_init` only expose the valid portion
    /// of the slab — closing coco P2's "stale pool bytes leak on partial
    /// recv" concern without breaking the all-fill loop callers.
    filled_len: usize,
}

impl PooledBuf {
    /// Mutable view of the requested region, for recv-into-dest.
    pub fn dest_mut(&mut self) -> &mut [u8] {
        let used = self.used;
        &mut self.slab.as_mut().expect("slab present").buf[..used]
    }

    /// The registered region, or `None` if this buffer is the unregistered
    /// over-cap fallback. Pass `reg().memh()` to the recv for zero-copy.
    pub fn reg(&self) -> Option<&RegisteredMem> {
        self.slab.as_ref().expect("slab present").reg.as_ref()
    }

    /// Disjoint split-borrow of the dest slice + its registration, so a single
    /// `recv_into(buf, reg)` call can take both from one `PooledBuf` (the
    /// `dest_mut()` + `reg()` pair can't be held simultaneously). This is the
    /// shape every server recv-into-registered loop needs: the loop owns the
    /// `PooledBuf`, calls `recv_into(dest, reg)`, and — because the recv's
    /// `InflightSlot` drains UCX on drop while the `PooledBuf` is still owned
    /// here — a cancelled recv can never leave the NIC writing a freed/recycled
    /// registered buffer (cancel-safety, see module note).
    pub fn dest_and_reg(&mut self) -> (&mut [u8], Option<&RegisteredMem>) {
        let used = self.used;
        let Slab { buf, reg } = self.slab.as_mut().expect("slab present");
        (&mut buf[..used], reg.as_ref())
    }

    /// Read-only view of the filled region (for sending the value onward).
    /// Narrowed by `SetLen::set_len` if the most recent recv returned
    /// fewer bytes than the writable area; defaults to the requested
    /// length (`used`) before any I/O happens.
    pub fn filled(&self) -> &[u8] {
        let filled = self.filled_len;
        &self.slab.as_ref().expect("slab present").buf[..filled]
    }

    /// Filled-bytes count — Vec-like semantics (initialized data length).
    /// Defaults to the requested capacity (`used`); narrows to actual recv
    /// count after `SetLen::set_len`.
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
}

/// So a `PooledBuf` can back a `bytes::Bytes` via `Bytes::from_owner(pb)` — the
/// value aliases the pool buffer (no copy) and returns to the pool when the last
/// `Bytes` clone drops. Used by `resolve_value` (R4) to hand the EN-read value
/// onward zero-copy.
impl AsRef<[u8]> for PooledBuf {
    fn as_ref(&self) -> &[u8] {
        self.filled()
    }
}

// F216-E: make `PooledBuf` a first-class compio buffer so the EN read path can
// `read_exact_at(pb, off)` straight into the registered, zeroed-once, pooled
// slab — eliminating the per-op `vec![0u8; read_size]` (per-op malloc + an 8 MiB
// memset that the pread immediately overwrites) and letting the UCX send find
// the `ucp_mem_map` registration in the rcache (stable address = rcache hit).
//
// compio's `read_exact_at` reads exactly `buf_capacity()` (= `as_uninit().len()`)
// bytes and tracks progress with its own counter, so exposing the requested
// `used` bytes as the capacity makes it read exactly the value length. `set_len`
// is a no-op: the slab is fixed-size and always fully initialized (allocated
// zeroed), and `used` is fixed at acquire — the read loop never relies on
// `set_len` to size the next read.
impl compio::buf::IoBuf for PooledBuf {
    fn as_init(&self) -> &[u8] {
        self.filled()
    }
}

impl compio::buf::SetLen for PooledBuf {
    /// Mark `len` bytes of the writable area as initialized. compio calls
    /// this internally after `read_exact_at` / `read_at` fills the slab;
    /// the UCX recv path (`endpoint.rs::recv_into`) calls it explicitly
    /// with the recv'd `got` count. Clamps to `used` to guarantee
    /// `filled_len <= cap()` even if a buggy caller passes a wild value.
    /// (The contract is "I just read `len` bytes into `as_uninit`" — `len`
    /// SHOULD always be `<= as_uninit().len() == used`. The clamp is
    /// defensive belt-and-braces.)
    unsafe fn set_len(&mut self, len: usize) {
        self.filled_len = len.min(self.used);
    }
}

impl compio::buf::IoBufMut for PooledBuf {
    fn as_uninit(&mut self) -> &mut [std::mem::MaybeUninit<u8>] {
        let used = self.used;
        let buf = &mut self.slab.as_mut().expect("slab present").buf[..used];
        // SAFETY: `u8` and `MaybeUninit<u8>` share layout; these bytes are
        // already initialized (the slab is zeroed on first alloc), so viewing
        // them as MaybeUninit is sound. `read_at` writes them before read-back.
        unsafe {
            std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut std::mem::MaybeUninit<u8>, used)
        }
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        let Some(slab) = self.slab.take() else { return };
        // F219: on **non-ucx** builds every slab is unregistered (there's no NIC
        // to register against), so they MUST be re-pooled — otherwise TCP
        // recv-into-pooled (`read_value_into_pooled` / `drain_zc_writes`) does a
        // fresh `vec![0u8; class]` (malloc + zero the whole slab) and free on
        // EVERY op, which is strictly worse than the regular reused-BytesMut
        // path (measured: 8 MiB TCP write 3× slower before this fix).
        //
        // On **ucx** builds, an unregistered slab is the rare over-memlock-cap
        // fallback; keep freeing those so registration is retried once pressure
        // eases (the normal case there has `reg = Some`, which is pooled below).
        #[cfg(feature = "ucx")]
        if slab.reg.is_none() {
            return;
        }
        let class = self.class;
        // Out-of-pool path (need > MAX_CLASS, see `acquire`): class isn't a
        // size class, no bucket exists for it, never re-pool.
        if class > MAX_CLASS {
            return;
        }
        // `try_with`, not `with`: a `PooledBuf` that escaped into another TLS
        // or task-local can outlive the regpool's TLS destructor; `with` would
        // then panic, and a Drop panic during stack-unwind aborts the process.
        // On TLS-shutdown we just let the slab drop directly (the Slab field
        // order above keeps that FFI-safe: RegisteredMem unmaps before buf
        // frees).
        // Returns the slab back to the caller iff we're evicting it. The slab
        // is then dropped OUTSIDE `POOL.borrow_mut`, so a hypothetical future
        // hook in `RegisteredMem::drop` that touches POOL won't deadlock on
        // the RefCell. (`RegisteredMem::drop` today only calls
        // `ucp_mem_unmap` — the safety is purely defensive.)
        let to_drop = POOL
            .try_with(|p| {
                let mut p = p.borrow_mut();
                if p.pooled_bytes + class <= cap_bytes() {
                    p.pooled_bytes += class;
                    p.buckets[class_index(class)].push(slab);
                    None
                } else {
                    // Eviction: keep `registered_bytes` in lockstep with what's
                    // actually pinned. Non-ucx builds register nothing, so this
                    // is a no-op there.
                    #[cfg(feature = "ucx")]
                    if slab.reg.is_some() {
                        p.registered_bytes = p.registered_bytes.saturating_sub(class);
                    }
                    Some(slab)
                }
            })
            .ok()
            .flatten();
        drop(to_drop); // borrow_mut released; safe to run Slab::drop now.
    }
}

/// Acquire a registered buffer with usable length `need` from the thread-local
/// pool. Reuses a pooled slab of the right size class, or allocates+registers a
/// fresh one. If registering would exceed the per-thread memlock cap, returns
/// an unregistered buffer (recv falls back to copy-out — still correct).
///
/// For `need > MAX_CLASS` (256 MiB), returns an unregistered out-of-pool
/// buffer of EXACTLY `need` bytes (no rounding, no pooling) — keeps the API
/// total while making "absurdly large" allocations cheap-and-non-pinning
/// instead of OOM-aborting via `next_power_of_two` panic.
pub fn acquire(need: usize) -> PooledBuf {
    let Some(class) = size_class(need) else {
        // Out-of-pool: allocate exact size, never register, never pool.
        // Marked by `class > MAX_CLASS` so `Drop` skips the re-pool path
        // even on non-ucx builds (where `reg.is_none()` is normal).
        let buf = vec![0u8; need];
        return PooledBuf {
            slab: Some(Slab { reg: None, buf }),
            class: need,
            used: need,
            filled_len: need,
        };
    };
    let slab = POOL.with(|p| {
        let mut p = p.borrow_mut();
        let idx = class_index(class);
        if let Some(slab) = p.buckets[idx].pop() {
            p.pooled_bytes -= class;
            return Some(slab);
        }
        None
    });

    if let Some(slab) = slab {
        return PooledBuf {
            slab: Some(slab),
            class,
            used: need,
            filled_len: need,
        };
    }

    // Allocate a fresh slab. zeroed (avoids reading uninit before recv).
    #[cfg_attr(not(feature = "ucx"), allow(unused_mut))]
    let mut buf = vec![0u8; class];
    #[cfg(feature = "ucx")]
    let reg = {
        // PRE-FLIGHT cap check: would registering `class` more bytes push the
        // thread's total pinned mem above cap? If so, skip register_memory —
        // an over-budget `ucp_mem_map` would either fail (memlock) or succeed
        // and silently push the process past its memlock allowance. Either
        // way the right answer is "fall back to unregistered + copy-out".
        let cap_ok = POOL.with(|p| {
            let p = p.borrow();
            p.registered_bytes.saturating_add(class) <= cap_bytes()
        });
        if !cap_ok {
            POOL.with(|p| {
                let mut p = p.borrow_mut();
                if !p.warned_over_cap {
                    p.warned_over_cap = true;
                    tracing::warn!(
                        class,
                        registered = p.registered_bytes,
                        cap = cap_bytes(),
                        "regpool: total registered would exceed cap; falling \
                         back to unregistered buffers (copy-out, not zero-copy)"
                    );
                }
            });
            None
        } else {
            match crate::register_memory(buf.as_mut_ptr() as *mut std::os::raw::c_void, class) {
                Ok(r) => {
                    POOL.with(|p| {
                        let mut p = p.borrow_mut();
                        p.registered_bytes = p.registered_bytes.saturating_add(class);
                    });
                    Some(r)
                }
                Err(e) => {
                    POOL.with(|p| {
                        let mut p = p.borrow_mut();
                        if !p.warned_over_cap {
                            p.warned_over_cap = true;
                            tracing::warn!(
                                class, error = %e,
                                "regpool: ucp_mem_map failed (memlock?); falling \
                                 back to unregistered buffers (copy-out, not zero-copy)"
                            );
                        }
                    });
                    None
                }
            }
        }
    };
    // No-ucx: RegisteredMem is uninhabited, so `reg` is always None.
    #[cfg(not(feature = "ucx"))]
    let reg: Option<RegisteredMem> = None;
    PooledBuf {
        slab: Some(Slab { buf, reg }),
        class,
        used: need,
        filled_len: need,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn class_index_covers_all_buckets() {
        assert_eq!(class_index(MIN_CLASS), 0);
        assert_eq!(class_index(8 * 1024), 1);
        assert_eq!(class_index(1 << 20), 8); // 1 MiB
        assert_eq!(class_index(8 * 1024 * 1024), 11);
        assert_eq!(class_index(MAX_CLASS), NUM_CLASSES - 1);
        // NUM_CLASSES sized exactly = exponent range + 1, covers every
        // valid in-pool class. Bucket index never escapes the array.
        assert!(class_index(MAX_CLASS) < NUM_CLASSES);
    }

    #[test]
    fn size_class_rounds_to_pow2_min_4k() {
        assert_eq!(size_class(1), Some(4096));
        assert_eq!(size_class(4096), Some(4096));
        assert_eq!(size_class(4097), Some(8192));
        assert_eq!(size_class(256 * 1024), Some(256 * 1024));
        assert_eq!(size_class(256 * 1024 + 1), Some(512 * 1024));
        assert_eq!(size_class(MAX_CLASS), Some(MAX_CLASS));
        // Above MAX_CLASS: out-of-pool sentinel.
        assert_eq!(size_class(MAX_CLASS + 1), None);
        // Stress: previously panicked via `next_power_of_two` overflow.
        assert_eq!(size_class(usize::MAX), None);
    }

    /// The cap counter must track registered bytes across the
    /// `acquire → drop → acquire` cycle. On a no-ucx build this is trivially
    /// zero (nothing registers), but the accounting plumbing still has to
    /// not corrupt state on the eviction branch.
    #[test]
    fn eviction_does_not_panic_or_double_count() {
        // Push N slabs into the pool, then force evictions by exceeding cap.
        // Tiny cap forces every drop to take the eviction branch.
        // (set_regpool_cap_bytes is first-call-wins; this test runs in a clean
        // process for `cargo test`, so subsequent test ordering doesn't matter
        // here — but be paranoid: only the first set wins, so set early.)
        let _ = set_regpool_cap_bytes(16 * 1024 * 1024); // min clamp = 16 MiB
        for _ in 0..4 {
            let pb = acquire(64 * 1024);
            drop(pb); // first time: registered (under cap), goes to free-list
        }
        // Now allocate MANY 8 MiB slabs in-flight to push past cap. Even on
        // no-ucx build (where reg=None and registered_bytes stays 0) the
        // free-list cap path should evict cleanly.
        let mut held: Vec<PooledBuf> = (0..3).map(|_| acquire(8 * 1024 * 1024)).collect();
        // Drop them: eviction branch must NOT panic on the registered_bytes
        // decrement (saturating_sub guards against underflow).
        for pb in held.drain(..) {
            drop(pb);
        }
    }

    /// Partial-recv narrowing: after `set_len(n)`, `filled()` / `AsRef` /
    /// `IoBuf::as_init()` must only expose `[..n]`, never the stale
    /// pool bytes between `[n..used]`. This is what coco P2 #4 was about.
    #[test]
    fn set_len_narrows_filled_view() {
        use compio::buf::{IoBuf, SetLen};
        let mut pb = acquire(8000); // class=8192
        assert_eq!(pb.cap(), 8000, "cap = requested capacity");
        assert_eq!(pb.len(), 8000, "default filled_len = used (legacy contract)");
        // Simulate the UCX recv_into path receiving fewer bytes than asked.
        unsafe { pb.set_len(3000) };
        assert_eq!(pb.len(), 3000, "set_len narrows filled_len");
        assert_eq!(pb.filled().len(), 3000, "filled() narrows");
        assert_eq!(pb.as_init().len(), 3000, "IoBuf::as_init narrows");
        assert_eq!(pb.cap(), 8000, "cap unchanged by set_len");
        // dest_mut still exposes the FULL writable area (unchanged).
        assert_eq!(pb.dest_mut().len(), 8000, "dest_mut returns writable cap");
        // Defensive clamp: set_len(> used) is clamped, never out-of-bounds.
        unsafe { pb.set_len(usize::MAX) };
        assert_eq!(pb.len(), 8000, "set_len clamps to used");
    }

    /// Out-of-pool acquire: any `need > MAX_CLASS` returns an unregistered
    /// buffer of EXACTLY `need` bytes, and Drop does NOT re-pool it (which
    /// would corrupt the size-class accounting).
    #[test]
    fn acquire_above_max_class_is_out_of_pool() {
        let need = MAX_CLASS + 1;
        let pb = acquire(need);
        assert_eq!(pb.class, need, "class==need flags out-of-pool");
        assert_eq!(pb.len(), need);
        assert_eq!(pb.filled().len(), need);
        // Drop must NOT push a non-pow2 slab into a bucket — no panic, no
        // stale entries.
        drop(pb);
    }

    #[test]
    fn acquire_release_reuses_same_class() {
        // Needs a UCX context; only run when the test bind is configured.
        if std::env::var("AUTUMN_UCX_TEST_BIND").is_err() {
            return;
        }
        let p1 = acquire(200_000); // class 256K
        let cap1 = p1.class;
        let ptr1 = p1.filled().as_ptr();
        assert_eq!(cap1, 256 * 1024);
        assert!(p1.reg().is_some());
        drop(p1);
        // Re-acquire the same class → should reuse the pooled slab (same ptr).
        let p2 = acquire(250_000);
        assert_eq!(p2.class, 256 * 1024);
        assert_eq!(p2.filled().as_ptr(), ptr1, "expected slab reuse");
    }

    #[test]
    fn dest_mut_len_matches_need() {
        if std::env::var("AUTUMN_UCX_TEST_BIND").is_err() {
            return;
        }
        let mut p = acquire(1000);
        assert_eq!(p.dest_mut().len(), 1000);
        assert_eq!(p.class, 4096);
        assert_eq!(p.len(), 1000);
    }

    // F216-E: `read_exact_at` into a PooledBuf must read exactly `used` bytes,
    // NOT the full slab `class`. `used=5000` rounds to `class=8192`; a file of
    // 5000 bytes would EOF-error if the read targeted the whole 8192-slab.
    // Validates the IoBufMut impl (as_uninit exposes `used`) on the no-ucx
    // build too (regpool returns an unregistered slab there; trait impl is the
    // same). Runs without UCX — pure compio file I/O.
    #[test]
    fn read_exact_at_into_pooled_reads_used_not_class() {
        use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
        let n = 5000usize;
        let pattern: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
        let path =
            std::env::temp_dir().join(format!("autumn_regpool_pread_{}.bin", std::process::id()));
        let rt = compio::runtime::Runtime::new().expect("compio rt");
        rt.block_on(async {
            {
                let mut f = compio::fs::File::create(&path).await.expect("create");
                f.write_all_at(pattern.clone(), 0).await.0.expect("write");
                f.sync_all().await.expect("sync");
            }
            let f = compio::fs::File::open(&path).await.expect("open");
            let pb = acquire(n);
            assert_eq!(pb.class, 8192, "5000 rounds to 8192");
            let compio::BufResult(res, pb) = f.read_exact_at(pb, 0).await;
            res.expect("read_exact_at must fill exactly `used` bytes (not class)");
            assert_eq!(pb.len(), n);
            assert_eq!(
                pb.filled(),
                &pattern[..],
                "pooled slab must hold file bytes"
            );
            let _ = compio::fs::remove_file(&path).await;
        });
    }
}
