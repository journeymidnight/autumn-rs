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

use crate::ucx::worker::{register_memory, RegisteredMem};
use std::cell::RefCell;
use std::collections::HashMap;
use std::os::raw::c_void;

/// Smallest size class. Values below this still round up to it.
const MIN_CLASS: usize = 4096;

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
    /// Capacity == the buffer's size class; address is stable for the slab's
    /// life (allocated zeroed, never re-grown), so `reg` stays valid.
    buf: Vec<u8>,
    /// `Some` = registered (zero-copy capable); `None` = over-cap fallback.
    reg: Option<RegisteredMem>,
}

struct PoolState {
    /// size class (bytes) -> free slabs of exactly that capacity.
    buckets: HashMap<usize, Vec<Slab>>,
    /// total registered bytes currently held in `buckets` (free-list only).
    pooled_bytes: usize,
    warned_over_cap: bool,
}

impl PoolState {
    fn new() -> Self {
        Self { buckets: HashMap::new(), pooled_bytes: 0, warned_over_cap: false }
    }
}

thread_local! {
    static POOL: RefCell<PoolState> = RefCell::new(PoolState::new());
}

/// Round `need` up to its size class (power of two, min `MIN_CLASS`).
fn size_class(need: usize) -> usize {
    need.max(MIN_CLASS).next_power_of_two()
}

/// A registered buffer borrowed from the thread-local pool. Returns to the
/// free-list on drop (if under cap). Hold it for the entire duration of any
/// in-flight recv that targets it (see module cancel-safety note).
pub struct PooledBuf {
    /// `Option` so `Drop` can move the slab back into the pool.
    slab: Option<Slab>,
    /// size class of the slab (its `buf.capacity()`).
    class: usize,
    /// logical length the caller asked for (`<= class`).
    used: usize,
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
    pub fn filled(&self) -> &[u8] {
        let used = self.used;
        &self.slab.as_ref().expect("slab present").buf[..used]
    }

    pub fn len(&self) -> usize {
        self.used
    }

    pub fn is_empty(&self) -> bool {
        self.used == 0
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        let Some(slab) = self.slab.take() else { return };
        // Only re-pool registered slabs; unregistered fallbacks are freed.
        if slab.reg.is_none() {
            return;
        }
        let class = self.class;
        POOL.with(|p| {
            let mut p = p.borrow_mut();
            if p.pooled_bytes + class <= cap_bytes() {
                p.pooled_bytes += class;
                p.buckets.entry(class).or_default().push(slab);
            }
            // else: over cap → drop the slab (RegisteredMem::drop unmaps).
        });
    }
}

/// Acquire a registered buffer with usable length `need` from the thread-local
/// pool. Reuses a pooled slab of the right size class, or allocates+registers a
/// fresh one. If registering would exceed the per-thread memlock cap, returns
/// an unregistered buffer (recv falls back to copy-out — still correct).
pub fn acquire(need: usize) -> PooledBuf {
    let class = size_class(need);
    let slab = POOL.with(|p| {
        let mut p = p.borrow_mut();
        if let Some(slab) = p.buckets.get_mut(&class).and_then(|v| v.pop()) {
            p.pooled_bytes -= class;
            return Some(slab);
        }
        None
    });

    if let Some(slab) = slab {
        return PooledBuf { slab: Some(slab), class, used: need };
    }

    // Allocate a fresh slab. zeroed (avoids reading uninit before recv).
    let mut buf = vec![0u8; class];
    let reg = match register_memory(buf.as_mut_ptr() as *mut c_void, class) {
        Ok(r) => Some(r),
        Err(e) => {
            POOL.with(|p| {
                let mut p = p.borrow_mut();
                if !p.warned_over_cap {
                    p.warned_over_cap = true;
                    tracing::warn!(
                        class, error = %e,
                        "regpool: ucp_mem_map failed (memlock?); falling back to \
                         unregistered buffers (copy-out, not zero-copy)"
                    );
                }
            });
            None
        }
    };
    PooledBuf { slab: Some(Slab { buf, reg }), class, used: need }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_class_rounds_to_pow2_min_4k() {
        assert_eq!(size_class(1), 4096);
        assert_eq!(size_class(4096), 4096);
        assert_eq!(size_class(4097), 8192);
        assert_eq!(size_class(256 * 1024), 256 * 1024);
        assert_eq!(size_class(256 * 1024 + 1), 512 * 1024);
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
}
