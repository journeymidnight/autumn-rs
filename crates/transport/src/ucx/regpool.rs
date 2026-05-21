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
use std::collections::HashMap;

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
    unsafe fn set_len(&mut self, _len: usize) {}
}

impl compio::buf::IoBufMut for PooledBuf {
    fn as_uninit(&mut self) -> &mut [std::mem::MaybeUninit<u8>] {
        let used = self.used;
        let buf = &mut self.slab.as_mut().expect("slab present").buf[..used];
        // SAFETY: `u8` and `MaybeUninit<u8>` share layout; these bytes are
        // already initialized (the slab is zeroed on first alloc), so viewing
        // them as MaybeUninit is sound. `read_at` writes them before read-back.
        unsafe {
            std::slice::from_raw_parts_mut(
                buf.as_mut_ptr() as *mut std::mem::MaybeUninit<u8>,
                used,
            )
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
    #[cfg_attr(not(feature = "ucx"), allow(unused_mut))]
    let mut buf = vec![0u8; class];
    #[cfg(feature = "ucx")]
    let reg = match crate::register_memory(buf.as_mut_ptr() as *mut std::os::raw::c_void, class) {
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
    // No-ucx: RegisteredMem is uninhabited, so `reg` is always None.
    #[cfg(not(feature = "ucx"))]
    let reg: Option<RegisteredMem> = None;
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
        let path = std::env::temp_dir()
            .join(format!("autumn_regpool_pread_{}.bin", std::process::id()));
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
            assert_eq!(pb.filled(), &pattern[..], "pooled slab must hold file bytes");
            let _ = compio::fs::remove_file(&path).await;
        });
    }
}
