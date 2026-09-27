//! Daemon-side readahead: for a file read sequentially, fetch the blocks ahead
//! of the reader in parallel and answer the kernel's READs from memory.
//!
//! The kernel's own readahead keeps about one window in flight per faulting
//! thread, so a single-stream reader waits a round trip per window, and when
//! several threads fault on one file their read-arounds fragment (measured: a
//! 4 MiB window fell to ~36 KiB READs at 124-188 MiB/s on nine cores). The
//! mount itself is not the limit — eight threads of `pread` on it reached
//! 1.6-2.7 GB/s at 4 ms per read. This module gives any reader that depth: the
//! dispatcher spots a sequential front ([`Detector`]) and plans the blocks
//! ahead of it; read workers fetch them into [`PrefetchCache`]; a READ that
//! falls inside fetched blocks is answered from there.
//!
//! How far ahead: a front prefetches as far as it has already read in sequence
//! (at least one [`BLOCK`], at most [`WINDOW`]). A safetensors load first
//! touches the head of every tensor — 2-4 MiB each, jumping 32-86 MiB — and
//! only then copies the tensors in order. Fetching a full window ahead of every
//! head fetched ~1.7x the file (3.3 GB fetched for 1.9 GB, 1.4 GB never read),
//! and releasing blocks once the readers had "moved past" them dropped exactly
//! the ones the copy pass came back for — so there is no such release.
//!
//! Memory: a block lives only between its fetch and the kernel taking it — the
//! kernel's page cache holds it from then on — so the daemon holds about the
//! window ahead of each active reader, not the file. The cache has a budget
//! (`--prefetch-mem-mb`); a block that does not fit is not fetched and the READ
//! goes to the cluster as it would without prefetch: no READ ever waits on the
//! budget. A block is freed when every byte of it has been served, when it has
//! sat unread for [`IDLE`], when its file closes, or when the file's content
//! generation changes. The bytes a front read from the cluster before its first
//! block existed are credited to that block at admission — otherwise it never
//! reaches "fully served", and such blocks (measured) held a 1 GiB budget full
//! within a 2 GB load. Blocks are plain heap memory: the transport's registered
//! pool holds receive buffers only, and the bytes are copied into the block.
//!
//! Consistency: blocks are tagged with the generation the dispatcher saw when it
//! planned them, and every READ carries the generation `read::prepare` just saw
//! — `meta::get_inode` re-reads the inode once an invalidation has marked it,
//! and this mount's own writes bump the generation. A READ whose generation
//! differs drops the file's blocks and goes to the cluster. Bytes fetched before
//! another client's writer closed can be served until that close's invalidation
//! arrives — the same window the kernel page cache itself has.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::channel::oneshot;

/// Prefetch unit. Blocks start at multiples of it, so several readers of one
/// file share them.
pub const BLOCK: u64 = 4 << 20;
/// The furthest ahead of a reader's front blocks are kept in flight.
pub const WINDOW: u64 = 64 << 20;
/// A READ this close to a front continues it: the kernel sends the READs of a
/// readahead window concurrently, so they arrive slightly out of order. One
/// kernel window (`--readahead-kb` default); 4 MiB let random 4 KiB reads of a
/// 2 GB file pass as sequential often enough to put their p99 at 24.7 ms —
/// they waited for a 4 MiB block where a direct read takes 2 ms.
const FRONT_TOLERANCE: u64 = 2 << 20;
/// Bytes a front must actually have read before it prefetches, and they must
/// cover at least half of the range it spans. Counting READs instead let a few
/// random 4 KiB reads that happened to land near each other — each one able to
/// push `next` almost FRONT_TOLERANCE ahead — pass as a run.
const MIN_RUN_BYTES: u64 = 3 << 20;
/// Fronts tracked per file: every thread faulting through a file makes its own,
/// and one more than the fronts evicts the oldest over and over.
const MAX_FRONTS: usize = 16;
/// A fetched block nobody has read for this long is dropped.
pub const IDLE: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------- detector

/// A block to fetch, and how much of it the reader already read from the
/// cluster (credited as served, so the block is freed once the rest is).
#[derive(Debug, PartialEq, Eq)]
pub struct Fetch {
    pub block: u64,
    pub already_read: u64,
}

/// Dispatcher-side: which files are being read sequentially, and which blocks
/// ahead of their readers have already been asked for.
#[derive(Default)]
pub struct Detector {
    files: HashMap<u64, FileFronts>,
}

struct FileFronts {
    generation: u64,
    fronts: Vec<Front>,
    /// Block indices planned under `generation`, never asked for twice: a
    /// block that was served sits in the kernel's page cache, and fetching it
    /// again would fetch bytes nobody reads.
    issued: BTreeSet<u64>,
    clock: u64,
}

struct Front {
    /// Where this run of sequential READs began.
    start: u64,
    /// Where this reader is expected to read next.
    next: u64,
    /// Bytes the run's READs actually asked for.
    read: u64,
    used: u64,
}

impl Detector {
    /// Record a READ of `[offset, offset + len)` of a `file_size`-byte file and
    /// return the blocks to fetch now: none until the READ continues a dense
    /// enough run, then those up to `next + clamp(read, BLOCK, WINDOW)` not yet
    /// asked for.
    pub fn on_read(
        &mut self,
        ino: u64,
        generation: u64,
        offset: u64,
        len: u64,
        file_size: u64,
    ) -> Vec<Fetch> {
        if len == 0 || file_size < 2 * BLOCK {
            return Vec::new();
        }
        let f = self.files.entry(ino).or_insert_with(|| FileFronts {
            generation,
            fronts: Vec::new(),
            issued: BTreeSet::new(),
            clock: 0,
        });
        if f.generation != generation {
            f.generation = generation;
            f.issued.clear();
        }
        f.clock += 1;
        let clock = f.clock;
        let end = offset + len;
        let continued = f.fronts.iter().position(|fr| {
            offset + FRONT_TOLERANCE >= fr.next && offset <= fr.next + FRONT_TOLERANCE
        });
        let idx = match continued {
            Some(i) => {
                let fr = &mut f.fronts[i];
                fr.read += len;
                fr.start = fr.start.min(offset);
                fr.next = fr.next.max(end);
                fr.used = clock;
                i
            }
            None => {
                if f.fronts.len() == MAX_FRONTS {
                    let oldest = (0..f.fronts.len())
                        .min_by_key(|&i| f.fronts[i].used)
                        .expect("MAX_FRONTS > 0");
                    f.fronts.swap_remove(oldest);
                }
                f.fronts.push(Front { start: offset, next: end, read: len, used: clock });
                f.fronts.len() - 1
            }
        };
        let front = &f.fronts[idx];
        if front.read < MIN_RUN_BYTES || front.read * 2 < front.next - front.start {
            return Vec::new();
        }
        let (start, next) = (front.start, front.next);
        let ahead = front.read.clamp(BLOCK, WINDOW);
        let first = next / BLOCK;
        let last = (next + ahead).min(file_size).div_ceil(BLOCK); // exclusive
        (first..last)
            .filter(|b| f.issued.insert(*b))
            .map(|b| {
                let block = b * BLOCK;
                // Only the block holding `next` lies partly behind it, and of
                // that only what this run read — from `start` on — was read.
                let already_read = next.saturating_sub(block.max(start)).min(BLOCK);
                Fetch { block, already_read }
            })
            .collect()
    }

    /// A planned block was not fetched after all (the budget refused it, or
    /// planning failed): let a later READ ask for it again.
    pub fn unissue(&mut self, ino: u64, block: u64) {
        if let Some(f) = self.files.get_mut(&ino) {
            f.issued.remove(&(block / BLOCK));
        }
    }

    /// The file closed (or the kernel forgot it).
    pub fn forget(&mut self, ino: u64) {
        self.files.remove(&ino);
    }
}

// ------------------------------------------------------------------- cache

/// Counters, cumulative since the mount started.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct Stats {
    /// READs answered from fetched blocks.
    pub hits: u64,
    /// READs that waited for a block still being fetched.
    pub waits: u64,
    /// READs of a file with blocks that the blocks could not answer.
    pub misses: u64,
    /// Blocks admitted for fetching.
    pub admitted: u64,
    /// Blocks not fetched because the budget was full.
    pub refused: u64,
    /// Block fetches that failed (their READs go to the cluster).
    pub failed: u64,
    /// Fetched bytes dropped before the kernel read them.
    pub wasted_bytes: u64,
}

/// Blocks fetched ahead of readers, shared by every read worker. The lock is
/// held for lookups and bookkeeping only, never across I/O.
pub struct PrefetchCache {
    budget: u64,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    files: HashMap<u64, FileBlocks>,
    used: u64,
    stats: Stats,
}

struct FileBlocks {
    generation: u64,
    blocks: BTreeMap<u64, Block>,
}

struct Block {
    /// Bytes the block holds (a block at the end of the file is short).
    len: u64,
    served: u64,
    touched: Instant,
    state: BlockState,
}

enum BlockState {
    /// Being fetched; READs that need it wait on these.
    Fetching(Vec<oneshot::Sender<()>>),
    Ready(Arc<Vec<u8>>),
}

/// Bytes answering a READ.
pub enum Served {
    /// A range of one block — no copy.
    Slice(Arc<Vec<u8>>, Range<usize>),
    /// Assembled from several blocks, or fetched from the cluster.
    Owned(Vec<u8>),
}

impl Served {
    pub fn bytes(&self) -> &[u8] {
        match self {
            Served::Slice(data, range) => &data[range.clone()],
            Served::Owned(data) => data,
        }
    }
}

pub enum Lookup {
    Hit(Served),
    /// A block the READ needs is being fetched: wait for it, then look again.
    /// The sender is dropped if the block is, so this always resolves.
    Wait(oneshot::Receiver<()>),
    Miss,
}

impl FileBlocks {
    /// Drop every block, returning the bytes they were charged and the fetched
    /// bytes nobody read.
    fn clear(&mut self) -> (u64, u64) {
        let mut charged = 0;
        let mut wasted = 0;
        for b in std::mem::take(&mut self.blocks).into_values() {
            charged += b.len;
            if matches!(b.state, BlockState::Ready(_)) {
                wasted += b.len.saturating_sub(b.served);
            }
        }
        (charged, wasted)
    }
}

/// A block admitted for fetching, owned by whoever fetches it. `complete`
/// hands the bytes over; dropping it uncompleted — a job lost with a dead
/// worker, a submit no worker took — drops the block, so the READs waiting on it
/// wake and go to the cluster instead of waiting out their reply timeout.
pub struct Admitted {
    cache: Arc<PrefetchCache>,
    ino: u64,
    generation: u64,
    block: u64,
    done: bool,
}

impl Admitted {
    pub fn ino(&self) -> u64 {
        self.ino
    }

    pub fn block(&self) -> u64 {
        self.block
    }

    pub fn complete(mut self, data: Option<Vec<u8>>) {
        self.done = true;
        self.cache.complete(self.ino, self.generation, self.block, data);
    }
}

impl Drop for Admitted {
    fn drop(&mut self) {
        if !self.done {
            self.cache.complete(self.ino, self.generation, self.block, None);
        }
    }
}

impl PrefetchCache {
    /// `admit`, returning the block's owner: see [`Admitted`].
    pub fn reserve(
        self: &Arc<Self>,
        ino: u64,
        generation: u64,
        block: u64,
        len: u64,
        already_read: u64,
    ) -> Option<Admitted> {
        self.admit(ino, generation, block, len, already_read).then(|| Admitted {
            cache: self.clone(),
            ino,
            generation,
            block,
            done: false,
        })
    }

    pub fn new(budget_bytes: u64) -> Self {
        Self { budget: budget_bytes, inner: Mutex::new(Inner::default()) }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A panic while holding the lock leaves plain bookkeeping behind, which
        // is still consistent enough to keep serving; poisoning would turn one
        // panic into EIO on every later read.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Reserve `[block, block + len)` of `ino` for fetching, `already_read`
    /// bytes of it counted as served. False when it is already there, when the
    /// reader already has all of it, or when the budget has no room; the caller
    /// then does not fetch.
    pub fn admit(&self, ino: u64, generation: u64, block: u64, len: u64, already_read: u64) -> bool {
        let mut g = self.lock();
        let inner = &mut *g;
        let file = inner
            .files
            .entry(ino)
            .or_insert_with(|| FileBlocks { generation, blocks: BTreeMap::new() });
        if file.generation != generation {
            let (charged, wasted) = file.clear();
            inner.used -= charged;
            inner.stats.wasted_bytes += wasted;
            file.generation = generation;
        }
        if file.blocks.contains_key(&block) || already_read >= len {
            return false;
        }
        if inner.used + len > self.budget {
            inner.stats.refused += 1;
            return false;
        }
        file.blocks.insert(
            block,
            Block {
                len,
                served: already_read,
                touched: Instant::now(),
                state: BlockState::Fetching(Vec::new()),
            },
        );
        inner.used += len;
        inner.stats.admitted += 1;
        true
    }

    /// A fetch finished: `Some(bytes)` makes the block readable, `None` drops it.
    /// Wakes whoever waited. A block dropped meanwhile (generation change, file
    /// closed) is simply gone; its bytes are discarded.
    pub fn complete(&self, ino: u64, generation: u64, block: u64, data: Option<Vec<u8>>) {
        let waiters = {
            let mut g = self.lock();
            let inner = &mut *g;
            let Some(file) = inner.files.get_mut(&ino).filter(|f| f.generation == generation) else {
                return;
            };
            let Some(b) = file.blocks.get_mut(&block) else {
                return;
            };
            let BlockState::Fetching(waiters) = &mut b.state else {
                return;
            };
            let waiters = std::mem::take(waiters);
            match data {
                Some(bytes) => {
                    b.state = BlockState::Ready(Arc::new(bytes));
                    b.touched = Instant::now();
                }
                None => {
                    let len = b.len;
                    file.blocks.remove(&block);
                    inner.used -= len;
                    inner.stats.failed += 1;
                }
            }
            waiters
        };
        for w in waiters {
            // A waiter whose READ already timed out dropped its receiver; there
            // is nobody left to tell.
            w.send(()).ok();
        }
    }

    /// Answer a READ of `[offset, offset + len)` from fetched blocks if they hold
    /// all of it. A READ carrying a different generation drops the file's blocks.
    pub fn lookup(&self, ino: u64, generation: u64, offset: u64, len: u64) -> Lookup {
        let mut g = self.lock();
        let inner = &mut *g;
        let Some(file) = inner.files.get_mut(&ino) else {
            return Lookup::Miss;
        };
        if file.generation != generation {
            let (charged, wasted) = file.clear();
            inner.used -= charged;
            inner.stats.wasted_bytes += wasted;
            inner.files.remove(&ino);
            inner.stats.misses += 1;
            return Lookup::Miss;
        }
        let end = offset + len;
        let first = offset / BLOCK * BLOCK;
        let mut start = first;
        while start < end {
            let covered = match file.blocks.get_mut(&start) {
                None => false,
                Some(b) => match &mut b.state {
                    BlockState::Fetching(waiters) => {
                        let (tx, rx) = oneshot::channel();
                        waiters.push(tx);
                        inner.stats.waits += 1;
                        return Lookup::Wait(rx);
                    }
                    BlockState::Ready(data) => data.len() as u64 >= end.min(start + BLOCK) - start,
                },
            };
            if !covered {
                inner.stats.misses += 1;
                return Lookup::Miss;
            }
            start += BLOCK;
        }

        // Every block is there: take the bytes and charge what was served.
        let now = Instant::now();
        let mut parts: Vec<(Arc<Vec<u8>>, Range<usize>)> = Vec::new();
        let mut start = first;
        while start < end {
            let b = file.blocks.get_mut(&start).expect("checked above");
            let from = offset.max(start) - start;
            let to = end.min(start + BLOCK) - start;
            let BlockState::Ready(data) = &b.state else { unreachable!("checked above") };
            parts.push((data.clone(), from as usize..to as usize));
            b.served += to - from;
            b.touched = now;
            if b.served >= b.len {
                let len = b.len;
                file.blocks.remove(&start);
                inner.used -= len;
            }
            start += BLOCK;
        }
        if file.blocks.is_empty() {
            inner.files.remove(&ino);
        }
        inner.stats.hits += 1;
        drop(g);
        Lookup::Hit(if parts.len() == 1 {
            let (data, range) = parts.pop().expect("one part");
            Served::Slice(data, range)
        } else {
            let mut out = Vec::with_capacity(len as usize);
            for (data, range) in &parts {
                out.extend_from_slice(&data[range.clone()]);
            }
            Served::Owned(out)
        })
    }

    /// The file closed: its blocks go.
    pub fn drop_file(&self, ino: u64) {
        let mut g = self.lock();
        let inner = &mut *g;
        if let Some(mut file) = inner.files.remove(&ino) {
            let (charged, wasted) = file.clear();
            inner.used -= charged;
            inner.stats.wasted_bytes += wasted;
        }
    }

    /// Drop fetched blocks nobody has read for [`IDLE`]. Blocks still being
    /// fetched stay: their fetch is bounded and completes or fails.
    pub fn sweep(&self, now: Instant) {
        let mut g = self.lock();
        let inner = &mut *g;
        let mut freed = 0;
        let mut wasted = 0;
        for file in inner.files.values_mut() {
            file.blocks.retain(|_, b| {
                let idle = matches!(b.state, BlockState::Ready(_))
                    && now.duration_since(b.touched) >= IDLE;
                if idle {
                    freed += b.len;
                    wasted += b.len.saturating_sub(b.served);
                }
                !idle
            });
        }
        inner.files.retain(|_, f| !f.blocks.is_empty());
        inner.used -= freed;
        inner.stats.wasted_bytes += wasted;
    }

    pub fn stats(&self) -> Stats {
        self.lock().stats
    }

    /// Bytes currently charged against the budget.
    pub fn used(&self) -> u64 {
        self.lock().used
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;
    const FILE: u64 = 1024 * MIB;

    fn blocks(plan: &[Fetch]) -> Vec<u64> {
        plan.iter().map(|f| f.block).collect()
    }

    #[test]
    fn a_second_sequential_read_starts_with_one_block_and_the_window_grows_with_the_run() {
        let mut d = Detector::default();
        assert!(d.on_read(1, 7, 0, MIB, FILE).is_empty(), "one READ is no pattern");
        assert!(d.on_read(1, 7, MIB, MIB, FILE).is_empty(), "nor are two");
        // 3 MiB read: one BLOCK ahead of `next`.
        assert_eq!(blocks(&d.on_read(1, 7, 2 * MIB, MIB, FILE)), vec![0, BLOCK]);
        let mut furthest = 0;
        for i in 3..96 {
            for f in d.on_read(1, 7, i * MIB, MIB, FILE) {
                furthest = furthest.max(f.block + BLOCK);
            }
        }
        // After 96 MiB read in sequence the full WINDOW is ahead.
        assert_eq!(furthest, 96 * MIB + WINDOW);
    }

    #[test]
    fn touching_the_heads_of_far_apart_regions_fetches_little() {
        // The first pass of a safetensors load: a few MiB at each tensor head.
        let mut d = Detector::default();
        let mut fetched = 0;
        for head in (0..40u64).map(|t| t * 86 * MIB) {
            for i in 0..4 {
                fetched += d.on_read(1, 7, head + i * MIB, MIB, 4096 * MIB).len() as u64;
            }
        }
        assert!(fetched <= 40 * 3, "fetched {} blocks for 40 heads", fetched);
    }

    #[test]
    fn scattered_reads_prefetch_nothing() {
        let mut d = Detector::default();
        for i in 0..20u64 {
            let off = (i * 37 % 20) * 40 * MIB;
            assert!(d.on_read(1, 7, off, 64 << 10, FILE).is_empty(), "read at {off}");
        }
    }

    #[test]
    fn random_small_reads_of_a_large_file_never_prefetch() {
        // The shape whose p99 prefetch once ruined: 4 KiB reads at random.
        let mut d = Detector::default();
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut fetched = 0;
        for _ in 0..4000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let off = x % (2048 * MIB) / 4096 * 4096;
            fetched += d.on_read(1, 7, off, 4096, 2048 * MIB).len();
        }
        assert_eq!(fetched, 0);
    }

    #[test]
    fn interleaved_fronts_each_prefetch() {
        let mut d = Detector::default();
        let (a, b) = (0, 512 * MIB);
        for i in 0..2 {
            assert!(d.on_read(1, 7, a + i * MIB, MIB, FILE).is_empty());
            assert!(d.on_read(1, 7, b + i * MIB, MIB, FILE).is_empty());
        }
        assert!(blocks(&d.on_read(1, 7, a + 2 * MIB, MIB, FILE)).contains(&a));
        assert!(blocks(&d.on_read(1, 7, b + 2 * MIB, MIB, FILE)).contains(&b));
    }

    #[test]
    fn a_block_is_asked_for_once_per_generation() {
        let mut d = Detector::default();
        d.on_read(1, 7, 0, MIB, FILE);
        d.on_read(1, 7, MIB, MIB, FILE);
        assert_eq!(blocks(&d.on_read(1, 7, 2 * MIB, MIB, FILE)), vec![0, BLOCK]);
        assert!(!blocks(&d.on_read(1, 7, 3 * MIB, MIB, FILE)).contains(&BLOCK));
        // Bytes of the old generation are stale: ahead of the reader, ask again.
        assert!(blocks(&d.on_read(1, 8, 4 * MIB, MIB, FILE)).contains(&BLOCK));
    }

    #[test]
    fn what_the_run_already_read_is_credited_to_its_first_block() {
        let mut d = Detector::default();
        d.on_read(1, 7, BLOCK + MIB / 2, MIB / 2, FILE);
        d.on_read(1, 7, BLOCK + MIB, MIB, FILE);
        d.on_read(1, 7, BLOCK + 2 * MIB, MIB, FILE);
        let plan = d.on_read(1, 7, BLOCK + 3 * MIB, MIB / 2, FILE);
        // The run began half a MiB into block 1: only [start, next) was read.
        assert_eq!(plan[0], Fetch { block: BLOCK, already_read: 3 * MIB });
        assert!(plan[1..].iter().all(|f| f.already_read == 0));
    }

    #[test]
    fn small_files_and_the_end_of_file_bound_the_window() {
        let mut d = Detector::default();
        for i in 0..4 {
            assert!(d.on_read(1, 7, i * MIB, MIB, BLOCK).is_empty(), "too small to bother");
        }
        let size = 2 * BLOCK + MIB;
        let all: Vec<u64> = (0..12).flat_map(|i| blocks(&d.on_read(2, 7, i * MIB, MIB, size))).collect();
        assert_eq!(all, vec![0, BLOCK, 2 * BLOCK], "every block once, none past the end");
    }

    fn filled(block: u64, len: u64) -> Vec<u8> {
        (block..block + len).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn a_read_inside_a_fetched_block_is_served_and_the_block_freed_once_consumed() {
        let c = PrefetchCache::new(64 * MIB);
        assert!(c.admit(1, 7, 0, BLOCK, 0));
        assert_eq!(c.used(), BLOCK);
        c.complete(1, 7, 0, Some(filled(0, BLOCK)));
        for i in 0..BLOCK / MIB {
            let Lookup::Hit(s) = c.lookup(1, 7, i * MIB, MIB) else { panic!("miss at {i}") };
            assert_eq!(s.bytes(), &filled(0, BLOCK)[(i * MIB) as usize..((i + 1) * MIB) as usize]);
        }
        assert_eq!(c.used(), 0, "a fully served block is freed");
        assert!(matches!(c.lookup(1, 7, 0, MIB), Lookup::Miss));
        assert_eq!(c.stats().wasted_bytes, 0);
    }

    #[test]
    fn a_block_credited_with_what_the_reader_already_read_is_freed_when_the_rest_is_served() {
        let c = PrefetchCache::new(64 * MIB);
        assert!(!c.admit(1, 7, 0, BLOCK, BLOCK), "nothing left to serve");
        assert!(c.admit(1, 7, 0, BLOCK, 3 * MIB));
        c.complete(1, 7, 0, Some(filled(0, BLOCK)));
        assert!(matches!(c.lookup(1, 7, 3 * MIB, MIB), Lookup::Hit(_)));
        assert_eq!(c.used(), 0);
        assert_eq!(c.stats().wasted_bytes, 0);
    }

    #[test]
    fn a_read_across_a_block_boundary_is_assembled_from_both() {
        let c = PrefetchCache::new(64 * MIB);
        for b in [0, BLOCK] {
            assert!(c.admit(1, 7, b, BLOCK, 0));
            c.complete(1, 7, b, Some(filled(b, BLOCK)));
        }
        let Lookup::Hit(s) = c.lookup(1, 7, BLOCK - MIB / 2, MIB) else { panic!("miss") };
        assert_eq!(s.bytes(), &filled(BLOCK - MIB / 2, MIB)[..]);
    }

    #[test]
    fn a_read_of_a_block_in_flight_waits_and_is_woken() {
        let c = PrefetchCache::new(64 * MIB);
        assert!(c.admit(1, 7, 0, BLOCK, 0));
        let Lookup::Wait(mut rx) = c.lookup(1, 7, 0, MIB) else { panic!("expected a wait") };
        assert_eq!(rx.try_recv(), Ok(None), "not before the fetch completes");
        c.complete(1, 7, 0, Some(filled(0, BLOCK)));
        assert_eq!(rx.try_recv(), Ok(Some(())));
        assert!(matches!(c.lookup(1, 7, 0, MIB), Lookup::Hit(_)));
    }

    #[test]
    fn a_failed_fetch_wakes_its_waiters_and_frees_the_budget() {
        let c = PrefetchCache::new(64 * MIB);
        assert!(c.admit(1, 7, 0, BLOCK, 0));
        let Lookup::Wait(mut rx) = c.lookup(1, 7, 0, MIB) else { panic!("expected a wait") };
        c.complete(1, 7, 0, None);
        assert_eq!(rx.try_recv(), Ok(Some(())));
        assert!(matches!(c.lookup(1, 7, 0, MIB), Lookup::Miss));
        assert_eq!(c.used(), 0);
        assert_eq!(c.stats().failed, 1);
    }

    #[test]
    fn a_read_of_another_generation_drops_the_files_blocks() {
        let c = PrefetchCache::new(64 * MIB);
        assert!(c.admit(1, 7, 0, BLOCK, 0));
        c.complete(1, 7, 0, Some(filled(0, BLOCK)));
        assert!(matches!(c.lookup(1, 8, 0, MIB), Lookup::Miss), "old bytes must not answer");
        assert_eq!(c.used(), 0);
        assert_eq!(c.stats().wasted_bytes, BLOCK);
        assert!(matches!(c.lookup(1, 7, 0, MIB), Lookup::Miss), "and they are gone");
    }

    #[test]
    fn a_fetch_finishing_after_its_generation_changed_is_discarded() {
        let c = PrefetchCache::new(64 * MIB);
        assert!(c.admit(1, 7, 0, BLOCK, 0));
        assert!(c.admit(1, 8, BLOCK, BLOCK, 0), "new generation clears the old block");
        c.complete(1, 7, 0, Some(filled(0, BLOCK)));
        assert!(matches!(c.lookup(1, 8, 0, MIB), Lookup::Miss));
        assert_eq!(c.used(), BLOCK, "only the new generation's block is charged");
    }

    #[test]
    fn the_budget_refuses_blocks_that_do_not_fit() {
        let c = PrefetchCache::new(2 * BLOCK);
        assert!(c.admit(1, 7, 0, BLOCK, 0));
        assert!(c.admit(1, 7, BLOCK, BLOCK, 0));
        assert!(!c.admit(1, 7, 2 * BLOCK, BLOCK, 0));
        assert!(!c.admit(1, 7, 0, BLOCK, 0), "already there");
        assert_eq!(c.stats().refused, 1);
        c.drop_file(1);
        assert_eq!(c.used(), 0);
        assert!(c.admit(1, 7, 2 * BLOCK, BLOCK, 0));
    }

    #[test]
    fn a_short_last_block_serves_up_to_its_end_only() {
        let c = PrefetchCache::new(64 * MIB);
        assert!(c.admit(1, 7, 0, MIB, 0));
        c.complete(1, 7, 0, Some(filled(0, MIB)));
        assert!(matches!(c.lookup(1, 7, MIB / 2, MIB), Lookup::Miss), "past the fetched bytes");
        assert!(matches!(c.lookup(1, 7, 0, MIB / 2), Lookup::Hit(_)));
    }

    #[test]
    fn a_reserved_block_dropped_before_completion_wakes_its_waiters_and_frees_the_budget() {
        let c = Arc::new(PrefetchCache::new(64 * MIB));
        let slot = c.reserve(1, 7, 0, BLOCK, 0).expect("admitted");
        let Lookup::Wait(mut rx) = c.lookup(1, 7, 0, MIB) else { panic!("expected a wait") };
        drop(slot);
        assert_eq!(rx.try_recv(), Ok(Some(())), "the waiter must not sit out its timeout");
        assert_eq!(c.used(), 0);
        assert!(matches!(c.lookup(1, 7, 0, MIB), Lookup::Miss));
        let slot = c.reserve(1, 7, 0, BLOCK, 0).expect("admitted again");
        slot.complete(Some(filled(0, BLOCK)));
        assert!(matches!(c.lookup(1, 7, 0, MIB), Lookup::Hit(_)), "completing is not undone by the drop");
    }

    #[test]
    fn an_unissued_block_is_asked_for_again() {
        let mut d = Detector::default();
        d.on_read(1, 7, 0, MIB, FILE);
        d.on_read(1, 7, MIB, MIB, FILE);
        assert!(blocks(&d.on_read(1, 7, 2 * MIB, MIB, FILE)).contains(&BLOCK));
        d.unissue(1, BLOCK);
        assert!(blocks(&d.on_read(1, 7, 3 * MIB, MIB, FILE)).contains(&BLOCK));
    }

    #[test]
    fn idle_blocks_are_swept_but_blocks_in_flight_stay() {
        let c = PrefetchCache::new(64 * MIB);
        assert!(c.admit(1, 7, 0, BLOCK, 0));
        assert!(c.admit(1, 7, BLOCK, BLOCK, 0));
        c.complete(1, 7, 0, Some(filled(0, BLOCK)));
        c.sweep(Instant::now() + IDLE);
        assert_eq!(c.used(), BLOCK, "the fetching block stays");
        assert_eq!(c.stats().wasted_bytes, BLOCK);
        assert!(matches!(c.lookup(1, 7, 0, MIB), Lookup::Miss));
    }
}
