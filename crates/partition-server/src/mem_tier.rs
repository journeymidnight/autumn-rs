//! Memory tier: values of keys under `--mem-tier-prefix` held in DRAM,
//! acknowledged without a log append, read before the LSM.
//!
//! **Not durable.** A PS restart, a partition reopen, split, merge or move
//! loses what is held here; only what was written through the normal path is
//! on disk. Meant for recomputable data (KV cache), where a lost value costs a
//! cache miss.
//!
//! One budget per PS (`--mem-tier-bytes`), shared by its partitions through an
//! atomic; each partition holds its own shard on its own thread, so the hot
//! path takes no lock. A shard evicts only its own entries: an insert that
//! cannot fit after evicting everything evictable in its shard is refused and
//! the caller writes through the durable path instead.
//!
//! Eviction goes by `priority = last read or write − rank × --mem-tier-rank-us`,
//! lowest first: least recently used, except that among entries touched at
//! about the same time a higher rank goes first. The writer sets the rank
//! (`BatchPutOp::evict_rank`); the KV-cache adapters give a page's position in
//! its prefix, in tokens. A hit is the run of consecutive pages from the start
//! of a prefix, so a prefix should lose its tail before its head: the pages
//! of one hit are read together and the deeper go first, and a chain written
//! over several batches, or extended by a later turn, keeps its head although
//! the head was written earlier. A rank converts to bounded time, so a cold
//! shallow entry still goes before a fresh deep one. Rank 0 is plain LRU.
//!
//! Entries sit in a slab (`slots` + `free`); `index` maps a key to its slot
//! and holds no key of its own, `order` is the eviction order.
//!
//! A durable write of a key held here (a delete, a conditional write, a fenced
//! put, a put or batch that did not fit) marks the entry superseded when it is
//! queued; the entry is dropped when that write is applied, so a read does not
//! fall back to a value older than the one held here. The exceptions drop the
//! entry outright: eviction, and a batch taking back the ops it had put in
//! before going durable (`discard`) — reads then see the LSM's value until the
//! durable write lands. A later memory put of the key replaces the entry and
//! clears the mark.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use hashbrown::HashTable;

/// Bookkeeping charged per entry on top of its key and value bytes: its slot
/// (~80 B), its `index` and `order` entries (~30 B), the key's allocation.
const ENTRY_OVERHEAD: u64 = 128;

/// The PS-wide side: configuration, the shared byte budget, counters.
pub(crate) struct MemTierBudget {
    cap: u64,
    /// `--mem-tier-rank-us`: the time one unit of rank is worth.
    rank_us: i64,
    prefixes: Vec<Vec<u8>>,
    /// Each prefix's `prefix_successor`.
    successors: Vec<Option<Vec<u8>>>,
    used: AtomicU64,
    pub(crate) hits: AtomicU64,
    pub(crate) inserts: AtomicU64,
    pub(crate) refused: AtomicU64,
    pub(crate) evicted_entries: AtomicU64,
    pub(crate) evicted_bytes: AtomicU64,
}

/// The first key past every key starting with `p`; None when there is none
/// (`p` is all 0xff).
fn prefix_successor(p: &[u8]) -> Option<Vec<u8>> {
    let mut s = p.to_vec();
    while let Some(last) = s.pop() {
        if last < 0xff {
            s.push(last + 1);
            return Some(s);
        }
    }
    None
}

impl MemTierBudget {
    /// None when the tier is off (no budget or no prefix).
    pub(crate) fn new(cap: u64, prefixes: &[String], rank_us: u64) -> Option<Arc<Self>> {
        let prefixes: Vec<Vec<u8>> =
            prefixes.iter().filter(|p| !p.is_empty()).map(|p| p.as_bytes().to_vec()).collect();
        if cap == 0 || prefixes.is_empty() {
            return None;
        }
        let successors = prefixes.iter().map(|p| prefix_successor(p)).collect();
        Some(Arc::new(MemTierBudget {
            cap,
            rank_us: rank_us.min(i64::MAX as u64) as i64,
            prefixes,
            successors,
            used: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            evicted_entries: AtomicU64::new(0),
            evicted_bytes: AtomicU64::new(0),
        }))
    }

    pub(crate) fn covers(&self, key: &[u8]) -> bool {
        self.prefixes.iter().any(|p| key.starts_with(p))
    }

    /// Whether a partition `[start, end)` (empty end = unbounded) can hold a
    /// covered key: the keys under a prefix are `[prefix, prefix's successor)`.
    pub(crate) fn overlaps(&self, start: &[u8], end: &[u8]) -> bool {
        self.prefixes.iter().zip(&self.successors).any(|(p, succ)| {
            let below_end = end.is_empty() || p.as_slice() < end;
            let above_start = succ.as_ref().is_none_or(|s| start < s.as_slice());
            below_end && above_start
        })
    }

    pub(crate) fn cap(&self) -> u64 {
        self.cap
    }

    pub(crate) fn used(&self) -> u64 {
        self.used.load(Relaxed)
    }

    /// Reserve `n` bytes if they fit under the cap.
    fn try_reserve(&self, n: u64) -> bool {
        let mut cur = self.used.load(Relaxed);
        loop {
            if cur.saturating_add(n) > self.cap {
                return false;
            }
            match self.used.compare_exchange_weak(cur, cur + n, Relaxed, Relaxed) {
                Ok(_) => return true,
                Err(now) => cur = now,
            }
        }
    }

    fn release(&self, n: u64) {
        self.used.fetch_sub(n, Relaxed);
    }
}

struct Slot {
    key: Box<[u8]>,
    value: Bytes,
    expires_at: u64,
    charge: u64,
    /// The `order` key: `(priority, this slot)`.
    priority: i64,
    rank: u32,
    superseded: bool,
}

/// A shard's entries; behind a `RefCell` because a read reorders them too.
struct Entries {
    slots: Vec<Option<Slot>>,
    free: Vec<u32>,
    /// Slot of each key, hashed by the key the slot holds.
    index: HashTable<u32>,
    /// Lowest first is evicted first.
    order: BTreeSet<(i64, u32)>,
}

fn hash_key(key: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(key)
}

impl Entries {
    fn find(&self, key: &[u8]) -> Option<u32> {
        let slots = &self.slots;
        self.index
            .find(hash_key(key), |&i| slots[i as usize].as_ref().is_some_and(|s| &*s.key == key))
            .copied()
    }

    fn slot(&self, i: u32) -> &Slot {
        self.slots[i as usize].as_ref().expect("an indexed slot is live")
    }

    /// Move slot `i` to `priority` in the eviction order.
    fn reorder(&mut self, i: u32, priority: i64) {
        let s = self.slots[i as usize].as_mut().expect("an indexed slot is live");
        if s.priority == priority {
            return;
        }
        self.order.remove(&(s.priority, i));
        s.priority = priority;
        self.order.insert((priority, i));
    }

    fn put(&mut self, slot: Slot) {
        let hash = hash_key(&slot.key);
        let priority = slot.priority;
        let i = match self.free.pop() {
            Some(i) => {
                self.slots[i as usize] = Some(slot);
                i
            }
            None => {
                self.slots.push(Some(slot));
                u32::try_from(self.slots.len() - 1).expect("under 4G entries per shard")
            }
        };
        let slots = &self.slots;
        self.index.insert_unique(hash, i, |&j| hash_key(&slots[j as usize].as_ref().expect("indexed").key));
        self.order.insert((priority, i));
    }

    /// Take slot `i` out of every structure.
    fn take(&mut self, i: u32) -> Slot {
        let slot = self.slots[i as usize].take().expect("an indexed slot is live");
        self.order.remove(&(slot.priority, i));
        match self.index.find_entry(hash_key(&slot.key), |&j| j == i) {
            Ok(e) => {
                e.remove();
            }
            Err(_) => unreachable!("a live slot is indexed"),
        }
        self.free.push(i);
        slot
    }
}

/// One partition's entries, owned by its thread.
pub(crate) struct MemShard {
    budget: Arc<MemTierBudget>,
    entries: RefCell<Entries>,
    /// Priorities are microseconds since this.
    epoch: Instant,
    /// Bytes this shard's entries are charged.
    held: u64,
}

/// What an entry is charged against the budget.
pub(crate) fn charge(key_len: usize, value_len: usize) -> u64 {
    key_len as u64 + value_len as u64 + ENTRY_OVERHEAD
}

impl MemShard {
    pub(crate) fn new(budget: Arc<MemTierBudget>) -> Self {
        MemShard {
            budget,
            entries: RefCell::new(Entries {
                slots: Vec::new(),
                free: Vec::new(),
                index: HashTable::new(),
                order: BTreeSet::new(),
            }),
            epoch: Instant::now(),
            held: 0,
        }
    }

    fn priority(&self, now: Instant, rank: u32) -> i64 {
        let t = now.saturating_duration_since(self.epoch).as_micros() as i64;
        t.saturating_sub(i64::from(rank).saturating_mul(self.budget.rank_us))
    }

    /// Whether `charge` bytes could fit with every evictable entry of this
    /// shard gone: the budget minus what the other partitions hold. Past that,
    /// evicting would empty the shard and still not make room.
    pub(crate) fn could_fit(&self, charge: u64) -> bool {
        charge <= self.budget.cap.saturating_sub(self.budget.used().saturating_sub(self.held))
    }

    pub(crate) fn covers(&self, key: &[u8]) -> bool {
        self.budget.covers(key)
    }

    pub(crate) fn budget(&self) -> Arc<MemTierBudget> {
        self.budget.clone()
    }

    /// Hold `value` for `key`, evicting for room the entries whose priority
    /// is below the new one's — never a better entry for a worse one, which
    /// would only go next, nor an equal one (a batch's other values, written
    /// at the same instant and rank). False when that does not make room;
    /// the caller then writes it durably, and a value held for the key is
    /// served until that write lands.
    pub(crate) fn insert(&mut self, key: &[u8], value: Bytes, expires_at: u64, rank: u32, now: Instant) -> bool {
        let charge = charge(key.len(), value.len());
        let priority = self.priority(now, rank);
        if !self.could_fit(charge) {
            self.budget.refused.fetch_add(1, Relaxed);
            self.supersede(key);
            return false;
        }
        while !self.budget.try_reserve(charge) {
            if !self.evict_below(priority) {
                self.budget.refused.fetch_add(1, Relaxed);
                self.supersede(key);
                return false;
            }
        }
        self.remove(key);
        let slot = Slot { key: key.into(), value, expires_at, charge, priority, rank, superseded: false };
        self.entries.get_mut().put(slot);
        self.held += charge;
        self.budget.inserts.fetch_add(1, Relaxed);
        true
    }

    /// The value held for `key` and its `expires_at`, if any and not
    /// expired. A hit counts as a use.
    pub(crate) fn get(&self, key: &[u8], now_secs: u64) -> Option<(Bytes, u64)> {
        self.get_at(key, now_secs, Instant::now())
    }

    fn get_at(&self, key: &[u8], now_secs: u64, now: Instant) -> Option<(Bytes, u64)> {
        let mut e = self.entries.borrow_mut();
        let i = e.find(key)?;
        let s = e.slot(i);
        if s.expires_at > 0 && s.expires_at <= now_secs {
            return None;
        }
        let hit = (s.value.clone(), s.expires_at);
        let priority = self.priority(now, s.rank);
        e.reorder(i, priority);
        self.budget.hits.fetch_add(1, Relaxed);
        Some(hit)
    }

    pub(crate) fn overlaps(&self, start: &[u8], end: &[u8]) -> bool {
        self.budget.overlaps(start, end)
    }

    /// Drop `key`'s entry: a batch that could not be held takes back what it
    /// had put in before going durable.
    pub(crate) fn discard(&mut self, key: &[u8]) {
        self.remove(key);
    }

    /// A durable write of `key` was queued: keep serving the held value until
    /// that write is applied (`applied`).
    pub(crate) fn supersede(&mut self, key: &[u8]) {
        let e = self.entries.get_mut();
        if let Some(i) = e.find(key) {
            e.slots[i as usize].as_mut().expect("indexed").superseded = true;
        }
    }

    /// A durable write of `key` reached the memtable.
    pub(crate) fn applied(&mut self, key: &[u8]) {
        let e = self.entries.get_mut();
        if e.find(key).is_some_and(|i| e.slot(i).superseded) {
            self.remove(key);
        }
    }

    fn remove(&mut self, key: &[u8]) {
        let e = self.entries.get_mut();
        if let Some(i) = e.find(key) {
            let s = e.take(i);
            self.budget.release(s.charge);
            self.held -= s.charge;
        }
    }

    /// Whether `need` more bytes fit without evicting anything at or above
    /// the priority a value of `rank` gets at `now`: the batch path checks
    /// its deepest value up front, so a batch that will be refused evicts
    /// nothing on the way.
    pub(crate) fn can_admit(&self, need: u64, rank: u32, now: Instant) -> bool {
        let floor = self.priority(now, rank);
        let mut room = self.budget.cap.saturating_sub(self.budget.used());
        if room >= need {
            return true;
        }
        let e = self.entries.borrow();
        for &(p, i) in &e.order {
            if p >= floor {
                return false;
            }
            room += e.slot(i).charge;
            if room >= need {
                return true;
            }
        }
        false
    }

    /// Drop the lowest-priority entry if its priority is below `priority`.
    /// False when there is none.
    fn evict_below(&mut self, priority: i64) -> bool {
        let e = self.entries.get_mut();
        let Some(&(p, i)) = e.order.first() else { return false };
        if p >= priority {
            return false;
        }
        let s = e.take(i);
        self.budget.release(s.charge);
        self.held -= s.charge;
        self.budget.evicted_entries.fetch_add(1, Relaxed);
        self.budget.evicted_bytes.fetch_add(s.charge, Relaxed);
        true
    }
}

impl Drop for MemShard {
    /// A closed partition gives its bytes back to the PS budget.
    fn drop(&mut self) {
        self.budget.release(self.held);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// 250 µs per rank unit (a token), the default.
    const RANK_US: u64 = 250;

    #[test]
    fn a_partition_overlaps_the_prefix_only_where_covered_keys_can_fall() {
        let b = MemTierBudget::new(1 << 20, &["kvc/".to_string()], RANK_US).unwrap();
        assert!(b.overlaps(b"", b""), "the whole key space");
        assert!(b.overlaps(b"kvc/m", b"kvc/n"));
        assert!(b.overlaps(b"a", b"kvc/0"), "end past the prefix start");
        assert!(!b.overlaps(b"a", b"kvc/"), "ends where covered keys begin");
        assert!(!b.overlaps(b"kvc0", b""), "starts past every covered key");
        assert!(b.overlaps(b"kvc/\xff", b""));
        assert!(!b.overlaps(b"fs/", b"fs0"));
    }

    fn shard_with(cap: u64, rank_us: u64) -> MemShard {
        MemShard::new(MemTierBudget::new(cap, &["kvc/".to_string()], rank_us).unwrap())
    }

    fn shard(cap: u64) -> MemShard {
        shard_with(cap, RANK_US)
    }

    fn v(n: usize) -> Bytes {
        Bytes::from(vec![7u8; n])
    }

    fn key(prefix: &str, i: usize) -> Vec<u8> {
        format!("kvc/{prefix}{i:04}").into_bytes()
    }

    /// The charge of one `v(1000)` entry under a `key(..)`.
    const ENTRY: u64 = 1000 + 9 + ENTRY_OVERHEAD;

    fn held(s: &MemShard, k: &[u8]) -> bool {
        s.entries.borrow().find(k).is_some()
    }

    #[test]
    fn off_without_budget_or_prefix() {
        assert!(MemTierBudget::new(0, &["kvc/".into()], RANK_US).is_none());
        assert!(MemTierBudget::new(1 << 20, &[], RANK_US).is_none());
        let b = MemTierBudget::new(1 << 20, &["kvc/".into(), "x/".into()], RANK_US).unwrap();
        assert!(b.covers(b"kvc/a") && b.covers(b"x/b") && !b.covers(b"fs/c"));
    }

    /// Rank 0 is LRU: the least recently written or read goes first.
    #[test]
    fn rank_zero_evicts_the_least_recently_used() {
        let mut s = shard(3 * ENTRY);
        let t0 = Instant::now();
        for i in 0..3 {
            assert!(s.insert(&key("k", i), v(1000), 0, 0, t0 + Duration::from_secs(i as u64)));
        }
        assert!(s.get_at(&key("k", 0), 0, t0 + Duration::from_secs(5)).is_some(), "read k0");
        assert!(s.insert(&key("k", 3), v(1000), 0, 0, t0 + Duration::from_secs(6)));
        assert!(held(&s, &key("k", 0)), "read since: kept");
        assert!(!held(&s, &key("k", 1)), "least recently used: evicted");
        assert!(held(&s, &key("k", 2)) && held(&s, &key("k", 3)));
    }

    /// Pages of a prefix, `pages` of them from `first`, written at `at`. A
    /// page here is 2048 tokens of rank, so 4 pages are one sglang batch
    /// (128 pages of 64 tokens): 8192 tokens, worth 2 s.
    fn write_pages(s: &mut MemShard, tag: &str, first: usize, pages: usize, at: Instant) {
        for i in first..first + pages {
            assert!(s.insert(&key(tag, i), v(1000), 0, (i * 2048) as u32, at));
        }
    }

    /// Room for `n` more entries: unrelated writes, rank 0, at `at`.
    fn press(s: &mut MemShard, n: usize, at: Instant) {
        for i in 0..n {
            assert!(s.insert(&key("p", i), v(1000), 0, 0, at));
        }
    }

    /// A chain written in two batches, head first: under pressure its tail
    /// goes before its head although the head was written earlier (LMCache
    /// #4867: a cache full of chains whose heads were gone). With no rank
    /// (`rank_us` 0) the head goes first.
    #[test]
    fn a_chain_written_in_two_batches_loses_its_tail_first() {
        let lost_first = |rank_us| {
            let mut s = shard_with(10 * ENTRY, rank_us);
            let t0 = Instant::now();
            write_pages(&mut s, "c", 0, 4, t0);
            write_pages(&mut s, "c", 4, 4, t0 + Duration::from_millis(500));
            press(&mut s, 4, t0 + Duration::from_secs(1));
            (0..8).filter(|&i| !held(&s, &key("c", i))).collect::<Vec<_>>()
        };
        assert_eq!(lost_first(RANK_US), vec![6, 7], "the deepest pages go first");
        assert_eq!(lost_first(0), vec![0, 1], "LRU alone drops the head");
    }

    /// A later turn appends deeper pages after reading the earlier ones (a
    /// prefix hit); under pressure the appended tail goes first, then the
    /// earlier turn's tail, never its head.
    #[test]
    fn a_later_turn_keeps_the_first_turns_head() {
        let mut s = shard(10 * ENTRY);
        let t0 = Instant::now();
        write_pages(&mut s, "t", 0, 4, t0);
        let t1 = t0 + Duration::from_secs(60);
        for i in 0..4 {
            assert!(s.get_at(&key("t", i), 0, t1).is_some());
        }
        write_pages(&mut s, "t", 4, 4, t1);
        press(&mut s, 6, t1 + Duration::from_millis(1));
        let kept: Vec<usize> = (0..8).filter(|&i| held(&s, &key("t", i))).collect();
        assert_eq!(kept, vec![0, 1, 2, 3]);
    }

    /// Rank is worth bounded time: an entry untouched for long goes before a
    /// fresh one however much deeper the fresh one is.
    #[test]
    fn a_cold_shallow_entry_goes_before_a_fresh_deep_one() {
        let mut s = shard(2 * ENTRY);
        let t0 = Instant::now();
        assert!(s.insert(b"kvc/old0", v(1000), 0, 0, t0));
        // 100K tokens deep: worth 25 s; the shallow entry is an hour older.
        assert!(s.insert(b"kvc/deep", v(1000), 0, 100_000, t0 + Duration::from_secs(3600)));
        assert!(s.insert(b"kvc/next", v(1000), 0, 0, t0 + Duration::from_secs(3601)));
        assert!(!held(&s, b"kvc/old0") && held(&s, b"kvc/deep"));
    }

    /// A value worse than everything held is refused rather than evicting a
    /// better one, and a value already held for its key is served until the
    /// durable write lands.
    #[test]
    fn a_worse_value_does_not_evict_better_ones() {
        let mut s = shard(2 * ENTRY);
        let t0 = Instant::now();
        assert!(s.insert(&key("a", 0), v(1000), 0, 0, t0));
        assert!(s.insert(&key("a", 1), v(1000), 0, 0, t0));
        // 8192 tokens deep, written 1 s later: worth 2 s, so worse than both.
        assert!(!s.insert(&key("d", 0), v(1000), 0, 8192, t0 + Duration::from_secs(1)));
        assert!(held(&s, &key("a", 0)) && held(&s, &key("a", 1)));
        // A rewrite of a held key, refused: the held value stays until applied.
        assert!(!s.insert(&key("a", 0), v(1000), 0, 8192, t0 + Duration::from_secs(1)));
        assert!(s.get_at(&key("a", 0), 0, t0).is_some());
        s.applied(&key("a", 0));
        assert!(!held(&s, &key("a", 0)));
        // Fresher still, it goes in.
        assert!(s.insert(&key("d", 0), v(1000), 0, 8192, t0 + Duration::from_secs(5)));
    }

    /// A batch whose deepest value could not make room evicts nothing.
    #[test]
    fn a_batch_that_cannot_be_admitted_is_seen_up_front() {
        let mut s = shard(3 * ENTRY);
        let t0 = Instant::now();
        for i in 0..3 {
            assert!(s.insert(&key("h", i), v(1000), 0, 0, t0));
        }
        let later = t0 + Duration::from_secs(1);
        assert!(!s.can_admit(2 * ENTRY, 8192, later), "worth 2 s: below everything held");
        assert!(s.can_admit(2 * ENTRY, 0, later));
    }

    /// The values of one batch (one instant, one rank) never evict each other:
    /// one that does not fit is refused, and the caller writes the batch
    /// durably.
    #[test]
    fn a_batch_does_not_evict_itself() {
        let mut s = shard(3 * ENTRY);
        let t0 = Instant::now();
        let fit: Vec<bool> = (0..4).map(|i| s.insert(&key("b", i), v(1000), 0, 64, t0)).collect();
        assert_eq!(fit, [true, true, true, false]);
        assert!((0..3).all(|i| held(&s, &key("b", i))));
    }

    /// Entries removed every way leave nothing behind: slots are reused and
    /// every structure holds exactly the live entries.
    #[test]
    fn removed_entries_leave_nothing_behind() {
        let mut s = shard(1 << 30);
        let t0 = Instant::now();
        for round in 0..50u64 {
            let at = t0 + Duration::from_secs(round);
            for i in 0..300 {
                assert!(s.insert(&key("r", i), v(10), 0, 0, at));
            }
        }
        {
            let e = s.entries.borrow();
            assert_eq!((e.index.len(), e.order.len()), (300, 300));
            assert!(e.slots.len() <= 301, "slots grew to {}", e.slots.len());
        }
        for i in 0..100 {
            s.supersede(&key("r", i));
            s.applied(&key("r", i));
        }
        for i in 100..200 {
            s.discard(&key("r", i));
        }
        let e = s.entries.borrow();
        assert_eq!((e.index.len(), e.order.len(), e.free.len()), (100, 100, e.slots.len() - 100));
        assert_eq!(s.held, 100 * charge(9, 10));
        assert_eq!(s.budget.used(), s.held);
    }

    /// A discarded entry is gone and its bytes are back; evicting what is
    /// left releases nothing twice.
    #[test]
    fn a_discarded_entry_releases_its_bytes_once() {
        let mut s = shard(1 << 20);
        let now = Instant::now();
        assert!(s.insert(b"kvc/a", Bytes::from(vec![1; 1000]), 0, 0, now));
        assert!(s.insert(b"kvc/b", Bytes::from(vec![2; 1000]), 0, 0, now));
        let before = s.budget.used();
        s.discard(b"kvc/a");
        assert_eq!(s.get(b"kvc/a", 0), None);
        assert_eq!(s.budget.used(), before - charge(5, 1000));
        assert!(s.evict_below(i64::MAX));
        assert!(!s.evict_below(i64::MAX), "empty");
        assert_eq!((s.budget.used(), s.held), (0, 0));
    }

    /// An insert that could not fit with its own shard emptied — the other
    /// partitions hold the rest of the budget — is refused before evicting
    /// anything.
    #[test]
    fn an_insert_past_what_the_shard_could_free_evicts_nothing() {
        let budget = MemTierBudget::new(10_000, &["kvc/".to_string()], RANK_US).unwrap();
        let mut other = MemShard::new(budget.clone());
        let mut s = MemShard::new(budget);
        let now = Instant::now();
        assert!(other.insert(b"kvc/o", Bytes::from(vec![0; 6000]), 0, 0, now));
        assert!(s.insert(b"kvc/a", Bytes::from(vec![1; 2000]), 0, 0, now));
        assert!(!s.insert(b"kvc/big", Bytes::from(vec![2; 4000]), 0, 0, now), "4000 + overhead does not fit in 10000 - 6000");
        assert!(s.get(b"kvc/a", 0).is_some(), "nothing was evicted for it");
        assert!(s.could_fit(charge(5, 1000)));
        assert!(!s.could_fit(charge(7, 4000)));
    }

    /// A full shard keeps taking writes, the newest held.
    #[test]
    fn nonstop_writes_keep_being_accepted() {
        let mut s = shard(200 * ENTRY);
        let t0 = Instant::now();
        let accepted =
            (0..3000).filter(|&i| s.insert(&key("n", i), v(1000), 0, 0, t0 + Duration::from_millis(i as u64))).count();
        assert_eq!(accepted, 3000);
        assert!((2800..3000).all(|i| held(&s, &key("n", i))));
    }

    #[test]
    fn a_value_that_cannot_fit_is_refused() {
        let mut s = shard(500);
        assert!(!s.insert(b"kvc/big", v(1000), 0, 0, Instant::now()));
        assert_eq!(s.budget.used(), 0);
        assert_eq!(s.budget.refused.load(Relaxed), 1);
    }

    #[test]
    fn expired_entries_read_as_absent() {
        let mut s = shard(1 << 20);
        assert!(s.insert(b"kvc/t", v(10), 100, 0, Instant::now()));
        assert!(s.get(b"kvc/t", 99).is_some());
        assert!(s.get(b"kvc/t", 100).is_none());
    }

    #[test]
    fn a_superseded_entry_goes_when_the_durable_write_lands() {
        let mut s = shard(1 << 20);
        assert!(s.insert(b"kvc/k", v(10), 0, 0, Instant::now()));
        s.supersede(b"kvc/k");
        assert!(s.get(b"kvc/k", 0).is_some(), "still served until the write applies");
        s.applied(b"kvc/k");
        assert!(s.get(b"kvc/k", 0).is_none());
        // A memory put after the durable write was queued survives its apply.
        assert!(s.insert(b"kvc/k", v(10), 0, 0, Instant::now()));
        s.applied(b"kvc/k");
        assert!(s.get(b"kvc/k", 0).is_some());
        assert_eq!(s.budget.used(), charge(5, 10));
    }

    #[test]
    fn dropping_a_shard_returns_its_bytes() {
        let budget = MemTierBudget::new(1 << 20, &["kvc/".to_string()], RANK_US).unwrap();
        let mut s = MemShard::new(budget.clone());
        assert!(s.insert(b"kvc/x", v(100), 0, 0, Instant::now()));
        assert!(budget.used() > 0);
        drop(s);
        assert_eq!(budget.used(), 0);
    }
}
