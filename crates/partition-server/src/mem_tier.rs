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
//! Eviction is CLOCK over GROUPS, not keys: a group is a burst of inserts with
//! no gap above `GROUP_GAP` (one engine batch, in practice), capped at
//! `GROUP_MAX_ENTRIES`. A KV-cache hit is the run of consecutive pages from the
//! start of a prefix, so evicting one page in the middle of a run wastes every
//! page after it; dropping a burst together does not. A read or existence
//! check of any member gives the whole group a second chance.
//!
//! A durable write of a key held here (a delete, a conditional write, a batch
//! put, a put that did not fit) marks the entry superseded when it is queued;
//! the entry is dropped when that write is applied, so a read never falls back
//! to a value older than the one held here. A later memory put of the key
//! replaces the entry and clears the mark.

use std::cell::Cell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

/// Inserts closer together than this belong to one group.
const GROUP_GAP: Duration = Duration::from_millis(50);
/// A group never grows past this many entries.
const GROUP_MAX_ENTRIES: usize = 256;
/// Bookkeeping charged per entry on top of its key (twice) and value bytes.
const ENTRY_OVERHEAD: u64 = 96;

/// The PS-wide side: configuration, the shared byte budget, counters.
pub(crate) struct MemTierBudget {
    cap: u64,
    prefixes: Vec<Vec<u8>>,
    used: AtomicU64,
    pub(crate) hits: AtomicU64,
    pub(crate) inserts: AtomicU64,
    pub(crate) refused: AtomicU64,
    pub(crate) evicted_entries: AtomicU64,
    pub(crate) evicted_bytes: AtomicU64,
}

impl MemTierBudget {
    /// None when the tier is off (no budget or no prefix).
    pub(crate) fn new(cap: u64, prefixes: &[String]) -> Option<Arc<Self>> {
        let prefixes: Vec<Vec<u8>> =
            prefixes.iter().filter(|p| !p.is_empty()).map(|p| p.as_bytes().to_vec()).collect();
        if cap == 0 || prefixes.is_empty() {
            return None;
        }
        Some(Arc::new(MemTierBudget {
            cap,
            prefixes,
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

struct Entry {
    value: Bytes,
    expires_at: u64,
    charge: u64,
    group: u64,
    referenced: Rc<Cell<bool>>,
    superseded: bool,
}

struct Group {
    id: u64,
    keys: Vec<Vec<u8>>,
    referenced: Rc<Cell<bool>>,
}

/// One partition's entries, owned by its thread.
pub(crate) struct MemShard {
    budget: Arc<MemTierBudget>,
    entries: HashMap<Vec<u8>, Entry>,
    /// CLOCK ring of groups; the front is the hand.
    groups: VecDeque<Group>,
    next_group: u64,
    last_insert: Option<Instant>,
}

impl MemShard {
    pub(crate) fn new(budget: Arc<MemTierBudget>) -> Self {
        MemShard { budget, entries: HashMap::new(), groups: VecDeque::new(), next_group: 0, last_insert: None }
    }

    pub(crate) fn covers(&self, key: &[u8]) -> bool {
        self.budget.covers(key)
    }

    pub(crate) fn budget(&self) -> Arc<MemTierBudget> {
        self.budget.clone()
    }

    /// Hold `value` for `key`. False when it does not fit even after evicting
    /// this shard's evictable groups; the caller then writes it durably.
    pub(crate) fn insert(&mut self, key: &[u8], value: Bytes, expires_at: u64, now: Instant) -> bool {
        // The key is stored twice: in the map and in its group.
        let charge = 2 * key.len() as u64 + value.len() as u64 + ENTRY_OVERHEAD;
        while !self.budget.try_reserve(charge) {
            if !self.evict_one_group() {
                self.budget.refused.fetch_add(1, Relaxed);
                // The caller writes it durably: keep serving the held value
                // until that write lands.
                self.supersede(key);
                return false;
            }
        }
        self.remove(key);
        let open = self.last_insert.is_some_and(|t| now.duration_since(t) <= GROUP_GAP)
            && self.groups.back().is_some_and(|g| g.keys.len() < GROUP_MAX_ENTRIES);
        if !open {
            self.groups.push_back(Group { id: self.next_group, keys: Vec::new(), referenced: Rc::new(Cell::new(false)) });
            self.next_group += 1;
        }
        self.last_insert = Some(now);
        let group = self.groups.back_mut().expect("a group was just ensured");
        group.keys.push(key.to_vec());
        let entry = Entry {
            value,
            expires_at,
            charge,
            group: group.id,
            referenced: group.referenced.clone(),
            superseded: false,
        };
        self.entries.insert(key.to_vec(), entry);
        self.budget.inserts.fetch_add(1, Relaxed);
        true
    }

    /// The value held for `key` and its `expires_at`, if any and not
    /// expired. A hit gives the key's group a second chance.
    pub(crate) fn get(&self, key: &[u8], now_secs: u64) -> Option<(Bytes, u64)> {
        let e = self.entries.get(key)?;
        if e.expires_at > 0 && e.expires_at <= now_secs {
            return None;
        }
        e.referenced.set(true);
        self.budget.hits.fetch_add(1, Relaxed);
        Some((e.value.clone(), e.expires_at))
    }

    /// A durable write of `key` was queued: keep serving the held value until
    /// that write is applied (`applied`).
    pub(crate) fn supersede(&mut self, key: &[u8]) {
        if let Some(e) = self.entries.get_mut(key) {
            e.superseded = true;
        }
    }

    /// A durable write of `key` reached the memtable.
    pub(crate) fn applied(&mut self, key: &[u8]) {
        if self.entries.get(key).is_some_and(|e| e.superseded) {
            self.remove(key);
        }
    }

    fn remove(&mut self, key: &[u8]) {
        if let Some(e) = self.entries.remove(key) {
            self.budget.release(e.charge);
        }
    }

    /// Drop the first group the hand finds unreferenced, clearing the
    /// reference of each it passes. False when the shard holds nothing.
    fn evict_one_group(&mut self) -> bool {
        // Two turns at most: the first clears every reference bit.
        for _ in 0..2 * self.groups.len() + 1 {
            let Some(g) = self.groups.pop_front() else { return false };
            if g.referenced.replace(false) {
                self.groups.push_back(g);
                continue;
            }
            let mut bytes = 0;
            let mut n = 0;
            for k in &g.keys {
                // A key re-inserted later belongs to a newer group.
                if self.entries.get(k).is_some_and(|e| e.group == g.id) {
                    let e = self.entries.remove(k).expect("checked above");
                    self.budget.release(e.charge);
                    bytes += e.charge;
                    n += 1;
                }
            }
            self.budget.evicted_entries.fetch_add(n, Relaxed);
            self.budget.evicted_bytes.fetch_add(bytes, Relaxed);
            return true;
        }
        false
    }
}

impl Drop for MemShard {
    /// A closed partition gives its bytes back to the PS budget.
    fn drop(&mut self) {
        let held: u64 = self.entries.values().map(|e| e.charge).sum();
        self.budget.release(held);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(cap: u64) -> MemShard {
        MemShard::new(MemTierBudget::new(cap, &["kvc/".to_string()]).unwrap())
    }

    fn v(n: usize) -> Bytes {
        Bytes::from(vec![7u8; n])
    }

    #[test]
    fn off_without_budget_or_prefix() {
        assert!(MemTierBudget::new(0, &["kvc/".into()]).is_none());
        assert!(MemTierBudget::new(1 << 20, &[]).is_none());
        let b = MemTierBudget::new(1 << 20, &["kvc/".into(), "x/".into()]).unwrap();
        assert!(b.covers(b"kvc/a") && b.covers(b"x/b") && !b.covers(b"fs/c"));
    }

    #[test]
    fn a_burst_is_evicted_together_and_a_read_saves_its_group() {
        let entry = 1000 + 2 * 6 + ENTRY_OVERHEAD;
        let mut s = shard(6 * entry);
        let t0 = Instant::now();
        // Two bursts of three, far apart: two groups.
        for (i, k) in [b"kvc/a1", b"kvc/a2", b"kvc/a3"].iter().enumerate() {
            assert!(s.insert(*k, v(1000), 0, t0 + Duration::from_millis(i as u64)));
        }
        let t1 = t0 + Duration::from_secs(1);
        for (i, k) in [b"kvc/b1", b"kvc/b2", b"kvc/b3"].iter().enumerate() {
            assert!(s.insert(*k, v(1000), 0, t1 + Duration::from_millis(i as u64)));
        }
        // Reading one member of the older group gives it a second chance.
        assert!(s.get(b"kvc/a2", 0).is_some());
        let t2 = t1 + Duration::from_secs(1);
        assert!(s.insert(b"kvc/c1", v(1000), 0, t2));
        for k in [b"kvc/b1", b"kvc/b2", b"kvc/b3"] {
            assert!(s.get(k, 0).is_none(), "the unread burst went as a whole");
        }
        for k in [b"kvc/a1", b"kvc/a2", b"kvc/a3", b"kvc/c1"] {
            assert!(s.get(k, 0).is_some());
        }
        assert_eq!(s.budget.used(), 4 * entry);
    }

    #[test]
    fn a_value_that_cannot_fit_is_refused() {
        let mut s = shard(500);
        assert!(!s.insert(b"kvc/big", v(1000), 0, Instant::now()));
        assert_eq!(s.budget.used(), 0);
        assert_eq!(s.budget.refused.load(Relaxed), 1);
    }

    #[test]
    fn expired_entries_read_as_absent() {
        let mut s = shard(1 << 20);
        assert!(s.insert(b"kvc/t", v(10), 100, Instant::now()));
        assert!(s.get(b"kvc/t", 99).is_some());
        assert!(s.get(b"kvc/t", 100).is_none());
    }

    #[test]
    fn a_superseded_entry_goes_when_the_durable_write_lands() {
        let mut s = shard(1 << 20);
        assert!(s.insert(b"kvc/k", v(10), 0, Instant::now()));
        s.supersede(b"kvc/k");
        assert!(s.get(b"kvc/k", 0).is_some(), "still served until the write applies");
        s.applied(b"kvc/k");
        assert!(s.get(b"kvc/k", 0).is_none());
        // A memory put after the durable write was queued survives its apply.
        assert!(s.insert(b"kvc/k", v(10), 0, Instant::now()));
        s.applied(b"kvc/k");
        assert!(s.get(b"kvc/k", 0).is_some());
        assert_eq!(s.budget.used(), 10 + 2 * 5 + ENTRY_OVERHEAD);
    }

    #[test]
    fn dropping_a_shard_returns_its_bytes() {
        let budget = MemTierBudget::new(1 << 20, &["kvc/".to_string()]).unwrap();
        let mut s = MemShard::new(budget.clone());
        assert!(s.insert(b"kvc/x", v(100), 0, Instant::now()));
        assert!(budget.used() > 0);
        drop(s);
        assert_eq!(budget.used(), 0);
    }
}
