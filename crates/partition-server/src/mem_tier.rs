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
//! page after it; dropping a burst together does not. Groups sit in a
//! `crate::clock`: a new group starts unreferenced, a read or check of any
//! member sets its reference bit, and the hand evicts the first group it finds
//! without one, clearing the bits it passes. The group a burst writes to is
//! remembered by slot and id, so a burst never spills into an older group.
//!
//! A write does not set the bit. When it did, every group was born referenced:
//! under steady writes the hand cleared a whole turn every few batches, and a
//! group read once between batches was evicted whenever the hand reached it
//! twice in that time — the first eviction was such a turn.
//!
//! A durable write of a key held here (a delete, a conditional write, a batch
//! put, a put that did not fit) marks the entry superseded when it is queued;
//! the entry is dropped when that write is applied, so a read never falls back
//! to a value older than the one held here. A later memory put of the key
//! replaces the entry and clears the mark.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::clock::Clock;

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
    /// The group's slot in `groups`.
    slot: usize,
    superseded: bool,
}

struct Group {
    id: u64,
    keys: Vec<Vec<u8>>,
}

/// One partition's entries, owned by its thread.
pub(crate) struct MemShard {
    budget: Arc<MemTierBudget>,
    entries: HashMap<Vec<u8>, Entry>,
    groups: Clock<Group>,
    next_group: u64,
    /// The group the current burst writes to: (slot, id).
    open: Option<(usize, u64)>,
    last_insert: Option<Instant>,
}

impl MemShard {
    pub(crate) fn new(budget: Arc<MemTierBudget>) -> Self {
        MemShard {
            budget,
            entries: HashMap::new(),
            groups: Clock::new(),
            next_group: 0,
            open: None,
            last_insert: None,
        }
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
        let in_burst = self.last_insert.is_some_and(|t| now.duration_since(t) <= GROUP_GAP);
        let open = self.open.filter(|&(slot, id)| {
            in_burst && self.groups.get(slot).is_some_and(|g| g.id == id && g.keys.len() < GROUP_MAX_ENTRIES)
        });
        let (slot, id) = open.unwrap_or_else(|| {
            let id = self.next_group;
            self.next_group += 1;
            (self.groups.insert(Group { id, keys: Vec::new() }), id)
        });
        self.open = Some((slot, id));
        self.last_insert = Some(now);
        self.groups.get_mut(slot).expect("the open group is live").keys.push(key.to_vec());
        let entry = Entry { value, expires_at, charge, group: id, slot, superseded: false };
        self.entries.insert(key.to_vec(), entry);
        self.budget.inserts.fetch_add(1, Relaxed);
        true
    }

    /// The value held for `key` and its `expires_at`, if any and not
    /// expired. A hit sets its group's reference bit.
    pub(crate) fn get(&self, key: &[u8], now_secs: u64) -> Option<(Bytes, u64)> {
        let e = self.entries.get(key)?;
        if e.expires_at > 0 && e.expires_at <= now_secs {
            return None;
        }
        self.groups.touch(e.slot);
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

    /// Drop the group the clock picks. False when the shard holds nothing.
    fn evict_one_group(&mut self) -> bool {
        let Some((_, g)) = self.groups.evict(None) else { return false };
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
        true
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

    /// Inserts `keys` as one burst at `at`.
    fn burst(s: &mut MemShard, keys: &[&[u8]], at: Instant) {
        for (i, k) in keys.iter().enumerate() {
            assert!(s.insert(k, v(1000), 0, at + Duration::from_millis(i as u64)));
        }
    }

    #[test]
    fn a_burst_is_evicted_together_oldest_first() {
        let entry = 1000 + 2 * 6 + ENTRY_OVERHEAD;
        let mut s = shard(6 * entry);
        let t0 = Instant::now();
        burst(&mut s, &[b"kvc/a1", b"kvc/a2", b"kvc/a3"], t0);
        burst(&mut s, &[b"kvc/b1", b"kvc/b2", b"kvc/b3"], t0 + Duration::from_secs(1));
        burst(&mut s, &[b"kvc/c1"], t0 + Duration::from_secs(2));
        for k in [b"kvc/a1", b"kvc/a2", b"kvc/a3"] {
            assert!(s.get(k, 0).is_none(), "the oldest burst went as a whole");
        }
        for k in [b"kvc/b1", b"kvc/b2", b"kvc/b3", b"kvc/c1"] {
            assert!(s.get(k, 0).is_some());
        }
        assert_eq!(s.budget.used(), 4 * entry);
    }

    /// A burst read since the hand last passed outlives one that was not.
    /// Ablation: a GET leaves the bit alone and k2 goes.
    #[test]
    fn a_read_since_the_last_sweep_saves_a_burst() {
        let entry = 1000 + 2 * 6 + ENTRY_OVERHEAD;
        let mut s = shard(3 * entry);
        let t0 = Instant::now();
        for (i, k) in [b"kvc/k1", b"kvc/k2", b"kvc/k3", b"kvc/k4"].iter().enumerate() {
            burst(&mut s, &[*k], t0 + Duration::from_secs(i as u64));
        }
        // k4 needed room: the hand evicted k1; k2 is next.
        assert!(s.get(b"kvc/k1", 0).is_none());
        assert!(s.get(b"kvc/k2", 0).is_some(), "read k2");
        burst(&mut s, &[b"kvc/k5"], t0 + Duration::from_secs(5));
        assert!(s.get(b"kvc/k2", 0).is_some(), "read since the sweep: kept");
        assert!(s.get(b"kvc/k3", 0).is_none(), "unread since the sweep: evicted");
    }

    /// Hot entries still held before each round. The budget holds the bytes of
    /// `groups` full groups;
    /// `pre` groups are written, then `hot` groups; each round reads every hot
    /// entry, then writes `churn` entries in bursts of seeded uneven sizes
    /// (full groups when `seed` is None).
    fn hot_left_per_round(pre: usize, rounds: usize, seed: Option<u64>) -> Vec<usize> {
        let (groups, hot, churn) = (16, 2, 4 * GROUP_MAX_ENTRIES);
        let entry = 100 + 2 * 14 + ENTRY_OVERHEAD;
        let mut s = shard(groups * GROUP_MAX_ENTRIES as u64 * entry);
        let mut t = Instant::now();
        let mut n = 0;
        let mut burst = |s: &mut MemShard, tag: &str, len: usize| -> Vec<Vec<u8>> {
            t += Duration::from_secs(1);
            n += 1;
            let keys: Vec<Vec<u8>> = (0..len).map(|i| format!("kvc/{tag}{n:05}{i:04}").into_bytes()).collect();
            for (i, k) in keys.iter().enumerate() {
                assert!(s.insert(k, v(100), 0, t + Duration::from_micros(i as u64)));
            }
            keys
        };
        for _ in 0..pre {
            burst(&mut s, "p", GROUP_MAX_ENTRIES);
        }
        let hot_keys: Vec<Vec<u8>> = (0..hot).flat_map(|_| burst(&mut s, "h", GROUP_MAX_ENTRIES)).collect();
        let mut rng = seed.unwrap_or(0);
        let mut left_per_round = Vec::new();
        for _ in 0..rounds {
            left_per_round.push(hot_keys.iter().filter(|k| s.get(k, 0).is_some()).count());
            let mut left = churn;
            while left > 0 {
                let len = match seed {
                    None => GROUP_MAX_ENTRIES,
                    Some(_) => {
                        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                        1 + (rng >> 33) as usize % GROUP_MAX_ENTRIES
                    }
                };
                burst(&mut s, "c", len.min(left));
                left -= len.min(left);
            }
        }
        left_per_round
    }

    /// Groups read before the first eviction survive it. When a write set the
    /// bit, the first eviction cleared a whole turn and then evicted in write
    /// order, the read groups first (round 4).
    #[test]
    fn groups_read_before_the_first_eviction_survive_it() {
        let left = hot_left_per_round(0, 20, None);
        assert!(left.iter().all(|&h| h == 2 * GROUP_MAX_ENTRIES), "hot entries per round: {left:?}");
    }

    /// Groups read once per round survive steady writes of uneven bursts. When
    /// a write set the bit, 16 of these 20 seeds lost them, first at round 15:
    /// the hand cleared them, then cleared a whole turn of fresh groups and
    /// reached them again before the next read.
    #[test]
    fn groups_read_every_round_survive_steady_writes() {
        for seed in 0..20 {
            let left = hot_left_per_round(24, 40, Some(seed));
            assert!(left.iter().all(|&h| h == 2 * GROUP_MAX_ENTRIES), "seed {seed}: hot entries per round {left:?}");
        }
    }

    /// A burst that needs room keeps its own group. When the open group was
    /// whatever sat last in the ring, the hand moving read groups behind it
    /// made the rest of the burst join one of them (c2 joined b).
    #[test]
    fn a_burst_that_evicts_keeps_its_own_group() {
        let entry = 1000 + 2 * 6 + ENTRY_OVERHEAD;
        let mut s = shard(8 * entry);
        let t0 = Instant::now();
        let (a, b): (&[&[u8]], &[&[u8]]) =
            (&[b"kvc/a1", b"kvc/a2", b"kvc/a3", b"kvc/a4"], &[b"kvc/b1", b"kvc/b2", b"kvc/b3"]);
        burst(&mut s, a, t0);
        burst(&mut s, b, t0 + Duration::from_secs(1));
        for k in a.iter().chain(b) {
            assert!(s.get(k, 0).is_some());
        }
        let old: std::collections::HashSet<u64> = s.entries.values().map(|e| e.group).collect();
        let c: &[&[u8]] = &[b"kvc/c1", b"kvc/c2", b"kvc/c3", b"kvc/c4"];
        burst(&mut s, c, t0 + Duration::from_secs(2));
        let held: Vec<u64> = c.iter().filter_map(|k| s.entries.get(*k).map(|e| e.group)).collect();
        assert!(held.len() >= 3, "burst keys held: {held:?}");
        assert!(held.iter().all(|g| !old.contains(g)), "burst keys joined an older group: {held:?} vs {old:?}");
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
