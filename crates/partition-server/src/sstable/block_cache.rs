//! process-wide bounded cache of decoded SST data blocks.
//!
//! Keyed by `(row_stream extent_id, absolute byte offset of the block within
//! the extent)` — stable across SstReader instances and partition reopens.
//! Paged `SstReader`s (data not resident) consult THIS cache only; resident
//! readers keep their per-reader slot vec (their memory is the resident
//! bytes themselves, the slot vec just skips re-decode).
//!
//! Eviction: CLOCK. Entries sit in a slot array; a hit sets the slot's
//! reference bit, and on overflow the hand clears set bits and evicts the
//! first clear one. Every entry is reachable by the hand. The previous
//! sampled LRU took the first 16 entries of the map's iteration order, which
//! is fixed for a given table layout: entries outside that window were never
//! evicted, so once the cache filled only ~16 slots still turned over and a
//! working set larger than that missed on every get.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use super::format::DecodedBlock;

struct Slot {
    key: (u64, u64),
    block: Arc<DecodedBlock>,
    size: usize,
    referenced: bool,
}

struct Inner {
    map: HashMap<(u64, u64), usize>,
    slots: Vec<Option<Slot>>,
    free: Vec<usize>,
    hand: usize,
    bytes: usize,
    hits: u64,
    misses: u64,
}

impl Inner {
    fn remove_slot(&mut self, idx: usize) {
        if let Some(s) = self.slots[idx].take() {
            self.map.remove(&s.key);
            self.bytes -= s.size;
            self.free.push(idx);
        }
    }
}

pub struct BlockCache {
    inner: Mutex<Inner>,
    cap_bytes: usize,
}

impl BlockCache {
    pub fn new(cap_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                slots: Vec::new(),
                free: Vec::new(),
                hand: 0,
                bytes: 0,
                hits: 0,
                misses: 0,
            }),
            cap_bytes,
        }
    }

    pub fn get(&self, key: (u64, u64)) -> Option<Arc<DecodedBlock>> {
        let mut g = self.inner.lock();
        match g.map.get(&key).copied() {
            Some(idx) => {
                g.hits += 1;
                let slot = g.slots[idx].as_mut().expect("mapped slot is live");
                slot.referenced = true;
                Some(slot.block.clone())
            }
            None => {
                g.misses += 1;
                None
            }
        }
    }

    pub fn insert(&self, key: (u64, u64), block: Arc<DecodedBlock>, size: usize) {
        let mut g = self.inner.lock();
        let g = &mut *g;
        let idx = match g.map.get(&key).copied() {
            Some(idx) => {
                let slot = g.slots[idx].as_mut().expect("mapped slot is live");
                g.bytes = g.bytes - slot.size + size;
                slot.block = block;
                slot.size = size;
                slot.referenced = true;
                idx
            }
            None => {
                // Unreferenced until hit: a block read once goes first.
                let slot = Some(Slot {
                    key,
                    block,
                    size,
                    referenced: false,
                });
                let idx = match g.free.pop() {
                    Some(i) => {
                        g.slots[i] = slot;
                        i
                    }
                    None => {
                        g.slots.push(slot);
                        g.slots.len() - 1
                    }
                };
                g.map.insert(key, idx);
                g.bytes += size;
                idx
            }
        };
        // Never evicts the entry just inserted. Each pass clears every bit
        // it passes, so a victim turns up within two turns of the hand.
        while g.bytes > self.cap_bytes && g.map.len() > 1 {
            let h = g.hand;
            g.hand = (h + 1) % g.slots.len();
            if h == idx {
                continue;
            }
            match g.slots[h].as_mut() {
                None => {}
                Some(s) if s.referenced => s.referenced = false,
                Some(_) => g.remove_slot(h),
            }
        }
    }

    /// Drop every cached block belonging to `extent_id`. TEST-ONLY: production
    /// never invalidates per-extent — extent ids are never reused within a
    /// cache's owner, so a punched extent's keys are never asked for again.
    #[cfg(test)]
    pub fn invalidate_extent(&self, extent_id: u64) {
        let mut g = self.inner.lock();
        let idxs: Vec<usize> = g
            .map
            .iter()
            .filter(|((e, _), _)| *e == extent_id)
            .map(|(_, &i)| i)
            .collect();
        for i in idxs {
            g.remove_slot(i);
        }
    }

    /// TEST-ONLY diagnostic snapshot: `(bytes, entries, hits, misses)`.
    #[cfg(test)]
    pub fn stats(&self) -> (usize, usize, u64, u64) {
        let g = self.inner.lock();
        (g.bytes, g.map.len(), g.hits, g.misses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn blk() -> Arc<DecodedBlock> {
        // Smallest valid decoded block isn't trivial to fabricate via decode;
        // construct via the test helper.
        Arc::new(DecodedBlock::test_dummy(Bytes::from_static(b"x")))
    }

    #[test]
    fn bounded_eviction_and_invalidate() {
        let c = BlockCache::new(100);
        for i in 0..20u64 {
            c.insert((1, i), blk(), 10);
        }
        let (bytes, n, _h, _m) = c.stats();
        assert!(bytes <= 100, "cap respected: {bytes}");
        assert!(n <= 10);
        c.invalidate_extent(1);
        let (bytes, n, _, _) = c.stats();
        assert_eq!((bytes, n), (0, 0));
    }

    /// A scan: the cache fills with blocks used once, then every round brings
    /// one new block while the same 56 blocks (8 readers x 7 tables) are hit.
    /// The old sampled LRU only ever evicted from the first 16 entries of the
    /// map, so the hot set kept missing; here it must stay resident.
    #[test]
    fn a_hot_set_survives_a_stream_of_cold_blocks() {
        const CAP: usize = 512;
        let c = BlockCache::new(CAP);
        let mut cold = 0u64;
        for _ in 0..CAP {
            c.insert((1, cold), blk(), 1);
            cold += 1;
        }
        let mut misses = 0;
        for round in 0..4000 {
            c.insert((1, cold), blk(), 1);
            cold += 1;
            for h in 0..56u64 {
                if c.get((2, h)).is_none() {
                    c.insert((2, h), blk(), 1);
                    if round >= 1000 {
                        misses += 1;
                    }
                }
            }
        }
        assert_eq!(misses, 0, "hot blocks missed after warm-up");
        let (bytes, n, _, _) = c.stats();
        assert!(bytes <= CAP && n <= CAP);
    }

    #[test]
    fn hit_updates_lru() {
        let c = BlockCache::new(30);
        c.insert((1, 0), blk(), 10);
        c.insert((1, 1), blk(), 10);
        c.insert((1, 2), blk(), 10);
        // touch (1,0) so it's MRU, then overflow — (1,0) should survive.
        assert!(c.get((1, 0)).is_some());
        c.insert((1, 3), blk(), 10);
        assert!(c.get((1, 0)).is_some(), "MRU entry survived eviction");
    }
}
