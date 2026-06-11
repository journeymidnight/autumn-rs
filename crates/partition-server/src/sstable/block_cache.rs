//! F261 — process-wide bounded cache of decoded SST data blocks.
//!
//! Keyed by `(row_stream extent_id, absolute byte offset of the block within
//! the extent)` — stable across SstReader instances and partition reopens.
//! Paged `SstReader`s (data not resident) consult THIS cache only; resident
//! readers keep their per-reader slot vec (their memory is the resident
//! bytes themselves, the slot vec just skips re-decode).
//!
//! Eviction: sampled-LRU (CLOCK-ish) — on overflow, scan up to
//! `EVICT_SAMPLE` entries from the map's (randomized) iteration order and
//! evict the least-recently-used of the sample, repeating until under cap.
//! O(1)-ish, no ordered structure to maintain on the hot hit path.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use super::format::DecodedBlock;

const EVICT_SAMPLE: usize = 16;

struct Entry {
    block: Arc<DecodedBlock>,
    size: usize,
    last_used: u64,
}

struct Inner {
    map: HashMap<(u64, u32), Entry>,
    bytes: usize,
    tick: u64,
    hits: u64,
    misses: u64,
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
                bytes: 0,
                tick: 0,
                hits: 0,
                misses: 0,
            }),
            cap_bytes,
        }
    }

    pub fn get(&self, key: (u64, u32)) -> Option<Arc<DecodedBlock>> {
        let mut g = self.inner.lock();
        g.tick += 1;
        let tick = g.tick;
        match g.map.get_mut(&key) {
            Some(e) => {
                e.last_used = tick;
                let b = e.block.clone();
                g.hits += 1;
                Some(b)
            }
            None => {
                g.misses += 1;
                None
            }
        }
    }

    pub fn insert(&self, key: (u64, u32), block: Arc<DecodedBlock>, size: usize) {
        let mut g = self.inner.lock();
        g.tick += 1;
        let tick = g.tick;
        if let Some(old) = g.map.insert(
            key,
            Entry {
                block,
                size,
                last_used: tick,
            },
        ) {
            g.bytes -= old.size;
        }
        g.bytes += size;
        while g.bytes > self.cap_bytes && g.map.len() > 1 {
            // Sampled LRU: pick the oldest of up to EVICT_SAMPLE entries,
            // never evicting the key we just inserted.
            let victim = g
                .map
                .iter()
                .filter(|(k, _)| **k != key)
                .take(EVICT_SAMPLE)
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| *k);
            match victim {
                Some(v) => {
                    if let Some(e) = g.map.remove(&v) {
                        g.bytes -= e.size;
                    }
                }
                None => break,
            }
        }
    }

    /// F261: drop every cached block belonging to `extent_id` — called when
    /// row_stream extents are truncated/punched after compaction so stale
    /// blocks can't be served for a recycled (extent, offset).
    pub fn invalidate_extent(&self, extent_id: u64) {
        let mut g = self.inner.lock();
        let keys: Vec<(u64, u32)> = g
            .map
            .keys()
            .filter(|(e, _)| *e == extent_id)
            .copied()
            .collect();
        for k in keys {
            if let Some(e) = g.map.remove(&k) {
                g.bytes -= e.size;
            }
        }
    }

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
        for i in 0..20u32 {
            c.insert((1, i), blk(), 10);
        }
        let (bytes, n, _h, _m) = c.stats();
        assert!(bytes <= 100, "cap respected: {bytes}");
        assert!(n <= 10);
        c.invalidate_extent(1);
        let (bytes, n, _, _) = c.stats();
        assert_eq!((bytes, n), (0, 0));
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
