use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use parking_lot::Mutex;

use std::rc::Rc;

use autumn_stream::StreamClient;

use super::block_cache::BlockCache;
use super::bloom::BloomFilter;
use super::format::{BlockOffset, DecodedBlock, MetaBlock};

/// where this SSTable's data blocks live.
pub enum SstSource {
    /// Full SSTable bytes resident in memory (legacy mode; flush-fresh
    /// readers before conversion, tests, materialized iteration copies).
    Resident(Bytes),
    /// Data blocks are NOT resident: fetched on demand from row_stream at
    /// `(extent_id, base_in_extent + relative_offset)` through the
    /// process-wide bounded `BlockCache`. Only the MetaBlock-derived state
    /// (index/bloom/extremes) stays in memory (~KBs vs ~128 MB).
    Paged {
        extent_id: u64,
        base_in_extent: u64,
        len_in_extent: u64,
    },
}

/// SSTable reader. Holds the full SSTable bytes in memory (Arc-shared).
///
/// Blocks are decoded on demand from the in-memory bytes.
/// The MetaBlock (block index + bloom filter) is parsed at open time.
/// Decoded blocks are cached to avoid repeated CRC checks and memcpy.
pub struct SstReader {
    source: SstSource,
    block_offsets: Vec<BlockOffset>,
    bloom: Option<BloomFilter>,
    pub smallest_key: Vec<u8>,
    pub biggest_key: Vec<u8>,
    seq_num: u64,
    pub vp_extent_id: u64,
    pub vp_offset: u64,
    /// DEPRECATED / INERT (vp_table_refs removal, Stage 1). Mirrors the
    /// on-disk `MetaBlock.vp_deps`; formerly read by `collect_partition_vp_refs`
    /// to report each SST's VP extent dependencies to the manager. Nothing
    /// reads it now — kept so the SST format decode stays byte-identical (no
    /// migration yet). Removed with the format bump in Stage 2.
    #[allow(dead_code)]
    pub vp_deps: Vec<u64>,
    estimated_size: u64,
    pub discards: HashMap<u64, i64>,
    /// Earliest non-zero expires_at across all entries (0 = no expiring keys).
    pub min_expires_at: u64,
    sst_base: u64,
    /// Decoded block cache — avoids re-decoding (CRC + memcpy) on repeated reads.
    /// Mutex (not RefCell) so SstReader is Sync and can be shared across
    /// P-log/P-sst via Arc without the unsafe Rc→Arc transmute that the
    /// codebase once carried. In practice only P-log reads blocks; P-sst
    /// only consumes freshly built SstReaders via oneshot move. Contention is
    /// near-zero; the two-phase locking in read_block allows idempotent
    /// concurrent misses.
    block_cache: Mutex<Vec<Option<Arc<DecodedBlock>>>>,
}

impl SstReader {
    /// Open an SSTable from a Bytes buffer containing the full SSTable bytes.
    pub fn from_bytes(data: Bytes) -> Result<Self> {
        Self::open_at(data, 0)
    }

    /// Open an SSTable starting at `sst_base` within a larger buffer.
    pub fn open_at(data: Bytes, sst_base: u64) -> Result<Self> {
        let base = sst_base as usize;
        if data.len() < base + 8 {
            return Err(anyhow!("SSTable too short at base={sst_base}"));
        }
        let sst_end = data.len();
        Self::parse(data, base, sst_end)
    }

    /// Open from a slice: data[sst_base..sst_base+sst_len].
    #[allow(dead_code)]
    pub fn open_slice(data: Bytes, sst_base: u64, sst_len: u64) -> Result<Self> {
        let base = sst_base as usize;
        let end = base + sst_len as usize;
        if end > data.len() {
            return Err(anyhow!(
                "SSTable slice out of bounds base={sst_base} len={sst_len} data_len={}",
                data.len()
            ));
        }
        Self::parse(data, base, end)
    }

    /// construct a PAGED reader from just the MetaBlock bytes — the
    /// recovery path reads ONLY the meta tail off row_stream (two small
    /// reads) instead of materializing the whole SST (which kept recovery
    /// RSS at O(dataset): 9.4 GB of SSTs page-faulted ~25 GB transient).
    /// `meta_bytes` = the meta region EXCLUDING the trailing meta_len u32.
    pub fn open_paged_from_meta(
        meta_bytes: &[u8],
        extent_id: u64,
        base_in_extent: u64,
        len_in_extent: u64,
    ) -> Result<Self> {
        let meta = MetaBlock::decode(meta_bytes)?;
        let bloom = if meta.bloom_data.is_empty() {
            None
        } else {
            BloomFilter::decode(&meta.bloom_data)
        };
        Ok(SstReader {
            block_offsets: meta.block_offsets,
            bloom,
            smallest_key: meta.smallest_key,
            biggest_key: meta.biggest_key,
            seq_num: meta.seq_num,
            vp_extent_id: meta.vp_extent_id,
            vp_offset: meta.vp_offset,
            vp_deps: meta.vp_deps,
            estimated_size: meta.estimated_size,
            discards: meta.discards,
            min_expires_at: meta.min_expires_at,
            sst_base: 0,
            block_cache: Mutex::new(Vec::new()),
            source: SstSource::Paged {
                extent_id,
                base_in_extent,
                len_in_extent,
            },
        })
    }

    fn parse(data: Bytes, sst_base: usize, sst_end: usize) -> Result<Self> {
        if sst_end < sst_base + 8 {
            return Err(anyhow!("SSTable too short"));
        }
        // Last 4 bytes of the SSTable: meta_len
        let meta_len = u32::from_le_bytes(data[sst_end - 4..sst_end].try_into().unwrap()) as usize;
        if meta_len == 0 || meta_len + 4 > sst_end - sst_base {
            return Err(anyhow!("invalid meta_len={meta_len}"));
        }
        let meta_start = sst_end - 4 - meta_len;
        let meta_bytes = &data[meta_start..meta_start + meta_len];
        let meta = MetaBlock::decode(meta_bytes)?;

        let bloom = if meta.bloom_data.is_empty() {
            None
        } else {
            BloomFilter::decode(&meta.bloom_data)
        };

        let num_blocks = meta.block_offsets.len();
        Ok(SstReader {
            block_offsets: meta.block_offsets,
            bloom,
            smallest_key: meta.smallest_key,
            biggest_key: meta.biggest_key,
            seq_num: meta.seq_num,
            vp_extent_id: meta.vp_extent_id,
            vp_offset: meta.vp_offset,
            vp_deps: meta.vp_deps,
            estimated_size: meta.estimated_size,
            discards: meta.discards,
            min_expires_at: meta.min_expires_at,
            sst_base: sst_base as u64,
            block_cache: Mutex::new(vec![None; num_blocks]),
            source: SstSource::Resident(data),
        })
    }

    // -----------------------------------------------------------------------
    // Bloom filter
    // -----------------------------------------------------------------------

    /// Returns `true` if `user_key` may be in this SSTable (bloom filter check).
    /// Always returns `true` if no bloom filter is present.
    pub fn bloom_may_contain(&self, user_key: &[u8]) -> bool {
        match &self.bloom {
            Some(bf) => bf.may_contain(user_key),
            None => true,
        }
    }

    // -----------------------------------------------------------------------
    // Block access
    // -----------------------------------------------------------------------

    pub fn block_count(&self) -> usize {
        self.block_offsets.len()
    }

    pub fn seq_num(&self) -> u64 {
        self.seq_num
    }

    pub fn estimated_size(&self) -> u64 {
        self.estimated_size
    }

    pub fn smallest_key(&self) -> &[u8] {
        &self.smallest_key
    }

    pub fn biggest_key(&self) -> &[u8] {
        &self.biggest_key
    }

    /// Read and decode block at index `idx`. Cached after first decode.
    pub fn read_block(&self, idx: usize) -> Result<Arc<DecodedBlock>> {
        {
            let guard = self.block_cache.lock();
            if let Some(cached) = guard.get(idx).and_then(|c| c.clone()) {
                return Ok(cached);
            }
        }
        let bo = self.block_offsets.get(idx).ok_or_else(|| {
            anyhow!(
                "block index {idx} out of range (total={})",
                self.block_offsets.len()
            )
        })?;
        let data = match &self.source {
            SstSource::Resident(d) => d,
            SstSource::Paged { .. } => {
                return Err(anyhow!(
                    "block {idx}: SST is paged — use read_block_via / materialize"
                ));
            }
        };
        let start = self.sst_base as usize + bo.relative_offset as usize;
        let end = start + bo.block_len as usize;
        if end > data.len() {
            return Err(anyhow!(
                "block {idx} out of bounds: start={start} end={end} data_len={}",
                data.len()
            ));
        }
        let block = Arc::new(DecodedBlock::decode(data.slice(start..end), &bo.key)?);
        {
            let mut guard = self.block_cache.lock();
            if let Some(slot) = guard.get_mut(idx) {
                *slot = Some(block.clone());
            }
        }
        Ok(block)
    }

    /// convert a freshly parsed reader into PAGED mode — the resident
    /// bytes are dropped (memory returned) and future block reads go through
    /// `read_block_via`. `base_in_extent` is where this SST starts inside
    /// `extent_id` on row_stream (`TableMeta.offset`), `len_in_extent` its
    /// byte length (`TableMeta.len`).
    pub fn into_paged(mut self, extent_id: u64, base_in_extent: u64, len_in_extent: u64) -> Self {
        self.source = SstSource::Paged {
            extent_id,
            base_in_extent,
            len_in_extent,
        };
        // Per-reader decoded-slot cache only serves resident mode; paged
        // blocks live in the bounded global BlockCache instead.
        *self.block_cache.lock() = Vec::new();
        self
    }

    /// block read that works for BOTH modes. Resident → the sync path;
    /// paged → bounded global cache, miss fetched from row_stream via `sc`
    /// (rides the replica rotation). The await happens with NO RefCell
    /// borrow held — callers snapshot `Arc<SstReader>`s first (note 15).
    pub async fn read_block_via(
        &self,
        idx: usize,
        sc: &Rc<StreamClient>,
        cache: &BlockCache,
    ) -> Result<Arc<DecodedBlock>> {
        let (extent_id, base, len_in_extent) = match &self.source {
            SstSource::Resident(_) => return self.read_block(idx),
            SstSource::Paged {
                extent_id,
                base_in_extent,
                len_in_extent,
            } => (*extent_id, *base_in_extent, *len_in_extent),
        };
        let bo = self.block_offsets.get(idx).ok_or_else(|| {
            anyhow!(
                "block index {idx} out of range (total={})",
                self.block_offsets.len()
            )
        })?;
        // coco P2: checked arithmetic + SST-window bound. The resident
        // path is protected by its `end > data.len()` check; the paged path
        // must equivalently refuse a corrupt/foreign MetaBlock whose block
        // offsets point outside `[base, base + len_in_extent)` — otherwise a
        // bad offset reads a NEIGHBOURING SST's bytes from the same row
        // extent (or u32-wraps to an arbitrary offset) and serves them as
        // this SST's block.
        let in_bounds = bo
            .relative_offset
            .checked_add(bo.block_len)
            .and_then(|e| self.sst_base.checked_add(e as u64))
            .is_some_and(|end| end <= len_in_extent);
        if !in_bounds {
            return Err(anyhow!(
                "paged block {idx} out of SST bounds (corrupt meta?): \
                 sst_base={} rel_off={} len={} sst_len={}",
                self.sst_base,
                bo.relative_offset,
                bo.block_len,
                len_in_extent
            ));
        }
        let abs = base
            .checked_add(self.sst_base)
            .and_then(|v| v.checked_add(bo.relative_offset as u64))
            .ok_or_else(|| anyhow!("paged block {idx}: absolute offset overflows u64"))?;
        let key = (extent_id, abs);
        if let Some(b) = cache.get(key) {
            return Ok(b);
        }
        let (raw, _end) = sc
            .read_bytes_from_extent(extent_id, abs, bo.block_len as u64)
            .await?;
        if raw.len() < bo.block_len as usize {
            return Err(anyhow!(
                "paged block short read: extent={extent_id} off={abs} need={} got={}",
                bo.block_len,
                raw.len()
            ));
        }
        let block = Arc::new(DecodedBlock::decode(Bytes::from(raw), &bo.key)?);
        cache.insert(key, block.clone(), bo.block_len as usize);
        Ok(block)
    }

    /// bulk-fetch the raw bytes of consecutive blocks starting at
    /// `start_idx` — as many whole blocks as fit in `max_bytes` (always at
    /// least one) — in ONE read. For sequential scans (compaction / split
    /// key-scan): one RPC per window instead of per 64 KiB block, and it
    /// BYPASSES the global BlockCache (scan-resistant — a full-SST sweep
    /// must not evict the point-lookup working set; mirrors RocksDB's
    /// `fill_cache=false` compaction reads).
    ///
    /// Returns `(bytes, end_idx_exclusive, win_start_rel)`; decode
    /// individual blocks out of it with `decode_block_from_window`.
    /// Pure span computation for `read_blocks_window`: how many whole
    /// blocks from `start_idx` fit in `max_bytes` (always at least one).
    /// Returns `(end_idx_exclusive, win_start_rel, win_len)`.
    fn window_span(&self, start_idx: usize, max_bytes: u32) -> Result<(usize, u32, u32)> {
        let n = self.block_offsets.len();
        if start_idx >= n {
            return Err(anyhow!("window start {start_idx} out of range (total={n})"));
        }
        let first = &self.block_offsets[start_idx];
        let win_start_rel = first.relative_offset;
        let mut end_rel = first
            .relative_offset
            .checked_add(first.block_len)
            .ok_or_else(|| anyhow!("block {start_idx}: offset overflows u32"))?;
        let mut end_idx = start_idx + 1;
        while end_idx < n {
            let bo = &self.block_offsets[end_idx];
            // coco P1: block offsets must be monotonically
            // non-overlapping. A CRC-valid but semantically corrupt
            // MetaBlock with a REGRESSING offset would otherwise underflow
            // the budget check (masked by saturating_sub) and wrap
            // `win_len` into a huge bogus read.
            if bo.relative_offset < end_rel {
                return Err(anyhow!(
                    "corrupt meta: block {end_idx} offset {} regresses below \
                     previous block end {end_rel}",
                    bo.relative_offset
                ));
            }
            let e = bo
                .relative_offset
                .checked_add(bo.block_len)
                .ok_or_else(|| anyhow!("block {end_idx}: offset overflows u32"))?;
            // e >= relative_offset >= end_rel >= win_start_rel ⇒ no underflow.
            if e - win_start_rel > max_bytes {
                break;
            }
            end_rel = e;
            end_idx += 1;
        }
        Ok((end_idx, win_start_rel, end_rel - win_start_rel))
    }

    pub async fn read_blocks_window(
        &self,
        start_idx: usize,
        max_bytes: u32,
        sc: &Rc<StreamClient>,
    ) -> Result<(Bytes, usize, u32)> {
        let (end_idx, win_start_rel, win_len) = self.window_span(start_idx, max_bytes)?;
        let end_rel = win_start_rel + win_len;
        match &self.source {
            SstSource::Resident(d) => {
                let start = self.sst_base as usize + win_start_rel as usize;
                let end = start + win_len as usize;
                if end > d.len() {
                    return Err(anyhow!(
                        "window out of bounds: start={start} end={end} data_len={}",
                        d.len()
                    ));
                }
                Ok((d.slice(start..end), end_idx, win_start_rel))
            }
            SstSource::Paged {
                extent_id,
                base_in_extent,
                len_in_extent,
            } => {
                // Same bounds discipline as read_block_via (coco P2): the
                // window must lie inside this SST's extent slice — a corrupt
                // MetaBlock must not read a neighbour SST's bytes.
                let in_bounds = self
                    .sst_base
                    .checked_add(end_rel as u64)
                    .is_some_and(|e| e <= *len_in_extent);
                if !in_bounds {
                    return Err(anyhow!(
                        "paged window out of SST bounds (corrupt meta?): \
                         sst_base={} win=[{},{}) sst_len={}",
                        self.sst_base,
                        win_start_rel,
                        end_rel,
                        len_in_extent
                    ));
                }
                let abs = base_in_extent
                    .checked_add(self.sst_base)
                    .and_then(|v| v.checked_add(win_start_rel as u64))
                    .ok_or_else(|| anyhow!("window absolute offset overflows u64"))?;
                let (raw, _end) = sc
                    .read_bytes_from_extent(*extent_id, abs, win_len as u64)
                    .await?;
                if raw.len() != win_len as usize {
                    return Err(anyhow!(
                        "paged window short read: extent={extent_id} off={abs} \
                         need={win_len} got={}",
                        raw.len()
                    ));
                }
                Ok((Bytes::from(raw), end_idx, win_start_rel))
            }
        }
    }

    /// decode block `idx` out of a window previously returned by
    /// `read_blocks_window` (whose `win_start_rel` must be passed back).
    pub fn decode_block_from_window(
        &self,
        win: &Bytes,
        win_start_rel: u32,
        idx: usize,
    ) -> Result<Arc<DecodedBlock>> {
        let bo = self.block_offsets.get(idx).ok_or_else(|| {
            anyhow!(
                "block index {idx} out of range (total={})",
                self.block_offsets.len()
            )
        })?;
        let start = bo
            .relative_offset
            .checked_sub(win_start_rel)
            .ok_or_else(|| anyhow!("block {idx} precedes window start"))?
            as usize;
        let end = start + bo.block_len as usize;
        if end > win.len() {
            return Err(anyhow!(
                "block {idx} exceeds window: start={start} end={end} win_len={}",
                win.len()
            ));
        }
        Ok(Arc::new(DecodedBlock::decode(win.slice(start..end), &bo.key)?))
    }

    /// Find the block index whose base key is <= `target_key` (internal-key
    /// comparator order) using binary search. Returns the index of the block
    /// that could contain `target_key`.
    pub fn find_block_for_key(&self, target_key: &[u8]) -> usize {
        if self.block_offsets.is_empty() {
            return 0;
        }
        // Binary search: find the last block whose base key <= target_key.
        let mut lo = 0usize;
        let mut hi = self.block_offsets.len();
        while lo + 1 < hi {
            let mid = lo + (hi - lo) / 2;
            if crate::cmp_internal_keys(&self.block_offsets[mid].key, target_key).is_le() {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        lo
    }
}

// ---------------------------------------------------------------------------
// tests — window-span math + window block decode (Resident fixture;
// the paged RPC path is covered e2e)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod window_tests {
    use super::*;
    use crate::sstable::builder::SstBuilder;

    fn ikey(user: &[u8], seq: u64) -> Vec<u8> {
        crate::key_with_ts(user, seq)
    }

    /// Multi-block SST: >1000 entries forces several blocks (per-block
    /// entry cap), exercising real block boundaries.
    fn multi_block_reader() -> SstReader {
        let mut b = SstBuilder::new(0, 0);
        for i in 0..3500u32 {
            let user = format!("key{i:06}");
            b.add(&ikey(user.as_bytes(), i as u64 + 1), 1, b"v", 0);
        }
        SstReader::from_bytes(Bytes::from(b.finish())).expect("reader")
    }

    #[test]
    fn window_span_budget_and_floor() {
        let r = multi_block_reader();
        let n = r.block_count();
        assert!(n >= 3, "fixture must be multi-block, got {n}");

        // Huge budget → one window covers every block.
        let (end, start_rel, len) = r.window_span(0, u32::MAX).unwrap();
        assert_eq!(end, n);
        assert_eq!(start_rel, r.block_offsets[0].relative_offset);
        let last = &r.block_offsets[n - 1];
        assert_eq!(len, last.relative_offset + last.block_len - start_rel);

        // Budget below one block → still exactly one block (floor).
        let (end, _, len) = r.window_span(0, 1).unwrap();
        assert_eq!(end, 1);
        assert_eq!(len, r.block_offsets[0].block_len);

        // Budget for ~2 blocks starting mid-table.
        let b1 = &r.block_offsets[1];
        let b2 = &r.block_offsets[2];
        let two = b2.relative_offset + b2.block_len - b1.relative_offset;
        let (end, start_rel, len) = r.window_span(1, two).unwrap();
        assert_eq!(end, 3);
        assert_eq!(start_rel, b1.relative_offset);
        assert_eq!(len, two);

        // Past-the-end start errors.
        assert!(r.window_span(n, u32::MAX).is_err());
    }

    #[test]
    fn decode_block_from_window_matches_direct_read() {
        let r = multi_block_reader();
        let n = r.block_count();
        let data = match &r.source {
            SstSource::Resident(d) => d.clone(),
            SstSource::Paged { .. } => unreachable!(),
        };

        // Manually slice a window over blocks [1, 3) and decode both
        // blocks out of it; entries must match the direct read_block path.
        let (end, start_rel, len) = r.window_span(1, {
            let b1 = &r.block_offsets[1];
            let b2 = &r.block_offsets[2];
            b2.relative_offset + b2.block_len - b1.relative_offset
        })
        .unwrap();
        assert_eq!(end, 3.min(n));
        let win = data.slice(
            r.sst_base as usize + start_rel as usize
                ..r.sst_base as usize + start_rel as usize + len as usize,
        );
        for idx in 1..end {
            let from_win = r.decode_block_from_window(&win, start_rel, idx).unwrap();
            let direct = r.read_block(idx).unwrap();
            assert_eq!(from_win.num_entries(), direct.num_entries());
            assert_eq!(
                from_win.get_entry(0).unwrap().0,
                direct.get_entry(0).unwrap().0,
                "block {idx} first key mismatch"
            );
        }

        // A block before the window start is rejected.
        assert!(r.decode_block_from_window(&win, start_rel, 0).is_err());
    }
}
