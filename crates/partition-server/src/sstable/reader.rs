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

/// F261 — where this SSTable's data blocks live.
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
        base_in_extent: u32,
        len_in_extent: u32,
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
    pub vp_offset: u32,
    pub vp_deps: Vec<u64>,
    estimated_size: u64,
    pub discards: HashMap<u64, i64>,
    /// Earliest non-zero expires_at across all entries (0 = no expiring keys).
    pub min_expires_at: u64,
    sst_base: u32,
    /// Decoded block cache — avoids re-decoding (CRC + memcpy) on repeated reads.
    /// Mutex (not RefCell) so SstReader is Sync and can be shared across
    /// P-log/P-bulk via Arc without the unsafe Rc→Arc transmute that the
    /// codebase carried pre-F092. In practice only P-log reads blocks; P-bulk
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
    pub fn open_at(data: Bytes, sst_base: u32) -> Result<Self> {
        let base = sst_base as usize;
        if data.len() < base + 8 {
            return Err(anyhow!("SSTable too short at base={sst_base}"));
        }
        let sst_end = data.len();
        Self::parse(data, base, sst_end)
    }

    /// Open from a slice: data[sst_base..sst_base+sst_len].
    #[allow(dead_code)]
    pub fn open_slice(data: Bytes, sst_base: u32, sst_len: u32) -> Result<Self> {
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

    /// F261: construct a PAGED reader from just the MetaBlock bytes — the
    /// recovery path reads ONLY the meta tail off row_stream (two small
    /// reads) instead of materializing the whole SST (which kept recovery
    /// RSS at O(dataset): 9.4 GB of SSTs page-faulted ~25 GB transient).
    /// `meta_bytes` = the meta region EXCLUDING the trailing meta_len u32.
    pub fn open_paged_from_meta(
        meta_bytes: &[u8],
        extent_id: u64,
        base_in_extent: u32,
        len_in_extent: u32,
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
            sst_base: sst_base as u32,
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
                    "block {idx}: SST is paged (F261) — use read_block_via / materialize"
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

    /// F261: convert a freshly parsed reader into PAGED mode — the resident
    /// bytes are dropped (memory returned) and future block reads go through
    /// `read_block_via`. `base_in_extent` is where this SST starts inside
    /// `extent_id` on row_stream (`TableMeta.offset`), `len_in_extent` its
    /// byte length (`TableMeta.len`).
    pub fn into_paged(mut self, extent_id: u64, base_in_extent: u32, len_in_extent: u32) -> Self {
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

    pub fn is_paged(&self) -> bool {
        matches!(self.source, SstSource::Paged { .. })
    }

    /// F261: `(extent_id, base, len)` when paged.
    pub fn paged_loc(&self) -> Option<(u64, u32, u32)> {
        match self.source {
            SstSource::Paged {
                extent_id,
                base_in_extent,
                len_in_extent,
            } => Some((extent_id, base_in_extent, len_in_extent)),
            SstSource::Resident(_) => None,
        }
    }

    /// F261: block read that works for BOTH modes. Resident → the sync path;
    /// paged → bounded global cache, miss fetched from row_stream via `sc`
    /// (rides the F258 replica rotation). The await happens with NO RefCell
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
        // coco P2 (F261): checked arithmetic + SST-window bound. The resident
        // path is protected by its `end > data.len()` check; the paged path
        // must equivalently refuse a corrupt/foreign MetaBlock whose block
        // offsets point outside `[base, base + len_in_extent)` — otherwise a
        // bad offset reads a NEIGHBOURING SST's bytes from the same row
        // extent (or u32-wraps to an arbitrary offset) and serves them as
        // this SST's block.
        let in_bounds = bo
            .relative_offset
            .checked_add(bo.block_len)
            .and_then(|e| self.sst_base.checked_add(e))
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
            .and_then(|v| v.checked_add(bo.relative_offset))
            .ok_or_else(|| anyhow!("paged block {idx}: absolute offset overflows u32"))?;
        let key = (extent_id, abs);
        if let Some(b) = cache.get(key) {
            return Ok(b);
        }
        let (raw, _end) = sc
            .read_bytes_from_extent(extent_id, abs, bo.block_len)
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

    /// F261: fetch the whole SST and return a RESIDENT reader for sync
    /// iteration (compaction / split key-scan / diag fullscan). Transient —
    /// caller drops it after the scan; concurrency is bounded by the
    /// existing compact/GC ConcurrencyController gates at the call sites.
    pub async fn materialize(&self, sc: &Rc<StreamClient>) -> Result<SstReader> {
        match self.source {
            SstSource::Resident(ref d) => SstReader::open_at(d.clone(), self.sst_base),
            SstSource::Paged {
                extent_id,
                base_in_extent,
                len_in_extent,
            } => {
                let (raw, _end) = sc
                    .read_bytes_from_extent(extent_id, base_in_extent, len_in_extent)
                    .await?;
                SstReader::from_bytes(Bytes::from(raw))
            }
        }
    }

    /// Find the block index whose base key is <= `target_key` using binary search.
    /// Returns the index of the block that could contain `target_key`.
    pub fn find_block_for_key(&self, target_key: &[u8]) -> usize {
        if self.block_offsets.is_empty() {
            return 0;
        }
        // Binary search: find the last block whose base key <= target_key.
        let mut lo = 0usize;
        let mut hi = self.block_offsets.len();
        while lo + 1 < hi {
            let mid = lo + (hi - lo) / 2;
            if self.block_offsets[mid].key.as_slice() <= target_key {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        lo
    }
}
