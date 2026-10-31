use std::collections::{BTreeSet, HashMap};

use bytes::Bytes;

use super::bloom::BloomFilterBuilder;
use super::format::{
    BlockOffset, EntryHeader, MetaBlock, BLOCK_SIZE_TARGET, ENTRY_VALUE_HEADER,
    MAX_ENTRIES_PER_BLOCK,
};


/// Builds a block-based SSTable in memory.
///
/// Call `add()` for each entry (internal keys in sorted order), then `finish()`
/// for the table as the segments its blocks were written into, appended as
/// they are (`append_segments`): no buffer ever holds the whole table.
///
/// SSTable layout:
///   [Block 0][Block 1]...[Block N][MetaBlock bytes][meta_len: u32 LE]
pub struct SstBuilder {
    /// Full segments: runs of whole blocks, each at least `segment_bytes`.
    segments: Vec<Vec<u8>>,
    /// The segment being written; the open block is its tail from `block_start`.
    seg: Vec<u8>,
    block_start: usize,
    segment_bytes: usize,
    block_offsets: Vec<BlockOffset>,
    running_offset: u32,

    // Current block state
    entry_offsets: Vec<u32>, // byte offsets of entries in the open block
    base_key: Vec<u8>,       // first key in current block

    // Bloom filter over all entries
    bloom: BloomFilterBuilder,

    // Metadata
    seq_num: u64,
    vp_extent_id: u64,
    vp_offset: u64,
    smallest_key: Vec<u8>,
    biggest_key: Vec<u8>,
    total_raw_bytes: u64,
    /// Discard stats to embed in MetaBlock (set via set_discards before finish()).
    discards: HashMap<u64, i64>,
    /// Distinct old log extents referenced by live ValuePointer entries.
    vp_deps: BTreeSet<u64>,
    /// Earliest non-zero expires_at seen across all entries.
    min_expires_at: u64,
    num_entries: u64,
    num_deletions: u64,
}

impl SstBuilder {
    /// `expected_keys` sizes the bloom filter (1% FPR at that many keys); a
    /// table holding more keys degrades its filter, never its correctness.
    pub fn new(vp_extent_id: u64, vp_offset: u64, expected_keys: usize) -> Self {
        SstBuilder {
            segments: Vec::new(),
            seg: Vec::new(),
            block_start: 0,
            segment_bytes: crate::params().sst_segment_bytes.get(),
            block_offsets: Vec::new(),
            running_offset: 0,
            entry_offsets: Vec::new(),
            base_key: Vec::new(),
            bloom: BloomFilterBuilder::new(expected_keys, crate::params().sst_bloom_fp_rate.get()),
            seq_num: 0,
            vp_extent_id,
            vp_offset,
            smallest_key: Vec::new(),
            biggest_key: Vec::new(),
            total_raw_bytes: 0,
            discards: HashMap::new(),
            vp_deps: BTreeSet::new(),
            min_expires_at: 0,
            num_entries: 0,
            num_deletions: 0,
        }
    }

    /// Segments of `n` bytes instead of `--sst-segment-bytes`.
    #[cfg(test)]
    pub fn with_segment_bytes(mut self, n: usize) -> Self {
        self.segment_bytes = n;
        self
    }

    /// Set the discard map to embed in this SSTable's MetaBlock.
    /// Should be called before `finish()`.
    pub fn set_discards(&mut self, discards: HashMap<u64, i64>) {
        self.discards = discards;
    }

    /// Raise the table's seq to at least `seq`. A table's seq is the newest seq
    /// it accounts for, which a compaction output sets to its inputs' newest:
    /// the entries it dropped were written at those seqs and may still be in
    /// the log.
    pub fn cover_seq(&mut self, seq: u64) {
        self.seq_num = self.seq_num.max(seq);
    }

    /// Add one entry to the SSTable.
    ///
    /// `internal_key` is a MVCC key (user_key ++ 8-byte inverted-seq suffix);
    /// callers must add entries in `cmp_internal_keys` order.
    /// `op` = 1 for put, 2 for delete, 0x81 (1|OP_VALUE_POINTER) for value-pointer.
    /// `value` bytes are appended verbatim (may be actual value or 16-byte ValuePointer).
    /// `expires_at` = 0 means no expiry.
    pub fn add(&mut self, internal_key: &[u8], op: u8, value: &[u8], expires_at: u64) {
        // Estimate if adding this entry would exceed the block size target.
        let entry_size = EntryHeader::SIZE + internal_key.len() + ENTRY_VALUE_HEADER + value.len();
        if !self.entry_offsets.is_empty()
            && (self.block_len() + entry_size > BLOCK_SIZE_TARGET
                || self.entry_offsets.len() >= MAX_ENTRIES_PER_BLOCK)
        {
            self.finish_block();
        }
        if self.seg.capacity() == 0 {
            // Once per segment, so it does not grow by doubling (each doubling
            // copies everything written so far); only the last may grow once,
            // for the MetaBlock.
            self.seg.reserve(self.segment_bytes);
        }

        // Track metadata
        self.num_entries += 1;
        if op & 0x7f == crate::OP_TOMBSTONE {
            self.num_deletions += 1;
        }
        let ts = crate::parse_ts(internal_key);
        if ts > self.seq_num {
            self.seq_num = ts;
        }

        if expires_at > 0 && (self.min_expires_at == 0 || expires_at < self.min_expires_at) {
            self.min_expires_at = expires_at;
        }

        if self.smallest_key.is_empty() {
            self.smallest_key = internal_key.to_vec();
        }
        self.biggest_key = internal_key.to_vec();

        // Bloom filter: hash on user key only (strip the fixed-width suffix)
        self.bloom.add_user_key(crate::parse_key(internal_key));

        // Prefix compression
        let overlap = if self.base_key.is_empty() {
            0usize
        } else {
            common_prefix_len(&self.base_key, internal_key)
        };
        let diff_key = &internal_key[overlap..];

        if self.base_key.is_empty() {
            self.base_key = internal_key.to_vec();
        }

        // Record entry offset (from start of current block)
        self.entry_offsets.push(self.block_len() as u32);

        // Write entry: [header:4][diff_key][op:1][val_len:4][expires_at:8][value]
        let hdr = EntryHeader {
            overlap: overlap as u16,
            diff_len: diff_key.len() as u16,
        };
        self.seg.extend_from_slice(&hdr.encode());
        self.seg.extend_from_slice(diff_key);
        self.seg.push(op);
        self.seg.extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.seg.extend_from_slice(&expires_at.to_le_bytes());
        self.seg.extend_from_slice(value);

        if op & crate::OP_VALUE_POINTER != 0 && value.len() >= crate::VALUE_POINTER_SIZE {
            let extent_id = u64::from_le_bytes(value[0..8].try_into().unwrap());
            self.vp_deps.insert(extent_id);
        }

        self.total_raw_bytes += entry_size as u64;
    }

    /// Encoded bytes so far: finished blocks plus the open block and its
    /// offset table (the MetaBlock, bloom and index come on top at `finish`).
    pub fn encoded_len(&self) -> u64 {
        self.running_offset as u64 + self.block_len() as u64 + 4 * self.entry_offsets.len() as u64
    }

    /// Returns `true` if no entries have been added yet.
    pub fn is_empty(&self) -> bool {
        self.block_offsets.is_empty() && self.entry_offsets.is_empty()
    }

    /// Bytes of the open block written so far (entries, before its footer).
    fn block_len(&self) -> usize {
        self.seg.len() - self.block_start
    }

    /// Finalise the SSTable: its segments, in order, the MetaBlock and its
    /// length closing the last.
    pub fn finish(mut self) -> BuiltSst {
        // Flush last block
        if !self.entry_offsets.is_empty() {
            self.finish_block();
        }

        let bloom_filter = self.bloom.finish();
        let meta = MetaBlock {
            block_offsets: self.block_offsets,
            bloom_data: bloom_filter.encode(),
            smallest_key: self.smallest_key,
            biggest_key: self.biggest_key,
            estimated_size: self.total_raw_bytes,
            seq_num: self.seq_num,
            vp_extent_id: self.vp_extent_id,
            vp_offset: self.vp_offset,
            vp_deps: self.vp_deps.into_iter().collect(),
            discards: self.discards,
            min_expires_at: self.min_expires_at,
            num_entries: self.num_entries,
            num_deletions: self.num_deletions,
        };
        let meta_bytes = meta.encode();
        let meta_len = meta_bytes.len() as u32;
        let meta_at = self.seg.len();
        self.seg.extend_from_slice(&meta_bytes);
        // Trailer: meta_len (4 bytes LE). Used by reader to locate the MetaBlock.
        self.seg.extend_from_slice(&meta_len.to_le_bytes());
        let last = Bytes::from(std::mem::take(&mut self.seg));
        let meta = last.slice(meta_at..meta_at + meta_bytes.len());
        let mut segments: Vec<Bytes> = self.segments.drain(..).map(Bytes::from).collect();
        segments.push(last);
        BuiltSst { segments, meta }
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    fn finish_block(&mut self) {
        if self.entry_offsets.is_empty() {
            return;
        }
        let block_start_offset = self.running_offset;

        // Append footer: [entry_offsets * 4B LE][num_entries: 4B LE]
        for &off in &self.entry_offsets {
            self.seg.extend_from_slice(&off.to_le_bytes());
        }
        let num = self.entry_offsets.len() as u32;
        self.seg.extend_from_slice(&num.to_le_bytes());

        // CRC32C over everything so far (entries + offsets + num_entries)
        let crc = crc32c::crc32c(&self.seg[self.block_start..]);
        self.seg.extend_from_slice(&crc.to_le_bytes());

        let block_len = self.block_len() as u32;
        let base_key = std::mem::take(&mut self.base_key);

        self.block_offsets.push(BlockOffset {
            key: base_key,
            relative_offset: block_start_offset,
            block_len,
        });

        self.running_offset += block_len;
        self.entry_offsets.clear();
        // Closed while it can no longer take a whole block, so it stays within
        // `segment_bytes` (a UCX send cuts anything larger into pieces).
        if self.seg.len() + MAX_BLOCK_BYTES > self.segment_bytes {
            self.segments.push(std::mem::take(&mut self.seg));
        }
        self.block_start = self.seg.len();
    }
}

/// A finished SSTable: `segments` in order are its bytes.
pub struct BuiltSst {
    pub segments: Vec<Bytes>,
    /// The MetaBlock (without the trailing length), a view into the last segment.
    meta: Bytes,
}

impl BuiltSst {
    pub fn len(&self) -> u64 {
        self.segments.iter().map(|s| s.len() as u64).sum()
    }

    /// The MetaBlock, for `SstReader::open_paged_from_meta`.
    pub fn meta(&self) -> &[u8] {
        &self.meta
    }

    /// The whole table in one buffer (a copy), for a reader over resident bytes.
    #[cfg(test)]
    pub fn to_bytes(&self) -> Bytes {
        if let [one] = self.segments.as_slice() {
            return one.clone();
        }
        let mut all = Vec::with_capacity(self.len() as usize);
        for s in &self.segments {
            all.extend_from_slice(s);
        }
        Bytes::from(all)
    }
}

/// The largest ordinary block: its entries up to `BLOCK_SIZE_TARGET` (an entry
/// holds a ValuePointer or a value of at most 4 KiB) and its footer. A block
/// with one huge key can be larger; its segment then goes in two pieces.
const MAX_BLOCK_BYTES: usize = BLOCK_SIZE_TARGET + 4 * MAX_ENTRIES_PER_BLOCK + 8;

/// Length of the common prefix of two byte slices.
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::iterator::TableIterator;
    use crate::sstable::reader::SstReader;

    fn ikey(user_key: &[u8], seq: u64) -> Vec<u8> {
        crate::key_with_ts(user_key, seq)
    }

    #[test]
    fn build_and_read_round_trip() {
        let mut b = SstBuilder::new(0, 0, 1024);
        // Add entries in sorted order (higher seq = sorts first for same user key)
        let keys = vec![
            (b"apple".as_ref(), 10u64),
            (b"banana".as_ref(), 5u64),
            (b"cherry".as_ref(), 20u64),
        ];
        for (uk, seq) in &keys {
            b.add(&ikey(uk, *seq), 1, b"value", 0);
        }
        let data = b.finish().to_bytes().to_vec();
        assert!(!data.is_empty());

        let reader = SstReader::from_bytes(bytes::Bytes::from(data)).expect("reader");
        assert_eq!(
            reader.block_count(),
            1,
            "should be one block for 3 small entries"
        );
        assert_eq!(reader.seq_num(), 20);
        assert!(!reader.smallest_key().is_empty());
    }

    #[test]
    fn crc_corruption_detected() {
        let mut b = SstBuilder::new(0, 0, 1024);
        b.add(&ikey(b"key", 1), 1, b"val", 0);
        let mut data = b.finish().to_bytes().to_vec();
        // Corrupt a byte in the first block
        if data.len() > 5 {
            data[5] ^= 0xFF;
        }
        // Reading should fail with a CRC error
        let reader = SstReader::from_bytes(bytes::Bytes::from(data)).expect("reader opens");
        // Trying to read the first block should produce a CRC error
        let result = reader.read_block(0);
        assert!(result.is_err(), "expected CRC error");
    }

    #[test]
    fn discards_round_trip() {
        let mut b = SstBuilder::new(42, 100, 1024);
        b.add(&ikey(b"key", 1), 1, b"val", 0);
        let mut discards = HashMap::new();
        discards.insert(10u64, 500i64);
        discards.insert(20u64, 1024i64);
        b.set_discards(discards.clone());
        let data = b.finish().to_bytes().to_vec();

        let reader = SstReader::from_bytes(bytes::Bytes::from(data)).expect("reader");
        assert_eq!(reader.discards.get(&10), Some(&500i64));
        assert_eq!(reader.discards.get(&20), Some(&1024i64));
        assert_eq!(reader.discards.len(), 2);
    }

    #[test]
    fn min_expires_at_with_expiry() {
        let mut b = SstBuilder::new(0, 0, 1024);
        b.add(&ikey(b"a", 1), 1, b"v1", 1000);
        b.add(&ikey(b"b", 2), 1, b"v2", 500);
        b.add(&ikey(b"c", 3), 1, b"v3", 0); // no expiry
        b.add(&ikey(b"d", 4), 1, b"v4", 800);
        let data = b.finish().to_bytes().to_vec();
        let reader = SstReader::from_bytes(bytes::Bytes::from(data)).expect("reader");
        assert_eq!(reader.min_expires_at, 500);
    }

    #[test]
    fn mixed_gallery_like_keys_round_trip() {
        let mut b = SstBuilder::new(0, 0, 1024);
        let keys = [
            b".thumb/320/Patreon--leeesovely-October-2024-MissKON.com-241.jpg".as_ref(),
            b".thumb/320/Patreon--leeesovely-October-2024-MissKON.com-299.jpg".as_ref(),
            b"Patreon--leeesovely-October-2024-MissKON.com-003.jpg".as_ref(),
            b"Patreon--leeesovely-October-2024-MissKON.com-300.jpg".as_ref(),
            b"Patreon--leeesovely-October-2024-MissKON.com-301.jpg".as_ref(),
        ];
        for (seq, key) in keys.iter().enumerate() {
            b.add(&ikey(key, seq as u64 + 1), 1, b"value", 0);
        }

        let data = b.finish().to_bytes().to_vec();
        let reader =
            std::sync::Arc::new(SstReader::from_bytes(bytes::Bytes::from(data)).expect("reader"));
        let mut it = TableIterator::new(reader);
        it.rewind();

        let mut seen = Vec::new();
        while let Some(item) = it.item() {
            seen.push(crate::parse_key(&item.key).to_vec());
            it.next();
        }

        assert_eq!(seen, keys.iter().map(|k| k.to_vec()).collect::<Vec<_>>());
    }

    #[test]
    fn gallery_like_rightmost_slice_round_trip() {
        let mut b = SstBuilder::new(0, 0, 1024);
        let mut keys: Vec<Vec<u8>> = (241..=301)
            .map(|i| {
                format!(".thumb/320/Patreon--leeesovely-October-2024-MissKON.com-{i:03}.jpg")
                    .into_bytes()
            })
            .collect();
        keys.push(b"Patreon--leeesovely-October-2024-MissKON.com-003.jpg".to_vec());
        keys.extend((276..=301).map(|i| {
            format!("Patreon--leeesovely-October-2024-MissKON.com-{i:03}.jpg").into_bytes()
        }));

        for (seq, key) in keys.iter().enumerate() {
            b.add(&ikey(key, seq as u64 + 1), 1, b"value", 0);
        }

        let data = b.finish().to_bytes().to_vec();
        let reader =
            std::sync::Arc::new(SstReader::from_bytes(bytes::Bytes::from(data)).expect("reader"));
        let mut it = TableIterator::new(reader);
        it.rewind();

        let mut seen = Vec::new();
        while let Some(item) = it.item() {
            seen.push(crate::parse_key(&item.key).to_vec());
            it.next();
        }

        assert_eq!(seen, keys);
    }

    #[test]
    fn the_meta_block_counts_entries_and_deletions() {
        let mut b = SstBuilder::new(0, 0, 1024);
        let mut vp = vec![0u8; crate::VALUE_POINTER_SIZE];
        vp[0] = 7;
        b.add(&ikey(b"a", 9), 1, b"v", 0);
        b.add(&ikey(b"a", 3), 1, b"old", 0);
        b.add(&ikey(b"b", 8), crate::OP_TOMBSTONE, b"", 0);
        b.add(&ikey(b"b", 2), 1 | crate::OP_VALUE_POINTER, &vp, 0);
        b.add(&ikey(b"c", 7), crate::OP_TOMBSTONE, b"", 0);
        let reader = SstReader::from_bytes(b.finish().to_bytes()).expect("reader");
        assert_eq!(reader.num_entries, 5);
        assert_eq!(reader.num_deletions, 2);

        let empty = SstReader::from_bytes(SstBuilder::new(0, 0, 1024).finish().to_bytes())
            .expect("empty reader");
        assert_eq!((empty.num_entries, empty.num_deletions), (0, 0));
    }

    /// A table written into small segments is the table written into one:
    /// every segment ends on a block boundary, and a reader opened from the
    /// meta alone sees the blocks a reader of the whole bytes does.
    #[test]
    fn segments_join_into_the_same_table() {
        let build = |segment_bytes: Option<usize>| {
            let mut b = SstBuilder::new(7, 9, 20_000);
            if let Some(n) = segment_bytes {
                b = b.with_segment_bytes(n);
            }
            for i in 0..20_000u32 {
                let key = crate::key_with_ts(format!("key-{i:08}").as_bytes(), u64::from(i) + 1);
                b.add(&key, crate::OP_PUT, &vec![(i % 251) as u8; 200 + (i % 7) as usize * 40], 0);
            }
            b.finish()
        };
        let whole = build(Some(64 << 20)); // one segment for this table
        let cut = build(Some(1 << 20));
        assert_eq!(whole.segments.len(), 1);
        assert!(cut.segments.len() >= 4, "{} segments", cut.segments.len());
        assert_eq!(cut.to_bytes(), whole.to_bytes());
        let meta = MetaBlock::decode(cut.meta()).expect("meta");
        assert_eq!(meta.num_entries, 20_000);
        let ends: Vec<u64> =
            meta.block_offsets.iter().map(|o| u64::from(o.relative_offset) + u64::from(o.block_len)).collect();
        let mut at = 0u64;
        for seg in &cut.segments[..cut.segments.len() - 1] {
            assert!(seg.len() <= 1 << 20, "a segment stays within its size: {}", seg.len());
            assert!(seg.len() + 2 * MAX_BLOCK_BYTES > 1 << 20, "and closes only when a block no longer fits: {}", seg.len());
            at += seg.len() as u64;
            assert!(ends.contains(&at), "segment ends at {at}, inside a block");
        }
        let resident = SstReader::from_bytes(whole.to_bytes()).expect("reader");
        let paged = SstReader::open_paged_from_meta(cut.meta(), 3, 0, cut.len()).expect("paged reader");
        assert_eq!(paged.block_count(), resident.block_count());
        assert_eq!((paged.smallest_key(), paged.biggest_key()), (resident.smallest_key(), resident.biggest_key()));
    }
}
