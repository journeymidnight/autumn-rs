//! Support for the one-off `convert_sst` tool, which rewrites every SST of a
//! stopped cluster into the current MetaBlock format. Delete this module with
//! the tool.
//!
//! It knows nothing about earlier formats: the tool parses the old MetaBlock
//! itself and hands over the data blocks, whose layout did not change. What
//! comes back is exactly what the current builder writes for those entries.

use std::collections::HashMap;

use anyhow::Result;
use bytes::Bytes;

use crate::sstable::format::DecodedBlock;
use crate::sstable::{SstBuilder, SstReader};

/// A rebuilt SST and the fields the tool compares against the old MetaBlock.
pub struct RebuiltSst {
    pub bytes: Vec<u8>,
    pub seq_num: u64,
    pub smallest_key: Vec<u8>,
    pub biggest_key: Vec<u8>,
    pub estimated_size: u64,
    pub vp_deps: Vec<u64>,
    pub min_expires_at: u64,
    pub num_entries: u64,
    pub num_deletions: u64,
}

/// Rebuild one SST from its data blocks, in order: each is the raw block
/// bytes (entries, offsets footer, CRC) and the base key its index entry
/// named. `vp_extent_id` / `vp_offset` / `discards` are carried over as given
/// (they describe the flush or compaction that wrote the SST, not its entries).
pub fn rebuild_sst(
    blocks: &[(Bytes, Vec<u8>)],
    vp_extent_id: u64,
    vp_offset: u64,
    discards: HashMap<u64, i64>,
) -> Result<RebuiltSst> {
    let mut builder = SstBuilder::new(vp_extent_id, vp_offset);
    for (raw, base_key) in blocks {
        let block = DecodedBlock::decode(raw.clone(), base_key)?;
        for idx in 0..block.num_entries() {
            let (key, op, value, expires_at) = block.get_entry(idx)?;
            builder.add(&key, op, &value, expires_at);
        }
    }
    builder.set_discards(discards);
    let bytes = builder.finish();
    let reader = SstReader::from_bytes(Bytes::from(bytes.clone()))?;
    Ok(RebuiltSst {
        seq_num: reader.seq_num(),
        smallest_key: reader.smallest_key.clone(),
        biggest_key: reader.biggest_key.clone(),
        estimated_size: reader.estimated_size(),
        vp_deps: reader.vp_deps.clone(),
        min_expires_at: reader.min_expires_at,
        num_entries: reader.num_entries,
        num_deletions: reader.num_deletions,
        bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rebuilding an SST from its own blocks reproduces its data blocks and
    /// MetaBlock fields; with at most one discard entry (the map's encode
    /// order is a `HashMap`'s) the whole SST comes back byte for byte.
    #[test]
    fn rebuilding_from_the_blocks_reproduces_the_sst() {
        let mut b = SstBuilder::new(9, 4096);
        for i in 0..3000u32 {
            let key = crate::key_with_ts(format!("k{i:06}").as_bytes(), 10_000 - i as u64);
            if i % 3 == 0 {
                b.add(&key, crate::OP_TOMBSTONE, b"", 0);
            } else {
                b.add(&key, 1, &vec![b'v'; 100], if i % 7 == 0 { 99 } else { 0 });
            }
        }
        let mut discards = HashMap::new();
        discards.insert(5, 777);
        b.set_discards(discards.clone());
        let original = b.finish();

        let n = original.len();
        let meta_len = u32::from_le_bytes(original[n - 4..].try_into().unwrap()) as usize;
        let meta = crate::sstable::format::MetaBlock::decode(&original[n - 4 - meta_len..n - 4])
            .unwrap();
        assert!(meta.block_offsets.len() > 1, "the test needs several blocks");
        let blocks: Vec<(Bytes, Vec<u8>)> = meta
            .block_offsets
            .iter()
            .map(|bo| {
                let start = bo.relative_offset as usize;
                (
                    Bytes::from(original[start..start + bo.block_len as usize].to_vec()),
                    bo.key.clone(),
                )
            })
            .collect();
        let rebuilt = rebuild_sst(&blocks, 9, 4096, discards).unwrap();
        assert_eq!(rebuilt.bytes, original);
        assert_eq!(rebuilt.num_entries, 3000);
        assert_eq!(rebuilt.num_deletions, 1000);
    }
}
