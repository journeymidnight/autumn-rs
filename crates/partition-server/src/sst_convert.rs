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

/// A meta-stream frame no build decodes as `TableLocations` (8 bytes; any
/// archived checkpoint is longer). The tool appends it right after the
/// checkpoint it republishes when the meta stream it read was not intact — a
/// frame that failed to decode, or a partial tail. Recovery trusts the
/// manager's covered-prefix marker only on an intact meta stream, because a
/// checkpoint recovered past a bad newer frame may be older than the one the
/// marker vouches for; republishing it as the only frame would hide that.
pub const NOT_INTACT_FRAME: &[u8] = b"AUCVNI01";

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

    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut f = (payload.len() as u32).to_le_bytes().to_vec();
        f.extend_from_slice(payload);
        f
    }

    /// Recovery reads a republished checkpoint followed by the marker frame
    /// as that checkpoint, from a meta stream that is NOT intact.
    #[test]
    fn the_not_intact_frame_keeps_the_checkpoint_and_disables_the_marker() {
        let ckpt = autumn_rpc::partition_rpc::TableLocations {
            locs: vec![],
            vp_extent_id: 7,
            vp_offset: 99,
            log_extent_count: 1,
            fence_floors: vec![],
        };
        let mut data = frame(&crate::rkyv_encode(&ckpt));
        let (_, intact) = crate::decode_last_table_checkpoint_with_health(&data).unwrap();
        assert!(intact);
        data.extend_from_slice(&frame(NOT_INTACT_FRAME));
        let (got, intact) = crate::decode_last_table_checkpoint_with_health(&data).unwrap();
        assert_eq!((got.vp_extent_id, got.vp_offset), (7, 99));
        assert!(!intact);
    }

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
