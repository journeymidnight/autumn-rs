//! At-rest content checksums for sealed extents.
//!
//! Every layer that owns a byte format checksums it — the partition layer's WAL
//! records and SST blocks, the RPC frame's header. The stream layer stores
//! opaque bytes and owns no format, so nothing here ever described its content:
//! `.meta`'s CRC32C covers its own 48 metadata bytes and never the `.dat`.
//!
//! That matters most in the repair paths, which run BELOW the layer holding the
//! checksums. Recovery's verify-after-fetch compares length and eversion, and a
//! bit flip moves neither, so a rebuilt replica is byte-identical to a corrupt
//! source. EC conversion encodes parity from whatever the coordinator reads.
//! Both make corruption authoritative before any consumer can notice.
//!
//! The sidecar is written once, when the extent seals, and is content-only: it
//! says nothing about which extent generation or layout is live, so it needs no
//! coordination with `.meta` beyond agreeing on the length it describes.

use crc32c::crc32c;

/// One checksummed unit.
///
/// Per block rather than per extent, for three reasons that all bite: a
/// whole-extent digest can only be checked by a whole-extent read, a scrub
/// could not say WHICH region rotted, and a multi-GiB extent could not be
/// hashed in bounded steps. At 1 MiB the sidecar costs 4 KiB per GiB.
pub(crate) const CK_BLOCK_BYTES: u64 = 1024 * 1024;

const CK_MAGIC: &[u8; 8] = b"EXTCKS\0\x01";
/// magic + extent_id + sealed_length + block_bytes + block_count.
const CK_HEADER_BYTES: usize = 8 + 8 + 8 + 4 + 4;
const CK_TRAILER_BYTES: usize = 4;

/// How many blocks cover `sealed_length`. Zero-length is zero blocks — a
/// sealed-empty extent is legal and has no content to describe.
pub(crate) fn block_count_for(sealed_length: u64, block_bytes: u64) -> usize {
    debug_assert!(block_bytes > 0);
    sealed_length.div_ceil(block_bytes) as usize
}

/// The byte range block `i` covers. The last block is short whenever
/// `sealed_length` is not a multiple of `block_bytes`.
///
/// Shared by the writer, the verifier and the tests on purpose: treating the
/// short tail block as a full one is the mistake that would make every extent
/// whose length is not a multiple of the block size look corrupt, and it should
/// only be possible to make it in one place.
pub(crate) fn block_range(i: usize, block_bytes: u64, sealed_length: u64) -> (u64, u64) {
    let start = (i as u64) * block_bytes;
    let end = start.saturating_add(block_bytes).min(sealed_length);
    (start, end)
}

/// What a read was checked against.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BlockMismatch {
    pub(crate) block: usize,
    pub(crate) offset: u64,
    pub(crate) expected: u32,
    pub(crate) found: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExtentChecksums {
    /// The content length these checksums describe. A sidecar whose length
    /// disagrees with the extent's seal describes different bytes and is not
    /// evidence about these ones.
    pub(crate) sealed_length: u64,
    pub(crate) block_bytes: u64,
    pub(crate) blocks: Vec<u32>,
}

impl ExtentChecksums {
    pub(crate) fn encode(&self, extent_id: u64) -> Vec<u8> {
        let mut buf =
            Vec::with_capacity(CK_HEADER_BYTES + self.blocks.len() * 4 + CK_TRAILER_BYTES);
        buf.extend_from_slice(CK_MAGIC);
        buf.extend_from_slice(&extent_id.to_le_bytes());
        buf.extend_from_slice(&self.sealed_length.to_le_bytes());
        buf.extend_from_slice(&(self.block_bytes as u32).to_le_bytes());
        buf.extend_from_slice(&(self.blocks.len() as u32).to_le_bytes());
        for c in &self.blocks {
            buf.extend_from_slice(&c.to_le_bytes());
        }
        let trailer = crc32c(&buf);
        buf.extend_from_slice(&trailer.to_le_bytes());
        buf
    }

    /// Parse a sidecar, or `None` if it is not one we may act on.
    ///
    /// Every rejection collapses to `None` on purpose. An unreadable or
    /// mismatched sidecar means "no evidence about this extent", never
    /// "this extent is corrupt" — the checksums are the thing under suspicion
    /// in that case, and condemning a replica on their say-so would turn a
    /// damaged 4 KiB sidecar into a rebuild of a healthy multi-GiB replica.
    /// `extent_id` is checked for the same reason `.meta` checks it: extent ids
    /// restart from small integers in the next cluster on the same host.
    pub(crate) fn decode(buf: &[u8], extent_id: u64) -> Option<Self> {
        if buf.len() < CK_HEADER_BYTES + CK_TRAILER_BYTES {
            return None;
        }
        if &buf[0..8] != CK_MAGIC {
            return None;
        }
        let stored_id = u64::from_le_bytes(buf[8..16].try_into().ok()?);
        if stored_id != extent_id {
            return None;
        }
        let sealed_length = u64::from_le_bytes(buf[16..24].try_into().ok()?);
        let block_bytes = u32::from_le_bytes(buf[24..28].try_into().ok()?) as u64;
        let block_count = u32::from_le_bytes(buf[28..32].try_into().ok()?) as usize;
        if block_bytes == 0 {
            return None;
        }
        if block_count != block_count_for(sealed_length, block_bytes) {
            return None;
        }
        let want_len = CK_HEADER_BYTES + block_count * 4 + CK_TRAILER_BYTES;
        if buf.len() != want_len {
            return None;
        }
        let body_end = CK_HEADER_BYTES + block_count * 4;
        let stored_crc = u32::from_le_bytes(buf[body_end..body_end + 4].try_into().ok()?);
        if crc32c(&buf[..body_end]) != stored_crc {
            return None;
        }
        let blocks = buf[CK_HEADER_BYTES..body_end]
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        Some(Self {
            sealed_length,
            block_bytes,
            blocks,
        })
    }

    /// Check the blocks this read FULLY covers.
    ///
    /// A partially covered block is skipped rather than reported: its remaining
    /// bytes are not in hand, so the only honest answer about it is silence.
    /// That is why a sub-block read verifies nothing and the scrub exists.
    pub(crate) fn verify_read(&self, offset: u64, data: &[u8]) -> Result<usize, BlockMismatch> {
        let read_end = offset.saturating_add(data.len() as u64);
        let mut checked = 0usize;
        let first = (offset / self.block_bytes) as usize;
        for i in first..self.blocks.len() {
            let (b_start, b_end) = block_range(i, self.block_bytes, self.sealed_length);
            if b_start >= read_end {
                break;
            }
            if b_start < offset || b_end > read_end {
                continue;
            }
            let from = (b_start - offset) as usize;
            let to = (b_end - offset) as usize;
            let found = crc32c(&data[from..to]);
            if found != self.blocks[i] {
                return Err(BlockMismatch {
                    block: i,
                    offset: b_start,
                    expected: self.blocks[i],
                    found,
                });
            }
            checked += 1;
        }
        Ok(checked)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Walks blocks exactly the way `write_extent_checksums` does — same
    /// `block_range`, same per-block `crc32c` — so these tests exercise the
    /// arithmetic production uses rather than a parallel implementation.
    fn checksums_over(content: &[u8], block_bytes: u64) -> ExtentChecksums {
        let sealed_length = content.len() as u64;
        let blocks = (0..block_count_for(sealed_length, block_bytes))
            .map(|i| {
                let (start, end) = block_range(i, block_bytes, sealed_length);
                crc32c(&content[start as usize..end as usize])
            })
            .collect();
        ExtentChecksums {
            sealed_length,
            block_bytes,
            blocks,
        }
    }

    #[test]
    fn a_sidecar_round_trips_and_is_pinned_to_its_extent() {
        let content: Vec<u8> = (0..2500u32).map(|i| i as u8).collect();
        let ck = checksums_over(&content, 1024);
        assert_eq!(ck.blocks.len(), 3, "2500 bytes over 1 KiB blocks is 3 blocks");
        let buf = ck.encode(77);
        assert_eq!(ExtentChecksums::decode(&buf, 77).as_ref(), Some(&ck));
        // Extent ids restart from small integers in the next cluster on this
        // host, so a sidecar left behind must not describe its successor.
        assert!(ExtentChecksums::decode(&buf, 78).is_none());
    }

    /// Every rejection is `None`, never a mismatch: a damaged sidecar is
    /// evidence about ITSELF, and treating it as evidence about the extent
    /// would rebuild a healthy replica on the strength of 4 KiB of rot.
    #[test]
    fn a_damaged_sidecar_reads_as_no_evidence() {
        let ck = checksums_over(&[7u8; 900], 1024);
        let good = ck.encode(5);

        let mut flipped_body = good.clone();
        let n = flipped_body.len();
        flipped_body[CK_HEADER_BYTES] ^= 0x01;
        assert!(ExtentChecksums::decode(&flipped_body, 5).is_none(), "body flip");

        let mut flipped_trailer = good.clone();
        flipped_trailer[n - 1] ^= 0x01;
        assert!(ExtentChecksums::decode(&flipped_trailer, 5).is_none(), "trailer flip");

        let mut bad_magic = good.clone();
        bad_magic[7] ^= 0x01;
        assert!(ExtentChecksums::decode(&bad_magic, 5).is_none(), "magic");

        assert!(ExtentChecksums::decode(&good[..good.len() - 1], 5).is_none(), "truncated");
        assert!(ExtentChecksums::decode(&[], 5).is_none(), "empty");
    }

    /// A sealed-empty extent is legal and describes no content.
    #[test]
    fn a_sealed_empty_extent_has_no_blocks() {
        let ck = checksums_over(&[], 1024);
        assert!(ck.blocks.is_empty());
        let buf = ck.encode(9);
        assert_eq!(ExtentChecksums::decode(&buf, 9), Some(ck));
    }

    #[test]
    fn a_full_read_checks_every_block_including_the_short_last_one() {
        let content: Vec<u8> = (0..2500u32).map(|i| (i * 7) as u8).collect();
        let ck = checksums_over(&content, 1024);
        assert_eq!(ck.verify_read(0, &content), Ok(3));

        // The tail block is 452 bytes, not 1024 — hashing it as a full block
        // would make every extent whose length is not a multiple of the block
        // size look corrupt.
        let mut rot = content.clone();
        rot[2400] ^= 0x01;
        let err = ck.verify_read(0, &rot).expect_err("tail rot must be caught");
        assert_eq!(err.block, 2);
        assert_eq!(err.offset, 2048);
    }

    /// The point of per-block: a read that covers whole blocks is checked, and
    /// one that covers none is silently unchecked rather than wrongly failed.
    #[test]
    fn only_fully_covered_blocks_are_checked() {
        let content: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        let ck = checksums_over(&content, 1024);

        // Exactly the middle two blocks.
        assert_eq!(ck.verify_read(1024, &content[1024..3072]), Ok(2));
        // A sub-block read covers nothing whole: unchecked, NOT an error.
        assert_eq!(ck.verify_read(1024, &content[1024..1088]), Ok(0));
        // Straddling a boundary still covers no block completely.
        assert_eq!(ck.verify_read(512, &content[512..1536]), Ok(0));

        // And a rotted byte inside a block the read does not fully cover is
        // invisible here — that is what the scrub is for.
        let mut rot = content.clone();
        rot[100] ^= 0x01;
        assert_eq!(ck.verify_read(0, &rot[0..512]), Ok(0));
        assert!(ck.verify_read(0, &rot).is_err(), "the full read still catches it");
    }

    /// A read starting past the first block must not mis-index its blocks.
    #[test]
    fn a_read_at_an_offset_maps_to_the_right_blocks() {
        let content: Vec<u8> = (0..8192u32).map(|i| (i % 253) as u8).collect();
        let ck = checksums_over(&content, 1024);
        let mut rot = content.clone();
        rot[5000] ^= 0x01;
        let err = ck
            .verify_read(4096, &rot[4096..8192])
            .expect_err("rot in block 4");
        assert_eq!(err.block, 4);
        assert_eq!(err.offset, 4096);
    }
}
