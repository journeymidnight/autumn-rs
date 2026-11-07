//! The checksum sidecar format: per-block CRC32C of one sealed payload file
//! (`extent-{id}.ck` for `.dat`, `extent-{id}.shard{i}.ck` for a shard).
//!
//! The stream layer stores opaque bytes and owns no format, so nothing else
//! describes its content: `.meta`'s CRC32C covers its own 48 metadata bytes.
//! The scrub (`extent_node/scrub.rs`) writes and reads it; the EC coordinator
//! also checks the `.dat` it encodes against it (`PieceCheck`), because the
//! conversion is the last moment clean replicas exist. It is content-only — it
//! says nothing about which generation or layout is live — and describes
//! exactly `sealed_length` bytes, which is how a sidecar left from other
//! content is told apart.

use crc32c::crc32c;

/// crc32c's reflected polynomial.
const POLY: u32 = 0x82F6_3B78;

/// `a * b mod P`, bit-reflected (zlib's `multmodp`).
fn multmodp(a: u32, mut b: u32) -> u32 {
    let mut m = 1u32 << 31;
    let mut p = 0u32;
    loop {
        if a & m != 0 {
            p ^= b;
            if a & (m - 1) == 0 {
                return p;
            }
        }
        m >>= 1;
        b = if b & 1 != 0 { (b >> 1) ^ POLY } else { b >> 1 };
    }
}

/// `X2N[k] = x^(2^k) mod P`, for every bit of a `u64` byte length. 64
/// entries, not zlib's 32 read `k & 31`: that wrap holds for CRC-32, whose
/// polynomial is primitive, not for crc32c's.
const X2N: [u32; 67] = {
    let mut t = [0u32; 67];
    t[0] = 1 << 30;
    let mut k = 1;
    while k < 67 {
        // multmodp(t[k-1], t[k-1]) in a const context.
        let (a, mut b) = (t[k - 1], t[k - 1]);
        let (mut m, mut p) = (1u32 << 31, 0u32);
        loop {
            if a & m != 0 {
                p ^= b;
                if a & (m - 1) == 0 {
                    break;
                }
            }
            m >>= 1;
            b = if b & 1 != 0 { (b >> 1) ^ POLY } else { b >> 1 };
        }
        t[k] = p;
        k += 1;
    }
    t
};

/// The crc32c of `A ++ B` from `crc(A)`, `crc(B)` and `B`'s length: one GF(2)
/// multiplication per set bit of the length, ~100 ns for 4 KiB where the
/// `crc32c` crate's matrix form takes ~10 us (zlib 1.2.12's method).
pub(crate) fn crc32c_combine(crc_a: u32, crc_b: u32, len_b: usize) -> u32 {
    let mut n = len_b as u64;
    let mut k = 3usize; // bytes -> bits
    let mut shift = 1u32 << 31;
    while n != 0 {
        if n & 1 != 0 {
            shift = multmodp(X2N[k], shift);
        }
        n >>= 1;
        k += 1;
    }
    multmodp(shift, crc_a) ^ crc_b
}

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
/// Shared by the scrub and the tests on purpose: treating the
/// short tail block as a full one is the mistake that would make every extent
/// whose length is not a multiple of the block size look corrupt, and it should
/// only be possible to make it in one place.
pub(crate) fn block_range(i: usize, block_bytes: u64, sealed_length: u64) -> (u64, u64) {
    let start = (i as u64) * block_bytes;
    let end = start.saturating_add(block_bytes).min(sealed_length);
    (start, end)
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
}

/// One read piece of a described file: its offset, length and CRC32C. A piece
/// never crosses a block boundary.
pub(crate) type Piece = (u64, u64, u32);

/// Cut `buf`, read at `offset`, at block boundaries and checksum each piece.
/// CPU only, so the caller can run it off the runtime thread.
pub(crate) fn piece_crcs(block_bytes: u64, offset: u64, buf: &[u8]) -> Vec<Piece> {
    let mut pieces = Vec::new();
    let mut at = 0usize;
    while at < buf.len() {
        let start = offset + at as u64;
        let room = (block_bytes - start % block_bytes) as usize;
        let len = room.min(buf.len() - at);
        pieces.push((start, len as u64, crc32c(&buf[at..at + len])));
        at += len;
    }
    pieces
}

/// `piece_crcs` over a payload in parts (as an append arrives), cut at the
/// block boundaries of the file it is written at `offset` in.
pub(crate) fn pieces_of_parts(block_bytes: u64, offset: u64, parts: &[bytes::Bytes]) -> Vec<Piece> {
    let mut pieces: Vec<Piece> = Vec::new();
    let mut at = offset;
    for part in parts {
        let mut i = 0usize;
        while i < part.len() {
            let room = (block_bytes - at % block_bytes) as usize;
            let len = room.min(part.len() - i);
            let bytes = &part[i..i + len];
            match pieces.last_mut() {
                // The previous part ended mid-block: this one continues its piece.
                Some(last) if last.0 + last.1 == at && at % block_bytes != 0 => {
                    last.2 = crc32c::crc32c_append(last.2, bytes);
                    last.1 += len as u64;
                }
                _ => pieces.push((at, len as u64, crc32c(bytes))),
            }
            i += len;
            at += len as u64;
        }
    }
    pieces
}

/// The crc32c of the bytes `pieces` cover, in order.
pub(crate) fn crc_of_pieces(pieces: &[Piece]) -> u32 {
    match pieces.split_first() {
        None => 0,
        Some((first, rest)) => rest.iter().fold(first.2, |crc, p| crc32c_combine(crc, p.2, p.1 as usize)),
    }
}

/// The block crcs of an open `.dat`, accumulated as appends become durable.
/// `end` is how far it has seen; `tail` the crc of the open block's bytes up
/// to `end`, known only when every byte of it passed through here (`valid`).
#[derive(Default)]
pub(crate) struct WriteCk {
    end: u64,
    tail: u32,
    valid: bool,
}

impl WriteCk {
    /// Start over at `at` (a restart, a truncate): the block holding `at`
    /// is known only if `at` is its first byte.
    pub(crate) fn reset(&mut self, at: u64) {
        *self = Self { end: at, tail: 0, valid: at % CK_BLOCK_BYTES == 0 };
    }

    /// Take a durable write at `start`, cut into `pieces`; returns the blocks
    /// it completed as (block index, crc).
    pub(crate) fn record(&mut self, start: u64, pieces: &[Piece]) -> Vec<(u64, u32)> {
        if start != self.end {
            self.reset(start);
        }
        let mut done = Vec::new();
        for &(at, len, crc) in pieces {
            if at % CK_BLOCK_BYTES == 0 {
                self.tail = crc;
                self.valid = true;
            } else if self.valid {
                self.tail = crc32c_combine(self.tail, crc, len as usize);
            }
            self.end = at + len;
            if self.valid && self.end % CK_BLOCK_BYTES == 0 {
                done.push((self.end / CK_BLOCK_BYTES - 1, self.tail));
            }
        }
        done
    }
}

/// `extent-{id}.ck.open`: the block crcs of a `.dat` recorded as its appends
/// became durable, before any seal. A header, then one 8-byte slot per block
/// at a fixed position — `[crc][check]`, `check` binding the crc to the extent
/// and the block — so an entry is written alone and a torn or zero slot reads
/// as absent. Written without fsync: a slot lost to a crash only leaves that
/// block to the scrub's first record.
const OPEN_MAGIC: &[u8; 8] = b"EXTCKO\0\x01";
pub(crate) const OPEN_HEADER_BYTES: u64 = 16;

pub(crate) fn open_header(extent_id: u64) -> [u8; 16] {
    let mut h = [0u8; 16];
    h[..8].copy_from_slice(OPEN_MAGIC);
    h[8..].copy_from_slice(&extent_id.to_le_bytes());
    h
}

fn open_check(extent_id: u64, block: u64, crc: u32) -> u32 {
    let mut b = [0u8; 20];
    b[..8].copy_from_slice(&extent_id.to_le_bytes());
    b[8..16].copy_from_slice(&block.to_le_bytes());
    b[16..].copy_from_slice(&crc.to_le_bytes());
    !crc32c(&b)
}

/// The slots for blocks `first..` with these crcs, and where they go.
pub(crate) fn open_slots(extent_id: u64, first: u64, crcs: &[u32]) -> (u64, Vec<u8>) {
    let mut buf = Vec::with_capacity(crcs.len() * 8);
    for (k, &crc) in crcs.iter().enumerate() {
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(&open_check(extent_id, first + k as u64, crc).to_le_bytes());
    }
    (OPEN_HEADER_BYTES + first * 8, buf)
}

/// The recorded crc of each block, `None` where no valid slot is; `None`
/// altogether if the file is not this extent's.
pub(crate) fn decode_open(buf: &[u8], extent_id: u64) -> Option<Vec<Option<u32>>> {
    if buf.len() < OPEN_HEADER_BYTES as usize || buf[..16] != open_header(extent_id) {
        return None;
    }
    Some(
        buf[16..]
            .chunks_exact(8)
            .enumerate()
            .map(|(i, s)| {
                let crc = u32::from_le_bytes([s[0], s[1], s[2], s[3]]);
                let check = u32::from_le_bytes([s[4], s[5], s[6], s[7]]);
                (check == open_check(extent_id, i as u64, crc)).then_some(crc)
            })
            .collect(),
    )
}

/// Checks a file against its recorded checksums from pieces read in any order,
/// so a reader that walks the file out of order — the EC encoder reads it
/// shard by shard — verifies it without a second pass. Each byte must arrive
/// exactly once.
pub(crate) struct PieceCheck {
    ck: ExtentChecksums,
    pending: std::collections::HashMap<usize, Vec<Piece>>,
    verified: usize,
}

impl PieceCheck {
    pub(crate) fn new(ck: ExtentChecksums) -> Self {
        Self { ck, pending: std::collections::HashMap::new(), verified: 0 }
    }

    pub(crate) fn block_bytes(&self) -> u64 {
        self.ck.block_bytes
    }

    /// Take pieces; `Err(block)` names the first completed block whose
    /// content differs from its checksum.
    pub(crate) fn add(&mut self, pieces: Vec<Piece>) -> Result<(), usize> {
        for piece in pieces {
            let block = (piece.0 / self.ck.block_bytes) as usize;
            let (start, end) = block_range(block, self.ck.block_bytes, self.ck.sealed_length);
            let parts = self.pending.entry(block).or_default();
            parts.push(piece);
            if parts.iter().map(|p| p.1).sum::<u64>() < end - start {
                continue;
            }
            let crc = block_crc(self.pending.remove(&block).unwrap_or_default());
            if self.ck.blocks.get(block) != Some(&crc) {
                return Err(block);
            }
            self.verified += 1;
        }
        Ok(())
    }

    /// Was every block seen and found clean?
    pub(crate) fn complete(&self) -> bool {
        self.verified == self.ck.blocks.len() && self.pending.is_empty()
    }
}

/// One block's crc from the pieces that make it up, in any order.
fn block_crc(mut parts: Vec<Piece>) -> u32 {
    parts.sort_unstable_by_key(|p| p.0);
    parts[1..].iter().fold(parts[0].2, |crc, p| crc32c_combine(crc, p.2, p.1 as usize))
}

/// Builds a file's checksums from pieces as they are written, in any order —
/// the description of a file whose bytes were checked on their way in, so it
/// needs no scrub's first record. Yields them only when every byte arrived
/// exactly once; anything else (a gap, a byte twice, a piece past the end)
/// leaves the file undescribed, for a scrub to record.
pub(crate) struct PieceRecord {
    length: u64,
    pending: std::collections::HashMap<usize, Vec<Piece>>,
    blocks: Vec<Option<u32>>,
    broken: bool,
}

impl PieceRecord {
    pub(crate) fn new(length: u64) -> Self {
        Self {
            length,
            pending: std::collections::HashMap::new(),
            blocks: vec![None; block_count_for(length, CK_BLOCK_BYTES)],
            broken: false,
        }
    }

    pub(crate) fn length(&self) -> u64 {
        self.length
    }

    pub(crate) fn add(&mut self, pieces: Vec<Piece>) {
        for piece in pieces {
            let block = (piece.0 / CK_BLOCK_BYTES) as usize;
            if piece.0 + piece.1 > self.length || self.blocks.get(block).is_none_or(|b| b.is_some()) {
                self.broken = true;
                return;
            }
            let (start, end) = block_range(block, CK_BLOCK_BYTES, self.length);
            let parts = self.pending.entry(block).or_default();
            parts.push(piece);
            let got: u64 = parts.iter().map(|p| p.1).sum();
            if got > end - start {
                self.broken = true;
                return;
            }
            if got == end - start {
                let parts = self.pending.remove(&block).unwrap_or_default();
                self.blocks[block] = Some(block_crc(parts));
            }
        }
    }

    /// The checksums, once every block is whole.
    pub(crate) fn finish(self) -> Option<ExtentChecksums> {
        if self.broken || !self.pending.is_empty() {
            return None;
        }
        let blocks = self.blocks.into_iter().collect::<Option<Vec<u32>>>()?;
        Some(ExtentChecksums { sealed_length: self.length, block_bytes: CK_BLOCK_BYTES, blocks })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Appends of any size, at any offset, in parts cut anywhere: the blocks
    /// they complete carry the crcs a scrub computes, the pieces fold back to
    /// the payload's crc, and a restart mid-block leaves that block unknown.
    #[test]
    fn write_time_crcs_match_the_scrub() {
        let mut content: Vec<u8> = (0..(CK_BLOCK_BYTES * 4 + 777)).map(|i| (i * 31 % 253) as u8).collect();
        let want = checksums_over(&content, CK_BLOCK_BYTES).blocks;
        let mut ck = WriteCk::default();
        let mut got = Vec::new();
        let mut at = 0usize;
        // 4 KiB appends, a 700 KiB one, an 8 MiB-ish one, each cut into parts.
        for len in [4096usize, 4096, 700_000, 1_500_000, 4096, content.len()] {
            let end = (at + len).min(content.len());
            if at == end {
                break;
            }
            let parts: Vec<bytes::Bytes> =
                content[at..end].chunks(300_001).map(bytes::Bytes::copy_from_slice).collect();
            let pieces = pieces_of_parts(CK_BLOCK_BYTES, at as u64, &parts);
            assert_eq!(crc_of_pieces(&pieces), crc32c(&content[at..end]));
            got.extend(ck.record(at as u64, &pieces));
            at = end;
        }
        let full = content.len() / CK_BLOCK_BYTES as usize;
        assert_eq!(got, (0..full as u64).map(|i| (i, want[i as usize])).collect::<Vec<_>>());

        // Restarted mid-block 1: block 1 is unknown, block 2 on is recorded.
        let mut ck = WriteCk::default();
        ck.reset(CK_BLOCK_BYTES + 10);
        let from = CK_BLOCK_BYTES as usize + 10;
        let parts = vec![bytes::Bytes::copy_from_slice(&content[from..])];
        let done = ck.record(from as u64, &pieces_of_parts(CK_BLOCK_BYTES, from as u64, &parts));
        assert_eq!(done.first(), Some(&(2, want[2])));

        // A write that does not continue where the record ends starts over.
        content[0] ^= 1;
        let mut ck = WriteCk::default();
        let parts = vec![bytes::Bytes::copy_from_slice(&content[..4096])];
        ck.record(0, &pieces_of_parts(CK_BLOCK_BYTES, 0, &parts));
        let parts = vec![bytes::Bytes::copy_from_slice(&content[8192..CK_BLOCK_BYTES as usize])];
        let done = ck.record(8192, &pieces_of_parts(CK_BLOCK_BYTES, 8192, &parts));
        assert!(done.is_empty(), "a gap leaves the block unknown");
    }

    /// The fast combine is the crc of the concatenation, at every length.
    #[test]
    fn combine_is_the_crc_of_the_concatenation() {
        let data: Vec<u8> = (0..300_000u32).map(|i| (i * 97 % 251) as u8).collect();
        for split in [0usize, 1, 7, 4096, 65_535, 131_072, 299_999, 300_000] {
            let (a, b) = data.split_at(split);
            assert_eq!(crc32c_combine(crc32c(a), crc32c(b), b.len()), crc32c(&data), "split {split}");
            assert_eq!(crc32c_combine(crc32c(a), crc32c(b), b.len()), crc32c::crc32c_combine(crc32c(a), crc32c(b), b.len()));
        }
    }

    /// Long lengths too: crc32c's polynomial is not primitive, so x^(2^k)
    /// does not wrap at k = 32 as it does for CRC-32 — a 32-entry table
    /// indexed `k & 31` is wrong from 512 MiB on.
    #[test]
    fn combine_holds_past_512_mib() {
        for len in [1usize << 29, (1 << 29) + 4096, 1 << 30, 1 << 32, (1 << 40) + 7] {
            for (a, b) in [(0x1234_5678u32, 0x9abc_def0u32), (0xffff_ffff, 1)] {
                assert_eq!(crc32c_combine(a, b, len), crc32c::crc32c_combine(a, b, len), "len {len}");
            }
        }
    }

    /// Open slots read back where written; a torn, zero or foreign slot is absent.
    #[test]
    fn open_slots_read_back_and_bad_ones_are_absent() {
        let mut file = open_header(9).to_vec();
        let (at, slots) = open_slots(9, 2, &[11, 22]);
        file.resize(at as usize, 0);
        file.extend_from_slice(&slots);
        assert_eq!(decode_open(&file, 9), Some(vec![None, None, Some(11), Some(22)]));
        let last = file.len() - 1;
        file[last] ^= 1;
        assert_eq!(decode_open(&file, 9), Some(vec![None, None, Some(11), None]));
        assert_eq!(decode_open(&file, 10), None, "another extent's file");
    }

    /// A record built from pieces in any order is the scrub's description of
    /// the same bytes; a gap or a repeat describes nothing.
    #[test]
    fn a_record_from_pieces_matches_the_whole_file() {
        let content: Vec<u8> = (0..(CK_BLOCK_BYTES * 3 + 4321)).map(|i| (i * 7 % 251) as u8).collect();
        let want = checksums_over(&content, CK_BLOCK_BYTES);
        let cut = |from: usize, to: usize| piece_crcs(CK_BLOCK_BYTES, from as u64, &content[from..to]);
        // Stripes not aligned to blocks, arriving out of order.
        let stripes = [(1_500_000, 2_900_000), (0, 1_500_000), (2_900_000, content.len())];
        let mut rec = PieceRecord::new(content.len() as u64);
        for (a, b) in stripes {
            rec.add(cut(a, b));
        }
        assert_eq!(rec.finish(), Some(want));

        let mut gap = PieceRecord::new(content.len() as u64);
        gap.add(cut(0, 1_500_000));
        gap.add(cut(2_900_000, content.len()));
        assert_eq!(gap.finish(), None, "a gap describes nothing");

        let mut twice = PieceRecord::new(content.len() as u64);
        twice.add(cut(0, content.len()));
        twice.add(cut(0, 1000));
        assert_eq!(twice.finish(), None, "a byte twice describes nothing");

        let mut past = PieceRecord::new(1000);
        past.add(cut(0, 2000));
        assert_eq!(past.finish(), None, "bytes past the length describe nothing");
    }

    /// Walks blocks exactly the way the scrub does — same `block_range`, same
    /// per-block `crc32c` — so these tests exercise the arithmetic production
    /// uses rather than a parallel implementation.
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
    fn pieces_read_out_of_order_verify_each_block_once_complete() {
        let content: Vec<u8> = (0..10_000u32).map(|i| (i * 7 % 251) as u8).collect();
        let ck = checksums_over(&content, 1024);
        let mut check = PieceCheck::new(ck);
        // Two "shards" of 5000 bytes, read in stripes of 1500, shard-major per
        // stripe — the EC encoder's order. Block 4 straddles the shard boundary.
        for s in (0..5000).step_by(1500) {
            for shard in 0..2 {
                let start = shard * 5000 + s;
                let end = (start + 1500).min(shard * 5000 + 5000);
                let pieces = piece_crcs(check.block_bytes(), start as u64, &content[start..end]);
                assert_eq!(check.add(pieces), Ok(()));
            }
        }
        assert!(check.complete());
    }

    #[test]
    fn a_rotted_piece_names_its_block() {
        let content: Vec<u8> = (0..4096u32).map(|i| (i % 253) as u8).collect();
        let mut check = PieceCheck::new(checksums_over(&content, 1024));
        let mut rotted = content.clone();
        rotted[2050] ^= 1;
        assert_eq!(check.add(piece_crcs(1024, 0, &rotted[..2000])), Ok(()));
        assert_eq!(check.add(piece_crcs(1024, 2000, &rotted[2000..])), Err(2));
        assert!(!check.complete());
    }

    #[test]
    fn a_missing_piece_is_not_complete() {
        let content = vec![9u8; 3000];
        let mut check = PieceCheck::new(checksums_over(&content, 1024));
        assert_eq!(check.add(piece_crcs(1024, 0, &content[..2500])), Ok(()));
        assert!(!check.complete());
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
}
