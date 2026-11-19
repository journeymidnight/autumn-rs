//! The partition half of `autumn-convert sst`: every SST of a stopped
//! partition from MetaBlock v2 to v3 (v3 dropped `vp_deps`). Delete this
//! module with that subcommand.
//!
//! The data blocks are the same in both versions, so an SST is converted by
//! keeping them byte for byte and re-encoding its MetaBlock; the result is
//! opened with this build's reader and every entry read back before it is
//! written. Per partition: take the owner lock (fencing any PS still holding
//! it, as an open does), fence and seal the tails, convert each v2 SST the
//! checkpoint lists, append the converted ones to the row stream, publish one
//! checkpoint naming them (every other field unchanged), read it back, and
//! cut the row stream before the first extent it still needs.
//!
//! A rerun finds the old record (and converts again; earlier appends are
//! unreferenced and go with the next row cut), the new one (all v3: only the
//! cuts remain), or both, when a run stopped between publishing and cutting
//! the meta stream: the second is then the first's converted form (same
//! cursor and floors, same tables in order, all v3) and the run finishes the
//! cuts. Any other second record is refused; `extent-records` refuses a
//! cluster where one may exist before anything is converted.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;

use autumn_rpc::partition_rpc::{rkyv_encode, SstLocation, TableLocations};
use autumn_stream::{ConnPool, StreamClient};

use crate::sstable::format::{BlockOffset, MetaBlock, FORMAT_VERSION, MAGIC};
use crate::sstable::SstReader;

pub const META_V2: u16 = 2;

/// An SST's MetaBlock (with its CRC, without the trailing `meta_len`) and the
/// bytes before it.
fn split_sst(sst: &[u8]) -> Result<(&[u8], &[u8])> {
    if sst.len() < 8 {
        bail!("SST too short: {} bytes", sst.len());
    }
    let n = sst.len();
    let meta_len = u32::from_le_bytes(sst[n - 4..].try_into().unwrap()) as usize;
    if meta_len == 0 || meta_len + 4 > n {
        bail!("invalid meta_len={meta_len} for an SST of {n} bytes");
    }
    let start = n - 4 - meta_len;
    Ok((&sst[..start], &sst[start..n - 4]))
}

/// The version field of a MetaBlock (its bytes, CRC included).
pub fn meta_version(meta: &[u8]) -> Result<u16> {
    if meta.len() < 10 {
        bail!("MetaBlock too short: {} bytes", meta.len());
    }
    let magic = u32::from_le_bytes(meta[0..4].try_into().unwrap());
    if magic != MAGIC {
        bail!("MetaBlock magic mismatch: {magic:#x}");
    }
    Ok(u16::from_le_bytes(meta[4..6].try_into().unwrap()))
}

struct Cursor<'a> {
    data: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.at + n > self.data.len() {
            bail!("MetaBlock v2 truncated at offset {} (need {n})", self.at);
        }
        let s = &self.data[self.at..self.at + n];
        self.at += n;
        Ok(s)
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}

/// A MetaBlock v2 (bytes with CRC), as this build's `MetaBlock`: every field
/// v3 keeps; `vp_deps`, between `vp_offset` and the compression byte, is read
/// past.
fn decode_meta_v2(meta: &[u8]) -> Result<MetaBlock> {
    if meta.len() < 10 {
        bail!("MetaBlock too short: {} bytes", meta.len());
    }
    let (payload, crc) = meta.split_at(meta.len() - 4);
    let stored = u32::from_le_bytes(crc.try_into().unwrap());
    let computed = crc32c::crc32c(payload);
    if stored != computed {
        bail!("MetaBlock CRC mismatch: stored={stored:#x} computed={computed:#x}");
    }
    let mut c = Cursor { data: payload, at: 0 };
    if c.u32()? != MAGIC {
        bail!("MetaBlock magic mismatch");
    }
    let version = c.u16()?;
    if version != META_V2 {
        bail!("expected MetaBlock v2, found v{version}");
    }
    let n = c.u32()? as usize;
    let mut block_offsets = Vec::with_capacity(n);
    for _ in 0..n {
        let klen = c.u16()? as usize;
        let key = c.take(klen)?.to_vec();
        let relative_offset = c.u32()?;
        let block_len = c.u32()?;
        block_offsets.push(BlockOffset { key, relative_offset, block_len });
    }
    let bloom_len = c.u32()? as usize;
    let bloom_data = c.take(bloom_len)?.to_vec();
    let sk_len = c.u16()? as usize;
    let smallest_key = c.take(sk_len)?.to_vec();
    let bk_len = c.u16()? as usize;
    let biggest_key = c.take(bk_len)?.to_vec();
    let estimated_size = c.u64()?;
    let seq_num = c.u64()?;
    let vp_extent_id = c.u64()?;
    let vp_offset = c.u64()?;
    let ndeps = c.u32()? as usize;
    c.take(ndeps.checked_mul(8).ok_or_else(|| anyhow!("vp_deps count {ndeps} overflows"))?)?;
    c.take(1)?; // compression_type
    let nd = c.u32()? as usize;
    let mut discards = HashMap::with_capacity(nd);
    for _ in 0..nd {
        let eid = c.u64()?;
        let sz = c.u64()? as i64;
        discards.insert(eid, sz);
    }
    let min_expires_at = c.u64()?;
    let num_entries = c.u64()?;
    let num_deletions = c.u64()?;
    if c.at != payload.len() {
        bail!("MetaBlock v2: {} trailing bytes", payload.len() - c.at);
    }
    Ok(MetaBlock {
        block_offsets,
        bloom_data,
        smallest_key,
        biggest_key,
        estimated_size,
        seq_num,
        vp_extent_id,
        vp_offset,
        discards,
        min_expires_at,
        num_entries,
        num_deletions,
    })
}

/// An SST with a v2 MetaBlock as v3: the same data blocks, the MetaBlock
/// re-encoded. Fails unless this build's reader opens the result and every
/// entry reads back, the counts matching what the old MetaBlock recorded.
pub fn sst_v2_to_v3(sst: &[u8]) -> Result<Vec<u8>> {
    let (data, meta) = split_sst(sst)?;
    let m = decode_meta_v2(meta)?;
    if let Some(b) = m
        .block_offsets
        .iter()
        .find(|b| b.relative_offset as usize + b.block_len as usize > data.len())
    {
        bail!("block at {}+{} past the data region ({} bytes)", b.relative_offset, b.block_len, data.len());
    }
    let (want_entries, want_deletions) = (m.num_entries, m.num_deletions);
    let new_meta = m.encode();
    let mut out = Vec::with_capacity(data.len() + new_meta.len() + 4);
    out.extend_from_slice(data);
    out.extend_from_slice(&new_meta);
    out.extend_from_slice(&(new_meta.len() as u32).to_le_bytes());

    let out = Bytes::from(out);
    let reader = SstReader::open_slice(out.clone(), 0, out.len() as u64).context("the v3 SST does not open")?;
    let (mut entries, mut deletions) = (0u64, 0u64);
    for idx in 0..reader.block_count() {
        let block = reader.read_block(idx).with_context(|| format!("block {idx}"))?;
        for e in 0..block.num_entries() {
            let (_, op, _, _) = block.get_entry(e).with_context(|| format!("block {idx} entry {e}"))?;
            entries += 1;
            if op & 0x7f == crate::OP_TOMBSTONE {
                deletions += 1;
            }
        }
    }
    if (entries, deletions) != (want_entries, want_deletions) {
        bail!(
            "read back {entries} entries / {deletions} tombstones, the MetaBlock records \
             {want_entries} / {want_deletions}"
        );
    }
    debug_assert_eq!(meta_version(&new_meta).ok(), Some(FORMAT_VERSION));
    drop(reader);
    Ok(Vec::from(out))
}

/// An SST as v2 wrote it: this build's output with `vp_deps` put back
/// after `vp_offset` and the version set to 2.
#[doc(hidden)]
pub fn sst_v3_to_v2_for_tests(v3: &[u8], vp_deps: &[u64]) -> Vec<u8> {
    let (data, meta) = split_sst(v3).unwrap();
    let payload = &meta[..meta.len() - 4];
    let m = MetaBlock::decode(meta).unwrap();
    // vp_offset ends after: header, index, bloom, two keys, 4 u64s.
    let mut at = 4 + 2 + 4;
    for b in &m.block_offsets {
        at += 2 + b.key.len() + 8;
    }
    at += 4 + m.bloom_data.len() + 2 + m.smallest_key.len() + 2 + m.biggest_key.len() + 32;
    let mut v2 = payload[..at].to_vec();
    v2[4..6].copy_from_slice(&META_V2.to_le_bytes());
    v2.extend_from_slice(&(vp_deps.len() as u32).to_le_bytes());
    for d in vp_deps {
        v2.extend_from_slice(&d.to_le_bytes());
    }
    v2.extend_from_slice(&payload[at..]);
    let crc = crc32c::crc32c(&v2);
    v2.extend_from_slice(&crc.to_le_bytes());
    let mut out = data.to_vec();
    out.extend_from_slice(&v2);
    out.extend_from_slice(&(v2.len() as u32).to_le_bytes());
    out
}

#[derive(Default, Debug)]
pub struct PartReport {
    pub converted: usize,
    pub already: usize,
    pub bytes: u64,
    pub truncated_extents: usize,
}

/// Every field but `locs` is the conversion's to keep.
fn same_except_locs(a: &TableLocations, b: &TableLocations) -> bool {
    (a.vp_extent_id, a.vp_offset, a.log_extent_count) == (b.vp_extent_id, b.vp_offset, b.log_extent_count)
        && a.fence_floors == b.fence_floors
}

fn loc_key(l: &SstLocation) -> (u64, u64, u64) {
    (l.extent_id, l.offset, l.len)
}

/// The last valid record of each non-empty meta extent, with that extent, in
/// stream order: what `read_all_table_locations` gives recovery. A non-empty
/// extent with no valid record is refused (recovery skips it with a warning;
/// an upgrade should not convert past it unseen).
async fn read_records(sc: &Rc<StreamClient>, meta_stream: u64) -> Result<Vec<(u64, TableLocations)>> {
    let info = sc.get_stream_info(meta_stream).await?;
    let mut out = Vec::new();
    for &eid in &info.extent_ids {
        let (payload, _) = sc.read_bytes_from_extent(eid, 0, 0).await?;
        if payload.is_empty() {
            continue;
        }
        let t = crate::decode_last_table_checkpoint(&payload)
            .with_context(|| format!("meta extent {eid} holds {} bytes and no valid checkpoint record", payload.len()))?;
        out.push((eid, t));
    }
    Ok(out)
}

/// `len` bytes at `offset` of an extent, read in windows.
async fn read_range(sc: &Rc<StreamClient>, extent_id: u64, offset: u64, len: u64) -> Result<Vec<u8>> {
    const WINDOW: u64 = 64 << 20;
    let mut out = Vec::with_capacity(len as usize);
    while (out.len() as u64) < len {
        let at = offset + out.len() as u64;
        let want = (len - out.len() as u64).min(WINDOW);
        let (b, _) = sc.read_bytes_from_extent(extent_id, at, want).await?;
        if b.is_empty() {
            bail!("extent {extent_id}: short read at {at} ({} of {len} bytes)", out.len());
        }
        out.extend_from_slice(&b);
    }
    out.truncate(len as usize);
    Ok(out)
}

/// The MetaBlock version of the SST at `loc`.
async fn sst_version(sc: &Rc<StreamClient>, loc: &SstLocation) -> Result<u16> {
    if loc.len < 8 {
        bail!("too short");
    }
    let tail = read_range(sc, loc.extent_id, loc.offset + loc.len - 4, 4).await?;
    let meta_len = u32::from_le_bytes(tail[..4].try_into().unwrap()) as u64;
    if meta_len == 0 || meta_len + 4 > loc.len {
        bail!("invalid meta_len {meta_len}");
    }
    let meta = read_range(sc, loc.extent_id, loc.offset + loc.len - 4 - meta_len, meta_len).await?;
    meta_version(&meta)
}

/// `b` is the record a run published for `a` and then stopped before cutting
/// `a`'s extent: the same cursor and floors, the same tables in the same
/// order, each `a`'s own or a rewrite at a location `a` does not list, at
/// least one rewritten, all at version `to`.
async fn is_converted_pair(sc: &Rc<StreamClient>, a: &TableLocations, b: &TableLocations, to: u16) -> Result<bool> {
    if !same_except_locs(a, b) || a.locs.len() != b.locs.len() {
        return Ok(false);
    }
    let a_set: HashSet<(u64, u64, u64)> = a.locs.iter().map(loc_key).collect();
    let mut rewritten = false;
    for (la, lb) in a.locs.iter().zip(&b.locs) {
        if loc_key(la) != loc_key(lb) {
            if a_set.contains(&loc_key(lb)) {
                return Ok(false);
            }
            rewritten = true;
        }
    }
    if !rewritten {
        return Ok(false);
    }
    for l in &b.locs {
        if sst_version(sc, l).await? != to {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Truncate, retried while the manager answers a transient conflict: split
/// siblings share row and meta extents, and converted side by side their cuts
/// race on those extents' `refs`.
async fn truncate(sc: &Rc<StreamClient>, stream_id: u64, extent_id: u64) -> Result<()> {
    let mut attempt = 0;
    loop {
        match sc.truncate(stream_id, extent_id).await {
            Ok(_) => return Ok(()),
            Err(e) if attempt < 20 && crate::manager_says_retry_later(&e) => {
                attempt += 1;
                compio::time::sleep(std::time::Duration::from_millis(100 * attempt)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Test failpoint: the next run stops right after publishing its checkpoint,
/// before it cuts anything.
#[doc(hidden)]
pub static FAIL_AFTER_PUBLISH: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Convert one partition's SSTs. Every PS must be stopped: one that reopens
/// the partition takes its owner lock and fences this run.
pub async fn convert_partition(
    manager_endpoint: &str,
    pool: Rc<ConnPool>,
    part_id: u64,
    streams: [u64; 3],
    max_extent_size: u64,
    dry_run: bool,
) -> Result<PartReport> {
    rewrite_partition(manager_endpoint, pool, part_id, streams, max_extent_size, dry_run, META_V2, FORMAT_VERSION, sst_v2_to_v3)
        .await
}

/// The reverse, for tests: a v3 partition as v2 wrote it.
#[doc(hidden)]
pub async fn downgrade_partition_for_tests(
    manager_endpoint: &str,
    pool: Rc<ConnPool>,
    part_id: u64,
    streams: [u64; 3],
) -> Result<PartReport> {
    rewrite_partition(manager_endpoint, pool, part_id, streams, 16 << 30, false, FORMAT_VERSION, META_V2, |s| {
        Ok(sst_v3_to_v2_for_tests(s, &[1]))
    })
    .await
}

/// Rewrite every SST at MetaBlock `from` through `f`; SSTs at `to` are kept.
#[allow(clippy::too_many_arguments)]
async fn rewrite_partition(
    manager_endpoint: &str,
    pool: Rc<ConnPool>,
    part_id: u64,
    streams: [u64; 3],
    max_extent_size: u64,
    dry_run: bool,
    from: u16,
    to: u16,
    f: fn(&[u8]) -> Result<Vec<u8>>,
) -> Result<PartReport> {
    let [log_stream, row_stream, meta_stream] = streams;
    let owner_key = format!("partition/{part_id}");
    let sc = if dry_run {
        // Reads only: no owner lock, nothing fenced.
        StreamClient::new_with_owner_epoch(manager_endpoint, owner_key, 0, max_extent_size, pool).await?
    } else {
        let sc = StreamClient::connect(manager_endpoint, owner_key, max_extent_size, pool)
            .await
            .context("owner lock")?;
        let epoch = sc.owner_epoch();
        for sid in [log_stream, row_stream, meta_stream] {
            sc.fence_tail(sid, epoch).await.with_context(|| format!("fence stream {sid}"))?;
        }
        // Reads then see exactly the sealed length, and the first append
        // starts a fresh tail.
        for sid in [row_stream, meta_stream] {
            sc.seal_tail(sid).await.with_context(|| format!("seal stream {sid}"))?;
        }
        sc
    };

    let records = read_records(&sc, meta_stream).await.context("read checkpoint")?;
    let mut report = PartReport::default();
    // The meta extent to keep once the converted record is in it.
    let (ckpt, mut meta_keep) = match records.as_slice() {
        [] => return Ok(report),
        [(_, r)] => (r.clone(), None),
        [(_, a), (eb, b)] if is_converted_pair(&sc, a, b, to).await? => (b.clone(), Some(*eb)),
        rs => bail!(
            "{} checkpoint records (meta extents {:?}); this converts a partition holding one. \
             `autumn-convert extent-records` refuses a cluster where one may hold more",
            rs.len(),
            rs.iter().map(|(e, _)| *e).collect::<Vec<_>>()
        ),
    };

    let mut new_locs: Vec<SstLocation> = Vec::with_capacity(ckpt.locs.len());
    for loc in &ckpt.locs {
        let what = || format!("SST extent={} offset={} len={}", loc.extent_id, loc.offset, loc.len);
        match sst_version(&sc, loc).await.with_context(what)? {
            v if v == to => {
                report.already += 1;
                new_locs.push(loc.clone());
            }
            v if v == from => {
                let sst = read_range(&sc, loc.extent_id, loc.offset, loc.len).await.with_context(what)?;
                let bytes = f(&sst).with_context(what)?;
                drop(sst);
                report.converted += 1;
                report.bytes += bytes.len() as u64;
                if dry_run {
                    new_locs.push(loc.clone());
                    continue;
                }
                let r = sc.append(row_stream, &bytes).await.context("append converted SST")?;
                new_locs.push(SstLocation { extent_id: r.extent_id, offset: r.offset, len: r.end - r.offset });
            }
            v => bail!("{}: MetaBlock v{v}; this converts v{from} to v{to}", what()),
        }
    }
    if dry_run {
        return Ok(report);
    }
    if report.converted > 0 {
        let new_ckpt = TableLocations { locs: new_locs.clone(), ..ckpt.clone() };
        let payload = rkyv_encode(&new_ckpt);
        let mut rec = Vec::with_capacity(4 + payload.len());
        rec.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        rec.extend_from_slice(&payload);
        let r = sc.append(meta_stream, &rec).await.context("publish checkpoint")?;
        // Read it back from its extent, the way recovery will, before cutting
        // anything: the old record still names the old tables until then.
        let (got, _) = sc.read_bytes_from_extent(r.extent_id, 0, 0).await?;
        let back = crate::decode_last_table_checkpoint(&got).context("re-read checkpoint")?;
        if !same_except_locs(&back, &new_ckpt) || !back.locs.iter().map(loc_key).eq(new_ckpt.locs.iter().map(loc_key)) {
            bail!("meta extent {} does not read back as the record just published; nothing was cut", r.extent_id);
        }
        if FAIL_AFTER_PUBLISH.swap(false, std::sync::atomic::Ordering::Relaxed) {
            bail!("test failpoint: stopped after publishing");
        }
        meta_keep = Some(r.extent_id);
    }
    if let Some(keep) = meta_keep {
        let info = sc.get_stream_info(meta_stream).await?;
        if info.extent_ids.first() != Some(&keep) {
            truncate(&sc, meta_stream, keep).await.context("truncate meta stream (rerun to finish)")?;
        }
    }
    let keep: HashSet<u64> = new_locs.iter().map(|l| l.extent_id).collect();
    let info = sc.get_stream_info(row_stream).await?;
    if let Some(cut) = crate::background::row_truncate_point(&info.extent_ids, &keep) {
        truncate(&sc, row_stream, cut).await.context("truncate row stream (rerun to finish)")?;
        report.truncated_extents = info.extent_ids.iter().position(|e| *e == cut).unwrap_or(0);
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::SstBuilder;

    fn sample() -> Vec<u8> {
        let mut b = SstBuilder::new(9, 4096, 4000);
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
        b.set_discards(discards);
        b.finish().segments.concat()
    }

    #[test]
    fn a_v2_sst_converts_back_to_what_this_build_writes() {
        let v3 = sample();
        let reader = SstReader::from_bytes(Bytes::from(v3.clone())).unwrap();
        assert!(reader.block_count() > 1, "the test needs several blocks");
        let v2 = sst_v3_to_v2_for_tests(&v3, &[21, 48]);
        let (_, meta2) = split_sst(&v2).unwrap();
        assert_eq!(meta_version(meta2).unwrap(), META_V2);
        assert!(SstReader::from_bytes(Bytes::from(v2.clone())).is_err(), "this build refuses v2");
        assert_eq!(sst_v2_to_v3(&v2).unwrap(), v3);
    }

    #[test]
    fn a_damaged_v2_sst_is_refused() {
        let v2 = sst_v3_to_v2_for_tests(&sample(), &[]);
        // A byte inside the first data block: its CRC no longer holds.
        let mut bad = v2.clone();
        bad[10] ^= 0xff;
        assert!(sst_v2_to_v3(&bad).is_err());
        // A v3 SST is not taken for v2.
        assert!(sst_v2_to_v3(&sample()).is_err());
    }
}
