//! F248 — the io_uring daemon reads autumn-fuse files (inode + F247 variable-
//! length extents), replacing the pre-F248 flat-key + whole-value-fetch model.
//!
//! The daemon is the high-perf data path for the fuse mount, whose target
//! workload is reading large model files. Pre-F248 it treated the SQE path as a
//! single flat KV key and pulled the WHOLE value per Read (`offset:0,length:0`)
//! then sliced — catastrophic byte amplification for a multi-GiB object, and it
//! couldn't read the chunked files the fuse mount actually writes. F248 makes it
//! resolve a fuse PATH → inode → F247 extent map at Open, and a Read fans out
//! across only the OVERLAPPING extents' exact sub-ranges via the shared
//! `ClusterClient::get_many_into` (= `fan_out`). Combined with the daemon's
//! F244-C `FuturesUnordered` SQ/CQ loop, there are two fan-out levels: across
//! concurrent SQEs (the loop) and across a single read's extents (`get_many_into`).
//!
//! This uses ONLY autumn-fuse's UNGATED `key` + `schema` modules — no `fuser`,
//! no FUSE mount layer — so the daemon stays a lean io_uring binary. ZC into the
//! shared ring region is deferred to F243 (needs `ucp_mem_map` registration);
//! here `get_many_into` reads into a local buffer (TCP large reads still drop the
//! rkyv wrap) which the daemon copies into the ring.

use anyhow::{anyhow, Result};

use autumn_client::{ClusterClient, GetManyItem, BATCH_GET_DEFAULT_CONCURRENCY};
use autumn_fuse::key;
use autumn_fuse::schema::{self, InodeMeta, MAX_EXTENT, ROOT_INO};

/// Range-scan page size when loading a large file's extent map.
const RANGE_PAGE: u32 = 8192;

/// An opened fuse file: inode, logical size, and the variable-length extent map
/// (`(logical_start, value_len)`, sorted by start) resolved at Open time and
/// cached on the daemon's `ring_fd` so each Read avoids re-resolving.
pub struct OpenedExtents {
    pub ino: u64,
    pub size: u64,
    pub extents: Vec<(u64, u32)>,
}

/// Resolve a fuse path (`"/a/b/file"` or `"a/b/file"`) to its inode + metadata by
/// walking dirents from the root. Empty path / `"/"` → the root inode.
pub async fn resolve_path(cluster: &ClusterClient, path: &[u8]) -> Result<(u64, InodeMeta)> {
    let mut ino = ROOT_INO;
    for comp in path.split(|&b| b == b'/') {
        if comp.is_empty() {
            continue; // leading / trailing / duplicate slashes
        }
        let dk = key::dirent_key(ino, comp);
        let dv = cluster
            .get(&dk)
            .await
            .map_err(|e| anyhow!("dirent get: {e}"))?
            .ok_or_else(|| anyhow!("ENOENT: {}", String::from_utf8_lossy(comp)))?;
        let dirent = schema::decode_dirent(&dv).map_err(|e| anyhow!("decode dirent: {e}"))?;
        ino = dirent.child_inode;
    }
    let iv = cluster
        .get(&key::inode_key(ino))
        .await
        .map_err(|e| anyhow!("inode get: {e}"))?
        .ok_or_else(|| anyhow!("ENOENT inode {ino}"))?;
    let meta = schema::decode_inode_meta(&iv).map_err(|e| anyhow!("decode inode: {e}"))?;
    Ok((ino, meta))
}

/// Load a file's extent map: paginated range-scan of `[0x03][ino]` start offsets,
/// then infer each extent's length from the next start (non-overlap upper bound)
/// / file size for the last, capped at `MAX_EXTENT`. Mirrors the fuse-side
/// `extent::extents_snapshot` cold-rebuild (kept separate to avoid the gated
/// `state::FsState` dependency).
pub async fn load_extent_map(
    cluster: &ClusterClient,
    ino: u64,
    size: u64,
) -> Result<Vec<(u64, u32)>> {
    let prefix = key::extent_prefix(ino);
    let mut starts: Vec<u64> = Vec::new();
    let mut start = prefix.clone();
    loop {
        let res = cluster
            .range(&prefix, &start, RANGE_PAGE)
            .await
            .map_err(|e| anyhow!("extent range: {e}"))?;
        let got = res.entries.len();
        for e in &res.entries {
            if let Some((_, off)) = key::parse_extent_key(&e.key) {
                starts.push(off);
            }
        }
        if got < RANGE_PAGE as usize {
            break;
        }
        let last = starts.last().copied().unwrap_or(0);
        start = key::extent_key(ino, last.saturating_add(1));
    }
    starts.sort_unstable();
    starts.dedup();
    Ok(infer_lengths(&starts, size))
}

fn infer_lengths(starts: &[u64], size: u64) -> Vec<(u64, u32)> {
    let mut out = Vec::with_capacity(starts.len());
    for (i, &s) in starts.iter().enumerate() {
        let end = if i + 1 < starts.len() {
            starts[i + 1]
        } else {
            size.max(s)
        };
        let len = (end.saturating_sub(s)).min(MAX_EXTENT as u64) as u32;
        out.push((s, len));
    }
    out
}

/// Resolve + load a fuse file for a daemon Open.
pub async fn open(cluster: &ClusterClient, path: &[u8]) -> Result<OpenedExtents> {
    let (ino, meta) = resolve_path(cluster, path).await?;
    let extents = load_extent_map(cluster, ino, meta.size).await?;
    Ok(OpenedExtents {
        ino,
        size: meta.size,
        extents,
    })
}

/// One extent slice to fetch for a read: KV key + in-extent sub-range
/// `[offset, offset+length)` + absolute position within the read buffer.
#[derive(Debug, PartialEq, Eq)]
pub struct ExtentSlice {
    pub key: Vec<u8>,
    pub offset: u32,
    pub length: u32,
    pub dest_offset: usize,
}

/// Plan the extent slices covering read `[off, off+len)` (EOF-clamped). Pure —
/// unit-testable. Returns `(actual_len, slices)`; gaps between slices in the
/// result buffer are left for the caller to zero-fill (sparse semantics).
pub fn plan_read(opened: &OpenedExtents, off: u64, len: u32) -> (usize, Vec<ExtentSlice>) {
    if off >= opened.size || len == 0 {
        return (0, Vec::new());
    }
    let read_end = (off + len as u64).min(opened.size);
    let actual = (read_end - off) as usize;
    let mut slices = Vec::new();
    for &(s, l) in &opened.extents {
        let e_end = s + l as u64;
        let ov_s = off.max(s);
        let ov_e = read_end.min(e_end);
        if ov_s >= ov_e {
            continue;
        }
        slices.push(ExtentSlice {
            key: key::extent_key(opened.ino, s),
            offset: (ov_s - s) as u32,
            length: (ov_e - ov_s) as u32,
            dest_offset: (ov_s - off) as usize,
        });
    }
    (actual, slices)
}

/// Read `[off, off+len)` of an opened fuse file into a freshly-allocated buffer,
/// fanning out across the covering extents via `get_many_into`. Sparse gaps /
/// short extents zero-fill. Returns the EOF-clamped bytes (may be shorter than
/// `len`, empty at/after EOF).
pub async fn read(
    cluster: &ClusterClient,
    opened: &OpenedExtents,
    off: u64,
    len: u32,
) -> Result<Vec<u8>> {
    let (actual, mut slices) = plan_read(opened, off, len);
    if actual == 0 {
        return Ok(Vec::new());
    }
    slices.sort_by_key(|s| s.dest_offset);

    let mut buf = vec![0u8; actual];
    let mut rest = buf.as_mut_slice();
    let mut consumed = 0usize;
    let mut dests: Vec<&mut [u8]> = Vec::with_capacity(slices.len());
    for sl in &slices {
        let gap = sl.dest_offset - consumed;
        let (_, after_gap) = rest.split_at_mut(gap);
        let (head, tail) = after_gap.split_at_mut(sl.length as usize);
        dests.push(head);
        rest = tail;
        consumed = sl.dest_offset + sl.length as usize;
    }

    let mut items: Vec<GetManyItem> = slices
        .iter()
        .zip(dests)
        .map(|(sl, dest)| GetManyItem {
            key: &sl.key,
            offset: sl.offset,
            length: sl.length,
            dest,
            reg: None,
        })
        .collect();
    let results = cluster
        .get_many_into(&mut items, BATCH_GET_DEFAULT_CONCURRENCY)
        .await;
    drop(items);
    for r in &results {
        if let Err(e) = r {
            return Err(anyhow!("extent read failed: {e}"));
        }
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opened(size: u64, extents: Vec<(u64, u32)>) -> OpenedExtents {
        OpenedExtents {
            ino: 7,
            size,
            extents,
        }
    }

    #[test]
    fn plan_full_two_extents() {
        let m = MAX_EXTENT as u64;
        let o = opened(
            m + 2 * 1024 * 1024,
            vec![(0, MAX_EXTENT as u32), (m, 2 * 1024 * 1024)],
        );
        let (actual, slices) = plan_read(&o, 0, u32::MAX);
        assert_eq!(actual, (m + 2 * 1024 * 1024) as usize);
        assert_eq!(slices.len(), 2);
        assert_eq!(slices[0].dest_offset, 0);
        assert_eq!(slices[0].length, MAX_EXTENT as u32);
        assert_eq!(slices[1].dest_offset, m as usize);
        assert_eq!(slices[1].length, 2 * 1024 * 1024);
    }

    #[test]
    fn plan_cross_extent_subrange() {
        let m = MAX_EXTENT as u64;
        let o = opened(m + m, vec![(0, MAX_EXTENT as u32), (m, MAX_EXTENT as u32)]);
        // read [m-100, m+100) spans the boundary
        let (actual, slices) = plan_read(&o, m - 100, 200);
        assert_eq!(actual, 200);
        assert_eq!(slices.len(), 2);
        // first extent contributes [m-100, m): in-extent offset m-100, len 100
        assert_eq!(slices[0].offset, (m - 100) as u32);
        assert_eq!(slices[0].length, 100);
        assert_eq!(slices[0].dest_offset, 0);
        // second extent contributes [m, m+100): in-extent offset 0, len 100
        assert_eq!(slices[1].offset, 0);
        assert_eq!(slices[1].length, 100);
        assert_eq!(slices[1].dest_offset, 100);
    }

    #[test]
    fn plan_eof_clamps() {
        let o = opened(1000, vec![(0, 1000)]);
        let (actual, slices) = plan_read(&o, 990, 4096);
        assert_eq!(actual, 10);
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].length, 10);
        // at/after EOF → empty
        assert_eq!(plan_read(&o, 1000, 10).0, 0);
        assert_eq!(plan_read(&o, 2000, 10).0, 0);
    }

    #[test]
    fn plan_sparse_gap_skipped() {
        // two extents with a hole between them: [0,1000) and [5000,6000)
        let o = opened(6000, vec![(0, 1000), (5000, 1000)]);
        let (actual, slices) = plan_read(&o, 0, 6000);
        assert_eq!(actual, 6000);
        // only the two real extents are fetched; the [1000,5000) hole stays zero
        assert_eq!(slices.len(), 2);
        assert_eq!(slices[0].dest_offset, 0);
        assert_eq!(slices[1].dest_offset, 5000);
    }
}
