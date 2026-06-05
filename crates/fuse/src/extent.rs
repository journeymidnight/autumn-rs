//! F247 variable-length extents: runtime extent map + the data read/write/
//! truncate/delete operations keyed by logical byte offset.
//!
//! Files are stored as **variable-length extents** keyed by their logical byte
//! offset (`[0x03][ino][logical_off BE]`, see `key.rs`), each value capped at
//! [`MAX_EXTENT`] (8 MiB). A sequential (write-once) stream coalesces into
//! `MAX_EXTENT`-sized extents; the last/partial extent is shorter → "variable
//! like Linux extents". This replaces the pre-F247 fixed-256 KiB-chunk scheme.
//!
//! **Persistent source of truth = the extent KV keys themselves** (the implicit-
//! key design: no extent list in `InodeMeta`). This module keeps a per-inode
//! *runtime* cache of `(logical_start, value_len)` (`InodeState.extents`) so the
//! hot read/write paths don't re-scan; it is rebuilt lazily from a range-scan of
//! the `[0x03][ino]` prefix (start offsets) + length inference (next start /
//! file size), and maintained incrementally on write / invalidated on truncate.
//!
//! **Invariant maintained by every writer here: extents NEVER overlap.** A read
//! therefore requests each overlapping extent's exact `[start, start+len)`
//! sub-range; the PS get clamps to the real value length and the caller's dest
//! is zero-filled, so short/last extents AND sparse holes both read correctly
//! even if a cached `len` over-estimates (cold inference of a sparse file).

use anyhow::{anyhow, Result};
use bytes::Bytes;

use crate::key;
use crate::schema::MAX_EXTENT;
use crate::state::FsState;

/// Pipeline depth for the append-write hot path. cp / dd / model-checkpoint
/// streamers send sequential 8 MiB extents — pre-F178 every put was awaited
/// serially (in_flight=1) and a single-stream cp ceiling was `8 MiB / RPC_RTT`
/// ≈ 100 MB/s. With this many in-flight puts the ceiling becomes
/// `N × 8 MiB / RPC_RTT`, bounded by the per-PS partition's pwritev throughput.
/// 8 is conservative — `autumn_client::BATCH_PUT_DEFAULT_CONCURRENCY` is 32 —
/// but a single fuse mount writing one file streams to ONE partition (one
/// inode → one partition), and the PS's `partition_loop` group-commit is
/// already amortising at the head; going wider mostly adds memory pressure
/// (each in-flight = one 8 MiB Bytes alloc).
const APPEND_PIPELINE_DEPTH: usize = 8;

/// Range-scan page size when rebuilding the extent map of a large file.
const RANGE_PAGE: u32 = 8192;

/// Return this inode's extent map (`(start, len)` sorted by start), loading the
/// runtime cache from the KV layer when cold. Returns a clone so callers can use
/// it without holding a borrow on `state` across `await`s.
pub async fn extents_snapshot(
    state: &mut FsState,
    ino: u64,
    file_size: u64,
) -> Result<Vec<(u64, u32)>> {
    if let Some(is) = state.inodes.get(&ino) {
        if let Some(ext) = &is.extents {
            return Ok(ext.clone());
        }
    }
    let extents = scan_extents(state, ino, file_size).await?;
    if let Some(is) = state.inodes.get_mut(&ino) {
        is.extents = Some(extents.clone());
    }
    Ok(extents)
}

/// Paginated range-scan of `[0x03][ino]` → sorted start offsets, then infer each
/// extent's value length (`next_start - start`, last clamped to `file_size`),
/// capped at `MAX_EXTENT`.
async fn scan_extents(state: &mut FsState, ino: u64, file_size: u64) -> Result<Vec<(u64, u32)>> {
    let prefix = key::extent_prefix(ino);
    let mut starts: Vec<u64> = Vec::new();
    let mut start_key = prefix.clone();
    loop {
        let keys = state.kv_range_keys(&prefix, &start_key, RANGE_PAGE).await?;
        let got = keys.len();
        for k in &keys {
            if let Some((_, off)) = key::parse_extent_key(k) {
                starts.push(off);
            }
        }
        if got < RANGE_PAGE as usize {
            break;
        }
        // Advance strictly past the last start: extent_key(ino, last+1) is the
        // smallest valid 17-byte key > the last returned extent key, so the
        // inclusive range `start` excludes it without re-reading.
        let last_off = starts.last().copied().unwrap_or(0);
        start_key = key::extent_key(ino, last_off.saturating_add(1));
    }
    starts.sort_unstable();
    starts.dedup();
    Ok(infer_lengths(&starts, file_size))
}

/// Infer extent lengths from neighbouring starts (non-overlap upper bound) and
/// the file size for the last extent, capped at `MAX_EXTENT`.
fn infer_lengths(starts: &[u64], file_size: u64) -> Vec<(u64, u32)> {
    let mut out = Vec::with_capacity(starts.len());
    for (i, &s) in starts.iter().enumerate() {
        let end = if i + 1 < starts.len() {
            starts[i + 1]
        } else {
            file_size.max(s)
        };
        let len = (end.saturating_sub(s)).min(MAX_EXTENT as u64) as u32;
        out.push((s, len));
    }
    out
}

/// Force a rebuild of the runtime extent cache on next access.
pub fn invalidate(state: &mut FsState, ino: u64) {
    if let Some(is) = state.inodes.get_mut(&ino) {
        is.extents = None;
    }
}

/// `(floor, next)` for `pos` over a sorted extent map: `floor` = the extent with
/// the largest start `<= pos`; `next` = the start of the first extent `> pos`.
fn floor_and_next(ext: &[(u64, u32)], pos: u64) -> (Option<(u64, u32)>, Option<u64>) {
    let idx = ext.partition_point(|&(s, _)| s <= pos);
    let floor = if idx > 0 { Some(ext[idx - 1]) } else { None };
    let next = ext.get(idx).map(|&(s, _)| s);
    (floor, next)
}

/// Insert or update `(start, len)` in a sorted extent map.
fn upsert(ext: &mut Vec<(u64, u32)>, start: u64, len: u32) {
    match ext.binary_search_by_key(&start, |&(s, _)| s) {
        Ok(i) => ext[i].1 = len,
        Err(i) => ext.insert(i, (start, len)),
    }
}

/// Write `data` at `[offset, offset+data.len())`, splitting into non-overlapping
/// extents of at most `MAX_EXTENT`. Maintains the runtime extent cache.
///
/// - **Append / gap (the write-once hot path):** `offset` lands past every
///   existing extent (or in a hole) → fresh `kv_put(extent_key(offset), …)`, no
///   read. Capped at the next extent's start so the new extent can't overlap.
/// - **Overwrite (rare for model files):** `offset` falls inside an existing
///   extent → read-modify-write that extent in place (its start key is kept, so
///   no overlap is introduced); the value may grow up to the next start / cap.
pub async fn write_region(
    state: &mut FsState,
    ino: u64,
    offset: u64,
    data: &[u8],
    file_size: u64,
) -> Result<()> {
    if data.is_empty() {
        return Ok(());
    }
    let mut ext = extents_snapshot(state, ino, file_size).await?;

    // Plan + batch the append steps so the append-only hot path (cp / dd /
    // model checkpoint streamer — `pos` strictly past every existing extent)
    // dispatches one put per ≤ 8 MiB chunk with `APPEND_PIPELINE_DEPTH` in
    // flight. RMW falls back to inline read-modify-write (rare; in-place
    // overwrite of a previously-written extent).
    //
    // Pre-pipelining this loop awaited every `state.kv_put` serially
    // (in_flight=1), making single-stream cp throughput `~8 MiB / RPC_RTT`.
    // We accumulate consecutive Append plans into `append_keys` /
    // `append_values`; an RMW interrupts → flush the pending batch
    // (preserving correctness: the RMW reads the same KV namespace and
    // could race a pending put on a nearby key) → do the RMW serial → keep
    // going. After the loop the trailing batch flushes.
    let mut pos = offset;
    let mut rest = data;
    let mut append_keys: Vec<Vec<u8>> = Vec::new();
    let mut append_values: Vec<Bytes> = Vec::new();
    let mut append_upserts: Vec<(u64, u32)> = Vec::new();

    while !rest.is_empty() {
        let (floor, next) = floor_and_next(&ext, pos);
        let next_start = next.unwrap_or(u64::MAX);

        match floor {
            // `pos` is inside an existing extent → read-modify-write it.
            Some((fs, fl)) if pos < fs + fl as u64 => {
                // Drain pending appends before any serial KV op on `state`.
                flush_appends(
                    state,
                    &mut append_keys,
                    &mut append_values,
                    &mut append_upserts,
                    &mut ext,
                )
                .await?;
                let cap = (fs + MAX_EXTENT as u64).min(next_start);
                let end = (pos + rest.len() as u64).min(cap);
                let n = (end - pos) as usize;
                let ck = key::extent_key(ino, fs);
                let mut val = state.kv_get(&ck).await.unwrap_or_default();
                let in_off = (pos - fs) as usize;
                if val.len() < in_off + n {
                    val.resize(in_off + n, 0);
                }
                val[in_off..in_off + n].copy_from_slice(&rest[..n]);
                let new_len = val.len() as u32;
                state.kv_put(&ck, &val).await?;
                upsert(&mut ext, fs, new_len);
                pos += n as u64;
                rest = &rest[n..];
            }
            // Fresh territory (append past EOF or a clean hole) → new extent.
            _ => {
                let cap = (pos + MAX_EXTENT as u64).min(next_start);
                let end = (pos + rest.len() as u64).min(cap);
                let n = (end - pos) as usize;
                append_keys.push(key::extent_key(ino, pos));
                // One alloc per chunk (same cost the pre-pipelined path paid
                // via the SDK's internal `value.to_vec()` in `put_opts`).
                append_values.push(Bytes::copy_from_slice(&rest[..n]));
                append_upserts.push((pos, n as u32));
                pos += n as u64;
                rest = &rest[n..];
            }
        }
    }
    // Trailing batch — the pure-append hot path lands here.
    flush_appends(
        state,
        &mut append_keys,
        &mut append_values,
        &mut append_upserts,
        &mut ext,
    )
    .await?;

    if let Some(is) = state.inodes.get_mut(&ino) {
        is.extents = Some(ext);
    }
    Ok(())
}

/// Dispatch a batch of append puts concurrently via `ClusterClient::put_many`
/// (sliding window depth = [`APPEND_PIPELINE_DEPTH`]), then apply the
/// corresponding `(start, len)` upserts to the in-memory extent map in input
/// order. Empties the input vecs.
///
/// Each item's value is a `Bytes` so ZC engages for ≥ 64 KiB (see
/// `autumn_client::zc_worthwhile`). On any item failure the whole batch is
/// reported as an error and the in-memory extent map is left UN-upserted for
/// every item past the first failure — callers treat this as a "this fuse
/// write failed, retry the whole flush" outcome (matches the pre-pipelined
/// fail-on-first-Err semantics).
async fn flush_appends(
    state: &FsState,
    keys: &mut Vec<Vec<u8>>,
    values: &mut Vec<Bytes>,
    upserts: &mut Vec<(u64, u32)>,
    ext: &mut Vec<(u64, u32)>,
) -> Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    debug_assert_eq!(keys.len(), values.len());
    debug_assert_eq!(keys.len(), upserts.len());
    let drained_keys = std::mem::take(keys);
    let drained_values = std::mem::take(values);
    let drained_upserts = std::mem::take(upserts);
    let items: Vec<(&[u8], Bytes)> = drained_keys
        .iter()
        .zip(drained_values.iter())
        .map(|(k, v)| (k.as_slice(), v.clone()))
        .collect();
    let results = state.client.put_many(&items, APPEND_PIPELINE_DEPTH).await;
    for (i, r) in results.into_iter().enumerate() {
        r.map_err(|e| anyhow!("KV put: {e}"))?;
        let (start, len) = drained_upserts[i];
        upsert(ext, start, len);
    }
    Ok(())
}

/// Delete every data extent of an inode (used by `unlink`). Range-scan + delete;
/// needs no length info, so it does not touch the runtime cache beyond
/// invalidating it.
pub async fn delete_all_extents(state: &mut FsState, ino: u64) -> Result<()> {
    let prefix = key::extent_prefix(ino);
    let mut start_key = prefix.clone();
    loop {
        let keys = state.kv_range_keys(&prefix, &start_key, RANGE_PAGE).await?;
        let got = keys.len();
        let mut last_off = 0u64;
        for k in &keys {
            if let Some((_, off)) = key::parse_extent_key(k) {
                last_off = off;
                let _ = state.kv_delete(k).await;
            }
        }
        if got < RANGE_PAGE as usize {
            break;
        }
        start_key = key::extent_key(ino, last_off.saturating_add(1));
    }
    invalidate(state, ino);
    Ok(())
}

/// Shrink a file's extents to `new_size`: delete extents that start at/after
/// `new_size`, and truncate the value of the extent straddling `new_size`.
/// Grow (new_size > old_size) is metadata-only (sparse tail) — no extent work.
pub async fn truncate_extents(
    state: &mut FsState,
    ino: u64,
    new_size: u64,
    old_size: u64,
) -> Result<()> {
    if new_size >= old_size {
        return Ok(());
    }
    let ext = extents_snapshot(state, ino, old_size).await?;
    for &(s, len) in &ext {
        if s >= new_size {
            // Extent begins beyond the new EOF → drop it entirely.
            let _ = state.kv_delete(&key::extent_key(ino, s)).await;
        } else if s + len as u64 > new_size {
            // Extent straddles the new EOF → truncate its value.
            let keep = (new_size - s) as usize;
            let ck = key::extent_key(ino, s);
            if let Ok(mut val) = state.kv_get(&ck).await {
                if val.len() > keep {
                    val.truncate(keep);
                    state.kv_put(&ck, &val).await?;
                }
            }
        }
    }
    invalidate(state, ino);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infer_lengths_contiguous() {
        // three contiguous extents: [0,8M)[8M,16M)[16M, 20M)
        let m = MAX_EXTENT as u64;
        let starts = vec![0, m, 2 * m];
        let got = infer_lengths(&starts, 2 * m + 4 * 1024 * 1024);
        assert_eq!(got[0], (0, MAX_EXTENT as u32));
        assert_eq!(got[1], (m, MAX_EXTENT as u32));
        assert_eq!(got[2], (2 * m, 4 * 1024 * 1024)); // last = file_size - start
    }

    #[test]
    fn infer_lengths_caps_at_max_extent() {
        // a sparse hole between two starts wider than MAX_EXTENT: len is capped,
        // not the full gap (the get will clamp the over-request to zeros).
        let starts = vec![0u64, 100 * 1024 * 1024];
        let got = infer_lengths(&starts, 200 * 1024 * 1024);
        assert_eq!(got[0].1, MAX_EXTENT as u32);
    }

    #[test]
    fn floor_and_next_basic() {
        let m = MAX_EXTENT as u64;
        let ext = vec![(0u64, MAX_EXTENT as u32), (m, MAX_EXTENT as u32)];
        // inside first extent
        let (f, n) = floor_and_next(&ext, 1024);
        assert_eq!(f, Some((0, MAX_EXTENT as u32)));
        assert_eq!(n, Some(m));
        // exactly at second start
        let (f, n) = floor_and_next(&ext, m);
        assert_eq!(f, Some((m, MAX_EXTENT as u32)));
        assert_eq!(n, None);
        // before everything
        let ext2 = vec![(m, 10u32)];
        let (f, n) = floor_and_next(&ext2, 0);
        assert_eq!(f, None);
        assert_eq!(n, Some(m));
    }

    #[test]
    fn upsert_keeps_sorted_and_updates() {
        let mut ext = vec![(0u64, 10u32), (100, 20u32)];
        upsert(&mut ext, 50, 5);
        assert_eq!(ext, vec![(0, 10), (50, 5), (100, 20)]);
        upsert(&mut ext, 0, 99);
        assert_eq!(ext[0], (0, 99));
    }
}
