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
use crate::schema::{StripeLayout, MAX_EXTENT};
use crate::state::FsState;

/// Pipeline depth for the append-write hot path. cp / dd / model-checkpoint
/// streamers send sequential 8 MiB extents — pre-F178 every put was awaited
/// serially (in_flight=1) and a single-stream cp ceiling was `8 MiB / RPC_RTT`
/// ≈ 100 MB/s. With this many in-flight puts the ceiling becomes
/// `N × 8 MiB / RPC_RTT`, bounded by the per-PS partition's pwritev throughput.
/// 8 is conservative — `autumn_client::BATCH_PUT_DEFAULT_CONCURRENCY` is 32 —
// APPEND_PIPELINE_DEPTH removed: `put_many` no longer takes a `concurrency`
// argument. The SDK groups items by owning partition and issues one
// MSG_BATCH_PUT (or per-op MSG_PUT_ZC for >=64 KiB values) per partition;
// each partition's RPCs ride the multiplexed PS connection. Fuse-side
// pipelining now happens naturally via the per-partition pipeline at the
// PS partition_loop level.

/// Range-scan page size when rebuilding the extent map of a large file.
const RANGE_PAGE: u32 = 8192;

/// Return this inode's extent map (`(start, len)` sorted by start), loading the
/// runtime cache from the KV layer when cold. Returns a clone so callers can use
/// it without holding a borrow on `state` across `await`s.
pub async fn extents_snapshot(
    state: &mut FsState,
    ino: u64,
    file_size: u64,
    stripe: Option<&StripeLayout>,
) -> Result<Vec<(u64, u32)>> {
    if let Some(is) = state.inodes.get(&ino) {
        if let Some(ext) = &is.extents {
            return Ok(ext.clone());
        }
    }
    // F-FS-STRIPE (coco P1): a striped inode's extents live under
    // `[0x03][lane][ino][off]`, NOT scannable by `[0x03][ino]`, so the map is
    // COMPUTED from size. `stripe` is passed EXPLICITLY by the caller from the
    // AUTHORITATIVE meta — NOT read from `state.inodes` (get_inode returns the
    // meta but does NOT populate the cache on a cold miss, so a cache lookup
    // would wrongly see None for a striped file → legacy scan → zeros).
    let extents = scan_extents(state, ino, file_size, stripe).await?;
    if let Some(is) = state.inodes.get_mut(&ino) {
        is.extents = Some(extents.clone());
    }
    Ok(extents)
}

/// Build the extent map. STRIPED (F-FS-STRIPE) → COMPUTE the MAX_EXTENT-granular
/// offsets from `file_size` (extents are aligned + bounded by size; the writer
/// never exceeds MAX_EXTENT). LEGACY → paginated range-scan of `[0x03][ino]` →
/// sorted starts. Both then `infer_lengths`.
async fn scan_extents(
    state: &mut FsState,
    ino: u64,
    file_size: u64,
    stripe: Option<&StripeLayout>,
) -> Result<Vec<(u64, u32)>> {
    if let Some(s) = stripe {
        // coco P3: bounded enumeration (corrupt huge size can't wrap / OOM).
        // The stride is the file's PERSISTED unit_bytes, not MAX_EXTENT — see
        // `striped_extent_offsets` for why reading it from the constant is a
        // silent-zero-fill trap the day the constant is retuned.
        let (_, unit) = s.checked().map_err(|e| anyhow!(e))?;
        let starts =
            crate::schema::striped_extent_offsets(file_size, unit).map_err(|e| anyhow!(e))?;
        return Ok(infer_lengths(&starts, file_size));
    }
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
    let mut ext = extents_snapshot(state, ino, file_size, None).await?;
    // BUG-LEASE-8 (coco P1): leftover extents from a crashed shrink must
    // not merge into / resurface under this write. Cold post-crash path —
    // whole-key leftovers are visible in the prefix scan for free; a
    // sparse grow is the one shape where a stale straddler TAIL (value
    // longer than size implies) becomes readable without any key-level
    // evidence, so it always sweeps.
    if offset > file_size || ext.iter().any(|&(s, _)| s >= file_size) {
        clean_beyond_eof(state, ino, file_size).await?;
        ext = extents_snapshot(state, ino, file_size, None).await?;
    }
    // BUG-LEASE-2 Phase 2: stamp every data-extent put with the held WRITE
    // lease's version so a revoked writer's late RPCs are fenced at the PS.
    let lease = state.write_lease_for(ino);

    // Plan + batch the append steps so the append-only hot path (cp / dd /
    // model checkpoint streamer — `pos` strictly past every existing extent)
    // dispatches one put per ≤ 8 MiB chunk via `put_many` (SDK groups by
    // partition + server-batches). RMW falls back to inline read-modify-write (rare; in-place
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
                    lease,
                )
                .await?;
                let cap = (fs + MAX_EXTENT as u64).min(next_start);
                let end = (pos + rest.len() as u64).min(cap);
                let n = (end - pos) as usize;
                let ck = key::extent_key(ino, fs);
                // RMW read BARRIER (must mirror `clean_beyond_eof`'s
                // kv_get_opt): a hard get error (transient RPC / routing /
                // storage failure) must PROPAGATE, never be swallowed. Treating
                // the existing extent as empty here zero-fills the untouched
                // prefix `[fs, pos)` and TRUNCATES the untouched suffix
                // `[pos+n, …)`, then `put`s the result — fabricating data on a
                // *successful* write (the one crash/transient window in this
                // layer that corrupts rather than just loses an un-fsynced
                // write). Propagate → fuse EIO, the app retries the overwrite;
                // a genuinely-absent key (`Ok(None)`) is the only case that may
                // be treated as empty (a mapped extent's key should always
                // exist, but if it's truly gone there is nothing to preserve —
                // empty is the safe sparse value).
                let mut val = state.kv_get_opt(&ck).await?.unwrap_or_default();
                let in_off = (pos - fs) as usize;
                if val.len() < in_off + n {
                    val.resize(in_off + n, 0);
                }
                val[in_off..in_off + n].copy_from_slice(&rest[..n]);
                let new_len = val.len() as u32;
                state.kv_put_fenced(&ck, &val, lease).await?;
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
        lease,
    )
    .await?;

    if let Some(is) = state.inodes.get_mut(&ino) {
        is.extents = Some(ext);
    }
    Ok(())
}

/// Dispatch a batch of append puts via `ClusterClient::put_many` (SDK
/// groups by partition and issues server-batched MSG_BATCH_PUT for small
/// values, per-op MSG_PUT_ZC for ≥ 64 KiB), then apply the
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
    lease: autumn_client::WriteLease,
) -> Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    debug_assert_eq!(keys.len(), values.len());
    debug_assert_eq!(keys.len(), upserts.len());
    let drained_keys = std::mem::take(keys);
    let drained_values = std::mem::take(values);
    let drained_upserts = std::mem::take(upserts);
    // This append path calls `put_many_fenced` directly on the client, which
    // prepends `fs/{tenant}/` — same as the RMW-overwrite branch (kv_put_fenced)
    // and the read path (read::prepare). The bare `key::*` keys are relative.
    let items: Vec<(&[u8], Bytes, u64)> = drained_keys
        .iter()
        .zip(drained_values.iter())
        .map(|(k, v)| (k.as_slice(), v.clone(), 0u64))
        .collect();
    let results = state.client.put_many_fenced(&items, lease).await;
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
/// UNLINK-1: remove an UNREACHABLE inode's data under an intent
/// tombstone — extents, the inode key, then the tombstone itself. Used
/// by unlink and rename-over-existing once the last dirent is gone, and
/// replayed by `sweep_unlink_tombstones` after a crash. Idempotent.
pub async fn remove_unreachable_inode(state: &mut FsState, ino: u64) -> Result<()> {
    let tk = crate::key::unlink_tombstone_key(ino);
    state.kv_put(&tk, b"1").await?;
    delete_all_extents(state, ino).await?;
    let ik = crate::key::inode_key(ino);
    let _ = state.kv_delete(&ik).await;
    let _ = state.kv_delete(&tk).await;
    state.inodes.remove(&ino);
    state.dirty_inodes.remove(&ino);
    Ok(())
}

/// UNLINK-1: mount-time replay of interrupted unlinks. Returns the
/// number of tombstones reaped.
pub async fn sweep_unlink_tombstones(state: &mut FsState) -> Result<usize> {
    let prefix = crate::key::unlink_tombstone_prefix();
    let mut start_key = prefix.clone();
    let mut n = 0;
    loop {
        // Paginated like delete_all_extents (coco P2: a single page
        // capped the per-mount sweep at 4096 tombstones).
        let keys = state.kv_range_keys(&prefix, &start_key, RANGE_PAGE).await?;
        let got = keys.len();
        let mut last_ino = 0u64;
        for k in &keys {
            if let Some(ino) = crate::key::parse_unlink_tombstone(k) {
                last_ino = ino;
                if let Err(e) = remove_unreachable_inode(state, ino).await {
                    tracing::warn!(
                        ino,
                        "unlink tombstone replay failed (retried next mount): {e}"
                    );
                } else {
                    n += 1;
                }
            }
        }
        if got < RANGE_PAGE as usize {
            break;
        }
        start_key = crate::key::unlink_tombstone_key(last_ino.saturating_add(1));
    }
    Ok(n)
}

pub async fn delete_all_extents(state: &mut FsState, ino: u64) -> Result<()> {
    // BUG-LEASE-2 Phase 2 (coco P1 #3): unlink's extent deletes are
    // fenced under the held WRITE lease (anonymous when none is held —
    // unlink without an open writer is a metadata-path operation).
    let lease = state.write_lease_for(ino);
    // F-FS-STRIPE: a striped inode's extents live under `[0x03][lane][ino][off]`,
    // NOT scannable by the `[0x03][ino]` prefix — a range-scan would MISS them and
    // leak. Look up stripe+size (cached from the caller's get_inode; on
    // tombstone-replay the cache is cold, so read the inode from KV if it still
    // exists) and delete the COMPUTED lane keys (MAX_EXTENT-granular up to size).
    let stripe_size: Option<(Option<StripeLayout>, u64)> =
        match state.inodes.get(&ino).map(|is| (is.meta.stripe.clone(), is.meta.size)) {
            Some(v) => Some(v),
            None => match state.kv_get_opt(&key::inode_key(ino)).await? {
                Some(bytes) => {
                    crate::schema::decode_inode_meta(&bytes).ok().map(|m| (m.stripe, m.size))
                }
                None => None, // inode already gone (crash mid-removal) — nothing to scan
            },
        };
    if let Some((Some(s), size)) = stripe_size {
        // coco P1: striped cleanup MUST fail-loud. `remove_unreachable_inode`
        // deletes the inode key + tombstone only AFTER this returns Ok; a striped
        // extent can't be re-enumerated without the inode's stripe+size, so if we
        // swallowed a checked()/delete error and returned Ok, the leftover lane
        // extents would leak PERMANENTLY (the tombstone-replay path loses its
        // recompute source). Propagate errors → the tombstone survives → next
        // mount retries. (Legacy path can range-scan on replay, so it stays
        // best-effort.)
        let (lanes, unit) = s.checked().map_err(|e| anyhow!("striped unlink {ino}: {e}"))?;
        // Stride from the PERSISTED unit_bytes: enumerating on today's
        // MAX_EXTENT after a retune would compute the WRONG keys and leak every
        // extent it failed to name (see `striped_extent_offsets`).
        for off in crate::schema::striped_extent_offsets(size, unit).map_err(|e| anyhow!(e))? {
            state
                .kv_delete_fenced(&key::extent_key_striped(ino, off, lanes, unit), lease)
                .await
                .map_err(|e| anyhow!("striped unlink {ino} extent @{off}: {e}"))?;
        }
        invalidate(state, ino);
        return Ok(());
    }
    // Legacy (non-striped, or meta unavailable): paginated range-scan + delete.
    let prefix = key::extent_prefix(ino);
    let mut start_key = prefix.clone();
    loop {
        let keys = state.kv_range_keys(&prefix, &start_key, RANGE_PAGE).await?;
        let got = keys.len();
        let mut last_off = 0u64;
        for k in &keys {
            if let Some((_, off)) = key::parse_extent_key(k) {
                last_off = off;
                let _ = state.kv_delete_fenced(k, lease).await;
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
    let ext = extents_snapshot(state, ino, old_size, None).await?;
    // BUG-LEASE-2 Phase 2 (coco P1 #3): truncate's whole-extent deletes
    // are fenced too — a revoked writer's late shrink must not delete
    // extents the new writer kept or rewrote.
    let lease = state.write_lease_for(ino);
    for &(s, len) in &ext {
        if s >= new_size {
            // Extent begins beyond the new EOF → drop it entirely.
            let _ = state.kv_delete_fenced(&key::extent_key(ino, s), lease).await;
        } else if s + len as u64 > new_size {
            // Extent straddles the new EOF → truncate its value.
            let keep = (new_size - s) as usize;
            let ck = key::extent_key(ino, s);
            if let Ok(mut val) = state.kv_get(&ck).await {
                if val.len() > keep {
                    val.truncate(keep);
                    state.kv_put_fenced(&ck, &val, lease).await?;
                }
            }
        }
    }
    invalidate(state, ino);
    Ok(())
}

/// BUG-LEASE-8 (coco P1): reap LEFTOVER extent state beyond `eof` — the
/// residue of a crash inside a shrink's post-commit cleanup window
/// (durable size already shrunk, extent destruction interrupted).
/// Leftovers are invisible while size stays put, but any later GROW
/// (truncate-up or a sparse write past EOF) would bring them back into
/// the readable range as RESURRECTED old data where POSIX requires
/// zeros. Called from the cold grow paths only:
/// - `write::truncate` grow branch (before the meta put), and
/// - `write_region` entry when leftovers are visible (a whole extent key
///   at/after `eof` in the prefix scan) or the write is a sparse grow
///   (`offset > file_size` — the one case a stale straddler tail can't
///   be detected from keys alone).
/// Deletes whole extent keys starting at/after `eof` and shortens the
/// straddling extent's VALUE (actual KV length, not the inferred one) to
/// `eof - start`. Idempotent; an error leaves leftovers for the next
/// grow to retry.
pub async fn clean_beyond_eof(state: &mut FsState, ino: u64, eof: u64) -> Result<()> {
    // Raw starts — inference is size-relative and exactly what we must
    // NOT trust here.
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
        let last_off = starts.last().copied().unwrap_or(0);
        start_key = key::extent_key(ino, last_off.saturating_add(1));
    }
    starts.sort_unstable();
    starts.dedup();
    let lease = state.write_lease_for(ino);
    for &s in &starts {
        if s >= eof {
            state
                .kv_delete_fenced(&key::extent_key(ino, s), lease)
                .await?;
        }
    }
    // Straddler: the largest start below eof may carry a stale tail
    // (value longer than the durable size implies). This function is the
    // pre-grow BARRIER — a hard kv error here must PROPAGATE (abort the
    // grow) rather than read as "already clean" (coco P1); only a
    // genuinely absent key (Ok(None)) may be skipped.
    if let Some(&s) = starts.iter().rev().find(|&&s| s < eof) {
        let keep = (eof - s) as usize;
        let ck = key::extent_key(ino, s);
        if let Some(mut val) = state.kv_get_opt(&ck).await? {
            if val.len() > keep {
                val.truncate(keep);
                state.kv_put_fenced(&ck, &val, lease).await?;
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
