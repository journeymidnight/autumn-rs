//! Read path: variable-length-extent read via the shared
//! `ClusterClient::get_many_into` batch primitive (F244-B + F247).
//!
//! Two-phase design (autumn-fuse perf fix #1):
//! - `prepare`: under the dispatcher's `&mut FsState`, do cheap work — flush an
//!   overlapping write buf, fetch inode meta, load the extent map (F247 runtime
//!   cache; range-scan on cold), and plan each overlapping extent (key +
//!   in-extent sub-range + absolute dest offset). Clones the `Rc<ClusterClient>`
//!   into the plan. `get_many_into` routes per key, so `execute` needs no state.
//! - `execute`: spawned task (see `dispatch.rs`). Pre-allocates the zero-filled
//!   result sized to the WHOLE read span, gives each extent its own disjoint
//!   `&mut` slice at the right absolute position, and issues ONE `get_many_into`.
//!   Sparse gaps between extents (and short/over-requested extents) stay zero; a
//!   hard RPC/routing error surfaces as `Err` (→ EIO).
//!
//! F247: data is variable-length extents keyed by logical offset (was fixed
//! 256 KiB chunks). The whole-extent reads are ≤ 8 MiB (`MAX_EXTENT`) so the
//! `zc_worthwhile` (≥ 64 KiB) per-extent ZC path (`MSG_GET_ZC`) engages on every
//! full-extent read — the win that motivated the move (model-file serving).
//! Sub-64 KiB tail/sub-range reads use the regular `MSG_GET` path (bounded by
//! the client's 30 s `rpc_timeout`). **Tradeoff:** the ZC path has no per-call
//! timeout (cancel-safety — the dest must outlive the recv), so a hung PS blocks
//! a large-extent read instead of erroring after 30 s (F239/F216-E tradeoff).

use std::rc::Rc;

use anyhow::{anyhow, Result};

use autumn_client::{ClusterClient, GetManyItem};

use crate::extent;
use crate::key;
use crate::meta::get_inode;
use crate::state::FsState;
use crate::write;

/// Plan handed off from `prepare` (sync over `&mut state`) to `execute`
/// (a spawned task — owns the `Rc<ClusterClient>` + chunk specs, no state ref).
pub struct ReadPlan {
    /// If `Some`, the read is fully satisfied without RPC (inline-data fast
    /// path, EOF, or empty range). `execute` returns this verbatim.
    pub inline_result: Option<Vec<u8>>,
    pub actual_size: usize,
    pub client: Rc<ClusterClient>,
    pub chunks: Vec<ChunkSpec>,
    /// F-DIRECT-MANY — captured from `FsState.direct_read` at `prepare` time
    /// (the spawned `execute` holds no `&FsState`). `true` → the batch read
    /// bypasses the PS via `get_many_direct`; `false` → PS-proxied `get_many_into`.
    pub direct_read: bool,
}

/// One extent slice to fetch: a KV key + the in-extent sub-range
/// `[offset, offset+length)` wanted from its value + where it lands in the
/// result buffer (`dest_offset`, absolute within the read span). `dest_offset`
/// makes the result robust to sparse gaps between extents (the gap stays zero).
pub struct ChunkSpec {
    pub key: Vec<u8>,
    pub offset: u32,
    pub length: u32,
    pub dest_offset: usize,
}

/// Phase 1: dispatcher-side synchronous-ish prep. Holds `&mut state` briefly for
/// the inode cache + write-buf flush. No route resolution, no long awaits.
pub async fn prepare(state: &mut FsState, ino: u64, offset: i64, size: u32) -> Result<ReadPlan> {
    if offset < 0 {
        return Err(anyhow!("negative offset"));
    }
    let offset = offset as u64;

    // Flush write buffer if it overlaps the read range (read-after-write consistency).
    if let Some(is) = state.inodes.get(&ino) {
        if let Some(ref wb) = is.write_buf {
            let wb_start = wb.offset as u64;
            let wb_end = wb_start + wb.len as u64;
            let read_end = offset + size as u64;
            if wb.len > 0 && wb_start < read_end && wb_end > offset {
                write::flush_inode(state, ino).await?;
            }
        }
    }

    let meta = get_inode(state, ino).await?;
    let file_size = meta.size;

    if offset >= file_size {
        return Ok(ReadPlan {
            inline_result: Some(Vec::new()),
            actual_size: 0,
            client: state.client.clone(),
            direct_read: state.direct_read,
            chunks: Vec::new(),
        });
    }

    let read_end = std::cmp::min(offset + size as u64, file_size);
    let actual_size = (read_end - offset) as usize;

    // Small file: inline data — return verbatim, no RPC.
    if let Some(ref data) = meta.inline_data {
        let start = offset as usize;
        let end = std::cmp::min(start + actual_size, data.len());
        let inline = if start >= data.len() {
            Vec::new()
        } else {
            data[start..end].to_vec()
        };
        return Ok(ReadPlan {
            inline_result: Some(inline),
            actual_size,
            client: state.client.clone(),
            direct_read: state.direct_read,
            chunks: Vec::new(),
        });
    }

    // Variable-extent read: load the extent map (F247) and plan each extent that
    // overlaps `[offset, read_end)`. Routing happens later inside `get_many_into`
    // (cached binary search per key) — `execute` needs no `&state`.
    let ext = extent::extents_snapshot(state, ino, file_size).await?;
    let mut chunks: Vec<ChunkSpec> = Vec::new();
    for &(start, len) in &ext {
        let e_end = start + len as u64;
        let ov_start = offset.max(start);
        let ov_end = read_end.min(e_end);
        if ov_start >= ov_end {
            continue;
        }
        chunks.push(ChunkSpec {
            key: key::extent_key(ino, start),
            offset: (ov_start - start) as u32,
            length: (ov_end - ov_start) as u32,
            dest_offset: (ov_start - offset) as usize,
        });
    }

    Ok(ReadPlan {
        inline_result: None,
        actual_size,
        client: state.client.clone(),
        direct_read: state.direct_read,
        chunks,
    })
}

/// Phase 2: spawned task — one batched read over all extent slices, via
/// `get_many_into` (PS-proxied ZC, default) or `get_many_direct` (EN-direct,
/// when the mount's `--direct-read` set `plan.direct_read`).
///
/// Pre-allocates the zero-filled result sized to the WHOLE read span and gives
/// each extent slice its OWN disjoint `&mut` sub-slice at its absolute
/// `dest_offset` (gaps between extents are skipped → stay zero, sparse-file
/// semantics). The batch primitive writes each successful extent into its slice
/// (`get_many_into`: ZC recv-into-dest for ≥ 64 KiB; `get_many_direct`: EN
/// direct read → copy, per-item PS-proxy fallback); a missing/short extent
/// leaves zeros. A hard RPC/routing error on any slice surfaces as `Err` → EIO.
/// Carve `region` — already sized to the read span and pre-zeroed by the caller
/// (sparse-gap / short-extent semantics) — into disjoint per-extent sub-slices
/// and issue ONE batched read into them. Shared by `execute` (allocates a `Vec`)
/// and `execute_into` (fills a caller-provided buffer, e.g. a CUDA-pinned host
/// tensor for the zero-copy `Fs.read_into` model-load seam).
async fn fill_region(
    client: Rc<ClusterClient>,
    mut chunks: Vec<ChunkSpec>,
    region: &mut [u8],
    direct_read: bool,
) -> Result<()> {
    // Sort by destination so the disjoint-slice carve walks forward.
    chunks.sort_by_key(|c| c.dest_offset);

    let mut rest = &mut region[..];
    let mut consumed = 0usize;
    let mut dests: Vec<&mut [u8]> = Vec::with_capacity(chunks.len());
    for c in &chunks {
        // Skip the gap before this extent (left as the caller pre-zeroed it),
        // then carve its slice.
        let gap = c.dest_offset - consumed;
        let (_, after_gap) = rest.split_at_mut(gap);
        let (head, tail) = after_gap.split_at_mut(c.length as usize);
        dests.push(head);
        rest = tail;
        consumed = c.dest_offset + c.length as usize;
    }

    let mut items: Vec<GetManyItem> = chunks
        .iter()
        .zip(dests)
        .map(|(c, dest)| GetManyItem {
            key: &c.key,
            offset: c.offset,
            length: c.length,
            dest,
        })
        .collect();
    // F-DIRECT-MANY: `direct_read` sends the ≥ 64 KiB whole-extent gets STRAIGHT
    // to an EN (`get_many_direct`), bypassing the PS on the data path; each item
    // falls back to the PS proxy on any direct-read failure, so it's safe even if
    // some ENs are unreachable. Default OFF ⇒ the PS-proxied ZC path
    // (`get_many_into`).
    let results = if direct_read {
        client.get_many_direct(&mut items).await
    } else {
        client.get_many_into(&mut items).await
    };
    drop(items); // release the &mut borrows of `region`

    // Propagate a hard RPC/routing failure (pre-F244 surfaced this at
    // prepare-time as EIO); `Ok(None)` (missing extent) stays sparse-zero-filled.
    for r in &results {
        if let Err(e) = r {
            return Err(anyhow!("extent read failed: {e}"));
        }
    }
    Ok(())
}

pub async fn execute(plan: ReadPlan) -> Result<Vec<u8>> {
    if let Some(inline) = plan.inline_result {
        return Ok(inline);
    }
    let mut result = vec![0u8; plan.actual_size];
    fill_region(plan.client, plan.chunks, &mut result, plan.direct_read).await?;
    Ok(result)
}

/// Zero-copy variant of `execute`: fill a CALLER-PROVIDED buffer (e.g. a
/// CUDA-pinned host buffer) instead of allocating a `Vec`. Writes the read span
/// into `dest[0..actual_size]` — pre-zeroing it for sparse / short-extent
/// semantics, exactly like `execute` — and returns the byte count. `dest` must
/// be at least `plan.actual_size` long. This is the seam behind `Fs.read_into`
/// that lets a model loader land extent bytes straight in pinned memory and
/// overlap the read with the H2D copy (the Run:ai-Model-Streamer pattern).
pub async fn execute_into(plan: ReadPlan, dest: &mut [u8]) -> Result<usize> {
    if let Some(inline) = plan.inline_result {
        let n = inline.len().min(dest.len());
        dest[..n].copy_from_slice(&inline[..n]);
        return Ok(n);
    }
    let actual_size = plan.actual_size;
    if dest.len() < actual_size {
        return Err(anyhow!(
            "read_into dest too small: {} < {actual_size}",
            dest.len()
        ));
    }
    let region = &mut dest[..actual_size];
    region.fill(0);
    fill_region(plan.client, plan.chunks, region, plan.direct_read).await?;
    Ok(actual_size)
}

/// Convenience wrapper for the in-thread (non-spawn) path: prepare + execute.
/// Kept for tests / fallback callers that don't want to spawn.
pub async fn read(state: &mut FsState, ino: u64, offset: i64, size: u32) -> Result<Vec<u8>> {
    let plan = prepare(state, ino, offset, size).await?;
    execute(plan).await
}

/// Zero-copy read into a caller buffer (the `Fs.read_into` seam): prepare +
/// `execute_into`. Reads up to `dest.len()` bytes from `offset` — capped at
/// 1 GiB per call (the `GetReq` size is u32; callers loop to fill a larger
/// buffer) — into `dest[0..n]` and returns `n`. Honors `state.direct_read`
/// (EN-direct when the connection set it).
pub async fn read_into(
    state: &mut FsState,
    ino: u64,
    offset: i64,
    dest: &mut [u8],
) -> Result<usize> {
    let cap = dest.len().min(1usize << 30);
    let plan = prepare(state, ino, offset, cap as u32).await?;
    execute_into(plan, &mut dest[..cap]).await
}

#[allow(dead_code)]
fn _read_typecheck_marker(
    state: &mut FsState,
    ino: u64,
    offset: i64,
    size: u32,
) -> impl std::future::Future<Output = Result<Vec<u8>>> + use<'_> {
    read(state, ino, offset, size)
}
