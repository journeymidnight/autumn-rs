//! Read path: chunk-based read via the shared `ClusterClient::get_many_into`
//! batch primitive (F244-B).
//!
//! Two-phase design (autumn-fuse perf fix #1):
//! - `prepare`: under the dispatcher's `&mut FsState`, do cheap synchronous
//!   work — flush an overlapping write buf, fetch inode meta, plan each chunk
//!   (key + sub-range). Clones the `Rc<ClusterClient>` into the plan. NO route
//!   pre-resolution: `get_many_into` routes per key (cached binary search), so
//!   `execute` needs no `&FsState`.
//! - `execute`: spawned task (see `dispatch.rs`). Pre-allocates the zero-filled
//!   result, gives each chunk its own disjoint `&mut` slice, and issues ONE
//!   `get_many_into` over all chunks (pipelined internally). Missing chunks
//!   leave zeros (sparse-file semantics); a hard RPC/routing error surfaces as
//!   `Err` (→ EIO), matching the pre-F244 prepare-time error.
//!
//! F244-B consolidated the hand-rolled `join_all` fan-out onto the shared
//! `get_many_into` (also used by the python kvcache `BatchClient`). Per-chunk ZC
//! (`MSG_GET_ZC`) engages for chunks >= 64 KiB via `zc_worthwhile` (the 256 KiB
//! `CHUNK_SIZE` qualifies); smaller chunks use the regular `MSG_GET` path
//! (bounded by the client's 30 s `rpc_timeout`). **Tradeoff:** the ZC path has
//! no per-call timeout (cancel-safety — the dest must outlive the recv), so a
//! hung PS blocks a large-chunk read instead of erroring after 30 s. This is the
//! F239/F216-E ZC tradeoff, accepted here for the copy elimination.

use std::rc::Rc;

use anyhow::{anyhow, Result};

use autumn_client::{ClusterClient, GetManyItem, BATCH_GET_DEFAULT_CONCURRENCY};

use crate::key;
use crate::meta::get_inode;
use crate::schema::*;
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
}

/// One chunk to fetch: a KV key + the sub-range `[offset, offset+length)`
/// wanted from its value.
pub struct ChunkSpec {
    pub key: Vec<u8>,
    pub offset: u32,
    pub length: u32,
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
            chunks: Vec::new(),
        });
    }

    // Chunked read: plan each chunk's key + sub-range. Routing happens later,
    // inside `get_many_into` (cached binary search per key) — no `&state` needed
    // in `execute`.
    let first_chunk = offset / CHUNK_SIZE as u64;
    let last_chunk = (read_end - 1) / CHUNK_SIZE as u64;
    let mut chunks: Vec<ChunkSpec> = Vec::with_capacity((last_chunk - first_chunk + 1) as usize);
    for chunk_idx in first_chunk..=last_chunk {
        let chunk_start = chunk_idx * CHUNK_SIZE as u64;
        let sub_offset = if offset > chunk_start {
            (offset - chunk_start) as u32
        } else {
            0
        };
        let chunk_remaining = CHUNK_SIZE as u32 - sub_offset;
        let bytes_left = (read_end - (chunk_start + sub_offset as u64)) as u32;
        let sub_length = std::cmp::min(chunk_remaining, bytes_left);

        chunks.push(ChunkSpec {
            key: key::chunk_key(ino, chunk_idx),
            offset: sub_offset,
            length: sub_length,
        });
    }

    Ok(ReadPlan {
        inline_result: None,
        actual_size,
        client: state.client.clone(),
        chunks,
    })
}

/// Phase 2: spawned task — one batched `get_many_into` over all chunks.
///
/// Pre-allocates the zero-filled result and gives each chunk its OWN disjoint
/// `&mut` sub-slice (sized to its `sub_length`). `get_many_into` writes each
/// successful chunk straight into its slice (ZC into the slice for >= 64 KiB
/// chunks); a missing chunk leaves zeros (sparse). A hard RPC/routing error on
/// any chunk surfaces as `Err` so the dispatcher returns EIO.
pub async fn execute(plan: ReadPlan) -> Result<Vec<u8>> {
    if let Some(inline) = plan.inline_result {
        return Ok(inline);
    }
    let actual_size = plan.actual_size;
    let client = plan.client;
    let chunks = plan.chunks;

    let total: usize = chunks.iter().map(|c| c.length as usize).sum();
    let mut result = vec![0u8; total];
    let mut rest = result.as_mut_slice();
    let mut dests: Vec<&mut [u8]> = Vec::with_capacity(chunks.len());
    for c in &chunks {
        let (head, tail) = rest.split_at_mut(c.length as usize);
        dests.push(head);
        rest = tail;
    }

    let mut items: Vec<GetManyItem> = chunks
        .iter()
        .zip(dests)
        .map(|(c, dest)| GetManyItem {
            key: &c.key,
            offset: c.offset,
            length: c.length,
            dest,
            reg: None,
        })
        .collect();
    let results = client
        .get_many_into(&mut items, BATCH_GET_DEFAULT_CONCURRENCY)
        .await;
    drop(items); // release the &mut borrows of `result`

    // Propagate a hard RPC/routing failure (pre-F244 surfaced this at
    // prepare-time as EIO); `Ok(None)` (missing chunk) stays sparse-zero-filled.
    for r in &results {
        if let Err(e) = r {
            return Err(anyhow!("chunk read failed: {e}"));
        }
    }

    result.truncate(actual_size);
    Ok(result)
}

/// Convenience wrapper for the in-thread (non-spawn) path: prepare + execute.
/// Kept for tests / fallback callers that don't want to spawn.
pub async fn read(state: &mut FsState, ino: u64, offset: i64, size: u32) -> Result<Vec<u8>> {
    let plan = prepare(state, ino, offset, size).await?;
    execute(plan).await
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
