//! Read path: chunk-based read with parallel chunk fetch.
//!
//! Two-phase design (autumn-fuse perf fix #1):
//! - `prepare`: under the dispatcher's `&mut FsState`, do cheap synchronous
//!   work — flush overlapping write buf, fetch inode meta, plan chunks,
//!   resolve each chunk's route (PS client + GetReq). Returns a `ReadPlan`
//!   that owns `Rc<RpcClient>` per chunk.
//! - `execute`: pure parallel RPC dispatch; no `FsState` reference, no
//!   borrow held. Caller spawns this on the compio runtime so the
//!   dispatcher loop can pop the next FsRequest immediately.
//!
//! Together they fix the two read-side bottlenecks identified at audit:
//! Bug #1 (single dispatcher serializes all FUSE reads — capped at
//! ~13 k ops/s for 4K) and Bug #2 (chunks within one read fetched in
//! series — capped 8 M throughput at ~460 MB/s vs perf_check 1.7 GB/s).

use std::rc::Rc;

use anyhow::{Result, anyhow};
use bytes::Bytes;
use futures::future::join_all;

use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc::{
    self, GetReq, GetResp, MSG_GET, rkyv_decode, rkyv_encode,
};

use crate::key;
use crate::meta::get_inode;
use crate::schema::*;
use crate::state::FsState;
use crate::write;

/// Plan handed off from `prepare` (sync over `&mut state`) to `execute`
/// (no state, runs as a spawned task).
pub struct ReadPlan {
    /// If `Some`, the read is fully satisfied without RPC (inline-data
    /// fast path, EOF, or empty range). `execute` returns this verbatim.
    pub inline_result: Option<Vec<u8>>,
    pub actual_size: usize,
    pub requests: Vec<ChunkRequest>,
}

pub struct ChunkRequest {
    pub ps: Rc<RpcClient>,
    pub req: GetReq,
    pub fallback_zero_len: u32,
}

/// Phase 1: dispatcher-side synchronous-ish prep. Holds `&mut state`
/// briefly for routing lookups + inode cache. No long awaits here.
pub async fn prepare(
    state: &mut FsState,
    ino: u64,
    offset: i64,
    size: u32,
) -> Result<ReadPlan> {
    if offset < 0 {
        return Err(anyhow!("negative offset"));
    }
    let offset = offset as u64;

    // Flush write buffer if it overlaps with the read range (read-after-write consistency).
    if let Some(is) = state.inodes.get(&ino) {
        if let Some(ref wb) = is.write_buf {
            let wb_start = wb.offset as u64;
            let wb_end = wb_start + wb.len as u64;
            let read_end = offset + size as u64;
            if wb.len > 0 && wb_start < read_end && wb_end > offset {
                // Overlaps — flush first
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
            requests: Vec::new(),
        });
    }

    let read_end = std::cmp::min(offset + size as u64, file_size);
    let actual_size = (read_end - offset) as usize;

    // Small file: inline data — return verbatim, no RPC needed.
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
            requests: Vec::new(),
        });
    }

    // Chunked read: resolve each chunk's PS route (sync hashmap lookups +
    // possibly a connection establish on first call). After warmup all
    // calls hit the cached `ps_conns` map and complete in microseconds.
    let first_chunk = offset / CHUNK_SIZE as u64;
    let last_chunk = (read_end - 1) / CHUNK_SIZE as u64;
    let mut requests: Vec<ChunkRequest> =
        Vec::with_capacity((last_chunk - first_chunk + 1) as usize);
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

        let ck = key::chunk_key(ino, chunk_idx);
        let (part_id, addr) = state.client.resolve_key(&ck).await?;
        let ps = state.client.get_ps_client(&addr).await?;
        requests.push(ChunkRequest {
            ps,
            req: GetReq {
                part_id,
                key: ck,
                offset: sub_offset,
                length: sub_length,
            },
            fallback_zero_len: sub_length,
        });
    }

    Ok(ReadPlan {
        inline_result: None,
        actual_size,
        requests,
    })
}

/// Phase 2: pure parallel RPC fanout. No state references — caller
/// spawns this on the compio runtime so the dispatcher loop can move
/// on to the next FsRequest immediately.
pub async fn execute(plan: ReadPlan) -> Result<Vec<u8>> {
    if let Some(inline) = plan.inline_result {
        return Ok(inline);
    }

    // Fire every chunk's MSG_GET concurrently via join_all.
    // autumn-rpc's RpcClient is multiplexed; N concurrent calls on the
    // same TCP connection share one socket, processed as a pipelined
    // batch by the server's read SQ/CQ pipeline.
    let futs = plan.requests.into_iter().map(|cr| {
        let ps = cr.ps;
        let req = cr.req;
        let zero_len = cr.fallback_zero_len;
        async move {
            let payload = rkyv_encode(&req);
            let resp_bytes = match ps.call(MSG_GET, Bytes::from(payload)).await {
                Ok(b) => b,
                Err(_) => return Err(zero_len),
            };
            let resp: GetResp = match rkyv_decode(&resp_bytes) {
                Ok(r) => r,
                Err(_) => return Err(zero_len),
            };
            if resp.code != partition_rpc::CODE_OK {
                return Err(zero_len);
            }
            Ok(resp.value)
        }
    });
    let chunk_results = join_all(futs).await;

    // Stitch chunks in chunk order. Missing chunks (NotFound) are
    // zero-filled (sparse file semantics).
    let mut result = Vec::with_capacity(plan.actual_size);
    for r in chunk_results {
        match r {
            Ok(data) => result.extend_from_slice(&data),
            Err(zero_len) => result.extend(std::iter::repeat(0u8).take(zero_len as usize)),
        }
    }
    result.truncate(plan.actual_size);
    Ok(result)
}

/// Convenience wrapper for the in-thread (non-spawn) path: prepare + execute.
/// Kept for tests / fallback callers that don't want to spawn.
pub async fn read(state: &mut FsState, ino: u64, offset: i64, size: u32) -> Result<Vec<u8>> {
    let plan = prepare(state, ino, offset, size).await?;
    execute(plan).await
}

#[allow(dead_code)]
fn _read_typecheck_marker(state: &mut FsState, ino: u64, offset: i64, size: u32) -> impl std::future::Future<Output = Result<Vec<u8>>> + use<'_> {
    read(state, ino, offset, size)
}
