//! Write path: write buffering + flush into variable-length extents (F247).
//!
//! Mirrors the 3FS InodeWriteBuf pattern, but the flush unit is a variable-
//! length extent (≤ `MAX_EXTENT` = 8 MiB) keyed by logical offset, not a fixed
//! 256 KiB chunk:
//! - Sequential writes accumulate in the buffer (cap [`WRITE_BUF_CAP`] =
//!   `WRITE_BUF_EXTENTS * MAX_EXTENT` = 64 MiB).
//! - Gap detection: a non-sequential offset flushes the buffer first.
//! - A full buffer flushes the WHOLE buffer at once via `extent::write_region`,
//!   which splits it into `WRITE_BUF_EXTENTS` ≤ `MAX_EXTENT` extents and
//!   dispatches the puts concurrently (`put_many` at depth
//!   `APPEND_PIPELINE_DEPTH`). Pre-pipelining the buffer was exactly
//!   `MAX_EXTENT` and the flush was a single serial `put` — the cp ceiling
//!   was `MAX_EXTENT / RPC_RTT` (~270 MB/s). Now `flush ≈ WRITE_BUF_EXTENTS *
//!   MAX_EXTENT / RPC_RTT` until disk + replica fanout saturates.
//! - fsync/close flushes whatever remains (≤ WRITE_BUF_CAP) as the same
//!   pipelined batch (1 to `WRITE_BUF_EXTENTS` extents, last may be shorter).
//!
//! All extent placement / read-modify-write / non-overlap maintenance lives in
//! `crate::extent`; this module only manages the in-memory buffer + inode meta.

use anyhow::Result;

use crate::extent;
use crate::meta::{get_inode, now_ts, put_inode};
use crate::schema::*;
use crate::state::FsState;

/// Write data to a file. Returns bytes written.
pub async fn write(state: &mut FsState, ino: u64, offset: i64, data: &[u8]) -> Result<u32> {
    if data.is_empty() {
        return Ok(0);
    }

    ensure_inode_cached(state, ino).await?;

    // Gap detection: if buffer has data and the write is not contiguous, flush first.
    let needs_flush = {
        let is = state.inodes.get(&ino).unwrap();
        if let Some(ref wb) = is.write_buf {
            wb.len > 0 && offset != wb.offset + wb.len as i64
        } else {
            false
        }
    };
    if needs_flush {
        flush_inode(state, ino).await?;
    }

    // Ensure write buffer exists
    {
        let is = state.inodes.get_mut(&ino).unwrap();
        if is.write_buf.is_none() {
            is.write_buf = Some(WriteBuffer::new());
        }
        let wb = is.write_buf.as_mut().unwrap();
        if wb.len == 0 {
            wb.offset = offset;
        }
    }

    let mut written = 0usize;
    let mut remaining = data;

    while !remaining.is_empty() {
        // Copy as much as fits into the buffer.
        let (flush_needed, copied) = {
            let is = state.inodes.get_mut(&ino).unwrap();
            let wb = is.write_buf.as_mut().unwrap();
            let space = WRITE_BUF_CAP - wb.len;
            let to_copy = std::cmp::min(space, remaining.len());
            if wb.buf.len() < wb.len + to_copy {
                wb.buf.resize(wb.len + to_copy, 0);
            }
            wb.buf[wb.len..wb.len + to_copy].copy_from_slice(&remaining[..to_copy]);
            wb.len += to_copy;
            (wb.len >= WRITE_BUF_CAP, to_copy)
        };
        written += copied;
        remaining = &remaining[copied..];

        if flush_needed {
            // Drain the WHOLE buffer (≤ WRITE_BUF_CAP) in one shot —
            // `write_region` splits into `≤ WRITE_BUF_EXTENTS` extents and
            // pipelines the puts via `put_many` at `APPEND_PIPELINE_DEPTH`.
            // Pre-pipelining we drained one MAX_EXTENT at a time → single
            // serial put → `cp` ceiling was `MAX_EXTENT / RPC_RTT`.
            let (flush_offset, flush_data) = {
                let is = state.inodes.get_mut(&ino).unwrap();
                let wb = is.write_buf.as_mut().unwrap();
                let fo = wb.offset;
                let fd: Vec<u8> = wb.buf[..wb.len].to_vec();
                wb.offset = fo + wb.len as i64;
                wb.len = 0;
                (fo, fd)
            };
            let file_size = state.inodes.get(&ino).map(|is| is.meta.size).unwrap_or(0);
            extent::write_region(state, ino, flush_offset as u64, &flush_data, file_size).await?;
        }
    }

    // Update file size and timestamps
    {
        let is = state.inodes.get_mut(&ino).unwrap();
        let new_end = offset as u64 + written as u64;
        if new_end > is.meta.size {
            is.meta.size = new_end;
        }
        let (s, ns) = now_ts();
        is.meta.mtime_secs = s;
        is.meta.mtime_nsecs = ns;
        is.dirty = true;
    }
    state.dirty_inodes.insert(ino);

    Ok(written as u32)
}

/// Flush all buffered writes for an inode.
pub async fn flush_inode(state: &mut FsState, ino: u64) -> Result<()> {
    // Extract buffered chunk data (if any) to drain before persisting meta.
    let (buf_data, buf_offset, buf_len) = {
        let is = match state.inodes.get_mut(&ino) {
            Some(is) => is,
            None => return Ok(()),
        };
        match is.write_buf.as_mut() {
            Some(wb) if wb.len > 0 => {
                let data = wb.buf[..wb.len].to_vec();
                let offset = wb.offset;
                let len = wb.len;
                wb.len = 0;
                (data, offset, len)
            }
            _ => (Vec::new(), 0i64, 0usize),
        }
    };

    // Persist buffered data as variable-length extents (no-op when buf_len == 0).
    // `write_region` splits into MAX_EXTENT-capped, non-overlapping extents.
    if buf_len > 0 {
        let file_size = state.inodes.get(&ino).map(|is| is.meta.size).unwrap_or(0);
        extent::write_region(
            state,
            ino,
            buf_offset as u64,
            &buf_data[..buf_len],
            file_size,
        )
        .await?;
    }

    // Persist the current InodeMeta if dirty — captures size/mtime updates from
    // writes whose chunk data was already flushed incrementally during write().
    let meta = {
        let is = match state.inodes.get_mut(&ino) {
            Some(is) => is,
            None => return Ok(()),
        };
        if !is.dirty {
            return Ok(());
        }
        is.dirty = false;
        is.meta.clone()
    };
    state.dirty_inodes.remove(&ino);
    put_inode(state, ino, &meta).await?;

    Ok(())
}

/// Ensure the inode is loaded in the cache.
async fn ensure_inode_cached(state: &mut FsState, ino: u64) -> Result<()> {
    if state.inodes.contains_key(&ino) {
        return Ok(());
    }
    let meta = get_inode(state, ino).await?;
    state.inodes.insert(
        ino,
        InodeState {
            meta,
            write_buf: None,
            dirty: false,
            open_count: 0,
            extents: None,
            cached_version: 0,
        },
    );
    Ok(())
}

/// Truncate a file to the given size.
pub async fn truncate(state: &mut FsState, ino: u64, new_size: u64) -> Result<()> {
    ensure_inode_cached(state, ino).await?;
    flush_inode(state, ino).await?;

    // Extract old_size and inline info
    let (old_size, has_inline) = {
        let is = state.inodes.get(&ino).unwrap();
        (is.meta.size, is.meta.inline_data.is_some())
    };

    if new_size == old_size {
        return Ok(());
    }

    if new_size < old_size {
        // Shrink: delete extents past the new EOF + truncate the straddling one
        // (F247 — variable-length extents keyed by logical offset).
        extent::truncate_extents(state, ino, new_size, old_size).await?;

        // Handle inline data
        if has_inline {
            let is = state.inodes.get_mut(&ino).unwrap();
            if let Some(ref mut data) = is.meta.inline_data {
                data.truncate(new_size as usize);
                if data.is_empty() {
                    is.meta.inline_data = None;
                }
            }
        }
    }

    // Update metadata
    let meta = {
        let is = state.inodes.get_mut(&ino).unwrap();
        is.meta.size = new_size;
        let (s, ns) = now_ts();
        is.meta.mtime_secs = s;
        is.meta.mtime_nsecs = ns;
        is.meta.ctime_secs = s;
        is.meta.ctime_nsecs = ns;
        is.meta.clone()
    };
    put_inode(state, ino, &meta).await?;

    Ok(())
}
