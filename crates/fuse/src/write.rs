//! Write path: write buffering + flush into variable-length extents.
//!
//! Mirrors the 3FS InodeWriteBuf pattern, but the flush unit is a variable-
//! length extent (≤ `MAX_EXTENT` = 8 MiB) keyed by logical offset, not a fixed
//! 256 KiB chunk:
//! - Sequential writes accumulate in the buffer (cap [`WRITE_BUF_CAP`] =
//!   `WRITE_BUF_EXTENTS * MAX_EXTENT` = 64 MiB).
//! - Gap detection: a non-sequential offset flushes the buffer first.
//! - A full buffer flushes the WHOLE buffer at once via `extent::write_region`,
//!   which splits it into `WRITE_BUF_EXTENTS` ≤ `MAX_EXTENT` extents and
//!   dispatches the puts via `put_many` (SDK groups by partition + one
//!   MSG_BATCH_PUT/MSG_PUT_BULK per group). Pre-pipelining the buffer was exactly
//!   `MAX_EXTENT` and the flush was a single serial `put` — the cp ceiling
//!   was `MAX_EXTENT / RPC_RTT` (~270 MB/s). Now `flush ≈ WRITE_BUF_EXTENTS *
//!   MAX_EXTENT / RPC_RTT` until disk + replica fanout saturates.
//! - fsync/close flushes whatever remains (≤ WRITE_BUF_CAP) as the same
//!   pipelined batch (1 to `WRITE_BUF_EXTENTS` extents, last may be shorter).
//!
//! All extent placement / read-modify-write / non-overlap maintenance lives in
//! `crate::extent`; this module only manages the in-memory buffer + inode meta.

use anyhow::Result;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::extent;
use crate::meta::{get_inode, now_ts, put_inode};
use crate::schema::*;
use crate::state::FsState;

// Where the write path's wall clock goes, logged once per FLUSH_LOG_EVERY
// flushes (1 GiB written). Guessing this has been wrong three times in a row —
// a bigger `max_write` and a deeper write buffer both looked obviously right
// and both measured SLOWER — so the split is kept rather than re-derived. On a
// 4 GiB write it reads roughly: fill 0.7 s, write_region 12 s, and ~4 s that is
// in neither, i.e. outside this function entirely (the FUSE bridge, the
// dispatcher, dd's own read of the source).
//
// Reading the line: the counters are PROCESS-GLOBAL — every inode, every mount
// and every PyO3 `Fs` worker in this process sum into them — so it describes a
// single-stream copy well and a mixed workload not at all. `total_in_write_ms`
// is added after each call returns while the sub-timers are added inside it, so
// the parts can exceed the total by up to one in-flight call; and a FAILED
// flush adds to the time but not to `flushes`/`mib`, which skews the ratios
// after an error. Everything is CUMULATIVE for the life of the process and
// never reset, so a second copy through the same daemon reads as double — take
// the numbers from a fresh mount, or subtract.
static FILL_NS: AtomicU64 = AtomicU64::new(0);
static FLUSH_NS: AtomicU64 = AtomicU64::new(0);
/// Time spent MOVING the filled buffer out at flush. Near zero by construction
/// now; kept because it was 12% of a 4 GiB write when it was a copy, and the
/// number is what proves it is gone.
static COPY_NS: AtomicU64 = AtomicU64::new(0);
static FLUSHES: AtomicU64 = AtomicU64::new(0);
static FLUSH_BYTES: AtomicU64 = AtomicU64::new(0);
static TOTAL_NS: AtomicU64 = AtomicU64::new(0);
static WRITE_CALLS: AtomicU64 = AtomicU64::new(0);
const FLUSH_LOG_EVERY: u64 = 16;

/// Wait for this inode's in-flight append flush, if any, and apply its
/// extent-map updates.
///
/// EVERY path that reads the extent map, publishes the inode's size, or writes
/// anywhere but contiguously past the buffer MUST come through here first —
/// while a flush is pending the map is missing its extents and the bytes are
/// not durable. The dispatcher does it for every request that is not a write
/// (`dispatch::handle_request`), so the obligation here is only the write
/// path's own non-contiguous shapes.
///
/// A failed flush is recorded on the inode as well as returned, because the
/// caller here is often the dispatcher's blanket drain, which only logs. The
/// report the application actually sees comes from `flush_inode` (fsync,
/// release, truncate, periodic sync), which consumes that record BEFORE it
/// would persist a size covering the bytes that never landed. Without the
/// record, fsync answered success over a hole.
///
/// It cannot surface at the `write()` that started it — that call returned
/// before the puts ran. That is ordinary writeback; the loser is a caller that
/// ignores both later writes and `fsync`.
pub async fn drain_pending(state: &mut FsState, ino: u64) -> Result<()> {
    let Some(pending) = state
        .inodes
        .get_mut(&ino)
        .and_then(|is| is.pending_flush.take())
    else {
        return Ok(());
    };
    let res = pending
        .task
        .await
        .unwrap_or_else(|e| Err(anyhow::anyhow!("append flush task: {e:?}")));
    match &res {
        // Apply the map updates only on success: a failed put wrote nothing,
        // and recording the extent would make a later read believe in bytes
        // that are not there.
        Ok(()) => {
            if let Some(is) = state.inodes.get_mut(&ino) {
                if let Some(ext) = is.extents.as_mut() {
                    for &(start, len) in &pending.upserts {
                        crate::extent::upsert(ext, start, len);
                    }
                }
            }
        }
        // STICK the failure to the inode as well as returning it. Returning
        // alone is not enough: the dispatcher drains ahead of every non-write
        // handler and only logs, so the fsync that must report this would find
        // nothing pending and acknowledge success over the missing bytes.
        Err(e) => {
            if let Some(is) = state.inodes.get_mut(&ino) {
                if is.flush_error.is_none() {
                    is.flush_error = Some(format!("{e:#}"));
                }
            }
        }
    }
    res
}

/// Take this inode's recorded flush failure, if any. Reporting CONSUMES it —
/// one report per failure, like `errseq_t`: the caller has been told, and a
/// later unrelated fsync should not fail again for it.
fn take_flush_error(state: &mut FsState, ino: u64) -> Option<String> {
    state
        .inodes
        .get_mut(&ino)
        .and_then(|is| is.flush_error.take())
}

/// Drain every inode's pending flush. The dispatcher calls this ahead of any
/// request that is not a write, so no other operation can observe a half-
/// applied extent map — one place instead of an audit of every call site.
pub async fn drain_all_pending(state: &mut FsState) -> Result<()> {
    let inos: Vec<u64> = state
        .inodes
        .iter()
        .filter(|(_, is)| is.pending_flush.is_some())
        .map(|(ino, _)| *ino)
        .collect();
    let mut first_err = None;
    for ino in inos {
        if let Err(e) = drain_pending(state, ino).await {
            tracing::warn!(ino, error = %e, "pending append flush failed");
            first_err.get_or_insert(e);
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Hand a flushed buffer's allocation back to the inode so the next fill reuses
/// it.
///
/// The buffer is installed AS-IS, length included. Clearing it would look
/// tidier and would cost a 64 MiB memset per flush: `mem::take` leaves a len-0
/// `Vec` behind, so a cleared buffer makes the fill loop's
/// `resize(wb.len + to_copy, 0)` run on every iteration, zero-writing exactly
/// the bytes `copy_from_slice` overwrites on the next line. Keeping the length
/// restores the steady state the copying version happened to have. Stale bytes
/// past `wb.len` are never read: `wb.len` is the length of record and all three
/// consumers — the fill, the flush, and `flush_inode` — slice `[..wb.len]`.
///
/// The two early returns and the capacity test remain unreachable: this is
/// called synchronously, before anything is spawned and while `&mut FsState` is
/// still held, so no other operation can drop the inode or grow a new buffer in
/// between. That is a property of the CALL SITE, not of this function — the
/// capacity test looks only at capacity, so against a genuinely concurrent
/// refill it could install over live buffered bytes.
fn reclaim_buffer(state: &mut FsState, ino: u64, buf: Vec<u8>) {
    let Some(is) = state.inodes.get_mut(&ino) else {
        return;
    };
    let Some(wb) = is.write_buf.as_mut() else {
        return;
    };
    if wb.buf.capacity() < buf.capacity() {
        wb.buf = buf;
    }
}

/// Write data to a file. Returns bytes written.
pub async fn write(state: &mut FsState, ino: u64, offset: i64, data: &[u8]) -> Result<u32> {
    let t_total = std::time::Instant::now();
    WRITE_CALLS.fetch_add(1, Ordering::Relaxed);
    let r = write_inner(state, ino, offset, data).await;
    TOTAL_NS.fetch_add(t_total.elapsed().as_nanos() as u64, Ordering::Relaxed);
    r
}

async fn write_inner(state: &mut FsState, ino: u64, offset: i64, data: &[u8]) -> Result<u32> {
    // Reject a negative offset (symmetric with `read::prepare`). The kernel FUSE
    // path never sends one, but a direct core caller (the PyO3 `autumn.Fs`
    // binding, M2) could — and `offset as u64` would wrap to a huge
    // logical offset, poisoning size/extent-key math on flush.
    if offset < 0 {
        return Err(anyhow::anyhow!("negative offset"));
    }
    if data.is_empty() {
        return Ok(0);
    }

    ensure_inode_cached(state, ino).await?;

    // (coco P1): fuse cannot yet MODIFY a striped file — writing here
    // would emit LEGACY-layout `[0x03][ino][off]` extents into an inode whose data
    // lives under `[0x03][lane][ino][off]`, mixing layouts + corrupting the file.
    // fuse READS striped files fine; deferred fuse write-striping (B2) will lift
    // this. Until then, refuse (use autumnfs to (re)write large striped files).
    if state.inodes.get(&ino).and_then(|is| is.meta.stripe.as_ref()).is_some() {
        return Err(anyhow::anyhow!(
            "fuse write to a striped file (ino {ino}) is not supported yet; use autumnfs"
        ));
    }

    // BUG-LEASE-8 (coco P0): a write that GROWS the file past the current
    // EOF must reap crashed-shrink leftovers BEFORE the in-memory size
    // bumps below — the bump erases the pre-grow EOF, so by flush time
    // `write_region` sees the grown size and a stale straddler tail looks
    // like legitimate in-file data (its RMW would merge the write into
    // the stale value and expose pre-shrink bytes in the sparse hole).
    // Cold path: only fires on a beyond-EOF write (offset > size).
    {
        let cur_size = state.inodes.get(&ino).map(|is| is.meta.size).unwrap_or(0);
        if offset as u64 > cur_size {
            // Reads and rewrites the extent map, so nothing may be in flight.
            drain_pending(state, ino).await?;
            extent::clean_beyond_eof(state, ino, cur_size).await?;
        }
    }

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
        // `flush_inode` drains for us. Do NOT drain unconditionally here: the
        // contiguous case only copies into the buffer and touches no extent
        // map, and waiting on the in-flight flush at every 1 MiB write is
        // exactly the serialization the pipelining exists to remove (measured:
        // it put the whole gain back, 242 vs the 244 it started from).
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
        let t_fill = std::time::Instant::now();
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
        FILL_NS.fetch_add(t_fill.elapsed().as_nanos() as u64, Ordering::Relaxed);
        written += copied;
        remaining = &remaining[copied..];

        if flush_needed {
            // Drain the WHOLE buffer (≤ WRITE_BUF_CAP) in one shot —
            // `write_region` splits into `≤ WRITE_BUF_EXTENTS` extents and
            // pipelines the puts via `put_many` (server-batched per partition).
            // Pre-pipelining we drained one MAX_EXTENT at a time → single
            // serial put → `cp` ceiling was `MAX_EXTENT / RPC_RTT`.
            // MOVE the buffer out rather than copying it. `write_region` needs
            // `&mut FsState`, which owns the buffer, so the data cannot simply
            // be borrowed across the call — the previous answer was
            // `wb.buf[..wb.len].to_vec()`, a fresh 64 MiB allocation and copy
            // per flush that measured 2.5 s of a 20 s 4 GiB write (12%), on top
            // of the copy the kernel's bytes already paid getting INTO the
            // buffer. `mem::take` is O(1); the `Vec` is handed back below so
            // the next fill reuses the allocation instead of growing a new
            // 64 MiB one.
            let t_copy = std::time::Instant::now();
            let (flush_offset, flush_len, flush_buf) = {
                let is = state.inodes.get_mut(&ino).unwrap();
                let wb = is.write_buf.as_mut().unwrap();
                let fo = wb.offset;
                let n = wb.len;
                let buf = std::mem::take(&mut wb.buf);
                wb.offset = fo + n as i64;
                wb.len = 0;
                (fo, n, buf)
            };
            COPY_NS.fetch_add(t_copy.elapsed().as_nanos() as u64, Ordering::Relaxed);
            let file_size = state.inodes.get(&ino).map(|is| is.meta.size).unwrap_or(0);
            let t_flush = std::time::Instant::now();
            let n = flush_len as u64;
            // The previous flush must land before this one is planned: its
            // extents are not in the map yet, and two in flight would make the
            // failure of either impossible to attribute.
            drain_pending(state, ino).await?;
            let plan = extent::plan_append_only(
                state,
                ino,
                flush_offset as u64,
                &flush_buf[..flush_len],
                file_size,
            )
            .await?;
            let plan = if state.pipelined_writes { plan } else { None };
            let res = match plan {
                // Pure append: hand the puts to a spawned task and go back to
                // the kernel. The plan owns copies of the bytes, so the buffer
                // is free the moment it exists — no second buffer, and the fill
                // of the NEXT 64 MiB overlaps these puts instead of waiting.
                Some(plan) => {
                    reclaim_buffer(state, ino, flush_buf);
                    let client = state.client.clone();
                    let upserts = plan.upserts().to_vec();
                    let task = compio::runtime::spawn(async move {
                        extent::execute_append(client, &plan).await
                    });
                    if let Some(is) = state.inodes.get_mut(&ino) {
                        is.pending_flush = Some(crate::schema::PendingFlush { task, upserts });
                    }
                    Ok(())
                }
                // Anything else — read-modify-write, a hole, beyond-EOF residue
                // — stays inline, because those read the extent map and it is
                // only complete while nothing is in flight.
                None => {
                    let r = extent::write_region(
                        state,
                        ino,
                        flush_offset as u64,
                        &flush_buf[..flush_len],
                        file_size,
                    )
                    .await;
                    reclaim_buffer(state, ino, flush_buf);
                    r
                }
            };
            FLUSH_NS.fetch_add(t_flush.elapsed().as_nanos() as u64, Ordering::Relaxed);
            res?;
            FLUSH_BYTES.fetch_add(n, Ordering::Relaxed);
            let f = FLUSHES.fetch_add(1, Ordering::Relaxed) + 1;
            if f % FLUSH_LOG_EVERY == 0 {
                let fill = FILL_NS.load(Ordering::Relaxed) / 1_000_000;
                let cp = COPY_NS.load(Ordering::Relaxed) / 1_000_000;
                let fl = FLUSH_NS.load(Ordering::Relaxed) / 1_000_000;
                let mb = FLUSH_BYTES.load(Ordering::Relaxed) / 1_048_576;
                tracing::info!(
                    flushes = f,
                    mib = mb,
                    fill_ms = fill,
                    buf_handoff_ms = cp,
                    write_region_ms = fl,
                    total_in_write_ms = TOTAL_NS.load(Ordering::Relaxed) / 1_000_000,
                    write_calls = WRITE_CALLS.load(Ordering::Relaxed),
                    "fuse write breakdown"
                );
            }
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
    // Drain first: this publishes the inode's size and reads the extent map,
    // and the crash-consistency rule is that extent puts ACK before a size
    // that covers them is persisted.
    let drained = drain_pending(state, ino).await;
    // Report a failure from ANY earlier flush of this inode, including one the
    // dispatcher's drain already consumed, and do it BEFORE persisting a size
    // that would cover the bytes that never landed. This is the whole point of
    // `flush_error`: fsync must not answer success over a hole.
    if let Some(msg) = take_flush_error(state, ino) {
        return Err(anyhow::anyhow!("earlier write flush failed: {msg}"));
    }
    drained?;

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
            pending_flush: None,
            flush_error: None,
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
    // (coco P1): fuse truncate of a striped file would run the legacy
    // range-scan extent cleanup, which finds no `[0x03][lane]…` keys → leaks the
    // striped extents (and a grow would emit legacy-layout ones). Refuse until
    // fuse write-striping (B2); reads work. Use autumnfs to manage striped files.
    if state.inodes.get(&ino).and_then(|is| is.meta.stripe.as_ref()).is_some() {
        return Err(anyhow::anyhow!(
            "fuse truncate of a striped file (ino {ino}) is not supported yet; use autumnfs"
        ));
    }
    flush_inode(state, ino).await?;

    // Extract old_size and inline info
    let (old_size, has_inline) = {
        let is = state.inodes.get(&ino).unwrap();
        (is.meta.size, is.meta.inline_data.is_some())
    };

    if new_size == old_size {
        return Ok(());
    }

    // BUG-LEASE-8 (shrink ordering): the inode-meta put is the COMMIT
    // POINT and must land BEFORE any extent destruction. Pre-fix the
    // shrink deleted/shortened extent KVs first and persisted the new
    // size after — a crash in between left durable size = old_size with
    // the data already destroyed, so reads in [new_size, old_size)
    // returned zeros INSIDE the file (silent corruption; the one
    // crash-window in this layer that fabricated data rather than just
    // losing a recent un-fsynced write). Meta-first inverts the leftover:
    // a crash after the put leaves extents beyond the new size — they are
    // invisible to reads (bounded by size) and self-heal via the
    // prefix-scan deletes (unlink / later truncate) or same-key rewrites
    // (a regrowing append re-derives the same [ino][off] keys).
    if new_size < old_size && has_inline {
        let is = state.inodes.get_mut(&ino).unwrap();
        if let Some(ref mut data) = is.meta.inline_data {
            data.truncate(new_size as usize);
            if data.is_empty() {
                is.meta.inline_data = None;
            }
        }
    }

    if new_size > old_size {
        // GROW (coco P1): reap any leftover extents beyond the old EOF
        // BEFORE the size expands over them — the residue of a crashed
        // shrink's cleanup window would otherwise re-enter the readable
        // range as resurrected old data where POSIX requires zeros.
        // Runs while the leftovers are still invisible (size = old), so
        // a crash mid-sweep is safe and the next grow retries.
        extent::clean_beyond_eof(state, ino, old_size).await?;
    }

    // Update + persist metadata (the commit point).
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

    if new_size < old_size {
        // Cleanup AFTER the commit: delete extents past the new EOF +
        // shorten the straddling one (variable-length extents
        // keyed by logical offset). The truncate is already COMMITTED
        // (meta landed), so a cleanup error must NOT surface as failure
        // (coco P1: the caller would retry, hit the `new_size ==
        // old_size` early-return no-op, and never re-clean) — leftovers
        // are benign while size stays put and every grow path sweeps
        // them via `clean_beyond_eof` before exposure.
        if let Err(e) = extent::truncate_extents(state, ino, new_size, old_size).await {
            tracing::warn!(ino, new_size, old_size, "post-commit truncate cleanup failed (leftovers reaped on next grow/unlink): {e}");
            extent::invalidate(state, ino);
        }
    }

    Ok(())
}
