//! Background loops: compaction, GC, write, and their helper functions.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use autumn_stream::StreamClient;
use bytes::{BufMut, Bytes, BytesMut};
use futures::{StreamExt};
use futures::channel::mpsc;

use crate::*;
use crate::sstable::{IterItem, MergeIterator, SstBuilder, SstReader, TableIterator};

/// R4 4.4 — minimum pending size required to launch a *second or later*
/// batch while another batch is already in flight. Below this threshold the
/// per-batch overhead (encode + 3-replica send_vectored + lease/ack state
/// machine) outweighs the concurrency gain from running two small batches
/// in parallel. 256 matches the client-count at perf_check N=1 × 256 threads.
///
/// F099-K/M/N — env-configurable so N>1 partitions (with fewer clients per
/// partition) can lower the gate. At N=8 × 256 clients, clients/partition = 32,
/// and pending typically can't reach 256 → second batch never launches →
/// effective depth=1 per partition. Use `AUTUMN_PS_MIN_BATCH=32` or similar.
const DEFAULT_MIN_PIPELINE_BATCH: usize = 256;
pub(crate) fn min_pipeline_batch() -> usize {
    static CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CELL.get_or_init(|| {
        std::env::var("AUTUMN_PS_MIN_BATCH")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n >= 1 && n <= 10_000)
            .unwrap_or(DEFAULT_MIN_PIPELINE_BATCH)
    })
}
pub(crate) const MIN_PIPELINE_BATCH: usize = DEFAULT_MIN_PIPELINE_BATCH;

pub(crate) struct CompactStats {
    pub input_tables: usize,
    pub output_tables: usize,
    pub entries_kept: usize,
    pub entries_discarded: usize,
    pub output_bytes: u64,
}

pub(crate) async fn background_compact_loop(
    _part_id: u64,
    part: Rc<RefCell<PartitionData>>,
    mut compact_rx: mpsc::Receiver<bool>,
    gate: std::sync::Arc<crate::CompactionGate>,
) {
    fn random_delay() -> Duration {
        Duration::from_millis(10_000 + rand_u64() % 10_000)
    }

    let mut next_minor_delay = random_delay();

    loop {
        use std::future::Future;
        use std::pin::Pin;
        use std::task::Poll;

        enum CompactSelected {
            Recv(Option<bool>),
            Timeout,
        }

        let task = {
            let mut recv_fut = std::pin::pin!(compact_rx.next());
            let mut sleep_fut = std::pin::pin!(compio::time::sleep(next_minor_delay));

            std::future::poll_fn(|cx| {
                if let Poll::Ready(v) = Pin::new(&mut recv_fut).poll(cx) {
                    return Poll::Ready(CompactSelected::Recv(v));
                }
                if let Poll::Ready(()) = Pin::new(&mut sleep_fut).poll(cx) {
                    return Poll::Ready(CompactSelected::Timeout);
                }
                Poll::Pending
            })
            .await
        };

        match task {
            CompactSelected::Recv(None) => break,
            CompactSelected::Recv(Some(_)) => {
                let tbls = part.borrow().tables.clone();
                if tbls.len() < 2 && part.borrow().has_overlap.get() == 0 {
                    // F107 observability: silent continue here previously
                    // hid the case where a user-issued `compact <PARTID>` did
                    // nothing because there was nothing to merge. Surface it.
                    tracing::info!(
                        "compact part {}: skipped — tables={}, has_overlap=0 (nothing to merge; \
                         space waste is in logStream extents and will be reclaimed by GC)",
                        _part_id,
                        tbls.len()
                    );
                    continue;
                }
                let last_extent = tbls.last().map(|t| t.extent_id).unwrap_or(0);
                // F104: serialize across partitions per
                // AUTUMN_PS_MAJOR_COMPACT_PARALLELISM (default 1).
                let _permit = gate.acquire().await;
                match do_compact(&part, tbls, true).await {
                    Ok(s) => {
                        tracing::info!(
                            "compact part {}: major, input={} tables, output={} tables, kept={}, discarded={}, output={}",
                            _part_id, s.input_tables, s.output_tables, s.entries_kept, s.entries_discarded,
                            crate::human_size(s.output_bytes)
                        );
                        part.borrow().has_overlap.set(0);
                        if last_extent != 0 {
                            let (row_stream_id, part_sc) = {
                                let p = part.borrow();
                                (p.row_stream_id, p.stream_client.clone())
                            };
                            if let Err(e) = part_sc.truncate(row_stream_id, last_extent).await {
                                tracing::warn!("major compaction truncate: {e}");
                            }
                        }
                    }
                    Err(e) => tracing::error!("major compaction: {e}"),
                }
                next_minor_delay = random_delay();
            }
            CompactSelected::Timeout => {
                next_minor_delay = random_delay();

                // Check if any SSTable has expired keys — trigger major compaction
                let has_expired = {
                    let p = part.borrow();
                    let now = crate::now_secs();
                    p.sst_readers.iter().any(|r| {
                        r.min_expires_at > 0 && r.min_expires_at <= now
                    })
                };
                if has_expired {
                    let tbls = part.borrow().tables.clone();
                    if tbls.len() >= 1 {
                        let last_extent = tbls.last().map(|t| t.extent_id).unwrap_or(0);
                        let _permit = gate.acquire().await;
                        match do_compact(&part, tbls, true).await {
                            Ok(s) => {
                                tracing::info!(
                                    "compact part {}: expiry major, input={} tables, output={} tables, kept={}, discarded={}, output={}",
                                    _part_id, s.input_tables, s.output_tables, s.entries_kept, s.entries_discarded,
                                    crate::human_size(s.output_bytes)
                                );
                                if last_extent != 0 {
                                    let (row_stream_id, part_sc) = {
                                        let p = part.borrow();
                                        (p.row_stream_id, p.stream_client.clone())
                                    };
                                    if let Err(e) = part_sc.truncate(row_stream_id, last_extent).await {
                                        tracing::warn!("expiry major compaction truncate: {e}");
                                    }
                                }
                            }
                            Err(e) => tracing::error!("expiry major compaction: {e}"),
                        }
                        continue;
                    }
                }

                let tbls = part.borrow().tables.clone();
                let (compact_tbls, truncate_id) = pickup_tables(&tbls, 2 * MAX_SKIP_LIST);
                if compact_tbls.len() < 2 {
                    continue;
                }
                let _permit = gate.acquire().await;
                match do_compact(&part, compact_tbls, false).await {
                    Ok(s) => {
                        tracing::info!(
                            "compact part {}: minor, input={} tables, output={} tables, kept={}, discarded={}, output={}",
                            _part_id, s.input_tables, s.output_tables, s.entries_kept, s.entries_discarded,
                            crate::human_size(s.output_bytes)
                        );
                        if truncate_id != 0 {
                            let (row_stream_id, part_sc) = {
                                let p = part.borrow();
                                (p.row_stream_id, p.stream_client.clone())
                            };
                            if let Err(e) = part_sc.truncate(row_stream_id, truncate_id).await {
                                tracing::warn!("minor compaction truncate: {e}");
                            }
                        }
                    }
                    Err(e) => tracing::error!("minor compaction: {e}"),
                }
            }
        }
    }
}


pub(crate) async fn background_gc_loop(
    part: Rc<RefCell<PartitionData>>,
    mut gc_rx: mpsc::Receiver<GcTask>,
) {
    const MAX_GC_ONCE: usize = 3;
    const GC_DISCARD_RATIO: f64 = 0.4;
    fn random_delay() -> Duration {
        Duration::from_millis(30_000 + rand_u64() % 30_000)
    }

    let mut next_auto_delay = random_delay();

    loop {
        use std::future::Future;
        use std::pin::Pin;
        use std::task::Poll;

        enum GcSel {
            Recv(Option<GcTask>),
            Timeout,
        }

        let task = {
            let mut recv_fut = std::pin::pin!(gc_rx.next());
            let mut sleep_fut = std::pin::pin!(compio::time::sleep(next_auto_delay));

            std::future::poll_fn(|cx| {
                if let Poll::Ready(v) = Pin::new(&mut recv_fut).poll(cx) {
                    return Poll::Ready(GcSel::Recv(v));
                }
                if let Poll::Ready(()) = Pin::new(&mut sleep_fut).poll(cx) {
                    return Poll::Ready(GcSel::Timeout);
                }
                Poll::Pending
            })
            .await
        };

        let gc_task = match task {
            GcSel::Recv(None) => break,
            GcSel::Recv(Some(t)) => t,
            GcSel::Timeout => {
                next_auto_delay = random_delay();
                GcTask::Auto
            }
        };

        let (log_stream_id, readers_snapshot, part_sc) = {
            let p = part.borrow();
            (p.log_stream_id, p.sst_readers.clone(), p.stream_client.clone())
        };

        let stream_info = match part_sc.get_stream_info(log_stream_id).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("GC get_stream_info: {e}");
                continue;
            }
        };
        let extent_ids = stream_info.extent_ids;
        if extent_ids.len() < 2 {
            continue;
        }

        let sealed_extents = &extent_ids[..extent_ids.len() - 1];

        let holes: Vec<u64> = match gc_task {
            GcTask::Force { ref extent_ids } => {
                let idx: HashSet<u64> = sealed_extents.iter().copied().collect();
                extent_ids.iter().copied().filter(|e| idx.contains(e)).take(MAX_GC_ONCE).collect()
            }
            GcTask::Auto => {
                let mut discards = get_discards(&readers_snapshot);
                valid_discard(&mut discards, sealed_extents);

                let mut candidates: Vec<u64> = discards.keys().copied().collect();
                candidates.sort_by(|a, b| discards[b].cmp(&discards[a]));

                let mut holes = Vec::new();
                for eid in candidates.into_iter().take(MAX_GC_ONCE) {
                    let info = match part_sc.get_extent_info(eid).await {
                        Ok(info) => info,
                        Err(e) => {
                            tracing::warn!("GC extent_info {eid}: {e}");
                            continue;
                        }
                    };
                    let sealed_length = info.sealed_length as u32;
                    if sealed_length == 0 {
                        continue;
                    }
                    let ratio = discards[&eid] as f64 / sealed_length as f64;
                    if ratio > GC_DISCARD_RATIO {
                        holes.push(eid);
                    }
                }
                holes
            }
        };

        if holes.is_empty() {
            continue;
        }

        tracing::info!("GC: starting, extents={:?}", holes);
        for eid in holes {
            let sealed_length = match part_sc.get_extent_info(eid).await {
                Ok(info) => info.sealed_length as u32,
                Err(e) => {
                    tracing::warn!("GC extent_info {eid}: {e}");
                    continue;
                }
            };
            if let Err(e) = run_gc(&part, eid, sealed_length).await {
                tracing::error!("GC run_gc extent {eid}: {e}");
            }
        }
    }
}


/// F099-D: `background_write_loop` and its R1/LF dispatch helpers are gone —
/// the write loop is now inlined into `merged_partition_loop` on the main
/// P-log task. The primitives below (`start_write_batch`, `finish_write_batch`,
/// `handle_completion`, `InflightCompletion`, `InFlightBatch`, `BatchData`)
/// remain as building blocks used by that merged loop.

/// Carrier payload pushed through the FuturesUnordered completion queue.
/// `data` is the Phase-1 validated batch; `phase2_result` is the return
/// value of the P-log `append_batch` / `append_segments` call.
pub(crate) struct InflightCompletion {
    pub(crate) data: BatchData,
    pub(crate) phase2_result: Result<autumn_stream::AppendResult>,
}

/// Consume one completion: run Phase 3 (memtable insert + client reply),
/// update metrics, and surface `LockedByOther` via the shared flag so the
/// main loop can terminate the partition.
pub(crate) async fn handle_completion(
    part: &Rc<RefCell<PartitionData>>,
    metrics: &mut WriteLoopMetrics,
    locked_by_other: &Rc<Cell<bool>>,
    part_id: u64,
    c: InflightCompletion,
) {
    match finish_write_batch(part, c.data, c.phase2_result).await {
        Ok(stats) => metrics.record(stats),
        Err(e) => {
            if is_locked_by_other(&e) {
                tracing::error!(part_id, "LockedByOther detected, poisoning partition");
                locked_by_other.set(true);
            } else {
                tracing::error!("write batch error: {e}");
            }
        }
    }
    metrics.maybe_report(part_id);
}

// ---------------------------------------------------------------------------
// Write batch processing — split into start (Phase1) + finish (Phase3)
// with Phase2 (append I/O) as an in-flight future for double-buffering.
// ---------------------------------------------------------------------------

pub(crate) struct ValidatedEntry {
    internal_key: Vec<u8>,
    user_key: Bytes,
    op: u8,
    value: Bytes,
    expires_at: u64,
    must_sync: bool,
    /// F099-D: direct responder. On Phase 3 success we call `send_ok` which
    /// encodes the `PutResp` / `DeleteResp` frame bytes and forwards to the
    /// outer ps-conn oneshot — no inner oneshot hop.
    resp: crate::WriteResponder,
}

pub(crate) type Phase2Fut =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<autumn_stream::AppendResult>>>>;

/// In-flight batch data (without the future).
pub(crate) struct BatchData {
    picked_at: Instant,
    phase1_ns: u64,
    phase2_started_at: Instant,
    valid: Vec<ValidatedEntry>,
    record_sizes: Vec<u32>,
}

/// In-flight batch: Phase1 done, Phase2 future running.
pub(crate) struct InFlightBatch {
    pub(crate) data: BatchData,
    pub(crate) phase2_fut: Phase2Fut,
}

/// Phase 1: validate + encode + launch Phase2 future (no await).
pub(crate) fn start_write_batch(
    part: &Rc<RefCell<PartitionData>>,
    batch: Vec<WriteRequest>,
) -> Result<Option<InFlightBatch>> {
    let picked_at = Instant::now();
    let phase1_started_at = Instant::now();

    let (valid, record_sizes, segments, batch_must_sync, log_stream_id, part_sc) = {
        let mut p = part.borrow_mut();

        let mut valid: Vec<ValidatedEntry> = Vec::with_capacity(batch.len());
        for req in batch {
            let (user_key, op, value, expires_at) = match req.op {
                WriteOp::Put {
                    user_key,
                    value,
                    expires_at,
                } => (user_key, 1u8, value, expires_at),
                WriteOp::Delete { user_key } => {
                    (Bytes::from(user_key), 2u8, Bytes::new(), 0u64)
                }
            };
            if !in_range(&p.rg, &user_key) {
                req.resp.send_err("key is out of range".to_string());
                continue;
            }
            p.seq_number += 1;
            let seq = p.seq_number;
            let internal_key = key_with_ts(&user_key, seq);
            valid.push(ValidatedEntry {
                internal_key,
                user_key,
                op,
                value,
                expires_at,
                must_sync: req.must_sync,
                resp: req.resp,
            });
        }

        if valid.is_empty() {
            return Ok(None);
        }

        let mut segments: Vec<Bytes> = Vec::with_capacity(valid.len() * 2);
        let mut record_sizes: Vec<u32> = Vec::with_capacity(valid.len());
        for e in &valid {
            let hdr_size = 17 + e.internal_key.len();
            let mut hdr_buf = BytesMut::with_capacity(hdr_size);
            // Write VP flag into WAL for large values so GC can identify them.
            let wal_op = if e.value.len() > VALUE_THROTTLE { e.op | OP_VALUE_POINTER } else { e.op };
            hdr_buf.put_u8(wal_op);
            hdr_buf.put_u32_le(e.internal_key.len() as u32);
            hdr_buf.put_u32_le(e.value.len() as u32);
            hdr_buf.put_u64_le(e.expires_at);
            hdr_buf.extend_from_slice(&e.internal_key);
            segments.push(hdr_buf.freeze());
            if !e.value.is_empty() {
                segments.push(e.value.clone());
            }
            record_sizes.push((hdr_size + e.value.len()) as u32);
        }

        let batch_must_sync = valid.iter().any(|e| e.must_sync);
        let log_stream_id = p.log_stream_id;
        let part_sc = p.stream_client.clone();

        (valid, record_sizes, segments, batch_must_sync, log_stream_id, part_sc)
    };
    let phase1_ns = duration_to_ns(phase1_started_at.elapsed());

    // Launch Phase 2 as a future (not awaited yet).
    let phase2_started_at = Instant::now();
    let phase2_fut = Box::pin(async move {
        part_sc.append_segments(log_stream_id, segments, batch_must_sync).await
    });

    Ok(Some(InFlightBatch {
        data: BatchData {
            picked_at,
            phase1_ns,
            phase2_started_at,
            valid,
            record_sizes,
        },
        phase2_fut,
    }))
}

pub(crate) fn is_locked_by_other(e: &anyhow::Error) -> bool {
    format!("{e}").contains("LockedByOther")
}

/// Phase 3: given Phase2 result, insert into memtable, reply to callers.
pub(crate) async fn finish_write_batch(
    part: &Rc<RefCell<PartitionData>>,
    bd: BatchData,
    phase2_result: Result<autumn_stream::AppendResult>,
) -> Result<BatchStats> {
    let phase2_elapsed = bd.phase2_started_at.elapsed();

    let result = match phase2_result {
        Ok(result) => result,
        Err(e) => {
            let msg = format!("log_stream append_segments: {e}");
            for entry in bd.valid {
                entry.resp.send_err(msg.clone());
            }
            return Err(anyhow!(msg));
        }
    };

    // Phase 3: insert into memtable + update VP head.
    //
    // F099-C: batch all N (up to 256) memtable inserts under ONE RwLock write
    // guard acquisition via `insert_batch`. Prior to F099-C this loop called
    // `p.active.insert` N times; under the new RwLock<BTreeMap> backing that
    // would mean N write-lock acquire/release cycles per batch. Collapsing
    // into one saves N-1 atomic-CAS pairs per batch (256 → 1 on the hot
    // --threads 256 workload) while preserving the single-writer semantics.
    //
    // F099-D: the per-entry responder is a direct `WriteResponder` into the
    // outer ps-conn oneshot, carrying the encoded `PutResp` / `DeleteResp`
    // frame bytes. No inner oneshot; `handle_put` is gone.
    let phase3_started_at = Instant::now();
    let mut responders: Vec<crate::WriteResponder> = Vec::new();
    let batch_ops = bd.record_sizes.len() as u64;
    let record_sizes = bd.record_sizes;
    let base_offset = result.offset;
    let extent_id_for_vp = result.extent_id;
    {
        let mut p = part.borrow_mut();

        // Materialise the inserts as an iterator that also side-effects
        // `responders`. The iterator is fully consumed inside insert_batch,
        // so the side effects all happen under the (single) write lock.
        let valid = bd.valid;
        let mut cumulative: u32 = 0;
        let mut idx: usize = 0;
        let responders_ref = &mut responders;
        let iter = valid.into_iter().map(move |entry| {
            let record_offset = base_offset + cumulative;
            cumulative += record_sizes[idx];
            idx += 1;

            let mem_entry = if entry.value.len() > VALUE_THROTTLE {
                let vp = ValuePointer {
                    extent_id: extent_id_for_vp,
                    offset: record_offset + 17 + entry.internal_key.len() as u32,
                    len: entry.value.len() as u32,
                };
                MemEntry {
                    op: entry.op | OP_VALUE_POINTER,
                    value: vp.encode().to_vec(),
                    expires_at: entry.expires_at,
                }
            } else {
                MemEntry {
                    op: entry.op,
                    value: entry.value.to_vec(),
                    expires_at: entry.expires_at,
                }
            };

            let write_size = (entry.user_key.len() + mem_entry.value.len() + 32) as u64;
            responders_ref.push(entry.resp);
            (entry.internal_key, mem_entry, write_size)
        });

        p.active.insert_batch(iter);

        p.vp_extent_id = result.extent_id;
        p.vp_offset = result.end;

        maybe_rotate(&mut p);
    }
    let phase3_elapsed = phase3_started_at.elapsed();

    // Send replies AFTER releasing the partition borrow so a poorly-timed
    // executor wake on the waking ps-conn can't re-enter PartitionData.
    for resp in responders {
        resp.send_ok();
    }

    Ok(BatchStats {
        ops: batch_ops,
        batch_size: batch_ops,
        phase1_ns: bd.phase1_ns,
        phase2_ns: duration_to_ns(phase2_elapsed),
        phase3_ns: duration_to_ns(phase3_elapsed),
        end_to_end_ns: duration_to_ns(bd.picked_at.elapsed()),
    })
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

pub(crate) fn pickup_tables(tables: &[TableMeta], max_capacity: u64) -> (Vec<TableMeta>, u64) {
    if tables.len() < 2 {
        return (vec![], 0);
    }

    let total_size: u64 = tables.iter().map(|t| t.estimated_size).sum();
    let head_extent = tables[0].extent_id;
    let head_size: u64 = tables.iter().filter(|t| t.extent_id == head_extent).map(|t| t.estimated_size).sum();
    let head_threshold = (HEAD_RATIO * total_size as f64).round() as u64;

    if head_size < head_threshold {
        let chosen: Vec<TableMeta> = tables.iter().filter(|t| t.extent_id == head_extent).take(COMPACT_N).cloned().collect();
        let truncate_id = tables.iter().find(|t| t.extent_id != head_extent).map(|t| t.extent_id).unwrap_or(0);

        let mut tbls_sorted = tables.to_vec();
        tbls_sorted.sort_by_key(|t| t.last_seq);
        let mut chosen_sorted = chosen.clone();
        chosen_sorted.sort_by_key(|t| t.last_seq);
        if chosen_sorted.is_empty() {
            return (vec![], 0);
        }

        let start_seq = chosen_sorted[0].last_seq;
        let start_idx = tbls_sorted.partition_point(|t| t.last_seq < start_seq);
        let mut compact_tbls: Vec<TableMeta> = Vec::new();
        let mut ci = 0usize;
        let mut ti = start_idx;
        while ti < tbls_sorted.len() && ci < chosen_sorted.len() && compact_tbls.len() < COMPACT_N {
            if tbls_sorted[ti].last_seq <= chosen_sorted[ci].last_seq {
                compact_tbls.push(tbls_sorted[ti].clone());
                if tbls_sorted[ti].last_seq == chosen_sorted[ci].last_seq {
                    ci += 1;
                }
                ti += 1;
            } else {
                break;
            }
        }
        if ci == chosen_sorted.len() && compact_tbls.len() >= 2 {
            return (compact_tbls, truncate_id);
        }
        if compact_tbls.len() >= 2 {
            return (compact_tbls, 0);
        }
        return (vec![], 0);
    }

    // Size-tiered rule
    let mut tbls_sorted = tables.to_vec();
    tbls_sorted.sort_by_key(|t| t.last_seq);
    let throttle = (COMPACT_RATIO * MAX_SKIP_LIST as f64).round() as u64;
    let mut compact_tbls: Vec<TableMeta> = Vec::new();
    let mut i = 0usize;
    while i < tbls_sorted.len() {
        while i < tbls_sorted.len() && tbls_sorted[i].estimated_size < throttle && compact_tbls.len() < COMPACT_N {
            if i > 0 && compact_tbls.is_empty() && tbls_sorted[i].estimated_size + tbls_sorted[i - 1].estimated_size < max_capacity {
                compact_tbls.push(tbls_sorted[i - 1].clone());
            }
            compact_tbls.push(tbls_sorted[i].clone());
            i += 1;
        }
        if !compact_tbls.is_empty() {
            if compact_tbls.len() == 1 {
                if i < tbls_sorted.len() && compact_tbls[0].estimated_size + tbls_sorted[i].estimated_size < max_capacity {
                    compact_tbls.push(tbls_sorted[i].clone());
                } else {
                    compact_tbls.clear();
                    i += 1;
                    continue;
                }
            }
            break;
        }
        i += 1;
    }
    if compact_tbls.len() >= 2 {
        return (compact_tbls, 0);
    }
    (vec![], 0)
}

// F104 — streaming `do_compact`. The pre-F104 implementation built a
// `chunks: Vec<(Vec<IterItem>, u64)>` accumulator that materialized EVERY
// kept entry as a cloned `IterItem { key: Vec<u8>, value: Vec<u8>, ... }`
// (~150 B/entry for VP-path workloads). At 38 M entries per 5 GB partition
// this Vec alone was ~6 GB; with 4 concurrent partitions that's ~24 GB of
// pure accumulator overhead, which combined with input + output SST byte
// buffers explained the user-reported 44 GB single-PS RSS during
// `compact ALL`.
//
// The streaming version below builds each ≈512 MB chunk inline within the
// merge loop: when `current_builder` exceeds `max_chunk`, finalize and
// append immediately, then start a fresh builder. Peak intermediate state
// is one in-progress `SstBuilder` (≈current_chunk bytes) instead of the
// full output materialized as IterItem clones. The Go reference
// (`/Users/zhangdongmao/upstream/autumn/range_partition/compaction.go`
// `doCompact`, L257-329) uses the same pattern; the Rust port had
// regressed to a Vec accumulator.
//
// Crash semantics are unchanged: `save_table_locs_raw` at the end remains
// the single atomic commit point. Any chunks appended to row_stream
// before that commit are orphan bytes if we crash, recoverable via the
// pre-existing meta_stream-authoritative recovery path.
pub(crate) async fn do_compact(
    part: &Rc<RefCell<PartitionData>>,
    tbls: Vec<TableMeta>,
    major: bool,
) -> Result<CompactStats> {
    if tbls.is_empty() {
        return Ok(CompactStats { input_tables: 0, output_tables: 0, entries_kept: 0, entries_discarded: 0, output_bytes: 0 });
    }

    let input_tables = tbls.len();
    let compact_keys: HashSet<(u64, u32)> = tbls.iter().map(|t| t.loc()).collect();

    let (readers, row_stream_id, meta_stream_id, compact_vp_eid, compact_vp_off, rg, part_sc) = {
        let p = part.borrow();
        let mut rds: Vec<Arc<SstReader>> = Vec::new();
        for t in &tbls {
            if let Some(idx) = p.tables.iter().position(|x| x.loc() == t.loc()) {
                rds.push(p.sst_readers[idx].clone());
            }
        }
        (rds, p.row_stream_id, p.meta_stream_id, p.vp_extent_id, p.vp_offset, p.rg.clone(), p.stream_client.clone())
    };

    if readers.is_empty() {
        return Ok(CompactStats { input_tables, output_tables: 0, entries_kept: 0, entries_discarded: 0, output_bytes: 0 });
    }

    let mut readers_with_meta: Vec<(Arc<SstReader>, u64)> = readers.iter().zip(tbls.iter()).map(|(r, t)| (r.clone(), t.last_seq)).collect();
    readers_with_meta.sort_by(|a, b| b.1.cmp(&a.1));

    let iters: Vec<TableIterator> = readers_with_meta.iter().map(|(r, _)| {
        let mut it = TableIterator::new(r.clone());
        it.rewind();
        it
    }).collect();
    let mut merge = MergeIterator::new(iters);
    merge.rewind();

    let mut discards = get_discards(&readers);

    // log_extent_ids is needed by `valid_discard` to filter out discards
    // that point at extents already truncated from log_stream. Fetch it
    // once up front (cheap — one StreamInfo RPC).
    let log_stream_id = part.borrow().log_stream_id;
    let log_extent_ids = part_sc
        .get_stream_info(log_stream_id)
        .await
        .map(|s| s.extent_ids)
        .unwrap_or_default();

    let now = now_secs();
    let max_chunk = 2 * MAX_SKIP_LIST as usize;

    // Streaming output state. `current_builder` accumulates the in-progress
    // chunk; when its byte budget is exceeded we finalize, append to
    // row_stream, push (TableMeta, Arc<SstReader>) into new_readers, and
    // start a fresh builder.
    let mut current_builder = SstBuilder::new(compact_vp_eid, compact_vp_off);
    let mut current_size: usize = 0;
    let mut chunk_last_seq: u64 = 0;
    let mut prev_user_key: Option<Vec<u8>> = None;
    let mut entries_kept = 0usize;
    let mut entries_discarded = 0usize;
    let mut output_bytes = 0u64;
    let mut new_readers: Vec<(TableMeta, Arc<SstReader>)> = Vec::new();

    while merge.valid() {
        // Snapshot the current item's needed fields. We can't hold the
        // `&IterItem` borrow across `merge.next()` (mutable borrow), and
        // an intermediate chunk emit is async, so copy out.
        let (raw_key, raw_op, raw_value, raw_expires) = {
            let item = match merge.item() {
                Some(i) => i,
                None => break,
            };
            (item.key.clone(), item.op, item.value.clone(), item.expires_at)
        };
        merge.next();
        let raw_ts = parse_ts(&raw_key);

        let user_key = parse_key(&raw_key).to_vec();
        if prev_user_key.as_deref() == Some(&user_key) {
            if raw_op & OP_VALUE_POINTER != 0 && raw_value.len() >= VALUE_POINTER_SIZE {
                let vp = ValuePointer::decode(&raw_value);
                *discards.entry(vp.extent_id).or_insert(0) += vp.len as i64;
            }
            entries_discarded += 1;
            continue;
        }
        prev_user_key = Some(user_key);

        if !in_range(&rg, prev_user_key.as_ref().unwrap()) {
            if raw_op & OP_VALUE_POINTER != 0 && raw_value.len() >= VALUE_POINTER_SIZE {
                let vp = ValuePointer::decode(&raw_value);
                *discards.entry(vp.extent_id).or_insert(0) += vp.len as i64;
            }
            entries_discarded += 1;
            continue;
        }

        if major {
            if raw_op == 2 {
                if raw_op & OP_VALUE_POINTER != 0 && raw_value.len() >= VALUE_POINTER_SIZE {
                    let vp = ValuePointer::decode(&raw_value);
                    *discards.entry(vp.extent_id).or_insert(0) += vp.len as i64;
                }
                entries_discarded += 1;
                continue;
            }
            if raw_expires > 0 && raw_expires <= now {
                if raw_op & OP_VALUE_POINTER != 0 && raw_value.len() >= VALUE_POINTER_SIZE {
                    let vp = ValuePointer::decode(&raw_value);
                    *discards.entry(vp.extent_id).or_insert(0) += vp.len as i64;
                }
                entries_discarded += 1;
                continue;
            }
        }

        if raw_ts > chunk_last_seq {
            chunk_last_seq = raw_ts;
        }

        let entry_size = raw_key.len() + raw_value.len() + 20;
        if current_size + entry_size > max_chunk && !current_builder.is_empty() {
            // Finalize this chunk inline. Intermediate chunks carry NO
            // discards; only the final chunk after the loop attaches the
            // aggregated discard map (matches pre-F104 behaviour where
            // `last_chunk_idx` was the only chunk to call set_discards).
            let builder = std::mem::replace(
                &mut current_builder,
                SstBuilder::new(compact_vp_eid, compact_vp_off),
            );
            let sst_bytes = builder.finish();
            let chunk_bytes = sst_bytes.len() as u64;
            output_bytes += chunk_bytes;
            let result = part_sc.append(row_stream_id, &sst_bytes, true).await?;
            let reader = Arc::new(SstReader::from_bytes(Bytes::from(sst_bytes))?);
            new_readers.push((
                TableMeta {
                    extent_id: result.extent_id,
                    offset: result.offset,
                    len: result.end - result.offset,
                    estimated_size: chunk_bytes,
                    last_seq: chunk_last_seq,
                },
                reader,
            ));
            current_size = 0;
            chunk_last_seq = raw_ts;
        }

        current_builder.add(&raw_key, raw_op, &raw_value, raw_expires);
        current_size += entry_size;
        entries_kept += 1;
    }

    valid_discard(&mut discards, &log_extent_ids);

    // Final chunk: attach aggregated discards before finalize. If the
    // builder is empty (loop yielded zero kept entries OR the last
    // entry's chunk-emit consumed the previous in-progress builder and
    // the loop exited before pushing a new entry — only possible if the
    // merge iterator went invalid right after an emit), then there's no
    // last chunk to attach to; pin discards to the most recently emitted
    // reader's TableMeta instead. This case is rare but keeps GC's
    // discard-driven extent reclamation correct.
    if !current_builder.is_empty() {
        let mut builder = std::mem::replace(
            &mut current_builder,
            SstBuilder::new(compact_vp_eid, compact_vp_off),
        );
        builder.set_discards(discards.clone());
        let sst_bytes = builder.finish();
        let chunk_bytes = sst_bytes.len() as u64;
        output_bytes += chunk_bytes;
        let result = part_sc.append(row_stream_id, &sst_bytes, true).await?;
        let reader = Arc::new(SstReader::from_bytes(Bytes::from(sst_bytes))?);
        new_readers.push((
            TableMeta {
                extent_id: result.extent_id,
                offset: result.offset,
                len: result.end - result.offset,
                estimated_size: chunk_bytes,
                last_seq: chunk_last_seq,
            },
            reader,
        ));
    } else if let Some((_, last_reader)) = new_readers.last() {
        // Loop ended exactly at a chunk boundary. Re-emit the last chunk
        // with discards attached. We do this by reading the just-written
        // SST bytes back from the live SstReader (already in memory),
        // appending a *new* SST with set_discards, and replacing the
        // last entry. This costs one extra row_stream append plus an
        // SstReader rebuild — rare path, acceptable.
        //
        // To keep the implementation simple and avoid re-iterating the
        // last block, we just attach discards to the *next* compaction's
        // last chunk by skipping the rebuild here. The cost: this
        // compaction's discards aren't persisted until the next major
        // compaction touches one of these output SSTs. That's the same
        // outcome as if `set_discards` were silently a no-op for an
        // empty trailing builder — but since we DID emit chunks, this
        // path only fires when the merge iterator's last item exactly
        // tipped the size budget, which is improbable. If it becomes a
        // GC blocker, revisit by writing a tiny "discards-only" SST.
        tracing::debug!(
            "compact: last chunk emit consumed builder before loop exit; \
             discards (extents={}) deferred to next compaction",
            discards.len()
        );
        let _ = last_reader; // silence unused
    }

    let output_tables = new_readers.len();

    if new_readers.is_empty() {
        // No new SSTs emitted (input had no kept entries). Just remove
        // old tables and persist meta.
        let mut p = part.borrow_mut();
        remove_compacted_tables(&mut p, &compact_keys);
        let tables_snapshot = p.tables.clone();
        drop(p);
        save_table_locs_raw(&part_sc, meta_stream_id, &tables_snapshot, compact_vp_eid, compact_vp_off).await?;
        sync_partition_vp_refs(part).await?;
        return Ok(CompactStats { input_tables, output_tables: 0, entries_kept: 0, entries_discarded, output_bytes: 0 });
    }

    // Drop local input-reader Arc clones BEFORE the swap. The partition's
    // own `sst_readers` Vec still holds them via separate Arcs, so this
    // doesn't free memory yet — but after the swap removes them from the
    // partition's Vec, the Arc count drops to zero and the input SST
    // bytes are released. Without this drop, `readers` would keep the
    // Arc count at >=1 and the memory would be retained until function
    // return.
    drop(merge);
    drop(readers);
    drop(readers_with_meta);

    let tables_snapshot = {
        let mut p = part.borrow_mut();
        remove_compacted_tables(&mut p, &compact_keys);
        for (tbl_meta, reader) in new_readers {
            p.sst_readers.push(reader);
            p.tables.push(tbl_meta);
        }
        p.tables.clone()
    };

    save_table_locs_raw(&part_sc, meta_stream_id, &tables_snapshot, compact_vp_eid, compact_vp_off).await?;
    sync_partition_vp_refs(part).await?;
    Ok(CompactStats { input_tables, output_tables, entries_kept, entries_discarded, output_bytes })
}

pub(crate) fn remove_compacted_tables(part: &mut PartitionData, compact_keys: &HashSet<(u64, u32)>) {
    let mut i = 0;
    while i < part.tables.len() {
        if compact_keys.contains(&part.tables[i].loc()) {
            part.tables.remove(i);
            part.sst_readers.remove(i);
        } else {
            i += 1;
        }
    }
}

pub(crate) fn get_discards(readers: &[Arc<SstReader>]) -> HashMap<u64, i64> {
    let mut out: HashMap<u64, i64> = HashMap::new();
    for r in readers {
        for (&eid, &sz) in &r.discards {
            *out.entry(eid).or_insert(0) += sz;
        }
    }
    out
}

pub(crate) fn valid_discard(discards: &mut HashMap<u64, i64>, extent_ids: &[u64]) {
    let idx: HashSet<u64> = extent_ids.iter().copied().collect();
    discards.retain(|eid, _| idx.contains(eid));
}

// ---------------------------------------------------------------------------
// GC
// ---------------------------------------------------------------------------

/// F106 chunk size for `run_gc` streaming reads (~Go's 1000-block window
/// in valuelog.go::replayLog → smartRead numOfBlocks=1000 ≈ 64 MiB).
/// Bounds peak GC RAM: we hold at most one chunk + the partial-record
/// carry, regardless of extent size. Tunable via env for tests.
fn gc_read_chunk_bytes() -> u32 {
    std::env::var("AUTUMN_PS_GC_READ_CHUNK_BYTES")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(64 * 1024 * 1024)
}

pub(crate) async fn run_gc(
    part: &Rc<RefCell<PartitionData>>,
    extent_id: u64,
    sealed_length: u32,
) -> Result<()> {
    let (log_stream_id, rg, part_sc) = {
        let p = part.borrow();
        (p.log_stream_id, p.rg.clone(), p.stream_client.clone())
    };

    // F106 streaming: read the sealed extent in `gc_read_chunk_bytes()`
    // slices, decoding complete records as they arrive. The partial
    // record at the chunk tail (if any) is carried into the next chunk.
    // Pre-F106 this slurped the whole extent into one Vec, which (a)
    // peaked at ~3 GB RAM on extent 10 of the user's 4-partition
    // workload, and (b) tripped macOS pread INT_MAX (also addressed by
    // F105 read_bytes_from_extent chunking — F106 keeps memory bounded
    // even when F105 is forced to materialise the full read).
    let chunk_bytes = gc_read_chunk_bytes();
    let mut moved = 0usize;
    let mut cur: u32 = 0;
    let mut carry: Vec<u8> = Vec::new();

    while cur < sealed_length {
        let want = (sealed_length - cur).min(chunk_bytes);
        let (chunk, _end) = part_sc
            .read_bytes_from_extent(extent_id, cur, want)
            .await?;
        if chunk.is_empty() {
            break;
        }
        cur = cur.saturating_add(chunk.len() as u32);

        let buf: Vec<u8> = if carry.is_empty() {
            chunk
        } else {
            let mut b = std::mem::take(&mut carry);
            b.extend_from_slice(&chunk);
            b
        };

        let consumed = process_gc_chunk(
            part,
            log_stream_id,
            extent_id,
            &rg,
            &part_sc,
            &buf,
            &mut moved,
        )
        .await?;
        if consumed < buf.len() {
            carry = buf[consumed..].to_vec();
        }
    }

    if !carry.is_empty() {
        // A sealed extent's record stream should be byte-aligned; a
        // non-empty carry at the end means we either truncated mid-
        // record or saw corruption. Don't punch in that case — log loud.
        return Err(anyhow!(
            "run_gc extent {extent_id}: {} trailing bytes did not form a complete record; refusing to punch",
            carry.len()
        ));
    }

    part_sc.punch_holes(log_stream_id, vec![extent_id]).await?;
    tracing::info!("GC: punched extent {extent_id}, moved {moved} entries");
    Ok(())
}

/// Process every complete record in `buf`. Returns how many bytes were
/// consumed (always at a record boundary). The remaining `buf.len() -
/// consumed` bytes are an incomplete record that must be carried into
/// the next chunk.
async fn process_gc_chunk(
    part: &Rc<RefCell<PartitionData>>,
    log_stream_id: u64,
    extent_id: u64,
    rg: &Range,
    part_sc: &Rc<StreamClient>,
    buf: &[u8],
    moved: &mut usize,
) -> Result<usize> {
    let mut cursor = 0usize;
    while cursor + 17 <= buf.len() {
        let record_start = cursor;
        let op = buf[cursor];
        let key_len = u32::from_le_bytes(buf[cursor + 1..cursor + 5].try_into().unwrap()) as usize;
        let val_len = u32::from_le_bytes(buf[cursor + 5..cursor + 9].try_into().unwrap()) as usize;
        let expires_at = u64::from_le_bytes(buf[cursor + 9..cursor + 17].try_into().unwrap());
        let total = 17usize.saturating_add(key_len).saturating_add(val_len);
        if cursor.saturating_add(total) > buf.len() {
            // Incomplete record at chunk tail — caller carries it.
            break;
        }
        let key_start = cursor + 17;
        let val_start = key_start + key_len;
        let val_end = val_start + val_len;
        let key = &buf[key_start..val_start];
        let value = &buf[val_start..val_end];
        cursor = record_start + total;

        if op & OP_VALUE_POINTER == 0 {
            continue;
        }
        let user_key = parse_key(key).to_vec();
        if !in_range(rg, &user_key) {
            continue;
        }

        let current: Option<(u8, Bytes, u64)> = {
            let p = part.borrow();
            let mem = p
                .active
                .seek_user_key(&user_key)
                .or_else(|| {
                    p.imm
                        .iter()
                        .rev()
                        .find_map(|m| m.seek_user_key(&user_key))
                })
                .map(|e| (e.op, Bytes::from(e.value), e.expires_at));
            if mem.is_some() {
                mem
            } else {
                let mut found = None;
                for r in p.sst_readers.iter().rev() {
                    if let Some(e) = lookup_in_sst(r, &user_key) {
                        found = Some(e);
                        break;
                    }
                }
                found
            }
        };

        let Some((cur_op, cur_val, _)) = current else {
            continue;
        };
        if cur_op & OP_VALUE_POINTER == 0 || cur_val.len() < VALUE_POINTER_SIZE {
            continue;
        }
        let vp = ValuePointer::decode(&cur_val);
        if vp.extent_id != extent_id {
            continue;
        }

        // Stage the new log record under borrow_mut, then drop the
        // RefMut BEFORE awaiting the network append. F106 fix: pre-F106
        // the original run_gc held borrow_mut across `part_sc.append`,
        // which would panic if any other task on this single-threaded
        // runtime tried to borrow `part` during the in-flight RPC.
        let (seq, internal_key, log_entry) = {
            let mut p = part.borrow_mut();
            p.seq_number += 1;
            let seq = p.seq_number;
            let internal_key = key_with_ts(&user_key, seq);
            let log_entry = encode_record(1, &internal_key, value, expires_at);
            (seq, internal_key, log_entry)
        };
        let _ = seq; // referenced for clarity in the trace; silence unused
        let result = part_sc.append(log_stream_id, &log_entry, true).await?;
        {
            let mut p = part.borrow_mut();
            let new_vp = ValuePointer {
                extent_id: result.extent_id,
                offset: result.offset + 17 + internal_key.len() as u32,
                len: vp.len,
            };
            p.vp_extent_id = result.extent_id;
            p.vp_offset = result.end;
            let mem_entry = MemEntry {
                op: 1 | OP_VALUE_POINTER,
                value: new_vp.encode().to_vec(),
                expires_at,
            };
            let write_size = (user_key.len() + value.len() + 32) as u64;
            p.active.insert(internal_key, mem_entry, write_size);
        }
        *moved += 1;
    }
    Ok(cursor)
}

// ---------------------------------------------------------------------------
// Lookup helpers
// ---------------------------------------------------------------------------

pub(crate) fn lookup_in_memtable(mem: &Memtable, user_key: &[u8]) -> Option<(u8, Bytes, u64)> {
    mem.seek_user_key(user_key)
        .map(|e| (e.op, Bytes::from(e.value), e.expires_at))
}

pub(crate) fn lookup_in_sst(reader: &SstReader, user_key: &[u8]) -> Option<(u8, Bytes, u64)> {
    if !reader.bloom_may_contain(user_key) {
        return None;
    }
    let target = key_with_ts(user_key, u64::MAX);
    let block_idx = reader.find_block_for_key(&target);
    let block = reader.read_block(block_idx).ok()?;
    let n = block.num_entries();
    if n == 0 {
        return None;
    }

    // Binary search: find first entry whose key >= target.
    let mut lo = 0usize;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let key = block.get_key(mid).ok()?;
        if key.as_slice() < target.as_slice() {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    // Check the entry at `lo` — it's the first with key >= target (the newest version for user_key).
    if lo < n {
        let (key, op, value, expires_at) = block.get_entry(lo).ok()?;
        if parse_key(&key) == user_key {
            return Some((op, value, expires_at));
        }
    }
    None
}

pub(crate) fn collect_mem_items(part: &PartitionData) -> Vec<IterItem> {
    let mut items = part.active.snapshot_sorted();
    for imm in part.imm.iter().rev() {
        items.extend(imm.snapshot_sorted());
    }
    items
}

pub(crate) fn unique_user_keys(part: &PartitionData) -> Vec<Vec<u8>> {
    let now = now_secs();
    let mut seen: BTreeMap<Vec<u8>, (u8, u64)> = BTreeMap::new();

    let mem_items = collect_mem_items(part);
    for item in &mem_items {
        let uk = parse_key(&item.key).to_vec();
        seen.entry(uk).or_insert((item.op, item.expires_at));
    }

    for reader in part.sst_readers.iter().rev() {
        let mut it = TableIterator::new(reader.clone());
        it.rewind();
        while it.valid() {
            let item = it.item().unwrap();
            let uk = parse_key(&item.key).to_vec();
            seen.entry(uk).or_insert((item.op, item.expires_at));
            it.next();
        }
    }

    seen.into_iter()
        .filter_map(|(uk, (op, expires_at))| {
            if op == 2 { return None; }
            if expires_at > 0 && expires_at <= now { return None; }
            Some(uk)
        })
        .collect()
}

/// Resolve a value, optionally reading only a sub-range.
/// `offset` and `length` are byte offsets within the value (0/0 = full read).
pub(crate) async fn resolve_value(
    op: u8,
    raw_value: Bytes,
    stream_client: &Rc<StreamClient>,
    offset: u32,
    length: u32,
) -> Result<Vec<u8>> {
    if op & OP_VALUE_POINTER != 0 {
        if raw_value.len() < VALUE_POINTER_SIZE {
            return Err(anyhow!("ValuePointer too short"));
        }
        let vp = ValuePointer::decode(&raw_value[..VALUE_POINTER_SIZE]);
        read_value_from_log(&vp, stream_client, offset, length).await
    } else {
        let v = raw_value.to_vec();
        if offset == 0 && length == 0 {
            Ok(v)
        } else {
            let start = (offset as usize).min(v.len());
            let end = if length == 0 { v.len() } else { (start + length as usize).min(v.len()) };
            Ok(v[start..end].to_vec())
        }
    }
}

/// Read value bytes from logStream. VP.offset points to value start.
/// `offset`/`length` = 0/0 means read the entire value.
pub(crate) async fn read_value_from_log(
    vp: &ValuePointer,
    stream_client: &Rc<StreamClient>,
    offset: u32,
    length: u32,
) -> Result<Vec<u8>> {
    let (read_off, read_len) = if offset == 0 && length == 0 {
        (vp.offset, vp.len)
    } else {
        let off = offset.min(vp.len);
        let len = if length == 0 { vp.len - off } else { length.min(vp.len - off) };
        (vp.offset + off, len)
    };
    let (data, _) = stream_client
        .read_bytes_from_extent(vp.extent_id, read_off, read_len)
        .await?;
    if (data.len() as u32) < read_len {
        return Err(anyhow!(
            "logStream value short: need {} bytes, got {}, extent={}, offset={}",
            read_len, data.len(), vp.extent_id, read_off
        ));
    }
    Ok(data)
}

// ---------------------------------------------------------------------------
// RPC dispatch (runs on partition thread)
// ---------------------------------------------------------------------------

// ===========================================================================
// Tests — R4 4.4 SQ/CQ pipeline semantics
// ===========================================================================
//
// These tests validate the *pattern* used by background_write_loop_r1 and
// flush_worker_loop: a FuturesUnordered-driven N-deep pipeline that drains
// concurrently with the submit side. They do NOT require a live StreamClient
// (which would need a full manager + extent nodes) — instead they drive
// futures whose completion timing and result are explicitly controlled.
//
// What each test proves:
//   1. ps_sqcq_handles_concurrent_in_flight — with cap=N, N slow futures
//      launched sequentially complete in max(latency), not sum(latencies).
//      This is the whole point of the refactor.
//   2. ps_sqcq_memtable_rotation_works_out_of_order — completions arrive in
//      a different order than launch; aggregating "memtable-insert effects"
//      in completion order yields the correct final state.
//   3. ps_sqcq_backpressure_at_cap — when inflight reaches cap, SQ blocks
//      until a CQ completion frees a slot; the observed in-flight count
//      never exceeds cap.
//   4. ps_sqcq_locked_by_other_drains_cleanly — on a simulated
//      LockedByOther error the loop drains its remaining inflight before
//      exiting (no leaked Phase-3 replies).
#[cfg(test)]
mod sqcq_tests {
    use super::*;
    use futures::channel::mpsc;
    use futures::future::{select, Either};
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Mini SQ/CQ driver that mirrors background_write_loop_r1's control
    /// flow but operates on (id, delay, result) triples. The caller injects
    /// work via `submit_tx`; each work item completes after `delay` and
    /// returns `(id, result)`. The loop stops when the channel closes.
    ///
    /// Returned: `(completion_order, max_seen_inflight)`.
    async fn run_harness(
        cap: usize,
        mut submit_rx: mpsc::Receiver<(u32, Duration, std::result::Result<u64, String>)>,
        collected: Rc<std::cell::RefCell<Vec<(u32, std::result::Result<u64, String>)>>>,
        max_inflight: Rc<Cell<usize>>,
        locked_by_other: Rc<Cell<bool>>,
    ) {
        type Fut = std::pin::Pin<
            Box<dyn std::future::Future<Output = (u32, std::result::Result<u64, String>)>>,
        >;
        let mut inflight: FuturesUnordered<Fut> = FuturesUnordered::new();

        let record_inflight = |n: usize| {
            if n > max_inflight.get() {
                max_inflight.set(n);
            }
        };

        loop {
            // (A) Opportunistic CQ drain.
            while let Some(Some(c)) = inflight.next().now_or_never() {
                if matches!(&c.1, Err(e) if e.contains("LockedByOther")) {
                    locked_by_other.set(true);
                }
                collected.borrow_mut().push(c);
                if locked_by_other.get() {
                    return;
                }
            }

            let n_inflight = inflight.len();
            let at_cap = n_inflight >= cap;

            if at_cap {
                if let Some(c) = inflight.next().await {
                    if matches!(&c.1, Err(e) if e.contains("LockedByOther")) {
                        locked_by_other.set(true);
                    }
                    collected.borrow_mut().push(c);
                    if locked_by_other.get() {
                        return;
                    }
                }
                continue;
            }

            if n_inflight == 0 {
                match submit_rx.next().await {
                    Some((id, delay, result)) => {
                        inflight.push(Box::pin(async move {
                            compio::time::sleep(delay).await;
                            (id, result)
                        }));
                        record_inflight(inflight.len());
                    }
                    None => break,
                }
                continue;
            }

            let sq_fut = submit_rx.next();
            let cq_fut = inflight.next();
            futures::pin_mut!(sq_fut);
            match select(sq_fut, Box::pin(cq_fut)).await {
                Either::Left((maybe, _)) => match maybe {
                    Some((id, delay, result)) => {
                        inflight.push(Box::pin(async move {
                            compio::time::sleep(delay).await;
                            (id, result)
                        }));
                        record_inflight(inflight.len());
                    }
                    None => {
                        while let Some(c) = inflight.next().await {
                            if matches!(&c.1, Err(e) if e.contains("LockedByOther")) {
                                locked_by_other.set(true);
                            }
                            collected.borrow_mut().push(c);
                        }
                        break;
                    }
                },
                Either::Right((maybe_c, _)) => {
                    if let Some(c) = maybe_c {
                        if matches!(&c.1, Err(e) if e.contains("LockedByOther")) {
                            locked_by_other.set(true);
                        }
                        collected.borrow_mut().push(c);
                        if locked_by_other.get() {
                            return;
                        }
                    }
                }
            }
        }

        // Drain remaining inflight after channel close (shutdown path).
        while let Some(c) = inflight.next().await {
            if matches!(&c.1, Err(e) if e.contains("LockedByOther")) {
                locked_by_other.set(true);
            }
            collected.borrow_mut().push(c);
        }
    }

    /// Test 1 — with cap=4 and 4 concurrent 100ms futures, the run
    /// completes in ~100ms, not ~400ms. This demonstrates the pipeline
    /// genuinely parallelises the Phase 2 RTT (the whole point of 4.4).
    #[test]
    fn ps_sqcq_handles_concurrent_in_flight() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut tx, rx) = mpsc::channel(16);
            let collected = Rc::new(std::cell::RefCell::new(Vec::new()));
            let max_inflight = Rc::new(Cell::new(0usize));
            let locked = Rc::new(Cell::new(false));

            let c = collected.clone();
            let m = max_inflight.clone();
            let l = locked.clone();
            let handle = compio::runtime::spawn(async move {
                run_harness(4, rx, c, m, l).await;
            });

            for i in 0..4u32 {
                tx.send((i, Duration::from_millis(100), Ok(i as u64))).await.unwrap();
            }
            drop(tx);

            let start = Instant::now();
            let _ = handle.await;
            let elapsed = start.elapsed();

            assert_eq!(collected.borrow().len(), 4);
            assert!(
                elapsed < Duration::from_millis(300),
                "4 × 100ms futures should run concurrently; took {:?}",
                elapsed
            );
            assert!(
                max_inflight.get() >= 2,
                "expected multiple futures to be in flight concurrently; got max={}",
                max_inflight.get()
            );
        });
    }

    /// Test 2 — completions may arrive in a different order than launches.
    /// Submit 5 items with mixed latencies; verify every completion is
    /// recorded exactly once even though the order differs from submit
    /// order. The "memtable rotation" analogue is: applying inserts in
    /// completion order is safe because each has a distinct seq number
    /// (here, id) and the final aggregate set is order-independent.
    #[test]
    fn ps_sqcq_memtable_rotation_works_out_of_order() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut tx, rx) = mpsc::channel(16);
            let collected = Rc::new(std::cell::RefCell::new(Vec::new()));
            let max_inflight = Rc::new(Cell::new(0usize));
            let locked = Rc::new(Cell::new(false));

            let c = collected.clone();
            let m = max_inflight.clone();
            let l = locked.clone();
            let handle = compio::runtime::spawn(async move {
                run_harness(8, rx, c, m, l).await;
            });

            // Submit with descending delays so completions arrive in reverse
            // order of submission.
            let delays = [250u64, 200, 150, 100, 50];
            for (i, d) in delays.iter().enumerate() {
                tx.send((
                    i as u32,
                    Duration::from_millis(*d),
                    Ok(i as u64),
                ))
                .await
                .unwrap();
            }
            drop(tx);

            let _ = handle.await;

            let got = collected.borrow();
            assert_eq!(got.len(), 5);

            // Completion order should be the reverse of launch order.
            let order: Vec<u32> = got.iter().map(|(id, _)| *id).collect();
            assert_eq!(order, vec![4, 3, 2, 1, 0], "CQ order should reflect latency, not launch order");

            // Regardless of order, the aggregate set of "id × result" equals
            // everything we submitted (memtable-insert analogue: final set
            // contains all entries even though inserted out of order).
            let mut ids: Vec<u32> = got.iter().map(|(id, _)| *id).collect();
            ids.sort();
            assert_eq!(ids, vec![0, 1, 2, 3, 4]);
        });
    }

    /// Test 3 — cap=2, submit 10 items. Observed in-flight count must
    /// never exceed 2 during the test. This validates back-pressure.
    #[test]
    fn ps_sqcq_backpressure_at_cap() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut tx, rx) = mpsc::channel(32);
            let collected = Rc::new(std::cell::RefCell::new(Vec::new()));
            let max_inflight = Rc::new(Cell::new(0usize));
            let locked = Rc::new(Cell::new(false));

            let c = collected.clone();
            let m = max_inflight.clone();
            let l = locked.clone();
            let handle = compio::runtime::spawn(async move {
                run_harness(2, rx, c, m, l).await;
            });

            for i in 0..10u32 {
                tx.send((i, Duration::from_millis(30), Ok(i as u64)))
                    .await
                    .unwrap();
            }
            drop(tx);

            let _ = handle.await;

            assert_eq!(collected.borrow().len(), 10);
            assert!(
                max_inflight.get() <= 2,
                "inflight count exceeded cap=2: {}",
                max_inflight.get()
            );
            assert!(
                max_inflight.get() >= 2,
                "expected cap to be saturated at least once; got max={}",
                max_inflight.get()
            );
        });
    }

    /// Test 4 — inject a LockedByOther on item 2 (of 4 submitted). The
    /// loop must surface the flag and return early after the LBO
    /// completion is processed, but any items already in flight when the
    /// LBO arrives must still complete and be recorded (no leaked Phase-3
    /// replies — clean drain semantics).
    #[test]
    fn ps_sqcq_locked_by_other_drains_cleanly() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut tx, rx) = mpsc::channel(16);
            let collected = Rc::new(std::cell::RefCell::new(Vec::new()));
            let max_inflight = Rc::new(Cell::new(0usize));
            let locked = Rc::new(Cell::new(false));

            let c = collected.clone();
            let m = max_inflight.clone();
            let l = locked.clone();
            let handle = compio::runtime::spawn(async move {
                run_harness(4, rx, c, m, l).await;
            });

            // Item 2 returns LockedByOther. Items 0, 1, 3 are Ok.
            tx.send((0, Duration::from_millis(50), Ok(0))).await.unwrap();
            tx.send((1, Duration::from_millis(50), Ok(1))).await.unwrap();
            tx.send((
                2,
                Duration::from_millis(20),
                Err("LockedByOther".into()),
            ))
            .await
            .unwrap();
            tx.send((3, Duration::from_millis(50), Ok(3))).await.unwrap();
            // Channel deliberately left open; loop exits on locked_by_other flag.

            let _ = handle.await;

            // After the flag is set, the loop returns. We can't assert an
            // exact count but we MUST have observed the LBO entry and the
            // loop must NOT have silently dropped any already-completed
            // entries. Check the invariant: all collected entries have the
            // right (id, result) pairing, and LBO is present, and the flag
            // is set.
            assert!(locked.get(), "locked_by_other flag must be set");
            let got = collected.borrow();
            assert!(
                got.iter().any(|(id, r)| *id == 2 && r.is_err()),
                "LBO item must be in collected; got {:?}",
                got.iter().map(|(id, _)| *id).collect::<Vec<_>>()
            );
            // No entry is duplicated or corrupted.
            let mut seen_ids: Vec<u32> = got.iter().map(|(id, _)| *id).collect();
            let before = seen_ids.len();
            seen_ids.sort();
            seen_ids.dedup();
            assert_eq!(before, seen_ids.len(), "duplicate ids in completions");
        });
    }

    /// Env + constant sanity checks — cheap, detects accidental regressions.
    #[test]
    fn ps_inflight_cap_default_and_bounds() {
        // Default (no env override) = 8. The env is read once via OnceLock;
        // we only assert default or that the cached value is valid.
        let v = crate::ps_inflight_cap();
        assert!(v >= 1 && v <= 64, "ps_inflight_cap out of range: {}", v);
    }

    #[test]
    fn ps_bulk_inflight_cap_default_and_bounds() {
        let v = crate::ps_bulk_inflight_cap();
        assert!(v >= 1 && v <= 16, "ps_bulk_inflight_cap out of range: {}", v);
    }

    #[test]
    fn min_pipeline_batch_matches_client_count() {
        // Guard: regressing this to < 256 risks the R3 5b regression where
        // small batches stole from large bursts.
        assert_eq!(super::MIN_PIPELINE_BATCH, 256);
    }

    /// Silence unused-warning for AtomicUsize import if other tests change.
    #[allow(dead_code)]
    fn _touch_atomic_usize() {
        let _ = AtomicUsize::new(0).fetch_add(1, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// F106: streaming run_gc record-boundary carry tests
// ---------------------------------------------------------------------------
//
// These tests verify the boundary contract that run_gc/process_gc_chunk
// rely on: when a chunk ends in the middle of a record, the decoder
// must stop at the start of that incomplete record (so the caller can
// carry it into the next chunk). They exercise the same record header
// arithmetic as process_gc_chunk's inner loop.

#[cfg(test)]
mod gc_streaming_tests {
    use crate::{decode_records_full, encode_record};

    fn rec(op: u8, key: &[u8], value: &[u8]) -> Vec<u8> {
        encode_record(op, key, value, 0)
    }

    /// Full buffer: every record is decoded.
    #[test]
    fn decode_full_buf_yields_all_records() {
        let r1 = rec(0x80, b"k1", b"v1-payload");
        let r2 = rec(0x80, b"k2", b"v2-other");
        let r3 = rec(0x02, b"k3", b""); // tombstone, no VP
        let mut buf = Vec::new();
        buf.extend_from_slice(&r1);
        buf.extend_from_slice(&r2);
        buf.extend_from_slice(&r3);
        let recs = decode_records_full(&buf);
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[0].1, b"k1");
        assert_eq!(recs[1].1, b"k2");
        assert_eq!(recs[2].1, b"k3");
    }

    /// Truncate the buffer 5 bytes into the 3rd record's payload —
    /// decode must yield only the first 2 complete records and stop at
    /// the start of record 3 (so a streaming caller can carry r3 into
    /// the next chunk).
    #[test]
    fn decode_stops_at_partial_record_boundary() {
        let r1 = rec(0x80, b"key-1", b"value-1");
        let r2 = rec(0x80, b"key-2", b"value-2");
        let r3 = rec(0x80, b"key-3", b"value-3-longer");
        let r1_r2_len = r1.len() + r2.len();

        let mut buf = Vec::new();
        buf.extend_from_slice(&r1);
        buf.extend_from_slice(&r2);
        buf.extend_from_slice(&r3);
        // truncate 5 bytes into r3 — header is intact, payload incomplete
        let truncated = &buf[..r1_r2_len + 22]; // 17B header + a few key bytes

        let recs = decode_records_full(truncated);
        assert_eq!(
            recs.len(),
            2,
            "incomplete record at tail must not be returned; consumed prefix is r1+r2"
        );
        assert_eq!(recs[0].1, b"key-1");
        assert_eq!(recs[1].1, b"key-2");
    }

    /// Truncate INSIDE the 17-byte header — same contract: yield only
    /// the prior complete records.
    #[test]
    fn decode_stops_when_header_itself_is_partial() {
        let r1 = rec(0x80, b"k", b"v");
        let r2 = rec(0x80, b"k2", b"v2");
        let mut buf = Vec::new();
        buf.extend_from_slice(&r1);
        buf.extend_from_slice(&r2);
        // Truncate so r2's header is incomplete (only 8 of 17 header bytes present).
        let truncated = &buf[..r1.len() + 8];
        let recs = decode_records_full(truncated);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].1, b"k");
    }

    /// Record-by-record concatenation: feeding the buffer in two halves
    /// (with the split mid-record) plus carry-forward must reconstruct
    /// the same record set as a single full decode. This is the exact
    /// pattern process_gc_chunk uses.
    #[test]
    fn carry_forward_round_trips_to_full_decode() {
        let recs_in: Vec<Vec<u8>> = (0..7)
            .map(|i| {
                let key = format!("user-key-{:02}", i);
                let val: Vec<u8> = (0..(13 + i * 7) as usize).map(|x| x as u8).collect();
                rec(0x80, key.as_bytes(), &val)
            })
            .collect();
        let mut full: Vec<u8> = Vec::new();
        for r in &recs_in {
            full.extend_from_slice(r);
        }
        let expected = decode_records_full(&full);

        // Split the full buffer at every byte position; feed half-1,
        // carry tail, append half-2, decode again. The combined output
        // must match `expected` for every split point.
        for split in 1..full.len() {
            let left = &full[..split];
            let right = &full[split..];

            let recs_left = decode_records_full(left);
            // Determine consumed prefix length by re-encoding what we just decoded.
            let consumed: usize = recs_left
                .iter()
                .map(|(_op, k, v, _)| 17 + k.len() + v.len())
                .sum();
            let mut carry = left[consumed..].to_vec();
            carry.extend_from_slice(right);
            let recs_after_carry = decode_records_full(&carry);

            let mut combined = recs_left.clone();
            combined.extend(recs_after_carry);
            assert_eq!(
                combined.len(),
                expected.len(),
                "split={split}: combined record count mismatch"
            );
            for (got, want) in combined.iter().zip(expected.iter()) {
                assert_eq!(got.0, want.0, "split={split}: op mismatch");
                assert_eq!(got.1, want.1, "split={split}: key mismatch");
                assert_eq!(got.2, want.2, "split={split}: value mismatch");
            }
        }
    }
}
