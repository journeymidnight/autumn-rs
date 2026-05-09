//! RPC dispatch and handler functions for partition operations.
//!
//! F099-D: `handle_put`, `handle_delete`, and `handle_stream_put` are gone —
//! writes decode inline in `merged_partition_loop::handle_incoming_req` and
//! push directly into the SQ/CQ pipeline's pending queue. Only read ops and
//! low-frequency control ops (SPLIT_PART, MAINTENANCE) are handled here.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_common::metrics::ns_to_ms;
use autumn_rpc::manager_rpc;
use autumn_rpc::partition_rpc::{self, *};
use autumn_rpc::{HandlerResult, StatusCode};
use autumn_stream::{ConnPool, StreamClient};
use bytes::Bytes;

use crate::*;

// Per-partition read metrics, tracked in thread-local since partition thread is single-threaded.
thread_local! {
    static READ_METRICS: RefCell<ReadMetrics> = RefCell::new(ReadMetrics::new());
}

struct ReadMetrics {
    started_at: Instant,
    ops: u64,
    lookup_ns: u64,
    encode_ns: u64,
    vp_resolve_ns: u64,
    vp_resolve_count: u64,
    found_in_mem: u64,
    found_in_imm: u64,
    found_in_sst: u64,
    not_found: u64,
}

impl ReadMetrics {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            ops: 0,
            lookup_ns: 0,
            encode_ns: 0,
            vp_resolve_ns: 0,
            vp_resolve_count: 0,
            found_in_mem: 0,
            found_in_imm: 0,
            found_in_sst: 0,
            not_found: 0,
        }
    }
    fn maybe_report(&mut self) {
        if self.started_at.elapsed() >= Duration::from_secs(1) && self.ops > 0 {
            let elapsed = self.started_at.elapsed();
            let ops = self.ops.max(1);
            let vp = self.vp_resolve_count.max(1);
            tracing::info!(
                ops = self.ops,
                ops_per_sec = self.ops as f64 / elapsed.as_secs_f64(),
                avg_lookup_ms = ns_to_ms(self.lookup_ns, ops),
                avg_encode_ms = ns_to_ms(self.encode_ns, ops),
                vp_resolve_count = self.vp_resolve_count,
                avg_vp_resolve_ms = ns_to_ms(self.vp_resolve_ns, vp),
                mem = self.found_in_mem,
                imm = self.found_in_imm,
                sst = self.found_in_sst,
                miss = self.not_found,
                "partition read summary",
            );
            *self = Self::new();
        }
    }
}

/// F099-D: PUT / DELETE / STREAM_PUT are handled by `merged_partition_loop`'s
/// direct `handle_incoming_req` path (no spawn, no inner oneshot). Only
/// reads and low-frequency control ops route through this dispatch function.
/// Receiving a write op here is a bug — we short-circuit with an error.
pub(crate) async fn dispatch_partition_rpc(
    msg_type: u8,
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
    part_sc: &Rc<StreamClient>,
    pool: &Rc<ConnPool>,
    manager_addr: &str,
    owner_key: &str,
    revision: i64,
) -> HandlerResult {
    match msg_type {
        MSG_GET => handle_get(payload, part, part_sc).await,
        MSG_HEAD => handle_head(payload, part).await,
        MSG_RANGE => handle_range(payload, part).await,
        MSG_GET_DISCARDS => handle_get_discards(payload, part, part_sc).await,
        MSG_SPLIT_PART => handle_split_part(payload, part, part_sc, pool, manager_addr, owner_key, revision).await,
        MSG_MAINTENANCE => handle_maintenance(payload, part).await,
        // F129 multipart upload — handled inline (not via merged_partition_loop's
        // batch pipeline). Begin/Chunk/Abort touch only the in-memory session
        // map; Chunk additionally appends a single OP_CHUNK_BLOB record to
        // log_stream; Commit allocates one seq + appends one V1 WAL record
        // (op = OP_VALUE_POINTER_MULTI | 1) + inserts one memtable entry.
        // The per-stream worker serialises log_stream appends so they don't
        // race with concurrent merged_partition_loop batches.
        MSG_PUT_BEGIN => handle_put_begin(payload, part).await,
        MSG_PUT_CHUNK => handle_put_chunk(payload, part, part_sc).await,
        MSG_PUT_COMMIT => handle_put_commit(payload, part, part_sc).await,
        MSG_PUT_ABORT => handle_put_abort(payload, part).await,
        MSG_PUT | MSG_DELETE | MSG_STREAM_PUT => Err((
            StatusCode::Internal,
            format!("write msg_type {msg_type} must be routed via merged_partition_loop"),
        )),
        _ => Err((StatusCode::InvalidArgument, format!("unknown msg_type {msg_type}"))),
    }
}

pub(crate) async fn handle_get(payload: Bytes, part: &Rc<RefCell<PartitionData>>, _part_sc: &Rc<StreamClient>) -> HandlerResult {
    let req: GetReq = partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

    let lookup_t0 = Instant::now();
    let p = part.borrow();
    if !in_range(&p.rg, &req.key) {
        return Err((StatusCode::InvalidArgument, "key is out of range".to_string()));
    }

    // Track where the key was found.
    let mut source = 0u8; // 0=miss, 1=mem, 2=imm, 3=sst
    let found: Option<(u8, Bytes, u64)> = lookup_in_memtable(&p.active, &req.key)
        .map(|r| { source = 1; r })
        .or_else(|| {
            for imm in p.imm.iter().rev() {
                if let Some(r) = lookup_in_memtable(imm, &req.key) { source = 2; return Some(r); }
            }
            None
        })
        .or_else(|| {
            for reader in p.sst_readers.iter().rev() {
                if let Some(r) = lookup_in_sst(reader, &req.key) { source = 3; return Some(r); }
            }
            None
        });
    let lookup_ns = lookup_t0.elapsed().as_nanos() as u64;

    let (op, raw_value, expires_at) = match found {
        Some(v) => v,
        None => {
            READ_METRICS.with(|m| {
                let mut m = m.borrow_mut();
                m.ops += 1; m.lookup_ns += lookup_ns; m.not_found += 1;
                m.maybe_report();
            });
            return Ok(partition_rpc::rkyv_encode(&GetResp { code: CODE_NOT_FOUND, message: "key not found".to_string(), value: vec![] }));
        }
    };
    if op == 2 || (expires_at > 0 && expires_at <= now_secs()) {
        READ_METRICS.with(|m| {
            let mut m = m.borrow_mut();
            m.ops += 1; m.lookup_ns += lookup_ns; m.not_found += 1;
            m.maybe_report();
        });
        return Ok(partition_rpc::rkyv_encode(&GetResp { code: CODE_NOT_FOUND, message: "key not found".to_string(), value: vec![] }));
    }

    let sc = p.stream_client.clone();
    let is_vp = (op & crate::OP_VALUE_POINTER) != 0;

    // F162 (MED-2): acquire a per-extent reader pin BEFORE dropping the
    // partition borrow, so a concurrent run_gc can't decide-and-punch the
    // log_stream extent during our await on read_bytes_from_extent.
    // The pin is taken only when the value is a VP (small inline values
    // never read from log_stream). If the writer (GC) currently holds the
    // pin, treat as not-found rather than racing the deletion.
    let _vp_pin = if is_vp && raw_value.len() >= crate::VALUE_POINTER_SIZE {
        let vp = crate::ValuePointer::decode(&raw_value[..crate::VALUE_POINTER_SIZE]);
        let pin = p.pin_for(vp.extent_id);
        match crate::acquire_reader_pin(pin) {
            Some(g) => Some(g),
            None => {
                // GC has acquired the writer pin on this extent — the bytes
                // are about to be deleted. Surface as NotFound rather than
                // racing the punch_holes RPC.
                READ_METRICS.with(|m| {
                    let mut m = m.borrow_mut();
                    m.ops += 1; m.lookup_ns += lookup_ns; m.not_found += 1;
                    m.maybe_report();
                });
                return Ok(partition_rpc::rkyv_encode(&GetResp {
                    code: CODE_NOT_FOUND,
                    message: "extent reclaimed by GC".to_string(),
                    value: vec![],
                }));
            }
        }
    } else {
        None
    };
    drop(p);

    let vp_t0 = Instant::now();
    let value = resolve_value(op, raw_value, &sc, req.offset, req.length).await.map_err(|e| (StatusCode::Internal, e.to_string()))?;
    let vp_resolve_ns = if is_vp { vp_t0.elapsed().as_nanos() as u64 } else { 0 };
    // _vp_pin guard drops here, releasing the pin.

    let encode_t0 = Instant::now();
    let resp = partition_rpc::rkyv_encode(&GetResp { code: CODE_OK, message: String::new(), value });
    let encode_ns = encode_t0.elapsed().as_nanos() as u64;

    READ_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.ops += 1;
        m.lookup_ns += lookup_ns;
        m.encode_ns += encode_ns;
        if is_vp {
            m.vp_resolve_ns += vp_resolve_ns;
            m.vp_resolve_count += 1;
        }
        match source {
            1 => m.found_in_mem += 1,
            2 => m.found_in_imm += 1,
            3 => m.found_in_sst += 1,
            _ => m.not_found += 1,
        }
        m.maybe_report();
    });

    Ok(resp)
}

pub(crate) async fn handle_head(payload: Bytes, part: &Rc<RefCell<PartitionData>>) -> HandlerResult {
    let req: HeadReq = partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

    let p = part.borrow();
    if !in_range(&p.rg, &req.key) {
        return Err((StatusCode::InvalidArgument, "key is out of range".to_string()));
    }

    let found = lookup_in_memtable(&p.active, &req.key)
        .or_else(|| { for imm in p.imm.iter().rev() { if let Some(r) = lookup_in_memtable(imm, &req.key) { return Some(r); } } None })
        .or_else(|| { for reader in p.sst_readers.iter().rev() { if let Some(r) = lookup_in_sst(reader, &req.key) { return Some(r); } } None });

    let (op, raw_value, expires_at) = match found {
        Some(v) => v,
        None => return Ok(partition_rpc::rkyv_encode(&HeadResp { code: CODE_NOT_FOUND, message: "key not found".to_string(), found: false, value_length: 0 })),
    };
    if op == 2 || (expires_at > 0 && expires_at <= now_secs()) {
        return Ok(partition_rpc::rkyv_encode(&HeadResp { code: CODE_NOT_FOUND, message: "key not found".to_string(), found: false, value_length: 0 }));
    }

    let value_len = if op & crate::OP_VALUE_POINTER_MULTI != 0 {
        // F129: multi-frag VP. raw_value is a MultiFragVp blob; total
        // value size = mfvp.total_len. Decode failures degrade to 0
        // (treat as not-found-shaped rather than panicking).
        crate::MultiFragVp::decode(&raw_value)
            .map(|m| m.total_len)
            .unwrap_or(0)
    } else if op & OP_VALUE_POINTER != 0 && raw_value.len() >= VALUE_POINTER_SIZE {
        ValuePointer::decode(&raw_value[..VALUE_POINTER_SIZE]).len as u64
    } else {
        raw_value.len() as u64
    };

    Ok(partition_rpc::rkyv_encode(&HeadResp { code: CODE_OK, message: String::new(), found: true, value_length: value_len }))
}

pub(crate) async fn handle_range(payload: Bytes, part: &Rc<RefCell<PartitionData>>) -> HandlerResult {
    let req: RangeReq = partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

    let p = part.borrow();
    if req.limit == 0 {
        return Ok(partition_rpc::rkyv_encode(&RangeResp { code: CODE_OK, message: String::new(), entries: vec![], has_more: true }));
    }

    let start_user_key = if req.start.is_empty() { req.prefix.clone() } else { req.start.clone() };
    let seek_key = key_with_ts(&start_user_key, u64::MAX);

    let mem_items = collect_mem_items(&p);
    let mut mem_it = MemtableIterator::new(mem_items);
    mem_it.seek(&seek_key);

    let sst_iters: Vec<TableIterator> = p.sst_readers.iter().rev().map(|r| {
        let mut it = TableIterator::new(r.clone());
        it.seek(&seek_key);
        it
    }).collect();
    let mut merge = MergeIterator::new(sst_iters);

    let now = now_secs();
    let check_overlap = p.has_overlap.get() != 0;
    let part_rg = p.rg.clone();
    drop(p);

    let mut out: Vec<RangeEntry> = Vec::new();
    let mut last_user_key: Option<Vec<u8>> = None;

    loop {
        let mem_key = if mem_it.valid() { mem_it.item().map(|i| i.key.as_slice()) } else { None };
        let sst_key = if merge.valid() { merge.item().map(|i| i.key.as_slice()) } else { None };

        let item = match (mem_key, sst_key) {
            (None, None) => break,
            (Some(_), None) => { let item = mem_it.item().unwrap().clone(); mem_it.next(); item }
            (None, Some(_)) => { let item = merge.item().unwrap().clone(); merge.next(); item }
            (Some(mk), Some(sk)) => {
                if mk <= sk {
                    let item = mem_it.item().unwrap().clone();
                    let uk_owned = parse_key(mk).to_vec();
                    mem_it.next();
                    while merge.valid() {
                        if let Some(si) = merge.item() {
                            if parse_key(&si.key) == uk_owned.as_slice() { merge.next(); } else { break; }
                        } else { break; }
                    }
                    item
                } else {
                    let item = merge.item().unwrap().clone();
                    let uk_owned = parse_key(sk).to_vec();
                    merge.next();
                    while mem_it.valid() {
                        if let Some(mi) = mem_it.item() {
                            if parse_key(&mi.key) == uk_owned.as_slice() { mem_it.next(); } else { break; }
                        } else { break; }
                    }
                    item
                }
            }
        };

        let uk = parse_key(&item.key);
        if check_overlap && !in_range(&part_rg, uk) { continue; }
        if !req.prefix.is_empty() && !uk.starts_with(&req.prefix as &[u8]) { break; }
        if last_user_key.as_deref() == Some(uk) { continue; }
        last_user_key = Some(uk.to_vec());

        if item.op == 2 { continue; }
        if item.expires_at > 0 && item.expires_at <= now { continue; }

        out.push(RangeEntry { key: uk.to_vec(), value: vec![] });
        if out.len() >= req.limit as usize { break; }
    }

    let has_more = out.len() == req.limit as usize;
    Ok(partition_rpc::rkyv_encode(&RangeResp { code: CODE_OK, message: String::new(), entries: out, has_more }))
}

pub(crate) async fn handle_split_part(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
    part_sc: &Rc<StreamClient>,
    pool: &Rc<ConnPool>,
    manager_addr: &str,
    _owner_key: &str,
    _revision: i64,
) -> HandlerResult {
    let req: SplitPartReq = partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

    if part.borrow().has_overlap.get() != 0 {
        return Err((StatusCode::FailedPrecondition, "cannot split: partition has overlapping keys".to_string()));
    }

    // F140: Acquire compact_gate then gc_gate before reading commit_length.
    // compact_gate ensures no RowAppendReq is in-flight on P-bulk (do_compact
    // holds the gate for its full duration and awaits every compact_row_append
    // oneshot before releasing). gc_gate ensures run_gc has no log_stream
    // append in-flight. Both are held through multi_modify_split and released
    // on function exit via RAII.
    let (compact_gate, gc_gate) = {
        let p = part.borrow();
        (p.compact_gate.clone(), p.gc_gate.clone())
    };
    let _compact_permit = compact_gate.acquire().await;
    let _gc_permit = gc_gate.acquire().await;

    // Fetch authoritative range from the manager. PartitionData.rg is set
    // at open_partition and is NOT refreshed by sync_regions_once for an
    // already-open partition, so after a previous split the local rg
    // still spans the old wide range. Picking mid_key against the stale
    // rg can yield a key outside the manager's narrowed range, which
    // multi_modify_split then rejects.
    let auth_rg: Range = {
        let resp_bytes = pool
            .call(manager_addr, manager_rpc::MSG_GET_REGIONS, Bytes::new())
            .await
            .map_err(|e| (StatusCode::Internal, format!("get_regions: {e}")))?;
        let resp: manager_rpc::GetRegionsResp = manager_rpc::rkyv_decode(&resp_bytes)
            .map_err(|e| (StatusCode::Internal, e))?;
        if resp.code != manager_rpc::CODE_OK {
            return Err((StatusCode::Internal, format!("get_regions: {}", resp.message)));
        }
        resp.regions.into_iter()
            .find(|(pid, _)| *pid == req.part_id)
            .and_then(|(_, info)| info.rg)
            .ok_or_else(|| (StatusCode::NotFound, format!("partition {} not in manager regions", req.part_id)))?
    };

    let user_keys = unique_user_keys(&part.borrow())
        .into_iter()
        .filter(|k| in_range(&auth_rg, k))
        .collect::<Vec<_>>();
    if user_keys.len() < 2 {
        return Err((StatusCode::FailedPrecondition,
            format!("part has fewer than 2 in-range keys (have {}; run major compaction first)", user_keys.len())));
    }

    flush_memtable_locked(part).await.map_err(|e| (StatusCode::Internal, e.to_string()))?;

    let mid = user_keys[user_keys.len() / 2].clone();
    let (log_stream_id, row_stream_id, meta_stream_id) = {
        let p = part.borrow();
        (p.log_stream_id, p.row_stream_id, p.meta_stream_id)
    };

    let log_end = part_sc.commit_length(log_stream_id).await.unwrap_or(0).max(1);
    let row_end = part_sc.commit_length(row_stream_id).await.unwrap_or(0).max(1);
    let meta_end = part_sc.commit_length(meta_stream_id).await.unwrap_or(0).max(1);

    // Call multi_modify_split on manager via StreamClient.
    let mut split_ok = false;
    let mut split_err = String::new();
    let mut backoff = Duration::from_millis(100);
    for _ in 0..8 {
        match part_sc
            .multi_modify_split(mid.clone(), req.part_id, [log_end as u64, row_end as u64, meta_end as u64])
            .await
        {
            Ok(()) => {
                split_ok = true;
                break;
            }
            Err(err) => {
                split_err = err.to_string();
                compio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2).min(Duration::from_secs(2));
            }
        }
    }

    if !split_ok {
        return Err((StatusCode::FailedPrecondition, split_err));
    }

    // The manager sealed all 3 stream tails as part of the split. The
    // P-log stream workers still cache the old (now-sealed) tails and
    // would keep appending beyond sealed_length. On recovery,
    // read_last_extent_data only reads up to sealed_length, so any
    // post-split checkpoint/SST appended beyond that point is invisible
    // — causing "invalid meta_len" or missing SSTs.
    part_sc.invalidate_stream(log_stream_id);
    part_sc.invalidate_stream(row_stream_id);
    part_sc.invalidate_stream(meta_stream_id);
    // P-bulk owns a separate StreamClient on another OS thread; signal
    // it to invalidate its row_stream worker on the next FlushReq.
    part.borrow().need_invalidate_row_stream.set(true);

    // Narrow PS-local rg to match the manager's new left range and
    // re-evaluate has_overlap against the SSTables. Without this,
    // sync_regions_once would leave the partition with a stale wide rg
    // and a stale has_overlap=0, perpetuating the bug above.
    {
        let mut p = part.borrow_mut();
        let new_rg = Range { start_key: auth_rg.start_key.clone(), end_key: mid.clone() };
        let mut overlap = false;
        for reader in &p.sst_readers {
            let sk = parse_key(reader.smallest_key());
            let bk = parse_key(reader.biggest_key());
            if !in_range(&new_rg, sk) || !in_range(&new_rg, bk) {
                overlap = true;
                break;
            }
        }
        p.rg = new_rg;
        if overlap {
            p.has_overlap.set(1);
        }
    }

    Ok(partition_rpc::rkyv_encode(&SplitPartResp { code: CODE_OK, message: String::new() }))
}

pub(crate) async fn handle_maintenance(payload: Bytes, part: &Rc<RefCell<PartitionData>>) -> HandlerResult {
    let req: MaintenanceReq = partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
    if req.op == MAINTENANCE_FLUSH {
        // Synchronous flush: rotate active memtable and flush all immutables.
        flush_memtable_locked(part).await.map_err(|e| (StatusCode::Internal, e.to_string()))?;
        return Ok(partition_rpc::rkyv_encode(&MaintenanceResp { code: CODE_OK, message: String::new() }));
    }
    let mut p = part.borrow_mut();
    let result = match req.op {
        MAINTENANCE_COMPACT => p.compact_tx.try_send(true).map_err(|_| "compaction busy"),
        MAINTENANCE_AUTO_GC => p.gc_tx.try_send(GcTask::Auto).map_err(|_| "gc busy"),
        MAINTENANCE_FORCE_GC => p.gc_tx.try_send(GcTask::Force { extent_ids: req.extent_ids }).map_err(|_| "gc busy"),
        _ => Err("unknown op"),
    };
    match result {
        Ok(()) => Ok(partition_rpc::rkyv_encode(&MaintenanceResp { code: CODE_OK, message: String::new() })),
        Err(e) => Ok(partition_rpc::rkyv_encode(&MaintenanceResp { code: CODE_ERROR, message: e.to_string() })),
    }
}

pub(crate) async fn handle_get_discards(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
    part_sc: &Rc<StreamClient>,
) -> HandlerResult {
    let _req: GetDiscardsReq = partition_rpc::rkyv_decode(&payload)
        .map_err(|e| (StatusCode::InvalidArgument, e))?;

    let (log_stream_id, readers) = {
        let p = part.borrow();
        (p.log_stream_id, p.sst_readers.clone())
    };

    let mut discards = crate::background::get_discards(&readers);

    let log_extent_ids = part_sc
        .get_stream_info(log_stream_id)
        .await
        .map(|s| s.extent_ids)
        .unwrap_or_default();
    crate::background::valid_discard(&mut discards, &log_extent_ids);

    Ok(partition_rpc::rkyv_encode(&GetDiscardsResp {
        code: CODE_OK,
        message: String::new(),
        discards: discards.into_iter().collect(),
    }))
}

// ---------------------------------------------------------------------------
// F129 PutStream multipart upload handlers
// ---------------------------------------------------------------------------

/// Generate a 128-bit upload_id from /dev/urandom (falls back to time-
/// based bits on read failure). Caller is responsible for collision
/// avoidance — at 2^128 keyspace this is safe even with millions of
/// concurrent uploads.
fn rand_upload_id() -> u128 {
    let mut buf = [0u8; 16];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        if std::io::Read::read_exact(&mut f, &mut buf).is_ok() {
            return u128::from_le_bytes(buf);
        }
    }
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0xdeadbeef);
    t as u128
}

pub(crate) async fn handle_put_begin(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
) -> HandlerResult {
    let req: PutBeginReq = partition_rpc::rkyv_decode(&payload)
        .map_err(|e| (StatusCode::InvalidArgument, e))?;

    {
        let p = part.borrow();
        if !crate::in_range(&p.rg, &req.key) {
            return Err((StatusCode::InvalidArgument, "key is out of range".into()));
        }
    }

    let now = crate::now_secs();
    let ttl = AUTUMN_PS_UPLOAD_TTL_SECS_DEFAULT;
    let cap = AUTUMN_PS_MAX_UPLOAD_SESSIONS_DEFAULT;

    let upload_id = {
        let mut p = part.borrow_mut();

        // Lazy TTL eviction: drop sessions whose last_seen has aged
        // out. Cheap (single linear scan over a HashMap that's bounded
        // at `cap` entries).
        p.upload_sessions
            .retain(|_, s| s.last_seen_secs.saturating_add(ttl) > now);

        if p.upload_sessions.len() >= cap {
            return Ok(partition_rpc::rkyv_encode(&PutBeginResp {
                code: CODE_ERROR,
                message: format!("too many in-flight uploads (cap = {cap})"),
                upload_id: 0,
            }));
        }

        // Loop until we draw an unused id. Vanishingly unlikely to
        // collide; bounded for safety.
        let mut id = rand_upload_id();
        for _ in 0..16 {
            if !p.upload_sessions.contains_key(&id) {
                break;
            }
            id = rand_upload_id();
        }
        if p.upload_sessions.contains_key(&id) {
            return Err((StatusCode::Internal, "upload_id collision".into()));
        }

        p.upload_sessions.insert(
            id,
            crate::UploadSession {
                user_key: req.key.clone(),
                expires_at: req.expires_at,
                started_at_secs: now,
                last_seen_secs: now,
                bytes_committed: 0,
                next_chunk_index: 0,
                fragments: Vec::new(),
            },
        );
        id
    };

    Ok(partition_rpc::rkyv_encode(&PutBeginResp {
        code: CODE_OK,
        message: String::new(),
        upload_id,
    }))
}

pub(crate) async fn handle_put_chunk(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
    part_sc: &Rc<StreamClient>,
) -> HandlerResult {
    let req: PutChunkReq = partition_rpc::rkyv_decode(&payload)
        .map_err(|e| (StatusCode::InvalidArgument, e))?;

    // Validate session + chunk_index ordering under brief borrow.
    let log_stream_id = {
        let p = part.borrow();
        let s = match p.upload_sessions.get(&req.upload_id) {
            Some(s) => s,
            None => {
                return Ok(partition_rpc::rkyv_encode(&PutChunkResp {
                    code: CODE_UPLOAD_NOT_FOUND,
                    message: "unknown upload_id (timed out, aborted, or never opened)".into(),
                    bytes_committed: 0,
                }));
            }
        };
        if s.next_chunk_index != req.chunk_index {
            return Ok(partition_rpc::rkyv_encode(&PutChunkResp {
                code: CODE_INVALID_ARGUMENT,
                message: format!(
                    "chunk_index {} out of order; expected {}",
                    req.chunk_index, s.next_chunk_index
                ),
                bytes_committed: s.bytes_committed,
            }));
        }
        p.log_stream_id
    };

    // Encode V1 WAL record: op = OP_CHUNK_BLOB, key = empty, value =
    // chunk data. Replay + GC skip records with this op flag.
    let chunk_len = req.data.len() as u32;
    let wal = crate::encode_record(crate::OP_CHUNK_BLOB, &[], &req.data, 0);
    let result = part_sc
        .append(log_stream_id, &wal)
        .await
        .map_err(|e| (StatusCode::Internal, format!("log_stream append: {e}")))?;

    // Within a V1 record the value bytes start at offset
    // `1 (sentinel) + 4 (length) + 17 (V0 inner header) + key.len()`.
    // For chunks key.len() == 0, so the value lives at offset 22
    // within the record. The record itself was appended at
    // `result.offset` in the extent.
    let value_offset_in_record: u32 = 1 + 4 + crate::wal_record::PAYLOAD_HEADER as u32;
    let frag = crate::ValuePointer {
        extent_id: result.extent_id,
        offset: result.offset + value_offset_in_record,
        len: chunk_len,
    };

    // Re-validate session under borrow_mut + commit fragment. The
    // session COULD have been aborted concurrently (TTL eviction or
    // explicit Abort); in that case the chunk bytes are already
    // durable in log_stream and become OP_CHUNK_BLOB garbage,
    // collected by GC when the host extent is punched.
    let bytes = {
        let mut p = part.borrow_mut();
        let s = match p.upload_sessions.get_mut(&req.upload_id) {
            Some(s) => s,
            None => {
                return Ok(partition_rpc::rkyv_encode(&PutChunkResp {
                    code: CODE_UPLOAD_NOT_FOUND,
                    message: "session disappeared mid-append".into(),
                    bytes_committed: 0,
                }));
            }
        };
        if s.next_chunk_index != req.chunk_index {
            // Concurrent retransmit landed first; this chunk is now
            // a duplicate. Don't double-commit fragment.
            return Ok(partition_rpc::rkyv_encode(&PutChunkResp {
                code: CODE_INVALID_ARGUMENT,
                message: "chunk superseded by concurrent retransmit".into(),
                bytes_committed: s.bytes_committed,
            }));
        }
        s.fragments.push(frag);
        s.bytes_committed = s.bytes_committed.saturating_add(chunk_len as u64);
        s.next_chunk_index = s.next_chunk_index.saturating_add(1);
        s.last_seen_secs = crate::now_secs();
        s.bytes_committed
    };

    Ok(partition_rpc::rkyv_encode(&PutChunkResp {
        code: CODE_OK,
        message: String::new(),
        bytes_committed: bytes,
    }))
}

pub(crate) async fn handle_put_commit(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
    part_sc: &Rc<StreamClient>,
) -> HandlerResult {
    let req: PutCommitReq = partition_rpc::rkyv_decode(&payload)
        .map_err(|e| (StatusCode::InvalidArgument, e))?;

    // Take + validate session in a single borrow_mut (so a concurrent
    // TTL evict / Abort can't race us).
    let session = {
        let mut p = part.borrow_mut();
        match p.upload_sessions.remove(&req.upload_id) {
            Some(s) => s,
            None => {
                return Ok(partition_rpc::rkyv_encode(&PutResp {
                    code: CODE_UPLOAD_NOT_FOUND,
                    message: "unknown upload_id".into(),
                    key: vec![],
                }));
            }
        }
    };

    // Bytes-total sanity check. On mismatch, re-insert the session so
    // the client can either re-submit the missing chunks (if any) or
    // call Abort.
    if req.expected_total_bytes != 0
        && req.expected_total_bytes != session.bytes_committed
    {
        let key = session.user_key.clone();
        part.borrow_mut().upload_sessions.insert(req.upload_id, session);
        return Ok(partition_rpc::rkyv_encode(&PutResp {
            code: CODE_INVALID_ARGUMENT,
            message: format!(
                "expected_total_bytes {} != session.bytes_committed",
                req.expected_total_bytes
            ),
            key,
        }));
    }

    // Range check.
    let in_range = {
        let p = part.borrow();
        crate::in_range(&p.rg, &session.user_key)
    };
    if !in_range {
        return Err((StatusCode::InvalidArgument, "key is out of range".into()));
    }

    // Build the multi-frag VP blob. This is what lands in the
    // memtable as the entry's value; resolve_value will decode it
    // and fan out per-fragment reads at Get time (Phase C).
    let mfvp = crate::MultiFragVp {
        total_len: session.bytes_committed,
        frags: session.fragments.clone(),
    };
    let blob = mfvp.encode();
    let key = session.user_key.clone();
    let expires_at = session.expires_at;
    let wal_op: u8 = crate::OP_VALUE_POINTER_MULTI | 1; // 1 = put

    // Allocate seq + build internal_key. Seq is monotonic on the
    // single-threaded P-log runtime; safe to bump from inline
    // dispatch_partition_rpc.
    let (seq, log_stream_id) = {
        let mut p = part.borrow_mut();
        p.seq_number += 1;
        (p.seq_number, p.log_stream_id)
    };
    let internal_key = crate::key_with_ts(&key, seq);
    let wal = crate::encode_record(wal_op, &internal_key, &blob, expires_at);

    // Append the commit WAL record to log_stream. The per-stream
    // worker serialises these against concurrent merged_loop batches
    // so the bytes order well in the extent.
    let result = part_sc
        .append(log_stream_id, &wal)
        .await
        .map_err(|e| (StatusCode::Internal, format!("commit append: {e}")))?;

    // Insert the memtable entry. Value is the multi-frag VP blob
    // (NOT the user value bytes, which live across the chunk
    // fragments in log_stream). vp_extent_id / vp_offset get
    // advanced too so subsequent flush includes this commit's tail.
    {
        let mut p = part.borrow_mut();
        let mem_entry = crate::MemEntry {
            op: wal_op,
            value: blob,
            expires_at,
        };
        let size = (internal_key.len() + mem_entry.value.len() + 32) as u64;
        p.active.insert(internal_key, mem_entry, size);
        p.vp_extent_id = result.extent_id;
        p.vp_offset = result.end;
        crate::maybe_rotate(&mut p);
    }

    Ok(partition_rpc::rkyv_encode(&PutResp {
        code: CODE_OK,
        message: String::new(),
        key,
    }))
}

pub(crate) async fn handle_put_abort(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
) -> HandlerResult {
    let req: PutAbortReq = partition_rpc::rkyv_decode(&payload)
        .map_err(|e| (StatusCode::InvalidArgument, e))?;

    // Idempotent: missing upload_id is treated as success.
    let _ = part.borrow_mut().upload_sessions.remove(&req.upload_id);

    Ok(partition_rpc::rkyv_encode(&PutAbortResp {
        code: CODE_OK,
        message: String::new(),
    }))
}
