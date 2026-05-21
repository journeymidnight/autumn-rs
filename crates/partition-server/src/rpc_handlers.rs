//! RPC dispatch and handler functions for partition operations.
//!
//! F099-D: `handle_put`, `handle_delete`, and `handle_stream_put` are gone —
//! writes decode inline in `partition_loop::handle_incoming_req` and
//! push directly into the SQ/CQ pipeline's pending queue. Only read ops and
//! low-frequency control ops (SPLIT_PART, MAINTENANCE) are handled here.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_common::metrics::ns_to_ms;
use autumn_rpc::manager_rpc;
use autumn_rpc::partition_rpc::{self, *};
use autumn_rpc::{HandlerResult, StatusCode};
use autumn_stream::{ConnPool, StaleVpOffset, StreamClient};
use bytes::Bytes;

use crate::*;

/// F204: translate VP-resolve errors into wire status codes that
/// distinguish "data permanently lost; clean up the key" from
/// "server bug; investigate".
///
/// Most read-side errors today bubble up as `anyhow::Error` and get
/// uniformly mapped to `StatusCode::Internal`. That collapses two
/// very different classes — transient/buggy server failures vs known
/// historical data corruption (`StaleVpOffset` sentinel from
/// `autumn-stream`). Operational tooling (per
/// `feedback_ops_tools_in_python` memory: future Python scripts that
/// consume `autumn-client` output) needs to distinguish them so it
/// can decide between "retry" and "delete the key + major compact".
///
/// The sentinel's Display string is a stable wire contract — see the
/// `StaleVpOffset` doc comment in `crates/stream/src/client.rs` for
/// the prefix + field-order guarantees.
fn map_storage_error(e: &anyhow::Error) -> (StatusCode, String) {
    if let Some(stale) = e.chain().find_map(|c| c.downcast_ref::<StaleVpOffset>()) {
        return (StatusCode::FailedPrecondition, stale.to_string());
    }
    (StatusCode::Internal, e.to_string())
}

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

/// F099-D: PUT / DELETE / STREAM_PUT are handled by `partition_loop`'s
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
        MSG_GET => handle_get(payload, part).await,
        MSG_HEAD => handle_head(payload, part).await,
        MSG_RANGE => handle_range(payload, part).await,
        MSG_GET_DISCARDS => handle_get_discards(payload, part, part_sc).await,
        // F210-C2: SPLIT_PART must NOT be invoked inline through
        // dispatch_partition_rpc — handle_split_part awaits an internal
        // drain signal that requires partition_loop to run, and
        // an inline call would self-deadlock (the loop's stack is parked
        // here). MSG_SPLIT_PART is now intercepted in
        // `handle_incoming_req` and dispatched via `compio::runtime::spawn`.
        MSG_SPLIT_PART => Err((
            StatusCode::Internal,
            "MSG_SPLIT_PART must not be dispatched inline — F210-C2 requires spawned task; \
             routed via handle_incoming_req's MSG_SPLIT_PART arm".to_string(),
        )),
        MSG_MAINTENANCE => handle_maintenance(payload, part).await,
        // F210-C4: manager pull of current vp_refs snapshot.
        MSG_PULL_VP_REFS => handle_pull_vp_refs(payload, part).await,
        // F129 server-side multipart (MSG_PUT_BEGIN/CHUNK/COMMIT/ABORT)
        // removed in F186. Stripe-write is now pure client-side via
        // ClusterClient::put_stream_begin (Ceph striperados pattern).
        MSG_PUT | MSG_DELETE | MSG_STREAM_PUT => Err((
            StatusCode::Internal,
            format!("write msg_type {msg_type} must be routed via partition_loop"),
        )),
        _ => Err((StatusCode::InvalidArgument, format!("unknown msg_type {msg_type}"))),
    }
}

/// Outcome of the shared GET resolve core: the value bytes, or a not-found.
/// `Value` is a `Bytes` that, for a VP read over UCX, ALIASES the registered
/// RegPool buffer (R4) — `handle_get_zc` sends it as its own iovec (no copy);
/// `handle_get` copies it into the rkyv `GetResp` (which copies regardless).
pub(crate) enum GetOutcome {
    NotFound,
    Value(Bytes),
}

/// rkyv-framed GET (generic SDK path).
pub(crate) async fn handle_get(payload: Bytes, part: &Rc<RefCell<PartitionData>>) -> HandlerResult {
    match get_value(payload, part).await? {
        GetOutcome::NotFound => Ok(partition_rpc::rkyv_encode(&GetResp {
            code: CODE_NOT_FOUND,
            message: "key not found".to_string(),
            value: vec![],
        })),
        // `value.into()` (NOT `to_vec()`): bytes' `From<Bytes> for Vec<u8>`
        // RECLAIMS the underlying Vec with no copy when this `Bytes` uniquely
        // owns a Vec-backed buffer — which is the copy-path case
        // (`read_value_from_log` → `Bytes::from(data)` on TCP / non-pooled VP
        // reads). `to_vec()` always copied, which regressed the generic large
        // read by one full value memcpy after R4 made `resolve_value` return
        // `Bytes` (caught by the perf baseline: TCP 8M read −25%). The rkyv
        // encode below still copies once (unavoidable for the wire archive).
        GetOutcome::Value(value) => Ok(partition_rpc::rkyv_encode(&GetResp {
            code: CODE_OK,
            message: String::new(),
            value: value.into(),
        })),
    }
}

/// F216 zero-copy GET (MSG_GET_ZC): returns the response as TWO segments —
/// `(head, value)` where `head = [V0 frame header][ZC meta: code + value_len +
/// value_crc32c]` and `value` ALIASES the RegPool buffer (R4: `Bytes::from_owner`
/// from `resolve_value`, no copy). The ps-conn pushes `head` then `value` into
/// `tx_bufs` so the single `write_vectored_all` emits them as one wire frame with
/// NO concat copy — fully zero-copy EN->PS->client. (Pre-R4 this concatenated
/// `[meta][value]` into a Vec, copied again by `encode_v0`.)
///
/// ALL outcomes (incl errors) map to a V0 ZC response — a V1 error frame would
/// corrupt the client's recv-into-dest parsing. The status rides in the meta
/// `code`; the SDK's get_into maps non-OK codes to refresh/retry. StatusCode
/// discriminants align with the partition CODE_* for the GET-relevant cases
/// (InvalidArgument=2, FailedPrecondition=3, Internal=4). So this never errors.
pub(crate) async fn handle_get_zc(
    req_id: u32,
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
) -> (Bytes, Bytes) {
    let (code, value): (u8, Bytes) = match get_value(payload, part).await {
        Ok(GetOutcome::Value(v)) => (CODE_OK, v),
        Ok(GetOutcome::NotFound) => (CODE_NOT_FOUND, Bytes::new()),
        Err((status, _msg)) => (status as u8, Bytes::new()),
    };
    (ps_zc_head(req_id, code, &value), value)
}

/// Build the MSG_GET_ZC response head = `[V0 frame header][zc_meta]`. The value
/// is sent as a SEPARATE `Bytes` right after (aliasing the RegPool buffer) so it
/// is never copied. Mirrors `extent_node::zc_read_head`. The header's
/// `payload_len` covers meta + value, so the client recvs the whole payload.
pub(crate) fn ps_zc_head(req_id: u32, code: u8, value: &[u8]) -> Bytes {
    use bytes::BufMut;
    let meta = autumn_rpc::client::encode_zc_meta(code, value);
    let payload_len = meta.len() + value.len();
    let mut head = bytes::BytesMut::with_capacity(autumn_rpc::HEADER_LEN + meta.len());
    head.put_u32_le(req_id);
    head.put_u8(MSG_GET_ZC);
    head.put_u8(autumn_rpc::frame::FLAG_RESPONSE); // V0; value crc rides in the meta
    head.put_u32_le(payload_len as u32);
    head.put_slice(&meta);
    head.freeze()
}

/// Shared GET resolve core: epoch/range check → memtable/imm/SST lookup →
/// VP resolve (read_value_from_log). Used by both `handle_get` (rkyv) and
/// `handle_get_zc` (value-separable). Carries the read metrics.
async fn get_value(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
) -> Result<GetOutcome, (StatusCode, String)> {
    let req: GetReq = partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

    let lookup_t0 = Instant::now();
    let p = part.borrow();
    // TiKV-style region epoch check. `0` from the client = "skip check"
    // (bootstrap / tests / legacy callers). On mismatch surface a
    // FailedPrecondition frame error so the SDK's existing `Err`-arm
    // refresh+retry path in `call_ps_for_key` engages.
    if req.region_epoch != 0 && req.region_epoch != p.region_epoch {
        return Err((StatusCode::FailedPrecondition, format!(
            "region epoch stale: part_id={} have={} got={}",
            p.part_id, p.region_epoch, req.region_epoch
        )));
    }
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
            return Ok(GetOutcome::NotFound);
        }
    };
    if op == 2 || (expires_at > 0 && expires_at <= now_secs()) {
        READ_METRICS.with(|m| {
            let mut m = m.borrow_mut();
            m.ops += 1; m.lookup_ns += lookup_ns; m.not_found += 1;
            m.maybe_report();
        });
        return Ok(GetOutcome::NotFound);
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
                return Ok(GetOutcome::NotFound);
            }
        }
    } else {
        None
    };
    drop(p);

    let vp_t0 = Instant::now();
    let value = resolve_value(op, raw_value, &sc, req.offset, req.length)
        .await
        .map_err(|e| map_storage_error(&e))?;
    let vp_resolve_ns = if is_vp { vp_t0.elapsed().as_nanos() as u64 } else { 0 };
    // _vp_pin guard drops here, releasing the pin.

    READ_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.ops += 1;
        m.lookup_ns += lookup_ns;
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

    Ok(GetOutcome::Value(value))
}

pub(crate) async fn handle_head(payload: Bytes, part: &Rc<RefCell<PartitionData>>) -> HandlerResult {
    let req: HeadReq = partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

    let p = part.borrow();
    if req.region_epoch != 0 && req.region_epoch != p.region_epoch {
        return Err((StatusCode::FailedPrecondition, format!(
            "region epoch stale: part_id={} have={} got={}",
            p.part_id, p.region_epoch, req.region_epoch
        )));
    }
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

    let value_len = if op & OP_VALUE_POINTER != 0 && raw_value.len() >= VALUE_POINTER_SIZE {
        ValuePointer::decode(&raw_value[..VALUE_POINTER_SIZE]).len as u64
    } else {
        raw_value.len() as u64
    };

    Ok(partition_rpc::rkyv_encode(&HeadResp { code: CODE_OK, message: String::new(), found: true, value_length: value_len }))
}

pub(crate) async fn handle_range(payload: Bytes, part: &Rc<RefCell<PartitionData>>) -> HandlerResult {
    let req: RangeReq = partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

    let p = part.borrow();
    // F-this: this is the load-bearing check for `range()` correctness
    // after a split. Pre-this, mismatched-epoch range requests were
    // silently filtered per-key (`continue` at line 351 below) and
    // returned a valid-but-partial result with `code:OK` — the gallery
    // bug. Now any range with a stale snapshot's epoch is rejected
    // up-front; SDK refreshes + re-runs.
    if req.region_epoch != 0 && req.region_epoch != p.region_epoch {
        return Err((StatusCode::FailedPrecondition, format!(
            "region epoch stale: part_id={} have={} got={}",
            p.part_id, p.region_epoch, req.region_epoch
        )));
    }
    // F-this Phase 4: snapshot the PS's authoritative end_key so the
    // response can carry it as a resume cursor for the SDK. Empty =
    // unbounded right side (last partition in the keyspace).
    let cur_end_key = p.rg.end_key.clone();
    if req.limit == 0 {
        return Ok(partition_rpc::rkyv_encode(&RangeResp { code: CODE_OK, message: String::new(), entries: vec![], has_more: true, cur_end_key }));
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
    Ok(partition_rpc::rkyv_encode(&RangeResp { code: CODE_OK, message: String::new(), entries: out, has_more, cur_end_key }))
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

    // F196: refuse split when this PS's static core budget can't host the
    // right child. The check fires only when `--cpuset` was supplied;
    // pre-F196 deployments keep the legacy unlimited behaviour. We reject
    // BEFORE any flush/commit_length/multi_modify_split so retries don't
    // burn extent-node IO. Operator response: grow --cpuset, migrate
    // partitions to another PS, or (future Stage D advisory) merge a cold
    // partition first to free a slot.
    {
        let budget = part.borrow().partition_budget.clone();
        if budget.would_exceed(1) {
            return Err((
                StatusCode::FailedPrecondition,
                format!(
                    "F196: PS core budget exhausted ({} / {} partitions); split refused. \
                     Operator: grow --cpuset or merge a cold partition first.",
                    budget.current(),
                    budget.max,
                ),
            ));
        }
    }

    // F140 + F196 D-r7: drain in-flight compact + GC before commit_length.
    // compact's PS-wide concurrency permit lives on
    // `ConcurrencyController.acquire_compact`. gc_gate stays a
    // per-partition CompactionGate for split-vs-gc synchronization within
    // this partition. Both held through multi_modify_split via RAII.
    let (concurrency, gc_gate) = {
        let p = part.borrow();
        (p.concurrency_ctrl.clone(), p.gc_gate.clone())
    };
    let _compact_permit = concurrency.acquire_compact().await;
    let _gc_permit = gc_gate.acquire().await;

    // Fetch authoritative range from the manager. PartitionData.rg is set
    // at open_partition and is NOT refreshed by sync_regions_once for an
    // already-open partition, so after a previous split the local rg
    // still spans the old wide range. Picking mid_key against the stale
    // rg can yield a key outside the manager's narrowed range, which
    // multi_modify_split then rejects.
    let auth_rg: Range = {
        // 10 s — read-only manager call. Fetches authoritative range
        // for the F103 stale-rg fix. Bounded so split doesn't wedge
        // on a hung manager.
        let resp_bytes = pool
            .call_timeout(manager_addr, manager_rpc::MSG_GET_REGIONS, Bytes::new(), Duration::from_secs(10))
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

    let mid = user_keys[user_keys.len() / 2].clone();
    let (log_stream_id, row_stream_id, meta_stream_id) = {
        let p = part.borrow();
        (p.log_stream_id, p.row_stream_id, p.meta_stream_id)
    };

    // F210-C2: PrepareSplit-style freeze + drain. Pre-F210-C2 split called
    // `flush_memtable_locked(part)` directly then `commit_length` — but
    // partition_loop could still process in-flight Phase 2 writes
    // (their work is on stream_worker_loop, independent of split's stack)
    // during the await window. Those writes' bytes landed on EN past the
    // captured commit_length, then manager sealed at the captured value,
    // leaving the trailing bytes invisible on recovery. The fix mirrors
    // F185's merge freeze: set frozen_for_split → halt new Put/Delete via
    // handle_incoming_req's reject branch → park split_drain_ack →
    // partition_loop drains pending+inflight+imm → fires the ack
    // signal → split resumes. After this drain, commit_length is stable.
    //
    // handle_split_part runs on a spawned task (see MSG_SPLIT_PART in
    // handle_incoming_req) so its awaits don't block partition_loop.
    //
    // Idempotency: if a previous split is already in flight on this
    // partition, refuse. Same shape as the merge freeze's "already in
    // progress" check.
    let (drain_tx, drain_rx) = futures::channel::oneshot::channel::<Result<(), String>>();
    {
        let p = part.borrow();
        if p.split_drain_ack.borrow().is_some() || p.frozen_for_split.get().is_some() {
            return Err((
                StatusCode::FailedPrecondition,
                "split already in progress on this partition".to_string(),
            ));
        }
        if p.frozen_for_merge.get().is_some() {
            return Err((
                StatusCode::FailedPrecondition,
                "partition is frozen for merge; retry split after merge completes".to_string(),
            ));
        }
        p.frozen_for_split.set(Some(std::time::Instant::now()));
        *p.split_drain_ack.borrow_mut() = Some(drain_tx);
        // F210-C2 fix — wake partition_loop so its idle-path
        // select observes that split_drain_ack just transitioned to
        // Some. Without this the loop sleeps through the full
        // FREEZE_TTL (30s) on an idle partition and the TTL backstop
        // is the only thing that ever unwedges the handler. See
        // PartitionData.split_wake_tx docstring for the full story.
        let _ = p.split_wake_tx.unbounded_send(());
    }

    // Await drain. On TTL-driven backstop, drain_rx receives
    // Err(message). On flush failure, same Err path. Either way we
    // unfreeze and propagate.
    let drain_outcome = drain_rx.await;
    match drain_outcome {
        Ok(Ok(())) => {} // drain succeeded
        Ok(Err(msg)) => {
            // Drain hit a flush failure or TTL. Clean up.
            part.borrow().frozen_for_split.set(None);
            return Err((StatusCode::Internal, format!("split drain failed: {msg}")));
        }
        Err(_) => {
            // Sender dropped without sending — shouldn't happen under
            // single-threaded compio, but defensive.
            part.borrow().frozen_for_split.set(None);
            return Err((
                StatusCode::Internal,
                "split drain ack sender dropped without signaling".to_string(),
            ));
        }
    }

    // commit_length on each stream — now stable, because no in-flight
    // Phase 2 can complete (drain emptied them) and no new writes can
    // launch (frozen_for_split halts handle_incoming_req).
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
        // F210-C2: unfreeze on multi_modify_split failure so the partition
        // resumes serving writes (client will retry split). Without this
        // the partition stays frozen until FREEZE_TTL backstop fires.
        part.borrow().frozen_for_split.set(None);
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
    //
    // F212-fix: bump `region_epoch` in lock-step with the manager's
    // `next_region_epoch` rule (rg-rewrite ⇒ +1). Pre-fix, a gallery
    // `range(b"", b"", MAX)` issued before `region_sync_loop` had a
    // chance to drop+reopen this partition saw BOTH sides stale at the
    // old epoch: handle_range passed the epoch check, returned the
    // left-only entries with `cur_end_key = new_rg.end_key (= mid)`,
    // SDK's still-stale cache routed the next iteration back to the
    // same partition with `cursor = mid`, handle_range now returned
    // empty (past the narrowed range) + `cur_end_key = mid`, SDK's
    // defensive "cur_end_key didn't advance" trip fired, and the user
    // saw an empty list (HTTP 500 in gallery's `list_handler_inner`).
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
        p.region_epoch = p.region_epoch.saturating_add(1).max(2);
    }

    // F212-fix-2: publish the new (rg, log, row, meta, region_epoch)
    // tuple to the cross-thread mirror so `sync_regions_once` on the
    // main thread observes `prev == latest` on its next tick and
    // SKIPS the drop+reopen. Pre-fix this mirror was a frozen
    // snapshot from open time; the partition would get torn down on
    // every split's first tick even though its in-memory state was
    // already perfectly correct. The lock is held for one tuple
    // write; no I/O while holding it. Written AFTER the rg/epoch
    // borrow_mut block above so a concurrent `sync_regions_once`
    // observes either the fully-old or fully-new state, never a
    // half-updated tuple.
    {
        let p = part.borrow();
        let mut shared = p.opened_with_shared.lock();
        *shared = (
            p.rg.clone(),
            p.log_stream_id,
            p.row_stream_id,
            p.meta_stream_id,
            p.region_epoch,
        );
    }

    // F210-C2: unfreeze on success — split commit landed; the LEFT
    // (this partition's) post-split rg is now in effect, and merged
    // commit_length matches the manager's sealed_length. Writes can
    // resume against the narrower range.
    part.borrow().frozen_for_split.set(None);

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
        MAINTENANCE_AUTO_GC => {
            // F201: decode multi-tier filter params from wire request.
            let params = crate::GcAutoParams {
                ratio: req.gc_ratio,
                max_size: req.gc_max_size,
                stream_debt: req.gc_stream_debt,
                empty_only: req.gc_empty_only,
            };
            p.gc_tx
                .try_send(GcTask::Auto(params))
                .map_err(|_| "gc busy")
        }
        MAINTENANCE_FORCE_GC => p
            .gc_tx
            .try_send(GcTask::Force { extent_ids: req.extent_ids })
            .map_err(|_| "gc busy"),
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

/// F210-C4: manager pull of the partition's current vp_refs.
/// Manager invokes this from `handle_multi_modify_split` /
/// `handle_merge_partitions` BEFORE its atomic etcd txn so the
/// `apply_partition_vp_refs` diff against `vp_table_refs` is computed
/// against a fresh snapshot — not the (possibly stale) cached one.
pub(crate) async fn handle_pull_vp_refs(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
) -> HandlerResult {
    use autumn_rpc::partition_rpc::{PullVpRefsReq, PullVpRefsResp};
    let req: PullVpRefsReq = partition_rpc::rkyv_decode(&payload)
        .map_err(|e| (StatusCode::InvalidArgument, e))?;

    // Single borrow — collect snapshot synchronously. Mirrors the
    // logic in `collect_partition_vp_refs` (lib.rs:5038).
    let (part_id, refs) = {
        let p = part.borrow();
        let mut counts = std::collections::BTreeMap::<u64, u32>::new();
        for reader in &p.sst_readers {
            for &extent_id in &reader.vp_deps {
                *counts.entry(extent_id).or_insert(0) += 1;
            }
        }
        (p.part_id, counts.into_iter().collect::<Vec<_>>())
    };

    if part_id != req.part_id {
        return Ok(partition_rpc::rkyv_encode(&PullVpRefsResp {
            code: partition_rpc::CODE_NOT_FOUND,
            message: format!(
                "partition {} not owned by this PS (this part_id = {})",
                req.part_id, part_id
            ),
            refs: Vec::new(),
        }));
    }

    // F210-C4: a successful pull is functionally equivalent to a
    // successful sync_partition_vp_refs — manager will apply the
    // snapshot. Clear vp_refs_dirty optimistically; if the manager's
    // apply fails, the next regular sync (or retry loop) will
    // re-mark dirty.
    part.borrow().vp_refs_dirty.set(false);

    Ok(partition_rpc::rkyv_encode(&PullVpRefsResp {
        code: CODE_OK,
        message: String::new(),
        refs,
    }))
}

// ---------------------------------------------------------------------------
// F204 — `map_storage_error` translation tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod f204_map_storage_error_tests {
    use super::map_storage_error;
    use autumn_rpc::StatusCode;
    use autumn_stream::StaleVpOffset;

    /// A `StaleVpOffset` anywhere in the anyhow chain must surface as
    /// `FailedPrecondition` with the sentinel's stable Display string
    /// preserved verbatim — the Python operational tooling contract.
    #[test]
    fn stale_vp_surfaces_as_failed_precondition() {
        let stale = StaleVpOffset {
            extent_id: 29,
            requested_offset: 264_784_123,
            requested_length: 10_918_365,
            sealed_length: 11_570_792,
        };
        let raw = anyhow::Error::new(stale).context("resolve_value");
        let (code, msg) = map_storage_error(&raw);
        assert_eq!(code, StatusCode::FailedPrecondition);
        // We surface the SENTINEL's Display, not anyhow's full chain.
        assert!(
            msg.starts_with("stale_vp_offset_past_sealed_length:"),
            "wire-contract prefix preserved: {msg}"
        );
        assert!(msg.contains("extent=29"));
        assert!(msg.contains("offset=264784123"));
        assert!(msg.contains("length=10918365"));
        assert!(msg.contains("sealed_length=11570792"));
    }

    /// Generic errors (network / disk / decode / etc.) keep mapping to
    /// `Internal` so we don't accidentally promote unrelated failures
    /// into the `FailedPrecondition` channel.
    #[test]
    fn unrecognised_error_falls_back_to_internal() {
        let raw = anyhow::anyhow!("connection closed mid-read");
        let (code, msg) = map_storage_error(&raw);
        assert_eq!(code, StatusCode::Internal);
        assert_eq!(msg, "connection closed mid-read");
    }

    /// Sentinel buried under multiple `.context()` layers must still
    /// be recognised — `resolve_value` adds context, `read_value_from_log`
    /// adds context, etc. The chain walk catches it at any depth.
    #[test]
    fn sentinel_deep_in_chain_still_recognised() {
        let stale = StaleVpOffset {
            extent_id: 7,
            requested_offset: 100,
            requested_length: 50,
            sealed_length: 80,
        };
        let raw = anyhow::Error::new(stale)
            .context("ec_subrange_read")
            .context("read_bytes_from_extent")
            .context("resolve_value");
        let (code, msg) = map_storage_error(&raw);
        assert_eq!(code, StatusCode::FailedPrecondition);
        assert!(msg.contains("extent=7"));
        assert!(msg.contains("sealed_length=80"));
    }
}
