//! RPC dispatch and handler functions for partition operations.
//!
//! `handle_put`, `handle_delete`, and `handle_stream_put` are gone —
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

use crate::sstable::{AsyncMergeIterator, AsyncTableIterator, FetchMode};
use crate::*;

/// translate VP-resolve errors into wire status codes that
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

/// TiKV-style routing-freshness check shared by the read handlers
/// (get / head / range / batch_get). `req_epoch == 0` skips the check
/// (bootstrap / tests / legacy callers). On mismatch returns a
/// `FailedPrecondition` so the SDK's `Err`-arm refresh+retry engages.
fn check_region_epoch(part_id: u64, have: u64, req_epoch: u64) -> Result<(), (StatusCode, String)> {
    if req_epoch != 0 && req_epoch != have {
        return Err((
            StatusCode::FailedPrecondition,
            format!("region epoch stale: part_id={part_id} have={have} got={req_epoch}"),
        ));
    }
    Ok(())
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

/// PUT / DELETE / STREAM_PUT are handled by `partition_loop`'s
/// direct `handle_incoming_req` path (no spawn, no inner oneshot). Only
/// reads and low-frequency control ops route through this dispatch function.
/// Receiving a write op here is a bug — we short-circuit with an error.
pub(crate) async fn dispatch_partition_rpc(
    msg_type: u8,
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
    part_sc: &Rc<StreamClient>,
    _pool: &Rc<ConnPool>,
    _manager_addr: &str,
    _owner_key: &str,
    _revision: i64,
) -> HandlerResult {
    match msg_type {
        MSG_GET => handle_get(payload, part).await,
        MSG_GET_REDIRECT => handle_get_redirect(payload, part).await,
        MSG_GET_REDIRECT_MANY => handle_get_redirect_many(payload, part).await,
        MSG_HEAD => handle_head(payload, part).await,
        MSG_RANGE => handle_range(payload, part).await,
        partition_rpc::MSG_BATCH_GET => handle_batch_get(payload, part).await,
        MSG_GET_DISCARDS => handle_get_discards(payload, part, part_sc).await,
        // SPLIT_PART must NOT be invoked inline through
        // dispatch_partition_rpc — handle_split_part awaits an internal
        // drain signal that requires partition_loop to run, and
        // an inline call would self-deadlock (the loop's stack is parked
        // here). MSG_SPLIT_PART is now intercepted in
        // `handle_incoming_req` and dispatched via `compio::runtime::spawn`.
        MSG_SPLIT_PART => Err((
            StatusCode::Internal,
            "MSG_SPLIT_PART must not be dispatched inline — requires a spawned task; \
             routed via handle_incoming_req's MSG_SPLIT_PART arm"
                .to_string(),
        )),
        MSG_MAINTENANCE => handle_maintenance(payload, part, part_sc).await,
        MSG_DIAG_TRACE_KEY => handle_diag_trace_key(payload, part).await,
        partition_rpc::MSG_DIAG_PARTITION_VP => {
            handle_diag_partition_vp(payload, part, part_sc).await
        }
        // server-side multipart (MSG_PUT_BEGIN/CHUNK/COMMIT/ABORT)
        // removed. Stripe-write is now pure client-side via
        // ClusterClient::put_stream_begin (Ceph striperados pattern).
        MSG_PUT | MSG_DELETE => Err((
            StatusCode::Internal,
            format!("write msg_type {msg_type} must be routed via partition_loop"),
        )),
        _ => Err((
            StatusCode::InvalidArgument,
            format!("unknown msg_type {msg_type}"),
        )),
    }
}

/// Outcome of the shared GET resolve core: the value bytes, or a not-found.
/// `Value` is a `Bytes` that, for a VP read over UCX, ALIASES the registered
/// RegPool buffer (R4) — `handle_get_bulk` sends it as its own iovec (no copy);
/// `handle_get` copies it into the rkyv `GetResp` (which copies regardless).
pub(crate) enum GetOutcome {
    NotFound,
    Value(Bytes),
    /// large full-value VP — the caller (handle_get_redirect) sends
    /// a descriptor instead of resolving the bytes through this PS.
    Redirect { extent_id: u64, value_offset: u64, value_len: u64 },
}

/// MSG_BATCH_GET: N keys on the SAME partition in ONE frame, all
/// resolved in this single handler invocation. The handler runs INLINE
/// on the ps-conn task (reads never go through `partition_loop`'s
/// mpsc), so the only "batching" benefit here is amortising the wire-
/// frame overhead: one decode of `BatchGetReq`, one encode of
/// `BatchGetResp` carrying all values, vs N independent GET round
/// trips. Per-key value lookup reuses the existing `get_value`
/// internal so VP resolution + read-pin semantics are unchanged.
/// `MSG_BATCH_GET_BULK` — the same resolve loop as `handle_batch_get`, but the
/// values leave as their own iovecs instead of being copied into the response.
///
/// `get_value` already hands back a `Bytes` (for a VP read it aliases the pool
/// buffer), so the inline form's `v.to_vec()` was pure loss; everything after it
/// — two copies through `rkyv_encode`, another when `Frame::encode` assembles
/// the wire buffer, and a CRC pass over the lot — is avoided by putting the
/// values in the tail and only the statuses and lengths in ctrl.
///
/// A batch-level failure (stale epoch) answers with a non-OK code and no
/// values; per-key outcomes ride `statuses`, exactly as the inline form does.
pub(crate) async fn handle_batch_get_bulk(
    req_id: u32,
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
) -> (Bytes, Vec<Bytes>) {
    let fail = |code: StatusCode, msg: String| {
        let ctrl = partition_rpc::rkyv_encode(&partition_rpc::BatchGetBulkCtrl {
            message: msg,
            statuses: Vec::new(),
            value_lens: Vec::new(),
        });
        (
            autumn_rpc::frame::encode_bulk_response_head_bytes(
                req_id,
                partition_rpc::MSG_BATCH_GET_BULK,
                code as u8,
                &ctrl,
                0,
            ),
            Vec::new(),
        )
    };
    let req: partition_rpc::BatchGetReq = match partition_rpc::rkyv_decode(&payload) {
        Ok(r) => r,
        Err(e) => return fail(StatusCode::InvalidArgument, e),
    };
    {
        let p = part.borrow();
        if let Err((code, msg)) = check_region_epoch(p.part_id, p.region_epoch, req.region_epoch) {
            drop(p);
            return fail(code, msg);
        }
    }
    let n = req.keys.len();
    let mut statuses: Vec<u8> = Vec::with_capacity(n);
    let mut value_lens: Vec<u32> = Vec::with_capacity(n);
    let mut values: Vec<Bytes> = Vec::with_capacity(n);
    for key in req.keys.into_iter() {
        // Route each key through `get_value` for the same VP resolution /
        // read-pin / not-found / out-of-range semantics as a per-key MSG_GET.
        let inner = partition_rpc::rkyv_encode(&GetReq {
            part_id: 0, // routing already done
            region_epoch: req.region_epoch,
            key,
            offset: 0,
            length: 0,
        });
        let (status, value) = match get_value(inner, part).await {
            Ok(GetOutcome::Value(v)) => (0u8, v),
            Ok(GetOutcome::NotFound) => (1u8, Bytes::new()),
            Ok(GetOutcome::Redirect { .. }) => unreachable!("get_value never redirects"),
            Err(_) => (2u8, Bytes::new()),
        };
        statuses.push(status);
        value_lens.push(value.len() as u32);
        if !value.is_empty() {
            values.push(value);
        }
    }
    let total: usize = value_lens.iter().map(|l| *l as usize).sum();
    let ctrl = partition_rpc::rkyv_encode(&partition_rpc::BatchGetBulkCtrl {
        message: String::new(),
        statuses,
        value_lens,
    });
    (
        autumn_rpc::frame::encode_bulk_response_head_bytes(
            req_id,
            partition_rpc::MSG_BATCH_GET_BULK,
            CODE_OK,
            &ctrl,
            total,
        ),
        values,
    )
}

pub(crate) async fn handle_batch_get(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
) -> HandlerResult {
    let req: partition_rpc::BatchGetReq =
        partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
    // Quick epoch check up front so a stale-routed batch fails fast
    // (without re-decoding for each key).
    {
        let p = part.borrow();
        check_region_epoch(p.part_id, p.region_epoch, req.region_epoch)?;
    }
    let mut items: Vec<partition_rpc::BatchGetItem> = Vec::with_capacity(req.keys.len());
    for key in req.keys.into_iter() {
        // Re-encode each per-key GetReq and route through `get_value`
        // so VP resolution / read-pin / not-found / out-of-range
        // semantics are IDENTICAL to a per-key MSG_GET. Per-call
        // borrow of `part` is brief (lookup_in_memtable + clone of
        // stream_client, see `get_value`); cheaper than fan-out
        // overhead on N separate frames.
        let inner = partition_rpc::rkyv_encode(&GetReq {
            part_id: 0, // not used inside get_value — routing already done
            region_epoch: req.region_epoch,
            key,
            offset: 0,
            length: 0,
        });
        match get_value(inner, part).await {
            Ok(GetOutcome::Value(v)) => items.push(partition_rpc::BatchGetItem {
                status: 0,
                value: v.to_vec(),
            }),
            Ok(GetOutcome::NotFound) => items.push(partition_rpc::BatchGetItem {
                status: 1,
                value: Vec::new(),
            }),
            // get_value (redirect=false) never yields Redirect.
            Ok(GetOutcome::Redirect { .. }) => unreachable!("get_value never redirects"),
            Err(_) => items.push(partition_rpc::BatchGetItem {
                status: 2,
                value: Vec::new(),
            }),
        }
    }
    Ok(partition_rpc::rkyv_encode(
        &partition_rpc::BatchGetResp {
            code: CODE_OK,
            message: String::new(),
            items,
        },
    ))
}

/// rkyv-framed GET (generic SDK path).
/// seal + roll the requested open tails so a fenced node's
/// replicas drain. The manager's recovery sweep sends this when an OPEN tail
/// (`!sealed`) has a replica on a Fenced node — recovery rebuilds only SEALED
/// extents, so without a roll an idle partition's open tail on a fenced node
/// never drains and blocks `remove` forever. Log/meta tails roll on P-log's
/// `part_sc` (the client that owns them); the row tail routes through P-sst's
/// `row_invalidate_tx` barrier with `seal_and_roll=true` (row_stream
/// single-writer invariant — lib.rs note 16). Idempotent: an entry whose
/// current tail no longer equals the requested `expected_tail` (already rolled
/// by a natural write or a prior sweep) is skipped. Best-effort: a failed roll
/// (e.g. all replicas unreachable → manager Precondition) is logged; the sweep
/// retries on its cooldown.
pub(crate) async fn handle_roll_tails(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
    part_sc: &Rc<StreamClient>,
) -> HandlerResult {
    let req: RollTailsReq =
        partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
    let (log_id, row_id, meta_id, part_id, row_inv_tx) = {
        let p = part.borrow();
        (
            p.log_stream_id,
            p.row_stream_id,
            p.meta_stream_id,
            p.part_id,
            p.row_invalidate_tx.clone(),
        )
    };
    if req.part_id != part_id {
        return Ok(partition_rpc::rkyv_encode(&RollTailsResp {
            code: CODE_NOT_FOUND,
            rolled: 0,
            message: format!("partition {} not served here", req.part_id),
        }));
    }
    let mut rolled = 0u32;
    for (stream_id, expected_tail) in req.entries {
        // Never roll while a split/merge freeze holds this partition: the
        // orchestration has (or is about to) capture per-stream commit lengths
        // and the manager applies them to whatever extent is the CURRENT tail
        // at commit time. A roll landing inside that window swaps the tail,
        // so the captured length would be stamped onto the roll's fresh empty
        // extent (sealed longer than any replica holds → the CoW child cannot
        // read it). The roll is best-effort with a manager-side retry
        // cooldown, so skipping here just defers it past the freeze.
        if part.borrow().frozen_for_split.get().is_some()
            || part.borrow().frozen_for_merge.get().is_some()
        {
            tracing::info!(
                target: "roll_trace",
                part_id,
                stream_id,
                "roll_tails: partition frozen for split/merge — deferring roll"
            );
            continue;
        }
        // Idempotency: skip if the tail already rolled (current != expected).
        let cur_tail = match part_sc.get_stream_info(stream_id).await {
            Ok(info) => info.extent_ids.last().copied().unwrap_or(0),
            Err(e) => {
                tracing::warn!(stream_id, error = %e, "roll_tails: get_stream_info failed");
                continue;
            }
        };
        if cur_tail != expected_tail {
            continue; // already rolled by a natural write or a prior sweep
        }
        // TEST SYNC-POINT: hold the roll here (freeze/idempotency checks done,
        // seal not yet issued) so a test can start a split freeze and reach its
        // commit phase while this roll is "in flight". Off in production (one
        // relaxed load; only `set_roll_tails_pause` sets it).
        if crate::roll_tails_paused() {
            crate::note_roll_tails_parked();
            while crate::roll_tails_paused() {
                compio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        }
        if stream_id == row_id {
            // row_stream single-writer invariant: seal+roll on P-sst's sst_sc
            // via the row-invalidate barrier (drains inflight to zero first).
            let (tx, rx) = futures::channel::oneshot::channel::<()>();
            let mut inv_tx = row_inv_tx.clone();
            let br = crate::RowInvalidateBarrierReq {
                row_stream_id: row_id,
                seal_and_roll: true,
                resp_tx: tx,
            };
            if inv_tx.send(br).await.is_ok() && rx.await.is_ok() {
                rolled += 1;
            }
        } else if stream_id == log_id || stream_id == meta_id {
            tracing::info!(
                target: "roll_trace",
                part_id,
                stream_id,
                expected_tail,
                kind = if stream_id == log_id { "log" } else { "meta" },
                "roll_tails: sealing+rolling live tail"
            );
            match part_sc.seal_and_roll_tail(stream_id).await {
                Ok(()) => rolled += 1,
                Err(e) => {
                    tracing::warn!(stream_id, error = %e, "roll_tails: seal_and_roll_tail failed")
                }
            }
        }
    }
    Ok(partition_rpc::rkyv_encode(&RollTailsResp {
        code: CODE_OK,
        rolled,
        message: String::new(),
    }))
}

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
        GetOutcome::Redirect { .. } => unreachable!("get_value never redirects"),
    }
}

/// (MSG_GET_REDIRECT): like `handle_get`, but a large full-value VP
/// answers with a descriptor (extent + value byte range + eversion +
/// replica addrs) so the client reads the bytes straight from an EN.
pub(crate) async fn handle_get_redirect(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
) -> HandlerResult {
    match get_value_inner(payload.clone(), part, true).await? {
        GetOutcome::NotFound => Ok(partition_rpc::rkyv_encode(&GetRedirectResp {
            code: CODE_NOT_FOUND,
            message: "key not found".to_string(),
            value: vec![],
            extent_id: 0,
            value_offset: 0,
            value_len: 0,
            eversion: 0,
            replica_addrs: vec![],
        })),
        GetOutcome::Value(value) => Ok(partition_rpc::rkyv_encode(&GetRedirectResp {
            code: CODE_OK,
            message: String::new(),
            value: value.into(),
            extent_id: 0,
            value_offset: 0,
            value_len: 0,
            eversion: 0,
            replica_addrs: vec![],
        })),
        GetOutcome::Redirect {
            extent_id,
            value_offset,
            value_len,
        } => {
            let sc = part.borrow().stream_client.clone();
            match sc.extent_read_descriptor(extent_id).await {
                Ok((eversion, replica_addrs)) => {
                    Ok(partition_rpc::rkyv_encode(&GetRedirectResp {
                        code: CODE_OK,
                        message: String::new(),
                        value: vec![],
                        extent_id,
                        value_offset,
                        value_len,
                        eversion,
                        replica_addrs,
                    }))
                }
                // Descriptor lookup failed (manager blip / cache miss):
                // resolve through the proxy path instead — redirect is an
                // optimization, never a correctness dependency.
                //
                // The `Err(_)` here threw away the only explanation that
                // existed. `extent_read_descriptor` refuses with a specific
                // reason ("extent N is EC-converted", "keeps its payload
                // outside .dat", or a manager lookup failure), and every one of
                // them was discarded — so a client seeing 100% failure on this
                // path got no reason from the PS, nothing in the PS log, and
                // nothing in the EN log (the ENs are never contacted). Three
                // silent channels, and the failure then presented as an opaque
                // rkyv decode error at the client. Log it.
                //
                // Note this arm answers a MSG_GET_REDIRECT with a `GetResp`,
                // not a `GetRedirectResp` — a wire-contract violation that is
                // what MAKES the client's decode fail. The client now degrades
                // to the proxy on an undecodable descriptor, but the right fix
                // is for this arm to return the inline shape above
                // (`extent_id: 0` + value), which costs one round trip instead
                // of two. Left as-is here only because that needs the resolved
                // value rather than an encoded response.
                Err(e) => {
                    tracing::warn!(
                        extent_id,
                        error = %e,
                        "get_redirect: descriptor lookup failed — answering via the proxy path"
                    );
                    handle_get(payload, part).await
                }
            }
        }
    }
}

/// (MSG_GET_REDIRECT_MANY): resolve N redirect descriptors in
/// ONE PS call — the batch mirror of `handle_get_redirect`. Loops
/// `get_value_inner` per item (same resolution + `extent_read_descriptor`
/// lookup + inline fallback as the single handler) and returns one
/// `GetRedirectResp` per item, in input order. An epoch/range error propagates
/// as a batch error (`?`) exactly like the single handler — the SDK refreshes +
/// retries (or falls back per item). The win: the client makes ONE round-trip
/// per partition instead of one per extent, so a large-file read's ~630 redirect
/// resolutions no longer serialize on this partition's task.
pub(crate) async fn handle_get_redirect_many(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
) -> HandlerResult {
    let req: GetRedirectManyReq =
        partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
    let not_found = || GetRedirectResp {
        code: CODE_NOT_FOUND,
        message: "key not found".to_string(),
        value: vec![],
        extent_id: 0,
        value_offset: 0,
        value_len: 0,
        eversion: 0,
        replica_addrs: vec![],
    };
    let inline = |value: Vec<u8>| GetRedirectResp {
        code: CODE_OK,
        message: String::new(),
        value,
        extent_id: 0,
        value_offset: 0,
        value_len: 0,
        eversion: 0,
        replica_addrs: vec![],
    };
    let mut results = Vec::with_capacity(req.items.len());
    for item in &req.items {
        let item_payload = partition_rpc::rkyv_encode(&GetReq {
            part_id: req.part_id,
            key: item.key.clone(),
            offset: item.offset,
            length: item.length,
            region_epoch: req.region_epoch,
        });
        let resp = match get_value_inner(item_payload, part, true).await? {
            GetOutcome::NotFound => not_found(),
            // Inline only ever carries a SMALL value here: get_value_inner
            // redirects any VP sub-range ≥ 64 KiB, so a Value outcome means the
            // value is below the redirect threshold (bounded — no batch bloat).
            GetOutcome::Value(value) => inline(value.into()),
            GetOutcome::Redirect {
                extent_id,
                value_offset,
                value_len,
            } => {
                let sc = part.borrow().stream_client.clone();
                match sc.extent_read_descriptor(extent_id).await {
                    Ok((eversion, replica_addrs)) => GetRedirectResp {
                        code: CODE_OK,
                        message: String::new(),
                        value: vec![],
                        extent_id,
                        value_offset,
                        value_len,
                        eversion,
                        replica_addrs,
                    },
                    // Descriptor lookup failed (manager blip / EC-converted /
                    // cache miss). Do NOT inline the (large) value here — the
                    // single handler inlines ONE, but a mass descriptor failure
                    // during a model load would aggregate hundreds of 8 MiB
                    // values into one GB-scale response frame (coco P1 = OOM /
                    // over-cap frame). FAIL the batch instead: the client leaves
                    // this partition's descriptors unset and re-reads each item
                    // via the per-item proxy path (get_range_direct_into →
                    // call_ps_for_key, which refreshes). The batched redirect is
                    // an optimization, never a correctness dependency.
                    Err(e) => {
                        return Err((
                            StatusCode::Unavailable,
                            format!("redirect descriptor lookup failed: {e}"),
                        ));
                    }
                }
            }
        };
        results.push(resp);
    }
    Ok(partition_rpc::rkyv_encode(&GetRedirectManyResp { results }))
}

/// zero-copy GET (MSG_GET_BULK): returns the response as TWO segments —
/// `(head, value)` where `head = [CRC-less frame header][bulk meta: code +
/// value_len + reserved]` and `value` ALIASES the RegPool buffer (R4: `Bytes::from_owner`
/// from `resolve_value`, no copy). The ps-conn pushes `head` then `value` into
/// `tx_bufs` so the single `write_vectored_all` emits them as one wire frame with
/// NO concat copy — fully zero-copy EN->PS->client. (Pre-R4 this concatenated
/// `[meta][value]` into a Vec, copied again by `encode_v0`.)
///
/// ALL outcomes (incl errors) map to a bulk-shaped response — the status (and,
/// since v28, a human-readable message) rides in the CRC-protected ctrl; the
/// SDK's get_into maps non-OK codes to refresh/retry. StatusCode discriminants
/// align with the partition CODE_* for the GET-relevant cases
/// (InvalidArgument=2, FailedPrecondition=3, Internal=4). So this never errors.
pub(crate) async fn handle_get_bulk(
    req_id: u32,
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
) -> (Bytes, Bytes) {
    let (code, msg, value): (u8, String, Bytes) = match get_value(payload, part).await {
        Ok(GetOutcome::Value(v)) => (CODE_OK, String::new(), v),
        Ok(GetOutcome::NotFound) => (CODE_NOT_FOUND, String::new(), Bytes::new()),
        // get_value (redirect=false) never yields Redirect.
        Ok(GetOutcome::Redirect { .. }) => unreachable!("get_value never redirects"),
        Err((status, msg)) => (status as u8, msg, Bytes::new()),
    };
    (ps_bulk_head(req_id, code, &msg, value.len()), value)
}

/// Build the MSG_GET_BULK response head — v28 value-separable frame head
/// `[header][ctrl_len][code+message][crc]` (crc covers header+ctrl; see
/// autumn-rpc frame.rs). The value is sent as a SEPARATE `Bytes` right after
/// (aliasing the RegPool buffer) so it is never copied and never crc-scanned.
/// Mirrors `extent_node::bulk_read_head`.
pub(crate) fn ps_bulk_head(req_id: u32, code: u8, msg: &str, value_len: usize) -> Bytes {
    autumn_rpc::frame::encode_bulk_response_head(req_id, MSG_GET_BULK, code, msg, value_len)
}

/// Shared GET resolve core: epoch/range check → memtable/imm/SST lookup →
/// VP resolve (read_value_from_log). Used by both `handle_get` (rkyv) and
/// `handle_get_bulk` (value-separable). Carries the read metrics.
// clippy false-positive: the `part.borrow()` (`p`) is explicitly `drop(p)`-ed
// (see below) BEFORE the only `.await` (`resolve_value`). The lint flags the
// borrow because an await exists later in the fn; it doesn't track the drop.
#[allow(clippy::await_holding_refcell_ref)]
async fn get_value(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
) -> Result<GetOutcome, (StatusCode, String)> {
    get_value_inner(payload, part, false).await
}

/// `redirect_large_vp` — when true, a FULL-value read of a VP whose
/// value length >= AUTUMN_PS_ZC_RECV_MIN (64 KiB, the bulk_worthwhile
/// threshold) returns `GetOutcome::Redirect` (extent + exact value byte
/// range) instead of resolving through this PS. Sub-range reads, inline
/// values and small VPs resolve as before. The GC writer-pin check still
/// runs first — an extent being punched surfaces NotFound exactly like
/// the proxy path (the client falls back / retries and sees the
/// rewritten VP).
#[allow(clippy::await_holding_refcell_ref)]
/// paged SST point-lookup with the compaction-truncate retry — shared by
/// `get_value_inner` and `handle_head` (the two MUST stay in sync).
///
/// A snapshot reader's backing row extent can be TRUNCATED by a compaction
/// that completes during our block read (paged readers hold metadata only,
/// not bytes). On a read error, if the live `sst_readers` set changed since
/// the snapshot, retry ONCE against the fresh set — the key (if it exists)
/// is in the compaction's output SSTs. Unchanged set or second failure = a
/// real read error, surfaced as Internal (coco P0: NEVER a false NotFound).
///
/// Borrows `part` internally and releases before every await (note 15);
/// returns with NO borrow held.
async fn sst_lookup_paged_retry(
    part: &Rc<RefCell<PartitionData>>,
    key: &[u8],
) -> Result<Option<(u8, Bytes, u64)>, (StatusCode, String)> {
    let mut attempt = 0u32;
    'sst_lookup: loop {
        attempt += 1;
        // Borrow scoped to the snapshot — released before any await.
        let (readers, sc, cache) = {
            let p = part.borrow();
            (
                p.sst_readers.to_vec(),
                p.stream_client.clone(),
                p.block_cache.clone(),
            )
        };
        for reader in readers.iter().rev() {
            match lookup_in_sst_via(reader, key, &sc, &cache).await {
                Ok(Some(r)) => return Ok(Some(r)),
                Ok(None) => {}
                Err(e) => {
                    let changed =
                        crate::sst_readers_changed(&part.borrow().sst_readers, &readers);
                    if attempt == 1 && changed {
                        continue 'sst_lookup;
                    }
                    return Err((StatusCode::Internal, format!("sst block read: {e}")));
                }
            }
        }
        return Ok(None);
    }
}

async fn get_value_inner(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
    redirect_large_vp: bool,
) -> Result<GetOutcome, (StatusCode, String)> {
    let req: GetReq =
        partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

    let lookup_t0 = Instant::now();
    // Borrow scoped to the epoch/range check + memtable lookups — released
    // before any await (note 15). `source` tracks where the key was found:
    // 0=miss, 1=mem, 2=imm, 3=sst.
    let mut source = 0u8;
    let mut found: Option<(u8, Bytes, u64)> = {
        let p = part.borrow();
        check_region_epoch(p.part_id, p.region_epoch, req.region_epoch)?;
        if !in_range(&p.rg, &req.key) {
            return Err((
                StatusCode::InvalidArgument,
                "key is out of range".to_string(),
            ));
        }
        lookup_in_memtable(&p.active, &req.key)
            .inspect(|_r| {
                source = 1;
            })
            .or_else(|| {
                for imm in p.imm.iter().rev() {
                    if let Some(r) = lookup_in_memtable(imm, &req.key) {
                        source = 2;
                        return Some(r);
                    }
                }
                None
            })
    };
    // the SST lookup may FETCH blocks from row_stream (paged
    // readers) — `sst_lookup_paged_retry` snapshots the reader set, drops
    // the borrow across its awaits (note 15), and handles the
    // compaction-truncate single retry. The re-borrow below may observe
    // post-split state for the pin / redirect section — same relaxation
    // batch_get already accepts.
    if found.is_none() {
        if let Some(r) = sst_lookup_paged_retry(part, &req.key).await? {
            source = 3;
            found = Some(r);
        }
    }
    let p = part.borrow();
    let lookup_ns = lookup_t0.elapsed().as_nanos() as u64;

    // One read-metrics record per outcome. `source` 0 = counted as
    // not_found (miss, tombstone/expired, or pinned-away); vp fields only
    // accumulate on the resolved-VP outcome.
    let record_read = |source: u8, is_vp: bool, vp_resolve_ns: u64| {
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
    };

    let (op, raw_value, expires_at) = match found {
        Some(v) => v,
        None => {
            record_read(0, false, 0);
            return Ok(GetOutcome::NotFound);
        }
    };
    if op == 2 || (expires_at > 0 && expires_at <= now_secs()) {
        record_read(0, false, 0);
        return Ok(GetOutcome::NotFound);
    }

    let sc = p.stream_client.clone();
    let is_vp = (op & crate::OP_VALUE_POINTER) != 0;

    // (MED-2): acquire a per-extent reader pin BEFORE dropping the
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
                // GC holds the writer pin on this extent. That means a punch is
                // imminent — it does NOT mean this value is gone: GC relocates
                // every live in-range value BEFORE punching (relocate-then-
                // punch), so a live key's bytes are either still here or already
                // rewritten elsewhere.
                //
                // This used to answer NotFound, which the comment justified as
                // "the client falls back / retries". It cannot: NotFound is
                // terminal on the client, so a live key read during a GC window
                // came back as absent — a false miss, exactly what
                // `sst_lookup_paged_retry` in this file refuses to produce.
                // Unavailable maps to a RETRYABLE client error, and the pin is
                // held only for the length of the punch, so the retry finds
                // either the original bytes or the relocated VP.
                record_read(0, false, 0);
                return Err((
                    StatusCode::Unavailable,
                    format!(
                        "extent {} is being reclaimed by GC; retry",
                        vp.extent_id
                    ),
                ));
            }
        }
    } else {
        None
    };
    // redirect a VP read (whole value OR a sub-range) to the
    // EN when the REQUESTED byte count is >= 64 KiB. `req.offset`/`req.length`
    // address bytes WITHIN the value (`length == 0` = to end); the descriptor
    // carries the absolute in-extent range `[vp.offset + req.offset, +req_len)`
    // so the client reads exactly the requested sub-range straight from a
    // replica. Whole-value single-key `get_direct` (offset=0,length=0) is the
    // `req.offset==0 && req_len==vp.len` special case — unchanged. Sub-ranges
    // past the value end (`req.offset > vp.len`) and sub-64 KiB requests fall
    // through to the inline proxy resolve below (identical to the whole-value path).
    if redirect_large_vp && is_vp && raw_value.len() >= crate::VALUE_POINTER_SIZE {
        let vp = crate::ValuePointer::decode(&raw_value[..crate::VALUE_POINTER_SIZE]);
        let r_off = req.offset as u64;
        if r_off <= vp.len {
            let r_len = if req.length == 0 {
                vp.len - r_off
            } else {
                (req.length as u64).min(vp.len - r_off)
            };
            if r_len >= 64 * 1024 {
                record_read(source, false, 0);
                // _vp_pin drops at return — the client's direct read is
                // deliberately unprotected: a GC punch in the window turns
                // into a failed EN read -> client proxy fallback (never a
                // torn read; extents are unlinked whole and eversion-fenced).
                return Ok(GetOutcome::Redirect {
                    extent_id: vp.extent_id,
                    value_offset: vp.offset + r_off,
                    value_len: r_len,
                });
            }
        }
    }
    drop(p);

    let vp_t0 = Instant::now();
    let value = resolve_value(op, raw_value, &sc, req.offset as u64, req.length as u64)
        .await
        .map_err(|e| map_storage_error(&e))?;
    let vp_resolve_ns = if is_vp {
        vp_t0.elapsed().as_nanos() as u64
    } else {
        0
    };
    // _vp_pin guard drops here, releasing the pin.

    record_read(source, is_vp, vp_resolve_ns);

    // Read-throughput accounting: bytes the PS actually served (resolved-value
    // path only; large-VP Redirect is read directly from the EN, not here).
    part.borrow()
        .metrics
        .read_bytes
        .fetch_add(value.len() as u64, std::sync::atomic::Ordering::Relaxed);

    Ok(GetOutcome::Value(value))
}

pub(crate) async fn handle_head(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
) -> HandlerResult {
    let req: HeadReq =
        partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

    // Borrow scoped to the epoch/range check + memtable lookups — released
    // before any await (note 15).
    let mut found = {
        let p = part.borrow();
        check_region_epoch(p.part_id, p.region_epoch, req.region_epoch)?;
        if !in_range(&p.rg, &req.key) {
            return Err((
                StatusCode::InvalidArgument,
                "key is out of range".to_string(),
            ));
        }
        lookup_in_memtable(&p.active, &req.key).or_else(|| {
            for imm in p.imm.iter().rev() {
                if let Some(r) = lookup_in_memtable(imm, &req.key) {
                    return Some(r);
                }
            }
            None
        })
    };
    // paged SST lookup awaits — `sst_lookup_paged_retry` snapshots +
    // drops the borrow (note 15) and runs the same compaction-truncate
    // single retry as get_value_inner.
    if found.is_none() {
        found = sst_lookup_paged_retry(part, &req.key).await?;
    }

    let (op, raw_value, expires_at) = match found {
        Some(v) => v,
        None => {
            return Ok(partition_rpc::rkyv_encode(&HeadResp {
                code: CODE_NOT_FOUND,
                message: "key not found".to_string(),
                found: false,
                value_length: 0,
            }))
        }
    };
    if op == 2 || (expires_at > 0 && expires_at <= now_secs()) {
        return Ok(partition_rpc::rkyv_encode(&HeadResp {
            code: CODE_NOT_FOUND,
            message: "key not found".to_string(),
            found: false,
            value_length: 0,
        }));
    }

    let value_len = if op & OP_VALUE_POINTER != 0 && raw_value.len() >= VALUE_POINTER_SIZE {
        ValuePointer::decode(&raw_value[..VALUE_POINTER_SIZE]).len
    } else {
        raw_value.len() as u64
    };

    Ok(partition_rpc::rkyv_encode(&HeadResp {
        code: CODE_OK,
        message: String::new(),
        found: true,
        value_length: value_len,
    }))
}

pub(crate) async fn handle_range(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
) -> HandlerResult {
    let req: RangeReq =
        partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

    let p = part.borrow();
    // Load-bearing for `range()` correctness after a split: without it,
    // mismatched-epoch range requests are silently filtered per-key and
    // return a valid-but-partial result with `code:OK` (the gallery bug).
    // A stale snapshot's epoch is rejected up-front; SDK refreshes + re-runs.
    check_region_epoch(p.part_id, p.region_epoch, req.region_epoch)?;
    // Snapshot the PS's authoritative end_key so the response can carry it
    // as a resume cursor for the SDK. Empty = unbounded right side (last
    // partition in the keyspace).
    let cur_end_key = p.rg.end_key.clone();
    if req.limit == 0 {
        return Ok(partition_rpc::rkyv_encode(&RangeResp {
            code: CODE_OK,
            message: String::new(),
            entries: vec![],
            has_more: true,
            cur_end_key,
        }));
    }

    let start_user_key = if req.start.is_empty() {
        req.prefix.clone()
    } else {
        req.start.clone()
    };
    let seek_key = key_with_ts(&start_user_key, u64::MAX);

    drop(p);

    // + coco P1: the scan reads blocks on demand, so a background
    // compaction that completes MID-SCAN can truncate a snapshot reader's
    // backing row extent (the Arc holds metadata only) — a longer exposure
    // window than Stage-1's up-front materialization had. Same remedy as
    // get/head: on a scan error, if the live sst_readers set changed since
    // the snapshot, redo the WHOLE scan once against the fresh set (range
    // is limit-bounded; one redo on a rare race is cheaper than mid-scan
    // resume bookkeeping). Unchanged set or second failure = real error.
    let mut attempt = 0u32;
    let out = loop {
        attempt += 1;
        let p = part.borrow();
        let mem_items = collect_mem_items(&p);
        // Unfiltered Arc snapshot for change detection (the scan set below
        // is pre-filtered, so it can't be ptr-compared against live state).
        let full_snap: Vec<std::sync::Arc<SstReader>> = p.sst_readers.to_vec();
        // async block-on-demand iteration over the (paged) SSTs — no
        // materialization. Blocks fetch through the global BlockCache
        // (FetchMode::Cached): repeated list calls over the same prefix hit
        // warm blocks, and per-request memory is O(open blocks), not O(SST
        // bytes).
        //
        // coco P2 (kept from Stage-1): pre-filter by SST key range — skip
        // SSTs entirely before the scan start (biggest < seek) and, when a
        // prefix bounds the scan, SSTs entirely after it. smallest/biggest
        // are INTERNAL keys (user ++ inv-seq), so the comparison MUST be
        // `cmp_internal_keys`; seek_key sorts before every real entry of
        // start_user_key, so `biggest < seek_key` is exact.
        let readers_snap: Vec<std::sync::Arc<SstReader>> = p
            .sst_readers
            .iter()
            .filter(|r| {
                if crate::cmp_internal_keys(r.biggest_key(), &seek_key).is_lt() {
                    return false;
                }
                if !req.prefix.is_empty() {
                    let s_user = parse_key(r.smallest_key());
                    if s_user > req.prefix.as_slice() && !s_user.starts_with(&req.prefix) {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();
        let sc_snap = p.stream_client.clone();
        let cache_snap = p.block_cache.clone();
        let now = now_secs();
        let check_overlap = p.has_overlap.get() != 0;
        let part_rg = p.rg.clone();
        drop(p);

        match range_scan_sst_merge(
            &req,
            &seek_key,
            mem_items,
            &readers_snap,
            &sc_snap,
            &cache_snap,
            now,
            check_overlap,
            &part_rg,
        )
        .await
        {
            Ok(entries) => break entries,
            Err(e) => {
                let q = part.borrow();
                let readers_changed = crate::sst_readers_changed(&q.sst_readers, &full_snap);
                drop(q);
                if attempt == 1 && readers_changed {
                    continue;
                }
                return Err((StatusCode::Internal, format!("range sst read: {e}")));
            }
        }
    };

    let has_more = out.len() == req.limit as usize;
    Ok(partition_rpc::rkyv_encode(&RangeResp {
        code: CODE_OK,
        message: String::new(),
        entries: out,
        has_more,
        cur_end_key,
    }))
}


/// + coco P1: one full SST-merge scan pass for `handle_range` — built
/// from fresh snapshots each call so the caller can retry the whole pass
/// when a concurrent compaction invalidates the reader set mid-scan.
/// Mirrors the earlier sync mem/SST 2-way merge exactly; block-read errors
/// propagate (caller decides retry vs Internal).
#[allow(clippy::too_many_arguments)]
async fn range_scan_sst_merge(
    req: &RangeReq,
    seek_key: &[u8],
    mem_items: Vec<IterItem>,
    readers_snap: &[std::sync::Arc<SstReader>],
    sc_snap: &Rc<StreamClient>,
    cache_snap: &std::sync::Arc<crate::sstable::BlockCache>,
    now: u64,
    check_overlap: bool,
    part_rg: &autumn_rpc::manager_rpc::MgrRange,
) -> anyhow::Result<Vec<RangeEntry>> {
    let mut mem_it = MemtableIterator::new(mem_items);
    mem_it.seek(seek_key);

    let mut sst_iters: Vec<AsyncTableIterator> = Vec::with_capacity(readers_snap.len());
    for r in readers_snap.iter().rev() {
        let mut it = AsyncTableIterator::new(
            r.clone(),
            sc_snap.clone(),
            cache_snap.clone(),
            FetchMode::Cached,
        );
        it.seek(seek_key).await?;
        sst_iters.push(it);
    }
    let mut merge = AsyncMergeIterator::new(sst_iters);

    let mut out: Vec<RangeEntry> = Vec::new();
    let mut last_user_key: Option<Vec<u8>> = None;

    loop {
        let mem_key = if mem_it.valid() {
            mem_it.item().map(|i| i.key.as_slice())
        } else {
            None
        };
        let sst_key = if merge.valid() {
            merge.item().map(|i| i.key.as_slice())
        } else {
            None
        };

        let item = match (mem_key, sst_key) {
            (None, None) => break,
            (Some(_), None) => {
                let item = mem_it.item().unwrap().clone();
                mem_it.next();
                item
            }
            (None, Some(_)) => {
                let item = merge.item().unwrap().clone();
                merge.next().await?;
                item
            }
            (Some(mk), Some(sk)) => {
                if crate::cmp_internal_keys(mk, sk).is_le() {
                    let item = mem_it.item().unwrap().clone();
                    let uk_owned = parse_key(mk).to_vec();
                    mem_it.next();
                    while merge.valid() {
                        if let Some(si) = merge.item() {
                            if parse_key(&si.key) == uk_owned.as_slice() {
                                merge.next().await?;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    item
                } else {
                    let item = merge.item().unwrap().clone();
                    let uk_owned = parse_key(sk).to_vec();
                    merge.next().await?;
                    while mem_it.valid() {
                        if let Some(mi) = mem_it.item() {
                            if parse_key(&mi.key) == uk_owned.as_slice() {
                                mem_it.next();
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    item
                }
            }
        };

        let uk = parse_key(&item.key);
        if check_overlap && !in_range(part_rg, uk) {
            continue;
        }
        if !req.prefix.is_empty() && !uk.starts_with(&req.prefix as &[u8]) {
            break;
        }
        if last_user_key.as_deref() == Some(uk) {
            continue;
        }
        last_user_key = Some(uk.to_vec());

        if item.op == 2 {
            continue;
        }
        if item.expires_at > 0 && item.expires_at <= now {
            continue;
        }

        out.push(RangeEntry {
            key: uk.to_vec(),
            value: vec![],
        });
        if out.len() >= req.limit as usize {
            break;
        }
    }

    Ok(out)
}

/// #6: per-attempt timeout for `multi_modify_split` in the split path. SHORT
/// (vs the StreamClient default) so the freeze critical section stays under
/// FREEZE_TTL — a split that COMMITS after the freeze lapsed seals the
/// log_stream at a stale commit_length and loses the writes that resumed
/// post-unfreeze.
const SPLIT_CALL_TIMEOUT: Duration = Duration::from_secs(8);

/// #6: stop launching split attempts once the freeze has been held this long.
/// A call launched at the deadline still completes/commits within
/// `deadline + SPLIT_CALL_TIMEOUT` < FREEZE_TTL (2 s margin), so any in-flight
/// manager commit lands while the partition is STILL frozen — never after the
/// freeze could have lapsed. Pure for unit-testing the budget invariant.
fn split_freeze_deadline(freeze_ttl: Duration, call_timeout: Duration) -> Duration {
    freeze_ttl
        .saturating_sub(call_timeout)
        .saturating_sub(Duration::from_secs(2))
}

/// Render an arbitrary byte string (e.g. a `split --at` key or a partition
/// range bound, which may contain binary bytes) as lowercase hex for error
/// messages. Local helper to avoid pulling the `hex` crate into this crate.
fn hex_str(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
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
    let req: SplitPartReq =
        partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

    if part.borrow().has_overlap.get() != 0 {
        return Err((
            StatusCode::FailedPrecondition,
            "cannot split: partition has overlapping keys".to_string(),
        ));
    }

    // refuse split when this PS's static core budget can't host the
    // right child. The check fires only when `--cpuset` was supplied;
    // deployments without it keep the legacy unlimited behaviour. We reject
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
                    "PS core budget exhausted ({} / {} partitions); split refused. \
                     Operator: grow --cpuset or merge a cold partition first.",
                    budget.current(),
                    budget.max,
                ),
            ));
        }
    }

    // Drain in-flight compaction + GC before commit_length. Two gates,
    // acquired outer→inner:
    //   1. **Per-partition `maintenance_gate`** — serializes vs the merged
    //      `background_maintenance_loop` on THIS partition. It runs compaction
    //      and GC on ONE task and acquires this gate around BOTH, so holding it
    //      means neither a `do_compact` (`compact_row_append` racing the
    //      row_stream seal) NOR a `run_gc` (log_stream append racing the log
    //      seal) is in flight. Unifies the former compact_gate + gc_gate;
    //      the PS-wide `acquire_compact` permit (default max=4) does NOT
    //      serialize same-partition (coco /findbugs v3, 2026-06-02), hence the
    //      dedicated per-partition gate.
    //   2. **PS-wide `concurrency.acquire_compact`** (D-r7) — caps
    //      cross-partition peak RAM. Inner to the gate.
    // Both RAII-held through `multi_modify_split` AND the P-sst barrier
    // ACK below.
    let (maintenance_gate, concurrency) = {
        let p = part.borrow();
        (p.maintenance_gate.clone(), p.concurrency_ctrl.clone())
    };
    let _local_maintenance_gate = maintenance_gate.acquire().await;
    let _compact_permit = concurrency.acquire_compact().await;

    // Fetch authoritative range from the manager. PartitionData.rg is set
    // at open_partition and is NOT refreshed by sync_regions_once for an
    // already-open partition, so after a previous split the local rg
    // still spans the old wide range. Picking mid_key against the stale
    // rg can yield a key outside the manager's narrowed range, which
    // multi_modify_split then rejects.
    let auth_rg: Range = {
        // 10 s — read-only manager call. Fetches authoritative range
        // for the stale-rg fix. Bounded so split doesn't wedge
        // on a hung manager.
        let resp_bytes = pool
            .call_timeout(
                manager_addr,
                manager_rpc::MSG_GET_REGIONS,
                Bytes::new(),
                Duration::from_secs(10),
            )
            .await
            .map_err(|e| (StatusCode::Internal, format!("get_regions: {e}")))?;
        let resp: manager_rpc::GetRegionsResp =
            manager_rpc::rkyv_decode(&resp_bytes).map_err(|e| (StatusCode::Internal, e))?;
        if resp.code != manager_rpc::CODE_OK {
            return Err((
                StatusCode::Internal,
                format!("get_regions: {}", resp.message),
            ));
        }
        resp.regions
            .into_iter()
            .find(|(pid, _)| *pid == req.part_id)
            .and_then(|(_, info)| info.rg)
            .ok_or_else(|| {
                (
                    StatusCode::NotFound,
                    format!("partition {} not in manager regions", req.part_id),
                )
            })?
    };

    // (design doc D4): the split point comes from one of two
    // sources.
    //   * EXPLICIT (`req.at_key = Some`): an operator/controller names the
    //     point. We validate it lies STRICTLY inside the authoritative
    //     `(start_key, end_key)` and use it verbatim — SKIPPING the (paged) SST
    //     key scan AND the `>= 2 keys` gate. This is what lets an empty / near-
    //     empty partition be presplit (cut into two empty children; D8 relies
    //     on it). The PS stays app-agnostic (D5): `at_key` is an arbitrary byte
    //     string, no namespace/prefix awareness.
    //   * IMPLICIT (`req.at_key = None`, legacy): median-by-key-count over the
    //     live in-range user keys, still gated at `>= 2 keys`.
    let mid = if let Some(at_key) = req.at_key.clone() {
        // In-range check is STRICT on both ends:
        //   at_key <= start_key ⇒ empty/backwards left child ⇒ reject.
        //   at_key >= end_key   ⇒ empty right child / out of range ⇒ reject
        //                         (end_key is the exclusive upper bound; an
        //                          empty end_key means +∞, so only the low
        //                          bound applies).
        // `in_range(rg, k)` accepts `k == start_key` (it is the range's own
        // lower bound), which is NOT a valid split point, so we check the
        // bounds explicitly here rather than reusing it.
        // NOTE: this validation runs BEFORE the freeze block below, so there is
        // no `frozen_for_split` to clear on the reject path; the RAII
        // maintenance_gate / compact_permit drop on return.
        if at_key.as_slice() <= auth_rg.start_key.as_slice() {
            return Err((
                StatusCode::InvalidArgument,
                format!(
                    "split --at key {} is at or below partition start {} \
                     (must be strictly inside [start, end))",
                    hex_str(&at_key),
                    hex_str(&auth_rg.start_key),
                ),
            ));
        }
        if !auth_rg.end_key.is_empty() && at_key.as_slice() >= auth_rg.end_key.as_slice() {
            return Err((
                StatusCode::InvalidArgument,
                format!(
                    "split --at key {} is at or above partition end {} \
                     (must be strictly inside [start, end))",
                    hex_str(&at_key),
                    hex_str(&auth_rg.end_key),
                ),
            ));
        }
        at_key
    } else {
        // async window scan over the (paged) SSTs — snapshot readers +
        // sc under one brief borrow, drop, await (note 15). Reader set is
        // stable: this path holds maintenance_gate.
        let (uuk_readers, uuk_sc, uuk_cache) = {
            let p = part.borrow();
            (
                p.sst_readers.to_vec(),
                p.stream_client.clone(),
                p.block_cache.clone(),
            )
        };
        let sst_seen = sst_user_key_versions(&uuk_readers, &uuk_sc, &uuk_cache)
            .await
            .map_err(|e| (StatusCode::Internal, format!("split key scan: {e}")))?;
        // coco P2: sample the memtable AFTER the (long) SST scan, so
        // writes that landed during it still count toward the `< 2 keys`
        // check and the midpoint choice.
        let uuk_mem_items = collect_mem_items(&part.borrow());
        let user_keys = finalize_unique_user_keys(&uuk_mem_items, sst_seen)
            .into_iter()
            .filter(|k| in_range(&auth_rg, k))
            .collect::<Vec<_>>();
        if user_keys.len() < 2 {
            return Err((
                StatusCode::FailedPrecondition,
                format!(
                    "part has fewer than 2 in-range keys (have {}; run major compaction \
                     first, or pass an explicit split point via `split --at`)",
                    user_keys.len()
                ),
            ));
        }

        user_keys[user_keys.len() / 2].clone()
    };
    let (log_stream_id, row_stream_id, meta_stream_id) = {
        let p = part.borrow();
        (p.log_stream_id, p.row_stream_id, p.meta_stream_id)
    };

    // PrepareSplit-style freeze + drain. Split formerly called
    // `flush_memtable_locked(part)` directly then `commit_length` — but
    // partition_loop could still process in-flight Phase 2 writes
    // (their work is on stream_worker_loop, independent of split's stack)
    // during the await window. Those writes' bytes landed on EN past the
    // captured commit_length, then manager sealed at the captured value,
    // leaving the trailing bytes invisible on recovery. The fix mirrors
    // the merge freeze: set frozen_for_split → halt new Put/Delete via
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
        // fix — wake partition_loop so its idle-path
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
    //
    // **Failure MUST abort the split, NOT default to 1.** Pre-fix
    // this swallowed errors with `unwrap_or(0).max(1)`. When one replica
    // is unreachable (e.g. concurrent fence + recovery dispatch),
    // the all-replica `commit_length` rightly returns `Err`. The old
    // `unwrap_or(0).max(1)` masked that as "sealed at byte 1", so the
    // manager sealed the tail extent at byte 1; the right-child opened
    // post-split with a 1-byte-sealed tail and lost every log_stream
    // write past byte 1 of that extent (chaos test split+fence repro
    // surfaced this as ~5–13 q* keys reverting to seq numbers from
    // hundreds of writes ago — the exact bytes that were in the tail
    // extent at fence time).
    //
    // Returning `FailedPrecondition` lets the client retry the split
    // once the cluster is healthy enough that all-replica commit_length
    // succeeds. The split path already unfreezes on the error return below, so
    // the partition resumes serving writes during the retry gap.
    let unfreeze_on_err = |e: anyhow::Error, what: &str| {
        part.borrow().frozen_for_split.set(None);
        (
            StatusCode::FailedPrecondition,
            format!("split aborted: {what} commit_length failed: {e}"),
        )
    };
    // commit==0 → seal at sealed_length=0, NOT 1. A frozen tail whose
    // all-replica commit is 0 (empty — e.g. a tail that just rolled and took
    // no writes) MUST seal empty: `compute_duplicate_stream` seals an empty
    // CoW tail as `sealed=true, sealed_length=0` (recoverable — each child
    // allocs a fresh tail; manager note 32). The OLD `.max(1)` over-sealed an
    // empty tail at byte 1 → on the child's cold reopen the WAL replay reaches
    // it, expects 1 byte, finds 0 → WAL-FAILSTOP → child un-openable. Same root
    // cause + fix as the merge over-seal (commit 80f29aa, handle_merge_partitions
    // dropped the same `.max(1)`); the empty-log-tail-at-split window is narrow
    // so this variant wasn't independently reproduced, but the mechanism is
    // identical and compute_duplicate_stream is symmetric to compute_merge_streams.
    // The `?` above still aborts on a genuine commit_length Err (unreachable
    // replica, the hazard documented above), so OK+0 = genuinely empty,
    // never a masked failure → safe to seal at 0.
    // Capture each commit length TOGETHER WITH the tail extent it was
    // measured on; the manager refuses the commit if any tail moved in
    // between (e.g. a fence-drain roll already in flight when the freeze
    // began) — sealing the roll's fresh empty tail at this captured length
    // would wedge the CoW child permanently.
    let (log_end, log_tail_eid) = part_sc
        .commit_length_with_tail(log_stream_id)
        .await
        .map_err(|e| unfreeze_on_err(e, "log_stream"))?;
    let (row_end, row_tail_eid) = part_sc
        .commit_length_with_tail(row_stream_id)
        .await
        .map_err(|e| unfreeze_on_err(e, "row_stream"))?;
    let (meta_end, meta_tail_eid) = part_sc
        .commit_length_with_tail(meta_stream_id)
        .await
        .map_err(|e| unfreeze_on_err(e, "meta_stream"))?;
    {
        let p = part.borrow();
        tracing::info!(
            target: "split_trace",
            part_id = req.part_id,
            log_stream_id,
            row_stream_id,
            meta_stream_id,
            log_end,
            row_end,
            meta_end,
            vp_extent_id = p.vp_extent_id,
            vp_offset = p.vp_offset,
            freeze_elapsed_ms = p
                .frozen_for_split
                .get()
                .map(|t| t.elapsed().as_millis() as u64)
                .unwrap_or(u64::MAX),
            "split captured commit lengths"
        );
    }

    // synchronous P-log → P-sst barrier. Sent BEFORE
    // `multi_modify_split` so that any failure here is cleanly abortable:
    // the manager has not yet sealed the row_stream tail, so unfreezing
    // and returning Err leaves the cluster in a coherent pre-split state.
    // Putting the barrier AFTER `multi_modify_split` would create an
    // unrecoverable window — manager-committed seal + local state not
    // converged + freeze cleared (per coco /findbugs 2026-06-02 v2 review).
    //
    // The barrier itself: P-sst drains its in-flight FuturesUnordered to
    // zero and calls `sst_sc.invalidate_stream(row_stream_id)` so the
    // cached per-stream worker is discarded. Any future P-sst op (after
    // gates release) re-fetches a fresh tail — by then `multi_modify_split`
    // will have sealed the old tail and the manager will return the
    // post-seal extent. At the moment of THIS send, gates are still held +
    // `frozen_for_split` halts new writes, so P-sst's queue is normally
    // empty (the priority-biased select in `flush_worker_loop` keeps the
    // ordering race-free defensively).
    //
    // Before the current barrier (and the earlier lazy-flag attempt) this was a
    // `Cell<bool> need_invalidate_row_stream` flag piggybacked on each
    // P-sst message — racy under P-sst's cap=2 FuturesUnordered (see
    // fix history in `partition-server/CLAUDE.md` programming note 16).
    let (inv_resp_tx, inv_resp_rx) = futures::channel::oneshot::channel::<()>();
    let mut inv_tx = part.borrow().row_invalidate_tx.clone();
    let inv_req = crate::RowInvalidateBarrierReq {
        row_stream_id,
        seal_and_roll: false, // split: manager seals the tail itself; only invalidate here
        resp_tx: inv_resp_tx,
    };
    // BOTH the send and the ACK await are bounded. `FREEZE_TTL`
    // (30 s, lib.rs) is the partition's unconditional "the handler is
    // wedged, unfreeze and resume writes" backstop (see
    // `check_freeze_ttls`). If EITHER step were unbounded, a wedged
    // P-sst could block the await past the TTL; `check_freeze_ttls`
    // would unfreeze, new writes would resume + extend the row_stream
    // tail past the already-captured `commit_length`, and the eventual
    // continuation would call `multi_modify_split` with the STALE
    // `row_end` — manager seals at the pre-TTL length, post-TTL writes
    // end up above sealed_length, invisible on recovery (coco /findbugs
    // v4/v5, 2026-06-02). The SEND can block independently of the ACK:
    // `row_invalidate_tx` is capacity 1 (only `handle_split_part`
    // sends), so a still-queued prior-split barrier whose P-sst
    // processing never completed (e.g. permanently-down replica on
    // flush) would back-pressure us here BEFORE the ACK timeout could
    // even arm. Two separate timers (5 s + 10 s) keep the total budget
    // (15 s) safely under FREEZE_TTL while still leaving ample
    // happy-path headroom (when nothing's wedged the send is
    // microseconds and the ACK lands on the next P-sst poll).
    let send_timeout = std::time::Duration::from_secs(5);
    let ack_timeout = std::time::Duration::from_secs(10);
    {
        let send_fut = inv_tx.send(inv_req);
        let send_timer = compio::time::sleep(send_timeout);
        futures::pin_mut!(send_fut);
        futures::pin_mut!(send_timer);
        match futures::future::select(send_fut, send_timer).await {
            futures::future::Either::Left((Ok(()), _)) => {}
            futures::future::Either::Left((Err(_), _)) => {
                part.borrow().frozen_for_split.set(None);
                return Err((
                    StatusCode::Internal,
                    "split: P-sst row_invalidate channel closed".to_string(),
                ));
            }
            futures::future::Either::Right(_elapsed) => {
                part.borrow().frozen_for_split.set(None);
                return Err((
                    StatusCode::FailedPrecondition,
                    format!(
                        "split: P-sst row_invalidate send timed out after {}s (channel full \
                         from prior wedged split); client may retry",
                        send_timeout.as_secs()
                    ),
                ));
            }
        }
    }
    let ack_timer = compio::time::sleep(ack_timeout);
    futures::pin_mut!(ack_timer);
    match futures::future::select(inv_resp_rx, ack_timer).await {
        futures::future::Either::Left((Ok(()), _)) => {}
        futures::future::Either::Left((Err(_canceled), _)) => {
            part.borrow().frozen_for_split.set(None);
            return Err((
                StatusCode::Internal,
                "split: P-sst row_invalidate ACK dropped (bulk thread aborted)".to_string(),
            ));
        }
        futures::future::Either::Right(_elapsed) => {
            part.borrow().frozen_for_split.set(None);
            return Err((
                StatusCode::FailedPrecondition,
                format!(
                    "split: P-sst row_invalidate barrier ACK timeout after {}s (bulk thread \
                     wedged); client may retry",
                    ack_timeout.as_secs()
                ),
            ));
        }
    }

    // TEST SYNC-POINT: hold the split here (captures + barrier done, manager
    // commit not yet issued) so a test can land a concurrent tail roll inside
    // this window. Off in production (one relaxed load; only
    // `set_split_commit_pause` sets it).
    if crate::split_commit_paused() {
        crate::note_split_commit_parked();
        while crate::split_commit_paused() {
            compio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    // Call multi_modify_split on the manager. #6: the freeze only guarantees a
    // STABLE commit_length while it is HELD. If `check_freeze_ttls` auto-
    // unfreezes (this handler exceeded FREEZE_TTL) and a split then commits, it
    // seals the log_stream at the now-stale `log_end` while writes have resumed
    // past it → those writes land above sealed_length and are LOST on recovery.
    // Pre-#6 this looped `for _ in 0..8` with a 30 s per-call timeout (up to ~4
    // min), trivially blowing past the 30 s TTL. Now: SHORT per-call timeout and
    // STOP launching once less than one call's budget remains before the TTL, so
    // any in-flight commit still lands while frozen. Out of budget → ABORT
    // cleanly (unfreeze + Err); the client retries the whole split when the
    // cluster is healthier. We NEVER let a split SUCCEED after the freeze could
    // have lapsed.
    let freeze_at = match part.borrow().frozen_for_split.get() {
        Some(t) => t,
        None => {
            // check_freeze_ttls already fired (or someone cleared it) before we
            // reached the commit phase — abort; the captured commit_length is no
            // longer protected.
            return Err((
                StatusCode::FailedPrecondition,
                "split: freeze lapsed before commit phase; retry".to_string(),
            ));
        }
    };
    let split_deadline = split_freeze_deadline(crate::FREEZE_TTL, SPLIT_CALL_TIMEOUT);
    let mut split_ok = false;
    let mut split_err =
        "split: ran out of freeze budget before multi_modify_split succeeded".to_string();
    let mut backoff = Duration::from_millis(100);
    loop {
        if freeze_at.elapsed() >= split_deadline {
            break; // out of freeze budget → abort below (no stale-seal commit)
        }
        match part_sc
            .multi_modify_split(
                mid.clone(),
                req.part_id,
                [log_end, row_end, meta_end],
                [log_tail_eid, row_tail_eid, meta_tail_eid],
                SPLIT_CALL_TIMEOUT,
            )
            .await
        {
            Ok(()) => {
                split_ok = true;
                break;
            }
            Err(err) => {
                tracing::info!(
                    target: "split_trace",
                    part_id = req.part_id,
                    freeze_elapsed_ms = freeze_at.elapsed().as_millis() as u64,
                    error = %err,
                    "split multi_modify_split attempt failed"
                );
                split_err = format!("{err:#}");
                // "captured tail moved" is DETERMINISTIC for these captured
                // values — every retry re-sends the same stale capture and
                // gets the same refusal. Abort now; the client retries the
                // whole split, whose fresh capture sees the rolled tails.
                if split_err.contains("split captured tail moved") {
                    break;
                }
                compio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2).min(Duration::from_secs(2));
            }
        }
    }
    tracing::info!(
        target: "split_trace",
        part_id = req.part_id,
        split_ok,
        freeze_elapsed_ms = freeze_at.elapsed().as_millis() as u64,
        "split commit phase done"
    );

    if !split_ok {
        // unfreeze on multi_modify_split failure so the partition
        // resumes serving writes (client will retry split). Without this
        // the partition stays frozen until FREEZE_TTL backstop fires.
        part.borrow().frozen_for_split.set(None);
        return Err((StatusCode::FailedPrecondition, split_err));
    }

    // #6 belt-and-braces behind the deadline-bound prevention: re-verify the
    // freeze HELD continuously through the commit. If `check_freeze_ttls`
    // cleared it anyway, the captured commit_length may be stale + writes
    // resumed → do NOT apply the split locally; surface loudly. region_sync
    // reconciles the manager's committed split; the client retries.
    if part.borrow().frozen_for_split.get().is_none() {
        return Err((
            StatusCode::FailedPrecondition,
            "split: freeze TTL lapsed during multi_modify_split — possible stale seal; \
             aborting local apply, retry"
                .to_string(),
        ));
    }
    // #6 VERDICT (2026-06-15, /loop — falsified): the stale-seal SILENT write-loss is
    // STRUCTURALLY PRECLUDED, not merely unreproduced. The loss requires the
    // manager's `multi_modify_split` etcd commit to LAND (succeed) at a wall-
    // clock time AFTER this PS auto-unfroze (>= split_deadline = 20 s) and acked
    // writes past the captured commit_length. But between the PS capturing
    // commit_length (the call's send) and the manager's commit,
    // `handle_multi_modify_split` has ONLY bounded awaits, each with a kill-
    // timeout that turns slowness into FAILURE (Err -> no commit -> no stale
    // seal), never late success: the main put_msgs_txn (etcd
    // request_timeout 10 s); Phase-1 compute before it is fully synchronous.
    // No code path sleeps there — the only way to land a SUCCESSFUL commit after
    // the freeze window is the TEMP /tmp/autumn_repro6 sleep (now removed), which
    // models a stall that no real component can both incur AND survive (a real
    // stall = timeout = Err = abort). The 2026-06-15 reproduction attempt instead
    // surfaced a DIFFERENT real bug — a PS retry storm against a slow manager
    // committing a SEPARATE split per retry (1->6 CASCADE) — now fixed MANAGER-
    // side by the per-partition split-inflight guard. Residual tail (documented,
    // NOT fixed): if all three bounded awaits sit at ~9 s and ALL succeed, the
    // commit could land at ~27 s > 20 s — three consecutive near-timeout-but-
    // healthy ops, not naturally reproducible; the only true close (manager re-
    // validate freeze/commit right before the commit) is revert-prone hot-split
    // mechanism and is deferred per reproduce-first. The freeze-held re-check
    // above stays as the belt-and-braces detector for the case the PS DOES
    // receive the Ok.

    // The manager sealed all 3 stream tails as part of the split. The
    // P-log stream workers still cache the old (now-sealed) tails and
    // would keep appending beyond sealed_length. On recovery,
    // read_last_extent_data only reads up to sealed_length, so any
    // post-split checkpoint/SST appended beyond that point is invisible
    // — causing "invalid meta_len" or missing SSTs.
    part_sc.invalidate_stream(log_stream_id);
    part_sc.invalidate_stream(row_stream_id);
    part_sc.invalidate_stream(meta_stream_id);
    // (The P-sst row_invalidate barrier was already done BEFORE
    // multi_modify_split above; see commentary at the barrier send site.)

    // Narrow PS-local rg to match the manager's new left range and
    // re-evaluate has_overlap against the SSTables. Without this,
    // sync_regions_once would leave the partition with a stale wide rg
    // and a stale has_overlap=0, perpetuating the bug above.
    //
    // fix: bump `region_epoch` in lock-step with the manager's
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
        let new_rg = Range {
            start_key: auth_rg.start_key.clone(),
            end_key: mid.clone(),
        };
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

    // fix-2: publish the new (rg, log, row, meta, region_epoch)
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

    // unfreeze on success — split commit landed; the LEFT
    // (this partition's) post-split rg is now in effect, and merged
    // commit_length matches the manager's sealed_length. Writes can
    // resume against the narrower range.
    part.borrow().frozen_for_split.set(None);

    Ok(partition_rpc::rkyv_encode(&SplitPartResp {
        code: CODE_OK,
        message: String::new(),
    }))
}

pub(crate) async fn handle_maintenance(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
    part_sc: &Rc<StreamClient>,
) -> HandlerResult {
    let req: MaintenanceReq =
        partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
    if req.op == MAINTENANCE_FORCE_GC {
        // #3: enqueue the Force GC AND return an advisory so the
        // operator learns SYNCHRONOUSLY (not by grepping the PS log) that a
        // requested extent sits inside the recovery replay window and will be
        // PROTECTED (correct, not a bug). The background loop recomputes the
        // floor authoritatively; this is a best-effort preview over the SAME
        // helper (`gc_replay_floor`). Task is still enqueued regardless.
        let (gc_tx, log_stream_id, sst_vp_eids) = {
            let p = part.borrow();
            (
                p.gc_tx.clone(),
                p.log_stream_id,
                p.sst_readers
                    .iter()
                    .map(|r| r.vp_extent_id)
                    .collect::<Vec<_>>(),
            )
        };
        let advisory = match part_sc.get_stream_info(log_stream_id).await {
            Ok(si) => {
                let ids = si.extent_ids;
                let (floor_pos, pos_by_eid) =
                    crate::background::gc_replay_floor(&ids, sst_vp_eids.iter().copied());
                let floor_eid = ids.get(floor_pos).copied().unwrap_or(0);
                // A requested extent that resolves at/after the floor position is
                // protected IF non-empty (matches `gc_extent_punchable`). We don't
                // probe sealed_length here, so qualify with "non-empty".
                let protected: Vec<u64> = req
                    .extent_ids
                    .iter()
                    .copied()
                    .filter(|e| pos_by_eid.get(e).is_some_and(|&pos| pos >= floor_pos))
                    .collect();
                if protected.is_empty() {
                    String::new()
                } else {
                    format!(
                        "advisory: extent(s) {protected:?} resolve AT/BEFORE the recovery replay \
                         floor (extent {floor_eid}, pos {floor_pos}); if non-empty they will be \
                         PROTECTED by GC (recovery replays from there) — expected, not a bug. \
                         Advance the floor: flush + MAJOR-compact past extent {floor_eid}, then retry."
                    )
                }
            }
            Err(_) => String::new(),
        };
        let mut gc_tx = gc_tx;
        let (code, message) = match gc_tx.try_send(GcTask::Force {
            extent_ids: req.extent_ids,
            op_id: req.op_id,
        }) {
            Ok(()) => (CODE_OK, advisory),
            Err(_) => (CODE_ERROR, "gc busy".to_string()),
        };
        return Ok(partition_rpc::rkyv_encode(&MaintenanceResp { code, message }));
    }
    if req.op == MAINTENANCE_FLUSH {
        // Synchronous flush: rotate active memtable and flush all immutables.
        flush_memtable_locked(part)
            .await
            .map_err(|e| (StatusCode::Internal, e.to_string()))?;
        return Ok(partition_rpc::rkyv_encode(&MaintenanceResp {
            code: CODE_OK,
            message: String::new(),
        }));
    }
    let mut p = part.borrow_mut();
    let result = match req.op {
        MAINTENANCE_COMPACT => p
            .compact_tx
            .try_send(crate::CompactTask { is_major: true, op_id: req.op_id })
            .map_err(|_| "compaction busy"),
        MAINTENANCE_AUTO_GC => {
            // decode multi-tier filter params from wire request.
            let params = crate::GcAutoParams {
                ratio: req.gc_ratio,
                max_size: req.gc_max_size,
                stream_debt: req.gc_stream_debt,
                empty_only: req.gc_empty_only,
            };
            p.gc_tx
                .try_send(GcTask::Auto { params, op_id: req.op_id })
                .map_err(|_| "gc busy")
        }
        // MAINTENANCE_FORCE_GC handled above (advisory preview path).
        _ => Err("unknown op"),
    };
    match result {
        Ok(()) => Ok(partition_rpc::rkyv_encode(&MaintenanceResp {
            code: CODE_OK,
            message: String::new(),
        })),
        Err(e) => Ok(partition_rpc::rkyv_encode(&MaintenanceResp {
            code: CODE_ERROR,
            message: e.to_string(),
        })),
    }
}

/// #2: snapshot the per-partition GC replay floor + per-SST
/// vp_heads so `autumn-op info --part` can show WHY a `forcegc` on a given
/// extent would be protected (correct, not a bug). Read-only; borrows briefly,
/// then does one `get_stream_info` to resolve the log extent order + floor.
pub(crate) async fn handle_diag_partition_vp(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
    part_sc: &Rc<StreamClient>,
) -> HandlerResult {
    let _req: partition_rpc::DiagPartitionVpReq =
        partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
    let (log_stream_id, sst_vp_heads, vp_seed_extent_id, vp_seed_offset) = {
        let p = part.borrow();
        let heads: Vec<(u64, u64)> = p
            .sst_readers
            .iter()
            .map(|r| (r.vp_extent_id, r.vp_offset))
            .collect();
        (p.log_stream_id, heads, p.vp_extent_id, p.vp_offset)
    };
    let log_extent_ids = match part_sc.get_stream_info(log_stream_id).await {
        Ok(s) => s.extent_ids,
        Err(e) => {
            return Ok(partition_rpc::rkyv_encode(
                &partition_rpc::DiagPartitionVpResp {
                    code: CODE_ERROR,
                    message: format!("get_stream_info: {e}"),
                    log_extent_ids: Vec::new(),
                    sst_vp_heads,
                    floor_pos: 0,
                    floor_extent_id: 0,
                    vp_seed_extent_id,
                    vp_seed_offset,
                },
            ));
        }
    };
    let (floor_pos, _pos_by_eid) = crate::background::gc_replay_floor(
        &log_extent_ids,
        sst_vp_heads.iter().map(|(eid, _)| *eid),
    );
    let floor_extent_id = log_extent_ids.get(floor_pos).copied().unwrap_or(0);
    Ok(partition_rpc::rkyv_encode(
        &partition_rpc::DiagPartitionVpResp {
            code: CODE_OK,
            message: String::new(),
            log_extent_ids,
            sst_vp_heads,
            floor_pos: floor_pos as u64,
            floor_extent_id,
            vp_seed_extent_id,
            vp_seed_offset,
        },
    ))
}

pub(crate) async fn handle_diag_trace_key(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
) -> HandlerResult {
    let req: partition_rpc::DiagTraceKeyReq =
        partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
    let p = part.borrow();
    let memtable_seq = p.active.seek_user_key_seq(&req.user_key);
    let imm_seqs: Vec<u64> = p
        .imm
        .iter()
        .map(|imm| imm.seek_user_key_seq(&req.user_key))
        .collect();
    let mut sst_seqs: Vec<u64> = Vec::with_capacity(p.sst_readers.len());
    let mut sst_seqs_nobloom: Vec<u64> = Vec::with_capacity(p.sst_readers.len());
    let mut sst_seqs_fullscan: Vec<u64> = Vec::with_capacity(p.sst_readers.len());
    let mut sst_last_seqs: Vec<u64> = Vec::with_capacity(p.tables.len());
    for (i, reader) in p.sst_readers.iter().enumerate() {
        sst_seqs.push(crate::background::lookup_in_sst_seq_opt(
            reader,
            &req.user_key,
            true,
        ));
        sst_seqs_nobloom.push(crate::background::lookup_in_sst_seq_opt(
            reader,
            &req.user_key,
            false,
        ));
        sst_seqs_fullscan.push(crate::background::lookup_in_sst_seq_fullscan(
            reader,
            &req.user_key,
        ));
        sst_last_seqs.push(p.tables.get(i).map(|t| t.last_seq).unwrap_or(0));
    }
    Ok(partition_rpc::rkyv_encode(
        &partition_rpc::DiagTraceKeyResp {
            code: partition_rpc::CODE_OK,
            message: String::new(),
            memtable_seq,
            imm_seqs,
            sst_seqs,
            sst_seqs_nobloom,
            sst_seqs_fullscan,
            sst_last_seqs,
        },
    ))
}

pub(crate) async fn handle_get_discards(
    payload: Bytes,
    part: &Rc<RefCell<PartitionData>>,
    part_sc: &Rc<StreamClient>,
) -> HandlerResult {
    let _req: GetDiscardsReq =
        partition_rpc::rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

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
// `map_storage_error` translation tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod split_freeze_budget_tests {
    use super::{split_freeze_deadline, SPLIT_CALL_TIMEOUT};
    use std::time::Duration;

    /// #6 invariant: a split attempt launched at the deadline must finish
    /// (commit on the manager) BEFORE FREEZE_TTL fires, so any in-flight commit
    /// lands while the partition is still frozen. If this fails, a stale-seal
    /// write-loss window has reopened (someone shrank FREEZE_TTL or grew the
    /// per-call timeout past the budget).
    #[test]
    fn deadline_leaves_room_for_one_call_before_ttl() {
        let ttl = crate::FREEZE_TTL;
        let deadline = split_freeze_deadline(ttl, SPLIT_CALL_TIMEOUT);
        assert!(
            deadline + SPLIT_CALL_TIMEOUT < ttl,
            "a call launched at the deadline ({deadline:?}) + its timeout \
             ({SPLIT_CALL_TIMEOUT:?}) must complete before FREEZE_TTL ({ttl:?})"
        );
        assert!(deadline > Duration::ZERO, "deadline must leave time to attempt at all");
    }

    /// Degenerate config (call timeout >= TTL) collapses the deadline to zero —
    /// the loop never attempts (aborts immediately), never commits-after-lapse.
    #[test]
    fn over_budget_config_yields_zero_deadline() {
        assert_eq!(
            split_freeze_deadline(Duration::from_secs(5), Duration::from_secs(8)),
            Duration::ZERO
        );
    }
}

#[cfg(test)]
mod map_storage_error_tests {
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

#[cfg(test)]
mod gc_pin_read_outcome_tests {
    use crate::{acquire_reader_pin, try_acquire_writer_pin};
    use std::rc::Rc;
    use std::sync::atomic::AtomicI64;

    /// The read path asks for a reader pin before resolving a ValuePointer, and
    /// gets `None` exactly while GC holds the writer pin. What it does with that
    /// `None` is the whole question: answering NotFound tells the client the key
    /// does not exist, which is terminal and wrong — GC relocates every live
    /// value before punching, so a live key is either still in place or already
    /// rewritten. `get_value_inner` now returns Unavailable, which the SDK maps
    /// to a retryable error.
    ///
    /// This pins the precondition that makes that branch reachable at all: a
    /// held writer pin denies readers, and releasing it lets them back in. If
    /// this ever stops holding, the Unavailable branch is dead code and the
    /// false-NotFound it replaced could quietly return.
    #[test]
    fn a_held_writer_pin_denies_readers_until_released() {
        let pin = Rc::new(AtomicI64::new(0));
        assert!(
            acquire_reader_pin(pin.clone()).is_some(),
            "an idle extent must serve reads"
        );

        assert!(try_acquire_writer_pin(&pin), "GC takes the writer pin");
        assert!(
            acquire_reader_pin(pin.clone()).is_none(),
            "while GC holds it the read path gets None — the branch that used to \
             answer a false NotFound"
        );

        // Release the way GC does, then readers must be admitted again: this is
        // why Unavailable-and-retry is the correct answer rather than a
        // terminal miss.
        pin.store(0, std::sync::atomic::Ordering::SeqCst);
        assert!(
            acquire_reader_pin(pin).is_some(),
            "after the punch completes the retry must succeed"
        );
    }
}
