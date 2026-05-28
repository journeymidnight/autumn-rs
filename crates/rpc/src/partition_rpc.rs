//! Wire codec for PartitionKv RPCs over autumn-rpc.
//!
//! All 8 PartitionKv RPCs use rkyv serialization.
//! Message type constants are in the 0x40–0x4F range.
//! Every request includes `part_id` for thread-per-partition routing.
//!
//! ## `region_epoch` (TiKV-style)
//!
//! Hot-path data RPCs (Put/Get/Delete/Head/Range/StreamPut) carry a
//! `region_epoch: u64` field. The client stamps the epoch it has
//! cached for the partition; the PS rejects the request with
//! `StatusCode::FailedPrecondition` when the stamp doesn't match its
//! current `region_epoch`, surfaced from `MgrRegionInfo.region_epoch`.
//! The manager bumps the epoch on every `rg` rewrite (split / merge).
//! `0` = "skip check" (bootstrap, tests, legacy callers).
//!
//! `RangeResp` additionally carries `cur_end_key: Vec<u8>` — the PS's
//! authoritative `rg.end_key`. The SDK uses it as a CockroachDB-style
//! ResumeSpan cursor so a split that happens DURING a multi-partition
//! scan auto-resolves on the next `resolve_key`. See
//! `crates/client/CLAUDE.md` for the SDK-side loop.

pub use crate::manager_rpc::{rkyv_decode, rkyv_encode};

use rkyv::{Archive, Deserialize, Serialize};

// ── msg_type constants ───────────────────────────────────────────────────────

pub const MSG_PUT: u8 = 0x40;
pub const MSG_GET: u8 = 0x41;
pub const MSG_DELETE: u8 = 0x42;
pub const MSG_HEAD: u8 = 0x43;
pub const MSG_RANGE: u8 = 0x44;
pub const MSG_SPLIT_PART: u8 = 0x45;
pub const MSG_STREAM_PUT: u8 = 0x46;
pub const MSG_MAINTENANCE: u8 = 0x47;
pub const MSG_GET_DISCARDS: u8 = 0x48;

// F129/F186 — server-side multipart upload was REMOVED in F186.
// Wire constants 0x49-0x4C remain RESERVED to prevent accidental re-use
// while old binaries with handlers may still be in flight in production
// rolling deploys. Stripe-write is now pure client-side (Ceph
// striperados pattern): each chunk is a normal Put under a reserved
// 0xff-prefixed key namespace, and the user key holds a 29-byte Meta
// blob. See `crates/client/src/lib.rs::PutStreamHandle` + `GetStream`.
//
//   pub const MSG_PUT_BEGIN:  u8 = 0x49;  // RESERVED, was F129
//   pub const MSG_PUT_CHUNK:  u8 = 0x4A;  // RESERVED, was F129
//   pub const MSG_PUT_COMMIT: u8 = 0x4B;  // RESERVED, was F129
//   pub const MSG_PUT_ABORT:  u8 = 0x4C;  // RESERVED, was F129

// F183: partition merge — sent to the SURVIVOR's PS.
pub const MSG_MERGE_PART: u8 = 0x4D;

// F185: PrepareMerge-style freeze. CLI/manager sends to BOTH the survivor and
// victim's PS BEFORE capturing commit_length so writes that would otherwise
// fall in the FLUSH→manager.commit window are halted at the source.
//
// `freeze=true`  → drain pending + inflight + flush all imm, set
//                  PartitionData.frozen_for_merge=true, ack.
//                  Subsequent Put/Delete/StreamPut return CODE_UNAVAILABLE
//                  until either an unfreeze RPC clears the flag OR the
//                  partition is reopened by region_sync_loop on rg/stream-id
//                  change (post-merge).
// `freeze=false` → reverse the flag (used by the CLI failure rollback path
//                  if the manager-side merge txn rejects the request).
pub const MSG_MERGE_FREEZE: u8 = 0x4E;

// F210-C4: manager → PS RPC. Manager pulls the partition's current
// vp_refs snapshot from PS so manager-side merge/split orchestration
// (`handle_multi_modify_split`, `handle_merge_partitions`) operates on
// a fresh view of `vp_table_refs` instead of trusting the cached
// snapshot (which may be stale if a previous PS-initiated sync
// failed). PS responds with the current `vp_deps` of every live SST,
// computed under a single `borrow()`. Manager applies via the same
// `apply_partition_vp_refs` path that handles regular
// MSG_SYNC_PARTITION_VP_REFS. If PS rejects (CODE_NOT_FOUND for
// not-owning the partition, CODE_PRECONDITION for ongoing
// open/recovery), manager aborts the merge/split with FailedPrecondition.
pub const MSG_PULL_VP_REFS: u8 = 0x4F;

// F216 zero-copy GET. Same request shape as MSG_GET (GetReq), but the response
// is value-separable for recv-into-registered-dest: a CRC-less frame whose
// payload is `[ZC meta: code(1)+value_len(4)+reserved(4)][raw value]` (see
// autumn_rpc::client::ZC_META_LEN; the reserved field held a value crc32c
// before F219 removed it). The client uses RpcClient::call_into_dest to
// land the value straight in its registered buffer (sglang page). Generic
// MSG_GET keeps the rkyv GetResp form.
pub const MSG_GET_ZC: u8 = 0x50;

// F216-E zero-copy PUT (client -> PS write hop). Same semantics as MSG_PUT but
// value-separable so the client sends the value as its OWN iovec straight from
// the (registered) sglang source pool — no `value.to_vec()`/clone/rkyv copy on
// the client, and the PS slices the value zero-copy out of the frame (vs an
// rkyv decode copy). Wire payload:
//   [part_id: u64 LE][region_epoch: u64 LE][expires_at: u64 LE][key_len: u32 LE]
//   [key: key_len bytes][value: rest]
// Sent via RpcClient::call_vectored(MSG_PUT_ZC, [meta, value]) — on UCX the
// value iovec is zero-copy via rcache when its memory is ucp_mem_map-registered;
// the frame CRC covers [meta||value] just like MSG_PUT. The
// response is a normal rkyv `PutResp` (tiny — no ZC framing needed back).
pub const MSG_PUT_ZC: u8 = 0x51;

/// Fixed prefix of the MSG_PUT_ZC meta: part_id(8)+region_epoch(8)+
/// expires_at(8)+key_len(4).
pub const PUT_ZC_HEADER_LEN: usize = 28;

/// Build the MSG_PUT_ZC meta block `[part_id][region_epoch][expires_at]
/// [key_len][key]` (value is appended by the caller as a separate iovec).
pub fn encode_put_zc_meta(
    part_id: u64,
    region_epoch: u64,
    expires_at: u64,
    key: &[u8],
) -> bytes::Bytes {
    use bytes::BufMut;
    let mut b = bytes::BytesMut::with_capacity(PUT_ZC_HEADER_LEN + key.len());
    b.put_u64_le(part_id);
    b.put_u64_le(region_epoch);
    b.put_u64_le(expires_at);
    b.put_u32_le(key.len() as u32);
    b.put_slice(key);
    b.freeze()
}

/// Parsed MSG_PUT_ZC meta + the offset where the value begins in the payload.
pub struct PutZcMeta {
    pub part_id: u64,
    pub region_epoch: u64,
    pub expires_at: u64,
    pub key_len: usize,
    /// Byte offset of the value within the original payload.
    pub value_offset: usize,
}

/// Parse the fixed prefix + key length of a MSG_PUT_ZC payload. Returns `None`
/// if the payload is too short to hold the header + declared key. The caller
/// reads the key as `payload[PUT_ZC_HEADER_LEN..value_offset]` and the value as
/// `payload[value_offset..]` (both zero-copy slices).
pub fn parse_put_zc_meta(payload: &[u8]) -> Option<PutZcMeta> {
    if payload.len() < PUT_ZC_HEADER_LEN {
        return None;
    }
    let part_id = u64::from_le_bytes(payload[0..8].try_into().ok()?);
    let region_epoch = u64::from_le_bytes(payload[8..16].try_into().ok()?);
    let expires_at = u64::from_le_bytes(payload[16..24].try_into().ok()?);
    let key_len = u32::from_le_bytes(payload[24..28].try_into().ok()?) as usize;
    let value_offset = PUT_ZC_HEADER_LEN.checked_add(key_len)?;
    if payload.len() < value_offset {
        return None;
    }
    Some(PutZcMeta {
        part_id,
        region_epoch,
        expires_at,
        key_len,
        value_offset,
    })
}

// ── Status codes ────────────────────────────────────────────────────────────

pub const CODE_OK: u8 = 0;
pub const CODE_NOT_FOUND: u8 = 1;
pub const CODE_INVALID_ARGUMENT: u8 = 2;
pub const CODE_PRECONDITION: u8 = 3;
pub const CODE_ERROR: u8 = 4;
/// F129/F186: regular `Put` value exceeds the
/// `AUTUMN_PS_MAX_INLINE_BYTES` cap. Caller should split the value into
/// chunks and stripe-write via `ClusterClient::put_stream_begin`
/// (client-side striperados — chunks are normal Puts to a
/// reserved-namespace key + a 29-byte Meta blob at the user key).
pub const CODE_VALUE_TOO_LARGE: u8 = 5;
//
// CODE 6 was `CODE_UPLOAD_NOT_FOUND` for the F129 server-side
// multipart upload session. Removed in F186 (no server-side sessions
// any more). Code 6 is RESERVED — don't reuse for at least one major
// version to avoid mis-decoding by stale clients.
/// F185: Put/Delete/StreamPut rejected because the partition is in the
/// `frozen_for_merge` window. Caller should refresh routing and retry —
/// the merged topology is committed on the manager and the survivor will
/// reopen with the wider rg on its next region_sync tick.
pub const CODE_UNAVAILABLE: u8 = 7;

/// Request's `region_epoch` doesn't match the PS's current
/// `MgrRegionInfo.region_epoch` — the partition's `rg` has been
/// rewritten on the manager (split / merge) since the client cached
/// it. SDK refreshes regions and retries. Reserved for future use as
/// an inline body code; today the PS surfaces this condition via
/// `StatusCode::FailedPrecondition` frame error which routes through
/// the same SDK refresh path.
pub const CODE_REGION_EPOCH_STALE: u8 = 8;

// ── Request/Response types ─────────────────────────────────────────────────

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PutReq {
    pub part_id: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    /// F178 follow-up: the `must_sync` field was removed. Every Put is
    /// now durable via the extent-node fsync coalescer (RocksDB-style
    /// group commit). The PS no longer threads any sync flag through.
    pub expires_at: u64,
    /// TiKV-style region epoch: client stamps the epoch from its
    /// routing cache; PS rejects with FailedPrecondition when its own
    /// epoch differs (split / merge has happened). `0` = skip check
    /// (bootstrap, tests, legacy paths).
    pub region_epoch: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PutResp {
    pub code: u8,
    pub message: String,
    pub key: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetReq {
    pub part_id: u64,
    pub key: Vec<u8>,
    /// Sub-range read: byte offset within the value. 0 = start.
    pub offset: u32,
    /// Sub-range read: number of bytes to read. 0 = read entire value.
    pub length: u32,
    /// See `PutReq.region_epoch`.
    pub region_epoch: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetResp {
    pub code: u8,
    pub message: String,
    pub value: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DeleteReq {
    pub part_id: u64,
    pub key: Vec<u8>,
    /// See `PutReq.region_epoch`.
    pub region_epoch: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DeleteResp {
    pub code: u8,
    pub message: String,
    pub key: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct HeadReq {
    pub part_id: u64,
    pub key: Vec<u8>,
    /// See `PutReq.region_epoch`.
    pub region_epoch: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct HeadResp {
    pub code: u8,
    pub message: String,
    pub found: bool,
    pub value_length: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RangeReq {
    pub part_id: u64,
    pub prefix: Vec<u8>,
    pub start: Vec<u8>,
    pub limit: u32,
    /// See `PutReq.region_epoch`. Especially important for range:
    /// `handle_range` historically `continue`d on out-of-range keys
    /// (rpc_handlers.rs:351), returning `Ok(RangeResp{entries: ...})`
    /// with valid-but-partial data after a split. Stamping epoch
    /// converts the silent-truncation failure into a refresh+retry.
    pub region_epoch: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RangeEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RangeResp {
    pub code: u8,
    pub message: String,
    pub entries: Vec<RangeEntry>,
    pub has_more: bool,
    /// CockroachDB-style ResumeSpan cursor: the PS's authoritative
    /// `rg.end_key` (the byte AFTER the last key this partition owns).
    /// On a successful scan, the SDK uses this as the start_key for
    /// the next partition lookup — so a split that happened DURING
    /// the scan auto-resolves: the cursor naturally falls into the
    /// new sibling's range on the next ps_call. Empty = end of
    /// keyspace (last partition's end_key was unbounded).
    pub cur_end_key: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct SplitPartReq {
    pub part_id: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct SplitPartResp {
    pub code: u8,
    pub message: String,
}

// F183 — partition merge. Sent to the SURVIVOR's PS; both partitions
// must currently be served by that PS (cross-PS merge unsupported).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MergePartReq {
    pub survivor_part_id: u64,
    pub victim_part_id: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MergePartResp {
    pub code: u8,
    pub message: String,
}

// F185 — PrepareMerge-style freeze RPC. The CLI sends this to each
// participating partition (survivor + victim) BEFORE capturing
// commit_length so the merge txn cannot lose writes that would otherwise
// race the FLUSH→commit window.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MergeFreezeReq {
    pub part_id: u64,
    /// true = enter frozen state (drain + flush + halt new writes);
    /// false = leave frozen state (used by the CLI's rollback path on
    /// manager-side failure).
    pub freeze: bool,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MergeFreezeResp {
    pub code: u8,
    pub message: String,
}

/// F210-C4: manager → PS pull of the partition's current vp_refs.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PullVpRefsReq {
    pub part_id: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PullVpRefsResp {
    pub code: u8,
    pub message: String,
    /// `(extent_id, count)` pairs — same shape as
    /// `SyncPartitionVpRefsReq.refs`. Count is the number of live SSTs
    /// whose `vp_deps` mention this extent within this partition.
    pub refs: Vec<(u64, u32)>,
}

/// StreamPut: entire value in one message (no chunked streaming).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct StreamPutReq {
    pub part_id: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    /// F178 follow-up: see `PutReq.must_sync` comment for context.
    pub expires_at: u64,
    /// See `PutReq.region_epoch`.
    pub region_epoch: u64,
}
// Response: PutResp

// F129 PutBegin / PutChunk / PutCommit / PutAbort req/resp removed in
// F186. Stripe-write is now pure client-side (Ceph striperados pattern):
// `ClusterClient::put_stream_begin` returns a `PutStreamHandle` that
// writes each chunk via plain `MSG_PUT` to a reserved-namespace key,
// then writes a 29-byte Meta blob to the user key as the atomic
// commit point.

/// Maintenance operations.
///
/// **F201 wire change (backward-incompatible)**: added four optional
/// fields to carry auto-GC filter parameters (`gc_ratio`,
/// `gc_max_size`, `gc_stream_debt`, `gc_empty_only`). Old binaries
/// that still encode the 3-field shape will fail to decode against
/// the new struct, and vice versa. Same-commit upgrade required;
/// cluster.sh handles this by stopping all roles before restart.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MaintenanceReq {
    pub part_id: u64,
    /// 0 = compact, 1 = auto_gc, 2 = force_gc, 3 = flush
    pub op: u8,
    pub extent_ids: Vec<u64>,
    /// F201: filter — discard ratio threshold (0.0..=1.0). `None` → 0.4
    /// (`GC_DISCARD_RATIO`). Used only when `op == MAINTENANCE_AUTO_GC`.
    pub gc_ratio: Option<f64>,
    /// F201: filter — only consider sealed extents whose `sealed_length`
    /// is at most this many bytes. Combined with a lower `gc_ratio`
    /// lets the caller say "punch small extents at even 10% dead".
    pub gc_max_size: Option<u64>,
    /// F201: stream-level dead-byte high-water hint. When the partition's
    /// total reclaimable bytes exceed this, the per-extent ratio is
    /// halved for this dispatch.
    pub gc_stream_debt: Option<u64>,
    /// F201: only pick `sealed_length == 0` non-tail extents (cheapest
    /// possible GC — no rewrite, just punch_holes). Overrides
    /// `gc_ratio` / `gc_max_size` when true.
    pub gc_empty_only: bool,
}

pub const MAINTENANCE_COMPACT: u8 = 0;
pub const MAINTENANCE_AUTO_GC: u8 = 1;
pub const MAINTENANCE_FORCE_GC: u8 = 2;
pub const MAINTENANCE_FLUSH: u8 = 3;

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MaintenanceResp {
    pub code: u8,
    pub message: String,
}

/// Snapshot of a partition's pending log_stream discards. Used by
/// `autumn-client info` to surface GC backlog without persisting any
/// counter state at the manager.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetDiscardsReq {
    pub part_id: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetDiscardsResp {
    pub code: u8,
    pub message: String,
    /// (extent_id, reclaimable_bytes). Extents not currently in the
    /// partition's `log_stream.extent_ids` are filtered out by the handler
    /// (matches what `background_gc_loop` already does via `valid_discard`).
    pub discards: Vec<(u64, i64)>,
}

// ── MetaStream persistence types ────────────────────────────────────────────

/// SSTable location in rowStream.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct SstLocation {
    pub extent_id: u64,
    pub offset: u32,
    pub len: u32,
}

/// Checkpoint written to metaStream after each flush/compaction.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct TableLocations {
    pub locs: Vec<SstLocation>,
    pub vp_extent_id: u64,
    pub vp_offset: u32,
    /// F243-merge: source partition's log_stream extent count at flush
    /// time. Used post-merge by `recover_partition` to derive each
    /// source's region (positions [cumsum, cumsum + count)) in the
    /// spliced log_stream, so replay dedup uses each source's OWN
    /// `sst_max_seq` (not the union max, which silently skips one
    /// source's post-vp_head tail records ≤ the OTHER source's max).
    /// 0 in legacy / fresh-state checkpoints — treated as "no boundary
    /// info; fall back to single-source replay" (= pre-fix behavior).
    pub log_extent_count: u32,
}

// ── Helper: extract part_id from any partition RPC payload ─────────────────

/// Extract the part_id from a partition RPC request payload.
/// Decodes the full request type based on msg_type. Returns 0 if decoding fails.
pub fn extract_part_id(msg_type: u8, payload: &[u8]) -> u64 {
    match msg_type {
        MSG_PUT => rkyv_decode::<PutReq>(payload)
            .map(|r| r.part_id)
            .unwrap_or(0),
        // MSG_PUT_ZC meta is binary: part_id is the first u64 LE.
        MSG_PUT_ZC => payload
            .get(0..8)
            .and_then(|b| b.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0),
        MSG_GET | MSG_GET_ZC => rkyv_decode::<GetReq>(payload)
            .map(|r| r.part_id)
            .unwrap_or(0),
        MSG_DELETE => rkyv_decode::<DeleteReq>(payload)
            .map(|r| r.part_id)
            .unwrap_or(0),
        MSG_HEAD => rkyv_decode::<HeadReq>(payload)
            .map(|r| r.part_id)
            .unwrap_or(0),
        MSG_RANGE => rkyv_decode::<RangeReq>(payload)
            .map(|r| r.part_id)
            .unwrap_or(0),
        MSG_SPLIT_PART => rkyv_decode::<SplitPartReq>(payload)
            .map(|r| r.part_id)
            .unwrap_or(0),
        MSG_STREAM_PUT => rkyv_decode::<StreamPutReq>(payload)
            .map(|r| r.part_id)
            .unwrap_or(0),
        MSG_MAINTENANCE => rkyv_decode::<MaintenanceReq>(payload)
            .map(|r| r.part_id)
            .unwrap_or(0),
        MSG_GET_DISCARDS => rkyv_decode::<GetDiscardsReq>(payload)
            .map(|r| r.part_id)
            .unwrap_or(0),
        MSG_MERGE_PART => rkyv_decode::<MergePartReq>(payload)
            .map(|r| r.survivor_part_id)
            .unwrap_or(0),
        MSG_MERGE_FREEZE => rkyv_decode::<MergeFreezeReq>(payload)
            .map(|r| r.part_id)
            .unwrap_or(0),
        MSG_PULL_VP_REFS => rkyv_decode::<PullVpRefsReq>(payload)
            .map(|r| r.part_id)
            .unwrap_or(0),
        _ => 0,
    }
}

#[cfg(test)]
mod msg_type_tests {
    use super::*;

    #[test]
    fn msg_type_constants_dont_collide() {
        let all = [
            MSG_PUT,
            MSG_GET,
            MSG_DELETE,
            MSG_HEAD,
            MSG_RANGE,
            MSG_SPLIT_PART,
            MSG_STREAM_PUT,
            MSG_MAINTENANCE,
            MSG_GET_DISCARDS,
            MSG_MERGE_PART,
            MSG_MERGE_FREEZE,
            MSG_PULL_VP_REFS,
            MSG_GET_ZC,
        ];
        for i in 0..all.len() {
            for j in i + 1..all.len() {
                assert_ne!(all[i], all[j], "msg_type collision at index {} vs {}", i, j);
            }
        }
    }
}
