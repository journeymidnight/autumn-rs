//! Wire codec for PartitionKv RPCs over autumn-rpc.
//!
//! All 8 PartitionKv RPCs use rkyv serialization.
//! Message type constants are in the 0x40–0x4F range.
//! Every request includes `part_id` for thread-per-partition routing.

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
}

// ── Helper: extract part_id from any partition RPC payload ─────────────────

/// Extract the part_id from a partition RPC request payload.
/// Decodes the full request type based on msg_type. Returns 0 if decoding fails.
pub fn extract_part_id(msg_type: u8, payload: &[u8]) -> u64 {
    match msg_type {
        MSG_PUT => rkyv_decode::<PutReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_GET => rkyv_decode::<GetReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_DELETE => rkyv_decode::<DeleteReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_HEAD => rkyv_decode::<HeadReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_RANGE => rkyv_decode::<RangeReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_SPLIT_PART => rkyv_decode::<SplitPartReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_STREAM_PUT => rkyv_decode::<StreamPutReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_MAINTENANCE => rkyv_decode::<MaintenanceReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_GET_DISCARDS => rkyv_decode::<GetDiscardsReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_MERGE_PART => rkyv_decode::<MergePartReq>(payload).map(|r| r.survivor_part_id).unwrap_or(0),
        MSG_MERGE_FREEZE => rkyv_decode::<MergeFreezeReq>(payload).map(|r| r.part_id).unwrap_or(0),
        _ => 0,
    }
}

#[cfg(test)]
mod msg_type_tests {
    use super::*;

    #[test]
    fn msg_type_constants_dont_collide() {
        let all = [
            MSG_PUT, MSG_GET, MSG_DELETE, MSG_HEAD, MSG_RANGE,
            MSG_SPLIT_PART, MSG_STREAM_PUT, MSG_MAINTENANCE,
            MSG_GET_DISCARDS, MSG_MERGE_PART, MSG_MERGE_FREEZE,
        ];
        for i in 0..all.len() {
            for j in i + 1..all.len() {
                assert_ne!(all[i], all[j], "msg_type collision at index {} vs {}", i, j);
            }
        }
    }
}
