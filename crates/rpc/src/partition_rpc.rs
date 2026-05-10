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

// F129 — S3-style multipart upload for values larger than the inline cap
// (`AUTUMN_PS_MAX_INLINE_BYTES`, default 64 MiB). Begin → 1+ Chunk → Commit
// or Abort. PS holds the fragment list on the upload session; the client
// only needs the upload_id. Get-side streaming is pure client-side
// looping over GetReq.offset/length, no new RPC needed there.
pub const MSG_PUT_BEGIN: u8 = 0x49;
pub const MSG_PUT_CHUNK: u8 = 0x4A;
pub const MSG_PUT_COMMIT: u8 = 0x4B;
pub const MSG_PUT_ABORT: u8 = 0x4C;

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

// ── Status codes ────────────────────────────────────────────────────────────

pub const CODE_OK: u8 = 0;
pub const CODE_NOT_FOUND: u8 = 1;
pub const CODE_INVALID_ARGUMENT: u8 = 2;
pub const CODE_PRECONDITION: u8 = 3;
pub const CODE_ERROR: u8 = 4;
/// F129: regular `Put` value exceeds the `AUTUMN_PS_MAX_INLINE_BYTES`
/// cap. Caller should retry via `PutStream` (Begin/Chunk/Commit).
pub const CODE_VALUE_TOO_LARGE: u8 = 5;
/// F129: `PutChunk` / `PutCommit` / `PutAbort` references an
/// upload_id that doesn't exist on this PS. Could be: unknown id,
/// expired (TTL), already committed, already aborted, or PS restart.
pub const CODE_UPLOAD_NOT_FOUND: u8 = 6;
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

// ── F129 PutStream / PutChunk / PutCommit / PutAbort ───────────────────────

/// Begin a multipart upload. PS allocates an `upload_id`, records
/// `(key, expires_at, started_at)` in its in-memory session map and
/// returns the id. No bytes are written yet.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PutBeginReq {
    pub part_id: u64,
    pub key: Vec<u8>,
    /// 0 = no expiry. Stored on the session and applied to the final
    /// memtable entry at commit time.
    pub expires_at: u64,
    /// Hint for the PS to reject early if it knows it can't satisfy.
    /// 0 = unknown. Not authoritative; PS will commit whatever
    /// chunks actually arrived.
    pub total_bytes_hint: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PutBeginResp {
    pub code: u8,
    pub message: String,
    /// 128-bit opaque session handle. Use as-is in subsequent
    /// PutChunk / PutCommit / PutAbort calls.
    pub upload_id: u128,
}

/// Append one chunk to an in-progress multipart upload.
///
/// The chunk's bytes are appended to log_stream as a single WAL
/// record with op `OP_CHUNK_BLOB`; (extent_id, offset, len) is added
/// to the session's fragment list. Order is enforced by `chunk_index`:
/// must be exactly `session.next_index` (0-based, monotonic). Out-of-
/// order chunks are rejected so retransmits don't double-append.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PutChunkReq {
    pub part_id: u64,
    pub upload_id: u128,
    pub chunk_index: u32,
    pub data: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PutChunkResp {
    pub code: u8,
    pub message: String,
    /// Running total of bytes successfully appended for this upload.
    /// The client uses this to drive backpressure and to verify
    /// `total_bytes` at commit time without re-counting locally.
    pub bytes_committed: u64,
}

/// Finalize a multipart upload. PS reads the session's fragment
/// list, builds a multi-fragment ValuePointer, and inserts a normal
/// memtable entry under the original key with the supplied expiry.
/// On success the value is visible to subsequent Get / GetStream
/// calls. The session is dropped from the in-memory map; its
/// fragments become part of the live data.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PutCommitReq {
    pub part_id: u64,
    pub upload_id: u128,
    /// Optional sanity check. If non-zero and != session.bytes_committed,
    /// PS returns `CODE_INVALID_ARGUMENT` and leaves the session
    /// in place so the client can either resume or abort. 0 = skip.
    pub expected_total_bytes: u64,
}
// Response: PutResp (carries the original key)

/// Drop a multipart upload session. The fragments stay in
/// log_stream as `OP_CHUNK_BLOB` records; GC + compaction skip
/// them and they're reclaimed when the surrounding extent is
/// punched. Idempotent on missing upload_id (returns CODE_OK).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PutAbortReq {
    pub part_id: u64,
    pub upload_id: u128,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PutAbortResp {
    pub code: u8,
    pub message: String,
}

/// Maintenance operations.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MaintenanceReq {
    pub part_id: u64,
    /// 0 = compact, 1 = auto_gc, 2 = force_gc
    pub op: u8,
    pub extent_ids: Vec<u64>,
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
        MSG_PUT_BEGIN => rkyv_decode::<PutBeginReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_PUT_CHUNK => rkyv_decode::<PutChunkReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_PUT_COMMIT => rkyv_decode::<PutCommitReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_PUT_ABORT => rkyv_decode::<PutAbortReq>(payload).map(|r| r.part_id).unwrap_or(0),
        MSG_MERGE_PART => rkyv_decode::<MergePartReq>(payload).map(|r| r.survivor_part_id).unwrap_or(0),
        MSG_MERGE_FREEZE => rkyv_decode::<MergeFreezeReq>(payload).map(|r| r.part_id).unwrap_or(0),
        _ => 0,
    }
}

#[cfg(test)]
mod f129_wire_tests {
    use super::*;

    #[test]
    fn put_begin_roundtrip() {
        let req = PutBeginReq {
            part_id: 42,
            key: b"big/dataset.bin".to_vec(),
            expires_at: 0,
            total_bytes_hint: 1 << 30,
        };
        let bytes = rkyv_encode(&req);
        let dec: PutBeginReq = rkyv_decode(&bytes).unwrap();
        assert_eq!(dec.part_id, 42);
        assert_eq!(dec.key, b"big/dataset.bin");
        assert_eq!(dec.total_bytes_hint, 1 << 30);
    }

    #[test]
    fn put_chunk_roundtrip() {
        let req = PutChunkReq {
            part_id: 7,
            upload_id: 0xdead_beef_cafe_babe_1234_5678_9abc_def0u128,
            chunk_index: 13,
            data: vec![0xab; 4096],
        };
        let bytes = rkyv_encode(&req);
        let dec: PutChunkReq = rkyv_decode(&bytes).unwrap();
        assert_eq!(dec.upload_id, 0xdead_beef_cafe_babe_1234_5678_9abc_def0u128);
        assert_eq!(dec.chunk_index, 13);
        assert_eq!(dec.data.len(), 4096);
        assert_eq!(dec.data[0], 0xab);
    }

    #[test]
    fn put_commit_abort_roundtrip() {
        let commit = PutCommitReq { part_id: 1, upload_id: 99, expected_total_bytes: 12345 };
        let abort = PutAbortReq { part_id: 1, upload_id: 99 };
        let cb = rkyv_encode(&commit);
        let ab = rkyv_encode(&abort);
        let cd: PutCommitReq = rkyv_decode(&cb).unwrap();
        let ad: PutAbortReq = rkyv_decode(&ab).unwrap();
        assert_eq!(cd.expected_total_bytes, 12345);
        assert_eq!(ad.upload_id, 99);
    }

    #[test]
    fn extract_part_id_routes_new_msg_types() {
        let begin = PutBeginReq { part_id: 11, key: vec![], expires_at: 0, total_bytes_hint: 0 };
        let chunk = PutChunkReq { part_id: 22, upload_id: 0, chunk_index: 0, data: vec![] };
        let commit = PutCommitReq { part_id: 33, upload_id: 0, expected_total_bytes: 0 };
        let abort = PutAbortReq { part_id: 44, upload_id: 0 };

        assert_eq!(extract_part_id(MSG_PUT_BEGIN, &rkyv_encode(&begin)), 11);
        assert_eq!(extract_part_id(MSG_PUT_CHUNK, &rkyv_encode(&chunk)), 22);
        assert_eq!(extract_part_id(MSG_PUT_COMMIT, &rkyv_encode(&commit)), 33);
        assert_eq!(extract_part_id(MSG_PUT_ABORT, &rkyv_encode(&abort)), 44);
    }

    #[test]
    fn msg_type_constants_dont_collide() {
        let all = [
            MSG_PUT, MSG_GET, MSG_DELETE, MSG_HEAD, MSG_RANGE,
            MSG_SPLIT_PART, MSG_STREAM_PUT, MSG_MAINTENANCE,
            MSG_GET_DISCARDS,
            MSG_PUT_BEGIN, MSG_PUT_CHUNK, MSG_PUT_COMMIT, MSG_PUT_ABORT,
        ];
        for i in 0..all.len() {
            for j in i + 1..all.len() {
                assert_ne!(all[i], all[j], "msg_type collision at index {} vs {}", i, j);
            }
        }
    }
}
