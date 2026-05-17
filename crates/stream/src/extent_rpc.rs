//! Wire codec for ExtentService RPCs over autumn-rpc.
//!
//! Hot-path messages (Append, ReadBytes, CommitLength) use fixed-size binary
//! headers for minimum overhead. Other RPCs use rkyv zero-copy serialization.
//! Large-payload RPCs (CopyExtent, WriteShard) use fixed binary headers +
//! raw payload to avoid serialization copies.

use autumn_rpc::StatusCode;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use rkyv::{Archive, Deserialize, Serialize};

// ── msg_type constants ───────────────────────────────────────────────────────

pub const MSG_APPEND: u8 = 1;
pub const MSG_READ_BYTES: u8 = 2;
pub const MSG_COMMIT_LENGTH: u8 = 3;
pub const MSG_ALLOC_EXTENT: u8 = 4;
pub const MSG_DF: u8 = 5;
pub const MSG_REQUIRE_RECOVERY: u8 = 6;
pub const MSG_RE_AVALI: u8 = 7;
pub const MSG_COPY_EXTENT: u8 = 8;
pub const MSG_CONVERT_TO_EC: u8 = 9;
pub const MSG_WRITE_SHARD: u8 = 10;
pub const MSG_DELETE_EXTENT: u8 = 11;
pub const MSG_COMMIT_EC_SHARD: u8 = 12;
// 13 = MSG_SYNC_EXTENT — retired in F150 Phase B (the F142 fsync barrier was
//      folded into `start_write_batch`'s rotation-trigger `must_sync=true`
//      batch promotion). F178 Phase 2 retires the rotation barrier in turn,
//      replacing both with the per-extent fsync coalescer + `MSG_SYNCED_LENGTH`
//      durability query so flush waits at flush-time, not at write-time.
/// F178 Phase 2: query the extent-node's coalesced fsync high-water mark.
/// Returned `length` = `Coalescer::last_synced` for `extent_id`. Used by
/// `flush_one_imm` to await durability of all log_stream bytes referenced
/// by the to-be-flushed memtable's ValuePointers BEFORE uploading the SST.
pub const MSG_SYNCED_LENGTH: u8 = 13;
/// Manager-only probe RPC. Returns CommitLengthResp-shaped
/// `(code, length)` without touching the owner-lock fence — no
/// revision check, no mutation of `last_revision`, no `.meta`
/// rewrite. Two call sites today:
///   - `manager/src/recovery.rs`'s `recovery_dispatch_loop`
///     liveness probe (uses `code == CODE_OK` to decide whether
///     to fire `dispatch_recovery_task`; ignores `length`).
///   - `autumn-client info`'s open-extent live-length display
///     (uses `length` to render `commit_length` for streams
///     where no PS-owner context is available).
/// External (non-manager) callers MUST NOT use this RPC for
/// seal/consensus reads — those go through `MSG_COMMIT_LENGTH`
/// with a real revision so the EN's fence handover side-effect
/// fires (see `extent_node.rs::handle_commit_length`).
pub const MSG_PROBE_EXTENT: u8 = 14;
// MSG_TYPE_PING = 0xFF is reserved by autumn-rpc for heartbeat

// ── Append (hot path) ────────────────────────────────────────────────────────

/// Fixed binary header for AppendRequest: 28 bytes + raw payload.
/// ```text
/// [extent_id: u64 LE][eversion: u64 LE][commit: u32 LE][revision: i64 LE]
/// [payload bytes...]
/// ```
///
/// F178 Phase 3 follow-up: `must_sync` byte removed. Every append is
/// always durable via the per-extent fsync coalescer (see
/// `extent_node.rs::Coalescer`); the handler unconditionally registers a
/// sync waiter and awaits coalesced `sync_data`. Pre-F178 this byte
/// distinguished sync vs. nosync writes; post-F178 there is no nosync
/// path. Wire format shrinks by 1 byte.
pub const APPEND_HEADER_LEN: usize = 28;

pub struct AppendReq {
    pub extent_id: u64,
    pub eversion: u64,
    pub commit: u32,
    pub revision: i64,
    pub payload: Bytes,
}

impl AppendReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(APPEND_HEADER_LEN + self.payload.len());
        buf.put_u64_le(self.extent_id);
        buf.put_u64_le(self.eversion);
        buf.put_u32_le(self.commit);
        buf.put_i64_le(self.revision);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    /// Encode only the 28-byte header (for vectored writes — payload sent separately).
    pub fn encode_header(
        extent_id: u64,
        eversion: u64,
        commit: u32,
        revision: i64,
    ) -> Bytes {
        let mut buf = BytesMut::with_capacity(APPEND_HEADER_LEN);
        buf.put_u64_le(extent_id);
        buf.put_u64_le(eversion);
        buf.put_u32_le(commit);
        buf.put_i64_le(revision);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < APPEND_HEADER_LEN {
            return Err("append request too short");
        }
        let extent_id = data.get_u64_le();
        let eversion = data.get_u64_le();
        let commit = data.get_u32_le();
        let revision = data.get_i64_le();
        let payload = data;
        Ok(Self {
            extent_id,
            eversion,
            commit,
            revision,
            payload,
        })
    }
}

/// Fixed binary AppendResponse: 9 bytes.
/// ```text
/// [code: u8][offset: u32 LE][end: u32 LE]
/// ```
pub struct AppendResp {
    pub code: u8,
    pub offset: u32,
    pub end: u32,
}

impl AppendResp {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(9);
        buf.put_u8(self.code);
        buf.put_u32_le(self.offset);
        buf.put_u32_le(self.end);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 9 {
            return Err("append response too short");
        }
        Ok(Self {
            code: data.get_u8(),
            offset: data.get_u32_le(),
            end: data.get_u32_le(),
        })
    }
}

// ── ReadBytes (hot path) ─────────────────────────────────────────────────────

/// ReadBytesRequest: 24 bytes.
/// ```text
/// [extent_id: u64 LE][eversion: u64 LE][offset: u32 LE][length: u32 LE]
/// ```
pub struct ReadBytesReq {
    pub extent_id: u64,
    pub eversion: u64,
    pub offset: u32,
    pub length: u32,
}

impl ReadBytesReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(24);
        buf.put_u64_le(self.extent_id);
        buf.put_u64_le(self.eversion);
        buf.put_u32_le(self.offset);
        buf.put_u32_le(self.length);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 24 {
            return Err("read_bytes request too short");
        }
        Ok(Self {
            extent_id: data.get_u64_le(),
            eversion: data.get_u64_le(),
            offset: data.get_u32_le(),
            length: data.get_u32_le(),
        })
    }
}

/// ReadBytesResponse: [code: u8][end: u32 LE][payload bytes...]
pub struct ReadBytesResp {
    pub code: u8,
    pub end: u32,
    pub payload: Bytes,
}

impl ReadBytesResp {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(5 + self.payload.len());
        buf.put_u8(self.code);
        buf.put_u32_le(self.end);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 5 {
            return Err("read_bytes response too short");
        }
        let code = data.get_u8();
        let end = data.get_u32_le();
        let payload = data;
        Ok(Self { code, end, payload })
    }
}

// ── CommitLength (hot path) ──────────────────────────────────────────────────

/// CommitLengthRequest: 16 bytes.
/// [extent_id: u64 LE][revision: i64 LE]
///
/// **Wire contract on `revision` (post-F210-H3 Tier 2, 2026-05-17):**
///
/// `revision` is an i64 but MUST be `> 0` on the wire — it carries the
/// caller's owner-lock claim. The EN's `handle_commit_length`:
///   - returns `CODE_INVALID_ARGUMENT` if `revision <= 0` (no
///     "probe sentinel" path — that escape hatch existed pre-F210-H2
///     and broke fence semantics; see `MSG_PROBE_EXTENT` instead);
///   - returns `CODE_LOCKED_BY_OTHER` if `revision < entry.last_revision`
///     (caller is a stale owner; reject);
///   - if `revision > entry.last_revision`, performs **fence handover**:
///     bumps `last_revision` and persists `.meta` so subsequent writes
///     from older owners are rejected by `handle_append` immediately.
///
/// This RPC is the canonical seal-consensus + ownership-acquisition
/// primitive. Manager's `commit_length_on_node(addr, eid, revision)`
/// helper forwards the caller's validated `req.revision` through
/// `handle_check_commit_length` and `handle_stream_alloc_extent`.
/// Manager probes WITHOUT an owner context (recovery liveness,
/// `autumn-client info` display) use `MSG_PROBE_EXTENT` instead —
/// that RPC returns the same `(code, length)` shape but skips the
/// fence interaction entirely.
pub struct CommitLengthReq {
    pub extent_id: u64,
    pub revision: i64,
}

impl CommitLengthReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u64_le(self.extent_id);
        buf.put_i64_le(self.revision);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 16 {
            return Err("commit_length request too short");
        }
        Ok(Self {
            extent_id: data.get_u64_le(),
            revision: data.get_i64_le(),
        })
    }
}

/// CommitLengthResponse: 5 bytes.
/// [code: u8][length: u32 LE]
pub struct CommitLengthResp {
    pub code: u8,
    pub length: u32,
}

impl CommitLengthResp {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(5);
        buf.put_u8(self.code);
        buf.put_u32_le(self.length);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 5 {
            return Err("commit_length response too short");
        }
        Ok(Self {
            code: data.get_u8(),
            length: data.get_u32_le(),
        })
    }
}

// ── ProbeExtent (manager-only, no fence) ────────────────────────────────────

/// ProbeExtentRequest: 8 bytes. `[extent_id: u64 LE]`
///
/// Manager-only liveness + length probe; see `MSG_PROBE_EXTENT` const
/// docstring for the call-site contract. Response shape matches
/// `CommitLengthResp` (`(code, length)`) so the manager can reuse the
/// same `(addr, eid)` plumbing without separate decode paths.
pub struct ProbeExtentReq {
    pub extent_id: u64,
}

impl ProbeExtentReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64_le(self.extent_id);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 8 {
            return Err("probe_extent request too short");
        }
        Ok(Self {
            extent_id: data.get_u64_le(),
        })
    }
}

/// ProbeExtentResponse: 5 bytes. `[code: u8][length: u32 LE]`. Same shape
/// as `CommitLengthResp` — `code` is `CODE_OK` (extent present) or
/// `CODE_NOT_FOUND` (extent missing locally); `length` carries
/// `coalescer.last_synced` for open extents or `sealed_length` for sealed.
pub type ProbeExtentResp = CommitLengthResp;

// (F150 Phase B removed SyncExtentReq/Resp + MSG_SYNC_EXTENT — the F142
// fsync barrier is now folded into `start_write_batch`'s rotation-trigger
// `must_sync=true` promotion in autumn-partition-server. F178 Phase 2
// then drops the rotation barrier altogether and adds MSG_SYNCED_LENGTH
// (below) for flush-time durability waits via the per-extent coalescer.)

// ── SyncedLength (F178 Phase 2) ──────────────────────────────────────────────

/// SyncedLengthRequest: 8 bytes.
/// `[extent_id: u64 LE]`
pub struct SyncedLengthReq {
    pub extent_id: u64,
}

impl SyncedLengthReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64_le(self.extent_id);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 8 {
            return Err("synced_length request too short");
        }
        Ok(Self {
            extent_id: data.get_u64_le(),
        })
    }
}

/// SyncedLengthResponse: 9 bytes.
/// `[code: u8][length: u64 LE]`
///
/// `length` is `Coalescer::last_synced` — the highest byte offset known to
/// be durable on this replica. Quorum is enforced by the client side
/// (see `StreamClient::await_log_synced_to`); the server reports its own
/// view only.
pub struct SyncedLengthResp {
    pub code: u8,
    pub length: u64,
}

impl SyncedLengthResp {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(9);
        buf.put_u8(self.code);
        buf.put_u64_le(self.length);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 9 {
            return Err("synced_length response too short");
        }
        Ok(Self {
            code: data.get_u8(),
            length: data.get_u64_le(),
        })
    }
}

// ── rkyv helpers ────────────────────────────────────────────────────────────

use rkyv::api::high::{HighDeserializer, HighSerializer};
use rkyv::rancor::Error as RkyvError;
use rkyv::ser::allocator::ArenaHandle;

/// Serialize a value to Bytes using rkyv.
pub fn rkyv_encode<T>(val: &T) -> Bytes
where
    T: for<'a> Serialize<HighSerializer<rkyv::util::AlignedVec, ArenaHandle<'a>, RkyvError>>,
{
    let buf = rkyv::to_bytes::<RkyvError>(val).expect("rkyv encode");
    Bytes::copy_from_slice(&buf)
}

/// Deserialize a value from bytes using rkyv with archive-bytes validation.
/// Copies into an AlignedVec if the input is not properly aligned.
///
/// F155: switched from `from_bytes_unchecked` to the checked `from_bytes` —
/// see the matching note in `crates/rpc/src/manager_rpc.rs` for rationale.
/// Validates archived bytes via bytecheck before deserialising; returns
/// `Err` on malformed input instead of UB.
pub fn rkyv_decode<T>(data: &[u8]) -> Result<T, String>
where
    T: Archive,
    T::Archived: Deserialize<T, HighDeserializer<RkyvError>>
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>,
{
    let mut v = rkyv::util::AlignedVec::<16>::with_capacity(data.len());
    v.extend_from_slice(data);
    rkyv::from_bytes::<T, RkyvError>(&v).map_err(|e| format!("rkyv decode: {e}"))
}

// ── Status code constants ────────────────────────────────────────────────────

/// Append response code constants for hot-path binary wire format.
pub const CODE_OK: u8 = 0;
pub const CODE_NOT_FOUND: u8 = 1;
pub const CODE_PRECONDITION: u8 = 3;
pub const CODE_ERROR: u8 = 4;
/// Returned when `header.revision < last_revision` — a newer owner has taken the lock.
pub const CODE_LOCKED_BY_OTHER: u8 = 5;
/// Returned by ReadBytes when the client's `eversion` is older than the
/// server's local view (e.g. the extent has been EC-converted under a
/// stale `StreamClient.extent_info_cache` entry). The client must
/// invalidate its cached `ExtentInfo` and refetch from the manager.
pub const CODE_EVERSION_MISMATCH: u8 = 6;

/// Convert a u8 code from binary wire format to autumn_rpc::StatusCode.
pub fn code_to_status(code: u8) -> StatusCode {
    match code {
        CODE_OK => StatusCode::Ok,
        CODE_NOT_FOUND => StatusCode::NotFound,
        CODE_PRECONDITION => StatusCode::FailedPrecondition,
        CODE_EVERSION_MISMATCH => StatusCode::FailedPrecondition,
        _ => StatusCode::Internal,
    }
}

/// Convert a u8 code from binary wire format to a descriptive string.
pub fn code_description(code: u8) -> &'static str {
    match code {
        CODE_OK => "ok",
        CODE_NOT_FOUND => "not found",
        CODE_PRECONDITION => "precondition failed",
        CODE_EVERSION_MISMATCH => "eversion mismatch (stale client cache)",
        _ => "error",
    }
}

// ── rkyv control-plane message types ────────────────────────────────────────

/// ExtentInfo — metadata about a single extent replica set.
/// Mirrors autumn.proto ExtentInfo; used internally and in manager RPC.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct ExtentInfo {
    pub extent_id: u64,
    pub replicates: Vec<u64>,
    pub parity: Vec<u64>,
    pub eversion: u64,
    pub refs: u64,
    pub sealed_length: u64,
    pub avali: u32,
    pub replicate_disks: Vec<u64>,
    pub parity_disks: Vec<u64>,
    /// True iff this extent has actually been EC-converted (sealed +
    /// RS-encoded by `apply_ec_conversion_done`). The read path uses
    /// this — NOT `parity.is_empty()` — to decide between
    /// `ec_subrange_read` and `read_replicated_with_failover`. The
    /// manager pre-fills `parity` at allocation time on EC streams,
    /// so an open / pre-conversion extent has `parity != []` but
    /// still holds full replicated data on every K+M node.
    pub ec_converted: bool,
}

/// StreamInfo — stream ID and its ordered list of extent IDs.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct StreamInfo {
    pub stream_id: u64,
    pub extent_ids: Vec<u64>,
    pub ec_data_shard: u32,
    pub ec_parity_shard: u32,
}

/// AllocExtent request: pre-create an empty extent file on this node.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct AllocExtentReq {
    pub extent_id: u64,
}

/// AllocExtent response.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct AllocExtentResp {
    pub code: u8,
    pub disk_id: u64,
    pub message: String,
}

/// Disk space statistics for one disk.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DiskStatus {
    pub total: u64,
    pub free: u64,
    pub online: bool,
}

/// A recovery task descriptor.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct RecoveryTask {
    pub extent_id: u64,
    pub replace_id: u64,
    pub node_id: u64,
    pub start_time: i64,
}

/// A completed recovery task.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RecoveryTaskDone {
    pub task: RecoveryTask,
    pub ready_disk_id: u64,
}

/// Df (disk-free + recovery heartbeat) request.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DfReq {
    pub tasks: Vec<RecoveryTask>,
    pub disk_ids: Vec<u64>,
}

/// Df response: completed recovery tasks + per-disk stats.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DfResp {
    pub done_tasks: Vec<RecoveryTaskDone>,
    /// (disk_id, DiskStatus) pairs (HashMap not used for rkyv compat).
    pub disk_status: Vec<(u64, DiskStatus)>,
}

/// RequireRecovery request: start a background recovery task.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RequireRecoveryReq {
    pub task: RecoveryTask,
}

/// Generic code + message response for control-plane RPCs.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct CodeResp {
    pub code: u8,
    pub message: String,
}

/// ReAvali request: re-mark a sealed extent as available on this node.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ReAvaliReq {
    pub extent_id: u64,
    pub eversion: u64,
}

/// DeleteExtent request: unlink the physical extent file (`.dat` + `.meta`).
/// Sent by the manager after an extent's refcount drops to 0
/// (`punch_holes` / `truncate` paths). Idempotent: a missing extent on the
/// receiving node returns `CODE_OK`, so manager retries are safe.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DeleteExtentReq {
    pub extent_id: u64,
}

/// ConvertToEc request: EC-encode a sealed extent and distribute shards.
///
/// `eversion` is the post-EC eversion the manager has decided on (one
/// greater than the pre-EC value). The coordinator and every target
/// node must adopt this value locally as part of installing their
/// shard, so that subsequent `MSG_READ_BYTES` requests carrying a
/// stale (pre-EC) eversion are rejected with `CODE_EVERSION_MISMATCH`.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ConvertToEcReq {
    pub extent_id: u64,
    pub data_shards: u32,
    pub parity_shards: u32,
    /// k+m target node addresses (data shard nodes first, then parity).
    pub target_addrs: Vec<String>,
    pub eversion: u64,
}

// ── CopyExtent (binary — large payload) ─────────────────────────────────────

/// CopyExtentRequest: 32 bytes fixed header.
/// ```text
/// [extent_id: u64 LE][offset: u64 LE][size: u64 LE][eversion: u64 LE]
/// ```
pub const COPY_EXTENT_REQ_LEN: usize = 32;

pub struct CopyExtentReq {
    pub extent_id: u64,
    pub offset: u64,
    pub size: u64,
    pub eversion: u64,
}

impl CopyExtentReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(COPY_EXTENT_REQ_LEN);
        buf.put_u64_le(self.extent_id);
        buf.put_u64_le(self.offset);
        buf.put_u64_le(self.size);
        buf.put_u64_le(self.eversion);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < COPY_EXTENT_REQ_LEN {
            return Err("copy_extent request too short");
        }
        Ok(Self {
            extent_id: data.get_u64_le(),
            offset: data.get_u64_le(),
            size: data.get_u64_le(),
            eversion: data.get_u64_le(),
        })
    }
}

/// CopyExtentResponse: [code: u8][payload_len: u64 LE][payload bytes...]
pub struct CopyExtentResp {
    pub code: u8,
    pub payload: Bytes,
}

impl CopyExtentResp {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(9 + self.payload.len());
        buf.put_u8(self.code);
        buf.put_u64_le(self.payload.len() as u64);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 9 {
            return Err("copy_extent response too short");
        }
        let code = data.get_u8();
        let payload_len = data.get_u64_le() as usize;
        if data.len() < payload_len {
            return Err("copy_extent response payload truncated");
        }
        let payload = data.split_to(payload_len);
        Ok(Self { code, payload })
    }
}

// ── WriteShard (binary — large payload) ─────────────────────────────────────

/// WriteShardRequest: [extent_id: u64 LE][shard_index: u32 LE][sealed_length: u64 LE][eversion: u64 LE][payload...]
///
/// `eversion` is the post-EC eversion the manager has decided on. The
/// receiving extent node bumps `entry.eversion` to this value when it
/// installs the shard, so subsequent ReadBytes requests with a stale
/// (pre-EC) eversion are rejected with `CODE_EVERSION_MISMATCH`.
pub const WRITE_SHARD_HEADER_LEN: usize = 28;

pub struct WriteShardReq {
    pub extent_id: u64,
    pub shard_index: u32,
    pub sealed_length: u64,
    pub eversion: u64,
    pub payload: Bytes,
}

impl WriteShardReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(WRITE_SHARD_HEADER_LEN + self.payload.len());
        buf.put_u64_le(self.extent_id);
        buf.put_u32_le(self.shard_index);
        buf.put_u64_le(self.sealed_length);
        buf.put_u64_le(self.eversion);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < WRITE_SHARD_HEADER_LEN {
            return Err("write_shard request too short");
        }
        let extent_id = data.get_u64_le();
        let shard_index = data.get_u32_le();
        let sealed_length = data.get_u64_le();
        let eversion = data.get_u64_le();
        let payload = data;
        Ok(Self { extent_id, shard_index, sealed_length, eversion, payload })
    }
}

/// WriteShardResponse: [code: u8]
pub struct WriteShardResp {
    pub code: u8,
}

impl WriteShardResp {
    pub fn encode(&self) -> Bytes {
        Bytes::copy_from_slice(&[self.code])
    }

    pub fn decode(data: Bytes) -> Result<Self, &'static str> {
        if data.is_empty() {
            return Err("write_shard response too short");
        }
        Ok(Self { code: data[0] })
    }
}

// ── CommitEcShard (binary — phase-2 of 2PC EC conversion) ────────────────────

/// CommitEcShardRequest: [extent_id: u64 LE][sealed_length: u64 LE][eversion: u64 LE]
pub const COMMIT_EC_SHARD_HEADER_LEN: usize = 24;

pub struct CommitEcShardReq {
    pub extent_id: u64,
    pub sealed_length: u64,
    pub eversion: u64,
}

impl CommitEcShardReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(COMMIT_EC_SHARD_HEADER_LEN);
        buf.put_u64_le(self.extent_id);
        buf.put_u64_le(self.sealed_length);
        buf.put_u64_le(self.eversion);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < COMMIT_EC_SHARD_HEADER_LEN {
            return Err("commit_ec_shard request too short");
        }
        let extent_id = data.get_u64_le();
        let sealed_length = data.get_u64_le();
        let eversion = data.get_u64_le();
        Ok(Self { extent_id, sealed_length, eversion })
    }
}

/// CommitEcShardResponse: [code: u8]
pub struct CommitEcShardResp {
    pub code: u8,
}

impl CommitEcShardResp {
    pub fn encode(&self) -> Bytes {
        Bytes::copy_from_slice(&[self.code])
    }

    pub fn decode(data: Bytes) -> Result<Self, &'static str> {
        if data.is_empty() {
            return Err("commit_ec_shard response too short");
        }
        Ok(Self { code: data[0] })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_extent_req_round_trip() {
        let req = DeleteExtentReq { extent_id: 0xdead_beef_cafe_0042 };
        let bytes = rkyv_encode(&req);
        let decoded: DeleteExtentReq = rkyv_decode(&bytes).expect("decode");
        assert_eq!(decoded.extent_id, req.extent_id);
    }

    #[test]
    fn delete_extent_resp_uses_generic_code_resp() {
        let resp = CodeResp { code: CODE_OK, message: String::new() };
        let bytes = rkyv_encode(&resp);
        let decoded: CodeResp = rkyv_decode(&bytes).expect("decode");
        assert_eq!(decoded.code, CODE_OK);
        assert!(decoded.message.is_empty());
    }

    /// F155: rkyv_decode rejects malformed input via bytecheck instead of UB.
    /// Pre-F155 this used `from_bytes_unchecked` and a corrupted payload
    /// (flipped bits past TCP CRC, mixed-version cluster, etc.) caused
    /// out-of-bounds reads or pointer dereferences into arbitrary memory.
    /// Post-F155 the checked decoder runs validation first and returns Err.
    #[test]
    fn f155_rkyv_decode_rejects_malformed() {
        // Encode a valid CodeResp, then mangle each byte and confirm the
        // decode path returns Err rather than panicking or reading garbage.
        let valid = rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: "hello".to_string(),
        });

        // Truncated payload — should fail validation, not UB.
        let truncated = &valid[..valid.len() / 2];
        let r: Result<CodeResp, _> = rkyv_decode(truncated);
        assert!(
            r.is_err(),
            "truncated input must Err (got {:?})",
            r.map(|v| v.code)
        );

        // Single-byte XOR corruption near the end of the payload — most
        // likely to land in archive headers / pointer offsets and trigger
        // the bytecheck validator. We don't assert ALL corruptions fail
        // (some may land in the inline string body and decode to garbage
        // text — still safe, just not-Err); we assert at least one of a
        // sweep does, which proves the validator is running.
        let mut any_err = false;
        for offset in 0..valid.len() {
            let mut corrupted = valid.to_vec();
            corrupted[offset] ^= 0xff;
            let r: Result<CodeResp, _> = rkyv_decode(&corrupted);
            if r.is_err() {
                any_err = true;
                break;
            }
        }
        assert!(
            any_err,
            "no XOR-corrupted byte triggered Err — bytecheck is probably not running"
        );

        // Empty payload — should fail (alignment / size validation).
        let r: Result<CodeResp, _> = rkyv_decode(&[]);
        assert!(r.is_err(), "empty input must Err");
    }
}
