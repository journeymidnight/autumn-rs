//! Wire codec for ExtentService RPCs over autumn-rpc.
//!
//! Hot-path messages (Append, ReadBytes, CommitLength) use fixed-size binary
//! headers for minimum overhead. Other RPCs use rkyv zero-copy serialization.
//! Large-payload RPCs (CopyExtent, WriteShard) use fixed binary headers +
//! raw payload to avoid serialization copies.

use crate::StatusCode;
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
/// owner_epoch check, no mutation of `owner_epoch`, no `.meta`
/// rewrite. Two call sites today:
///   - `manager/src/recovery.rs`'s `recovery_dispatch_loop`
///     liveness probe (uses `code == CODE_OK` to decide whether
///     to fire `dispatch_recovery_task`; ignores `length`).
///   - `autumn-client info`'s open-extent live-length display
///     (uses `length` to render `commit_length` for streams
///     where no PS-owner context is available).
/// External (non-manager) callers MUST NOT use this RPC for
/// seal/consensus reads — those go through `MSG_COMMIT_LENGTH`
/// with a real owner_epoch so the EN's fence handover side-effect
/// fires (see `extent_node.rs::handle_commit_length`).
pub const MSG_PROBE_EXTENT: u8 = 14;
/// F216-E zero-copy read (EN -> PS). Same request shape as MSG_READ_BYTES
/// (ReadBytesReq), but the response is value-separable for recv-into-registered:
/// a V0 frame whose payload is `[ZC meta: code(1)+value_len(4)+value_crc32c(4)]
/// [raw value]` (autumn_rpc::client::ZC_META_LEN). The EN emits it as TWO Bytes
/// (header+meta, value) so the value Bytes aliases the pread buffer — no
/// `ReadBytesResp.encode()` + `Frame::encode()` double copy. The PS recvs the
/// value straight into a registered RegPool buffer via call_into_pooled. No
/// `end` field — VP-value reads (resolve_value) discard it. Falls back to
/// MSG_READ_BYTES for EC / chunked / TCP.
pub const MSG_READ_BYTES_ZC: u8 = 15;

/// F260 — chained append (large-payload replication pipeline). Payload:
///
/// ```text
/// [n_chain: u8][ per hop: addr_len u16 LE + addr utf8 ]...[AppendReq bytes]
/// ```
///
/// The receiving EN (1) decodes the chain prefix, (2) SUBMITS the forward of
/// `[chain minus itself][same AppendReq]` to `chain[0]` synchronously in
/// frame-arrival order (per-extent ordering: arrival order on this socket =
/// the writer's lease order, and the forward submit inherits it), (3) runs
/// the local append, (4) acks only when BOTH the local write and the
/// downstream ack succeed. `n_chain == 0` behaves exactly like MSG_APPEND.
/// Every hop validates its own owner_epoch / eversion / commit as usual —
/// fencing and commit-truncation are per-replica invariants, unchanged.
pub const MSG_APPEND_CHAIN: u8 = 16;

/// F260: encode the chain prefix (`[n][len+addr]...`) for MSG_APPEND_CHAIN.
/// The full request is `[prefix][AppendReq::encode_header()][payload...]` —
/// senders use vectored writes so the payload stays zero-copy.
pub fn encode_chain_prefix(chain: &[String]) -> Bytes {
    let mut buf = BytesMut::with_capacity(1 + chain.iter().map(|a| 2 + a.len()).sum::<usize>());
    buf.put_u8(chain.len() as u8);
    for a in chain {
        buf.put_u16_le(a.len() as u16);
        buf.extend_from_slice(a.as_bytes());
    }
    buf.freeze()
}

/// F260: split a MSG_APPEND_CHAIN payload into `(chain, AppendReq bytes)`.
/// The AppendReq remainder is returned as Bytes (zero-copy slice) so the
/// forward path can re-send it without re-encoding.
pub fn decode_chain_prefix(mut data: Bytes) -> Result<(Vec<String>, Bytes), &'static str> {
    if data.is_empty() {
        return Err("chain append too short");
    }
    let n = data.get_u8() as usize;
    let mut chain = Vec::with_capacity(n);
    for _ in 0..n {
        if data.len() < 2 {
            return Err("chain addr truncated");
        }
        let l = data.get_u16_le() as usize;
        if data.len() < l {
            return Err("chain addr truncated");
        }
        let addr = data.split_to(l);
        chain.push(
            std::str::from_utf8(&addr)
                .map_err(|_| "chain addr not utf8")?
                .to_string(),
        );
    }
    Ok((chain, data))
}
// MSG_TYPE_PING = 0xFF is reserved by autumn-rpc for heartbeat

// ── Append (hot path) ────────────────────────────────────────────────────────

/// Fixed binary header for AppendRequest: 28 bytes + raw payload.
/// ```text
/// [extent_id: u64 LE][eversion: u64 LE][commit: u64 LE][owner_epoch: i64 LE]
/// [payload bytes...]
/// ```
///
/// F178 Phase 3 follow-up: `must_sync` byte removed. Every append is
/// always durable via the per-extent fsync coalescer (see
/// `extent_node.rs::Coalescer`); the handler unconditionally registers a
/// sync waiter and awaits coalesced `sync_data`. Pre-F178 this byte
/// distinguished sync vs. nosync writes; post-F178 there is no nosync
/// path. Wire format shrinks by 1 byte.
// u64-offset widening: commit is a byte position in the extent (up to
// max_extent_size, now > 4 GiB), so it is u64. Header = 8+8+8(commit)+8 = 32.
pub const APPEND_HEADER_LEN: usize = 32;

pub struct AppendReq {
    pub extent_id: u64,
    pub eversion: u64,
    pub commit: u64,
    pub owner_epoch: i64,
    pub payload: Bytes,
}

impl AppendReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(APPEND_HEADER_LEN + self.payload.len());
        buf.put_u64_le(self.extent_id);
        buf.put_u64_le(self.eversion);
        buf.put_u64_le(self.commit);
        buf.put_i64_le(self.owner_epoch);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    /// Encode only the 32-byte header (for vectored writes — payload sent separately).
    pub fn encode_header(extent_id: u64, eversion: u64, commit: u64, owner_epoch: i64) -> Bytes {
        let mut buf = BytesMut::with_capacity(APPEND_HEADER_LEN);
        buf.put_u64_le(extent_id);
        buf.put_u64_le(eversion);
        buf.put_u64_le(commit);
        buf.put_i64_le(owner_epoch);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < APPEND_HEADER_LEN {
            return Err("append request too short");
        }
        let extent_id = data.get_u64_le();
        let eversion = data.get_u64_le();
        let commit = data.get_u64_le();
        let owner_epoch = data.get_i64_le();
        let payload = data;
        Ok(Self {
            extent_id,
            eversion,
            commit,
            owner_epoch,
            payload,
        })
    }
}

/// Fixed binary AppendResponse: 17 bytes.
/// ```text
/// [code: u8][offset: u64 LE][end: u64 LE]
/// ```
/// offset/end are byte positions in the extent (u64-offset widening).
pub struct AppendResp {
    pub code: u8,
    pub offset: u64,
    pub end: u64,
}

impl AppendResp {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(17);
        buf.put_u8(self.code);
        buf.put_u64_le(self.offset);
        buf.put_u64_le(self.end);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 17 {
            return Err("append response too short");
        }
        Ok(Self {
            code: data.get_u8(),
            offset: data.get_u64_le(),
            end: data.get_u64_le(),
        })
    }
}

// ── ReadBytes (hot path) ─────────────────────────────────────────────────────

/// ReadBytesRequest: 32 bytes (u64-offset widening — offset/length are byte
/// positions/spans in the extent, now > 4 GiB).
/// ```text
/// [extent_id: u64 LE][eversion: u64 LE][offset: u64 LE][length: u64 LE]
/// ```
pub struct ReadBytesReq {
    pub extent_id: u64,
    pub eversion: u64,
    pub offset: u64,
    pub length: u64,
}

impl ReadBytesReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(32);
        buf.put_u64_le(self.extent_id);
        buf.put_u64_le(self.eversion);
        buf.put_u64_le(self.offset);
        buf.put_u64_le(self.length);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 32 {
            return Err("read_bytes request too short");
        }
        Ok(Self {
            extent_id: data.get_u64_le(),
            eversion: data.get_u64_le(),
            offset: data.get_u64_le(),
            length: data.get_u64_le(),
        })
    }
}

/// ReadBytesResponse: [code: u8][end: u64 LE][payload bytes...]
pub struct ReadBytesResp {
    pub code: u8,
    pub end: u64,
    pub payload: Bytes,
}

impl ReadBytesResp {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(9 + self.payload.len());
        buf.put_u8(self.code);
        buf.put_u64_le(self.end);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 9 {
            return Err("read_bytes response too short");
        }
        let code = data.get_u8();
        let end = data.get_u64_le();
        let payload = data;
        Ok(Self { code, end, payload })
    }
}

// ── CommitLength (hot path) ──────────────────────────────────────────────────

/// CommitLengthRequest: 16 bytes.
/// [extent_id: u64 LE][owner_epoch: i64 LE]
///
/// **Wire contract on `owner_epoch` (post-F210-H3 Tier 2, 2026-05-17):**
///
/// `owner_epoch` is an i64 but MUST be `> 0` on the wire — it carries the
/// caller's owner-lock claim. The EN's `handle_commit_length`:
///   - returns `CODE_INVALID_ARGUMENT` if `owner_epoch <= 0` (no
///     "probe sentinel" path — that escape hatch existed pre-F210-H2
///     and broke fence semantics; see `MSG_PROBE_EXTENT` instead);
///   - returns `CODE_LOCKED_BY_OTHER` if `owner_epoch < entry.owner_epoch`
///     (caller is a stale owner; reject);
///   - if `owner_epoch >= entry.owner_epoch`, returns the length WITHOUT
///     mutating `owner_epoch` (CHECK-ONLY; the three-concepts rule).
///
/// commit_length is a length PROBE + stale-owner fence CHECK. It does NOT
/// perform fence handover — write-ownership is established EXCLUSIVELY by
/// the APPEND path (`handle_append*` bumps `owner_epoch` when a
/// higher-owner_epoch owner writes). The old "owner_epoch > owner_epoch →
/// bump + persist .meta" handover was removed 2026-05-29: the manager's
/// control-plane probes (`admin-merge:<v>:<s>` lock in
/// `handle_merge_partitions`, the seal in `handle_stream_alloc_extent`)
/// carry a high global owner-owner_epoch counter that does NOT represent a
/// new PS write-owner; bumping `owner_epoch` on such a probe fenced out
/// the LIVE PS (which never re-reads the climbing counter) → poison.
///
/// This RPC is the canonical seal-consensus primitive. Manager's
/// `commit_length_on_node(addr, eid, owner_epoch)` helper forwards the
/// caller's validated `req.owner_epoch` through `handle_check_commit_length`
/// and `handle_stream_alloc_extent`. Manager probes WITHOUT an owner
/// context (recovery liveness, `autumn-client info` display) use
/// `MSG_PROBE_EXTENT` instead — that RPC returns the same `(code, length)`
/// shape but skips the fence CHECK entirely.
pub struct CommitLengthReq {
    pub extent_id: u64,
    pub owner_epoch: i64,
}

impl CommitLengthReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u64_le(self.extent_id);
        buf.put_i64_le(self.owner_epoch);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 16 {
            return Err("commit_length request too short");
        }
        Ok(Self {
            extent_id: data.get_u64_le(),
            owner_epoch: data.get_i64_le(),
        })
    }
}

/// CommitLengthResponse: 9 bytes.
/// [code: u8][length: u64 LE]
pub struct CommitLengthResp {
    pub code: u8,
    pub length: u64,
}

impl CommitLengthResp {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(9);
        buf.put_u8(self.code);
        buf.put_u64_le(self.length);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 9 {
            return Err("commit_length response too short");
        }
        Ok(Self {
            code: data.get_u8(),
            length: data.get_u64_le(),
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

/// ProbeExtentResponse: 9 bytes. `[code: u8][length: u64 LE]`. Same shape
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
/// Returned when `header.owner_epoch < owner_epoch` — a newer owner has taken the lock.
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
    /// Mirror of `MgrExtentInfo.sealed`: the authoritative "is this extent
    /// sealed (immutable)" flag. NOT `sealed_length > 0`, because an
    /// authoritative empty seal is `sealed = true, sealed_length = 0` (e.g. a
    /// CoW-shared empty tail frozen by split/merge). `ensure_tail_initialised`
    /// allocs a fresh tail when this is set, so a child never appends to a
    /// shared sealed tail. `sealed_length` stays the read-bound.
    pub sealed: bool,
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
    /// Raw filesystem capacity (statvfs `f_blocks * f_frsize`).
    pub total: u64,
    /// Raw filesystem free bytes (statvfs `f_bavail * f_frsize`).
    pub free: u64,
    pub online: bool,
    /// Cluster-df: sum of THIS disk's live extent file lengths
    /// (`ExtentEntry.len`) — the REAL autumn physical footprint on this
    /// disk: replicas count their full copy, EC shards count shard size,
    /// open tails count live appended bytes. The manager sums these across
    /// all disks/nodes into `physical_used` with no amplification formula.
    pub extent_bytes: u64,
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
    /// F211-D Tier 2: owner-lock owner_epoch propagated from manager.
    /// Coord puts this into every `WriteShardReq.owner_epoch` and
    /// `CommitEcShardReq.owner_epoch` so a fenced ex-coord whose in-flight
    /// 2PC continues against bumped revisions on remote ENs is
    /// rejected with `CODE_LOCKED_BY_OTHER`. `0` = legacy no-fence.
    pub owner_epoch: i64,
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

/// WriteShardRequest: [extent_id: u64 LE][shard_index: u32 LE][sealed_length: u64 LE][eversion: u64 LE][owner_epoch: i64 LE][shard_offset: u64 LE][payload...]
///
/// `eversion` is the post-EC eversion the manager has decided on. The
/// receiving extent node bumps `entry.eversion` to this value when it
/// installs the shard, so subsequent ReadBytes requests with a stale
/// (pre-EC) eversion are rejected with `CODE_EVERSION_MISMATCH`.
///
/// `owner_epoch` (F211-D) carries the owner-lock owner_epoch the caller
/// claims. When `owner_epoch > 0` the extent-node refuses with
/// `CODE_LOCKED_BY_OTHER` if `owner_epoch < entry.owner_epoch` — same
/// fence model as the append path. `owner_epoch = 0` means "no fence
/// requested" (pre-F211-D wire-compat).
///
/// `shard_offset` (chunked EC convert): the byte offset WITHIN the shard
/// at which `payload` is written into the staging `.ec.dat`. EC convert
/// streams a shard as a sequence of stripes (each `payload` ≤ a chunk) so a
/// single RPC never exceeds the frame `payload_len: u32` ceiling — load-bearing
/// once an extent (hence a shard) can exceed 4 GiB. `shard_offset = 0` with the
/// whole shard as `payload` is the degenerate single-stripe form.
pub const WRITE_SHARD_HEADER_LEN: usize = 44;

pub struct WriteShardReq {
    pub extent_id: u64,
    pub shard_index: u32,
    pub sealed_length: u64,
    pub eversion: u64,
    pub owner_epoch: i64,
    pub shard_offset: u64,
    pub payload: Bytes,
}

impl WriteShardReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(WRITE_SHARD_HEADER_LEN + self.payload.len());
        buf.put_u64_le(self.extent_id);
        buf.put_u32_le(self.shard_index);
        buf.put_u64_le(self.sealed_length);
        buf.put_u64_le(self.eversion);
        buf.put_i64_le(self.owner_epoch);
        buf.put_u64_le(self.shard_offset);
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
        let owner_epoch = data.get_i64_le();
        let shard_offset = data.get_u64_le();
        let payload = data;
        Ok(Self {
            extent_id,
            shard_index,
            sealed_length,
            eversion,
            owner_epoch,
            shard_offset,
            payload,
        })
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

/// CommitEcShardRequest: [extent_id: u64 LE][sealed_length: u64 LE][eversion: u64 LE][owner_epoch: i64 LE]
///
/// F211-D: `owner_epoch` fence — see `WriteShardReq` for semantics.
pub const COMMIT_EC_SHARD_HEADER_LEN: usize = 32;

pub struct CommitEcShardReq {
    pub extent_id: u64,
    pub sealed_length: u64,
    pub eversion: u64,
    pub owner_epoch: i64,
}

impl CommitEcShardReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(COMMIT_EC_SHARD_HEADER_LEN);
        buf.put_u64_le(self.extent_id);
        buf.put_u64_le(self.sealed_length);
        buf.put_u64_le(self.eversion);
        buf.put_i64_le(self.owner_epoch);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < COMMIT_EC_SHARD_HEADER_LEN {
            return Err("commit_ec_shard request too short");
        }
        let extent_id = data.get_u64_le();
        let sealed_length = data.get_u64_le();
        let eversion = data.get_u64_le();
        let owner_epoch = data.get_i64_le();
        Ok(Self {
            extent_id,
            sealed_length,
            eversion,
            owner_epoch,
        })
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
        let req = DeleteExtentReq {
            extent_id: 0xdead_beef_cafe_0042,
        };
        let bytes = rkyv_encode(&req);
        let decoded: DeleteExtentReq = rkyv_decode(&bytes).expect("decode");
        assert_eq!(decoded.extent_id, req.extent_id);
    }

    #[test]
    fn delete_extent_resp_uses_generic_code_resp() {
        let resp = CodeResp {
            code: CODE_OK,
            message: String::new(),
        };
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

#[cfg(test)]
mod f260_chain_codec_tests {
    use super::*;

    #[test]
    fn chain_prefix_round_trips() {
        let chain = vec!["127.0.0.1:20002".to_string(), "[::1]:20003".to_string()];
        let req = AppendReq {
            extent_id: 7,
            eversion: 3,
            commit: 4096,
            owner_epoch: 9,
            payload: Bytes::from_static(b"hello-world"),
        };
        let mut full = BytesMut::new();
        full.extend_from_slice(&encode_chain_prefix(&chain));
        full.extend_from_slice(&req.encode());
        let (got_chain, rest) = decode_chain_prefix(full.freeze()).unwrap();
        assert_eq!(got_chain, chain);
        let got = AppendReq::decode(rest).unwrap();
        assert_eq!(got.extent_id, 7);
        assert_eq!(got.commit, 4096);
        assert_eq!(&got.payload[..], b"hello-world");
    }

    #[test]
    fn empty_chain_is_plain_append() {
        let req = AppendReq {
            extent_id: 1,
            eversion: 1,
            commit: 0,
            owner_epoch: 1,
            payload: Bytes::from_static(b"x"),
        };
        let mut full = BytesMut::new();
        full.extend_from_slice(&encode_chain_prefix(&[]));
        full.extend_from_slice(&req.encode());
        let (chain, rest) = decode_chain_prefix(full.freeze()).unwrap();
        assert!(chain.is_empty());
        assert_eq!(AppendReq::decode(rest).unwrap().extent_id, 1);
    }
}
