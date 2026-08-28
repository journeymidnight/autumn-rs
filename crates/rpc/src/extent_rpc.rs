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
// 12 = MSG_COMMIT_EC_SHARD — retired with the per-node commit phase. EC
//      conversion is copy-on-write: a shard is staged as an ADDITIVE
//      `extent-{id}.shard{i}` file and the manager's layout flip is the sole
//      commit point, so no node ever publishes a shard over its own `.dat`.
//      The number stays reserved — msg_type values are append-only.
// 13 = MSG_SYNC_EXTENT — retired when the fsync barrier was
//      folded into `start_write_batch`'s rotation-trigger `must_sync=true`
//      batch promotion. A later phase retires the rotation barrier in turn,
//      replacing both with the per-extent fsync coalescer + `MSG_SYNCED_LENGTH`
//      durability query so flush waits at flush-time, not at write-time.
/// Phase 2: query the extent-node's coalesced fsync high-water mark.
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
/// zero-copy read (EN -> PS). Same request shape as MSG_READ_BYTES
/// (ReadBytesReq), but the response is value-separable for recv-into-registered:
/// a V0 frame whose payload is `[bulk meta: code(1)+value_len(4)+value_crc32c(4)]
/// [raw value]` (autumn_rpc::client::ZC_META_LEN). The EN emits it as TWO Bytes
/// (header+meta, value) so the value Bytes aliases the pread buffer — no
/// `ReadBytesResp.encode()` + `Frame::encode()` double copy. The PS recvs the
/// value straight into a registered RegPool buffer via call_into_pooled. No
/// `end` field — VP-value reads (resolve_value) discard it. Falls back to
/// MSG_READ_BYTES for EC / chunked / TCP.
pub const MSG_READ_BYTES_BULK: u8 = 15;

/// chained append (large-payload replication pipeline). Payload:
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

/// fence an extent WITHOUT appending: raise the EN's per-extent
/// `owner_epoch` fence floor to `req.owner_epoch` (durably, across replicas).
///
/// Used on partition TAKEOVER to EAGERLY fence the previous owner at the new
/// owner's epoch (E_new) BEFORE the partition serves any request. Closes the
/// idle-takeover window: the EN floor is otherwise only raised by the new
/// owner's FIRST APPEND, so on an idle takeover it stayed at the OLD owner's
/// epoch (E_old) — and a paused-then-resumed "zombie writer" (the old owner)
/// whose in-flight append still carries E_old would pass the fence
/// (`E_old == stored`), land in the log extent, and be ACKed: a silent lost
/// update the new owner never sees.
///
/// The handler mirrors the APPEND fence prologue (`owner_epoch.fetch_max` +
/// `ensure_fence_durable`, fail-closed on a persist error) MINUS the write.
/// Raising the floor on a SEALED extent is a harmless no-op (a sealed tail
/// already rejects the zombie via the sealed check; fencing it is still safe),
/// so the handler deliberately does NOT special-case-reject sealed.
pub const MSG_FENCE_EXTENT: u8 = 17;

/// encode the chain prefix (`[n][len+addr]...`) for MSG_APPEND_CHAIN.
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

/// split a MSG_APPEND_CHAIN payload into `(chain, AppendReq bytes)`.
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
/// Phase 3 follow-up: `must_sync` byte removed. Every append is
/// always durable via the per-extent fsync coalescer (see
/// `extent_node.rs::Coalescer`); the handler unconditionally registers a
/// sync waiter and awaits coalesced `sync_data`. This byte previously
/// distinguished sync vs. nosync writes; now there is no nosync
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

/// ReadBytesRequest: 40 bytes (u64-offset widening — offset/length are byte
/// positions/spans in the extent, now > 4 GiB).
/// ```text
/// [extent_id: u64 LE][eversion: u64 LE][offset: u64 LE][length: u64 LE]
/// [payload_location: u8][pad: 3][shard_index: u32 LE]
/// ```
///
/// `payload_location` + `shard_index` NAME the file to serve from. Both are
/// required: a node can legitimately hold shard files at two indices (two
/// attempts, or a parity slot plus a data slot after a reassignment), so the
/// location alone does not identify a file. The server serves the named file or
/// answers `CODE_PAYLOAD_NOT_HERE` — never the other file.
///
/// `0, 0` = `(InDat, shard 0)`, which is what every pre-CoW caller means and
/// what the pre-existing wire form decoded to.
pub struct ReadBytesReq {
    pub extent_id: u64,
    pub eversion: u64,
    pub offset: u64,
    pub length: u64,
    pub payload_location: u8,
    pub shard_index: u32,
}

impl ReadBytesReq {
    /// Build a request naming `payload` on the target node.
    pub fn new(extent_id: u64, eversion: u64, offset: u64, length: u64, payload: PayloadRef) -> Self {
        Self {
            extent_id,
            eversion,
            offset,
            length,
            payload_location: payload.location.as_byte(),
            shard_index: payload.shard_index,
        }
    }

    pub fn payload_ref(&self) -> PayloadRef {
        PayloadRef::for_extent(self.payload_location, self.shard_index)
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(40);
        buf.put_u64_le(self.extent_id);
        buf.put_u64_le(self.eversion);
        buf.put_u64_le(self.offset);
        buf.put_u64_le(self.length);
        buf.put_u8(self.payload_location);
        buf.put_slice(&[0u8; 3]);
        buf.put_u32_le(self.shard_index);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 32 {
            return Err("read_bytes request too short");
        }
        let extent_id = data.get_u64_le();
        let eversion = data.get_u64_le();
        let offset = data.get_u64_le();
        let length = data.get_u64_le();
        // The payload-file selector is a trailing addition, so a 32-byte
        // request decodes to `(InDat, 0)` — exactly what it meant before the
        // field existed.
        let (payload_location, shard_index) = if data.len() >= 8 {
            let loc = data.get_u8();
            let _pad = data.get_uint_le(3);
            (loc, data.get_u32_le())
        } else {
            (PAYLOAD_LOCATION_IN_DAT, 0)
        };
        Ok(Self {
            extent_id,
            eversion,
            offset,
            length,
            payload_location,
            shard_index,
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
/// **Wire contract on `owner_epoch` (Tier 2, 2026-05-17):**
///
/// `owner_epoch` is an i64 but MUST be `> 0` on the wire — it carries the
/// caller's owner-lock claim. The EN's `handle_commit_length`:
///   - returns `CODE_INVALID_ARGUMENT` if `owner_epoch <= 0` (no
///     "probe sentinel" path — that escape hatch existed earlier
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

// (SyncExtentReq/Resp + MSG_SYNC_EXTENT were removed — the
// fsync barrier is now folded into `start_write_batch`'s rotation-trigger
// `must_sync=true` promotion in autumn-partition-server. A later phase
// then drops the rotation barrier altogether and adds MSG_SYNCED_LENGTH
// (below) for flush-time durability waits via the per-extent coalescer.)

// ── SyncedLength ─────────────────────────────────────────────────────────────

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

// ── FenceExtent (raise the owner_epoch fence floor, no append) ───────────────

/// FenceExtentRequest: 16 bytes.
/// `[extent_id: u64 LE][owner_epoch: i64 LE]`
///
/// Same fixed-header shape as `CommitLengthReq`, but a MUTATING fence op: the
/// EN raises `entry.owner_epoch` to `owner_epoch` (monotonic `fetch_max`) and
/// persists it durably (`ensure_fence_durable`); it neither reads nor returns
/// a length. `owner_epoch` MUST be `> 0` (a real acquired owner epoch); the EN
/// rejects `<= 0` as a protocol error. See the `MSG_FENCE_EXTENT` docstring.
pub struct FenceExtentReq {
    pub extent_id: u64,
    pub owner_epoch: i64,
}

impl FenceExtentReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u64_le(self.extent_id);
        buf.put_i64_le(self.owner_epoch);
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.len() < 16 {
            return Err("fence_extent request too short");
        }
        Ok(Self {
            extent_id: data.get_u64_le(),
            owner_epoch: data.get_i64_le(),
        })
    }
}

/// FenceExtentResponse: `[code: u8][message: utf8 …]`
///
/// `code`:
///   - `CODE_OK` — the floor is now `>= owner_epoch` AND durable on this
///     replica (fence installed, or already at/above it).
///   - `CODE_LOCKED_BY_OTHER` — a HIGHER owner already holds this extent
///     (`owner_epoch < stored`); the caller is stale. `fence_tail` treats
///     this as "fine — someone newer already owns it".
///   - `CODE_PRECONDITION` — fail-closed: the durable `.meta` persist failed
///     (disk marked offline) or the `.meta` is quarantined, so the fence is
///     NOT guaranteed durable on this replica.
/// `message` carries a human-readable reason for the non-OK codes.
pub struct FenceExtentResp {
    pub code: u8,
    pub message: String,
}

impl FenceExtentResp {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(1 + self.message.len());
        buf.put_u8(self.code);
        buf.extend_from_slice(self.message.as_bytes());
        buf.freeze()
    }

    pub fn decode(mut data: Bytes) -> Result<Self, &'static str> {
        if data.is_empty() {
            return Err("fence_extent response too short");
        }
        let code = data.get_u8();
        let message = String::from_utf8_lossy(&data).into_owned();
        Ok(Self { code, message })
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
/// switched from `from_bytes_unchecked` to the checked `from_bytes` —
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
/// Returned by ReadBytes when this node does not hold the payload file the
/// request NAMED (`payload_location` + `shard_index`).
///
/// It is deliberately distinct from `CODE_NOT_FOUND` ("no such extent here")
/// and never a silent fallback to whichever payload file this node does hold:
/// serving shard bytes as a whole value, or the reverse, is exactly the
/// corruption the location field exists to prevent. The client's response is to
/// refresh its layout from the manager and retry, which converges because the
/// manager is the authority on where an extent's payload lives.
pub const CODE_PAYLOAD_NOT_HERE: u8 = 7;

/// Which file on an extent-node holds an extent's payload.
///
/// Both forms can exist on one node at once (a staged shard beside a still-live
/// `.dat`), so the layout has to SAY which is authoritative rather than letting
/// each node infer its own role. The manager owns this decision; the EN obeys
/// it. Wire form is a `u8` — this codebase keeps rkyv-derived enums out of the
/// schema (same reason `ExtentOpKind` is a byte).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadLocation {
    /// `extent-{id}.dat` holds the payload: a full replica, or a shard written
    /// by the pre-CoW conversion scheme, which renamed its shard over `.dat`.
    /// This is the DEFAULT for every extent that predates the field.
    InDat,
    /// `extent-{id}.shard{i}` holds this node's shard; `.dat` is either absent
    /// or a redundant full copy awaiting cleanup.
    InShardFile,
}

pub const PAYLOAD_LOCATION_IN_DAT: u8 = 0;
pub const PAYLOAD_LOCATION_IN_SHARD_FILE: u8 = 1;

impl PayloadLocation {
    pub fn as_byte(self) -> u8 {
        match self {
            Self::InDat => PAYLOAD_LOCATION_IN_DAT,
            Self::InShardFile => PAYLOAD_LOCATION_IN_SHARD_FILE,
        }
    }

    /// An unknown byte decodes to `InDat`, never an error: it can only come
    /// from a peer that knows a location this build does not, and the safe
    /// reading of "I don't understand where the payload is" is the pre-existing
    /// layout — which is also what an absent field means.
    pub fn from_byte(b: u8) -> Self {
        match b {
            PAYLOAD_LOCATION_IN_SHARD_FILE => Self::InShardFile,
            _ => Self::InDat,
        }
    }
}

impl Default for PayloadLocation {
    fn default() -> Self {
        Self::InDat
    }
}

/// NAMES one payload file on one node: a location plus, when that location is
/// `InShardFile`, which shard index.
///
/// Both halves are needed — a node can hold shard files at two different
/// indices (two attempts, or a parity slot plus a data slot after a
/// reassignment), so the location alone does not identify a file. They travel
/// together as one value because every read path has to carry them through
/// several layers, and a pair that must not be split is better modelled than
/// remembered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PayloadRef {
    pub location: PayloadLocation,
    pub shard_index: u32,
}

impl PayloadRef {
    /// The whole payload in `extent-{id}.dat` — a full replica, or a shard the
    /// pre-CoW scheme renamed over `.dat`. The default, and what every extent
    /// that predates the layout field means.
    pub fn in_dat() -> Self {
        Self {
            location: PayloadLocation::InDat,
            shard_index: 0,
        }
    }

    pub fn shard(shard_index: u32) -> Self {
        Self {
            location: PayloadLocation::InShardFile,
            shard_index,
        }
    }

    /// Resolve against an extent's published layout: the extent says WHERE its
    /// payload lives, the caller says WHICH shard it is reading.
    ///
    /// The index is dropped for `InDat`, because there it names nothing: one
    /// `.dat` is the payload whatever slot the caller happens to be reading
    /// from. Normalising here — rather than at each comparison — keeps this
    /// value a true file identity, so the server can group two requests iff
    /// they name the same file. (Without it, replicated reads of one extent
    /// from different slots would carry different indices and stop batching,
    /// even though every one of them means `.dat`.)
    pub fn for_extent(payload_location: u8, shard_index: u32) -> Self {
        match PayloadLocation::from_byte(payload_location) {
            PayloadLocation::InDat => Self::in_dat(),
            PayloadLocation::InShardFile => Self::shard(shard_index),
        }
    }
}

/// Convert a u8 code from binary wire format to autumn_rpc::StatusCode.
pub fn code_to_status(code: u8) -> StatusCode {
    match code {
        CODE_OK => StatusCode::Ok,
        CODE_NOT_FOUND => StatusCode::NotFound,
        CODE_PRECONDITION => StatusCode::FailedPrecondition,
        CODE_EVERSION_MISMATCH => StatusCode::FailedPrecondition,
        CODE_PAYLOAD_NOT_HERE => StatusCode::FailedPrecondition,
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
        CODE_LOCKED_BY_OTHER => "fenced by a newer owner (stale owner_epoch)",
        CODE_PAYLOAD_NOT_HERE => "payload not in the named file on this node",
        // Callers MUST print the numeric code alongside this string. Rendering
        // every unnamed code as one generic word collapses distinct refusals
        // into the same useless message — a stale-fence rejection reading as
        // "error" is what hid a permanently wedged EC conversion behind an
        // ordinary-looking retry log.
        _ => "unrecognised status code",
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
    /// Where each member node keeps this extent's payload
    /// (`PayloadLocation::from_byte`). `0` = `InDat`, which is both the default
    /// and what every extent predating this field means — the manager stores it
    /// beside `extents/<id>` rather than inside it, so old records decode
    /// unchanged and simply read as `InDat`.
    ///
    /// `ec_converted` says the extent's bytes ARE shards; this says which FILE
    /// they live in. The pre-CoW scheme renamed the shard over `.dat`, so a
    /// legacy converted extent is `ec_converted = true, InDat` and keeps
    /// working with no backfill.
    pub payload_location: u8,
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

/// One finished EC conversion, reported by the coordinator EN on its next `df`.
/// Deliberately MINIMAL: the manager does NOT trust these fields as the layout
/// to write — it applies the assignment PINNED in the etcd `ConvertToEc` marker
/// and uses this only to (a) learn the conversion finished and (b) cross-check
/// `new_eversion`. A mismatch is refused fail-loud rather than applied.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct EcConvertDone {
    pub extent_id: u64,
    pub new_eversion: u64,
    /// Which ATTEMPT produced this report. Echoed back from
    /// `ConvertToEcReq.attempt_nonce`; the manager refuses to apply a report
    /// whose nonce is not the live marker's.
    ///
    /// `new_eversion` alone cannot separate attempts: it is `live + 1`, and an
    /// abandoned attempt never bumps the extent's eversion, so a re-issued
    /// attempt is handed the SAME value. A stale report from a previous
    /// attempt's coordinator would then match the current marker and fire the
    /// layout flip while the current attempt has staged nothing — after which
    /// cleanup deletes the last full replicas. `0` = a legacy marker created
    /// before nonces existed (accepted only against a marker that also has 0).
    pub attempt_nonce: u64,
}

/// Df (disk-free + recovery heartbeat) request.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DfReq {
    pub tasks: Vec<RecoveryTask>,
    pub disk_ids: Vec<u64>,
}

/// Df response: completed recovery tasks + per-disk stats.
///
/// M1b — the trailing three fields ECHO the EN's own live
/// identity so the manager's `node_health_loop` (the single df caller) can
/// self-heal stored-location drift and detect pod-IP reuse (a DIFFERENT process
/// answering at a stored address → `node_uuid` mismatch → refuse to heal). All
/// three are empty when the EN was not launched with `--advertise` (test /
/// pre-M1 deployments) → the manager skips the echo checks. Appended (not
/// inserted) so the rkyv layout stays compatible with `manager_rpc::ExtDfResp`.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DfResp {
    pub done_tasks: Vec<RecoveryTaskDone>,
    /// Completed EC conversions, drained on each `df` (same at-most-once
    /// heartbeat channel as `done_tasks`). The coordinator EN ACKs
    /// `MSG_CONVERT_TO_EC` immediately and encodes in the BACKGROUND, so this is
    /// how the manager learns the 2PC actually finished — it then applies the
    /// layout from the etcd marker's PINNED assignment. A lost report converges
    /// via re-dispatch: the EN's idempotency guard re-reports (adopt).
    pub ec_done: Vec<EcConvertDone>,
    /// (disk_id, DiskStatus) pairs (HashMap not used for rkyv compat).
    pub disk_status: Vec<(u64, DiskStatus)>,
    /// M1b: the EN's stable identity (empty = not registered).
    pub node_uuid: String,
    /// M1b: the address the EN advertises (empty = not registered).
    pub advertise_addr: String,
    /// M1b: the shard ports this EN process actually binds.
    pub shard_ports: Vec<u16>,
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
///
/// MIRRORED by `manager_rpc::ExtDeleteExtentReq`, which is what the manager
/// actually encodes with — the two definitions must stay byte-identical or the
/// decode silently yields garbage. Edit both.
///
/// Sent by the manager after an extent's refcount drops to 0
/// (`punch_holes` / `truncate` paths). Idempotent: a missing extent on the
/// receiving node returns `CODE_OK`, so manager retries are safe.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DeleteExtentReq {
    pub extent_id: u64,
    /// WHICH node this delete is for. The one RPC that destroys data used to
    /// name only an extent id and execute for whoever answered at that
    /// address — less identity checking than the read-only `df`, which already
    /// echoes a uuid to catch imposters.
    ///
    /// Within one cluster an id is never reused, so a late delete is at worst
    /// an idempotent no-op. ACROSS clusters ids restart from small integers: if
    /// cluster A is torn down while its manager still holds persisted delete
    /// retries, and cluster B comes up on the same host and ports (shared-host
    /// port bases, pod-IP reuse), A's retry unlinks B's LIVE extent with the
    /// matching id. The target echoes its own identity here to refuse that.
    ///
    /// Empty = unspecified (a legacy persisted retry entry, or a test caller):
    /// the receiver skips the check, matching `classify_df_echo`.
    pub node_uuid: String,
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
    /// Tier 2: owner-lock owner_epoch propagated from manager.
    /// Coord puts this into every `WriteShardReq.owner_epoch` and
    /// `CommitEcShardReq.owner_epoch` so a fenced ex-coord whose in-flight
    /// 2PC continues against bumped revisions on remote ENs is
    /// rejected with `CODE_LOCKED_BY_OTHER`. `0` = legacy no-fence.
    pub owner_epoch: i64,
    /// Identity of THIS conversion attempt — the etcd revision that created
    /// the manager's marker, so it is unique per attempt and monotonic across
    /// them. The coordinator forwards it into every `WriteShardReq` and echoes
    /// it in `EcConvertDone`; see `EcConvertDone::attempt_nonce` for what it
    /// defends against. `0` = legacy / memory-only.
    pub attempt_nonce: u64,
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

/// WriteShardRequest: [extent_id: u64 LE][shard_index: u32 LE][sealed_length: u64 LE][eversion: u64 LE][owner_epoch: i64 LE][shard_offset: u64 LE][attempt_nonce: u64 LE][payload...]
///
/// `eversion` is the post-EC eversion the manager has decided on. The
/// receiving extent node bumps `entry.eversion` to this value when it
/// installs the shard, so subsequent ReadBytes requests with a stale
/// (pre-EC) eversion are rejected with `CODE_EVERSION_MISMATCH`.
///
/// `owner_epoch` carries the owner-lock owner_epoch the caller
/// claims. When `owner_epoch > 0` the extent-node refuses with
/// `CODE_LOCKED_BY_OTHER` if `owner_epoch < entry.owner_epoch` — same
/// fence model as the append path. `owner_epoch = 0` means "no fence
/// requested" (wire-compat).
///
/// `shard_offset` (chunked EC convert): the byte offset WITHIN the shard
/// at which `payload` is written into the staging `.ec.dat`. EC convert
/// streams a shard as a sequence of stripes (each `payload` ≤ a chunk) so a
/// single RPC never exceeds the frame `payload_len: u32` ceiling — load-bearing
/// once an extent (hence a shard) can exceed 4 GiB. `shard_offset = 0` with the
/// whole shard as `payload` is the degenerate single-stripe form.
///
/// `attempt_nonce` identifies the conversion attempt this stripe belongs to
/// (see `ConvertToEcReq::attempt_nonce`). Nonces are etcd revisions, hence
/// MONOTONIC, which is what lets a receiver refuse a stripe from an attempt
/// older than the one it is already staging — a coordinator whose marker was
/// released keeps streaming into the same staging file that its successor is
/// now filling.
pub const WRITE_SHARD_HEADER_LEN: usize = 52;

pub struct WriteShardReq {
    pub extent_id: u64,
    pub shard_index: u32,
    pub sealed_length: u64,
    pub eversion: u64,
    pub owner_epoch: i64,
    pub shard_offset: u64,
    pub attempt_nonce: u64,
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
        buf.put_u64_le(self.attempt_nonce);
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
        let attempt_nonce = data.get_u64_le();
        let payload = data;
        Ok(Self {
            extent_id,
            shard_index,
            sealed_length,
            eversion,
            owner_epoch,
            shard_offset,
            attempt_nonce,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_extent_req_round_trip() {
        let req = DeleteExtentReq {
            extent_id: 0xdead_beef_cafe_0042,
            node_uuid: "uuid-target-node".to_string(),
        };
        let bytes = rkyv_encode(&req);
        let decoded: DeleteExtentReq = rkyv_decode(&bytes).expect("decode");
        assert_eq!(decoded.extent_id, req.extent_id);
        assert_eq!(decoded.node_uuid, req.node_uuid);

        // The manager encodes with its MIRROR of this struct and the node
        // decodes with this one, so the two layouts must agree byte for byte.
        // Decoding the mirror's bytes here is what would catch a field added
        // to only one side.
        let mirrored = rkyv_encode(&crate::manager_rpc::ExtDeleteExtentReq {
            extent_id: req.extent_id,
            node_uuid: req.node_uuid.clone(),
        });
        let cross: DeleteExtentReq = rkyv_decode(&mirrored).expect("mirror decode");
        assert_eq!(cross.extent_id, req.extent_id);
        assert_eq!(cross.node_uuid, req.node_uuid);
    }

    #[test]
    fn fence_extent_req_round_trip() {
        let req = FenceExtentReq {
            extent_id: 0x0102_0304_0506_0708,
            owner_epoch: 14396,
        };
        let decoded = FenceExtentReq::decode(req.encode()).expect("decode");
        assert_eq!(decoded.extent_id, req.extent_id);
        assert_eq!(decoded.owner_epoch, req.owner_epoch);
    }

    #[test]
    fn fence_extent_resp_round_trip() {
        // OK: empty message.
        let ok = FenceExtentResp {
            code: CODE_OK,
            message: String::new(),
        };
        let d = FenceExtentResp::decode(ok.encode()).expect("decode ok");
        assert_eq!(d.code, CODE_OK);
        assert!(d.message.is_empty());

        // Non-OK carries a readable message.
        let locked = FenceExtentResp {
            code: CODE_LOCKED_BY_OTHER,
            message: "higher owner holds extent 7".to_string(),
        };
        let d = FenceExtentResp::decode(locked.encode()).expect("decode locked");
        assert_eq!(d.code, CODE_LOCKED_BY_OTHER);
        assert_eq!(d.message, "higher owner holds extent 7");

        // Truncated (empty) response is a decode error, not a panic.
        assert!(FenceExtentResp::decode(Bytes::new()).is_err());
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

    /// rkyv_decode rejects malformed input via bytecheck instead of UB.
    /// Previously this used `from_bytes_unchecked` and a corrupted payload
    /// (flipped bits past TCP CRC, mixed-version cluster, etc.) caused
    /// out-of-bounds reads or pointer dereferences into arbitrary memory.
    /// Now the checked decoder runs validation first and returns Err.
    #[test]
    fn rkyv_decode_rejects_malformed() {
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
mod chain_codec_tests {
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

    #[test]
    fn read_bytes_req_roundtrips_the_named_payload_file() {
        let req = ReadBytesReq::new(9, 4, 100, 200, PayloadRef::shard(3));
        let got = ReadBytesReq::decode(req.encode()).unwrap();
        assert_eq!(got.extent_id, 9);
        assert_eq!(got.eversion, 4);
        assert_eq!(got.offset, 100);
        assert_eq!(got.length, 200);
        assert_eq!(got.payload_ref(), PayloadRef::shard(3));
    }

    /// A request written before the payload selector existed is 32 bytes, and
    /// means `.dat` — the same thing an explicit `InDat` means. Decoding it as
    /// anything else would make an upgrade reinterpret in-flight reads.
    #[test]
    fn a_request_without_the_selector_decodes_as_in_dat() {
        let mut legacy = BytesMut::with_capacity(32);
        legacy.put_u64_le(9);
        legacy.put_u64_le(4);
        legacy.put_u64_le(100);
        legacy.put_u64_le(200);
        let got = ReadBytesReq::decode(legacy.freeze()).unwrap();
        assert_eq!(got.payload_ref(), PayloadRef::in_dat());
        assert_eq!(got.length, 200);
    }

    /// `InDat` names ONE file whatever slot the caller read from, so the shard
    /// index must not survive into the identity — otherwise replicated reads of
    /// one extent from different slots would look like different files and the
    /// server would stop batching them.
    #[test]
    fn in_dat_has_one_identity_regardless_of_slot() {
        assert_eq!(
            PayloadRef::for_extent(PAYLOAD_LOCATION_IN_DAT, 3),
            PayloadRef::for_extent(PAYLOAD_LOCATION_IN_DAT, 0)
        );
        assert_ne!(
            PayloadRef::for_extent(PAYLOAD_LOCATION_IN_SHARD_FILE, 3),
            PayloadRef::for_extent(PAYLOAD_LOCATION_IN_SHARD_FILE, 0),
            "two shard files on one node are different files"
        );
    }

    #[test]
    fn an_unknown_location_byte_reads_as_in_dat() {
        assert_eq!(PayloadLocation::from_byte(200), PayloadLocation::InDat);
    }
}
