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
// MSG_STREAM_PUT (0x46) REMOVED 2026-06-11: zero callers once
// stripe-writes moved client-side, and its PS handler bypassed BOTH the inline
// value cap AND the BUG-LEASE-2 fence check (an unfenced unbounded-write
// backdoor). 0x46 stays RESERVED (reserved-constants pattern) to prevent
// accidental re-use against in-flight old binaries.
//   pub const MSG_STREAM_PUT: u8 = 0x46;  // RESERVED, removed
pub const MSG_MAINTENANCE: u8 = 0x47;
pub const MSG_GET_DISCARDS: u8 = 0x48;

/// (PS slice): the cluster-MUTATING PS ops gated on the admin
/// token — split and every maintenance op (compact / auto-gc / force-gc /
/// flush, all under `MSG_MAINTENANCE`). Token carried as the SAME payload prefix
/// the manager slice uses (`manager_rpc::{prefix,strip}_admin_token`). The data
/// plane (PUT/GET/DELETE/HEAD/RANGE/*_ZC) is never gated here — that is Layer-A/B.
/// Senders that must prefix: `autumn-op` (operator) AND the MANAGER itself (its
/// auto-policy controller drives split + gc/compact, and merge drives flush, all
/// as manager→PS calls).
#[inline]
pub fn is_admin_ps_msg(msg_type: u8) -> bool {
    matches!(msg_type, MSG_SPLIT_PART | MSG_MAINTENANCE)
}

// server-side multipart upload was REMOVED.
// Wire constants 0x49-0x4C remain RESERVED to prevent accidental re-use
// while old binaries with handlers may still be in flight in production
// rolling deploys. Stripe-write is now pure client-side (Ceph
// striperados pattern): each chunk is a normal Put under a reserved
// 0xff-prefixed key namespace, and the user key holds a 29-byte Meta
// blob. See `crates/client/src/lib.rs::PutStreamHandle` + `GetStream`.
//
//   pub const MSG_PUT_BEGIN:  u8 = 0x49;  // RESERVED (was multipart upload)
//   pub const MSG_PUT_CHUNK:  u8 = 0x4A;  // RESERVED (was multipart upload)
//   pub const MSG_PUT_COMMIT: u8 = 0x4B;  // RESERVED (was multipart upload)
//   pub const MSG_PUT_ABORT:  u8 = 0x4C;  // RESERVED (was multipart upload)

// partition merge — sent to the SURVIVOR's PS.
pub const MSG_MERGE_PART: u8 = 0x4D;

// PrepareMerge-style freeze. CLI/manager sends to BOTH the survivor and
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

// 0x4F retired with the vp_table_refs removal (was MSG_PULL_VP_REFS,
// manager→PS vp_refs pull). Extent retention is now driven by
// `refs` (stream membership) alone — see manager `extent_can_delete`.

// zero-copy GET. Same request shape as MSG_GET (GetReq), but the response
// is value-separable for recv-into-registered-dest: a CRC-less frame whose
// payload is `[bulk meta: code(1)+value_len(4)+reserved(4)][raw value]` (see
// autumn_rpc::client::ZC_META_LEN; the reserved field held a value crc32c
// before it was removed). The client uses RpcClient::call_into_dest to
// land the value straight in its registered buffer (sglang page). Generic
// MSG_GET keeps the rkyv GetResp form.
pub const MSG_GET_BULK: u8 = 0x50;

// zero-copy PUT (client -> PS write hop). Same semantics as MSG_PUT but
// value-separable so the client sends the value as its OWN iovec straight from
// the (registered) sglang source pool — no `value.to_vec()`/clone/rkyv copy on
// the client, and the PS slices the value zero-copy out of the frame (vs an
// rkyv decode copy). Wire payload:
//   [part_id: u64 LE][region_epoch: u64 LE][expires_at: u64 LE][key_len: u32 LE]
//   [key: key_len bytes][value: rest]
// Sent via RpcClient::call_vectored(MSG_PUT_BULK, [meta, value]) — on UCX the
// value iovec is zero-copy via rcache when its memory is ucp_mem_map-registered;
// the frame CRC covers [meta||value] just like MSG_PUT. The
// response is a normal rkyv `PutResp` (tiny — no bulk framing needed back).
pub const MSG_PUT_BULK: u8 = 0x51;

/// Batched PUT: N operations on the SAME partition in ONE frame. The PS
/// decodes the frame once and injects all N ops into `partition_loop`'s
/// `pending` as a single mpsc message via the `BatchPut` variant —
/// `partition_loop` then sees `pending += N` atomically and can fire
/// a full-size batch immediately, exercising both `INFLIGHT_CAP` (8)
/// AND a wide `batch_size` (up to `MAX_WRITE_BATCH = 256`).
///
/// Non-bulk ONLY (values < 64 KiB). Large values keep per-op `MSG_PUT_BULK`
/// — for them the wire payload IS the bottleneck and per-op overhead
/// is negligible; bundling them into one frame would force a 256 MB
/// frame for a 32-op batch of 8 MiB values which the wire framing
/// reject. See `docs/perf_4k_loopback_vs_xhost_scaling.md` for the
/// motivating cross-host write cliff this closes.
///
/// Client side: routes by part_id (every op must hash to the SAME
/// partition — the client groups by partition before issuing this RPC).
/// `region_epoch` is the cached `MgrRegionInfo.region_epoch` for the
/// partition; PS rejects with `FailedPrecondition` on mismatch (same
/// shape as `MSG_PUT`).
pub const MSG_BATCH_PUT: u8 = 0x53;

/// Batched GET: N keys on the SAME partition in ONE frame. Symmetric to
/// `MSG_BATCH_PUT` — values are returned inline in the response (a
/// rkyv `BatchGetResp { code, statuses, values }`). NotFound is a
/// per-key status, not a per-batch error.
pub const MSG_BATCH_GET: u8 = 0x54;

/// redirect GET. Same request shape as MSG_GET (`GetReq`). For a
/// large (>= 64 KiB) full-value ValuePointer read the PS answers with a
/// DESCRIPTOR (extent + the value's exact byte range inside the extent +
/// replica addresses + eversion) instead of proxying the bytes; the
/// client then reads that range straight from an EN, taking the PS out
/// of the large-value data path. Inline / small / sub-range / non-VP
/// values come back inline (`extent_id == 0`). The descriptor is an
/// OPTIMIZATION, never a correctness dependency: any client-side
/// direct-read failure (eversion bumped by EC conversion, extent GC'd
/// between redirect and read, replica down) falls back to the plain
/// MSG_GET / MSG_GET_BULK proxy path, which re-resolves through the PS.
pub const MSG_GET_REDIRECT: u8 = 0x56;

/// first-frame connection authentication. The client sends a signed
/// Ed25519 capability token (see `crate::cap_token`); the PS verifies it once
/// against its cached public keys and binds the connection's `{allowed_prefixes,
/// exp}` principal. Subsequent per-request enforcement is a byte `starts_with`
/// + `exp` check with no re-verify. AUTH_HELLO is OPTIONAL (a connection that
/// never sends it is anonymous → denied on protected prefixes only). It carries
/// no `part_id` — it's connection-level, handled before the routing check.
pub const MSG_AUTH_HELLO: u8 = 0x55;

/// manager → PS "seal + roll these stream tails". The recovery
/// sweep sends this when it finds an OPEN tail extent (`!sealed`) with a replica
/// on a Fenced node — recovery only rebuilds SEALED extents, so an idle
/// partition's open tail on a fenced node would otherwise never drain and block
/// `remove` forever. The PS routes each `(stream_id, tail_extent_id)` into the
/// owning partition's writer thread and calls the existing
/// `StreamClient::seal_and_roll_tail` (WAL self-heal already uses it): the tail
/// seals via the manager's lenient probe (dead replicas don't block; all
/// replicas unreachable → the seal Precondition surfaces), a fresh tail rolls on
/// healthy nodes (Part A placement exclusion keeps it off fenced nodes), and the
/// next recovery tick rebuilds the now-sealed extent's fenced slots. Idempotent:
/// `tail_extent_id` is compared against the current tail; a mismatch (already
/// rolled) is a no-op.
pub const MSG_ROLL_TAILS: u8 = 0x57;

/// `MSG_AUTH_HELLO` request — the opaque capability token bytes.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct AuthHelloReq {
    pub token: Vec<u8>,
}

/// `MSG_AUTH_HELLO` response. `code == CODE_OK` binds the principal; otherwise
/// `message` carries the reject reason (`cap_token::AuthReject` label).
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct AuthHelloResp {
    pub code: u8,
    pub message: String,
}

/// response for `MSG_GET_REDIRECT` (rkyv).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetRedirectResp {
    pub code: u8,
    pub message: String,
    /// Inline value when no redirect applies (`extent_id == 0`).
    pub value: Vec<u8>,
    /// 0 = no redirect (value is inline above).
    pub extent_id: u64,
    /// Byte offset of the VALUE bytes inside the extent (the PS already
    /// skipped the WAL record framing + key, so the client reads value
    /// bytes directly and needs no WAL format knowledge).
    pub value_offset: u64,
    pub value_len: u64,
    /// EN-side eversion fence for the read.
    pub eversion: u64,
    /// Replica addresses (shard-routed) so the client needs no manager
    /// round-trip. Read any replica — VP extents referenced by live
    /// reads are sealed or all-replica-acked at the VP offset.
    pub replica_addrs: Vec<String>,
}

/// resolve MANY redirect descriptors in ONE PS call — the
/// batch mirror of `MSG_GET_REDIRECT`. A large-file read (model load / dataset
/// stream) redirects one extent per `MSG_GET_REDIRECT`, funnelling ~630 PS
/// round-trips through the single owning partition's task; this resolves them
/// all in one call so the client then fans the EN direct-reads out across all
/// replicas with the PS out of the metadata loop.
pub const MSG_GET_REDIRECT_MANY: u8 = 0x59;

/// One item in a `GetRedirectManyReq` — a key + sub-range, same semantics as a
/// `GetReq`'s `(key, offset, length)` (`length == 0` = whole value).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetRedirectItem {
    pub key: Vec<u8>,
    pub offset: u32,
    pub length: u32,
}

/// request. All items share `part_id` + `region_epoch` (the
/// client groups items by owning partition and sends one frame per partition).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetRedirectManyReq {
    pub part_id: u64,
    pub region_epoch: u64,
    pub items: Vec<GetRedirectItem>,
}

/// response: one `GetRedirectResp` per request item, in input
/// order (`results.len() == req.items.len()`).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetRedirectManyResp {
    pub results: Vec<GetRedirectResp>,
}

/// One op inside a `BatchPutReq` / `BatchGetReq`. All ops in a batch
/// share the parent's `part_id` + `region_epoch`. `expires_at = 0`
/// means no TTL (matches `PutReq`).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct BatchPutOp {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub expires_at: u64,
    /// BUG-LEASE-2 Phase 2: per-op fencing identity. 0 = anonymous
    /// (skip fencing). A fenced op gets `CODE_FENCED` in its
    /// `BatchPutResp.statuses` slot without failing the whole batch.
    pub inode_hint: u64,
    pub lease_epoch: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct BatchPutReq {
    pub part_id: u64,
    pub region_epoch: u64,
    pub must_sync: bool,
    pub ops: Vec<BatchPutOp>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct BatchPutResp {
    /// `CODE_OK` on full success; otherwise statuses[i] carries the
    /// per-op result.
    pub code: u8,
    pub message: String,
    /// Per-op status code. Empty iff `code != CODE_OK` AND the failure
    /// was batch-level (e.g. wrong part_id / stale epoch). Otherwise
    /// has `req.ops.len()` entries.
    pub statuses: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct BatchGetReq {
    pub part_id: u64,
    pub region_epoch: u64,
    pub keys: Vec<Vec<u8>>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct BatchGetItem {
    /// 0 = OK (value present), 1 = NotFound, other = error.
    pub status: u8,
    pub value: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct BatchGetResp {
    /// Batch-level code: `CODE_OK` if the request was acceptable;
    /// per-key statuses live in `items`. Stale-epoch and wrong-partition
    /// surface here.
    pub code: u8,
    pub message: String,
    pub items: Vec<BatchGetItem>,
}

/// diagnostic: trace where a user_key's MVCC entries live across
/// memtable / imm / sst — used by the f250 reproducer to localise
/// data-loss bugs without guessing.
pub const MSG_DIAG_TRACE_KEY: u8 = 0x52;

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DiagTraceKeyReq {
    pub part_id: u64,
    pub user_key: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DiagTraceKeyResp {
    pub code: u8,
    pub message: String,
    /// Newest seq found in the active memtable, or 0 if absent.
    pub memtable_seq: u64,
    /// Per-imm (front..back) newest seq found, 0 = absent.
    pub imm_seqs: Vec<u64>,
    /// Per-SST (oldest..newest by vec order) newest seq found, 0 = absent.
    /// Bloom-gated (mirrors the real GET path).
    pub sst_seqs: Vec<u64>,
    /// Same scan but bypassing the bloom filter. A divergence from
    /// `sst_seqs` (nobloom finds it, bloom-gated returns 0) is a bloom
    /// FALSE NEGATIVE — the read path silently skips this SST.
    pub sst_seqs_nobloom: Vec<u64>,
    /// Per-SST full linear scan over ALL blocks (ignores bloom AND
    /// find_block_for_key). A divergence from `sst_seqs_nobloom`
    /// (fullscan finds it, single-block scan returns 0) is a block-
    /// SELECTION bug in the read path, not data loss.
    pub sst_seqs_fullscan: Vec<u64>,
    /// Per-SST table.last_seq (for diagnostic context).
    pub sst_last_seqs: Vec<u64>,
}

/// per-partition GC replay-floor + per-SST vp_head snapshot.
/// Lets `autumn-op info --part P` show WHY a `forcegc P E` on a given extent
/// would be protected (extent E sits at/before the recovery replay floor →
/// correct protection, not a bug) without grepping the PS log.
pub const MSG_DIAG_PARTITION_VP: u8 = 0x58;

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DiagPartitionVpReq {
    pub part_id: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct DiagPartitionVpResp {
    pub code: u8,
    pub message: String,
    /// log_stream extent ids in stream order (index 0 = oldest = position 0).
    pub log_extent_ids: Vec<u64>,
    /// Per live SST (oldest..newest by vec order): (vp_extent_id, vp_offset).
    /// The GC replay floor is the MIN stream-position over these vp_extent_ids.
    pub sst_vp_heads: Vec<(u64, u64)>,
    /// GC replay floor = MIN stream-position over the SST vp_heads (0 when
    /// none resolve). GC never punches a NON-EMPTY extent at/after this.
    pub floor_pos: u64,
    /// The log extent id at `floor_pos` (0 if the log is empty).
    pub floor_extent_id: u64,
    /// `p.vp` seed = the committed log TAIL; the active memtable's next
    /// rotation stamps its SST vp_head here (so a fresh flush ADVANCES the
    /// floor toward this). Contrast with `floor_extent_id` (a lagging SST).
    pub vp_seed_extent_id: u64,
    pub vp_seed_offset: u64,
}

/// Fixed prefix of the MSG_PUT_BULK meta: part_id(8)+region_epoch(8)+
/// expires_at(8)+key_len(4)+inode_hint(8)+lease_epoch(8).
/// BUG-LEASE-2 Phase 2 extended this 28 → 44 (the two fence fields);
/// no version byte — same-commit cluster upgrade, like every other
/// wire-shape change in this repo.
pub const PUT_BULK_HEADER_LEN: usize = 44;

/// Build the MSG_PUT_BULK meta block `[part_id][region_epoch][expires_at]
/// [key_len][inode_hint][lease_epoch][key]` (value is appended by the
/// caller as a separate iovec).
#[allow(clippy::too_many_arguments)]
pub fn encode_put_bulk_meta(
    part_id: u64,
    region_epoch: u64,
    expires_at: u64,
    key: &[u8],
    inode_hint: u64,
    lease_epoch: u64,
) -> bytes::Bytes {
    use bytes::BufMut;
    let mut b = bytes::BytesMut::with_capacity(PUT_BULK_HEADER_LEN + key.len());
    b.put_u64_le(part_id);
    b.put_u64_le(region_epoch);
    b.put_u64_le(expires_at);
    b.put_u32_le(key.len() as u32);
    b.put_u64_le(inode_hint);
    b.put_u64_le(lease_epoch);
    b.put_slice(key);
    b.freeze()
}

/// Parsed MSG_PUT_BULK meta + the offset where the value begins in the payload.
pub struct PutBulkMeta {
    pub part_id: u64,
    pub region_epoch: u64,
    pub expires_at: u64,
    pub key_len: usize,
    /// BUG-LEASE-2: 0 = anonymous write, skip fencing.
    pub inode_hint: u64,
    pub lease_epoch: u64,
    /// Byte offset of the value within the original payload.
    pub value_offset: usize,
}

/// Parse the fixed prefix + key length of a MSG_PUT_BULK payload. Returns `None`
/// if the payload is too short to hold the header + declared key. The caller
/// reads the key as `payload[PUT_BULK_HEADER_LEN..value_offset]` and the value as
/// `payload[value_offset..]` (both zero-copy slices).
pub fn parse_put_bulk_meta(payload: &[u8]) -> Option<PutBulkMeta> {
    if payload.len() < PUT_BULK_HEADER_LEN {
        return None;
    }
    let part_id = u64::from_le_bytes(payload[0..8].try_into().ok()?);
    let region_epoch = u64::from_le_bytes(payload[8..16].try_into().ok()?);
    let expires_at = u64::from_le_bytes(payload[16..24].try_into().ok()?);
    let key_len = u32::from_le_bytes(payload[24..28].try_into().ok()?) as usize;
    let inode_hint = u64::from_le_bytes(payload[28..36].try_into().ok()?);
    let lease_epoch = u64::from_le_bytes(payload[36..44].try_into().ok()?);
    let value_offset = PUT_BULK_HEADER_LEN.checked_add(key_len)?;
    if payload.len() < value_offset {
        return None;
    }
    Some(PutBulkMeta {
        part_id,
        region_epoch,
        expires_at,
        key_len,
        inode_hint,
        lease_epoch,
        value_offset,
    })
}

// ── Status codes ────────────────────────────────────────────────────────────

pub const CODE_OK: u8 = 0;
pub const CODE_NOT_FOUND: u8 = 1;
pub const CODE_INVALID_ARGUMENT: u8 = 2;
pub const CODE_PRECONDITION: u8 = 3;
pub const CODE_ERROR: u8 = 4;
/// regular `Put` value exceeds the
/// `AUTUMN_PS_MAX_INLINE_BYTES` cap. Caller should split the value into
/// chunks and stripe-write via `ClusterClient::put_stream_begin`
/// (client-side striperados — chunks are normal Puts to a
/// reserved-namespace key + a 29-byte Meta blob at the user key).
pub const CODE_VALUE_TOO_LARGE: u8 = 5;
//
// CODE 6 was `CODE_UPLOAD_NOT_FOUND` for the server-side
// multipart upload session. Removed (no server-side sessions
// any more). Code 6 is RESERVED — don't reuse for at least one major
// version to avoid mis-decoding by stale clients.
/// Put/Delete/StreamPut rejected because the partition is in the
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
/// BUG-LEASE-2 (coco P0 #2, 2026-06-05) — storage-layer fencing
/// token: the PS rejects a write whose stamped `lease_epoch` is
/// strictly LESS than the per-inode floor the PS has already
/// observed. A client that has been force-revoked or TTL-revoked
/// by the manager carries an old `lease_epoch`; without this
/// fence, the ~5s window between revoke push and the client's
/// `held_leases` eviction (BUG-LEASE-3 closes the user-space
/// half) could still let a stale writer's RPC land at the PS.
/// SDK maps this to a typed error so apps can re-AcquireLease
/// and retry.
pub const CODE_FENCED: u8 = 9;

// ── Request/Response types ─────────────────────────────────────────────────

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PutReq {
    pub part_id: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    /// follow-up: the `must_sync` field was removed. Every Put is
    /// now durable via the extent-node fsync coalescer (RocksDB-style
    /// group commit). The PS no longer threads any sync flag through.
    pub expires_at: u64,
    /// TiKV-style region epoch: client stamps the epoch from its
    /// routing cache; PS rejects with FailedPrecondition when its own
    /// epoch differs (split / merge has happened). `0` = skip check
    /// (bootstrap, tests, legacy paths).
    pub region_epoch: u64,
    /// BUG-LEASE-2 (coco P0 #2, 2026-06-05) — storage-layer fencing.
    /// `inode_hint = 0` (the default) means "no fencing for this
    /// write" (KV CLI, non-lease-aware paths, bootstrap, tests).
    /// `inode_hint != 0` means "this write logically belongs to
    /// inode N; please apply per-inode fencing using `lease_epoch`."
    pub inode_hint: u64,
    /// BUG-LEASE-2 — monotonic per-inode epoch from the manager
    /// (`MgrInodeLeaseInfo.version` from the last successful
    /// AcquireLease). PS rejects with `CODE_FENCED` when
    /// `inode_hint != 0` AND `lease_epoch < current_floor`. The
    /// PS also bumps its in-memory floor to
    /// `max(floor, lease_epoch)` on every accepted write so a
    /// new writer's first stamped write learns the higher floor.
    pub lease_epoch: u64,
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
    /// BUG-LEASE-2 Phase 2 (coco P1 #3): deletes are fenced like puts —
    /// a revoked writer's late truncate/unlink must not remove the new
    /// writer's extents. 0 = anonymous (skip fencing).
    pub inode_hint: u64,
    pub lease_epoch: u64,
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
    /// Where the scan begins, **INCLUSIVE** — an entry whose key equals
    /// `start` IS returned.
    ///
    /// There is no way for a caller to express "resume strictly after key K",
    /// and the two obvious tricks are both WRONG in opposite directions.
    /// Internal keys are `user_key ++ 0x00 ++ BE(u64::MAX - seq)` and
    /// `parse_key` strips a fixed-width suffix rather than scanning for the
    /// separator, so a user key may legally contain `0x00`:
    ///
    /// - `K ++ 0x00` sorts BEFORE every version of K (its inverted-seq byte is
    ///   `0xFF` for small seq, and `0x00 < 0xFF`), so the boundary key comes
    ///   back again — a silent double-count across page boundaries.
    /// - `K ++ 0x01` skips any real user key of the form `K ++ 0x00 ++ …` —
    ///   silent data loss, and this repo has such keys (fs keys are binary:
    ///   `[0x03][lane][ino][off]`, big-endian integers full of zero bytes).
    ///
    /// So paginate by passing the previous page's LAST key and dropping the
    /// first returned entry, which is that same key. `MemoryStore::scan_keys`
    /// and `memory-mcp`'s `wipe_agent` both do exactly that; the property is
    /// pinned by `crates/autumn-memory/tests/scan_boundary.rs`.
    ///
    /// The clean fix is a server-computed opaque page cursor on `RangeResp`
    /// (the encoding belongs on the server, and `cur_end_key` already
    /// establishes the cursor idea one level up, at partition granularity).
    /// Until that exists, this comment is the contract.
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
    /// (design doc D4): operator/controller-specified split
    /// point. When `Some(key)`, the PS validates the key lies STRICTLY inside
    /// the partition's authoritative `(start_key, end_key)` interval and uses
    /// it verbatim as `mid_key`, SKIPPING both median selection AND the
    /// `>= 2 keys` gate — so an empty / near-empty partition can be split
    /// (the D8 per-(namespace,tenant) presplit primitive: cut an empty pair
    /// into empty children). The key is an ARBITRARY byte string; the PS is
    /// app-agnostic (D5) — it never inspects namespace/prefix structure.
    /// `None` = legacy behaviour (median-by-key-count over live user keys,
    /// which still enforces `>= 2 keys`).
    pub at_key: Option<Vec<u8>>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct SplitPartResp {
    pub code: u8,
    pub message: String,
}

// partition merge. Sent to the SURVIVOR's PS; both partitions
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

// PrepareMerge-style freeze RPC. The CLI sends this to each
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

// manager → PS "seal + roll these open tails" (see MSG_ROLL_TAILS).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RollTailsReq {
    pub part_id: u64,
    /// `(stream_id, expected_tail_extent_id)` pairs. The PS seals+rolls each
    /// stream whose current tail still equals `expected_tail_extent_id`
    /// (idempotent: a stale/already-rolled entry is skipped).
    pub entries: Vec<(u64, u64)>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RollTailsResp {
    pub code: u8,
    /// Number of tails actually sealed+rolled this call (0 if all were stale
    /// or the partition isn't served here).
    pub rolled: u32,
    pub message: String,
}

// StreamPutReq REMOVED 2026-06-11 with MSG_STREAM_PUT (see the reserved
// constant note near the msg_type list).
// PutBegin / PutChunk / PutCommit / PutAbort req/resp removed in
// Stripe-write is now pure client-side (Ceph striperados pattern):
// `ClusterClient::put_stream_begin` returns a `PutStreamHandle` that
// writes each chunk via plain `MSG_PUT` to a reserved-namespace key,
// then writes a 29-byte Meta blob to the user key as the atomic
// commit point.

/// Maintenance operations.
///
/// **Wire change (backward-incompatible)**: added four optional
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
    /// filter — discard ratio threshold (0.0..=1.0). `None` → 0.4
    /// (`GC_DISCARD_RATIO`). Used only when `op == MAINTENANCE_AUTO_GC`.
    pub gc_ratio: Option<f64>,
    /// filter — only consider sealed extents whose `sealed_length`
    /// is at most this many bytes. Combined with a lower `gc_ratio`
    /// lets the caller say "punch small extents at even 10% dead".
    pub gc_max_size: Option<u64>,
    /// stream-level dead-byte high-water hint. When the partition's
    /// total reclaimable bytes exceed this, the per-extent ratio is
    /// halved for this dispatch.
    pub gc_stream_debt: Option<u64>,
    /// only pick `sealed_length == 0` non-tail extents (cheapest
    /// possible GC — no rewrite, just punch_holes). Overrides
    /// `gc_ratio` / `gc_max_size` when true.
    pub gc_empty_only: bool,
    /// manager op-ledger correlation id. `0` = untracked (the PS-local
    /// maintenance scheduler and legacy SDK callers): the PS runs the op but
    /// records no terminal `MaintenanceOutcome`. Non-zero = the manager
    /// submitted this op through the ledger and wants the outcome reported back
    /// (piggybacked on the load heartbeat).
    pub op_id: u64,
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
    pub offset: u64,
    pub len: u64,
}

/// Checkpoint written to metaStream after each flush/compaction.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct TableLocations {
    pub locs: Vec<SstLocation>,
    pub vp_extent_id: u64,
    pub vp_offset: u64,
    /// merge: source partition's log_stream extent count at flush
    /// time. Used post-merge by `recover_partition` to derive each
    /// source's region (positions [cumsum, cumsum + count)) in the
    /// spliced log_stream, so replay dedup uses each source's OWN
    /// `sst_max_seq` (not the union max, which silently skips one
    /// source's post-vp_head tail records ≤ the OTHER source's max).
    /// 0 in legacy / fresh-state checkpoints — treated as "no boundary
    /// info; fall back to single-source replay" (= pre-fix behavior).
    pub log_extent_count: u32,
    /// BUG-LEASE-2 Phase 2: snapshot of the partition's per-ino fence
    /// floors `(ino, lease_epoch)` at checkpoint time. Recovery seeds
    /// `PartitionData.fence_floors` from this, then WAL replay max-merges
    /// any later `OP_FENCE_BUMP` records — floors raised before the
    /// replay window survive PS restarts. Floors change only on lease
    /// epoch bumps (rare), so this stays small. Empty in legacy
    /// checkpoints. rkyv struct change ⇒ same-commit cluster upgrade.
    pub fence_floors: Vec<(u64, u64)>,
}

// ── Helper: extract part_id from any partition RPC payload ─────────────────

/// Extract the part_id from a partition RPC request payload.
/// Decodes the full request type based on msg_type. Returns 0 if decoding fails.
pub fn extract_part_id(msg_type: u8, payload: &[u8]) -> u64 {
    match msg_type {
        MSG_PUT => rkyv_decode::<PutReq>(payload)
            .map(|r| r.part_id)
            .unwrap_or(0),
        // MSG_PUT_BULK meta is binary: part_id is the first u64 LE.
        MSG_PUT_BULK => payload
            .get(0..8)
            .and_then(|b| b.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0),
        MSG_GET | MSG_GET_BULK | MSG_GET_REDIRECT => rkyv_decode::<GetReq>(payload)
            .map(|r| r.part_id)
            .unwrap_or(0),
        MSG_GET_REDIRECT_MANY => rkyv_decode::<GetRedirectManyReq>(payload)
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
        MSG_ROLL_TAILS => rkyv_decode::<RollTailsReq>(payload)
            .map(|r| r.part_id)
            .unwrap_or(0),
        MSG_DIAG_TRACE_KEY => rkyv_decode::<DiagTraceKeyReq>(payload)
            .map(|r| r.part_id)
            .unwrap_or(0),
        MSG_BATCH_PUT => rkyv_decode::<BatchPutReq>(payload)
            .map(|r| r.part_id)
            .unwrap_or(0),
        MSG_BATCH_GET => rkyv_decode::<BatchGetReq>(payload)
            .map(|r| r.part_id)
            .unwrap_or(0),
        MSG_DIAG_PARTITION_VP => rkyv_decode::<DiagPartitionVpReq>(payload)
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
            MSG_MAINTENANCE,
            MSG_GET_DISCARDS,
            MSG_MERGE_PART,
            MSG_MERGE_FREEZE,
            MSG_GET_BULK,
            MSG_BATCH_PUT,
            MSG_BATCH_GET,
            MSG_GET_REDIRECT,
            MSG_GET_REDIRECT_MANY,
            MSG_AUTH_HELLO,
            MSG_ROLL_TAILS,
            MSG_DIAG_TRACE_KEY,
            MSG_DIAG_PARTITION_VP,
        ];
        for i in 0..all.len() {
            for j in i + 1..all.len() {
                assert_ne!(all[i], all[j], "msg_type collision at index {} vs {}", i, j);
            }
        }
    }

    #[test]
    fn extract_part_id_covers_diag_partition_vp() {
        // Regression: without the MSG_DIAG_PARTITION_VP arm, extract_part_id
        // falls to `_ => 0`, so the PS conn layer misroutes the diag frame as
        // NotFound (0 != owner_part) and `autumn-op info --part` never shows the
        // replay floor. Assert the arm resolves the real part_id.
        let payload = rkyv_encode(&DiagPartitionVpReq { part_id: 4242 });
        assert_eq!(extract_part_id(MSG_DIAG_PARTITION_VP, &payload), 4242);
    }
}

#[cfg(test)]
mod bug_lease_2_phase2_bulk_ctrl_tests {
    use super::*;

    #[test]
    fn put_bulk_meta_round_trips_fence_fields() {
        let key = b"some/extent/key";
        let meta = encode_put_bulk_meta(7, 3, 99, key, 42, 5);
        assert_eq!(meta.len(), PUT_BULK_HEADER_LEN + key.len());
        // [meta][value] is the wire payload; parse over meta+key+value.
        let mut payload = meta.to_vec();
        payload.extend_from_slice(b"VALUE");
        let parsed = parse_put_bulk_meta(&payload).expect("parse");
        assert_eq!(parsed.part_id, 7);
        assert_eq!(parsed.region_epoch, 3);
        assert_eq!(parsed.expires_at, 99);
        assert_eq!(parsed.key_len, key.len());
        assert_eq!(parsed.inode_hint, 42);
        assert_eq!(parsed.lease_epoch, 5);
        assert_eq!(&payload[PUT_BULK_HEADER_LEN..parsed.value_offset], key);
        assert_eq!(&payload[parsed.value_offset..], b"VALUE");
    }

    #[test]
    fn put_bulk_meta_anonymous_zeroes() {
        let meta = encode_put_bulk_meta(1, 0, 0, b"k", 0, 0);
        let parsed = parse_put_bulk_meta(&meta).expect("parse");
        assert_eq!(parsed.inode_hint, 0);
        assert_eq!(parsed.lease_epoch, 0);
    }

    #[test]
    fn put_bulk_meta_short_payload_rejected() {
        // One byte short of the fixed header.
        let meta = encode_put_bulk_meta(1, 0, 0, b"", 9, 9);
        assert!(parse_put_bulk_meta(&meta[..PUT_BULK_HEADER_LEN - 1]).is_none());
    }
}
