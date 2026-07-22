//! Wire codec for Manager RPCs over autumn-rpc.
//!
//! All 16 manager RPCs are control-plane and use rkyv zero-copy serialization.
//! Message type constants are in the 0x20–0x3F range to avoid collision with
//! ExtentService RPCs (0x01–0x0A).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use rkyv::api::high::{HighDeserializer, HighSerializer};
use rkyv::rancor::Error as RkyvError;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::{Archive, Deserialize, Serialize};

// ── msg_type constants ───────────────────────────────────────────────────────

// StreamManagerService (12 RPCs)
pub const MSG_STATUS: u8 = 0x20;
pub const MSG_ACQUIRE_OWNER_LOCK: u8 = 0x21;
pub const MSG_REGISTER_NODE: u8 = 0x22;
pub const MSG_CREATE_STREAM: u8 = 0x23;
pub const MSG_STREAM_INFO: u8 = 0x24;
pub const MSG_EXTENT_INFO: u8 = 0x25;
pub const MSG_NODES_INFO: u8 = 0x26;
pub const MSG_CHECK_COMMIT_LENGTH: u8 = 0x27;
pub const MSG_STREAM_ALLOC_EXTENT: u8 = 0x28;
pub const MSG_STREAM_PUNCH_HOLES: u8 = 0x29;
pub const MSG_TRUNCATE: u8 = 0x2A;
pub const MSG_MULTI_MODIFY_SPLIT: u8 = 0x2B;

// PartitionManagerService (4 RPCs)
pub const MSG_REGISTER_PS: u8 = 0x2C;
pub const MSG_UPSERT_PARTITION: u8 = 0x2D;
pub const MSG_GET_REGIONS: u8 = 0x2E;
pub const MSG_HEARTBEAT_PS: u8 = 0x2F;

// F099-K: per-partition listener address registration (PS reports the
// `host:port` of each partition's TcpListener to the manager so clients
// can target the owning partition's CPU shard directly).
pub const MSG_REGISTER_PARTITION_ADDR: u8 = 0x30;

// F109: extent-node startup orphan reconcile. The node sends every
// `extent_id` it loaded from disk; the manager replies with the subset
// no longer present in `s.extents` so the node can unlink them.
pub const MSG_RECONCILE_EXTENTS: u8 = 0x31;

// FOPS-03: mutate an existing stream's EC configuration.
// After this call the manager's ec_conversion_dispatch_loop will pick up
// any sealed extents in that stream and convert them on the next tick (~5s).
pub const MSG_UPDATE_STREAM_EC: u8 = 0x32;

// 0x33 retired with the vp_table_refs removal (was
// MSG_SYNC_PARTITION_VP_REFS, partition→manager VP-dependency sync).

// F183: partition merge primitive (inverse of MSG_MULTI_MODIFY_SPLIT).
pub const MSG_MULTI_MODIFY_MERGE: u8 = 0x34;
// F183: advisory engine — query split/merge candidates.
pub const MSG_GET_POLICY_CANDIDATES: u8 = 0x35;
// F183: per-partition load metrics report (PS → manager, 5 s cadence).
pub const MSG_REPORT_PARTITION_LOAD: u8 = 0x36;
// F185: orchestrated partition merge — inputs are just (survivor, victim).
// The manager (which is leader-fenced + persistent) acquires its own
// admin owner lock, freezes both PSes via MSG_MERGE_FREEZE, captures the
// 6 commit_lengths under the freeze, then runs the existing
// MSG_MULTI_MODIFY_MERGE handler logic atomically. CLI is a thin wrapper
// so a CLI crash mid-merge is benign — the manager either commits the
// txn (region_sync_loop on the survivor's PS picks up the wider rg and
// drops the frozen PartitionData on next tick) or it doesn't (PSes
// auto-unfreeze after FREEZE_TTL seconds).
pub const MSG_MERGE_PARTITIONS: u8 = 0x37;
// F192: PS → manager push-based disk failure signal. Fire-and-forget
// (req_id = 0); manager deduplicates by reporter_part_id, applies a
// 60 s sliding window + 3-distinct-reporter quorum before flipping
// `node.disks[*].online = false`. Pre-F192 the manager's disk online
// view was purely pull-based via the df loop (`node_health_loop` since
// F222, 2 s; was `disk_status_update_loop`, 10 s), so between F190's
// per-stream alloc route-around and the next DF poll the global view
// lagged the per-stream truth.
pub const MSG_REPORT_DISK_FAILURE: u8 = 0x38;

// F203: per-extent EC convert trigger. Replaces the deleted
// ec_conversion_dispatch_loop fresh-candidate scan. Persists a rich
// `pending_ec_dispatch` marker for `extent_id`; the next dispatch_loop
// tick drains it via the F198 replay path.
pub const MSG_FORCE_EC_CONVERT: u8 = 0x39;

// F203: external policy controller helper — query the manager's
// latest cached `PartitionLoad` for a given partition. Lets `client
// info --part PID --detail` surface SST tombstone / expired /
// out-of-range / minor pending byte counts without a separate PS RPC.
pub const MSG_GET_PARTITION_DETAIL: u8 = 0x3A;

// F210-F1: const-dump of the `POLICY_KIND_*` enum so external
// controllers (Python, ops scripts) can introspect the wire values
// without hardcoding them. The values themselves are wire-stable
// (frozen — pre-F210-F1 a docstring drift made the docs say `1..7`
// while the code said `0..6`, which produced an off-by-one bug in
// every Python controller that trusted the docs). After F210-F1 the
// names + values come from the manager binary as the single source
// of truth; any future addition appends a new `(name, value)` pair.
pub const MSG_GET_POLICY_KIND_NAMES: u8 = 0x3B;

// ── F211 operator-driven node lifecycle ─────────────────────────────────────
//
// F211-B: read-only health reporting — the operator policy script reads
// these to decide whether to `mgr_fence_node` / `mgr_set_node_maintenance`.
pub const MSG_LIST_NODE_STATES: u8 = 0x3C;
pub const MSG_EXTENT_HEALTH_REPORT: u8 = 0x3D;
pub const MSG_LIST_EC_INFLIGHT_MARKERS: u8 = 0x3E;

// F211-C: admin RPCs that mutate `node_override/<node_id>` (etcd-persisted)
// — operator explicitly fences / maintains / clears / removes nodes.
pub const MSG_FENCE_NODE: u8 = 0x3F;
pub const MSG_SET_NODE_MAINTENANCE: u8 = 0x40;
pub const MSG_CLEAR_NODE_OVERRIDE: u8 = 0x41;
pub const MSG_REMOVE_NODE: u8 = 0x42;

// F211-H: monitoring of the recovery throttle / queue depth.
pub const MSG_RECOVERY_STATS: u8 = 0x43;

// F211-I: append-only operator audit log query.
pub const MSG_QUERY_AUDIT_LOG: u8 = 0x44;

// F214-A: read-only cluster identity. The manager CAS-writes a UUID to
// etcd `autumn-rs/cluster_id` on first leader promotion; this RPC exposes
// it to `autumn-op format` (stamps cluster_id into each formatted disk)
// and to `autumn-extent-node` (verifies the disk's stamped cluster_id
// matches the manager's before serving). Followers can answer from
// replayed state — no leader check.
pub const MSG_GET_CLUSTER_ID: u8 = 0x45;

// ── F-ioring-lease-1: inode-level lease + close-to-open coherence ──────────
//
// JuiceFS-style single-writer / many-reader leases on each fuse inode,
// served by the manager (same etcd backing as owner locks). The lease
// service is consumed by autumn-fuse (the io_uring daemon, the original
// second consumer, was removed 2026-06-30). Clients appear as
// `MgrClientId` to the manager, so the protocol stays daemon-kind-
// agnostic for any future consumer. See docs/autumn_fs_lease_plan.md.
pub const MSG_ACQUIRE_LEASE: u8 = 0x46;
pub const MSG_RELEASE_LEASE: u8 = 0x47;
pub const MSG_HEARTBEAT_LEASE: u8 = 0x48;
pub const MSG_POLL_INVALIDATIONS: u8 = 0x49;

// ── R1 rolling upgrade: persisted cluster_version (design §3-R1) ──────────
//
// etcd key `autumn-rs/cluster_version`, ASCII decimal. GET servable from
// any replica (replayed state). BUMP is leader-only, monotonic, exactly
// +1, and capped at the manager's own WIRE_VERSION_MAX. Operators bump
// via `autumn-op upgrade-version` AFTER every member binary is upgraded;
// new wire forms / persisted formats gate on the bumped value.
pub const MSG_GET_CLUSTER_VERSION: u8 = 0x4A;
pub const MSG_BUMP_CLUSTER_VERSION: u8 = 0x4B;

// ── WAL self-heal A5: report a corrupt replica (docs/wal_selfheal_design.md) ──
//
// A PS that detected a bit-rotted log_stream replica during replay
// (per-record CRC mismatch, WAL-FAILSTOP) reports it so the manager can
// ISOLATE that replica from the serving set (clear its `avali` bit + bump
// eversion, etcd-first) before the partition serves (I1). Fenced: the report
// carries the partition `owner_epoch` + extent `eversion`; the manager CAS-
// validates both so a stale PS can't mutate the layout (I4).
pub const MSG_REPORT_CORRUPT_REPLICA: u8 = 0x4C;

// ── cluster-df: aggregate capacity summary (Ceph-`ceph df` style) ─────────────
//
// Leader-only read of a manager-maintained in-memory snapshot. RAW + autumn
// physical-used come from summing each EN's `df` report (the EN is the data
// owner; physical_used = Σ real extent file bytes, no amplification formula);
// logical_stored is the manager's read-only Σ distinct sealed_length. The wire
// carries only raw u64 facts — amplification factor / writable range / EC
// shape are computed by the consumer (autumn-op df, fuse statfs), never sent
// as floats. See docs / plan: cluster capacity is a RANGE under EC, so the
// single point-estimate is left to the display layer.
pub const MSG_CLUSTER_DF: u8 = 0x4D;

/// Dashboard-oriented compact cluster overview (observability batch 2).
/// Returns a per-partition rollup (range / ps / live_size / extent count) and a
/// per-node extent count WITHOUT the giant per-extent array that `MSG_STREAM_INFO`
/// (full dump) ships — so a web dashboard scales to 数千 partition / 数万 extent
/// (browser payload + refresh cost bounded by PARTITION count, not extent count).
/// Per-partition extents are fetched lazily via scoped `MSG_STREAM_INFO`.
pub const MSG_GET_CLUSTER_OVERVIEW: u8 = 0x4E;

// ── F-AUTHZ-1: manager-as-KDC (data-plane authz) ──────────────────────────────
//
// The manager (leader) is a KDC: it holds an Ed25519 signing private key + a
// tenant account DB, mints short-TTL capability tokens, and publishes its
// PUBLIC keys so the PS (KV layer) can verify locally without ever calling
// back. See docs/data_plane_authz_design.md.
//
// Client → manager: authenticate with a permanent tenant credential, get a
// short-TTL signed token (renewed in the background before exp).
pub const MSG_MINT_TOKEN: u8 = 0x4F;
// PS → manager (poll, cached): fetch the public keys + protected prefixes so
// the PS can verify tokens + know which key ranges to enforce.
pub const MSG_GET_AUTHZ_CONFIG: u8 = 0x50;
// admin → manager (low-frequency, admin-token gated): create/delete a tenant
// account (credential_hash + allowed_prefixes) in the KDC's account DB.
pub const MSG_TENANT_CREATE: u8 = 0x51;
pub const MSG_TENANT_DELETE: u8 = 0x52;
// F-FS-UNIFY M0: fuse-fs inode-number allocation moved into the manager
// (leader-fenced etcd CAS). The old scheme — every allocator doing a
// non-CAS read-modify-write on the `[0x04]next_inode` fs KV key — hands
// out DUPLICATE inode batches under concurrent allocators (two mounts,
// or a mount + the Python `autumn.Fs` client). The manager is already
// the grantor of every other monotonic token (owner_epoch, lease_epoch),
// so inode ranges follow the same pattern.
pub const MSG_ALLOC_INODES: u8 = 0x53;

// ── rkyv helpers ────────────────────────────────────────────────────────────

/// Serialize a value to Bytes using rkyv.
pub fn rkyv_encode<T>(val: &T) -> Bytes
where
    T: for<'a> Serialize<HighSerializer<rkyv::util::AlignedVec, ArenaHandle<'a>, RkyvError>>,
{
    let buf = rkyv::to_bytes::<RkyvError>(val).expect("rkyv encode");
    Bytes::copy_from_slice(&buf)
}

/// Deserialize a value from bytes using rkyv with archive-bytes validation.
///
/// F155: switched from `from_bytes_unchecked` to the checked `from_bytes`.
/// Pre-F155 a malformed payload (corrupted by a flipped bit that escaped TCP
/// CRC, a peer running an incompatible struct layout from a partial rolling
/// upgrade, or — in a future where the wire crosses an untrusted boundary —
/// a hostile sender) caused undefined behaviour: out-of-bounds reads, pointer
/// dereferences into arbitrary memory, or silent partial decoding into a
/// corrupt struct that downstream code then trusted. The checked path runs
/// `bytecheck`'s validation pass on the archived bytes (size, alignment,
/// pointer relativity, sum-type discriminants, container length bounds) and
/// returns a clean `Err` on any inconsistency. The validation cost is small
/// and bounded by the payload size — a few percent on the hot path at most;
/// negligible against rkyv's existing per-archive zero-copy work.
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

// ── Shared domain types ─────────────────────────────────────────────────────

/// Range of keys [start_key, end_key). Empty end_key means unbounded.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct MgrRange {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

/// Node metadata.
///
/// F099-M: `shard_ports` lists the per-shard TCP listener ports on the
/// extent-node process. Clients route hot-path RPCs (append, read_bytes,
/// commit_length) by `extent_id % shard_ports.len()`. When `shard_ports`
/// is empty, clients fall back to `address` (legacy single-thread mode).
///
/// F191: `control_address` is the extent-node's *separate* control-plane
/// listener — by convention `host:port + 1000`. Manager DF / future
/// heartbeat / disk-failure-report RPCs travel on this address via a
/// dedicated `control_pool`, so a sustained data-plane RPC (CONVERT_TO_EC,
/// COPY_EXTENT, RECOVERY) cannot starve the manager's liveness probe and
/// trigger a spurious `disk online → offline` flap. Empty = legacy node
/// that hasn't been re-registered yet; manager falls back to `address` so
/// upgrades are zero-downtime.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MgrNodeInfo {
    pub node_id: u64,
    pub address: String,
    pub disks: Vec<u64>,
    /// Optional per-shard ports. Empty = legacy single-thread extent-node.
    pub shard_ports: Vec<u16>,
    /// F191: optional control-plane address. Empty = legacy node;
    /// manager falls back to `address` for DF.
    pub control_address: String,
    /// F-EN-DYNSHARD M0: stable node identity (UUID v4), decoupled from the
    /// network address so an EN can change IP / shard-port layout across
    /// restarts and still be recognised as the SAME node (mirrors the PS's
    /// `ps_id`-vs-address split). Persisted WITH the node; the manager matches a
    /// registration by this first (scanning `nodes`), falling back to address
    /// for uuid-less legacy rows until they adopt. Empty = legacy node.
    pub node_uuid: String,
}

/// Disk metadata.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MgrDiskInfo {
    pub disk_id: u64,
    pub online: bool,
    pub uuid: String,
}

/// F198: rkyv-encoded value of the `ecConversionInflight/<extent_id>` etcd
/// marker. F173 originally persisted only the existence of the marker
/// (empty value); on leader failover or process restart the new leader
/// could not safely re-dispatch because it didn't know which `target_nodes`
/// the deposed leader had selected (a fresh `shuffle().take(extra)` would
/// pick different parity-node IDs, and `alloc_extent_on_node` against a
/// node that already received shard data resets that node's in-memory
/// state — silently corrupting EC layout). F198 widens the marker so
/// re-dispatch can reuse the original assignment.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrEcDispatchInflight {
    pub extent_id: u64,
    /// Full data+parity node id list, in shard-index order (data shards
    /// first, then parity shards). `replicates ++ parity` after a
    /// successful `apply_ec_conversion_done`.
    pub target_nodes: Vec<u64>,
    /// Disk-id-per-extra-parity-node returned by `alloc_extent_on_node`
    /// during the original dispatch. Used to rebuild `replicate_disks /
    /// parity_disks` in `apply_ec_conversion_done` on re-dispatch.
    pub extra_disk_ids: Vec<u64>,
    pub data_shards: u32,
    pub new_eversion: u64,
    /// F211-D Tier 2: owner-lock owner_epoch the manager held when this
    /// dispatch was authorised. Threaded into `ExtConvertToEcReq.owner_epoch`
    /// → `WriteShardReq.owner_epoch` / `CommitEcShardReq.owner_epoch` so a
    /// fenced ex-coord whose in-flight 2PC continues with the OLD owner_epoch
    /// is rejected by remote ENs via the existing `req.owner_epoch <
    /// entry.owner_epoch → CODE_LOCKED_BY_OTHER` check. A bumped
    /// owner_epoch is pushed to live targets by
    /// `auto_abandon_for_fenced_node` (fence-handover via
    /// `MSG_CHECK_COMMIT_LENGTH`) so the EN-side check fires.
    ///
    /// `0` keeps the pre-F211-D-Tier-2 no-fence behaviour for tests /
    /// memory-only mode where no owner_lock has been acquired.
    pub owner_epoch: i64,
}

/// Extent metadata — mirrors proto ExtentInfo.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrExtentInfo {
    pub extent_id: u64,
    pub replicates: Vec<u64>,
    pub parity: Vec<u64>,
    pub eversion: u64,
    pub refs: u64,
    /// Count of live SSTables whose ValuePointers referenced this extent — an
    /// indirect-retention "net" on top of `refs`.
    ///
    /// vp_table_refs-removal STAGING (Stage 1): the *maintenance* machinery
    /// that bumped/decremented this (the PS sync/pull RPCs + manager
    /// aggregation) is gone, so the field is now WRITE-FROZEN — every extent
    /// allocated under this build leaves it 0. It is still READ by
    /// `extent_can_delete` (which keeps `refs == 0 && vp_table_refs == 0`) as an
    /// UPGRADE-SAFETY GUARD: a cluster upgraded from a pre-removal build may
    /// hold legacy extents legitimately retained at `refs == 0 && vp_table_refs
    /// > 0`, and reaping them would lose live data (see manager
    /// `extent_can_delete`). Kept in the persisted `extents/<id>` rkyv layout so
    /// existing records decode unchanged. Stage 2's migration clears it and
    /// removes the field (collapsing the gate to `refs == 0`).
    pub vp_table_refs: u64,
    pub sealed_length: u64,
    /// True once this extent has been SEALED (immutable; no more appends).
    /// The explicit flag — NOT `sealed_length > 0` — is the authoritative
    /// "is sealed" signal, because an authoritative failover seal can seal an
    /// EMPTY tail (`sealed = true, sealed_length = 0`: nothing was ever
    /// all-replica-acked, so the writer's commit is 0). `sealed_length` remains
    /// the LENGTH (used for "is empty" / EC-min-size / read-bound checks);
    /// `sealed` is the STATE. Invariant: `sealed_length > 0 ⇒ sealed`. A fresh
    /// open extent is `sealed = false`.
    pub sealed: bool,
    pub avali: u32,
    pub replicate_disks: Vec<u64>,
    pub parity_disks: Vec<u64>,
    /// True iff `apply_ec_conversion_done` has rewritten this extent's
    /// shards into RS-encoded form. False for (a) freshly allocated
    /// open extents — even on EC streams where `parity` is pre-filled
    /// at allocation time — and (b) sealed extents still waiting for
    /// the manager's `ec_conversion_dispatch_loop` to fire. Read paths
    /// branch on this, NOT on `parity.is_empty()` — see programming
    /// note in `crates/stream/CLAUDE.md`.
    pub ec_converted: bool,
}

/// Stream metadata — mirrors proto StreamInfo.
///
/// The triple `(replicates, ec_data_shard, ec_parity_shard)` fully
/// describes the stream's data layout:
///
/// - `replicates`: open-extent replica count. While an extent is open
///   (not yet sealed), the data is fully replicated across this many
///   nodes (typically 3). Independent of `ec_data_shard` — a 3-replica
///   open stream can be encoded to 4+1, 7+1, etc. on seal.
/// - `ec_data_shard` (K): data-shard count of the post-seal EC encoding.
///   For a replication-only stream, equals `replicates` and `ec_parity_shard`
///   is 0.
/// - `ec_parity_shard` (M): parity-shard count of the post-seal EC encoding.
///   `0` means pure replication (no EC conversion will fire).
///
/// Legacy values (pre-2026-04-27 schema): streams persisted before this
/// field was introduced deserialise with `replicates = 0`. Callers must
/// treat `replicates == 0` as "unknown — derive from one of the stream's
/// open extents' `replicates.len()`".
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrStreamInfo {
    pub stream_id: u64,
    pub extent_ids: Vec<u64>,
    pub ec_data_shard: u32,
    pub ec_parity_shard: u32,
    pub replicates: u32,
}

/// Partition metadata.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrPartitionMeta {
    pub part_id: u64,
    pub log_stream: u64,
    pub row_stream: u64,
    pub meta_stream: u64,
    pub rg: Option<MgrRange>,
}

/// Region (partition→PS assignment).
///
/// `region_epoch` is a monotonic counter bumped by the manager whenever
/// `rg` is rewritten — split (both children get a new epoch), merge
/// (survivor gets a new epoch). Modeled after TiKV's `region_epoch`:
/// clients stamp their cached epoch on every PS request so the PS can
/// reject stale routing without a manager round-trip. Bootstrap value
/// is 1; `0` is reserved as "unknown / skip check" so legacy callers
/// and tests that don't populate it stay backward-compatible.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MgrRegionInfo {
    pub rg: Option<MgrRange>,
    pub part_id: u64,
    pub ps_id: u64,
    pub log_stream: u64,
    pub row_stream: u64,
    pub meta_stream: u64,
    pub region_epoch: u64,
}

/// Partition server detail.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MgrPsDetail {
    pub ps_id: u64,
    pub address: String,
}

/// Recovery task descriptor.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrRecoveryTask {
    pub extent_id: u64,
    pub replace_id: u64,
    pub node_id: u64,
    pub start_time: i64,
}

/// Completed recovery task.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MgrRecoveryTaskDone {
    pub task: MgrRecoveryTask,
    pub ready_disk_id: u64,
}

// ── Generic response ────────────────────────────────────────────────────────

/// Generic code + message response shared across many RPCs.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct CodeResp {
    pub code: u8,
    pub message: String,
}

pub const CODE_OK: u8 = 0;
pub const CODE_NOT_FOUND: u8 = 1;
pub const CODE_INVALID_ARGUMENT: u8 = 2;
pub const CODE_PRECONDITION: u8 = 3;
pub const CODE_ERROR: u8 = 4;
pub const CODE_NOT_LEADER: u8 = 5;
/// F-lease-preempt: `AcquireLease(force=true)` against a held writer
/// surfaces this; the response's `lease.ttl_secs` is repurposed to
/// carry the grace milliseconds REMAINING in the current revoke
/// window. Client retries the acquire after that delay (or sooner
/// if it sees a `WriterClosed` invalidation).
pub const CODE_REVOKE_PENDING: u8 = 6;
/// F-KEY-NS D7 Layer-A: a put-class write whose key falls in NO registered
/// namespace prefix. Semantics = NotFound-class (the target namespace does not
/// exist). **Defined in SD-1, RETURNED by the PS Layer-A gate in SD-2.** 7/8/9
/// are already taken by `partition_rpc` (Unavailable / RegionEpochStale /
/// Fenced), so this is 10 to stay collision-free across both wire modules.
pub const CODE_NAMESPACE_UNKNOWN: u8 = 10;

// ── StreamManagerService request/response types ────────────────────────────

// --- Status ---
// Request: empty payload (0 bytes)
// Response: CodeResp

// --- AcquireOwnerLock ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct AcquireOwnerLockReq {
    pub owner_key: String,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct AcquireOwnerLockResp {
    pub code: u8,
    pub message: String,
    pub owner_epoch: i64,
}

// --- RegisterNode ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RegisterNodeReq {
    pub addr: String,
    pub disk_uuids: Vec<String>,
    /// F099-M: per-shard ports the extent-node listens on. Empty = legacy
    /// single-thread mode (clients route all extents to `addr`).
    pub shard_ports: Vec<u16>,
    /// F191: extent-node's control-plane listener address (typically
    /// `host:port + 1000`). Empty = caller wants the manager to fall back
    /// to the data-plane `addr` for DF (legacy / mid-upgrade behaviour).
    pub control_address: String,
    /// F-EN-DYNSHARD M0: stable node identity (UUID v4), decoupled from the
    /// network address so a node can change IP / shard-port layout across
    /// restarts and still be recognised as the SAME node (mirrors the PS's
    /// `ps_id`-vs-address split). Empty = legacy uuid-less caller (matched by
    /// address, then adopted). The manager scans `MgrNodeInfo.node_uuid`
    /// (the identity rides INSIDE the persisted node record — no sidecar
    /// index); the layout change is a same-commit stop-world deploy requiring
    /// an etcd reset (repo convention — no rolling upgrade).
    pub node_uuid: String,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RegisterNodeResp {
    pub code: u8,
    pub message: String,
    pub node_id: u64,
    /// uuid -> disk_id mapping
    pub disk_uuids: Vec<(String, u64)>,
}

// --- CreateStream ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct CreateStreamReq {
    pub replicates: u32,
    pub ec_data_shard: u32,
    pub ec_parity_shard: u32,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct CreateStreamResp {
    pub code: u8,
    pub message: String,
    pub stream: Option<MgrStreamInfo>,
    pub extent: Option<MgrExtentInfo>,
}

// --- StreamInfo ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct StreamInfoReq {
    pub stream_ids: Vec<u64>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct StreamInfoResp {
    pub code: u8,
    pub message: String,
    /// (stream_id, MgrStreamInfo) pairs
    pub streams: Vec<(u64, MgrStreamInfo)>,
    /// (extent_id, MgrExtentInfo) pairs
    pub extents: Vec<(u64, MgrExtentInfo)>,
}

// --- ExtentInfo ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtentInfoReq {
    pub extent_id: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtentInfoResp {
    pub code: u8,
    pub message: String,
    pub extent: Option<MgrExtentInfo>,
}

// --- NodesInfo ---
// Request: empty payload (0 bytes)
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct NodesInfoResp {
    pub code: u8,
    pub message: String,
    /// (node_id, MgrNodeInfo) pairs
    pub nodes: Vec<(u64, MgrNodeInfo)>,
    /// (disk_id, MgrDiskInfo) pairs
    pub disks_info: Vec<(u64, MgrDiskInfo)>,
}

// --- CheckCommitLength ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct CheckCommitLengthReq {
    pub stream_id: u64,
    pub owner_key: String,
    pub owner_epoch: i64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct CheckCommitLengthResp {
    pub code: u8,
    pub message: String,
    pub stream_info: Option<MgrStreamInfo>,
    pub end: u64,
    pub last_ex_info: Option<MgrExtentInfo>,
}

// --- StreamAllocExtent ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct StreamAllocExtentReq {
    pub stream_id: u64,
    pub owner_key: String,
    pub owner_epoch: i64,
    /// How to seal the CURRENT tail before allocating the new extent:
    /// - `Some(c)` — AUTHORITATIVE: seal at EXACTLY `c`, do NOT probe. `c` is
    ///   the writer's own all-replica-acked commit (`state.commit`) captured at
    ///   a QUIESCED point via the `SealCommit` worker handshake (in-flight
    ///   drained first), or a known exact end (preemptive roll). Honoured even
    ///   for `Some(0)` (an empty tail where nothing was ever all-acked → sealed
    ///   empty, `sealed = true, sealed_length = 0`). This prevents the F227
    ///   phantom seal: a probe over reachable members can capture a
    ///   speculative/un-acked byte that only one (soon-dead) member holds,
    ///   sealing at a length no replica durably retains (the seed=13 stuck-
    ///   recovery bug).
    /// - `None` — PROBE: the writer has no commit cursor of its own (new-owner
    ///   takeover / sealed-tail init); the manager derives the seal via
    ///   `compute_commit_seal` (seal-over-reachable). Moot when the tail is
    ///   already sealed (the existing seal is preserved untouched).
    pub seal_commit: Option<u64>,
    /// F190: per-stream "recently failed" node ids the writer wants the
    /// manager to skip when picking nodes for the new extent. Empty Vec
    /// means "no exclusions" (legacy / cold-start clients). The manager
    /// filters candidate nodes by this set before scoring; if the filter
    /// empties the pool, it falls back to the full set rather than
    /// blocking allocation on stale excludes.
    pub exclude_node_ids: Vec<u64>,
    /// BUG2-IDEMPOTENT-ROLL: the extent id the writer captured `seal_commit`
    /// for (the tail at SealCommit-handshake time). `0` = no specific target
    /// (legacy / probe / `None` seal — seal the current tail as before).
    ///
    /// Makes seal-and-roll IDEMPOTENT on retry. A retried alloc (first attempt
    /// succeeded on the manager — sealed the tail + rolled a fresh one — but its
    /// response was lost) re-sends the SAME `seal_commit` for the SAME old tail.
    /// Without this the manager would seal the now-current FRESH tail at the
    /// stale `seal_commit`, over-sealing an extent that does not durably hold
    /// that many bytes → unrecoverable extent → any partition replaying it
    /// WAL-FAILSTOPs and never opens (chaos seed=603 split-child wedge). With
    /// it, the manager seals ONLY when the current tail still equals
    /// `seal_extent_id`; otherwise the requested target was already sealed/rolled
    /// → idempotent no-op (return the current tail untouched).
    pub seal_extent_id: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct StreamAllocExtentResp {
    pub code: u8,
    pub message: String,
    pub stream_info: Option<MgrStreamInfo>,
    pub last_ex_info: Option<MgrExtentInfo>,
}

// --- PunchHoles ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PunchHolesReq {
    pub stream_id: u64,
    pub owner_key: String,
    pub owner_epoch: i64,
    pub extent_ids: Vec<u64>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PunchHolesResp {
    pub code: u8,
    pub message: String,
    pub stream: Option<MgrStreamInfo>,
}

// --- Truncate ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct TruncateReq {
    pub stream_id: u64,
    pub owner_key: String,
    pub owner_epoch: i64,
    pub extent_id: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct TruncateResp {
    pub code: u8,
    pub message: String,
    pub updated_stream_info: Option<MgrStreamInfo>,
}

// --- MultiModifySplit ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MultiModifySplitReq {
    pub part_id: u64,
    pub owner_key: String,
    pub owner_epoch: i64,
    pub mid_key: Vec<u8>,
    pub log_stream_sealed_length: u64,
    pub row_stream_sealed_length: u64,
    pub meta_stream_sealed_length: u64,
}

// Response: CodeResp

// ── PartitionManagerService request/response types ─────────────────────────

// --- RegisterPs ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RegisterPsReq {
    pub ps_id: u64,
    pub address: String,
}
// Response: CodeResp

// --- UpsertPartition ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct UpsertPartitionReq {
    pub meta: MgrPartitionMeta,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct UpsertPartitionResp {
    pub code: u8,
    pub message: String,
    pub part_id: u64,
}

// --- GetRegions ---
// Request: empty payload (0 bytes)
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetRegionsResp {
    pub code: u8,
    pub message: String,
    /// (part_id, MgrRegionInfo) pairs
    pub regions: Vec<(u64, MgrRegionInfo)>,
    /// (ps_id, MgrPsDetail) pairs
    pub ps_details: Vec<(u64, MgrPsDetail)>,
    /// F099-K: per-partition listener addresses (`host:port`). Populated
    /// by `RegisterPartitionAddr` calls from the PS; one entry per open
    /// partition. Clients prefer this over `ps_details[ps_id].address`
    /// when present, so traffic is routed to the specific partition's
    /// listener thread (Seastar-style thread-per-shard).
    pub part_addrs: Vec<(u64, String)>,
}

// --- HeartbeatPs ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct HeartbeatPsReq {
    pub ps_id: u64,
}
// Response: CodeResp

// --- RegisterPartitionAddr (F099-K) ---
// PS calls this once per partition after binding that partition's
// dedicated TcpListener. Manager records `(part_id -> address)` in memory
// and returns it via `GetRegionsResp.part_addrs`. Clients use it to
// target the exact CPU-shard listener that owns the partition.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RegisterPartitionAddrReq {
    pub ps_id: u64,
    pub part_id: u64,
    pub address: String,
}
// Response: CodeResp

// --- ReconcileExtents (F109) ---
// Extent node calls this on startup (after `load_extents`) with every
// `extent_id` it found on disk. Manager checks each id against
// `s.extents` and returns the subset that no longer exists — those are
// orphans that the node should unlink (their refcount went to 0 while
// the node was offline or while the manager's in-memory pending-delete
// queue was lost to a manager restart).
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct ReconcileExtentsReq {
    pub node_id: u64,
    pub extent_ids: Vec<u64>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct ReconcileExtentsResp {
    pub code: u8,
    pub message: String,
    /// Subset of `req.extent_ids` that the manager no longer knows about.
    /// Node should unlink the corresponding `.dat` + `.meta` files.
    pub garbage: Vec<u64>,
}

// ── Extent node RPC types needed by the manager ────────────────────────────
// The manager calls extent nodes for alloc/commit_length/re_avali/df/recovery/ec.
// These duplicate the minimal subset from extent_rpc.rs to avoid manager→stream dep.

/// AllocExtent request (manager → extent node).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtAllocExtentReq {
    pub extent_id: u64,
}

/// AllocExtent response (extent node → manager).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtAllocExtentResp {
    pub code: u8,
    pub disk_id: u64,
    pub message: String,
}

/// ReAvali request (manager → extent node).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtReAvaliReq {
    pub extent_id: u64,
    pub eversion: u64,
}

/// Generic code response from extent node.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtCodeResp {
    pub code: u8,
    pub message: String,
}

/// RequireRecovery request (manager → extent node).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtRequireRecoveryReq {
    pub task: MgrRecoveryTask,
}

/// Df request (manager → extent node).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtDfReq {
    pub tasks: Vec<MgrRecoveryTask>,
    pub disk_ids: Vec<u64>,
}

/// Df response (extent node → manager).
///
/// F-unify: `disk_status` now nests the canonical `extent_rpc::DiskStatus`
/// (the pure-wire `ExtDiskStatus` mirror was deleted — it had no manager
/// domain equivalent, so it was pure duplication with twin-drift risk).
/// `done_tasks` keeps the manager DOMAIN type `MgrRecoveryTaskDone`
/// (persisted in etcd `recoveryTasks/`, woven into the F207 inflight
/// ledger) — that is a legitimate domain/wire separation, NOT a mirror,
/// so it is intentionally preserved. rkyv layout is unchanged either way.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtDfResp {
    pub done_tasks: Vec<MgrRecoveryTaskDone>,
    pub disk_status: Vec<(u64, crate::extent_rpc::DiskStatus)>,
    /// F-EN-DYNSHARD M1b: the EN's echoed identity (see `extent_rpc::DfResp`).
    /// Appended in the SAME order as `DfResp` so the manager decodes the EN's
    /// `DfResp` bytes into this struct unchanged. Empty when the EN did not
    /// self-register (`--advertise` unset).
    pub node_uuid: String,
    pub advertise_addr: String,
    pub shard_ports: Vec<u16>,
}

// --- UpdateStreamEc (FOPS-03) ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct UpdateStreamEcReq {
    pub stream_id: u64,
    pub ec_data_shard: u32,
    pub ec_parity_shard: u32,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct UpdateStreamEcResp {
    pub code: u8,
    pub message: String,
    pub stream: Option<MgrStreamInfo>,
}

/// DeleteExtent request (manager → extent node, F109).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtDeleteExtentReq {
    pub extent_id: u64,
}

/// ConvertToEc request (manager → extent node coordinator).
///
/// `eversion` is the new manager-decided eversion that every target
/// node (data + parity) must adopt locally during the conversion.
/// Pairs with `apply_ec_conversion_done` on the manager so that
/// subsequent stale-cache reads see a mismatch and refresh.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtConvertToEcReq {
    pub extent_id: u64,
    pub data_shards: u32,
    pub parity_shards: u32,
    pub target_addrs: Vec<String>,
    pub eversion: u64,
    /// F211-D Tier 2: owner-lock owner_epoch threaded from the manager's
    /// `MgrEcDispatchInflight.owner_epoch`. Coord forwards into each
    /// `WriteShardReq.owner_epoch` / `CommitEcShardReq.owner_epoch`. `0` =
    /// no-fence (legacy / memory-only mode).
    pub owner_epoch: i64,
}

// ── CommitLength binary codec (hot path, duplicated from extent_rpc) ───────

/// CommitLengthRequest: 16 bytes. [extent_id: u64 LE][owner_epoch: i64 LE]
///
/// Wire contract on `owner_epoch`: see `extent_rpc::CommitLengthReq` docstring.
/// Manager-side construction MUST pass a strictly-positive owner_epoch
/// (the caller's validated owner-lock claim). For fence-free probes
/// without an owner context, use `ExtProbeExtentReq` instead.
pub struct ExtCommitLengthReq {
    pub extent_id: u64,
    pub owner_epoch: i64,
}

impl ExtCommitLengthReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u64_le(self.extent_id);
        buf.put_i64_le(self.owner_epoch);
        buf.freeze()
    }
}

/// CommitLengthResponse: 9 bytes. [code: u8][length: u64 LE]
pub struct ExtCommitLengthResp {
    pub code: u8,
    pub length: u64,
}

impl ExtCommitLengthResp {
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

// ── ProbeExtent binary codec (manager-only, duplicated from extent_rpc) ────

/// `MSG_PROBE_EXTENT` request: 8 bytes. `[extent_id: u64 LE]`
///
/// See `extent_rpc::ProbeExtentReq` for full semantics. Manager
/// construction sites: `commit_length_on_node`'s probe sibling
/// `probe_extent_on_node` (recovery liveness check + autumn-client
/// info open-extent display).
pub struct ExtProbeExtentReq {
    pub extent_id: u64,
}

impl ExtProbeExtentReq {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64_le(self.extent_id);
        buf.freeze()
    }
}

/// `MSG_PROBE_EXTENT` response: 9 bytes (same shape as
/// `ExtCommitLengthResp`). `code` is `CODE_OK` / `CODE_NOT_FOUND`.
pub type ExtProbeExtentResp = ExtCommitLengthResp;

// ── F183: partition merge + policy advisory ────────────────────────────────

// --- MultiModifyMerge ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MultiModifyMergeReq {
    pub survivor_part_id: u64,
    pub victim_part_id: u64,
    pub owner_key: String,
    pub owner_epoch: i64,
    /// commit_length per stream type, indexed [0]=survivor, [1]=victim
    pub log_sealed_lengths: [u64; 2],
    pub row_sealed_lengths: [u64; 2],
    pub meta_sealed_lengths: [u64; 2],
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MultiModifyMergeResp {
    pub code: u8,
    pub message: String,
    /// extent_id of the freshly allocated empty tail for survivor's
    /// log_stream; used as vp_head when writing the merged
    /// TableLocations checkpoint on the PS.
    pub new_log_tail_extent_id: u64,
}

// --- F185: MergePartitions (manager-orchestrated merge) ---
//
// Drop-in replacement for the F183 client-orchestrated merge sequence.
// Caller passes only the two partition ids; the manager handles owner-
// lock acquisition, PS freeze, commit_length capture, and the multi-
// modify-merge txn internally. This relocates the orchestration's
// failure boundary onto the leader-fenced + etcd-replayable manager.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MergePartitionsReq {
    pub survivor_part_id: u64,
    pub victim_part_id: u64,
    /// F-FS-GEOM-DECLARED step 4: override the sacred-boundary refusal. Merging
    /// two partitions destroys the boundary between them; when that boundary is
    /// a recorded presplit point, the merge silently undoes an operator's
    /// declared layout (for fs lane boundaries the symptom is that every
    /// SUBSEQUENT large file stripes narrower, with no error anywhere). Refused
    /// by default, allowed with this flag.
    pub force: bool,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MergePartitionsResp {
    pub code: u8,
    pub message: String,
    pub new_log_tail_extent_id: u64,
}

// --- F-REGION-REBALANCE: active region→PS load rebalance ---
//
// `rebalance_regions` (the eviction path) is STICKY — it keeps a region on its
// currently-registered PS and only reassigns regions whose PS is unregistered.
// So after a rolling restart every partition can pile onto whichever PS
// registered first, and nothing ever re-spreads them (a restart doesn't move a
// registered PS's regions). This RPC is the active balancer (WAS PM /
// Bigtable-master / TiKV-PD `balance-region` equivalent): it reassigns
// partitions from the most-loaded PS to the least-loaded until balanced,
// bounded by `max_moves` so the partition-reopen storm on the targets is
// throttled. `max_moves == 0` = as many as needed to balance.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct RebalanceRegionsReq {
    pub max_moves: u32,
}

/// One reassignment the manager performed: `part_id` moved `from_ps`→`to_ps`.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RebalanceMove {
    pub part_id: u64,
    pub from_ps: u64,
    pub to_ps: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct RebalanceRegionsResp {
    pub code: u8,
    pub message: String,
    /// The moves applied (empty when already balanced). `moved == moves.len()`.
    pub moves: Vec<RebalanceMove>,
    pub moved: u32,
}

// --- ReportPartitionLoad (PS → manager periodic metrics) ---
//
// F202 wire change (backward-incompatible): adds 5 new fields
// surfacing per-partition SST dead-data breakdown and minor-compact
// debt. Same-commit upgrade required (cluster.sh stops all roles
// before restart).
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct PartitionLoad {
    pub part_id: u64,
    pub size_bytes: u64,
    pub req_per_sec: u32,
    /// Real write throughput: value bytes/sec ingested by Put (the bytes/sec
    /// companion to req_per_sec's IOPS).
    pub write_bytes_per_sec: u64,
    /// Real read throughput: value bytes/sec served by GET (resolved-value path;
    /// large-VP client-direct reads from the EN are not counted here).
    pub read_bytes_per_sec: u64,
    pub imm_full_per_sec: u32,
    pub p99_us: u32,
    /// F187: Σ reclaimable bytes on still-live sealed log_stream extents.
    /// "GC debt" — rises with deletes/overwrites, drops on `punch_holes`.
    pub gc_debt_bytes: u64,
    /// F187: bytes that the next compact tick would feed into `do_compact`
    /// (overlap-tagged tables when has_overlap == 1, else
    /// pickup_tables(...)). "Major-compaction debt".
    pub pending_compaction_bytes: u64,
    /// F187: 1 while `background_gc_loop` is inside `run_gc`, else 0.
    pub gc_inflight: u32,
    /// F187: 1 while `background_compact_loop` is inside `do_compact`,
    /// else 0.
    pub compact_inflight: u32,
    /// F187: unix-epoch seconds of the last successful GC `punch_holes`.
    /// 0 = never since process start.
    pub last_gc_at: i64,
    /// F187: unix-epoch seconds of the last successful `do_compact`.
    /// 0 = never since process start.
    pub last_compact_at: i64,
    /// F202: total bytes occupied by tombstone (op==2) records inside
    /// the partition's live SSTs. Drives external controllers'
    /// "should-major-compact" decisions; advisory-only here.
    pub sst_tombstone_bytes: u64,
    /// F202: total bytes occupied by SST records with `expires_at <= now`.
    /// Dead-data driver alongside tombstones.
    pub sst_expired_bytes: u64,
    /// F202: total bytes of SST records whose keys fall outside the
    /// partition's current `rg` range (only > 0 when `has_overlap == 1`,
    /// i.e. post-split CoW-shared SSTables haven't been compacted yet).
    pub sst_out_of_range_bytes: u64,
    /// F202: total bytes the next *minor* compact tick would feed into
    /// `do_compact` (size-tiered + head-extent pickup output). Distinct
    /// from `pending_compaction_bytes` which captures the major-compact
    /// pickup. Both can be non-zero simultaneously.
    pub minor_compact_pending_bytes: u64,
    /// F202: number of sealed log_stream extents currently in
    /// `extent_ids[..len-1]` (informational; helps OP correlate
    /// `gc_debt_bytes` against the extent population).
    pub sealed_log_extent_count: u32,
    /// F-OVERVIEW-OPENTAIL: Σ committed bytes on this partition's three
    /// stream OPEN-TAIL extents (log + row + meta), as the PS knows them
    /// locally. An open extent's manager-side `sealed_length` is 0, so the
    /// manager's authoritative sealed-length sum (the cluster-overview
    /// `live_size`) excludes exactly these bytes — a fully-compacted or
    /// log-heavy partition whose data lives entirely in open tails renders
    /// 0 B without this. The manager adds it to its sealed sum:
    /// `live_size = Σ sealed_length + open_tail_bytes`, matching what
    /// `autumn-op info --part` gets by probing the EN. Refreshed on the PS
    /// maintenance loop (throttled, non-blocking); 0 until first refresh.
    pub open_tail_bytes: u64,
    /// F-DF-WALDEBT: dead (overwritten/deleted) large-value bytes on the OPEN
    /// log_stream tail extent. `gc_debt_bytes` is sealed-only (GC can't punch
    /// an unsealed tail), so a log-heavy / all-open-tail partition reports
    /// `gc_debt_bytes = 0` while still holding real dead bytes here. Together
    /// `gc_debt_bytes + open_tail_dead_bytes` is the partition's full
    /// reclaimable WAL debt. Refreshed on the PS GC tick from the persisted SST
    /// discard maps; 0 until the first tick.
    pub open_tail_dead_bytes: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct ReportPartitionLoadReq {
    pub ps_id: u64,
    pub partitions: Vec<PartitionLoad>,
}

// --- GetPolicyCandidates (advisory) ---
//
// F210-F1: **WIRE-STABILITY CONTRACT** — the numeric values below are
// frozen. New advisory kinds must be APPENDED with the next unused
// value (POLICY_KIND_EC = 6 → next would be 7). Renumbering breaks
// every external controller, dashboard, and historical log analysis.
// Operators discovered this when an off-by-one between this code
// (SPLIT=0..EC=6) and a stale CLAUDE.md note (which said "1..7")
// caused every Python `cluster/policy_controller.py` decision to
// fire on the wrong kind. Treat docs + this file + the const-dump
// (`MSG_GET_POLICY_KIND_NAMES`) as a CI-checkable triad — drift on
// any one is a wire-breaking event.
pub const POLICY_KIND_SPLIT: u8 = 0;
pub const POLICY_KIND_MERGE: u8 = 1;
/// F187: GC debt advisory — partition's `gc_debt_bytes` exceeds the
/// configured high-water threshold for `policy.required_buckets`
/// consecutive sliding-window buckets and the partition is outside the
/// gc cooldown window. Stage 1 is advisory-only — no auto-trigger.
pub const POLICY_KIND_GC: u8 = 2;
/// F187: major-compaction debt advisory — partition's
/// `pending_compaction_bytes` exceeds the configured high-water
/// threshold for `policy.required_buckets` consecutive buckets and
/// outside the compact cooldown. F202 renamed from `POLICY_KIND_COMPACT`
/// to disambiguate against the new minor-compact advisory.
pub const POLICY_KIND_MAJOR_COMPACT: u8 = 3;
/// F202 compatibility alias: pre-F202 callers used `POLICY_KIND_COMPACT`.
/// Same wire value (3). Marked `#[deprecated]` so new code prefers the
/// `_MAJOR_COMPACT` name.
#[deprecated(note = "use POLICY_KIND_MAJOR_COMPACT (F202 rename, same wire value)")]
pub const POLICY_KIND_COMPACT: u8 = POLICY_KIND_MAJOR_COMPACT;
/// F196 Stage D: hot/cold imbalance advisory. Emitted once per PS
/// when sustained `max(req_per_sec) / min(req_per_sec) ≥ HOT_COLD_RATIO`
/// AND hottest > `HOT_COLD_MIN_HOT_QPS`, OR the same imbalance on
/// `size_bytes`. `primary_part_id` = hottest partition, `secondary_part_id`
/// = coldest partition; `reason` carries `ps_id=N qps_ratio=N qps_hot=[…]
/// qps_cold=[…] size_ratio=N size_hot=[…] size_cold=[…]`.
/// `same_ps = true` always (the advisory is by definition per-PS).
pub const POLICY_KIND_HOT_COLD: u8 = 4;
/// F202: minor-compaction debt advisory. PS-local `pickup_tables`
/// would emit a non-empty set on the next tick and the partition's
/// `minor_compact_pending_bytes` exceeds the configured low-water
/// threshold over `required_buckets` consecutive buckets.
/// `primary_part_id` = partition; `secondary_part_id = 0`;
/// `reason` carries the byte volume.
pub const POLICY_KIND_MINOR_COMPACT: u8 = 5;
/// F202: EC-conversion advisory. A sealed log/row stream extent has
/// not been EC-converted yet AND its `sealed_length` exceeds the
/// minimum-payoff threshold (default 64 MiB — below that, EC
/// overhead outweighs the space savings, so we don't even suggest
/// it). `primary_part_id = 0` (EC is per-extent, not per-partition);
/// `secondary_part_id` carries the extent_id; `size_bytes` carries
/// `sealed_length`. Manager emits these from a direct scan of
/// `s.streams`, NOT from PartitionLoad.
pub const POLICY_KIND_EC: u8 = 6;
/// F-REGION-REBALANCE Phase B: CLUSTER-level region→PS load-balance advisory.
/// Emitted (at most one per tick) when the per-PS partition-count spread exceeds
/// the configured threshold. `primary_part_id = 0` / `secondary_part_id = 0`
/// (cluster-scoped, not a single partition/extent); `reason` carries the per-PS
/// counts + gap. Manager sources it from `regions` + `ps_nodes` directly, NOT
/// from PartitionLoad. The armed auto-policy controller actuates it via
/// `MSG_REBALANCE_REGIONS` with a bounded per-tick `max_moves`.
pub const POLICY_KIND_REBALANCE: u8 = 7;

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PolicyCandidate {
    pub kind: u8,               // POLICY_KIND_SPLIT or POLICY_KIND_MERGE
    pub primary_part_id: u64,   // split: target; merge: survivor
    pub secondary_part_id: u64, // split: 0; merge: victim
    pub reason: String,
    pub size_bytes: u64,
    pub req_per_sec: u32,
    pub imm_full_per_sec: u32,
    pub same_ps: bool,
    pub last_op_at: i64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct GetPolicyCandidatesReq {}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetPolicyCandidatesResp {
    pub code: u8,
    pub message: String,
    pub candidates: Vec<PolicyCandidate>,
}

// --- GetPolicyKindNames (F210-F1 const-dump) ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct GetPolicyKindNamesReq {}

/// `(name, value)` pairs for every defined `POLICY_KIND_*` constant in
/// THIS binary. External controllers should pull this at startup and
/// resolve string names locally rather than hardcoding numeric values
/// — the numeric values are wire-stable by contract (see freeze
/// docstring above the `POLICY_KIND_*` block), but new kinds may be
/// added at any binary release.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetPolicyKindNamesResp {
    pub code: u8,
    pub message: String,
    pub kinds: Vec<(String, u8)>,
}

/// Returns the canonical name → wire-value mapping for every
/// currently-defined `POLICY_KIND_*` constant. Update this list when
/// a new kind is added (the binary is the source of truth for what
/// it serves; external tooling reads via `MSG_GET_POLICY_KIND_NAMES`).
pub fn policy_kind_names() -> Vec<(String, u8)> {
    vec![
        ("POLICY_KIND_SPLIT".to_string(), POLICY_KIND_SPLIT),
        ("POLICY_KIND_MERGE".to_string(), POLICY_KIND_MERGE),
        ("POLICY_KIND_GC".to_string(), POLICY_KIND_GC),
        (
            "POLICY_KIND_MAJOR_COMPACT".to_string(),
            POLICY_KIND_MAJOR_COMPACT,
        ),
        ("POLICY_KIND_HOT_COLD".to_string(), POLICY_KIND_HOT_COLD),
        (
            "POLICY_KIND_MINOR_COMPACT".to_string(),
            POLICY_KIND_MINOR_COMPACT,
        ),
        ("POLICY_KIND_EC".to_string(), POLICY_KIND_EC),
        ("POLICY_KIND_REBALANCE".to_string(), POLICY_KIND_REBALANCE),
    ]
}

// --- ReportDiskFailure (F192) ---
/// Generic transport/append failure on a known replica. The manager
/// does not act on `error_kind` directly today; it's surfaced for
/// future per-kind policy tuning (e.g. demote vs. trigger immediate
/// recovery) and for operator log readability.
pub const REPORT_DISK_FAILURE_KIND_GENERIC: u8 = 0;

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ReportDiskFailureReq {
    /// Replica node_id that produced the failure.
    pub node_id: u64,
    /// The extent the append/operation was targeting; carried so the
    /// manager log makes the failure attributable end-to-end.
    pub extent_id: u64,
    /// One of the `REPORT_DISK_FAILURE_KIND_*` constants.
    pub error_kind: u8,
    /// Reporter partition id (used by the manager's debounce / quorum
    /// — 3 distinct reporter_part_id within the eviction window).
    pub reporter_part_id: u64,
    /// Wall-clock ms when the failure was observed; informational
    /// only — manager uses its own Instant for the sliding window.
    pub ts_ms: i64,
}

// Response: fire-and-forget (req_id = 0). No reply needed; manager
// logs are the operator-facing surface.

// --- ForceEcConvert (F203) ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ForceEcConvertReq {
    /// Sealed-but-unconverted extent the OP wants to EC-encode now.
    pub extent_id: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ForceEcConvertResp {
    pub code: u8,
    pub message: String,
}

// --- GetPartitionDetail (F203) ---
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetPartitionDetailReq {
    pub part_id: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetPartitionDetailResp {
    pub code: u8,
    pub message: String,
    /// Latest cached PartitionLoad in the manager's policy window.
    /// `part_id == 0` if the partition has no metrics history (PS
    /// hasn't sent a `MSG_REPORT_PARTITION_LOAD` for it yet).
    pub load: PartitionLoad,
    /// Unix-epoch seconds of the bucket this load was stamped at.
    pub bucket_ts: i64,
}

// ── Dashboard cluster overview (MSG_GET_CLUSTER_OVERVIEW) ──────────────────
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct GetClusterOverviewReq {}

/// One compact partition row (no per-extent detail; that is lazy).
/// `live_size` sums the partition's distinct extents' `sealed_length`
/// (manager-authoritative, NO extent-node probe) — open-tail bytes are
/// excluded, so it is an overview estimate; the per-partition detail
/// (`info --part P`) probes for the exact figure.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PartitionOverview {
    pub part_id: u64,
    /// The serving PS INSTANCE id. Distinct from `ps_addr`, which under F099-K
    /// is the partition's OWN listener (`base_port + ord`) — many partitions on
    /// ONE PS each get a different `ps_addr`, so PS-count must group by `ps_id`.
    pub ps_id: u64,
    pub ps_addr: String,
    pub range_start: Vec<u8>,
    pub range_end: Vec<u8>,
    pub live_size: u64,
    pub total_extents: u32,
    pub log_stream: u64,
    pub row_stream: u64,
    pub meta_stream: u64,
    /// Latest reported IOPS (PartitionLoad.req_per_sec) from the manager's
    /// policy window; 0 if the PS hasn't reported load yet. Cheap (already in
    /// memory) so the overview can show per-partition IOPS + a cluster sum
    /// without per-partition detail RPCs.
    pub req_per_sec: u64,
    /// Latest reported write / read throughput (bytes/sec) from the policy window.
    pub write_bytes_per_sec: u64,
    pub read_bytes_per_sec: u64,
}

/// Per-node rollup — the one thing a client can't compute without the full
/// extent array: how many extent shards this node hosts (replicates ∪ parity).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct NodeOverview {
    pub node_id: u64,
    pub address: String,
    pub extent_count: u32,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetClusterOverviewResp {
    pub code: u8,
    pub message: String,
    pub partitions: Vec<PartitionOverview>,
    pub nodes: Vec<NodeOverview>,
    /// Cluster-aggregate IOPS = Σ partitions' latest req_per_sec.
    pub total_req_per_sec: u64,
    /// Cluster-aggregate write / read throughput (bytes/sec).
    pub total_write_bytes_per_sec: u64,
    pub total_read_bytes_per_sec: u64,
    /// Distinct serving PS instances (by `ps_id`, NOT `ps_addr`).
    pub ps_count: u32,
}

// ── Extent msg_type constants (needed by manager for node calls) ──────────

pub const EXT_MSG_COMMIT_LENGTH: u8 = 3;
pub const EXT_MSG_ALLOC_EXTENT: u8 = 4;
pub const EXT_MSG_DF: u8 = 5;
pub const EXT_MSG_REQUIRE_RECOVERY: u8 = 6;
pub const EXT_MSG_RE_AVALI: u8 = 7;
pub const EXT_MSG_CONVERT_TO_EC: u8 = 9;
pub const EXT_MSG_DELETE_EXTENT: u8 = 11;
pub const EXT_MSG_COMMIT_EC_SHARD: u8 = 12;
pub const EXT_MSG_PROBE_EXTENT: u8 = 14;

// ── F211 wire types ─────────────────────────────────────────────────────────
//
// All F211 message payloads use the same rkyv encode/decode helpers as the
// rest of this file. New `MgrNode*` types are append-only; existing wire
// shapes (MgrNodeInfo, MgrExtentInfo, etc.) are unchanged.

/// F211-A/B + F214-B: auto-tracked state byte. Wire-stable; APPEND-only.
pub const NODE_AUTO_STATE_ONLINE: u8 = 0;
pub const NODE_AUTO_STATE_SUSPECTED: u8 = 1;
/// F214-B: registered, never verified alive (no successful df yet).
/// Initial state for any node freshly added via `MSG_REGISTER_NODE`.
pub const NODE_AUTO_STATE_SUSPEND: u8 = 2;

/// F211-C: operator override kind. Wire-stable; new kinds APPEND-only.
pub const NODE_OVERRIDE_NONE: u8 = 0;
pub const NODE_OVERRIDE_FENCED: u8 = 1;
pub const NODE_OVERRIDE_MAINTENANCE: u8 = 2;

/// F211-C: rkyv'd value at etcd prefix `node_override/<node_id>`. Replayed
/// on leader failover; cleared by `mgr_clear_node_override` or by the
/// `NodeStateTracker.tick()` Maintenance-TTL sweep.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrNodeOverride {
    pub node_id: u64,
    /// `NODE_OVERRIDE_FENCED` or `NODE_OVERRIDE_MAINTENANCE`. `NONE` is
    /// never serialised (caller deletes the etcd key instead).
    pub kind: u8,
    pub set_at: i64,
    pub set_by: String,
    pub reason: String,
    /// Optional Maintenance TTL — unix-epoch seconds at which the
    /// `NodeStateTracker.tick()` automatically clears the override.
    /// `0` = no TTL (Fenced is permanent until explicit clear).
    pub expire_at: u64,
    /// F-EN-DYNSHARD M0: the fenced/decommissioned node's stable `node_uuid`,
    /// captured at override time. Load-bearing for the `decommissioned/`
    /// tombstone: `remove_node` deletes `nodes/<id>`, so after removal the
    /// node_id→uuid mapping only survives HERE — the re-register zombie check
    /// scans tombstones by this uuid so a removed node returning under its own
    /// identity is refused even though its `MgrNodeInfo` is gone. Empty for
    /// legacy/replayed overrides written before M0 (matches no uuid).
    pub node_uuid: String,
}

// --- ListNodeStates (F211-B) ---

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct ListNodeStatesReq {}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct NodeStateEntry {
    pub node_id: u64,
    pub address: String,
    /// `NODE_AUTO_STATE_*`.
    pub auto_state: u8,
    /// Seconds since the most recent successful df, or `u64::MAX` if
    /// the manager has never observed one (e.g. node registered but no
    /// df probe completed yet).
    pub last_heartbeat_secs_ago: u64,
    /// Seconds since auto_state flipped to Suspected, or 0 when Online.
    pub suspected_age_secs: u64,
    /// Operator override if any (`NODE_OVERRIDE_NONE` if absent).
    pub override_kind: u8,
    pub override_reason: String,
    pub override_set_by: String,
    pub override_set_at: i64,
    pub override_expire_at: u64,
    /// F-EN-DYNSHARD M1c: the node's stable identity + the shard ports it last
    /// registered (`shard_ports.len()` = its shard count). Lets `list-nodes`
    /// show the identity and verify a reshard took effect. Empty for a pre-M0
    /// node (impossible on a reset cluster) / legacy single-shard registration.
    pub node_uuid: String,
    pub shard_ports: Vec<u16>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ListNodeStatesResp {
    pub code: u8,
    pub message: String,
    pub nodes: Vec<NodeStateEntry>,
}

// --- ExtentHealthReport (F211-B) ---

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct ExtentHealthReq {
    /// Empty Vec = "every unhealthy extent". Non-empty = "every extent
    /// whose any-slot's node_id appears in this filter, healthy or not".
    pub node_id_filter: Vec<u64>,
    /// If true and `node_id_filter` is empty, return ALL extents
    /// regardless of health (used by `inspect-extent` paths). Default
    /// false (only unhealthy) keeps the typical OP poll cheap.
    pub include_healthy: bool,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtentSlotHealth {
    /// 0..(K+M-1). Layered: data shards first, then parity shards.
    pub slot_index: u32,
    pub node_id: u64,
    /// Mirror of `MgrExtentInfo.avali bit & (1<<slot)`.
    pub avali: bool,
    /// Node's `NODE_AUTO_STATE_*` at report time.
    pub auto_state: u8,
    /// `NODE_OVERRIDE_*` at report time.
    pub override_kind: u8,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtentHealth {
    pub extent_id: u64,
    pub eversion: u64,
    pub sealed_length: u64,
    pub ec_converted: bool,
    pub slots: Vec<ExtentSlotHealth>,
    /// True iff any slot has `avali == false` OR any slot's node is
    /// Suspected / Fenced / Maintenance.
    pub unhealthy: bool,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ExtentHealthResp {
    pub code: u8,
    pub message: String,
    pub extents: Vec<ExtentHealth>,
}

// --- ListEcInflightMarkers (F211-B) ---

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct ListEcInflightMarkersReq {}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct InflightWithCoordState {
    pub extent_id: u64,
    pub coord_node_id: u64,
    pub coord_auto_state: u8,
    pub coord_override_kind: u8,
    pub target_nodes: Vec<u64>,
    pub data_shards: u32,
    pub new_eversion: u64,
    /// Marker `started_at` from `MgrExtentInflightRecord` (unix-epoch s).
    pub started_at: i64,
    /// Convenience: now - started_at, in seconds.
    pub age_secs: i64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ListEcInflightMarkersResp {
    pub code: u8,
    pub message: String,
    pub markers: Vec<InflightWithCoordState>,
}

// --- FenceNode / SetNodeMaintenance / ClearNodeOverride / RemoveNode (F211-C) ---

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct FenceNodeReq {
    pub node_id: u64,
    pub reason: String,
    pub set_by: String,
    /// If true, skip the capacity precheck (operator accepts risk).
    pub force: bool,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct SetNodeMaintenanceReq {
    pub node_id: u64,
    pub reason: String,
    pub set_by: String,
    /// `0` = no TTL.
    pub expire_at: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct ClearNodeOverrideReq {
    pub node_id: u64,
    pub set_by: String,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct RemoveNodeReq {
    pub node_id: u64,
    pub set_by: String,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct RemoveNodeResp {
    pub code: u8,
    pub message: String,
    /// Populated on `CODE_PRECONDITION` so the OP sees exactly which
    /// extents / markers still reference the node.
    pub blocking_extent_ids: Vec<u64>,
    pub blocking_marker_extent_ids: Vec<u64>,
}

// --- RecoveryStats (F211-H) ---

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct RecoveryStatsReq {}

/// Per-(extent, slot) backoff detail so `recovery-stats` can show WHY a
/// recovery is backing off (the last dispatch error), since when, and
/// until when — not just a count.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct RecoveryBackoffEntry {
    pub extent_id: u64,
    pub slot: u32,
    pub consecutive_failures: u32,
    /// Unix epoch seconds of the last failed dispatch attempt.
    pub last_attempt_at: i64,
    /// Unix epoch seconds when the dispatch loop will next retry this
    /// (extent, slot): `last_attempt_at + 2^consecutive_failures` capped
    /// at 300 s.
    pub next_retry_at: i64,
    /// The most recent dispatch error string.
    pub reason: String,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct RecoveryStatsResp {
    pub code: u8,
    pub message: String,
    pub global_inflight: u32,
    pub max_global: u32,
    pub max_per_source: u32,
    pub max_per_target: u32,
    /// `(node_id, inflight_count)` snapshot of per-source counters.
    pub per_source: Vec<(u64, u32)>,
    pub per_target: Vec<(u64, u32)>,
    /// Number of (extent, slot) entries currently in exponential
    /// backoff (consecutive_failures > 0).
    pub backoff_entries: u32,
    /// Per-entry backoff detail (one per `backoff_entries`): which
    /// (extent, slot), how many consecutive failures, since when, next
    /// retry time, and the last failure reason.
    pub backoff: Vec<RecoveryBackoffEntry>,
}

// --- Audit log (F211-I) ---

pub const AUDIT_OP_FENCE_NODE: u8 = 1;
pub const AUDIT_OP_SET_NODE_MAINTENANCE: u8 = 2;
pub const AUDIT_OP_CLEAR_NODE_OVERRIDE: u8 = 3;
pub const AUDIT_OP_REMOVE_NODE: u8 = 4;
pub const AUDIT_OP_FORCE_EC_CONVERT: u8 = 5;
pub const AUDIT_OP_FORCE_ABANDON_EC_MARKER: u8 = 6;

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrAuditEntry {
    /// `AUDIT_OP_*`.
    pub op: u8,
    pub node_id: u64,
    pub extent_id: u64,
    pub by: String,
    pub reason: String,
    /// 0 = OK; non-zero = `CODE_*` failure.
    pub result_code: u8,
    pub result_message: String,
    /// Unix-epoch nanoseconds for unique key suffix.
    pub ts_ns: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct QueryAuditLogReq {
    /// `0` = no filter; otherwise only entries with matching `op` byte.
    pub op_filter: u8,
    /// `0` = no filter.
    pub node_id_filter: u64,
    /// Unix-epoch seconds; `0` = no lower bound.
    pub since_ts_s: i64,
    /// Unix-epoch seconds; `0` = no upper bound.
    pub until_ts_s: i64,
    /// `0` = unlimited (caller should cap).
    pub limit: u32,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct QueryAuditLogResp {
    pub code: u8,
    pub message: String,
    pub entries: Vec<MgrAuditEntry>,
}

// --- GetClusterId ---
// F214-A: read-only cluster identity exposed via MSG_GET_CLUSTER_ID.
//
// ⚠️ R1 FREEZE (coco P1): GetClusterIdReq/Resp ARE the version-negotiation
// channel — every long-lived process decodes this resp BEFORE any compat
// decision can run. If this struct's rkyv layout ever changes again, a
// newer peer fails the decode against an older manager and the interval
// handshake becomes unreachable (the refusal is still LOUD, but rolling
// upgrades regress to same-commit). From R1 on these two structs are
// FROZEN; additions go in NEW msg_types, never here.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetClusterIdReq {}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetClusterIdResp {
    pub code: u8,
    pub message: String,
    /// UUID string. Empty when `code` != `CODE_OK`.
    pub cluster_id: String,
    /// WIRE-1: the responding manager's `autumn_rpc::WIRE_FINGERPRINT`.
    /// R1 relaxed the equality check to `wire_compat_check` (interval
    /// overlap, fingerprint kept as the same-build fast path + for
    /// diagnostics in the refusal message).
    pub wire_fingerprint: String,
    /// R1: the responding manager's `[WIRE_VERSION_MIN, WIRE_VERSION_MAX]`.
    /// Callers refuse to join when their own interval has no overlap.
    pub wire_version_min: u32,
    pub wire_version_max: u32,
    /// R1: the persisted cluster_version (the operator-bumped feature
    /// gate, NOT this binary's wire version). New wire forms / persisted
    /// formats versioned N may only be EMITTED once cluster_version >= N.
    /// 0 when the manager hasn't bootstrapped it yet.
    pub cluster_version: u32,
}

// --- ClusterVersion (R1 rolling upgrade) --------------------------------
// Persisted in etcd as ASCII decimal (format-stable across serialization
// eras). Read servable from any replica; bump is leader-only, monotonic,
// and exactly +1 per call (design §3-R1).

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetClusterVersionReq {}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct GetClusterVersionResp {
    pub code: u8,
    pub message: String,
    pub cluster_version: u32,
    /// The responding manager binary's own wire interval — surfaced so
    /// `autumn-op cluster-version` can show operators how much headroom
    /// a bump has (`cluster_version < wire_version_max` ⇒ bump possible).
    pub wire_version_min: u32,
    pub wire_version_max: u32,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct BumpClusterVersionReq {
    /// Target version. Must equal current cluster_version + 1, and must
    /// not exceed the manager's own WIRE_VERSION_MAX.
    pub to: u32,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct BumpClusterVersionResp {
    pub code: u8,
    pub message: String,
    /// The cluster_version after this call (the new value on success,
    /// the unchanged current value on refusal).
    pub cluster_version: u32,
}

// --- ReportCorruptReplica (WAL self-heal A5) ---------------------------------
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ReportCorruptReplicaReq {
    /// Partition whose replay found the corruption — used to CAS-validate the
    /// reporter is the current owner (`owner_epochs["partition/<id>"]`).
    pub partition_id: u64,
    /// The reporting PS's partition owner_epoch (fencing — a stale PS whose
    /// epoch no longer matches is rejected).
    pub owner_epoch: i64,
    /// The partition's log_stream id. The manager verifies `extent_id` is a
    /// member of this stream (scoping: an owner can only isolate replicas of an
    /// extent in the stream it is replaying — mirrors punch_holes/truncate
    /// operating only on their named stream's extents).
    pub log_stream_id: u64,
    pub extent_id: u64,
    /// The extent eversion the PS saw; the manager rejects the report if it
    /// has since changed (a concurrent recovery / EC bump).
    pub eversion: u64,
    /// Node ids whose copy of `extent_id` failed the per-record CRC during
    /// replay (the PS confirmed at least one OTHER replica decodes clean).
    pub corrupt_node_ids: Vec<u64>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ReportCorruptReplicaResp {
    pub code: u8,
    pub message: String,
}

// ── cluster-df wire types (MSG_CLUSTER_DF) ───────────────────────────────────

/// `MSG_CLUSTER_DF` request — empty (the whole cluster summary; no params).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ClusterDfReq {}

/// Per-node capacity rollup (sum over the node's online disks).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct NodeCapWire {
    pub node_id: u64,
    pub total: u64,
    pub free: u64,
    /// Σ this node's per-disk `DiskStatus.extent_bytes` (real autumn footprint).
    pub extent_bytes: u64,
    /// false = the node's df probe failed this cycle (unknown != truly offline).
    pub online: bool,
}

/// `MSG_CLUSTER_DF` response — raw u64 facts only; the consumer computes the
/// amplification factor (`physical_used / logical_stored`) and the writable
/// RANGE (`[raw_free/3, raw_free/best_ec_factor]`). No floats on the wire.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ClusterDfResp {
    pub code: u8,
    pub message: String,
    /// Σ all online disks' raw capacity / free (statvfs truth).
    pub raw_total: u64,
    pub raw_free: u64,
    /// Σ all nodes' `extent_bytes` — exact autumn physical footprint
    /// (replicas + EC shards + open tails, no amplification formula).
    pub physical_used: u64,
    /// Manager's read-only Σ distinct sealed_length (de-amplified, sealed-only).
    pub logical_stored: u64,
    /// F-DF-OPENTAIL: Σ PS-reported open-tail committed bytes across all
    /// partitions (log + row + meta OPEN tails, one copy — open tails are
    /// refs=1 partition-private, so no CoW dedup needed). physical_used
    /// INCLUDES these bytes (they are largely LIVE large-value / VP data
    /// sitting in the open log tail), so the amplification MUST use
    /// `logical_stored + logical_open_tail` as the denominator — otherwise a
    /// partition whose data lives in open tails inflates amp ~15× (physical
    /// counts the open bytes, sealed-only logical drops them). 0 until the PS
    /// reports (falls back to sealed-only).
    pub logical_open_tail: u64,
    /// F-DF-WALDEBT: Σ reclaimable DEAD bytes across all partitions — sealed
    /// (`PartitionLoad.gc_debt_bytes`) + open-tail
    /// (`PartitionLoad.open_tail_dead_bytes`). The dead fraction of the logical
    /// footprint; `physical_used` carries these bytes (they occupy replicas
    /// until GC punches them). Pre-F-DF-WALDEBT a log-heavy partition's
    /// open-tail dead bytes were invisible (gc_debt is sealed-only). Lets `df`
    /// print a dead-vs-live breakdown. 0 until the PS reports.
    pub logical_wal_debt: u64,
    /// Online EN count — bounds the best achievable EC shape for the writable
    /// range upper bound (K = min(4, node_count-1)).
    pub node_count: u64,
    /// Snapshot freshness (ms since unix epoch) — RAW/physical refreshed by
    /// node_health_loop; `logical_last_update_ms` by the read-only scan.
    pub last_update_ms: u64,
    pub logical_last_update_ms: u64,
    pub per_node: Vec<NodeCapWire>,
}

// ── F-ioring-lease-1 wire types ────────────────────────────────────────────

/// Daemon kind discriminant for `MgrClientId`. Manager treats both kinds
/// identically — this is purely diagnostic so `autumn-op` can tell which
/// daemon owns a lease. Wire-stable; new variants APPEND only.
pub const LEASE_CLIENT_KIND_FUSE: u8 = 1;
pub const LEASE_CLIENT_KIND_IORING: u8 = 2;

/// Lease mode (Read or Write). Wire-stable; APPEND-only.
pub const LEASE_MODE_READ: u8 = 1;
pub const LEASE_MODE_WRITE: u8 = 2;

/// Invalidation kind (push reason). Wire-stable; APPEND-only.
pub const LEASE_INVAL_WRITER_CLOSED: u8 = 1;
pub const LEASE_INVAL_LEASE_REVOKED: u8 = 2;
pub const LEASE_INVAL_META_CHANGED: u8 = 3;
/// F-lease-preempt: pre-revocation grace notification pushed to a
/// writer that another client has tried to force-acquire. The
/// `MgrInvalidation.version` field carries the grace period in
/// milliseconds — writer should flush + release within that
/// window or the manager will force-revoke after the deadline
/// (which then pushes a `LEASE_INVAL_LEASE_REVOKED`).
pub const LEASE_INVAL_WILL_REVOKE_IN: u8 = 4;

/// Stable client identity. `uuid` is the 16-byte UUID generated by the
/// daemon at process start; `kind` is the daemon family. The triple
/// `(kind, uuid)` is the lease key — `host` is purely diagnostic and is
/// NOT compared by the manager.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct MgrClientId {
    pub kind: u8,
    pub uuid: [u8; 16],
    pub host: String,
}

/// Snapshot of an inode lease returned to the client on Acquire / Heartbeat.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MgrInodeLeaseInfo {
    pub ino: u64,
    /// Monotonic generation. Bumps on every writer-close and on every
    /// manager-revoke. Clients tag every cached `(ino, version)` entry
    /// with this value and discard on mismatch (close-to-open coherence).
    pub version: u64,
    /// `true` while a writer holds the lease (informational; readers
    /// don't need to act on this — they only need to honour invalidations).
    pub writer_present: bool,
    /// TTL the manager assigned. Clients must heartbeat within this
    /// window or the lease is revoked (writers) / dropped (readers).
    pub ttl_secs: u32,
}

// --- AcquireLease (msg_type = MSG_ACQUIRE_LEASE = 0x46) -----------------
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct AcquireLeaseReq {
    pub client: MgrClientId,
    pub ino: u64,
    /// `LEASE_MODE_READ` or `LEASE_MODE_WRITE`.
    pub mode: u8,
    /// F-lease-preempt: when true, the manager starts a revocation
    /// of any current writer (pushes `LEASE_INVAL_WILL_REVOKE_IN`
    /// with a grace period) instead of returning a conflict
    /// immediately. The first force-acquire call on a held writer
    /// returns `CODE_REVOKE_PENDING` (= 6) with `lease.ttl_secs`
    /// repurposed to carry the milliseconds-remaining; the
    /// caller retries until granted or until the wait budget
    /// is exhausted. On voluntary release within the grace
    /// window: the next non-force or force acquire is granted
    /// cleanly. On grace expiry: the next force-acquire
    /// force-revokes (bumps version, pushes
    /// `LEASE_INVAL_LEASE_REVOKED`) and is granted.
    pub force: bool,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct AcquireLeaseResp {
    pub code: u8,
    pub message: String,
    /// Populated iff `code == CODE_OK`.
    pub lease: Option<MgrInodeLeaseInfo>,
}

// --- ReleaseLease (msg_type = MSG_RELEASE_LEASE = 0x47) -----------------
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ReleaseLeaseReq {
    pub client: MgrClientId,
    pub ino: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ReleaseLeaseResp {
    pub code: u8,
    pub message: String,
    /// New `version` after the release (writer-close bumps it; reader-
    /// release leaves it unchanged). `None` if the lease was already
    /// absent (idempotent release).
    pub new_version: Option<u64>,
}

// --- HeartbeatLease (msg_type = MSG_HEARTBEAT_LEASE = 0x48) -------------
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct HeartbeatLeaseReq {
    pub client: MgrClientId,
    pub ino: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct HeartbeatLeaseResp {
    pub code: u8,
    pub message: String,
    /// Populated iff `code == CODE_OK`. `CODE_NOT_FOUND` here means the
    /// lease was revoked / expired — the client must drop its cache and
    /// re-acquire.
    pub lease: Option<MgrInodeLeaseInfo>,
}

/// One invalidation event pushed from manager → client.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MgrInvalidation {
    pub ino: u64,
    pub version: u64,
    /// One of `LEASE_INVAL_*`.
    pub kind: u8,
}

// --- PollInvalidations (msg_type = MSG_POLL_INVALIDATIONS = 0x49) -------
//
// F-ioring-lease-1 ships the simple synchronous "drain my queue"
// semantics — returns the queued events immediately (or an empty list).
// F-ioring-lease-3 will wrap this in a long-poll loop on the client and
// add a server-side wait-for-event hook so the round-trip rate stays low.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PollInvalidationsReq {
    pub client: MgrClientId,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct PollInvalidationsResp {
    pub code: u8,
    pub message: String,
    pub events: Vec<MgrInvalidation>,
}

// --- AllocInodes (msg_type = MSG_ALLOC_INODES = 0x53) --------------------
//
// F-FS-UNIFY M0: grant a contiguous batch of fuse-fs inode numbers
// `[base, base + count)`. The manager owns the counter (etcd key
// `autumn-rs/fs/next_inode`, big-endian u64) and every grant is a
// leader-fenced etcd CAS txn — concurrent allocators can never receive
// overlapping ranges, even across a leader transition.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct AllocInodesReq {
    /// How many inode numbers to allocate (fuse uses INODE_ALLOC_BATCH =
    /// 1000; a short-lived client may ask for fewer). Must be ≥ 1.
    pub count: u32,
    /// Migration floor: the allocator's counter is raised to at least this
    /// before granting. Pre-M0 filesystems kept the cursor in the fs KV
    /// superblock key (`[0x04]next_inode`, client-side non-CAS RMW); the
    /// fuse mount passes that legacy value here on its first batch so an
    /// existing filesystem migrates without duplicate inodes. 0 = none.
    pub floor: u64,
    /// F-KEY-NS D1: the per-volume identity (the canonicalized `fs/{tenant}/
    /// {volume}/` prefix, or empty for the pre-D1 single global counter). SD-1
    /// FREEZES this field into the wire; `handle_alloc_inodes` IGNORES it for
    /// now. SD-3 wires the per-volume etcd counter
    /// (`autumn-rs/fs/{tenant}/{volume}/next_inode`). Empty = legacy global
    /// counter (existing callers keep encoding unchanged). Additive rkyv field.
    pub volume: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct AllocInodesResp {
    pub code: u8,
    pub message: String,
    /// First inode number of the granted range; the caller owns
    /// `[base, base + count)`. Meaningful iff `code == CODE_OK`.
    pub base: u64,
}

// --- F-DASH-IN-MGR: auto-policy controller (msg_types 0x54/0x55) -----------
//
// The in-manager auto-policy controller, folded in from the retired Python
// `python/dashboard/`. Config is leader-owned and persisted to etcd
// (`autoPolicy/config` + `autoPolicy/cooldowns`) so the active policy survives
// leader failover — the crash-safety win over a killable Python webserver.
// The controller loop runs ONLY on the leader and is DEFAULT-OFF (a fresh
// cluster stays pure-mechanism, F203). Built-in presets are compiled-in (never
// persisted); only custom entries + the (mode, active) selection go to etcd.
pub const MSG_AUTOPOLICY_GET: u8 = 0x54;
pub const MSG_AUTOPOLICY_SET: u8 = 0x55;

/// F-REGION-REBALANCE: active region→PS load rebalance (leader-only mutation).
pub const MSG_REBALANCE_REGIONS: u8 = 0x56;

// ── F-KEY-NS D2: namespace registry (admin → manager, low-frequency) ──────────
// admin creates/deletes a `namespace/<name>` etcd registry row (leader-fenced,
// admin-token gated — same posture as MSG_TENANT_CREATE/DELETE). The registry is
// the authoritative source for D7 Layer-A (writes must fall in a registered
// namespace) + the D6 protected-prefix bridge. See docs/key_namespace_split_design.md §7.1.
pub const MSG_NAMESPACE_CREATE: u8 = 0x57;
pub const MSG_NAMESPACE_DELETE: u8 = 0x58;
// admin → manager (leader-gated): list the full registry (rich rows). The 5 s
// `MSG_GET_AUTHZ_CONFIG` poll stays LEAN (prefixes only, for Layer-A); the rich
// data (owner/presplit/created_at) rides this dedicated low-frequency RPC.
pub const MSG_NAMESPACE_LIST: u8 = 0x59;
// admin → manager (leader-gated): list every registered principal with its
// grants. F-NS-PRINCIPAL-LIST — the symmetric counterpart of NAMESPACE_LIST:
// `principal-create`/`principal-delete` shipped without a way to SEE what
// exists, leaving `ls $DATA_ROOT/authz/*.cred` (only what turnkey happened to
// write) or an etcd key scan (names only — the value is rkyv, so grants are
// invisible) as the only options.
pub const MSG_PRINCIPAL_LIST: u8 = 0x5A;
// admin → manager (leader-gated): record the split points a presplit actually
// applied, so merge can refuse to undo them. F-FS-GEOM-DECLARED step 4 — the
// namespace registry's `presplit` field was frozen and empty since SD-1 waiting
// for exactly this consumer. `namespace-create --presplit` can only declare
// points for a namespace being CREATED; presplit runs against existing ones.
pub const MSG_NAMESPACE_SET_PRESPLIT: u8 = 0x5B;

/// `AutoPolicySetReq.op` values.
pub const AUTOPOLICY_OP_SET_MODE: u8 = 0;
pub const AUTOPOLICY_OP_SET_ACTIVE: u8 = 1;
pub const AUTOPOLICY_OP_UPSERT: u8 = 2;
pub const AUTOPOLICY_OP_DELETE: u8 = 3;

/// One named policy (preset or custom). `switches` is [split, ec, compact, gc,
/// merge] — a `Vec<bool>` (not a fixed array) for forward-compat; the controller
/// reads the first 5, absent = false.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrAutoPolicyEntry {
    pub name: String,
    pub desc: String,
    pub switches: Vec<bool>,
    pub interval_sec: u64,
    pub cooldown_sec: u64,
    pub max_actions: u32,
    pub builtin: bool,
}

/// Persisted controller config (etcd `autoPolicy/config`). Only CUSTOM policies
/// are stored; presets are compiled-in constants.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrAutoPolicyConfig {
    pub ver: u32,
    /// 0=Off 1=DryRun 2=Armed (`auto_policy::AutoPolicyMode`).
    pub mode: u8,
    /// Active policy name ("" = none selected).
    pub active: String,
    pub policies: Vec<MgrAutoPolicyEntry>,
}

/// Persisted per-target cooldown stamps (etcd `autoPolicy/cooldowns`).
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrAutoPolicyCooldowns {
    pub entries: Vec<(String, i64)>,
}

/// One rolling action-log entry (leader-local; served by AutoPolicyGet).
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct AutoPolicyLogEntry {
    pub ts: i64,
    /// "issued" | "would" | "refused" | "error".
    pub level: String,
    pub msg: String,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct AutoPolicyGetReq {}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct AutoPolicyGetResp {
    pub code: u8,
    pub message: String,
    pub mode: u8,
    pub active: String,
    /// Whether the process was started with `--dashboard-allow-mutations`
    /// (Armed is honored only when true).
    pub allow_mutations: bool,
    /// Presets + custom (full list, for display).
    pub policies: Vec<MgrAutoPolicyEntry>,
    pub log: Vec<AutoPolicyLogEntry>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct AutoPolicySetReq {
    /// One of `AUTOPOLICY_OP_*`.
    pub op: u8,
    /// For SET_MODE.
    pub mode: u8,
    /// For SET_ACTIVE / DELETE (and the name of an UPSERT entry).
    pub name: String,
    /// For UPSERT.
    pub entry: Option<MgrAutoPolicyEntry>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct AutoPolicySetResp {
    pub code: u8,
    pub message: String,
    /// Resulting state echoed back so callers re-render without a follow-up GET.
    pub mode: u8,
    pub active: String,
    pub policies: Vec<MgrAutoPolicyEntry>,
}

/// Persisted shape of a writer lease in etcd (`inode_leases/<ino>`).
/// Reader leases are NOT persisted — they're ephemeral; a manager
/// failover invalidates all reader caches via the
/// "subscribe-reconnect drops every cached version" rule (plan §6.4).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MgrInodeLeaseRecord {
    pub ino: u64,
    pub writer: MgrClientId,
    pub version: u64,
    /// Unix seconds. The persisted deadline survives leader failover so
    /// the new leader's TTL revoke loop fires on the correct schedule.
    pub expires_at: i64,
}

// ── F-AUTHZ-1 wire + persisted types (manager-as-KDC) ─────────────────────────

/// Persisted tenant account in etcd (`tenantAccount/<tenant>`). Replayed on
/// leader failover. Holds only the credential HASH (never the credential) +
/// the key prefixes this tenant may access.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrTenantAccount {
    pub tenant: String,
    /// SHA-256 of the tenant's permanent credential. Verified (constant-time)
    /// at mint time; the raw credential is never stored.
    pub credential_hash: [u8; 32],
    /// Key prefixes this tenant may access. Each MUST end with `b'/'`.
    pub allowed_prefixes: Vec<Vec<u8>>,
}

/// `MSG_TENANT_CREATE` — admin creates/rotates a tenant account. `admin_token`
/// is checked (constant-time) against the manager's configured admin token
/// (admin_auth_design.md Option A). Returns the freshly-generated permanent
/// credential (shown once; the manager stores only its hash).
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct TenantCreateReq {
    pub admin_token: String,
    pub tenant: String,
    pub allowed_prefixes: Vec<Vec<u8>>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct TenantCreateResp {
    pub code: u8,
    pub message: String,
    /// The generated permanent credential (raw bytes). Empty on error.
    pub credential: Vec<u8>,
}

/// `MSG_TENANT_DELETE` — admin removes a tenant account (stops renewal; the
/// tenant's current token still works until it expires). Resp = `CodeResp`.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct TenantDeleteReq {
    pub admin_token: String,
    pub tenant: String,
}

// ── F-KEY-NS D2: namespace registry types (SD-1) ─────────────────────────────

/// Persisted namespace registry row in etcd (`namespace/<name>`). Replayed on
/// leader failover. Modelled on `MgrTenantAccount` (string-keyed etcd prefix +
/// rkyv + fail-loud replay). Registry granularity = top-level family (`fs/` is
/// ONE row, not one per volume — the app owns the sub-structure; Layer-A only
/// checks top-level membership). See docs/key_namespace_split_design.md §3.7③.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrNamespace {
    /// Namespace name (`[a-z0-9._-]+`, single path segment). The etcd key is
    /// `namespace/<name>`; the stored `prefix` is `name + "/"`.
    pub name: String,
    /// The registered key prefix = `name + "/"`. This is the byte prefix
    /// Layer-A / authz / presplit match against (all namespaces are disjoint).
    pub prefix: Vec<u8>,
    /// Owning tenant, if any. `Some(_)` marks the namespace PROTECTED — its
    /// prefix is bridged into authz `protected_prefixes` (D6). The three
    /// bootstrap-seeded families (`fs`/`kvc`/`mem`) start `None` (existence-only
    /// until an owner is later assigned). `None` ⇒ registered but not protected.
    pub owner_tenant: Option<String>,
    /// D8 presplit points (raw split keys inside `[prefix, prefix-successor)`).
    /// SD-1 FREEZES this field + stores it verbatim; CONSUMPTION (looping
    /// `split --at` at namespace-create) is D8. Empty = no presplit.
    pub presplit: Vec<Vec<u8>>,
    /// Unix seconds at creation (diagnostic).
    pub created_at: i64,
}

/// `MSG_NAMESPACE_CREATE` — admin registers a new namespace. Leader-only,
/// admin-token gated (same posture as `TenantCreateReq`). Rejects reserved names
/// (`fs`/`kvc`/`mem`/`default`) + any name whose `name/` prefix is a
/// `starts_with` relation with an existing namespace prefix (disjointness).
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct NamespaceCreateReq {
    pub admin_token: String,
    pub name: String,
    /// Optional owner tenant — `Some(_)` makes the namespace protected (bridged
    /// into authz `protected_prefixes`).
    pub owner_tenant: Option<String>,
    /// D8 presplit split points (raw keys). Freeze-only in SD-1 (stored, not
    /// acted upon). Empty = none.
    pub presplit: Vec<Vec<u8>>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct NamespaceCreateResp {
    pub code: u8,
    pub message: String,
}

/// `MSG_NAMESPACE_DELETE` — admin removes a namespace registry row. Leader-only,
/// admin-token gated. Refuses the three built-in families. The NON-EMPTY guard
/// (refuse deleting a namespace whose prefix still holds data unless `--force`)
/// is enforced CLIENT-SIDE in `autumn-op` (the manager has no KV data-plane
/// client); this handler only removes the etcd registry row. Resp = `CodeResp`.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct NamespaceDeleteReq {
    pub admin_token: String,
    pub name: String,
}

/// `MSG_NAMESPACE_LIST` — admin lists the full registry (rich rows: name,
/// prefix, owner_tenant, presplit, created_at). Leader-only (the registry is
/// leader-maintained; a follower's shadow is empty/stale). Request payload is
/// empty. Not admin-token gated — listing is read-only inspection.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct NamespaceListResp {
    pub code: u8,
    pub message: String,
    pub namespaces: Vec<MgrNamespace>,
}

/// `MSG_NAMESPACE_SET_PRESPLIT` — record split points that a presplit actually
/// applied, onto an EXISTING namespace registry row. Leader-only, admin-token
/// gated (it changes what merge will refuse). Points are UNIONed with whatever
/// the row already carries, never replaced: widening a namespace's presplit
/// (e.g. fs 6 lanes → 24) is a superset, and dropping a sacred boundary must be
/// a deliberate separate act, not a side effect of re-running presplit.
/// Resp = `CodeResp`.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct NamespaceSetPresplitReq {
    pub admin_token: String,
    pub name: String,
    /// Raw split keys (absolute, as passed to `split --at`).
    pub points: Vec<Vec<u8>>,
}

/// One row of `MSG_PRINCIPAL_LIST`. Deliberately NOT a `MgrTenantAccount`:
/// that type carries `credential_hash`, and an inspection RPC must not hand
/// out the verifier for a credential — offline-guessing a weak credential
/// against a leaked hash is exactly the attack the KDC design avoids by never
/// storing the credential itself. Name + grants are all an operator needs.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct PrincipalRow {
    /// The principal (credential owner) name — the key under `tenantAccount/`.
    pub name: String,
    /// The key prefixes this principal may access (`allowed_prefixes`), each
    /// ending in `b'/'`. Rendered as `{ns}/…` under Option 3.
    pub grants: Vec<Vec<u8>>,
}

/// `MSG_PRINCIPAL_LIST` — admin lists every registered principal + its grants.
/// Leader-only (the account map is leader-maintained; a follower's shadow is
/// empty/stale until it replays on promotion). Request payload is empty.
/// NOT admin-token gated — read-only inspection, same posture as
/// `MSG_NAMESPACE_LIST`; the secret-bearing field is omitted from the row type
/// instead (see `PrincipalRow`).
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct PrincipalListResp {
    pub code: u8,
    pub message: String,
    pub principals: Vec<PrincipalRow>,
}

/// `MSG_MINT_TOKEN` — client authenticates with its permanent credential and
/// receives a short-TTL signed capability token.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MintTokenReq {
    /// F-NS-PRINCIPAL-UNIFIED: the principal NAME (credential owner) to mint for.
    /// The manager looks up the account under this name (`tenantAccount/<name>`).
    pub principal: String,
    pub credential: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MintTokenResp {
    pub code: u8,
    pub message: String,
    /// The signed capability token (`cap_token` wire bytes). Empty on error.
    pub token: Vec<u8>,
    /// Token expiry (unix seconds) — lets the client schedule background renew
    /// without parsing the opaque token.
    pub exp: u64,
}

/// One published verification key. The PS builds its verify-keyring from these.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct AuthzPublicKey {
    pub kid: u32,
    /// 32-byte Ed25519 public key.
    pub ed25519_pub: Vec<u8>,
    /// Disabled keys are published (so the PS learns to reject their `kid`) but
    /// never used to sign new tokens — this is emergency bulk revocation.
    pub disabled: bool,
}

/// `MSG_GET_AUTHZ_CONFIG` — PS polls this (cached). Request payload is empty.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct GetAuthzConfigResp {
    pub code: u8,
    pub message: String,
    /// Authz turned on at the manager (a signing key was configured)? When
    /// false the PS does not enforce (opt-in; fuse/kvcache/dev unaffected).
    pub enabled: bool,
    pub public_keys: Vec<AuthzPublicKey>,
    /// Key prefixes under which default-DENY applies (e.g. `mem/`). A request
    /// key outside every protected prefix is not gated.
    pub protected_prefixes: Vec<Vec<u8>>,
    /// F-KEY-NS D7: ALL registered namespace prefixes (the D2 registry). This is
    /// Layer-A's data source (a put-class write must fall in one of these).
    /// POPULATED by the manager in SD-1; CONSUMED by the PS Layer-A gate in SD-2
    /// (the PS stores it now, unused). Distinct from `protected_prefixes`, which
    /// is the OWNED subset (Layer-B). Additive rkyv field (same-commit deploy).
    pub namespaces: Vec<Vec<u8>>,
    /// The TTL the manager mints tokens with (seconds) — advisory, lets the PS
    /// size its clock-skew leeway / the client its renew cadence.
    pub token_ttl_secs: u64,
    /// Clock-skew leeway (seconds) the PS should apply to `nbf`/`exp`.
    pub clock_skew_secs: u64,
    /// This cluster's `cluster_id` = the token `aud` the manager mints with. The
    /// PS enforces `token.aud == cluster_id` at AUTH_HELLO so a token minted for
    /// another cluster (that happens to share signing keys) can't be replayed
    /// here. Empty = unknown (manager not yet bootstrapped) → PS skips the check.
    pub cluster_id: String,
}
