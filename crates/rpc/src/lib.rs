//! autumn-rpc: wire protocol framing for custom binary RPC.
//!
//! Provides a 10-byte-header wire protocol with request multiplexing IDs.
//!
//! # Wire Format
//!
//! ```text
//! [req_id: u32 LE][msg_type: u8][flags: u8][payload_len: u32 LE][payload]
//! ```

pub mod cap_token;
pub mod client;
pub mod error;
pub mod extent_rpc;
pub mod frame;
pub mod manager_rpc;
pub mod partition_rpc;

/// Re-exported so consumers of the recv-into seam (`autumn_transport::
/// ReadHalf::recv_into(reg: Option<&RegisteredMem>)`) don't need a direct
/// autumn-transport dependency. (Uninhabited stub on non-ucx builds — `reg`
/// is always `None` there.)
pub use autumn_transport::RegisteredMem;
/// Re-exported so `call_into_pooled` consumers (autumn-stream's StreamClient)
/// reference `autumn_rpc::PooledBuf` without a direct autumn-transport dep.
/// Transport-agnostic: registered on `ucx`, plain (copy-out) on TCP/no-ucx.
pub use autumn_transport::{regpool_acquire, PooledBuf};
/// Re-exported for SDK-level source-staging decisions (autumn-client
/// `ValueBuf` docs): staging into a pool slab only pays off on a UCX runtime.
pub use autumn_transport::runtime_transport_is_ucx;
pub use error::{Result, RpcError, StatusCode};
pub use frame::{Frame, FrameDecoder, HEADER_LEN};

/// Handler result type for RPC dispatch.
pub type HandlerResult = std::result::Result<bytes::Bytes, (StatusCode, String)>;

/// Msg type reserved for heartbeat ping/pong.
pub const MSG_TYPE_PING: u8 = 0xFF;

/// the canonical extent → shard-index map. This is the ONE
/// source of truth shared by the ExtentNode (`owns_extent` + sibling forward)
/// and the manager / StreamClient shard routing (`shard_addr_for_extent`), so
/// every layer agrees which shard serves an extent.
///
/// A splitmix64 finalizer (same mixer as `rotated_replica_start`) DECORRELATES
/// the sequential extent ids `autumn-op bootstrap` allocates (7 stream ids per
/// partition, contiguous) from the shard modulus: a raw `extent_id %
/// shard_count` aliased every partition's data extents onto shard 0 (their ids
/// were all ≡ 0 mod the shard count), concentrating all client-direct reads on
/// one EN data port. The hash spreads them across all shards.
///
/// **Changing this remaps ownership of EXISTING extents, so it is a
/// STOP-THE-WORLD reshard** (every EN shard + the manager must run the same
/// mapping). It is byte-free — EN shards share the hashed on-disk data dirs, so
/// only the logical shard→extent ownership re-partitions on restart; no etcd
/// struct changes, so no reset is needed, just a coordinated restart.
///
/// `shard_count <= 1` (legacy single-shard / empty `shard_ports`) → shard 0.
#[inline]
pub fn shard_for_extent(extent_id: u64, shard_count: u32) -> u32 {
    if shard_count <= 1 {
        return 0;
    }
    let mut z = extent_id.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^= z >> 31;
    (z % shard_count as u64) as u32
}

/// WIRE-1: build-time fingerprint of the wire-schema source files
/// (manager_rpc / partition_rpc / frame / extent_rpc). Same-commit
/// deploys share it; ANY wire-struct edit changes it. Exchanged via
/// `GetClusterIdResp.wire_fingerprint` and checked at every long-lived
/// process's startup — a mixed-version join refuses LOUDLY instead of
/// silently decoding garbage (rkyv has no cross-version compat).
pub const WIRE_FINGERPRINT: &str = env!("AUTUMN_WIRE_FINGERPRINT");

/// The wire-version interval this binary
/// speaks. `MAX` is the version of the schema compiled into this binary;
/// `MIN` is the oldest peer version it can still interoperate with.
/// Bump discipline (enforced by `wire_version_registry_tests`):
///
/// - ANY wire-schema source edit changes `WIRE_FINGERPRINT`, which fails
///   the registry test until the developer records the new fingerprint —
///   and decides compatibility explicitly:
///   - pre-R2/R3 (rkyv has no cross-version decode): bump `MAX` AND set
///     `MIN = MAX` — the new version is incompatible with everything
///     before it; deploys stay same-commit.
///   - post-R3 (frozen V1 + explicit V2 msg_types): bump `MAX`, keep
///     `MIN = MAX - 1` — the binary serves both forms during a rolling
///     window (the compat window is exactly N ↔ N-1).
pub const WIRE_VERSION_MIN: u32 = 34;
pub const WIRE_VERSION_MAX: u32 = 34;

/// Registry pinning each declared wire version to the schema fingerprint
/// it was declared against. The companion test fails the build's test run
/// whenever the schema changes without a conscious version decision —
/// this is what makes interval overlap trustworthy as a runtime check
/// (a forgotten bump can't silently claim compatibility).
pub const WIRE_VERSION_FINGERPRINTS: &[(u32, &str)] = &[
    // v1: R1 baseline — GetClusterIdResp grew {wire_version_min,
    // wire_version_max, cluster_version}; MSG_GET/BUMP_CLUSTER_VERSION.
    (1, "321a8b3684f0bbb9"),
    // v2: WAL self-heal A5 — MSG_REPORT_CORRUPT_REPLICA (0x4C) +
    // ReportCorruptReplicaReq{partition_id, owner_epoch, log_stream_id,
    // extent_id, eversion, corrupt_node_ids}/Resp. Pre-R3: MIN=MAX=2
    // (same-commit deploy; rkyv has no cross-version decode).
    (2, "5d94c026c08e69ca"),
    // v3: wire-schema unify — extent_rpc relocated from autumn-stream into autumn-rpc
    // (single wire-schema home alongside manager_rpc/partition_rpc); the
    // pure-wire `ExtDiskStatus` mirror deleted, `ExtDfResp.disk_status` now
    // nests canonical `extent_rpc::DiskStatus`. rkyv layout unchanged, but
    // the hashed schema file set changed → new fingerprint. Pre-R3: MIN=MAX=3.
    (3, "f2a5079b44de98d1"),
    // v4: cluster-df — DiskStatus grew `extent_bytes` (EN self-reports its
    // real per-disk extent footprint); MSG_CLUSTER_DF (0x4D) +
    // ClusterDfReq/ClusterDfResp/NodeCapWire. Pre-R3: MIN=MAX=4.
    (4, "d96a8af74454f7ef"),
    // v5: u64-offset widening — every extent byte position on the
    // read+append path widened u32→u64 so extents can exceed 4 GiB
    // (AppendReq.commit, AppendResp.offset/end, ReadBytesReq.offset/length,
    // ReadBytesResp.end, CommitLengthResp.length, ExtCommitLengthResp.length;
    // rkyv: SstLocation.offset/len, TableLocations.vp_offset, SstLocation,
    // CheckCommitLengthResp.end, StreamAllocExtentReq.seal_commit,
    // MultiModifySplitReq.{log,row,meta}_stream_sealed_length,
    // GetRedirectResp.value_offset/value_len). Companion: max_extent_size
    // default 3 GiB → 16 GiB (`autumn-ps --max-extent-size-bytes`).
    // Pre-R3: MIN=MAX=5.
    (5, "5254fafce73f6ffe"),
    // v6: chunked EC convert — WriteShardReq grew `shard_offset: u64`
    // (header 36→44 B) so an EC shard is streamed as offset-tagged stripes,
    // keeping each WriteShard under the frame `payload_len: u32` ceiling for
    // >4 GiB shards (16+ GiB extents) and bounding the encode transient to
    // (K+M)×stripe instead of ~2× the whole extent. Pre-R3: MIN=MAX=6.
    (6, "e9fb9f2e6a582867"),
    // v7: vp_table_refs removal Stage 1 — retired MSG_SYNC_PARTITION_VP_REFS
    // (0x33) + MSG_PULL_VP_REFS (0x4F) and their req/resp (SyncPartitionVpRefs*,
    // PullVpRefs*, MgrPartitionVpRefs). MgrExtentInfo.vp_table_refs kept inert
    // (no rkyv layout change) so persisted `extents/<id>` still decode; the
    // deletion gate keeps `refs==0 && vp_table_refs==0` as an upgrade-safety
    // guard until Stage 2 migrates + collapses it to `refs==0`. Pre-R3: MIN=MAX=7.
    // (Fingerprint updated within the same logical v7 by the coco-P0 follow-up —
    // comment/doc edits only, no wire-layout change; v7 not yet deployed.)
    (7, "96f3131d4e1e0038"),
    // v8: BUG2-IDEMPOTENT-ROLL — StreamAllocExtentReq grew `seal_extent_id: u64`
    // (the tail the writer captured `seal_commit` for; 0 = no pinned target).
    // Makes seal-and-roll idempotent on retry: the manager seals ONLY when the
    // current tail still equals `seal_extent_id`, else returns the current tail
    // untouched — so a retried roll never over-seals the freshly-rolled tail
    // (chaos seed=603 split-child WAL-FAILSTOP wedge). Pre-R3: MIN=MAX=8.
    // (Fingerprint updated within the same logical v8 by the read-response
    // single-alloc perf change — added `Frame::encode_response_with` (an
    // additive encoder producing byte-identical frames, no wire-layout change);
    // v8 not yet deployed.)
    // (Fingerprint updated again within v8 by the io_uring-daemon removal —
    // a comment edit in `manager_rpc.rs` only, no wire-layout change; v8 not
    // yet deployed, so updated in place rather than bumped.)
    (8, "ba19149e1ed99c41"),
    // v9: data-plane authz — new `cap_token.rs` (Ed25519 capability
    // token layout, added to the fingerprint hash set) + manager_rpc gained
    // MSG_MINT_TOKEN (0x4F) / MSG_GET_AUTHZ_CONFIG (0x50) / MSG_TENANT_CREATE
    // (0x51) / MSG_TENANT_DELETE (0x52) and their req/resp + MgrTenantAccount /
    // AuthzPublicKey. Pre-R3: MIN=MAX=9 (same-commit deploy; rkyv has no
    // cross-version decode).
    (9, "839d196b5be56249"),
    // v10: data-plane authz Stage 2 (PS enforcement) — partition_rpc gained
    // MSG_AUTH_HELLO (0x55) + AuthHelloReq/AuthHelloResp (per-connection token
    // bind). StatusCode::PermissionDenied (7) was also added but error.rs is
    // not in the fingerprint set, so only the partition_rpc addition bumps it.
    // Pre-R3: MIN=MAX=10 (same-commit deploy).
    // (Fingerprint updated in place within v10 — coco Stage-2 fix added
    // GetAuthzConfigResp.cluster_id so the PS can enforce token aud == cluster_id;
    // v10 not yet deployed, so no bump.)
    (10, "64554258323fbe51"),
    // v11: fs-unify M0 — manager_rpc gained MSG_ALLOC_INODES (0x53) +
    // AllocInodesReq{count, floor}/AllocInodesResp{code, message, base}:
    // fuse-fs inode-number allocation moved into the manager (leader-fenced
    // etcd CAS on `autumn-rs/fs/next_inode`), replacing the client-side
    // non-CAS RMW on the `[0x04]next_inode` fs KV key that duplicated inode
    // batches under concurrent allocators. Pre-R3: MIN=MAX=11 (same-commit
    // deploy; rkyv has no cross-version decode).
    (11, "8a586475b2930f0f"),
    // v12: dashboard-in-manager M2 — manager_rpc gained MSG_AUTOPOLICY_GET (0x54) /
    // MSG_AUTOPOLICY_SET (0x55) + MgrAutoPolicyEntry / MgrAutoPolicyConfig /
    // MgrAutoPolicyCooldowns / AutoPolicyLogEntry / AutoPolicyGetReq/Resp /
    // AutoPolicySetReq/Resp: the in-manager auto-policy controller's headless
    // control + etcd-persisted config (folded in from the retired Python
    // dashboard). Pre-R3: MIN=MAX=12 (same-commit deploy; rkyv has no
    // cross-version decode).
    (12, "c2ae65efd6e168f6"),
    // v13: fence-drain — partition_rpc gained MSG_ROLL_TAILS (0x57) +
    // RollTailsReq{part_id, entries}/RollTailsResp{code, rolled, message}: the
    // manager's recovery sweep tells a PS to seal+roll open tail extents that
    // sit on a fenced node (recovery only rebuilds SEALED extents, so an idle
    // partition's open tail on a fenced node would never drain / block remove).
    // Pre-R3: MIN=MAX=13 (same-commit deploy; rkyv has no cross-version decode).
    (13, "87ad9d165bd07e15"),
    // v14: gc-floor observability — partition_rpc gained MSG_DIAG_PARTITION_VP (0x58) +
    // DiagPartitionVpReq{part_id}/DiagPartitionVpResp{code, message,
    // log_extent_ids, sst_vp_heads, floor_pos, floor_extent_id,
    // vp_seed_extent_id, vp_seed_offset}: `autumn-op info --part` surfaces the
    // GC replay floor + per-SST vp_heads so operators can tell a correct
    // forcegc-protected extent from a bug. Pre-R3: MIN=MAX=14 (same-commit
    // deploy; rkyv has no cross-version decode).
    (14, "e9c149b55565f2ad"),
    // v15: overview open-tail — PartitionLoad grew `open_tail_bytes: u64`
    // (Σ committed bytes on the partition's log/row/meta OPEN-tail extents,
    // PS-reported). The manager's cluster overview adds it to its
    // authoritative sealed-length sum so an all-open-tail (compacted /
    // log-heavy) partition no longer renders 0 B. Pre-R3: MIN=MAX=15
    // (same-commit deploy; rkyv has no cross-version decode).
    (15, "4bbbbf8faeb05923"),
    // v16: df open-tail — ClusterDfResp grew `logical_open_tail: u64` (Σ
    // PS-reported open-tail committed bytes across partitions). The df
    // amplification denominator becomes `logical_stored + logical_open_tail`
    // so an all-open-tail (VP/log-heavy) cluster no longer shows a ~15×
    // inflated amp (physical counts open bytes, sealed-only logical dropped
    // them). Pre-R3: MIN=MAX=16 (same-commit deploy; rkyv has no
    // cross-version decode).
    (16, "4c2dd28d4b3ff567"),
    // v17: redirect-batch — MSG_GET_REDIRECT_MANY (0x59) + GetRedirectItem /
    // GetRedirectManyReq / GetRedirectManyResp (batched redirect resolution).
    // Same-commit deploy (MIN=MAX=17). Fingerprint filled after the build test
    // reports it.
    (17, "55411e9479326ff8"),
    // v18: df wal-debt — PartitionLoad grew `open_tail_dead_bytes: u64` (dead
    // large-value bytes on the OPEN log tail, the discard-map entry gc_debt's
    // sealed-only filter drops) and ClusterDfResp grew `logical_wal_debt: u64`
    // (Σ gc_debt + open_tail_dead across partitions). `autumn-op df` shows a
    // dead-vs-live breakdown that includes the open-tail debt gc_debt hid.
    // Same-commit deploy (MIN=MAX=18).
    (18, "dd40c423722d2a44"),
    // v19: EN dyn-shard M0 — RegisterNodeReq + MgrNodeInfo grew `node_uuid:
    // String` (stable node identity decoupled from the address, so an EN can
    // self-register a changed IP / shard-port layout and stay the same node).
    // MgrNodeInfo is persisted, so this is a same-commit STOP-THE-WORLD deploy
    // that requires an etcd reset (no migration). Fingerprint filled after the
    // build test reports it.
    (19, "44ac1df3e38f6e77"),
    // v20 — EN dyn-shard M1b: `DfResp` / `ExtDfResp` gained the EN identity
    // echo fields (`node_uuid`, `advertise_addr`, `shard_ports`) so the manager
    // `node_health_loop` can self-heal stored-location drift + detect pod-IP
    // reuse. Df is NOT persisted, but wire is same-commit either way.
    (20, "23a6b7794ff8fb6a"),
    // v21 — EN dyn-shard M1c: `NodeStateEntry` gained `node_uuid` +
    // `shard_ports` so `autumn-op list-nodes` shows identity + shard count
    // (reshard verification). Additive to a non-persisted response struct.
    (21, "a76409bf80fbdb21"),
    // v22 — region-rebalance: `MSG_REBALANCE_REGIONS` + `RebalanceRegionsReq`
    // / `RebalanceMove` / `RebalanceRegionsResp` (active region→PS load
    // rebalance). New msg_type + structs; no persisted-struct change.
    (22, "57fe8f244d8fd5c1"),
    // v23 — region-rebalance Phase B: `POLICY_KIND_REBALANCE` (7) + its
    // `policy_kind_names` entry (cluster-level region→PS rebalance advisory kind
    // for the dashboard auto-policy). Additive const + name-map row.
    (23, "6b0224cb5e4f13f3"),
    // v24 — split-at-key (design doc D4): `SplitPartReq` grew
    // `at_key: Option<Vec<u8>>` (operator/controller-specified split point;
    // `None` = legacy median selection). Additive rkyv field on an admin
    // (region_epoch-exempt) request struct. Pre-R3: MIN=MAX=24 (same-commit
    // deploy; rkyv has no cross-version decode).
    (24, "eeaf88720200e4da"),
    // v25 — key-namespace D1+D7+D2 stop-world batch wire freeze (SD-1). One bump
    // covers every struct the batch touches (SD-2/SD-3 add NO further wire):
    //   • MSG_NAMESPACE_CREATE (0x57) / MSG_NAMESPACE_DELETE (0x58) + the
    //     NamespaceCreate{Req,Resp} / NamespaceDeleteReq / MgrNamespace structs
    //     (D2 registry).
    //   • GetAuthzConfigResp grew `namespaces: Vec<Vec<u8>>` (D7 Layer-A data
    //     source; PS stores it, consumes it in SD-2).
    //   • AllocInodesReq grew `volume: Vec<u8>` (D1 per-volume inode counter;
    //     frozen only, handle_alloc_inodes ignores it — SD-3 wires it).
    //   • CODE_NAMESPACE_UNKNOWN (10) error code (D7 Layer-A reject; PS returns
    //     it in SD-2).
    //   • SD-2 addendum (sanctioned in-place v25 refinement): MSG_NAMESPACE_LIST
    //     (0x59) + NamespaceListResp{code,message,namespaces:Vec<MgrNamespace>}
    //     (rich registry list; the 5s GET_AUTHZ_CONFIG poll stays lean). This
    //     changed the v25 fp below — NOT a version bump (still MIN=MAX=25).
    // Pre-R3: MIN=MAX=25 (same-commit deploy; rkyv has no cross-version decode).
    (25, "6bb3e2105b2845db"),
    // v26 — namespace-principal-unified (Option 3, docs/key_namespace_split_design.md
    // §8): keys drop the tenant segment (`{tenant}/{ns}/` → `{ns}/…`) and the
    // authz identity becomes `principal`. `MintTokenReq.tenant` → `principal`
    // (the only wire-STRUCT change; the key-layout flip is client/PS-side and
    // carries no struct change, but §8.6 MANDATES this version bump so a stale
    // tenant-first image is fenced at the handshake instead of silently reading
    // empty). Pre-R3: MIN=MAX=26 (same-commit deploy; rkyv has no cross-version).
    (26, "c408d5e3de95883a"),
    // v27 — two same-batch additions (v27 was never deployed, so the second one
    // REFRESHES this fingerprint in place rather than taking a v28; same
    // sanctioned in-place refinement the v25 SD-2 addendum used):
    //   • namespace-principal-list: MSG_PRINCIPAL_LIST (0x5A) + PrincipalRow{name,
    //     grants} / PrincipalListResp. Deliberately NOT a mirror of
    //     MgrTenantAccount — the row omits `credential_hash` so an inspection
    //     RPC cannot hand out the verifier for a credential.
    //   • fs-geom-declared step 4: MSG_NAMESPACE_SET_PRESPLIT (0x5B) +
    //     NamespaceSetPresplitReq{admin_token,name,points} (record the split
    //     points a presplit applied onto an EXISTING namespace row), and
    //     MergePartitionsReq grew `force: bool` (override the sacred-boundary
    //     refusal). The registry's `presplit` field itself needed no change —
    //     it has been frozen and empty since SD-1 waiting for this consumer.
    //   • admin-op-auth manager slice: is_admin_mgr_msg + the admin-token
    //     payload codec (prefix_admin_token / strip_admin_token) in
    //     manager_rpc.rs (pure fns + consts, no rkyv struct changed shape).
    //   • admin-op-auth PS slice: GetAuthzConfigResp grew `admin_token:
    //     Vec<u8>` (additive) + is_admin_ps_msg in partition_rpc.rs — so the PS
    //     can gate split/maintenance on the manager's admin secret.
    //   • key-namespace UX-fix (M3): MSG_MULTI_MODIFY_MERGE added to is_admin_mgr_msg
    //     (pure fn edit) so the raw merge txn can't be dispatched to bypass
    //     the freeze + sacred-boundary guard.
    // The payload prefix is an out-of-band convention the manager/PS strip
    // before decoding; the ONLY rkyv struct change is the additive
    // GetAuthzConfigResp field. All within undeployed v27 → fingerprint
    // refreshes in place. Pre-R3 rkyv has no cross-version decode, so MIN=MAX=27
    // and the deploy stays same-commit.
    (27, "706da8704a419602"),
    // v28: wire CRC-unify — ONE frame shape, CRC structural (frame.rs):
    //   [header][ctrl_len:4][ctrl…][crc32c:4][value…], crc over header+ctrl,
    //   value raw (transport + storage integrity). FLAG_CRC bit retired
    //   (protection is no longer optional → no flag to flip off); the 9-byte
    //   bulk_ctrl + encode_no_crc CRC-less shape deleted; bulk read responses
    //   carry `[code][message]` in the CRC'd ctrl (errors finally have a
    //   human-readable message); MSG_PUT_BULK's value is excluded from the crc
    //   (the sender no longer crc-scans the value).
    //   MSG_APPEND is the ONE deliberate exception: its bulk payload stays
    //   inside ctrl (durability path keeps in-transit CRC).
    //   NOTE mixed-deploy failure mode: the FRAME layer changed, so an
    //   old-binary peer fails at the first frame with CrcMismatch/Malformed
    //   (loud connection error) instead of reaching the GetClusterId version
    //   handshake — acceptable under same-commit deploys, better than silent
    //   garbage. Pre-R3: MIN=MAX=28.
    //   v28 fingerprint refreshed IN PLACE twice pre-deploy (sanctioned
    //   refinement precedent, v25 SD-2 / v27): ① the value-buffer batch,
    //   ② the bulk-vocabulary rename (zc→bulk everywhere: the old name
    //   claimed an effect — zero-copy — that is a property of memory
    //   provenance × transport, not of the wire structure; the structure is
    //   value-separable "bulk" framing. Wire BYTES unchanged: msg_type
    //   values 0x50/0x51/15 keep their numbers, only const/API names moved).
    //   v28 fingerprint refreshed IN PLACE again (③, same sanctioned
    //   pre-deploy-refinement precedent): the G1 "SIGSTOP zombie writer"
    //   takeover-fence fix ADDS `MSG_FENCE_EXTENT` (0x11) + `FenceExtentReq`/
    //   `FenceExtentResp` to `extent_rpc.rs` — a NEW append-only message type
    //   (existing decode paths unaffected) that perturbs the hashed schema.
    //   Pre-R3 same-commit stop-world deploy ⇒ MIN=MAX=28 unchanged.
    (28, "bba50c4a490d2c0d"),
    // v29: async op ledger — every long-running op (split/merge/rebalance/
    // compact/gc/forcegc/ec-convert) is submitted through the leader and made
    // queryable, recovering the failure reason the fire-and-forget maintenance
    // ops used to drop. Wire additions: MSG_OP_SUBMIT (0x5C) / MSG_OP_QUERY
    // (0x5D) + OpSubmitReq/Resp, OpQueryReq/Resp, OpRecord, MaintenanceOutcome,
    // OP_KIND_*/OP_STATE_* (manager_rpc.rs); PartitionLoad grew
    // `maintenance_outcomes` (outcomes piggyback the load heartbeat);
    // MaintenanceReq grew `op_id` (partition_rpc.rs); MSG_OP_SUBMIT joined
    // is_admin_mgr_msg; AUDIT_OP_* 7..=12 for durable terminal history. Pre-R3
    // same-commit stop-world deploy ⇒ MIN=MAX=29.
    //   v29 fingerprint refreshed IN PLACE (sanctioned pre-deploy refinement,
    //   same precedent as v25 SD-2 / v27 / v28 ①②③ — v29 has never been
    //   deployed): the ledger grew `OP_KIND_RECOVERY` (8) plus
    //   `OpRecord.error_code` + `OpRecord.attempts`, so auto-dispatched extent
    //   recoveries are listable with their retry count and last failure
    //   reason/code (previously visible only in aggregate via `recovery-stats`).
    //   v29 refreshed IN PLACE again (still undeployed): EC conversion moved to
    //   the recovery model — the coordinator EN ACKs `MSG_CONVERT_TO_EC`
    //   immediately and encodes in the BACKGROUND, reporting completion on its
    //   next `df` via `DfResp.ec_done: Vec<EcConvertDone>` (mirrored in
    //   `ExtDfResp` AT THE SAME FIELD POSITION — that struct decodes the EN's
    //   `DfResp` bytes, so field order is the compatibility contract). This
    //   removes the "RPC timeout vs dead EN" ambiguity that made a stuck EC
    //   marker un-releasable, and unblocks conversions longer than one RPC
    //   timeout.
    //   v29 refreshed IN PLACE again (still undeployed): EC conversion gained a
    //   per-attempt nonce — `ConvertToEcReq`/`ExtConvertToEcReq`, `WriteShardReq`
    //   (binary header 44 → 52) and `EcConvertDone` all carry `attempt_nonce`.
    //   `new_eversion` is `live + 1` and an abandoned attempt never bumps the
    //   extent, so a reissued attempt reuses it; the nonce is what separates a
    //   completion report (or a staged stripe) belonging to one attempt from
    //   another's.
    //   v29 refreshed IN PLACE again (still undeployed): a read now NAMES the
    //   payload file it wants. `ReadBytesReq` grew `(payload_location,
    //   shard_index)` (32 → 40 bytes; a 32-byte request still decodes, as
    //   `InDat`), `extent_rpc::ExtentInfo` and `ExtentInfoResp` grew
    //   `payload_location`, and `CODE_PAYLOAD_NOT_HERE` (7) is the refusal when
    //   a node does not hold the named file. The manager keeps the location in
    //   a sibling etcd key, so the persisted `MgrExtentInfo` is untouched and
    //   every existing extent reads as `InDat`.
    //   v29 refreshed IN PLACE again (still undeployed): the reconcile answer
    //   became FILE-granular — `ReconcileExtentsResp` grew `placements:
    //   Vec<ExtentPlacement{extent_id, payload_location, shard_index}>` beside
    //   `garbage`. An extent-granular answer can only say "you should not have
    //   this extent"; it cannot say "keep the extent, drop the `.dat`", which
    //   is the post-conversion cleanup. An extent in NEITHER list has no
    //   verdict this round and is left alone.
    //   Same in-place v29 refresh also gave `ReconcileExtentsReq` a
    //   `node_uuid`: every verdict in the answer is relative to ONE node, and
    //   the EN knows only its uuid (the manager assigns node ids), so a
    //   reporter it cannot resolve gets no verdict at all.
    //   v29 refreshed IN PLACE again (still undeployed): the per-node EC commit
    //   phase is GONE. `MSG_COMMIT_EC_SHARD` (12) is retired to a reserved
    //   tombstone and `CommitEcShardReq/Resp` are deleted — conversion stages an
    //   additive `extent-{id}.shard{i}` and the manager's layout flip is the
    //   sole commit point, so no node publishes a shard over its own `.dat`.
    //   v29 refreshed IN PLACE again (still undeployed): `ReconcileExtentsReq`
    //   grew `shard_idx`. Every shard of an EN reconciles its own disjoint
    //   extents under one shared node_id, so without it the manager's grace
    //   counters — pruned to what the reporter still holds — were wiped by
    //   every sibling's report and no verdict could reach three rounds.
    //   v29 refreshed IN PLACE again (still undeployed): `code_description` now
    //   names CODE_LOCKED_BY_OTHER and CODE_PAYLOAD_NOT_HERE, and callers print
    //   the numeric code beside it. No struct changed — but rendering every
    //   unnamed code as the bare word "error" made a permanently fenced EC
    //   conversion read like a generic transient being retried.
    //   v29 refreshed IN PLACE again (still undeployed): op progress. OpRecord
    //   grew `progress_done`/`progress_total` and PartitionLoad grew
    //   `active_maintenance: Vec<MaintenanceProgress>` — raw counts, not a
    //   percentage, so the consumer keeps both the ratio and the magnitude.
    //   v29 refreshed IN PLACE again (still undeployed): MSG_OP_HISTORY (0x5E)
    //   + OpHistoryReq/Resp — durable terminal op history read from etcd, kept
    //   a separate message from the live-ledger MSG_OP_QUERY.
    //   v29 refreshed IN PLACE again (still undeployed): MultiModifySplitReq
    //   grew `log/row/meta_tail_extent_id` — the tail extents the three sealed
    //   lengths were CAPTURED FOR. The manager refuses the split commit when a
    //   tail moved between the PS's capture and the commit (a fence-drain roll
    //   in flight when the split froze), because compute_duplicate_stream
    //   seals the CURRENT tail: the captured length would be stamped onto the
    //   roll's fresh empty extent, sealed longer than any replica holds, and
    //   the CoW child's replay could never read it. `0` = no claim (skip).
    //   v29 refreshed IN PLACE again (still undeployed): RangeReq.start now
    //   documents that it is INCLUSIVE and that neither `K+0x00` nor
    //   `K+0x01` expresses "resume after K" — comment-only, but the
    //   fingerprint hashes whole schema files.
    //   v29 refreshed IN PLACE again (still undeployed): RangeReq.start's
    //   doc updated for the PS's internal-key comparator (user key first,
    //   fixed-width inverted-seq suffix second; no separator byte) —
    //   `K ++ 0x00` is now the exact "resume strictly after K" start.
    //   Comment-only; no struct changed. NOTE the same commit changes the
    //   PS's PERSISTED SST/WAL key ordering — old partition data is
    //   unreadable; a dev cluster must be rebuilt.
    //   v29 refreshed IN PLACE again (still undeployed): `DeleteExtentReq`
    //   (and its `manager_rpc::ExtDeleteExtentReq` mirror) grew `node_uuid`,
    //   the identity the target checks before unlinking. The one RPC that
    //   destroys data previously named only an extent id and ran for whoever
    //   answered at that address — fine within a cluster (ids are never
    //   reused) but not across one, where a torn-down cluster's persisted
    //   delete retries can unlink a NEW cluster's live extent that reuses the
    //   id at the same address. Empty uuid = unspecified, check skipped.
    //   v29 refreshed IN PLACE again (still undeployed): the extent-service
    //   messages `manager_rpc` used to MIRROR are now re-exports of the
    //   `extent_rpc` definitions — `ExtAllocExtentReq/Resp`, `ExtCodeResp`,
    //   `ExtConvertToEcReq`, `ExtDeleteExtentReq`, `ExtDfReq/Resp`,
    //   `ExtReAvaliReq`, `ExtRequireRecoveryReq`, `MgrRecoveryTask(Done)` are
    //   gone as separate types and their call sites name the canonical ones.
    //   (Older notes above still use the retired names; they describe the
    //   schema as it stood then.) rkyv layout is unchanged — the mirrors were
    //   field-identical, which is exactly why divergence would have been
    //   silent — but the hashed schema files changed. `extent_rpc`'s
    //   `one_definition_only` now makes a reintroduced mirror a build error,
    //   which is a stronger guarantee than the fingerprint could ever give
    //   here: both files feed the SAME hash, so it can never distinguish them.
    //   v29 refreshed IN PLACE again (still undeployed): `DfResp` gained
    //   `op_progress: Vec<ExtentOpProgress>` — live samples for the
    //   extent-scoped ops the NODE executes (EC conversion, recovery), which
    //   previously reported only their terminal outcome, so a multi-hour
    //   conversion showed a bare RUNNING. Keyed by extent_id because the node
    //   never learns the manager's op id. `manager_rpc` also gained
    //   `op_kind_name` (display mapping, moved out of the CLI so it has one
    //   definition).
    (29, "f8b58703a0162c62"),
    // v30: `BatchPutReq` lost `must_sync`. `PutReq` and `AppendReq` dropped
    // theirs when every write became durable through the extent-node fsync
    // coalescer; the batch form outlived that pass. It was write-only — the
    // three construction sites all set `true` and no PS code path ever read
    // it — so a peer that still sends the field is not asking for anything
    // different, but rkyv has no cross-version decode and the extra bool
    // shifts the layout, so this is a same-commit deploy like every other
    // schema change here. Pre-R3: MIN=MAX=30.
    (30, "d3b49bd995789e83"),
    // v31: `MSG_BATCH_PUT_BULK` (0x5A) + `BatchPutBulkReq`/`BatchPutBulkOp` —
    // a batched PUT whose values ride the frame's raw tail as their own iovecs
    // instead of being copied into the rkyv payload. The bulk decision moved
    // from the ITEM's size to the GROUP's total, because what makes a transfer
    // worth its own iovec is how much the FRAME carries and a batch is what
    // makes a frame big. Measured against the inline form on the same rig:
    // +16% write throughput at 32 KiB values over loopback TCP, and ~+61% at
    // 4 KiB values over RoCE (rc_mlx5) — the transport that matters, where the
    // inline path also swung 47-68 MB/s run to run while the bulk path held
    // 93.3. At 4 KiB over loopback TCP the two are identical, because that
    // operating point sits on the single-partition ~30k ops/s ceiling where no
    // byte-side saving can show. Pre-R3: MIN=MAX=31.
    (31, "564167951e81d407"),
    // v32: `MSG_BATCH_GET_BULK` (0x5B) + `BatchGetBulkCtrl` — the read mirror
    // of v31. Request shape is unchanged (`BatchGetReq`); the REPLY moves its
    // values out of the rkyv response into the frame's raw tail, keeping only
    // per-key status and length in the CRC'd ctrl. The inline form copied every
    // value byte four times on the server — out of the memtable result, twice
    // through `rkyv_encode`, once more assembling the frame — and then CRC'd
    // all of it, because a normal response protects its whole payload. It also
    // ran through `partition_loop`, so a batched read queued behind the
    // single-writer group-commit actor; the bulk form is served on the
    // connection task next to `MSG_GET_BULK`. `frame.rs` gained
    // `encode_bulk_response_head_bytes` (same layout, binary ctrl tail) and
    // `BulkResp` now carries that tail verbatim, since lossy UTF-8 would
    // corrupt binary statuses. Pre-R3: MIN=MAX=32.
    (32, "58a279d2f64a5206"),
    // v33: the inline `MSG_BATCH_GET` (0x54) and its `BatchGetResp`/`BatchGetItem`
    // are deleted. `MSG_BATCH_GET_BULK` replaced it in v32 and nothing sent the
    // old form afterwards; on a same-commit deploy a second, slower way to do
    // the same read is not a rollback path, it is the shape `must_sync` had
    // right before it became a fail-open switch nobody remembered. `BatchGetReq`
    // stays — the bulk form uses it unchanged. Pre-R3: MIN=MAX=33.
    (33, "699f4edcec554d41"),
    // v34: `GetRedirectResp` gained `ec_data_shards` + `ec_sealed_length`, so a
    // redirect can describe an EC extent instead of refusing to. The fields are
    // additive but the version is NOT optional: `replica_addrs` changes meaning
    // when `ec_data_shards > 0` — entries become positional data-shard homes
    // holding one slice each, not interchangeable full copies — and a client
    // that ignored the discriminator would read shard bytes as a value. Same
    // reason the descriptor used to be refused for EC entirely.
    (34, "f98d5faa6834f357"),
];

/// R1: peer wire-compat check, replacing WIRE-1's single-point
/// fingerprint equality. Accept iff:
/// - the peer's fingerprint equals ours (identical schema build — always
///   safe, the common same-commit case), OR
/// - the version intervals overlap (an explicitly-declared compatible
///   release pair; trustworthy because the registry test forces every
///   schema edit through a conscious MIN/MAX decision).
///
/// Callers treat a TRANSPORT failure fetching the peer's values as
/// best-effort-skip (the peer may be briefly down; availability wins),
/// but a SUCCESSFUL response that fails this check is a hard startup
/// refusal. A peer reporting `max == 0` (empty/pre-R1) is refused.
pub fn wire_compat_check(
    remote_fp: &str,
    remote_min: u32,
    remote_max: u32,
) -> std::result::Result<(), String> {
    if remote_fp == WIRE_FINGERPRINT {
        return Ok(());
    }
    // Registry cross-check (coco P1): if the peer claims a max version
    // WE have a registered fingerprint for, the fingerprints must match —
    // a peer claiming OUR version with a DIFFERENT schema is precisely
    // the forgot-to-bump corruption case the registry exists to prevent,
    // now caught at runtime too (not only by the registry test). A peer
    // claiming a version we don't know (newer than us) can't be checked
    // here; its own registry validated us symmetrically.
    if let Some((_, expected_fp)) = WIRE_VERSION_FINGERPRINTS
        .iter()
        .find(|(v, _)| *v == remote_max)
    {
        if remote_fp != *expected_fp {
            return Err(format!(
                "wire-version fraud: peer claims wire version {remote_max} but its schema \
fingerprint {remote_fp} differs from the registered fingerprint {expected_fp} for that \
version — a wire-schema edit shipped without a version bump. Rebuild the peer from a \
clean tree (and fix its WIRE_VERSION_FINGERPRINTS registry)."
            ));
        }
    }
    if remote_max >= 1 && remote_min <= remote_max {
        let lo = WIRE_VERSION_MIN.max(remote_min);
        let hi = WIRE_VERSION_MAX.min(remote_max);
        if lo <= hi {
            return Ok(());
        }
    }
    Err(format!(
        "wire-version mismatch: local=[{WIRE_VERSION_MIN},{WIRE_VERSION_MAX}] \
fp={WIRE_FINGERPRINT}, peer=[{remote_min},{remote_max}] fp={remote_fp} — \
no common wire version (rkyv wire structs have no implicit cross-version \
compatibility; a mixed deploy decodes garbage silently). Upgrade one step at a \
time (compat window is N ↔ N-1), or rebuild \
this binary/wheel from the cluster's commit."
    ))
}

#[cfg(test)]
mod shard_for_extent_tests {
    use super::shard_for_extent;

    #[test]
    fn legacy_single_shard_is_zero() {
        for id in [0u64, 1, 7, 12345, u64::MAX] {
            assert_eq!(shard_for_extent(id, 0), 0);
            assert_eq!(shard_for_extent(id, 1), 0);
        }
    }

    #[test]
    fn result_is_always_in_range() {
        for count in [2u32, 3, 4, 8, 16] {
            for id in 0..1000u64 {
                assert!(shard_for_extent(id, count) < count);
            }
        }
    }

    #[test]
    fn deterministic() {
        assert_eq!(shard_for_extent(999, 4), shard_for_extent(999, 4));
    }

    #[test]
    fn bootstrap_contiguous_ids_spread_across_all_shards() {
        // The regression this hash fixes: `autumn-op bootstrap` allocates
        // a contiguous run of stream/extent ids (7 per partition), which under a
        // raw `id % 4` all aliased onto shard 0. A well-mixed hash must hit every
        // shard across such a run.
        let count = 4u32;
        let mut hit = [0usize; 4];
        // Simulate 32 partitions × 7 contiguous ids each (ids 100..324).
        for id in 100u64..324 {
            hit[shard_for_extent(id, count) as usize] += 1;
        }
        for (shard, &n) in hit.iter().enumerate() {
            assert!(n > 0, "shard {shard} got no extents — aliasing regressed");
        }
        // And the raw modulo it replaces DOES alias a strided subset: every 4th
        // id maps to the same shard under `%`, but not under the hash.
        let strided: Vec<u32> = (0u64..4).map(|k| shard_for_extent(100 + k * 4, count)).collect();
        assert!(
            strided.iter().collect::<std::collections::HashSet<_>>().len() > 1,
            "strided ids must NOT all land on one shard under the hash"
        );
    }
}

#[cfg(test)]
mod admin_token_prefix_tests {
    use crate::manager_rpc::*;

    #[test]
    fn prefix_then_strip_round_trips() {
        let tok = b"deadbeef";
        let payload = b"the original rkyv payload bytes";
        let wire = prefix_admin_token(tok, payload);
        let (got_tok, rest) = strip_admin_token(&wire).expect("well-formed");
        assert_eq!(got_tok, tok);
        assert_eq!(rest, payload);
    }

    #[test]
    fn empty_token_and_empty_payload_are_valid() {
        let wire = prefix_admin_token(b"", b"");
        let (t, r) = strip_admin_token(&wire).unwrap();
        assert!(t.is_empty() && r.is_empty());
        // An empty payload with a real token.
        let wire = prefix_admin_token(b"tok", b"");
        let (t, r) = strip_admin_token(&wire).unwrap();
        assert_eq!(t, b"tok");
        assert!(r.is_empty());
    }

    #[test]
    fn malformed_prefix_is_none_never_run_bare() {
        // A bare (unprefixed) admin payload must NOT be mistaken for a valid
        // strip — the manager treats None as a failed check, not "run it bare".
        assert!(strip_admin_token(b"").is_none()); // no length header at all
        assert!(strip_admin_token(b"\x02\x00").is_none()); // header truncated (<4 B)
        // length says 100 but only 3 bytes follow → runs past the buffer.
        let mut bad = 100u32.to_le_bytes().to_vec();
        bad.extend_from_slice(b"abc");
        assert!(strip_admin_token(&bad).is_none());
    }

    #[test]
    fn the_admin_set_is_mutating_ops_only() {
        // A representative mutating op is gated …
        assert!(is_admin_mgr_msg(MSG_FENCE_NODE));
        assert!(is_admin_mgr_msg(MSG_MERGE_PARTITIONS));
        assert!(is_admin_mgr_msg(MSG_CREATE_STREAM));
        assert!(is_admin_mgr_msg(MSG_BUMP_CLUSTER_VERSION));
        // M3: the raw merge txn is gated so it can't bypass the guard.
        assert!(is_admin_mgr_msg(MSG_MULTI_MODIFY_MERGE));
        // … but MULTI_MODIFY_SPLIT stays ungated — it IS PS-driven.
        assert!(!is_admin_mgr_msg(MSG_MULTI_MODIFY_SPLIT));
        // … while read-only observability and the struct-field authz ops are NOT
        // (those carry their own admin_token field and stay fail-closed).
        assert!(!is_admin_mgr_msg(MSG_STATUS));
        assert!(!is_admin_mgr_msg(MSG_NODES_INFO));
        assert!(!is_admin_mgr_msg(MSG_TENANT_CREATE));
        assert!(!is_admin_mgr_msg(MSG_NAMESPACE_CREATE));
        assert!(!is_admin_mgr_msg(MSG_PRINCIPAL_LIST));
        // REGISTER_NODE is explicitly NOT gated (deviates from the design list):
        // the EXTENT NODE self-registers with it and has no admin token, so
        // gating it would wedge bring-up.
        assert!(!is_admin_mgr_msg(MSG_REGISTER_NODE));
    }
}

#[cfg(test)]
mod wire_version_tests {
    use super::*;

    #[test]
    fn fingerprint_is_nonempty_hex() {
        assert_eq!(WIRE_FINGERPRINT.len(), 16);
        assert!(WIRE_FINGERPRINT.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn registry_pins_current_schema_to_max_version() {
        // THE bump-enforcement test. If this fails you edited a wire-schema
        // source file: decide compatibility, bump WIRE_VERSION_MAX (and MIN
        // per the rules on WIRE_VERSION_MIN's doc), and record the new
        // fingerprint in WIRE_VERSION_FINGERPRINTS.
        let (last_ver, last_fp) = *WIRE_VERSION_FINGERPRINTS.last().unwrap();
        assert_eq!(
            last_ver, WIRE_VERSION_MAX,
            "registry tail must be WIRE_VERSION_MAX"
        );
        assert_eq!(
            last_fp, WIRE_FINGERPRINT,
            "wire schema changed without a version decision: bump \
WIRE_VERSION_MAX (pre-R3: also set MIN=MAX) and record the new fingerprint \
{WIRE_FINGERPRINT:?} in WIRE_VERSION_FINGERPRINTS"
        );
        // Registry is append-only and strictly increasing.
        let mut prev = 0;
        for (v, fp) in WIRE_VERSION_FINGERPRINTS {
            assert!(*v > prev, "registry versions must be strictly increasing");
            assert_eq!(fp.len(), 16);
            prev = *v;
        }
        assert!(WIRE_VERSION_MIN >= 1 && WIRE_VERSION_MIN <= WIRE_VERSION_MAX);
    }

    #[test]
    fn compat_accepts_same_fingerprint() {
        // Same fp accepts regardless of declared interval (the same-commit
        // fast path; also covers a buggy peer that zeroes the interval).
        assert!(wire_compat_check(WIRE_FINGERPRINT, 0, 0).is_ok());
        assert!(wire_compat_check(WIRE_FINGERPRINT, WIRE_VERSION_MIN, WIRE_VERSION_MAX).is_ok());
    }

    #[test]
    fn compat_accepts_overlapping_interval_from_newer_peer() {
        // Different build whose max is NEWER than anything we know,
        // overlapping declared interval → accept (the rolling window).
        assert!(
            wire_compat_check("deadbeefdeadbeef", WIRE_VERSION_MAX, WIRE_VERSION_MAX + 1).is_ok()
        );
    }

    #[test]
    fn compat_rejects_same_version_claim_with_different_schema() {
        // Forgot-to-bump caught at runtime: peer claims a version we have
        // a registered fingerprint for, but its schema differs (coco P1).
        let err = wire_compat_check("deadbeefdeadbeef", WIRE_VERSION_MIN, WIRE_VERSION_MAX)
            .unwrap_err();
        assert!(err.contains("wire-version fraud"), "{err}");
    }

    #[test]
    fn compat_rejects_disjoint_and_pre_r1() {
        // Disjoint interval → refuse with the actionable message.
        let err =
            wire_compat_check("deadbeefdeadbeef", WIRE_VERSION_MAX + 1, WIRE_VERSION_MAX + 2)
                .unwrap_err();
        assert!(err.contains("no common wire version"), "{err}");
        // Pre-R1 peer (empty fp, zero interval) → refuse.
        assert!(wire_compat_check("", 0, 0).is_err());
        // Malformed interval (min > max) → refuse.
        assert!(wire_compat_check("deadbeefdeadbeef", 3, 2).is_err());
    }
}
