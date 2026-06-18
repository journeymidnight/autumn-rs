//! autumn-rpc: wire protocol framing for custom binary RPC.
//!
//! Provides a 10-byte-header wire protocol with request multiplexing IDs.
//!
//! # Wire Format
//!
//! ```text
//! [req_id: u32 LE][msg_type: u8][flags: u8][payload_len: u32 LE][payload]
//! ```

pub mod client;
pub mod error;
pub mod extent_rpc;
pub mod frame;
pub mod manager_rpc;
pub mod partition_rpc;

/// Re-exported so consumers of `RpcClient::call_into_dest(reg: Option<&RegisteredMem>)`
/// don't need a direct autumn-transport dependency. (Uninhabited stub on
/// non-ucx builds — `reg` is always `None` there.)
pub use autumn_transport::RegisteredMem;
/// Re-exported so `call_into_pooled` consumers (autumn-stream's StreamClient)
/// reference `autumn_rpc::PooledBuf` without a direct autumn-transport dep.
/// Transport-agnostic: registered on `ucx`, plain (copy-out) on TCP/no-ucx.
pub use autumn_transport::{regpool_acquire, PooledBuf};
pub use error::{Result, RpcError, StatusCode};
pub use frame::{Frame, FrameDecoder, HEADER_LEN};

/// Handler result type for RPC dispatch.
pub type HandlerResult = std::result::Result<bytes::Bytes, (StatusCode, String)>;

/// Msg type reserved for heartbeat ping/pong.
pub const MSG_TYPE_PING: u8 = 0xFF;

/// WIRE-1: build-time fingerprint of the wire-schema source files
/// (manager_rpc / partition_rpc / frame / extent_rpc). Same-commit
/// deploys share it; ANY wire-struct edit changes it. Exchanged via
/// `GetClusterIdResp.wire_fingerprint` and checked at every long-lived
/// process's startup — a mixed-version join refuses LOUDLY instead of
/// silently decoding garbage (rkyv has no cross-version compat; F275).
pub const WIRE_FINGERPRINT: &str = env!("AUTUMN_WIRE_FINGERPRINT");

/// R1 (rolling upgrade design): the wire-version interval this binary
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
///     window (design §5: compat window is exactly N ↔ N-1).
pub const WIRE_VERSION_MIN: u32 = 8;
pub const WIRE_VERSION_MAX: u32 = 8;

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
    // v3: F-unify — extent_rpc relocated from autumn-stream into autumn-rpc
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
    (8, "92ea7fc8dd2afaa5"),
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
time (compat window is N ↔ N-1, docs/rolling_upgrade_design.md §5), or rebuild \
this binary/wheel from the cluster's commit."
    ))
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
