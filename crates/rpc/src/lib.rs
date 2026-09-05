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

/// The wire-version interval this binary speaks. `MAX` is the version of the
/// schema compiled into this binary; `MIN` is the oldest peer version it can
/// still interoperate with.
///
/// ─────────────────────────────────────────────────────────────────────────
///  ⚠️  EDIT A WIRE STRUCT → BUMP `WIRE_VERSION_MAX`. NOTHING CHECKS THIS
///      FOR YOU ANY MORE. There is no fingerprint and no registry test; this
///      constant is the ONLY thing standing between a schema change and a
///      silent corruption.
/// ─────────────────────────────────────────────────────────────────────────
///
/// The wire schema is `manager_rpc.rs`, `partition_rpc.rs`, `frame.rs`,
/// `extent_rpc.rs` and `cap_token.rs`. Adding, removing, reordering or
/// retyping any field of an `Archive` type in those files — or changing what
/// an existing field MEANS — is a wire change.
///
/// What happens if you forget: rkyv has no cross-version decode and no
/// version tag of its own. Two binaries claiming the same version with
/// different layouts do not fail the handshake; they decode each other's
/// bytes as whatever their own layout says, and carry on. That failure has
/// been seen here — a stale python wheel decoded `PutReq` with `part_id = 0`
/// and every write failed with nothing anywhere pointing at the cause.
///
/// How to decide the interval:
/// - pre-R2/R3 (where this tree is): bump `MAX` **and set `MIN = MAX`**. The
///   new version is incompatible with everything before it; deploying it is
///   stop-the-world, and every image carrying an embedded client must be
///   rebuilt at the same commit.
/// - post-R3 (frozen V1 + explicit V2 msg_types): bump `MAX`, keep
///   `MIN = MAX - 1`, so the binary serves both forms during a rolling
///   window. This tree is NOT post-R3: the client runs its compatibility
///   check once at connect and keeps nothing, so no call site can gate on
///   the negotiated version.
pub const WIRE_VERSION_MIN: u32 = 36;
pub const WIRE_VERSION_MAX: u32 = 36;


/// Peer wire-compat check: accept iff the version intervals overlap.
///
/// Callers treat a TRANSPORT failure fetching the peer's values as
/// best-effort-skip (the peer may be briefly down; availability wins),
/// but a SUCCESSFUL response that fails this check is a hard startup
/// refusal. A peer reporting `max == 0` (empty/pre-R1) is refused.
///
/// This used to also compare a build-time fingerprint of the schema source,
/// which caught one case the interval cannot: a peer that changed the schema
/// and did NOT bump its version. That check was removed deliberately. It had
/// cost more than it caught — hashing the schema files byte for byte meant a
/// translated comment once split a live cluster mid-rollout, and every false
/// alarm taught the reflex of refreshing the recorded value without looking,
/// which is how a real change would have been waved through anyway.
///
/// The cost of removing it is real and worth stating where someone will read
/// it: a forgotten `WIRE_VERSION_MAX` bump is now UNDETECTED. Two binaries
/// claiming the same version with different layouts will handshake happily
/// and then decode each other's bytes as garbage. See the discipline block on
/// `WIRE_VERSION_MIN`.
pub fn wire_compat_check(
    remote_min: u32,
    remote_max: u32,
) -> std::result::Result<(), String> {
    if remote_max >= 1 && remote_min <= remote_max {
        let lo = WIRE_VERSION_MIN.max(remote_min);
        let hi = WIRE_VERSION_MAX.min(remote_max);
        if lo <= hi {
            return Ok(());
        }
    }
    Err(format!(
        "wire-version mismatch: local=[{WIRE_VERSION_MIN},{WIRE_VERSION_MAX}], \
peer=[{remote_min},{remote_max}] — no common wire version (rkyv wire structs \
have no implicit cross-version compatibility; a mixed deploy decodes garbage \
silently). Upgrade one step at a time (compat window is N ↔ N-1), or rebuild \
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
    fn compat_accepts_overlapping_interval_from_newer_peer() {
        // Different build whose max is NEWER than anything we know,
        // overlapping declared interval → accept (the rolling window).
        assert!(wire_compat_check(WIRE_VERSION_MAX, WIRE_VERSION_MAX + 1).is_ok());
    }

    /// A peer claiming OUR version is accepted, full stop.
    ///
    /// This is the guarantee that was given up when the schema fingerprint
    /// was removed. There used to be a `compat_rejects_same_version_claim_
    /// with_different_schema` test here, and it passed: a peer that had
    /// edited the schema without bumping its version was caught at the
    /// handshake. Nothing catches that now — the version integer is the whole
    /// check, and it is maintained by hand.
    ///
    /// The test is kept, inverted, so the loss is visible to whoever reads
    /// this module rather than only to whoever reads the commit that removed
    /// it. If a schema check ever comes back, this assertion is what should
    /// fail first.
    #[test]
    fn compat_no_longer_verifies_the_peers_schema() {
        assert!(wire_compat_check(WIRE_VERSION_MIN, WIRE_VERSION_MAX).is_ok());
    }

    #[test]
    fn compat_rejects_disjoint_and_pre_r1() {
        // Disjoint interval → refuse with the actionable message.
        let err = wire_compat_check(WIRE_VERSION_MAX + 1, WIRE_VERSION_MAX + 2).unwrap_err();
        assert!(err.contains("no common wire version"), "{err}");
        // Pre-R1 peer (zero interval) → refuse.
        assert!(wire_compat_check(0, 0).is_err());
        // Malformed interval (min > max) → refuse.
        assert!(wire_compat_check(3, 2).is_err());
    }
}
