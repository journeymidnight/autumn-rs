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
pub mod client_hello;
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
pub use frame::{Frame, FrameDecoder, ReadWindow, HEADER_LEN};

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

/// The version of the wire schema compiled into this binary.
///
/// A CLUSTER peer must speak this exact version (`cluster_peer_compat_check`);
/// a CLIENT must fall inside `[MIN_CLIENT_WIRE_VERSION, WIRE_VERSION]`
/// (`client_compat_check`). Those are different questions and neither is an
/// interval overlap — see the two functions.
///
/// ─────────────────────────────────────────────────────────────────────────
///  ⚠️  EDIT A WIRE STRUCT → BUMP `WIRE_VERSION`. NOTHING CHECKS THIS FOR YOU
///      ANY MORE. There is no fingerprint and no registry test; this constant
///      is the ONLY thing standing between a schema change and a silent
///      corruption.
/// ─────────────────────────────────────────────────────────────────────────
///
/// The wire schema is `manager_rpc.rs`, `partition_rpc.rs`, `frame.rs`,
/// `extent_rpc.rs` and `cap_token.rs`. Adding, removing, reordering or
/// retyping any field of an `Archive` type in those files — or changing what
/// an existing field MEANS — is a wire change. (`client_hello.rs` is wire too,
/// but FROZEN rather than versioned: it is the negotiation channel, so it is
/// never edited at all.)
///
/// **A pure msg_type ADDITION is not a bump.** Until the hello landed the tree
/// treated one as a bump anyway, which is what made a new opcode expensive; an
/// old peer that never sends a msg_type cannot be affected by its existence,
/// and §7 of `docs/client_wire_compat_design.md` needs new opcodes to be cheap
/// because that is how a call site comes to serve two forms.
///
/// What happens if you forget: rkyv has no cross-version decode and no
/// version tag of its own. Two binaries claiming the same version with
/// different layouts do not fail the handshake; they decode each other's
/// bytes as whatever their own layout says, and carry on. That failure has
/// been seen here — a stale python wheel decoded `PutReq` with `part_id = 0`
/// and every write failed with nothing anywhere pointing at the cause.
///
/// Bump it on every wire change. There is no separate "oldest cluster peer"
/// constant: peers compare for EQUALITY, so a floor pinned to this value would
/// say nothing.
pub const WIRE_VERSION: u32 = 47;

/// The oldest CLIENT this binary serves — the floor of the client window
/// `[MIN_CLIENT_WIRE_VERSION, WIRE_VERSION]`.
///
/// **43, with `WIRE_VERSION` above it: the window is OPEN.** It was opened by
/// raising the CEILING. Lowering this floor instead was tried, and it is
/// UNSAFE — see `FIRST_WIRE_VERSION_WITH_PEER_EQUALITY`, which is the rule
/// that came out of it.
///
/// Raising it is the expensive direction, and only a change that breaks the
/// client-facing surface justifies it: this is the one constant answering
/// "does this force every image carrying an embedded client to be rebuilt",
/// and a client is not ours to restart — it lives inside an inference pod, a
/// mounted fuse daemon, an s3 gateway, somebody else's image. Raising
/// `WIRE_VERSION` alone leaves every client inside the window untouched, which
/// is the entire point and is now a fact about the tree rather than a plan.
///
/// It rides in `GetClusterIdResp`'s `wire_version_min` FIELD. That struct is
/// frozen (it is the negotiation channel, decoded before any compat decision
/// can be made), so the field name outlives the constant it carries; the
/// mismatch is deliberate and noted at both ends.
///
pub const MIN_CLIENT_WIRE_VERSION: u32 = 43;

/// An inverted window would refuse every client while every server came up
/// happy — `cluster_peer_compat_check` never looks at the floor, so nothing
/// else would notice. A `const` rather than a test because it must hold for
/// every value either constant ever takes.
const _: () = assert!(MIN_CLIENT_WIRE_VERSION <= WIRE_VERSION);

/// The first version whose CLUSTER PEERS police themselves by EQUALITY.
///
/// Before this version a partition server and an extent node checked
/// themselves with an interval OVERLAP against the pair the manager reports
/// (`wire_compat_check`, deleted in `f17f533`). Those binaries are still on
/// disk and still get launched — see `project_clustersh_uses_stale_release_binaries`
/// — and they read `wire_version_min` as a PEER floor, because when they were
/// written it was one.
///
/// So the floor is not only a client promise: to every pre-43 binary it is an
/// ENTRY TICKET. Lowering it to 42 was implemented and reverted for exactly
/// this. A stale 42 partition server computes `[42,42] ∩ [43,43] = ∅` and
/// refuses itself today; against a reported `[42,43]` it computes `{42}` and
/// JOINS. Nothing server-side catches it afterwards — `RegisterPsReq` and
/// `RegisterNodeReq` carry no version, they are outside the client-surface
/// gate, and a silent connection is read as 43 regardless. That is a
/// mixed-version cluster on the INTERNAL plane, which is the one thing
/// stop-the-world exists to make impossible.
///
/// **INVARIANT: the client floor may never go below this.** Raising the
/// ceiling is the safe way to open the window, and it is safe in every
/// direction: a pre-43 peer's overlap misses a window that starts at 43, and a
/// 43-or-later peer demands exact equality and so never looks at the floor at
/// all.
pub const FIRST_WIRE_VERSION_WITH_PEER_EQUALITY: u32 = 43;

/// The version that introduced `MSG_GET_CLIENT_REGIONS` — the narrowed routing
/// reply an embedded client gets instead of the partition server's seven-field
/// `MgrRegionInfo`.
///
/// The SDK must ASK this before it sends that opcode, because a cluster below
/// it has no handler: the manager's dispatch refuses an unknown msg_type with
/// `InvalidArgument`, so a client that asked blind would fail every refresh. `negotiated_cluster_wire` starts at 0 and a silent connection
/// never raises it, so `negotiated >= this` fails CLOSED — an unknown cluster
/// gets the old `MSG_GET_REGIONS`, which every cluster in the window serves.
///
/// Choosing WHICH OPCODE TO SEND on the negotiated version is not the thing
/// design §7 forbids. What it forbids is branching the INTERPRETATION of a
/// received frame on connection state, because rkyv mis-decodes silently and a
/// missed hello would then read bytes at the wrong version. Here the reply is
/// self-describing: it comes back under the msg_type the request named.
pub const WIRE_VERSION_WITH_CLIENT_REGIONS: u32 = 45;

const _: () = assert!(WIRE_VERSION_WITH_CLIENT_REGIONS <= WIRE_VERSION);

/// The version that introduced `MSG_COMPARE_WRITE` (fenced conditional put or
/// delete). Gated like `WIRE_VERSION_WITH_CLIENT_REGIONS`: the SDK refuses to
/// send it to a cluster negotiated below this rather than let an older
/// partition server reject an opcode it has no handler for.
pub const WIRE_VERSION_WITH_COMPARE_WRITE: u32 = 47;

const _: () = assert!(WIRE_VERSION_WITH_COMPARE_WRITE <= WIRE_VERSION);

const _: () = assert!(MIN_CLIENT_WIRE_VERSION >= FIRST_WIRE_VERSION_WITH_PEER_EQUALITY);

/// A client that says nothing is assumed to speak
/// `client_hello::WIRE_VERSION_WITH_CLIENT_HELLO`, so a floor above that
/// number refuses every silent connection at once — every client image built
/// before the handshake existed. That is a legitimate future act (it is what a
/// client-facing break costs), but it is a fleet-wide one, so it may not be
/// reached by editing a number: DELETE this line deliberately, with the
/// announcement that goes with it.
///
/// Together with the assertion above, the floor is pinned at exactly 43 until
/// someone removes one of them on purpose.
const _: () = assert!(MIN_CLIENT_WIRE_VERSION <= client_hello::WIRE_VERSION_WITH_CLIENT_HELLO);


/// CLUSTER-peer compat check: accept iff the peer speaks our exact version.
///
/// Equality, not interval overlap, and the difference is load-bearing. The
/// manager reports `MIN_CLIENT_WIRE_VERSION` in the `wire_version_min` slot
/// because that is what a client needs, so an overlap test would admit a stale
/// PS or EN sitting anywhere inside the CLIENT window — and the handshake is
/// the only thing enforcing stop-the-world. Equality is also the honest
/// spelling of the rule: manager, PS and EN binaries swap in one window and
/// never face a peer of another version.
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
/// it: a forgotten `WIRE_VERSION` bump is now UNDETECTED. Two binaries
/// claiming the same version with different layouts will handshake happily
/// and then decode each other's bytes as garbage. See the discipline block on
/// `WIRE_VERSION`.
pub fn cluster_peer_compat_check(remote_max: u32) -> std::result::Result<(), String> {
    if remote_max == WIRE_VERSION {
        return Ok(());
    }
    Err(format!(
        "wire-version mismatch: this binary speaks {WIRE_VERSION}, the cluster \
speaks {remote_max}. Cluster members must all run the SAME commit (rkyv wire \
structs have no implicit cross-version compatibility; a mixed deploy decodes \
garbage — and rkyv does not always fail loudly when it does). Stop every manager, partition server \
and extent node, swap the binaries together, and start."
    ))
}

/// CLIENT compat check: accept iff OUR version falls inside the window the
/// cluster serves.
///
/// Membership, and BOTH ends refuse. Below the floor the cluster no longer
/// keeps the behavior this client needs. Above `remote_max` the cluster cannot
/// speak what this client will send — not a corner case here, since images are
/// built from `main` and a wheel routinely runs ahead of a cluster nobody has
/// upgraded yet.
///
/// `remote_min` arrives in `GetClusterIdResp.wire_version_min`, which carries
/// the cluster's `MIN_CLIENT_WIRE_VERSION` (frozen field name, different
/// constant — see `MIN_CLIENT_WIRE_VERSION`). A cluster reporting `max == 0`
/// (empty/pre-R1) is refused.
///
/// This runs at connect and is a courtesy, not the gate: it is skipped when the
/// fetch itself fails, and the design puts admission at the server. Until that
/// lands, nothing validates an incoming client at all.
pub fn client_compat_check(
    remote_min: u32,
    remote_max: u32,
) -> std::result::Result<(), String> {
    if (remote_min..=remote_max).contains(&WIRE_VERSION) {
        return Ok(());
    }
    // An empty range (`min > max`) and a pre-R1 `max == 0` both fall out of
    // `contains` on their own; neither needs its own guard.
    let why = if remote_max == 0 {
        // NOT "unbootstrapped": that arm of `handle_get_cluster_id` fills both
        // versions from the constants, so it reports real ones.
        "the cluster reported no wire version at all — it predates the version \
handshake"
    } else if WIRE_VERSION > remote_max {
        "this client is NEWER than the cluster — deploy the cluster, or build \
the client from the cluster's commit"
    } else {
        "this client is older than the window the cluster still serves — \
rebuild it from a commit inside that window"
    };
    Err(format!(
        "wire-version mismatch: this client speaks {WIRE_VERSION}, the cluster \
serves [{remote_min},{remote_max}] — {why}."
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
    fn a_cluster_peer_must_match_exactly() {
        assert!(cluster_peer_compat_check(WIRE_VERSION).is_ok());
        for other in [WIRE_VERSION + 1, WIRE_VERSION - 1, 0] {
            assert!(
                cluster_peer_compat_check(other).is_err(),
                "peer at {other} must be refused"
            );
        }
    }

    /// The reason peers cannot reuse the client rule. `wire_version_min`
    /// carries the CLIENT floor, so anything interval-shaped would admit a
    /// stale server sitting inside the client window — and this handshake is
    /// the only thing enforcing stop-the-world.
    #[test]
    fn a_stale_server_inside_the_client_window_is_still_refused() {
        // The REAL window, not a synthetic one. This test used to place the
        // stale server at `WIRE_VERSION + 2` — OUTSIDE the window — so its
        // name claimed more than its fixture and it survived merging the two
        // predicates back into one. With the window genuinely open the
        // interesting server sits at the FLOOR: a client there is served, a
        // server there must not be.
        let (floor, ceiling) = (MIN_CLIENT_WIRE_VERSION, WIRE_VERSION);
        assert!(
            floor < ceiling,
            "this test needs an OPEN window to mean anything, and the window \
             just closed. If you raised the floor for a client-facing break \
             that is expected: see `the_client_window_is_open_and_the_floor_is_\
             where_it_belongs`, which carries the checklist. Do not delete \
             this assertion to make the test pass."
        );
        assert!(
            client_compat_check(floor, ceiling).is_ok(),
            "a CLIENT inside the window is served"
        );
        assert!(
            cluster_peer_compat_check(floor).is_err(),
            "a SERVER at the floor is inside the CLIENT window and must still \
             be refused — the floor is not a licence to join"
        );
        // And one above the ceiling, which the old fixture was really testing.
        assert!(cluster_peer_compat_check(ceiling + 2).is_err());
    }

    #[test]
    fn a_client_is_admitted_inside_the_window_and_refused_at_both_ends() {
        // Inside, including both boundaries.
        assert!(client_compat_check(WIRE_VERSION, WIRE_VERSION).is_ok());
        assert!(client_compat_check(WIRE_VERSION - 1, WIRE_VERSION).is_ok());
        assert!(client_compat_check(WIRE_VERSION, WIRE_VERSION + 1).is_ok());

        // Below the floor.
        let err = client_compat_check(WIRE_VERSION + 1, WIRE_VERSION + 3).unwrap_err();
        assert!(err.contains("older than the window"), "{err}");

        // ABOVE the ceiling — a wheel built from main against a lagging
        // cluster. The message has to name which way round it is, because the
        // fix differs: deploy the cluster, or rebuild the client.
        let err = client_compat_check(WIRE_VERSION - 2, WIRE_VERSION - 1).unwrap_err();
        assert!(err.contains("NEWER than the cluster"), "{err}");
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
        assert!(cluster_peer_compat_check(WIRE_VERSION).is_ok());
        assert!(client_compat_check(MIN_CLIENT_WIRE_VERSION, WIRE_VERSION).is_ok());
    }

    #[test]
    fn compat_rejects_pre_r1_and_malformed() {
        // Pre-R1 peer (zero interval) → refuse on both paths, and say so
        // rather than blaming the client's age.
        assert!(cluster_peer_compat_check(0).is_err());
        let err = client_compat_check(0, 0).unwrap_err();
        assert!(err.contains("predates the version handshake"), "{err}");
        // Malformed (min > max) → an empty range contains nothing → refuse.
        assert!(client_compat_check(3, 2).is_err());
    }

    /// The window is OPEN: `[43, 47]`, opened by raising the CEILING and
    /// widened by `MSG_GET_CLIENT_REGIONS` and `MSG_COMPARE_WRITE`.
    ///
    /// Pinned to literals so that the two silently becoming equal again — which
    /// would take the whole window's coverage down with it — cannot pass.
    /// **If this fails because you bumped `WIRE_VERSION`: that is correct and
    /// expected.** Move the ceiling up and leave the 43 alone; the floor rises only
    /// for a change that breaks the client-facing surface, and the `const`
    /// assertions beside the constants say what pins it there.
    #[test]
    fn the_client_window_is_open_and_the_floor_is_where_it_belongs() {
        assert_eq!(MIN_CLIENT_WIRE_VERSION, 43, "read this test's comment");
        assert_eq!(WIRE_VERSION, 47, "read this test's comment");
    }

    /// The check that actually decides whether a stale SERVER joins is the one
    /// compiled into THAT server, not the one in this binary — and before
    /// `FIRST_WIRE_VERSION_WITH_PEER_EQUALITY` it was an interval overlap
    /// against the pair the manager reports. So the floor is an entry ticket to
    /// every pre-43 binary, and this models their predicate rather than ours.
    ///
    /// Lowering the floor to 42 was implemented, and this is what sent it back.
    #[test]
    fn the_floor_never_hands_a_pre_equality_peer_a_ticket() {
        // `wire_compat_check` as it stood at 42, verbatim in behaviour: an
        // overlap of the peer's own [v, v] against the reported window.
        let stale_peer_would_join =
            |v: u32, rep_min: u32, rep_max: u32| rep_min.max(v) <= rep_max.min(v);

        // What this cluster reports. No pre-equality binary overlaps it.
        let (lo, hi) = (MIN_CLIENT_WIRE_VERSION, WIRE_VERSION);
        for stale in 1..FIRST_WIRE_VERSION_WITH_PEER_EQUALITY {
            assert!(
                !stale_peer_would_join(stale, lo, hi),
                "a wire-{stale} partition server or extent node would JOIN a \
                 cluster reporting [{lo},{hi}] — the floor became an entry \
                 ticket. See FIRST_WIRE_VERSION_WITH_PEER_EQUALITY."
            );
        }
        // And the shape that proves the test can fail: drop the floor by one.
        assert!(stale_peer_would_join(
            FIRST_WIRE_VERSION_WITH_PEER_EQUALITY - 1,
            FIRST_WIRE_VERSION_WITH_PEER_EQUALITY - 1,
            hi
        ));
    }

    /// A client built at the floor — the one this feature exists for, and the
    /// first that an open window actually serves.
    ///
    /// It models the CLIENT's predicate as that client compiled it. A 43 client
    /// is the first with the equality-era code, so it runs `client_compat_check`
    /// with its own `WIRE_VERSION` of 43 against the reported pair.
    #[test]
    fn a_client_one_version_behind_the_cluster_is_served() {
        let (lo, hi) = (MIN_CLIENT_WIRE_VERSION, WIRE_VERSION);
        assert!(lo < hi, "nothing below is meaningful with the window shut");
        // The 43 client's own check, which used to refuse it: `contains(43)`.
        assert!((lo..=hi).contains(&MIN_CLIENT_WIRE_VERSION));
        // This binary (the cluster's own commit) is served by its own report.
        assert!(client_compat_check(lo, hi).is_ok());
        // A SERVER is still held to equality — the floor is no licence to join.
        assert!(cluster_peer_compat_check(hi).is_ok());
        assert!(cluster_peer_compat_check(lo).is_err());
    }
}
