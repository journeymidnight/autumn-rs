//! `MSG_CLIENT_HELLO` — the client→server half of the version handshake, and
//! the msg_type set the server admits on it.
//!
//! `GetClusterIdResp` already carries server→client. Nothing carried
//! client→server: `MSG_AUTH_HELLO` has no version field, and a client with no
//! credential never sends it at all, so an authz-disabled cluster — fuse,
//! kvcache, every dev cluster — would see it never.
//!
//! ⚠️ **This module is FROZEN.** It is the negotiation channel: its bytes are
//! decoded before any compatibility decision can be made, so a client already
//! deployed reads them with code that cannot be changed. Additions go in new
//! msg_types. `crates/rpc/tests/negotiation_freeze.rs` pins the encodings byte
//! for byte.
//!
//! Hand-coded fixed-layout binary, NOT rkyv, and the reason is specific rather
//! than stylistic: rkyv's archived root sits at the END of its buffer, so a
//! decoder reading a longer peer's struct reads its SUFFIX. A two-`u64` struct
//! decoding a three-`u64` one returns `Ok` with the fields shifted, and a `u32`
//! added into tail padding round-trips `Ok` in both directions with the new
//! field reading zero. Only `Vec`/`String` shapes fail loudly — and the one
//! message whose job is to detect a version mismatch must not depend on its own
//! shape to do it.

use crate::manager_rpc;
use crate::partition_rpc;

/// Sent once per connection, before any other frame, on every manager and PS
/// connection a client opens. A new msg_type, so it changes no existing struct.
///
/// Free in BOTH the manager and the partition-server number spaces, which it
/// has to be — the same frame goes to both.
pub const MSG_CLIENT_HELLO: u8 = 0x5F;

/// `b"AUH1"` read little-endian. Its job is to stop an unrelated frame that
/// lands on this msg_type from being read as a version: without it any 8-byte
/// payload would parse, and the number it yielded would decide admission.
pub const CLIENT_HELLO_MAGIC: u32 = u32::from_le_bytes(*b"AUH1");

/// The wire version in which the hello was introduced, and therefore what a
/// SILENT connection is assumed to speak.
///
/// FROZEN at 43 forever — it is a fact about history, not about this binary.
/// Following `WIRE_VERSION` would make every silent connection look current and
/// the assumption would stop meaning anything.
///
/// This is what made server-side admission inert on arrival: on the
/// introduction commit `MIN_CLIENT_WIRE_VERSION == WIRE_VERSION == 43`, so a
/// client built the day before (silent, assumed 43) and one built from that
/// commit (says 43) were admitted alike. The window has since opened DOWNWARD
/// to `[42, 43]`, which only admits more, so silence still means served.
///
/// A floor above this number would refuse every silent connection at once —
/// `lib.rs` carries a `const` assertion against that, since it is the one way
/// to turn a routine-looking constant edit into a fleet-wide outage.
pub const WIRE_VERSION_WITH_CLIENT_HELLO: u32 = 43;

/// Request payload: `[magic: u32 LE][client_wire_version: u32 LE]`.
pub const CLIENT_HELLO_REQ_LEN: usize = 8;
/// Response payload: `[server_wire_version: u32 LE][min_client_wire_version: u32 LE]`.
///
/// No status byte. A refusal is a normal `FLAG_ERROR` frame carrying
/// `StatusCode::FailedPrecondition` and a message, because a refusal that
/// cannot say WHICH WAY ROUND it is (rebuild the client vs deploy the cluster)
/// is not worth sending, and the tree already has exactly one way to carry text.
pub const CLIENT_HELLO_RESP_LEN: usize = 8;

/// Field names here match the constants they carry, unlike `GetClusterIdResp`'s
/// — nothing already deployed reads these, so they were free to be named right.
pub fn encode_hello_req(client_wire_version: u32) -> [u8; CLIENT_HELLO_REQ_LEN] {
    let mut out = [0u8; CLIENT_HELLO_REQ_LEN];
    out[0..4].copy_from_slice(&CLIENT_HELLO_MAGIC.to_le_bytes());
    out[4..8].copy_from_slice(&client_wire_version.to_le_bytes());
    out
}

/// `None` = not a hello (wrong length or wrong magic). The server answers that
/// `InvalidArgument`; it never guesses a version out of it.
pub fn parse_hello_req(payload: &[u8]) -> Option<u32> {
    if payload.len() != CLIENT_HELLO_REQ_LEN {
        return None;
    }
    let magic = u32::from_le_bytes(payload[0..4].try_into().ok()?);
    if magic != CLIENT_HELLO_MAGIC {
        return None;
    }
    Some(u32::from_le_bytes(payload[4..8].try_into().ok()?))
}

pub fn encode_hello_resp(
    server_wire_version: u32,
    min_client_wire_version: u32,
) -> [u8; CLIENT_HELLO_RESP_LEN] {
    let mut out = [0u8; CLIENT_HELLO_RESP_LEN];
    out[0..4].copy_from_slice(&server_wire_version.to_le_bytes());
    out[4..8].copy_from_slice(&min_client_wire_version.to_le_bytes());
    out
}

/// `(server_wire_version, min_client_wire_version)`.
pub fn parse_hello_resp(payload: &[u8]) -> Option<(u32, u32)> {
    if payload.len() != CLIENT_HELLO_RESP_LEN {
        return None;
    }
    let server = u32::from_le_bytes(payload[0..4].try_into().ok()?);
    let min_client = u32::from_le_bytes(payload[4..8].try_into().ok()?);
    Some((server, min_client))
}

/// The SERVER's admission predicate: does this client fall inside our window?
///
/// The mirror image of `client_compat_check`, and both bounds refuse for
/// different reasons. Below `MIN_CLIENT_WIRE_VERSION` this server no longer
/// keeps the behavior that client needs. Above `WIRE_VERSION` it cannot speak
/// what the client will send — not a corner case, since images are built from
/// `main` and a wheel routinely runs ahead of a cluster nobody has upgraded yet.
///
/// The refusal names the cluster's version, because the fix differs by
/// direction and the operator needs to know which.
pub fn admit_client(client_wire_version: u32) -> std::result::Result<(), String> {
    admit_client_within(
        crate::MIN_CLIENT_WIRE_VERSION,
        crate::WIRE_VERSION,
        client_wire_version,
    )
}

/// `admit_client` with the window supplied instead of reached for.
///
/// The mirror of `client_compat_check(remote_min, remote_max)`, which has taken
/// its bounds as arguments since the two checks were split. This one did not,
/// so no caller had ever handed the SERVER predicate an open window. (The
/// client one had: `lib.rs`'s tests pass distinct bounds. What neither had ever
/// done is put the checked version strictly INSIDE the window — every existing
/// open-window call sits on a boundary.)
///
/// What an equal pair hides is narrower than it first looks, and worth stating
/// precisely rather than grandly. The two refusal ARMS were always
/// distinguishable, since the branch is on `v > hi` and not on the bounds —
/// swapping the two pieces of advice reds the older test too. What it hides is
/// (a) the window's INTERIOR, the only client this feature was built to keep
/// serving, which did not exist as a value; and (b) anything that reads one
/// bound where it means the other, the refusal text included: `[{lo},{hi}]`
/// printed as `[{hi},{hi}]` renders identically until the day the window
/// opens.
pub fn admit_client_within(
    lo: u32,
    hi: u32,
    client_wire_version: u32,
) -> std::result::Result<(), String> {
    if (lo..=hi).contains(&client_wire_version) {
        return Ok(());
    }
    let why = if client_wire_version > hi {
        "that client is NEWER than this cluster — deploy the cluster, or build \
the client from the cluster's commit"
    } else {
        "that client is older than the window this cluster still serves — \
rebuild its image from a commit inside that window"
    };
    Err(format!(
        "wire-version mismatch: this cluster speaks {hi} and serves clients \
[{lo},{hi}], the client speaks {client_wire_version} — {why}."
    ))
}

/// Admission for a connection that may never have said anything: `None` is a
/// connection that sent no hello, which is assumed to speak
/// `WIRE_VERSION_WITH_CLIENT_HELLO`.
pub fn admit_connection(conn_wire_version: Option<u32>) -> std::result::Result<(), String> {
    admit_connection_within(
        crate::MIN_CLIENT_WIRE_VERSION,
        crate::WIRE_VERSION,
        conn_wire_version,
    )
}

/// `admit_connection` with the window supplied. See `admit_client_within`.
pub fn admit_connection_within(
    lo: u32,
    hi: u32,
    conn_wire_version: Option<u32>,
) -> std::result::Result<(), String> {
    admit_client_within(
        lo,
        hi,
        conn_wire_version.unwrap_or(WIRE_VERSION_WITH_CLIENT_HELLO),
    )
}

/// The version pair this binary reports, mapped onto the FROZEN field names
/// once instead of at every site that reports it.
///
/// The mapping is not mechanical, which is the whole reason it has a home:
/// `wire_version_min` does not carry a cluster minimum, it carries
/// `MIN_CLIENT_WIRE_VERSION`, a CLIENT floor — the names were frozen before
/// the two questions were told apart, and `GetClusterIdResp` is the
/// negotiation channel, so they cannot be renamed. Someone filling those
/// fields from the names alone gets it wrong.
///
/// The stakes are why this exists rather than the hello's. A swapped pair here
/// reaches every partition server and extent node at startup
/// (`cluster_peer_compat_check(resp.wire_version_max)`), every pre-hello
/// client, `ClusterClient::connect`, and `autumn-op`: once the window opens,
/// `max` carrying the floor refuses every server's startup check and the
/// reversed range refuses every client. Today, with the constants equal, the
/// swap is invisible to every test in the tree — so the answer is one site,
/// not a test that cannot see it.
pub struct ReportedWireVersions {
    /// → `wire_version_min`. The oldest CLIENT served.
    pub min: u32,
    /// → `wire_version_max`. What this binary speaks.
    pub max: u32,
}

pub fn reported_wire_versions() -> ReportedWireVersions {
    ReportedWireVersions {
        min: crate::MIN_CLIENT_WIRE_VERSION,
        max: crate::WIRE_VERSION,
    }
}

/// The hello response THIS server sends, built in one place for the same
/// reason as `reported_wire_versions` — though with lower stakes, since the
/// only production reader of `parse_hello_resp` keeps the first number and
/// discards the second (`crates/client/src/lib.rs`). A swap here would make
/// `negotiated_cluster_wire` the floor rather than the ceiling. That is no
/// longer merely latent: `refresh_regions` branches on the number, so a swapped
/// pair would report 43 and push every client permanently onto the OLD routing
/// opcode. Still not an outage — the old form is SERVED, which is the whole
/// point of two forms — but a silent fallback that no test would notice, so
/// keep the pair built here and nowhere else.
pub fn server_hello_resp() -> [u8; CLIENT_HELLO_RESP_LEN] {
    let v = reported_wire_versions();
    encode_hello_resp(v.max, v.min)
}

/// The PS msg_types an EMBEDDED CLIENT sends — the set admission is scoped to.
///
/// **Scoped by msg_type, never by connection, and that is not a refinement:
/// without it the first floor move is a cluster outage.** The listeners that
/// serve clients also serve internal peers, and that peer traffic is SILENT —
/// PS→manager and EN→manager go through `autumn_stream::ConnPool` straight to
/// `RpcClient::connect` with no handshake, and manager→PS drives split /
/// maintenance / merge-freeze / roll-tails the same way. Nothing in a frame
/// says which role sent it, so a connection-scoped rule has nothing to key on.
///
/// (The one peer that DOES handshake is the extent node's startup identity
/// check, which is a `ClusterClient` — `verify_manager_cluster_id` and
/// `register_with_manager`. It is always at `WIRE_VERSION`, so it is always
/// admitted; and both messages it sends are un-gated anyway.)
///
/// Out, because they are operator or manager traffic rather than SDK traffic:
/// `MSG_SPLIT_PART`, `MSG_MAINTENANCE`, `MSG_MERGE_PART`, `MSG_MERGE_FREEZE`,
/// `MSG_ROLL_TAILS`, `MSG_GET_DISCARDS`, and the two `MSG_DIAG_*`.
///
/// `autumn-op`'s messages compile into the same `autumn-client` crate an
/// embedded client links, so "belongs to autumn-op" is not a property the
/// LINKER can see — which is why this is a hand-maintained set with a test
/// against the SDK's data-plane entry points, rather than a module split.
pub fn is_client_surface_ps_msg(msg_type: u8) -> bool {
    use partition_rpc::*;
    matches!(
        msg_type,
        MSG_PUT
            | MSG_PUT_BULK
            | MSG_DELETE
            | MSG_HEAD
            | MSG_RANGE
            | MSG_GET_BULK
            | MSG_BATCH_PUT
            | MSG_BATCH_PUT_BULK
            | MSG_BATCH_GET_BULK
            | MSG_BATCH_DELETE
            | MSG_GET_REDIRECT
            | MSG_GET_REDIRECT_MANY
            | MSG_AUTH_HELLO
            | MSG_COMPARE_PUT
            | MSG_COMPARE_WRITE
    )
}

/// The manager msg_types an embedded client sends.
///
/// `MSG_GET_CLUSTER_ID` is EXEMPT: it is how a peer finds out what it is
/// talking to, and gating the question on its own answer admits nobody.
///
/// **The OPERATOR surface is deliberately uncovered.** `MSG_STATUS`, the
/// stream/extent info calls, `namespace_*`, `tenant_*`, the op-ledger
/// (`MSG_OP_SUBMIT` / `QUERY` / `HISTORY`), autopolicy and the
/// `MSG_MULTI_MODIFY_*` family stay reachable from a client of any version.
/// They are `autumn-op`'s messages, and `autumn-op` ships WITH the cluster —
/// it is built and deployed at the same commit as the servers, so it is never
/// out of window in practice and a window would buy it nothing. The cost of
/// being wrong about that is real, since those are rkyv structs and a
/// cross-version decode is only SOMETIMES loud: an operator running a stale
/// `autumn-op` gets the same silent misread this mechanism protects the data
/// plane from. Covering it means paying a second maintained window for a
/// binary nobody embeds; the trade is recorded rather than hidden.
///
/// **`MSG_GET_REGIONS` is deliberately NOT here, and it is the one message this
/// set cannot cover.** It is on both surfaces — an SDK routes with it, and so
/// does every partition server's `sync_regions_once`. A PS sends no hello, so
/// once the floor rises above `WIRE_VERSION_WITH_CLIENT_HELLO` gating it would
/// refuse region sync for the whole fleet: the outage this scoping exists to
/// prevent, arriving through the set instead of through the connection. What
/// that leaves is a below-floor client able to fetch routing and nothing else —
/// every data-plane message it then sends is refused, which is where the
/// damage would be. Closing it properly means teaching cluster peers to
/// identify themselves, which is a different change.
///
/// **`MSG_GET_CLIENT_REGIONS` IS here, and that is the residue shrinking.**
/// The narrowed routing reply is client-only by construction — no partition
/// server sends it, because a PS needs the stream ids the narrow form drops —
/// so it can be gated where its dual-surface predecessor could not. A client
/// inside the window keeps using it; one below the floor is refused here
/// instead of being handed routing it has no business acting on. The old
/// opcode stays un-gated for the PS's sake, so the hole is not closed, only
/// narrowed to clients old enough to still be asking with it.
pub fn is_client_surface_mgr_msg(msg_type: u8) -> bool {
    use manager_rpc::*;
    matches!(
        msg_type,
        MSG_GET_CLIENT_REGIONS
            | MSG_MINT_TOKEN
            | MSG_ALLOC_INODES
            | MSG_ACQUIRE_LEASE
            | MSG_RELEASE_LEASE
            | MSG_HEARTBEAT_LEASE
            | MSG_POLL_INVALIDATIONS
            | MSG_CLUSTER_DF
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MIN_CLIENT_WIRE_VERSION, WIRE_VERSION};

    #[test]
    fn the_hello_opcode_is_free_in_both_number_spaces() {
        // The same frame goes to the manager AND the PS, so a collision in
        // either space would route it into somebody else's handler.
        //
        // This asserts only that the opcode is outside the four sets THIS
        // module reasons about. It cannot catch a future `MSG_FOO = 0x5F`:
        // both gates intercept 0x5F ahead of dispatch, so a collision would be
        // silently SHADOWED rather than fail here. The real guard is that the
        // opcode is frozen and recorded in `negotiation_freeze.rs`, so the
        // collision has to be introduced by the newcomer, not by this file.
        assert!(!manager_rpc::is_admin_mgr_msg(MSG_CLIENT_HELLO));
        assert!(!partition_rpc::is_admin_ps_msg(MSG_CLIENT_HELLO));
        assert!(!is_client_surface_ps_msg(MSG_CLIENT_HELLO));
        assert!(!is_client_surface_mgr_msg(MSG_CLIENT_HELLO));
        assert_ne!(MSG_CLIENT_HELLO, crate::MSG_TYPE_PING);
    }

    #[test]
    fn a_hello_round_trips() {
        for v in [0u32, 1, 43, u32::MAX] {
            assert_eq!(parse_hello_req(&encode_hello_req(v)), Some(v));
        }
        for (s, m) in [(43u32, 43u32), (45, 40), (0, 0)] {
            assert_eq!(parse_hello_resp(&encode_hello_resp(s, m)), Some((s, m)));
        }
    }

    #[test]
    fn a_payload_that_is_not_a_hello_yields_no_version() {
        // Both guards matter. Without the magic, ANY 8-byte payload landing on
        // this msg_type would parse and the number it yielded would decide
        // admission; without the length check a longer frame's first 8 bytes
        // would.
        let good = encode_hello_req(43);
        assert_eq!(parse_hello_req(&good), Some(43));

        let mut wrong_magic = good;
        wrong_magic[0] ^= 0xFF;
        assert_eq!(parse_hello_req(&wrong_magic), None);

        assert_eq!(parse_hello_req(&[]), None);
        assert_eq!(parse_hello_req(&good[..7]), None);
        let mut too_long = good.to_vec();
        too_long.push(0);
        assert_eq!(parse_hello_req(&too_long), None);

        assert_eq!(parse_hello_resp(&[]), None);
        assert_eq!(parse_hello_resp(&[0u8; 9]), None);
    }

    #[test]
    fn admission_refuses_at_both_ends_and_says_which() {
        assert!(admit_client(WIRE_VERSION).is_ok());

        let too_new = admit_client(WIRE_VERSION + 1).unwrap_err();
        assert!(too_new.contains("NEWER"), "{too_new}");
        assert!(too_new.contains("deploy the cluster"), "{too_new}");

        let too_old = admit_client(MIN_CLIENT_WIRE_VERSION - 1).unwrap_err();
        assert!(too_old.contains("older"), "{too_old}");
        assert!(too_old.contains("rebuild"), "{too_old}");
    }

    #[test]
    fn an_open_window_admits_its_whole_range_and_refuses_outside_it() {
        // The configuration this feature exists for: `lo < hi`, with the
        // checked version strictly INSIDE. No test anywhere had done that —
        // `lib.rs` does pass the client predicate distinct bounds, but always
        // with the version on a boundary.
        //
        // The two ARMS were always reachable — the branch is on `v > hi`, not
        // on the bounds, so the existing test already drove both (confirmed by
        // ablation: swapping the two pieces of advice reds it too). What an
        // equal pair could not express is the interior, and a refusal message
        // that reads one bound where it means the other.
        let (lo, hi) = (40, 45);
        for v in lo..=hi {
            assert!(
                admit_client_within(lo, hi, v).is_ok(),
                "{v} is inside [{lo},{hi}]"
            );
        }

        let below = admit_client_within(lo, hi, lo - 1).unwrap_err();
        assert!(below.contains("older"), "{below}");
        assert!(below.contains("rebuild its image"), "{below}");

        let above = admit_client_within(lo, hi, hi + 1).unwrap_err();
        assert!(above.contains("NEWER"), "{above}");
        assert!(above.contains("deploy the cluster"), "{above}");

        // Both name the window AND the cluster's own version, because the
        // operator's next move depends on which side of it they are on. Two
        // separate numbers that rendered identically until the window opened.
        for msg in [&below, &above] {
            assert!(msg.contains("[40,45]"), "{msg}");
            assert!(msg.contains("speaks 45"), "{msg}");
        }
    }

    #[test]
    fn a_pre_hello_client_falls_out_of_the_window_when_the_floor_passes_it() {
        // A silent connection is read as the version the hello arrived in, so
        // raising the floor past that literal is exactly what stops serving
        // every client built before the handshake existed. That is the
        // intended effect of raising it, and it is the reason raising it is a
        // decision about rebuilding images rather than an edit.
        let v = WIRE_VERSION_WITH_CLIENT_HELLO;
        assert!(admit_connection_within(v, v + 2, None).is_ok());
        let refused = admit_connection_within(v + 1, v + 2, None).unwrap_err();
        assert!(refused.contains("older"), "{refused}");
        assert!(refused.contains("rebuild its image"), "{refused}");
    }


    #[test]
    fn a_silent_connection_is_the_version_the_hello_arrived_in() {
        // The whole inertness argument: a client built the day before this
        // commit sends nothing, and must still be served.
        assert_eq!(
            admit_connection(None),
            admit_client(WIRE_VERSION_WITH_CLIENT_HELLO)
        );
        assert!(admit_connection(None).is_ok());
    }

    #[test]
    fn the_introduction_version_does_not_follow_wire_version() {
        // It is a fact about history. If it ever tracks WIRE_VERSION, every
        // silent connection looks current and the assumption means nothing —
        // so this is pinned to the literal, not to the constant.
        assert_eq!(WIRE_VERSION_WITH_CLIENT_HELLO, 43);
    }

    #[test]
    fn internal_peer_traffic_is_outside_the_client_surface() {
        // A connection-scoped refusal would reject these the moment the floor
        // moved: register_ps, heartbeats, register_node, reconcile, split,
        // merge-freeze, roll-tails. Assert the set cannot reach them.
        for m in [
            partition_rpc::MSG_SPLIT_PART,
            partition_rpc::MSG_MAINTENANCE,
            partition_rpc::MSG_MERGE_FREEZE,
            partition_rpc::MSG_ROLL_TAILS,
            partition_rpc::MSG_MERGE_PART,
            partition_rpc::MSG_GET_DISCARDS,
            partition_rpc::MSG_DIAG_TRACE_KEY,
            partition_rpc::MSG_DIAG_PARTITION_VP,
        ] {
            assert!(!is_client_surface_ps_msg(m), "ps msg {m:#x}");
        }
        for m in [
            manager_rpc::MSG_REGISTER_PS,
            manager_rpc::MSG_HEARTBEAT_PS,
            manager_rpc::MSG_REGISTER_NODE,
            manager_rpc::MSG_RECONCILE_EXTENTS,
            manager_rpc::MSG_REPORT_PARTITION_LOAD,
            manager_rpc::MSG_STREAM_ALLOC_EXTENT,
            manager_rpc::MSG_GET_AUTHZ_CONFIG,
            manager_rpc::MSG_ACQUIRE_OWNER_LOCK,
            manager_rpc::MSG_MULTI_MODIFY_SPLIT,
            manager_rpc::MSG_MULTI_MODIFY_MERGE,
        ] {
            assert!(!is_client_surface_mgr_msg(m), "mgr msg {m:#x}");
        }
    }

    #[test]
    fn the_negotiation_messages_are_exempt_from_their_own_gate() {
        // Gating the question on its own answer admits nobody.
        assert!(!is_client_surface_mgr_msg(manager_rpc::MSG_GET_CLUSTER_ID));
        assert!(!is_client_surface_mgr_msg(MSG_CLIENT_HELLO));
        assert!(!is_client_surface_ps_msg(MSG_CLIENT_HELLO));
    }

    #[test]
    fn get_regions_is_excluded_because_a_partition_server_also_sends_it() {
        // Pinned, not incidental: a PS sends no hello, so gating this refuses
        // `sync_regions_once` for the whole fleet the moment the floor rises.
        // Delete this assertion only together with a way for a cluster peer to
        // identify itself.
        assert!(!is_client_surface_mgr_msg(manager_rpc::MSG_GET_REGIONS));
    }

    #[test]
    fn the_sdk_data_plane_is_covered() {
        // Adding a data-plane message without classifying it lands it OUTSIDE
        // the window silently. This is the list the SDK's entry points emit.
        for m in [
            partition_rpc::MSG_PUT,
            partition_rpc::MSG_PUT_BULK,
            partition_rpc::MSG_DELETE,
            partition_rpc::MSG_HEAD,
            partition_rpc::MSG_RANGE,
            partition_rpc::MSG_GET_BULK,
            partition_rpc::MSG_BATCH_PUT,
            partition_rpc::MSG_BATCH_PUT_BULK,
            partition_rpc::MSG_BATCH_GET_BULK,
            partition_rpc::MSG_BATCH_DELETE,
            partition_rpc::MSG_GET_REDIRECT,
            partition_rpc::MSG_GET_REDIRECT_MANY,
            partition_rpc::MSG_AUTH_HELLO,
            partition_rpc::MSG_COMPARE_PUT,
            partition_rpc::MSG_COMPARE_WRITE,
        ] {
            assert!(is_client_surface_ps_msg(m), "ps msg {m:#x}");
        }
        for m in [
            // The NARROW routing opcode is gated where its dual-surface
            // predecessor could not be. Listed here because nothing else
            // notices if it silently leaves the set: the freeze records its
            // bytes independently, and `client_wire_admission` would go on
            // passing while a below-floor client quietly kept its routing.
            manager_rpc::MSG_GET_CLIENT_REGIONS,
            manager_rpc::MSG_MINT_TOKEN,
            manager_rpc::MSG_ALLOC_INODES,
            manager_rpc::MSG_ACQUIRE_LEASE,
            manager_rpc::MSG_RELEASE_LEASE,
            manager_rpc::MSG_HEARTBEAT_LEASE,
            manager_rpc::MSG_POLL_INVALIDATIONS,
            manager_rpc::MSG_CLUSTER_DF,
        ] {
            assert!(is_client_surface_mgr_msg(m), "mgr msg {m:#x}");
        }
    }
}
