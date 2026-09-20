//! Byte-for-byte record of how each persisted manager record encodes.
//!
//! # What a failure here MEANS, and what it does NOT mean
//!
//! A red test in this file says: **you changed a format that is already on disk
//! in a running cluster.** The response is never "update the recorded bytes".
//! It is:
//!
//! 1. bump that record's `FORMAT_VERSION` in `persist/records.rs`;
//! 2. add a step to the converter bin (`migratev<from>_v<to>`) that rewrites
//!    existing values into the new shape;
//! 3. re-record the bytes IN THE SAME COMMIT as those two, so the recorded
//!    value and the version it belongs to move together.
//!
//! Doing only step 3 leaves every stored record unreadable by the binary that
//! is supposed to read it — and because `persist::decode` verifies rather than
//! guesses, the failure lands as a manager that will not take leadership, at
//! the worst possible moment.
//!
//! # Why this file exists at all
//!
//! Until these records were split out of `manager_rpc.rs`, editing one forced a
//! `WIRE_VERSION` bump, which forced a stop-the-world deploy. That was ACCIDENTAL
//! protection — the guard came from the record sharing a file with the wire
//! schema, not from anyone deciding persisted formats deserved a guard. Moving
//! the records here removes it. Handing back a narrower but deliberate guard is
//! the point: a wire bump was far too blunt (it also stopped every PS and EN for
//! a change none of them could see), and nothing at all would be far too loose.
//!
//! # Why a byte record is legitimate here
//!
//! The same objection applies that applied to the deleted schema fingerprint:
//! false alarms teach the reflex of refreshing a recorded value, and that reflex
//! is how a real change once shipped. Two things invert it here. The fingerprint
//! hashed SOURCE, so comments and reordered imports moved it; this records the
//! ENCODING, which only a layout or type change moves. And the fingerprint
//! covered the cluster-internal wire, where an in-place edit IS the right answer
//! (bump, stop the world) — so it fired on changes whose correct response was
//! "yes, I know". On a PERSISTED record an in-place edit is never the right
//! answer, so a red here is the rule speaking, not noise.
//!
//! One shape can still breed the reflex, and it is named here rather than left
//! to be discovered under pressure: a MASS red, every record at once, from
//! rkyv's archived format moving under a dependency bump or a feature another
//! crate turns on (Cargo unifies features, and several crates depend on rkyv
//! independently). No per-record fix expresses that. The answer is the same
//! machinery at a larger size — every record's `FORMAT_VERSION` moves, one
//! converter pass rewrites everything, all of it in one commit — and NOT a
//! re-record.

use super::records::{AuditRecord, DiskRecord, NamespaceRecord, TenantAccountRecord};
use super::{decode, encode, PersistRecord};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Every field a distinct, non-default value.
///
/// Distinctness is load-bearing, not tidiness: adjacent same-typed fields
/// holding the same value make a SWAP of those two fields invisible to a byte
/// record, which is the one defect this file is least able to survive — it
/// would re-encode identically while meaning something else entirely.
fn audit_fixture() -> AuditRecord {
    AuditRecord {
        op: 0x11,
        node_id: 0x2122232425262728,
        extent_id: 0x3132333435363738,
        by: "by-field".to_string(),
        reason: "reason-field".to_string(),
        result_code: 0x41,
        result_message: "result-message-field".to_string(),
        ts_ns: 0x5152535455565758,
    }
}

fn tenant_fixture() -> TenantAccountRecord {
    TenantAccountRecord {
        tenant: "tenant-field".to_string(),
        credential_hash: {
            // Not all-equal bytes: a truncation or an off-by-one inside the
            // array has to be able to show up.
            let mut h = [0u8; 32];
            for (i, b) in h.iter_mut().enumerate() {
                *b = (i as u8).wrapping_mul(7).wrapping_add(3);
            }
            h
        },
        allowed_prefixes: vec![b"alpha/".to_vec(), b"beta/".to_vec()],
    }
}

fn namespace_fixture() -> NamespaceRecord {
    NamespaceRecord {
        name: "name-field".to_string(),
        prefix: b"prefix-field/".to_vec(),
        owner_tenant: Some("owner-tenant-field".to_string()),
        presplit: vec![b"cut-one".to_vec(), b"cut-two".to_vec()],
        created_at: 0x6162636465666768,
    }
}

fn disk_fixture() -> DiskRecord {
    DiskRecord {
        disk_id: 0x7172737475767778,
        // NOT the default: `false` is what a zeroed record reads as, so a
        // fixture carrying the default cannot show a field that stopped being
        // written at all.
        online: false,
        uuid: "uuid-field".to_string(),
    }
}

/// The recorded encodings. Read the file header before changing one.
const AUDIT_FROZEN: &str = "41554d470101726561736f6e2d6669656c64726573756c742d6d6573736167652d6669656c6411000000000000002827262524232221383736353433323162792d6669656c648c000000c0ffffff4100000094000000c0ffffff000000005857565554535251";
const TENANT_FROZEN: &str = "41554d47020174656e616e742d6669656c64616c7068612f626574612f00f4ffffff06000000f2ffffff050000008c000000d8ffffff030a11181f262d343b424950575e656c737a81888f969da4abb2b9c0c7ced5dcc8ffffff02000000";
const DISK_FROZEN: &str = "41554d470701757569642d6669656c640000000000007877767574737271000000008a000000e4ffffff00000000";
const NAMESPACE_FROZEN: &str = "41554d4703016e616d652d6669656c647072656669782d6669656c642f6f776e65722d74656e616e742d6669656c646375742d6f6e656375742d74776f00f1ffffff07000000f0ffffff070000008a000000b8ffffffbaffffff0d0000000100000092000000bbffffffd4ffffff02000000000000006867666564636261";

#[test]
fn the_persisted_encodings_are_frozen() {
    for (name, actual, frozen) in [
        ("audit", hex(&encode(&audit_fixture())), AUDIT_FROZEN),
        ("tenantAccount", hex(&encode(&tenant_fixture())), TENANT_FROZEN),
        ("namespace", hex(&encode(&namespace_fixture())), NAMESPACE_FROZEN),
        ("disk", hex(&encode(&disk_fixture())), DISK_FROZEN),
    ] {
        assert_eq!(
            actual, frozen,
            "the persisted encoding of the {name} record MOVED. Read this file's \
             header: bump its FORMAT_VERSION and add a converter step, then \
             re-record — never re-record alone.\n  actual: {actual}"
        );
    }
}

/// The recorded value is decoded back, so a mistyped digit cannot be frozen in
/// as the contract. Test 1 alone would happily pin a string that no encoder
/// produces and no decoder accepts.
#[test]
fn every_recorded_encoding_is_a_value_this_binary_can_read_back() {
    let raw = encode(&audit_fixture());
    let back: AuditRecord = decode("mgr_audit_log/x", &raw).expect("audit decodes");
    assert_eq!(back.by, "by-field");
    assert_eq!(back.result_message, "result-message-field");

    let raw = encode(&tenant_fixture());
    let back: TenantAccountRecord = decode("tenantAccount/t", &raw).expect("tenant decodes");
    assert_eq!(back.tenant, "tenant-field");
    assert_eq!(back.allowed_prefixes.len(), 2);

    let raw = encode(&namespace_fixture());
    let back: NamespaceRecord = decode("namespace/n", &raw).expect("namespace decodes");
    assert_eq!(back.owner_tenant.as_deref(), Some("owner-tenant-field"));
    assert_eq!(back.presplit.len(), 2);
}

/// The envelope is the first six bytes of every record, and the version in it
/// is the one the freeze is recorded AT. Pinned separately so that a bump
/// without a re-record fails HERE, naming the version, rather than only as an
/// opaque hex diff above.
#[test]
fn each_frozen_encoding_carries_the_version_it_was_recorded_at() {
    for (name, frozen, record_type, version) in [
        (
            "audit",
            AUDIT_FROZEN,
            AuditRecord::RECORD_TYPE,
            AuditRecord::FORMAT_VERSION,
        ),
        (
            "tenantAccount",
            TENANT_FROZEN,
            TenantAccountRecord::RECORD_TYPE,
            TenantAccountRecord::FORMAT_VERSION,
        ),
        (
            "namespace",
            NAMESPACE_FROZEN,
            NamespaceRecord::RECORD_TYPE,
            NamespaceRecord::FORMAT_VERSION,
        ),
        (
            "disk",
            DISK_FROZEN,
            DiskRecord::RECORD_TYPE,
            DiskRecord::FORMAT_VERSION,
        ),
    ] {
        assert_eq!(&frozen[0..8], "41554d47", "{name}: magic");
        assert_eq!(
            u8::from_str_radix(&frozen[8..10], 16).unwrap(),
            record_type,
            "{name}: record type"
        );
        assert_eq!(
            u8::from_str_radix(&frozen[10..12], 16).unwrap(),
            version,
            "{name}: the recorded bytes are at a different FORMAT_VERSION than \
             the record claims — a bump landed without re-recording, or the \
             reverse"
        );
    }
}


