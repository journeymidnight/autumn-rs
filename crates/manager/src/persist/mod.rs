//! The manager's own persisted records, and the envelope that versions them.
//!
//! # Why this module exists
//!
//! Five schemas in this system, each answering "who is writing to whom":
//! SST/WAL/checkpoint (a PS to a future PS), `.meta`/`.ck` (an EN to a future
//! EN), manager records (a manager to a future manager), the cluster-internal
//! wire (live peers), the client wire (live clients). Four of them carry their
//! own version — `AU7B` + `FORMAT_VERSION`, `EXTMETA\x02`, `WIRE_VERSION`
//! exact equality, the client window. The manager's records did not: they were
//! defined in `crates/rpc/src/manager_rpc.rs` under the wire-schema banner, so
//! changing what the MANAGER remembers meant bumping the version every PS, EN
//! and embedded client is judged by. Measured: of 44 wire-version intervals
//! only 8 (18%) genuinely needed manager + PS + EN to move together, while the
//! extent node — the expensive one to restart, since it re-scans every extent
//! file — could have stayed up for 30 of them (68%). That difference is what
//! borrowing the wire version costs.
//!
//! So a persisted record lives HERE, is `pub(crate)` (the "only the manager may
//! reference it" rule, enforced by the compiler rather than by discipline), and
//! converts to and from its wire form explicitly.
//!
//! # The envelope
//!
//! ```text
//! [magic b"AUMG": 4][record_type: u8][format_version: u8][rkyv bytes …]
//! ```
//!
//! `record_type` catches a key/value mix-up — an extent record written under a
//! namespace key decodes as garbage otherwise, and rkyv will happily do that.
//! `format_version` is PER RECORD, which is the whole point: adding a field to
//! the namespace record moves that record's number and nothing else.
//!
//! # Decoding VERIFIES; it never sniffs, and there is no fallback
//!
//! A value that does not carry the expected envelope is an ERROR — replay fails
//! and the manager refuses leadership — never "maybe it is the older form".
//! Two reasons, and the first is measured rather than argued:
//!
//! - **Sniffing provably cannot work here.** rkyv puts its archived root at the
//!   END of the buffer, so a persisted value BEGINS with variable-length
//!   business content. A namespace named `AUMG…` encodes to a value whose first
//!   four bytes are literally `41 55 4d 47`. Any "does it start with our magic"
//!   test misreads that record. Leaning on an input validator to forbid such a
//!   name would make this format's safety depend on an unrelated check that
//!   someone may later relax — the exact hidden coupling this module removes.
//! - **In-code compatibility does not stay small.** It means every persisted
//!   type keeps every shape it has ever had, each reachable, each needing a
//!   test, forever.
//!
//! # How a format change is delivered
//!
//! Bump that record's `FORMAT_VERSION` and add a step to a converter bin named
//! `migratev<from>_v<to>`, run ONCE by hand against a STOPPED cluster and then
//! deleted. The tool is in the tree; the compatibility is not. See
//! `crates/manager/CLAUDE.md`, "Upgrade safety".
//!
//! A stored version that is not this binary's is therefore never something to
//! cope with — it means the converter has not run (lower) or a newer binary
//! wrote it and was rolled back (higher). Both refuse, and the message names
//! the tool.

pub(crate) mod records;

#[cfg(test)]
mod freeze;

use autumn_rpc::manager_rpc::{rkyv_decode, rkyv_encode};
use rkyv::api::high::{HighDeserializer, HighSerializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::Error as RkyvError;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

/// Marks this as an Autumn ManaGer record. Four bytes so a mix-up with a bare
/// rkyv value is not a coin flip.
pub(crate) const PERSIST_MAGIC: [u8; 4] = *b"AUMG";

/// `[magic 4][record_type 1][format_version 1]`.
pub(crate) const PERSIST_HEADER_LEN: usize = 6;

/// One persisted manager record.
///
/// `RECORD_TYPE` values are **FROZEN and APPEND-ONLY** — they are written into
/// every stored value, so renumbering one silently re-labels every record
/// already on disk. Add a new record by taking the next free number; never
/// reuse a retired one.
pub(crate) trait PersistRecord {
    /// Stable, frozen. See the warning above.
    const RECORD_TYPE: u8;
    /// This record's OWN version. Moves only when this record's layout or the
    /// meaning of one of its fields changes, and only together with a converter
    /// step. Independent of `WIRE_VERSION`.
    const FORMAT_VERSION: u8;
    /// For error messages, so a refusal names the record a human recognises.
    const NAME: &'static str;
}

// ── record_type registry (frozen, append-only) ──────────────────────────────
//
// The three records this module currently owns. The remaining six persisted
// types (extents, streams, nodes, disks, partitions, regions) keep their
// numbers reserved here so that splitting them later cannot collide with a
// number already written to disk.
pub(crate) const RECORD_TYPE_AUDIT: u8 = 1;
pub(crate) const RECORD_TYPE_TENANT_ACCOUNT: u8 = 2;
pub(crate) const RECORD_TYPE_NAMESPACE: u8 = 3;
// RESERVED, not yet split out of the wire schema:
//   4 = extent, 5 = stream, 6 = node, 7 = disk, 8 = partition, 9 = region.

/// Wrap a record in its envelope. The body is the same rkyv codec the rest of
/// the manager uses — the envelope is what this module adds, not a new
/// serializer.
pub(crate) fn encode<T>(record: &T) -> Vec<u8>
where
    T: PersistRecord + for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
{
    let body = rkyv_encode(record);
    let mut out = Vec::with_capacity(PERSIST_HEADER_LEN + body.len());
    out.extend_from_slice(&PERSIST_MAGIC);
    out.push(T::RECORD_TYPE);
    out.push(T::FORMAT_VERSION);
    out.extend_from_slice(&body);
    out
}

/// Read a record back, verifying the envelope first.
///
/// Every failure is fatal to replay by design; the caller turns it into a
/// refusal to lead. The messages are written for whoever is holding a stopped
/// cluster at 3am, so each one says which record, which key, and what to run.
pub(crate) fn decode<T>(key: &str, raw: &[u8]) -> Result<T, String>
where
    T: PersistRecord + Archive,
    T::Archived: Deserialize<T, HighDeserializer<RkyvError>>
        + for<'a> CheckBytes<HighValidator<'a, RkyvError>>,
{
    if raw.len() < PERSIST_HEADER_LEN {
        return Err(format!(
            "{key}: {} bytes is too short to be a persisted {} record",
            raw.len(),
            T::NAME
        ));
    }
    if raw[0..4] != PERSIST_MAGIC {
        return Err(format!(
            "{key}: not a persisted manager record (no {} envelope). This is what \
             an un-migrated value written before the records were split out of the \
             wire schema looks like — run `migratev0_v1` against the STOPPED \
             cluster before starting this binary",
            String::from_utf8_lossy(&PERSIST_MAGIC)
        ));
    }
    let record_type = raw[4];
    if record_type != T::RECORD_TYPE {
        return Err(format!(
            "{key}: holds record type {record_type}, but this key must hold {} \
             (type {}). A record is under the wrong key — decoding it would read \
             one record's bytes as another's",
            T::NAME,
            T::RECORD_TYPE
        ));
    }
    let format_version = raw[5];
    if format_version != T::FORMAT_VERSION {
        return Err(format!(
            "{key}: {} record is at format version {format_version}, this binary \
             speaks {}. {}",
            T::NAME,
            T::FORMAT_VERSION,
            if format_version < T::FORMAT_VERSION {
                "The converter has not run — run the migratev<from>_v<to> bin \
                 against the STOPPED cluster"
            } else {
                "A NEWER binary wrote this record and was then rolled back. This \
                 binary cannot read it; go forward to the one that wrote it"
            }
        ));
    }
    rkyv_decode(&raw[PERSIST_HEADER_LEN..]).map_err(|e| {
        format!(
            "{key}: {} record carries a correct envelope but its body does not \
             decode: {e}",
            T::NAME
        )
    })
}

#[cfg(test)]
mod tests {
    use super::records::{AuditRecord, NamespaceRecord, TenantAccountRecord};
    use super::*;

    fn audit() -> AuditRecord {
        AuditRecord {
            op: 3,
            node_id: 7,
            extent_id: 9,
            by: "operator".to_string(),
            reason: "because".to_string(),
            result_code: 0,
            result_message: "ok".to_string(),
            ts_ns: 1_700_000_000_000_000_000,
        }
    }

    #[test]
    fn a_record_round_trips_through_its_envelope() {
        let raw = encode(&audit());
        assert_eq!(&raw[0..4], b"AUMG");
        assert_eq!(raw[4], RECORD_TYPE_AUDIT);
        assert_eq!(raw[5], AuditRecord::FORMAT_VERSION);
        let back: AuditRecord = decode("mgr_audit_log/x", &raw).expect("round trip");
        assert_eq!(back.by, "operator");
        assert_eq!(back.ts_ns, 1_700_000_000_000_000_000);
    }

    /// The case the whole module is built around: a value written before the
    /// records were split out of the wire schema. It must REFUSE, and the
    /// refusal must send the reader to the converter — not fall back to a bare
    /// decode, which is the silent-misread path.
    #[test]
    fn a_bare_unmigrated_value_is_refused_and_names_the_converter() {
        let bare = autumn_rpc::manager_rpc::rkyv_encode(&audit()).to_vec();
        let err = decode::<AuditRecord>("mgr_audit_log/x", &bare)
            .expect_err("a bare value carries no envelope");
        assert!(err.contains("no AUMG envelope"), "{err}");
        assert!(
            err.contains("migratev0_v1"),
            "names the BIN to run, not just the idea of one — the no-envelope \
             state is by definition v0, so this name cannot go stale: {err}"
        );
    }

    /// Sniffing cannot save us, and this is the fixture that proves it: a
    /// namespace legitimately named `AUMG…` produces a BARE value whose first
    /// four bytes ARE the magic. Anything that treated a leading magic as
    /// "already migrated" would read this record's own name as its header.
    #[test]
    fn a_bare_value_can_begin_with_the_magic_itself() {
        let ns = NamespaceRecord {
            name: "AUMGnamespace".to_string(),
            prefix: b"AUMGnamespace/".to_vec(),
            owner_tenant: None,
            presplit: vec![],
            created_at: 1,
        };
        let bare = autumn_rpc::manager_rpc::rkyv_encode(&ns).to_vec();
        assert_eq!(
            &bare[0..4],
            &PERSIST_MAGIC,
            "this is the measured collision: a bare value starting with the magic"
        );
        assert!(decode::<NamespaceRecord>("namespace/AUMGnamespace", &bare).is_err());
    }

    /// A bare value can impersonate the WHOLE six-byte envelope, not just the
    /// magic — and when it does, the converter skips it and this decoder is the
    /// only thing left. It must refuse.
    ///
    /// Reachable because two of the three records have an unvalidated free-text
    /// field at offset 0: a tenant name (nothing validates one — there is no
    /// `validate_tenant_name` in the tree) and an audit `reason`. A namespace
    /// cannot do it: `validate_namespace_name` allows only `[a-z0-9._-]`, so
    /// byte 0 is never `A`.
    ///
    /// The refusal is STRUCTURAL rather than lucky, which is worth knowing
    /// before anyone tries to "improve" the converter into guessing. rkyv pads
    /// an archive so its root sits at an aligned offset from the START of the
    /// buffer; these records archive to alignment 4 or 8, so stripping a
    /// six-byte header always leaves the root at offset ≡ 2, and `from_bytes`
    /// rejects it deterministically with `unaligned pointer`. Measured. The one
    /// way to weaken it would be a future record whose archived alignment is
    /// 1 or 2 — nothing here is, and a record that was would need its own
    /// argument.
    #[test]
    fn a_bare_value_impersonating_the_whole_envelope_is_still_refused() {
        let acct = TenantAccountRecord {
            tenant: "AUMG\u{2}\u{1}longname".to_string(),
            credential_hash: [7u8; 32],
            allowed_prefixes: vec![b"fs/".to_vec()],
        };
        let bare = autumn_rpc::manager_rpc::rkyv_encode(&acct).to_vec();
        assert_eq!(&bare[0..4], &PERSIST_MAGIC);
        assert_eq!(bare[4], TenantAccountRecord::RECORD_TYPE);
        assert_eq!(
            bare[5],
            TenantAccountRecord::FORMAT_VERSION,
            "this fixture exists because the full six-byte check CAN be passed              by a bare value's own content"
        );
        let err = decode::<TenantAccountRecord>("tenantAccount/x", &bare)
            .expect_err("a skipped-as-converted bare value must not decode");
        assert!(
            err.contains("does not decode"),
            "it gets past the envelope checks and must be caught by the body: {err}"
        );

        let entry = AuditRecord {
            reason: "AUMG\u{1}\u{1}operator-typed-this".to_string(),
            ..audit()
        };
        let bare = autumn_rpc::manager_rpc::rkyv_encode(&entry).to_vec();
        assert_eq!(&bare[0..4], &PERSIST_MAGIC);
        assert_eq!(bare[4], AuditRecord::RECORD_TYPE);
        assert_eq!(bare[5], AuditRecord::FORMAT_VERSION);
        assert!(decode::<AuditRecord>("mgr_audit_log/x", &bare).is_err());
    }

    #[test]
    fn a_record_under_the_wrong_key_is_refused_by_type() {
        let raw = encode(&audit());
        let err = decode::<TenantAccountRecord>("tenantAccount/t1", &raw)
            .expect_err("an audit record is not a tenant account");
        assert!(err.contains("record type"), "{err}");
    }

    #[test]
    fn a_version_this_binary_does_not_speak_is_refused_in_both_directions() {
        let mut older = encode(&audit());
        older[5] = AuditRecord::FORMAT_VERSION - 1;
        let err = decode::<AuditRecord>("mgr_audit_log/x", &older).expect_err("older");
        assert!(err.contains("converter has not run"), "{err}");

        let mut newer = encode(&audit());
        newer[5] = AuditRecord::FORMAT_VERSION + 1;
        let err = decode::<AuditRecord>("mgr_audit_log/x", &newer).expect_err("newer");
        assert!(err.contains("rolled back"), "{err}");
    }

    /// The REVERSE direction, which the acceptance row asks to be decided and
    /// pinned rather than left to chance: an OLD binary — one that decodes the
    /// whole etcd value as bare rkyv, because it predates the envelope — meets
    /// a value this build wrote. **The answer is REFUSE**, and this pins that
    /// it is refuse and not "reads it and gets something".
    ///
    /// It is worth knowing WHY, because the obvious guess is wrong. rkyv locates
    /// its root relative to the buffer's END, so prepending six bytes moves the
    /// length and the root together and the root IS found — the envelope does
    /// not hide it. What rejects the value is alignment: the whole archive is
    /// displaced by 6, these records' roots align to 4 or 8, and `from_bytes`
    /// answers `unaligned pointer`.
    ///
    /// So the refusal is STRUCTURAL but INCIDENTAL — it falls out of rkyv's
    /// layout rules rather than from anything here defending against it. That
    /// is fine for the guarantee we need (the upgrade is stop → convert →
    /// start, and going backwards is unsupported), but a future record whose
    /// archived alignment is 1 or 2 would not get it, and this test would go
    /// green while meaning something weaker. Do not read a pass here as "old
    /// binaries are safely locked out by design".
    #[test]
    fn an_old_bare_decoder_refuses_a_value_this_build_wrote() {
        use autumn_rpc::manager_rpc::{rkyv_decode, MgrNamespace};

        let record = NamespaceRecord {
            name: "kvc".to_string(),
            prefix: b"kvc/".to_vec(),
            owner_tenant: Some("t1".to_string()),
            presplit: vec![b"kvc/a".to_vec()],
            created_at: 99,
        };
        let stored = encode(&record);

        // Exactly what a pre-envelope manager does with the stored bytes.
        let as_old_binary = rkyv_decode::<MgrNamespace>(&stored);
        assert!(
            as_old_binary.is_err(),
            "an old binary must REFUSE a value this build wrote, not decode one"
        );

        // And this build still reads its own, so the test cannot pass merely
        // because the fixture is malformed.
        let back: NamespaceRecord = decode("namespace/kvc", &stored).expect("we read our own");
        assert_eq!(back.created_at, 99);
    }

    /// `RECORD_TYPE` is written into every stored value, so these numbers are
    /// frozen. Pinned BY NAME: a consistent renumbering is invisible to a round
    /// trip, and would silently re-label every record already on disk.
    #[test]
    fn the_record_type_numbers_are_frozen() {
        assert_eq!(AuditRecord::RECORD_TYPE, 1);
        assert_eq!(TenantAccountRecord::RECORD_TYPE, 2);
        assert_eq!(NamespaceRecord::RECORD_TYPE, 3);
    }
}
