//! One-shot converter: wrap the manager's persisted etcd records in their
//! `persist` envelope. **Run once against a STOPPED cluster, then delete this
//! file.**
//!
//! # Why this bin exists instead of compatibility code in the manager
//!
//! A persisted-format change is delivered by a converter, never by in-code
//! dual-read (`crates/manager/CLAUDE.md`, "Upgrade safety"). The servers speak
//! exactly one shape of each record; a value that is not that shape is an error
//! that refuses leadership, not something to cope with. The tool is in the tree
//! and the compatibility is not.
//!
//! # Why it does not decode anything
//!
//! Splitting a record out of the wire schema is a RENAME: the persisted struct
//! has the same field list as the wire struct it came from, and rkyv's layout
//! does not depend on the type's name — measured, `MgrExtentInfo` and an
//! identically shaped `ExtentRecord` both encode to the same 152 bytes. So this
//! conversion is a pure PREFIX INSERTION. The tool reads a value, prepends six
//! bytes, writes it back. It links no schema, knows no field, and cannot
//! mis-encode one.
//!
//! That property is specific to this conversion, and the next converter will
//! not have it for free. A change that alters a record's FIELDS has to decode
//! with the old shape and encode with the new one; it must then vendor the old
//! definition ITSELF rather than reaching for one in the tree, because by then
//! the tree only has the new one.
//!
//! # Idempotence, and its one sharp edge
//!
//! A value already carrying `[AUMG][type][version]` is left alone, so an
//! interrupted run is simply re-run. The check is six bytes, and it is fair to
//! ask whether a BARE value could begin with them — it can begin with the magic
//! (a namespace named `AUMG…` produces exactly that, measured), so the question
//! is real.
//!
//! `namespace/` cannot pass the full six: byte 0 of a bare namespace value is
//! `name[0]`, and `validate_namespace_name` allows only `[a-z0-9._-]`, never
//! `A`. The other two CAN. Nothing validates a tenant name beyond non-empty
//! (there is no `validate_tenant_name` in the tree), and an audit `reason` is
//! free text an operator types; each lands at offset 0 of its record. A tenant
//! named `AUMG\x02\x01longname` produces a bare value that passes this check —
//! measured, not hypothesised.
//!
//! It is left as a KNOWN, VISIBLE edge rather than defended against, because
//! the consequence is bounded and the detection is free:
//!
//! - **A false skip cannot become a silent misread.** rkyv pads an archive so
//!   its root sits at an aligned offset from the start of the buffer; these
//!   records archive to alignment 4 or 8, so a value that keeps its six
//!   impersonating bytes has its root at offset ≡ 2 and `rkyv_decode` rejects
//!   it with `unaligned pointer`, deterministically. Measured, and pinned by
//!   `persist::tests::a_bare_value_impersonating_the_whole_envelope_is_still_refused`.
//!   For `tenantAccount/` that lands as a manager refusing leadership and
//!   NAMING the key. For `mgr_audit_log/` it is softer and worth stating
//!   plainly: audit is never replayed, so the row is dropped from a query with
//!   a WARN — one missing audit row, after an operator typed control bytes into
//!   `--reason`.
//! - **Detection is free.** A run that converts some values and skips others
//!   prints a warning below; on a first pass everything should convert.
//!
//! Guarding it properly would mean decoding, which is the one thing this tool
//! is valuable for not doing.
//!
//! ```bash
//! cargo run --bin migratev0_v1 -- --etcd http://127.0.0.1:2379 --dry-run
//! cargo run --bin migratev0_v1 -- --etcd http://127.0.0.1:2379
//! ```

use std::process::ExitCode;

/// Must match `crates/manager/src/persist/mod.rs`. Duplicated deliberately:
/// those items are `pub(crate)` because nothing outside the manager may hold a
/// persisted record, and this tool is emphatically outside. Six bytes copied
/// here is a smaller price than widening that boundary for a file that is
/// about to be deleted.
const PERSIST_MAGIC: [u8; 4] = *b"AUMG";
const FORMAT_VERSION: u8 = 1;

/// Must match `crates/manager/src/lib.rs`. Its presence means a manager is
/// still running, which is the one situation this tool must not be used in.
const LEADER_KEY: &str = "autumn-rs/stream-manager/leader";

const RECORD_TYPE_AUDIT: u8 = 1;
const RECORD_TYPE_TENANT_ACCOUNT: u8 = 2;
const RECORD_TYPE_NAMESPACE: u8 = 3;
const RECORD_TYPE_EXTENT: u8 = 4;
const RECORD_TYPE_STREAM: u8 = 5;
const RECORD_TYPE_NODE: u8 = 6;
const RECORD_TYPE_DISK: u8 = 7;
const RECORD_TYPE_PARTITION: u8 = 8;
const RECORD_TYPE_REGION: u8 = 9;

/// Every prefix this conversion covers, with the record type its values hold.
///
/// This is ALL NINE split records. Every OTHER persisted key — `opLog/`,
/// `extent_inflight/`, `extentDeleteRetry/`, `node_override/`,
/// `decommissioned/`, `inode_leases/`, `autoPolicy/*`, `extentLayout/`,
/// `extentCorrupt/`, `ec_convert_advisory/` — is NOT enveloped and must stay
/// bare: those types were already defined inside the manager crate. Wrapping
/// one here would make the manager unable to read it.
const PREFIXES: &[(&str, u8)] = &[
    ("mgr_audit_log/", RECORD_TYPE_AUDIT),
    ("tenantAccount/", RECORD_TYPE_TENANT_ACCOUNT),
    ("namespace/", RECORD_TYPE_NAMESPACE),
    ("nodes/", RECORD_TYPE_NODE),
    ("disks/", RECORD_TYPE_DISK),
    ("extents/", RECORD_TYPE_EXTENT),
    ("streams/", RECORD_TYPE_STREAM),
    ("partitions/", RECORD_TYPE_PARTITION),
    ("regions/", RECORD_TYPE_REGION),
];

struct Counts {
    converted: usize,
    already: usize,
}

fn envelope(record_type: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(6 + body.len());
    out.extend_from_slice(&PERSIST_MAGIC);
    out.push(record_type);
    out.push(FORMAT_VERSION);
    out.extend_from_slice(body);
    out
}

fn already_enveloped(value: &[u8], record_type: u8) -> bool {
    value.len() >= 6
        && value[0..4] == PERSIST_MAGIC
        && value[4] == record_type
        && value[5] == FORMAT_VERSION
}

async fn convert_prefix(
    client: &autumn_etcd::EtcdClient,
    prefix: &str,
    record_type: u8,
    dry_run: bool,
) -> Result<Counts, String> {
    let resp = client
        .get_prefix(prefix)
        .await
        .map_err(|e| format!("get_prefix {prefix}: {e}"))?;
    let mut counts = Counts {
        converted: 0,
        already: 0,
    };
    for kv in &resp.kvs {
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        if already_enveloped(&kv.value, record_type) {
            counts.already += 1;
            continue;
        }
        if !dry_run {
            let wrapped = envelope(record_type, &kv.value);
            client
                .put(&kv.key, &wrapped)
                .await
                .map_err(|e| format!("put {key}: {e}"))?;
        }
        counts.converted += 1;
    }
    Ok(counts)
}

#[compio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let mut endpoints = String::new();
    let mut dry_run = false;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--etcd" => {
                i += 1;
                endpoints = args.get(i).cloned().unwrap_or_default();
            }
            "--dry-run" => dry_run = true,
            other => {
                eprintln!("unknown argument: {other}");
                eprintln!("usage: migratev0_v1 --etcd <http://host:2379[,…]> [--dry-run]");
                return ExitCode::FAILURE;
            }
        }
        i += 1;
    }
    if endpoints.is_empty() {
        eprintln!("usage: migratev0_v1 --etcd <http://host:2379[,…]> [--dry-run]");
        return ExitCode::FAILURE;
    }

    let list: Vec<String> = endpoints.split(',').map(|s| s.trim().to_string()).collect();
    let client = match autumn_etcd::EtcdClient::connect_many(&list).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("connect etcd {endpoints}: {e}");
            return ExitCode::FAILURE;
        }
    };

    // The documented precondition is a STOPPED cluster, and the whole risk of
    // this tool is being run against a live one: reads and writes are separate
    // round trips with no CAS, so a leader committing between them would have
    // its write silently overwritten. Test the precondition rather than paper
    // over its violation — a leader key present means somebody is still up.
    // (A NEW manager cannot be that leader: it refuses to lead on un-converted
    // data. An OLD one can.)
    match client.get(LEADER_KEY.as_bytes()).await {
        Ok(resp) if !resp.kvs.is_empty() => {
            let who = String::from_utf8_lossy(&resp.kvs[0].value).into_owned();
            eprintln!(
                "REFUSING: a manager still holds the leader key \
                 ({LEADER_KEY} = {who}). This converter reads and writes \
                 without CAS, so a live leader's write can be silently \
                 overwritten. Stop every manager, then re-run."
            );
            return ExitCode::FAILURE;
        }
        Ok(_) => {}
        Err(e) => {
            eprintln!("could not read {LEADER_KEY} to check the cluster is stopped: {e}");
            return ExitCode::FAILURE;
        }
    }

    if dry_run {
        println!("DRY RUN — nothing is written");
    }
    let mut total = Counts {
        converted: 0,
        already: 0,
    };
    for (prefix, record_type) in PREFIXES {
        match convert_prefix(&client, prefix, *record_type, dry_run).await {
            Ok(c) => {
                println!(
                    "{prefix:<16} converted={:<6} already={:<6}",
                    c.converted, c.already
                );
                total.converted += c.converted;
                total.already += c.already;
            }
            Err(e) => {
                // Stop on the first failure rather than limping on: a partial
                // pass is safe to re-run, a pass that hid an error is not.
                eprintln!("FAILED on {prefix}: {e}");
                eprintln!("nothing further was written; fix the cause and re-run (idempotent)");
                return ExitCode::FAILURE;
            }
        }
    }
    println!(
        "total converted={} already={}",
        total.converted, total.already
    );
    if total.converted == 0 && total.already > 0 {
        println!("everything was already converted — this looks like a re-run");
    } else if total.converted > 0 && total.already > 0 {
        // Mixed. Usually an interrupted run, which is fine. But it is also the
        // only symptom of the tenant-name edge in this file's header — a value
        // whose own bytes impersonate the envelope — so it is worth a look
        // rather than a silent pass.
        println!(
            "WARNING: {} values converted but {} were skipped as already done. \
             That is normal after an interrupted run. If this was a FIRST run, \
             check the skipped keys: a value can impersonate the envelope (see \
             this tool's header) and would then stay bare, which the manager \
             will refuse at replay.",
            total.converted, total.already
        );
    }
    if total.converted == 0 && total.already == 0 {
        println!("no records found at all — check --etcd points at the right cluster");
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bare_value_is_wrapped_without_touching_its_bytes() {
        let bare = vec![0xDEu8, 0xAD, 0xBE, 0xEF];
        let wrapped = envelope(RECORD_TYPE_NAMESPACE, &bare);
        assert_eq!(&wrapped[0..4], b"AUMG");
        assert_eq!(wrapped[4], RECORD_TYPE_NAMESPACE);
        assert_eq!(wrapped[5], FORMAT_VERSION);
        assert_eq!(
            &wrapped[6..],
            &bare[..],
            "the body must survive byte for byte — this tool re-encodes nothing"
        );
    }

    #[test]
    fn wrapping_is_idempotent_per_record_type() {
        let bare = vec![1u8, 2, 3];
        let once = envelope(RECORD_TYPE_AUDIT, &bare);
        assert!(already_enveloped(&once, RECORD_TYPE_AUDIT));
        assert!(
            !already_enveloped(&once, RECORD_TYPE_NAMESPACE),
            "an audit record under a namespace key must NOT read as already done"
        );
    }

    /// The measured collision, guarded: a bare value CAN begin with the magic.
    /// It must still be converted, because the bytes after the magic cannot
    /// form this record's type and version.
    #[test]
    fn a_bare_value_beginning_with_the_magic_is_still_converted() {
        let bare = b"AUMGnamespace-with-a-long-name".to_vec();
        assert_eq!(&bare[0..4], &PERSIST_MAGIC);
        assert!(
            !already_enveloped(&bare, RECORD_TYPE_NAMESPACE),
            "byte 4 is 'n', not a record type; the full six-byte check is what \
             keeps the magic alone from being mistaken for an envelope"
        );
    }
}
