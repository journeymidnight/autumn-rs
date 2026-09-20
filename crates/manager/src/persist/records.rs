//! The persisted manager records themselves.
//!
//! Each is `pub(crate)`: "only the manager may reference a persisted record" is
//! the rule, and this is the compiler enforcing it rather than a comment asking
//! nicely.
//!
//! # Conversions are EXHAUSTIVE, and that is the mechanism
//!
//! Every conversion to or from a wire type destructures the source fully — no
//! `..`, no field-by-field `src.foo` access that silently ignores a new
//! neighbour. Add a field to either side and the conversion stops compiling.
//!
//! This is what makes having two definitions safe here when `ExtDfReq` taught
//! the opposite lesson. That incident was not "two definitions exist"; it was
//! **two definitions with nothing deciding which one is read** — the manager
//! encoded through its copy while the node decoded through `extent_rpc`'s, and
//! a field added to one side was a silent rkyv mis-decode. `extent_rpc`'s
//! `one_definition_only!` makes reintroducing THAT a build error, and it still
//! should. Here the discriminator is the conversion function itself: the
//! persisted form is reachable only through `persist::decode`, the wire form
//! only through the wire codec, and the only bridge between them is code the
//! compiler forces you to update.

use autumn_rpc::manager_rpc::{MgrAuditEntry, MgrDiskInfo, MgrNamespace};
use rkyv::{Archive, Deserialize, Serialize};

use super::{
    PersistRecord, RECORD_TYPE_AUDIT, RECORD_TYPE_DISK, RECORD_TYPE_NAMESPACE,
    RECORD_TYPE_TENANT_ACCOUNT,
};

// ── audit log ───────────────────────────────────────────────────────────────

/// `mgr_audit_log/<ts_ns>_<seq>` — who asked for what, and how it came out.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub(crate) struct AuditRecord {
    /// `AUDIT_OP_*` on the wire side.
    pub op: u8,
    pub node_id: u64,
    pub extent_id: u64,
    pub by: String,
    pub reason: String,
    /// 0 = OK; non-zero = `CODE_*` failure.
    pub result_code: u8,
    pub result_message: String,
    /// Unix-epoch nanoseconds; also the key's sort prefix.
    pub ts_ns: u64,
}

impl PersistRecord for AuditRecord {
    const RECORD_TYPE: u8 = RECORD_TYPE_AUDIT;
    const FORMAT_VERSION: u8 = 1;
    const NAME: &'static str = "audit";
}

impl From<&MgrAuditEntry> for AuditRecord {
    fn from(e: &MgrAuditEntry) -> Self {
        let MgrAuditEntry {
            op,
            node_id,
            extent_id,
            by,
            reason,
            result_code,
            result_message,
            ts_ns,
        } = e;
        Self {
            op: *op,
            node_id: *node_id,
            extent_id: *extent_id,
            by: by.clone(),
            reason: reason.clone(),
            result_code: *result_code,
            result_message: result_message.clone(),
            ts_ns: *ts_ns,
        }
    }
}

impl From<&AuditRecord> for MgrAuditEntry {
    fn from(r: &AuditRecord) -> Self {
        let AuditRecord {
            op,
            node_id,
            extent_id,
            by,
            reason,
            result_code,
            result_message,
            ts_ns,
        } = r;
        Self {
            op: *op,
            node_id: *node_id,
            extent_id: *extent_id,
            by: by.clone(),
            reason: reason.clone(),
            result_code: *result_code,
            result_message: result_message.clone(),
            ts_ns: *ts_ns,
        }
    }
}

// ── tenant accounts ─────────────────────────────────────────────────────────

/// `tenantAccount/<tenant>` — the authz principal DB.
///
/// This record has **no wire twin at all**, and that is deliberate rather than
/// an omission: `PrincipalListResp` carries a `PrincipalRow` precisely so that
/// an inspection RPC can never hand out `credential_hash`, the verifier for a
/// credential. So there is no conversion below — nothing outside the manager
/// has any business holding one of these.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub(crate) struct TenantAccountRecord {
    pub tenant: String,
    /// SHA-256 of the tenant's permanent credential. Verified constant-time at
    /// mint time; the raw credential is never stored.
    pub credential_hash: [u8; 32],
    /// Key prefixes this tenant may access. Each MUST end with `b'/'`.
    pub allowed_prefixes: Vec<Vec<u8>>,
}

impl PersistRecord for TenantAccountRecord {
    const RECORD_TYPE: u8 = RECORD_TYPE_TENANT_ACCOUNT;
    const FORMAT_VERSION: u8 = 1;
    const NAME: &'static str = "tenantAccount";
}

// ── namespace registry ──────────────────────────────────────────────────────

/// `namespace/<name>` — the registry, and the operator-declared presplit cuts.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub(crate) struct NamespaceRecord {
    pub name: String,
    pub prefix: Vec<u8>,
    /// `None` = existence-only (the built-in families `fs` / `kvc` / `mem`).
    pub owner_tenant: Option<String>,
    /// Declared split points — the sacred boundaries a merge may not cross.
    pub presplit: Vec<Vec<u8>>,
    pub created_at: i64,
}

impl PersistRecord for NamespaceRecord {
    const RECORD_TYPE: u8 = RECORD_TYPE_NAMESPACE;
    const FORMAT_VERSION: u8 = 1;
    const NAME: &'static str = "namespace";
}

impl From<&MgrNamespace> for NamespaceRecord {
    fn from(n: &MgrNamespace) -> Self {
        let MgrNamespace {
            name,
            prefix,
            owner_tenant,
            presplit,
            created_at,
        } = n;
        Self {
            name: name.clone(),
            prefix: prefix.clone(),
            owner_tenant: owner_tenant.clone(),
            presplit: presplit.clone(),
            created_at: *created_at,
        }
    }
}

impl From<&NamespaceRecord> for MgrNamespace {
    fn from(r: &NamespaceRecord) -> Self {
        let NamespaceRecord {
            name,
            prefix,
            owner_tenant,
            presplit,
            created_at,
        } = r;
        Self {
            name: name.clone(),
            prefix: prefix.clone(),
            owner_tenant: owner_tenant.clone(),
            presplit: presplit.clone(),
            created_at: *created_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_audit_conversion_carries_every_field_both_ways() {
        let wire = MgrAuditEntry {
            op: 5,
            node_id: 42,
            extent_id: 4242,
            by: "admin".to_string(),
            reason: "drain".to_string(),
            result_code: 7,
            result_message: "refused".to_string(),
            ts_ns: 1_700_000_000_000_000_001,
        };
        let back: MgrAuditEntry = (&AuditRecord::from(&wire)).into();
        // Field by field rather than a derived PartialEq: a missing `PartialEq`
        // would make an `assert_eq!` impossible to write, and adding one just
        // for the test would hide a field that the CONVERSION dropped but the
        // comparison also ignored.
        assert_eq!(back.op, 5);
        assert_eq!(back.node_id, 42);
        assert_eq!(back.extent_id, 4242);
        assert_eq!(back.by, "admin");
        assert_eq!(back.reason, "drain");
        assert_eq!(back.result_code, 7);
        assert_eq!(back.result_message, "refused");
        assert_eq!(back.ts_ns, 1_700_000_000_000_000_001);
    }

    #[test]
    fn the_namespace_conversion_carries_every_field_both_ways() {
        let wire = MgrNamespace {
            name: "kvc".to_string(),
            prefix: b"kvc/".to_vec(),
            owner_tenant: Some("t1".to_string()),
            presplit: vec![b"kvc/a".to_vec(), b"kvc/b".to_vec()],
            created_at: 1_700_000_000,
        };
        let back: MgrNamespace = (&NamespaceRecord::from(&wire)).into();
        assert_eq!(back.name, "kvc");
        assert_eq!(back.prefix, b"kvc/");
        assert_eq!(back.owner_tenant.as_deref(), Some("t1"));
        assert_eq!(back.presplit, vec![b"kvc/a".to_vec(), b"kvc/b".to_vec()]);
        assert_eq!(back.created_at, 1_700_000_000);
    }
}

// ── disks ───────────────────────────────────────────────────────────────────

/// `disks/<id>` — one disk on one extent node.
///
/// `online` is the manager's own bookkeeping, NOT the node's per-disk health
/// verdict: it carries three different meanings (the node said faulted, the
/// node did not answer `df` at all, a quorum of partition servers reported the
/// node). Recovery keys on `faulted_disks`, which is in-memory and leader-local
/// — see the recovery section of this crate's guide.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub(crate) struct DiskRecord {
    pub disk_id: u64,
    pub online: bool,
    pub uuid: String,
}

impl PersistRecord for DiskRecord {
    const RECORD_TYPE: u8 = RECORD_TYPE_DISK;
    const FORMAT_VERSION: u8 = 1;
    const NAME: &'static str = "disk";
}

impl From<&MgrDiskInfo> for DiskRecord {
    fn from(d: &MgrDiskInfo) -> Self {
        let MgrDiskInfo {
            disk_id,
            online,
            uuid,
        } = d;
        Self {
            disk_id: *disk_id,
            online: *online,
            uuid: uuid.clone(),
        }
    }
}

impl From<&DiskRecord> for MgrDiskInfo {
    fn from(r: &DiskRecord) -> Self {
        let DiskRecord {
            disk_id,
            online,
            uuid,
        } = r;
        Self {
            disk_id: *disk_id,
            online: *online,
            uuid: uuid.clone(),
        }
    }
}
