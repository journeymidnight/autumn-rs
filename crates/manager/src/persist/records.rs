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

use autumn_rpc::manager_rpc::{
    ClientRegion, MgrAuditEntry, MgrDiskInfo, MgrExtentInfo, MgrNamespace, MgrNodeInfo,
    MgrPartitionMeta, MgrRange, MgrRegionInfo, MgrStreamInfo,
};
use rkyv::{Archive, Deserialize, Serialize};

use super::{
    PersistRecord, RECORD_TYPE_AUDIT, RECORD_TYPE_DISK, RECORD_TYPE_NAMESPACE,
    RECORD_TYPE_EXTENT, RECORD_TYPE_NODE, RECORD_TYPE_PARTITION, RECORD_TYPE_REGION,
    RECORD_TYPE_STREAM, RECORD_TYPE_TENANT_ACCOUNT,
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

    /// The six records split in the second round, round-tripped field by field.
    ///
    /// The exhaustive destructure catches a DROPPED field — it stops compiling.
    /// It cannot catch a SWAP between two fields of the same type, and these
    /// records are full of them: a partition and a region each carry three
    /// adjacent `u64` stream ids, an extent carries two `bool`s and four
    /// `Vec<u64>`s. `log_stream: *row_stream` compiles, encodes to a valid
    /// record, and is silently wrong. The freeze test cannot see it either —
    /// it records what the ENCODER produces, and a swapped conversion never
    /// reaches the encoder. Only a value-distinct round trip does.
    #[test]
    fn the_second_round_conversions_carry_every_field_both_ways() {
        let part = MgrPartitionMeta {
            part_id: 9,
            // Distinct, so a swap moves an assertion.
            log_stream: 11,
            row_stream: 22,
            meta_stream: 33,
            rg: Some(MgrRange {
                start_key: b"aa".to_vec(),
                end_key: b"bb".to_vec(),
            }),
        };
        let back: MgrPartitionMeta = (&PartitionRecord::from(&part)).into();
        assert_eq!(back.part_id, 9);
        assert_eq!(back.log_stream, 11);
        assert_eq!(back.row_stream, 22);
        assert_eq!(back.meta_stream, 33);
        let rg = back.rg.expect("range survives");
        assert_eq!(rg.start_key, b"aa");
        assert_eq!(rg.end_key, b"bb");

        let region = MgrRegionInfo {
            rg: Some(MgrRange {
                start_key: b"cc".to_vec(),
                end_key: b"dd".to_vec(),
            }),
            part_id: 1,
            ps_id: 2,
            log_stream: 3,
            row_stream: 4,
            meta_stream: 5,
            region_epoch: 6,
        };
        let back: MgrRegionInfo = (&RegionRecord::from(&region)).into();
        assert_eq!(
            (
                back.part_id,
                back.ps_id,
                back.log_stream,
                back.row_stream,
                back.meta_stream,
                back.region_epoch
            ),
            (1, 2, 3, 4, 5, 6)
        );
        let rg = back.rg.expect("range survives");
        assert_eq!(rg.start_key, b"cc");
        assert_eq!(rg.end_key, b"dd");

        let stream = MgrStreamInfo {
            stream_id: 7,
            extent_ids: vec![1, 2, 3],
            // Different from each other AND from `replicates`.
            ec_data_shard: 4,
            ec_parity_shard: 2,
            replicates: 3,
        };
        let back: MgrStreamInfo = (&StreamRecord::from(&stream)).into();
        assert_eq!(back.stream_id, 7);
        assert_eq!(back.extent_ids, vec![1, 2, 3]);
        assert_eq!(back.ec_data_shard, 4);
        assert_eq!(back.ec_parity_shard, 2);
        assert_eq!(back.replicates, 3);

        let node = MgrNodeInfo {
            node_id: 8,
            address: "addr".to_string(),
            disks: vec![1, 2],
            shard_ports: vec![9],
            // Distinct strings: two String fields side by side swap invisibly
            // if they hold the same text.
            control_address: "ctrl".to_string(),
            node_uuid: "uuid".to_string(),
        };
        let back: MgrNodeInfo = (&NodeRecord::from(&node)).into();
        assert_eq!(back.node_id, 8);
        assert_eq!(back.address, "addr");
        assert_eq!(back.disks, vec![1, 2]);
        assert_eq!(back.shard_ports, vec![9]);
        assert_eq!(back.control_address, "ctrl");
        assert_eq!(back.node_uuid, "uuid");

        let disk = MgrDiskInfo {
            disk_id: 5,
            online: false,
            uuid: "duuid".to_string(),
        };
        let back: MgrDiskInfo = (&DiskRecord::from(&disk)).into();
        assert_eq!(back.disk_id, 5);
        assert!(!back.online);
        assert_eq!(back.uuid, "duuid");

        let extent = MgrExtentInfo {
            extent_id: 10,
            // All four vectors distinct in content.
            replicates: vec![1, 2],
            parity: vec![3],
            eversion: 4,
            refs: 5,
            vp_table_refs: 6,
            sealed_length: 7,
            // Opposite bools: equal ones hide a swap.
            sealed: true,
            avali: 8,
            replicate_disks: vec![9, 10],
            parity_disks: vec![11],
            ec_converted: false,
        };
        let back: MgrExtentInfo = (&ExtentRecord::from(&extent)).into();
        assert_eq!(back.extent_id, 10);
        assert_eq!(back.replicates, vec![1, 2]);
        assert_eq!(back.parity, vec![3]);
        assert_eq!((back.eversion, back.refs, back.vp_table_refs), (4, 5, 6));
        assert_eq!(back.sealed_length, 7);
        assert!(back.sealed);
        assert_eq!(back.avali, 8);
        assert_eq!(back.replicate_disks, vec![9, 10]);
        assert_eq!(back.parity_disks, vec![11]);
        assert!(!back.ec_converted);
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

// ── extent nodes ────────────────────────────────────────────────────────────

/// `nodes/<id>` — one extent node.
///
/// `node_uuid` is the STABLE identity; `address` and `shard_ports` are where it
/// happened to be last time it registered, and the EN rewrites them on every
/// boot. Empty uuid = a legacy row that has not adopted one yet.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub(crate) struct NodeRecord {
    pub node_id: u64,
    pub address: String,
    pub disks: Vec<u64>,
    /// Empty = legacy single-thread extent node.
    pub shard_ports: Vec<u16>,
    /// Empty = legacy node; the manager falls back to `address` for df.
    pub control_address: String,
    /// Stable identity, decoupled from the network address so an EN can change
    /// IP or shard-port layout across restarts and still be the SAME node.
    pub node_uuid: String,
}

impl PersistRecord for NodeRecord {
    const RECORD_TYPE: u8 = RECORD_TYPE_NODE;
    const FORMAT_VERSION: u8 = 1;
    const NAME: &'static str = "node";
}

impl From<&MgrNodeInfo> for NodeRecord {
    fn from(n: &MgrNodeInfo) -> Self {
        let MgrNodeInfo {
            node_id,
            address,
            disks,
            shard_ports,
            control_address,
            node_uuid,
        } = n;
        Self {
            node_id: *node_id,
            address: address.clone(),
            disks: disks.clone(),
            shard_ports: shard_ports.clone(),
            control_address: control_address.clone(),
            node_uuid: node_uuid.clone(),
        }
    }
}

impl From<&NodeRecord> for MgrNodeInfo {
    fn from(r: &NodeRecord) -> Self {
        let NodeRecord {
            node_id,
            address,
            disks,
            shard_ports,
            control_address,
            node_uuid,
        } = r;
        Self {
            node_id: *node_id,
            address: address.clone(),
            disks: disks.clone(),
            shard_ports: shard_ports.clone(),
            control_address: control_address.clone(),
            node_uuid: node_uuid.clone(),
        }
    }
}

// ── streams ─────────────────────────────────────────────────────────────────

/// `streams/<id>` — the ordered extent list that IS the stream.
///
/// `replicates` reads `0` on a legacy row, meaning "unknown"; nothing back-fills
/// it, so a reader must treat 0 as absent rather than as a replication factor.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub(crate) struct StreamRecord {
    pub stream_id: u64,
    pub extent_ids: Vec<u64>,
    pub ec_data_shard: u32,
    pub ec_parity_shard: u32,
    /// `0` = unknown (a row written before the field existed).
    pub replicates: u32,
}

impl PersistRecord for StreamRecord {
    const RECORD_TYPE: u8 = RECORD_TYPE_STREAM;
    const FORMAT_VERSION: u8 = 1;
    const NAME: &'static str = "stream";
}

impl From<&MgrStreamInfo> for StreamRecord {
    fn from(v: &MgrStreamInfo) -> Self {
        let MgrStreamInfo {
            stream_id,
            extent_ids,
            ec_data_shard,
            ec_parity_shard,
            replicates,
        } = v;
        Self {
            stream_id: *stream_id,
            extent_ids: extent_ids.clone(),
            ec_data_shard: *ec_data_shard,
            ec_parity_shard: *ec_parity_shard,
            replicates: *replicates,
        }
    }
}

impl From<&StreamRecord> for MgrStreamInfo {
    fn from(r: &StreamRecord) -> Self {
        let StreamRecord {
            stream_id,
            extent_ids,
            ec_data_shard,
            ec_parity_shard,
            replicates,
        } = r;
        Self {
            stream_id: *stream_id,
            extent_ids: extent_ids.clone(),
            ec_data_shard: *ec_data_shard,
            ec_parity_shard: *ec_parity_shard,
            replicates: *replicates,
        }
    }
}

// ── key ranges (nested inside partitions and regions) ───────────────────────

/// `[start_key, end_key)`. An empty `end_key` means unbounded.
///
/// Not a record of its own — it has no key and no `RECORD_TYPE`; it is a FIELD
/// of the partition and region records, which carry the version that covers it.
/// Changing this shape therefore moves BOTH of their `FORMAT_VERSION`s.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RangeRecord {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

impl From<&MgrRange> for RangeRecord {
    fn from(r: &MgrRange) -> Self {
        let MgrRange { start_key, end_key } = r;
        Self {
            start_key: start_key.clone(),
            end_key: end_key.clone(),
        }
    }
}

impl From<&RangeRecord> for MgrRange {
    fn from(r: &RangeRecord) -> Self {
        let RangeRecord { start_key, end_key } = r;
        Self {
            start_key: start_key.clone(),
            end_key: end_key.clone(),
        }
    }
}

// ── partitions ──────────────────────────────────────────────────────────────

/// `partitions/<id>` — a partition's three stream ids and its key range.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub(crate) struct PartitionRecord {
    pub part_id: u64,
    pub log_stream: u64,
    pub row_stream: u64,
    pub meta_stream: u64,
    pub rg: Option<RangeRecord>,
}

impl PersistRecord for PartitionRecord {
    const RECORD_TYPE: u8 = RECORD_TYPE_PARTITION;
    const FORMAT_VERSION: u8 = 1;
    const NAME: &'static str = "partition";
}

impl From<&MgrPartitionMeta> for PartitionRecord {
    fn from(v: &MgrPartitionMeta) -> Self {
        let MgrPartitionMeta {
            part_id,
            log_stream,
            row_stream,
            meta_stream,
            rg,
        } = v;
        Self {
            part_id: *part_id,
            log_stream: *log_stream,
            row_stream: *row_stream,
            meta_stream: *meta_stream,
            rg: rg.as_ref().map(RangeRecord::from),
        }
    }
}

impl From<&PartitionRecord> for MgrPartitionMeta {
    fn from(r: &PartitionRecord) -> Self {
        let PartitionRecord {
            part_id,
            log_stream,
            row_stream,
            meta_stream,
            rg,
        } = r;
        Self {
            part_id: *part_id,
            log_stream: *log_stream,
            row_stream: *row_stream,
            meta_stream: *meta_stream,
            rg: rg.as_ref().map(MgrRange::from),
        }
    }
}

// ── regions ─────────────────────────────────────────────────────────────────

/// `regions/<id>` — which PS serves a partition, and the epoch clients stamp.
///
/// **This is the PERSISTED shape only.** The ledger's Scope 3 splits the region
/// three ways — persisted, PS-facing, and a four-field CLIENT routing record
/// that drops the three `*_stream` ids the SDK never reads. That third one
/// changes what a client receives, so it is a client-facing wire change and
/// belongs with the two-form rule, not here.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub(crate) struct RegionRecord {
    pub rg: Option<RangeRecord>,
    pub part_id: u64,
    pub ps_id: u64,
    pub log_stream: u64,
    pub row_stream: u64,
    pub meta_stream: u64,
    pub region_epoch: u64,
}

impl PersistRecord for RegionRecord {
    const RECORD_TYPE: u8 = RECORD_TYPE_REGION;
    const FORMAT_VERSION: u8 = 1;
    const NAME: &'static str = "region";
}

impl From<&MgrRegionInfo> for RegionRecord {
    fn from(v: &MgrRegionInfo) -> Self {
        let MgrRegionInfo {
            rg,
            part_id,
            ps_id,
            log_stream,
            row_stream,
            meta_stream,
            region_epoch,
        } = v;
        Self {
            rg: rg.as_ref().map(RangeRecord::from),
            part_id: *part_id,
            ps_id: *ps_id,
            log_stream: *log_stream,
            row_stream: *row_stream,
            meta_stream: *meta_stream,
            region_epoch: *region_epoch,
        }
    }
}

impl From<&RegionRecord> for MgrRegionInfo {
    fn from(r: &RegionRecord) -> Self {
        let RegionRecord {
            rg,
            part_id,
            ps_id,
            log_stream,
            row_stream,
            meta_stream,
            region_epoch,
        } = r;
        Self {
            rg: rg.as_ref().map(MgrRange::from),
            part_id: *part_id,
            ps_id: *ps_id,
            log_stream: *log_stream,
            row_stream: *row_stream,
            meta_stream: *meta_stream,
            region_epoch: *region_epoch,
        }
    }
}

// ── extents ─────────────────────────────────────────────────────────────────

/// `extents/<id>` — the replica set, and everything about the extent's state
/// EXCEPT where its payload physically sits.
///
/// Two things deliberately are NOT fields here, and both are sibling etcd keys
/// for the same reason: widening this record makes every stored extent fail
/// validation on replay, which refuses leadership rather than degrading.
/// `extentLayout/<id>` holds the payload location (absent ⇒ `InDat`) and
/// `extentCorrupt/<id>` the corrupt-slot bitmap. That constraint is softer now
/// that this record has its own `FORMAT_VERSION` and a converter — widening it
/// is a version bump plus a converter step rather than an impossibility — but
/// neither key is worth folding in without a reason.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub(crate) struct ExtentRecord {
    pub extent_id: u64,
    pub replicates: Vec<u64>,
    pub parity: Vec<u64>,
    pub eversion: u64,
    pub refs: u64,
    /// WRITE-FROZEN at 0 for anything this build manages. Kept because a legacy
    /// extent stuck at `refs == 0 && vp_table_refs > 0` must not be reaped
    /// until a migration re-confirms it — see `extent_can_delete`.
    pub vp_table_refs: u64,
    pub sealed_length: u64,
    /// The authoritative "is sealed" STATE. NOT `sealed_length > 0`: an
    /// authoritative EMPTY seal is `sealed = true, sealed_length = 0`.
    pub sealed: bool,
    pub avali: u32,
    pub replicate_disks: Vec<u64>,
    pub parity_disks: Vec<u64>,
    pub ec_converted: bool,
}

impl PersistRecord for ExtentRecord {
    const RECORD_TYPE: u8 = RECORD_TYPE_EXTENT;
    const FORMAT_VERSION: u8 = 1;
    const NAME: &'static str = "extent";
}

impl From<&MgrExtentInfo> for ExtentRecord {
    fn from(v: &MgrExtentInfo) -> Self {
        let MgrExtentInfo {
            extent_id,
            replicates,
            parity,
            eversion,
            refs,
            vp_table_refs,
            sealed_length,
            sealed,
            avali,
            replicate_disks,
            parity_disks,
            ec_converted,
        } = v;
        Self {
            extent_id: *extent_id,
            replicates: replicates.clone(),
            parity: parity.clone(),
            eversion: *eversion,
            refs: *refs,
            vp_table_refs: *vp_table_refs,
            sealed_length: *sealed_length,
            sealed: *sealed,
            avali: *avali,
            replicate_disks: replicate_disks.clone(),
            parity_disks: parity_disks.clone(),
            ec_converted: *ec_converted,
        }
    }
}

impl From<&ExtentRecord> for MgrExtentInfo {
    fn from(r: &ExtentRecord) -> Self {
        let ExtentRecord {
            extent_id,
            replicates,
            parity,
            eversion,
            refs,
            vp_table_refs,
            sealed_length,
            sealed,
            avali,
            replicate_disks,
            parity_disks,
            ec_converted,
        } = r;
        Self {
            extent_id: *extent_id,
            replicates: replicates.clone(),
            parity: parity.clone(),
            eversion: *eversion,
            refs: *refs,
            vp_table_refs: *vp_table_refs,
            sealed_length: *sealed_length,
            sealed: *sealed,
            avali: *avali,
            replicate_disks: replicate_disks.clone(),
            parity_disks: parity_disks.clone(),
            ec_converted: *ec_converted,
        }
    }
}

/// The CLIENT's third form of a region: four fields, no stream ids.
///
/// The three `*_stream` ids are dropped by NAME rather than by `..`, so this is
/// still an exhaustive destructure — adding a field to the record stops this
/// compiling, and a reader can see at a glance exactly what the client is not
/// told. That is the whole point of the narrowing: a stream id is a
/// stream-layer identity and the SDK has never read one, so shipping them
/// leaked a lower layer's identifiers into every embedded image.
impl From<&RegionRecord> for ClientRegion {
    fn from(r: &RegionRecord) -> Self {
        let RegionRecord {
            rg,
            part_id,
            ps_id,
            log_stream: _,
            row_stream: _,
            meta_stream: _,
            region_epoch,
        } = r;
        Self {
            rg: rg.as_ref().map(MgrRange::from),
            part_id: *part_id,
            ps_id: *ps_id,
            region_epoch: *region_epoch,
        }
    }
}
