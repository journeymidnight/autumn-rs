//! Where an extent's payload physically lives, per extent.
//!
//! `MgrExtentInfo` says what an extent IS (members, eversion, sealed length,
//! whether its bytes are erasure-coded). This module says which FILE on each
//! member holds those bytes — `extent-{id}.dat` or `extent-{id}.shard{i}`.
//!
//! It is a sibling etcd key rather than a field on `MgrExtentInfo` for one
//! decisive reason: that struct is the persisted `extents/<id>` value, and
//! widening a persisted rkyv struct makes an existing cluster's stored records
//! fail validation on replay — which does not degrade gracefully, it refuses
//! leadership. A separate key lets every pre-existing extent keep decoding
//! exactly as before and simply read as `InDat`, which is what it is: the
//! pre-CoW conversion scheme renamed each shard over `.dat`.
//!
//! Absent ⇒ `InDat`. That equivalence is the whole migration story.

use std::collections::HashMap;

use autumn_common::error::AppError;
use autumn_rpc::extent_rpc::{PayloadLocation, PAYLOAD_LOCATION_IN_DAT};

use crate::AutumnManager;

pub(crate) const EXTENT_LAYOUT_PREFIX: &str = "extentLayout/";

pub(crate) fn extent_layout_key(extent_id: u64) -> String {
    format!("{EXTENT_LAYOUT_PREFIX}{extent_id}")
}

impl AutumnManager {
    /// Where `extent_id`'s payload lives. Unknown extent ⇒ `InDat`.
    pub(crate) fn payload_location_of(&self, extent_id: u64) -> PayloadLocation {
        PayloadLocation::from_byte(
            self.extent_payload_location
                .borrow()
                .get(&extent_id)
                .copied()
                .unwrap_or(PAYLOAD_LOCATION_IN_DAT),
        )
    }

    /// Publish the in-memory location AFTER the caller's txn committed, so
    /// memory never claims a layout etcd has not accepted (the etcd-first rule).
    pub(crate) fn commit_payload_location(&self, extent_id: u64, loc: PayloadLocation) {
        self.extent_payload_location
            .borrow_mut()
            .insert(extent_id, loc.as_byte());
    }

    /// Drop an extent's location when the extent itself is gone. Ids are never
    /// reused, so a leaked entry is not a correctness problem — but it is
    /// unbounded growth on a long-lived cluster, and the key would outlive
    /// every trace of what it described.
    pub(crate) async fn forget_payload_location(&self, extent_id: u64) -> Result<(), AppError> {
        let had = self
            .extent_payload_location
            .borrow_mut()
            .remove(&extent_id)
            .is_some();
        if !had {
            // Never persisted (the overwhelmingly common `InDat` case) — no key
            // to delete, so skip the round-trip.
            return Ok(());
        }
        if let Some(etcd) = &self.etcd {
            etcd.put_and_delete_txn(Vec::new(), vec![extent_layout_key(extent_id)])
                .await?;
        }
        Ok(())
    }

    /// Rebuild the in-memory view on promotion. Only non-default entries are
    /// stored, so this map is empty on any cluster that has never converted an
    /// extent under the CoW scheme.
    pub(crate) fn install_replayed_payload_locations(&self, decoded: HashMap<u64, u8>) {
        *self.extent_payload_location.borrow_mut() = decoded;
    }

    /// Decode the `extentLayout/` prefix. A malformed value is dropped with a
    /// WARN and reads as `InDat`: refusing leadership over a byte that only
    /// selects between two files — where the default is the pre-existing
    /// behaviour — would trade a cosmetic inconsistency for an outage.
    pub(crate) fn decode_extent_layout_kvs<'a>(
        kvs: impl Iterator<Item = (u64, &'a [u8])>,
    ) -> HashMap<u64, u8> {
        let mut out = HashMap::new();
        for (id, value) in kvs {
            match value.first() {
                Some(&b) if id != 0 => {
                    out.insert(id, b);
                }
                _ => {
                    tracing::warn!(
                        extent_id = id,
                        "extentLayout entry is malformed; treating the extent as InDat"
                    );
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absent_entry_reads_as_in_dat() {
        let m = AutumnManager::new();
        assert_eq!(m.payload_location_of(42), PayloadLocation::InDat);
    }

    #[test]
    fn a_committed_location_reads_back() {
        let m = AutumnManager::new();
        m.commit_payload_location(42, PayloadLocation::InShardFile);
        assert_eq!(m.payload_location_of(42), PayloadLocation::InShardFile);
        assert_eq!(
            m.payload_location_of(43),
            PayloadLocation::InDat,
            "one extent's layout must not leak onto another"
        );
    }

    #[test]
    fn replay_restores_the_map() {
        let raw: Vec<(u64, &[u8])> = vec![(7, &[1u8]), (9, &[0u8]), (11, &[])];
        let decoded = AutumnManager::decode_extent_layout_kvs(raw.into_iter());
        let m = AutumnManager::new();
        m.install_replayed_payload_locations(decoded);
        assert_eq!(m.payload_location_of(7), PayloadLocation::InShardFile);
        assert_eq!(m.payload_location_of(9), PayloadLocation::InDat);
        assert_eq!(
            m.payload_location_of(11),
            PayloadLocation::InDat,
            "a malformed entry must not block replay"
        );
    }
}
