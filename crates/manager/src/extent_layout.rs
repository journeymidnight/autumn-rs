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
use autumn_rpc::extent_rpc::PayloadLocation;

use crate::AutumnManager;

pub(crate) const EXTENT_LAYOUT_PREFIX: &str = "extentLayout/";

pub(crate) fn extent_layout_key(extent_id: u64) -> String {
    format!("{EXTENT_LAYOUT_PREFIX}{extent_id}")
}

impl AutumnManager {
    /// Where `extent_id`'s payload lives. Unknown extent ⇒ `InDat`.
    pub(crate) fn payload_location_of(&self, extent_id: u64) -> PayloadLocation {
        self.extent_payload_location
            .borrow()
            .get(&extent_id)
            .copied()
            .unwrap_or(PayloadLocation::InDat)
    }

    /// Publish the in-memory location AFTER the caller's txn committed, so
    /// memory never claims a layout etcd has not accepted (the etcd-first rule).
    pub(crate) fn commit_payload_location(&self, extent_id: u64, loc: PayloadLocation) {
        self.extent_payload_location
            .borrow_mut()
            .insert(extent_id, loc);
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
    pub(crate) fn install_replayed_payload_locations(
        &self,
        decoded: HashMap<u64, PayloadLocation>,
    ) {
        *self.extent_payload_location.borrow_mut() = decoded;
    }

    /// Decode the `extentLayout/` prefix, FAIL-LOUD like every other persisted
    /// value: an entry this build cannot read refuses leadership.
    ///
    /// It used to drop a malformed entry with a WARN and let the extent read as
    /// `InDat`, on the stated grounds that refusing leadership "over a byte
    /// that only selects between two files — where the default is the
    /// pre-existing behaviour" traded an outage for a cosmetic inconsistency.
    /// The premise is false in both halves. `InDat` is not a neutral default
    /// here, it is a positive claim that `extent-{id}.dat` holds the payload,
    /// published to every reader on `ExtentInfoResp`; on an extent whose bytes
    /// have moved into a shard file that claim serves shard bytes as a whole
    /// value. And this value is written by nothing but this cluster's own
    /// managers, so a byte outside the set means a newer manager wrote it and
    /// was rolled back — the case `replay_from_etcd` is fail-loud about
    /// everywhere else, and the case the stop-the-world discipline exists for.
    ///
    /// ABSENT is still `InDat`, and that is untouched: no key at all is what
    /// every extent predating the CoW scheme has, and it is the whole migration
    /// story (see this module's header). Absent is the absence of a claim;
    /// an unreadable byte is a claim this build cannot honour.
    /// Takes the RAW kvs, key parsing included, so that a test of this function
    /// covers the whole decision `replay_from_etcd` makes and only a `?`
    /// separates the two. Parsing the key at the call site put the "drop it
    /// quietly" shape back where a test could not see it — and a dropped key
    /// leaves its extent reading as `InDat`, which is the same wrong claim an
    /// unreadable value would make.
    pub(crate) fn decode_extent_layout_kvs(
        kvs: &[autumn_etcd::proto::KeyValue],
    ) -> Result<HashMap<u64, PayloadLocation>, String> {
        let mut out = HashMap::new();
        for kv in kvs {
            let id = Self::parse_id_from_key(EXTENT_LAYOUT_PREFIX, &kv.key)
                .map_err(|e| format!("undecodable extentLayout key: {e}"))?;
            if id == 0 {
                return Err("extentLayout entry keyed by extent 0, which is not an extent".into());
            }
            let Some(&b) = kv.value.first() else {
                return Err(format!("extentLayout/{id} holds an empty value"));
            };
            let Some(loc) = PayloadLocation::from_wire_byte(b) else {
                return Err(format!(
                    "extentLayout/{id} names payload location {b}, which this build does not \
                     have — a newer manager wrote it; refusing to lead rather than serve the \
                     wrong file for this extent"
                ));
            };
            out.insert(id, loc);
        }
        Ok(out)
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

    fn kv(key: &str, value: &[u8]) -> autumn_etcd::proto::KeyValue {
        autumn_etcd::proto::KeyValue {
            key: key.as_bytes().to_vec(),
            value: value.to_vec(),
            ..Default::default()
        }
    }

    #[test]
    fn replay_restores_the_map() {
        let decoded = AutumnManager::decode_extent_layout_kvs(&[
            kv("extentLayout/7", &[1u8]),
            kv("extentLayout/9", &[0u8]),
        ])
        .expect("both entries name a location this build has");
        let m = AutumnManager::new();
        m.install_replayed_payload_locations(decoded);
        assert_eq!(m.payload_location_of(7), PayloadLocation::InShardFile);
        assert_eq!(m.payload_location_of(9), PayloadLocation::InDat);
        assert_eq!(
            m.payload_location_of(11),
            PayloadLocation::InDat,
            "an extent with NO entry is the migration case and still reads as .dat"
        );
    }

    /// The reversal this module's `decode_extent_layout_kvs` doc argues for.
    /// A location byte only a NEWER manager could have written must refuse
    /// leadership, not read as `.dat` — `.dat` is a claim about which file
    /// holds the value, and on a converted extent it is the wrong one.
    #[test]
    fn a_location_this_build_cannot_read_refuses_leadership() {
        let err = AutumnManager::decode_extent_layout_kvs(&[
            kv("extentLayout/7", &[1u8]),
            kv("extentLayout/9", &[2u8]),
        ])
        .expect_err("byte 2 names no location this build has");
        assert!(err.contains("extentLayout/9"), "names the entry: {err}");
        assert!(err.contains('2'), "names the byte it could not read: {err}");
    }

    /// An EMPTY value is corruption, not a default: nothing this cluster runs
    /// can write one, since every writer goes through `PayloadLocation`.
    #[test]
    fn an_empty_entry_refuses_leadership() {
        let err = AutumnManager::decode_extent_layout_kvs(&[kv("extentLayout/7", &[])])
            .expect_err("an empty value names nothing");
        assert!(err.contains("extentLayout/7"), "names the entry: {err}");
    }

    /// A key this build cannot parse is refused for the SAME reason its value
    /// would be: dropping it leaves the extent reading as `InDat`, and a
    /// silently-skipped key is indistinguishable from an extent that never had
    /// an entry. Pinned here rather than at the call site because the key parse
    /// now lives inside this function — that is what keeps a test of it a test
    /// of what replay actually does.
    #[test]
    fn an_unparseable_key_refuses_leadership() {
        let err = AutumnManager::decode_extent_layout_kvs(&[kv("extentLayout/not-a-number", &[1])])
            .expect_err("the id is not a u64");
        assert!(
            err.contains("undecodable extentLayout key"),
            "says what it could not read: {err}"
        );
    }
}
