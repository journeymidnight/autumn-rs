//! Which of an extent's slots hold bytes a partition owner PROVED wrong.
//!
//! `avali` says a slot is not serving. It does not say WHY, and the two reasons
//! need opposite handling:
//!
//! - **behind** — the replica is short. `re_avali` refetches the missing tail.
//! - **corrupt** — the replica is full-length and wrong. `re_avali` cannot fix
//!   it; its entire test is `local_len >= sealed_length`, which a bit-rotted
//!   copy passes. Only a rebuild replaces the bytes.
//!
//! Without somewhere to record the difference, a corrupt report clears the bit
//! and nothing else. Under the default `fenced_only` recovery gate the dispatch
//! loop skips the slot before it ever reads `avali`, so the extent sits at RF-1
//! forever: isolated, unrepaired, and silent. Corruption is a STRONGER signal
//! than the conditions that do trigger a rebuild — the owner replayed those
//! bytes and proved them wrong — so it must not need a weaker one to be acted
//! on.
//!
//! Kept in a sibling key rather than widening `MgrExtentInfo`, following
//! `extent_layout`: that struct is the persisted `extents/<id>` value, and
//! growing it would make every stored extent fail rkyv validation on replay,
//! which refuses leadership rather than degrading.

use std::collections::HashMap;

use autumn_common::AppError;

use crate::AutumnManager;

pub(crate) const EXTENT_CORRUPT_PREFIX: &str = "extentCorrupt/";

pub(crate) fn extent_corrupt_key(extent_id: u64) -> String {
    format!("{EXTENT_CORRUPT_PREFIX}{extent_id}")
}

impl AutumnManager {
    /// Slots of `extent_id` proven corrupt, as a bitmap. Absent ⇒ none.
    pub(crate) fn corrupt_slots_of(&self, extent_id: u64) -> u32 {
        self.extent_corrupt_slots
            .borrow()
            .get(&extent_id)
            .copied()
            .unwrap_or(0)
    }

    /// Is this specific slot known corrupt?
    pub(crate) fn slot_is_corrupt(&self, extent_id: u64, slot: usize) -> bool {
        slot < 32 && (self.corrupt_slots_of(extent_id) & (1u32 << slot)) != 0
    }

    /// Record `bits` as corrupt, ORed onto what is already known. Etcd-first:
    /// memory is only updated once the key is durable, so the loop never
    /// dispatches a rebuild justified by a reason that did not survive.
    pub(crate) async fn mark_slots_corrupt(
        &self,
        extent_id: u64,
        bits: u32,
    ) -> Result<(), AppError> {
        if bits == 0 {
            return Ok(());
        }
        let merged = self.corrupt_slots_of(extent_id) | bits;
        if let Some(etcd) = &self.etcd {
            let key = extent_corrupt_key(extent_id);
            etcd.put_and_delete_txn(vec![(key, merged.to_le_bytes().to_vec())], vec![])
                .await?;
        }
        self.extent_corrupt_slots
            .borrow_mut()
            .insert(extent_id, merged);
        Ok(())
    }

    /// Clear one slot's corrupt mark — the rebuild that replaces those bytes has
    /// landed. Dropping the whole key once the last bit clears keeps this from
    /// growing without bound on a long-lived cluster.
    pub(crate) async fn clear_corrupt_slot(
        &self,
        extent_id: u64,
        slot: usize,
    ) -> Result<(), AppError> {
        if slot >= 32 {
            return Ok(());
        }
        let cur = self.corrupt_slots_of(extent_id);
        let next = cur & !(1u32 << slot);
        if next == cur {
            return Ok(());
        }
        if let Some(etcd) = &self.etcd {
            let key = extent_corrupt_key(extent_id);
            if next == 0 {
                etcd.put_and_delete_txn(vec![], vec![key]).await?;
            } else {
                etcd.put_and_delete_txn(vec![(key, next.to_le_bytes().to_vec())], vec![])
                    .await?;
            }
        }
        let mut m = self.extent_corrupt_slots.borrow_mut();
        if next == 0 {
            m.remove(&extent_id);
        } else {
            m.insert(extent_id, next);
        }
        Ok(())
    }

    /// Drop an extent's marks when the extent itself is gone.
    pub(crate) async fn forget_corrupt_slots(&self, extent_id: u64) -> Result<(), AppError> {
        let had = self
            .extent_corrupt_slots
            .borrow_mut()
            .remove(&extent_id)
            .is_some();
        if !had {
            return Ok(());
        }
        if let Some(etcd) = &self.etcd {
            etcd.put_and_delete_txn(vec![], vec![extent_corrupt_key(extent_id)])
                .await?;
        }
        Ok(())
    }

    pub(crate) fn install_replayed_corrupt_slots(&self, decoded: HashMap<u64, u32>) {
        *self.extent_corrupt_slots.borrow_mut() = decoded;
    }

    /// Decode replayed `extentCorrupt/` values. A malformed value is DROPPED
    /// rather than failing replay: losing a mark costs a missed rebuild, while
    /// refusing leadership over one costs the whole cluster.
    pub(crate) fn decode_extent_corrupt_kvs<'a>(
        kvs: impl Iterator<Item = (u64, &'a [u8])>,
    ) -> HashMap<u64, u32> {
        let mut out = HashMap::new();
        for (id, raw) in kvs {
            if let Ok(arr) = <[u8; 4]>::try_from(raw) {
                let bits = u32::from_le_bytes(arr);
                if bits != 0 {
                    out.insert(id, bits);
                }
            } else {
                tracing::warn!(
                    extent_id = id,
                    len = raw.len(),
                    "malformed extentCorrupt value; dropping the mark"
                );
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_drops_malformed_and_empty_values() {
        let three = [1u8, 2, 3];
        let five = [0u8; 5];
        let ok = 0b101u32.to_le_bytes();
        let zero = 0u32.to_le_bytes();
        let decoded = AutumnManager::decode_extent_corrupt_kvs(
            [
                (1u64, ok.as_slice()),
                (2u64, three.as_slice()),
                (3u64, five.as_slice()),
                (4u64, zero.as_slice()),
            ]
            .into_iter(),
        );
        assert_eq!(decoded.get(&1).copied(), Some(0b101));
        assert!(!decoded.contains_key(&2), "3-byte value is malformed");
        assert!(!decoded.contains_key(&3), "5-byte value is malformed");
        assert!(!decoded.contains_key(&4), "an empty bitmap is not a mark");
    }

    #[test]
    fn key_is_prefixed_and_parseable() {
        assert_eq!(extent_corrupt_key(42), "extentCorrupt/42");
        assert!(extent_corrupt_key(7).starts_with(EXTENT_CORRUPT_PREFIX));
    }
}
