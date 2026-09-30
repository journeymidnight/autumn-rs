//! Durable lower bound for stream replay.
//!
//! `streamCoveredBefore/<stream_id>` stores one little-endian extent id.  A
//! present value means every extent before that member in the stream's ordered
//! `extent_ids` is represented by a durable checkpoint.  Recovery may begin at
//! the marked extent (offset zero), or at a later ordinary checkpoint cursor.
//!
//! This is deliberately a sibling key instead of a `StreamRecord` field.  The
//! latter is persisted rkyv and widening it would make every existing
//! `streams/<id>` value unreadable without an offline conversion.  Absent is
//! the legacy format and means "no extra lower bound".

use std::collections::HashMap;

use crate::AutumnManager;

pub(crate) const STREAM_COVERED_PREFIX: &str = "streamCoveredBefore/";

pub(crate) fn stream_covered_key(stream_id: u64) -> String {
    format!("{STREAM_COVERED_PREFIX}{stream_id}")
}

impl AutumnManager {
    pub(crate) fn stream_covered_before(&self, stream_id: u64) -> Option<u64> {
        self.stream_covered_before.borrow().get(&stream_id).copied()
    }

    pub(crate) fn commit_stream_covered_before(&self, stream_id: u64, extent_id: u64) {
        debug_assert_ne!(stream_id, 0);
        debug_assert_ne!(extent_id, 0);
        self.stream_covered_before
            .borrow_mut()
            .insert(stream_id, extent_id);
    }

    pub(crate) fn forget_stream_covered_before(&self, stream_id: u64) {
        self.stream_covered_before.borrow_mut().remove(&stream_id);
    }

    pub(crate) fn install_replayed_stream_covered_before(&self, decoded: HashMap<u64, u64>) {
        *self.stream_covered_before.borrow_mut() = decoded;
    }

    /// Decode fail-loud: an existing malformed key is not equivalent to an
    /// absent legacy marker, because accepting it as absent can turn a bounded
    /// reopen into a full-stream scan after an upgrade or rollback accident.
    pub(crate) fn decode_stream_covered_kvs(
        kvs: &[autumn_etcd::proto::KeyValue],
    ) -> Result<HashMap<u64, u64>, String> {
        let mut out = HashMap::new();
        for kv in kvs {
            let stream_id = Self::parse_id_from_key(STREAM_COVERED_PREFIX, &kv.key)
                .map_err(|e| format!("undecodable streamCoveredBefore key: {e}"))?;
            if stream_id == 0 {
                return Err("streamCoveredBefore entry keyed by stream 0".into());
            }
            let raw: [u8; 8] = kv.value.as_slice().try_into().map_err(|_| {
                format!(
                    "streamCoveredBefore/{stream_id} has {} bytes, expected 8",
                    kv.value.len()
                )
            })?;
            let extent_id = u64::from_le_bytes(raw);
            if extent_id == 0 {
                return Err(format!("streamCoveredBefore/{stream_id} names extent 0"));
            }
            out.insert(stream_id, extent_id);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv(key: &str, value: &[u8]) -> autumn_etcd::proto::KeyValue {
        autumn_etcd::proto::KeyValue {
            key: key.as_bytes().to_vec(),
            value: value.to_vec(),
            ..Default::default()
        }
    }

    #[test]
    fn absent_is_legacy_and_present_round_trips() {
        let m = AutumnManager::new();
        assert_eq!(m.stream_covered_before(7), None);
        m.commit_stream_covered_before(7, 99);
        assert_eq!(m.stream_covered_before(7), Some(99));
        m.forget_stream_covered_before(7);
        assert_eq!(m.stream_covered_before(7), None);
    }

    #[test]
    fn replay_decodes_exact_u64_values() {
        let decoded = AutumnManager::decode_stream_covered_kvs(&[
            kv("streamCoveredBefore/7", &99u64.to_le_bytes()),
            kv("streamCoveredBefore/8", &123u64.to_le_bytes()),
        ])
        .unwrap();
        assert_eq!(decoded.get(&7), Some(&99));
        assert_eq!(decoded.get(&8), Some(&123));
    }

    #[test]
    fn malformed_marker_refuses_replay() {
        for bad in [
            kv("streamCoveredBefore/nope", &99u64.to_le_bytes()),
            kv("streamCoveredBefore/7", &[1, 2, 3]),
            kv("streamCoveredBefore/7", &0u64.to_le_bytes()),
        ] {
            assert!(AutumnManager::decode_stream_covered_kvs(&[bad]).is_err());
        }
    }
}
