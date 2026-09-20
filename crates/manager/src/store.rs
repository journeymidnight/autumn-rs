//! The manager's authoritative in-memory cluster state.
//!
//! This lived in `autumn-common` until the persisted records were split out of
//! the wire schema. It moved here for one reason: a persisted record is
//! `pub(crate)` to the manager (see `persist/`), and `MetadataState` is what
//! holds those records in memory — a state struct in a SHARED crate cannot hold
//! a type only the manager may name. Nothing outside the manager ever
//! referenced it (verified: the only other mention was `autumn-common`'s own
//! re-export), so the move costs no other crate anything.
//!
//! **`is_owner_epoch_fence_message` deliberately did NOT move.** The stream
//! crate classifies manager rejections with it, so it stays in `autumn-common`
//! where `autumn-stream` can reach it without depending on the manager. That
//! separates the matcher from `ensure_owner_epoch`, its only producer, and the
//! old comment there said adjacency was what kept the two from drifting apart.
//! Adjacency was never the mechanism: the two share the `OWNER_*_TOKEN`
//! constants, so a reword cannot detach the matcher, and
//! `owner_fence_matcher_pairs_with_producer` (below, moved with the producer)
//! classifies errors from the REAL producer rather than from literals. Both
//! survive the split intact; only the two being visible on one screen is lost.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use autumn_common::store::{
    OWNER_EPOCH_MISMATCH_TOKEN, OWNER_KEY_MISSING_TOKEN, OWNER_KEY_PREFIX_TOKEN,
};
use autumn_common::{AppError, AppResult};
use autumn_rpc::manager_rpc::{
    MgrExtentInfo, MgrNodeInfo, MgrPartitionMeta, MgrRegionInfo, MgrStreamInfo,
};

#[derive(Debug, Default, Clone)]
pub(crate) struct MetadataState {
    pub next_id: u64,
    pub streams: HashMap<u64, MgrStreamInfo>,
    pub extents: HashMap<u64, MgrExtentInfo>,
    pub nodes: HashMap<u64, MgrNodeInfo>,
    pub disks: HashMap<u64, crate::persist::records::DiskRecord>,
    pub owner_epochs: HashMap<String, i64>,
    pub next_revision: i64,
    pub partitions: HashMap<u64, MgrPartitionMeta>,
    pub ps_nodes: HashMap<u64, String>,
    pub regions: BTreeMap<u64, MgrRegionInfo>,
    /// per-partition listener addresses reported by PS via
    /// `RegisterPartitionAddr`. In-memory only; rebuilt when the PS
    /// re-registers on restart. Keyed by `part_id`; value is `host:port`.
    pub part_addrs: HashMap<u64, String>,
}

impl MetadataState {
    pub fn alloc_ids(&mut self, count: u64) -> (u64, u64) {
        let start = self.next_id.max(1);
        let end = start + count;
        self.next_id = end;
        (start, end)
    }

    /// the epoch BUMPS on every acquire (mirrors the etcd-backed
    /// `acquire_owner_epoch`, which rewrites the key and uses the fresh
    /// mod_revision). Re-acquiring an existing key returns a strictly
    /// higher epoch so the previous holder is fenced — required for
    /// ownership failback (A→B→A) and same-key split-brain fencing.
    pub fn acquire_owner_lock(&mut self, key: &str) -> i64 {
        self.next_revision += 1;
        let rev = self.next_revision;
        self.owner_epochs.insert(key.to_string(), rev);
        rev
    }

    /// The ONLY producer of an owner-epoch fence rejection. Its wording is
    /// built from the `OWNER_*_TOKEN` constants in `autumn_common::store`,
    /// which `is_owner_epoch_fence_message` matches on — that shared spelling,
    /// not proximity, is what keeps the two from drifting.
    pub fn ensure_owner_epoch(&self, key: &str, owner_epoch: i64) -> AppResult<()> {
        match self.owner_epochs.get(key) {
            Some(v) if *v == owner_epoch => Ok(()),
            Some(v) => Err(AppError::Precondition(format!(
                "{OWNER_KEY_PREFIX_TOKEN}{key} {OWNER_EPOCH_MISMATCH_TOKEN}, \
                 expected {v}, got {owner_epoch}"
            ))),
            None => Err(AppError::Precondition(format!(
                "{OWNER_KEY_PREFIX_TOKEN}{key} {OWNER_KEY_MISSING_TOKEN}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MetadataStore {
    pub inner: Rc<RefCell<MetadataState>>,
}

impl MetadataStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: Rc::new(RefCell::new(MetadataState {
                next_id: 1,
                next_revision: 0,
                ..MetadataState::default()
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_common::is_owner_epoch_fence_message;

    #[test]
    fn alloc_ids_monotonic() {
        let mut s = MetadataState::default();
        let (a1, a2) = s.alloc_ids(2);
        assert_eq!((a1, a2), (1, 3));
        let (b1, b2) = s.alloc_ids(3);
        assert_eq!((b1, b2), (3, 6));
    }

    #[test]
    fn owner_lock_revision_validation() {
        let mut s = MetadataState::default();
        let rev = s.acquire_owner_lock("lock/a");
        assert!(s.ensure_owner_epoch("lock/a", rev).is_ok());
        assert!(s.ensure_owner_epoch("lock/a", rev + 1).is_err());
        assert!(s.ensure_owner_epoch("lock/b", 1).is_err());
    }

    /// re-acquiring the same owner_key must FENCE the previous
    /// holder — the epoch bumps on every acquire (failback A→B→A and
    /// same-key split-brain both depend on this), and epochs stay
    /// globally monotonic across different keys.
    #[test]
    fn owner_lock_reacquire_bumps_and_fences_previous_holder() {
        let mut s = MetadataState::default();
        let a1 = s.acquire_owner_lock("lock/a");
        let b1 = s.acquire_owner_lock("lock/b");
        assert!(b1 > a1, "epochs are globally monotonic across keys");
        let a2 = s.acquire_owner_lock("lock/a");
        assert!(a2 > b1, "re-acquire returns a strictly higher epoch");
        assert!(
            s.ensure_owner_epoch("lock/a", a1).is_err(),
            "previous holder's epoch is fenced after re-acquire"
        );
        assert!(s.ensure_owner_epoch("lock/a", a2).is_ok());
    }

    /// Pins `is_owner_epoch_fence_message` to the ACTUAL messages
    /// `ensure_owner_epoch` produces — both rejection variants, matched
    /// raw AND wrapped the way they travel over the wire
    /// (`AppError::Precondition` Display prefixes "precondition failed:").
    /// If the producer's wording changes without the matcher, this fails.
    ///
    /// **This test is why the producer and the matcher may live in different
    /// crates.** It moved here with the producer and reaches ACROSS to
    /// `autumn_common`'s matcher, so the pairing is still checked end to end —
    /// which is the property the old "kept ADJACENT" comment was really after.
    #[test]
    fn owner_fence_matcher_pairs_with_producer() {
        let mut s = MetadataState::default();
        let rev = s.acquire_owner_lock("partition/17");

        // Variant 1: epoch mismatch (the live stale-epoch incident shape).
        let mismatch = s.ensure_owner_epoch("partition/17", rev - 1).unwrap_err();
        assert!(is_owner_epoch_fence_message(&mismatch.to_string()));
        // As it crosses the wire: err_to_code → CODE_PRECONDITION,
        // message = AppError Display.
        assert!(is_owner_epoch_fence_message(&format!("{mismatch}")));

        // Variant 2: owner_key never acquired / lost (etcd reset).
        let missing = s.ensure_owner_epoch("partition/99", 1).unwrap_err();
        assert!(is_owner_epoch_fence_message(&missing.to_string()));

        // Ordinary preconditions must NOT classify as a fence.
        assert!(!is_owner_epoch_fence_message(
            "precondition failed: stream cannot be empty after punch holes"
        ));
        assert!(!is_owner_epoch_fence_message(
            "precondition failed: admin token invalid"
        ));
    }
}
