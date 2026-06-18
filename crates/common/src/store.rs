use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use autumn_rpc::manager_rpc::{
    MgrDiskInfo, MgrExtentInfo, MgrNodeInfo, MgrPartitionMeta, MgrRegionInfo, MgrStreamInfo,
};

use crate::{AppError, AppResult};

#[derive(Debug, Default, Clone)]
pub struct MetadataState {
    pub next_id: u64,
    pub streams: HashMap<u64, MgrStreamInfo>,
    pub extents: HashMap<u64, MgrExtentInfo>,
    pub nodes: HashMap<u64, MgrNodeInfo>,
    pub disks: HashMap<u64, MgrDiskInfo>,
    pub owner_epochs: HashMap<String, i64>,
    pub next_revision: i64,
    pub partitions: HashMap<u64, MgrPartitionMeta>,
    pub ps_nodes: HashMap<u64, String>,
    pub regions: BTreeMap<u64, MgrRegionInfo>,
    /// F099-K — per-partition listener addresses reported by PS via
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

    /// F265: the epoch BUMPS on every acquire (mirrors the etcd-backed
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

    pub fn ensure_owner_epoch(&self, key: &str, owner_epoch: i64) -> AppResult<()> {
        match self.owner_epochs.get(key) {
            Some(v) if *v == owner_epoch => Ok(()),
            Some(v) => Err(AppError::Precondition(format!(
                "owner_key={key} owner_epoch mismatch, expected {v}, got {owner_epoch}"
            ))),
            None => Err(AppError::Precondition(format!(
                "owner_key={key} does not exist"
            ))),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MetadataStore {
    pub inner: Rc<RefCell<MetadataState>>,
}

impl MetadataStore {
    pub fn new() -> Self {
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

    /// F265: re-acquiring the same owner_key must FENCE the previous
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
}
