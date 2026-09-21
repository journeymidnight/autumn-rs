use autumn_common::AppError;
use autumn_etcd::{Cmp, Op};
use autumn_rpc::manager_rpc::rkyv_encode;

use crate::extent_inflight::ExtentOpKind;
use crate::persist::records::ExtentRecord;
use crate::AutumnManager;

/// The marker identity observed before an apply or release starts.
///
/// The persisted bytes reject a different assignment. The etcd modification
/// revision rejects a byte-identical assignment created after this one.
pub(crate) struct InflightSnapshot {
    pub(crate) extent_id: u64,
    nonce: u64,
    record: Vec<u8>,
}

impl AutumnManager {
    /// Capture the marker identity before the first await in a commit path.
    pub(crate) fn snapshot_inflight(
        &self,
        extent_id: u64,
        kind: ExtentOpKind,
    ) -> Result<InflightSnapshot, AppError> {
        let inflight = self.inflight.borrow();
        let record = inflight
            .get(&extent_id)
            .filter(|record| record.kind() == Some(kind))
            .ok_or_else(|| AppError::Precondition("inflight marker changed".into()))?;
        let nonce = self.extent_inflight_nonce(extent_id);
        if self.etcd.is_some() && nonce == 0 {
            return Err(AppError::Precondition(
                "inflight marker has no revision".into(),
            ));
        }
        Ok(InflightSnapshot {
            extent_id,
            nonce,
            record: rkyv_encode(record).to_vec(),
        })
    }

    pub(crate) async fn commit_inflight_txn(
        &self,
        snapshot: &InflightSnapshot,
        mut comparisons: Vec<autumn_etcd::proto::Compare>,
        operations: Vec<autumn_etcd::proto::RequestOp>,
    ) -> Result<(), AppError> {
        #[cfg(test)]
        tests::checkpoint(tests::Stage::BeforeTxn).await?;

        if let Some(etcd) = &self.etcd {
            let key = Self::extent_inflight_key(snapshot.extent_id);
            comparisons.push(Cmp::mod_revision(&key, snapshot.nonce as i64));
            comparisons.push(Cmp::value(&key, &snapshot.record));
            if !etcd.txn_fenced(comparisons, operations, vec![]).await? {
                return Err(AppError::Precondition(
                    "inflight marker or extent changed before commit".into(),
                ));
            }
        }

        #[cfg(test)]
        tests::checkpoint(tests::Stage::AfterTxn).await?;

        if self.extent_inflight_nonce(snapshot.extent_id) != snapshot.nonce
            || !self
                .inflight
                .borrow()
                .get(&snapshot.extent_id)
                .is_some_and(|record| rkyv_encode(record).as_ref() == snapshot.record.as_slice())
        {
            return Err(AppError::Precondition(
                "inflight attempt changed while committing".into(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn commit_inflight_extent(
        &self,
        snapshot: &InflightSnapshot,
        baseline: Vec<u8>,
        updated: ExtentRecord,
        mut extra_operations: Vec<autumn_etcd::proto::RequestOp>,
    ) -> Result<(), AppError> {
        let extent_id = snapshot.extent_id;
        let key = format!("extents/{extent_id}");
        extra_operations.push(Op::put(&key, crate::persist::encode(&updated)));
        extra_operations.push(Op::delete(Self::extent_inflight_key(extent_id)));
        self.commit_inflight_txn(
            snapshot,
            vec![Cmp::value(&key, &baseline)],
            extra_operations,
        )
        .await?;
        let mut store = self.store.inner.borrow_mut();
        if !store
            .extents
            .get(&extent_id)
            .is_some_and(|extent| crate::persist::encode(extent) == baseline)
        {
            drop(store);
            if self.etcd.is_some() {
                self.commit_extent_inflight_release(extent_id);
            }
            return Err(AppError::Precondition(
                "extent changed while committing inflight operation".into(),
            ));
        }
        store.extents.insert(extent_id, updated);
        drop(store);
        self.commit_extent_inflight_release(extent_id);
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests;
