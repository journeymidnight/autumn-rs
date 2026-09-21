//! Recovery snapshot, separate from the unchanged inflight record encoding.
use autumn_common::AppError;
use autumn_rpc::extent_rpc::{RecoveryAttempt, RequireRecoveryReq};
use autumn_rpc::manager_rpc::{rkyv_decode, rkyv_encode, RecoveryTask};
use rkyv::{Archive, Deserialize, Serialize};

use crate::persist::PersistRecord;
use crate::AutumnManager;

pub(crate) const PREFIX: &str = "recoveryAttempt/";
pub(crate) fn key(extent_id: u64) -> String {
    format!("{PREFIX}{extent_id}")
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub(crate) struct RecoveryRecord {
    pub source_eversion: u64,
    pub slot: u32,
    pub sealed_length: u64,
    pub ec_converted: bool,
    pub payload_location: u8,
    pub target_uuid: String,
    pub target_disks: Vec<(u64, String)>,
}

impl PersistRecord for RecoveryRecord {
    const RECORD_TYPE: u8 = crate::persist::RECORD_TYPE_RECOVERY_ATTEMPT;
    const FORMAT_VERSION: u8 = 1;
    const NAME: &'static str = "recovery attempt";
}

impl From<&RecoveryRecord> for RecoveryAttempt {
    fn from(record: &RecoveryRecord) -> Self {
        let RecoveryRecord {
            source_eversion,
            slot,
            sealed_length,
            ec_converted,
            payload_location,
            target_uuid,
            target_disks,
        } = record;
        Self {
            nonce: 0,
            source_eversion: *source_eversion,
            slot: *slot,
            sealed_length: *sealed_length,
            ec_converted: *ec_converted,
            payload_location: *payload_location,
            target_uuid: target_uuid.clone(),
            target_disks: target_disks.clone(),
        }
    }
}
impl From<RecoveryAttempt> for RecoveryRecord {
    fn from(attempt: RecoveryAttempt) -> Self {
        let RecoveryAttempt {
            nonce: _,
            source_eversion,
            slot,
            sealed_length,
            ec_converted,
            payload_location,
            target_uuid,
            target_disks,
        } = attempt;
        Self {
            source_eversion,
            slot,
            sealed_length,
            ec_converted,
            payload_location,
            target_uuid,
            target_disks,
        }
    }
}

impl AutumnManager {
    pub(crate) fn capture_recovery_attempt(
        &self,
        task: &RecoveryTask,
    ) -> Result<RecoveryRecord, AppError> {
        let s = self.store.inner.borrow();
        let ex = s
            .extents
            .get(&task.extent_id)
            .ok_or_else(|| AppError::NotFound("recovery extent".into()))?;
        let slot = Self::extent_slot(ex, task.replace_id)
            .ok_or_else(|| AppError::Precondition("recovery source slot disappeared".into()))?;
        let node = s
            .nodes
            .get(&task.node_id)
            .ok_or_else(|| AppError::NotFound("recovery target node".into()))?;
        if !ex.sealed
            || Self::extent_nodes(ex).contains(&task.node_id)
            || self.node_overrides.borrow().contains_key(&task.node_id)
            || self.decommissioned.borrow().contains_key(&task.node_id)
            || self
                .node_states
                .borrow()
                .state_of(task.node_id)
                .is_suspected()
        {
            return Err(AppError::Precondition(
                "recovery source or target is unavailable".into(),
            ));
        }
        let disks = node
            .disks
            .iter()
            .filter_map(|id| {
                s.disks
                    .get(id)
                    .filter(|d| d.online)
                    .map(|d| (*id, d.uuid.clone()))
            })
            .collect::<Vec<_>>();
        if disks.is_empty() {
            return Err(AppError::Precondition(
                "recovery target has no online disk".into(),
            ));
        }
        Ok(RecoveryRecord {
            source_eversion: ex.eversion,
            slot: slot as u32,
            sealed_length: ex.sealed_length,
            ec_converted: ex.ec_converted,
            payload_location: self.payload_location_of(task.extent_id) as u8,
            target_uuid: node.node_uuid.clone(),
            target_disks: disks,
        })
    }

    pub(crate) fn recovery_attempt(&self, extent_id: u64) -> Option<RecoveryAttempt> {
        let mut attempt = RecoveryAttempt::from(self.recovery_attempts.borrow().get(&extent_id)?);
        attempt.nonce = self.extent_inflight_nonce(extent_id);
        (attempt.nonce != 0).then_some(attempt)
    }

    pub(crate) fn validate_recovery_attempt(
        &self,
        task: &RecoveryTask,
        attempt: &RecoveryAttempt,
        disk_id: Option<u64>,
    ) -> Result<(), AppError> {
        let pinned = self.extent_inflight_payload_recovery(task.extent_id);
        if attempt.nonce == 0
            || self.recovery_attempt(task.extent_id).as_ref() != Some(attempt)
            || !pinned.is_some_and(|p| rkyv_encode(&p) == rkyv_encode(task))
        {
            return Err(AppError::Precondition("recovery attempt changed".into()));
        }
        let current = RecoveryAttempt::from(&self.capture_recovery_attempt(task)?);
        if current.source_eversion != attempt.source_eversion
            || current.slot != attempt.slot
            || current.sealed_length != attempt.sealed_length
            || current.ec_converted != attempt.ec_converted
            || current.payload_location != attempt.payload_location
            || current.target_uuid != attempt.target_uuid
        {
            return Err(AppError::Precondition(
                "recovery source layout or target identity changed".into(),
            ));
        }
        if let Some(id) = disk_id {
            if !attempt
                .target_disks
                .iter()
                .any(|d| d.0 == id && current.target_disks.contains(d))
            {
                return Err(AppError::Precondition(
                    "recovery target disk identity changed".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) async fn handle_validate_recovery(
        &self,
        payload: bytes::Bytes,
    ) -> Result<bytes::Bytes, (autumn_rpc::StatusCode, String)> {
        let req: RequireRecoveryReq =
            rkyv_decode(&payload).map_err(|e| (autumn_rpc::StatusCode::InvalidArgument, e))?;
        let result = self
            .ensure_leader()
            .and_then(|_| self.validate_recovery_attempt(&req.task, &req.attempt, None));
        let (code, message) = match result {
            Ok(()) => (autumn_rpc::manager_rpc::CODE_OK, String::new()),
            Err(e) => (Self::err_to_code(&e), e.to_string()),
        };
        Ok(rkyv_encode(&autumn_rpc::extent_rpc::CodeResp {
            code,
            message,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extent_inflight::ExtentOpPayload;
    use crate::persist::records::{DiskRecord, ExtentRecord, NodeRecord};
    use autumn_rpc::manager_rpc::*;

    fn fixture() -> (AutumnManager, RecoveryTask) {
        let m = AutumnManager::new();
        let task = RecoveryTask {
            extent_id: 42,
            node_id: 9,
            replace_id: 1,
            start_time: 1,
        };
        {
            let mut s = m.store.inner.borrow_mut();
            s.extents.insert(
                42,
                ExtentRecord {
                    extent_id: 42,
                    replicates: vec![1, 3],
                    replicate_disks: vec![10, 30],
                    sealed: true,
                    sealed_length: 4096,
                    eversion: 3,
                    refs: 1,
                    avali: 3,
                    ..Default::default()
                },
            );
            s.nodes.insert(
                9,
                NodeRecord {
                    node_id: 9,
                    disks: vec![90],
                    node_uuid: "target".into(),
                    ..Default::default()
                },
            );
            s.disks.insert(
                90,
                DiskRecord {
                    disk_id: 90,
                    uuid: "disk".into(),
                    online: true,
                },
            );
        }
        (m, task)
    }
    async fn acquire(m: &AutumnManager, task: &RecoveryTask) -> RecoveryTaskDone {
        m.acquire_extent_inflight(42, ExtentOpPayload::Recovery(task.clone()))
            .await
            .unwrap();
        RecoveryTaskDone {
            task: task.clone(),
            ready_disk_id: 90,
            attempt: m.recovery_attempt(42).unwrap(),
        }
    }
    async fn fence(m: &AutumnManager) {
        let response = m
            .handle_fence_node(rkyv_encode(&FenceNodeReq {
                node_id: 9,
                force: true,
                set_by: "test".into(),
                reason: "retire".into(),
            }))
            .await
            .unwrap();
        assert_eq!(rkyv_decode::<CodeResp>(&response).unwrap().code, CODE_OK);
    }
    async fn remove(m: &AutumnManager) -> RemoveNodeResp {
        rkyv_decode(
            &m.handle_remove_node(rkyv_encode(&RemoveNodeReq {
                node_id: 9,
                set_by: "test".into(),
            }))
            .await
            .unwrap(),
        )
        .unwrap()
    }

    #[compio::test]
    async fn legacy_marker_without_snapshot_is_cancelled_then_reissued() {
        let (m, task) = fixture();
        let old = acquire(&m, &task).await;
        m.recovery_attempts.borrow_mut().remove(&42);
        // The marker bytes still decode; it cannot authorize a completion.
        assert!(m.apply_recovery_done(old).await.is_err());
        m.recovery_dispatch_tick_under(crate::recovery::RecoveryGateMode::FencedOnly)
            .await;
        assert!(m.extent_inflight_payload_recovery(42).is_none());
        let new = acquire(&m, &task).await;
        m.apply_recovery_done(new).await.unwrap();
    }

    #[compio::test]
    async fn same_assignment_old_completion_cannot_apply_or_release_successor() {
        let (m, task) = fixture();
        let a = acquire(&m, &task).await;
        m.drain_extent_inflight_marker(42, "reissue").await.unwrap();
        let b = acquire(&m, &task).await;
        assert_ne!(a.attempt.nonce, b.attempt.nonce);
        assert!(m.apply_recovery_done(a).await.is_err());
        assert_eq!(m.recovery_attempt(42), Some(b.attempt.clone()));
        assert_eq!(m.store.inner.borrow().extents[&42].replicates, vec![1, 3]);
        m.apply_recovery_done(b).await.unwrap();
        assert_eq!(m.store.inner.borrow().extents[&42].replicates, vec![9, 3]);
    }

    #[compio::test]
    async fn conversion_and_disk_identity_changes_refuse_old_results() {
        let (m, task) = fixture();
        let a = acquire(&m, &task).await;
        m.drain_extent_inflight_marker(42, "convert").await.unwrap();
        {
            let mut s = m.store.inner.borrow_mut();
            let ex = s.extents.get_mut(&42).unwrap();
            ex.ec_converted = true;
            ex.eversion += 1;
        }
        m.commit_payload_location(42, autumn_rpc::extent_rpc::PayloadLocation::InShardFile);
        let b = acquire(&m, &task).await;
        assert!(m.apply_recovery_done(a).await.is_err());
        assert_eq!(m.recovery_attempt(42), Some(b.attempt.clone()));
        m.store.inner.borrow_mut().disks.get_mut(&90).unwrap().uuid = "replacement-disk".into();
        assert!(m.apply_recovery_done(b).await.is_err());
        assert_eq!(m.store.inner.borrow().extents[&42].replicates, vec![1, 3]);
    }

    #[compio::test]
    async fn fence_remove_before_received_completion_never_resurrects_target() {
        let (m, task) = fixture();
        let done = acquire(&m, &task).await;
        fence(&m).await;
        assert!(m.recovery_attempt(42).is_none());
        assert_eq!(remove(&m).await.code, CODE_OK);
        m.apply_recovery_done(done).await.unwrap();
        assert!(!m.store.inner.borrow().nodes.contains_key(&9));
        assert!(!m.store.inner.borrow().disks.contains_key(&90));
        assert_eq!(m.store.inner.borrow().extents[&42].replicates, vec![1, 3]);
    }

    #[compio::test]
    async fn failed_fence_cancellation_keeps_remove_blocker() {
        use crate::inflight_commit::tests::{arm, Stage};
        let (m, task) = fixture();
        let done = acquire(&m, &task).await;
        arm(Stage::BeforeTxn, || {
            Err(AppError::Internal("etcd unavailable".into()))
        });
        fence(&m).await;
        let response = remove(&m).await;
        assert_eq!(response.code, CODE_PRECONDITION);
        assert_eq!(response.blocking_marker_extent_ids, vec![42]);
        assert!(m.apply_recovery_done(done).await.is_err());
        assert_eq!(remove(&m).await.code, CODE_OK);
    }

    #[compio::test]
    async fn completion_in_commit_serializes_fence_and_remove() {
        use crate::inflight_commit::tests::{arm_async, Stage};
        use futures::{channel::oneshot, FutureExt};
        for stage in [Stage::BeforeTxn, Stage::AfterTxn] {
            let (m, task) = fixture();
            let done = acquire(&m, &task).await;
            let (entered_tx, entered_rx) = oneshot::channel();
            let (resume_tx, resume_rx) = oneshot::channel();
            arm_async(stage, async move {
                entered_tx.send(()).unwrap();
                resume_rx.await.unwrap();
                Ok(())
            });
            let applying = m.clone();
            let apply =
                compio::runtime::spawn(async move { applying.apply_recovery_done(done).await });
            entered_rx.await.unwrap();
            let fencing = m.clone();
            let (fenced_tx, mut fenced_rx) = oneshot::channel();
            compio::runtime::spawn(async move {
                fence(&fencing).await;
                fenced_tx.send(()).unwrap();
            })
            .detach();
            compio::time::sleep(std::time::Duration::from_millis(10)).await;
            assert!(
                (&mut fenced_rx).now_or_never().is_none(),
                "Fence must wait for the committing membership update"
            );
            resume_tx.send(()).unwrap();
            apply.await.unwrap().unwrap();
            fenced_rx.await.unwrap();
            let response = remove(&m).await;
            assert_eq!(response.blocking_extent_ids, vec![42]);
            assert!(m.store.inner.borrow().nodes.contains_key(&9));
            assert!(m.store.inner.borrow().disks.contains_key(&90));
        }
    }
}

#[cfg(test)]
mod encoding_tests {
    use super::*;
    #[test]
    fn recovery_snapshot_encoding_is_frozen() {
        let record = RecoveryRecord {
            source_eversion: 0x1112131415161718,
            slot: 0x21222324,
            sealed_length: 0x3132333435363738,
            ec_converted: true,
            payload_location: 1,
            target_uuid: "recovery-target".into(),
            target_disks: vec![(0x4142434445464748, "recovery-disk".into())],
        };
        let bytes = crate::persist::encode(&record);
        let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, "41554d470a017265636f766572792d7461726765747265636f766572792d6469736b0000000048474645444342418d000000e7ffffff181716151413121124232221000000003837363534333231010100008f000000b4ffffffccffffff0100000000000000");
        let back = crate::persist::decode::<RecoveryRecord>("recoveryAttempt/42", &bytes).unwrap();
        assert_eq!(RecoveryAttempt::from(&back), RecoveryAttempt::from(&record));
    }
}
