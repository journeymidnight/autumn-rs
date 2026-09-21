use std::cell::RefCell;

use autumn_common::AppError;
use autumn_rpc::manager_rpc::{MgrEcDispatchInflight, RecoveryTask, RecoveryTaskDone};

use crate::extent_inflight::ExtentOpPayload;
use crate::persist::records::{DiskRecord, ExtentRecord, NodeRecord};
use crate::AutumnManager;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stage {
    BeforeTxn,
    AfterTxn,
}

type Hook =
    Box<dyn FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), AppError>>>>>;

thread_local! {
    static HOOK: RefCell<Option<(Stage, Hook)>> = RefCell::new(None);
}

pub(super) async fn checkpoint(stage: Stage) -> Result<(), AppError> {
    let hook = HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.as_ref().is_some_and(|(armed, _)| *armed == stage) {
            slot.take().map(|(_, hook)| hook)
        } else {
            None
        }
    });
    if let Some(hook) = hook {
        hook().await?;
    }
    Ok(())
}

pub(crate) fn arm(stage: Stage, hook: impl FnOnce() -> Result<(), AppError> + 'static) {
    HOOK.with(|slot| {
        assert!(
            slot.borrow().is_none(),
            "an inflight commit hook is already armed"
        );
        *slot.borrow_mut() = Some((stage, Box::new(move || Box::pin(async move { hook() }))));
    });
}

fn extent(extent_id: u64) -> ExtentRecord {
    ExtentRecord {
        extent_id,
        replicates: vec![1, 3, 5],
        parity: vec![],
        eversion: 3,
        refs: 1,
        vp_table_refs: 0,
        sealed_length: 4096,
        sealed: true,
        avali: 0b111,
        replicate_disks: vec![10, 30, 50],
        parity_disks: vec![],
        ec_converted: false,
    }
}

fn replace_attempt(manager: &AutumnManager, extent_id: u64, payload: ExtentOpPayload) {
    let successor_nonce = manager.extent_inflight_nonce(extent_id) + 1;
    let recovery = manager.recovery_attempts.borrow().get(&extent_id).cloned();
    manager.commit_extent_inflight_release(extent_id);
    if let Some(record) = recovery {
        manager
            .recovery_attempts
            .borrow_mut()
            .insert(extent_id, record);
    }
    let record = crate::extent_inflight::MgrExtentInflightRecord::new(
        extent_id,
        payload,
        "successor".to_string(),
    );
    manager.inflight.borrow_mut().insert(extent_id, record);
    manager
        .inflight_attempt_nonce
        .borrow_mut()
        .insert(extent_id, successor_nonce);
}

fn run<F: std::future::Future<Output = T>, T>(future: F) -> T {
    compio::runtime::Runtime::new().unwrap().block_on(future)
}

#[test]
fn recovery_request_delayed_past_same_assignment_reissue_is_refused() {
    run(async {
        let manager = AutumnManager::new();
        manager.store.inner.borrow_mut().nodes.insert(
            9,
            NodeRecord {
                node_id: 9,
                disks: vec![90],
                ..Default::default()
            },
        );
        manager.store.inner.borrow_mut().disks.insert(
            90,
            DiskRecord {
                disk_id: 90,
                online: true,
                ..Default::default()
            },
        );
        let extent_id = 9101;
        manager
            .store
            .inner
            .borrow_mut()
            .extents
            .insert(extent_id, extent(extent_id));
        let task = RecoveryTask {
            extent_id,
            replace_id: 3,
            node_id: 9,
            start_time: 1,
        };
        manager
            .acquire_extent_inflight(extent_id, ExtentOpPayload::Recovery(task.clone()))
            .await
            .unwrap();
        let successor = manager.clone();
        let successor_task = task.clone();
        arm(Stage::BeforeTxn, move || {
            replace_attempt(
                &successor,
                extent_id,
                ExtentOpPayload::Recovery(successor_task),
            );
            Ok(())
        });

        let result = manager
            .apply_recovery_done(RecoveryTaskDone {
                task: task.clone(),
                ready_disk_id: 90,
                attempt: manager.recovery_attempt(extent_id).unwrap(),
            })
            .await;
        assert!(result.is_err(), "the old request must be refused");
        assert_eq!(
            manager
                .extent_inflight_payload_recovery(extent_id)
                .unwrap()
                .node_id,
            task.node_id,
            "the identical successor marker must remain"
        );
        assert_eq!(
            manager.store.inner.borrow().extents[&extent_id].replicates,
            vec![1, 3, 5],
            "the stale completion must not change the layout"
        );
    });
}

#[test]
fn recovery_response_delayed_past_same_assignment_reissue_keeps_successor() {
    run(async {
        let manager = AutumnManager::new();
        manager.store.inner.borrow_mut().nodes.insert(
            9,
            NodeRecord {
                node_id: 9,
                disks: vec![90],
                ..Default::default()
            },
        );
        manager.store.inner.borrow_mut().disks.insert(
            90,
            DiskRecord {
                disk_id: 90,
                online: true,
                ..Default::default()
            },
        );
        let extent_id = 9103;
        manager
            .store
            .inner
            .borrow_mut()
            .extents
            .insert(extent_id, extent(extent_id));
        let task = RecoveryTask {
            extent_id,
            replace_id: 3,
            node_id: 9,
            start_time: 1,
        };
        manager
            .acquire_extent_inflight(extent_id, ExtentOpPayload::Recovery(task.clone()))
            .await
            .unwrap();
        let successor = manager.clone();
        let successor_task = task.clone();
        arm(Stage::AfterTxn, move || {
            replace_attempt(
                &successor,
                extent_id,
                ExtentOpPayload::Recovery(successor_task),
            );
            Ok(())
        });

        let result = manager
            .apply_recovery_done(RecoveryTaskDone {
                task,
                ready_disk_id: 90,
                attempt: manager.recovery_attempt(extent_id).unwrap(),
            })
            .await;
        assert!(result.is_err(), "the delayed old response must be refused");
        assert!(
            manager
                .extent_inflight_payload_recovery(extent_id)
                .is_some(),
            "the successor Recovery marker must remain"
        );
        assert_eq!(
            manager.store.inner.borrow().extents[&extent_id].replicates,
            vec![1, 3, 5],
            "the stale response must not install the old layout in memory"
        );
    });
}

#[test]
fn ec_response_delayed_past_same_assignment_reissue_keeps_successor() {
    run(async {
        let manager = AutumnManager::new();
        manager.store.inner.borrow_mut().nodes.insert(
            9,
            NodeRecord {
                node_id: 9,
                disks: vec![90],
                ..Default::default()
            },
        );
        manager.store.inner.borrow_mut().disks.insert(
            90,
            DiskRecord {
                disk_id: 90,
                online: true,
                ..Default::default()
            },
        );
        let extent_id = 9102;
        manager
            .store
            .inner
            .borrow_mut()
            .extents
            .insert(extent_id, extent(extent_id));
        let params = MgrEcDispatchInflight {
            extent_id,
            target_nodes: vec![1, 3, 5, 7],
            extra_disk_ids: vec![70],
            data_shards: 3,
            new_eversion: 4,
            owner_epoch: 0,
        };
        manager
            .acquire_extent_inflight(extent_id, ExtentOpPayload::ConvertToEc(params.clone()))
            .await
            .unwrap();
        let successor = manager.clone();
        arm(Stage::AfterTxn, move || {
            replace_attempt(&successor, extent_id, ExtentOpPayload::ConvertToEc(params));
            Ok(())
        });

        let result = manager
            .apply_ec_conversion_done(extent_id, vec![1, 3, 5, 7], vec![70], 3, 4)
            .await;
        assert!(
            result.is_err(),
            "the delayed old response must not install memory state"
        );
        assert!(
            manager.extent_inflight_payload_ec(extent_id).is_some(),
            "the identical successor EC marker must remain"
        );
        assert!(
            !manager.store.inner.borrow().extents[&extent_id].ec_converted,
            "the stale response must not publish EC state in memory"
        );
    });
}

#[test]
fn ec_request_delayed_past_same_assignment_reissue_is_refused() {
    run(async {
        let manager = AutumnManager::new();
        manager.store.inner.borrow_mut().nodes.insert(
            9,
            NodeRecord {
                node_id: 9,
                disks: vec![90],
                ..Default::default()
            },
        );
        manager.store.inner.borrow_mut().disks.insert(
            90,
            DiskRecord {
                disk_id: 90,
                online: true,
                ..Default::default()
            },
        );
        let extent_id = 9104;
        manager
            .store
            .inner
            .borrow_mut()
            .extents
            .insert(extent_id, extent(extent_id));
        let params = MgrEcDispatchInflight {
            extent_id,
            target_nodes: vec![1, 3, 5, 7],
            extra_disk_ids: vec![70],
            data_shards: 3,
            new_eversion: 4,
            owner_epoch: 0,
        };
        manager
            .acquire_extent_inflight(extent_id, ExtentOpPayload::ConvertToEc(params.clone()))
            .await
            .unwrap();
        let successor = manager.clone();
        arm(Stage::BeforeTxn, move || {
            replace_attempt(&successor, extent_id, ExtentOpPayload::ConvertToEc(params));
            Ok(())
        });

        let result = manager
            .apply_ec_conversion_done(extent_id, vec![1, 3, 5, 7], vec![70], 3, 4)
            .await;
        assert!(result.is_err(), "the old request must be refused");
        assert!(
            manager.extent_inflight_payload_ec(extent_id).is_some(),
            "the identical successor EC marker must remain"
        );
        assert!(
            !manager.store.inner.borrow().extents[&extent_id].ec_converted,
            "the stale request must not publish EC state"
        );
    });
}

pub(crate) fn arm_async(
    stage: Stage,
    hook: impl std::future::Future<Output = Result<(), AppError>> + 'static,
) {
    HOOK.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some((stage, Box::new(move || Box::pin(hook))));
    });
}
