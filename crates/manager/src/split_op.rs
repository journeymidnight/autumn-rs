//! A submitted split whose PS reply never came.
//!
//! The PS runs a split on its own task, so the manager giving up on the reply
//! cancels nothing: the split can still commit. Such an op stays RUNNING in
//! `unknown_splits` until a fact ends it:
//! - the commit (`handle_multi_modify_split` names the op) → SUCCEEDED;
//! - the PS's failure on the load report (`reconcile_outcome`) → FAILED;
//! - the partition reopened (its owner epoch moved), so the split can no
//!   longer pass `ensure_owner_epoch` → FAILED;
//! - no load report has named the op for `SPLIT_SILENCE_SECS` (the request
//!   never reached a handler, or the PS lost it) → FAILED.
//!
//! The last two are verdicts, not observations, and the commit fence makes
//! them true: `handle_multi_modify_split` refuses an op the ledger has ended.

use autumn_rpc::manager_rpc::{MgrAuditEntry, OP_KIND_SPLIT, OP_STATE_FAILED, OP_STATE_SUCCEEDED};

use crate::AutumnManager;

/// Load reports come every 5 s and name a running split in its phase sample.
pub(crate) const SPLIT_SILENCE_SECS: i64 = 30;

/// A dispatched split whose reply did not arrive (timeout, dropped
/// connection). The request may or may not have reached the PS.
#[derive(Debug)]
pub(crate) struct SplitReplyLost(pub String);

impl std::fmt::Display for SplitReplyLost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SplitReplyLost {}

pub(crate) struct UnknownSplit {
    pub part_id: u64,
    /// `partition/<id>` epoch when the split was dispatched; `None` = no owner.
    pub owner_epoch: Option<i64>,
    pub last_heard_s: i64,
}

pub(crate) fn owner_epoch_of(state: &crate::store::MetadataState, part_id: u64) -> Option<i64> {
    state
        .owner_epochs
        .get(&format!("partition/{part_id}"))
        .copied()
}

impl AutumnManager {
    /// The reply is lost. Returns the message for the still-RUNNING op, or
    /// `None` when something already ended it (the commit can land before the
    /// reply times out).
    pub(crate) fn split_outcome_unknown(
        &self,
        op_id: u64,
        part_id: u64,
        owner_epoch: Option<i64>,
        why: &str,
        now_s: i64,
    ) -> Option<String> {
        if !self.ops.borrow().is_active_op(op_id) {
            return None;
        }
        self.unknown_splits.borrow_mut().insert(
            op_id,
            UnknownSplit {
                part_id,
                owner_epoch,
                last_heard_s: now_s,
            },
        );
        Some(format!(
            "outcome unknown ({why}); waiting for the split to commit, fail, or go \
             unreported for {SPLIT_SILENCE_SECS} s"
        ))
    }

    /// A load report named the op (its phase or its outcome).
    pub(crate) fn note_split_heard(&self, op_id: u64, now_s: i64) {
        if let Some(u) = self.unknown_splits.borrow_mut().get_mut(&op_id) {
            u.last_heard_s = now_s;
        }
    }

    /// The commit of a split serving `op_id`. Must run with no await between
    /// the etcd commit and here, so no verdict can land in between.
    pub(crate) fn end_split_op_committed(&self, op_id: u64, left: u64, right: u64, now_s: i64) {
        self.unknown_splits.borrow_mut().remove(&op_id);
        let ended = self.ops.borrow_mut().finish(
            op_id,
            OP_STATE_SUCCEEDED,
            String::new(),
            format!("split part {left} in two (new part {right})"),
            now_s,
        );
        if ended {
            tracing::info!(op_id, part_id = left, right, "op succeeded (split committed)");
            // Off the commit path: the PS is waiting on this reply under its
            // freeze budget.
            let mgr = self.clone();
            compio::runtime::spawn(async move { mgr.audit_split_end(op_id).await }).detach();
        }
    }

    /// End every unknown split a fact has settled. Skips a partition whose
    /// commit is in flight: that commit decides.
    pub(crate) async fn reconcile_unknown_splits(&self, now_s: i64) {
        let mut ended: Vec<(u64, Option<String>)> = Vec::new();
        {
            let unknown = self.unknown_splits.borrow();
            if unknown.is_empty() {
                return;
            }
            let ops = self.ops.borrow();
            let committing = self.split_inflight.borrow();
            let state = self.store.inner.borrow();
            for (&op_id, u) in unknown.iter() {
                if !ops.is_active_op(op_id) {
                    ended.push((op_id, None));
                } else if committing.contains(&u.part_id) {
                } else if owner_epoch_of(&state, u.part_id) != u.owner_epoch {
                    ended.push((
                        op_id,
                        Some(format!(
                            "partition {} was reopened (owner epoch {:?} -> {:?}) before the \
                             split committed; it can no longer commit",
                            u.part_id,
                            u.owner_epoch,
                            owner_epoch_of(&state, u.part_id)
                        )),
                    ));
                } else if now_s - u.last_heard_s > SPLIT_SILENCE_SECS {
                    ended.push((
                        op_id,
                        Some(format!(
                            "no reply, and no load report from partition {} has named this \
                             split for {SPLIT_SILENCE_SECS} s; it is not running and will not \
                             be committed",
                            u.part_id
                        )),
                    ));
                }
            }
        }
        // Every verdict lands before the first await: a commit passing the
        // fence in that gap would contradict a verdict applied after it.
        let mut failed = Vec::new();
        for (op_id, why) in ended {
            self.unknown_splits.borrow_mut().remove(&op_id);
            let Some(why) = why else { continue };
            if self
                .ops
                .borrow_mut()
                .finish(op_id, OP_STATE_FAILED, why.clone(), String::new(), now_s)
            {
                tracing::warn!(op_id, error = %why, "op FAILED (split outcome settled)");
                failed.push(op_id);
            }
        }
        for op_id in failed {
            self.audit_split_end(op_id).await;
        }
    }

    async fn audit_split_end(&self, op_id: u64) {
        let Some(rec) = self.ops.borrow().record(op_id) else {
            return;
        };
        self.append_audit(MgrAuditEntry {
            op: crate::op_kind_audit_code(OP_KIND_SPLIT),
            node_id: rec.part_id,
            extent_id: 0,
            by: if rec.requested_by.is_empty() {
                "cli".to_string()
            } else {
                rec.requested_by
            },
            reason: String::new(),
            result_code: if rec.state == OP_STATE_SUCCEEDED { 0 } else { 1 },
            result_message: if rec.error.is_empty() {
                rec.message
            } else {
                rec.error
            },
            ts_ns: 0,
        })
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_rpc::manager_rpc::{
        rkyv_decode, rkyv_encode, CodeResp, MaintenanceOutcome, MultiModifySplitReq,
        PartitionLoad, ReportPartitionLoadReq, CODE_PRECONDITION, OP_STATE_RUNNING,
    };

    fn run<F: std::future::Future<Output = T>, T>(f: F) -> T {
        compio::runtime::Runtime::new().unwrap().block_on(f)
    }

    /// A submitted split on `part_id` whose reply was lost at `now`.
    fn unknown_split(m: &AutumnManager, part_id: u64, epoch: i64, now: i64) -> u64 {
        m.store
            .inner
            .borrow_mut()
            .owner_epochs
            .insert(format!("partition/{part_id}"), epoch);
        let (op_id, _) = m
            .ops
            .borrow_mut()
            .submit(OP_KIND_SPLIT, part_id, 0, vec![], "test".into(), 0, 0);
        m.ops.borrow_mut().set_running(op_id, now);
        assert!(m
            .split_outcome_unknown(op_id, part_id, Some(epoch), "RPC timed out", now)
            .is_some());
        op_id
    }

    fn state(m: &AutumnManager, op_id: u64) -> u8 {
        m.ops.borrow().record(op_id).unwrap().state
    }

    #[test]
    fn a_reopened_partition_ends_its_unknown_split_unless_a_commit_is_in_flight() {
        let m = AutumnManager::new();
        let op = unknown_split(&m, 5, 7, 100);
        m.store
            .inner
            .borrow_mut()
            .owner_epochs
            .insert("partition/5".into(), 8);
        m.split_inflight.borrow_mut().insert(5);
        run(m.reconcile_unknown_splits(101));
        assert_eq!(state(&m, op), OP_STATE_RUNNING, "the in-flight commit decides");
        m.split_inflight.borrow_mut().remove(&5);
        run(m.reconcile_unknown_splits(102));
        assert_eq!(state(&m, op), OP_STATE_FAILED);
        assert!(m.ops.borrow().record(op).unwrap().error.contains("reopened"));
        assert!(m.unknown_splits.borrow().is_empty());
    }

    #[test]
    fn an_unknown_split_no_report_names_ends_failed_after_the_silence() {
        let m = AutumnManager::new();
        let op = unknown_split(&m, 5, 7, 100);
        m.note_split_heard(op, 100 + SPLIT_SILENCE_SECS);
        run(m.reconcile_unknown_splits(100 + SPLIT_SILENCE_SECS + 1));
        assert_eq!(state(&m, op), OP_STATE_RUNNING, "named by a report: still running");
        run(m.reconcile_unknown_splits(100 + 2 * SPLIT_SILENCE_SECS + 1));
        assert_eq!(state(&m, op), OP_STATE_FAILED);
    }

    #[test]
    fn an_ended_split_op_is_refused_at_commit() {
        let m = AutumnManager::new();
        let op = unknown_split(&m, 5, 7, 100);
        run(m.reconcile_unknown_splits(100 + SPLIT_SILENCE_SECS + 1));
        assert_eq!(state(&m, op), OP_STATE_FAILED);
        let resp = run(m.handle_multi_modify_split(rkyv_encode(&MultiModifySplitReq {
            part_id: 5,
            owner_key: String::new(),
            owner_epoch: 0,
            mid_key: b"m".to_vec(),
            log_stream_sealed_length: 0,
            row_stream_sealed_length: 0,
            meta_stream_sealed_length: 0,
            log_tail_extent_id: 0,
            row_tail_extent_id: 0,
            meta_tail_extent_id: 0,
            op_id: op,
        })))
        .unwrap();
        let r: CodeResp = rkyv_decode(&resp).unwrap();
        assert_eq!(r.code, CODE_PRECONDITION);
        assert!(r.message.contains("already ended"), "{}", r.message);
    }

    #[test]
    fn a_split_failure_report_waits_for_an_in_flight_commit() {
        let m = AutumnManager::new();
        let op = unknown_split(&m, 5, 7, 100);
        let report = || {
            rkyv_encode(&ReportPartitionLoadReq {
                ps_id: 1,
                partitions: vec![PartitionLoad {
                    part_id: 5,
                    maintenance_outcomes: vec![MaintenanceOutcome {
                        op_id: op,
                        kind: OP_KIND_SPLIT,
                        state: OP_STATE_FAILED,
                        error: "gave up waiting on the commit".into(),
                        message: String::new(),
                        finished_at: 0,
                    }],
                    ..Default::default()
                }],
            })
        };
        m.split_inflight.borrow_mut().insert(5);
        run(m.handle_report_partition_load(report())).unwrap();
        assert_eq!(state(&m, op), OP_STATE_RUNNING);
        m.split_inflight.borrow_mut().remove(&5);
        run(m.handle_report_partition_load(report())).unwrap();
        assert_eq!(state(&m, op), OP_STATE_FAILED);
    }
}
