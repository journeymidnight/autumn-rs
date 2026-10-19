//! Phase 0 — unified extent-level in-flight ledger.
//!
//! Replaces (across three migration phases) the four scattered
//! inflight bookkeeping mechanisms:
//!
//! | Prior mechanism                                             | Persistence            |
//! |-------------------------------------------------------------|------------------------|
//! | `ec_conversion_inflight: HashSet<u64>`                      | in-memory              |
//! | `pending_ec_dispatch: HashMap<u64, MgrEcDispatchInflight>`  | etcd `ecConversionInflight/<id>` |
//! | `recovery_tasks: HashMap<u64, RecoveryTask>`             | etcd `recoveryTasks/<id>` |
//! | `pending_extent_deletes: VecDeque<PendingDelete>`           | in-memory              |
//!
//! Phase 0 (this commit) adds the infrastructure types + acquire / release /
//! probe API + `replay_from_etcd` arm. **No existing handler is migrated
//! yet.** The new field is dormant; existing tests pass unchanged. Phase 1
//! migrates EC convert, Phase 2 recovery + delete, Phase 3 cleanup.
//!
//! See `~/.claude/plans/stream-merge-split-ps-sorted-dijkstra.md` for the
//! full plan and PS-layer ↔ stream-layer interaction model.

use autumn_etcd::Cmp;
use autumn_rpc::manager_rpc::{rkyv_decode, rkyv_encode, MgrEcDispatchInflight, RecoveryTask};
use rkyv::{Archive, Deserialize, Serialize};

use autumn_common::error::AppError;

use crate::AutumnManager;

/// Etcd key prefix for the unified ledger. Note the snake_case spelling —
/// the older `ecConversionInflight/` and `recoveryTasks/` prefixes use
/// camelCase, but those will be drained one-cycle-after-deploy and removed
/// later. The new prefix matches our other snake_case prefixes
/// (`partitionVpRefs/` is the camelCase outlier; consistency was already
/// imperfect — pick whichever).
pub const EXTENT_INFLIGHT_PREFIX: &str = "extent_inflight/";

/// Discriminator for the kind of stream-layer op currently in flight on an
/// extent. Returned by the `extent_inflight_op` read-only probe so callers
/// can render a useful refuse-at-start error message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExtentOpKind {
    ConvertToEc,
    Recovery,
    Delete,
}

impl ExtentOpKind {
    const CONVERT_TO_EC: u8 = 1;
    const RECOVERY: u8 = 2;
    const DELETE: u8 = 3;

    fn as_byte(self) -> u8 {
        match self {
            Self::ConvertToEc => Self::CONVERT_TO_EC,
            Self::Recovery => Self::RECOVERY,
            Self::Delete => Self::DELETE,
        }
    }

    fn from_byte(b: u8) -> Option<Self> {
        match b {
            Self::CONVERT_TO_EC => Some(Self::ConvertToEc),
            Self::RECOVERY => Some(Self::Recovery),
            Self::DELETE => Some(Self::Delete),
            _ => None,
        }
    }
}

/// On-disk shape of a pending delete record. Mirrors
/// `crate::extent_delete::PendingDelete` but drops the in-memory `attempts`
/// retry counter — retries reset on leader failover, which is correct (a
/// new leader's first attempt is its own "attempt 1"). Used as a variant
/// payload in `MgrExtentInflightRecord`.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct PersistedPendingDelete {
    pub extent_id: u64,
    pub pending_targets: Vec<crate::extent_delete::DeleteTarget>,
}

/// Full in-flight record. One per extent_id in the `extent_inflight/`
/// etcd prefix and in `AutumnManager.inflight`. The `op_kind` byte
/// + exactly-one-of-three optional payloads is structurally equivalent
/// to a tagged union; we use this flat shape rather than an rkyv enum
/// because the surrounding codebase (`crates/rpc/src/manager_rpc.rs`)
/// has zero rkyv-derived enums and we don't want this commit to be the
/// proving ground for rkyv 0.8 enum CheckBytes support.
///
/// Invariant: `op_kind` matches exactly one Some-valued payload field;
/// the others are None. Enforced by the `MgrExtentInflightRecord::new`
/// constructor and by `rkyv_decode` validity-checks at read time
/// (`unpack` returns `None` on a malformed combination).
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct MgrExtentInflightRecord {
    pub extent_id: u64,
    /// 1=ConvertToEc, 2=Recovery, 3=Delete.
    pub op_kind: u8,
    /// Unix epoch seconds when the marker was acquired. Used by future
    /// stale-marker WARN sweep.
    pub started_at: i64,
    /// `instance_id` of the leader that acquired the marker. Parity with
    /// the leader-fence pattern; useful for diagnostics.
    pub leader_id: String,
    pub ec_payload: Option<MgrEcDispatchInflight>,
    pub recovery_payload: Option<RecoveryTask>,
    pub delete_payload: Option<PersistedPendingDelete>,
}

/// Typed view over a `MgrExtentInflightRecord`. Constructors enforce the
/// "exactly one variant populated" invariant; `op_kind()` derives the
/// discriminator from the variant rather than relying on the byte field.
#[derive(Clone, Debug)]
pub enum ExtentOpPayload {
    ConvertToEc(MgrEcDispatchInflight),
    Recovery(RecoveryTask),
    Delete(PersistedPendingDelete),
}

impl ExtentOpPayload {
    pub fn kind(&self) -> ExtentOpKind {
        match self {
            Self::ConvertToEc(_) => ExtentOpKind::ConvertToEc,
            Self::Recovery(_) => ExtentOpKind::Recovery,
            Self::Delete(_) => ExtentOpKind::Delete,
        }
    }
}

impl MgrExtentInflightRecord {
    pub(crate) fn new(extent_id: u64, payload: ExtentOpPayload, leader_id: String) -> Self {
        let op_kind = payload.kind().as_byte();
        let started_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let (ec_payload, recovery_payload, delete_payload) = match payload {
            ExtentOpPayload::ConvertToEc(p) => (Some(p), None, None),
            ExtentOpPayload::Recovery(p) => (None, Some(p), None),
            ExtentOpPayload::Delete(p) => (None, None, Some(p)),
        };
        Self {
            extent_id,
            op_kind,
            started_at,
            leader_id,
            ec_payload,
            recovery_payload,
            delete_payload,
        }
    }

    /// Recover the typed `(kind, payload)` from a record, validating the
    /// flat-struct invariant. Returns `None` if `op_kind` and the
    /// populated payload field disagree (malformed record — caller should
    /// log and skip on replay).
    pub(crate) fn unpack(&self) -> Option<(ExtentOpKind, ExtentOpPayload)> {
        let kind = ExtentOpKind::from_byte(self.op_kind)?;
        match kind {
            ExtentOpKind::ConvertToEc => self
                .ec_payload
                .clone()
                .filter(|_| self.recovery_payload.is_none() && self.delete_payload.is_none())
                .map(|p| (kind, ExtentOpPayload::ConvertToEc(p))),
            ExtentOpKind::Recovery => self
                .recovery_payload
                .clone()
                .filter(|_| self.ec_payload.is_none() && self.delete_payload.is_none())
                .map(|p| (kind, ExtentOpPayload::Recovery(p))),
            ExtentOpKind::Delete => self
                .delete_payload
                .clone()
                .filter(|_| self.ec_payload.is_none() && self.recovery_payload.is_none())
                .map(|p| (kind, ExtentOpPayload::Delete(p))),
        }
    }

    /// Cheap kind probe without cloning the payload. `None` if `op_kind`
    /// is an unknown discriminator byte (replay-time defense; in-memory
    /// records always have valid op_kind by construction).
    pub(crate) fn kind(&self) -> Option<ExtentOpKind> {
        ExtentOpKind::from_byte(self.op_kind)
    }
}

// Phase 0: API is wired but not yet called by any production handler.
// Phase 1 migrates EC convert; Phase 2 migrates
// recovery + delete. Until those land, the methods are exercised only by
// the unit tests below.
#[allow(dead_code)]
impl AutumnManager {
    /// Compose the etcd key for a given extent_id.
    pub fn extent_inflight_key(extent_id: u64) -> String {
        format!("{}{}", EXTENT_INFLIGHT_PREFIX, extent_id)
    }

    /// Atomically take exclusive in-flight on `extent_id`. CAS via etcd
    /// `create_revision == 0` + leader fence (`txn_fenced`).
    ///
    /// Returns `Err(Precondition)` if any op is already in-flight (either
    /// the etcd CAS refused because the key exists, or the in-memory shadow
    /// already shows it). Returns `Err(NotLeader)` if the leader fence
    /// broke during the CAS.
    ///
    /// In-memory mode (no etcd): the in-memory shadow alone is consulted;
    /// useful for unit tests.
    ///
    /// **Phase 0 note:** this function is NOT yet called by any production
    /// handler. It's added now so later phases only swap call sites.
    pub async fn acquire_extent_inflight(
        &self,
        extent_id: u64,
        payload: ExtentOpPayload,
    ) -> Result<(), AppError> {
        let _lifecycle = self.node_lifecycle_lock.lock().await;
        if self.inflight.borrow().contains_key(&extent_id) {
            return Err(AppError::Precondition(format!(
                "extent {extent_id} already has an in-flight op (in-memory)"
            )));
        }

        if let ExtentOpPayload::ConvertToEc(params) = &payload {
            let classes = self.classes();
            let why = {
                let s = self.store.inner.borrow();
                crate::recovery::ec_target_disks(
                    s.extents.get(&extent_id),
                    &params.target_nodes,
                    &params.extra_disk_ids,
                )
                    .into_iter()
                    .find_map(|(n, d)| crate::recovery::abnormal_target(&classes, n, d))
            };
            if let Some(why) = why {
                return Err(AppError::Precondition(format!("EC target: {why}")));
            }
            // The coordinator encodes from its OWN copy when that copy is full
            // length, and a corrupt-marked copy is full length. Rebuild the
            // marked slot first; the conversion is proposed again after.
            if self.corrupt_slots_of(extent_id) != 0 {
                return Err(AppError::Precondition(format!(
                    "extent {extent_id} has a corrupt-marked slot; it is rebuilt before any \
                     EC conversion"
                )));
            }
        }
        let recovery = match &payload {
            ExtentOpPayload::Recovery(task) => Some(self.capture_recovery_attempt(task)?),
            _ => None,
        };
        let record = MgrExtentInflightRecord::new(extent_id, payload, (*self.instance_id).clone());

        // The attempt nonce is this creation's etcd revision, so it must come
        // from the acquiring txn itself (see `txn_fenced_revision`). In memory
        // mode there are no revisions; the store's monotonic counter stands in.
        let nonce = if let Some(etcd) = &self.etcd {
            let key = Self::extent_inflight_key(extent_id);
            let value = rkyv_encode(&record).to_vec();
            let extra_cmp = vec![Cmp::create_revision(key.as_bytes(), 0)];
            let put_op = autumn_etcd::Op::put(key.as_bytes(), &value);
            let mut puts = vec![put_op];
            if let Some(record) = &recovery {
                puts.push(autumn_etcd::Op::put(
                    crate::recovery_attempt::key(extent_id),
                    crate::persist::encode(record),
                ));
            }
            match etcd.txn_fenced_revision(extra_cmp, puts).await? {
                Some(rev) => rev as u64,
                None => {
                    return Err(AppError::Precondition(format!(
                        "extent {extent_id} already has an in-flight op (etcd CAS refused)"
                    )))
                }
            }
        } else {
            let mut s = self.store.inner.borrow_mut();
            s.next_revision += 1;
            s.next_revision as u64
        };

        if let Some(recovery) = recovery {
            self.recovery_attempts
                .borrow_mut()
                .insert(extent_id, recovery);
        }
        self.inflight.borrow_mut().insert(extent_id, record);
        self.inflight_attempt_nonce
            .borrow_mut()
            .insert(extent_id, nonce);
        Ok(())
    }

    /// Identity of the attempt currently in flight on `extent_id`; `0` when
    /// there is no marker or it predates nonces. See
    /// `AutumnManager::inflight_attempt_nonce`.
    pub(crate) fn extent_inflight_nonce(&self, extent_id: u64) -> u64 {
        self.inflight_attempt_nonce
            .borrow()
            .get(&extent_id)
            .copied()
            .unwrap_or(0)
    }

    /// In-memory release of the inflight marker. Call AFTER the apply-done
    /// etcd txn (which deletes `extent_inflight_key(extent_id)`) has
    /// succeeded (etcd-first invariant per `crates/manager/CLAUDE.md`
    /// programming note 1).
    pub(crate) fn commit_extent_inflight_release(&self, extent_id: u64) {
        self.recovery_attempts.borrow_mut().remove(&extent_id);
        self.inflight.borrow_mut().remove(&extent_id);
        self.inflight_attempt_nonce.borrow_mut().remove(&extent_id);
    }

    /// Read-only probe — returns the op kind currently in flight on
    /// `extent_id`, or `None` if none. Single-line replacement for the 9
    /// scattered "ec_inflight.contains || recovery_tasks.contains_key ||
    /// pending_deletes…" checks across `rpc_handlers.rs`.
    pub(crate) fn extent_inflight_op(&self, extent_id: u64) -> Option<ExtentOpKind> {
        self.inflight
            .borrow()
            .get(&extent_id)
            .and_then(|r| r.kind())
    }

    /// helper for PS-layer handlers that iterate over many
    /// extent_ids and need to consult the ledger multiple times under a
    /// `store.inner.borrow_mut`. Snapshots the ConvertToEc-kind and
    /// Recovery-kind sets in one ledger borrow. Replaces the prior
    /// `(let ec_inflight = self.ec_conversion_inflight.borrow(); let
    /// recovery_inflight = self.recovery_tasks.borrow())` pair.
    /// The PINNED `ConvertToEc` parameters for `extent_id`, if a marker is held.
    /// This is the authoritative assignment (target nodes / disks / shape /
    /// eversion) chosen at the ORIGINAL dispatch and persisted to etcd — every
    /// re-dispatch and the completion-apply MUST use it rather than recomputing,
    /// or a second assignment could be written over the first one's shard bytes.
    pub(crate) fn extent_inflight_payload_ec(
        &self,
        extent_id: u64,
    ) -> Option<autumn_rpc::manager_rpc::MgrEcDispatchInflight> {
        match self
            .inflight
            .borrow()
            .get(&extent_id)
            .and_then(|r| r.unpack())
        {
            Some((_, ExtentOpPayload::ConvertToEc(p))) => Some(p),
            _ => None,
        }
    }

    /// The PINNED recovery task for `extent_id`, if a Recovery marker is held.
    /// The marker names its executor (`task.node_id`), so a re-dispatch goes back
    /// to that node rather than re-running candidate selection — the assignment
    /// is decided once, at acquire time.
    pub(crate) fn extent_inflight_payload_recovery(
        &self,
        extent_id: u64,
    ) -> Option<autumn_rpc::manager_rpc::RecoveryTask> {
        match self
            .inflight
            .borrow()
            .get(&extent_id)
            .and_then(|r| r.unpack())
        {
            Some((_, ExtentOpPayload::Recovery(t))) => Some(t),
            _ => None,
        }
    }

    /// The EC and Recovery markers with each one's `started_at` — the ledger
    /// seeds replayed entries from it so a leader change does not reset how
    /// long the work has been running.
    pub(crate) fn inflight_started_at_ec_recovery(&self) -> (Vec<(u64, i64)>, Vec<(u64, i64)>) {
        let mut ec = Vec::new();
        let mut rec = Vec::new();
        for (id, r) in self.inflight.borrow().iter() {
            match r.kind() {
                Some(ExtentOpKind::ConvertToEc) => ec.push((*id, r.started_at)),
                Some(ExtentOpKind::Recovery) => rec.push((*id, r.started_at)),
                _ => {}
            }
        }
        (ec, rec)
    }

    #[cfg(test)]
    pub(crate) async fn _test_acquire_marker(
        &self,
        extent_id: u64,
        payload: ExtentOpPayload,
    ) -> Result<(), AppError> {
        if self.inflight.borrow().contains_key(&extent_id) {
            return Err(AppError::Precondition("test marker already exists".into()));
        }
        match payload {
            ExtentOpPayload::Recovery(task) => {
                self._test_mark_recovery_inflight(extent_id, task);
                Ok(())
            }
            other => self.acquire_extent_inflight(extent_id, other).await,
        }
    }

    /// Test-only convenience: simulate that `extent_id` is in flight with
    /// a ConvertToEc op. Replaces today's
    /// `m.ec_conversion_inflight.borrow_mut().insert(extent_id)` pattern in
    /// / mark_extent_available / split / etc. tests. Bypasses etcd
    /// CAS and the leader fence for direct in-memory state injection.
    #[cfg(test)]
    pub(crate) fn _test_mark_ec_inflight(&self, extent_id: u64) {
        self._test_mark_ec_inflight_by(extent_id, vec![]);
    }

    /// `_test_mark_ec_inflight` with a coordinator (`target_nodes[0]`). With
    /// none, the dead-executor release drops the marker on the next recovery
    /// tick; a registered node that never answered (`Suspend`) keeps it and is
    /// never dispatched to.
    #[doc(hidden)]
    pub fn _test_mark_ec_inflight_by(&self, extent_id: u64, target_nodes: Vec<u64>) {
        let payload = ExtentOpPayload::ConvertToEc(MgrEcDispatchInflight {
            extent_id,
            target_nodes,
            extra_disk_ids: vec![],
            data_shards: 0,
            new_eversion: 0,
            owner_epoch: 0,
        });
        let record = MgrExtentInflightRecord::new(extent_id, payload, "test".to_string());
        self.inflight.borrow_mut().insert(extent_id, record);
    }

    /// Test-only convenience: clear an in-flight marker (any op kind) on
    /// `extent_id`. Replaces today's
    /// `m.ec_conversion_inflight.borrow_mut().remove(&extent_id)` pattern.
    #[doc(hidden)]
    pub fn _test_clear_inflight(&self, extent_id: u64) {
        self.inflight.borrow_mut().remove(&extent_id);
    }

    /// test-only convenience for the Recovery op kind. Mirrors
    /// `_test_mark_ec_inflight` but inserts a Recovery payload.
    #[cfg(test)]
    pub(crate) fn _test_mark_recovery_inflight(&self, extent_id: u64, task: RecoveryTask) {
        let mut attempt = self
            .capture_recovery_attempt(&task)
            .map(|r| autumn_rpc::extent_rpc::RecoveryAttempt::from(&r))
            .unwrap_or_default();
        // Some ledger tests inject an already-stale marker. Describe the
        // source as far as the fixture allows without changing store state.
        if let Some(ex) = self.store.inner.borrow().extents.get(&extent_id) {
            attempt.source_eversion = ex.eversion;
            attempt.sealed_length = ex.sealed_length;
            attempt.ec_converted = ex.ec_converted;
            attempt.slot = Self::extent_slot(ex, task.replace_id).unwrap_or(0) as u32;
            attempt.payload_location = self.payload_location_of(extent_id) as u8;
        }
        let nonce = {
            let mut s = self.store.inner.borrow_mut();
            s.next_revision += 1;
            s.next_revision as u64
        };
        self.inflight_attempt_nonce
            .borrow_mut()
            .insert(extent_id, nonce);
        self.recovery_attempts.borrow_mut().insert(
            extent_id,
            crate::recovery_attempt::RecoveryRecord::from(attempt),
        );
        let payload = ExtentOpPayload::Recovery(task);
        let record = MgrExtentInflightRecord::new(extent_id, payload, "test".to_string());
        self.inflight.borrow_mut().insert(extent_id, record);
    }

    /// test-only convenience for the Delete op kind.
    #[cfg(test)]
    pub(crate) fn _test_mark_delete_inflight(&self, extent_id: u64, pending_addrs: Vec<String>) {
        let pending_targets: Vec<crate::extent_delete::DeleteTarget> = pending_addrs
            .into_iter()
            .map(|addr| crate::extent_delete::DeleteTarget {
                addr,
                node_uuid: String::new(),
            })
            .collect();
        let payload = ExtentOpPayload::Delete(PersistedPendingDelete {
            extent_id,
            pending_targets,
        });
        let record = MgrExtentInflightRecord::new(extent_id, payload, "test".to_string());
        self.inflight.borrow_mut().insert(extent_id, record);
    }

    /// stale-marker sweep with auto-release.
    ///
    /// Runs on the manager (only when leader). Every
    /// `--inflight-sweep-interval-secs` (default 60s), iterates
    /// the in-memory ledger; for any marker whose `started_at` is older
    /// than `--inflight-stale-threshold-secs` (default 600s =
    /// 10 min), atomically releases it: one `txn_fenced` deletes
    /// `extent_inflight/<id>` from etcd under the leader fence,
    /// then `commit_extent_inflight_release` drops the in-memory shadow.
    /// The release is the same primitive the apply paths
    /// (`apply_recovery_done` / `apply_ec_conversion_done` /
    /// `extent_delete_loop::release_delete_marker`) use — atomic, no
    /// mutation of `extents/<id>`, no EN-side RPC.
    ///
    /// **Safety against false positives:** auto-release only touches the
    /// ledger marker, NOT the extent's `replicates / parity / eversion /
    /// avali` state. If the EN-side task that the released marker
    /// referred to is genuinely still running (long-running recovery on
    /// a multi-GB extent over a slow network), the worst case for the
    /// auto-released kinds is:
    /// - Recovery: EN completes, pushes `recovery_done`, manager's next
    ///   df probe drains it; `apply_recovery_done` runs with the
    ///   marker already cleared → the in-memory `inflight` lookup
    ///   misses → no defer → apply proceeds normally.
    /// - Delete: EN-side `handle_delete_extent` is idempotent
    ///   (NotFound is downgraded to Ok) — re-dispatch from a future
    ///   GC tick is safe.
    ///
    /// **ConvertToEc is WARN-only here.** Not because cancelling it is unsafe —
    /// it is (see below) — but because age is no evidence: an encode of a
    /// multi-GiB extent legitimately runs for minutes. A conversion is cancelled
    /// by an event instead: an abnormal target (`cancel_markers_on_abnormal_nodes`)
    /// or the attempt's own report (`dispatch_one_ec_conversion`). The WARN fires
    /// every sweep tick so a marker stuck for another reason is visible.
    ///
    /// **Cancelling an attempt on its own report or on an abnormal node is a
    /// deliberate exception, and here is why it is safe** (`drain_attempt`,
    /// recovery.rs): the danger above is a live EN dispatch applying its layout
    /// after a second dispatch changed the assignment. Since this doc was written, three things
    /// closed that: there is no EN-side commit phase at all (shards are additive
    /// per-index staging, `.dat` is untouched, and the manager's nonce-checked
    /// flip is the ONLY commit point); a completion report whose nonce does not
    /// match the live marker is refused as `DifferentAttempt`, and a report with
    /// no marker at all is ignored; and participant staging is nonce-ordered by
    /// `claim_ec_staging`, whose contract names the released-but-alive
    /// coordinator explicitly. So a late report from an abandoned attempt cannot
    /// commit anything, and cannot poison a successor — the coordinator's
    /// idempotent-skip keys on eversion, which only the manager's flip advances.
    /// Any future change that reintroduces an EN-side commit, or makes the flip
    /// accept a report without matching the live nonce, invalidates this and the
    /// cancel must go back to operator-only.
    ///
    /// **Leader-failover-as-reconcile invariant:** when the leader
    /// changes, the new leader's `replay_from_etcd` rebuilds the
    /// in-memory ledger from etcd from scratch. `started_at` is part
    /// of the rkyv'd `MgrExtentInflightRecord` payload, persisted
    /// since acquire time — so the stale-detection clock continues
    /// across leader changes. The sweep on the new leader picks up
    /// any markers that were already stale on the deposed leader.
    /// In-memory drift between manager state and etcd state ONLY
    /// arises from a human running `etcdctl del extent_inflight/<id>`
    /// directly; the supported remediation for that is to restart the
    /// manager (or wait for next failover). The sweep does NOT reconcile
    /// against etcd on each tick — the operator-error scenario is too
    /// rare to justify the per-tick prefix read.
    pub(crate) async fn extent_inflight_stale_sweep_loop(self) {
        loop {
            let every = self.params().inflight_sweep_interval_secs.get();
            compio::time::sleep(std::time::Duration::from_secs(every)).await;
            if !self.leader.get() {
                continue;
            }
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            let threshold = self.params().inflight_stale_threshold_secs.get() as i64;
            self.sweep_stale_inflight_once(now, threshold).await;
        }
    }

    // NOTE: there is no Recovery-specific stale threshold. One existed
    // (`AUTUMN_MGR_RECOVERY_INFLIGHT_STALE_SECS`, default 120 s), and it
    // existed only because a held marker froze re-dispatch, which
    // made a timeout the sole way back — and its value had to be hand-tuned to
    // sit just past the EN's own 10 x 10 s internal give-up budget, a magic
    // number pinned to another magic number (found by decommission chaos,
    // 2026-07-10). Recovery markers are now standing instructions that are
    // re-sent every tick, and `recovery_dispatch_loop` releases a marker against
    // live state on either of two facts: its pinned executor is no longer
    // online, or its source slot no longer needs rebuilding. Delete
    // keeps the general threshold (it has its own retry queue);
    // ConvertToEc stays WARN-only.

    /// one sweep iteration. Extracted from the loop so unit tests
    /// can drive it with controlled `now` + `threshold_secs` values.
    ///
    /// Only Delete markers are released here (their targets handed to the
    /// retry queue); ConvertToEc is WARN-only and Recovery is not swept — see
    /// the loop's docstring.
    pub(crate) async fn sweep_stale_inflight_once(&self, now: i64, threshold_secs: i64) -> usize {
        // Recovery markers release faster (see
        // `recovery_stale_threshold_secs`). `.min(threshold_secs)` so a caller
        // (unit test) passing an even-shorter general threshold still wins —
        // recovery is never released LATER than the general threshold.
        // Snapshot stale candidates under a single ledger borrow so we
        // don't hold the borrow across the await.
        let stale: Vec<(u64, ExtentOpKind, i64)> = self
            .inflight
            .borrow()
            .iter()
            .filter_map(|(eid, rec)| {
                rec.kind().and_then(|kind| {
                    // RECOVERY IS NOT SWEPT BY WALL CLOCK ANY MORE. A timeout is
                    // a guess about liveness, and both cases it used to cover are
                    // now handled by mechanisms that KNOW:
                    //   - executor alive but its task died (restart / silent
                    //     give-up) → the marker is a standing instruction and
                    //     `dispatch_recovery_task` re-sends it every tick;
                    //   - executor no longer online → `recovery_dispatch_loop`
                    //     releases the marker (level-triggered against live node
                    //     state, so no edge can be missed).
                    // Deleting it also removes the brittle coupling that made the
                    // threshold 120 s: it had to sit just past the EN's own
                    // 10 x 10 s internal give-up budget.
                    if matches!(kind, ExtentOpKind::Recovery) {
                        return None;
                    }
                    let age = now - rec.started_at;
                    if age >= threshold_secs {
                        Some((*eid, kind, age))
                    } else {
                        None
                    }
                })
            })
            .collect();
        let mut released = 0usize;
        for (eid, kind, age_secs) in stale {
            // ConvertToEc is not released by age: a long encode is slow, not
            // stuck. Events cancel it (an abnormal target, the attempt's own
            // report).
            if matches!(kind, ExtentOpKind::ConvertToEc) {
                tracing::warn!(
                    extent_id = eid,
                    age_secs,
                    threshold_secs,
                    "long-running ConvertToEc marker — not released by age; check the \
                     coordinator's log for the conversion's progress"
                );
                continue;
            }
            // A Delete marker's work is not dropped: what is still pending goes
            // to the persisted retry queue, the same hand-off the delete loop
            // makes after its attempt budget.
            if matches!(kind, ExtentOpKind::Delete) {
                // No progress entry: the delete loop holds it mid-pass (replay
                // rebuilds one for every Delete marker), so what is still
                // pending is known only there — look again next tick.
                let Some(pending) = self
                    .delete_progress
                    .borrow()
                    .get(&eid)
                    .map(|d| d.pending_targets.clone())
                else {
                    continue;
                };
                if let Err(e) = self.persist_failed_delete(eid, pending).await {
                    tracing::warn!(
                        extent_id = eid,
                        error = %e,
                        "stale Delete marker: could not hand its targets to the retry queue; \
                         will retry next sweep tick"
                    );
                    continue;
                }
            }
            // Atomic etcd delete under leader fence. On error, log and
            // skip — next tick will retry.
            if let Some(etcd) = &self.etcd {
                if let Err(e) = etcd
                    .put_and_delete_txn(Vec::new(), vec![Self::extent_inflight_key(eid)])
                    .await
                {
                    tracing::warn!(
                        extent_id = eid,
                        op = ?kind,
                        age_secs,
                        error = %e,
                        "failed to auto-release stale inflight marker; \
                         will retry next sweep tick"
                    );
                    continue;
                }
            }
            // In-memory release. Also drop any Delete progress state
            // (the per-attempt VecDeque entry is no longer load-bearing
            // since we've abandoned the op).
            self.commit_extent_inflight_release(eid);
            if matches!(kind, ExtentOpKind::Delete) {
                self.delete_progress.borrow_mut().remove(&eid);
            }
            tracing::warn!(
                extent_id = eid,
                op = ?kind,
                age_secs,
                threshold_secs,
                "auto-released stale inflight marker — no manager-side \
                 apply within threshold. EN-side task (if any) remains \
                 independent; idempotency on retry handles convergence."
            );
            released += 1;
        }
        released
    }

    /// Decode raw etcd values from the `extent_inflight/` prefix into
    /// `(extent_id, record)` pairs, dropping any malformed records with a
    /// WARN log. Called from `replay_from_etcd`.
    pub(crate) fn decode_extent_inflight_kvs<'a>(
        kvs: impl Iterator<Item = (u64, &'a [u8])>,
    ) -> Vec<(u64, MgrExtentInflightRecord)> {
        let mut out = Vec::new();
        for (extent_id, bytes) in kvs {
            match rkyv_decode::<MgrExtentInflightRecord>(bytes) {
                Ok(rec) => {
                    if rec.unpack().is_some() {
                        out.push((extent_id, rec));
                    } else {
                        tracing::warn!(
                            extent_id,
                            "extent_inflight record has malformed op_kind / payload \
                             combination; skipping on replay"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        extent_id,
                        error = %e,
                        "failed to rkyv-decode extent_inflight value; skipping on replay"
                    );
                }
            }
        }
        out
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persist::records::{ExtentRecord, NodeRecord};
    use autumn_rpc::manager_rpc::{MgrNodeOverride, RecoveryTask, NODE_OVERRIDE_FENCED};

    fn ec_payload(extent_id: u64) -> ExtentOpPayload {
        ExtentOpPayload::ConvertToEc(MgrEcDispatchInflight {
            extent_id,
            target_nodes: vec![1, 3, 5, 7],
            extra_disk_ids: vec![70],
            data_shards: 3,
            new_eversion: 4,
            owner_epoch: 0,
        })
    }

    fn recovery_payload(extent_id: u64) -> ExtentOpPayload {
        ExtentOpPayload::Recovery(RecoveryTask {
            extent_id,
            replace_id: 1,
            node_id: 9,
            start_time: 0,
        })
    }

    fn delete_payload(extent_id: u64) -> ExtentOpPayload {
        ExtentOpPayload::Delete(PersistedPendingDelete {
            extent_id,
            pending_targets: ["127.0.0.1:9101", "127.0.0.1:9102"]
                .into_iter()
                .map(|a| crate::extent_delete::DeleteTarget {
                    addr: a.to_string(),
                    node_uuid: String::new(),
                })
                .collect(),
        })
    }

    fn run<F: std::future::Future<Output = T>, T>(f: F) -> T {
        compio::runtime::Runtime::new().unwrap().block_on(f)
    }

    /// Phase 0: acquire on an empty extent succeeds; in-memory shadow
    /// reflects the op kind.
    #[test]
    fn acquire_extent_inflight_succeeds_when_empty() {
        run(async {
            let m = AutumnManager::new();
            m._test_register_health_nodes([1, 3, 5, 7]);
            m.acquire_extent_inflight(42, ec_payload(42))
                .await
                .expect("acquire");
            assert_eq!(m.extent_inflight_op(42), Some(ExtentOpKind::ConvertToEc));
        })
    }

    /// Phase 0: second acquire on the same extent is refused with
    /// Precondition. Replaces today's ad-hoc checks once
    /// the migration phases land.
    #[test]
    fn acquire_extent_inflight_rejects_duplicate() {
        run(async {
            let m = AutumnManager::new();
            m._test_register_health_nodes([1, 3, 5, 7]);
            m.acquire_extent_inflight(42, ec_payload(42))
                .await
                .expect("first acquire");
            let err = m
                ._test_acquire_marker(42, recovery_payload(42))
                .await
                .expect_err("second acquire must fail");
            assert!(matches!(err, AppError::Precondition(_)), "got {err:?}");
            // The original op's kind is unchanged.
            assert_eq!(m.extent_inflight_op(42), Some(ExtentOpKind::ConvertToEc));
        })
    }

    /// A reissued attempt must get a DIFFERENT identity from the one it
    /// replaced — that difference is the only thing separating the two
    /// attempts' completion reports, since both carry the same coordinator and
    /// the same `new_eversion` (see `recovery::classify_ec_done`).
    #[test]
    fn a_reissued_attempt_gets_a_fresh_nonce() {
        run(async {
            let m = AutumnManager::new();
            m._test_register_health_nodes([1, 3, 5, 7]);
            m.acquire_extent_inflight(42, ec_payload(42))
                .await
                .expect("acquire");
            let first = m.extent_inflight_nonce(42);
            assert_ne!(first, 0, "a live marker must have an identity");

            // Abandon and reissue, exactly as a fenced coordinator would.
            m.commit_extent_inflight_release(42);
            assert_eq!(
                m.extent_inflight_nonce(42),
                0,
                "a released marker leaves no identity behind"
            );
            m.acquire_extent_inflight(42, ec_payload(42))
                .await
                .expect("re-acquire");

            assert!(
                m.extent_inflight_nonce(42) > first,
                "the reissued attempt must be distinguishable from, and ordered after, \
                 the one it replaced"
            );
        })
    }

    /// Phase 0: in-memory release clears the probe.
    #[test]
    fn commit_extent_inflight_release_clears_probe() {
        run(async {
            let m = AutumnManager::new();
            m._test_register_health_nodes([1, 3, 5, 7]);
            m.acquire_extent_inflight(42, ec_payload(42))
                .await
                .expect("acquire");
            m.commit_extent_inflight_release(42);
            assert_eq!(m.extent_inflight_op(42), None);
            // Re-acquire after release works.
            m._test_acquire_marker(42, recovery_payload(42))
                .await
                .expect("re-acquire");
            assert_eq!(m.extent_inflight_op(42), Some(ExtentOpKind::Recovery));
        })
    }

    /// Phase 0: rkyv round-trip on all three payload variants.
    #[test]
    fn record_rkyv_round_trip_all_variants() {
        for payload in [ec_payload(42), recovery_payload(43), delete_payload(44)] {
            let kind = payload.kind();
            let extent_id = match &payload {
                ExtentOpPayload::ConvertToEc(p) => p.extent_id,
                ExtentOpPayload::Recovery(p) => p.extent_id,
                ExtentOpPayload::Delete(p) => p.extent_id,
            };
            let record =
                MgrExtentInflightRecord::new(extent_id, payload, "test-instance".to_string());
            let bytes = rkyv_encode(&record);
            let decoded: MgrExtentInflightRecord =
                rkyv_decode(&bytes).expect("rkyv round-trip decode");
            let (decoded_kind, _) = decoded.unpack().expect("valid record unpacks");
            assert_eq!(decoded_kind, kind);
            assert_eq!(decoded.extent_id, extent_id);
            assert_eq!(decoded.leader_id, "test-instance");
        }
    }

    /// Phase 0: unpack rejects records whose `op_kind` and populated
    /// payload field disagree. Replay drops these with a WARN.
    #[test]
    fn unpack_rejects_malformed_records() {
        // op_kind = ConvertToEc but no ec_payload populated.
        let bad = MgrExtentInflightRecord {
            extent_id: 42,
            op_kind: ExtentOpKind::CONVERT_TO_EC,
            started_at: 0,
            leader_id: "test".to_string(),
            ec_payload: None,
            recovery_payload: Some(RecoveryTask::default()),
            delete_payload: None,
        };
        assert!(
            bad.unpack().is_none(),
            "mismatched op_kind/payload must not unpack"
        );

        // op_kind = Delete and both delete_payload AND ec_payload set.
        let bad = MgrExtentInflightRecord {
            extent_id: 42,
            op_kind: ExtentOpKind::DELETE,
            started_at: 0,
            leader_id: "test".to_string(),
            ec_payload: Some(MgrEcDispatchInflight::default()),
            recovery_payload: None,
            delete_payload: Some(PersistedPendingDelete::default()),
        };
        assert!(
            bad.unpack().is_none(),
            "extra payload field set must not unpack"
        );

        // Unknown op_kind byte.
        let bad = MgrExtentInflightRecord {
            extent_id: 42,
            op_kind: 99,
            started_at: 0,
            leader_id: "test".to_string(),
            ec_payload: None,
            recovery_payload: None,
            delete_payload: None,
        };
        assert!(bad.unpack().is_none(), "unknown op_kind must not unpack");
    }

    /// Phase 0: decode_extent_inflight_kvs filters malformed records.
    #[test]
    fn decode_drops_malformed() {
        let good = MgrExtentInflightRecord::new(42, ec_payload(42), "leader-a".to_string());
        let good_bytes = rkyv_encode(&good).to_vec();

        let kvs: Vec<(u64, Vec<u8>)> = vec![(42, good_bytes), (99, b"not rkyv at all".to_vec())];
        let decoded = AutumnManager::decode_extent_inflight_kvs(
            kvs.iter().map(|(id, b)| (*id, b.as_slice())),
        );
        assert_eq!(decoded.len(), 1, "malformed record dropped");
        assert_eq!(decoded[0].0, 42);
    }

    /// sweep auto-releases Recovery + Delete markers
    /// older than the threshold; ConvertToEc is WARN-only and survives;
    /// fresh markers stay alone; cleans `delete_progress` for Delete.
    /// R3: a Recovery marker whose pinned executor is no longer online is
    /// released — this is what replaced the wall-clock TTL. Level-triggered, so
    /// it fires from live node state rather than from a transition hook.
    #[test]
    fn recovery_marker_released_when_pinned_executor_is_not_online() {
        run(async {
            let m = AutumnManager::new();
            // `recovery_payload` pins node_id = 9, which is not registered here
            // — i.e. the executor is gone as far as the manager can tell.
            m._test_acquire_marker(20, recovery_payload(20))
                .await
                .expect("recovery");
            assert_eq!(m.extent_inflight_op(20), Some(ExtentOpKind::Recovery));

            let released = m.cancel_markers_on_abnormal_nodes().await;
            assert_eq!(
                released,
                vec![20],
                "a dead executor's marker must be released"
            );
            assert_eq!(
                m.extent_inflight_op(20),
                None,
                "released so the next tick can re-dispatch to a LIVE target"
            );
        })
    }

    /// The converse, and the reason this cannot simply release everything: a
    /// marker whose executor IS online is a healthy in-flight recovery and must
    /// survive. (Its progress is kept alive by re-dispatch, not by this sweep.)
    #[test]
    fn recovery_marker_survives_while_its_executor_is_online() {
        run(async {
            let m = AutumnManager::new();
            m._test_acquire_marker(20, recovery_payload(20))
                .await
                .expect("recovery");
            // Register node 9 and drive it to Online the way a heartbeat does.
            {
                let mut s = m.store.inner.borrow_mut();
                s.nodes.insert(
                    9,
                    crate::persist::records::NodeRecord {
                        node_id: 9,
                        address: "127.0.0.1:9109".to_string(),
                        disks: vec![90],
                        shard_ports: vec![],
                        control_address: "127.0.0.1:10109".to_string(),
                        node_uuid: String::new(),
                    },
                );
            }
            m.node_states.borrow_mut().on_heartbeat_ok(9);

            let released = m.cancel_markers_on_abnormal_nodes().await;
            assert!(
                released.is_empty(),
                "a live executor's marker must NOT be released: {released:?}"
            );
            assert_eq!(m.extent_inflight_op(20), Some(ExtentOpKind::Recovery));
        })
    }

    fn set_override(m: &AutumnManager, node_id: u64, kind: u8) {
        m.node_overrides.borrow_mut().insert(
            node_id,
            MgrNodeOverride {
                node_id,
                kind,
                set_at: 0,
                set_by: "test".into(),
                reason: String::new(),
                expire_at: 0,
                node_uuid: String::new(),
            },
        );
    }

    /// Extent 42 replicated on 1/3/5 (disks 10/30/50), its conversion targeting
    /// 1/3/5 plus 7 (extra disk 70), all `Health`.
    fn ec_in_flight() -> AutumnManager {
        let m = AutumnManager::new();
        m._test_register_health_nodes([1, 3, 5, 7]);
        m.store.inner.borrow_mut().extents.insert(
            42,
            ExtentRecord {
                extent_id: 42,
                replicates: vec![1, 3, 5],
                replicate_disks: vec![10, 30, 50],
                sealed: true,
                sealed_length: 4096,
                avali: 0b111,
                ..Default::default()
            },
        );
        m.store.inner.borrow_mut().streams.insert(
            100,
            crate::persist::records::StreamRecord {
                stream_id: 100,
                extent_ids: vec![42],
                ec_data_shard: 3,
                ec_parity_shard: 1,
                replicates: 3,
            },
        );
        m.inflight.borrow_mut().insert(
            42,
            MgrExtentInflightRecord::new(42, ec_payload(42), "test".to_string()),
        );
        m
    }

    /// Every EC target receives a shard, so a conversion whose PARTICIPANT (not
    /// its coordinator) is in maintenance is cancelled, and its op closed FAILED
    /// naming the node, while a conversion on healthy nodes survives.
    #[test]
    fn an_ec_participant_in_maintenance_cancels_the_conversion() {
        run(async {
            let m = ec_in_flight();
            let (op, _) = m.ops.borrow_mut().submit(
                autumn_rpc::manager_rpc::OP_KIND_EC_CONVERT,
                0,
                42,
                vec![],
                "auto-policy".into(),
                0,
                0,
            );
            set_override(&m, 5, autumn_rpc::manager_rpc::NODE_OVERRIDE_MAINTENANCE);
            assert_eq!(m.cancel_markers_on_abnormal_nodes().await, vec![42]);
            let rec = m.ops.borrow().record(op).expect("op");
            assert_eq!(rec.state, autumn_rpc::manager_rpc::OP_STATE_FAILED);
            assert!(rec.error.contains("node 5"), "{}", rec.error);
        })
    }

    #[test]
    fn an_ec_conversion_on_health_nodes_survives() {
        run(async {
            let m = ec_in_flight();
            assert!(m.cancel_markers_on_abnormal_nodes().await.is_empty());
            assert_eq!(m.extent_inflight_op(42), Some(ExtentOpKind::ConvertToEc));
        })
    }

    /// The disk a target writes to counts, not just its node: a replica target
    /// whose disk its node reports Faulted cancels the conversion, and so does
    /// the extra target's allocated disk. A Faulted disk the conversion does
    /// not use changes nothing.
    #[test]
    fn an_ec_target_disk_faulted_cancels_the_conversion() {
        run(async {
            let m = ec_in_flight();
            m.faulted_disks.borrow_mut().insert(999);
            assert!(m.cancel_markers_on_abnormal_nodes().await.is_empty());
            m.faulted_disks.borrow_mut().insert(30);
            assert_eq!(m.cancel_markers_on_abnormal_nodes().await, vec![42]);

            let m = ec_in_flight();
            m.faulted_disks.borrow_mut().insert(70);
            assert_eq!(m.cancel_markers_on_abnormal_nodes().await, vec![42]);
        })
    }

    /// Submit refuses what the next tick would cancel: a target whose disk its
    /// node reports Faulted, not only a target whose node is abnormal.
    #[test]
    fn an_ec_target_on_a_faulted_disk_is_refused_at_submit() {
        run(async {
            let m = ec_in_flight();
            m._test_clear_inflight(42);
            m.faulted_disks.borrow_mut().insert(30);
            let err = m.acquire_extent_inflight(42, ec_payload(42)).await.unwrap_err();
            assert!(err.to_string().contains("disk 30"), "{err}");
        })
    }

    /// Right after a promotion no node has answered a df, so nothing takes new
    /// data yet. That is not a failed dispatch: it is deferred, without the
    /// backoff a failure would start.
    #[test]
    fn recovery_without_a_proven_target_is_deferred() {
        run(async {
            let m = AutumnManager::new();
            m._test_register_health_nodes([1, 9]);
            m.store.inner.borrow_mut().extents.insert(
                20,
                ExtentRecord {
                    extent_id: 20,
                    replicates: vec![1],
                    replicate_disks: vec![10],
                    sealed: true,
                    sealed_length: 4096,
                    ..Default::default()
                },
            );
            m.node_states.borrow_mut().on_term_start([1, 9]);
            let outcome = m.dispatch_recovery_task(20, 1).await.unwrap();
            assert_eq!(outcome, crate::recovery::DispatchOutcome::Deferred);
        })
    }

    /// Another op holding the extent defers a recovery: it is neither a
    /// dispatch (which would clear the slot's backoff) nor a failure.
    #[test]
    fn recovery_under_another_op_is_deferred() {
        run(async {
            let m = ec_in_flight();
            let outcome = m.dispatch_recovery_task(42, 1).await.unwrap();
            assert_eq!(outcome, crate::recovery::DispatchOutcome::Deferred);
        })
    }

    /// The advisory cache can be a policy tick old: at actuation the arbiter is
    /// asked again, and a candidate it now refuses is dropped.
    #[test]
    fn a_stale_ec_candidate_is_dropped_at_actuation() {
        let m = ec_in_flight();
        let cand = autumn_rpc::manager_rpc::PolicyCandidate {
            kind: autumn_rpc::manager_rpc::POLICY_KIND_EC,
            primary_part_id: 0,
            secondary_part_id: 42,
            reason: String::new(),
            size_bytes: 0,
            req_per_sec: 0,
            imm_full_per_sec: 0,
            same_ps: true,
            last_op_at: 0,
        };
        let why = m
            .stale_candidate(&m.arbiter_view(), &cand)
            .expect("the conversion in flight refuses");
        assert!(why.contains("in-flight"), "{why}");
        m._test_clear_inflight(42);
        assert_eq!(m.stale_candidate(&m.arbiter_view(), &cand), None);
    }

    /// A re-send the executor cannot even be reached for cancels the attempt
    /// (nothing listens at node 9's address); the next tick derives a new one.
    #[test]
    fn a_re_send_to_an_unreachable_executor_cancels_the_attempt() {
        run(async {
            let m = AutumnManager::new();
            m._test_register_health_nodes([1, 9]);
            m.store.inner.borrow_mut().extents.insert(
                20,
                ExtentRecord {
                    extent_id: 20,
                    replicates: vec![1],
                    replicate_disks: vec![10],
                    sealed: true,
                    sealed_length: 4096,
                    ..Default::default()
                },
            );
            m._test_acquire_marker(20, recovery_payload(20)).await.unwrap();
            m.dispatch_recovery_task(20, 1).await.unwrap();
            assert_eq!(m.extent_inflight_op(20), None, "the unreachable attempt is cancelled");
        })
    }

    /// A recovery depends on its executor: one that enters maintenance has its
    /// marker cancelled (only a Suspected or unregistered executor used to be).
    #[test]
    fn a_recovery_whose_executor_enters_maintenance_is_cancelled() {
        run(async {
            let m = AutumnManager::new();
            m._test_register_health_nodes([1, 9]);
            m._test_acquire_marker(20, recovery_payload(20)).await.unwrap();
            assert!(m.cancel_markers_on_abnormal_nodes().await.is_empty());
            set_override(&m, 9, autumn_rpc::manager_rpc::NODE_OVERRIDE_MAINTENANCE);
            assert_eq!(m.cancel_markers_on_abnormal_nodes().await, vec![20]);
        })
    }

    /// Right after a failover every node is `Suspend`. That is unknown, not
    /// abnormal: the markers stay and wait for the nodes' first df.
    #[test]
    fn a_new_term_cancels_nothing() {
        run(async {
            let m = ec_in_flight();
            m._test_register_health_nodes([9]);
            m._test_acquire_marker(20, recovery_payload(20)).await.unwrap();
            m.node_states.borrow_mut().on_term_start([1, 3, 5, 7, 9]);
            assert!(m.cancel_markers_on_abnormal_nodes().await.is_empty());
            assert_eq!(m.extent_inflight_op(20), Some(ExtentOpKind::Recovery));
            assert_eq!(m.extent_inflight_op(42), Some(ExtentOpKind::ConvertToEc));
        })
    }

    /// A df that times out clears every disk's `online` bit node-wide; that says
    /// nothing about the disks and must not retire the recovery. A disk its node
    /// reports Faulted does.
    #[test]
    fn a_df_timeout_does_not_retire_a_recovery() {
        let m = AutumnManager::new();
        m._test_register_health_nodes([1, 9]);
        m.store.inner.borrow_mut().extents.insert(
            20,
            ExtentRecord {
                extent_id: 20,
                replicates: vec![1],
                replicate_disks: vec![10],
                sealed: true,
                sealed_length: 4096,
                ..Default::default()
            },
        );
        let task = RecoveryTask { extent_id: 20, replace_id: 1, node_id: 9, start_time: 0 };
        m.store.inner.borrow_mut().disks.get_mut(&90).unwrap().online = false;
        assert!(m.capture_recovery_attempt(&task).is_ok());
        m.faulted_disks.borrow_mut().insert(90);
        assert!(m.capture_recovery_attempt(&task).is_err());
    }

    /// A recovery target takes new data: a `Health` node with a `Health` disk.
    /// Not a node nobody has heard from this term, nor one whose only disk is
    /// Full or Faulted.
    #[test]
    fn recovery_targets_are_nodes_that_take_new_data() {
        let m = AutumnManager::new();
        m._test_register_health_nodes([1, 2, 3, 4, 5]);
        m.store.inner.borrow_mut().extents.insert(
            20,
            ExtentRecord {
                extent_id: 20,
                replicates: vec![1],
                replicate_disks: vec![10],
                sealed: true,
                sealed_length: 4096,
                ..Default::default()
            },
        );
        m.node_states.borrow_mut().on_term_start([2]);
        m.store
            .inner
            .borrow_mut()
            .en_reports
            .entry(3)
            .or_default()
            .full_disks
            .push(30);
        m.faulted_disks.borrow_mut().insert(40);
        let (_, order) = m.recovery_candidate_order(20).unwrap();
        let ids: Vec<u64> = order.iter().map(|n| n.node_id).collect();
        assert_eq!(ids, vec![5]);
    }

    #[test]
    fn recovery_marker_unfence_releases_only_a_healthy_slot() {
        run(async {
            let m = AutumnManager::new();
            m.store.inner.borrow_mut().nodes.insert(
                1,
                NodeRecord {
                    node_id: 1,
                    address: "127.0.0.1:9101".into(),
                    disks: vec![10],
                    shard_ports: vec![],
                    control_address: String::new(),
                    node_uuid: String::new(),
                },
            );
            m.node_states.borrow_mut().on_heartbeat_ok(1);
            // This leader has heard node 1's own `df`; without that
            // evidence a release cannot tell a healthy disk from a
            // faulted one it has not been told about yet.
            m.node_max_free.borrow_mut().insert(1, 1 << 30);
            m.store.inner.borrow_mut().disks.insert(
                10,
                crate::persist::records::DiskRecord {
                    disk_id: 10,
                    online: true,
                    uuid: String::new(),
                },
            );
            m.store.inner.borrow_mut().extents.insert(
                20,
                ExtentRecord {
                    extent_id: 20,
                    sealed: true,
                    replicates: vec![1],
                    replicate_disks: vec![10],
                    avali: 1,
                    ..Default::default()
                },
            );
            for _ in 0..10 {
                m.node_overrides.borrow_mut().insert(
                    1,
                    MgrNodeOverride {
                        node_id: 1,
                        kind: NODE_OVERRIDE_FENCED,
                        ..Default::default()
                    },
                );
                m._test_acquire_marker(20, recovery_payload(20))
                    .await
                    .unwrap();
                m.reseed_recovery_limiter();
                assert_eq!(m.recovery_limiter.borrow().global_inflight, 1);
                assert!(m
                    .release_recovery_markers_for_healthy_slots()
                    .await
                    .is_empty());
                m.node_overrides.borrow_mut().remove(&1);
                assert_eq!(
                    m.release_recovery_markers_for_healthy_slots().await,
                    vec![20]
                );
                m.reseed_recovery_limiter();
                let lim = m.recovery_limiter.borrow();
                assert_eq!(lim.global_inflight, 0);
                assert_eq!(lim.snapshot(), (vec![], vec![]));
            }
            m._test_acquire_marker(20, recovery_payload(20))
                .await
                .unwrap();
            m.faulted_disks.borrow_mut().insert(10);
            assert!(m
                .release_recovery_markers_for_healthy_slots()
                .await
                .is_empty());
            m.faulted_disks.borrow_mut().clear();
            m.store
                .inner
                .borrow_mut()
                .disks
                .get_mut(&10)
                .unwrap()
                .online = false;
            assert!(m
                .release_recovery_markers_for_healthy_slots()
                .await
                .is_empty());
            m.store
                .inner
                .borrow_mut()
                .disks
                .get_mut(&10)
                .unwrap()
                .online = true;
            m.extent_corrupt_slots.borrow_mut().insert(20, 1);
            assert!(m
                .release_recovery_markers_for_healthy_slots()
                .await
                .is_empty());
            m.extent_corrupt_slots.borrow_mut().clear();
            m.store
                .inner
                .borrow_mut()
                .extents
                .get_mut(&20)
                .unwrap()
                .avali = 0;
            assert!(m
                .release_recovery_markers_for_healthy_slots()
                .await
                .is_empty());
            {
                let mut s = m.store.inner.borrow_mut();
                let ex = s.extents.get_mut(&20).unwrap();
                ex.replicates = vec![2];
                ex.parity = vec![1];
                ex.replicate_disks = vec![99];
                ex.parity_disks = vec![10];
                ex.avali = 3;
            }
            assert_eq!(
                m.release_recovery_markers_for_healthy_slots().await,
                vec![20]
            );
        });
    }

    /// A re-election happens in the SAME process, so "this leader has heard
    /// the node's own `df`" is only true if promotion forgets what the
    /// previous term heard. Without that, a manager that led before answers
    /// for a node whose disk faulted while someone else held the term, and
    /// releases the marker for a rebuild that is genuinely needed.
    #[test]
    fn promotion_forgets_the_previous_terms_df() {
        run(async {
            let m = AutumnManager::new();
            m.node_max_free.borrow_mut().insert(7, 1 << 30);
            m.faulted_disks.borrow_mut().insert(70);
            assert!(m.has_first_hand_df(7));

            // Drive PROMOTION, not the helper: a reset the promotion path
            // never reaches is the same dead-mechanism shape as a release the
            // loop never calls.
            m.replay_from_etcd().await.unwrap();

            assert!(
                !m.has_first_hand_df(7),
                "a fresh term must not answer for a node only an earlier term heard from"
            );
            assert!(
                m.faulted_disks.borrow().is_empty(),
                "and it must not keep the earlier term's disk verdicts either — the \
                 field documents itself as empty at the start of a leader's life"
            );
        });
    }

    /// A marker on a slot that nothing says should move — node registered,
    /// Online, heard from this term, disk online, `avali` set, no override, no
    /// corrupt mark — is a rebuild nobody wants any more, and it is released.
    #[test]
    fn a_marker_on_a_healthy_slot_is_released() {
        run(async {
            let m = AutumnManager::new();
            m.store.inner.borrow_mut().nodes.insert(
                1,
                NodeRecord {
                    node_id: 1,
                    address: "127.0.0.1:9101".into(),
                    disks: vec![10],
                    shard_ports: vec![],
                    control_address: String::new(),
                    node_uuid: String::new(),
                },
            );
            m.node_states.borrow_mut().on_heartbeat_ok(1);
            // This leader has heard node 1's own `df`; without that
            // evidence a release cannot tell a healthy disk from a
            // faulted one it has not been told about yet.
            m.node_max_free.borrow_mut().insert(1, 1 << 30);
            m.store.inner.borrow_mut().disks.insert(
                10,
                crate::persist::records::DiskRecord {
                    disk_id: 10,
                    online: true,
                    uuid: String::new(),
                },
            );
            m.store.inner.borrow_mut().extents.insert(
                20,
                ExtentRecord {
                    extent_id: 20,
                    sealed: true,
                    replicates: vec![1],
                    replicate_disks: vec![10],
                    avali: 1,
                    ..Default::default()
                },
            );
            m._test_acquire_marker(20, recovery_payload(20))
                .await
                .unwrap();
            assert_eq!(
                m.release_recovery_markers_for_healthy_slots().await,
                vec![20],
                "nothing but an override could have asked for this rebuild, \
                 and it is gone"
            );
        });
    }

    /// The release above is only worth anything if the dispatch loop reaches
    /// it, and a call site nothing drives is exactly the shape of bug that
    /// leaves every unit test green while the mechanism is dead. So drive one
    /// real tick rather than the predicate.
    #[test]
    fn the_dispatch_tick_releases_an_obsolete_marker() {
        run(async {
            let m = AutumnManager::new();
            m.store.inner.borrow_mut().nodes.insert(
                1,
                NodeRecord {
                    node_id: 1,
                    address: "127.0.0.1:9101".into(),
                    disks: vec![10],
                    shard_ports: vec![],
                    control_address: String::new(),
                    node_uuid: String::new(),
                },
            );
            // The marker's pinned EXECUTOR has to be alive too, or the
            // dead-executor release retires it first and the unfence proves
            // nothing. Nothing listens at its address, so the tick's re-send
            // is refused and the marker stays — which is the point.
            m.store.inner.borrow_mut().nodes.insert(
                9,
                NodeRecord {
                    node_id: 9,
                    address: "127.0.0.1:9109".into(),
                    disks: vec![90],
                    shard_ports: vec![],
                    control_address: String::new(),
                    node_uuid: String::new(),
                },
            );
            m.node_states.borrow_mut().on_heartbeat_ok(1);
            // This leader has heard node 1's own `df`; without that
            // evidence a release cannot tell a healthy disk from a
            // faulted one it has not been told about yet.
            m.node_max_free.borrow_mut().insert(1, 1 << 30);
            // The executor has not answered this term (no EN runs here): not
            // abnormal, so its marker is kept, and nothing is re-sent to it.
            m.node_states.borrow_mut().on_register_first(9);
            m.store.inner.borrow_mut().disks.insert(
                10,
                crate::persist::records::DiskRecord {
                    disk_id: 10,
                    online: true,
                    uuid: String::new(),
                },
            );
            m.store.inner.borrow_mut().extents.insert(
                20,
                ExtentRecord {
                    extent_id: 20,
                    sealed: true,
                    sealed_length: 4096,
                    replicates: vec![1],
                    replicate_disks: vec![10],
                    avali: 1,
                    ..Default::default()
                },
            );
            m.node_overrides.borrow_mut().insert(
                1,
                MgrNodeOverride {
                    node_id: 1,
                    kind: NODE_OVERRIDE_FENCED,
                    ..Default::default()
                },
            );
            m.store.inner.borrow_mut().disks.insert(
                90,
                crate::persist::records::DiskRecord {
                    disk_id: 90,
                    online: true,
                    ..Default::default()
                },
            );
            m._test_acquire_marker(20, recovery_payload(20))
                .await
                .unwrap();
            m.ops.borrow_mut().note_recovery_dispatch(20, 0, 9, 1, 1_000);

            m.recovery_dispatch_tick().await;
            assert_eq!(
                m.extent_inflight_op(20),
                Some(ExtentOpKind::Recovery),
                "the fence still stands, so the rebuild is still wanted"
            );

            m.node_overrides.borrow_mut().remove(&1);
            m.recovery_dispatch_tick().await;
            assert_eq!(
                m.extent_inflight_op(20),
                None,
                "one tick after the unfence the marker must be gone — the loop \
                 has to REACH the release, not merely contain it"
            );
            let lim = m.recovery_limiter.borrow();
            assert_eq!(lim.global_inflight, 0);
            assert_eq!(lim.snapshot(), (vec![], vec![]));
            let ops = m.ops.borrow().query(&autumn_rpc::manager_rpc::OpQueryReq {
                op_id: 0,
                active_only: false,
                kind_filter: autumn_rpc::manager_rpc::OP_KIND_RECOVERY,
                limit: 0,
            });
            assert_eq!(ops.len(), 1);
            assert_eq!(ops[0].state, autumn_rpc::manager_rpc::OP_STATE_SUCCEEDED);
            assert_eq!(
                ops[0].message,
                "no rebuild needed any more: source slot is healthy again"
            );
        });
    }

    /// A rebuild whose dispatch failed leaves a backoff entry and a RUNNING
    /// recovery op behind it, with no marker. When the need goes away (unfence:
    /// the copy serves again) the tick must end both — otherwise
    /// `recovery-stats` and `ops list` keep reporting a repair that `health`
    /// rightly says is not happening.
    #[test]
    fn an_unfence_ends_the_failed_rebuild_s_backoff_and_op() {
        use autumn_rpc::manager_rpc::{OpQueryReq, OP_KIND_RECOVERY};
        run(async {
            let m = AutumnManager::new();
            // Node 1 holds the copy; node 9 is the only rebuild candidate and
            // nothing listens at its address, so every dispatch fails.
            for (id, addr, disk) in [(1u64, "127.0.0.1:1", 10u64), (9, "127.0.0.1:9", 90)] {
                m.store.inner.borrow_mut().nodes.insert(
                    id,
                    NodeRecord {
                        node_id: id,
                        address: addr.into(),
                        disks: vec![disk],
                        shard_ports: vec![],
                        control_address: String::new(),
                        node_uuid: String::new(),
                    },
                );
                m.node_states.borrow_mut().on_heartbeat_ok(id);
                m.node_max_free.borrow_mut().insert(id, 1 << 30);
                m.store.inner.borrow_mut().disks.insert(
                    disk,
                    crate::persist::records::DiskRecord {
                        disk_id: disk,
                        online: true,
                        uuid: String::new(),
                    },
                );
            }
            m.store.inner.borrow_mut().extents.insert(
                20,
                ExtentRecord {
                    extent_id: 20,
                    sealed: true,
                    sealed_length: 4096,
                    replicates: vec![1],
                    replicate_disks: vec![10],
                    avali: 1,
                    ..Default::default()
                },
            );
            m.node_overrides.borrow_mut().insert(
                1,
                MgrNodeOverride {
                    node_id: 1,
                    kind: NODE_OVERRIDE_FENCED,
                    ..Default::default()
                },
            );
            let active_recovery = |m: &AutumnManager| {
                m.ops.borrow().query(&OpQueryReq {
                    op_id: 0,
                    active_only: true,
                    kind_filter: OP_KIND_RECOVERY,
                    limit: 0,
                })
            };

            m.recovery_dispatch_tick().await;
            assert_eq!(m.extent_inflight_op(20), None, "the failed dispatch drained its marker");
            assert_eq!(
                m.recovery_limiter.borrow().backoff_snapshot().len(),
                1,
                "the failure backs the slot off"
            );
            assert_eq!(active_recovery(&m).len(), 1, "and leaves the repair RUNNING");

            // Still fenced: the rebuild is still owed, so both stay.
            m.recovery_dispatch_tick().await;
            assert_eq!(m.recovery_limiter.borrow().backoff_snapshot().len(), 1);
            assert_eq!(active_recovery(&m).len(), 1);

            m.node_overrides.borrow_mut().remove(&1);
            m.recovery_dispatch_tick().await;
            assert!(
                m.recovery_limiter.borrow().backoff_snapshot().is_empty(),
                "the copy serves again: nothing is retrying, so nothing may back off"
            );
            assert!(
                active_recovery(&m).is_empty(),
                "and the repair it was for is over: {:?}",
                active_recovery(&m)
            );
            let all = m.ops.borrow().query(&OpQueryReq {
                op_id: 0,
                active_only: false,
                kind_filter: OP_KIND_RECOVERY,
                limit: 0,
            });
            let last = all.first().expect("the recovery entry");
            assert_eq!(last.state, autumn_rpc::manager_rpc::OP_STATE_SUCCEEDED);
            assert!(
                last.message.starts_with("no rebuild needed any more"),
                "{last:?}"
            );
        });
    }

    /// Keep is not always a verdict. A node this term has no `df` from may sit
    /// on a faulted disk that reads healthy, and a repair request waiting on an
    /// in-place catch-up becomes a rebuild again if the catch-up finds the copy
    /// gone. A failed rebuild's backoff and RUNNING entry must outlive both.
    #[test]
    fn an_undecided_keep_holds_the_failed_rebuild_s_backoff_and_op() {
        use autumn_rpc::manager_rpc::{OpQueryReq, OP_KIND_RECOVERY};
        run(async {
            let m = AutumnManager::new();
            m.store.inner.borrow_mut().nodes.insert(
                1,
                NodeRecord {
                    node_id: 1,
                    address: "127.0.0.1:1".into(),
                    disks: vec![10],
                    shard_ports: vec![],
                    control_address: String::new(),
                    node_uuid: String::new(),
                },
            );
            m.node_states.borrow_mut().on_heartbeat_ok(1);
            m.store.inner.borrow_mut().disks.insert(
                10,
                crate::persist::records::DiskRecord {
                    disk_id: 10,
                    online: true,
                    uuid: String::new(),
                },
            );
            m.store.inner.borrow_mut().extents.insert(
                20,
                ExtentRecord {
                    extent_id: 20,
                    sealed: true,
                    sealed_length: 4096,
                    replicates: vec![1],
                    replicate_disks: vec![10],
                    avali: 1,
                    ..Default::default()
                },
            );
            // What a failed dispatch leaves: a backoff and a RUNNING entry.
            m.recovery_limiter
                .borrow_mut()
                .record_failure(20, 0, 1, "no candidate node for recovery");
            m.ops
                .borrow_mut()
                .record_recovery_failure(20, "no candidate node for recovery".into(), 3, 1, 1_000);
            let held = |m: &AutumnManager| {
                let active = m.ops.borrow().query(&OpQueryReq {
                    op_id: 0,
                    active_only: true,
                    kind_filter: OP_KIND_RECOVERY,
                    limit: 0,
                });
                (m.recovery_limiter.borrow().backoff_snapshot().len(), active.len())
            };

            // No first-hand df from node 1 this term.
            m.recovery_dispatch_tick().await;
            assert_eq!(held(&m), (1, 1), "no df yet: Keep is not a verdict");

            // Heard from, but a repair request waits on an in-place catch-up.
            m.node_max_free.borrow_mut().insert(1, 1 << 30);
            m.store.inner.borrow_mut().extents.get_mut(&20).unwrap().avali = 0;
            m.extent_repair_slots.borrow_mut().insert(20, 1);
            m.recovery_dispatch_tick().await;
            assert_eq!(held(&m), (1, 1), "a suspended rebuild is not a withdrawn one");

            // The request gone and the copy serving: now the need is over.
            m.extent_repair_slots.borrow_mut().clear();
            m.store.inner.borrow_mut().extents.get_mut(&20).unwrap().avali = 1;
            m.recovery_dispatch_tick().await;
            assert_eq!(held(&m), (0, 0));
        });
    }

    /// A repair request yields once its node answers again — on first-hand
    /// evidence, a `df` THIS leader term applied. A copy that serves has its
    /// request withdrawn; a behind copy is caught up in place first while the
    /// request waits, and only repeated failed catch-ups let the request move
    /// it. A request on a node that does not answer stands, and so does one on
    /// a node that merely READS Online because a promotion replayed it so
    /// (every replayed node does, dead or not, until its first `df`).
    #[test]
    fn a_repair_request_is_withdrawn_when_its_node_answers_again() {
        run(async {
            let m = AutumnManager::new();
            for (node_id, addr, disk) in [
                (1u64, "127.0.0.1:1", 10u64),
                (2, "127.0.0.1:9102", 20),
                (3, "127.0.0.1:9103", 30),
                (4, "127.0.0.1:9104", 40),
            ] {
                m.store.inner.borrow_mut().nodes.insert(
                    node_id,
                    NodeRecord {
                        node_id,
                        address: addr.into(),
                        disks: vec![disk],
                        shard_ports: vec![],
                        control_address: String::new(),
                        node_uuid: String::new(),
                    },
                );
                m.store.inner.borrow_mut().disks.insert(
                    disk,
                    crate::persist::records::DiskRecord {
                        disk_id: disk,
                        online: true,
                        uuid: String::new(),
                    },
                );
            }
            // Nodes 1 and 2 answer (this term applied their `df`); node 3
            // registered and never has; node 4 reads Online only because a
            // promotion replayed it so — no `df` this term.
            for n in [1u64, 2, 4] {
                m.node_states.borrow_mut().on_heartbeat_ok(n);
            }
            m.node_max_free.borrow_mut().insert(1, 1 << 30);
            m.node_max_free.borrow_mut().insert(2, 1 << 30);
            m.node_states.borrow_mut().on_register_first(3);
            // Slot 0 of each: 30 on node 1 (behind), 31 on node 3 (silent),
            // 32 on node 4 (replayed Online, unheard), 33 on node 1 (serving).
            for (id, first, avali) in [
                (30u64, 1u64, 0b10u32),
                (31, 3, 0b10),
                (32, 4, 0b10),
                (33, 1, 0b11),
            ] {
                m.store.inner.borrow_mut().extents.insert(
                    id,
                    ExtentRecord {
                        extent_id: id,
                        sealed: true,
                        sealed_length: 4096,
                        replicates: vec![first, 2],
                        replicate_disks: vec![first * 10, 20],
                        avali,
                        ..Default::default()
                    },
                );
                m.extent_repair_slots.borrow_mut().insert(id, 0b01);
            }

            m.recovery_dispatch_tick().await;

            assert_eq!(m.repair_slots_of(33), 0, "node 1 answers and the copy serves: withdrawn");
            assert_eq!(m.repair_slots_of(30), 0b01, "a behind copy: the request waits…");
            assert_eq!(m.extent_inflight_op(30), None, "…and nothing is moved yet");
            let start = std::time::Instant::now();
            while !m
                .catch_up_backoff
                .borrow()
                .backoff_snapshot()
                .iter()
                .any(|(e, slot, ..)| *e == 30 && *slot == 0)
            {
                assert!(
                    start.elapsed() < std::time::Duration::from_secs(5),
                    "the behind copy is caught up in place instead"
                );
                compio::time::sleep(std::time::Duration::from_millis(50)).await;
            }

            assert_eq!(m.repair_slots_of(31), 0b01, "node 3 is still silent: the request stands");
            assert_eq!(
                m.repair_slots_of(32),
                0b01,
                "node 4 reads Online from replay alone: not evidence, the request stands"
            );

            // A failing catch-up alone (nothing listens on node 1 here) is no
            // evidence the copy is gone: the request keeps waiting.
            m.recovery_dispatch_tick().await;
            let rebuild_tried = |m: &AutumnManager| {
                m.extent_inflight_op(30).is_some()
                    || m
                        .recovery_limiter
                        .borrow()
                        .backoff_snapshot()
                        .iter()
                        .any(|(e, slot, ..)| *e == 30 && *slot == 0)
            };
            assert!(!rebuild_tried(&m), "a failed catch-up alone does not move the copy");
            // The node answers that it has no such extent (a wiped node
            // rejoined): the request moves the copy after all.
            m.catch_up_copy_gone.borrow_mut().insert((30, 0));
            m.recovery_dispatch_tick().await;
            assert!(rebuild_tried(&m), "a copy its node says is gone is rebuilt elsewhere");
            assert!(
                m.recovery_limiter
                    .borrow()
                    .backoff_snapshot()
                    .iter()
                    .any(|(e, slot, ..)| *e == 31 && *slot == 0),
                "and its rebuild was attempted (no spare node here, so it backs off)"
            );
        });
    }

    /// No new EC conversion of an extent with a corrupt-marked slot: the
    /// coordinator would encode from a full-length copy without asking where it
    /// came from, and a marked copy is full length.
    #[test]
    fn no_ec_conversion_starts_on_an_extent_with_a_corrupt_slot() {
        run(async {
            let m = AutumnManager::new();
            m._test_register_health_nodes([1, 3, 5, 7]);
            m.extent_corrupt_slots.borrow_mut().insert(10, 0b1);
            assert!(matches!(
                m.acquire_extent_inflight(10, ec_payload(10)).await,
                Err(AppError::Precondition(_))
            ));
            m.extent_corrupt_slots.borrow_mut().clear();
            m.acquire_extent_inflight(10, ec_payload(10)).await.unwrap();
        });
    }

    /// A behind slot is caught up in place only once its extent has no op in
    /// flight. Marking the slot available bumps the eversion every such op
    /// pinned — a sibling slot's rebuild would be judged stale at its next
    /// re-send, retired, its finished copy refused, and started over.
    #[test]
    fn a_behind_slot_waits_for_the_op_in_flight_on_its_extent() {
        run(async {
            let m = AutumnManager::new();
            for (node_id, addr, disk) in [
                (1u64, "127.0.0.1:9101", 10u64),
                (2, "127.0.0.1:1", 11),
                (9, "127.0.0.1:9109", 90),
            ] {
                m.store.inner.borrow_mut().nodes.insert(
                    node_id,
                    NodeRecord {
                        node_id,
                        address: addr.into(),
                        disks: vec![disk],
                        shard_ports: vec![],
                        control_address: String::new(),
                        node_uuid: String::new(),
                    },
                );
                // The executor (9) has not answered this term — no EN runs
                // here — so its marker is kept and nothing is re-sent to it.
                if node_id == 9 {
                    m.node_states.borrow_mut().on_register_first(node_id);
                } else {
                    m.node_states.borrow_mut().on_heartbeat_ok(node_id);
                }
                m.store.inner.borrow_mut().disks.insert(
                    disk,
                    crate::persist::records::DiskRecord {
                        disk_id: disk,
                        online: true,
                        uuid: String::new(),
                    },
                );
            }
            // Slot 0 (node 2) missed the seal; slot 1 (node 1) is being
            // rebuilt onto node 9.
            m.store.inner.borrow_mut().extents.insert(
                20,
                ExtentRecord {
                    extent_id: 20,
                    sealed: true,
                    sealed_length: 4096,
                    replicates: vec![2, 1],
                    replicate_disks: vec![11, 10],
                    avali: 0b10,
                    ..Default::default()
                },
            );
            m._test_acquire_marker(20, recovery_payload(20))
                .await
                .unwrap();
            let attempted = |m: &AutumnManager| {
                m.catch_up_backoff
                    .borrow()
                    .backoff_snapshot()
                    .iter()
                    .any(|(e, slot, ..)| *e == 20 && *slot == 0)
            };

            m.recovery_dispatch_tick().await;
            compio::time::sleep(std::time::Duration::from_millis(300)).await;
            assert_eq!(m.extent_inflight_op(20), Some(ExtentOpKind::Recovery));
            assert!(
                !attempted(&m) && m.catch_up_inflight.borrow().is_empty(),
                "no catch-up while the extent's rebuild is in flight"
            );

            m._test_release_recovery(20).await.unwrap();
            m.recovery_dispatch_tick().await;
            // Nothing listens on node 2's address, so the attempt fails and
            // lands in the slot's backoff — proof that it ran.
            let start = std::time::Instant::now();
            while !attempted(&m) {
                assert!(
                    start.elapsed() < std::time::Duration::from_secs(5),
                    "with the op done, the behind slot is caught up"
                );
                compio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            assert!(
                !m.recovery_limiter
                    .borrow()
                    .backoff_snapshot()
                    .iter()
                    .any(|(e, slot, ..)| *e == 20 && *slot == 0),
                "a failed catch-up must not hold back a rebuild of the slot"
            );
        });
    }

    #[test]
    fn sweep_auto_releases_stale_markers() {
        run(async {
            let m = AutumnManager::new();
            m._test_register_health_nodes([1, 3, 5, 7]);
            // Three markers, one of each kind.
            m.acquire_extent_inflight(10, ec_payload(10))
                .await
                .expect("ec");
            m._test_acquire_marker(20, recovery_payload(20))
                .await
                .expect("recovery");
            m.acquire_extent_inflight(30, delete_payload(30))
                .await
                .expect("delete");
            // Mimic the populate-delete-progress step that enqueue_pending_deletes does.
            m.delete_progress.borrow_mut().insert(
                30,
                crate::extent_delete::PendingDelete {
                    extent_id: 30,
                    pending_targets: vec![crate::extent_delete::DeleteTarget {
                        addr: "127.0.0.1:9101".to_string(),
                        node_uuid: String::new(),
                    }],
                    attempts: 0,
                },
            );

            // All three are present.
            assert_eq!(m.extent_inflight_op(10), Some(ExtentOpKind::ConvertToEc));
            assert_eq!(m.extent_inflight_op(20), Some(ExtentOpKind::Recovery));
            assert_eq!(m.extent_inflight_op(30), Some(ExtentOpKind::Delete));

            // Sweep with `now` in the far future and a 1 s threshold —
            // every marker is stale.
            let now_future = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                + 7200;
            let released = m.sweep_stale_inflight_once(now_future, 1).await;
            // ONLY Delete (30) is released by wall clock now.
            //   - ConvertToEc (10): WARN-only — releasing it could race the
            //     coordinator's in-flight 2PC.
            //   - Recovery (20): no longer swept AT ALL. A timeout is a guess
            //     about liveness; both cases it used to cover now have
            //     mechanisms that know — the marker is a standing instruction
            //     re-sent every tick (executor alive but its task died), and
            //     `recovery_dispatch_loop` releases markers whose pinned
            //     executor is no longer online (executor gone).
            assert_eq!(
                released, 1,
                "only Delete is wall-clock released; ConvertToEc is WARN-only and \
                 Recovery is no longer swept by time at all"
            );
            assert_eq!(
                m.extent_inflight_op(10),
                Some(ExtentOpKind::ConvertToEc),
                "stale ConvertToEc marker must survive sweep"
            );
            assert_eq!(
                m.extent_inflight_op(20),
                Some(ExtentOpKind::Recovery),
                "a Recovery marker must NOT be released by wall clock — it is \
                 re-dispatched, and released on executor-not-online instead"
            );
            assert_eq!(m.extent_inflight_op(30), None);
            // Delete progress entry also cleaned.
            assert!(
                !m.delete_progress.borrow().contains_key(&30),
                "Delete release must also drop delete_progress entry"
            );
            // ... and its work is not dropped: what was pending is in the
            // persisted retry queue.
            let retry = m.failed_deletes.borrow().get(&30).cloned().expect("handed off");
            assert_eq!(retry.pending_targets.len(), 1);
            assert_eq!(retry.pending_targets[0].addr, "127.0.0.1:9101");
        })
    }

    /// While the delete loop holds a Delete's progress mid-pass, only it knows
    /// what is still pending: the stale sweep leaves that marker for its next
    /// tick instead of handing off the ORIGINAL target list.
    #[test]
    fn the_stale_sweep_waits_for_a_delete_mid_pass() {
        run(async {
            let m = AutumnManager::new();
            m.acquire_extent_inflight(30, delete_payload(30)).await.expect("delete");
            let far = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                + 7200;
            assert_eq!(m.sweep_stale_inflight_once(far, 1).await, 0);
            assert_eq!(m.extent_inflight_op(30), Some(ExtentOpKind::Delete));
            assert!(m.failed_deletes.borrow().get(&30).is_none());
        })
    }

    /// GC's question: of these extents, which have an op in flight. A follower
    /// has an empty ledger, so it says NOT_LEADER rather than "none".
    #[test]
    fn extents_in_flight_answers_which_are_busy() {
        use autumn_rpc::manager_rpc::{ExtentsInFlightReq, ExtentsInFlightResp, CODE_NOT_LEADER};
        let m = ec_in_flight();
        m.leader.set(true);
        let ask = |m: &AutumnManager| -> ExtentsInFlightResp {
            let bytes = m
                .handle_extents_in_flight(rkyv_encode(&ExtentsInFlightReq { extent_ids: vec![41, 42, 43] }))
                .unwrap();
            autumn_rpc::manager_rpc::rkyv_decode(&bytes).unwrap()
        };
        let r = ask(&m);
        assert_eq!(r.code, autumn_rpc::manager_rpc::CODE_OK, "{}", r.message);
        assert_eq!(r.in_flight, vec![42]);
        m.leader.set(false);
        assert_eq!(ask(&m).code, CODE_NOT_LEADER);
    }

    /// A fence node is not waited for by a delete: its files are reaped by its
    /// reconcile if it ever returns. A node no longer registered is fence too;
    /// a target without a uuid (a legacy node) cannot be judged and is kept.
    #[test]
    fn a_delete_does_not_wait_for_a_fence_target() {
        let m = AutumnManager::new();
        m._test_register_health_nodes([1, 2]);
        for (id, uuid) in [(1u64, "u-1"), (2, "u-2")] {
            m.store.inner.borrow_mut().nodes.get_mut(&id).unwrap().node_uuid = uuid.into();
        }
        let target = |uuid: &str| crate::extent_delete::DeleteTarget {
            addr: "127.0.0.1:9".to_string(),
            node_uuid: uuid.to_string(),
        };
        assert!(!m.delete_target_is_fence(&target("u-1")));
        set_override(&m, 1, NODE_OVERRIDE_FENCED);
        assert!(m.delete_target_is_fence(&target("u-1")));
        set_override(&m, 2, autumn_rpc::manager_rpc::NODE_OVERRIDE_MAINTENANCE);
        assert!(!m.delete_target_is_fence(&target("u-2")), "maintenance is waited for");
        assert!(m.delete_target_is_fence(&target("u-gone")));
        assert!(!m.delete_target_is_fence(&target("")));
    }

    /// stale ConvertToEc marker is WARN-only across multiple
    /// sweep ticks — never auto-released. Operator must manually
    /// remediate via Python ops tooling.
    #[test]
    fn sweep_excludes_convert_to_ec_from_auto_release() {
        run(async {
            let m = AutumnManager::new();
            m._test_register_health_nodes([1, 3, 5, 7]);
            m.acquire_extent_inflight(77, ec_payload(77))
                .await
                .expect("acquire");

            let now_future = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                + 7200;
            // Sweep multiple times — ConvertToEc must survive every tick.
            for _ in 0..3 {
                let released = m.sweep_stale_inflight_once(now_future, 1).await;
                assert_eq!(released, 0, "ConvertToEc must never count toward released");
                assert_eq!(
                    m.extent_inflight_op(77),
                    Some(ExtentOpKind::ConvertToEc),
                    "stale ConvertToEc marker survives every sweep tick"
                );
            }
        })
    }

    /// sweep does NOT release markers younger than the threshold. Uses a
    /// Delete marker — the only kind the sweep releases by age.
    #[test]
    fn sweep_leaves_fresh_markers_alone() {
        run(async {
            let m = AutumnManager::new();
            m.acquire_extent_inflight(42, delete_payload(42))
                .await
                .expect("acquire");
            m.delete_progress.borrow_mut().insert(
                42,
                crate::extent_delete::PendingDelete {
                    extent_id: 42,
                    pending_targets: vec![crate::extent_delete::DeleteTarget {
                        addr: "127.0.0.1:9101".to_string(),
                        node_uuid: String::new(),
                    }],
                    attempts: 0,
                },
            );

            // Sweep with `now` matching the marker's started_at — age = 0,
            // way below any sensible threshold.
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            let released = m.sweep_stale_inflight_once(now, 600).await;
            assert_eq!(released, 0, "fresh marker must not be released");
            assert_eq!(
                m.extent_inflight_op(42),
                Some(ExtentOpKind::Delete),
                "fresh Delete marker survives the sweep"
            );
        })
    }
}
