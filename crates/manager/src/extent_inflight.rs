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
        if self.inflight.borrow().contains_key(&extent_id) {
            return Err(AppError::Precondition(format!(
                "extent {extent_id} already has an in-flight op (in-memory)"
            )));
        }

        let record = MgrExtentInflightRecord::new(extent_id, payload, (*self.instance_id).clone());

        // The attempt nonce is this creation's etcd revision, so it must come
        // from the acquiring txn itself (see `txn_fenced_revision`). In memory
        // mode there are no revisions; the store's monotonic counter stands in.
        let nonce = if let Some(etcd) = &self.etcd {
            let key = Self::extent_inflight_key(extent_id);
            let value = rkyv_encode(&record).to_vec();
            let extra_cmp = vec![Cmp::create_revision(key.as_bytes(), 0)];
            let put_op = autumn_etcd::Op::put(key.as_bytes(), &value);
            match etcd.txn_fenced_revision(extra_cmp, vec![put_op]).await? {
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
        match self.inflight.borrow().get(&extent_id).and_then(|r| r.unpack()) {
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
        match self.inflight.borrow().get(&extent_id).and_then(|r| r.unpack()) {
            Some((_, ExtentOpPayload::Recovery(t))) => Some(t),
            _ => None,
        }
    }

    pub(crate) fn inflight_snapshot_ec_recovery(
        &self,
    ) -> (
        std::collections::HashSet<u64>,
        std::collections::HashSet<u64>,
    ) {
        let mut ec = std::collections::HashSet::new();
        let mut rec = std::collections::HashSet::new();
        for (id, r) in self.inflight.borrow().iter() {
            match r.kind() {
                Some(ExtentOpKind::ConvertToEc) => {
                    ec.insert(*id);
                }
                Some(ExtentOpKind::Recovery) => {
                    rec.insert(*id);
                }
                _ => {}
            }
        }
        (ec, rec)
    }

    /// Test-only convenience: simulate that `extent_id` is in flight with
    /// a ConvertToEc op. Replaces today's
    /// `m.ec_conversion_inflight.borrow_mut().insert(extent_id)` pattern in
    /// / mark_extent_available / split / etc. tests. Bypasses etcd
    /// CAS and the leader fence for direct in-memory state injection.
    #[cfg(test)]
    pub(crate) fn _test_mark_ec_inflight(&self, extent_id: u64) {
        let payload = ExtentOpPayload::ConvertToEc(MgrEcDispatchInflight {
            extent_id,
            target_nodes: vec![],
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
    #[cfg(test)]
    pub(crate) fn _test_clear_inflight(&self, extent_id: u64) {
        self.inflight.borrow_mut().remove(&extent_id);
    }

    /// test-only convenience for the Recovery op kind. Mirrors
    /// `_test_mark_ec_inflight` but inserts a Recovery payload.
    #[cfg(test)]
    pub(crate) fn _test_mark_recovery_inflight(&self, extent_id: u64, task: RecoveryTask) {
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
    /// `AUTUMN_MGR_INFLIGHT_SWEEP_INTERVAL_SECS` (default 60s), iterates
    /// the in-memory ledger; for any marker whose `started_at` is older
    /// than `AUTUMN_MGR_INFLIGHT_STALE_THRESHOLD_SECS` (default 600s =
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
    /// **ConvertToEc is WARN-only (supersedes the earlier blanket
    /// auto-release).** A released ConvertToEc marker opens a race with
    /// the original EN-side dispatch: a fresh `handle_force_ec_convert`
    /// can succeed in the gap and shuffle a *different* parity node
    /// assignment, then apply_ec_conversion_done writes the new layout
    /// while the original EN-side `convert_to_ec` is still running. The
    /// coordinator serialises the two, but the manager state can
    /// still record the second dispatch's `target_nodes` while the first
    /// dispatch's bytes are what hit disk. This is exactly the failure
    /// mode the rich EC-dispatch marker was added to prevent. The remediation for
    /// a stuck ConvertToEc marker is operator intervention via Python
    /// ops (`etcdctl get extent_inflight/<id>` + inspect EN state +
    /// decide whether to manually delete the marker after confirming EN
    /// finished). The WARN log fires every sweep tick so operators get
    /// the signal.
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
        let sweep_interval =
            std::time::Duration::from_secs(Self::stale_sweep_interval_secs().max(1) as u64);
        loop {
            compio::time::sleep(sweep_interval).await;
            if !self.leader.get() {
                continue;
            }
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            let threshold = Self::stale_threshold_secs();
            self.sweep_stale_inflight_once(now, threshold).await;
        }
    }

    /// stale threshold in seconds. Override with env
    /// `AUTUMN_MGR_INFLIGHT_STALE_THRESHOLD_SECS`. Default 600
    /// (10 minutes) — generous for typical recovery / EC convert
    /// durations on local + LAN setups; bumped if you have
    /// multi-GiB extents on slow networks. Clamped to >= 60 to
    /// avoid pathological values that would race against the
    /// acquire path.
    fn stale_threshold_secs() -> i64 {
        std::env::var("AUTUMN_MGR_INFLIGHT_STALE_THRESHOLD_SECS")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(600)
            .max(60)
    }

    /// RECOVERY markers get a SHORTER stale threshold than the
    /// general (Delete) one. Override with `AUTUMN_MGR_RECOVERY_INFLIGHT_STALE_SECS`,
    /// default 120s, clamped >= 30.
    ///
    // NOTE: the Recovery-specific stale threshold
    // (`AUTUMN_MGR_RECOVERY_INFLIGHT_STALE_SECS`, default 120 s) and its helper
    // are GONE. It existed only because a held marker froze re-dispatch, which
    // made a timeout the sole way back — and its value had to be hand-tuned to
    // sit just past the EN's own 10 x 10 s internal give-up budget, a magic
    // number pinned to another magic number (found by decommission chaos,
    // 2026-07-10). Recovery markers are now standing instructions that are
    // re-sent every tick, and a marker whose pinned executor is no longer online
    // is released by `recovery_dispatch_loop` against live node state. Delete
    // keeps the general 600 s threshold (it has its own retry queue);
    // ConvertToEc stays WARN-only.

    /// sweep interval in seconds. Override with env
    /// `AUTUMN_MGR_INFLIGHT_SWEEP_INTERVAL_SECS`. Default 60s.
    /// Clamped to >= 1.
    fn stale_sweep_interval_secs() -> i64 {
        std::env::var("AUTUMN_MGR_INFLIGHT_SWEEP_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(60)
            .max(1)
    }

    /// one sweep iteration. Extracted from the loop so unit tests
    /// can drive it with controlled `now` + `threshold_secs` values.
    ///
    /// ConvertToEc markers are WARN-only and never auto-released —
    /// see the loop's docstring for the safety argument. Only Recovery
    /// and Delete are auto-released here.
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
            // ConvertToEc is WARN-only. Releasing the marker would
            // open a race with the original EN-side dispatch — a fresh
            // force-ec-convert could shuffle a different parity assignment
            // and apply_ec_conversion_done would write the new layout while
            // the original EN-side bytes are what hit disk. Operator must
            // manually remediate (Python ops tooling).
            if matches!(kind, ExtentOpKind::ConvertToEc) {
                tracing::warn!(
                    extent_id = eid,
                    age_secs,
                    threshold_secs,
                    "stale ConvertToEc inflight marker — NOT auto-released. \
                     Operator must inspect EN state and decide manually. \
                     See crates/manager/CLAUDE.md note 21 for rationale."
                );
                continue;
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
            m.acquire_extent_inflight(42, ec_payload(42))
                .await
                .expect("first acquire");
            let err = m
                .acquire_extent_inflight(42, recovery_payload(42))
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
            m.acquire_extent_inflight(42, ec_payload(42))
                .await
                .expect("acquire");
            m.commit_extent_inflight_release(42);
            assert_eq!(m.extent_inflight_op(42), None);
            // Re-acquire after release works.
            m.acquire_extent_inflight(42, recovery_payload(42))
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
            m.acquire_extent_inflight(20, recovery_payload(20))
                .await
                .expect("recovery");
            assert_eq!(m.extent_inflight_op(20), Some(ExtentOpKind::Recovery));

            let released = m.release_recovery_markers_for_dead_executors().await;
            assert_eq!(released, vec![20], "a dead executor's marker must be released");
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
            m.acquire_extent_inflight(20, recovery_payload(20))
                .await
                .expect("recovery");
            // Register node 9 and drive it to Online the way a heartbeat does.
            {
                let mut s = m.store.inner.borrow_mut();
                s.nodes.insert(
                    9,
                    autumn_rpc::manager_rpc::MgrNodeInfo {
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

            let released = m.release_recovery_markers_for_dead_executors().await;
            assert!(
                released.is_empty(),
                "a live executor's marker must NOT be released: {released:?}"
            );
            assert_eq!(m.extent_inflight_op(20), Some(ExtentOpKind::Recovery));
        })
    }

    #[test]
    fn sweep_auto_releases_stale_markers() {
        run(async {
            let m = AutumnManager::new();
            // Three markers, one of each kind.
            m.acquire_extent_inflight(10, ec_payload(10))
                .await
                .expect("ec");
            m.acquire_extent_inflight(20, recovery_payload(20))
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
        })
    }

    /// stale ConvertToEc marker is WARN-only across multiple
    /// sweep ticks — never auto-released. Operator must manually
    /// remediate via Python ops tooling.
    #[test]
    fn sweep_excludes_convert_to_ec_from_auto_release() {
        run(async {
            let m = AutumnManager::new();
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

    /// sweep does NOT release markers younger than the threshold.
    #[test]
    fn sweep_leaves_fresh_markers_alone() {
        run(async {
            let m = AutumnManager::new();
            m.acquire_extent_inflight(42, ec_payload(42))
                .await
                .expect("acquire");

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
                Some(ExtentOpKind::ConvertToEc),
                "fresh ConvertToEc marker survives the sweep"
            );
        })
    }

    /// env var overrides clamp to safe minimums.
    #[test]
    fn env_var_clamping() {
        std::env::set_var("AUTUMN_MGR_INFLIGHT_STALE_THRESHOLD_SECS", "5");
        std::env::set_var("AUTUMN_MGR_INFLIGHT_SWEEP_INTERVAL_SECS", "0");
        // Threshold floor is 60, sweep interval floor is 1.
        assert_eq!(AutumnManager::stale_threshold_secs(), 60);
        assert_eq!(AutumnManager::stale_sweep_interval_secs(), 1);
        std::env::remove_var("AUTUMN_MGR_INFLIGHT_STALE_THRESHOLD_SECS");
        std::env::remove_var("AUTUMN_MGR_INFLIGHT_SWEEP_INTERVAL_SECS");
        // Without env, defaults apply.
        assert_eq!(AutumnManager::stale_threshold_secs(), 600);
        assert_eq!(AutumnManager::stale_sweep_interval_secs(), 60);
    }
}
