//! F207 Phase 0 — unified extent-level in-flight ledger.
//!
//! Replaces (over the course of F207-B / F207-C / F207-D) the four scattered
//! inflight bookkeeping mechanisms:
//!
//! | Pre-F207 mechanism                                          | Persistence            |
//! |-------------------------------------------------------------|------------------------|
//! | `ec_conversion_inflight: HashSet<u64>` (F138)               | in-memory              |
//! | `pending_ec_dispatch: HashMap<u64, MgrEcDispatchInflight>`  | etcd `ecConversionInflight/<id>` (F198) |
//! | `recovery_tasks: HashMap<u64, MgrRecoveryTask>`             | etcd `recoveryTasks/<id>` |
//! | `pending_extent_deletes: VecDeque<PendingDelete>` (F109)    | in-memory              |
//!
//! Phase 0 (this commit) adds the infrastructure types + acquire / release /
//! probe API + `replay_from_etcd` arm. **No existing handler is migrated
//! yet.** The new field is dormant; existing tests pass unchanged. Phase 1
//! (F207-B) migrates EC convert, Phase 2 (F207-C) recovery + delete,
//! Phase 3 (F207-D) cleanup.
//!
//! See `~/.claude/plans/stream-merge-split-ps-sorted-dijkstra.md` for the
//! full plan and PS-layer ↔ stream-layer interaction model.

use autumn_etcd::Cmp;
use autumn_etcd::proto::RequestOp;
use autumn_rpc::manager_rpc::{
    MgrEcDispatchInflight, MgrRecoveryTask, rkyv_decode, rkyv_encode,
};
use rkyv::{Archive, Deserialize, Serialize};

use autumn_common::error::AppError;

use crate::AutumnManager;

/// Etcd key prefix for the unified ledger. Note the snake_case spelling —
/// the older `ecConversionInflight/` and `recoveryTasks/` prefixes use
/// camelCase, but those will be drained one-cycle-after-deploy and removed
/// in F207-D. The new prefix matches our other snake_case prefixes
/// (`partitionVpRefs/` is the camelCase outlier; consistency was already
/// imperfect — pick whichever).
pub(crate) const EXTENT_INFLIGHT_PREFIX: &str = "extent_inflight/";

/// Discriminator for the kind of stream-layer op currently in flight on an
/// extent. Returned by the `extent_inflight_op` read-only probe so callers
/// can render a useful refuse-at-start error message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExtentOpKind {
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
    pub pending_addrs: Vec<String>,
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
    /// stale-marker WARN sweep (F207-D).
    pub started_at: i64,
    /// `instance_id` of the leader that acquired the marker. Parity with
    /// F149's pattern; useful for diagnostics.
    pub leader_id: String,
    pub ec_payload: Option<MgrEcDispatchInflight>,
    pub recovery_payload: Option<MgrRecoveryTask>,
    pub delete_payload: Option<PersistedPendingDelete>,
}

/// Typed view over a `MgrExtentInflightRecord`. Constructors enforce the
/// "exactly one variant populated" invariant; `op_kind()` derives the
/// discriminator from the variant rather than relying on the byte field.
#[derive(Clone, Debug)]
pub(crate) enum ExtentOpPayload {
    ConvertToEc(MgrEcDispatchInflight),
    Recovery(MgrRecoveryTask),
    Delete(PersistedPendingDelete),
}

impl ExtentOpPayload {
    pub(crate) fn kind(&self) -> ExtentOpKind {
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

// F207 Phase 0: API is wired but not yet called by any production handler.
// Phase 1 (F207-B) migrates EC convert; Phase 2 (F207-C) migrates
// recovery + delete. Until those land, the methods are exercised only by
// the unit tests below.
#[allow(dead_code)]
impl AutumnManager {
    /// Compose the etcd key for a given extent_id.
    pub(crate) fn extent_inflight_key(extent_id: u64) -> String {
        format!("{}{}", EXTENT_INFLIGHT_PREFIX, extent_id)
    }

    /// Atomically take exclusive in-flight on `extent_id`. CAS via etcd
    /// `create_revision == 0` + F149 leader fence (`txn_fenced`).
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
    /// handler. It's added now so F207-B / F207-C only swap call sites.
    pub(crate) async fn acquire_extent_inflight(
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

        if let Some(etcd) = &self.etcd {
            let key = Self::extent_inflight_key(extent_id);
            let value = rkyv_encode(&record).to_vec();
            let extra_cmp = vec![Cmp::create_revision(key.as_bytes(), 0)];
            let put_op = autumn_etcd::Op::put(key.as_bytes(), &value);
            let acquired = etcd.txn_fenced(extra_cmp, vec![put_op], vec![]).await?;
            if !acquired {
                return Err(AppError::Precondition(format!(
                    "extent {extent_id} already has an in-flight op (etcd CAS refused)"
                )));
            }
        }

        self.inflight.borrow_mut().insert(extent_id, record);
        Ok(())
    }

    /// Build the etcd delete op for the marker on `extent_id`. Caller
    /// appends this to its own op's apply-done txn batch so release is
    /// atomic with the state mutation (invariant I3).
    pub(crate) fn build_extent_inflight_release_op(&self, extent_id: u64) -> RequestOp {
        let key = Self::extent_inflight_key(extent_id);
        autumn_etcd::Op::delete(key.as_bytes())
    }

    /// In-memory companion to `build_extent_inflight_release_op`. Call
    /// AFTER your apply-done etcd txn has succeeded (etcd-first invariant
    /// per `crates/manager/CLAUDE.md` programming note 1).
    pub(crate) fn commit_extent_inflight_release(&self, extent_id: u64) {
        self.inflight.borrow_mut().remove(&extent_id);
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

    /// F207-C helper for PS-layer handlers that iterate over many
    /// extent_ids and need to consult the ledger multiple times under a
    /// `store.inner.borrow_mut`. Snapshots the ConvertToEc-kind and
    /// Recovery-kind sets in one ledger borrow. Replaces the pre-F207
    /// `(let ec_inflight = self.ec_conversion_inflight.borrow(); let
    /// recovery_inflight = self.recovery_tasks.borrow())` pair.
    pub(crate) fn inflight_snapshot_ec_recovery(
        &self,
    ) -> (std::collections::HashSet<u64>, std::collections::HashSet<u64>) {
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

    /// F207-C helper: ec + recovery + delete snapshot. Used by
    /// `handle_multi_modify_merge` which today consults all three sets.
    pub(crate) fn inflight_snapshot_three(
        &self,
    ) -> (
        std::collections::HashSet<u64>,
        std::collections::HashSet<u64>,
        std::collections::HashSet<u64>,
    ) {
        let mut ec = std::collections::HashSet::new();
        let mut rec = std::collections::HashSet::new();
        let mut del = std::collections::HashSet::new();
        for (id, r) in self.inflight.borrow().iter() {
            match r.kind() {
                Some(ExtentOpKind::ConvertToEc) => {
                    ec.insert(*id);
                }
                Some(ExtentOpKind::Recovery) => {
                    rec.insert(*id);
                }
                Some(ExtentOpKind::Delete) => {
                    del.insert(*id);
                }
                _ => {}
            }
        }
        (ec, rec, del)
    }

    /// Test-only convenience: simulate that `extent_id` is in flight with
    /// a ConvertToEc op. Replaces today's
    /// `m.ec_conversion_inflight.borrow_mut().insert(extent_id)` pattern in
    /// F138 / mark_extent_available / split / etc. tests. Bypasses etcd
    /// CAS and the F149 fence for direct in-memory state injection.
    #[cfg(test)]
    pub(crate) fn _test_mark_ec_inflight(&self, extent_id: u64) {
        let payload = ExtentOpPayload::ConvertToEc(MgrEcDispatchInflight {
            extent_id,
            target_nodes: vec![],
            extra_disk_ids: vec![],
            data_shards: 0,
            new_eversion: 0,
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

    /// F207-C: test-only convenience for the Recovery op kind. Mirrors
    /// `_test_mark_ec_inflight` but inserts a Recovery payload.
    #[cfg(test)]
    pub(crate) fn _test_mark_recovery_inflight(&self, extent_id: u64, task: MgrRecoveryTask) {
        let payload = ExtentOpPayload::Recovery(task);
        let record = MgrExtentInflightRecord::new(extent_id, payload, "test".to_string());
        self.inflight.borrow_mut().insert(extent_id, record);
    }

    /// F207-C: test-only convenience for the Delete op kind.
    #[cfg(test)]
    pub(crate) fn _test_mark_delete_inflight(&self, extent_id: u64, pending_addrs: Vec<String>) {
        let payload = ExtentOpPayload::Delete(PersistedPendingDelete {
            extent_id,
            pending_addrs,
        });
        let record = MgrExtentInflightRecord::new(extent_id, payload, "test".to_string());
        self.inflight.borrow_mut().insert(extent_id, record);
    }

    /// F207-D: stale-marker WARN sweep. Iterates the in-memory ledger
    /// every 5 minutes and logs WARN for any marker older than the
    /// stale threshold (24h by default). Auto-clearing is intentionally
    /// not done — a stuck marker usually signals a real bug we want to
    /// surface, not silently paper over. Operator runs the Python ops
    /// `--clear-stale-inflight extent <id>` script after investigating.
    ///
    /// Invariant I2 in the F207 plan: every acquire has a matching
    /// release, OR leader failover replay reclaims the in-memory shadow
    /// from etcd. A stale marker means BOTH paths failed for some
    /// extent — usually a manager bug (e.g., the apply path returned
    /// early without bundling the release into its txn).
    pub(crate) async fn extent_inflight_stale_sweep_loop(self) {
        const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300); // 5 min
        const STALE_THRESHOLD_SECS: i64 = 24 * 3600; // 24 h
        loop {
            compio::time::sleep(SWEEP_INTERVAL).await;
            if !self.leader.get() {
                continue;
            }
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            let stale: Vec<(u64, ExtentOpKind, i64)> = self
                .inflight
                .borrow()
                .iter()
                .filter_map(|(eid, rec)| {
                    rec.kind().and_then(|kind| {
                        let age = now - rec.started_at;
                        if age >= STALE_THRESHOLD_SECS {
                            Some((*eid, kind, age))
                        } else {
                            None
                        }
                    })
                })
                .collect();
            for (eid, kind, age_secs) in stale {
                tracing::warn!(
                    extent_id = eid,
                    op = ?kind,
                    age_secs,
                    "F207-D: stale extent_inflight marker (> 24h). Indicates a \
                     leader-replay missed release or an aborted apply path. \
                     Investigate with `etcdctl get extent_inflight/<id>`; run \
                     Python ops `--clear-stale-inflight extent <id>` after \
                     verifying the op is genuinely abandoned."
                );
            }
        }
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
                            "F207: extent_inflight record has malformed op_kind / payload \
                             combination; skipping on replay"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        extent_id,
                        error = %e,
                        "F207: failed to rkyv-decode extent_inflight value; skipping on replay"
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
        })
    }

    fn recovery_payload(extent_id: u64) -> ExtentOpPayload {
        ExtentOpPayload::Recovery(MgrRecoveryTask {
            extent_id,
            replace_id: 1,
            node_id: 9,
            start_time: 0,
        })
    }

    fn delete_payload(extent_id: u64) -> ExtentOpPayload {
        ExtentOpPayload::Delete(PersistedPendingDelete {
            extent_id,
            pending_addrs: vec!["127.0.0.1:9101".to_string(), "127.0.0.1:9102".to_string()],
        })
    }

    fn run<F: std::future::Future<Output = T>, T>(f: F) -> T {
        compio::runtime::Runtime::new().unwrap().block_on(f)
    }

    /// F207 Phase 0: acquire on an empty extent succeeds; in-memory shadow
    /// reflects the op kind.
    #[test]
    fn f207_acquire_extent_inflight_succeeds_when_empty() {
        run(async {
            let m = AutumnManager::new();
            m.acquire_extent_inflight(42, ec_payload(42)).await.expect("acquire");
            assert_eq!(m.extent_inflight_op(42), Some(ExtentOpKind::ConvertToEc));
        })
    }

    /// F207 Phase 0: second acquire on the same extent is refused with
    /// Precondition. Replaces today's F126/F138/F139 ad-hoc checks once
    /// the migration phases land.
    #[test]
    fn f207_acquire_extent_inflight_rejects_duplicate() {
        run(async {
            let m = AutumnManager::new();
            m.acquire_extent_inflight(42, ec_payload(42)).await.expect("first acquire");
            let err = m
                .acquire_extent_inflight(42, recovery_payload(42))
                .await
                .expect_err("second acquire must fail");
            assert!(matches!(err, AppError::Precondition(_)), "got {err:?}");
            // The original op's kind is unchanged.
            assert_eq!(m.extent_inflight_op(42), Some(ExtentOpKind::ConvertToEc));
        })
    }

    /// F207 Phase 0: in-memory release clears the probe.
    #[test]
    fn f207_commit_extent_inflight_release_clears_probe() {
        run(async {
            let m = AutumnManager::new();
            m.acquire_extent_inflight(42, ec_payload(42)).await.expect("acquire");
            m.commit_extent_inflight_release(42);
            assert_eq!(m.extent_inflight_op(42), None);
            // Re-acquire after release works.
            m.acquire_extent_inflight(42, recovery_payload(42)).await.expect("re-acquire");
            assert_eq!(m.extent_inflight_op(42), Some(ExtentOpKind::Recovery));
        })
    }

    /// F207 Phase 0: rkyv round-trip on all three payload variants.
    #[test]
    fn f207_record_rkyv_round_trip_all_variants() {
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

    /// F207 Phase 0: unpack rejects records whose `op_kind` and populated
    /// payload field disagree. Replay drops these with a WARN.
    #[test]
    fn f207_unpack_rejects_malformed_records() {
        // op_kind = ConvertToEc but no ec_payload populated.
        let bad = MgrExtentInflightRecord {
            extent_id: 42,
            op_kind: ExtentOpKind::CONVERT_TO_EC,
            started_at: 0,
            leader_id: "test".to_string(),
            ec_payload: None,
            recovery_payload: Some(MgrRecoveryTask::default()),
            delete_payload: None,
        };
        assert!(bad.unpack().is_none(), "mismatched op_kind/payload must not unpack");

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
        assert!(bad.unpack().is_none(), "extra payload field set must not unpack");

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

    /// F207 Phase 0: decode_extent_inflight_kvs filters malformed records.
    #[test]
    fn f207_decode_drops_malformed() {
        let good = MgrExtentInflightRecord::new(
            42,
            ec_payload(42),
            "leader-a".to_string(),
        );
        let good_bytes = rkyv_encode(&good).to_vec();

        let kvs: Vec<(u64, Vec<u8>)> = vec![
            (42, good_bytes),
            (99, b"not rkyv at all".to_vec()),
        ];
        let decoded = AutumnManager::decode_extent_inflight_kvs(
            kvs.iter().map(|(id, b)| (*id, b.as_slice())),
        );
        assert_eq!(decoded.len(), 1, "malformed record dropped");
        assert_eq!(decoded[0].0, 42);
    }
}
