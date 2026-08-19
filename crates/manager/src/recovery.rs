//! Recovery dispatch/collect loops and EC conversion for AutumnManager.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use autumn_common::AppError;
use autumn_rpc::manager_rpc::*;

use crate::{AutumnManager, PendingDelete};

/// The coordinator pinned by an EC dispatch marker: shard index 0 drives the
/// conversion, so `target_nodes[0]` is the ONLY node whose completion report may
/// be applied for that marker.
fn params_coordinator(p: &MgrEcDispatchInflight) -> Option<u64> {
    p.target_nodes.first().copied()
}

/// One EC conversion the dispatch loop will (re-)dispatch this tick: the sealed
/// `ex`, the `stream` whose `(K, M)` shape it converts to, and the authoritative
/// ledger marker (`params`: target nodes / extra disks / post-EC eversion /
/// owner_epoch) to reuse verbatim. Built by `collect_ec_dispatch_candidates`,
/// consumed by `dispatch_one_ec_conversion`.
struct EcDispatchCandidate {
    ex: MgrExtentInfo,
    stream: MgrStreamInfo,
    params: MgrEcDispatchInflight,
}

// ── TEST-ONLY failpoint for the G4 / BUG-EC-APPLY-FAIL reproduce harness ──
// A one-shot, self-clearing flag that forces the NEXT `apply_ec_conversion_done`
// call to return a transient `Internal` error WITHOUT touching etcd/leadership
// (models an etcd blip while this manager stays leader). Compiled out of every
// non-test build. See `ec_g4_wedge_harness.rs`.
#[cfg(test)]
thread_local! {
    static EC_APPLY_FAIL_ONCE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Arm the one-shot EC-apply failpoint (test-only).
#[cfg(test)]
pub(crate) fn _test_arm_ec_apply_fail_once() {
    EC_APPLY_FAIL_ONCE.with(|c| c.set(true));
}

impl AutumnManager {
    /// Release Recovery markers whose pinned executor can no longer run them.
    ///
    /// This is the event that ends a marker's life when completion never comes.
    /// It is evaluated LEVEL-TRIGGERED — re-derived from live node state every
    /// tick rather than hooked onto the fence/remove/suspect transitions — so it
    /// cannot miss an edge, needs no bookkeeping in three handlers, and
    /// re-converges after a leader failover that never saw the transition.
    ///
    /// Safe unconditionally: nothing is committed until `apply_recovery_done`,
    /// so releasing only discards an attempt. If the executor was in fact still
    /// working and finishes later, its completion is refused (no marker) — and
    /// the next tick re-detects the degraded slot, re-dispatches, and that node
    /// ADOPTS its now verified-complete copy and re-reports. Convergent either
    /// way.
    ///
    /// Returns the extents whose markers were released (for tests/diagnostics).
    pub(crate) async fn release_recovery_markers_for_dead_executors(&self) -> Vec<u64> {
        let dead: Vec<(u64, u64)> = {
            let states = self.node_states.borrow();
            let nodes = self.store.inner.borrow();
            self.inflight
                .borrow()
                .values()
                .filter_map(|rec| match rec.unpack() {
                    Some((_, crate::extent_inflight::ExtentOpPayload::Recovery(t))) => {
                        let gone = !nodes.nodes.contains_key(&t.node_id)
                            || !states.state_of(t.node_id).is_online();
                        gone.then_some((t.extent_id, t.node_id))
                    }
                    _ => None,
                })
                .collect()
        };
        let mut released = Vec::new();
        for (extent_id, node_id) in dead {
            match self.drain_extent_inflight_marker(extent_id).await {
                Ok(()) => {
                    tracing::info!(
                        extent_id,
                        node_id,
                        "released Recovery marker: its pinned executor is no longer online \
                         — re-dispatch will pick a live target"
                    );
                    released.push(extent_id);
                }
                Err(e) => tracing::warn!(
                    extent_id,
                    node_id,
                    "could not release the Recovery marker of a dead executor \
                     (retried next tick): {e}"
                ),
            }
        }
        released
    }

    /// Re-send an already-pinned Recovery marker to the node it named.
    ///
    /// This is the "standing instruction" half of the marker model. It NEVER
    /// re-runs candidate selection (the assignment was decided once, at acquire
    /// time) and NEVER drains the marker: an unreachable executor is not
    /// evidence that the task should be abandoned, only that this tick could not
    /// reach it. Releasing a marker whose target is genuinely gone is an
    /// event-driven decision (node offline/fenced/removed), not a timeout.
    ///
    /// Always `Ok`: a re-send is a keep-alive, not an attempt whose failure
    /// should feed the (extent, slot) backoff.
    async fn redispatch_pinned_recovery(&self, extent_id: u64) -> Result<(), AppError> {
        let Some(task) = self.extent_inflight_payload_recovery(extent_id) else {
            // Raced with a release — the next tick re-derives from scratch.
            return Ok(());
        };
        // Don't spend a timeout on a node the manager already knows is not
        // Online — a keep-alive to a corpse costs the whole dispatch tick, once
        // per pinned marker. Releasing such a marker is event-driven (the node
        // going offline/fenced/removed), not this path's job.
        if !self
            .node_states
            .borrow()
            .state_of(task.node_id)
            .is_online()
        {
            return Ok(());
        }
        let addr = {
            let s = self.store.inner.borrow();
            match s.nodes.get(&task.node_id) {
                Some(n) => {
                    let base = Self::normalize_endpoint(&n.address);
                    Self::shard_addr_for_extent(&base, &n.shard_ports, extent_id)
                }
                None => return Ok(()), // node gone from the map; offline handling owns it
            }
        };
        let node_id = task.node_id;
        let payload = rkyv_encode(&ExtRequireRecoveryReq { task });
        match self
            .conn_pool
            .call_timeout(
                &addr,
                EXT_MSG_REQUIRE_RECOVERY,
                payload,
                // Short: this is a keep-alive to a node believed Online, not the
                // initial dispatch. A slow answer just means we re-send next tick.
                Duration::from_secs(5),
            )
            .await
        {
            Ok(resp) => match rkyv_decode::<ExtCodeResp>(&resp) {
                // CODE_OK covers both "started it" and "already running it".
                Ok(r) if r.code == CODE_OK => {}
                Ok(r) => tracing::debug!(
                    extent_id,
                    node_id,
                    "recovery re-dispatch refused: {}",
                    r.message
                ),
                Err(e) => tracing::debug!(extent_id, "recovery re-dispatch decode: {e}"),
            },
            Err(e) => tracing::debug!(
                extent_id,
                node_id,
                "recovery re-dispatch unreachable: {e}"
            ),
        }
        Ok(())
    }

    pub(crate) async fn dispatch_recovery_task(
        &self,
        extent_id: u64,
        replace_id: u64,
    ) -> Result<(), AppError> {
        // any in-flight stream-layer op on this
        // extent blocks recovery dispatch. This was previously three
        // separate ad-hoc checks (recovery_tasks dedup, ec_conversion_inflight,
        // pending_extent_deletes), now collapsed into one ledger
        // read. The probe distinguishes "already-recovering (idempotent
        // OK)" from "different op in flight (caller retries)".
        match self.extent_inflight_op(extent_id) {
            // A Recovery marker is a STANDING INSTRUCTION, not a "someone else
            // has this, skip" flag: re-send it to the node it pinned. Skipping
            // was what made a wall-clock TTL necessary — an executor that
            // restarted (losing its in-memory in-flight set) or gave up silently
            // left a marker nobody would ever act on again, so the only way back
            // was to time the marker out. Re-sending covers those cases in one
            // dispatch tick instead, and the EN answers an already-running task
            // with CODE_OK (idempotent accept).
            Some(crate::extent_inflight::ExtentOpKind::Recovery) => {
                return self.redispatch_pinned_recovery(extent_id).await;
            }
            Some(_) => return Ok(()),
            None => {}
        }

        // never rebuild a replica onto a fenced / maintenance /
        // suspected node — that would just create more work to migrate off.
        // Captured before the store borrow (disjoint RefCells).
        let hard_excluded = self.placement_excluded_node_ids();
        let (extent, candidates) = {
            let s = self.store.inner.borrow();
            let extent = s
                .extents
                .get(&extent_id)
                .cloned()
                .ok_or_else(|| AppError::NotFound(format!("extent {extent_id}")))?;
            let occupied = Self::extent_nodes(&extent)
                .into_iter()
                .collect::<HashSet<_>>();
            let mut all = s
                .nodes
                .values()
                .filter(|n| !occupied.contains(&n.node_id))
                .filter(|n| !hard_excluded.contains(&n.node_id))
                .cloned()
                .collect::<Vec<_>>();
            all.sort_by_key(|n| n.node_id);
            (extent, all)
        };

        if candidates.is_empty() {
            return Err(AppError::Precondition(
                "no candidate node for recovery".to_string(),
            ));
        }

        let mut rate_limited = false;
        for candidate in &candidates {
            // gate this (source -> target) dispatch on the rate
            // limiter (reseeded from the ledger at the top of the dispatch
            // loop). On cap-hit try the NEXT candidate — a different target
            // may have headroom; if every candidate is capped we return Ok
            // below (deferred, NOT a failure: no backoff, retried next tick
            // once capacity frees). The slot is released on any RPC-failure
            // path below; on success it stays counted (and is re-derived
            // from the ledger on the next tick's reseed).
            if !self
                .recovery_limiter
                .borrow_mut()
                .try_acquire(replace_id, candidate.node_id)
            {
                rate_limited = true;
                continue;
            }
            let base = Self::normalize_endpoint(&candidate.address);
            // recovery targets a specific extent_id → route to owner shard.
            let addr = Self::shard_addr_for_extent(&base, &candidate.shard_ports, extent_id);

            let task = MgrRecoveryTask {
                extent_id,
                replace_id,
                node_id: candidate.node_id,
                start_time: Self::epoch_seconds(),
            };

            // acquire the unified inflight marker BEFORE the
            // EN RPC. The order was previously reversed (RPC → check
            // code → acquire): if `acquire_extent_inflight` failed
            // (NotLeader during the etcd CAS await, etcd transient,
            // or someone else acquired between our probe at L24 and
            // our acquire), the EN was ALREADY running `run_recovery_task`
            // with no corresponding manager ledger entry. apply_recovery_done
            // (which validates state from the ledger) would later be
            // missing the I3 atomic put_and_delete_txn's delete target,
            // and the ledger invariants drift.
            //
            // Now: acquire first; on success → RPC; if RPC fails or EN
            // rejects, drain the marker (release etcd + in-memory) and
            // try the next candidate. If acquire returns Precondition
            // (someone else holds the marker), return Ok — the in-flight
            // recovery is functionally what we wanted.
            match self
                .acquire_extent_inflight(
                    extent.extent_id,
                    crate::extent_inflight::ExtentOpPayload::Recovery(task.clone()),
                )
                .await
            {
                Ok(()) => {}
                Err(AppError::Precondition(_)) => return Ok(()),
                Err(other) => return Err(other),
            }

            let payload = rkyv_encode(&ExtRequireRecoveryReq { task });
            // 30 s ceiling — REQUIRE_RECOVERY only kicks off the
            // background `run_recovery_task` on the EN; the EN returns
            // OK immediately. A paged-out / dead EN otherwise wedges
            // this loop and starves recovery of all other extents.
            let resp = match self
                .conn_pool
                .call_timeout(
                    &addr,
                    EXT_MSG_REQUIRE_RECOVERY,
                    payload,
                    Duration::from_secs(30),
                )
                .await
            {
                Ok(v) => v,
                Err(_) => {
                    // RPC failed → release the marker we just acquired
                    // and try the next candidate. We don't know if the
                    // EN received the request; if it did, the EN-side
                    // recovery_inflight tracks it; we'll re-dispatch
                    // on the next tick and the EN-side check will idempotently
                    // refuse the duplicate.
                    let _ = self.drain_extent_inflight_marker(extent.extent_id).await;
                    // release the limiter slot we took above.
                    self.recovery_limiter
                        .borrow_mut()
                        .release(replace_id, candidate.node_id);
                    continue;
                }
            };
            let r: ExtCodeResp = match rkyv_decode(&resp) {
                Ok(v) => v,
                Err(_) => {
                    let _ = self.drain_extent_inflight_marker(extent.extent_id).await;
                    // release the limiter slot we took above.
                    self.recovery_limiter
                        .borrow_mut()
                        .release(replace_id, candidate.node_id);
                    continue;
                }
            };
            if r.code != CODE_OK {
                // EN rejected (e.g. extent exists locally already, or
                // recovery_inflight conflict). Release marker
                // and try next candidate.
                let _ = self.drain_extent_inflight_marker(extent.extent_id).await;
                // release the limiter slot we took above.
                self.recovery_limiter
                    .borrow_mut()
                    .release(replace_id, candidate.node_id);
                continue;
            }

            // Both acquire AND RPC succeeded. Marker stays in place
            // until apply_recovery_done's atomic put_and_delete_txn
            // releases it (invariant I3).
            //
            // Op-ledger: the EN accepted the rebuild, so this extent's recovery
            // is genuinely RUNNING — record it (create-or-count-attempt) so an
            // operator can see it in `ops list --kind recovery` instead of only
            // in aggregate `recovery-stats`.
            {
                let slot = {
                    let s = self.store.inner.borrow();
                    s.extents
                        .get(&extent.extent_id)
                        .and_then(|ex| Self::extent_slot(ex, replace_id))
                        .unwrap_or(0) as u32
                };
                let (now_s, now_ms) = Self::now_s_ms();
                self.ops.borrow_mut().note_recovery_dispatch(
                    extent.extent_id,
                    slot,
                    candidate.node_id,
                    now_s,
                    now_ms,
                );
            }
            return Ok(());
        }

        // if we never reached the RPC because every candidate was
        // rate-limited, this is a deferral, not a failure. Return Ok so
        // the dispatch loop records success (no backoff) and retries next
        // tick once the limiter has headroom. A genuine all-candidates-
        // RPC-rejected outcome (rate_limited == false) still backs off.
        if rate_limited {
            return Ok(());
        }
        Err(AppError::Precondition(
            "all recovery candidates rejected".to_string(),
        ))
    }

    /// best-effort Recovery inflight-marker release. Drops the etcd
    /// `extent_inflight/<id>` marker and the in-memory marker. The etcd delete
    /// is best-effort: a transient failure is WARN-logged, NOT propagated —
    /// the in-memory marker is released regardless so the extent isn't blocked,
    /// and the stale-marker sweep (~10 min ceiling) reclaims the etcd
    /// marker. Propagating (`?`) would skip the in-memory release + each
    /// caller's follow-up cleanup (e.g. the extent-removed branch's
    /// `enqueue_pending_deletes`) — a worse regression than a transient
    /// recovery-cleanup retry that is already backstopped by the sweep +
    /// node-startup reconcile. (Dedups the 3 byte-identical release blocks in
    /// `apply_recovery_done`'s ec-inflight / slot-gone / extent-removed paths.)
    async fn release_recovery_marker_best_effort(&self, extent_id: u64) {
        if let Some(etcd) = &self.etcd {
            if let Err(e) = etcd
                .put_and_delete_txn(Vec::new(), vec![Self::extent_inflight_key(extent_id)])
                .await
            {
                tracing::warn!(
                    extent_id,
                    error = %e,
                    "recovery inflight marker etcd-release failed; in-memory released, stale-marker sweep reclaims the etcd marker"
                );
            }
        }
        self.commit_extent_inflight_release(extent_id);
    }

    pub(crate) async fn apply_recovery_done(
        &self,
        done_task: MgrRecoveryTaskDone,
    ) -> Result<(), AppError> {
        let task = &done_task.task;

        // if EC conversion is in flight for this extent,
        // defer the recovery apply. apply_ec_conversion_done would
        // overwrite both ex.replicates (reverting the slot replacement)
        // and ex.eversion (losing the recovery's eversion bump). The
        // Recovery marker is KEPT (we return before any release).
        // CAVEAT (pre-existing, deferred): the df handler
        // mem::take's recovery_done once, so THIS completion is consumed
        // and NOT re-delivered — the "retry next tick" the old comment
        // promised does not happen. Convergence is instead via the
        // stale-marker sweep (releases the kept marker, ~10 min ceiling)
        // followed by recovery_dispatch_loop re-evaluating the now-EC'd
        // extent's per-slot health and re-recovering any genuinely-missing
        // shard. A manager-side completion-retry queue would converge
        // faster but is a new mechanism in a revert-prone path — deferred
        // until the slow-convergence is reproduced as real harm (stale-marker
        // sweep + orphan-reconcile backstop correctness today).
        // reads the unified ledger via `extent_inflight_op`.
        if matches!(
            self.extent_inflight_op(task.extent_id),
            Some(crate::extent_inflight::ExtentOpKind::ConvertToEc)
        ) {
            return Err(AppError::Precondition(format!(
                "ec conversion in flight on extent {}; deferring recovery apply",
                task.extent_id
            )));
        }

        // precheck — if `task.node_id` is already present in this
        // extent at a slot OTHER than the failed `replace_id`, the layout
        // has changed since dispatch (typically EC conversion completed
        // during recovery and assigned this node as parity). Applying the
        // recovery would produce a duplicate-node state where one node
        // holds two shards; reads of the duplicated slot would return
        // whatever shard EC conversion last wrote there (parity bytes in
        // the documented production case), corrupting the read path.
        // Discard the stale task (memory + etcd) so the dedup check in
        // `dispatch_recovery_task` doesn't permanently block future
        // attempts to repair this slot.
        let layout_changed = {
            let s = self.store.inner.borrow();
            match s.extents.get(&task.extent_id) {
                Some(ex) => Self::extent_slot(ex, task.replace_id).map(|slot| {
                    Self::extent_nodes(ex)
                        .iter()
                        .enumerate()
                        .any(|(i, &id)| i != slot && id == task.node_id)
                }),
                None => Some(false),
            }
        };
        if matches!(layout_changed, Some(true)) {
            // release the Recovery marker so the dedup check in
            // dispatch_recovery_task doesn't permanently block future
            // attempts to repair this slot. Legacy `recoveryTasks/<id>`
            // delete dropped (the backward-compat dual-key path lived in
            // only).
            self.release_recovery_marker_best_effort(task.extent_id).await;
            return Err(AppError::Precondition(format!(
                "recovery target {} for extent {} already in extent node list at a different slot; \
                 likely EC conversion completed during recovery — discarding stale apply",
                task.node_id, task.extent_id
            )));
        }

        // layout_changed == None ⇒ the extent exists but
        // `task.replace_id` is no longer in any slot (extent_slot
        // returned None above). This case previously fell through to
        // the apply block where the inner `slot None` branch did
        // `return Err(...)` from inside borrow_mut WITHOUT releasing
        // the Recovery marker — violating invariant I3 (every
        // acquire has a matching release). The marker survived until
        // the stale-marker sweep (~10 min), blocking any other op on the extent
        // for that window. Release now, then return.
        if layout_changed.is_none() {
            self.release_recovery_marker_best_effort(task.extent_id).await;
            return Err(AppError::Precondition(format!(
                "replace_id {} not in extent {}",
                task.replace_id, task.extent_id
            )));
        }

        // etcd-first: compute updated_extent from a clone under
        // read-only borrow. The borrow_mut block previously mutated
        // s.extents[task.extent_id] in place, then the etcd put_and_delete_txn
        // below ran. If etcd failed (NotLeader / fence break), the in-memory
        // mutation had already advanced ex.replicates / eversion / avali —
        // a window where reads observed the new state but etcd had the old.
        // Replay rolled in-memory back later, but during the window other
        // mutating handlers could derive their snapshots from the
        // "fake-applied" state.
        let updated_extent = {
            let s = self.store.inner.borrow();
            match s.extents.get(&task.extent_id) {
                Some(ex) => {
                    let slot = match Self::extent_slot(ex, task.replace_id) {
                        Some(v) => v,
                        // Unreachable under single-threaded compio: the
                        // layout_changed.is_none() branch above
                        // already covered this. Kept as defense.
                        None => {
                            return Err(AppError::Precondition(format!(
                                "replace_id {} not in extent {}",
                                task.replace_id, task.extent_id
                            )));
                        }
                    };

                    let mut new_ex = ex.clone();
                    if slot < new_ex.replicates.len() {
                        new_ex.replicates[slot] = task.node_id;
                        if new_ex.replicate_disks.len() <= slot {
                            new_ex.replicate_disks.resize(slot + 1, 0);
                        }
                        new_ex.replicate_disks[slot] = done_task.ready_disk_id;
                    } else {
                        let parity_slot = slot - new_ex.replicates.len();
                        new_ex.parity[parity_slot] = task.node_id;
                        if new_ex.parity_disks.len() <= parity_slot {
                            new_ex.parity_disks.resize(parity_slot + 1, 0);
                        }
                        new_ex.parity_disks[parity_slot] = done_task.ready_disk_id;
                    }
                    new_ex.avali |= 1u32 << slot;
                    new_ex.eversion += 1;
                    Some(new_ex)
                }
                None => None,
            }
        };

        let Some(updated_extent) = updated_extent else {
            // The extent was removed from manager state before recovery
            // completed. Release the Recovery marker, then
            // enqueue a targeted delete for the recovering node so the
            // resurrected on-disk files are reaped promptly instead of
            // waiting for the 5-minute orphan-reconcile sweep. Recovery
            // release MUST happen before the Delete acquire because the
            // ledger is exclusive-per-extent.
            let maybe_addr: Option<String> = {
                let s = self.store.inner.borrow();
                s.nodes.get(&task.node_id).map(|n| {
                    let base = Self::normalize_endpoint(&n.address);
                    Self::shard_addr_for_extent(&base, &n.shard_ports, task.extent_id)
                })
            };
            // Release Recovery (etcd + in-memory). The legacy-key delete
            // entry was removed.
            self.release_recovery_marker_best_effort(task.extent_id).await;
            // Then enqueue Delete (best effort — extent_delete_loop will
            // pick it up on next tick).
            if let Some(addr) = maybe_addr {
                let _ = self
                    .enqueue_pending_deletes(vec![PendingDelete {
                        extent_id: task.extent_id,
                        pending_addrs: vec![addr],
                        attempts: 0,
                    }])
                    .await;
            }
            return Ok(());
        };

        if let Some(etcd) = &self.etcd {
            // atomic put + delete txn. Releases the Recovery
            // marker in the same txn that writes the updated extent
            // state. Legacy `recoveryTasks/<id>` delete dropped.
            let ex_payload = rkyv_encode(&updated_extent).to_vec();
            etcd.put_and_delete_txn(
                vec![(format!("extents/{}", updated_extent.extent_id), ex_payload)],
                vec![Self::extent_inflight_key(updated_extent.extent_id)],
            )
            .await?;
        }

        // only AFTER etcd success do we apply to in-memory.
        // (This was previously the borrow_mut block above; now the
        // borrow_mut here is the sole in-memory write.)
        {
            let mut s = self.store.inner.borrow_mut();
            s.extents
                .insert(updated_extent.extent_id, updated_extent.clone());
        }
        self.commit_extent_inflight_release(updated_extent.extent_id);
        // Op-ledger: the extent layout is repaired — close the recovery entry.
        {
            let (now_s, _) = Self::now_s_ms();
            self.ops.borrow_mut().complete_recovery(
                updated_extent.extent_id,
                format!("recovered slot onto node {}", task.node_id),
                now_s,
            );
        }
        Ok(())
    }

    /// Dispatch a recovery for `(extent_id, slot → node_id)` and record the
    /// outcome in the rate limiter (success clears backoff; failure backs off
    /// `(extent_id, slot)`). Dedups the four byte-identical dispatch tails in
    /// `recovery_dispatch_loop` (fenced / disk-offline / avali==0 / unhealthy-
    /// probe). Keeps the `&Result` passthrough so the failure reason is never
    /// dropped (note 30).
    async fn dispatch_and_record(&self, extent_id: u64, slot: u32, node_id: u64, now_s: i64) {
        let res = self.dispatch_recovery_task(extent_id, node_id).await;
        self.record_dispatch_outcome(extent_id, slot, now_s, &res);
    }

    pub(crate) async fn recovery_dispatch_loop(self) {
        loop {
            compio::time::sleep(Duration::from_secs(2)).await;
            if !self.leader.get() {
                continue;
            }

            // gate on `AUTUMN_MGR_RECOVERY_GATE`:
            //   - `fenced_only` (default): trigger recovery ONLY when the
            //     replica's node is operator-Fenced. Pre-fence transient
            //     failures stop causing cross-node rebuilds.
            //   - `auto_disk`: legacy behaviour (trigger on disk.online
            //     == false). For ops who haven't yet stood up the
            //     OP policy script.
            let gate_mode = Self::recovery_gate_mode();

            // maintenance-TTL tick — clear expired Maintenance
            // overrides before the dispatch decision. Cheap.
            self.tick_maintenance_ttl().await;

            // snapshot operator overrides so the body's fenced-gate
            // decision is consistent within this tick. (The node auto-state
            // snapshot was dead — recovery dispatch gates on Fenced only;
            // Suspected/Maintenance are consulted by the EC dispatch loop, not
            // here — so it was removed.)
            let overrides = self.node_overrides.borrow().clone();
            let now_s = Self::epoch_seconds();

            self.release_recovery_markers_for_dead_executors().await;

            // reseed the recovery rate limiter from the inflight
            // ledger so its counters reflect actually-in-flight recoveries.
            // The ledger is the source of truth (survives leader failover);
            // re-deriving every tick means no manual release bookkeeping
            // (a completed recovery drops out of the ledger → out of the
            // count next tick) and `recovery-stats` reports real numbers.
            // Backoff state is preserved (reset_counts leaves it). The
            // per-candidate `try_acquire` in `dispatch_recovery_task` then
            // gates NEW dispatches against the caps on top of this baseline.
            {
                let mut lim = self.recovery_limiter.borrow_mut();
                lim.reset_counts();
                for rec in self.inflight.borrow().values() {
                    if let Some((_, crate::extent_inflight::ExtentOpPayload::Recovery(t))) =
                        rec.unpack()
                    {
                        lim.seed_inflight(t.replace_id, t.node_id);
                    }
                }
            }

            // pre-filter under the store borrow so we DON'T clone
            // extents that the loop body will skip on the next line. The
            // loop body's first checks are `if ex.sealed_length == 0
            // { continue; }` and `if ec_conversion_inflight.contains(...)
            // { continue; }`. This previously cloned every single extent in
            // `s.extents` (~200 B each for the 4 Vec fields) only to drop
            // most on the floor — a 10K-extent cluster cloned 2 MB inline
            // per 2 s tick on the manager's compio runtime, blocking
            // heartbeat / register_ps / get_regions handlers for a few ms
            // each tick. The ec_conversion_inflight gating is unchanged
            // — `apply_recovery_done` / `mark_extent_available` /
            // `handle_multi_modify_split` still re-check the set at apply
            // time, so a stale snapshot here is safe (drops at most one
            // tick's worth of dispatch latency on the racing extent).
            let (extents, nodes, disks) = {
                let s = self.store.inner.borrow();
                // read the unified inflight ledger instead of the
                // old `ec_conversion_inflight` HashSet. We filter for
                // ConvertToEc specifically — recovery dispatch on an extent
                // that's mid-Recovery or mid-Delete is handled by
                // `dispatch_recovery_task`'s own refuse-at-start (which
                // collapses those into the same probe).
                let inflight = self.inflight.borrow();
                let extents: Vec<MgrExtentInfo> = s
                    .extents
                    .values()
                    .filter(|ex| {
                        // gate on the authoritative `sealed` STATE,
                        // NOT `sealed_length == 0`. A sealed-EMPTY extent
                        // (`sealed = true, sealed_length = 0` — a split/merge tail
                        // seal, or an open tail sealed by the fence drain) is a
                        // real recovery candidate: its fenced slots must be
                        // rebuilt so `remove` can proceed, and EN recovery handles
                        // the 0-byte copy (`stream_extent_from_sources` returns
                        // Ok(0) on `total == 0`, then sets the `sealed` flag).
                        // Open tails (`!sealed`) are still skipped here — they are
                        // drained by the fence sweep's PS-driven roll first.
                        if !ex.sealed {
                            return false;
                        }
                        !matches!(
                            inflight.get(&ex.extent_id).and_then(|r| r.kind()),
                            Some(crate::extent_inflight::ExtentOpKind::ConvertToEc)
                        )
                    })
                    .cloned()
                    .collect();
                (extents, s.nodes.clone(), s.disks.clone())
            };

            for ex in extents {
                let copies = Self::extent_nodes(&ex);
                for (slot, node_id) in copies.iter().copied().enumerate() {
                    let bit = 1u32 << slot;
                    let node = nodes.get(&node_id).cloned();

                    // backoff gate. If the (extent, slot) pair
                    // has consecutive failures, skip this tick.
                    if self
                        .recovery_limiter
                        .borrow()
                        .in_backoff(ex.extent_id, slot as u32, now_s)
                    {
                        continue;
                    }

                    // under `fenced_only`, only dispatch when the
                    // replica's owning node has an operator Fenced
                    // override. Suspected alone is NOT enough (matches
                    // the HDFS decommission analogue). Under `auto_disk`,
                    // fall through to the legacy disk.online check below.
                    let is_fenced = matches!(
                        overrides.get(&node_id).map(|o| o.kind),
                        Some(NODE_OVERRIDE_FENCED)
                    );

                    // under `fenced_only`, the operator must
                    // explicitly fence before we dispatch recovery. The
                    // backoff-from-failure path still applies once a
                    // dispatch attempt does fire.
                    if gate_mode == RecoveryGateMode::FencedOnly && !is_fenced {
                        continue;
                    }

                    // a Fenced node MUST have all its slots
                    // rebuilt regardless of probe outcome (the whole
                    // point of fence is to migrate data off). Skip the
                    // disk + probe shortcuts and dispatch immediately.
                    if is_fenced {
                        self.dispatch_and_record(ex.extent_id, slot as u32, node_id, now_s)
                            .await;
                        continue;
                    }

                    // Check per-disk health: if the disk holding this replica is
                    // offline, dispatch recovery even if the node is reachable.
                    let disk_id = if slot < ex.replicate_disks.len() {
                        Some(ex.replicate_disks[slot])
                    } else {
                        let parity_slot = slot.checked_sub(ex.replicates.len());
                        parity_slot.and_then(|ps| ex.parity_disks.get(ps).copied())
                    };
                    if let Some(did) = disk_id {
                        if let Some(disk) = disks.get(&did) {
                            if !disk.online {
                                self.dispatch_and_record(ex.extent_id, slot as u32, node_id, now_s)
                                    .await;
                                continue;
                            }
                        }
                    }

                    if (ex.avali & bit) == 0 {
                        if let Some(n) = node.clone() {
                            let base = Self::normalize_endpoint(&n.address);
                            // re_avali on specific extent → owner shard.
                            let addr =
                                Self::shard_addr_for_extent(&base, &n.shard_ports, ex.extent_id);
                            let payload = rkyv_encode(&ExtReAvaliReq {
                                extent_id: ex.extent_id,
                                eversion: ex.eversion,
                            });
                            // 30 s — RE_AVALI may copy the full extent
                            // from peers if local data lags
                            // sealed_length, so allow real work; cap to
                            // prevent paged-out-EN wedge.
                            if let Ok(resp) = self
                                .conn_pool
                                .call_timeout(
                                    &addr,
                                    EXT_MSG_RE_AVALI,
                                    payload,
                                    Duration::from_secs(30),
                                )
                                .await
                            {
                                if let Ok(r) = rkyv_decode::<ExtCodeResp>(&resp) {
                                    if r.code == CODE_OK {
                                        let _ =
                                            self.mark_extent_available(ex.extent_id, slot).await;
                                        continue;
                                    }
                                }
                            }
                        }
                        self.dispatch_and_record(ex.extent_id, slot as u32, node_id, now_s)
                            .await;
                        continue;
                    }

                    // Tier 2: switched from `commit_length_on_node`
                    // (fence-gated, requires PS-owner owner_epoch) to the
                    // dedicated fence-free `probe_extent_on_node`. The
                    // recovery loop has no owner context and only uses
                    // `.is_ok()` for liveness — gating it on the
                    // owner-lock fence was always wrong (pre-Tier 2 we
                    // worked around it by hardcoding `owner_epoch: 0` + a
                    // server-side escape hatch; that escape silently
                    // broke and forced this same fix).
                    let healthy = match node {
                        Some(n) => self
                            .probe_extent_on_node(&n.address, ex.extent_id)
                            .await
                            .is_ok(),
                        None => false,
                    };
                    if !healthy {
                        self.dispatch_and_record(ex.extent_id, slot as u32, node_id, now_s)
                            .await;
                    }
                }
            }

            // seal + roll open tails that sit on a fenced node so
            // recovery (above) can rebuild them and `remove` can proceed. Runs
            // each tick after the sealed-extent recovery dispatch.
            self.drain_fenced_open_tails().await;
        }
    }

    /// find OPEN tail extents (`!sealed`) whose replica set
    /// includes an operator-Fenced node and ask each owning partition's PS to
    /// seal + roll them (`MSG_ROLL_TAILS`). Recovery only rebuilds SEALED
    /// extents, so without this an idle partition's open tail on a fenced node
    /// never drains and blocks `remove` forever. Maintenance / Suspected nodes
    /// are NOT drained (they aren't being decommissioned). Per-partition 30 s
    /// cooldown keeps a repeatedly-failing roll (e.g. all replicas unreachable)
    /// from hammering the PS every tick.
    async fn drain_fenced_open_tails(&self) {
        let now_s = Self::epoch_seconds();
        let fenced: HashSet<u64> = self
            .node_overrides
            .borrow()
            .iter()
            .filter(|(_, o)| o.kind == NODE_OVERRIDE_FENCED)
            .map(|(id, _)| *id)
            .collect();
        if fenced.is_empty() {
            return;
        }
        // Snapshot under one store borrow: (part_id, PS addr, [(stream, tail)]).
        let mut work: Vec<(u64, String, Vec<(u64, u64)>)> = Vec::new();
        {
            let s = self.store.inner.borrow();
            for meta in s.partitions.values() {
                let mut entries: Vec<(u64, u64)> = Vec::new();
                for stream_id in [meta.log_stream, meta.row_stream, meta.meta_stream] {
                    let Some(stream) = s.streams.get(&stream_id) else {
                        continue;
                    };
                    let Some(&tail_id) = stream.extent_ids.last() else {
                        continue;
                    };
                    let Some(ex) = s.extents.get(&tail_id) else {
                        continue;
                    };
                    if ex.sealed {
                        continue; // only OPEN tails need draining
                    }
                    if ex
                        .replicates
                        .iter()
                        .chain(ex.parity.iter())
                        .any(|n| fenced.contains(n))
                    {
                        entries.push((stream_id, tail_id));
                    }
                }
                if entries.is_empty() {
                    continue;
                }
                // Resolve PS addr: per-partition hint first, region fallback.
                let addr = s.part_addrs.get(&meta.part_id).cloned().or_else(|| {
                    s.regions
                        .get(&meta.part_id)
                        .and_then(|r| s.ps_nodes.get(&r.ps_id).cloned())
                });
                match addr {
                    Some(addr) => work.push((meta.part_id, addr, entries)),
                    None => tracing::warn!(
                        part_id = meta.part_id,
                        "open tail on fenced node but no PS address to roll it (awaiting reassignment)"
                    ),
                }
            }
        }
        for (part_id, addr, entries) in work {
            // Per-partition 30 s cooldown.
            {
                let mut cd = self.roll_tails_cooldown.borrow_mut();
                if let Some(&last) = cd.get(&part_id) {
                    if now_s - last < 30 {
                        continue;
                    }
                }
                cd.insert(part_id, now_s);
            }
            let req = autumn_rpc::partition_rpc::RollTailsReq {
                part_id,
                entries,
            };
            let payload = autumn_rpc::partition_rpc::rkyv_encode(&req);
            match self
                .conn_pool
                .call_timeout(
                    &addr,
                    autumn_rpc::partition_rpc::MSG_ROLL_TAILS,
                    payload,
                    Duration::from_secs(30),
                )
                .await
            {
                Ok(bytes) => {
                    match autumn_rpc::partition_rpc::rkyv_decode::<
                        autumn_rpc::partition_rpc::RollTailsResp,
                    >(&bytes)
                    {
                        Ok(resp) if resp.rolled > 0 => tracing::info!(
                            part_id,
                            rolled = resp.rolled,
                            "rolled open tail(s) off fenced node(s)"
                        ),
                        Ok(_) => {}
                        Err(e) => {
                            tracing::warn!(part_id, error = %e, "bad RollTailsResp")
                        }
                    }
                }
                Err(e) => tracing::warn!(
                    part_id, addr = %addr, error = %e,
                    "roll_tails RPC failed (will retry)"
                ),
            }
        }
    }

    /// load the dispatch gate mode from env. Default
    /// `fenced_only` (operator-driven). `auto_disk` opts back into the
    /// legacy always-auto-rebuild behaviour for ops who haven't yet
    /// stood up the OP policy script.
    pub(crate) fn recovery_gate_mode() -> RecoveryGateMode {
        match std::env::var("AUTUMN_MGR_RECOVERY_GATE")
            .ok()
            .as_deref()
            .unwrap_or("fenced_only")
        {
            "auto_disk" => RecoveryGateMode::AutoDisk,
            _ => RecoveryGateMode::FencedOnly,
        }
    }

    /// record a (success / failure) outcome for the
    /// (extent, slot) pair so the rate-limiter's backoff window
    /// updates correctly.
    pub(crate) fn record_dispatch_outcome(
        &self,
        extent_id: u64,
        slot: u32,
        now_s: i64,
        res: &Result<(), AppError>,
    ) {
        let mut l = self.recovery_limiter.borrow_mut();
        match res {
            Ok(()) => l.record_success(extent_id, slot),
            // Capture WHY it failed so `recovery-stats`
            // can show the reason, not just a count. Pre-this the call
            // sites passed `res.is_ok()` and the error was discarded.
            Err(e) => {
                let consecutive = l.record_failure(extent_id, slot, now_s, &e.to_string());
                drop(l);
                // Op-ledger: keep the entry RUNNING (the loop retries with
                // exponential backoff — it never gives up) but carry the LAST
                // failure + its code + the consecutive-failure count, so
                // `ops status` shows a repair that is looping instead of
                // converging. This is the reason recovery belongs in the ledger.
                let (now_s2, now_ms) = Self::now_s_ms();
                self.ops.borrow_mut().record_recovery_failure(
                    extent_id,
                    e.to_string(),
                    Self::err_to_code(e),
                    consecutive,
                    now_s2,
                    now_ms,
                );
            }
        }
    }

    /// #6: Maintenance auto-clear. Walk overrides; for any
    /// Maintenance entry whose `expire_at` is in the past, delete the
    /// etcd key + in-memory entry. Logs an INFO; no audit entry (the
    /// system, not the operator, did this — the operator scheduled it
    /// via `expire_at`).
    pub(crate) async fn tick_maintenance_ttl(&self) {
        let now = Self::epoch_seconds() as u64;
        let expired: Vec<u64> = self
            .node_overrides
            .borrow()
            .iter()
            .filter_map(|(id, o)| {
                if o.kind == NODE_OVERRIDE_MAINTENANCE && o.expire_at > 0 && o.expire_at <= now {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();
        for id in expired {
            let key = format!("{}{}", crate::NODE_OVERRIDE_PREFIX, id);
            if let Some(etcd) = &self.etcd {
                let _ = etcd.put_and_delete_txn(Vec::new(), vec![key]).await;
            }
            self.node_overrides.borrow_mut().remove(&id);
            tracing::info!(
                node_id = id,
                "Maintenance override expired; auto-cleared"
            );
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecoveryGateMode {
    AutoDisk,
    FencedOnly,
}

impl crate::AutumnManager {
    /// unified node-health + recovery-collect loop. Merges the
    /// former `recovery_collect_loop` (2 s, recovery-target nodes only,
    /// non-empty `tasks`) and `disk_status_update_loop` (10 s, all nodes,
    /// empty `tasks`) into a SINGLE `EXT_MSG_DF` caller per node per tick.
    ///
    /// Why merge: both old loops called `df` independently. The EN's
    /// `handle_df` drains its ENTIRE `recovery_done` via `std::mem::take`
    /// whenever `req.tasks.is_empty()` — which is exactly what
    /// `disk_status_update_loop` sent. That loop then DISCARDED the
    /// returned `done_tasks` (it only updated disk status), so whenever
    /// its 10 s sweep won the race against the 2 s collect loop, the
    /// recovery completion was lost: `apply_recovery_done` never ran, the
    /// extent's slot was never repaired in manager metadata, the recovered
    /// copy became an orphan, and the inflight marker sat until the
    /// stale sweep released it ~10 min later. One loop = one df caller =
    /// completions are always applied.
    ///
    /// Cadence 2 s (the recovery-responsive pace; `df` is a cheap
    /// control-plane statvfs + Vec drain). The disk-status liveness signal
    /// is now sampled every 2 s instead of 10 s — strictly better for
    /// Suspected detection. `tasks` is always empty so the EN drains its
    /// full `recovery_done`, and every completion is applied right here,
    /// so nothing is ever stranded in the EN buffer.
    pub(crate) async fn node_health_loop(self) {
        // cluster-df logical-scan rotation state (persists across ticks). The
        // logical_stored scan (Σ distinct sealed_length) is O(extents); at
        // 10M+ extents a single-tick full scan would hold the store borrow for
        // ~100s of ms, stalling the single-threaded manager loop. Instead we
        // snapshot the id list once per cycle and process it in bounded chunks
        // across ticks, committing the new total when the cycle completes (the
        // committed value is republished every tick until then). A capacity
        // gauge tolerates the resulting staleness (≤ ~one cycle).
        const LOGICAL_SCAN_CHUNK: usize = 100_000; // ids per tick
        let mut logical_committed: u64 = 0;
        let mut logical_committed_ms: u64 = 0;
        let mut logical_cycle_ids: Vec<u64> = Vec::new();
        let mut logical_cursor: usize = 0;
        let mut logical_partial: u64 = 0;
        loop {
            compio::time::sleep(Duration::from_secs(2)).await;
            if !self.leader.get() {
                continue;
            }

            let nodes = {
                let s = self.store.inner.borrow();
                s.nodes.clone()
            };

            // cluster-df: accumulate this tick's RAW + physical-used snapshot
            // (the EN self-reports its real per-disk extent_bytes; manager
            // only sums — no amplification formula, no extent scan here).
            let mut cdf_raw_total = 0u64;
            let mut cdf_raw_free = 0u64;
            let mut cdf_physical_used = 0u64;
            let mut cdf_node_count = 0u64;
            let mut cdf_per_node: Vec<(u64, crate::NodeCap)> = Vec::with_capacity(nodes.len());

            for node in nodes.values() {
                // prefer the control_address; fall back to data
                // plane address for legacy / not-yet-re-registered nodes.
                let raw_addr = if node.control_address.is_empty() {
                    &node.address
                } else {
                    &node.control_address
                };
                let addr = Self::normalize_endpoint(raw_addr);
                // empty `tasks` → EN returns its full `recovery_done`
                // (std::mem::take). We apply ALL of it below, so no
                // completion is ever discarded (the merge's whole point).
                // P0: bound DF at 5 s via control_pool.call_timeout.
                let payload = rkyv_encode(&ExtDfReq {
                    tasks: Vec::new(),
                    disk_ids: Vec::new(),
                });
                let resp = match self
                    .control_pool
                    .call_timeout(&addr, EXT_MSG_DF, payload, Duration::from_secs(5))
                    .await
                {
                    Ok(v) => v,
                    Err(_) => {
                        // peer unreachable — mark its disks offline
                        // so allocation/recovery skip it. ConnPool already
                        // evicts the broken conn so the next poll reconnects.
                        // feed the auto-state tracker (Online →
                        // Suspected after the soft timeout). NOT a recovery
                        // trigger — that requires explicit fence.
                        Self::mark_node_disks_offline(&self.store, node);
                        self.node_states
                            .borrow_mut()
                            .on_heartbeat_fail(node.node_id);
                        // cluster-df: unreachable this tick (unknown != offline,
                        // but its capacity can't be summed) — record online=false.
                        cdf_per_node.push((
                            node.node_id,
                            crate::NodeCap {
                                online: false,
                                ..Default::default()
                            },
                        ));
                        continue;
                    }
                };
                let df: ExtDfResp = match rkyv_decode(&resp) {
                    Ok(v) => v,
                    Err(_) => {
                        cdf_per_node.push((
                            node.node_id,
                            crate::NodeCap {
                                online: false,
                                ..Default::default()
                            },
                        ));
                        continue;
                    }
                };
                // M1b: act on the df identity echo (pure decision
                // in `classify_df_echo`).
                match classify_df_echo(
                    &node.node_uuid,
                    &node.address,
                    &node.shard_ports,
                    &df.node_uuid,
                    &df.advertise_addr,
                    &df.shard_ports,
                ) {
                    // A DIFFERENT process answers at this address (pod-IP reuse).
                    // Do NOT heal the location to it — treat the df as FAILED for
                    // liveness: the real node_id's process is gone (→ Suspected),
                    // the new process is its own node_id via its own self-register.
                    // A k8s safety net the pre-M1 (address-only) system couldn't
                    // express.
                    DfEchoAction::Imposter => {
                        tracing::warn!(
                            node_id = node.node_id,
                            stored_uuid = %node.node_uuid,
                            echo_uuid = %df.node_uuid,
                            addr = %addr,
                            "df echo uuid != stored — a DIFFERENT \
                             process answers at this address (pod-IP reuse?); NOT \
                             healing, treating df as failed"
                        );
                        Self::mark_node_disks_offline(&self.store, node);
                        self.node_states.borrow_mut().on_heartbeat_fail(node.node_id);
                        cdf_per_node.push((
                            node.node_id,
                            crate::NodeCap {
                                online: false,
                                ..Default::default()
                            },
                        ));
                        continue;
                    }
                    // uuid matches but the echoed location drifted from etcd
                    // (hand-edited etcd — M1a's startup register makes the
                    // lost-txn shape unreachable). WARN only: the EN's next boot
                    // self-register, or an operator, is the authoritative fix. We
                    // deliberately do NOT auto-write a healed record from the
                    // loop-start snapshot — that could clobber a concurrent
                    // register/remove (coco P1). The node stays Online (df
                    // succeeded — it IS reachable); only the stored routing
                    // location is stale, which the WARN surfaces.
                    DfEchoAction::DriftWarn => {
                        tracing::warn!(
                            node_id = node.node_id,
                            stored_addr = %node.address,
                            echo_addr = %df.advertise_addr,
                            stored_ports = ?node.shard_ports,
                            echo_ports = ?df.shard_ports,
                            "stored EN location differs from the df echo \
                             (stale etcd?); re-register the EN or correct etcd — NOT \
                             auto-healing to avoid clobbering concurrent updates"
                        );
                    }
                    DfEchoAction::Ok => {}
                }

                // a successful df proves the node reachable — promote
                // on the call-level signal, not per-payload disk_id (the
                // wire status keys on the extent-node's local disk_id,
                // unrelated to the manager's allocated disk_id).
                Self::mark_node_disks_online(&self.store, node);
                // ENOSPC-1: stash the node's max per-disk free for the
                // allocation free-space soft filter. Uses the df payload's
                // aggregate only — the per-disk ids in it are EN-local and
                // unrelated to manager disk_ids (note 7), but the
                // MAX across disks needs no id mapping.
                let max_free = df
                    .disk_status
                    .iter()
                    .map(|(_, st)| st.free)
                    .max()
                    .unwrap_or(0);
                self.node_max_free
                    .borrow_mut()
                    .insert(node.node_id, max_free);
                // cluster-df: sum this node's ONLINE-disk capacity + real
                // extent footprint (excludes offline disks — unusable space).
                let mut n_total = 0u64;
                let mut n_free = 0u64;
                let mut n_ext = 0u64;
                for (_, st) in &df.disk_status {
                    if st.online {
                        n_total = n_total.saturating_add(st.total);
                        n_free = n_free.saturating_add(st.free);
                        n_ext = n_ext.saturating_add(st.extent_bytes);
                    }
                }
                cdf_raw_total = cdf_raw_total.saturating_add(n_total);
                cdf_raw_free = cdf_raw_free.saturating_add(n_free);
                cdf_physical_used = cdf_physical_used.saturating_add(n_ext);
                cdf_node_count += 1;
                cdf_per_node.push((
                    node.node_id,
                    crate::NodeCap {
                        total: n_total,
                        free: n_free,
                        extent_bytes: n_ext,
                        online: true,
                    },
                ));
                // drop stale push-based failure reports so a residual
                // burst can't re-flip the node offline on the next tick.
                self.recent_failure_reports
                    .borrow_mut()
                    .remove(&node.node_id);
                // heartbeat OK → flip Suspected back to Online.
                self.node_states.borrow_mut().on_heartbeat_ok(node.node_id);
                // apply EVERY completed recovery task — the step the
                // old disk_status_update_loop omitted (it discarded them).
                for done in df.done_tasks {
                    // No-swallow: never `let _ =` the apply result. A completion
                    // is delivered exactly once (the EN mem::take's it from
                    // recovery_done), so on ANY error this completion is gone —
                    // there is no immediate re-delivery. Convergence for the
                    // kept-marker cases is via the stale-marker sweep +
                    // re-dispatch (see the match arms). We surface the error so
                    // it is never silent; we deliberately do NOT add a
                    // manager-side completion-retry queue (the stale-marker sweep
                    // backstops correctness; the slow-convergence harm is bounded
                    // and unreproduced — a reproduce-first follow-up).
                    if let Err(e) = self.apply_recovery_done(done).await {
                        match e {
                            AppError::Precondition(_) => {
                                // Benign: stale-discard (layout changed / replace_id
                                // gone) — marker released, completion correctly
                                // dropped; OR EC-in-flight defer — marker kept,
                                // completion dropped (the EN mem::take'd it once),
                                // convergence via the stale-marker sweep + re-dispatch on the
                                // EC'd extent. NOT an immediate completion-retry.
                                tracing::trace!(error = %e, "apply_recovery_done deferred/stale (benign); converges via sweep / re-dispatch");
                            }
                            _ => {
                                tracing::warn!(error = %e, "apply_recovery_done failed (etcd); inflight marker retained — converges via stale-marker sweep");
                            }
                        }
                    }
                }

                // Apply every EC conversion the coordinator reported finished. The
                // dispatch RPC only ACCEPTS; THIS is where a conversion becomes real.
                // The layout comes from the etcd marker's PINNED assignment, never
                // from the report — the report only says "it finished" and carries
                // `new_eversion` for a cross-check, so a stale/forged report can't
                // steer the layout.
                for done in df.ec_done {
                    let params = match self.extent_inflight_payload_ec(done.extent_id) {
                        Some(p) if params_coordinator(&p) != Some(node.node_id) => {
                            // REPORTER IDENTITY: only the marker's own coordinator
                            // may complete it. `new_eversion` is `live + 1` and a
                            // released-then-reissued attempt reuses the SAME value,
                            // so the eversion cross-check alone cannot separate
                            // attempts: a stale report from a PREVIOUS attempt's
                            // coordinator would otherwise apply the CURRENT
                            // attempt's layout while its targets hold no shards.
                            tracing::warn!(
                                extent_id = done.extent_id,
                                reporter = node.node_id,
                                coordinator = ?params_coordinator(&p),
                                "ec_done from a node that is not this marker's \
                                 coordinator — IGNORING (marker retained)"
                            );
                            continue;
                        }
                        Some(p) => p,
                        None => {
                            // No marker: an already-applied conversion re-reported
                            // (df is at-most-once but re-dispatch can re-adopt), or
                            // an abandoned one. Benign — nothing to apply.
                            tracing::debug!(
                                extent_id = done.extent_id,
                                "ec_done for an extent with no inflight marker; ignoring"
                            );
                            continue;
                        }
                    };
                    if params.new_eversion != done.new_eversion {
                        tracing::warn!(
                            extent_id = done.extent_id,
                            marker_eversion = params.new_eversion,
                            reported_eversion = done.new_eversion,
                            "ec_done eversion disagrees with the pinned marker — REFUSING to apply \
                             (marker retained; re-dispatch will reconcile)"
                        );
                        continue;
                    }
                    let data_shards = params.data_shards as usize;
                    self.finalize_ec_dispatch_after_convert(
                        done.extent_id,
                        params.target_nodes,
                        params.extra_disk_ids,
                        data_shards,
                        params.new_eversion,
                    )
                    .await;
                }
            }

            // cluster-df: publish this tick's RAW + physical snapshot (cheap —
            // one entry per disk), then drive the chunked logical-scan rotation.
            let now_ms = autumn_common::metrics::unix_time_ms();

            // (1) Start a fresh cycle when idle AND ≥30 s since the last commit:
            // snapshot the extent id list (one O(N) borrow, just u64 copies).
            if logical_cycle_ids.is_empty()
                && now_ms.saturating_sub(logical_committed_ms) >= 30_000
            {
                logical_cycle_ids = self.store.inner.borrow().extents.keys().copied().collect();
                logical_cursor = 0;
                logical_partial = 0;
                if logical_cycle_ids.is_empty() {
                    // Empty cluster — commit 0 now (nothing to chunk through).
                    logical_committed = 0;
                    logical_committed_ms = now_ms;
                }
            }
            // (2) Process one bounded chunk of the in-progress cycle. Each id is
            // re-looked-up (it may have been deleted since the snapshot) and
            // filtered: skip extents pending physical delete (refs==0 &&
            // vp_table_refs==0) — they no longer count as stored.
            if !logical_cycle_ids.is_empty() {
                let end = (logical_cursor + LOGICAL_SCAN_CHUNK).min(logical_cycle_ids.len());
                {
                    let s = self.store.inner.borrow();
                    for id in &logical_cycle_ids[logical_cursor..end] {
                        if let Some(ex) = s.extents.get(id) {
                            if ex.refs != 0 || ex.vp_table_refs != 0 {
                                logical_partial = logical_partial.saturating_add(ex.sealed_length);
                            }
                        }
                    }
                }
                logical_cursor = end;
                if logical_cursor >= logical_cycle_ids.len() {
                    // Cycle complete — publish the new total + free the id vec.
                    logical_committed = logical_partial;
                    logical_committed_ms = now_ms;
                    logical_cycle_ids = Vec::new();
                }
            }
            // Σ latest PS-reported open-tail committed bytes
            // across partitions (cheap — one back()-of-window read per
            // partition). physical_used INCLUDES these bytes, so the amp
            // denominator adds them to the sealed logical scan. Open tails are
            // refs=1 partition-private → simple sum, no CoW dedup.
            let logical_open_tail: u64 = {
                let pol = self.policy.borrow();
                pol.metrics
                    .values()
                    .filter_map(|w| w.buckets.back())
                    .map(|(_, l)| l.open_tail_bytes)
                    .sum()
            };
            // Σ reclaimable dead bytes = sealed (gc_debt) +
            // open-tail dead, across the same latest-bucket window. gc_debt is
            // sealed-only, so adding open_tail_dead surfaces the debt a
            // log-heavy / all-open-tail partition otherwise hides at 0.
            let logical_wal_debt: u64 = {
                let pol = self.policy.borrow();
                pol.metrics
                    .values()
                    .filter_map(|w| w.buckets.back())
                    .map(|(_, l)| l.gc_debt_bytes.saturating_add(l.open_tail_dead_bytes))
                    .sum()
            };
            {
                let mut snap = self.cluster_cap.borrow_mut();
                snap.raw_total = cdf_raw_total;
                snap.raw_free = cdf_raw_free;
                snap.physical_used = cdf_physical_used;
                snap.node_count = cdf_node_count;
                snap.last_update_ms = now_ms;
                // Republish the last fully-committed logical total every tick
                // (a mid-flight cycle hasn't changed it yet).
                snap.logical_stored = logical_committed;
                snap.logical_open_tail = logical_open_tail;
                snap.logical_wal_debt = logical_wal_debt;
                snap.logical_last_update_ms = logical_committed_ms;
                snap.per_node = cdf_per_node;
            }
        }
    }

    /// helper: flip `online=false` for every disk owned by `node`
    /// when its `df` RPC fails. In-memory only — the manager reseeds
    /// disk state from etcd on leader promotion via `replay_from_etcd`,
    /// and a recovered node will overwrite `online=true` on the next
    /// successful `df` poll.
    pub(crate) fn mark_node_disks_offline(
        store: &autumn_common::MetadataStore,
        node: &autumn_rpc::manager_rpc::MgrNodeInfo,
    ) {
        if node.disks.is_empty() {
            return;
        }
        let mut s = store.inner.borrow_mut();
        let mut changed = false;
        for disk_id in &node.disks {
            if let Some(disk) = s.disks.get_mut(disk_id) {
                if disk.online {
                    disk.online = false;
                    changed = true;
                }
            }
        }
        if changed {
            tracing::warn!(
                node_id = node.node_id,
                addr = %node.address,
                "df RPC failed; marked node disks offline"
            );
        }
    }

    /// helper: counterpart to `mark_node_disks_offline`. Flip
    /// `online=true` on a successful df. Keys on `MgrNodeInfo.disks`
    /// (manager-allocated disk_ids) instead of the response payload's
    /// extent-node-local disk_ids, which historically failed to map.
    fn mark_node_disks_online(
        store: &autumn_common::MetadataStore,
        node: &autumn_rpc::manager_rpc::MgrNodeInfo,
    ) {
        if node.disks.is_empty() {
            return;
        }
        let mut s = store.inner.borrow_mut();
        for disk_id in &node.disks {
            if let Some(disk) = s.disks.get_mut(disk_id) {
                if !disk.online {
                    disk.online = true;
                    tracing::info!(
                        node_id = node.node_id,
                        disk_id,
                        "df RPC succeeded; disk back online"
                    );
                }
            }
        }
    }

    /// Drain-only EC-conversion dispatcher. Every ~5 s the leader drains
    /// the ledger's ConvertToEc markers (written by `handle_force_ec_convert`
    /// / restored by `replay_from_etcd`) and (re-)dispatches each to its
    /// coordinator. Two phases per tick, split into helpers for clarity:
    /// `collect_ec_dispatch_candidates` (snapshot + filter, holds the store
    /// borrow) then `dispatch_one_ec_conversion` per candidate (RPC + apply,
    /// no borrow held across the await).
    pub(crate) async fn ec_conversion_dispatch_loop(self) {
        // short initial delay so post-restart re-dispatch of replay-loaded
        // markers fires quickly (otherwise PS startup sees up to 5 s of
        // eversion-mismatch against extents whose `apply_ec_conversion_done`
        // didn't commit last lifetime); steady-state cadence is 5 s thereafter.
        let mut delay = Duration::from_millis(500);
        loop {
            compio::time::sleep(delay).await;
            delay = Duration::from_secs(5);
            if !self.leader.get() {
                continue;
            }
            let (candidates, node_addrs) = self.collect_ec_dispatch_candidates();
            // Dispatch CONCURRENTLY (bounded): these are now accept-ACKs, but a
            // paged-out coordinator still burns its full RPC timeout, and
            // serialising meant one such extent stalled every other conversion
            // in the cluster for that tick. The cap keeps the fan-out (and the
            // manager's socket use) bounded.
            const EC_DISPATCH_CONCURRENCY: usize = 8;
            let this = &self;
            let addrs = &node_addrs;
            futures::stream::StreamExt::for_each_concurrent(
                futures::stream::iter(candidates),
                EC_DISPATCH_CONCURRENCY,
                move |cand| async move { this.dispatch_one_ec_conversion(cand, addrs).await },
            )
            .await;
        }
    }

    /// Best-effort drop of a stale/invalid ConvertToEc ledger marker. Spawned
    /// detached because callers may hold the `store.inner` borrow and cannot
    /// await inline; the drain is idempotent and re-attempted next tick, so a
    /// failure is logged (not swallowed) rather than propagated.
    fn spawn_drain_stale_ec_marker(&self, extent_id: u64) {
        let mgr = self.clone();
        compio::runtime::spawn(async move {
            if let Err(e) = mgr.drain_extent_inflight_marker(extent_id).await {
                // WARN (not debug): a persistent drain failure leaks the marker
                // and re-fires every ~5 s; it must be visible at prod log level,
                // matching this loop's other warns (the no-swallow principle).
                tracing::warn!(extent_id, error = %e, "drain stale EC marker failed; retried next tick");
            }
        })
        .detach();
    }

    /// PHASE 1 of `ec_conversion_dispatch_loop`: snapshot the cluster once and
    /// build the set of EC conversions to (re-)dispatch this tick.
    ///
    /// Drain-only: candidates are the ledger's ConvertToEc markers,
    /// NOT a fresh `s.streams` scan — the manager is pure mechanism, an external
    /// controller decides via `MSG_FORCE_EC_CONVERT`. The marker IS the rich
    /// authoritative dispatch record (`MgrEcDispatchInflight`); we reuse its
    /// target nodes / disks / eversion verbatim (replay-safe). Filters:
    /// - skip extents with a Recovery in flight (avoid colliding with
    ///   `run_recovery_task`);
    /// - skip when the coordinator (`target_nodes[0]`) is Suspected / Suspend /
    ///   operator-overridden — silently, marker kept;
    /// - drop the marker for an extent that vanished, was already converted, or
    ///   truncated to 0 (stale).
    /// dedup is structural: ledger keys are unique by construction.
    /// Returns the candidates + the per-tick node-address snapshot that
    /// PHASE 2 resolves target addresses against.
    fn collect_ec_dispatch_candidates(
        &self,
    ) -> (Vec<EcDispatchCandidate>, HashMap<u64, String>) {
        let recovery_inflight_extents: HashSet<u64> = self
            .inflight
            .borrow()
            .iter()
            .filter_map(|(id, rec)| match rec.kind() {
                Some(crate::extent_inflight::ExtentOpKind::Recovery) => Some(*id),
                _ => None,
            })
            .collect();

        let s = self.store.inner.borrow();
        let node_addrs: HashMap<u64, String> = s
            .nodes
            .iter()
            .map(|(id, n)| (*id, n.address.clone()))
            .collect();
        let pending: Vec<(u64, MgrEcDispatchInflight)> = self
            .inflight
            .borrow()
            .iter()
            .filter_map(|(eid, rec)| match rec.unpack() {
                Some((_, crate::extent_inflight::ExtentOpPayload::ConvertToEc(p))) => {
                    Some((*eid, p))
                }
                _ => None,
            })
            .collect();
        let node_state_snap: HashMap<u64, crate::node_state::NodeAutoState> = self
            .node_states
            .borrow()
            .snapshot()
            .into_iter()
            .map(|(id, st, _)| (id, st))
            .collect();
        let overrides_snap = self.node_overrides.borrow().clone();

        let mut candidates = Vec::new();
        for (eid, params) in pending {
            if recovery_inflight_extents.contains(&eid) {
                continue;
            }
            if let Some(coord) = params.target_nodes.first() {
                let auto_st = node_state_snap
                    .get(coord)
                    .copied()
                    .unwrap_or(crate::node_state::NodeAutoState::Online);
                let is_overridden = overrides_snap.contains_key(coord);
                if auto_st.is_suspected() || auto_st.is_suspend() || is_overridden {
                    continue;
                }
            }
            let ex = match s.extents.get(&eid) {
                Some(e) => e.clone(),
                None => {
                    // Extent deleted between marker write and this tick.
                    self.spawn_drain_stale_ec_marker(eid);
                    continue;
                }
            };
            if ex.ec_converted || ex.sealed_length == 0 {
                // Already converted, or truncated to 0 — marker is stale.
                self.spawn_drain_stale_ec_marker(eid);
                continue;
            }
            // Any owning stream gives the EC shape: CoW-shared extents (refs >= 2)
            // appear in multiple streams, but `compute_duplicate_stream` clones
            // `(ec_data_shard, ec_parity_shard)`, so the first hit suffices.
            let stream = match s
                .streams
                .values()
                .find(|st| st.ec_parity_shard > 0 && st.extent_ids.contains(&eid))
                .cloned()
            {
                Some(st) => st,
                None => continue,
            };
            candidates.push(EcDispatchCandidate { ex, stream, params });
        }
        (candidates, node_addrs)
    }

    /// PHASE 2 of `ec_conversion_dispatch_loop`: (re-)dispatch one candidate.
    /// Resolves the coordinator + per-target shard addresses, sends
    /// `EXT_MSG_CONVERT_TO_EC` (60 s ceiling), and on CODE_OK runs
    /// `finalize_ec_dispatch_after_convert` (apply + marker release). The
    /// ledger marker is the eversion-bump lock across the whole RPC→apply window.
    /// A target-count/stream mismatch drops the marker (operator-induced
    /// inconsistency); a transiently-missing node address defers (marker kept,
    /// retried next tick).
    async fn dispatch_one_ec_conversion(
        &self,
        cand: EcDispatchCandidate,
        node_addrs: &HashMap<u64, String>,
    ) {
        let EcDispatchCandidate { ex, stream, params } = cand;
        let extent_id = ex.extent_id;
        let data_shards = stream.ec_data_shard as usize;
        let parity_shards = stream.ec_parity_shard as usize;
        let total_shards = data_shards + parity_shards;

        // Marker target count must match the stream's current K+M. A mismatch
        // only arises from operator-induced inconsistency (`set-stream-ec`
        // between `force-ec-convert` and this tick); drop the stale marker so
        // the operator can re-issue under the new shape.
        if params.target_nodes.len() != total_shards {
            tracing::warn!(
                extent_id,
                marker_targets = params.target_nodes.len(),
                stream_total_shards = total_shards,
                "dispatch marker target count != stream K+M; dropping stale marker"
            );
            self.spawn_drain_stale_ec_marker(extent_id);
            return;
        }

        let mut target_addrs: Vec<String> = Vec::with_capacity(params.target_nodes.len());
        for &nid in &params.target_nodes {
            match node_addrs.get(&nid) {
                Some(addr) => target_addrs.push(addr.clone()),
                None => {
                    tracing::warn!(
                        extent_id,
                        target_nodes = ?params.target_nodes,
                        "marker target_node missing from cluster; deferring re-dispatch"
                    );
                    return;
                }
            }
        }

        // coordinator = the shard owning `extent_id` on the first
        // replica; it reads the full extent locally and fans WriteShard out to
        // each target node's owner shard for `extent_id`.
        let coordinator_base = Self::normalize_endpoint(&target_addrs[0]);
        let coordinator_shard_ports = self.shard_ports_for_addr(&coordinator_base);
        let coordinator_addr =
            Self::shard_addr_for_extent(&coordinator_base, &coordinator_shard_ports, extent_id);
        let ec_target_addrs: Vec<String> = target_addrs
            .iter()
            .map(|a| {
                let b = Self::normalize_endpoint(a);
                let sp = self.shard_ports_for_addr(&b);
                Self::shard_addr_for_extent(&b, &sp, extent_id)
            })
            .collect();

        // send the post-conversion eversion in-band so every target node
        // bumps `entry.eversion` to match what `apply_ec_conversion_done`
        // persists, closing the read-side stale-cache window. Tier 2:
        // owner_epoch lets the coord stamp WriteShard / CommitEcShard for
        // EN-side fence rejection of a ghost ex-coord.
        let payload = rkyv_encode(&ExtConvertToEcReq {
            extent_id,
            data_shards: data_shards as u32,
            parity_shards: parity_shards as u32,
            target_addrs: ec_target_addrs,
            eversion: params.new_eversion,
            owner_epoch: params.owner_epoch,
        });

        // 60 s ceiling so a paged-out / silently dead EN can't wedge the loop;
        // the convert itself can legitimately take seconds (RS-encode of
        // multi-GiB extents + 3-replica WriteShard fanout + commit-rename).
        let result = self
            .conn_pool
            .call_timeout(
                &coordinator_addr,
                EXT_MSG_CONVERT_TO_EC,
                payload,
                Duration::from_secs(60),
            )
            .await;

        let rpc_ok = match result {
            Ok(resp_data) => match rkyv_decode::<ExtCodeResp>(&resp_data) {
                Ok(r) if r.code == CODE_OK => true,
                Ok(r) => {
                    tracing::warn!("EC conversion failed for extent {extent_id}: {}", r.message);
                    false
                }
                Err(e) => {
                    tracing::warn!(
                        "EC conversion: failed to decode response for extent {extent_id}: {e}"
                    );
                    false
                }
            },
            Err(e) => {
                tracing::warn!("EC conversion failed for extent {extent_id}: {e}");
                false
            }
        };

        // NOTE: CODE_OK here means ACCEPTED, not DONE. The coordinator encodes
        // in the background and reports completion on its next `df`
        // (`DfResp.ec_done`), which `node_health_loop` turns into
        // `finalize_ec_dispatch_after_convert`. The marker deliberately stays
        // until then, so this dispatch is a safe idempotent re-send every tick.
        if rpc_ok {
            tracing::debug!(extent_id, "EC convert accepted by coordinator; awaiting df report");
        }
    }

    /// Post-RPC finalize after a successful `EXT_MSG_CONVERT_TO_EC`: apply the
    /// conversion to etcd + memory, then release the in-memory inflight marker.
    ///
    /// BUG2-EC-APPLY-FAIL (coco arch, verified 2026-06-19): the in-memory marker
    /// MUST be released ONLY when `apply_ec_conversion_done` SUCCEEDS. Pre-fix
    /// this was two unconditional `if rpc_ok` blocks — the apply error was
    /// swallowed (`let _ =`) and the marker released regardless. If apply's
    /// `txn_fenced` failed transiently WITHOUT losing leadership (etcd blip,
    /// non-fence error), etcd kept the marker + pre-EC layout while THIS leader
    /// dropped the in-memory marker; because `ec_conversion_dispatch_loop` is
    /// drain-only (it enumerates the in-memory shadow, not a fresh etcd
    /// scan), it never re-dispatched, so the extent stayed manager-pre-EC /
    /// EN-post-EC and every read wedged on `EVERSION_MISMATCH` until a leader
    /// failover replayed the etcd marker. Keeping the marker on apply-failure
    /// lets the next tick re-dispatch; repeated convert calls are
    /// idempotent, so a re-dispatch after the EN already converted is a no-op.
    async fn finalize_ec_dispatch_after_convert(
        &self,
        extent_id: u64,
        target_nodes: Vec<u64>,
        extra_disk_ids: Vec<u64>,
        data_shards: usize,
        new_eversion: u64,
    ) {
        match self
            .apply_ec_conversion_done(
                extent_id,
                target_nodes,
                extra_disk_ids,
                data_shards,
                new_eversion,
            )
            .await
        {
            Ok(()) => {
                // apply's atomic put_and_delete_txn already removed the etcd
                // marker; drop the in-memory shadow to match.
                self.commit_extent_inflight_release(extent_id);
            }
            Err(e) => {
                // Leadership-retained apply failure: keep the marker so the
                // next dispatch tick re-dispatches (idempotent).
                // A leadership-LOST failure (NotLeader) also keeps it; the new
                // leader's replay reloads it from etcd either way.
                tracing::warn!(
                    extent_id,
                    error = %e,
                    "EC apply failed after a successful convert RPC; keeping inflight marker for re-dispatch"
                );
            }
        }
    }

    /// drop a stale or no-longer-valid `extent_inflight/<id>` marker.
    /// Used by the dispatch loop when it observes a ledger entry for an
    /// extent that has been deleted or that has incompatible state (e.g.,
    /// already EC-converted) — best-effort cleanup so the next tick's
    /// candidate set shrinks. Idempotent.
    async fn drain_extent_inflight_marker(&self, extent_id: u64) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            // Use `put_and_delete_txn` (one-element delete list) so the leader
            // fence applies. A `false` return from the underlying CAS is
            // impossible here (no extra_cmp); only NotLeader can happen and
            // bubbles up.
            etcd.put_and_delete_txn(Vec::new(), vec![Self::extent_inflight_key(extent_id)])
                .await?;
        }
        self.commit_extent_inflight_release(extent_id);
        Ok(())
    }

    pub async fn apply_ec_conversion_done(
        &self,
        extent_id: u64,
        target_nodes: Vec<u64>,
        extra_disk_ids: Vec<u64>,
        data_shards: usize,
        new_eversion: u64,
    ) -> Result<(), AppError> {
        // TEST-ONLY (G4 harness): one-shot transient failure BEFORE any etcd /
        // leadership interaction — a faithful model of "apply's etcd txn blipped
        // while this manager stayed leader". No-op in production builds.
        #[cfg(test)]
        if EC_APPLY_FAIL_ONCE.with(|c| c.replace(false)) {
            return Err(AppError::Internal(
                "test-injected transient EC apply failure (G4)".into(),
            ));
        }
        // etcd-first: compute `updated` from a clone under
        // read-only borrow. The borrow_mut block previously mutated
        // s.extents[extent_id] in place (ec_converted=true, replicates,
        // parity, avali, eversion), then the etcd put_and_delete_txn
        // ran. If etcd failed (NotLeader / fence break), in-memory had
        // the new EC-converted shape but etcd still showed the pre-EC
        // shape — replay rolled in-memory back later, but in the
        // window concurrent reads observed EC state that wasn't durable.
        let updated = {
            let s = self.store.inner.borrow();
            let ex = s
                .extents
                .get(&extent_id)
                .ok_or_else(|| AppError::NotFound(format!("extent {extent_id}")))?;

            let mut all_disks = ex.replicate_disks.clone();
            all_disks.extend_from_slice(&extra_disk_ids);
            all_disks.truncate(target_nodes.len());

            let mut new_ex = ex.clone();
            new_ex.ec_converted = true;
            new_ex.replicates = target_nodes[..data_shards].to_vec();
            new_ex.parity = target_nodes[data_shards..].to_vec();
            new_ex.replicate_disks = all_disks[..data_shards].to_vec();
            new_ex.parity_disks = all_disks[data_shards..].to_vec();
            // Use the eversion sent in-band to the extent nodes via
            // ExtConvertToEcReq. Manager + every shard host now agree on
            // the same post-EC eversion.
            new_ex.eversion = new_eversion;
            // post-EC the extent has K+M shards across K+M nodes;
            // every slot is available by construction.
            new_ex.avali = Self::all_bits(target_nodes.len());
            new_ex
        };

        if let Some(etcd) = &self.etcd {
            // Bundle the marker delete into the same etcd txn
            // as the `extents/<id>` put. This was previously two separate
            // etcd round-trips, now atomic. The in-memory apply happens
            // ONLY after this txn lands.
            let key = format!("extents/{}", extent_id);
            let val = rkyv_encode(&updated).to_vec();
            etcd.put_and_delete_txn(vec![(key, val)], vec![Self::extent_inflight_key(extent_id)])
                .await?;
        }

        // only after etcd success do we apply to in-memory.
        {
            let mut s = self.store.inner.borrow_mut();
            s.extents.insert(extent_id, updated);
        }

        // Close any op-ledger EC-convert entry for this extent (authoritative —
        // the manager IS the EC orchestrator; identity is exact by extent id).
        {
            let (now_s, _) = Self::now_s_ms();
            self.ops.borrow_mut().complete_ec(
                extent_id,
                autumn_rpc::manager_rpc::OP_STATE_SUCCEEDED,
                "ec conversion done".to_string(),
                String::new(),
                now_s,
            );
        }

        Ok(())
    }
}

/// M1b: what `node_health_loop` should do with a df identity echo.
pub(crate) enum DfEchoAction {
    /// No echo (EN not self-registered), or echo agrees with stored state.
    Ok,
    /// The echoed `node_uuid` differs from the stored one for this node_id — a
    /// DIFFERENT process answers at this address (pod-IP reuse). Do NOT heal;
    /// treat the df as failed for liveness. This is the load-bearing k8s safety
    /// net (self-PROTECTING, no write).
    Imposter,
    /// uuid matches but the echoed location drifted from etcd — WARN only (no
    /// auto-write; see the note on `classify_df_echo`).
    DriftWarn,
}

/// Pure decision for the df identity echo (M1b). `echo_uuid` empty = the EN did
/// not self-register (`--advertise` unset) → `Ok` (skip all checks). A non-empty
/// echo uuid that differs from a non-empty stored uuid = `Imposter`. Otherwise
/// the uuid matches (or the stored uuid is empty — a legacy record) and a
/// location difference is `DriftWarn`. Empty echo fields are "unspecified",
/// never a drift.
///
/// **Why `DriftWarn`, not an auto-heal write (coco M1b P1/P2 + repo norms):**
/// the EN's STARTUP self-register (M1a) already writes the authoritative
/// location to etcd, and `register_with_manager` only returns Ok on a committed
/// `CODE_OK`, so the "registration txn lost" drift shape the design worried
/// about cannot occur; the only residual drift source is a hand-edited etcd
/// value (operator error). Auto-WRITING a healed record from the loop-start
/// `nodes` snapshot after a `mirror_register_node` await could clobber a
/// concurrent register / remove (resurrect a deleted node — coco P1), and it
/// would defend a near-unreachable scenario with a real data-safety risk. So
/// M1b surfaces drift as a WARN (the operator, or the EN's next boot
/// self-register, resolves it) and keeps only the self-protecting imposter
/// check. A CAS-safe auto-heal (re-read + verify + `nodes/<id>` value-CAS) is a
/// deferred, reproduce-first follow-up (feature_list M1b note).
pub(crate) fn classify_df_echo(
    stored_uuid: &str,
    stored_addr: &str,
    stored_ports: &[u16],
    echo_uuid: &str,
    echo_addr: &str,
    echo_ports: &[u16],
) -> DfEchoAction {
    if echo_uuid.is_empty() {
        return DfEchoAction::Ok;
    }
    if !stored_uuid.is_empty() && echo_uuid != stored_uuid {
        return DfEchoAction::Imposter;
    }
    let addr_drift = !echo_addr.is_empty() && echo_addr != stored_addr;
    let ports_drift = !echo_ports.is_empty() && echo_ports != stored_ports;
    if addr_drift || ports_drift {
        DfEchoAction::DriftWarn
    } else {
        DfEchoAction::Ok
    }
}

#[cfg(test)]
mod df_echo_tests {
    use super::{classify_df_echo, DfEchoAction};

    #[test]
    fn no_echo_is_ok() {
        // EN did not self-register (--advertise unset) → skip all checks.
        assert!(matches!(
            classify_df_echo("uuid-A", "10.0.0.1:9101", &[9101], "", "", &[]),
            DfEchoAction::Ok
        ));
    }

    #[test]
    fn matching_echo_is_ok() {
        assert!(matches!(
            classify_df_echo(
                "uuid-A",
                "10.0.0.1:9101",
                &[9101, 9111],
                "uuid-A",
                "10.0.0.1:9101",
                &[9101, 9111],
            ),
            DfEchoAction::Ok
        ));
    }

    #[test]
    fn different_uuid_is_imposter() {
        // Pod-IP reuse: a different process answers at this address.
        assert!(matches!(
            classify_df_echo("uuid-A", "10.0.0.1:9101", &[9101], "uuid-B", "10.0.0.1:9101", &[9101]),
            DfEchoAction::Imposter
        ));
    }

    #[test]
    fn drifted_location_under_same_uuid_warns() {
        // Address drifted, ports unchanged → DriftWarn.
        assert!(matches!(
            classify_df_echo(
                "uuid-A",
                "10.0.0.1:9101",
                &[9101, 9111],
                "uuid-A",
                "10.0.0.9:9101",
                &[9101, 9111],
            ),
            DfEchoAction::DriftWarn
        ));

        // Ports drifted (a reshard), address unchanged → DriftWarn.
        assert!(matches!(
            classify_df_echo(
                "uuid-A",
                "10.0.0.1:9101",
                &[9101],
                "uuid-A",
                "10.0.0.1:9101",
                &[9101, 9111, 9121],
            ),
            DfEchoAction::DriftWarn
        ));
    }

    #[test]
    fn empty_echo_fields_never_flag_drift() {
        // uuid matches but advertise_addr / shard_ports echo empty → "unspecified",
        // must NOT be treated as a drift.
        assert!(matches!(
            classify_df_echo("uuid-A", "10.0.0.1:9101", &[9101], "uuid-A", "", &[]),
            DfEchoAction::Ok
        ));
    }
}

// G4 / BUG-EC-APPLY-FAIL loop-level reproduce harness. Kept in its own file;
// declared here (child of `recovery`) so it can reach the module-private
// `finalize_ec_dispatch_after_convert` / `collect_ec_dispatch_candidates`.
#[cfg(test)]
#[path = "ec_g4_wedge_harness.rs"]
mod ec_g4_wedge_harness;

#[cfg(test)]
mod ec_apply_fail_tests {
    //! BUG2-EC-APPLY-FAIL: `finalize_ec_dispatch_after_convert` must keep the
    //! in-memory inflight marker when `apply_ec_conversion_done` FAILS, so the
    //! drain-only dispatch loop re-dispatches on the next tick. Releasing it on
    //! a leadership-retained apply failure strands the extent manager-pre-EC /
    //! EN-post-EC → permanent EVERSION_MISMATCH wedge until a failover.
    //!
    //! Memory-mode (no etcd): `apply_ec_conversion_done` returns `NotFound`
    //! for an absent extent BEFORE touching etcd or leadership — a faithful,
    //! deterministic model of "apply failed while this manager stays leader".
    use crate::extent_inflight::ExtentOpKind;
    use crate::AutumnManager;
    use autumn_rpc::manager_rpc::MgrExtentInfo;

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        compio::runtime::Runtime::new().unwrap().block_on(f)
    }

    fn pre_ec_extent(extent_id: u64) -> MgrExtentInfo {
        MgrExtentInfo {
            extent_id,
            replicates: vec![1, 3, 5],
            parity: vec![],
            eversion: 3,
            refs: 1,
            vp_table_refs: 0,
            sealed_length: 4096,
            sealed: true,
            avali: 0x7,
            replicate_disks: vec![10, 30, 50],
            parity_disks: vec![],
            ec_converted: false,
        }
    }

    /// REPRODUCE: apply fails (extent absent → NotFound, leadership retained).
    /// The inflight marker MUST survive so the next dispatch tick retries.
    /// Pre-fix (unconditional release on rpc_ok) this dropped the marker.
    #[test]
    fn apply_failure_keeps_inflight_marker_for_redispatch() {
        block_on(async {
            let m = AutumnManager::new();
            let extent_id: u64 = 7001;
            // Marker present, but NO extent in the store → apply -> NotFound.
            m._test_mark_ec_inflight(extent_id);
            assert_eq!(
                m.extent_inflight_op(extent_id),
                Some(ExtentOpKind::ConvertToEc),
                "precondition: marker must be in flight before finalize"
            );

            m.finalize_ec_dispatch_after_convert(extent_id, vec![1, 3, 5, 7], vec![70], 3, 4)
                .await;

            assert_eq!(
                m.extent_inflight_op(extent_id),
                Some(ExtentOpKind::ConvertToEc),
                "BUG2-EC-APPLY-FAIL: marker MUST be retained when apply fails \
                 (else the drain-only loop never re-dispatches → permanent wedge)"
            );
        });
    }

    /// CONTROL: apply succeeds (extent present, memory-mode) → marker released.
    #[test]
    fn apply_success_releases_inflight_marker() {
        block_on(async {
            let m = AutumnManager::new();
            let extent_id: u64 = 7002;
            m.store
                .inner
                .borrow_mut()
                .extents
                .insert(extent_id, pre_ec_extent(extent_id));
            m._test_mark_ec_inflight(extent_id);

            m.finalize_ec_dispatch_after_convert(extent_id, vec![1, 3, 5, 7], vec![70], 3, 4)
                .await;

            assert_eq!(
                m.extent_inflight_op(extent_id),
                None,
                "apply success must release the in-memory marker"
            );
            // And the extent is now EC-converted in memory.
            assert!(
                m.store
                    .inner
                    .borrow()
                    .extents
                    .get(&extent_id)
                    .map(|e| e.ec_converted)
                    .unwrap_or(false),
                "apply success must flip ec_converted in memory"
            );
        });
    }
}
