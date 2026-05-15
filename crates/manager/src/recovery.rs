//! Recovery dispatch/collect loops and EC conversion for AutumnManager.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use autumn_common::AppError;
use autumn_rpc::manager_rpc::*;
use bytes::Bytes;

use crate::{AutumnManager, PendingDelete};

impl AutumnManager {
    pub(crate) async fn dispatch_recovery_task(
        &self,
        extent_id: u64,
        replace_id: u64,
    ) -> Result<(), AppError> {
        // F126 / F139 / F207-C: any in-flight stream-layer op on this
        // extent blocks recovery dispatch. Pre-F207-C this was three
        // separate ad-hoc checks (recovery_tasks dedup, ec_conversion_inflight,
        // pending_extent_deletes). F207-C collapses them into one ledger
        // read. The probe distinguishes "already-recovering (idempotent
        // OK)" from "different op in flight (caller retries)".
        match self.extent_inflight_op(extent_id) {
            Some(crate::extent_inflight::ExtentOpKind::Recovery) => return Ok(()),
            Some(_) => return Ok(()),
            None => {}
        }

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

        for candidate in &candidates {
            let base = Self::normalize_endpoint(&candidate.address);
            // F099-M: recovery targets a specific extent_id → route to owner shard.
            let addr = Self::shard_addr_for_extent(
                &base,
                &candidate.shard_ports,
                extent_id,
            );

            let task = MgrRecoveryTask {
                extent_id,
                replace_id,
                node_id: candidate.node_id,
                start_time: Self::epoch_seconds(),
            };

            let payload = rkyv_encode(&ExtRequireRecoveryReq { task: task.clone() });
            // 30 s ceiling — REQUIRE_RECOVERY only kicks off the
            // background `run_recovery_task` on the EN; the EN returns
            // OK immediately. A paged-out / dead EN otherwise wedges
            // this loop and starves recovery of all other extents.
            let resp = match self
                .conn_pool
                .call_timeout(&addr, EXT_MSG_REQUIRE_RECOVERY, payload, Duration::from_secs(30))
                .await
            {
                Ok(v) => v,
                Err(_) => continue,
            };
            let r: ExtCodeResp = match rkyv_decode(&resp) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if r.code != CODE_OK {
                continue;
            }

            // F207-C: acquire the unified inflight marker via CAS +
            // F149 leader fence. Replaces the pre-F207-C
            // `etcd.txn_fenced(create_revision==0, put recoveryTasks/<id>)`
            // + in-memory `recovery_tasks.insert` pair with one call. The
            // CAS makes "already in-flight" a clean Precondition error
            // path (which we map to Ok — the caller's retry is moot
            // because someone else is already on it).
            match self
                .acquire_extent_inflight(
                    extent.extent_id,
                    crate::extent_inflight::ExtentOpPayload::Recovery(task),
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(AppError::Precondition(_)) => return Ok(()),
                Err(other) => return Err(other),
            }
        }

        Err(AppError::Precondition(
            "all recovery candidates rejected".to_string(),
        ))
    }

    pub(crate) async fn apply_recovery_done(
        &self,
        done_task: MgrRecoveryTaskDone,
    ) -> Result<(), AppError> {
        let task = &done_task.task;

        // F138 / F207-B: if EC conversion is in flight for this extent,
        // defer the recovery apply. apply_ec_conversion_done would
        // overwrite both ex.replicates (reverting the slot replacement)
        // and ex.eversion (losing the recovery's eversion bump). The
        // recovery_collect_loop retries on the next 2 s tick after EC
        // clears. F207-B: reads the unified ledger via `extent_inflight_op`.
        if matches!(
            self.extent_inflight_op(task.extent_id),
            Some(crate::extent_inflight::ExtentOpKind::ConvertToEc)
        ) {
            return Err(AppError::Precondition(format!(
                "ec conversion in flight on extent {}; deferring recovery apply",
                task.extent_id
            )));
        }

        // F126: precheck — if `task.node_id` is already present in this
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
                Some(ex) => match Self::extent_slot(ex, task.replace_id) {
                    Some(slot) => Some(
                        Self::extent_nodes(ex)
                            .iter()
                            .enumerate()
                            .any(|(i, &id)| i != slot && id == task.node_id),
                    ),
                    None => None,
                },
                None => Some(false),
            }
        };
        if matches!(layout_changed, Some(true)) {
            // F207-D: release the Recovery marker so the dedup check in
            // dispatch_recovery_task doesn't permanently block future
            // attempts to repair this slot. Legacy `recoveryTasks/<id>`
            // delete dropped (the backward-compat dual-key path lived in
            // F207-C only).
            if let Some(etcd) = &self.etcd {
                let _ = etcd
                    .put_and_delete_txn(
                        Vec::new(),
                        vec![Self::extent_inflight_key(task.extent_id)],
                    )
                    .await;
            }
            self.commit_extent_inflight_release(task.extent_id);
            return Err(AppError::Precondition(format!(
                "recovery target {} for extent {} already in extent node list at a different slot; \
                 likely EC conversion completed during recovery — discarding stale apply",
                task.node_id, task.extent_id
            )));
        }

        // F209-B: layout_changed == None ⇒ the extent exists but
        // `task.replace_id` is no longer in any slot (extent_slot
        // returned None above). Pre-F209-B this case fell through to
        // the apply block where the inner `slot None` branch did
        // `return Err(...)` from inside borrow_mut WITHOUT releasing
        // the Recovery marker — violating invariant I3 (every
        // acquire has a matching release). The marker survived until
        // F208's 10-min sweep, blocking any other op on the extent
        // for that window. Release now, then return.
        if layout_changed.is_none() {
            if let Some(etcd) = &self.etcd {
                let _ = etcd
                    .put_and_delete_txn(
                        Vec::new(),
                        vec![Self::extent_inflight_key(task.extent_id)],
                    )
                    .await;
            }
            self.commit_extent_inflight_release(task.extent_id);
            return Err(AppError::Precondition(format!(
                "replace_id {} not in extent {}",
                task.replace_id, task.extent_id
            )));
        }

        // F210-A1 etcd-first: compute updated_extent from a clone under
        // read-only borrow. Pre-F210-A1 the borrow_mut block mutated
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
                        // F209-B layout_changed.is_none() branch above
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
            // completed. F139 / F207-C: release the Recovery marker, then
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
            // Release Recovery (etcd + in-memory). F207-D removed the
            // legacy-key delete entry.
            if let Some(etcd) = &self.etcd {
                let _ = etcd
                    .put_and_delete_txn(
                        Vec::new(),
                        vec![Self::extent_inflight_key(task.extent_id)],
                    )
                    .await;
            }
            self.commit_extent_inflight_release(task.extent_id);
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
            // F207-D: atomic put + delete txn. Releases the Recovery
            // marker in the same txn that writes the updated extent
            // state. Legacy `recoveryTasks/<id>` delete dropped.
            let ex_payload = rkyv_encode(&updated_extent).to_vec();
            etcd.put_and_delete_txn(
                vec![(format!("extents/{}", updated_extent.extent_id), ex_payload)],
                vec![Self::extent_inflight_key(updated_extent.extent_id)],
            )
            .await?;
        }

        // F210-A1: only AFTER etcd success do we apply to in-memory.
        // (Pre-F210-A1 this was the borrow_mut block above; now the
        // borrow_mut here is the sole in-memory write.)
        {
            let mut s = self.store.inner.borrow_mut();
            s.extents.insert(updated_extent.extent_id, updated_extent.clone());
        }
        self.commit_extent_inflight_release(updated_extent.extent_id);
        Ok(())
    }

    pub(crate) async fn recovery_dispatch_loop(self) {
        loop {
            compio::time::sleep(Duration::from_secs(2)).await;
            if !self.leader.get() {
                continue;
            }

            // F172-A: pre-filter under the store borrow so we DON'T clone
            // extents that the loop body will skip on the next line. The
            // loop body's first checks are `if ex.sealed_length == 0
            // { continue; }` and `if ec_conversion_inflight.contains(...)
            // { continue; }`. Pre-F172 we cloned every single extent in
            // `s.extents` (~200 B each for the 4 Vec fields) only to drop
            // most on the floor — a 10K-extent cluster cloned 2 MB inline
            // per 2 s tick on the manager's compio runtime, blocking
            // heartbeat / register_ps / get_regions handlers for a few ms
            // each tick. F138's ec_conversion_inflight gating is unchanged
            // — `apply_recovery_done` / `mark_extent_available` /
            // `handle_multi_modify_split` still re-check the set at apply
            // time, so a stale snapshot here is safe (drops at most one
            // tick's worth of dispatch latency on the racing extent).
            let (extents, nodes, disks) = {
                let s = self.store.inner.borrow();
                // F207-B: read the unified inflight ledger instead of the
                // old `ec_conversion_inflight` HashSet. We filter for
                // ConvertToEc specifically — recovery dispatch on an extent
                // that's mid-Recovery or mid-Delete is handled by
                // `dispatch_recovery_task`'s own refuse-at-start (F207-C
                // collapses those into the same probe).
                let inflight = self.inflight.borrow();
                let extents: Vec<MgrExtentInfo> = s
                    .extents
                    .values()
                    .filter(|ex| {
                        if ex.sealed_length == 0 {
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
                                let _ = self
                                    .dispatch_recovery_task(ex.extent_id, node_id)
                                    .await;
                                continue;
                            }
                        }
                    }

                    if (ex.avali & bit) == 0 {
                        if let Some(n) = node.clone() {
                            let base = Self::normalize_endpoint(&n.address);
                            // F099-M: re_avali on specific extent → owner shard.
                            let addr = Self::shard_addr_for_extent(
                                &base,
                                &n.shard_ports,
                                ex.extent_id,
                            );
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
                                .call_timeout(&addr, EXT_MSG_RE_AVALI, payload, Duration::from_secs(30))
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
                        let _ = self.dispatch_recovery_task(ex.extent_id, node_id).await;
                        continue;
                    }

                    let healthy = match node {
                        Some(n) => self
                            .commit_length_on_node(&n.address, ex.extent_id)
                            .await
                            .is_ok(),
                        None => false,
                    };
                    if !healthy {
                        let _ = self.dispatch_recovery_task(ex.extent_id, node_id).await;
                    }
                }
            }
        }
    }

    pub(crate) async fn recovery_collect_loop(self) {
        loop {
            compio::time::sleep(Duration::from_secs(2)).await;
            if !self.leader.get() {
                continue;
            }

            // F207-C: source the active Recovery tasks from the unified
            // ledger instead of the deleted `recovery_tasks` HashMap.
            let tasks: Vec<MgrRecoveryTask> = self
                .inflight
                .borrow()
                .values()
                .filter_map(|rec| match rec.unpack() {
                    Some((_, crate::extent_inflight::ExtentOpPayload::Recovery(t))) => Some(t),
                    _ => None,
                })
                .collect();
            if tasks.is_empty() {
                continue;
            }

            let nodes = {
                let s = self.store.inner.borrow();
                s.nodes.clone()
            };

            let mut by_node: HashMap<u64, Vec<MgrRecoveryTask>> = HashMap::new();
            for task in &tasks {
                by_node.entry(task.node_id).or_default().push(task.clone());
            }

            for (node_id, node_tasks) in by_node {
                let Some(node) = nodes.get(&node_id) else {
                    continue;
                };
                // F191: prefer the control_address; fall back to data
                // plane address for legacy / not-yet-re-registered nodes.
                let raw_addr = if node.control_address.is_empty() {
                    &node.address
                } else {
                    &node.control_address
                };
                let addr = Self::normalize_endpoint(raw_addr);
                let payload = rkyv_encode(&ExtDfReq {
                    tasks: node_tasks,
                    disk_ids: Vec::new(),
                });
                // F191 P0: bound DF at 5 s via control_pool.call_timeout.
                // Pre-F191 the comment in disk_status_update_loop claimed
                // the call was timeout-bounded but the conn_pool.call
                // path had no timeout, so a single slow / stuck DF could
                // hang the loop tick.
                let resp = match self
                    .control_pool
                    .call_timeout(&addr, EXT_MSG_DF, payload, Duration::from_secs(5))
                    .await
                {
                    Ok(v) => v,
                    Err(_) => {
                        // F121: peer is unreachable — mark all of its
                        // disks offline so allocation/recovery skip it.
                        // `ConnPool::call_timeout` already evicts the
                        // broken conn so the next poll reconnects.
                        Self::mark_node_disks_offline(&self.store, node);
                        continue;
                    }
                };
                let df: ExtDfResp = match rkyv_decode(&resp) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                // F121: see disk_status_update_loop — promote on the
                // call-level signal, not per-payload disk_id, because
                // the wire status uses the extent-node's local disk_id.
                Self::mark_node_disks_online(&self.store, node);
                for done in df.done_tasks {
                    let _ = self.apply_recovery_done(done).await;
                }
            }
        }
    }

    /// Periodically polls all extent nodes for disk status updates.
    /// Matches Go's `routineUpdateDF` (10-20s interval).
    pub(crate) async fn disk_status_update_loop(self) {
        loop {
            compio::time::sleep(Duration::from_secs(10)).await;
            if !self.leader.get() {
                continue;
            }

            let nodes = {
                let s = self.store.inner.borrow();
                s.nodes.clone()
            };

            for node in nodes.values() {
                // F191: route DF over the dedicated control_pool against
                // node.control_address (fall back to address for legacy
                // nodes). Replaces the F121 "bound df at 5 s" comment
                // with the actually-applied 5 s timeout via
                // control_pool.call_timeout — the previous conn_pool.call
                // had no timeout, so under heavy data-plane load
                // (CONVERT_TO_EC, COPY_EXTENT, RECOVERY) on the same
                // multiplexed RpcClient the next DF could stall long
                // enough to surface as ConnectionClosed and falsely
                // mark the node offline.
                let raw_addr = if node.control_address.is_empty() {
                    &node.address
                } else {
                    &node.control_address
                };
                let addr = Self::normalize_endpoint(raw_addr);
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
                        // F121: see recovery_collect_loop comment.
                        Self::mark_node_disks_offline(&self.store, node);
                        continue;
                    }
                };
                if rkyv_decode::<ExtDfResp>(&resp).is_err() {
                    continue;
                }
                // F121: a successful df proves the node is reachable, so
                // promote each of its `MgrNodeInfo.disks` back to
                // `online=true`. The per-disk-id status carried in the
                // response keys on the *extent-node's* local disk_id
                // (e.g. `--disk-id 4`), which is unrelated to the
                // manager's allocated disk_id (e.g. 8). Treating the
                // call result as the liveness signal sidesteps that
                // mismatch entirely; recovery-side per-disk failure is
                // still surfaced via `mark_disk_offline_for_extent` on
                // the extent-node and propagated through dedicated
                // recovery RPCs.
                Self::mark_node_disks_online(&self.store, node);
                // F192: drop any pending push-based failure reports for
                // this node — the call-level liveness signal proves the
                // node is reachable, so a residual stale burst of
                // reports must not re-flip the node offline on the next
                // tick's quorum check.
                self.recent_failure_reports
                    .borrow_mut()
                    .remove(&node.node_id);
            }
        }
    }

    /// F121 helper: flip `online=false` for every disk owned by `node`
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

    /// F121 helper: counterpart to `mark_node_disks_offline`. Flip
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

    pub(crate) async fn ec_conversion_dispatch_loop(self) {
        // F198: short initial delay so post-restart re-dispatch of
        // replay-loaded markers fires quickly. Without this, PS startup
        // could see up to 5 s of eversion-mismatch errors against extents
        // whose `apply_ec_conversion_done` didn't commit in the previous
        // lifetime. After the first tick, fall back to the steady-state
        // 5 s cadence.
        let mut delay = Duration::from_millis(500);
        loop {
            compio::time::sleep(delay).await;
            delay = Duration::from_secs(5);
            if !self.leader.get() {
                continue;
            }

            // F203: drain-only. Pre-F203 this loop scanned `s.streams` and
            // built a fresh candidate set from every sealed-not-converted
            // extent. Stage 3 of the mechanism/policy separation refactor
            // removes that — the manager is no longer the policy decider;
            // an external controller queries `MSG_GET_POLICY_CANDIDATES`
            // (where `POLICY_KIND_EC` advice lives, F202) and calls
            // `MSG_FORCE_EC_CONVERT` (F203) to ask for a specific extent.
            // That handler persists a rich `pending_ec_dispatch` marker
            // to etcd; this loop is the consumer that drains the marker
            // set. F198 leader-failover replay continues to work the
            // same way — `replay_from_etcd` rehydrates the markers and
            // the next tick drains them.
            //
            // Recovery-inflight skip (F126) still applies to avoid
            // colliding with `run_recovery_task` on the same extent.
            // F207-C: read from the unified ledger.
            let recovery_inflight_extents: HashSet<u64> = self
                .inflight
                .borrow()
                .iter()
                .filter_map(|(id, rec)| match rec.kind() {
                    Some(crate::extent_inflight::ExtentOpKind::Recovery) => Some(*id),
                    _ => None,
                })
                .collect();

            // F172-B: snapshot node_addrs ONCE per loop tick.
            let node_addrs: HashMap<u64, String>;
            // F207-B: candidate set = ledger entries with kind == ConvertToEc.
            // Pre-F207 this was `pending_ec_dispatch` keys; the ledger
            // collapses that map + `ec_conversion_inflight` HashSet into one
            // source of truth. The matching extent + stream entries are
            // looked up under the same borrow so we get a consistent
            // snapshot.
            //
            // F119-D dedup is structural here: HashMap keys are unique
            // by construction, so the seen-set guard is no longer needed.
            let candidates: Vec<(MgrExtentInfo, MgrStreamInfo, MgrEcDispatchInflight)>;
            {
                let s = self.store.inner.borrow();
                node_addrs = s
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
                let mut out = Vec::new();
                for (eid, params) in pending {
                    if recovery_inflight_extents.contains(&eid) {
                        continue;
                    }
                    let ex = match s.extents.get(&eid) {
                        Some(e) => e.clone(),
                        None => {
                            // Marker for an extent that no longer exists
                            // (deleted between marker write and this
                            // tick). Drop the marker.
                            let mgr = self.clone();
                            compio::runtime::spawn(async move {
                                let _ = mgr.drain_extent_inflight_marker(eid).await;
                            })
                            .detach();
                            continue;
                        }
                    };
                    if ex.ec_converted || ex.sealed_length == 0 {
                        // Marker stale (already converted, or extent
                        // got truncated to 0). Drop the marker.
                        let mgr = self.clone();
                        compio::runtime::spawn(async move {
                            let _ = mgr.drain_extent_inflight_marker(eid).await;
                        })
                        .detach();
                        continue;
                    }
                    // Find the stream that owns this extent so we recover
                    // its EC shape. CoW-shared extents (refs >= 2) appear
                    // in multiple streams; any pick works because
                    // `compute_duplicate_stream` clones
                    // `(ec_data_shard, ec_parity_shard)`.
                    let stream = s
                        .streams
                        .values()
                        .find(|st| {
                            st.ec_parity_shard > 0 && st.extent_ids.contains(&eid)
                        })
                        .cloned();
                    let stream = match stream {
                        Some(s) => s,
                        None => continue,
                    };
                    out.push((ex, stream, params));
                }
                candidates = out;
            }

            for (ex, stream, replay_params) in candidates {
                let extent_id = ex.extent_id;
                let data_shards = stream.ec_data_shard as usize;
                let parity_shards = stream.ec_parity_shard as usize;
                let total_shards = data_shards + parity_shards;

                // F198 / F207-B: the marker in the ledger IS the rich
                // dispatch record — every iteration enters this loop with a
                // valid `MgrEcDispatchInflight`. There is no "pre-F198 empty
                // legacy marker" branch under F207-B because the ledger only
                // accepts typed records. Replay folds legacy markers into
                // the ledger or drops them with WARN (see
                // `replay_from_etcd`).
                //
                // We split the path into "replay re-use" vs "fresh assign"
                // based on whether the marker pre-existed BEFORE this tick
                // (replay) or we're about to write it (fresh). For
                // simplicity we treat the marker's presence as load-bearing:
                // since `acquire_extent_inflight` is the only writer and is
                // called below in the "fresh" branch, any marker we observe
                // at the top of this iteration came from either replay or
                // `handle_force_ec_convert` — both of which represent an
                // authoritative assignment we should reuse.

                // F207-B: the ledger marker is authoritative — reuse the
                // exact target_nodes + extra_disk_ids + new_eversion that
                // were committed when the marker was acquired (either by
                // `handle_force_ec_convert` or restored by
                // `replay_from_etcd`). Under F203 the dispatch loop is
                // drain-only; under F207-B it never computes a fresh
                // assignment of its own. If `handle_force_ec_convert`
                // ever needs to choose between data_shards (K) and
                // K+M targets, it does so at acquire time and the loop
                // body just consumes.
                let params = &replay_params;
                let target_nodes = params.target_nodes.clone();
                let extra_disk_ids = params.extra_disk_ids.clone();
                let new_eversion = params.new_eversion;

                // Discard total_shards-mismatched markers loudly — these
                // only arise from operator-induced inconsistency (e.g.,
                // calling `set-stream-ec` to change K+M between
                // `handle_force_ec_convert` and a later dispatch tick).
                // Dropping the marker is the conservative choice; the
                // operator can re-issue `force-ec-convert` to start a
                // fresh dispatch under the new shape.
                if target_nodes.len() != total_shards {
                    tracing::warn!(
                        extent_id,
                        marker_targets = target_nodes.len(),
                        stream_total_shards = total_shards,
                        "F207-B: dispatch marker target count != stream K+M; \
                         dropping stale marker"
                    );
                    let mgr = self.clone();
                    compio::runtime::spawn(async move {
                        let _ = mgr.drain_extent_inflight_marker(extent_id).await;
                    })
                    .detach();
                    continue;
                }

                let mut target_addrs: Vec<String> = Vec::with_capacity(target_nodes.len());
                for &nid in &target_nodes {
                    if let Some(addr) = node_addrs.get(&nid) {
                        target_addrs.push(addr.clone());
                    } else {
                        target_addrs.clear();
                        break;
                    }
                }
                if target_addrs.len() < target_nodes.len() {
                    tracing::warn!(
                        extent_id,
                        target_nodes = ?params.target_nodes,
                        "F207-B: marker target_node missing from cluster; deferring re-dispatch"
                    );
                    continue;
                }

                // F099-M: coordinator is the shard that owns `extent_id` on
                // the first replica. For convert_to_ec, the coordinator reads
                // the full extent locally, then dispatches shards to targets.
                let coordinator_base = Self::normalize_endpoint(&target_addrs[0]);
                let coordinator_shard_ports = self.shard_ports_for_addr(&coordinator_base);
                let coordinator_addr = Self::shard_addr_for_extent(
                    &coordinator_base,
                    &coordinator_shard_ports,
                    extent_id,
                );
                // Rewrite target_addrs to each target node's owner shard for
                // `extent_id` so the coordinator's WriteShard RPCs land on the
                // correct shard on each peer.
                let ec_target_addrs: Vec<String> = target_addrs
                    .iter()
                    .map(|a| {
                        let b = Self::normalize_endpoint(a);
                        let sp = self.shard_ports_for_addr(&b);
                        Self::shard_addr_for_extent(&b, &sp, extent_id)
                    })
                    .collect();
                let target_nodes_clone = target_nodes.clone();
                let extra_disk_ids_clone = extra_disk_ids.clone();
                // F198: `new_eversion` was computed above (fresh: `ex.eversion+1`;
                // replay: persisted value from `pending_ec_dispatch`). The
                // post-conversion eversion is sent in-band so every target
                // node bumps `entry.eversion` to match what
                // `apply_ec_conversion_done` will persist to etcd, closing
                // the read-side stale-cache window.

                let payload = rkyv_encode(&ExtConvertToEcReq {
                    extent_id,
                    data_shards: data_shards as u32,
                    parity_shards: parity_shards as u32,
                    target_addrs: ec_target_addrs,
                    eversion: new_eversion,
                });

                // 60 s ceiling so a paged-out / silently dead EN doesn't
                // wedge the dispatch loop indefinitely. The convert path
                // itself can take seconds (RS-encode of multi-GiB extents
                // + 3-replica WriteShard fanout + commit-rename), so the
                // bound is generous; the goal is only to bound the
                // pathological case (TCP keepalive timer is hours).
                let result = self
                    .conn_pool
                    .call_timeout(
                        &coordinator_addr,
                        EXT_MSG_CONVERT_TO_EC,
                        payload,
                        Duration::from_secs(60),
                    )
                    .await;

                // F138 / F207-B: the ledger marker is the eversion-bump
                // lock; it must cover the full dispatch → RPC await → apply
                // window. `apply_ec_conversion_done` releases it atomically
                // with the `extents/<id>` etcd write (one `txn_fenced` for
                // both keys); the in-memory ledger entry is then cleared
                // in this loop after apply succeeds.
                let rpc_ok = match result {
                    Ok(resp_data) => match rkyv_decode::<ExtCodeResp>(&resp_data) {
                        Ok(r) if r.code == CODE_OK => true,
                        Ok(r) => {
                            tracing::warn!(
                                "EC conversion failed for extent {extent_id}: {}",
                                r.message
                            );
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

                if rpc_ok {
                    let _ = self
                        .apply_ec_conversion_done(
                            extent_id,
                            target_nodes_clone,
                            extra_disk_ids_clone,
                            data_shards,
                            new_eversion,
                        )
                        .await;
                }
                // F207-B: on RPC failure the ledger marker stays in place;
                // the next tick re-dispatches against the SAME assignment
                // (coordinator's F119-D / F153 make repeated convert calls
                // idempotent). On RPC success `apply_ec_conversion_done`
                // already deleted the etcd marker as part of its atomic
                // put-and-delete txn; we just clear the in-memory shadow
                // here.
                if rpc_ok {
                    self.commit_extent_inflight_release(extent_id);
                }
            }
        }
    }

    /// F207-B: drop a stale or no-longer-valid `extent_inflight/<id>` marker.
    /// Used by the dispatch loop when it observes a ledger entry for an
    /// extent that has been deleted or that has incompatible state (e.g.,
    /// already EC-converted) — best-effort cleanup so the next tick's
    /// candidate set shrinks. Idempotent.
    async fn drain_extent_inflight_marker(&self, extent_id: u64) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let del_op = self.build_extent_inflight_release_op(extent_id);
            // Use `put_and_delete_txn` (one-element delete list) so the F149
            // fence applies. A `false` return from the underlying CAS is
            // impossible here (no extra_cmp); only NotLeader can happen and
            // bubbles up.
            etcd.put_and_delete_txn(
                Vec::new(),
                vec![Self::extent_inflight_key(extent_id)],
            )
            .await?;
            let _ = del_op; // silence unused (kept for symmetry with apply path)
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
        // F210-A1 etcd-first: compute `updated` from a clone under
        // read-only borrow. Pre-F210-A1 the borrow_mut block mutated
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
            // F206: post-EC the extent has K+M shards across K+M nodes;
            // every slot is available by construction.
            new_ex.avali = Self::all_bits(target_nodes.len());
            new_ex
        };

        if let Some(etcd) = &self.etcd {
            // F207-B/D: bundle the marker delete into the same etcd txn
            // as the `extents/<id>` put. Pre-F207 this was two separate
            // etcd round-trips; F207 made them atomic. F210-A1 makes
            // the in-memory apply happen ONLY after this txn lands.
            let key = format!("extents/{}", extent_id);
            let val = rkyv_encode(&updated).to_vec();
            etcd.put_and_delete_txn(
                vec![(key, val)],
                vec![Self::extent_inflight_key(extent_id)],
            )
            .await?;
        }

        // F210-A1: only after etcd success do we apply to in-memory.
        {
            let mut s = self.store.inner.borrow_mut();
            s.extents.insert(extent_id, updated);
        }

        Ok(())
    }
}
