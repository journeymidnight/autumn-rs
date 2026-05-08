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
        if self.recovery_tasks.borrow().contains_key(&extent_id) {
            return Ok(());
        }

        // F126: do not dispatch recovery while EC conversion is in flight on
        // this extent. Recovery would write a full replica copy to the node's
        // `.dat` file; EC conversion's `commit_shard_local` then renames
        // `.ec.dat` over the same path, silently clobbering recovery's data
        // and leaving the recovery target node holding parity bytes while
        // `apply_recovery_done` still rewrites `replicates[slot]` to point
        // there — producing a duplicate-node corrupt state where the same
        // node id appears in both `replicates` and `parity`.
        if self.ec_conversion_inflight.borrow().contains(&extent_id) {
            return Ok(());
        }

        // F139: skip recovery dispatch if the extent is already queued for
        // physical deletion. Once the queue drains, s.extents will no longer
        // contain this extent and dispatch_recovery_task returns NotFound on
        // the next tick — recovery is automatically moot. Symmetric to the
        // F126 ec_conversion_inflight guard above.
        if self
            .pending_extent_deletes
            .borrow()
            .iter()
            .any(|p| p.extent_id == extent_id)
        {
            return Ok(());
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
            let resp = match self
                .conn_pool
                .call(&addr, EXT_MSG_REQUIRE_RECOVERY, payload)
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

            if let Some(etcd) = &self.etcd {
                // F149: route through the leader-fenced txn helper. The
                // create_revision==0 CAS guarantees we don't double-record an
                // in-flight recovery task; if a task already exists we return
                // Ok(()) (caller retries on next dispatch tick). If the
                // leader fence itself fails, NotLeader bubbles up so the
                // background loop short-circuits and the new leader takes
                // over.
                let key = format!("recoveryTasks/{extent_id}");
                let payload = rkyv_encode(&task).to_vec();
                let extra_cmp = vec![autumn_etcd::Cmp::create_revision(key.as_bytes(), 0)];
                let put_op = autumn_etcd::Op::put(key.as_bytes(), &payload);
                if !etcd.txn_fenced(extra_cmp, vec![put_op], vec![]).await? {
                    return Ok(());
                }
            }

            self.recovery_tasks
                .borrow_mut()
                .insert(extent.extent_id, task);
            return Ok(());
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

        // F138: if EC conversion is in flight for this extent, defer the
        // recovery apply. apply_ec_conversion_done would overwrite both
        // ex.replicates (reverting the slot replacement) and ex.eversion
        // (losing the recovery's eversion bump). The recovery_collect_loop
        // retries on the next 2 s tick after EC clears.
        if self
            .ec_conversion_inflight
            .borrow()
            .contains(&task.extent_id)
        {
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
            self.recovery_tasks
                .borrow_mut()
                .remove(&task.extent_id);
            if let Some(etcd) = &self.etcd {
                let _ = etcd
                    .put_and_delete_txn(
                        Vec::new(),
                        vec![format!("recoveryTasks/{}", task.extent_id)],
                    )
                    .await;
            }
            return Err(AppError::Precondition(format!(
                "recovery target {} for extent {} already in extent node list at a different slot; \
                 likely EC conversion completed during recovery — discarding stale apply",
                task.node_id, task.extent_id
            )));
        }

        let updated_extent = {
            let mut s = self.store.inner.borrow_mut();
            match s.extents.get_mut(&task.extent_id) {
                Some(ex) => {
                    let slot = match Self::extent_slot(ex, task.replace_id) {
                        Some(v) => v,
                        None => {
                            return Err(AppError::Precondition(format!(
                                "replace_id {} not in extent {}",
                                task.replace_id, task.extent_id
                            )));
                        }
                    };

                    if slot < ex.replicates.len() {
                        ex.replicates[slot] = task.node_id;
                        if ex.replicate_disks.len() <= slot {
                            ex.replicate_disks.resize(slot + 1, 0);
                        }
                        ex.replicate_disks[slot] = done_task.ready_disk_id;
                    } else {
                        let parity_slot = slot - ex.replicates.len();
                        ex.parity[parity_slot] = task.node_id;
                        if ex.parity_disks.len() <= parity_slot {
                            ex.parity_disks.resize(parity_slot + 1, 0);
                        }
                        ex.parity_disks[parity_slot] = done_task.ready_disk_id;
                    }

                    ex.avali |= 1u32 << slot;
                    ex.eversion += 1;
                    Some(ex.clone())
                }
                None => None,
            }
        };

        let Some(updated_extent) = updated_extent else {
            // The extent was removed from manager state before recovery
            // completed. F139: enqueue a targeted delete for the recovering
            // node so the resurrected on-disk files are reaped promptly
            // instead of waiting for the 5-minute orphan-reconcile sweep.
            let maybe_addr: Option<String> = {
                let s = self.store.inner.borrow();
                s.nodes.get(&task.node_id).map(|n| {
                    let base = Self::normalize_endpoint(&n.address);
                    Self::shard_addr_for_extent(&base, &n.shard_ports, task.extent_id)
                })
            };
            if let Some(addr) = maybe_addr {
                self.enqueue_pending_deletes(vec![PendingDelete {
                    extent_id: task.extent_id,
                    pending_addrs: vec![addr],
                    attempts: 0,
                }]);
            }
            self.recovery_tasks.borrow_mut().remove(&task.extent_id);
            return Ok(());
        };

        if let Some(etcd) = &self.etcd {
            let ex_payload = rkyv_encode(&updated_extent).to_vec();
            etcd.put_and_delete_txn(
                vec![(format!("extents/{}", updated_extent.extent_id), ex_payload)],
                vec![format!("recoveryTasks/{}", updated_extent.extent_id)],
            )
            .await?;
        }

        self.recovery_tasks
            .borrow_mut()
            .remove(&updated_extent.extent_id);
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
                let ec_inflight = self.ec_conversion_inflight.borrow();
                let extents: Vec<MgrExtentInfo> = s
                    .extents
                    .values()
                    .filter(|ex| ex.sealed_length > 0 && !ec_inflight.contains(&ex.extent_id))
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
                            if let Ok(resp) = self
                                .conn_pool
                                .call(&addr, EXT_MSG_RE_AVALI, payload)
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

            let tasks = self.recovery_tasks.borrow().clone();
            if tasks.is_empty() {
                continue;
            }

            let nodes = {
                let s = self.store.inner.borrow();
                s.nodes.clone()
            };

            let mut by_node: HashMap<u64, Vec<MgrRecoveryTask>> = HashMap::new();
            for task in tasks.values() {
                by_node.entry(task.node_id).or_default().push(task.clone());
            }

            for (node_id, node_tasks) in by_node {
                let Some(node) = nodes.get(&node_id) else {
                    continue;
                };
                let addr = Self::normalize_endpoint(&node.address);
                let payload = rkyv_encode(&ExtDfReq {
                    tasks: node_tasks,
                    disk_ids: Vec::new(),
                });
                let resp = match self.conn_pool.call(&addr, EXT_MSG_DF, payload).await {
                    Ok(v) => v,
                    Err(_) => {
                        // F121: peer is unreachable — mark all of its
                        // disks offline so allocation/recovery skip it.
                        // `ConnPool::call` already evicts the broken
                        // conn so the next poll reconnects.
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
                let addr = Self::normalize_endpoint(&node.address);
                let payload = rkyv_encode(&ExtDfReq {
                    tasks: Vec::new(),
                    disk_ids: Vec::new(),
                });
                // F121: bound df at 5 s; on failure mark this node's
                // disks offline (the pre-F121 `Err(_) => continue` left
                // a stale `online=true` and `info` would lie about a
                // dead node indefinitely).
                let resp = match self.conn_pool.call(&addr, EXT_MSG_DF, payload).await {
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
            }
        }
    }

    /// F121 helper: flip `online=false` for every disk owned by `node`
    /// when its `df` RPC fails. In-memory only — the manager reseeds
    /// disk state from etcd on leader promotion via `replay_from_etcd`,
    /// and a recovered node will overwrite `online=true` on the next
    /// successful `df` poll.
    fn mark_node_disks_offline(
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
        loop {
            compio::time::sleep(Duration::from_secs(5)).await;
            if !self.leader.get() {
                continue;
            }

            // F119-D: dedupe candidates by extent_id. After a partition
            // split, a CoW-shared extent (refs >= 2) appears in BOTH
            // child streams' extent_ids. Without dedup, the inner for
            // loop processes the same extent twice — the first pass
            // encodes the original payload into K data + M parity
            // shards, and the second pass reads the (now-shard) local
            // file and RE-ENCODES it as if it were the original
            // payload, producing sub-shards of size
            // shard_size(shard_size(original)) = original / K^2. The
            // manager state (ec_converted=true, sealed_length=original)
            // looks correct, but on-disk shards only encode original/K
            // bytes — every read past offset shard_size returns short
            // data and surfaces upstream as `logStream value short` or
            // `ec_read_full_and_slice: offset N past decoded payload
            // len M`.
            //
            // Use a HashSet on extent_id to keep the first stream's
            // entry and skip the duplicate. Stream-specific fields
            // (ec_data_shard, ec_parity_shard) are identical across
            // CoW-shared streams by construction (compute_duplicate_stream
            // copies them), so picking either stream is equivalent.
            // F126: snapshot recovery_tasks so we skip extents that have an
            // in-flight recovery. Without this, an EC conversion can dispatch
            // shards (write_shard_local + commit_shard_local rename) to the
            // SAME node that recovery is concurrently writing a full-replica
            // copy to — the rename clobbers recovery's data, leaving the
            // recovering node holding parity bytes while the manager later
            // updates `replicates[slot]` to point at it (duplicate-node
            // corrupt state).
            let recovery_inflight_extents: HashSet<u64> = self
                .recovery_tasks
                .borrow()
                .keys()
                .copied()
                .collect();

            // F172-B: snapshot node_addrs ONCE per loop tick. Pre-F172 we
            // re-collected the full `s.nodes -> address` map inside the
            // `for (ex, stream) in candidates` loop (one clone per
            // candidate extent), even though `s.nodes` is identical for
            // every iteration of a single tick — N candidates × M nodes
            // of String clones × 5 s cadence. Hoisting saves
            // `O((candidates - 1) * nodes)` String allocations per tick
            // while preserving the existing same-tick consistency
            // semantics (race vs concurrent register_node lasts at most
            // 5 s, recovered on next tick).
            let candidates: Vec<(MgrExtentInfo, MgrStreamInfo)>;
            let node_addrs: HashMap<u64, String>;
            {
                let s = self.store.inner.borrow();
                node_addrs = s
                    .nodes
                    .iter()
                    .map(|(id, n)| (*id, n.address.clone()))
                    .collect();
                let mut out = Vec::new();
                let mut seen: HashSet<u64> = HashSet::new();
                for stream in s.streams.values() {
                    if stream.ec_parity_shard == 0 {
                        continue;
                    }
                    for &eid in &stream.extent_ids {
                        if !seen.insert(eid) {
                            continue;
                        }
                        if recovery_inflight_extents.contains(&eid) {
                            continue;
                        }
                        if let Some(ex) = s.extents.get(&eid) {
                            if ex.sealed_length == 0 || ex.ec_converted {
                                continue;
                            }
                            out.push((ex.clone(), stream.clone()));
                        }
                    }
                }
                candidates = out;
            }

            for (ex, stream) in candidates {
                let extent_id = ex.extent_id;
                let data_shards = stream.ec_data_shard as usize;
                let parity_shards = stream.ec_parity_shard as usize;
                let total_shards = data_shards + parity_shards;

                if self.ec_conversion_inflight.borrow().contains(&extent_id) {
                    continue;
                }

                let mut target_nodes: Vec<u64> = ex.replicates.clone();
                let mut target_addrs: Vec<String> = Vec::new();
                let mut extra_disk_ids: Vec<u64> = Vec::new();

                for &nid in &target_nodes {
                    if let Some(addr) = node_addrs.get(&nid) {
                        target_addrs.push(addr.clone());
                    } else {
                        target_addrs.clear();
                        break;
                    }
                }
                if target_addrs.is_empty() {
                    continue;
                }

                if total_shards > target_nodes.len() {
                    let extra_needed = total_shards - target_nodes.len();
                    // F144: shuffle candidates so EC parity slots don't
                    // always land on the same low-`node_id` peers.
                    // Pre-F144 used HashMap iteration order + take(N),
                    // which is deterministic per program run and biased
                    // toward whichever node_id happened to be visited
                    // first.
                    let extra_candidates: Vec<_> = {
                        use rand::seq::SliceRandom;
                        let s = self.store.inner.borrow();
                        let existing: HashSet<u64> = target_nodes.iter().copied().collect();
                        let mut pool: Vec<_> = s
                            .nodes
                            .values()
                            .filter(|n| !existing.contains(&n.node_id))
                            .cloned()
                            .collect();
                        pool.shuffle(&mut rand::thread_rng());
                        pool.into_iter().take(extra_needed).collect()
                    };
                    if extra_candidates.len() < extra_needed {
                        continue;
                    }
                    for node in &extra_candidates {
                        match self.alloc_extent_on_node(&node.address, extent_id).await {
                            Ok(disk_id) => {
                                target_nodes.push(node.node_id);
                                target_addrs.push(node.address.clone());
                                extra_disk_ids.push(disk_id);
                            }
                            Err(_) => {
                                target_nodes.clear();
                                break;
                            }
                        }
                    }
                    if target_nodes.len() < total_shards {
                        continue;
                    }
                }

                target_nodes.truncate(total_shards);
                target_addrs.truncate(total_shards);

                self.ec_conversion_inflight
                    .borrow_mut()
                    .insert(extent_id);

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
                // The post-conversion eversion. Sent in-band so every
                // target node bumps `entry.eversion` to match what
                // `apply_ec_conversion_done` will persist to etcd. This
                // closes the read-side stale-cache window: once the
                // coordinator returns OK, any client read with a stale
                // (pre-EC) eversion is rejected with
                // CODE_EVERSION_MISMATCH and the client refetches.
                let new_eversion = ex.eversion + 1;

                let payload = rkyv_encode(&ExtConvertToEcReq {
                    extent_id,
                    data_shards: data_shards as u32,
                    parity_shards: parity_shards as u32,
                    target_addrs: ec_target_addrs,
                    eversion: new_eversion,
                });

                let result = self
                    .conn_pool
                    .call(&coordinator_addr, EXT_MSG_CONVERT_TO_EC, payload)
                    .await;

                // F138: keep the extent in ec_conversion_inflight until AFTER
                // apply_ec_conversion_done. The lock must cover the full
                // dispatch → RPC await → apply window so that apply_recovery_done,
                // mark_extent_available, and handle_multi_modify_split see the lock
                // and defer rather than racing the in-memory + etcd write.
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
                        Err(_) => false,
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

                // Release the lock only after apply completes (or after RPC failure).
                self.ec_conversion_inflight.borrow_mut().remove(&extent_id);
            }
        }
    }

    pub(crate) async fn apply_ec_conversion_done(
        &self,
        extent_id: u64,
        target_nodes: Vec<u64>,
        extra_disk_ids: Vec<u64>,
        data_shards: usize,
        new_eversion: u64,
    ) -> Result<(), AppError> {
        let updated = {
            let mut s = self.store.inner.borrow_mut();
            let ex = s
                .extents
                .get_mut(&extent_id)
                .ok_or_else(|| AppError::NotFound(format!("extent {extent_id}")))?;

            let mut all_disks = ex.replicate_disks.clone();
            all_disks.extend_from_slice(&extra_disk_ids);
            all_disks.truncate(target_nodes.len());

            ex.ec_converted = true;
            ex.replicates = target_nodes[..data_shards].to_vec();
            ex.parity = target_nodes[data_shards..].to_vec();
            ex.replicate_disks = all_disks[..data_shards].to_vec();
            ex.parity_disks = all_disks[data_shards..].to_vec();
            // Use the eversion sent in-band to the extent nodes via
            // ExtConvertToEcReq. Manager + every shard host now agree on
            // the same post-EC eversion.
            ex.eversion = new_eversion;
            ex.clone()
        };

        if let Some(etcd) = &self.etcd {
            let key = format!("extents/{}", extent_id);
            let val = rkyv_encode(&updated).to_vec();
            etcd.put_msgs_txn(vec![(key, val)]).await?;
        }

        Ok(())
    }
}
