//! Recovery dispatch/collect loops and EC conversion for AutumnManager.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use autumn_common::AppError;
use autumn_rpc::manager_rpc::*;
use bytes::Bytes;

use crate::AutumnManager;

impl AutumnManager {
    async fn dispatch_recovery_task(
        &self,
        extent_id: u64,
        replace_id: u64,
    ) -> Result<(), AppError> {
        if self.recovery_tasks.borrow().contains_key(&extent_id) {
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
                let key = format!("recoveryTasks/{extent_id}");
                let payload = rkyv_encode(&task).to_vec();
                let cmp =
                    autumn_etcd::Cmp::create_revision(key.as_bytes(), 0);
                let txn = autumn_etcd::proto::TxnRequest {
                    compare: vec![cmp],
                    success: vec![autumn_etcd::Op::put(key.as_bytes(), &payload)],
                    failure: vec![],
                };
                let c = etcd.client.borrow_mut();
                let resp = c
                    .txn(txn)
                    .await
                    .map_err(|e| AppError::Internal(e.to_string()))?;
                if !resp.succeeded {
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

    async fn apply_recovery_done(
        &self,
        done_task: MgrRecoveryTaskDone,
    ) -> Result<(), AppError> {
        let task = &done_task.task;

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
            self.recovery_tasks.borrow_mut().remove(&task.extent_id);
            return Ok(());
        };

        if let Some(etcd) = &self.etcd {
            let ex_payload = rkyv_encode(&updated_extent).to_vec();
            etcd.put_and_delete_txn(
                vec![(format!("extents/{}", updated_extent.extent_id), ex_payload)],
                vec![format!("recoveryTasks/{}", updated_extent.extent_id)],
            )
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
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

            let (extents, nodes, disks) = {
                let s = self.store.inner.borrow();
                (
                    s.extents.values().cloned().collect::<Vec<_>>(),
                    s.nodes.clone(),
                    s.disks.clone(),
                )
            };

            for ex in extents {
                if ex.sealed_length == 0 {
                    continue;
                }
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
            let candidates: Vec<(MgrExtentInfo, MgrStreamInfo)> = {
                let s = self.store.inner.borrow();
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
                        if let Some(ex) = s.extents.get(&eid) {
                            if ex.sealed_length == 0 || ex.ec_converted {
                                continue;
                            }
                            out.push((ex.clone(), stream.clone()));
                        }
                    }
                }
                out
            };

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

                let node_addrs: HashMap<u64, String> = {
                    let s = self.store.inner.borrow();
                    s.nodes
                        .iter()
                        .map(|(id, n)| (*id, n.address.clone()))
                        .collect()
                };

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
                    let extra_candidates: Vec<_> = {
                        let s = self.store.inner.borrow();
                        let existing: HashSet<u64> = target_nodes.iter().copied().collect();
                        s.nodes
                            .values()
                            .filter(|n| !existing.contains(&n.node_id))
                            .take(extra_needed)
                            .cloned()
                            .collect()
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

                self.ec_conversion_inflight.borrow_mut().remove(&extent_id);

                match result {
                    Ok(resp_data) => {
                        if let Ok(r) = rkyv_decode::<ExtCodeResp>(&resp_data) {
                            if r.code != CODE_OK {
                                tracing::warn!(
                                    "EC conversion failed for extent {extent_id}: {}",
                                    r.message
                                );
                                continue;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("EC conversion failed for extent {extent_id}: {e}");
                        continue;
                    }
                }

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
        }
    }

    async fn apply_ec_conversion_done(
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
            etcd.put_msgs_txn(vec![(key, val)])
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?;
        }

        Ok(())
    }
}
