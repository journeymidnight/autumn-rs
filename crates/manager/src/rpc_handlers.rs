//! RPC serve, dispatch, and handler methods for AutumnManager.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::Result;
use autumn_common::AppError;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::{Frame, FrameDecoder, HandlerResult, StatusCode};
use bytes::Bytes;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::BufResult;

use std::rc::Rc;

use crate::{AutumnManager, ConnPool, PendingDelete};

impl AutumnManager {
    // ── F210-C4: pull-sync vp_refs from PS ──────────────────────────────
    //
    // Before `handle_multi_modify_split` / `handle_merge_partitions`
    // commit their atomic etcd txn, manager actively pulls the
    // current vp_refs snapshot from the relevant PS and applies it
    // via the same path `handle_sync_partition_vp_refs` uses. This
    // closes the race where a previous PS-initiated sync failed
    // (PS marked dirty per F210-C4's wrapper), leaving manager's
    // `vp_table_refs` stale, and a subsequent merge/split would
    // compute `modified_extents` against that stale view —
    // potentially under-counting refs and approving deletion of
    // extents whose live VPs are in a newly-published SST.
    async fn pull_and_apply_vp_refs(&self, part_id: u64, part_addr: &str) -> Result<(), AppError> {
        let req = autumn_rpc::partition_rpc::PullVpRefsReq { part_id };
        let payload = autumn_rpc::partition_rpc::rkyv_encode(&req);
        // 10 s — partition-side handler is a single `borrow()` over
        // sst_readers' vp_deps; bounded so a wedged PS doesn't make
        // merge/split hang indefinitely.
        let resp_bytes = self
            .conn_pool
            .call_timeout(
                part_addr,
                autumn_rpc::partition_rpc::MSG_PULL_VP_REFS,
                payload,
                Duration::from_secs(10),
            )
            .await
            .map_err(|e| {
                AppError::Internal(format!("F210-C4 pull_vp_refs RPC to {part_addr}: {e}"))
            })?;
        let resp: autumn_rpc::partition_rpc::PullVpRefsResp =
            autumn_rpc::partition_rpc::rkyv_decode(&resp_bytes).map_err(AppError::Internal)?;
        if resp.code != autumn_rpc::partition_rpc::CODE_OK {
            return Err(AppError::Precondition(format!(
                "F210-C4 pull_vp_refs from {part_addr}: {}",
                resp.message
            )));
        }
        // Synthesize a SyncPartitionVpRefsReq and feed it through the
        // existing handler. This re-uses all the F147-A refuse-at-start
        // checks, verify-BEFORE-mirror (F210-A2), and etcd txn logic.
        let sync_req = SyncPartitionVpRefsReq {
            part_id,
            refs: resp.refs,
        };
        let sync_payload = rkyv_encode(&sync_req);
        let sync_resp_bytes = self
            .handle_sync_partition_vp_refs(sync_payload)
            .await
            .map_err(|(_, msg)| AppError::Internal(msg))?;
        let sync_resp: SyncPartitionVpRefsResp =
            rkyv_decode(&sync_resp_bytes).map_err(AppError::Internal)?;
        if sync_resp.code != CODE_OK {
            return Err(AppError::Precondition(format!(
                "F210-C4 apply pulled vp_refs for part {part_id}: {}",
                sync_resp.message
            )));
        }
        Ok(())
    }

    // ── Serve ──────────────────────────────────────────────────────────

    pub async fn serve(&self, addr: SocketAddr) -> Result<()> {
        self.start_runtime_tasks();
        let mut listener = autumn_transport::current_or_init().bind(addr).await?;
        tracing::info!(addr = %addr, "manager listening");
        loop {
            let (conn, peer) = listener.accept().await?;
            if let Some(s) = conn.as_tcp() {
                if let Err(e) = s.set_nodelay(true) {
                    tracing::warn!(peer = %peer, error = %e, "set_nodelay failed");
                }
            }
            let mgr = self.clone();
            compio::runtime::spawn(async move {
                tracing::debug!(peer = %peer, "new manager rpc connection");
                if let Err(e) = Self::handle_connection(conn, mgr).await {
                    tracing::debug!(peer = %peer, error = %e, "manager rpc connection ended");
                }
            })
            .detach();
        }
    }

    async fn handle_connection(conn: autumn_transport::Conn, mgr: AutumnManager) -> Result<()> {
        let (mut reader, mut writer) = conn.into_split();
        let mut decoder = FrameDecoder::new();
        let mut buf = vec![0u8; 64 * 1024];

        loop {
            let BufResult(result, buf_back) = reader.read(buf).await;
            buf = buf_back;
            let n = result?;
            if n == 0 {
                return Ok(());
            }

            decoder.feed(&buf[..n]);

            loop {
                match decoder.try_decode().map_err(|e| anyhow::anyhow!(e))? {
                    Some(frame) if frame.req_id != 0 => {
                        let req_id = frame.req_id;
                        let msg_type = frame.msg_type;
                        let payload = frame.payload;
                        let resp_frame = match mgr.dispatch(msg_type, payload).await {
                            Ok(p) => Frame::response(req_id, msg_type, p),
                            Err((code, message)) => {
                                let p = autumn_rpc::RpcError::encode_status(code, &message);
                                Frame::error(req_id, msg_type, p)
                            }
                        };
                        let data = resp_frame.encode();
                        let BufResult(result, _) = writer.write_all(data).await;
                        result?;
                    }
                    Some(_) => continue,
                    None => break,
                }
            }
        }
    }

    async fn dispatch(&self, msg_type: u8, payload: Bytes) -> HandlerResult {
        match msg_type {
            MSG_STATUS => self.handle_status().await,
            MSG_ACQUIRE_OWNER_LOCK => self.handle_acquire_owner_lock(payload).await,
            MSG_REGISTER_NODE => self.handle_register_node(payload).await,
            MSG_CREATE_STREAM => self.handle_create_stream(payload).await,
            MSG_STREAM_INFO => self.handle_stream_info(payload).await,
            MSG_EXTENT_INFO => self.handle_extent_info(payload).await,
            MSG_NODES_INFO => self.handle_nodes_info().await,
            MSG_CHECK_COMMIT_LENGTH => self.handle_check_commit_length(payload).await,
            MSG_STREAM_ALLOC_EXTENT => self.handle_stream_alloc_extent(payload).await,
            MSG_STREAM_PUNCH_HOLES => self.handle_stream_punch_holes(payload).await,
            MSG_TRUNCATE => self.handle_truncate(payload).await,
            MSG_MULTI_MODIFY_SPLIT => self.handle_multi_modify_split(payload).await,
            MSG_MULTI_MODIFY_MERGE => self.handle_multi_modify_merge(payload).await,
            MSG_MERGE_PARTITIONS => self.handle_merge_partitions(payload).await,
            MSG_GET_POLICY_CANDIDATES => self.handle_get_policy_candidates(payload).await,
            MSG_REPORT_PARTITION_LOAD => self.handle_report_partition_load(payload).await,
            MSG_REPORT_DISK_FAILURE => self.handle_report_disk_failure(payload).await,
            MSG_REGISTER_PS => self.handle_register_ps(payload).await,
            MSG_UPSERT_PARTITION => self.handle_upsert_partition(payload).await,
            MSG_GET_REGIONS => self.handle_get_regions().await,
            MSG_HEARTBEAT_PS => self.handle_heartbeat_ps(payload).await,
            MSG_REGISTER_PARTITION_ADDR => self.handle_register_partition_addr(payload).await,
            MSG_SYNC_PARTITION_VP_REFS => self.handle_sync_partition_vp_refs(payload).await,
            MSG_RECONCILE_EXTENTS => self.handle_reconcile_extents(payload).await,
            MSG_UPDATE_STREAM_EC => self.handle_update_stream_ec(payload).await,
            MSG_FORCE_EC_CONVERT => self.handle_force_ec_convert(payload).await,
            MSG_GET_PARTITION_DETAIL => self.handle_get_partition_detail(payload).await,
            MSG_GET_POLICY_KIND_NAMES => self.handle_get_policy_kind_names(payload).await,
            // ── F211 operator-driven node lifecycle ──────────────────────
            MSG_LIST_NODE_STATES => self.handle_list_node_states(payload).await,
            MSG_EXTENT_HEALTH_REPORT => self.handle_extent_health_report(payload).await,
            MSG_LIST_EC_INFLIGHT_MARKERS => self.handle_list_ec_inflight_markers(payload).await,
            MSG_FENCE_NODE => self.handle_fence_node(payload).await,
            MSG_SET_NODE_MAINTENANCE => self.handle_set_node_maintenance(payload).await,
            MSG_CLEAR_NODE_OVERRIDE => self.handle_clear_node_override(payload).await,
            MSG_REMOVE_NODE => self.handle_remove_node(payload).await,
            MSG_RECOVERY_STATS => self.handle_recovery_stats(payload).await,
            MSG_QUERY_AUDIT_LOG => self.handle_query_audit_log(payload).await,
            MSG_GET_CLUSTER_ID => self.handle_get_cluster_id().await,
            // ── F-ioring-lease-1: inode-level lease + close-to-open ─────
            MSG_ACQUIRE_LEASE => self.handle_acquire_lease(payload).await,
            MSG_RELEASE_LEASE => self.handle_release_lease(payload).await,
            MSG_HEARTBEAT_LEASE => self.handle_heartbeat_lease(payload).await,
            MSG_POLL_INVALIDATIONS => self.handle_poll_invalidations(payload).await,
            _ => Err((
                StatusCode::InvalidArgument,
                format!("unknown msg_type {msg_type}"),
            )),
        }
    }

    // ── RPC handlers ───────────────────────────────────────────────────

    async fn handle_status(&self) -> HandlerResult {
        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    /// F214-A: read-only cluster identity. Servable from any replica
    /// (followers answer from replayed state); no leader gate. The only
    /// failure mode is "the manager has never run leader election yet
    /// against a fresh etcd" — surfaced as `CODE_UNAVAILABLE` so the
    /// caller (typically `autumn-op format`) knows to retry.
    async fn handle_get_cluster_id(&self) -> HandlerResult {
        let id = self.cluster_id.borrow().clone();
        if id.is_empty() {
            return Ok(rkyv_encode(&GetClusterIdResp {
                code: CODE_ERROR,
                message: "manager not yet bootstrapped".to_string(),
                cluster_id: String::new(),
            }));
        }
        Ok(rkyv_encode(&GetClusterIdResp {
            code: CODE_OK,
            message: String::new(),
            cluster_id: id,
        }))
    }

    pub(crate) async fn handle_acquire_owner_lock(&self, payload: Bytes) -> HandlerResult {
        let req: AcquireOwnerLockReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        match self.acquire_owner_revision(&req.owner_key).await {
            Ok(rev) => Ok(rkyv_encode(&AcquireOwnerLockResp {
                code: CODE_OK,
                message: String::new(),
                revision: rev,
            })),
            Err(err) => Ok(rkyv_encode(&AcquireOwnerLockResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                revision: 0,
            })),
        }
    }

    pub async fn handle_register_node(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&RegisterNodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                node_id: 0,
                disk_uuids: vec![],
            }));
        }

        let req: RegisterNodeReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // F211-C #2: zombie defense. Refuse re-registration when the
        // address is associated with a decommissioned node_id or a
        // currently-Fenced override. The operator must explicitly
        // `mgr_clear_node_override` before the node can come back.
        {
            let s = self.store.inner.borrow();
            let prior = s
                .nodes
                .values()
                .find(|n| n.address == req.addr)
                .map(|n| n.node_id);
            if let Some(pid) = prior {
                if self.decommissioned.borrow().contains_key(&pid) {
                    return Ok(rkyv_encode(&RegisterNodeResp {
                        code: CODE_PRECONDITION,
                        message: format!(
                            "address {} was previously decommissioned (node {}); operator must clear tombstone",
                            req.addr, pid
                        ),
                        node_id: 0,
                        disk_uuids: vec![],
                    }));
                }
                if let Some(o) = self.node_overrides.borrow().get(&pid) {
                    if o.kind == NODE_OVERRIDE_FENCED {
                        return Ok(rkyv_encode(&RegisterNodeResp {
                            code: CODE_PRECONDITION,
                            message: format!(
                                "node {} is Fenced; operator must clear override before re-registering",
                                pid
                            ),
                            node_id: 0,
                            disk_uuids: vec![],
                        }));
                    }
                }
            }
        }

        // Re-registration: if the address is already known, reuse the existing
        // node_id and disk_ids rather than rejecting. This allows extent nodes
        // to recover from a restart without requiring a full cluster wipe.
        let existing = {
            let s = self.store.inner.borrow();
            s.nodes.values().find(|n| n.address == req.addr).map(|n| {
                (
                    n.clone(),
                    n.disks
                        .iter()
                        .filter_map(|did| s.disks.get(did).cloned())
                        .collect::<Vec<_>>(),
                )
            })
        };

        if let Some((mut existing_node, existing_disks)) = existing {
            let node_id = existing_node.node_id;
            let uuid_map: Vec<(String, u64)> = req
                .disk_uuids
                .iter()
                .filter_map(|uuid| {
                    existing_disks
                        .iter()
                        .find(|d| &d.uuid == uuid)
                        .map(|d| (uuid.clone(), d.disk_id))
                })
                .collect();

            if uuid_map.is_empty() {
                return Ok(rkyv_encode(&RegisterNodeResp {
                    code: CODE_PRECONDITION,
                    message: format!(
                        "address {} already registered by node {} with different disks",
                        existing_node.address, node_id
                    ),
                    node_id: 0,
                    disk_uuids: vec![],
                }));
            }

            // Update shard_ports + control_address if the node restarted
            // with a different config.
            // F152: etcd-first ordering (CLAUDE.md note 1) — mirror to etcd
            // BEFORE updating in-memory store. The shard_ports change drives
            // route resolution; a crash mid-mirror leaves the new leader
            // routing to the OLD shard layout while the deposed leader's
            // memory had the new one.
            // F191: same applies to `control_address` — the manager's DF
            // probe uses it; updating in-memory before mirror could cause
            // the new leader to inherit a stale value on replay.
            if existing_node.shard_ports != req.shard_ports
                || existing_node.control_address != req.control_address
            {
                existing_node.shard_ports = req.shard_ports;
                existing_node.control_address = req.control_address;
                if let Err(err) = self.mirror_register_node(&existing_node, &[]).await {
                    return Ok(rkyv_encode(&RegisterNodeResp {
                        code: Self::err_to_code(&err),
                        message: err.to_string(),
                        node_id: 0,
                        disk_uuids: vec![],
                    }));
                }
                self.store
                    .inner
                    .borrow_mut()
                    .nodes
                    .insert(node_id, existing_node.clone());
            }

            // F211-A: re-registration counts as a heartbeat — flip
            // Suspected → Online so the operator-facing health report
            // reflects the recovery immediately, not on the next df tick.
            self.node_states.borrow_mut().on_heartbeat_ok(node_id);
            return Ok(rkyv_encode(&RegisterNodeResp {
                code: CODE_OK,
                message: String::new(),
                node_id,
                disk_uuids: uuid_map,
            }));
        }

        // F152: etcd-first ordering (CLAUDE.md note 1). Compute node +
        // disk_infos (and reserve their IDs via alloc_ids) under a single
        // borrow_mut, mirror to etcd, then apply to memory in a fresh
        // borrow_mut. alloc_ids is reserved upfront because IDs must be
        // monotonic across the whole cluster — wasted IDs from a failed
        // mirror are safe per note 5 (alloc_ids regeneration on replay
        // takes max(all_entity_ids)+1, so the gap is harmless).
        let (node, disk_infos, uuid_map, node_id) = {
            let mut s = self.store.inner.borrow_mut();
            let (start, _) = s.alloc_ids((req.disk_uuids.len() + 1) as u64);
            let node_id = start;

            let mut disk_ids = Vec::with_capacity(req.disk_uuids.len());
            let mut disk_infos = Vec::with_capacity(req.disk_uuids.len());
            let mut uuid_map = Vec::new();
            for (idx, uuid) in req.disk_uuids.iter().enumerate() {
                let disk_id = node_id + idx as u64 + 1;
                disk_ids.push(disk_id);
                let disk = MgrDiskInfo {
                    disk_id,
                    online: true,
                    uuid: uuid.clone(),
                };
                disk_infos.push(disk);
                uuid_map.push((uuid.clone(), disk_id));
            }

            let node = MgrNodeInfo {
                node_id,
                address: req.addr,
                disks: disk_ids,
                shard_ports: req.shard_ports,
                control_address: req.control_address,
            };
            (node, disk_infos, uuid_map, node_id)
        };

        if let Err(err) = self.mirror_register_node(&node, &disk_infos).await {
            return Ok(rkyv_encode(&RegisterNodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                node_id: 0,
                disk_uuids: vec![],
            }));
        }

        {
            let mut s = self.store.inner.borrow_mut();
            for disk in &disk_infos {
                s.disks.insert(disk.disk_id, disk.clone());
            }
            s.nodes.insert(node_id, node.clone());
        }
        // F214-B: first-time register seeds `Suspend` — a registered
        // but never-verified-alive state. The first successful df from
        // `disk_status_update_loop` transitions to `Online` via
        // `on_heartbeat_ok`. Pre-F214-B this seeded `Online` directly,
        // which created a 10-20 s ghost window where a registered-but-
        // not-yet-started EN was eligible for `select_nodes`.
        self.node_states.borrow_mut().on_register_first(node_id);

        Ok(rkyv_encode(&RegisterNodeResp {
            code: CODE_OK,
            message: String::new(),
            node_id,
            disk_uuids: uuid_map,
        }))
    }

    async fn handle_create_stream(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&CreateStreamResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
                extent: None,
            }));
        }

        let req: CreateStreamReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let ec_data = req.ec_data_shard;
        let ec_parity = req.ec_parity_shard;

        // Validate encoding:
        //   - Replication stream (ec_parity == 0): replicates == ec_data
        //     (K data nodes, no parity).
        //   - EC stream (ec_parity >= 1): K >= 2, M >= 1. `replicates`
        //     and `ec_data` are INDEPENDENT here. `replicates` is the
        //     open-extent replica count (typically 3), `ec_data` is the
        //     post-seal data-shard count (e.g. 4, 7), and `ec_parity`
        //     is the parity-shard count. The ec_conversion_dispatch_loop
        //     reads the sealed payload from any one of the open
        //     replicas, encodes into K+M shards, and allocates the
        //     extra `(K + M − replicates)` host slots needed.
        //
        //     Concretely: a 3-replica stream can be converted to 4+1
        //     EC (K=4 ≠ replicates=3) — `ec_conversion_dispatch_loop`
        //     allocates 5 − 3 = 2 extra host slots and writes 5 shards
        //     in total. This decouples the open-write topology from
        //     the storage-encoded topology.
        //
        //     Pre-fix EC streams required `replicates == K+M`, which
        //     pushed the open-extent allocation onto K+M nodes (each
        //     holding a full replica). The M extra replicas got
        //     overwritten with parity bytes on EC conversion anyway,
        //     so the up-front fanout was pure waste — and any
        //     seal/EC race had a wider blast radius across K+M nodes
        //     instead of just the K_open replicas.
        let total_replicas = req.replicates as usize;
        let err_msg: Option<String> = if total_replicas == 0 {
            Some("replicates must be >= 1".to_string())
        } else if ec_data == 0 {
            Some(
                "ec_data_shard must be >= 1 (use ec_data=N, ec_parity=0 for replica streams)"
                    .to_string(),
            )
        } else if ec_parity == 0 {
            // Replica path: ec_data must equal replicates exactly.
            if ec_data as usize != total_replicas {
                Some("ec_data_shard must equal replicates for a replica stream".to_string())
            } else {
                None
            }
        } else {
            // EC path: K >= 2, M >= 1. replicates and ec_data are
            // independent — open extents go on `replicates` nodes;
            // EC conversion expands to K+M total shards.
            if ec_data < 2 {
                Some("ec_data_shard >= 2 required for EC streams".to_string())
            } else {
                None
            }
        };
        if let Some(msg) = err_msg {
            let err = AppError::InvalidArgument(msg);
            return Ok(rkyv_encode(&CreateStreamResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
                extent: None,
            }));
        }

        // F214-B: capture the verified-online node set BEFORE borrowing
        // the store; select_nodes uses it as the primary allocation
        // filter so a freshly-registered (but not-yet-df'd) EN doesn't
        // get picked. Two separate borrows are fine — node_states is an
        // independent RefCell.
        let online_node_ids = self.node_states.borrow().online_node_ids();
        let (stream_id, extent_id, selected) = {
            let mut s = self.store.inner.borrow_mut();
            let selected =
                match Self::select_nodes(&s.nodes, &s.disks, &online_node_ids, total_replicas, &[])
                {
                    Ok(v) => v,
                    Err(err) => {
                        return Ok(rkyv_encode(&CreateStreamResp {
                            code: Self::err_to_code(&err),
                            message: err.to_string(),
                            stream: None,
                            extent: None,
                        }))
                    }
                };
            let (start, _) = s.alloc_ids(2);
            (start, start + 1, selected)
        };

        // F121/F190-style fallback walk: if a selected node refuses
        // alloc_extent (process dead, port closed, etc.), try another
        // node from the remaining pool. Pre-this, handle_create_stream
        // failed fast on the first replica's error, so a stream couldn't
        // be created when ANY one of the picked nodes was unreachable —
        // even though other healthy nodes existed. Mirrors the pattern
        // in handle_stream_alloc_extent above.
        let selected_ids: HashSet<u64> = selected.iter().map(|n| n.node_id).collect();
        let mut fallback_nodes: Vec<MgrNodeInfo> = {
            let s = self.store.inner.borrow();
            s.nodes
                .values()
                .filter(|n| !selected_ids.contains(&n.node_id))
                .cloned()
                .collect()
        };
        {
            use rand::seq::SliceRandom;
            fallback_nodes.shuffle(&mut rand::thread_rng());
        }
        let mut fallback_iter = fallback_nodes.into_iter();

        let mut node_ids = Vec::with_capacity(selected.len());
        let mut disk_ids = Vec::with_capacity(selected.len());
        for n in &selected {
            let mut candidate = n.clone();
            let (node_id, disk) = loop {
                match self
                    .alloc_extent_on_node(&candidate.address, extent_id)
                    .await
                {
                    Ok(disk) => break (candidate.node_id, disk),
                    Err(_) => match fallback_iter.next() {
                        Some(alt) => candidate = alt,
                        None => {
                            let err = AppError::Precondition(format!(
                                "no healthy node available to allocate extent {extent_id} for new stream"
                            ));
                            return Ok(rkyv_encode(&CreateStreamResp {
                                code: Self::err_to_code(&err),
                                message: err.to_string(),
                                stream: None,
                                extent: None,
                            }));
                        }
                    },
                }
            };
            node_ids.push(node_id);
            disk_ids.push(disk);
        }

        let stream = MgrStreamInfo {
            stream_id,
            extent_ids: vec![extent_id],
            ec_data_shard: ec_data,
            ec_parity_shard: ec_parity,
            replicates: req.replicates,
        };
        let extent = MgrExtentInfo {
            extent_id,
            replicates: node_ids,
            parity: vec![],
            eversion: 1,
            refs: 1,
            vp_table_refs: 0,
            sealed_length: 0,
            sealed: false,
            avali: 0,
            replicate_disks: disk_ids,
            parity_disks: vec![],
            ec_converted: false,
        };

        // F152: etcd-first ordering (CLAUDE.md note 1). Mirror to etcd
        // BEFORE applying to in-memory store. Pre-F152 the inserts at
        // s.streams / s.extents happened first; a manager crash between
        // memory-insert and etcd-write left the new leader (post-replay)
        // without the stream record while the extent files existed on
        // remote nodes as orphans. F125 fixed the same anti-pattern in
        // handle_stream_alloc_extent; this handler was missed.
        if let Err(err) = self.mirror_create_stream(&stream, &extent).await {
            return Ok(rkyv_encode(&CreateStreamResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
                extent: None,
            }));
        }

        {
            let mut s = self.store.inner.borrow_mut();
            s.streams.insert(stream_id, stream.clone());
            s.extents.insert(extent_id, extent.clone());
        }

        Ok(rkyv_encode(&CreateStreamResp {
            code: CODE_OK,
            message: String::new(),
            stream: Some(stream.clone()),
            extent: Some(extent.clone()),
        }))
    }

    async fn handle_update_stream_ec(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&UpdateStreamEcResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
            }));
        }

        let req: UpdateStreamEcReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        if req.ec_data_shard < 2 || req.ec_parity_shard == 0 {
            let err = AppError::InvalidArgument(
                "ec_data_shard >= 2 and ec_parity_shard >= 1 required".to_string(),
            );
            return Ok(rkyv_encode(&UpdateStreamEcResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
            }));
        }

        // F152: etcd-first ordering (CLAUDE.md note 1). Compute the new
        // stream snapshot under a read-only borrow, mirror to etcd, then
        // apply to memory. Pre-F152 the handler mutated the in-memory
        // ec_data_shard / ec_parity_shard before the etcd mirror, so a
        // crash between memory-mutate and etcd-write left the new leader
        // dispatching the OLD EC shape via ec_conversion_dispatch_loop
        // while the deposed leader thought it was already updated.
        let stream = {
            let s = self.store.inner.borrow();
            match s.streams.get(&req.stream_id) {
                Some(st) => {
                    let mut updated = st.clone();
                    updated.ec_data_shard = req.ec_data_shard;
                    updated.ec_parity_shard = req.ec_parity_shard;
                    updated
                }
                None => {
                    let err = AppError::NotFound(format!("stream {} not found", req.stream_id));
                    return Ok(rkyv_encode(&UpdateStreamEcResp {
                        code: Self::err_to_code(&err),
                        message: err.to_string(),
                        stream: None,
                    }));
                }
            }
        };

        if let Err(err) = self.mirror_stream_meta_update(&stream).await {
            return Ok(rkyv_encode(&UpdateStreamEcResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
            }));
        }

        {
            let mut s = self.store.inner.borrow_mut();
            // Apply to memory only after etcd persistence succeeds. If the
            // stream was concurrently removed (e.g. by a future delete RPC)
            // the get_mut returns None and we silently skip — the etcd
            // mirror already wrote the update; replay would resurrect it.
            // Today no delete-stream path exists so this is unreachable.
            if let Some(st) = s.streams.get_mut(&req.stream_id) {
                st.ec_data_shard = stream.ec_data_shard;
                st.ec_parity_shard = stream.ec_parity_shard;
            }
        }

        Ok(rkyv_encode(&UpdateStreamEcResp {
            code: CODE_OK,
            message: String::new(),
            stream: Some(stream),
        }))
    }

    async fn handle_stream_info(&self, payload: Bytes) -> HandlerResult {
        let req: StreamInfoReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let s = self.store.inner.borrow();

        let ids = if req.stream_ids.is_empty() {
            s.streams.keys().copied().collect::<Vec<_>>()
        } else {
            req.stream_ids
        };

        let mut streams = Vec::new();
        let mut extents = Vec::new();

        for id in ids {
            if let Some(st) = s.streams.get(&id) {
                streams.push((id, st.clone()));
                for extent_id in &st.extent_ids {
                    if let Some(e) = s.extents.get(extent_id) {
                        extents.push((*extent_id, e.clone()));
                    }
                }
            }
        }

        Ok(rkyv_encode(&StreamInfoResp {
            code: CODE_OK,
            message: String::new(),
            streams,
            extents,
        }))
    }

    async fn handle_extent_info(&self, payload: Bytes) -> HandlerResult {
        let req: ExtentInfoReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let s = self.store.inner.borrow();
        match s.extents.get(&req.extent_id) {
            Some(e) => Ok(rkyv_encode(&ExtentInfoResp {
                code: CODE_OK,
                message: String::new(),
                extent: Some(e.clone()),
            })),
            None => Ok(rkyv_encode(&ExtentInfoResp {
                code: CODE_NOT_FOUND,
                message: format!("extent {} not found", req.extent_id),
                extent: None,
            })),
        }
    }

    async fn handle_nodes_info(&self) -> HandlerResult {
        let s = self.store.inner.borrow();
        let nodes = s.nodes.iter().map(|(&id, n)| (id, n.clone())).collect();
        let disks_info = s.disks.iter().map(|(&id, d)| (id, d.clone())).collect();
        Ok(rkyv_encode(&NodesInfoResp {
            code: CODE_OK,
            message: String::new(),
            nodes,
            disks_info,
        }))
    }

    /// F227: nodes with an in-flight Recovery targeting `extent_id`.
    /// These are *catching-up* members — they hold only a partial replica
    /// while their slot is being rebuilt — so they MUST be excluded from
    /// any commit-length `min`. Including a catching-up replica's short
    /// length would crater the seal below the all-replica-ACK'd commit
    /// length and silently drop acked data. See the seal/commit sites for
    /// the full rationale.
    fn recovering_nodes_for_extent(&self, extent_id: u64) -> std::collections::HashSet<u64> {
        let mut set = std::collections::HashSet::new();
        for rec in self.inflight.borrow().values() {
            if let Some((_, crate::extent_inflight::ExtentOpPayload::Recovery(t))) = rec.unpack() {
                if t.extent_id == extent_id {
                    set.insert(t.replace_id);
                }
            }
        }
        set
    }

    /// F227: minimum number of committed (non-catching-up) members that
    /// must be reachable to seal / read a commit length. Default 1 — under
    /// all-replica-ACK any single committed member holds the full acked
    /// prefix, so 1 already prevents acked-data loss; raise for a stricter
    /// durability posture. This is a durability gate, NOT a quorum vote on
    /// the commit *position* (the position is always `min` over the
    /// committed members that respond).
    fn seal_durability_floor() -> usize {
        std::env::var("AUTUMN_MGR_SEAL_DURABILITY_FLOOR")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1)
            .max(1)
    }

    /// F227: pure WAS-faithful commit/seal-length decision (unit-tested,
    /// shared by `handle_stream_alloc_extent` seal + `handle_check_commit_length`).
    ///
    /// `members` = (slot_idx, node_id) over `replicates ++ parity` in slot
    /// order. `recovering` = catching-up node_ids (in-flight Recovery) to
    /// EXCLUDE. `responses` = node_id → reported commit_length for committed
    /// members that answered the probe.
    ///
    /// Returns `(commit_len, avali_bits)` where `commit_len` is the `min`
    /// over the **reachable** committed (non-catching-up) members.
    ///
    /// **WAS seal-over-reachable (the bug-#3 fix).** Earlier F227 required
    /// EVERY committed member to respond (`reachable == committed`), else
    /// `Err` — consistency-over-availability. But a node kill+restart leaves
    /// a committed member unreachable/behind that is NOT in `recovering`
    /// (recovery is fence-gated, F211), so the seal blocked forever → the
    /// write path wedged → reads starved (bug #3). WAS does NOT block on a
    /// slow/dead replica: the Stream Manager seals at the committed length
    /// over the REACHABLE members and re-replicates the laggard out of band.
    /// We now require only `floor` committed members to be reachable.
    ///
    /// **Why this never drops acked data:** the append path is
    /// all-replica-ACK, so the acked length is present on EVERY committed
    /// member (reachable or not). Each reachable committed member therefore
    /// holds ≥ the acked length, so `min` over the reachable ones is ALSO ≥
    /// the acked length. The ONLY member that can sit BELOW acked is a
    /// catching-up replica — and those are excluded via `recovering`. So
    /// `min`-over-reachable-committed ≥ acked, always. (`floor` ≥ 1
    /// guarantees at least one such member exists + responds, i.e. at least
    /// one full acked prefix survives the seal.) An unreachable committed
    /// member gets its `avali` bit left UNSET → the recovery/re_avali path
    /// reconciles it to `sealed_length` later (the laggard may hold MORE —
    /// un-acked speculation — which is then truncated; or LESS — which is
    /// re-replicated up). Either way acked data is safe.
    ///
    /// `Err` only when fewer than `floor` committed members exist OR fewer
    /// than `floor` of them responded (can't establish a durable seal point).
    pub(crate) fn compute_commit_seal(
        members: &[(usize, u64)],
        recovering: &std::collections::HashSet<u64>,
        responses: &std::collections::HashMap<u64, u32>,
        floor: usize,
    ) -> std::result::Result<(u32, u32), String> {
        let mut min_len: Option<u32> = None;
        let mut avali: u32 = 0;
        let mut committed = 0usize;
        let mut reachable = 0usize;
        for &(idx, node_id) in members {
            if recovering.contains(&node_id) {
                continue;
            }
            committed += 1;
            if let Some(&v) = responses.get(&node_id) {
                reachable += 1;
                avali |= 1u32 << idx;
                min_len = Some(min_len.map_or(v, |c| c.min(v)));
            }
        }
        // WAS seal-over-reachable: require `floor` committed members to exist
        // AND `floor` of them to respond — NOT all (which blocked on a
        // kill+restarted laggard, bug #3). Safe because min-over-reachable ≥
        // acked under all-replica-ACK (see doc).
        if committed < floor || reachable < floor {
            return Err(format!(
                "{reachable}/{committed} committed members reachable (need >= floor {floor})"
            ));
        }
        Ok((min_len.unwrap_or(0), avali))
    }

    pub(crate) async fn handle_check_commit_length(&self, payload: Bytes) -> HandlerResult {
        let req: CheckCommitLengthReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        let (stream, ex, nodes) = {
            let s = self.store.inner.borrow();
            if let Err(err) = Self::ensure_owner_revision(&req.owner_key, req.revision, &s) {
                return Ok(rkyv_encode(&CheckCommitLengthResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    stream_info: None,
                    end: 0,
                    last_ex_info: None,
                }));
            }

            let stream = match s.streams.get(&req.stream_id).cloned() {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&CheckCommitLengthResp {
                        code: CODE_NOT_FOUND,
                        message: format!("stream {}", req.stream_id),
                        stream_info: None,
                        end: 0,
                        last_ex_info: None,
                    }))
                }
            };
            let tail = match stream.extent_ids.last().copied() {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&CheckCommitLengthResp {
                        code: CODE_NOT_FOUND,
                        message: format!("tail extent in stream {}", req.stream_id),
                        stream_info: None,
                        end: 0,
                        last_ex_info: None,
                    }))
                }
            };
            let ex = match s.extents.get(&tail).cloned() {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&CheckCommitLengthResp {
                        code: CODE_NOT_FOUND,
                        message: format!("extent {tail}"),
                        stream_info: None,
                        end: 0,
                        last_ex_info: None,
                    }))
                }
            };
            (stream, ex, s.nodes.clone())
        };

        if ex.sealed {
            // Sealed extent (possibly empty: sealed_length may be 0) → its
            // committed length is fixed at sealed_length, no probe needed.
            return Ok(rkyv_encode(&CheckCommitLengthResp {
                code: CODE_OK,
                message: String::new(),
                stream_info: Some(stream.clone()),
                end: ex.sealed_length as u32,
                last_ex_info: Some(ex.clone()),
            }));
        }

        // F227: WAS-faithful commit-length read. The append path is
        // all-replica-ACK (`apply_completion` requires every replica to
        // ack), so every COMMITTED member holds >= the acked commit
        // length. Therefore `min` over the committed members never drops
        // acked data — PROVIDED we (a) exclude catching-up members
        // (in-flight Recovery, partial replica) from the min, and
        // (b) require all committed members to agree (no majority quorum
        // subset, which could seal below the acked length by including a
        // short catching-up replica, or above it by excluding a member).
        // F227: probe committed members, then decide via the shared pure
        // `compute_commit_seal` (no quorum; excludes catching-up members;
        // requires all committed members to respond).
        let recovering = self.recovering_nodes_for_extent(ex.extent_id);
        let members: Vec<(usize, u64)> = ex
            .replicates
            .iter()
            .copied()
            .chain(ex.parity.iter().copied())
            .enumerate()
            .collect();
        let mut responses: std::collections::HashMap<u64, u32> = std::collections::HashMap::new();
        for &(_, node_id) in &members {
            if recovering.contains(&node_id) {
                continue;
            }
            if let Some(n) = nodes.get(&node_id) {
                // F210-H3 Tier 2: pass `req.revision` (validated above) so
                // the EN's fence-handover side-effect fires on first probe.
                if let Ok(v) = self
                    .commit_length_on_node(&n.address, ex.extent_id, req.revision)
                    .await
                {
                    responses.insert(node_id, v);
                }
            }
        }
        let end = match Self::compute_commit_seal(
            &members,
            &recovering,
            &responses,
            Self::seal_durability_floor(),
        ) {
            Ok((len, _avali)) => len,
            Err(reason) => {
                let err = AppError::Precondition(format!(
                    "commit-length extent {}: {}",
                    ex.extent_id, reason
                ));
                return Ok(rkyv_encode(&CheckCommitLengthResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    stream_info: None,
                    end: 0,
                    last_ex_info: None,
                }));
            }
        };
        Ok(rkyv_encode(&CheckCommitLengthResp {
            code: CODE_OK,
            message: String::new(),
            stream_info: Some(stream.clone()),
            end,
            last_ex_info: Some(ex.clone()),
        }))
    }

    pub(crate) async fn handle_stream_alloc_extent(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&StreamAllocExtentResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream_info: None,
                last_ex_info: None,
            }));
        }

        let req: StreamAllocExtentReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // F214-B: capture the verified-online node set before borrowing
        // the store. See `handle_create_stream` for the same pattern.
        let online_node_ids = self.node_states.borrow().online_node_ids();
        let (mut tail, selected, extent_id, data, nodes_map) = {
            let mut s = self.store.inner.borrow_mut();
            if let Err(err) = Self::ensure_owner_revision(&req.owner_key, req.revision, &s) {
                return Ok(rkyv_encode(&StreamAllocExtentResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    stream_info: None,
                    last_ex_info: None,
                }));
            }

            let stream = match s.streams.get(&req.stream_id).cloned() {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: CODE_NOT_FOUND,
                        message: format!("stream {}", req.stream_id),
                        stream_info: None,
                        last_ex_info: None,
                    }))
                }
            };
            let tail_id = match stream.extent_ids.last().copied() {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: CODE_NOT_FOUND,
                        message: format!("tail extent in stream {}", req.stream_id),
                        stream_info: None,
                        last_ex_info: None,
                    }))
                }
            };
            let tail = match s.extents.get(&tail_id).cloned() {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: CODE_NOT_FOUND,
                        message: format!("extent {tail_id}"),
                        stream_info: None,
                        last_ex_info: None,
                    }))
                }
            };

            // F146: refuse-at-start. Symmetric to F138 (apply_recovery_done,
            // mark_extent_available, handle_multi_modify_split) and F145
            // (handle_stream_punch_holes, handle_truncate). Without these
            // guards, a concurrent EC conversion or recovery on the tail
            // extent would have its eversion+replicates writeback silently
            // overwritten by our verify-at-apply block below.
            // F207-C: collapse F138 (EC) + F139 (Recovery) refuse-at-start
            // checks into one ledger probe.
            //
            // SEED13-FIX (2026-05-29): only refuse when this alloc will
            // actually re-seal + re-write the tail — i.e. when the tail is
            // still OPEN (`sealed_length == 0`). When the tail is ALREADY
            // sealed, the seal block below is skipped and the apply path
            // (below) no longer writes the tail back to etcd / memory at all,
            // so a concurrent stream-layer op cannot be clobbered and the
            // guard is unnecessary. Every ledger op (Recovery / ConvertToEc /
            // Delete) acts ONLY on a sealed extent, so an in-flight op
            // implies the tail is sealed; gating here is what lifts the wedge
            // where a stuck Recovery on the sealed tail (no source replica for
            // 60s+) blocked new-extent allocation indefinitely, freezing the
            // write / flush / range paths even though the new extent lands on
            // entirely different, healthy nodes.
            if !tail.sealed {
                if let Some(op) = self.extent_inflight_op(tail_id) {
                    let msg = format!(
                        "extent {tail_id} has in-flight {op:?}; \
                         defer alloc_extent until it completes"
                    );
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: CODE_PRECONDITION,
                        message: msg,
                        stream_info: None,
                        last_ex_info: None,
                    }));
                }
            }

            // The new extent is allocated as an OPEN, REPLICATED extent
            // on `stream.replicates` nodes. For legacy streams persisted
            // before `replicates` was added to MgrStreamInfo (default
            // 0), fall back to `tail.replicates.len()`, which on a
            // pre-EC-converted tail equals the open replica count.
            let data = if stream.replicates > 0 {
                stream.replicates as usize
            } else {
                tail.replicates.len()
            };
            let selected = match Self::select_nodes(
                &s.nodes,
                &s.disks,
                &online_node_ids,
                data,
                &req.exclude_node_ids,
            ) {
                Ok(v) => v,
                Err(err) => {
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: Self::err_to_code(&err),
                        message: err.to_string(),
                        stream_info: None,
                        last_ex_info: None,
                    }))
                }
            };
            let (extent_id, _) = s.alloc_ids(1);
            (tail, selected, extent_id, data, s.nodes.clone())
        };

        // F146: capture the tail's eversion BEFORE any mutation so the
        // verify-at-apply block below can detect concurrent bumps.
        let expected_eversion = tail.eversion;

        // Seal old extent.
        //
        // **Idempotency / EC-corruption guard**: if the tail is already
        // sealed (some prior caller set `sealed_length > 0`), DO NOT
        // re-query commit_length and DO NOT overwrite sealed_length.
        //
        // Why: after EC conversion of a sealed extent, each replica's
        // local `entry.len` is rewritten to `shard_size` (the per-shard
        // payload size, ~ original_sealed_length / data_shards) by
        // `write_shard_local`. A naive re-seal would query
        // commit_length, get `shard_size` back from every replica,
        // take the min (= shard_size), and clobber the manager's
        // `tail.sealed_length` from `original_payload_len` down to
        // `shard_size`. Any VP at offset in `[shard_size,
        // original_payload_len)` would then suddenly be "past
        // sealed_length" — out-of-bounds on the read path even though
        // the underlying EC shards still encode the full original
        // payload. That triggered the production
        // `range start index N out of range for slice of length L`
        // panic in the partition server.
        //
        // A re-seal request typically arrives via the writer's
        // soft-error retry path: it observes that the cached tail was
        // sealed by another owner / split / EC dispatch and calls
        // `alloc_new_extent(stream_id, 0)` to obtain a fresh tail. We
        // honor the "allocate a new tail" intent while preserving the
        // existing seal point.
        let already_sealed = tail.sealed;

        // Assigned exactly once on every branch below (deferred init — no dead
        // default, no `mut` needed).
        let min_len: Option<u32>;
        let avali: u32;
        if already_sealed {
            // Preserve the existing seal — do not touch sealed_length,
            // eversion, or avali. The new-tail allocation below proceeds.
            min_len = Some(tail.sealed_length as u32);
            avali = tail.avali;
        } else if let Some(c) = req.seal_commit {
            // AUTHORITATIVE: the writer supplied its OWN all-replica-acked
            // commit on this tail (captured at a quiesced point via the
            // SealCommit handshake), or a known exact end (preemptive roll).
            // Seal at EXACTLY `c` and do NOT probe — even when `c == 0` (a tail
            // where nothing was ever all-acked → sealed empty). Under
            // all-replica-ACK every committed member holds >= the writer's
            // commit, so sealing there never drops acked data; and because we
            // do not probe, a speculative/un-acked byte that only one
            // (soon-dead) reachable member holds is NEVER promoted into
            // sealed_length — the root fix for the F227 phantom seal (seed=13
            // Mode A). The probe path below (`None`) is reserved for genuine
            // new-owner takeover, where the writer has no commit cursor.
            min_len = Some(c);
            avali = Self::all_bits(tail.replicates.len() + tail.parity.len());
        } else {
            // PROBE (`req.seal_commit == None`): WAS-faithful failover seal (the
            // writer did not supply a known commit, so this owner must derive it).
            // commit length = `min` over COMMITTED members only. The append
            // path is all-replica-ACK, so every committed member holds >=
            // the acked length; min over them is therefore >= acked and
            // never drops acked data — as long as catching-up members are
            // excluded and all committed members agree (no quorum subset).
            //
            // Pre-F227 this took `min` over a majority-quorum subset of
            // responders: a catching-up replica (partial data from an
            // in-flight recovery) included in the min cratered
            // sealed_length below the acked length (silent data loss); a
            // leading-only subset could also seal above the true commit
            // (keeping un-acked data).
            let recovering = self.recovering_nodes_for_extent(tail.extent_id);
            let members: Vec<(usize, u64)> = tail
                .replicates
                .iter()
                .copied()
                .chain(tail.parity.iter().copied())
                .enumerate()
                .collect();
            let mut responses: std::collections::HashMap<u64, u32> =
                std::collections::HashMap::new();
            for &(_, node_id) in &members {
                if recovering.contains(&node_id) {
                    continue;
                }
                if let Some(node) = nodes_map.get(&node_id) {
                    // F210-H3 Tier 2: pass req.revision (validated above)
                    // so the EN's fence-handover side-effect fires.
                    if let Ok(v) = self
                        .commit_length_on_node(&node.address, tail.extent_id, req.revision)
                        .await
                    {
                        responses.insert(node_id, v);
                    }
                }
            }
            // BUG2 trace (opt-in): the per-member commit_length probe results
            // that feed the seal min. A `responses` map of all-zero (or empty)
            // while the extent holds acked data pins the under-seal to the
            // probe path (vs the authoritative SealCommit path).
            tracing::info!(
                target: "bug2_trace",
                extent_id = tail.extent_id,
                stream_id = req.stream_id,
                ?responses,
                recovering = ?recovering,
                "BUG2 probe commit_length responses"
            );
            // Shared pure decision: no quorum, exclude catching-up members,
            // seal at min over the REACHABLE committed members (>= floor;
            // WAS seal-over-reachable — a kill+restarted laggard no longer
            // blocks). apply_recovery_done / re_avali set an unset slot's
            // avali bit when its reconcile to sealed_length completes.
            match Self::compute_commit_seal(
                &members,
                &recovering,
                &responses,
                Self::seal_durability_floor(),
            ) {
                Ok((len, av)) => {
                    min_len = Some(len);
                    avali = av;
                }
                Err(reason) => {
                    let err = AppError::Precondition(format!(
                        "seal extent {}: {}",
                        tail.extent_id, reason
                    ));
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: Self::err_to_code(&err),
                        message: err.to_string(),
                        stream_info: None,
                        last_ex_info: None,
                    }));
                }
            }
        }

        let sealed_len = match min_len {
            Some(v) => v,
            None => {
                let err = AppError::Precondition(format!(
                    "no available commit length for extent {}",
                    tail.extent_id
                ));
                return Ok(rkyv_encode(&StreamAllocExtentResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    stream_info: None,
                    last_ex_info: None,
                }));
            }
        };
        if !already_sealed {
            tail.sealed = true;
            tail.sealed_length = sealed_len as u64;
            tail.eversion += 1;
            tail.avali = avali;
        }
        // BUG2 trace (opt-in, target `bug2_trace`): the decisive event. A
        // `sealed_len == 0` here on a tail that physically held VP/SST-acked
        // data at offset > 0 is the under-seal that makes a split child
        // un-openable (`stale_vp_offset_past_sealed_length`). `seal_path`
        // distinguishes the three causes: an `authoritative_seal_commit` of 0
        // means the writer's SealCommit handshake returned a stale/reset
        // worker's `state.commit=0`; a `probe`-path 0 means every reachable
        // committed member reported commit_length 0 at seal time.
        let seal_path = if already_sealed {
            "already_sealed"
        } else if req.seal_commit.is_some() {
            "authoritative_seal_commit"
        } else {
            "probe"
        };
        tracing::info!(
            target: "bug2_trace",
            extent_id = tail.extent_id,
            stream_id = req.stream_id,
            seal_commit = ?req.seal_commit,
            seal_path,
            sealed_len,
            eversion_old = expected_eversion,
            eversion_new = tail.eversion,
            revision = req.revision,
            owner = %req.owner_key,
            "BUG2 alloc-seal applied"
        );
        // Suppress unused warning when `already_sealed` skips the real seal.
        let _ = sealed_len;

        // Allocate new extent on nodes with fallback
        let mut node_ids = Vec::with_capacity(selected.len());
        let mut disk_ids = Vec::with_capacity(selected.len());
        let selected_ids: HashSet<u64> = selected.iter().map(|n| n.node_id).collect();
        // F190: prefer fallbacks not in the writer's recent-failure set; fall
        // back to the unfiltered set if the exclusion would empty the iter.
        let exclude_set: HashSet<u64> = req.exclude_node_ids.iter().copied().collect();
        let unfiltered: Vec<MgrNodeInfo> = nodes_map
            .values()
            .filter(|n| !selected_ids.contains(&n.node_id))
            .cloned()
            .collect();
        let after_exclude: Vec<MgrNodeInfo> = unfiltered
            .iter()
            .filter(|n| !exclude_set.contains(&n.node_id))
            .cloned()
            .collect();
        let mut fallback_nodes = if after_exclude.is_empty() {
            unfiltered
        } else {
            after_exclude
        };
        // F144: walk fallbacks in random order — ID-sorted order
        // re-introduces the same low-ID bias that `select_nodes` was
        // changed to avoid.
        {
            use rand::seq::SliceRandom;
            fallback_nodes.shuffle(&mut rand::thread_rng());
        }
        let mut fallback_iter = fallback_nodes.into_iter();

        for n in &selected {
            let mut candidate = n.clone();
            let (node_id, disk) = loop {
                match self
                    .alloc_extent_on_node(&candidate.address, extent_id)
                    .await
                {
                    Ok(disk) => break (candidate.node_id, disk),
                    Err(_) => match fallback_iter.next() {
                        Some(alt) => candidate = alt,
                        None => {
                            let err = AppError::Precondition(format!(
                                "no healthy node available to allocate extent {extent_id}"
                            ));
                            return Ok(rkyv_encode(&StreamAllocExtentResp {
                                code: Self::err_to_code(&err),
                                message: err.to_string(),
                                stream_info: None,
                                last_ex_info: None,
                            }));
                        }
                    },
                }
            };
            node_ids.push(node_id);
            disk_ids.push(disk);
        }

        let new_extent = MgrExtentInfo {
            extent_id,
            replicates: node_ids[..data].to_vec(),
            parity: node_ids[data..].to_vec(),
            eversion: 1,
            refs: 1,
            vp_table_refs: 0,
            sealed_length: 0,
            sealed: false,
            avali: 0,
            replicate_disks: disk_ids[..data].to_vec(),
            parity_disks: disk_ids[data..].to_vec(),
            ec_converted: false,
        };

        // F125: compute stream_after without modifying store, mirror to
        // etcd FIRST, then apply to in-memory state on success.
        let (stream_after, alloc_stream_baseline) = {
            let s = self.store.inner.borrow();
            let st = match s.streams.get(&req.stream_id) {
                Some(v) => v,
                None => {
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: CODE_NOT_FOUND,
                        message: format!("stream {}", req.stream_id),
                        stream_info: None,
                        last_ex_info: None,
                    }))
                }
            };
            // Item 3: CAS baseline = the stream's current value (etcd holds
            // exactly this until a concurrent op commits). The mirror txn below
            // value-CAS's `streams/<id>` against it, so a punch_holes/truncate
            // committing during our RTT makes our write fail → retry, instead of
            // resurrecting the removed extent.
            let baseline = rkyv_encode(st).to_vec();
            let mut stream_after = st.clone();
            stream_after.extent_ids.push(extent_id);
            (stream_after, baseline)
        };

        // F210-A2 verify-BEFORE-mirror (replaces the pre-F210-A2 F146
        // verify-AFTER-mirror form). If a concurrent mutator
        // (recovery_done, ec_conversion_done, punch_holes, truncate,
        // split) bumped `tail.eversion` during our commit_length /
        // alloc_extent_on_node await window above, the etcd write we
        // would otherwise make is stale relative to live memory.
        //
        // Pre-F210-A2 the check ran AFTER the etcd mirror — when verify
        // failed, the client got `Precondition` but etcd had already
        // durable-committed the stale write. Failover replay then
        // re-loaded the stale write as if successful, while the client
        // believed the call failed. Linearization point unexplainable.
        //
        // Verify-BEFORE keeps both etcd and in-memory untouched on the
        // failure path. A narrow residual window remains (concurrent
        // mutation during the etcd mirror RTT itself); fully closing it
        // requires acquiring an exclusive ledger marker for the
        // alloc_extent op, which is filed as F210-A1-followup (PS-layer
        // ops currently don't enroll in the F207 ledger by design).
        //
        // coco P1 — stream-membership baseline verify (runs for BOTH paths).
        // The etcd mirror + in-memory apply below write `stream_after`
        // (= the live stream's `extent_ids` captured at build time, plus our
        // new extent). If a concurrent `punch_holes` / `truncate` / `split`
        // changed this stream's `extent_ids` during our alloc / mirror await
        // window, overwriting with `stream_after` would resurrect a removed
        // extent or roll back the membership change. Refuse (Precondition) when
        // the live stream no longer matches the baseline we built from — the
        // client retries with a fresh snapshot. Membership is independent of
        // the tail seal, so this guard applies whether or not `already_sealed`.
        // (A narrow residual remains for a mutation landing during the etcd
        // mirror RTT itself — the same F210-A1-followup window the eversion
        // verify below documents.)
        {
            let s = self.store.inner.borrow();
            match s.streams.get(&req.stream_id) {
                Some(live) => {
                    let baseline = &stream_after.extent_ids[..stream_after.extent_ids.len() - 1];
                    if live.extent_ids.as_slice() != baseline {
                        let msg = format!(
                            "stream {} membership changed during alloc_extent; \
                             retry with fresh snapshot",
                            req.stream_id
                        );
                        return Ok(rkyv_encode(&StreamAllocExtentResp {
                            code: CODE_PRECONDITION,
                            message: msg,
                            stream_info: None,
                            last_ex_info: None,
                        }));
                    }
                }
                None => {
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: CODE_NOT_FOUND,
                        message: format!("stream {}", req.stream_id),
                        stream_info: None,
                        last_ex_info: None,
                    }));
                }
            }
        }

        // SEED13-FIX: the eversion verify (and the tail writeback below) are
        // ONLY relevant when this alloc re-seals + re-writes the tail
        // (`!already_sealed`). When the tail is already sealed we do not
        // touch the tail at all — the sealer already persisted it and a
        // concurrent Recovery / ConvertToEc owns its own writeback — so a
        // tail-eversion bump during our await window is none of our business
        // and must not abort the new-extent allocation (that abort, paired
        // with a stuck recovery holding the inflight marker, was the wedge).
        if !already_sealed {
            let s = self.store.inner.borrow();
            let live_eversion = match s.extents.get(&tail.extent_id) {
                Some(ex) => ex.eversion,
                None => {
                    let msg = format!("extent {} was deleted during alloc_extent", tail.extent_id);
                    return Ok(rkyv_encode(&StreamAllocExtentResp {
                        code: CODE_PRECONDITION,
                        message: msg,
                        stream_info: None,
                        last_ex_info: None,
                    }));
                }
            };
            if live_eversion != expected_eversion {
                let msg = format!(
                    "extent {} eversion changed during alloc_extent \
                     ({} -> {}); retry with fresh snapshot",
                    tail.extent_id, expected_eversion, live_eversion
                );
                return Ok(rkyv_encode(&StreamAllocExtentResp {
                    code: CODE_PRECONDITION,
                    message: msg,
                    stream_info: None,
                    last_ex_info: None,
                }));
            }
        }

        // SEED13-FIX: pass the tail to the etcd mirror ONLY when we actually
        // changed it (`!already_sealed`). An already-sealed tail is left
        // untouched so a concurrent Recovery's `replicates` / `eversion`
        // writeback (which can land during the mirror RTT) is never
        // clobbered by our stale early snapshot.
        let sealed_old = if already_sealed { None } else { Some(&tail) };
        if let Err(err) = self
            .mirror_stream_alloc_extent(
                &stream_after,
                sealed_old,
                &new_extent,
                Some(alloc_stream_baseline),
            )
            .await
        {
            return Ok(rkyv_encode(&StreamAllocExtentResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream_info: None,
                last_ex_info: None,
            }));
        }

        {
            let mut s = self.store.inner.borrow_mut();
            if let Some(st) = s.streams.get_mut(&req.stream_id) {
                *st = stream_after.clone();
            }
            // Mirror the etcd decision: only re-insert the tail when we
            // re-sealed it. An already-sealed tail's in-memory entry may have
            // been advanced by a concurrent Recovery — leave it as the live
            // store has it.
            if !already_sealed {
                s.extents.insert(tail.extent_id, tail.clone());
            }
            s.extents.insert(extent_id, new_extent.clone());
        }

        Ok(rkyv_encode(&StreamAllocExtentResp {
            code: CODE_OK,
            message: String::new(),
            stream_info: Some(stream_after.clone()),
            last_ex_info: Some(new_extent.clone()),
        }))
    }

    pub(crate) async fn handle_stream_punch_holes(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&PunchHolesResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
            }));
        }

        let req: PunchHolesReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // F207-C: snapshot the ConvertToEc + Recovery sets from the
        // unified ledger once. Pre-F207 these were `ec_inflight` Ref into
        // the deleted HashSet + `recovery_inflight` Ref into the deleted
        // HashMap. Single-threaded compio — snapshot-then-consult preserves
        // semantics.
        let (ec_inflight_set, recovery_inflight_set) = self.inflight_snapshot_ec_recovery();

        // F210-A1 etcd-first refactor (was: mutate-store then mirror-etcd
        // then enqueue). The pre-F210-A1 form computed mutations inside a
        // borrow_mut block — mirror failure (NotLeader / etcd transient)
        // left in-memory state advanced while etcd was unchanged. note 1's
        // step 1 says "compute mutations without modifying store". The
        // closure below now does so by working on clones.
        let out = {
            let guard = self.store.inner.borrow();
            let s: &autumn_common::MetadataState = &guard;
            (|| -> Result<
                (
                    MgrStreamInfo,
                    Vec<MgrExtentInfo>,
                    Vec<u64>,
                    Vec<PendingDelete>,
                    // Item 3: CAS baseline = the stream's value BEFORE this
                    // punch (etcd currently holds it). The mirror value-CAS's
                    // `streams/<id>` against it.
                    Vec<u8>,
                ),
                AppError,
            > {
                Self::ensure_owner_revision(&req.owner_key, req.revision, s)?;
                let requested: HashSet<u64> = req.extent_ids.into_iter().collect();
                let stream = s
                    .streams
                    .get(&req.stream_id)
                    .ok_or_else(|| AppError::NotFound(format!("stream {}", req.stream_id)))?
                    .clone();
                let stream_baseline = rkyv_encode(&stream).to_vec();

                // F126: only operate on extents that actually belong to this
                // stream. Without this, a malformed request could decrement
                // refs on unrelated streams' extents.
                let members: HashSet<u64> = stream.extent_ids.iter().copied().collect();
                let removed: HashSet<u64> = requested
                    .into_iter()
                    .filter(|id| members.contains(id))
                    .collect();

                // F139 / F207-C: if any extent that would drop to refs=0
                // is currently being recovered, refuse the entire call.
                for eid in &removed {
                    if recovery_inflight_set.contains(eid) {
                        if let Some(ex) = s.extents.get(eid) {
                            if ex.refs == 1 && ex.vp_table_refs == 0 {
                                return Err(AppError::Precondition(format!(
                                    "extent {eid} has in-flight recovery; \
                                     defer punch_holes until recovery completes"
                                )));
                            }
                        }
                    }
                }
                // F145 / F207-B: refuse if any to-be-removed extent is mid-EC.
                for eid in &removed {
                    if ec_inflight_set.contains(eid) {
                        return Err(AppError::Precondition(format!(
                            "extent {eid} has in-flight EC conversion; \
                             defer punch_holes until conversion completes"
                        )));
                    }
                }

                let mut updated = stream;
                updated.extent_ids.retain(|id| !removed.contains(id));
                if updated.extent_ids.is_empty() {
                    return Err(AppError::Precondition(
                        "stream cannot be empty after punch holes".to_string(),
                    ));
                }
                let mut extent_puts = Vec::new();
                let mut extent_deletes = Vec::new();
                let mut pending_deletes = Vec::new();

                // F109: build pending_deletes snapshot for extents that
                // would physically delete (refs would hit 0 and not EC-inflight).
                for &eid in &removed {
                    if let Some(extent) = s.extents.get(&eid) {
                        if extent.refs == 1
                            && extent.vp_table_refs == 0
                            && !ec_inflight_set.contains(&eid)
                        {
                            let pending_addrs =
                                Self::snapshot_replica_addrs(&s.nodes, eid, extent);
                            pending_deletes.push(PendingDelete {
                                extent_id: eid,
                                pending_addrs,
                                attempts: 0,
                            });
                        }
                    }
                }

                for extent_id in &removed {
                    if let Some(extent) = s.extents.get(extent_id) {
                        let mut new_ext = extent.clone();
                        if new_ext.refs <= 1 {
                            new_ext.refs = 0;
                            if Self::extent_can_delete(&new_ext)
                                && !ec_inflight_set.contains(extent_id)
                            {
                                extent_deletes.push(*extent_id);
                            } else {
                                new_ext.eversion += 1;
                                extent_puts.push(new_ext);
                            }
                        } else {
                            new_ext.refs -= 1;
                            new_ext.eversion += 1;
                            extent_puts.push(new_ext);
                        }
                    }
                }
                Ok((
                    updated,
                    extent_puts,
                    extent_deletes,
                    pending_deletes,
                    stream_baseline,
                ))
            })()
        };

        match out {
            Ok((stream, extent_puts, extent_deletes, pending_deletes, stream_baseline)) => {
                // Step 2: persist to etcd FIRST. Failure → in-memory zero
                // changes (the closure above produced clones only).
                if let Err(err) = self
                    .mirror_stream_extent_mutation(
                        &stream,
                        &extent_puts,
                        &extent_deletes,
                        Some(stream_baseline),
                    )
                    .await
                {
                    return Ok(rkyv_encode(&PunchHolesResp {
                        code: Self::err_to_code(&err),
                        message: err.to_string(),
                        stream: None,
                    }));
                }
                // Step 3: apply pre-computed mutations to in-memory store.
                // Etcd is authoritative; this just brings the cache forward.
                {
                    let mut s = self.store.inner.borrow_mut();
                    if let Some(st) = s.streams.get_mut(&req.stream_id) {
                        *st = stream.clone();
                    }
                    for ex in &extent_puts {
                        s.extents.insert(ex.extent_id, ex.clone());
                    }
                    for &eid in &extent_deletes {
                        s.extents.remove(&eid);
                    }
                }
                // F207-C: each enqueue is now an etcd CAS via the unified
                // ledger; errors are downgraded inside enqueue (with WARN
                // logging) so a single failed acquire doesn't fail the
                // whole punch_holes call.
                let _ = self.enqueue_pending_deletes(pending_deletes).await;
                Ok(rkyv_encode(&PunchHolesResp {
                    code: CODE_OK,
                    message: String::new(),
                    stream: Some(stream.clone()),
                }))
            }
            Err(err) => Ok(rkyv_encode(&PunchHolesResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
            })),
        }
    }

    pub(crate) async fn handle_truncate(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&TruncateResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                updated_stream_info: None,
            }));
        }

        let req: TruncateReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // F207-C: snapshot ConvertToEc + Recovery inflight sets.
        let (ec_inflight_set, recovery_inflight_set) = self.inflight_snapshot_ec_recovery();

        // F210-A1 etcd-first refactor (same shape as handle_stream_punch_holes).
        let out = {
            let guard = self.store.inner.borrow();
            let s: &autumn_common::MetadataState = &guard;
            (|| -> Result<
                (
                    MgrStreamInfo,
                    Vec<MgrExtentInfo>,
                    Vec<u64>,
                    Vec<PendingDelete>,
                    // Item 3: CAS baseline (stream value before this truncate).
                    Vec<u8>,
                ),
                AppError,
            > {
                Self::ensure_owner_revision(&req.owner_key, req.revision, s)?;
                let stream = s
                    .streams
                    .get(&req.stream_id)
                    .cloned()
                    .ok_or_else(|| AppError::NotFound(format!("stream {}", req.stream_id)))?;
                let stream_baseline = rkyv_encode(&stream).to_vec();

                let pos = stream
                    .extent_ids
                    .iter()
                    .position(|id| *id == req.extent_id)
                    .ok_or_else(|| {
                        AppError::NotFound(format!("extent {} in stream", req.extent_id))
                    })?;

                if pos == 0 {
                    return Err(AppError::Precondition(
                        "truncate target is first extent, nothing to truncate".to_string(),
                    ));
                }

                let removed: HashSet<u64> = stream.extent_ids[..pos].iter().copied().collect();

                // F139 / F207-C: refuse if any to-be-removed extent is
                // mid-recovery.
                for eid in &removed {
                    if recovery_inflight_set.contains(eid) {
                        if let Some(ex) = s.extents.get(eid) {
                            if ex.refs == 1 && ex.vp_table_refs == 0 {
                                return Err(AppError::Precondition(format!(
                                    "extent {eid} has in-flight recovery; \
                                     defer truncate until recovery completes"
                                )));
                            }
                        }
                    }
                }
                // F145 / F207-B: refuse if any to-be-truncated extent is mid-EC.
                for eid in &removed {
                    if ec_inflight_set.contains(eid) {
                        return Err(AppError::Precondition(format!(
                            "extent {eid} has in-flight EC conversion; \
                             defer truncate until conversion completes"
                        )));
                    }
                }

                let mut updated = stream;
                updated.extent_ids.retain(|id| !removed.contains(id));
                let mut extent_puts = Vec::new();
                let mut extent_deletes = Vec::new();
                let mut pending_deletes = Vec::new();

                // F109: build pending_deletes for extents that physically delete.
                for &eid in &removed {
                    if let Some(extent) = s.extents.get(&eid) {
                        if extent.refs == 1
                            && extent.vp_table_refs == 0
                            && !ec_inflight_set.contains(&eid)
                        {
                            let pending_addrs =
                                Self::snapshot_replica_addrs(&s.nodes, eid, extent);
                            pending_deletes.push(PendingDelete {
                                extent_id: eid,
                                pending_addrs,
                                attempts: 0,
                            });
                        }
                    }
                }

                for extent_id in &removed {
                    if let Some(extent) = s.extents.get(extent_id) {
                        let mut new_ext = extent.clone();
                        if new_ext.refs <= 1 {
                            new_ext.refs = 0;
                            if Self::extent_can_delete(&new_ext)
                                && !ec_inflight_set.contains(extent_id)
                            {
                                extent_deletes.push(*extent_id);
                            } else {
                                new_ext.eversion += 1;
                                extent_puts.push(new_ext);
                            }
                        } else {
                            new_ext.refs -= 1;
                            new_ext.eversion += 1;
                            extent_puts.push(new_ext);
                        }
                    }
                }
                Ok((
                    updated,
                    extent_puts,
                    extent_deletes,
                    pending_deletes,
                    stream_baseline,
                ))
            })()
        };

        match out {
            Ok((stream, extent_puts, extent_deletes, pending_deletes, stream_baseline)) => {
                if let Err(err) = self
                    .mirror_stream_extent_mutation(
                        &stream,
                        &extent_puts,
                        &extent_deletes,
                        Some(stream_baseline),
                    )
                    .await
                {
                    return Ok(rkyv_encode(&TruncateResp {
                        code: Self::err_to_code(&err),
                        message: err.to_string(),
                        updated_stream_info: None,
                    }));
                }
                // Step 3: apply pre-computed mutations to in-memory store.
                {
                    let mut s = self.store.inner.borrow_mut();
                    if let Some(st) = s.streams.get_mut(&req.stream_id) {
                        *st = stream.clone();
                    }
                    for ex in &extent_puts {
                        s.extents.insert(ex.extent_id, ex.clone());
                    }
                    for &eid in &extent_deletes {
                        s.extents.remove(&eid);
                    }
                }
                let _ = self.enqueue_pending_deletes(pending_deletes).await;
                Ok(rkyv_encode(&TruncateResp {
                    code: CODE_OK,
                    message: String::new(),
                    updated_stream_info: Some(stream.clone()),
                }))
            }
            Err(err) => Ok(rkyv_encode(&TruncateResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                updated_stream_info: None,
            })),
        }
    }

    pub(crate) async fn handle_multi_modify_split(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }

        let req: MultiModifySplitReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // F210-C4: pull-sync vp_refs from the source partition's PS
        // BEFORE the atomic etcd txn below. Pre-F210-C4 the txn used
        // the cached `partition_vp_refs[req.part_id]` snapshot, which
        // could be stale if a previous PS sync_partition_vp_refs
        // failed. A stale snapshot under-counts `vp_table_refs` on
        // extents referenced by SSTs that were published since the
        // last successful sync — `apply_split_mutations` would split
        // those into left/right children with a wrong count, and a
        // subsequent `extent_can_delete` check could approve deletion
        // of an extent whose live VPs are still in some SST. By
        // pulling here we refresh the manager's view to the
        // authoritative PS-side state before committing.
        let part_addr = {
            let s = self.store.inner.borrow();
            s.part_addrs.get(&req.part_id).cloned()
        };
        if let Some(addr) = part_addr {
            if let Err(e) = self.pull_and_apply_vp_refs(req.part_id, &addr).await {
                return Ok(rkyv_encode(&CodeResp {
                    code: Self::err_to_code(&e),
                    message: format!("F210-C4 pull_vp_refs pre-split: {e}"),
                }));
            }
        }
        // If part_addr is unknown (PS hasn't registered yet), skip the
        // pull — split would fail later for other reasons (no PS to
        // serve the split children either way).

        // Phase 1: Compute all mutations without modifying store
        // (only alloc_ids touches state.next_id, which is safe to waste on failure)
        let out = {
            let mut s = self.store.inner.borrow_mut();
            (|| -> Result<(
                Vec<MgrStreamInfo>,
                Vec<MgrExtentInfo>,
                MgrPartitionMeta,
                MgrPartitionMeta,
                MgrPartitionVpRefs,
                HashMap<u64, u64>,
            ), AppError> {
                Self::ensure_owner_revision(&req.owner_key, req.revision, &s)?;

                let src_meta = s
                    .partitions
                    .get(&req.part_id)
                    .cloned()
                    .ok_or_else(|| AppError::NotFound(format!("part {}", req.part_id)))?;

                let rg = src_meta
                    .rg
                    .clone()
                    .ok_or_else(|| AppError::Internal("partition range missing".to_string()))?;

                let in_range = req.mid_key >= rg.start_key
                    && (rg.end_key.is_empty() || req.mid_key < rg.end_key);
                if !in_range {
                    return Err(AppError::Precondition(
                        "mid_key is not in partition range".to_string(),
                    ));
                }

                // F138 / F207-B: reject split if any source-stream extent
                // is undergoing EC conversion. compute_duplicate_stream
                // bumps eversion on the source extents; if
                // apply_ec_conversion_done runs concurrently it would
                // overwrite those bumps. Fail fast — client retries with
                // backoff. F207-B: reads the unified ledger via
                // `extent_inflight_op`.
                {
                    for &sid in &[src_meta.log_stream, src_meta.row_stream, src_meta.meta_stream] {
                        if let Some(stream) = s.streams.get(&sid) {
                            for &eid in &stream.extent_ids {
                                if matches!(
                                    self.extent_inflight_op(eid),
                                    Some(crate::extent_inflight::ExtentOpKind::ConvertToEc)
                                ) {
                                    return Err(AppError::Precondition(format!(
                                        "ec conversion in flight on extent {eid}; retry split"
                                    )));
                                }
                            }
                        }
                    }
                }
                // F146: symmetric guard against in-flight recovery on any
                // source-stream extent. apply_recovery_done bumps eversion and
                // rewrites replicates; Phase-3's apply_split_mutations would
                // overwrite both with the Phase-1 captured snapshot.
                // F146 / F207-C: read Recovery from the unified ledger.
                {
                    for &sid in &[src_meta.log_stream, src_meta.row_stream, src_meta.meta_stream] {
                        if let Some(stream) = s.streams.get(&sid) {
                            for &eid in &stream.extent_ids {
                                if matches!(
                                    self.extent_inflight_op(eid),
                                    Some(crate::extent_inflight::ExtentOpKind::Recovery)
                                ) {
                                    return Err(AppError::Precondition(format!(
                                        "recovery in flight on extent {eid}; retry split"
                                    )));
                                }
                            }
                        }
                    }
                }

                // F146: snapshot pre-mutation eversions so Phase-3 can verify
                // no concurrent mutator ran during Phase-2's etcd await.
                let pre_bump_eversion: HashMap<u64, u64> = {
                    let mut m = HashMap::new();
                    for &sid in &[src_meta.log_stream, src_meta.row_stream, src_meta.meta_stream] {
                        if let Some(stream) = s.streams.get(&sid) {
                            for &eid in &stream.extent_ids {
                                if let Some(ex) = s.extents.get(&eid) {
                                    m.insert(eid, ex.eversion);
                                }
                            }
                        }
                    }
                    m
                };

                let (start, end) = s.alloc_ids(4);
                let new_log_stream = start;
                let new_row_stream = start + 1;
                let new_meta_stream = start + 2;
                let new_part_id = end - 1;

                // Compute stream duplications without modifying state
                let (log_dup, log_exts) = Self::compute_duplicate_stream(
                    &s, src_meta.log_stream, new_log_stream, req.log_stream_sealed_length,
                )?;
                let (row_dup, row_exts) = Self::compute_duplicate_stream(
                    &s, src_meta.row_stream, new_row_stream, req.row_stream_sealed_length,
                )?;
                let (meta_dup, meta_exts) = Self::compute_duplicate_stream(
                    &s, src_meta.meta_stream, new_meta_stream, req.meta_stream_sealed_length,
                )?;

                let new_streams = vec![log_dup, row_dup, meta_dup];
                let mut all_extents = Vec::new();
                all_extents.extend(log_exts);
                all_extents.extend(row_exts);
                all_extents.extend(meta_exts);

                let mut left = src_meta.clone();
                let mut right = src_meta;
                left.rg = Some(MgrRange {
                    start_key: rg.start_key.clone(),
                    end_key: req.mid_key.clone(),
                });
                right.part_id = new_part_id;
                right.log_stream = new_log_stream;
                right.row_stream = new_row_stream;
                right.meta_stream = new_meta_stream;
                right.rg = Some(MgrRange {
                    start_key: req.mid_key,
                    end_key: rg.end_key,
                });

                let right_snapshot = Self::split_partition_vp_snapshot(&s, req.part_id, new_part_id);
                let vp_extent_puts = Self::preview_partition_vp_refs_apply(&s, &right_snapshot);
                all_extents = Self::merge_extent_updates(all_extents, vp_extent_puts);

                Ok((new_streams, all_extents, left, right, right_snapshot, pre_bump_eversion))
            })()
        };

        match out {
            Ok((new_streams, modified_extents, left, right, right_snapshot, pre_bump_eversion)) => {
                // F210-A2 verify-BEFORE-mirror (was verify-after-mirror at
                // Phase 3 in the F146 form). If any source-stream extent's
                // eversion drifted during the Phase-1 awaits, the etcd txn
                // we'd otherwise send is computed from a stale base —
                // refuse before committing to etcd. Pre-F210-A2 we caught
                // this AFTER the etcd commit, leaving etcd durable but
                // returning `Precondition` to the client (replay would
                // load the stale write as if successful).
                {
                    let s = self.store.inner.borrow();
                    for (eid, expected) in &pre_bump_eversion {
                        if let Some(live) = s.extents.get(eid).map(|ex| ex.eversion) {
                            if live != *expected {
                                return Ok(rkyv_encode(&CodeResp {
                                    code: CODE_PRECONDITION,
                                    message: format!(
                                        "extent {eid} eversion drift during split \
                                         ({expected} -> {live}); retry split"
                                    ),
                                }));
                            }
                        }
                    }
                }

                // Phase 2: Persist ALL mutations to etcd in ONE atomic txn
                // (F124: partitions + regions are included here, not in a
                // separate txn, to prevent orphan streams on crash.)
                if let Some(etcd) = &self.etcd {
                    let mut kvs =
                        Vec::with_capacity(new_streams.len() + modified_extents.len() + 5);
                    for st in &new_streams {
                        kvs.push((
                            format!("streams/{}", st.stream_id),
                            rkyv_encode(st).to_vec(),
                        ));
                    }
                    for ex in &modified_extents {
                        kvs.push((
                            format!("extents/{}", ex.extent_id),
                            rkyv_encode(ex).to_vec(),
                        ));
                    }
                    kvs.push((
                        format!("partitionVpRefs/{}", right_snapshot.part_id),
                        rkyv_encode(&right_snapshot).to_vec(),
                    ));
                    kvs.push((
                        format!("partitions/{}", left.part_id),
                        rkyv_encode(&left).to_vec(),
                    ));
                    kvs.push((
                        format!("partitions/{}", right.part_id),
                        rkyv_encode(&right).to_vec(),
                    ));
                    // Pre-compute region entries for left and right partitions
                    // so they are included in the same atomic txn.
                    {
                        let s = self.store.inner.borrow();
                        let left_region = Self::compute_region_for_partition(&s, &left);
                        let right_region = Self::compute_region_for_partition(&s, &right);
                        kvs.push((
                            format!("regions/{}", left.part_id),
                            rkyv_encode(&left_region).to_vec(),
                        ));
                        kvs.push((
                            format!("regions/{}", right.part_id),
                            rkyv_encode(&right_region).to_vec(),
                        ));
                    }
                    // F183: stamp last_op_at on both children so the
                    // policy engine's cooldown gate is correct.
                    let now = Self::epoch_seconds();
                    kvs.push((
                        format!("partitionLastOp/{}", left.part_id),
                        now.to_le_bytes().to_vec(),
                    ));
                    kvs.push((
                        format!("partitionLastOp/{}", right.part_id),
                        now.to_le_bytes().to_vec(),
                    ));
                    etcd.put_msgs_txn(kvs)
                        .await
                        .map_err(|e| Self::err_to_status(&e))?;
                }

                // Phase 3: Apply to in-memory store AFTER etcd success.
                // F210-A2: verify moved up before the Phase-2 mirror; here
                // we only apply (no verify).
                {
                    let mut s = self.store.inner.borrow_mut();
                    let _ = pre_bump_eversion; // captured for the verify-BEFORE block above
                    let left_id = left.part_id;
                    let right_id = right.part_id;
                    Self::apply_split_mutations(
                        &mut s,
                        &new_streams,
                        &modified_extents,
                        left,
                        right,
                    );
                    s.partition_vp_refs
                        .insert(right_snapshot.part_id, right_snapshot);
                    drop(s);
                    // F183: in-memory last_op_at update (mirror of etcd write above)
                    let now = Self::epoch_seconds();
                    self.last_op_at.borrow_mut().insert(left_id, now);
                    self.last_op_at.borrow_mut().insert(right_id, now);
                }

                Ok(rkyv_encode(&CodeResp {
                    code: CODE_OK,
                    message: String::new(),
                }))
            }
            Err(err) => Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            })),
        }
    }

    // ── PartitionManagerService handlers ───────────────────────────────

    // ── F183: handle_multi_modify_merge ─────────────────────────────────────
    // Inverse of handle_multi_modify_split. Atomically:
    //   - Splices victim's three streams' extent_ids into survivor's
    //   - Allocates a fresh log_stream tail extent (E_new) on K replicas
    //   - Merges victim's partition_vp_refs snapshot into survivor's
    //   - Widens survivor.rg.end_key to victim.rg.end_key
    //   - Deletes victim's partitions/streams/regions/partitionVpRefs/
    //     partitionLastOp keys
    //
    // Single-txn etcd commit (F124-style) — crash mid-merge means no state
    // change. F138/F145/F146 inflight checks. F146-style verify-at-apply on
    // pre_bump_eversion. F149 fence already applied via put_and_delete_txn.
    pub(crate) async fn handle_multi_modify_merge(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&MultiModifyMergeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                new_log_tail_extent_id: 0,
            }));
        }
        let req: MultiModifyMergeReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // F214-B: capture verified-online node set BEFORE borrowing the
        // store. Passed into the Phase-1 select_nodes call.
        let online_node_ids = self.node_states.borrow().online_node_ids();

        // Phase 1: compute under borrow_mut, NO awaits inside.
        // Returns alloc-IDs reserved + selected nodes for Phase 1.5.
        struct Phase1Result {
            new_streams: Vec<MgrStreamInfo>,
            modified_extents: Vec<MgrExtentInfo>,
            survivor_meta: MgrPartitionMeta,
            merged_vp: MgrPartitionVpRefs,
            victim_part_id: u64,
            victim_log: u64,
            victim_row: u64,
            victim_meta: u64,
            new_tail_id: u64,
            selected_nodes: Vec<MgrNodeInfo>,
            new_tail_replicas: u32,
            pre_bump_eversion: HashMap<u64, u64>,
            // Item 3 (uniform CAS): value-CAS baseline for each survivor stream
            // (log/row/meta) that the splice rewrites — `(streams/<id>,
            // pre-splice rkyv bytes)`. The Phase-2 txn CAS's these so a
            // concurrent alloc/punch/truncate committing on a survivor stream
            // during merge's etcd RTT makes the merge fail+retry instead of
            // resurrecting the concurrently-removed extent.
            survivor_stream_baselines: Vec<(String, Vec<u8>)>,
        }

        let phase1: Result<Phase1Result, AppError> = {
            let mut s = self.store.inner.borrow_mut();
            (|| -> Result<Phase1Result, AppError> {
                Self::ensure_owner_revision(&req.owner_key, req.revision, &s)?;

                if req.survivor_part_id == req.victim_part_id {
                    return Err(AppError::Precondition(
                        "survivor and victim are the same partition".to_string(),
                    ));
                }
                let survivor_meta = s
                    .partitions
                    .get(&req.survivor_part_id)
                    .cloned()
                    .ok_or_else(|| {
                        AppError::NotFound(format!("partition {}", req.survivor_part_id))
                    })?;
                let victim_meta =
                    s.partitions
                        .get(&req.victim_part_id)
                        .cloned()
                        .ok_or_else(|| {
                            AppError::NotFound(format!("partition {}", req.victim_part_id))
                        })?;
                let s_rg = survivor_meta
                    .rg
                    .clone()
                    .ok_or_else(|| AppError::Internal("survivor range missing".into()))?;
                let v_rg = victim_meta
                    .rg
                    .clone()
                    .ok_or_else(|| AppError::Internal("victim range missing".into()))?;
                if s_rg.end_key != v_rg.start_key {
                    return Err(AppError::Precondition(format!(
                        "partitions are not adjacent (survivor.end={:?}, victim.start={:?})",
                        s_rg.end_key, v_rg.start_key
                    )));
                }

                let all_streams = [
                    survivor_meta.log_stream,
                    survivor_meta.row_stream,
                    survivor_meta.meta_stream,
                    victim_meta.log_stream,
                    victim_meta.row_stream,
                    victim_meta.meta_stream,
                ];
                {
                    // F207-C: collapse the EC + Recovery + Delete checks
                    // into one ledger probe. Pre-F207 this was three
                    // separate Refs (ec_conversion_inflight,
                    // recovery_tasks, pending_extent_deletes) each
                    // queried per-extent. Now: one probe per extent
                    // returning the typed op kind. The typed error
                    // message preserves the operator-facing semantics
                    // (caller can tell which class of op is blocking).
                    for &sid in &all_streams {
                        if let Some(stream) = s.streams.get(&sid) {
                            for &eid in &stream.extent_ids {
                                if let Some(op) = self.extent_inflight_op(eid) {
                                    return Err(AppError::Precondition(format!(
                                        "extent {eid} has in-flight {op:?}; retry merge"
                                    )));
                                }
                            }
                        }
                    }
                }

                let pre_bump_eversion: HashMap<u64, u64> = {
                    let mut m = HashMap::new();
                    for &sid in &all_streams {
                        if let Some(stream) = s.streams.get(&sid) {
                            for &eid in &stream.extent_ids {
                                if let Some(ex) = s.extents.get(&eid) {
                                    m.insert(eid, ex.eversion);
                                }
                            }
                        }
                    }
                    m
                };

                let (new_tail_id, _) = s.alloc_ids(1);
                // Pick K replica nodes for E_new (replication factor matches
                // survivor's log_stream).
                let log_stream_meta =
                    s.streams.get(&survivor_meta.log_stream).ok_or_else(|| {
                        AppError::Internal(format!("stream {}", survivor_meta.log_stream))
                    })?;
                let target_replicas = if log_stream_meta.replicates > 0 {
                    log_stream_meta.replicates as usize
                } else {
                    3
                };
                let selected =
                    Self::select_nodes(&s.nodes, &s.disks, &online_node_ids, target_replicas, &[])?;
                let new_tail = MgrExtentInfo {
                    extent_id: new_tail_id,
                    replicates: selected.iter().map(|n| n.node_id).collect(),
                    parity: vec![],
                    replicate_disks: vec![0u64; selected.len()],
                    parity_disks: vec![],
                    sealed_length: 0,
                    sealed: false,
                    avali: 0,
                    eversion: 1,
                    refs: 1,
                    vp_table_refs: 0,
                    ec_converted: false,
                };

                let (log_dup, log_exts) = Self::compute_merge_streams(
                    &s,
                    survivor_meta.log_stream,
                    victim_meta.log_stream,
                    req.log_sealed_lengths[0] as u32,
                    req.log_sealed_lengths[1] as u32,
                    new_tail.clone(),
                )?;
                let (row_dup, row_exts) = Self::splice_streams_without_new_tail(
                    &s,
                    survivor_meta.row_stream,
                    victim_meta.row_stream,
                    req.row_sealed_lengths[0] as u32,
                    req.row_sealed_lengths[1] as u32,
                )?;
                let (meta_dup, meta_exts) = Self::splice_streams_without_new_tail(
                    &s,
                    survivor_meta.meta_stream,
                    victim_meta.meta_stream,
                    req.meta_sealed_lengths[0] as u32,
                    req.meta_sealed_lengths[1] as u32,
                )?;

                // Item 3 (uniform CAS): capture each survivor stream's
                // PRE-splice value (what etcd currently holds) as the CAS
                // baseline. `compute_*` returned clones, so `s.streams` still
                // holds the pre-splice survivor streams here.
                let survivor_stream_baselines: Vec<(String, Vec<u8>)> = [
                    survivor_meta.log_stream,
                    survivor_meta.row_stream,
                    survivor_meta.meta_stream,
                ]
                .into_iter()
                .filter_map(|sid| {
                    s.streams
                        .get(&sid)
                        .map(|st| (format!("streams/{sid}"), rkyv_encode(st).to_vec()))
                })
                .collect();

                let new_streams = vec![log_dup, row_dup, meta_dup];
                let mut all_extents = Vec::new();
                all_extents.extend(log_exts);
                all_extents.extend(row_exts);
                all_extents.extend(meta_exts);

                let merged_vp =
                    Self::merged_partition_vp_refs(&s, req.survivor_part_id, req.victim_part_id);
                let vp_extent_puts = Self::preview_partition_vp_refs_apply(&s, &merged_vp);
                let all_extents = Self::merge_extent_updates(all_extents, vp_extent_puts);

                let mut new_survivor_meta = survivor_meta.clone();
                new_survivor_meta.rg = Some(MgrRange {
                    start_key: s_rg.start_key,
                    end_key: v_rg.end_key,
                });

                Ok(Phase1Result {
                    new_streams,
                    modified_extents: all_extents,
                    survivor_meta: new_survivor_meta,
                    merged_vp,
                    victim_part_id: req.victim_part_id,
                    victim_log: victim_meta.log_stream,
                    victim_row: victim_meta.row_stream,
                    victim_meta: victim_meta.meta_stream,
                    new_tail_id,
                    selected_nodes: selected,
                    new_tail_replicas: target_replicas as u32,
                    pre_bump_eversion,
                    survivor_stream_baselines,
                })
            })()
        };

        let p1 = match phase1 {
            Ok(t) => t,
            Err(e) => {
                return Ok(rkyv_encode(&MultiModifyMergeResp {
                    code: Self::err_to_code(&e),
                    message: e.to_string(),
                    new_log_tail_extent_id: 0,
                }))
            }
        };

        // Phase 1.5: alloc_extent_on_node for E_new on each replica.
        // On per-node failure, fall back to other healthy nodes (mirrors
        // handle_stream_alloc_extent's fallback walk).
        let p1_selected_ids: HashSet<u64> = p1.selected_nodes.iter().map(|n| n.node_id).collect();
        let mut fallback_nodes: Vec<MgrNodeInfo> = {
            let s = self.store.inner.borrow();
            s.nodes
                .values()
                .filter(|n| !p1_selected_ids.contains(&n.node_id))
                .cloned()
                .collect()
        };
        {
            use rand::seq::SliceRandom;
            fallback_nodes.shuffle(&mut rand::thread_rng());
        }
        let mut fallback_iter = fallback_nodes.into_iter();
        let mut final_node_ids: Vec<u64> = Vec::with_capacity(p1.selected_nodes.len());
        let mut final_disk_ids: Vec<u64> = Vec::with_capacity(p1.selected_nodes.len());
        for n in &p1.selected_nodes {
            let mut candidate = n.clone();
            let (node_id, disk_id) = loop {
                match self
                    .alloc_extent_on_node(&candidate.address, p1.new_tail_id)
                    .await
                {
                    Ok(disk) => break (candidate.node_id, disk),
                    Err(_) => match fallback_iter.next() {
                        Some(alt) => candidate = alt,
                        None => {
                            return Ok(rkyv_encode(&MultiModifyMergeResp {
                                code: CODE_PRECONDITION,
                                message: format!(
                                    "no healthy node available to allocate E_new {}",
                                    p1.new_tail_id
                                ),
                                new_log_tail_extent_id: 0,
                            }));
                        }
                    },
                }
            };
            final_node_ids.push(node_id);
            final_disk_ids.push(disk_id);
        }

        // Patch E_new with the actual node/disk ids (Phase 1's selected_nodes
        // may have been replaced via fallback walk).
        let mut modified_extents = p1.modified_extents;
        let _ = p1.new_tail_replicas; // reserved for diagnostics
        if let Some(e_new) = modified_extents
            .iter_mut()
            .find(|e| e.extent_id == p1.new_tail_id)
        {
            e_new.replicates = final_node_ids;
            e_new.replicate_disks = final_disk_ids;
        }

        // F210-A2 verify-BEFORE-mirror (was verify-after-mirror at
        // Phase 3 in the F183/F185 form). If any source-stream extent's
        // eversion drifted during Phase 1.5 awaits (alloc_extent_on_node
        // for E_new across each replica node), the etcd txn we'd send
        // is computed from a stale base. Refuse before committing.
        {
            let s = self.store.inner.borrow();
            for (eid, expected) in &p1.pre_bump_eversion {
                if let Some(live) = s.extents.get(eid).map(|ex| ex.eversion) {
                    if live != *expected {
                        return Ok(rkyv_encode(&MultiModifyMergeResp {
                            code: CODE_PRECONDITION,
                            message: format!(
                                "extent {eid} eversion drift during merge \
                                 ({expected} -> {live}); retry merge"
                            ),
                            new_log_tail_extent_id: 0,
                        }));
                    }
                }
            }
        }

        // Phase 2: single fenced etcd txn.
        if let Some(etcd) = &self.etcd {
            let now = Self::epoch_seconds();
            let mut kvs = Vec::with_capacity(p1.new_streams.len() + modified_extents.len() + 6);
            for st in &p1.new_streams {
                kvs.push((
                    format!("streams/{}", st.stream_id),
                    rkyv_encode(st).to_vec(),
                ));
            }
            for ex in &modified_extents {
                kvs.push((
                    format!("extents/{}", ex.extent_id),
                    rkyv_encode(ex).to_vec(),
                ));
            }
            kvs.push((
                format!("partitionVpRefs/{}", p1.merged_vp.part_id),
                rkyv_encode(&p1.merged_vp).to_vec(),
            ));
            kvs.push((
                format!("partitions/{}", p1.survivor_meta.part_id),
                rkyv_encode(&p1.survivor_meta).to_vec(),
            ));
            {
                let s = self.store.inner.borrow();
                let region = Self::compute_region_for_partition(&s, &p1.survivor_meta);
                kvs.push((
                    format!("regions/{}", p1.survivor_meta.part_id),
                    rkyv_encode(&region).to_vec(),
                ));
            }
            kvs.push((
                format!("partitionLastOp/{}", p1.survivor_meta.part_id),
                now.to_le_bytes().to_vec(),
            ));

            let deletes = vec![
                format!("partitions/{}", p1.victim_part_id),
                format!("streams/{}", p1.victim_log),
                format!("streams/{}", p1.victim_row),
                format!("streams/{}", p1.victim_meta),
                format!("partitionVpRefs/{}", p1.victim_part_id),
                format!("regions/{}", p1.victim_part_id),
                format!("partitionLastOp/{}", p1.victim_part_id),
            ];
            // Item 3 (uniform CAS): value-CAS each survivor stream against its
            // pre-splice baseline so a concurrent alloc/punch/truncate that
            // committed on a survivor stream during this RTT makes the merge
            // fail+retry (CODE_PRECONDITION) instead of overwriting it with the
            // stale spliced membership (resurrecting a removed extent).
            etcd.put_delete_txn_cas(kvs, deletes, p1.survivor_stream_baselines.clone())
                .await
                .map_err(|e| Self::err_to_status(&e))?;
        }

        // Phase 3: in-memory apply. F210-A2: verify moved up before the
        // Phase-2 mirror; here we only apply.
        {
            let mut s = self.store.inner.borrow_mut();
            Self::apply_merge_mutations(
                &mut s,
                &p1.new_streams,
                &modified_extents,
                p1.survivor_meta.clone(),
                p1.merged_vp,
                p1.victim_part_id,
                p1.victim_log,
                p1.victim_row,
                p1.victim_meta,
            );
        }
        let now = Self::epoch_seconds();
        self.last_op_at
            .borrow_mut()
            .insert(p1.survivor_meta.part_id, now);
        self.last_op_at.borrow_mut().remove(&p1.victim_part_id);

        Ok(rkyv_encode(&MultiModifyMergeResp {
            code: CODE_OK,
            message: String::new(),
            new_log_tail_extent_id: p1.new_tail_id,
        }))
    }

    // ── F185: handle_merge_partitions (orchestrated merge) ─────────────
    //
    // Wraps the F183 multi-modify-merge txn with a PrepareMerge-style
    // freeze sequence, mirroring TiKV's pattern of letting the leader-
    // fenced control plane drive the cross-PS choreography. The sequence:
    //
    //   1. ensure_leader (manager state belongs to one instance only)
    //   2. resolve survivor + victim part_addr / stream ids in one borrow
    //   3. acquire admin owner-lock (so the embedded MultiModifyMerge txn
    //      has a fresh revision F149 can fence on)
    //   4. send MSG_MERGE_FREEZE to victim's PS, await OK
    //      (drains pending+inflight + flushes imm; no new writes accepted)
    //   5. send MSG_MERGE_FREEZE to survivor's PS, await OK
    //   6. capture commit_length × 6 (3 streams × 2 partitions) — these
    //      are the sealed_lengths that the manager merge txn will use
    //   7. invoke handle_multi_modify_merge synchronously (existing F183
    //      Phase-1 / 1.5 / 2 / 3 logic; etcd put_and_delete_txn is the
    //      atomic linearization point)
    //   8a. on success: do NOT explicitly unfreeze — region_sync_loop on
    //       both PSes will, on its next ~2 s tick, observe the new region
    //       state (survivor's rg widened, victim's region gone) and drop
    //       the frozen `PartitionData` entirely. The reopened survivor
    //       starts fresh with `frozen_for_merge = None`.
    //   8b. on failure: send freeze=false to anyone we already froze.
    //       Best-effort — if the unfreeze RPC also fails, the PS-side
    //       FREEZE_TTL (30 s) is the final backstop.
    //
    // Crash semantics:
    //   - manager crash before step 7's etcd commit: failover sees no
    //     in-progress merge in etcd, no rollback needed; PSes auto-
    //     unfreeze via FREEZE_TTL.
    //   - manager crash after step 7's etcd commit: merge is durable;
    //     region_sync_loop on PSes drives the reload normally.
    //   - PS crash mid-flow: in-memory freeze flag lost on restart;
    //     either the merge committed (PS reopens with merged state) or
    //     it didn't (PS reopens with original state).
    pub(crate) async fn handle_merge_partitions(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&MergePartitionsResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                new_log_tail_extent_id: 0,
            }));
        }
        let req: MergePartitionsReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // Resolve PS endpoints and stream ids in one borrow.
        struct PartInfo {
            part_addr: String,
            log_stream: u64,
            row_stream: u64,
            meta_stream: u64,
        }
        let (s_info, v_info): (PartInfo, PartInfo) = {
            let s = self.store.inner.borrow();
            let resolve = |pid: u64| -> Result<PartInfo, AppError> {
                let pm = s
                    .partitions
                    .get(&pid)
                    .ok_or_else(|| AppError::NotFound(format!("partition {pid}")))?;
                let addr = s.part_addrs.get(&pid).cloned().ok_or_else(|| {
                    AppError::Precondition(format!("partition {pid} has no PS addr"))
                })?;
                Ok(PartInfo {
                    part_addr: addr,
                    log_stream: pm.log_stream,
                    row_stream: pm.row_stream,
                    meta_stream: pm.meta_stream,
                })
            };
            match (resolve(req.survivor_part_id), resolve(req.victim_part_id)) {
                (Ok(s), Ok(v)) => (s, v),
                (Err(e), _) | (_, Err(e)) => {
                    return Ok(rkyv_encode(&MergePartitionsResp {
                        code: Self::err_to_code(&e),
                        message: e.to_string(),
                        new_log_tail_extent_id: 0,
                    }));
                }
            }
        };

        // Owner lock keyed on the partition pair so two concurrent merge
        // attempts targeting the same survivor serialize on the manager.
        let owner_key = format!(
            "admin-merge:{}:{}",
            req.survivor_part_id, req.victim_part_id
        );
        let revision = match self.acquire_owner_revision(&owner_key).await {
            Ok(r) => r,
            Err(e) => {
                return Ok(rkyv_encode(&MergePartitionsResp {
                    code: Self::err_to_code(&e),
                    message: e.to_string(),
                    new_log_tail_extent_id: 0,
                }));
            }
        };

        // Helper closures.
        let send_freeze = |addr: String, part_id: u64, freeze: bool| {
            let pool = self.conn_pool.clone();
            async move {
                let req = autumn_rpc::partition_rpc::MergeFreezeReq { part_id, freeze };
                let payload = autumn_rpc::partition_rpc::rkyv_encode(&req);
                // 30 s — MERGE_FREEZE drains pending+inflight on PS,
                // flushes every imm, halts new writes. Real work, but
                // bounded to avoid manager wedging on a dead PS.
                let resp_bytes = pool
                    .call_timeout(
                        &addr,
                        autumn_rpc::partition_rpc::MSG_MERGE_FREEZE,
                        payload,
                        Duration::from_secs(30),
                    )
                    .await
                    .map_err(|e| AppError::Internal(format!("freeze rpc to {addr}: {e}")))?;
                let resp: autumn_rpc::partition_rpc::MergeFreezeResp =
                    autumn_rpc::partition_rpc::rkyv_decode(&resp_bytes)
                        .map_err(AppError::Internal)?;
                if resp.code != autumn_rpc::partition_rpc::CODE_OK {
                    return Err(AppError::Precondition(format!(
                        "freeze({freeze}) on partition {part_id}: {}",
                        resp.message
                    )));
                }
                Ok(())
            }
        };

        // Track which PSes we successfully froze, in reverse order, for
        // best-effort rollback on failure.
        let mut to_unfreeze: Vec<(String, u64)> = Vec::new();
        let rollback = |list: Vec<(String, u64)>, pool: Rc<ConnPool>| async move {
            for (addr, pid) in list.into_iter().rev() {
                let unfreeze = autumn_rpc::partition_rpc::MergeFreezeReq {
                    part_id: pid,
                    freeze: false,
                };
                let payload = autumn_rpc::partition_rpc::rkyv_encode(&unfreeze);
                // 10 s — best-effort rollback unfreeze; PS may already
                // be torn down. Don't wedge the rollback path either.
                let _ = pool
                    .call_timeout(
                        &addr,
                        autumn_rpc::partition_rpc::MSG_MERGE_FREEZE,
                        payload,
                        Duration::from_secs(10),
                    )
                    .await;
            }
        };

        // Freeze victim first (matches the dual-gate ordering convention
        // in `crates/partition-server/CLAUDE.md` — victim < survivor for
        // deadlock-safe lock acquisition; here the freezes don't deadlock
        // each other but we keep the order for consistency with future
        // PS-side gate work).
        if let Err(e) = send_freeze(v_info.part_addr.clone(), req.victim_part_id, true).await {
            return Ok(rkyv_encode(&MergePartitionsResp {
                code: Self::err_to_code(&e),
                message: e.to_string(),
                new_log_tail_extent_id: 0,
            }));
        }
        to_unfreeze.push((v_info.part_addr.clone(), req.victim_part_id));

        if let Err(e) = send_freeze(s_info.part_addr.clone(), req.survivor_part_id, true).await {
            rollback(to_unfreeze.clone(), self.conn_pool.clone()).await;
            return Ok(rkyv_encode(&MergePartitionsResp {
                code: Self::err_to_code(&e),
                message: e.to_string(),
                new_log_tail_extent_id: 0,
            }));
        }
        to_unfreeze.push((s_info.part_addr.clone(), req.survivor_part_id));

        // F210-C4: pull-sync vp_refs from BOTH PSes after freeze
        // succeeds but BEFORE capturing commit_length / running the
        // atomic merge txn. Freeze guarantees no new writes can land,
        // so the pulled snapshot is stable. Without this, the manager's
        // `partition_vp_refs` for either partition might be stale if a
        // previous flush/compact sync failed — `apply_merge_mutations`
        // would compute the merged snapshot against the stale view and
        // miss vp_table_refs on extents that the survivor/victim's SSTs
        // actually reference, opening a deletion race after merge.
        for (addr, pid) in &[
            (&v_info.part_addr, req.victim_part_id),
            (&s_info.part_addr, req.survivor_part_id),
        ] {
            if let Err(e) = self.pull_and_apply_vp_refs(*pid, addr).await {
                rollback(to_unfreeze.clone(), self.conn_pool.clone()).await;
                return Ok(rkyv_encode(&MergePartitionsResp {
                    code: Self::err_to_code(&e),
                    message: format!("F210-C4 pull_vp_refs pre-merge: {e}"),
                    new_log_tail_extent_id: 0,
                }));
            }
        }

        // Capture commit_length on each of the 6 streams. Reuse the
        // existing handle_check_commit_length so we hit the same
        // sealed-vs-live + min-replica path the F183 code expects.
        let read_commit_len = |stream_id: u64| {
            let owner_key = owner_key.clone();
            async move {
                let req = CheckCommitLengthReq {
                    stream_id,
                    owner_key,
                    revision,
                };
                let resp_bytes = self.handle_check_commit_length(rkyv_encode(&req)).await?;
                let resp: CheckCommitLengthResp =
                    rkyv_decode(&resp_bytes).map_err(|e| (StatusCode::Internal, e))?;
                if resp.code != CODE_OK {
                    return Err((
                        StatusCode::Internal,
                        format!("commit_length stream {stream_id}: {}", resp.message),
                    ));
                }
                // Match the CLI's `.max(1)` — F183's manager treats 0 as
                // "use the existing sealed_length" / no-op for this stream.
                Ok::<u64, (StatusCode, String)>((resp.end as u64).max(1))
            }
        };

        // Six commit_lengths in the order [survivor_log, victim_log,
        // survivor_row, victim_row, survivor_meta, victim_meta]. Captured
        // serially under the freeze; concurrency would not save much
        // here and serial keeps the failure mode simpler.
        let log_lens = match (
            read_commit_len(s_info.log_stream).await,
            read_commit_len(v_info.log_stream).await,
        ) {
            (Ok(s), Ok(v)) => [s, v],
            (Err((code, msg)), _) | (_, Err((code, msg))) => {
                rollback(to_unfreeze.clone(), self.conn_pool.clone()).await;
                return Err((code, msg));
            }
        };
        let row_lens = match (
            read_commit_len(s_info.row_stream).await,
            read_commit_len(v_info.row_stream).await,
        ) {
            (Ok(s), Ok(v)) => [s, v],
            (Err((code, msg)), _) | (_, Err((code, msg))) => {
                rollback(to_unfreeze.clone(), self.conn_pool.clone()).await;
                return Err((code, msg));
            }
        };
        let meta_lens = match (
            read_commit_len(s_info.meta_stream).await,
            read_commit_len(v_info.meta_stream).await,
        ) {
            (Ok(s), Ok(v)) => [s, v],
            (Err((code, msg)), _) | (_, Err((code, msg))) => {
                rollback(to_unfreeze.clone(), self.conn_pool.clone()).await;
                return Err((code, msg));
            }
        };

        // Run the existing F183 merge txn under the same owner-lock.
        let mmm_req = MultiModifyMergeReq {
            survivor_part_id: req.survivor_part_id,
            victim_part_id: req.victim_part_id,
            owner_key: owner_key.clone(),
            revision,
            log_sealed_lengths: log_lens,
            row_sealed_lengths: row_lens,
            meta_sealed_lengths: meta_lens,
        };
        let mmm_resp_bytes = match self.handle_multi_modify_merge(rkyv_encode(&mmm_req)).await {
            Ok(b) => b,
            Err((code, msg)) => {
                rollback(to_unfreeze.clone(), self.conn_pool.clone()).await;
                return Err((code, msg));
            }
        };
        let mmm_resp: MultiModifyMergeResp =
            rkyv_decode(&mmm_resp_bytes).map_err(|e| (StatusCode::Internal, e))?;

        if mmm_resp.code != CODE_OK {
            // Rollback freezes — txn refused, both PSes should resume.
            rollback(to_unfreeze.clone(), self.conn_pool.clone()).await;
            return Ok(rkyv_encode(&MergePartitionsResp {
                code: mmm_resp.code,
                message: mmm_resp.message,
                new_log_tail_extent_id: 0,
            }));
        }

        // Success path: leave both PSes frozen. Their region_sync_loop
        // will, on its next ~2 s tick, observe the new region state and
        // drop the frozen `PartitionData` entirely — natural unfreeze.
        Ok(rkyv_encode(&MergePartitionsResp {
            code: CODE_OK,
            message: String::new(),
            new_log_tail_extent_id: mmm_resp.new_log_tail_extent_id,
        }))
    }

    // ── F183: handle_get_policy_candidates / handle_report_partition_load ──

    pub(crate) async fn handle_get_policy_candidates(&self, _payload: Bytes) -> HandlerResult {
        // F210-F6: leader gate. Pre-F210-F6 the handler returned
        // `advisory_cache` on any node, but only the leader's
        // `policy_tick_loop` populates the cache (follower's stays
        // empty). An external controller polling `MSG_GET_POLICY_CANDIDATES`
        // against a follower silently received an empty candidate list
        // — indistinguishable from "nothing to do" — and would never
        // notice it was asking the wrong node. Same fix pattern as
        // F209-A's `handle_get_partition_detail` gate.
        if !self.leader.get() {
            return Ok(rkyv_encode(&GetPolicyCandidatesResp {
                code: CODE_NOT_LEADER,
                message: "not leader".to_string(),
                candidates: Vec::new(),
            }));
        }
        let p = self.policy.borrow();
        let candidates = p.advisory_cache.clone();
        Ok(rkyv_encode(&GetPolicyCandidatesResp {
            code: CODE_OK,
            message: String::new(),
            candidates,
        }))
    }

    /// F210-F1: const-dump of the `POLICY_KIND_*` enum so external
    /// controllers can introspect the wire mapping at startup rather
    /// than hardcoding numeric values that may have drifted across
    /// docs/code (the pre-F210-F1 off-by-one was caused by exactly
    /// that drift). No leader gate — the answer is a compile-time
    /// constant of THIS binary; any node can serve it.
    pub(crate) async fn handle_get_policy_kind_names(&self, _payload: Bytes) -> HandlerResult {
        Ok(rkyv_encode(&GetPolicyKindNamesResp {
            code: CODE_OK,
            message: String::new(),
            kinds: policy_kind_names(),
        }))
    }

    pub(crate) async fn handle_report_partition_load(&self, payload: Bytes) -> HandlerResult {
        let req: ReportPartitionLoadReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let now = Self::epoch_seconds();
        let mut p = self.policy.borrow_mut();
        // F210-F5: honour the configured `window_buckets / bucket_sec`
        // (was hardcoded `POLICY_WINDOW_BUCKETS / POLICY_BUCKET_SEC`,
        // making the `PolicyConfig` fields dead). With this in place
        // `set_policy_config` actually reshapes the history window;
        // tests using a small `window_buckets / bucket_sec` no longer
        // need to call internal helpers.
        let cap = p.config.window_buckets.max(1);
        let bucket_sec = p.config.bucket_sec.max(1);
        for load in req.partitions {
            p.metrics
                .entry(load.part_id)
                .or_default()
                .push_with_cap_and_bucket(now, load, cap, bucket_sec);
        }
        drop(p);
        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    /// F203: OP-driven per-extent EC convert trigger. Validates the
    /// extent is sealed, not already converted, and references an
    /// EC-policy stream. Persists a rich `pending_ec_dispatch` marker
    /// to etcd + memory; the next `ec_conversion_dispatch_loop` tick
    /// (within ~5 s) drains it via the F198 replay path and runs the
    /// existing 2PC encode + commit flow.
    ///
    /// Idempotent: re-invocation against an already-pending or
    /// already-converted extent returns CODE_OK. Out-of-policy
    /// requests (non-EC stream, sealed_length=0, missing extent)
    /// return CODE_PRECONDITION with a descriptive message.
    pub(crate) async fn handle_force_ec_convert(&self, payload: Bytes) -> HandlerResult {
        if !self.leader.get() {
            return Ok(rkyv_encode(&ForceEcConvertResp {
                code: CODE_NOT_LEADER,
                message: "not leader".to_string(),
            }));
        }
        let req: ForceEcConvertReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let extent_id = req.extent_id;

        // F207-B: already in-flight (any stream-layer op)? Idempotent OK
        // for the ConvertToEc case (caller's intent matches the in-flight
        // op); Precondition for Recovery / Delete (different ops, retry
        // later).
        match self.extent_inflight_op(extent_id) {
            Some(crate::extent_inflight::ExtentOpKind::ConvertToEc) => {
                return Ok(rkyv_encode(&ForceEcConvertResp {
                    code: CODE_OK,
                    message: "already pending dispatch".to_string(),
                }));
            }
            Some(other) => {
                return Ok(rkyv_encode(&ForceEcConvertResp {
                    code: CODE_PRECONDITION,
                    message: format!(
                        "extent {extent_id} has in-flight {other:?}; retry after it completes"
                    ),
                }));
            }
            None => {}
        }

        // Look up current state + the owning stream's EC shape under
        // a single borrow.
        let (ex, stream, node_addrs) = {
            let s = self.store.inner.borrow();
            let ex = match s.extents.get(&extent_id) {
                Some(e) => e.clone(),
                None => {
                    return Ok(rkyv_encode(&ForceEcConvertResp {
                        code: CODE_PRECONDITION,
                        message: format!("extent {extent_id} not found"),
                    }));
                }
            };
            if ex.sealed_length == 0 {
                return Ok(rkyv_encode(&ForceEcConvertResp {
                    code: CODE_PRECONDITION,
                    message: format!(
                        "extent {extent_id} not sealed (sealed_length=0); use GC for empty slots"
                    ),
                }));
            }
            if ex.ec_converted {
                return Ok(rkyv_encode(&ForceEcConvertResp {
                    code: CODE_OK,
                    message: format!("extent {extent_id} already ec_converted"),
                }));
            }
            let stream = s
                .streams
                .values()
                .find(|st| st.ec_parity_shard > 0 && st.extent_ids.contains(&extent_id));
            let stream = match stream {
                Some(s) => s.clone(),
                None => {
                    return Ok(rkyv_encode(&ForceEcConvertResp {
                        code: CODE_PRECONDITION,
                        message: format!(
                            "extent {extent_id} is not on an EC-policy stream (set-stream-ec first)"
                        ),
                    }));
                }
            };
            let node_addrs: HashMap<u64, String> = s
                .nodes
                .iter()
                .map(|(id, n)| (*id, n.address.clone()))
                .collect();
            (ex, stream, node_addrs)
        };

        // Derive target_nodes + extra_disk_ids the same way
        // `ec_conversion_dispatch_loop` did in its pre-F203 fresh path.
        let data_shards = stream.ec_data_shard as usize;
        let parity_shards = stream.ec_parity_shard as usize;
        let total_shards = data_shards + parity_shards;

        let mut target_nodes = ex.replicates.clone();
        let mut extra_disk_ids: Vec<u64> = Vec::new();
        let mut target_addrs: Vec<String> = Vec::new();
        for &nid in &target_nodes {
            match node_addrs.get(&nid) {
                Some(addr) => target_addrs.push(addr.clone()),
                None => {
                    return Ok(rkyv_encode(&ForceEcConvertResp {
                        code: CODE_PRECONDITION,
                        message: format!("target node {nid} not in nodes map"),
                    }));
                }
            }
        }

        if total_shards > target_nodes.len() {
            let extra_needed = total_shards - target_nodes.len();
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
                return Ok(rkyv_encode(&ForceEcConvertResp {
                    code: CODE_PRECONDITION,
                    message: format!(
                        "not enough nodes for EC {data_shards}+{parity_shards} ({} of {total_shards} available)",
                        target_nodes.len() + extra_candidates.len()
                    ),
                }));
            }
            for node in &extra_candidates {
                match self.alloc_extent_on_node(&node.address, extent_id).await {
                    Ok(disk_id) => {
                        target_nodes.push(node.node_id);
                        extra_disk_ids.push(disk_id);
                    }
                    Err(e) => {
                        return Ok(rkyv_encode(&ForceEcConvertResp {
                            code: CODE_ERROR,
                            message: format!("alloc_extent_on_node({}): {e}", node.address),
                        }));
                    }
                }
            }
        }
        target_nodes.truncate(total_shards);

        // F209-D verify-BEFORE-acquire (revised after codex review of
        // F209-D's initial verify-AFTER-acquire form). The race we close:
        // between the L2436 snapshot and our `acquire_extent_inflight`
        // call below there are N `alloc_extent_on_node` awaits — during
        // them an `apply_recovery_done` for this extent can complete
        // (Recovery marker present at snapshot time would have been
        // caught by L2416's `extent_inflight_op` probe; the race is for
        // a Recovery that started after L2416 finished and completed
        // during alloc await). Recovery bumps `ex.eversion` + rewrites
        // `ex.replicates`. If we proceeded to acquire with our stale
        // snapshot's `ex.eversion + 1`, the dispatch loop would later
        // run `apply_ec_conversion_done` with that stale `new_eversion`
        // and overwrite recovery's slot change.
        //
        // **Why verify-before, not verify-after:** an initial F209-D
        // form did verify-after-acquire + drain-on-mismatch. Codex
        // review flagged: if `drain_extent_inflight_marker` fails
        // (NotLeader during the drain await, or transient etcd error),
        // the stale marker stays in etcd. The dispatch loop's next
        // 5 s tick (or a successor leader's replay) then runs
        // `apply_ec_conversion_done` with the stale `new_eversion` —
        // exactly the corruption the check was supposed to prevent.
        //
        // Verify-before sidesteps the problem entirely: no marker is
        // ever written if the state has drifted, so no drain is needed.
        // After our `acquire_extent_inflight` succeeds, `ex.eversion`
        // is frozen until our `apply_ec_conversion_done` runs —
        // every other mutator (apply_recovery_done, handle_*_punch_holes,
        // handle_truncate, handle_multi_modify_split / merge,
        // handle_sync_partition_vp_refs, handle_stream_alloc_extent)
        // checks `extent_inflight_op` and refuses on ConvertToEc.
        // Recovery cannot even start (its `acquire_extent_inflight` CAS
        // would fail against our marker). So no verify-after is needed.
        let pre_eversion = ex.eversion;
        let live_eversion = self
            .store
            .inner
            .borrow()
            .extents
            .get(&extent_id)
            .map(|e| e.eversion);
        let live_eversion = match live_eversion {
            Some(v) => v,
            None => {
                return Ok(rkyv_encode(&ForceEcConvertResp {
                    code: CODE_PRECONDITION,
                    message: format!(
                        "extent {extent_id} removed during force-ec-convert (concurrent gc)"
                    ),
                }));
            }
        };
        if live_eversion != pre_eversion {
            return Ok(rkyv_encode(&ForceEcConvertResp {
                code: CODE_PRECONDITION,
                message: format!(
                    "extent {extent_id} eversion changed during force-ec-convert \
                     (pre={pre_eversion}, live={live_eversion}); retry to pick up new state"
                ),
            }));
        }

        let new_eversion = live_eversion + 1;

        // F211-D Tier 2: capture the current owner_lock revision for the
        // partition that owns this extent. Threaded through dispatch ->
        // coord -> WriteShard/CommitEcShard so a fenced ex-coord's
        // in-flight 2PC is rejected by remote ENs once
        // `auto_abandon_for_fenced_node` bumps their `entry.owner_revision`
        // via fence-handover. CoW-shared extents (refs >= 2) appear in
        // multiple partitions' streams; any of them works because all
        // sharing partitions hold the same owner_lock revision at any
        // moment (revisions are bumped uniformly by F211-D).
        let dispatch_revision: i64 = {
            let s = self.store.inner.borrow();
            let mut found: i64 = 0;
            'outer: for part in s.partitions.values() {
                let streams = [part.log_stream, part.row_stream, part.meta_stream];
                for sid in streams {
                    if s.streams
                        .get(&sid)
                        .map(|st| st.extent_ids.contains(&extent_id))
                        .unwrap_or(false)
                    {
                        let key = format!("partition/{}", part.part_id);
                        if let Some(&rev) = s.owner_revisions.get(&key) {
                            found = rev;
                        }
                        break 'outer;
                    }
                }
            }
            found
        };

        let dispatch_record = MgrEcDispatchInflight {
            extent_id,
            target_nodes,
            extra_disk_ids,
            data_shards: data_shards as u32,
            new_eversion,
            revision: dispatch_revision,
        };

        // F207-B: acquire the unified inflight marker. CAS via
        // create_revision==0 + F149 leader fence in a single etcd txn —
        // replaces the pre-F207 `persist_ec_conversion_inflight + in-memory
        // insert` pair (two operations, the in-memory write could observe
        // an etcd failure post-facto). The CAS makes "already in-flight"
        // a clean Precondition error path rather than a silent overwrite.
        if let Err(e) = self
            .acquire_extent_inflight(
                extent_id,
                crate::extent_inflight::ExtentOpPayload::ConvertToEc(dispatch_record),
            )
            .await
        {
            return Ok(rkyv_encode(&ForceEcConvertResp {
                code: match &e {
                    AppError::Precondition(_) => CODE_PRECONDITION,
                    AppError::NotLeader => CODE_NOT_LEADER,
                    _ => CODE_ERROR,
                },
                message: format!("acquire marker: {e}"),
            }));
        }

        Ok(rkyv_encode(&ForceEcConvertResp {
            code: CODE_OK,
            message: format!(
                "marker persisted for extent {extent_id}; next ec dispatch tick (~5s) will convert"
            ),
        }))
    }

    /// F203: external policy controller — return the manager's most
    /// recent cached `PartitionLoad` for `part_id`. Sourced from the
    /// last bucket of `PolicyEngine.metrics`, populated by
    /// `MSG_REPORT_PARTITION_LOAD`. Lets `client info --detail`
    /// surface per-partition F202 metrics without a dedicated PS RPC.
    pub(crate) async fn handle_get_partition_detail(&self, payload: Bytes) -> HandlerResult {
        // F209-A: followers' `policy.metrics` is empty (only the leader's
        // policy_tick_loop populates it from MSG_REPORT_PARTITION_LOAD).
        // Without this gate, querying a follower silently returned
        // `CODE_OK` + all-zero PartitionLoad — operators couldn't tell
        // "no metrics yet" from "queried the wrong node".
        if !self.leader.get() {
            return Ok(rkyv_encode(&GetPartitionDetailResp {
                code: CODE_NOT_LEADER,
                message: "not leader".to_string(),
                load: PartitionLoad::default(),
                bucket_ts: 0,
            }));
        }
        let req: GetPartitionDetailReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let p = self.policy.borrow();
        let bucket = p.metrics.get(&req.part_id).and_then(|w| w.buckets.back());
        let (load, bucket_ts) = match bucket {
            Some((ts, l)) => (l.clone(), *ts),
            None => (PartitionLoad::default(), 0),
        };
        Ok(rkyv_encode(&GetPartitionDetailResp {
            code: CODE_OK,
            message: String::new(),
            load,
            bucket_ts,
        }))
    }

    /// F192: PS pushes a per-replica failure observation; manager
    /// debounces with a 60 s sliding window and 3-distinct-reporter
    /// quorum before flipping `node.disks[*].online = false`. The flip
    /// is in-memory only and the call is fire-and-forget on the wire
    /// — leader-fence isn't required for correctness because
    /// `disk_status_update_loop` (every 10 s) is the authoritative
    /// truth and will overwrite this purely-advisory state on the
    /// next successful DF. We deliberately do NOT trigger
    /// `require_recovery` from here — that's still owned by
    /// `recovery_dispatch_loop` (5 s tick) so a transient regional
    /// hiccup doesn't kick off a recovery storm.
    pub(crate) async fn handle_report_disk_failure(&self, payload: Bytes) -> HandlerResult {
        let req: ReportDiskFailureReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        // Even on a follower (non-leader) we accept the report — the
        // follower will replay manager state on promotion and the
        // quorum is purely advisory. Skip the leader gate; the call
        // is fire-and-forget so the client doesn't observe a refusal.
        let now = Instant::now();
        // F195: read F192 quorum config from `AutumnManager` fields
        // populated at construction / binary-flag time. No env reads.
        let window = self.report_disk_failure_window.get();
        let quorum: usize = self.report_disk_failure_quorum.get();
        let cutoff = now.checked_sub(window).unwrap_or(now);

        let reached_quorum = {
            let mut reports = self.recent_failure_reports.borrow_mut();
            let entry = reports.entry(req.node_id).or_default();
            // Evict expired first so the deduplicated-reporter count
            // reflects only the current window.
            while let Some(&(t, _)) = entry.front() {
                if t < cutoff {
                    entry.pop_front();
                } else {
                    break;
                }
            }
            // Avoid double-counting the same reporter_part_id within the
            // active window. The producer's per-stream bad_nodes TTL
            // (30 s) bounds spam from the same writer; this dedup is
            // belt-and-braces against multi-stream PSes that observe
            // the same dead node from multiple streams in the same
            // window — they should count as ONE reporter for quorum.
            if !entry.iter().any(|(_, rp)| *rp == req.reporter_part_id) {
                entry.push_back((now, req.reporter_part_id));
            }
            let distinct: HashSet<u64> = entry.iter().map(|(_, rp)| *rp).collect();
            tracing::debug!(
                node_id = req.node_id,
                extent_id = req.extent_id,
                error_kind = req.error_kind,
                reporter = req.reporter_part_id,
                ts_ms = req.ts_ms,
                window_size = entry.len(),
                distinct_reporters = distinct.len(),
                quorum,
                "f192 report_disk_failure"
            );
            distinct.len() >= quorum
        };

        if reached_quorum {
            // Apply: mark every disk on the node offline. Same path
            // taken by `node_health_loop` (F222) on a failed DF.
            let nodes_clone = {
                let s = self.store.inner.borrow();
                s.nodes.clone()
            };
            if let Some(node) = nodes_clone.get(&req.node_id) {
                Self::mark_node_disks_offline(&self.store, node);
                tracing::warn!(
                    node_id = req.node_id,
                    quorum,
                    "f192 quorum reached — node marked offline (advisory; \
                     node_health_loop reconciles on next DF tick)"
                );
            }
            // Defuse: clear so we don't re-flip on a stale residual
            // burst after the next successful DF promotes the node
            // back online.
            self.recent_failure_reports
                .borrow_mut()
                .remove(&req.node_id);
        }

        // Fire-and-forget on the wire; reply is technically dropped
        // by the client but we still return a CODE_OK frame so the
        // RpcServer doesn't surface this as an error.
        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    pub(crate) async fn handle_register_ps(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }

        let req: RegisterPsReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let ps_id = req.ps_id;
        {
            let mut s = self.store.inner.borrow_mut();
            s.ps_nodes.insert(ps_id, req.address);
            Self::rebalance_regions(&mut s);
        }
        self.ps_last_heartbeat
            .borrow_mut()
            .insert(ps_id, Instant::now());
        if let Err(err) = self.mirror_partition_snapshot().await {
            return Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }
        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    pub(crate) async fn handle_upsert_partition(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&UpsertPartitionResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                part_id: 0,
            }));
        }

        let req: UpsertPartitionReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        let assigned_part_id = {
            let mut s = self.store.inner.borrow_mut();
            let mut meta = req.meta;
            // Auto-assign part_id via alloc_ids when client sends 0
            if meta.part_id == 0 {
                let (id, _) = s.alloc_ids(1);
                meta.part_id = id;
            }
            let pid = meta.part_id;
            s.partitions.insert(pid, meta);
            Self::rebalance_regions(&mut s);
            pid
        };
        if let Err(err) = self.mirror_partition_snapshot().await {
            return Ok(rkyv_encode(&UpsertPartitionResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                part_id: 0,
            }));
        }

        Ok(rkyv_encode(&UpsertPartitionResp {
            code: CODE_OK,
            message: String::new(),
            part_id: assigned_part_id,
        }))
    }

    pub(crate) async fn handle_get_regions(&self) -> HandlerResult {
        let s = self.store.inner.borrow();
        let regions = s.regions.iter().map(|(&id, r)| (id, r.clone())).collect();
        let ps_details = s
            .ps_nodes
            .iter()
            .map(|(&ps_id, addr)| {
                (
                    ps_id,
                    MgrPsDetail {
                        ps_id,
                        address: addr.clone(),
                    },
                )
            })
            .collect();
        // F099-K: per-partition listener addresses. Only emit entries for
        // partitions that actually have a region assignment — this keeps
        // stale `part_addrs` entries (e.g. from a dropped partition whose
        // registration entry wasn't cleared) from being returned to
        // clients and confusing routing.
        let part_addrs: Vec<(u64, String)> = s
            .part_addrs
            .iter()
            .filter(|(pid, _)| s.regions.contains_key(*pid))
            .map(|(&pid, addr)| (pid, addr.clone()))
            .collect();
        Ok(rkyv_encode(&GetRegionsResp {
            code: CODE_OK,
            message: String::new(),
            regions,
            ps_details,
            part_addrs,
        }))
    }

    pub(crate) async fn handle_heartbeat_ps(&self, payload: Bytes) -> HandlerResult {
        let req: HeartbeatPsReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let known = {
            let s = self.store.inner.borrow();
            s.ps_nodes.contains_key(&req.ps_id)
        };
        if known {
            self.ps_last_heartbeat
                .borrow_mut()
                .insert(req.ps_id, Instant::now());
            Ok(rkyv_encode(&CodeResp {
                code: CODE_OK,
                message: String::new(),
            }))
        } else {
            // Surface eviction so the PS can re-register instead of staying
            // invisible to clients (`ps=unknown` in `info` output).
            Ok(rkyv_encode(&CodeResp {
                code: CODE_NOT_FOUND,
                message: format!("ps {} not registered", req.ps_id),
            }))
        }
    }

    /// F109: extent-node startup orphan reconcile. Node sends every
    /// `extent_id` it found on disk; we return those that are no longer
    /// in `s.extents`. The node then unlinks the corresponding files.
    /// Best-effort: failure is logged on the node side but doesn't block
    /// startup. Read-only with respect to manager state.
    async fn handle_reconcile_extents(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&ReconcileExtentsResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                garbage: Vec::new(),
            }));
        }
        let req: ReconcileExtentsReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let garbage: Vec<u64> = {
            let s = self.store.inner.borrow();
            req.extent_ids
                .iter()
                .copied()
                .filter(|eid| !s.extents.contains_key(eid))
                .collect()
        };
        if !garbage.is_empty() {
            tracing::info!(
                node_id = req.node_id,
                local_extents = req.extent_ids.len(),
                garbage = garbage.len(),
                "F109 reconcile_extents: returning orphan list to node",
            );
        }
        Ok(rkyv_encode(&ReconcileExtentsResp {
            code: CODE_OK,
            message: String::new(),
            garbage,
        }))
    }

    async fn handle_register_partition_addr(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }
        let req: RegisterPartitionAddrReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        // F099-K — record the per-partition listener address. We do NOT
        // validate that `part_id` is owned by `ps_id` here: the manager's
        // region table is the source of truth for ownership, and the
        // mapping is re-validated on `GetRegions` (only partitions with
        // an assigned region are returned). Overwrites are allowed —
        // if a PS re-binds a partition on a new port (restart, split),
        // the latest report wins.
        let mut s = self.store.inner.borrow_mut();
        let _ = req.ps_id; // reserved for future validation
        s.part_addrs.insert(req.part_id, req.address);
        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    pub(crate) async fn handle_sync_partition_vp_refs(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&SyncPartitionVpRefsResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }
        let req: SyncPartitionVpRefsReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let snapshot = MgrPartitionVpRefs {
            part_id: req.part_id,
            refs: req.refs,
        };
        // F147-A: single borrow block — compute deltas once, then (1) check
        // in-flight guards, (2) build extent_puts, and (3) snapshot pre_eversion.
        // Pre-F147-A this was two separate borrow blocks each calling
        // partition_vp_ref_deltas, allocating the HashMap twice.
        let (extent_puts, pre_eversion) = {
            let s = self.store.inner.borrow();
            let deltas = Self::partition_vp_ref_deltas(&s, &snapshot);
            // Refuse-at-start: if any touched extent is in-flight for another
            // eversion-bumping operation, concurrent mutators (recovery,
            // EC conversion) may bump eversion during the etcd await below;
            // our pre-await blobs in extent_puts would then overwrite fresher
            // data in etcd (last-writer-wins). PS must retry after the
            // in-flight op clears.
            for extent_id in deltas.keys().copied() {
                // F207-B: read the unified ledger (was F138/F147-A `ec_conversion_inflight`).
                if matches!(
                    self.extent_inflight_op(extent_id),
                    Some(crate::extent_inflight::ExtentOpKind::ConvertToEc)
                ) {
                    return Ok(rkyv_encode(&SyncPartitionVpRefsResp {
                        code: CODE_PRECONDITION,
                        message: format!(
                            "extent {extent_id} has in-flight EC conversion; \
                             defer sync_partition_vp_refs until conversion completes"
                        ),
                    }));
                }
                // F207-C: Recovery check via the unified ledger.
                if matches!(
                    self.extent_inflight_op(extent_id),
                    Some(crate::extent_inflight::ExtentOpKind::Recovery)
                ) {
                    return Ok(rkyv_encode(&SyncPartitionVpRefsResp {
                        code: CODE_PRECONDITION,
                        message: format!(
                            "extent {extent_id} has in-flight recovery; \
                             defer sync_partition_vp_refs until recovery completes"
                        ),
                    }));
                }
            }
            let puts = Self::preview_partition_vp_refs_apply(&s, &snapshot);
            let evs: HashMap<u64, u64> = deltas
                .keys()
                .filter_map(|&eid| s.extents.get(&eid).map(|ex| (eid, ex.eversion)))
                .collect();
            (puts, evs)
        };

        // F210-A2 verify-BEFORE-mirror (replaces the post-F147-A
        // verify-after-mirror form). If any touched extent's eversion
        // drifted between the snapshot above and now, the etcd write
        // we'd otherwise make is computed from a stale base; refuse
        // before committing to etcd.
        {
            let s = self.store.inner.borrow();
            for (&extent_id, &pre_ev) in &pre_eversion {
                if let Some(live) = s.extents.get(&extent_id) {
                    if live.eversion != pre_ev {
                        return Ok(rkyv_encode(&SyncPartitionVpRefsResp {
                            code: CODE_PRECONDITION,
                            message: format!(
                                "extent {extent_id} eversion changed ({pre_ev} → {}) \
                                 during vp_refs build; PS must retry",
                                live.eversion
                            ),
                        }));
                    }
                }
            }
        }

        if let Err(err) = self.mirror_partition_vp_refs(&snapshot, &extent_puts).await {
            return Ok(rkyv_encode(&SyncPartitionVpRefsResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }
        {
            let mut s = self.store.inner.borrow_mut();
            Self::apply_partition_vp_refs(&mut s, snapshot);
        }
        Ok(rkyv_encode(&SyncPartitionVpRefsResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    // ── F211-B / F211-C / F211-H / F211-I admin & health RPCs ──────────────
    //
    // All admin-mutating handlers (F211-C, force_ec_convert, future
    // force_abandon_ec_marker) wrap their result in `append_audit`
    // (F211-I). The audit append is best-effort: a failed audit write
    // logs WARN but doesn't surface to the caller (the primary
    // operation already succeeded).

    pub async fn handle_list_node_states(&self, _payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&ListNodeStatesResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                nodes: vec![],
            }));
        }
        let (nodes_meta, overrides, snapshot) = {
            let s = self.store.inner.borrow();
            let nodes: Vec<(u64, String)> = s
                .nodes
                .iter()
                .map(|(id, n)| (*id, n.address.clone()))
                .collect();
            let overrides = self.node_overrides.borrow().clone();
            let snap = self.node_states.borrow().snapshot();
            (nodes, overrides, snap)
        };
        // Merge: every registered node MUST appear (even if the tracker
        // has no entry yet). Tracker-only entries (e.g. for a node
        // dropped from `s.nodes` mid-failover) are dropped here.
        let snap_map: HashMap<u64, (crate::node_state::NodeAutoState, Option<u64>)> = snapshot
            .into_iter()
            .map(|(id, st, secs)| (id, (st, secs)))
            .collect();
        let mut out: Vec<NodeStateEntry> = nodes_meta
            .into_iter()
            .map(|(node_id, address)| {
                let (auto_state, last_secs) = snap_map
                    .get(&node_id)
                    .copied()
                    .unwrap_or((crate::node_state::NodeAutoState::Online, None));
                let auto_state_byte = match auto_state {
                    crate::node_state::NodeAutoState::Online => NODE_AUTO_STATE_ONLINE,
                    crate::node_state::NodeAutoState::Suspected { .. } => NODE_AUTO_STATE_SUSPECTED,
                    crate::node_state::NodeAutoState::Suspend => NODE_AUTO_STATE_SUSPEND,
                };
                let suspected_age = match auto_state {
                    crate::node_state::NodeAutoState::Suspected { since } => {
                        since.elapsed().as_secs()
                    }
                    _ => 0,
                };
                let ovr = overrides.get(&node_id);
                NodeStateEntry {
                    node_id,
                    address,
                    auto_state: auto_state_byte,
                    last_heartbeat_secs_ago: last_secs.unwrap_or(u64::MAX),
                    suspected_age_secs: suspected_age,
                    override_kind: ovr.map(|o| o.kind).unwrap_or(NODE_OVERRIDE_NONE),
                    override_reason: ovr.map(|o| o.reason.clone()).unwrap_or_default(),
                    override_set_by: ovr.map(|o| o.set_by.clone()).unwrap_or_default(),
                    override_set_at: ovr.map(|o| o.set_at).unwrap_or(0),
                    override_expire_at: ovr.map(|o| o.expire_at).unwrap_or(0),
                }
            })
            .collect();
        out.sort_by_key(|e| e.node_id);
        Ok(rkyv_encode(&ListNodeStatesResp {
            code: CODE_OK,
            message: String::new(),
            nodes: out,
        }))
    }

    pub async fn handle_extent_health_report(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&ExtentHealthResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                extents: vec![],
            }));
        }
        let req: ExtentHealthReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let filter: HashSet<u64> = req.node_id_filter.iter().copied().collect();
        let (extents, overrides, snapshot) = {
            let s = self.store.inner.borrow();
            let extents: Vec<MgrExtentInfo> = s.extents.values().cloned().collect();
            let overrides = self.node_overrides.borrow().clone();
            let snap = self.node_states.borrow().snapshot();
            (extents, overrides, snap)
        };
        let snap_map: HashMap<u64, crate::node_state::NodeAutoState> =
            snapshot.into_iter().map(|(id, st, _)| (id, st)).collect();
        let mut out: Vec<ExtentHealth> = Vec::new();
        for ex in extents {
            let copies = Self::extent_nodes(&ex);
            let mut slots: Vec<ExtentSlotHealth> = Vec::with_capacity(copies.len());
            let mut any_match = filter.is_empty();
            let mut any_unhealthy = false;
            for (idx, &node_id) in copies.iter().enumerate() {
                if filter.contains(&node_id) {
                    any_match = true;
                }
                let bit = 1u32 << idx;
                let avali = (ex.avali & bit) != 0;
                let auto = snap_map
                    .get(&node_id)
                    .copied()
                    .unwrap_or(crate::node_state::NodeAutoState::Online);
                let auto_byte = match auto {
                    crate::node_state::NodeAutoState::Online => NODE_AUTO_STATE_ONLINE,
                    crate::node_state::NodeAutoState::Suspected { .. } => NODE_AUTO_STATE_SUSPECTED,
                    crate::node_state::NodeAutoState::Suspend => NODE_AUTO_STATE_SUSPEND,
                };
                let ovr = overrides
                    .get(&node_id)
                    .map(|o| o.kind)
                    .unwrap_or(NODE_OVERRIDE_NONE);
                if !avali || auto_byte != NODE_AUTO_STATE_ONLINE || ovr != NODE_OVERRIDE_NONE {
                    any_unhealthy = true;
                }
                slots.push(ExtentSlotHealth {
                    slot_index: idx as u32,
                    node_id,
                    avali,
                    auto_state: auto_byte,
                    override_kind: ovr,
                });
            }
            if !any_match {
                continue;
            }
            if !req.include_healthy && !any_unhealthy && filter.is_empty() {
                continue;
            }
            out.push(ExtentHealth {
                extent_id: ex.extent_id,
                eversion: ex.eversion,
                sealed_length: ex.sealed_length,
                ec_converted: ex.ec_converted,
                slots,
                unhealthy: any_unhealthy,
            });
        }
        out.sort_by_key(|e| e.extent_id);
        Ok(rkyv_encode(&ExtentHealthResp {
            code: CODE_OK,
            message: String::new(),
            extents: out,
        }))
    }

    pub async fn handle_list_ec_inflight_markers(&self, _payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&ListEcInflightMarkersResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                markers: vec![],
            }));
        }
        let snapshot = self.node_states.borrow().snapshot();
        let snap_map: HashMap<u64, crate::node_state::NodeAutoState> =
            snapshot.into_iter().map(|(id, st, _)| (id, st)).collect();
        let overrides = self.node_overrides.borrow().clone();
        let now_s = Self::epoch_seconds();
        let mut markers: Vec<InflightWithCoordState> = Vec::new();
        for (eid, rec) in self.inflight.borrow().iter() {
            let Some((kind, payload)) = rec.unpack() else {
                continue;
            };
            if kind != crate::extent_inflight::ExtentOpKind::ConvertToEc {
                continue;
            }
            let crate::extent_inflight::ExtentOpPayload::ConvertToEc(p) = payload else {
                continue;
            };
            let coord = p.target_nodes.first().copied().unwrap_or(0);
            let auto = snap_map
                .get(&coord)
                .copied()
                .unwrap_or(crate::node_state::NodeAutoState::Online);
            let auto_byte = match auto {
                crate::node_state::NodeAutoState::Online => NODE_AUTO_STATE_ONLINE,
                crate::node_state::NodeAutoState::Suspected { .. } => NODE_AUTO_STATE_SUSPECTED,
                crate::node_state::NodeAutoState::Suspend => NODE_AUTO_STATE_SUSPEND,
            };
            let ovr = overrides
                .get(&coord)
                .map(|o| o.kind)
                .unwrap_or(NODE_OVERRIDE_NONE);
            markers.push(InflightWithCoordState {
                extent_id: *eid,
                coord_node_id: coord,
                coord_auto_state: auto_byte,
                coord_override_kind: ovr,
                target_nodes: p.target_nodes.clone(),
                data_shards: p.data_shards,
                new_eversion: p.new_eversion,
                started_at: rec.started_at,
                age_secs: now_s.saturating_sub(rec.started_at),
            });
        }
        markers.sort_by_key(|m| m.extent_id);
        Ok(rkyv_encode(&ListEcInflightMarkersResp {
            code: CODE_OK,
            message: String::new(),
            markers,
        }))
    }

    // ── F211-C admin RPCs ────────────────────────────────────────────────

    pub async fn handle_fence_node(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }
        let req: FenceNodeReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let result = self.fence_node_impl(&req).await;
        let (code, message) = match &result {
            Ok(()) => (CODE_OK, String::new()),
            Err(e) => (Self::err_to_code(e), e.to_string()),
        };
        self.append_audit(MgrAuditEntry {
            op: AUDIT_OP_FENCE_NODE,
            node_id: req.node_id,
            extent_id: 0,
            by: req.set_by.clone(),
            reason: req.reason.clone(),
            result_code: code,
            result_message: message.clone(),
            ts_ns: 0,
        })
        .await;
        Ok(rkyv_encode(&CodeResp { code, message }))
    }

    pub async fn handle_set_node_maintenance(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }
        let req: SetNodeMaintenanceReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        // F211-C: zombie defense — refuse if node was decommissioned.
        if self.decommissioned.borrow().contains_key(&req.node_id) {
            let msg = format!(
                "node {} was previously decommissioned; cannot mark maintenance",
                req.node_id
            );
            self.append_audit(MgrAuditEntry {
                op: AUDIT_OP_SET_NODE_MAINTENANCE,
                node_id: req.node_id,
                extent_id: 0,
                by: req.set_by.clone(),
                reason: req.reason.clone(),
                result_code: CODE_PRECONDITION,
                result_message: msg.clone(),
                ts_ns: 0,
            })
            .await;
            return Ok(rkyv_encode(&CodeResp {
                code: CODE_PRECONDITION,
                message: msg,
            }));
        }
        let ovr = MgrNodeOverride {
            node_id: req.node_id,
            kind: NODE_OVERRIDE_MAINTENANCE,
            set_at: Self::epoch_seconds(),
            set_by: req.set_by.clone(),
            reason: req.reason.clone(),
            expire_at: req.expire_at,
        };
        let key = format!("{}{}", crate::NODE_OVERRIDE_PREFIX, req.node_id);
        let value = rkyv_encode(&ovr).to_vec();
        if let Some(etcd) = &self.etcd {
            if let Err(err) = etcd.put_msgs_txn(vec![(key, value)]).await {
                self.append_audit(MgrAuditEntry {
                    op: AUDIT_OP_SET_NODE_MAINTENANCE,
                    node_id: req.node_id,
                    extent_id: 0,
                    by: req.set_by.clone(),
                    reason: req.reason.clone(),
                    result_code: Self::err_to_code(&err),
                    result_message: err.to_string(),
                    ts_ns: 0,
                })
                .await;
                return Ok(rkyv_encode(&CodeResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                }));
            }
        }
        self.node_overrides.borrow_mut().insert(req.node_id, ovr);
        self.append_audit(MgrAuditEntry {
            op: AUDIT_OP_SET_NODE_MAINTENANCE,
            node_id: req.node_id,
            extent_id: 0,
            by: req.set_by.clone(),
            reason: req.reason.clone(),
            result_code: CODE_OK,
            result_message: String::new(),
            ts_ns: 0,
        })
        .await;
        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    pub async fn handle_clear_node_override(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }
        let req: ClearNodeOverrideReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let key = format!("{}{}", crate::NODE_OVERRIDE_PREFIX, req.node_id);
        if let Some(etcd) = &self.etcd {
            if let Err(err) = etcd.put_and_delete_txn(Vec::new(), vec![key]).await {
                self.append_audit(MgrAuditEntry {
                    op: AUDIT_OP_CLEAR_NODE_OVERRIDE,
                    node_id: req.node_id,
                    extent_id: 0,
                    by: req.set_by.clone(),
                    reason: String::new(),
                    result_code: Self::err_to_code(&err),
                    result_message: err.to_string(),
                    ts_ns: 0,
                })
                .await;
                return Ok(rkyv_encode(&CodeResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                }));
            }
        }
        self.node_overrides.borrow_mut().remove(&req.node_id);
        self.append_audit(MgrAuditEntry {
            op: AUDIT_OP_CLEAR_NODE_OVERRIDE,
            node_id: req.node_id,
            extent_id: 0,
            by: req.set_by.clone(),
            reason: String::new(),
            result_code: CODE_OK,
            result_message: String::new(),
            ts_ns: 0,
        })
        .await;
        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    pub async fn handle_remove_node(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&RemoveNodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                blocking_extent_ids: vec![],
                blocking_marker_extent_ids: vec![],
            }));
        }
        let req: RemoveNodeReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let result = self.remove_node_impl(&req).await;
        let (code, message, ext_blockers, mark_blockers) = match result {
            Ok(()) => (CODE_OK, String::new(), vec![], vec![]),
            Err((c, m, e, k)) => (c, m, e, k),
        };
        self.append_audit(MgrAuditEntry {
            op: AUDIT_OP_REMOVE_NODE,
            node_id: req.node_id,
            extent_id: 0,
            by: req.set_by.clone(),
            reason: String::new(),
            result_code: code,
            result_message: message.clone(),
            ts_ns: 0,
        })
        .await;
        Ok(rkyv_encode(&RemoveNodeResp {
            code,
            message,
            blocking_extent_ids: ext_blockers,
            blocking_marker_extent_ids: mark_blockers,
        }))
    }

    // F211-C inner helpers — separated so the handlers' audit-wrap is
    // tight + the unit-test surface is direct.

    async fn fence_node_impl(&self, req: &FenceNodeReq) -> Result<(), AppError> {
        if !self.store.inner.borrow().nodes.contains_key(&req.node_id) {
            return Err(AppError::NotFound(format!(
                "node {} not registered",
                req.node_id
            )));
        }
        // F211-C #5 capacity precheck (unless --force).
        if !req.force {
            self.check_capacity_for_fence(req.node_id)?;
        }
        // Persist the override.
        let ovr = MgrNodeOverride {
            node_id: req.node_id,
            kind: NODE_OVERRIDE_FENCED,
            set_at: Self::epoch_seconds(),
            set_by: req.set_by.clone(),
            reason: req.reason.clone(),
            expire_at: 0,
        };
        let key = format!("{}{}", crate::NODE_OVERRIDE_PREFIX, req.node_id);
        let value = rkyv_encode(&ovr).to_vec();
        if let Some(etcd) = &self.etcd {
            etcd.put_msgs_txn(vec![(key, value)]).await?;
        }
        self.node_overrides.borrow_mut().insert(req.node_id, ovr);
        // BUG #3 Layer B fix: do NOT bump partition owner-lock revisions when
        // fencing an EN data node. The owner-lock revision is the PARTITION
        // OWNER's (PS) token for split-brain prevention; an EN data node is
        // never a partition owner. The old F211-D bump
        // (`bump_owner_revisions_for_node`) walked every partition whose
        // log/row/meta stream merely had a REPLICA on the fenced node and
        // bumped THAT partition's owner revision — fencing out the legitimate
        // PS owner (which holds its acquire-time revision and never
        // re-acquires), so the PS's next append got CODE_LOCKED_BY_OTHER and
        // `partition_loop` self-poisoned + reopen-thrashed (the chaos seed=6
        // wedge after the Layer-A seal fix). It was also redundant: a fenced
        // EN is handled by the normal append-fail → seal-over-reachable (Layer
        // A) → alloc-new-extent path, and post-recovery topology changes are
        // picked up via EVERSION refresh, not owner-revision. Real split-brain
        // protection is the NEW PS's `acquire_owner_lock` on takeover (higher
        // revision), unaffected by this removal — see
        // `system_locked_by_other.rs::owner_lock_fencing_rejects_stale_revision`.
        // F211-F: auto-abandon EC convert markers whose coord matches
        // the freshly-fenced node.
        let _ = self.auto_abandon_for_fenced_node(req.node_id).await;
        Ok(())
    }

    /// F211-C #5: refuse fence if the cluster doesn't have enough
    /// remaining free space to absorb the node's data. Returns
    /// Precondition when the safety factor (default 1.2x) is not met.
    fn check_capacity_for_fence(&self, node_id: u64) -> Result<(), AppError> {
        let s = self.store.inner.borrow();
        // Sum sealed_length of extents that have any slot on this node.
        let mut data_to_migrate: u64 = 0;
        for ex in s.extents.values() {
            if Self::extent_nodes(ex).contains(&node_id) {
                // Per-shard size for EC, full size for replication.
                let shard_size =
                    if ex.ec_converted && !ex.replicates.is_empty() && !ex.parity.is_empty() {
                        let k = ex.replicates.len() as u64;
                        ex.sealed_length.div_ceil(k.max(1))
                    } else {
                        ex.sealed_length
                    };
                data_to_migrate = data_to_migrate.saturating_add(shard_size);
            }
        }
        // Estimate remaining capacity from disk metadata; treat missing
        // sizes as 0 (conservative — refuses if we have no signal).
        // The MgrDiskInfo struct doesn't track free bytes today, so we
        // do a coarse "is there at least one online disk on a different
        // node?" check — recovery dispatch needs >= 1 healthy target.
        let has_alt_targets = s.nodes.values().any(|n| {
            n.node_id != node_id
                && n.disks
                    .iter()
                    .any(|did| s.disks.get(did).map(|d| d.online).unwrap_or(false))
        });
        if !has_alt_targets && data_to_migrate > 0 {
            return Err(AppError::Precondition(format!(
                "no healthy target nodes available to receive ~{} bytes from node {} (use --force to override)",
                data_to_migrate, node_id
            )));
        }
        Ok(())
    }

    // BUG #3 Layer B: `bump_owner_revisions_for_node` (F211-D) was removed.
    // It bumped the PARTITION owner-lock revision of every partition whose
    // streams merely had a REPLICA on a fenced EN data node, fencing out the
    // legitimate PS owner (→ CODE_LOCKED_BY_OTHER → partition self-poison +
    // reopen-thrash). It was redundant (fenced-EN handling = append-fail →
    // seal-over-reachable → realloc + eversion topology refresh) and harmful.
    // Real split-brain protection is the new PS's acquire_owner_lock on
    // takeover, not an EN fence. See the removal note in `fence_node_impl`.

    async fn remove_node_impl(
        &self,
        req: &RemoveNodeReq,
    ) -> Result<(), (u8, String, Vec<u64>, Vec<u64>)> {
        let cur = self.node_overrides.borrow().get(&req.node_id).cloned();
        let is_fenced = matches!(cur.as_ref().map(|o| o.kind), Some(NODE_OVERRIDE_FENCED));
        if !is_fenced {
            return Err((
                CODE_PRECONDITION,
                format!("node {} must be Fenced before remove", req.node_id),
                vec![],
                vec![],
            ));
        }
        // Scan for residual references.
        let (ext_refs, marker_refs) = {
            let s = self.store.inner.borrow();
            let mut ext_refs: Vec<u64> = Vec::new();
            for ex in s.extents.values() {
                if Self::extent_nodes(ex).contains(&req.node_id) {
                    ext_refs.push(ex.extent_id);
                }
            }
            let mut marker_refs: Vec<u64> = Vec::new();
            for (eid, rec) in self.inflight.borrow().iter() {
                if let Some((
                    crate::extent_inflight::ExtentOpKind::ConvertToEc,
                    crate::extent_inflight::ExtentOpPayload::ConvertToEc(p),
                )) = rec.unpack()
                {
                    if p.target_nodes.contains(&req.node_id) {
                        marker_refs.push(*eid);
                    }
                }
            }
            ext_refs.sort();
            marker_refs.sort();
            (ext_refs, marker_refs)
        };
        if !ext_refs.is_empty() || !marker_refs.is_empty() {
            return Err((
                CODE_PRECONDITION,
                format!(
                    "node {} still referenced by {} extents and {} EC markers",
                    req.node_id,
                    ext_refs.len(),
                    marker_refs.len()
                ),
                ext_refs,
                marker_refs,
            ));
        }
        // All clear — persist tombstone + delete override + delete
        // nodes/<id> + delete disks/<id>. Single atomic txn.
        let now = Self::epoch_seconds();
        let tomb = MgrNodeOverride {
            node_id: req.node_id,
            kind: NODE_OVERRIDE_FENCED,
            set_at: now,
            set_by: req.set_by.clone(),
            reason: "removed".to_string(),
            expire_at: 0,
        };
        let tomb_key = format!("{}{}", crate::DECOMMISSIONED_PREFIX, req.node_id);
        let tomb_val = rkyv_encode(&tomb).to_vec();
        let override_key = format!("{}{}", crate::NODE_OVERRIDE_PREFIX, req.node_id);
        let node_key = format!("nodes/{}", req.node_id);
        let disk_ids: Vec<u64> = self
            .store
            .inner
            .borrow()
            .nodes
            .get(&req.node_id)
            .map(|n| n.disks.clone())
            .unwrap_or_default();
        let disk_keys: Vec<String> = disk_ids.iter().map(|d| format!("disks/{}", d)).collect();
        if let Some(etcd) = &self.etcd {
            let mut deletes = vec![override_key.clone(), node_key.clone()];
            deletes.extend(disk_keys.iter().cloned());
            if let Err(e) = etcd
                .put_and_delete_txn(vec![(tomb_key, tomb_val)], deletes)
                .await
            {
                return Err((Self::err_to_code(&e), e.to_string(), vec![], vec![]));
            }
        }
        // Apply to in-memory.
        {
            let mut s = self.store.inner.borrow_mut();
            s.nodes.remove(&req.node_id);
            for did in &disk_ids {
                s.disks.remove(did);
            }
        }
        self.node_overrides.borrow_mut().remove(&req.node_id);
        self.node_states.borrow_mut().drop_node(req.node_id);
        self.decommissioned.borrow_mut().insert(req.node_id, tomb);
        Ok(())
    }

    // ── F211-H recovery stats ────────────────────────────────────────────

    pub async fn handle_recovery_stats(&self, _payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&RecoveryStatsResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                ..Default::default()
            }));
        }
        let l = self.recovery_limiter.borrow();
        let (src, tgt) = l.snapshot();
        let backoff: Vec<RecoveryBackoffEntry> = l
            .backoff_snapshot()
            .into_iter()
            .map(
                |(
                    extent_id,
                    slot,
                    consecutive_failures,
                    last_attempt_at,
                    next_retry_at,
                    reason,
                )| {
                    RecoveryBackoffEntry {
                        extent_id,
                        slot,
                        consecutive_failures,
                        last_attempt_at,
                        next_retry_at,
                        reason,
                    }
                },
            )
            .collect();
        Ok(rkyv_encode(&RecoveryStatsResp {
            code: CODE_OK,
            message: String::new(),
            global_inflight: l.global_inflight,
            max_global: l.max_global,
            max_per_source: l.max_per_source,
            max_per_target: l.max_per_target,
            per_source: src,
            per_target: tgt,
            backoff_entries: l.backoff.len() as u32,
            backoff,
        }))
    }

    // ── F211-I audit log query ───────────────────────────────────────────

    pub async fn handle_query_audit_log(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&QueryAuditLogResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                entries: vec![],
            }));
        }
        let req: QueryAuditLogReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let entries = self
            .query_audit(
                req.op_filter,
                req.node_id_filter,
                req.since_ts_s,
                req.until_ts_s,
                req.limit,
            )
            .await;
        Ok(rkyv_encode(&QueryAuditLogResp {
            code: CODE_OK,
            message: String::new(),
            entries,
        }))
    }

    // ── F-ioring-lease-1: inode lease handlers ─────────────────────────────
    //
    // Plan reference: `docs/autumn_fs_lease_plan.md`. Manager is the
    // single decision-maker (§6 invariant 1); writer leases are
    // persisted to etcd (§3.1) while reader leases stay in-memory only
    // (§7 "lease 数量爆炸"). Every etcd write routes through
    // `put_msgs_txn` / `put_and_delete_txn` so the F149 leader fence
    // travels with it.

    pub async fn handle_acquire_lease(&self, payload: Bytes) -> HandlerResult {
        let req: AcquireLeaseReq = rkyv_decode(&payload)
            .map_err(|e| (StatusCode::InvalidArgument, format!("decode: {e}")))?;
        // Etcd write needs leader status; non-leader rejects with
        // NOT_LEADER so the client retries against the new leader
        // (matches MSG_GET_REGIONS / MSG_ACQUIRE_OWNER_LOCK pattern).
        if self.etcd.is_some() && !self.leader.get() {
            return Ok(rkyv_encode(&AcquireLeaseResp {
                code: CODE_NOT_LEADER,
                message: "not leader".to_string(),
                lease: None,
            }));
        }

        let now = Instant::now();
        let outcome = {
            let mut reg = self.inode_leases.borrow_mut();
            reg.acquire(&req.client, req.ino, req.mode, now)
        };

        match outcome {
            crate::inode_lease::AcquireOutcome::Granted {
                version,
                writer_present,
                ttl_secs,
            } => {
                // Etcd-first: writer leases persist; reader leases don't.
                if req.mode == LEASE_MODE_WRITE {
                    let record = self
                        .inode_leases
                        .borrow()
                        .writer_record(req.ino)
                        .expect("writer just acquired");
                    let key = format!("{}{}", crate::INODE_LEASES_PREFIX, req.ino);
                    let value = rkyv_encode(&record).to_vec();
                    if let Some(etcd) = &self.etcd {
                        if let Err(e) = etcd.put_msgs_txn(vec![(key, value)]).await {
                            // Roll back the in-memory grant — the
                            // client never saw the OK, and the next
                            // tick must not see a phantom writer.
                            let _ = self
                                .inode_leases
                                .borrow_mut()
                                .release(&req.client, req.ino);
                            return Ok(rkyv_encode(&AcquireLeaseResp {
                                code: Self::err_to_code(&e),
                                message: e.to_string(),
                                lease: None,
                            }));
                        }
                    }
                }
                Ok(rkyv_encode(&AcquireLeaseResp {
                    code: CODE_OK,
                    message: String::new(),
                    lease: Some(MgrInodeLeaseInfo {
                        ino: req.ino,
                        version,
                        writer_present,
                        ttl_secs,
                    }),
                }))
            }
            crate::inode_lease::AcquireOutcome::WriteConflict {
                held_by_kind,
                held_by_host,
            } => Ok(rkyv_encode(&AcquireLeaseResp {
                code: CODE_PRECONDITION,
                message: format!(
                    "writer lease held by kind={held_by_kind} host={held_by_host}"
                ),
                lease: None,
            })),
            crate::inode_lease::AcquireOutcome::InvalidMode => Ok(rkyv_encode(&AcquireLeaseResp {
                code: CODE_INVALID_ARGUMENT,
                message: format!("invalid lease mode {}", req.mode),
                lease: None,
            })),
        }
    }

    pub async fn handle_release_lease(&self, payload: Bytes) -> HandlerResult {
        let req: ReleaseLeaseReq = rkyv_decode(&payload)
            .map_err(|e| (StatusCode::InvalidArgument, format!("decode: {e}")))?;
        if self.etcd.is_some() && !self.leader.get() {
            return Ok(rkyv_encode(&ReleaseLeaseResp {
                code: CODE_NOT_LEADER,
                message: "not leader".to_string(),
                new_version: None,
            }));
        }

        let outcome = {
            let mut reg = self.inode_leases.borrow_mut();
            reg.release(&req.client, req.ino)
        };

        match outcome {
            crate::inode_lease::ReleaseOutcome::WriterClosed { new_version } => {
                // Writer-close → delete the persisted record so
                // failover replay doesn't resurrect a stale writer.
                let key = format!("{}{}", crate::INODE_LEASES_PREFIX, req.ino);
                if let Some(etcd) = &self.etcd {
                    if let Err(e) = etcd.put_and_delete_txn(vec![], vec![key]).await {
                        // The in-memory release already fired (version
                        // bumped, readers notified). The persisted
                        // record is stale until the next revoke tick.
                        // Acceptable: the new leader's `tick(now)` will
                        // expire it within `lease_ttl` even if it
                        // never sees the delete.
                        tracing::warn!(
                            ino = req.ino,
                            error = %e,
                            "F-ioring-lease-1: writer-close etcd delete failed; TTL revoke will clean up"
                        );
                    }
                }
                Ok(rkyv_encode(&ReleaseLeaseResp {
                    code: CODE_OK,
                    message: String::new(),
                    new_version: Some(new_version),
                }))
            }
            crate::inode_lease::ReleaseOutcome::ReaderReleased => {
                Ok(rkyv_encode(&ReleaseLeaseResp {
                    code: CODE_OK,
                    message: String::new(),
                    new_version: None,
                }))
            }
            crate::inode_lease::ReleaseOutcome::NotHeld => Ok(rkyv_encode(&ReleaseLeaseResp {
                code: CODE_OK,
                message: "not held (idempotent)".to_string(),
                new_version: None,
            })),
        }
    }

    pub async fn handle_heartbeat_lease(&self, payload: Bytes) -> HandlerResult {
        let req: HeartbeatLeaseReq = rkyv_decode(&payload)
            .map_err(|e| (StatusCode::InvalidArgument, format!("decode: {e}")))?;
        // Heartbeats don't write etcd directly — the writer's
        // `expires_at` in the persisted record is refreshed lazily,
        // i.e. on the next AcquireLease or by the next failover
        // (writer must re-acquire after a failover regardless).
        // Heartbeat is therefore safe to serve on a follower IF we
        // ever decide to; current design is leader-only to avoid
        // serving stale state.
        if self.etcd.is_some() && !self.leader.get() {
            return Ok(rkyv_encode(&HeartbeatLeaseResp {
                code: CODE_NOT_LEADER,
                message: "not leader".to_string(),
                lease: None,
            }));
        }

        let now = Instant::now();
        let outcome = {
            let mut reg = self.inode_leases.borrow_mut();
            reg.heartbeat(&req.client, req.ino, now)
        };
        match outcome {
            crate::inode_lease::HeartbeatOutcome::Renewed {
                version,
                writer_present,
                ttl_secs,
            } => Ok(rkyv_encode(&HeartbeatLeaseResp {
                code: CODE_OK,
                message: String::new(),
                lease: Some(MgrInodeLeaseInfo {
                    ino: req.ino,
                    version,
                    writer_present,
                    ttl_secs,
                }),
            })),
            crate::inode_lease::HeartbeatOutcome::NotHeld => {
                Ok(rkyv_encode(&HeartbeatLeaseResp {
                    code: CODE_NOT_FOUND,
                    message: "lease not held".to_string(),
                    lease: None,
                }))
            }
        }
    }

    pub async fn handle_poll_invalidations(&self, payload: Bytes) -> HandlerResult {
        let req: PollInvalidationsReq = rkyv_decode(&payload)
            .map_err(|e| (StatusCode::InvalidArgument, format!("decode: {e}")))?;
        // Followers carry no state; surface as NOT_LEADER so the
        // client reconnects to the new leader (and per plan §6.4
        // invalidates all cache on reconnect).
        if self.etcd.is_some() && !self.leader.get() {
            return Ok(rkyv_encode(&PollInvalidationsResp {
                code: CODE_NOT_LEADER,
                message: "not leader".to_string(),
                events: Vec::new(),
            }));
        }

        // F-ioring-lease-3: long-poll. Atomic drain-or-park: returns
        // queued events immediately if any; else installs a waker
        // and returns the matching receiver. We await it with a
        // bounded timeout so an idle client still round-trips at
        // most once per `LONG_POLL_WAIT` (keeps heartbeats alive
        // even on connections that prefer to coalesce traffic).
        let (events, overflowed, parked) = {
            let mut reg = self.inode_leases.borrow_mut();
            reg.drain_or_park(&req.client)
        };
        let (events, overflowed) = if let Some(rx) = parked {
            // No events — wait up to LONG_POLL_WAIT for one to arrive
            // or for the waker to fire.
            const LONG_POLL_WAIT: Duration = Duration::from_secs(10);
            let timer = compio::time::sleep(LONG_POLL_WAIT);
            futures::pin_mut!(timer);
            let _ = futures::future::select(rx, timer).await;
            // Re-drain. Either branch is acceptable: the waker fires
            // → events are queued; the timer fires → still empty (the
            // poll-loop on the client side reissues immediately, no
            // round-trip cost beyond the connection's keep-alive).
            let mut reg = self.inode_leases.borrow_mut();
            reg.drain_invalidations(&req.client)
        } else {
            (events, overflowed)
        };

        // Overflow ⇒ tell the client to wholesale-invalidate via a
        // sentinel MetaChanged event with ino=0. The F-ioring-lease-3
        // daemon poll-loop turns this into a session-wide cache drop
        // (plan §6.4 "subscribe disconnect = invalidate everything").
        let mut out_events = events;
        if overflowed {
            out_events.push(MgrInvalidation {
                ino: 0,
                version: 0,
                kind: LEASE_INVAL_META_CHANGED,
            });
        }
        Ok(rkyv_encode(&PollInvalidationsResp {
            code: CODE_OK,
            message: String::new(),
            events: out_events,
        }))
    }
}

#[cfg(test)]
mod f227_commit_seal_tests {
    use crate::AutumnManager;
    use std::collections::{HashMap, HashSet};

    // Slots: idx 0 -> node 1, idx 1 -> node 3, idx 2 -> node 5.
    fn members3() -> Vec<(usize, u64)> {
        vec![(0, 1u64), (1, 3u64), (2, 5u64)]
    }

    #[test]
    fn all_committed_respond_takes_min_all_avali() {
        let m = members3();
        let rec = HashSet::new();
        let mut resp = HashMap::new();
        resp.insert(1u64, 20_000_000u32);
        resp.insert(3u64, 20_000_000u32);
        resp.insert(5u64, 18_000_000u32);
        let (len, avali) = AutumnManager::compute_commit_seal(&m, &rec, &resp, 1).unwrap();
        assert_eq!(len, 18_000_000, "seal = min over all committed members");
        assert_eq!(avali, 0b111);
    }

    #[test]
    fn excludes_catching_up_member_does_not_crater_min() {
        // F227 core invariant: slot 5 is catching-up (in-flight Recovery).
        // It holds only a partial replica, so it must NOT contribute to the
        // min. The seal = min over committed members {1,3} = 20 MB, NOT the
        // short value a catching-up replica would report (the production bug
        // cratered sealed_length to a recovery target's partial length).
        let m = members3();
        let rec: HashSet<u64> = [5u64].into_iter().collect();
        let mut resp = HashMap::new();
        resp.insert(1u64, 20_000_000u32);
        resp.insert(3u64, 20_000_000u32);
        // node 5 deliberately absent (would have reported a short length).
        let (len, avali) = AutumnManager::compute_commit_seal(&m, &rec, &resp, 1).unwrap();
        assert_eq!(len, 20_000_000);
        assert_eq!(avali, 0b011, "slot 2 (node 5) avali bit stays unset");
    }

    #[test]
    fn seals_over_reachable_when_a_committed_member_is_silent() {
        // WAS seal-over-reachable (bug #3 fix): a committed member that is
        // unreachable (e.g. a kill+restarted laggard not yet in `recovering`)
        // no longer blocks the seal. With floor 1 and {1,3} reachable, seal at
        // min(1,3) = 20 MB (which is >= acked under all-replica-ACK), and
        // node 5's avali bit stays UNSET so it is reconciled out of band.
        let m = members3();
        let rec = HashSet::new();
        let mut resp = HashMap::new();
        resp.insert(1u64, 20_000_000u32);
        resp.insert(3u64, 20_000_000u32);
        // node 5 committed but silent (unreachable).
        let (len, avali) = AutumnManager::compute_commit_seal(&m, &rec, &resp, 1).unwrap();
        assert_eq!(
            len, 20_000_000,
            "seal = min over the REACHABLE committed members"
        );
        assert_eq!(
            avali, 0b011,
            "silent node 5's avali bit stays unset → reconcile later"
        );
    }

    #[test]
    fn refuses_when_fewer_than_floor_members_reachable() {
        // The floor still gates: with floor 2 but only node 1 reachable
        // (node 3 also silent), we cannot establish a durable-enough seal.
        let m = members3();
        let rec = HashSet::new();
        let mut resp = HashMap::new();
        resp.insert(1u64, 20_000_000u32);
        // nodes 3 and 5 silent → only 1 reachable < floor 2.
        assert!(AutumnManager::compute_commit_seal(&m, &rec, &resp, 2).is_err());
        // floor 1 is satisfied by the single reachable member.
        assert!(AutumnManager::compute_commit_seal(&m, &rec, &resp, 1).is_ok());
    }

    #[test]
    fn refuses_below_durability_floor() {
        // All members catching-up -> 0 committed -> below floor 1.
        let m = members3();
        let rec: HashSet<u64> = [1u64, 3, 5].into_iter().collect();
        let resp = HashMap::new();
        assert!(AutumnManager::compute_commit_seal(&m, &rec, &resp, 1).is_err());
    }

    #[test]
    fn floor_gates_committed_member_count() {
        // 2 committed members (5 catching-up) both respond.
        let m = members3();
        let rec: HashSet<u64> = [5u64].into_iter().collect();
        let mut resp = HashMap::new();
        resp.insert(1u64, 20u32);
        resp.insert(3u64, 20u32);
        assert!(AutumnManager::compute_commit_seal(&m, &rec, &resp, 3).is_err());
        assert!(AutumnManager::compute_commit_seal(&m, &rec, &resp, 2).is_ok());
    }
}
// end of rpc_handlers.rs
