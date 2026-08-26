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

/// #6: RAII removal of a partition from `AutumnManager.split_inflight` on every
/// exit path of `handle_multi_modify_split` (success + all early-return errors).
struct SplitInflightGuard {
    set: Rc<std::cell::RefCell<HashSet<u64>>>,
    part_id: u64,
}
impl Drop for SplitInflightGuard {
    fn drop(&mut self) {
        self.set.borrow_mut().remove(&self.part_id);
    }
}

/// The PS-side `FREEZE_TTL` (`crates/partition-server/src/lib.rs`, 30 s at time
/// of writing — this const MUST stay comfortably below it; if FREEZE_TTL
/// changes, revisit this) auto-unfreezes a merge/split freeze after that long.
/// The manager MUST land the merge txn while the pair is PROVABLY still frozen,
/// otherwise the seal records a STALE captured `commit_length` and silently
/// drops the writes that resumed on the victim tail post-unfreeze (reproduced
/// deterministically: `system_merge_freeze_lostupdate`). Mirrors the split
/// path's PS-side `split_freeze_deadline` budget — merge is manager-driven, so
/// the budget lives here.
///
/// Unlike split (which bounds its commit RPC with `SPLIT_CALL_TIMEOUT` = 8 s),
/// the merge Phase-2 `txn_fenced` here is NOT wrapped in an explicit hard
/// timeout — it relies on the etcd client's own bound, which can be ~10 s under
/// a degraded etcd. So the deadline is set conservatively: FREEZE_TTL(30) −
/// worst-case txn(~12) − safety(2) = 15 s. A merge that reaches the budget check
/// within 15 s of issuing the freeze then commits with ≥ 3 s of headroom before
/// the PS could unfreeze even if the txn itself runs long. (A merge slower than
/// 15 s here is pathological; the abort is retryable, so a rare false-abort only
/// costs a retry, never data.)
const MERGE_FREEZE_COMMIT_DEADLINE: Duration = Duration::from_secs(15);

/// Test-only failpoint: sleep this many ms between the commit_length capture and
/// the merge txn inside `handle_merge_partitions`, simulating a paused/slow
/// coordinator so the freeze-budget guard can be exercised deterministically.
/// Always compiled (the failpoints idiom) so an integration test can arm it —
/// production always leaves it 0, making the hook a single relaxed load per
/// (rare) merge. A process-global (not thread-local) so a test thread can arm it
/// for the manager's own runtime thread. Re-exported from `lib.rs`.
#[doc(hidden)]
pub static MERGE_TEST_PAUSE_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

impl AutumnManager {
    // ── Serve ──────────────────────────────────────────────────────────

    pub async fn serve(&self, addr: SocketAddr) -> Result<()> {
        self.start_runtime_tasks();
        let mut listener = autumn_transport::current_or_init().bind(addr).await?;
        tracing::info!(addr = %addr, "manager listening");
        // the bind above may have retried through a killed
        // predecessor's TIME_WAIT window (~60 s on ucx). No PS
        // heartbeat could arrive before this point, so restart every
        // PS's liveness clock and only now allow the eviction sweep
        // (`ps_liveness_check_loop` gates on `serving`).
        self.mark_serving();
        loop {
            // accept errors are CONNECTION-scoped, not process-scoped.
            // This was previously `listener.accept().await?` — on UCX the
            // accept path flushes the just-created ep, so a peer that dies
            // mid-handshake (e.g. a mass client kill: 1024 conns RST at
            // once) surfaced `ucp_ep_flush cb: Connection reset by remote
            // peer` here, and the `?` took down the WHOLE manager process
            // (observed 2026-06-10; the PS fleet then heartbeat-suicided).
            // One bad handshake must never kill the control plane: log +
            // brief backoff (avoid a busy error loop on a persistent
            // failure like EMFILE) + keep accepting. Mirrors the
            // partition-server per-partition accept task.
            //
            // Known residual (coco P1, accepted): on UCX a failed accept
            // leaves the half-created server-side ep allocated until
            // worker destroy — there is NO working close path under
            // UCP_ERR_HANDLING_MODE_NONE (FORCE close is rejected, FLUSH
            // close deadlocks on loopback, MODE_PEER tears down live EPs
            // under load — see crates/transport/src/ucx/endpoint.rs "EP
            // lifetime" doc). The leak is one ep per FAILED handshake,
            // bounded by the storm size, and strictly better than the
            // previous behavior (whole-process death on the same event).
            let (conn, peer) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, "manager accept failed; continuing");
                    compio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
            };
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

    /// Bug #3 fix (2026-06-06) — pipeline frame dispatch.
    ///
    /// **Pre-fix** this loop was sequential: `read frame → await
    /// dispatch → write response → read next frame`. The await on
    /// dispatch held off the next read. With long-poll handlers
    /// (`handle_poll_invalidations` parks for `LONG_POLL_WAIT = 10 s`),
    /// **every subsequent request on the same TCP conn waited for the
    /// parked poll to return** — head-of-line blocking.
    ///
    /// The ioring daemon multiplexes all 8 sessions' lease RPCs
    /// (8 × poll_invalidations + 8 × heartbeats) onto the single
    /// `ClusterClient.mgr_conn`. The first poll parked for 10 s; the
    /// 7 follow-up polls + every AcquireLease behind them stalled.
    /// Daemon's 30 s `DEFAULT_RPC_TIMEOUT` fired, evicted the conn,
    /// reconnected — but the 16 in-flight `mgr_call` futures still
    /// each held an `Rc<RpcClient>` for their own 30 s wait. Daemon
    /// accumulated **stale RpcClient instances (TCP fds) at ~0.15 conn/s**
    /// (487 ESTABLISHED to manager observed after 90 min).
    ///
    /// **Post-fix**: every frame's dispatch runs in a detached task;
    /// completed responses are funneled through an unbounded
    /// `futures::channel::mpsc` to a dedicated writer task. Reader,
    /// writer, and dispatchers all run concurrently on the same
    /// compio runtime (thread-per-core, !Send is fine — no atomics
    /// needed). Mirrors `partition-server::handle_ps_connection`
    /// (Section 13.1) and `stream::extent_node::handle_connection`.
    /// Response order is best-effort completion order (not request
    /// order), which is correct: every frame carries its own
    /// `req_id` and the client's `pending` map dispatches by id.
    ///
    /// Drop semantics: dropping `resp_tx` after the reader loop
    /// exits signals the writer task to flush + exit. Detached
    /// dispatch tasks for in-flight requests will silently fail
    /// when they try to `unbounded_send` into the closed channel —
    /// that's correct (client already gave up).
    async fn handle_connection(conn: autumn_transport::Conn, mgr: AutumnManager) -> Result<()> {
        use futures::StreamExt;
        let (mut reader, mut writer) = conn.into_split();
        let mut decoder = FrameDecoder::new();
        let mut buf = vec![0u8; 64 * 1024];

        let (resp_tx, mut resp_rx) =
            futures::channel::mpsc::unbounded::<Bytes>();

        // Writer task: drain encoded responses, write to socket
        // in completion order. Single writer = no concurrent
        // `write_all` interleaving.
        let writer_task = compio::runtime::spawn(async move {
            while let Some(data) = resp_rx.next().await {
                let BufResult(result, _) = writer.write_all(data).await;
                if let Err(e) = result {
                    tracing::warn!(error = %e, "manager writer task exit");
                    break;
                }
            }
        });

        let reader_result: Result<()> = async {
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
                            let mgr_c = mgr.clone();
                            let tx = resp_tx.clone();
                            compio::runtime::spawn(async move {
                                let resp_frame = match mgr_c.dispatch(msg_type, payload).await {
                                    Ok(p) => Frame::response(req_id, msg_type, p),
                                    Err((code, message)) => {
                                        let p = autumn_rpc::RpcError::encode_status(
                                            code, &message,
                                        );
                                        Frame::error(req_id, msg_type, p)
                                    }
                                };
                                // best-effort send: if resp_rx already
                                // dropped (conn closed), the dispatch's
                                // work is wasted but no error to
                                // propagate.
                                let _ = tx.unbounded_send(resp_frame.encode());
                            })
                            .detach();
                        }
                        Some(_) => continue,
                        None => break,
                    }
                }
            }
        }
        .await;

        // Reader exited (EOF or err) → close resp_tx so writer drains
        // remaining responses and exits.
        drop(resp_tx);
        let _ = writer_task.await;
        reader_result
    }

    async fn dispatch(&self, msg_type: u8, payload: Bytes) -> HandlerResult {
        // gate cluster-MUTATING ops on a shared admin token,
        // carried as a length-prefix on the payload (zero wire-struct change).
        // OPT-IN: only enforced when this manager was configured with a token —
        // a token-less manager (dev/test/bench/chaos) runs these bare. When a
        // token IS set, the payload MUST carry a matching prefix; the stripped
        // remainder is what the real handler decodes.
        let payload = if autumn_rpc::manager_rpc::is_admin_mgr_msg(msg_type) {
            if let Some(tok) = self.admin_token.borrow().as_ref() {
                let Some((got, rest)) = autumn_rpc::manager_rpc::strip_admin_token(&payload) else {
                    return Err((
                        StatusCode::FailedPrecondition,
                        "admin op requires an admin token (malformed or missing prefix) — pass \
                         --admin-token-file to autumn-op"
                            .to_string(),
                    ));
                };
                if !crate::authz::ct_eq_secret(tok, &String::from_utf8_lossy(got)) {
                    return Err((
                        StatusCode::FailedPrecondition,
                        "admin token invalid".to_string(),
                    ));
                }
                payload.slice_ref(rest)
            } else {
                payload
            }
        } else {
            payload
        };
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
            MSG_RECONCILE_EXTENTS => self.handle_reconcile_extents(payload).await,
            MSG_UPDATE_STREAM_EC => self.handle_update_stream_ec(payload).await,
            MSG_FORCE_EC_CONVERT => self.handle_force_ec_convert(payload).await,
            MSG_GET_PARTITION_DETAIL => self.handle_get_partition_detail(payload).await,
            MSG_GET_POLICY_KIND_NAMES => self.handle_get_policy_kind_names(payload).await,
            // ── operator-driven node lifecycle ──────────────────────
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
            // ── R1 rolling upgrade: cluster_version gate ─────────────────
            MSG_GET_CLUSTER_VERSION => self.handle_get_cluster_version().await,
            MSG_BUMP_CLUSTER_VERSION => self.handle_bump_cluster_version(payload).await,
            // ── WAL self-heal A5: isolate a corrupt log_stream replica ──
            MSG_REPORT_CORRUPT_REPLICA => self.handle_report_corrupt_replica(payload).await,
            // ── cluster-df: aggregate capacity summary ──────────────────
            MSG_CLUSTER_DF => self.handle_cluster_df().await,
            // ── dashboard compact overview (no per-extent array) ────────
            MSG_GET_CLUSTER_OVERVIEW => self.handle_get_cluster_overview().await,
            // ── inode-level lease + close-to-open ──────────────────────
            MSG_ACQUIRE_LEASE => self.handle_acquire_lease(payload).await,
            MSG_RELEASE_LEASE => self.handle_release_lease(payload).await,
            MSG_HEARTBEAT_LEASE => self.handle_heartbeat_lease(payload).await,
            MSG_POLL_INVALIDATIONS => self.handle_poll_invalidations(payload).await,
            // ── manager-as-KDC (data-plane authz) ────────────
            MSG_MINT_TOKEN => self.handle_mint_token(payload).await,
            MSG_GET_AUTHZ_CONFIG => self.handle_get_authz_config().await,
            MSG_TENANT_CREATE => self.handle_tenant_create(payload).await,
            MSG_TENANT_DELETE => self.handle_tenant_delete(payload).await,
            // ── namespace registry ─────────────────────────
            MSG_NAMESPACE_CREATE => self.handle_namespace_create(payload).await,
            MSG_NAMESPACE_DELETE => self.handle_namespace_delete(payload).await,
            MSG_NAMESPACE_LIST => self.handle_namespace_list().await,
            MSG_PRINCIPAL_LIST => self.handle_principal_list().await,
            MSG_NAMESPACE_SET_PRESPLIT => self.handle_namespace_set_presplit(payload).await,
            // ── crash-safe fuse-fs inode allocation ──────
            MSG_ALLOC_INODES => self.handle_alloc_inodes(payload).await,
            MSG_AUTOPOLICY_GET => self.handle_autopolicy_get(payload).await,
            MSG_AUTOPOLICY_SET => self.handle_autopolicy_set(payload).await,
            MSG_REBALANCE_REGIONS => self.handle_rebalance_regions(payload).await,
            MSG_OP_SUBMIT => self.handle_op_submit(payload).await,
            MSG_OP_QUERY => self.handle_op_query(payload).await,
            _ => Err((
                StatusCode::InvalidArgument,
                format!("unknown msg_type {msg_type}"),
            )),
        }
    }

    // ── RPC handlers ───────────────────────────────────────────────────

    async fn handle_status(&self) -> HandlerResult {
        Self::code_resp(CODE_OK, String::new())
    }

    /// read-only cluster identity. Servable from any replica
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
                wire_fingerprint: autumn_rpc::WIRE_FINGERPRINT.to_string(),
                wire_version_min: autumn_rpc::WIRE_VERSION_MIN,
                wire_version_max: autumn_rpc::WIRE_VERSION_MAX,
                cluster_version: self.cluster_version.get(),
            }));
        }
        Ok(rkyv_encode(&GetClusterIdResp {
            code: CODE_OK,
            message: String::new(),
            cluster_id: id,
            wire_fingerprint: autumn_rpc::WIRE_FINGERPRINT.to_string(),
            wire_version_min: autumn_rpc::WIRE_VERSION_MIN,
            wire_version_max: autumn_rpc::WIRE_VERSION_MAX,
            cluster_version: self.cluster_version.get(),
        }))
    }

    // ── manager-as-KDC (data-plane authz) ─────────────────────────

    /// `MSG_MINT_TOKEN` — a client authenticates with its permanent tenant
    /// credential and receives a short-TTL signed capability token. Leader-only:
    /// the tenant account DB is authoritative only on the leader (replayed on
    /// promotion); a follower's copy may be stale/empty.
    async fn handle_mint_token(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&MintTokenResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                token: Vec::new(),
                exp: 0,
            }));
        }
        let req: MintTokenReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // Authz must be enabled (a signing key was configured) and have an
        // enabled signing key. No awaits below → the keyring borrow is safe.
        let kr = self.authz_keyring.borrow();
        let (kid, sk) = match kr.as_ref().and_then(|k| k.active()) {
            Some(v) => v,
            None => {
                return Ok(rkyv_encode(&MintTokenResp {
                    code: CODE_ERROR,
                    message: "authz not enabled / no signing key on this manager".to_string(),
                    token: Vec::new(),
                    exp: 0,
                }));
            }
        };

        // Verify the credential (constant-time). A missing tenant and a wrong
        // credential return the SAME opaque error (don't reveal which).
        let allowed_prefixes = {
            let accts = self.tenant_accounts.borrow();
            match accts.get(&req.principal) {
                Some(acct)
                    if crate::authz::ct_eq_32(
                        &crate::authz::credential_hash(&req.credential),
                        &acct.credential_hash,
                    ) =>
                {
                    acct.allowed_prefixes.clone()
                }
                _ => {
                    return Ok(rkyv_encode(&MintTokenResp {
                        code: CODE_PRECONDITION,
                        message: "tenant or credential invalid".to_string(),
                        token: Vec::new(),
                        exp: 0,
                    }));
                }
            }
        };

        let now = Self::epoch_seconds().max(0) as u64;
        // saturating_add so a (clamped, but defensively) large TTL can't wrap
        // now+ttl into a past exp (coco P2).
        let exp = now.saturating_add(self.token_ttl_secs.get());
        let claims = autumn_rpc::cap_token::CapClaims {
            ver: autumn_rpc::cap_token::CAP_VER,
            typ: autumn_rpc::cap_token::CAP_TYP.to_string(),
            kid,
            iss: "autumn-mgr".to_string(),
            aud: self.cluster_id.borrow().clone(),
            iat: now,
            nbf: now,
            exp,
            allowed_prefixes,
        };
        match autumn_rpc::cap_token::sign_claims(sk, &claims) {
            Ok(token) => Ok(rkyv_encode(&MintTokenResp {
                code: CODE_OK,
                message: String::new(),
                token,
                exp,
            })),
            Err(e) => Ok(rkyv_encode(&MintTokenResp {
                code: CODE_ERROR,
                message: format!("sign failed: {e}"),
                token: Vec::new(),
                exp: 0,
            })),
        }
    }

    /// `MSG_GET_AUTHZ_CONFIG` — PS polls this (cached) to learn the public keys
    /// + protected prefixes + the registered namespace list.
    ///
    /// **LEADER-GATED (D2, coco P1).** Pre-D2 this was intentionally
    /// follower-answerable because the response was STATIC local config (the same
    /// signing-key file on every manager via cluster.sh). D2 folded in DYNAMIC,
    /// leader-maintained state — the `namespaces` list + the owner-derived
    /// auto-protected prefixes come from the etcd registry, which only the leader
    /// replays. A follower's shadow is empty/stale, so answering from it would
    /// publish an EMPTY namespace list (Layer-A would then reject every write in
    /// SD-2) and drop the auto-protected prefixes (an owned namespace would go
    /// unprotected). So we refuse from a follower with `CODE_NOT_LEADER`; the PS
    /// `fetch_authz_config_once` rotates to the leader on that code AND keeps its
    /// last-known cached config through the election window (it only `install`s
    /// on `CODE_OK`), so enforcement never fail-opens on a transient follower hit.
    async fn handle_get_authz_config(&self) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&GetAuthzConfigResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                ..Default::default()
            }));
        }
        let kr = self.authz_keyring.borrow();
        let (enabled, public_keys) = match kr.as_ref() {
            Some(k) => (true, k.published()),
            None => (false, Vec::new()),
        };
        // D2/D7: derive both prefix lists from the namespace registry.
        //  - `namespaces` = ALL registered prefixes (Layer-A data source, SD-2).
        //  - `protected_prefixes` = the manually-configured D6 list (kept as a
        //    fallback / union member so `--auth-protected-prefix` never breaks)
        //    UNIONED with every registry namespace whose owner_tenant.is_some()
        //    (auto-protected — replaces the hand-maintained list over time).
        // Both are de-duplicated so a manually-listed prefix that is also an
        // owned namespace appears once.
        let mut protected: Vec<Vec<u8>> = self.protected_prefixes.borrow().clone();
        let mut namespaces: Vec<Vec<u8>> = Vec::new();
        {
            let ns = self.namespaces.borrow();
            for row in ns.values() {
                namespaces.push(row.prefix.clone());
                if row.owner_tenant.is_some() && !protected.contains(&row.prefix) {
                    protected.push(row.prefix.clone());
                }
            }
        }
        Ok(rkyv_encode(&GetAuthzConfigResp {
            code: CODE_OK,
            message: String::new(),
            enabled,
            public_keys,
            protected_prefixes: protected,
            namespaces,
            token_ttl_secs: self.token_ttl_secs.get(),
            clock_skew_secs: self.clock_skew_secs.get(),
            cluster_id: self.cluster_id.borrow().clone(),
            // (PS slice): hand the PS the admin secret so it can
            // gate split/maintenance. Empty when unconfigured → PS runs them bare.
            admin_token: self
                .admin_token
                .borrow()
                .as_ref()
                .map(|t| t.as_bytes().to_vec())
                .unwrap_or_default(),
        }))
    }

    /// `MSG_TENANT_CREATE` — admin creates/rotates a tenant account. Leader-only,
    /// admin-token gated. Returns the freshly-generated permanent credential
    /// (shown once; only its SHA-256 hash is stored).
    async fn handle_tenant_create(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&TenantCreateResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                credential: Vec::new(),
            }));
        }
        let req: TenantCreateReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // Admin-token gate (admin_auth_design.md Option A). Fail-closed: refuse
        // if no admin token is configured; constant-time compare otherwise.
        match self.admin_token.borrow().as_ref() {
            Some(cfg) if crate::authz::ct_eq_secret(cfg, &req.admin_token) => {}
            Some(_) => {
                return Ok(rkyv_encode(&TenantCreateResp {
                    code: CODE_PRECONDITION,
                    message: "admin token invalid".to_string(),
                    credential: Vec::new(),
                }));
            }
            None => {
                return Ok(rkyv_encode(&TenantCreateResp {
                    code: CODE_ERROR,
                    message: "admin RPCs disabled (no --admin-token configured)".to_string(),
                    credential: Vec::new(),
                }));
            }
        }

        if req.tenant.is_empty() {
            return Ok(rkyv_encode(&TenantCreateResp {
                code: CODE_INVALID_ARGUMENT,
                message: "tenant must be non-empty".to_string(),
                credential: Vec::new(),
            }));
        }
        if req.allowed_prefixes.is_empty() {
            return Ok(rkyv_encode(&TenantCreateResp {
                code: CODE_INVALID_ARGUMENT,
                message: "at least one allowed_prefix required".to_string(),
                credential: Vec::new(),
            }));
        }
        // Normalize each prefix to end with '/' (unforgeable segment boundary).
        let mut allowed_prefixes = req.allowed_prefixes.clone();
        for p in &mut allowed_prefixes {
            if p.is_empty() {
                return Ok(rkyv_encode(&TenantCreateResp {
                    code: CODE_INVALID_ARGUMENT,
                    message: "empty allowed_prefix".to_string(),
                    credential: Vec::new(),
                }));
            }
            if p.last() != Some(&b'/') {
                p.push(b'/');
            }
        }

        // Fresh 32-byte credential from the OS CSPRNG (returned once).
        let mut cred = [0u8; 32];
        {
            use rand::RngCore;
            rand::rngs::OsRng.fill_bytes(&mut cred);
        }
        let acct = MgrTenantAccount {
            tenant: req.tenant.clone(),
            credential_hash: crate::authz::credential_hash(&cred),
            allowed_prefixes,
        };
        // Serialize the whole write critical section (etcd → memory apply) so a
        // concurrent same-tenant op can't commit to etcd in one order but apply
        // to memory in the other (coco P1). etcd-first (Programming Note 1),
        // fenced txn.
        let _admin = self.tenant_admin_lock.lock().await;
        let key = format!("{}{}", crate::TENANT_ACCOUNT_PREFIX, req.tenant);
        if let Some(etcd) = &self.etcd {
            if let Err(err) = etcd
                .put_msgs_txn(vec![(key, rkyv_encode(&acct).to_vec())])
                .await
            {
                return Ok(rkyv_encode(&TenantCreateResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                    credential: Vec::new(),
                }));
            }
        }
        self.tenant_accounts
            .borrow_mut()
            .insert(req.tenant.clone(), acct);
        Ok(rkyv_encode(&TenantCreateResp {
            code: CODE_OK,
            message: String::new(),
            credential: cred.to_vec(),
        }))
    }

    /// `MSG_TENANT_DELETE` — admin removes a tenant account (stops renewal; the
    /// tenant's current token still works until it expires). Leader-only,
    /// admin-token gated.
    async fn handle_tenant_delete(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Self::code_resp(Self::err_to_code(&err), err.to_string());
        }
        let req: TenantDeleteReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        match self.admin_token.borrow().as_ref() {
            Some(cfg) if crate::authz::ct_eq_secret(cfg, &req.admin_token) => {}
            Some(_) => return Self::code_resp(CODE_PRECONDITION, "admin token invalid".to_string()),
            None => {
                return Self::code_resp(
                    CODE_ERROR,
                    "admin RPCs disabled (no --admin-token configured)".to_string(),
                )
            }
        }
        // Same serialization as tenant-create (coco P1): create/delete of the
        // same tenant must not reorder between etcd and memory.
        let _admin = self.tenant_admin_lock.lock().await;
        let key = format!("{}{}", crate::TENANT_ACCOUNT_PREFIX, req.tenant);
        if let Some(etcd) = &self.etcd {
            if let Err(err) = etcd.put_and_delete_txn(Vec::new(), vec![key]).await {
                return Self::code_resp(Self::err_to_code(&err), err.to_string());
            }
        }
        self.tenant_accounts.borrow_mut().remove(&req.tenant);
        Self::code_resp(CODE_OK, String::new())
    }

    /// `MSG_NAMESPACE_CREATE` (D2) — admin registers a namespace.
    /// Leader-only, admin-token gated. Rejects reserved names + prefix-overlap;
    /// etcd-first (Programming Note 1), leader-fenced, serialized on
    /// `namespace_admin_lock`. Mirrors `handle_tenant_create`.
    async fn handle_namespace_create(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&NamespaceCreateResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }
        let req: NamespaceCreateReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // Admin-token gate (fail-closed; constant-time compare). Same as tenant.
        match self.admin_token.borrow().as_ref() {
            Some(cfg) if crate::authz::ct_eq_secret(cfg, &req.admin_token) => {}
            Some(_) => {
                return Ok(rkyv_encode(&NamespaceCreateResp {
                    code: CODE_PRECONDITION,
                    message: "admin token invalid".to_string(),
                }));
            }
            None => {
                return Ok(rkyv_encode(&NamespaceCreateResp {
                    code: CODE_ERROR,
                    message: "admin RPCs disabled (no --admin-token configured)".to_string(),
                }));
            }
        }

        // Name charset validation (single path segment).
        if let Err(msg) = crate::validate_namespace_name(&req.name) {
            return Ok(rkyv_encode(&NamespaceCreateResp {
                code: CODE_INVALID_ARGUMENT,
                message: msg,
            }));
        }
        // Reserved-name reject (fs/kvc/mem/default).
        if crate::RESERVED_NAMESPACE_NAMES.contains(&req.name.as_str()) {
            return Ok(rkyv_encode(&NamespaceCreateResp {
                code: CODE_INVALID_ARGUMENT,
                message: format!("'{}' is a reserved namespace name", req.name),
            }));
        }

        let new_prefix = format!("{}/", req.name).into_bytes();

        // Serialize the whole critical section (existence + disjointness check →
        // etcd write → in-mem apply) so two concurrent creates can't both pass
        // the checks and then commit in a conflicting order (mirrors the tenant
        // admin lock — coco P1 class).
        let _admin = self.namespace_admin_lock.lock().await;

        // Already-exists + prefix-disjointness check (under the lock).
        {
            let ns = self.namespaces.borrow();
            if ns.contains_key(&req.name) {
                return Ok(rkyv_encode(&NamespaceCreateResp {
                    code: CODE_PRECONDITION,
                    message: format!("namespace '{}' already exists", req.name),
                }));
            }
            let existing: Vec<&[u8]> = ns.values().map(|r| r.prefix.as_slice()).collect();
            if crate::namespace_prefix_conflicts(&new_prefix, &existing) {
                return Ok(rkyv_encode(&NamespaceCreateResp {
                    code: CODE_INVALID_ARGUMENT,
                    message: format!(
                        "namespace '{}' prefix overlaps an existing namespace \
                         (all namespace prefixes must be pairwise disjoint)",
                        req.name
                    ),
                }));
            }
        }

        let row = MgrNamespace {
            name: req.name.clone(),
            prefix: new_prefix,
            owner_tenant: req.owner_tenant.clone(),
            presplit: req.presplit.clone(),
            created_at: Self::epoch_seconds(),
        };
        let key = format!("{}{}", crate::NAMESPACE_PREFIX, req.name);
        if let Some(etcd) = &self.etcd {
            if let Err(err) = etcd
                .put_msgs_txn(vec![(key, rkyv_encode(&row).to_vec())])
                .await
            {
                return Ok(rkyv_encode(&NamespaceCreateResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                }));
            }
        }
        self.namespaces.borrow_mut().insert(req.name.clone(), row);
        Ok(rkyv_encode(&NamespaceCreateResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    /// `MSG_NAMESPACE_DELETE` (D2) — admin removes a namespace registry
    /// row. Leader-only, admin-token gated. Refuses the three built-in families
    /// (`fs`/`kvc`/`mem`). The NON-EMPTY guard (`--force`) is enforced
    /// CLIENT-SIDE in `autumn-op` (the manager has no KV data-plane client), so
    /// this handler only drops the etcd registry row. Mirrors
    /// `handle_tenant_delete`.
    async fn handle_namespace_delete(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Self::code_resp(Self::err_to_code(&err), err.to_string());
        }
        let req: NamespaceDeleteReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        match self.admin_token.borrow().as_ref() {
            Some(cfg) if crate::authz::ct_eq_secret(cfg, &req.admin_token) => {}
            Some(_) => return Self::code_resp(CODE_PRECONDITION, "admin token invalid".to_string()),
            None => {
                return Self::code_resp(
                    CODE_ERROR,
                    "admin RPCs disabled (no --admin-token configured)".to_string(),
                )
            }
        }
        // Built-in families are non-deletable (bootstrap-seeded).
        if crate::BUILTIN_NAMESPACES.contains(&req.name.as_str()) {
            return Self::code_resp(
                CODE_INVALID_ARGUMENT,
                format!("built-in namespace '{}' cannot be deleted", req.name),
            );
        }
        let _admin = self.namespace_admin_lock.lock().await;
        if !self.namespaces.borrow().contains_key(&req.name) {
            return Self::code_resp(
                CODE_NOT_FOUND,
                format!("namespace '{}' not found", req.name),
            );
        }
        let key = format!("{}{}", crate::NAMESPACE_PREFIX, req.name);
        if let Some(etcd) = &self.etcd {
            if let Err(err) = etcd.put_and_delete_txn(Vec::new(), vec![key]).await {
                return Self::code_resp(Self::err_to_code(&err), err.to_string());
            }
        }
        self.namespaces.borrow_mut().remove(&req.name);
        Self::code_resp(CODE_OK, String::new())
    }

    /// `MSG_NAMESPACE_LIST` (D2) — list the full registry (rich rows).
    /// Leader-gated (the registry is leader-maintained; a follower's shadow is
    /// empty/stale — same reason `GET_AUTHZ_CONFIG` is leader-gated). Read-only,
    /// not admin-token gated.
    async fn handle_namespace_list(&self) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&NamespaceListResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                namespaces: Vec::new(),
            }));
        }
        let mut namespaces: Vec<MgrNamespace> =
            self.namespaces.borrow().values().cloned().collect();
        // Stable order (by name) for deterministic CLI output.
        namespaces.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(rkyv_encode(&NamespaceListResp {
            code: CODE_OK,
            message: String::new(),
            namespaces,
        }))
    }

    /// step 4: record split points an operator's presplit
    /// actually applied, so `merge` can refuse to undo them.
    ///
    /// UNIONs rather than replaces — see `NamespaceSetPresplitReq`. Admin-gated
    /// because it changes what merge refuses; leader-gated because the registry
    /// is leader-maintained.
    async fn handle_namespace_set_presplit(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&CodeResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }
        let req: NamespaceSetPresplitReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        // UX-fix (M2): recording sacred boundaries is OPT-IN on the
        // admin token, mirroring `is_admin_mgr_msg` — NOT fail-closed like the
        // tenant/namespace-create family. Rationale: this op only *records* a
        // layout an operator already declared (it grants no capability and
        // exposes no secret), and the WHOLE POINT is the merge guard + auto-split
        // snap. Fail-closing it meant a token-less cluster (dev / bench / chaos /
        // memory-mode) could NEVER arm the protection, while its auto-policy
        // controller could still merge boundaries away. So:
        //   • manager has NO token  → accept bare (record the boundaries);
        //   • manager HAS a token   → the request MUST carry a matching one.
        // This makes "merge is safe" unconditional instead of contingent on a
        // two-position secret ritual.
        if let Some(cfg) = self.admin_token.borrow().as_ref() {
            if !crate::authz::ct_eq_secret(cfg, &req.admin_token) {
                return Ok(rkyv_encode(&CodeResp {
                    code: CODE_PRECONDITION,
                    message: "admin token invalid".to_string(),
                }));
            }
        }
        // Build the updated row WITHOUT touching the live map: etcd is written
        // first and memory only commits on success, same discipline as
        // `handle_namespace_create`. Mutating in place and then failing to
        // persist would leave memory claiming boundaries that a leader failover
        // (which replays from etcd) would silently forget — merge would then
        // start allowing what this leader refuses.
        let mut updated = match self.namespaces.borrow().get(&req.name) {
            Some(row) => row.clone(),
            None => {
                return Ok(rkyv_encode(&CodeResp {
                    code: CODE_NOT_FOUND,
                    message: format!("namespace {} is not registered", req.name),
                }));
            }
        };
        for p in &req.points {
            if !updated.presplit.contains(p) {
                updated.presplit.push(p.clone());
            }
        }
        // Sorted so the persisted row is stable regardless of the order the
        // operator's cuts happened to land in.
        updated.presplit.sort();
        if let Some(etcd) = &self.etcd {
            let key = format!("{}{}", crate::NAMESPACE_PREFIX, req.name);
            if let Err(err) = etcd
                .put_msgs_txn(vec![(key, rkyv_encode(&updated).to_vec())])
                .await
            {
                return Ok(rkyv_encode(&CodeResp {
                    code: Self::err_to_code(&err),
                    message: err.to_string(),
                }));
            }
        }
        self.namespaces.borrow_mut().insert(req.name.clone(), updated);
        Ok(rkyv_encode(&CodeResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    /// step 4: is `key` a boundary an operator declared via
    /// presplit? Returns the owning namespace name if so.
    ///
    /// Deliberately GENERIC — the manager never learns what a "lane" is. The
    /// rule is "an operator-declared boundary is sacred", which is why fs lane
    /// boundaries, kvc hash buckets and mem agent cuts all get the protection
    /// from one predicate.
    pub(crate) fn sacred_boundary_owner(&self, key: &[u8]) -> Option<String> {
        if key.is_empty() {
            return None; // the keyspace start is nobody's declared cut
        }
        self.namespaces
            .borrow()
            .values()
            .find(|ns| ns.presplit.iter().any(|p| p.as_slice() == key))
            .map(|ns| ns.name.clone())
    }

    /// the declared-but-uncut boundary a split of `part_id`
    /// should snap to, or `None` to let the PS pick a median user key.
    ///
    /// Picks the declared point nearest the MIDDLE of the partition's range so a
    /// split halves the lane span rather than shaving one lane off an end — the
    /// same balance argument as median selection, applied to the declared grid
    /// instead of to the data.
    ///
    /// "Strictly inside" matters: a point equal to `start_key` is already this
    /// partition's boundary (splitting there produces an empty child, which the
    /// PS rejects), and `end_key` belongs to the neighbour.
    pub(crate) fn declared_split_point_within(&self, part_id: u64) -> Option<Vec<u8>> {
        let (start, end) = {
            let s = self.store.inner.borrow();
            let rg = s.partitions.get(&part_id)?.rg.as_ref()?;
            (rg.start_key.clone(), rg.end_key.clone())
        };
        let ns = self.namespaces.borrow();
        let mut inside: Vec<&Vec<u8>> = ns
            .values()
            .flat_map(|n| n.presplit.iter())
            .filter(|p| {
                p.as_slice() > start.as_slice()
                    && (end.is_empty() || p.as_slice() < end.as_slice())
            })
            .collect();
        if inside.is_empty() {
            return None;
        }
        inside.sort();
        Some(inside[inside.len() / 2].clone())
    }

    /// Every declared boundary in the cluster — the policy engine's input for
    /// skipping merge candidates before they are ever advertised.
    pub(crate) fn sacred_boundaries(&self) -> std::collections::HashSet<Vec<u8>> {
        self.namespaces
            .borrow()
            .values()
            .flat_map(|ns| ns.presplit.iter().cloned())
            .collect()
    }

    /// list every principal + its grants. Mirrors
    /// `handle_namespace_list` — leader-gated, read-only, no admin-token gate.
    /// `credential_hash` is dropped on the way out (see `PrincipalRow`).
    async fn handle_principal_list(&self) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&PrincipalListResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                principals: Vec::new(),
            }));
        }
        let mut principals: Vec<PrincipalRow> = self
            .tenant_accounts
            .borrow()
            .values()
            .map(|a| PrincipalRow {
                name: a.tenant.clone(),
                grants: a.allowed_prefixes.clone(),
            })
            .collect();
        // Stable order (by name) for deterministic CLI output — the map is a
        // HashMap, so without this the listing shuffles between calls.
        principals.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(rkyv_encode(&PrincipalListResp {
            code: CODE_OK,
            message: String::new(),
            principals,
        }))
    }

    /// R1: read the persisted cluster_version. Servable from any replica,
    /// but a follower's in-memory copy only updates on replay (leader
    /// promotion) — after a bump it would stay stale indefinitely (coco
    /// P2). This is a rare operator RPC, so do a FRESH etcd read (and
    /// heal the local cache); fall back to the in-memory value only when
    /// etcd is unreachable/absent (memory mode).
    async fn handle_get_cluster_version(&self) -> HandlerResult {
        if let Some(etcd) = &self.etcd {
            if let Ok(resp) = etcd.client.get(crate::CLUSTER_VERSION_KEY.as_bytes()).await {
                if let Some(kv) = resp.kvs.first() {
                    match AutumnManager::parse_cluster_version(&kv.value) {
                        Ok(v) => self.cluster_version.set(v),
                        Err(err) => {
                            // Out-of-bound (this binary older than the
                            // committed format level) or garbage: report
                            // it rather than serving a misleading number.
                            return Ok(rkyv_encode(&GetClusterVersionResp {
                                code: CODE_ERROR,
                                message: err.to_string(),
                                cluster_version: self.cluster_version.get(),
                                wire_version_min: autumn_rpc::WIRE_VERSION_MIN,
                                wire_version_max: autumn_rpc::WIRE_VERSION_MAX,
                            }));
                        }
                    }
                }
            }
        }
        Ok(rkyv_encode(&GetClusterVersionResp {
            code: CODE_OK,
            message: String::new(),
            cluster_version: self.cluster_version.get(),
            wire_version_min: autumn_rpc::WIRE_VERSION_MIN,
            wire_version_max: autumn_rpc::WIRE_VERSION_MAX,
        }))
    }

    /// R1: operator bump (leader-only, monotonic +1, value-CAS'd —
    /// validation in `bump_cluster_version`).
    async fn handle_bump_cluster_version(&self, payload: Bytes) -> HandlerResult {
        let req: BumpClusterVersionReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        match self.bump_cluster_version(req.to).await {
            Ok(v) => Ok(rkyv_encode(&BumpClusterVersionResp {
                code: CODE_OK,
                message: String::new(),
                cluster_version: v,
            })),
            Err(err) => Ok(rkyv_encode(&BumpClusterVersionResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                cluster_version: self.cluster_version.get(),
            })),
        }
    }

    /// WAL self-heal A5: isolate a bit-rotted log_stream replica reported by a
    /// PS replay. Fenced (owner_epoch + eversion CAS) + etcd-first. Clears the
    /// corrupt slots' `avali` bits on a SEALED extent (so the A1 read filter
    /// stops serving from them) and bumps eversion (invalidates client caches).
    /// OPEN-extent corruption needs seal-and-roll (A4) — refused here with
    /// CODE_PRECONDITION so the PS fails the open loud rather than self-heal
    /// an extent whose length isn't yet frozen.
    async fn handle_report_corrupt_replica(&self, payload: Bytes) -> HandlerResult {
        let req: ReportCorruptReplicaReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        if let Err(e) = self.ensure_leader() {
            return Ok(rkyv_encode(&ReportCorruptReplicaResp {
                code: Self::err_to_code(&e),
                message: e.to_string(),
            }));
        }
        // Compute the etcd-first update under a read-only borrow (no mutation
        // until the persist succeeds — coco I5).
        let updated: Result<MgrExtentInfo, (u8, String)> = {
            let s = self.store.inner.borrow();
            // I4 fencing: the reporter must be the current partition owner.
            let owner_key = format!("partition/{}", req.partition_id);
            match s.owner_epochs.get(&owner_key) {
                Some(&cur) if cur == req.owner_epoch => {}
                other => {
                    // Fencing CAS failed: the reporter is not the current owner
                    // (stale PS). Manager-namespace code 5 = NOT_LEADER, so use
                    // PRECONDITION (the report's precondition — being the owner
                    // — does not hold). The PS treats any non-OK as "don't trust".
                    return Ok(rkyv_encode(&ReportCorruptReplicaResp {
                        code: CODE_PRECONDITION,
                        message: format!(
                            "stale corrupt-replica report: partition {} owner_epoch {} != current {:?}",
                            req.partition_id, req.owner_epoch, other
                        ),
                    }));
                }
            }
            // I4 scoping (coco P1 #4 + #2): the named log_stream must actually
            // belong to the partition whose owner_epoch authorized this report,
            // AND the extent must be a member of that stream. Without the
            // partition→log_stream binding, a PS owning partition A could name
            // partition B's log_stream + a B extent and isolate B's replicas.
            // Mirrors punch_holes/truncate operating only on their named
            // stream's extents, plus the owner→stream ownership tie.
            match s.partitions.get(&req.partition_id) {
                Some(pm) if pm.log_stream == req.log_stream_id => {}
                _ => {
                    return Ok(rkyv_encode(&ReportCorruptReplicaResp {
                        code: CODE_PRECONDITION,
                        message: format!(
                            "log_stream {} is not partition {}'s log_stream — refusing \
                             cross-partition corrupt-replica report",
                            req.log_stream_id, req.partition_id
                        ),
                    }));
                }
            }
            match s.streams.get(&req.log_stream_id) {
                Some(si) if si.extent_ids.contains(&req.extent_id) => {}
                _ => {
                    return Ok(rkyv_encode(&ReportCorruptReplicaResp {
                        code: CODE_PRECONDITION,
                        message: format!(
                            "extent {} is not a member of log_stream {} — refusing out-of-scope \
                             corrupt-replica report",
                            req.extent_id, req.log_stream_id
                        ),
                    }));
                }
            }
            match s.extents.get(&req.extent_id) {
                None => Err((
                    CODE_NOT_FOUND,
                    format!("extent {} not found", req.extent_id),
                )),
                Some(ex) if ex.eversion != req.eversion => Err((
                    CODE_PRECONDITION,
                    format!(
                        "extent {} eversion {} != reported {} (concurrent op); retry",
                        req.extent_id, ex.eversion, req.eversion
                    ),
                )),
                // coco P2 #5: an EC-converted extent has shard bytes, not full
                // replicas — `avali` bits mean shard availability and clearing
                // one would corrupt the EC read/repair semantics. The replicated
                // self-heal does not apply; refuse (the detect→EC-convert race or
                // a stale PS). EC shard repair routes through recovery, not here.
                Some(ex) if ex.ec_converted => Err((
                    CODE_PRECONDITION,
                    format!(
                        "extent {} is EC-converted; replicated corrupt-replica isolation does \
                         not apply (EC shard repair routes through recovery)",
                        req.extent_id
                    ),
                )),
                Some(ex) if !ex.sealed => Err((
                    CODE_PRECONDITION,
                    format!(
                        "extent {} is OPEN; corruption isolation on an unsealed tail needs \
                         seal-and-roll (A4, not yet implemented) — failing the report so the \
                         PS open fails loud",
                        req.extent_id
                    ),
                )),
                Some(ex) => {
                    let mut new_ex = ex.clone();
                    let slots: Vec<u64> = new_ex
                        .replicates
                        .iter()
                        .chain(new_ex.parity.iter())
                        .copied()
                        .collect();
                    let mut cleared = 0u32;
                    let mut found = 0u32; // reported nodes that ARE replicas
                    for nid in &req.corrupt_node_ids {
                        if let Some(slot) = slots.iter().position(|s| s == nid) {
                            found += 1;
                            // coco P2 #6: `avali` is u32 — guard the shift. K+M
                            // is capped well below 32 today, but never UB on a
                            // malformed/future-wide layout.
                            if slot >= 32 {
                                continue;
                            }
                            let bit = 1u32 << slot;
                            if new_ex.avali & bit != 0 {
                                new_ex.avali &= !bit;
                                cleared += 1;
                            }
                        }
                    }
                    if found == 0 {
                        // coco P2 #5: NONE of the reported nodes are replicas of
                        // this extent — a stale-layout / buggy report. Returning
                        // OK here would falsely assert "isolated" while the node
                        // the PS actually saw corruption from is unaddressed
                        // (isolation-before-serving violated). Refuse so the PS
                        // refetches ExtentInfo + retries (or fails the open loud).
                        return Ok(rkyv_encode(&ReportCorruptReplicaResp {
                            code: CODE_PRECONDITION,
                            message: format!(
                                "none of the reported corrupt nodes {:?} are replicas of extent \
                                 {} (slots {:?}) — stale layout; PS should refetch + retry",
                                req.corrupt_node_ids, req.extent_id, slots
                            ),
                        }));
                    }
                    if cleared == 0 {
                        // Reported nodes ARE replicas but their avali bits are
                        // already clear — genuine idempotent success (a retried
                        // report after the first isolation landed).
                        return Ok(rkyv_encode(&ReportCorruptReplicaResp {
                            code: CODE_OK,
                            message: "no-op (reported replica(s) already isolated)".into(),
                        }));
                    }
                    // Defense: never isolate the LAST healthy replica — that
                    // would make the extent unreadable. If clearing would leave
                    // zero avali bits, refuse (all replicas reported corrupt =
                    // unrecoverable, the PS must fail loud).
                    if new_ex.avali == 0 {
                        return Ok(rkyv_encode(&ReportCorruptReplicaResp {
                            code: CODE_PRECONDITION,
                            message: format!(
                                "refusing to isolate the last replica(s) of extent {} (all \
                                 reported corrupt) — unrecoverable, PS must fail loud",
                                req.extent_id
                            ),
                        }));
                    }
                    new_ex.eversion += 1;
                    Ok(new_ex)
                }
            }
        };
        let updated = match updated {
            Ok(u) => u,
            Err((code, message)) => {
                return Ok(rkyv_encode(&ReportCorruptReplicaResp { code, message }))
            }
        };
        // etcd-first: persist the avali change before touching in-memory.
        // NOTE (coco P0, deferred per manager CLAUDE.md note 33): this is a
        // blind put on extents/<id>, not a value-CAS, so a concurrent extent-
        // state mutator (recovery/EC/seal/split) landing during this await can
        // be rolled back in etcd by our stale clone. A5 is in the SAME deferred
        // class as apply_recovery_done / apply_ec_conversion_done / split-seal /
        // sync_vp_refs — all blind-put + verify-at-apply (the pattern
        // below), accepting the etcd-RTT residual that note 33's reproduce-first
        // investigation found "not reproducible and structurally near-precluded"
        // (recovery/EC serialize per extent via the inflight ledger; A5 only fires
        // on actual bit-rot during a partition open). The generalized
        // put_delete_txn_cas is kept ready to apply here IF an extent-state
        // clobber is ever actually reproduced — do NOT add it speculatively.
        if let Err(err) = self.persist_extent(&updated).await {
            return Ok(rkyv_encode(&ReportCorruptReplicaResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
            }));
        }
        // Verify-at-apply (coco P1 #1, the same pattern): a concurrent mutator
        // (recovery_done / ec_convert_done / seal / split) could have bumped
        // this extent's eversion during the persist await. We snapshotted the
        // pre-bump baseline as `req.eversion`; if the live extent no longer
        // matches it, another op ran — do NOT stomp it with our stale clone.
        // Refuse (the orphan etcd revision is benign: failover replay reads the
        // latest per key, and the PS retry re-detects + re-reports).
        {
            let mut s = self.store.inner.borrow_mut();
            match s.extents.get(&updated.extent_id) {
                Some(live) if live.eversion != req.eversion => {
                    return Ok(rkyv_encode(&ReportCorruptReplicaResp {
                        code: CODE_PRECONDITION,
                        message: format!(
                            "extent {} eversion changed during isolation ({} != snapshot {}); \
                             concurrent op — PS should retry",
                            updated.extent_id, live.eversion, req.eversion
                        ),
                    }));
                }
                _ => {}
            }
            s.extents.insert(updated.extent_id, updated.clone());
        }
        tracing::warn!(
            extent_id = updated.extent_id,
            avali = updated.avali,
            corrupt = ?req.corrupt_node_ids,
            "A5: isolated corrupt log_stream replica(s) (avali cleared, eversion bumped)"
        );
        Ok(rkyv_encode(&ReportCorruptReplicaResp {
            code: CODE_OK,
            message: String::new(),
        }))
    }

    pub(crate) async fn handle_acquire_owner_lock(&self, payload: Bytes) -> HandlerResult {
        let req: AcquireOwnerLockReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        match self.acquire_owner_epoch(&req.owner_key).await {
            Ok(rev) => Ok(rkyv_encode(&AcquireOwnerLockResp {
                code: CODE_OK,
                message: String::new(),
                owner_epoch: rev,
            })),
            Err(err) => Ok(rkyv_encode(&AcquireOwnerLockResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                owner_epoch: 0,
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

        // M0: resolve node identity BEFORE any decision. The
        // `node_uuid` is the STABLE key (mirrors the PS `ps_id`); the address is
        // just current location. Precedence:
        //   - uuid in the index          → that node (uuid match, no adopt);
        //   - new uuid + address matches a UUID-LESS (legacy) node
        //                                → that node ADOPTS this uuid;
        //   - new uuid + address matches a node that ALREADY has a (different)
        //     uuid, or no address match  → a genuinely NEW node (create) — this
        //     is what lets a fresh node take over a decommissioned node's IP;
        //   - uuid-less caller           → legacy address match, no adopt.
        // `addr_conflict` carries the `node_id` currently holding `req.addr`
        // under a DIFFERENT non-empty uuid (see the create-refusal below).
        let (matched, adopt, addr_conflict): (Option<u64>, bool, Option<u64>) = {
            let s = self.store.inner.borrow();
            if !req.node_uuid.is_empty() {
                if let Some(nid) = s
                    .nodes
                    .values()
                    .find(|n| n.node_uuid == req.node_uuid)
                    .map(|n| n.node_id)
                {
                    (Some(nid), false, None)
                } else if !req.addr.is_empty() {
                    // New uuid: ADOPT an existing UUID-LESS (legacy) node at this
                    // address; a node that already has a (different) uuid means
                    // the address is claimed by a live node under another
                    // identity → REFUSE (do not create a second record at one
                    // address — see the conflict return below).
                    match s.nodes.values().find(|n| n.address == req.addr) {
                        Some(n) if n.node_uuid.is_empty() => (Some(n.node_id), true, None),
                        Some(n) => (None, false, Some(n.node_id)),
                        None => (None, false, None),
                    }
                } else {
                    (None, false, None)
                }
            } else if !req.addr.is_empty() {
                (
                    s.nodes
                        .values()
                        .find(|n| n.address == req.addr)
                        .map(|n| n.node_id),
                    false,
                    None,
                )
            } else {
                (None, false, None)
            }
        };

        // M0 (#1): tombstone-by-UUID. The decommission/fence
        // tombstone travels with the stable identity even after `remove_node`
        // deletes `nodes/<id>` (which is why `matched` would be None). Scan the
        // tombstones by uuid so a removed/fenced node returning under its own
        // identity — at ANY address — is refused, per the decommission
        // runbook. Empty tombstone uuids (legacy/pre-M0) match nothing.
        if !req.node_uuid.is_empty() {
            let tombstoned = self
                .decommissioned
                .borrow()
                .values()
                .any(|o| o.node_uuid == req.node_uuid)
                || self
                    .node_overrides
                    .borrow()
                    .values()
                    .any(|o| o.kind == NODE_OVERRIDE_FENCED && o.node_uuid == req.node_uuid);
            if tombstoned {
                return Ok(rkyv_encode(&RegisterNodeResp {
                    code: CODE_PRECONDITION,
                    message: format!(
                        "node_uuid {} was fenced/decommissioned; clear it \
                         (`autumn-op unfence <id>`) or wipe the data dirs for a \
                         fresh identity before rejoining",
                        req.node_uuid
                    ),
                    node_id: 0,
                    disk_uuids: vec![],
                }));
            }
        }

        // M0 (#3): never create a SECOND node record at an address
        // a live node already holds under a different uuid. Two records at one
        // address make one physical EN look like two failure domains — RF
        // double-placement — and the df loop (no identity echo until M1) would
        // keep BOTH Online from the single EN's heartbeat. The recycled-pod-IP
        // case is legitimate ONLY after the old node is fence+removed (gone from
        // `s.nodes` → no conflict). Fail loud; the operator removes the old
        // record first.
        if let Some(holder) = addr_conflict {
            return Ok(rkyv_encode(&RegisterNodeResp {
                code: CODE_PRECONDITION,
                message: format!(
                    "address {} is held by node {} under a different uuid; \
                     fence + remove it before reusing the address",
                    req.addr, holder
                ),
                node_id: 0,
                disk_uuids: vec![],
            }));
        }

        // #2 zombie defense — UUID-KEYED via the matched node. The
        // decommission/Fence tombstone travels with the NODE, not the IP: a
        // recycled pod IP under a fresh uuid is not falsely refused, and a
        // decommissioned node returning under its own uuid at any address IS.
        if let Some(pid) = matched {
            if self.decommissioned.borrow().contains_key(&pid) {
                return Ok(rkyv_encode(&RegisterNodeResp {
                    code: CODE_PRECONDITION,
                    message: format!(
                        "node {pid} was previously decommissioned; operator must clear tombstone"
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
                            "node {pid} is Fenced; operator must clear override before re-registering"
                        ),
                        node_id: 0,
                        disk_uuids: vec![],
                    }));
                }
            }
        }

        // Re-registration / update path. Reuse the matched node_id + disk_ids
        // rather than rejecting, so an EN recovers from a restart (possibly at a
        // new address / shard layout) without a cluster wipe.
        if let Some(node_id) = matched {
            let (mut existing_node, existing_disks) = {
                let s = self.store.inner.borrow();
                match s.nodes.get(&node_id).cloned() {
                    Some(n) => {
                        let disks = n
                            .disks
                            .iter()
                            .filter_map(|did| s.disks.get(did).cloned())
                            .collect::<Vec<_>>();
                        (n, disks)
                    }
                    None => {
                        // uuid index points at a node that's gone — stale; the
                        // caller retries and the create path takes over.
                        return Ok(rkyv_encode(&RegisterNodeResp {
                            code: CODE_PRECONDITION,
                            message: format!("node {node_id} identity index stale; retry"),
                            node_id: 0,
                            disk_uuids: vec![],
                        }));
                    }
                }
            };

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

            if uuid_map.is_empty() && !req.disk_uuids.is_empty() {
                return Ok(rkyv_encode(&RegisterNodeResp {
                    code: CODE_PRECONDITION,
                    message: format!(
                        "node {node_id} matched but no disk_uuid overlaps — cloned identity file?"
                    ),
                    node_id: 0,
                    disk_uuids: vec![],
                }));
            }

            // Update location if changed. An identity-only re-register (empty
            // `addr` — the M1 `format` re-stamp shape) carries NO live location,
            // so it must not touch address/shard_ports/control_address at all
            // (an empty `shard_ports`/`control_address` there is "unspecified",
            // NOT "clear them"). Only a real self-registration (non-empty addr,
            // which always ships the live ports + ctrl) updates routing. A
            // reshard rides this path: the EN re-registers with the SAME addr
            // and NEW shard_ports → `ports_changed` fires.
            // etcd-first — mirror (node + optional uuid adoption)
            // BEFORE the in-memory apply; a crash mid-mirror leaves the new
            // leader routing to the OLD layout, never a half-applied new one.
            let has_location = !req.addr.is_empty();
            let addr_changed = has_location && existing_node.address != req.addr;
            let ports_changed = has_location && existing_node.shard_ports != req.shard_ports;
            let ctrl_changed = has_location && existing_node.control_address != req.control_address;
            if addr_changed || ports_changed || ctrl_changed || adopt {
                if addr_changed {
                    existing_node.address = req.addr.clone();
                }
                if ports_changed {
                    existing_node.shard_ports = req.shard_ports.clone();
                }
                if ctrl_changed {
                    existing_node.control_address = req.control_address.clone();
                }
                if adopt {
                    existing_node.node_uuid = req.node_uuid.clone();
                }
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

            // a LOCATED re-registration (the EN process itself
            // self-registering, `req.addr` non-empty) counts as a heartbeat —
            // flip Suspected → Online so the operator-facing health report
            // reflects the recovery immediately, not on the next df tick.
            // M1c: an IDENTITY-ONLY re-register (`req.addr`
            // empty — the M1c `autumn-op format` idempotent re-run) is NOT
            // proof the EN is up (format runs BEFORE the EN starts), so it must
            // NOT bump liveness — else a formatted-but-not-yet-booted node would
            // flip Online with its stale location and get selected for
            // allocation → a black-hole until the EN actually self-registers.
            // It stays Suspend/Suspected until a real self-register or a
            // successful df ("unbooted stays Suspend" property).
            if !req.addr.is_empty() {
                self.node_states.borrow_mut().on_heartbeat_ok(node_id);
            }
            return Ok(rkyv_encode(&RegisterNodeResp {
                code: CODE_OK,
                message: String::new(),
                node_id,
                disk_uuids: uuid_map,
            }));
        }

        // etcd-first ordering (CLAUDE.md note 1). Compute node +
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
                node_uuid: req.node_uuid,
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
        // first-time register seeds `Suspend` — a registered
        // but never-verified-alive state. The first successful df from
        // `disk_status_update_loop` transitions to `Online` via
        // `on_heartbeat_ok`. This previously seeded `Online` directly,
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

        // capture the verified-online node set BEFORE borrowing
        // the store; select_nodes uses it as the primary allocation
        // filter so a freshly-registered (but not-yet-df'd) EN doesn't
        // get picked. Two separate borrows are fine — node_states is an
        // independent RefCell.
        let online_node_ids = self.node_states.borrow().online_node_ids();
        let space_low_node_ids = self.space_low_node_ids();
        // Fenced/Maintenance/Suspected nodes are hard-excluded
        // from allocation AND the fallback walk below.
        let hard_excluded = self.placement_excluded_node_ids();
        let (stream_id, extent_id, selected) = {
            let mut s = self.store.inner.borrow_mut();
            let selected =
                match Self::select_nodes(
                    &s.nodes,
                    &s.disks,
                    &online_node_ids,
                    &space_low_node_ids,
                    &hard_excluded,
                    total_replicas,
                    &[],
                )
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

        // style fallback walk: if a selected node refuses
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
                .filter(|n| !hard_excluded.contains(&n.node_id))                .cloned()
                .collect()
        };
        {
            use rand::seq::SliceRandom;
            fallback_nodes.shuffle(&mut rand::thread_rng());
        }
        let mut fallback_iter = fallback_nodes.into_iter();

        let Some((node_ids, disk_ids)) = self
            .place_extents_with_fallback(&selected, &mut fallback_iter, extent_id)
            .await
        else {
            let err = AppError::Precondition(format!(
                "no healthy node available to allocate extent {extent_id} for new stream"
            ));
            return Ok(rkyv_encode(&CreateStreamResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                stream: None,
                extent: None,
            }));
        };

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

        // etcd-first ordering (CLAUDE.md note 1). Mirror to etcd
        // BEFORE applying to in-memory store. The inserts at
        // s.streams / s.extents previously happened first; a manager crash between
        // memory-insert and etcd-write left the new leader (post-replay)
        // without the stream record while the extent files existed on
        // remote nodes as orphans. The same anti-pattern was fixed in
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

        // etcd-first ordering (CLAUDE.md note 1). Compute the new
        // stream snapshot under a read-only borrow, mirror to etcd, then
        // apply to memory. The handler previously mutated the in-memory
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

        let full_dump = req.stream_ids.is_empty();
        let ids = if full_dump {
            s.streams.keys().copied().collect::<Vec<_>>()
        } else {
            req.stream_ids
        };

        let mut streams = Vec::new();
        let mut extents = Vec::new();
        let mut member_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();

        for id in ids {
            if let Some(st) = s.streams.get(&id) {
                streams.push((id, st.clone()));
                for extent_id in &st.extent_ids {
                    member_ids.insert(*extent_id);
                    if let Some(e) = s.extents.get(extent_id) {
                        extents.push((*extent_id, e.clone()));
                    }
                }
            }
        }

        // Observability: on a full cluster dump (`stream_ids` empty), also
        // surface extents that exist in the store but are referenced by NO
        // stream's `extent_ids` — orphan / non-member extents. The loop above
        // only walks stream membership, so these are otherwise invisible. A
        // non-member at `refs==0 && vp_table_refs==0` is reclaimable (the
        // EXTENT10-AUTORECLAIM sweep reaps it); one with `vp_table_refs>0` is a
        // legacy extent retained by the upgrade-safety guard (live VPs, Stage-2
        // migration target). `vp_table_refs` is on the wire (MgrExtentInfo) so
        // the CLI can show WHY a non-member is retained. Targeted stream_ids
        // queries (hot path, client.rs) keep the membership-only behaviour.
        if full_dump {
            for (eid, e) in s.extents.iter() {
                if !member_ids.contains(eid) {
                    extents.push((*eid, e.clone()));
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
        let payload_location = self.payload_location_of(req.extent_id).as_byte();
        let s = self.store.inner.borrow();
        match s.extents.get(&req.extent_id) {
            Some(e) => Ok(rkyv_encode(&ExtentInfoResp {
                code: CODE_OK,
                message: String::new(),
                extent: Some(e.clone()),
                payload_location,
            })),
            None => Ok(rkyv_encode(&ExtentInfoResp {
                code: CODE_NOT_FOUND,
                message: format!("extent {} not found", req.extent_id),
                extent: None,
                payload_location,
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

    /// cluster-df (MSG_CLUSTER_DF): leader-only read of the in-memory capacity
    /// snapshot `node_health_loop` maintains. Raw u64 facts only — the
    /// consumer (autumn-op df / fuse statfs) computes the amplification factor
    /// (`physical_used/logical_stored`) and the EC-dependent writable RANGE.
    /// No scan / no compute here (done off the request path); O(per_node).
    /// Pure builder for the cluster-df capacity snapshot (no encode). Shared by
    /// the `MSG_CLUSTER_DF` handler and the in-process embedded dashboard
    /// (`dashboard.rs::overview_json`) so both read the identical facts without
    /// a self-RPC round-trip.
    pub(crate) fn compute_cluster_df_resp(&self) -> ClusterDfResp {
        if !self.leader.get() {
            // A follower's snapshot is replay-stale + its node_health_loop
            // doesn't run — answer NOT_LEADER so the caller rotates.
            return ClusterDfResp {
                code: CODE_NOT_LEADER,
                message: "not leader".to_string(),
                raw_total: 0,
                raw_free: 0,
                physical_used: 0,
                logical_stored: 0,
                logical_open_tail: 0,
                logical_wal_debt: 0,
                node_count: 0,
                last_update_ms: 0,
                logical_last_update_ms: 0,
                per_node: Vec::new(),
            };
        }
        let snap = self.cluster_cap.borrow();
        let per_node = snap
            .per_node
            .iter()
            .map(|(id, c)| NodeCapWire {
                node_id: *id,
                total: c.total,
                free: c.free,
                extent_bytes: c.extent_bytes,
                online: c.online,
            })
            .collect();
        ClusterDfResp {
            code: CODE_OK,
            message: String::new(),
            raw_total: snap.raw_total,
            raw_free: snap.raw_free,
            physical_used: snap.physical_used,
            logical_stored: snap.logical_stored,
            logical_open_tail: snap.logical_open_tail,
            logical_wal_debt: snap.logical_wal_debt,
            node_count: snap.node_count,
            last_update_ms: snap.last_update_ms,
            logical_last_update_ms: snap.logical_last_update_ms,
            per_node,
        }
    }

    async fn handle_cluster_df(&self) -> HandlerResult {
        Ok(rkyv_encode(&self.compute_cluster_df_resp()))
    }

    /// nodes with an in-flight Recovery targeting `extent_id`.
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

    /// minimum number of committed (non-catching-up) members that
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

    /// pure WAS-faithful commit/seal-length decision (unit-tested,
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
    /// **WAS seal-over-reachable (the bug-#3 fix).** Earlier revisions required
    /// EVERY committed member to respond (`reachable == committed`), else
    /// `Err` — consistency-over-availability. But a node kill+restart leaves
    /// a committed member unreachable/behind that is NOT in `recovering`
    /// (recovery is fence-gated), so the seal blocked forever → the
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
        responses: &std::collections::HashMap<u64, u64>,
        floor: usize,
    ) -> std::result::Result<(u64, u32), String> {
        let mut min_len: Option<u64> = None;
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
            if let Err(err) = Self::ensure_owner_epoch(&req.owner_key, req.owner_epoch, &s) {
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
                end: ex.sealed_length,
                last_ex_info: Some(ex.clone()),
            }));
        }

        // WAS-faithful commit-length read. The append path is
        // all-replica-ACK (`apply_completion` requires every replica to
        // ack), so every COMMITTED member holds >= the acked commit
        // length. Therefore `min` over the committed members never drops
        // acked data — PROVIDED we (a) exclude catching-up members
        // (in-flight Recovery, partial replica) from the min, and
        // (b) require all committed members to agree (no majority quorum
        // subset, which could seal below the acked length by including a
        // short catching-up replica, or above it by excluding a member).
        // probe committed members, then decide via the shared pure
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
        let mut responses: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
        // probe the replicas CONCURRENTLY, not one-at-a-time.
        // Each `commit_length_on_node` is bounded at 5 s; a SEQUENTIAL 3-replica
        // loop takes up to 3×5 s = 15 s when a replica is cold/hiccupping —
        // exactly the PS-side `CHECK_COMMIT_LENGTH` deadline (client.rs:2507),
        // so ONE slow replica timed out the whole partition-open probe. Seen
        // live: after a stop-the-world restart the PS opens all partitions
        // concurrently, firing 32×3 commit_length checks
        // at once while the ENs are still loading extents — the sequential
        // fanout amplified each EN hiccup 3×. Concurrent fanout bounds the
        // handler to max(5 s) = one replica's timeout. The manager is
        // single-threaded compio, so this just overlaps the 3 network waits on
        // one thread; per-EN fence side-effects (owner_epoch bump) are
        // independent across replicas so ordering doesn't matter.
        let to_probe: Vec<(u64, String)> = members
            .iter()
            .filter(|(_, node_id)| !recovering.contains(node_id))
            .filter_map(|(_, node_id)| nodes.get(node_id).map(|n| (*node_id, n.address.clone())))
            .collect();
        let probe_results =
            futures::future::join_all(to_probe.into_iter().map(|(node_id, addr)| async move {
                let r = self
                    .commit_length_on_node(&addr, ex.extent_id, req.owner_epoch)
                    .await;
                (node_id, addr, r)
            }))
            .await;
        for (node_id, addr, r) in probe_results {
            match r {
                Ok(v) => {
                    responses.insert(node_id, v);
                }
                // Errors are surfaced at WARN so a silently-routed-to-wrong-
                // shard misconfiguration (e.g. AUTUMN_EXTENT_SHARDS mismatch →
                // empty shard_ports → every probe lands on shard 0) shows up in
                // the manager log instead of only as "0/N members reachable".
                Err(e) => {
                    tracing::warn!(
                        extent_id = ex.extent_id,
                        node_id,
                        addr = %addr,
                        error = %e,
                        "check_commit_length: commit_length_on_node failed"
                    );
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

    /// Build a generic `CodeResp { code, message }` reply — the manager's most
    /// common response shape. Every handler that returns only a status + message
    /// (success, not-found, precondition, leader/owner/routable rejects) goes
    /// through here instead of repeating the `Ok(rkyv_encode(&CodeResp { .. }))`
    /// boilerplate.
    fn code_resp(code: u8, message: String) -> HandlerResult {
        Ok(rkyv_encode(&CodeResp { code, message }))
    }

    /// Build a `ForceEcConvertResp { code, message }` reply — the only response
    /// shape `handle_force_ec_convert` emits (idempotent-OK / out-of-policy
    /// precondition / not-leader / error). Same role as `code_resp`, distinct
    /// wire type.
    fn force_ec_resp(code: u8, message: String) -> HandlerResult {
        Ok(rkyv_encode(&ForceEcConvertResp { code, message }))
    }

    /// Build a `StreamAllocExtentResp` rejection (no stream/extent payload).
    /// Every guard in `handle_stream_alloc_extent` (leader / owner-epoch /
    /// not-found / in-flight / seal-probe / membership-CAS / mirror) returns one
    /// of these — centralising the `stream_info: None, last_ex_info: None`
    /// boilerplate keeps each guard a single line. The success + idempotent-no-op
    /// returns carry `Some(..)` payloads and stay inline.
    fn alloc_reject(code: u8, message: String) -> HandlerResult {
        Ok(rkyv_encode(&StreamAllocExtentResp {
            code,
            message,
            stream_info: None,
            last_ex_info: None,
        }))
    }

    /// style placement walk shared by `handle_create_stream` /
    /// `handle_stream_alloc_extent` / `handle_multi_modify_merge`: try
    /// `alloc_extent_on_node` on each selected node, walking the (shuffled)
    /// fallback pool on failure. Returns the placed `(node_ids, disk_ids)`,
    /// or `None` when the pool is exhausted — the caller emits its own typed
    /// reject response. Fallback-pool CONSTRUCTION stays at each call site
    /// (their filters differ, e.g. the exclude set).
    async fn place_extents_with_fallback(
        &self,
        selected: &[MgrNodeInfo],
        fallback_iter: &mut std::vec::IntoIter<MgrNodeInfo>,
        extent_id: u64,
    ) -> Option<(Vec<u64>, Vec<u64>)> {
        let mut node_ids = Vec::with_capacity(selected.len());
        let mut disk_ids = Vec::with_capacity(selected.len());
        for n in selected {
            let mut candidate = n.clone();
            let (node_id, disk) = loop {
                match self
                    .alloc_extent_on_node(&candidate.address, extent_id)
                    .await
                {
                    Ok(disk) => break (candidate.node_id, disk),
                    Err(_) => match fallback_iter.next() {
                        Some(alt) => candidate = alt,
                        None => return None,
                    },
                }
            };
            node_ids.push(node_id);
            disk_ids.push(disk);
        }
        Some((node_ids, disk_ids))
    }

    pub(crate) async fn handle_stream_alloc_extent(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Self::alloc_reject(Self::err_to_code(&err), err.to_string());
        }

        let req: StreamAllocExtentReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // capture the verified-online node set before borrowing
        // the store. See `handle_create_stream` for the same pattern.
        let online_node_ids = self.node_states.borrow().online_node_ids();
        let space_low_node_ids = self.space_low_node_ids();
        let hard_excluded = self.placement_excluded_node_ids();        let (mut tail, selected, extent_id, data, nodes_map) = {
            let mut s = self.store.inner.borrow_mut();
            if let Err(err) = Self::ensure_owner_epoch(&req.owner_key, req.owner_epoch, &s) {
                return Self::alloc_reject(Self::err_to_code(&err), err.to_string());
            }

            let stream = match s.streams.get(&req.stream_id).cloned() {
                Some(v) => v,
                None => {
                    return Self::alloc_reject(CODE_NOT_FOUND, format!("stream {}", req.stream_id))
                }
            };
            let tail_id = match stream.extent_ids.last().copied() {
                Some(v) => v,
                None => {
                    return Self::alloc_reject(CODE_NOT_FOUND, format!("tail extent in stream {}", req.stream_id))
                }
            };
            let tail = match s.extents.get(&tail_id).cloned() {
                Some(v) => v,
                None => {
                    return Self::alloc_reject(CODE_NOT_FOUND, format!("extent {tail_id}"))
                }
            };

            // BUG2-IDEMPOTENT-ROLL: the writer pinned `seal_extent_id` = the tail
            // it captured `seal_commit` for. If that is NO LONGER the current
            // tail, this is a STALE / RETRIED seal-and-roll: a prior attempt
            // already sealed that extent + rolled a fresh tail, but its response
            // was lost so the writer retried with the same `seal_commit`. Sealing
            // the current FRESH tail at the stale `seal_commit` would over-seal an
            // extent that does NOT durably hold that many bytes → the extent is
            // unrecoverable → any partition replaying it WAL-FAILSTOPs and never
            // opens (chaos seed=603 split-child wedge). Idempotent no-op: return
            // the current tail untouched (it IS the OPEN extent the first attempt
            // rolled) so the writer adopts it. `seal_extent_id == 0` = no pinned
            // target (probe / `None` seal) → fall through to the normal path.
            //
            // coco P1: gate on `!tail.sealed`. If the current tail is itself
            // SEALED (a later op rolled-and-sealed past the first attempt's fresh
            // tail), returning it would hand the writer a sealed extent as a
            // "fresh" tail → its appends fail → roll/retry wedge. In that case
            // fall through to the existing `already_sealed` path, which preserves
            // the seal AND allocates a NEW open tail to return.
            if req.seal_extent_id != 0 && tail_id != req.seal_extent_id && !tail.sealed {
                return Ok(rkyv_encode(&StreamAllocExtentResp {
                    code: CODE_OK,
                    message: String::new(),
                    stream_info: Some(stream.clone()),
                    last_ex_info: Some(tail.clone()),
                }));
            }

            // refuse-at-start. Symmetric to the EC refuse-at-start guards
            // (apply_recovery_done, mark_extent_available, handle_multi_modify_split)
            // and the GC guards (handle_stream_punch_holes, handle_truncate).
            // Without these guards, a concurrent EC conversion or recovery on the tail
            // extent would have its eversion+replicates writeback silently
            // overwritten by our verify-at-apply block below.
            // collapse the EC + Recovery refuse-at-start
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
                    return Self::alloc_reject(CODE_PRECONDITION, msg);
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
                &space_low_node_ids,
                &hard_excluded,
                data,
                &req.exclude_node_ids,
            ) {
                Ok(v) => v,
                Err(err) => {
                    return Self::alloc_reject(Self::err_to_code(&err), err.to_string())
                }
            };
            let (extent_id, _) = s.alloc_ids(1);
            (tail, selected, extent_id, data, s.nodes.clone())
        };

        // capture the tail's eversion BEFORE any mutation so the
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
        let min_len: Option<u64>;
        let avali: u32;
        if already_sealed {
            // Preserve the existing seal — do not touch sealed_length,
            // eversion, or avali. The new-tail allocation below proceeds.
            min_len = Some(tail.sealed_length);
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
            // sealed_length — the root fix for the phantom seal (seed=13
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
            // This previously took `min` over a majority-quorum subset of
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
            let mut responses: std::collections::HashMap<u64, u64> =
                std::collections::HashMap::new();
            // probe replicas CONCURRENTLY (same rationale as
            // handle_check_commit_length) — a sequential 3-replica loop at 5 s
            // each is a 15 s worst case on a cold/hiccupping EN. Fence
            // side-effects are per-EN independent, so ordering doesn't matter.
            let to_probe: Vec<(u64, String)> = members
                .iter()
                .filter(|(_, node_id)| !recovering.contains(node_id))
                .filter_map(|(_, node_id)| {
                    nodes_map.get(node_id).map(|n| (*node_id, n.address.clone()))
                })
                .collect();
            for (node_id, r) in
                futures::future::join_all(to_probe.into_iter().map(|(node_id, addr)| async move {
                    let r = self
                        .commit_length_on_node(&addr, tail.extent_id, req.owner_epoch)
                        .await;
                    (node_id, r)
                }))
                .await
            {
                if let Ok(v) = r {
                    responses.insert(node_id, v);
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
                    return Self::alloc_reject(Self::err_to_code(&err), err.to_string());
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
                return Self::alloc_reject(Self::err_to_code(&err), err.to_string());
            }
        };
        if !already_sealed {
            tail.sealed = true;
            tail.sealed_length = sealed_len;
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
            owner_epoch = req.owner_epoch,
            owner = %req.owner_key,
            "BUG2 alloc-seal applied"
        );
        // Suppress unused warning when `already_sealed` skips the real seal.
        let _ = sealed_len;

        // Allocate new extent on nodes with fallback
        let selected_ids: HashSet<u64> = selected.iter().map(|n| n.node_id).collect();
        // prefer fallbacks not in the writer's recent-failure set; fall
        // back to the unfiltered set if the exclusion would empty the iter.
        let exclude_set: HashSet<u64> = req.exclude_node_ids.iter().copied().collect();
        // hard-exclude fenced/maintenance/suspected at the source
        // so the `after_exclude.is_empty() → unfiltered` fallback can't re-admit
        // them either.
        let unfiltered: Vec<MgrNodeInfo> = nodes_map
            .values()
            .filter(|n| !selected_ids.contains(&n.node_id))
            .filter(|n| !hard_excluded.contains(&n.node_id))
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
        // walk fallbacks in random order — ID-sorted order
        // re-introduces the same low-ID bias that `select_nodes` was
        // changed to avoid.
        {
            use rand::seq::SliceRandom;
            fallback_nodes.shuffle(&mut rand::thread_rng());
        }
        let mut fallback_iter = fallback_nodes.into_iter();

        let Some((node_ids, disk_ids)) = self
            .place_extents_with_fallback(&selected, &mut fallback_iter, extent_id)
            .await
        else {
            let err = AppError::Precondition(format!(
                "no healthy node available to allocate extent {extent_id}"
            ));
            return Self::alloc_reject(Self::err_to_code(&err), err.to_string());
        };

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

        // compute stream_after without modifying store, mirror to
        // etcd FIRST, then apply to in-memory state on success.
        let (stream_after, alloc_stream_baseline) = {
            let s = self.store.inner.borrow();
            let st = match s.streams.get(&req.stream_id) {
                Some(v) => v,
                None => {
                    return Self::alloc_reject(CODE_NOT_FOUND, format!("stream {}", req.stream_id))
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

        // verify-BEFORE-mirror (replaces the earlier
        // verify-AFTER-mirror form). If a concurrent mutator
        // (recovery_done, ec_conversion_done, punch_holes, truncate,
        // split) bumped `tail.eversion` during our commit_length /
        // alloc_extent_on_node await window above, the etcd write we
        // would otherwise make is stale relative to live memory.
        //
        // The check previously ran AFTER the etcd mirror — when verify
        // failed, the client got `Precondition` but etcd had already
        // durable-committed the stale write. Failover replay then
        // re-loaded the stale write as if successful, while the client
        // believed the call failed. Linearization point unexplainable.
        //
        // Verify-BEFORE keeps both etcd and in-memory untouched on the
        // failure path. A narrow residual window remains (concurrent
        // mutation during the etcd mirror RTT itself); fully closing it
        // requires acquiring an exclusive ledger marker for the
        // alloc_extent op, which is filed as a follow-up (PS-layer
        // ops currently don't enroll in the ledger by design).
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
        // mirror RTT itself — the same follow-up window the eversion
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
                        return Self::alloc_reject(CODE_PRECONDITION, msg);
                    }
                }
                None => {
                    return Self::alloc_reject(CODE_NOT_FOUND, format!("stream {}", req.stream_id));
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
                    return Self::alloc_reject(CODE_PRECONDITION, msg);
                }
            };
            if live_eversion != expected_eversion {
                let msg = format!(
                    "extent {} eversion changed during alloc_extent \
                     ({} -> {}); retry with fresh snapshot",
                    tail.extent_id, expected_eversion, live_eversion
                );
                return Self::alloc_reject(CODE_PRECONDITION, msg);
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
            return Self::alloc_reject(Self::err_to_code(&err), err.to_string());
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

    /// Refuse a punch_holes / truncate when any to-be-removed extent has a
    /// stream-layer op in flight: recovery (only when the extent would drop
    /// to refs==0 — `refs==1 && vp_table_refs==0`) or EC conversion
    /// (unconditional). `op` names the operation for the error message.
    /// Pure read over `s`; shared refuse-at-start by
    /// handle_stream_punch_holes + handle_truncate.
    fn refuse_if_removed_extent_inflight(
        s: &autumn_common::MetadataState,
        removed: &HashSet<u64>,
        recovery_inflight_set: &HashSet<u64>,
        ec_inflight_set: &HashSet<u64>,
        op: &str,
    ) -> Result<(), AppError> {
        // If any extent that would drop to refs=0 is currently being
        // recovered, refuse the entire call.
        for eid in removed {
            if recovery_inflight_set.contains(eid) {
                if let Some(ex) = s.extents.get(eid) {
                    if ex.refs == 1 && ex.vp_table_refs == 0 {
                        return Err(AppError::Precondition(format!(
                            "extent {eid} has in-flight recovery; \
                             defer {op} until recovery completes"
                        )));
                    }
                }
            }
        }
        // Refuse if any to-be-removed extent is mid-EC.
        for eid in removed {
            if ec_inflight_set.contains(eid) {
                return Err(AppError::Precondition(format!(
                    "extent {eid} has in-flight EC conversion; \
                     defer {op} until conversion completes"
                )));
            }
        }
        Ok(())
    }

    /// For every extent in `removed`, compute its ref-drop effect and build
    /// `(extent_puts, extent_deletes, pending_deletes)`. An extent dropping
    /// to refs==0 that is physically deletable (`extent_can_delete`) and not
    /// EC-inflight goes to `extent_deletes` + gets a `PendingDelete`
    /// snapshot; otherwise refs is decremented (eversion bumped) into
    /// `extent_puts`. Pure read over `s` (returns clones). Shared by
    /// handle_stream_punch_holes + handle_truncate.
    fn compute_extent_ref_drops(
        s: &autumn_common::MetadataState,
        removed: &HashSet<u64>,
        ec_inflight_set: &HashSet<u64>,
    ) -> (
        Vec<MgrExtentInfo>,
        Vec<u64>,
        Vec<PendingDelete>,
        // Per-extent value-CAS baseline (`extents/<id>` == its value BEFORE this
        // refs-drop). The refs RMW reads `extent`, decrements, and writes
        // `extents/<id>` — but the txn historically value-CAS'd only
        // `streams/<id>` (membership). When the SAME CoW-shared extent (refs>=2
        // from a prior split) is punched/truncated CONCURRENTLY via two
        // DIFFERENT streams, both read the same refs and the second
        // `extents/<id>` write clobbered the first → a lost decrement leaves an
        // orphan (refs>0, in no stream) — and the symmetric lost-INCREMENT (a
        // racing split) would drop refs too low → premature delete → data loss.
        // CASing `extents/<id>` against this baseline makes the losing op fail
        // with Precondition and retry on a fresh read. Reproduced by
        // system_chaos seed 769351064 (extent 14 orphan-leak).
        Vec<(String, Vec<u8>)>,
    ) {
        let mut extent_puts = Vec::new();
        let mut extent_deletes = Vec::new();
        let mut pending_deletes = Vec::new();
        let mut extent_cas: Vec<(String, Vec<u8>)> = Vec::new();

        // Build pending_deletes snapshot for extents that would physically
        // delete (refs would hit 0 and not EC-inflight).
        for &eid in removed {
            if let Some(extent) = s.extents.get(&eid) {
                if extent.refs == 1 && extent.vp_table_refs == 0 && !ec_inflight_set.contains(&eid)
                {
                    let pending_addrs = Self::snapshot_replica_addrs(&s.nodes, eid, extent);
                    pending_deletes.push(PendingDelete {
                        extent_id: eid,
                        pending_addrs,
                        attempts: 0,
                    });
                }
            }
        }

        for extent_id in removed {
            if let Some(extent) = s.extents.get(extent_id) {
                // Capture the pre-drop value as the `extents/<id>` CAS baseline
                // for BOTH the put (refs decremented) and delete (refs->0) paths
                // — a delete must also fail if a concurrent split bumped refs.
                extent_cas.push((
                    format!("extents/{extent_id}"),
                    rkyv_encode(extent).to_vec(),
                ));
                let mut new_ext = extent.clone();
                if new_ext.refs <= 1 {
                    new_ext.refs = 0;
                    if Self::extent_can_delete(&new_ext) && !ec_inflight_set.contains(extent_id) {
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

        (extent_puts, extent_deletes, pending_deletes, extent_cas)
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

        // Snapshot the ConvertToEc + Recovery sets from the inflight ledger
        // once. Single-threaded compio — snapshot-then-consult preserves
        // semantics.
        let (ec_inflight_set, recovery_inflight_set) = self.inflight_snapshot_ec_recovery();

        // Etcd-first: compute mutations on clones (no store mutation), persist
        // to etcd, then apply — so a mirror failure (NotLeader / etcd
        // transient) leaves in-memory state unchanged.
        let out = {
            let guard = self.store.inner.borrow();
            let s: &autumn_common::MetadataState = &guard;
            (|| -> Result<
                (
                    MgrStreamInfo,
                    Vec<MgrExtentInfo>,
                    Vec<u64>,
                    Vec<PendingDelete>,
                    // CAS baseline = the stream's value BEFORE this punch
                    // (etcd currently holds it). The mirror value-CAS's
                    // `streams/<id>` against it.
                    Vec<u8>,
                    // Per-extent `extents/<id>` CAS baselines (see
                    // compute_extent_ref_drops) — guards the shared extent
                    // write against a concurrent CoW-stream punch/split.
                    Vec<(String, Vec<u8>)>,
                ),
                AppError,
            > {
                Self::ensure_owner_epoch(&req.owner_key, req.owner_epoch, s)?;
                let requested: HashSet<u64> = req.extent_ids.into_iter().collect();
                let stream = s
                    .streams
                    .get(&req.stream_id)
                    .ok_or_else(|| AppError::NotFound(format!("stream {}", req.stream_id)))?
                    .clone();
                let stream_baseline = rkyv_encode(&stream).to_vec();

                // Only operate on extents that actually belong to this
                // stream. Without this, a malformed request could decrement
                // refs on unrelated streams' extents.
                let members: HashSet<u64> = stream.extent_ids.iter().copied().collect();
                let removed: HashSet<u64> = requested
                    .into_iter()
                    .filter(|id| members.contains(id))
                    .collect();

                Self::refuse_if_removed_extent_inflight(
                    s,
                    &removed,
                    &recovery_inflight_set,
                    &ec_inflight_set,
                    "punch_holes",
                )?;

                let mut updated = stream;
                updated.extent_ids.retain(|id| !removed.contains(id));
                if updated.extent_ids.is_empty() {
                    return Err(AppError::Precondition(
                        "stream cannot be empty after punch holes".to_string(),
                    ));
                }
                let (extent_puts, extent_deletes, pending_deletes, extent_cas) =
                    Self::compute_extent_ref_drops(s, &removed, &ec_inflight_set);
                Ok((
                    updated,
                    extent_puts,
                    extent_deletes,
                    pending_deletes,
                    stream_baseline,
                    extent_cas,
                ))
            })()
        };

        match out {
            Ok((stream, extent_puts, extent_deletes, pending_deletes, stream_baseline, extent_cas)) => {
                // Persist to etcd FIRST. Failure → in-memory zero changes
                // (the closure above produced clones only).
                if let Err(err) = self
                    .mirror_stream_extent_mutation(
                        &stream,
                        &extent_puts,
                        &extent_deletes,
                        Some(stream_baseline),
                        extent_cas,
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
                for &eid in &extent_deletes {
                    if let Err(e) = self.forget_payload_location(eid).await {
                        tracing::warn!(
                            extent_id = eid,
                            error = %e,
                            "could not drop the deleted extent's payload-location key"
                        );
                    }
                }
                // Each enqueue is an etcd CAS via the inflight ledger; errors
                // are downgraded inside enqueue (WARN-logged) so a single
                // failed acquire doesn't fail the whole punch_holes call.
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

        // Snapshot ConvertToEc + Recovery inflight sets.
        let (ec_inflight_set, recovery_inflight_set) = self.inflight_snapshot_ec_recovery();

        // Etcd-first (same shape as handle_stream_punch_holes).
        let out = {
            let guard = self.store.inner.borrow();
            let s: &autumn_common::MetadataState = &guard;
            (|| -> Result<
                (
                    MgrStreamInfo,
                    Vec<MgrExtentInfo>,
                    Vec<u64>,
                    Vec<PendingDelete>,
                    // CAS baseline (stream value before this truncate).
                    Vec<u8>,
                    // Per-extent `extents/<id>` CAS baselines (see
                    // compute_extent_ref_drops).
                    Vec<(String, Vec<u8>)>,
                ),
                AppError,
            > {
                Self::ensure_owner_epoch(&req.owner_key, req.owner_epoch, s)?;
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

                Self::refuse_if_removed_extent_inflight(
                    s,
                    &removed,
                    &recovery_inflight_set,
                    &ec_inflight_set,
                    "truncate",
                )?;

                let mut updated = stream;
                updated.extent_ids.retain(|id| !removed.contains(id));
                let (extent_puts, extent_deletes, pending_deletes, extent_cas) =
                    Self::compute_extent_ref_drops(s, &removed, &ec_inflight_set);
                Ok((
                    updated,
                    extent_puts,
                    extent_deletes,
                    pending_deletes,
                    stream_baseline,
                    extent_cas,
                ))
            })()
        };

        match out {
            Ok((stream, extent_puts, extent_deletes, pending_deletes, stream_baseline, extent_cas)) => {
                if let Err(err) = self
                    .mirror_stream_extent_mutation(
                        &stream,
                        &extent_puts,
                        &extent_deletes,
                        Some(stream_baseline),
                        extent_cas,
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
                for &eid in &extent_deletes {
                    if let Err(e) = self.forget_payload_location(eid).await {
                        tracing::warn!(
                            extent_id = eid,
                            error = %e,
                            "could not drop the deleted extent's payload-location key"
                        );
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

    /// Snapshot `extent_id -> eversion` over every extent of `stream_ids`'
    /// streams — the verify-at-apply BASELINE both split and merge capture
    /// in Phase 1 (before the Phase-2 etcd await). `first_eversion_drift` is the
    /// matching verify side: after the await it refuses if any of these
    /// eversions moved (a concurrent recovery / EC / punch / truncate).
    fn snapshot_stream_extent_eversions(
        state: &autumn_common::MetadataState,
        stream_ids: &[u64],
    ) -> HashMap<u64, u64> {
        let mut m = HashMap::new();
        for &sid in stream_ids {
            if let Some(stream) = state.streams.get(&sid) {
                for &eid in &stream.extent_ids {
                    if let Some(ex) = state.extents.get(&eid) {
                        m.insert(eid, ex.eversion);
                    }
                }
            }
        }
        m
    }

    /// Verify-before-mirror drift check shared by split + merge: re-read the
    /// live eversion of every source extent snapshotted in `pre_bump_eversion`
    /// (captured before the Phase-1 awaits) and return the FIRST
    /// `(extent_id, expected, live)` that drifted, or `None` if all still
    /// match. The caller encodes its own handler-specific refusal response
    /// (CodeResp vs MultiModifyMergeResp). Refusing here — before the etcd
    /// txn — keeps a stale-base mutation from landing durably (committing
    /// then returning Precondition would leave etcd holding a write that
    /// replay loads as if successful).
    fn first_eversion_drift(&self, pre_bump_eversion: &HashMap<u64, u64>) -> Option<(u64, u64, u64)> {
        let s = self.store.inner.borrow();
        for (eid, expected) in pre_bump_eversion {
            if let Some(live) = s.extents.get(eid).map(|ex| ex.eversion) {
                if live != *expected {
                    return Some((*eid, *expected, live));
                }
            }
        }
        None
    }

    pub(crate) async fn handle_multi_modify_split(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Self::code_resp(Self::err_to_code(&err), err.to_string());
        }

        let req: MultiModifySplitReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // #6: serialize splits per partition. A PS that retries
        // multi_modify_split against a SLOW manager (each call timing out but
        // the manager still committing later) used to commit a SEPARATE split
        // per retry — a reproduced 1→6 partition cascade (scripts/split_repro6).
        // Refuse a concurrent request for the same partition; the RAII guard
        // clears it on every exit path. Only ONE split per partition can be
        // in-flight, so retries can't multiply into extra splits.
        let _split_guard = {
            let mut inflight = self.split_inflight.borrow_mut();
            if inflight.contains(&req.part_id) {
                return Self::code_resp(
                    CODE_PRECONDITION,
                    format!(
                        "split already in progress for partition {}; retry later",
                        req.part_id
                    ),
                );
            }
            inflight.insert(req.part_id);
            SplitInflightGuard {
                set: self.split_inflight.clone(),
                part_id: req.part_id,
            }
        };

        // Phase 1: Compute all mutations without modifying store
        // (only alloc_ids touches state.next_id, which is safe to waste on failure)
        let out = {
            let mut s = self.store.inner.borrow_mut();
            (|| -> Result<(
                Vec<MgrStreamInfo>,
                Vec<MgrExtentInfo>,
                MgrPartitionMeta,
                MgrPartitionMeta,
                HashMap<u64, u64>,
            ), AppError> {
                Self::ensure_owner_epoch(&req.owner_key, req.owner_epoch, &s)?;

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

                // reject split if any source-stream extent
                // is undergoing EC conversion. compute_duplicate_stream
                // bumps eversion on the source extents; if
                // apply_ec_conversion_done runs concurrently it would
                // overwrite those bumps. Fail fast — client retries with
                // backoff. Reads the unified ledger via
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
                // symmetric guard against in-flight recovery on any
                // source-stream extent. apply_recovery_done bumps eversion and
                // rewrites replicates; Phase-3's apply_split_mutations would
                // overwrite both with the Phase-1 captured snapshot.
                // read Recovery from the unified ledger.
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

                // snapshot pre-mutation eversions so Phase-3 can verify
                // no concurrent mutator ran during Phase-2's etcd await.
                let pre_bump_eversion = Self::snapshot_stream_extent_eversions(
                    &s,
                    &[src_meta.log_stream, src_meta.row_stream, src_meta.meta_stream],
                );

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

                Ok((new_streams, all_extents, left, right, pre_bump_eversion))
            })()
        };

        match out {
            Ok((new_streams, modified_extents, left, right, pre_bump_eversion)) => {
                // Verify-BEFORE-mirror: if any source-stream extent's
                // eversion drifted during the Phase-1 awaits, the etcd txn
                // we'd otherwise send is computed from a stale base — refuse
                // before committing to etcd.
                if let Some((eid, expected, live)) = self.first_eversion_drift(&pre_bump_eversion) {
                    return Self::code_resp(
                        CODE_PRECONDITION,
                        format!(
                            "extent {eid} eversion drift during split \
                             ({expected} -> {live}); retry split"
                        ),
                    );
                }

                // Phase 2: Persist ALL mutations to etcd in ONE atomic txn
                // (partitions + regions are included here, not in a separate
                // txn, to prevent orphan streams on crash.)
                if let Some(etcd) = &self.etcd {
                    let mut kvs =
                        Vec::with_capacity(new_streams.len() + modified_extents.len() + 4);
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
                    // Stamp last_op_at on both children so the
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
                    // Value-CAS each modified extent against its pre-split value.
                    // Split increments refs on CoW-shared extents (and seals the
                    // source tail); compute_duplicate_stream reads `state` and
                    // returns clones, so the in-memory store still holds the
                    // pre-mutation value here = the etcd baseline. Without this,
                    // a concurrent cross-partition punch/truncate on a CoW-shared
                    // extent could lose the refs increment -> refs too low ->
                    // premature delete -> data loss. Same class as
                    // compute_extent_ref_drops.
                    //
                    // No source streams/<id> CAS is needed even though split
                    // READS the source membership to derive the right streams:
                    // the source partition is frozen_for_split AND holds
                    // gc_gate+compact_gate through this whole multi_modify_split
                    // (PS-side gc/compact gates), so its streams cannot mutate
                    // concurrently. The ONLY reachable race is a DIFFERENT
                    // CoW-sharing partition (from a prior split) GC'ing a shared
                    // extent — that partition isn't frozen — and this extent CAS
                    // catches exactly that. (Split also has no Phase-1.5 await,
                    // so capturing the baseline here, with no await since
                    // modified_extents was computed, is consistent — unlike merge
                    // which must capture in Phase-1 before its alloc await.)
                    let extent_cas: Vec<(String, Vec<u8>)> = {
                        let s = self.store.inner.borrow();
                        modified_extents
                            .iter()
                            .filter_map(|ex| {
                                s.extents.get(&ex.extent_id).map(|orig| {
                                    (format!("extents/{}", ex.extent_id), rkyv_encode(orig).to_vec())
                                })
                            })
                            .collect()
                    };
                    etcd.put_delete_txn_cas(kvs, Vec::new(), extent_cas)
                        .await
                        .map_err(|e| Self::err_to_status(&e))?;
                }

                // Phase 3: Apply to in-memory store AFTER etcd success.
                // Verify moved up before the Phase-2 mirror; here we only
                // apply (no verify).
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
                    drop(s);
                    // in-memory last_op_at update (mirror of etcd write above)
                    let now = Self::epoch_seconds();
                    self.last_op_at.borrow_mut().insert(left_id, now);
                    self.last_op_at.borrow_mut().insert(right_id, now);
                }

                Self::code_resp(CODE_OK, String::new())
            }
            Err(err) => Self::code_resp(Self::err_to_code(&err), err.to_string()),
        }
    }

    // ── PartitionManagerService handlers ───────────────────────────────

    // ── handle_multi_modify_merge ─────────────────────────────────────
    // Inverse of handle_multi_modify_split. Atomically:
    //   - Splices victim's three streams' extent_ids into survivor's
    //   - Allocates a fresh log_stream tail extent (E_new) on K replicas
    //   - Widens survivor.rg.end_key to victim.rg.end_key
    //   - Deletes victim's partitions/streams/regions/partitionLastOp keys
    //
    // Single-txn etcd commit — crash mid-merge means no state
    // change. Inflight checks + verify-at-apply on
    // pre_bump_eversion. The leader fence is already applied via put_and_delete_txn.
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

        // capture verified-online node set BEFORE borrowing the
        // store. Passed into the Phase-1 select_nodes call.
        let online_node_ids = self.node_states.borrow().online_node_ids();
        let space_low_node_ids = self.space_low_node_ids();
        let hard_excluded = self.placement_excluded_node_ids();
        // Phase 1: compute under borrow_mut, NO awaits inside.
        // Returns alloc-IDs reserved + selected nodes for Phase 1.5.
        struct Phase1Result {
            new_streams: Vec<MgrStreamInfo>,
            modified_extents: Vec<MgrExtentInfo>,
            survivor_meta: MgrPartitionMeta,
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
            // Value-CAS baseline for each EXISTING modified extent (the
            // refs-spliced / tail-sealed ones), captured from the SAME Phase-1
            // snapshot `modified_extents` was computed against — NOT re-read at
            // Phase 2 (after the Phase-1.5 alloc await), which would capture a
            // value a concurrent punch/truncate already mutated and let the
            // merge clobber it (coco P1). A concurrently-deleted extent has no
            // etcd value here so its CAS fails -> merge retries (no resurrect).
            // The freshly-created new_tail has no pre-existing baseline and is
            // intentionally absent (it is a create, not a CAS'd update).
            extent_baselines: Vec<(String, Vec<u8>)>,
        }

        let phase1: Result<Phase1Result, AppError> = {
            let mut s = self.store.inner.borrow_mut();
            (|| -> Result<Phase1Result, AppError> {
                Self::ensure_owner_epoch(&req.owner_key, req.owner_epoch, &s)?;

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
                    // collapse the EC + Recovery + Delete checks
                    // into one ledger probe. This was previously three
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

                let pre_bump_eversion = Self::snapshot_stream_extent_eversions(&s, &all_streams);

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
                    Self::select_nodes(
                    &s.nodes,
                    &s.disks,
                    &online_node_ids,
                    &space_low_node_ids,
                    &hard_excluded,
                    target_replicas,
                    &[],
                )?;
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
                    req.log_sealed_lengths[0],
                    req.log_sealed_lengths[1],
                    new_tail.clone(),
                )?;
                let (row_dup, row_exts) = Self::splice_streams_without_new_tail(
                    &s,
                    survivor_meta.row_stream,
                    victim_meta.row_stream,
                    req.row_sealed_lengths[0],
                    req.row_sealed_lengths[1],
                )?;
                let (meta_dup, meta_exts) = Self::splice_streams_without_new_tail(
                    &s,
                    survivor_meta.meta_stream,
                    victim_meta.meta_stream,
                    req.meta_sealed_lengths[0],
                    req.meta_sealed_lengths[1],
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

                // Capture extent CAS baselines from THIS Phase-1 snapshot (the
                // value `modified_extents` was computed against). compute_* read
                // `s` and returned clones, so `s.extents` still holds the
                // pre-mutation value. The new_tail (just-built, not yet in
                // `s.extents`) has no baseline and is correctly skipped.
                let extent_baselines: Vec<(String, Vec<u8>)> = all_extents
                    .iter()
                    .filter_map(|ex| {
                        s.extents.get(&ex.extent_id).map(|orig| {
                            (format!("extents/{}", ex.extent_id), rkyv_encode(orig).to_vec())
                        })
                    })
                    .collect();

                let mut new_survivor_meta = survivor_meta.clone();
                new_survivor_meta.rg = Some(MgrRange {
                    start_key: s_rg.start_key,
                    end_key: v_rg.end_key,
                });

                Ok(Phase1Result {
                    new_streams,
                    modified_extents: all_extents,
                    survivor_meta: new_survivor_meta,
                    victim_part_id: req.victim_part_id,
                    victim_log: victim_meta.log_stream,
                    victim_row: victim_meta.row_stream,
                    victim_meta: victim_meta.meta_stream,
                    new_tail_id,
                    selected_nodes: selected,
                    new_tail_replicas: target_replicas as u32,
                    pre_bump_eversion,
                    survivor_stream_baselines,
                    extent_baselines,
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
                .filter(|n| !hard_excluded.contains(&n.node_id))                .cloned()
                .collect()
        };
        {
            use rand::seq::SliceRandom;
            fallback_nodes.shuffle(&mut rand::thread_rng());
        }
        let mut fallback_iter = fallback_nodes.into_iter();
        let Some((final_node_ids, final_disk_ids)) = self
            .place_extents_with_fallback(&p1.selected_nodes, &mut fallback_iter, p1.new_tail_id)
            .await
        else {
            return Ok(rkyv_encode(&MultiModifyMergeResp {
                code: CODE_PRECONDITION,
                message: format!(
                    "no healthy node available to allocate E_new {}",
                    p1.new_tail_id
                ),
                new_log_tail_extent_id: 0,
            }));
        };

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

        // Verify-BEFORE-mirror: if any source-stream extent's eversion
        // drifted during the Phase-1.5 awaits (alloc_extent_on_node for
        // E_new across each replica node), the etcd txn we'd send is
        // computed from a stale base. Refuse before committing.
        if let Some((eid, expected, live)) = self.first_eversion_drift(&p1.pre_bump_eversion) {
            return Ok(rkyv_encode(&MultiModifyMergeResp {
                code: CODE_PRECONDITION,
                message: format!(
                    "extent {eid} eversion drift during merge \
                     ({expected} -> {live}); retry merge"
                ),
                new_log_tail_extent_id: 0,
            }));
        }

        // Phase 2: single fenced etcd txn.
        if let Some(etcd) = &self.etcd {
            let now = Self::epoch_seconds();
            let mut kvs = Vec::with_capacity(p1.new_streams.len() + modified_extents.len() + 5);
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
                format!("regions/{}", p1.victim_part_id),
                format!("partitionLastOp/{}", p1.victim_part_id),
            ];
            // Item 3 (uniform CAS): value-CAS each survivor stream against its
            // pre-splice baseline so a concurrent alloc/punch/truncate that
            // committed on a survivor stream during this RTT makes the merge
            // fail+retry (CODE_PRECONDITION) instead of overwriting it with the
            // stale spliced membership (resurrecting a removed extent).
            // CAS = survivor-stream membership baselines + each modified extent's
            // baseline. Both were captured from the Phase-1 snapshot (NOT re-read
            // here after the Phase-1.5 await), so a concurrent punch/truncate/
            // split that mutated or deleted a CoW-shared extent during the await
            // makes this txn fail+retry instead of clobbering it / resurrecting
            // a deleted extent (coco P1). Same lost-update class the
            // compute_extent_ref_drops CAS closes for punch/truncate.
            //
            // The deleted VICTIM streams need no membership CAS: the victim is
            // frozen_for_merge so no concurrent alloc can ADD an extent
            // to it (coco's orphan-via-alloc scenario is precluded). The only
            // reachable concurrent victim mutation is a GC punch/truncate, which
            // ALSO writes the affected extents/<id> (refs-- or delete) — and
            // splice_victim_extents baselines EVERY victim extent into
            // extent_baselines, so that write trips the extent CAS above ->
            // merge retries. (Survivor-side concurrent punches trip
            // survivor_stream_baselines.) No victim membership CAS adds coverage.
            let mut cas = p1.survivor_stream_baselines.clone();
            cas.extend(p1.extent_baselines.clone());
            etcd.put_delete_txn_cas(kvs, deletes, cas)
                .await
                .map_err(|e| Self::err_to_status(&e))?;
        }

        // Phase 3: in-memory apply. Verify moved up before the
        // Phase-2 mirror; here we only apply.
        {
            let mut s = self.store.inner.borrow_mut();
            Self::apply_merge_mutations(
                &mut s,
                &p1.new_streams,
                &modified_extents,
                p1.survivor_meta.clone(),
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

    // ── handle_merge_partitions (orchestrated merge) ─────────────
    //
    // Wraps the multi-modify-merge txn with a PrepareMerge-style
    // freeze sequence, mirroring TiKV's pattern of letting the leader-
    // fenced control plane drive the cross-PS choreography. The sequence:
    //
    //   1. ensure_leader (manager state belongs to one instance only)
    //   2. resolve survivor + victim part_addr / stream ids in one borrow
    //   3. acquire admin owner-lock (so the embedded MultiModifyMerge txn
    //      has a fresh owner_epoch the leader fence can act on)
    //   4. send MSG_MERGE_FREEZE to victim's PS, await OK
    //      (drains pending+inflight + flushes imm; no new writes accepted)
    //   5. send MSG_MERGE_FREEZE to survivor's PS, await OK
    //   6. capture commit_length × 6 (3 streams × 2 partitions) — these
    //      are the sealed_lengths that the manager merge txn will use
    //   7. invoke handle_multi_modify_merge synchronously (existing
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

        // step 4: merging destroys the boundary between the
        // two partitions — which is the start_key of whichever one sits on the
        // RIGHT. If an operator declared that boundary via presplit, undoing it
        // is almost never what they meant, and the damage is silent: for fs lane
        // boundaries every SUBSEQUENT large file stripes narrower, with no error
        // and no log line, so it surfaces only as "throughput got worse at some
        // point". Refuse by default; `--force` for the deliberate case.
        if !req.force {
            let boundary = {
                let s = self.store.inner.borrow();
                let start_of = |pid: u64| -> Option<Vec<u8>> {
                    s.partitions
                        .get(&pid)?
                        .rg
                        .as_ref()
                        .map(|r| r.start_key.clone())
                };
                match (start_of(req.survivor_part_id), start_of(req.victim_part_id)) {
                    // The vanishing boundary is the greater of the two starts.
                    (Some(a), Some(b)) => Some(a.max(b)),
                    _ => None,
                }
            };
            if let Some(owner) = boundary.as_deref().and_then(|b| self.sacred_boundary_owner(b)) {
                return Ok(rkyv_encode(&MergePartitionsResp {
                    code: CODE_PRECONDITION,
                    message: format!(
                        "refusing to merge {} into {}: the boundary between them is a presplit \
                         point declared for namespace '{}'. Merging it would silently undo that \
                         layout (for fs lane boundaries: every later large file stripes \
                         narrower, with no error). Re-run with --force if that is intended.",
                        req.victim_part_id, req.survivor_part_id, owner
                    ),
                    new_log_tail_extent_id: 0,
                }));
            }
        }

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
        let owner_epoch = match self.acquire_owner_epoch(&owner_key).await {
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

        // Freeze budget: measured from BEFORE the first freeze is issued (the
        // earliest a PS could have started its FREEZE_TTL clock) so the elapsed
        // check below is conservative. The txn must land within
        // MERGE_FREEZE_COMMIT_DEADLINE of here or we abort rather than seal at a
        // possibly-stale commit_length.
        let freeze_start = Instant::now();

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

        // Capture commit_length on each of the 6 streams. Reuse the
        // existing handle_check_commit_length so we hit the same
        // sealed-vs-live + min-replica path the merge txn code expects.
        let read_commit_len = |stream_id: u64| {
            let owner_key = owner_key.clone();
            async move {
                let req = CheckCommitLengthReq {
                    stream_id,
                    owner_key,
                    owner_epoch,
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
                // Pass the REAL committed length, INCLUDING 0. A frozen tail
                // whose all-replica commit is 0 (empty — e.g. a freshly-rolled
                // victim log tail that took no writes) MUST seal at
                // `sealed_length = 0`, not 1. `compute_merge_streams` /
                // `splice_streams_without_new_tail` seal an empty tail as
                // `sealed = true, sealed_length = 0` (manager note 32, after the
                // `&& *_sealed > 0` guard was dropped) → sealed-empty is
                // recoverable: each child allocs a fresh tail and replay reads 0
                // bytes there. The OLD `.max(1)` over-sealed an empty spliced
                // VICTIM log tail at byte 1; on cold reopen the survivor's WAL
                // replay reaches that extent, expects 1 byte, finds 0, and trips
                // WAL-FAILSTOP "got 0 of 1 expected bytes" → the merge survivor
                // is permanently un-openable (reproduced deterministically:
                // /tmp/soak/repro.sh round 1, survivor part 15, log extent 68).
                // The stale "0 = no-op / use existing" comment predates note 32;
                // neither compute fn nor handle_multi_modify_merge special-cases
                // 0. (handle_check_commit_length already returns Err — caught
                // above — when a replica is unreachable, so OK+0 = genuinely
                // empty, never a masked failure: the phantom-seal hazard the split-side
                // `unwrap_or(0).max(1)` fix addressed does not apply here.)
                Ok::<u64, (StatusCode, String)>(resp.end as u64)
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

        // Test failpoint (always 0 in production): simulate a slow/paused
        // coordinator between the commit_length capture and the txn so the
        // freeze-budget guard below is exercised deterministically (no real
        // SIGSTOP needed).
        {
            let pause = MERGE_TEST_PAUSE_MS.load(std::sync::atomic::Ordering::Relaxed);
            if pause > 0 {
                compio::time::sleep(Duration::from_millis(pause)).await;
            }
        }

        // Freeze-budget guard (fixes the merge-freeze lost-update): if the
        // freeze has been held long enough that the PS-side FREEZE_TTL could
        // have lapsed and resumed writes on the victim tail, DO NOT commit — the
        // captured commit_lengths may be stale and the txn would seal the tail
        // BELOW post-unfreeze acked writes (silent lost update). Roll back the
        // freezes and abort; the merge is retryable (auto-policy re-evaluates).
        if freeze_start.elapsed() >= MERGE_FREEZE_COMMIT_DEADLINE {
            let held = freeze_start.elapsed();
            rollback(to_unfreeze.clone(), self.conn_pool.clone()).await;
            return Ok(rkyv_encode(&MergePartitionsResp {
                code: Self::err_to_code(&AppError::Precondition(String::new())),
                message: format!(
                    "merge freeze budget exceeded: held {:.1}s >= {:.1}s deadline \
                     (PS FREEZE_TTL may have lapsed); aborting to avoid a stale-length seal — retry",
                    held.as_secs_f64(),
                    MERGE_FREEZE_COMMIT_DEADLINE.as_secs_f64(),
                ),
                new_log_tail_extent_id: 0,
            }));
        }

        // Run the existing merge txn under the same owner-lock.
        let mmm_req = MultiModifyMergeReq {
            survivor_part_id: req.survivor_part_id,
            victim_part_id: req.victim_part_id,
            owner_key: owner_key.clone(),
            owner_epoch,
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

    /// actively re-spread partitions across the registered
    /// PS fleet (most-loaded → least-loaded, bounded by `max_moves`). Leader-
    /// only. Each move rewrites a region's `ps_id`; the old PS's
    /// `sync_regions_once` drops the partition and the new PS opens it (the same
    /// mechanism eviction-driven reassignment already uses). `rg` is unchanged
    /// so `region_epoch` is NOT bumped — the key RANGE didn't move, only which
    /// PS serves it; clients re-resolve the partition's listener via the
    /// refreshed `part_addr`.
    ///
    /// Persistence mirrors the eviction path (memory-first, then
    /// `mirror_partition_snapshot` — the regions/ mirror re-reads the store so
    /// it captures any concurrent change): apply in-memory, then persist. On a
    /// mirror failure the in-memory reassignment stands for this leader's life
    /// but a leader failover replays the (stale) etcd regions — idempotent, the
    /// operator just re-runs `rebalance`.
    ///
    /// SAFETY of moving a partition off a still-LIVE PS (no freeze/drain needed):
    /// the new PS's `open_partition` acquires the per-partition owner epoch
    /// (`partition/<id>`) BEFORE it reads `commit_length` — so the old owner is
    /// fenced (manager equality + EN floor) before the new owner snapshots the
    /// tail. Any write the old PS durably ack'd happened before that fence and
    /// is therefore ≤ the new owner's commit_length (no lost update); a write
    /// the old PS accepted but couldn't ack (its append now fails
    /// `LockedByOther` → self-eviction) is retried by the client onto the new
    /// PS. This is exactly the failover/eviction handoff — a pure PS reassignment
    /// needs no merge-style freeze (data isn't combined; the target reopens from
    /// the durable streams). `region_epoch` is NOT bumped (the key range is
    /// unchanged); routing follows the partition's `part_addr`, which we clear
    /// below so clients stop resolving to the old PS immediately.
    pub(crate) async fn handle_rebalance_regions(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&RebalanceRegionsResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                moves: vec![],
                moved: 0,
            }));
        }
        let req: RebalanceRegionsReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // Compute + apply in-memory under one borrow (no await inside).
        let moves = {
            let mut s = self.store.inner.borrow_mut();
            let moves = Self::compute_rebalance_moves(&s, req.max_moves);
            for m in &moves {
                if let Some(r) = s.regions.get_mut(&m.part_id) {
                    r.ps_id = m.to_ps;
                }
                // Drop the stale per-partition listener addr (registered by the
                // OLD PS). `GetRegions` filters `part_addrs` only by "region
                // still exists", and the client prefers `part_addrs[part_id]`
                // over the PS base address — so without this a refreshed client
                // keeps routing to the old PS's (soon-closed) listener until the
                // new PS reopens + re-registers. Removing it makes the client
                // fall back to the target PS immediately (self-heal then
                // installs the new addr on the new PS's first sync).
                s.part_addrs.remove(&m.part_id);
            }
            moves
        };

        if !moves.is_empty() {
            if let Err(err) = self.mirror_partition_snapshot().await {
                return Ok(rkyv_encode(&RebalanceRegionsResp {
                    code: Self::err_to_code(&err),
                    message: format!("rebalance persist failed: {err}"),
                    moves: vec![],
                    moved: 0,
                }));
            }
            tracing::info!("rebalance: moved {} partition(s)", moves.len());
        }

        let moved = moves.len() as u32;
        Ok(rkyv_encode(&RebalanceRegionsResp {
            code: CODE_OK,
            message: String::new(),
            moves,
            moved,
        }))
    }

    /// `MSG_OP_SUBMIT` — record a long-running op in the ledger and actuate it in
    /// the background, returning the assigned `op_id` immediately. Leader-gated +
    /// admin-gated (the dispatch already stripped/verified the token prefix).
    pub(crate) async fn handle_op_submit(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&OpSubmitResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                op_id: 0,
            }));
        }
        let req: OpSubmitReq = rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // Validate kind + the target its actuation needs, up front — a malformed
        // submit is a request error (no ledger entry), not a FAILED op.
        let need_part = matches!(
            req.kind,
            OP_KIND_SPLIT | OP_KIND_COMPACT | OP_KIND_GC | OP_KIND_FORCE_GC
        );
        let bad = match req.kind {
            // Recovery is AUTO-dispatched by the recovery loop (it repairs a
            // degraded replica when one is detected); there is no meaningful
            // operator "start a recovery" — it appears in the ledger on its own.
            OP_KIND_RECOVERY => Some(
                "recovery is auto-dispatched, not submittable — watch it with \
                 `ops list --kind recovery`",
            ),
            OP_KIND_SPLIT | OP_KIND_MERGE | OP_KIND_REBALANCE | OP_KIND_COMPACT | OP_KIND_GC
            | OP_KIND_FORCE_GC | OP_KIND_EC_CONVERT => {
                if need_part && req.part_id == 0 {
                    Some("this op requires a part_id")
                } else if req.kind == OP_KIND_MERGE
                    && (req.part_id == 0 || req.secondary_id == 0)
                {
                    Some("merge requires survivor part_id + victim secondary_id")
                } else if req.kind == OP_KIND_EC_CONVERT && req.secondary_id == 0 {
                    Some("ec-convert requires an extent id (secondary_id)")
                } else if req.kind == OP_KIND_FORCE_GC && req.extent_ids.is_empty() {
                    Some("forcegc requires at least one extent id")
                } else {
                    None
                }
            }
            _ => Some("unknown op kind"),
        };
        if let Some(msg) = bad {
            return Ok(rkyv_encode(&OpSubmitResp {
                code: CODE_INVALID_ARGUMENT,
                message: msg.to_string(),
                op_id: 0,
            }));
        }

        let (now_s, now_ms) = Self::now_s_ms();
        let requested_by = if req.requested_by.is_empty() {
            "cli".to_string()
        } else {
            req.requested_by.clone()
        };
        let (op_id, attached) = self.ops.borrow_mut().submit(
            req.kind,
            req.part_id,
            req.secondary_id,
            req.extent_ids.clone(),
            requested_by,
            now_s,
            now_ms,
        );
        if attached {
            return Ok(rkyv_encode(&OpSubmitResp {
                code: CODE_OK,
                message: "attached to an in-flight op with the same target".to_string(),
                op_id,
            }));
        }
        // Actuate in the background; the caller polls `ops status`.
        let mgr = self.clone();
        compio::runtime::spawn(async move { mgr.run_submitted_op(op_id, req).await }).detach();
        Ok(rkyv_encode(&OpSubmitResp {
            code: CODE_OK,
            message: String::new(),
            op_id,
        }))
    }

    /// `MSG_OP_QUERY` — `ops status <id>` (one record, UNKNOWN if not in this
    /// leader's ledger) or `ops list` (filtered). Leader-gated (a follower's
    /// ledger is empty).
    pub(crate) async fn handle_op_query(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&OpQueryResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                ops: vec![],
            }));
        }
        let req: OpQueryReq = rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let ops = self.ops.borrow().query(&req);
        Ok(rkyv_encode(&OpQueryResp {
            code: CODE_OK,
            message: String::new(),
            ops,
        }))
    }

    // ── handle_get_policy_candidates / handle_report_partition_load ──

    pub(crate) async fn handle_get_policy_candidates(&self, _payload: Bytes) -> HandlerResult {
        // leader gate. The handler previously returned
        // `advisory_cache` on any node, but only the leader's
        // `policy_tick_loop` populates the cache (follower's stays
        // empty). An external controller polling `MSG_GET_POLICY_CANDIDATES`
        // against a follower silently received an empty candidate list
        // — indistinguishable from "nothing to do" — and would never
        // notice it was asking the wrong node. Same fix pattern as
        // the `handle_get_partition_detail` gate.
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

    // ── auto-policy controller RPCs (headless control) ────

    pub(crate) async fn handle_autopolicy_get(&self, _payload: Bytes) -> HandlerResult {
        // Leader-only: the live state + action log are leader-local, and the
        // controller loop only runs on the leader (a follower's replayed config
        // is stale). Sister to the MSG_GET_POLICY_CANDIDATES gate.
        if !self.leader.get() {
            return Ok(rkyv_encode(&AutoPolicyGetResp {
                code: CODE_NOT_LEADER,
                message: "not leader".to_string(),
                mode: 0,
                active: String::new(),
                allow_mutations: false,
                policies: Vec::new(),
                log: Vec::new(),
            }));
        }
        Ok(rkyv_encode(&self.autopolicy_snapshot()))
    }

    pub(crate) async fn handle_autopolicy_set(&self, payload: Bytes) -> HandlerResult {
        let req: AutoPolicySetReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        // `autopolicy_set` gates on leader (its etcd write is leader-fenced) and
        // returns the resulting state; map any AppError to a coded resp.
        match self.autopolicy_set(req.op, req.mode, req.name, req.entry).await {
            Ok(resp) => Ok(rkyv_encode(&resp)),
            Err(e) => Ok(rkyv_encode(&AutoPolicySetResp {
                code: Self::err_to_code(&e),
                message: e.to_string(),
                mode: 0,
                active: String::new(),
                policies: Vec::new(),
            })),
        }
    }

    /// const-dump of the `POLICY_KIND_*` enum so external
    /// controllers can introspect the wire mapping at startup rather
    /// than hardcoding numeric values that may have drifted across
    /// docs/code (an earlier off-by-one was caused by exactly
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
        // Collect PS-reported maintenance outcomes before the loop consumes
        // `req.partitions`; reconciled into the op-ledger after the metrics
        // borrow is released (append_audit is async).
        let outcomes: Vec<autumn_rpc::manager_rpc::MaintenanceOutcome> = req
            .partitions
            .iter()
            .flat_map(|l| l.maintenance_outcomes.iter().cloned())
            .collect();
        let mut p = self.policy.borrow_mut();
        // honour the configured `window_buckets / bucket_sec`
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
        // Reconcile PS-executed op outcomes into the ledger (known op_id only,
        // idempotent) and audit each terminal transition exactly once.
        for o in outcomes {
            let transitioned = self.ops.borrow_mut().reconcile_outcome(
                o.op_id,
                o.state,
                o.error.clone(),
                o.message.clone(),
                now,
            );
            if transitioned {
                // EVENT vs STATE: the ledger answers "how is this op doing"
                // and is a bounded in-memory ring that dies with the leader;
                // the audit entry records only a 0/1 result_code. Neither
                // preserves WHY. Emit the reason to the leader log, where it
                // is durable and can be scraped (the dashboard surfaces
                // notifications by reading these lines). Fires once per op —
                // `transitioned` is the idempotent first-claim.
                if o.state != OP_STATE_SUCCEEDED {
                    tracing::error!(
                        target: "autumn::op_event",
                        op_id = o.op_id,
                        kind = o.kind,
                        state = o.state,
                        error = %o.error,
                        message = %o.message,
                        "maintenance op FAILED"
                    );
                } else {
                    tracing::info!(
                        target: "autumn::op_event",
                        op_id = o.op_id,
                        kind = o.kind,
                        message = %o.message,
                        "maintenance op succeeded"
                    );
                }
                self.append_audit(MgrAuditEntry {
                    op: crate::op_kind_audit_code(o.kind),
                    node_id: 0,
                    extent_id: 0,
                    by: "cli".to_string(),
                    reason: String::new(),
                    result_code: if o.state == OP_STATE_SUCCEEDED { 0 } else { 1 },
                    result_message: if o.error.is_empty() { o.message } else { o.error },
                    ts_ns: 0,
                })
                .await;
            }
        }
        Self::code_resp(CODE_OK, String::new())
    }

    /// OP-driven per-extent EC convert trigger. Validates the
    /// extent is sealed, not already converted, and references an
    /// EC-policy stream. Persists a rich `pending_ec_dispatch` marker
    /// to etcd + memory; the next `ec_conversion_dispatch_loop` tick
    /// (within ~5 s) drains it via the replay path and runs the
    /// existing 2PC encode + commit flow.
    ///
    /// Idempotent: re-invocation against an already-pending or
    /// already-converted extent returns CODE_OK. Out-of-policy
    /// requests (non-EC stream, sealed_length=0, missing extent)
    /// return CODE_PRECONDITION with a descriptive message.
    pub(crate) async fn handle_force_ec_convert(&self, payload: Bytes) -> HandlerResult {
        if !self.leader.get() {
            return Self::force_ec_resp(CODE_NOT_LEADER, "not leader".to_string());
        }
        let req: ForceEcConvertReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let extent_id = req.extent_id;

        // already in-flight (any stream-layer op)? Idempotent OK
        // for the ConvertToEc case (caller's intent matches the in-flight
        // op); Precondition for Recovery / Delete (different ops, retry
        // later).
        match self.extent_inflight_op(extent_id) {
            Some(crate::extent_inflight::ExtentOpKind::ConvertToEc) => {
                return Self::force_ec_resp(CODE_OK, "already pending dispatch".to_string());
            }
            Some(other) => {
                return Self::force_ec_resp(
                    CODE_PRECONDITION,
                    format!(
                        "extent {extent_id} has in-flight {other:?}; retry after it completes"
                    ),
                );
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
                    return Self::force_ec_resp(
                        CODE_PRECONDITION,
                        format!("extent {extent_id} not found"),
                    );
                }
            };
            if ex.sealed_length == 0 {
                return Self::force_ec_resp(
                    CODE_PRECONDITION,
                    format!(
                        "extent {extent_id} not sealed (sealed_length=0); use GC for empty slots"
                    ),
                );
            }
            if ex.ec_converted {
                return Self::force_ec_resp(
                    CODE_OK,
                    format!("extent {extent_id} already ec_converted"),
                );
            }
            let stream = s
                .streams
                .values()
                .find(|st| st.ec_parity_shard > 0 && st.extent_ids.contains(&extent_id));
            let stream = match stream {
                Some(s) => s.clone(),
                None => {
                    return Self::force_ec_resp(
                        CODE_PRECONDITION,
                        format!(
                            "extent {extent_id} is not on an EC-policy stream (set-stream-ec first)"
                        ),
                    );
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
        // `ec_conversion_dispatch_loop` did in its earlier fresh-scan path.
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
                    return Self::force_ec_resp(
                        CODE_PRECONDITION,
                        format!("target node {nid} not in nodes map"),
                    );
                }
            }
        }

        if total_shards > target_nodes.len() {
            let extra_needed = total_shards - target_nodes.len();
            let hard_excluded = self.placement_excluded_node_ids();            let extra_candidates: Vec<_> = {
                use rand::seq::SliceRandom;
                let s = self.store.inner.borrow();
                let existing: HashSet<u64> = target_nodes.iter().copied().collect();
                let mut pool: Vec<_> = s
                    .nodes
                    .values()
                    .filter(|n| !existing.contains(&n.node_id))
                    .filter(|n| !hard_excluded.contains(&n.node_id))
                    .cloned()
                    .collect();
                pool.shuffle(&mut rand::thread_rng());
                pool.into_iter().take(extra_needed).collect()
            };
            if extra_candidates.len() < extra_needed {
                return Self::force_ec_resp(
                    CODE_PRECONDITION,
                    format!(
                        "not enough nodes for EC {data_shards}+{parity_shards} ({} of {total_shards} available)",
                        target_nodes.len() + extra_candidates.len()
                    ),
                );
            }
            for node in &extra_candidates {
                match self.alloc_extent_on_node(&node.address, extent_id).await {
                    Ok(disk_id) => {
                        target_nodes.push(node.node_id);
                        extra_disk_ids.push(disk_id);
                    }
                    Err(e) => {
                        return Self::force_ec_resp(
                            CODE_ERROR,
                            format!("alloc_extent_on_node({}): {e}", node.address),
                        );
                    }
                }
            }
        }
        target_nodes.truncate(total_shards);

        // verify-BEFORE-acquire (revised after codex review of
        // the initial verify-AFTER-acquire form). The race we close:
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
        // **Why verify-before, not verify-after:** an initial
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
                return Self::force_ec_resp(
                    CODE_PRECONDITION,
                    format!(
                        "extent {extent_id} removed during force-ec-convert (concurrent gc)"
                    ),
                );
            }
        };
        if live_eversion != pre_eversion {
            return Self::force_ec_resp(
                CODE_PRECONDITION,
                format!(
                    "extent {extent_id} eversion changed during force-ec-convert \
                     (pre={pre_eversion}, live={live_eversion}); retry to pick up new state"
                ),
            );
        }

        let new_eversion = live_eversion + 1;

        // Tier 2: capture the current owner_lock owner_epoch for the
        // partition that owns this extent. Threaded through dispatch ->
        // coord -> WriteShard/CommitEcShard so a fenced ex-coord's
        // in-flight 2PC is rejected by remote ENs once
        // `auto_abandon_for_fenced_node` bumps their `entry.owner_epoch`
        // via fence-handover. CoW-shared extents (refs >= 2) appear in
        // multiple partitions' streams; any of them works because all
        // sharing partitions hold the same owner_lock owner_epoch at any
        // moment (revisions are bumped uniformly by the fence-handover).
        // Seed only. The dispatch loop re-resolves this every tick
        // (`dispatch_owner_epoch_for_extent`) because the epoch is bumped by any
        // partition reopen; a frozen value fences the conversion out of its own
        // participants forever.
        let dispatch_revision: i64 = {
            let s = self.store.inner.borrow();
            crate::recovery::dispatch_owner_epoch_for_extent(&s, extent_id)
        };

        let dispatch_record = MgrEcDispatchInflight {
            extent_id,
            target_nodes,
            extra_disk_ids,
            data_shards: data_shards as u32,
            new_eversion,
            owner_epoch: dispatch_revision,
        };

        // acquire the unified inflight marker. CAS via
        // create_revision==0 + leader fence in a single etcd txn —
        // replaces the earlier `persist_ec_conversion_inflight + in-memory
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
            return Self::force_ec_resp(
                match &e {
                    AppError::Precondition(_) => CODE_PRECONDITION,
                    AppError::NotLeader => CODE_NOT_LEADER,
                    _ => CODE_ERROR,
                },
                format!("acquire marker: {e}"),
            );
        }

        // Both forms coexist from staging until the reconcile reclaims the
        // pre-conversion `.dat` (see `apply_placements`), so the window is
        // bounded by the sweep interval rather than unbounded — but a stalled
        // reconcile still holds it open, which is worth a line when a
        // conversion begins.
        tracing::info!(
            extent_id,
            "EC conversion started: both the shards and the pre-conversion .dat \
             occupy disk until the reconcile reclaims the .dat"
        );

        Self::force_ec_resp(
            CODE_OK,
            format!(
                "marker persisted for extent {extent_id}; next ec dispatch tick (~5s) will convert"
            ),
        )
    }

    /// external policy controller — return the manager's most
    /// recent cached `PartitionLoad` for `part_id`. Sourced from the
    /// last bucket of `PolicyEngine.metrics`, populated by
    /// `MSG_REPORT_PARTITION_LOAD`. Lets `client info --detail`
    /// surface per-partition metrics without a dedicated PS RPC.
    /// Pure builder for one partition's latest cached load metrics. Shared by
    /// the `MSG_GET_PARTITION_DETAIL` handler and the in-process dashboard
    /// (`dashboard.rs::partition_detail_json`).
    ///
    /// followers' `policy.metrics` is empty (only the leader's
    /// policy_tick_loop populates it from MSG_REPORT_PARTITION_LOAD). Without
    /// this gate, querying a follower silently returned `CODE_OK` + all-zero
    /// PartitionLoad — operators couldn't tell "no metrics yet" from "queried
    /// the wrong node".
    pub(crate) fn compute_partition_detail_resp(&self, part_id: u64) -> GetPartitionDetailResp {
        if !self.leader.get() {
            return GetPartitionDetailResp {
                code: CODE_NOT_LEADER,
                message: "not leader".to_string(),
                load: PartitionLoad::default(),
                bucket_ts: 0,
            };
        }
        let p = self.policy.borrow();
        let bucket = p.metrics.get(&part_id).and_then(|w| w.buckets.back());
        let (load, bucket_ts) = match bucket {
            Some((ts, l)) => (l.clone(), *ts),
            None => (PartitionLoad::default(), 0),
        };
        GetPartitionDetailResp {
            code: CODE_OK,
            message: String::new(),
            load,
            bucket_ts,
        }
    }

    pub(crate) async fn handle_get_partition_detail(&self, payload: Bytes) -> HandlerResult {
        let req: GetPartitionDetailReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        Ok(rkyv_encode(&self.compute_partition_detail_resp(req.part_id)))
    }

    /// Dashboard compact overview: per-partition rollup (range / ps / live_size
    /// / extent count) + per-node extent-shard count, computed entirely from
    /// in-memory state with NO extent-node probe and NO per-extent array on the
    /// wire. Bounded by partition + node count, so a web dashboard scales to
    /// 数千 partition / 数万 extent. Leader-gated (a follower's `regions` /
    /// `extents` are replay-stale; the dashboard should scrape the leader).
    /// Pure builder for the cluster overview (no encode). Shared by the
    /// `MSG_GET_CLUSTER_OVERVIEW` handler and the in-process embedded dashboard
    /// (`dashboard.rs::overview_json`).
    pub(crate) fn compute_cluster_overview_resp(&self) -> GetClusterOverviewResp {
        if !self.leader.get() {
            return GetClusterOverviewResp {
                code: CODE_NOT_LEADER,
                message: "not leader".to_string(),
                partitions: Vec::new(),
                nodes: Vec::new(),
                total_req_per_sec: 0,
                total_write_bytes_per_sec: 0,
                total_read_bytes_per_sec: 0,
                ps_count: 0,
            };
        }
        let s = self.store.inner.borrow();
        // Latest reported load per partition (policy window; defaults if no report).
        let pol = self.policy.borrow();
        // (req_per_sec, write_bytes_per_sec, read_bytes_per_sec)
        let load_of = |pid: u64| -> (u64, u64, u64) {
            pol.metrics
                .get(&pid)
                .and_then(|w| w.buckets.back())
                .map(|(_, l)| (l.req_per_sec as u64, l.write_bytes_per_sec, l.read_bytes_per_sec))
                .unwrap_or((0, 0, 0))
        };

        // Per-node extent-shard count = how many extents list the node in
        // replicates ∪ parity (the one fact a client can't derive without the
        // full extent array). One pass over all extents.
        let mut node_ext: std::collections::HashMap<u64, u32> = std::collections::HashMap::new();
        for e in s.extents.values() {
            for nid in e.replicates.iter().chain(e.parity.iter()) {
                *node_ext.entry(*nid).or_insert(0) += 1;
            }
        }
        let mut nodes: Vec<NodeOverview> = s
            .nodes
            .values()
            .map(|n| NodeOverview {
                node_id: n.node_id,
                address: n.address.clone(),
                extent_count: node_ext.get(&n.node_id).copied().unwrap_or(0),
            })
            .collect();
        nodes.sort_by_key(|n| n.node_id);

        // latest PS-reported open-tail bytes (Σ committed
        // length on the partition's log/row/meta OPEN tails). The manager's
        // sealed-length sum below is authoritative for SEALED extents but an
        // open tail's `sealed_length` is 0, so a compacted / log-heavy
        // partition whose data lives entirely in open tails would render 0 B.
        // Adding the PS-reported open-tail bytes makes the overview match what
        // `autumn-op info --part` gets by probing the EN — without a
        // per-partition EN probe here (stays one-RPC / scalable). 0 when the
        // PS hasn't reported yet (falls back to the sealed sum alone).
        let open_tail_of = |pid: u64| -> u64 {
            pol.metrics
                .get(&pid)
                .and_then(|w| w.buckets.back())
                .map(|(_, l)| l.open_tail_bytes)
                .unwrap_or(0)
        };

        // Per-partition rollup. live_size = Σ distinct extents' sealed_length
        // over the partition's 3 streams (manager-authoritative) + the
        // PS-reported open-tail committed bytes.
        let partitions: Vec<PartitionOverview> = s
            .regions
            .values()
            .map(|r| {
                let ps_addr = s
                    .part_addrs
                    .get(&r.part_id)
                    .or_else(|| s.ps_nodes.get(&r.ps_id))
                    .cloned()
                    .unwrap_or_default();
                let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
                let mut live_size = 0u64;
                for sid in [r.log_stream, r.row_stream, r.meta_stream] {
                    if let Some(st) = s.streams.get(&sid) {
                        for eid in &st.extent_ids {
                            if seen.insert(*eid) {
                                if let Some(e) = s.extents.get(eid) {
                                    live_size = live_size.saturating_add(e.sealed_length);
                                }
                            }
                        }
                    }
                }
                // add the PS-reported open-tail bytes to
                // the authoritative sealed sum for the displayed size.
                let live_size = live_size.saturating_add(open_tail_of(r.part_id));
                let (range_start, range_end) = r
                    .rg
                    .as_ref()
                    .map(|g| (g.start_key.clone(), g.end_key.clone()))
                    .unwrap_or_default();
                PartitionOverview {
                    part_id: r.part_id,
                    ps_addr,
                    range_start,
                    range_end,
                    live_size,
                    total_extents: seen.len() as u32,
                    log_stream: r.log_stream,
                    row_stream: r.row_stream,
                    meta_stream: r.meta_stream,
                    req_per_sec: load_of(r.part_id).0,
                    write_bytes_per_sec: load_of(r.part_id).1,
                    read_bytes_per_sec: load_of(r.part_id).2,
                    ps_id: r.ps_id,
                }
            })
            .collect();

        let total_req_per_sec = partitions.iter().map(|p| p.req_per_sec).sum();
        let total_write_bytes_per_sec = partitions.iter().map(|p| p.write_bytes_per_sec).sum();
        let total_read_bytes_per_sec = partitions.iter().map(|p| p.read_bytes_per_sec).sum();
        let ps_count = s
            .regions
            .values()
            .map(|r| r.ps_id)
            .collect::<std::collections::HashSet<_>>()
            .len() as u32;

        GetClusterOverviewResp {
            code: CODE_OK,
            message: String::new(),
            partitions,
            nodes,
            total_req_per_sec,
            total_write_bytes_per_sec,
            total_read_bytes_per_sec,
            ps_count,
        }
    }

    pub(crate) async fn handle_get_cluster_overview(&self) -> HandlerResult {
        Ok(rkyv_encode(&self.compute_cluster_overview_resp()))
    }

    /// PS pushes a per-replica failure observation; manager
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
        // read quorum config from `AutumnManager` fields
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
            // taken by `node_health_loop` on a failed DF.
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
        Self::code_resp(CODE_OK, String::new())
    }

    pub(crate) async fn handle_register_ps(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Self::code_resp(Self::err_to_code(&err), err.to_string());
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
            return Self::code_resp(Self::err_to_code(&err), err.to_string());
        }
        Self::code_resp(CODE_OK, String::new())
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
        // routing comes from the LEADER only. A follower's replayed
        // regions look plausible but its `part_addrs` (in-memory, healed
        // by PSes against the LEADER) is empty/stale — serving them
        // black-holed every client that connected to a freshly-rejoined
        // follower first (manager-HA chaos H3). NOT_LEADER makes callers
        // rotate (client `refresh_regions`, PS `sync_regions_once`).
        // etcd-chaos D1 refinement: `ensure_routable` (not the strict
        // leader gate) — an UN-DISPLACED ex-leader during an etcd outage
        // holds the freshest routing in existence and nothing can
        // supersede it while etcd is down; gating it black-holed every
        // fresh client for the whole outage. The H3 case stays gated
        // (a rejoined follower is `displaced`).
        if let Err(err) = self.ensure_routable() {
            return Ok(rkyv_encode(&GetRegionsResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                regions: Vec::new(),
                ps_details: Vec::new(),
                part_addrs: Vec::new(),
            }));
        }
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
        // per-partition listener addresses. Only emit entries for
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
        // a follower answering OK pins the PS's shared manager
        // rotation to itself forever (the PS only rotates on failure) —
        // while its region/part_addr serving is gated. NOT_LEADER tells
        // the PS heartbeat loop to rotate (and burn the leaderless exit
        // budget). etcd-chaos D1: `ensure_routable` — an un-displaced
        // ex-leader answers OK through an etcd outage so the PS fleet
        // never approaches its exit budget while reassignment is
        // impossible anyway.
        if let Err(err) = self.ensure_routable() {
            return Self::code_resp(Self::err_to_code(&err), err.to_string());
        }
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
            Self::code_resp(CODE_OK, String::new())
        } else {
            // Surface eviction so the PS can re-register instead of staying
            // invisible to clients (`ps=unknown` in `info` output).
            Self::code_resp(CODE_NOT_FOUND, format!("ps {} not registered", req.ps_id))
        }
    }

    /// Tell a node what it should be holding. It sends every `extent_id` it
    /// found on disk; the answer sorts them into `garbage` (not a member —
    /// delete everything) and `placements` (a member — here is the ONE payload
    /// file you should have). Anything the node holds beyond that is residue.
    ///
    /// Best-effort: failure is logged on the node side but doesn't block
    /// startup. Read-only with respect to manager state.
    pub(crate) async fn handle_reconcile_extents(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Ok(rkyv_encode(&ReconcileExtentsResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                garbage: Vec::new(),
                placements: Vec::new(),
            }));
        }
        let req: ReconcileExtentsReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;

        // WHO IS ASKING? Every verdict below is relative to one node — "you are
        // not a member of this", "your payload lives in that file" — so an
        // unidentified reporter gets NO verdict. Answering with a node_id of 0
        // would make the caller a member of nothing and mark its entire disk as
        // garbage, and because the grace counter is keyed by (node, extent),
        // several unidentified nodes would share one counter and burn the grace
        // in a single round each. FAIL CLOSED: never tell a caller you cannot
        // identify to delete anything.
        let node_id = {
            let s = self.store.inner.borrow();
            if req.node_id != 0 && s.nodes.contains_key(&req.node_id) {
                Some(req.node_id)
            } else if !req.node_uuid.is_empty() {
                s.nodes
                    .iter()
                    .find(|(_, n)| n.node_uuid == req.node_uuid)
                    .map(|(id, _)| *id)
            } else {
                None
            }
        };
        let Some(node_id) = node_id else {
            tracing::warn!(
                reported_node_id = req.node_id,
                node_uuid = %req.node_uuid,
                local_extents = req.extent_ids.len(),
                "reconcile_extents: cannot identify the reporting node — returning no verdict \
                 (nothing is deleted on behalf of an unidentified caller)"
            );
            return Ok(rkyv_encode(&ReconcileExtentsResp {
                code: CODE_OK,
                message: "reporter not identified; no verdict".to_string(),
                garbage: Vec::new(),
                placements: Vec::new(),
            }));
        };
        let req = ReconcileExtentsReq { node_id, ..req };

        // Two kinds of garbage, with deliberately different confidence:
        //
        //   1. the manager has FORGOTTEN the extent → collect immediately (it
        //      cannot become a member of something that no longer exists);
        //   2. the extent is alive but this node is NOT one of its members →
        //      collect only after the verdict has held for several rounds.
        //
        // (2) is what reaps the residue of a recovery that died mid-copy: the
        // partial file reloads as an ordinary extent, so the extent is very much
        // alive and (1) can never see it — which is why such a stub used to
        // survive forever. Membership is the right question because a non-member
        // copy is referenced by no VP, no SST and no checkpoint.
        //
        // The grace period exists because the membership view is momentarily
        // wrong in normal operation — `apply_recovery_done` swaps a slot, a
        // freshly promoted leader is still settling — and deleting real data on a
        // transient is far worse than holding residue for a few more minutes.
        const NON_MEMBER_ROUNDS_BEFORE_GC: u32 = 3;
        let mut placements: Vec<ExtentPlacement> = Vec::new();
        let garbage: Vec<u64> = {
            let s = self.store.inner.borrow();
            let mut seen = self.reconcile_non_member.borrow_mut();
            // Only keep counters for extents this SHARD still reports, so the
            // map cannot grow without bound. Scoping to the shard matters: its
            // siblings share this node_id and report disjoint extents, so a
            // node-wide prune would let each sibling erase the others' grace
            // and no verdict would ever reach three rounds.
            let reported: std::collections::HashSet<u64> = req.extent_ids.iter().copied().collect();
            seen.retain(|(n, sh, eid), _| {
                *n != req.node_id || *sh != req.shard_idx || reported.contains(eid)
            });

            req.extent_ids
                .iter()
                .copied()
                .filter(|eid| {
                    let Some(ex) = s.extents.get(eid) else {
                        // (1) unknown extent — immediate.
                        seen.remove(&(req.node_id, req.shard_idx, *eid));
                        return true;
                    };
                    // An op in flight on this extent means the file set is
                    // MID-CHANGE, so no verdict can be given: a participant
                    // staging a shard for a not-yet-flipped conversion holds a
                    // file the current layout does not name, and a recovery
                    // target is writing the copy that will make it a member.
                    // The node-side guards only see the ops that node itself
                    // runs — the manager is the only party that knows about an
                    // attempt driven from elsewhere.
                    if self.extent_inflight_op(*eid).is_some() {
                        seen.remove(&(req.node_id, req.shard_idx, *eid));
                        return false;
                    }
                    if let Some(slot) = Self::extent_nodes(ex)
                        .iter()
                        .position(|n| *n == req.node_id)
                    {
                        seen.remove(&(req.node_id, req.shard_idx, *eid));
                        // A member: tell it WHICH file it should be holding, so
                        // it can drop the other one. The slot in
                        // `replicates ++ parity` is this node's shard index.
                        placements.push(ExtentPlacement {
                            extent_id: *eid,
                            payload_location: self.payload_location_of(*eid).as_byte(),
                            shard_index: slot as u32,
                        });
                        return false;
                    }
                    // (2) non-member — count the rounds.
                    let c = seen.entry((req.node_id, req.shard_idx, *eid)).or_insert(0);
                    *c += 1;
                    *c >= NON_MEMBER_ROUNDS_BEFORE_GC
                })
                .collect()
        };
        if !garbage.is_empty() {
            tracing::info!(
                node_id = req.node_id,
                local_extents = req.extent_ids.len(),
                garbage = garbage.len(),
                "reconcile_extents: returning orphan list to node",
            );
        }
        Ok(rkyv_encode(&ReconcileExtentsResp {
            code: CODE_OK,
            message: String::new(),
            garbage,
            placements,
        }))
    }

    async fn handle_register_partition_addr(&self, payload: Bytes) -> HandlerResult {
        // deliberately NOT leader-gated. `part_addrs` is an
        // in-memory, etcd-less routing hint that is LOST on manager
        // restart; the PS self-heal in `sync_regions_once` re-reports it
        // every ~2 s when missing. Gating on leadership stretched the
        // post-restart outage by the whole election wait, and a follower
        // accepting the registration is harmless (idempotent overwrite,
        // refreshed continuously by the same PS tick after any failover).
        let req: RegisterPartitionAddrReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        // record the per-partition listener address. We do NOT
        // validate that `part_id` is owned by `ps_id` here: the manager's
        // region table is the source of truth for ownership, and the
        // mapping is re-validated on `GetRegions` (only partitions with
        // an assigned region are returned). Overwrites are allowed —
        // if a PS re-binds a partition on a new port (restart, split),
        // the latest report wins.
        let mut s = self.store.inner.borrow_mut();
        let _ = req.ps_id; // reserved for future validation
        s.part_addrs.insert(req.part_id, req.address);
        Self::code_resp(CODE_OK, String::new())
    }

    // ── admin & health RPCs ──────────────
    //
    // All admin-mutating handlers (node overrides, force_ec_convert, future
    // force_abandon_ec_marker) wrap their result in `append_audit`.
    // The audit append is best-effort: a failed audit write
    // logs WARN but doesn't surface to the caller (the primary
    // operation already succeeded).

    /// Pure builder for the per-node auto-state + override table. Shared by the
    /// `MSG_LIST_NODE_STATES` handler and the in-process dashboard
    /// (`dashboard.rs` node `auto_state` merge).
    pub(crate) fn compute_list_node_states_resp(&self) -> ListNodeStatesResp {
        if let Err(err) = self.ensure_leader() {
            return ListNodeStatesResp {
                code: Self::err_to_code(&err),
                message: err.to_string(),
                nodes: vec![],
            };
        }
        let (nodes_meta, overrides, snapshot) = {
            let s = self.store.inner.borrow();
            let nodes: Vec<(u64, String, String, Vec<u16>)> = s
                .nodes
                .iter()
                .map(|(id, n)| (*id, n.address.clone(), n.node_uuid.clone(), n.shard_ports.clone()))
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
            .map(|(node_id, address, node_uuid, shard_ports)| {
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
                    node_uuid,
                    shard_ports,
                }
            })
            .collect();
        out.sort_by_key(|e| e.node_id);
        ListNodeStatesResp {
            code: CODE_OK,
            message: String::new(),
            nodes: out,
        }
    }

    pub async fn handle_list_node_states(&self, _payload: Bytes) -> HandlerResult {
        Ok(rkyv_encode(&self.compute_list_node_states_resp()))
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

    // ── admin RPCs ────────────────────────────────────────────────

    pub async fn handle_fence_node(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Self::code_resp(Self::err_to_code(&err), err.to_string());
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
        Self::code_resp(code, message)
    }

    pub async fn handle_set_node_maintenance(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Self::code_resp(Self::err_to_code(&err), err.to_string());
        }
        let req: SetNodeMaintenanceReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        // zombie defense — refuse if node was decommissioned.
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
            return Self::code_resp(CODE_PRECONDITION, msg);
        }
        let node_uuid = self
            .store
            .inner
            .borrow()
            .nodes
            .get(&req.node_id)
            .map(|n| n.node_uuid.clone())
            .unwrap_or_default();
        let ovr = MgrNodeOverride {
            node_id: req.node_id,
            kind: NODE_OVERRIDE_MAINTENANCE,
            set_at: Self::epoch_seconds(),
            set_by: req.set_by.clone(),
            reason: req.reason.clone(),
            expire_at: req.expire_at,
            node_uuid,
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
                return Self::code_resp(Self::err_to_code(&err), err.to_string());
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
        Self::code_resp(CODE_OK, String::new())
    }

    pub async fn handle_clear_node_override(&self, payload: Bytes) -> HandlerResult {
        if let Err(err) = self.ensure_leader() {
            return Self::code_resp(Self::err_to_code(&err), err.to_string());
        }
        let req: ClearNodeOverrideReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let key = format!("{}{}", crate::NODE_OVERRIDE_PREFIX, req.node_id);
        // M0: also lift the `decommissioned/` tombstone so this is
        // a REAL remedy for the re-register refusal message (the uuid-keyed
        // zombie check scans that prefix; without clearing it, a removed node
        // could never rejoin under its old identity).
        let tomb_key = format!("{}{}", crate::DECOMMISSIONED_PREFIX, req.node_id);
        if let Some(etcd) = &self.etcd {
            if let Err(err) = etcd
                .put_and_delete_txn(Vec::new(), vec![key, tomb_key])
                .await
            {
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
                return Self::code_resp(Self::err_to_code(&err), err.to_string());
            }
        }
        self.node_overrides.borrow_mut().remove(&req.node_id);
        self.decommissioned.borrow_mut().remove(&req.node_id);
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
        Self::code_resp(CODE_OK, String::new())
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

    // inner helpers — separated so the handlers' audit-wrap is
    // tight + the unit-test surface is direct.

    async fn fence_node_impl(&self, req: &FenceNodeReq) -> Result<(), AppError> {
        if !self.store.inner.borrow().nodes.contains_key(&req.node_id) {
            return Err(AppError::NotFound(format!(
                "node {} not registered",
                req.node_id
            )));
        }
        // #5 capacity precheck (unless --force).
        if !req.force {
            self.check_capacity_for_fence(req.node_id)?;
        }
        // Persist the override. Capture the node's stable
        // uuid so the tombstone-by-uuid re-register check survives a later
        // `remove_node` (which deletes `nodes/<id>`).
        let node_uuid = self
            .store
            .inner
            .borrow()
            .nodes
            .get(&req.node_id)
            .map(|n| n.node_uuid.clone())
            .unwrap_or_default();
        let ovr = MgrNodeOverride {
            node_id: req.node_id,
            kind: NODE_OVERRIDE_FENCED,
            set_at: Self::epoch_seconds(),
            set_by: req.set_by.clone(),
            reason: req.reason.clone(),
            expire_at: 0,
            node_uuid,
        };
        let key = format!("{}{}", crate::NODE_OVERRIDE_PREFIX, req.node_id);
        let value = rkyv_encode(&ovr).to_vec();
        if let Some(etcd) = &self.etcd {
            etcd.put_msgs_txn(vec![(key, value)]).await?;
        }
        self.node_overrides.borrow_mut().insert(req.node_id, ovr);
        // BUG #3 Layer B fix: do NOT bump partition owner-lock revisions when
        // fencing an EN data node. The owner-lock owner_epoch is the PARTITION
        // OWNER's (PS) token for split-brain prevention; an EN data node is
        // never a partition owner. The old bump
        // (`bump_owner_epochs_for_node`) walked every partition whose
        // log/row/meta stream merely had a REPLICA on the fenced node and
        // bumped THAT partition's owner owner_epoch — fencing out the legitimate
        // PS owner (which holds its acquire-time owner_epoch and never
        // re-acquires), so the PS's next append got CODE_LOCKED_BY_OTHER and
        // `partition_loop` self-poisoned + reopen-thrashed (the chaos seed=6
        // wedge after the Layer-A seal fix). It was also redundant: a fenced
        // EN is handled by the normal append-fail → seal-over-reachable (Layer
        // A) → alloc-new-extent path, and post-recovery topology changes are
        // picked up via EVERSION refresh, not owner-owner_epoch. Real split-brain
        // protection is the NEW PS's `acquire_owner_lock` on takeover (higher
        // owner_epoch), unaffected by this removal — see
        // `system_locked_by_other.rs::owner_lock_fencing_rejects_stale_revision`.
        // auto-abandon EC convert markers whose coord matches
        // the freshly-fenced node.
        let _ = self.auto_abandon_for_fenced_node(req.node_id).await;
        Ok(())
    }

    /// #5: refuse fence if the cluster doesn't have enough
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

    // BUG #3 Layer B: `bump_owner_epochs_for_node` was removed.
    // It bumped the PARTITION owner-lock owner_epoch of every partition whose
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
        // M0: capture the uuid BEFORE the `s.nodes.remove` below —
        // the tombstone is the only place the node_id→uuid mapping survives the
        // node-record deletion, and the re-register zombie check scans it by uuid.
        let removed_uuid = self
            .store
            .inner
            .borrow()
            .nodes
            .get(&req.node_id)
            .map(|n| n.node_uuid.clone())
            .unwrap_or_default();
        let tomb = MgrNodeOverride {
            node_id: req.node_id,
            kind: NODE_OVERRIDE_FENCED,
            set_at: now,
            set_by: req.set_by.clone(),
            reason: "removed".to_string(),
            expire_at: 0,
            node_uuid: removed_uuid,
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

    // ── recovery stats ────────────────────────────────────────────

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

    // ── audit log query ───────────────────────────────────────────

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

    // ── inode lease handlers ───────────────────────────────────────────────
    //
    // Plan reference: `docs/autumn_fs_lease_plan.md`. Manager is the
    // single decision-maker (§6 invariant 1); writer leases are
    // persisted to etcd (§3.1) while reader leases stay in-memory only
    // (§7 "lease 数量爆炸"). Every etcd write routes through
    // `put_msgs_txn` / `put_and_delete_txn` so the leader fence
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
        // coco P1 #6 + BUG-LEASE-4 (P1 #4): snapshot
        // the pre-acquire InodeLeaseState BEFORE mutating so an
        // etcd-write failure can ROLL BACK precisely — restoring
        // writer slot + version (the latter, BUG-LEASE-4 fix) without
        // touching readers that subscribed during the etcd await.
        // **BUG-LEASE-4:** ALSO collect the invalidation pushes the
        // force-revoke arm of `acquire_with_force` would have queued,
        // into a DeferredPushes bundle. The pushes are then flushed
        // only AFTER etcd commit — so an etcd-fail leaves clients
        // with NO phantom LeaseRevoked / WillRevokeIn events from a
        // preemption that didn't actually persist.
        let (pre_snapshot, outcome, mut deferred_pushes) = {
            let mut reg = self.inode_leases.borrow_mut();
            let snap = reg.inodes.get(&req.ino).cloned();
            let mut pushes = crate::inode_lease::DeferredPushes::default();
            let out = reg.acquire_with_force_deferred(
                &req.client,
                req.ino,
                req.mode,
                req.force,
                now,
                &mut pushes,
            );
            (snap, out, pushes)
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
                            // BUG-LEASE-4: precise revert that
                            // also rewinds the force-revoke
                            // version bump + DROPS the deferred
                            // pushes (clients never see the
                            // phantom revoke).
                            self.inode_leases
                                .borrow_mut()
                                .revert_writer_acquire(req.ino, pre_snapshot);
                            std::mem::take(&mut deferred_pushes); // drop without flush
                            return Ok(rkyv_encode(&AcquireLeaseResp {
                                code: Self::err_to_code(&e),
                                message: e.to_string(),
                                lease: None,
                            }));
                        }
                    }
                }
                // BUG-LEASE-4: etcd committed (or no etcd) → flush
                // the staged LeaseRevoked / WillRevokeIn pushes so
                // deposed writer + current readers see the revoke.
                self.inode_leases
                    .borrow_mut()
                    .flush_deferred_pushes(deferred_pushes);
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
            } => {
                // No pushes were staged for WriteConflict; the
                // borrow is dropped + bundle is empty. Belt-and-
                // braces explicit drop documents intent.
                drop(deferred_pushes);
                Ok(rkyv_encode(&AcquireLeaseResp {
                    code: CODE_PRECONDITION,
                    message: format!(
                        "writer lease held by kind={held_by_kind} host={held_by_host}"
                    ),
                    lease: None,
                }))
            }
            crate::inode_lease::AcquireOutcome::InvalidMode => {
                drop(deferred_pushes);
                Ok(rkyv_encode(&AcquireLeaseResp {
                    code: CODE_INVALID_ARGUMENT,
                    message: format!("invalid lease mode {}", req.mode),
                    lease: None,
                }))
            }
            // Pre-revocation grace window in
            // progress. Surface as `CODE_REVOKE_PENDING` with the
            // remaining milliseconds in `lease.ttl_secs` (we
            // repurpose this u32 field to carry the eta_ms so the
            // SDK can sleep precisely; documented on the wire
            // const). The lease itself is not yet granted —
            // `writer_present` is true because someone ELSE
            // holds it.
            //
            // BUG-LEASE-4: this arm produced the WillRevokeIn push
            // (the deferred bundle has 0 or 1 item). There is no
            // etcd persist for this arm — `pending_revoke_at`
            // lives in memory only — so we flush directly. (If
            // we EVER persist `pending_revoke_at`, the flush
            // moves below an etcd-write block and gets the same
            // commit/revert treatment as Granted.)
            crate::inode_lease::AcquireOutcome::RevokePending {
                eta_ms,
                held_by_kind,
                held_by_host,
            } => {
                self.inode_leases
                    .borrow_mut()
                    .flush_deferred_pushes(deferred_pushes);
                Ok(rkyv_encode(&AcquireLeaseResp {
                    code: CODE_REVOKE_PENDING,
                    message: format!(
                        "revoke pending: writer held by kind={held_by_kind} host={held_by_host}; retry in {eta_ms}ms"
                    ),
                    lease: Some(MgrInodeLeaseInfo {
                        ino: req.ino,
                        version: 0,
                        writer_present: true,
                        ttl_secs: eta_ms,
                    }),
                }))
            }
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

        // coco P2 #7: etcd-first ordering for the
        // writer-close case. Pre-fix the in-memory release ran first
        // (bumping version + pushing WriterClosed to readers); if
        // the subsequent etcd delete failed the client saw CODE_OK
        // but the persisted record stayed alive — a leader failover
        // would re-install the released writer, blocking new
        // acquires until the TTL revoke fires (≤ 30s). And the
        // released-then-re-revoked sequence would push a SECOND
        // WriterClosed at a higher version, forcing readers to
        // reload again. The reordered shape: preview the release →
        // delete etcd if it's a writer-close → only then commit the
        // memory mutate.
        let preview = self
            .inode_leases
            .borrow()
            .preview_release(&req.client, req.ino);

        if let crate::inode_lease::ReleaseOutcome::WriterClosed { .. } = preview {
            let key = format!("{}{}", crate::INODE_LEASES_PREFIX, req.ino);
            if let Some(etcd) = &self.etcd {
                if let Err(e) = etcd.put_and_delete_txn(vec![], vec![key]).await {
                    // Etcd failed; leave the in-memory writer in
                    // place + surface the error so the client
                    // retries. The TTL revoke loop is the eventual
                    // backstop if the client gives up.
                    return Ok(rkyv_encode(&ReleaseLeaseResp {
                        code: Self::err_to_code(&e),
                        message: e.to_string(),
                        new_version: None,
                    }));
                }
            }
        }

        // Either reader release / not-held (no etcd write), or
        // writer-close whose etcd delete just succeeded. Commit the
        // memory mutate now.
        let outcome = {
            let mut reg = self.inode_leases.borrow_mut();
            reg.release(&req.client, req.ino)
        };

        match outcome {
            crate::inode_lease::ReleaseOutcome::WriterClosed { new_version } => {
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
        // BUG-LEASE-1 (coco P0 #1, 2026-06-05): heartbeat MUST
        // refresh the persisted `MgrInodeLeaseRecord.expires_at`
        // for WRITER leases. Pre-fix the in-memory `writer_expires_at`
        // moved forward but etcd's record stayed at the original
        // acquire-time deadline — a manager failover would then
        // replay the stale deadline and a writer that had been
        // heartbeating happily for minutes could be erroneously
        // revoked the moment the new leader came up. Reader leases
        // aren't persisted (plan §6.4: subscribe-disconnect =
        // invalidate everything), so reader heartbeats don't write
        // etcd.
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
            } => {
                // BUG-LEASE-1 fix: if THIS client is the writer
                // (we are — heartbeat succeeded AND a writer holds
                // the slot AND it's us), refresh the persisted
                // record so failover replay sees the updated
                // deadline. Other shapes (reader heartbeat,
                // writer-not-me cases) leave etcd alone.
                //
                // coco R2-P0 #1 (round 2, 2026-06-06): the original
                // BUG-LEASE-1 fix used a BLIND `put_msgs_txn` —
                // between our in-memory `heartbeat()` mutation and
                // the etcd put, a concurrent `ReleaseLease` could
                // have deleted the etcd record, or a force-revoke +
                // new acquire could have overwritten it with a
                // DIFFERENT writer's record. The blind put would
                // then resurrect (release case) or overwrite (new
                // writer case) that change. The fix is
                // `EtcdMirror::read_then_cas_put`: read the current
                // record, CAS-put against it. If CAS fails (record
                // changed since our read) — silently skip; the
                // in-memory deadline is still good and the next
                // heartbeat reads the fresh state.
                if writer_present {
                    let is_writer = {
                        let reg = self.inode_leases.borrow();
                        let me = crate::inode_lease::ClientKey::from_wire(&req.client);
                        reg.inodes
                            .get(&req.ino)
                            .and_then(|s| s.writer.as_ref().map(|w| w == &me))
                            .unwrap_or(false)
                    };
                    if is_writer {
                        let record = self
                            .inode_leases
                            .borrow()
                            .writer_record(req.ino)
                            .expect("writer just heartbeated successfully");
                        let key = format!("{}{}", crate::INODE_LEASES_PREFIX, req.ino);
                        let value = rkyv_encode(&record).to_vec();
                        if let Some(etcd) = &self.etcd {
                            match etcd.read_then_cas_put(&key, value).await {
                                Ok(true) => {
                                    // Refresh landed; failover-safe.
                                }
                                Ok(false) => {
                                    // CAS conflict OR record deleted.
                                    // Concurrent Release / new-writer
                                    // Acquire raced us. In-memory
                                    // state already reflects this via
                                    // the writer-still-me check above
                                    // → if the writer slot is empty
                                    // or no longer ours, our
                                    // `is_writer` check would have
                                    // returned false. So this path is
                                    // "etcd diverged from our memory
                                    // momentarily" — next heartbeat
                                    // will reconcile.
                                    tracing::debug!(
                                        ino = req.ino,
                                        "BUG-LEASE-1 R2-P0 #1: heartbeat etcd CAS skipped (concurrent change); next heartbeat will retry"
                                    );
                                }
                                Err(e) => {
                                    // Genuine etcd error. In-memory
                                    // deadline is still good; the
                                    // worst-case failover window is
                                    // bounded by lease_ttl -
                                    // heartbeat_interval = 25s, same
                                    // as pre-R2 fix.
                                    tracing::warn!(
                                        ino = req.ino,
                                        error = %e,
                                        "BUG-LEASE-1: heartbeat etcd refresh failed; retry on next heartbeat"
                                    );
                                }
                            }
                        }
                    }
                }
                Ok(rkyv_encode(&HeartbeatLeaseResp {
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

        // Long-poll. Atomic drain-or-park: returns
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
        // sentinel MetaChanged event with ino=0. The long-poll
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

    /// M0: grant a batch of fuse-fs inode numbers. See
    /// `fs_alloc.rs` for the CAS grant loop + migration-floor semantics.
    pub async fn handle_alloc_inodes(&self, payload: Bytes) -> HandlerResult {
        let req: AllocInodesReq = rkyv_decode(&payload)
            .map_err(|e| (StatusCode::InvalidArgument, format!("decode: {e}")))?;
        if req.count == 0 {
            return Ok(rkyv_encode(&AllocInodesResp {
                code: CODE_INVALID_ARGUMENT,
                message: "count must be >= 1".to_string(),
                base: 0,
            }));
        }
        // SD-3 (review P2-4): the volume is concatenated into an etcd
        // counter key, so reject anything but empty or a canonical
        // `ns/tenant/volume/` prefix (prevents forging the global key / churning
        // another tenant's counter / non-canonical duplicate counters).
        if !crate::fs_alloc::valid_alloc_volume(&req.volume) {
            return Ok(rkyv_encode(&AllocInodesResp {
                code: CODE_INVALID_ARGUMENT,
                message: "volume must be empty or a canonical ns/tenant/volume/ prefix"
                    .to_string(),
                base: 0,
            }));
        }
        // Etcd write needs leader status; non-leader rejects with NOT_LEADER
        // so the client retries against the new leader (same pattern as
        // MSG_ACQUIRE_LEASE / MSG_ACQUIRE_OWNER_LOCK).
        if self.etcd.is_some() && !self.leader.get() {
            return Ok(rkyv_encode(&AllocInodesResp {
                code: CODE_NOT_LEADER,
                message: "not leader".to_string(),
                base: 0,
            }));
        }
        match self.alloc_fs_inodes(req.count as u64, req.floor, &req.volume).await {
            Ok(base) => Ok(rkyv_encode(&AllocInodesResp {
                code: CODE_OK,
                message: String::new(),
                base,
            })),
            Err(AppError::NotLeader) => Ok(rkyv_encode(&AllocInodesResp {
                code: CODE_NOT_LEADER,
                message: "deposed during grant".to_string(),
                base: 0,
            })),
            Err(e) => Ok(rkyv_encode(&AllocInodesResp {
                code: CODE_ERROR,
                message: format!("alloc_fs_inodes: {e}"),
                base: 0,
            })),
        }
    }
}

#[cfg(test)]
mod commit_seal_tests {
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
        resp.insert(1u64, 20_000_000u64);
        resp.insert(3u64, 20_000_000u64);
        resp.insert(5u64, 18_000_000u64);
        let (len, avali) = AutumnManager::compute_commit_seal(&m, &rec, &resp, 1).unwrap();
        assert_eq!(len, 18_000_000, "seal = min over all committed members");
        assert_eq!(avali, 0b111);
    }

    #[test]
    fn excludes_catching_up_member_does_not_crater_min() {
        // core invariant: slot 5 is catching-up (in-flight Recovery).
        // It holds only a partial replica, so it must NOT contribute to the
        // min. The seal = min over committed members {1,3} = 20 MB, NOT the
        // short value a catching-up replica would report (the production bug
        // cratered sealed_length to a recovery target's partial length).
        let m = members3();
        let rec: HashSet<u64> = [5u64].into_iter().collect();
        let mut resp = HashMap::new();
        resp.insert(1u64, 20_000_000u64);
        resp.insert(3u64, 20_000_000u64);
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
        resp.insert(1u64, 20_000_000u64);
        resp.insert(3u64, 20_000_000u64);
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
        resp.insert(1u64, 20_000_000u64);
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
        resp.insert(1u64, 20u64);
        resp.insert(3u64, 20u64);
        assert!(AutumnManager::compute_commit_seal(&m, &rec, &resp, 3).is_err());
        assert!(AutumnManager::compute_commit_seal(&m, &rec, &resp, 2).is_ok());
    }
}

#[cfg(test)]
mod selfheal_a5_tests {
    //! WAL self-heal A5: `handle_report_corrupt_replica` isolates a bit-rotted
    //! log_stream replica. Borrows then awaits; every borrow is dropped before
    //! the await (single-threaded test runtime).
    #![allow(clippy::await_holding_refcell_ref)]
    use crate::AutumnManager;
    use autumn_rpc::manager_rpc::*;
    use bytes::Bytes;

    fn run<F: std::future::Future<Output = T>, T>(f: F) -> T {
        compio::runtime::Runtime::new().unwrap().block_on(f)
    }

    const LOG_STREAM_ID: u64 = 700;

    /// A sealed 3-replica extent (slots 0/1/2 → nodes 1/3/5) with all avali
    /// bits set, owned by partition `part_id` at `owner_epoch`, and a member of
    /// log_stream `LOG_STREAM_ID`.
    fn seed(m: &AutumnManager, part_id: u64, owner_epoch: i64, extent_id: u64) {
        let mut s = m.store.inner.borrow_mut();
        s.owner_epochs
            .insert(format!("partition/{part_id}"), owner_epoch);
        s.partitions.insert(
            part_id,
            MgrPartitionMeta {
                part_id,
                log_stream: LOG_STREAM_ID,
                row_stream: 0,
                meta_stream: 0,
                rg: None,
            },
        );
        s.streams.insert(
            LOG_STREAM_ID,
            MgrStreamInfo {
                stream_id: LOG_STREAM_ID,
                extent_ids: vec![extent_id],
                ec_data_shard: 0,
                ec_parity_shard: 0,
                replicates: 3,
            },
        );
        s.extents.insert(
            extent_id,
            MgrExtentInfo {
                extent_id,
                replicates: vec![1, 3, 5],
                parity: vec![],
                eversion: 7,
                refs: 1,
                vp_table_refs: 0,
                sealed_length: 20_000_000,
                sealed: true,
                avali: 0b111,
                replicate_disks: vec![],
                parity_disks: vec![],
                ec_converted: false,
            },
        );
    }

    fn fire(
        m: &AutumnManager,
        part_id: u64,
        owner_epoch: i64,
        extent_id: u64,
        eversion: u64,
        corrupt: Vec<u64>,
    ) -> ReportCorruptReplicaResp {
        let req = ReportCorruptReplicaReq {
            partition_id: part_id,
            owner_epoch,
            log_stream_id: LOG_STREAM_ID,
            extent_id,
            eversion,
            corrupt_node_ids: corrupt,
        };
        let payload: Bytes = rkyv_encode(&req);
        let resp = run(async { m.handle_report_corrupt_replica(payload).await.unwrap() });
        rkyv_decode::<ReportCorruptReplicaResp>(&resp).expect("decode resp")
    }

    #[test]
    fn valid_report_clears_avali_bit_and_bumps_eversion() {
        let m = AutumnManager::new();
        seed(&m, 100, 42, 9);
        // node 3 = slot 1 reported corrupt.
        let r = fire(&m, 100, 42, 9, 7, vec![3]);
        assert_eq!(r.code, CODE_OK, "{}", r.message);
        let s = m.store.inner.borrow();
        let ex = s.extents.get(&9).unwrap();
        assert_eq!(ex.avali, 0b101, "slot 1 (node 3) bit cleared");
        assert_eq!(ex.eversion, 8, "eversion bumped to invalidate caches");
    }

    #[test]
    fn stale_owner_epoch_is_rejected() {
        let m = AutumnManager::new();
        seed(&m, 100, 42, 9);
        // reporter claims a stale owner_epoch (41 != current 42).
        let r = fire(&m, 100, 41, 9, 7, vec![3]);
        assert_eq!(r.code, CODE_PRECONDITION);
        let s = m.store.inner.borrow();
        let ex = s.extents.get(&9).unwrap();
        assert_eq!(ex.avali, 0b111, "no change on a fenced-out report");
        assert_eq!(ex.eversion, 7);
    }

    #[test]
    fn stale_eversion_is_rejected() {
        let m = AutumnManager::new();
        seed(&m, 100, 42, 9);
        // reporter saw eversion 6, manager now has 7 (concurrent op) → retry.
        let r = fire(&m, 100, 42, 9, 6, vec![3]);
        assert_eq!(r.code, CODE_PRECONDITION);
        let s = m.store.inner.borrow();
        assert_eq!(s.extents.get(&9).unwrap().avali, 0b111);
    }

    #[test]
    fn refuses_to_isolate_the_last_replicas() {
        let m = AutumnManager::new();
        seed(&m, 100, 42, 9);
        // all 3 reported corrupt → would clear every avali bit → refuse.
        let r = fire(&m, 100, 42, 9, 7, vec![1, 3, 5]);
        assert_eq!(r.code, CODE_PRECONDITION);
        let s = m.store.inner.borrow();
        assert_eq!(
            s.extents.get(&9).unwrap().avali,
            0b111,
            "unrecoverable: avali left intact so PS fails loud"
        );
    }

    #[test]
    fn open_extent_is_refused_pending_seal_and_roll() {
        let m = AutumnManager::new();
        seed(&m, 100, 42, 9);
        {
            let mut s = m.store.inner.borrow_mut();
            let ex = s.extents.get_mut(&9).unwrap();
            ex.sealed = false;
            ex.sealed_length = 0;
            ex.avali = 0; // open extents are avali=0 normally
        }
        let r = fire(&m, 100, 42, 9, 7, vec![3]);
        assert_eq!(r.code, CODE_PRECONDITION, "open → A4 seal-and-roll, not A5");
    }

    #[test]
    fn ec_converted_extent_is_refused() {
        let m = AutumnManager::new();
        seed(&m, 100, 42, 9);
        {
            let mut s = m.store.inner.borrow_mut();
            s.extents.get_mut(&9).unwrap().ec_converted = true;
        }
        let r = fire(&m, 100, 42, 9, 7, vec![3]);
        assert_eq!(r.code, CODE_PRECONDITION, "EC extent → replicated isolation N/A");
        let s = m.store.inner.borrow();
        assert_eq!(s.extents.get(&9).unwrap().avali, 0b111, "unchanged");
    }

    #[test]
    fn out_of_scope_extent_is_refused() {
        let m = AutumnManager::new();
        seed(&m, 100, 42, 9);
        // Report a DIFFERENT extent (8) that is not in the named log_stream.
        let req = ReportCorruptReplicaReq {
            partition_id: 100,
            owner_epoch: 42,
            log_stream_id: LOG_STREAM_ID,
            extent_id: 8,
            eversion: 7,
            corrupt_node_ids: vec![3],
        };
        let payload: Bytes = rkyv_encode(&req);
        let resp = run(async { m.handle_report_corrupt_replica(payload).await.unwrap() });
        let r = rkyv_decode::<ReportCorruptReplicaResp>(&resp).unwrap();
        assert_eq!(r.code, CODE_PRECONDITION, "extent not in log_stream → refused");
    }

    #[test]
    fn report_node_not_in_layout_is_refused_not_false_ok() {
        let m = AutumnManager::new();
        seed(&m, 100, 42, 9);
        // node 99 is not a replica (slots are 1/3/5) — stale/buggy report.
        // Must refuse (not falsely claim "isolated").
        let r = fire(&m, 100, 42, 9, 7, vec![99]);
        assert_eq!(r.code, CODE_PRECONDITION, "{}", r.message);
        let s = m.store.inner.borrow();
        assert_eq!(s.extents.get(&9).unwrap().avali, 0b111, "unchanged");
    }

    #[test]
    fn cross_partition_log_stream_is_refused() {
        let m = AutumnManager::new();
        seed(&m, 100, 42, 9);
        // Partition 100 owns LOG_STREAM_ID; claim a DIFFERENT log_stream id the
        // partition does not own → refused before any avali mutation.
        let req = ReportCorruptReplicaReq {
            partition_id: 100,
            owner_epoch: 42,
            log_stream_id: LOG_STREAM_ID + 1,
            extent_id: 9,
            eversion: 7,
            corrupt_node_ids: vec![3],
        };
        let payload: Bytes = rkyv_encode(&req);
        let resp = run(async { m.handle_report_corrupt_replica(payload).await.unwrap() });
        let r = rkyv_decode::<ReportCorruptReplicaResp>(&resp).unwrap();
        assert_eq!(r.code, CODE_PRECONDITION, "cross-partition stream → refused");
    }

    #[test]
    fn idempotent_when_bit_already_cleared() {
        let m = AutumnManager::new();
        seed(&m, 100, 42, 9);
        // node 3 already isolated (bit 1 clear).
        {
            let mut s = m.store.inner.borrow_mut();
            s.extents.get_mut(&9).unwrap().avali = 0b101;
        }
        let r = fire(&m, 100, 42, 9, 7, vec![3]);
        assert_eq!(r.code, CODE_OK, "{}", r.message);
        let s = m.store.inner.borrow();
        let ex = s.extents.get(&9).unwrap();
        assert_eq!(ex.avali, 0b101, "no-op, no further clear");
        assert_eq!(ex.eversion, 7, "no eversion bump on a no-op report");
    }
}

#[cfg(test)]
mod split_inflight_guard_tests {
    //! #6: a concurrent split request for a partition already being split is
    //! refused — so a PS retry storm against a slow manager can't commit a
    //! separate split per retry (the reproduced 1→6 cascade).
    use crate::AutumnManager;
    use autumn_rpc::manager_rpc::*;
    use bytes::Bytes;

    fn run<F: std::future::Future<Output = T>, T>(f: F) -> T {
        compio::runtime::Runtime::new().unwrap().block_on(f)
    }

    #[test]
    fn concurrent_split_for_same_partition_is_refused() {
        let m = AutumnManager::new(); // memory-mode = always leader
        // Simulate a split already in flight for partition 99 (the RAII guard a
        // first, slow handler invocation would hold).
        m.split_inflight.borrow_mut().insert(99);

        let req = MultiModifySplitReq {
            part_id: 99,
            owner_key: "partition/99".to_string(),
            owner_epoch: 1,
            mid_key: vec![0x80],
            log_stream_sealed_length: 1,
            row_stream_sealed_length: 1,
            meta_stream_sealed_length: 1,
        };
        let payload: Bytes = rkyv_encode(&req);
        let resp = run(async { m.handle_multi_modify_split(payload).await.unwrap() });
        let r: CodeResp = rkyv_decode(&resp).expect("decode");
        assert_eq!(
            r.code, CODE_PRECONDITION,
            "a concurrent split for the same partition must be refused"
        );
        assert!(
            r.message.contains("already in progress"),
            "actionable message: {}",
            r.message
        );
        // The guard set is untouched by the refused call (still just our entry).
        assert!(m.split_inflight.borrow().contains(&99));
    }
}

#[cfg(test)]
mod authz_kdc_tests {
    //! Stage 1 acceptance: drive the KDC handlers end-to-end in
    //! memory mode (leader=true, no etcd) — tenant-create → mint → publish
    //! config → verify → expiry-fail → byte-flip-fail → delete-stops-renewal.
    use super::*;
    use crate::AutumnManager;
    use autumn_rpc::cap_token::{public_key_from_seed, verify_token, AuthReject};
    use ed25519_dalek::VerifyingKey;

    /// A one-key signing file: kid=1, seed = 0x00..01.
    fn keyfile() -> String {
        "1 0000000000000000000000000000000000000000000000000000000000000001".to_string()
    }

    fn seed_1() -> [u8; 32] {
        let mut s = [0u8; 32];
        s[31] = 1;
        s
    }

    /// Build a kid→VerifyingKey resolver from a published config (what a PS does).
    fn resolver<'a>(cfg: &'a GetAuthzConfigResp) -> impl Fn(u32) -> Option<VerifyingKey> + 'a {
        move |kid| {
            cfg.public_keys
                .iter()
                .find(|k| k.kid == kid && !k.disabled)
                .and_then(|k| {
                    let arr: [u8; 32] = k.ed25519_pub.as_slice().try_into().ok()?;
                    VerifyingKey::from_bytes(&arr).ok()
                })
        }
    }

    #[test]
    fn kdc_mint_verify_expiry_byteflip() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let mgr = AutumnManager::new(); // memory mode → leader = true
            mgr.set_authz_keyring(
                crate::authz::AuthzKeyring::from_file_contents(&keyfile()).unwrap(),
            );
            mgr.set_admin_token("admin-secret".to_string());
            mgr.set_protected_prefixes(vec![b"mem/".to_vec()]);
            mgr.set_token_ttl_secs(3600);
            let cluster_id = mgr.cluster_id.borrow().clone();

            // ── (1) tenant-create (admin) ──────────────────────────────
            let ok_create = rkyv_encode(&TenantCreateReq {
                admin_token: "admin-secret".to_string(),
                tenant: "acme".to_string(),
                allowed_prefixes: vec![b"mem/acme/".to_vec()],
            });
            let resp: TenantCreateResp =
                rkyv_decode(&mgr.handle_tenant_create(ok_create).await.unwrap()).unwrap();
            assert_eq!(resp.code, CODE_OK, "{}", resp.message);
            let cred = resp.credential;
            assert_eq!(cred.len(), 32);

            // wrong admin token → refused
            let bad_admin = rkyv_encode(&TenantCreateReq {
                admin_token: "wrong".to_string(),
                tenant: "acme2".to_string(),
                allowed_prefixes: vec![b"mem/acme2/".to_vec()],
            });
            let r: TenantCreateResp =
                rkyv_decode(&mgr.handle_tenant_create(bad_admin).await.unwrap()).unwrap();
            assert_ne!(r.code, CODE_OK);

            // ── (2) mint a token with the credential ───────────────────
            let mint = rkyv_encode(&MintTokenReq {
                principal: "acme".to_string(),
                credential: cred.clone(),
            });
            let mresp: MintTokenResp =
                rkyv_decode(&mgr.handle_mint_token(mint).await.unwrap()).unwrap();
            assert_eq!(mresp.code, CODE_OK, "{}", mresp.message);
            assert!(!mresp.token.is_empty());
            let exp = mresp.exp;

            // wrong credential → refused (same opaque error as unknown tenant)
            let mint_bad = rkyv_encode(&MintTokenReq {
                principal: "acme".to_string(),
                credential: vec![9u8; 32],
            });
            let mr: MintTokenResp =
                rkyv_decode(&mgr.handle_mint_token(mint_bad).await.unwrap()).unwrap();
            assert_ne!(mr.code, CODE_OK);
            let mint_unknown = rkyv_encode(&MintTokenReq {
                principal: "ghost".to_string(),
                credential: cred.clone(),
            });
            let mru: MintTokenResp =
                rkyv_decode(&mgr.handle_mint_token(mint_unknown).await.unwrap()).unwrap();
            assert_ne!(mru.code, CODE_OK);

            // ── (3) publish config → verify token (what a PS does) ─────
            let cfg: GetAuthzConfigResp =
                rkyv_decode(&mgr.handle_get_authz_config().await.unwrap()).unwrap();
            assert!(cfg.enabled);
            assert_eq!(cfg.public_keys.len(), 1);
            assert_eq!(cfg.protected_prefixes, vec![b"mem/".to_vec()]);
            assert_eq!(
                cfg.public_keys[0].ed25519_pub,
                public_key_from_seed(&seed_1()).to_vec()
            );

            let rk = resolver(&cfg);
            let claims = verify_token(&mresp.token, &rk, exp - 1, cfg.clock_skew_secs)
                .expect("token should verify");
            assert_eq!(claims.aud, cluster_id, "aud must equal cluster_id");
            assert_eq!(claims.allowed_prefixes, vec![b"mem/acme/".to_vec()]);
            assert_eq!(claims.kid, 1);
            assert_eq!(claims.exp, exp);

            // ── (4) expiry-fail: now past exp + skew leeway ────────────
            let err = verify_token(
                &mresp.token,
                &rk,
                exp + cfg.clock_skew_secs + 1,
                cfg.clock_skew_secs,
            )
            .unwrap_err();
            assert_eq!(err, AuthReject::Expired);

            // ── (5) byte-flip-fail: tamper the signature ───────────────
            let mut tampered = mresp.token.clone();
            let last = tampered.len() - 1;
            tampered[last] ^= 0x01;
            let err = verify_token(&tampered, &rk, exp - 1, cfg.clock_skew_secs).unwrap_err();
            assert_eq!(err, AuthReject::BadSignature);

            // ── (6) tenant-delete stops future renewal ─────────────────
            let del = rkyv_encode(&TenantDeleteReq {
                admin_token: "admin-secret".to_string(),
                tenant: "acme".to_string(),
            });
            let dresp: CodeResp =
                rkyv_decode(&mgr.handle_tenant_delete(del).await.unwrap()).unwrap();
            assert_eq!(dresp.code, CODE_OK);
            let mint_after_del = rkyv_encode(&MintTokenReq {
                principal: "acme".to_string(),
                credential: cred,
            });
            let mr2: MintTokenResp =
                rkyv_decode(&mgr.handle_mint_token(mint_after_del).await.unwrap()).unwrap();
            assert_ne!(mr2.code, CODE_OK, "deleted tenant must not renew");
        });
    }

    #[test]
    fn kdc_disabled_when_no_signing_key() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let mgr = AutumnManager::new();
            // No keyring set → authz disabled.
            let cfg: GetAuthzConfigResp =
                rkyv_decode(&mgr.handle_get_authz_config().await.unwrap()).unwrap();
            assert!(!cfg.enabled);
            assert!(cfg.public_keys.is_empty());
            // mint refused (no signing key), tenant-create refused (no admin token).
            let mint = rkyv_encode(&MintTokenReq {
                principal: "acme".to_string(),
                credential: vec![1u8; 32],
            });
            let mr: MintTokenResp =
                rkyv_decode(&mgr.handle_mint_token(mint).await.unwrap()).unwrap();
            assert_ne!(mr.code, CODE_OK);
            let tc = rkyv_encode(&TenantCreateReq {
                admin_token: "x".to_string(),
                tenant: "acme".to_string(),
                allowed_prefixes: vec![b"mem/acme/".to_vec()],
            });
            let r: TenantCreateResp =
                rkyv_decode(&mgr.handle_tenant_create(tc).await.unwrap()).unwrap();
            assert_ne!(r.code, CODE_OK);
        });
    }
}

#[cfg(test)]
mod namespace_registry_tests {
    //! D2 (SD-1): namespace registry create/delete + bootstrap seed +
    //! the GetAuthzConfig bridge. Memory-mode manager (leader==true, no etcd);
    //! the etcd replay/persist path is covered by
    //! `tests/namespace_registry_etcd.rs` (needs the etcd binary).
    #![allow(clippy::await_holding_refcell_ref)]
    use crate::{AutumnManager, MgrNamespace};
    use autumn_rpc::manager_rpc::*;
    use autumn_rpc::StatusCode;
    use bytes::Bytes;

    const ADMIN: &str = "admin-secret";

    fn run<F: std::future::Future<Output = T>, T>(f: F) -> T {
        compio::runtime::Runtime::new().unwrap().block_on(f)
    }

    fn mgr() -> AutumnManager {
        let m = AutumnManager::new();
        m.set_admin_token(ADMIN.to_string());
        m
    }

    fn create(
        m: &AutumnManager,
        admin: &str,
        name: &str,
        owner: Option<&str>,
        presplit: Vec<Vec<u8>>,
    ) -> NamespaceCreateResp {
        let req = NamespaceCreateReq {
            admin_token: admin.to_string(),
            name: name.to_string(),
            owner_tenant: owner.map(|s| s.to_string()),
            presplit,
        };
        let payload: Bytes = rkyv_encode(&req);
        let resp = run(async { m.handle_namespace_create(payload).await.unwrap() });
        rkyv_decode::<NamespaceCreateResp>(&resp).expect("decode NamespaceCreateResp")
    }

    fn delete(m: &AutumnManager, admin: &str, name: &str) -> CodeResp {
        let req = NamespaceDeleteReq {
            admin_token: admin.to_string(),
            name: name.to_string(),
        };
        let payload: Bytes = rkyv_encode(&req);
        let resp = run(async { m.handle_namespace_delete(payload).await.unwrap() });
        rkyv_decode::<CodeResp>(&resp).expect("decode CodeResp")
    }

    fn authz_config(m: &AutumnManager) -> GetAuthzConfigResp {
        let resp = run(async { m.handle_get_authz_config().await.unwrap() });
        rkyv_decode::<GetAuthzConfigResp>(&resp).expect("decode GetAuthzConfigResp")
    }

    fn list(m: &AutumnManager) -> NamespaceListResp {
        let resp = run(async { m.handle_namespace_list().await.unwrap() });
        rkyv_decode::<NamespaceListResp>(&resp).expect("decode NamespaceListResp")
    }

    // ── payload-prefix admin gate on cluster-mutating ops ──

    /// Drive a mutating op through `dispatch` (where the gate lives). Returns the
    /// frame-level result: `Ok` = passed the gate (the handler then ran and
    /// answered on its own merits), `Err(code,msg)` = the gate rejected it.
    fn dispatch_merge(m: &AutumnManager, wire_payload: Bytes) -> Result<Bytes, (StatusCode, String)> {
        run(async { m.dispatch(MSG_MERGE_PARTITIONS, wire_payload).await })
    }

    fn merge_body() -> Bytes {
        rkyv_encode(&MergePartitionsReq {
            survivor_part_id: 1,
            victim_part_id: 2,
            force: false,
        })
    }

    #[test]
    fn admin_gate_skipped_when_manager_has_no_token() {
        // Opt-in: a token-less manager runs mutating ops BARE (dev/test/bench/
        // chaos never set a token). The bare body passes the gate and reaches the
        // handler, which then fails for its OWN reason (no such partition) — the
        // point is it was NOT rejected by the gate.
        let m = AutumnManager::new(); // no set_admin_token
        let r = dispatch_merge(&m, merge_body());
        assert!(r.is_ok(), "gate must not reject when no admin token is configured");
    }

    #[test]
    fn admin_gate_rejects_missing_and_wrong_token_accepts_correct() {
        let m = AutumnManager::new();
        m.set_admin_token(ADMIN.to_string());

        // No prefix at all (a stale/rogue client that doesn't know about the gate).
        let bare = dispatch_merge(&m, merge_body());
        let (code, msg) = bare.expect_err("token-ON manager must reject an unprefixed admin op");
        assert_eq!(code, StatusCode::FailedPrecondition);
        assert!(msg.contains("admin token"), "{msg}");

        // Wrong token.
        let wrong = dispatch_merge(
            &m,
            autumn_rpc::manager_rpc::prefix_admin_token(b"not-the-secret", &merge_body()),
        );
        let (code, msg) = wrong.expect_err("wrong token must be rejected");
        assert_eq!(code, StatusCode::FailedPrecondition);
        assert_eq!(msg, "admin token invalid");

        // Correct token → passes the gate (handler then runs and fails on its own
        // merits — a missing partition — which is NOT a gate rejection).
        let ok = dispatch_merge(
            &m,
            autumn_rpc::manager_rpc::prefix_admin_token(ADMIN.as_bytes(), &merge_body()),
        );
        assert!(ok.is_ok(), "correct token must pass the gate");
    }

    #[test]
    fn admin_gate_leaves_read_only_ops_untouched() {
        // A read op is never prefixed and never stripped, even with a token set.
        let m = AutumnManager::new();
        m.set_admin_token(ADMIN.to_string());
        let r = run(async { m.dispatch(MSG_NODES_INFO, Bytes::new()).await });
        assert!(r.is_ok(), "read-only op must not be gated");
    }

    #[test]
    fn namespace_list_returns_rich_registry_sorted() {
        let m = mgr();
        run(async { m.seed_builtin_namespaces().await.unwrap() });
        assert_eq!(
            create(&m, ADMIN, "bench", Some("acme"), vec![vec![0x01u8, 0x02]]).code,
            CODE_OK
        );
        let r = list(&m);
        assert_eq!(r.code, CODE_OK);
        let names: Vec<&str> = r.namespaces.iter().map(|n| n.name.as_str()).collect();
        for want in ["fs", "kvc", "mem", "bench"] {
            assert!(names.contains(&want), "missing {want}");
        }
        // Sorted by name (deterministic CLI output).
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
        // Rich fields carried through.
        let bench = r.namespaces.iter().find(|n| n.name == "bench").unwrap();
        assert_eq!(bench.prefix, b"bench/".to_vec());
        assert_eq!(bench.owner_tenant.as_deref(), Some("acme"));
        assert_eq!(bench.presplit, vec![vec![0x01u8, 0x02]]);
        let fs = r.namespaces.iter().find(|n| n.name == "fs").unwrap();
        assert!(fs.owner_tenant.is_none(), "builtin fs is existence-only");
    }

    #[test]
    fn namespace_list_is_leader_gated() {
        let m = mgr();
        run(async { m.seed_builtin_namespaces().await.unwrap() });
        m.set_leader(false);
        let r = list(&m);
        assert_eq!(r.code, CODE_NOT_LEADER);
        assert!(r.namespaces.is_empty());
    }

    // ── principal-list ──────────────────────────────────────────────

    fn principal_create(m: &AutumnManager, name: &str, grants: &[&[u8]]) -> TenantCreateResp {
        let req = TenantCreateReq {
            admin_token: ADMIN.to_string(),
            tenant: name.to_string(),
            allowed_prefixes: grants.iter().map(|g| g.to_vec()).collect(),
        };
        let payload: Bytes = rkyv_encode(&req);
        let resp = run(async { m.handle_tenant_create(payload).await.unwrap() });
        rkyv_decode::<TenantCreateResp>(&resp).expect("decode TenantCreateResp")
    }

    fn principal_list(m: &AutumnManager) -> PrincipalListResp {
        let resp = run(async { m.handle_principal_list().await.unwrap() });
        rkyv_decode::<PrincipalListResp>(&resp).expect("decode PrincipalListResp")
    }

    #[test]
    fn principal_list_returns_names_and_grants_sorted() {
        let m = mgr();
        assert_eq!(principal_create(&m, "fs", &[b"fs/"]).code, CODE_OK);
        assert_eq!(
            principal_create(&m, "agent7", &[b"mem/agent7/", b"kvc/"]).code,
            CODE_OK
        );
        let r = principal_list(&m);
        assert_eq!(r.code, CODE_OK);
        let names: Vec<&str> = r.principals.iter().map(|p| p.name.as_str()).collect();
        // sorted by name — the backing map is a HashMap, so without the sort
        // the CLI output would shuffle between invocations
        assert_eq!(names, vec!["agent7", "fs"]);
        let a = r.principals.iter().find(|p| p.name == "agent7").unwrap();
        assert_eq!(a.grants, vec![b"mem/agent7/".to_vec(), b"kvc/".to_vec()]);
        let f = r.principals.iter().find(|p| p.name == "fs").unwrap();
        assert_eq!(f.grants, vec![b"fs/".to_vec()]);
    }

    #[test]
    fn principal_list_never_leaks_credential_material() {
        // The row type has no credential_hash field at all, so this is a
        // structural guarantee — assert the account DOES hold a hash while the
        // listing carries only (name, grants), i.e. the drop is deliberate.
        let m = mgr();
        let created = principal_create(&m, "fs", &[b"fs/"]);
        assert_eq!(created.code, CODE_OK);
        assert!(!created.credential.is_empty(), "create returns the credential once");
        assert_ne!(
            m.tenant_accounts.borrow().get("fs").unwrap().credential_hash,
            [0u8; 32],
            "the account stores a real credential hash"
        );
        let r = principal_list(&m);
        let row = &r.principals[0];
        // Everything the row can possibly serialise:
        assert_eq!(row.name, "fs");
        assert_eq!(row.grants, vec![b"fs/".to_vec()]);
    }

    #[test]
    fn principal_list_is_leader_gated() {
        let m = mgr();
        assert_eq!(principal_create(&m, "fs", &[b"fs/"]).code, CODE_OK);
        m.set_leader(false);
        let r = principal_list(&m);
        assert_eq!(r.code, CODE_NOT_LEADER);
        assert!(r.principals.is_empty());
    }

    // ── sacred presplit boundaries ────────────

    fn set_presplit(m: &AutumnManager, name: &str, points: &[&[u8]]) -> CodeResp {
        let req = NamespaceSetPresplitReq {
            admin_token: ADMIN.to_string(),
            name: name.to_string(),
            points: points.iter().map(|p| p.to_vec()).collect(),
        };
        let payload: Bytes = rkyv_encode(&req);
        let resp = run(async { m.handle_namespace_set_presplit(payload).await.unwrap() });
        rkyv_decode::<CodeResp>(&resp).expect("decode CodeResp")
    }

    #[test]
    fn set_presplit_unions_and_marks_boundaries_sacred() {
        let m = mgr();
        run(async { m.seed_builtin_namespaces().await.unwrap() });
        // fs lane boundaries: `fs/` ++ [0x03][lane]
        assert_eq!(set_presplit(&m, "fs", &[b"fs/\x03\x01", b"fs/\x03\x02"]).code, CODE_OK);
        assert_eq!(
            m.sacred_boundary_owner(b"fs/\x03\x01").as_deref(),
            Some("fs")
        );
        assert!(m.sacred_boundary_owner(b"fs/\x03\x09").is_none());

        // Re-running presplit at a WIDER lane count unions rather than replaces:
        // dropping a protected boundary must be a deliberate separate act, not a
        // side effect of re-running presplit.
        assert_eq!(set_presplit(&m, "fs", &[b"fs/\x03\x02", b"fs/\x03\x03"]).code, CODE_OK);
        let row = m.namespaces.borrow().get("fs").unwrap().clone();
        assert_eq!(
            row.presplit,
            vec![
                b"fs/\x03\x01".to_vec(),
                b"fs/\x03\x02".to_vec(),
                b"fs/\x03\x03".to_vec()
            ],
            "points must union AND stay sorted"
        );
        assert_eq!(m.sacred_boundaries().len(), 3);
    }

    #[test]
    fn set_presplit_is_admin_and_leader_gated_and_needs_a_real_namespace() {
        let m = mgr();
        run(async { m.seed_builtin_namespaces().await.unwrap() });
        // wrong token
        let bad = NamespaceSetPresplitReq {
            admin_token: "wrong".to_string(),
            name: "fs".to_string(),
            points: vec![b"fs/\x03\x01".to_vec()],
        };
        let r = run(async {
            m.handle_namespace_set_presplit(rkyv_encode(&bad)).await.unwrap()
        });
        assert_eq!(rkyv_decode::<CodeResp>(&r).unwrap().code, CODE_PRECONDITION);
        assert!(m.sacred_boundaries().is_empty(), "a rejected call must record nothing");

        // unknown namespace
        assert_eq!(set_presplit(&m, "nope", &[b"nope/\x01"]).code, CODE_NOT_FOUND);

        // follower
        m.set_leader(false);
        assert_eq!(set_presplit(&m, "fs", &[b"fs/\x03\x01"]).code, CODE_NOT_LEADER);
    }

    #[test]
    fn set_presplit_is_opt_in_bare_on_a_tokenless_manager() {
        // UX-fix (M2): recording is OPT-IN like is_admin_mgr_msg — a
        // manager with NO admin token accepts a bare (empty-token) call and
        // records, so a token-less cluster (dev/bench/chaos) can ARM the merge
        // guard + auto-split snap. Fail-closing it (the old behaviour) made the
        // whole protection impossible to enable there.
        let m = AutumnManager::new(); // deliberately NO set_admin_token
        run(async { m.seed_builtin_namespaces().await.unwrap() });
        let bare = NamespaceSetPresplitReq {
            admin_token: String::new(),
            name: "fs".to_string(),
            points: vec![b"fs/\x03\x06".to_vec(), b"fs/\x03\x0c".to_vec()],
        };
        let r = run(async {
            m.handle_namespace_set_presplit(rkyv_encode(&bare)).await.unwrap()
        });
        assert_eq!(rkyv_decode::<CodeResp>(&r).unwrap().code, CODE_OK);
        assert_eq!(m.sacred_boundaries().len(), 2, "bare call must record on a tokenless manager");
        // And the guard is now live on this tokenless cluster.
        assert!(m.sacred_boundary_owner(b"fs/\x03\x06").is_some());
    }

    /// Put a partition with an explicit range into the store so the split-snap
    /// helper has something to look at.
    fn mk_range(m: &AutumnManager, id: u64, start: &[u8], end: &[u8]) {
        let mut s = m.store.inner.borrow_mut();
        s.partitions.insert(
            id,
            autumn_rpc::manager_rpc::MgrPartitionMeta {
                part_id: id,
                log_stream: 0,
                row_stream: 0,
                meta_stream: 0,
                rg: Some(autumn_rpc::manager_rpc::MgrRange {
                    start_key: start.to_vec(),
                    end_key: end.to_vec(),
                }),
            },
        );
    }

    #[test]
    fn auto_split_snaps_to_the_middle_declared_boundary() {
        let m = mgr();
        run(async { m.seed_builtin_namespaces().await.unwrap() });
        // 24 lanes over 1 partition: declare the boundaries for 4 parts.
        assert_eq!(
            set_presplit(
                &m,
                "fs",
                &[b"fs/\x03\x06", b"fs/\x03\x0c", b"fs/\x03\x12"]
            )
            .code,
            CODE_OK
        );
        mk_range(&m, 1, b"fs/", b"fs0");
        // Nearest the middle → halves the lane span, rather than shaving one
        // lane off an end the way "first declared point" would.
        assert_eq!(
            m.declared_split_point_within(1).as_deref(),
            Some(&b"fs/\x03\x0c"[..])
        );

        // After that cut, each half snaps to its own remaining boundary.
        mk_range(&m, 2, b"fs/", b"fs/\x03\x0c");
        mk_range(&m, 3, b"fs/\x03\x0c", b"fs0");
        assert_eq!(
            m.declared_split_point_within(2).as_deref(),
            Some(&b"fs/\x03\x06"[..])
        );
        assert_eq!(
            m.declared_split_point_within(3).as_deref(),
            Some(&b"fs/\x03\x12"[..])
        );
    }

    #[test]
    fn auto_split_falls_back_to_median_when_no_boundary_is_left() {
        let m = mgr();
        run(async { m.seed_builtin_namespaces().await.unwrap() });
        assert_eq!(set_presplit(&m, "fs", &[b"fs/\x03\x06"]).code, CODE_OK);
        // A partition already cut down to a single lane run holds no declared
        // point → None → the PS picks a median, which is the RIGHT choice there
        // (an intra-lane inode split is exactly what's wanted at that stage).
        mk_range(&m, 7, b"fs/\x03\x06", b"fs/\x03\x07");
        assert!(m.declared_split_point_within(7).is_none());

        // A point EQUAL to start_key is this partition's own boundary, not an
        // interior cut — splitting there would ask for an empty child.
        mk_range(&m, 8, b"fs/\x03\x06", b"fs0");
        assert!(
            m.declared_split_point_within(8).is_none(),
            "start_key must not be offered as an interior split point"
        );

        // A partition in another namespace is unaffected by fs's declarations.
        mk_range(&m, 9, b"kvc/", b"kvc0");
        assert!(m.declared_split_point_within(9).is_none());
    }

    #[test]
    fn declared_but_uncut_points_never_false_positive_the_merge_guard() {
        // Why recording INTENT (not just applied cuts) is safe: the merge guard
        // compares against `max(start_a, start_b)`, always a REAL partition
        // start, and an uncut declared point is nobody's start.
        let m = mgr();
        run(async { m.seed_builtin_namespaces().await.unwrap() });
        assert_eq!(
            set_presplit(&m, "fs", &[b"fs/\x03\x06", b"fs/\x03\x0c"]).code,
            CODE_OK
        );
        // Only \x06 was actually cut; \x0c is declared-but-uncut.
        mk_range(&m, 1, b"fs/", b"fs/\x03\x06");
        mk_range(&m, 2, b"fs/\x03\x06", b"fs0");
        // The real boundary IS protected …
        assert!(m.sacred_boundary_owner(b"fs/\x03\x06").is_some());
        // … and the uncut one, while recorded, is not any partition's start, so
        // it can never be the boundary a merge computes.
        let starts: Vec<Vec<u8>> = {
            let s = m.store.inner.borrow();
            s.partitions
                .values()
                .filter_map(|p| p.rg.as_ref().map(|r| r.start_key.clone()))
                .collect()
        };
        assert!(!starts.contains(&b"fs/\x03\x0c".to_vec()));
    }

    #[test]
    fn empty_key_is_never_a_sacred_boundary() {
        // The keyspace start is not a declared cut — treating it as one would
        // make the very first partition unmergeable forever.
        let m = mgr();
        run(async { m.seed_builtin_namespaces().await.unwrap() });
        assert_eq!(set_presplit(&m, "fs", &[b""]).code, CODE_OK);
        assert!(m.sacred_boundary_owner(b"").is_none());
    }

    #[test]
    fn principal_list_is_empty_before_any_create() {
        let r = principal_list(&mgr());
        assert_eq!(r.code, CODE_OK);
        assert!(r.principals.is_empty());
    }

    #[test]
    fn bootstrap_seeds_the_three_builtin_families() {
        let m = mgr();
        run(async { m.seed_builtin_namespaces().await.unwrap() });
        let ns = m.namespaces.borrow();
        for name in ["fs", "kvc", "mem"] {
            let row = ns.get(name).unwrap_or_else(|| panic!("{name} not seeded"));
            assert_eq!(row.prefix, format!("{name}/").into_bytes());
            assert!(row.owner_tenant.is_none(), "{name} should be existence-only");
        }
        // Idempotent: a second seed leaves them untouched (memory mode).
        run(async { m.seed_builtin_namespaces().await.unwrap() });
        assert_eq!(m.namespaces.borrow().len(), 3);
    }

    #[test]
    fn create_delete_round_trip() {
        let m = mgr();
        let r = create(&m, ADMIN, "bench", None, Vec::new());
        assert_eq!(r.code, CODE_OK, "{}", r.message);
        assert!(m.namespaces.borrow().contains_key("bench"));

        // Re-create is a precondition failure (already exists).
        let dup = create(&m, ADMIN, "bench", None, Vec::new());
        assert_eq!(dup.code, CODE_PRECONDITION);

        let d = delete(&m, ADMIN, "bench");
        assert_eq!(d.code, CODE_OK, "{}", d.message);
        assert!(!m.namespaces.borrow().contains_key("bench"));

        // Delete of a now-absent namespace = NOT_FOUND.
        let gone = delete(&m, ADMIN, "bench");
        assert_eq!(gone.code, CODE_NOT_FOUND);
    }

    #[test]
    fn presplit_points_are_stored_verbatim() {
        let m = mgr();
        let pts = vec![vec![0x01u8, 0x02], vec![0xffu8]];
        let r = create(&m, ADMIN, "bench", Some("acme"), pts.clone());
        assert_eq!(r.code, CODE_OK, "{}", r.message);
        let ns = m.namespaces.borrow();
        let row = ns.get("bench").unwrap();
        assert_eq!(row.presplit, pts);
        assert_eq!(row.owner_tenant.as_deref(), Some("acme"));
    }

    #[test]
    fn reserved_names_are_rejected() {
        let m = mgr();
        for name in ["fs", "kvc", "mem", "default"] {
            let r = create(&m, ADMIN, name, None, Vec::new());
            assert_eq!(r.code, CODE_INVALID_ARGUMENT, "{name} must be reserved");
        }
    }

    #[test]
    fn invalid_charset_is_rejected() {
        let m = mgr();
        for bad in ["Bench", "a/b", "has space", "", "up_UP"] {
            let r = create(&m, ADMIN, bad, None, Vec::new());
            assert_eq!(r.code, CODE_INVALID_ARGUMENT, "'{bad}' must be rejected");
        }
    }

    #[test]
    fn prefix_overlap_is_rejected() {
        let m = mgr();
        // Seed a namespace with a NESTED prefix (a/b/) directly into the shadow;
        // a new `a/` would then be a `starts_with` ancestor of it → conflict.
        m.namespaces.borrow_mut().insert(
            "deep".to_string(),
            MgrNamespace {
                name: "deep".to_string(),
                prefix: b"a/b/".to_vec(),
                owner_tenant: None,
                presplit: Vec::new(),
                created_at: 0,
            },
        );
        let r = create(&m, ADMIN, "a", None, Vec::new());
        assert_eq!(r.code, CODE_INVALID_ARGUMENT, "overlapping prefix must reject");
        // A disjoint name is still accepted.
        let ok = create(&m, ADMIN, "bench", None, Vec::new());
        assert_eq!(ok.code, CODE_OK, "{}", ok.message);
    }

    #[test]
    fn builtin_families_cannot_be_deleted() {
        let m = mgr();
        run(async { m.seed_builtin_namespaces().await.unwrap() });
        for name in ["fs", "kvc", "mem"] {
            let d = delete(&m, ADMIN, name);
            assert_eq!(d.code, CODE_INVALID_ARGUMENT, "{name} must be non-deletable");
            assert!(m.namespaces.borrow().contains_key(name));
        }
    }

    #[test]
    fn admin_token_is_enforced() {
        let m = mgr();
        // Wrong token.
        let bad = create(&m, "wrong", "bench", None, Vec::new());
        assert_eq!(bad.code, CODE_PRECONDITION);
        assert!(!m.namespaces.borrow().contains_key("bench"));
        // No admin token configured at all → RPCs disabled.
        let m2 = AutumnManager::new();
        let disabled = create(&m2, ADMIN, "bench", None, Vec::new());
        assert_eq!(disabled.code, CODE_ERROR);
    }

    #[test]
    fn get_authz_config_bridges_registry() {
        let m = mgr();
        run(async { m.seed_builtin_namespaces().await.unwrap() });
        // An OWNED namespace is auto-protected; an unowned one is registered only.
        assert_eq!(create(&m, ADMIN, "bench", Some("acme"), Vec::new()).code, CODE_OK);
        assert_eq!(create(&m, ADMIN, "scratch", None, Vec::new()).code, CODE_OK);

        let cfg = authz_config(&m);
        // `namespaces` carries EVERY registered prefix (Layer-A data source).
        for p in [b"fs/".to_vec(), b"kvc/".to_vec(), b"mem/".to_vec(), b"bench/".to_vec(), b"scratch/".to_vec()] {
            assert!(cfg.namespaces.contains(&p), "namespaces missing {p:?}");
        }
        // `protected_prefixes` carries ONLY the owned namespace (bench), not the
        // existence-only families or the unowned `scratch`.
        assert!(cfg.protected_prefixes.contains(&b"bench/".to_vec()));
        assert!(!cfg.protected_prefixes.contains(&b"fs/".to_vec()));
        assert!(!cfg.protected_prefixes.contains(&b"scratch/".to_vec()));
    }

    #[test]
    fn get_authz_config_is_leader_gated() {
        // D2 (coco P1): the registry is leader-maintained, so a
        // follower must refuse rather than publish an empty/stale namespace list.
        let m = mgr();
        run(async { m.seed_builtin_namespaces().await.unwrap() });
        let ok = authz_config(&m);
        assert_eq!(ok.code, CODE_OK);
        assert!(!ok.namespaces.is_empty(), "leader must carry the registry");

        m.set_leader(false);
        let follower = authz_config(&m);
        assert_eq!(follower.code, CODE_NOT_LEADER, "follower must refuse");
        // A refused response carries NO registry data — a PS never installs an
        // empty namespace list or drops protected prefixes from a follower.
        assert!(follower.namespaces.is_empty());
        assert!(follower.protected_prefixes.is_empty());
        assert!(follower.public_keys.is_empty());
    }

    #[test]
    fn manual_protected_prefix_list_is_preserved_as_union_member() {
        let m = mgr();
        // The D6 manual `--auth-protected-prefix` list must survive the bridge.
        m.set_protected_prefixes(vec![b"legacy/".to_vec()]);
        assert_eq!(create(&m, ADMIN, "bench", Some("acme"), Vec::new()).code, CODE_OK);
        let cfg = authz_config(&m);
        assert!(cfg.protected_prefixes.contains(&b"legacy/".to_vec()), "manual list dropped");
        assert!(cfg.protected_prefixes.contains(&b"bench/".to_vec()), "owned ns not bridged");
    }
}
// end of rpc_handlers.rs
