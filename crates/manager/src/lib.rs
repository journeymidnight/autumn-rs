pub mod audit;
pub mod authz;
pub mod ec_abandon;
mod extent_delete;
pub mod extent_inflight;
mod extent_corrupt;
mod op_log;
mod extent_layout;
mod fs_alloc;
pub mod inode_lease;
pub mod node_state;
pub mod policy;
#[cfg(test)]
mod policy_tests;
mod recovery;
pub mod recovery_rate_limiter;
mod rpc_handlers;
/// Test-only merge-freeze failpoint (always 0 in production); see its doc in
/// `rpc_handlers`. Re-exported so integration tests can arm it.
#[doc(hidden)]
pub use rpc_handlers::MERGE_TEST_PAUSE_MS;

// Pure `/api/overview` composer, shared with `autumn-op overview` so the
// standalone dashboard app (examples/dashboard) can render the same view the
// manager used to serve. The manager itself no longer serves a web UI — only
// the leader-fenced auto-policy controller below survives in-process.
pub mod dashboard_compose;
// ported pure decision helpers (M1) → leader-fenced controller (M2).
mod auto_policy;
// The async op-ledger: every long-running op (split/merge/rebalance/compact/gc/
// forcegc/ec-convert) is submitted through the leader, actuated in the
// background, and made queryable — recovering the failure reason the
// fire-and-forget maintenance ops used to drop.
mod op_ledger;

/// How a submitted op's actuation should transition its ledger entry.
enum ActuationResult {
    /// Closes the entry now — split / merge / rebalance complete synchronously
    /// (their RPC returns the true outcome), and any kind that fails to dispatch.
    Terminal {
        state: u8,
        error: String,
        message: String,
    },
    /// Leaves the entry RUNNING — compact/gc/forcegc were enqueued on the PS
    /// (the terminal outcome arrives on the load heartbeat), and ec-convert's
    /// marker was acquired (closed by `apply_ec_conversion_done`). Carries an
    /// advisory message (e.g. the forcegc replay-floor preview) to surface now.
    Dispatched { message: String },
}

/// `OP_KIND_*` → the `AUDIT_OP_*` code for its durable terminal record.
fn op_kind_audit_code(kind: u8) -> u8 {
    match kind {
        OP_KIND_SPLIT => AUDIT_OP_SPLIT,
        OP_KIND_MERGE => AUDIT_OP_MERGE,
        OP_KIND_REBALANCE => AUDIT_OP_REBALANCE,
        OP_KIND_COMPACT => AUDIT_OP_COMPACT,
        OP_KIND_GC => AUDIT_OP_GC,
        OP_KIND_FORCE_GC => AUDIT_OP_FORCE_GC,
        OP_KIND_EC_CONVERT => AUDIT_OP_FORCE_EC_CONVERT,
        _ => 0,
    }
}

pub(crate) use extent_delete::PendingDelete;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::rc::Rc;
use std::str;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use autumn_common::{AppError, MetadataStore};
use autumn_rpc::manager_rpc::*;
use autumn_rpc::{Frame, FrameDecoder, StatusCode};
use bytes::Bytes;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::BufResult;

// ── EtcdMirror ─────────────────────────────────────────────────────────────

/// Etcd path for the manager leader-key. Also used as the fence target
/// for every manager etcd write txn.
pub(crate) const LEADER_KEY: &str = "autumn-rs/stream-manager/leader";

/// etcd prefix for persistent operator overrides
/// (`node_override/<node_id>` → rkyv'd `MgrNodeOverride`).
pub const NODE_OVERRIDE_PREFIX: &str = "node_override/";

/// etcd prefix for hard-removed node tombstones
/// (`decommissioned/<node_id>` → rkyv'd `MgrNodeOverride`). Same value
/// shape as the override prefix — the existence of the key is what
/// blocks re-registration.
pub const DECOMMISSIONED_PREFIX: &str = "decommissioned/";

/// cluster identity key. Written exactly once: the first leader
/// to win the election CAS-creates this key (create_revision==0) with a
/// fresh UUID. Subsequent leaders inherit via `replay_from_etcd`. Read
/// by `MSG_GET_CLUSTER_ID` so `autumn-op format` can stamp each
/// formatted disk and `autumn-extent-node` can verify on startup.
pub const CLUSTER_ID_KEY: &str = "autumn-rs/cluster_id";
/// R1 rolling upgrade: persisted cluster_version. Value is ASCII decimal
/// (e.g. b"3") — deliberately NOT rkyv, so it stays readable across every
/// future serialization era (it gates exactly those transitions).
pub const CLUSTER_VERSION_KEY: &str = "autumn-rs/cluster_version";

/// Writer-lease etcd prefix. One key per inode that
/// currently has a writer (reader leases are NOT persisted — they're
/// ephemeral, and a manager failover invalidates every reader's cache
/// per plan §6.4).
pub const INODE_LEASES_PREFIX: &str = "inode_leases/";

/// etcd prefix for the KDC tenant account DB
/// (`tenantAccount/<tenant>` → rkyv'd `MgrTenantAccount`). Replayed on leader
/// failover; the credential HASH is stored, never the raw credential. The
/// tenant name is a string suffix (percent-encoded segment), not a u64 id.
pub const TENANT_ACCOUNT_PREFIX: &str = "tenantAccount/";

/// D2: etcd prefix for the namespace registry
/// (`namespace/<name>` → rkyv'd `MgrNamespace`). Replayed on leader failover;
/// mutated only via the admin namespace-create/delete RPCs. The three built-in
/// families (`fs`/`kvc`/`mem`) are CAS-preregistered on first leader promotion
/// (`seed_builtin_namespaces`). See docs/key_namespace_split_design.md.
pub const NAMESPACE_PREFIX: &str = "namespace/";

/// D2: the built-in namespace families, CAS-preregistered by the first
/// leader. `owner_tenant = None` (existence-only until an owner is later
/// assigned), so they are registered but NOT protected out of the box.
pub const BUILTIN_NAMESPACES: [&str; 3] = ["fs", "kvc", "mem"];

/// D2: names that `namespace-create` refuses. `fs`/`kvc`/`mem` are the
/// bootstrap-seeded families (created + non-deletable); `default` is reserved
/// purely to prevent confusion (it is a conventional TENANT name, never a
/// namespace — see §3.7③).
pub const RESERVED_NAMESPACE_NAMES: [&str; 4] = ["fs", "kvc", "mem", "default"];

/// D2: validate a namespace name — a single path segment matching
/// `[a-z0-9._-]+` (same charset as the D1 tenant/volume components). Returns the
/// reason string on rejection. Pure (unit-tested).
pub(crate) fn validate_namespace_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("namespace name must be non-empty".to_string());
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(format!(
            "namespace name '{name}' has invalid chars (allowed: [a-z0-9._-])"
        ));
    }
    Ok(())
}

/// D2: prefix-disjointness. A new namespace prefix `new_prefix`
/// (`name + "/"`) may not be in a `starts_with` relation — in EITHER direction —
/// with any existing registered prefix, so all namespace intervals stay pairwise
/// non-overlapping (Layer-A / authz / presplit prefix matching is then
/// unambiguous). Returns `true` when `new_prefix` CONFLICTS. Pure (unit-tested).
pub(crate) fn namespace_prefix_conflicts(new_prefix: &[u8], existing: &[&[u8]]) -> bool {
    existing
        .iter()
        .any(|p| new_prefix.starts_with(p) || p.starts_with(new_prefix))
}

/// ENOSPC-1: default allocation free-space floor — a node whose best
/// disk has less free than this is soft-avoided by `select_nodes`.
/// 256 MiB comfortably covers a fresh extent + its metadata while small
/// enough not to strand mostly-full-but-usable disks.
pub const DEFAULT_MIN_ALLOC_FREE_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct EtcdMirror {
    client: Rc<autumn_etcd::EtcdClient>,
    /// identity used in the leader-fence compare. Set at connect time
    /// from `AutumnManager::instance_id`.
    instance_id: Rc<String>,
    /// shared with `AutumnManager.leader`. Flipped to `false` when the
    /// fence compare detects a deposition, so the in-process state agrees with
    /// the etcd ground truth before the next operation runs.
    leader: Rc<Cell<bool>>,
    /// etcd-chaos D1: shared with `AutumnManager.displaced`. Set `true`
    /// when the fence diagnosis observes a DIFFERENT instance holding the
    /// leader key (true displacement — our state can go stale); left
    /// untouched when the key is merely GONE (lease expiry / etcd blip —
    /// no one superseded us).
    displaced: Rc<Cell<bool>>,
}

impl EtcdMirror {
    async fn connect(
        endpoints: Vec<String>,
        instance_id: Rc<String>,
        leader: Rc<Cell<bool>>,
        displaced: Rc<Cell<bool>>,
    ) -> Result<Self> {
        let client = autumn_etcd::EtcdClient::connect_many(&endpoints).await?;
        Ok(Self {
            client: Rc::new(client),
            instance_id,
            leader,
            displaced,
        })
    }

    /// run a fenced txn. Always prepends a
    /// `Cmp::value(LEADER_KEY) == instance_id` compare to `extra_cmp`.
    ///
    /// Returns:
    ///   - `Ok(true)` — the txn (fence + extra) succeeded; success ops applied.
    ///   - `Ok(false)` — the fence held but `extra_cmp` failed (e.g., a
    ///     create_revision==0 CAS rejected because the key already exists).
    ///     Caller-visible "soft-failure"; semantics identical to a vanilla
    ///     `succeeded=false` from etcd.
    ///   - `Err(AppError::NotLeader)` — the fence itself failed. The shared
    ///     `leader` Cell is flipped to `false` so subsequent in-process
    ///     `ensure_leader()` calls reject. Callers should bubble this up so
    ///     the client receives `CODE_NOT_LEADER` and retries against whoever
    ///     etcd currently lists as leader.
    async fn txn_fenced(
        &self,
        extra_cmp: Vec<autumn_etcd::proto::Compare>,
        success: Vec<autumn_etcd::proto::RequestOp>,
        failure: Vec<autumn_etcd::proto::RequestOp>,
    ) -> Result<bool, AppError> {
        let mut compare = Vec::with_capacity(1 + extra_cmp.len());
        compare.push(autumn_etcd::Cmp::value(
            LEADER_KEY.as_bytes(),
            self.instance_id.as_bytes(),
        ));
        compare.extend(extra_cmp);

        let txn = autumn_etcd::proto::TxnRequest {
            compare,
            success,
            failure,
        };

        let resp = {
            self.client
                .txn(txn)
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?
        };

        if resp.succeeded {
            return Ok(true);
        }
        self.diagnose_post_txn_fence().await?;
        Ok(false)
    }

    /// Shared post-txn fence diagnosis: a fenced txn came back
    /// `succeeded == false` — distinguish fence-failure from
    /// extra_cmp-failure by reading the current leader-key value. If it
    /// still matches our instance_id, the fence held and only a business
    /// CAS failed (e.g., create_revision==0 refused because the key already
    /// exists) — returns `Ok(())` and the caller decides how to surface the
    /// soft-fail. If it differs (or is gone), we have been deposed —
    /// flips `leader`/`displaced` and returns `Err(NotLeader)`.
    async fn diagnose_post_txn_fence(&self) -> Result<(), AppError> {
        let got = {
            self.client
                .get(LEADER_KEY.as_bytes())
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?
        };
        let still_leader = got
            .kvs
            .first()
            .map(|kv| kv.value.as_slice() == self.instance_id.as_bytes())
            .unwrap_or(false);
        if !still_leader {
            // etcd-chaos D1: a DIFFERENT holder = true displacement (our
            // state can be superseded); a missing key = lease expiry with
            // no successor — leaderless, not displaced.
            if !got.kvs.is_empty() {
                self.displaced.set(true);
            }
            self.leader.set(false);
            return Err(AppError::NotLeader);
        }
        Ok(())
    }

    /// A fenced txn with NO extra_cmp returned `succeeded == false` even
    /// though the fence held — only possible on a server-side anomaly.
    fn empty_extra_cmp_err() -> AppError {
        AppError::Internal("etcd txn rejected with empty extra_cmp".to_string())
    }

    /// fenced txn whose success value is the txn's COMMIT REVISION
    /// (`ResponseHeader.revision`). For a txn containing exactly one PUT
    /// on a key, that revision IS the PUT's `mod_revision` — read
    /// atomically from the same txn response, with no separate GET that
    /// a concurrent same-key writer could interleave (coco P1: a
    /// PUT-then-GET pair let two concurrent acquires both observe the
    /// later writer's mod_revision and share one fencing epoch).
    /// `succeeded == false` with no extra_cmp is a server-side anomaly
    /// and surfaces as an error — never a silently-reused epoch.
    async fn txn_fenced_put_revision(
        &self,
        success: Vec<autumn_etcd::proto::RequestOp>,
    ) -> Result<i64, AppError> {
        // No extra_cmp, so a held fence with `succeeded == false` is a
        // server-side anomaly rather than a business CAS refusal.
        self.txn_fenced_revision(vec![], success)
            .await?
            .ok_or_else(Self::empty_extra_cmp_err)
    }

    /// `txn_fenced` + the txn's COMMIT REVISION. `Ok(None)` is the soft CAS
    /// refusal (`extra_cmp` failed under a held fence), matching
    /// `txn_fenced`'s `Ok(false)`.
    ///
    /// The revision is what callers use as an identity for the thing the txn
    /// created — an owner epoch, or an in-flight marker's attempt nonce. It
    /// must come from THIS txn's response: a separate GET could observe a
    /// concurrent same-key writer's revision, handing two creations one
    /// identity, which is precisely what such an identity exists to prevent.
    async fn txn_fenced_revision(
        &self,
        extra_cmp: Vec<autumn_etcd::proto::Compare>,
        success: Vec<autumn_etcd::proto::RequestOp>,
    ) -> Result<Option<i64>, AppError> {
        let mut compare = Vec::with_capacity(1 + extra_cmp.len());
        compare.push(autumn_etcd::Cmp::value(
            LEADER_KEY.as_bytes(),
            self.instance_id.as_bytes(),
        ));
        compare.extend(extra_cmp);
        let txn = autumn_etcd::proto::TxnRequest {
            compare,
            success,
            failure: vec![],
        };
        let resp = {
            self.client
                .txn(txn)
                .await
                .map_err(|e| AppError::Internal(e.to_string()))?
        };
        if resp.succeeded {
            let rev = resp
                .header
                .as_ref()
                .map(|h| h.revision)
                .filter(|r| *r > 0)
                .ok_or_else(|| {
                    AppError::Internal("etcd txn response missing header revision".to_string())
                })?;
            return Ok(Some(rev));
        }
        self.diagnose_post_txn_fence().await?;
        Ok(None)
    }

    async fn put_msgs_txn(&self, kvs: Vec<(String, Vec<u8>)>) -> Result<(), AppError> {
        if kvs.is_empty() {
            return Ok(());
        }
        let ops = kvs
            .into_iter()
            .map(|(k, v)| autumn_etcd::Op::put(k.as_bytes(), &v))
            .collect::<Vec<_>>();
        match self.txn_fenced(vec![], ops, vec![]).await? {
            true => Ok(()),
            false => Err(Self::empty_extra_cmp_err()),
        }
    }

    async fn put_and_delete_txn(
        &self,
        puts: Vec<(String, Vec<u8>)>,
        deletes: Vec<String>,
    ) -> Result<(), AppError> {
        if puts.is_empty() && deletes.is_empty() {
            return Ok(());
        }
        let mut ops = Vec::with_capacity(puts.len() + deletes.len());
        ops.extend(
            puts.into_iter()
                .map(|(k, v)| autumn_etcd::Op::put(k.as_bytes(), &v)),
        );
        ops.extend(
            deletes
                .into_iter()
                .map(|k| autumn_etcd::Op::delete(k.as_bytes())),
        );
        match self.txn_fenced(vec![], ops, vec![]).await? {
            true => Ok(()),
            false => Err(Self::empty_extra_cmp_err()),
        }
    }

    /// Item 3: put+delete txn with an optional single-key value-CAS. `cas =
    /// Some((key, baseline))` prepends `Cmp::value(key) == baseline` to the
    /// fenced compare. The fenced txn returning `Ok(false)` means the
    /// value-compare failed — `key` changed concurrently since the caller
    /// captured `baseline` (a `punch_holes`/`alloc`/`truncate` committed on the
    /// same stream during our etcd RTT) — so we surface `Precondition`; the
    /// handler maps it to `CODE_PRECONDITION` and the client retries with a
    /// fresh snapshot. Unlike a serialization lock this never BLOCKS the write
    /// path: conflicting ops proceed concurrently and only a genuine conflict
    /// retries (the per-stream-lock attempt blocked alloc behind slow GC/split
    /// under kill and lost writes — see claude-progress.txt Item 3).
    /// BUG-LEASE-1 R2-P0 #1 (coco arch review round 2, 2026-06-06):
    /// "read-then-CAS-put" used by the heartbeat refresh path. The
    /// raw `put_msgs_txn` path the original BUG-LEASE-1 fix used
    /// was an unconditional blind write — between
    /// `LeaseRegistry::heartbeat()` (which sets in-memory deadline)
    /// and the etcd put, a concurrent `ReleaseLease` could have
    /// deleted the etcd record, or a force-revoke + new-writer
    /// acquire could have overwritten it with a different writer.
    /// The blind put would then resurrect / overwrite that change.
    ///
    /// This helper does the safe form: read the current etcd value
    /// for `key`, build a CAS-put that's gated on
    /// `Cmp::value(key) == baseline`. If the record was deleted
    /// or changed since our read, the txn returns `Ok(false)` →
    /// `Precondition` → caller treats as "skip, next heartbeat
    /// will retry". The leader fence is still threaded via the
    /// underlying `txn_fenced`.
    ///
    /// Returns: `Ok(true)` if the CAS-put committed, `Ok(false)`
    /// if a concurrent change beat us (caller should NOT treat
    /// this as an error — in-memory state is still authoritative).
    /// `Err` for genuine etcd / network / not-leader failures.
    async fn read_then_cas_put(
        &self,
        key: &str,
        new_value: Vec<u8>,
    ) -> Result<bool, AppError> {
        // Read the current record. If absent, the record was deleted
        // (release happened) — skip the put.
        let resp = self
            .client
            .get(key.as_bytes())
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        let baseline = match resp.kvs.first() {
            Some(kv) => kv.value.clone(),
            None => return Ok(false),
        };
        // CAS-put: only succeeds if etcd value still matches baseline.
        let extra_cmp = vec![autumn_etcd::Cmp::value(key.as_bytes(), baseline.as_slice())];
        let ops = vec![autumn_etcd::Op::put(key.as_bytes(), &new_value)];
        match self.txn_fenced(extra_cmp, ops, vec![]).await? {
            true => Ok(true),
            false => Ok(false),
        }
    }

    async fn put_delete_txn_cas(
        &self,
        puts: Vec<(String, Vec<u8>)>,
        deletes: Vec<String>,
        // Item 3: one `(key, baseline)` value-compare per existing key this txn
        // read-modify-writes. ALL must still match for the txn to apply (etcd
        // ANDs the compares); any mismatch = a concurrent change → Ok(false) →
        // Precondition → caller retries. Empty = no CAS (plain fenced put).
        cas: Vec<(String, Vec<u8>)>,
    ) -> Result<(), AppError> {
        if puts.is_empty() && deletes.is_empty() {
            return Ok(());
        }
        let extra_cmp: Vec<_> = cas
            .iter()
            .map(|(k, v)| autumn_etcd::Cmp::value(k.as_bytes(), v.as_slice()))
            .collect();
        let mut ops = Vec::with_capacity(puts.len() + deletes.len());
        ops.extend(
            puts.into_iter()
                .map(|(k, v)| autumn_etcd::Op::put(k.as_bytes(), &v)),
        );
        ops.extend(
            deletes
                .into_iter()
                .map(|k| autumn_etcd::Op::delete(k.as_bytes())),
        );
        match self.txn_fenced(extra_cmp, ops, vec![]).await? {
            true => Ok(()),
            false => Err(AppError::Precondition(
                autumn_common::alloc_conflict::cas_conflict_message(),
            )),
        }
    }
}

// ── ConnPool (single-threaded compio, Rc-based) ────────────────────────────

/// Minimal connection pool for manager → extent node calls.
/// Duplicates the pattern from stream::conn_pool to avoid manager→stream dep.
struct RpcConn {
    reader: autumn_transport::ReadHalf,
    writer: autumn_transport::WriteHalf,
    decoder: FrameDecoder,
    next_id: u32,
    read_buf: Vec<u8>,
}

impl RpcConn {
    async fn connect(addr: SocketAddr) -> Result<Self> {
        let conn = autumn_transport::current_or_init().connect(addr).await?;
        if let Some(s) = conn.as_tcp() {
            s.set_nodelay(true)?;
        }
        let (reader, writer) = conn.into_split();
        Ok(Self {
            reader,
            writer,
            decoder: FrameDecoder::new(),
            next_id: 1,
            read_buf: vec![0u8; 64 * 1024],
        })
    }

    async fn call(&mut self, msg_type: u8, payload: Bytes) -> Result<Bytes> {
        let req_id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);

        let frame = Frame::request(req_id, msg_type, payload);
        let data = frame.encode();
        let BufResult(result, _) = self.writer.write_all(data).await;
        result?;

        loop {
            match self
                .decoder
                .try_decode()
                .map_err(|e| anyhow::anyhow!("{e}"))?
            {
                Some(resp) if resp.req_id == req_id => {
                    if resp.is_error() {
                        let (code, message) = autumn_rpc::RpcError::decode_status(&resp.payload);
                        return Err(anyhow::anyhow!("rpc error ({:?}): {}", code, message));
                    }
                    return Ok(resp.payload);
                }
                Some(_) => continue,
                None => {}
            }

            let BufResult(result, buf_back) =
                self.reader.read(std::mem::take(&mut self.read_buf)).await;
            self.read_buf = buf_back;
            let n = result?;
            if n == 0 {
                return Err(anyhow::anyhow!("connection closed"));
            }
            self.decoder.feed(&self.read_buf[..n]);
        }
    }
}

pub(crate) struct ConnPool {
    conns: RefCell<HashMap<SocketAddr, Rc<RefCell<RpcConn>>>>,
}

impl ConnPool {
    fn new() -> Self {
        Self {
            conns: RefCell::new(HashMap::new()),
        }
    }

    #[allow(dead_code)]
    async fn call(&self, addr: &str, msg_type: u8, payload: Bytes) -> Result<Bytes> {
        let sock = parse_addr(addr)?;
        // Get or create the connection. We must drop the Rc<RefCell> borrow
        // before the async call to avoid holding RefMut across await.
        // Since we're single-threaded compio, there's no concurrent access.
        let conn = self.get_or_connect(sock).await?;
        // SAFETY: single-threaded compio runtime — no concurrent borrow possible.
        let conn_ptr = conn.as_ptr();
        let result = unsafe { &mut *conn_ptr }.call(msg_type, payload).await;
        if result.is_err() {
            // Evict broken connection so next call reconnects.
            self.conns.borrow_mut().remove(&sock);
        }
        result
    }

    /// bound an RPC at `timeout`. Same connection / eviction
    /// semantics as `call`; on the timeout branch we deliberately evict
    /// because the underlying connection is now mid-protocol (we sent a
    /// request but stopped reading) and reusing it could deadlock the
    /// next caller waiting on an unrelated response.
    async fn call_timeout(
        &self,
        addr: &str,
        msg_type: u8,
        payload: Bytes,
        timeout: std::time::Duration,
    ) -> Result<Bytes> {
        let sock = parse_addr(addr)?;
        let conn = self.get_or_connect(sock).await?;
        // SAFETY: single-threaded compio runtime — no concurrent borrow possible.
        let conn_ptr = conn.as_ptr();
        let result =
            compio::time::timeout(timeout, unsafe { &mut *conn_ptr }.call(msg_type, payload)).await;
        match result {
            Ok(Ok(bytes)) => Ok(bytes),
            Ok(Err(e)) => {
                self.conns.borrow_mut().remove(&sock);
                Err(e)
            }
            Err(_elapsed) => {
                // Mid-protocol: we sent a request but stopped reading.
                // Reusing this conn could starve the next caller.
                self.conns.borrow_mut().remove(&sock);
                Err(anyhow::anyhow!("rpc to {addr} timed out after {timeout:?}"))
            }
        }
    }

    async fn get_or_connect(&self, addr: SocketAddr) -> Result<Rc<RefCell<RpcConn>>> {
        if let Some(conn) = self.conns.borrow().get(&addr) {
            return Ok(conn.clone());
        }
        // (1A): bound the TCP connect. `call_timeout` wraps only the
        // request future, NOT this connect — a hung connect to a dead /
        // firewalled peer would wedge the calling background loop forever
        // despite call_timeout. Default 5 s, env AUTUMN_MGR_CONNECT_TIMEOUT_MS.
        let connect_to = connect_timeout();
        let raw = compio::time::timeout(connect_to, RpcConn::connect(addr))
            .await
            .map_err(|_| anyhow::anyhow!("connect to {addr} timed out after {connect_to:?}"))??;
        let conn = Rc::new(RefCell::new(raw));
        self.conns.borrow_mut().insert(addr, conn.clone());
        Ok(conn)
    }
}

/// (1A): TCP connect timeout for the manager's ConnPool. See
/// `get_or_connect`. Env `AUTUMN_MGR_CONNECT_TIMEOUT_MS` (default 5 s).
fn connect_timeout() -> std::time::Duration {
    let ms = std::env::var("AUTUMN_MGR_CONNECT_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(5_000)
        .max(500);
    std::time::Duration::from_millis(ms)
}

fn parse_addr(addr: &str) -> Result<SocketAddr> {
    let stripped = addr
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    stripped
        .parse::<SocketAddr>()
        .map_err(|e| anyhow::anyhow!("invalid address {:?}: {}", addr, e))
}

// ── cluster-df capacity snapshot (in-memory; serves MSG_CLUSTER_DF) ──────────

/// Per-node capacity rollup (sum over the node's online disks), refreshed
/// each `node_health_loop` tick from the EN's df report.
#[derive(Default, Clone)]
pub(crate) struct NodeCap {
    pub total: u64,
    pub free: u64,
    /// Σ this node's per-disk `DiskStatus.extent_bytes` — real autumn footprint.
    pub extent_bytes: u64,
    /// false = the node's df probe failed this tick (unknown != truly offline).
    pub online: bool,
}

/// Cluster capacity snapshot. RAW + physical_used refreshed every tick from
/// df; logical_stored from a periodic read-only scan. Display layer derives
/// the amplification factor and the EC-dependent writable RANGE.
#[derive(Default, Clone)]
pub(crate) struct ClusterCapSnapshot {
    pub raw_total: u64,
    pub raw_free: u64,
    /// Σ all nodes' extent_bytes (exact physical footprint, no formula).
    pub physical_used: u64,
    /// Online EN count (df-reachable this tick) — bounds best EC shape.
    pub node_count: u64,
    pub last_update_ms: u64,
    /// Read-only Σ distinct sealed_length (de-amplified, sealed-only).
    pub logical_stored: u64,
    /// Σ PS-reported open-tail committed bytes across
    /// partitions (one copy). Refreshed every tick from the policy load
    /// window (cheap sum). The amp denominator is `logical_stored + this`.
    pub logical_open_tail: u64,
    /// Σ reclaimable dead bytes across partitions — sealed
    /// (`PartitionLoad.gc_debt_bytes`) + open-tail (`open_tail_dead_bytes`).
    /// Refreshed every tick from the same policy load window; the dead fraction
    /// of the footprint that GC can eventually reclaim.
    pub logical_wal_debt: u64,
    pub logical_last_update_ms: u64,
    pub per_node: Vec<(u64, NodeCap)>,
}

// ── AutumnManager ──────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct AutumnManager {
    pub store: MetadataStore,
    leader: Rc<Cell<bool>>,
    etcd: Option<EtcdMirror>,
    /// Owned via `Rc` so `EtcdMirror` (cloned from this) can use the same
    /// identity in its leader-fence compare without shipping a string per
    /// txn.
    instance_id: Rc<String>,
    /// unified extent-level in-flight ledger. Authoritative
    /// source of truth for every stream-layer op currently in flight
    /// (ConvertToEc / Recovery / Delete). Replaces the four scattered
    /// inflight bookkeeping mechanisms that existed before the unified ledger
    /// (`ec_conversion_inflight` HashSet, `pending_ec_dispatch` HashMap,
    /// `recovery_tasks` HashMap, `pending_extent_deletes` VecDeque).
    /// Persisted at etcd prefix `extent_inflight/`. See
    /// `crates/manager/src/extent_inflight.rs` for the API + invariants
    /// and `~/.claude/plans/stream-merge-split-ps-sorted-dijkstra.md` for
    /// the migration plan.
    pub(crate) inflight: Rc<RefCell<HashMap<u64, crate::extent_inflight::MgrExtentInflightRecord>>>,
    /// Attempt identity for each live marker in `inflight`, keyed the same way:
    /// the etcd revision of the txn that CREATED that marker. Unique per
    /// attempt (a released-then-reissued marker is a different creation) and
    /// monotonic (etcd revisions only grow), which is what lets an EN refuse a
    /// stripe from an attempt older than the one it is staging.
    ///
    /// It rides beside the record rather than inside it because
    /// `MgrEcDispatchInflight` is nested in the PERSISTED
    /// `MgrExtentInflightRecord`: widening it changes that struct's archived
    /// layout, and since the payloads are `Option<T>` the size shift would make
    /// every live marker — recovery and delete included — fail to decode on
    /// replay, blocking leadership on upgrade. etcd already stores this value
    /// as the key's `mod_revision`, so replay rebuilds the map for free.
    ///
    /// A missing entry reads as `0` = "legacy attempt, no identity". Because
    /// the dispatch and the completion-apply both read THIS map, a divergent
    /// entry can only weaken the check to its pre-nonce behaviour — it can
    /// never reject a legitimate report.
    pub(crate) inflight_attempt_nonce: Rc<RefCell<HashMap<u64, u64>>>,
    /// Which payload file holds each extent's bytes, for the extents that are
    /// not in the default `InDat` shape. See `extent_layout.rs`; persisted at
    /// the `extentLayout/` prefix, absent ⇒ `InDat`.
    pub(crate) extent_payload_location: Rc<RefCell<HashMap<u64, u8>>>,
    /// Per-process sequence + amortised-rotation counter for the durable
    /// op-log (see `op_log`).
    pub(crate) op_log_seq: Cell<u64>,
    pub(crate) op_log_writes_since_gc: Cell<u32>,
    /// Slots proven to hold corrupt bytes, per extent (see `extent_corrupt`).
    /// Distinct from a clear `avali` bit, which cannot say WHY a slot is out.
    pub(crate) extent_corrupt_slots: Rc<RefCell<HashMap<u64, u32>>>,
    /// #6: per-partition split-in-flight guard (in-memory; single-threaded
    /// manager). `handle_multi_modify_split` inserts `part_id` before its
    /// (possibly slow) etcd txn and removes it on completion via a RAII guard.
    /// A concurrent split request for the SAME partition is refused with
    /// `CODE_PRECONDITION` — so a PS retry storm against a slow manager can no
    /// longer commit multiple separate splits (the reproduced 1→6 cascade).
    /// Not persisted: the cascade is within one manager's slow window; a cross-
    /// failover duplicate is a far narrower residual (the new leader hasn't
    /// processed the in-flight request).
    pub(crate) split_inflight: Rc<RefCell<std::collections::HashSet<u64>>>,
    /// in-memory live retry state for Delete ops. The ledger
    /// entry's `PersistedPendingDelete` payload is a snapshot of the
    /// original addrs (captured at enqueue time); the live "which
    /// addrs are still pending an ack" state lives here and is NOT
    /// persisted (retry attempts reset on failover, which is correct —
    /// a new leader's first attempt is its own "attempt 1"). Populated
    /// on `enqueue_pending_deletes` and on `replay_from_etcd` (from
    /// Delete-kind ledger entries with attempts=0).
    pub(crate) delete_progress: Rc<RefCell<HashMap<u64, crate::extent_delete::PendingDelete>>>,
    /// persisted "tried 60 times in extent_delete_loop and still
    /// failed" queue. Hydrated from the `extentDeleteRetry/` etcd
    /// prefix at replay; updated by `persist_failed_delete` and
    /// `extent_delete_retry_loop`. Survives manager restart + leader
    /// failover (the in-memory shadow rebuilds from etcd; an extent
    /// stays in the queue until every replica's `EXT_MSG_DELETE_EXTENT`
    /// acks). Independent from `delete_progress` (primary 2 s loop)
    /// and from the inflight ledger (Delete marker is released when
    /// the entry moves to this queue).
    pub(crate) failed_deletes:
        Rc<RefCell<HashMap<u64, crate::extent_delete::MgrExtentDeleteRetry>>>,
    /// Etcd-less (memory-only) shadow of the
    /// fuse-fs inode-allocator counter, keyed PER-VOLUME (the canonicalized
    /// `fs/{tenant}/{volume}/` prefix; empty key = the legacy global counter).
    /// In etcd-backed mode the AUTHORITATIVE per-volume counters live at
    /// `fs_alloc::fs_next_inode_key(volume)` and every grant is a leader-fenced
    /// CAS txn (this map is unused there); memory-only mode (tests/dev)
    /// allocates straight from this map. NOT part of `alloc_ids` (note 5):
    /// that counter numbers stream/extent/partition ENTITIES replayed from
    /// etcd prefixes; inode numbers are fs-layer data with their own key.
    pub(crate) fs_next_inode: Rc<RefCell<HashMap<Vec<u8>, u64>>>,
    runtime_started: Rc<Cell<bool>>,
    /// true once `serve()`'s listener is actually BOUND and
    /// accepting. The UCX listener bind can retry through a killed
    /// predecessor's TIME_WAIT window for ~60 s — far past
    /// `PS_DEAD_TIMEOUT` — during which no PS heartbeat can possibly
    /// arrive. `ps_liveness_check_loop` must not evict before this is
    /// set (observed: a respawned ucx manager won the election at ~4 s,
    /// evicted the ENTIRE healthy PS fleet at ~10 s, and only bound its
    /// listener at ~54 s).
    serving: Rc<Cell<bool>>,
    ps_last_heartbeat: Rc<RefCell<HashMap<u64, Instant>>>,
    /// ENOSPC-1: per-node max per-disk free bytes, refreshed from every
    /// successful df probe (`node_health_loop`). In-memory only — it is
    /// a 2 s-fresh routing hint, not state worth persisting. Keyed by
    /// node_id; absent = "unknown" (treated as spacious: cold leader /
    /// pre-first-df nodes must stay allocatable).
    pub(crate) node_max_free: Rc<RefCell<HashMap<u64, u64>>>,
    /// cluster-df: in-memory capacity snapshot for `MSG_CLUSTER_DF`. RAW +
    /// physical_used are summed from every EN's df report each
    /// `node_health_loop` tick (the EN self-reports its real per-disk extent
    /// footprint — no amplification formula); `logical_stored` is a periodic
    /// read-only Σ distinct sealed_length. Not persisted (volatile, rebuilt
    /// from df + scan); leader-only meaning.
    pub(crate) cluster_cap: Rc<RefCell<ClusterCapSnapshot>>,
    /// ENOSPC-1: allocation soft-avoids nodes whose max per-disk free is
    /// below this (`--min-alloc-free-bytes`, default 256 MiB; 0 =
    /// disabled). Soft: select_nodes falls back to the full healthy set
    /// when too few spacious nodes remain.
    min_alloc_free_bytes: Rc<Cell<u64>>,
    /// audit log retention (days; `--audit-retention-days`,
    /// default 90, 0 = disabled). Enforced by `audit_gc_loop` — the GC
    /// helper already existed but had NO caller, so `mgr_audit_log/`
    /// grew in etcd unboundedly.
    pub(crate) audit_retention_days: Rc<Cell<u64>>,
    /// etcd-chaos D1: WHY are we not leader? `true` (the safe default —
    /// every fresh/rejoined process starts displaced) = another instance
    /// holds/held leadership or we never won it: our in-memory state may
    /// be arbitrarily stale → routing reads answer NOT_LEADER (the
    /// rejoined-follower fix). `false` while `!leader` = we WERE the leader and lost the
    /// lease without anyone replacing us (etcd outage / lease blip): our
    /// in-memory routing is the freshest that exists and NO new leader
    /// can be elected or mutate anything while etcd is down — serving
    /// get_regions + heartbeats STALE-WHILE-LEADERLESS keeps the data
    /// plane fully alive through etcd maintenance (pre-fix, a >90 s etcd
    /// outage black-holed fresh clients AND suicided the PS fleet).
    /// Shared with `EtcdMirror` so the leader-fence diagnosis paths can
    /// flip it when they observe a DIFFERENT leader id.
    displaced: Rc<Cell<bool>>,
    /// etcd-chaos D1 (coco P1): when the stale-while-leaderless window
    /// opened (keepalive lost without observed displacement). Bounds the
    /// mode with `ROUTABLE_STALE_TTL`: in an ASYMMETRIC partition (only
    /// this manager lost etcd; another instance takes over) displacement
    /// is only detected once OUR etcd link recovers (election CAS sees
    /// the new holder) — without a TTL this manager would serve stale
    /// routing/heartbeats indefinitely and pin the PS fleet away from
    /// the real leader. 15 min covers routine etcd maintenance; past it
    /// the gate fails closed (pre-fix behavior).
    leaderless_since: Rc<Cell<Option<Instant>>>,
    conn_pool: Rc<ConnPool>,
    /// dedicated pool for control-plane RPCs to extent nodes
    /// (`EXT_MSG_DF`, future `MSG_REPORT_DISK_FAILURE`, future heartbeat).
    /// Separate from `conn_pool` (which carries data-plane RPCs like
    /// `CONVERT_TO_EC`, `COPY_EXTENT`, `RECOVERY`). The split prevents a
    /// large data-plane RPC's TCP send buffer / io_uring CQ pressure
    /// from delaying the next DF probe to the point where the
    /// `RpcClient` flips its `closed` flag and the disk_status loop
    /// flaps the node `online → offline → online`. All DF traffic goes
    /// here at `node.control_address` (or `node.address` for legacy
    /// nodes whose `control_address` is empty).
    control_pool: Rc<ConnPool>,
    // `pending_extent_deletes` field deleted. Replaced by
    // `inflight` (etcd-persisted exclusion + snapshot for failover) +
    // `delete_progress` (in-memory live retry state). The delete
    // semantics — "extents whose refs dropped to 0 still need to be
    // unlinked on every replica" — are unchanged; the persistence
    // model upgraded from in-memory only to etcd-backed (closes the
    // pre-ledger footnote that manager restart lost pending entries).
    /// per-partition unix-epoch timestamp of the last split or merge
    /// involving this partition. Sourced from etcd prefix
    /// `partitionLastOp/<part_id>` (i64 little-endian). Default 0 for
    /// partitions never split/merged. Used by the policy engine for
    /// cooldown.
    pub(crate) last_op_at: Rc<RefCell<HashMap<u64, i64>>>,
    /// policy engine — split/merge candidate computation over a
    /// 30-min sliding window of per-partition load metrics. It was
    /// later extended to also generate minor-compact + EC advisories.
    /// deleted the auto-dispatch consumer of this output — the
    /// engine is now advisory-only; external controllers query
    /// `MSG_GET_POLICY_CANDIDATES` and call client subcommands to
    /// act on what they see.
    pub(crate) policy: Rc<RefCell<crate::policy::PolicyEngine>>,
    /// M2: in-manager auto-policy controller state — mode +
    /// active policy + custom policies + cooldowns + rolling action log.
    /// Config (mode/active/custom) is etcd-persisted (`autoPolicy/config`,
    /// leader-fenced) + cooldowns (`autoPolicy/cooldowns`), replayed on leader
    /// promotion so the active policy survives failover. The controller loop
    /// runs ONLY on the leader and is DEFAULT-OFF (a fresh cluster stays
    /// pure-mechanism).
    pub(crate) auto_policy: Rc<RefCell<crate::auto_policy::AutoPolicyState>>,
    /// Consecutive reconcile rounds in which a node reported holding an extent
    /// it is NOT a member of, keyed by `(node_id, extent_id)`. Residue is only
    /// collected after the verdict has been stable for several rounds — a
    /// momentarily-wrong view (mid-`apply_recovery_done` slot swap, a freshly
    /// promoted leader) must never delete real data. Leader-local: a leader
    /// change resets the counters, which only ever DELAYS a deletion.
    /// Keyed `(node_id, shard_idx, extent_id)` — see `ReconcileExtentsReq`.
    pub(crate) reconcile_non_member: Rc<RefCell<HashMap<(u64, u32, u64), u32>>>,
    /// Extent ids whose files have been created on their nodes but whose
    /// `extents/<id>` record is not published yet.
    ///
    /// Allocation creates the files FIRST (`place_extents_with_fallback`) and
    /// commits to etcd after, so inside that window a node's reconcile report
    /// names an extent the store has no record of — which the orphan filter
    /// would otherwise read as garbage and order deleted seconds after it was
    /// legitimately created. Held only for the length of one allocation, by an
    /// RAII guard, so no error path can leave an id stuck here.
    pub(crate) allocating_extents: Rc<RefCell<std::collections::HashSet<u64>>>,
    /// leader-local, in-memory ledger of submitted long-running ops (the
    /// queryable state + failure reason for split/merge/rebalance/compact/gc/
    /// forcegc/ec-convert). Terminal outcomes also go to the durable audit log.
    pub(crate) ops: Rc<RefCell<crate::op_ledger::OpLedger>>,
    /// preset to seed as the active policy (Armed) on
    /// a FRESH cluster — one with no persisted `autoPolicy/config`. Set from the
    /// bin (`--auto-policy-default <preset>`, deploy-layer default `balanced`);
    /// `None` = leave the controller Off until an operator activates one (cluster.sh
    /// / tests / dev). Seed is IN-MEMORY only, so the first operator config change
    /// (or a deactivate) persists over it and later failovers replay that instead.
    pub(crate) auto_policy_default: Rc<RefCell<Option<String>>>,
    /// did the last etcd replay find a persisted
    /// `autoPolicy/config`? Set during replay, read by `apply_auto_policy_default`
    /// (which runs from the bin after flags are set) to decide whether to seed.
    pub(crate) auto_policy_had_persisted_config: Rc<Cell<bool>>,
    /// per-node sliding-window of push-based failure reports from
    /// PSes. Eviction window = `report_disk_failure_window`; quorum
    /// threshold = `report_disk_failure_quorum` distinct
    /// `reporter_part_id` → `mark_node_disks_offline` (in-memory only —
    /// the result reflects truth that the manager learns from
    /// `disk_status_update_loop` on the next 10 s tick, so it does NOT
    /// need to persist to etcd). A successful DF in
    /// `disk_status_update_loop` clears this entry so a stale burst of
    /// reports doesn't re-trip the quorum after the node recovers.
    pub(crate) recent_failure_reports:
        Rc<RefCell<HashMap<u64, std::collections::VecDeque<(Instant, u64)>>>>,
    /// per-extent-node auto-tracked liveness (Online ↔ Suspected).
    /// Fed by `disk_status_update_loop` (df ok / fail) and `register_node`
    /// (initial heartbeat). Consumed by health-report RPCs,
    /// recovery dispatch gate, and the EC dispatch loop's
    /// Suspected-window skip. **No automatic Down transition** — fence
    /// is operator-driven (`mgr_fence_node`).
    pub(crate) node_states: Rc<RefCell<crate::node_state::NodeStateTracker>>,
    /// persistent operator overrides keyed on node_id. Mirrors
    /// the etcd prefix `node_override/<node_id>`. Mutated only via the
    /// admin RPCs `mgr_fence_node` / `mgr_set_node_maintenance` /
    /// `mgr_clear_node_override` / by Maintenance TTL expiry inside
    /// `node_states.tick()`. Survives leader failover via etcd replay.
    pub(crate) node_overrides: Rc<RefCell<HashMap<u64, MgrNodeOverride>>>,
    /// tombstones for `mgr_remove_node`. Etcd prefix
    /// `decommissioned/<node_id>` — written when the OP removes a node.
    /// Read by `handle_register_node`'s zombie-defense check.
    pub(crate) decommissioned: Rc<RefCell<HashMap<u64, MgrNodeOverride>>>,
    /// in-memory recovery throttle counters (per-source /
    /// per-target / global). Mutated by `dispatch_recovery_task` on
    /// acquire and by `apply_recovery_done` / `drain_extent_inflight_marker`
    /// on release. NOT persisted — limits are advisory, not safety
    /// invariants.
    pub(crate) recovery_limiter: Rc<RefCell<crate::recovery_rate_limiter::RecoveryRateLimiter>>,
    /// per-partition last `MSG_ROLL_TAILS` send time (unix secs).
    /// The open-tail drain sweep in `recovery_dispatch_loop` uses a 30 s
    /// cooldown so a repeatedly-failing roll doesn't hammer the PS every tick.
    /// In-memory only (a routing hint; safe to lose on failover).
    pub(crate) roll_tails_cooldown: Rc<RefCell<HashMap<u64, i64>>>,
    /// per-process audit-log sequence counter. Combined with
    /// the unix-nanosecond timestamp to form the `mgr_audit_log/`
    /// suffix so ordering is unique even for concurrent appends.
    pub(crate) audit_seq: Rc<Cell<u64>>,
    /// quorum debounce — sliding-window length. Default 60 s.
    /// Configured via the manager binary's `--report-disk-failure-window-secs`
    /// CLI flag (was previously `AUTUMN_REPORT_DISK_FAILURE_WINDOW_SECS`).
    pub(crate) report_disk_failure_window: Cell<Duration>,
    /// quorum debounce — distinct-reporter threshold to flip
    /// node offline. Default 3. Configured via the manager binary's
    /// `--report-disk-failure-quorum` CLI flag (was previously
    /// `AUTUMN_REPORT_DISK_FAILURE_QUORUM`).
    pub(crate) report_disk_failure_quorum: Cell<usize>,
    /// persistent cluster identity. CAS-created in etcd
    /// (`autumn-rs/cluster_id`) by the first leader; inherited by
    /// subsequent leaders via `replay_from_etcd`. Empty when the manager
    /// is running in memory-only mode (no etcd) — in that mode
    /// `MSG_GET_CLUSTER_ID` reports the per-process random UUID set in
    /// `Self::new()` so dev/test workflows still work end-to-end. Read
    /// by `handle_get_cluster_id`.
    pub(crate) cluster_id: Rc<RefCell<String>>,
    /// R1 rolling upgrade: persisted cluster_version (etcd
    /// `autumn-rs/cluster_version`, ASCII decimal). The operator-bumped
    /// operator-driven feature gate — new wire
    /// forms / persisted formats versioned N may only be EMITTED once
    /// this reaches N. CAS-seeded to the first leader's
    /// `WIRE_VERSION_MAX` by `imprint_cluster_version`; bumped only via
    /// `MSG_BUMP_CLUSTER_VERSION` (monotonic, exactly +1, capped at this
    /// binary's own WIRE_VERSION_MAX). Memory-only mode starts at this
    /// binary's WIRE_VERSION_MAX.
    pub(crate) cluster_version: Rc<Cell<u32>>,
    /// Inode-level lease registry shared between the
    /// AcquireLease / ReleaseLease / HeartbeatLease / PollInvalidations
    /// handlers and the `inode_lease_revoke_loop` background task.
    /// Writer leases are persisted under the `inode_leases/` etcd
    /// prefix; reader leases live in memory only. See
    /// `crates/manager/src/inode_lease.rs` and
    /// `docs/autumn_fs_lease_plan.md`.
    pub(crate) inode_leases: crate::inode_lease::SharedRegistry,
    /// the manager's Ed25519 signing keyring (KDC private
    /// material), loaded once from `--auth-signing-key-file`. `None` = authz
    /// disabled (opt-in; fuse/kvcache/dev unaffected). Set at startup only.
    pub(crate) authz_keyring: Rc<RefCell<Option<crate::authz::AuthzKeyring>>>,
    /// admin token gating the tenant-create/delete RPCs
    /// (admin_auth_design.md Option A). `None` = those admin RPCs are refused.
    pub(crate) admin_token: Rc<RefCell<Option<String>>>,
    /// key prefixes under which the PS applies default-DENY (e.g.
    /// `mem/`). Published in `GET_AUTHZ_CONFIG`. Each ends with `/`.
    pub(crate) protected_prefixes: Rc<RefCell<Vec<Vec<u8>>>>,
    /// TTL (seconds) minted tokens get. Default 3600 (1 h).
    pub(crate) token_ttl_secs: Rc<Cell<u64>>,
    /// clock-skew leeway (seconds) advertised to the PS. Default 60.
    pub(crate) clock_skew_secs: Rc<Cell<u64>>,
    /// tenant account DB (etcd `tenantAccount/<tenant>` →
    /// `MgrTenantAccount`). Replayed on leader failover; mutated only via the
    /// admin RPCs. Stores the credential HASH, never the raw credential.
    pub(crate) tenant_accounts: Rc<RefCell<HashMap<String, MgrTenantAccount>>>,
    /// serializes the tenant create/delete critical section
    /// (build → etcd write → in-memory apply). Handlers are spawned per-frame
    /// and interleave at the etcd await, and a tenant account's value is a
    /// NON-idempotent freshly-generated secret — without this, two concurrent
    /// same-tenant ops could commit to etcd in one order but apply to memory in
    /// the other, leaving the live leader's in-memory hash out of sync with
    /// etcd (coco P1). Low-frequency admin path → a global async mutex is free.
    pub(crate) tenant_admin_lock: Rc<futures::lock::Mutex<()>>,
    /// D2: namespace registry shadow (etcd `namespace/<name>` →
    /// `MgrNamespace`). Replayed on leader failover; mutated only via the admin
    /// namespace-create/delete RPCs + `seed_builtin_namespaces`. Keyed by name.
    pub(crate) namespaces: Rc<RefCell<HashMap<String, MgrNamespace>>>,
    /// D2: serializes the namespace create/delete critical section
    /// (build → etcd write → in-memory apply), mirroring `tenant_admin_lock`.
    /// Low-frequency admin path → a global async mutex is free.
    pub(crate) namespace_admin_lock: Rc<futures::lock::Mutex<()>>,
}

impl Default for AutumnManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Keeps an extent id in `AutumnManager::allocating_extents` for as long as its
/// allocation is in flight, and removes it on drop — including on every early
/// return and error path, which is the point of making it a guard rather than a
/// pair of calls.
pub(crate) struct AllocatingExtentGuard {
    set: Rc<RefCell<std::collections::HashSet<u64>>>,
    extent_id: u64,
}

impl Drop for AllocatingExtentGuard {
    fn drop(&mut self) {
        self.set.borrow_mut().remove(&self.extent_id);
    }
}

impl AutumnManager {
    pub fn new() -> Self {
        Self {
            store: MetadataStore::new(),
            leader: Rc::new(Cell::new(true)),
            etcd: None,
            instance_id: Rc::new(uuid::Uuid::new_v4().to_string()),
            inflight: Rc::new(RefCell::new(HashMap::new())),
            inflight_attempt_nonce: Rc::new(RefCell::new(HashMap::new())),
            extent_payload_location: Rc::new(RefCell::new(HashMap::new())),
            extent_corrupt_slots: Rc::new(RefCell::new(HashMap::new())),
            op_log_seq: Cell::new(0),
            op_log_writes_since_gc: Cell::new(0),
            split_inflight: Rc::new(RefCell::new(std::collections::HashSet::new())),
            delete_progress: Rc::new(RefCell::new(HashMap::new())),
            failed_deletes: Rc::new(RefCell::new(HashMap::new())),
            fs_next_inode: Rc::new(RefCell::new(HashMap::new())),
            runtime_started: Rc::new(Cell::new(false)),
            serving: Rc::new(Cell::new(false)),
            ps_last_heartbeat: Rc::new(RefCell::new(HashMap::new())),
            node_max_free: Rc::new(RefCell::new(HashMap::new())),
            cluster_cap: Rc::new(RefCell::new(ClusterCapSnapshot::default())),
            min_alloc_free_bytes: Rc::new(Cell::new(DEFAULT_MIN_ALLOC_FREE_BYTES)),
            audit_retention_days: Rc::new(Cell::new(90)),
            displaced: Rc::new(Cell::new(true)),
            leaderless_since: Rc::new(Cell::new(None)),
            conn_pool: Rc::new(ConnPool::new()),
            control_pool: Rc::new(ConnPool::new()),
            last_op_at: Rc::new(RefCell::new(HashMap::new())),
            policy: Rc::new(RefCell::new(crate::policy::PolicyEngine::default())),
            auto_policy: Rc::new(RefCell::new(crate::auto_policy::AutoPolicyState::default())),
            reconcile_non_member: Rc::new(RefCell::new(HashMap::new())),
            allocating_extents: Rc::new(RefCell::new(std::collections::HashSet::new())),
            ops: Rc::new(RefCell::new(crate::op_ledger::OpLedger::new())),
            auto_policy_default: Rc::new(RefCell::new(None)),
            auto_policy_had_persisted_config: Rc::new(Cell::new(false)),
            recent_failure_reports: Rc::new(RefCell::new(HashMap::new())),
            // defaults match the legacy env defaults.
            report_disk_failure_window: Cell::new(Duration::from_secs(60)),
            report_disk_failure_quorum: Cell::new(3),
            // env-controlled soft-timeout, default 10 s.
            node_states: Rc::new(RefCell::new(crate::node_state::NodeStateTracker::default())),
            // starts empty; populated by replay / admin RPCs.
            node_overrides: Rc::new(RefCell::new(HashMap::new())),
            decommissioned: Rc::new(RefCell::new(HashMap::new())),
            // starts at the env-configured default limits.
            recovery_limiter: Rc::new(RefCell::new(
                crate::recovery_rate_limiter::RecoveryRateLimiter::from_env(),
            )),
            roll_tails_cooldown: Rc::new(RefCell::new(HashMap::new())),
            audit_seq: Rc::new(Cell::new(0)),
            // in memory-only mode this serves as the cluster
            // identity. Overwritten by `try_become_leader` /
            // `replay_from_etcd` when etcd is configured.
            cluster_id: Rc::new(RefCell::new(uuid::Uuid::new_v4().to_string())),
            // R1: memory-only mode runs at this binary's max wire
            // version. Overwritten by `try_become_leader` /
            // `replay_from_etcd` when etcd is configured.
            cluster_version: Rc::new(Cell::new(autumn_rpc::WIRE_VERSION_MAX)),
            // Empty registry; populated on
            // AcquireLease and on `replay_from_etcd`.
            inode_leases: Rc::new(RefCell::new(crate::inode_lease::LeaseRegistry::with_ttl(
                std::time::Duration::from_secs(
                    crate::inode_lease::DEFAULT_LEASE_TTL_SECS as u64,
                ),
            ))),
            // authz OFF unless the binary loads a signing-key file.
            authz_keyring: Rc::new(RefCell::new(None)),
            admin_token: Rc::new(RefCell::new(None)),
            protected_prefixes: Rc::new(RefCell::new(Vec::new())),
            token_ttl_secs: Rc::new(Cell::new(3600)),
            clock_skew_secs: Rc::new(Cell::new(60)),
            tenant_accounts: Rc::new(RefCell::new(HashMap::new())),
            tenant_admin_lock: Rc::new(futures::lock::Mutex::new(())),
            // D2: empty; populated by replay / admin RPCs /
            // seed_builtin_namespaces on leader promotion.
            namespaces: Rc::new(RefCell::new(HashMap::new())),
            namespace_admin_lock: Rc::new(futures::lock::Mutex::new(())),
        }
    }

    /// Quorum debounce config setter. Called by the manager
    /// binary's main() after CLI parsing; the public API mirrors the
    /// existing `set_auto_split` / `set_policy_config` pattern.
    /// `quorum` is clamped to at least 1.
    /// Announce that `extent_id`'s files are being created on their nodes, so
    /// the orphan reconcile gives no verdict on it until the guard drops (which
    /// is after the allocation has either committed or failed).
    pub(crate) fn mark_allocating(&self, extent_id: u64) -> AllocatingExtentGuard {
        self.allocating_extents.borrow_mut().insert(extent_id);
        AllocatingExtentGuard {
            set: self.allocating_extents.clone(),
            extent_id,
        }
    }

    pub fn set_report_disk_failure_config(&self, window: Duration, quorum: usize) {
        self.report_disk_failure_window.set(window);
        self.report_disk_failure_quorum.set(quorum.max(1));
    }

    /// install the KDC signing keyring (parsed by the binary from
    /// `--auth-signing-key-file`). Its presence ENABLES data-plane authz.
    pub fn set_authz_keyring(&self, keyring: crate::authz::AuthzKeyring) {
        *self.authz_keyring.borrow_mut() = Some(keyring);
    }

    /// set the admin token gating tenant-create/delete
    /// (`--admin-token`). Without it those admin RPCs are refused.
    pub fn set_admin_token(&self, token: String) {
        *self.admin_token.borrow_mut() = Some(token);
    }

    /// (PS slice): prefix the manager's admin token onto a
    /// payload bound for a PS `is_admin_ps_msg` (split / maintenance). The
    /// manager DRIVES those ops itself — the auto-policy controller's split +
    /// gc/compact and merge's flush are manager→PS calls — so with the PS gate
    /// on (the manager configured a token, which the PS learns via
    /// GetAuthzConfigResp), the manager must authenticate exactly like an
    /// operator's autumn-op. No token configured → unchanged payload (the PS
    /// gate is off too, so it runs bare).
    fn admin_prefix_ps(&self, payload: bytes::Bytes) -> bytes::Bytes {
        match self.admin_token.borrow().as_ref() {
            Some(tok) => autumn_rpc::manager_rpc::prefix_admin_token(tok.as_bytes(), &payload),
            None => payload,
        }
    }

    /// set the protected (default-DENY) key prefixes
    /// (`--auth-protected-prefix`, repeatable). Each is normalized to end `/`.
    pub fn set_protected_prefixes(&self, mut prefixes: Vec<Vec<u8>>) {
        for p in &mut prefixes {
            if p.last() != Some(&b'/') {
                p.push(b'/');
            }
        }
        *self.protected_prefixes.borrow_mut() = prefixes;
    }

    /// set the minted-token TTL in seconds. Clamped to
    /// [60, 30 days] — the design wants short TTLs (prod hours); the 30-day
    /// ceiling both discourages long-lived bearer tokens and prevents an absurd
    /// value from overflowing `now + ttl` (coco P2). 30-day cap is generous.
    pub fn set_token_ttl_secs(&self, secs: u64) {
        self.token_ttl_secs.set(secs.clamp(60, 30 * 24 * 3600));
    }

    /// set the advertised clock-skew leeway in seconds. Capped at 1 h
    /// (design suggests 30-120 s; the cap bounds `exp + skew` against wrap and a
    /// misconfigured huge leeway that would neuter expiry).
    pub fn set_clock_skew_secs(&self, secs: u64) {
        self.clock_skew_secs.set(secs.min(3600));
    }

    /// read the last_op_at timestamp for a partition (0 if never op'd).
    #[allow(dead_code)]
    pub(crate) fn last_op_at_for(&self, part_id: u64) -> i64 {
        self.last_op_at.borrow().get(&part_id).copied().unwrap_or(0)
    }

    /// test helper: dispatch a SPLIT against `part_id` as if the
    /// policy engine had picked it. Snapshots state internally.
    /// test helper: override the policy engine's thresholds.
    /// Tests can lower `required_buckets` and `tick_interval_sec` to
    /// fast-mode the full policy_tick_loop.
    pub fn set_policy_config(&self, config: crate::policy::PolicyConfig) {
        self.policy.borrow_mut().set_config(config);
    }

    pub async fn force_auto_split(&self, part_id: u64) -> Result<()> {
        let state = (*self.store.inner.borrow()).clone();
        let cand = autumn_rpc::manager_rpc::PolicyCandidate {
            kind: autumn_rpc::manager_rpc::POLICY_KIND_SPLIT,
            primary_part_id: part_id,
            secondary_part_id: 0,
            reason: "test forced split".to_string(),
            size_bytes: 0,
            req_per_sec: 0,
            imm_full_per_sec: 0,
            same_ps: true,
            last_op_at: 0,
        };
        self.auto_dispatch_split(&cand, None, &state).await
    }

    /// test helper: orchestrate a MERGE for (survivor, victim) as
    /// if the policy engine had picked it. Snapshots state internally.
    pub async fn force_auto_merge(&self, survivor: u64, victim: u64) -> Result<()> {
        let state = (*self.store.inner.borrow()).clone();
        let cand = autumn_rpc::manager_rpc::PolicyCandidate {
            kind: autumn_rpc::manager_rpc::POLICY_KIND_MERGE,
            primary_part_id: survivor,
            secondary_part_id: victim,
            reason: "test forced merge".to_string(),
            size_bytes: 0,
            req_per_sec: 0,
            imm_full_per_sec: 0,
            same_ps: true,
            last_op_at: 0,
        };
        self.auto_dispatch_merge(&cand, &state).await
    }

    pub async fn new_with_etcd(endpoints: Vec<String>) -> Result<Self> {
        let mut s = Self::new();
        s.leader.set(false);
        s.etcd = Some(
            EtcdMirror::connect(
                endpoints,
                s.instance_id.clone(),
                s.leader.clone(),
                s.displaced.clone(),
            )
            .await?,
        );
        s.replay_from_etcd().await?;
        let _ = s.try_become_leader().await;
        s.start_runtime_tasks();
        Ok(s)
    }

    pub fn set_leader(&self, leader: bool) {
        self.leader.set(leader);
    }

    fn ensure_leader(&self) -> Result<(), AppError> {
        if self.leader.get() {
            Ok(())
        } else {
            Err(AppError::NotLeader)
        }
    }

    /// etcd-chaos D1: gate for READ-ONLY routing/liveness RPCs
    /// (`get_regions`, `heartbeat_ps`). Serves when leader OR when
    /// leaderless WITHOUT displacement (we were the last leader and no
    /// one can have superseded our state — etcd outage). Mutating
    /// handlers stay on the strict `ensure_leader` (they need etcd
    /// anyway). The rejoined-follower blackhole stays closed:
    /// a process that never won (or observed another holder) has
    /// `displaced == true`.
    pub(crate) fn ensure_routable(&self) -> Result<(), AppError> {
        if self.leader.get() {
            return Ok(());
        }
        // Bounded stale-while-leaderless (coco P1): only while we have
        // never observed a successor AND the window is fresh. In an
        // asymmetric partition displacement is detected when OUR etcd
        // link recovers (election CAS sees the new holder); the TTL caps
        // the harm until then.
        const ROUTABLE_STALE_TTL: Duration = Duration::from_secs(900);
        if !self.displaced.get() {
            if let Some(t0) = self.leaderless_since.get() {
                if t0.elapsed() < ROUTABLE_STALE_TTL {
                    return Ok(());
                }
            }
        }
        Err(AppError::NotLeader)
    }

    /// Start background loops. Called from `new_with_etcd` and `serve`.
    /// Idempotent — safe to call multiple times.
    /// (1C): spawn a manager background loop with panic-isolation +
    /// auto-restart. Previously each loop was a bare
    /// `compio::runtime::spawn(...).detach()` with NO supervision: a panic
    /// (e.g. a RefCell double-borrow, an index out of bounds) silently
    /// killed just that one task while the rest of the manager kept
    /// running — there was no log, no restart, no signal. That is exactly
    /// the failure shape that let the node_health_loop freeze go
    /// undetected for ~11 minutes in production. The supervisor wraps the
    /// loop future in `catch_unwind`; on a panic OR an unexpected return
    /// it logs ERROR (loud, greppable) and restarts the loop after a 1 s
    /// backoff, cloning a fresh manager handle each time.
    ///
    /// NOTE: `catch_unwind` cannot rescue a *hung* `.await` — a stuck
    /// future never returns, so the supervisor would wait forever too.
    /// Hangs are prevented separately (1A): every await a loop can
    /// reach is now bounded (etcd `unary_call` timeout, ConnPool connect +
    /// request timeouts). The two together close both failure modes.
    ///
    /// NOTE on layered `catch_unwind`: `compio::runtime::spawn` already
    /// wraps the future in `AssertUnwindSafe(future).catch_unwind()`
    /// internally (compio-runtime-0.11.0/src/runtime/mod.rs:202); its
    /// `JoinHandle<T>` is `Task<Result<T, Box<dyn Any + Send>>>`. That's
    /// why the earlier `spawn(loop).detach()` was "silently dead" — compio
    /// caught the panic, then `.detach()` dropped the captured `Err`. Our
    /// inner `catch_unwind` here is for OBSERVABILITY + RESTART decisioning
    /// (read the Result to log + sleep + reschedule), NOT to keep the
    /// runtime alive (which is compio's job). Don't try to "remove the
    /// duplicate" — you'd silently break the restart loop.
    fn spawn_supervised<F, Fut>(name: &'static str, make: F)
    where
        F: Fn() -> Fut + 'static,
        Fut: std::future::Future<Output = ()> + 'static,
    {
        compio::runtime::spawn(async move {
            use futures::future::FutureExt;
            loop {
                let outcome = std::panic::AssertUnwindSafe(make()).catch_unwind().await;
                match outcome {
                    Ok(()) => tracing::error!(
                        bg_loop = name,
                        "manager background loop returned unexpectedly; restarting in 1s"
                    ),
                    Err(_) => tracing::error!(
                        bg_loop = name,
                        "manager background loop PANICKED; restarting in 1s"
                    ),
                }
                compio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        })
        .detach();
    }

    pub fn start_runtime_tasks(&self) {
        if self.runtime_started.get() {
            return;
        }
        self.runtime_started.set(true);

        // (1C): every loop runs under spawn_supervised (panic ->
        // ERROR log + restart) instead of a bare detached spawn.

        // Leader election only needed with etcd (non-etcd is always leader).
        if self.etcd.is_some() {
            let mgr = self.clone();
            Self::spawn_supervised("leader_election", move || {
                mgr.clone().leader_election_loop()
            });
        }

        let mgr = self.clone();
        Self::spawn_supervised("recovery_dispatch", move || {
            mgr.clone().recovery_dispatch_loop()
        });

        // single df caller — merges the former recovery_collect_loop
        // (2 s, apply done_tasks) and disk_status_update_loop (10 s, disk +
        // node liveness). Eliminates the race where the empty-`tasks` df
        // drained the EN's recovery_done and discarded the completions.
        let mgr = self.clone();
        Self::spawn_supervised("node_health", move || mgr.clone().node_health_loop());

        // completion: audit retention GC was a dead helper —
        // mgr_audit_log/ grew unboundedly. Daily leader-only sweep.
        if self.etcd.is_some() {
            let mgr = self.clone();
            Self::spawn_supervised("audit_retention_gc", move || mgr.clone().audit_gc_loop());
        }

        let mgr = self.clone();
        Self::spawn_supervised("ec_conversion_dispatch", move || {
            mgr.clone().ec_conversion_dispatch_loop()
        });

        let mgr = self.clone();
        Self::spawn_supervised("ps_liveness_check", move || {
            mgr.clone().ps_liveness_check_loop()
        });

        // physical extent file deletion fanout.
        let mgr = self.clone();
        Self::spawn_supervised("extent_delete", move || mgr.clone().extent_delete_loop());

        // persisted-retry slow loop for deletes that exhausted
        // the primary 60-attempt budget.
        let mgr = self.clone();
        Self::spawn_supervised("extent_delete_retry", move || {
            mgr.clone().extent_delete_retry_loop()
        });

        // EXTENT10-AUTORECLAIM: reclaim both-zero orphan extents (refs==0 &&
        // vp_table_refs==0, in no stream) that the punch/truncate refs-side
        // delete path never sees because they lost their last membership
        // out-of-band (extent-10 class).
        let mgr = self.clone();
        Self::spawn_supervised("extent_both_zero_sweep", move || {
            mgr.clone().extent_both_zero_sweep_loop()
        });

        // policy advisory tick.
        let mgr = self.clone();
        Self::spawn_supervised("policy_tick", move || mgr.clone().policy_tick_loop());

        // Leader-fenced auto-policy controller (DEFAULT-OFF;
        // ticks + actuates ONLY on the leader; actuation is gated per-policy by
        // the Armed vs DryRun mode).
        let mgr = self.clone();
        Self::spawn_supervised("auto_policy", move || mgr.clone().auto_policy_tick_loop());

        // stale-marker WARN sweep. Iterates the inflight ledger
        // every 5 minutes and logs WARN for any marker > 24h old.
        // Auto-clearing is INTENTIONALLY not done — a stuck marker
        // usually signals a real bug worth surfacing. Operator runs the
        // Python ops `--clear-stale-inflight extent <id>` script after
        // investigating.
        let mgr = self.clone();
        Self::spawn_supervised("extent_inflight_stale_sweep", move || {
            mgr.clone().extent_inflight_stale_sweep_loop()
        });

        // TTL revoke pass — once per second, sweep
        // expired writer leases (queues `LEASE_REVOKED` invalidations
        // for readers; etcd-deletes the persisted record) and silently
        // drops expired reader leases.
        let mgr = self.clone();
        Self::spawn_supervised("inode_lease_revoke", move || {
            mgr.clone().inode_lease_revoke_loop()
        });
    }

    /// every POLICY_TICK_INTERVAL_SEC, leader recomputes split/merge
    /// candidates from the per-partition load windows + last_op_at +
    /// region owners. Logs new candidates at INFO; exposes the cache
    /// via MSG_GET_POLICY_CANDIDATES.
    async fn policy_tick_loop(self) {
        loop {
            // Re-read tick interval each cycle so set_policy_config takes
            // effect immediately (matters in tests; production stays at 60s).
            let interval =
                Duration::from_secs(self.policy.borrow().config.tick_interval_sec.max(1) as u64);
            compio::time::sleep(interval).await;
            if !self.leader.get() {
                continue;
            }
            // TTL backstop: flip any RUNNING PS-executed op (compact/gc/forcegc)
            // whose terminal outcome never came back to UNKNOWN, keeping
            // `ops status` honest instead of RUNNING forever.
            self.ops.borrow_mut().sweep_running_ttl(Self::epoch_seconds());
            // Backstop drain: kinds no PS reports (recovery, ec-convert) close
            // outside the load heartbeat, so without this their history would
            // wait for unrelated PS traffic.
            self.flush_op_log().await;
            let now = Self::epoch_seconds();
            let owners: HashMap<u64, u64> = {
                let s = self.store.inner.borrow();
                s.regions.iter().map(|(id, r)| (*id, r.ps_id)).collect()
            };
            let last_op = self.last_op_at.borrow().clone();
            let state_snapshot: autumn_common::MetadataState = (*self.store.inner.borrow()).clone();
            // Recompute the full advisory cache for this tick (prune + all five
            // advisory passes + cache write) under a single policy borrow.
            let cands = self.recompute_advisory_cache(&state_snapshot, &last_op, &owners, now);
            if !cands.is_empty() {
                tracing::info!("policy: {} candidate(s)", cands.len());
                for c in &cands {
                    let kind = match c.kind {
                        POLICY_KIND_SPLIT => "SPLIT",
                        POLICY_KIND_MERGE => "MERGE",
                        autumn_rpc::manager_rpc::POLICY_KIND_GC => "GC",
                        autumn_rpc::manager_rpc::POLICY_KIND_MAJOR_COMPACT => "MAJOR_COMPACT",
                        autumn_rpc::manager_rpc::POLICY_KIND_HOT_COLD => "HOT_COLD",
                        autumn_rpc::manager_rpc::POLICY_KIND_MINOR_COMPACT => "MINOR_COMPACT",
                        autumn_rpc::manager_rpc::POLICY_KIND_EC => "EC",
                        autumn_rpc::manager_rpc::POLICY_KIND_REBALANCE => "REBALANCE",
                        _ => "UNKNOWN",
                    };
                    tracing::info!(
                        "  {} primary={} secondary={} reason='{}' size={}MB qps={} imm/s={} same_ps={}",
                        kind,
                        c.primary_part_id,
                        c.secondary_part_id,
                        c.reason,
                        c.size_bytes / (1024 * 1024),
                        c.req_per_sec,
                        c.imm_full_per_sec,
                        c.same_ps,
                    );
                }
            }

            // in-kernel auto-dispatch removed. The advisory_cache
            // is the manager's only contribution to operational policy;
            // an external controller queries `MSG_GET_POLICY_CANDIDATES`
            // (or `client policy`) and acts via the existing client
            // subcommands. The `auto_dispatch_split` /
            // `auto_dispatch_merge` helpers below remain as the
            // mechanism layer; tests + a future programmable trigger
            // surface still depend on them. Keeping the helpers but
            // dropping the in-loop dispatch is the entire point of the
            // mechanism / policy separation.
        }
    }

    /// Recompute the policy advisory cache for one `policy_tick_loop` tick:
    /// prune stale metrics, run all five advisory passes (split/merge
    /// candidates, maintenance GC/major+minor-compact, hot/cold, EC), store the
    /// union into `advisory_cache` (read by `MSG_GET_POLICY_CANDIDATES`), and
    /// return it for logging. Advisory-only: no etcd write, no
    /// extent-state mutation, no fencing. All passes run under a SINGLE
    /// `self.policy` borrow (there is no await), collapsing what were six
    /// separate borrow scopes in the loop body.
    fn recompute_advisory_cache(
        &self,
        state: &autumn_common::MetadataState,
        last_op: &HashMap<u64, i64>,
        owners: &HashMap<u64, u64>,
        now: i64,
    ) -> Vec<PolicyCandidate> {
        // step 4: refresh the operator-declared boundaries
        // BEFORE borrowing the engine (`sacred_boundaries()` borrows
        // `self.namespaces`, and holding the policy borrow across it would be
        // fine today but is exactly the kind of nested-borrow that later grows
        // into a RefCell panic). Cheap: a handful of namespaces × a few points.
        let sacred = self.sacred_boundaries();
        let mut p = self.policy.borrow_mut();
        p.sacred_boundaries = sacred;
        // prune metrics for partitions that no longer exist
        // (post-split / merge / PS-evict) whose latest bucket has aged past
        // STALE_METRICS_AGE_SEC — else advisories fire off zombie metrics
        // indefinitely after a partition is merged away.
        p.prune_stale_metrics(state, now);
        let mut cands = p.compute_candidates(crate::policy::ComputeArgs {
            state,
            last_op_at: last_op,
            region_owners: owners,
            now,
        });
        // maintenance (GC + major/minor compact) — windowed metrics only
        // (`last_gc_at` / `last_compact_at` come from the PS-reported buckets).
        cands.append(&mut p.compute_maintenance_advisory(now));
        // Stage D: hot/cold imbalance (kind = POLICY_KIND_HOT_COLD), ridden
        // on the same advisory_cache for `client info` rendering.
        // the size dimension consumes the same
        // effective-size口径 as split/merge (sealed sums + PS-reported
        // open-tail/debt gauges), so a VP-heavy partition is visible to it.
        let sealed_sums = crate::policy::partition_sealed_sums(state);
        cands.append(&mut p.compute_hot_cold_advisory(owners, &sealed_sums, now));
        // EC advisory — per-extent, sourced from streams + extents (not
        // partition-windowed); the helper filters extents < ec_min_extent_bytes.
        cands.append(&mut p.compute_ec_advisory(state, now));
        // Phase B: cluster-level region→PS imbalance advisory
        // (kind = POLICY_KIND_REBALANCE), sourced from regions + ps_nodes.
        cands.append(&mut p.compute_rebalance_advisory(state, now));
        // Persist the union so MSG_GET_POLICY_CANDIDATES returns all 8 kinds
        // (split, merge, gc, major_compact, hot_cold, minor_compact, ec, rebalance).
        p.advisory_cache = cands.clone();
        p.advisory_cache_at = now;
        cands
    }

    // ── auto-policy controller config ──────────────────────────────────

    /// set the preset to seed as the active policy on
    /// a fresh cluster. Validated (must be a known preset) — an unknown name is a
    /// startup error, surfaced by the bin.
    pub fn set_auto_policy_default(&self, preset: String) {
        *self.auto_policy_default.borrow_mut() = Some(preset);
    }

    /// True iff `name` names a built-in preset. Lets the bin fail-loud on
    /// `--auto-policy-default garbage` before serving.
    pub fn is_known_auto_policy_preset(name: &str) -> bool {
        crate::auto_policy::is_preset_name(name)
    }

    /// seed the default active policy when the cluster
    /// has NO persisted `autoPolicy/config` (a fresh cluster). In-memory only —
    /// the moment an operator changes the config (or deactivates) it persists and
    /// this never fires again for that cluster. `mode = Armed`, so a seeded
    /// policy actuates on its own — arming is per-policy (Armed vs DryRun), with
    /// no separate process-wide gate.
    ///
    /// Called from the bin AFTER `set_auto_policy_default`, because `new_with_etcd`
    /// runs the first replay + election in the constructor, before the flag exists.
    /// Safe to call whether or not this process won the election: it only seeds
    /// in-memory state, which the tick loop reads only while leader; a follower's
    /// seeded state is harmless and is replaced by the real config if it ever
    /// promotes (the replay refreshes `auto_policy_had_persisted_config`).
    pub fn apply_auto_policy_default(&self) {
        if self.auto_policy_had_persisted_config.get() {
            return; // operator/previous-leader config wins — never re-seed
        }
        let Some(preset) = self.auto_policy_default.borrow().clone() else {
            return; // no --auto-policy-default: stay Off (cluster.sh / tests / dev)
        };
        if !crate::auto_policy::is_preset_name(&preset) {
            // Defensive: the bin already validated, but never seed a bogus name.
            tracing::warn!(preset, "auto-policy-default is not a known preset — leaving controller Off");
            return;
        }
        let mut st = self.auto_policy.borrow_mut();
        st.active = preset.clone();
        st.mode = crate::auto_policy::AutoPolicyMode::Armed;
        tracing::info!(
            preset,
            "seeded default active policy on a fresh cluster (Armed → actuates)"
        );
    }

    /// Current controller state for `MSG_AUTOPOLICY_GET`.
    pub(crate) fn autopolicy_snapshot(&self) -> AutoPolicyGetResp {
        let st = self.auto_policy.borrow();
        AutoPolicyGetResp {
            code: CODE_OK,
            message: String::new(),
            mode: st.mode.as_u8(),
            active: st.active.clone(),
            // No process-wide gate anymore — an Armed policy actuates. Kept on the
            // wire (auto-policy status / the page) as "mutations are permitted".
            allow_mutations: true,
            policies: st.all_policies(),
            log: st.log.iter().cloned().collect(),
        }
    }

    /// Persist the cooldown stamps to etcd `autoPolicy/cooldowns` (leader-fenced,
    /// best-effort — a lost stamp on failover is bounded by the manager's own
    /// per-kind cooldowns + inflight flags).
    pub(crate) async fn autopolicy_persist_cooldowns(&self) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let cds = self.auto_policy.borrow().to_cooldowns();
            let value = rkyv_encode(&cds).to_vec();
            etcd.put_msgs_txn(vec![("autoPolicy/cooldowns".to_string(), value)])
                .await?;
        }
        Ok(())
    }

    /// Apply an `AutoPolicySet` op (SET_MODE / SET_ACTIVE / UPSERT / DELETE),
    /// persist the new config to etcd, and echo the resulting state. Leader-only
    /// (the etcd write is leader-fenced; a follower fails NotLeader). Shared by the
    /// `MSG_AUTOPOLICY_SET` handler, the dashboard `/api/policies/*`, and
    /// `autumn-op auto-policy`.
    pub(crate) async fn autopolicy_set(
        &self,
        op: u8,
        mode: u8,
        name: String,
        entry: Option<MgrAutoPolicyEntry>,
    ) -> Result<AutoPolicySetResp, AppError> {
        self.ensure_leader()?;

        // Phase 1 (no await): validate + compute the NEW config on CLONES of the
        // current state, and claim the update slot. Any validation error returns
        // before touching shared state or claiming the slot.
        let (new_mode, new_active, new_custom) = {
            let mut st = self.auto_policy.borrow_mut();
            if st.updating {
                return Err(AppError::Precondition(
                    "another auto-policy update is in progress; retry".to_string(),
                ));
            }
            let mut new_mode = st.mode;
            let mut new_active = st.active.clone();
            let mut new_custom = st.custom.clone();
            match op {
                AUTOPOLICY_OP_SET_MODE => {
                    new_mode = crate::auto_policy::AutoPolicyMode::from_u8(mode);
                }
                AUTOPOLICY_OP_SET_ACTIVE => {
                    // "" = deactivate (no active policy selected).
                    if !name.is_empty() && st.find_policy(&name).is_none() {
                        return Err(AppError::NotFound(format!("no such policy '{name}'")));
                    }
                    new_active = name;
                }
                AUTOPOLICY_OP_UPSERT => {
                    let mut e = entry.ok_or_else(|| {
                        AppError::InvalidArgument("upsert requires an entry".to_string())
                    })?;
                    if e.name.is_empty() {
                        return Err(AppError::InvalidArgument("policy name required".to_string()));
                    }
                    if crate::auto_policy::is_preset_name(&e.name) {
                        return Err(AppError::InvalidArgument(format!(
                            "'{}' is a built-in preset; pick another name",
                            e.name
                        )));
                    }
                    e.builtin = false;
                    crate::auto_policy::sanitize_entry(&mut e); // clamp interval/cooldown/max_actions
                    if let Some(slot) = new_custom.iter_mut().find(|p| p.name == e.name) {
                        *slot = e;
                    } else {
                        new_custom.push(e);
                    }
                }
                AUTOPOLICY_OP_DELETE => {
                    if crate::auto_policy::is_preset_name(&name) {
                        return Err(AppError::InvalidArgument(
                            "cannot delete a built-in preset".to_string(),
                        ));
                    }
                    new_custom.retain(|p| p.name != name);
                    if new_active == name {
                        new_active = String::new();
                        new_mode = crate::auto_policy::AutoPolicyMode::Off;
                    }
                }
                _ => {
                    return Err(AppError::InvalidArgument(format!(
                        "unknown autopolicy op {op}"
                    )))
                }
            }
            st.updating = true; // serialize: no concurrent set until Phase 3 clears it
            (new_mode, new_active, new_custom)
        };

        // Phase 2 (await): persist to etcd FIRST (etcd-first pattern, Note 1) — a
        // failed write must NOT leave the in-memory state (and the actuating
        // loop) running on an unpersisted config.
        let cfg = MgrAutoPolicyConfig {
            ver: 1,
            mode: new_mode.as_u8(),
            active: new_active.clone(),
            policies: new_custom.clone(),
        };
        let persist = if let Some(etcd) = &self.etcd {
            let value = rkyv_encode(&cfg).to_vec();
            etcd.put_msgs_txn(vec![("autoPolicy/config".to_string(), value)])
                .await
        } else {
            Ok(())
        };

        // Phase 3 (no await): release the slot; apply ONLY if etcd succeeded.
        {
            let mut st = self.auto_policy.borrow_mut();
            st.updating = false;
            if persist.is_ok() {
                st.mode = new_mode;
                st.active = new_active;
                st.custom = new_custom;
            }
        }
        persist?; // propagate an etcd failure (in-memory state left unchanged)

        let st = self.auto_policy.borrow();
        Ok(AutoPolicySetResp {
            code: CODE_OK,
            message: String::new(),
            mode: st.mode.as_u8(),
            active: st.active.clone(),
            policies: st.all_policies(),
        })
    }

    /// Send a fully-populated MAINTENANCE request to its partition's owning PS
    /// (in-process, same ConnPool as auto_dispatch_*) and return the decoded
    /// response WITHOUT interpreting its code. The op-ledger path needs the raw
    /// `MaintenanceResp` (op_id correlation + the forcegc advisory in `message`);
    /// `actuate_maintenance` wraps this for the controller's fire-and-forget use.
    async fn send_maintenance(
        &self,
        req: autumn_rpc::partition_rpc::MaintenanceReq,
        state: &autumn_common::MetadataState,
    ) -> Result<autumn_rpc::partition_rpc::MaintenanceResp> {
        let part_id = req.part_id;
        let ps_addr = state
            .part_addrs
            .get(&part_id)
            .cloned()
            .or_else(|| {
                state
                    .regions
                    .get(&part_id)
                    .and_then(|r| state.ps_nodes.get(&r.ps_id).cloned())
            })
            .ok_or_else(|| anyhow::anyhow!("no address for part {part_id}"))?;
        let payload = autumn_rpc::partition_rpc::rkyv_encode(&req);
        // (PS slice): authenticate the manager's own maintenance
        // call so the PS gate (when a token is configured) admits it.
        let payload = self.admin_prefix_ps(payload);
        let resp_bytes = self
            .conn_pool
            .call_timeout(
                &ps_addr,
                autumn_rpc::partition_rpc::MSG_MAINTENANCE,
                payload,
                Duration::from_secs(60),
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        autumn_rpc::partition_rpc::rkyv_decode(&resp_bytes).map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Send a MAINTENANCE op (gc / compact / forcegc) to a partition's owning PS.
    /// `extent_ids` is used only by `MAINTENANCE_FORCE_GC`; gc/compact pass empty.
    /// Untracked (`op_id = 0`) — the controller's fire-and-forget path.
    async fn actuate_maintenance(
        &self,
        part_id: u64,
        op: u8,
        extent_ids: Vec<u64>,
        state: &autumn_common::MetadataState,
    ) -> Result<()> {
        let resp = self
            .send_maintenance(
                autumn_rpc::partition_rpc::MaintenanceReq {
                    part_id,
                    op,
                    extent_ids,
                    gc_ratio: None,
                    gc_max_size: None,
                    gc_stream_debt: None,
                    gc_empty_only: false,
                    op_id: 0,
                },
                state,
            )
            .await?;
        if resp.code != autumn_rpc::partition_rpc::CODE_OK {
            anyhow::bail!("maintenance code {}: {}", resp.code, resp.message);
        }
        Ok(())
    }

    /// Actuate ONE advisory candidate IN-PROCESS (no autumn-op subprocess).
    /// split → auto_dispatch_split; merge → the freeze-drain handler (NOT
    /// the raw flush path — avoids the ~5% loss window); gc/compact → PS
    /// MSG_MAINTENANCE; ec → handle_force_ec_convert. Every underlying op is
    /// already crash-safe + idempotent-on-retry (leader fence / inflight ledger /
    /// freeze-drain), so a refusal is logged + retried next tick.
    async fn actuate_candidate(
        &self,
        cand: &PolicyCandidate,
        state: &autumn_common::MetadataState,
    ) -> Result<()> {
        match cand.kind {
            POLICY_KIND_SPLIT => self.auto_dispatch_split(cand, None, state).await,
            POLICY_KIND_MERGE => {
                let req = MergePartitionsReq {
                    survivor_part_id: cand.primary_part_id,
                    victim_part_id: cand.secondary_part_id,
                    // NEVER force from the automatic path: the controller is
                    // precisely the actor that must not silently erase an
                    // operator-declared presplit boundary (the declared-geometry
                    // merge guard). `merge_candidates` already skips these, so this
                    // is the belt to that suspenders.
                    force: false,
                };
                let resp_bytes = self
                    .handle_merge_partitions(rkyv_encode(&req))
                    .await
                    .map_err(|(_, m)| anyhow::anyhow!("{m}"))?;
                let resp: MergePartitionsResp =
                    rkyv_decode(&resp_bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
                if resp.code != CODE_OK {
                    anyhow::bail!("merge code {}: {}", resp.code, resp.message);
                }
                Ok(())
            }
            POLICY_KIND_GC => {
                self.actuate_maintenance(
                    cand.primary_part_id,
                    autumn_rpc::partition_rpc::MAINTENANCE_AUTO_GC,
                    vec![],
                    state,
                )
                .await
            }
            POLICY_KIND_MAJOR_COMPACT | POLICY_KIND_MINOR_COMPACT => {
                self.actuate_maintenance(
                    cand.primary_part_id,
                    autumn_rpc::partition_rpc::MAINTENANCE_COMPACT,
                    vec![],
                    state,
                )
                .await
            }
            POLICY_KIND_EC => {
                let req = ForceEcConvertReq {
                    extent_id: cand.secondary_part_id,
                };
                let resp_bytes = self
                    .handle_force_ec_convert(rkyv_encode(&req))
                    .await
                    .map_err(|(_, m)| anyhow::anyhow!("{m}"))?;
                let resp: ForceEcConvertResp =
                    rkyv_decode(&resp_bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
                if resp.code != CODE_OK {
                    anyhow::bail!("ec code {}: {}", resp.code, resp.message);
                }
                Ok(())
            }
            POLICY_KIND_REBALANCE => {
                // Phase B: move a BOUNDED batch per tick so a
                // concentrated cluster converges gradually (the target PSes take
                // a reopen storm otherwise). The advisory's own cooldown paces
                // re-emission; this cap paces each actuation.
                let max_moves = self.policy.borrow().config.rebalance_max_moves_per_tick;
                let req = RebalanceRegionsReq { max_moves };
                let resp_bytes = self
                    .handle_rebalance_regions(rkyv_encode(&req))
                    .await
                    .map_err(|(_, m)| anyhow::anyhow!("{m}"))?;
                let resp: RebalanceRegionsResp =
                    rkyv_decode(&resp_bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
                if resp.code != CODE_OK {
                    anyhow::bail!("rebalance code {}: {}", resp.code, resp.message);
                }
                Ok(())
            }
            _ => anyhow::bail!("candidate kind {} not actionable", cand.kind),
        }
    }

    /// Wall-clock as `(epoch_seconds, epoch_millis)`.
    fn now_s_ms() -> (i64, i64) {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        (d.as_secs() as i64, d.as_millis() as i64)
    }

    /// Seed the op-ledger with the in-flight EC conversions AND extent
    /// recoveries from the (just-replayed) etcd markers, so `ops list` shows
    /// them as RUNNING after a leader change. Unlike compact/gc (PS-local,
    /// unknowable post-failover → UNKNOWN), both are DURABLE — the marker
    /// survived and this leader keeps working the task — so they belong in the
    /// ledger, closing normally via `complete_ec` / `complete_recovery`. The
    /// original op_ids died with the previous leader; fresh "replay" ids carry
    /// the still-running work. Called on promotion; idempotent.
    pub(crate) fn seed_ec_ledger_from_inflight(&self) {
        let (ec_inflight, recovery_inflight) = self.inflight_snapshot_ec_recovery();
        if ec_inflight.is_empty() && recovery_inflight.is_empty() {
            return;
        }
        let (now_s, now_ms) = Self::now_s_ms();
        let mut led = self.ops.borrow_mut();
        led.seed_replay(OP_KIND_EC_CONVERT, ec_inflight, now_s, now_ms);
        led.seed_replay(OP_KIND_RECOVERY, recovery_inflight, now_s, now_ms);
    }

    /// Background one-shot for a submitted op: mark RUNNING, actuate (reusing the
    /// controller's dispatch), then record the terminal outcome + a durable audit
    /// entry. A panic in the actuation records FAILED (compio catches the panic
    /// itself, but without this the entry would sit RUNNING until the TTL sweep).
    async fn run_submitted_op(&self, op_id: u64, spec: OpSubmitReq) {
        let (now_s, _) = Self::now_s_ms();
        self.ops.borrow_mut().set_running(op_id, now_s);
        let state = (*self.store.inner.borrow()).clone();
        let outcome = match futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
            self.actuate_submitted_op(op_id, &spec, &state),
        ))
        .await
        {
            Ok(r) => r,
            Err(_) => ActuationResult::Terminal {
                state: OP_STATE_FAILED,
                error: "actuation task panicked".to_string(),
                message: String::new(),
            },
        };
        let (now_s, _) = Self::now_s_ms();
        match outcome {
            ActuationResult::Terminal {
                state,
                error,
                message,
            } => {
                self.ops
                    .borrow_mut()
                    .finish(op_id, state, error.clone(), message.clone(), now_s);
                let by = if spec.requested_by.is_empty() {
                    "cli".to_string()
                } else {
                    spec.requested_by.clone()
                };
                // Forensics: node_id carries the primary target (part id, or 0 for
                // rebalance); extent_id the secondary (ec/forcegc). The op code
                // disambiguates. Best-effort — never fails the op.
                self.append_audit(MgrAuditEntry {
                    op: op_kind_audit_code(spec.kind),
                    node_id: spec.part_id,
                    extent_id: spec.secondary_id.max(spec.extent_ids.first().copied().unwrap_or(0)),
                    by,
                    reason: String::new(),
                    result_code: if state == OP_STATE_SUCCEEDED { 0 } else { 1 },
                    result_message: if error.is_empty() { message } else { error },
                    ts_ns: 0,
                })
                .await;
            }
            ActuationResult::Dispatched { message } => {
                // Stays RUNNING: compact/gc/forcegc close via the PS heartbeat,
                // ec-convert via apply_ec_conversion_done. Surface the advisory
                // (e.g. forcegc replay-floor preview) in `ops status` immediately.
                if !message.is_empty() {
                    self.ops.borrow_mut().set_message(op_id, message);
                }
            }
        }
    }

    /// Dispatch ONE submitted op through the controller's actuation building
    /// blocks and report how the ledger should transition.
    async fn actuate_submitted_op(
        &self,
        op_id: u64,
        spec: &OpSubmitReq,
        state: &autumn_common::MetadataState,
    ) -> ActuationResult {
        let terminal_err = |e: String| ActuationResult::Terminal {
            state: OP_STATE_FAILED,
            error: e,
            message: String::new(),
        };
        match spec.kind {
            OP_KIND_SPLIT => {
                let cand = PolicyCandidate {
                    kind: POLICY_KIND_SPLIT,
                    primary_part_id: spec.part_id,
                    secondary_part_id: 0,
                    reason: "manual".to_string(),
                    size_bytes: 0,
                    req_per_sec: 0,
                    imm_full_per_sec: 0,
                    same_ps: false,
                    last_op_at: 0,
                };
                match self
                    .auto_dispatch_split(&cand, spec.at_key.clone(), state)
                    .await
                {
                    Ok(()) => ActuationResult::Terminal {
                        state: OP_STATE_SUCCEEDED,
                        error: String::new(),
                        message: format!("split part {} dispatched", spec.part_id),
                    },
                    Err(e) => terminal_err(format!("{e:#}")),
                }
            }
            OP_KIND_MERGE => {
                let req = MergePartitionsReq {
                    survivor_part_id: spec.part_id,
                    victim_part_id: spec.secondary_id,
                    force: spec.force,
                };
                match self.handle_merge_partitions(rkyv_encode(&req)).await {
                    Ok(bytes) => match rkyv_decode::<MergePartitionsResp>(&bytes) {
                        Ok(resp) if resp.code == CODE_OK => ActuationResult::Terminal {
                            state: OP_STATE_SUCCEEDED,
                            error: String::new(),
                            message: format!(
                                "merged part {} into {}",
                                spec.secondary_id, spec.part_id
                            ),
                        },
                        Ok(resp) => terminal_err(resp.message),
                        Err(e) => terminal_err(format!("decode merge resp: {e}")),
                    },
                    Err((_, m)) => terminal_err(m),
                }
            }
            OP_KIND_REBALANCE => {
                let max_moves = if spec.max_moves != 0 {
                    spec.max_moves
                } else {
                    self.policy.borrow().config.rebalance_max_moves_per_tick
                };
                let req = RebalanceRegionsReq { max_moves };
                match self.handle_rebalance_regions(rkyv_encode(&req)).await {
                    Ok(bytes) => match rkyv_decode::<RebalanceRegionsResp>(&bytes) {
                        Ok(resp) if resp.code == CODE_OK => ActuationResult::Terminal {
                            state: OP_STATE_SUCCEEDED,
                            error: String::new(),
                            message: format!("moved {} partition(s)", resp.moved),
                        },
                        Ok(resp) => terminal_err(resp.message),
                        Err(e) => terminal_err(format!("decode rebalance resp: {e}")),
                    },
                    Err((_, m)) => terminal_err(m),
                }
            }
            OP_KIND_EC_CONVERT => {
                let req = ForceEcConvertReq {
                    extent_id: spec.secondary_id,
                };
                match self.handle_force_ec_convert(rkyv_encode(&req)).await {
                    Ok(bytes) => match rkyv_decode::<ForceEcConvertResp>(&bytes) {
                        Ok(resp) if resp.code == CODE_OK => ActuationResult::Dispatched {
                            message: if resp.message.is_empty() {
                                "ec conversion started".to_string()
                            } else {
                                resp.message
                            },
                        },
                        Ok(resp) => terminal_err(resp.message),
                        Err(e) => terminal_err(format!("decode ec resp: {e}")),
                    },
                    Err((_, m)) => terminal_err(m),
                }
            }
            OP_KIND_COMPACT | OP_KIND_GC | OP_KIND_FORCE_GC => {
                use autumn_rpc::partition_rpc::{
                    MAINTENANCE_AUTO_GC, MAINTENANCE_COMPACT, MAINTENANCE_FORCE_GC,
                };
                let (op, extent_ids) = match spec.kind {
                    OP_KIND_COMPACT => (MAINTENANCE_COMPACT, vec![]),
                    OP_KIND_GC => (MAINTENANCE_AUTO_GC, vec![]),
                    _ => (MAINTENANCE_FORCE_GC, spec.extent_ids.clone()),
                };
                let req = autumn_rpc::partition_rpc::MaintenanceReq {
                    part_id: spec.part_id,
                    op,
                    extent_ids,
                    gc_ratio: spec.gc_ratio,
                    gc_max_size: spec.gc_max_size,
                    gc_stream_debt: spec.gc_stream_debt,
                    gc_empty_only: spec.gc_empty_only,
                    op_id,
                };
                match self.send_maintenance(req, state).await {
                    Ok(resp) if resp.code == autumn_rpc::partition_rpc::CODE_OK => {
                        ActuationResult::Dispatched {
                            message: resp.message,
                        }
                    }
                    Ok(resp) => {
                        terminal_err(format!("maintenance code {}: {}", resp.code, resp.message))
                    }
                    Err(e) => terminal_err(format!("{e:#}")),
                }
            }
            other => terminal_err(format!("unknown op kind {other}")),
        }
    }

    /// The leader-fenced auto-policy controller tick loop.
    ///
    /// **INVARIANT (leader-only):** every tick begins with `leader.get()` — no
    /// candidate read, no decision, no actuation on a follower. DEFAULT-OFF: an
    /// `Off` mode (fresh cluster) does nothing, preserving pure-mechanism.
    /// `Armed` actuates; `DryRun` logs "would: …" but never mutates — the mode
    /// is the only gate (per-policy, no process-wide flag). Registered under
    /// `spawn_supervised`. Replaces the retired Python `AutoPolicy` loop,
    /// hosted on the crash-safe leader instead of a killable webserver.
    async fn auto_policy_tick_loop(self) {
        loop {
            compio::time::sleep(Duration::from_secs(1)).await;
            if !self.leader.get() {
                continue;
            }
            // Resolve the active policy under a short borrow.
            let (mode, interval, cooldown, max_actions, enabled) = {
                let st = self.auto_policy.borrow();
                if st.mode == crate::auto_policy::AutoPolicyMode::Off {
                    continue;
                }
                let Some(pol) = st.find_policy(&st.active) else {
                    continue; // no active policy selected
                };
                let kinds = crate::auto_policy::kinds_from_switches(&pol.switches);
                if kinds.is_empty() {
                    continue;
                }
                (
                    st.mode,
                    // Saturating: never let a huge value wrap i64 negative and
                    // bypass the cadence gates (coco P1; sanitize also clamps).
                    pol.interval_sec.min(crate::auto_policy::MAX_INTERVAL_SEC) as i64,
                    pol.cooldown_sec.min(crate::auto_policy::MAX_COOLDOWN_SEC) as i64,
                    pol.max_actions as usize,
                    kinds,
                )
            };
            let now = Self::epoch_seconds();
            // Re-decide only every `interval_sec` (the 1 s tick is just cadence).
            let cooling = { now - self.auto_policy.borrow().last_tick_at < interval };
            if cooling {
                continue;
            }
            self.auto_policy.borrow_mut().last_tick_at = now;

            // Actuate only when the active policy is Armed; DryRun records "would".
            // (There is no longer a process-wide mutation gate — arming is per
            // policy via `auto-policy activate --arm`.)
            let armed = mode == crate::auto_policy::AutoPolicyMode::Armed;

            let candidates = self.policy.borrow().advisory_cache.clone();
            let cooldowns = self.auto_policy.borrow().cooldowns.clone();
            let actions = crate::auto_policy::decide_actions(
                &candidates,
                &cooldowns,
                &enabled,
                now,
                cooldown,
                max_actions,
            );
            if actions.is_empty() {
                continue;
            }
            let state_snapshot = self.store.inner.borrow().clone();
            let mut any_issued = false;
            for (cand, cmd, key) in actions {
                if !self.leader.get() {
                    break; // lost leadership mid-batch — stragglers are leader-fenced anyway
                }
                let desc = crate::auto_policy::describe_candidate(&cand);
                let cmd_str = cmd.join(" ");
                if !armed {
                    self.auto_policy.borrow_mut().record(
                        now,
                        "would",
                        format!("would: autumn-op {cmd_str} ({desc})"),
                    );
                    continue;
                }
                match self.actuate_candidate(&cand, &state_snapshot).await {
                    Ok(()) => {
                        {
                            let mut st = self.auto_policy.borrow_mut();
                            st.cooldowns.insert(key, now);
                            st.record(now, "issued", format!("autumn-op {cmd_str} ({desc})"));
                        }
                        any_issued = true;
                        tracing::info!("auto-policy issued: autumn-op {cmd_str}");
                    }
                    Err(e) => {
                        self.auto_policy.borrow_mut().record(
                            now,
                            "refused",
                            format!("autumn-op {cmd_str}: {e}"),
                        );
                    }
                }
            }
            // Best-effort persist cooldowns once per tick (not per action).
            if any_issued {
                if let Err(e) = self.autopolicy_persist_cooldowns().await {
                    tracing::warn!(error = %e, "auto-policy: cooldown persist failed");
                }
            }
        }
    }

    /// auto-dispatch SPLIT to the owning PS for a SPLIT candidate.
    /// The PS handler (`handle_split_part`) already implements the full
    /// dual-gate + auth-rg flow; we just send the RPC.
    pub async fn auto_dispatch_split(
        &self,
        cand: &PolicyCandidate,
        // Explicit split point (raw key bytes) from a manual `ops` submit. `Some`
        // overrides the declared-boundary snap and is used verbatim; `None` keeps
        // the controller's snap-to-declared-boundary-else-PS-median behavior.
        explicit_at_key: Option<Vec<u8>>,
        state: &autumn_common::MetadataState,
    ) -> Result<()> {
        // Look up the owning PS via regions + ps_nodes.
        let region = state
            .regions
            .get(&cand.primary_part_id)
            .ok_or_else(|| anyhow::anyhow!("no region for part {}", cand.primary_part_id))?;
        // Prefer per-partition address when present; fall back to PS-level.
        let ps_addr = state
            .part_addrs
            .get(&cand.primary_part_id)
            .cloned()
            .or_else(|| state.ps_nodes.get(&region.ps_id).cloned())
            .ok_or_else(|| anyhow::anyhow!("no address for part {}", cand.primary_part_id))?;
        // SNAP to an operator-declared boundary when one
        // lies inside this partition, instead of letting the PS pick a median
        // user key.
        //
        // Median selection cuts wherever the data happens to sit — for fs that
        // is in the middle of some inode's extents, i.e. INSIDE a lane. That
        // breaks the invariant a partition owns whole lanes, and since merge now
        // refuses to cross a declared boundary, an un-snapped split makes the
        // layout drift one way only: messier, never tidier. Declared boundaries
        // are where you split FIRST and never merge — the symmetric pair.
        //
        // Generic, like the merge guard: the manager still has no idea what a
        // "lane" is, so kvc hash buckets and mem agent cuts get it for free.
        // Side benefit: an operator no longer HAS to run presplit — declare the
        // points and the cluster walks itself toward that layout as it grows.
        // (Auto-split is local/reactive, so it converges on "each partition owns
        // a run of whole lanes", NOT on a perfectly even parts-divides-lanes
        // split; that evenness stays a planned, presplit-time property.)
        let at_key = explicit_at_key
            .or_else(|| self.declared_split_point_within(cand.primary_part_id));
        if let Some(k) = &at_key {
            tracing::info!(
                part_id = cand.primary_part_id,
                point = ?String::from_utf8_lossy(k),
                "split point (explicit or snapped to a declared presplit boundary)"
            );
        }
        let payload =
            autumn_rpc::partition_rpc::rkyv_encode(&autumn_rpc::partition_rpc::SplitPartReq {
                part_id: cand.primary_part_id,
                // None ⇒ the partition holds no declared boundary (already cut
                // down to one), so fall back to PS median selection — at that
                // point an intra-lane cut is exactly what's wanted.
                at_key,
            });
        // (PS slice): the controller's auto-split is a manager→PS
        // MSG_SPLIT_PART, gated by the PS — prefix the manager's admin token.
        let payload = self.admin_prefix_ps(payload);
        // 60 s — split has to flush memtable + commit_length × 3 + a
        // manager round-trip. PS-side flush can take a few seconds
        // under contention, but anything > 60 s is a real wedge worth
        // surfacing (auto-split policy will retry on the next tick).
        let resp_bytes = self
            .conn_pool
            .call_timeout(
                &ps_addr,
                autumn_rpc::partition_rpc::MSG_SPLIT_PART,
                payload,
                Duration::from_secs(60),
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let resp: autumn_rpc::partition_rpc::SplitPartResp =
            autumn_rpc::partition_rpc::rkyv_decode(&resp_bytes)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        if resp.code != autumn_rpc::partition_rpc::CODE_OK {
            anyhow::bail!("split returned code {}: {}", resp.code, resp.message);
        }
        tracing::info!(
            "auto-split part={} dispatched OK",
            cand.primary_part_id
        );
        Ok(())
    }

    /// auto-orchestrate MERGE for a same-PS adjacent cold pair.
    /// Mirrors the CLI orchestration (FLUSH both → admin owner-lock →
    /// commit_lengths → multi_modify_merge). PS-side state catches up
    /// via region_sync_loop within ~2 s.
    pub async fn auto_dispatch_merge(
        &self,
        cand: &PolicyCandidate,
        state: &autumn_common::MetadataState,
    ) -> Result<()> {
        let survivor_id = cand.primary_part_id;
        let victim_id = cand.secondary_part_id;
        // Resolve PS addresses (per-partition first).
        let resolve = |pid: u64| -> Option<String> {
            state.part_addrs.get(&pid).cloned().or_else(|| {
                state
                    .regions
                    .get(&pid)
                    .and_then(|r| state.ps_nodes.get(&r.ps_id).cloned())
            })
        };
        let s_addr = resolve(survivor_id)
            .ok_or_else(|| anyhow::anyhow!("no address for survivor {survivor_id}"))?;
        let v_addr = resolve(victim_id)
            .ok_or_else(|| anyhow::anyhow!("no address for victim {victim_id}"))?;

        // FLUSH both partitions.
        // (PS slice): merge's flush is a manager→PS MSG_MAINTENANCE;
        // capture the admin token so the closure can prefix it (the closure moves
        // `pool`, not `self`).
        let admin_tok: Option<Vec<u8>> =
            self.admin_token.borrow().as_ref().map(|t| t.as_bytes().to_vec());
        let flush = |addr: String, pid: u64| {
            let pool = self.conn_pool.clone();
            let admin_tok = admin_tok.clone();
            async move {
                let payload = autumn_rpc::partition_rpc::rkyv_encode(
                    &autumn_rpc::partition_rpc::MaintenanceReq {
                        part_id: pid,
                        op: autumn_rpc::partition_rpc::MAINTENANCE_FLUSH,
                        extent_ids: vec![],
                        // wire fields — ignored for FLUSH op.
                        gc_ratio: None,
                        gc_max_size: None,
                        gc_stream_debt: None,
                        gc_empty_only: false,
                        op_id: 0,
                    },
                );
                let payload = match &admin_tok {
                    Some(t) => autumn_rpc::manager_rpc::prefix_admin_token(t, &payload),
                    None => payload,
                };
                // 60 s — MAINTENANCE_FLUSH rotates active + drains the
                // imm queue (each imm is up to FLUSH_MEM_BYTES = 256 MiB).
                pool.call_timeout(
                    &addr,
                    autumn_rpc::partition_rpc::MSG_MAINTENANCE,
                    payload,
                    Duration::from_secs(60),
                )
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
                Ok::<(), anyhow::Error>(())
            }
        };
        flush(s_addr.clone(), survivor_id).await?;
        flush(v_addr.clone(), victim_id).await?;

        // Acquire an admin owner-lock. The manager is `self` so we call
        // through `acquire_owner_epoch` directly — same owner_epoch the
        // CLI obtains via MSG_ACQUIRE_OWNER_LOCK.
        let owner_key = format!("auto-merge:{survivor_id}:{victim_id}");
        let owner_epoch = self.acquire_owner_epoch(&owner_key).await?;

        // commit_length per stream type for both partitions.
        let s_region = state
            .regions
            .get(&survivor_id)
            .ok_or_else(|| anyhow::anyhow!("no region for survivor {survivor_id}"))?;
        let v_region = state
            .regions
            .get(&victim_id)
            .ok_or_else(|| anyhow::anyhow!("no region for victim {victim_id}"))?;
        let log_lens = [
            self.commit_length_for_stream(s_region.log_stream, &owner_key, owner_epoch)
                .await?
                .max(1),
            self.commit_length_for_stream(v_region.log_stream, &owner_key, owner_epoch)
                .await?
                .max(1),
        ];
        let row_lens = [
            self.commit_length_for_stream(s_region.row_stream, &owner_key, owner_epoch)
                .await?
                .max(1),
            self.commit_length_for_stream(v_region.row_stream, &owner_key, owner_epoch)
                .await?
                .max(1),
        ];
        let meta_lens = [
            self.commit_length_for_stream(s_region.meta_stream, &owner_key, owner_epoch)
                .await?
                .max(1),
            self.commit_length_for_stream(v_region.meta_stream, &owner_key, owner_epoch)
                .await?
                .max(1),
        ];

        // Issue the merge directly through the local handler — manager is `self`.
        let req = MultiModifyMergeReq {
            survivor_part_id: survivor_id,
            victim_part_id: victim_id,
            owner_key,
            owner_epoch,
            log_sealed_lengths: log_lens,
            row_sealed_lengths: row_lens,
            meta_sealed_lengths: meta_lens,
        };
        let resp_bytes = self
            .handle_multi_modify_merge(rkyv_encode(&req))
            .await
            .map_err(|(_, msg)| anyhow::anyhow!("{msg}"))?;
        let resp: MultiModifyMergeResp =
            rkyv_decode(&resp_bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
        if resp.code != CODE_OK {
            anyhow::bail!("merge returned code {}: {}", resp.code, resp.message);
        }
        tracing::info!(
            "auto-merge survivor={survivor_id} victim={victim_id} OK \
             (E_new={})",
            resp.new_log_tail_extent_id
        );
        Ok(())
    }

    /// helper: query commit_length for one stream by hitting the
    /// stream's tail extent's replicas via ConnPool.
    async fn commit_length_for_stream(
        &self,
        stream_id: u64,
        owner_key: &str,
        owner_epoch: i64,
    ) -> Result<u64> {
        let req = rkyv_encode(&CheckCommitLengthReq {
            stream_id,
            owner_key: owner_key.to_string(),
            owner_epoch,
        });
        let resp_bytes = self
            .handle_check_commit_length(req)
            .await
            .map_err(|(_, msg)| anyhow::anyhow!("{msg}"))?;
        let resp: CheckCommitLengthResp =
            rkyv_decode(&resp_bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
        if resp.code != CODE_OK {
            anyhow::bail!("commit_length code {}: {}", resp.code, resp.message);
        }
        Ok(resp.end as u64)
    }

    // ── Leader election ────────────────────────────────────────────────

    async fn leader_election_loop(self) {
        const RETRY: Duration = Duration::from_secs(2);
        loop {
            if self.leader.get() {
                compio::time::sleep(RETRY).await;
                continue;
            }
            match self.try_become_leader().await {
                Ok(true) => continue,
                Ok(false) => {
                    // CAS failed — another leader holds the key.
                    // Watch for deletion instead of blind polling.
                    if let Some(etcd) = &self.etcd {
                        let addr = etcd.client.current_endpoint();
                        tracing::info!("watching leader key for deletion");
                        match autumn_etcd::watch_key_until_delete(&addr, LEADER_KEY.as_bytes())
                            .await
                        {
                            Ok(()) => {
                                tracing::info!("leader key deleted, retrying election");
                                continue;
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "watch leader key failed");
                                compio::time::sleep(RETRY).await;
                            }
                        }
                    } else {
                        compio::time::sleep(RETRY).await;
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "leader election error");
                    compio::time::sleep(RETRY).await;
                }
            }
        }
    }

    async fn try_become_leader(&self) -> Result<bool> {
        const LEASE_TTL_SECS: i64 = 10;
        let etcd = match &self.etcd {
            Some(v) => v,
            None => return Ok(false),
        };

        let lease = {
            let c = etcd.client.clone();
            c.lease_grant(LEASE_TTL_SECS).await?
        };
        let lease_id = lease.id;

        let cmp = autumn_etcd::Cmp::create_revision(LEADER_KEY.as_bytes(), 0);
        let put = autumn_etcd::Op::put_with_lease(
            LEADER_KEY.as_bytes(),
            self.instance_id.as_bytes(),
            lease_id,
        );
        let txn = autumn_etcd::proto::TxnRequest {
            compare: vec![cmp],
            success: vec![put],
            failure: vec![],
        };
        let resp = {
            let c = etcd.client.clone();
            c.txn(txn).await?
        };
        if !resp.succeeded {
            // etcd-chaos D1: the CAS failing proves etcd is ALIVE and the
            // key exists — read the holder. A DIFFERENT instance holding
            // leadership means our state can go stale under us: mark
            // displaced (gates the stale-while-leaderless serving). Our
            // OWN id (stale key from a spurious step-down, lease still
            // live) is not displacement.
            let holder = {
                let c = etcd.client.clone();
                c.get(LEADER_KEY.as_bytes()).await
            };
            if let Ok(got) = holder {
                if let Some(kv) = got.kvs.first() {
                    if kv.value.as_slice() != self.instance_id.as_bytes() {
                        self.displaced.set(true);
                    }
                }
            }
            return Ok(false);
        }

        // replay_from_etcd BEFORE set_leader(true). Previously,
        // set_leader(true) ran first; during the (typically short) replay
        // window any concurrent mutating RPC saw leader=true but the
        // in-memory store was still empty / being repopulated, and could
        // compute mutations against a stale base, durably mirroring them
        // to etcd via the leader fence (which only checks instance_id, not
        // "post-replay"). Replay then re-overwrote in-memory with the
        // (now-corrupted) etcd state.
        //
        // After the reordering: ensure_leader() (= self.leader.get()) returns
        // false during replay; mutating handlers reject with
        // CODE_NOT_LEADER; client retries land after replay completes and
        // the handler runs with a fully-rebuilt store.
        //
        // Lease TTL is 10 s. Typical replay is sub-second; if etcd is so
        // big replay exceeds 10 s, the lease expires before set_leader
        // and the next mutating RPC's leader fence flips us back to
        // non-leader — the election loop retries. The deeper fix (start
        // keepalive between CAS and replay so the lease stays alive
        // through arbitrarily long replays) is filed as a P3 follow-up
        // — it needs a stop-signal to revoke the lease on replay error.
        self.replay_from_etcd().await?;
        self.set_leader(true);
        self.displaced.set(false);
        self.leaderless_since.set(None);

        // Seed the op-ledger with the in-flight EC conversions just replayed from
        // etcd (see the method doc).
        self.seed_ec_ledger_from_inflight();

        // ensure the cluster identity is imprinted in etcd. The
        // CAS uses create_revision==0, so only the first leader ever to
        // run against a fresh etcd actually writes; subsequent leaders
        // re-CAS, observe `succeeded == false`, and read the existing
        // value. `replay_from_etcd` already loaded any prior value, so
        // this path also handles "I'm the first leader on a never-bootstrapped
        // etcd". Best-effort: a failure here logs WARN and leaves the
        // per-process UUID as the cluster_id; the next election retry
        // will try again. Wire through `txn_fenced` so the write
        // inherits the leader-fence guarantee.
        if let Err(err) = self.imprint_cluster_id().await {
            tracing::warn!(error = %err, "imprint_cluster_id failed");
        }

        // R1: ensure cluster_version exists in etcd (same CAS-imprint
        // pattern as cluster_id above). Best-effort is SAFE here (coco
        // P2 considered): (a) no code gates on cluster_version yet (R1
        // is plumbing; first consumer arrives with the first V2 form),
        // (b) a bump against a missing key CAS-fails → refused, so the
        // gate can never advance unpersisted, (c) every election retry
        // re-imprints. Revisit fail-closed when the first gate consumer
        // lands. NOTE: an out-of-bound persisted value does NOT take
        // this lenient path — replay_from_etcd already hard-failed on
        // it before we got here (rollback safety).
        if let Err(err) = self.imprint_cluster_version().await {
            tracing::warn!(error = %err, "R1: imprint_cluster_version failed");
        }

        // D2: CAS-preregister the built-in namespace families
        // (`fs`/`kvc`/`mem`). Same best-effort posture as imprint_cluster_id:
        // a failure logs WARN and the next election retry re-seeds. Idempotent
        // (CAS create_revision==0 — a namespace already present is left as-is,
        // preserving any owner assigned later).
        if let Err(err) = self.seed_builtin_namespaces().await {
            tracing::warn!(error = %err, "seed_builtin_namespaces failed");
        }

        let mgr = self.clone();
        compio::runtime::spawn(async move {
            mgr.leader_keepalive_loop(lease_id).await;
        })
        .detach();

        Ok(true)
    }

    /// CAS-imprint the cluster_id key in etcd. Idempotent.
    /// On first ever leader: generates a fresh UUID and CAS-writes
    /// `create_revision==0`. On subsequent leaders the CAS fails (key
    /// already exists), and we read the existing value to install it
    /// in-memory. Memory-only mode (no etcd) short-circuits with the
    /// per-process UUID seeded in `Self::new()`.
    async fn imprint_cluster_id(&self) -> Result<(), AppError> {
        let etcd = match &self.etcd {
            Some(v) => v,
            None => return Ok(()),
        };

        // If replay already populated cluster_id from etcd, we're done.
        // The `Self::new()` seed is a random UUID; distinguish "replayed
        // from etcd" from "still the new() seed" by re-reading etcd.
        let existing = {
            etcd.client
                .get(CLUSTER_ID_KEY.as_bytes())
                .await
                .map_err(|e| AppError::Internal(format!("get cluster_id: {e}")))?
        };
        if let Some(kv) = existing.kvs.first() {
            let id = str::from_utf8(&kv.value)
                .map_err(|e| AppError::Internal(format!("cluster_id utf8: {e}")))?
                .to_string();
            *self.cluster_id.borrow_mut() = id;
            return Ok(());
        }

        // Key doesn't exist — try to create it. The CAS races other
        // leaders only in pathological promotion-storm scenarios; any
        // race loser re-reads and installs the winner's value.
        let fresh = uuid::Uuid::new_v4().to_string();
        let cmp = autumn_etcd::Cmp::create_revision(CLUSTER_ID_KEY.as_bytes(), 0);
        let put = autumn_etcd::Op::put(CLUSTER_ID_KEY.as_bytes(), fresh.as_bytes());
        match etcd.txn_fenced(vec![cmp], vec![put], vec![]).await? {
            true => {
                *self.cluster_id.borrow_mut() = fresh;
                tracing::info!(
                    cluster_id = %self.cluster_id.borrow().as_str(),
                    "imprinted fresh cluster_id"
                );
                Ok(())
            }
            false => {
                // CAS lost — re-read whoever wrote first.
                let resp = {
                    etcd.client
                        .get(CLUSTER_ID_KEY.as_bytes())
                        .await
                        .map_err(|e| AppError::Internal(format!("re-get cluster_id: {e}")))?
                };
                if let Some(kv) = resp.kvs.first() {
                    let id = str::from_utf8(&kv.value)
                        .map_err(|e| AppError::Internal(format!("cluster_id utf8: {e}")))?
                        .to_string();
                    *self.cluster_id.borrow_mut() = id;
                    Ok(())
                } else {
                    Err(AppError::Internal(
                        "cluster_id CAS lost but key absent on re-read".into(),
                    ))
                }
            }
        }
    }

    /// R1: parse an etcd cluster_version value (ASCII decimal) and
    /// enforce the rollback-safety bound (coco P1): a persisted value
    /// ABOVE this binary's WIRE_VERSION_MAX means the cluster was bumped
    /// past what this binary speaks — i.e. an old binary was rolled back
    /// AFTER the bump, exactly the "bump 后不可滚回" rule from design
    /// §3-R1. Fail closed: the error propagates out of replay /
    /// imprint, so this manager refuses to install the state (and a
    /// replay failure prevents it from becoming leader) instead of
    /// silently serving — or persisting — formats it cannot understand.
    /// This single helper is the only decode point (imprint, CAS-lost
    /// re-reads, replay, bump heal all route through it).
    fn parse_cluster_version(raw: &[u8]) -> Result<u32, AppError> {
        let v = str::from_utf8(raw)
            .map_err(|e| AppError::Internal(format!("cluster_version utf8: {e}")))?
            .trim()
            .parse::<u32>()
            .map_err(|e| AppError::Internal(format!("cluster_version parse: {e}")))?;
        if v > autumn_rpc::WIRE_VERSION_MAX {
            return Err(AppError::Precondition(format!(
                "persisted cluster_version {v} exceeds this binary's WIRE_VERSION_MAX={} — \
this binary is OLDER than the cluster's committed format level (rollback past a \
cluster_version bump is unsupported); deploy a binary with wire version >= {v}",
                autumn_rpc::WIRE_VERSION_MAX
            )));
        }
        Ok(v)
    }

    /// R1: CAS-imprint the cluster_version key in etcd. Same shape as
    /// `imprint_cluster_id`: first leader ever seeds it to its own
    /// `WIRE_VERSION_MAX` (a fresh cluster runs at the version it was
    /// born with — there is nothing older to be compatible with);
    /// subsequent leaders read the existing value. Memory-only mode
    /// keeps the `Self::new()` seed.
    async fn imprint_cluster_version(&self) -> Result<(), AppError> {
        let etcd = match &self.etcd {
            Some(v) => v,
            None => return Ok(()),
        };

        let existing = etcd
            .client
            .get(CLUSTER_VERSION_KEY.as_bytes())
            .await
            .map_err(|e| AppError::Internal(format!("get cluster_version: {e}")))?;
        if let Some(kv) = existing.kvs.first() {
            self.cluster_version.set(Self::parse_cluster_version(&kv.value)?);
            return Ok(());
        }

        let fresh = autumn_rpc::WIRE_VERSION_MAX;
        let cmp = autumn_etcd::Cmp::create_revision(CLUSTER_VERSION_KEY.as_bytes(), 0);
        let put = autumn_etcd::Op::put(
            CLUSTER_VERSION_KEY.as_bytes(),
            fresh.to_string().into_bytes(),
        );
        match etcd.txn_fenced(vec![cmp], vec![put], vec![]).await? {
            true => {
                self.cluster_version.set(fresh);
                tracing::info!(cluster_version = fresh, "R1: imprinted fresh cluster_version");
                Ok(())
            }
            false => {
                let resp = etcd
                    .client
                    .get(CLUSTER_VERSION_KEY.as_bytes())
                    .await
                    .map_err(|e| AppError::Internal(format!("re-get cluster_version: {e}")))?;
                if let Some(kv) = resp.kvs.first() {
                    self.cluster_version.set(Self::parse_cluster_version(&kv.value)?);
                    Ok(())
                } else {
                    Err(AppError::Internal(
                        "cluster_version CAS lost but key absent on re-read".into(),
                    ))
                }
            }
        }
    }

    /// D2: CAS-preregister the built-in namespace families
    /// (`fs`/`kvc`/`mem`). Runs on every leader promotion (after replay), same
    /// idempotent best-effort shape as `imprint_cluster_id`: a family already in
    /// the registry (loaded by replay or seeded by a prior leader) is left
    /// untouched so an owner assigned later survives. Memory-only mode inserts
    /// directly into the in-mem shadow.
    async fn seed_builtin_namespaces(&self) -> Result<(), AppError> {
        for name in BUILTIN_NAMESPACES {
            // Already present (replay / prior seed / a create) → nothing to do.
            if self.namespaces.borrow().contains_key(name) {
                continue;
            }
            let row = MgrNamespace {
                name: name.to_string(),
                prefix: format!("{name}/").into_bytes(),
                // Existence-only until an owner is explicitly assigned.
                owner_tenant: None,
                presplit: Vec::new(),
                created_at: 0,
            };
            let etcd = match &self.etcd {
                // Memory-only mode: no etcd — just populate the shadow.
                None => {
                    self.namespaces
                        .borrow_mut()
                        .insert(name.to_string(), row);
                    continue;
                }
                Some(v) => v,
            };
            let key = format!("{NAMESPACE_PREFIX}{name}");
            // CAS-create (create_revision==0) so a promotion storm can't
            // double-write; a race loser re-reads and installs the winner's row.
            let cmp = autumn_etcd::Cmp::create_revision(key.as_bytes(), 0);
            let put = autumn_etcd::Op::put(key.as_bytes(), rkyv_encode(&row).as_ref());
            match etcd.txn_fenced(vec![cmp], vec![put], vec![]).await? {
                true => {
                    self.namespaces
                        .borrow_mut()
                        .insert(name.to_string(), row);
                    tracing::info!(namespace = name, "preregistered built-in namespace");
                }
                false => {
                    // CAS lost — re-read whoever wrote first and install it.
                    let resp = etcd
                        .client
                        .get(key.as_bytes())
                        .await
                        .map_err(|e| AppError::Internal(format!("re-get namespace/{name}: {e}")))?;
                    if let Some(kv) = resp.kvs.first() {
                        let existing: MgrNamespace = rkyv_decode(&kv.value).map_err(|e| {
                            AppError::Internal(format!("decode namespace/{name}: {e}"))
                        })?;
                        self.namespaces
                            .borrow_mut()
                            .insert(name.to_string(), existing);
                    }
                }
            }
        }
        Ok(())
    }

    /// R1: validate + persist a cluster_version bump. Refusal reasons are
    /// returned as `Precondition` with an operator-actionable message.
    /// The etcd write is a value-CAS against the CURRENT version so two
    /// racing bumps can't both land (the loser sees the txn fail and
    /// re-reads).
    pub(crate) async fn bump_cluster_version(&self, to: u32) -> Result<u32, AppError> {
        self.ensure_leader()?;
        let cur = self.cluster_version.get();
        if to != cur + 1 {
            return Err(AppError::Precondition(format!(
                "cluster_version bump must be exactly current+1: current={cur}, requested={to}"
            )));
        }
        if to > autumn_rpc::WIRE_VERSION_MAX {
            return Err(AppError::Precondition(format!(
                "cluster_version {to} exceeds this manager's WIRE_VERSION_MAX={} — upgrade \
the manager binaries first (design §6: bump comes AFTER all members run the new binary)",
                autumn_rpc::WIRE_VERSION_MAX
            )));
        }
        if let Some(etcd) = &self.etcd {
            let cmp = autumn_etcd::Cmp::value(
                CLUSTER_VERSION_KEY.as_bytes(),
                cur.to_string().into_bytes(),
            );
            let put = autumn_etcd::Op::put(
                CLUSTER_VERSION_KEY.as_bytes(),
                to.to_string().into_bytes(),
            );
            match etcd.txn_fenced(vec![cmp], vec![put], vec![]).await? {
                true => {}
                false => {
                    // CAS lost: another bump (or an operator etcdctl write)
                    // moved the value. Re-read so our in-memory view heals,
                    // then refuse — the caller re-runs against fresh state.
                    if let Ok(resp) = etcd.client.get(CLUSTER_VERSION_KEY.as_bytes()).await {
                        if let Some(kv) = resp.kvs.first() {
                            if let Ok(v) = Self::parse_cluster_version(&kv.value) {
                                self.cluster_version.set(v);
                            }
                        }
                    }
                    return Err(AppError::Precondition(format!(
                        "cluster_version changed concurrently (now {}); re-check and retry",
                        self.cluster_version.get()
                    )));
                }
            }
        }
        self.cluster_version.set(to);
        tracing::info!(cluster_version = to, "R1: cluster_version bumped");
        Ok(to)
    }

    async fn leader_keepalive_loop(self, lease_id: i64) {
        let keeper = {
            let c = match self.etcd.as_ref() {
                Some(v) => v.client.clone(),
                None => {
                    self.leaderless_since.set(Some(Instant::now()));
                    self.set_leader(false);
                    return;
                }
            };
            match c.lease_keep_alive(lease_id).await {
                Ok(k) => k,
                Err(_) => {
                    self.leaderless_since.set(Some(Instant::now()));
                    self.set_leader(false);
                    return;
                }
            }
        };

        loop {
            compio::time::sleep(Duration::from_secs(2)).await;
            match keeper.keep_alive().await {
                Ok(r) if r.ttl > 0 => {}
                _ => break,
            }
        }
        // etcd-chaos D1 (coco P1): open the BOUNDED stale-while-leaderless
        // window before stepping down — `ensure_routable` serves from it
        // for at most ROUTABLE_STALE_TTL.
        self.leaderless_since.set(Some(Instant::now()));
        self.set_leader(false);
    }

    // ── Etcd replay ────────────────────────────────────────────────────

    /// Wrap an etcd-value rkyv decode failure during replay with an
    /// actionable hint. The persisted metadata format is rkyv (memory
    /// layout): a decode failure here almost always means the binary's
    /// struct layout differs from what wrote the value — i.e. a
    /// schema-changing upgrade was deployed against a preserved etcd
    /// WITHOUT a migration. Production never runs `cluster.sh reset`, so
    /// the operator must deploy a matching binary or ship the one-shot
    /// migration for that release. Failing the replay (→ this manager
    /// does NOT become leader) is the SAFE outcome: rkyv's checked decode
    /// refuses loudly rather than silently mis-reading (the stop-the-world
    /// upgrade-safety guarantee). See `feedback_stopworld_restart_primary`.
    fn replay_decode_err(e: String) -> anyhow::Error {
        anyhow::anyhow!(
            "etcd metadata decode failed during replay: {e}\n\
             HINT: this binary's persisted-struct layout differs from what wrote this \
             etcd value — a schema-changing upgrade needs a matching binary or a one-shot \
             migration (production must NOT `cluster.sh reset`). Refusing to lead rather \
             than risk reading stale/garbage metadata."
        )
    }

    /// Replay helper: decode every `prefix/<id>` kv into a
    /// `HashMap<u64, T>`, folding each parsed id into `max_id`.
    /// Centralizes the parse-id → checked-rkyv-decode → max-id → insert
    /// loop that `replay_from_etcd` runs identically for nodes / disks /
    /// streams / extents / partitions. The fail-loud
    /// `replay_decode_err` mapping (note 39 / upgrade safety: a
    /// layout-mismatched persisted value must refuse leadership, never
    /// decode to garbage) now lives in this single site.
    fn replay_decode_id_map<T>(
        kvs: &[autumn_etcd::proto::KeyValue],
        prefix: &str,
        max_id: &mut u64,
    ) -> Result<HashMap<u64, T>>
    where
        T: rkyv::Archive,
        T::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    {
        let mut out = HashMap::new();
        for kv in kvs {
            let id = Self::parse_id_from_key(prefix, &kv.key)?;
            let v: T = rkyv_decode(&kv.value).map_err(Self::replay_decode_err)?;
            *max_id = (*max_id).max(id);
            out.insert(id, v);
        }
        Ok(out)
    }

    /// Replay a `node_override`-shaped etcd prefix (`MgrNodeOverride` values
    /// keyed by node id) into `out`: clear, then per key parse the id (fail-loud
    /// via `?`) and rkyv-decode the value — a malformed payload is SKIPPED with a
    /// WARN (these are per-node localizable, so a bad one is dropped, not fatal —
    /// unlike core metadata, which fails loud via `replay_decode_id_map`).
    /// `label` names the prefix in the skip warning. Shared by `node_override/`
    /// and `decommissioned/`.
    fn replay_node_override_map(
        kvs: &[autumn_etcd::proto::KeyValue],
        prefix: &str,
        label: &str,
        out: &mut HashMap<u64, MgrNodeOverride>,
    ) -> Result<()> {
        out.clear();
        for kv in kvs {
            let id = Self::parse_id_from_key(prefix, &kv.key)?;
            let ovr: MgrNodeOverride = match rkyv_decode(&kv.value) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        node_id = id,
                        error = %e,
                        "skipping malformed {} entry",
                        label
                    );
                    continue;
                }
            };
            out.insert(id, ovr);
        }
        Ok(())
    }

    async fn replay_from_etcd(&self) -> Result<()> {
        let etcd = match &self.etcd {
            Some(v) => v,
            None => return Ok(()),
        };

        let c = etcd.client.clone();

        let nodes = c.get_prefix("nodes/").await?;
        let disks = c.get_prefix("disks/").await?;
        let streams = c.get_prefix("streams/").await?;
        let extents = c.get_prefix("extents/").await?;
        let owner_locks = c.get_prefix("ownerLocks/").await?;
        let partitions = c.get_prefix("partitions/").await?;
        let ps_nodes = c.get_prefix("psNodes/").await?;
        let regions = c.get_prefix("regions/").await?;
        // per-partition last_op_at sidecar
        let last_op = c.get_prefix("partitionLastOp/").await?;
        // unified extent in-flight ledger. Authoritative source of
        // truth for stream-layer ops in flight on each extent.
        let extent_inflight_raw = c
            .get_prefix(crate::extent_inflight::EXTENT_INFLIGHT_PREFIX)
            .await?;
        // persisted retry queue for extent deletes that
        // exhausted the primary in-memory loop's budget.
        let failed_delete_raw = c
            .get_prefix(crate::extent_delete::EXTENT_DELETE_RETRY_PREFIX)
            .await?;
        // Per-extent payload location. Only non-default (`InShardFile`) entries
        // exist, so this prefix is empty on a cluster that has never converted
        // an extent under the CoW scheme.
        let extent_layout_raw = c
            .get_prefix(crate::extent_layout::EXTENT_LAYOUT_PREFIX)
            .await?;
        // Per-extent corrupt slots. A rebuild scheduled by a corrupt report
        // must survive the leader change that interrupted it, or the extent is
        // left isolated at RF-1 with the reason gone.
        let extent_corrupt_raw = c
            .get_prefix(crate::extent_corrupt::EXTENT_CORRUPT_PREFIX)
            .await?;
        // persistent operator overrides + decommissioned tombstones.
        let node_override_raw = c.get_prefix(NODE_OVERRIDE_PREFIX).await?;
        let decommissioned_raw = c.get_prefix(DECOMMISSIONED_PREFIX).await?;
        // cluster identity (single key, not a prefix).
        let cluster_id_kv = c.get(CLUSTER_ID_KEY.as_bytes()).await?;
        // R1: persisted cluster_version (single key, ASCII decimal).
        let cluster_version_kv = c.get(CLUSTER_VERSION_KEY.as_bytes()).await?;
        // Persisted writer leases.
        let inode_leases_raw = c.get_prefix(INODE_LEASES_PREFIX).await?;
        // persisted tenant account DB.
        let tenant_account_raw = c.get_prefix(TENANT_ACCOUNT_PREFIX).await?;
        // D2: persisted namespace registry.
        let namespace_raw = c.get_prefix(NAMESPACE_PREFIX).await?;
        // M2: auto-policy controller config + cooldowns (single keys)
        // so the active policy + mode survive leader failover.
        let autopolicy_config_kv = c.get(b"autoPolicy/config").await?;
        let autopolicy_cooldowns_kv = c.get(b"autoPolicy/cooldowns").await?;
        drop(c);

        let mut max_id = 0u64;
        let decoded_nodes: HashMap<u64, MgrNodeInfo> =
            Self::replay_decode_id_map(&nodes.kvs, "nodes/", &mut max_id)?;
        let decoded_disks: HashMap<u64, MgrDiskInfo> =
            Self::replay_decode_id_map(&disks.kvs, "disks/", &mut max_id)?;
        let decoded_streams: HashMap<u64, MgrStreamInfo> =
            Self::replay_decode_id_map(&streams.kvs, "streams/", &mut max_id)?;
        let decoded_extents: HashMap<u64, MgrExtentInfo> =
            Self::replay_decode_id_map(&extents.kvs, "extents/", &mut max_id)?;

        let mut decoded_owner_revs = HashMap::new();
        let mut max_revision = 0i64;
        for kv in &owner_locks.kvs {
            let raw = str::from_utf8(&kv.key)?;
            let owner_key = raw
                .strip_prefix("ownerLocks/")
                .ok_or_else(|| anyhow::anyhow!("invalid owner lock key: {raw}"))?
                .to_string();
            // epoch = mod_revision (bumped on every acquire), NOT
            // create_revision. Must match `acquire_owner_epoch` or the
            // post-failover `ensure_owner_epoch` equality check rejects
            // every live owner.
            let rev = kv.mod_revision;
            max_revision = max_revision.max(rev);
            decoded_owner_revs.insert(owner_key, rev);
        }

        let decoded_partitions: HashMap<u64, MgrPartitionMeta> =
            Self::replay_decode_id_map(&partitions.kvs, "partitions/", &mut max_id)?;

        let mut decoded_ps_nodes = HashMap::new();
        for kv in &ps_nodes.kvs {
            let id = Self::parse_id_from_key("psNodes/", &kv.key)?;
            let addr = str::from_utf8(&kv.value)?.to_string();
            decoded_ps_nodes.insert(id, addr);
        }

        let mut decoded_regions = BTreeMap::new();
        for kv in &regions.kvs {
            let id = Self::parse_id_from_key("regions/", &kv.key)?;
            let region: MgrRegionInfo =
                rkyv_decode(&kv.value).map_err(Self::replay_decode_err)?;
            decoded_regions.insert(id, region);
        }

        // parse partitionLastOp/ sidecar (i64 little-endian)
        let mut decoded_last_op: HashMap<u64, i64> = HashMap::new();
        for kv in &last_op.kvs {
            let id = Self::parse_id_from_key("partitionLastOp/", &kv.key)?;
            if kv.value.len() >= 8 {
                let ts = i64::from_le_bytes(kv.value[..8].try_into().unwrap());
                decoded_last_op.insert(id, ts);
            }
        }

        {
            let mut s = self.store.inner.borrow_mut();
            s.nodes = decoded_nodes;
            s.disks = decoded_disks;
            s.streams = decoded_streams;
            s.extents = decoded_extents;
            s.owner_epochs = decoded_owner_revs;
            s.next_revision = s.next_revision.max(max_revision);
            s.partitions = decoded_partitions;
            s.ps_nodes = decoded_ps_nodes;
            s.regions = decoded_regions;
            s.next_id = s.next_id.max(max_id.saturating_add(1));
        }
        // seed the node-state tracker with an OK heartbeat for
        // every replayed EN node so the new leader starts with all
        // nodes Online; the next `df` poll (10 s tick) will re-derive
        // the truth from RPC outcomes. Mirrors the approach
        // for PS heartbeats below.
        {
            let mut t = self.node_states.borrow_mut();
            let s = self.store.inner.borrow();
            for node_id in s.nodes.keys() {
                t.on_heartbeat_ok(*node_id);
            }
        }
        // replay persistent operator overrides (Fenced /
        // Maintenance). Overrides survive leader failover so the new
        // leader's `recovery_dispatch_loop` sees the same
        // Fenced set as the deposed leader.
        Self::replay_node_override_map(
            &node_override_raw.kvs,
            NODE_OVERRIDE_PREFIX,
            "node_override",
            &mut self.node_overrides.borrow_mut(),
        )?;
        Self::replay_node_override_map(
            &decommissioned_raw.kvs,
            DECOMMISSIONED_PREFIX,
            "decommissioned",
            &mut self.decommissioned.borrow_mut(),
        )?;
        // replay the KDC tenant account DB. String-keyed (tenant
        // name), so we can't use replay_node_override_map (u64 id); mirror the
        // ownerLocks/ inline pattern. A malformed account is fail-loud (a bad
        // authz record must not silently start the KDC half-armed — it would
        // let a tenant that should have prefixes mint with none / stale ones).
        {
            let mut accts = self.tenant_accounts.borrow_mut();
            accts.clear();
            for kv in &tenant_account_raw.kvs {
                let raw = str::from_utf8(&kv.key)
                    .map_err(|e| anyhow::anyhow!("non-utf8 tenantAccount key: {e}"))?;
                let tenant = raw
                    .strip_prefix(TENANT_ACCOUNT_PREFIX)
                    .ok_or_else(|| anyhow::anyhow!("invalid tenantAccount key: {raw}"))?
                    .to_string();
                let acct: MgrTenantAccount = rkyv_decode(&kv.value).map_err(|e| {
                    anyhow::anyhow!("malformed tenantAccount/{tenant}: {e}")
                })?;
                accts.insert(tenant, acct);
            }
        }
        // D2: replay the namespace registry. String-keyed (namespace
        // name), so mirror the tenantAccount/ inline pattern above. A malformed
        // row is fail-loud (note 39): a bad registry record must not silently
        // start the manager with a half-populated namespace set — Layer-A / the
        // authz bridge would then act on it. Fail-loud covers BOTH (a) rkyv
        // decode failure AND (b) SEMANTIC corruption — the etcd key suffix, the
        // stored `row.name`, and the stored `row.prefix` must all agree and obey
        // the namespace naming rules; otherwise `handle_get_authz_config` could
        // publish e.g. a `bench` row carrying `prefix = mem/` and wrongly
        // protect/expose another keyspace (coco P2). A leadership-refusing error
        // is the right response — the operator repairs etcd, not the manager.
        {
            let mut ns = self.namespaces.borrow_mut();
            ns.clear();
            for kv in &namespace_raw.kvs {
                let raw = str::from_utf8(&kv.key)
                    .map_err(|e| anyhow::anyhow!("non-utf8 namespace key: {e}"))?;
                let name = raw
                    .strip_prefix(NAMESPACE_PREFIX)
                    .ok_or_else(|| anyhow::anyhow!("invalid namespace key: {raw}"))?
                    .to_string();
                let row: MgrNamespace = rkyv_decode(&kv.value)
                    .map_err(|e| anyhow::anyhow!("malformed namespace/{name}: {e}"))?;
                // Semantic consistency — the stored fields must match the key and
                // the naming rules the create path enforces.
                if let Err(msg) = validate_namespace_name(&name) {
                    return Err(anyhow::anyhow!("namespace/{name} invalid name: {msg}"));
                }
                if row.name != name {
                    return Err(anyhow::anyhow!(
                        "namespace/{name} row.name mismatch ('{}' != key '{name}')",
                        row.name
                    ));
                }
                let expect_prefix = format!("{name}/").into_bytes();
                if row.prefix != expect_prefix {
                    return Err(anyhow::anyhow!(
                        "namespace/{name} row.prefix mismatch (got {:?}, expected {:?})",
                        row.prefix,
                        expect_prefix
                    ));
                }
                ns.insert(name, row);
            }
        }
        // seed `ps_last_heartbeat` with `Instant::now()` for
        // every replayed PS. Previously the map was empty post-failover,
        // and the liveness loop's `None` arm treated unknown PSes as
        // "alive" — so a PS that died right before the failover (with
        // its evicted etcd entry still lingering) was
        // resurrected on replay and stayed forever unevictable. Seeding
        // grants every replayed PS a fresh `PS_DEAD_TIMEOUT` window to
        // start heartbeating again. If it doesn't, the regular eviction
        // path fires after the window expires (now reachable because
        // `Some(t)` from the seed engages the `.elapsed() > timeout`
        // branch).
        {
            let mut hb = self.ps_last_heartbeat.borrow_mut();
            let now = Instant::now();
            let s = self.store.inner.borrow();
            for ps_id in s.ps_nodes.keys() {
                hb.entry(*ps_id).or_insert(now);
            }
        }
        // install last_op_at sidecar so policy engine cooldown
        // gating is correct on cold-start as well.
        *self.last_op_at.borrow_mut() = decoded_last_op;
        // install the unified inflight ledger. Records with
        // malformed op_kind/payload combinations are dropped with a WARN
        // inside `decode_extent_inflight_kvs`. The `extent_inflight/`
        // prefix is the single source of truth — no legacy fold-in.
        {
            let decoded =
                Self::decode_extent_inflight_kvs(extent_inflight_raw.kvs.iter().map(|kv| {
                    let id = Self::parse_id_from_key(
                        crate::extent_inflight::EXTENT_INFLIGHT_PREFIX,
                        &kv.key,
                    )
                    .unwrap_or(0);
                    (id, kv.value.as_slice())
                }));
            // Each marker's attempt nonce is the etcd revision that created it,
            // which etcd hands back as the key's `mod_revision` — so the map is
            // rebuilt from etcd itself rather than from anything we persisted.
            // A marker is written once at acquire and never rewritten, so this
            // is the same value the acquiring leader recorded.
            let revs: HashMap<u64, i64> = extent_inflight_raw
                .kvs
                .iter()
                .filter_map(|kv| {
                    let id = Self::parse_id_from_key(
                        crate::extent_inflight::EXTENT_INFLIGHT_PREFIX,
                        &kv.key,
                    )
                    .ok()?;
                    Some((id, kv.mod_revision))
                })
                .collect();
            let mut map = self.inflight.borrow_mut();
            let mut nonces = self.inflight_attempt_nonce.borrow_mut();
            map.clear();
            nonces.clear();
            for (id, rec) in decoded {
                if id != 0 {
                    map.insert(id, rec);
                    if let Some(rev) = revs.get(&id).filter(|r| **r > 0) {
                        nonces.insert(id, *rev as u64);
                    }
                }
            }
        }
        self.install_replayed_payload_locations(Self::decode_extent_layout_kvs(
            extent_layout_raw.kvs.iter().filter_map(|kv| {
                let id =
                    Self::parse_id_from_key(crate::extent_layout::EXTENT_LAYOUT_PREFIX, &kv.key)
                        .ok()?;
                Some((id, kv.value.as_slice()))
            }),
        ));
        self.install_replayed_corrupt_slots(Self::decode_extent_corrupt_kvs(
            extent_corrupt_raw.kvs.iter().filter_map(|kv| {
                let id =
                    Self::parse_id_from_key(crate::extent_corrupt::EXTENT_CORRUPT_PREFIX, &kv.key)
                        .ok()?;
                Some((id, kv.value.as_slice()))
            }),
        ));
        // rehydrate in-memory `delete_progress` from Delete-kind
        // ledger entries so the new leader's extent_delete_loop picks up
        // pending fanouts immediately. Attempts reset to 0 (correct
        // behaviour — a new leader's first attempt is its own "1").
        {
            let inflight = self.inflight.borrow();
            let mut progress = self.delete_progress.borrow_mut();
            progress.clear();
            for (id, rec) in inflight.iter() {
                if let Some((_, crate::extent_inflight::ExtentOpPayload::Delete(p))) = rec.unpack()
                {
                    progress.insert(
                        *id,
                        crate::extent_delete::PendingDelete {
                            extent_id: p.extent_id,
                            pending_targets: p.pending_targets,
                            attempts: 0,
                        },
                    );
                }
            }
        }
        // install cluster_id from etcd. The key may legitimately
        // be absent on a brand-new cluster where no leader has yet run
        // `imprint_cluster_id`; in that case `try_become_leader` will
        // write it right after replay completes. Followers see the
        // stable value on every subsequent replay.
        if let Some(kv) = cluster_id_kv.kvs.first() {
            let id = str::from_utf8(&kv.value)
                .map_err(|e| anyhow::anyhow!("cluster_id utf8: {e}"))?
                .to_string();
            *self.cluster_id.borrow_mut() = id;
        }
        // R1: install cluster_version. Absent on a pre-R1 / brand-new
        // cluster — `try_become_leader` imprints it right after replay.
        if let Some(kv) = cluster_version_kv.kvs.first() {
            self.cluster_version
                .set(Self::parse_cluster_version(&kv.value).map_err(|e| anyhow::anyhow!("{e}"))?);
        }
        // M2: rehydrate the auto-policy controller config +
        // cooldowns so the active policy + mode + custom policies survive leader
        // failover (the crash-safety win over the killable Python webserver).
        // Malformed → fail-loud (note 39) rather than silently reset the policy.
        let had_persisted_config = autopolicy_config_kv.kvs.first().is_some();
        if let Some(kv) = autopolicy_config_kv.kvs.first() {
            let cfg: MgrAutoPolicyConfig =
                rkyv_decode(&kv.value).map_err(Self::replay_decode_err)?;
            self.auto_policy.borrow_mut().load_config(cfg);
        }
        // record whether this cluster carried a
        // persisted auto-policy config, so `apply_auto_policy_default` (called
        // from the bin AFTER the flags are set) knows if it may seed. The seed
        // can't happen HERE: `new_with_etcd` runs the first replay + leader
        // election in the CONSTRUCTOR, before the bin has called
        // `set_auto_policy_default`, so the flag isn't set yet.
        self.auto_policy_had_persisted_config.set(had_persisted_config);
        if let Some(kv) = autopolicy_cooldowns_kv.kvs.first() {
            let cds: MgrAutoPolicyCooldowns =
                rkyv_decode(&kv.value).map_err(Self::replay_decode_err)?;
            self.auto_policy.borrow_mut().load_cooldowns(cds);
        }
        // rehydrate `failed_deletes` from the persisted retry
        // prefix. `attempts` + `last_attempt_at` are kept as written so
        // the new leader respects any in-flight backoff window from
        // the deposed leader's most recent attempt.
        {
            let mut map = self.failed_deletes.borrow_mut();
            map.clear();
            for kv in &failed_delete_raw.kvs {
                let id = match Self::parse_id_from_key(
                    crate::extent_delete::EXTENT_DELETE_RETRY_PREFIX,
                    &kv.key,
                ) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "replay skipped malformed extentDeleteRetry/ key"
                        );
                        continue;
                    }
                };
                let entry: crate::extent_delete::MgrExtentDeleteRetry =
                    match autumn_rpc::manager_rpc::rkyv_decode(&kv.value) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(
                                extent_id = id,
                                error = %e,
                                "replay skipped malformed extentDeleteRetry/ payload"
                            );
                            continue;
                        }
                    };
                map.insert(id, entry);
            }
        }

        // Install persisted writer leases. Reader leases are
        // NOT persisted (plan §6.4 — reader subscribe-reconnect drops every
        // cached version, so losing the reader set across failover is benign).
        //
        // FAIL-LOUD (chaos-gap round 2, coco P0): an UNDECODABLE persisted
        // writer lease MUST refuse leadership, never be silently skipped.
        // A writer lease is the single-writer safety boundary for its inode.
        // Skipping a malformed one leaves the new leader with no record of that
        // writer (and no `last_version` high-water), so it would grant a SECOND
        // writer for the same inode while the old writer's cache / dirty pages
        // / writeback are still live → double-writer corruption (interleaved
        // content, version regression, broken invalidation order). Unlike a
        // legitimately-expired record (which `install_persisted_writer` clamps
        // to a TTL and the revoke loop later deletes), a skipped malformed
        // record has NO TTL backstop. So we fail-loud exactly like core
        // metadata (`replay_decode_err`): refuse to lead rather than serve a
        // state we cannot prove is single-writer. (extent_inflight / node_override
        // get per-key quarantine instead — see chaos-gap round 2 — because they
        // can be localized; a writer lease cannot, so global fail-loud is the
        // only safe granularity until a per-inode unknown-writer quarantine
        // exists.)
        {
            // Two-phase (coco P2): parse + validate EVERY record into a temp Vec
            // BEFORE touching the registry, so a fail-loud return leaves ZERO
            // partial in-memory state. (Pre-this-fix the block used warn+continue
            // and always completed; introducing a mid-loop `?` without two-phase
            // would let an early valid record install while a later corrupt one
            // aborts — a stale writer that a subsequent merge-replay never clears.)
            let mut writers: Vec<autumn_rpc::manager_rpc::MgrInodeLeaseRecord> =
                Vec::with_capacity(inode_leases_raw.kvs.len());
            for kv in &inode_leases_raw.kvs {
                let id = Self::parse_id_from_key(INODE_LEASES_PREFIX, &kv.key).map_err(|e| {
                    Self::replay_decode_err(format!(
                        "inode_leases key {}: {e}",
                        String::from_utf8_lossy(&kv.key)
                    ))
                })?;
                let rec: autumn_rpc::manager_rpc::MgrInodeLeaseRecord =
                    autumn_rpc::manager_rpc::rkyv_decode(&kv.value).map_err(|e| {
                        Self::replay_decode_err(format!("inode_leases/{id} payload: {e}"))
                    })?;
                // The registry installs by `rec.ino`, NOT the key's id (coco P1):
                // a key/payload inode mismatch would install the writer under the
                // wrong inode, leaving the key's inode writer-less → second-writer
                // hazard. A semantically-corrupt record is as unsafe as an
                // undecodable one, so it is also fail-loud.
                if rec.ino != id {
                    return Err(Self::replay_decode_err(format!(
                        "inode_leases/{id} payload ino mismatch: rec.ino={}",
                        rec.ino
                    )));
                }
                writers.push(rec);
            }
            // All records valid — now commit to the registry.
            let mut reg = self.inode_leases.borrow_mut();
            let now = Instant::now();
            for rec in writers {
                reg.install_persisted_writer(rec, now);
            }
        }

        Ok(())
    }

    // ── Helpers ────────────────────────────────────────────────────────

    fn parse_id_from_key(prefix: &str, key: &[u8]) -> Result<u64> {
        let raw = str::from_utf8(key)?;
        let suffix = raw
            .strip_prefix(prefix)
            .ok_or_else(|| anyhow::anyhow!("invalid key prefix for {raw}"))?;
        Ok(suffix.parse::<u64>()?)
    }

    fn err_to_code(err: &AppError) -> u8 {
        match err {
            AppError::NotLeader => CODE_NOT_LEADER,
            AppError::NotFound(_) => CODE_NOT_FOUND,
            AppError::Precondition(_) => CODE_PRECONDITION,
            AppError::InvalidArgument(_) => CODE_INVALID_ARGUMENT,
            AppError::Internal(_) => CODE_ERROR,
        }
    }

    fn err_to_status(err: &AppError) -> (StatusCode, String) {
        match err {
            AppError::NotLeader => (StatusCode::Unavailable, err.to_string()),
            AppError::NotFound(_) => (StatusCode::NotFound, err.to_string()),
            AppError::Precondition(_) => (StatusCode::FailedPrecondition, err.to_string()),
            AppError::InvalidArgument(_) => (StatusCode::InvalidArgument, err.to_string()),
            AppError::Internal(_) => (StatusCode::Internal, err.to_string()),
        }
    }

    /// node_ids that must NOT receive new extent placements —
    /// operator-Fenced or -Maintenance (`node_overrides`) plus auto-Suspected
    /// (`node_states`). Capture this owned set BEFORE any `store.inner` borrow
    /// (the RefCells are disjoint), mirroring the `online_node_ids`
    /// capture pattern, then thread it into `select_nodes` + every fallback walk
    /// + recovery-target selection so a decommissioning / flaky node never gets
    /// data we'd immediately have to migrate off.
    pub(crate) fn placement_excluded_node_ids(&self) -> HashSet<u64> {
        let mut set: HashSet<u64> = self
            .node_overrides
            .borrow()
            .iter()
            .filter(|(_, o)| {
                o.kind == NODE_OVERRIDE_FENCED || o.kind == NODE_OVERRIDE_MAINTENANCE
            })
            .map(|(id, _)| *id)
            .collect();
        set.extend(self.node_states.borrow().suspected_node_ids());
        set
    }

    /// pick `count` candidate nodes for a fresh extent allocation.
    ///
    /// Prefers nodes that have **at least one online disk** so the very
    /// first AllocExtent fan-out doesn't include a peer the manager has
    /// already detected as dead (see `mark_node_disks_offline` in
    /// `recovery.rs`). Falls back to the full node set when too few
    /// nodes appear online — this keeps the existing
    /// fall-back-to-fresh-node path in `handle_stream_alloc_extent`
    /// available even in degraded states (e.g. cold leader before the
    /// first `df` poll has run).
    ///
    /// pick is **shuffled** (uniform random `count`-subset) instead
    /// of "lowest `node_id` first". The previous deterministic order
    /// concentrated load on the first `count` nodes by ID — e.g. a 4-node
    /// cluster {1,3,5,7} with 3-replica streams placed every extent on
    /// {1,3,5}, leaving node 7 idle until one of the first three failed.
    ///
    /// `exclude_node_ids` carries the writer's per-stream "recently
    /// failed" set (30 s TTL on the client). Filter the candidate pool by
    /// this set BEFORE the online-disk filter; if the result is too small
    /// to satisfy `count`, drop the exclusion and retry — never block
    /// allocation on a stale exclude.
    ///
    /// `online_node_ids` is the set of nodes whose
    /// `NodeStateTracker` state is `Online` (i.e. registered AND verified
    /// alive via at least one successful df). Suspend / Suspected nodes
    /// are excluded at the primary filter; the cold-leader fallback
    /// still applies — when too few `Online` nodes exist (e.g. the
    /// manager has just won leader election and hasn't run its first
    /// df sweep), the pool widens to honour the existing
    /// "fall-back-to-fresh-node" path in `handle_stream_alloc_extent`.
    ///
    /// `hard_excluded` (Fenced / Maintenance / Suspected, from
    /// `placement_excluded_node_ids`) is removed from EVERY placement path,
    /// INCLUDING the cold-leader degraded fallback below — unlike the
    /// `exclude_node_ids` soft hint (which is backfilled when it under-fills),
    /// a hard-excluded node must never receive a new extent even if that means
    /// allocation fails loudly (the caller's per-RPC fallback / client retry
    /// surfaces it) rather than placing data we'd immediately migrate off.
    fn select_nodes(
        nodes: &HashMap<u64, MgrNodeInfo>,
        disks: &HashMap<u64, MgrDiskInfo>,
        online_node_ids: &HashSet<u64>,
        space_low_node_ids: &HashSet<u64>,
        hard_excluded: &HashSet<u64>,
        count: usize,
        exclude_node_ids: &[u64],
    ) -> Result<Vec<MgrNodeInfo>, AppError> {
        use rand::seq::SliceRandom;
        // Hard-exclude up front so the count precheck AND the degraded
        // `pool = all` fallback both inherit it (all downstream pools derive
        // from `all_unfiltered`).
        let all_unfiltered: Vec<MgrNodeInfo> = nodes
            .values()
            .filter(|n| !hard_excluded.contains(&n.node_id))
            .cloned()
            .collect();
        if all_unfiltered.len() < count {
            return Err(AppError::Precondition(format!(
                "not enough nodes: need {count}, got {} (after excluding {} fenced/maintenance/suspected)",
                all_unfiltered.len(),
                hard_excluded.len()
            )));
        }
        let exclude_set: HashSet<u64> = exclude_node_ids.iter().copied().collect();
        let after_exclude: Vec<MgrNodeInfo> = all_unfiltered
            .iter()
            .filter(|n| !exclude_set.contains(&n.node_id))
            .cloned()
            .collect();
        // only honor the exclude set if at least `count` non-excluded
        // nodes remain — otherwise stale excludes would block allocation.
        let all = if after_exclude.len() >= count {
            after_exclude
        } else {
            all_unfiltered
        };
        // prefer nodes that are BOTH verified-Online AND
        // have at least one online disk. The two filters layer naturally
        // — the state filter is the new gate, the disk filter is the
        // existing post-df health signal.
        let healthy: Vec<MgrNodeInfo> = all
            .iter()
            .filter(|n| online_node_ids.contains(&n.node_id))
            .filter(|n| {
                n.disks
                    .iter()
                    .any(|d| disks.get(d).map(|di| di.online).unwrap_or(false))
            })
            .cloned()
            .collect();
        let mut rng = rand::thread_rng();
        // ENOSPC-1: among healthy nodes, prefer those NOT known to be low
        // on space (per the df probe's max per-disk free vs
        // `min_alloc_free_bytes`). Soft preference, never a hard gate:
        // when too few spacious nodes remain, fall back to the full
        // healthy set — a capacity-crunched cluster should still attempt
        // allocation (the EN-side Full gate fails fast and the per-RPC
        // fallback walk takes over) rather than refuse outright.
        let spacious: Vec<MgrNodeInfo> = healthy
            .iter()
            .filter(|n| !space_low_node_ids.contains(&n.node_id))
            .cloned()
            .collect();
        if spacious.len() >= count {
            let mut pool = spacious;
            pool.shuffle(&mut rng);
            return Ok(pool.into_iter().take(count).collect());
        }
        if healthy.len() >= count {
            let mut pool = healthy;
            pool.shuffle(&mut rng);
            return Ok(pool.into_iter().take(count).collect());
        }
        // Degraded fallback: not enough verified-online nodes with
        // online disks. Preserve the legacy fallback —
        // widen to the full node set so the post-RPC fall-back path
        // in `handle_stream_alloc_extent` can still recover (it pings
        // the candidate per-RPC and walks alternates on failure). Cold-
        // leader case (no df sweep yet → online_node_ids empty) is
        // covered here.
        let mut pool = all;
        pool.shuffle(&mut rng);
        Ok(pool.into_iter().take(count).collect())
    }

    fn all_bits(size: usize) -> u32 {
        if size >= 32 {
            u32::MAX
        } else {
            (1u32 << size) - 1
        }
    }

    fn ensure_owner_epoch(
        owner_key: &str,
        owner_epoch: i64,
        state: &autumn_common::MetadataState,
    ) -> Result<(), AppError> {
        if owner_key.is_empty() {
            return Ok(());
        }
        state.ensure_owner_epoch(owner_key, owner_epoch)
    }

    async fn acquire_owner_epoch(&self, owner_key: &str) -> Result<i64, AppError> {
        if owner_key.is_empty() {
            return Ok(0);
        }

        if let Some(etcd) = &self.etcd {
            let key = format!("ownerLocks/{owner_key}");
            // the epoch BUMPS on every acquire. Previously this was a
            // create_revision==0 CAS that reused the key's stable
            // create_revision forever — so an owner_key's epoch NEVER rose
            // again after first creation. Consequences (both observed in
            // the transport chaos run): (a) failback wedge — once a
            // later-created owner (higher revision) touched an extent, the
            // EN floor stayed above the earlier owner's frozen epoch and
            // ownership could never transfer BACK (PS1 → PS2 → PS1 left
            // every probe rejected with CODE_LOCKED_BY_OTHER); (b) two live
            // processes acquiring the same owner_key SHARED one epoch — no
            // mutual fencing at all (split-brain). An unconditional
            // leader-fenced PUT makes every acquire rewrite the key; the
            // resulting mod_revision is a fresh GLOBAL etcd revision, so it
            // is monotonic across acquires AND across different owner_keys
            // — exactly what the EN's `header.owner_epoch < floor` check
            // and the manager's exact-equality `ensure_owner_epoch` need.
            // The epoch is the COMMIT REVISION of this very txn (==
            // the PUT's mod_revision), read atomically from the txn
            // response. A separate post-commit GET would race a
            // concurrent same-key acquire (both observing the later
            // mod_revision and sharing one epoch — coco P1).
            let put_op = autumn_etcd::Op::put(key.as_bytes(), self.instance_id.as_bytes());
            let rev = etcd.txn_fenced_put_revision(vec![put_op]).await?;

            let mut s = self.store.inner.borrow_mut();
            s.owner_epochs.insert(owner_key.to_string(), rev);
            s.next_revision = s.next_revision.max(rev);
            return Ok(rev);
        }

        let mut s = self.store.inner.borrow_mut();
        Ok(s.acquire_owner_lock(owner_key))
    }

    /// called by `serve()` once the RPC listener is BOUND. Restarts
    /// every PS's liveness clock (heartbeats physically could not arrive
    /// earlier) and unblocks the eviction sweep. See the `serving` field
    /// doc for the ucx TIME_WAIT motivation.
    /// ENOSPC-1: override the allocation free-space floor (CLI
    /// `--min-alloc-free-bytes`; 0 disables the filter).
    pub fn set_min_alloc_free_bytes(&self, v: u64) {
        self.min_alloc_free_bytes.set(v);
    }

    /// Audit-log retention window (CLI `--audit-retention-days`; 0 = off).
    pub fn set_audit_retention_days(&self, v: u64) {
        self.audit_retention_days.set(v);
    }

    /// Daily audit-log retention GC (leader-only; etcd mode only). The
    /// first pass runs ~10 min after start so leader election + replay
    /// settle; the daily cadence is far above the helper's cost (one
    /// prefix read + batched deletes).
    async fn audit_gc_loop(self) {
        compio::time::sleep(Duration::from_secs(600)).await;
        loop {
            if self.leader.get() {
                self.audit_retention_gc().await;
            }
            compio::time::sleep(Duration::from_secs(86_400)).await;
        }
    }

    /// ENOSPC-1: nodes whose latest df probe showed max per-disk free
    /// BELOW the floor. Unknown nodes (no df yet) are NOT low — a cold
    /// leader must keep allocating.
    pub(crate) fn space_low_node_ids(&self) -> HashSet<u64> {
        let floor = self.min_alloc_free_bytes.get();
        if floor == 0 {
            return HashSet::new();
        }
        self.node_max_free
            .borrow()
            .iter()
            .filter(|(_, free)| **free < floor)
            .map(|(id, _)| *id)
            .collect()
    }

    pub fn mark_serving(&self) {
        let now = Instant::now();
        for t in self.ps_last_heartbeat.borrow_mut().values_mut() {
            *t = now;
        }
        self.serving.set(true);
    }

    /// Observability batch 1: Prometheus text snapshot of control-plane
    /// state. Called by the manager binary's 2 s publisher task ON the
    /// compio runtime (the store is `Rc<RefCell>`, !Send); the rendered
    /// string is what crosses to the metrics HTTP thread.
    pub fn metrics_text(&self) -> String {
        use autumn_common::metrics_http::{push_metric, push_type};
        let mut out = String::with_capacity(1024);
        push_type(&mut out, "autumn_manager_leader", "gauge");
        push_metric(&mut out, "autumn_manager_leader", &[], self.leader.get() as u32);
        push_type(&mut out, "autumn_manager_serving", "gauge");
        push_metric(&mut out, "autumn_manager_serving", &[], self.serving.get() as u32);
        {
            let s = self.store.inner.borrow();
            for (name, v) in [
                ("autumn_manager_streams", s.streams.len()),
                ("autumn_manager_extents", s.extents.len()),
                ("autumn_manager_extent_nodes", s.nodes.len()),
                ("autumn_manager_partitions", s.partitions.len()),
                ("autumn_manager_ps_nodes", s.ps_nodes.len()),
                ("autumn_manager_regions", s.regions.len()),
                ("autumn_manager_part_addrs", s.part_addrs.len()),
            ] {
                push_type(&mut out, name, "gauge");
                push_metric(&mut out, name, &[], v as u32);
            }
            // Per-disk online state as the manager sees it (the df-driven
            // call-result signal, CLAUDE.md note 7).
            push_type(&mut out, "autumn_manager_disk_online", "gauge");
            for (disk_id, d) in &s.disks {
                push_metric(
                    &mut out,
                    "autumn_manager_disk_online",
                    &[("disk_id", disk_id.to_string())],
                    d.online as u32,
                );
            }
        }
        push_type(&mut out, "autumn_manager_extent_inflight_ops", "gauge");
        push_metric(
            &mut out,
            "autumn_manager_extent_inflight_ops",
            &[],
            self.inflight.borrow().len() as u32,
        );
        out
    }

    // ── Background loops ───────────────────────────────────────────────

    // (1C): takes `self` by value (was `&self`) for uniformity with the
    // other 8 supervised loops — `spawn_supervised` clones a fresh handle per
    // restart, so the loop future must own it.
    async fn ps_liveness_check_loop(self) {
        const CHECK_INTERVAL: Duration = Duration::from_secs(2);
        const PS_DEAD_TIMEOUT: Duration = Duration::from_secs(10);

        loop {
            compio::time::sleep(CHECK_INTERVAL).await;
            // never evict while our OWN listener isn't accepting —
            // a PS cannot heartbeat into an unbound socket. The ucx bind
            // retry (TIME_WAIT) holds `serve()` for up to ~60 s
            // after a kill+respawn; evicting during that window
            // de-assigns the entire healthy fleet. `serve()` re-seeds
            // the heartbeat clocks when the listener comes up.
            if !self.leader.get() || !self.serving.get() {
                continue;
            }

            let dead_ps: Vec<u64> = {
                let hb = self.ps_last_heartbeat.borrow();
                let s = self.store.inner.borrow();
                s.ps_nodes
                    .keys()
                    .filter(|ps_id| match hb.get(ps_id) {
                        Some(t) => t.elapsed() > PS_DEAD_TIMEOUT,
                        None => false,
                    })
                    .copied()
                    .collect()
            };

            if dead_ps.is_empty() {
                continue;
            }

            for ps_id in &dead_ps {
                tracing::warn!("PS {ps_id} heartbeat timed out, removing and reassigning regions");
            }

            {
                let mut s = self.store.inner.borrow_mut();
                for ps_id in &dead_ps {
                    s.ps_nodes.remove(ps_id);
                }
                Self::rebalance_regions(&mut s);
            }
            {
                let mut hb = self.ps_last_heartbeat.borrow_mut();
                for ps_id in &dead_ps {
                    hb.remove(ps_id);
                }
            }

            // explicit delete of every evicted PS's etcd key.
            // `mirror_partition_snapshot` only PUTs survivors and never
            // DELETEs — previously the evicted `psNodes/<id>` key
            // persisted in etcd indefinitely. On manager failover the
            // new leader's `replay_from_etcd` rehydrated it back into
            // `s.ps_nodes`, but `ps_last_heartbeat` (in-memory only) was
            // empty, so the liveness check's `None` arm short-circuited
            // to `false` (treated as live) and the resurrected ghost
            // PS was unevictable forever. Deleting the etcd key here
            // closes the resurrection path entirely.
            if let Some(etcd) = &self.etcd {
                let deletes: Vec<String> =
                    dead_ps.iter().map(|id| format!("psNodes/{id}")).collect();
                if let Err(e) = etcd.put_and_delete_txn(Vec::new(), deletes).await {
                    tracing::error!("delete evicted psNodes/ keys failed: {e}");
                }
            }

            if let Err(e) = self.mirror_partition_snapshot().await {
                tracing::error!("mirror after PS eviction failed: {e}");
            }
        }
    }

    /// Compute the `region_epoch` for a region about to be (re)written.
    ///
    /// Rules:
    ///   - No prior region in state → start at `1` (bootstrap). `0` is
    ///     reserved as "unknown / skip check" on the wire.
    ///   - rg byte-for-byte equal to prior → keep old epoch (idempotent
    ///     rebalance, PS reassignment without range change, etc.).
    ///   - rg changed (split narrowing, merge widening) → bump by 1.
    ///
    /// Called from BOTH `compute_region_for_partition` (etcd-bound
    /// rkyv blob) and `rebalance_regions` (in-memory shadow). Both
    /// MUST agree or etcd ↔ memory drifts on leader failover.
    fn next_region_epoch(
        state: &autumn_common::MetadataState,
        part_id: u64,
        new_rg: &Option<MgrRange>,
    ) -> u64 {
        match state.regions.get(&part_id) {
            Some(r) if r.rg == *new_rg => r.region_epoch.max(1),
            Some(r) => r.region_epoch.saturating_add(1).max(2),
            None => 1,
        }
    }

    fn rebalance_regions(state: &mut autumn_common::MetadataState) {
        let part_ids: HashSet<u64> = state.partitions.keys().copied().collect();
        let stale: Vec<u64> = state
            .regions
            .keys()
            .copied()
            .filter(|part_id| !part_ids.contains(part_id))
            .collect();
        for part_id in stale {
            state.regions.remove(&part_id);
            // a dropped region implies the old per-partition
            // listener address is also invalid; drop it so clients can't
            // be handed back a dead addr via GetRegions.
            state.part_addrs.remove(&part_id);
        }

        if state.ps_nodes.is_empty() {
            return;
        }

        let mut load: HashMap<u64, usize> = state.ps_nodes.keys().map(|&id| (id, 0)).collect();
        for region in state.regions.values() {
            if let Some(cnt) = load.get_mut(&region.ps_id) {
                *cnt += 1;
            }
        }

        let mut ids: Vec<u64> = part_ids.into_iter().collect();
        ids.sort_unstable();

        for part_id in ids {
            let meta = match state.partitions.get(&part_id) {
                Some(m) => m,
                None => continue,
            };

            let ps_id = if let Some(r) = state.regions.get(&part_id) {
                if state.ps_nodes.contains_key(&r.ps_id) {
                    r.ps_id
                } else {
                    match load.iter().min_by_key(|(_, &cnt)| cnt).map(|(&id, _)| id) {
                        Some(id) => {
                            *load.entry(id).or_insert(0) += 1;
                            id
                        }
                        None => continue,
                    }
                }
            } else {
                match load.iter().min_by_key(|(_, &cnt)| cnt).map(|(&id, _)| id) {
                    Some(id) => {
                        *load.entry(id).or_insert(0) += 1;
                        id
                    }
                    None => continue,
                }
            };

            let region_epoch = Self::next_region_epoch(state, part_id, &meta.rg);
            state.regions.insert(
                part_id,
                MgrRegionInfo {
                    rg: meta.rg.clone(),
                    part_id,
                    ps_id,
                    log_stream: meta.log_stream,
                    row_stream: meta.row_stream,
                    meta_stream: meta.meta_stream,
                    region_epoch,
                },
            );
        }
    }

    /// compute the moves that bring the per-PS partition
    /// COUNT as even as possible — repeatedly reassign one partition from the
    /// most-loaded registered PS to the least-loaded, stopping when the gap is
    /// ≤ 1 (perfectly balanced up to the remainder) or `max_moves` is reached
    /// (`0` = unbounded). Unlike `rebalance_regions` (which keeps a registered
    /// PS's regions STICKY), this ACTIVELY moves regions off an overloaded PS —
    /// the WAS-PM / TiKV-PD `balance-region` behaviour.
    ///
    /// PURE + DETERMINISTIC (a dry-run matches the applied set): only PS in
    /// `ps_nodes` participate; ties on load break by lowest `ps_id`; the
    /// partition moved off the most-loaded PS is its largest `part_id`. The
    /// caller applies each move by rewriting `regions[part_id].ps_id`.
    ///
    /// Count-based (not load/QPS-based) by design for v1 — partition count is
    /// the coarse-but-robust signal (HBase `SimpleLoadBalancer`); a future
    /// req/s-weighted variant can reuse the same apply path.
    fn compute_rebalance_moves(
        state: &autumn_common::MetadataState,
        max_moves: u32,
    ) -> Vec<RebalanceMove> {
        // Partition ids per REGISTERED PS (a region on an unregistered PS is
        // the eviction path's job, not ours; it isn't a movable source here).
        let mut by_ps: HashMap<u64, Vec<u64>> =
            state.ps_nodes.keys().map(|&id| (id, Vec::new())).collect();
        for (part_id, region) in &state.regions {
            if let Some(v) = by_ps.get_mut(&region.ps_id) {
                v.push(*part_id);
            }
        }
        if by_ps.len() < 2 {
            return Vec::new(); // nothing to balance across
        }
        for v in by_ps.values_mut() {
            v.sort_unstable(); // largest part_id is popped first (deterministic)
        }
        let cap = if max_moves == 0 {
            usize::MAX
        } else {
            max_moves as usize
        };
        let mut moves = Vec::new();
        while moves.len() < cap {
            // most-loaded (ties → lowest ps_id), least-loaded (ties → lowest ps_id)
            let most = by_ps
                .iter()
                .map(|(id, v)| (v.len(), std::cmp::Reverse(*id)))
                .max()
                .map(|(_, r)| r.0);
            let least = by_ps
                .iter()
                .map(|(id, v)| (v.len(), *id))
                .min_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)))
                .map(|(_, id)| id);
            let (Some(most), Some(least)) = (most, least) else {
                break;
            };
            if most == least {
                break;
            }
            let most_n = by_ps[&most].len();
            let least_n = by_ps[&least].len();
            if most_n <= least_n + 1 {
                break; // balanced: gap of 1 is the irreducible remainder
            }
            let part_id = by_ps.get_mut(&most).unwrap().pop().unwrap();
            by_ps.get_mut(&least).unwrap().push(part_id);
            moves.push(RebalanceMove {
                part_id,
                from_ps: most,
                to_ps: least,
            });
        }
        moves
    }

    fn compute_region_for_partition(
        state: &autumn_common::MetadataState,
        part: &MgrPartitionMeta,
    ) -> MgrRegionInfo {
        let ps_id = state
            .regions
            .get(&part.part_id)
            .filter(|r| state.ps_nodes.contains_key(&r.ps_id))
            .map(|r| r.ps_id)
            .or_else(|| {
                let mut load: HashMap<u64, usize> =
                    state.ps_nodes.keys().map(|&id| (id, 0)).collect();
                for region in state.regions.values() {
                    if let Some(cnt) = load.get_mut(&region.ps_id) {
                        *cnt += 1;
                    }
                }
                load.into_iter()
                    .min_by_key(|&(_, cnt)| cnt)
                    .map(|(id, _)| id)
            })
            .unwrap_or(0);
        let region_epoch = Self::next_region_epoch(state, part.part_id, &part.rg);
        MgrRegionInfo {
            rg: part.rg.clone(),
            part_id: part.part_id,
            ps_id,
            log_stream: part.log_stream,
            row_stream: part.row_stream,
            meta_stream: part.meta_stream,
            region_epoch,
        }
    }

    /// Compute the mutations for duplicating a stream (CoW for split).
    /// Returns (new_stream, modified_extents) WITHOUT modifying state.
    fn compute_duplicate_stream(
        state: &autumn_common::MetadataState,
        src_stream_id: u64,
        dst_stream_id: u64,
        sealed_length: u64,
    ) -> Result<(MgrStreamInfo, Vec<MgrExtentInfo>), AppError> {
        let src = state
            .streams
            .get(&src_stream_id)
            .cloned()
            .ok_or_else(|| AppError::NotFound(format!("stream {src_stream_id}")))?;

        let mut dst = MgrStreamInfo {
            stream_id: dst_stream_id,
            extent_ids: vec![],
            ec_data_shard: src.ec_data_shard,
            ec_parity_shard: src.ec_parity_shard,
            replicates: src.replicates,
        };

        let mut modified_extents = Vec::new();
        for (idx, extent_id) in src.extent_ids.iter().enumerate() {
            let extent = state
                .extents
                .get(extent_id)
                .ok_or_else(|| AppError::NotFound(format!("extent {extent_id}")))?;
            let mut ex = extent.clone();
            ex.refs += 1;
            ex.eversion += 1;
            // Seal the shared tail at the split-time commit — EVEN at 0 (an
            // empty tail): both parent + child CoW-share this extent, so it
            // MUST be frozen (sealed=true, avali set) or both writers could
            // append to the same open extent (coco P1 — CoW isolation). A
            // sealed-empty tail (sealed=true, sealed_length=0) makes each
            // stream alloc a fresh tail on init instead of sharing this one.
            if idx == src.extent_ids.len() - 1 && !ex.sealed {
                ex.sealed = true;
                ex.sealed_length = sealed_length;
                ex.avali = Self::all_bits(ex.replicates.len() + ex.parity.len());
                // BUG2 trace (opt-in): split CoW-tail seal. A `sealed_length=0`
                // here freezes a shared tail that may hold VP/SST-acked data →
                // child opens fail with stale_vp_offset_past_sealed_length.
                tracing::info!(
                    target: "bug2_trace",
                    extent_id = *extent_id,
                    dst_stream = dst_stream_id,
                    sealed_length,
                    "BUG2 split duplicate CoW-tail seal"
                );
            }
            modified_extents.push(ex);
            dst.extent_ids.push(*extent_id);
        }

        Ok((dst, modified_extents))
    }

    /// splice victim's extents onto the END of survivor's
    /// extent_ids list, then append `new_tail` as the new active tail.
    ///
    /// Order invariant (load-bearing):
    ///   updated.extent_ids = [survivor.existing] + [victim.existing] + [new_tail]
    ///
    /// Refs are membership-neutral on victim extents (transfer victim→survivor;
    /// CoW-shared ⇒ refs-=1 + dedup — see compute_merge_streams). Sealing rules:
    ///   - survivor's old tail (last existing extent) sealed at `survivor_sealed`
    ///     if it was open
    ///   - victim's old tail (last victim extent) sealed at `victim_sealed`
    ///     if it was open
    ///   - new_tail is appended as-is (caller has already built its
    ///     MgrExtentInfo via select_nodes + alloc_extent_on_node)
    ///
    /// Caller (handle_multi_modify_merge) is responsible for the
    /// inflight checks before calling this.
    /// Seal the survivor stream's existing tail extent at
    /// `survivor_sealed` and push the sealed record to `modified_extents`.
    /// The tail is sealed EVEN WHEN EMPTY (length 0): after the merge it
    /// is no longer the active tail (a new tail follows) and is CoW-shared,
    /// so it must be frozen for CoW isolation (coco P1). No-op when the
    /// survivor has no extents or its tail is already sealed. Shared by
    /// `compute_merge_streams` + `splice_streams_without_new_tail`.
    fn seal_survivor_old_tail(
        state: &autumn_common::MetadataState,
        survivor: &MgrStreamInfo,
        survivor_sealed: u64,
        modified_extents: &mut Vec<MgrExtentInfo>,
    ) -> Result<(), AppError> {
        if let Some(&tail_id) = survivor.extent_ids.last() {
            let extent = state
                .extents
                .get(&tail_id)
                .ok_or_else(|| AppError::NotFound(format!("extent {tail_id}")))?;
            let mut ex = extent.clone();
            if !ex.sealed {
                ex.sealed = true;
                ex.sealed_length = survivor_sealed;
                ex.eversion += 1;
                ex.avali = Self::all_bits(ex.replicates.len() + ex.parity.len());
                modified_extents.push(ex);
            }
        }
        Ok(())
    }

    /// Splice victim's extents onto the survivor and push each (eversion-
    /// bumped, tail sealed) record to `modified_extents`.
    ///
    /// Merge refs accounting: `refs` is MEMBERSHIP-NEUTRAL for victim extents
    /// (refs == # of streams whose `extent_ids` list the extent). A victim
    /// extent is one of:
    ///   * CoW-shared (also in the survivor, from a prior split→merge-back):
    ///     the merge collapses the two memberships {survivor, victim} into
    ///     one {survivor}, so refs -= 1 AND the extent is NOT re-listed (the
    ///     caller's dedup filter drops it). Re-listing would put it twice in
    ///     the survivor's extent_ids, and a later GC `punch_holes` (whose
    ///     `retain` drops ALL occurrences but decrements refs by only 1)
    ///     could never reconcile it.
    ///   * victim-only: its membership simply transfers victim→survivor, so
    ///     refs is UNCHANGED (the +1 for the survivor splice cancels the -1
    ///     for the deleted victim stream).
    /// Pre-fix this did an unconditional `ex.refs += 1` while
    /// `apply_merge_mutations` deleted the victim stream WITHOUT a
    /// compensating decrement → +1 leak per victim extent per merge.
    ///
    /// The victim's old tail is sealed even at length 0 (CoW isolation,
    /// coco P1); it is a post-split unique extent, never in `survivor_set`,
    /// so this never collides with the dedup. Shared by
    /// `compute_merge_streams` + `splice_streams_without_new_tail`.
    fn splice_victim_extents(
        state: &autumn_common::MetadataState,
        survivor_set: &HashSet<u64>,
        victim: &MgrStreamInfo,
        victim_sealed: u64,
        modified_extents: &mut Vec<MgrExtentInfo>,
    ) -> Result<(), AppError> {
        for (idx, &eid) in victim.extent_ids.iter().enumerate() {
            let extent = state
                .extents
                .get(&eid)
                .ok_or_else(|| AppError::NotFound(format!("extent {eid}")))?;
            let mut ex = extent.clone();
            if survivor_set.contains(&eid) {
                ex.refs = ex.refs.saturating_sub(1);
            }
            ex.eversion += 1;
            if idx == victim.extent_ids.len() - 1 && !ex.sealed {
                ex.sealed = true;
                ex.sealed_length = victim_sealed;
                ex.avali = Self::all_bits(ex.replicates.len() + ex.parity.len());
            }
            modified_extents.push(ex);
        }
        Ok(())
    }

    fn compute_merge_streams(
        state: &autumn_common::MetadataState,
        survivor_stream_id: u64,
        victim_stream_id: u64,
        survivor_sealed: u64,
        victim_sealed: u64,
        new_tail: MgrExtentInfo,
    ) -> Result<(MgrStreamInfo, Vec<MgrExtentInfo>), AppError> {
        let survivor = state
            .streams
            .get(&survivor_stream_id)
            .cloned()
            .ok_or_else(|| AppError::NotFound(format!("stream {survivor_stream_id}")))?;
        let victim = state
            .streams
            .get(&victim_stream_id)
            .cloned()
            .ok_or_else(|| AppError::NotFound(format!("stream {victim_stream_id}")))?;

        let mut modified_extents = Vec::new();
        Self::seal_survivor_old_tail(state, &survivor, survivor_sealed, &mut modified_extents)?;

        let survivor_set: HashSet<u64> = survivor.extent_ids.iter().copied().collect();
        Self::splice_victim_extents(
            state,
            &survivor_set,
            &victim,
            victim_sealed,
            &mut modified_extents,
        )?;

        // Splice extent_ids: [survivor.existing] + [victim.existing NOT already
        // in survivor] + [new_tail]. The dedup keeps each extent listed once;
        // shared extents stay in their survivor (front) position, preserving
        // the load-bearing vp_head replay order.
        let mut new_extent_ids = survivor.extent_ids.clone();
        new_extent_ids.extend(
            victim
                .extent_ids
                .iter()
                .copied()
                .filter(|e| !survivor_set.contains(e)),
        );
        new_extent_ids.push(new_tail.extent_id);

        let updated = MgrStreamInfo {
            stream_id: survivor.stream_id,
            extent_ids: new_extent_ids,
            ec_data_shard: survivor.ec_data_shard,
            ec_parity_shard: survivor.ec_parity_shard,
            replicates: survivor.replicates,
        };

        modified_extents.push(new_tail);

        Ok((updated, modified_extents))
    }

    /// same as compute_merge_streams but without appending a new
    /// tail. Used for row_stream + meta_stream where the post-merge
    /// stream's tail is just victim's last existing extent (sealed by
    /// the caller's commit_length capture).
    fn splice_streams_without_new_tail(
        state: &autumn_common::MetadataState,
        survivor_stream_id: u64,
        victim_stream_id: u64,
        survivor_sealed: u64,
        victim_sealed: u64,
    ) -> Result<(MgrStreamInfo, Vec<MgrExtentInfo>), AppError> {
        let survivor = state
            .streams
            .get(&survivor_stream_id)
            .cloned()
            .ok_or_else(|| AppError::NotFound(format!("stream {survivor_stream_id}")))?;
        let victim = state
            .streams
            .get(&victim_stream_id)
            .cloned()
            .ok_or_else(|| AppError::NotFound(format!("stream {victim_stream_id}")))?;

        let mut modified_extents = Vec::new();
        Self::seal_survivor_old_tail(state, &survivor, survivor_sealed, &mut modified_extents)?;

        let survivor_set: HashSet<u64> = survivor.extent_ids.iter().copied().collect();
        Self::splice_victim_extents(
            state,
            &survivor_set,
            &victim,
            victim_sealed,
            &mut modified_extents,
        )?;

        let mut new_extent_ids = survivor.extent_ids.clone();
        new_extent_ids.extend(
            victim
                .extent_ids
                .iter()
                .copied()
                .filter(|e| !survivor_set.contains(e)),
        );
        Ok((
            MgrStreamInfo {
                stream_id: survivor.stream_id,
                extent_ids: new_extent_ids,
                ec_data_shard: survivor.ec_data_shard,
                ec_parity_shard: survivor.ec_parity_shard,
                replicates: survivor.replicates,
            },
            modified_extents,
        ))
    }

    /// Extent retention predicate. Reclaimable iff no stream lists it
    /// (`refs == 0`) AND no live SST ValuePointer is recorded against it
    /// (`vp_table_refs == 0`).
    ///
    /// vp_table_refs-removal STAGING NOTE: the vp_table_refs *maintenance*
    /// machinery (the PS sync/pull RPCs, the manager-side aggregation) is gone,
    /// so the field is now write-FROZEN — every extent allocated/managed under
    /// this build has `vp_table_refs == 0`, making this gate effectively
    /// `refs == 0` for them. The `&& vp_table_refs == 0` clause is RETAINED as
    /// an UPGRADE-SAFETY GUARD: a cluster upgraded from a pre-removal build may
    /// hold legacy extents legitimately retained at `refs == 0 && vp_table_refs
    /// > 0` (live VPs that the old net protected, e.g. extent 10). Collapsing
    /// to `refs == 0` here would reap them and lose data, because the
    /// `refs == 0 ⇒ no live VP` invariant only holds for extents that reached
    /// `refs == 0` under the post-GC-VP-IDENTITY relocate-then-punch path — not
    /// for state frozen in etcd by an older buggy GC. Stage 2's migration
    /// (re-confirm no live VP / major-compact, then clear the field) is what
    /// lets this collapse to `refs == 0`. Until then the guard stays; such
    /// legacy extents are simply not reclaimed (a bounded space leak, never a
    /// loss). See manager/CLAUDE.md "VP lifetime after split".
    fn extent_can_delete(extent: &MgrExtentInfo) -> bool {
        extent.refs == 0 && extent.vp_table_refs == 0
    }

    /// Apply computed split mutations to the in-memory store.
    fn apply_split_mutations(
        state: &mut autumn_common::MetadataState,
        new_streams: &[MgrStreamInfo],
        modified_extents: &[MgrExtentInfo],
        left: MgrPartitionMeta,
        right: MgrPartitionMeta,
    ) {
        for ex in modified_extents {
            state.extents.insert(ex.extent_id, ex.clone());
        }
        for st in new_streams {
            state.streams.insert(st.stream_id, st.clone());
        }
        state.partitions.insert(left.part_id, left);
        state.partitions.insert(right.part_id, right);
        Self::rebalance_regions(state);
    }

    /// apply computed merge mutations. Mirror of `apply_split_mutations`.
    /// Caller (handle_multi_modify_merge Phase 3) verifies eversion drift
    /// before invoking. Drops victim partition + its three stream metas;
    /// rebalances regions to remove the victim's region.
    #[allow(clippy::too_many_arguments)]
    fn apply_merge_mutations(
        state: &mut autumn_common::MetadataState,
        survivor_streams: &[MgrStreamInfo],
        modified_extents: &[MgrExtentInfo],
        survivor_meta: MgrPartitionMeta,
        victim_part_id: u64,
        victim_log_stream: u64,
        victim_row_stream: u64,
        victim_meta_stream: u64,
    ) {
        for ex in modified_extents {
            state.extents.insert(ex.extent_id, ex.clone());
        }
        for st in survivor_streams {
            state.streams.insert(st.stream_id, st.clone());
        }
        state
            .partitions
            .insert(survivor_meta.part_id, survivor_meta);

        // Drop victim entries.
        state.partitions.remove(&victim_part_id);
        state.streams.remove(&victim_log_stream);
        state.streams.remove(&victim_row_stream);
        state.streams.remove(&victim_meta_stream);
        state.regions.remove(&victim_part_id);

        Self::rebalance_regions(state);
    }

    fn extent_nodes(extent: &MgrExtentInfo) -> Vec<u64> {
        extent
            .replicates
            .iter()
            .copied()
            .chain(extent.parity.iter().copied())
            .collect()
    }

    fn extent_slot(extent: &MgrExtentInfo, node_id: u64) -> Option<usize> {
        Self::extent_nodes(extent)
            .iter()
            .position(|id| *id == node_id)
    }

    fn epoch_seconds() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    fn normalize_endpoint(endpoint: &str) -> String {
        endpoint
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .to_string()
    }

    // ── Extent node RPC helpers ─────────────────────────────────────────

    /// look up `shard_ports` for a node by address, so we can
    /// route extent RPCs to the owning shard. Returns empty Vec if the
    /// node isn't found (shouldn't happen in practice but stays safe).
    fn shard_ports_for_addr(&self, addr: &str) -> Vec<u16> {
        let normalized = Self::normalize_endpoint(addr);
        let s = self.store.inner.borrow();
        for node in s.nodes.values() {
            if Self::normalize_endpoint(&node.address) == normalized {
                return node.shard_ports.clone();
            }
        }
        Vec::new()
    }

    /// route an address to the shard listening for `extent_id`.
    /// If `shard_ports` is empty, returns `addr` unchanged (legacy mode).
    fn shard_addr_for_extent(addr: &str, shard_ports: &[u16], extent_id: u64) -> String {
        if shard_ports.is_empty() {
            return addr.to_string();
        }
        let k = shard_ports.len();
        // canonical hashed extent→shard map (was
        // `extent_id % k`, which aliased bootstrap's contiguous ids onto shard
        // 0). MUST match the EN `owns_extent` + StreamClient conn_pool routing.
        let port = shard_ports[autumn_rpc::shard_for_extent(extent_id, k as u32) as usize];
        let trimmed = Self::normalize_endpoint(addr);
        if let Some(colon) = trimmed.rfind(':') {
            format!("{}:{}", &trimmed[..colon], port)
        } else {
            format!("{trimmed}:{port}")
        }
    }

    async fn alloc_extent_on_node(&self, addr: &str, extent_id: u64) -> Result<u64, AppError> {
        let base = Self::normalize_endpoint(addr);
        let shard_ports = self.shard_ports_for_addr(&base);
        let routed = Self::shard_addr_for_extent(&base, &shard_ports, extent_id);
        let payload = rkyv_encode(&ExtAllocExtentReq { extent_id });
        // 10 s — alloc_extent is a fast op (create empty file pair +
        // sidecar fsync). A paged-out EN that doesn't respond inside
        // 10 s is treated as a failed candidate; the caller's
        // shuffled-fallback walk picks another node.
        let resp = self
            .conn_pool
            .call_timeout(
                &routed,
                EXT_MSG_ALLOC_EXTENT,
                payload,
                Duration::from_secs(10),
            )
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        let r: ExtAllocExtentResp = rkyv_decode(&resp).map_err(AppError::Internal)?;
        if r.code != CODE_OK {
            return Err(AppError::Internal(format!(
                "alloc_extent failed: {}",
                r.message
            )));
        }
        Ok(r.disk_id)
    }

    /// Seal-consensus probe: query an EN's `commit_length` for one extent
    /// under the caller's validated owner-lock owner_epoch. This path
    /// (2026-05-17) plumbs the PS-validated owner_epoch through (was: hardcoded
    /// `0` + EN-side escape hatch) so the EN's fence-handover side-effect
    /// (`if req.owner_epoch > last { bump + persist .meta }`) actually fires
    /// when a new owner first contacts an EN. Callers without an owner
    /// context (recovery liveness, autumn-client info display) MUST use
    /// `probe_extent_on_node` instead — that helper hits `MSG_PROBE_EXTENT`,
    /// which skips the fence entirely.
    pub(crate) async fn commit_length_on_node(
        &self,
        addr: &str,
        extent_id: u64,
        owner_epoch: i64,
    ) -> Result<u64, AppError> {
        debug_assert!(
            owner_epoch > 0,
            "commit_length_on_node requires owner_epoch > 0; use probe_extent_on_node for fence-free probes"
        );
        let base = Self::normalize_endpoint(addr);
        let shard_ports = self.shard_ports_for_addr(&base);
        let routed = Self::shard_addr_for_extent(&base, &shard_ports, extent_id);
        let req = ExtCommitLengthReq {
            extent_id,
            owner_epoch,
        };
        // 5 s — commit_length is a tiny in-memory read on EN (atomic
        // load of `entry.len`). Generous bound so a hiccupping EN
        // doesn't hang split / commit-len consensus paths.
        let resp = self
            .conn_pool
            .call_timeout(
                &routed,
                EXT_MSG_COMMIT_LENGTH,
                req.encode(),
                Duration::from_secs(5),
            )
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        let r = ExtCommitLengthResp::decode(resp).map_err(|e| AppError::Internal(e.to_string()))?;
        if r.code != CODE_OK {
            return Err(AppError::Internal(format!(
                "commit_length failed on {routed}: code {}",
                r.code
            )));
        }
        Ok(r.length)
    }

    /// Tier 2 fence-free probe. Used by:
    ///   - `recovery_dispatch_loop` liveness check (ignores `length`,
    ///     uses `code == CODE_OK` to decide whether to fire
    ///     `dispatch_recovery_task`).
    ///   - Future: any manager-internal "is this extent on this EN +
    ///     what's its current length" query without an owner context.
    /// Does NOT touch the EN's `owner_epoch`. NotFound (extent missing
    /// locally) and RPC error are both surfaced as `Err(Internal(...))`
    /// so callers can treat both as "dispatch recovery" without branching.
    pub(crate) async fn probe_extent_on_node(
        &self,
        addr: &str,
        extent_id: u64,
    ) -> Result<u64, AppError> {
        let base = Self::normalize_endpoint(addr);
        let shard_ports = self.shard_ports_for_addr(&base);
        let routed = Self::shard_addr_for_extent(&base, &shard_ports, extent_id);
        let req = ExtProbeExtentReq { extent_id };
        let resp = self
            .conn_pool
            .call_timeout(
                &routed,
                EXT_MSG_PROBE_EXTENT,
                req.encode(),
                Duration::from_secs(5),
            )
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        let r = ExtProbeExtentResp::decode(resp).map_err(|e| AppError::Internal(e.to_string()))?;
        if r.code != CODE_OK {
            return Err(AppError::Internal(format!(
                "probe_extent on {routed}: code {}",
                r.code
            )));
        }
        Ok(r.length)
    }

    // ── Etcd mirroring ─────────────────────────────────────────────────

    async fn persist_extent(&self, extent: &MgrExtentInfo) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let value = rkyv_encode(extent).to_vec();
            etcd.put_msgs_txn(vec![(format!("extents/{}", extent.extent_id), value)])
                .await?;
        }
        Ok(())
    }

    /// persist a marker that extent X is currently mid-EC-conversion
    /// from THIS leader's perspective. Called BEFORE the
    /// `EXT_MSG_CONVERT_TO_EC` RPC is dispatched. If this leader dies
    /// mid-flight, the new leader's `replay_from_etcd` repopulates
    async fn mark_extent_available(&self, extent_id: u64, slot: usize) -> Result<(), AppError> {
        // defer while EC conversion is in flight on this
        // extent. re_avali was sent to the extent-node (eversion bump
        // there), but the manager-side eversion bump must not race
        // apply_ec_conversion_done's overwrite. The recovery_dispatch_loop
        // retries on the next tick. Reads the unified ledger via
        // `extent_inflight_op`.
        if matches!(
            self.extent_inflight_op(extent_id),
            Some(crate::extent_inflight::ExtentOpKind::ConvertToEc)
        ) {
            return Err(AppError::Precondition(format!(
                "ec conversion in flight on extent {extent_id}; deferring mark_extent_available"
            )));
        }
        let updated = {
            let mut s = self.store.inner.borrow_mut();
            let ex = s
                .extents
                .get_mut(&extent_id)
                .ok_or_else(|| AppError::NotFound(format!("extent {extent_id}")))?;
            if slot >= ex.replicates.len() + ex.parity.len() {
                return Err(AppError::InvalidArgument(format!(
                    "invalid slot {slot} for extent {extent_id}"
                )));
            }
            let bit = 1u32 << slot;
            if (ex.avali & bit) != 0 {
                return Ok(());
            }
            ex.avali |= bit;
            ex.eversion += 1;
            ex.clone()
        };
        self.persist_extent(&updated).await?;
        Ok(())
    }

    // ── Etcd mirror helpers ────────────────────────────────────────────

    /// Build a `("<prefix>/<id>", rkyv_encode(value))` etcd txn entry.
    /// Centralizes the key-format + rkyv-encode pattern repeated across the
    /// mirror_* helpers.
    fn kv_entry<T>(prefix: &str, id: u64, value: &T) -> (String, Vec<u8>)
    where
        T: for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::rancor::Error,
            >,
        >,
    {
        (format!("{prefix}/{id}"), rkyv_encode(value).to_vec())
    }

    async fn mirror_register_node(
        &self,
        node: &MgrNodeInfo,
        disks: &[MgrDiskInfo],
    ) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            // M0: `node.node_uuid` (the stable identity) rides
            // inside the persisted `MgrNodeInfo` — no separate index kv.
            let mut kvs = Vec::with_capacity(1 + disks.len());
            kvs.push(Self::kv_entry("nodes", node.node_id, node));
            for disk in disks {
                kvs.push(Self::kv_entry("disks", disk.disk_id, disk));
            }
            etcd.put_msgs_txn(kvs).await?;
        }
        Ok(())
    }

    async fn mirror_stream_meta_update(&self, stream: &MgrStreamInfo) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let kvs = vec![Self::kv_entry("streams", stream.stream_id, stream)];
            etcd.put_msgs_txn(kvs).await?;
        }
        Ok(())
    }

    async fn mirror_create_stream(
        &self,
        stream: &MgrStreamInfo,
        extent: &MgrExtentInfo,
    ) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let kvs = vec![
                Self::kv_entry("streams", stream.stream_id, stream),
                Self::kv_entry("extents", extent.extent_id, extent),
            ];
            etcd.put_msgs_txn(kvs).await?;
        }
        Ok(())
    }

    /// `sealed_old` is `Some` only when this alloc actually re-sealed the old
    /// tail (the `!already_sealed` path). When the tail was already sealed it
    /// is `None`: re-persisting the early-snapshotted tail would clobber a
    /// concurrent Recovery's `replicates` / `eversion` writeback that lands
    /// during this txn's RTT (the seed=13 wedge fix). The sealer already
    /// durably persisted the tail, so skipping it loses nothing.
    async fn mirror_stream_alloc_extent(
        &self,
        stream: &MgrStreamInfo,
        sealed_old: Option<&MgrExtentInfo>,
        new_extent: &MgrExtentInfo,
        // `Some(bytes)` = value-CAS the `streams/<id>` write against the
        // membership baseline the handler read. If a concurrent punch_holes /
        // truncate / another alloc changed the stream during our etcd RTT, the
        // CAS fails → `Precondition` → client retries (instead of overwriting
        // the concurrent change and resurrecting a removed extent).
        stream_cas: Option<Vec<u8>>,
    ) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let mut kvs = vec![Self::kv_entry("streams", stream.stream_id, stream)];
            if let Some(sealed_old) = sealed_old {
                kvs.push(Self::kv_entry("extents", sealed_old.extent_id, sealed_old));
            }
            kvs.push(Self::kv_entry("extents", new_extent.extent_id, new_extent));
            let cas: Vec<(String, Vec<u8>)> = stream_cas
                .map(|v| (format!("streams/{}", stream.stream_id), v))
                .into_iter()
                .collect();
            etcd.put_delete_txn_cas(kvs, vec![], cas).await?;
        }
        Ok(())
    }

    async fn mirror_stream_extent_mutation(
        &self,
        stream: &MgrStreamInfo,
        extent_puts: &[MgrExtentInfo],
        extent_deletes: &[u64],
        // Value-CAS baseline for the `streams/<id>` membership write
        // (see `mirror_stream_alloc_extent`).
        stream_cas: Option<Vec<u8>>,
        // Per-extent value-CAS baselines (`extents/<id>` == pre-mutation value).
        // Closes the lost-refs-update race when the SAME CoW-shared extent is
        // mutated concurrently via two different streams: the membership CAS
        // above only guards `streams/<id>`, which never conflicts across
        // different streams, so the shared `extents/<id>` write needs its own
        // compare. See `compute_extent_ref_drops`.
        extent_cas: Vec<(String, Vec<u8>)>,
    ) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let mut puts = Vec::with_capacity(1 + extent_puts.len());
            puts.push(Self::kv_entry("streams", stream.stream_id, stream));
            for ex in extent_puts {
                puts.push(Self::kv_entry("extents", ex.extent_id, ex));
            }
            let deletes = extent_deletes
                .iter()
                .map(|id| format!("extents/{id}"))
                .collect::<Vec<_>>();
            let mut cas: Vec<(String, Vec<u8>)> = stream_cas
                .map(|v| (format!("streams/{}", stream.stream_id), v))
                .into_iter()
                .collect();
            cas.extend(extent_cas);
            etcd.put_delete_txn_cas(puts, deletes, cas).await?;
        }
        Ok(())
    }

    async fn mirror_partition_snapshot(&self) -> Result<(), AppError> {
        if let Some(etcd) = &self.etcd {
            let (ps_nodes, partitions, regions) = {
                let s = self.store.inner.borrow();
                (s.ps_nodes.clone(), s.partitions.clone(), s.regions.clone())
            };
            let mut kvs = Vec::with_capacity(ps_nodes.len() + partitions.len() + regions.len());
            for (ps_id, addr) in ps_nodes {
                kvs.push((format!("psNodes/{ps_id}"), addr.into_bytes()));
            }
            for (part_id, part) in partitions {
                kvs.push(Self::kv_entry("partitions", part_id, &part));
            }
            for (part_id, region) in regions {
                kvs.push(Self::kv_entry("regions", part_id, &region));
            }
            etcd.put_msgs_txn(kvs).await?;
        }
        Ok(())
    }

}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    // These tests borrow `store.inner` then `.await` a handler; clippy flags
    // await_holding_refcell_ref, but every such borrow is explicitly `drop()`-ed
    // before the await (and tests are single-threaded, so no concurrent borrow
    // races). False-positive — allow at the module level.
    #![allow(clippy::await_holding_refcell_ref)]
    use super::*;

    // ── pure namespace-validation helpers ──────────────────────────────
    #[test]
    fn validate_namespace_name_accepts_valid_segments() {
        for ok in ["bench", "a", "kv-cache", "app.v2", "under_score", "0", "a1._-"] {
            assert!(validate_namespace_name(ok).is_ok(), "'{ok}' should be valid");
        }
    }

    #[test]
    fn validate_namespace_name_rejects_bad_segments() {
        // empty, uppercase, path separator, whitespace, other punctuation.
        for bad in ["", "Bench", "a/b", "a b", "a+b", "a:b", "acme/prod"] {
            assert!(validate_namespace_name(bad).is_err(), "'{bad}' should be invalid");
        }
    }

    #[test]
    fn namespace_prefix_conflicts_detects_both_directions() {
        // new is a descendant of an existing prefix.
        assert!(namespace_prefix_conflicts(b"a/b/", &[b"a/"]));
        // new is an ancestor of an existing prefix.
        assert!(namespace_prefix_conflicts(b"a/", &[b"a/b/"]));
        // identical prefixes conflict.
        assert!(namespace_prefix_conflicts(b"a/", &[b"a/"]));
        // sibling single-segment prefixes are disjoint (trailing '/' guarantees it).
        assert!(!namespace_prefix_conflicts(b"bench/", &[b"kvc/", b"mem/", b"fs/"]));
        assert!(!namespace_prefix_conflicts(b"f/", &[b"fs/"]));
        // empty existing set never conflicts.
        assert!(!namespace_prefix_conflicts(b"anything/", &[]));
    }

    fn test_extent(extent_id: u64, refs: u64, vp_table_refs: u64) -> MgrExtentInfo {
        MgrExtentInfo {
            extent_id,
            replicates: vec![],
            parity: vec![],
            eversion: 1,
            refs,
            vp_table_refs,
            sealed_length: 0,
            sealed: false,
            avali: 0,
            replicate_disks: vec![],
            parity_disks: vec![],
            ec_converted: false,
        }
    }

    fn run<F: std::future::Future<Output = T>, T>(f: F) -> T {
        compio::runtime::Runtime::new().unwrap().block_on(f)
    }

    /// registers a node in-memory so a subsequent
    /// `handle_report_disk_failure` quorum trip has a node to flip
    /// offline. Skips etcd / mirror by writing to the in-memory store
    /// directly. Used only by the quorum-debounce unit tests below.
    fn add_node_and_disk(m: &AutumnManager, node_id: u64, disk_id: u64) {
        let mut s = m.store.inner.borrow_mut();
        s.nodes.insert(
            node_id,
            MgrNodeInfo {
                node_id,
                address: format!("127.0.0.1:{}", 9100 + node_id),
                disks: vec![disk_id],
                shard_ports: vec![],
                control_address: format!("127.0.0.1:{}", 10100 + node_id),
                node_uuid: String::new(),
            },
        );
        s.disks.insert(
            disk_id,
            MgrDiskInfo {
                disk_id,
                online: true,
                uuid: format!("uuid-{disk_id}"),
            },
        );
    }

    fn fire_report(m: &AutumnManager, node_id: u64, reporter_part_id: u64, ts_ms: i64) -> CodeResp {
        let req = rkyv_encode(&ReportDiskFailureReq {
            node_id,
            extent_id: 1,
            error_kind: REPORT_DISK_FAILURE_KIND_GENERIC,
            reporter_part_id,
            ts_ms,
        });
        let resp = run(async { m.handle_report_disk_failure(req).await.unwrap() });
        rkyv_decode::<CodeResp>(&resp).expect("decode CodeResp")
    }

    #[test]
    fn op_submit_rejects_bad_kind_and_missing_target() {
        let m = AutumnManager::new(); // memory mode ⇒ leader = true
        // unknown kind.
        let r: OpSubmitResp = rkyv_decode(&run(async {
            m.handle_op_submit(rkyv_encode(&OpSubmitReq { kind: 0, ..Default::default() }))
                .await
                .unwrap()
        }))
        .unwrap();
        assert_eq!(r.code, CODE_INVALID_ARGUMENT);
        assert_eq!(r.op_id, 0);
        // split with no part_id.
        let r: OpSubmitResp = rkyv_decode(&run(async {
            m.handle_op_submit(rkyv_encode(&OpSubmitReq {
                kind: OP_KIND_SPLIT,
                part_id: 0,
                ..Default::default()
            }))
            .await
            .unwrap()
        }))
        .unwrap();
        assert_eq!(r.code, CODE_INVALID_ARGUMENT);
        // merge missing victim.
        let r: OpSubmitResp = rkyv_decode(&run(async {
            m.handle_op_submit(rkyv_encode(&OpSubmitReq {
                kind: OP_KIND_MERGE,
                part_id: 3,
                secondary_id: 0,
                ..Default::default()
            }))
            .await
            .unwrap()
        }))
        .unwrap();
        assert_eq!(r.code, CODE_INVALID_ARGUMENT);
    }

    #[test]
    fn op_submit_records_pending_then_query_finds_it() {
        let m = AutumnManager::new();
        // A gc submit is accepted + recorded (the spawned actuation will fail to
        // reach a PS in this bare test, but the ledger entry exists immediately).
        let sub: OpSubmitResp = rkyv_decode(&run(async {
            m.handle_op_submit(rkyv_encode(&OpSubmitReq {
                kind: OP_KIND_GC,
                part_id: 9,
                ..Default::default()
            }))
            .await
            .unwrap()
        }))
        .unwrap();
        assert_eq!(sub.code, CODE_OK);
        assert_ne!(sub.op_id, 0);
        // status <id> → the recorded op (Pending/Running/terminal — all valid,
        // just not UNKNOWN and matching kind/target).
        let q: OpQueryResp = rkyv_decode(&run(async {
            m.handle_op_query(rkyv_encode(&OpQueryReq { op_id: sub.op_id, ..Default::default() }))
                .await
                .unwrap()
        }))
        .unwrap();
        assert_eq!(q.ops.len(), 1);
        assert_eq!(q.ops[0].op_id, sub.op_id);
        assert_eq!(q.ops[0].kind, OP_KIND_GC);
        assert_eq!(q.ops[0].part_id, 9);
        assert_ne!(q.ops[0].state, OP_STATE_UNKNOWN);
        // an unknown id → synthesized UNKNOWN, never a false RUNNING.
        let q: OpQueryResp = rkyv_decode(&run(async {
            m.handle_op_query(rkyv_encode(&OpQueryReq { op_id: 777_777, ..Default::default() }))
                .await
                .unwrap()
        }))
        .unwrap();
        assert_eq!(q.ops[0].state, OP_STATE_UNKNOWN);
    }

    /// A recovery completion must match the LIVE marker. The release path
    /// explicitly contemplates an executor that keeps working after its marker
    /// is dropped ("if it finishes later, its completion is refused") — this is
    /// that refusal, and it did not exist.
    ///
    /// What it cost: a df blip makes the pinned node Suspected, so the marker
    /// is released routinely. If the extent is then EC-converted, the old
    /// executor's late report of a PRE-conversion full copy would swap the slot
    /// onto a node holding a `.dat` while the layout names a shard file — and
    /// the replaced node, now a non-member, gets its real shard reaped.
    #[test]
    fn recovery_completion_from_a_released_attempt_is_refused() {
        let m = AutumnManager::new();
        let extent_id = 42;
        {
            let mut s = m.store.inner.borrow_mut();
            let mut ex = test_extent(extent_id, 1, 0);
            ex.replicates = vec![1, 3, 5];
            ex.sealed = true;
            ex.sealed_length = 100;
            s.extents.insert(extent_id, ex);
        }
        // No marker: the attempt was released while its executor kept working.
        let done = MgrRecoveryTaskDone {
            task: MgrRecoveryTask {
                extent_id,
                replace_id: 3,
                node_id: 9,
                start_time: 0,
            },
            ready_disk_id: 99,
        };
        run(async { m.apply_recovery_done(done).await }).expect("refusal is not an error");
        let s = m.store.inner.borrow();
        assert_eq!(
            s.extents.get(&extent_id).unwrap().replicates,
            vec![1, 3, 5],
            "a completion with no live marker must not swap the slot"
        );
    }

    /// The same completion, WITH the marker that asked for it, applies.
    #[test]
    fn recovery_completion_matching_the_marker_applies() {
        let m = AutumnManager::new();
        let extent_id = 43;
        {
            let mut s = m.store.inner.borrow_mut();
            let mut ex = test_extent(extent_id, 1, 0);
            ex.replicates = vec![1, 3, 5];
            ex.sealed = true;
            ex.sealed_length = 100;
            s.extents.insert(extent_id, ex);
        }
        let task = MgrRecoveryTask {
            extent_id,
            replace_id: 3,
            node_id: 9,
            start_time: 0,
        };
        m._test_mark_recovery_inflight(extent_id, task.clone());
        run(async {
            m.apply_recovery_done(MgrRecoveryTaskDone {
                task,
                ready_disk_id: 99,
            })
            .await
        })
        .expect("apply");
        let s = m.store.inner.borrow();
        assert!(
            s.extents.get(&extent_id).unwrap().replicates.contains(&9),
            "the pinned executor's completion must take the slot"
        );
    }

    /// R4: residue is collected by MEMBERSHIP, not by "the manager forgot this
    /// extent". A recovery that died mid-copy leaves a partial file that reloads
    /// as an ordinary extent, so the extent stays very much alive and the old
    /// existence-only predicate could never see it.
    ///
    /// And it is collected only after the verdict has HELD: the membership view
    /// is momentarily wrong in normal operation (an `apply_recovery_done` slot
    /// swap, a settling leader), and deleting real data on a transient is far
    /// worse than holding residue a few minutes longer.
    #[test]
    fn reconcile_collects_non_members_only_after_the_verdict_holds() {
        let m = AutumnManager::new();
        add_node_and_disk(&m, 7, 70);
        // Extent 5 exists and node 7 is NOT one of its members.
        {
            let mut s = m.store.inner.borrow_mut();
            let mut ex = test_extent(5, 1, 0);
            ex.replicates = vec![1, 2, 3];
            s.extents.insert(5, ex);
        }
        let ask = || -> Vec<u64> {
            let resp: ReconcileExtentsResp = rkyv_decode::<ReconcileExtentsResp>(&run(async {
                m.handle_reconcile_extents(rkyv_encode(&ReconcileExtentsReq {
                    node_id: 7,
                    node_uuid: String::new(),
                    shard_idx: 0,
                    extent_ids: vec![5],
                }))
                .await
                .unwrap()
            }))
            .unwrap();
            resp.garbage
        };
        assert!(ask().is_empty(), "round 1: too early to delete");
        assert!(ask().is_empty(), "round 2: still within the grace period");
        assert_eq!(ask(), vec![5], "round 3: the verdict has held — collect it");
    }

    /// A reporter the manager cannot identify gets NO verdict — not an empty
    /// membership.
    ///
    /// This is the difference between "you are not a member of anything" and "I
    /// don't know who you are", and it is worth a live extent: the EN does not
    /// know its own node_id, so it once reported 0. Against a membership
    /// predicate that made every extent on the node look like garbage, and
    /// because the grace counter is keyed by (node, extent), three nodes each
    /// reporting once shared ONE counter and burned the whole grace period in a
    /// single round apiece. The third node was told to delete a live extent,
    /// and did.
    #[test]
    fn reconcile_gives_no_verdict_to_an_unidentified_reporter() {
        let m = AutumnManager::new();
        {
            let mut s = m.store.inner.borrow_mut();
            let mut ex = test_extent(5, 1, 0);
            ex.replicates = vec![1, 2, 3];
            s.extents.insert(5, ex);
        }
        // Ten rounds — far past the grace period — from a caller with neither a
        // known id nor a known uuid.
        for round in 1..=10 {
            let resp: ReconcileExtentsResp = rkyv_decode::<ReconcileExtentsResp>(&run(async {
                m.handle_reconcile_extents(rkyv_encode(&ReconcileExtentsReq {
                    node_id: 0,
                    node_uuid: String::new(),
                    shard_idx: 0,
                    extent_ids: vec![5],
                }))
                .await
                .unwrap()
            }))
            .unwrap();
            assert!(
                resp.garbage.is_empty() && resp.placements.is_empty(),
                "round {round}: gave a verdict to a caller it cannot identify"
            );
        }
    }

    /// A node that knows only its UUID (which is all an EN knows — the manager
    /// assigns node ids) must still get a real answer.
    #[test]
    fn reconcile_identifies_a_reporter_by_uuid() {
        let m = AutumnManager::new();
        add_node_and_disk(&m, 7, 70);
        {
            let mut s = m.store.inner.borrow_mut();
            s.nodes.get_mut(&7).unwrap().node_uuid = "uuid-of-7".to_string();
            let mut ex = test_extent(5, 1, 0);
            ex.replicates = vec![1, 7, 3]; // node 7 IS a member, at slot 1
            s.extents.insert(5, ex);
        }
        let resp: ReconcileExtentsResp = rkyv_decode::<ReconcileExtentsResp>(&run(async {
            m.handle_reconcile_extents(rkyv_encode(&ReconcileExtentsReq {
                node_id: 0,
                node_uuid: "uuid-of-7".to_string(),
                shard_idx: 0,
                extent_ids: vec![5],
            }))
            .await
            .unwrap()
        }))
        .unwrap();
        assert!(resp.garbage.is_empty(), "a member was collected");
        assert_eq!(resp.placements.len(), 1);
        assert_eq!(resp.placements[0].extent_id, 5);
        assert_eq!(
            resp.placements[0].shard_index, 1,
            "the placement must carry this node's own slot"
        );
    }

    /// Sibling shards of one EN share a node_id and report DISJOINT extents.
    /// The grace counters are pruned to "what the reporter still holds", so
    /// without scoping that prune to the reporting shard each sibling erases the
    /// others' progress and no verdict ever reaches three rounds — the backstop
    /// silently stops collecting anything.
    #[test]
    fn sibling_shards_do_not_erase_each_others_grace() {
        let m = AutumnManager::new();
        add_node_and_disk(&m, 7, 70);
        {
            let mut s = m.store.inner.borrow_mut();
            for eid in [5u64, 6] {
                let mut ex = test_extent(eid, 1, 0);
                ex.replicates = vec![1, 2, 3]; // node 7 is a member of neither
                s.extents.insert(eid, ex);
            }
        }
        let ask = |shard: u32, eid: u64| -> Vec<u64> {
            let resp: ReconcileExtentsResp = rkyv_decode::<ReconcileExtentsResp>(&run(async {
                m.handle_reconcile_extents(rkyv_encode(&ReconcileExtentsReq {
                    node_id: 7,
                    node_uuid: String::new(),
                    shard_idx: shard,
                    extent_ids: vec![eid],
                }))
                .await
                .unwrap()
            }))
            .unwrap();
            resp.garbage
        };
        // Two shards report in an interleaved fashion, as they would in life.
        for _ in 0..2 {
            assert!(ask(0, 5).is_empty());
            assert!(ask(1, 6).is_empty());
        }
        assert_eq!(ask(0, 5), vec![5], "shard 0's grace was reset by its sibling");
        assert_eq!(ask(1, 6), vec![6], "shard 1's grace was reset by its sibling");
    }

    /// A node that IS a member must never be told to delete, no matter how many
    /// rounds pass — and the counter must reset, so a node that briefly looked
    /// like a non-member does not carry that history forever.
    #[test]
    fn reconcile_never_collects_a_member() {
        let m = AutumnManager::new();
        add_node_and_disk(&m, 7, 70);
        {
            let mut s = m.store.inner.borrow_mut();
            let mut ex = test_extent(5, 1, 0);
            ex.replicates = vec![1, 7, 3]; // node 7 IS a member
            s.extents.insert(5, ex);
        }
        for round in 1..=5 {
            let resp: ReconcileExtentsResp = rkyv_decode::<ReconcileExtentsResp>(&run(async {
                m.handle_reconcile_extents(rkyv_encode(&ReconcileExtentsReq {
                    node_id: 7,
                    node_uuid: String::new(),
                    shard_idx: 0,
                    extent_ids: vec![5],
                }))
                .await
                .unwrap()
            }))
            .unwrap();
            assert!(resp.garbage.is_empty(), "round {round}: a member was collected");
        }
    }

    /// A recovery TARGET is a non-member by construction — it is building the
    /// copy that will make it one. Listing it would delete the recovery out from
    /// under itself, so an in-flight marker suppresses collection entirely.
    #[test]
    fn reconcile_does_not_collect_an_extent_with_an_op_in_flight() {
        let m = AutumnManager::new();
        add_node_and_disk(&m, 7, 70);
        {
            let mut s = m.store.inner.borrow_mut();
            let mut ex = test_extent(5, 1, 0);
            ex.replicates = vec![1, 2, 3];
            s.extents.insert(5, ex);
        }
        run(async {
            m.acquire_extent_inflight(
                5,
                crate::extent_inflight::ExtentOpPayload::Recovery(MgrRecoveryTask {
                    extent_id: 5,
                    replace_id: 2,
                    node_id: 7,
                    start_time: 0,
                }),
            )
            .await
            .expect("acquire");
        });
        for round in 1..=5 {
            let resp: ReconcileExtentsResp = rkyv_decode::<ReconcileExtentsResp>(&run(async {
                m.handle_reconcile_extents(rkyv_encode(&ReconcileExtentsReq {
                    node_id: 7,
                    node_uuid: String::new(),
                    shard_idx: 0,
                    extent_ids: vec![5],
                }))
                .await
                .unwrap()
            }))
            .unwrap();
            assert!(
                resp.garbage.is_empty(),
                "round {round}: collected an extent with a recovery in flight"
            );
        }
    }

    #[test]
    fn promotion_seeds_ledger_from_inflight_ec_markers() {
        let m = AutumnManager::new();
        // Simulate an EC conversion in flight (as replay_from_etcd would rebuild
        // from a durable ConvertToEc marker after a leader change).
        m._test_mark_ec_inflight(88);
        m.seed_ec_ledger_from_inflight();
        // `ops list --kind ec` now shows it RUNNING under a replay op_id.
        let q: OpQueryResp = rkyv_decode(&run(async {
            m.handle_op_query(rkyv_encode(&OpQueryReq {
                kind_filter: OP_KIND_EC_CONVERT,
                ..Default::default()
            }))
            .await
            .unwrap()
        }))
        .unwrap();
        assert_eq!(q.ops.len(), 1);
        assert_eq!(q.ops[0].secondary_id, 88);
        assert_eq!(q.ops[0].state, OP_STATE_RUNNING);
        assert_eq!(q.ops[0].requested_by, "replay");
        // idempotent across a re-promotion.
        m.seed_ec_ledger_from_inflight();
        let q2: OpQueryResp = rkyv_decode(&run(async {
            m.handle_op_query(rkyv_encode(&OpQueryReq {
                kind_filter: OP_KIND_EC_CONVERT,
                ..Default::default()
            }))
            .await
            .unwrap()
        }))
        .unwrap();
        assert_eq!(q2.ops.len(), 1);
    }

    #[test]
    fn report_load_reconciles_maintenance_outcome_into_ledger() {
        let m = AutumnManager::new();
        // Seed a RUNNING gc op directly (bypassing the spawned actuation, which
        // in a bare test has no PS to reach).
        let (op_id, _) =
            m.ops
                .borrow_mut()
                .submit(OP_KIND_GC, 5, 0, vec![], "cli".to_string(), 1, 1_000_000);
        m.ops.borrow_mut().set_running(op_id, 1);
        // A heartbeat carrying the terminal outcome (as the PS would report it).
        let load = PartitionLoad {
            part_id: 5,
            maintenance_outcomes: vec![MaintenanceOutcome {
                op_id,
                kind: OP_KIND_GC,
                state: OP_STATE_FAILED,
                error: "extent 9: precondition failed".to_string(),
                message: String::new(),
                finished_at: 10,
            }],
            ..Default::default()
        };
        let req = rkyv_encode(&ReportPartitionLoadReq { ps_id: 1, partitions: vec![load] });
        run(async { m.handle_report_partition_load(req).await.unwrap() });
        // The ledger entry is now terminal with the surfaced error string.
        let q: OpQueryResp = rkyv_decode(&run(async {
            m.handle_op_query(rkyv_encode(&OpQueryReq { op_id, ..Default::default() }))
                .await
                .unwrap()
        }))
        .unwrap();
        assert_eq!(q.ops[0].state, OP_STATE_FAILED);
        assert_eq!(q.ops[0].error, "extent 9: precondition failed");
    }

    #[test]
    fn two_distinct_reporters_below_quorum_no_offline() {
        let m = AutumnManager::new();
        add_node_and_disk(&m, 7, 70);

        let r = fire_report(&m, 7, 100, 0);
        assert_eq!(r.code, CODE_OK);
        let r = fire_report(&m, 7, 101, 0);
        assert_eq!(r.code, CODE_OK);

        let s = m.store.inner.borrow();
        let disk = s.disks.get(&70).unwrap();
        assert!(
            disk.online,
            "node 7's disk must still be online below quorum"
        );
    }

    #[test]
    fn three_distinct_reporters_flips_offline() {
        let m = AutumnManager::new();
        add_node_and_disk(&m, 7, 70);

        for rp in [100u64, 101, 102] {
            let r = fire_report(&m, 7, rp, 0);
            assert_eq!(r.code, CODE_OK);
        }

        let s = m.store.inner.borrow();
        let disk = s.disks.get(&70).unwrap();
        assert!(
            !disk.online,
            "node 7's disk must be flipped offline at 3 distinct reporters"
        );
    }

    #[test]
    fn duplicate_reporter_does_not_count_toward_quorum() {
        let m = AutumnManager::new();
        add_node_and_disk(&m, 7, 70);

        // Same reporter_part_id repeated five times — must NOT count
        // toward the 3-distinct quorum. The window may grow, but
        // distinct.len() stays at 1.
        for _ in 0..5 {
            let r = fire_report(&m, 7, 100, 0);
            assert_eq!(r.code, CODE_OK);
        }

        let s = m.store.inner.borrow();
        let disk = s.disks.get(&70).unwrap();
        assert!(disk.online, "duplicate reporter must not trip quorum");
    }

    // EXTENT10-AUTORECLAIM: a both-zero orphan (refs==0 && vp_table_refs==0,
    // in no stream) must be auto-reclaimed by the sweep; a retained orphan
    // (vp_table_refs>0 — the upgrade-safety guard, see extent_can_delete) and a
    // stream member (refs>0) must be kept. Reproduces the extent-10 leak:
    // pre-sweep nothing reclaims a both-zero non-member.
    #[test]
    fn extent10_both_zero_orphan_is_auto_reclaimed_referenced_kept() {
        let m = AutumnManager::new();
        let mk = |id: u64, refs: u64, vp: u64| MgrExtentInfo {
            extent_id: id,
            replicates: vec![],
            parity: vec![],
            replicate_disks: vec![],
            parity_disks: vec![],
            sealed_length: 0,
            sealed: true,
            avali: 0,
            eversion: 0,
            refs,
            vp_table_refs: vp,
            ec_converted: false,
        };
        {
            let mut s = m.store.inner.borrow_mut();
            s.extents.insert(10, mk(10, 0, 0)); // both-zero orphan, no stream → reclaim
            s.extents.insert(11, mk(11, 0, 1)); // retained by vp_table_refs (legacy guard) → keep
            s.extents.insert(12, mk(12, 1, 0)); // stream member (refs>0) → keep
            // both-zero BUT still listed in a stream (refs under-count bug):
            // must NOT be reclaimed (coco P1 #2 — would dangle the membership).
            s.extents.insert(13, mk(13, 0, 0));
            s.streams.insert(
                500,
                MgrStreamInfo {
                    stream_id: 500,
                    extent_ids: vec![13],
                    ec_data_shard: 1,
                    ec_parity_shard: 0,
                    replicates: 3,
                },
            );
        }
        // The leak: today no path reclaims a both-zero non-member extent.
        assert!(m.store.inner.borrow().extents.contains_key(&10));

        let n = run(async { m.extent_both_zero_sweep_once().await });
        assert_eq!(n, 1, "exactly the both-zero non-member orphan must be reclaimed");

        let s = m.store.inner.borrow();
        assert!(!s.extents.contains_key(&10), "both-zero orphan must be removed");
        assert!(
            s.extents.contains_key(&11),
            "vp_table_refs>0 must be retained (upgrade-safety guard for legacy live VPs)"
        );
        assert!(s.extents.contains_key(&12), "refs>0 (stream member) must be retained");
        assert!(
            s.extents.contains_key(&13),
            "both-zero but still in a stream must NOT be reclaimed (would dangle membership)"
        );
        drop(s);
        assert!(
            m.delete_progress.borrow().contains_key(&10),
            "reclaimed orphan must be enqueued for physical delete"
        );
    }

    #[test]
    fn quorum_clears_after_trip_and_does_not_re_fire() {
        let m = AutumnManager::new();
        add_node_and_disk(&m, 7, 70);

        for rp in [100u64, 101, 102] {
            let _ = fire_report(&m, 7, rp, 0);
        }
        // After the quorum trip, the handler clears the per-node entry
        // so a stale burst of reports doesn't re-trip after the disk
        // is promoted back online externally.
        let reports = m.recent_failure_reports.borrow();
        assert!(
            reports.get(&7).is_none_or(|v| v.is_empty()),
            "quorum trip must clear recent_failure_reports for the node"
        );
    }

    #[test]
    fn register_node_duplicate_addr_rejected() {
        run(async {
            let m = AutumnManager::new();

            let req = rkyv_encode(&RegisterNodeReq {
                addr: "127.0.0.1:4001".to_string(),
                disk_uuids: vec!["d1".to_string()],
                shard_ports: vec![],
                control_address: String::new(),
                node_uuid: String::new(),
            });
            let resp = m.handle_register_node(req).await.unwrap();
            let r: RegisterNodeResp = rkyv_decode(&resp).unwrap();
            assert_eq!(r.code, CODE_OK);

            let req2 = rkyv_encode(&RegisterNodeReq {
                addr: "127.0.0.1:4001".to_string(),
                disk_uuids: vec!["d2".to_string()],
                shard_ports: vec![],
                control_address: String::new(),
                node_uuid: String::new(),
            });
            let resp2 = m.handle_register_node(req2).await.unwrap();
            let r2: RegisterNodeResp = rkyv_decode(&resp2).unwrap();
            assert_eq!(r2.code, CODE_PRECONDITION);
        })
    }

    // --- EN dynamic-shard acceptance matrix: UUID identity vs address ---

    async fn reg_node(
        m: &AutumnManager,
        uuid: &str,
        addr: &str,
        disk: &str,
        shard_ports: &[u16],
        ctrl: &str,
    ) -> RegisterNodeResp {
        let req = rkyv_encode(&RegisterNodeReq {
            addr: addr.to_string(),
            disk_uuids: vec![disk.to_string()],
            shard_ports: shard_ports.to_vec(),
            control_address: ctrl.to_string(),
            node_uuid: uuid.to_string(),
        });
        let resp = m.handle_register_node(req).await.unwrap();
        rkyv_decode(&resp).unwrap()
    }

    /// The headline M0 capability: a node keeps its `node_id` across an
    /// address change because the UUID — not the IP — is its identity. This is
    /// the k8s reschedule case (pod gets a fresh IP but the same PVC/uuid).
    #[test]
    fn en_dynshard_uuid_match_survives_address_change() {
        run(async {
            let m = AutumnManager::new();
            let r1 = reg_node(&m, "uuid-A", "10.0.0.1:9101", "disk-A", &[9101], "10.0.0.1:9100").await;
            assert_eq!(r1.code, CODE_OK);
            let nid = r1.node_id;

            // SAME uuid, DIFFERENT address + shard ports (pod rescheduled).
            let r2 = reg_node(&m, "uuid-A", "10.0.0.2:9111", "disk-A", &[9111], "10.0.0.2:9110").await;
            assert_eq!(r2.code, CODE_OK);
            assert_eq!(
                r2.node_id, nid,
                "uuid identity must map to the SAME node_id across an address change"
            );
            let s = m.store.inner.borrow();
            let n = &s.nodes[&nid];
            assert_eq!(n.address, "10.0.0.2:9111", "routing address follows the uuid");
            assert_eq!(n.shard_ports, vec![9111u16], "shard-port layout updates in place");
            assert_eq!(n.control_address, "10.0.0.2:9110");
            assert_eq!(n.node_uuid, "uuid-A");
        })
    }

    /// A pre-M0 (uuid-less) node re-registering WITH a uuid at the same
    /// address ADOPTS that uuid; afterwards the uuid alone (any address)
    /// resolves to the same node — the legacy→identity migration path.
    #[test]
    fn en_dynshard_legacy_address_node_adopts_uuid() {
        run(async {
            let m = AutumnManager::new();
            let r1 = reg_node(&m, "", "10.0.0.1:9101", "disk-A", &[9101], "").await;
            assert_eq!(r1.code, CODE_OK);
            let nid = r1.node_id;
            assert!(m.store.inner.borrow().nodes[&nid].node_uuid.is_empty());

            // Restart WITH a uuid at the same address → adopt (no new node).
            let r2 = reg_node(&m, "uuid-A", "10.0.0.1:9101", "disk-A", &[9101], "").await;
            assert_eq!(r2.code, CODE_OK);
            assert_eq!(r2.node_id, nid);
            assert_eq!(m.store.inner.borrow().nodes[&nid].node_uuid, "uuid-A");

            // The uuid is now the stable key — resolves even at a fresh address.
            let r3 = reg_node(&m, "uuid-A", "10.9.9.9:9101", "disk-A", &[9101], "").await;
            assert_eq!(r3.node_id, nid, "after adoption the uuid outranks the address");
        })
    }

    /// An identity-only re-register (uuid + EMPTY addr/ports/ctrl — the M1
    /// `format` re-stamp shape) is idempotent and must NEVER clobber ANY live
    /// routing metadata: empty ports/ctrl mean "unspecified", NOT "clear them".
    #[test]
    fn en_dynshard_identity_only_reregister_preserves_location() {
        run(async {
            let m = AutumnManager::new();
            let r1 =
                reg_node(&m, "uuid-A", "10.0.0.1:9101", "disk-A", &[9101, 9102], "10.0.0.1:10101")
                    .await;
            let nid = r1.node_id;

            // uuid + empty addr + empty ports + empty ctrl.
            let r2 = reg_node(&m, "uuid-A", "", "disk-A", &[], "").await;
            assert_eq!(r2.code, CODE_OK);
            assert_eq!(r2.node_id, nid);
            let s = m.store.inner.borrow();
            let n = &s.nodes[&nid];
            assert_eq!(n.address, "10.0.0.1:9101", "empty addr must not clobber address");
            assert_eq!(
                n.shard_ports,
                vec![9101u16, 9102],
                "empty ports must not clobber shard routing"
            );
            assert_eq!(
                n.control_address, "10.0.0.1:10101",
                "empty ctrl must not clobber the control address"
            );
        })
    }

    /// The decommission tombstone travels with the UUID, not the IP, and
    /// survives the node-record deletion that a REAL `remove_node` performs: a
    /// removed node returning under its OWN uuid at a FRESH address is refused.
    /// Clearing the tombstone lets it rejoin.
    #[test]
    fn en_dynshard_decommissioned_uuid_refused_at_new_address() {
        run(async {
            let m = AutumnManager::new();
            let r1 = reg_node(&m, "uuid-A", "10.0.0.1:9101", "disk-A", &[9101], "").await;
            let nid = r1.node_id;

            // Reproduce the state a real `remove_node` leaves: the node record is
            // GONE from `s.nodes`; the `decommissioned` tombstone carries the uuid.
            m.store.inner.borrow_mut().nodes.remove(&nid);
            m.decommissioned.borrow_mut().insert(
                nid,
                MgrNodeOverride {
                    node_id: nid,
                    kind: NODE_OVERRIDE_FENCED,
                    node_uuid: "uuid-A".to_string(),
                    ..Default::default()
                },
            );

            let r2 = reg_node(&m, "uuid-A", "10.0.0.7:9101", "disk-A", &[9101], "").await;
            assert_eq!(
                r2.code, CODE_PRECONDITION,
                "tombstone is keyed by uuid and survives node deletion — a new IP must not launder it"
            );

            // Operator lifts the tombstone → the node may rejoin.
            m.decommissioned.borrow_mut().remove(&nid);
            let r3 = reg_node(&m, "uuid-A", "10.0.0.7:9101", "disk-A", &[9101], "").await;
            assert_eq!(r3.code, CODE_OK, "clearing the tombstone re-admits the uuid");
        })
    }

    /// The inverse: after the old node is fence+REMOVED (gone from `s.nodes`,
    /// its address freed), a BRAND-NEW node (fresh uuid) landing on the recycled
    /// pod IP is accepted as a new node.
    #[test]
    fn en_dynshard_recycled_ip_under_fresh_uuid_accepted() {
        run(async {
            let m = AutumnManager::new();
            let r1 = reg_node(&m, "uuid-A", "10.0.0.1:9101", "disk-A", &[9101], "").await;
            let nid = r1.node_id;
            // Real recycle: the old record is removed (address freed) first.
            m.store.inner.borrow_mut().nodes.remove(&nid);
            m.decommissioned.borrow_mut().insert(
                nid,
                MgrNodeOverride {
                    node_id: nid,
                    kind: NODE_OVERRIDE_FENCED,
                    node_uuid: "uuid-A".to_string(),
                    ..Default::default()
                },
            );

            let r2 = reg_node(&m, "uuid-B", "10.0.0.1:9101", "disk-B", &[9101], "").await;
            assert_eq!(
                r2.code, CODE_OK,
                "a fresh uuid on a freed recycled IP is a new node, not the tombstoned one"
            );
            assert_ne!(r2.node_id, nid);
        })
    }

    /// A DIFFERENT uuid must NOT create a second node record at an address a
    /// live node already holds (a lost/duplicated `node_uuid` file, or a
    /// misconfigured second process) — two records at one address would make one
    /// physical EN two failure domains (RF double-placement). The node's OWN
    /// uuid re-registering at its own address stays fine.
    #[test]
    fn en_dynshard_duplicate_address_different_uuid_refused() {
        run(async {
            let m = AutumnManager::new();
            let r1 = reg_node(&m, "uuid-A", "10.0.0.1:9101", "disk-A", &[9101], "").await;
            let nid = r1.node_id;

            let r2 = reg_node(&m, "uuid-B", "10.0.0.1:9101", "disk-B", &[9101], "").await;
            assert_eq!(
                r2.code, CODE_PRECONDITION,
                "one address may not host two node records under different uuids"
            );

            let r3 = reg_node(&m, "uuid-A", "10.0.0.1:9101", "disk-A", &[9101], "").await;
            assert_eq!(r3.code, CODE_OK, "the address holder re-registering is fine");
            assert_eq!(r3.node_id, nid);
        })
    }

    #[test]
    fn partition_region_rebalance() {
        run(async {
            let m = AutumnManager::new();
            let req = rkyv_encode(&RegisterPsReq {
                ps_id: 11,
                address: "127.0.0.1:9955".to_string(),
            });
            let resp = m.handle_register_ps(req).await.unwrap();
            let r: CodeResp = rkyv_decode(&resp).unwrap();
            assert_eq!(r.code, CODE_OK);

            let req = rkyv_encode(&UpsertPartitionReq {
                meta: MgrPartitionMeta {
                    log_stream: 1,
                    row_stream: 2,
                    meta_stream: 3,
                    part_id: 101,
                    rg: Some(MgrRange {
                        start_key: b"a".to_vec(),
                        end_key: b"z".to_vec(),
                    }),
                },
            });
            let resp = m.handle_upsert_partition(req).await.unwrap();
            let r: CodeResp = rkyv_decode(&resp).unwrap();
            assert_eq!(r.code, CODE_OK);

            let resp = m.handle_get_regions().await.unwrap();
            let r: GetRegionsResp = rkyv_decode(&resp).unwrap();
            assert_eq!(r.code, CODE_OK);
            assert_eq!(r.regions.len(), 1);
        })
    }

    #[test]
    fn least_loaded_allocation() {
        run(async {
            let m = AutumnManager::new();

            for ps_id in [10u64, 20u64] {
                let req = rkyv_encode(&RegisterPsReq {
                    ps_id,
                    address: format!("127.0.0.1:999{ps_id}"),
                });
                let resp = m.handle_register_ps(req).await.unwrap();
                let r: CodeResp = rkyv_decode(&resp).unwrap();
                assert_eq!(r.code, CODE_OK);
            }

            for (part_id, start, end) in [
                (1u64, b"a" as &[u8], b"e" as &[u8]),
                (2, b"e", b"j"),
                (3, b"j", b"n"),
                (4, b"n", b"z"),
            ] {
                let req = rkyv_encode(&UpsertPartitionReq {
                    meta: MgrPartitionMeta {
                        log_stream: part_id,
                        row_stream: part_id + 100,
                        meta_stream: part_id + 200,
                        part_id,
                        rg: Some(MgrRange {
                            start_key: start.to_vec(),
                            end_key: end.to_vec(),
                        }),
                    },
                });
                let resp = m.handle_upsert_partition(req).await.unwrap();
                let r: CodeResp = rkyv_decode(&resp).unwrap();
                assert_eq!(r.code, CODE_OK);
            }

            let resp = m.handle_get_regions().await.unwrap();
            let regions: GetRegionsResp = rkyv_decode(&resp).unwrap();
            assert_eq!(regions.regions.len(), 4);

            let mut counts: HashMap<u64, usize> = HashMap::new();
            for (_, r) in &regions.regions {
                *counts.entry(r.ps_id).or_insert(0) += 1;
            }
            assert_eq!(*counts.get(&10).unwrap_or(&0), 2);
            assert_eq!(*counts.get(&20).unwrap_or(&0), 2);
        })
    }

    #[test]
    fn ps_eviction_reassigns_regions() {
        run(async {
            let m = AutumnManager::new();

            for (ps_id, addr) in [(1u64, "ps1:9001"), (2, "ps2:9002")] {
                let req = rkyv_encode(&RegisterPsReq {
                    ps_id,
                    address: addr.to_string(),
                });
                m.handle_register_ps(req).await.unwrap();
            }

            for (part_id, start, end) in [(101u64, b"a" as &[u8], b"m" as &[u8]), (102, b"m", b"")]
            {
                let req = rkyv_encode(&UpsertPartitionReq {
                    meta: MgrPartitionMeta {
                        log_stream: part_id,
                        row_stream: part_id + 100,
                        meta_stream: part_id + 200,
                        part_id,
                        rg: Some(MgrRange {
                            start_key: start.to_vec(),
                            end_key: end.to_vec(),
                        }),
                    },
                });
                m.handle_upsert_partition(req).await.unwrap();
            }

            {
                let s = m.store.inner.borrow();
                let ps1 = s.regions.values().filter(|r| r.ps_id == 1).count();
                let ps2 = s.regions.values().filter(|r| r.ps_id == 2).count();
                assert_eq!(ps1, 1);
                assert_eq!(ps2, 1);
            }

            {
                let mut s = m.store.inner.borrow_mut();
                s.ps_nodes.remove(&1);
                AutumnManager::rebalance_regions(&mut s);
            }

            let s = m.store.inner.borrow();
            for r in s.regions.values() {
                assert_eq!(r.ps_id, 2);
            }
        })
    }

    #[test]
    fn heartbeat_updates_timestamp() {
        run(async {
            let m = AutumnManager::new();
            let req = rkyv_encode(&RegisterPsReq {
                ps_id: 55,
                address: "ps55:9055".to_string(),
            });
            m.handle_register_ps(req).await.unwrap();

            compio::time::sleep(Duration::from_millis(10)).await;

            let req = rkyv_encode(&HeartbeatPsReq { ps_id: 55 });
            m.handle_heartbeat_ps(req).await.unwrap();

            let hb = m.ps_last_heartbeat.borrow();
            let recorded = hb.get(&55).expect("timestamp recorded");
            assert!(recorded.elapsed() < Duration::from_millis(500));
        })
    }

    // (removed: partition_vp_refs_diff_updates_extent_counters — the
    // vp_table_refs maintenance path it exercised was deleted; extent
    // retention is now `refs`-only.)

    #[test]
    fn compute_merge_streams_extent_ids_order_and_refs() {
        let mut state = autumn_common::MetadataState::default();
        let mk = |id: u64, refs: u64, sealed: u64| MgrExtentInfo {
            extent_id: id,
            replicates: vec![1],
            parity: vec![],
            replicate_disks: vec![1],
            parity_disks: vec![],
            sealed_length: sealed,
            sealed: sealed > 0,
            avali: 1,
            eversion: 0,
            refs,
            vp_table_refs: 0,
            ec_converted: false,
        };
        state.extents.insert(10, mk(10, 1, 1024));
        state.extents.insert(11, mk(11, 1, 0));
        state.streams.insert(
            100,
            MgrStreamInfo {
                stream_id: 100,
                extent_ids: vec![10, 11],
                ec_data_shard: 1,
                ec_parity_shard: 0,
                replicates: 3,
            },
        );
        state.extents.insert(20, mk(20, 1, 2048));
        state.extents.insert(21, mk(21, 1, 0));
        state.streams.insert(
            200,
            MgrStreamInfo {
                stream_id: 200,
                extent_ids: vec![20, 21],
                ec_data_shard: 1,
                ec_parity_shard: 0,
                replicates: 3,
            },
        );
        let new_tail = mk(99, 1, 0);

        let (updated, modified) =
            AutumnManager::compute_merge_streams(&state, 100, 200, 4096, 8192, new_tail.clone())
                .unwrap();

        assert_eq!(updated.extent_ids, vec![10, 11, 20, 21, 99]);
        assert_eq!(updated.stream_id, 100);

        let e11 = modified.iter().find(|e| e.extent_id == 11).unwrap();
        assert_eq!(e11.sealed_length, 4096);
        assert_eq!(e11.refs, 1);

        let e10 = modified.iter().find(|e| e.extent_id == 10);
        assert!(
            e10.is_none(),
            "non-tail survivor extent unchanged → not in modified"
        );

        // Merge refs accounting: victim-only extents transfer victim→survivor, so
        // after the merge they are in exactly ONE stream → refs stays 1 (was
        // asserted as 2, which baked in the +1 leak that orphaned extents).
        let e20 = modified.iter().find(|e| e.extent_id == 20).unwrap();
        assert_eq!(e20.refs, 1);
        assert_eq!(e20.sealed_length, 2048);

        let e21 = modified.iter().find(|e| e.extent_id == 21).unwrap();
        assert_eq!(e21.refs, 1);
        assert_eq!(e21.sealed_length, 8192);

        let e99 = modified.iter().find(|e| e.extent_id == 99).unwrap();
        assert_eq!(e99.sealed_length, 0);
        assert_eq!(e99.refs, 1);
    }

    #[test]
    fn splice_streams_without_new_tail_no_e_new() {
        let mut state = autumn_common::MetadataState::default();
        let mk = |id: u64, refs: u64| MgrExtentInfo {
            extent_id: id,
            replicates: vec![1],
            parity: vec![],
            replicate_disks: vec![1],
            parity_disks: vec![],
            sealed_length: 0,
            sealed: false,
            avali: 1,
            eversion: 0,
            refs,
            vp_table_refs: 0,
            ec_converted: false,
        };
        state.extents.insert(30, mk(30, 1));
        state.extents.insert(40, mk(40, 1));
        state.streams.insert(
            300,
            MgrStreamInfo {
                stream_id: 300,
                extent_ids: vec![30],
                ec_data_shard: 1,
                ec_parity_shard: 0,
                replicates: 3,
            },
        );
        state.streams.insert(
            400,
            MgrStreamInfo {
                stream_id: 400,
                extent_ids: vec![40],
                ec_data_shard: 1,
                ec_parity_shard: 0,
                replicates: 3,
            },
        );
        let (updated, modified) =
            AutumnManager::splice_streams_without_new_tail(&state, 300, 400, 100, 200).unwrap();
        assert_eq!(updated.extent_ids, vec![30, 40]);
        // Merge refs accounting: victim-only extent transfers victim→survivor →
        // refs stays 1 (was asserted as 2 = the leak).
        let e40 = modified.iter().find(|e| e.extent_id == 40).unwrap();
        assert_eq!(e40.refs, 1);
        assert_eq!(e40.sealed_length, 200);
    }

    /// Merge refs-leak regression: merging back a split (survivor + victim
    /// CoW-share the pre-split extents) must NOT leak refs and must NOT list a
    /// shared extent twice. Pre-fix the shared extent got refs += 1 AND a
    /// duplicate entry in extent_ids; over repeated split→merge cycles it drove
    /// extents to refs>0 with zero stream membership (invisible orphans).
    #[test]
    fn merge_refs_leak_cow_shared_extent_dedup_and_refs() {
        let mut state = autumn_common::MetadataState::default();
        let mk = |id: u64, refs: u64, sealed: u64| MgrExtentInfo {
            extent_id: id,
            replicates: vec![1],
            parity: vec![],
            replicate_disks: vec![1],
            parity_disks: vec![],
            sealed_length: sealed,
            sealed: sealed > 0,
            avali: 1,
            eversion: 0,
            refs,
            vp_table_refs: 0,
            ec_converted: false,
        };
        // Shared ancestor extent 10 is CoW-shared by both children of a prior
        // split → refs=2. Each child also has its own unique tail (50 / 60).
        state.extents.insert(10, mk(10, 2, 1024));
        state.extents.insert(50, mk(50, 1, 0)); // survivor tail (open)
        state.extents.insert(60, mk(60, 1, 0)); // victim tail (open)
        state.streams.insert(
            100,
            MgrStreamInfo {
                stream_id: 100,
                extent_ids: vec![10, 50],
                ec_data_shard: 1,
                ec_parity_shard: 0,
                replicates: 3,
            },
        );
        state.streams.insert(
            200,
            MgrStreamInfo {
                stream_id: 200,
                extent_ids: vec![10, 60],
                ec_data_shard: 1,
                ec_parity_shard: 0,
                replicates: 3,
            },
        );
        let new_tail = mk(99, 1, 0);

        let (updated, modified) =
            AutumnManager::compute_merge_streams(&state, 100, 200, 4096, 8192, new_tail)
                .unwrap();

        // Shared extent 10 listed ONCE (front), not duplicated.
        assert_eq!(updated.extent_ids, vec![10, 50, 60, 99]);

        // Shared extent 10: two memberships {100, 200} collapse to one {100} →
        // refs 2 → 1.
        let e10 = modified.iter().find(|e| e.extent_id == 10).unwrap();
        assert_eq!(e10.refs, 1, "shared extent must drop one membership");

        // Victim-only tail 60: transfers victim→survivor → refs unchanged (1),
        // sealed at victim_sealed.
        let e60 = modified.iter().find(|e| e.extent_id == 60).unwrap();
        assert_eq!(e60.refs, 1);
        assert_eq!(e60.sealed_length, 8192);

        // Survivor tail 50: sealed at survivor_sealed, refs unchanged (1).
        let e50 = modified.iter().find(|e| e.extent_id == 50).unwrap();
        assert_eq!(e50.refs, 1);
        assert_eq!(e50.sealed_length, 4096);

        // INVARIANT: after merge, every extent's refs == the number of streams
        // (here only the survivor) whose extent_ids list it, each at most once.
        let occ = updated.extent_ids.iter().filter(|&&e| e == 10).count();
        assert_eq!(occ, 1, "no duplicate listing → GC can reconcile refs to 0");
    }

    /// Merge refs-leak regression for the row/meta splice path (no new tail).
    #[test]
    fn merge_refs_leak_splice_cow_shared_extent_dedup_and_refs() {
        let mut state = autumn_common::MetadataState::default();
        let mk = |id: u64, refs: u64| MgrExtentInfo {
            extent_id: id,
            replicates: vec![1],
            parity: vec![],
            replicate_disks: vec![1],
            parity_disks: vec![],
            sealed_length: 0,
            sealed: false,
            avali: 1,
            eversion: 0,
            refs,
            vp_table_refs: 0,
            ec_converted: false,
        };
        state.extents.insert(30, mk(30, 2)); // CoW-shared by both
        state.extents.insert(31, mk(31, 1)); // survivor-only tail
        state.extents.insert(41, mk(41, 1)); // victim-only tail
        state.streams.insert(
            300,
            MgrStreamInfo {
                stream_id: 300,
                extent_ids: vec![30, 31],
                ec_data_shard: 1,
                ec_parity_shard: 0,
                replicates: 3,
            },
        );
        state.streams.insert(
            400,
            MgrStreamInfo {
                stream_id: 400,
                extent_ids: vec![30, 41],
                ec_data_shard: 1,
                ec_parity_shard: 0,
                replicates: 3,
            },
        );
        let (updated, modified) =
            AutumnManager::splice_streams_without_new_tail(&state, 300, 400, 100, 200).unwrap();
        assert_eq!(updated.extent_ids, vec![30, 31, 41]);
        let e30 = modified.iter().find(|e| e.extent_id == 30).unwrap();
        assert_eq!(e30.refs, 1, "shared extent drops one membership");
        let e41 = modified.iter().find(|e| e.extent_id == 41).unwrap();
        assert_eq!(e41.refs, 1);
        assert_eq!(e41.sealed_length, 200);
    }

    // MERGE-REFS-RECOMPUTE reproduction attempt (2026-06-18): the
    // `refs.saturating_sub(1)` in compute_merge_streams is only dangerous if
    // `refs` is ALREADY under-counted. This drives realistic split→merge-back
    // cycles (2-way repeated, then 3-way) through the real compute+apply fns and
    // asserts `refs == #streams listing the extent` after EVERY step. If the
    // invariant holds, saturating_sub never fires on an under-counted value →
    // no reproducible harm → MERGE-REFS-RECOMPUTE is belt-and-braces, defer.
    // (Also a permanent regression guard against refs/membership drift.)
    #[test]
    fn merge_refs_invariant_holds_across_split_merge_cycles() {
        fn check(state: &autumn_common::MetadataState, label: &str) {
            for (eid, ex) in &state.extents {
                let mem = state
                    .streams
                    .values()
                    .filter(|s| s.extent_ids.contains(eid))
                    .count() as u64;
                assert_eq!(
                    ex.refs, mem,
                    "{label}: extent {eid} refs={} != stream membership={}",
                    ex.refs, mem
                );
            }
        }
        let mk = |id: u64, refs: u64, sealed: u64| MgrExtentInfo {
            extent_id: id,
            replicates: vec![1],
            parity: vec![],
            replicate_disks: vec![1],
            parity_disks: vec![],
            sealed_length: sealed,
            sealed: sealed > 0,
            avali: 1,
            eversion: 0,
            refs,
            vp_table_refs: 0,
            ec_converted: false,
        };

        let mut state = autumn_common::MetadataState::default();
        state.extents.insert(10, mk(10, 1, 1024)); // shared ancestor
        state.extents.insert(50, mk(50, 1, 0)); // tail
        state.streams.insert(
            100,
            MgrStreamInfo {
                stream_id: 100,
                extent_ids: vec![10, 50],
                ec_data_shard: 1,
                ec_parity_shard: 0,
                replicates: 3,
            },
        );
        check(&state, "init");

        let split = |state: &mut autumn_common::MetadataState, src: u64, dst: u64| {
            let (dst_stream, modified) =
                AutumnManager::compute_duplicate_stream(state, src, dst, 1024).unwrap();
            for ex in &modified {
                state.extents.insert(ex.extent_id, ex.clone());
            }
            state.streams.insert(dst, dst_stream);
        };
        let merge = |state: &mut autumn_common::MetadataState, surv: u64, vic: u64, tail: u64| {
            let new_tail = mk(tail, 1, 0);
            let (updated, modified) =
                AutumnManager::compute_merge_streams(state, surv, vic, 4096, 8192, new_tail)
                    .unwrap();
            for ex in &modified {
                state.extents.insert(ex.extent_id, ex.clone());
            }
            state.streams.insert(surv, updated);
            state.streams.remove(&vic);
        };

        // 5 rounds of 2-way split→merge-back: refs(10) goes 1→2→1 each round.
        let mut dst = 200u64;
        let mut tail = 90u64;
        for _ in 0..5 {
            split(&mut state, 100, dst);
            check(&state, "after split");
            merge(&mut state, 100, dst, tail);
            check(&state, "after merge-back");
            dst += 1;
            tail += 1;
        }

        // 3-way: split 100 twice (→ shared refs=3), merge both back one at a time.
        split(&mut state, 100, 700);
        check(&state, "3way split #1");
        split(&mut state, 100, 701);
        check(&state, "3way split #2");
        merge(&mut state, 100, 700, 95);
        check(&state, "3way merge #1");
        merge(&mut state, 100, 701, 96);
        check(&state, "3way merge #2");

        // Final: the shared ancestor 10 is back to exactly one membership.
        assert_eq!(state.extents.get(&10).unwrap().refs, 1);
        assert_eq!(
            state.streams.values().filter(|s| s.extent_ids.contains(&10)).count(),
            1
        );
    }

    #[test]
    fn apply_merge_mutations_drops_victim_entries() {
        let mut state = autumn_common::MetadataState::default();
        // Survivor partition 1 with streams 100/101/102
        state.partitions.insert(
            1,
            MgrPartitionMeta {
                part_id: 1,
                log_stream: 100,
                row_stream: 101,
                meta_stream: 102,
                rg: Some(MgrRange {
                    start_key: b"a".to_vec(),
                    end_key: b"m".to_vec(),
                }),
            },
        );
        // Victim partition 2 with streams 200/201/202
        state.partitions.insert(
            2,
            MgrPartitionMeta {
                part_id: 2,
                log_stream: 200,
                row_stream: 201,
                meta_stream: 202,
                rg: Some(MgrRange {
                    start_key: b"m".to_vec(),
                    end_key: b"z".to_vec(),
                }),
            },
        );
        for sid in [100, 101, 102, 200, 201, 202] {
            state.streams.insert(
                sid,
                MgrStreamInfo {
                    stream_id: sid,
                    extent_ids: vec![],
                    ec_data_shard: 1,
                    ec_parity_shard: 0,
                    replicates: 3,
                },
            );
        }
        let new_survivor_meta = MgrPartitionMeta {
            part_id: 1,
            log_stream: 100,
            row_stream: 101,
            meta_stream: 102,
            rg: Some(MgrRange {
                start_key: b"a".to_vec(),
                end_key: b"z".to_vec(),
            }),
        };

        AutumnManager::apply_merge_mutations(
            &mut state,
            &[],
            &[],
            new_survivor_meta,
            2,
            200,
            201,
            202,
        );

        assert!(state.partitions.contains_key(&1));
        assert!(!state.partitions.contains_key(&2));
        assert!(state.streams.contains_key(&100));
        assert!(!state.streams.contains_key(&200));
        assert!(!state.streams.contains_key(&201));
        assert!(!state.streams.contains_key(&202));
        assert_eq!(
            state
                .partitions
                .get(&1)
                .unwrap()
                .rg
                .as_ref()
                .unwrap()
                .end_key,
            b"z".to_vec()
        );
    }

    // (removed: merged_partition_vp_refs_sums_per_extent and
    // split_partition_vp_snapshot_clones_parent_refs — both exercised the
    // deleted partition_vp_refs maintenance fns.)

    #[test]
    fn compute_region_keeps_existing_ps_for_left_partition() {
        let mut state = autumn_common::MetadataState::default();
        state.ps_nodes.insert(10, "ps10:9001".to_string());
        state.ps_nodes.insert(20, "ps20:9002".to_string());

        // Pre-existing region: part 101 is on ps 10
        state.regions.insert(
            101,
            MgrRegionInfo {
                rg: Some(MgrRange {
                    start_key: b"a".to_vec(),
                    end_key: b"z".to_vec(),
                }),
                part_id: 101,
                ps_id: 10,
                log_stream: 1,
                row_stream: 2,
                meta_stream: 3,
                region_epoch: 1,
            },
        );

        let left = MgrPartitionMeta {
            part_id: 101,
            log_stream: 1,
            row_stream: 2,
            meta_stream: 3,
            rg: Some(MgrRange {
                start_key: b"a".to_vec(),
                end_key: b"m".to_vec(),
            }),
        };

        let region = AutumnManager::compute_region_for_partition(&state, &left);
        assert_eq!(
            region.ps_id, 10,
            "left partition should keep its existing PS"
        );
        assert_eq!(region.part_id, 101);
        assert_eq!(region.rg.as_ref().unwrap().end_key, b"m".to_vec());
    }

    #[test]
    fn compute_region_assigns_least_loaded_for_new_partition() {
        let mut state = autumn_common::MetadataState::default();
        state.ps_nodes.insert(10, "ps10:9001".to_string());
        state.ps_nodes.insert(20, "ps20:9002".to_string());

        // ps 10 already has 2 regions, ps 20 has 0
        for part_id in [101, 102] {
            state.regions.insert(
                part_id,
                MgrRegionInfo {
                    rg: Some(MgrRange {
                        start_key: vec![],
                        end_key: vec![],
                    }),
                    part_id,
                    ps_id: 10,
                    log_stream: part_id,
                    row_stream: part_id + 100,
                    meta_stream: part_id + 200,
                    region_epoch: 1,
                },
            );
        }

        // New partition (right child from split)
        let right = MgrPartitionMeta {
            part_id: 999,
            log_stream: 50,
            row_stream: 51,
            meta_stream: 52,
            rg: Some(MgrRange {
                start_key: b"m".to_vec(),
                end_key: b"z".to_vec(),
            }),
        };

        let region = AutumnManager::compute_region_for_partition(&state, &right);
        assert_eq!(
            region.ps_id, 20,
            "new partition should go to least-loaded PS (ps 20 has 0 regions)"
        );
        assert_eq!(region.part_id, 999);
    }

    // ── region rebalance: compute_rebalance_moves ────────────────────────────

    /// Build a state with `ps_ids` registered and `assignments` = (part_id, ps_id).
    fn rebal_state(ps_ids: &[u64], assignments: &[(u64, u64)]) -> autumn_common::MetadataState {
        let mut state = autumn_common::MetadataState::default();
        for &id in ps_ids {
            state.ps_nodes.insert(id, format!("ps{id}:9001"));
        }
        for &(part_id, ps_id) in assignments {
            state.regions.insert(
                part_id,
                MgrRegionInfo {
                    rg: Some(MgrRange {
                        start_key: vec![],
                        end_key: vec![],
                    }),
                    part_id,
                    ps_id,
                    log_stream: part_id,
                    row_stream: part_id + 1000,
                    meta_stream: part_id + 2000,
                    region_epoch: 1,
                },
            );
        }
        state
    }

    /// Apply the moves the way the handler does, then return per-PS counts.
    fn counts_after(
        state: &autumn_common::MetadataState,
        moves: &[RebalanceMove],
    ) -> std::collections::BTreeMap<u64, usize> {
        let mut regions = state.regions.clone();
        for m in moves {
            regions.get_mut(&m.part_id).unwrap().ps_id = m.to_ps;
        }
        let mut c: std::collections::BTreeMap<u64, usize> =
            state.ps_nodes.keys().map(|&id| (id, 0)).collect();
        for r in regions.values() {
            *c.get_mut(&r.ps_id).unwrap() += 1;
        }
        c
    }

    #[test]
    fn rebalance_spreads_all_on_one_ps_evenly() {
        // The live-cluster symptom: 32 partitions all on ps 3, ps 1/2 idle.
        let assignments: Vec<(u64, u64)> = (100..132).map(|p| (p, 3)).collect();
        let state = rebal_state(&[1, 2, 3], &assignments);
        let moves = AutumnManager::compute_rebalance_moves(&state, 0);
        let c = counts_after(&state, &moves);
        // 32 / 3 = 10,11,11 — max-min gap must be <= 1.
        let max = *c.values().max().unwrap();
        let min = *c.values().min().unwrap();
        assert!(max - min <= 1, "not balanced: {c:?}");
        assert_eq!(c.values().sum::<usize>(), 32);
        // Every move is off the overloaded ps 3 onto a lighter PS.
        assert!(moves.iter().all(|m| m.from_ps == 3 && m.to_ps != 3));
    }

    #[test]
    fn rebalance_respects_max_moves_cap() {
        let assignments: Vec<(u64, u64)> = (100..132).map(|p| (p, 3)).collect();
        let state = rebal_state(&[1, 2, 3], &assignments);
        let moves = AutumnManager::compute_rebalance_moves(&state, 5);
        assert_eq!(moves.len(), 5, "capped at max_moves");
    }

    #[test]
    fn rebalance_noop_when_already_balanced() {
        // 11/11/10 across ps 1/2/3 — gap already 1, nothing to do.
        let mut assignments = Vec::new();
        for (i, p) in (100..132).enumerate() {
            assignments.push((p, [1u64, 2, 3][i % 3]));
        }
        let state = rebal_state(&[1, 2, 3], &assignments);
        let moves = AutumnManager::compute_rebalance_moves(&state, 0);
        assert!(moves.is_empty(), "already balanced, got {moves:?}");
    }

    #[test]
    fn rebalance_is_deterministic() {
        let assignments: Vec<(u64, u64)> = (100..132).map(|p| (p, 3)).collect();
        let state = rebal_state(&[1, 2, 3], &assignments);
        let a = AutumnManager::compute_rebalance_moves(&state, 0);
        let b = AutumnManager::compute_rebalance_moves(&state, 0);
        assert_eq!(
            a.iter().map(|m| (m.part_id, m.from_ps, m.to_ps)).collect::<Vec<_>>(),
            b.iter().map(|m| (m.part_id, m.from_ps, m.to_ps)).collect::<Vec<_>>(),
            "dry-run must match the applied set"
        );
    }

    #[test]
    fn rebalance_single_ps_is_noop() {
        let assignments: Vec<(u64, u64)> = (100..110).map(|p| (p, 1)).collect();
        let state = rebal_state(&[1], &assignments);
        assert!(AutumnManager::compute_rebalance_moves(&state, 0).is_empty());
    }

    // (removed: merge_extent_updates_preserves_ref_and_vp_changes —
    // merge_extent_updates was deleted with the vp_table_refs machinery.)

    /// handle_stream_alloc_extent must not modify the in-memory store
    /// when the handler fails partway through. When alloc_extent_on_node
    /// fails (no running extent nodes), the store must remain unchanged.
    /// Previously the handler mutated the store before the etcd mirror, so
    /// any early return left stale mutations behind.
    #[test]
    fn alloc_extent_no_store_mutation_on_failure() {
        run(async {
            let m = AutumnManager::new();

            // Register nodes (unreachable — no actual servers).
            for (nid, addr) in [
                (1, "127.0.0.1:4001"),
                (2, "127.0.0.1:4002"),
                (3, "127.0.0.1:4003"),
            ] {
                let req = rkyv_encode(&RegisterNodeReq {
                    addr: addr.to_string(),
                    disk_uuids: vec![format!("disk-{nid}")],
                    shard_ports: vec![],
                    control_address: String::new(),
                    node_uuid: String::new(),
                });
                let resp = m.handle_register_node(req).await.unwrap();
                let r: RegisterNodeResp = rkyv_decode(&resp).unwrap();
                assert_eq!(r.code, CODE_OK, "register node {nid}");
            }

            let owner_key = "test-owner-f125".to_string();
            let rev = {
                let req = rkyv_encode(&AcquireOwnerLockReq {
                    owner_key: owner_key.clone(),
                });
                let resp = m.handle_acquire_owner_lock(req).await.unwrap();
                let r: AcquireOwnerLockResp = rkyv_decode(&resp).unwrap();
                assert_eq!(r.code, CODE_OK);
                r.owner_epoch
            };

            // Seed stream + tail in store (nodes not actually running).
            let stream_id;
            let tail_id;
            {
                let mut s = m.store.inner.borrow_mut();
                let (sid, _) = s.alloc_ids(1);
                stream_id = sid;
                let (eid, _) = s.alloc_ids(1);
                tail_id = eid;
                s.streams.insert(
                    stream_id,
                    MgrStreamInfo {
                        stream_id,
                        extent_ids: vec![tail_id],
                        ec_data_shard: 0,
                        ec_parity_shard: 0,
                        replicates: 3,
                    },
                );
                s.extents.insert(
                    tail_id,
                    MgrExtentInfo {
                        extent_id: tail_id,
                        replicates: vec![1, 2, 3],
                        parity: vec![],
                        eversion: 1,
                        refs: 1,
                        vp_table_refs: 0,
                        sealed_length: 0,
                        sealed: false,
                        avali: 0,
                        replicate_disks: vec![1, 2, 3],
                        parity_disks: vec![],
                        ec_converted: false,
                    },
                );
            }

            // Snapshot before.
            let tail_before = m
                .store
                .inner
                .borrow()
                .extents
                .get(&tail_id)
                .cloned()
                .unwrap();
            let stream_before = m
                .store
                .inner
                .borrow()
                .streams
                .get(&stream_id)
                .cloned()
                .unwrap();

            // Call alloc_extent with end=100 — nodes unreachable, so the
            // handler returns a precondition error after failing to allocate.
            let req = rkyv_encode(&StreamAllocExtentReq {
                stream_id,
                owner_key,
                owner_epoch: rev,
                seal_commit: Some(100),
                exclude_node_ids: vec![],
                seal_extent_id: 0,
            });
            let resp = m.handle_stream_alloc_extent(req).await.unwrap();
            let r: StreamAllocExtentResp = rkyv_decode(&resp).unwrap();
            assert_ne!(r.code, CODE_OK, "should fail: no running extent nodes");

            // invariant: store must be unchanged after failed alloc.
            let tail_after = m
                .store
                .inner
                .borrow()
                .extents
                .get(&tail_id)
                .cloned()
                .unwrap();
            let stream_after = m
                .store
                .inner
                .borrow()
                .streams
                .get(&stream_id)
                .cloned()
                .unwrap();

            assert_eq!(
                tail_after.sealed_length, tail_before.sealed_length,
                "tail sealed_length must not change on failed alloc"
            );
            assert_eq!(
                tail_after.eversion, tail_before.eversion,
                "tail eversion must not change on failed alloc"
            );
            assert_eq!(
                stream_after.extent_ids.len(),
                stream_before.extent_ids.len(),
                "stream extent_ids must not change on failed alloc"
            );
        })
    }

    /// handle_stream_punch_holes only removes extents that are
    /// members of the target stream. Non-member extent IDs in the
    /// request are silently ignored — their ref counts must NOT change.
    #[test]
    fn punch_holes_ignores_non_member_extents() {
        run(async {
            let m = AutumnManager::new();

            let owner_key = "test-owner-f126".to_string();
            let rev = {
                let req = rkyv_encode(&AcquireOwnerLockReq {
                    owner_key: owner_key.clone(),
                });
                let resp = m.handle_acquire_owner_lock(req).await.unwrap();
                let r: AcquireOwnerLockResp = rkyv_decode(&resp).unwrap();
                r.owner_epoch
            };

            // Seed two streams: stream A owns extents [10, 11, 12],
            // stream B owns extent [20].
            {
                let mut s = m.store.inner.borrow_mut();
                s.next_id = 100;
                s.streams.insert(
                    1,
                    MgrStreamInfo {
                        stream_id: 1,
                        extent_ids: vec![10, 11, 12],
                        ec_data_shard: 0,
                        ec_parity_shard: 0,
                        replicates: 3,
                    },
                );
                for eid in [10, 11, 12] {
                    s.extents.insert(
                        eid,
                        MgrExtentInfo {
                            extent_id: eid,
                            replicates: vec![],
                            parity: vec![],
                            eversion: 1,
                            refs: 1,
                            vp_table_refs: 0,
                            sealed_length: 100,
                            sealed: true,
                            avali: 1,
                            replicate_disks: vec![],
                            parity_disks: vec![],
                            ec_converted: false,
                        },
                    );
                }

                s.streams.insert(
                    2,
                    MgrStreamInfo {
                        stream_id: 2,
                        extent_ids: vec![20],
                        ec_data_shard: 0,
                        ec_parity_shard: 0,
                        replicates: 3,
                    },
                );
                s.extents.insert(
                    20,
                    MgrExtentInfo {
                        extent_id: 20,
                        replicates: vec![],
                        parity: vec![],
                        eversion: 1,
                        refs: 1,
                        vp_table_refs: 0,
                        sealed_length: 200,
                        sealed: true,
                        avali: 1,
                        replicate_disks: vec![],
                        parity_disks: vec![],
                        ec_converted: false,
                    },
                );
            }

            // Punch stream 1 with extent_ids [10, 20, 999].
            //   10 is a member  → should be removed
            //   20 is NOT a member of stream 1 → must be ignored
            //   999 doesn't exist → must be ignored
            let req = rkyv_encode(&PunchHolesReq {
                stream_id: 1,
                owner_key,
                owner_epoch: rev,
                extent_ids: vec![10, 20, 999],
            });
            let resp = m.handle_stream_punch_holes(req).await.unwrap();
            let r: PunchHolesResp = rkyv_decode(&resp).unwrap();
            assert_eq!(r.code, CODE_OK, "punch_holes should succeed: {}", r.message);

            let s = m.store.inner.borrow();

            // Stream 1 should only have [11, 12] left.
            let stream_a = s.streams.get(&1).unwrap();
            assert_eq!(stream_a.extent_ids, vec![11, 12]);

            // Extent 20 (stream B) must be untouched: refs still 1.
            let ext20 = s.extents.get(&20).unwrap();
            assert_eq!(ext20.refs, 1, "non-member extent 20 refs must not change");

            // Extent 10 was the only member punched.
            // With refs=1 and no vp_table_refs, it should have been deleted
            // from the extents map.
            assert!(
                !s.extents.contains_key(&10),
                "extent 10 should be removed (refs was 1)"
            );
        })
    }

    // ── recovery + EC conversion mutual exclusion ──────────────────────

    #[test]
    fn apply_recovery_done_rejects_duplicate_target() {
        run(async {
            let m = AutumnManager::new();

            // Simulate: extent 20 was correctly EC-converted to (data=[1,3,5],
            // parity=[7]). Then a recovery task that was dispatched BEFORE EC
            // conversion (when parity was still []) completes — the task says
            // "replace node 1 with node 7". Applying this would produce
            // replicates=[7,3,5], parity=[7] (duplicate node 7).
            let extent_id = 20u64;
            let ex = MgrExtentInfo {
                extent_id,
                replicates: vec![1, 3, 5],
                parity: vec![7],
                eversion: 3,
                refs: 1,
                vp_table_refs: 0,
                sealed_length: 100_000,
                sealed: true,
                avali: 0xF, // all 4 slots available before
                replicate_disks: vec![10, 11, 12],
                parity_disks: vec![13],
                ec_converted: true,
            };
            m.store
                .inner
                .borrow_mut()
                .extents
                .insert(extent_id, ex.clone());

            let task = MgrRecoveryTask {
                extent_id,
                replace_id: 1,
                node_id: 7, // already in parity[]
                start_time: 0,
            };
            m._test_mark_recovery_inflight(extent_id, task.clone());

            let done = MgrRecoveryTaskDone {
                task,
                ready_disk_id: 99,
            };
            let result = m.apply_recovery_done(done).await;

            assert!(
                result.is_err(),
                "apply_recovery_done must reject duplicate-node state"
            );
            // Recovery_tasks entry should be cleaned up so future dispatches
            // can re-attempt (e.g., once the original failed node is back
            // online, re_avali can repair without going through dispatch).
            assert!(
                !matches!(
                    m.extent_inflight_op(extent_id),
                    Some(crate::extent_inflight::ExtentOpKind::Recovery)
                ),
                "stale recovery task must be removed on duplicate-node rejection"
            );
            // Extent layout must be unchanged.
            let s = m.store.inner.borrow();
            let ex_after = s.extents.get(&extent_id).unwrap();
            assert_eq!(ex_after.replicates, vec![1, 3, 5]);
            assert_eq!(ex_after.parity, vec![7]);
            assert_eq!(
                ex_after.eversion, 3,
                "eversion must not be bumped on rejection"
            );
        })
    }

    #[test]
    fn apply_recovery_done_succeeds_when_target_is_unique() {
        // Sanity check: the duplicate-target check must not interfere with
        // normal recovery applies. With a 5-node cluster, recovery from
        // node 1 → node 9 (not in extent_nodes) should succeed cleanly.
        run(async {
            let m = AutumnManager::new();

            let extent_id = 30u64;
            let ex = MgrExtentInfo {
                extent_id,
                replicates: vec![1, 3, 5],
                parity: vec![7],
                eversion: 3,
                refs: 1,
                vp_table_refs: 0,
                sealed_length: 100_000,
                sealed: true,
                avali: 0xE, // slot 0 marked unavailable
                replicate_disks: vec![10, 11, 12],
                parity_disks: vec![13],
                ec_converted: true,
            };
            m.store.inner.borrow_mut().extents.insert(extent_id, ex);

            let task = MgrRecoveryTask {
                extent_id,
                replace_id: 1,
                node_id: 9, // fresh node, NOT in extent_nodes
                start_time: 0,
            };
            m._test_mark_recovery_inflight(extent_id, task.clone());

            let done = MgrRecoveryTaskDone {
                task,
                ready_disk_id: 88,
            };
            let result = m.apply_recovery_done(done).await;

            assert!(
                result.is_ok(),
                "normal recovery apply must succeed: {result:?}"
            );
            let s = m.store.inner.borrow();
            let ex_after = s.extents.get(&extent_id).unwrap();
            assert_eq!(
                ex_after.replicates,
                vec![9, 3, 5],
                "slot 0 should be replaced"
            );
            assert_eq!(ex_after.parity, vec![7]);
            assert_eq!(ex_after.eversion, 4, "eversion must be bumped on apply");
            assert_eq!(ex_after.avali, 0xF, "slot 0 avali bit should be set");
        })
    }

    // ── eversion lost-update during EC conversion await ────────────────────

    fn make_ec_extent(extent_id: u64, eversion: u64) -> MgrExtentInfo {
        MgrExtentInfo {
            extent_id,
            replicates: vec![1, 3, 5],
            parity: vec![],
            eversion,
            refs: 1,
            vp_table_refs: 0,
            sealed_length: 100_000,
            sealed: true,
            avali: 0x7,
            replicate_disks: vec![10, 30, 50],
            parity_disks: vec![],
            ec_converted: false,
        }
    }

    /// apply_recovery_done must defer (return Err) when a ConvertToEc op
    /// is in flight on the extent. The exclusive ledger makes
    /// "EC + Recovery simultaneously in flight" structurally impossible,
    /// so this test now exercises the defense-in-depth path:
    /// apply_recovery_done sees a ConvertToEc marker (left behind by a
    /// concurrent dispatch tick) and refuses to write through.
    #[test]
    fn apply_recovery_done_during_ec_inflight_defers() {
        run(async {
            let m = AutumnManager::new();
            let extent_id = 200u64;
            m.store
                .inner
                .borrow_mut()
                .extents
                .insert(extent_id, make_ec_extent(extent_id, 5));

            // Ledger holds ConvertToEc (simulates EC dispatch ahead of
            // recovery completion).
            m._test_mark_ec_inflight(extent_id);

            let task = MgrRecoveryTask {
                extent_id,
                replace_id: 1,
                node_id: 9,
                start_time: 0,
            };
            let done = MgrRecoveryTaskDone {
                task: task.clone(),
                ready_disk_id: 99,
            };
            let result = m.apply_recovery_done(done.clone()).await;
            assert!(
                result.is_err(),
                "apply_recovery_done must return Err while ConvertToEc in flight"
            );
            assert_eq!(
                m.extent_inflight_op(extent_id),
                Some(crate::extent_inflight::ExtentOpKind::ConvertToEc),
                "ConvertToEc marker must be preserved on deferral"
            );
            let s = m.store.inner.borrow();
            let ex = s.extents.get(&extent_id).unwrap();
            assert_eq!(
                ex.replicates,
                vec![1, 3, 5],
                "replicates unchanged during deferral"
            );
            assert_eq!(ex.eversion, 5, "eversion unchanged during deferral");
            drop(s);

            // EC clears (transitions to Recovery being the active op); retry succeeds.
            m._test_clear_inflight(extent_id);
            m._test_mark_recovery_inflight(extent_id, task);
            let result = m.apply_recovery_done(done).await;
            assert!(
                result.is_ok(),
                "recovery apply must succeed after EC clears"
            );
            let s = m.store.inner.borrow();
            let ex = s.extents.get(&extent_id).unwrap();
            assert_eq!(ex.replicates, vec![9, 3, 5], "slot 0 replaced after retry");
            assert_eq!(ex.eversion, 6, "eversion bumped after retry");
        })
    }

    /// mark_extent_available must defer when ec_conversion_inflight contains
    /// the extent.
    #[test]
    fn mark_extent_available_during_ec_inflight_defers() {
        run(async {
            let m = AutumnManager::new();
            let extent_id = 201u64;
            let mut ex = make_ec_extent(extent_id, 7);
            ex.avali = 0x6; // slot 0 unavailable
            m.store.inner.borrow_mut().extents.insert(extent_id, ex);

            m._test_mark_ec_inflight(extent_id);

            let result = m.mark_extent_available(extent_id, 0).await;
            assert!(
                result.is_err(),
                "mark_extent_available must return Err while ec_conversion_inflight"
            );
            let s = m.store.inner.borrow();
            let ex = s.extents.get(&extent_id).unwrap();
            assert_eq!(ex.eversion, 7, "eversion unchanged during deferral");
            assert_eq!(ex.avali, 0x6, "avali unchanged during deferral");
            drop(s);

            // After EC clears, retry succeeds.
            m._test_clear_inflight(extent_id);
            let result = m.mark_extent_available(extent_id, 0).await;
            assert!(
                result.is_ok(),
                "mark_extent_available must succeed after EC clears"
            );
            let s = m.store.inner.borrow();
            let ex = s.extents.get(&extent_id).unwrap();
            assert_eq!(ex.eversion, 8, "eversion bumped after retry");
            assert_eq!(ex.avali, 0x7, "avali bit set after retry");
        })
    }

    /// End-to-end interleave: EC dispatch → recovery defers → EC apply →
    /// EC lock released → recovery retries. Final state reflects BOTH bumps
    /// and recovery's slot replacement is preserved.
    #[test]
    fn full_race_recovery_after_ec_apply() {
        run(async {
            let m = AutumnManager::new();
            let extent_id = 202u64;
            m.store
                .inner
                .borrow_mut()
                .extents
                .insert(extent_id, make_ec_extent(extent_id, 5));

            // Step 1: EC dispatch — acquire lock, capture eversion.
            m._test_mark_ec_inflight(extent_id);
            let captured_eversion = {
                let s = m.store.inner.borrow();
                s.extents.get(&extent_id).unwrap().eversion
            };
            let new_eversion_for_ec = captured_eversion + 1; // = 6

            // Step 2: recovery's done report arrives during EC. Under
            // the exclusive ledger we can't ALSO have a Recovery marker;
            // the ConvertToEc marker alone is what triggers the defer.
            // apply_recovery_done's defense-in-depth check fires and
            // refuses to apply, preserving the EC's pending eversion bump.
            let task = MgrRecoveryTask {
                extent_id,
                replace_id: 1,
                node_id: 9,
                start_time: 0,
            };
            let done = MgrRecoveryTaskDone {
                task: task.clone(),
                ready_disk_id: 99,
            };
            let r = m.apply_recovery_done(done.clone()).await;
            assert!(r.is_err(), "recovery must defer while EC in flight");

            // Step 3: EC RPC returns OK; apply runs.
            m.apply_ec_conversion_done(
                extent_id,
                vec![1, 3, 5, 7], // target_nodes captured at dispatch time
                vec![70],         // extra_disk_ids (parity disk)
                3,                // data_shards
                new_eversion_for_ec,
            )
            .await
            .unwrap();
            // Step 3b: lock released (mirrors the moved remove in ec_conversion_dispatch_loop).
            m._test_clear_inflight(extent_id);

            // Step 4: deferred recovery retries — must succeed now. Under
            // the exclusive ledger, retry rehydrates the Recovery marker (mimicking
            // recovery_collect_loop's behaviour after the EC tick cleared).
            m._test_mark_recovery_inflight(extent_id, task);
            let r = m.apply_recovery_done(done).await;
            assert!(
                r.is_ok(),
                "recovery apply must succeed after EC clears: {r:?}"
            );

            // Final state: both eversion bumps preserved; slot replacement survived.
            let s = m.store.inner.borrow();
            let ex = s.extents.get(&extent_id).unwrap();
            assert_eq!(
                ex.replicates,
                vec![9, 3, 5],
                "recovery's slot replacement (node 1→9) must survive EC apply"
            );
            assert_eq!(ex.parity, vec![7], "parity node added by EC");
            assert_eq!(
                ex.eversion, 7,
                "eversion must reflect EC bump (5→6) + recovery bump (6→7)"
            );
            assert!(ex.ec_converted);
        })
    }

    /// handle_multi_modify_split must return Precondition when any source-
    /// stream extent is in ec_conversion_inflight.
    #[test]
    fn split_aborts_when_source_extent_is_ec_inflight() {
        run(async {
            let m = AutumnManager::new();

            // Minimal cluster state: one owner owner_epoch, one partition with
            // three streams (log=10, row=11, meta=12), each having one extent.
            let owner_key = "owner-test".to_string();
            let owner_epoch = {
                let mut s = m.store.inner.borrow_mut();
                s.acquire_owner_lock(&owner_key)
            };

            let log_stream_id = 10u64;
            let row_stream_id = 11u64;
            let meta_stream_id = 12u64;
            let part_id = 1u64;
            let log_extent = 100u64;
            let row_extent = 101u64;
            let meta_extent = 102u64;

            {
                let mut s = m.store.inner.borrow_mut();
                s.next_id = 200;

                for (sid, eid) in [
                    (log_stream_id, log_extent),
                    (row_stream_id, row_extent),
                    (meta_stream_id, meta_extent),
                ] {
                    s.streams.insert(
                        sid,
                        MgrStreamInfo {
                            stream_id: sid,
                            extent_ids: vec![eid],
                            ec_data_shard: 0,
                            ec_parity_shard: 0,
                            replicates: 3,
                        },
                    );
                    s.extents.insert(
                        eid,
                        MgrExtentInfo {
                            extent_id: eid,
                            replicates: vec![1, 3, 5],
                            parity: vec![],
                            eversion: 1,
                            refs: 1,
                            vp_table_refs: 0,
                            sealed_length: 1000,
                            sealed: true,
                            avali: 0x7,
                            replicate_disks: vec![10, 30, 50],
                            parity_disks: vec![],
                            ec_converted: false,
                        },
                    );
                }

                s.partitions.insert(
                    part_id,
                    MgrPartitionMeta {
                        part_id,
                        log_stream: log_stream_id,
                        row_stream: row_stream_id,
                        meta_stream: meta_stream_id,
                        rg: Some(MgrRange {
                            start_key: b"a".to_vec(),
                            end_key: b"z".to_vec(),
                        }),
                    },
                );
            }

            // Lock the row_stream's extent — simulates EC conversion in flight.
            m._test_mark_ec_inflight(row_extent);

            let req = rkyv_encode(&MultiModifySplitReq {
                part_id,
                owner_key: owner_key.clone(),
                owner_epoch,
                mid_key: b"m".to_vec(),
                log_stream_sealed_length: 500,
                row_stream_sealed_length: 500,
                meta_stream_sealed_length: 500,
            log_tail_extent_id: 0,
            row_tail_extent_id: 0,
            meta_tail_extent_id: 0,
            });
            let resp = m.handle_multi_modify_split(req).await.unwrap();
            let r: CodeResp = rkyv_decode(&resp).unwrap();

            assert_ne!(
                r.code, CODE_OK,
                "split must be rejected when source extent is ec_inflight"
            );
            assert!(
                r.message.contains("ec conversion in flight"),
                "error message must identify the cause: {}",
                r.message
            );

            // Partitions and streams must be unchanged.
            let s = m.store.inner.borrow();
            assert_eq!(
                s.partitions.len(),
                1,
                "no new partition must be created on rejection"
            );
        })
    }

    /// merge handler rejects non-adjacent partitions.
    #[test]
    fn merge_refuses_non_adjacent() {
        run(async {
            let m = AutumnManager::new();
            let owner_key = "owner-test".to_string();
            let owner_epoch = {
                let mut s = m.store.inner.borrow_mut();
                s.acquire_owner_lock(&owner_key)
            };
            // Two partitions with a GAP in keyspace.
            {
                let mut s = m.store.inner.borrow_mut();
                s.next_id = 200;
                for (pid, sids, start, end) in [
                    (1u64, [10u64, 11, 12], b"a".to_vec(), b"f".to_vec()),
                    (2u64, [20u64, 21, 22], b"m".to_vec(), b"z".to_vec()),
                ] {
                    for sid in sids {
                        s.streams.insert(
                            sid,
                            MgrStreamInfo {
                                stream_id: sid,
                                extent_ids: vec![],
                                ec_data_shard: 0,
                                ec_parity_shard: 0,
                                replicates: 3,
                            },
                        );
                    }
                    s.partitions.insert(
                        pid,
                        MgrPartitionMeta {
                            part_id: pid,
                            log_stream: sids[0],
                            row_stream: sids[1],
                            meta_stream: sids[2],
                            rg: Some(MgrRange {
                                start_key: start,
                                end_key: end,
                            }),
                        },
                    );
                }
            }
            let req = rkyv_encode(&MultiModifyMergeReq {
                survivor_part_id: 1,
                victim_part_id: 2,
                owner_key,
                owner_epoch,
                log_sealed_lengths: [0, 0],
                row_sealed_lengths: [0, 0],
                meta_sealed_lengths: [0, 0],
            });
            let resp = m.handle_multi_modify_merge(req).await.unwrap();
            let r: MultiModifyMergeResp = rkyv_decode(&resp).unwrap();
            assert_ne!(r.code, CODE_OK);
            assert!(
                r.message.contains("not adjacent"),
                "error must identify non-adjacency: {}",
                r.message
            );
        })
    }

    /// merge handler rejects when survivor == victim.
    #[test]
    fn merge_refuses_self_merge() {
        run(async {
            let m = AutumnManager::new();
            let owner_key = "owner-test".to_string();
            let owner_epoch = {
                let mut s = m.store.inner.borrow_mut();
                s.acquire_owner_lock(&owner_key)
            };
            let req = rkyv_encode(&MultiModifyMergeReq {
                survivor_part_id: 5,
                victim_part_id: 5,
                owner_key,
                owner_epoch,
                log_sealed_lengths: [0, 0],
                row_sealed_lengths: [0, 0],
                meta_sealed_lengths: [0, 0],
            });
            let resp = m.handle_multi_modify_merge(req).await.unwrap();
            let r: MultiModifyMergeResp = rkyv_decode(&resp).unwrap();
            assert_ne!(r.code, CODE_OK);
            assert!(
                r.message.contains("same partition"),
                "error must identify self-merge: {}",
                r.message
            );
        })
    }

    /// merge handler rejects when any source extent is in
    /// ec_conversion_inflight (mirrors the recovery/EC guard).
    #[test]
    fn merge_refuses_when_ec_inflight() {
        run(async {
            let m = AutumnManager::new();
            let owner_key = "owner-test".to_string();
            let owner_epoch = {
                let mut s = m.store.inner.borrow_mut();
                s.acquire_owner_lock(&owner_key)
            };
            {
                let mut s = m.store.inner.borrow_mut();
                s.next_id = 200;
                for (pid, sids, start, end, eids) in [
                    (
                        1u64,
                        [10u64, 11, 12],
                        b"a".to_vec(),
                        b"m".to_vec(),
                        [100u64, 101, 102],
                    ),
                    (
                        2u64,
                        [20u64, 21, 22],
                        b"m".to_vec(),
                        b"z".to_vec(),
                        [200u64, 201, 202],
                    ),
                ] {
                    for (sid, eid) in sids.iter().copied().zip(eids.iter().copied()) {
                        s.streams.insert(
                            sid,
                            MgrStreamInfo {
                                stream_id: sid,
                                extent_ids: vec![eid],
                                ec_data_shard: 0,
                                ec_parity_shard: 0,
                                replicates: 3,
                            },
                        );
                        s.extents.insert(
                            eid,
                            MgrExtentInfo {
                                extent_id: eid,
                                replicates: vec![1],
                                parity: vec![],
                                eversion: 1,
                                refs: 1,
                                vp_table_refs: 0,
                                sealed_length: 1000,
                                sealed: true,
                                avali: 1,
                                replicate_disks: vec![10],
                                parity_disks: vec![],
                                ec_converted: false,
                            },
                        );
                    }
                    s.partitions.insert(
                        pid,
                        MgrPartitionMeta {
                            part_id: pid,
                            log_stream: sids[0],
                            row_stream: sids[1],
                            meta_stream: sids[2],
                            rg: Some(MgrRange {
                                start_key: start,
                                end_key: end,
                            }),
                        },
                    );
                }
            }
            // Mark victim's row_stream extent as EC-inflight.
            m._test_mark_ec_inflight(201);

            let req = rkyv_encode(&MultiModifyMergeReq {
                survivor_part_id: 1,
                victim_part_id: 2,
                owner_key,
                owner_epoch,
                log_sealed_lengths: [0, 0],
                row_sealed_lengths: [0, 0],
                meta_sealed_lengths: [0, 0],
            });
            let resp = m.handle_multi_modify_merge(req).await.unwrap();
            let r: MultiModifyMergeResp = rkyv_decode(&resp).unwrap();
            assert_ne!(r.code, CODE_OK);
            assert!(
                r.message.contains("in-flight ConvertToEc"),
                "error must identify EC inflight: {}",
                r.message
            );
        })
    }

    /// merge handler refuses when any source extent is in
    /// recovery_tasks (mirrors the split-side guard).
    #[test]
    fn merge_refuses_when_recovery_inflight() {
        run(async {
            let m = AutumnManager::new();
            let owner_key = "owner-test".to_string();
            let owner_epoch = {
                let mut s = m.store.inner.borrow_mut();
                s.acquire_owner_lock(&owner_key)
            };
            {
                let mut s = m.store.inner.borrow_mut();
                s.next_id = 200;
                for (pid, sids, start, end, eids) in [
                    (
                        1u64,
                        [10u64, 11, 12],
                        b"a".to_vec(),
                        b"m".to_vec(),
                        [100u64, 101, 102],
                    ),
                    (
                        2u64,
                        [20u64, 21, 22],
                        b"m".to_vec(),
                        b"z".to_vec(),
                        [200u64, 201, 202],
                    ),
                ] {
                    for (sid, eid) in sids.iter().copied().zip(eids.iter().copied()) {
                        s.streams.insert(
                            sid,
                            MgrStreamInfo {
                                stream_id: sid,
                                extent_ids: vec![eid],
                                ec_data_shard: 0,
                                ec_parity_shard: 0,
                                replicates: 3,
                            },
                        );
                        s.extents.insert(
                            eid,
                            MgrExtentInfo {
                                extent_id: eid,
                                replicates: vec![1],
                                parity: vec![],
                                eversion: 1,
                                refs: 1,
                                vp_table_refs: 0,
                                sealed_length: 1000,
                                sealed: true,
                                avali: 1,
                                replicate_disks: vec![10],
                                parity_disks: vec![],
                                ec_converted: false,
                            },
                        );
                    }
                    s.partitions.insert(
                        pid,
                        MgrPartitionMeta {
                            part_id: pid,
                            log_stream: sids[0],
                            row_stream: sids[1],
                            meta_stream: sids[2],
                            rg: Some(MgrRange {
                                start_key: start,
                                end_key: end,
                            }),
                        },
                    );
                }
            }
            // Simulate active recovery on victim's row_stream extent.
            m._test_mark_recovery_inflight(
                201,
                MgrRecoveryTask {
                    extent_id: 201,
                    replace_id: 999,
                    node_id: 1,
                    start_time: 0,
                },
            );

            let req = rkyv_encode(&MultiModifyMergeReq {
                survivor_part_id: 1,
                victim_part_id: 2,
                owner_key,
                owner_epoch,
                log_sealed_lengths: [0, 0],
                row_sealed_lengths: [0, 0],
                meta_sealed_lengths: [0, 0],
            });
            let resp = m.handle_multi_modify_merge(req).await.unwrap();
            let r: MultiModifyMergeResp = rkyv_decode(&resp).unwrap();
            assert_ne!(r.code, CODE_OK);
            assert!(
                r.message.contains("in-flight Recovery"),
                "must identify recovery inflight cause: {}",
                r.message
            );
        })
    }

    /// merge handler refuses when any source extent is queued
    /// for physical delete (mirrors the delete-vs-recovery guard).
    #[test]
    fn merge_refuses_when_pending_delete() {
        run(async {
            let m = AutumnManager::new();
            let owner_key = "owner-test".to_string();
            let owner_epoch = {
                let mut s = m.store.inner.borrow_mut();
                s.acquire_owner_lock(&owner_key)
            };
            {
                let mut s = m.store.inner.borrow_mut();
                s.next_id = 200;
                for (pid, sids, start, end, eids) in [
                    (
                        1u64,
                        [10u64, 11, 12],
                        b"a".to_vec(),
                        b"m".to_vec(),
                        [100u64, 101, 102],
                    ),
                    (
                        2u64,
                        [20u64, 21, 22],
                        b"m".to_vec(),
                        b"z".to_vec(),
                        [200u64, 201, 202],
                    ),
                ] {
                    for (sid, eid) in sids.iter().copied().zip(eids.iter().copied()) {
                        s.streams.insert(
                            sid,
                            MgrStreamInfo {
                                stream_id: sid,
                                extent_ids: vec![eid],
                                ec_data_shard: 0,
                                ec_parity_shard: 0,
                                replicates: 3,
                            },
                        );
                        s.extents.insert(
                            eid,
                            MgrExtentInfo {
                                extent_id: eid,
                                replicates: vec![1],
                                parity: vec![],
                                eversion: 1,
                                refs: 1,
                                vp_table_refs: 0,
                                sealed_length: 1000,
                                sealed: true,
                                avali: 1,
                                replicate_disks: vec![10],
                                parity_disks: vec![],
                                ec_converted: false,
                            },
                        );
                    }
                    s.partitions.insert(
                        pid,
                        MgrPartitionMeta {
                            part_id: pid,
                            log_stream: sids[0],
                            row_stream: sids[1],
                            meta_stream: sids[2],
                            rg: Some(MgrRange {
                                start_key: start,
                                end_key: end,
                            }),
                        },
                    );
                }
            }
            // Queue extent 100 (survivor's log_stream extent) for physical delete.
            m._test_mark_delete_inflight(100, vec![]);

            let req = rkyv_encode(&MultiModifyMergeReq {
                survivor_part_id: 1,
                victim_part_id: 2,
                owner_key,
                owner_epoch,
                log_sealed_lengths: [0, 0],
                row_sealed_lengths: [0, 0],
                meta_sealed_lengths: [0, 0],
            });
            let resp = m.handle_multi_modify_merge(req).await.unwrap();
            let r: MultiModifyMergeResp = rkyv_decode(&resp).unwrap();
            assert_ne!(r.code, CODE_OK);
            assert!(
                r.message.contains("in-flight Delete"),
                "must identify pending-delete cause: {}",
                r.message
            );
        })
    }

    /// merge then last_op_at must be updated on the
    /// survivor and removed for the victim.
    #[test]
    fn merge_updates_last_op_at_correctly() {
        let mut state = autumn_common::MetadataState::default();
        let mut m = HashMap::new();
        m.insert(1u64, 1_700_000_000i64);
        m.insert(2u64, 1_700_000_500i64);

        // Simulate the in-memory updates applied at end of
        // handle_multi_modify_merge Phase 3.
        let now = 1_800_000_000i64;
        m.insert(1u64, now);
        m.remove(&2);

        assert_eq!(m.get(&1), Some(&now));
        assert!(!m.contains_key(&2));
        // Suppress unused-state warning.
        let _ = state.partitions.is_empty();
        // Avoid `state` mut warning.
        state.next_id = 1;
    }

    /// with 4 nodes and count=3, every node must appear in a
    /// non-trivial fraction of selections — previously the lowest-id 3
    /// always won and node 7 never showed up.
    #[test]
    fn select_nodes_distribution() {
        let mut nodes: HashMap<u64, MgrNodeInfo> = HashMap::new();
        let mut disks: HashMap<u64, MgrDiskInfo> = HashMap::new();
        for (idx, &nid) in [1u64, 3, 5, 7].iter().enumerate() {
            let did = 100 + idx as u64;
            nodes.insert(
                nid,
                MgrNodeInfo {
                    node_id: nid,
                    address: format!("127.0.0.1:{}", 9000 + nid),
                    disks: vec![did],
                    shard_ports: vec![],
                    control_address: String::new(),
                    node_uuid: String::new(),
                },
            );
            disks.insert(
                did,
                MgrDiskInfo {
                    disk_id: did,
                    online: true,
                    uuid: format!("uuid-{nid}"),
                },
            );
        }

        // tests assume all nodes are verified-Online.
        let online_node_ids: HashSet<u64> = nodes.keys().copied().collect();
        const ITERS: usize = 1000;
        let mut counts: HashMap<u64, usize> = HashMap::new();
        for _ in 0..ITERS {
            let picked =
                AutumnManager::select_nodes(&nodes, &disks, &online_node_ids, &HashSet::new(), &HashSet::new(), 3, &[])
                    .unwrap();
            assert_eq!(picked.len(), 3);
            let mut ids: Vec<u64> = picked.iter().map(|n| n.node_id).collect();
            ids.sort();
            ids.dedup();
            assert_eq!(ids.len(), 3, "selection must be 3 distinct nodes");
            for id in ids {
                *counts.entry(id).or_insert(0) += 1;
            }
        }
        // Each of the 4 nodes should appear in ~750/1000 selections
        // (3/4 = 75%). Allow a generous [60%, 90%] window so the test
        // is statistically robust without a fixed RNG seed.
        for &nid in &[1u64, 3, 5, 7] {
            let c = *counts.get(&nid).unwrap_or(&0);
            assert!(
                (600..=900).contains(&c),
                "node {nid} appeared in {c}/{ITERS} selections; expected 600..=900"
            );
        }
    }

    /// Degraded fallback (no online disks) must also shuffle so that
    /// repeated retries from a cold leader spread across the cluster
    /// instead of always pinging the lowest-id node first.
    #[test]
    fn select_nodes_degraded_fallback_shuffles() {
        let mut nodes: HashMap<u64, MgrNodeInfo> = HashMap::new();
        let disks: HashMap<u64, MgrDiskInfo> = HashMap::new(); // empty = nothing online
        for &nid in &[1u64, 3, 5, 7] {
            nodes.insert(
                nid,
                MgrNodeInfo {
                    node_id: nid,
                    address: format!("127.0.0.1:{}", 9000 + nid),
                    disks: vec![100 + nid],
                    shard_ports: vec![],
                    control_address: String::new(),
                    node_uuid: String::new(),
                },
            );
        }

        // tests assume all nodes are verified-Online; the
        // degraded fallback here is "no online disks" (empty `disks`
        // map), not "no Online state nodes".
        let online_node_ids: HashSet<u64> = nodes.keys().copied().collect();
        let mut first_node_seen: HashSet<u64> = HashSet::new();
        for _ in 0..200 {
            let picked =
                AutumnManager::select_nodes(&nodes, &disks, &online_node_ids, &HashSet::new(), &HashSet::new(), 1, &[])
                    .unwrap();
            first_node_seen.insert(picked[0].node_id);
        }
        assert!(
            first_node_seen.len() >= 3,
            "degraded fallback should pick at least 3 distinct nodes across 200 tries; got {first_node_seen:?}"
        );
    }

    /// ENOSPC-1: the spacious layer soft-avoids space-low nodes when
    /// enough remain, and falls back to the full healthy set when the
    /// avoidance would under-fill the selection.
    #[test]
    fn enospc_select_nodes_avoids_space_low_with_fallback() {
        let mut nodes: HashMap<u64, MgrNodeInfo> = HashMap::new();
        let mut disks: HashMap<u64, MgrDiskInfo> = HashMap::new();
        for (idx, &nid) in [1u64, 3, 5, 7].iter().enumerate() {
            let did = 100 + idx as u64;
            nodes.insert(
                nid,
                MgrNodeInfo {
                    node_id: nid,
                    address: format!("127.0.0.1:{}", 9000 + nid),
                    disks: vec![did],
                    shard_ports: vec![],
                    control_address: String::new(),
                    node_uuid: String::new(),
                },
            );
            disks.insert(
                did,
                MgrDiskInfo {
                    disk_id: did,
                    online: true,
                    uuid: format!("uuid-{nid}"),
                },
            );
        }
        let online: HashSet<u64> = nodes.keys().copied().collect();

        // Node 7 is space-low; 3 spacious nodes remain for count=3 →
        // node 7 must NEVER be picked.
        let low: HashSet<u64> = [7u64].into_iter().collect();
        for _ in 0..200 {
            let picked =
                AutumnManager::select_nodes(&nodes, &disks, &online, &low, &HashSet::new(), 3, &[]).unwrap();
            assert!(
                picked.iter().all(|n| n.node_id != 7),
                "space-low node 7 picked despite 3 spacious candidates"
            );
        }

        // 2 of 4 are space-low and count=3 → spacious under-fills, the
        // fallback widens to all healthy nodes (allocation must proceed
        // on a capacity-crunched cluster, not refuse).
        let low2: HashSet<u64> = [5u64, 7].into_iter().collect();
        let picked =
            AutumnManager::select_nodes(&nodes, &disks, &online, &low2, &HashSet::new(), 3, &[]).unwrap();
        assert_eq!(picked.len(), 3);
    }

    /// A dispatch tick that never reached an RPC (every candidate capped by the
    /// rate limiter) must not be filed as a success: `record_success` DELETES
    /// the backoff entry, so during a mass fence — exactly when the limiter
    /// binds — a persistently failing slot would have its 300 s backoff reset
    /// to 2 s every tick and retry at nearly full rate.
    #[test]
    fn deferred_dispatch_leaves_recovery_backoff_intact() {
        run(async {
            let m = AutumnManager::new();
            let (eid, slot, now) = (77u64, 1u32, 1_000i64);
            for _ in 0..3 {
                m.recovery_limiter
                    .borrow_mut()
                    .record_failure(eid, slot, now, "boom");
            }
            assert!(
                m.recovery_limiter.borrow().in_backoff(eid, slot, now),
                "precondition: three failures must put the slot in backoff"
            );

            m.record_dispatch_outcome(eid, slot, now, &Ok(crate::recovery::DispatchOutcome::Deferred));
            assert!(
                m.recovery_limiter.borrow().in_backoff(eid, slot, now),
                "a rate-limited deferral says nothing about this slot — backoff must survive"
            );

            m.record_dispatch_outcome(eid, slot, now, &Ok(crate::recovery::DispatchOutcome::Dispatched));
            assert!(
                !m.recovery_limiter.borrow().in_backoff(eid, slot, now),
                "an actual dispatch does clear the backoff"
            );
        })
    }

    // ── extent-node delete vs in-flight recovery ────────────────────────────

    /// dispatch_recovery_task must return Ok without populating recovery_tasks
    /// when the extent is already queued for physical deletion.
    #[test]
    fn dispatch_recovery_skips_when_pending_delete_queued() {
        run(async {
            let m = AutumnManager::new();
            let extent_id = 300u64;
            m.store
                .inner
                .borrow_mut()
                .extents
                .insert(extent_id, make_ec_extent(extent_id, 1));

            // Simulate GC having queued a delete for this extent.
            m._test_mark_delete_inflight(extent_id, vec!["127.0.0.1:9101".to_string()]);

            // dispatch_recovery_task must skip — delete is already queued.
            let result = m.dispatch_recovery_task(extent_id, /*replace_id=*/ 1).await;
            assert!(
                result.is_ok(),
                "dispatch_recovery_task must return Ok when delete queued: {result:?}"
            );
            assert!(
                !matches!(
                    m.extent_inflight_op(extent_id),
                    Some(crate::extent_inflight::ExtentOpKind::Recovery)
                ),
                "recovery_tasks must NOT be populated when delete is queued"
            );
        })
    }

    /// handle_stream_punch_holes must return Precondition (not remove the
    /// extent) when the to-be-deleted extent is currently being recovered.
    #[test]
    fn punch_holes_aborts_when_extent_is_in_recovery() {
        run(async {
            let m = AutumnManager::new();

            let owner_key = "owner-f139-ph".to_string();
            let owner_epoch = {
                let mut s = m.store.inner.borrow_mut();
                s.acquire_owner_lock(&owner_key)
            };

            let stream_id = 50u64;
            let extent_id = 301u64;
            {
                let mut s = m.store.inner.borrow_mut();
                s.streams.insert(
                    stream_id,
                    MgrStreamInfo {
                        stream_id,
                        extent_ids: vec![extent_id, 399],
                        ec_data_shard: 0,
                        ec_parity_shard: 0,
                        replicates: 3,
                    },
                );
                // Keep a second extent so the stream isn't left empty after punching 301.
                s.extents.insert(399, make_ec_extent(399, 1));
                let mut ex = make_ec_extent(extent_id, 1);
                ex.refs = 1;
                ex.vp_table_refs = 0;
                s.extents.insert(extent_id, ex);
            }

            // Mark this extent as in-flight for recovery.
            m._test_mark_recovery_inflight(
                extent_id,
                MgrRecoveryTask {
                    extent_id,
                    replace_id: 1,
                    node_id: 9,
                    start_time: 0,
                },
            );

            let req = rkyv_encode(&PunchHolesReq {
                stream_id,
                owner_key: owner_key.clone(),
                owner_epoch,
                extent_ids: vec![extent_id],
            });
            let resp = m.handle_stream_punch_holes(req).await.unwrap();
            let r: PunchHolesResp = rkyv_decode(&resp).unwrap();

            assert_ne!(
                r.code, CODE_OK,
                "punch_holes must be rejected when target extent is in recovery"
            );
            assert!(
                r.message.contains("in-flight recovery"),
                "error must mention in-flight recovery: {}",
                r.message
            );
            // Extent must still be in s.extents — not removed.
            let s = m.store.inner.borrow();
            assert!(
                s.extents.contains_key(&extent_id),
                "extent must not be removed from store on rejection"
            );
            drop(s);
            // No pending delete must have been enqueued.
            assert!(
                m.delete_progress.borrow().is_empty(),
                "delete_progress must be empty on rejection"
            );
        })
    }

    /// handle_truncate must return Precondition when any to-be-truncated
    /// extent that would drop to refs=0 is currently being recovered.
    #[test]
    fn truncate_aborts_when_any_extent_is_in_recovery() {
        run(async {
            let m = AutumnManager::new();

            let owner_key = "owner-f139-tr".to_string();
            let owner_epoch = {
                let mut s = m.store.inner.borrow_mut();
                s.acquire_owner_lock(&owner_key)
            };

            let stream_id = 51u64;
            let extent_a = 302u64; // will be truncated (pos < truncate target)
            let extent_b = 303u64; // truncate target (kept)
            let extent_c = 304u64; // kept
            {
                let mut s = m.store.inner.borrow_mut();
                s.streams.insert(
                    stream_id,
                    MgrStreamInfo {
                        stream_id,
                        extent_ids: vec![extent_a, extent_b, extent_c],
                        ec_data_shard: 0,
                        ec_parity_shard: 0,
                        replicates: 3,
                    },
                );
                for &eid in &[extent_a, extent_b, extent_c] {
                    let mut ex = make_ec_extent(eid, 1);
                    ex.refs = 1;
                    ex.vp_table_refs = 0;
                    s.extents.insert(eid, ex);
                }
            }

            // extent_a is being recovered — truncate should be refused.
            m._test_mark_recovery_inflight(
                extent_a,
                MgrRecoveryTask {
                    extent_id: extent_a,
                    replace_id: 1,
                    node_id: 9,
                    start_time: 0,
                },
            );

            let req = rkyv_encode(&TruncateReq {
                stream_id,
                owner_key: owner_key.clone(),
                owner_epoch,
                extent_id: extent_b, // truncate everything before extent_b
            });
            let resp = m.handle_truncate(req).await.unwrap();
            let r: TruncateResp = rkyv_decode(&resp).unwrap();

            assert_ne!(
                r.code, CODE_OK,
                "truncate must be rejected when a to-be-removed extent is in recovery"
            );
            assert!(
                r.message.contains("in-flight recovery"),
                "error must mention in-flight recovery: {}",
                r.message
            );
            // Stream must still contain extent_a.
            let s = m.store.inner.borrow();
            let stream = s.streams.get(&stream_id).unwrap();
            assert!(
                stream.extent_ids.contains(&extent_a),
                "extent_a must not be removed from stream on rejection"
            );
        })
    }

    /// Full-race cycle: punch_holes is rejected while recovery is in flight,
    /// then recovery completes, then punch_holes succeeds and extent is
    /// enqueued for physical deletion.
    #[test]
    fn full_race_recovery_after_punch_holes_attempt() {
        run(async {
            let m = AutumnManager::new();

            let owner_key = "owner-f139-full".to_string();
            let owner_epoch = {
                let mut s = m.store.inner.borrow_mut();
                s.acquire_owner_lock(&owner_key)
            };

            let stream_id = 52u64;
            let extent_id = 305u64;
            let keep_id = 306u64;
            {
                let mut s = m.store.inner.borrow_mut();
                s.streams.insert(
                    stream_id,
                    MgrStreamInfo {
                        stream_id,
                        extent_ids: vec![extent_id, keep_id],
                        ec_data_shard: 0,
                        ec_parity_shard: 0,
                        replicates: 3,
                    },
                );
                let mut ex = make_ec_extent(extent_id, 1);
                ex.refs = 1;
                ex.vp_table_refs = 0;
                s.extents.insert(extent_id, ex);
                s.extents.insert(keep_id, make_ec_extent(keep_id, 1));
            }

            // Recovery is in flight.
            m._test_mark_recovery_inflight(
                extent_id,
                MgrRecoveryTask {
                    extent_id,
                    replace_id: 1,
                    node_id: 9,
                    start_time: 0,
                },
            );

            // Phase 1: punch_holes must be rejected.
            let req_bytes = rkyv_encode(&PunchHolesReq {
                stream_id,
                owner_key: owner_key.clone(),
                owner_epoch,
                extent_ids: vec![extent_id],
            });
            let resp = m
                .handle_stream_punch_holes(req_bytes.clone())
                .await
                .unwrap();
            let r: PunchHolesResp = rkyv_decode(&resp).unwrap();
            assert_ne!(r.code, CODE_OK, "Phase 1: punch_holes must be rejected");

            // Phase 2: recovery completes — clear recovery_tasks.
            m._test_clear_inflight(extent_id);

            // Phase 3: punch_holes must now succeed.
            let resp2 = m.handle_stream_punch_holes(req_bytes).await.unwrap();
            let r2: PunchHolesResp = rkyv_decode(&resp2).unwrap();
            assert_eq!(
                r2.code, CODE_OK,
                "Phase 3: punch_holes must succeed after recovery clears: {}",
                r2.message
            );

            // Extent must be removed from the stream.
            let s = m.store.inner.borrow();
            let stream = s.streams.get(&stream_id).unwrap();
            assert!(
                !stream.extent_ids.contains(&extent_id),
                "extent must be removed from stream after successful punch_holes"
            );
            drop(s);
            // Extent must be queued for physical deletion: check
            // both the in-memory progress map and the unified ledger.
            assert!(
                m.delete_progress.borrow().contains_key(&extent_id),
                "extent must be enqueued for physical deletion after refs→0"
            );
            assert_eq!(
                m.extent_inflight_op(extent_id),
                Some(crate::extent_inflight::ExtentOpKind::Delete),
                "ledger entry must reflect Delete in flight"
            );
        })
    }

    // ── punch_holes/truncate vs in-flight EC conversion ──────────────────────

    /// handle_stream_punch_holes must return Precondition (not bump eversion)
    /// when any to-be-removed extent is currently undergoing EC conversion.
    #[test]
    fn punch_holes_refuses_when_ec_inflight() {
        run(async {
            let m = AutumnManager::new();

            let owner_key = "owner-f145-ph".to_string();
            let owner_epoch = {
                let mut s = m.store.inner.borrow_mut();
                s.acquire_owner_lock(&owner_key)
            };

            let stream_id = 60u64;
            let extent_id = 401u64;
            let extent_keep = 402u64;
            {
                let mut s = m.store.inner.borrow_mut();
                s.streams.insert(
                    stream_id,
                    MgrStreamInfo {
                        stream_id,
                        extent_ids: vec![extent_id, extent_keep],
                        ec_data_shard: 0,
                        ec_parity_shard: 0,
                        replicates: 3,
                    },
                );
                s.extents
                    .insert(extent_keep, make_ec_extent(extent_keep, 1));
                let mut ex = make_ec_extent(extent_id, 1);
                ex.refs = 1;
                ex.vp_table_refs = 0;
                s.extents.insert(extent_id, ex);
            }

            // Simulate EC dispatch: extent is mid-conversion.
            m._test_mark_ec_inflight(extent_id);
            let eversion_before = m.store.inner.borrow().extents[&extent_id].eversion;

            let req = rkyv_encode(&PunchHolesReq {
                stream_id,
                owner_key: owner_key.clone(),
                owner_epoch,
                extent_ids: vec![extent_id],
            });
            let resp = m.handle_stream_punch_holes(req).await.unwrap();
            let r: PunchHolesResp = rkyv_decode(&resp).unwrap();

            assert_ne!(
                r.code, CODE_OK,
                "punch_holes must be rejected when target extent is mid-EC"
            );
            assert!(
                r.message.contains("in-flight EC conversion"),
                "error must mention in-flight EC conversion: {}",
                r.message
            );
            // Eversion must not have been bumped.
            let s = m.store.inner.borrow();
            let ex = s
                .extents
                .get(&extent_id)
                .expect("extent must not be removed");
            assert_eq!(
                ex.eversion, eversion_before,
                "eversion must not be bumped during mid-EC punch_holes"
            );
            drop(s);
            assert!(
                m.delete_progress.borrow().is_empty(),
                "no pending delete must be enqueued on rejection"
            );

            // After EC completes (remove from inflight), punch_holes must succeed.
            m._test_clear_inflight(extent_id);
            let req2 = rkyv_encode(&PunchHolesReq {
                stream_id,
                owner_key: owner_key.clone(),
                owner_epoch,
                extent_ids: vec![extent_id],
            });
            let resp2 = m.handle_stream_punch_holes(req2).await.unwrap();
            let r2: PunchHolesResp = rkyv_decode(&resp2).unwrap();
            assert_eq!(
                r2.code, CODE_OK,
                "punch_holes must succeed after EC completes: {}",
                r2.message
            );
            let s2 = m.store.inner.borrow();
            assert!(
                !s2.streams[&stream_id].extent_ids.contains(&extent_id),
                "extent must be removed from stream after successful punch_holes"
            );
        })
    }

    /// handle_truncate must return Precondition (not bump eversion) when any
    /// to-be-truncated extent is currently undergoing EC conversion.
    #[test]
    fn truncate_refuses_when_ec_inflight() {
        run(async {
            let m = AutumnManager::new();

            let owner_key = "owner-f145-tr".to_string();
            let owner_epoch = {
                let mut s = m.store.inner.borrow_mut();
                s.acquire_owner_lock(&owner_key)
            };

            let stream_id = 61u64;
            let extent_a = 403u64; // to be truncated
            let extent_b = 404u64; // truncate target (kept)
            let extent_c = 405u64; // kept
            {
                let mut s = m.store.inner.borrow_mut();
                s.streams.insert(
                    stream_id,
                    MgrStreamInfo {
                        stream_id,
                        extent_ids: vec![extent_a, extent_b, extent_c],
                        ec_data_shard: 0,
                        ec_parity_shard: 0,
                        replicates: 3,
                    },
                );
                for &eid in &[extent_a, extent_b, extent_c] {
                    let mut ex = make_ec_extent(eid, 1);
                    ex.refs = 1;
                    ex.vp_table_refs = 0;
                    s.extents.insert(eid, ex);
                }
            }

            // extent_a is mid-EC conversion — truncate should be refused.
            m._test_mark_ec_inflight(extent_a);
            let eversion_before = m.store.inner.borrow().extents[&extent_a].eversion;

            let req = rkyv_encode(&TruncateReq {
                stream_id,
                owner_key: owner_key.clone(),
                owner_epoch,
                extent_id: extent_b, // truncate everything before extent_b
            });
            let resp = m.handle_truncate(req).await.unwrap();
            let r: TruncateResp = rkyv_decode(&resp).unwrap();

            assert_ne!(
                r.code, CODE_OK,
                "truncate must be rejected when a to-be-removed extent is mid-EC"
            );
            assert!(
                r.message.contains("in-flight EC conversion"),
                "error must mention in-flight EC conversion: {}",
                r.message
            );
            // Stream must still contain extent_a; eversion must be unchanged.
            let s = m.store.inner.borrow();
            let stream = s.streams.get(&stream_id).unwrap();
            assert!(
                stream.extent_ids.contains(&extent_a),
                "extent_a must not be removed from stream on rejection"
            );
            let ex = s
                .extents
                .get(&extent_a)
                .expect("extent_a must still be in store");
            assert_eq!(
                ex.eversion, eversion_before,
                "eversion must not be bumped during mid-EC truncate"
            );
        })
    }

    // ── alloc_extent / split lost-update races ───────────────────────────────

    /// handle_stream_alloc_extent must return Precondition (not proceed to
    /// network calls) when the current tail extent is in ec_conversion_inflight.
    #[test]
    fn alloc_extent_refuses_when_ec_inflight() {
        run(async {
            let m = AutumnManager::new();

            let owner_key = "owner-f146-ae-ec".to_string();
            let owner_epoch = {
                let mut s = m.store.inner.borrow_mut();
                s.acquire_owner_lock(&owner_key)
            };

            let stream_id = 80u64;
            let tail_id = 500u64;
            {
                let mut s = m.store.inner.borrow_mut();
                s.streams.insert(
                    stream_id,
                    MgrStreamInfo {
                        stream_id,
                        extent_ids: vec![tail_id],
                        ec_data_shard: 0,
                        ec_parity_shard: 0,
                        replicates: 0,
                    },
                );
                s.extents.insert(
                    tail_id,
                    MgrExtentInfo {
                        extent_id: tail_id,
                        replicates: vec![],
                        parity: vec![],
                        eversion: 5,
                        refs: 1,
                        vp_table_refs: 0,
                        sealed_length: 0,
                        sealed: false,
                        avali: 0,
                        replicate_disks: vec![],
                        parity_disks: vec![],
                        ec_converted: false,
                    },
                );
            }

            // Tail is mid-EC: alloc_extent must refuse immediately.
            m._test_mark_ec_inflight(tail_id);
            let eversion_before = m.store.inner.borrow().extents[&tail_id].eversion;

            let req = rkyv_encode(&StreamAllocExtentReq {
                stream_id,
                owner_key: owner_key.clone(),
                owner_epoch,
                seal_commit: Some(100),
                exclude_node_ids: vec![],
                seal_extent_id: 0,
            });
            let resp = m.handle_stream_alloc_extent(req).await.unwrap();
            let r: StreamAllocExtentResp = rkyv_decode(&resp).unwrap();

            assert_ne!(
                r.code, CODE_OK,
                "alloc_extent must be rejected when tail is mid-EC"
            );
            assert!(
                r.message.contains("in-flight ConvertToEc"),
                "error must mention in-flight ConvertToEc: {}",
                r.message
            );
            let ev_after = m.store.inner.borrow().extents[&tail_id].eversion;
            assert_eq!(
                ev_after, eversion_before,
                "eversion must not be bumped when alloc_extent is rejected mid-EC"
            );
        })
    }

    /// An extent whose allocation is in flight must get NO reconcile verdict.
    /// Its files are created on the nodes before `extents/<id>` is published,
    /// so a sweep answered inside that window would otherwise read it as an
    /// orphan and order the deletion of an extent that was just created.
    /// Residue that is NOT mid-allocation must still be condemned in one round
    /// — the orphan sweep's whole job.
    #[test]
    fn reconcile_gives_no_verdict_while_an_allocation_is_in_flight() {
        run(async {
            let m = AutumnManager::new();
            let node_id = 1u64;
            {
                let mut s = m.store.inner.borrow_mut();
                s.nodes.insert(
                    node_id,
                    MgrNodeInfo {
                        node_id,
                        address: "127.0.0.1:9101".to_string(),
                        disks: vec![1],
                        shard_ports: vec![],
                        control_address: String::new(),
                        node_uuid: "uuid-alloc-guard".to_string(),
                    },
                );
            }
            let newborn = 900u64;
            let orphan = 901u64;

            async fn garbage(m: &AutumnManager, ids: Vec<u64>) -> Vec<u64> {
                let req = rkyv_encode(&ReconcileExtentsReq {
                    node_id: 1,
                    node_uuid: "uuid-alloc-guard".to_string(),
                    shard_idx: 0,
                    extent_ids: ids,
                });
                let resp = m.handle_reconcile_extents(req).await.unwrap();
                let r: ReconcileExtentsResp = rkyv_decode(&resp).unwrap();
                assert_eq!(r.code, CODE_OK, "reconcile rejected: {}", r.message);
                r.garbage
            }

            {
                let _allocating = m.mark_allocating(newborn);
                let g = garbage(&m, vec![newborn, orphan]).await;
                assert!(
                    !g.contains(&newborn),
                    "an extent mid-allocation must not be condemned"
                );
                assert!(
                    g.contains(&orphan),
                    "real residue must still be collected in the same round"
                );
            }

            // Guard dropped (allocation committed or failed): no more shield.
            assert!(
                garbage(&m, vec![newborn, orphan]).await.contains(&newborn),
                "once the allocation is over the id is ordinary residue again"
            );
        })
    }

    /// handle_stream_alloc_extent must return Precondition when the tail
    /// extent has an in-flight recovery task (symmetric to EC guard above).
    #[test]
    fn alloc_extent_refuses_when_recovery_inflight() {
        run(async {
            let m = AutumnManager::new();

            let owner_key = "owner-f146-ae-rec".to_string();
            let owner_epoch = {
                let mut s = m.store.inner.borrow_mut();
                s.acquire_owner_lock(&owner_key)
            };

            let stream_id = 81u64;
            let tail_id = 501u64;
            {
                let mut s = m.store.inner.borrow_mut();
                s.streams.insert(
                    stream_id,
                    MgrStreamInfo {
                        stream_id,
                        extent_ids: vec![tail_id],
                        ec_data_shard: 0,
                        ec_parity_shard: 0,
                        replicates: 0,
                    },
                );
                s.extents.insert(
                    tail_id,
                    MgrExtentInfo {
                        extent_id: tail_id,
                        replicates: vec![],
                        parity: vec![],
                        eversion: 7,
                        refs: 1,
                        vp_table_refs: 0,
                        sealed_length: 0,
                        sealed: false,
                        avali: 0,
                        replicate_disks: vec![],
                        parity_disks: vec![],
                        ec_converted: false,
                    },
                );
            }

            // Tail is under active recovery: alloc_extent must refuse.
            m._test_mark_recovery_inflight(
                tail_id,
                MgrRecoveryTask {
                    extent_id: tail_id,
                    replace_id: 0,
                    node_id: 1,
                    start_time: 0,
                },
            );
            let eversion_before = m.store.inner.borrow().extents[&tail_id].eversion;

            let req = rkyv_encode(&StreamAllocExtentReq {
                stream_id,
                owner_key: owner_key.clone(),
                owner_epoch,
                seal_commit: Some(100),
                exclude_node_ids: vec![],
                seal_extent_id: 0,
            });
            let resp = m.handle_stream_alloc_extent(req).await.unwrap();
            let r: StreamAllocExtentResp = rkyv_decode(&resp).unwrap();

            assert_ne!(
                r.code, CODE_OK,
                "alloc_extent must be rejected when tail is mid-recovery"
            );
            assert!(
                r.message.contains("in-flight Recovery"),
                "error must mention in-flight Recovery: {}",
                r.message
            );
            let ev_after = m.store.inner.borrow().extents[&tail_id].eversion;
            assert_eq!(
                ev_after, eversion_before,
                "eversion must not be bumped when alloc_extent is rejected mid-recovery"
            );
        })
    }

    /// handle_multi_modify_split must return Precondition when any source-
    /// stream extent is currently undergoing recovery (symmetric to the
    /// ec_conversion_inflight guard).
    #[test]
    fn split_refuses_when_recovery_inflight() {
        run(async {
            let m = AutumnManager::new();

            let owner_key = "owner-f146-split".to_string();
            let owner_epoch = {
                let mut s = m.store.inner.borrow_mut();
                s.acquire_owner_lock(&owner_key)
            };

            let log_stream_id = 20u64;
            let row_stream_id = 21u64;
            let meta_stream_id = 22u64;
            let part_id = 5u64;
            let log_extent = 200u64;
            let row_extent = 201u64;
            let meta_extent = 202u64;

            {
                let mut s = m.store.inner.borrow_mut();
                s.next_id = 300;
                for (sid, eid) in [
                    (log_stream_id, log_extent),
                    (row_stream_id, row_extent),
                    (meta_stream_id, meta_extent),
                ] {
                    s.streams.insert(
                        sid,
                        MgrStreamInfo {
                            stream_id: sid,
                            extent_ids: vec![eid],
                            ec_data_shard: 0,
                            ec_parity_shard: 0,
                            replicates: 3,
                        },
                    );
                    s.extents.insert(
                        eid,
                        MgrExtentInfo {
                            extent_id: eid,
                            replicates: vec![1, 3, 5],
                            parity: vec![],
                            eversion: 1,
                            refs: 1,
                            vp_table_refs: 0,
                            sealed_length: 1000,
                            sealed: true,
                            avali: 0x7,
                            replicate_disks: vec![10, 30, 50],
                            parity_disks: vec![],
                            ec_converted: false,
                        },
                    );
                }
                s.partitions.insert(
                    part_id,
                    MgrPartitionMeta {
                        part_id,
                        log_stream: log_stream_id,
                        row_stream: row_stream_id,
                        meta_stream: meta_stream_id,
                        rg: Some(MgrRange {
                            start_key: b"a".to_vec(),
                            end_key: b"z".to_vec(),
                        }),
                    },
                );
            }

            // Simulate recovery in flight on the log_stream's extent.
            m._test_mark_recovery_inflight(
                log_extent,
                MgrRecoveryTask {
                    extent_id: log_extent,
                    replace_id: 0,
                    node_id: 2,
                    start_time: 0,
                },
            );
            let eversion_before = m.store.inner.borrow().extents[&log_extent].eversion;

            let req = rkyv_encode(&MultiModifySplitReq {
                part_id,
                owner_key: owner_key.clone(),
                owner_epoch,
                mid_key: b"m".to_vec(),
                log_stream_sealed_length: 500,
                row_stream_sealed_length: 500,
                meta_stream_sealed_length: 500,
            log_tail_extent_id: 0,
            row_tail_extent_id: 0,
            meta_tail_extent_id: 0,
            });
            let resp = m.handle_multi_modify_split(req).await.unwrap();
            let r: CodeResp = rkyv_decode(&resp).unwrap();

            assert_ne!(
                r.code, CODE_OK,
                "split must be rejected when source extent is mid-recovery"
            );
            assert!(
                r.message.contains("recovery in flight"),
                "error must mention recovery in flight: {}",
                r.message
            );
            // Source streams and partitions must be unchanged.
            let s = m.store.inner.borrow();
            assert_eq!(
                s.extents[&log_extent].eversion, eversion_before,
                "eversion must not be bumped when split is rejected"
            );
            assert!(
                s.partitions.contains_key(&part_id),
                "original partition must still exist on split rejection"
            );
        })
    }

    // ── rich-marker rkyv roundtrip + pending_ec_dispatch bookkeeping ──

    /// The marker value persisted to etcd must round-trip rkyv encode/decode
    /// without losing any field. Previously the marker had an empty value;
    /// now the value carries `target_nodes` so re-dispatch after
    /// failover uses the original assignment instead of a fresh shuffle.
    #[test]
    fn ec_dispatch_inflight_rkyv_roundtrip() {
        let original = MgrEcDispatchInflight {
            extent_id: 42,
            target_nodes: vec![1, 3, 5, 7],
            extra_disk_ids: vec![19],
            data_shards: 3,
            new_eversion: 9,
            owner_epoch: 17,
        };
        let bytes = rkyv_encode(&original).to_vec();
        let decoded: MgrEcDispatchInflight = rkyv_decode(&bytes).expect("decode");
        assert_eq!(decoded.extent_id, original.extent_id);
        assert_eq!(decoded.target_nodes, original.target_nodes);
        assert_eq!(decoded.extra_disk_ids, original.extra_disk_ids);
        assert_eq!(decoded.data_shards, original.data_shards);
        assert_eq!(decoded.new_eversion, original.new_eversion);
        assert_eq!(decoded.owner_epoch, original.owner_epoch);
    }

    /// R1: cluster_version bump validation (memory-mode — etcd CAS is
    /// exercised by the live cluster smoke; this pins the refusal rules).
    #[test]
    fn r1_bump_cluster_version_validation() {
        let m = AutumnManager::new();
        // Memory mode seeds cluster_version = WIRE_VERSION_MAX.
        assert_eq!(m.cluster_version.get(), autumn_rpc::WIRE_VERSION_MAX);
        run(async {
            // +1 beyond this binary's max → refused (nothing to upgrade to).
            let err = m
                .bump_cluster_version(autumn_rpc::WIRE_VERSION_MAX + 1)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("WIRE_VERSION_MAX"), "{err}");

            // Simulate a cluster running one version behind this binary
            // (the post-rolling-upgrade state where a bump is legal).
            m.cluster_version.set(autumn_rpc::WIRE_VERSION_MAX - 1);
            // Skip (+2) and same (+0) and backwards are all refused.
            for bad in [
                autumn_rpc::WIRE_VERSION_MAX + 1,
                autumn_rpc::WIRE_VERSION_MAX - 1,
                0,
            ] {
                let err = m.bump_cluster_version(bad).await.unwrap_err();
                assert!(err.to_string().contains("exactly current+1"), "{err}");
            }
            // Exactly +1 (and within max) succeeds.
            let v = m
                .bump_cluster_version(autumn_rpc::WIRE_VERSION_MAX)
                .await
                .unwrap();
            assert_eq!(v, autumn_rpc::WIRE_VERSION_MAX);
            assert_eq!(m.cluster_version.get(), autumn_rpc::WIRE_VERSION_MAX);

            // Non-leader refuses before any validation.
            m.cluster_version.set(autumn_rpc::WIRE_VERSION_MAX - 1);
            m.leader.set(false);
            assert!(m
                .bump_cluster_version(autumn_rpc::WIRE_VERSION_MAX)
                .await
                .is_err());
            m.leader.set(true);
        });
    }

    /// R1 (coco P1): a persisted cluster_version ABOVE this binary's
    /// WIRE_VERSION_MAX is the rolled-back-past-a-bump case — every
    /// decode point (replay / imprint / CAS-lost re-reads) must refuse.
    #[test]
    fn r1_parse_cluster_version_rejects_rollback_and_garbage() {
        let max = autumn_rpc::WIRE_VERSION_MAX;
        assert_eq!(
            AutumnManager::parse_cluster_version(max.to_string().as_bytes()).unwrap(),
            max
        );
        assert_eq!(AutumnManager::parse_cluster_version(b"1").unwrap(), 1);
        let err =
            AutumnManager::parse_cluster_version((max + 1).to_string().as_bytes()).unwrap_err();
        assert!(err.to_string().contains("rollback"), "{err}");
        assert!(AutumnManager::parse_cluster_version(b"").is_err());
        assert!(AutumnManager::parse_cluster_version(b"not-a-number").is_err());
        assert!(AutumnManager::parse_cluster_version(b"-1").is_err());
    }

    /// the unified inflight ledger (the
    /// successor to `pending_ec_dispatch`) starts empty; acquire +
    /// commit_release round-trip ConvertToEc payloads correctly.
    #[test]
    fn pending_ec_dispatch_in_memory_bookkeeping() {
        let m = AutumnManager::new();
        assert!(
            m.inflight.borrow().is_empty(),
            "ledger starts empty"
        );

        let rec = MgrEcDispatchInflight {
            extent_id: 7,
            target_nodes: vec![1, 3, 5, 7],
            extra_disk_ids: vec![19],
            data_shards: 3,
            new_eversion: 4,
            owner_epoch: 0,
        };
        run(async {
            m.acquire_extent_inflight(
                rec.extent_id,
                crate::extent_inflight::ExtentOpPayload::ConvertToEc(rec.clone()),
            )
            .await
            .expect("acquire");
        });
        let inflight_view = m.inflight.borrow();
        let stored = inflight_view.get(&7).expect("present").clone();
        drop(inflight_view);
        match stored.unpack().expect("valid record").1 {
            crate::extent_inflight::ExtentOpPayload::ConvertToEc(p) => {
                assert_eq!(p.target_nodes, vec![1, 3, 5, 7]);
                assert_eq!(p.new_eversion, 4);
            }
            _ => panic!("expected ConvertToEc payload"),
        }
        assert_eq!(
            m.extent_inflight_op(7),
            Some(crate::extent_inflight::ExtentOpKind::ConvertToEc)
        );

        m.commit_extent_inflight_release(7);
        assert!(
            m.inflight.borrow().is_empty(),
            "ledger cleared after release"
        );
    }

    /// `apply_ec_conversion_done` must set `avali` to
    /// `all_bits(K + M)`. Previously it left `avali` at the pre-EC value
    /// (`all_bits(K)`), leaving the parity slot(s) marked unavailable.
    /// The `recovery_dispatch_loop` then fired RE_AVALI on parity holders
    /// every 2 s, which on the extent-node side ran
    /// `fetch_full_extent_from_sources` and allocated sealed_length-sized
    /// Vec<u8> per peer attempt (observed as multi-GB RSS swings on an
    /// idle cluster after `cluster.sh restart`).
    // seed the default policy only on a fresh cluster.
    #[test]
    fn autopolicy_boot_default_seeds_only_a_fresh_cluster() {
        use crate::auto_policy::AutoPolicyMode;

        // No --auto-policy-default configured → stays Off (cluster.sh / tests).
        let m = AutumnManager::new();
        m.apply_auto_policy_default();
        assert_eq!(m.auto_policy.borrow().mode, AutoPolicyMode::Off);
        assert!(m.auto_policy.borrow().active.is_empty());

        // Configured + fresh (no persisted config) → seed balanced/Armed.
        let m = AutumnManager::new();
        m.set_auto_policy_default("balanced".to_string());
        m.apply_auto_policy_default();
        assert_eq!(m.auto_policy.borrow().mode, AutoPolicyMode::Armed);
        assert_eq!(m.auto_policy.borrow().active, "balanced");

        // Configured but NOT fresh (a persisted config was replayed) → never
        // re-seed: the operator's / previous leader's choice wins on failover.
        let m = AutumnManager::new();
        m.set_auto_policy_default("balanced".to_string());
        m.auto_policy_had_persisted_config.set(true);
        m.apply_auto_policy_default();
        assert_eq!(m.auto_policy.borrow().mode, AutoPolicyMode::Off);
        assert!(m.auto_policy.borrow().active.is_empty());

        // A bogus preset name never seeds (belt to the bin's fail-loud check).
        let m = AutumnManager::new();
        m.set_auto_policy_default("no-such-preset".to_string());
        m.apply_auto_policy_default();
        assert_eq!(m.auto_policy.borrow().mode, AutoPolicyMode::Off);

        // The preset-name validator the bin uses.
        assert!(AutumnManager::is_known_auto_policy_preset("balanced"));
        assert!(AutumnManager::is_known_auto_policy_preset("aggressive"));
        assert!(!AutumnManager::is_known_auto_policy_preset("nope"));
    }

    #[test]
    fn apply_ec_conversion_done_sets_avali_for_all_shards() {
        run(async {
            let m = AutumnManager::new();
            let extent_id = 206u64;
            // Pre-EC: 3 replicates, K data shards = 3, M parity = 0.
            // The seal path would have set avali = all_bits(3) = 0b0111.
            let pre = MgrExtentInfo {
                extent_id,
                replicates: vec![1, 3, 5],
                parity: vec![],
                eversion: 3,
                refs: 1,
                vp_table_refs: 0,
                sealed_length: 2_961_566_856,
                sealed: true,
                avali: 0x7,
                replicate_disks: vec![10, 30, 50],
                parity_disks: vec![],
                ec_converted: false,
            };
            m.store.inner.borrow_mut().extents.insert(extent_id, pre);

            // EC convert with K=3, M=1; coordinator picked node 7 / disk 70
            // as the new parity holder.
            m.apply_ec_conversion_done(extent_id, vec![1, 3, 5, 7], vec![70], 3, 4)
                .await
                .expect("apply_ec_conversion_done");

            let s = m.store.inner.borrow();
            let ex = s.extents.get(&extent_id).expect("extent present");
            assert!(ex.ec_converted, "ec_converted must flip true");
            assert_eq!(ex.replicates, vec![1, 3, 5]);
            assert_eq!(ex.parity, vec![7]);
            assert_eq!(ex.eversion, 4);
            // The load-bearing assertion: avali covers ALL K+M = 4 slots,
            // not just the K=3 from the pre-EC seal path.
            assert_eq!(
                ex.avali, 0xF,
                "avali must mark every post-EC slot available; \
                 leaving the parity bit clear causes the recovery loop to \
                 fire RE_AVALI on the parity holder indefinitely"
            );
        })
    }
}
