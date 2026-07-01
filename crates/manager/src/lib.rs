pub mod audit;
pub mod authz;
pub mod ec_abandon;
mod extent_delete;
pub mod extent_inflight;
pub mod inode_lease;
pub mod node_state;
pub mod policy;
#[cfg(test)]
mod policy_tests;
mod recovery;
pub mod recovery_rate_limiter;
mod rpc_handlers;

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

/// Etcd path for the manager leader-key. F149: also used as the fence target
/// for every manager etcd write txn.
pub(crate) const LEADER_KEY: &str = "autumn-rs/stream-manager/leader";

/// F211-C: etcd prefix for persistent operator overrides
/// (`node_override/<node_id>` → rkyv'd `MgrNodeOverride`).
pub const NODE_OVERRIDE_PREFIX: &str = "node_override/";

/// F211-C: etcd prefix for hard-removed node tombstones
/// (`decommissioned/<node_id>` → rkyv'd `MgrNodeOverride`). Same value
/// shape as the override prefix — the existence of the key is what
/// blocks re-registration.
pub const DECOMMISSIONED_PREFIX: &str = "decommissioned/";

/// F214-A: cluster identity key. Written exactly once: the first leader
/// to win the election CAS-creates this key (create_revision==0) with a
/// fresh UUID. Subsequent leaders inherit via `replay_from_etcd`. Read
/// by `MSG_GET_CLUSTER_ID` so `autumn-op format` can stamp each
/// formatted disk and `autumn-extent-node` can verify on startup.
pub const CLUSTER_ID_KEY: &str = "autumn-rs/cluster_id";
/// R1 rolling upgrade: persisted cluster_version. Value is ASCII decimal
/// (e.g. b"3") — deliberately NOT rkyv, so it stays readable across every
/// future serialization era (it gates exactly those transitions).
pub const CLUSTER_VERSION_KEY: &str = "autumn-rs/cluster_version";

/// F-ioring-lease-1: writer-lease etcd prefix. One key per inode that
/// currently has a writer (reader leases are NOT persisted — they're
/// ephemeral, and a manager failover invalidates every reader's cache
/// per plan §6.4).
pub const INODE_LEASES_PREFIX: &str = "inode_leases/";

/// F-AUTHZ-1: etcd prefix for the KDC tenant account DB
/// (`tenantAccount/<tenant>` → rkyv'd `MgrTenantAccount`). Replayed on leader
/// failover; the credential HASH is stored, never the raw credential. The
/// tenant name is a string suffix (percent-encoded segment), not a u64 id.
pub const TENANT_ACCOUNT_PREFIX: &str = "tenantAccount/";

/// ENOSPC-1: default allocation free-space floor — a node whose best
/// disk has less free than this is soft-avoided by `select_nodes`.
/// 256 MiB comfortably covers a fresh extent + its metadata while small
/// enough not to strand mostly-full-but-usable disks.
pub const DEFAULT_MIN_ALLOC_FREE_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct EtcdMirror {
    client: Rc<autumn_etcd::EtcdClient>,
    /// F149: identity used in the leader-fence compare. Set at connect time
    /// from `AutumnManager::instance_id`.
    instance_id: Rc<String>,
    /// F149: shared with `AutumnManager.leader`. Flipped to `false` when the
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

    /// F149: run a fenced txn. Always prepends a
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

        // Distinguish fence-failure from extra_cmp-failure by reading the
        // current leader-key value. If it still matches our instance_id, the
        // fence held and only a business CAS failed (e.g., create_revision==0
        // refused because the key already exists). If it differs (or is
        // gone), we have been deposed.
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
            if got.kvs.first().is_some() {
                self.displaced.set(true);
            }
            self.leader.set(false);
            return Err(AppError::NotLeader);
        }

        Ok(false)
    }

    /// F265: fenced txn whose success value is the txn's COMMIT REVISION
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
        let compare = vec![autumn_etcd::Cmp::value(
            LEADER_KEY.as_bytes(),
            self.instance_id.as_bytes(),
        )];
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
            return Ok(rev);
        }
        // Same fence-vs-anomaly diagnosis as `txn_fenced`.
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
            if got.kvs.first().is_some() {
                self.displaced.set(true);
            }
            self.leader.set(false);
            return Err(AppError::NotLeader);
        }
        Err(AppError::Internal(
            "etcd txn rejected with empty extra_cmp".to_string(),
        ))
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
            // No extra_cmp was supplied, so a `false` here would mean etcd
            // returned `succeeded=false` despite an empty compare list — that
            // can only happen on a server-side bug. Surface as Internal.
            false => Err(AppError::Internal(
                "etcd txn rejected with empty extra_cmp".to_string(),
            )),
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
            false => Err(AppError::Internal(
                "etcd txn rejected with empty extra_cmp".to_string(),
            )),
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
    /// will retry". F149 leader fence is still threaded via the
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
                "stream changed concurrently (CAS conflict); retry with a fresh snapshot"
                    .to_string(),
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

    /// F191: bound an RPC at `timeout`. Same connection / eviction
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
        // F228 (1A): bound the TCP connect. `call_timeout` wraps only the
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

/// F228 (1A): TCP connect timeout for the manager's ConnPool. See
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
    /// txn (F149).
    instance_id: Rc<String>,
    /// F207-C: unified extent-level in-flight ledger. Authoritative
    /// source of truth for every stream-layer op currently in flight
    /// (ConvertToEc / Recovery / Delete). Replaces the four scattered
    /// inflight bookkeeping mechanisms that existed pre-F207
    /// (`ec_conversion_inflight` HashSet, `pending_ec_dispatch` HashMap,
    /// `recovery_tasks` HashMap, `pending_extent_deletes` VecDeque).
    /// Persisted at etcd prefix `extent_inflight/`. See
    /// `crates/manager/src/extent_inflight.rs` for the API + invariants
    /// and `~/.claude/plans/stream-merge-split-ps-sorted-dijkstra.md` for
    /// the migration plan.
    pub(crate) inflight: Rc<RefCell<HashMap<u64, crate::extent_inflight::MgrExtentInflightRecord>>>,
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
    /// F207-C: in-memory live retry state for Delete ops. The ledger
    /// entry's `PersistedPendingDelete` payload is a snapshot of the
    /// original addrs (captured at enqueue time); the live "which
    /// addrs are still pending an ack" state lives here and is NOT
    /// persisted (retry attempts reset on failover, which is correct —
    /// a new leader's first attempt is its own "attempt 1"). Populated
    /// on `enqueue_pending_deletes` and on `replay_from_etcd` (from
    /// Delete-kind ledger entries with attempts=0).
    pub(crate) delete_progress: Rc<RefCell<HashMap<u64, crate::extent_delete::PendingDelete>>>,
    /// F210-G2: persisted "tried 60 times in extent_delete_loop and still
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
    runtime_started: Rc<Cell<bool>>,
    /// F265: true once `serve()`'s listener is actually BOUND and
    /// accepting. The UCX listener bind can retry through a killed
    /// predecessor's TIME_WAIT window for ~60 s (F264) — far past
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
    /// F211-I audit log retention (days; `--audit-retention-days`,
    /// default 90, 0 = disabled). Enforced by `audit_gc_loop` — the GC
    /// helper existed since F211-I but had NO caller, so `mgr_audit_log/`
    /// grew in etcd unboundedly.
    pub(crate) audit_retention_days: Rc<Cell<u64>>,
    /// etcd-chaos D1: WHY are we not leader? `true` (the safe default —
    /// every fresh/rejoined process starts displaced) = another instance
    /// holds/held leadership or we never won it: our in-memory state may
    /// be arbitrarily stale → routing reads answer NOT_LEADER (the F267
    /// H3 fix). `false` while `!leader` = we WERE the leader and lost the
    /// lease without anyone replacing us (etcd outage / lease blip): our
    /// in-memory routing is the freshest that exists and NO new leader
    /// can be elected or mutate anything while etcd is down — serving
    /// get_regions + heartbeats STALE-WHILE-LEADERLESS keeps the data
    /// plane fully alive through etcd maintenance (pre-fix, a >90 s etcd
    /// outage black-holed fresh clients AND suicided the PS fleet).
    /// Shared with `EtcdMirror` so the F149 fence-diagnosis paths can
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
    /// F191: dedicated pool for control-plane RPCs to extent nodes
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
    // F207-C: `pending_extent_deletes` field deleted. Replaced by
    // `inflight` (etcd-persisted exclusion + snapshot for failover) +
    // `delete_progress` (in-memory live retry state). The F109
    // semantics — "extents whose refs dropped to 0 still need to be
    // unlinked on every replica" — are unchanged; the persistence
    // model upgraded from in-memory only to etcd-backed (closes the
    // pre-F207 footnote that manager restart lost pending entries).
    /// F183: per-partition unix-epoch timestamp of the last split or merge
    /// involving this partition. Sourced from etcd prefix
    /// `partitionLastOp/<part_id>` (i64 little-endian). Default 0 for
    /// partitions never split/merged. Used by the policy engine for
    /// cooldown.
    pub(crate) last_op_at: Rc<RefCell<HashMap<u64, i64>>>,
    /// F183: policy engine — split/merge candidate computation over a
    /// 30-min sliding window of per-partition load metrics. F202
    /// extended it to also generate minor-compact + EC advisories.
    /// F203 deleted the auto-dispatch consumer of this output — the
    /// engine is now advisory-only; external controllers query
    /// `MSG_GET_POLICY_CANDIDATES` and call client subcommands to
    /// act on what they see.
    pub(crate) policy: Rc<RefCell<crate::policy::PolicyEngine>>,
    /// F192: per-node sliding-window of push-based failure reports from
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
    /// F211-A: per-extent-node auto-tracked liveness (Online ↔ Suspected).
    /// Fed by `disk_status_update_loop` (df ok / fail) and `register_node`
    /// (initial heartbeat). Consumed by F211-B health-report RPCs,
    /// F211-E recovery dispatch gate, and F211-F EC dispatch loop's
    /// Suspected-window skip. **No automatic Down transition** — fence
    /// is operator-driven (F211-C `mgr_fence_node`).
    pub(crate) node_states: Rc<RefCell<crate::node_state::NodeStateTracker>>,
    /// F211-C: persistent operator overrides keyed on node_id. Mirrors
    /// the etcd prefix `node_override/<node_id>`. Mutated only via the
    /// admin RPCs `mgr_fence_node` / `mgr_set_node_maintenance` /
    /// `mgr_clear_node_override` / by Maintenance TTL expiry inside
    /// `node_states.tick()`. Survives leader failover via etcd replay.
    pub(crate) node_overrides: Rc<RefCell<HashMap<u64, MgrNodeOverride>>>,
    /// F211-C: tombstones for `mgr_remove_node`. Etcd prefix
    /// `decommissioned/<node_id>` — written when the OP removes a node.
    /// Read by `handle_register_node`'s zombie-defense check.
    pub(crate) decommissioned: Rc<RefCell<HashMap<u64, MgrNodeOverride>>>,
    /// F211-H: in-memory recovery throttle counters (per-source /
    /// per-target / global). Mutated by `dispatch_recovery_task` on
    /// acquire and by `apply_recovery_done` / `drain_extent_inflight_marker`
    /// on release. NOT persisted — limits are advisory, not safety
    /// invariants.
    pub(crate) recovery_limiter: Rc<RefCell<crate::recovery_rate_limiter::RecoveryRateLimiter>>,
    /// F211-I: per-process audit-log sequence counter. Combined with
    /// the unix-nanosecond timestamp to form the `mgr_audit_log/`
    /// suffix so ordering is unique even for concurrent appends.
    pub(crate) audit_seq: Rc<Cell<u64>>,
    /// F195: F192 quorum debounce — sliding-window length. Default 60 s.
    /// Configured via the manager binary's `--report-disk-failure-window-secs`
    /// CLI flag (was previously `AUTUMN_REPORT_DISK_FAILURE_WINDOW_SECS`).
    pub(crate) report_disk_failure_window: Cell<Duration>,
    /// F195: F192 quorum debounce — distinct-reporter threshold to flip
    /// node offline. Default 3. Configured via the manager binary's
    /// `--report-disk-failure-quorum` CLI flag (was previously
    /// `AUTUMN_REPORT_DISK_FAILURE_QUORUM`).
    pub(crate) report_disk_failure_quorum: Cell<usize>,
    /// F214-A: persistent cluster identity. CAS-created in etcd
    /// (`autumn-rs/cluster_id`) by the first leader; inherited by
    /// subsequent leaders via `replay_from_etcd`. Empty when the manager
    /// is running in memory-only mode (no etcd) — in that mode
    /// `MSG_GET_CLUSTER_ID` reports the per-process random UUID set in
    /// `Self::new()` so dev/test workflows still work end-to-end. Read
    /// by `handle_get_cluster_id`.
    pub(crate) cluster_id: Rc<RefCell<String>>,
    /// R1 rolling upgrade: persisted cluster_version (etcd
    /// `autumn-rs/cluster_version`, ASCII decimal). The operator-bumped
    /// feature gate from docs/rolling_upgrade_design.md §3-R1 — new wire
    /// forms / persisted formats versioned N may only be EMITTED once
    /// this reaches N. CAS-seeded to the first leader's
    /// `WIRE_VERSION_MAX` by `imprint_cluster_version`; bumped only via
    /// `MSG_BUMP_CLUSTER_VERSION` (monotonic, exactly +1, capped at this
    /// binary's own WIRE_VERSION_MAX). Memory-only mode starts at this
    /// binary's WIRE_VERSION_MAX.
    pub(crate) cluster_version: Rc<Cell<u32>>,
    /// F-ioring-lease-1: inode-level lease registry shared between the
    /// AcquireLease / ReleaseLease / HeartbeatLease / PollInvalidations
    /// handlers and the `inode_lease_revoke_loop` background task.
    /// Writer leases are persisted under the `inode_leases/` etcd
    /// prefix; reader leases live in memory only. See
    /// `crates/manager/src/inode_lease.rs` and
    /// `docs/autumn_fs_lease_plan.md`.
    pub(crate) inode_leases: crate::inode_lease::SharedRegistry,
    /// F-AUTHZ-1: the manager's Ed25519 signing keyring (KDC private
    /// material), loaded once from `--auth-signing-key-file`. `None` = authz
    /// disabled (opt-in; fuse/kvcache/dev unaffected). Set at startup only.
    pub(crate) authz_keyring: Rc<RefCell<Option<crate::authz::AuthzKeyring>>>,
    /// F-AUTHZ-1: admin token gating the tenant-create/delete RPCs
    /// (admin_auth_design.md Option A). `None` = those admin RPCs are refused.
    pub(crate) admin_token: Rc<RefCell<Option<String>>>,
    /// F-AUTHZ-1: key prefixes under which the PS applies default-DENY (e.g.
    /// `mem/`). Published in `GET_AUTHZ_CONFIG`. Each ends with `/`.
    pub(crate) protected_prefixes: Rc<RefCell<Vec<Vec<u8>>>>,
    /// F-AUTHZ-1: TTL (seconds) minted tokens get. Default 3600 (1 h).
    pub(crate) token_ttl_secs: Rc<Cell<u64>>,
    /// F-AUTHZ-1: clock-skew leeway (seconds) advertised to the PS. Default 60.
    pub(crate) clock_skew_secs: Rc<Cell<u64>>,
    /// F-AUTHZ-1: tenant account DB (etcd `tenantAccount/<tenant>` →
    /// `MgrTenantAccount`). Replayed on leader failover; mutated only via the
    /// admin RPCs. Stores the credential HASH, never the raw credential.
    pub(crate) tenant_accounts: Rc<RefCell<HashMap<String, MgrTenantAccount>>>,
}

impl Default for AutumnManager {
    fn default() -> Self {
        Self::new()
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
            split_inflight: Rc::new(RefCell::new(std::collections::HashSet::new())),
            delete_progress: Rc::new(RefCell::new(HashMap::new())),
            failed_deletes: Rc::new(RefCell::new(HashMap::new())),
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
            recent_failure_reports: Rc::new(RefCell::new(HashMap::new())),
            // F195 defaults match the pre-F195 env defaults (F192).
            report_disk_failure_window: Cell::new(Duration::from_secs(60)),
            report_disk_failure_quorum: Cell::new(3),
            // F211-A: env-controlled soft-timeout, default 10 s.
            node_states: Rc::new(RefCell::new(crate::node_state::NodeStateTracker::default())),
            // F211-C: starts empty; populated by replay / admin RPCs.
            node_overrides: Rc::new(RefCell::new(HashMap::new())),
            decommissioned: Rc::new(RefCell::new(HashMap::new())),
            // F211-H: starts at the env-configured default limits.
            recovery_limiter: Rc::new(RefCell::new(
                crate::recovery_rate_limiter::RecoveryRateLimiter::from_env(),
            )),
            audit_seq: Rc::new(Cell::new(0)),
            // F214-A: in memory-only mode this serves as the cluster
            // identity. Overwritten by `try_become_leader` /
            // `replay_from_etcd` when etcd is configured.
            cluster_id: Rc::new(RefCell::new(uuid::Uuid::new_v4().to_string())),
            // R1: memory-only mode runs at this binary's max wire
            // version. Overwritten by `try_become_leader` /
            // `replay_from_etcd` when etcd is configured.
            cluster_version: Rc::new(Cell::new(autumn_rpc::WIRE_VERSION_MAX)),
            // F-ioring-lease-1: empty registry; populated on
            // AcquireLease and on `replay_from_etcd`.
            inode_leases: Rc::new(RefCell::new(crate::inode_lease::LeaseRegistry::with_ttl(
                std::time::Duration::from_secs(
                    crate::inode_lease::DEFAULT_LEASE_TTL_SECS as u64,
                ),
            ))),
            // F-AUTHZ-1: authz OFF unless the binary loads a signing-key file.
            authz_keyring: Rc::new(RefCell::new(None)),
            admin_token: Rc::new(RefCell::new(None)),
            protected_prefixes: Rc::new(RefCell::new(Vec::new())),
            token_ttl_secs: Rc::new(Cell::new(3600)),
            clock_skew_secs: Rc::new(Cell::new(60)),
            tenant_accounts: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    /// F195: F192 quorum debounce config setter. Called by the manager
    /// binary's main() after CLI parsing; the public API mirrors the
    /// existing `set_auto_split` / `set_policy_config` pattern.
    /// `quorum` is clamped to at least 1.
    pub fn set_report_disk_failure_config(&self, window: Duration, quorum: usize) {
        self.report_disk_failure_window.set(window);
        self.report_disk_failure_quorum.set(quorum.max(1));
    }

    /// F-AUTHZ-1: install the KDC signing keyring (parsed by the binary from
    /// `--auth-signing-key-file`). Its presence ENABLES data-plane authz.
    pub fn set_authz_keyring(&self, keyring: crate::authz::AuthzKeyring) {
        *self.authz_keyring.borrow_mut() = Some(keyring);
    }

    /// F-AUTHZ-1: set the admin token gating tenant-create/delete
    /// (`--admin-token`). Without it those admin RPCs are refused.
    pub fn set_admin_token(&self, token: String) {
        *self.admin_token.borrow_mut() = Some(token);
    }

    /// F-AUTHZ-1: set the protected (default-DENY) key prefixes
    /// (`--auth-protected-prefix`, repeatable). Each is normalized to end `/`.
    pub fn set_protected_prefixes(&self, mut prefixes: Vec<Vec<u8>>) {
        for p in &mut prefixes {
            if p.last() != Some(&b'/') {
                p.push(b'/');
            }
        }
        *self.protected_prefixes.borrow_mut() = prefixes;
    }

    /// F-AUTHZ-1: set the minted-token TTL in seconds (clamped ≥ 60).
    pub fn set_token_ttl_secs(&self, secs: u64) {
        self.token_ttl_secs.set(secs.max(60));
    }

    /// F-AUTHZ-1: set the advertised clock-skew leeway in seconds.
    pub fn set_clock_skew_secs(&self, secs: u64) {
        self.clock_skew_secs.set(secs);
    }

    /// F183: read the last_op_at timestamp for a partition (0 if never op'd).
    #[allow(dead_code)]
    pub(crate) fn last_op_at_for(&self, part_id: u64) -> i64 {
        self.last_op_at.borrow().get(&part_id).copied().unwrap_or(0)
    }

    /// F184 test helper: dispatch a SPLIT against `part_id` as if the
    /// policy engine had picked it. Snapshots state internally.
    /// F184 test helper: override the policy engine's thresholds.
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
        self.auto_dispatch_split(&cand, &state).await
    }

    /// F184 test helper: orchestrate a MERGE for (survivor, victim) as
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
    /// anyway). The F267/H3 rejoined-follower blackhole stays closed:
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
    /// F228 (1C): spawn a manager background loop with panic-isolation +
    /// auto-restart. Pre-F228 each loop was a bare
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
    /// Hangs are prevented separately by F228 (1A): every await a loop can
    /// reach is now bounded (etcd `unary_call` timeout, ConnPool connect +
    /// request timeouts). The two together close both failure modes.
    ///
    /// NOTE on layered `catch_unwind`: `compio::runtime::spawn` already
    /// wraps the future in `AssertUnwindSafe(future).catch_unwind()`
    /// internally (compio-runtime-0.11.0/src/runtime/mod.rs:202); its
    /// `JoinHandle<T>` is `Task<Result<T, Box<dyn Any + Send>>>`. That's
    /// why pre-F228 `spawn(loop).detach()` was "silently dead" — compio
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

        // F228 (1C): every loop runs under spawn_supervised (panic ->
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

        // F222: single df caller — merges the former recovery_collect_loop
        // (2 s, apply done_tasks) and disk_status_update_loop (10 s, disk +
        // node liveness). Eliminates the race where the empty-`tasks` df
        // drained the EN's recovery_done and discarded the completions.
        let mgr = self.clone();
        Self::spawn_supervised("node_health", move || mgr.clone().node_health_loop());

        // F211-I completion: audit retention GC was a dead helper —
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

        // F109: physical extent file deletion fanout.
        let mgr = self.clone();
        Self::spawn_supervised("extent_delete", move || mgr.clone().extent_delete_loop());

        // F210-G2: persisted-retry slow loop for deletes that exhausted
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

        // F183: policy advisory tick.
        let mgr = self.clone();
        Self::spawn_supervised("policy_tick", move || mgr.clone().policy_tick_loop());

        // F207-D: stale-marker WARN sweep. Iterates the inflight ledger
        // every 5 minutes and logs WARN for any marker > 24h old.
        // Auto-clearing is INTENTIONALLY not done — a stuck marker
        // usually signals a real bug worth surfacing. Operator runs the
        // Python ops `--clear-stale-inflight extent <id>` script after
        // investigating.
        let mgr = self.clone();
        Self::spawn_supervised("extent_inflight_stale_sweep", move || {
            mgr.clone().extent_inflight_stale_sweep_loop()
        });

        // F-ioring-lease-1: TTL revoke pass — once per second, sweep
        // expired writer leases (queues `LEASE_REVOKED` invalidations
        // for readers; etcd-deletes the persisted record) and silently
        // drops expired reader leases.
        let mgr = self.clone();
        Self::spawn_supervised("inode_lease_revoke", move || {
            mgr.clone().inode_lease_revoke_loop()
        });
    }

    /// F183: every POLICY_TICK_INTERVAL_SEC, leader recomputes split/merge
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
                tracing::info!("F183/F187/F202 policy: {} candidate(s)", cands.len());
                for c in &cands {
                    let kind = match c.kind {
                        POLICY_KIND_SPLIT => "SPLIT",
                        POLICY_KIND_MERGE => "MERGE",
                        autumn_rpc::manager_rpc::POLICY_KIND_GC => "GC",
                        autumn_rpc::manager_rpc::POLICY_KIND_MAJOR_COMPACT => "MAJOR_COMPACT",
                        autumn_rpc::manager_rpc::POLICY_KIND_HOT_COLD => "HOT_COLD",
                        autumn_rpc::manager_rpc::POLICY_KIND_MINOR_COMPACT => "MINOR_COMPACT",
                        autumn_rpc::manager_rpc::POLICY_KIND_EC => "EC",
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

            // F203: in-kernel auto-dispatch removed. The advisory_cache
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
    /// return it for logging. Advisory-only (F203): no etcd write, no
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
        let mut p = self.policy.borrow_mut();
        // F210-F3: prune metrics for partitions that no longer exist
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
        // F187: maintenance (GC + major/minor compact) — windowed metrics only
        // (`last_gc_at` / `last_compact_at` come from the PS-reported buckets).
        cands.append(&mut p.compute_maintenance_advisory(now));
        // F196 Stage D: hot/cold imbalance (kind = POLICY_KIND_HOT_COLD), ridden
        // on the same advisory_cache for `client info` rendering.
        cands.append(&mut p.compute_hot_cold_advisory(owners, now));
        // F202: EC advisory — per-extent, sourced from streams + extents (not
        // partition-windowed); the helper filters extents < ec_min_extent_bytes.
        cands.append(&mut p.compute_ec_advisory(state, now));
        // Persist the union so MSG_GET_POLICY_CANDIDATES returns all 7 kinds
        // (split, merge, gc, major_compact, hot_cold, minor_compact, ec).
        p.advisory_cache = cands.clone();
        p.advisory_cache_at = now;
        cands
    }

    /// F184: auto-dispatch SPLIT to the owning PS for a SPLIT candidate.
    /// The PS handler (`handle_split_part`) already implements the full
    /// F140 dual-gate + F103 auth-rg flow; we just send the RPC.
    pub async fn auto_dispatch_split(
        &self,
        cand: &PolicyCandidate,
        state: &autumn_common::MetadataState,
    ) -> Result<()> {
        // Look up the owning PS via regions + ps_nodes.
        let region = state
            .regions
            .get(&cand.primary_part_id)
            .ok_or_else(|| anyhow::anyhow!("no region for part {}", cand.primary_part_id))?;
        // Prefer per-partition address (F099-K) when present; fall back to PS-level.
        let ps_addr = state
            .part_addrs
            .get(&cand.primary_part_id)
            .cloned()
            .or_else(|| state.ps_nodes.get(&region.ps_id).cloned())
            .ok_or_else(|| anyhow::anyhow!("no address for part {}", cand.primary_part_id))?;
        let payload =
            autumn_rpc::partition_rpc::rkyv_encode(&autumn_rpc::partition_rpc::SplitPartReq {
                part_id: cand.primary_part_id,
            });
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
            "F184 auto-split part={} dispatched OK",
            cand.primary_part_id
        );
        Ok(())
    }

    /// F184: auto-orchestrate MERGE for a same-PS adjacent cold pair.
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
        let flush = |addr: String, pid: u64| {
            let pool = self.conn_pool.clone();
            async move {
                let payload = autumn_rpc::partition_rpc::rkyv_encode(
                    &autumn_rpc::partition_rpc::MaintenanceReq {
                        part_id: pid,
                        op: autumn_rpc::partition_rpc::MAINTENANCE_FLUSH,
                        extent_ids: vec![],
                        // F201 wire fields — ignored for FLUSH op.
                        gc_ratio: None,
                        gc_max_size: None,
                        gc_stream_debt: None,
                        gc_empty_only: false,
                    },
                );
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
            "F184 auto-merge survivor={survivor_id} victim={victim_id} OK \
             (E_new={})",
            resp.new_log_tail_extent_id
        );
        Ok(())
    }

    /// F184 helper: query commit_length for one stream by hitting the
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

        // F210-A3: replay_from_etcd BEFORE set_leader(true). Pre-F210-A3,
        // set_leader(true) ran first; during the (typically short) replay
        // window any concurrent mutating RPC saw leader=true but the
        // in-memory store was still empty / being repopulated, and could
        // compute mutations against a stale base, durably mirroring them
        // to etcd via the F149 fence (which only checks instance_id, not
        // "post-replay"). Replay then re-overwrote in-memory with the
        // (now-corrupted) etcd state.
        //
        // After F210-A3: ensure_leader() (= self.leader.get()) returns
        // false during replay; mutating handlers reject with
        // CODE_NOT_LEADER; client retries land after replay completes and
        // the handler runs with a fully-rebuilt store.
        //
        // Lease TTL is 10 s. Typical replay is sub-second; if etcd is so
        // big replay exceeds 10 s, the lease expires before set_leader
        // and the next mutating RPC's F149 fence flips us back to
        // non-leader — the election loop retries. The deeper fix (start
        // keepalive between CAS and replay so the lease stays alive
        // through arbitrarily long replays) is filed as a P3 follow-up
        // — it needs a stop-signal to revoke the lease on replay error.
        self.replay_from_etcd().await?;
        self.set_leader(true);
        self.displaced.set(false);
        self.leaderless_since.set(None);

        // F214-A: ensure the cluster identity is imprinted in etcd. The
        // CAS uses create_revision==0, so only the first leader ever to
        // run against a fresh etcd actually writes; subsequent leaders
        // re-CAS, observe `succeeded == false`, and read the existing
        // value. `replay_from_etcd` already loaded any prior value, so
        // this path also handles "I'm the first leader on a never-bootstrapped
        // etcd". Best-effort: a failure here logs WARN and leaves the
        // per-process UUID as the cluster_id; the next election retry
        // will try again. Wire through `txn_fenced` so the write
        // inherits the F149 leader-fence guarantee.
        if let Err(err) = self.imprint_cluster_id().await {
            tracing::warn!(error = %err, "F214-A: imprint_cluster_id failed");
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

        let mgr = self.clone();
        compio::runtime::spawn(async move {
            mgr.leader_keepalive_loop(lease_id).await;
        })
        .detach();

        Ok(true)
    }

    /// F214-A: CAS-imprint the cluster_id key in etcd. Idempotent.
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
                    "F214-A: imprinted fresh cluster_id"
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
                        "F211-C: skipping malformed {} entry",
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
        // F183: per-partition last_op_at sidecar
        let last_op = c.get_prefix("partitionLastOp/").await?;
        // F207: unified extent in-flight ledger. Authoritative source of
        // truth for stream-layer ops in flight on each extent.
        let extent_inflight_raw = c
            .get_prefix(crate::extent_inflight::EXTENT_INFLIGHT_PREFIX)
            .await?;
        // F210-G2: persisted retry queue for extent deletes that
        // exhausted the primary in-memory loop's budget.
        let failed_delete_raw = c
            .get_prefix(crate::extent_delete::EXTENT_DELETE_RETRY_PREFIX)
            .await?;
        // F211-C: persistent operator overrides + decommissioned tombstones.
        let node_override_raw = c.get_prefix(NODE_OVERRIDE_PREFIX).await?;
        let decommissioned_raw = c.get_prefix(DECOMMISSIONED_PREFIX).await?;
        // F214-A: cluster identity (single key, not a prefix).
        let cluster_id_kv = c.get(CLUSTER_ID_KEY.as_bytes()).await?;
        // R1: persisted cluster_version (single key, ASCII decimal).
        let cluster_version_kv = c.get(CLUSTER_VERSION_KEY.as_bytes()).await?;
        // F-ioring-lease-1: persisted writer leases.
        let inode_leases_raw = c.get_prefix(INODE_LEASES_PREFIX).await?;
        // F-AUTHZ-1: persisted tenant account DB.
        let tenant_account_raw = c.get_prefix(TENANT_ACCOUNT_PREFIX).await?;
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
            // F265: epoch = mod_revision (bumped on every acquire), NOT
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

        // F183: parse partitionLastOp/ sidecar (i64 little-endian)
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
        // F211-A: seed the node-state tracker with an OK heartbeat for
        // every replayed EN node so the new leader starts with all
        // nodes Online; the next `df` poll (10 s tick) will re-derive
        // the truth from RPC outcomes. Mirrors the F210-G1 approach
        // for PS heartbeats below.
        {
            let mut t = self.node_states.borrow_mut();
            let s = self.store.inner.borrow();
            for node_id in s.nodes.keys() {
                t.on_heartbeat_ok(*node_id);
            }
        }
        // F211-C: replay persistent operator overrides (Fenced /
        // Maintenance). Overrides survive leader failover so the new
        // leader's `recovery_dispatch_loop` (F211-E) sees the same
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
        // F-AUTHZ-1: replay the KDC tenant account DB. String-keyed (tenant
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
                    anyhow::anyhow!("F-AUTHZ-1: malformed tenantAccount/{tenant}: {e}")
                })?;
                accts.insert(tenant, acct);
            }
        }
        // F210-G1: seed `ps_last_heartbeat` with `Instant::now()` for
        // every replayed PS. Pre-F210-G1 the map was empty post-failover,
        // and the liveness loop's `None` arm treated unknown PSes as
        // "alive" — so a PS that died right before the failover (with
        // its evicted etcd entry still lingering pre-F210-G1) was
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
        // F183: install last_op_at sidecar so policy engine cooldown
        // gating is correct on cold-start as well.
        *self.last_op_at.borrow_mut() = decoded_last_op;
        // F207: install the unified inflight ledger. Records with
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
            let mut map = self.inflight.borrow_mut();
            map.clear();
            for (id, rec) in decoded {
                if id != 0 {
                    map.insert(id, rec);
                }
            }
        }
        // F207-C: rehydrate in-memory `delete_progress` from Delete-kind
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
                            pending_addrs: p.pending_addrs,
                            attempts: 0,
                        },
                    );
                }
            }
        }
        // F214-A: install cluster_id from etcd. The key may legitimately
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
        // F210-G2: rehydrate `failed_deletes` from the persisted retry
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
                            "F210-G2: replay skipped malformed extentDeleteRetry/ key"
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
                                "F210-G2: replay skipped malformed extentDeleteRetry/ payload"
                            );
                            continue;
                        }
                    };
                map.insert(id, entry);
            }
        }

        // F-ioring-lease-1: install persisted writer leases. Reader leases are
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

    /// F121: pick `count` candidate nodes for a fresh extent allocation.
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
    /// F144: pick is **shuffled** (uniform random `count`-subset) instead
    /// of "lowest `node_id` first". The pre-F144 deterministic order
    /// concentrated load on the first `count` nodes by ID — e.g. a 4-node
    /// cluster {1,3,5,7} with 3-replica streams placed every extent on
    /// {1,3,5}, leaving node 7 idle until one of the first three failed.
    ///
    /// F190: `exclude_node_ids` carries the writer's per-stream "recently
    /// failed" set (30 s TTL on the client). Filter the candidate pool by
    /// this set BEFORE the online-disk filter; if the result is too small
    /// to satisfy `count`, drop the exclusion and retry — never block
    /// allocation on a stale exclude.
    ///
    /// F214-B: `online_node_ids` is the set of nodes whose
    /// `NodeStateTracker` state is `Online` (i.e. registered AND verified
    /// alive via at least one successful df). Suspend / Suspected nodes
    /// are excluded at the primary filter; the cold-leader fallback
    /// still applies — when too few `Online` nodes exist (e.g. the
    /// manager has just won leader election and hasn't run its first
    /// df sweep), the pool widens to honour the existing
    /// "fall-back-to-fresh-node" path in `handle_stream_alloc_extent`.
    fn select_nodes(
        nodes: &HashMap<u64, MgrNodeInfo>,
        disks: &HashMap<u64, MgrDiskInfo>,
        online_node_ids: &HashSet<u64>,
        space_low_node_ids: &HashSet<u64>,
        count: usize,
        exclude_node_ids: &[u64],
    ) -> Result<Vec<MgrNodeInfo>, AppError> {
        use rand::seq::SliceRandom;
        let all_unfiltered: Vec<MgrNodeInfo> = nodes.values().cloned().collect();
        if all_unfiltered.len() < count {
            return Err(AppError::Precondition(format!(
                "not enough nodes: need {count}, got {}",
                all_unfiltered.len()
            )));
        }
        let exclude_set: HashSet<u64> = exclude_node_ids.iter().copied().collect();
        let after_exclude: Vec<MgrNodeInfo> = all_unfiltered
            .iter()
            .filter(|n| !exclude_set.contains(&n.node_id))
            .cloned()
            .collect();
        // F190: only honor the exclude set if at least `count` non-excluded
        // nodes remain — otherwise stale excludes would block allocation.
        let all = if after_exclude.len() >= count {
            after_exclude
        } else {
            all_unfiltered
        };
        // F214-B + F121: prefer nodes that are BOTH verified-Online AND
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
        // online disks. Preserve the pre-F121 / pre-F214-B fallback —
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
            // F265: the epoch BUMPS on every acquire. Pre-F265 this was a
            // create_revision==0 CAS that reused the key's stable
            // create_revision forever — so an owner_key's epoch NEVER rose
            // again after first creation. Consequences (both observed in
            // the F265 transport chaos run): (a) failback wedge — once a
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

    /// F265: called by `serve()` once the RPC listener is BOUND. Restarts
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

    // F228 (1C): takes `self` by value (was `&self`) for uniformity with the
    // other 8 supervised loops — `spawn_supervised` clones a fresh handle per
    // restart, so the loop future must own it.
    async fn ps_liveness_check_loop(self) {
        const CHECK_INTERVAL: Duration = Duration::from_secs(2);
        const PS_DEAD_TIMEOUT: Duration = Duration::from_secs(10);

        loop {
            compio::time::sleep(CHECK_INTERVAL).await;
            // F265: never evict while our OWN listener isn't accepting —
            // a PS cannot heartbeat into an unbound socket. The ucx bind
            // retry (F264 TIME_WAIT) holds `serve()` for up to ~60 s
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

            // F210-G1: explicit delete of every evicted PS's etcd key.
            // `mirror_partition_snapshot` only PUTs survivors and never
            // DELETEs — pre-F210-G1 the evicted `psNodes/<id>` key
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
            // F099-K: a dropped region implies the old per-partition
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

    /// F183: splice victim's extents onto the END of survivor's
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
    /// Caller (handle_multi_modify_merge) is responsible for the F138/
    /// F145/F146 inflight checks before calling this.
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
    /// F-merge-refs-leak: `refs` is MEMBERSHIP-NEUTRAL for victim extents
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

    /// F183: same as compute_merge_streams but without appending a new
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

    /// F183: apply computed merge mutations. Mirror of `apply_split_mutations`.
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

    /// F099-M: look up `shard_ports` for a node by address, so we can
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

    /// F099-M: route an address to the shard listening for `extent_id`.
    /// If `shard_ports` is empty, returns `addr` unchanged (legacy mode).
    fn shard_addr_for_extent(addr: &str, shard_ports: &[u16], extent_id: u64) -> String {
        if shard_ports.is_empty() {
            return addr.to_string();
        }
        let k = shard_ports.len();
        let port = shard_ports[(extent_id as usize) % k];
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
    /// under the caller's validated owner-lock owner_epoch. F210-H3 Tier 2
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

    /// F210-H3 Tier 2 fence-free probe. Used by:
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

    /// F173: persist a marker that extent X is currently mid-EC-conversion
    /// from THIS leader's perspective. Called BEFORE the
    /// `EXT_MSG_CONVERT_TO_EC` RPC is dispatched. If this leader dies
    /// mid-flight, the new leader's `replay_from_etcd` repopulates
    async fn mark_extent_available(&self, extent_id: u64, slot: usize) -> Result<(), AppError> {
        // F138 / F207-B: defer while EC conversion is in flight on this
        // extent. re_avali was sent to the extent-node (eversion bump
        // there), but the manager-side eversion bump must not race
        // apply_ec_conversion_done's overwrite. The recovery_dispatch_loop
        // retries on the next tick. F207-B: reads the unified ledger via
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

    /// F192: registers a node in-memory so a subsequent
    /// `handle_report_disk_failure` quorum trip has a node to flip
    /// offline. Skips etcd / mirror by writing to the in-memory store
    /// directly. Used only by the F192 unit tests below.
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
    fn f192_two_distinct_reporters_below_quorum_no_offline() {
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
    fn f192_three_distinct_reporters_flips_offline() {
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
    fn f192_duplicate_reporter_does_not_count_toward_quorum() {
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
    fn f192_quorum_clears_after_trip_and_does_not_re_fire() {
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
            });
            let resp = m.handle_register_node(req).await.unwrap();
            let r: RegisterNodeResp = rkyv_decode(&resp).unwrap();
            assert_eq!(r.code, CODE_OK);

            let req2 = rkyv_encode(&RegisterNodeReq {
                addr: "127.0.0.1:4001".to_string(),
                disk_uuids: vec!["d2".to_string()],
                shard_ports: vec![],
                control_address: String::new(),
            });
            let resp2 = m.handle_register_node(req2).await.unwrap();
            let r2: RegisterNodeResp = rkyv_decode(&resp2).unwrap();
            assert_eq!(r2.code, CODE_PRECONDITION);
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
    fn f019_least_loaded_allocation() {
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
    fn f019_ps_eviction_reassigns_regions() {
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
    fn f019_heartbeat_updates_timestamp() {
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
    fn f181_compute_merge_streams_extent_ids_order_and_refs() {
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

        // F-merge-refs-leak: victim-only extents transfer victim→survivor, so
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
    fn f181_splice_streams_without_new_tail_no_e_new() {
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
        // F-merge-refs-leak: victim-only extent transfers victim→survivor →
        // refs stays 1 (was asserted as 2 = the leak).
        let e40 = modified.iter().find(|e| e.extent_id == 40).unwrap();
        assert_eq!(e40.refs, 1);
        assert_eq!(e40.sealed_length, 200);
    }

    /// F-merge-refs-leak regression: merging back a split (survivor + victim
    /// CoW-share the pre-split extents) must NOT leak refs and must NOT list a
    /// shared extent twice. Pre-fix the shared extent got refs += 1 AND a
    /// duplicate entry in extent_ids; over repeated split→merge cycles it drove
    /// extents to refs>0 with zero stream membership (invisible orphans).
    #[test]
    fn f_merge_refs_leak_cow_shared_extent_dedup_and_refs() {
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

    /// F-merge-refs-leak regression for the row/meta splice path (no new tail).
    #[test]
    fn f_merge_refs_leak_splice_cow_shared_extent_dedup_and_refs() {
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
    fn f181_apply_merge_mutations_drops_victim_entries() {
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

    // (removed: f181_merged_partition_vp_refs_sums_per_extent and
    // split_partition_vp_snapshot_clones_parent_refs — both exercised the
    // deleted partition_vp_refs maintenance fns.)

    #[test]
    fn f124_compute_region_keeps_existing_ps_for_left_partition() {
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
    fn f124_compute_region_assigns_least_loaded_for_new_partition() {
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

    // (removed: merge_extent_updates_preserves_ref_and_vp_changes —
    // merge_extent_updates was deleted with the vp_table_refs machinery.)

    /// F125: handle_stream_alloc_extent must not modify the in-memory store
    /// when the handler fails partway through. When alloc_extent_on_node
    /// fails (no running extent nodes), the store must remain unchanged.
    /// Pre-F125, the handler mutated the store before the etcd mirror, so
    /// any early return left stale mutations behind.
    #[test]
    fn f125_alloc_extent_no_store_mutation_on_failure() {
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

            // F125 invariant: store must be unchanged after failed alloc.
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

    /// F126: handle_stream_punch_holes only removes extents that are
    /// members of the target stream. Non-member extent IDs in the
    /// request are silently ignored — their ref counts must NOT change.
    #[test]
    fn f126_punch_holes_ignores_non_member_extents() {
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

    // ── F126: recovery + EC conversion mutual exclusion ─────────────────

    #[test]
    fn f126_apply_recovery_done_rejects_duplicate_target() {
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
    fn f126_apply_recovery_done_succeeds_when_target_is_unique() {
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

    // ── F138: eversion lost-update during EC conversion await ──────────────

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
    /// is in flight on the extent. F207-C: the exclusive ledger makes
    /// "EC + Recovery simultaneously in flight" structurally impossible,
    /// so this test now exercises the defense-in-depth path:
    /// apply_recovery_done sees a ConvertToEc marker (left behind by a
    /// concurrent dispatch tick) and refuses to write through.
    #[test]
    fn f138_apply_recovery_done_during_ec_inflight_defers() {
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
                "F138: apply_recovery_done must return Err while ConvertToEc in flight"
            );
            assert_eq!(
                m.extent_inflight_op(extent_id),
                Some(crate::extent_inflight::ExtentOpKind::ConvertToEc),
                "F207-C: ConvertToEc marker must be preserved on deferral"
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
                "F138: recovery apply must succeed after EC clears"
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
    fn f138_mark_extent_available_during_ec_inflight_defers() {
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
                "F138: mark_extent_available must return Err while ec_conversion_inflight"
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
                "F138: mark_extent_available must succeed after EC clears"
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
    fn f138_full_race_recovery_after_ec_apply() {
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

            // Step 2: recovery's done report arrives during EC. F207-C: under
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
            // F207-C, retry rehydrates the Recovery marker (mimicking
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
                "F138: recovery's slot replacement (node 1→9) must survive EC apply"
            );
            assert_eq!(ex.parity, vec![7], "parity node added by EC");
            assert_eq!(
                ex.eversion, 7,
                "F138: eversion must reflect EC bump (5→6) + recovery bump (6→7)"
            );
            assert!(ex.ec_converted);
        })
    }

    /// handle_multi_modify_split must return Precondition when any source-
    /// stream extent is in ec_conversion_inflight.
    #[test]
    fn f138_split_aborts_when_source_extent_is_ec_inflight() {
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
            });
            let resp = m.handle_multi_modify_split(req).await.unwrap();
            let r: CodeResp = rkyv_decode(&resp).unwrap();

            assert_ne!(
                r.code, CODE_OK,
                "F138: split must be rejected when source extent is ec_inflight"
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

    /// F183: merge handler rejects non-adjacent partitions.
    #[test]
    fn f181_merge_refuses_non_adjacent() {
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

    /// F183: merge handler rejects when survivor == victim.
    #[test]
    fn f181_merge_refuses_self_merge() {
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

    /// F183: merge handler rejects when any source extent is in
    /// ec_conversion_inflight (mirrors F138).
    #[test]
    fn f181_merge_refuses_when_ec_inflight() {
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

    /// F183: merge handler refuses when any source extent is in
    /// recovery_tasks (mirrors F146 split-side guard).
    #[test]
    fn f183_merge_refuses_when_recovery_inflight() {
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

    /// F183: merge handler refuses when any source extent is queued
    /// for physical delete (mirrors F139).
    #[test]
    fn f183_merge_refuses_when_pending_delete() {
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

    /// F183 + F184: merge then last_op_at must be updated on the
    /// survivor and removed for the victim.
    #[test]
    fn f184_merge_updates_last_op_at_correctly() {
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

    /// F144: with 4 nodes and count=3, every node must appear in a
    /// non-trivial fraction of selections — pre-F144 the lowest-id 3
    /// always won and node 7 never showed up.
    #[test]
    fn f144_select_nodes_distribution() {
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

        // F214-B: tests assume all nodes are verified-Online.
        let online_node_ids: HashSet<u64> = nodes.keys().copied().collect();
        const ITERS: usize = 1000;
        let mut counts: HashMap<u64, usize> = HashMap::new();
        for _ in 0..ITERS {
            let picked =
                AutumnManager::select_nodes(&nodes, &disks, &online_node_ids, &HashSet::new(), 3, &[])
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
    fn f144_select_nodes_degraded_fallback_shuffles() {
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
                },
            );
        }

        // F214-B: tests assume all nodes are verified-Online; the
        // degraded fallback here is "no online disks" (empty `disks`
        // map), not "no Online state nodes".
        let online_node_ids: HashSet<u64> = nodes.keys().copied().collect();
        let mut first_node_seen: HashSet<u64> = HashSet::new();
        for _ in 0..200 {
            let picked =
                AutumnManager::select_nodes(&nodes, &disks, &online_node_ids, &HashSet::new(), 1, &[])
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
                AutumnManager::select_nodes(&nodes, &disks, &online, &low, 3, &[]).unwrap();
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
            AutumnManager::select_nodes(&nodes, &disks, &online, &low2, 3, &[]).unwrap();
        assert_eq!(picked.len(), 3);
    }

    // ── F139: extent-node delete vs in-flight recovery ──────────────────────

    /// dispatch_recovery_task must return Ok without populating recovery_tasks
    /// when the extent is already queued for physical deletion.
    #[test]
    fn f139_dispatch_recovery_skips_when_pending_delete_queued() {
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
                "F139: dispatch_recovery_task must return Ok when delete queued: {result:?}"
            );
            assert!(
                !matches!(
                    m.extent_inflight_op(extent_id),
                    Some(crate::extent_inflight::ExtentOpKind::Recovery)
                ),
                "F139: recovery_tasks must NOT be populated when delete is queued"
            );
        })
    }

    /// handle_stream_punch_holes must return Precondition (not remove the
    /// extent) when the to-be-deleted extent is currently being recovered.
    #[test]
    fn f139_punch_holes_aborts_when_extent_is_in_recovery() {
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
                "F139: punch_holes must be rejected when target extent is in recovery"
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
                "F139: extent must not be removed from store on rejection"
            );
            drop(s);
            // No pending delete must have been enqueued.
            assert!(
                m.delete_progress.borrow().is_empty(),
                "F139: delete_progress must be empty on rejection"
            );
        })
    }

    /// handle_truncate must return Precondition when any to-be-truncated
    /// extent that would drop to refs=0 is currently being recovered.
    #[test]
    fn f139_truncate_aborts_when_any_extent_is_in_recovery() {
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
                "F139: truncate must be rejected when a to-be-removed extent is in recovery"
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
                "F139: extent_a must not be removed from stream on rejection"
            );
        })
    }

    /// Full-race cycle: punch_holes is rejected while recovery is in flight,
    /// then recovery completes, then punch_holes succeeds and extent is
    /// enqueued for physical deletion.
    #[test]
    fn f139_full_race_recovery_after_punch_holes_attempt() {
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
                "F139: extent must be removed from stream after successful punch_holes"
            );
            drop(s);
            // Extent must be queued for physical deletion. F207-C: check
            // both the in-memory progress map and the unified ledger.
            assert!(
                m.delete_progress.borrow().contains_key(&extent_id),
                "F139: extent must be enqueued for physical deletion after refs→0"
            );
            assert_eq!(
                m.extent_inflight_op(extent_id),
                Some(crate::extent_inflight::ExtentOpKind::Delete),
                "F207-C: ledger entry must reflect Delete in flight"
            );
        })
    }

    // ── F145: punch_holes/truncate vs in-flight EC conversion ────────────────

    /// handle_stream_punch_holes must return Precondition (not bump eversion)
    /// when any to-be-removed extent is currently undergoing EC conversion.
    #[test]
    fn f145_punch_holes_refuses_when_ec_inflight() {
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
                "F145: punch_holes must be rejected when target extent is mid-EC"
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
                .expect("F145: extent must not be removed");
            assert_eq!(
                ex.eversion, eversion_before,
                "F145: eversion must not be bumped during mid-EC punch_holes"
            );
            drop(s);
            assert!(
                m.delete_progress.borrow().is_empty(),
                "F145: no pending delete must be enqueued on rejection"
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
                "F145: punch_holes must succeed after EC completes: {}",
                r2.message
            );
            let s2 = m.store.inner.borrow();
            assert!(
                !s2.streams[&stream_id].extent_ids.contains(&extent_id),
                "F145: extent must be removed from stream after successful punch_holes"
            );
        })
    }

    /// handle_truncate must return Precondition (not bump eversion) when any
    /// to-be-truncated extent is currently undergoing EC conversion.
    #[test]
    fn f145_truncate_refuses_when_ec_inflight() {
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
                "F145: truncate must be rejected when a to-be-removed extent is mid-EC"
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
                "F145: extent_a must not be removed from stream on rejection"
            );
            let ex = s
                .extents
                .get(&extent_a)
                .expect("F145: extent_a must still be in store");
            assert_eq!(
                ex.eversion, eversion_before,
                "F145: eversion must not be bumped during mid-EC truncate"
            );
        })
    }

    // ── F146: alloc_extent / split lost-update races ─────────────────────────

    /// handle_stream_alloc_extent must return Precondition (not proceed to
    /// network calls) when the current tail extent is in ec_conversion_inflight.
    #[test]
    fn f146_alloc_extent_refuses_when_ec_inflight() {
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
                "F146: alloc_extent must be rejected when tail is mid-EC"
            );
            assert!(
                r.message.contains("in-flight ConvertToEc"),
                "error must mention in-flight ConvertToEc: {}",
                r.message
            );
            let ev_after = m.store.inner.borrow().extents[&tail_id].eversion;
            assert_eq!(
                ev_after, eversion_before,
                "F146: eversion must not be bumped when alloc_extent is rejected mid-EC"
            );
        })
    }

    /// handle_stream_alloc_extent must return Precondition when the tail
    /// extent has an in-flight recovery task (symmetric to EC guard above).
    #[test]
    fn f146_alloc_extent_refuses_when_recovery_inflight() {
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
                "F146: alloc_extent must be rejected when tail is mid-recovery"
            );
            assert!(
                r.message.contains("in-flight Recovery"),
                "error must mention in-flight Recovery: {}",
                r.message
            );
            let ev_after = m.store.inner.borrow().extents[&tail_id].eversion;
            assert_eq!(
                ev_after, eversion_before,
                "F146: eversion must not be bumped when alloc_extent is rejected mid-recovery"
            );
        })
    }

    /// handle_multi_modify_split must return Precondition when any source-
    /// stream extent is currently undergoing recovery (symmetric to F138's
    /// ec_conversion_inflight guard).
    #[test]
    fn f146_split_refuses_when_recovery_inflight() {
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
            });
            let resp = m.handle_multi_modify_split(req).await.unwrap();
            let r: CodeResp = rkyv_decode(&resp).unwrap();

            assert_ne!(
                r.code, CODE_OK,
                "F146: split must be rejected when source extent is mid-recovery"
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
                "F146: eversion must not be bumped when split is rejected"
            );
            assert!(
                s.partitions.contains_key(&part_id),
                "F146: original partition must still exist on split rejection"
            );
        })
    }

    // ── F198: rich-marker rkyv roundtrip + pending_ec_dispatch bookkeeping ──

    /// The marker value persisted to etcd must round-trip rkyv encode/decode
    /// without losing any field. Pre-F198 the marker had an empty value;
    /// post-F198 the value carries `target_nodes` so re-dispatch after
    /// failover uses the original assignment instead of a fresh shuffle.
    #[test]
    fn f198_ec_dispatch_inflight_rkyv_roundtrip() {
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

    /// F198 / F207-B: the unified inflight ledger (the post-F207-B
    /// successor to `pending_ec_dispatch`) starts empty; acquire +
    /// commit_release round-trip ConvertToEc payloads correctly.
    #[test]
    fn f198_pending_ec_dispatch_in_memory_bookkeeping() {
        let m = AutumnManager::new();
        assert!(
            m.inflight.borrow().is_empty(),
            "F207-B: ledger starts empty"
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
            "F207-B: ledger cleared after release"
        );
    }

    /// F206: `apply_ec_conversion_done` must set `avali` to
    /// `all_bits(K + M)`. Pre-F206 it left `avali` at the pre-EC value
    /// (`all_bits(K)`), leaving the parity slot(s) marked unavailable.
    /// The `recovery_dispatch_loop` then fired RE_AVALI on parity holders
    /// every 2 s, which on the extent-node side ran
    /// `fetch_full_extent_from_sources` and allocated sealed_length-sized
    /// Vec<u8> per peer attempt (observed as multi-GB RSS swings on an
    /// idle cluster after `cluster.sh restart`).
    #[test]
    fn f206_apply_ec_conversion_done_sets_avali_for_all_shards() {
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
                "F206: avali must mark every post-EC slot available; \
                 leaving the parity bit clear causes the recovery loop to \
                 fire RE_AVALI on the parity holder indefinitely"
            );
        })
    }
}
