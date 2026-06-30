use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc::{self, *};
use autumn_rpc::{RpcError, StatusCode};
use bytes::Bytes;

/// F-fuse-lease-1: inode-lease client helpers. Used by autumn-fuse
/// `open`/`release` callbacks. (Originated for the io_uring daemon,
/// which was removed 2026-06-30; autumn-fuse is now the sole caller.)
pub mod lease;

// ── Re-exports for SDK consumers ────────────────────────────────────────────

pub use autumn_rpc::partition_rpc::RangeEntry;

// ── Public helpers ──────────────────────────────────────────────────────────

pub fn parse_addr(addr: &str) -> Result<SocketAddr> {
    addr.parse()
        .with_context(|| format!("invalid address: {addr}"))
}

pub fn decode_err(e: String) -> anyhow::Error {
    anyhow!("rkyv decode: {e}")
}

// ── Error type ──────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum AutumnError {
    NotFound,
    InvalidArgument(String),
    PreconditionFailed(String),
    ServerError(String),
    RoutingError(String),
    ConnectionError(String),
    /// F129/F186: value exceeds the inline `Put` cap. Caller should retry
    /// via `put_stream_begin` / `PutStreamHandle::send` / `commit`
    /// (client-side striped write, no server-side multipart RPC any more).
    ValueTooLarge {
        size: u64,
        cap: u64,
    },
    /// BUG-LEASE-2: the PS fence floor rejected this write — a HIGHER
    /// lease epoch has been seen for this inode, i.e. this writer's
    /// lease was revoked. The caller must stop writing under this lease
    /// and re-acquire.
    Fenced(String),
}

impl std::fmt::Display for AutumnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AutumnError::NotFound => write!(f, "key not found"),
            AutumnError::Fenced(msg) => write!(f, "write fenced (lease revoked): {msg}"),
            AutumnError::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
            AutumnError::PreconditionFailed(msg) => write!(f, "precondition failed: {msg}"),
            AutumnError::ServerError(msg) => write!(f, "server error: {msg}"),
            AutumnError::RoutingError(msg) => write!(f, "routing error: {msg}"),
            AutumnError::ConnectionError(msg) => write!(f, "connection error: {msg}"),
            AutumnError::ValueTooLarge { size, cap } => {
                if *cap > 0 {
                    write!(
                        f,
                        "value {size} bytes exceeds inline cap {cap} — use put_stream_begin"
                    )
                } else {
                    write!(f, "value {size} bytes exceeds the partition server's inline cap — use put_stream_begin")
                }
            }
        }
    }
}

impl std::error::Error for AutumnError {}

/// BUG-LEASE-2 Phase 2: write-fencing identity stamped on lease-covered
/// puts. `ANON` (all-zero) = anonymous write, PS skips fencing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteLease {
    /// Inode this write belongs to (fuse ino). 0 = anonymous.
    pub inode_hint: u64,
    /// Manager lease version (`MgrInodeLeaseRecord.version`) the writer
    /// holds for that inode.
    pub lease_epoch: u64,
}

impl WriteLease {
    pub const ANON: WriteLease = WriteLease { inode_hint: 0, lease_epoch: 0 };
}

fn code_to_error(code: u8, message: String) -> AutumnError {
    match code {
        partition_rpc::CODE_NOT_FOUND => AutumnError::NotFound,
        partition_rpc::CODE_INVALID_ARGUMENT => AutumnError::InvalidArgument(message),
        partition_rpc::CODE_PRECONDITION => AutumnError::PreconditionFailed(message),
        partition_rpc::CODE_FENCED => AutumnError::Fenced(message),
        partition_rpc::CODE_VALUE_TOO_LARGE => {
            // The cap is in the message; we don't try to parse it. Surface the
            // raw size if the caller doesn't already know.
            AutumnError::ValueTooLarge { size: 0, cap: 0 }
        }
        _ => AutumnError::ServerError(message),
    }
}

/// Shared response-code guard: bail with the mapped `code_to_error` when a
/// PS (partition-server) response body carries a non-OK application code.
/// PS-ONLY — `code_to_error` maps `partition_rpc::CODE_*`; manager
/// responses use a different code space (e.g. `CODE_NOT_LEADER`) and must
/// NOT be routed here. Borrows `message` so it is only cloned on the
/// (error) bail path.
fn check_ps_code(code: u8, message: &str) -> std::result::Result<(), AutumnError> {
    if code != partition_rpc::CODE_OK {
        return Err(code_to_error(code, message.to_string()));
    }
    Ok(())
}

/// F225: map an autumn-rpc FRAME-level error (`Err((StatusCode, msg))` returned
/// by a PS handler — e.g. `handle_split_part`'s overlap precondition) into a
/// typed `AutumnError`. `ps_call` returns this (wrapped in `anyhow`) instead of
/// stringifying, so the routing-retry loops can classify terminal vs transient
/// failures by `downcast`-ing rather than string-matching. Distinct from
/// `code_to_error` above, which maps APPLICATION-level `CODE_*` carried in a
/// successful response body.
fn rpc_status_to_error(e: RpcError) -> AutumnError {
    match e {
        RpcError::Status { code, message } => match code {
            StatusCode::NotFound => AutumnError::NotFound,
            StatusCode::InvalidArgument => AutumnError::InvalidArgument(message),
            StatusCode::FailedPrecondition => AutumnError::PreconditionFailed(message),
            // Unavailable is transient (overloaded / draining) → keep retryable.
            StatusCode::Unavailable => AutumnError::ConnectionError(message),
            // Ok-as-error / Internal / AlreadyExists: surface as ServerError
            // (retryable in the loops, same as the pre-F225 stringified path).
            StatusCode::Ok | StatusCode::Internal | StatusCode::AlreadyExists => {
                AutumnError::ServerError(message)
            }
        },
        // Non-status RpcError (ConnectionClosed / Cancelled / Frame / Io) is a
        // transport failure → ConnectionError so the loops refresh + retry.
        other => AutumnError::ConnectionError(other.to_string()),
    }
}

/// F129 client-side hard cap. Matches the PS's `AUTUMN_PS_MAX_INLINE_BYTES_HARD`
/// (256 MiB). Pre-checked in `put_opts` to avoid sending a 256 MB+ request
/// over the wire only to get rejected. The PS may be configured with a
/// stricter (lower) cap; in that case the server still rejects, mapped to
/// `AutumnError::ValueTooLarge` with `cap = 0` (the size + cap parsed
/// from the message would require parsing, which we skip).
pub const CLIENT_PUT_HARD_CAP: u64 = 256 * 1024 * 1024;

/// F216-E — minimum value size for which the UCX zero-copy READ path
/// (`get_into` / `MSG_GET_ZC`) is worth taking. Below this, the per-op
/// registered-recv machinery (regpool_acquire + `UCP_OP_ATTR_FIELD_MEMH` +
/// the 2-stage header/value recv) costs MORE than the single small copy it
/// saves, so a small read regresses (~18% at 4 KiB in the perf_check A/B).
/// At/above this, ZC reads are parity→2.3× (8 MiB). This is the
/// "ucx ⟹ zerocopy" READ default's size guard; WRITES are always ZC on UCX
/// (cheaper at every size — they drop the rkyv encode + value copies).
/// The inline/VP boundary is `VALUE_THROTTLE` (4 KiB); 64 KiB is the
/// conservative crossover (the mid-range is parity, large is a clear win).
pub const UCX_ZC_READ_MIN_BYTES: usize = 64 * 1024;

/// F235: the single zero-copy selection rule. Engage ZC (`get_into`/`MSG_GET_ZC`,
/// `put_zc`/`MSG_PUT_ZC`) iff the value is at least `UCX_ZC_READ_MIN_BYTES`
/// (64 KiB). SYMMETRIC across reads and writes AND across transports — it mirrors
/// the PS-side recv gates (`UCX_ZC_READ_MIN_BYTES` here + `AUTUMN_PS_ZC_RECV_MIN_BYTES`
/// on the PS, both 64 KiB). Below 64 KiB the per-op registered/pooled-recv machinery
/// costs more than the copy it saves (small UCX read regresses ~18%) AND the PS recv
/// side doesn't ZC anyway, so end-to-end ZC only engages at/above 64 KiB. This is the
/// ONE source of truth — perf-check, the python `BatchClient`, and `get_many_into`
/// all call it (F234 found 3 hand-rolled copies had drifted; F235 collapsed them and
/// made the write rule symmetric — was `is_ucx || large`, which only saved
/// client-side allocs on small UCX writes while the PS still FrameDecoder-copied).
pub fn zc_worthwhile(value_size: usize) -> bool {
    value_size >= UCX_ZC_READ_MIN_BYTES
}

/// F235: default in-flight concurrency for `get_many_into` (sliding-window pipeline
/// over the per-partition multiplexed connections). Mirrors the stream worker's
/// inflight cap; modest to dodge the UCX rendezvous cliff on large batches.
pub const BATCH_GET_DEFAULT_CONCURRENCY: usize = 32;

/// F236: default in-flight concurrency for `put_many` (write sibling of
/// `BATCH_GET_DEFAULT_CONCURRENCY`).
pub const BATCH_PUT_DEFAULT_CONCURRENCY: usize = 32;

/// F245: the streaming fan-out primitive — the ONE concurrency base under every
/// batch API. Drives `futs` with bounded `concurrency` (sliding window) and
/// yields `(input_index, output)` as each future COMPLETES (completion order,
/// NOT input order). This is the same SQ/CQ "fire N, reap as they land" shape the
/// EN/PS server loops use, lifted to the client.
///
/// Two consumption modes on top of it:
/// - **collect** ([`fan_out_collect`]): re-order into an index-aligned `Vec`
///   (`get_many_into` / `put_many` / `delete_many` / `head_many`).
/// - **streaming** (caller drives the stream): act on each completion as it lands
///   — e.g. the io_uring daemon pushing one CQE per finished SQE, with no
///   head-of-line wait on the rest of the batch.
pub fn fan_out<Fut: std::future::Future>(
    futs: impl IntoIterator<Item = Fut>,
    concurrency: usize,
) -> impl futures::Stream<Item = (usize, Fut::Output)> {
    use futures::stream::StreamExt;
    futures::stream::iter(
        futs.into_iter()
            .enumerate()
            .map(|(i, f)| async move { (i, f.await) }),
    )
    .buffer_unordered(concurrency.max(1))
}

/// F245: collect convenience over [`fan_out`] — runs all `futs` with bounded
/// `concurrency` and returns the outputs in INPUT order (result `i` ⇔ `futs[i]`).
pub async fn fan_out_collect<Fut: std::future::Future>(
    futs: impl IntoIterator<Item = Fut>,
    concurrency: usize,
) -> Vec<Fut::Output> {
    use futures::stream::StreamExt;
    let mut stream = fan_out(futs, concurrency);
    let mut out: Vec<Option<Fut::Output>> = Vec::new();
    while let Some((i, o)) = stream.next().await {
        if out.len() <= i {
            out.resize_with(i + 1, || None);
        }
        out[i] = Some(o);
    }
    out.into_iter()
        .map(|o| o.expect("fan_out yields each input index exactly once"))
        .collect()
}

// ── F212-fix-2 — TiKV-style retry/backoff for async-split-tolerant routing ─
//
// Split is async on this codebase (see `crates/partition-server/CLAUDE.md`
// "Partition Split"): `handle_split_part` returns the moment
// `multi_modify_split` commits to etcd. The new sibling is opened on its
// assigned PS asynchronously by `region_sync_loop` (~2 s tick) +
// `open_partition`'s log_stream replay (variable; tens of seconds for
// deep WAL tails). The SOURCE partition is ALSO dropped + reopened on
// its host PS because `opened_with` no longer matches the post-split
// (rg, region_epoch). During those windows clients see either
// CODE_NOT_FOUND (mis-routed via `ps_details` fallback under F099-K)
// or TCP-refused/timeout (listener torn down for reopen).
//
// Modelled on `tikv-client-rust/src/backoff.rs::NoJitterBackoff`:
// exponential backoff with cap + bounded retry count + refresh routing
// cache between attempts. TiKV defaults `(base=2ms, cap=500ms,
// attempts=10)` work because their region split is ~100 ms; our
// `open_partition` replay can run ~1000× longer, so the budget is
// correspondingly larger. CockroachDB's DistSender + Cassandra's
// coordinator retry loops follow the same pattern: hide async control
// plane convergence behind client-side retry so the user sees one
// "send-receive" with a latency spike, not a hard error during
// topology change.
//
// Defaults: `MAX_PS_REFRESHES = 10`, `base=100ms`, `cap=2000ms`.
// Cumulative max wait through attempt 10: ~9 s. Typical 1-2 retries
// finish in <500 ms. This budget targets the inherent right-child
// open window (~2-4 s). It is deliberately NOT large enough to mask
// PS-side correctness bugs that produce minute-scale unavailability
// (e.g., the F212-fix-2 `opened_with` staleness that caused the
// SOURCE partition to be needlessly dropped + reopened by
// `sync_regions_once`). Such bugs should fail loudly here so they
// get fixed at the source, not papered over with bigger retries.
pub(crate) const MAX_PS_REFRESHES: u32 = 10;

/// TiKV-style `NoJitterBackoff` step: sleep BEFORE the Nth refresh.
/// Sequence: 100, 200, 400, 800, 1600, 2000, 2000, ..., 2000 ms.
/// Cumulative: 100, 300, 700, 1500, 3100, 5100, 7100, 9100 ms at
/// attempt=10. Attempt 0 returns `Duration::ZERO` for callers that
/// gate the first call without sleeping.
pub(crate) fn refresh_backoff(attempt: u32) -> Duration {
    if attempt == 0 {
        return Duration::ZERO;
    }
    let shift = (attempt - 1).min(10);
    let ms = 100u64.saturating_mul(1u64 << shift);
    Duration::from_millis(ms.min(2000))
}

// ── Range scan result ───────────────────────────────────────────────────────

pub struct RangeResult {
    pub entries: Vec<RangeEntry>,
    pub has_more: bool,
}

// ── Key metadata ────────────────────────────────────────────────────────────

pub struct KeyMeta {
    pub found: bool,
    pub value_length: u64,
}

/// One item for [`ClusterClient::get_many_into`]. `length == 0` reads the
/// whole value; `offset`/`length` give a sub-range (fuse chunk reads, io_uring
/// `sqe.offset`/`len`). `dest` is sized by the caller (e.g. from `head`, a fixed
/// chunk size, or the ring-buffer slot) and MUST outlive the call.
///
/// UCX RDMA into `dest` is automatic: the first read into a fresh address
/// pays a one-time UCX rcache miss (~100 µs `ibv_reg_mr`); subsequent reads
/// into the SAME address hit the rcache for free. Power users who want to
/// pre-warm the rcache (skip the first-call cost on a hot path) can call
/// `autumn_transport::register_memory` directly — the registration lands in
/// the same rcache and the SDK finds it transparently. No SDK-API hook
/// needed.
pub struct GetManyItem<'a> {
    pub key: &'a [u8],
    pub offset: u32,
    pub length: u32,
    pub dest: &'a mut [u8],
}

// ── ClusterClient ───────────────────────────────────────────────────────────

/// Client for interacting with an autumn-rs cluster.
///
/// Supports multiple manager addresses with round-robin failover on
/// NotLeader or connection errors. PS connections auto-reconnect on failure.
///
/// **All hot-path methods take `&self`** so an `Rc<ClusterClient>` can be
/// shared across concurrent compio tasks (e.g., FUSE dispatcher spawning
/// per-request tasks). Internal mutability is provided via `RefCell` for
/// the routing caches and `Cell` for the manager round-robin index.
/// Borrows are deliberately scoped: every `borrow()` / `borrow_mut()` is
/// released before any `.await`.
pub struct ClusterClient {
    /// Manager addresses (comma-separated on construction).
    manager_addrs: Vec<String>,
    /// Current manager index (round-robin).
    current_mgr: Cell<usize>,
    /// Cached manager RPC connection. Recreated on error.
    mgr_conn: Rc<RefCell<Option<Rc<RpcClient>>>>,
    /// Cached PS RPC connections. Dropped on error, recreated on next use.
    ps_conns: RefCell<HashMap<String, Rc<RpcClient>>>,
    /// F259: extent-node connections for redirect direct reads.
    en_pool: autumn_stream::ConnPool,
    /// Routing cache. Populated on `connect`, refreshed on `refresh_regions`.
    /// `RefCell` so concurrent tasks holding `Rc<ClusterClient>` can do
    /// brief lookup-and-clone borrows without blocking each other.
    regions: RefCell<Vec<(u64, MgrRegionInfo)>>,
    ps_details: RefCell<HashMap<u64, MgrPsDetail>>,
    /// F099-K — per-partition listener addresses, indexed by `part_id`.
    /// When an entry is present, it supersedes `ps_details[ps_id].address`
    /// for routing decisions (thread-per-partition shard target).
    part_addrs: RefCell<HashMap<u64, String>>,
    /// Per-call timeout for ALL cluster RPCs (PS-bound AND manager-bound).
    ///
    /// Default = `Some(DEFAULT_RPC_TIMEOUT)` — never wait forever. The
    /// principle is "an unbounded RPC is a bug": a paged-out / hung peer
    /// must surface as `ConnectionError` so the caller's
    /// `refresh_regions` + retry path (or the user's own timeout) can
    /// engage.
    ///
    /// `set_rpc_timeout(Duration)` overrides for tighter loops (tests
    /// driving split/merge typically use 2 s); `clear_rpc_timeout()`
    /// resets back to the default (NOT to `None` — wait-forever is no
    /// longer a supported mode).
    ///
    /// Originally introduced as F184 to fix the partition-server
    /// dropping a partition's `req_rx` mid-call (region_sync_loop
    /// reload after merge / graceful shutdown drain): the per-request
    /// response oneshot closes WITHOUT closing the underlying TCP
    /// connection (other partitions on the same PS still use it).
    /// autumn-rpc's F121 `closed: Cell<bool>` flag fires on TCP close
    /// but not on req_rx drop, so without a per-call timeout the
    /// caller's `.await` would hang forever.
    ///
    /// Now also applied to `mgr_call` so a paged-out / hung manager
    /// surfaces as `ConnectionError`; the existing `mgr_call_retry`
    /// path's `rotate_manager` then walks to the next manager address.
    rpc_timeout: Cell<Option<Duration>>,
    /// Bug #2 fix (2026-06-06) — first-attempt timeout for
    /// `call_ps_for_key` / `call_ps_for_part`. See
    /// `DEFAULT_FIRST_ATTEMPT_TIMEOUT` for the rationale. `None`
    /// disables fast-fail (every attempt uses the full `rpc_timeout`).
    first_attempt_timeout: Cell<Option<Duration>>,
}

/// Default per-call timeout for cluster RPCs. Generous enough to cover
/// realistic slow-disk + 3-replica fanout + a couple of retries inside
/// the server, tight enough to prevent open-ended hangs from a
/// paged-out / dead peer.
pub const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// Bug #2 fix (2026-06-06) — short timeout used ONLY for the first
/// attempt of `call_ps_for_key` / `call_ps_for_part`. Subsequent
/// retries fall back to the full `DEFAULT_RPC_TIMEOUT`.
///
/// **Why this exists:** when a PS process is killed and restarted
/// (or the manager evicts + reassigns a partition between PS instances),
/// the SDK's cached `ps_conns[ps_addr]` points at the dead listener.
/// The next call against that conn hangs until TCP RST (which can take
/// minutes on a `kill -9` of the listener) OR the 30 s
/// `DEFAULT_RPC_TIMEOUT` — whichever wins. Either way, every key
/// touching that PS pays a 30 s penalty before the retry loop can
/// drop the conn + reconnect.
///
/// With this 5 s first-attempt cap:
///   - Healthy steady-state calls return well under 5 s → no change.
///   - Stale-conn calls fail at 5 s → `ps_conns.remove(ps_addr)` →
///     `refresh_regions()` + retry uses the FULL 30 s timeout against
///     a freshly-resolved listener. Total worst case: ~5 s + a healthy
///     RTT, vs. ~30 s pre-fix.
///   - Legit long calls (rare, e.g. bulk stream_put of multi-GiB) fail
///     fast on attempt 0 then succeed on attempt 1. Costs ~5 s of
///     extra latency for callers that can run > 5 s — acceptable
///     trade-off and avoidable via `set_rpc_timeout` for callers that
///     know they need it.
///
/// Tunable via `set_first_attempt_timeout`; `None` disables the
/// fast-fail (every attempt uses the full `rpc_timeout`).
pub const DEFAULT_FIRST_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

impl ClusterClient {
    /// Current manager address.
    fn manager_addr(&self) -> &str {
        &self.manager_addrs[self.current_mgr.get() % self.manager_addrs.len()]
    }

    /// Rotate to next manager.
    fn rotate_manager(&self) {
        if self.manager_addrs.len() > 1 {
            let next = (self.current_mgr.get() + 1) % self.manager_addrs.len();
            self.current_mgr.set(next);
            // Drop cached connection so next call reconnects to new manager
            *self.mgr_conn.borrow_mut() = None;
        }
    }

    /// Get or create a manager RPC connection. Auto-reconnects on failure.
    async fn mgr_client(&self) -> Result<Rc<RpcClient>> {
        {
            let guard = self.mgr_conn.borrow();
            if let Some(c) = guard.as_ref() {
                return Ok(c.clone());
            }
        }
        let addr = parse_addr(self.manager_addr())?;
        let client = RpcClient::connect(addr)
            .await
            .with_context(|| format!("connect manager {}", self.manager_addr()))?;
        *self.mgr_conn.borrow_mut() = Some(client.clone());
        Ok(client)
    }

    /// Call the current manager. On error, drop connection (auto-reconnect next time).
    ///
    /// Honors `rpc_timeout` (default `DEFAULT_RPC_TIMEOUT`). A timeout
    /// surfaces as a transport error; `mgr_call_retry`'s
    /// `rotate_manager` walks to the next manager address on the next
    /// attempt.
    pub async fn mgr_call(&self, msg_type: u8, payload: Bytes) -> Result<Bytes> {
        let client = self.mgr_client().await?;
        let outcome = match self.rpc_timeout.get() {
            None => client.call(msg_type, payload).await,
            Some(t) => client.call_timeout(msg_type, payload, t).await,
        };
        match outcome {
            Ok(resp) => Ok(resp),
            Err(e) => {
                // Drop connection so next call reconnects
                *self.mgr_conn.borrow_mut() = None;
                Err(anyhow!("{e}"))
            }
        }
    }

    /// cluster-df: fetch the aggregate cluster capacity summary
    /// (`MSG_CLUSTER_DF`). Shared by `autumn-op df` and the FUSE statfs
    /// background refresh. A follower answers `CODE_NOT_LEADER`; only the
    /// leader's `node_health_loop` maintains the snapshot, so we round-robin
    /// to the next manager on that (mgr_call_retry only rotates on transport
    /// errors — NOT_LEADER comes back as a successful response body).
    pub async fn cluster_df(&self) -> Result<ClusterDfResp> {
        let req = rkyv_encode(&ClusterDfReq {});
        let mut attempt = 0u32;
        loop {
            let bytes = self.mgr_call_retry(MSG_CLUSTER_DF, req.clone(), 3).await?;
            let resp: ClusterDfResp = rkyv_decode(&bytes).map_err(|e| anyhow!("{e}"))?;
            if resp.code == CODE_NOT_LEADER {
                attempt += 1;
                if attempt > 3 {
                    return Err(anyhow!("cluster_df: no leader available"));
                }
                self.rotate_manager();
                compio::time::sleep(Duration::from_millis(300)).await;
                continue;
            }
            if resp.code != autumn_rpc::manager_rpc::CODE_OK {
                return Err(anyhow!("cluster_df failed: {}", resp.message));
            }
            return Ok(resp);
        }
    }

    /// Call manager with retry and round-robin on NotLeader/connection error.
    pub async fn mgr_call_retry(
        &self,
        msg_type: u8,
        payload: Bytes,
        max_retries: u32,
    ) -> Result<Bytes> {
        let mut attempt = 0u32;
        loop {
            match self.mgr_call(msg_type, payload.clone()).await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    attempt += 1;
                    if attempt > max_retries {
                        return Err(e.context(format!("failed after {max_retries} retries")));
                    }
                    self.rotate_manager();
                    compio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    pub fn mgr(&self) -> Result<Rc<RpcClient>> {
        self.mgr_conn
            .borrow()
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("manager not connected"))
    }

    /// Connect to the cluster. Accepts comma-separated manager addresses.
    pub async fn connect(manager: &str) -> Result<Self> {
        let manager_addrs: Vec<String> = manager.split(',').map(|s| s.trim().to_string()).collect();

        let client = Self {
            manager_addrs,
            current_mgr: Cell::new(0),
            mgr_conn: Rc::new(RefCell::new(None)),
            ps_conns: RefCell::new(HashMap::new()),
            en_pool: autumn_stream::ConnPool::new(),
            regions: RefCell::new(Vec::new()),
            ps_details: RefCell::new(HashMap::new()),
            part_addrs: RefCell::new(HashMap::new()),
            rpc_timeout: Cell::new(Some(DEFAULT_RPC_TIMEOUT)),
            first_attempt_timeout: Cell::new(Some(DEFAULT_FIRST_ATTEMPT_TIMEOUT)),
        };

        // Try connecting to each manager until one responds
        let mut connected = false;
        for idx in 0..client.manager_addrs.len() {
            client.current_mgr.set(idx);
            match client.mgr_client().await {
                Ok(_) => {
                    connected = true;
                    break;
                }
                Err(_) => continue,
            }
        }
        if !connected {
            return Err(anyhow!("cannot connect to any manager: {}", manager));
        }

        // WIRE-1: startup wire-schema cross-check. A SUCCESSFUL response
        // with a different fingerprint is a hard refusal (mixed
        // same-commit deploy — rkyv would decode garbage; the F275 stale
        // python wheel failed exactly this way, silently). A transport
        // failure is skipped: availability wins while the manager is
        // briefly down, and every RPC after this would fail loudly anyway.
        if let Ok(resp_bytes) = client
            .mgr_call(MSG_GET_CLUSTER_ID, rkyv_encode(&GetClusterIdReq {}))
            .await
        {
            // A SUCCESSFUL response that fails to DECODE is itself the
            // mismatch this check exists for (an older manager's resp
            // lacks the field) — hard fail, never skip (coco P1).
            let resp = rkyv_decode::<GetClusterIdResp>(&resp_bytes).map_err(|e| {
                anyhow!("decode GetClusterIdResp failed ({e}) — possible wire-schema mismatch; rebuild from the cluster's commit")
            })?;
            // R1: interval-overlap compat check (same-fingerprint fast
            // path inside; refusal message carries both intervals).
            if let Err(msg) = autumn_rpc::wire_compat_check(
                &resp.wire_fingerprint,
                resp.wire_version_min,
                resp.wire_version_max,
            ) {
                return Err(anyhow!(msg));
            }
        }

        client.refresh_regions().await?;
        Ok(client)
    }

    pub async fn refresh_regions(&self) -> Result<()> {
        // F267: get_regions is leader-gated (a follower's part_addrs view
        // is empty/stale — manager-HA chaos H3 black-holed every client
        // that connected to a rejoined follower first). On NOT_LEADER,
        // rotate and retry until the leader answers.
        let mut resp: Option<GetRegionsResp> = None;
        for _ in 0..(self.manager_addrs.len() * 3).max(3) {
            let resp_bytes = self
                .mgr_call_retry(MSG_GET_REGIONS, Bytes::new(), 3)
                .await
                .context("get regions")?;
            let r: GetRegionsResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
            if r.code == CODE_NOT_LEADER {
                self.rotate_manager();
                *self.mgr_conn.borrow_mut() = None;
                compio::time::sleep(Duration::from_millis(300)).await;
                continue;
            }
            resp = Some(r);
            break;
        }
        let resp =
            resp.ok_or_else(|| anyhow!("get regions: no leader among {:?}", self.manager_addrs))?;
        let mut sorted: Vec<(u64, MgrRegionInfo)> = resp.regions.into_iter().collect();
        sorted.sort_by(|a, b| {
            a.1.rg
                .as_ref()
                .map(|r| r.start_key.as_slice())
                .unwrap_or(&[])
                .cmp(
                    b.1.rg
                        .as_ref()
                        .map(|r| r.start_key.as_slice())
                        .unwrap_or(&[]),
                )
        });
        for i in 0..sorted.len().saturating_sub(1) {
            let end_key = sorted[i]
                .1
                .rg
                .as_ref()
                .map(|r| r.end_key.as_slice())
                .unwrap_or(&[]);
            let next_start = sorted[i + 1]
                .1
                .rg
                .as_ref()
                .map(|r| r.start_key.as_slice())
                .unwrap_or(&[]);
            if end_key != next_start {
                eprintln!(
                    "WARNING: region gap: partition {} end_key != partition {} start_key",
                    sorted[i].1.part_id,
                    sorted[i + 1].1.part_id
                );
            }
        }
        // Brief swap — no .await held under any borrow.
        *self.regions.borrow_mut() = sorted;
        *self.ps_details.borrow_mut() = resp.ps_details.into_iter().collect();
        *self.part_addrs.borrow_mut() = resp.part_addrs.into_iter().collect();
        Ok(())
    }

    /// Wait until every partition reported by the manager has a
    /// registered `part_addr` (the per-partition F099-K listener) AND
    /// each `part_addr` accepts TCP connections. Polls `refresh_regions`
    /// every `poll_interval` until the readiness condition holds or
    /// `timeout` elapses.
    ///
    /// **Why this is needed (Bug #1 fix, 2026-06-06):** right after
    /// `cluster.sh start` bootstraps a presplit cluster, the manager
    /// returns `MgrRegionInfo` for every partition before each
    /// partition's PS thread has bound its dedicated `base_port + ord`
    /// listener AND called `RegisterPartitionAddr`. Until that
    /// register lands, `part_addrs[part_id]` is missing → the SDK's
    /// `lookup_key` falls back to `ps_details[ps_id].address` (the
    /// base PS port). For a partition whose `ord != 0`, sending its
    /// PutReq to the base port hits a `partition_loop` that owns a
    /// DIFFERENT partition → mis-routed → synthesised `NotFound`
    /// frame → `call_ps_for_key` retries 10×, exhausts → caller
    /// (e.g. autumn-fuse's `init_root` kv_put) returns
    /// `connection error: ps_call after 10 refreshes: key not found`.
    ///
    /// Callers that boot against a possibly-fresh cluster (fuse mount
    /// daemon, ioring daemon, long-lived service binaries) should call
    /// this between `connect` and the first request to ride out the
    /// startup window. Short-lived one-shot tools (autumn-client CLI
    /// invocations from operators) can skip it: a transient
    /// `key not found` surfacing to the operator IS the right UX.
    ///
    /// Returns `Ok(())` on readiness or `Err` on timeout. The error
    /// includes a count of partitions still missing `part_addr` so the
    /// caller can decide whether to retry or fail-fast.
    pub async fn wait_for_cluster_ready(
        &self,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        loop {
            self.refresh_regions().await?;
            let summary = {
                let regions = self.regions.borrow();
                let part_addrs = self.part_addrs.borrow();
                let ps_details = self.ps_details.borrow();
                let regions_view: Vec<(u64, u64)> = regions
                    .iter()
                    .map(|(_, r)| (r.part_id, r.ps_id))
                    .collect();
                let part_addrs_view: HashMap<u64, String> = part_addrs.clone();
                let ps_details_view: HashMap<u64, String> = ps_details
                    .iter()
                    .map(|(id, d)| (*id, d.address.clone()))
                    .collect();
                cluster_ready_summary(&regions_view, &part_addrs_view, &ps_details_view)
            };

            if summary.total_partitions == 0 {
                if start.elapsed() >= timeout {
                    return Err(anyhow!(
                        "cluster_ready: manager reports zero partitions after {:?}",
                        timeout
                    ));
                }
                compio::time::sleep(poll_interval).await;
                continue;
            }
            if summary.missing_addrs.is_empty() {
                // Every partition has SOME ps_addr cached. Probe each
                // distinct ps_addr with one quick TCP connect to weed
                // out the case where the manager reports a listener
                // that the PS process hasn't actually bound yet.
                let mut unreachable: Vec<String> = Vec::new();
                for addr in &summary.probe_addrs {
                    match self.get_ps_client(addr).await {
                        Ok(_) => {}
                        Err(_) => unreachable.push(addr.clone()),
                    }
                }
                if unreachable.is_empty() {
                    return Ok(());
                }
                if start.elapsed() >= timeout {
                    return Err(anyhow!(
                        "cluster_ready: {} PS endpoint(s) unreachable after {:?}: {:?}",
                        unreachable.len(),
                        timeout,
                        unreachable
                    ));
                }
            } else if start.elapsed() >= timeout {
                return Err(anyhow!(
                    "cluster_ready: {} of {} partition(s) still missing part_addr after {:?}: {:?}",
                    summary.missing_addrs.len(),
                    summary.total_partitions,
                    timeout,
                    summary.missing_addrs
                ));
            }
            compio::time::sleep(poll_interval).await;
        }
    }

    /// Get or create a PS RPC connection. Auto-reconnects on failure.
    pub async fn get_ps_client(&self, ps_addr: &str) -> Result<Rc<RpcClient>> {
        {
            let conns = self.ps_conns.borrow();
            if let Some(c) = conns.get(ps_addr) {
                return Ok(c.clone());
            }
        }
        let addr = parse_addr(ps_addr)?;
        let client = RpcClient::connect(addr)
            .await
            .with_context(|| format!("connect PS {ps_addr}"))?;
        self.ps_conns
            .borrow_mut()
            .insert(ps_addr.to_string(), client.clone());
        Ok(client)
    }

    /// Call a PS. On error, drop connection (auto-reconnect next time).
    ///
    /// Honors `rpc_timeout` (default `DEFAULT_RPC_TIMEOUT`); see the
    /// field docstring for the partition-handle-drop hang this guards
    /// against.
    pub async fn ps_call(&self, ps_addr: &str, msg_type: u8, payload: Bytes) -> Result<Bytes> {
        self.ps_call_with_timeout(ps_addr, msg_type, payload, self.rpc_timeout.get())
            .await
    }

    /// Bug #2 fix (2026-06-06) — `ps_call` variant that takes an
    /// explicit per-call timeout override. `call_ps_for_key` /
    /// `call_ps_for_part` use this on attempt 0 to fast-fail a stale
    /// PS connection (the post-`kill -9`-then-restart window) without
    /// waiting the full 30 s `rpc_timeout`. See
    /// `first_attempt_effective_timeout`.
    pub async fn ps_call_with_timeout(
        &self,
        ps_addr: &str,
        msg_type: u8,
        payload: Bytes,
        timeout: Option<Duration>,
    ) -> Result<Bytes> {
        let client = self.get_ps_client(ps_addr).await?;
        let outcome = match timeout {
            None => client.call(msg_type, payload).await,
            Some(t) => client.call_timeout(msg_type, payload, t).await,
        };
        match outcome {
            Ok(resp) => Ok(resp),
            Err(e) => {
                // Drop connection so next call reconnects
                self.ps_conns.borrow_mut().remove(ps_addr);
                // F225: preserve the typed error inside anyhow (downcastable)
                // instead of stringifying, so the routing-retry loops can tell
                // a deterministic precondition from a transient routing miss.
                Err(anyhow::Error::new(rpc_status_to_error(e)))
            }
        }
    }

    /// Override the per-call timeout for ALL cluster RPCs (PS + manager).
    /// Default is `DEFAULT_RPC_TIMEOUT` (30 s). Recommended in
    /// production: 2-5 s for read-heavy loads, 30+ s for bulk uploads.
    /// Tests that drive split/merge typically use 2 s.
    pub fn set_rpc_timeout(&self, t: Duration) {
        self.rpc_timeout.set(Some(t));
    }

    /// Reset the per-call timeout to the crate default
    /// (`DEFAULT_RPC_TIMEOUT`). Wait-forever is no longer a supported
    /// mode — every RPC must be bounded.
    pub fn clear_rpc_timeout(&self) {
        self.rpc_timeout.set(Some(DEFAULT_RPC_TIMEOUT));
    }

    /// Read the current per-call timeout. Mostly for tests.
    /// Always `Some(_)` in normal use; `None` only appears if a caller
    /// has explicitly opted out (discouraged).
    pub fn rpc_timeout(&self) -> Option<Duration> {
        self.rpc_timeout.get()
    }

    /// Bug #2 fix (2026-06-06) — override the first-attempt timeout
    /// for `call_ps_for_key` / `call_ps_for_part`. See
    /// `DEFAULT_FIRST_ATTEMPT_TIMEOUT` for the rationale.
    pub fn set_first_attempt_timeout(&self, t: Option<Duration>) {
        self.first_attempt_timeout.set(t);
    }

    /// Read the current first-attempt timeout.
    pub fn first_attempt_timeout(&self) -> Option<Duration> {
        self.first_attempt_timeout.get()
    }

    /// Resolve the effective timeout for the Nth attempt of a key /
    /// part-bound RPC. Pure-fn so the policy is unit-testable.
    ///   - `attempt == 0`: `min(rpc_timeout, first_attempt_timeout)`
    ///     if both are Some — fast-fail a stale conn within the
    ///     first-attempt budget so the retry path engages quickly.
    ///     If `first_attempt_timeout` is None: use `rpc_timeout`.
    ///   - `attempt > 0`: `rpc_timeout` — the cached conn is freshly
    ///     dropped, the refresh has run, give the new connection the
    ///     full budget.
    ///
    /// Both `None` ⇒ `None` (wait forever — discouraged but supported
    /// for callers that explicitly opted out).
    fn first_attempt_effective_timeout(&self, attempt: u32) -> Option<Duration> {
        let rpc_t = self.rpc_timeout.get();
        if attempt > 0 {
            return rpc_t;
        }
        match (rpc_t, self.first_attempt_timeout.get()) {
            (Some(r), Some(f)) => Some(r.min(f)),
            (Some(r), None) => Some(r),
            (None, Some(f)) => Some(f),
            (None, None) => None,
        }
    }

    /// Look up the cached `region_epoch` for a partition. Returns 0 when
    /// the partition is unknown (no cached region) — `0` is the wire-level
    /// "skip check" sentinel so a request stamped with 0 is processed
    /// without epoch validation. Used internally by `call_ps_for_key`
    /// + `call_ps_for_part`; FUSE / ioring / CLI perf-check call it
    /// directly when assembling a Req struct manually.
    pub fn lookup_epoch_for_part(&self, part_id: u64) -> u64 {
        self.regions
            .borrow()
            .iter()
            .find(|(_, r)| r.part_id == part_id)
            .map(|(_, r)| r.region_epoch)
            .unwrap_or(0)
    }

    pub fn lookup_key(&self, key: &[u8]) -> Option<(u64, String)> {
        let regions = self.regions.borrow();
        if regions.is_empty() {
            return None;
        }
        let idx = regions.partition_point(|(_, region)| match region.rg.as_ref() {
            Some(rg) if !rg.end_key.is_empty() => rg.end_key.as_slice() <= key,
            _ => false,
        });
        if idx >= regions.len() {
            return None;
        }
        let (_, region) = &regions[idx];
        // F099-K: prefer per-partition listener if registered.
        let part_addrs = self.part_addrs.borrow();
        let ps_details = self.ps_details.borrow();
        let addr = match part_addrs.get(&region.part_id) {
            Some(a) => a.clone(),
            None => ps_details.get(&region.ps_id)?.address.clone(),
        };
        Some((region.part_id, addr))
    }

    pub async fn resolve_key(&self, key: &[u8]) -> Result<(u64, String)> {
        if let Some(result) = self.lookup_key(key) {
            return Ok(result);
        }
        self.refresh_regions().await?;
        self.lookup_key(key)
            .ok_or_else(|| anyhow!("key is out of range"))
    }

    pub async fn resolve_part_id(&self, part_id: u64) -> Result<String> {
        // F099-K: prefer the per-partition listener address when
        // registered; fall back to the PS-level base address otherwise.
        // Borrows are scoped — released before any `.await`.
        let lookup = |this: &Self| -> Option<String> {
            let regions = this.regions.borrow();
            let region = regions.iter().find(|(_, r)| r.part_id == part_id)?;
            let part_addrs = this.part_addrs.borrow();
            if let Some(a) = part_addrs.get(&region.1.part_id) {
                return Some(a.clone());
            }
            let ps_details = this.ps_details.borrow();
            ps_details.get(&region.1.ps_id).map(|d| d.address.clone())
        };
        if let Some(addr) = lookup(self) {
            return Ok(addr);
        }
        self.refresh_regions().await?;
        lookup(self).ok_or_else(|| anyhow!("partition {} not found", part_id))
    }

    pub async fn all_partitions(&self) -> Result<Vec<(u64, String)>> {
        if self.regions.borrow().is_empty() {
            self.refresh_regions().await?;
        }
        let regions = self.regions.borrow();
        let part_addrs = self.part_addrs.borrow();
        let ps_details = self.ps_details.borrow();
        let mut result: Vec<(u64, String)> = regions
            .iter()
            .map(|(_, region)| {
                // F099-K: prefer per-partition listener address when present.
                let addr = part_addrs
                    .get(&region.part_id)
                    .cloned()
                    .or_else(|| ps_details.get(&region.ps_id).map(|d| d.address.clone()))
                    .unwrap_or_default();
                (region.part_id, addr)
            })
            .collect();
        result.sort_by_key(|(pid, _)| *pid);
        Ok(result)
    }

    /// F099-N-c — like `all_partitions`, but also returns each partition's
    /// `(start_key, end_key)` range so bench tools can generate keys that
    /// actually land in each partition. Prior bench tools used a constant
    /// prefix like "pc_{tid}_{seq}" / "bench_{tid}_{seq}" which lexically
    /// always fell in ONE partition, making N>1 perf tests measure a single
    /// partition with (N-1) rejecting load.
    pub async fn all_partitions_with_range(&self) -> Result<Vec<(u64, String, Vec<u8>, Vec<u8>)>> {
        if self.regions.borrow().is_empty() {
            self.refresh_regions().await?;
        }
        let regions = self.regions.borrow();
        let part_addrs = self.part_addrs.borrow();
        let ps_details = self.ps_details.borrow();
        let mut result: Vec<(u64, String, Vec<u8>, Vec<u8>)> = regions
            .iter()
            .map(|(_, region)| {
                let addr = part_addrs
                    .get(&region.part_id)
                    .cloned()
                    .or_else(|| ps_details.get(&region.ps_id).map(|d| d.address.clone()))
                    .unwrap_or_default();
                let (start_key, end_key) = region
                    .rg
                    .as_ref()
                    .map(|r| (r.start_key.clone(), r.end_key.clone()))
                    .unwrap_or_default();
                (region.part_id, addr, start_key, end_key)
            })
            .collect();
        result.sort_by_key(|(pid, _, _, _)| *pid);
        Ok(result)
    }

    // ── Internal: PS call with routing retry ─────────────────────────────────

    /// Resolve key to (part_id, ps_addr), call PS, retry with TiKV-style
    /// backoff on failure.
    ///
    /// F212-fix-2: same retry shape as `range()` so point queries
    /// (get/put/delete/head/stream_put) absorb the same async-split
    /// / async-merge convergence window. Pre-fix this had only ONE
    /// retry with no backoff — adequate for stable steady state but
    /// not for the post-split window where the right child takes
    /// seconds to open. See module-level `refresh_backoff` doc for
    /// the TiKV `NoJitterBackoff` derivation.
    ///
    /// The `build_payload` closure is invoked with `(part_id, region_epoch)`
    /// so callers can stamp the epoch from the SDK's routing cache into
    /// every request. After a refresh the closure is re-invoked with the
    /// freshly-cached epoch.
    async fn call_ps_for_key(
        &self,
        key: &[u8],
        msg_type: u8,
        build_payload: impl Fn(u64, u64) -> Bytes,
    ) -> std::result::Result<Bytes, AutumnError> {
        let mut attempt: u32 = 0;
        let mut last_err: Option<String> = None;
        while attempt <= MAX_PS_REFRESHES {
            let (part_id, ps_addr) = self
                .resolve_key(key)
                .await
                .map_err(|e| AutumnError::RoutingError(e.to_string()))?;
            let region_epoch = self.lookup_epoch_for_part(part_id);
            // Bug #2 fix: fast-fail a stale conn within
            // `first_attempt_effective_timeout(0)` instead of the full
            // 30 s `rpc_timeout`. Subsequent attempts use the full
            // budget against the freshly-reconnected listener.
            let t = self.first_attempt_effective_timeout(attempt);
            match self
                .ps_call_with_timeout(&ps_addr, msg_type, build_payload(part_id, region_epoch), t)
                .await
            {
                Ok(b) => return Ok(b),
                Err(e) => {
                    last_err = Some(e.to_string());
                    if !self.refresh_and_backoff(&mut attempt).await {
                        break;
                    }
                }
            }
        }
        Err(AutumnError::ConnectionError(format!(
            "ps_call after {attempt} refreshes: {}",
            last_err.unwrap_or_else(|| "unknown".to_string())
        )))
    }

    /// One routing-retry step shared by all four PS-call loops
    /// (`call_ps_for_key` / `call_ps_for_part` / `get_into` / `get_range_into`):
    /// if the `MAX_PS_REFRESHES` budget is spent return `false` (the caller
    /// breaks out of its loop); otherwise bump `attempt`, TiKV-style back off,
    /// refresh the region cache, and return `true` to retry against the
    /// (possibly re-resolved) routing.
    async fn refresh_and_backoff(&self, attempt: &mut u32) -> bool {
        if *attempt >= MAX_PS_REFRESHES {
            return false;
        }
        *attempt += 1;
        let sleep = refresh_backoff(*attempt);
        if !sleep.is_zero() {
            compio::time::sleep(sleep).await;
        }
        let _ = self.refresh_regions().await;
        true
    }

    /// Resolve part_id to ps_addr, call PS, retry with TiKV-style
    /// backoff on failure.
    ///
    /// F212-fix-2: same retry shape as `call_ps_for_key`. Admin ops
    /// (split/compact/gc/flush) don't carry `region_epoch` on the
    /// wire, but they CAN race the same post-split topology change
    /// window — e.g. `compact <new_part_id>` issued immediately
    /// after split can land while the new partition's part_addr
    /// isn't registered yet.
    async fn call_ps_for_part(
        &self,
        part_id: u64,
        msg_type: u8,
        payload: Bytes,
    ) -> std::result::Result<Bytes, AutumnError> {
        let mut attempt: u32 = 0;
        let mut last_err: Option<String> = None;
        while attempt <= MAX_PS_REFRESHES {
            let ps_addr = self
                .resolve_part_id(part_id)
                .await
                .map_err(|e| AutumnError::RoutingError(e.to_string()))?;
            // Bug #2 fix: same fast-fail-on-attempt-0 as call_ps_for_key.
            let t = self.first_attempt_effective_timeout(attempt);
            match self
                .ps_call_with_timeout(&ps_addr, msg_type, payload.clone(), t)
                .await
            {
                Ok(b) => return Ok(b),
                Err(e) => {
                    // F225: short-circuit DETERMINISTIC failures. Admin ops
                    // (split/compact/gc/flush) are region_epoch-exempt, so a
                    // FailedPrecondition here is never stale-routing — it's a
                    // real precondition (e.g. "partition has overlapping keys",
                    // "needs >= 2 keys") that refreshing routing cannot fix.
                    // Surface it immediately instead of burning MAX_PS_REFRESHES
                    // (~9 s of backoff). Only transient errors (routing miss /
                    // NotFound from a not-yet-registered post-split partition /
                    // connection) fall through to refresh + retry.
                    match e.downcast::<AutumnError>() {
                        Ok(
                            ae @ (AutumnError::PreconditionFailed(_)
                            | AutumnError::InvalidArgument(_)
                            | AutumnError::ValueTooLarge { .. }),
                        ) => {
                            return Err(ae);
                        }
                        Ok(ae) => last_err = Some(ae.to_string()),
                        Err(other) => last_err = Some(other.to_string()),
                    }
                    if !self.refresh_and_backoff(&mut attempt).await {
                        break;
                    }
                }
            }
        }
        Err(AutumnError::ConnectionError(format!(
            "ps_call(part {part_id}) after {attempt} refreshes: {}",
            last_err.unwrap_or_else(|| "unknown".to_string())
        )))
    }

    // ── High-level SDK API ──────────────────────────────────────────────────

    /// Put a key-value pair. Retries once on routing miss.
    ///
    /// F178: every Put is durable. Pre-F178 the API took a `must_sync: bool`
    /// flag; after F178 the field was removed from the wire and every
    /// append goes through the extent-node fsync coalescer (RocksDB-style
    /// group commit). Callers no longer have a "fast but unsafe" mode.
    pub async fn put(&self, key: &[u8], value: &[u8]) -> std::result::Result<(), AutumnError> {
        self.put_opts(key, value, 0, WriteLease::ANON).await
    }

    /// BUG-LEASE-2 Phase 2: lease-fenced put. Stamps `(inode_hint,
    /// lease_epoch)` so the PS fence floor rejects this write with
    /// `AutumnError::Fenced` once a newer writer's epoch has been seen
    /// for the inode. Lease-holding writers (autumn-fuse, the ioring
    /// daemon) MUST use this; anonymous KV callers keep plain `put`.
    pub async fn put_fenced(
        &self,
        key: &[u8],
        value: &[u8],
        lease: WriteLease,
    ) -> std::result::Result<(), AutumnError> {
        self.put_opts(key, value, 0, lease).await
    }

    /// Helper: convert a relative TTL (seconds from now) to an
    /// absolute Unix-epoch `expires_at`. `0` is preserved (no expiry).
    /// Used by the single-key `put` ttl convenience and by callers
    /// preparing `put_many` items.
    ///
    /// `saturating_add`: an absurdly large `ttl_secs` that would overflow
    /// `u64` epoch seconds saturates to `u64::MAX`, which the read-path expiry
    /// check (`expires_at <= now`) treats as "never expires" — the safe reading
    /// of "expire impossibly far in the future". (Without it, release-mode
    /// wraparound could flip a huge TTL into instant-expiry or `0`/never; the
    /// PyO3 `ttl_secs` arg is an unchecked `u64` from Python.)
    pub fn ttl_to_expires_at(ttl_secs: u64) -> u64 {
        if ttl_secs == 0 {
            return 0;
        }
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_add(ttl_secs)
    }

    async fn put_opts(
        &self,
        key: &[u8],
        value: &[u8],
        expires_at: u64,
        lease: WriteLease,
    ) -> std::result::Result<(), AutumnError> {
        // F129: client-side pre-check against the hard cap. Avoids sending
        // a 256 MB+ payload over the wire only to be rejected post-decode.
        // The PS still authoritatively rejects > its configured (default
        // 64 MiB) cap; for anything in (64 MiB, 256 MiB] we let the server
        // decide (its default rejects, but it could be raised via env).
        if value.len() as u64 > CLIENT_PUT_HARD_CAP {
            return Err(AutumnError::ValueTooLarge {
                size: value.len() as u64,
                cap: CLIENT_PUT_HARD_CAP,
            });
        }
        let key = key.to_vec();
        let value = value.to_vec();
        let resp_bytes = self
            .call_ps_for_key(&key, MSG_PUT, |part_id, region_epoch| {
                rkyv_encode(&PutReq {
                    part_id,
                    key: key.clone(),
                    value: value.clone(),
                    expires_at,
                    region_epoch,
                    inode_hint: lease.inode_hint,
                    lease_epoch: lease.lease_epoch,
                })
            })
            .await?;
        let resp: PutResp = rkyv_decode(&resp_bytes).map_err(AutumnError::ServerError)?;
        if resp.code == partition_rpc::CODE_VALUE_TOO_LARGE {
            // PS-reported cap (lower than CLIENT_PUT_HARD_CAP). Surface the
            // value's size so the caller can size the next put_stream
            // chunks appropriately.
            return Err(AutumnError::ValueTooLarge {
                size: value.len() as u64,
                cap: 0, // unknown server-side cap (would need to parse `resp.message`)
            });
        }
        check_ps_code(resp.code, &resp.message)?;
        Ok(())
    }

    /// F216-E zero-copy PUT: write a value with NO intermediate value copy on
    /// the client. The value is sent as its own iovec (via `MSG_PUT_ZC` +
    /// `RpcClient::call_vectored`) straight from `value`'s backing memory — on
    /// UCX that is a zero-copy send via rcache when `value`'s memory is
    /// `ucp_mem_map`-registered (the kvcache caller holds a `RegisteredMem` over
    /// the sglang source pool and passes a `Bytes` aliasing it → 0 copies). The
    /// regular `put` path copies the value 3× (to_vec → clone → rkyv_encode);
    /// this copies 0. Same routing + epoch-stale refresh + RPC-retry shape as
    /// `call_ps_for_key`. Inline-cap rules are identical to `put` (PS rejects
    /// over the inline cap with `CODE_VALUE_TOO_LARGE`).
    pub async fn put_zc(&self, key: &[u8], value: Bytes) -> std::result::Result<(), AutumnError> {
        self.put_zc_opts(key, value, 0, WriteLease::ANON).await
    }

    /// BUG-LEASE-2 Phase 2: lease-fenced zero-copy put (see `put_fenced`).
    pub async fn put_zc_fenced(
        &self,
        key: &[u8],
        value: Bytes,
        lease: WriteLease,
    ) -> std::result::Result<(), AutumnError> {
        self.put_zc_opts(key, value, 0, lease).await
    }

    pub(crate) async fn put_zc_opts(
        &self,
        key: &[u8],
        value: Bytes,
        expires_at: u64,
        lease: WriteLease,
    ) -> std::result::Result<(), AutumnError> {
        if value.len() as u64 > CLIENT_PUT_HARD_CAP {
            return Err(AutumnError::ValueTooLarge {
                size: value.len() as u64,
                cap: CLIENT_PUT_HARD_CAP,
            });
        }
        let key = key.to_vec();
        let mut attempt: u32 = 0;
        let mut last_err: Option<String> = None;
        while attempt <= MAX_PS_REFRESHES {
            let (part_id, ps_addr) = self
                .resolve_key(&key)
                .await
                .map_err(|e| AutumnError::RoutingError(e.to_string()))?;
            let region_epoch = self.lookup_epoch_for_part(part_id);
            let meta = partition_rpc::encode_put_zc_meta(
                part_id,
                region_epoch,
                expires_at,
                &key,
                lease.inode_hint,
                lease.lease_epoch,
            );
            match self.get_ps_client(&ps_addr).await {
                Ok(client) => {
                    // [meta, value] — value is a zero-copy iovec; no copy here.
                    match client
                        .call_vectored(partition_rpc::MSG_PUT_ZC, vec![meta, value.clone()])
                        .await
                    {
                        Ok(resp_bytes) => {
                            let resp: PutResp =
                                rkyv_decode(&resp_bytes).map_err(AutumnError::ServerError)?;
                            match resp.code {
                                partition_rpc::CODE_OK => return Ok(()),
                                partition_rpc::CODE_VALUE_TOO_LARGE => {
                                    return Err(AutumnError::ValueTooLarge {
                                        size: value.len() as u64,
                                        cap: 0,
                                    });
                                }
                                // Epoch stale / frozen-for-merge → refresh + retry.
                                partition_rpc::CODE_PRECONDITION
                                | partition_rpc::CODE_REGION_EPOCH_STALE
                                | partition_rpc::CODE_UNAVAILABLE => {
                                    last_err = Some(resp.message);
                                }
                                other => return Err(code_to_error(other, resp.message)),
                            }
                        }
                        Err(e) => {
                            self.ps_conns.borrow_mut().remove(&ps_addr);
                            last_err = Some(e.to_string());
                        }
                    }
                }
                Err(e) => last_err = Some(e.to_string()),
            }
            if !self.refresh_and_backoff(&mut attempt).await {
                break;
            }
        }
        Err(AutumnError::ConnectionError(format!(
            "put_zc after {attempt} refreshes: {}",
            last_err.unwrap_or_else(|| "unknown".to_string())
        )))
    }

    /// Get a value by key. Returns None if not found.
    pub async fn get(&self, key: &[u8]) -> std::result::Result<Option<Vec<u8>>, AutumnError> {
        self.get_range(key, 0, 0).await
    }

    /// Get a sub-range of a value: bytes `[offset, offset+length)`.
    /// `length == 0` means "from offset to the end of the value" (matches the
    /// underlying `GetReq` semantics). Returns None if the key is not found.
    ///
    /// Routes through `call_ps_for_key` so the cached PS connection is dropped
    /// on RPC error and routing is refreshed on the second attempt — same
    /// resilience as `get`/`put`/`head` after a cluster restart.
    pub async fn get_range(
        &self,
        key: &[u8],
        offset: u32,
        length: u32,
    ) -> std::result::Result<Option<Vec<u8>>, AutumnError> {
        let key = key.to_vec();
        let resp_bytes = self
            .call_ps_for_key(&key, MSG_GET, |part_id, region_epoch| {
                rkyv_encode(&GetReq {
                    part_id,
                    key: key.clone(),
                    offset,
                    length,
                    region_epoch,
                })
            })
            .await?;
        let resp: GetResp = rkyv_decode(&resp_bytes).map_err(AutumnError::ServerError)?;
        if resp.code == partition_rpc::CODE_NOT_FOUND {
            return Ok(None);
        }
        check_ps_code(resp.code, &resp.message)?;
        Ok(Some(resp.value))
    }

    /// F259: GET with PS-bypass for large VP values. Issues
    /// `MSG_GET_REDIRECT`; when the PS answers with a descriptor (>= 64 KiB
    /// ValuePointer value), the value bytes are read STRAIGHT from an
    /// extent-node replica — the PS never touches the data. Replica choice
    /// rotates by (extent, offset); failover walks the rest. ANY direct-read
    /// failure (eversion bumped, extent GC'd in the window, replica down)
    /// falls back to the plain proxy `get` — the redirect is an
    /// optimization, never a correctness dependency.
    pub async fn get_direct(
        &self,
        key: &[u8],
    ) -> std::result::Result<Option<bytes::Bytes>, AutumnError> {
        let key_v = key.to_vec();
        let resp_bytes = self
            .call_ps_for_key(&key_v, MSG_GET_REDIRECT, |part_id, region_epoch| {
                rkyv_encode(&GetReq {
                    part_id,
                    key: key_v.clone(),
                    offset: 0,
                    length: 0,
                    region_epoch,
                })
            })
            .await?;
        let resp: GetRedirectResp =
            rkyv_decode(&resp_bytes).map_err(AutumnError::ServerError)?;
        if resp.code == partition_rpc::CODE_NOT_FOUND {
            return Ok(None);
        }
        check_ps_code(resp.code, &resp.message)?;
        if resp.extent_id == 0 {
            return Ok(Some(bytes::Bytes::from(resp.value)));
        }
        let n = resp.replica_addrs.len();
        if n > 0 {
            let start =
                (resp.extent_id ^ (resp.value_offset as u64) << 32) as usize % n;
            for i in 0..n {
                let addr = &resp.replica_addrs[(start + i) % n];
                match autumn_stream::read_extent_value_direct(
                    &self.en_pool,
                    addr,
                    resp.extent_id,
                    resp.eversion,
                    resp.value_offset,
                    resp.value_len,
                )
                .await
                {
                    Ok(v) => return Ok(Some(v)),
                    Err(e) => {
                        tracing::debug!(
                            extent_id = resp.extent_id,
                            addr = %addr,
                            error = %e,
                            "F259 direct read failed; trying next replica"
                        );
                    }
                }
            }
        }
        // All replicas failed → proxy fallback re-resolves through the PS
        // (fresh VP after GC rewrite / fresh eversion after EC conversion).
        Ok(self.get(key).await?.map(bytes::Bytes::from))
    }

    /// Zero-copy GET: read a key's value straight into `dest` (no
    /// intermediate Vec). Returns `Some(value_len)` (value written to
    /// `dest[..value_len]`) or `None` if not found. `dest` must be at least the
    /// value size.
    ///
    /// On UCX, the first call into a fresh `dest` address pays a one-time
    /// rcache miss (`~100 µs ibv_reg_mr`); subsequent calls into the SAME
    /// address hit the rcache for free — so any pool / long-lived buffer is
    /// effectively zero-copy from the second call onward. Power users who
    /// want to skip the first-call cost on a hot path can call
    /// `autumn_transport::register_memory` directly to pre-populate the
    /// rcache; the SDK finds the registration transparently. On TCP the
    /// kernel copy off the wire is the lower bound regardless.
    ///
    /// Uses `MSG_GET_ZC` + `RpcClient::call_into_dest`; same routing +
    /// epoch-stale refresh + RPC retry shape as `call_ps_for_key`.
    pub async fn get_into(
        &self,
        key: &[u8],
        dest: &mut [u8],
    ) -> std::result::Result<Option<usize>, AutumnError> {
        self.get_range_into(key, 0, 0, dest).await
    }

    /// Sub-range variant of [`get_into`] — reads bytes `[offset, offset+length)`
    /// (`length == 0` = whole value) straight into `dest` via `MSG_GET_ZC`.
    /// `get_into(key, dest)` is exactly `get_range_into(key, 0, 0, dest)`.
    /// Same routing + epoch-stale refresh + RPC retry shape as `call_ps_for_key`;
    /// same cancel-safety contract (`dest` must outlive the call; no per-call
    /// timeout). Used by `get_many_into` and by sub-range callers (fuse /
    /// ioring) that route per key.
    pub async fn get_range_into(
        &self,
        key: &[u8],
        offset: u32,
        length: u32,
        dest: &mut [u8],
    ) -> std::result::Result<Option<usize>, AutumnError> {
        let key = key.to_vec();
        let mut attempt: u32 = 0;
        let mut last_err: Option<String> = None;
        while attempt <= MAX_PS_REFRESHES {
            let (part_id, ps_addr) = self
                .resolve_key(&key)
                .await
                .map_err(|e| AutumnError::RoutingError(e.to_string()))?;
            let region_epoch = self.lookup_epoch_for_part(part_id);
            let payload = rkyv_encode(&GetReq {
                part_id,
                key: key.clone(),
                offset,
                length,
                region_epoch,
            });
            match self.get_ps_client(&ps_addr).await {
                Ok(client) => {
                    match client
                        .call_into_dest(
                            partition_rpc::MSG_GET_ZC,
                            payload,
                            dest.as_mut_ptr(),
                            dest.len(),
                            None,
                        )
                        .await
                    {
                        Ok(meta) => match meta.code {
                            partition_rpc::CODE_OK => return Ok(Some(meta.value_len)),
                            partition_rpc::CODE_NOT_FOUND => return Ok(None),
                            // Epoch stale → fall through to refresh + retry.
                            partition_rpc::CODE_PRECONDITION
                            | partition_rpc::CODE_REGION_EPOCH_STALE => {
                                last_err = Some("region epoch stale".to_string());
                            }
                            other => return Err(code_to_error(other, String::new())),
                        },
                        Err(e) => {
                            // Drop the conn so the next attempt reconnects.
                            self.ps_conns.borrow_mut().remove(&ps_addr);
                            last_err = Some(e.to_string());
                        }
                    }
                }
                Err(e) => last_err = Some(e.to_string()),
            }
            if !self.refresh_and_backoff(&mut attempt).await {
                break;
            }
        }
        Err(AutumnError::ConnectionError(format!(
            "get_range_into after {attempt} refreshes: {}",
            last_err.unwrap_or_else(|| "unknown".to_string())
        )))
    }

    /// F235/F244: batched point reads — the ONE client-side fan-out primitive
    /// (no server `MSG_BATCH_GET`). All batch-read callers route through this:
    /// the python `BatchClient` (kvcache), fuse `read::execute`, and the io_uring
    /// daemon. Each item is read concurrently (sliding window of `concurrency` —
    /// callers pass `BATCH_GET_DEFAULT_CONCURRENCY` or their own tuned cap, e.g.
    /// python's `per_worker_cap`) over the per-partition multiplexed PS
    /// connections, amortising per-call await latency + letting the writer_task
    /// batch syscalls. Per item the ZC decision is `zc_worthwhile(read_len)`
    /// (read_len = `length` for a sub-range, else `dest.len()`): >= 64 KiB →
    /// `get_range_into` (`MSG_GET_ZC`, recv straight into `dest`; RDMA on UCX with a
    /// registered `reg`); else `get_range` (`MSG_GET`) + one copy into `dest`.
    /// Result `i` matches `items[i]`: `Ok(Some(n))` = value len
    /// (`dest[..n.min(dest.len())]` filled; `n > dest.len()` ⇒ truncated to fit),
    /// `Ok(None)` = not found, `Err` = that item's RPC failed (others still ran).
    /// Each `dest` MUST outlive the call (same cancel-safety contract as `get_into`).
    pub async fn get_many_into(
        &self,
        items: &mut [GetManyItem<'_>],
    ) -> Vec<std::result::Result<Option<usize>, AutumnError>> {
        // Homogeneous small whole-value case → MSG_BATCH_GET (server
        // batches per partition; measured 4× lower read p99 on
        // loopback). Conditions: every item is a whole-value read
        // (offset == 0 && length == 0) whose dest is below the ZC
        // threshold. Mixed / range / large-ZC inputs fall through to
        // the per-op fan_out which keeps the ZC RDMA path.
        let homogeneous_small = !items.is_empty()
            && items.iter().all(|it| {
                it.offset == 0 && it.length == 0 && !zc_worthwhile(it.dest.len())
            });
        if homogeneous_small {
            let keys: Vec<&[u8]> = items.iter().map(|it| it.key).collect();
            let resp = self.get_many(&keys).await;
            // Copy each response into its dest in input order; the
            // dest mutable borrow is taken per-iteration so each
            // dest gets its own `&mut` slice.
            let mut out: Vec<std::result::Result<Option<usize>, AutumnError>> =
                Vec::with_capacity(items.len());
            for (it, r) in items.iter_mut().zip(resp.into_iter()) {
                match r {
                    Ok(Some(v)) => {
                        let n = v.len().min(it.dest.len());
                        it.dest[..n].copy_from_slice(&v[..n]);
                        out.push(Ok(Some(v.len())));
                    }
                    Ok(None) => out.push(Ok(None)),
                    Err(e) => out.push(Err(e)),
                }
            }
            return out;
        }
        let concurrency = BATCH_GET_DEFAULT_CONCURRENCY;
        let futs = items.iter_mut().map(|it| {
            let key: &[u8] = it.key;
            let offset = it.offset;
            let length = it.length;
            let dest: &mut [u8] = &mut *it.dest;
            async move {
                let read_len = if length > 0 { length as usize } else { dest.len() };
                if zc_worthwhile(read_len) {
                    self.get_range_into(key, offset, length, dest).await
                } else {
                    match self.get_range(key, offset, length).await {
                        Ok(Some(v)) => {
                            let n = v.len().min(dest.len());
                            dest[..n].copy_from_slice(&v[..n]);
                            Ok(Some(v.len()))
                        }
                        Ok(None) => Ok(None),
                        Err(e) => Err(e),
                    }
                }
            }
        });
        fan_out_collect(futs, concurrency).await
    }

    /// F236/F245: batched zero-copy writes — the write mirror of `get_many_into`,
    /// built on the shared `fan_out_collect`. Pure client-side fan-out (no server
    /// `MSG_BATCH_PUT`): each `(key, value)` is written concurrently (sliding window
    /// of `concurrency`) over the per-partition multiplexed PS connections. Per item
    /// the ZC decision
    /// is `zc_worthwhile(value.len())`: >= 64 KiB → `put_zc` (`MSG_PUT_ZC`, value
    /// sent as its own iovec from the `Bytes` backing memory; RDMA on UCX when that
    /// memory is registered); else `put` (`MSG_PUT`). Values are `Bytes` so the ZC
    /// path needs no copy (`clone` = Arc bump). Result `i` matches `items[i]`:
    /// `Ok(())` = stored, `Err` = that item's RPC failed (others still ran).
    #[allow(clippy::type_complexity)]
    pub async fn put_many(
        &self,
        items: &[(&[u8], bytes::Bytes, u64)],
    ) -> Vec<std::result::Result<(), AutumnError>> {
        // The ONE public batched-write API. Each tuple is
        // `(key, value, expires_at)`; `expires_at = 0` means no TTL.
        // Use `ClusterClient::ttl_to_expires_at(ttl_secs)` when you
        // have a relative TTL.
        //
        // Internal routing (encapsulated in `batch_put`):
        //   * value < 64 KiB → packed into MSG_BATCH_PUT (one frame per
        //     owning partition, server decodes once and injects all ops
        //     into partition_loop.pending atomically). Measured 6.9×
        //     over the pre-merge client-side fan-out on cross-host TCP.
        //   * value >= 64 KiB → per-op MSG_PUT_ZC (zero-copy value
        //     transfer; on UCX with registered Bytes → RDMA from caller
        //     memory). Batching ZC values would bloat the frame.
        //
        // No `concurrency` parameter: batch_put issues one RPC per
        // owning partition and the underlying multiplexed PS connection
        // does the rest. Callers needing finer pacing should call
        // `batch_put` directly (pub(crate)).
        self.batch_put(items, WriteLease::ANON).await
    }

    /// BUG-LEASE-2 Phase 2: lease-fenced `put_many` — every item is
    /// stamped with the SAME `(inode_hint, lease_epoch)` (the batch
    /// belongs to one inode's flush, e.g. autumn-fuse `flush_inode`).
    /// A fenced item returns `AutumnError::Fenced` in its result slot.
    pub async fn put_many_fenced(
        &self,
        items: &[(&[u8], bytes::Bytes, u64)],
        lease: WriteLease,
    ) -> Vec<std::result::Result<(), AutumnError>> {
        self.batch_put(items, lease).await
    }

    /// **The batched-read API that does not require caller-owned
    /// dest buffers.** Mirror of `put_many`. SDK allocates a
    /// `Vec<u8>` per returned value; result `i` matches `keys[i]`.
    ///
    /// Wire: one `MSG_BATCH_GET` per owning partition. The PS handler
    /// runs INLINE on the ps-conn task (reads never go through
    /// `partition_loop`), so the win is amortising one wire frame +
    /// one rkyv decode over N keys.
    ///
    /// When to use `get_many` vs `get_many_into`:
    /// - **`get_many`** — when you don't know the value sizes (or
    ///   don't care to alloc dests), or values are small (< 64 KiB)
    ///   so ZC wouldn't engage anyway. SDK allocates each `Vec<u8>`.
    /// - **`get_many_into`** — when values are ≥ 64 KiB AND you have
    ///   caller-owned dest buffers (especially `RegisteredMem` for
    ///   UCX RDMA into pinned memory like sglang pages / torch
    ///   tensors). True end-to-end zero-copy.
    /// Below 64 KiB both APIs do one rkyv decode-copy regardless;
    /// `get_many` saves you the dest-sizing footwork.
    ///
    /// Per-key result: `Ok(Some(value))` = present, `Ok(None)` = not
    /// found, `Err` = per-key error.
    pub async fn get_many(
        &self,
        keys: &[&[u8]],
    ) -> Vec<std::result::Result<Option<Vec<u8>>, AutumnError>> {
        let mut results: Vec<std::result::Result<Option<Vec<u8>>, AutumnError>> =
            (0..keys.len()).map(|_| Ok(None)).collect();
        let mut groups: std::collections::HashMap<u64, Vec<(usize, &[u8])>> =
            std::collections::HashMap::new();
        for (i, &k) in keys.iter().enumerate() {
            let part_id = match self.resolve_key(k).await {
                Ok((pid, _addr)) => pid,
                Err(e) => {
                    results[i] = Err(AutumnError::RoutingError(e.to_string()));
                    continue;
                }
            };
            groups.entry(part_id).or_default().push((i, k));
        }
        for (part_id, group) in groups {
            let region_epoch = self.lookup_epoch_for_part(part_id);
            let keys_payload: Vec<Vec<u8>> = group.iter().map(|(_, k)| k.to_vec()).collect();
            let payload = rkyv_encode(&partition_rpc::BatchGetReq {
                part_id,
                region_epoch,
                keys: keys_payload,
            });
            let resp_bytes = match self
                .call_ps_for_part(part_id, partition_rpc::MSG_BATCH_GET, payload.into())
                .await
            {
                Ok(b) => b,
                Err(AutumnError::PreconditionFailed(_)) => {
                    // Same stale-epoch handling as batch_put — see comment
                    // there. Fall back to per-key `get` (call_ps_for_key
                    // refreshes natively + handles post-split re-routing).
                    let _ = self.refresh_regions().await;
                    for (idx, k) in group.iter() {
                        results[*idx] = self.get(k).await;
                    }
                    continue;
                }
                Err(e) => {
                    let s = e.to_string();
                    for (idx, _) in group.iter() {
                        results[*idx] = Err(AutumnError::ConnectionError(s.clone()));
                    }
                    continue;
                }
            };
            let resp: partition_rpc::BatchGetResp = match rkyv_decode(&resp_bytes) {
                Ok(r) => r,
                Err(e) => {
                    for (idx, _) in group.iter() {
                        results[*idx] = Err(AutumnError::ServerError(e.clone()));
                    }
                    continue;
                }
            };
            if resp.code != partition_rpc::CODE_OK {
                let code = resp.code;
                let msg = resp.message;
                for (idx, _) in group.iter() {
                    results[*idx] = Err(code_to_error(code, msg.clone()));
                }
                continue;
            }
            if resp.items.len() != group.len() {
                let mismatch = format!(
                    "get_many: items len {} != group len {}",
                    resp.items.len(),
                    group.len()
                );
                for (idx, _) in group.iter() {
                    results[*idx] = Err(AutumnError::ServerError(mismatch.clone()));
                }
                continue;
            }
            for ((idx, _), item) in group.iter().zip(resp.items.into_iter()) {
                results[*idx] = match item.status {
                    0 => Ok(Some(item.value)),
                    1 => Ok(None),
                    s => Err(AutumnError::ServerError(format!(
                        "get_many op status={s}"
                    ))),
                };
            }
        }
        results
    }

    /// **Server-side batched PUT** (`MSG_BATCH_PUT`): N ops on the SAME
    /// partition packed into ONE frame. The PS decodes once, injects
    /// all N ops into `partition_loop::pending` as ONE mpsc message,
    /// fires a wide batch immediately + can run multiple concurrent
    /// batches (exercises `INFLIGHT_CAP`). This is the
    /// fix for the cross-host 4K-write small-batch cliff documented in
    /// `docs/perf_4k_loopback_vs_xhost_scaling.md`.
    ///
    /// Constraints (vs the client-side `put_many` fan-out):
    /// * **Non-ZC only** — values >= 64 KiB skip this path (the wire
    ///   bandwidth dominates per-op overhead; bundling them gives no
    ///   win and bloats the frame). The caller may still pass large
    ///   values; they're routed to per-op `put_zc` individually here.
    /// * **One frame per partition** — the client groups `items` by
    ///   owning partition first, then issues one BATCH RPC per
    ///   partition. A 32-key batch spread across 16 partitions becomes
    ///   16 RPCs of ~2 ops each — still one server-side mpsc message
    ///   per group, but the wire fan-out is per-partition not per-key.
    /// * **Per-op result preserved**: returns `Vec<Result<()>>` with
    ///   one entry per input item in input order.
    pub(crate) async fn batch_put(
        &self,
        items: &[(&[u8], bytes::Bytes, u64)],
        lease: WriteLease,
    ) -> Vec<std::result::Result<(), AutumnError>> {
        let mut results: Vec<std::result::Result<(), AutumnError>> =
            (0..items.len()).map(|_| Ok(())).collect();
        // (idx, key, value, expires_at) — keep all four pieces threaded so the
        // ZC fallback and per-partition packing can read them without
        // re-indexing into `items` (which is borrowed immutably).
        let mut groups: std::collections::HashMap<u64, Vec<(usize, &[u8], bytes::Bytes, u64)>> =
            std::collections::HashMap::new();
        let mut zc_only: Vec<(usize, &[u8], bytes::Bytes, u64)> = Vec::new();
        for (i, (k, v, ttl)) in items.iter().enumerate() {
            if zc_worthwhile(v.len()) {
                zc_only.push((i, *k, v.clone(), *ttl));
                continue;
            }
            let part_id = match self.resolve_key(k).await {
                Ok((pid, _addr)) => pid,
                Err(e) => {
                    results[i] = Err(AutumnError::RoutingError(e.to_string()));
                    continue;
                }
            };
            groups
                .entry(part_id)
                .or_default()
                .push((i, *k, v.clone(), *ttl));
        }
        for (part_id, group) in groups {
            let region_epoch = self.lookup_epoch_for_part(part_id);
            let ops: Vec<partition_rpc::BatchPutOp> = group
                .iter()
                .map(|(_, k, v, ttl)| partition_rpc::BatchPutOp {
                    inode_hint: lease.inode_hint,
                    lease_epoch: lease.lease_epoch,
                    key: k.to_vec(),
                    value: v.to_vec(),
                    expires_at: *ttl,
                })
                .collect();
            let payload = rkyv_encode(&partition_rpc::BatchPutReq {
                part_id,
                region_epoch,
                must_sync: true,
                ops,
            });
            let resp_bytes = match self
                .call_ps_for_part(part_id, partition_rpc::MSG_BATCH_PUT, payload.into())
                .await
            {
                Ok(b) => b,
                Err(AutumnError::PreconditionFailed(_)) => {
                    // Stale region_epoch: server returns FailedPrecondition
                    // (`MSG_BATCH_PUT` enforces epoch on the data path, see
                    // partition-server lib.rs:5830). `call_ps_for_part`
                    // short-circuits PreconditionFailed under F225 admin-op
                    // semantics — correct for split/compact/gc/flush, WRONG
                    // for the data path where the SDK's job is to refresh
                    // routing and retry. Fall back to per-op `put_opts` for
                    // this group; the single-key path routes via
                    // `call_ps_for_key` which refreshes natively and handles
                    // post-split re-routing (keys may no longer all belong to
                    // `part_id` after the topology change).
                    let _ = self.refresh_regions().await;
                    for (idx, k, v, ttl) in group.iter() {
                        results[*idx] = self.put_opts(k, v.as_ref(), *ttl, lease).await;
                    }
                    continue;
                }
                Err(e) => {
                    let s = e.to_string();
                    for (idx, _, _, _) in group.iter() {
                        results[*idx] = Err(AutumnError::ConnectionError(s.clone()));
                    }
                    continue;
                }
            };
            let resp: partition_rpc::BatchPutResp = match rkyv_decode(&resp_bytes) {
                Ok(r) => r,
                Err(e) => {
                    for (idx, _, _, _) in group.iter() {
                        results[*idx] = Err(AutumnError::ServerError(e.clone()));
                    }
                    continue;
                }
            };
            if resp.code != partition_rpc::CODE_OK {
                let code = resp.code;
                let msg = resp.message;
                for (idx, _, _, _) in group.iter() {
                    results[*idx] = Err(code_to_error(code, msg.clone()));
                }
                continue;
            }
            if resp.statuses.len() != group.len() {
                let mismatch = format!(
                    "batch_put: statuses len {} != group len {}",
                    resp.statuses.len(),
                    group.len()
                );
                for (idx, _, _, _) in group.iter() {
                    results[*idx] = Err(AutumnError::ServerError(mismatch.clone()));
                }
                continue;
            }
            for ((idx, _, _, _), status) in group.iter().zip(resp.statuses.iter()) {
                if *status == partition_rpc::CODE_FENCED {
                    results[*idx] = Err(AutumnError::Fenced(format!(
                        "batch_put op fenced (ino={}, epoch={})",
                        lease.inode_hint, lease.lease_epoch
                    )));
                } else if *status != 0 {
                    results[*idx] = Err(AutumnError::ServerError(format!(
                        "batch_put op status={status}"
                    )));
                }
            }
        }
        // ZC-only entries: per-op put_zc_opts so the TTL from put_many's
        // 3-tuple is preserved end-to-end (PutZcMeta carries expires_at
        // on the wire; the single-key `put_zc` convenience hardcodes 0).
        for (idx, k, v, ttl) in zc_only {
            results[idx] = self.put_zc_opts(k, v, ttl, lease).await;
        }
        results
    }

    /// F237: batched deletes — pure client-side fan-out (no server `MSG_BATCH_*`),
    /// `buffered` over the per-partition multiplexed connections. No ZC (delete is
    /// tiny). Result `i` matches `keys[i]` (`Ok(())` even if the key didn't exist,
    /// same as `delete`; `Err` = that item's RPC failed, others still ran).
    pub async fn delete_many(&self, keys: &[&[u8]]) -> Vec<std::result::Result<(), AutumnError>> {
        // delete is a mutation → reuse the write-side concurrency cap.
        let futs = keys
            .iter()
            .map(|&key| async move { self.delete(key).await });
        fan_out_collect(futs, BATCH_PUT_DEFAULT_CONCURRENCY).await
    }

    /// F237: batched metadata lookups — pure client-side fan-out, `buffered` over
    /// the per-partition multiplexed connections. No ZC (head carries no value).
    /// Result `i` matches `keys[i]`: `Ok(KeyMeta{ found, value_length })`
    /// (`found=false` for a missing key, NOT an `Err`); `Err` = that item's RPC
    /// failed (others still ran).
    pub async fn head_many(
        &self,
        keys: &[&[u8]],
    ) -> Vec<std::result::Result<KeyMeta, AutumnError>> {
        // head is a read → reuse the read-side concurrency cap.
        let futs = keys.iter().map(|&key| async move { self.head(key).await });
        fan_out_collect(futs, BATCH_GET_DEFAULT_CONCURRENCY).await
    }

    /// Delete a key. Returns Ok(()) even if key didn't exist.
    pub async fn delete(&self, key: &[u8]) -> std::result::Result<(), AutumnError> {
        self.delete_opts(key, WriteLease::ANON).await
    }

    /// BUG-LEASE-2 Phase 2: lease-fenced delete — a revoked writer's late
    /// truncate/unlink must not remove the new writer's data (same fence
    /// floor as puts; `AutumnError::Fenced` on rejection).
    pub async fn delete_fenced(
        &self,
        key: &[u8],
        lease: WriteLease,
    ) -> std::result::Result<(), AutumnError> {
        self.delete_opts(key, lease).await
    }

    async fn delete_opts(
        &self,
        key: &[u8],
        lease: WriteLease,
    ) -> std::result::Result<(), AutumnError> {
        let key = key.to_vec();
        let resp_bytes = self
            .call_ps_for_key(&key, MSG_DELETE, |part_id, region_epoch| {
                rkyv_encode(&DeleteReq {
                    part_id,
                    key: key.clone(),
                    region_epoch,
                    inode_hint: lease.inode_hint,
                    lease_epoch: lease.lease_epoch,
                })
            })
            .await?;
        let resp: DeleteResp = rkyv_decode(&resp_bytes).map_err(AutumnError::ServerError)?;
        if resp.code != partition_rpc::CODE_OK && resp.code != partition_rpc::CODE_NOT_FOUND {
            return Err(code_to_error(resp.code, resp.message));
        }
        Ok(())
    }

    /// Get key metadata (existence and value length).
    pub async fn head(&self, key: &[u8]) -> std::result::Result<KeyMeta, AutumnError> {
        let key = key.to_vec();
        let resp_bytes = self
            .call_ps_for_key(&key, MSG_HEAD, |part_id, region_epoch| {
                rkyv_encode(&HeadReq {
                    part_id,
                    key: key.clone(),
                    region_epoch,
                })
            })
            .await?;
        let resp: HeadResp = rkyv_decode(&resp_bytes).map_err(AutumnError::ServerError)?;
        Ok(KeyMeta {
            found: resp.found,
            value_length: resp.value_length,
        })
    }

    /// Range scan with prefix filter. Scans across partitions like Go's Range().
    ///
    /// CockroachDB-style resume cursor. The SDK drives a per-partition
    /// loop using `RangeResp.cur_end_key` from each successful response
    /// as the next cursor — so a split that happened DURING the scan
    /// auto-resolves when the cursor falls into the new sibling's range
    /// on the next `resolve_key`. Already-accumulated results from
    /// successful earlier partitions are KEPT across refresh+retry; only
    /// the partition that returned epoch-stale gets re-resolved and
    /// re-scanned from `cursor` (= the failing partition's expected
    /// start). At most `MAX_RANGE_REFRESHES` refresh cycles per call to
    /// bound work under pathological topology churn.
    pub async fn range(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: u32,
    ) -> std::result::Result<RangeResult, AutumnError> {
        // F212-fix-2 — uses the shared `refresh_backoff` + `MAX_PS_REFRESHES`
        // helpers (module-level docs explain the TiKV-style budget). Same
        // retry shape as point queries (`call_ps_for_key`) so async-split
        // / async-merge windows are absorbed uniformly.
        // Defensive iteration cap. Per-call work is also bounded by
        // `limit`; this only fires under truly pathological topology
        // churn (every partition splitting between requests).
        const MAX_RANGE_ITERATIONS: u32 = 10_000;

        if self.regions.borrow().is_empty() {
            self.refresh_regions()
                .await
                .map_err(|e| AutumnError::RoutingError(e.to_string()))?;
        }

        let mut cursor: Vec<u8> = if start.is_empty() {
            prefix.to_vec()
        } else {
            start.to_vec()
        };
        let mut all_entries: Vec<RangeEntry> = Vec::new();
        let mut remaining = limit;
        let mut has_more = false;
        let mut refreshes_used: u32 = 0;
        let mut iterations: u32 = 0;

        loop {
            if remaining == 0 {
                has_more = true;
                break;
            }
            iterations += 1;
            if iterations > MAX_RANGE_ITERATIONS {
                return Err(AutumnError::ConnectionError(
                    "range scan exceeded MAX_RANGE_ITERATIONS — topology churning".to_string(),
                ));
            }

            // Resolve cursor → partition under a tight borrow scope.
            let (part_id, ps_addr, region_start_key, region_epoch) = {
                let regions = self.regions.borrow();
                let part_addrs = self.part_addrs.borrow();
                let ps_details = self.ps_details.borrow();
                let idx = regions.partition_point(|(_, region)| match region.rg.as_ref() {
                    Some(rg) if !rg.end_key.is_empty() => {
                        rg.end_key.as_slice() <= cursor.as_slice()
                    }
                    _ => false,
                });
                if idx >= regions.len() {
                    // Past the last partition's end_key → scan done.
                    break;
                }
                let (_, region) = &regions[idx];
                let ps_addr = match part_addrs.get(&region.part_id) {
                    Some(a) => a.clone(),
                    None => match ps_details.get(&region.ps_id) {
                        Some(d) => d.address.clone(),
                        None => {
                            return Err(AutumnError::RoutingError(format!(
                                "no address for partition {} (ps_id {})",
                                region.part_id, region.ps_id
                            )));
                        }
                    },
                };
                let region_start_key = region
                    .rg
                    .as_ref()
                    .map(|r| r.start_key.clone())
                    .unwrap_or_default();
                (
                    region.part_id,
                    ps_addr,
                    region_start_key,
                    region.region_epoch,
                )
            };

            // Prefix-exit: only after we've made forward progress.
            // The first iteration might legitimately land on a
            // partition whose `start_key` is lexically past `prefix`
            // when the user passed an explicit `start` outside the
            // prefix range.
            if iterations > 1
                && !prefix.is_empty()
                && !region_start_key.is_empty()
                && !region_start_key.starts_with(prefix)
            {
                break;
            }

            let resp_bytes = match self
                .ps_call(
                    &ps_addr,
                    MSG_RANGE,
                    rkyv_encode(&RangeReq {
                        part_id,
                        prefix: prefix.to_vec(),
                        start: cursor.clone(),
                        limit: remaining,
                        region_epoch,
                    }),
                )
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    // Epoch stale, transport error (incl. CODE_NOT_FOUND
                    // from a mis-routed fallback on a freshly-split right
                    // child whose part_addr isn't registered yet), or
                    // generic connection failure. Keep results accumulated
                    // so far; back off → refresh → retry with same cursor.
                    if refreshes_used >= MAX_PS_REFRESHES {
                        return Err(AutumnError::ConnectionError(format!(
                            "range on partition {part_id} after {refreshes_used} refreshes: {e}"
                        )));
                    }
                    refreshes_used += 1;
                    let sleep = refresh_backoff(refreshes_used);
                    if !sleep.is_zero() {
                        compio::time::sleep(sleep).await;
                    }
                    let _ = self.refresh_regions().await;
                    continue;
                }
            };
            let resp: RangeResp = rkyv_decode(&resp_bytes).map_err(AutumnError::ServerError)?;
            check_ps_code(resp.code, &resp.message)?;

            let count = resp.entries.len() as u32;
            all_entries.extend(resp.entries);
            remaining = remaining.saturating_sub(count);
            if resp.has_more {
                has_more = true;
            }

            if resp.cur_end_key.is_empty() {
                // Last partition in the keyspace (unbounded right).
                break;
            }
            // Forward-progress check. A non-advancing cur_end_key almost
            // always means our routing cache is stale: we just hit a
            // partition whose end_key is at-or-before the cursor we
            // sent, so partition_point will keep landing on the same
            // partition next iteration and we'd loop forever. Refresh
            // the cache and retry from the same cursor — typically the
            // post-split sibling now appears in the cache and the next
            // iteration's partition_point routes correctly. Bounded by
            // MAX_PS_REFRESHES so a genuinely bogus PS still
            // surfaces an error.
            if resp.cur_end_key.as_slice() <= cursor.as_slice() {
                if refreshes_used >= MAX_PS_REFRESHES {
                    return Err(AutumnError::ServerError(format!(
                        "range on partition {part_id}: cur_end_key did not advance cursor after {refreshes_used} refreshes"
                    )));
                }
                refreshes_used += 1;
                let sleep = refresh_backoff(refreshes_used);
                if !sleep.is_zero() {
                    compio::time::sleep(sleep).await;
                }
                let _ = self.refresh_regions().await;
                continue;
            }
            cursor = resp.cur_end_key;
        }

        // Dedup by key — after split, overlapping SSTables may return
        // the same key from multiple partitions before compaction cleans up.
        // Keep the first occurrence (from the authoritative partition).
        {
            let mut seen = std::collections::HashSet::new();
            all_entries.retain(|e| seen.insert(e.key.clone()));
        }

        Ok(RangeResult {
            entries: all_entries,
            has_more,
        })
    }


    // ── F129 multipart upload ──────────────────────────────────────────────

    /// Open a multipart upload session for `key`. Returns a handle that
    /// F186 — Begin a striped (Ceph-striperados-style) put. Returns a
    /// handle that takes value bytes via `send` and finalises via
    /// `commit`. Internals (no new server RPCs):
    ///   - each `send(chunk)` writes `chunk` to a deterministic chunk
    ///     key under a reserved `\xff\xfe`-prefixed namespace via the
    ///     existing `put` RPC
    ///   - `commit` writes a 28-byte `Meta` blob to `key` (magic +
    ///     version + total_bytes + chunk_count + chunk_size + crc32c)
    ///     — its presence is the atomic linearisation point: until
    ///     `commit` returns, `get(key)` returns NotFound and orphan
    ///     chunks are invisible
    ///   - `abort` best-effort deletes the partial chunks
    ///
    /// Replaces the deprecated server-side multipart upload (F129
    /// `MSG_PUT_BEGIN/CHUNK/COMMIT/ABORT` + multi-fragment ValuePointer +
    /// F130 GC active rewrite). Total simplification: ~1500 server-side
    /// lines deleted, ~200 client-side lines added; behaviour from the
    /// caller's perspective is identical.
    ///
    /// `expires_at` is currently ignored — chunk-level TTL would
    /// require setting `expires_at` on each chunk's Put. Future work.
    pub fn put_stream_begin(&self, key: &[u8], _expires_at: u64) -> PutStreamHandle<'_> {
        PutStreamHandle {
            cluster: self,
            user_key: key.to_vec(),
            chunk_size: STRIPE_CHUNK_SIZE,
            next_chunk_index: 0,
            bytes_sent: 0,
            state: PutStreamState::Open,
        }
    }

    /// F186 — Open a streaming reader. Auto-detects:
    ///   - **striped** values (key holds a 28-byte `Meta` blob with the
    ///     magic) → reads chunks under the reserved chunk-key namespace
    ///     and yields them via `next_chunk()`
    ///   - **inline** values (anything else, including a normal small
    ///     `put`) → yields the entire value in one `next_chunk()` call
    ///
    /// `chunk_size_hint` controls how much each `next_chunk()` returns
    /// for striped values (chunks are buffered and re-sliced on the
    /// client). Hint <= stored chunk_size yields slices; hint > stored
    /// yields whatever the next stored chunk holds (no merging — keeps
    /// memory bounded).
    ///
    /// Returns `Ok(None)` if the key doesn't exist.
    pub async fn get_stream(
        &self,
        key: &[u8],
        chunk_size_hint: u32,
    ) -> std::result::Result<Option<GetStream<'_>>, AutumnError> {
        let blob = match self.get(key).await? {
            Some(b) => b,
            None => return Ok(None),
        };
        let key_vec = key.to_vec();
        match StripeMeta::try_decode(&blob) {
            Some(meta) => Ok(Some(GetStream {
                cluster: self,
                user_key: key_vec,
                inline: None,
                striped: Some(meta),
                cursor: 0,
                next_chunk_index: 0,
                pending_buf: Vec::new(),
                pending_pos: 0,
                yield_size: chunk_size_hint.max(1),
            })),
            None => Ok(Some(GetStream {
                cluster: self,
                user_key: key_vec,
                inline: Some(blob),
                striped: None,
                cursor: 0,
                next_chunk_index: 0,
                pending_buf: Vec::new(),
                pending_pos: 0,
                yield_size: chunk_size_hint.max(1),
            })),
        }
    }

    /// F186 — Cascade-delete a striped value: read meta, delete all
    /// chunks, delete meta. For non-striped keys this is just a regular
    /// delete on the meta key (chunks don't exist). Idempotent on
    /// already-deleted keys.
    ///
    /// Plain `delete()` (existing) only removes the meta key, leaving
    /// chunks orphan — use this method when the caller has been
    /// using `put_stream_begin` for the key.
    pub async fn delete_stream(&self, key: &[u8]) -> std::result::Result<(), AutumnError> {
        if let Some(blob) = self.get(key).await? {
            if let Some(meta) = StripeMeta::try_decode(&blob) {
                for i in 0..meta.chunk_count as u64 {
                    let ck = make_chunk_key(key, i);
                    let _ = self.delete(&ck).await;
                }
            }
        }
        self.delete(key).await
    }

    /// Trigger partition split.
    pub async fn split(&self, part_id: u64) -> std::result::Result<(), AutumnError> {
        self.call_ps_for_part(
            part_id,
            MSG_SPLIT_PART,
            rkyv_encode(&SplitPartReq { part_id }),
        )
        .await?;
        Ok(())
    }

    /// Trigger compaction on a partition.
    pub async fn compact(&self, part_id: u64) -> std::result::Result<(), AutumnError> {
        self.maintenance(part_id, MAINTENANCE_COMPACT, vec![]).await
    }

    /// Trigger automatic GC on a partition.
    pub async fn gc(&self, part_id: u64) -> std::result::Result<(), AutumnError> {
        self.gc_with_params(part_id, GcAutoParams::default()).await
    }

    /// F201: trigger automatic GC with multi-tier filter parameters.
    /// `params` is forwarded verbatim to the PS-side selection logic.
    /// Default `GcAutoParams::default()` matches pre-F201 behaviour
    /// (`discard_ratio > 0.4`, plus the new empty-sealed-extent free
    /// path).
    pub async fn gc_with_params(
        &self,
        part_id: u64,
        params: GcAutoParams,
    ) -> std::result::Result<(), AutumnError> {
        let req = MaintenanceReq {
            part_id,
            op: MAINTENANCE_AUTO_GC,
            extent_ids: vec![],
            gc_ratio: params.ratio,
            gc_max_size: params.max_size,
            gc_stream_debt: params.stream_debt,
            gc_empty_only: params.empty_only,
        };
        let resp_bytes = self
            .call_ps_for_part(part_id, MSG_MAINTENANCE, rkyv_encode(&req))
            .await?;
        let resp: MaintenanceResp = rkyv_decode(&resp_bytes).map_err(AutumnError::ServerError)?;
        check_ps_code(resp.code, &resp.message)?;
        Ok(())
    }

    /// Force GC of specific extents on a partition.
    pub async fn force_gc(
        &self,
        part_id: u64,
        extent_ids: Vec<u64>,
    ) -> std::result::Result<(), AutumnError> {
        self.maintenance(part_id, MAINTENANCE_FORCE_GC, extent_ids)
            .await
    }

    /// Trigger flush on a partition.
    pub async fn flush(&self, part_id: u64) -> std::result::Result<(), AutumnError> {
        self.maintenance(part_id, MAINTENANCE_FLUSH, vec![]).await
    }

    /// F183: merge two adjacent partitions. Survivor keeps its part_id;
    /// victim is deleted from the manager.
    ///
    /// Stage 1 implementation orchestrates the merge from the client:
    ///   1. FLUSH on both partitions (drains imm into SSTs durable in row_stream).
    ///   2. Read commit_length on each of the six streams via stream_info.
    ///   3. Call manager's MSG_MULTI_MODIFY_MERGE with the sealed lengths.
    ///   4. On success, the survivor's PS picks up the wider rg + spliced
    ///      extent_ids on the next region_sync_loop tick (~2 s). Brief
    ///      unavailability during the reopen is the trade-off for not
    ///      requiring a PS-side splice handler in Stage 1.
    ///
    /// F185: thin wrapper around the manager-orchestrated `MSG_MERGE_PARTITIONS`.
    ///
    /// The manager (which is leader-fenced and crash-recoverable via etcd)
    /// owns the full sequence:
    ///   - acquire admin owner-lock for the partition pair
    ///   - send `MSG_MERGE_FREEZE` to both PSes (drains pending+inflight,
    ///     flushes all imm, halts new writes with `CODE_UNAVAILABLE`)
    ///   - capture `commit_length` × 6 under the freeze (closes the
    ///     ~5 % loss window measured by F184-K)
    ///   - run the F183 atomic etcd merge txn
    ///   - on success: leave PSes frozen — region_sync_loop drops the
    ///     frozen `PartitionData` on its next ~2 s tick
    ///   - on failure: send `freeze=false` rollback to both PSes
    ///
    /// CLI crashes mid-call are now benign: the manager continues
    /// serving the orchestrated merge, and a 30 s `FREEZE_TTL` on the
    /// PS side is the final backstop for an orchestrator crash.
    pub async fn merge_partitions(
        &self,
        survivor_part_id: u64,
        victim_part_id: u64,
    ) -> std::result::Result<(), AutumnError> {
        self.flush(survivor_part_id).await?;
        self.flush(victim_part_id).await?;
        let req = rkyv_encode(&MergePartitionsReq {
            survivor_part_id,
            victim_part_id,
        });
        let resp_bytes = self
            .mgr_call(MSG_MERGE_PARTITIONS, req)
            .await
            .map_err(|e| AutumnError::ServerError(e.to_string()))?;
        let resp: MergePartitionsResp =
            rkyv_decode(&resp_bytes).map_err(AutumnError::ServerError)?;
        if resp.code != autumn_rpc::manager_rpc::CODE_OK {
            return Err(AutumnError::ServerError(resp.message));
        }
        Ok(())
    }

    /// F183: query the manager's policy-engine advisory cache.
    pub async fn policy_candidates(
        &self,
    ) -> std::result::Result<Vec<PolicyCandidate>, AutumnError> {
        let resp_bytes = self
            .mgr_call(
                MSG_GET_POLICY_CANDIDATES,
                rkyv_encode(&GetPolicyCandidatesReq::default()),
            )
            .await
            .map_err(|e| AutumnError::ServerError(e.to_string()))?;
        let resp: GetPolicyCandidatesResp =
            rkyv_decode(&resp_bytes).map_err(AutumnError::ServerError)?;
        if resp.code != autumn_rpc::manager_rpc::CODE_OK {
            return Err(AutumnError::ServerError(resp.message));
        }
        Ok(resp.candidates)
    }

    async fn maintenance(
        &self,
        part_id: u64,
        op: u8,
        extent_ids: Vec<u64>,
    ) -> std::result::Result<(), AutumnError> {
        let resp_bytes = self
            .call_ps_for_part(
                part_id,
                MSG_MAINTENANCE,
                rkyv_encode(&MaintenanceReq {
                    part_id,
                    op,
                    extent_ids,
                    // F201: legacy callers (compact / force_gc / flush)
                    // don't supply GC tier params — they're ignored when
                    // op != MAINTENANCE_AUTO_GC anyway. Defaults match
                    // pre-F201 wire shape semantically.
                    gc_ratio: None,
                    gc_max_size: None,
                    gc_stream_debt: None,
                    gc_empty_only: false,
                }),
            )
            .await?;
        let resp: MaintenanceResp = rkyv_decode(&resp_bytes).map_err(AutumnError::ServerError)?;
        check_ps_code(resp.code, &resp.message)?;
        Ok(())
    }
}

/// F201: SDK-visible mirror of the PS-side `GcAutoParams`. Forwarded
/// verbatim through `MaintenanceReq` to the partition's GC loop.
/// Default matches pre-F201 behaviour (`discard_ratio > 0.4`).
#[derive(Default, Clone, Debug)]
pub struct GcAutoParams {
    pub ratio: Option<f64>,
    pub max_size: Option<u64>,
    pub stream_debt: Option<u64>,
    pub empty_only: bool,
}

// ── F186 client-side striperados (Ceph-style) ─────────────────────────────
//
// Replaces F129's server-side multipart upload + multi-fragment ValuePointer
// + F130 GC active rewrite. Pure client-side striping over the existing
// MSG_PUT / MSG_GET / MSG_DELETE primitives — no new server RPCs, no
// changes to the WAL / memtable / SSTable shape.
//
// Layout:
//   - Meta object lives at the user's key. 28-byte fixed-shape blob with
//     magic + version + total_bytes + chunk_count + chunk_size + crc32c.
//     Its presence is the atomic linearisation point: if the meta is
//     visible, the value is committed; if absent, any orphan chunks are
//     invisible.
//   - Chunks live at deterministic keys under a reserved 0xff-prefixed
//     namespace so range scans of user prefixes don't see them.

/// F186 — meta object header magic + version, 9 bytes total. The last
/// byte is the version; bumping the wire format means bumping that
/// byte. Chosen to start with `0xfe 0xfd` so a casual `head` of a
/// striped key returns a recognisable hex prefix in operator tooling.
const META_MAGIC: [u8; 9] = *b"\xfeAUTSTRP\x01";
const META_VERSION_BYTE_INDEX: usize = 8;
const META_VERSION: u8 = 0x01;
/// 9 (magic+version) + 8 (total_bytes) + 4 (chunk_count) + 4 (chunk_size)
/// + 4 (crc32c) = 29 bytes. CRC covers magic+version+total_bytes+
/// chunk_count+chunk_size i.e. the first 25 bytes.
const META_ENCODED_LEN: usize = 29;

/// F186 — default chunk size for `put_stream_begin`. 4 MiB matches
/// the inline-VP threshold ratio used by other F129-replacement
/// systems (S3 multipart's 5 MiB minimum, Ceph striperados 4 MiB
/// default). Each chunk is one `MSG_PUT` RPC + one VP entry on the
/// PS — large enough to amortise per-chunk RPC framing, small enough
/// that a transient connection error costs only a 4 MiB resend.
pub const STRIPE_CHUNK_SIZE: u32 = 4 * 1024 * 1024;

/// F186 — chunk-key namespace prefix. Starts with `\xff\xfe` so chunks
/// sort AFTER all normal user keys; range scans within a user prefix
/// naturally skip them. Followed by a 4-byte format tag (`acv1` =
/// "Autumn Chunk v1") and a separator byte.
const CHUNK_KEY_PREFIX: &[u8] = b"\xff\xfeacv1\xff";

/// F186 — encode a chunk key as
/// `[CHUNK_KEY_PREFIX][user_key_len:u32 BE][user_key][chunk_index:u64 BE]`.
/// Length-prefixing the user key avoids ambiguity if it contains the
/// separator pattern; BE encoding on the index keeps chunks of one
/// stripe sorted in numerical order.
pub(crate) fn make_chunk_key(user_key: &[u8], chunk_index: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(CHUNK_KEY_PREFIX.len() + 4 + user_key.len() + 8);
    k.extend_from_slice(CHUNK_KEY_PREFIX);
    k.extend_from_slice(&(user_key.len() as u32).to_be_bytes());
    k.extend_from_slice(user_key);
    k.extend_from_slice(&chunk_index.to_be_bytes());
    k
}

/// F186 — striped-value meta header. Fixed 28-byte layout makes
/// `try_decode` cheap and unambiguous.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StripeMeta {
    pub total_bytes: u64,
    pub chunk_count: u32,
    pub chunk_size: u32,
}

impl StripeMeta {
    fn encode(&self) -> [u8; META_ENCODED_LEN] {
        let mut buf = [0u8; META_ENCODED_LEN];
        buf[0..9].copy_from_slice(&META_MAGIC);
        buf[9..17].copy_from_slice(&self.total_bytes.to_le_bytes());
        buf[17..21].copy_from_slice(&self.chunk_count.to_le_bytes());
        buf[21..25].copy_from_slice(&self.chunk_size.to_le_bytes());
        let crc = crc32c::crc32c(&buf[0..25]);
        buf[25..29].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// F186 — try to interpret `blob` as a stripe meta. Returns None if
    /// length, magic, or CRC don't match — the caller treats the blob
    /// as an inline value in that case.
    ///
    /// False-positive analysis: the magic is 8 bytes (~64 bits), the
    /// CRC is 32 bits over magic + total_bytes + chunk_count +
    /// chunk_size, AND length must be exactly 28. The probability that
    /// a user-supplied 28-byte value matches all three by accident is
    /// negligible (~2^-96). A user could MALICIOUSLY craft a 28-byte
    /// value that matches; the SDK handles "missing chunks" gracefully
    /// (returns NotFound or a clean error).
    pub(crate) fn try_decode(blob: &[u8]) -> Option<Self> {
        if blob.len() != META_ENCODED_LEN {
            return None;
        }
        if blob[0..META_VERSION_BYTE_INDEX] != META_MAGIC[0..META_VERSION_BYTE_INDEX] {
            return None;
        }
        if blob[META_VERSION_BYTE_INDEX] != META_VERSION {
            return None;
        }
        let stored_crc = u32::from_le_bytes(blob[25..29].try_into().ok()?);
        let computed_crc = crc32c::crc32c(&blob[0..25]);
        if stored_crc != computed_crc {
            return None;
        }
        Some(StripeMeta {
            total_bytes: u64::from_le_bytes(blob[9..17].try_into().ok()?),
            chunk_count: u32::from_le_bytes(blob[17..21].try_into().ok()?),
            chunk_size: u32::from_le_bytes(blob[21..25].try_into().ok()?),
        })
    }
}

/// Bug #1 fix (2026-06-06) — pure-fn extracted from
/// `wait_for_cluster_ready` so the readiness predicate is unit-testable
/// without booting a real manager. Resolves every partition's
/// `(part_id, ps_id)` to the address the SDK's `lookup_key` would
/// produce (`part_addrs[part_id]` if present, else
/// `ps_details[ps_id]`), and reports:
///
///   - `total_partitions`: regions count (manager's view).
///   - `missing_addrs`: partitions whose `(part_id, ps_id)` resolved
///     to NO address at all — the manager hasn't published a
///     `RegisterPartitionAddr` for this partition AND there's no
///     `ps_details` fallback. Treated as "not ready yet."
///   - `probe_addrs`: distinct ps_addrs the caller should TCP-probe
///     to confirm the listener is actually bound (a partition can
///     have a `part_addr` in the manager's response and yet have a
///     listener that won't accept until the PS thread's
///     `partition_loop` runs `partition listener bound`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterReadySummary {
    pub total_partitions: usize,
    pub missing_addrs: Vec<u64>,
    pub probe_addrs: Vec<String>,
}

pub fn cluster_ready_summary(
    regions: &[(u64, u64)],
    part_addrs: &HashMap<u64, String>,
    ps_details: &HashMap<u64, String>,
) -> ClusterReadySummary {
    let mut missing_addrs: Vec<u64> = Vec::new();
    let mut probe_addrs: Vec<String> = Vec::new();
    for &(part_id, ps_id) in regions {
        let addr = part_addrs
            .get(&part_id)
            .cloned()
            .or_else(|| ps_details.get(&ps_id).cloned());
        match addr {
            Some(a) => probe_addrs.push(a),
            None => missing_addrs.push(part_id),
        }
    }
    probe_addrs.sort();
    probe_addrs.dedup();
    missing_addrs.sort();
    ClusterReadySummary {
        total_partitions: regions.len(),
        missing_addrs,
        probe_addrs,
    }
}

#[cfg(test)]
mod cluster_ready_tests {
    use super::*;

    fn psd(addr: &str) -> String {
        addr.to_string()
    }

    #[test]
    fn empty_cluster_reports_zero_partitions() {
        let s = cluster_ready_summary(&[], &HashMap::new(), &HashMap::new());
        assert_eq!(s.total_partitions, 0);
        assert!(s.missing_addrs.is_empty());
        assert!(s.probe_addrs.is_empty());
    }

    #[test]
    fn all_partitions_have_part_addrs_no_missing() {
        // Healthy steady state: every partition has a per-partition
        // listener registered.
        let regions = vec![(13, 1), (20, 1), (27, 1)];
        let part_addrs: HashMap<u64, String> = [
            (13, psd("127.0.0.1:9301")),
            (20, psd("127.0.0.1:9302")),
            (27, psd("127.0.0.1:9303")),
        ]
        .into_iter()
        .collect();
        let ps_details: HashMap<u64, String> = [(1, psd("127.0.0.1:9301"))].into_iter().collect();
        let s = cluster_ready_summary(&regions, &part_addrs, &ps_details);
        assert_eq!(s.total_partitions, 3);
        assert!(s.missing_addrs.is_empty());
        assert_eq!(
            s.probe_addrs,
            vec![
                "127.0.0.1:9301".to_string(),
                "127.0.0.1:9302".to_string(),
                "127.0.0.1:9303".to_string(),
            ]
        );
    }

    #[test]
    fn missing_part_addr_with_ps_details_fallback_counts_as_resolved() {
        // partition 20 has no per-partition listener YET — the
        // manager reports it but `RegisterPartitionAddr` hasn't
        // landed. ps_details for ps_id=1 has the base port. SDK's
        // lookup_key would route to 9301 (the base PS port), which
        // serves the FIRST partition opened on that PS (part 13).
        //
        // **Sending a part_id=20 PutReq to 9301 mis-routes** —
        // partition_loop synths NotFound. The pure-fn here marks
        // this as resolved (we DO have an address to try) but the
        // probe step will still flag if 9301 isn't accepting yet.
        let regions = vec![(13, 1), (20, 1)];
        let part_addrs: HashMap<u64, String> =
            [(13, psd("127.0.0.1:9301"))].into_iter().collect();
        let ps_details: HashMap<u64, String> = [(1, psd("127.0.0.1:9301"))].into_iter().collect();
        let s = cluster_ready_summary(&regions, &part_addrs, &ps_details);
        assert_eq!(s.total_partitions, 2);
        // Note: missing_addrs is empty (the ps_details fallback
        // provided SOMETHING). This is the Bug #1 false-positive
        // case — the SDK has SOMETHING to call but it mis-routes.
        // The caller's NEXT retry of refresh_regions picks up the
        // real part_addr once PS registers it, fixing the resolve.
        // wait_for_cluster_ready relies on the probe step + the
        // 60 s retry budget to ride out this window.
        assert!(s.missing_addrs.is_empty());
        // Both partitions probe the same base port → deduped to one.
        assert_eq!(s.probe_addrs, vec!["127.0.0.1:9301".to_string()]);
    }

    #[test]
    fn missing_part_addr_and_missing_ps_details_reports_missing() {
        // The strict "not ready" state: manager has assigned the
        // partition to ps_id=2 but the PS process hasn't connected
        // at all (no ps_details entry). No address to even try.
        let regions = vec![(13, 1), (99, 2)];
        let part_addrs: HashMap<u64, String> =
            [(13, psd("127.0.0.1:9301"))].into_iter().collect();
        let ps_details: HashMap<u64, String> = [(1, psd("127.0.0.1:9301"))].into_iter().collect();
        let s = cluster_ready_summary(&regions, &part_addrs, &ps_details);
        assert_eq!(s.total_partitions, 2);
        assert_eq!(s.missing_addrs, vec![99]);
    }

    #[test]
    fn probe_addrs_are_sorted_and_deduped() {
        // Multiple partitions share the same per-partition listener
        // (would only happen on a misconfiguration but be robust).
        let regions = vec![(20, 1), (13, 1), (27, 1)];
        let part_addrs: HashMap<u64, String> = [
            (13, psd("127.0.0.1:9301")),
            (20, psd("127.0.0.1:9301")),
            (27, psd("127.0.0.1:9301")),
        ]
        .into_iter()
        .collect();
        let ps_details: HashMap<u64, String> = HashMap::new();
        let s = cluster_ready_summary(&regions, &part_addrs, &ps_details);
        assert_eq!(s.probe_addrs, vec!["127.0.0.1:9301".to_string()]);
    }

    #[test]
    fn missing_addrs_are_sorted_for_diagnostic_stability() {
        // The error message must be deterministic across runs so
        // operators can grep for it.
        let regions = vec![(99, 7), (13, 1), (50, 9)];
        let part_addrs: HashMap<u64, String> = HashMap::new();
        let ps_details: HashMap<u64, String> = HashMap::new();
        let s = cluster_ready_summary(&regions, &part_addrs, &ps_details);
        assert_eq!(s.missing_addrs, vec![13, 50, 99]);
    }
}

#[cfg(test)]
mod first_attempt_timeout_tests {
    //! Bug #2 fix (2026-06-06) — `first_attempt_effective_timeout` is
    //! the policy that fast-fails a stale PS conn within 5 s on the
    //! first attempt of `call_ps_for_key` / `call_ps_for_part`,
    //! falling back to the full 30 s `rpc_timeout` on retries against
    //! the freshly-reconnected listener.

    use super::*;

    fn client_with(rpc: Option<Duration>, first: Option<Duration>) -> ClusterClient {
        // We can't actually `connect` in unit tests, so build a fake
        // shell. The internal fields are private but the public
        // setters cover what the test needs.
        let cluster = compio::runtime::Runtime::new().unwrap().block_on(async {
            // Touch nothing on the wire; just exercise the policy.
            // We construct a fake ClusterClient via the visible API:
            // there isn't one, so we use a real address that won't
            // be dialled (the policy fn doesn't dial).
            ClusterClient {
                manager_addrs: vec!["127.0.0.1:1".to_string()],
                current_mgr: Cell::new(0),
                mgr_conn: Rc::new(RefCell::new(None)),
                ps_conns: RefCell::new(HashMap::new()),
            en_pool: autumn_stream::ConnPool::new(),
                regions: RefCell::new(Vec::new()),
                ps_details: RefCell::new(HashMap::new()),
                part_addrs: RefCell::new(HashMap::new()),
                rpc_timeout: Cell::new(rpc),
                first_attempt_timeout: Cell::new(first),
            }
        });
        cluster
    }

    #[test]
    fn attempt_0_uses_min_of_rpc_and_first_attempt() {
        let c = client_with(
            Some(Duration::from_secs(30)),
            Some(Duration::from_secs(5)),
        );
        assert_eq!(
            c.first_attempt_effective_timeout(0),
            Some(Duration::from_secs(5)),
            "attempt 0 → fast-fail at 5 s (min)"
        );
    }

    #[test]
    fn attempt_1_falls_back_to_full_rpc_timeout() {
        let c = client_with(
            Some(Duration::from_secs(30)),
            Some(Duration::from_secs(5)),
        );
        assert_eq!(
            c.first_attempt_effective_timeout(1),
            Some(Duration::from_secs(30)),
            "attempt 1 → freshly reconnected, give it the full 30 s"
        );
    }

    #[test]
    fn no_first_attempt_timeout_uses_rpc_timeout() {
        let c = client_with(Some(Duration::from_secs(30)), None);
        assert_eq!(c.first_attempt_effective_timeout(0), Some(Duration::from_secs(30)));
        assert_eq!(c.first_attempt_effective_timeout(1), Some(Duration::from_secs(30)));
    }

    #[test]
    fn no_rpc_timeout_uses_first_attempt_on_first() {
        // Discouraged but supported.
        let c = client_with(None, Some(Duration::from_secs(5)));
        assert_eq!(c.first_attempt_effective_timeout(0), Some(Duration::from_secs(5)));
        assert_eq!(c.first_attempt_effective_timeout(1), None);
    }

    #[test]
    fn both_none_means_wait_forever() {
        let c = client_with(None, None);
        assert_eq!(c.first_attempt_effective_timeout(0), None);
        assert_eq!(c.first_attempt_effective_timeout(1), None);
    }

    #[test]
    fn first_attempt_longer_than_rpc_clamps_to_rpc() {
        // Edge case: an operator sets `set_first_attempt_timeout`
        // to a value LARGER than `rpc_timeout`. The min ensures we
        // never go above `rpc_timeout` (it's a hard ceiling).
        let c = client_with(
            Some(Duration::from_secs(2)),
            Some(Duration::from_secs(10)),
        );
        assert_eq!(c.first_attempt_effective_timeout(0), Some(Duration::from_secs(2)));
    }
}

#[cfg(test)]
mod zc_rule_tests {
    use super::*;

    // F235: ZC is engaged iff value >= 64 KiB — symmetric across read/write +
    // transport. Guards against re-introducing an `is_ucx`-gated asymmetry.
    #[test]
    fn zc_worthwhile_is_symmetric_at_64k() {
        assert!(!zc_worthwhile(0));
        assert!(!zc_worthwhile(4 * 1024));
        assert!(!zc_worthwhile(UCX_ZC_READ_MIN_BYTES - 1));
        assert!(zc_worthwhile(UCX_ZC_READ_MIN_BYTES));
        assert!(zc_worthwhile(8 * 1024 * 1024));
    }
}

#[cfg(test)]
mod fan_out_tests {
    use super::*;
    use futures::stream::StreamExt;
    use std::time::Duration;

    // F245: `fan_out_collect` returns INPUT order even when futures complete in
    // reverse (index 0 sleeps longest). The reorder is deterministic regardless
    // of timing, so this is not flaky.
    #[test]
    fn collect_returns_input_order_despite_reverse_completion() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let futs = (0..10usize).map(|i| async move {
                compio::time::sleep(Duration::from_millis((10 - i as u64) * 3)).await;
                i * 7
            });
            let out = fan_out_collect(futs, 4).await;
            let expect: Vec<usize> = (0..10).map(|i| i * 7).collect();
            assert_eq!(out, expect);
        });
    }

    // F245: `fan_out` (the streaming base the io_uring daemon will use) yields
    // every input index exactly once, paired with its own output.
    #[test]
    fn fan_out_yields_every_index_once() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let futs = (0..20usize).map(|i| async move { i });
            let mut seen = [false; 20];
            let mut count = 0;
            let mut s = fan_out(futs, 5);
            while let Some((i, v)) = s.next().await {
                assert_eq!(v, i, "value paired with its index");
                assert!(!seen[i], "index {i} yielded twice");
                seen[i] = true;
                count += 1;
            }
            assert_eq!(count, 20);
            assert!(seen.iter().all(|&b| b), "every index yielded");
        });
    }
}

#[cfg(test)]
mod stripe_meta_tests {
    use super::*;

    #[test]
    fn meta_roundtrip() {
        let m = StripeMeta {
            total_bytes: 12 * 1024 * 1024,
            chunk_count: 4,
            chunk_size: 3 * 1024 * 1024,
        };
        let encoded = m.encode();
        assert_eq!(encoded.len(), META_ENCODED_LEN);
        let dec = StripeMeta::try_decode(&encoded).expect("decode");
        assert_eq!(dec.total_bytes, m.total_bytes);
        assert_eq!(dec.chunk_count, m.chunk_count);
        assert_eq!(dec.chunk_size, m.chunk_size);
    }

    #[test]
    fn meta_rejects_wrong_length() {
        assert!(StripeMeta::try_decode(&[0u8; 28]).is_none());
        assert!(StripeMeta::try_decode(&[0u8; 30]).is_none());
        assert!(StripeMeta::try_decode(b"hello world").is_none());
    }

    #[test]
    fn meta_rejects_bad_magic() {
        let m = StripeMeta {
            total_bytes: 100,
            chunk_count: 1,
            chunk_size: 100,
        };
        let mut e = m.encode();
        e[0] ^= 0x01;
        assert!(StripeMeta::try_decode(&e).is_none());
    }

    #[test]
    fn meta_rejects_corrupted_crc() {
        let m = StripeMeta {
            total_bytes: 100,
            chunk_count: 1,
            chunk_size: 100,
        };
        let mut e = m.encode();
        // Flip a bit in the body — CRC will mismatch
        e[12] ^= 0x01;
        assert!(StripeMeta::try_decode(&e).is_none());
    }

    #[test]
    fn chunk_key_disambiguates_user_keys() {
        // user_key "ab" + index 5 vs "abc" + index 5 must encode
        // distinctly (length-prefix prevents ambiguity).
        let k1 = make_chunk_key(b"ab", 5);
        let k2 = make_chunk_key(b"abc", 5);
        assert_ne!(k1, k2);
        // Both must start with the reserved prefix.
        assert!(k1.starts_with(CHUNK_KEY_PREFIX));
        assert!(k2.starts_with(CHUNK_KEY_PREFIX));
    }

    #[test]
    fn chunk_keys_sort_by_index() {
        let k0 = make_chunk_key(b"key", 0);
        let k1 = make_chunk_key(b"key", 1);
        let k_big = make_chunk_key(b"key", u64::MAX);
        assert!(k0 < k1);
        assert!(k1 < k_big);
    }
}

/// Lifecycle state of a `PutStreamHandle`. Open → (Committed | Aborted)
/// transitions are one-way; the `commit` / `abort` consumers move the
/// handle. A drop in the `Open` state logs a WARN; the chunks already
/// written under the reserved namespace become orphan until a sweep
/// cleans them up (no automatic TTL — that was an F129 server-side
/// feature; client-side striping pushes orphan handling to the
/// application layer or a periodic GC tool).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PutStreamState {
    Open,
    Committed,
    Aborted,
}

/// F186 — In-progress striped put. Borrows the `ClusterClient` so each
/// `send`/`commit` can route its sub-Put through the SDK's existing
/// resolution + connection caching. The handle is `!Send` (transitively
/// through the SDK's `Rc` internals); don't move it across runtimes.
pub struct PutStreamHandle<'a> {
    cluster: &'a ClusterClient,
    user_key: Vec<u8>,
    chunk_size: u32,
    next_chunk_index: u64,
    bytes_sent: u64,
    state: PutStreamState,
}

impl<'a> PutStreamHandle<'a> {
    /// Total bytes successfully appended (sum of all `send` sizes).
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent
    }

    /// Number of chunks successfully appended.
    pub fn chunks_sent(&self) -> u64 {
        self.next_chunk_index
    }

    /// Override the default 4 MiB chunk size. Must be called before
    /// the first `send`.
    pub fn with_chunk_size(mut self, chunk_size: u32) -> Self {
        debug_assert_eq!(self.next_chunk_index, 0, "chunk_size set after send");
        self.chunk_size = chunk_size.max(1);
        self
    }

    /// Append one chunk. The SDK writes it as a normal `Put` to a
    /// reserved-namespace chunk key. Returns the running total bytes
    /// committed.
    ///
    /// Caller is responsible for chunk sizing — typically 1–4 MiB. The
    /// 4 KiB inline-VP threshold on the PS means each chunk goes via
    /// the VP path on the server; very small chunks would inflate
    /// memtable entry counts proportionally.
    pub async fn send(&mut self, chunk: &[u8]) -> std::result::Result<u64, AutumnError> {
        if self.state != PutStreamState::Open {
            return Err(AutumnError::InvalidArgument(format!(
                "PutStreamHandle is {:?}, not Open",
                self.state
            )));
        }
        let chunk_key = make_chunk_key(&self.user_key, self.next_chunk_index);
        self.cluster.put(&chunk_key, chunk).await?;
        self.next_chunk_index = self.next_chunk_index.saturating_add(1);
        self.bytes_sent = self.bytes_sent.saturating_add(chunk.len() as u64);
        Ok(self.bytes_sent)
    }

    /// Finalise the upload. Writes a meta blob to the user key — its
    /// presence is the atomic linearisation point. Until this returns
    /// successfully, `get(key)` returns NotFound and orphan chunks
    /// stay invisible. After this returns, `get` / `get_stream` /
    /// `head` / `delete_stream` see the assembled value.
    pub async fn commit(mut self) -> std::result::Result<(), AutumnError> {
        if self.state != PutStreamState::Open {
            return Err(AutumnError::InvalidArgument(format!(
                "PutStreamHandle is {:?}, cannot commit",
                self.state
            )));
        }
        // Range guard: we encode chunk_count as u32 in the meta blob.
        if self.next_chunk_index > u32::MAX as u64 {
            self.state = PutStreamState::Aborted;
            return Err(AutumnError::InvalidArgument(format!(
                "chunk_count {} exceeds u32::MAX",
                self.next_chunk_index
            )));
        }
        let meta = StripeMeta {
            total_bytes: self.bytes_sent,
            chunk_count: self.next_chunk_index as u32,
            chunk_size: self.chunk_size,
        };
        let blob = meta.encode();
        match self.cluster.put(&self.user_key, &blob).await {
            Ok(()) => {
                self.state = PutStreamState::Committed;
                Ok(())
            }
            Err(e) => {
                // The chunks are now orphan. We don't auto-clean here
                // (`commit` already failed; the user can retry or call
                // `delete_stream` after a successful retry).
                self.state = PutStreamState::Aborted;
                Err(e)
            }
        }
    }

    /// Discard the in-progress upload by best-effort deleting any
    /// already-written chunks. The meta blob was never written, so
    /// `get(key)` already returns NotFound.
    ///
    /// Failures during chunk delete are swallowed — orphans become a
    /// background-sweep concern rather than blocking the abort path.
    pub async fn abort(mut self) -> std::result::Result<(), AutumnError> {
        if self.state != PutStreamState::Open {
            return Ok(());
        }
        for i in 0..self.next_chunk_index {
            let ck = make_chunk_key(&self.user_key, i);
            let _ = self.cluster.delete(&ck).await;
        }
        self.state = PutStreamState::Aborted;
        Ok(())
    }
}

impl<'a> Drop for PutStreamHandle<'a> {
    fn drop(&mut self) {
        if self.state == PutStreamState::Open {
            tracing::warn!(
                key = ?String::from_utf8_lossy(&self.user_key),
                bytes_sent = self.bytes_sent,
                chunks_sent = self.next_chunk_index,
                "PutStreamHandle dropped without commit/abort; meta blob \
                 was not written so the value is invisible to readers, but \
                 {} chunks are now orphan under the reserved namespace and \
                 require a background sweep or `delete_stream` retry to clean",
                self.next_chunk_index
            );
        }
    }
}

/// F186 — streaming reader. For striped values: walks chunk keys
/// in order. For inline values: yields the entire blob in one
/// `next_chunk` call. The same `GetStream` shape so callers don't
/// need to know which path they're on.
pub struct GetStream<'a> {
    cluster: &'a ClusterClient,
    user_key: Vec<u8>,
    /// `Some` for inline values — the blob to yield in one shot.
    inline: Option<Vec<u8>>,
    /// `Some` for striped values — describes the chunk layout.
    striped: Option<StripeMeta>,
    /// Bytes yielded so far (assembled-value cursor).
    cursor: u64,
    /// Next chunk index to fetch (only for striped).
    next_chunk_index: u64,
    /// Buffered chunk bytes from the most recent fetch (striped path).
    pending_buf: Vec<u8>,
    /// Read cursor inside `pending_buf`.
    pending_pos: usize,
    /// Max bytes to yield per `next_chunk` call.
    yield_size: u32,
}

impl<'a> GetStream<'a> {
    /// Total assembled-value size in bytes.
    pub fn total_bytes(&self) -> u64 {
        if let Some(m) = &self.striped {
            m.total_bytes
        } else if let Some(b) = &self.inline {
            b.len() as u64
        } else {
            0
        }
    }

    /// Bytes yielded so far.
    pub fn position(&self) -> u64 {
        self.cursor
    }

    /// Bytes remaining to yield.
    pub fn remaining(&self) -> u64 {
        self.total_bytes().saturating_sub(self.cursor)
    }

    /// Pull the next chunk. Returns `Ok(None)` when the value is
    /// exhausted. The yielded chunk is at most `yield_size` bytes;
    /// the final chunk may be shorter.
    pub async fn next_chunk(&mut self) -> std::result::Result<Option<Vec<u8>>, AutumnError> {
        if self.cursor >= self.total_bytes() {
            return Ok(None);
        }

        // Inline path — single yield of the whole blob.
        if let Some(blob) = self.inline.take() {
            self.cursor = blob.len() as u64;
            return Ok(Some(blob));
        }

        // Striped path — refill pending buf if drained.
        let meta = self.striped.as_ref().expect("inline or striped");
        while self.pending_pos >= self.pending_buf.len() {
            if self.next_chunk_index >= meta.chunk_count as u64 {
                return Ok(None);
            }
            let ck = make_chunk_key(&self.user_key, self.next_chunk_index);
            let chunk = match self.cluster.get(&ck).await? {
                Some(c) => c,
                None => {
                    return Err(AutumnError::ServerError(format!(
                        "striped chunk {} missing for key — value may have been \
                         partially deleted; call delete_stream + re-upload",
                        self.next_chunk_index
                    )));
                }
            };
            self.pending_buf = chunk;
            self.pending_pos = 0;
            self.next_chunk_index = self.next_chunk_index.saturating_add(1);
        }

        let avail = self.pending_buf.len() - self.pending_pos;
        let want = (self.yield_size as usize).min(avail);
        let out = self.pending_buf[self.pending_pos..self.pending_pos + want].to_vec();
        self.pending_pos += want;
        self.cursor = self.cursor.saturating_add(want as u64);
        Ok(Some(out))
    }
}
