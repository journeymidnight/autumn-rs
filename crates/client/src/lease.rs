//! F-ioring-lease-2 + F-fuse-lease-1 — client-side helpers for the
//! four inode lease RPCs (`MSG_ACQUIRE_LEASE` / `MSG_RELEASE_LEASE` /
//! `MSG_HEARTBEAT_LEASE` / `MSG_POLL_INVALIDATIONS`).
//!
//! Wraps the rkyv encode/decode + manager round-trip into typed
//! methods so callers (autumn-ioring daemon AND autumn-fuse mount)
//! keep their Open/Close paths small. Defines the per-process
//! `DaemonClientId` (one UUID per session; reused for every RPC so
//! the manager's lease state stays stable across reconnects).
//!
//! Layering: this module reaches the manager via
//! `ClusterClient::mgr_call_retry` — the same auto-rotate /
//! reconnect path the SDK uses for `MSG_GET_REGIONS`. Manager-side
//! errors surface as `LeaseError::Conflict` (someone else holds the
//! writer slot), `LeaseError::NotLeader` (transient), or
//! `LeaseError::Manager` (other).
//!
//! **History**: lived in `autumn-ioring::lease` for
//! F-ioring-lease-1 through F-ioring-lease-4; moved here in
//! F-fuse-lease-1 so autumn-fuse can call it without pulling in
//! the heavy `autumn-ioring` daemon feature.
//! `autumn-ioring::lease` re-exports from this module.

use anyhow::Result;
use crate::ClusterClient;
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, AcquireLeaseReq, AcquireLeaseResp, HeartbeatLeaseReq,
    HeartbeatLeaseResp, MgrClientId, MgrInodeLeaseInfo, MgrInvalidation, PollInvalidationsReq,
    PollInvalidationsResp, ReleaseLeaseReq, ReleaseLeaseResp, CODE_NOT_FOUND, CODE_NOT_LEADER,
    CODE_OK, CODE_PRECONDITION, CODE_REVOKE_PENDING, LEASE_CLIENT_KIND_IORING, LEASE_MODE_READ,
    LEASE_MODE_WRITE, MSG_ACQUIRE_LEASE, MSG_HEARTBEAT_LEASE, MSG_POLL_INVALIDATIONS,
    MSG_RELEASE_LEASE,
};

/// Stable per-runtime daemon identity. Generated once at runtime
/// startup and reused for every lease RPC. The manager keys the
/// lease registry on `(kind, uuid)`; the host string is diagnostic.
#[derive(Clone, Debug)]
pub struct DaemonClientId {
    inner: MgrClientId,
}

impl DaemonClientId {
    /// Generate a fresh identity. `host` is observable to operators
    /// in `autumn-op` lease listings — typically `hostname` +
    /// `runtime_idx`. Anything is acceptable; identity is the UUID.
    pub fn new(host: impl Into<String>) -> Self {
        Self::new_with_kind(LEASE_CLIENT_KIND_IORING, host)
    }

    /// F-fuse-lease-1: fresh identity for an autumn-fuse mount
    /// (`kind = LEASE_CLIENT_KIND_FUSE`). Functionally identical to
    /// `new` — the manager treats every kind the same; the
    /// discriminant is diagnostic so `autumn-op` can label which
    /// daemon family owns each lease.
    pub fn new_fuse(host: impl Into<String>) -> Self {
        Self::new_with_kind(autumn_rpc::manager_rpc::LEASE_CLIENT_KIND_FUSE, host)
    }

    fn new_with_kind(kind: u8, host: impl Into<String>) -> Self {
        DaemonClientId {
            inner: MgrClientId {
                kind,
                uuid: *uuid::Uuid::new_v4().as_bytes(),
                host: host.into(),
            },
        }
    }

    /// Build from a pre-existing wire identity (used by tests that
    /// want a deterministic UUID).
    pub fn from_wire(inner: MgrClientId) -> Self {
        DaemonClientId { inner }
    }

    pub fn as_wire(&self) -> &MgrClientId {
        &self.inner
    }
}

/// Result of `acquire`.
#[derive(Debug, Clone)]
pub enum AcquireResult {
    /// Lease granted.
    Granted(MgrInodeLeaseInfo),
    /// Another client holds the writer lease for this inode.
    Conflict { manager_message: String },
    /// F-lease-preempt: `acquire_force` started (or re-observed) a
    /// pre-revocation grace window for the held writer. Caller
    /// should sleep ~`eta_ms` and retry; on retry the writer may
    /// have voluntarily released (→ `Granted`) or the grace
    /// may have expired (→ force-revoke + `Granted`).
    RevokePending { eta_ms: u32, manager_message: String },
}

/// Result of `heartbeat`. `NotHeld` means the manager has revoked /
/// expired the client's lease; the daemon must surface this to the
/// open session (cache invalidated) and stop heartbeating.
#[derive(Debug, Clone)]
pub enum HeartbeatResult {
    Renewed(MgrInodeLeaseInfo),
    NotHeld,
}

/// Daemon-side typed errors. Manager errors that don't fit
/// `Conflict` / `NotHeld` are bundled under `Manager`.
#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    #[error("manager not leader (transient)")]
    NotLeader,
    #[error("manager error: code={code} message={message}")]
    Manager { code: u8, message: String },
    #[error("transport: {0}")]
    Transport(String),
}

const RETRY: u32 = 3;

/// Acquire a `(mode = LEASE_MODE_READ | LEASE_MODE_WRITE)` lease on
/// `ino`. The manager auto-rotates / reconnects internally
/// (`mgr_call_retry`). On WRITE conflict, returns
/// `AcquireResult::Conflict` so the caller can map it to `EBUSY` /
/// `EAGAIN`.
pub async fn acquire(
    cluster: &ClusterClient,
    client: &DaemonClientId,
    ino: u64,
    mode: u8,
) -> Result<AcquireResult, LeaseError> {
    acquire_inner(cluster, client, ino, mode, false).await
}

/// F-lease-preempt: acquire with the `force` flag. Identical to
/// `acquire` except: when a different writer is currently held,
/// the manager pushes `WillRevokeIn` to that writer and returns
/// `RevokePending { eta_ms }`. Caller sleeps `eta_ms` and retries
/// (typically via `acquire_with_preempt_wait` which wraps the
/// retry loop). On retry within the grace window: same
/// `RevokePending` (or `Granted` if the writer released
/// voluntarily). After the grace window expires: force-revoke +
/// `Granted`.
///
/// Use `force=true` ONLY when a user-level signal demands
/// preemption (admin tool, NFSv4-style delegation recall) — the
/// non-preempt `acquire` is the default for routine open/close.
pub async fn acquire_force(
    cluster: &ClusterClient,
    client: &DaemonClientId,
    ino: u64,
    mode: u8,
) -> Result<AcquireResult, LeaseError> {
    acquire_inner(cluster, client, ino, mode, true).await
}

async fn acquire_inner(
    cluster: &ClusterClient,
    client: &DaemonClientId,
    ino: u64,
    mode: u8,
    force: bool,
) -> Result<AcquireResult, LeaseError> {
    debug_assert!(
        mode == LEASE_MODE_READ || mode == LEASE_MODE_WRITE,
        "lease mode must be READ or WRITE"
    );
    let req = AcquireLeaseReq {
        client: client.as_wire().clone(),
        ino,
        mode,
        force,
    };
    let bytes = cluster
        .mgr_call_retry(MSG_ACQUIRE_LEASE, rkyv_encode(&req), RETRY)
        .await
        .map_err(|e| LeaseError::Transport(e.to_string()))?;
    let resp: AcquireLeaseResp =
        rkyv_decode(&bytes).map_err(|e| LeaseError::Transport(format!("decode: {e}")))?;
    match resp.code {
        CODE_OK => Ok(AcquireResult::Granted(
            resp.lease.expect("CODE_OK must carry lease"),
        )),
        CODE_PRECONDITION => Ok(AcquireResult::Conflict {
            manager_message: resp.message,
        }),
        CODE_REVOKE_PENDING => {
            // The manager packs `eta_ms` into the response's
            // `lease.ttl_secs` field (repurposed; see the
            // `LEASE_INVAL_WILL_REVOKE_IN` constant on the wire).
            let eta_ms = resp
                .lease
                .as_ref()
                .map(|l| l.ttl_secs)
                .unwrap_or(0);
            Ok(AcquireResult::RevokePending {
                eta_ms,
                manager_message: resp.message,
            })
        }
        CODE_NOT_LEADER => Err(LeaseError::NotLeader),
        c => Err(LeaseError::Manager {
            code: c,
            message: resp.message,
        }),
    }
}

/// F-lease-preempt: convenience wrapper that wraps `acquire_force`
/// with the retry loop the protocol expects. Polls `acquire_force`
/// until the manager returns `Granted` or `Conflict`, or until
/// `max_wait_ms` has elapsed (in which case the last
/// `RevokePending` is propagated). Between polls it sleeps
/// `eta_ms` (capped at 500ms to keep retry-rate sane).
pub async fn acquire_with_preempt_wait(
    cluster: &ClusterClient,
    client: &DaemonClientId,
    ino: u64,
    mode: u8,
    max_wait_ms: u32,
) -> Result<AcquireResult, LeaseError> {
    use std::time::{Duration, Instant};
    let deadline = Instant::now() + Duration::from_millis(max_wait_ms as u64);
    loop {
        match acquire_force(cluster, client, ino, mode).await? {
            r @ AcquireResult::Granted(_) | r @ AcquireResult::Conflict { .. } => {
                return Ok(r);
            }
            r @ AcquireResult::RevokePending { eta_ms, .. } => {
                let now = Instant::now();
                if now >= deadline {
                    return Ok(r);
                }
                let sleep_ms = eta_ms.clamp(50, 500);
                let remaining_u128 = deadline.saturating_duration_since(now).as_millis();
                let remaining = u32::try_from(remaining_u128).unwrap_or(u32::MAX);
                let actual = sleep_ms.min(remaining);
                compio::time::sleep(Duration::from_millis(u64::from(actual))).await;
            }
        }
    }
}

/// Release a lease (writer-close OR reader-disconnect). Idempotent:
/// double-release is `CODE_OK` with `new_version = None`. Returns
/// the post-release version when the manager actually unwound a
/// writer slot, `None` otherwise (reader release / already-released).
pub async fn release(
    cluster: &ClusterClient,
    client: &DaemonClientId,
    ino: u64,
) -> Result<Option<u64>, LeaseError> {
    let req = ReleaseLeaseReq {
        client: client.as_wire().clone(),
        ino,
    };
    let bytes = cluster
        .mgr_call_retry(MSG_RELEASE_LEASE, rkyv_encode(&req), RETRY)
        .await
        .map_err(|e| LeaseError::Transport(e.to_string()))?;
    let resp: ReleaseLeaseResp =
        rkyv_decode(&bytes).map_err(|e| LeaseError::Transport(format!("decode: {e}")))?;
    match resp.code {
        CODE_OK => Ok(resp.new_version),
        CODE_NOT_LEADER => Err(LeaseError::NotLeader),
        c => Err(LeaseError::Manager {
            code: c,
            message: resp.message,
        }),
    }
}

/// Renew a held lease. `HeartbeatResult::NotHeld` ⇒ the manager
/// revoked or expired the lease; the daemon must drop the
/// corresponding cache entry and stop heartbeating this inode.
pub async fn heartbeat(
    cluster: &ClusterClient,
    client: &DaemonClientId,
    ino: u64,
) -> Result<HeartbeatResult, LeaseError> {
    let req = HeartbeatLeaseReq {
        client: client.as_wire().clone(),
        ino,
    };
    let bytes = cluster
        .mgr_call_retry(MSG_HEARTBEAT_LEASE, rkyv_encode(&req), RETRY)
        .await
        .map_err(|e| LeaseError::Transport(e.to_string()))?;
    let resp: HeartbeatLeaseResp =
        rkyv_decode(&bytes).map_err(|e| LeaseError::Transport(format!("decode: {e}")))?;
    match resp.code {
        CODE_OK => Ok(HeartbeatResult::Renewed(
            resp.lease.expect("CODE_OK must carry lease"),
        )),
        CODE_NOT_FOUND => Ok(HeartbeatResult::NotHeld),
        CODE_NOT_LEADER => Err(LeaseError::NotLeader),
        c => Err(LeaseError::Manager {
            code: c,
            message: resp.message,
        }),
    }
}

/// Drain queued invalidation events for `client`. Empty vec means no
/// events at this poll. F-ioring-lease-2 only ships the call site;
/// the persistent long-poll loop that consumes them lands in
/// F-ioring-lease-3.
pub async fn poll_invalidations(
    cluster: &ClusterClient,
    client: &DaemonClientId,
) -> Result<Vec<MgrInvalidation>, LeaseError> {
    let req = PollInvalidationsReq {
        client: client.as_wire().clone(),
    };
    let bytes = cluster
        .mgr_call_retry(MSG_POLL_INVALIDATIONS, rkyv_encode(&req), RETRY)
        .await
        .map_err(|e| LeaseError::Transport(e.to_string()))?;
    let resp: PollInvalidationsResp =
        rkyv_decode(&bytes).map_err(|e| LeaseError::Transport(format!("decode: {e}")))?;
    match resp.code {
        CODE_OK => Ok(resp.events),
        CODE_NOT_LEADER => Err(LeaseError::NotLeader),
        c => Err(LeaseError::Manager {
            code: c,
            message: resp.message,
        }),
    }
}

// ──────────────────── F-ioring-lease-4 ─────────────────────────────────
//
// Per-session "minimum valid version" map. The
// `session_invalidation_poll_loop` updates this on every per-ino
// invalidation event; the Read SQE arm consults it to decide whether
// to reload the cached extent map.

use std::collections::HashMap;

/// Per-inode minimum version a cached `OpenedExtents.lease_version`
/// must match to be considered fresh. An inode absent from the map
/// has never been invalidated → cache is always fresh (within the
/// lease TTL — see the heartbeat path for the orthogonal staleness
/// mode).
pub type InvalidationMap = HashMap<u64, u64>;

/// Update the invalidation map from a batch of incoming events.
/// Returns `true` iff any event was the overflow sentinel
/// (`MetaChanged { ino == 0 }`); the caller then wholesale-
/// invalidates the session's ring_fds + held_leases per plan §6.4.
///
/// Pure-fn: tested in `apply_invalidation_*` below.
pub fn apply_invalidation(
    events: &[autumn_rpc::manager_rpc::MgrInvalidation],
    inv: &mut InvalidationMap,
) -> bool {
    let mut saw_overflow = false;
    for ev in events {
        if ev.kind == autumn_rpc::manager_rpc::LEASE_INVAL_META_CHANGED && ev.ino == 0 {
            saw_overflow = true;
            continue;
        }
        // Take the max: out-of-order pushes shouldn't roll back the
        // valid-version floor.
        let slot = inv.entry(ev.ino).or_insert(0);
        if ev.version > *slot {
            *slot = ev.version;
        }
    }
    saw_overflow
}

/// True when the cached `(ino, lease_version)` has been overtaken by
/// a known invalidation. Pure-fn — the caller passes a snapshot of
/// the session's `InvalidationMap` taken under a brief borrow.
pub fn cache_is_stale(ino: u64, lease_version: u64, inv: &InvalidationMap) -> bool {
    match inv.get(&ino) {
        Some(min_valid) => lease_version < *min_valid,
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_rpc::manager_rpc::{
        LEASE_CLIENT_KIND_FUSE, LEASE_INVAL_LEASE_REVOKED, LEASE_INVAL_META_CHANGED,
        LEASE_INVAL_WRITER_CLOSED,
    };

    fn ev(ino: u64, version: u64, kind: u8) -> autumn_rpc::manager_rpc::MgrInvalidation {
        autumn_rpc::manager_rpc::MgrInvalidation { ino, version, kind }
    }

    #[test]
    fn apply_invalidation_records_writer_closed() {
        let mut m = InvalidationMap::new();
        let overflow = apply_invalidation(&[ev(7, 5, LEASE_INVAL_WRITER_CLOSED)], &mut m);
        assert!(!overflow);
        assert_eq!(m.get(&7).copied(), Some(5));
    }

    #[test]
    fn apply_invalidation_takes_max_across_events() {
        let mut m = InvalidationMap::new();
        apply_invalidation(
            &[
                ev(7, 3, LEASE_INVAL_LEASE_REVOKED),
                ev(7, 8, LEASE_INVAL_WRITER_CLOSED),
                ev(7, 5, LEASE_INVAL_WRITER_CLOSED), // out of order
            ],
            &mut m,
        );
        assert_eq!(m.get(&7).copied(), Some(8), "must NOT roll back the floor");
    }

    #[test]
    fn apply_invalidation_overflow_sentinel_returns_true() {
        let mut m = InvalidationMap::new();
        let overflow = apply_invalidation(
            &[
                ev(0, 0, LEASE_INVAL_META_CHANGED),
                ev(9, 4, LEASE_INVAL_WRITER_CLOSED),
            ],
            &mut m,
        );
        assert!(overflow, "ino=0 + MetaChanged means overflow");
        assert_eq!(m.get(&9).copied(), Some(4), "non-sentinel events still apply");
    }

    #[test]
    fn cache_is_stale_when_lease_version_below_floor() {
        let mut m = InvalidationMap::new();
        m.insert(7, 5);
        assert!(cache_is_stale(7, 3, &m));
        assert!(cache_is_stale(7, 4, &m));
        assert!(!cache_is_stale(7, 5, &m));
        assert!(!cache_is_stale(7, 6, &m));
        // No invalidation entry → always fresh.
        assert!(!cache_is_stale(8, 0, &m));
    }

    #[test]
    fn fresh_identity_has_iouring_kind() {
        let id = DaemonClientId::new("test-host");
        assert_eq!(id.as_wire().kind, LEASE_CLIENT_KIND_IORING);
        assert_eq!(id.as_wire().host, "test-host");
        assert_ne!(id.as_wire().uuid, [0u8; 16], "uuid must be random");
    }

    #[test]
    fn from_wire_preserves_arbitrary_kind() {
        // The from_wire constructor is the only path tests use to
        // simulate a fuse-daemon identity; assert it preserves kind.
        let wire = MgrClientId {
            kind: LEASE_CLIENT_KIND_FUSE,
            uuid: [42u8; 16],
            host: "fake".to_string(),
        };
        let id = DaemonClientId::from_wire(wire);
        assert_eq!(id.as_wire().kind, LEASE_CLIENT_KIND_FUSE);
        assert_eq!(id.as_wire().uuid, [42u8; 16]);
    }
}
