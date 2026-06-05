//! F-ioring-lease-2 — daemon-side client helpers for the four inode
//! lease RPCs (`MSG_ACQUIRE_LEASE` / `MSG_RELEASE_LEASE` /
//! `MSG_HEARTBEAT_LEASE` / `MSG_POLL_INVALIDATIONS`).
//!
//! Wraps the rkyv encode/decode + manager round-trip into typed methods
//! so the daemon's Open/Close paths stay small. Also defines the
//! per-process `DaemonClientId` (one UUID per daemon runtime; reused
//! for every RPC so the manager's lease state is stable across
//! reconnects within the runtime's lifetime).
//!
//! Layering: this module reaches the manager via
//! `ClusterClient::mgr_call_retry` — the same auto-rotate /
//! reconnect path the SDK uses for `MSG_GET_REGIONS`. Manager-side
//! errors surface as `LeaseError::Conflict` (someone else holds the
//! writer slot), `LeaseError::NotLeader` (transient), or
//! `LeaseError::Manager` (other).

use anyhow::Result;
use autumn_client::ClusterClient;
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, AcquireLeaseReq, AcquireLeaseResp, HeartbeatLeaseReq,
    HeartbeatLeaseResp, MgrClientId, MgrInodeLeaseInfo, MgrInvalidation, PollInvalidationsReq,
    PollInvalidationsResp, ReleaseLeaseReq, ReleaseLeaseResp, CODE_NOT_FOUND, CODE_NOT_LEADER,
    CODE_OK, CODE_PRECONDITION, LEASE_CLIENT_KIND_IORING, LEASE_MODE_READ, LEASE_MODE_WRITE,
    MSG_ACQUIRE_LEASE, MSG_HEARTBEAT_LEASE, MSG_POLL_INVALIDATIONS, MSG_RELEASE_LEASE,
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
        DaemonClientId {
            inner: MgrClientId {
                kind: LEASE_CLIENT_KIND_IORING,
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
    debug_assert!(
        mode == LEASE_MODE_READ || mode == LEASE_MODE_WRITE,
        "lease mode must be READ or WRITE"
    );
    let req = AcquireLeaseReq {
        client: client.as_wire().clone(),
        ino,
        mode,
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
        CODE_NOT_LEADER => Err(LeaseError::NotLeader),
        c => Err(LeaseError::Manager {
            code: c,
            message: resp.message,
        }),
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

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_rpc::manager_rpc::LEASE_CLIENT_KIND_FUSE;

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
