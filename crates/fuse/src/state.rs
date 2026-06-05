//! Filesystem state owned by the compio thread.
//!
//! Contains ClusterClient, inode cache, dirty tracking, and KV helper methods.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use anyhow::{anyhow, Context, Result};

use autumn_client::lease::{DaemonClientId, InvalidationMap};
use autumn_client::ClusterClient;

use crate::schema::{InodeState, ROOT_INO};

/// F-fuse-lease-1: per-inode lease bookkeeping on the fuse mount side.
/// Mirrors `autumn-ioring`'s `SessionLease` shape so the same
/// `apply_invalidation` / `cache_is_stale` helpers work both sides.
#[derive(Clone, Debug)]
pub struct FuseLease {
    /// `LEASE_MODE_READ` or `LEASE_MODE_WRITE`, pinned at first Open.
    pub mode: u8,
    /// Refcount across this mount's `Open` calls for the same inode.
    /// `Release` decrements; the 1→0 transition fires `ReleaseLease`
    /// to the manager.
    pub refcount: u32,
    /// Server-side version handed back at AcquireLease. Used by the
    /// (future, F-fuse-lease-4-equivalent) cache invalidation path.
    pub version: u64,
    /// R2-P0 #2/#3 (2026-06-06) — sticky flag set when the manager's
    /// invalidation poll observes `LEASE_INVAL_LEASE_REVOKED` for
    /// this ino. The entry is intentionally KEPT in the map (not
    /// removed) so:
    ///   - `Write` can fast-fail with EIO on a revoked lease (the
    ///     stale fd's bytes must not reach the new writer's view —
    ///     this is the client-side half of BUG-LEASE-2's fencing).
    ///   - `Release` can recognise that a flush is required even
    ///     when the kernel passed `flush=false` AND `refcount > 1`,
    ///     because the lease is gone server-side and the dirty
    ///     buffer would otherwise be silently dropped on the next
    ///     refcount→0 (no entry, no `release_now_pred`, no flush).
    /// Cleared on the next successful `AcquireLease` for the same
    /// ino (Open path drops the revoked entry and re-acquires).
    pub revoked: bool,
}

/// Central filesystem state, lives on the compio thread (single-threaded, no locks).
pub struct FsState {
    /// `Rc` so the spawned `read::execute` task can hold a clone and call
    /// `get_many_into` without an `&FsState` reference (F244-B).
    pub client: Rc<ClusterClient>,
    pub inodes: HashMap<u64, InodeState>,
    pub dirty_inodes: HashSet<u64>,
    pub next_inode: u64,
    pub inode_batch_end: u64,
    /// FUSE lookup refcounts (separate from open_count).
    pub lookup_count: HashMap<u64, u64>,

    // ── F-fuse-lease-1 ────────────────────────────────────────────────
    /// Per-mount daemon identity (kind = `LEASE_CLIENT_KIND_FUSE`,
    /// fresh UUID at mount). Reused for every lease RPC so the
    /// manager's lease-registry state stays stable for this mount.
    /// `Rc` so the per-mount heartbeat / invalidation poll tasks can
    /// hold clones without borrowing `FsState`.
    pub client_id: Rc<DaemonClientId>,
    /// Per-inode lease refcount + mode + version. Open allocates;
    /// Release decrements; 1→0 fires `ReleaseLease`.
    pub held_leases: Rc<RefCell<HashMap<u64, FuseLease>>>,
    /// Per-inode minimum-valid-version. Updated by the per-mount
    /// `session_invalidation_poll_loop` from the manager's
    /// `WriterClosed` / `LeaseRevoked` push events; the read path
    /// will use `cache_is_stale` against it for close-to-open
    /// coherence (full path eviction wires in F-fuse-lease-2's
    /// `notify_inval_inode` work).
    pub invalidations: Rc<RefCell<InvalidationMap>>,
}

impl FsState {
    pub async fn new(manager_addr: &str) -> Result<Self> {
        let client = ClusterClient::connect(manager_addr)
            .await
            .context("connect to manager")?;
        let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "fuse".to_string());
        Ok(Self {
            client: Rc::new(client),
            inodes: HashMap::new(),
            dirty_inodes: HashSet::new(),
            next_inode: ROOT_INO + 1,
            inode_batch_end: ROOT_INO + 1, // will trigger batch alloc on first use
            lookup_count: HashMap::new(),
            client_id: Rc::new(DaemonClientId::new_fuse(host)),
            held_leases: Rc::new(RefCell::new(HashMap::new())),
            invalidations: Rc::new(RefCell::new(InvalidationMap::new())),
        })
    }

    // ── KV helpers ──────────────────────────────────────────────────────────

    /// Get a value from the KV store by key.
    pub async fn kv_get(&mut self, k: &[u8]) -> Result<Vec<u8>> {
        // 2026-06-04 fix — was hand-assembling GetReq + `ps_call`, which
        // BYPASSES the SDK's `call_ps_for_key` retry+region-refresh loop.
        // On a split, the PS rejects the stale region_epoch with
        // FailedPrecondition; without retry it bubbles up as EIO to FUSE
        // and the routing cache stays stale for the rest of the process's
        // lifetime — `ls` / `cat` / `cp` all fail until autumn-fuse is
        // restarted. `client.get` is the same RPC underneath but goes
        // through the standard retry loop (MAX_PS_REFRESHES=10).
        // Same fix applied to kv_get_range / kv_put / kv_delete /
        // kv_range_keys / kv_exists below.
        match self
            .client
            .get(k)
            .await
            .map_err(|e| anyhow!("KV get: {e}"))?
        {
            Some(v) => Ok(v),
            None => Err(anyhow!("not found")),
        }
    }

    /// Get a sub-range of a value from the KV store.
    pub async fn kv_get_range(&mut self, k: &[u8], offset: u32, length: u32) -> Result<Vec<u8>> {
        match self
            .client
            .get_range(k, offset, length)
            .await
            .map_err(|e| anyhow!("KV get_range: {e}"))?
        {
            Some(v) => Ok(v),
            None => Err(anyhow!("not found")),
        }
    }

    /// Put a key-value pair into the KV store.
    ///
    /// F178: every Put is durable (no `must_sync` flag). Pre-F178 there
    /// was a `kv_put` (must_sync=false) and `kv_put_sync` (must_sync=
    /// true) split; post-F178 they collapse to one method because the
    /// extent-node fsync coalescer makes every append durable
    /// regardless. The `kv_put_sync` alias is retained as a no-op
    /// pass-through for callers that explicitly want to read as
    /// "durable Put".
    pub async fn kv_put(&mut self, k: &[u8], v: &[u8]) -> Result<()> {
        self.client
            .put(k, v)
            .await
            .map_err(|e| anyhow!("KV put: {e}"))
    }

    /// F178: alias for `kv_put`. See `kv_put` doc — no semantic difference
    /// post-F178 since every write is durable.
    pub async fn kv_put_sync(&mut self, k: &[u8], v: &[u8]) -> Result<()> {
        self.kv_put(k, v).await
    }

    /// Delete a key from the KV store.
    pub async fn kv_delete(&mut self, k: &[u8]) -> Result<()> {
        self.client
            .delete(k)
            .await
            .map_err(|e| anyhow!("KV delete: {e}"))
    }

    /// Range scan with prefix and optional start key.
    ///
    /// Returns keys only — PS `handle_range` does not populate values on the wire.
    /// Callers that need values must issue a separate `kv_get` per key.
    pub async fn kv_range_keys(
        &mut self,
        prefix: &[u8],
        start: &[u8],
        limit: u32,
    ) -> Result<Vec<Vec<u8>>> {
        let r = self
            .client
            .range(prefix, start, limit)
            .await
            .map_err(|e| anyhow!("KV range: {e}"))?;
        Ok(r.entries.into_iter().map(|e| e.key).collect())
    }

    /// Check if a key exists (uses Head RPC).
    pub async fn kv_exists(&mut self, k: &[u8]) -> Result<bool> {
        let meta = self
            .client
            .head(k)
            .await
            .map_err(|e| anyhow!("KV head: {e}"))?;
        Ok(meta.found)
    }
}
