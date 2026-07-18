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
/// The `apply_invalidation` / `cache_is_stale` helpers operate on this
/// shape (a writer-XOR-readers lease keyed per inode).
#[derive(Clone, Debug)]
pub struct FuseLease {
    /// `LEASE_MODE_READ` or `LEASE_MODE_WRITE`, pinned at first Open.
    pub mode: u8,
    /// Refcount across this mount's `Open` calls for the same inode.
    /// `Release` decrements; the 1→0 transition fires `ReleaseLease`
    /// to the manager.
    pub refcount: u32,
    /// The lease's fencing epoch — `MgrInodeLeaseInfo.version` handed
    /// back at AcquireLease (the manager wire keeps the name `version`;
    /// every client-side cache + stamp uses `lease_epoch` uniformly:
    /// here, `OpenedExtents.lease_epoch`, `WriteLease.lease_epoch`).
    pub lease_epoch: u64,
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

    /// BUG-LEASE-6 (P2 #7, 2026-06-06) — sticky set of inos whose
    /// most recent `notify_inval_inode` kernel call FAILED. The
    /// poll-loop invalidator writes to this set on error; the
    /// Open/Read arms check it and force a fresh `get_inode`
    /// reload + retry `notify_inval_inode` for any sticky-failed
    /// ino, so a transient kernel-notification failure can't
    /// indefinitely strand readers on stale page-cache contents.
    /// On the retry succeeding the entry is removed.
    pub notify_inval_failed: Rc<RefCell<std::collections::HashSet<u64>>>,

    /// BUG-LEASE-6 (P2 #7) — clone of the per-mount kernel
    /// `Notifier::inval_inode` closure, kept here so the Open
    /// arm can retry the notify when `notify_inval_failed`
    /// contains the ino. `None` in tests + headless contexts
    /// (no live `fuser::Session`) — the Open arm short-circuits
    /// to "drop the cached InodeState" and skips the kernel
    /// retry. The closure type is the same `Rc<dyn Fn(u64)>`
    /// shape as `dispatch::InodeInvalidator`; we don't import
    /// the alias here to avoid a dispatch ↔ state circular dep.
    pub kernel_invalidator: RefCell<Option<Rc<dyn Fn(u64)>>>,

    /// F-DIRECT-MANY — when true, whole-extent reads (≥ 64 KiB) bypass the PS
    /// and read straight from an extent node (`get_many_direct`); otherwise the
    /// PS-proxied ZC path (`get_many_into`). Topology-dependent (needs the fuse
    /// host to reach EN data ports), so DEFAULT FALSE — the fuse binary flips it
    /// from `--direct-read`; the PyO3 binding leaves it false. Threaded into
    /// each `ReadPlan` at `prepare` time so the spawned `read::execute` task
    /// (which holds no `&FsState`) can pick the batch primitive.
    pub direct_read: bool,

    /// F-KEY-NS SD-3: the `{volume}/` byte prefix prepended to EVERY KV key
    /// before it reaches the (scoped) client. This mount connects
    /// `scoped(fs, tenant)`, so the client owns the `fs/{tenant}/` half and
    /// strips it back off returned range keys; `vol` owns the `{volume}/` half.
    /// Net wire key = `fs/{tenant}/{volume}/[type][fields]`, so a mount can only
    /// ever touch its own volume (scope locked by construction) and Layer-A gates
    /// the writes on the `fs` namespace. The bare `key::*` builders and every
    /// call site stay unchanged — the volume prefix lives entirely at the KV
    /// choke point below (`wire` / `kv_*`).
    vol: Vec<u8>,
}

/// F-KEY-NS SD-3: a volume segment is embedded verbatim into
/// `fs/{tenant}/{volume}/`, so an empty segment or one containing `/` would
/// forge a nested/aliased scope. Enforce the same `[a-z0-9._-]+` charset the
/// manager uses for namespace names (and the client uses for ns/tenant at
/// connect). The client validates ns+tenant inside `connect`; the volume is
/// fuse's to validate.
fn is_valid_volume(v: &str) -> bool {
    !v.is_empty()
        && v.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-'))
}

impl FsState {
    pub async fn new(manager_addr: &str, tenant: &str, volume: &str) -> Result<Self> {
        // The fuse binary keeps the env-derived hostname default (the daemon
        // has no CLI flag for it); the PyO3 `autumn.Fs` binding (F-FS-UNIFY
        // M2) passes an explicit host via `new_with_host` so no env read
        // leaks into the library path ([[feedback_no_env_in_rs]]).
        let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "fuse".to_string());
        Self::new_with_host(manager_addr, host, tenant, volume).await
    }

    /// Connect with an explicit daemon-identity host (no env read). The
    /// `host` seeds `DaemonClientId::new_fuse` — the per-mount/per-client
    /// lease identity the manager keys its lease registry on.
    ///
    /// F-KEY-NS SD-3: `tenant` + `volume` scope this mount. The client connects
    /// `scoped(fs, tenant)` (owns + validates the `fs/{tenant}/` half); `volume`
    /// is fuse's `{volume}/` half, validated here. Every KV key this mount
    /// writes/reads lands under `fs/{tenant}/{volume}/`.
    pub async fn new_with_host(
        manager_addr: &str,
        host: String,
        tenant: &str,
        volume: &str,
    ) -> Result<Self> {
        if !is_valid_volume(volume) {
            return Err(anyhow!(
                "invalid volume {volume:?}: must be non-empty and match [a-z0-9._-]+"
            ));
        }
        // F-KEY-NS SD-3: scoped connect — the client prepends `fs/{tenant}/` to
        // every key (and strips it off returned range keys), validating the
        // ns+tenant charset at connect. `vol` below prepends `{volume}/` on top
        // (see the field doc). The bare `key::*` builders stay relative to
        // `fs/{tenant}/{volume}/`.
        let client = ClusterClient::connect(manager_addr, "fs", tenant)
            .await
            .context("connect to manager")?;
        Ok(Self {
            client: Rc::new(client),
            vol: format!("{volume}/").into_bytes(),
            inodes: HashMap::new(),
            dirty_inodes: HashSet::new(),
            next_inode: ROOT_INO + 1,
            inode_batch_end: ROOT_INO + 1, // will trigger batch alloc on first use
            lookup_count: HashMap::new(),
            client_id: Rc::new(DaemonClientId::new_fuse(host)),
            held_leases: Rc::new(RefCell::new(HashMap::new())),
            invalidations: Rc::new(RefCell::new(InvalidationMap::new())),
            notify_inval_failed: Rc::new(RefCell::new(HashSet::new())),
            kernel_invalidator: RefCell::new(None),
            direct_read: false,
        })
    }

    // ── KV helpers ──────────────────────────────────────────────────────────

    /// F-KEY-NS SD-3: map a bare `key::*` key to the volume-relative wire key by
    /// prepending `{volume}/`. The scoped client then prepends `fs/{tenant}/`, so
    /// the on-wire key is `fs/{tenant}/{volume}/[type][fields]`. Returns an owned
    /// `Vec` (no borrow of `self` outlives the call), so `&mut self` KV helpers
    /// can build it then hand `&wk` to `self.client`.
    ///
    /// `pub(crate)` because the BATCH data path bypasses the `kv_*` helpers for
    /// performance — `read::fill_region`'s `get_many_into`/`get_many_direct` and
    /// `extent::flush_appends`'s `put_many_fenced` call the client directly, so
    /// they MUST wire their extent keys here too or the data would land at
    /// `fs/{tenant}/[0x03]…` (missing `{volume}/`) while the RMW/metadata paths
    /// use `fs/{tenant}/{volume}/…` — a cross-volume collision + intra-volume
    /// append-vs-overwrite key mismatch.
    pub(crate) fn wire(&self, k: &[u8]) -> Vec<u8> {
        [self.vol.as_slice(), k].concat()
    }

    /// F-KEY-NS SD-3: the canonicalized `fs/{tenant}/{volume}/` identity for
    /// this mount — the scoped client's `fs/{tenant}/` binding prefix plus this
    /// mount's `{volume}/`. Passed to the manager's `alloc_inodes` so the inode
    /// counter is keyed per-volume (each volume numbers from 2, disjoint from
    /// every other). Empty only under a Raw binding (no scoped prefix), where
    /// the manager falls back to the single global counter.
    pub fn volume_identity(&self) -> Vec<u8> {
        match self.client.binding().prefix() {
            Some(p) => [p, self.vol.as_slice()].concat(),
            None => Vec::new(),
        }
    }

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
        let wk = self.wire(k);
        match self
            .client
            .get(&wk)
            .await
            .map_err(|e| anyhow!("KV get: {e}"))?
        {
            Some(v) => Ok(v),
            None => Err(anyhow!("not found")),
        }
    }

    /// Like `kv_get` but distinguishes "key absent" (Ok(None)) from a
    /// hard RPC/routing/storage error (Err). Barrier-style callers
    /// (`clean_beyond_eof`) MUST NOT treat a transient failure as
    /// "already cleaned" (coco P1).
    pub async fn kv_get_opt(&mut self, k: &[u8]) -> Result<Option<Vec<u8>>> {
        let wk = self.wire(k);
        self.client.get(&wk).await.map_err(|e| anyhow!("KV get: {e}"))
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
        let wk = self.wire(k);
        self.client
            .put(&wk, v)
            .await
            .map_err(|e| anyhow!("KV put: {e}"))
    }

    /// BUG-LEASE-2 Phase 2: lease-fenced put for writes covered by a held
    /// WRITE lease (data extents + inode meta). Anonymous when no live
    /// write lease is held for `ino` (e.g. internal bookkeeping).
    pub async fn kv_put_fenced(
        &mut self,
        k: &[u8],
        v: &[u8],
        lease: autumn_client::WriteLease,
    ) -> Result<()> {
        let wk = self.wire(k);
        self.client
            .put_fenced(&wk, v, lease)
            .await
            .map_err(|e| anyhow!("KV put: {e}"))
    }

    /// BUG-LEASE-2 Phase 2: the fencing identity to stamp on writes for
    /// `ino` — the held WRITE lease's version, or ANON when this mount
    /// holds no live write lease (legacy paths, internal counters).
    /// A REVOKED entry still stamps its (now-stale) version: the PS floor
    /// then rejects the write with `Fenced`, which is exactly the
    /// storage-side half of the revoke protocol (the client-side half is
    /// dispatch's EIO fast-fail).
    pub fn write_lease_for(&self, ino: u64) -> autumn_client::WriteLease {
        match self.held_leases.borrow().get(&ino) {
            Some(l) if l.mode == autumn_rpc::manager_rpc::LEASE_MODE_WRITE && l.lease_epoch != 0 => {
                autumn_client::WriteLease {
                    inode_hint: ino,
                    lease_epoch: l.lease_epoch,
                }
            }
            _ => autumn_client::WriteLease::ANON,
        }
    }

    /// Delete a key from the KV store.
    pub async fn kv_delete(&mut self, k: &[u8]) -> Result<()> {
        let wk = self.wire(k);
        self.client
            .delete(&wk)
            .await
            .map_err(|e| anyhow!("KV delete: {e}"))
    }

    /// BUG-LEASE-2 Phase 2 (coco P1 #3): lease-fenced delete for
    /// truncate/unlink under a held WRITE lease — a revoked writer's
    /// late delete must not remove the new writer's extents.
    pub async fn kv_delete_fenced(
        &mut self,
        k: &[u8],
        lease: autumn_client::WriteLease,
    ) -> Result<()> {
        let wk = self.wire(k);
        self.client
            .delete_fenced(&wk, lease)
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
        // F-KEY-NS SD-3: prepend `{volume}/` to both prefix and start. Every fuse
        // caller passes a non-empty `start` (== prefix, or an in-scope resume
        // key), so `wire(start)` correctly seeds the cursor at this volume's
        // lower bound. The scoped client then prepends `fs/{tenant}/`, clamps at
        // the tenant boundary, and strips `fs/{tenant}/` off returned keys — so
        // here each key still carries the `{volume}/` prefix, which we strip back
        // off to hand callers the bare `[type][fields]` key they parse.
        let wprefix = self.wire(prefix);
        let wstart = self.wire(start);
        let r = self
            .client
            .range(&wprefix, &wstart, limit)
            .await
            .map_err(|e| anyhow!("KV range: {e}"))?;
        let vlen = self.vol.len();
        Ok(r
            .entries
            .into_iter()
            .map(|e| {
                if e.key.starts_with(self.vol.as_slice()) {
                    e.key[vlen..].to_vec()
                } else {
                    // Should not happen (PS bounds the scan by the wire prefix,
                    // which starts with `{volume}/`); pass through defensively.
                    e.key
                }
            })
            .collect())
    }

    /// Check if a key exists (uses Head RPC).
    pub async fn kv_exists(&mut self, k: &[u8]) -> Result<bool> {
        let wk = self.wire(k);
        let meta = self
            .client
            .head(&wk)
            .await
            .map_err(|e| anyhow!("KV head: {e}"))?;
        Ok(meta.found)
    }
}
