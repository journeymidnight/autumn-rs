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

/// Reclamation a caller hands to a background reclaimer instead of doing it
/// inline (`FsState::reclaim_later`). Each one names durable state that
/// already says what to reclaim — a tombstone, a terminal upload record — so
/// a hand-off that is lost only delays the work until the next sweep.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Reclaim {
    /// A tombstoned unreachable inode (`extent::reclaim_unreachable`).
    Inode(u64),
    /// An aborted or completed multipart upload (`multipart::cleanup`).
    Upload(u64),
}

/// Per-inode lease bookkeeping on the fuse mount side.
/// The `apply_invalidation` / `cache_is_stale` helpers operate on this
/// shape (a writer-XOR-readers lease keyed per inode).
///
/// **Refcounts are per ROLE, because one process legitimately holds a write
/// fd and a read fd on the same file at the same time.** ffmpeg's MP4
/// faststart does exactly that: it keeps the write handle open and reopens the
/// file `O_RDONLY` to move the moov atom. With a single `mode` slot the second
/// open was refused with EBUSY, so every ComfyUI SaveVideo failed. The manager
/// always allowed it — `acquire(READ)` inserts a reader regardless of who holds
/// the writer, and only a DIFFERENT client's writer conflicts — so the refusal
/// was purely this mount's bookkeeping.
#[derive(Clone, Debug)]
pub struct FuseLease {
    /// Open fds on this mount in each role: `O_WRONLY`/`O_RDWR` are writers,
    /// `O_RDONLY` readers. `Release` decrements the role the fd was opened
    /// with; the manager is told only when a ROLE empties (writer→reader
    /// downgrade) or the inode goes fully unopened (release).
    pub writer_refs: u32,
    pub reader_refs: u32,
    /// The strongest lease this mount still HOLDS AT THE MANAGER. It is not a
    /// restatement of the refcounts: it stays `LEASE_MODE_WRITE` after the last
    /// write fd closes if the write lease was deliberately kept (a failed
    /// last-writer flush), and it drops to `LEASE_MODE_READ` the moment the
    /// writer slot is actually handed back. `write_lease_for` stamps the
    /// fencing epoch off THIS field, so it must never claim a lease the manager
    /// has taken away.
    pub mode: u8,
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
    ///     when the kernel passed `flush=false` AND other fds are still
    ///     open, because the lease is gone server-side and the dirty
    ///     buffer would otherwise be silently dropped on the last
    ///     release (no entry, no `release_now_pred`, no flush).
    /// Cleared on the next successful `AcquireLease` for the same
    /// ino (Open path drops the revoked entry and re-acquires).
    pub revoked: bool,
}

impl FuseLease {
    /// Open fds in both roles. Zero means the inode is fully closed on this
    /// mount and the manager-side lease can go.
    pub fn total_refs(&self) -> u32 {
        self.writer_refs.saturating_add(self.reader_refs)
    }

    /// Count one more open fd in `role` (`LEASE_MODE_WRITE` / `LEASE_MODE_READ`).
    pub fn add_ref(&mut self, role: u8) {
        if role == autumn_rpc::manager_rpc::LEASE_MODE_WRITE {
            self.writer_refs = self.writer_refs.saturating_add(1);
        } else {
            self.reader_refs = self.reader_refs.saturating_add(1);
        }
    }

    /// Drop one open fd in `role`.
    pub fn drop_ref(&mut self, role: u8) {
        if role == autumn_rpc::manager_rpc::LEASE_MODE_WRITE {
            self.writer_refs = self.writer_refs.saturating_sub(1);
        } else {
            self.reader_refs = self.reader_refs.saturating_sub(1);
        }
    }
}

/// Central filesystem state, lives on the compio thread (single-threaded, no locks).
pub struct FsState {
    /// `Rc` so the spawned `read::execute` task can hold a clone and call
    /// `get_many_into` without an `&FsState` reference.
    pub client: Rc<ClusterClient>,
    pub inodes: HashMap<u64, InodeState>,
    pub dirty_inodes: HashSet<u64>,
    pub next_inode: u64,
    pub inode_batch_end: u64,
    /// FUSE lookup refcounts (separate from open_count).
    pub lookup_count: HashMap<u64, u64>,

    // ── inode leases ──────────────────────────────────────────────────
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
    /// coherence (full path eviction wires in the
    /// `notify_inval_inode` work).
    pub invalidations: Rc<RefCell<InvalidationMap>>,

    /// BUG-LEASE-6 (P2 #7, 2026-06-06) — sticky set of inos whose
    /// most recent `notify_inval_inode` kernel call FAILED. The
    /// mount writes each notify's outcome here (a failure inserts,
    /// a success removes); the Open arm checks it and forces a
    /// fresh `get_inode` reload + queues `notify_inval_inode` again
    /// for any sticky-failed ino, so a transient kernel-notification
    /// failure can't indefinitely strand readers on stale page-cache
    /// contents. Outcomes arrive after the notify runs, so the set
    /// lags the kernel by the queue: an Open racing a failure that
    /// is not recorded yet skips the retry, and the next Open makes
    /// it.
    pub notify_inval_failed: Rc<RefCell<std::collections::HashSet<u64>>>,

    /// BUG-LEASE-6 (P2 #7) — clone of the per-mount kernel
    /// invalidator (queues `Notifier::inval_inode` on the mount's
    /// invalidation thread), kept here so the Open arm can retry
    /// the notify when `notify_inval_failed` contains the ino.
    /// `None` in tests + headless contexts
    /// (no live `fuser::Session`) — the Open arm short-circuits
    /// to "drop the cached InodeState" and skips the kernel
    /// retry. The closure type is the same `Rc<dyn Fn(u64)>`
    /// shape as `dispatch::InodeInvalidator`; we don't import
    /// the alias here to avoid a dispatch ↔ state circular dep.
    pub kernel_invalidator: RefCell<Option<Rc<dyn Fn(u64)>>>,

    /// when true, whole-extent reads (≥ 64 KiB) bypass the PS
    /// and read straight from an extent node (`get_many_direct`); otherwise the
    /// PS-proxied bulk path (`get_many_into`). Topology-dependent (needs the fuse
    /// host to reach EN data ports), so DEFAULT FALSE — the fuse binary flips it
    /// from `--direct-read`; the PyO3 binding leaves it false. Threaded into
    /// each `ReadPlan` at `prepare` time so the spawned `read::execute` task
    /// (which holds no `&FsState`) can pick the batch primitive.
    pub direct_read: bool,

    /// Whether a buffer flush may run as a SPAWNED task while the caller goes
    /// back to serving requests (see `write::write`). Off by default and turned
    /// on ONLY by the fuse mount, because pipelining is safe exactly where the
    /// drain discipline exists: the mount funnels every non-write operation
    /// through `dispatch::handle_request`, which drains first. The PyO3
    /// `autumn.Fs` worker calls the core ops directly and never goes through
    /// that dispatcher, so with pipelining on, a `read` right after a `write`
    /// would miss the in-flight flush's extents and return zeros for bytes the
    /// write acknowledged.
    pub pipelined_writes: bool,

    /// Segment-map pages fetched for reads. Pages are immutable, so a cached
    /// page is never stale; see `segment::load_range`.
    pub segment_pages: crate::segment::PageCache,

    /// The filesystem's declared stripe geometry, read once per session when
    /// the first data object is written.
    pub stripe_geom: Option<crate::schema::StripeLayout>,

    /// Unreachable inodes this session still had open when their last name
    /// went: their data is reclaimed at the last close, not at the unlink.
    pub unlinked_open: HashSet<u64>,

    /// This session's publishing-session inode (`publish::session_lease`).
    pub session: Option<u64>,

    /// Segmented files this session changed (each marked `segg/` first);
    /// reclaimed at the last close (`segment::reclaim_live`).
    pub segment_garbage: HashSet<u64>,

    /// Where the next `segment::sweep_garbage` page starts, so markers held
    /// elsewhere cannot keep the sweep from ever reaching the rest.
    pub garbage_sweep_from: Option<Vec<u8>>,

    /// Where data reclamation goes instead of running inline, for a caller
    /// that shares this state between requests behind one lock (the S3
    /// gateway): an abort or a delete then records its intent and returns,
    /// and the reclaimer — its own state, its own client identity — deletes
    /// the bytes without holding anyone's lock. `None` reclaims inline.
    pub reclaim_later: Option<Box<dyn Fn(Reclaim)>>,
}

impl FsState {
    pub async fn new(manager_addr: &str) -> Result<Self> {
        // The fuse binary keeps the env-derived hostname default (the daemon
        // has no CLI flag for it); the PyO3 `autumn.Fs` binding (M2)
        // passes an explicit host via `new_with_host` so no env read
        // leaks into the library path ([[feedback_no_env_in_rs]]).
        let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "fuse".to_string());
        Self::new_with_host(manager_addr, host).await
    }

    /// `new` with an authz credential — same HOSTNAME-derived
    /// daemon identity, but connects via `connect_with_credential`. See
    /// [`Self::new_with_host_credential`].
    pub async fn new_with_credential(
        manager_addr: &str,
        principal: &str,
        credential: Vec<u8>,
    ) -> Result<Self> {
        let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "fuse".to_string());
        Self::new_with_host_credential(manager_addr, host, principal, credential).await
    }

    /// Connect with an explicit daemon-identity host (no env read). The
    /// `host` seeds `DaemonClientId::new_fuse` — the per-mount/per-client
    /// lease identity the manager keys its lease registry on.
    ///
    /// this mount is scoped to the WHOLE `fs/` namespace
    /// (Option 3 dropped the tenant segment — fuse is one global tree; multi-tree
    /// isolation is by distinct namespaces, §8.9). The client `connect(mgr, "fs")`
    /// prepends `fs/` to every key (and strips it off returned range keys).
    pub async fn new_with_host(
        manager_addr: &str,
        host: String,
    ) -> Result<Self> {
        let client = ClusterClient::connect(manager_addr, "fs")
            .await
            .context("connect to manager")?;
        Ok(Self::from_client(client, host))
    }

    /// connect with an authz credential. Used when the deploy
    /// protects the `fs/` namespace — the client presents `principal` (credential
    /// owner from the credential file's name line) + `credential`, and
    /// `connect_with_credential` FAILS FAST at connect if that credential does
    /// not cover `fs/` (or the manager rejects it). When authz is off on the
    /// manager the credential is harmlessly ignored, so passing one is safe.
    pub async fn new_with_host_credential(
        manager_addr: &str,
        host: String,
        principal: &str,
        credential: Vec<u8>,
    ) -> Result<Self> {
        let client = ClusterClient::connect_with_credential(
            manager_addr,
            "fs",
            principal.to_string(),
            credential,
        )
        .await
        .context("connect to manager (with credential)")?;
        Ok(Self::from_client(client, host))
    }

    /// Wrap an already-connected client (scoped to `fs`). `host` seeds the
    /// lease identity. For tools that connect themselves, like `autumnfs`.
    pub fn from_client(client: ClusterClient, host: String) -> Self {
        Self {
            client: Rc::new(client),
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
            pipelined_writes: false,
            segment_pages: crate::segment::PageCache::new(),
            stripe_geom: None,
            unlinked_open: HashSet::new(),
            session: None,
            segment_garbage: HashSet::new(),
            garbage_sweep_from: None,
            reclaim_later: None,
        }
    }

    /// Hand `r` to the background reclaimer, if this state has one. `false`
    /// means the caller reclaims inline.
    pub fn defer_reclaim(&self, r: Reclaim) -> bool {
        match &self.reclaim_later {
            Some(f) => {
                f(r);
                true
            }
            None => false,
        }
    }

    // ── KV helpers ──────────────────────────────────────────────────────────
    //
    // Keys are the bare `key::*` builders (relative to `fs/{tenant}/`). The
    // scoped client (`scoped(fs, tenant)`) prepends `fs/{tenant}/` on the wire
    // and strips it back off range results, so these helpers hand it the key
    // straight through — the net wire key is `fs/{tenant}/[type][fields]`.

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

    /// Like `kv_get` but distinguishes "key absent" (Ok(None)) from a
    /// hard RPC/routing/storage error (Err). Barrier-style callers
    /// (`clean_beyond_eof`) MUST NOT treat a transient failure as
    /// "already cleaned" (coco P1).
    pub async fn kv_get_opt(&mut self, k: &[u8]) -> Result<Option<Vec<u8>>> {
        self.client.get(k).await.map_err(|e| anyhow!("KV get: {e}"))
    }


    /// Put a key-value pair into the KV store.
    ///
    /// every Put is durable (no `must_sync` flag). Previously there
    /// was a `kv_put` (must_sync=false) and `kv_put_sync` (must_sync=
    /// true) split; they now collapse to one method because the
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

    /// BUG-LEASE-2 Phase 2: lease-fenced put for writes covered by a held
    /// WRITE lease (data extents + inode meta). Anonymous when no live
    /// write lease is held for `ino` (e.g. internal bookkeeping).
    pub async fn kv_put_fenced(
        &mut self,
        k: &[u8],
        v: &[u8],
        lease: autumn_client::WriteLease,
    ) -> Result<()> {
        self.client
            .put_fenced(k, v, lease)
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
    /// Keyed on `mode`, NOT on `writer_refs` — the two answer different
    /// questions and only `mode` answers this one. `writer_refs > 0` asks "does
    /// this mount have an open write fd", which is what `check_write_allowed`
    /// needs; the fencing stamp asks "does this mount still hold the WRITE
    /// LEASE at the manager", and those diverge exactly when it matters. When a
    /// last-writer flush FAILS, the fd is gone (`writer_refs == 0`) but the
    /// write lease is deliberately kept (see the Release arm), and the leftover
    /// dirty meta is retried later: keyed on `writer_refs` that retry would
    /// stamp ANON, which tells the PS to skip fencing — an unfenced write of a
    /// stale size, from a mount whose exclusivity nothing is checking.
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
        self.client
            .delete(k)
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
        self.client
            .delete_fenced(k, lease)
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
        // The scoped client prepends `fs/{tenant}/` to prefix+start, clamps the
        // scan at the tenant boundary, and strips `fs/{tenant}/` back off returned
        // keys — so callers get the bare `[type][fields]` key they parse.
        let r = self
            .client
            .range(prefix, start, limit)
            .await
            .map_err(|e| anyhow!("KV range: {e}"))?;
        Ok(r.entries.into_iter().map(|e| e.key).collect())
    }

    /// `kv_range_keys` plus whether the scan may hold more keys after this
    /// page. Use this rather than `keys.len() == limit` to decide whether to
    /// continue: the client deduplicates keys across partitions after the
    /// limit is reached, so a page can come back short while more remain.
    pub async fn kv_range_page(
        &mut self,
        prefix: &[u8],
        start: &[u8],
        limit: u32,
    ) -> Result<(Vec<Vec<u8>>, bool)> {
        let r = self
            .client
            .range(prefix, start, limit)
            .await
            .map_err(|e| anyhow!("KV range: {e}"))?;
        Ok((r.entries.into_iter().map(|e| e.key).collect(), r.has_more))
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
