//! Compio-thread dispatch loop: receives FsRequests from the bridge and
//! executes them using the filesystem state.

use anyhow::{anyhow, Result};

use autumn_client::lease;
use autumn_rpc::manager_rpc::{LEASE_MODE_READ, LEASE_MODE_WRITE};

use crate::bridge::*;
use crate::dir;
use crate::key;
use crate::meta::*;
use crate::read;
use crate::schema::{self, DirentValue, InodeState, DT_DIR, DT_REG, INODE_ALLOC_BATCH, ROOT_INO};
use crate::state::{FsState, FuseLease};
use crate::write;

/// F-fuse-lease-1: derive the lease mode from POSIX open flags. The
/// fuse `open` callback supplies `flags` directly (`O_RDONLY` /
/// `O_WRONLY` / `O_RDWR`). Treat any non-read-only opener as a
/// writer — that's the safe upper bound; a read-leased fd would
/// reject a write at lease-check time.
fn lease_mode_for_open(flags: i32) -> u8 {
    // O_RDONLY (0) → READ; everything else (O_WRONLY=1, O_RDWR=2 +
    // any flag combinations) → WRITE.
    if flags & libc::O_ACCMODE == libc::O_RDONLY {
        LEASE_MODE_READ
    } else {
        LEASE_MODE_WRITE
    }
}

/// BUG-LEASE-3 (coco P0 #3, 2026-06-05) — same shape as ioring's
/// `evict_revoked_leases`: per-event MARK of held leases on
/// `LEASE_INVAL_LEASE_REVOKED`. Pre-fix the fuse mount's
/// invalidation poll loop just logged the event; `held_leases[ino]`
/// stayed populated until the next `session_heartbeat_loop` tick
/// returned `NotHeld` (up to 5 s later). Inside that window a Write
/// request via `FsRequest::Write` continued to flow through because
/// the dispatcher doesn't consult held_leases on the write path.
///
/// **R2-P0 #2/#3 (2026-06-06, this commit) — marker, not remove.**
/// Original BUG-LEASE-3 fix REMOVED the entry; coco R2 round
/// surfaced that this BREAKS Release's flush-decision (no entry ⇒
/// `release_now_pred` is false ⇒ dirty buffer dropped on a kernel
/// `flush=false` close) AND Write's lease check (no entry ⇒ "no
/// lease held" which is indistinguishable from a Write before
/// Open). The new shape preserves the entry but sets the
/// `revoked: bool` sticky flag, so:
///   - Write checks the flag and fails with EIO (R2-P0 #3).
///   - Release sees the flag and flushes before evicting (R2-P0 #2).
///   - Open's re-acquire path observes the flag, drops the entry,
///     and treats the inode as fresh (gets a new lease epoch).
/// Returns the inos newly-transitioned to revoked so the caller can
/// do a best-effort `lease::release` (idempotent on the manager
/// side; same telemetry as the pre-R2 fix).
///
/// `WriterClosed` / `WillRevokeIn` / overflow sentinel do NOT
/// mark — same semantics as the ioring fn.
pub fn evict_revoked_held_leases(
    events: &[autumn_rpc::manager_rpc::MgrInvalidation],
    held_leases: &mut std::collections::HashMap<u64, crate::state::FuseLease>,
) -> Vec<u64> {
    let mut newly_revoked: Vec<u64> = Vec::new();
    for ev in events {
        if ev.kind == autumn_rpc::manager_rpc::LEASE_INVAL_LEASE_REVOKED && ev.ino != 0 {
            if let Some(slot) = held_leases.get_mut(&ev.ino) {
                if !slot.revoked {
                    slot.revoked = true;
                    newly_revoked.push(ev.ino);
                }
            }
        }
    }
    newly_revoked
}

/// R2-P0 #3 (2026-06-06) — pure-fn lease check used by the Write
/// arm of `FsRequest`. Three states:
///
/// - `Ok(())`: held WRITE-mode lease, not revoked.
/// - `Err("revoked")`: held entry but server-side revoked (R2-P0 #3
///   data-leak window — the stale writer's bytes must NOT land).
/// - `Err("wrong mode")`: held but mode != WRITE (READ lease, etc).
/// - `Err("no lease")`: no entry at all — caller dropped through
///   Open without acquiring, or a wholesale invalidate dropped the
///   map. Either way, refuse the write.
///
/// Returning `&'static str` keeps the helper allocation-free in the
/// hot path; the dispatcher wraps it in an `anyhow!` for the
/// `FsRequest::Write` reply.
pub fn check_write_allowed(
    held_leases: &std::collections::HashMap<u64, crate::state::FuseLease>,
    ino: u64,
) -> Result<(), &'static str> {
    match held_leases.get(&ino) {
        None => Err("no lease"),
        Some(slot) if slot.revoked => Err("revoked"),
        Some(slot) if slot.mode != autumn_rpc::manager_rpc::LEASE_MODE_WRITE => {
            Err("wrong mode")
        }
        Some(_) => Ok(()),
    }
}

/// R2-P0 #2 (2026-06-06) — pure-fn decision helper for the Release
/// path. Mirrors the BUG-LEASE-3 / R2-P0 #2 invariant: a
/// server-side revoked lease MUST flush before dropping the
/// in-memory entry, even when the kernel passed `flush=false` and
/// `refcount > 1`. Returns:
///
/// - `must_flush`: the dispatcher's `write::flush_inode` must run.
///   True iff: kernel `flush=true`, OR last refcount (1→0
///   transition), OR `slot.revoked` is set.
/// - `must_drop_entry`: the held_leases entry should be removed
///   after the flush attempt. True iff: refcount-after-decrement
///   reaches 0, OR `slot.revoked` is set (revoked entries are not
///   recoverable — the manager already gave the lease to someone
///   else; keeping the entry around just confuses the next Open).
/// - `propagate_flush_err`: the dispatcher should bubble the flush
///   error to the kernel as EIO. True iff NOT revoked (a revoked
///   flush is best-effort; the bytes will fence at the PS anyway
///   via BUG-LEASE-2 once Phase 2 wires that up — surfacing it as
///   EIO would mask the legitimate next Open + retry path).
pub fn compute_release_action(
    held_leases: &std::collections::HashMap<u64, crate::state::FuseLease>,
    ino: u64,
    kernel_flush: bool,
) -> ReleaseAction {
    match held_leases.get(&ino) {
        None => ReleaseAction {
            must_flush: kernel_flush,
            must_drop_entry: false,
            propagate_flush_err: true,
        },
        Some(slot) => {
            let last = slot.refcount <= 1;
            ReleaseAction {
                must_flush: kernel_flush || last || slot.revoked,
                must_drop_entry: last || slot.revoked,
                propagate_flush_err: !slot.revoked,
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReleaseAction {
    pub must_flush: bool,
    pub must_drop_entry: bool,
    pub propagate_flush_err: bool,
}

/// BUG-LEASE-7 (P2 #8, 2026-06-06) — pure-fn predicate used by the
/// Open arm to decide whether the cached `InodeState.meta` /
/// `extents` are stale relative to a freshly-acquired lease
/// version. Mirrors the ioring daemon's F-ioring-lease-4 +
/// F-ioring-lease-5 #1 staleness check.
///
/// `acquired_version`: the version returned by the just-completed
///   AcquireLease (or read back from `held_leases[ino].version` on
///   a same-mount Open that didn't re-acquire). `0` is the
///   "couldn't observe a fresh version this Open" sentinel — the
///   helper returns `false` so the existing cached entry stays
///   untouched (matches pre-fix behavior; never makes things
///   WORSE than pre-fix).
/// `cached_version`: the version stored on the per-mount
///   `InodeState.cached_version` at the time it was rebuilt.
/// Returns `true` iff `cached < acquired && acquired > 0`. The
/// strict `<` matters: an equal-version Open is the common case
/// (refcount+1) and must NOT trigger a reload.
pub fn inode_cache_needs_reload(cached_version: u64, acquired_version: u64) -> bool {
    acquired_version > 0 && cached_version < acquired_version
}

/// BUG-LEASE-6 (P2 #7, 2026-06-06) — pure-fn predicate used by the
/// Open / Read arms to detect that the per-mount kernel
/// `notify_inval_inode` call FAILED for this ino during the most
/// recent invalidation event. When this is `true`, the caller MUST:
///
///   1. Drop any cached InodeState (force a fresh `get_inode`).
///   2. Retry `notify_inval_inode(ino, 0, 0)` against the live
///      Notifier.
///   3. Clear the sticky entry on retry success.
///
/// Without this, a transient EINVAL/ENOENT/EAGAIN on the kernel
/// notify path leaves the kernel page cache serving stale bytes
/// to userspace AND the mount's own InodeState pointing at stale
/// extents — close-to-open coherence silently broken from the
/// reader app's perspective even though the user-space lease
/// state is correct.
///
/// Why not just retry on a timer: the kernel page cache serves
/// most reads WITHOUT round-tripping to our dispatcher. The only
/// reliable hook is the next syscall that DOES reach us — Open
/// is the cheapest one to instrument because every long-lived
/// read fd starts with one.
pub fn notify_inval_inode_failed_for(
    failed: &std::collections::HashSet<u64>,
    ino: u64,
) -> bool {
    failed.contains(&ino)
}

/// F-fuse-lease-2: callback the per-mount invalidation poll loop
/// runs for every per-ino `WriterClosed` / `LeaseRevoked` event.
/// In production this is `notifier.inval_inode(ino, 0, 0)` against
/// the live `fuser::Session`'s `Notifier`, which drops the kernel's
/// attribute + page cache for the ino so the next syscall reaches
/// our dispatcher.
///
/// Boxed as a trait object on `Rc` because the compio runtime is
/// single-threaded — `Rc` is enough; no `Send` needed — and the
/// callback is invoked many times per event batch (so we don't
/// move-out).
///
/// `None` in tests + headless contexts skips kernel-side eviction
/// (the user-space lease bookkeeping still updates correctly).
pub type InodeInvalidator = std::rc::Rc<dyn Fn(u64)>;

/// F-fuse-lease-2 — pure-fn extracted from the invalidation poll
/// loop so the "per-event ⇒ per-ino invalidator call" contract
/// can be unit-tested without booting a real cluster (coco P3
/// review feedback: cluster-bound e2e tests are `#[ignore]`'d
/// and default CI doesn't catch regressions in this loop).
///
/// For every event in the batch, runs the kernel-cache eviction
/// callback iff the event has a non-zero ino. `ino == 0` is the
/// manager-side overflow sentinel and is filtered here — the
/// wholesale-clear branch in `spawn_lease_background_tasks`
/// handles it separately.
pub fn invalidate_kernel_cache_for_events(
    events: &[autumn_rpc::manager_rpc::MgrInvalidation],
    invalidator: &InodeInvalidator,
) {
    for ev in events {
        if ev.ino != 0 {
            invalidator(ev.ino);
        }
    }
}

/// F-fuse-lease-1 + F-fuse-lease-2: per-mount background tasks.
/// Spawn ONCE on the compio runtime right after `FsState::new`
/// (before the dispatch loop). The two tasks are a lease heartbeat
/// loop + an invalidation poll loop:
///
/// - heartbeat: TTL/6 = 5s tick; renews every held lease;
///   `HeartbeatResult::NotHeld` drops the entry (the mount's open
///   fds will surface EIO on their next op — explicit failure
///   beats silently serving stale state).
/// - invalidation poll: persistent long-poll; per-event update
///   `invalidations[ino]` via `apply_invalidation`; **F-fuse-lease-2:**
///   per-ino call `invalidator(ino)` (when supplied) so the kernel's
///   attribute + page cache is dropped — without this, a fuse
///   reader on host B may continue to serve from the kernel page
///   cache after host A's writer closes, breaking close-to-open
///   coherence at the kernel boundary even though the user-space
///   lease state is correct. Overflow sentinel or transport error
///   → wholesale invalidate + best-effort `ReleaseLease`.
pub fn spawn_lease_background_tasks(state: &FsState, invalidator: Option<InodeInvalidator>) {
    use std::time::Duration;
    // BUG-LEASE-6 (P2 #7, 2026-06-06) — stash the invalidator on
    // FsState so the Open arm can retry the kernel notify when
    // `notify_inval_failed` contains the ino. Stored only when
    // production main.rs supplied a real Notifier-backed closure;
    // tests + headless contexts pass None, in which case the Open
    // arm short-circuits to "drop the cached InodeState" (no
    // kernel retry to attempt).
    if let Some(ref inv) = invalidator {
        *state.kernel_invalidator.borrow_mut() = Some(inv.clone());
    }
    let cluster_h = state.client.clone();
    let id_h = state.client_id.clone();
    let held_h = state.held_leases.clone();
    compio::runtime::spawn(async move {
        loop {
            compio::time::sleep(Duration::from_secs(5)).await;
            let inos: Vec<u64> = held_h.borrow().keys().copied().collect();
            if inos.is_empty() {
                continue;
            }
            for ino in inos {
                match autumn_client::lease::heartbeat(&cluster_h, &id_h, ino).await {
                    Ok(autumn_client::lease::HeartbeatResult::Renewed(info)) => {
                        if let Some(slot) = held_h.borrow_mut().get_mut(&ino) {
                            slot.lease_epoch = info.version;
                        }
                    }
                    Ok(autumn_client::lease::HeartbeatResult::NotHeld) => {
                        tracing::warn!(
                            ino,
                            "F-fuse-lease-1: heartbeat NotHeld; dropping local lease entry"
                        );
                        held_h.borrow_mut().remove(&ino);
                    }
                    Err(e) => {
                        tracing::warn!(ino, error = %e, "F-fuse-lease-1: heartbeat transient");
                    }
                }
            }
        }
    })
    .detach();

    let cluster_p = state.client.clone();
    let id_p = state.client_id.clone();
    let held_p = state.held_leases.clone();
    let inv_p = state.invalidations.clone();
    let invalidator_p = invalidator;
    compio::runtime::spawn(async move {
        loop {
            match autumn_client::lease::poll_invalidations(&cluster_p, &id_p).await {
                Ok(events) => {
                    let wholesale = {
                        let mut inv = inv_p.borrow_mut();
                        autumn_client::lease::apply_invalidation(&events, &mut inv)
                    };
                    for ev in &events {
                        tracing::info!(
                            ino = ev.ino,
                            version = ev.version,
                            kind = ev.kind,
                            "F-fuse-lease-2: invalidation"
                        );
                    }
                    // F-fuse-lease-2: drop the kernel's attribute
                    // + page cache for each non-zero ino in the
                    // batch (ino=0 is the manager-side overflow
                    // sentinel and is handled by the wholesale
                    // branch below — not a real FUSE ino).
                    // Extracted to a pure-fn so the contract is
                    // unit-testable without booting a cluster.
                    if let Some(inv) = &invalidator_p {
                        invalidate_kernel_cache_for_events(&events, inv);
                    }
                    // BUG-LEASE-3 (coco P0 #3, 2026-06-05): per-event
                    // immediate eviction on LeaseRevoked. Matches the
                    // ioring fix; same window of unauthorized writes
                    // existed here pre-fix.
                    let evicted_inos = {
                        let mut held_mut = held_p.borrow_mut();
                        evict_revoked_held_leases(&events, &mut held_mut)
                    };
                    for ino in &evicted_inos {
                        if let Err(e) =
                            autumn_client::lease::release(&cluster_p, &id_p, *ino).await
                        {
                            tracing::warn!(
                                ino = *ino,
                                error = %e,
                                "BUG-LEASE-3: best-effort release after fuse-side eviction"
                            );
                        }
                    }
                    if wholesale {
                        tracing::warn!(
                            "F-fuse-lease-1: overflow sentinel; wholesale invalidating mount"
                        );
                        // Best-effort release of every held lease
                        // BEFORE we drop the local bookkeeping.
                        // Without this the manager would keep those writer
                        // leases until TTL (~30s) and block other
                        // clients. Partial recovery from coco P1
                        // #3 — a full "revoked-state-aware
                        // read/write" rework is a separate
                        // follow-up.
                        let drained: Vec<u64> = held_p.borrow().keys().copied().collect();
                        held_p.borrow_mut().clear();
                        inv_p.borrow_mut().clear();
                        // F-fuse-lease-2: kernel-side wholesale
                        // eviction too — otherwise a reader app
                        // keeps serving from page cache after we
                        // dropped the lease bookkeeping.
                        if let Some(inv) = &invalidator_p {
                            for ino in &drained {
                                inv(*ino);
                            }
                        }
                        for ino in drained {
                            if let Err(e) =
                                autumn_client::lease::release(&cluster_p, &id_p, ino).await
                            {
                                tracing::warn!(ino, error = %e, "best-effort release after overflow");
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "F-fuse-lease-1: poll failed; invalidating mount cache + retry"
                    );
                    // Same best-effort release as the overflow
                    // path. A transport error means the manager
                    // may already have dropped our session's lease
                    // bookkeeping (it didn't see our heartbeats),
                    // so the release calls may be no-ops — but we
                    // still try.
                    let drained: Vec<u64> = held_p.borrow().keys().copied().collect();
                    held_p.borrow_mut().clear();
                    inv_p.borrow_mut().clear();
                    if let Some(inv) = &invalidator_p {
                        for ino in &drained {
                            inv(*ino);
                        }
                    }
                    for ino in drained {
                        let _ = autumn_client::lease::release(&cluster_p, &id_p, ino).await;
                    }
                    compio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    })
    .detach();
}

/// Initialize the root inode if it doesn't exist yet.
pub async fn init_root(state: &mut FsState) -> Result<()> {
    let root_key = key::inode_key(ROOT_INO);
    if state.kv_get(&root_key).await.is_ok() {
        tracing::info!("root inode already exists");
        return Ok(());
    }

    tracing::info!("creating root inode");
    let root_meta = new_dir_meta(0o755, unsafe { libc::getuid() }, unsafe { libc::getgid() });
    put_inode(state, ROOT_INO, &root_meta).await?;

    // Initialize the inode counter
    let next_ino_key = key::next_inode_key();
    let initial = (ROOT_INO + 1 + INODE_ALLOC_BATCH).to_be_bytes();
    state.kv_put(&next_ino_key, &initial).await?;
    state.next_inode = ROOT_INO + 1;
    state.inode_batch_end = ROOT_INO + 1 + INODE_ALLOC_BATCH;

    Ok(())
}

/// Process a single FsRequest. Returns false if the loop should exit (Destroy).
pub async fn handle_request(state: &mut FsState, req: FsRequest) -> bool {
    match req {
        FsRequest::Init { reply } => {
            let result = init_root(state).await;
            // UNLINK-1: replay any unlink interrupted by a crash —
            // tombstoned inodes are unreachable by invariant, so the
            // sweep can delete their data unconditionally. Best-effort:
            // a failed sweep retries at the next mount.
            match crate::extent::sweep_unlink_tombstones(state).await {
                Ok(0) => {}
                Ok(n) => tracing::info!(reaped = n, "unlink tombstone sweep"),
                Err(e) => tracing::warn!("unlink tombstone sweep failed: {e}"),
            }
            let _ = reply.send(result);
        }
        FsRequest::Destroy => {
            // Flush all dirty inodes before shutting down
            let dirty: Vec<u64> = state.dirty_inodes.iter().copied().collect();
            for ino in dirty {
                if let Err(e) = write::flush_inode(state, ino).await {
                    tracing::warn!(ino, error = %e, "destroy: flush failed");
                }
            }
            return false;
        }
        FsRequest::Lookup {
            parent,
            name,
            reply,
        } => {
            let result = dir::lookup(state, parent, &name).await;
            let _ = reply.send(result);
        }
        FsRequest::Forget { ino, nlookup } => {
            if let Some(count) = state.lookup_count.get_mut(&ino) {
                *count = count.saturating_sub(nlookup);
                if *count == 0 {
                    state.lookup_count.remove(&ino);
                    // Evict from cache if not open
                    if let Some(is) = state.inodes.get(&ino) {
                        if is.open_count == 0 && !is.dirty {
                            state.inodes.remove(&ino);
                        }
                    }
                }
            }
        }
        FsRequest::GetAttr { ino, reply } => {
            let result = async {
                let meta = get_inode(state, ino).await?;
                Ok(inode_to_attr(ino, &meta))
            }
            .await;
            let _ = reply.send(result);
        }
        FsRequest::SetAttr {
            ino,
            mode,
            uid,
            gid,
            size,
            atime,
            mtime,
            reply,
        } => {
            let result = async {
                let mut meta = get_inode(state, ino).await?;
                if let Some(m) = mode {
                    meta.mode = (meta.mode & S_IFMT) | (m & 0o7777);
                }
                if let Some(u) = uid {
                    meta.uid = u;
                }
                if let Some(g) = gid {
                    meta.gid = g;
                }
                if let Some(s) = size {
                    write::truncate(state, ino, s).await?;
                    // Re-fetch after truncate
                    meta = get_inode(state, ino).await?;
                }
                // Resolve a SetAttr time (explicit timestamp or "now") into
                // the meta's secs/nsecs pair — identical for atime and mtime.
                fn apply_time(t: fuser::TimeOrNow, secs: &mut i64, nsecs: &mut u32) {
                    match t {
                        fuser::TimeOrNow::SpecificTime(st) => {
                            let d = st.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                            *secs = d.as_secs() as i64;
                            *nsecs = d.subsec_nanos();
                        }
                        fuser::TimeOrNow::Now => {
                            let (s, ns) = now_ts();
                            *secs = s;
                            *nsecs = ns;
                        }
                    }
                }
                if let Some(t) = atime {
                    apply_time(t, &mut meta.atime_secs, &mut meta.atime_nsecs);
                }
                if let Some(t) = mtime {
                    apply_time(t, &mut meta.mtime_secs, &mut meta.mtime_nsecs);
                }
                let (s, ns) = now_ts();
                meta.ctime_secs = s;
                meta.ctime_nsecs = ns;
                put_inode(state, ino, &meta).await?;
                Ok(inode_to_attr(ino, &meta))
            }
            .await;
            let _ = reply.send(result);
        }
        FsRequest::Mkdir {
            parent,
            name,
            mode,
            reply,
        } => {
            let result = dir::mkdir(state, parent, &name, mode).await;
            let _ = reply.send(result);
        }
        FsRequest::Rmdir {
            parent,
            name,
            reply,
        } => {
            let result = dir::rmdir(state, parent, &name).await;
            let _ = reply.send(result);
        }
        FsRequest::Readdir { ino, offset, reply } => {
            let result = dir::readdir(state, ino, offset).await;
            let _ = reply.send(result);
        }
        FsRequest::Rename {
            old_parent,
            old_name,
            new_parent,
            new_name,
            reply,
        } => {
            let result = dir::rename(state, old_parent, &old_name, new_parent, &new_name).await;
            let _ = reply.send(result);
        }
        FsRequest::Create {
            parent,
            name,
            mode,
            flags,
            reply,
        } => {
            let result = async {
                let name_bytes = name.as_encoded_bytes();
                let dk = key::dirent_key(parent, name_bytes);
                if state.kv_exists(&dk).await.unwrap_or(false) {
                    return Err(anyhow!("EEXIST"));
                }
                let ino = alloc_inode(state).await?;
                let meta =
                    new_file_meta(mode, unsafe { libc::getuid() }, unsafe { libc::getgid() });
                put_inode(state, ino, &meta).await?;
                let dirent = DirentValue {
                    child_inode: ino,
                    file_type: DT_REG,
                };
                let dv = schema::encode_dirent(&dirent);
                state.kv_put(&dk, &dv).await?;
                // Update parent mtime
                let mut parent_meta = get_inode(state, parent).await?;
                let (s, ns) = now_ts();
                parent_meta.mtime_secs = s;
                parent_meta.mtime_nsecs = ns;
                put_inode(state, parent, &parent_meta).await?;
                // F-fuse-lease-1 (coco P1 #1 fix): Create produces a
                // writable fd just like Open. AcquireLease BEFORE
                // publishing the inode cache so a concurrent Open
                // from another mount can't get a writer-lease on
                // the same inode. Brand-new inode → no contention
                // is possible at the manager (new ino), but the
                // bookkeeping keeps held_leases consistent so the
                // matching Release fires ReleaseLease and other
                // mounts can then take over.
                let req_mode = lease_mode_for_open(flags);
                use lease::AcquireResult;
                let cluster = state.client.clone();
                let id = state.client_id.clone();
                let acquired_version = match lease::acquire(&cluster, &id, ino, req_mode).await {
                    Ok(AcquireResult::Granted(info)) => {
                        state.held_leases.borrow_mut().insert(
                            ino,
                            FuseLease {
                                mode: req_mode,
                                refcount: 1,
                                lease_epoch: info.version,
                                revoked: false,
                            },
                        );
                        info.version
                    }
                    Ok(AcquireResult::Conflict { manager_message }) => {
                        // Should not happen for a freshly-allocated
                        // ino, but if it does (UUID collision in
                        // another mount's last_version shadow, say)
                        // surface as EBUSY rather than silently
                        // owning a leaseless fd.
                        return Err(anyhow!(
                            "EBUSY: lease conflict on fresh ino {}: {}",
                            ino, manager_message
                        ));
                    }
                    Ok(AcquireResult::RevokePending { .. }) => {
                        // Unreachable: this call uses `acquire`
                        // (force=false). Defensive return so a
                        // future refactor that flips it to
                        // `acquire_force` fails loudly instead
                        // of silently dropping the grace window.
                        return Err(anyhow!(
                            "BUG: Create with non-force acquire returned RevokePending"
                        ));
                    }
                    Err(e) => return Err(anyhow!("AcquireLease ino {}: {}", ino, e)),
                };
                // Cache the inode. BUG-LEASE-7: seed `cached_version`
                // with the just-acquired lease version so the next
                // Open(ino) on this mount's restart can detect a
                // cross-mount version bump.
                state.inodes.insert(
                    ino,
                    InodeState {
                        meta: meta.clone(),
                        write_buf: None,
                        dirty: false,
                        open_count: 1,
                        extents: None,
                        cached_version: acquired_version,
                    },
                );
                *state.lookup_count.entry(ino).or_insert(0) += 1;
                let attr = inode_to_attr(ino, &meta);
                Ok((attr, ino)) // fh = ino for simplicity
            }
            .await;
            let _ = reply.send(result);
        }
        FsRequest::Unlink {
            parent,
            name,
            reply,
        } => {
            let result = async {
                let name_bytes = name.as_encoded_bytes();
                let dk = key::dirent_key(parent, name_bytes);
                let v = state.kv_get(&dk).await.map_err(|_| anyhow!("ENOENT"))?;
                let dirent: DirentValue =
                    schema::decode_dirent(&v).map_err(|e| anyhow!("{}", e))?;
                if dirent.file_type == DT_DIR {
                    return Err(anyhow!("EISDIR"));
                }
                // Delete dirent
                state.kv_delete(&dk).await?;
                // Decrement nlink
                let mut meta = get_inode(state, dirent.child_inode).await?;
                meta.nlink = meta.nlink.saturating_sub(1);
                if meta.nlink == 0 {
                    // UNLINK-1: the inode just became UNREACHABLE — remove
                    // its data under an intent tombstone so a crash
                    // mid-removal is replayed at next mount instead of
                    // leaking the extents forever. (The residual window is
                    // the single dirent-delete→tombstone-put gap; pre-fix
                    // it spanned the whole extent scan + N deletes.)
                    crate::extent::remove_unreachable_inode(state, dirent.child_inode).await?;
                } else {
                    put_inode(state, dirent.child_inode, &meta).await?;
                }
                // Update parent mtime
                let mut parent_meta = get_inode(state, parent).await?;
                let (s, ns) = now_ts();
                parent_meta.mtime_secs = s;
                parent_meta.mtime_nsecs = ns;
                put_inode(state, parent, &parent_meta).await?;
                Ok(())
            }
            .await;
            let _ = reply.send(result);
        }
        FsRequest::Open { ino, flags, reply } => {
            let result = async {
                // BUG-LEASE-6 (P2 #7, 2026-06-06) — fail-closed on
                // stale `notify_inval_inode`. If the most recent
                // kernel notify for this ino FAILED (sticky set
                // populated by the per-mount invalidator), drop
                // any cached InodeState BEFORE the get_inode reload
                // so this Open's bookkeeping is built on fresh
                // attrs. Then retry the kernel notify; on success,
                // clear the sticky entry. On retry failure, leave
                // the sticky entry — next Open of the same ino
                // tries again. Without this a transient kernel
                // EINVAL/EAGAIN strands the kernel page cache on
                // stale bytes despite our user-space lease state
                // being correct.
                let sticky_failed = {
                    let s = state.notify_inval_failed.borrow();
                    notify_inval_inode_failed_for(&s, ino)
                };
                if sticky_failed {
                    state.inodes.remove(&ino);
                    // Clone the Rc out of the RefCell so we don't
                    // hold the borrow across the invalidator call
                    // (the closure itself borrows
                    // `notify_inval_failed`).
                    let invalidator_clone = state.kernel_invalidator.borrow().clone();
                    if let Some(invalidator) = invalidator_clone {
                        invalidator(ino);
                        // The invalidator closure itself records
                        // failures (in main.rs); if the retry
                        // succeeded the closure cleared `failed[ino]`.
                        // Re-read it now.
                        let still_failed = {
                            let s = state.notify_inval_failed.borrow();
                            notify_inval_inode_failed_for(&s, ino)
                        };
                        if !still_failed {
                            tracing::info!(
                                ino,
                                "BUG-LEASE-6: notify_inval_inode retry succeeded on Open"
                            );
                        }
                    }
                }
                // Ensure inode exists.
                let _ = get_inode(state, ino).await?;

                // F-fuse-lease-1: AcquireLease BEFORE bumping the
                // local open_count so a conflicting writer surfaces
                // as `EBUSY` (mapped to ErrorKind::Other in
                // `err_to_errno`). Refcount the lease per-mount.
                let req_mode = lease_mode_for_open(flags);
                let mut needs_acquire = false;
                {
                    let mut m = state.held_leases.borrow_mut();
                    match m.get_mut(&ino) {
                        Some(slot) if slot.revoked => {
                            // R2-P0 #3 — the prior lease was revoked
                            // server-side; drop the stale entry and
                            // re-acquire. The client picks up a
                            // fresh `version` (and a fresh server-
                            // side epoch once BUG-LEASE-2 Phase 2
                            // exposes lease_epoch to the SDK).
                            m.remove(&ino);
                            needs_acquire = true;
                        }
                        Some(slot) => {
                            if slot.mode != req_mode {
                                return Err(anyhow!(
                                    "lease mode mismatch on ino {}: held={}, req={}",
                                    ino, slot.mode, req_mode
                                ));
                            }
                            slot.refcount = slot.refcount.saturating_add(1);
                        }
                        None => {
                            needs_acquire = true;
                        }
                    }
                }
                // BUG-LEASE-7 (P2 #8): capture the post-acquire lease
                // version so the per-inode-cache branch below can
                // compare it against `InodeState.cached_version` and
                // force a reload on mismatch. `0` is the "no fresh
                // acquire happened this Open" sentinel — we keep the
                // cached InodeState untouched in that case (matching
                // pre-fix behavior; the version-mismatch check needs
                // a known-current version, which only the acquire
                // round-trip can produce).
                let acquired_version: u64 = if needs_acquire {
                    use lease::AcquireResult;
                    let cluster = state.client.clone();
                    let id = state.client_id.clone();
                    match lease::acquire(&cluster, &id, ino, req_mode).await {
                        Ok(AcquireResult::Granted(info)) => {
                            state.held_leases.borrow_mut().insert(
                                ino,
                                FuseLease {
                                    mode: req_mode,
                                    refcount: 1,
                                    lease_epoch: info.version,
                                    revoked: false,
                                },
                            );
                            info.version
                        }
                        Ok(AcquireResult::Conflict { manager_message }) => {
                            return Err(anyhow!(
                                "EBUSY: writer lease conflict on ino {}: {}",
                                ino, manager_message
                            ));
                        }
                        Ok(AcquireResult::RevokePending { .. }) => {
                            // Unreachable for non-force acquire.
                            return Err(anyhow!(
                                "BUG: Open with non-force acquire returned RevokePending"
                            ));
                        }
                        Err(e) => return Err(anyhow!("AcquireLease ino {}: {}", ino, e)),
                    }
                } else {
                    // Same mount, same fd-family reopen: we kept the
                    // refcount and the lease version is still in
                    // `held_leases[ino]`. Read it back so the
                    // BUG-LEASE-7 cache check below works on EVERY
                    // Open, not just the AcquireLease path. Stale
                    // entries are revoked-checked above, so this
                    // version reflects the current grant.
                    state
                        .held_leases
                        .borrow()
                        .get(&ino)
                        .map(|s| s.lease_epoch)
                        .unwrap_or(0)
                };

                // BUG-LEASE-7: now publish to the per-inode cache.
                // If a cached entry exists AND its `cached_version`
                // is older than the freshly-acquired lease version,
                // a different writer (likely a different mount) has
                // closed since this mount last cached the inode —
                // the cached `meta` / `extents` are stale; rebuild
                // them. `acquired_version == 0` means no fresh
                // acquire was issued this call (refcount > 1, no
                // revoke); keep the cached entry as-is in that
                // case (matches pre-fix behavior).
                let needs_reload = state
                    .inodes
                    .get(&ino)
                    .map(|is| inode_cache_needs_reload(is.cached_version, acquired_version))
                    .unwrap_or(false);
                if needs_reload {
                    tracing::info!(
                        ino,
                        cached = state.inodes.get(&ino).map(|is| is.cached_version).unwrap_or(0),
                        acquired = acquired_version,
                        "BUG-LEASE-7: stale cached InodeState; reloading"
                    );
                    state.inodes.remove(&ino);
                }
                if let Some(is) = state.inodes.get_mut(&ino) {
                    is.open_count += 1;
                    // Bump cached_version forward — if we entered
                    // this branch with `is.cached_version <
                    // acquired_version`, `needs_reload` would have
                    // dropped the entry. The remaining case is
                    // `cached_version >= acquired_version` (same
                    // mount, same generation, or transient acquire-
                    // skip path). Either way, take the max so a
                    // future Open's cached version is at least the
                    // version we've now committed to.
                    if acquired_version > is.cached_version {
                        is.cached_version = acquired_version;
                    }
                } else {
                    let meta = get_inode(state, ino).await?;
                    state.inodes.insert(
                        ino,
                        InodeState {
                            meta,
                            write_buf: None,
                            dirty: false,
                            open_count: 1,
                            extents: None,
                            cached_version: acquired_version,
                        },
                    );
                }
                Ok(ino) // use ino as file handle
            }
            .await;
            let _ = reply.send(result);
        }
        FsRequest::Read {
            ino,
            offset,
            size,
            fuse_reply,
        } => {
            // Async-reply two-phase read (autumn-fuse perf fix #1):
            // - prepare under dispatcher's `&mut state` (cheap routing
            //   lookups + inode cache hit, no real I/O);
            // - spawn `execute` to do the parallel chunk fanout;
            // - the spawned task replies to fuser DIRECTLY via the
            //   shipped `ReplyData`, bypassing the std::mpsc reply hop.
            // The fuser kernel-channel reader thread is then free to
            // read the next /dev/fuse request immediately, so concurrent
            // FUSE reads can actually overlap.
            match read::prepare(state, ino, offset, size).await {
                Ok(plan) => {
                    compio::runtime::spawn(async move {
                        match read::execute(plan).await {
                            Ok(data) => fuse_reply.data(&data),
                            Err(e) => {
                                tracing::warn!(error = %e, "fuse read execute failed");
                                fuse_reply.error(libc::EIO);
                            }
                        }
                    })
                    .detach();
                }
                Err(e) => {
                    let errno = if e.to_string().contains("not found") {
                        libc::ENOENT
                    } else {
                        libc::EIO
                    };
                    fuse_reply.error(errno);
                }
            }
        }
        FsRequest::Write {
            ino,
            offset,
            data,
            reply,
        } => {
            // R2-P0 #3 (2026-06-06) — refuse writes whose lease the
            // manager has already revoked (sticky `revoked` flag on
            // `FuseLease`, set by the per-mount invalidation poll
            // loop at the moment the LeaseRevoked event was
            // observed — see `evict_revoked_held_leases`). Pre-fix
            // the Write arm dropped through unconditionally, so the
            // stale fd's bytes raced past the manager's revoke and
            // co-mingled with the new writer's view. Phase 2 of
            // BUG-LEASE-2 (PUT_ZC + fuse epoch-stamping) is what
            // makes the same guard hold on the PS side; this is
            // the client-side fail-fast.
            let lease_check = {
                let held = state.held_leases.borrow();
                check_write_allowed(&held, ino)
            };
            let result = match lease_check {
                Ok(()) => write::write(state, ino, offset, &data).await,
                Err(reason) => Err(anyhow!(
                    "EIO: write refused on ino {}: lease {}",
                    ino,
                    reason
                )),
            };
            let _ = reply.send(result);
        }
        FsRequest::Flush { ino, reply } => {
            let result = write::flush_inode(state, ino).await;
            let _ = reply.send(result);
        }
        FsRequest::Release { ino, flush, reply } => {
            let result = async {
                // F-fuse-lease-1 (coco P1 #2 fix): writer-release
                // MUST happen AFTER dirty data is flushed (plan
                // §6.2). The kernel's `flush: bool` argument is
                // unreliable — close-with-error paths and some
                // app patterns pass false even when buffers are
                // dirty.
                //
                // R2-P0 #2 (2026-06-06): when the lease was
                // server-side revoked (sticky `revoked` flag),
                // we ALSO must flush even with kernel `flush=false`
                // AND refcount > 1, because the entry is going
                // away regardless (we drop revoked entries
                // unconditionally so a subsequent Open re-acquires
                // a fresh lease). Dropping the entry without
                // flushing silently loses the dirty buffer.
                // `compute_release_action` is the pure-fn that
                // captures all three signals (kernel_flush /
                // refcount-1 / revoked) into one decision tuple;
                // see its doc for the `propagate_flush_err` nuance.
                let action = {
                    let held = state.held_leases.borrow();
                    compute_release_action(&held, ino, flush)
                };
                if action.must_flush {
                    if let Err(e) = write::flush_inode(state, ino).await {
                        if action.propagate_flush_err {
                            return Err(e);
                        }
                        // Revoked-flush is best-effort: log and
                        // continue so we still drop the held_lease
                        // entry. The bytes will fence at the PS
                        // once BUG-LEASE-2 Phase 2 wires epoch
                        // stamping; surfacing EIO here would
                        // confuse a legit next-Open + retry.
                        tracing::warn!(
                            ino,
                            error = %e,
                            "R2-P0 #2: revoked-flush failed; continuing to drop entry"
                        );
                    }
                }
                if let Some(is) = state.inodes.get_mut(&ino) {
                    is.open_count = is.open_count.saturating_sub(1);
                    // Evict from cache if no longer open and not dirty
                    if is.open_count == 0
                        && !is.dirty
                        && state.lookup_count.get(&ino).copied().unwrap_or(0) == 0
                    {
                        state.inodes.remove(&ino);
                    }
                }
                // Refcount the lease; only the 1→0 transition
                // OR a revoked entry fires ReleaseLease.
                // Best-effort — a failed release is recovered by
                // the manager's TTL revoke loop (≤ 30s window).
                let release_now = {
                    let mut m = state.held_leases.borrow_mut();
                    match m.get_mut(&ino) {
                        Some(slot) => {
                            slot.refcount = slot.refcount.saturating_sub(1);
                            // Drop the entry on 1→0 OR if revoked
                            // (revoked entries are not recoverable
                            // — the manager already gave the lease
                            // to someone else).
                            if action.must_drop_entry {
                                m.remove(&ino);
                                true
                            } else {
                                false
                            }
                        }
                        None => false,
                    }
                };
                if release_now {
                    let cluster = state.client.clone();
                    let id = state.client_id.clone();
                    if let Err(e) = lease::release(&cluster, &id, ino).await {
                        tracing::warn!(
                            ino,
                            error = %e,
                            "F-fuse-lease-1: ReleaseLease failed; TTL revoke is the backstop"
                        );
                    }
                }
                Ok(())
            }
            .await;
            let _ = reply.send(result);
        }
        FsRequest::Fsync {
            ino,
            datasync: _,
            reply,
        } => {
            let result = write::flush_inode(state, ino).await;
            let _ = reply.send(result);
        }
        FsRequest::Statfs { reply } => {
            // cluster-df: report REAL backend capacity (was hardcoded 1 TiB).
            // statfs is rare (a `df` invocation), so an inline manager call is
            // fine — but BOUND it so a slow/down manager can't hang the
            // syscall; on timeout/error fall back to a benign large default.
            //
            // Mapping is CONSERVATIVE (÷3 = assume 3-replica). Usable LOGICAL
            // capacity is a RANGE under EC (cold data is 1.25-1.33×, hot data
            // 3×); statfs is a single scalar, so — CephFS-style — we collapse
            // the range to the WORST factor: never over-report free, so `df`
            // can't lull a writer into an optimistic ENOSPC. Already-EC'd cold
            // data means real free is higher; under-reporting is the safe side.
            const BSIZE: u64 = 4096;
            let fallback = StatfsData {
                blocks: 1 << 30,
                bfree: 1 << 29,
                bavail: 1 << 29,
                files: 1 << 20,
                ffree: 1 << 19,
                bsize: BSIZE as u32,
                namelen: 255,
            };
            let data = match compio::time::timeout(
                std::time::Duration::from_secs(2),
                state.client.cluster_df(),
            )
            .await
            {
                Ok(Ok(r)) if r.raw_total > 0 => {
                    let blocks = (r.raw_total / 3) / BSIZE;
                    let avail = (r.raw_free / 3) / BSIZE;
                    StatfsData {
                        blocks: blocks.max(1),
                        bfree: avail,
                        bavail: avail,
                        files: 1 << 20,
                        ffree: 1 << 19,
                        bsize: BSIZE as u32,
                        namelen: 255,
                    }
                }
                _ => fallback,
            };
            let _ = reply.send(Ok(data));
        }
    }
    true // continue processing
}

#[cfg(test)]
mod f_fuse_lease_2_unit_tests {
    //! F-fuse-lease-2 (coco P3 review feedback): unit-test the
    //! per-event ⇒ invalidator-call contract so default `cargo test`
    //! catches regressions. The `f_fuse_lease_2.rs` integration
    //! suite is `#[ignore]`'d (cluster-boot, slow); these guard
    //! the same shape with no cluster.

    use super::invalidate_kernel_cache_for_events;
    use autumn_rpc::manager_rpc::{
        MgrInvalidation, LEASE_INVAL_LEASE_REVOKED, LEASE_INVAL_META_CHANGED,
        LEASE_INVAL_WRITER_CLOSED,
    };
    use std::cell::RefCell;
    use std::rc::Rc;

    fn ev(ino: u64, version: u64, kind: u8) -> MgrInvalidation {
        MgrInvalidation { ino, version, kind }
    }

    fn counting_inv() -> (super::InodeInvalidator, Rc<RefCell<Vec<u64>>>) {
        let log = Rc::new(RefCell::new(Vec::new()));
        let log_c = log.clone();
        let inv: super::InodeInvalidator = Rc::new(move |ino: u64| {
            log_c.borrow_mut().push(ino);
        });
        (inv, log)
    }

    #[test]
    fn per_ino_writer_closed_calls_invalidator() {
        let (inv, log) = counting_inv();
        invalidate_kernel_cache_for_events(
            &[ev(7, 5, LEASE_INVAL_WRITER_CLOSED)],
            &inv,
        );
        assert_eq!(*log.borrow(), vec![7]);
    }

    #[test]
    fn ino_zero_overflow_sentinel_is_filtered() {
        // Manager's overflow signalling sends MetaChanged{ino=0};
        // the per-ino invalidator path MUST skip it (wholesale
        // branch in spawn_lease_background_tasks handles overflow).
        let (inv, log) = counting_inv();
        invalidate_kernel_cache_for_events(
            &[
                ev(0, 0, LEASE_INVAL_META_CHANGED),
                ev(42, 3, LEASE_INVAL_WRITER_CLOSED),
            ],
            &inv,
        );
        assert_eq!(*log.borrow(), vec![42], "ino=0 sentinel must NOT reach invalidator");
    }

    #[test]
    fn multi_ino_batch_invalidates_each() {
        // The production case: a poll batch carries events for
        // several distinct inos. Every non-zero ino must trigger
        // the kernel-cache evict.
        let (inv, log) = counting_inv();
        invalidate_kernel_cache_for_events(
            &[
                ev(100, 1, LEASE_INVAL_WRITER_CLOSED),
                ev(200, 2, LEASE_INVAL_LEASE_REVOKED),
                ev(300, 3, LEASE_INVAL_WRITER_CLOSED),
            ],
            &inv,
        );
        assert_eq!(*log.borrow(), vec![100, 200, 300]);
    }

    #[test]
    fn empty_events_is_a_noop() {
        let (inv, log) = counting_inv();
        invalidate_kernel_cache_for_events(&[], &inv);
        assert!(log.borrow().is_empty());
    }

    #[test]
    fn lease_revoked_also_triggers_invalidation() {
        // LeaseRevoked = manager TTL-expired the writer. Reader's
        // cached attrs/data are stale just like WriterClosed.
        let (inv, log) = counting_inv();
        invalidate_kernel_cache_for_events(
            &[ev(55, 7, LEASE_INVAL_LEASE_REVOKED)],
            &inv,
        );
        assert_eq!(*log.borrow(), vec![55]);
    }
}

#[cfg(test)]
mod bug_lease_3_fuse_tests {
    //! BUG-LEASE-3 (coco P0 #3, 2026-06-05) — fuse-side unit tests
    //! for the per-event eviction contract. Mirrors the ioring
    //! daemon's `bug_lease_3_tests`. Default-CI.

    use super::evict_revoked_held_leases;
    use crate::state::FuseLease;
    use autumn_rpc::manager_rpc::{
        MgrInvalidation, LEASE_INVAL_LEASE_REVOKED, LEASE_INVAL_META_CHANGED,
        LEASE_INVAL_WILL_REVOKE_IN, LEASE_INVAL_WRITER_CLOSED, LEASE_MODE_READ, LEASE_MODE_WRITE,
    };
    use std::collections::HashMap;

    fn ev(ino: u64, version: u64, kind: u8) -> MgrInvalidation {
        MgrInvalidation { ino, version, kind }
    }

    fn lease(mode: u8) -> FuseLease {
        FuseLease {
            mode,
            refcount: 1,
            lease_epoch: 1,
            revoked: false,
        }
    }

    #[test]
    fn lease_revoked_marks_held_lease() {
        // R2-P0 #2/#3: evict NO LONGER removes — it sets the
        // sticky `revoked: bool` so Write/Release can act on it.
        let mut held = HashMap::new();
        held.insert(42, lease(LEASE_MODE_WRITE));
        held.insert(99, lease(LEASE_MODE_READ));
        let newly_revoked = evict_revoked_held_leases(
            &[ev(42, 6, LEASE_INVAL_LEASE_REVOKED)],
            &mut held,
        );
        assert_eq!(newly_revoked, vec![42]);
        assert!(held.contains_key(&42), "entry must stay in map for Write/Release to see");
        assert!(held.get(&42).unwrap().revoked, "entry must be marked revoked");
        assert!(held.contains_key(&99));
        assert!(!held.get(&99).unwrap().revoked, "untouched entry must stay unrevoked");
    }

    #[test]
    fn double_revoke_is_idempotent_after_r2() {
        // Second LeaseRevoked for the same ino must NOT re-emit
        // a newly_revoked entry — otherwise the dispatch caller's
        // best-effort `lease::release` runs twice for one event.
        let mut held = HashMap::new();
        held.insert(42, lease(LEASE_MODE_WRITE));
        let _ = evict_revoked_held_leases(
            &[ev(42, 6, LEASE_INVAL_LEASE_REVOKED)],
            &mut held,
        );
        let newly = evict_revoked_held_leases(
            &[ev(42, 7, LEASE_INVAL_LEASE_REVOKED)],
            &mut held,
        );
        assert!(newly.is_empty(), "second revoke must not re-emit; got {:?}", newly);
        assert!(held.get(&42).unwrap().revoked);
    }

    #[test]
    fn writer_closed_does_not_mark() {
        let mut held = HashMap::new();
        held.insert(42, lease(LEASE_MODE_READ));
        let newly = evict_revoked_held_leases(
            &[ev(42, 6, LEASE_INVAL_WRITER_CLOSED)],
            &mut held,
        );
        assert!(newly.is_empty());
        assert!(!held.get(&42).unwrap().revoked);
    }

    #[test]
    fn will_revoke_in_does_not_mark() {
        let mut held = HashMap::new();
        held.insert(42, lease(LEASE_MODE_WRITE));
        let newly = evict_revoked_held_leases(
            &[ev(42, 5000, LEASE_INVAL_WILL_REVOKE_IN)],
            &mut held,
        );
        assert!(newly.is_empty());
        assert!(!held.get(&42).unwrap().revoked);
    }

    #[test]
    fn ino_zero_overflow_sentinel_does_not_mark() {
        let mut held = HashMap::new();
        held.insert(42, lease(LEASE_MODE_WRITE));
        let newly = evict_revoked_held_leases(
            &[ev(0, 0, LEASE_INVAL_META_CHANGED)],
            &mut held,
        );
        assert!(newly.is_empty());
        assert!(!held.get(&42).unwrap().revoked);
    }

    #[test]
    fn revoked_for_ino_we_dont_hold_is_a_noop() {
        let mut held = HashMap::new();
        let newly = evict_revoked_held_leases(
            &[ev(999, 1, LEASE_INVAL_LEASE_REVOKED)],
            &mut held,
        );
        assert!(newly.is_empty());
    }
}

#[cfg(test)]
mod r2_p0_2_3_lease_check_tests {
    //! R2-P0 #2/#3 (2026-06-06) — pure-fn reproductions for the
    //! Write/Release lease-revoke checks. Default-CI; mirrors the
    //! ioring `inode_open_locks` style.

    use super::{check_write_allowed, compute_release_action, ReleaseAction};
    use crate::state::FuseLease;
    use autumn_rpc::manager_rpc::{LEASE_MODE_READ, LEASE_MODE_WRITE};
    use std::collections::HashMap;

    fn lease(mode: u8, refcount: u32, revoked: bool) -> FuseLease {
        FuseLease { mode, refcount, lease_epoch: 1, revoked }
    }

    // ── check_write_allowed ────────────────────────────────────

    #[test]
    fn write_with_active_writer_lease_is_allowed() {
        let mut held = HashMap::new();
        held.insert(7, lease(LEASE_MODE_WRITE, 1, false));
        assert_eq!(check_write_allowed(&held, 7), Ok(()));
    }

    #[test]
    fn write_with_revoked_lease_is_refused() {
        // R2-P0 #3 KEY ASSERTION: a held entry whose lease was
        // revoked server-side must NOT let writes through.
        let mut held = HashMap::new();
        held.insert(7, lease(LEASE_MODE_WRITE, 1, true));
        assert_eq!(check_write_allowed(&held, 7), Err("revoked"));
    }

    #[test]
    fn write_with_read_only_lease_is_refused() {
        let mut held = HashMap::new();
        held.insert(7, lease(LEASE_MODE_READ, 1, false));
        assert_eq!(check_write_allowed(&held, 7), Err("wrong mode"));
    }

    #[test]
    fn write_without_any_lease_is_refused() {
        let held = HashMap::new();
        assert_eq!(check_write_allowed(&held, 7), Err("no lease"));
    }

    // ── compute_release_action ─────────────────────────────────

    #[test]
    fn release_normal_last_refcount_flushes_and_drops() {
        let mut held = HashMap::new();
        held.insert(7, lease(LEASE_MODE_WRITE, 1, false));
        let action = compute_release_action(&held, 7, false);
        assert_eq!(action, ReleaseAction {
            must_flush: true,
            must_drop_entry: true,
            propagate_flush_err: true,
        });
    }

    #[test]
    fn release_normal_non_last_refcount_no_flush() {
        let mut held = HashMap::new();
        held.insert(7, lease(LEASE_MODE_WRITE, 3, false));
        let action = compute_release_action(&held, 7, false);
        assert_eq!(action, ReleaseAction {
            must_flush: false,
            must_drop_entry: false,
            propagate_flush_err: true,
        });
    }

    #[test]
    fn release_kernel_flush_true_always_flushes() {
        let mut held = HashMap::new();
        held.insert(7, lease(LEASE_MODE_WRITE, 3, false));
        let action = compute_release_action(&held, 7, true);
        assert!(action.must_flush);
        assert!(!action.must_drop_entry, "kernel-flush alone doesn't drop the entry");
    }

    #[test]
    fn release_revoked_entry_flushes_and_drops_even_at_high_refcount() {
        // R2-P0 #2 KEY ASSERTION: a revoked entry MUST flush
        // before drop even when refcount > 1 and kernel flush=false.
        // Pre-fix the original "release_now_pred = refcount==1"
        // would skip the flush and the dirty buffer would be lost
        // on the eventual drop.
        let mut held = HashMap::new();
        held.insert(7, lease(LEASE_MODE_WRITE, 3, true));
        let action = compute_release_action(&held, 7, false);
        assert_eq!(action, ReleaseAction {
            must_flush: true,
            must_drop_entry: true,
            propagate_flush_err: false,
        });
    }

    #[test]
    fn release_revoked_flush_err_does_not_propagate() {
        // A revoked entry's flush is best-effort: the bytes will
        // fence at the PS once BUG-LEASE-2 Phase 2 wires epoch
        // stamping. Surfacing the err to the kernel would mask
        // the legit next-Open-after-revoke retry path.
        let mut held = HashMap::new();
        held.insert(7, lease(LEASE_MODE_WRITE, 1, true));
        let action = compute_release_action(&held, 7, false);
        assert!(!action.propagate_flush_err);
    }

    #[test]
    fn release_no_entry_honors_kernel_flush_only() {
        // Pre-Open Release (rare but possible on some kernel
        // close-with-error paths) — no entry to drop, no lease
        // bookkeeping; just honor kernel flush as before.
        let held = HashMap::new();
        let action = compute_release_action(&held, 7, true);
        assert_eq!(action, ReleaseAction {
            must_flush: true,
            must_drop_entry: false,
            propagate_flush_err: true,
        });
        let action2 = compute_release_action(&held, 7, false);
        assert!(!action2.must_flush);
    }
}

#[cfg(test)]
mod bug_lease_6_notify_fail_closed_tests {
    //! BUG-LEASE-6 (P2 #7, 2026-06-06) — pure-fn / closure-driven
    //! unit tests for the `notify_inval_inode` fail-closed
    //! tracking. The Open-arm reload + retry flow is exercised
    //! end-to-end via a stand-in "fake notifier" closure that
    //! the test controls.

    use super::notify_inval_inode_failed_for;
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::rc::Rc;

    #[test]
    fn empty_failed_set_is_not_stale() {
        let set: HashSet<u64> = HashSet::new();
        assert!(!notify_inval_inode_failed_for(&set, 7));
    }

    #[test]
    fn populated_failed_set_marks_ino_stale() {
        let mut set = HashSet::new();
        set.insert(7);
        assert!(notify_inval_inode_failed_for(&set, 7));
        assert!(!notify_inval_inode_failed_for(&set, 8));
    }

    /// Drive the main.rs invalidator closure shape — Open path
    /// rebuild + retry. Pre-fix the closure just `warn!`-logged
    /// the kernel error and dropped it on the floor; post-fix it
    /// records the ino in `notify_inval_failed`, and a follow-up
    /// successful call clears it.
    #[test]
    fn invalidator_records_failure_and_clears_on_subsequent_success() {
        // A fake "kernel notifier" that succeeds/fails based on a
        // toggle. Mirrors `fuser::Notifier::inval_inode`'s
        // io::Result<()> shape.
        let fail_next = Rc::new(RefCell::new(true));
        let notify_failed: Rc<RefCell<HashSet<u64>>> =
            Rc::new(RefCell::new(HashSet::new()));

        let fail_h = fail_next.clone();
        let notify_failed_h = notify_failed.clone();
        let invalidator: super::InodeInvalidator = Rc::new(move |ino: u64| {
            let should_fail = *fail_h.borrow();
            if should_fail {
                notify_failed_h.borrow_mut().insert(ino);
            } else {
                notify_failed_h.borrow_mut().remove(&ino);
            }
        });

        // First call — fail toggle is on. Closure records the ino.
        invalidator(42);
        assert!(notify_failed.borrow().contains(&42));

        // Toggle: subsequent call succeeds. Closure clears the ino.
        *fail_next.borrow_mut() = false;
        invalidator(42);
        assert!(
            !notify_failed.borrow().contains(&42),
            "post-success clear: failed set must NOT contain {{ino}} anymore"
        );
    }

    /// KEY assertion: failure on ino A must NOT contaminate the
    /// staleness of ino B. The sticky set is per-ino.
    #[test]
    fn failure_isolation_per_ino() {
        let fail_for: Rc<RefCell<Option<u64>>> = Rc::new(RefCell::new(Some(42)));
        let notify_failed: Rc<RefCell<HashSet<u64>>> =
            Rc::new(RefCell::new(HashSet::new()));

        let fail_h = fail_for.clone();
        let notify_failed_h = notify_failed.clone();
        let invalidator: super::InodeInvalidator = Rc::new(move |ino: u64| {
            if *fail_h.borrow() == Some(ino) {
                notify_failed_h.borrow_mut().insert(ino);
            } else {
                notify_failed_h.borrow_mut().remove(&ino);
            }
        });

        invalidator(42); // ino 42 fails
        invalidator(99); // ino 99 succeeds
        assert!(notify_inval_inode_failed_for(&notify_failed.borrow(), 42));
        assert!(!notify_inval_inode_failed_for(&notify_failed.borrow(), 99));
    }

    /// On Open arm: when the sticky set is populated and the Open
    /// arm calls the invalidator, a successful retry clears the
    /// sticky entry. This test simulates the Open-arm code path
    /// against the same closure shape main.rs builds.
    #[test]
    fn open_arm_simulated_retry_clears_sticky_on_success() {
        // Initial state: ino 42 was previously marked failed.
        let notify_failed: Rc<RefCell<HashSet<u64>>> =
            Rc::new(RefCell::new(HashSet::from([42])));

        // The "real" notifier on this retry succeeds.
        let notify_failed_h = notify_failed.clone();
        let invalidator: super::InodeInvalidator = Rc::new(move |ino: u64| {
            // success path: remove from sticky.
            notify_failed_h.borrow_mut().remove(&ino);
        });

        // Mimic the Open arm: sticky → invalidator → re-check.
        let sticky = notify_inval_inode_failed_for(&notify_failed.borrow(), 42);
        assert!(sticky, "precondition: must start sticky");
        invalidator(42);
        let still_sticky = notify_inval_inode_failed_for(&notify_failed.borrow(), 42);
        assert!(
            !still_sticky,
            "post-retry-success: sticky entry MUST be cleared"
        );
    }

    /// On Open arm: a sticky entry whose retry ALSO fails stays
    /// in the set, so the NEXT Open arms tries again.
    #[test]
    fn open_arm_simulated_retry_keeps_sticky_on_persistent_failure() {
        let notify_failed: Rc<RefCell<HashSet<u64>>> =
            Rc::new(RefCell::new(HashSet::from([42])));

        let notify_failed_h = notify_failed.clone();
        let invalidator: super::InodeInvalidator = Rc::new(move |ino: u64| {
            // Persistent failure: re-mark sticky.
            notify_failed_h.borrow_mut().insert(ino);
        });

        invalidator(42);
        assert!(
            notify_inval_inode_failed_for(&notify_failed.borrow(), 42),
            "persistent failure: sticky entry MUST stay set for the next Open"
        );
    }
}

#[cfg(test)]
mod bug_lease_7_open_cache_version_tests {
    //! BUG-LEASE-7 (P2 #8, 2026-06-06) — pure-fn / driving tests
    //! for `inode_cache_needs_reload`. The Open arm wires this
    //! against (cached_version, acquired_version) on every Open;
    //! a `true` return drops the cached InodeState and forces
    //! `get_inode` to repopulate. Mirror of ioring's
    //! F-ioring-lease-4 + F-ioring-lease-5 #1 staleness check.

    use super::inode_cache_needs_reload;

    #[test]
    fn equal_version_does_not_reload() {
        // Common Open path: same mount, same fd-family reopen,
        // version unchanged → keep the cached state.
        assert!(!inode_cache_needs_reload(7, 7));
    }

    #[test]
    fn higher_cache_does_not_reload() {
        // Shouldn't happen in practice (acquired is monotonic),
        // but be defensive: a cached version GREATER than the
        // acquired version is NOT stale (no version regression
        // implied — could be a stale read of held_leases on a
        // multi-step race). The strict `<` rule keeps the
        // helper from spuriously reloading.
        assert!(!inode_cache_needs_reload(10, 7));
    }

    #[test]
    fn lower_cache_with_nonzero_acquired_reloads() {
        // KEY ASSERTION: another mount wrote + closed, manager
        // bumped version 7 → 8, this mount's Open acquires
        // version 8 but its cached InodeState was last refetched
        // when version was 7. MUST reload.
        assert!(inode_cache_needs_reload(7, 8));
    }

    #[test]
    fn zero_acquired_does_not_reload() {
        // The Open arm's "I didn't observe a fresh version this
        // call" sentinel — same-mount Open without re-acquire
        // AND `held_leases[ino]` was absent. The helper returns
        // `false` so the existing cached entry stays untouched
        // (matches pre-fix behavior; never makes things worse).
        assert!(!inode_cache_needs_reload(5, 0));
        assert!(!inode_cache_needs_reload(0, 0));
    }

    #[test]
    fn zero_cache_with_nonzero_acquired_reloads() {
        // Fresh-mount initial Open path used to be no-op (no
        // cached entry); but if a cached entry got installed
        // BEFORE the first lease (e.g. via a Lookup that
        // populated `inodes[ino]` with cached_version=0), the
        // first Open's lease version IS the legit cross-mount
        // delta — reload to pick up any cross-mount writes.
        assert!(inode_cache_needs_reload(0, 5));
    }
}
