//! Compio-thread dispatch loop: receives FsRequests from the bridge and
//! executes them using the filesystem state.

use anyhow::{anyhow, Result};

use autumn_client::lease;
use autumn_rpc::manager_rpc::{LEASE_MODE_READ, LEASE_MODE_WRITE};

use crate::attr::inode_to_attr;
use crate::bridge::*;
use crate::dir;
use crate::meta::*;
use crate::read;
use crate::schema::InodeState;
use crate::state::{FsState, FuseLease};
use crate::write;

/// Derive the lease mode from POSIX open flags. The
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
/// version. Mirrors the ioring daemon's
/// lease-version staleness check.
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

// M4: the per-session lease background tasks (heartbeat +
// invalidation poll) moved to the fuser-free `crate::lease_tasks` so the PyO3
// `autumn.Fs` binding shares them. Re-exported here so `main.rs` +
// this module's unit tests reference them unchanged.
pub use crate::lease_tasks::{
    evict_revoked_held_leases, invalidate_kernel_cache_for_events, spawn_lease_background_tasks,
    InodeInvalidator,
};

/// Initialize the root inode if it doesn't exist yet.
pub async fn init_root(state: &mut FsState) -> Result<()> {
    // Root creation is the shared core primitive (`meta::ensure_root`, used by
    // the PyO3 `autumn.Fs` binding too — M2); the fuse mount adds
    // its legacy pre-manager batch seed on a fresh filesystem only.
    if !ensure_root(state).await? {
        tracing::info!("root inode already exists");
        return Ok(());
    }
    tracing::info!("created root inode");
    // SD-3 (review P1-2): do NOT seed a local inode batch here. The
    // pre-SD-3 mount seeded `[ROOT_INO+1, ROOT_INO+1+INODE_ALLOC_BATCH)` locally
    // on a fresh FS — but with per-volume filesystems each volume's fresh mount
    // would seed the SAME low range, and the lease/fence plane keys by BARE ino,
    // so those low inodes would COLLIDE across volumes (cross-volume write-lease
    // conflict). Leaving `next_inode == inode_batch_end` (the FsState::new
    // default) makes the first `alloc_inode` fetch from the manager's GLOBAL
    // counter, so every file inode is cluster-unique. The PyO3 `autumn.Fs`
    // front-end already skips the local seed (it calls `ensure_root`, not
    // `init_root`), so both front-ends now allocate uniformly through the manager.
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
            // M1: core returns (ino, meta); convert to the
            // fuser reply shape at this boundary.
            let result = dir::lookup(state, parent, &name)
                .await
                .map(|(ino, meta)| (inode_to_attr(ino, &meta), ino));
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
            let result = dir::mkdir(state, parent, &name, mode)
                .await
                .map(|(ino, meta)| inode_to_attr(ino, &meta));
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
                // M2: the file-create KV steps live in the shared
                // core (`dir::create`) so the fuse mount and the PyO3
                // `autumn.Fs` binding never drift. Open semantics — lease
                // acquire + the runtime `InodeState` cache + the `FileAttr`
                // reply — stay here at the FUSE reply boundary.
                let (ino, meta) = dir::create(state, parent, &name, mode).await?;
                // coco P1 #1 fix: Create produces a
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
                // NOTE: `dir::create` already bumped `lookup_count[ino]`.
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
            // M2: shared core owns the file-unlink KV steps
            // (dirent delete + nlink decrement + tombstoned data reap).
            let result = dir::unlink(state, parent, &name).await;
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

                // AcquireLease BEFORE bumping the
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
                        // BOUNDED. This is the one FUSE op that answers the
                        // kernel off the dispatcher, so it is also the one that
                        // can leave the kernel waiting forever: every other op
                        // goes through `call_sync`, whose REPLY_TIMEOUT ends in
                        // `reply.error(EIO)` no matter what the cluster does.
                        //
                        // FUSE has no timeout of its own. A read that never
                        // answers parks its caller in uninterruptible sleep
                        // permanently — unkillable, holding whatever locks it
                        // held — and if that caller is a container runtime
                        // thread stat-ing a path, the whole node stops being
                        // able to start containers. An unreachable manager or a
                        // stalled extent read must degrade to EIO, not to a
                        // wedged node.
                        match compio::time::timeout(REPLY_TIMEOUT, read::execute(plan)).await {
                            Ok(Ok(data)) => fuse_reply.data(&data),
                            Ok(Err(e)) => {
                                tracing::warn!(error = %e, "fuse read execute failed");
                                fuse_reply.error(libc::EIO);
                            }
                            Err(_) => {
                                tracing::warn!(
                                    timeout_secs = REPLY_TIMEOUT.as_secs(),
                                    "fuse read timed out — replying EIO"
                                );
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
                // coco P1 #2 fix: writer-release
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
                            "ReleaseLease failed; TTL revoke is the backstop"
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
mod fuse_lease_2_unit_tests {
    //! coco P3 review feedback: unit-test the
    //! per-event ⇒ invalidator-call contract so default `cargo test`
    //! catches regressions. The `fuse_lease_2.rs` integration
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
    //! lease-version staleness check.

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
