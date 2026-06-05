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

/// F-fuse-lease-1 + F-fuse-lease-2: per-mount background tasks.
/// Spawn ONCE on the compio runtime right after `FsState::new`
/// (before the dispatch loop). Mirrors `autumn-ioring`'s
/// `session_heartbeat_loop` + `session_invalidation_poll_loop`:
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
                            slot.version = info.version;
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
                        // F-fuse-lease-2: drop the kernel's
                        // attribute + page cache for this ino so
                        // the next syscall reaches our dispatcher
                        // and re-reads the post-close bytes.
                        // ino=0 sentinel + non-positive inos are
                        // skipped (overflow path handles the
                        // wholesale case below; ino=0 isn't a real
                        // FUSE ino).
                        if ev.ino != 0 {
                            if let Some(inv) = &invalidator_p {
                                inv(ev.ino);
                            }
                        }
                    }
                    if wholesale {
                        tracing::warn!(
                            "F-fuse-lease-1: overflow sentinel; wholesale invalidating mount"
                        );
                        // Best-effort release of every held lease
                        // BEFORE we drop the local bookkeeping —
                        // mirrors autumn-ioring's pattern. Without
                        // this the manager would keep those writer
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
                if let Some(t) = atime {
                    match t {
                        fuser::TimeOrNow::SpecificTime(st) => {
                            let d = st.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                            meta.atime_secs = d.as_secs() as i64;
                            meta.atime_nsecs = d.subsec_nanos();
                        }
                        fuser::TimeOrNow::Now => {
                            let (s, ns) = now_ts();
                            meta.atime_secs = s;
                            meta.atime_nsecs = ns;
                        }
                    }
                }
                if let Some(t) = mtime {
                    match t {
                        fuser::TimeOrNow::SpecificTime(st) => {
                            let d = st.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                            meta.mtime_secs = d.as_secs() as i64;
                            meta.mtime_nsecs = d.subsec_nanos();
                        }
                        fuser::TimeOrNow::Now => {
                            let (s, ns) = now_ts();
                            meta.mtime_secs = s;
                            meta.mtime_nsecs = ns;
                        }
                    }
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
                match lease::acquire(&cluster, &id, ino, req_mode).await {
                    Ok(AcquireResult::Granted(info)) => {
                        state.held_leases.borrow_mut().insert(
                            ino,
                            FuseLease {
                                mode: req_mode,
                                refcount: 1,
                                version: info.version,
                            },
                        );
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
                    Err(e) => return Err(anyhow!("AcquireLease ino {}: {}", ino, e)),
                }
                // Cache the inode
                state.inodes.insert(
                    ino,
                    InodeState {
                        meta: meta.clone(),
                        write_buf: None,
                        dirty: false,
                        open_count: 1,
                        extents: None,
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
                    // Delete all data extents (F247 — variable-length, keyed by
                    // logical offset; range-scan rather than arithmetic).
                    crate::extent::delete_all_extents(state, dirent.child_inode).await?;
                    // Delete inode
                    let ik = key::inode_key(dirent.child_inode);
                    state.kv_delete(&ik).await?;
                    state.inodes.remove(&dirent.child_inode);
                    state.dirty_inodes.remove(&dirent.child_inode);
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
                // Ensure inode exists.
                let _ = get_inode(state, ino).await?;

                // F-fuse-lease-1: AcquireLease BEFORE bumping the
                // local open_count so a conflicting writer surfaces
                // as `EBUSY` (mapped to ErrorKind::Other in
                // `err_to_errno`). Refcount the lease per-mount —
                // same pattern as autumn-ioring's `held_leases`.
                let req_mode = lease_mode_for_open(flags);
                let mut needs_acquire = false;
                {
                    let mut m = state.held_leases.borrow_mut();
                    match m.get_mut(&ino) {
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
                if needs_acquire {
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
                                    version: info.version,
                                },
                            );
                        }
                        Ok(AcquireResult::Conflict { manager_message }) => {
                            return Err(anyhow!(
                                "EBUSY: writer lease conflict on ino {}: {}",
                                ino, manager_message
                            ));
                        }
                        Err(e) => return Err(anyhow!("AcquireLease ino {}: {}", ino, e)),
                    }
                }

                // Now publish to the per-inode cache.
                if let Some(is) = state.inodes.get_mut(&ino) {
                    is.open_count += 1;
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
            let result = write::write(state, ino, offset, &data).await;
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
                // dirty. So we ALWAYS flush before checking
                // whether this Release fires `ReleaseLease`, and
                // we ONLY proceed to ReleaseLease (refcount→0
                // transition) if the flush succeeded. A flush
                // failure keeps the writer lease alive so the
                // client / a retry / the TTL backstop preserves
                // the "writer-flush-before-release" invariant.
                let release_now_pred = {
                    state
                        .held_leases
                        .borrow()
                        .get(&ino)
                        .map(|s| s.refcount == 1)
                        .unwrap_or(false)
                };
                if flush || release_now_pred {
                    write::flush_inode(state, ino).await?;
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
                // fires ReleaseLease. Best-effort — a failed
                // release is recovered by the manager's TTL revoke
                // loop (≤ 30s window).
                let release_now = {
                    let mut m = state.held_leases.borrow_mut();
                    match m.get_mut(&ino) {
                        Some(slot) => {
                            slot.refcount = slot.refcount.saturating_sub(1);
                            if slot.refcount == 0 {
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
            let _ = reply.send(Ok(StatfsData {
                blocks: 1 << 30,
                bfree: 1 << 29,
                bavail: 1 << 29,
                files: 1 << 20,
                ffree: 1 << 19,
                bsize: 4096,
                namelen: 255,
            }));
        }
    }
    true // continue processing
}
