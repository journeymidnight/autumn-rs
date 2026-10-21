//! Inode metadata operations: getattr, setattr, create, unlink.
//!
//! All functions run on the compio thread, accessing ClusterClient directly.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};

use crate::key;
use crate::schema::{self, inode_alloc_batch, InodeMeta, ROOT_INO};
use crate::state::FsState;

// `libc::S_IF*` widths differ across libc targets — `u16` on some (e.g.
// CI's ubuntu-latest libc bindings), `u32` on others (glibc x86_64). Our
// `InodeMeta.mode` is `u32` everywhere, so normalise the libc constants
// once here and use these `u32` aliases from every call site (meta.rs +
// dispatch.rs). `#[allow]` covers the host where the cast is a no-op.
// `pub` (M1): these are the persisted `InodeMeta.mode` format
// bits — part of the core's public vocabulary (the fuse-gated `attr.rs`
// and the M2 PyO3 binding both classify inodes with them).
#[allow(clippy::unnecessary_cast)]
pub const S_IFMT: u32 = libc::S_IFMT as u32;
#[allow(clippy::unnecessary_cast)]
pub const S_IFDIR: u32 = libc::S_IFDIR as u32;
#[allow(clippy::unnecessary_cast)]
pub const S_IFLNK: u32 = libc::S_IFLNK as u32;
#[allow(clippy::unnecessary_cast)]
pub const S_IFREG: u32 = libc::S_IFREG as u32;

// M1: `inode_to_attr` / `mode_to_filetype` (the InodeMeta →
// fuser::FileAttr conversions) moved to `attr.rs` — the fuse-gated reply
// boundary. This module is part of the fuser-free FS core.

/// Current timestamp.
pub fn now_ts() -> (i64, u32) {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (d.as_secs() as i64, d.subsec_nanos())
}

/// The content generation of a newly created file.
pub const FIRST_GENERATION: u64 = 1;

/// Create a new regular file inode.
pub fn new_file_meta(mode: u32, uid: u32, gid: u32) -> InodeMeta {
    let (secs, nsecs) = now_ts();
    InodeMeta {
        mode: S_IFREG | (mode & 0o7777),
        uid,
        gid,
        size: 0,
        nlink: 1,
        atime_secs: secs,
        atime_nsecs: nsecs,
        mtime_secs: secs,
        mtime_nsecs: nsecs,
        ctime_secs: secs,
        ctime_nsecs: nsecs,
        inline_data: None,
        symlink_target: None,
        stripe: None,
        generation: FIRST_GENERATION,
        segments: None,
    }
}

/// Create a new directory inode.
pub fn new_dir_meta(mode: u32, uid: u32, gid: u32) -> InodeMeta {
    let (secs, nsecs) = now_ts();
    InodeMeta {
        mode: S_IFDIR | (mode & 0o7777),
        uid,
        gid,
        size: 0,
        nlink: 2, // . and parent link
        atime_secs: secs,
        atime_nsecs: nsecs,
        mtime_secs: secs,
        mtime_nsecs: nsecs,
        ctime_secs: secs,
        ctime_nsecs: nsecs,
        inline_data: None,
        symlink_target: None,
        stripe: None,
        generation: 1,
        segments: None,
    }
}

/// Create the root directory inode (`ROOT_INO`) if it does not exist yet.
/// Returns `true` iff it created the root (the filesystem was fresh).
///
/// Idempotent + fail-loud: a genuinely-absent root is created; a hard KV
/// error PROPAGATES (never silently recreates the root over a transient
/// failure via `kv_get`'s absent-vs-error conflation). Does NOT seed the
/// local inode-alloc cursor — every inode is granted by the manager
/// (M0), so this is safe for any co-equal writer (the fuse mount's
/// `init_root` layers its legacy pre-manager batch seed on top).
pub async fn ensure_root(state: &mut FsState) -> Result<bool> {
    // SD-3: verify/stamp the volume's layout version BEFORE any
    // root read/write, so an incompatible layout refuses to mount up front.
    ensure_schema_version(state).await?;
    let root_key = key::inode_key(ROOT_INO);
    if state.kv_get_opt(&root_key).await?.is_some() {
        return Ok(false);
    }
    let root_meta = new_dir_meta(0o755, unsafe { libc::getuid() }, unsafe { libc::getgid() });
    put_new_inode(state, ROOT_INO, &root_meta).await?;
    Ok(true)
}

/// verify (or stamp) this filesystem's on-disk layout version, kept at
/// `fs/[0x04]schema_version`.
///
/// - **absent** → a fresh volume: stamp the current `SCHEMA_VERSION`.
/// - **present, matches** → OK.
/// - **present, differs (or malformed)** → FAIL LOUD: refuse to mount a layout
///   this binary can't interpret rather than silently reading/writing garbage.
///
/// Fail-loud on a transient KV error too — `kv_get_opt` propagates hard errors
/// (never conflates "absent" with "unreachable", which would re-stamp a real
/// volume as fresh).
pub async fn ensure_schema_version(state: &mut FsState) -> Result<()> {
    let k = key::schema_version_key();
    match state.kv_get_opt(&k).await? {
        None => {
            // "No stamp" must mean a genuinely fresh filesystem, not a tree
            // some other writer populated without ever stamping it: the `autumnfs` CLI and the S3 gateway did not stamp
            // before v4, so a tree they alone built holds v3 inodes and no
            // stamp. Stamping the current version over those would declare
            // them readable as a layout they are not.
            let inodes = state.kv_range_keys(&[0x01], &[0x01], 1).await?;
            if !inodes.is_empty() {
                return Err(anyhow!(
                    "fs/ holds inodes but no schema stamp — it was built by a tool that \
                     did not stamp (autumnfs or the S3 gateway before v4), so its \
                     inodes are v3. Refusing to stamp v{} over them; use the \
                     migration procedure for the release that wrote this data.",
                    schema::SCHEMA_VERSION
                ));
            }
            state
                .kv_put(&k, &schema::SCHEMA_VERSION.to_be_bytes())
                .await
                .context("stamp schema_version on fresh filesystem")?;
            Ok(())
        }
        Some(v) => {
            let found: u64 = v
                .as_slice()
                .try_into()
                .map(u64::from_be_bytes)
                .map_err(|_| {
                    anyhow!(
                        "schema_version holds {} bytes, want 8 (BE u64) — refusing to mount",
                        v.len()
                    )
                })?;
            if found == schema::SCHEMA_VERSION {
                Ok(())
            } else {
                Err(anyhow!(
                    "on-disk fs schema version {} != supported {} — refusing to mount \
                     (incompatible layout; wipe/re-create this volume or use a matching build)",
                    found,
                    schema::SCHEMA_VERSION
                ))
            }
        }
    }
}

/// Fetch inode metadata from the cache, or from KV when it is not cached or
/// an invalidation has overtaken the cached copy (`FsState::meta_invalidated`).
///
/// The cached copy stays authoritative while this session holds the inode's
/// write lease (not revoked) or has it dirty: nobody else can change the file
/// then, so a mark (a `WillRevokeIn` to us) says nothing about its content —
/// and this session's size may be ahead of KV's (`dirty` clears only once the
/// flush's put landed). Adopting KV's smaller size there
/// is the data loss `get_inode_uncached` describes. The mark is taken BEFORE
/// the read, so an event arriving during it marks the inode again; a failed
/// read puts it back and returns the error.
pub async fn get_inode(state: &mut FsState, ino: u64) -> Result<InodeMeta> {
    if let Some(is) = state.inodes.get(&ino) {
        let own_writer = state
            .held_leases
            .borrow()
            .get(&ino)
            .is_some_and(|l| l.mode == autumn_rpc::manager_rpc::LEASE_MODE_WRITE && !l.revoked);
        if is.dirty || own_writer || !state.meta_invalidated.borrow().contains(&ino) {
            return Ok(is.meta.clone());
        }
    }
    let marked = state.meta_invalidated.borrow_mut().remove(&ino);
    let fresh = match fetch_inode(state, ino).await {
        Ok(m) => m,
        Err(e) => {
            if marked {
                state.meta_invalidated.borrow_mut().insert(ino);
            }
            return Err(e);
        }
    };
    if let Some(is) = state.inodes.get_mut(&ino) {
        is.meta = fresh.clone();
        // The striped extent map is computed from size; drop it with the meta.
        is.extents = None;
    }
    Ok(fresh)
}

/// Read inode metadata from KV, ignoring and not touching the cache.
pub async fn fetch_inode(state: &mut FsState, ino: u64) -> Result<InodeMeta> {
    let value = state.kv_get(&key::inode_key(ino)).await?;
    schema::decode_inode_meta(&value)
        .map_err(|e| anyhow!("decode InodeMeta for ino {}: {}", ino, e))
}

/// Re-read inode metadata from KV, BYPASSING the cache, and refresh the cached
/// copy ONLY when KV is LARGER.
///
/// The cache is pinned across opens by the kernel's dentry refcount, so a meta
/// captured once — mid-upload, say — survives every reopen until the kernel
/// FORGETs the dentry. That is how a mount comes to believe a 5 GB file is
/// 64 MiB and serves clean EOF against it for twenty minutes. Callers use this
/// where believing a stale size CORRUPTS the answer rather than merely dating
/// it. Returns the fresh meta and whether the cache was refreshed — which is
/// only ever the grew-in-KV direction; see the comment on the check itself for
/// why the other direction must be refused.
pub async fn get_inode_uncached(state: &mut FsState, ino: u64) -> Result<(InodeMeta, bool)> {
    let fresh = fetch_inode(state, ino).await?;
    let mut changed = false;
    if let Some(is) = state.inodes.get_mut(&ino) {
        // ONLY adopt a LARGER size, and report only that as `changed`.
        //
        // This helper exists to catch a cache that is stale-SMALL — the shape
        // that makes `read` clamp a live file to a premature EOF — so that is
        // the one direction it may correct.
        //
        // The other direction is NOT "never stale": another mount truncating
        // the file leaves this cache genuinely, wrongly large. The asymmetry is
        // in the CONSEQUENCES, not in the evidence. A cache larger than KV is
        // also the ordinary state of a file being written through this mount,
        // because extents land long before the size covering them is published
        // (fsync / release) — so the two cases are indistinguishable here, and
        // the tie must break toward the one that cannot destroy anything.
        // Refusing a shrink costs a stale `getattr` until the lease-version
        // reload or FORGET, and cannot change what any read answers: the only
        // caller is the EOF confirm, which already established
        // `offset >= size`, so the clamp returns empty either way. Adopting a
        // shrink, by contrast, does this:
        //
        // Adopting the smaller KV size there does not just date this read, it
        // destroys data. The next write sees `cur_size < offset`, calls
        // `clean_beyond_eof`, and DELETES every extent this inode has already
        // landed; with no further write, `flush_inode` publishes the shrunken
        // size and the file closes at 0. Gating on `is.dirty` would not be
        // enough either — a size can be unpublished while the inode is
        // momentarily clean — so the guard is the direction itself.
        //
        // Scope, so this is not read as more than it is: it closes the SHRINK
        // direction only. The grow arm below still adopts `fresh` wholesale
        // onto a possibly-dirty inode, so if two mounts could ever grow the
        // same inode concurrently, this would publish the other's meta as
        // ours. Exclusive per-inode write leases are what make that
        // unreachable — not this check.
        if fresh.size > is.meta.size {
            changed = true;
            is.meta = fresh.clone();
            // The striped extent map is COMPUTED from size, so a stale size
            // yields a self-consistently stale map. Drop it with the meta.
            is.extents = None;
        }
    }
    Ok((fresh, changed))
}

/// Writes `meta` over inode `ino`'s stored metadata — every field but the
/// link count, which only `change_links` moves: the stored count is kept, by
/// a compare-and-set on the bytes read. A whole-inode write from a cached
/// copy (a flush, a truncate, a setattr) used to put back a count another
/// session had just changed, and an unlink then removed data a link still
/// named. An inode that is gone stays gone (ENOENT). Fenced like any write
/// under a held WRITE lease. Returns the inode as written.
pub async fn put_inode(state: &mut FsState, ino: u64, meta: &InodeMeta) -> Result<InodeMeta> {
    let k = key::inode_key(ino);
    let lease = state.write_lease_for(ino);
    for _ in 0..16 {
        let Some(stored) = state.kv_get_opt(&k).await? else {
            return Err(anyhow!("ENOENT: inode {ino} is gone"));
        };
        let current = schema::decode_inode_meta(&stored).map_err(|e| anyhow!("inode {ino}: {e}"))?;
        let mut next = meta.clone();
        next.nlink = current.nlink;
        // Never older than a link change made since the caller's copy.
        if (current.ctime_secs, current.ctime_nsecs) > (next.ctime_secs, next.ctime_nsecs) {
            next.ctime_secs = current.ctime_secs;
            next.ctime_nsecs = current.ctime_nsecs;
        }
        let bytes = schema::encode_inode_meta(&next);
        // An attempt whose outcome is unknown is settled by reading again:
        // these bytes read back means it landed, and an attempt still in
        // flight expected bytes that are then gone.
        let written = bytes == stored
            || match state.client.compare_write(&k, Some(&stored), Some(&bytes), lease).await {
                Ok(w) => w,
                Err(autumn_client::AutumnError::OutcomeUnknown(_)) => false,
                Err(e) => return Err(anyhow::Error::new(e).context("KV put")),
            };
        if written {
            if let Some(is) = state.inodes.get_mut(&ino) {
                is.meta = next.clone();
            }
            return Ok(next);
        }
    }
    Err(anyhow!("EBUSY: inode {ino} kept changing"))
}

/// Writes the metadata of an inode nobody else can name yet (just
/// allocated, or the root on a fresh volume).
pub async fn put_new_inode(state: &mut FsState, ino: u64, meta: &InodeMeta) -> Result<()> {
    let lease = state.write_lease_for(ino);
    state.kv_put_fenced(&key::inode_key(ino), &schema::encode_inode_meta(meta), lease).await?;
    if let Some(is) = state.inodes.get_mut(&ino) {
        is.meta = meta.clone();
    }
    Ok(())
}

/// Moves inode `ino`'s link count by `delta` (and its ctime) with a
/// compare-and-set on its stored bytes; the inode as written. Refuses
/// (ENOENT) to add a link to an inode that is gone or has none left: its
/// data is being removed. The caller that takes the count to 0 removes it.
/// Not fenced: a link count is namespace state, like the name it follows,
/// and a stale write lease of this mount must not strand a removed name's
/// count.
pub async fn change_links(state: &mut FsState, ino: u64, delta: i32) -> Result<InodeMeta> {
    let k = key::inode_key(ino);
    // An attempt whose outcome is unknown is settled by reading again. Each
    // value sent carries a fresh ctime, so reading one back means that attempt
    // landed; reading what it expected means none landed, and another try
    // (expecting the same bytes) excludes it. Anything else may be a landing
    // already overwritten by another writer: applying the change again could
    // count a link down twice and reclaim a named file, so that is reported,
    // a count left high rather than low.
    let mut sent: Vec<(Vec<u8>, InodeMeta)> = Vec::new();
    let mut unknown_expected: Option<Vec<u8>> = None;
    for _ in 0..16 {
        let Some(stored) = state.kv_get_opt(&k).await? else {
            return Err(anyhow!("ENOENT: inode {ino} is gone"));
        };
        if let Some((_, m)) = sent.iter().find(|(v, _)| *v == stored) {
            let m = m.clone();
            note_links(state, ino, &m);
            return Ok(m);
        }
        if unknown_expected.as_ref().is_some_and(|e| *e != stored) {
            return Err(anyhow!("link count of inode {ino}: outcome unknown"));
        }
        let mut m = schema::decode_inode_meta(&stored).map_err(|e| anyhow!("inode {ino}: {e}"))?;
        if delta > 0 && m.nlink == 0 {
            return Err(anyhow!("ENOENT: inode {ino} has no link left"));
        }
        m.nlink = u32::try_from((m.nlink as i64 + delta as i64).max(0)).map_err(|_| anyhow!("EMLINK"))?;
        let (s, ns) = now_ts();
        m.ctime_secs = s;
        m.ctime_nsecs = ns;
        let value = schema::encode_inode_meta(&m);
        let anon = autumn_client::WriteLease::ANON;
        let result = state.client.compare_write(&k, Some(&stored), Some(&value), anon).await;
        match lost_reply_for_test(result).await {
            Ok(true) => {
                note_links(state, ino, &m);
                return Ok(m);
            }
            Ok(false) => {}
            Err(autumn_client::AutumnError::OutcomeUnknown(_)) => {
                sent.push((value, m));
                unknown_expected = Some(stored);
            }
            Err(e) => return Err(anyhow::Error::new(e).context("KV put")),
        }
    }
    Err(anyhow!("EBUSY: inode {ino} kept changing"))
}

/// Test-only failpoint: 1 = armed; the next `change_links` attempt that
/// lands takes it (2), reports its outcome as unknown — a reply lost after
/// the write — and parks until the test stores 0. One relaxed CAS per
/// attempt.
#[doc(hidden)]
pub static CHANGE_LINKS_TEST_LOST_REPLY: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

async fn lost_reply_for_test(
    result: std::result::Result<bool, autumn_client::AutumnError>,
) -> std::result::Result<bool, autumn_client::AutumnError> {
    use std::sync::atomic::Ordering::Relaxed;
    if !matches!(result, Ok(true)) || CHANGE_LINKS_TEST_LOST_REPLY.compare_exchange(1, 2, Relaxed, Relaxed).is_err() {
        return result;
    }
    while CHANGE_LINKS_TEST_LOST_REPLY.load(Relaxed) == 2 {
        compio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    Err(autumn_client::AutumnError::OutcomeUnknown("lost reply (test)".into()))
}

fn note_links(state: &mut FsState, ino: u64, m: &InodeMeta) {
    if let Some(is) = state.inodes.get_mut(&ino) {
        is.meta.nlink = m.nlink;
        is.meta.ctime_secs = m.ctime_secs;
        is.meta.ctime_nsecs = m.ctime_nsecs;
    }
}

/// Allocate a new inode number from the batch allocator.
///
/// M0: batches are granted by the MANAGER
/// (`ClusterClient::alloc_inodes` — leader-fenced etcd CAS), replacing the
/// pre-M0 non-CAS read-modify-write on the `[0x04]next_inode` fs KV key,
/// which handed out duplicate batches under concurrent allocators (two
/// mounts, or a mount + the Python `autumn.Fs` client).
pub async fn alloc_inode(state: &mut FsState) -> Result<u64> {
    if state.next_inode < state.inode_batch_end {
        let ino = state.next_inode;
        state.next_inode += 1;
        return Ok(ino);
    }
    // Batch refill. The legacy KV counter is passed as the migration FLOOR:
    // a pre-M0 filesystem stored its cursor there, so the manager's grant
    // never re-issues an inode below it. Missing/short key → no floor.
    let k = key::next_inode_key();
    let floor = match state.kv_get(&k).await {
        Ok(v) if v.len() == 8 => u64::from_be_bytes(v[..8].try_into().unwrap()),
        _ => ROOT_INO + 1,
    };
    // Pass an EMPTY volume so the manager grants from its single GLOBAL inode
    // counter — inodes stay cluster-unique. The lease/fence plane (manager
    // `inode_leases/<ino>`, PS `fence_floors`, `WriteLease.inode_hint`) keys by
    // BARE ino, so a global counter is the right granularity. The
    // `floor` above is this tree's own `[0x04]next_inode` cursor — harmless
    // with a global grant since `max(cur,floor)` only ever RAISES the shared
    // counter, never rewinds it.
    let base = state
        .client
        .alloc_inodes(inode_alloc_batch() as u32, floor)
        .await
        .context("alloc_inodes from manager")?;
    // Best-effort: keep the legacy KV cursor roughly current so a disaster
    // rebuild (etcd state lost, fs data preserved) re-migrates from a recent
    // floor instead of the pre-M0 value. MONOTONIC-GUARDED (coco P2): re-read
    // and only write when our end is higher, so a slow allocator's smaller
    // batch can't overwrite a concurrent allocator's larger cursor. A
    // read↔put race window remains (plain KV, no CAS) — which is why this
    // key is advisory freshness only, NEVER a trusted disaster floor: a
    // real etcd-lost recovery must offline-scan max(ino) instead. The
    // manager counter stays authoritative while etcd lives (grants use
    // max(counter, floor), so a stale floor can't rewind it).
    let new_end = base + inode_alloc_batch();
    let cur_cursor = match state.kv_get(&k).await {
        Ok(v) if v.len() == 8 => u64::from_be_bytes(v[..8].try_into().unwrap()),
        _ => 0,
    };
    if new_end > cur_cursor {
        if let Err(e) = state.kv_put(&k, &new_end.to_be_bytes()).await {
            tracing::warn!(error = %e, "legacy next_inode cursor update failed (advisory)");
        }
    }
    state.next_inode = base;
    state.inode_batch_end = base + inode_alloc_batch();
    let ino = state.next_inode;
    state.next_inode += 1;
    Ok(ino)
}
