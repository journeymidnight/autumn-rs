//! Inode metadata operations: getattr, setattr, create, unlink.
//!
//! All functions run on the compio thread, accessing ClusterClient directly.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};

use crate::key;
use crate::schema::{self, InodeMeta, INODE_ALLOC_BATCH, ROOT_INO};
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
    put_inode(state, ROOT_INO, &root_meta).await?;
    Ok(true)
}

/// verify (or stamp) this filesystem's on-disk layout version.
/// Each tenant carries its own `[0x04]schema_version` (relative → it lives at
/// `fs/{tenant}/[0x04]schema_version`).
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
            // "No stamp" must mean a genuinely fresh filesystem — NOT an
            // un-migrated pre-`{volume}`-removal tree. The old SD-3 layout put
            // everything under `fs/{tenant}/{volume}/…`, i.e. relative keys that
            // begin with a volume-name byte (`[a-z0-9._-]`, all ≥ 0x2d). Our own
            // keys begin with a type byte (0x01–0x04). So if ANY relative key
            // ≥ 0x05 exists, this tenant holds old volume-scoped data that our
            // `fs/{tenant}/…` keys would SHADOW — mounting would silently show an
            // empty FS and write a second dataset. Refuse loudly instead
            // (the deploy is a stop-world reset; wipe or migrate first).
            let stragglers = state.kv_range_keys(b"", b"\x05", 1).await?;
            if !stragglers.is_empty() {
                return Err(anyhow!(
                    "fs/{{tenant}}/ has no schema stamp but holds keys from the \
                     pre-`{{volume}}`-removal layout (e.g. {:?}) — refusing to mount \
                     (it would shadow that data as an empty FS). Wipe/reset this \
                     tenant's fs keyspace or run against a matching build.",
                    String::from_utf8_lossy(&stragglers[0])
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

/// Fetch inode metadata from KV store (or cache).
pub async fn get_inode(state: &mut FsState, ino: u64) -> Result<InodeMeta> {
    // Check cache first
    if let Some(is) = state.inodes.get(&ino) {
        return Ok(is.meta.clone());
    }
    // Fetch from KV
    let k = key::inode_key(ino);
    let value = state.kv_get(&k).await?;
    let meta: InodeMeta = schema::decode_inode_meta(&value)
        .map_err(|e| anyhow!("decode InodeMeta for ino {}: {}", ino, e))?;
    Ok(meta)
}

/// Re-read inode metadata from KV, BYPASSING the cache, and refresh the cached
/// copy if it moved.
///
/// The cache is pinned across opens by the kernel's dentry refcount, so a meta
/// captured once — mid-upload, say — survives every reopen until the kernel
/// FORGETs the dentry. That is how a mount comes to believe a 5 GB file is
/// 64 MiB and serves clean EOF against it for twenty minutes. Callers use this
/// where believing a stale size CORRUPTS the answer rather than merely dating
/// it. Returns the fresh meta and whether it differed.
pub async fn get_inode_uncached(state: &mut FsState, ino: u64) -> Result<(InodeMeta, bool)> {
    let k = key::inode_key(ino);
    let value = state.kv_get(&k).await?;
    let fresh: InodeMeta = schema::decode_inode_meta(&value)
        .map_err(|e| anyhow!("decode InodeMeta for ino {}: {}", ino, e))?;
    let mut changed = false;
    if let Some(is) = state.inodes.get_mut(&ino) {
        if is.meta.size != fresh.size {
            changed = true;
            is.meta = fresh.clone();
            // The striped extent map is COMPUTED from size, so a stale size
            // yields a self-consistently stale map. Drop it with the meta.
            is.extents = None;
        }
    }
    Ok((fresh, changed))
}

/// Write inode metadata to KV store and update cache.
pub async fn put_inode(state: &mut FsState, ino: u64, meta: &InodeMeta) -> Result<()> {
    let k = key::inode_key(ino);
    let v = schema::encode_inode_meta(meta);
    // BUG-LEASE-2 Phase 2: inode-meta updates under a held WRITE lease are
    // fence-stamped too (size bumps from a revoked writer must not land).
    let lease = state.write_lease_for(ino);
    state.kv_put_fenced(&k, &v, lease).await?;
    // Update cache
    if let Some(is) = state.inodes.get_mut(&ino) {
        is.meta = meta.clone();
    }
    Ok(())
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
    // BARE ino, so a global counter is the right granularity. The manager's
    // per-volume counter machinery + the frozen `AllocInodesReq.volume` field
    // stay DORMANT (reserved for a future volume-aware-lease feature). The
    // `floor` above is this tenant's own `[0x04]next_inode` cursor — harmless
    // with a global grant since `max(cur,floor)` only ever RAISES the shared
    // counter, never rewinds it.
    let base = state
        .client
        .alloc_inodes(INODE_ALLOC_BATCH as u32, floor, b"")
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
    let new_end = base + INODE_ALLOC_BATCH;
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
    state.inode_batch_end = base + INODE_ALLOC_BATCH;
    let ino = state.next_inode;
    state.next_inode += 1;
    Ok(ino)
}
