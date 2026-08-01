//! Directory operations: lookup, readdir, mkdir, rmdir, rename.

use std::ffi::OsStr;

use anyhow::{anyhow, Result};

use crate::key;
use crate::meta::*;
use crate::schema::{self, DirentValue, InodeMeta, ReaddirEntry, DT_DIR, DT_REG, ROOT_INO};
use crate::state::FsState;

/// Look up a child entry WITHOUT touching the FUSE lookup refcount, returning
/// `Ok(None)` for a genuinely-absent name and `Err` for a hard KV/decode error.
///
/// Uses the `kv_get_opt` barrier so a transient KV/routing failure does NOT
/// masquerade as "not found" — this is the barrier-correct primitive the PyO3
/// `autumn.Fs` binding (M2) needs (a false miss would let a facade
/// wrongly `create`/`rename`-over on a transient error). The FUSE `lookup`
/// layers the ENOENT mapping + lookup-count bump on top.
pub async fn lookup_opt(
    state: &mut FsState,
    parent: u64,
    name: &OsStr,
) -> Result<Option<(u64, InodeMeta)>> {
    let k = key::dirent_key(parent, name.as_encoded_bytes());
    let v = match state.kv_get_opt(&k).await? {
        Some(v) => v,
        None => return Ok(None),
    };
    let dirent: DirentValue = schema::decode_dirent(&v).map_err(|e| anyhow!("{}", e))?;
    let meta = get_inode(state, dirent.child_inode).await?;
    Ok(Some((dirent.child_inode, meta)))
}

/// Lookup a child entry in a directory. Returns `(ino, meta)` — the FUSE
/// layer converts to `FileAttr` at the reply boundary (M1). A miss
/// (or, pre-M2-behavior-preserving, any error mapped by `ops.rs` to ENOENT)
/// is `Err("ENOENT")`.
pub async fn lookup(state: &mut FsState, parent: u64, name: &OsStr) -> Result<(u64, InodeMeta)> {
    match lookup_opt(state, parent, name).await? {
        Some((ino, meta)) => {
            *state.lookup_count.entry(ino).or_insert(0) += 1;
            Ok((ino, meta))
        }
        None => Err(anyhow!("ENOENT")),
    }
}

/// Resolve an absolute path to its inode by walking dirents from `ROOT_INO`.
///
/// M2: the FUSE kernel always supplies a parent inode, so the mount
/// path never needs this; the PyO3 `autumn.Fs` binding (and the M3 fsspec
/// facade) work in `str` paths and need a path→ino walk. Empty components and
/// `"."` are skipped. `".."` is rejected — the lean POSIX surface (Q5) does not
/// track parent pointers, and fsspec normalizes paths before calling us.
///
/// Returns `Ok(None)` when a component is genuinely absent (ENOENT); a hard
/// KV/routing error surfaces as `Err` (via the `kv_get_opt` barrier — a
/// transient failure must NOT be mistaken for "path does not exist").
pub async fn resolve(state: &mut FsState, path: &str) -> Result<Option<u64>> {
    let mut ino = ROOT_INO;
    for comp in path.split('/') {
        if comp.is_empty() || comp == "." {
            continue;
        }
        if comp == ".." {
            return Err(anyhow!("unsupported '..' component in path"));
        }
        let dk = key::dirent_key(ino, comp.as_bytes());
        let v = match state.kv_get_opt(&dk).await? {
            Some(v) => v,
            None => return Ok(None),
        };
        let dirent: DirentValue = schema::decode_dirent(&v).map_err(|e| anyhow!("{}", e))?;
        ino = dirent.child_inode;
    }
    Ok(Some(ino))
}

/// Read directory entries.
pub async fn readdir(state: &mut FsState, ino: u64, offset: i64) -> Result<Vec<ReaddirEntry>> {
    let mut entries = Vec::new();

    if offset <= 0 {
        entries.push(ReaddirEntry {
            ino,
            offset: 1,
            kind: DT_DIR,
            name: ".".into(),
        });
    }
    if offset <= 1 {
        entries.push(ReaddirEntry {
            ino,
            offset: 2,
            kind: DT_DIR,
            name: "..".into(),
        });
    }

    let prefix = key::dirent_prefix(ino);
    let keys = state.kv_range_keys(&prefix, &prefix, 4096).await?;

    for (i, k) in keys.into_iter().enumerate() {
        let entry_offset = (i as i64) + 3;
        if entry_offset <= offset {
            continue;
        }

        let (_, name_bytes) = match key::parse_dirent_key(&k) {
            Some(parsed) => (parsed.0, parsed.1.to_vec()),
            None => continue,
        };

        let v = match state.kv_get(&k).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        let dirent: DirentValue = match schema::decode_dirent(&v) {
            Ok(d) => d,
            Err(_) => continue,
        };
        let name = unsafe { std::ffi::OsString::from_encoded_bytes_unchecked(name_bytes) };

        entries.push(ReaddirEntry {
            ino: dirent.child_inode,
            offset: entry_offset,
            kind: dirent.file_type,
            name,
        });
    }

    Ok(entries)
}

/// Create a directory. Returns `(ino, meta)` — the FUSE layer converts to
/// `FileAttr` at the reply boundary (M1).
pub async fn mkdir(
    state: &mut FsState,
    parent: u64,
    name: &OsStr,
    mode: u32,
) -> Result<(u64, InodeMeta)> {
    let name_bytes = name.as_encoded_bytes();
    let dk = key::dirent_key(parent, name_bytes);
    if state.kv_exists(&dk).await.unwrap_or(false) {
        return Err(anyhow!("EEXIST"));
    }

    let ino = alloc_inode(state).await?;
    let meta = new_dir_meta(mode, unsafe { libc::getuid() }, unsafe { libc::getgid() });
    put_inode(state, ino, &meta).await?;

    let dirent = DirentValue {
        child_inode: ino,
        file_type: DT_DIR,
    };
    let dv = schema::encode_dirent(&dirent);
    state.kv_put(&dk, &dv).await?;

    let mut parent_meta = get_inode(state, parent).await?;
    parent_meta.nlink += 1;
    let (s, ns) = now_ts();
    parent_meta.mtime_secs = s;
    parent_meta.mtime_nsecs = ns;
    parent_meta.ctime_secs = s;
    parent_meta.ctime_nsecs = ns;
    put_inode(state, parent, &parent_meta).await?;

    *state.lookup_count.entry(ino).or_insert(0) += 1;
    Ok((ino, meta))
}

/// Create an empty regular file. Returns `(ino, meta)` — the FUSE layer
/// converts to `FileAttr` and drives the open-lease at the reply boundary;
/// the PyO3 `autumn.Fs` binding uses the plain `(ino, meta)` directly
/// (M2). This is the single source of the file-create KV steps
/// (`dispatch.rs` Create calls it, then layers on lease acquire + the
/// FUSE-runtime `InodeState` cache) — no drift between the two front-ends.
///
/// NOTE: does NOT acquire a lease and does NOT seed the runtime `InodeState`
/// cache; those are front-end concerns (fuse Open semantics / M4 facade
/// lease-on-open).
pub async fn create(
    state: &mut FsState,
    parent: u64,
    name: &OsStr,
    mode: u32,
) -> Result<(u64, InodeMeta)> {
    let name_bytes = name.as_encoded_bytes();
    let dk = key::dirent_key(parent, name_bytes);
    if state.kv_exists(&dk).await.unwrap_or(false) {
        return Err(anyhow!("EEXIST"));
    }

    let ino = alloc_inode(state).await?;
    let meta = new_file_meta(mode, unsafe { libc::getuid() }, unsafe { libc::getgid() });
    put_inode(state, ino, &meta).await?;

    let dirent = DirentValue {
        child_inode: ino,
        file_type: DT_REG,
    };
    let dv = schema::encode_dirent(&dirent);
    state.kv_put(&dk, &dv).await?;

    let mut parent_meta = get_inode(state, parent).await?;
    let (s, ns) = now_ts();
    parent_meta.mtime_secs = s;
    parent_meta.mtime_nsecs = ns;
    put_inode(state, parent, &parent_meta).await?;

    *state.lookup_count.entry(ino).or_insert(0) += 1;
    Ok((ino, meta))
}

/// Remove a directory (must be empty).
pub async fn rmdir(state: &mut FsState, parent: u64, name: &OsStr) -> Result<()> {
    let name_bytes = name.as_encoded_bytes();
    let dk = key::dirent_key(parent, name_bytes);
    let v = state.kv_get(&dk).await.map_err(|_| anyhow!("ENOENT"))?;
    let dirent: DirentValue = schema::decode_dirent(&v).map_err(|e| anyhow!("{}", e))?;

    if dirent.file_type != DT_DIR {
        return Err(anyhow!("ENOTDIR"));
    }

    let prefix = key::dirent_prefix(dirent.child_inode);
    let children = state.kv_range_keys(&prefix, &prefix, 1).await?;
    if !children.is_empty() {
        return Err(anyhow!("ENOTEMPTY"));
    }

    state.kv_delete(&dk).await?;
    let ik = key::inode_key(dirent.child_inode);
    state.kv_delete(&ik).await?;

    let mut parent_meta = get_inode(state, parent).await?;
    if parent_meta.nlink > 2 {
        parent_meta.nlink -= 1;
    }
    let (s, ns) = now_ts();
    parent_meta.mtime_secs = s;
    parent_meta.mtime_nsecs = ns;
    parent_meta.ctime_secs = s;
    parent_meta.ctime_nsecs = ns;
    put_inode(state, parent, &parent_meta).await?;

    state.inodes.remove(&dirent.child_inode);
    Ok(())
}

/// Unlink a regular file (drop one name; reap data when it becomes
/// unreachable). Single source of the file-unlink KV steps — `dispatch.rs`
/// Unlink calls this (M2), as does the PyO3 `autumn.Fs` binding.
///
/// UNLINK-1: the `nlink == 0` transition removes the inode's extents through
/// `extent::remove_unreachable_inode` (tombstoned so a crash mid-removal is
/// replayed at next mount rather than leaking data KVs forever).
pub async fn unlink(state: &mut FsState, parent: u64, name: &OsStr) -> Result<()> {
    let name_bytes = name.as_encoded_bytes();
    let dk = key::dirent_key(parent, name_bytes);
    let v = state.kv_get(&dk).await.map_err(|_| anyhow!("ENOENT"))?;
    let dirent: DirentValue = schema::decode_dirent(&v).map_err(|e| anyhow!("{}", e))?;
    if dirent.file_type == DT_DIR {
        return Err(anyhow!("EISDIR"));
    }

    // Delete dirent, then decrement nlink.
    state.kv_delete(&dk).await?;
    let mut meta = get_inode(state, dirent.child_inode).await?;
    meta.nlink = meta.nlink.saturating_sub(1);
    if meta.nlink == 0 {
        // The inode just became UNREACHABLE — remove its data under an intent
        // tombstone so a crash mid-removal is replayed at next mount instead
        // of leaking the extents forever.
        crate::extent::remove_unreachable_inode(state, dirent.child_inode).await?;
    } else {
        put_inode(state, dirent.child_inode, &meta).await?;
    }

    let mut parent_meta = get_inode(state, parent).await?;
    let (s, ns) = now_ts();
    parent_meta.mtime_secs = s;
    parent_meta.mtime_nsecs = ns;
    put_inode(state, parent, &parent_meta).await?;
    Ok(())
}

/// Rename a file or directory.
pub async fn rename(
    state: &mut FsState,
    old_parent: u64,
    old_name: &OsStr,
    new_parent: u64,
    new_name: &OsStr,
) -> Result<()> {
    let old_name_bytes = old_name.as_encoded_bytes();
    let new_name_bytes = new_name.as_encoded_bytes();

    let old_dk = key::dirent_key(old_parent, old_name_bytes);
    let v = state.kv_get(&old_dk).await.map_err(|_| anyhow!("ENOENT"))?;
    let old_dirent: DirentValue = schema::decode_dirent(&v).map_err(|e| anyhow!("{}", e))?;

    // Remember an overwritten file target — its removal runs AFTER the
    // dirent overwrite below makes it unreachable.
    let new_dk = key::dirent_key(new_parent, new_name_bytes);
    let mut replaced_file_ino: Option<u64> = None;
    if let Ok(tv) = state.kv_get(&new_dk).await {
        if let Ok(target_dirent) = schema::decode_dirent(&tv) {
            // POSIX: when old and new resolve to the SAME file (same
            // path, or two hard links of one inode), rename succeeds and
            // performs NO other action (coco P1: treating the source's
            // own inode as a "replaced target" decremented its nlink and
            // — post-UNLINK-1 — destroyed its data).
            if target_dirent.child_inode == old_dirent.child_inode {
                return Ok(());
            }
            if target_dirent.file_type != DT_DIR {
                replaced_file_ino = Some(target_dirent.child_inode);
            }
        }
    }

    state.kv_delete(&old_dk).await?;

    let dv = schema::encode_dirent(&old_dirent);
    state.kv_put(&new_dk, &dv).await?;

    // UNLINK-1: rename-over-existing drops the target's last name. The
    // pre-fix code deleted the target INODE but never its EXTENTS — the
    // POSIX atomic-save pattern (`write tmp; mv tmp file`) leaked the
    // ENTIRE previous file content on every save. Now the target goes
    // through the same tombstoned removal as unlink, placed AFTER the
    // dirent overwrite (the unreachability point — a tombstone for a
    // still-reachable inode would let the sweep destroy a live file).
    if let Some(t_ino) = replaced_file_ino {
        if let Ok(mut target_meta) = get_inode(state, t_ino).await {
            target_meta.nlink = target_meta.nlink.saturating_sub(1);
            if target_meta.nlink == 0 {
                crate::extent::remove_unreachable_inode(state, t_ino).await?;
            } else {
                put_inode(state, t_ino, &target_meta).await?;
            }
        }
    }

    let (s, ns) = now_ts();
    if old_parent == new_parent {
        let mut pmeta = get_inode(state, old_parent).await?;
        pmeta.mtime_secs = s;
        pmeta.mtime_nsecs = ns;
        put_inode(state, old_parent, &pmeta).await?;
    } else if old_dirent.file_type == DT_DIR {
        let mut old_pmeta = get_inode(state, old_parent).await?;
        old_pmeta.nlink = old_pmeta.nlink.saturating_sub(1);
        old_pmeta.mtime_secs = s;
        old_pmeta.mtime_nsecs = ns;
        put_inode(state, old_parent, &old_pmeta).await?;

        let mut new_pmeta = get_inode(state, new_parent).await?;
        new_pmeta.nlink += 1;
        new_pmeta.mtime_secs = s;
        new_pmeta.mtime_nsecs = ns;
        put_inode(state, new_parent, &new_pmeta).await?;
    } else {
        let mut old_pmeta = get_inode(state, old_parent).await?;
        old_pmeta.mtime_secs = s;
        old_pmeta.mtime_nsecs = ns;
        put_inode(state, old_parent, &old_pmeta).await?;

        let mut new_pmeta = get_inode(state, new_parent).await?;
        new_pmeta.mtime_secs = s;
        new_pmeta.mtime_nsecs = ns;
        put_inode(state, new_parent, &new_pmeta).await?;
    }

    Ok(())
}

// M1: `dt_to_filetype` moved to `attr.rs` (fuse-gated reply
// boundary) — readdir entries carry the raw `DT_*` byte now.
