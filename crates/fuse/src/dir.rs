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

/// Dirent-key page size for directory scans.
const DIR_PAGE: u32 = 4096;

/// One stored directory entry.
pub struct DirChild {
    pub name: Vec<u8>,
    pub ino: u64,
    /// `DT_REG` / `DT_DIR` / `DT_LNK`.
    pub kind: u8,
}

/// Up to `limit` children of `dir` whose names are `>= from`, in name order,
/// plus where to resume: the successor of the last name SCANNED, or `None`
/// once the scan is exhausted. Resuming from the scan rather than from the
/// last child returned matters when every scanned entry was deleted before
/// its value was read — resuming from the survivors would end the directory
/// there.
///
/// The values arrive in one batched get per partition rather than one
/// round trip per entry. A dirent removed between the key scan and the get
/// is skipped, since the name no longer exists; any other per-entry error
/// fails the call, because a listing that silently drops a live entry is
/// the one answer a caller cannot detect.
pub async fn list_children(
    state: &mut FsState,
    dir: u64,
    from: &[u8],
    limit: u32,
) -> Result<(Vec<DirChild>, Option<Vec<u8>>)> {
    let prefix = key::dirent_prefix(dir);
    let mut start = prefix.clone();
    start.extend_from_slice(from);
    let (keys, has_more) = state.kv_range_page(&prefix, &start, limit).await?;
    let resume = match keys.last() {
        Some(last) if has_more => {
            let (_, name) = key::parse_dirent_key(last).ok_or_else(|| anyhow!("bad dirent key"))?;
            Some(name_successor(name))
        }
        _ => None,
    };
    let children = get_children(state, dir, keys).await?;
    Ok((children, resume))
}

/// The children of `dir` with exactly these names, in the given order,
/// skipping names that do not exist. One batched get.
pub async fn lookup_children(state: &mut FsState, dir: u64, names: &[Vec<u8>]) -> Result<Vec<DirChild>> {
    let keys = names.iter().map(|n| key::dirent_key(dir, n)).collect();
    get_children(state, dir, keys).await
}

async fn get_children(state: &mut FsState, dir: u64, keys: Vec<Vec<u8>>) -> Result<Vec<DirChild>> {
    let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    let values = state.client.get_many(&refs).await;
    let mut out = Vec::with_capacity(keys.len());
    for (k, v) in keys.iter().zip(values) {
        let Some(v) = v.map_err(|e| anyhow!("KV get dirent: {e}"))? else {
            continue;
        };
        let Some((parent, name)) = key::parse_dirent_key(k) else {
            continue;
        };
        debug_assert_eq!(parent, dir);
        let d: DirentValue = schema::decode_dirent(&v).map_err(|e| anyhow!("{}", e))?;
        out.push(DirChild {
            name: name.to_vec(),
            ino: d.child_inode,
            kind: d.file_type,
        });
    }
    Ok(out)
}

/// The smallest name strictly greater than `name` (range starts are inclusive).
pub fn name_successor(name: &[u8]) -> Vec<u8> {
    let mut next = name.to_vec();
    next.push(0);
    next
}

/// Read every directory entry after `offset`.
pub async fn readdir(state: &mut FsState, ino: u64, offset: i64) -> Result<Vec<ReaddirEntry>> {
    readdir_bounded(state, ino, offset, usize::MAX).await
}

/// Read at most `max` directory entries after `offset`.
///
/// Offsets are positions in name order (`.` = 1, `..` = 2, children from 3),
/// so resuming at an offset has to count the names before it. That count
/// scans keys only; values are fetched just for the entries returned. The
/// FUSE mount passes a bound because the kernel takes one reply buffer per
/// call and asks again from the last offset, so fetching the rest of a large
/// directory on every call would be quadratic.
pub async fn readdir_bounded(
    state: &mut FsState,
    ino: u64,
    offset: i64,
    max: usize,
) -> Result<Vec<ReaddirEntry>> {
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
    // Skip whole pages of names that precede `offset` without fetching values.
    let mut skip = (offset - 2).max(0) as usize;
    let mut index = 0usize;
    let mut from: Vec<u8> = Vec::new();
    while skip > 0 {
        let mut start = prefix.clone();
        start.extend_from_slice(&from);
        let want = skip.min(DIR_PAGE as usize) as u32;
        let (keys, has_more) = state.kv_range_page(&prefix, &start, want).await?;
        let Some(last) = keys.last() else {
            return Ok(entries);
        };
        index += keys.len();
        skip -= keys.len();
        let (_, name) = key::parse_dirent_key(last).ok_or_else(|| anyhow!("bad dirent key"))?;
        from = name_successor(name);
        if !has_more {
            return Ok(entries);
        }
    }

    while entries.len() < max {
        let want = (max - entries.len()).min(DIR_PAGE as usize) as u32;
        let (children, resume) = list_children(state, ino, &from, want).await?;
        for c in children {
            index += 1;
            // SAFETY: names are stored as the OS-encoded bytes they were
            // created from.
            let name = unsafe { std::ffi::OsString::from_encoded_bytes_unchecked(c.name) };
            entries.push(ReaddirEntry {
                ino: c.ino,
                offset: index as i64 + 2,
                kind: c.kind,
                name,
            });
        }
        match resume {
            Some(next) => from = next,
            None => break,
        }
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

/// Add a name to a regular file. The directory entry is create-only so two
/// manifest publishers cannot replace each other's winning inode.
///
/// Persist the reference count before the name: a crash between those writes
/// leaks a reference instead of letting unlink of the old name destroy a
/// published file. Like existing rename/unlink this is not a multi-key txn.
pub async fn link(state: &mut FsState, ino: u64, parent: u64, name: &OsStr) -> Result<InodeMeta> {
    let mut inode = get_inode(state, ino).await?;
    if inode.mode & S_IFMT != S_IFREG {
        return Err(anyhow!("EPERM"));
    }
    let mut parent_meta = get_inode(state, parent).await?;
    if parent_meta.mode & S_IFMT != S_IFDIR {
        return Err(anyhow!("ENOTDIR"));
    }
    let key = key::dirent_key(parent, name.as_encoded_bytes());
    if state.kv_get_opt(&key).await?.is_some() {
        return Err(anyhow!("EEXIST"));
    }
    let previous = inode.clone();
    inode.nlink = inode
        .nlink
        .checked_add(1)
        .ok_or_else(|| anyhow!("EMLINK"))?;
    let (secs, nanos) = now_ts();
    inode.ctime_secs = secs;
    inode.ctime_nsecs = nanos;
    put_inode(state, ino, &inode).await?;
    let value = schema::encode_dirent(&DirentValue {
        child_inode: ino,
        file_type: DT_REG,
    });
    let created = state.client.compare_put(&key, None, &value).await?;
    if !created {
        // A retry after a lost ACK can see our own inode. Never decrement a
        // reference whose publication might already have succeeded.
        if state.kv_get_opt(&key).await?.as_deref() != Some(value.as_slice()) {
            put_inode(state, ino, &previous).await?;
            return Err(anyhow!("EEXIST"));
        }
    }
    parent_meta.mtime_secs = secs;
    parent_meta.mtime_nsecs = nanos;
    put_inode(state, parent, &parent_meta).await?;
    *state.lookup_count.entry(ino).or_insert(0) += 1;
    Ok(inode)
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
