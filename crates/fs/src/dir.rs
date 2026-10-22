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

/// One read-modify-write of a directory's inode, compared-and-set against
/// the bytes read. `Ok(false)`: another change landed in between.
async fn cas_dir(state: &mut FsState, dir: u64, change: &impl Fn(&mut InodeMeta)) -> Result<bool> {
    let k = key::inode_key(dir);
    let old = state.kv_get_opt(&k).await?.ok_or_else(|| anyhow!("ENOENT: directory {dir}"))?;
    let mut meta = schema::decode_inode_meta(&old).map_err(|e| anyhow!("inode {dir}: {e}"))?;
    change(&mut meta);
    let new = schema::encode_inode_meta(&meta);
    if new == old {
        return Ok(true);
    }
    let swapped = state
        .client
        .compare_put(&k, Some(&old), &new)
        .await
        .map_err(|e| anyhow!("KV compare_put: {e}"))?;
    if swapped {
        if let Some(is) = state.inodes.get_mut(&dir) {
            is.meta = meta;
        }
    }
    Ok(swapped)
}

/// Changes a directory's inode until the change lands, so two clients
/// changing one directory (a subdirectory's link) do not lose each other's
/// update. Callers have already changed a name — the commit point — so a
/// directory that keeps changing, or a failed read, is logged, not returned:
/// the count is then off (stat's nlink), nothing is unreachable.
pub(crate) async fn update_dir(state: &mut FsState, dir: u64, change: impl Fn(&mut InodeMeta)) {
    for _ in 0..16 {
        match cas_dir(state, dir, &change).await {
            Ok(true) => return,
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(dir, error = %e, "directory inode not updated after a name change");
                return;
            }
        }
    }
    tracing::warn!(dir, "directory inode kept changing; not updated after a name change");
}

/// Sets a directory's mtime and ctime to now, unless its mtime already falls
/// in the current wall-clock second: a burst of creates in one directory then
/// does not put a write of it — a compare-and-set on its one busy key — on
/// every create. Directory times thus have one-second resolution, as on a
/// filesystem with 1 s timestamps: a change in a later second always moves
/// them. One try: losing the race is fine, the change that won landed after
/// this one started.
async fn touch_dir_mtime(state: &mut FsState, dir: u64) {
    let (s, ns) = now_ts();
    let r = cas_dir(state, dir, &|m: &mut InodeMeta| {
        if m.mtime_secs != s {
            m.mtime_secs = s;
            m.mtime_nsecs = ns;
            m.ctime_secs = s;
            m.ctime_nsecs = ns;
        }
    })
    .await;
    if let Err(e) = r {
        tracing::warn!(dir, error = %e, "directory mtime not updated");
    }
}

fn touch(m: &mut InodeMeta) {
    let (s, ns) = now_ts();
    m.mtime_secs = s;
    m.mtime_nsecs = ns;
    m.ctime_secs = s;
    m.ctime_nsecs = ns;
}

/// A directory's link count moved by `delta`, never below its own two.
fn add_links(m: &mut InodeMeta, delta: i64) {
    m.nlink = (m.nlink as i64 + delta).max(2) as u32;
}

/// Writes a new inode and publishes it under `parent/name` only if the name
/// is free: of two clients creating one name, one wins and the other gets
/// EEXIST and removes the inode it wrote.
async fn create_child(
    state: &mut FsState,
    parent: u64,
    name: &OsStr,
    meta: InodeMeta,
    file_type: u8,
) -> Result<u64> {
    // No existence check first: the create-only publish decides, and the
    // kernel looked the name up before asking.
    let dk = key::dirent_key(parent, name.as_encoded_bytes());
    let ino = alloc_inode(state).await?;
    put_new_inode(state, ino, &meta).await?;
    let dv = schema::encode_dirent(&DirentValue { child_inode: ino, file_type });
    let created = state
        .client
        .compare_put(&dk, None, &dv)
        .await
        .map_err(|e| anyhow!("KV compare_put: {e}"))?;
    if !created {
        state.kv_delete(&key::inode_key(ino)).await?;
        return Err(anyhow!("EEXIST"));
    }
    Ok(ino)
}

/// Create a directory. Returns `(ino, meta)` — the FUSE layer converts to
/// `FileAttr` at the reply boundary (M1).
pub async fn mkdir(
    state: &mut FsState,
    parent: u64,
    name: &OsStr,
    mode: u32,
) -> Result<(u64, InodeMeta)> {
    let meta = new_dir_meta(mode, unsafe { libc::getuid() }, unsafe { libc::getgid() });
    let ino = create_child(state, parent, name, meta.clone(), DT_DIR).await?;
    update_dir(state, parent, |m| {
        add_links(m, 1);
        touch(m);
    })
    .await;
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
    let meta = new_file_meta(mode, unsafe { libc::getuid() }, unsafe { libc::getgid() });
    let ino = create_child(state, parent, name, meta.clone(), DT_REG).await?;
    touch_dir_mtime(state, parent).await;
    *state.lookup_count.entry(ino).or_insert(0) += 1;
    Ok((ino, meta))
}

/// Remove a directory (must be empty).
pub async fn rmdir(state: &mut FsState, parent: u64, name: &OsStr) -> Result<()> {
    let dk = key::dirent_key(parent, name.as_encoded_bytes());
    let mut removed = None;
    for _ in 0..8 {
        let v = state.kv_get_opt(&dk).await?.ok_or_else(|| anyhow!("ENOENT"))?;
        let dirent: DirentValue = schema::decode_dirent(&v).map_err(|e| anyhow!("{}", e))?;
        if dirent.file_type != DT_DIR {
            return Err(anyhow!("ENOTDIR"));
        }
        let prefix = key::dirent_prefix(dirent.child_inode);
        if !state.kv_range_keys(&prefix, &prefix, 1).await?.is_empty() {
            return Err(anyhow!("ENOTEMPTY"));
        }
        // Only the name as read: one renamed or replaced meanwhile is not
        // ours to remove, nor is its inode.
        if remove_name(state, &dk, &v).await? {
            removed = Some(dirent);
            break;
        }
    }
    let dirent = removed.ok_or_else(|| anyhow!("EBUSY: the name kept changing"))?;
    state.inodes.remove(&dirent.child_inode);
    // After the commit point (the name is gone): a failure leaks the inode.
    if let Err(e) = state.kv_delete(&key::inode_key(dirent.child_inode)).await {
        tracing::warn!(ino = dirent.child_inode, error = %e, "removed directory's inode not deleted");
    }
    update_dir(state, parent, |m| {
        add_links(m, -1);
        touch(m);
    })
    .await;
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
    let dk = key::dirent_key(parent, name.as_encoded_bytes());
    // Remove the name as read, then decrement nlink: a name another client
    // replaced meanwhile names an inode this unlink must not count down.
    let mut removed = None;
    for _ in 0..8 {
        let v = state.kv_get_opt(&dk).await?.ok_or_else(|| anyhow!("ENOENT"))?;
        let dirent: DirentValue = schema::decode_dirent(&v).map_err(|e| anyhow!("{}", e))?;
        if dirent.file_type == DT_DIR {
            return Err(anyhow!("EISDIR"));
        }
        if remove_name(state, &dk, &v).await? {
            removed = Some(dirent);
            break;
        }
    }
    let dirent = removed.ok_or_else(|| anyhow!("EBUSY: the name kept changing"))?;
    if change_links(state, dirent.child_inode, -1).await?.nlink == 0 {
        // The inode just became UNREACHABLE — remove its data under an intent
        // tombstone so a crash mid-removal is replayed at next mount instead
        // of leaking the extents forever.
        crate::extent::remove_unreachable_inode(state, dirent.child_inode).await?;
    }
    touch_dir_mtime(state, parent).await;
    Ok(())
}

/// Deletes the name `dk` if it still holds `v`.
async fn remove_name(state: &mut FsState, dk: &[u8], v: &[u8]) -> Result<bool> {
    state
        .client
        .compare_write(dk, Some(v), None, autumn_client::WriteLease::ANON)
        .await
        .map_err(|e| anyhow!("KV compare_write: {e}"))
}

/// Add a name to a regular file. The directory entry is create-only so two
/// manifest publishers cannot replace each other's winning inode.
///
/// Persist the reference count before the name: a crash between those writes
/// leaks a reference instead of letting unlink of the old name destroy a
/// published file. Like existing rename/unlink this is not a multi-key txn.
/// The count moves by compare-and-set, and an inode whose last name another
/// session just dropped is refused (ENOENT) rather than named again.
pub async fn link(state: &mut FsState, ino: u64, parent: u64, name: &OsStr) -> Result<InodeMeta> {
    let inode = get_inode(state, ino).await?;
    if inode.mode & S_IFMT != S_IFREG {
        return Err(anyhow!("EPERM"));
    }
    let parent_meta = get_inode(state, parent).await?;
    if parent_meta.mode & S_IFMT != S_IFDIR {
        return Err(anyhow!("ENOTDIR"));
    }
    let key = key::dirent_key(parent, name.as_encoded_bytes());
    if state.kv_get_opt(&key).await?.is_some() {
        return Err(anyhow!("EEXIST"));
    }
    let inode = change_links(state, ino, 1).await?;
    let value = schema::encode_dirent(&DirentValue {
        child_inode: ino,
        file_type: DT_REG,
    });
    let created = state.client.compare_put(&key, None, &value).await?;
    if !created {
        // A retry after a lost ACK can see our own inode. Never decrement a
        // reference whose publication might already have succeeded.
        if state.kv_get_opt(&key).await?.as_deref() != Some(value.as_slice()) {
            // Another session's unlink may have dropped the name this
            // link counted on meanwhile.
            if change_links(state, ino, -1).await?.nlink == 0 {
                crate::extent::remove_unreachable_inode(state, ino).await?;
            }
            return Err(anyhow!("EEXIST"));
        }
    }
    touch_dir_mtime(state, parent).await;
    *state.lookup_count.entry(ino).or_insert(0) += 1;
    Ok(inode)
}

/// What a rename of `source` onto a name holding `target` does.
enum Onto {
    Free,
    Replace(DirentValue),
    /// Both names already name the source's inode.
    Same,
}

/// The new name may hold nothing, a file (a file replaces it) or an empty
/// directory (a directory replaces it); anything else is refused.
async fn rename_onto(state: &mut FsState, source: &DirentValue, target: Option<&[u8]>) -> Result<Onto> {
    let Some(tv) = target else { return Ok(Onto::Free) };
    let t = schema::decode_dirent(tv).map_err(|e| anyhow!("{}", e))?;
    // POSIX: two names of one inode (or one path twice) is a no-op (coco P1:
    // treating the source as a replaced target destroyed its data).
    if t.child_inode == source.child_inode {
        return Ok(Onto::Same);
    }
    match (source.file_type == DT_DIR, t.file_type == DT_DIR) {
        (true, false) => Err(anyhow!("ENOTDIR")),
        (false, true) => Err(anyhow!("EISDIR")),
        (true, true) => {
            let prefix = key::dirent_prefix(t.child_inode);
            if state.kv_range_keys(&prefix, &prefix, 1).await?.is_empty() {
                Ok(Onto::Replace(t))
            } else {
                Err(anyhow!("ENOTEMPTY"))
            }
        }
        (false, false) => Ok(Onto::Replace(t)),
    }
}

/// Rename a file or directory; see `rename_onto` for what the new name may
/// hold. Both names are compared-and-set against what was checked: the old
/// name is removed first (a crash in between loses the name, never leaves
/// two names on one link count), and a new name another client changed
/// meanwhile is checked again before it is replaced.
pub async fn rename(
    state: &mut FsState,
    old_parent: u64,
    old_name: &OsStr,
    new_parent: u64,
    new_name: &OsStr,
) -> Result<()> {
    let old_dk = key::dirent_key(old_parent, old_name.as_encoded_bytes());
    let new_dk = key::dirent_key(new_parent, new_name.as_encoded_bytes());
    let mut removed = None;
    for _ in 0..8 {
        let v = state.kv_get_opt(&old_dk).await?.ok_or_else(|| anyhow!("ENOENT"))?;
        let source: DirentValue = schema::decode_dirent(&v).map_err(|e| anyhow!("{}", e))?;
        let target = state.kv_get_opt(&new_dk).await?;
        if let Onto::Same = rename_onto(state, &source, target.as_deref()).await? {
            return Ok(());
        }
        let removed_now = remove_name(state, &old_dk, &v)
            .await
            .map_err(|e| anyhow!("rename: {e}; inode {} may have no name", source.child_inode))?;
        if removed_now {
            removed = Some((v, source));
            break;
        }
    }
    let Some((v, source)) = removed else {
        return Err(anyhow!("EBUSY: the old name kept changing"));
    };
    let gone = |e| anyhow!("rename: {e}; inode {} may have no name", source.child_inode);
    for _ in 0..8 {
        let target = state.kv_get_opt(&new_dk).await.map_err(gone)?;
        let replaced = match rename_onto(state, &source, target.as_deref()).await {
            Ok(Onto::Free) => None,
            Ok(Onto::Replace(t)) => Some(t),
            // Refused now, or the new name came to name the source too: the
            // old name goes back.
            r @ (Ok(Onto::Same) | Err(_)) => {
                if !compare_put_settled(&state.client, &old_dk, None, &v).await.map_err(gone)? {
                    return Err(gone(anyhow!("its name was taken while it was being moved")));
                }
                return r.map(drop);
            }
        };
        if !compare_put_settled(&state.client, &new_dk, target.as_deref(), &v).await.map_err(gone)? {
            continue;
        }
        return finish_rename(state, old_parent, new_parent, source.file_type == DT_DIR, replaced).await;
    }
    // Every try lost to a definite conflict, so nothing was published.
    if !compare_put_settled(&state.client, &old_dk, None, &v).await.map_err(gone)? {
        return Err(gone(anyhow!("its name was taken while it was being moved")));
    }
    Err(anyhow!("EBUSY: the new name kept changing"))
}

/// Test-only failpoint for the next `compare_put_settled` attempt:
/// `SETTLE_TEST_UNSENT` reports its outcome unknown without sending it;
/// `SETTLE_TEST_LANDS_LATE` does the same, then sends it after the re-read,
/// just before the next attempt — a copy held up behind a slow append.
#[doc(hidden)]
pub static SETTLE_TEST_FAULT: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
#[doc(hidden)]
pub const SETTLE_TEST_UNSENT: u8 = 1;
#[doc(hidden)]
pub const SETTLE_TEST_LANDS_LATE: u8 = 2;
/// Set when a `SETTLE_TEST_LANDS_LATE` copy was sent and landed.
#[doc(hidden)]
pub static SETTLE_TEST_LATE_LANDED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Compare-and-sets the name `dk` from `expected` to `value`, settling an
/// unknown outcome by reading again: `value` means it landed; `expected`
/// means it has not, and the same compare-and-set goes again (of copies
/// expecting the same bytes at most one lands); anything else means it did
/// not and cannot. Once an attempt was unknown, a later attempt's refusal
/// is settled the same way: it may have been refused by the earlier copy.
/// This holds while no other session writes the name in the window; one
/// that does can make the read-back mislead (fs CLAUDE.md, rename).
pub async fn compare_put_settled(
    client: &autumn_client::ClusterClient,
    dk: &[u8],
    expected: Option<&[u8]>,
    value: &[u8],
) -> Result<bool> {
    use std::sync::atomic::Ordering::Relaxed;
    let (mut unsure, mut held) = (false, false);
    for _ in 0..8 {
        if std::mem::take(&mut held) {
            let landed = client.compare_put(dk, expected, value).await.map_err(|e| anyhow!("KV compare_put: {e}"))?;
            SETTLE_TEST_LATE_LANDED.store(landed, Relaxed);
        }
        let fault = match SETTLE_TEST_FAULT.load(Relaxed) {
            0 => 0,
            _ => SETTLE_TEST_FAULT.swap(0, Relaxed),
        };
        let sent = if fault != 0 {
            held = fault == SETTLE_TEST_LANDS_LATE;
            Err(autumn_client::AutumnError::OutcomeUnknown("not sent (test)".into()))
        } else {
            client.compare_put(dk, expected, value).await
        };
        match sent {
            Ok(true) => return Ok(true),
            Ok(false) if !unsure => return Ok(false),
            Ok(false) | Err(autumn_client::AutumnError::OutcomeUnknown(_)) => {
                unsure = true;
                let now = client.get(dk).await.map_err(|e| anyhow!("KV get: {e}"))?;
                if now.as_deref() == Some(value) {
                    return Ok(true);
                }
                if now.as_deref() != expected {
                    return Ok(false);
                }
            }
            Err(e) => return Err(anyhow!("KV compare_put: {e}")),
        }
    }
    Err(anyhow!("outcome unknown: the name never settled"))
}

/// After the new name points at the source — the commit point: removes what
/// it replaced and updates both parents. A failure here is logged; it leaks
/// what was replaced, the rename has happened.
async fn finish_rename(
    state: &mut FsState,
    old_parent: u64,
    new_parent: u64,
    moved_dir: bool,
    replaced: Option<DirentValue>,
) -> Result<()> {
    let mut replaced_dir = false;
    match replaced {
        Some(t) if t.file_type == DT_DIR => {
            // Checked empty; nothing else names it now.
            state.inodes.remove(&t.child_inode);
            if let Err(e) = state.kv_delete(&key::inode_key(t.child_inode)).await {
                tracing::warn!(ino = t.child_inode, error = %e, "replaced directory's inode not deleted");
            }
            replaced_dir = true;
        }
        // UNLINK-1: rename-over drops the target file's name. Its data goes
        // through the same tombstoned removal as unlink, AFTER the dirent
        // overwrite above (the unreachability point — a tombstone for a
        // still-reachable inode would let the sweep destroy a live file).
        Some(t) => {
            let dropped = async {
                if change_links(state, t.child_inode, -1).await?.nlink == 0 {
                    crate::extent::remove_unreachable_inode(state, t.child_inode).await?;
                }
                anyhow::Ok(())
            }
            .await;
            if let Err(e) = dropped {
                tracing::warn!(ino = t.child_inode, error = %e, "replaced file's name not dropped from its inode");
            }
        }
        None => {}
    }

    // A moved directory's `..` leaves one parent for the other; a replaced
    // directory's leaves the new parent.
    let moved = moved_dir as i64;
    let lost = replaced_dir as i64;
    if old_parent == new_parent {
        update_dir(state, old_parent, |m| {
            add_links(m, -lost);
            touch(m);
        })
        .await;
    } else {
        update_dir(state, old_parent, |m| {
            add_links(m, -moved);
            touch(m);
        })
        .await;
        update_dir(state, new_parent, |m| {
            add_links(m, moved - lost);
            touch(m);
        })
        .await;
    }
    Ok(())
}

// M1: `dt_to_filetype` moved to `attr.rs` (fuse-gated reply
// boundary) — readdir entries carry the raw `DT_*` byte now.
