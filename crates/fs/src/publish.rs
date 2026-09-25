//! Publishing a whole file under a name, atomically and conditionally.
//!
//! The S3 gateway's PUT, Copy and multipart Complete all end the same way: a
//! fully written file in a FRESH inode becomes visible under a name in one
//! step, or not at all. The step is one fenced `compare_write` on the dirent,
//! so `If-None-Match: *` (expected absent) and `If-Match` (expected the dirent
//! naming the matched inode) are decided by the partition server, never by a
//! read followed by a write. A reader of the name sees the old file or the
//! new one, whole.
//!
//! **Sessions.** Everything a publisher writes is stamped with the epoch of a
//! WRITE lease on its session inode, renewed by the ordinary lease heartbeat.
//! Before it writes anything an operation records itself under the session
//! (`key::pending_key`), and deletes the record once done or undone. When a
//! session's owner dies its lease expires; a sweeper then takes that lease —
//! which raises the epoch — raises every `fs/` partition's fence floor for the
//! session to it, so no late write of the dead owner can land anywhere, and
//! finishes or undoes each recorded operation by looking at what the dirent
//! names. Without the fence, a paused owner could publish a name after the
//! sweeper had judged it dead and deleted the inode's data.

use std::ffi::OsStr;

use anyhow::{anyhow, Result};
use bytes::BytesMut;

use autumn_client::lease::{self, AcquireResult};
use autumn_client::{ClusterClient, WriteLease};
use autumn_rpc::manager_rpc::{LEASE_MODE_REPLACE, LEASE_MODE_WRITE};

use crate::key;
use crate::meta::{self, S_IFDIR, S_IFMT, S_IFREG};
use crate::schema::{self, DirentValue, InodeMeta, PendingOp, SegmentMap, DT_DIR, DT_REG};
use crate::segment;
use crate::state::{FsState, FuseLease};

/// Why a publish or delete did not happen.
#[derive(Debug)]
pub enum PublishError {
    /// The condition did not hold (`If-None-Match: *` over an existing name,
    /// `If-Match` naming another version).
    PreconditionFailed,
    /// `If-Match` on a name that does not exist.
    NoSuchKey,
    /// Another client is writing the file in place; S3 reports this as a
    /// conflict immediately rather than waiting or preempting.
    Busy(String),
    /// A path component, or the name itself, is a directory where a file is
    /// needed, or a file where a directory is.
    NotAFile(String),
    Other(anyhow::Error),
}

impl From<anyhow::Error> for PublishError {
    fn from(e: anyhow::Error) -> Self {
        PublishError::Other(e)
    }
}

impl std::fmt::Display for PublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PublishError::PreconditionFailed => write!(f, "precondition failed"),
            PublishError::NoSuchKey => write!(f, "no such key"),
            PublishError::Busy(m) => write!(f, "busy: {m}"),
            PublishError::NotAFile(m) => write!(f, "not a file: {m}"),
            PublishError::Other(e) => write!(f, "{e:#}"),
        }
    }
}

/// What the name must currently hold for a publish or delete to go ahead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Condition {
    /// Anything, including nothing.
    None,
    /// Nothing (`If-None-Match: *`).
    Absent,
    /// This version of a file (`If-Match`): inode and content generation.
    Version { ino: u64, generation: u64 },
}

/// The S3 ETag of one version of a file: its inode and content generation.
pub fn etag(ino: u64, generation: u64) -> String {
    format!("{ino:016x}{generation:016x}")
}

/// Parse an ETag made by [`etag`] (quotes and a weak prefix tolerated).
pub fn parse_etag(s: &str) -> Option<(u64, u64)> {
    let s = s.trim().trim_start_matches("W/").trim_matches('"');
    if s.len() != 32 {
        return None;
    }
    Some((u64::from_str_radix(&s[..16], 16).ok()?, u64::from_str_radix(&s[16..], 16).ok()?))
}

// ── session ─────────────────────────────────────────────────────────────────

/// This session's fence, creating the session on first use or after the
/// previous one was lost (its heartbeat found the lease gone).
pub async fn session_lease(state: &mut FsState) -> Result<WriteLease> {
    if let Some(s) = state.session {
        if let Some(l) = state.held_leases.borrow().get(&s) {
            if !l.revoked {
                return Ok(WriteLease { inode_hint: s, lease_epoch: l.lease_epoch });
            }
        }
        tracing::warn!(session = s, "publishing session lost its lease; starting a new one");
        state.session = None;
    }
    let s = meta::alloc_inode(state).await?;
    let cluster = state.client.clone();
    let id = state.client_id.clone();
    let epoch = match lease::acquire(&cluster, &id, s, LEASE_MODE_WRITE)
        .await
        .map_err(|e| anyhow!("session lease: {e}"))?
    {
        AcquireResult::Granted(info) => info.version,
        other => return Err(anyhow!("session lease on a fresh inode not granted: {other:?}")),
    };
    let lease = WriteLease { inode_hint: s, lease_epoch: epoch };
    let host = state.client_id.as_wire().host.clone();
    // The session key first: a session nobody can find is one nobody would
    // recover. A failure hands the lease straight back.
    if let Err(e) = state.kv_put_fenced(&key::session_key(s), host.as_bytes(), lease).await {
        let _ = lease::release(&cluster, &id, s).await;
        return Err(e);
    }
    state.held_leases.borrow_mut().insert(
        s,
        FuseLease { writer_refs: 1, reader_refs: 0, mode: LEASE_MODE_WRITE, lease_epoch: epoch, revoked: false },
    );
    state.session = Some(s);
    Ok(lease)
}

// ── writing a new file ───────────────────────────────────────────────────────

/// A file being written into a fresh inode, not yet visible under any name.
pub struct NewFile {
    pub ino: u64,
    pub parent: u64,
    pub name: Vec<u8>,
    lease: WriteLease,
    /// Bytes held while the file may still be small enough to inline.
    small: BytesMut,
    /// The single data object, created on the first byte past the inline
    /// threshold.
    object: Option<segment::ObjectStream>,
}

impl NewFile {
    /// Begin a file that will be published as `parent`/`name`. Records the
    /// operation under the session before anything else is written.
    pub async fn begin(state: &mut FsState, parent: u64, name: &[u8]) -> Result<Self> {
        let lease = session_lease(state).await?;
        let ino = meta::alloc_inode(state).await?;
        let op = PendingOp::Publish { parent, name: name.to_vec(), new_ino: ino };
        state
            .kv_put_fenced(&key::pending_key(lease.inode_hint, ino), &schema::encode_pending(&op), lease)
            .await?;
        Ok(NewFile { ino, parent, name: name.to_vec(), lease, small: BytesMut::new(), object: None })
    }

    /// Append `data`.
    pub async fn write(&mut self, state: &mut FsState, data: &[u8]) -> Result<()> {
        if let Some(o) = &mut self.object {
            return o.write(&state.client, data).await;
        }
        self.small.extend_from_slice(data);
        if self.small.len() > schema::INLINE_THRESHOLD {
            self.start_object(state).await?;
        }
        Ok(())
    }

    /// Give the file its data object now rather than when it outgrows the
    /// inline threshold, so every later [`NewFile::write_streamed`] needs only
    /// the client. For a writer that knows the file is not small (a PUT whose
    /// length exceeds the threshold, or is unknown) and does not want to hold
    /// the `FsState` while the body arrives.
    pub async fn start_object(&mut self, state: &mut FsState) -> Result<()> {
        if self.object.is_some() {
            return Ok(());
        }
        let ino = self.ino;
        // Recorded under the new file, so an undo — or a reclaim once
        // published — finds it.
        let mut o = segment::ObjectStream::new(state, |d| key::segc_key(ino, d), self.lease).await?;
        let held = self.small.split();
        o.write(&state.client, &held).await?;
        self.object = Some(o);
        Ok(())
    }

    /// Append `data` through the client alone. Only after
    /// [`NewFile::start_object`]: the inline buffer would need the `FsState`
    /// to spill into an object.
    pub async fn write_streamed(&mut self, client: &ClusterClient, data: &[u8]) -> Result<()> {
        match &mut self.object {
            Some(o) => o.write(client, data).await,
            None => Err(anyhow!("write_streamed before start_object")),
        }
    }

    /// Write the rest and the inode. Returns the inode's metadata, which is
    /// what the file will be once published. Still invisible.
    pub async fn finish(&mut self, state: &mut FsState) -> Result<InodeMeta> {
        let (uid, gid) = (unsafe { libc::getuid() }, unsafe { libc::getgid() });
        let mut m = meta::new_file_meta(0o644, uid, gid);
        match &mut self.object {
            None => {
                m.size = self.small.len() as u64;
                if !self.small.is_empty() {
                    m.inline_data = Some(self.small.split().to_vec());
                }
            }
            Some(o) => {
                let (len, _) = o.finish(&state.client).await?;
                m.size = len;
                m.segments = Some(SegmentMap { inline: vec![o.segment(0)], map_id: 0, page_starts: Vec::new(), count: 1 });
            }
        }
        state
            .kv_put_fenced(&key::inode_key(self.ino), &schema::encode_inode_meta(&m), self.lease)
            .await?;
        Ok(m)
    }

    /// Make the finished file visible under its name if `cond` holds.
    pub async fn publish(self, state: &mut FsState, cond: Condition) -> std::result::Result<(), PublishError> {
        let r = publish_inode(state, self.parent, &self.name, self.ino, DT_REG, cond, self.lease).await;
        match r {
            Ok(()) => {
                state.kv_delete_fenced(&key::pending_key(self.lease.inode_hint, self.ino), self.lease).await?;
                Ok(())
            }
            // Whether the swap landed is unknown: the pending record stays,
            // and the session's recovery decides by what the dirent names.
            Err(e @ PublishError::Other(_)) => Err(e),
            Err(e) => {
                undo_new_file(state, self.ino, self.lease).await?;
                Err(e)
            }
        }
    }

    /// Discard an unpublished file (a failed or cancelled upload).
    pub async fn abort(self, state: &mut FsState) -> Result<()> {
        undo_new_file(state, self.ino, self.lease).await
    }
}

/// Delete an unpublished inode, its data and its pending record.
async fn undo_new_file(state: &mut FsState, ino: u64, lease: WriteLease) -> Result<()> {
    segment::reclaim(state, ino, None, lease).await?;
    state.kv_delete_fenced(&key::inode_key(ino), lease).await?;
    state.kv_delete_fenced(&key::pending_key(lease.inode_hint, ino), lease).await?;
    Ok(())
}

// ── names ────────────────────────────────────────────────────────────────────

fn dirent_bytes(ino: u64, kind: u8) -> Vec<u8> {
    schema::encode_dirent(&DirentValue { child_inode: ino, file_type: kind })
}

/// Point `parent`/`name` at `ino` if `cond` holds, then retire whatever it
/// named before. `Condition::None` retries until its CAS lands.
///
/// The CAS is the commit point. `PublishError::Other` means its outcome is
/// unknown (the caller must not undo); every other error means it did not
/// land. Nothing after it fails the publish: retiring the old inode is
/// recorded first (`PendingOp::Retire`) and left to recovery if it fails.
pub(crate) async fn publish_inode(
    state: &mut FsState,
    parent: u64,
    name: &[u8],
    ino: u64,
    kind: u8,
    cond: Condition,
    lease: WriteLease,
) -> std::result::Result<(), PublishError> {
    let dk = key::dirent_key(parent, name);
    let new = dirent_bytes(ino, kind);
    loop {
        let current = state.kv_get_opt(&dk).await?;
        let old = match &current {
            Some(v) => Some(schema::decode_dirent(v).map_err(|e| anyhow!("dirent: {e}"))?),
            None => None,
        };
        match (cond, &old) {
            (Condition::Absent, Some(_)) => return Err(PublishError::PreconditionFailed),
            (Condition::Version { .. }, None) => return Err(PublishError::NoSuchKey),
            _ => {}
        }
        if let Some(d) = &old {
            if d.file_type == DT_DIR {
                return Err(PublishError::NotAFile(String::from_utf8_lossy(name).into_owned()));
            }
        }
        let replacing = old.as_ref().map(|d| d.child_inode);
        let mut recorded = false;
        if let Some(o) = replacing {
            recorded = hold_for_swap(state, parent, name, o, Some(ino), cond, lease).await?;
        }
        let swapped = state.client.compare_write(&dk, current.as_deref(), Some(&new), lease).await;
        let swapped = match swapped {
            Ok(s) => s,
            // The reply was lost, not necessarily the write: look.
            // A request still in flight may land later even if the name reads
            // unchanged now, so short of seeing it landed the outcome stays
            // unknown and the retire record stays with it.
            Err(e) => match state.kv_get_opt(&dk).await {
                Ok(v) if v.as_deref() == Some(new.as_slice()) => true,
                _ => {
                    if let Some(o) = replacing {
                        release_lease(state, o).await;
                    }
                    return Err(anyhow!("publish {}: outcome unknown: {e}", String::from_utf8_lossy(name)).into());
                }
            },
        };
        if !swapped && state.kv_get_opt(&dk).await?.as_deref() != Some(new.as_slice()) {
            end_swap(state, replacing, recorded, lease).await;
            match cond {
                Condition::None => continue,
                _ => return Err(PublishError::PreconditionFailed),
            }
        }
        // Landed (or found landed by a retry after a lost acknowledgement).
        if let Some(o) = replacing {
            retire(state, parent, name, o, lease).await;
        }
        return Ok(());
    }
}

/// Before a swap away from `o` to `successor` (`None` = a delete): hold `o`
/// against in-place writers (an open writer is a conflict now, not a lost
/// write later), check an `If-Match` under that hold, and record the
/// retirement of `o`. Returns whether this call created the record: one
/// left by an earlier swap whose outcome is unknown is kept as it is, since
/// that swap may still land.
async fn hold_for_swap(
    state: &mut FsState,
    parent: u64,
    name: &[u8],
    o: u64,
    successor: Option<u64>,
    cond: Condition,
    lease: WriteLease,
) -> std::result::Result<bool, PublishError> {
    // The manager lets a client replace over its own WRITE; this session's
    // own open writer is just as much a conflict. Only a WRITER: a reader or
    // an S3 GET's STABLE pin held here does not stop a replace, exactly as the
    // same holders on another client do not.
    let own_writer = state
        .held_leases
        .borrow()
        .get(&o)
        .is_some_and(|l| l.writer_refs > 0 || l.mode == LEASE_MODE_WRITE);
    if own_writer {
        return Err(PublishError::Busy(format!("inode {o} is open for writing by this client")));
    }
    hold_replace(state, o).await?;
    let r = async {
        if let Condition::Version { ino: want, generation } = cond {
            // Read under the hold: no other client can change `o` now. Gone
            // means another publisher replaced it first.
            let Some(bytes) = state.kv_get_opt(&key::inode_key(o)).await? else {
                return Err(PublishError::PreconditionFailed);
            };
            let m = schema::decode_inode_meta(&bytes).map_err(|e| anyhow!("inode {o}: {e}"))?;
            if o != want || m.generation != generation {
                return Err(PublishError::PreconditionFailed);
            }
        }
        let op = PendingOp::Retire { parent, name: name.to_vec(), ino: o, successor };
        let created = state
            .client
            .compare_write(&key::pending_key(lease.inode_hint, o), None, Some(&schema::encode_pending(&op)), lease)
            .await
            .map_err(|e| anyhow!("retire record of {o}: {e}"))?;
        Ok(created)
    }
    .await;
    if r.is_err() {
        release_lease(state, o).await;
    }
    r
}

/// A swap away from `o` did not land: drop the hold, and the retire record
/// if this swap created it.
async fn end_swap(state: &mut FsState, replacing: Option<u64>, recorded: bool, lease: WriteLease) {
    if let Some(o) = replacing {
        if recorded {
            if let Err(e) = state.kv_delete_fenced(&key::pending_key(lease.inode_hint, o), lease).await {
                tracing::warn!(ino = o, error = %e, "dropping a retire record failed; recovery re-checks the name");
            }
        }
        release_lease(state, o).await;
    }
}

/// The name no longer names `o`: drop the name from it, then its record.
/// A failure leaves the record for the session's recovery.
async fn retire(state: &mut FsState, parent: u64, name: &[u8], o: u64, lease: WriteLease) {
    let r = drop_name_of(state, o).await;
    release_lease(state, o).await;
    match r {
        Ok(()) => {
            if let Err(e) = state.kv_delete_fenced(&key::pending_key(lease.inode_hint, o), lease).await {
                tracing::warn!(ino = o, error = %e, "dropping a retire record failed");
            }
        }
        Err(e) => tracing::warn!(
            ino = o,
            name = %String::from_utf8_lossy(name),
            parent,
            error = %e,
            "retiring a replaced inode failed; the session's recovery retries"
        ),
    }
}

/// Remove `parent`/`name` if it names a file and `cond` holds. Returns
/// whether a file was removed; a missing name or a directory is not an error
/// (S3 deletes are idempotent and prefixes are not objects).
pub async fn delete_name(
    state: &mut FsState,
    parent: u64,
    name: &[u8],
    cond: Condition,
) -> std::result::Result<bool, PublishError> {
    let lease = session_lease(state).await?;
    let dk = key::dirent_key(parent, name);
    loop {
        let Some(current) = state.kv_get_opt(&dk).await? else {
            return match cond {
                Condition::Version { .. } => Err(PublishError::NoSuchKey),
                _ => Ok(false),
            };
        };
        if cond == Condition::Absent {
            return Err(PublishError::PreconditionFailed);
        }
        let d = schema::decode_dirent(&current).map_err(|e| anyhow!("dirent: {e}"))?;
        if d.file_type == DT_DIR {
            return Ok(false);
        }
        let recorded = hold_for_swap(state, parent, name, d.child_inode, None, cond, lease).await?;
        let removed = match state.client.compare_write(&dk, Some(&current), None, lease).await {
            Ok(r) => r,
            Err(e) => match state.kv_get_opt(&dk).await {
                Ok(None) => true,
                _ => {
                    release_lease(state, d.child_inode).await;
                    return Err(anyhow!("delete {}: outcome unknown: {e}", String::from_utf8_lossy(name)).into());
                }
            },
        };
        if !removed {
            end_swap(state, Some(d.child_inode), recorded, lease).await;
            continue;
        }
        retire(state, parent, name, d.child_inode, lease).await;
        return Ok(true);
    }
}

/// A name of inode `ino` is gone: count it down, and once none remain retire
/// the inode (tombstoned; data reclaimed once no other client holds it).
async fn drop_name_of(state: &mut FsState, ino: u64) -> Result<()> {
    let Some(bytes) = state.kv_get_opt(&key::inode_key(ino)).await? else {
        return Ok(());
    };
    let mut m = schema::decode_inode_meta(&bytes).map_err(|e| anyhow!("inode {ino}: {e}"))?;
    if m.nlink <= 1 {
        state.inodes.remove(&ino);
        crate::extent::remove_unreachable_inode(state, ino).await
    } else {
        m.nlink -= 1;
        meta::put_inode(state, ino, &m).await
    }
}

async fn hold_replace(state: &mut FsState, ino: u64) -> std::result::Result<(), PublishError> {
    let cluster = state.client.clone();
    let id = state.client_id.clone();
    match lease::acquire(&cluster, &id, ino, LEASE_MODE_REPLACE)
        .await
        .map_err(|e| anyhow!("replace lease {ino}: {e}"))?
    {
        AcquireResult::Granted(_) => Ok(()),
        AcquireResult::Conflict { manager_message } | AcquireResult::RevokePending { manager_message, .. } => {
            Err(PublishError::Busy(manager_message))
        }
    }
}

async fn release_lease(state: &mut FsState, ino: u64) {
    let cluster = state.client.clone();
    let id = state.client_id.clone();
    if let Err(e) = lease::release(&cluster, &id, ino).await {
        tracing::warn!(ino, error = %e, "releasing a replace lease failed; TTL revoke is the backstop");
    }
}

/// Resolve (creating as needed) the directory `components` below `root`.
/// A directory another publisher creates concurrently is found and used; a
/// file in the way is `NotAFile`.
pub async fn ensure_dirs(
    state: &mut FsState,
    root: u64,
    components: &[&[u8]],
) -> std::result::Result<u64, PublishError> {
    let mut dir = root;
    for comp in components {
        loop {
            if let Some((ino, m)) = crate::dir::lookup_opt(state, dir, OsStr::from_bytes_compat(comp)).await? {
                if m.mode & S_IFMT != S_IFDIR {
                    return Err(PublishError::NotAFile(String::from_utf8_lossy(comp).into_owned()));
                }
                dir = ino;
                break;
            }
            let lease = session_lease(state).await?;
            let ino = meta::alloc_inode(state).await?;
            let m = meta::new_dir_meta(0o755, unsafe { libc::getuid() }, unsafe { libc::getgid() });
            state.kv_put_fenced(&key::inode_key(ino), &schema::encode_inode_meta(&m), lease).await?;
            let created = state
                .client
                .compare_write(&key::dirent_key(dir, comp), None, Some(&dirent_bytes(ino, DT_DIR)), lease)
                .await
                .map_err(|e| anyhow!("mkdir {}: {e}", String::from_utf8_lossy(comp)))?;
            if created {
                // `..` of the new directory, as `dir::mkdir` counts it.
                let mut pm = meta::get_inode(state, dir).await?;
                pm.nlink += 1;
                meta::put_inode(state, dir, &pm).await?;
                dir = ino;
                break;
            }
            // Someone else created it first: drop ours and use theirs.
            state.kv_delete_fenced(&key::inode_key(ino), lease).await?;
        }
    }
    Ok(dir)
}

/// `OsStr` from raw name bytes on the platforms this builds for.
trait OsStrCompat {
    fn from_bytes_compat(b: &[u8]) -> &OsStr;
}

impl OsStrCompat for OsStr {
    fn from_bytes_compat(b: &[u8]) -> &OsStr {
        std::os::unix::ffi::OsStrExt::from_bytes(b)
    }
}

/// Whether a meta describes a regular file.
pub fn is_file(m: &InodeMeta) -> bool {
    m.mode & S_IFMT == S_IFREG
}

// ── recovery ─────────────────────────────────────────────────────────────────

/// Probe value no stored value equals: a `compare_write` expecting it never
/// writes, and so is a pure fence-floor raise.
const FENCE_PROBE: &[u8] = b"\x00autumn-fence-probe\x00never-a-stored-value\x00";

/// Raise every `fs/` partition's fence floor for `lease.inode_hint` to
/// `lease.lease_epoch`: after this, no write stamped with an older epoch of
/// that session lands anywhere in the tree.
///
/// Regions are re-read first (a long-lived client's cache can predate a
/// split, and a floor raised on the parent after the split is not in the
/// child), and the pass repeats until no split moved the set under it.
pub async fn fence_all(state: &mut FsState, lease: WriteLease) -> Result<()> {
    const NS: &[u8] = b"fs/";
    const NS_END: &[u8] = b"fs0";
    let mut fenced = std::collections::HashSet::new();
    loop {
        state.client.refresh_regions().await?;
        let mut fresh = false;
        for (part, _, start, end) in state.client.all_partitions_with_range().await? {
            if start.as_slice() >= NS_END || (!end.is_empty() && end.as_slice() <= NS) || fenced.contains(&part) {
                continue;
            }
            let probe = if start.as_slice() > NS { &start[NS.len()..] } else { &[][..] };
            state
                .client
                .compare_write(probe, Some(FENCE_PROBE), None, lease)
                .await
                .map_err(|e| anyhow!("fence partition at {:?}: {e}", String::from_utf8_lossy(&start)))?;
            fenced.insert(part);
            fresh = true;
        }
        // A pass that found nothing new saw every partition that exists
        // after the last floor was raised.
        if !fresh {
            return Ok(());
        }
    }
}

/// Take over every publishing session whose owner is gone, finish or undo
/// what it left, and remove it. Returns the sessions recovered. A live
/// session's lease refuses the takeover, so running this anywhere, any time,
/// is safe.
pub async fn recover_dead_sessions(state: &mut FsState) -> Result<usize> {
    let prefix = key::session_prefix();
    let (keys, _) = state.kv_range_page(&prefix, &prefix, 1024).await?;
    let mut recovered = 0;
    for k in keys {
        let Some(s) = key::parse_session_key(&k) else { continue };
        if Some(s) == state.session {
            continue;
        }
        let cluster = state.client.clone();
        let id = state.client_id.clone();
        let epoch = match lease::acquire(&cluster, &id, s, LEASE_MODE_WRITE).await {
            Ok(AcquireResult::Granted(info)) => info.version,
            Ok(_) => continue,
            Err(e) => {
                tracing::warn!(session = s, error = %e, "session takeover failed");
                continue;
            }
        };
        let lease = WriteLease { inode_hint: s, lease_epoch: epoch };
        let r = recover_session(state, s, lease).await;
        let _ = lease::release(&cluster, &id, s).await;
        match r {
            Ok(()) => recovered += 1,
            Err(e) => tracing::warn!(session = s, error = %e, "session recovery incomplete; retried next sweep"),
        }
    }
    Ok(recovered)
}

/// Finish or undo every operation session `s` left, then remove the session.
/// The caller holds `s`'s lease (`lease`), taken over from its dead owner.
pub(crate) async fn recover_session(state: &mut FsState, s: u64, lease: WriteLease) -> Result<()> {
    fence_all(state, lease).await?;
    let prefix = key::pending_prefix(s);
    loop {
        let (keys, has_more) = state.kv_range_page(&prefix, &prefix, 256).await?;
        for k in &keys {
            let Some(bytes) = state.kv_get_opt(k).await? else { continue };
            match schema::decode_pending(&bytes).map_err(|e| anyhow!("pending record: {e}"))? {
                PendingOp::Publish { parent, name, new_ino } => {
                    let named = state.kv_get_opt(&key::dirent_key(parent, &name)).await?;
                    let published = named
                        .as_deref()
                        .and_then(|v| schema::decode_dirent(v).ok())
                        .is_some_and(|d| d.child_inode == new_ino);
                    if !published {
                        segment::reclaim(state, new_ino, None, lease).await?;
                        state.kv_delete_fenced(&key::inode_key(new_ino), lease).await?;
                    }
                }
                PendingOp::Retire { parent, name, ino, successor } => {
                    let named = state.kv_get_opt(&key::dirent_key(parent, &name)).await?;
                    let holder = named.as_deref().and_then(|v| schema::decode_dirent(v).ok()).map(|d| d.child_inode);
                    // Only this session's own swap landing hands the name to
                    // exactly `successor`; any other holder retired `ino`
                    // itself, and dropping a name from it again could delete
                    // an inode another link still names.
                    if holder == successor {
                        drop_name_of(state, ino).await?;
                    }
                }
                PendingOp::Part { upload, part, data_ino } => {
                    crate::multipart::recover_part(state, s, upload, part, data_ino, lease).await?;
                }
                PendingOp::Complete { upload } => {
                    let (_, new_ino) = key::parse_pending_key(k).ok_or_else(|| anyhow!("pending key {k:?}"))?;
                    crate::multipart::recover_complete(state, s, upload, new_ino, lease).await?;
                }
            }
            state.kv_delete_fenced(k, lease).await?;
        }
        if !has_more {
            break;
        }
    }
    state.kv_delete_fenced(&key::session_key(s), lease).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn etags_round_trip() {
        let e = etag(0x1234, 7);
        assert_eq!(parse_etag(&format!("\"{e}\"")), Some((0x1234, 7)));
        assert_eq!(parse_etag(&format!("W/\"{e}\"")), Some((0x1234, 7)));
        assert_eq!(parse_etag("\"d41d8cd98f00b204e9800998ecf8427e-2\""), None);
    }
}
