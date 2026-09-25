//! S3 multipart uploads over the FS core.
//!
//! Each part is written once, as its own immutable data object, and
//! CompleteMultipartUpload turns the ordered parts into a segmented file whose
//! map names those objects in place. **Complete reads, copies and rewrites no
//! part body**: it reads part records, writes the file's records, map and
//! inode, and publishes the name — work that grows with the part count, never
//! with the bytes.
//!
//! **One state machine, CASed.** The upload record (`key::upload_key`) moves
//! `Open → Completing → Completed` or `Open → Aborted` only by `compare_write`,
//! so a Complete and an Abort have exactly one winner. `Completing` names the
//! session doing it and freezes the list of data objects it publishes;
//! whatever a late or failed UploadPart writes afterwards is not in that list
//! and so can never be deleted out from under the file, nor sneak into it.
//!
//! **Ownership of data objects.** Every object an upload creates is recorded
//! at `key::upload_alloc_key(upload, session, object)` before it is written.
//! Until Complete the upload owns them all; Complete also records the frozen
//! objects under the file (`segc/`), which then owns them. A terminal upload's
//! cleanup deletes every recorded object that is not frozen into the file —
//! superseded retries of a part number, parts left out of the Complete list,
//! everything after an Abort — except those a live session is still writing
//! (it has a pending record): that session cleans its own when it sees the
//! upload is no longer open, and the upload record stays until nothing is left.
//!
//! **Sessions and recovery.** Every write is fenced by this session's lease
//! and each part upload and Complete records itself under the session
//! (`schema::PendingOp::{Part, Complete}`), so `publish::recover_dead_sessions`
//! finishes or undoes what a dead gateway left: see [`recover_part`] and
//! [`recover_complete`].
//!
//! **The publish is the commit point, as in `publish.rs`.** Nothing after the
//! dirent swap lands can fail the Complete, and a swap whose outcome is
//! unknown is never undone: the request may still land, and undoing would
//! leave the name pointing at a deleted inode. That case keeps the upload
//! `Completing` with its pending record; a retry by the same session finishes
//! it once the dirent names the file, and the session's recovery settles it
//! otherwise.

use std::collections::HashSet;

use anyhow::{anyhow, Result};

use autumn_client::lease::{self, AcquireResult};
use autumn_client::{ClusterClient, WriteLease};
use autumn_rpc::manager_rpc::LEASE_MODE_WRITE;

use crate::key;
use crate::meta;
use crate::publish::{self, Condition, PublishError};
use crate::schema::{self, PartRecord, PendingOp, SegcRecord, Segment, UploadRecord, UploadState, DT_REG};
use crate::segment;
use crate::state::{FsState, Reclaim};

/// Smallest part S3 accepts anywhere but last.
pub const MIN_PART: u64 = 5 << 20;
/// Highest part number S3 allows.
pub const MAX_PART_NUMBER: u32 = 10_000;
/// How long a finished Complete's answer outlives its upload record, so a
/// retry whose first reply was lost still gets it. SDK retries give up
/// within minutes (`object_store`: 3 min by default).
pub const COMPLETED_RETENTION_SECS: u64 = 3600;

/// Why a multipart operation did not happen, in the terms S3 reports.
#[derive(Debug)]
pub enum MultipartError {
    /// No such upload, or it was aborted or completed.
    NoSuchUpload,
    /// A part number out of range, a listed part that does not exist, or an
    /// ETag that is not the part's current one.
    InvalidPart(u32),
    /// The Complete list is not in strictly ascending part order.
    InvalidPartOrder,
    /// A part other than the last is smaller than [`MIN_PART`].
    EntityTooSmall(u32),
    /// The Complete list is empty.
    Malformed(&'static str),
    /// A Complete is in progress, or an in-place writer holds the target.
    Busy(String),
    /// The publish itself did not happen (its condition, a directory in the
    /// way), or — `PublishError::Other` — its outcome is unknown.
    Publish(PublishError),
    Other(anyhow::Error),
}

impl From<anyhow::Error> for MultipartError {
    fn from(e: anyhow::Error) -> Self {
        MultipartError::Other(e)
    }
}

impl std::fmt::Display for MultipartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MultipartError::NoSuchUpload => write!(f, "no such upload"),
            MultipartError::InvalidPart(p) => write!(f, "invalid part {p}"),
            MultipartError::InvalidPartOrder => write!(f, "parts not in ascending order"),
            MultipartError::EntityTooSmall(p) => write!(f, "part {p} is smaller than the minimum"),
            MultipartError::Malformed(m) => write!(f, "malformed: {m}"),
            MultipartError::Busy(m) => write!(f, "busy: {m}"),
            MultipartError::Publish(e) => write!(f, "{e}"),
            MultipartError::Other(e) => write!(f, "{e:#}"),
        }
    }
}

/// The ETag of one part upload: its data object and CRC32C. The object
/// number makes every attempt's ETag distinct, so a Complete list can only
/// name the attempt the client saw succeed.
pub fn part_etag(data_ino: u64, crc32c: u32) -> String {
    format!("{data_ino:016x}{crc32c:08x}")
}

fn etag_matches(given: &str, rec: &PartRecord) -> bool {
    given.trim().trim_matches('"') == part_etag(rec.data_ino, rec.crc32c)
}

async fn load(client: &ClusterClient, upload: u64) -> Result<Option<(Vec<u8>, UploadRecord)>> {
    let Some(bytes) = client.get(&key::upload_key(upload)).await.map_err(|e| anyhow!("KV get: {e}"))? else {
        return Ok(None);
    };
    let rec = schema::decode_upload(&bytes).map_err(|e| anyhow!("upload {upload}: {e}"))?;
    Ok(Some((bytes, rec)))
}

/// The upload's record, for a gateway checking that a request names the
/// upload's own key.
pub async fn get(state: &mut FsState, upload: u64) -> Result<Option<UploadRecord>> {
    Ok(load(&state.client, upload).await?.map(|(_, r)| r))
}

/// Start an upload that Complete will publish as `parent`/`name`. `s3_key`
/// is the S3 key, kept for listings. Returns the upload id.
pub async fn create(state: &mut FsState, parent: u64, name: &[u8], s3_key: &[u8]) -> Result<u64> {
    let lease = publish::session_lease(state).await?;
    let id = meta::alloc_inode(state).await?;
    let rec = UploadRecord { parent, name: name.to_vec(), key: s3_key.to_vec(), state: UploadState::Open };
    state.kv_put_fenced(&key::upload_key(id), &schema::encode_upload(&rec), lease).await?;
    Ok(id)
}

/// The error for a part request against an upload that is not open.
fn not_open(state: Option<&UploadState>) -> MultipartError {
    match state {
        Some(UploadState::Completing { .. }) => MultipartError::Busy("the upload is being completed".into()),
        _ => MultipartError::NoSuchUpload,
    }
}

// ── parts ────────────────────────────────────────────────────────────────────

/// One UploadPart in progress.
pub struct PartWriter {
    upload: u64,
    part: u32,
    lease: WriteLease,
    stream: segment::ObjectStream,
}

impl PartWriter {
    /// Begin part `part` of an open upload. Records itself under the session
    /// before anything is written.
    pub async fn begin(state: &mut FsState, upload: u64, part: u32) -> std::result::Result<Self, MultipartError> {
        if !(1..=MAX_PART_NUMBER).contains(&part) {
            return Err(MultipartError::InvalidPart(part));
        }
        let lease = publish::session_lease(state).await?;
        match load(&state.client, upload).await? {
            Some((_, r)) if r.state == UploadState::Open => {}
            other => return Err(not_open(other.as_ref().map(|(_, r)| &r.state))),
        }
        let s = lease.inode_hint;
        let stream = segment::ObjectStream::new(state, |d| key::upload_alloc_key(upload, s, d), lease).await?;
        let op = PendingOp::Part { upload, part, data_ino: stream.data_ino };
        state
            .kv_put_fenced(&key::pending_key(s, stream.data_ino), &schema::encode_pending(&op), lease)
            .await?;
        Ok(PartWriter { upload, part, lease, stream })
    }

    /// Append `data`. Needs only the client, so the caller may hold the
    /// `FsState` just for `begin` and `finish`.
    pub async fn write(&mut self, client: &ClusterClient, data: &[u8]) -> Result<()> {
        self.stream.write(client, data).await
    }

    /// Write the rest and make this the part's current data. Returns the
    /// part's ETag and size. Needs only the client, like `write`.
    pub async fn finish(mut self, client: &ClusterClient) -> std::result::Result<(String, u64), MultipartError> {
        let (size, crc32c) = self.stream.finish(client).await?;
        let d = self.stream.data_ino;
        let rec = PartRecord { data_ino: d, size, crc32c, lanes: self.stream.lanes, unit: self.stream.unit };
        // A plain put: a retry of the same part number replaces the record,
        // and the object it named stays recorded for the terminal cleanup.
        client
            .put_fenced(&key::upload_part_key(self.upload, self.part), &schema::encode_part(&rec), self.lease)
            .await
            .map_err(|e| anyhow!("KV put: {e}"))?;
        // Did the upload stay open until the part landed? A Complete that
        // froze its list before this put does not name `d`.
        let now = load(client, self.upload).await?;
        if now.as_ref().is_some_and(|(_, r)| owns(&r.state, d, true)) {
            client
                .delete_fenced(&key::pending_key(self.lease.inode_hint, d), self.lease)
                .await
                .map_err(|e| anyhow!("KV delete: {e}"))?;
            return Ok((part_etag(d, crc32c), size));
        }
        drop_part_object(client, self.upload, self.part, self.lease.inode_hint, d, self.lease).await?;
        Err(not_open(now.as_ref().map(|(_, r)| &r.state)))
    }

    /// Discard this part (a failed or cancelled request). Needs only the
    /// client: the object is this request's own and nobody reads it.
    pub async fn abort(self, client: &ClusterClient) -> Result<()> {
        drop_part_object(client, self.upload, self.part, self.lease.inode_hint, self.stream.data_ino, self.lease).await
    }
}

/// Whether data object `d` of an upload in `state` belongs to it (to the
/// open upload, or frozen into its file). `named` says whether the part
/// record still names `d`; an open upload keeps only what its records name.
fn owns(state: &UploadState, d: u64, named: bool) -> bool {
    match state {
        UploadState::Open => named,
        UploadState::Completing { frozen, .. } | UploadState::Completed { frozen, .. } => frozen.contains(&d),
        UploadState::Aborted => false,
    }
}

/// Delete data object `d` written by session `s` for `part`, with its
/// allocation record, the part record if it still names `d`, and the
/// pending record. Idempotent.
async fn drop_part_object(client: &ClusterClient, upload: u64, part: u32, s: u64, d: u64, lease: WriteLease) -> Result<()> {
    let pk = key::upload_part_key(upload, part);
    if let Some(cur) = client.get(&pk).await.map_err(|e| anyhow!("KV get: {e}"))? {
        let r = schema::decode_part(&cur).map_err(|e| anyhow!("part {upload}/{part}: {e}"))?;
        if r.data_ino == d {
            client.compare_write(&pk, Some(&cur), None, lease).await.map_err(|e| anyhow!("{e}"))?;
        }
    }
    delete_allocated(client, upload, s, d, lease).await?;
    client.delete_fenced(&key::pending_key(s, d), lease).await.map_err(|e| anyhow!("KV delete: {e}"))
}

/// Delete an upload's data object `d` (written by session `s`) and its
/// allocation record, which says how long it can be. The record goes last,
/// so a crash in between leaves it for a retry.
async fn delete_allocated(client: &ClusterClient, upload: u64, s: u64, d: u64, lease: WriteLease) -> Result<()> {
    let ak = key::upload_alloc_key(upload, s, d);
    if let Some(bytes) = client.get(&ak).await.map_err(|e| anyhow!("KV get: {e}"))? {
        if let SegcRecord::Object { len, lanes, unit } =
            schema::decode_segc(&bytes).map_err(|e| anyhow!("alloc {upload}/{d}: {e}"))?
        {
            segment::delete_object(client, d, len, lanes, unit, lease).await?;
        }
        client.delete_fenced(&ak, lease).await.map_err(|e| anyhow!("KV delete: {e}"))?;
    }
    Ok(())
}

// ── complete ─────────────────────────────────────────────────────────────────

/// What a Complete answers: the file it published, at the version it
/// published. The file is new, so that is its first generation — also for a
/// retry answered after the file was changed or deleted, as S3 answers it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Completed {
    pub ino: u64,
    pub generation: u64,
}

fn completed(ino: u64) -> Completed {
    Completed { ino, generation: meta::FIRST_GENERATION }
}

/// Publish the listed parts, in order, as the upload's file if `cond` holds.
/// Only metadata is read and written. `s3_key` must be the key the upload
/// was created for.
///
/// A retry of a Complete that already succeeded gets the same answer: from
/// the upload record while it lasts, then for [`COMPLETED_RETENTION_SECS`]
/// from the record `cleanup` leaves behind.
pub async fn complete(
    state: &mut FsState,
    upload: u64,
    s3_key: &[u8],
    list: &[(u32, String)],
    cond: Condition,
) -> std::result::Result<Completed, MultipartError> {
    let lease = publish::session_lease(state).await?;
    let s = lease.inode_hint;
    let Some((cur, rec)) = load(&state.client, upload).await? else {
        return completed_earlier(state, upload, s3_key).await;
    };
    if rec.key != s3_key {
        return Err(MultipartError::NoSuchUpload);
    }
    match &rec.state {
        UploadState::Open => {}
        // A retry whose earlier attempt succeeded but lost its reply.
        UploadState::Completed { new_ino, .. } => return Ok(completed(*new_ino)),
        // This session's own earlier Complete stopped here: its publish had
        // an unknown outcome, or what follows a landed publish failed. The
        // dirent says which; only a landed one can be finished now.
        UploadState::Completing { new_ino, session, frozen } if *session == s => {
            let (n, frozen) = (*new_ino, frozen.clone());
            if names_file(state, &rec, n).await? {
                settle(state, upload, &rec, n, frozen, lease).await;
                return Ok(completed(n));
            }
            return Err(MultipartError::Busy("an earlier Complete of this upload has an unknown outcome".into()));
        }
        UploadState::Completing { .. } => return Err(MultipartError::Busy("another Complete is in progress".into())),
        UploadState::Aborted => return Err(MultipartError::NoSuchUpload),
    }
    let parts = check_list(state, upload, list).await?;

    let new_ino = meta::alloc_inode(state).await?;
    let op = PendingOp::Complete { upload };
    state
        .kv_put_fenced(&key::pending_key(s, new_ino), &schema::encode_pending(&op), lease)
        .await?;
    let frozen: Vec<u64> = parts.iter().map(|p| p.data_ino).collect();
    let completing = UploadRecord {
        state: UploadState::Completing { new_ino, session: s, frozen: frozen.clone() },
        ..rec.clone()
    };
    // An error here leaves the pending record: the CAS may still land, and
    // the record is how recovery finds and reopens the upload if it does.
    let swapped = state
        .client
        .compare_write(&key::upload_key(upload), Some(&cur), Some(&schema::encode_upload(&completing)), lease)
        .await
        .map_err(|e| anyhow!("complete {upload}: {e}"))?;
    if !swapped {
        state.kv_delete_fenced(&key::pending_key(s, new_ino), lease).await?;
        return Err(match load(&state.client, upload).await? {
            Some((_, r)) => not_open(Some(&r.state)),
            None => MultipartError::NoSuchUpload,
        });
    }

    match write_and_publish(state, &rec, new_ino, &parts, cond, lease).await {
        Ok(()) => {
            settle(state, upload, &completing, new_ino, frozen, lease).await;
            Ok(completed(new_ino))
        }
        // The swap may still land: nothing is undone, and the upload stays
        // Completing under this session's pending record.
        Err(e @ MultipartError::Publish(PublishError::Other(_))) => Err(e),
        // Definitely not published.
        Err(e) => {
            undo_complete(state, upload, new_ino, lease).await?;
            state.kv_delete_fenced(&key::pending_key(s, new_ino), lease).await?;
            Err(e)
        }
    }
}

/// Check a Complete list against the part records: ascending part numbers,
/// every part present under the listed ETag, every part but the last at
/// least [`MIN_PART`]. Returns the records in list order.
async fn check_list(
    state: &mut FsState,
    upload: u64,
    list: &[(u32, String)],
) -> std::result::Result<Vec<PartRecord>, MultipartError> {
    if list.is_empty() {
        return Err(MultipartError::Malformed("a Complete names at least one part"));
    }
    if list.windows(2).any(|w| w[0].0 >= w[1].0) {
        return Err(MultipartError::InvalidPartOrder);
    }
    let keys: Vec<Vec<u8>> = list.iter().map(|(p, _)| key::upload_part_key(upload, *p)).collect();
    let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    let values = state.client.get_many(&refs).await;
    let mut out = Vec::with_capacity(list.len());
    for ((p, etag), v) in list.iter().zip(values) {
        let v = v.map_err(|e| anyhow!("part {upload}/{p}: {e}"))?;
        let Some(bytes) = v else { return Err(MultipartError::InvalidPart(*p)) };
        let rec = schema::decode_part(&bytes).map_err(|e| anyhow!("part {upload}/{p}: {e}"))?;
        if !etag_matches(etag, &rec) {
            return Err(MultipartError::InvalidPart(*p));
        }
        out.push(rec);
    }
    for (i, r) in out.iter().enumerate() {
        if i + 1 < out.len() && r.size < MIN_PART {
            return Err(MultipartError::EntityTooSmall(list[i].0));
        }
    }
    Ok(out)
}

/// Write the file (its object records, map and inode) and publish it.
async fn write_and_publish(
    state: &mut FsState,
    rec: &UploadRecord,
    new_ino: u64,
    parts: &[PartRecord],
    cond: Condition,
    lease: WriteLease,
) -> std::result::Result<(), MultipartError> {
    // The file owns the objects from here: a later overwrite or unlink of it
    // reclaims them through these records.
    let mut rkeys = Vec::with_capacity(parts.len());
    let mut rvals = Vec::with_capacity(parts.len());
    let mut segs: Vec<Segment> = Vec::with_capacity(parts.len());
    let mut off = 0u64;
    for p in parts {
        rkeys.push(key::segc_key(new_ino, p.data_ino));
        rvals.push(bytes::Bytes::from(schema::encode_segc(&SegcRecord::Object {
            len: p.size,
            lanes: p.lanes,
            unit: p.unit,
        })));
        if p.size > 0 {
            segs.push(Segment { off, len: p.size, data_ino: p.data_ino, data_off: 0, lanes: p.lanes, unit: p.unit });
        }
        off += p.size;
    }
    let items: Vec<(&[u8], bytes::Bytes, u64)> =
        rkeys.iter().zip(rvals).map(|(k, v)| (k.as_slice(), v, 0u64)).collect();
    for r in state.client.put_many_fenced(&items, lease).await {
        r.map_err(|e| anyhow!("file records of {new_ino}: {e}"))?;
    }
    let map = segment::publish_map(state, new_ino, segs, lease).await?;
    let mut m = meta::new_file_meta(0o644, unsafe { libc::getuid() }, unsafe { libc::getgid() });
    m.size = off;
    m.segments = Some(map);
    state
        .kv_put_fenced(&key::inode_key(new_ino), &schema::encode_inode_meta(&m), lease)
        .await?;
    publish::publish_inode(state, rec.parent, &rec.name, new_ino, DT_REG, cond, lease)
        .await
        .map_err(|e| match e {
            PublishError::Busy(m) => MultipartError::Busy(m),
            other => MultipartError::Publish(other),
        })
}

/// Whether the upload's name currently names `new_ino`.
async fn names_file(state: &mut FsState, rec: &UploadRecord, new_ino: u64) -> Result<bool> {
    let named = state.kv_get_opt(&key::dirent_key(rec.parent, &rec.name)).await?;
    Ok(named.as_deref().and_then(|v| schema::decode_dirent(v).ok()).is_some_and(|d| d.child_inode == new_ino))
}

/// After a Complete's publish landed: mark the upload Completed, drop the
/// pending record, reclaim the leftovers. None of it can fail the Complete;
/// whatever does not happen here is left for a retry, the session's recovery
/// or the sweep, and the order keeps each of those able to finish it.
async fn settle(
    state: &mut FsState,
    upload: u64,
    completing: &UploadRecord,
    new_ino: u64,
    frozen: Vec<u64>,
    lease: WriteLease,
) {
    if let Err(e) = finish_completed(state, upload, completing, new_ino, frozen, lease).await {
        tracing::warn!(upload, error = %e, "marking a published upload completed failed; left for a retry or recovery");
        return;
    }
    if let Err(e) = state.kv_delete_fenced(&key::pending_key(lease.inode_hint, new_ino), lease).await {
        tracing::warn!(upload, error = %e, "dropping a Complete's pending record failed; recovery drops it");
    }
    cleanup_now_or_later(state, upload, lease).await;
}

/// Reclaim a terminal upload: through the background reclaimer if the state
/// has one, else here. Failures are left to the sweep.
async fn cleanup_now_or_later(state: &mut FsState, upload: u64, lease: WriteLease) {
    if state.defer_reclaim(Reclaim::Upload(upload)) {
        return;
    }
    if let Err(e) = cleanup(state, upload, lease).await {
        tracing::warn!(upload, error = %e, "multipart cleanup incomplete; the sweep retries");
    }
}

async fn finish_completed(
    state: &mut FsState,
    upload: u64,
    completing: &UploadRecord,
    new_ino: u64,
    frozen: Vec<u64>,
    lease: WriteLease,
) -> Result<()> {
    let done = UploadRecord { state: UploadState::Completed { new_ino, frozen }, ..completing.clone() };
    let swapped = state
        .client
        .compare_write(
            &key::upload_key(upload),
            Some(&schema::encode_upload(completing)),
            Some(&schema::encode_upload(&done)),
            lease,
        )
        .await
        .map_err(|e| anyhow!("complete {upload}: {e}"))?;
    if !swapped {
        // Only the Completing session moves the record on, and a recovery
        // that took the session over fenced this one out first.
        return Err(anyhow!("upload {upload} left Completing under its own Complete"));
    }
    Ok(())
}

/// A Complete of an upload whose record is gone: the answer of the Complete
/// that finished it, if that was recent enough to be remembered.
async fn completed_earlier(state: &mut FsState, upload: u64, s3_key: &[u8]) -> std::result::Result<Completed, MultipartError> {
    let Some(bytes) = state.kv_get_opt(&key::completed_upload_key(upload)).await? else {
        return Err(MultipartError::NoSuchUpload);
    };
    let c = schema::decode_completed(&bytes).map_err(|e| anyhow!("completed upload {upload}: {e}"))?;
    if c.key != s3_key {
        return Err(MultipartError::NoSuchUpload);
    }
    Ok(completed(c.new_ino))
}

/// Undo an unpublished Complete: the file's records, map pages and inode go
/// (the objects stay: they are the upload's again), and the upload reopens.
async fn undo_complete(state: &mut FsState, upload: u64, new_ino: u64, lease: WriteLease) -> Result<()> {
    let prefix = key::segc_prefix(new_ino);
    loop {
        let (keys, has_more) = state.kv_range_page(&prefix, &prefix, 1024).await?;
        let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        let values = state.client.get_many(&refs).await;
        for (k, v) in keys.iter().zip(values) {
            let Some((_, id)) = key::parse_segc_key(k) else { continue };
            if let Some(bytes) = v.map_err(|e| anyhow!("{e}"))? {
                if let SegcRecord::Pages { count } =
                    schema::decode_segc(&bytes).map_err(|e| anyhow!("record {new_ino}/{id}: {e}"))?
                {
                    segment::delete_pages(&state.client, id, count, lease).await?;
                }
            }
        }
        for r in state.client.delete_many_fenced(&refs, lease).await {
            r.map_err(|e| anyhow!("records of {new_ino}: {e}"))?;
        }
        if !has_more {
            break;
        }
    }
    state.kv_delete_fenced(&key::inode_key(new_ino), lease).await?;
    if let Some((cur, rec)) = load(&state.client, upload).await? {
        if matches!(rec.state, UploadState::Completing { new_ino: n, .. } if n == new_ino) {
            let open = UploadRecord { state: UploadState::Open, ..rec };
            state
                .client
                .compare_write(&key::upload_key(upload), Some(&cur), Some(&schema::encode_upload(&open)), lease)
                .await
                .map_err(|e| anyhow!("reopen {upload}: {e}"))?;
        }
    }
    Ok(())
}

// ── abort ────────────────────────────────────────────────────────────────────

/// Abort an upload. Once this returns no Complete can publish it; its data
/// is reclaimed now (or by the state's background reclaimer) or, for parts
/// still being written, by their writers and the sweep. A Completed upload
/// is `NoSuchUpload`; one being completed by a live session is `Busy`.
pub async fn abort(state: &mut FsState, upload: u64) -> std::result::Result<(), MultipartError> {
    let lease = publish::session_lease(state).await?;
    loop {
        let Some((cur, rec)) = load(&state.client, upload).await? else {
            return Err(MultipartError::NoSuchUpload);
        };
        match &rec.state {
            UploadState::Open => {
                let aborted = UploadRecord { state: UploadState::Aborted, ..rec.clone() };
                let swapped = state
                    .client
                    .compare_write(&key::upload_key(upload), Some(&cur), Some(&schema::encode_upload(&aborted)), lease)
                    .await
                    .map_err(|e| anyhow!("abort {upload}: {e}"))?;
                if !swapped {
                    continue;
                }
            }
            UploadState::Aborted => {}
            UploadState::Completed { .. } => return Err(MultipartError::NoSuchUpload),
            UploadState::Completing { session, .. } => {
                // Its Complete decides unless its session is dead; then
                // recover that session and look again.
                if *session == lease.inode_hint || !take_over_dead(state, *session).await? {
                    return Err(MultipartError::Busy("the upload is being completed".into()));
                }
                continue;
            }
        }
        cleanup_now_or_later(state, upload, lease).await;
        return Ok(());
    }
}

/// If session `s`'s owner is gone, recover it. Returns whether it was.
async fn take_over_dead(state: &mut FsState, s: u64) -> Result<bool> {
    let cluster = state.client.clone();
    let id = state.client_id.clone();
    let epoch = match lease::acquire(&cluster, &id, s, LEASE_MODE_WRITE)
        .await
        .map_err(|e| anyhow!("session {s}: {e}"))?
    {
        AcquireResult::Granted(info) => info.version,
        _ => return Ok(false),
    };
    let r = publish::recover_session(state, s, WriteLease { inode_hint: s, lease_epoch: epoch }).await;
    let _ = lease::release(&cluster, &id, s).await;
    r.map(|_| true)
}

// ── cleanup ──────────────────────────────────────────────────────────────────

/// Reclaim a terminal upload: every data object it recorded that is not
/// frozen into its file, then its part records and the upload record. An
/// object a session is still writing (its pending record exists) is left to
/// that session, and so is the upload record, for a later sweep. A completed
/// upload leaves what its Complete answered behind (`remember_completed`).
/// Returns whether the upload is gone.
pub async fn cleanup(state: &mut FsState, upload: u64, lease: WriteLease) -> Result<bool> {
    let Some((cur, rec)) = load(&state.client, upload).await? else {
        return Ok(true);
    };
    let frozen: HashSet<u64> = match &rec.state {
        UploadState::Completed { frozen, .. } => frozen.iter().copied().collect(),
        UploadState::Aborted => HashSet::new(),
        _ => return Ok(false),
    };
    let prefix = key::upload_alloc_prefix(upload);
    let mut from = prefix.clone();
    let mut left = 0usize;
    loop {
        let (keys, has_more) = state.kv_range_page(&prefix, &from, 256).await?;
        for k in &keys {
            let Some((s, d)) = key::parse_upload_alloc_key(k) else { continue };
            if frozen.contains(&d) {
                // The file's own `segc/` record now accounts for it.
                state.kv_delete_fenced(k, lease).await?;
            } else if state.kv_get_opt(&key::pending_key(s, d)).await?.is_some() {
                left += 1;
            } else {
                delete_allocated(&state.client, upload, s, d, lease).await?;
            }
        }
        match (has_more, keys.last()) {
            (true, Some(last)) => from = crate::dir::name_successor(last),
            _ => break,
        }
    }
    if left > 0 {
        return Ok(false);
    }
    let pprefix = key::upload_part_prefix(upload);
    loop {
        let (keys, has_more) = state.kv_range_page(&pprefix, &pprefix, 1024).await?;
        let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        for r in state.client.delete_many_fenced(&refs, lease).await {
            r.map_err(|e| anyhow!("part records of {upload}: {e}"))?;
        }
        if !has_more {
            break;
        }
    }
    if let UploadState::Completed { new_ino, .. } = &rec.state {
        remember_completed(state, upload, &rec.key, *new_ino, lease).await?;
    }
    state
        .client
        .compare_write(&key::upload_key(upload), Some(&cur), None, lease)
        .await
        .map_err(|e| anyhow!("upload {upload}: {e}"))
}

fn now_secs() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

/// Keep a completed upload's answer for [`COMPLETED_RETENTION_SECS`] after
/// its record goes. The expiry entry is written first, so no answer is ever
/// left without one.
async fn remember_completed(state: &mut FsState, upload: u64, s3_key: &[u8], new_ino: u64, lease: WriteLease) -> Result<()> {
    let deadline = now_secs() + COMPLETED_RETENTION_SECS;
    state.kv_put_fenced(&key::completed_expiry_key(deadline, upload), b"", lease).await?;
    let c = schema::CompletedUpload { key: s3_key.to_vec(), new_ino };
    state
        .kv_put_fenced(&key::completed_upload_key(upload), &schema::encode_completed(&c), lease)
        .await
}

/// Forget completed uploads' answers whose retention has passed. Reads only
/// the expiry entries that are due. Returns how many were forgotten.
pub async fn expire_completed(state: &mut FsState, now_secs: u64) -> Result<usize> {
    let lease = publish::session_lease(state).await?;
    let prefix = key::completed_expiry_prefix();
    let mut n = 0;
    loop {
        let (keys, has_more) = state.kv_range_page(&prefix, &prefix, 256).await?;
        let due: Vec<(&Vec<u8>, u64)> = keys
            .iter()
            .filter_map(|k| key::parse_completed_expiry_key(k).map(|(t, id)| (k, t, id)))
            .take_while(|(_, t, _)| *t <= now_secs)
            .map(|(k, _, id)| (k, id))
            .collect();
        if due.is_empty() {
            return Ok(n);
        }
        // The answer goes before its expiry entry, so a crash in between
        // leaves the entry to retry.
        let answers: Vec<Vec<u8>> = due.iter().map(|(_, id)| key::completed_upload_key(*id)).collect();
        let refs: Vec<&[u8]> = answers.iter().map(Vec::as_slice).collect();
        for r in state.client.delete_many_fenced(&refs, lease).await {
            r.map_err(|e| anyhow!("completed uploads: {e}"))?;
        }
        let entries: Vec<&[u8]> = due.iter().map(|(k, _)| k.as_slice()).collect();
        for r in state.client.delete_many_fenced(&entries, lease).await {
            r.map_err(|e| anyhow!("completed upload expiries: {e}"))?;
        }
        n += due.len();
        if due.len() < keys.len() || !has_more {
            return Ok(n);
        }
    }
}

/// Finish the cleanup of every terminal upload, and forget completed
/// uploads' answers whose retention has passed. Returns the uploads removed.
/// Open and completing uploads are passed over, never touched.
pub async fn sweep_uploads(state: &mut FsState) -> Result<usize> {
    if let Err(e) = expire_completed(state, now_secs()).await {
        tracing::warn!(error = %e, "expiring completed uploads failed; retried next sweep");
    }
    let lease = publish::session_lease(state).await?;
    let prefix = key::upload_prefix();
    let mut from = prefix.clone();
    let mut n = 0;
    loop {
        let (keys, has_more) = state.kv_range_page(&prefix, &from, 256).await?;
        for k in &keys {
            let Some(id) = key::parse_upload_key(k) else { continue };
            match cleanup(state, id, lease).await {
                Ok(true) => n += 1,
                Ok(false) => {}
                Err(e) => tracing::warn!(upload = id, error = %e, "multipart cleanup failed; retried next sweep"),
            }
        }
        match (has_more, keys.last()) {
            (true, Some(last)) => from = crate::dir::name_successor(last),
            _ => return Ok(n),
        }
    }
}

// ── recovery of a dead session's operations ──────────────────────────────────

/// A dead session was writing object `d` for `part`: keep it if the upload
/// owns it, else delete it.
pub(crate) async fn recover_part(
    state: &mut FsState,
    s: u64,
    upload: u64,
    part: u32,
    d: u64,
    lease: WriteLease,
) -> Result<()> {
    let keep = match load(&state.client, upload).await? {
        Some((_, r)) => {
            let named = match state.kv_get_opt(&key::upload_part_key(upload, part)).await? {
                Some(b) => schema::decode_part(&b).map_err(|e| anyhow!("part {upload}/{part}: {e}"))?.data_ino == d,
                None => false,
            };
            owns(&r.state, d, named)
        }
        None => false,
    };
    if !keep {
        drop_part_object(&state.client, upload, part, s, d, lease).await?;
    }
    Ok(())
}

/// A dead session was completing `upload` into `new_ino`: finish it if the
/// name was published, else undo it and reopen the upload.
pub(crate) async fn recover_complete(state: &mut FsState, s: u64, upload: u64, new_ino: u64, lease: WriteLease) -> Result<()> {
    let Some((_, rec)) = load(&state.client, upload).await? else { return Ok(()) };
    let UploadState::Completing { new_ino: n, session, frozen } = &rec.state else {
        return Ok(());
    };
    if *n != new_ino || *session != s {
        return Ok(());
    }
    if names_file(state, &rec, new_ino).await? {
        finish_completed(state, upload, &rec, new_ino, frozen.clone(), lease).await?;
        if let Err(e) = cleanup(state, upload, lease).await {
            tracing::warn!(upload, error = %e, "multipart cleanup incomplete; the sweep retries");
        }
    } else {
        undo_complete(state, upload, new_ino, lease).await?;
    }
    Ok(())
}
