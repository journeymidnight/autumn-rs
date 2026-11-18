//! The autumn `fs/` tree seen as S3 buckets and objects.
//!
//! Mapping: `s3://<bucket>/<key>` is the autumn path `/<bucket>/<key>`, so the
//! first level under the `fs/` root is the bucket list and everything below it
//! is object keys. Directories are not objects; they surface only as
//! `CommonPrefixes` in a delimited listing, which is what `aws s3 ls` shows.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use autumn_client::lease::{self, AcquireResult};
use autumn_client::ClusterClient;
use autumn_fs::dir::DirChild;
use autumn_fs::read::{self, ReadPlan};
use autumn_fs::schema::{self, InodeMeta, DT_DIR};
use autumn_fs::state::{FsState, FuseLease};
use autumn_fs::{dir, key, publish};
use autumn_rpc::manager_rpc::LEASE_MODE_STABLE;

use crate::listing::{list_page, DirSource, Item};
use crate::s3::ObjectRow;

/// `FsState` is `!Send` by design (it holds `Rc`s into the compio runtime), so
/// each gateway worker runs on one compio thread with its own state.
///
/// The mutex is an ASYNC one, and it matters which. Every namespace op here
/// awaits an RPC while holding `&mut FsState`; with a `RefCell` the second
/// concurrent request would panic on `already borrowed`, and the streamer
/// opens many parallel ranged GETs, so that is the normal case, not a corner.
/// An async mutex makes those requests queue instead.
///
/// Serializing metadata is the same trade the fuse mount makes — its dispatch
/// loop holds the only `&mut FsState` — and it costs nothing on the hot paths,
/// because both of them split off the part that needs no state: a read into
/// `prepare` (needs the state, cheap) and `execute` (no state, does the I/O),
/// a write into begin/finish (need the state) and the body's data writes
/// (client only). Only the stateful halves take the lock, so the data I/O of
/// concurrent requests still overlaps.
///
/// The mutex is FIFO. `futures::lock::Mutex` is not: on this one thread a
/// woken waiter runs only after requests arriving meanwhile have taken the
/// lock again, and under 64 concurrent PUTs some waited 40 s.
///
/// Reads that need only the KV (HEAD, a GET of an inline object) use
/// `client` and never queue on the lock: with every GET's lookups under it, a
/// worker served one GET at a time, and 4 KiB GETs stopped at ~16K/s for 8
/// workers whatever the concurrency.
pub type Fs = Rc<Shared>;
pub type FsLock<T> = tokio::sync::Mutex<T>;

pub struct Shared {
    state: FsLock<FsState>,
    /// The state's own client and lease table.
    pub client: Rc<ClusterClient>,
    held_leases: Rc<RefCell<HashMap<u64, FuseLease>>>,
    /// The bucket and prefix directories GET and HEAD walk through.
    dirs: dir::DirCache,
}

impl Shared {
    pub fn new(state: FsState) -> Self {
        Shared {
            client: state.client.clone(),
            held_leases: state.held_leases.clone(),
            dirs: dir::DirCache::default(),
            state: FsLock::new(state),
        }
    }

    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, FsState> {
        self.state.lock().await
    }
}

/// A file's identity as S3 reports it.
pub struct Stat {
    pub ino: u64,
    pub size: u64,
    pub mtime_secs: i64,
    pub etag: String,
}

/// The S3 ETag: inode and content generation, so a same-size rewrite in the
/// same second still changes it, and `If-Match` can name one version exactly.
pub fn etag(ino: u64, generation: u64) -> String {
    publish::etag(ino, generation)
}

fn is_dir(m: &InodeMeta) -> bool {
    m.mode & 0o170_000 == 0o040_000
}

fn is_symlink(m: &InodeMeta) -> bool {
    m.mode & autumn_fs::meta::S_IFMT == autumn_fs::meta::S_IFLNK
}

fn stat_of(ino: u64, m: &InodeMeta) -> Stat {
    Stat { ino, size: m.size, mtime_secs: m.mtime_secs, etag: etag(ino, m.generation) }
}

/// An inode's metadata straight from the KV, never from this worker's cache:
/// a mount writes files in place, so a cached copy can carry a stale
/// generation and hand out a stale ETag.
pub async fn inode_meta(st: &mut FsState, ino: u64) -> Result<Option<InodeMeta>> {
    inode_meta_with(&st.client, ino).await
}

async fn inode_meta_with(client: &ClusterClient, ino: u64) -> Result<Option<InodeMeta>> {
    match client.get(&key::inode_key(ino)).await.map_err(|e| anyhow!("KV get: {e}"))? {
        Some(b) => Ok(Some(schema::decode_inode_meta(&b).map_err(|e| anyhow!("inode {ino}: {e}"))?)),
        None => Ok(None),
    }
}

/// The directory a bucket names, or `None` if there is no such bucket.
pub async fn bucket_dir(st: &mut FsState, bucket: &str) -> Result<Option<u64>> {
    Ok(dir::resolve_typed(st, &format!("/{bucket}")).await?.filter(|&(_, t)| t == DT_DIR).map(|(ino, _)| ino))
}

/// Resolve an absolute autumn path to its inode, following symbolic links,
/// or `None` if absent.
async fn resolve(fs: &Fs, path: &str) -> Result<Option<u64>> {
    let mut st = fs.lock().await;
    follow(&mut st, path).await
}

/// The inode a path leads to, through any symbolic links (a HuggingFace
/// snapshot's files are links into its `blobs/`), and its type. A link that
/// loops leads nowhere, like a dangling one: no object, not a failed request.
async fn follow_typed(st: &mut FsState, path: &str) -> Result<Option<(u64, u8)>> {
    follow_typed_with(&st.client, path).await
}

async fn follow_typed_with(client: &ClusterClient, path: &str) -> Result<Option<(u64, u8)>> {
    match dir::resolve_follow_with(client, path, true).await {
        Err(e) if e.to_string().starts_with("ELOOP") => Ok(None),
        r => r,
    }
}

async fn follow(st: &mut FsState, path: &str) -> Result<Option<u64>> {
    follow_with(&st.client, path).await
}

async fn follow_with(client: &ClusterClient, path: &str) -> Result<Option<u64>> {
    Ok(follow_typed_with(client, path).await?.map(|(ino, _)| ino))
}

/// The inode an object's path leads to, through this worker's directory
/// cache. A walk trusting the cache that finds nothing is redone reading every
/// entry, which refreshes the cache (the only way it is updated); `fresh`
/// goes straight to that, for a retry after the inode vanished. A directory
/// renamed elsewhere is therefore still followed by its old name until a
/// lookup under it fails, which is why fs and S3 should not serve one tree at
/// once (docs/ops.md).
async fn lookup(fs: &Fs, path: &str, fresh: bool) -> Result<Option<u64>> {
    if !fresh {
        if let Some(ino) = follow_cached(fs, path, dir::DirCacheUse::Trust).await? {
            return Ok(Some(ino));
        }
    }
    follow_cached(fs, path, dir::DirCacheUse::Refresh).await
}

async fn follow_cached(fs: &Fs, path: &str, usage: dir::DirCacheUse) -> Result<Option<u64>> {
    match dir::resolve_follow_cached(&fs.client, path, true, &fs.dirs, usage).await {
        Err(e) if e.to_string().starts_with("ELOOP") => Ok(None),
        r => Ok(r?.map(|(ino, _)| ino)),
    }
}

/// The `fs/` root's subdirectories, which are this gateway's buckets.
pub async fn list_buckets(fs: &Fs) -> Result<Vec<String>> {
    let Some(root) = resolve(fs, "/").await? else {
        return Ok(Vec::new());
    };
    let mut st = fs.lock().await;
    let entries = dir::readdir(&mut st, root, 0).await?;
    let mut out: Vec<String> = entries
        .into_iter()
        .filter(|e| e.kind == DT_DIR)
        .map(|e| e.name.to_string_lossy().into_owned())
        .filter(|n| n != "." && n != "..")
        .collect();
    out.sort();
    Ok(out)
}

/// Only first-level directories are buckets; a file with the same name is not.
pub async fn bucket_exists(fs: &Fs, bucket: &str) -> Result<bool> {
    let mut st = fs.lock().await;
    Ok(bucket_dir(&mut st, bucket).await?.is_some())
}

/// Stat one object. `None` means no such key (or the key names a directory,
/// which is not an object).
pub async fn stat(fs: &Fs, bucket: &str, key: &str) -> Result<Option<Stat>> {
    let path = format!("/{bucket}/{key}");
    for attempt in 0..2 {
        let Some(ino) = lookup(fs, &path, attempt > 0).await? else {
            return Ok(None);
        };
        match inode_meta_with(&fs.client, ino).await? {
            None => continue,
            m => return Ok(m.filter(|m| !is_dir(m)).map(|m| stat_of(ino, &m))),
        }
    }
    Ok(None)
}

// ── pinning an object for a GET ─────────────────────────────────────────────

/// How long an unused STABLE pin is kept before it is handed back. Ranged
/// GETs of one file arrive one after another (the weight streamer, a Lance
/// scan); without the linger each would pay a manager acquire and release.
/// While it lingers a mount's writer on the file gets EBUSY, so it is short
/// (`--pin-linger-ms`).
fn pin_linger() -> Duration {
    Duration::from_millis(super::params().pin_linger_ms.get())
}

/// Why an object could not be pinned.
pub enum OpenError {
    /// Another client is changing it in place (or reclaiming it).
    Busy(String),
    Other(anyhow::Error),
}

impl From<anyhow::Error> for OpenError {
    fn from(e: anyhow::Error) -> Self {
        OpenError::Other(e)
    }
}

/// A STABLE lease held for one request. While any is held no other client can
/// change the file's content or reclaim its data, so a GET streams one
/// version whole. This worker's requests share one lease per inode
/// (`FuseLease.reader_refs` counts them); the shared lease heartbeat renews
/// it with the rest of `held_leases`.
pub struct Pin {
    fs: Fs,
    ino: u64,
    held: Rc<RefCell<HashMap<u64, FuseLease>>>,
}

impl Pin {
    /// Whether the lease is still held. A heartbeat that finds it gone (a
    /// manager failover forgets STABLE) or a lost invalidation stream drops
    /// it; a GET then stops rather than risk streaming a changed file.
    pub fn alive(&self) -> bool {
        self.held.borrow().get(&self.ino).is_some_and(|l| l.mode == LEASE_MODE_STABLE)
    }
}

impl Drop for Pin {
    fn drop(&mut self) {
        let (fs, ino) = (self.fs.clone(), self.ino);
        compio::runtime::spawn(async move {
            let mut st = fs.lock().await;
            unpin_locked(&fs, &mut st, ino);
        })
        .detach();
    }
}

/// Take (or share) this worker's STABLE lease on `ino`. Under the state lock,
/// so acquire and release never interleave for one inode.
async fn pin_locked(st: &mut FsState, ino: u64) -> Result<(), OpenError> {
    let shared = match st.held_leases.borrow_mut().get_mut(&ino) {
        Some(l) if l.mode == LEASE_MODE_STABLE => {
            l.reader_refs += 1;
            true
        }
        // Nothing but STABLE is ever taken on a data file in the gateway.
        Some(l) => return Err(OpenError::Other(anyhow!("inode {ino} is held here in mode {}", l.mode))),
        None => false,
    };
    if shared {
        return Ok(());
    }
    let (client, id) = (st.client.clone(), st.client_id.clone());
    match lease::acquire(&client, &id, ino, LEASE_MODE_STABLE).await.map_err(|e| anyhow!("pin {ino}: {e}"))? {
        AcquireResult::Granted(info) => {
            st.held_leases.borrow_mut().insert(
                ino,
                FuseLease { writer_refs: 0, reader_refs: 1, mode: LEASE_MODE_STABLE, lease_epoch: info.version, revoked: false },
            );
            Ok(())
        }
        AcquireResult::Conflict { manager_message } | AcquireResult::RevokePending { manager_message, .. } => {
            Err(OpenError::Busy(manager_message))
        }
    }
}

/// Drop one request's share; the last one schedules the release.
fn unpin_locked(fs: &Fs, st: &mut FsState, ino: u64) {
    let idle = match st.held_leases.borrow_mut().get_mut(&ino) {
        Some(l) if l.mode == LEASE_MODE_STABLE => {
            l.reader_refs = l.reader_refs.saturating_sub(1);
            l.reader_refs == 0
        }
        _ => false,
    };
    if idle {
        let fs = fs.clone();
        compio::runtime::spawn(async move {
            compio::time::sleep(pin_linger()).await;
            let mut st = fs.lock().await;
            release_if_idle(&mut st, ino).await;
        })
        .detach();
    }
}

/// Hand an unused pin back, then reclaim the file if it became unreachable
/// while pinned here: a replace or delete on this worker defers that
/// (`FsState::unlinked_open`) rather than hand the reclaimer a file this
/// worker is still streaming.
async fn release_if_idle(st: &mut FsState, ino: u64) {
    let idle = st
        .held_leases
        .borrow()
        .get(&ino)
        .is_some_and(|l| l.mode == LEASE_MODE_STABLE && l.reader_refs == 0);
    if !idle {
        return;
    }
    st.held_leases.borrow_mut().remove(&ino);
    let (client, id) = (st.client.clone(), st.client_id.clone());
    if let Err(e) = lease::release(&client, &id, ino).await {
        tracing::warn!(ino, error = %e, "releasing a GET pin failed; its TTL ends it");
    }
    if st.unlinked_open.remove(&ino) {
        // Released above, so this client no longer stops the reclaimer.
        autumn_fs::extent::reclaim_now_or_later(st, ino).await;
    }
}

/// An object pinned for reading, with the metadata read under the pin, so
/// the headers and the body describe the same version.
pub struct Opened {
    pub stat: Stat,
    pub pin: Pin,
}

/// What a GET reads: the bytes themselves, or a pin to stream them under.
pub enum ForGet {
    /// A file whose whole content is in its inode. The one KV get that read
    /// it is one version, and there is no data to reclaim under it, so it
    /// needs no pin and costs the manager nothing.
    Inline { stat: Stat, data: Vec<u8> },
    Pinned(Opened),
}

/// Whether the inode holds the file's whole content inline.
fn whole_inline(m: &InodeMeta) -> bool {
    m.inline_data.as_ref().is_some_and(|d| d.len() as u64 == m.size) && m.segments.is_none() && m.stripe.is_none()
}

/// Open an object for a GET. `None` is no such key (or a directory).
pub async fn open_for_get(fs: &Fs, bucket: &str, key: &str) -> Result<Option<ForGet>, OpenError> {
    let path = format!("/{bucket}/{key}");
    // The name can move between the lookup and the pin, and an inode
    // reclaimed in that gap is simply gone: look again.
    for attempt in 0..3 {
        // The lookups take no lock: they need only the client, and under the
        // lock a worker would serve one GET at a time.
        let Some(ino) = lookup(fs, &path, attempt > 0).await? else {
            return Ok(None);
        };
        // A file this worker pins already is streamed, not inline (the
        // ranged GETs of one large file): skip straight to sharing its pin.
        if !fs.held_leases.borrow().contains_key(&ino) {
            match inode_meta_with(&fs.client, ino).await? {
                None => continue,
                Some(m) if is_dir(&m) => return Ok(None),
                Some(mut m) if whole_inline(&m) => {
                    let stat = stat_of(ino, &m);
                    let data = m.inline_data.take().expect("checked inline");
                    return Ok(Some(ForGet::Inline { stat, data }));
                }
                Some(_) => {}
            }
        }
        match pin(fs, ino).await? {
            Pinned::Object(o) => return Ok(Some(ForGet::Pinned(o))),
            Pinned::NotAnObject => return Ok(None),
            Pinned::Gone => {}
        }
    }
    Ok(None)
}

/// Resolve and pin an object. `None` is no such key (or a directory).
pub async fn open_pinned(fs: &Fs, bucket: &str, key: &str) -> Result<Option<Opened>, OpenError> {
    let path = format!("/{bucket}/{key}");
    for attempt in 0..3 {
        let Some(ino) = lookup(fs, &path, attempt > 0).await? else {
            return Ok(None);
        };
        match pin(fs, ino).await? {
            Pinned::Object(o) => return Ok(Some(o)),
            Pinned::NotAnObject => return Ok(None),
            Pinned::Gone => {}
        }
    }
    Ok(None)
}

enum Pinned {
    Object(Opened),
    /// A directory.
    NotAnObject,
    /// Reclaimed since its name was read.
    Gone,
}

/// Pin `ino` and read its metadata under the pin.
async fn pin(fs: &Fs, ino: u64) -> Result<Pinned, OpenError> {
    let mut st = fs.lock().await;
    pin_locked(&mut st, ino).await?;
    match inode_meta(&mut st, ino).await {
        Ok(Some(m)) if !is_dir(&m) => {
            let held = st.held_leases.clone();
            Ok(Pinned::Object(Opened { stat: stat_of(ino, &m), pin: Pin { fs: fs.clone(), ino, held } }))
        }
        Ok(Some(_)) => {
            unpin_locked(fs, &mut st, ino);
            Ok(Pinned::NotAnObject)
        }
        Ok(None) => {
            unpin_locked(fs, &mut st, ino);
            Ok(Pinned::Gone)
        }
        Err(e) => {
            unpin_locked(fs, &mut st, ino);
            Err(e.into())
        }
    }
}

/// The result of one `ListObjectsV2` page.
pub struct Listing {
    pub rows: Vec<ObjectRow>,
    pub common_prefixes: Vec<String>,
    pub next_token: Option<String>,
}

/// Reads directories for the listing walk, taking the state lock per page.
struct FsDirs<'a>(&'a Fs);

impl DirSource for FsDirs<'_> {
    async fn children(&mut self, dir: u64, from: &[u8], limit: u32) -> Result<(Vec<DirChild>, Option<Vec<u8>>)> {
        let mut st = self.0.lock().await;
        dir::list_children(&mut st, dir, from, limit).await
    }

    async fn lookup(&mut self, dir: u64, names: &[Vec<u8>]) -> Result<Vec<DirChild>> {
        let mut st = self.0.lock().await;
        dir::lookup_children(&mut st, dir, names).await
    }
}

/// List a bucket. `delimiter` is honoured only for the `/` case that S3
/// clients actually use; any other delimiter falls back to a flat listing,
/// which is a superset and keeps `s3_glob`'s client-side filter correct.
pub async fn list_objects(
    fs: &Fs,
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    start_after: Option<&str>,
    max_keys: usize,
) -> Result<Option<Listing>> {
    if !bucket_exists(fs, bucket).await? {
        return Ok(None);
    }
    // The prefix up to its last `/` names the directory to start from; the
    // rest filters names in it. A missing directory is an empty listing.
    let dir_part = &prefix[..prefix.rfind('/').map_or(0, |i| i + 1)];
    let Some(root) = resolve(fs, &format!("/{bucket}/{dir_part}")).await? else {
        return Ok(Some(Listing { rows: Vec::new(), common_prefixes: Vec::new(), next_token: None }));
    };
    let recursive = delimiter != Some("/");
    let (items, next_token) =
        list_page(&mut FsDirs(fs), root, prefix, recursive, start_after, max_keys).await?;

    let inos: Vec<u64> = items
        .iter()
        .filter_map(|i| match i {
            Item::Object { ino, .. } => Some(*ino),
            Item::Prefix(_) => None,
        })
        .collect();
    let metas = {
        let st = fs.lock().await;
        let keys: Vec<Vec<u8>> = inos.iter().map(|&i| key::inode_key(i)).collect();
        let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        st.client.get_many(&refs).await
    };
    let mut metas = metas.into_iter();
    let (mut rows, mut common_prefixes) = (Vec::new(), Vec::new());
    for item in items {
        match item {
            Item::Prefix(p) => common_prefixes.push(p),
            Item::Object { key, ino } => {
                let m = metas.next().expect("one meta per object");
                // An object deleted since the walk read its name is gone; the
                // token still resumes after it.
                let Some(bytes) = m.map_err(|e| anyhow!("KV get inode {ino}: {e}"))? else {
                    continue;
                };
                let mut m = schema::decode_inode_meta(&bytes).map_err(|e| anyhow!("inode {ino}: {e}"))?;
                let mut ino = ino;
                if is_symlink(&m) {
                    // Listed as the file it leads to, as a GET of the key
                    // reads it; a link to a directory or to nothing is no
                    // object.
                    let mut st = fs.lock().await;
                    let target = follow_typed(&mut st, &format!("/{bucket}/{key}")).await?;
                    let Some((t, schema::DT_REG)) = target else { continue };
                    let Some(tm) = inode_meta(&mut st, t).await? else { continue };
                    (ino, m) = (t, tm);
                }
                rows.push(ObjectRow { key, size: m.size, mtime_secs: m.mtime_secs, etag: etag(ino, m.generation) });
            }
        }
    }
    Ok(Some(Listing { rows, common_prefixes, next_token }))
}

/// Plan a read of `[offset, offset+len)`. Holds the state lock only for the
/// routing lookup; `len` must fit a `u32` because that is the read RPC's size
/// field. Pair with [`run_read`], which does the I/O with the lock released.
pub async fn plan_read(fs: &Fs, ino: u64, offset: u64, len: u32) -> Result<ReadPlan> {
    let mut st = fs.lock().await;
    read::prepare(&mut st, ino, offset as i64, len).await
}

/// Execute a planned read. Takes no state, so concurrent GETs fan out in
/// parallel across extents. A chunk that is one whole extent comes back in
/// the buffer it was received into (`read::execute_bytes`).
pub async fn run_read(plan: ReadPlan) -> Result<Bytes> {
    read::execute_bytes(plan).await
}

#[cfg(test)]
mod fs_lock_tests {
    use std::rc::Rc;
    use std::time::Duration;

    use futures::FutureExt;

    use super::FsLock;

    /// A request queued on the lock gets it before one that arrives after the
    /// release; otherwise a steady stream of arrivals starves it.
    #[test]
    fn a_queued_waiter_is_served_before_a_later_arrival() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let lock = Rc::new(FsLock::new(()));
            let held = lock.lock().await;
            let waiter = {
                let lock = lock.clone();
                compio::runtime::spawn(async move {
                    let _g = lock.lock().await;
                })
            };
            compio::time::sleep(Duration::from_millis(10)).await;
            drop(held);
            assert!(lock.lock().now_or_never().is_none(), "a later arrival took the lock ahead of the queued waiter");
            waiter.await.unwrap();
        });
    }
}
