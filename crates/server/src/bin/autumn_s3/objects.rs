//! The autumn `fs/` tree seen as S3 buckets and objects.
//!
//! Mapping: `s3://<bucket>/<key>` is the autumn path `/<bucket>/<key>`, so the
//! first level under the `fs/` root is the bucket list and everything below it
//! is object keys. Directories are not objects; they surface only as
//! `CommonPrefixes` in a delimited listing, which is what `aws s3 ls` shows.

use std::rc::Rc;

use anyhow::{anyhow, Result};
use autumn_fs::dir::DirChild;
use autumn_fs::read::{self, ReadPlan};
use autumn_fs::schema::{self, DT_DIR};
use autumn_fs::state::FsState;
use autumn_fs::{dir, key, meta};

use crate::listing::{list_page, DirSource, Item};
use crate::s3::ObjectRow;

/// `FsState` is `!Send` by design (it holds `Rc`s into the compio runtime), so
/// the whole gateway runs on one compio thread.
///
/// The mutex is an ASYNC one, and it matters which. Every namespace op here
/// awaits an RPC while holding `&mut FsState`; with a `RefCell` the second
/// concurrent request would panic on `already borrowed`, and the streamer
/// opens many parallel ranged GETs, so that is the normal case, not a corner.
/// An async mutex makes those requests queue instead.
///
/// Serializing metadata is the same trade the fuse mount makes — its dispatch
/// loop holds the only `&mut FsState` — and it costs nothing on the hot path,
/// because the read path splits into `prepare` (needs the state, cheap) and
/// `execute` (no state, does the I/O). Only `prepare` takes the lock, so the
/// actual chunk fan-out of concurrent GETs still overlaps.
pub type Fs = Rc<futures::lock::Mutex<FsState>>;

/// A file's identity as S3 reports it.
pub struct Stat {
    pub ino: u64,
    pub size: u64,
    pub mtime_secs: i64,
    pub etag: String,
}

/// Stable synthetic ETag. Not an MD5 — S3 only requires opacity unless the
/// client is verifying a multipart upload, which a read-only gateway never
/// serves. Changes whenever the file does.
fn etag(ino: u64, size: u64, mtime: i64) -> String {
    format!("{ino:x}-{size:x}-{mtime:x}")
}

/// Resolve an absolute autumn path to its inode, or `None` if absent.
async fn resolve(fs: &Fs, path: &str) -> Result<Option<u64>> {
    let mut st = fs.lock().await;
    dir::resolve(&mut st, path).await
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
    let Some(ino) = resolve(fs, &format!("/{bucket}")).await? else {
        return Ok(false);
    };
    let mut st = fs.lock().await;
    let m = meta::get_inode(&mut st, ino).await?;
    Ok(m.mode & 0o170_000 == 0o040_000)
}

/// Stat one object. `None` means no such key (or the key names a directory,
/// which is not an object).
pub async fn stat(fs: &Fs, bucket: &str, key: &str) -> Result<Option<Stat>> {
    let Some(ino) = resolve(fs, &format!("/{bucket}/{key}")).await? else {
        return Ok(None);
    };
    let mut st = fs.lock().await;
    let m = meta::get_inode(&mut st, ino).await?;
    // S_IFDIR — a directory is a prefix, never an object.
    if m.mode & 0o170_000 == 0o040_000 {
        return Ok(None);
    }
    Ok(Some(Stat {
        ino,
        size: m.size,
        mtime_secs: m.mtime_secs,
        etag: etag(ino, m.size, m.mtime_secs),
    }))
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
                let m = schema::decode_inode_meta(&bytes).map_err(|e| anyhow!("inode {ino}: {e}"))?;
                rows.push(ObjectRow {
                    key,
                    size: m.size,
                    mtime_secs: m.mtime_secs,
                    etag: etag(ino, m.size, m.mtime_secs),
                });
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
/// parallel across extents.
pub async fn run_read(plan: ReadPlan) -> Result<Vec<u8>> {
    read::execute(plan).await
}
