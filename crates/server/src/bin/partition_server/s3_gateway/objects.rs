//! The autumn `fs/` tree seen as S3 buckets and objects.
//!
//! Mapping: `s3://<bucket>/<key>` is the autumn path `/<bucket>/<key>`, so the
//! first level under the `fs/` root is the bucket list and everything below it
//! is object keys. Directories are not objects; they surface only as
//! `CommonPrefixes` in a delimited listing, which is what `aws s3 ls` shows.

use std::rc::Rc;

use anyhow::Result;
use autumn_fuse::read::{self, ReadPlan};
use autumn_fuse::schema::{DT_DIR, DT_REG};
use autumn_fuse::state::FsState;
use autumn_fuse::{dir, meta};

use super::s3::ObjectRow;

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

/// Cap on how many tree entries one undelimited (recursive) listing will walk.
/// A model directory is tens of files; this only guards against someone
/// listing the root of a large tree with no delimiter.
const MAX_WALK: usize = 100_000;

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
    if resolve(fs, &format!("/{bucket}")).await?.is_none() {
        return Ok(None);
    }

    // Split the prefix at its last `/`: everything before it names a real
    // directory to start from, everything after is a filename filter. This is
    // what turns `prefix=llama/model-` into "readdir llama/, keep model-*"
    // instead of a walk of the whole bucket.
    let (dir_part, name_part) = match prefix.rfind('/') {
        Some(i) => (&prefix[..=i], &prefix[i + 1..]),
        None => ("", prefix),
    };

    let mut keys: Vec<String> = Vec::new();
    let mut common: Vec<String> = Vec::new();

    if delimiter == Some("/") {
        let base = format!("/{bucket}/{dir_part}");
        if let Some(ino) = resolve(fs, &base).await? {
            let mut st = fs.lock().await;
            for e in dir::readdir(&mut st, ino, 0).await? {
                let name = e.name.to_string_lossy().into_owned();
                if name == "." || name == ".." || !name.starts_with(name_part) {
                    continue;
                }
                match e.kind {
                    DT_DIR => common.push(format!("{dir_part}{name}/")),
                    DT_REG => keys.push(format!("{dir_part}{name}")),
                    _ => {}
                }
            }
        }
    } else {
        walk(fs, bucket, dir_part, prefix, &mut keys).await?;
    }

    keys.sort();
    common.sort();

    // Both `continuation-token` and `start-after` mean the same thing here:
    // resume strictly after this key. Using the key itself as the token keeps
    // paging stateless.
    if let Some(after) = start_after {
        keys.retain(|k| k.as_str() > after);
        common.retain(|p| p.as_str() > after);
    }

    let truncated = keys.len() > max_keys;
    keys.truncate(max_keys);
    let next_token = if truncated { keys.last().cloned() } else { None };

    // Stat is one lookup per key; a model directory is tens of files, and the
    // inode cache absorbs repeats within a session.
    let mut rows = Vec::with_capacity(keys.len());
    for k in keys {
        if let Some(s) = stat(fs, bucket, &k).await? {
            rows.push(ObjectRow {
                key: k,
                size: s.size,
                mtime_secs: s.mtime_secs,
                etag: s.etag,
            });
        }
    }

    Ok(Some(Listing {
        rows,
        common_prefixes: common,
        next_token,
    }))
}

/// Depth-first walk under `dir_part`, collecting every regular file whose key
/// starts with `prefix`.
async fn walk(
    fs: &Fs,
    bucket: &str,
    dir_part: &str,
    prefix: &str,
    out: &mut Vec<String>,
) -> Result<()> {
    let mut stack = vec![dir_part.to_string()];
    let mut seen = 0usize;

    while let Some(rel) = stack.pop() {
        let Some(ino) = resolve(fs, &format!("/{bucket}/{rel}")).await? else {
            continue;
        };
        let entries = {
            let mut st = fs.lock().await;
            dir::readdir(&mut st, ino, 0).await?
        };
        for e in entries {
            let name = e.name.to_string_lossy().into_owned();
            if name == "." || name == ".." {
                continue;
            }
            seen += 1;
            if seen > MAX_WALK {
                tracing::warn!(
                    bucket, prefix, MAX_WALK,
                    "listing truncated at the walk cap; use a delimiter or a narrower prefix"
                );
                return Ok(());
            }
            let key = format!("{rel}{name}");
            match e.kind {
                DT_DIR => stack.push(format!("{key}/")),
                DT_REG if key.starts_with(prefix) => out.push(key),
                _ => {}
            }
        }
    }
    Ok(())
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
