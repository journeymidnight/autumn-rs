//! The S3 write operations: PutObject, CopyObject, DeleteObject,
//! DeleteObjects and the multipart upload calls.
//!
//! Every one of them is a thin adapter over `autumn-fs`: a new object is a
//! `publish::NewFile` published with one fenced compare-and-write on its
//! dirent, so `If-None-Match: *` and `If-Match` are decided by the partition
//! server, never by a read followed by a write; multipart is
//! `autumn_fs::multipart`, whose Complete touches no part body.
//!
//! **The state lock is held only for metadata steps** (begin, the inode put,
//! publish, namespace lookups; a body of at most the inline threshold is
//! buffered into the inode under it). No data I/O happens under it: a request
//! body — its tail included — streams into its data object through the
//! client alone; a delete, an overwrite, an Abort and a Complete's leftovers record
//! what to reclaim and hand it to the background reclaimer
//! (`FsState::reclaim_later`); a cancelled request's own objects are deleted
//! through the client. So a large PUT, UploadPart, delete or Abort does not
//! stall the other requests this worker is serving.
//!
//! **Nothing unfinished is left behind by a request that goes away.** An
//! unpublished file or an unfinished part is held by a guard that undoes it
//! when dropped: on any error path, and when the client disconnects and the
//! request future is dropped mid-await. Left alone, its pending record would
//! sit under this worker's session, which stays alive, so no recovery would
//! ever look at it.

use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use axum::body::{Body, BodyDataStream, Bytes};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use futures::StreamExt;
use md5::Digest as _;

use autumn_client::ClusterClient;
use autumn_fs::multipart::{self, MultipartError, PartWriter};
use autumn_fs::publish::{self, Condition, NewFile, PublishError};
use autumn_fs::schema::INLINE_THRESHOLD;
use autumn_fs::state::FsState;
use autumn_fs::dir;

use crate::objects::{self, Fs, OpenError};
use crate::s3::{self, AwsChunked, CondOutcome, DeleteOutcome, S3Error};

/// Read chunk for CopyObject's source, the same as a GET's.
const COPY_CHUNK: u64 = 8 << 20;
/// The largest XML request body accepted (a 10000-part Complete is ~1 MiB).
const MAX_XML_BODY: usize = 8 << 20;

fn resource(bucket: &str, key: &str) -> String {
    format!("{bucket}/{key}")
}

fn header_str<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
    h.get(name).and_then(|v| v.to_str().ok())
}

fn internal(e: impl std::fmt::Display, res: &str) -> S3Error {
    S3Error::internal(e.to_string(), res)
}

fn quoted_etag(etag: &str) -> HeaderValue {
    HeaderValue::from_str(&format!("\"{etag}\"")).expect("an etag is hex")
}

fn xml(body: String) -> Response {
    (StatusCode::OK, [(header::CONTENT_TYPE, "application/xml")], body).into_response()
}

fn no_content() -> Response {
    StatusCode::NO_CONTENT.into_response()
}

fn respond(r: Result<Response, S3Error>) -> Response {
    r.unwrap_or_else(IntoResponse::into_response)
}

// ── conditions and errors ──────────────────────────────────────────────────

/// An `If-Match` value as a publish condition. A tag this gateway did not
/// issue names no version it has; `ino 0` never exists, so it fails like any
/// other mismatch (412, or 404 when there is no object at all).
fn if_match_condition(v: &str) -> Condition {
    match publish::parse_etag(v) {
        Some((ino, generation)) => Condition::Version { ino, generation },
        None => Condition::Version { ino: 0, generation: 0 },
    }
}

/// The condition a write carries: `If-None-Match: *` (create only) or
/// `If-Match` (replace exactly that version). S3 accepts only `*` for a
/// write's `If-None-Match`.
fn write_condition(h: &HeaderMap, res: &str) -> Result<Condition, S3Error> {
    match (header_str(h, "if-match"), header_str(h, "if-none-match")) {
        (Some(_), Some(_)) => Err(S3Error::invalid_argument("If-Match and If-None-Match cannot both be given", res)),
        (None, Some(v)) if v.trim() == "*" => Ok(Condition::Absent),
        (None, Some(_)) => Err(S3Error::not_implemented("If-None-Match other than *")),
        (Some(v), None) if v.trim() == "*" => Err(S3Error::not_implemented("If-Match: *")),
        (Some(v), None) => Ok(if_match_condition(v)),
        (None, None) => Ok(Condition::None),
    }
}

fn publish_error(e: PublishError, res: &str) -> S3Error {
    match e {
        PublishError::PreconditionFailed => S3Error::precondition_failed(res),
        PublishError::NoSuchKey => S3Error::no_such_key(res),
        PublishError::Busy(m) => S3Error::conditional_conflict(m, res),
        PublishError::NotAFile(m) => S3Error::invalid_request(
            format!("'{m}' is a directory where an object is needed, or an object where a directory is"),
            res,
        ),
        PublishError::Other(e) => S3Error::internal(format!("{e:#}"), res),
    }
}

fn multipart_error(e: MultipartError, res: &str) -> S3Error {
    match e {
        MultipartError::NoSuchUpload => S3Error::no_such_upload(res),
        MultipartError::InvalidPart(p) => S3Error::invalid_part(p, res),
        MultipartError::InvalidPartOrder => S3Error::invalid_part_order(res),
        MultipartError::EntityTooSmall(p) => S3Error::entity_too_small(p, res),
        MultipartError::Malformed(m) => S3Error::malformed_xml(m, res),
        MultipartError::Busy(m) => S3Error::conditional_conflict(m, res),
        MultipartError::Publish(e) => publish_error(e, res),
        MultipartError::Other(e) => S3Error::internal(format!("{e:#}"), res),
    }
}

fn open_error(e: OpenError, res: &str) -> S3Error {
    match e {
        OpenError::Busy(m) => S3Error::slow_down(format!("the object is being changed: {m}"), res),
        OpenError::Other(e) => S3Error::internal(format!("{e:#}"), res),
    }
}

// ── request bodies ─────────────────────────────────────────────────────────

/// A request body, chunk by chunk: `aws-chunked` framing removed, length and
/// `Content-MD5` checked. MD5 is computed only when the client sent one, so
/// an ordinary PUT does not pay a single-core hash over every byte.
pub struct BodyReader {
    stream: BodyDataStream,
    chunked: Option<AwsChunked>,
    ready: VecDeque<Bytes>,
    expected: Option<u64>,
    got: u64,
    md5: Option<(md5::Md5, Vec<u8>)>,
    ended: bool,
    res: String,
}

impl BodyReader {
    pub fn new(h: &HeaderMap, body: Body, res: &str) -> Result<Self, S3Error> {
        let chunked = header_str(h, "content-encoding").is_some_and(|v| v.contains("aws-chunked"))
            || header_str(h, "x-amz-content-sha256").is_some_and(|v| v.starts_with("STREAMING-"));
        let len_header = if chunked { "x-amz-decoded-content-length" } else { "content-length" };
        let expected = match header_str(h, len_header) {
            Some(v) => Some(v.trim().parse::<u64>().map_err(|_| S3Error::invalid_argument(format!("bad {len_header}"), res))?),
            None => None,
        };
        let md5 = match header_str(h, "content-md5") {
            Some(v) => {
                let want = base64::engine::general_purpose::STANDARD
                    .decode(v.trim())
                    .ok()
                    .filter(|d| d.len() == 16)
                    .ok_or_else(|| S3Error::invalid_digest(res))?;
                Some((md5::Md5::new(), want))
            }
            None => None,
        };
        Ok(BodyReader {
            stream: body.into_data_stream(),
            chunked: chunked.then(AwsChunked::default),
            ready: VecDeque::new(),
            expected,
            got: 0,
            md5,
            ended: false,
            res: res.to_string(),
        })
    }

    /// The length the client declared, if it declared one.
    pub fn expected_len(&self) -> Option<u64> {
        self.expected
    }

    pub async fn next(&mut self) -> Result<Option<Bytes>, S3Error> {
        loop {
            if let Some(b) = self.ready.pop_front() {
                if b.is_empty() {
                    continue;
                }
                self.got += b.len() as u64;
                if self.expected.is_some_and(|n| self.got > n) {
                    return Err(S3Error::incomplete_body("the body is longer than its declared length", &self.res));
                }
                if let Some((h, _)) = &mut self.md5 {
                    h.update(&b);
                }
                return Ok(Some(b));
            }
            if self.ended {
                return Ok(None);
            }
            match self.stream.next().await {
                Some(Ok(b)) => match &mut self.chunked {
                    Some(d) => {
                        let mut out = Vec::new();
                        d.push(b, &mut out).map_err(|m| S3Error::incomplete_body(m, &self.res))?;
                        self.ready.extend(out);
                    }
                    None => self.ready.push_back(b),
                },
                Some(Err(e)) => return Err(S3Error::incomplete_body(format!("reading the body: {e}"), &self.res)),
                None => self.ended = true,
            }
        }
    }

    /// After the last chunk: the declared length, the chunk framing and the
    /// digest all have to agree with what arrived.
    pub fn verify(self) -> Result<(), S3Error> {
        if self.chunked.as_ref().is_some_and(|d| !d.is_done()) {
            return Err(S3Error::incomplete_body("the aws-chunked body ended early", &self.res));
        }
        if let Some(n) = self.expected {
            if self.got != n {
                return Err(S3Error::incomplete_body(format!("got {} of {n} bytes", self.got), &self.res));
            }
        }
        if let Some((h, want)) = self.md5 {
            if h.finalize().as_slice() != want.as_slice() {
                return Err(S3Error::bad_digest(&self.res));
            }
        }
        Ok(())
    }

    /// The whole body, for the small XML requests.
    pub async fn collect(mut self, limit: usize) -> Result<Bytes, S3Error> {
        let mut out = bytes::BytesMut::new();
        while let Some(b) = self.next().await? {
            if out.len() + b.len() > limit {
                return Err(S3Error::invalid_request("the request body is too large", &self.res));
            }
            out.extend_from_slice(&b);
        }
        self.verify()?;
        Ok(out.freeze())
    }
}

async fn xml_body(h: &HeaderMap, body: Body, res: &str) -> Result<String, S3Error> {
    let b = BodyReader::new(h, body, res)?.collect(MAX_XML_BODY).await?;
    String::from_utf8(b.to_vec()).map_err(|_| S3Error::malformed_xml("the body is not UTF-8", res))
}

// ── guards for unfinished writes ───────────────────────────────────────────

/// A new file that is not published yet; dropped unpublished, it is undone
/// through the client, without the state lock.
struct Unpublished {
    client: Rc<ClusterClient>,
    file: Option<NewFile>,
}

impl Unpublished {
    fn file(&mut self) -> &mut NewFile {
        self.file.as_mut().expect("taken only to publish")
    }
}

impl Drop for Unpublished {
    fn drop(&mut self) {
        if let Some(f) = self.file.take() {
            let client = self.client.clone();
            compio::runtime::spawn(async move {
                if let Err(e) = f.abort(&client).await {
                    tracing::warn!(error = %e, "undoing an unpublished PUT failed; the session's recovery finishes it");
                }
            })
            .detach();
        }
    }
}

/// A part being uploaded; dropped unfinished, it is discarded through the
/// client, without the state lock.
struct UnfinishedPart {
    client: Rc<ClusterClient>,
    part: Option<PartWriter>,
}

impl Drop for UnfinishedPart {
    fn drop(&mut self) {
        if let Some(p) = self.part.take() {
            let client = self.client.clone();
            compio::runtime::spawn(async move {
                if let Err(e) = p.abort(&client).await {
                    tracing::warn!(error = %e, "discarding an unfinished part failed; the upload's cleanup finishes it");
                }
            })
            .detach();
        }
    }
}

// ── PutObject / CopyObject ─────────────────────────────────────────────────

/// The directory `dirs` under a bucket, created as needed.
async fn parent_for_write(st: &mut FsState, bucket: &str, dirs: &[&str], res: &str) -> Result<u64, S3Error> {
    let Some(b) = objects::bucket_dir(st, bucket).await.map_err(|e| internal(e, res))? else {
        return Err(S3Error::no_such_bucket(bucket));
    };
    if dirs.is_empty() {
        return Ok(b);
    }
    let comps: Vec<&[u8]> = dirs.iter().map(|d| d.as_bytes()).collect();
    publish::ensure_dirs(st, b, &comps).await.map_err(|e| publish_error(e, res))
}

/// Begin a new file under `parent`; `large` gives it its data object now, so
/// the body can stream in through the client alone. `declared` is the body's
/// length when the request says it.
async fn begin_file(
    st: &mut FsState,
    parent: u64,
    name: &str,
    large: bool,
    declared: Option<u64>,
    res: &str,
) -> Result<Unpublished, S3Error> {
    let f = NewFile::begin(st, parent, name.as_bytes()).await.map_err(|e| internal(e, res))?;
    let mut w = Unpublished { client: st.client.clone(), file: Some(f) };
    if large {
        w.file().start_object(st, declared).await.map_err(|e| internal(e, res))?;
    }
    Ok(w)
}

/// Where a finish-and-publish spent its time, for the PUT breakdown.
#[derive(Default)]
struct PublishTimes {
    flush: std::time::Duration,
    lock: std::time::Duration,
    finish: std::time::Duration,
    publish: std::time::Duration,
}

/// Finish and publish; returns the new object's ETag and mtime. The data
/// object's buffered tail goes out before the lock.
async fn finish_and_publish(fs: &Fs, mut w: Unpublished, cond: Condition, res: &str) -> Result<(String, i64, PublishTimes), S3Error> {
    let mut t = PublishTimes::default();
    let mut at = std::time::Instant::now();
    let mut lap = |d: &mut std::time::Duration| {
        *d = at.elapsed();
        at = std::time::Instant::now();
    };
    w.file().flush_streamed().await.map_err(|e| internal(e, res))?;
    lap(&mut t.flush);
    let mut st = fs.lock().await;
    lap(&mut t.lock);
    let m = w.file().finish(&mut st).await.map_err(|e| internal(e, res))?;
    lap(&mut t.finish);
    // From here `publish` owns the outcome: it undoes a publish that
    // certainly did not happen and leaves one whose outcome is unknown to
    // the session's recovery.
    let f = w.file.take().expect("not yet taken");
    let ino = f.ino;
    f.publish(&mut st, cond).await.map_err(|e| publish_error(e, res))?;
    lap(&mut t.publish);
    Ok((objects::etag(ino, m.generation), m.mtime_secs, t))
}

/// `PUT /{bucket}/{key}`: PutObject, or CopyObject when the request names a
/// copy source.
pub async fn put_object(fs: &Fs, bucket: String, key: String, h: HeaderMap, body: Body) -> Response {
    let res = resource(&bucket, &key);
    if let Some(src) = header_str(&h, "x-amz-copy-source") {
        let src = src.to_string();
        return respond(copy_object(fs, &bucket, &key, &src, &h, &res).await);
    }
    respond(put(fs, &bucket, &key, &h, body, &res).await)
}

async fn put(fs: &Fs, bucket: &str, key: &str, h: &HeaderMap, body: Body, res: &str) -> Result<Response, S3Error> {
    let cond = write_condition(h, res)?;
    let (dirs, name) = s3::key_path(key).map_err(|m| S3Error::invalid_argument(m, res))?;
    let mut body = BodyReader::new(h, body, res)?;
    let Some(name) = name else {
        // A key ending in `/` is a directory here, and a directory holds no
        // bytes.
        if body.next().await?.is_some() {
            return Err(S3Error::invalid_argument("a key ending in '/' names a directory and cannot carry data", res));
        }
        body.verify()?;
        let mut st = fs.lock().await;
        let d = parent_for_write(&mut st, bucket, &dirs, res).await?;
        let mut resp = StatusCode::OK.into_response();
        resp.headers_mut().insert(header::ETAG, quoted_etag(&objects::etag(d, 0)));
        return Ok(resp);
    };
    // Unknown length (a chunked body) is treated as large.
    let small = body.expected_len().is_some_and(|n| n <= INLINE_THRESHOLD as u64);
    let t0 = std::time::Instant::now();
    let mut w = {
        let mut st = fs.lock().await;
        let parent = parent_for_write(&mut st, bucket, &dirs, res).await?;
        begin_file(&mut st, parent, name, !small, body.expected_len(), res).await?
    };
    let t_begin = t0.elapsed();
    while let Some(chunk) = body.next().await? {
        if small {
            let mut st = fs.lock().await;
            w.file().write(&mut st, &chunk).await.map_err(|e| internal(e, res))?;
        } else {
            w.file().write_streamed(&chunk).await.map_err(|e| internal(e, res))?;
        }
    }
    body.verify()?;
    let t_body = t0.elapsed();
    let (etag, _, t) = finish_and_publish(fs, w, cond, res).await?;
    tracing::debug!(
        key = res,
        begin_ms = t_begin.as_secs_f64() * 1e3,
        body_ms = (t_body - t_begin).as_secs_f64() * 1e3,
        flush_ms = t.flush.as_secs_f64() * 1e3,
        lock_ms = t.lock.as_secs_f64() * 1e3,
        finish_ms = t.finish.as_secs_f64() * 1e3,
        publish_ms = t.publish.as_secs_f64() * 1e3,
        "PUT breakdown"
    );
    let mut resp = StatusCode::OK.into_response();
    resp.headers_mut().insert(header::ETAG, quoted_etag(&etag));
    Ok(resp)
}

fn copy_source_conditions(h: &HeaderMap) -> s3::ReadConditions {
    s3::ReadConditions {
        if_match: header_str(h, "x-amz-copy-source-if-match").map(Into::into),
        if_none_match: header_str(h, "x-amz-copy-source-if-none-match").map(Into::into),
        if_modified_since: header_str(h, "x-amz-copy-source-if-modified-since").and_then(s3::parse_http_date),
        if_unmodified_since: header_str(h, "x-amz-copy-source-if-unmodified-since").and_then(s3::parse_http_date),
    }
}

/// CopyObject: the source is read under a STABLE pin, so it cannot change
/// mid-copy, and written into a new file published like any PUT. The bytes
/// are copied; a segmented source's objects are not shared, because each is
/// owned — and reclaimed — by exactly one file.
async fn copy_object(fs: &Fs, bucket: &str, key: &str, src: &str, h: &HeaderMap, res: &str) -> Result<Response, S3Error> {
    let (sb, sk) = s3::parse_copy_source(src)
        .ok_or_else(|| S3Error::invalid_argument("x-amz-copy-source must be /bucket/key", res))?;
    let src_res = resource(&sb, &sk);
    let cond = write_condition(h, res)?;
    let (dirs, name) = s3::key_path(key).map_err(|m| S3Error::invalid_argument(m, res))?;
    let name = name.ok_or_else(|| S3Error::invalid_argument("cannot copy onto a directory key", res))?;
    let opened = match objects::open_pinned(fs, &sb, &sk).await {
        Ok(Some(o)) => o,
        Ok(None) => return Err(S3Error::no_such_key(src_res)),
        Err(e) => return Err(open_error(e, &src_res)),
    };
    let (ino, size) = (opened.stat.ino, opened.stat.size);
    // For a copy both a failed match and "not modified" are a 412.
    if s3::evaluate_read(&copy_source_conditions(h), &opened.stat.etag, opened.stat.mtime_secs) != CondOutcome::Proceed {
        return Err(S3Error::precondition_failed(src_res));
    }
    let large = size > INLINE_THRESHOLD as u64;
    let mut w = {
        let mut st = fs.lock().await;
        let parent = parent_for_write(&mut st, bucket, &dirs, res).await?;
        begin_file(&mut st, parent, name, large, Some(size), res).await?
    };
    let mut off = 0u64;
    while off < size {
        if !opened.pin.alive() {
            return Err(S3Error::internal("lost the pin on the copy source", src_res));
        }
        let want = (size - off).min(COPY_CHUNK) as u32;
        let plan = objects::plan_read(fs, ino, off, want).await.map_err(|e| internal(e, &src_res))?;
        let buf = objects::run_read(plan).await.map_err(|e| internal(e, &src_res))?;
        if buf.is_empty() {
            return Err(S3Error::internal("the copy source ended early", src_res));
        }
        if large {
            w.file().write_streamed(&buf).await.map_err(|e| internal(e, res))?;
        } else {
            let mut st = fs.lock().await;
            w.file().write(&mut st, &buf).await.map_err(|e| internal(e, res))?;
        }
        off += buf.len() as u64;
    }
    let (etag, mtime, _) = finish_and_publish(fs, w, cond, res).await?;
    drop(opened);
    Ok(xml(s3::copy_object_xml(&etag, mtime)))
}

// ── DeleteObject / DeleteObjects ───────────────────────────────────────────

/// Remove one object. A missing key (or directory) is success: S3 deletes
/// are idempotent, and a prefix is not an object.
async fn delete_one(fs: &Fs, bucket: &str, key: &str, cond: Condition) -> Result<(), S3Error> {
    let res = resource(bucket, key);
    let missing = || match cond {
        Condition::Version { .. } => Err(S3Error::no_such_key(res.clone())),
        _ => Ok(()),
    };
    let Ok((dirs, Some(name))) = s3::key_path(key) else {
        return missing();
    };
    let mut st = fs.lock().await;
    let Some(b) = objects::bucket_dir(&mut st, bucket).await.map_err(|e| internal(e, &res))? else {
        return Err(S3Error::no_such_bucket(bucket));
    };
    let parent = if dirs.is_empty() {
        b
    } else {
        match dir::resolve(&mut st, &format!("/{bucket}/{}", dirs.join("/"))).await.map_err(|e| internal(e, &res))? {
            Some(p) => p,
            None => return missing(),
        }
    };
    publish::delete_name(&mut st, parent, name.as_bytes(), cond).await.map_err(|e| publish_error(e, &res))?;
    Ok(())
}

/// `DELETE /{bucket}/{key}` without an upload id. It never cancels a
/// multipart upload to the same key.
pub async fn delete_object(fs: &Fs, bucket: String, key: String, h: HeaderMap) -> Response {
    let cond = header_str(&h, "if-match").map_or(Condition::None, if_match_condition);
    match delete_one(fs, &bucket, &key, cond).await {
        Ok(()) => no_content(),
        Err(e) => e.into_response(),
    }
}

/// `POST /{bucket}?delete`: DeleteObjects, reporting every key's outcome.
pub async fn delete_objects(fs: &Fs, bucket: String, h: HeaderMap, body: Body) -> Response {
    respond(
        async {
            let x = xml_body(&h, body, &bucket).await?;
            let req = s3::parse_delete_xml(&x).map_err(|m| S3Error::malformed_xml(m, &bucket))?;
            if !objects::bucket_exists(fs, &bucket).await.map_err(|e| internal(e, &bucket))? {
                return Err(S3Error::no_such_bucket(&bucket));
            }
            let mut outcomes = Vec::with_capacity(req.keys.len());
            // One key at a time, taking the state lock per key, so a large
            // batch interleaves with the worker's other requests.
            for key in req.keys {
                outcomes.push(match delete_one(fs, &bucket, &key, Condition::None).await {
                    Ok(()) => DeleteOutcome::Deleted(key),
                    Err(e) => DeleteOutcome::Failed { key, code: e.code, message: e.message },
                });
            }
            Ok(xml(s3::delete_result_xml(&outcomes, req.quiet)))
        }
        .await,
    )
}

// ── multipart ──────────────────────────────────────────────────────────────

fn upload_id(q: &HashMap<String, String>, res: &str) -> Result<u64, S3Error> {
    q.get("uploadId").and_then(|v| s3::parse_upload_id(v)).ok_or_else(|| S3Error::no_such_upload(res))
}

/// The upload must exist and belong to this bucket and key.
async fn check_upload(st: &mut FsState, id: u64, res: &str) -> Result<(), S3Error> {
    match multipart::get(st, id).await.map_err(|e| internal(e, res))? {
        Some(rec) if rec.key == res.as_bytes() => Ok(()),
        _ => Err(S3Error::no_such_upload(res)),
    }
}

/// `POST /{bucket}/{key}?uploads`.
pub async fn create_multipart(fs: &Fs, bucket: String, key: String) -> Response {
    let res = resource(&bucket, &key);
    respond(
        async {
            let (dirs, name) = s3::key_path(&key).map_err(|m| S3Error::invalid_argument(m, &res))?;
            let name = name.ok_or_else(|| S3Error::invalid_argument("a key ending in '/' names a directory", &res))?;
            let mut st = fs.lock().await;
            let parent = parent_for_write(&mut st, &bucket, &dirs, &res).await?;
            let id = multipart::create(&mut st, parent, name.as_bytes(), res.as_bytes())
                .await
                .map_err(|e| internal(e, &res))?;
            Ok(xml(s3::initiate_multipart_xml(&bucket, &key, &s3::format_upload_id(id))))
        }
        .await,
    )
}

/// `PUT /{bucket}/{key}?partNumber=N&uploadId=X`.
pub async fn upload_part(fs: &Fs, bucket: String, key: String, q: HashMap<String, String>, h: HeaderMap, body: Body) -> Response {
    let res = resource(&bucket, &key);
    respond(
        async {
            if h.contains_key("x-amz-copy-source") {
                return Err(S3Error::not_implemented("UploadPartCopy"));
            }
            let part: u32 = q
                .get("partNumber")
                .and_then(|v| v.parse().ok())
                .ok_or_else(|| S3Error::invalid_argument("partNumber must be a number", &res))?;
            let id = upload_id(&q, &res)?;
            let mut body = BodyReader::new(&h, body, &res)?;
            let mut w = {
                let mut st = fs.lock().await;
                check_upload(&mut st, id, &res).await?;
                let p = PartWriter::begin(&mut st, id, part, body.expected_len())
                    .await
                    .map_err(|e| multipart_error(e, &res))?;
                UnfinishedPart { client: st.client.clone(), part: Some(p) }
            };
            while let Some(chunk) = body.next().await? {
                let p = w.part.as_mut().expect("taken only to finish");
                p.write(&chunk).await.map_err(|e| internal(e, &res))?;
            }
            body.verify()?;
            // Only the begin needed the state; the rest is the client's.
            let p = w.part.take().expect("not yet taken");
            let (etag, _) = p.finish(&w.client).await.map_err(|e| multipart_error(e, &res))?;
            let mut resp = StatusCode::OK.into_response();
            resp.headers_mut().insert(header::ETAG, quoted_etag(&etag));
            Ok(resp)
        }
        .await,
    )
}

/// `POST /{bucket}/{key}?uploadId=X`: CompleteMultipartUpload. Reads and
/// writes metadata only. A retry of one that succeeded (its reply lost) gets
/// the same answer, as from S3; `multipart::complete` checks the key.
pub async fn complete_multipart(fs: &Fs, bucket: String, key: String, q: HashMap<String, String>, h: HeaderMap, body: Body) -> Response {
    let res = resource(&bucket, &key);
    respond(
        async {
            let id = upload_id(&q, &res)?;
            let cond = write_condition(&h, &res)?;
            let x = xml_body(&h, body, &res).await?;
            let list = s3::parse_complete_xml(&x).map_err(|m| S3Error::malformed_xml(m, &res))?;
            let mut st = fs.lock().await;
            let done = multipart::complete(&mut st, id, res.as_bytes(), &list, cond)
                .await
                .map_err(|e| multipart_error(e, &res))?;
            Ok(xml(s3::complete_multipart_xml(&bucket, &key, &objects::etag(done.ino, done.generation))))
        }
        .await,
    )
}

/// `DELETE /{bucket}/{key}?uploadId=X`: AbortMultipartUpload. Moves the
/// upload to Aborted and hands its data to the background reclaimer.
pub async fn abort_multipart(fs: &Fs, bucket: String, key: String, q: HashMap<String, String>) -> Response {
    let res = resource(&bucket, &key);
    respond(
        async {
            let id = upload_id(&q, &res)?;
            let mut st = fs.lock().await;
            check_upload(&mut st, id, &res).await?;
            multipart::abort(&mut st, id).await.map_err(|e| multipart_error(e, &res))?;
            Ok(no_content())
        }
        .await,
    )
}
