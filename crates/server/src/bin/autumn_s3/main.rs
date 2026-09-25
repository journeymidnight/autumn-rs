//! `autumn-s3` — an unauthenticated S3-compatible gateway over autumn's `fs/`
//! tree.
//!
//! It started read-only, for inference engines that have no loader plugin
//! seam but ship a `runai_streamer` load format that speaks S3. It now also
//! writes, so a stock S3 client — LanceDB's `object_store` among them — can
//! create and update data on autumn: PutObject (with `If-None-Match: *` and
//! `If-Match`), CopyObject, DeleteObject, DeleteObjects and multipart uploads,
//! plus conditional GET/HEAD.
//!
//! Deliberately NOT implemented: versioning, ACLs, virtual-host addressing,
//! SigV4 verification, object metadata (`Content-Type`, `x-amz-meta-*`),
//! UploadPartCopy, ListParts and ListMultipartUploads. Requests are served
//! whatever their `Authorization` header says — including none. Clients still
//! need dummy credentials set, because the AWS SDK's credential chain runs
//! before the request is ever sent.
//!
//! Everything goes through `autumn-fs`, the same layer the fuse mount and the
//! `autumn.Fs` binding use, so lane striping, EN-direct reads, authz, leases
//! and the file format all apply unchanged, and an object written here is a
//! file there. This is an adapter over the partition layer, not a second data
//! plane.

mod listing;
mod objects;
mod s3;
mod write;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::rc::Rc;

use anyhow::{bail, Context, Result};
use axum::body::{Body, Bytes};
use axum::extract::{Path, Query};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use autumn_fs::state::{FsState, Reclaim};
use futures::channel::mpsc;
use futures::StreamExt;
use send_wrapper::SendWrapper;

use objects::{Fs, OpenError};
use s3::{CondOutcome, RangeSpec, ReadConditions, S3Error};

/// Body chunk for a streamed GET. Matches the fs layer's max extent, so each
/// chunk is one extent read rather than a straddle.
const CHUNK: u64 = 8 << 20;

/// S3's default and maximum page size.
const DEFAULT_MAX_KEYS: usize = 1000;

struct Args {
    manager: String,
    listen: String,
    port: u16,
    host: String,
    credential_file: Option<PathBuf>,
    direct_read: bool,
    workers: usize,
    sweep_interval_secs: u64,
}

/// Accept threads to run by default. The gateway is CPU-bound on socket work,
/// not on autumn reads: an AWS-CRT client drains its sockets slowly enough that
/// each body write fragments into many partial writes, and a single accept
/// thread saturates well below what the read path can deliver. It is also a
/// sidecar, so it should not claim every core on a GPU node.
const DEFAULT_WORKERS: usize = 8;

/// How often the sweeper recovers dead publishing sessions and reclaims
/// finished uploads, unlinked files and segment garbage.
const DEFAULT_SWEEP_INTERVAL_SECS: u64 = 30;

fn parse_args() -> Result<Args> {
    let mut a = Args {
        manager: String::new(),
        listen: "0.0.0.0".into(),
        port: 9000,
        host: "autumn-s3".into(),
        credential_file: None,
        direct_read: true,
        workers: std::thread::available_parallelism()
            .map(|n| n.get().min(DEFAULT_WORKERS))
            .unwrap_or(1),
        sweep_interval_secs: DEFAULT_SWEEP_INTERVAL_SECS,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || {
            it.next()
                .ok_or_else(|| anyhow::anyhow!("{flag} needs a value"))
        };
        match flag.as_str() {
            "--manager" => a.manager = val()?,
            "--listen" => a.listen = val()?,
            "--port" => a.port = val()?.parse().context("--port")?,
            "--host" => a.host = val()?,
            "--credential-file" => a.credential_file = Some(PathBuf::from(val()?)),
            "--direct-read" => a.direct_read = val()?.parse().context("--direct-read")?,
            "--workers" => a.workers = val()?.parse().context("--workers")?,
            "--sweep-interval-secs" => a.sweep_interval_secs = val()?.parse().context("--sweep-interval-secs")?,
            "-h" | "--help" => {
                println!(
                    "autumn-s3 --manager <host:port> [--listen 0.0.0.0] [--port 9000]\n\
                     \x20            [--host <daemon-identity>] [--credential-file <path>]\n\
                     \x20            [--direct-read true|false] [--workers N]\n\
                     \x20            [--sweep-interval-secs 30]  (0 = no periodic sweeps)"
                );
                std::process::exit(0);
            }
            other => bail!("unknown flag {other} (try --help)"),
        }
    }
    if a.manager.is_empty() {
        bail!("--manager is required");
    }
    if a.workers == 0 {
        bail!("--workers must be at least 1");
    }
    Ok(a)
}

/// `GET /` — the buckets are the `fs/` root's subdirectories.
async fn list_buckets(fs: &Fs) -> Response {
    match objects::list_buckets(fs).await {
        Ok(b) => xml(s3::list_buckets_xml(&b)),
        Err(e) => S3Error::internal(e.to_string(), "/").into_response(),
    }
}

/// `HEAD /{bucket}` — the existence probe used by S3 clients.
async fn head_bucket(fs: &Fs, bucket: String) -> Response {
    match objects::bucket_exists(fs, &bucket).await {
        Ok(true) => StatusCode::OK.into_response(),
        Ok(false) => S3Error::no_such_bucket(bucket).into_response(),
        Err(e) => S3Error::internal(e.to_string(), bucket).into_response(),
    }
}

/// `GET /{bucket}` — `ListObjectsV2`. `list-type=1` (the legacy listing) is
/// answered with the same body; the fields v2 adds are additive and no client
/// we serve asks for v1.
async fn list_objects(fs: &Fs, bucket: String, q: HashMap<String, String>) -> Response {
    let prefix = q.get("prefix").map(String::as_str).unwrap_or("");
    let delimiter = q.get("delimiter").map(String::as_str).filter(|d| !d.is_empty());
    let url_encoded = q.get("encoding-type").map(String::as_str) == Some("url");
    let max_keys = q
        .get("max-keys")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_KEYS)
        .min(DEFAULT_MAX_KEYS);
    // The continuation token IS the last key of the previous page, so paging
    // needs no server-side cursor state.
    let after = q
        .get("continuation-token")
        .or_else(|| q.get("start-after"))
        .map(String::as_str);

    match objects::list_objects(fs, &bucket, prefix, delimiter, after, max_keys).await {
        Ok(None) => S3Error::no_such_bucket(bucket).into_response(),
        Ok(Some(l)) => xml(s3::list_objects_xml(
            &bucket,
            prefix,
            delimiter,
            max_keys,
            url_encoded,
            &l.rows,
            &l.common_prefixes,
            l.next_token.as_deref(),
        )),
        Err(e) => S3Error::internal(e.to_string(), bucket).into_response(),
    }
}

fn read_conditions(h: &HeaderMap) -> ReadConditions {
    let get = |n: &str| h.get(n).and_then(|v| v.to_str().ok());
    ReadConditions {
        if_match: get("if-match").map(Into::into),
        if_none_match: get("if-none-match").map(Into::into),
        if_modified_since: get("if-modified-since").and_then(s3::parse_http_date),
        if_unmodified_since: get("if-unmodified-since").and_then(s3::parse_http_date),
    }
}

/// The validators every object response carries, including a 304.
fn validators(etag: &str, mtime_secs: i64) -> [(header::HeaderName, String); 2] {
    [(header::ETAG, format!("\"{etag}\"")), (header::LAST_MODIFIED, s3::http_date(mtime_secs))]
}

/// 304 and 412 for a conditional read, `None` to serve it.
fn conditional(h: &HeaderMap, stat: &objects::Stat, resource: &str) -> Option<Response> {
    match s3::evaluate_read(&read_conditions(h), &stat.etag, stat.mtime_secs) {
        CondOutcome::Proceed => None,
        CondOutcome::NotModified => {
            Some((StatusCode::NOT_MODIFIED, validators(&stat.etag, stat.mtime_secs)).into_response())
        }
        CondOutcome::PreconditionFailed => Some(S3Error::precondition_failed(resource).into_response()),
    }
}

/// `HEAD /{bucket}/{key}`.
async fn head_object(fs: &Fs, bucket: String, key: String, headers: HeaderMap) -> Response {
    let resource = format!("{bucket}/{key}");
    match objects::stat(fs, &bucket, &key).await {
        Ok(None) => S3Error::no_such_key(resource).into_response(),
        Err(e) => S3Error::internal(e.to_string(), resource).into_response(),
        Ok(Some(s)) => {
            if let Some(r) = conditional(&headers, &s, &resource) {
                return r;
            }
            let [etag, lm] = validators(&s.etag, s.mtime_secs);
            (
                StatusCode::OK,
                [
                    (header::CONTENT_LENGTH, s.size.to_string()),
                    (header::CONTENT_TYPE, "application/octet-stream".into()),
                    (header::ACCEPT_RANGES, "bytes".into()),
                    etag,
                    lm,
                ],
            )
                .into_response()
        }
    }
}

/// `GET /{bucket}/{key}`, with or without a `Range`.
async fn get_object(fs: &Fs, bucket: String, key: String, headers: HeaderMap) -> Response {
    let resource = format!("{bucket}/{key}");
    // Pinned before its metadata is read, so the headers and every byte of
    // the body are one version, and no other client can change or reclaim it
    // until the body is done (or the client goes away).
    let opened = match objects::open_pinned(fs, &bucket, &key).await {
        Ok(Some(o)) => o,
        Ok(None) => return S3Error::no_such_key(resource).into_response(),
        Err(OpenError::Busy(m)) => {
            return S3Error::slow_down(format!("the object is being changed: {m}"), resource).into_response()
        }
        Err(OpenError::Other(e)) => return S3Error::internal(e.to_string(), resource).into_response(),
    };
    let stat = &opened.stat;
    if let Some(r) = conditional(&headers, stat, &resource) {
        return r;
    }

    let raw_range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    let (status, start, len) = match s3::parse_range(raw_range, stat.size) {
        RangeSpec::Whole => (StatusCode::OK, 0, stat.size),
        RangeSpec::Partial { start, end } => {
            (StatusCode::PARTIAL_CONTENT, start, end - start + 1)
        }
        RangeSpec::Unsatisfiable => {
            let (err, size) = S3Error::range_not_satisfiable(resource, stat.size);
            let mut resp = err.into_response();
            resp.headers_mut().insert(
                header::CONTENT_RANGE,
                format!("bytes */{size}").parse().expect("ascii"),
            );
            return resp;
        }
    };

    let [etag, lm] = validators(&stat.etag, stat.mtime_secs);
    let mut fields = vec![
        (header::CONTENT_LENGTH, len.to_string()),
        (header::CONTENT_TYPE, "application/octet-stream".to_string()),
        (header::ACCEPT_RANGES, "bytes".to_string()),
        etag,
        lm,
    ];
    if status == StatusCode::PARTIAL_CONTENT {
        fields.push((
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", start, start + len - 1, stat.size),
        ));
    }
    let mut resp_headers = HeaderMap::new();
    for (name, v) in fields {
        // Every value here is generated ASCII; a parse failure would mean a
        // filename leaked into a header, which none of these carry.
        if let Ok(hv) = v.parse() {
            resp_headers.insert(name, hv);
        }
    }

    // Stream rather than buffer: a `Range` from the streamer is chunk-sized,
    // but a plain `aws s3 cp` of a shard is gigabytes.
    let ino = stat.ino;
    let fs = fs.clone();
    // The pin travels with the stream and is dropped with it — at the end of
    // the body, or when the client disconnects.
    let pin = opened.pin;
    let stream = futures::stream::unfold(
        (fs, ino, start, len, pin),
        |(fs, ino, off, remaining, pin)| async move {
            if remaining == 0 {
                return None;
            }
            if !pin.alive() {
                return Some((
                    Err(std::io::Error::other("lost the pin on the object mid-read")),
                    (fs, ino, off, 0, pin),
                ));
            }
            let want = remaining.min(CHUNK) as u32;
            let read = async {
                let plan = objects::plan_read(&fs, ino, off, want).await?;
                objects::run_read(plan).await
            };
            match read.await {
                // A short read before the expected end means the file changed
                // under us; stop rather than pad the body with zeros.
                Ok(buf) if buf.is_empty() => None,
                Ok(buf) => {
                    let n = buf.len() as u64;
                    Some((
                        Ok::<Bytes, std::io::Error>(Bytes::from(buf)),
                        (fs, ino, off + n, remaining - n.min(remaining), pin),
                    ))
                }
                Err(e) => Some((
                    Err(std::io::Error::other(e.to_string())),
                    (fs, ino, off, 0, pin),
                )),
            }
        },
    );

    // compio runs this whole server on one thread, so the `!Send` `Rc` inside
    // the stream never crosses threads (the dashboard/gallery idiom).
    (status, resp_headers, Body::from_stream(SendWrapper::new(stream))).into_response()
}

fn xml(body: String) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/xml")],
        body,
    )
        .into_response()
}

/// One SO_REUSEPORT listener. Every worker binds the same address; the kernel
/// spreads incoming connections across them, so N accept threads share the load
/// without a hand-off (passing an accepted socket between io_uring runtimes is
/// not free, and would put the contention back on one thread).
fn reuseport_listener(addr: SocketAddr) -> Result<std::net::TcpListener> {
    let domain = if addr.is_ipv6() {
        socket2::Domain::IPV6
    } else {
        socket2::Domain::IPV4
    };
    let sock = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
    sock.set_reuse_address(true)?;
    // The point of the exercise: without this the second worker's bind fails.
    sock.set_reuse_port(true)?;
    sock.bind(&addr.into())?;
    sock.listen(1024)?;
    Ok(sock.into())
}

/// This process's `FsState`, under the daemon identity `host`.
async fn connect_state(args: &Args, host: String, credential: Option<(String, Vec<u8>)>) -> Result<FsState> {
    let mut state = match credential {
        Some((who, secret)) => FsState::new_with_host_credential(&args.manager, host, &who, secret).await?,
        None => FsState::new_with_host(&args.manager, host).await?,
    };
    state.direct_read = args.direct_read;
    // Renews every lease this state holds: the publishing session (which
    // lapses after 30 s without it, and a sweeper would then recover the
    // session and fence out its writes) and the GET pins.
    autumn_fs::lease_tasks::spawn_lease_background_tasks(&state, None);
    Ok(state)
}

/// Serve on one thread: its own compio runtime, its own `FsState` (so workers
/// share no lock), its own listener. Data reclamation goes to `reclaims`.
fn serve_worker(
    idx: usize,
    args: &Args,
    credential: Option<(String, Vec<u8>)>,
    addr: SocketAddr,
    reclaims: mpsc::UnboundedSender<Reclaim>,
) -> Result<()> {
    let rt = compio::runtime::Runtime::new().context("compio runtime")?;
    rt.block_on(async move {
        // A distinct daemon identity per worker: the manager keys its lease
        // registry on it, and two workers sharing one would look like a single
        // client reconnecting.
        let mut state = connect_state(args, format!("{}-{idx}", args.host), credential).await?;
        state.reclaim_later = Some(Box::new(move |r| {
            if reclaims.unbounded_send(r).is_err() {
                tracing::warn!(?r, "the reclaimer is gone; the next sweep reclaims this");
            }
        }));
        let fs: Fs = Rc::new(futures::lock::Mutex::new(state));

        let listener = compio::net::TcpListener::from_std(reuseport_listener(addr)?)?;
        cyper_axum::serve(listener, router(fs)).await?;
        Ok::<_, anyhow::Error>(())
    })
}

/// The reclaimer, on a thread of its own so deleting data never holds a
/// serving worker's state lock:
/// - what the workers hand over (`FsState::reclaim_later`): files a delete or
///   an overwrite left unreachable, and aborted or completed multipart
///   uploads, reclaimed as soon as they arrive. The worker has already made
///   each durable (a tombstone, a terminal upload record), so a hand-off
///   that is lost — a crash — is picked up by a sweep;
/// - every `--sweep-interval-secs` (0: never), the sweeps: publishing
///   sessions whose owner died (another gateway, or this one before a
///   restart) are taken over, fenced, and their operations finished or
///   undone; terminal multipart uploads, unlinked files whose data another
///   client was still holding, and segmented files' dropped objects are
///   reclaimed once nobody holds them.
///
/// It is its own client, so a GET a worker is streaming (its pin) stops the
/// reclaim of that file the way any other client's would; that is also why a
/// worker releases its own leases on a file before handing it over. Every
/// step is idempotent and safe to run in several gateways at once.
fn reclaim_worker(
    args: &Args,
    credential: Option<(String, Vec<u8>)>,
    mut reclaims: mpsc::UnboundedReceiver<Reclaim>,
) -> Result<()> {
    let rt = compio::runtime::Runtime::new().context("compio runtime")?;
    rt.block_on(async move {
        let mut st = connect_state(args, format!("{}-sweep", args.host), credential).await?;
        let every = (args.sweep_interval_secs > 0).then(|| std::time::Duration::from_secs(args.sweep_interval_secs));
        let mut next_sweep = every.map(|d| std::time::Instant::now() + d);
        // Until every worker has exited.
        let mut open = true;
        loop {
            // A due sweep goes first, so a steady stream of hand-offs cannot
            // put it off.
            if next_sweep.is_some_and(|at| std::time::Instant::now() >= at) {
                sweep(&mut st).await;
                next_sweep = every.map(|d| std::time::Instant::now() + d);
                continue;
            }
            let handed = match (open, next_sweep) {
                (true, None) => reclaims.next().await,
                (true, Some(at)) => {
                    let wait = std::pin::pin!(compio::time::sleep(at.saturating_duration_since(std::time::Instant::now())));
                    match futures::future::select(reclaims.next(), wait).await {
                        futures::future::Either::Left((r, _)) => r,
                        futures::future::Either::Right(_) => continue,
                    }
                }
                (false, Some(at)) => {
                    compio::time::sleep(at.saturating_duration_since(std::time::Instant::now())).await;
                    continue;
                }
                (false, None) => return Ok(()),
            };
            let Some(first) = handed else {
                open = false;
                continue;
            };
            // Everything already queued, once each.
            let mut batch = vec![first];
            let mut seen = std::collections::HashSet::from([first]);
            while let Ok(r) = reclaims.try_recv() {
                if seen.insert(r) {
                    batch.push(r);
                }
            }
            for r in batch {
                reclaim(&mut st, r).await;
            }
        }
    })
}

async fn sweep(st: &mut FsState) {
    match autumn_fs::publish::recover_dead_sessions(st).await {
        Ok(0) => {}
        Ok(n) => tracing::info!(sessions = n, "recovered dead publishing sessions"),
        Err(e) => tracing::warn!(error = %e, "session recovery sweep failed"),
    }
    match autumn_fs::multipart::sweep_uploads(st).await {
        Ok(0) => {}
        Ok(n) => tracing::info!(uploads = n, "reclaimed finished multipart uploads"),
        Err(e) => tracing::warn!(error = %e, "multipart upload sweep failed"),
    }
    match autumn_fs::extent::sweep_unlink_tombstones(st).await {
        Ok(0) => {}
        Ok(n) => tracing::info!(inodes = n, "reclaimed unlinked files"),
        Err(e) => tracing::warn!(error = %e, "unlink tombstone sweep failed"),
    }
    match autumn_fs::segment::sweep_garbage(st).await {
        Ok(0) => {}
        Ok(n) => tracing::info!(files = n, "reclaimed segment garbage"),
        Err(e) => tracing::warn!(error = %e, "segment garbage sweep failed"),
    }
}

/// Reclaim one hand-off. A file still held by another client (a GET on
/// another worker or gateway, a mount) keeps its tombstone for the sweep.
async fn reclaim(st: &mut FsState, r: Reclaim) {
    match r {
        Reclaim::Inode(ino) => match autumn_fs::extent::reclaim_unreachable(st, ino).await {
            Ok(true) => {}
            Ok(false) => tracing::debug!(ino, "an unlinked file is still held elsewhere; the sweep reclaims it"),
            Err(e) => tracing::warn!(ino, error = %e, "reclaiming an unlinked file failed; the sweep retries"),
        },
        Reclaim::Upload(id) => {
            let r = match autumn_fs::publish::session_lease(st).await {
                Ok(lease) => autumn_fs::multipart::cleanup(st, id, lease).await.map(|_| ()),
                Err(e) => Err(e),
            };
            if let Err(e) = r {
                tracing::warn!(upload = id, error = %e, "multipart cleanup failed; the sweep retries");
            }
        }
    }
}

/// Wrap a handler future for axum. compio runs each worker on one thread, so
/// the `!Send` `Rc`s inside never cross threads (the dashboard idiom).
fn local<F: std::future::Future<Output = Response>>(f: F) -> SendWrapper<F> {
    SendWrapper::new(f)
}

type Q = Query<HashMap<String, String>>;

fn router(fs: Fs) -> Router {
    let f = SendWrapper::new(fs.clone());
    let buckets_route = get(move || {
        let f = f.clone();
        local(async move { list_buckets(&f).await })
    });

    let (f, g, p) = (SendWrapper::new(fs.clone()), SendWrapper::new(fs.clone()), SendWrapper::new(fs.clone()));
    let bucket_route = get(move |Path(b): Path<String>, Query(q): Q| {
        let f = f.clone();
        local(async move { list_objects(&f, b, q).await })
    })
    .head(move |Path(b): Path<String>| {
        let g = g.clone();
        local(async move { head_bucket(&g, b).await })
    })
    // `POST /{bucket}?delete` is DeleteObjects; nothing else posts to a bucket.
    .post(move |Path(b): Path<String>, Query(q): Q, headers: HeaderMap, body: Body| {
        let p = p.clone();
        local(async move {
            if q.contains_key("delete") {
                write::delete_objects(&p, b, headers, body).await
            } else {
                S3Error::not_implemented("this bucket operation").into_response()
            }
        })
    });

    let (f, g, u, p, d) = (
        SendWrapper::new(fs.clone()),
        SendWrapper::new(fs.clone()),
        SendWrapper::new(fs.clone()),
        SendWrapper::new(fs.clone()),
        SendWrapper::new(fs),
    );
    let object_route = get(move |Path((b, k)): Path<(String, String)>, headers: HeaderMap| {
        let f = f.clone();
        local(async move { get_object(&f, b, k, headers).await })
    })
    .head(move |Path((b, k)): Path<(String, String)>, headers: HeaderMap| {
        let g = g.clone();
        local(async move { head_object(&g, b, k, headers).await })
    })
    // PutObject / CopyObject, or UploadPart when the query names an upload.
    .put(move |Path((b, k)): Path<(String, String)>, Query(q): Q, headers: HeaderMap, body: Body| {
        let u = u.clone();
        local(async move {
            if q.contains_key("uploadId") {
                write::upload_part(&u, b, k, q, headers, body).await
            } else {
                write::put_object(&u, b, k, headers, body).await
            }
        })
    })
    // CreateMultipartUpload (`?uploads`) and CompleteMultipartUpload (`?uploadId`).
    .post(move |Path((b, k)): Path<(String, String)>, Query(q): Q, headers: HeaderMap, body: Body| {
        let p = p.clone();
        local(async move {
            if q.contains_key("uploads") {
                write::create_multipart(&p, b, k).await
            } else if q.contains_key("uploadId") {
                write::complete_multipart(&p, b, k, q, headers, body).await
            } else {
                S3Error::not_implemented("this object operation").into_response()
            }
        })
    })
    // DeleteObject, or AbortMultipartUpload with `?uploadId`.
    .delete(move |Path((b, k)): Path<(String, String)>, Query(q): Q, headers: HeaderMap| {
        let d = d.clone();
        local(async move {
            if q.contains_key("uploadId") {
                write::abort_multipart(&d, b, k, q).await
            } else {
                write::delete_object(&d, b, k, headers).await
            }
        })
    });

    // Everything else answers with a parseable S3 <Error> rather than axum's
    // bare 405, so a client gets "NotImplemented" instead of an empty body it
    // cannot decode.
    Router::new()
        .route("/", buckets_route)
        .route("/{bucket}", bucket_route)
        .route("/{bucket}/{*key}", object_route)
        .fallback(|| async { S3Error::not_implemented("this operation").into_response() })
        .method_not_allowed_fallback(|| async { S3Error::not_implemented("this method").into_response() })
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = parse_args()?;

    // Read the credential up front so a bad path fails at startup rather than
    // as a mid-stream PermissionDenied. The principal travels in the file.
    let credential: Option<(String, Vec<u8>)> = match &args.credential_file {
        Some(path) => {
            let (principal, secret) = autumn_client::read_credential_file(path)?;
            if principal.is_empty() {
                bail!(
                    "--credential-file {}: missing principal name (expected '<principal>\\n<hex>')",
                    path.display()
                );
            }
            Some((principal, secret))
        }
        None => None,
    };

    let addr: SocketAddr = format!("{}:{}", args.listen, args.port)
        .parse()
        .with_context(|| format!("--listen/--port: {}:{}", args.listen, args.port))?;

    tracing::info!(
        manager = %args.manager,
        authz = args.credential_file.is_some(),
        direct_read = args.direct_read,
        workers = args.workers,
        sweep_interval_secs = args.sweep_interval_secs,
        "autumn-s3 (unauthenticated) on http://{addr}"
    );

    let (reclaim_tx, reclaim_rx) = mpsc::unbounded();
    let (exit_tx, exit_rx) = std::sync::mpsc::channel();
    for idx in 0..args.workers {
        let args = Args {
            manager: args.manager.clone(),
            listen: args.listen.clone(),
            port: args.port,
            host: args.host.clone(),
            credential_file: args.credential_file.clone(),
            direct_read: args.direct_read,
            workers: args.workers,
            sweep_interval_secs: args.sweep_interval_secs,
        };
        let cred = credential.clone();
        let tx = reclaim_tx.clone();
        spawn_role(format!("autumn-s3-{idx}"), exit_tx.clone(), move || serve_worker(idx, &args, cred, addr, tx))?;
    }
    drop(reclaim_tx);
    {
        let sweep_args = Args {
            manager: args.manager.clone(),
            listen: args.listen.clone(),
            port: args.port,
            host: args.host.clone(),
            credential_file: args.credential_file.clone(),
            direct_read: args.direct_read,
            workers: args.workers,
            sweep_interval_secs: args.sweep_interval_secs,
        };
        let cred = credential.clone();
        spawn_role("autumn-s3-sweep".into(), exit_tx.clone(), move || reclaim_worker(&sweep_args, cred, reclaim_rx))?;
    }
    // No thread returns while the gateway is healthy: a worker only on error,
    // the reclaimer only once every worker is gone. The first to stop ends the
    // process rather than let it serve on fewer threads than asked for, or —
    // the reclaimer — keep deleting names while nothing reclaims their data.
    let (name, r) = exit_rx.recv().context("every gateway thread vanished")?;
    match r {
        Ok(()) => bail!("{name} stopped"),
        Err(e) => Err(e.context(format!("{name} stopped"))),
    }
}

/// Run `f` on a thread named `name`; its result, or a panic, is reported on
/// `exited`.
fn spawn_role(
    name: String,
    exited: std::sync::mpsc::Sender<(String, Result<()>)>,
    f: impl FnOnce() -> Result<()> + Send + 'static,
) -> Result<()> {
    /// Reports a panic: `Drop` runs while the thread unwinds.
    struct Report(Option<(String, std::sync::mpsc::Sender<(String, Result<()>)>)>);
    impl Drop for Report {
        fn drop(&mut self) {
            if let Some((name, tx)) = self.0.take() {
                // Fails only when `main` has already stopped listening.
                tx.send((name, Err(anyhow::anyhow!("panicked")))).ok();
            }
        }
    }
    std::thread::Builder::new().name(name.clone()).spawn(move || {
        let mut report = Report(Some((name, exited)));
        let r = f();
        if let Some((name, tx)) = report.0.take() {
            tx.send((name, r)).ok();
        }
    })?;
    Ok(())
}
