//! `autumn-s3` — a read-only, unauthenticated S3-compatible gateway over
//! autumn's `fs/` tree.
//!
//! It exists for one reason: inference engines that have no loader plugin seam
//! still ship a `runai_streamer` load format, and that streamer speaks S3.
//! Serving the three operations it actually issues — `ListObjectsV2`, ranged
//! `GetObject`, whole `GetObject` — is enough to give SGLang and FreeToken a
//! concurrent streaming weight path with no engine patches, and incidentally
//! lets every other S3 tool read autumn.
//!
//! Deliberately NOT implemented: PUT/DELETE, multipart, versioning, ACLs,
//! virtual-host addressing, and SigV4 verification. Requests are served
//! whatever their `Authorization` header says — including none. Clients still
//! need dummy credentials set, because the AWS SDK's credential chain runs
//! before the request is ever sent.
//!
//! Reads go through the same `autumn-fuse` core the `autumn.Fs` binding uses,
//! so lane striping, EN-direct reads and authz all apply unchanged. This is an
//! adapter over the partition layer, not a second data plane.

mod objects;
mod s3;

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
use autumn_fuse::state::FsState;
use send_wrapper::SendWrapper;

use objects::Fs;
use s3::{RangeSpec, S3Error};

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
}

/// Accept threads to run by default. The gateway is CPU-bound on socket work,
/// not on autumn reads: an AWS-CRT client drains its sockets slowly enough that
/// each body write fragments into many partial writes, and a single accept
/// thread saturates well below what the read path can deliver. It is also a
/// sidecar, so it should not claim every core on a GPU node.
const DEFAULT_WORKERS: usize = 8;

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
            "-h" | "--help" => {
                println!(
                    "autumn-s3 --manager <host:port> [--listen 0.0.0.0] [--port 9000]\n\
                     \x20            [--host <daemon-identity>] [--credential-file <path>]\n\
                     \x20            [--direct-read true|false] [--workers N]"
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
        .filter(|n| *n > 0)
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

/// `HEAD /{bucket}/{key}`.
async fn head_object(fs: &Fs, bucket: String, key: String) -> Response {
    match objects::stat(fs, &bucket, &key).await {
        Ok(None) => S3Error::no_such_key(format!("{bucket}/{key}")).into_response(),
        Err(e) => S3Error::internal(e.to_string(), format!("{bucket}/{key}")).into_response(),
        Ok(Some(s)) => (
            StatusCode::OK,
            [
                (header::CONTENT_LENGTH, s.size.to_string()),
                (header::CONTENT_TYPE, "application/octet-stream".into()),
                (header::ACCEPT_RANGES, "bytes".into()),
                (header::ETAG, format!("\"{}\"", s.etag)),
                (header::LAST_MODIFIED, s3::iso8601(s.mtime_secs)),
            ],
        )
            .into_response(),
    }
}

/// `GET /{bucket}/{key}`, with or without a `Range`.
async fn get_object(fs: &Fs, bucket: String, key: String, headers: HeaderMap) -> Response {
    let resource = format!("{bucket}/{key}");
    let stat = match objects::stat(fs, &bucket, &key).await {
        Ok(Some(s)) => s,
        Ok(None) => return S3Error::no_such_key(resource).into_response(),
        Err(e) => return S3Error::internal(e.to_string(), resource).into_response(),
    };

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

    let mut fields = vec![
        (header::CONTENT_LENGTH, len.to_string()),
        (header::CONTENT_TYPE, "application/octet-stream".to_string()),
        (header::ACCEPT_RANGES, "bytes".to_string()),
        (header::ETAG, format!("\"{}\"", stat.etag)),
        (header::LAST_MODIFIED, s3::iso8601(stat.mtime_secs)),
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
    let stream = futures::stream::unfold(
        (fs, ino, start, len),
        |(fs, ino, off, remaining)| async move {
            if remaining == 0 {
                return None;
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
                        (fs, ino, off + n, remaining - n.min(remaining)),
                    ))
                }
                Err(e) => Some((
                    Err(std::io::Error::other(e.to_string())),
                    (fs, ino, off, 0),
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

/// Serve on one thread: its own compio runtime, its own `FsState` (so workers
/// share no lock), its own listener.
fn serve_worker(
    idx: usize,
    args: &Args,
    credential: Option<(String, Vec<u8>)>,
    addr: SocketAddr,
) -> Result<()> {
    let rt = compio::runtime::Runtime::new().context("compio runtime")?;
    rt.block_on(async move {
        // A distinct daemon identity per worker: the manager keys its lease
        // registry on it, and two workers sharing one would look like a single
        // client reconnecting.
        let host = format!("{}-{idx}", args.host);
        let mut state = match credential {
            Some((who, secret)) => {
                FsState::new_with_host_credential(&args.manager, host, &who, secret).await?
            }
            None => FsState::new_with_host(&args.manager, host).await?,
        };
        state.direct_read = args.direct_read;
        let fs: Fs = Rc::new(futures::lock::Mutex::new(state));

        let listener = compio::net::TcpListener::from_std(reuseport_listener(addr)?)?;
        cyper_axum::serve(listener, router(fs)).await?;
        Ok::<_, anyhow::Error>(())
    })
}

fn router(fs: Fs) -> Router {
    let f = SendWrapper::new(fs.clone());
    let buckets_route = get(move || {
        let f = f.clone();
        SendWrapper::new(async move { list_buckets(&f).await })
    });
    let f = SendWrapper::new(fs.clone());
    let bucket_route = get(move |Path(b): Path<String>, Query(q): Query<HashMap<String, String>>| {
        let f = f.clone();
        SendWrapper::new(async move { list_objects(&f, b, q).await })
    });
    let f = SendWrapper::new(fs.clone());
    let g = SendWrapper::new(fs);
    let object_route = get(
        move |Path((b, k)): Path<(String, String)>, headers: HeaderMap| {
            let f = f.clone();
            SendWrapper::new(async move { get_object(&f, b, k, headers).await })
        },
    )
    .head(move |Path((b, k)): Path<(String, String)>| {
        let g = g.clone();
        SendWrapper::new(async move { head_object(&g, b, k).await })
    });

    // Every mutating verb answers with a parseable S3 <Error> rather than
    // axum's bare 405, so a client that tries to write gets "NotImplemented"
    // instead of an empty body it cannot decode.
    Router::new()
        .route("/", buckets_route)
        .route("/{bucket}", bucket_route)
        .route("/{bucket}/{*key}", object_route)
        .fallback(|| async { S3Error::not_implemented("this operation").into_response() })
        .method_not_allowed_fallback(|| async {
            S3Error::not_implemented("writing through this gateway").into_response()
        })
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
        "autumn-s3 (read-only, unauthenticated) on http://{addr}"
    );

    let mut handles = Vec::with_capacity(args.workers);
    for idx in 0..args.workers {
        let args = Args {
            manager: args.manager.clone(),
            listen: args.listen.clone(),
            port: args.port,
            host: args.host.clone(),
            credential_file: args.credential_file.clone(),
            direct_read: args.direct_read,
            workers: args.workers,
        };
        let cred = credential.clone();
        handles.push(
            std::thread::Builder::new()
                .name(format!("autumn-s3-{idx}"))
                .spawn(move || serve_worker(idx, &args, cred, addr))?,
        );
    }
    // A worker only returns on error; surface the first one and let the process
    // die rather than silently serving on fewer threads than asked for.
    for h in handles {
        match h.join() {
            Ok(r) => r?,
            Err(_) => bail!("a gateway worker thread panicked"),
        }
    }
    Ok(())
}
