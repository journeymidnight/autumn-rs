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
}

fn parse_args() -> Result<Args> {
    let mut a = Args {
        manager: String::new(),
        listen: "0.0.0.0".into(),
        port: 9000,
        host: "autumn-s3".into(),
        credential_file: None,
        direct_read: true,
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
            "-h" | "--help" => {
                println!(
                    "autumn-s3 --manager <host:port> [--listen 0.0.0.0] [--port 9000]\n\
                     \x20            [--host <daemon-identity>] [--credential-file <path>]\n\
                     \x20            [--direct-read true|false]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown flag {other} (try --help)"),
        }
    }
    if a.manager.is_empty() {
        bail!("--manager is required");
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

#[compio::main]
async fn main() -> Result<()> {
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

    let mut state = match credential {
        Some((who, secret)) => {
            FsState::new_with_host_credential(&args.manager, args.host.clone(), &who, secret).await?
        }
        None => FsState::new_with_host(&args.manager, args.host.clone()).await?,
    };
    state.direct_read = args.direct_read;
    let fs: Fs = Rc::new(futures::lock::Mutex::new(state));

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
    let g = SendWrapper::new(fs.clone());
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
    let app = Router::new()
        .route("/", buckets_route)
        .route("/{bucket}", bucket_route)
        .route("/{bucket}/{*key}", object_route)
        .fallback(|| async { S3Error::not_implemented("this operation").into_response() })
        .method_not_allowed_fallback(|| async {
            S3Error::not_implemented("writing through this gateway").into_response()
        });

    let listener =
        compio::net::TcpListener::bind(format!("{}:{}", args.listen, args.port)).await?;
    tracing::info!(
        manager = %args.manager,
        authz = args.credential_file.is_some(),
        direct_read = args.direct_read,
        "autumn-s3 (read-only, unauthenticated) on http://{}:{}",
        args.listen,
        args.port
    );
    cyper_axum::serve(listener, app).await?;
    Ok(())
}
