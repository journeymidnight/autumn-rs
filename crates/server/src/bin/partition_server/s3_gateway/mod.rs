//! A read-only, unauthenticated S3-compatible gateway over autumn's `fs/`
//! tree, served by the partition server when `--s3-gateway` is on (default
//! off).
//!
//! It runs on its OWN OS threads with their own compio runtimes and its own
//! per-worker `FsState`, so it shares no runtime and no lock with the partition
//! threads: an engine hammering the gateway cannot take accept latency away
//! from partition traffic.
//!
//! It does NOT share their CPU budget in the way the flag names suggest.
//! `--cpuset` populates a list that the partition and shard runtimes consult
//! when pinning THEIR OWN threads; it does not set a process affinity mask, and
//! these threads pin nothing. They inherit whatever mask the process was
//! launched with — so on a box where the cpuset exists to dodge a tenant, the
//! gateway will not dodge it. `taskset` the process if that matters.
//!
//! What moving it off the engine's node buys is one copy on that node: a
//! sidecar received the bytes over the network and sent them again over
//! loopback, and now the engine's node receives them once. It does NOT make the
//! reads local — replica choice is a hash rotation with no locality preference
//! (`ClusterClient`), so on a multi-node cluster most of these reads still come
//! from a remote extent node.
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

/// What the gateway needs from the partition server that hosts it. The manager
/// address is the PS's; the credential is NOT — the gateway reads `fs/` as a
/// client, so it needs a principal granted that prefix, which the partition
/// server's own data-plane identity is not.
#[derive(Clone)]
pub struct Config {
    pub manager: String,
    pub listen: String,
    pub port: u16,
    pub host: String,
    pub credential_file: Option<PathBuf>,
    pub direct_read: bool,
    pub workers: usize,
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
    args: &Config,
    credential: Option<(String, Vec<u8>)>,
    listener: std::net::TcpListener,
) -> Result<()> {
    let rt = compio::runtime::Runtime::new().context("compio runtime")?;
    rt.block_on(async move {
        // A per-worker label, for humans. The lease registry keys on a fresh
        // UUID per `FsState`, not on this string, so identical names could not
        // collide — the suffix exists so `autumn-op` listings say which worker
        // a connection belongs to.
        let host = format!("{}-{idx}", args.host);
        let mut state = match credential {
            Some((who, secret)) => {
                FsState::new_with_host_credential(&args.manager, host, &who, secret).await?
            }
            None => FsState::new_with_host(&args.manager, host).await?,
        };
        state.direct_read = args.direct_read;
        let fs: Fs = Rc::new(futures::lock::Mutex::new(state));

        let listener = compio::net::TcpListener::from_std(listener)?;
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

/// Start the gateway on `cfg.workers` dedicated OS threads and return.
///
/// Fails fast on what an operator can fix before traffic arrives: an unreadable
/// credential, an unparseable address, an address that cannot be bound. It does
/// NOT catch a credential the manager rejects — that surfaces on the first
/// connect, inside a worker — nor a port another process already holds, because
/// SO_REUSEPORT is what lets the workers share one port and it lets a second
/// process share it too.
///
/// A worker that RETURNS AN ERROR is logged loudly and does not take the
/// partition server with it: serving partitions is this process's job and a
/// read-only gateway is not worth dropping partitions for. **A worker that
/// PANICS is a different matter** — the release profile sets
/// `panic = "abort"`, so any panic anywhere in this process, gateway included,
/// aborts the partition server. That is the price of hosting the gateway
/// in-process instead of beside the engine, where a panic cost only the
/// sidecar; it cannot be bought back without a process boundary.
pub fn spawn(cfg: Config) -> Result<()> {
    // Read the credential up front so a bad path fails at startup rather than
    // as a mid-stream PermissionDenied. The principal travels in the file.
    let credential: Option<(String, Vec<u8>)> = match &cfg.credential_file {
        Some(path) => {
            let (principal, secret) = autumn_client::read_credential_file(path)?;
            if principal.is_empty() {
                bail!(
                    "--s3-gateway-credential-file {}: missing principal name \
                     (expected '<principal>\\n<hex>')",
                    path.display()
                );
            }
            Some((principal, secret))
        }
        None => None,
    };

    let addr: SocketAddr = format!("{}:{}", cfg.listen, cfg.port)
        .parse()
        .with_context(|| format!("--s3-gateway-listen/--s3-gateway-port: {}:{}", cfg.listen, cfg.port))?;

    tracing::info!(
        manager = %cfg.manager,
        authz = cfg.credential_file.is_some(),
        direct_read = cfg.direct_read,
        workers = cfg.workers,
        "s3 gateway (read-only, unauthenticated) on http://{addr}"
    );

    // Each worker reports its own exit rather than the supervisor joining
    // handles in order. Joining in order means a worker's death is invisible
    // until every LOWER-indexed worker has also died — worker 3 could be gone
    // for hours while the supervisor blocks on worker 0, which is exactly the
    // silence this supervisor exists to prevent. A channel reports in the
    // order deaths actually happen.
    // Bind every listener HERE, not inside the workers. Binding on the worker
    // threads meant an unusable address killed all of them at startup while
    // `spawn` had already returned `Ok`, leaving a partition server running
    // with no gateway and only log lines to say so. Binding first makes that
    // what the flag implies: fatal.
    //
    // Note what this does NOT catch. These listeners set SO_REUSEPORT — that is
    // how N workers share one port — so a port already held by ANOTHER process
    // binds successfully. Two partition servers with the gateway on the same
    // port on the same host will each get a share of the connections rather
    // than one of them refusing to start.
    let mut listeners = Vec::with_capacity(cfg.workers);
    for _ in 0..cfg.workers {
        listeners.push(
            reuseport_listener(addr)
                .with_context(|| format!("binding the s3 gateway on {addr}"))?,
        );
    }

    let (tx, rx) = std::sync::mpsc::channel::<(usize, String)>();
    for (idx, listener) in listeners.into_iter().enumerate() {
        let cfg = cfg.clone();
        let cred = credential.clone();
        let tx = tx.clone();
        std::thread::Builder::new()
            .name(format!("autumn-s3-{idx}"))
            .spawn(move || {
                // Report from a guard's Drop, not after the call: a worker that
                // PANICS never reaches the line after it, so with 7 of 8 healthy
                // the eighth's death produced no log at all — "silently serving
                // on fewer threads than asked for", which is the failure this
                // supervisor exists to prevent. Drop runs during unwind, so the
                // panic is reported like any other exit. (Release builds set
                // `panic = "abort"`, so there is no unwind and the process is
                // already gone — see the note on the failure policy above.)
                struct Report(usize, std::sync::mpsc::Sender<(usize, String)>, String);
                impl Drop for Report {
                    fn drop(&mut self) {
                        let _ = self.1.send((self.0, std::mem::take(&mut self.2)));
                    }
                }
                let mut report = Report(idx, tx, "panicked".to_string());
                report.2 = match serve_worker(idx, &cfg, cred, listener) {
                    Ok(()) => "returned".to_string(),
                    Err(e) => format!("{e:#}"),
                };
            })?;
    }
    // Drop the template sender so the channel closes when the last worker is
    // gone (or has panicked), which is what ends the supervisor's loop.
    drop(tx);

    let n = cfg.workers;
    std::thread::Builder::new()
        .name("autumn-s3-sup".into())
        .spawn(move || {
            let mut stopped = 0usize;
            while let Ok((idx, why)) = rx.recv() {
                stopped += 1;
                tracing::error!(
                    worker = idx,
                    remaining = n - stopped,
                    error = %why,
                    "s3 gateway worker stopped; the partition server keeps serving partitions"
                );
            }
            tracing::error!(
                reported = stopped,
                workers = n,
                "s3 gateway is DOWN — no worker is still serving"
            );
        })?;
    Ok(())
}
