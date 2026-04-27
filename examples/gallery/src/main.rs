use std::cell::RefCell;
use std::io::Cursor;
use std::rc::Rc;
use std::time::Instant;

use futures::lock::Mutex;

use anyhow::Result;
use autumn_client::{AutumnError, ClusterClient};
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Multipart, Path};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::routing::{delete, get, post};
use axum::Router;
use image::codecs::jpeg::JpegEncoder;
use image::ImageReader;
use send_wrapper::SendWrapper;

type Client = Rc<Mutex<ClusterClient>>;
type MetricsRef = Rc<RefCell<PerfMetrics>>;

// ---------------------------------------------------------------------------
// Storage-layer perf metrics (EMA, single-threaded, RefCell)
// ---------------------------------------------------------------------------

/// Exponentially weighted moving averages of autumn-client call latency and
/// throughput. The HUD shows these so users see what the underlying KV
/// store is delivering, not what the browser perceives end-to-end.
#[derive(Default)]
struct PerfMetrics {
    put_lat_ms: f64,
    put_bw: f64, // bytes/sec
    get_lat_ms: f64,
    get_bw: f64,
    thumb_build_ms: f64, // CPU time inside spawn_blocking
}

impl PerfMetrics {
    const ALPHA: f64 = 0.3;

    fn ema(prev: f64, sample: f64) -> f64 {
        if prev <= 0.0 {
            sample
        } else {
            prev * (1.0 - Self::ALPHA) + sample * Self::ALPHA
        }
    }

    fn record_put(&mut self, ms: f64, bytes: u64) {
        if ms <= 0.0 {
            return;
        }
        self.put_lat_ms = Self::ema(self.put_lat_ms, ms);
        self.put_bw = Self::ema(self.put_bw, (bytes as f64) / (ms / 1000.0));
    }

    fn record_get(&mut self, ms: f64, bytes: u64) {
        if ms <= 0.0 {
            return;
        }
        self.get_lat_ms = Self::ema(self.get_lat_ms, ms);
        self.get_bw = Self::ema(self.get_bw, (bytes as f64) / (ms / 1000.0));
    }

    fn record_thumb_build(&mut self, ms: f64) {
        if ms < 0.0 {
            return;
        }
        self.thumb_build_ms = Self::ema(self.thumb_build_ms, ms);
    }

    fn to_json(&self) -> String {
        format!(
            r#"{{"put_ms":{:.2},"put_bps":{:.0},"get_ms":{:.2},"get_bps":{:.0},"thumb_build_ms":{:.2}}}"#,
            self.put_lat_ms, self.put_bw, self.get_lat_ms, self.get_bw, self.thumb_build_ms
        )
    }
}

fn elapsed_ms(t0: Instant) -> f64 {
    t0.elapsed().as_secs_f64() * 1000.0
}

// ---------------------------------------------------------------------------
// Thumbnail helpers
// ---------------------------------------------------------------------------

const THUMB_PREFIX: &str = ".thumb/";
const THUMB_WIDTH: u32 = 320;
const THUMB_QUALITY: u8 = 80;
// Must match the bind in main(). Used so ffmpeg can pull video bytes from
// our own /get/ route via HTTP Range, avoiding a full-blob fetch.
const LISTEN_PORT: u16 = 5001;

fn thumb_key(name: &str) -> String {
    format!("{THUMB_PREFIX}{THUMB_WIDTH}/{name}")
}

fn ext_of(name: &str) -> String {
    name.rsplit('.').next().unwrap_or("").to_ascii_lowercase()
}

fn is_image_ext(ext: &str) -> bool {
    matches!(ext, "jpg" | "jpeg" | "png" | "gif" | "bmp" | "webp")
}

fn is_video_ext(ext: &str) -> bool {
    matches!(ext, "mp4" | "webm" | "ogg" | "mov" | "m4v")
}

fn is_svg_ext(ext: &str) -> bool {
    ext == "svg"
}

/// Decode the original image bytes, downscale to THUMB_WIDTH preserving aspect,
/// re-encode as JPEG. Pure-CPU work; small for typical photos but flagged for
/// callers that care about latency.
fn build_thumbnail(bytes: &[u8]) -> Result<Vec<u8>> {
    let img = ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()?
        .decode()?;
    let thumb = img.thumbnail(THUMB_WIDTH, THUMB_WIDTH);
    let rgb = thumb.to_rgb8();
    let mut out = Vec::with_capacity(32 * 1024);
    let encoder = JpegEncoder::new_with_quality(&mut out, THUMB_QUALITY);
    rgb.write_with_encoder(encoder)?;
    Ok(out)
}

/// Extract the first keyframe past 0.5s from a video file and re-encode it
/// as a `THUMB_WIDTH`-wide JPEG via ffmpeg subprocess. Requires `ffmpeg` in
/// PATH; on missing-binary or codec failure returns Err and the caller
/// falls back to no-thumbnail (front-end shows the play glyph alone).
///
/// Input is an HTTP URL pointing at our own /get/ route. ffmpeg's HTTP
/// demuxer issues Range requests, so it only fetches the bytes it needs
/// to find the moov atom + the keyframe near `-ss 0.5` — usually a few
/// hundred KB, regardless of video size. This avoids the old "download
/// the whole file then spool to tempfile" cost.
fn build_video_thumbnail(url: &str) -> Result<Vec<u8>> {
    use std::process::{Command, Stdio};

    // `-2` rounds height down to a multiple of 2 — JPEG and MJPEG can't
    // encode odd dimensions cleanly.
    let scale = format!("scale={THUMB_WIDTH}:-2");

    let output = Command::new("ffmpeg")
        .args([
            "-y",
            "-loglevel",
            "error",
            // input-side seek: faster than `-ss` after `-i`, accurate to
            // the nearest keyframe (good enough for a thumbnail)
            "-ss",
            "0.5",
            "-i",
            url,
            "-vframes",
            "1",
            "-vf",
            &scale,
            "-q:v",
            "5",
            "-f",
            "mjpeg",
            "-",
        ])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()?;

    if !output.status.success() {
        anyhow::bail!("ffmpeg exited with {}", output.status);
    }
    if output.stdout.is_empty() {
        anyhow::bail!("ffmpeg produced no frame");
    }
    Ok(output.stdout)
}

// ---------------------------------------------------------------------------
// HTTP response helpers
// ---------------------------------------------------------------------------

fn ok_response(body: impl Into<Body>) -> Response<Body> {
    Response::builder().body(body.into()).unwrap()
}

fn error_response(status: StatusCode, msg: String) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Body::from(msg))
        .unwrap()
}

// ---------------------------------------------------------------------------
// HTTP handlers (each returns SendWrapper future for axum Send bound)
// ---------------------------------------------------------------------------

async fn index_handler() -> Response<Body> {
    Response::builder()
        .header("content-type", "text/html; charset=utf-8")
        .body(Body::from(include_str!("../static/index.html")))
        .unwrap()
}

async fn put_handler_inner(
    client: &Client,
    metrics: &MetricsRef,
    mut multipart: Multipart,
) -> Response<Body> {
    while let Ok(Some(field)) = multipart.next_field().await {
        let filename = match field.file_name() {
            Some(name) => name.to_string(),
            None => continue,
        };
        let data = match field.bytes().await {
            Ok(b) => b.to_vec(),
            Err(e) => return error_response(StatusCode::BAD_REQUEST, format!("read field: {e}")),
        };
        let bytes = data.len() as u64;
        let t0 = Instant::now();
        let put_res = client.lock().await.put(filename.as_bytes(), &data, true).await;
        let dt = elapsed_ms(t0);
        if let Err(e) = put_res {
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("put: {e}"));
        }
        metrics.borrow_mut().record_put(dt, bytes);
        // Invalidate any cached thumbnail so the next /thumb/<name> rebuilds.
        let _ = client
            .lock()
            .await
            .delete(thumb_key(&filename).as_bytes())
            .await;
        return ok_response(filename);
    }
    error_response(StatusCode::BAD_REQUEST, "no file field".into())
}

async fn get_handler_inner(
    client: &Client,
    metrics: &MetricsRef,
    name: String,
    headers: HeaderMap,
) -> Response<Body> {
    let mime = mime_guess::from_path(&name)
        .first_or_octet_stream()
        .to_string();

    // Check for HTTP Range header — use sub-range read to avoid loading entire value.
    if let Some(range_header) = headers.get("range") {
        if let Ok(range_str) = range_header.to_str() {
            if let Some(bytes_range) = range_str.strip_prefix("bytes=") {
                let parts: Vec<&str> = bytes_range.splitn(2, '-').collect();
                if parts.len() == 2 {
                    let start: usize = parts[0].parse().unwrap_or(0);
                    let end_str = parts[1];

                    // Always call head() to get total size — Safari requires the exact total
                    // in every Content-Range response (bytes X-Y/*  is rejected by Safari).
                    let total_size: usize = match client.lock().await.head(name.as_bytes()).await {
                        Ok(meta) if meta.found => meta.value_length as usize,
                        Ok(_) => return error_response(StatusCode::NOT_FOUND, "not found".into()),
                        Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("head: {e}")),
                    };

                    let end: usize = if end_str.is_empty() {
                        total_size.saturating_sub(1)
                    } else {
                        end_str.parse().unwrap_or(usize::MAX).min(total_size.saturating_sub(1))
                    };

                    if start <= end {
                        let length = end - start + 1;
                        let t0 = Instant::now();
                        let range_res = client
                            .lock()
                            .await
                            .get_range(name.as_bytes(), start as u32, length as u32)
                            .await;
                        let dt = elapsed_ms(t0);
                        let slice = match range_res {
                            Ok(Some(v)) => v,
                            Ok(None) => return error_response(StatusCode::NOT_FOUND, "not found".into()),
                            Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("get: {e}")),
                        };
                        metrics.borrow_mut().record_get(dt, slice.len() as u64);
                        let actual_end = start + slice.len() - 1;
                        return Response::builder()
                            .status(StatusCode::PARTIAL_CONTENT)
                            .header("content-type", &mime)
                            .header("accept-ranges", "bytes")
                            .header("content-length", slice.len())
                            .header("content-range", format!("bytes {start}-{actual_end}/{total_size}"))
                            .body(Body::from(slice))
                            .unwrap();
                    }
                }
            }
        }
    }

    // No Range header — full read via SDK.
    let t0 = Instant::now();
    let res = client.lock().await.get(name.as_bytes()).await;
    let dt = elapsed_ms(t0);
    match res {
        Ok(Some(v)) => {
            metrics.borrow_mut().record_get(dt, v.len() as u64);
            Response::builder()
                .header("content-type", &mime)
                .header("accept-ranges", "bytes")
                .header("content-length", v.len())
                .body(Body::from(v))
                .unwrap()
        }
        Ok(None) => error_response(StatusCode::NOT_FOUND, "not found".into()),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("get: {e}")),
    }
}

async fn delete_handler_inner(client: &Client, name: String) -> Response<Body> {
    let result = client.lock().await.delete(name.as_bytes()).await;
    match result {
        Ok(()) => {
            // Best-effort cleanup of the cached thumbnail. NotFound is expected
            // for non-image files and for images that were never previewed.
            let _ = client
                .lock()
                .await
                .delete(thumb_key(&name).as_bytes())
                .await;
            ok_response("OK")
        }
        Err(AutumnError::NotFound) => error_response(StatusCode::NOT_FOUND, "File not found".into()),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("delete: {e}")),
    }
}

async fn list_handler_inner(client: &Client) -> Response<Body> {
    match client.lock().await.range(b"", b"", u32::MAX).await {
        Ok(result) => {
            let keys: Vec<String> = result
                .entries
                .iter()
                .filter_map(|e| {
                    let s = String::from_utf8_lossy(&e.key).to_string();
                    // Hide the thumbnail cache from user-facing listings.
                    (!s.starts_with(THUMB_PREFIX)).then_some(s)
                })
                .collect();
            Response::builder()
                .header("content-type", "text/plain; charset=utf-8")
                .body(Body::from(keys.join("\n")))
                .unwrap()
        }
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("list: {e}")),
    }
}

async fn thumb_handler_inner(
    client: &Client,
    metrics: &MetricsRef,
    name: String,
) -> Response<Body> {
    let ext = ext_of(&name);

    // SVG: no point rasterizing — just serve the original bytes.
    if is_svg_ext(&ext) {
        let t0 = Instant::now();
        let res = client.lock().await.get(name.as_bytes()).await;
        let dt = elapsed_ms(t0);
        return match res {
            Ok(Some(v)) => {
                metrics.borrow_mut().record_get(dt, v.len() as u64);
                Response::builder()
                    .header("content-type", "image/svg+xml")
                    .header("cache-control", "public, max-age=86400")
                    .body(Body::from(v))
                    .unwrap()
            }
            Ok(None) => error_response(StatusCode::NOT_FOUND, "not found".into()),
            Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("get: {e}")),
        };
    }

    let is_video = is_video_ext(&ext);
    if !is_image_ext(&ext) && !is_video {
        return error_response(StatusCode::NOT_FOUND, "no thumbnail for this file type".into());
    }

    let key = thumb_key(&name);

    // Cache hit fast path.
    let t0 = Instant::now();
    let cache_res = client.lock().await.get(key.as_bytes()).await;
    let dt = elapsed_ms(t0);
    match cache_res {
        Ok(Some(v)) => {
            metrics.borrow_mut().record_get(dt, v.len() as u64);
            return Response::builder()
                .header("content-type", "image/jpeg")
                .header("cache-control", "public, max-age=86400")
                .header("content-length", v.len())
                .body(Body::from(v))
                .unwrap();
        }
        Ok(None) => { /* fall through to generation */ }
        Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("get thumb: {e}")),
    }

    // Image path: decode+resize via the `image` crate. Failure falls back
    // to serving the original bytes (so a malformed PNG still renders).
    // Video path: ffmpeg subprocess pulled via HTTP from our own /get/
    // route — Range requests fetch only the bytes needed for the keyframe,
    // so we never download the full video. Failure (codec/missing-binary)
    // gives up with 404 — front-end shows the play glyph alone.
    enum BuildOutcome {
        Ok(Vec<u8>),
        ImageFallback(anyhow::Error, Vec<u8>),
        VideoGiveUp(anyhow::Error),
    }

    let build_t0 = Instant::now();
    let build_res = if is_video {
        // No client.get(name) here — ffmpeg fetches the bytes itself via
        // HTTP Range against /get/, so a 1 GB video costs only the
        // ~few-hundred-KB ffmpeg actually demuxes.
        let encoded = percent_encoding::utf8_percent_encode(
            &name,
            percent_encoding::NON_ALPHANUMERIC,
        )
        .to_string();
        let url = format!("http://127.0.0.1:{LISTEN_PORT}/get/{encoded}");
        compio::runtime::spawn_blocking(move || {
            match build_video_thumbnail(&url) {
                Ok(b) => BuildOutcome::Ok(b),
                Err(e) => BuildOutcome::VideoGiveUp(e),
            }
        })
        .await
    } else {
        // Image path still loads the full original — image decoders aren't
        // range-friendly and these payloads are small anyway.
        let t0 = Instant::now();
        let orig_res = client.lock().await.get(name.as_bytes()).await;
        let dt = elapsed_ms(t0);
        let original = match orig_res {
            Ok(Some(v)) => {
                metrics.borrow_mut().record_get(dt, v.len() as u64);
                v
            }
            Ok(None) => return error_response(StatusCode::NOT_FOUND, "not found".into()),
            Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("get: {e}")),
        };
        compio::runtime::spawn_blocking(move || {
            match build_thumbnail(&original) {
                Ok(b) => BuildOutcome::Ok(b),
                Err(e) => BuildOutcome::ImageFallback(e, original),
            }
        })
        .await
    };
    let build_ms = elapsed_ms(build_t0);
    let thumb = match build_res {
        Ok(BuildOutcome::Ok(b)) => {
            metrics.borrow_mut().record_thumb_build(build_ms);
            b
        }
        Ok(BuildOutcome::ImageFallback(e, original)) => {
            tracing::warn!("thumbnail build failed for {name}: {e} — serving original");
            let mime = mime_guess::from_path(&name)
                .first_or_octet_stream()
                .to_string();
            return Response::builder()
                .header("content-type", mime)
                .body(Body::from(original))
                .unwrap();
        }
        Ok(BuildOutcome::VideoGiveUp(e)) => {
            tracing::warn!("video thumbnail failed for {name}: {e}");
            return error_response(
                StatusCode::NOT_FOUND,
                "no video thumbnail (ffmpeg missing or codec unsupported)".into(),
            );
        }
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "thumbnail worker panicked".into(),
            );
        }
    };

    // Write back to autumn for next time. Best-effort and detached: the
    // response ships the JPEG bytes to the client without waiting on the
    // cache put. Bytes is Arc-backed so the body and the spawned put share
    // the buffer without an extra copy.
    let thumb = bytes::Bytes::from(thumb);
    {
        let client = client.clone();
        let thumb = thumb.clone();
        compio::runtime::spawn(async move {
            if let Err(e) = client
                .lock()
                .await
                .put(key.as_bytes(), &thumb, false)
                .await
            {
                tracing::warn!("cache thumbnail put failed for {name}: {e}");
            }
        })
        .detach();
    }

    Response::builder()
        .header("content-type", "image/jpeg")
        .header("cache-control", "public, max-age=86400")
        .header("content-length", thumb.len())
        .body(Body::from(thumb))
        .unwrap()
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[compio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let manager = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9001".to_string());

    let client: Client = Rc::new(Mutex::new(
        ClusterClient::connect(&manager).await?,
    ));
    let metrics: MetricsRef = Rc::new(RefCell::new(PerfMetrics::default()));

    // Wrap captures in SendWrapper to satisfy axum's Send requirement.
    // Safe because compio runs everything on a single thread.
    let cm = SendWrapper::new((client.clone(), metrics.clone()));
    let put_route = post(move |multipart: Multipart| {
        let cm = cm.clone();
        SendWrapper::new(async move {
            let (c, m) = (&cm.0, &cm.1);
            put_handler_inner(c, m, multipart).await
        })
    });

    let cm = SendWrapper::new((client.clone(), metrics.clone()));
    let get_route = get(move |Path(name): Path<String>, headers: HeaderMap| {
        let cm = cm.clone();
        SendWrapper::new(async move {
            let (c, m) = (&cm.0, &cm.1);
            get_handler_inner(c, m, name, headers).await
        })
    });

    let c = SendWrapper::new(client.clone());
    let del_route = delete(move |Path(name): Path<String>| {
        let c = c.clone();
        SendWrapper::new(async move { delete_handler_inner(&c, name).await })
    });

    let c = SendWrapper::new(client.clone());
    let list_route = get(move || {
        let c = c.clone();
        SendWrapper::new(async move { list_handler_inner(&c).await })
    });

    let cm = SendWrapper::new((client.clone(), metrics.clone()));
    let thumb_route = get(move |Path(name): Path<String>| {
        let cm = cm.clone();
        SendWrapper::new(async move {
            let (c, m) = (&cm.0, &cm.1);
            thumb_handler_inner(c, m, name).await
        })
    });

    let m = SendWrapper::new(metrics.clone());
    let metrics_route = get(move || {
        let m = m.clone();
        SendWrapper::new(async move {
            // borrow_mut not needed — read-only snapshot.
            let body = m.borrow().to_json();
            Response::builder()
                .header("content-type", "application/json")
                .header("cache-control", "no-store")
                .body(Body::from(body))
                .unwrap()
        })
    });

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/put/", put_route)
        .route("/get/{name}", get_route)
        .route("/thumb/{name}", thumb_route)
        .route("/del/{name}", del_route)
        .route("/list/", list_route)
        .route("/metrics/", metrics_route)
        .layer(DefaultBodyLimit::max(1024 * 1024 * 1024)); // 1 GB

    let listener = compio::net::TcpListener::bind(format!("0.0.0.0:{LISTEN_PORT}")).await?;
    tracing::info!("Gallery listening on http://0.0.0.0:{LISTEN_PORT}");
    cyper_axum::serve(listener, app).await?;
    Ok(())
}
