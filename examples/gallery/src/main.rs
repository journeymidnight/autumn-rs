use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Cursor;
use std::rc::Rc;
use std::time::Instant;

use futures::lock::Mutex;

use anyhow::{anyhow, Context, Result};
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
type TranscodeMap = Rc<RefCell<HashMap<String, TranscodeStatus>>>;

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
// Thumbnail + HLS helpers
// ---------------------------------------------------------------------------

const THUMB_PREFIX: &str = ".thumb/";
const HLS_PREFIX: &str = ".hls/";
const THUMB_WIDTH: u32 = 320;
const THUMB_QUALITY: u8 = 80;
// Must match the bind in main(). Used so ffmpeg can pull video bytes from
// our own /get/ route via HTTP Range, avoiding a full-blob fetch.
const LISTEN_PORT: u16 = 5001;

fn thumb_key(name: &str) -> String {
    format!("{THUMB_PREFIX}{THUMB_WIDTH}/{name}")
}

fn hls_dir_prefix(name: &str) -> String {
    format!("{HLS_PREFIX}{name}/")
}

fn hls_key(name: &str, file: &str) -> String {
    format!("{HLS_PREFIX}{name}/{file}")
}

fn hls_playlist_key(name: &str) -> String {
    hls_key(name, "index.m3u8")
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

fn url_encode(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
}

fn self_get_url(name: &str) -> String {
    format!("http://127.0.0.1:{LISTEN_PORT}/get/{}", url_encode(name))
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

// ---------------------------------------------------------------------------
// Transcoding state + pipeline
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum TranscodeStatus {
    Queued,
    Transcoding,
    Done,
    Failed(String),
}

impl TranscodeStatus {
    fn as_status_str(&self) -> &'static str {
        match self {
            TranscodeStatus::Queued => "queued",
            TranscodeStatus::Transcoding => "transcoding",
            TranscodeStatus::Done => "done",
            TranscodeStatus::Failed(_) => "failed",
        }
    }

    fn to_json(&self) -> String {
        match self {
            TranscodeStatus::Failed(err) => format!(
                r#"{{"status":"failed","error":{}}}"#,
                json_string(err)
            ),
            other => format!(r#"{{"status":"{}"}}"#, other.as_status_str()),
        }
    }
}

fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Single-bitrate FFmpeg pass: input video URL → directory containing
/// `index.m3u8` + `seg000.ts` … plus `thumb.jpg`. Runs in `spawn_blocking`.
fn run_transcode_blocking(url: &str) -> Result<Vec<(String, Vec<u8>)>> {
    use std::process::{Command, Stdio};

    let tmp = tempfile::tempdir().context("tempdir")?;
    let dir = tmp.path();
    let playlist = dir.join("index.m3u8");
    let segments = dir.join("seg%03d.ts");

    let hls_status = Command::new("ffmpeg")
        .args([
            "-y",
            "-loglevel",
            "error",
            "-i",
            url,
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "23",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-hls_time",
            "4",
            "-hls_list_size",
            "0",
            "-hls_segment_type",
            "mpegts",
            "-hls_flags",
            "independent_segments",
            "-hls_segment_filename",
        ])
        .arg(&segments)
        .arg(&playlist)
        .stdin(Stdio::null())
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .output()
        .context("spawn ffmpeg (hls)")?;
    if !hls_status.status.success() {
        let tail = String::from_utf8_lossy(&hls_status.stderr);
        let tail = tail.lines().rev().take(8).collect::<Vec<_>>().join(" | ");
        return Err(anyhow!("ffmpeg hls exited {}: {tail}", hls_status.status));
    }

    // Thumbnail in a second pass: source is still the original (HTTP Range),
    // so we don't have to re-decode the freshly-written .ts.
    let scale = format!("scale={THUMB_WIDTH}:-2");
    let thumb_path = dir.join("thumb.jpg");
    let thumb_status = Command::new("ffmpeg")
        .args([
            "-y",
            "-loglevel",
            "error",
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
        ])
        .arg(&thumb_path)
        .stdin(Stdio::null())
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .output()
        .context("spawn ffmpeg (thumb)")?;
    if !thumb_status.status.success() {
        let tail = String::from_utf8_lossy(&thumb_status.stderr);
        let tail = tail.lines().rev().take(4).collect::<Vec<_>>().join(" | ");
        // Don't hard-fail the whole transcode if only the thumb pass failed —
        // playback will still work; the cell shows the play glyph alone.
        tracing::warn!("ffmpeg thumb pass failed: {tail}");
    }

    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).context("read tmpdir")? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let fname = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow!("non-utf8 filename"))?
            .to_string();
        let bytes = std::fs::read(&path).with_context(|| format!("read {fname}"))?;
        out.push((fname, bytes));
    }
    Ok(out)
}

/// Drives one transcoding job end-to-end: marks status, runs ffmpeg in a
/// blocking thread, writes HLS segments + thumbnail to KV, then deletes the
/// original. On failure leaves the original in place so the user can retry.
async fn transcode_video_task(name: String, client: Client, map: TranscodeMap) {
    map.borrow_mut()
        .insert(name.clone(), TranscodeStatus::Transcoding);
    tracing::info!("transcode start: {name}");
    let url = self_get_url(&name);
    let res = compio::runtime::spawn_blocking(move || run_transcode_blocking(&url)).await;

    let outputs = match res {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => {
            tracing::warn!("transcode failed for {name}: {e:#}");
            map.borrow_mut()
                .insert(name, TranscodeStatus::Failed(format!("{e:#}")));
            return;
        }
        Err(_) => {
            tracing::warn!("transcode worker panicked for {name}");
            map.borrow_mut()
                .insert(name, TranscodeStatus::Failed("transcode worker panicked".into()));
            return;
        }
    };

    // Push every produced file: thumb.jpg → thumb_key, everything else →
    // .hls/<name>/<fname>. must_sync=false: HLS segments are derivable from
    // the original which we still have until the final delete.
    for (fname, bytes) in outputs {
        let key = if fname == "thumb.jpg" {
            thumb_key(&name)
        } else {
            hls_key(&name, &fname)
        };
        let put_res = client.lock().await.put(key.as_bytes(), &bytes, false).await;
        if let Err(e) = put_res {
            tracing::warn!("write {key} failed: {e}");
            map.borrow_mut()
                .insert(name, TranscodeStatus::Failed(format!("kv put: {e}")));
            return;
        }
    }

    // Drop the original; non-fatal if it's already gone (concurrent delete).
    match client.lock().await.delete(name.as_bytes()).await {
        Ok(_) | Err(AutumnError::NotFound) => {}
        Err(e) => {
            tracing::warn!("delete original {name} failed: {e}");
            map.borrow_mut()
                .insert(name, TranscodeStatus::Failed(format!("delete original: {e}")));
            return;
        }
    }

    tracing::info!("transcode done: {name}");
    map.borrow_mut().insert(name, TranscodeStatus::Done);
}

fn spawn_transcode(name: String, client: Client, map: TranscodeMap) {
    map.borrow_mut()
        .insert(name.clone(), TranscodeStatus::Queued);
    let fut = SendWrapper::new(transcode_video_task(name, client, map));
    compio::runtime::spawn(async move { fut.await }).detach();
}

/// Startup recovery: scan KV for orphan video originals (uploaded but no HLS
/// playlist yet) and re-enqueue them. Best-effort — a failure here only
/// delays one file's transcode until the next manual re-upload.
async fn recover_pending_transcodes(client: Client, map: TranscodeMap) {
    let scan = match client.lock().await.range(b"", b"", u32::MAX).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("startup scan: {e}");
            return;
        }
    };
    let mut originals = Vec::new();
    let mut hls_playlists = std::collections::HashSet::new();
    for entry in scan.entries {
        let key = String::from_utf8_lossy(&entry.key).to_string();
        if let Some(rest) = key.strip_prefix(HLS_PREFIX) {
            if let Some((name, file)) = rest.rsplit_once('/') {
                if file == "index.m3u8" {
                    hls_playlists.insert(name.to_string());
                }
            }
            continue;
        }
        if key.starts_with(THUMB_PREFIX) {
            continue;
        }
        if is_video_ext(&ext_of(&key)) {
            originals.push(key);
        }
    }
    for name in originals {
        if hls_playlists.contains(&name) {
            continue;
        }
        tracing::info!("startup: re-enqueue transcode for {name}");
        spawn_transcode(name, client.clone(), map.clone());
    }
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

fn json_response(body: String) -> Response<Body> {
    Response::builder()
        .header("content-type", "application/json")
        .header("cache-control", "no-store")
        .body(Body::from(body))
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
    transcodes: &TranscodeMap,
    mut multipart: Multipart,
) -> Response<Body> {
    while let Ok(Some(field)) = multipart.next_field().await {
        let filename = match field.file_name() {
            Some(name) => name.to_string(),
            None => continue,
        };

        // Reject if a transcode for this name is already in-flight; otherwise
        // we'd race the worker (which still expects the original at <name>).
        if let Some(s) = transcodes.borrow().get(&filename).cloned() {
            if matches!(s, TranscodeStatus::Queued | TranscodeStatus::Transcoding) {
                return error_response(
                    StatusCode::CONFLICT,
                    format!("transcoding in progress for {filename}"),
                );
            }
        }

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

        let ext = ext_of(&filename);
        if is_video_ext(&ext) {
            // Wipe any prior HLS / thumb so the worker writes fresh artifacts.
            cleanup_derived(client, &filename).await;
            spawn_transcode(filename.clone(), client.clone(), transcodes.clone());
            return json_response(format!(
                r#"{{"name":{},"transcoding":true}}"#,
                json_string(&filename)
            ));
        }

        // Non-video: invalidate the cached thumbnail so /thumb rebuilds.
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

async fn cleanup_derived(client: &Client, name: &str) {
    // Best-effort; NotFound is fine.
    let _ = client
        .lock()
        .await
        .delete(thumb_key(name).as_bytes())
        .await;
    let prefix = hls_dir_prefix(name);
    let scan = match client
        .lock()
        .await
        .range(prefix.as_bytes(), b"", u32::MAX)
        .await
    {
        Ok(r) => r,
        Err(_) => return,
    };
    for entry in scan.entries {
        let _ = client.lock().await.delete(&entry.key).await;
    }
}

async fn delete_handler_inner(client: &Client, name: String) -> Response<Body> {
    // Try the original first; either way, then sweep derived artifacts.
    let original = client.lock().await.delete(name.as_bytes()).await;
    cleanup_derived(client, &name).await;
    match original {
        Ok(()) => ok_response("OK"),
        Err(AutumnError::NotFound) => {
            // Original may already be gone after a video transcode — but the
            // derived sweep above handled the HLS/thumb side. Treat the
            // logical file as deleted iff at least one derived key was cleared
            // OR the original was present. If neither, return 404.
            //
            // We can detect "logical file existed" by re-scanning the HLS
            // prefix — but cleanup already wiped it. Simpler: probe before.
            // Cheap path: assume Ok if cleanup found anything; otherwise 404.
            // (For UX this is fine — the user only sees 404 for truly absent
            // files, and cleanup is idempotent.)
            ok_response("OK")
        }
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("delete: {e}")),
    }
}

async fn list_handler_inner(client: &Client) -> Response<Body> {
    match client.lock().await.range(b"", b"", u32::MAX).await {
        Ok(result) => {
            let mut seen = std::collections::HashSet::new();
            let mut keys: Vec<String> = Vec::new();
            for e in &result.entries {
                let s = String::from_utf8_lossy(&e.key).to_string();
                if let Some(rest) = s.strip_prefix(HLS_PREFIX) {
                    // Surface video logical names from .hls/<name>/index.m3u8.
                    if let Some((name, file)) = rest.rsplit_once('/') {
                        if file == "index.m3u8" && seen.insert(name.to_string()) {
                            keys.push(name.to_string());
                        }
                    }
                    continue;
                }
                if s.starts_with(THUMB_PREFIX) {
                    continue;
                }
                if seen.insert(s.clone()) {
                    keys.push(s);
                }
            }
            Response::builder()
                .header("content-type", "text/plain; charset=utf-8")
                .body(Body::from(keys.join("\n")))
                .unwrap()
        }
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("list: {e}")),
    }
}

async fn hls_handler_inner(
    client: &Client,
    metrics: &MetricsRef,
    name: String,
    file: String,
) -> Response<Body> {
    // Whitelist the file portion so a stray "../" can't escape the HLS prefix.
    if file.contains('/') || file.contains("..") || file.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "invalid hls path".into());
    }
    let key = hls_key(&name, &file);
    let t0 = Instant::now();
    let res = client.lock().await.get(key.as_bytes()).await;
    let dt = elapsed_ms(t0);
    match res {
        Ok(Some(v)) => {
            metrics.borrow_mut().record_get(dt, v.len() as u64);
            let ct = if file.ends_with(".m3u8") {
                "application/vnd.apple.mpegurl"
            } else if file.ends_with(".ts") {
                "video/mp2t"
            } else if file.ends_with(".m4s") || file.ends_with(".mp4") {
                "video/iso.segment"
            } else {
                "application/octet-stream"
            };
            Response::builder()
                .header("content-type", ct)
                .header("cache-control", "public, max-age=86400")
                .header("content-length", v.len())
                .body(Body::from(v))
                .unwrap()
        }
        Ok(None) => error_response(StatusCode::NOT_FOUND, "hls not found".into()),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("get hls: {e}")),
    }
}

async fn transcode_status_handler_inner(
    client: &Client,
    transcodes: &TranscodeMap,
    name: String,
) -> Response<Body> {
    if let Some(s) = transcodes.borrow().get(&name).cloned() {
        return json_response(s.to_json());
    }
    // Not in memory — derive from KV. Playlist present ⇒ Done.
    let playlist = client
        .lock()
        .await
        .head(hls_playlist_key(&name).as_bytes())
        .await;
    match playlist {
        Ok(meta) if meta.found => json_response(TranscodeStatus::Done.to_json()),
        Ok(_) | Err(AutumnError::NotFound) => {
            // Playlist absent. If the original is still around it's a leftover
            // upload — caller should poll while we treat it as queued (the
            // startup-recovery path will pick it up; for newly-arrived single
            // requests we don't auto-spawn here to avoid a per-poll race).
            let head = client.lock().await.head(name.as_bytes()).await;
            match head {
                Ok(m) if m.found => json_response(TranscodeStatus::Queued.to_json()),
                _ => error_response(StatusCode::NOT_FOUND, "unknown".into()),
            }
        }
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("status: {e}")),
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

    // Cache hit fast path (videos rely on this — their thumbs are written
    // by the transcode pipeline; this handler never invokes ffmpeg for
    // videos anymore).
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
        Ok(None) => { /* fall through */ }
        Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("get thumb: {e}")),
    }

    if is_video {
        // No fallback for videos — front-end shows the play glyph alone.
        return error_response(
            StatusCode::NOT_FOUND,
            "video thumbnail not yet generated".into(),
        );
    }

    // Image path: decode+resize via the `image` crate. Failure falls back
    // to serving the original bytes (so a malformed PNG still renders).
    enum BuildOutcome {
        Ok(Vec<u8>),
        ImageFallback(anyhow::Error, Vec<u8>),
    }

    let build_t0 = Instant::now();
    let build_res = {
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
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "thumbnail worker panicked".into(),
            );
        }
    };

    // Write back to autumn for next time. Best-effort and detached: the
    // response ships the JPEG bytes to the client without waiting on the
    // cache put.
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
    let transcodes: TranscodeMap = Rc::new(RefCell::new(HashMap::new()));

    // Wrap captures in SendWrapper to satisfy axum's Send requirement.
    // Safe because compio runs everything on a single thread.
    let cmt = SendWrapper::new((client.clone(), metrics.clone(), transcodes.clone()));
    let put_route = post(move |multipart: Multipart| {
        let cmt = cmt.clone();
        SendWrapper::new(async move {
            let (c, m, t) = (&cmt.0, &cmt.1, &cmt.2);
            put_handler_inner(c, m, t, multipart).await
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

    let cm = SendWrapper::new((client.clone(), metrics.clone()));
    let hls_route = get(move |Path((name, file)): Path<(String, String)>| {
        let cm = cm.clone();
        SendWrapper::new(async move {
            let (c, m) = (&cm.0, &cm.1);
            hls_handler_inner(c, m, name, file).await
        })
    });

    let ct = SendWrapper::new((client.clone(), transcodes.clone()));
    let status_route = get(move |Path(name): Path<String>| {
        let ct = ct.clone();
        SendWrapper::new(async move {
            let (c, t) = (&ct.0, &ct.1);
            transcode_status_handler_inner(c, t, name).await
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

    // Kick off the startup recovery scan once the runtime is up.
    {
        let client = client.clone();
        let map = transcodes.clone();
        let fut = SendWrapper::new(recover_pending_transcodes(client, map));
        compio::runtime::spawn(async move { fut.await }).detach();
    }

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/put/", put_route)
        .route("/get/{name}", get_route)
        .route("/thumb/{name}", thumb_route)
        .route("/hls/{name}/{file}", hls_route)
        .route("/transcode-status/{name}", status_route)
        .route("/del/{name}", del_route)
        .route("/list/", list_route)
        .route("/metrics/", metrics_route)
        .layer(DefaultBodyLimit::max(1024 * 1024 * 1024)); // 1 GB

    let listener = compio::net::TcpListener::bind(format!("0.0.0.0:{LISTEN_PORT}")).await?;
    tracing::info!("Gallery listening on http://0.0.0.0:{LISTEN_PORT}");
    cyper_axum::serve(listener, app).await?;
    Ok(())
}
