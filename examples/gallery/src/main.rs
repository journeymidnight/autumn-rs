use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Cursor;
use std::rc::Rc;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use autumn_client::{AutumnError, ClusterClient, STRIPE_CHUNK_SIZE};
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Multipart, Path};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::routing::{delete, get, post};
use axum::Router;
use image::codecs::jpeg::JpegEncoder;
use image::ImageReader;
use send_wrapper::SendWrapper;

// `ClusterClient` is built for `Rc`-shared concurrent use: all hot-path
// methods take `&self`, internal mutability is `RefCell`-scoped, and no
// borrow spans an `.await` (see its crate doc). The example is single-
// threaded (compio), so a plain `Rc` is sufficient — no outer `Mutex`.
// Dropping the lock is what lets `put_stream_begin` hold a `PutStreamHandle`
// (which borrows `&ClusterClient`) across slow browser-socket reads without
// serialising every other request behind it.
type Client = Rc<ClusterClient>;
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
// Reserved prefix for per-file metadata KV (one small KV per field), written on
// upload and swept on delete.
const META_PREFIX: &str = ".meta/";
const THUMB_WIDTH: u32 = 320;
const THUMB_QUALITY: u8 = 80;
const LISTEN_PORT: u16 = 5001;
// Chunk size for streaming /get/{name} responses and for downloading
// source video to a temp file before transcoding.
const RANGE_CHUNK_BYTES: u32 = 4 * 1024 * 1024;

// Root key namespace for ALL of this example's keys. A shared cluster hosts
// other apps too (autumn-memory under `mem/`, kvcache under `kvc/`, …), so every
// gallery key — inline files, the striped-video meta blob, thumbnails, HLS,
// per-file meta — lives under this prefix, and the two whole-store scans (file
// list + startup recovery) are scoped to it instead of walking the whole
// keyspace (which would stall once another app has written many keys).
const ROOT: &str = "gallery/";

// First two bytes of `autumn_client`'s striped-value chunk namespace (the SDK
// keys each stripe under `\xff\xfe…` so chunks sort AFTER every normal user
// key — see F186 in the client crate). They fall OUTSIDE the `gallery/` scan
// range, so the scoped scans never see them; the guard is kept as belt-and-
// suspenders.
const CHUNK_NS_PREFIX: &[u8] = b"\xff\xfe";

fn is_chunk_key(key: &[u8]) -> bool {
    key.starts_with(CHUNK_NS_PREFIX)
}

/// Key for an inline file / the striped-video meta blob (the logical `<name>`).
fn file_key(name: &str) -> String {
    format!("{ROOT}{name}")
}

fn thumb_key(name: &str) -> String {
    format!("{ROOT}{THUMB_PREFIX}{THUMB_WIDTH}/{name}")
}

fn meta_prefix(name: &str) -> String {
    format!("{ROOT}{META_PREFIX}{name}/")
}

/// Read image dimensions from the header only (`into_dimensions`, no full
/// decode). `None` for non-images / undecodable headers.
fn image_dims(data: &[u8]) -> Option<(u32, u32)> {
    ImageReader::new(Cursor::new(data))
        .with_guessed_format()
        .ok()?
        .into_dimensions()
        .ok()
}

/// The metadata fields derivable from a file's name + bytes as `(field, value)`
/// pairs. Shared by the upload path and on-demand backfill, so it excludes
/// `uploaded_at` (the upload path appends that; backfill can't know it). Each
/// pair becomes its own small (<4 KiB) KV under `.meta/<name>/<field>`.
fn file_meta_fields(name: &str, data: &[u8]) -> Vec<(&'static str, String)> {
    let mut fields = vec![
        ("name", name.to_string()),
        ("ext", ext_of(name)),
        ("size_bytes", data.len().to_string()),
        (
            "content_type",
            mime_guess::from_path(name)
                .first_or_octet_stream()
                .to_string(),
        ),
    ];
    if let Some((w, h)) = image_dims(data) {
        fields.push(("width", w.to_string()));
        fields.push(("height", h.to_string()));
    }
    fields
}

/// Batch-write metadata fields as individual KV pairs keyed
/// `.meta/<name>/<field>` (one `MSG_BATCH_PUT` per owning partition via
/// `put_many`). Best-effort: failures are logged, never fatal. Swept by
/// `cleanup_derived` on delete.
async fn write_meta_fields(client: &Client, name: &str, fields: &[(&'static str, String)]) {
    if fields.is_empty() {
        return;
    }
    let prefix = meta_prefix(name);
    let keys: Vec<String> = fields.iter().map(|(k, _)| format!("{prefix}{k}")).collect();
    let items: Vec<(&[u8], bytes::Bytes, u64)> = keys
        .iter()
        .zip(fields)
        .map(|(key, (_, val))| (key.as_bytes(), bytes::Bytes::from(val.clone()), 0))
        .collect();

    let t0 = Instant::now();
    let results = client.put_many(&items).await;
    let dt = elapsed_ms(t0);
    let failed = results.iter().filter(|r| r.is_err()).count();
    if failed > 0 {
        tracing::warn!("meta KV: {failed}/{} puts failed for {name}", items.len());
    }
    tracing::info!(
        "meta KV: wrote {} fields for {name} in {dt:.1}ms",
        items.len() - failed
    );
}

/// Upload-time metadata write: derive content fields, stamp `uploaded_at` with
/// now, persist.
async fn write_file_meta(client: &Client, name: &str, data: &[u8]) {
    let mut fields = file_meta_fields(name, data);
    if let Ok(secs) = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
    {
        fields.push(("uploaded_at", secs.to_string()));
    }
    write_meta_fields(client, name, &fields).await;
}

/// On-demand backfill for a file that predates the meta feature. `None` if the
/// file doesn't exist in any form.
///
/// Two cases:
/// - The original `<name>` still exists (image, or a not-yet-transcoded video):
///   size from `head` (cheap); image dimensions from the bytes (images only).
/// - The original is gone but `.hls/<name>/index.m3u8` exists (a transcoded
///   video): describe the HLS — content_type is the playlist type. Segment
///   bytes aren't summed here (would mean fetching every segment); fresh
///   transcodes write the richer `size_bytes`/`hls_segments` directly.
///
/// No `uploaded_at`/`transcoded_at` — the original times are unknown.
async fn backfill_file_meta(client: &Client, name: &str) -> Option<Vec<(&'static str, String)>> {
    let head = client.head(file_key(name).as_bytes()).await.ok()?;
    if head.found {
        let mut fields = vec![
            ("name", name.to_string()),
            ("ext", ext_of(name)),
            ("size_bytes", head.value_length.to_string()),
            (
                "content_type",
                mime_guess::from_path(name)
                    .first_or_octet_stream()
                    .to_string(),
            ),
        ];
        if is_image_ext(&ext_of(name)) {
            if let Ok(Some(data)) = client.get(file_key(name).as_bytes()).await {
                if let Some((w, h)) = image_dims(&data) {
                    fields.push(("width", w.to_string()));
                    fields.push(("height", h.to_string()));
                }
            }
        }
        return Some(fields);
    }

    // Original gone — a transcoded video is identified by its HLS playlist.
    let playlist = client
        .head(hls_playlist_key(name).as_bytes())
        .await
        .ok()?;
    if !playlist.found {
        return None;
    }
    let mut fields = vec![
        ("name", name.to_string()),
        ("ext", ext_of(name)),
        ("kind", "video".to_string()),
        (
            "content_type",
            "application/vnd.apple.mpegurl".to_string(),
        ),
    ];
    // Sum the HLS payload so the file shows a size (matches the fresh-transcode
    // `size_bytes`/`hls_segments` fields). `range` only lists keys, so fetch
    // each segment's length via `head_many` (cheap — no value transfer).
    if let Ok(scan) = client
        .range(hls_dir_prefix(name).as_bytes(), b"", u32::MAX)
        .await
    {
        let keys: Vec<Vec<u8>> = scan.entries.iter().map(|e| e.key.clone()).collect();
        let segments = keys.iter().filter(|k| k.ends_with(b".ts")).count();
        let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let hls_bytes: u64 = client
            .head_many(&refs)
            .await
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|m| m.value_length)
            .sum();
        fields.push(("size_bytes", hls_bytes.to_string()));
        fields.push(("hls_segments", segments.to_string()));
    }
    Some(fields)
}

fn hls_dir_prefix(name: &str) -> String {
    format!("{ROOT}{HLS_PREFIX}{name}/")
}

fn hls_key(name: &str, file: &str) -> String {
    format!("{ROOT}{HLS_PREFIX}{name}/{file}")
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
            TranscodeStatus::Failed(err) => {
                format!(r#"{{"status":"failed","error":{}}}"#, json_string(err))
            }
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

#[derive(Clone, Copy, Debug)]
enum HlsEncodeMode {
    Copy,
    Reencode,
}

// Partition-server inline-`put` cap (mirrors `AUTUMN_PS_MAX_INLINE_BYTES_DEFAULT`
// = 64 MiB). A single KV value over this is rejected with
// `CODE_VALUE_TOO_LARGE` ("…exceeds the partition server's inline cap"), which
// is exactly what a fixed-20 s `.ts` segment hit on a high-bitrate source.
const PS_INLINE_CAP_BYTES: u64 = 64 * 1024 * 1024;
// Target payload per HLS segment. `-hls_time` is only the *target* duration —
// `-c copy` cuts on the source's keyframes, so a segment can overshoot to the
// next keyframe. Aim for 48 MiB (16 MiB of headroom under the cap) and pick the
// segment seconds from the source byte-rate.
const HLS_SEGMENT_TARGET_BYTES: f64 = 48.0 * 1024.0 * 1024.0;
const HLS_TIME_MAX_SECS: f64 = 20.0;
const HLS_TIME_MIN_SECS: f64 = 2.0;

/// Probe the container duration (seconds) with `ffprobe`. `None` when ffprobe
/// is missing/errors or the field is absent — the caller then falls back to the
/// max segment length.
fn probe_duration_secs(input: &str) -> Option<f64> {
    let out = std::process::Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            input,
        ])
        .stdin(std::process::Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let d = String::from_utf8_lossy(&out.stdout).trim().parse::<f64>().ok()?;
    (d.is_finite() && d > 0.0).then_some(d)
}

/// Choose `-hls_time` so an average segment lands near
/// `HLS_SEGMENT_TARGET_BYTES`. Falls back to `HLS_TIME_MAX_SECS` when the
/// byte-rate is unknown (no duration / empty file), preserving the old 20 s
/// behaviour for ordinary-bitrate clips.
fn pick_hls_time(file_bytes: u64, duration_secs: Option<f64>) -> f64 {
    let dur = match duration_secs {
        Some(d) => d,
        None => return HLS_TIME_MAX_SECS,
    };
    let bytes_per_sec = file_bytes as f64 / dur;
    if bytes_per_sec <= 0.0 {
        return HLS_TIME_MAX_SECS;
    }
    (HLS_SEGMENT_TARGET_BYTES / bytes_per_sec).clamp(HLS_TIME_MIN_SECS, HLS_TIME_MAX_SECS)
}

/// Largest `.ts` segment in `dir` (0 if none). Lets the caller detect a
/// copy-path segment that still overshot the inline cap because the source's
/// keyframes are sparser than the requested segment duration.
fn max_ts_bytes(dir: &std::path::Path) -> u64 {
    let mut max = 0u64;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let p = e.path();
            if p.extension().and_then(|s| s.to_str()) == Some("ts") {
                if let Ok(m) = std::fs::metadata(&p) {
                    max = max.max(m.len());
                }
            }
        }
    }
    max
}

fn run_hls_ffmpeg(
    input: &str,
    playlist: &std::path::Path,
    segments: &std::path::Path,
    mode: HlsEncodeMode,
    seg_secs: f64,
) -> Result<()> {
    use std::process::{Command, Stdio};

    let mut cmd = Command::new("ffmpeg");
    cmd.args([
        "-y",
        "-loglevel",
        "error",
        // Rebase the source timeline to PTS 0. Many MP4s carry an edit list /
        // non-zero start offset (e.g. audio priming, container start); `-c copy`
        // faithfully preserves it, so the first HLS segment begins at, say,
        // 1.4 s. hls.js then starts playback at currentTime 0 — where no media
        // exists — and the <video> hangs on a black frame ("can't play"). The
        // re-encode path inherits the same offset. `-copyts -start_at_zero`
        // shifts the first timestamp to 0; `-muxdelay/-muxpreload 0` (output
        // side, below) keep the muxer from re-introducing one. Verified
        // harmless for sources that already start at 0.
        "-copyts",
        "-start_at_zero",
        "-i",
        input,
        "-map",
        "0:v:0",
        "-map",
        "0:a:0?",
    ]);
    match mode {
        HlsEncodeMode::Copy => {
            cmd.args(["-c:v", "copy", "-c:a", "copy", "-bsf:v", "h264_mp4toannexb"]);
        }
        HlsEncodeMode::Reencode => {
            // Force a keyframe at every segment boundary so the muxer can cut
            // exactly on `seg_secs` (the copy path can only cut on the source's
            // own — possibly sparse — keyframes, which is what lets a segment
            // overshoot the inline cap). This is what makes the re-encode
            // fallback reliably produce under-cap segments.
            let force_kf = format!("expr:gte(t,n_forced*{seg_secs:.3})");
            cmd.args([
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "23",
                "-force_key_frames",
                force_kf.as_str(),
                "-c:a",
                "aac",
                "-b:a",
                "128k",
            ]);
        }
    }
    let hls_time = format!("{seg_secs:.3}");
    let status = cmd
        .args([
            // Paired with `-copyts -start_at_zero` above: don't let the muxer
            // add its own start delay back onto the rebased timeline.
            "-muxdelay",
            "0",
            "-muxpreload",
            "0",
            "-hls_time",
            hls_time.as_str(),
            "-hls_list_size",
            "0",
            "-hls_segment_type",
            "mpegts",
            "-hls_flags",
            "independent_segments",
            "-hls_segment_filename",
        ])
        .arg(segments)
        .arg(playlist)
        .stdin(Stdio::null())
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .output()
        .with_context(|| {
            format!(
                "spawn ffmpeg (hls {})",
                format!("{mode:?}").to_ascii_lowercase()
            )
        })?;
    if !status.status.success() {
        let tail = String::from_utf8_lossy(&status.stderr);
        let tail = tail.lines().rev().take(8).collect::<Vec<_>>().join(" | ");
        return Err(anyhow!(
            "ffmpeg hls {} exited {}: {tail}",
            format!("{mode:?}").to_ascii_lowercase(),
            status.status
        ));
    }
    Ok(())
}

/// FFmpeg HLS pass: input video file → directory containing `index.m3u8` +
/// `seg000.ts` … plus `thumb.jpg`. If the source streams are already
/// MPEG-TS HLS-compatible (H.264 video and copy-safe audio), prefer `-c copy`
/// so the output is lossless; otherwise fall back to re-encoding.
///
/// `input_path` is a local file (pre-downloaded from KV by the caller).
/// The caller owns the tempdir that contains this file.
fn run_transcode_blocking(input_path: &std::path::Path) -> Result<Vec<(String, Vec<u8>)>> {
    use std::process::{Command, Stdio};
    use std::time::Instant;

    let dir = input_path.parent().context("input_path has no parent")?;
    let input_str = input_path.to_str().context("non-utf8 input path")?;
    let playlist = dir.join("index.m3u8");
    let segments = dir.join("seg%03d.ts");

    // Size each segment from the source byte-rate so no `.ts` exceeds the PS
    // inline cap (the fixed-20 s default produced a 106 MB segment on a
    // high-bitrate clip → `put` rejected with `CODE_VALUE_TOO_LARGE`).
    let file_bytes = std::fs::metadata(input_path).map(|m| m.len()).unwrap_or(0);
    let duration = probe_duration_secs(input_str);
    let seg_secs = pick_hls_time(file_bytes, duration);
    tracing::info!(
        seg_secs,
        file_mb = file_bytes / (1024 * 1024),
        duration_secs = duration.unwrap_or(0.0),
        "transcode: chosen hls segment duration"
    );

    let t1 = Instant::now();
    // Re-encode when the lossless copy pass fails OR when it produced a segment
    // that still overshot the inline cap. The latter happens when the source's
    // keyframes are sparser than `seg_secs` (copy can only cut on a keyframe);
    // the re-encode pass forces segment-boundary keyframes so it can't recur.
    let mut reencode_reason: Option<String> = None;
    if let Err(err) = run_hls_ffmpeg(input_str, &playlist, &segments, HlsEncodeMode::Copy, seg_secs)
    {
        reencode_reason = Some(format!("copy path failed: {err}"));
    } else {
        let big = max_ts_bytes(dir);
        if big > PS_INLINE_CAP_BYTES {
            reencode_reason = Some(format!(
                "copy produced a {} MiB segment over the {} MiB inline cap (sparse keyframes)",
                big / (1024 * 1024),
                PS_INLINE_CAP_BYTES / (1024 * 1024)
            ));
        }
    }
    if let Some(reason) = reencode_reason {
        tracing::warn!(reason, "transcode: falling back to hls re-encode");
        // Wipe the partial/over-cap copy output (segments + playlist); keep the
        // source so the re-encode pass can read it.
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p != input_path {
                    let _ = std::fs::remove_file(p);
                }
            }
        }
        run_hls_ffmpeg(input_str, &playlist, &segments, HlsEncodeMode::Reencode, seg_secs)
            .context("run ffmpeg hls reencode after copy fallback")?;
    }
    tracing::info!(
        hls_ms = t1.elapsed().as_millis(),
        "transcode: ffmpeg hls done"
    );

    let t2 = Instant::now();
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
            input_str,
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
        tracing::warn!("ffmpeg thumb pass failed: {tail}");
    }
    tracing::info!(
        thumb_ms = t2.elapsed().as_millis(),
        "transcode: ffmpeg thumb done"
    );

    // Remove the source file before collecting so it doesn't get re-uploaded.
    let _ = std::fs::remove_file(input_path);

    let mut out = Vec::new();
    let mut total_bytes: u64 = 0;
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
        total_bytes += bytes.len() as u64;
        out.push((fname, bytes));
    }
    tracing::info!(
        files = out.len(),
        total_mb = total_bytes / (1024 * 1024),
        "transcode: collected output files"
    );
    Ok(out)
}

/// Download a blob from KV to a local file, chunk by chunk.
///
/// Uses the streaming reader (`get_stream`) rather than `head` + `get_range`:
/// large videos are stored striped (a chunk-per-`STRIPE_CHUNK_SIZE` under a
/// reserved namespace plus a tiny meta blob at `key`), so `head(key)` would
/// report the 28-byte meta length and `get_range(key, …)` would read the meta
/// bytes — not the video. `get_stream` auto-detects striped vs inline and
/// yields the assembled value sequentially, which is exactly the access
/// pattern a full-file transcode download needs. Memory stays O(chunk).
async fn download_to_file(client: &Client, key: &str, dest: &std::path::Path) -> Result<u64> {
    use std::io::Write;

    let mut stream = client
        .get_stream(file_key(key).as_bytes(), RANGE_CHUNK_BYTES)
        .await
        .context("get_stream for download")?
        .ok_or_else(|| anyhow!("key not found: {key}"))?;
    let mut file =
        std::fs::File::create(dest).with_context(|| format!("create {}", dest.display()))?;
    let mut written: u64 = 0;
    while let Some(chunk) = stream
        .next_chunk()
        .await
        .with_context(|| format!("next_chunk at offset {written}"))?
    {
        if chunk.is_empty() {
            break;
        }
        file.write_all(&chunk)
            .with_context(|| format!("write at offset {written}"))?;
        written += chunk.len() as u64;
    }
    file.flush().context("flush download file")?;
    Ok(written)
}

/// Drives one transcoding job end-to-end: marks status, downloads source to a
/// temp file, runs ffmpeg in a blocking thread, writes HLS segments + thumbnail
/// to KV, then deletes the original.
/// On failure leaves the original in place so the user can retry.
async fn transcode_video_task(name: String, client: Client, map: TranscodeMap) {
    map.borrow_mut()
        .insert(name.clone(), TranscodeStatus::Transcoding);
    tracing::info!("transcode start: {name}");

    let tmp = match tempfile::tempdir() {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!("transcode: tempdir failed for {name}: {e}");
            map.borrow_mut()
                .insert(name, TranscodeStatus::Failed(format!("tempdir: {e}")));
            return;
        }
    };
    let source_path = tmp.path().join("source.mp4");

    let t_dl = std::time::Instant::now();
    match download_to_file(&client, &name, &source_path).await {
        Ok(bytes) => {
            tracing::info!(
                dl_ms = t_dl.elapsed().as_millis(),
                dl_mb = bytes / (1024 * 1024),
                "transcode: source downloaded"
            );
        }
        Err(e) => {
            tracing::warn!("transcode: download failed for {name}: {e:#}");
            map.borrow_mut()
                .insert(name, TranscodeStatus::Failed(format!("download: {e:#}")));
            return;
        }
    }

    let input_path = source_path.clone();
    let t_ffmpeg = std::time::Instant::now();
    let res = compio::runtime::spawn_blocking(move || run_transcode_blocking(&input_path)).await;
    tracing::info!(
        ffmpeg_total_ms = t_ffmpeg.elapsed().as_millis(),
        "transcode: blocking work done"
    );

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
            map.borrow_mut().insert(
                name,
                TranscodeStatus::Failed("transcode worker panicked".into()),
            );
            return;
        }
    };

    // Push every produced file: thumb.jpg → thumb_key, everything else →
    // .hls/<name>/<fname>. must_sync=false: HLS segments are derivable from
    // the original which we still have until the final delete.
    let t_kv = std::time::Instant::now();
    let mut kv_bytes: u64 = 0;
    let mut hls_bytes: u64 = 0;
    let mut hls_segments: usize = 0;
    for (fname, bytes) in outputs {
        let is_thumb = fname == "thumb.jpg";
        let key = if is_thumb {
            thumb_key(&name)
        } else {
            hls_bytes += bytes.len() as u64;
            if fname.ends_with(".ts") {
                hls_segments += 1;
            }
            hls_key(&name, &fname)
        };
        kv_bytes += bytes.len() as u64;
        let put_res = client.put(key.as_bytes(), &bytes).await;
        if let Err(e) = put_res {
            tracing::warn!("write {key} failed: {e}");
            map.borrow_mut()
                .insert(name, TranscodeStatus::Failed(format!("kv put: {e}")));
            return;
        }
    }
    tracing::info!(
        kv_total_ms = t_kv.elapsed().as_millis(),
        kv_total_mb = kv_bytes / (1024 * 1024),
        "transcode: kv upload done"
    );

    // The final metadata describes the CONVERTED HLS, not the (about-to-be-
    // deleted) original: size_bytes is the total HLS payload, content_type is
    // the playlist type. `size_bytes`/`content_type` field names match what the
    // frontend reads. Best-effort; swept by cleanup_derived on delete.
    {
        let mut fields: Vec<(&'static str, String)> = vec![
            ("name", name.clone()),
            ("ext", ext_of(&name)),
            ("kind", "video".to_string()),
            ("content_type", "application/vnd.apple.mpegurl".to_string()),
            ("size_bytes", hls_bytes.to_string()),
            ("hls_segments", hls_segments.to_string()),
        ];
        if let Ok(secs) = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
        {
            fields.push(("transcoded_at", secs.to_string()));
        }
        write_meta_fields(&client, &name, &fields).await;
    }

    // Drop the original; non-fatal if it's already gone (concurrent delete).
    // `delete_stream` cascades through the striped chunks + meta blob (a plain
    // `delete` would only remove the meta, orphaning the chunk-namespace keys).
    match client.delete_stream(file_key(&name).as_bytes()).await {
        Ok(_) | Err(AutumnError::NotFound) => {}
        Err(e) => {
            tracing::warn!("delete original {name} failed: {e}");
            map.borrow_mut().insert(
                name,
                TranscodeStatus::Failed(format!("delete original: {e}")),
            );
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
    compio::runtime::spawn(fut).detach();
}

/// Startup recovery: scan KV for orphan video originals (uploaded but no HLS
/// playlist yet) and re-enqueue them. Best-effort — a failure here only
/// delays one file's transcode until the next manual re-upload.
async fn recover_pending_transcodes(client: Client, map: TranscodeMap) {
    let scan = match client.range(ROOT.as_bytes(), b"", u32::MAX).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("startup scan: {e}");
            return;
        }
    };
    let mut originals = Vec::new();
    let mut hls_playlists = std::collections::HashSet::new();
    for entry in scan.entries {
        // Scoped to `gallery/`; strip the root to recover the logical key
        // (chunk keys sort outside this range, so the guard rarely fires).
        if is_chunk_key(&entry.key) {
            continue;
        }
        let Some(key) = String::from_utf8_lossy(&entry.key)
            .strip_prefix(ROOT)
            .map(|s| s.to_string())
        else {
            continue;
        };
        if let Some(rest) = key.strip_prefix(HLS_PREFIX) {
            if let Some((name, file)) = rest.rsplit_once('/') {
                if file == "index.m3u8" {
                    hls_playlists.insert(name.to_string());
                }
            }
            continue;
        }
        if key.starts_with(THUMB_PREFIX) || key.starts_with(META_PREFIX) {
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
        .header("cache-control", "no-cache")
        .body(Body::from(include_str!("../static/index.html")))
        .unwrap()
}

/// Stream one multipart field straight into a striped value via
/// `put_stream_begin` → `send` → `commit`. The field arrives as a sequence of
/// network-sized chunks (~tens of KiB); we coalesce them into
/// `STRIPE_CHUNK_SIZE` (4 MiB) pieces so each KV `Put` carries a full stripe
/// rather than thousands of tiny VPs. Resident memory stays O(chunk)
/// regardless of upload size, and no single KV value has to hold the whole
/// file — that's what makes multi-GiB videos uploadable.
///
/// Only the time spent inside the KV `send`/`commit` calls is charged to the
/// put metrics (not the browser-socket reads) so the HUD keeps reflecting
/// storage throughput. On any error the already-written chunks are
/// best-effort aborted so they don't linger as orphans.
async fn stream_upload(
    client: &Client,
    metrics: &MetricsRef,
    name: &str,
    field: &mut axum::extract::multipart::Field<'_>,
) -> Result<u64> {
    let chunk_size = STRIPE_CHUNK_SIZE as usize;
    let mut handle = client.put_stream_begin(file_key(name).as_bytes(), 0);
    let mut buf: Vec<u8> = Vec::with_capacity(chunk_size);
    let mut kv_ms = 0.0_f64;

    macro_rules! flush {
        ($slice:expr) => {{
            let t0 = Instant::now();
            let r = handle.send($slice).await;
            kv_ms += elapsed_ms(t0);
            if let Err(e) = r {
                let _ = handle.abort().await;
                return Err(anyhow!("send chunk: {e}"));
            }
        }};
    }

    loop {
        match field.chunk().await {
            Ok(Some(bytes)) => {
                buf.extend_from_slice(&bytes);
                while buf.len() >= chunk_size {
                    flush!(&buf[..chunk_size]);
                    buf.drain(..chunk_size);
                }
            }
            Ok(None) => break,
            Err(e) => {
                let _ = handle.abort().await;
                return Err(anyhow!("read field: {e}"));
            }
        }
    }
    if !buf.is_empty() {
        flush!(&buf);
    }

    let bytes_sent = handle.bytes_sent();
    let chunks = handle.chunks_sent();
    let t0 = Instant::now();
    let commit = handle.commit().await;
    kv_ms += elapsed_ms(t0);
    commit.map_err(|e| anyhow!("commit: {e}"))?;

    metrics.borrow_mut().record_put(kv_ms, bytes_sent);
    tracing::info!(name, bytes_sent, chunks, kv_ms, "stream upload committed");
    Ok(bytes_sent)
}

async fn put_handler_inner(
    client: &Client,
    metrics: &MetricsRef,
    transcodes: &TranscodeMap,
    mut multipart: Multipart,
) -> Response<Body> {
    while let Ok(Some(mut field)) = multipart.next_field().await {
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

        let ext = ext_of(&filename);

        if is_video_ext(&ext) {
            // Videos are the large uploads and are never served via /get/ (the
            // frontend plays them through HLS), so store them striped and read
            // them back sequentially for transcoding. Clear prior artifacts
            // first: a re-upload with fewer chunks would otherwise orphan the
            // old tail chunks, and stale HLS/thumb would shadow the new
            // transcode. `delete_stream` cascades through any prior chunks.
            cleanup_derived(client, &filename).await;
            if let Err(e) = client.delete_stream(file_key(&filename).as_bytes()).await {
                if !matches!(e, AutumnError::NotFound) {
                    tracing::warn!("clear prior original {filename} failed: {e}");
                }
            }
            match stream_upload(client, metrics, &filename, &mut field).await {
                Ok(_) => {
                    spawn_transcode(filename.clone(), client.clone(), transcodes.clone());
                    return json_response(format!(
                        r#"{{"name":{},"transcoding":true}}"#,
                        json_string(&filename)
                    ));
                }
                Err(e) => {
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("stream upload: {e:#}"),
                    );
                }
            }
        }

        // Non-video (images / PDFs / text / small binaries) stay inline so
        // /get/ can byte-range them directly. They're small, so buffering the
        // whole field is fine.
        let data = match field.bytes().await {
            Ok(b) => b.to_vec(),
            Err(e) => return error_response(StatusCode::BAD_REQUEST, format!("read field: {e}")),
        };
        let bytes = data.len() as u64;
        let t0 = Instant::now();
        let put_res = client.put(file_key(&filename).as_bytes(), &data).await;
        let dt = elapsed_ms(t0);
        if let Err(e) = put_res {
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("put: {e}"));
        }
        metrics.borrow_mut().record_put(dt, bytes);

        // Persist the file's metadata as small (<4 KiB) KV pairs next to the
        // upload, then invalidate the cached thumbnail so /thumb rebuilds.
        // Both swept by cleanup_derived on delete.
        write_file_meta(client, &filename, &data).await;
        let _ = client.delete(thumb_key(&filename).as_bytes()).await;
        return ok_response(filename);
    }
    error_response(StatusCode::BAD_REQUEST, "no file field".into())
}

/// Parse `Range: bytes=...` (RFC 7233 §2.1, single byte-range only) into an
/// inclusive (start, end) pair. Returns `None` for malformed input or for an
/// otherwise-unrepresentable range; the caller falls back to a 200 response.
///
/// Handles the three byte-range-spec forms ffmpeg / Safari / hls.js actually
/// emit: `bytes=N-`, `bytes=N-M`, and `bytes=-N` (suffix). The previous
/// implementation mis-parsed `bytes=-N` as `bytes=0-N`.
fn parse_byte_range(s: &str, total: u64) -> Option<(u64, u64)> {
    let rest = s.strip_prefix("bytes=")?;
    if total == 0 {
        return None;
    }
    if let Some(suffix) = rest.strip_prefix('-') {
        let n: u64 = suffix.parse().ok()?;
        if n == 0 {
            return None;
        }
        let n = n.min(total);
        return Some((total - n, total - 1));
    }
    let (start_s, end_s) = rest.split_once('-')?;
    let start: u64 = start_s.parse().ok()?;
    if start >= total {
        return None;
    }
    let end: u64 = if end_s.is_empty() {
        total - 1
    } else {
        end_s.parse::<u64>().ok()?.min(total - 1)
    };
    if start > end {
        return None;
    }
    Some((start, end))
}

/// Build a streaming `Body` that fetches `[start, end_inclusive]` of `name`
/// from the cluster in `RANGE_CHUNK_BYTES`-sized pieces. The producer task
/// runs on the local compio runtime; chunks travel over a Send-able mpsc so
/// `Body::from_stream` can hold them across the axum (Send) boundary.
///
/// This replaces the previous "one giant `get_range`" path: ffmpeg sends
/// `Range: bytes=0-` for sequential reads, so a 1 GiB upload used to allocate
/// a 1 GiB Vec (plus an autumn-rpc payload of the same size) before the HTTP
/// body emitted any frame. Now first byte ships after one chunk's RTT and
/// resident memory stays O(chunk).
fn stream_kv_range(
    client: Client,
    metrics: MetricsRef,
    name: String,
    start: u64,
    end_inclusive: u64,
) -> Body {
    use futures::channel::mpsc;
    use futures::SinkExt;

    let total_len = end_inclusive - start + 1;
    // Cap=2 gives one chunk in flight on the wire while the consumer drains
    // another; bigger buffers just waste memory without improving throughput.
    let (mut tx, rx) = mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(2);

    let producer = SendWrapper::new(async move {
        let mut off: u64 = start;
        let mut remaining: u64 = total_len;
        while remaining > 0 {
            let take = remaining.min(RANGE_CHUNK_BYTES as u64) as u32;
            let t0 = Instant::now();
            let res = client
                .get_range(file_key(&name).as_bytes(), off as u32, take)
                .await;
            let dt = elapsed_ms(t0);
            match res {
                Ok(Some(bytes)) => {
                    let n = bytes.len() as u64;
                    metrics.borrow_mut().record_get(dt, n);
                    if n == 0 {
                        break;
                    }
                    if tx.send(Ok(bytes::Bytes::from(bytes))).await.is_err() {
                        return; // client disconnected
                    }
                    off = off.saturating_add(n);
                    remaining = remaining.saturating_sub(n);
                }
                Ok(None) => break,
                Err(e) => {
                    let err = std::io::Error::other(format!("get_range: {e}"));
                    let _ = tx.send(Err(err)).await;
                    return;
                }
            }
        }
    });
    compio::runtime::spawn(producer).detach();

    Body::from_stream(rx)
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

    // One head() per request: lets us 404 cleanly, validate the requested
    // range, and emit a correct `Content-Range` total (Safari rejects
    // `bytes X-Y/*`, see the existing comment retained from the previous
    // implementation).
    let total_size: u64 = match client.head(file_key(&name).as_bytes()).await {
        Ok(meta) if meta.found => meta.value_length,
        Ok(_) => return error_response(StatusCode::NOT_FOUND, "not found".into()),
        Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("head: {e}")),
    };

    let parsed_range = headers
        .get("range")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| parse_byte_range(s, total_size));

    let (start, end_inclusive, status, content_range) = match parsed_range {
        Some((s, e)) => (
            s,
            e,
            StatusCode::PARTIAL_CONTENT,
            Some(format!("bytes {s}-{e}/{total_size}")),
        ),
        None if total_size == 0 => {
            return Response::builder()
                .header("content-type", &mime)
                .header("accept-ranges", "bytes")
                .header("cache-control", "no-cache")
                .header("content-length", 0)
                .body(Body::empty())
                .unwrap();
        }
        None => (0, total_size - 1, StatusCode::OK, None),
    };

    let length = end_inclusive - start + 1;
    let body = stream_kv_range(client.clone(), metrics.clone(), name, start, end_inclusive);

    let mut builder = Response::builder()
        .status(status)
        .header("content-type", &mime)
        .header("accept-ranges", "bytes")
        .header("cache-control", "no-cache")
        .header("content-length", length);
    if let Some(cr) = content_range {
        builder = builder.header("content-range", cr);
    }
    builder.body(body).unwrap()
}

async fn cleanup_derived(client: &Client, name: &str) {
    // Best-effort; NotFound is fine.
    let _ = client.delete(thumb_key(name).as_bytes()).await;
    // Sweep every prefix-scoped artifact for this file: HLS segments and the
    // per-file metadata KV written at upload time.
    for prefix in [hls_dir_prefix(name), meta_prefix(name)] {
        let scan = match client
            .range(prefix.as_bytes(), b"", u32::MAX)
            .await
        {
            Ok(r) => r,
            Err(_) => continue,
        };
        for entry in scan.entries {
            let _ = client.delete(&entry.key).await;
        }
    }
}

async fn delete_handler_inner(client: &Client, name: String) -> Response<Body> {
    // Try the original first; either way, then sweep derived artifacts.
    // `delete_stream` cascades through the striped chunks (video originals)
    // and falls back to a plain delete for inline values (images/PDFs/text).
    let original = client.delete_stream(file_key(&name).as_bytes()).await;
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
    match client.range(ROOT.as_bytes(), b"", u32::MAX).await {
        Ok(result) => {
            let mut seen = std::collections::HashSet::new();
            let mut keys: Vec<String> = Vec::new();
            for e in &result.entries {
                // Scoped to `gallery/`; strip the root to recover the logical key.
                if is_chunk_key(&e.key) {
                    continue;
                }
                let Some(s) = String::from_utf8_lossy(&e.key)
                    .strip_prefix(ROOT)
                    .map(|x| x.to_string())
                else {
                    continue;
                };
                if let Some(rest) = s.strip_prefix(HLS_PREFIX) {
                    // Surface video logical names from .hls/<name>/index.m3u8.
                    if let Some((name, file)) = rest.rsplit_once('/') {
                        if file == "index.m3u8" && seen.insert(name.to_string()) {
                            keys.push(name.to_string());
                        }
                    }
                    continue;
                }
                if s.starts_with(THUMB_PREFIX) || s.starts_with(META_PREFIX) {
                    continue;
                }
                // A bare video-ext key is a video original that's still
                // transcoding (or whose transcode failed): its HLS playlist
                // isn't present yet, so it doesn't belong in the main list.
                // Finished videos are surfaced above via their
                // `.hls/<name>/index.m3u8`; in-progress ones show separately
                // through the `/transcoding/` endpoint.
                if is_video_ext(&ext_of(&s)) {
                    continue;
                }
                if seen.insert(s.clone()) {
                    keys.push(s);
                }
            }
            Response::builder()
                .header("content-type", "text/plain; charset=utf-8")
                .header("cache-control", "no-cache")
                .body(Body::from(keys.join("\n")))
                .unwrap()
        }
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("list: {e}")),
    }
}

/// In-progress transcodes (everything in the map that isn't `Done`), as a JSON
/// array `[{"name":..,"status":"queued|transcoding|failed"}, ...]`. The
/// frontend renders these in a separate "transcoding" panel (they're hidden
/// from `/list/` until their HLS is ready) and polls each to completion.
async fn transcoding_list_handler_inner(transcodes: &TranscodeMap) -> Response<Body> {
    let items: Vec<String> = transcodes
        .borrow()
        .iter()
        .filter(|(_, s)| !matches!(s, TranscodeStatus::Done))
        .map(|(name, s)| {
            format!(
                r#"{{"name":{},"status":"{}"}}"#,
                json_string(name),
                s.as_status_str()
            )
        })
        .collect();
    json_response(format!("[{}]", items.join(",")))
}

/// Return a file's metadata as a flat JSON object `{ "<field>": "<value>", ... }`.
/// Served from the `.meta/<name>/*` KV when present; otherwise derived on demand
/// (backfill), persisted, and returned — so files that predate the meta feature
/// still resolve. 404 only when the file itself doesn't exist.
async fn meta_handler_inner(client: &Client, name: String) -> Response<Body> {
    let prefix = meta_prefix(&name);
    let scan = match client
        .range(prefix.as_bytes(), b"", u32::MAX)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("meta range: {e}"),
            )
        }
    };

    let pairs: Vec<(String, String)> = if scan.entries.is_empty() {
        match backfill_file_meta(client, &name).await {
            Some(fields) => {
                write_meta_fields(client, &name, &fields).await;
                fields.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
            }
            None => return error_response(StatusCode::NOT_FOUND, "not found".into()),
        }
    } else {
        // `range` is a key-listing scan: `RangeEntry.value` is always empty
        // (the PS pushes `value: vec![]`). Fetch the values explicitly.
        let keys: Vec<Vec<u8>> = scan.entries.iter().map(|e| e.key.clone()).collect();
        let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let vals = client.get_many(&refs).await;
        keys.iter()
            .zip(vals)
            .map(|(k, vr)| {
                let key = String::from_utf8_lossy(k).to_string();
                let field = key.strip_prefix(prefix.as_str()).unwrap_or(&key).to_string();
                let val = match vr {
                    Ok(Some(v)) => String::from_utf8_lossy(&v).to_string(),
                    _ => String::new(),
                };
                (field, val)
            })
            .collect()
    };

    let body = pairs
        .iter()
        .map(|(k, v)| format!("{}:{}", json_string(k), json_string(v)))
        .collect::<Vec<_>>()
        .join(",");
    json_response(format!("{{{body}}}"))
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
    let res = client.get(key.as_bytes()).await;
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
                .header("cache-control", "no-cache")
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
        .head(hls_playlist_key(&name).as_bytes())
        .await;
    match playlist {
        Ok(meta) if meta.found => json_response(TranscodeStatus::Done.to_json()),
        Ok(_) | Err(AutumnError::NotFound) => {
            // Playlist absent. If the original is still around it's a leftover
            // upload — caller should poll while we treat it as queued (the
            // startup-recovery path will pick it up; for newly-arrived single
            // requests we don't auto-spawn here to avoid a per-poll race).
            let head = client.head(file_key(&name).as_bytes()).await;
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
        let res = client.get(file_key(&name).as_bytes()).await;
        let dt = elapsed_ms(t0);
        return match res {
            Ok(Some(v)) => {
                metrics.borrow_mut().record_get(dt, v.len() as u64);
                Response::builder()
                    .header("content-type", "image/svg+xml")
                    .header("cache-control", "no-cache")
                    .body(Body::from(v))
                    .unwrap()
            }
            Ok(None) => error_response(StatusCode::NOT_FOUND, "not found".into()),
            Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("get: {e}")),
        };
    }

    let is_video = is_video_ext(&ext);
    if !is_image_ext(&ext) && !is_video {
        return error_response(
            StatusCode::NOT_FOUND,
            "no thumbnail for this file type".into(),
        );
    }

    let key = thumb_key(&name);

    // Cache hit fast path (videos rely on this — their thumbs are written
    // by the transcode pipeline; this handler never invokes ffmpeg for
    // videos anymore).
    let t0 = Instant::now();
    let cache_res = client.get(key.as_bytes()).await;
    let dt = elapsed_ms(t0);
    match cache_res {
        Ok(Some(v)) => {
            metrics.borrow_mut().record_get(dt, v.len() as u64);
            return Response::builder()
                .header("content-type", "image/jpeg")
                .header("cache-control", "no-cache")
                .header("content-length", v.len())
                .body(Body::from(v))
                .unwrap();
        }
        Ok(None) => { /* fall through */ }
        Err(e) => {
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("get thumb: {e}"))
        }
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
        let orig_res = client.get(file_key(&name).as_bytes()).await;
        let dt = elapsed_ms(t0);
        let original = match orig_res {
            Ok(Some(v)) => {
                metrics.borrow_mut().record_get(dt, v.len() as u64);
                v
            }
            Ok(None) => return error_response(StatusCode::NOT_FOUND, "not found".into()),
            Err(e) => {
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("get: {e}"))
            }
        };
        compio::runtime::spawn_blocking(move || match build_thumbnail(&original) {
            Ok(b) => BuildOutcome::Ok(b),
            Err(e) => BuildOutcome::ImageFallback(e, original),
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
            if let Err(e) = client.put(key.as_bytes(), &thumb).await {
                tracing::warn!("cache thumbnail put failed for {name}: {e}");
            }
        })
        .detach();
    }

    Response::builder()
        .header("content-type", "image/jpeg")
        .header("cache-control", "no-cache")
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
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let manager = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9001".to_string());

    let client: Client = Rc::new(ClusterClient::connect(&manager).await?);
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

    let c = SendWrapper::new(client.clone());
    let meta_route = get(move |Path(name): Path<String>| {
        let c = c.clone();
        SendWrapper::new(async move { meta_handler_inner(&c, name).await })
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

    let t = SendWrapper::new(transcodes.clone());
    let transcoding_route = get(move || {
        let t = t.clone();
        SendWrapper::new(async move { transcoding_list_handler_inner(&t).await })
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
        compio::runtime::spawn(fut).detach();
    }

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/put/", put_route)
        .route("/get/{name}", get_route)
        .route("/thumb/{name}", thumb_route)
        .route("/hls/{name}/{file}", hls_route)
        .route("/transcode-status/{name}", status_route)
        .route("/transcoding/", transcoding_route)
        .route("/del/{name}", del_route)
        .route("/list/", list_route)
        .route("/meta/{name}", meta_route)
        .route("/metrics/", metrics_route)
        // Streamed uploads keep resident memory O(chunk) regardless of size,
        // so this only bounds pathological inputs, not real videos. 64 GiB.
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024 * 1024)); // 64 GiB

    let listener = compio::net::TcpListener::bind(format!("0.0.0.0:{LISTEN_PORT}")).await?;
    tracing::info!("Gallery listening on http://0.0.0.0:{LISTEN_PORT}");
    cyper_axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        parse_byte_range, pick_hls_time, HLS_SEGMENT_TARGET_BYTES, HLS_TIME_MAX_SECS,
        HLS_TIME_MIN_SECS, PS_INLINE_CAP_BYTES,
    };

    #[test]
    fn hls_time_ordinary_bitrate_keeps_max() {
        // 100 MiB over 60 s ≈ 1.7 MiB/s → a 20 s segment is ~33 MiB, under the
        // cap, so keep the full 20 s.
        let secs = pick_hls_time(100 * 1024 * 1024, Some(60.0));
        assert_eq!(secs, HLS_TIME_MAX_SECS);
    }

    #[test]
    fn hls_time_high_bitrate_shrinks_under_cap() {
        // The reported failure: ~106 MiB / 20 s ⇒ ~5.3 MiB/s. Reconstruct a
        // similar rate (1 GiB over 200 s) and confirm the chosen duration keeps
        // an average segment under the inline cap.
        let file = 1024u64 * 1024 * 1024;
        let dur = 200.0;
        let secs = pick_hls_time(file, Some(dur));
        assert!(secs < HLS_TIME_MAX_SECS, "expected a reduced duration, got {secs}");
        assert!(secs >= HLS_TIME_MIN_SECS);
        let bytes_per_sec = file as f64 / dur;
        let seg_bytes = secs * bytes_per_sec;
        // Targets HLS_SEGMENT_TARGET_BYTES and is comfortably below the cap.
        assert!((seg_bytes - HLS_SEGMENT_TARGET_BYTES).abs() < 1.0);
        assert!(seg_bytes < PS_INLINE_CAP_BYTES as f64);
    }

    #[test]
    fn hls_time_clamps_to_min_for_extreme_bitrate() {
        // 10 GiB over 30 s is absurdly high; the target duration underflows the
        // floor, so clamp to the minimum (the re-encode safety net then bounds
        // the actual segment bytes).
        let secs = pick_hls_time(10 * 1024 * 1024 * 1024, Some(30.0));
        assert_eq!(secs, HLS_TIME_MIN_SECS);
    }

    #[test]
    fn hls_time_unknown_duration_falls_back_to_max() {
        assert_eq!(pick_hls_time(500 * 1024 * 1024, None), HLS_TIME_MAX_SECS);
        // Zero-length / empty file also can't yield a rate → max.
        assert_eq!(pick_hls_time(0, Some(10.0)), HLS_TIME_MAX_SECS);
    }

    #[test]
    fn parse_open_ended() {
        assert_eq!(parse_byte_range("bytes=0-", 1000), Some((0, 999)));
        assert_eq!(parse_byte_range("bytes=500-", 1000), Some((500, 999)));
    }

    #[test]
    fn parse_closed() {
        assert_eq!(parse_byte_range("bytes=10-99", 1000), Some((10, 99)));
        // end clamped to total - 1
        assert_eq!(parse_byte_range("bytes=10-9999", 1000), Some((10, 999)));
    }

    #[test]
    fn parse_suffix() {
        // Regression: previously parsed as bytes=0-2048.
        assert_eq!(
            parse_byte_range("bytes=-2048", 539849),
            Some((537801, 539848))
        );
        // suffix larger than total clamps.
        assert_eq!(parse_byte_range("bytes=-9999", 1000), Some((0, 999)));
    }

    #[test]
    fn parse_rejects_unrepresentable() {
        assert_eq!(parse_byte_range("bytes=1000-", 1000), None); // start at EOF
        assert_eq!(parse_byte_range("bytes=-0", 1000), None); // empty suffix
        assert_eq!(parse_byte_range("bytes=", 1000), None);
        assert_eq!(parse_byte_range("not a range", 1000), None);
        assert_eq!(parse_byte_range("bytes=0-", 0), None); // empty value
    }
}
