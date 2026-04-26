use std::io::Cursor;
use std::rc::Rc;

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

// ---------------------------------------------------------------------------
// Thumbnail helpers
// ---------------------------------------------------------------------------

const THUMB_PREFIX: &str = ".thumb/";
const THUMB_WIDTH: u32 = 320;
const THUMB_QUALITY: u8 = 80;

fn thumb_key(name: &str) -> String {
    format!("{THUMB_PREFIX}{THUMB_WIDTH}/{name}")
}

fn ext_of(name: &str) -> String {
    name.rsplit('.').next().unwrap_or("").to_ascii_lowercase()
}

fn is_image_ext(ext: &str) -> bool {
    matches!(ext, "jpg" | "jpeg" | "png" | "gif" | "bmp" | "webp")
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

async fn put_handler_inner(client: &Client, mut multipart: Multipart) -> Response<Body> {
    while let Ok(Some(field)) = multipart.next_field().await {
        let filename = match field.file_name() {
            Some(name) => name.to_string(),
            None => continue,
        };
        let data = match field.bytes().await {
            Ok(b) => b.to_vec(),
            Err(e) => return error_response(StatusCode::BAD_REQUEST, format!("read field: {e}")),
        };
        if let Err(e) = client.lock().await.put(filename.as_bytes(), &data, true).await {
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("put: {e}"));
        }
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
                        let slice = match client
                            .lock()
                            .await
                            .get_range(name.as_bytes(), start as u32, length as u32)
                            .await
                        {
                            Ok(Some(v)) => v,
                            Ok(None) => return error_response(StatusCode::NOT_FOUND, "not found".into()),
                            Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("get: {e}")),
                        };
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
    match client.lock().await.get(name.as_bytes()).await {
        Ok(Some(v)) => Response::builder()
            .header("content-type", &mime)
            .header("accept-ranges", "bytes")
            .header("content-length", v.len())
            .body(Body::from(v))
            .unwrap(),
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

async fn thumb_handler_inner(client: &Client, name: String) -> Response<Body> {
    let ext = ext_of(&name);

    // SVG: no point rasterizing — just serve the original bytes.
    if is_svg_ext(&ext) {
        return match client.lock().await.get(name.as_bytes()).await {
            Ok(Some(v)) => Response::builder()
                .header("content-type", "image/svg+xml")
                .header("cache-control", "public, max-age=86400")
                .body(Body::from(v))
                .unwrap(),
            Ok(None) => error_response(StatusCode::NOT_FOUND, "not found".into()),
            Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("get: {e}")),
        };
    }

    if !is_image_ext(&ext) {
        return error_response(StatusCode::NOT_FOUND, "no thumbnail for this file type".into());
    }

    let key = thumb_key(&name);

    // Cache hit fast path.
    match client.lock().await.get(key.as_bytes()).await {
        Ok(Some(v)) => {
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

    // Cache miss: load original.
    let original = match client.lock().await.get(name.as_bytes()).await {
        Ok(Some(v)) => v,
        Ok(None) => return error_response(StatusCode::NOT_FOUND, "not found".into()),
        Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("get: {e}")),
    };

    // Decode + resize. On failure, degrade gracefully by serving the original.
    let thumb = match build_thumbnail(&original) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("thumbnail build failed for {name}: {e} — serving original");
            let mime = mime_guess::from_path(&name)
                .first_or_octet_stream()
                .to_string();
            return Response::builder()
                .header("content-type", mime)
                .body(Body::from(original))
                .unwrap();
        }
    };

    // Write back to autumn for next time. Best-effort: failure here doesn't
    // block the response.
    if let Err(e) = client
        .lock()
        .await
        .put(key.as_bytes(), &thumb, false)
        .await
    {
        tracing::warn!("cache thumbnail put failed for {name}: {e}");
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

    // Wrap captures in SendWrapper to satisfy axum's Send requirement.
    // Safe because compio runs everything on a single thread.
    let c = SendWrapper::new(client.clone());
    let put_route = post(move |multipart: Multipart| {
        let c = c.clone();
        SendWrapper::new(async move { put_handler_inner(&c, multipart).await })
    });

    let c = SendWrapper::new(client.clone());
    let get_route = get(move |Path(name): Path<String>, headers: HeaderMap| {
        let c = c.clone();
        SendWrapper::new(async move { get_handler_inner(&c, name, headers).await })
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
    let thumb_route = get(move |Path(name): Path<String>| {
        let c = c.clone();
        SendWrapper::new(async move { thumb_handler_inner(&c, name).await })
    });

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/put/", put_route)
        .route("/get/{name}", get_route)
        .route("/thumb/{name}", thumb_route)
        .route("/del/{name}", del_route)
        .route("/list/", list_route)
        .layer(DefaultBodyLimit::max(1024 * 1024 * 1024)); // 1 GB

    let listener = compio::net::TcpListener::bind("0.0.0.0:5001").await?;
    tracing::info!("Gallery listening on http://0.0.0.0:5001");
    cyper_axum::serve(listener, app).await?;
    Ok(())
}
