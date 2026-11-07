//! HTTP cache validators for the gallery's static application assets and
//! mutable media URLs.
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{header, HeaderMap, HeaderValue, Method, Response, StatusCode};
use bytes::Bytes;
use sha2::{Digest, Sha256};

pub fn static_response(
    headers: &HeaderMap,
    method: &Method,
    content_type: &str,
    data: Bytes,
) -> Response<Body> {
    // Deliberately computed on every request: these assets are tiny and this
    // keeps the validator tied directly to the bytes being returned.
    let etag = format!("\"{:x}\"", Sha256::digest(&data));
    let builder = Response::builder()
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::ETAG, &etag);
    if etag_matches(headers, &etag) {
        return builder
            .status(StatusCode::NOT_MODIFIED)
            .header(header::CONTENT_LENGTH, data.len())
            .body(Body::empty())
            .unwrap();
    }
    builder
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, data.len())
        .body(if method == Method::HEAD {
            Body::empty()
        } else {
            Body::from(data)
        })
        .unwrap()
}

fn etag_matches(headers: &HeaderMap, etag: &str) -> bool {
    headers.get_all(header::IF_NONE_MATCH).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value.split(',').any(|candidate| {
                let candidate = candidate.trim();
                candidate == "*" || candidate.strip_prefix("W/").unwrap_or(candidate) == etag
            })
        })
    })
}

pub fn system_time(seconds: Option<u64>) -> Option<SystemTime> {
    seconds.and_then(|seconds| UNIX_EPOCH.checked_add(Duration::from_secs(seconds)))
}

pub fn not_modified(headers: &HeaderMap, modified: Option<SystemTime>) -> bool {
    if headers.contains_key(header::IF_NONE_MATCH) {
        return false;
    }
    let Some(modified) = modified else {
        return false;
    };
    headers
        .get(header::IF_MODIFIED_SINCE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| httpdate::parse_http_date(value).ok())
        .is_some_and(|since| modified <= since)
}

pub fn if_range_matches(headers: &HeaderMap, modified: Option<SystemTime>) -> bool {
    let Some(value) = headers.get(header::IF_RANGE) else {
        return true;
    };
    let Some(modified) = modified else {
        return false;
    };
    value
        .to_str()
        .ok()
        .and_then(|value| httpdate::parse_http_date(value).ok())
        .is_some_and(|validator| modified <= validator)
}

pub fn media_response(
    headers: &HeaderMap,
    method: &Method,
    content_type: &str,
    data: Bytes,
    modified: Option<SystemTime>,
    ranges: bool,
) -> Response<Body> {
    let mut builder = media_builder(content_type, data.len() as u64, modified, ranges);
    if not_modified(headers, modified) {
        return builder
            .status(StatusCode::NOT_MODIFIED)
            .body(Body::empty())
            .unwrap();
    }
    if ranges && method == Method::GET && if_range_matches(headers, modified) {
        if let Some((start, end)) = headers
            .get(header::RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| super::parse_byte_range(value, data.len() as u64))
        {
            set_content_length(&mut builder, end - start + 1);
            return builder
                .status(StatusCode::PARTIAL_CONTENT)
                .header(
                    header::CONTENT_RANGE,
                    format!("bytes {start}-{end}/{}", data.len()),
                )
                .body(Body::from(data.slice(start as usize..=end as usize)))
                .unwrap();
        }
    }
    builder
        .body(if method == Method::HEAD {
            Body::empty()
        } else {
            Body::from(data)
        })
        .unwrap()
}

/// Return an empty HEAD/304 response from metadata alone. Callers must first
/// confirm that the concrete resource exists, so a deleted URL cannot validate.
pub fn metadata_response(
    headers: &HeaderMap,
    method: &Method,
    content_type: &str,
    content_length: u64,
    modified: Option<SystemTime>,
    ranges: bool,
) -> Option<Response<Body>> {
    let builder = media_builder(content_type, content_length, modified, ranges);
    if not_modified(headers, modified) {
        return Some(
            builder
                .status(StatusCode::NOT_MODIFIED)
                .body(Body::empty())
                .unwrap(),
        );
    }
    if method == Method::HEAD {
        return Some(builder.body(Body::empty()).unwrap());
    }
    None
}

pub fn media_builder(
    content_type: &str,
    content_length: u64,
    modified: Option<SystemTime>,
    ranges: bool,
) -> axum::http::response::Builder {
    let mut builder = Response::builder()
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, content_length);
    if ranges {
        builder = builder.header(header::ACCEPT_RANGES, "bytes");
    }
    if let Some(modified) = modified {
        builder = builder.header(header::LAST_MODIFIED, httpdate::fmt_http_date(modified));
    }
    builder
}

pub fn set_content_length(builder: &mut axum::http::response::Builder, length: u64) {
    builder
        .headers_mut()
        .expect("response builder headers")
        .insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&length.to_string()).expect("decimal content length"),
        );
}

/// Covers extractor failures, unknown routes and method-not-allowed responses,
/// which do not pass through the application's response helpers.
pub async fn default_no_store(mut response: Response<Body>) -> Response<Body> {
    if response.status().is_client_error()
        || response.status().is_server_error()
        || !response.headers().contains_key(header::CACHE_CONTROL)
    {
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }
    response
}
