//! HTTP validators are derived from the same bytes as the response, never from
//! a filename, length, or separately written metadata that can lag an overwrite.
use axum::body::Body;
use axum::http::{header, HeaderMap, HeaderValue, Method, Response, StatusCode};
use bytes::Bytes;
use sha2::{Digest, Sha256};

pub fn etag(data: &[u8]) -> String {
    format!("\"{:x}\"", Sha256::digest(data))
}

fn not_modified(headers: &HeaderMap, etag: &str) -> bool {
    headers.get_all(header::IF_NONE_MATCH).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value.split(',').any(|candidate| {
                let candidate = candidate.trim();
                candidate == "*" || candidate.strip_prefix("W/").unwrap_or(candidate) == etag
            })
        })
    })
}

/// Used only after the resource has been successfully read. A missing resource
/// must return 404 even if the request carries its former ETag or `*`.
pub fn response(
    headers: &HeaderMap,
    method: &Method,
    content_type: &str,
    data: Bytes,
    ranges: bool,
) -> Response<Body> {
    let etag = etag(&data);
    let mut builder = Response::builder()
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::ETAG, &etag);
    // Preconditions are evaluated before Range, including on HEAD requests.
    if not_modified(headers, &etag) {
        return builder
            .status(StatusCode::NOT_MODIFIED)
            // Axum fills in Content-Length from the empty body's size hint
            // otherwise. On 304 this header must describe the full 200 body.
            .header(header::CONTENT_LENGTH, data.len())
            .body(Body::empty())
            .unwrap();
    }
    builder = builder.header(header::CONTENT_TYPE, content_type);
    if ranges {
        builder = builder.header(header::ACCEPT_RANGES, "bytes");
    }
    // We have no Last-Modified validator: dates and weak tags in If-Range
    // cannot establish a match, so send the complete current representation.
    let range_allowed = ranges
        && method == Method::GET
        && headers
            .get(header::IF_RANGE)
            .is_none_or(|value| value.as_bytes() == etag.as_bytes());
    if range_allowed {
        if let Some((start, end)) = headers
            .get(header::RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| super::parse_byte_range(value, data.len() as u64))
        {
            return builder
                .status(StatusCode::PARTIAL_CONTENT)
                .header(
                    header::CONTENT_RANGE,
                    format!("bytes {start}-{end}/{}", data.len()),
                )
                .header(header::CONTENT_LENGTH, end - start + 1)
                .body(Body::from(data.slice(start as usize..=end as usize)))
                .unwrap();
        }
    }
    builder
        .header(header::CONTENT_LENGTH, data.len())
        .body(if method == Method::HEAD {
            Body::empty()
        } else {
            Body::from(data)
        })
        .unwrap()
}

/// Also covers extractor failures, unknown routes and method-not-allowed
/// responses, which do not go through the application's response helpers.
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use futures::executor::block_on;

    fn body(response: Response<Body>) -> Bytes {
        block_on(to_bytes(response.into_body(), usize::MAX)).unwrap()
    }

    fn serve(headers: &HeaderMap, method: Method, data: &'static [u8]) -> Response<Body> {
        response(
            headers,
            &method,
            "image/png",
            Bytes::from_static(data),
            true,
        )
    }

    #[test]
    fn validators_revalidate_content_including_same_size_replacements() {
        let first = serve(&HeaderMap::new(), Method::GET, b"old");
        assert_eq!(first.headers()[header::CACHE_CONTROL], "no-cache");
        let tag = first.headers()[header::ETAG].clone();
        assert_eq!(body(first), "old");
        let mut headers = HeaderMap::new();
        headers.insert(header::IF_NONE_MATCH, tag.clone());
        let unchanged = serve(&headers, Method::GET, b"old");
        assert_eq!(unchanged.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(unchanged.headers()[header::ETAG], tag);
        assert_eq!(unchanged.headers()[header::CACHE_CONTROL], "no-cache");
        assert_eq!(unchanged.headers()[header::CONTENT_LENGTH], "3");
        assert!(body(unchanged).is_empty());
        let replaced = serve(&headers, Method::GET, b"new");
        assert_eq!(replaced.status(), StatusCode::OK);
        assert_ne!(replaced.headers()[header::ETAG], tag);
        assert_eq!(body(replaced), "new");
    }

    #[test]
    fn if_none_match_accepts_weak_lists_repeated_fields_and_wildcard() {
        let tag = etag(b"data");
        for value in [
            format!("W/{tag}"),
            format!("\"other\", W/{tag}"),
            "*".into(),
        ] {
            let mut headers = HeaderMap::new();
            headers.append(header::IF_NONE_MATCH, HeaderValue::from_static("\"miss\""));
            headers.append(header::IF_NONE_MATCH, value.parse().unwrap());
            assert_eq!(
                serve(&headers, Method::GET, b"data").status(),
                StatusCode::NOT_MODIFIED
            );
        }
        let mut headers = HeaderMap::new();
        headers.insert(
            header::IF_NONE_MATCH,
            HeaderValue::from_static("\"other,tag\""),
        );
        headers.insert(
            header::IF_MODIFIED_SINCE,
            HeaderValue::from_static("Wed, 01 Jan 2098 00:00:00 GMT"),
        );
        assert_eq!(
            serve(&headers, Method::GET, b"data").status(),
            StatusCode::OK
        );
    }

    #[test]
    fn range_requires_current_strong_if_range_and_uses_whole_value_etag() {
        let tag = etag(b"abcdef");
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=1-3"));
        headers.insert(header::IF_RANGE, tag.parse().unwrap());
        let partial = serve(&headers, Method::GET, b"abcdef");
        assert_eq!(partial.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(partial.headers()[header::ETAG], tag);
        assert_eq!(partial.headers()[header::CONTENT_RANGE], "bytes 1-3/6");
        assert_eq!(partial.headers()[header::CONTENT_LENGTH], "3");
        assert_eq!(body(partial), "bcd");
        for stale in [
            etag(b"ghijkl"),
            format!("W/{tag}"),
            "Wed, 01 Jan 2020 00:00:00 GMT".into(),
        ] {
            headers.insert(header::IF_RANGE, stale.parse().unwrap());
            let full = serve(&headers, Method::GET, b"abcdef");
            assert_eq!(full.status(), StatusCode::OK);
            assert!(!full.headers().contains_key(header::CONTENT_RANGE));
            assert_eq!(body(full), "abcdef");
        }
        headers.insert(header::IF_NONE_MATCH, tag.parse().unwrap());
        let validated = serve(&headers, Method::GET, b"abcdef");
        assert_eq!(validated.status(), StatusCode::NOT_MODIFIED);
        assert!(body(validated).is_empty());
    }

    #[test]
    fn head_ignores_range_and_empty_values_are_validatable() {
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=0-1"));
        let head = serve(&headers, Method::HEAD, b"abcd");
        assert_eq!(head.status(), StatusCode::OK);
        assert_eq!(head.headers()[header::CONTENT_LENGTH], "4");
        assert!(body(head).is_empty());
        let empty = serve(&headers, Method::GET, b"");
        assert_eq!(empty.status(), StatusCode::OK);
        assert_eq!(empty.headers()[header::CONTENT_LENGTH], "0");
        headers.insert(header::IF_NONE_MATCH, empty.headers()[header::ETAG].clone());
        assert!(body(empty).is_empty());
        assert_eq!(
            serve(&headers, Method::HEAD, b"").status(),
            StatusCode::NOT_MODIFIED
        );
    }

    #[test]
    fn router_applies_no_store_to_api_errors_and_preserves_static_validation() {
        use axum::{extract::Path, http::Request, middleware, routing::get, Router};
        use tower::ServiceExt;
        block_on(async {
            let app = Router::new()
                .route("/", get(crate::index_handler))
                .route("/api", get(|| async { crate::json_response("{}".into()) }))
                .route(
                    "/number/{n}",
                    get(|Path(_): Path<u32>| async { crate::ok_response("OK") }),
                )
                .layer(middleware::map_response(default_no_store));
            for (method, path, status) in [
                (Method::GET, "/api", StatusCode::OK),
                (Method::GET, "/missing", StatusCode::NOT_FOUND),
                (Method::GET, "/number/invalid", StatusCode::BAD_REQUEST),
                (Method::POST, "/api", StatusCode::METHOD_NOT_ALLOWED),
            ] {
                let request = Request::builder()
                    .method(method)
                    .uri(path)
                    .header(header::IF_NONE_MATCH, "*")
                    .body(Body::empty())
                    .unwrap();
                let result = app.clone().oneshot(request).await.unwrap();
                assert_eq!(result.status(), status);
                assert_eq!(result.headers()[header::CACHE_CONTROL], "no-store");
            }
            let first = app
                .clone()
                .oneshot(Request::new(Body::empty()))
                .await
                .unwrap();
            let tag = first.headers()[header::ETAG].clone();
            assert_eq!(first.headers()[header::CACHE_CONTROL], "no-cache");
            let request = Request::builder()
                .header(header::IF_NONE_MATCH, tag)
                .body(Body::empty())
                .unwrap();
            let next = app.oneshot(request).await.unwrap();
            assert_eq!(next.status(), StatusCode::NOT_MODIFIED);
            assert_eq!(next.headers()[header::CACHE_CONTROL], "no-cache");
            assert_eq!(
                next.headers()[header::CONTENT_LENGTH],
                include_bytes!("../static/index.html").len().to_string()
            );
            assert!(to_bytes(next.into_body(), usize::MAX)
                .await
                .unwrap()
                .is_empty());
        });
    }
}
