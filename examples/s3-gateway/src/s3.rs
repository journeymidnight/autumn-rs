//! S3 wire shapes: XML rendering, error bodies, `Range` parsing, timestamps.
//!
//! Everything here is pure — no autumn types — so the parts the AWS SDK is
//! picky about can be unit-tested without a cluster.

use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

/// Escape the five XML metacharacters. Object keys come from user-created
/// filenames, so this is not optional.
pub fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// RFC 3986 percent-encoding for `encoding-type=url` listings. S3 encodes
/// everything outside the unreserved set, `/` included.
pub fn url_encode(s: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char)
            }
            _ => {
                out.push('%');
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 0x0f) as usize] as char);
            }
        }
    }
    out
}

/// Unix seconds -> `YYYY-MM-DDTHH:MM:SS.000Z`, the only timestamp format S3
/// listings use. Hand-rolled (days-from-civil, Howard Hinnant) rather than
/// pulling `chrono` in for one function.
pub fn iso8601(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (h, mi, s) = (tod / 3600, (tod % 3600) / 60, tod % 60);

    // days -> civil date
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.000Z")
}

/// A parsed `Range: bytes=...` header, already clamped to the object size.
#[derive(Debug, PartialEq, Eq)]
pub enum RangeSpec {
    /// Serve `[start, end]` inclusive as a 206.
    Partial { start: u64, end: u64 },
    /// No `Range` header — serve the whole object as a 200.
    Whole,
    /// Syntactically valid but outside the object — 416.
    Unsatisfiable,
}

/// Parse the single-range forms S3 clients actually send: `bytes=a-b`,
/// `bytes=a-`, `bytes=-suffix`. Multi-range (`a-b,c-d`) is not supported by
/// S3 either, so an unparseable header degrades to `Whole` exactly as S3 does.
pub fn parse_range(header_value: Option<&str>, size: u64) -> RangeSpec {
    let Some(raw) = header_value else {
        return RangeSpec::Whole;
    };
    let Some(spec) = raw.trim().strip_prefix("bytes=") else {
        return RangeSpec::Whole;
    };
    if spec.contains(',') {
        return RangeSpec::Whole;
    }
    let Some((from, to)) = spec.split_once('-') else {
        return RangeSpec::Whole;
    };
    let (from, to) = (from.trim(), to.trim());

    let (start, end) = match (from.is_empty(), to.is_empty()) {
        // `bytes=-N` — the LAST n bytes. N=0 is unsatisfiable per RFC 9110.
        (true, false) => {
            let Ok(n) = to.parse::<u64>() else {
                return RangeSpec::Whole;
            };
            if n == 0 {
                return RangeSpec::Unsatisfiable;
            }
            (size.saturating_sub(n), size.saturating_sub(1))
        }
        // `bytes=N-` — from N to the end.
        (false, true) => {
            let Ok(s) = from.parse::<u64>() else {
                return RangeSpec::Whole;
            };
            (s, size.saturating_sub(1))
        }
        // `bytes=A-B` — B is clamped to the last byte, which is legal and is
        // what the streamer relies on for the final chunk of a shard.
        (false, false) => {
            let (Ok(s), Ok(e)) = (from.parse::<u64>(), to.parse::<u64>()) else {
                return RangeSpec::Whole;
            };
            (s, e.min(size.saturating_sub(1)))
        }
        (true, true) => return RangeSpec::Whole,
    };

    if size == 0 || start >= size || start > end {
        RangeSpec::Unsatisfiable
    } else {
        RangeSpec::Partial { start, end }
    }
}

/// One `<Contents>` row.
pub struct ObjectRow {
    pub key: String,
    pub size: u64,
    pub mtime_secs: i64,
    pub etag: String,
}

/// Render a `ListObjectsV2` response body.
#[allow(clippy::too_many_arguments)]
pub fn list_objects_xml(
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: usize,
    url_encoded: bool,
    rows: &[ObjectRow],
    common_prefixes: &[String],
    next_token: Option<&str>,
) -> String {
    // With `encoding-type=url` every key-shaped field is percent-encoded
    // BEFORE xml-escaping; without it, only xml-escaped.
    let enc = |s: &str| {
        if url_encoded {
            xml_escape(&url_encode(s))
        } else {
            xml_escape(s)
        }
    };

    let mut x = String::with_capacity(512 + rows.len() * 256);
    x.push_str(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    x.push_str(r#"<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#);
    x.push_str(&format!("<Name>{}</Name>", xml_escape(bucket)));
    x.push_str(&format!("<Prefix>{}</Prefix>", enc(prefix)));
    x.push_str(&format!("<KeyCount>{}</KeyCount>", rows.len() + common_prefixes.len()));
    x.push_str(&format!("<MaxKeys>{max_keys}</MaxKeys>"));
    if let Some(d) = delimiter {
        x.push_str(&format!("<Delimiter>{}</Delimiter>", enc(d)));
    }
    if url_encoded {
        x.push_str("<EncodingType>url</EncodingType>");
    }
    x.push_str(&format!(
        "<IsTruncated>{}</IsTruncated>",
        if next_token.is_some() { "true" } else { "false" }
    ));
    if let Some(t) = next_token {
        x.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            xml_escape(t)
        ));
    }
    for r in rows {
        x.push_str("<Contents>");
        x.push_str(&format!("<Key>{}</Key>", enc(&r.key)));
        x.push_str(&format!(
            "<LastModified>{}</LastModified>",
            iso8601(r.mtime_secs)
        ));
        x.push_str(&format!("<ETag>&quot;{}&quot;</ETag>", xml_escape(&r.etag)));
        x.push_str(&format!("<Size>{}</Size>", r.size));
        x.push_str("<StorageClass>STANDARD</StorageClass>");
        x.push_str("</Contents>");
    }
    for p in common_prefixes {
        x.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            enc(p)
        ));
    }
    x.push_str("</ListBucketResult>");
    x
}

/// Render a `ListBuckets` response body.
pub fn list_buckets_xml(buckets: &[String]) -> String {
    let mut x = String::new();
    x.push_str(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    x.push_str(r#"<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#);
    x.push_str("<Owner><ID>autumn</ID><DisplayName>autumn</DisplayName></Owner><Buckets>");
    for b in buckets {
        x.push_str(&format!(
            "<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>",
            xml_escape(b),
            iso8601(0)
        ));
    }
    x.push_str("</Buckets></ListAllMyBucketsResult>");
    x
}

/// An S3 `<Error>` body with the matching HTTP status. The AWS SDK parses this
/// to produce a typed error, so an unstructured 500 body would surface to the
/// engine as an opaque parse failure instead of "no such key".
pub struct S3Error {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
    pub resource: String,
}

impl S3Error {
    pub fn no_such_key(resource: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "NoSuchKey",
            message: "The specified key does not exist.".into(),
            resource: resource.into(),
        }
    }

    pub fn no_such_bucket(resource: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "NoSuchBucket",
            message: "The specified bucket does not exist.".into(),
            resource: resource.into(),
        }
    }

    pub fn not_implemented(what: &str) -> Self {
        Self {
            status: StatusCode::NOT_IMPLEMENTED,
            code: "NotImplemented",
            message: format!("{what} is not supported by this read-only gateway."),
            resource: String::new(),
        }
    }

    pub fn internal(message: impl Into<String>, resource: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "InternalError",
            message: message.into(),
            resource: resource.into(),
        }
    }

    pub fn range_not_satisfiable(resource: impl Into<String>, size: u64) -> (Self, u64) {
        (
            Self {
                status: StatusCode::RANGE_NOT_SATISFIABLE,
                code: "InvalidRange",
                message: "The requested range is not satisfiable.".into(),
                resource: resource.into(),
            },
            size,
        )
    }
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>{}</Code><Message>{}</Message><Resource>{}</Resource></Error>"#,
            self.code,
            xml_escape(&self.message),
            xml_escape(&self.resource),
        );
        (
            self.status,
            [(header::CONTENT_TYPE, "application/xml")],
            body,
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso8601_matches_known_instants() {
        assert_eq!(iso8601(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso8601(1_000_000_000), "2001-09-09T01:46:40.000Z");
        // A leap day, because days-from-civil is where such code goes wrong.
        assert_eq!(iso8601(1_709_164_800), "2024-02-29T00:00:00.000Z");
    }

    #[test]
    fn range_forms_the_streamer_sends() {
        assert_eq!(parse_range(None, 100), RangeSpec::Whole);
        assert_eq!(
            parse_range(Some("bytes=0-7"), 100),
            RangeSpec::Partial { start: 0, end: 7 }
        );
        assert_eq!(
            parse_range(Some("bytes=90-"), 100),
            RangeSpec::Partial { start: 90, end: 99 }
        );
        assert_eq!(
            parse_range(Some("bytes=-10"), 100),
            RangeSpec::Partial { start: 90, end: 99 }
        );
        // An end past EOF is clamped, not rejected — the last chunk of a
        // safetensors shard is requested this way.
        assert_eq!(
            parse_range(Some("bytes=95-999"), 100),
            RangeSpec::Partial { start: 95, end: 99 }
        );
    }

    #[test]
    fn range_edges() {
        assert_eq!(parse_range(Some("bytes=100-200"), 100), RangeSpec::Unsatisfiable);
        assert_eq!(parse_range(Some("bytes=0-0"), 0), RangeSpec::Unsatisfiable);
        assert_eq!(parse_range(Some("bytes=-0"), 100), RangeSpec::Unsatisfiable);
        // Unparseable / multi-range degrade to the whole object, as S3 does.
        assert_eq!(parse_range(Some("items=0-7"), 100), RangeSpec::Whole);
        assert_eq!(parse_range(Some("bytes=0-7,9-10"), 100), RangeSpec::Whole);
    }

    #[test]
    fn keys_are_escaped_and_optionally_encoded() {
        assert_eq!(xml_escape("a&b<c>"), "a&amp;b&lt;c&gt;");
        assert_eq!(url_encode("models/llama 3/x.safetensors"), "models%2Fllama%203%2Fx.safetensors");
    }

    #[test]
    fn list_xml_carries_the_fields_the_sdk_reads() {
        let rows = vec![ObjectRow {
            key: "llama/model-00001.safetensors".into(),
            size: 4096,
            mtime_secs: 0,
            etag: "deadbeef".into(),
        }];
        let x = list_objects_xml("models", "llama/", Some("/"), 1000, false, &rows, &[], None);
        assert!(x.contains("<Name>models</Name>"));
        assert!(x.contains("<Key>llama/model-00001.safetensors</Key>"));
        assert!(x.contains("<Size>4096</Size>"));
        assert!(x.contains("<IsTruncated>false</IsTruncated>"));
        assert!(x.contains("<KeyCount>1</KeyCount>"));
    }
}
