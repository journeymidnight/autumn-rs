//! S3 wire shapes: XML rendering and parsing, error bodies, `Range` and
//! conditional headers, HTTP dates, `aws-chunked` bodies, key validation.
//!
//! Everything here is pure — no autumn types — so the parts the AWS SDK is
//! picky about can be unit-tested without a cluster.

use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::{Bytes, BytesMut};

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
            message: format!("{what} is not supported by this gateway."),
            resource: String::new(),
        }
    }

    fn new(status: StatusCode, code: &'static str, message: impl Into<String>, resource: impl Into<String>) -> Self {
        Self { status, code, message: message.into(), resource: resource.into() }
    }

    pub fn precondition_failed(resource: impl Into<String>) -> Self {
        Self::new(
            StatusCode::PRECONDITION_FAILED,
            "PreconditionFailed",
            "At least one of the pre-conditions you specified did not hold.",
            resource,
        )
    }

    /// Another writer holds the object: an in-place writer on a mount, or a
    /// concurrent Complete. S3 reports it at once rather than waiting.
    pub fn conditional_conflict(message: impl Into<String>, resource: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "ConditionalRequestConflict", message, resource)
    }

    /// Retryable: SDKs back off and retry a 503 on their own.
    pub fn slow_down(message: impl Into<String>, resource: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, "SlowDown", message, resource)
    }

    pub fn no_such_upload(resource: impl Into<String>) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "NoSuchUpload",
            "The specified multipart upload does not exist.",
            resource,
        )
    }

    pub fn invalid_part(part: u32, resource: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "InvalidPart",
            format!("Part {part} could not be found, or its ETag does not match the part's."),
            resource,
        )
    }

    pub fn invalid_part_order(resource: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "InvalidPartOrder",
            "The list of parts was not in ascending order.",
            resource,
        )
    }

    pub fn entity_too_small(part: u32, resource: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "EntityTooSmall",
            format!("Part {part} is smaller than the minimum allowed size."),
            resource,
        )
    }

    pub fn malformed_xml(message: impl Into<String>, resource: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "MalformedXML", message, resource)
    }

    pub fn invalid_argument(message: impl Into<String>, resource: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "InvalidArgument", message, resource)
    }

    pub fn invalid_request(message: impl Into<String>, resource: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "InvalidRequest", message, resource)
    }

    pub fn bad_digest(resource: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "BadDigest",
            "The Content-MD5 you specified did not match what we received.",
            resource,
        )
    }

    pub fn invalid_digest(resource: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "InvalidDigest", "The Content-MD5 you specified is not valid.", resource)
    }

    pub fn incomplete_body(message: impl Into<String>, resource: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "IncompleteBody", message, resource)
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

// ── HTTP dates ───────────────────────────────────────────────────────────────

const WEEKDAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
const MONTHS: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Unix seconds -> IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`), the form
/// `Last-Modified` must carry. ISO 8601 there is not a valid HTTP date, and
/// clients drop a header they cannot parse.
pub fn http_date(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{}, {d:02} {} {y:04} {:02}:{:02}:{:02} GMT",
        WEEKDAYS[days.rem_euclid(7) as usize],
        MONTHS[(m - 1) as usize],
        tod / 3600,
        (tod % 3600) / 60,
        tod % 60
    )
}

/// Parse an IMF-fixdate. The two obsolete forms RFC 9110 still lets a
/// server accept are not produced by any current client; an unparseable date
/// makes its condition be ignored, which is what RFC 9110 prescribes.
pub fn parse_http_date(s: &str) -> Option<i64> {
    let mut it = s.split_whitespace();
    let _weekday = it.next()?;
    let d: i64 = it.next()?.parse().ok()?;
    let mon = it.next()?;
    let m = MONTHS.iter().position(|x| x.eq_ignore_ascii_case(mon))? as i64 + 1;
    let y: i64 = it.next()?.parse().ok()?;
    let hms = it.next()?;
    if it.next()? != "GMT" {
        return None;
    }
    let mut t = hms.split(':').map(|x| x.parse::<i64>().ok());
    let (h, mi, sec) = (t.next()??, t.next()??, t.next()??);
    if !(1..=31).contains(&d) || h > 23 || mi > 59 || sec > 60 {
        return None;
    }
    Some(days_from_civil(y, m, d) * 86_400 + h * 3600 + mi * 60 + sec)
}

// ── conditional reads ────────────────────────────────────────────────────────

/// Whether an `If-Match` / `If-None-Match` value names `etag`. The value is a
/// comma-separated list of quoted tags, possibly weak, or `*`.
pub fn etag_list_matches(header: &str, etag: &str) -> bool {
    header.split(',').map(str::trim).any(|t| {
        t == "*" || t.trim_start_matches("W/").trim_matches('"') == etag
    })
}

/// The conditional headers of a GET or HEAD (or, for CopyObject, the
/// `x-amz-copy-source-if-*` headers about the source).
#[derive(Default, Debug)]
pub struct ReadConditions {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<i64>,
    pub if_unmodified_since: Option<i64>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CondOutcome {
    Proceed,
    NotModified,
    PreconditionFailed,
}

/// RFC 9110 §13.2.2 order, which is also what S3 does: If-Match, else
/// If-Unmodified-Since; then If-None-Match, else If-Modified-Since. So a true
/// If-Match overrides a false If-Unmodified-Since, and a failed If-None-Match
/// is a 304 whatever If-Modified-Since says.
pub fn evaluate_read(c: &ReadConditions, etag: &str, mtime: i64) -> CondOutcome {
    match &c.if_match {
        Some(v) if !etag_list_matches(v, etag) => return CondOutcome::PreconditionFailed,
        Some(_) => {}
        None => {
            if c.if_unmodified_since.is_some_and(|t| mtime > t) {
                return CondOutcome::PreconditionFailed;
            }
        }
    }
    match &c.if_none_match {
        Some(v) if etag_list_matches(v, etag) => CondOutcome::NotModified,
        Some(_) => CondOutcome::Proceed,
        None if c.if_modified_since.is_some_and(|t| mtime <= t) => CondOutcome::NotModified,
        None => CondOutcome::Proceed,
    }
}

// ── XML request bodies ──────────────────────────────────────────────────────

/// The inner text of every `<tag>` element (attributes allowed, no nesting of
/// the same tag), in document order. Enough for the two request bodies S3
/// clients send here; not a general XML parser.
fn elements<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        // `<Part>` must not match `<PartNumber>`.
        match after.chars().next() {
            Some('>') | Some(' ') | Some('\t') | Some('\n') | Some('\r') => {}
            Some('/') => {
                // `<Tag/>`: empty.
                out.push("");
                rest = &after[1..];
                continue;
            }
            _ => {
                rest = after;
                continue;
            }
        }
        let Some(gt) = after.find('>') else { break };
        let body = &after[gt + 1..];
        let Some(end) = body.find(&close) else { break };
        out.push(&body[..end]);
        rest = &body[end + close.len()..];
    }
    out
}

/// Undo XML escaping: the five named entities and numeric references.
pub fn xml_unescape(s: &str) -> Option<String> {
    if !s.contains('&') {
        return Some(s.to_string());
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        let after = &rest[i + 1..];
        let semi = after.find(';')?;
        let ent = &after[..semi];
        match ent {
            "amp" => out.push('&'),
            "lt" => out.push('<'),
            "gt" => out.push('>'),
            "quot" => out.push('"'),
            "apos" => out.push('\''),
            _ => {
                let n = if let Some(h) = ent.strip_prefix("#x").or_else(|| ent.strip_prefix("#X")) {
                    u32::from_str_radix(h, 16).ok()?
                } else {
                    ent.strip_prefix('#')?.parse().ok()?
                };
                out.push(char::from_u32(n)?);
            }
        }
        rest = &after[semi + 1..];
    }
    out.push_str(rest);
    Some(out)
}

/// A CompleteMultipartUpload body: `(part number, ETag)` in document order.
/// The order and the numbers themselves are checked by the multipart layer.
pub fn parse_complete_xml(xml: &str) -> Result<Vec<(u32, String)>, &'static str> {
    if elements(xml, "CompleteMultipartUpload").is_empty() {
        return Err("expected a CompleteMultipartUpload element");
    }
    let mut out = Vec::new();
    for part in elements(xml, "Part") {
        let num = elements(part, "PartNumber");
        let etag = elements(part, "ETag");
        let (Some(num), Some(etag)) = (num.first(), etag.first()) else {
            return Err("every Part needs a PartNumber and an ETag");
        };
        let num: u32 = num.trim().parse().map_err(|_| "PartNumber is not a number")?;
        let etag = xml_unescape(etag.trim()).ok_or("bad escape in ETag")?;
        out.push((num, etag));
    }
    Ok(out)
}

/// The most keys one DeleteObjects request may name.
pub const MAX_DELETE_KEYS: usize = 1000;

/// A DeleteObjects body.
#[derive(Debug, PartialEq, Eq)]
pub struct DeleteRequest {
    pub quiet: bool,
    pub keys: Vec<String>,
}

pub fn parse_delete_xml(xml: &str) -> Result<DeleteRequest, &'static str> {
    if elements(xml, "Delete").is_empty() {
        return Err("expected a Delete element");
    }
    let quiet = elements(xml, "Quiet").first().is_some_and(|q| q.trim().eq_ignore_ascii_case("true"));
    let mut keys = Vec::new();
    for obj in elements(xml, "Object") {
        let Some(k) = elements(obj, "Key").first().copied() else {
            return Err("every Object needs a Key");
        };
        keys.push(xml_unescape(k).ok_or("bad escape in Key")?);
    }
    if keys.is_empty() || keys.len() > MAX_DELETE_KEYS {
        return Err("a Delete names between 1 and 1000 objects");
    }
    Ok(DeleteRequest { quiet, keys })
}

// ── XML response bodies ─────────────────────────────────────────────────────

const XML_HEAD: &str = r#"<?xml version="1.0" encoding="UTF-8"?>"#;
const XMLNS: &str = r#" xmlns="http://s3.amazonaws.com/doc/2006-03-01/""#;

pub fn initiate_multipart_xml(bucket: &str, key: &str, upload_id: &str) -> String {
    format!(
        "{XML_HEAD}<InitiateMultipartUploadResult{XMLNS}><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id)
    )
}

pub fn complete_multipart_xml(bucket: &str, key: &str, etag: &str) -> String {
    format!(
        "{XML_HEAD}<CompleteMultipartUploadResult{XMLNS}><Location>/{}/{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>&quot;{}&quot;</ETag></CompleteMultipartUploadResult>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(etag)
    )
}

pub fn copy_object_xml(etag: &str, mtime_secs: i64) -> String {
    format!(
        "{XML_HEAD}<CopyObjectResult{XMLNS}><LastModified>{}</LastModified><ETag>&quot;{}&quot;</ETag></CopyObjectResult>",
        iso8601(mtime_secs),
        xml_escape(etag)
    )
}

/// One key's outcome in a DeleteObjects response.
pub enum DeleteOutcome {
    Deleted(String),
    Failed { key: String, code: &'static str, message: String },
}

/// Quiet mode reports only the failures.
pub fn delete_result_xml(outcomes: &[DeleteOutcome], quiet: bool) -> String {
    let mut x = String::with_capacity(128 + outcomes.len() * 96);
    x.push_str(XML_HEAD);
    x.push_str(&format!("<DeleteResult{XMLNS}>"));
    for o in outcomes {
        match o {
            DeleteOutcome::Deleted(k) if !quiet => {
                x.push_str(&format!("<Deleted><Key>{}</Key></Deleted>", xml_escape(k)))
            }
            DeleteOutcome::Deleted(_) => {}
            DeleteOutcome::Failed { key, code, message } => x.push_str(&format!(
                "<Error><Key>{}</Key><Code>{code}</Code><Message>{}</Message></Error>",
                xml_escape(key),
                xml_escape(message)
            )),
        }
    }
    x.push_str("</DeleteResult>");
    x
}

// ── identifiers ─────────────────────────────────────────────────────────────

/// Upload ids are the upload record's number in hex.
pub fn format_upload_id(id: u64) -> String {
    format!("{id:016x}")
}

pub fn parse_upload_id(s: &str) -> Option<u64> {
    (s.len() == 16).then(|| u64::from_str_radix(s, 16).ok()).flatten()
}

/// Percent-decode (`+` is not a space here: copy sources are paths).
pub fn percent_decode(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let h = std::str::from_utf8(b.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(h, 16).ok()?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// `x-amz-copy-source`: `[/]bucket/key[?versionId=...]`, URL-encoded. A
/// version id is ignored: this store keeps one version of every object.
pub fn parse_copy_source(v: &str) -> Option<(String, String)> {
    let path = v.split_once('?').map_or(v, |(p, _)| p);
    let path = percent_decode(path)?;
    let path = path.strip_prefix('/').unwrap_or(&path);
    let (bucket, key) = path.split_once('/')?;
    (!bucket.is_empty() && !key.is_empty()).then(|| (bucket.to_string(), key.to_string()))
}

/// A key as a path under its bucket: the directories, and the object's name
/// (`None` for a key ending in `/`, a directory marker). Empty, `.` and `..`
/// components have no file-system meaning and are refused.
pub fn key_path(key: &str) -> Result<(Vec<&str>, Option<&str>), &'static str> {
    if key.is_empty() {
        return Err("empty key");
    }
    if key.contains('\0') {
        return Err("a key may not contain NUL");
    }
    let (body, marker) = match key.strip_suffix('/') {
        Some(b) => (b, true),
        None => (key, false),
    };
    let mut comps: Vec<&str> = if body.is_empty() { Vec::new() } else { body.split('/').collect() };
    if comps.iter().any(|c| c.is_empty() || *c == "." || *c == "..") {
        return Err("a key component may not be empty, '.' or '..'");
    }
    if marker {
        return Ok((comps, None));
    }
    let name = comps.pop().expect("a non-empty key without a trailing slash has a last component");
    Ok((comps, Some(name)))
}

// ── aws-chunked bodies ──────────────────────────────────────────────────────

/// Decoder for `Content-Encoding: aws-chunked` bodies (what current AWS SDKs
/// send by default to carry a trailing checksum): `hex-size[;ext]\r\n`, the
/// data, `\r\n`, repeated; then a zero-size chunk, optional trailer lines, and
/// an empty line. Chunk signatures and trailing checksums are not verified —
/// the gateway does not verify SigV4 at all. Data is handed on as slices of
/// the input, never copied.
#[derive(Default)]
pub struct AwsChunked {
    line: BytesMut,
    state: ChunkState,
}

#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
enum ChunkState {
    #[default]
    Header,
    Data(u64),
    DataEnd,
    Trailer,
    Done,
}

/// The longest header or trailer line accepted.
const MAX_CHUNK_LINE: usize = 4096;

impl AwsChunked {
    pub fn is_done(&self) -> bool {
        self.state == ChunkState::Done
    }

    /// Feed `input`; decoded data goes to `out`.
    pub fn push(&mut self, mut input: Bytes, out: &mut Vec<Bytes>) -> Result<(), &'static str> {
        while !input.is_empty() {
            match self.state {
                ChunkState::Done => return Err("data after the final chunk"),
                ChunkState::Data(left) => {
                    let n = (left as usize).min(input.len());
                    out.push(input.split_to(n));
                    self.state = if left as usize == n { ChunkState::DataEnd } else { ChunkState::Data(left - n as u64) };
                }
                ChunkState::Header | ChunkState::DataEnd | ChunkState::Trailer => {
                    let Some(line) = self.take_line(&mut input)? else { continue };
                    self.state = match self.state {
                        ChunkState::DataEnd if line.is_empty() => ChunkState::Header,
                        ChunkState::DataEnd => return Err("chunk data longer than its size"),
                        ChunkState::Header => {
                            let text = std::str::from_utf8(&line).map_err(|_| "bad chunk header")?;
                            let hex = text.split(';').next().unwrap_or("").trim();
                            let size = u64::from_str_radix(hex, 16).map_err(|_| "bad chunk size")?;
                            if size == 0 { ChunkState::Trailer } else { ChunkState::Data(size) }
                        }
                        ChunkState::Trailer if line.is_empty() => ChunkState::Done,
                        ChunkState::Trailer => ChunkState::Trailer,
                        _ => unreachable!(),
                    };
                }
            }
        }
        Ok(())
    }

    /// Accumulate up to and including `\r\n`; `Some(line)` without it once
    /// complete.
    fn take_line(&mut self, input: &mut Bytes) -> Result<Option<BytesMut>, &'static str> {
        match input.iter().position(|&b| b == b'\n') {
            Some(i) => {
                self.line.extend_from_slice(&input.split_to(i + 1));
                let mut line = self.line.split();
                if line.len() < 2 || line[line.len() - 2] != b'\r' {
                    return Err("chunk line not ended by CRLF");
                }
                line.truncate(line.len() - 2);
                Ok(Some(line))
            }
            None => {
                self.line.extend_from_slice(input);
                input.clear();
                if self.line.len() > MAX_CHUNK_LINE {
                    return Err("chunk header too long");
                }
                Ok(None)
            }
        }
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

    #[test]
    fn http_dates_round_trip() {
        assert_eq!(http_date(784_111_777), "Sun, 06 Nov 1994 08:49:37 GMT");
        assert_eq!(http_date(0), "Thu, 01 Jan 1970 00:00:00 GMT");
        assert_eq!(parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT"), Some(784_111_777));
        for t in [0, 1_709_164_800, 1_790_000_123] {
            assert_eq!(parse_http_date(&http_date(t)), Some(t));
        }
        assert_eq!(parse_http_date("2024-02-29T00:00:00.000Z"), None);
        assert_eq!(parse_http_date("Sun, 06 Nov 1994 08:49:37 PST"), None);
    }

    #[test]
    fn read_conditions_follow_rfc_order() {
        let e = "abc";
        let c = |m: Option<&str>, n: Option<&str>, ms: Option<i64>, us: Option<i64>| ReadConditions {
            if_match: m.map(Into::into),
            if_none_match: n.map(Into::into),
            if_modified_since: ms,
            if_unmodified_since: us,
        };
        assert_eq!(evaluate_read(&c(None, None, None, None), e, 100), CondOutcome::Proceed);
        assert_eq!(evaluate_read(&c(Some("\"abc\""), None, None, None), e, 100), CondOutcome::Proceed);
        assert_eq!(evaluate_read(&c(Some("\"x\", W/\"abc\""), None, None, None), e, 100), CondOutcome::Proceed);
        assert_eq!(evaluate_read(&c(Some("\"x\""), None, None, None), e, 100), CondOutcome::PreconditionFailed);
        assert_eq!(evaluate_read(&c(Some("*"), None, None, None), e, 100), CondOutcome::Proceed);
        // A true If-Match overrides a false If-Unmodified-Since.
        assert_eq!(evaluate_read(&c(Some("\"abc\""), None, None, Some(50)), e, 100), CondOutcome::Proceed);
        assert_eq!(evaluate_read(&c(None, None, None, Some(50)), e, 100), CondOutcome::PreconditionFailed);
        assert_eq!(evaluate_read(&c(None, Some("\"abc\""), None, None), e, 100), CondOutcome::NotModified);
        assert_eq!(evaluate_read(&c(None, Some("\"x\""), Some(200), None), e, 100), CondOutcome::Proceed);
        assert_eq!(evaluate_read(&c(None, None, Some(200), None), e, 100), CondOutcome::NotModified);
        assert_eq!(evaluate_read(&c(None, None, Some(50), None), e, 100), CondOutcome::Proceed);
    }

    #[test]
    fn complete_and_delete_bodies_parse() {
        let x = r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Part><ETag>"aa"</ETag><PartNumber>1</PartNumber></Part>
  <Part><PartNumber>2</PartNumber><ETag>&quot;bb&quot;</ETag><ChecksumCRC32>x</ChecksumCRC32></Part>
</CompleteMultipartUpload>"#;
        assert_eq!(parse_complete_xml(x).unwrap(), vec![(1, "\"aa\"".into()), (2, "\"bb\"".into())]);
        assert!(parse_complete_xml("<Other/>").is_err());
        assert!(parse_complete_xml("<CompleteMultipartUpload><Part><PartNumber>x</PartNumber><ETag>a</ETag></Part></CompleteMultipartUpload>").is_err());

        let d = r#"<Delete><Quiet>true</Quiet><Object><Key>a/b&amp;c.lance</Key></Object><Object><Key>d&#x20;e</Key><VersionId>null</VersionId></Object></Delete>"#;
        assert_eq!(
            parse_delete_xml(d).unwrap(),
            DeleteRequest { quiet: true, keys: vec!["a/b&c.lance".into(), "d e".into()] }
        );
        assert!(parse_delete_xml("<Delete></Delete>").is_err());
        let many: String = (0..1001).map(|i| format!("<Object><Key>k{i}</Key></Object>")).collect();
        assert!(parse_delete_xml(&format!("<Delete>{many}</Delete>")).is_err());
    }

    #[test]
    fn response_bodies_escape_keys() {
        let x = delete_result_xml(
            &[
                DeleteOutcome::Deleted("a&b".into()),
                DeleteOutcome::Failed { key: "c".into(), code: "InternalError", message: "x<y".into() },
            ],
            false,
        );
        assert!(x.contains("<Deleted><Key>a&amp;b</Key></Deleted>"));
        assert!(x.contains("<Error><Key>c</Key><Code>InternalError</Code><Message>x&lt;y</Message></Error>"));
        assert!(!delete_result_xml(&[DeleteOutcome::Deleted("a".into())], true).contains("<Deleted>"));
        assert!(complete_multipart_xml("b", "k&1", "e").contains("<Key>k&amp;1</Key><ETag>&quot;e&quot;</ETag>"));
    }

    #[test]
    fn identifiers_and_keys() {
        assert_eq!(parse_upload_id(&format_upload_id(0xabc)), Some(0xabc));
        assert_eq!(parse_upload_id("abc"), None);
        assert_eq!(parse_copy_source("/src/a%20b/c.lance?versionId=x"), Some(("src".into(), "a b/c.lance".into())));
        assert_eq!(parse_copy_source("src/k"), Some(("src".into(), "k".into())));
        assert_eq!(parse_copy_source("/src"), None);
        assert_eq!(key_path("a/b/c.lance"), Ok((vec!["a", "b"], Some("c.lance"))));
        assert_eq!(key_path("c"), Ok((vec![], Some("c"))));
        assert_eq!(key_path("a/b/"), Ok((vec!["a", "b"], None)));
        assert!(key_path("a//b").is_err());
        assert!(key_path("a/../b").is_err());
        assert!(key_path("").is_err());
    }

    #[test]
    fn aws_chunked_decodes_across_arbitrary_splits() {
        let body = b"5;chunk-signature=ab\r\nhello\r\n6\r\n world\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n";
        for split in 1..body.len() {
            let mut d = AwsChunked::default();
            let mut out = Vec::new();
            for piece in body.chunks(split) {
                d.push(Bytes::copy_from_slice(piece), &mut out).unwrap();
            }
            assert!(d.is_done(), "split {split}");
            let got: Vec<u8> = out.iter().flat_map(|b| b.iter().copied()).collect();
            assert_eq!(got, b"hello world", "split {split}");
        }
        let mut d = AwsChunked::default();
        let mut out = Vec::new();
        assert!(d.push(Bytes::from_static(b"3\r\nabcd\r\n"), &mut out).is_err(), "data longer than its size");
        let mut d = AwsChunked::default();
        assert!(d.push(Bytes::from_static(b"zz\r\n"), &mut out).is_err());
    }
}
