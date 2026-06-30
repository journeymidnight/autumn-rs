//! Key-namespace schema for autumn-memory.
//!
//! Memory records share the autumn KV namespace with the other interfaces
//! (fuse / kvcache / client), under a reserved `mem/` prefix. See
//! `docs/autumn_memory_plan.md` §6. Layout:
//!
//! ```text
//! episodic:  mem/{tenant}/{agent}/ep/{session}/{suffix}   -> event bytes
//! fact:      mem/{tenant}/{agent}/fact/{namespace}/{key}  -> fact bytes
//! shared:    mem/{tenant}/shared/{namespace}/{key}        -> cross-agent
//! ```
//!
//! Structural separators are literal `/`; every dynamic component (tenant,
//! agent, session, namespace, fact key) is percent-encoded so a component that
//! itself contains `/` can never be confused with a separator and break a
//! prefix scan. The episodic `{suffix}` is a 12-byte BINARY tail
//!
//! ```text
//! suffix = BE(u64::MAX - ts_ns)  ++  BE(u32::MAX - counter)
//! ```
//!
//! inverting both the ns timestamp and the per-store counter so a plain
//! ascending range scan yields NEWEST-FIRST order (plan §6). The counter
//! breaks ties when two appends land in the same nanosecond.

/// Reserved namespace prefix — distinguishes agent-memory keys from
/// fuse / kvcache / client keys on the shared cluster.
pub const NS: &str = "mem";

/// Width of the episodic binary suffix (8-byte inv ts + 4-byte inv counter).
pub const EP_SUFFIX_LEN: usize = 12;

/// Percent-encode one dynamic key component (encodes `/` and everything else
/// outside the RFC 3986 unreserved set).
pub fn q(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => {
                out.push('%');
                out.push(hex_upper(b >> 4));
                out.push(hex_upper(b & 0x0f));
            }
        }
    }
    out
}

/// Inverse of [`q`].
pub fn unq(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_upper(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        _ => (b'A' + (n - 10)) as char,
    }
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Prefix covering ALL of one agent's private memory.
pub fn agent_prefix(tenant: &str, agent: &str) -> Vec<u8> {
    format!("{}/{}/{}/", NS, q(tenant), q(agent)).into_bytes()
}

// ----------------------------------------------------------------- episodic --

/// Prefix covering one session's event log (range-scannable, newest-first).
pub fn episodic_prefix(tenant: &str, agent: &str, session: &str) -> Vec<u8> {
    let mut v = agent_prefix(tenant, agent);
    v.extend_from_slice(format!("ep/{}/", q(session)).as_bytes());
    v
}

/// The 12-byte newest-first binary suffix.
pub fn episodic_suffix(ts_ns: u64, counter: u32) -> [u8; EP_SUFFIX_LEN] {
    let inv_ts = (u64::MAX - ts_ns).to_be_bytes();
    let inv_ctr = (u32::MAX - counter).to_be_bytes();
    let mut s = [0u8; EP_SUFFIX_LEN];
    s[..8].copy_from_slice(&inv_ts);
    s[8..].copy_from_slice(&inv_ctr);
    s
}

pub fn episodic_key(tenant: &str, agent: &str, session: &str, ts_ns: u64, counter: u32) -> Vec<u8> {
    let mut v = episodic_prefix(tenant, agent, session);
    v.extend_from_slice(&episodic_suffix(ts_ns, counter));
    v
}

/// Recover the wall-clock ns timestamp from an episodic key given its prefix.
pub fn parse_episodic_ts(key: &[u8], prefix: &[u8]) -> Option<u64> {
    let suffix = key.get(prefix.len()..)?;
    if suffix.len() < 8 {
        return None;
    }
    let inv = u64::from_be_bytes(suffix[..8].try_into().ok()?);
    Some(u64::MAX - inv)
}

// -------------------------------------------------------------------- facts --

/// Prefix covering one (agent, namespace) fact set — point-get + list.
pub fn fact_prefix(tenant: &str, agent: &str, namespace: &str) -> Vec<u8> {
    let mut v = agent_prefix(tenant, agent);
    v.extend_from_slice(format!("fact/{}/", q(namespace)).as_bytes());
    v
}

pub fn fact_key(tenant: &str, agent: &str, namespace: &str, key: &str) -> Vec<u8> {
    let mut v = fact_prefix(tenant, agent, namespace);
    v.extend_from_slice(q(key).as_bytes());
    v
}

/// Recover the original fact key string from a stored key + its prefix.
pub fn fact_key_name(key: &[u8], prefix: &[u8]) -> String {
    let tail = &key[prefix.len().min(key.len())..];
    unq(&String::from_utf8_lossy(tail))
}

// ------------------------------------------------------------------- shared --

pub fn shared_prefix(tenant: &str, namespace: Option<&str>) -> Vec<u8> {
    let mut v = format!("{}/{}/shared/", NS, q(tenant)).into_bytes();
    if let Some(ns) = namespace {
        v.extend_from_slice(format!("{}/", q(ns)).as_bytes());
    }
    v
}

pub fn shared_key(tenant: &str, namespace: &str, key: &str) -> Vec<u8> {
    let mut v = shared_prefix(tenant, Some(namespace));
    v.extend_from_slice(q(key).as_bytes());
    v
}

// ----------------------------------------------- recall: docs + inverted ---

/// Prefix covering all indexed memory documents for an agent.
pub fn doc_prefix(tenant: &str, agent: &str) -> Vec<u8> {
    let mut v = agent_prefix(tenant, agent);
    v.extend_from_slice(b"doc/");
    v
}

/// Key for one indexed memory document (the authoritative record).
pub fn doc_key(tenant: &str, agent: &str, doc_id: &str) -> Vec<u8> {
    let mut v = doc_prefix(tenant, agent);
    v.extend_from_slice(q(doc_id).as_bytes());
    v
}

/// Prefix covering one term's inverted posting list.
pub fn idx_term_prefix(tenant: &str, agent: &str, term: &str) -> Vec<u8> {
    let mut v = agent_prefix(tenant, agent);
    v.extend_from_slice(format!("idx/{}/", q(term)).as_bytes());
    v
}

/// Posting key: an existence marker that `term` occurs in `doc_id`
/// (empty value — discovery is a keys-only range scan).
pub fn idx_posting_key(tenant: &str, agent: &str, term: &str, doc_id: &str) -> Vec<u8> {
    let mut v = idx_term_prefix(tenant, agent, term);
    v.extend_from_slice(q(doc_id).as_bytes());
    v
}

/// Recover the `doc_id` from a posting key given its term prefix.
pub fn idx_posting_doc_id(key: &[u8], term_prefix: &[u8]) -> String {
    let tail = &key[term_prefix.len().min(key.len())..];
    unq(&String::from_utf8_lossy(tail))
}

/// BM25 corpus stats key (n_docs + sum_doc_len) for idf / length-norm.
pub fn stats_key(tenant: &str, agent: &str) -> Vec<u8> {
    let mut v = agent_prefix(tenant, agent);
    v.extend_from_slice(b"meta/stats");
    v
}

// ------------------------------------------------ recall: IVF vector index ---
// Bucket id is a fixed 4-byte BE centroid index, so `ivf/{4 BE}{vec_id}` is
// parsed by offset (the 4 bytes may contain 0x2F — never treated as a sep).
// `ivf_meta/...` does NOT collide with the `ivf/` prefix ("ivf_" != "ivf/").

/// Prefix covering ALL of an agent's IVF postings (every bucket) — the
/// brute-force / rebuild scan range.
pub fn ivf_all_prefix(tenant: &str, agent: &str) -> Vec<u8> {
    let mut v = agent_prefix(tenant, agent);
    v.extend_from_slice(b"ivf/");
    v
}

/// Prefix covering ONE centroid's posting list (bucket).
pub fn ivf_bucket_prefix(tenant: &str, agent: &str, centroid: u32) -> Vec<u8> {
    let mut v = ivf_all_prefix(tenant, agent);
    v.extend_from_slice(&centroid.to_be_bytes());
    v
}

/// Posting key: `vec_id` lives in `centroid`'s bucket. Value = the vector.
pub fn ivf_posting_key(tenant: &str, agent: &str, centroid: u32, vec_id: &str) -> Vec<u8> {
    let mut v = ivf_bucket_prefix(tenant, agent, centroid);
    v.extend_from_slice(q(vec_id).as_bytes());
    v
}

/// Recover `vec_id` from a posting key given its BUCKET prefix (`ivf/`+4 bytes).
pub fn ivf_vec_id(key: &[u8], bucket_prefix: &[u8]) -> String {
    let tail = &key[bucket_prefix.len().min(key.len())..];
    unq(&String::from_utf8_lossy(tail))
}

/// From an ALL-bucket scan key (`ivf/`-prefixed), read the centroid id.
pub fn ivf_all_centroid(key: &[u8], all_prefix: &[u8]) -> Option<u32> {
    let tail = key.get(all_prefix.len()..)?;
    if tail.len() < 4 {
        return None;
    }
    Some(u32::from_be_bytes(tail[..4].try_into().ok()?))
}

/// From an ALL-bucket scan key, recover the `vec_id` (after the 4 centroid bytes).
pub fn ivf_all_vec_id(key: &[u8], all_prefix: &[u8]) -> Option<String> {
    let tail = key.get(all_prefix.len()..)?;
    if tail.len() < 4 {
        return None;
    }
    Some(unq(&String::from_utf8_lossy(&tail[4..])))
}

/// The IVF centroid manifest key.
pub fn ivf_centroids_key(tenant: &str, agent: &str) -> Vec<u8> {
    let mut v = agent_prefix(tenant, agent);
    v.extend_from_slice(b"ivf_meta/centroids");
    v
}

/// Reverse pointer `vec_id -> centroid` (value = 4-byte BE centroid) so deletion
/// can reap a vector's IVF posting in O(1) — no full-bucket scan. Lives under
/// `ivf_meta/` (NOT the `ivf/` posting range), so it is invisible to bucket /
/// rebuild scans (`ivf_all_prefix`).
pub fn ivf_vptr_key(tenant: &str, agent: &str, vec_id: &str) -> Vec<u8> {
    let mut v = agent_prefix(tenant, agent);
    v.extend_from_slice(b"ivf_meta/vptr/");
    v.extend_from_slice(q(vec_id).as_bytes());
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_roundtrip() {
        for s in ["plain", "a/b", "x y/z", "key=val&x", "中文/路径", "100%"] {
            assert_eq!(unq(&q(s)), s, "roundtrip {s}");
        }
        assert_eq!(q("a/b"), "a%2Fb");
        assert!(!q("a/b").contains('/'), "encoded component must not contain a separator");
    }

    #[test]
    fn fact_key_roundtrip() {
        let prefix = fact_prefix("acme", "agent-1", "profile");
        let k = fact_key("acme", "agent-1", "profile", "home/address");
        assert!(k.starts_with(&prefix));
        assert_eq!(fact_key_name(&k, &prefix), "home/address");
    }

    #[test]
    fn posting_key_roundtrip() {
        let tprefix = idx_term_prefix("acme", "agent-1", "error");
        let pk = idx_posting_key("acme", "agent-1", "error", "doc/77");
        assert!(pk.starts_with(&tprefix));
        assert_eq!(idx_posting_doc_id(&pk, &tprefix), "doc/77");
        // term with a slash cannot bleed into another term's posting list
        let weird = idx_term_prefix("acme", "agent-1", "a/b");
        assert!(!weird.starts_with(&idx_term_prefix("acme", "agent-1", "a")));
    }

    #[test]
    fn ivf_key_roundtrip() {
        let all = ivf_all_prefix("acme", "agent-1");
        // bucket id 0x2F2F2F2F's BE bytes are literal `////` — must NOT confuse parsing
        let bucket = 0x2F2F2F2Fu32;
        let bprefix = ivf_bucket_prefix("acme", "agent-1", bucket);
        let pk = ivf_posting_key("acme", "agent-1", bucket, "vec-9");
        assert!(pk.starts_with(&bprefix) && pk.starts_with(&all));
        assert_eq!(ivf_vec_id(&pk, &bprefix), "vec-9");
        assert_eq!(ivf_all_centroid(&pk, &all), Some(bucket));
        assert_eq!(ivf_all_vec_id(&pk, &all).as_deref(), Some("vec-9"));
        // the centroid manifest key is NOT caught by the `ivf/` bucket scan
        assert!(!ivf_centroids_key("acme", "agent-1").starts_with(&all));
        // the reverse pointer is under ivf_meta/ — also OUT of the bucket scan
        // (so train's ivf_all_prefix scan never mistakes a vptr for a posting)
        let vptr = ivf_vptr_key("acme", "agent-1", "vec-9");
        assert!(!vptr.starts_with(&all), "vptr must not be in the ivf/ scan range");
    }

    #[test]
    fn episodic_is_newest_first() {
        // larger ts (newer) must sort BEFORE smaller ts (older).
        let older = episodic_key("t", "a", "s", 1_000, 0);
        let newer = episodic_key("t", "a", "s", 2_000, 0);
        assert!(newer < older, "newer event must sort first (newest-first)");
        // same ts: larger counter (later) sorts before smaller (newest-first).
        let first = episodic_key("t", "a", "s", 5_000, 0);
        let second = episodic_key("t", "a", "s", 5_000, 1);
        assert!(second < first, "later same-ts event must sort first");
    }

    #[test]
    fn episodic_ts_roundtrip() {
        let prefix = episodic_prefix("t", "a", "s");
        let key = episodic_key("t", "a", "s", 123_456_789, 7);
        assert_eq!(parse_episodic_ts(&key, &prefix), Some(123_456_789));
    }

    #[test]
    fn agent_prefix_isolation() {
        let a_key = episodic_key("acme", "agent-A", "s", 1, 0);
        let b_prefix = agent_prefix("acme", "agent-B");
        assert!(!a_key.starts_with(&b_prefix), "agent A key must not match agent B prefix");
        // a component containing a slash cannot forge another agent's prefix
        let evil = agent_prefix("acme", "agent-A/../agent-B");
        assert!(!evil.starts_with(&agent_prefix("acme", "agent-B")));
    }
}
