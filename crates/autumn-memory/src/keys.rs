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

/// Prefix covering ALL of one agent's private memory, RELATIVE to the client's
/// `mem/{tenant}/` binding scope (D7 Prepend-only: the `ClusterClient`
/// owns the `mem/{tenant}/` prefix and prepends it, so scope is locked by
/// construction). `_tenant` is now owned by the binding and ignored here — kept
/// on the signature only so the many callers don't churn; the full param removal
/// is a follow-up.
pub fn agent_prefix(_tenant: &str, agent: &str) -> Vec<u8> {
    format!("{}/", q(agent)).into_bytes()
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

/// Cross-agent shared prefix, RELATIVE to `mem/{tenant}/` (see `agent_prefix`).
pub fn shared_prefix(_tenant: &str, namespace: Option<&str>) -> Vec<u8> {
    let mut v = b"shared/".to_vec();
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

/// Prefix covering ALL of an agent's reverse pointers — the reconcile scan.
pub fn ivf_vptr_prefix(tenant: &str, agent: &str) -> Vec<u8> {
    let mut v = agent_prefix(tenant, agent);
    v.extend_from_slice(b"ivf_meta/vptr/");
    v
}

/// Reverse pointer `vec_id -> centroid` (value = 4-byte BE centroid) so deletion
/// can reap a vector's IVF posting in O(1) — no full-bucket scan. Lives under
/// `ivf_meta/` (NOT the `ivf/` posting range), so it is invisible to bucket /
/// rebuild scans (`ivf_all_prefix`).
pub fn ivf_vptr_key(tenant: &str, agent: &str, vec_id: &str) -> Vec<u8> {
    let mut v = ivf_vptr_prefix(tenant, agent);
    v.extend_from_slice(q(vec_id).as_bytes());
    v
}

/// Recover `vec_id` from a vptr key given the vptr prefix.
pub fn ivf_vptr_vec_id(key: &[u8], vptr_prefix: &[u8]) -> String {
    let tail = &key[vptr_prefix.len().min(key.len())..];
    unq(&String::from_utf8_lossy(tail))
}

// --------------------------------------------------- graph: nodes + edges ---
// A generic node/edge graph as adjacency lists. Every component is a string, so
// (like the BM25 posting keys) each is `q()`-encoded and `/`-separated — a `/`
// inside any component is escaped and can never forge a separator. Four
// families, none a byte-prefix of another's scan range (`node/` vs `nidx/` vs
// `edge/` vs `redge/` diverge in their first 1-3 bytes):
//   node/{id}                 -> NodeRecord (authoritative)
//   nidx/{kind}/{id}          -> existence marker (list nodes by kind)
//   edge/{src}/{type}/{dst}   -> edge attrs (authoritative forward edge)
//   redge/{dst}/{type}/{src}  -> existence marker (reverse index / hint)

/// Split a key tail (after `prefix`) into its first two `q()`-encoded, `/`-
/// separated components. Safe because `q` escapes any `/` inside a component,
/// so the FIRST literal `/` is always the true separator.
fn split2(key: &[u8], prefix: &[u8]) -> Option<(String, String)> {
    let tail = key.get(prefix.len()..)?;
    let s = std::str::from_utf8(tail).ok()?;
    let slash = s.find('/')?;
    Some((unq(&s[..slash]), unq(&s[slash + 1..])))
}

/// Prefix covering ALL of an agent's node records — the reconcile scan.
pub fn node_prefix(tenant: &str, agent: &str) -> Vec<u8> {
    let mut v = agent_prefix(tenant, agent);
    v.extend_from_slice(b"node/");
    v
}

/// Key for one node record (authoritative).
pub fn node_key(tenant: &str, agent: &str, id: &str) -> Vec<u8> {
    let mut v = node_prefix(tenant, agent);
    v.extend_from_slice(q(id).as_bytes());
    v
}

/// Recover a node id from a node key given the node prefix.
pub fn node_id_name(key: &[u8], node_prefix: &[u8]) -> String {
    let tail = &key[node_prefix.len().min(key.len())..];
    unq(&String::from_utf8_lossy(tail))
}

/// Prefix covering one kind's node index.
pub fn nidx_kind_prefix(tenant: &str, agent: &str, kind: &str) -> Vec<u8> {
    let mut v = agent_prefix(tenant, agent);
    v.extend_from_slice(format!("nidx/{}/", q(kind)).as_bytes());
    v
}

/// Existence marker: node `id` has kind `kind` (empty value).
pub fn nidx_key(tenant: &str, agent: &str, kind: &str, id: &str) -> Vec<u8> {
    let mut v = nidx_kind_prefix(tenant, agent, kind);
    v.extend_from_slice(q(id).as_bytes());
    v
}

/// Recover the node id from a nidx key given its kind prefix.
pub fn nidx_id_name(key: &[u8], kind_prefix: &[u8]) -> String {
    let tail = &key[kind_prefix.len().min(key.len())..];
    unq(&String::from_utf8_lossy(tail))
}

/// Prefix covering ALL of an agent's forward edges — the reconcile scan.
pub fn edge_all_prefix(tenant: &str, agent: &str) -> Vec<u8> {
    let mut v = agent_prefix(tenant, agent);
    v.extend_from_slice(b"edge/");
    v
}

/// Prefix covering all of `src`'s outgoing edges.
pub fn edge_src_prefix(tenant: &str, agent: &str, src: &str) -> Vec<u8> {
    let mut v = edge_all_prefix(tenant, agent);
    v.extend_from_slice(format!("{}/", q(src)).as_bytes());
    v
}

/// Prefix covering `src`'s outgoing edges of one type.
pub fn edge_src_type_prefix(tenant: &str, agent: &str, src: &str, etype: &str) -> Vec<u8> {
    let mut v = edge_src_prefix(tenant, agent, src);
    v.extend_from_slice(format!("{}/", q(etype)).as_bytes());
    v
}

/// Forward edge key `edge/{src}/{type}/{dst}` (value = edge attrs; authoritative).
pub fn edge_key(tenant: &str, agent: &str, src: &str, etype: &str, dst: &str) -> Vec<u8> {
    let mut v = edge_src_type_prefix(tenant, agent, src, etype);
    v.extend_from_slice(q(dst).as_bytes());
    v
}

/// Recover `(etype, dst)` from a forward-edge key given its `src` prefix.
pub fn edge_parse_tail(key: &[u8], src_prefix: &[u8]) -> Option<(String, String)> {
    split2(key, src_prefix)
}

/// Recover `(src, etype, dst)` from a forward-edge key given the ALL prefix.
pub fn edge_all_parse(key: &[u8], all_prefix: &[u8]) -> Option<(String, String, String)> {
    let tail = key.get(all_prefix.len()..)?;
    let s = std::str::from_utf8(tail).ok()?;
    let mut it = s.splitn(3, '/');
    let src = it.next()?;
    let etype = it.next()?;
    let dst = it.next()?;
    Some((unq(src), unq(etype), unq(dst)))
}

/// Prefix covering ALL of an agent's reverse-edge markers — the reconcile scan.
pub fn redge_all_prefix(tenant: &str, agent: &str) -> Vec<u8> {
    let mut v = agent_prefix(tenant, agent);
    v.extend_from_slice(b"redge/");
    v
}

/// Prefix covering all of `dst`'s incoming edges (reverse markers).
pub fn redge_dst_prefix(tenant: &str, agent: &str, dst: &str) -> Vec<u8> {
    let mut v = redge_all_prefix(tenant, agent);
    v.extend_from_slice(format!("{}/", q(dst)).as_bytes());
    v
}

/// Prefix covering `dst`'s incoming edges of one type.
pub fn redge_dst_type_prefix(tenant: &str, agent: &str, dst: &str, etype: &str) -> Vec<u8> {
    let mut v = redge_dst_prefix(tenant, agent, dst);
    v.extend_from_slice(format!("{}/", q(etype)).as_bytes());
    v
}

/// Reverse-edge marker `redge/{dst}/{type}/{src}` (empty value; a hint).
pub fn redge_key(tenant: &str, agent: &str, src: &str, etype: &str, dst: &str) -> Vec<u8> {
    let mut v = redge_dst_type_prefix(tenant, agent, dst, etype);
    v.extend_from_slice(q(src).as_bytes());
    v
}

/// Recover `(etype, src)` from a reverse-edge key given its `dst` prefix.
pub fn redge_parse_tail(key: &[u8], dst_prefix: &[u8]) -> Option<(String, String)> {
    split2(key, dst_prefix)
}

/// Recover `(dst, etype, src)` from a reverse-edge key given the ALL prefix.
pub fn redge_all_parse(key: &[u8], all_prefix: &[u8]) -> Option<(String, String, String)> {
    let tail = key.get(all_prefix.len()..)?;
    let s = std::str::from_utf8(tail).ok()?;
    let mut it = s.splitn(3, '/');
    let dst = it.next()?;
    let etype = it.next()?;
    let src = it.next()?;
    Some((unq(dst), unq(etype), unq(src)))
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
    fn graph_key_roundtrip() {
        // node id with a slash round-trips and is isolated from other families.
        let np = node_prefix("acme", "a1");
        let nk = node_key("acme", "a1", "pkg/mod::Foo");
        assert!(nk.starts_with(&np));
        assert_eq!(node_id_name(&nk, &np), "pkg/mod::Foo");

        // forward edge with '/' in src and dst components.
        let eall = edge_all_prefix("acme", "a1");
        let sp = edge_src_prefix("acme", "a1", "a/b");
        let ek = edge_key("acme", "a1", "a/b", "CALLS", "c/d");
        assert!(ek.starts_with(&sp) && ek.starts_with(&eall));
        assert_eq!(edge_parse_tail(&ek, &sp), Some(("CALLS".into(), "c/d".into())));
        assert_eq!(
            edge_all_parse(&ek, &eall),
            Some(("a/b".into(), "CALLS".into(), "c/d".into()))
        );
        // a src "a/b" edge must not leak into src "a"'s outgoing scan.
        assert!(!sp.starts_with(&edge_src_prefix("acme", "a1", "a")));

        // reverse edge mirrors, keyed by dst.
        let rall = redge_all_prefix("acme", "a1");
        let rdp = redge_dst_prefix("acme", "a1", "c/d");
        let rk = redge_key("acme", "a1", "a/b", "CALLS", "c/d");
        assert!(rk.starts_with(&rdp) && rk.starts_with(&rall));
        assert_eq!(redge_parse_tail(&rk, &rdp), Some(("CALLS".into(), "a/b".into())));
        assert_eq!(
            redge_all_parse(&rk, &rall),
            Some(("c/d".into(), "CALLS".into(), "a/b".into()))
        );

        // cross-family isolation: no scan range catches another family.
        assert!(!nk.starts_with(&eall) && !ek.starts_with(&np) && !ek.starts_with(&rall));
        let nik = nidx_key("acme", "a1", "Function", "a/b");
        assert!(!nik.starts_with(&np), "nidx must not be caught by the node scan");
        assert_eq!(nidx_id_name(&nik, &nidx_kind_prefix("acme", "a1", "Function")), "a/b");
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
