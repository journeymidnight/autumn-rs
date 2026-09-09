//! Optional lightweight text→vector embedder for the vector / hybrid legs.
//!
//! autumn-memory itself takes caller-supplied vectors (`index_vector` /
//! `search_vector` want a `&[f32]`) — production feeds them from a shared
//! sglang/vLLM endpoint. This convenience module gives callers that DON'T want
//! to stand up a model server a built-in embedder:
//!
//!   * [`HashEmbedder`] — zero-dep, always available. Signed-FNV bag-of-words
//!     hashing. Deterministic, reproducible; real plumbing, weak semantics.
//!   * [`StaticTableEmbedder`] — a Model2Vec-style static int8 lookup table
//!     (feature `static-embed`): tokenize → int8 row lookup → dequant →
//!     mean-pool. Real semantics, no network, no GPU.
//!   * [`OpenAiEmbedder`] — any server speaking OpenAI's `/v1/embeddings`
//!     (feature `openai-embed`): llama.cpp on a CPU, vLLM, sglang, or the
//!     vendor. Real model, real cost, over the network.
//!
//! An enum ([`Embedder`]) dispatches between them, and every variant emits an
//! **L2-normalized** vector so scores stay comparable. The built-in embedders
//! emit `EMBED_DIM`; an external model emits whatever it emits, and the vector
//! index stores the width per record, so nothing here has to agree with 256.
//!
//! One thing this module does NOT do is stop you mixing them. Vectors written
//! by one embedder and searched with another are silent nonsense, not an error
//! — the store takes `&[f32]` and cannot tell whose. Re-index when you switch.

use std::fmt;

/// Output dimension every embedder honors. 256 matches Model2Vec
/// `potion-base-8M`, so a static table needs no reprojection.
pub const EMBED_DIM: usize = 256;

/// Error from the (fallible) static-table embedder — loading a table/tokenizer
/// or tokenizing. `HashEmbedder` never fails.
#[derive(Debug)]
pub struct EmbedError(pub String);

impl fmt::Display for EmbedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for EmbedError {}
impl From<std::io::Error> for EmbedError {
    fn from(e: std::io::Error) -> Self {
        EmbedError(e.to_string())
    }
}

fn l2_normalize(mut v: Vec<f32>) -> Vec<f32> {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in v.iter_mut() {
            *x /= norm;
        }
    }
    v
}

fn tokenize(text: &str) -> impl Iterator<Item = String> + '_ {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
}

// ---------------------------------------------------------------------------
// Hash embedder (default, zero deps)
// ---------------------------------------------------------------------------

pub struct HashEmbedder;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h = FNV_OFFSET;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

impl HashEmbedder {
    pub fn embed(&self, text: &str) -> Vec<f32> {
        let mut acc = vec![0.0f32; EMBED_DIM];
        for tok in tokenize(text) {
            let h = fnv1a(tok.as_bytes());
            let bucket = (h % EMBED_DIM as u64) as usize;
            acc[bucket] += if (h >> 63) & 1 == 1 { 1.0 } else { -1.0 };
        }
        l2_normalize(acc)
    }
}

// ---------------------------------------------------------------------------
// Static int8 table embedder (Model2Vec-style), feature `static-embed`
// ---------------------------------------------------------------------------

#[cfg(feature = "static-embed")]
pub struct StaticTableEmbedder {
    vocab: usize,
    dim: usize,
    scale: f32,
    table: Vec<u8>, // int8 rows, read as i8
    tokenizer: tokenizers::Tokenizer,
}

#[cfg(feature = "static-embed")]
impl StaticTableEmbedder {
    /// `M2VS` format: [u8;4 "M2VS"][u32 version][u32 vocab][u32 dim][f32 scale][i8 vocab*dim]
    pub fn load(model_path: &str, tokenizer_path: &str) -> Result<Self, EmbedError> {
        let bytes = std::fs::read(model_path)?;
        if bytes.len() < 20 || &bytes[0..4] != b"M2VS" {
            return Err(EmbedError(format!("{model_path}: not an M2VS table")));
        }
        let rd = |o: usize| u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
        let vocab = rd(8) as usize;
        let dim = rd(12) as usize;
        let scale = f32::from_le_bytes(bytes[16..20].try_into().unwrap());
        if dim != EMBED_DIM {
            return Err(EmbedError(format!("{model_path}: dim {dim} != EMBED_DIM {EMBED_DIM}")));
        }
        let want = 20 + vocab * dim;
        if bytes.len() < want {
            return Err(EmbedError(format!("{model_path}: truncated")));
        }
        let tokenizer = tokenizers::Tokenizer::from_file(tokenizer_path)
            .map_err(|e| EmbedError(format!("tokenizer {tokenizer_path}: {e}")))?;
        Ok(Self {
            vocab,
            dim,
            scale,
            table: bytes[20..want].to_vec(),
            tokenizer,
        })
    }

    pub fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        let enc = self
            .tokenizer
            .encode(text, false)
            .map_err(|e| EmbedError(format!("tokenize: {e}")))?;
        let mut acc = vec![0.0f32; self.dim];
        let mut count = 0usize;
        for &id in enc.get_ids() {
            let id = id as usize;
            if id >= self.vocab {
                continue;
            }
            let row = &self.table[id * self.dim..(id + 1) * self.dim];
            for (a, &b) in acc.iter_mut().zip(row) {
                *a += (b as i8) as f32 * self.scale;
            }
            count += 1;
        }
        if count > 0 {
            let inv = 1.0 / count as f32;
            for a in acc.iter_mut() {
                *a *= inv;
            }
        }
        Ok(l2_normalize(acc))
    }
}

// ---------------------------------------------------------------------------
// OpenAI-compatible embedder (feature `openai-embed`)
// ---------------------------------------------------------------------------

/// Embeddings from an external server speaking OpenAI's `/v1/embeddings`.
///
/// The point of this variant is that the model is somebody else's problem: run
/// llama.cpp on spare CPU, point this at it, and the vector leg becomes worth
/// using. [`HashEmbedder`] exists to exercise the plumbing, not to retrieve —
/// see [`Embedder::is_semantic`].
#[cfg(feature = "openai-embed")]
pub struct OpenAiEmbedder {
    client: cyper::Client,
    url: String,
    model: String,
    api_key: Option<String>,
    timeout: std::time::Duration,
    /// What the server actually returned last, so `dim()` reports the truth
    /// rather than a number the caller guessed. `0` until the first call.
    dim: std::cell::Cell<usize>,
}

#[cfg(feature = "openai-embed")]
impl OpenAiEmbedder {
    /// `base` may be the server root, the `/v1` prefix, or the full endpoint —
    /// all three are things people paste, and guessing wrong costs a 404 that
    /// reads like the model is missing.
    pub fn new(base: &str, model: &str) -> Self {
        Self {
            client: cyper::Client::new(),
            url: normalize_embeddings_url(base),
            model: model.to_string(),
            api_key: None,
            timeout: std::time::Duration::from_secs(60),
            dim: std::cell::Cell::new(0),
        }
    }

    pub fn with_api_key(mut self, key: impl Into<String>) -> Self {
        let k = key.into();
        self.api_key = if k.is_empty() { None } else { Some(k) };
        self
    }

    pub fn with_timeout(mut self, d: std::time::Duration) -> Self {
        self.timeout = d;
        self
    }

    pub fn endpoint(&self) -> &str {
        &self.url
    }

    pub fn dim(&self) -> usize {
        self.dim.get()
    }

    pub async fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        let mut v = self.embed_batch(&[text]).await?;
        if v.is_empty() {
            return Err(EmbedError("embeddings response held no vector".into()));
        }
        Ok(v.remove(0))
    }

    /// One request for many texts, because the endpoint is batch-shaped and an
    /// index run is a loop. A caller that embeds one at a time pays a round
    /// trip per document.
    pub async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let body = serde_json::json!({ "model": self.model, "input": texts });

        let mut req = self
            .client
            .post(&self.url)
            .map_err(|e| EmbedError(format!("bad embeddings url {}: {e}", self.url)))?
            .json(&body)
            .map_err(|e| EmbedError(format!("encode embeddings request: {e}")))?;
        if let Some(k) = &self.api_key {
            req = req
                .bearer_auth(k)
                .map_err(|e| EmbedError(format!("bad api key: {e}")))?;
        }

        // cyper has no timeout of its own, and the failure this guards against
        // is not a refused connection — it is a server that accepts and then
        // never answers, which without this stalls the caller with no error.
        //
        // The BODY read is inside the timeout too, and that is not a detail:
        // "sends headers, then stops" is a distinct hang from "accepts, then
        // stops", and a timeout around only the send covers neither the second
        // one nor a body that arrives a byte a minute.
        let exchange = async {
            let resp = req
                .send()
                .await
                .map_err(|e| EmbedError(format!("embeddings request to {}: {e}", self.url)))?;
            let status = resp.status();
            let text = resp
                .text()
                .await
                .map_err(|e| EmbedError(format!("read embeddings response: {e}")))?;
            Ok::<_, EmbedError>((status, text))
        };
        let (status, text) = compio::time::timeout(self.timeout, exchange)
            .await
            .map_err(|_| {
                EmbedError(format!(
                    "embeddings request to {} timed out after {:?}",
                    self.url, self.timeout
                ))
            })??;
        if !status.is_success() {
            // Carry the body: a 404 on the wrong path and a 400 on an unknown
            // model are the two likely mistakes, and only the body separates
            // them.
            return Err(EmbedError(format!(
                "embeddings {} returned {}: {}",
                self.url,
                status,
                snippet(&text)
            )));
        }

        let out = parse_embeddings_response(&text, texts.len())?;
        if let Some(first) = out.first() {
            self.dim.set(first.len());
        }
        Ok(out)
    }
}

/// Accept the three things people paste as a base URL.
#[cfg(feature = "openai-embed")]
fn normalize_embeddings_url(base: &str) -> String {
    let b = base.trim_end_matches('/');
    if b.ends_with("/embeddings") {
        b.to_string()
    } else if b.ends_with("/v1") {
        format!("{b}/embeddings")
    } else {
        format!("{b}/v1/embeddings")
    }
}

#[cfg(feature = "openai-embed")]
fn snippet(s: &str) -> String {
    let t = s.trim();
    if t.chars().count() <= 200 {
        return t.to_string();
    }
    let cut: String = t.chars().take(200).collect();
    format!("{cut}…")
}

/// Pull the vectors out of an `/v1/embeddings` response, in the caller's order.
///
/// Kept separate from the request so it can be tested without a server, and
/// because the ordering is the part that bites: the response carries an
/// `index` per row precisely because the array is not promised to arrive in
/// input order, and a server that batches internally can return it shuffled.
/// Trusting the array order would pair every document with someone else's
/// vector — silently, since both are just `Vec<f32>`.
#[cfg(feature = "openai-embed")]
fn parse_embeddings_response(body: &str, want: usize) -> Result<Vec<Vec<f32>>, EmbedError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| EmbedError(format!("embeddings response is not json: {e}")))?;
    if let Some(msg) = v.get("error") {
        return Err(EmbedError(format!("embeddings server error: {msg}")));
    }
    let rows = v.get("data").and_then(|d| d.as_array()).ok_or_else(|| {
        // The likely cause is a URL pointing at something that is not the
        // OpenAI-shaped endpoint — llama.cpp's own /embeddings, for one, which
        // answers a bare array and nests each vector one deeper. Say so; the
        // body alone reads like the server is broken.
        EmbedError(format!(
            "embeddings response has no data array (is {} an OpenAI-compatible \
             /v1/embeddings endpoint?): {}",
            "the configured url",
            snippet(body)
        ))
    })?;
    if rows.len() != want {
        return Err(EmbedError(format!(
            "asked for {want} embeddings, got {}",
            rows.len()
        )));
    }

    let mut slots: Vec<Option<Vec<f32>>> = (0..want).map(|_| None).collect();
    for (pos, row) in rows.iter().enumerate() {
        let idx = row.get("index").and_then(|i| i.as_u64()).unwrap_or(pos as u64) as usize;
        if idx >= want {
            return Err(EmbedError(format!("embedding index {idx} out of range")));
        }
        let arr = row
            .get("embedding")
            .and_then(|e| e.as_array())
            .ok_or_else(|| EmbedError(format!("embedding {idx} has no vector")))?;
        let vec: Vec<f32> = arr.iter().filter_map(|x| x.as_f64()).map(|x| x as f32).collect();
        if vec.len() != arr.len() {
            return Err(EmbedError(format!("embedding {idx} holds a non-number")));
        }
        if vec.is_empty() {
            return Err(EmbedError(format!("embedding {idx} is empty")));
        }
        if slots[idx].is_some() {
            return Err(EmbedError(format!("embedding index {idx} arrived twice")));
        }
        slots[idx] = Some(l2_normalize(vec));
    }
    slots
        .into_iter()
        .enumerate()
        .map(|(i, s)| s.ok_or_else(|| EmbedError(format!("no embedding for input {i}"))))
        .collect()
}

// ---------------------------------------------------------------------------
// The dispatch enum
// ---------------------------------------------------------------------------

pub enum Embedder {
    Hash(HashEmbedder),
    #[cfg(feature = "static-embed")]
    Static(StaticTableEmbedder),
    #[cfg(feature = "openai-embed")]
    OpenAi(OpenAiEmbedder),
}

impl Embedder {
    /// The width of the vectors this embedder emits. For an external model
    /// that is whatever the server returns, and it is `0` until the first call
    /// has come back — nothing here can know it before then.
    pub fn dim(&self) -> usize {
        match self {
            Embedder::Hash(_) => EMBED_DIM,
            #[cfg(feature = "static-embed")]
            Embedder::Static(_) => EMBED_DIM,
            #[cfg(feature = "openai-embed")]
            Embedder::OpenAi(o) => o.dim(),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Embedder::Hash(_) => "hash",
            #[cfg(feature = "static-embed")]
            Embedder::Static(_) => "static-int8",
            #[cfg(feature = "openai-embed")]
            Embedder::OpenAi(_) => "openai",
        }
    }

    /// Whether this embedder's vectors carry MEANING, i.e. whether nearby
    /// vectors imply related text.
    ///
    /// `HashEmbedder` is a signed-FNV bag-of-words projection: deterministic and
    /// useful for exercising the vector path, but two texts about the same topic
    /// land no closer than two unrelated ones. Vector and hybrid search over it
    /// return noise, so anything CHOOSING a retrieval mode on the user's behalf
    /// must ask this rather than assume a vector index means vector search
    /// works.
    pub fn is_semantic(&self) -> bool {
        match self {
            Embedder::Hash(_) => false,
            #[cfg(feature = "static-embed")]
            Embedder::Static(_) => true,
            #[cfg(feature = "openai-embed")]
            Embedder::OpenAi(_) => true,
        }
    }

    /// Async because one variant is a network call. The local embedders finish
    /// without yielding; splitting the API in two so they could stay sync would
    /// push the choice onto every call site, which is exactly the thing this
    /// enum exists to hide.
    pub async fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        match self {
            Embedder::Hash(h) => Ok(h.embed(text)),
            #[cfg(feature = "static-embed")]
            Embedder::Static(s) => s.embed(text),
            #[cfg(feature = "openai-embed")]
            Embedder::OpenAi(o) => o.embed(text).await,
        }
    }

    /// Many texts, one round trip where the backend has one. The local
    /// embedders just loop; there is nothing to batch.
    pub async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        match self {
            #[cfg(feature = "openai-embed")]
            Embedder::OpenAi(o) => o.embed_batch(texts).await,
            _ => {
                let mut out = Vec::with_capacity(texts.len());
                for t in texts {
                    out.push(self.embed(t).await?);
                }
                Ok(out)
            }
        }
    }
}

#[cfg(all(test, feature = "openai-embed"))]
mod openai_tests {
    use super::*;

    /// The three shapes people paste. Getting this wrong costs a 404 that reads
    /// like a missing model rather than a wrong path.
    #[test]
    fn a_base_url_is_accepted_in_all_three_shapes() {
        for base in [
            "http://llama:8080",
            "http://llama:8080/",
            "http://llama:8080/v1",
            "http://llama:8080/v1/",
            "http://llama:8080/v1/embeddings",
        ] {
            assert_eq!(
                normalize_embeddings_url(base),
                "http://llama:8080/v1/embeddings",
                "base {base}"
            );
        }
    }

    /// The response carries an `index` per row because the array is NOT
    /// promised in input order. Trusting the array would pair each text with
    /// someone else's vector, and both are `Vec<f32>`, so nothing downstream
    /// would notice.
    #[test]
    fn rows_are_placed_by_index_not_by_arrival() {
        let body = r#"{"data":[
            {"index":2,"embedding":[0.0,0.0,3.0]},
            {"index":0,"embedding":[1.0,0.0,0.0]},
            {"index":1,"embedding":[0.0,2.0,0.0]}
        ]}"#;
        let got = parse_embeddings_response(body, 3).expect("parse");
        assert_eq!(got.len(), 3);
        // Each is L2-normalized, so the surviving signal is WHICH axis is set.
        assert!(got[0][0] > 0.99, "input 0 got {:?}", got[0]);
        assert!(got[1][1] > 0.99, "input 1 got {:?}", got[1]);
        assert!(got[2][2] > 0.99, "input 2 got {:?}", got[2]);
    }

    #[test]
    fn every_vector_comes_back_l2_normalized() {
        let body = r#"{"data":[{"index":0,"embedding":[3.0,4.0]}]}"#;
        let got = parse_embeddings_response(body, 1).expect("parse");
        let norm: f32 = got[0].iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-6, "norm was {norm}");
    }

    /// A short count means some input silently got no vector. Returning the
    /// short list would shift every later document onto the wrong text.
    #[test]
    fn a_short_or_duplicated_response_is_refused() {
        let short = r#"{"data":[{"index":0,"embedding":[1.0]}]}"#;
        assert!(parse_embeddings_response(short, 2).is_err());

        let dup = r#"{"data":[{"index":0,"embedding":[1.0]},{"index":0,"embedding":[1.0]}]}"#;
        let e = parse_embeddings_response(dup, 2).expect_err("duplicate index");
        assert!(e.0.contains("twice"), "message was {}", e.0);
    }

    /// An OpenAI-shaped error body is JSON with 200-looking structure on some
    /// servers; say what it said rather than "no data array".
    #[test]
    fn a_server_error_body_is_reported_as_itself() {
        let body = r#"{"error":{"message":"model not found","type":"invalid_request_error"}}"#;
        let e = parse_embeddings_response(body, 1).expect_err("error body");
        assert!(e.0.contains("model not found"), "message was {}", e.0);
    }

    #[test]
    fn a_non_number_in_the_vector_is_refused_rather_than_dropped() {
        let body = r#"{"data":[{"index":0,"embedding":[1.0,null,2.0]}]}"#;
        let e = parse_embeddings_response(body, 1).expect_err("non-number");
        assert!(e.0.contains("non-number"), "message was {}", e.0);
    }
}

/// End-to-end over a real socket. The response parser is tested above without a
/// server; this covers the half that only fails in practice — the URL the
/// client actually asks for, the body it actually sends, and the timeout,
/// none of which a parser test can see.
#[cfg(all(test, feature = "openai-embed"))]
mod openai_wire_tests {
    use super::*;
    use compio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
    use compio::net::TcpListener;

    // The request text the stand-in server saw. A cell rather than a channel:
    // the runtime is single-threaded and the call under test cannot return
    // before the server has replied, so it is already written by the time the
    // assertions read it — and this needs no extra dependency to say so.
    thread_local! {
        static SEEN: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
    }

    /// Serve exactly one request with `body`, and record what was asked for.
    async fn serve_once(body: &'static str) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        compio::runtime::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            // One read: the request is small and arrives in one segment on
            // loopback. This is a stand-in, not a proxy.
            let (n, buf) = stream.read(Vec::with_capacity(8192)).await.unwrap();
            SEEN.with(|c| *c.borrow_mut() = String::from_utf8_lossy(&buf[..n]).to_string());
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.into_bytes()).await;
            let _ = AsyncWrite::flush(&mut stream).await;
        })
        .detach();
        port
    }

    #[compio::test]
    async fn it_posts_to_v1_embeddings_and_returns_the_vector() {
        let body = r#"{"data":[{"index":0,"embedding":[0.0,3.0,4.0]}]}"#;
        let port = serve_once(body).await;
        let e = OpenAiEmbedder::new(&format!("http://127.0.0.1:{port}"), "nomic-embed-text");
        let v = e.embed("hello").await.expect("embed");

        assert_eq!(v.len(), 3);
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-6, "norm {norm}");
        // dim() reports what the server returned, not a configured guess.
        assert_eq!(e.dim(), 3);

        let req = SEEN.with(|c| c.borrow().clone());
        assert!(req.starts_with("POST /v1/embeddings "), "request line: {req}");
        assert!(req.contains("nomic-embed-text"), "model missing from body: {req}");
        assert!(req.contains("hello"), "input missing from body: {req}");
    }

    /// Headers, then nothing. A distinct hang from the one below: the request
    /// has been answered, so a timeout wrapped around only `send()` has already
    /// returned by the time the body stalls, and the caller waits forever on a
    /// read nothing is guarding.
    #[compio::test]
    async fn a_server_that_stops_after_the_headers_times_out_too() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        compio::runtime::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let (n, _buf) = stream.read(Vec::with_capacity(8192)).await.unwrap();
            let _ = n;
            // Promise 64 bytes of body and never send them.
            let head = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                        Content-Length: 64\r\n\r\n";
            let _ = stream.write_all(head.as_bytes().to_vec()).await;
            let _ = AsyncWrite::flush(&mut stream).await;
            compio::time::sleep(std::time::Duration::from_secs(30)).await;
            drop(stream);
        })
        .detach();

        let e = OpenAiEmbedder::new(&format!("http://127.0.0.1:{port}"), "m")
            .with_timeout(std::time::Duration::from_millis(300));
        let err = e.embed("hello").await.expect_err("must time out");
        assert!(err.0.contains("timed out"), "message was {}", err.0);
    }

    /// A server that accepts and then says nothing must not stall the caller.
    /// This is the failure cyper cannot report on its own — it has no timeout.
    #[compio::test]
    async fn a_silent_server_times_out_rather_than_hanging() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        compio::runtime::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            // Accept, then never answer, holding the connection open.
            compio::time::sleep(std::time::Duration::from_secs(30)).await;
            drop(stream);
        })
        .detach();

        let e = OpenAiEmbedder::new(&format!("http://127.0.0.1:{port}"), "m")
            .with_timeout(std::time::Duration::from_millis(300));
        let err = e.embed("hello").await.expect_err("must time out");
        assert!(err.0.contains("timed out"), "message was {}", err.0);
    }
}
