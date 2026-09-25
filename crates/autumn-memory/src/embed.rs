//! Optional lightweight text→vector embedder for the vector / hybrid legs.
//!
//! autumn-memory itself takes caller-supplied vectors (`index_vector` /
//! `search_vector` want a `&[f32]`) — production feeds them from a shared
//! sglang/vLLM endpoint. This convenience module gives callers that DON'T want
//! to stand up a model server a built-in embedder:
//!
//!   * [`StaticTableEmbedder`] — a Model2Vec-style static int8 lookup table
//!     (feature `static-embed`): tokenize → int8 row lookup → dequant →
//!     mean-pool. Real semantics, no network, no GPU.
//!   * [`OpenAiEmbedder`] — any server speaking OpenAI's `/v1/embeddings`
//!     (feature `openai-embed`): llama.cpp on a CPU, vLLM, sglang, or the
//!     vendor. Real model, real cost, over the network.
//!
//! An enum ([`Embedder`]) dispatches between them, and every variant emits an
//! **L2-normalized** vector so scores stay comparable. The static table emits
//! `EMBED_DIM`; an external model emits whatever it emits, and the vector index
//! stores the width per record, so nothing here has to agree with 256.
//!
//! There is no built-in fallback embedder, and that is the point. One used to
//! live here — a signed-FNV bag of words, always available, zero dependencies —
//! and being the DEFAULT is what made it harmful: its vectors are deterministic
//! but carry no meaning, so vector and hybrid search over them ranked noise
//! confidently rather than failing. Every caller then needed a way to ask
//! whether its own embedder was lying. Having no embedder is an honest state
//! and callers can see it; having a fake one is not.
//!
//! One thing this module does NOT do is stop you mixing them. Vectors written
//! by one embedder and searched with another are silent nonsense, not an error
//! — the store takes `&[f32]` and cannot tell whose. Re-index when you switch.

use std::fmt;

/// Output dimension every embedder honors. 256 matches Model2Vec
/// `potion-base-8M`, so a static table needs no reprojection.
pub const EMBED_DIM: usize = 256;

/// Error from the (fallible) static-table embedder — loading a table/tokenizer
/// or tokenizing, or a request to an external server.
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
/// llama.cpp on spare CPU, and point this at it.
/// Token budget for one embedding input, under BGE-M3's 8192-token context
/// with room for the server's own framing.
#[cfg(feature = "openai-embed")]
const EMBED_TOKEN_BUDGET: usize = 7000;

/// How many estimated tokens one REQUEST may carry. The server's physical
/// batch is 8192 tokens; a request that asks it to embed more than that in
/// one go is refused or, worse, dropped at the connection.
#[cfg(feature = "openai-embed")]
const BATCH_TOKEN_BUDGET: usize = 7000;

/// How many times one embedding request is tried before the caller hears
/// about it.
#[cfg(feature = "openai-embed")]
const EMBED_ATTEMPTS: u32 = 3;

/// At most this many inputs per request, whatever the token estimate says.
/// The estimate can be wrong; a count cannot.
#[cfg(feature = "openai-embed")]
const BATCH_MAX_INPUTS: usize = 64;

/// Estimated tokens for one input — see `clip` for why ASCII counts a third.
#[cfg(feature = "openai-embed")]
fn est_tokens(text: &str) -> usize {
    let ascii = text.chars().filter(|c| c.is_ascii()).count();
    let wide = text.chars().count() - ascii;
    ascii / 3 + wide
}

/// Split `texts` into request-sized runs. Order is preserved and every input
/// appears exactly once, because the caller matches vectors to inputs by
/// position and a lost one leaves a symbol invisible to vector search with
/// nothing to say so.
#[cfg(feature = "openai-embed")]
fn batches(texts: &[&str]) -> Vec<std::ops::Range<usize>> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut tokens = 0usize;
    for (i, t) in texts.iter().enumerate() {
        let n = est_tokens(t);
        let full = i - start >= BATCH_MAX_INPUTS;
        let over = i > start && tokens + n > BATCH_TOKEN_BUDGET;
        if full || over {
            out.push(start..i);
            start = i;
            tokens = 0;
        }
        tokens += n;
    }
    if start < texts.len() {
        out.push(start..texts.len());
    }
    out
}

/// Cut `text` to something the embedding server will accept.
///
/// Tokens are estimated, not counted: this crate has no tokenizer and pulling
/// one in to guard a rare case would cost every build. The estimate weights
/// ASCII at 1/3 of a token (English and code average ~4 chars per token) and
/// everything else at a whole one, because CJK runs about a token per
/// character — a single char budget cannot serve both, and the Chinese corpus
/// is the one that would silently lose its tail under a code-shaped guess.
/// Deliberately conservative: an input clipped a little short still embeds to
/// something useful, while one token too many is a 500 that ends the run.
#[cfg(feature = "openai-embed")]
fn clip(text: &str) -> &str {
    let mut est = 0f32;
    for (i, ch) in text.char_indices() {
        est += if ch.is_ascii() { 1.0 / 3.0 } else { 1.0 };
        if est > EMBED_TOKEN_BUDGET as f32 {
            // `char_indices` gives a boundary, so this never splits a char.
            return &text[..i];
        }
    }
    text
}

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
    pub fn new(base: &str, model: &str) -> Result<Self, EmbedError> {
        Ok(Self {
            client: cyper::Client::new().map_err(|e| EmbedError(format!("HTTP client: {e}")))?,
            url: normalize_embeddings_url(base),
            model: model.to_string(),
            api_key: None,
            timeout: std::time::Duration::from_secs(60),
            dim: std::cell::Cell::new(0),
        })
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
    ///
    /// Inputs are CLIPPED to the model's context first — see `clip`. An
    /// embedding server refuses an over-long input with a 500, and one such
    /// input in a corpus is enough to end an index run that had no other
    /// problem: a 8372-token `impl` block stopped a 200-file repo at the
    /// first file, over and over, because the failure is fatal and the retry
    /// starts from the same symbol.
    pub async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let clipped: Vec<&str> = texts.iter().map(|t| clip(t)).collect();
        // One request per run, concatenated in order. A loop rather than
        // recursion: an async fn that calls itself needs boxing, and there is
        // nothing recursive about the problem.
        let mut out = Vec::with_capacity(clipped.len());
        for r in batches(&clipped) {
            out.extend(self.embed_with_retry(&clipped[r]).await?);
        }
        Ok(out)
    }

    /// `embed_once`, retried. An index run makes thousands of these calls
    /// over minutes, against a server that may be restarting, reloading a
    /// model or simply dropping an idle pooled connection; one such moment
    /// ending the whole run is a poor trade against waiting a second. The
    /// waits are short because the caller is already slow: a run does not
    /// get faster by failing early, it just has to start over.
    async fn embed_with_retry(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        let mut last: Option<EmbedError> = None;
        for attempt in 0..EMBED_ATTEMPTS {
            if attempt > 0 {
                compio::time::sleep(std::time::Duration::from_millis(250 << attempt)).await;
            }
            match self.embed_once(texts).await {
                Ok(v) => return Ok(v),
                // Silent between attempts on purpose: this crate has no
                // logger, and the one failure that matters — the last —
                // reaches the caller, which does.
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| EmbedError("embeddings call failed with no error".into())))
    }

    /// One request, exactly as given. `embed_batch` is what decides how much
    /// may go in one, and `embed_with_retry` how many times it is tried.
    async fn embed_once(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
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

/// Uninhabited with no feature enabled: a build that compiled in no embedder
/// cannot produce one, and the compiler says so rather than a default that
/// returns plausible nonsense.
pub enum Embedder {
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
            #[cfg(feature = "static-embed")]
            Embedder::Static(_) => EMBED_DIM,
            #[cfg(feature = "openai-embed")]
            Embedder::OpenAi(o) => o.dim(),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            #[cfg(feature = "static-embed")]
            Embedder::Static(_) => "static-int8",
            #[cfg(feature = "openai-embed")]
            Embedder::OpenAi(_) => "openai",
        }
    }

    /// Async because one variant is a network call. The local embedders finish
    /// without yielding; splitting the API in two so they could stay sync would
    /// push the choice onto every call site, which is exactly the thing this
    /// enum exists to hide.
    pub async fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        match self {
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
            // Named, not a `_` catch-all: with only the external embedder
            // compiled in there is nothing left for a wildcard to match, and it
            // becomes an unreachable pattern the compiler warns about.
            #[cfg(feature = "static-embed")]
            Embedder::Static(_) => {
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

    /// A caller hands one file's symbols to `embed_batch` — hundreds of them
    /// for a big source file — and the server has a physical batch of 8192
    /// tokens. Unsplit, that request is refused or dropped at the connection
    /// (`hyper client error (SendRequest)`), which ends the index run.
    #[test]
    fn batches_stay_within_one_request() {
        let one = "x".repeat(3_000); // ~1000 estimated tokens
        let texts: Vec<&str> = std::iter::repeat(one.as_str()).take(30).collect();
        let runs = batches(&texts);
        assert!(runs.len() > 1, "30k tokens must not go in one request");
        for r in &runs {
            let sum: usize = texts[r.clone()].iter().map(|t| est_tokens(t)).sum();
            // One input alone may exceed the budget (clip already bounded it);
            // two or more may not.
            assert!(sum <= BATCH_TOKEN_BUDGET || r.len() == 1, "run {r:?} sums to {sum}");
        }
        // Every input exactly once, in order.
        let covered: Vec<usize> = runs.iter().flat_map(|r| r.clone()).collect();
        assert_eq!(covered, (0..texts.len()).collect::<Vec<_>>());
    }

    /// The count bound catches what the token estimate cannot.
    #[test]
    fn a_long_run_of_tiny_inputs_is_still_split() {
        let texts: Vec<&str> = std::iter::repeat("a").take(500).collect();
        let runs = batches(&texts);
        assert!(runs.iter().all(|r| r.len() <= BATCH_MAX_INPUTS));
        assert_eq!(runs.iter().map(|r| r.len()).sum::<usize>(), 500);
    }

    /// Code and prose have to fit through the same budget, and the two have
    /// token densities that differ by 3x. Whatever the estimate is, a clipped
    /// input must stay valid UTF-8 and must not exceed the budget on the
    /// worst input (every char a token).
    #[test]
    fn clip_keeps_both_alphabets_under_the_budget() {
        let ascii = "fn f() { }\n".repeat(20_000);
        let clipped = clip(&ascii);
        assert!(clipped.len() < ascii.len(), "an oversized input must be cut");
        assert!(clipped.len() <= EMBED_TOKEN_BUDGET * 3 + 1);

        // One token per char, the dense case: the cut lands at a char
        // boundary (slicing mid-char would panic) and inside the budget.
        let cjk = "地藏菩萨本愿经".repeat(5_000);
        let clipped = clip(&cjk);
        assert!(clipped.chars().count() <= EMBED_TOKEN_BUDGET);
        assert!(cjk.starts_with(clipped));

        // Short inputs are handed through untouched — the common case must
        // not pay for the guard.
        assert_eq!(clip("hello"), "hello");
    }

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
        let e = OpenAiEmbedder::new(&format!("http://127.0.0.1:{port}"), "nomic-embed-text").expect("HTTP client");
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
            .expect("HTTP client")
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
            .expect("HTTP client")
            .with_timeout(std::time::Duration::from_millis(300));
        let err = e.embed("hello").await.expect_err("must time out");
        assert!(err.0.contains("timed out"), "message was {}", err.0);
    }

    /// An `https://` embedder must fail as an error, not a panic. rustls needs
    /// a process-level crypto provider, and cyper's rustls backend enables
    /// none: without `compio/ring` in the `openai-embed` feature the TLS
    /// handshake panics ("Could not automatically determine the process-level
    /// CryptoProvider"), which under `panic = "abort"` kills memory-mcp on its
    /// first embed call. The listener accepts and says nothing, so the call
    /// gets as far as building the TLS client and then fails on the handshake.
    #[compio::test]
    async fn an_https_embedder_fails_without_panicking() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        compio::runtime::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            drop(stream);
        })
        .detach();
        let e = OpenAiEmbedder::new(&format!("https://127.0.0.1:{port}"), "nomic-embed-text")
            .expect("HTTP client");
        assert!(e.embed("hello").await.is_err());
    }
}
