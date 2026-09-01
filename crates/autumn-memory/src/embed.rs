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
//!
//! An enum ([`Embedder`]) dispatches between them; all variants emit an
//! `EMBED_DIM`-length **L2-normalized** vector so results stay comparable.

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
// The dispatch enum
// ---------------------------------------------------------------------------

pub enum Embedder {
    Hash(HashEmbedder),
    #[cfg(feature = "static-embed")]
    Static(StaticTableEmbedder),
}

impl Embedder {
    pub fn dim(&self) -> usize {
        EMBED_DIM
    }

    pub fn name(&self) -> &'static str {
        match self {
            Embedder::Hash(_) => "hash",
            #[cfg(feature = "static-embed")]
            Embedder::Static(_) => "static-int8",
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
        }
    }

    pub fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        match self {
            Embedder::Hash(h) => Ok(h.embed(text)),
            #[cfg(feature = "static-embed")]
            Embedder::Static(s) => s.embed(text),
        }
    }
}
