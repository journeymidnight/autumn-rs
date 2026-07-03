//! Text→vector embedder for the code index. autumn-memory takes caller-supplied
//! vectors; this is where they come from.
//!
//!   * `Hash`  — default, zero deps. Signed-FNV bag-of-words hashing. Real
//!               plumbing, weak semantics; makes `cargo run` work with no model.
//!   * `Static` — Model2Vec-style static int8 lookup table (feature
//!               `static-embed`): tokenize → int8 row → dequant → mean-pool.
//!               Real semantics, no service. Build the table with
//!               `tools/fetch_model.py`.
//!
//! All variants emit an `EMBED_DIM`-length L2-normalized vector.

use anyhow::Result;

pub const EMBED_DIM: usize = 256;

fn l2_normalize(mut v: Vec<f32>) -> Vec<f32> {
    let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if n > 0.0 {
        for x in v.iter_mut() {
            *x /= n;
        }
    }
    v
}

fn tokenize(text: &str) -> impl Iterator<Item = String> + '_ {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
}

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

pub struct HashEmbedder;

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
    pub fn load(model_path: &str, tokenizer_path: &str) -> Result<Self> {
        let bytes = std::fs::read(model_path)?;
        if bytes.len() < 20 || &bytes[0..4] != b"M2VS" {
            anyhow::bail!("{model_path}: not an M2VS table");
        }
        let rd = |o: usize| u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
        let vocab = rd(8) as usize;
        let dim = rd(12) as usize;
        let scale = f32::from_le_bytes(bytes[16..20].try_into().unwrap());
        if dim != EMBED_DIM {
            anyhow::bail!("{model_path}: dim {dim} != EMBED_DIM {EMBED_DIM}");
        }
        let want = 20 + vocab * dim;
        if bytes.len() < want {
            anyhow::bail!("{model_path}: truncated");
        }
        let tokenizer = tokenizers::Tokenizer::from_file(tokenizer_path)
            .map_err(|e| anyhow::anyhow!("tokenizer {tokenizer_path}: {e}"))?;
        Ok(Self {
            vocab,
            dim,
            scale,
            table: bytes[20..want].to_vec(),
            tokenizer,
        })
    }

    pub fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let enc = self
            .tokenizer
            .encode(text, false)
            .map_err(|e| anyhow::anyhow!("tokenize: {e}"))?;
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

    pub fn embed(&self, text: &str) -> Result<Vec<f32>> {
        match self {
            Embedder::Hash(h) => Ok(h.embed(text)),
            #[cfg(feature = "static-embed")]
            Embedder::Static(s) => s.embed(text),
        }
    }
}
