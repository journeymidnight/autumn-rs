//! Vector retrieval primitives (SPFresh-IVF-on-KV) for autumn-memory — plan §7
//! 向量腿. Pure, deterministic, no I/O — the embedder lives elsewhere (sglang);
//! this module only does the math + (de)serialization that the IVF index needs.
//!
//! IVF (inverted file): k-means partitions the corpus into `nlist` centroids,
//! each owning a posting list of its assigned vectors. On autumn the posting
//! lists are key prefixes `ivf/{centroid}/{vec_id}` (plan §7). Query: find the
//! `nprobe` nearest centroids, scan only those buckets, cosine-rerank.

/// Cosine similarity (0 for a zero vector). Used for the final rerank score.
pub(crate) fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    let n = a.len().min(b.len());
    for i in 0..n {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

/// Squared Euclidean distance — the k-means / centroid-assignment metric.
pub(crate) fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
    let mut s = 0.0f32;
    let n = a.len().min(b.len());
    for i in 0..n {
        let d = a[i] - b[i];
        s += d * d;
    }
    s
}

/// L2-normalize to a unit vector (zero vector unchanged). We store + cluster +
/// probe on NORMALIZED vectors so the L2 assignment metric is order-consistent
/// with the cosine rerank score (for cosine search, magnitude is irrelevant).
pub(crate) fn normalize(v: &[f32]) -> Vec<f32> {
    let n = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if n == 0.0 {
        return v.to_vec();
    }
    v.iter().map(|x| x / n).collect()
}

/// Index of the nearest centroid (by L2) — a vector's bucket assignment.
pub(crate) fn nearest(v: &[f32], centroids: &[Vec<f32>]) -> usize {
    let mut best = 0usize;
    let mut bd = f32::MAX;
    for (i, c) in centroids.iter().enumerate() {
        let d = l2_sq(v, c);
        if d < bd {
            bd = d;
            best = i;
        }
    }
    best
}

/// The `nprobe` nearest centroid indices to `query` (by L2), nearest first.
pub(crate) fn nearest_centroids(query: &[f32], centroids: &[Vec<f32>], nprobe: usize) -> Vec<usize> {
    let mut scored: Vec<(usize, f32)> = centroids
        .iter()
        .enumerate()
        .map(|(i, c)| (i, l2_sq(query, c)))
        .collect();
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    scored
        .into_iter()
        .take(nprobe.max(1))
        .map(|(i, _)| i)
        .collect()
}

/// Lloyd's k-means. Deterministic given `seed` (LCG init pick). Empty clusters
/// keep their previous centroid. Returns ≤ `min(k, n)` centroids.
pub(crate) fn kmeans(vectors: &[Vec<f32>], k: usize, max_iter: usize, seed: u64) -> Vec<Vec<f32>> {
    let n = vectors.len();
    if n == 0 || k == 0 {
        return Vec::new();
    }
    let k = k.min(n);
    let dim = vectors[0].len();

    // deterministic init: LCG-pick k distinct seed vectors
    let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(k);
    let mut chosen = std::collections::HashSet::new();
    let mut state = seed | 1;
    while centroids.len() < k {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let idx = ((state >> 33) as usize) % n;
        if chosen.insert(idx) {
            centroids.push(vectors[idx].clone());
        }
    }

    for _ in 0..max_iter.max(1) {
        let mut sums = vec![vec![0.0f32; dim]; k];
        let mut counts = vec![0usize; k];
        for v in vectors {
            let c = nearest(v, &centroids);
            counts[c] += 1;
            for d in 0..dim.min(v.len()) {
                sums[c][d] += v[d];
            }
        }
        let mut moved = false;
        for c in 0..k {
            if counts[c] == 0 {
                continue;
            }
            for d in 0..dim {
                let m = sums[c][d] / counts[c] as f32;
                if (m - centroids[c][d]).abs() > 1e-6 {
                    moved = true;
                }
                centroids[c][d] = m;
            }
        }
        if !moved {
            break;
        }
    }
    centroids
}

// -- codecs ------------------------------------------------------------------

/// `[dim u32 LE][f32 LE]*dim`
pub(crate) fn encode_vec(v: &[f32]) -> Vec<u8> {
    let mut b = Vec::with_capacity(4 + v.len() * 4);
    b.extend_from_slice(&(v.len() as u32).to_le_bytes());
    for &x in v {
        b.extend_from_slice(&x.to_le_bytes());
    }
    b
}

pub(crate) fn decode_vec(b: &[u8]) -> Option<Vec<f32>> {
    if b.len() < 4 {
        return None;
    }
    let n = u32::from_le_bytes(b[..4].try_into().ok()?) as usize;
    if b.len() < 4 + n * 4 {
        return None;
    }
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        let o = 4 + i * 4;
        v.push(f32::from_le_bytes(b[o..o + 4].try_into().ok()?));
    }
    Some(v)
}

/// The IVF centroid manifest (`ivf_meta/centroids`).
pub(crate) struct Centroids {
    pub epoch: u32,
    pub dim: u32,
    pub cs: Vec<Vec<f32>>,
}

impl Centroids {
    /// `[epoch u32][dim u32][n u32]( f32 LE * dim )*n`
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(12 + self.cs.len() * self.dim as usize * 4);
        b.extend_from_slice(&self.epoch.to_le_bytes());
        b.extend_from_slice(&self.dim.to_le_bytes());
        b.extend_from_slice(&(self.cs.len() as u32).to_le_bytes());
        for c in &self.cs {
            for &x in c {
                b.extend_from_slice(&x.to_le_bytes());
            }
        }
        b
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() < 12 {
            return None;
        }
        let epoch = u32::from_le_bytes(b[0..4].try_into().ok()?);
        let dim = u32::from_le_bytes(b[4..8].try_into().ok()?) as usize;
        let n = u32::from_le_bytes(b[8..12].try_into().ok()?) as usize;
        if dim == 0 || b.len() < 12 + n * dim * 4 {
            return None;
        }
        let mut cs = Vec::with_capacity(n);
        let mut o = 12;
        for _ in 0..n {
            let mut c = Vec::with_capacity(dim);
            for _ in 0..dim {
                c.push(f32::from_le_bytes(b[o..o + 4].try_into().ok()?));
                o += 4;
            }
            cs.push(c);
        }
        Some(Centroids {
            epoch,
            dim: dim as u32,
            cs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cosine_basics() {
        assert!((cosine(&[1.0, 0.0], &[1.0, 0.0]) - 1.0).abs() < 1e-6);
        assert!(cosine(&[1.0, 0.0], &[0.0, 1.0]).abs() < 1e-6); // orthogonal
        assert!((cosine(&[1.0, 2.0, 3.0], &[2.0, 4.0, 6.0]) - 1.0).abs() < 1e-6); // parallel
        assert_eq!(cosine(&[0.0, 0.0], &[1.0, 1.0]), 0.0); // zero vector
    }

    #[test]
    fn kmeans_separates_two_clusters() {
        // two tight clusters around (0,0) and (10,10)
        let mut v = Vec::new();
        for i in 0..20 {
            let j = (i % 5) as f32 * 0.1;
            v.push(vec![j, j]); // near origin
            v.push(vec![10.0 + j, 10.0 + j]); // near (10,10)
        }
        let cs = kmeans(&v, 2, 50, 42);
        assert_eq!(cs.len(), 2);
        // one centroid near origin, one near (10,10)
        let near_origin = cs.iter().any(|c| l2_sq(c, &[0.0, 0.0]) < 1.0);
        let near_ten = cs.iter().any(|c| l2_sq(c, &[10.0, 10.0]) < 1.0);
        assert!(near_origin && near_ten, "centroids: {cs:?}");
    }

    #[test]
    fn ivf_probe_finds_l2_nearest() {
        // two well-separated clusters at 0 and 20; the true L2-nearest vector
        // to the query must live in a probed (L2-nearest-centroid) bucket —
        // IVF's core recall property (assignment + probing share the metric).
        let mut vecs = Vec::new();
        for c in 0..2 {
            let base = c as f32 * 20.0;
            for i in 0..10 {
                let o = i as f32 * 0.1;
                vecs.push(vec![base + o, base - o]);
            }
        }
        let cs = kmeans(&vecs, 2, 100, 7);
        assert_eq!(cs.len(), 2);
        let q = vec![20.2, 19.8];
        let true_best = (0..vecs.len())
            .min_by(|&a, &b| {
                l2_sq(&q, &vecs[a])
                    .partial_cmp(&l2_sq(&q, &vecs[b]))
                    .unwrap()
            })
            .unwrap();
        let probed = nearest_centroids(&q, &cs, 1);
        assert!(
            probed.contains(&nearest(&vecs[true_best], &cs)),
            "true L2-nearest's bucket must be probed"
        );
    }

    #[test]
    fn normalize_makes_l2_match_cosine() {
        // after normalization, L2 ordering == cosine ordering, so IVF-by-L2 is
        // consistent with the cosine rerank score.
        let q = normalize(&[1.0, 0.1]);
        let a = normalize(&[1.0, 0.0]); // closer in angle
        let b = normalize(&[0.0, 1.0]); // farther
        assert!(cosine(&q, &a) > cosine(&q, &b));
        assert!(l2_sq(&q, &a) < l2_sq(&q, &b));
        // normalizing is idempotent on unit vectors
        assert!((normalize(&a).iter().map(|x| x * x).sum::<f32>() - 1.0).abs() < 1e-5);
    }

    #[test]
    fn vec_codec_roundtrip() {
        let v = vec![1.5f32, -2.0, 0.0, 3.25];
        assert_eq!(decode_vec(&encode_vec(&v)), Some(v));
        assert!(decode_vec(&[0, 0]).is_none());
    }

    #[test]
    fn centroids_codec_roundtrip() {
        let c = Centroids {
            epoch: 3,
            dim: 2,
            cs: vec![vec![1.0, 2.0], vec![3.0, 4.0]],
        };
        let dec = Centroids::decode(&c.encode()).expect("decode");
        assert_eq!(dec.epoch, 3);
        assert_eq!(dec.dim, 2);
        assert_eq!(dec.cs, vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
    }
}
