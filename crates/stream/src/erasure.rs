use anyhow::{anyhow, Result};
use reed_solomon_erasure::galois_8::ReedSolomon;

/// Per-shard byte size for a given payload length and number of data shards.
///
/// `ceil(payload_len / data_shards)` — the last data shard is zero-padded up to
/// this length. The original payload length is NOT stored in the shards: the
/// authoritative length is `sealed_length` (manager-tracked), passed into
/// `ec_decode`. (A 4-byte length trailer in the last data shard used to round
/// `shard_size` up by 4; it was removed as redundant with `sealed_length`.)
pub fn shard_size(payload_len: usize, data_shards: usize) -> usize {
    payload_len.div_ceil(data_shards).max(1)
}

/// Encode a payload into `data_shards + parity_shards` equal-length byte slices.
///
/// Encoding format:
///   - Payload bytes are distributed across the first `data_shards` slices
///     (the last data shard is zero-padded up to `shard_size`).
///   - Parity shards are computed by the Reed-Solomon encoder.
///
/// No length trailer is stored — `ec_decode` is given the authoritative
/// `original_size` (`sealed_length`) by the caller.
/// All returned shards have length `shard_size(payload.len(), data_shards)`.
pub fn ec_encode(payload: &[u8], data_shards: usize, parity_shards: usize) -> Result<Vec<Vec<u8>>> {
    if data_shards == 0 {
        return Err(anyhow!("data_shards must be > 0"));
    }
    if parity_shards == 0 {
        return Err(anyhow!("parity_shards must be > 0 for EC encode"));
    }

    let size = payload.len();
    let per_shard = shard_size(size, data_shards);
    let total_shards = data_shards + parity_shards;

    debug_assert!(
        per_shard * data_shards >= size,
        "shard too small for payload: per_shard={} data_shards={} size={}",
        per_shard,
        data_shards,
        size,
    );

    // Allocate all shards zero-filled.
    let mut shards: Vec<Vec<u8>> = (0..total_shards).map(|_| vec![0u8; per_shard]).collect();

    // Copy payload across data shards.
    let mut remaining = size;
    for (i, shard) in shards.iter_mut().enumerate().take(data_shards) {
        if remaining == 0 {
            break;
        }
        let start = i * per_shard;
        let n = remaining.min(per_shard);
        shard[..n].copy_from_slice(&payload[start..start + n]);
        remaining -= n;
    }

    // Compute parity shards.
    let rs = ReedSolomon::new(data_shards, parity_shards)
        .map_err(|e| anyhow!("ReedSolomon::new failed: {e}"))?;
    rs.encode(&mut shards)
        .map_err(|e| anyhow!("RS encode failed: {e}"))?;

    Ok(shards)
}

/// Stripe-wise parity encode for the chunked EC-convert path.
///
/// Reed-Solomon over GF(256) is byte-wise across the data shards at the SAME
/// offset: `parity[j][b]` depends only on `data[0][b] .. data[K-1][b]`. So a
/// "stripe" `[s, s+stripe_len)` of every shard can be encoded independently of
/// the rest of the extent. `data_stripes` holds the K data-shard sub-ranges at
/// some shard offset `s` (each EXACTLY `stripe_len` bytes — the caller
/// zero-pads the last data shard's tail). Returns the M parity-shard
/// sub-ranges for that same offset.
///
/// Invariant (proved by `ec_encode_stripe_matches_whole`): concatenating the
/// stripe outputs over all offsets `s ∈ [0, shard_size)` yields byte-for-byte
/// the same shards as a single `ec_encode` over the whole payload. So the
/// chunked write path produces an on-disk EC layout identical to the
/// whole-extent encode — reads (`ec_subrange_read` / `ec_read_full`) are
/// unaffected by which encode path wrote the shards.
pub fn ec_encode_stripe(
    data_stripes: &[&[u8]],
    parity_shards: usize,
) -> Result<Vec<Vec<u8>>> {
    let data_shards = data_stripes.len();
    if data_shards == 0 {
        return Err(anyhow!("data_stripes must be non-empty"));
    }
    if parity_shards == 0 {
        return Err(anyhow!("parity_shards must be > 0 for EC encode"));
    }
    let stripe_len = data_stripes[0].len();
    for (i, d) in data_stripes.iter().enumerate() {
        if d.len() != stripe_len {
            return Err(anyhow!(
                "ec_encode_stripe: data stripe {i} len {} != stripe_len {stripe_len} \
                 (all data stripes must be equal length)",
                d.len()
            ));
        }
    }
    let total_shards = data_shards + parity_shards;
    let mut shards: Vec<Vec<u8>> = Vec::with_capacity(total_shards);
    for d in data_stripes {
        shards.push(d.to_vec());
    }
    for _ in 0..parity_shards {
        shards.push(vec![0u8; stripe_len]);
    }
    let rs = ReedSolomon::new(data_shards, parity_shards)
        .map_err(|e| anyhow!("ReedSolomon::new failed: {e}"))?;
    rs.encode(&mut shards)
        .map_err(|e| anyhow!("RS encode failed: {e}"))?;
    // Keep only the M parity stripes (data stripes are the caller's own slices).
    Ok(shards.split_off(data_shards))
}

/// Decode the original payload from `data_shards + parity_shards` shards.
///
/// Up to `parity_shards` elements of `shards` may be `None` (missing/unavailable).
/// Requires at least `data_shards` non-None elements. `original_size` is the
/// authoritative payload length (`sealed_length`) — the decoded data shards are
/// concatenated and truncated to it (no length trailer is stored in the shards).
pub fn ec_decode(
    mut shards: Vec<Option<Vec<u8>>>,
    data_shards: usize,
    parity_shards: usize,
    original_size: usize,
) -> Result<Vec<u8>> {
    if shards.len() != data_shards + parity_shards {
        return Err(anyhow!(
            "shards len {} != data+parity {}",
            shards.len(),
            data_shards + parity_shards
        ));
    }

    let per_shard = shards
        .iter()
        .find_map(|s| s.as_ref().map(|v| v.len()))
        .ok_or_else(|| anyhow!("all shards are None"))?;

    let rs = ReedSolomon::new(data_shards, parity_shards)
        .map_err(|e| anyhow!("ReedSolomon::new failed: {e}"))?;

    // Reconstruct only data shards (more efficient than reconstructing parity too).
    rs.reconstruct_data(&mut shards)
        .map_err(|e| anyhow!("RS reconstruct_data failed: {e}"))?;

    // Concatenate data shards and truncate to the caller-provided original size.
    let mut out = Vec::with_capacity(original_size);
    let mut remaining = original_size;
    for (i, shard_opt) in shards.iter().enumerate().take(data_shards) {
        if remaining == 0 {
            break;
        }
        let shard = shard_opt
            .as_ref()
            .ok_or_else(|| anyhow!("data shard {} missing after reconstruct", i))?;
        let n = remaining.min(per_shard);
        out.extend_from_slice(&shard[..n]);
        remaining -= n;
    }
    Ok(out)
}

/// Reconstruct a single missing shard at `missing_index`.
///
/// Used for node recovery: reads shards from `data_shards` healthy peers and
/// reconstructs the exact shard bytes that the failed node should hold.
/// The returned bytes should be written verbatim to the recovering node's extent file.
pub fn ec_reconstruct_shard(
    mut shards: Vec<Option<Vec<u8>>>,
    data_shards: usize,
    parity_shards: usize,
    missing_index: usize,
) -> Result<Vec<u8>> {
    if shards.len() != data_shards + parity_shards {
        return Err(anyhow!(
            "shards len {} != data+parity {}",
            shards.len(),
            data_shards + parity_shards
        ));
    }
    if missing_index >= shards.len() {
        return Err(anyhow!(
            "missing_index {} out of range {}",
            missing_index,
            shards.len()
        ));
    }
    if shards[missing_index].is_some() {
        return Err(anyhow!(
            "shards[{}] is not None — expected missing shard slot",
            missing_index
        ));
    }

    let rs = ReedSolomon::new(data_shards, parity_shards)
        .map_err(|e| anyhow!("ReedSolomon::new failed: {e}"))?;

    // Full reconstruct (data + parity) to rebuild whichever shard is missing.
    rs.reconstruct(&mut shards)
        .map_err(|e| anyhow!("RS reconstruct failed: {e}"))?;

    shards[missing_index]
        .take()
        .ok_or_else(|| anyhow!("shard {} still None after reconstruct", missing_index))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(payload: &[u8], data_shards: usize, parity_shards: usize) {
        let shards = ec_encode(payload, data_shards, parity_shards).unwrap();
        assert_eq!(shards.len(), data_shards + parity_shards);
        let shard_len = shards[0].len();
        for s in &shards {
            assert_eq!(s.len(), shard_len);
        }
        let decoded = ec_decode(
            shards.into_iter().map(Some).collect(),
            data_shards,
            parity_shards,
            payload.len(),
        )
        .unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_encode_decode_roundtrip_various_sizes() {
        for &size in &[1usize, 3, 7, 8, 100, 1024, 4096, 65536, 1 << 20] {
            let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
            roundtrip(&payload, 2, 1);
            roundtrip(&payload, 3, 1);
            roundtrip(&payload, 3, 2);
        }
    }

    #[test]
    fn test_encode_decode_6_3_config() {
        let payload: Vec<u8> = (0..8 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        roundtrip(&payload, 6, 3);
    }

    #[test]
    fn test_decode_with_missing_data_shard() {
        let payload: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let shards = ec_encode(&payload, 3, 2).unwrap();
        // Remove shard 1 (a data shard).
        let mut shards_opt: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        shards_opt[1] = None;
        let decoded = ec_decode(shards_opt, 3, 2, payload.len()).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_decode_with_missing_parity_shard() {
        let payload: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let shards = ec_encode(&payload, 2, 1).unwrap();
        let mut shards_opt: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        shards_opt[2] = None; // Remove parity shard.
        let decoded = ec_decode(shards_opt, 2, 1, payload.len()).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_decode_max_missing_shards() {
        // With 3 parity shards, can lose any 3.
        let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let shards = ec_encode(&payload, 6, 3).unwrap();
        let mut shards_opt: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        // Remove shards 1, 3, 5 (2 data, 1 parity).
        shards_opt[1] = None;
        shards_opt[3] = None;
        shards_opt[5] = None;
        let decoded = ec_decode(shards_opt, 6, 3, payload.len()).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_reconstruct_shard_data() {
        let payload: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let shards = ec_encode(&payload, 2, 1).unwrap();
        let original_shard0 = shards[0].clone();
        let mut shards_opt: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        shards_opt[0] = None; // Simulate shard 0 being missing.
        let reconstructed = ec_reconstruct_shard(shards_opt, 2, 1, 0).unwrap();
        assert_eq!(reconstructed, original_shard0);
    }

    #[test]
    fn test_reconstruct_shard_parity() {
        let payload: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let shards = ec_encode(&payload, 2, 1).unwrap();
        let original_parity = shards[2].clone();
        let mut shards_opt: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        shards_opt[2] = None; // Simulate parity shard missing.
        let reconstructed = ec_reconstruct_shard(shards_opt, 2, 1, 2).unwrap();
        assert_eq!(reconstructed, original_parity);
    }

    #[test]
    fn test_shard_size_formula() {
        // ceil(payload_len / data_shards), min 1 (no +4 trailer rounding).
        assert_eq!(shard_size(0, 2), 1); // empty → 1 (RS needs non-zero shards)
        assert_eq!(shard_size(1, 2), 1); // ceil(1/2) = 1
        assert_eq!(shard_size(8, 2), 4); // ceil(8/2) = 4
        assert_eq!(shard_size(100, 4), 25); // ceil(100/4) = 25
        assert_eq!(shard_size(1024, 6), 171); // ceil(1024/6) = 171
        assert_eq!(shard_size(6144, 3), 2048); // exact division
    }

    #[test]
    fn test_shard_size_all_equal() {
        let payload: Vec<u8> = (0..999).map(|i| i as u8).collect();
        let shards = ec_encode(&payload, 3, 2).unwrap();
        let first_len = shards[0].len();
        for s in &shards {
            assert_eq!(s.len(), first_len, "all shards must have equal length");
        }
    }

    /// Verify that data shards contain raw payload slices — the fast-path
    /// assumption that sub-range reads from a data shard return original bytes.
    #[test]
    fn test_data_shards_contain_raw_payload() {
        let payload: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let shards = ec_encode(&payload, 2, 1).unwrap();
        let per_shard = shards[0].len();

        // shard[0] should contain payload[0..per_shard] verbatim (no trailer)
        assert_eq!(
            &shards[0][..per_shard.min(payload.len())],
            &payload[..per_shard.min(payload.len())]
        );

        // Reading a sub-range from shard[0] gives the original payload bytes
        assert_eq!(&shards[0][100..200], &payload[100..200]);

        // shard[1] starts at payload[per_shard..]
        let remaining = payload.len() - per_shard;
        assert_eq!(
            &shards[1][..remaining],
            &payload[per_shard..per_shard + remaining]
        );
    }

    /// Verify that partial shards cannot be decoded — this is the bug that
    /// ec_read_from_extent had (passing offset/length to all shards).
    #[test]
    fn test_partial_shards_cannot_decode() {
        let payload: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let shards = ec_encode(&payload, 2, 1).unwrap();
        let _per_shard = shards[0].len();

        // Take a sub-range from each shard (simulating the old buggy behavior)
        let partial: Vec<Option<Vec<u8>>> =
            shards.iter().map(|s| Some(s[100..200].to_vec())).collect();

        // This should fail because partial shards have wrong length for RS decode
        let result = ec_decode(partial, 2, 1, payload.len());
        // The decode itself may "succeed" but return garbage (shards too short).
        // Either way, the output won't match the expected payload sub-range.
        if let Ok(decoded) = result {
            // If it somehow decodes, the result is garbage — not payload[100..200]
            assert_ne!(
                decoded,
                payload[100..200].to_vec(),
                "partial shard decode should NOT return correct payload sub-range"
            );
        }
        // If it errors, that's also correct — partial shards can't be decoded.
    }

    /// 2+1 EC: missing one data shard, decode from remaining data + parity.
    #[test]
    fn test_2_plus_1_missing_data_shard_0() {
        let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let shards = ec_encode(&payload, 2, 1).unwrap();
        let mut shards_opt: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        shards_opt[0] = None; // data shard 0 gone
        let decoded = ec_decode(shards_opt, 2, 1, payload.len()).unwrap();
        assert_eq!(decoded, payload, "should recover from 1 data + 1 parity");
    }

    /// 2+1 EC: missing data shard 1, decode from shard 0 + parity.
    #[test]
    fn test_2_plus_1_missing_data_shard_1() {
        let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let shards = ec_encode(&payload, 2, 1).unwrap();
        let mut shards_opt: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        shards_opt[1] = None; // data shard 1 gone
        let decoded = ec_decode(shards_opt, 2, 1, payload.len()).unwrap();
        assert_eq!(decoded, payload, "should recover from 1 data + 1 parity");
    }

    /// verify that `ec_reconstruct_shard` operates correctly on
    /// SUB-RANGE shard slices (not full-size shards). This is the
    /// load-bearing invariant for `ec_reconstruct_shard_subrange` in
    /// `client.rs` — without it, sub-range reconstruction would have
    /// to fall back to full-extent decode (the earlier hot path that
    /// caused macOS GC pressure).
    ///
    /// Galois-8 RS encodes byte-by-byte: at each row position `i` the
    /// `(K + M)` shard bytes form an independent RS codeword. So a
    /// sub-range `[a, b)` of length N on the missing shard can be
    /// reconstructed from `[a, b)` of length N on K healthy shards.
    #[test]
    fn reconstruct_shard_subrange_data_shard() {
        let payload: Vec<u8> = (0..(1 << 16)).map(|i| (i % 251) as u8).collect();
        let data_shards = 3usize;
        let parity_shards = 1usize;
        let shards = ec_encode(&payload, data_shards, parity_shards).unwrap();
        let per_shard = shards[0].len();
        // Take a sub-range [a, b) within the shard, away from edges.
        let a = 1024usize;
        let b = 5120usize;
        assert!(b <= per_shard);

        // Pre-compute the truth: shard 0's bytes at [a, b).
        let truth = shards[0][a..b].to_vec();

        // Build sub-range shards: shard 0 missing, others present at [a, b).
        let sub_shards: Vec<Option<Vec<u8>>> = (0..(data_shards + parity_shards))
            .map(|i| {
                if i == 0 {
                    None
                } else {
                    Some(shards[i][a..b].to_vec())
                }
            })
            .collect();

        let reconstructed =
            ec_reconstruct_shard(sub_shards, data_shards, parity_shards, 0).unwrap();
        assert_eq!(
            reconstructed, truth,
            "sub-range reconstruction must match the original shard's sub-range"
        );
    }

    /// same property holds for K-1 healthy data shards + 1 parity
    /// (the typical online-failure scenario where one data-shard host
    /// is offline). Reconstructs shard 1's sub-range from shards
    /// {0, 2, parity}.
    #[test]
    fn reconstruct_shard_subrange_with_one_parity_present() {
        let payload: Vec<u8> = (0..(1 << 18)).map(|i| (i % 251) as u8).collect();
        let data_shards = 3usize;
        let parity_shards = 1usize;
        let shards = ec_encode(&payload, data_shards, parity_shards).unwrap();
        let per_shard = shards[0].len();
        let a = 100usize;
        let b = per_shard - 100;
        let truth = shards[1][a..b].to_vec();

        let sub_shards: Vec<Option<Vec<u8>>> = vec![
            Some(shards[0][a..b].to_vec()),
            None, // missing
            Some(shards[2][a..b].to_vec()),
            Some(shards[3][a..b].to_vec()), // parity
        ];

        let reconstructed =
            ec_reconstruct_shard(sub_shards, data_shards, parity_shards, 1).unwrap();
        assert_eq!(reconstructed, truth);
    }

    /// verify the shard_size detection condition used by
    /// handle_convert_to_ec to detect a crash between rename(.ec.dat → .dat)
    /// and save_meta. After the crash, local file len == shard_size but meta
    /// still has the old eversion. The condition is:
    ///   local_len < sealed_length && local_len == expected_shard
    #[test]
    fn shard_size_detection_for_crash_recovery() {
        for &(payload_len, data_shards) in &[
            (1024usize, 2usize),
            (4096, 3),
            (10000, 3),
            (65536, 4),
            (1 << 20, 3),
        ] {
            let expected_shard = shard_size(payload_len, data_shards);
            assert!(
                expected_shard < payload_len,
                "shard_size({payload_len}, {data_shards}) = {expected_shard} should be < {payload_len}"
            );
            let local_len = expected_shard as u64;
            let sealed_length = payload_len as u64;
            let detected = local_len < sealed_length && local_len == expected_shard as u64;
            assert!(
                detected,
                "crash-recovery detection should fire for payload={payload_len}, data_shards={data_shards}"
            );

            let not_crash = payload_len as u64;
            let not_detected = not_crash < sealed_length && not_crash == expected_shard as u64;
            assert!(
                !not_detected,
                "full payload should NOT trigger crash-recovery detection"
            );
        }
    }

    /// The chunked EC-convert path must produce byte-for-byte the same shards
    /// as the whole-extent `ec_encode`. Encode each stripe with
    /// `ec_encode_stripe` and assert the reassembled shards equal `ec_encode`.
    #[test]
    fn ec_encode_stripe_matches_whole() {
        for &(payload_len, k, m) in &[
            (1usize, 2usize, 1usize),
            (5, 3, 1),
            (4096, 3, 1),
            (10_000, 3, 2),
            (65_537, 4, 1),
            (1 << 20, 3, 1),
            ((1 << 20) + 7, 5, 2),
        ] {
            // Deterministic pseudo-random payload (no Math.random in tests).
            let payload: Vec<u8> = (0..payload_len)
                .map(|i| ((i * 2654435761usize) >> 13) as u8 ^ (i as u8).wrapping_mul(31))
                .collect();
            let whole = ec_encode(&payload, k, m).expect("whole encode");
            let per_shard = shard_size(payload_len, k);

            for &stripe in &[1usize, 7, 1000, per_shard.max(1), per_shard + 5] {
                // Reassemble shards from stripe encodes.
                let mut got: Vec<Vec<u8>> = (0..k + m).map(|_| vec![0u8; per_shard]).collect();
                let mut s = 0usize;
                while s < per_shard {
                    let stripe_len = (per_shard - s).min(stripe);
                    // Build K data stripes (zero-padded past payload).
                    let data_bufs: Vec<Vec<u8>> = (0..k)
                        .map(|i| {
                            let start = i * per_shard + s;
                            let mut b = vec![0u8; stripe_len];
                            let avail = payload_len.saturating_sub(start).min(stripe_len);
                            if avail > 0 {
                                b[..avail].copy_from_slice(&payload[start..start + avail]);
                            }
                            b
                        })
                        .collect();
                    let data_refs: Vec<&[u8]> = data_bufs.iter().map(|v| v.as_slice()).collect();
                    let parity = ec_encode_stripe(&data_refs, m).expect("stripe encode");
                    // Place data + parity stripes back at offset s.
                    for i in 0..k {
                        got[i][s..s + stripe_len].copy_from_slice(&data_bufs[i]);
                    }
                    for j in 0..m {
                        got[k + j][s..s + stripe_len].copy_from_slice(&parity[j]);
                    }
                    s += stripe_len;
                }
                assert_eq!(
                    got, whole,
                    "stripe={stripe} payload_len={payload_len} k={k} m={m}: reassembled shards \
                     must equal whole ec_encode"
                );
            }
        }
    }
}
