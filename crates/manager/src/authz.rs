//! F-AUTHZ-1: manager-side KDC key material + credential helpers.
//!
//! This is CONFIG loading (not wire). The token codec + claims layout live in
//! `autumn_rpc::cap_token` (the shared signer/verifier source of truth). Here
//! we only: parse the signing-key file, pick the active signing key, publish
//! the public keys, and hash/compare credentials. See
//! `docs/data_plane_authz_design.md`.

use std::collections::BTreeMap;

use autumn_rpc::manager_rpc::AuthzPublicKey;
use ed25519_dalek::SigningKey;
use sha2::{Digest, Sha256};

/// One signing key parsed from the key file.
struct KeyEntry {
    sk: SigningKey,
    disabled: bool,
}

/// The manager's Ed25519 signing keyring (KDC private material). Loaded from
/// `--auth-signing-key-file`. Its mere presence on the manager = authz enabled.
pub struct AuthzKeyring {
    /// kid → key. `BTreeMap` so "active = highest enabled kid" is a cheap
    /// reverse scan and iteration order is deterministic.
    keys: BTreeMap<u32, KeyEntry>,
}

impl AuthzKeyring {
    /// Parse the signing-key file. Format — one key per line:
    /// ```text
    /// <kid> <hex-32-byte-seed> [disabled]
    /// ```
    /// Blank lines and `#` comments are ignored. Fail-loud on ANY malformed
    /// line: a bad KDC key file must never silently start authz half-armed.
    pub fn from_file_contents(text: &str) -> Result<Self, String> {
        let mut keys = BTreeMap::new();
        for (idx, raw) in text.lines().enumerate() {
            let lineno = idx + 1;
            // Strip trailing `# comment`, then trim.
            let line = raw.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let mut it = line.split_whitespace();
            let kid_s = it
                .next()
                .ok_or_else(|| format!("line {lineno}: missing kid"))?;
            let seed_s = it
                .next()
                .ok_or_else(|| format!("line {lineno}: missing seed"))?;
            let flag = it.next();
            if it.next().is_some() {
                return Err(format!("line {lineno}: too many fields"));
            }
            let kid: u32 = kid_s
                .parse()
                .map_err(|_| format!("line {lineno}: bad kid {kid_s:?}"))?;
            let seed = decode_hex32(seed_s).map_err(|e| format!("line {lineno}: {e}"))?;
            let disabled = match flag {
                None => false,
                Some("disabled") => true,
                Some(other) => return Err(format!("line {lineno}: unknown flag {other:?}")),
            };
            if keys
                .insert(
                    kid,
                    KeyEntry {
                        sk: SigningKey::from_bytes(&seed),
                        disabled,
                    },
                )
                .is_some()
            {
                return Err(format!("line {lineno}: duplicate kid {kid}"));
            }
        }
        if keys.is_empty() {
            return Err("signing-key file has no keys".to_string());
        }
        if !keys.values().any(|e| !e.disabled) {
            return Err("signing-key file has no ENABLED key (cannot mint)".to_string());
        }
        Ok(Self { keys })
    }

    /// The kid used to sign NEW tokens = the highest-numbered ENABLED key.
    /// (Rotation: add a higher kid, mint moves to it; disable the old one once
    /// its tokens have expired.)
    pub fn active(&self) -> Option<(u32, &SigningKey)> {
        self.keys
            .iter()
            .rev()
            .find(|(_, e)| !e.disabled)
            .map(|(k, e)| (*k, &e.sk))
    }

    /// The public keys to publish to the PS (ALL kids incl. disabled, so the PS
    /// learns to reject a disabled kid's tokens).
    pub fn published(&self) -> Vec<AuthzPublicKey> {
        self.keys
            .iter()
            .map(|(kid, e)| AuthzPublicKey {
                kid: *kid,
                ed25519_pub: e.sk.verifying_key().to_bytes().to_vec(),
                disabled: e.disabled,
            })
            .collect()
    }
}

/// Decode exactly 64 hex chars into a 32-byte seed.
fn decode_hex32(s: &str) -> Result<[u8; 32], String> {
    // ASCII-guard BEFORE the byte-index slicing below (coco P3): a 64-BYTE seed
    // containing a multi-byte UTF-8 char would make `&s[i*2..i*2+2]` slice mid-
    // char and panic; we want a diagnosable Err (fail-loud via Result), not a
    // crash. ASCII ⇒ 1 byte per char ⇒ the slicing is always on char boundaries.
    if !s.is_ascii() {
        return Err("seed must be ASCII hex".to_string());
    }
    if s.len() != 64 {
        return Err(format!("seed must be 64 hex chars, got {}", s.len()));
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .map_err(|_| "seed is not valid hex".to_string())?;
    }
    Ok(out)
}

/// SHA-256 of a credential. Deterministic across processes — used both to store
/// the account hash and to verify (constant-time) a presented credential.
pub fn credential_hash(cred: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(cred);
    h.finalize().into()
}

/// Constant-time equality of two 32-byte hashes (no early exit → no timing
/// oracle on the secret).
pub fn ct_eq_32(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for i in 0..32 {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// Constant-time equality of two secrets given as strings (e.g. admin tokens).
/// Both are SHA-256'd first, so the compare is fixed-width and independent of
/// the (public-ish) length — avoids leaking length via an early return.
pub fn ct_eq_secret(a: &str, b: &str) -> bool {
    ct_eq_32(&credential_hash(a.as_bytes()), &credential_hash(b.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_multi_key_with_comments_and_disabled() {
        let text = "\
# KDC signing keys
1 0000000000000000000000000000000000000000000000000000000000000001
2 0000000000000000000000000000000000000000000000000000000000000002   # rotated in
3 0000000000000000000000000000000000000000000000000000000000000003 disabled
";
        let kr = AuthzKeyring::from_file_contents(text).unwrap();
        // active = highest ENABLED = kid 2 (3 is disabled).
        let (kid, _sk) = kr.active().unwrap();
        assert_eq!(kid, 2);
        let pubs = kr.published();
        assert_eq!(pubs.len(), 3);
        assert!(pubs.iter().find(|p| p.kid == 3).unwrap().disabled);
        assert!(!pubs.iter().find(|p| p.kid == 1).unwrap().disabled);
        // published pub key matches derivation from the seed.
        let expect1 =
            autumn_rpc::cap_token::public_key_from_seed(&{ let mut s = [0u8; 32]; s[31] = 1; s });
        assert_eq!(pubs.iter().find(|p| p.kid == 1).unwrap().ed25519_pub, expect1.to_vec());
    }

    #[test]
    fn rejects_malformed() {
        // bad hex length
        assert!(AuthzKeyring::from_file_contents("1 abc").is_err());
        // duplicate kid
        assert!(AuthzKeyring::from_file_contents(
            "1 0000000000000000000000000000000000000000000000000000000000000001\n\
             1 0000000000000000000000000000000000000000000000000000000000000002"
        )
        .is_err());
        // unknown flag
        assert!(AuthzKeyring::from_file_contents(
            "1 0000000000000000000000000000000000000000000000000000000000000001 bogus"
        )
        .is_err());
        // 64-BYTE but non-ASCII seed (62 ascii + one 2-byte 'é') — must Err, not
        // panic on a mid-char slice (coco P3).
        let non_ascii_seed = format!("1 {}é", "a".repeat(62));
        assert_eq!(non_ascii_seed.split_whitespace().nth(1).unwrap().len(), 64);
        assert!(AuthzKeyring::from_file_contents(&non_ascii_seed).is_err());
        // empty
        assert!(AuthzKeyring::from_file_contents("# only comments\n\n").is_err());
        // all disabled → cannot mint
        assert!(AuthzKeyring::from_file_contents(
            "1 0000000000000000000000000000000000000000000000000000000000000001 disabled"
        )
        .is_err());
    }

    #[test]
    fn credential_hash_and_ct_eq() {
        let h1 = credential_hash(b"s3cr3t");
        let h2 = credential_hash(b"s3cr3t");
        let h3 = credential_hash(b"other");
        assert!(ct_eq_32(&h1, &h2));
        assert!(!ct_eq_32(&h1, &h3));
        assert!(ct_eq_secret("admintok", "admintok"));
        assert!(!ct_eq_secret("admintok", "admintol"));
        assert!(!ct_eq_secret("admintok", "admintok2"));
    }
}
