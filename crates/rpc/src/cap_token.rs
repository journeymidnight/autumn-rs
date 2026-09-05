//! Ed25519 capability-token codec — the shared wire + crypto core for
//! data-plane authz (`docs/data_plane_authz_design.md`).
//!
//! The manager (leader) acts as a KDC and **signs** short-TTL capability
//! tokens with an Ed25519 private key. The partition-server (KV layer)
//! **verifies** them with only the public key — so a compromised PS can
//! verify but never forge (asymmetric, unlike Ceph's shared-secret cephx).
//! The client just forwards the opaque token bytes.
//!
//! This ONE module is the single source of truth for the claims layout, the
//! canonical signing bytes, and the domain-separation prefix, so the signer
//! (manager) and the verifier (PS) can never drift.
//!
//! **This file participates in the wire fingerprint** (`build.rs` hashes it):
//! the token layout is wire schema, so any change to `CapClaims` bumps
//! a conscious `WIRE_VERSION_MAX` bump — exactly
//! like `manager_rpc.rs` / `partition_rpc.rs`.

// ═══════════════════════════════════════════════════════════════════════════
//  ⚠️  WIRE SCHEMA. Edit an `Archive` type here — add, remove, reorder or
//  retype a field, or change what one MEANS — and you MUST bump
//  `WIRE_VERSION_MAX` (and set `MIN = MAX`) in `crates/rpc/src/lib.rs`.
//
//  NOTHING CHECKS THIS FOR YOU. The schema fingerprint that used to catch a
//  forgotten bump was removed; the version integer is the only guard left,
//  and it is maintained by hand. Forget it and two binaries claiming the same
//  version will handshake, then decode each other's bytes as garbage — no
//  error, no log, just wrong data. That has happened here before.
// ═══════════════════════════════════════════════════════════════════════════

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use rkyv::{Archive, Deserialize, Serialize};

/// Domain-separation prefix mixed into every signature input, so a signature
/// minted for a data-plane capability can never be replayed as (or confused
/// with) any other Ed25519 signature in the system. Changing this string
/// invalidates every previously-issued token.
pub const CAP_DOMAIN: &[u8] = b"autumn-rs data-plane cap v1";

/// The `typ` tag every capability token carries. Guards against a token minted
/// for a different purpose being accepted here (type confusion).
pub const CAP_TYP: &str = "autumn.cap.v1";

/// The only `ver` this build issues / accepts. Additive fields would bump this.
pub const CAP_VER: u32 = 1;

/// An Ed25519 signature is exactly 64 bytes. The wire token is
/// `canonical_claims_bytes ‖ sig[64]`; the claims length is therefore
/// `token.len() - SIG_LEN` (no length prefix needed — the signature is fixed).
pub const SIG_LEN: usize = 64;

/// Hard upper bound on a wire token, enforced at the TOP of `verify_token`
/// BEFORE any decode. `verify_token` runs on the PS data-plane verify path
/// against UNAUTHENTICATED input, and decoding claims allocates a `String` +
/// `Vec<Vec<u8>>` — without this cap an attacker could send a large,
/// bytecheck-valid-but-unsigned claims blob to force allocation + CPU before
/// the signature check rejects it (remote resource exhaustion, coco P1). A
/// legitimate token is ~300 bytes (small claims + 64 B sig); 8 KiB is very
/// generous (room for ~100+ prefixes) while bounding the untrusted work.
pub const MAX_CAP_TOKEN_LEN: usize = 8 * 1024;

/// Claims of an Ed25519-signed capability token.
///
/// rkyv gives a deterministic byte layout, but correctness does NOT rely on
/// re-encoding determinism: the token carries the exact `canonical_bytes` that
/// were signed, and `verify_token` verifies over *those* carried bytes (not a
/// re-encode), then decodes them. Determinism only matters so an independent
/// re-mint of identical claims produces an identical token — not for verify.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CapClaims {
    /// Claims format version (see [`CAP_VER`]). Lets us add fields compatibly.
    pub ver: u32,
    /// Token type tag (see [`CAP_TYP`]). Guards against cross-purpose reuse.
    pub typ: String,
    /// Which public key verifies this token → enables multi-key rotation.
    pub kid: u32,
    /// Issuer, e.g. `"autumn-mgr"`.
    pub iss: String,
    /// Audience = the `cluster_id`. A token is only valid for its cluster, so a
    /// dev token can't be replayed against prod (and vice-versa).
    pub aud: String,
    /// Issued-at (unix seconds).
    pub iat: u64,
    /// Not-before (unix seconds). Reject before this.
    pub nbf: u64,
    /// Expiry (unix seconds). Reject after this. Kept SHORT (prod hours).
    pub exp: u64,
    /// Authorized key prefixes. A request key must `starts_with` one of these
    /// to be admitted. Each prefix MUST end with `b'/'` (a normalized segment
    /// boundary) so `mem/ac` can't match `mem/acme/`.
    pub allowed_prefixes: Vec<Vec<u8>>,
}

/// Why a token was rejected. The PS surfaces these as distinct auth-failure
/// metrics (expired / not-yet-valid / bad-signature / …) per the design.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthReject {
    /// Token shorter than a signature, or claims bytes failed rkyv validation.
    Malformed,
    /// `typ` / `ver` did not match this build's [`CAP_TYP`] / [`CAP_VER`].
    WrongType,
    /// No enabled public key for the token's `kid` (unknown or disabled kid).
    UnknownKid,
    /// Ed25519 signature did not verify against the resolved public key.
    BadSignature,
    /// `now + skew < nbf` — token not valid yet.
    NotYetValid,
    /// `exp + skew < now` — token expired.
    Expired,
}

impl AuthReject {
    /// Stable short label for metrics / logs.
    pub fn label(self) -> &'static str {
        match self {
            AuthReject::Malformed => "malformed",
            AuthReject::WrongType => "wrong_type",
            AuthReject::UnknownKid => "unknown_kid",
            AuthReject::BadSignature => "bad_signature",
            AuthReject::NotYetValid => "not_yet_valid",
            AuthReject::Expired => "expired",
        }
    }
}

impl core::fmt::Display for AuthReject {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.label())
    }
}

/// Serialize claims to their canonical signing bytes (rkyv deterministic layout).
fn canonical_bytes(claims: &CapClaims) -> Result<Vec<u8>, String> {
    rkyv::to_bytes::<rkyv::rancor::Error>(claims)
        .map(|b| b.to_vec())
        .map_err(|e| format!("cap claims encode: {e}"))
}

/// Decode canonical claims bytes with rkyv's checked (bytecheck) path — SAFE on
/// untrusted input (the token arrives from the network before its signature is
/// verified; we must read `kid` to pick the key).
fn decode_claims(bytes: &[u8]) -> Result<CapClaims, String> {
    let mut v = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
    v.extend_from_slice(bytes);
    rkyv::from_bytes::<CapClaims, rkyv::rancor::Error>(&v).map_err(|e| format!("cap claims decode: {e}"))
}

/// Sign `claims` into a wire token: `canonical_bytes(claims) ‖ sig[64]`, where
/// `sig = Ed25519(sk, CAP_DOMAIN ‖ canonical_bytes)`. Manager-side.
pub fn sign_claims(sk: &SigningKey, claims: &CapClaims) -> Result<Vec<u8>, String> {
    let cb = canonical_bytes(claims)?;
    let mut msg = Vec::with_capacity(CAP_DOMAIN.len() + cb.len());
    msg.extend_from_slice(CAP_DOMAIN);
    msg.extend_from_slice(&cb);
    let sig = sk.sign(&msg);
    let mut token = Vec::with_capacity(cb.len() + SIG_LEN);
    token.extend_from_slice(&cb);
    token.extend_from_slice(&sig.to_bytes());
    Ok(token)
}

/// Verify + decode a wire token. `resolve_key(kid)` returns the enabled public
/// key for that kid, or `None` if the kid is unknown / disabled. `now` is unix
/// seconds; `skew` is the clock-skew leeway (seconds) applied to `nbf`/`exp`.
///
/// On success returns the validated `CapClaims`; the caller then enforces
/// `aud == cluster_id` and the per-request `allowed_prefixes` check.
///
/// The signature is verified over the token's *carried* claims bytes (not a
/// re-encode), and `verify_strict` rejects non-canonical / small-order points.
pub fn verify_token(
    token: &[u8],
    resolve_key: impl Fn(u32) -> Option<VerifyingKey>,
    now: u64,
    skew: u64,
) -> Result<CapClaims, AuthReject> {
    // Bound the UNAUTHENTICATED input BEFORE any allocation/decode (coco P1
    // DoS): reject too-short (no room for a sig) or absurdly large tokens.
    if token.len() <= SIG_LEN || token.len() > MAX_CAP_TOKEN_LEN {
        return Err(AuthReject::Malformed);
    }
    let (cb, sigb) = token.split_at(token.len() - SIG_LEN);

    // Decode (checked) to read kid + fields. This is untrusted input; the
    // bytecheck pass guards against malformed/hostile bytes before we touch it.
    let claims = decode_claims(cb).map_err(|_| AuthReject::Malformed)?;
    if claims.typ != CAP_TYP || claims.ver != CAP_VER {
        return Err(AuthReject::WrongType);
    }

    let vk = resolve_key(claims.kid).ok_or(AuthReject::UnknownKid)?;
    let sig_arr: [u8; SIG_LEN] = sigb.try_into().map_err(|_| AuthReject::Malformed)?;
    let sig = Signature::from_bytes(&sig_arr);

    let mut msg = Vec::with_capacity(CAP_DOMAIN.len() + cb.len());
    msg.extend_from_slice(CAP_DOMAIN);
    msg.extend_from_slice(cb);
    vk.verify_strict(&msg, &sig)
        .map_err(|_| AuthReject::BadSignature)?;

    // Time window (with skew leeway). `saturating_add` so a huge exp/skew
    // can't wrap (coco P2) — a saturated bound just means "always in window
    // on that side", never a silent wrap into the opposite verdict.
    if now.saturating_add(skew) < claims.nbf {
        return Err(AuthReject::NotYetValid);
    }
    if claims.exp.saturating_add(skew) < now {
        return Err(AuthReject::Expired);
    }
    Ok(claims)
}

/// Derive the 32-byte Ed25519 public key from a 32-byte private seed. Used by
/// key-file loading (manager) and `autumn-op gen-signing-key` so the published
/// public key is always derived from the seed, never stored separately (no drift).
pub fn public_key_from_seed(seed: &[u8; 32]) -> [u8; 32] {
    SigningKey::from_bytes(seed).verifying_key().to_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sk_from_seed(b: u8) -> SigningKey {
        SigningKey::from_bytes(&[b; 32])
    }

    fn claims_at(now: u64, ttl: u64) -> CapClaims {
        CapClaims {
            ver: CAP_VER,
            typ: CAP_TYP.to_string(),
            kid: 1,
            iss: "autumn-mgr".to_string(),
            aud: "cluster-abc".to_string(),
            iat: now,
            nbf: now,
            exp: now + ttl,
            allowed_prefixes: vec![b"mem/acme/".to_vec()],
        }
    }

    #[test]
    fn mint_then_verify_roundtrips() {
        let sk = sk_from_seed(7);
        let vk = sk.verifying_key();
        let now = 1_000_000;
        let claims = claims_at(now, 3600);
        let token = sign_claims(&sk, &claims).unwrap();

        let got = verify_token(&token, |kid| (kid == 1).then_some(vk), now, 60).unwrap();
        assert_eq!(got, claims);
        assert_eq!(got.allowed_prefixes, vec![b"mem/acme/".to_vec()]);
    }

    #[test]
    fn expired_token_is_rejected() {
        let sk = sk_from_seed(7);
        let vk = sk.verifying_key();
        let now = 1_000_000;
        let token = sign_claims(&sk, &claims_at(now, 100)).unwrap();
        // now well past exp, beyond skew leeway.
        let err = verify_token(&token, |_| Some(vk), now + 100 + 61, 60).unwrap_err();
        assert_eq!(err, AuthReject::Expired);
        // still inside skew leeway → ok.
        assert!(verify_token(&token, |_| Some(vk), now + 100 + 30, 60).is_ok());
    }

    #[test]
    fn not_yet_valid_is_rejected() {
        let sk = sk_from_seed(7);
        let vk = sk.verifying_key();
        let now = 1_000_000;
        let mut claims = claims_at(now, 3600);
        claims.nbf = now + 500;
        let token = sign_claims(&sk, &claims).unwrap();
        let err = verify_token(&token, |_| Some(vk), now, 60).unwrap_err();
        assert_eq!(err, AuthReject::NotYetValid);
    }

    #[test]
    fn claims_byte_flip_fails_signature() {
        let sk = sk_from_seed(7);
        let vk = sk.verifying_key();
        let now = 1_000_000;
        let mut token = sign_claims(&sk, &claims_at(now, 3600)).unwrap();
        // Flip a byte inside the claims region (not the signature).
        token[4] ^= 0xFF;
        let err = verify_token(&token, |_| Some(vk), now, 60).unwrap_err();
        // Either the claims no longer decode, or the signature no longer matches
        // — both are hard rejects (never accepted).
        assert!(matches!(err, AuthReject::BadSignature | AuthReject::Malformed | AuthReject::WrongType));
    }

    #[test]
    fn signature_byte_flip_fails() {
        let sk = sk_from_seed(7);
        let vk = sk.verifying_key();
        let now = 1_000_000;
        let mut token = sign_claims(&sk, &claims_at(now, 3600)).unwrap();
        let last = token.len() - 1;
        token[last] ^= 0x01;
        let err = verify_token(&token, |_| Some(vk), now, 60).unwrap_err();
        assert_eq!(err, AuthReject::BadSignature);
    }

    #[test]
    fn wrong_public_key_fails() {
        let sk = sk_from_seed(7);
        let other_vk = sk_from_seed(9).verifying_key();
        let now = 1_000_000;
        let token = sign_claims(&sk, &claims_at(now, 3600)).unwrap();
        let err = verify_token(&token, |_| Some(other_vk), now, 60).unwrap_err();
        assert_eq!(err, AuthReject::BadSignature);
    }

    #[test]
    fn unknown_or_disabled_kid_fails() {
        let sk = sk_from_seed(7);
        let now = 1_000_000;
        let token = sign_claims(&sk, &claims_at(now, 3600)).unwrap();
        // resolver returns None (kid disabled / unknown) → reject before verify.
        let err = verify_token(&token, |_| None, now, 60).unwrap_err();
        assert_eq!(err, AuthReject::UnknownKid);
    }

    #[test]
    fn wrong_type_or_version_fails() {
        let sk = sk_from_seed(7);
        let vk = sk.verifying_key();
        let now = 1_000_000;
        let mut claims = claims_at(now, 3600);
        claims.typ = "autumn.cap.v2".to_string();
        // Re-sign so the signature is valid; only the type is wrong.
        let token = sign_claims(&sk, &claims).unwrap();
        let err = verify_token(&token, |_| Some(vk), now, 60).unwrap_err();
        assert_eq!(err, AuthReject::WrongType);
    }

    #[test]
    fn truncated_token_is_malformed() {
        let sk = sk_from_seed(7);
        let vk = sk.verifying_key();
        let now = 1_000_000;
        let token = sign_claims(&sk, &claims_at(now, 3600)).unwrap();
        let err = verify_token(&token[..SIG_LEN], |_| Some(vk), now, 60).unwrap_err();
        assert_eq!(err, AuthReject::Malformed);
    }

    #[test]
    fn oversize_token_rejected_before_decode() {
        let vk = sk_from_seed(7).verifying_key();
        // A blob larger than the cap is rejected as Malformed WITHOUT decoding
        // (coco P1 DoS guard). Also assert a legit token is well under the cap.
        let big = vec![0u8; MAX_CAP_TOKEN_LEN + 1];
        assert_eq!(
            verify_token(&big, |_| Some(vk), 1_000_000, 60).unwrap_err(),
            AuthReject::Malformed
        );
        let token = sign_claims(&sk_from_seed(7), &claims_at(1_000_000, 3600)).unwrap();
        assert!(token.len() < MAX_CAP_TOKEN_LEN);
    }

    #[test]
    fn huge_ttl_and_skew_do_not_overflow() {
        let sk = sk_from_seed(7);
        let vk = sk.verifying_key();
        // exp = now + u64::MAX would wrap without saturating_add; a token with a
        // saturated exp verifies fine, and a huge skew never wraps a verdict.
        let mut claims = claims_at(1_000_000, 0);
        claims.exp = u64::MAX;
        let token = sign_claims(&sk, &claims).unwrap();
        assert!(verify_token(&token, |_| Some(vk), 2_000_000, u64::MAX).is_ok());
    }

    #[test]
    fn public_key_from_seed_matches_dalek() {
        let seed = [42u8; 32];
        let expect = SigningKey::from_bytes(&seed).verifying_key().to_bytes();
        assert_eq!(public_key_from_seed(&seed), expect);
    }
}
