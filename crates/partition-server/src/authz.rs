//! F-AUTHZ-1 Stage 2 — PS-side data-plane authorization enforcement.
//!
//! The PS is the KV-layer enforcement point (per the design decision "auth 在
//! kv 层做"). It holds ONLY the manager's PUBLIC verifying keys (fetched via
//! `MSG_GET_AUTHZ_CONFIG`), verifies a capability token ONCE per connection
//! (`MSG_AUTH_HELLO`), and does a per-request byte `starts_with` + `exp` check.
//! It NEVER calls the manager to enforce — the data plane is independent of the
//! control plane. See `docs/data_plane_authz_design.md`.
//!
//! Opt-in: with no signing key configured cluster-wide, `AuthzState.enabled`
//! stays false and the hot path is a single relaxed atomic load — fuse /
//! kvcache / dev deployments are untouched.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use autumn_rpc::cap_token::{verify_token, AuthReject};
use autumn_rpc::manager_rpc::GetAuthzConfigResp;
use autumn_rpc::partition_rpc::{
    self, parse_put_zc_meta, BatchGetReq, BatchPutReq, DeleteReq, GetReq, HeadReq, PutReq,
    RangeReq, MSG_AUTH_HELLO, MSG_BATCH_GET, MSG_BATCH_PUT, MSG_DELETE, MSG_GET, MSG_GET_REDIRECT,
    MSG_GET_ZC, MSG_HEAD, MSG_PUT, MSG_PUT_ZC, MSG_RANGE, PUT_ZC_HEADER_LEN,
};
use autumn_rpc::StatusCode;
use ed25519_dalek::VerifyingKey;
use parking_lot::RwLock;

/// Immutable snapshot of the authz config the PS enforces against.
pub struct AuthzInner {
    /// kid → verifying key. Only ENABLED kids are present — a disabled kid's
    /// tokens then reject as `UnknownKid` (emergency bulk revocation).
    pub keys: HashMap<u32, VerifyingKey>,
    /// Key prefixes under which default-DENY applies (e.g. `mem/`). A request
    /// key outside every protected prefix is NOT gated.
    pub protected_prefixes: Vec<Vec<u8>>,
    /// Clock-skew leeway (seconds) applied to token `exp`.
    pub clock_skew_secs: u64,
    /// This cluster's id = the required token `aud`. A token whose `aud` differs
    /// (minted for another cluster that shares signing keys) is rejected at
    /// AUTH_HELLO. Empty = unknown → the aud check is skipped (degraded).
    pub cluster_id: String,
}

impl AuthzInner {
    fn empty() -> Self {
        Self {
            keys: HashMap::new(),
            protected_prefixes: Vec::new(),
            clock_skew_secs: 60,
            cluster_id: String::new(),
        }
    }
}

/// Cross-thread shared authz runtime on `PartitionServer` (`Arc`, since
/// connection tasks run on the partition OS threads, not the main thread). The
/// main-thread poll loop swaps `inner`; connection tasks read it.
pub struct AuthzState {
    /// Fast gate: is enforcement on? A single relaxed load on the hot path when
    /// OFF (the common case).
    enabled: AtomicBool,
    inner: RwLock<Arc<AuthzInner>>,
}

impl Default for AuthzState {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthzState {
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            inner: RwLock::new(Arc::new(AuthzInner::empty())),
        }
    }

    /// Hot-path gate. `false` ⇒ no enforcement (single relaxed atomic load).
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Take a cheap `Arc` snapshot of the current config (one read-lock + one
    /// atomic bump), then drop the lock before using it.
    pub fn snapshot(&self) -> Arc<AuthzInner> {
        self.inner.read().clone()
    }

    /// Install a freshly-polled `GET_AUTHZ_CONFIG`. Builds the verify-keyring
    /// from the published public keys (skipping disabled kids). A malformed
    /// public key (wrong length / not on-curve) is skipped with the rest kept —
    /// its kid then rejects as `UnknownKid`, which is the safe direction.
    pub fn install(&self, resp: &GetAuthzConfigResp) {
        let mut keys = HashMap::new();
        for k in &resp.public_keys {
            if k.disabled {
                continue;
            }
            match <[u8; 32]>::try_from(k.ed25519_pub.as_slice()) {
                Ok(arr) => match VerifyingKey::from_bytes(&arr) {
                    Ok(vk) => {
                        keys.insert(k.kid, vk);
                    }
                    Err(e) => tracing::warn!(kid = k.kid, error = %e, "authz: bad public key, skipping kid"),
                },
                Err(_) => tracing::warn!(kid = k.kid, "authz: public key wrong length, skipping kid"),
            }
        }
        let inner = Arc::new(AuthzInner {
            keys,
            protected_prefixes: resp.protected_prefixes.clone(),
            clock_skew_secs: resp.clock_skew_secs,
            cluster_id: resp.cluster_id.clone(),
        });
        *self.inner.write() = inner;
        // Publish `enabled` AFTER the config is in place so a concurrent reader
        // never sees enabled==true with an empty keyring.
        self.enabled.store(resp.enabled, Ordering::Relaxed);
    }
}

/// The per-connection principal bound by a successful `MSG_AUTH_HELLO`.
#[derive(Clone, Debug)]
pub struct BoundPrincipal {
    pub allowed_prefixes: Vec<Vec<u8>>,
    /// Token expiry (unix seconds).
    pub exp: u64,
    /// The kid this token was verified against. Re-checked against the live
    /// keyring on EVERY request so a disabled/rotated-out kid revokes even
    /// already-bound long connections (coco P1: `install` drops disabled kids,
    /// but the per-request check is what enforces it on live connections).
    pub kid: u32,
}

/// Verify an `AUTH_HELLO` token against the cached public keys → the bound
/// principal, or a reject reason (string, for the AuthHelloResp message /
/// metrics). `now`/skew gate `nbf`/`exp`; the `aud` must equal this cluster's id
/// (when known) so a token minted for another cluster can't be replayed here.
pub fn verify_auth_hello(
    token: &[u8],
    inner: &AuthzInner,
    now: u64,
) -> Result<BoundPrincipal, String> {
    let claims = verify_token(
        token,
        |kid| inner.keys.get(&kid).copied(),
        now,
        inner.clock_skew_secs,
    )
    .map_err(|r: AuthReject| r.label().to_string())?;
    // aud must match this cluster (defends cross-cluster replay when signing
    // keys are shared). Skipped only when this PS doesn't know its cluster_id.
    if !inner.cluster_id.is_empty() && claims.aud != inner.cluster_id {
        return Err("wrong_audience".to_string());
    }
    Ok(BoundPrincipal {
        allowed_prefixes: claims.allowed_prefixes,
        exp: claims.exp,
        kid: claims.kid,
    })
}

/// Is `key` under any protected prefix?
fn is_protected(key: &[u8], protected: &[Vec<u8>]) -> bool {
    protected.iter().any(|p| key.starts_with(p))
}

fn denied(msg: &str) -> Option<(StatusCode, String)> {
    Some((StatusCode::PermissionDenied, msg.to_string()))
}

/// Authorize ONE key. `Some((code,msg))` = deny, `None` = allow.
fn check_key(
    key: &[u8],
    principal: Option<&BoundPrincipal>,
    inner: &AuthzInner,
    now: u64,
) -> Option<(StatusCode, String)> {
    if !is_protected(key, &inner.protected_prefixes) {
        return None; // ungated namespace — not our concern
    }
    let p = match principal {
        Some(p) => p,
        None => return denied("protected key requires a capability token (no AUTH_HELLO on this connection)"),
    };
    if !inner.keys.contains_key(&p.kid) {
        // kid disabled / rotated out since this connection bound — revoke it
        // (coco P1: closes the "emergency bulk revocation misses live conns" gap).
        return denied("signing key disabled/rotated; re-authenticate");
    }
    if now > p.exp.saturating_add(inner.clock_skew_secs) {
        return denied("capability token expired; re-authenticate");
    }
    if p.allowed_prefixes.iter().any(|ap| key.starts_with(ap)) {
        None
    } else {
        denied("key outside the connection's authorized prefixes")
    }
}

/// Authorize a RANGE scan. The WHOLE returnable interval must be ⊆ one allowed
/// prefix, else a request like `prefix=mem/, start=mem/acme/` would scan into
/// `mem/other/`. Returnable keys all `starts_with(prefix)`, so the interval is
/// ⊆ an allowed prefix `AP` iff `prefix.starts_with(AP)` — the prefix filter
/// itself is within a granted prefix. An empty prefix (unbounded scan) can
/// never be ⊆ a non-empty allowed prefix, so it's denied whenever it could
/// touch a protected range.
fn check_range(
    prefix: &[u8],
    principal: Option<&BoundPrincipal>,
    inner: &AuthzInner,
    now: u64,
) -> Option<(StatusCode, String)> {
    // Gated iff the scan could return a protected key: {k: starts_with(prefix)}
    // intersects {k: starts_with(PP)} iff prefix⊇PP or PP⊇prefix.
    let gated = inner
        .protected_prefixes
        .iter()
        .any(|pp| prefix.starts_with(pp) || pp.starts_with(prefix));
    if !gated {
        return None;
    }
    let p = match principal {
        Some(p) => p,
        None => return denied("protected range requires a capability token (no AUTH_HELLO on this connection)"),
    };
    if !inner.keys.contains_key(&p.kid) {
        return denied("signing key disabled/rotated; re-authenticate");
    }
    if now > p.exp.saturating_add(inner.clock_skew_secs) {
        return denied("capability token expired; re-authenticate");
    }
    if !prefix.is_empty() && p.allowed_prefixes.iter().any(|ap| prefix.starts_with(ap)) {
        None
    } else {
        denied("range prefix not within an authorized prefix (whole scan interval must be ⊆ one allowed prefix)")
    }
}

/// Per-request authorization gate, dispatched by `msg_type`. Returns
/// `Some((code, msg))` to REJECT (the caller synthesizes a `PermissionDenied`
/// error frame and skips serve/delegate), `None` to admit.
///
/// A frame that fails to decode returns `None` (admit) — the real handler will
/// reject it with `InvalidArgument`. This is safe because the gate and the
/// handler extract the key with the SAME decode (`rkyv_decode` for the rkyv
/// requests, `parse_put_zc_meta` for `MSG_PUT_ZC`): a payload that fails to
/// parse here fails identically in the handler, so no bytes are ever served.
///
/// **INVARIANT (load-bearing — an authz bypass = cross-tenant data exposure):
/// every client data-plane msg_type that carries a USER KEY must have an arm
/// here that extracts the key and calls `check_key` / `check_range`.** The
/// catch-all `_ => None` admits ungated, which is correct ONLY for
/// non-key-scoped ops (maintenance / split / merge / discards / diag — admin
/// auth is a separate concern) and `AUTH_HELLO` (handled by the connection
/// loop). Adding a new keyed read/write RPC without an arm here silently lets
/// it read/write any tenant's `mem/` prefix. If you add one, add it here too.
pub fn authz_check(
    msg_type: u8,
    payload: &[u8],
    principal: Option<&BoundPrincipal>,
    inner: &AuthzInner,
    now: u64,
) -> Option<(StatusCode, String)> {
    match msg_type {
        MSG_GET | MSG_GET_ZC | MSG_GET_REDIRECT => {
            let r = partition_rpc::rkyv_decode::<GetReq>(payload).ok()?;
            check_key(&r.key, principal, inner, now)
        }
        MSG_HEAD => {
            let r = partition_rpc::rkyv_decode::<HeadReq>(payload).ok()?;
            check_key(&r.key, principal, inner, now)
        }
        MSG_DELETE => {
            let r = partition_rpc::rkyv_decode::<DeleteReq>(payload).ok()?;
            check_key(&r.key, principal, inner, now)
        }
        MSG_PUT => {
            // Value copied by rkyv_decode, but MSG_PUT is only used for values
            // < 64 KiB (large values go MSG_PUT_ZC, key extracted below without
            // copying the value), so the copy is bounded — and only when authz
            // is enabled.
            let r = partition_rpc::rkyv_decode::<PutReq>(payload).ok()?;
            check_key(&r.key, principal, inner, now)
        }
        MSG_PUT_ZC => {
            // Zero value copy: the key is a slice of the binary meta header.
            let meta = parse_put_zc_meta(payload)?;
            let key = payload.get(PUT_ZC_HEADER_LEN..meta.value_offset)?;
            check_key(key, principal, inner, now)
        }
        MSG_RANGE => {
            let r = partition_rpc::rkyv_decode::<RangeReq>(payload).ok()?;
            check_range(&r.prefix, principal, inner, now)
        }
        MSG_BATCH_GET => {
            let r = partition_rpc::rkyv_decode::<BatchGetReq>(payload).ok()?;
            for k in &r.keys {
                if let Some(d) = check_key(k, principal, inner, now) {
                    return Some(d);
                }
            }
            None
        }
        MSG_BATCH_PUT => {
            let r = partition_rpc::rkyv_decode::<BatchPutReq>(payload).ok()?;
            for op in &r.ops {
                if let Some(d) = check_key(&op.key, principal, inner, now) {
                    return Some(d);
                }
            }
            None
        }
        // Catch-all = ADMIT ungated. Correct ONLY for non-key-scoped ops
        // (maintenance / split / merge / discards / diag) and AUTH_HELLO (bound
        // by the connection loop). A new KEYED data RPC landing here is an authz
        // bypass — see the INVARIANT on this fn's doc comment.
        _ => {
            debug_assert_ne!(msg_type, MSG_AUTH_HELLO);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_rpc::cap_token::{sign_claims, CapClaims, CAP_TYP, CAP_VER};
    use autumn_rpc::manager_rpc::AuthzPublicKey;
    use autumn_rpc::partition_rpc::rkyv_encode;
    use ed25519_dalek::SigningKey;

    fn inner_with(prefixes: Vec<Vec<u8>>) -> AuthzInner {
        // Include kid 1 so a principal bound to kid 1 passes the revocation check.
        let mut keys = HashMap::new();
        keys.insert(1u32, SigningKey::from_bytes(&[1u8; 32]).verifying_key());
        AuthzInner {
            keys,
            protected_prefixes: prefixes,
            clock_skew_secs: 60,
            cluster_id: String::new(),
        }
    }

    fn acme() -> BoundPrincipal {
        BoundPrincipal {
            allowed_prefixes: vec![b"mem/acme/".to_vec()],
            exp: 1_000_000,
            kid: 1,
        }
    }

    #[test]
    fn check_key_matrix() {
        let inner = inner_with(vec![b"mem/".to_vec()]);
        let p = acme();
        let now = 999_000;
        // authorized protected key → allow
        assert!(check_key(b"mem/acme/fact/1", Some(&p), &inner, now).is_none());
        // protected key outside allowed prefix → deny
        assert!(check_key(b"mem/other/fact/1", Some(&p), &inner, now).is_some());
        // anonymous on protected → deny
        assert!(check_key(b"mem/acme/fact/1", None, &inner, now).is_some());
        // non-protected key → allow (even anonymous)
        assert!(check_key(b"fuse/inode/1", None, &inner, now).is_none());
        // expired token → deny
        assert!(check_key(b"mem/acme/fact/1", Some(&p), &inner, p.exp + 61).is_some());
        // within skew leeway → allow
        assert!(check_key(b"mem/acme/fact/1", Some(&p), &inner, p.exp + 30).is_none());
    }

    #[test]
    fn prefix_boundary_not_forgeable() {
        // `mem/acme` (no trailing /) must NOT be authorized by allowed `mem/acme/`
        // — but percent-encoding + the trailing-/ normalization make a real
        // cross-tenant key like `mem/acmeevil/` fall outside `mem/acme/`.
        let inner = inner_with(vec![b"mem/".to_vec()]);
        let p = acme();
        let now = 999_000;
        assert!(check_key(b"mem/acmeevil/x", Some(&p), &inner, now).is_some());
        assert!(check_key(b"mem/acme/x", Some(&p), &inner, now).is_none());
    }

    #[test]
    fn range_whole_interval_subseteq_prefix() {
        let inner = inner_with(vec![b"mem/".to_vec()]);
        let p = acme();
        let now = 999_000;
        // prefix ⊆ allowed → allow
        assert!(check_range(b"mem/acme/", Some(&p), &inner, now).is_none());
        assert!(check_range(b"mem/acme/fact/", Some(&p), &inner, now).is_none());
        // prefix spans into other tenants (prefix ⊋ allowed) → deny
        assert!(check_range(b"mem/", Some(&p), &inner, now).is_some());
        // empty prefix (unbounded scan) touching protected → deny
        assert!(check_range(b"", Some(&p), &inner, now).is_some());
        // anonymous protected range → deny
        assert!(check_range(b"mem/acme/", None, &inner, now).is_some());
        // ungated range (outside protected) → allow
        assert!(check_range(b"fuse/", None, &inner, now).is_none());
    }

    #[test]
    fn authz_check_dispatch_get_and_put() {
        let inner = inner_with(vec![b"mem/".to_vec()]);
        let p = acme();
        let now = 999_000;
        // GET authorized
        let g = rkyv_encode(&GetReq {
            part_id: 1,
            key: b"mem/acme/doc/1".to_vec(),
            offset: 0,
            length: 0,
            region_epoch: 0,
        });
        assert!(authz_check(MSG_GET, &g, Some(&p), &inner, now).is_none());
        // GET cross-tenant denied
        let g2 = rkyv_encode(&GetReq {
            part_id: 1,
            key: b"mem/other/doc/1".to_vec(),
            offset: 0,
            length: 0,
            region_epoch: 0,
        });
        assert!(authz_check(MSG_GET, &g2, Some(&p), &inner, now).is_some());
        // PUT cross-tenant denied (value not copied into the assertion path)
        let put = rkyv_encode(&PutReq {
            part_id: 1,
            key: b"mem/other/doc/1".to_vec(),
            value: vec![7u8; 100],
            expires_at: 0,
            region_epoch: 0,
            inode_hint: 0,
            lease_epoch: 0,
        });
        assert!(authz_check(MSG_PUT, &put, Some(&p), &inner, now).is_some());
        // non-data-plane msg_type → not gated
        assert!(authz_check(0x47 /* MSG_MAINTENANCE */, &[], Some(&p), &inner, now).is_none());
        // authz off (no protected prefixes) → everything allowed
        let open = inner_with(vec![]);
        assert!(authz_check(MSG_GET, &g2, None, &open, now).is_none());
    }

    #[test]
    fn verify_auth_hello_binds_principal() {
        let sk = SigningKey::from_bytes(&[3u8; 32]);
        let vk = sk.verifying_key();
        let mut keys = HashMap::new();
        keys.insert(1u32, vk);
        let inner = AuthzInner {
            keys,
            protected_prefixes: vec![b"mem/".to_vec()],
            clock_skew_secs: 60,
            cluster_id: "cluster-x".to_string(),
        };
        let now = 1_000_000;
        let mk = |aud: &str| CapClaims {
            ver: CAP_VER,
            typ: CAP_TYP.to_string(),
            kid: 1,
            iss: "autumn-mgr".to_string(),
            aud: aud.to_string(),
            iat: now,
            nbf: now,
            exp: now + 3600,
            allowed_prefixes: vec![b"mem/acme/".to_vec()],
        };
        let token = sign_claims(&sk, &mk("cluster-x")).unwrap();
        let p = verify_auth_hello(&token, &inner, now).unwrap();
        assert_eq!(p.allowed_prefixes, vec![b"mem/acme/".to_vec()]);
        assert_eq!(p.exp, now + 3600);
        assert_eq!(p.kid, 1);
        // wrong kid (not in keyring) → reject
        let inner2 = AuthzInner {
            keys: HashMap::new(),
            protected_prefixes: vec![],
            clock_skew_secs: 60,
            cluster_id: "cluster-x".to_string(),
        };
        assert!(verify_auth_hello(&token, &inner2, now).is_err());
        // wrong audience (token minted for a DIFFERENT cluster) → reject (coco P1)
        let cross = sign_claims(&sk, &mk("cluster-OTHER")).unwrap();
        assert!(verify_auth_hello(&cross, &inner, now).is_err());
    }

    #[test]
    fn disabled_kid_revokes_live_connection() {
        // A principal bound to kid 1, then kid 1 is rotated out of the keyring
        // (disabled) → every subsequent request on that live connection denies.
        let inner_disabled = AuthzInner {
            keys: HashMap::new(), // kid 1 no longer present
            protected_prefixes: vec![b"mem/".to_vec()],
            clock_skew_secs: 60,
            cluster_id: String::new(),
        };
        let p = acme(); // kid 1
        assert!(check_key(b"mem/acme/fact/1", Some(&p), &inner_disabled, 999_000).is_some());
        assert!(check_range(b"mem/acme/", Some(&p), &inner_disabled, 999_000).is_some());
    }

    #[test]
    fn install_enables_and_builds_keyring() {
        let sk = SigningKey::from_bytes(&[5u8; 32]);
        let st = AuthzState::new();
        assert!(!st.is_enabled());
        st.install(&GetAuthzConfigResp {
            code: 0,
            message: String::new(),
            enabled: true,
            public_keys: vec![
                AuthzPublicKey {
                    kid: 1,
                    ed25519_pub: sk.verifying_key().to_bytes().to_vec(),
                    disabled: false,
                },
                AuthzPublicKey {
                    kid: 2,
                    ed25519_pub: vec![0u8; 32], // valid length; a zero key is on-curve-checkable
                    disabled: true,             // disabled → not in keyring
                },
            ],
            protected_prefixes: vec![b"mem/".to_vec()],
            token_ttl_secs: 3600,
            clock_skew_secs: 45,
            cluster_id: "cluster-abc".to_string(),
        });
        assert!(st.is_enabled());
        let snap = st.snapshot();
        assert!(snap.keys.contains_key(&1));
        assert!(!snap.keys.contains_key(&2)); // disabled kid excluded
        assert_eq!(snap.protected_prefixes, vec![b"mem/".to_vec()]);
        assert_eq!(snap.clock_skew_secs, 45);
        assert_eq!(snap.cluster_id, "cluster-abc");
    }
}
