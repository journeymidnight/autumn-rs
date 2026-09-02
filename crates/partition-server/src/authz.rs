//! Stage 2 — PS-side data-plane authorization enforcement.
//!
//! The PS is the KV-layer enforcement point (per the design decision that auth
//! is enforced at the KV layer). It holds ONLY the manager's PUBLIC verifying keys (fetched via
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
    self, parse_put_bulk_meta, BatchGetReq, BatchPutReq, DeleteReq, GetRedirectManyReq, GetReq,
    BatchPutBulkReq, HeadReq, PutReq, RangeReq, MSG_AUTH_HELLO, MSG_BATCH_GET,
    MSG_BATCH_GET_BULK, MSG_BATCH_PUT, MSG_BATCH_PUT_BULK, MSG_DELETE, MSG_GET,
    MSG_GET_REDIRECT, MSG_GET_REDIRECT_MANY, MSG_GET_BULK, MSG_HEAD, MSG_PUT, MSG_PUT_BULK, MSG_RANGE,
    PUT_BULK_HEADER_LEN,
};
use autumn_rpc::StatusCode;
use ed25519_dalek::VerifyingKey;
use parking_lot::RwLock;

/// Immutable snapshot of the authz config the PS enforces against.
pub struct AuthzInner {
    /// D7 (coco P2): Layer-B (protected-prefix / token) enforcement on
    /// (a signing key was configured). Carried IN the snapshot so the gate's
    /// per-request decision comes from ONE consistent object — never "flag from
    /// one atomic + config from a different-time snapshot". The `enabled`
    /// AtomicBool remains only as the lock-free fast-path skip hint.
    pub enabled: bool,
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
    /// D7: ALL registered namespace prefixes (the D2 registry, from
    /// `GetAuthzConfigResp.namespaces`). Layer-A's data source: `check_layer_a`
    /// admits a put-class write only if its key `starts_with` one of these.
    pub namespaces: Vec<Vec<u8>>,
    /// (PS slice): the manager's admin secret, for gating
    /// `is_admin_ps_msg` (split / maintenance). EMPTY = unconfigured → those ops
    /// run bare (opt-in). Carried in the snapshot so the gate reads one
    /// consistent object.
    pub admin_token: Vec<u8>,
}

impl AuthzInner {
    fn empty() -> Self {
        Self {
            enabled: false,
            keys: HashMap::new(),
            protected_prefixes: Vec::new(),
            clock_skew_secs: 60,
            cluster_id: String::new(),
            namespaces: Vec::new(),
            admin_token: Vec::new(),
        }
    }
}

/// Cross-thread shared authz runtime on `PartitionServer` (`Arc`, since
/// connection tasks run on the partition OS threads, not the main thread). The
/// main-thread poll loop swaps `inner`; connection tasks read it.
pub struct AuthzState {
    /// Fast gate: is Layer-B (protected-prefix / token) enforcement on? A single
    /// relaxed load on the hot path when OFF (the common case). True iff the
    /// manager configured a signing key.
    enabled: AtomicBool,
    /// D7: is Layer-A (put-class must fall in a REGISTERED namespace)
    /// enforcement on? True iff the registry is non-empty. Independent of
    /// `enabled` — a dev cluster with namespaces but no signing key still gets
    /// Layer-A (token-free), and an unconfigured/cold PS (empty registry) doesn't
    /// brick writes.
    layer_a_enabled: AtomicBool,
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
            layer_a_enabled: AtomicBool::new(false),
            inner: RwLock::new(Arc::new(AuthzInner::empty())),
        }
    }

    /// Hot-path gate. `false` ⇒ no Layer-B enforcement (single relaxed load).
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// D7 hot-path gate: `false` ⇒ no Layer-A enforcement (empty
    /// registry). A single relaxed load.
    #[inline]
    pub fn layer_a_enabled(&self) -> bool {
        self.layer_a_enabled.load(Ordering::Relaxed)
    }

    /// True iff either Layer runs — the gate's fast skip test.
    #[inline]
    pub fn gate_active(&self) -> bool {
        self.is_enabled() || self.layer_a_enabled()
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
            enabled: resp.enabled,
            keys,
            protected_prefixes: resp.protected_prefixes.clone(),
            clock_skew_secs: resp.clock_skew_secs,
            cluster_id: resp.cluster_id.clone(),
            // D7: the registered-namespace list — Layer-A's data source.
            namespaces: resp.namespaces.clone(),
            admin_token: resp.admin_token.clone(),
        });
        // D7: Layer-A is on iff the registry is non-empty (independent
        // of the signing key). Compute BEFORE the swap so the flag matches the
        // installed snapshot.
        let layer_a = !resp.namespaces.is_empty();
        *self.inner.write() = inner;
        // Publish `enabled` / `layer_a_enabled` AFTER the config is in place so a
        // concurrent reader never sees enabled==true with an empty keyring / a
        // stale namespace list.
        self.enabled.store(resp.enabled, Ordering::Relaxed);
        self.layer_a_enabled.store(layer_a, Ordering::Relaxed);
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

fn denied(msg: &str) -> Option<(StatusCode, String)> {
    Some((StatusCode::PermissionDenied, msg.to_string()))
}

/// Authorize ONE key. `Some((code,msg))` = deny, `None` = allow.
///
/// PROTECT-EVERYTHING (tenant-first, 2026-07-19): when authz is enabled EVERY
/// key requires a token (this fn only runs under `snap.enabled`), so there is no
/// `is_protected` prefix filter — a credential grants `{tenant}/` (whole tenant)
/// or `{tenant}/{ns}/` and the key must fall under one of those grants. The old
/// `protected_prefixes` list is retired (there is no un-protected range once
/// every write is mandatorily tenant-scoped).
fn check_key(
    key: &[u8],
    principal: Option<&BoundPrincipal>,
    inner: &AuthzInner,
    now: u64,
) -> Option<(StatusCode, String)> {
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
    // PROTECT-EVERYTHING: every range needs a token when authz is on (this fn
    // only runs under `snap.enabled`). An empty (unbounded) prefix can never be
    // ⊆ a non-empty grant → denied; a `{tenant}/`-scoped scan is ⊆ the tenant
    // grant → allowed.
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
/// requests, `parse_put_bulk_meta` for `MSG_PUT_BULK`): a payload that fails to
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
/// (PS slice): constant-time byte equality for the admin token.
/// The token is a fixed-width hex secret, so a length mismatch is not sensitive;
/// equal lengths are compared with an XOR-accumulate that never short-circuits,
/// so a matching prefix can't be timed out. (The PS has no sha2/subtle dep, so
/// this is a small local impl rather than the manager's hash-then-compare.)
pub fn ct_eq_bytes(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

pub fn authz_check(
    msg_type: u8,
    payload: &[u8],
    principal: Option<&BoundPrincipal>,
    inner: &AuthzInner,
    now: u64,
) -> Option<(StatusCode, String)> {
    match msg_type {
        MSG_GET | MSG_GET_BULK | MSG_GET_REDIRECT => {
            let r = partition_rpc::rkyv_decode::<GetReq>(payload).ok()?;
            check_key(&r.key, principal, inner, now)
        }
        // carries a USER KEY per item — gate each like MSG_BATCH_GET.
        MSG_GET_REDIRECT_MANY => {
            let r = partition_rpc::rkyv_decode::<GetRedirectManyReq>(payload).ok()?;
            for item in &r.items {
                if let Some(d) = check_key(&item.key, principal, inner, now) {
                    return Some(d);
                }
            }
            None
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
            // < 64 KiB (large values go MSG_PUT_BULK, key extracted below without
            // copying the value), so the copy is bounded — and only when authz
            // is enabled.
            let r = partition_rpc::rkyv_decode::<PutReq>(payload).ok()?;
            check_key(&r.key, principal, inner, now)
        }
        MSG_PUT_BULK => {
            // Zero value copy: the key is a slice of the binary meta header.
            let meta = parse_put_bulk_meta(payload)?;
            let key = payload.get(PUT_BULK_HEADER_LEN..meta.value_offset)?;
            check_key(key, principal, inner, now)
        }
        MSG_RANGE => {
            let r = partition_rpc::rkyv_decode::<RangeReq>(payload).ok()?;
            check_range(&r.prefix, principal, inner, now)
        }
        // Both batched read forms carry the same `BatchGetReq`; only the
        // response shape differs, and authz never looks at responses.
        MSG_BATCH_GET | MSG_BATCH_GET_BULK => {
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
        // The zero-copy batch carries its keys in ctrl exactly like the inline
        // form; only the values moved to the frame tail, and authz never looked
        // at values. `payload` here is the ctrl block.
        MSG_BATCH_PUT_BULK => {
            let r = partition_rpc::rkyv_decode::<BatchPutBulkReq>(payload).ok()?;
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

/// D7 **Layer-A**: a put-class write's key(s) MUST fall under some
/// REGISTERED namespace prefix (`AuthzInner.namespaces`); otherwise the target
/// namespace does not exist and the write is refused with
/// `StatusCode::NamespaceUnknown`. Distinct from Layer-B (`authz_check`):
/// - **Token-FREE** — pure prefix match; anonymous connections are checked too.
/// - **PUT / PUT_ZC / BATCH_PUT only** — Layer-A prevents creating UNOWNED data
///   via WRITES. Deletes / reads / ranges are NOT Layer-A gated (a delete can't
///   create unowned data; read isolation is Layer-B's job).
/// - Runs whenever the registry is non-empty (the caller gates on
///   `layer_a_enabled`), INDEPENDENT of the signing key.
/// Returns `Some((code, msg))` to REJECT, `None` to admit. A frame that fails to
/// decode returns `None` (the real handler rejects it) — same safety argument as
/// `authz_check`.
pub fn check_layer_a(
    msg_type: u8,
    payload: &[u8],
    inner: &AuthzInner,
) -> Option<(StatusCode, String)> {
    // (Option 3): the wire key is `{ns}/…` (NO tenant
    // segment), so a registered namespace (`{name}/`, e.g. `fs/`) is the FIRST
    // `/`-delimited segment. Extract `{ns}/` (the bytes up to and including the
    // first `/`) and require an EXACT match against a registered namespace. `{ns}`
    // is ASCII-no-slash (`is_valid_scope_segment`), so the first slash bounds it
    // even if the binary key tail contains `/`.
    fn in_a_namespace(key: &[u8], namespaces: &[Vec<u8>]) -> bool {
        let Some(s1) = key.iter().position(|&b| b == b'/') else {
            return false;
        };
        let ns_with_slash = &key[..=s1]; // `{ns}/`
        namespaces.iter().any(|ns| ns.as_slice() == ns_with_slash)
    }
    fn reject(key: &[u8]) -> Option<(StatusCode, String)> {
        Some((
            StatusCode::NamespaceUnknown,
            format!(
                "write to unregistered namespace: key {:?} is under no registered namespace prefix",
                String::from_utf8_lossy(&key[..key.len().min(64)])
            ),
        ))
    }
    match msg_type {
        MSG_PUT => {
            let r = partition_rpc::rkyv_decode::<PutReq>(payload).ok()?;
            if in_a_namespace(&r.key, &inner.namespaces) {
                None
            } else {
                reject(&r.key)
            }
        }
        MSG_PUT_BULK => {
            let meta = parse_put_bulk_meta(payload)?;
            let key = payload.get(PUT_BULK_HEADER_LEN..meta.value_offset)?;
            if in_a_namespace(key, &inner.namespaces) {
                None
            } else {
                reject(key)
            }
        }
        MSG_BATCH_PUT => {
            let r = partition_rpc::rkyv_decode::<BatchPutReq>(payload).ok()?;
            for op in &r.ops {
                if !in_a_namespace(&op.key, &inner.namespaces) {
                    return reject(&op.key);
                }
            }
            None
        }
        MSG_BATCH_PUT_BULK => {
            let r = partition_rpc::rkyv_decode::<BatchPutBulkReq>(payload).ok()?;
            for op in &r.ops {
                if !in_a_namespace(&op.key, &inner.namespaces) {
                    return reject(&op.key);
                }
            }
            None
        }
        // Not a put-class op → Layer-A does not apply (delete/get/range/admin).
        _ => None,
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
            enabled: true,
            keys,
            protected_prefixes: prefixes,
            clock_skew_secs: 60,
            cluster_id: String::new(),
            namespaces: Vec::new(),
            admin_token: Vec::new(),
        }
    }

    // TENANT-FIRST: a tenant credential grants the WHOLE tenant `acme/` (covers
    // every namespace `acme/fs/`, `acme/kvc/`, `acme/mem/`).
    fn acme() -> BoundPrincipal {
        BoundPrincipal {
            allowed_prefixes: vec![b"acme/".to_vec()],
            exp: 1_000_000,
            kid: 1,
        }
    }

    #[test]
    fn check_key_matrix() {
        // protected_prefixes is retired (protect-everything) — pass empty.
        let inner = inner_with(vec![]);
        let p = acme();
        let now = 999_000;
        // key inside the granted tenant → allow (ANY namespace within it)
        assert!(check_key(b"acme/mem/fact/1", Some(&p), &inner, now).is_none());
        assert!(check_key(b"acme/fs/\x01x", Some(&p), &inner, now).is_none());
        // another tenant → deny (outside the acme/ grant)
        assert!(check_key(b"other/mem/fact/1", Some(&p), &inner, now).is_some());
        // anonymous → deny (protect-everything: every key needs a token)
        assert!(check_key(b"acme/mem/fact/1", None, &inner, now).is_some());
        assert!(check_key(b"acme/fs/\x01x", None, &inner, now).is_some());
        // expired token → deny
        assert!(check_key(b"acme/mem/fact/1", Some(&p), &inner, p.exp + 61).is_some());
        // within skew leeway → allow
        assert!(check_key(b"acme/mem/fact/1", Some(&p), &inner, p.exp + 30).is_none());
    }

    #[test]
    fn prefix_boundary_not_forgeable() {
        // `acmeevil/` must NOT be authorized by the `acme/` grant (the trailing
        // `/` on the grant makes `acmeevil/…` fall outside it).
        let inner = inner_with(vec![]);
        let p = acme();
        let now = 999_000;
        assert!(check_key(b"acmeevil/mem/x", Some(&p), &inner, now).is_some());
        assert!(check_key(b"acme/mem/x", Some(&p), &inner, now).is_none());
    }

    #[test]
    fn range_whole_interval_subseteq_prefix() {
        let inner = inner_with(vec![]);
        let p = acme();
        let now = 999_000;
        // prefix ⊆ the tenant grant → allow (incl. scanning the whole tenant)
        assert!(check_range(b"acme/mem/", Some(&p), &inner, now).is_none());
        assert!(check_range(b"acme/mem/fact/", Some(&p), &inner, now).is_none());
        assert!(check_range(b"acme/", Some(&p), &inner, now).is_none());
        // another tenant (prefix ⊄ acme/) → deny
        assert!(check_range(b"other/mem/", Some(&p), &inner, now).is_some());
        // empty prefix (unbounded scan) → deny
        assert!(check_range(b"", Some(&p), &inner, now).is_some());
        // anonymous → deny (protect-everything)
        assert!(check_range(b"acme/mem/", None, &inner, now).is_some());
    }

    #[test]
    fn authz_check_dispatch_get_and_put() {
        let inner = inner_with(vec![]);
        let p = acme();
        let now = 999_000;
        // GET authorized (within the acme/ tenant grant)
        let g = rkyv_encode(&GetReq {
            part_id: 1,
            key: b"acme/mem/doc/1".to_vec(),
            offset: 0,
            length: 0,
            region_epoch: 0,
        });
        assert!(authz_check(MSG_GET, &g, Some(&p), &inner, now).is_none());
        // GET cross-tenant denied
        let g2 = rkyv_encode(&GetReq {
            part_id: 1,
            key: b"other/mem/doc/1".to_vec(),
            offset: 0,
            length: 0,
            region_epoch: 0,
        });
        assert!(authz_check(MSG_GET, &g2, Some(&p), &inner, now).is_some());
        // PUT cross-tenant denied (value not copied into the assertion path)
        let put = rkyv_encode(&PutReq {
            part_id: 1,
            key: b"other/mem/doc/1".to_vec(),
            value: vec![7u8; 100],
            expires_at: 0,
            region_epoch: 0,
            inode_hint: 0,
            lease_epoch: 0,
        });
        assert!(authz_check(MSG_PUT, &put, Some(&p), &inner, now).is_some());
        // non-data-plane msg_type → not gated
        assert!(authz_check(0x47 /* MSG_MAINTENANCE */, &[], Some(&p), &inner, now).is_none());
        // PROTECT-EVERYTHING: authz_check assumes enabled (the caller gates on
        // `snap.enabled`), so ANY key without a token is denied — there is no
        // "empty protected ⇒ allowed" escape hatch anymore.
        assert!(authz_check(MSG_GET, &g2, None, &inner, now).is_some());
    }

    #[test]
    fn verify_auth_hello_binds_principal() {
        let sk = SigningKey::from_bytes(&[3u8; 32]);
        let vk = sk.verifying_key();
        let mut keys = HashMap::new();
        keys.insert(1u32, vk);
        let inner = AuthzInner {
            enabled: true,
            keys,
            protected_prefixes: vec![b"mem/".to_vec()],
            clock_skew_secs: 60,
            cluster_id: "cluster-x".to_string(),
            namespaces: Vec::new(),
            admin_token: Vec::new(),
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
            enabled: true,
            keys: HashMap::new(),
            protected_prefixes: vec![],
            clock_skew_secs: 60,
            cluster_id: "cluster-x".to_string(),
            namespaces: Vec::new(),
            admin_token: Vec::new(),
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
            enabled: true,
            keys: HashMap::new(), // kid 1 no longer present
            protected_prefixes: vec![b"mem/".to_vec()],
            clock_skew_secs: 60,
            cluster_id: String::new(),
            namespaces: Vec::new(),
            admin_token: Vec::new(),
        };
        let p = acme(); // kid 1
        assert!(check_key(b"mem/acme/fact/1", Some(&p), &inner_disabled, 999_000).is_some());
        assert!(check_range(b"mem/acme/", Some(&p), &inner_disabled, 999_000).is_some());
    }

    #[test]
    fn ct_eq_bytes_matches_only_exact() {
        assert!(ct_eq_bytes(b"deadbeef", b"deadbeef"));
        assert!(!ct_eq_bytes(b"deadbeef", b"deadbee0"));
        assert!(!ct_eq_bytes(b"deadbeef", b"deadbee")); // length differs
        assert!(ct_eq_bytes(b"", b""));
    }

    #[test]
    fn install_caches_admin_token_from_config() {
        // (PS slice): the PS learns the admin secret from the
        // manager's GetAuthzConfigResp and stores it in the snapshot.
        let st = AuthzState::new();
        // Absent by default (opt-in: the gate runs bare).
        assert!(st.snapshot().admin_token.is_empty());
        st.install(&GetAuthzConfigResp {
            admin_token: b"the-secret".to_vec(),
            ..Default::default()
        });
        assert_eq!(st.snapshot().admin_token, b"the-secret");
        // An empty token in a later poll clears it (manager dropped its token).
        st.install(&GetAuthzConfigResp::default());
        assert!(st.snapshot().admin_token.is_empty());
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
            namespaces: Vec::new(),
            admin_token: Vec::new(),
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

    // ── Layer-A ──────────────────────────────────────────────────────────
    fn inner_with_namespaces(namespaces: Vec<Vec<u8>>) -> AuthzInner {
        let mut inner = inner_with(Vec::new());
        inner.namespaces = namespaces;
        inner
    }

    fn put_payload(key: &[u8]) -> Vec<u8> {
        rkyv_encode(&PutReq {
            part_id: 1,
            key: key.to_vec(),
            value: b"v".to_vec(),
            expires_at: 0,
            region_epoch: 0,
            inode_hint: 0,
            lease_epoch: 0,
        })
        .to_vec()
    }

    #[test]
    fn layer_a_admits_put_in_registered_namespace() {
        // namespace is the FIRST segment of `{ns}/…`.
        let inner = inner_with_namespaces(vec![b"kvc/".to_vec(), b"mem/".to_vec()]);
        assert!(check_layer_a(MSG_PUT, &put_payload(b"kvc/x"), &inner).is_none());
        assert!(check_layer_a(MSG_PUT, &put_payload(b"mem/acme/y"), &inner).is_none());
    }

    #[test]
    fn layer_a_rejects_put_in_unregistered_namespace() {
        let inner = inner_with_namespaces(vec![b"kvc/".to_vec()]);
        // UNregistered 1st-segment namespace → reject.
        let d = check_layer_a(MSG_PUT, &put_payload(b"scratch/x"), &inner);
        assert!(matches!(d, Some((StatusCode::NamespaceUnknown, _))), "{d:?}");
        // A raw key with no `{ns}/` structure is rejected too.
        let d = check_layer_a(MSG_PUT, &put_payload(b"\x01\x00\x00\x00"), &inner);
        assert!(matches!(d, Some((StatusCode::NamespaceUnknown, _))));
    }

    #[test]
    fn batched_bulk_read_keys_are_gated_like_the_inline_form() {
        // `authz_check`'s match also ends in a catch-all that ADMITS, so a
        // batched READ with no arm would hand back values for keys the caller
        // has no grant on. Both batch-read msg types carry the same request, so
        // the arm covers them together — this pins that it actually does.
        let inner = inner_with(vec![]);
        let p = acme();
        let mk = |k2: &[u8]| {
            partition_rpc::rkyv_encode(&BatchGetReq {
                part_id: 1,
                region_epoch: 0,
                keys: vec![b"acme/mem/1".to_vec(), k2.to_vec()],
            })
            .to_vec()
        };
        for msg in [MSG_BATCH_GET, MSG_BATCH_GET_BULK] {
            assert!(
                authz_check(msg, &mk(b"acme/mem/2"), Some(&p), &inner, 0).is_none(),
                "in-grant keys must pass ({msg:#x})"
            );
            assert!(
                authz_check(msg, &mk(b"other/mem/2"), Some(&p), &inner, 0).is_some(),
                "an out-of-grant key must be refused ({msg:#x})"
            );
        }
    }

    #[test]
    fn layer_a_batch_put_bulk_is_gated_like_the_inline_form() {
        // The msg_type match in `check_layer_a` ends in `_ => None`, which
        // means "not a put-class op, nothing to enforce" — so a keyed write
        // that nobody adds an arm for is admitted with its namespace never
        // checked. This test is what makes adding one non-optional.
        let inner = inner_with_namespaces(vec![b"kvc/".to_vec()]);
        let mk = |k2: &[u8]| {
            rkyv_encode(&partition_rpc::BatchPutBulkReq {
                part_id: 1,
                region_epoch: 0,
                ops: vec![
                    partition_rpc::BatchPutBulkOp { inode_hint: 0, lease_epoch: 0, key: b"kvc/1".to_vec(), value_len: 4, expires_at: 0 },
                    partition_rpc::BatchPutBulkOp { inode_hint: 0, lease_epoch: 0, key: k2.to_vec(), value_len: 4, expires_at: 0 },
                ],
            })
            .to_vec()
        };
        assert!(check_layer_a(MSG_BATCH_PUT_BULK, &mk(b"kvc/2"), &inner).is_none());
        assert!(matches!(
            check_layer_a(MSG_BATCH_PUT_BULK, &mk(b"scratch/2"), &inner),
            Some((StatusCode::NamespaceUnknown, _))
        ));
    }

    #[test]
    fn layer_a_batch_put_rejects_if_any_op_out_of_namespace() {
        let inner = inner_with_namespaces(vec![b"kvc/".to_vec()]);
        let ok = rkyv_encode(&BatchPutReq {
            part_id: 1,
            region_epoch: 0,
            ops: vec![
                partition_rpc::BatchPutOp { inode_hint: 0, lease_epoch: 0, key: b"kvc/1".to_vec(), value: vec![], expires_at: 0 },
                partition_rpc::BatchPutOp { inode_hint: 0, lease_epoch: 0, key: b"kvc/2".to_vec(), value: vec![], expires_at: 0 },
            ],
        })
        .to_vec();
        assert!(check_layer_a(MSG_BATCH_PUT, &ok, &inner).is_none());
        let bad = rkyv_encode(&BatchPutReq {
            part_id: 1,
            region_epoch: 0,
            ops: vec![
                partition_rpc::BatchPutOp { inode_hint: 0, lease_epoch: 0, key: b"kvc/1".to_vec(), value: vec![], expires_at: 0 },
                partition_rpc::BatchPutOp { inode_hint: 0, lease_epoch: 0, key: b"scratch/2".to_vec(), value: vec![], expires_at: 0 },
            ],
        })
        .to_vec();
        assert!(matches!(
            check_layer_a(MSG_BATCH_PUT, &bad, &inner),
            Some((StatusCode::NamespaceUnknown, _))
        ));
    }

    #[test]
    fn layer_a_does_not_gate_delete_get_or_range() {
        // Layer-A is put-class only. A delete/get/range of an unregistered key
        // is NOT a Layer-A concern (reads/deletes are Layer-B's job).
        let inner = inner_with_namespaces(vec![b"kvc/".to_vec()]);
        let del = rkyv_encode(&DeleteReq {
            part_id: 1,
            key: b"scratch/x".to_vec(),
            region_epoch: 0,
            inode_hint: 0,
            lease_epoch: 0,
        })
        .to_vec();
        assert!(check_layer_a(MSG_DELETE, &del, &inner).is_none());
        let get = rkyv_encode(&GetReq {
            part_id: 1,
            key: b"scratch/x".to_vec(),
            offset: 0,
            length: 0,
            region_epoch: 0,
        })
        .to_vec();
        assert!(check_layer_a(MSG_GET, &get, &inner).is_none());
    }

    #[test]
    fn layer_a_enabled_tracks_namespace_list() {
        let st = AuthzState::new();
        assert!(!st.layer_a_enabled());
        // Install a config WITH namespaces but no signing key → Layer-A on,
        // Layer-B off (dev cluster gets Layer-A token-free).
        st.install(&GetAuthzConfigResp {
            code: 0,
            message: String::new(),
            enabled: false,
            public_keys: vec![],
            protected_prefixes: vec![],
            namespaces: vec![b"kvc/".to_vec()],
            admin_token: Vec::new(),
            token_ttl_secs: 0,
            clock_skew_secs: 0,
            cluster_id: String::new(),
        });
        assert!(st.layer_a_enabled(), "non-empty registry → Layer-A on");
        assert!(!st.is_enabled(), "no signing key → Layer-B off");
        assert!(st.gate_active());
        // Empty registry → Layer-A off (cold PS doesn't brick writes).
        st.install(&GetAuthzConfigResp {
            code: 0,
            message: String::new(),
            enabled: false,
            public_keys: vec![],
            protected_prefixes: vec![],
            namespaces: vec![],
            admin_token: Vec::new(),
            token_ttl_secs: 0,
            clock_skew_secs: 0,
            cluster_id: String::new(),
        });
        assert!(!st.layer_a_enabled());
        assert!(!st.gate_active());
    }
}
