# Data-plane key-range authorization (server-side authz)

## 1. Architecture in one sentence

**The manager (leader) acts as a KDC and issues short-lived capability tokens; the PS verifies the signature locally and enforces at the KV layer.**
Asymmetric Ed25519: the manager holds the private key and signs, the PS holds only the public key and verifies (the PS cannot forge).

```
 issue/renew (KDC)       verify + enforce (data plane)
┌──────────────┐        ┌──────────────────────────┐
│ manager      │ pubkey │ partition-server (KV)    │
│ leader:      │ ─────▶ │  CLIENT_AUTH verify(pub) │
│  · signs with│        │  per request: key prefix │
│    priv key  │        │    + exp                 │
│  · principal │        │  ★ never calls manager   │
│    store     │        │    to enforce            │
└──────┬───────┘        └──────────────────────────┘
       │ MINT_TOKEN(short TTL)     ▲ CLIENT_AUTH(token)
       ▼                           │
   ┌────────────────────────────────┐
   │ client (SDK / fuse / kvcache)   │
   │  holds long-lived principal     │
   │  credential → auto-renews       │
   └────────────────────────────────┘
```

**Key distinction**: **issuing** (can live in the manager) and **enforcing** (must live in the KV layer) are two separate roles.
"Auth in the KV layer" = enforcement is local to the PS, per request, and **never calls back to the manager**. The manager as KDC
only issues and never touches enforcement — the two do not conflict.

The unit of identity is the **principal** (a component holding a credential); the unit of authorization is its **grant** (a left-anchored byte
prefix, usually one namespace, such as `fs/`). For the key layout and the namespace registry see
`key_namespace_split_design.md`.

## 2. Threat model

**Trusted internal network** (for example, the internal RoCE network of an inference cluster, plaintext cluster-wide). Defends against: one component's code / a rogue or test
client misreading / cross-writing another namespace's data. **Does not defend against**: network MITM / packet capture (no TLS),
a compromised manager (a compromised manager = everything compromised; it controls partitions and routing, so it is out of scope anyway).

A namespace segment is a single path segment matching `[a-z0-9._-]+` (validated at connect time by `is_valid_scope_segment`),
so `{ns}/` is an **unforgeable** boundary; if an application layers further inside a namespace,
it must still encode dynamic path segments unambiguously.

## 3. Why asymmetric (not symmetric)

- Symmetric (Ceph cephx / shared secret + HMAC): the verifier can also issue → **one compromised PS
  can forge a token for any principal**. Rejected.
- Asymmetric (FDB / Ed25519): **the signing private key lives only in the manager (the trusted central authority)**; the PS holds only the
  public key and **can only verify, never forge**. This combines Ceph's KDC convenience with FDB's "the data plane cannot forge".

## 4. Token — an Ed25519-signed capability

`crates/rpc/src/cap_token.rs`:

```
claims = CapClaims {
  ver: 1,                        // CAP_VER, format version (compatible field additions)
  typ: "autumn.cap.v1",          // CAP_TYP, type (prevents mixing with other tokens)
  kid: u32,                      // which public key verifies → multiple keys / rotation
  iss: "autumn-mgr",             // issuer
  aud: <cluster_id>,             // only for this cluster (stops a dev token hitting prod)
  iat, nbf, exp,                 // issued / not-before / expiry (short TTL)
  allowed_prefixes: [b"fs/"],    // authorized scope, each must end with `/`
}
sig = Ed25519_Sign(mgr_priv, CAP_DOMAIN ‖ canonical_bytes(claims))
token(wire) = canonical_bytes(claims) ‖ sig[64]     // SIG_LEN = 64
```

- **canonical serialization**: the signing input is a deterministic byte sequence (rkyv deterministic layout). Verification uses
  **the claims bytes carried in the token itself**, rather than re-encoding and comparing; untrusted input goes through
  checked-rkyv decoding.
- **domain separation**: the prefix `CAP_DOMAIN = b"autumn-rs data-plane cap v1"` goes into the
  signing input, so a valid signature from another context cannot be reused here.
- Each field closes a hole: no `exp` → a leak is a permanent compromise; no `aud` → cross-environment replay;
  no `kid` → no rotation; no `allowed_prefixes` → no authorization scope.
- Rejection reasons are classified as `AuthReject` (malformed / wrong type / unknown or disabled kid /
  not-yet-valid / expired / bad-signature), and the PS reports a metric per class.

## 5. Flow

### 5.1 Create a principal (admin, infrequent)

`autumn-op principal-create --principal <name> --grant <prefix> [--grant …]` →
the manager stores `{name, credential_hash = SHA-256(cred), allowed_prefixes}` in etcd at
`principal/<name>` (leader-fenced, replay fails loudly) and returns that principal's
**long-lived credential** (≈ a refresh token, handed to that component).

Grant string semantics: **non-empty, left-anchored byte prefix, a trailing `/` is forced**.

The credential file format is **two lines**: `<principal-name>\n<hex-secret>`. The name travels with the file,
so the data plane needs no `--principal` flag.

This RPC is accepted only on an Admin connection, and an Admin connection must hold the cluster secret
(`cluster_secret_design.md` §4).

### 5.2 Issue / renew a token (client, frequent)

client → **leader manager** `MSG_MINT_TOKEN{principal, credential}` →
the manager checks `SHA-256(cred) == credential_hash` in constant time → signs with the **private key** a capability token with
`exp = now + TTL` (`allowed_prefixes` taken from the principal store) → returns
`{token, exp}`. unknown-principal and wrong-credential return **the same opaque error**.

The client library mints lazily and renews automatically `--token-renew-margin-secs = 300` seconds before `exp`;
renewal evicts PS connections authenticated with the old token. Long-lived cred = infrequent and long-lasting; token = frequent and short-lived.

### 5.3 Connect + enforce (PS)

client connects to PS → first frame `MSG_CLIENT_AUTH{token}` (`0x55`) → the PS looks up the public key by `kid`,
**verifies the signature locally**, checks `aud == cluster_id` → binds the connection to
`BoundPrincipal{allowed_prefixes, exp, kid}` → **every KV request** gets a prefix + validity
check; a failure returns `StatusCode::PermissionDenied`(=7). **The PS never calls the manager in this path.**

### 5.4 PS fetches public keys and config (poll, cached)

The PS polls the manager with `MSG_GET_AUTHZ_CONFIG` → `GetAuthzConfigResp{enabled,
public_keys:[{kid, ed25519_pub, disabled}], namespaces, token_ttl_secs,
clock_skew_secs, cluster_id, …}` → local cache
(`AuthzState::install`, a whole-page swap of `RwLock<Arc<AuthzInner>>`).
The first sync happens in `finish_connect` (enforcement is armed before the first connection), and after that once every
`--config-poll-secs` (default 30 s) (`authz_config_poll_loop`). On a namespace (Layer-A) or kid (CLIENT_AUTH) not in the config,
it immediately fetches once more (at most once per second); the Layer-A
rejection first returns `Unavailable` (the SDK refreshes and retries, by which time the newly created namespace is in the config). Manager down
→ enforcement continues from the cache (only rotation / new public keys need the manager online); the extra fetch then fails, writes to an unknown namespace
keep returning `Unavailable`, and once the SDK's retry budget is exhausted it reports a connection error. The "return a terminal result" decision is tracked per namespace (per kid on EN):
the time of the first rejection is recorded, and `NamespaceUnknown` is returned only if the name is still absent from a config installed after that time (the snapshot
the request check used). A fetch that merely "just happened" does not count, otherwise a namespace created right after a periodic fetch would be misjudged.
The ledger holds at most 1024 names with a 60 s expiry; when full, it returns the terminal result directly without an extra fetch (degrading to per-period effect). Changes that trigger no extra fetch and
can take up to one period (30 s) to take effect: deleting a namespace (writes to the old ns are still allowed), disabling a kid
(already-bound connections remain usable), the manager enabling or disabling authz (PS / EN keep their previous state until the next fetch).
While the PS has not installed any config yet (first fetch failed), it retries every 5 s.

## 6. Enforcement details

- **There is a single choke point**: `authz_gate` sits **at the top of per-frame dispatch (before routing)**,
  on both paths, `push_one_frame_to_inflight` and `d1_fast_path_round_trip`.
  `AuthzState::is_enabled()` is a single `AtomicBool`; when off it costs one relaxed load, i.e. zero.
- **protect-everything**: once authz is on, **every key and every range requires a token**.
  There is no notion of an "unprotected range" — an anonymous connection (one that never sent CLIENT_AUTH) is rejected for any key.
  `GetAuthzConfigResp` carries only public keys and registered namespaces (the Layer-A data source), no prefix list.
- **The `authz_check` INVARIANT (load-bearing)**: every data-plane
  msg_type carrying a user key **must** have an arm in `authz_check` that extracts the key and calls
  `check_key` / `check_range`. The catch-all `_ => None` (allow) is correct only for ops that are not key-scoped
  (maintenance / split / merge / discards / diag) and for `CLIENT_AUTH`.
  Adding a read/write RPC that carries a key and forgetting its arm = an authz bypass.
  Arms currently present: `MSG_GET` / `MSG_GET_AM` / `MSG_HEAD` / `MSG_DELETE` /
  `MSG_PUT` / `MSG_RANGE` / `MSG_BATCH_PUT` / `MSG_BATCH_GET` /
  `MSG_BATCH_DELETE` / `MSG_COMPARE_WRITE`.
- **Frames that fail to decode are allowed through**: the gate and the actual handler use **the same** decode
  (both `rkyv_decode`),
  so anything that fails to decode here also fails in the handler → it is rejected with `InvalidArgument` and never returns bytes.
- **Writes do not copy the value**: the ctrl of `MSG_PUT` / `MSG_BATCH_PUT` / `MSG_COMPARE_WRITE`
  contains only the key and other metadata; values are in the frame tail, and the gate decodes only the ctrl.
- **A Range must be wholly ⊆ some allowed_prefix** (not just the first key checked, otherwise
  `prefix=mem/, start=mem/acme/` would scan into `mem/other/`). Every returnable key
  satisfies `starts_with(prefix)`, so the criterion is equivalent to `prefix.starts_with(some AP)`.
  **An empty prefix (unbounded full scan) is always rejected** — an empty range can never be ⊆ a non-empty grant.
- **Batches are checked per op**; if any op is denied the whole batch is rejected; an explicit `PermissionDenied` is returned,
  **not folded into NotFound** (which would amplify blind retries). On the client side `PermissionDenied` is a
  **terminal** error and is not retried.
- **kid revocation applies to live connections**: `check_key`/`check_range` check
  `inner.keys.contains_key(&p.kid)` every time, so a disabled kid invalidates
  already-bound connections as soon as the next poll installs it.
- **allowed_prefixes normalization**: must end with `/` (stops `mem/ac` from matching `mem/acme/`).
- **Fail fast at connect time**: `ClusterClient::validate_credential_scope` mints once at connect
  and checks that "this client's scope falls under some grant", so a misconfigured credential fails at
  connect instead of surfacing as `PermissionDenied` on the first write. It is skipped only when "the manager has not enabled
  authz".
- **opt-in**: the manager has no `--auth-signing-key-file` → `enabled=false` → the PS does not
  enforce (zero impact on fuse / kvcache / dev / chaos).

## 7. Key rotation + revocation

- **Rotation**: the manager holds several signing private keys + the matching public keys (each keyfile line is
  `<kid> <hex-32-byte-seed> [disabled]`, generated by `autumn-op gen-signing-key`);
  tokens carry `kid`. Add a new kid → sign new tokens with the new kid → wait for old tokens to expire → mark the old kid
  `disabled` (still published; the PS rejects that kid after its next poll). active = the highest enabled kid.
- **Revocation** (manager-as-KDC is **stronger** than purely offline):
  1. **Stop renewal**: delete / disable the principal in the principal store → it cannot renew → **it loses access
     when the current token expires** (window = TTL).
  2. **Emergency revoke-all**: disable a `kid` → **all** tokens signed by that kid become invalid immediately, including
     already-authenticated live connections.
  - **No** per-token deny-list (pushing a blacklist = dragging enforcement back into the control plane).
- **TTL**: `--auth-token-ttl-secs`, default 3600. Hours in prod (renewed automatically, transparent);
  can be relaxed for dev. **Not 30d** (bearer leak window). `--auth-clock-skew-secs` defaults to 60,
  used as leeway for `nbf`/`exp`.

**Operational note**: with enforcement on, the data plane gains an availability dependency on the control plane with a **grace period = TTL**
— enforcement itself never calls the manager, but **renewal is leader-only**. If the manager is unreachable for longer than
`TTL − 300 s` → renewal fails → the token expires → the PS rejects. TTL is the knob trading "revocation window vs manager failure
tolerance".

## 8. Connection-layer rules (preventing principal mixing)

- **One connection = one principal**: the PS binds it after the first CLIENT_AUTH frame; **silent rebind while there is inflight
  work is not allowed** (to re-auth, drain inflight + auth epoch, or just close and reconnect).
- **The client connection pool is partitioned by principal**: switching tokens forcibly drops that principal's PS connections
  (the `ensure_token` renewal path does this). **Serving multiple principals from one `ClusterClient` is
  not recommended.**
- When the token reaches `exp`: the PS rejects and requires re-authentication; the client renews 300 s before exp and reconnects.

## 9. Non-goals

TLS / mTLS; per-user RBAC / roles / ACL tables; resistance to a compromised manager; resistance to MITM / packet-capture
replay; a per-token revocation blacklist; EN direct reads authorized by key range (see §10).

## 10. EN direct reads: identity only

`get_direct` / `get_many_direct` let the client send `MSG_READ` directly to an EN using the location the PS returned in its
`MSG_GET` / `MSG_BATCH_GET` reply (extent, offset, replica addresses). The PS runs `check_key` before handing out
the location, but the EN receives only coordinates and does not know which key they belong to.

- **When authz is on**, the EN serves direct reads only on Client connections bound to a principal: the client
  first sends the same `MSG_CLIENT_AUTH{token}`, the EN verifies it with the same public keys, checks `aud`, and binds
  `BoundPrincipal`; thereafter every read request checks that the kid is still enabled and the token has not expired (the same function as the PS's
  `still_valid`, `cap_token.rs`). Unbound, expired token, disabled kid:
  all `PermissionDenied`.
- **A rejection is the result**: when the EN rejects this client (CLIENT_AUTH rejected, or a read rejected on credentials),
  the SDK does not switch replicas or fall back to the PS proxy; it returns `AutumnError::PermissionDenied` directly and leaves it to the caller
  to decide what to do: the same credential gets the same verdict on every EN, so retrying elsewhere only hides the rejection.
  Other direct-read failures (node unreachable, timeout, stale layout) still switch replicas and then fall back to the proxy.
  New kids are the exception: the first rejection of an unknown kid on an EN returns `Unavailable` and immediately fetches again (the SDK falls back
  to the proxy); only if the kid is still unknown in the config after that fetch is `PermissionDenied` returned.
- **The EN does not check ranges**: a valid principal can still read an extent outside its grant by guessing coordinates.
  Closing this would require the PS to sign each descriptor and the EN to verify it; not done.
- **EN config fetching**: one per process (`SharedAuthz`, fetched by shard 0, shared by all shards); every
  `--config-poll-secs` (default 30 s) it polls `MSG_GET_AUTHZ_CONFIG`, and fetches again on an unknown kid.
  Before the config arrives (`Unknown`) it rejects direct reads and CLIENT_AUTH, both with `Unavailable` (transient,
  not a verdict on the credential; the SDK falls back to the proxy as usual), and never allows traffic while it does not know whether authz is on;
  when the manager is unreachable it keeps the previous result.
- **SDK**: every time `ensure_token` obtains a token it hands it to the direct-read connection pool
  (`ConnPool::set_auth_token`); new connections send `CLIENT_AUTH` automatically after VERSION_HELLO;
  when the token changes, all old connections in the pool are dropped and reconnected.
- **Cluster members (Peer)**: EN-to-EN and PS-to-EN connections do not send CLIENT_AUTH; they are authenticated by the cluster secret
  (`cluster_secret_design.md`) and may call every EN interface.

### Threat model for destructive EN ops

Direct reads give a client a TCP address that reaches an EN; that connection must not double as a write/delete surface. The EN puts peers
into three classes:

- **Client role** (declared in VERSION_HELLO): only `READ` /
  `CLIENT_AUTH` are allowed (`check_opcode`, checked per frame). `APPEND` / `COMMIT_LENGTH` /
  `DELETE_EXTENT` / `FENCE_EXTENT` / `ALLOC_EXTENT` /
  `CONVERT_TO_EC` / `WRITE_SHARD` and every other op get `PermissionDenied`; the request is not
  decoded, let alone executed.
- **Peer / Admin role**: after VERSION_HELLO it must pass PEER_AUTH (a cluster-secret HMAC challenge);
  only then are all EN opcodes open. Faking the role does not help: a connection that claims Peer but cannot produce the secret is rejected before the first
  business frame. (This gate exists only when a cluster secret is installed; production binaries refuse to start without one.)
- There is no per-node token: the admission credential for destructive ops is the cluster secret, and its holders (manager / PS / EN
  / operators) are already the trusted computing base. The cap token a client holds only buys direct reads and has nothing to do with them.

Not defended: a compromised process that holds the cluster secret (see §9; the secret is the root of trust); a Client reading
extents outside its grant through EN direct reads (§10).

Rejections are not counted: a rejected frame or connection receives `PermissionDenied` (a PEER_AUTH failure additionally logs the peer in the EN log).
`Unavailable` (the EN has not obtained the authz config yet) is not a rejection.

## 11. References

FoundationDB tenant authorization (asymmetric JWT, verified at the storage layer, no revocation beyond short TTL);
Ceph cephx (MON as KDC + automatic ticket renewal, symmetric — we made it asymmetric); etcd RBAC
(central policy, the rejected per-request central-lookup shape); OAuth (access + refresh = our
token + long-lived credential).
