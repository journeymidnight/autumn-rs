# autumn-etcd Crate Guide

## Purpose

Minimal compio-native etcd v3 client. Eliminates tokio/etcd-client dependency from autumn-manager by implementing gRPC over HTTP/2 directly on the compio runtime: cleartext (h2c) by default, TLS (rustls) when configured, optionally authenticated as an etcd user.

## Architecture

```
compio::net::TcpStream
  → [compio::tls::TlsStream] (rustls, ALPN h2 — only with EtcdConfig.tls)
    → cyper_core::HyperStream (compio→hyper I/O adapter)
      → hyper::client::conn::http2::handshake()
        → HTTP/2 POST to gRPC endpoints, `token` header when authenticated
```

**Key insight**: hyper's low-level `client::conn::http2` module supports h2c (HTTP/2 over plaintext TCP) without any ALPN/TLS negotiation. We just do the HTTP/2 handshake directly on a raw TCP stream wrapped in cyper-core's `HyperStream` adapter. TLS is the same stack with a `TlsStream` under the adapter; etcd picks HTTP/2 by ALPN, so the client offers `h2`.

## TLS and auth

- `EtcdConfig { endpoints, tls, credentials }`, `EtcdClient::connect_with`. `connect` / `connect_many` are the cleartext, unauthenticated forms every test uses.
- `EtcdEndpoint { addr, server_name }`: the manager resolves names once at startup, so `addr` is an IP literal; `server_name` is the host as the operator wrote it and is what the server certificate is checked against (an IP is checked against the certificate's IP SANs).
- `TlsConfig::from_pem_files(ca, Some((cert, key)))`: roots from the CA file only (no system roots), ring provider chosen explicitly (not the process default, which panics when two providers are linked).
- **Each connection authenticates for itself** (`GrpcChannel::connect` → `Auth/Authenticate`), as Go clientv3 does per stream. A simple token is valid on every member, but its expiry runs on each member separately (reset by use there), so a token live on one member can already be gone on the next after a reconnect; a JWT expires at a fixed time wherever it is used. A token is never carried from one connection to another.
- **A refused token is re-authenticated and the request sent once more — txn included.** `is_stale_token`: gRPC `Unauthenticated` (expired / unknown token, refused before the raft proposal) or "revision of auth store is old" (refused at apply, before any write). Re-sending a txn after either cannot apply it twice; this is the one exception to "a txn is never re-sent". After that second send a txn fails definitely only if the token is refused again; any other error, a gRPC status included ("request timed out" is answered after the proposal), is `UncertainOutcome` (`resend_outcome`, unit test `txn_after_reauth_is_uncertain_unless_the_token_is_refused_again`). Only the txn needs this path to succeed (`expired_token_is_replaced` goes red on the txn without it): reads and keepalives also recover through the reconnect path, whose new connection authenticates.
- gRPC status is read from the headers (trailers-only answer) **and from the trailers**: etcd answers a failed `Authenticate` with an empty body and the status in the trailers. Errors are typed `GrpcStatus { path, code, message }`.
- A wrong password or an untrusted / misnamed certificate fails `connect_with` with each endpoint's cause in the message — the manager does not start.

## API Surface

```rust
// Connect
let client = EtcdClient::connect("127.0.0.1:2379").await?;
let client = EtcdClient::connect_many(&["ep1:2379".into(), "ep2:2379".into()]).await?;
let client = EtcdClient::connect_with(EtcdConfig { endpoints, tls: Some(tls), credentials: Some(creds) }, Duration::from_secs(10)).await?;

// KV
client.get(b"key").await?;                     // single key
client.get_prefix(b"prefix/").await?;          // all keys with prefix
client.put(b"key", b"value").await?;           // put
client.put_with_lease(b"key", b"val", lease).await?;  // put with lease
client.delete(b"key").await?;                  // delete

// Txn (CAS + batch)
client.txn(TxnRequest {
    compare: vec![Cmp::create_revision(b"key", 0)],  // key doesn't exist
    success: vec![Op::put(b"key", b"val")],           // then create it
    failure: vec![],
}).await?;

// Lease
let grant = client.lease_grant(10).await?;     // 10-second TTL
let keeper = client.lease_keep_alive(grant.id, Duration::from_secs(2)).await?;  // per-step bound
let resp = keeper.keep_alive().await?;         // send one keepalive, get response
client.lease_revoke(grant.id).await?;          // revoke lease

// Any unwrapped unary RPC (tests: auth admin)
let resp: R = client.unary("/etcdserverpb.Auth/AuthEnable", &req).await?;
```

`Cmp::mod_revision(key, rev)` is the attempt-identity compare for keys that may
be deleted and recreated with byte-identical values. `Cmp::value` alone cannot
distinguish those two lifetimes.

## gRPC Framing

Manual 5-byte frame: `[compress:0][length:4 BE]` + protobuf body.
- Content-Type: `application/grpc`
- TE: `trailers`
- Encoding: prost

## Protobuf Types

Hand-defined in `proto.rs` using `prost::Message` derive. Only the ~15 message types needed by autumn-manager:
- `KeyValue`, `ResponseHeader`
- `RangeRequest/Response`, `PutRequest/Response`, `DeleteRangeRequest/Response`
- `Compare`, `RequestOp`, `ResponseOp`, `TxnRequest/Response`
- `LeaseGrantRequest/Response`, `LeaseKeepAliveRequest/Response`, `LeaseRevokeRequest/Response`
- `AuthenticateRequest/Response`

No watch: the manager's standby polls the leader key (see crates/manager
"Leader election"), so the client has no streaming call at all.

## Threading Model

Single-threaded compio (`Conn { channel: RefCell<GrpcChannel>, .. }`, shared by `EtcdClient` and each `LeaseKeeper`'s own connection). Not Send/Sync — matches the rest of autumn-rs's compio design. Each connection is used from one thread. Concurrent in-flight RPCs pipeline over one TCP connection via HTTP/2 multiplexing.

**INVARIANT — never hold a channel borrow across `.await`.** The `RefCell<GrpcChannel>` exists only so `Conn::reconnect` can swap the whole channel synchronously. Every call path clones the underlying `http2::SendRequest` out of the cell first (cheap — mpsc handle + Arc), drops the borrow, then awaits via the free function `transport::call_with_sender`. Holding `RefMut<GrpcChannel>` across `.await` panics the next concurrent task on the same runtime with `RefCell already borrowed`. When adding a new RPC method, **never** write `self.channel.borrow_mut().<anything>().await` — use `let mut call = self.channel.borrow().call();` (borrow dropped at the semicolon) then `call_with_sender(&mut call, path, body).await`. A re-authentication writes the new token into that call's connection (`Call.token` is the connection's slot), so a token never lands on a channel swapped in meanwhile.

**Future size.** The rare paths are boxed out of every call's future: `Conn::reconnect` and the re-authentication (`reauth_and_resend`) under `Box::pin`, and the connection's stream as `Box<HyperStream>` (the h2 connection holds it by value in several layers; the TLS variant is over a KB). Measured in a debug build: `get`/`txn` 2.9 KB, `connect` 1.9 KB; unboxed they were 21.7 KB / 7.2 KB, enough to overflow the 2 MiB stacks of the manager's test threads.

**INVARIANT — callers hold `Rc<EtcdClient>`, not `Rc<RefCell<EtcdClient>>`.** `EtcdClient` is all-`&self`, constructed once, never replaced, and self-reconnects internally — so a manager RPC is just `self.client.method().await` with no borrow held across the await. Do not wrap `EtcdClient` in an outer `RefCell`.

**INVARIANT — every etcd call is timeout-bounded, by type.** `within` and `Conn::call` take a `Duration`, not an `Option`: there is no way to say "no deadline". `Conn::call` bounds each `call_with_sender` await, the re-authentication, and each endpoint `reconnect` tries; `Conn::open_at` bounds the whole connect (TCP, TLS, h2, authenticate) — a frozen etcd still completes the TCP handshake (the kernel does it), and with TLS or auth the connect then hangs. The bound is per endpoint, not one over the whole round: one bound let a frozen member eat it, so the members after it were never tried and `current_ep` never moved (`leader_reconnects_past_a_frozen_member`: 5 members with auth, the current one dead and the next frozen — the leader stepped down at 10 s with a quorum alive). `connect_with` and `lease_keep_alive` open through `Conn::open_any` (each endpoint once, from the first / from the client's current one). `connect_with(config, request_timeout)` takes the bound of its first connect (per endpoint) and of every unary call (the manager's `--etcd-request-timeout-ms`, default 10 s; `connect`/`connect_many` use 10 s). A `LeaseKeeper` uses the `attempt_timeout` given to `lease_keep_alive` for its connect and every renewal step (the manager passes TTL/5, so a renewal moves to another member in time). Only the mid-call `reconnect` caps each endpoint at `CONNECT_TIMEOUT` (2 s; milliseconds on a LAN): without the cap a round over the members cost n request timeouts. The first connect is not capped — with auth, `Authenticate` is a linearizable read and waits for an etcd leader (an election takes seconds), and a failed startup connect exits the manager. Worst case of one call with timeout T over n endpoints: max(2T + n·min(T, 2 s), 3T) (send, reconnect round, resend; or send, re-authenticate, resend) — 30 s for the main client (T = 10 s) on 3 members. The keeper once passed no timeout: a frozen etcd hung the leader's renewal forever and it kept answering as the leader (`crates/manager/tests/leader_keepalive_half_open.rs`). etcd-over-h2c has no built-in per-request deadline, so an unbounded call on a half-open TCP hangs the caller forever — and every manager background loop ultimately awaits an etcd call. On timeout it falls through to the bounded reconnect path (identical to a connection error) and, for an idempotent call (reads, puts, deletes, leases), re-sends once on the new connection; a second failure returns `Err`. Any new path MUST go through `Conn::call` — never add a bare `call_with_sender`.

**INVARIANT — a txn is never re-sent.** `txn` reconnects after a failure or timeout but returns `UncertainOutcome` (a typed error the caller can `downcast_ref`) instead of sending the request again: the first copy may already have committed — or may still commit when its connection drains — and a compare-and-set sent twice refuses its own first write. Deciding is the caller's (the manager's `fenced_txn`: it moves its fence, reads the written keys back, and re-sends only a txn that provably did not land). Do not add a retry here for "just this one" txn.

## Dependencies

- `compio` (net, time, io-compat, rustls, ring) — async runtime + TCP + TLS
- `rustls` (ring) — TLS client config, PEM loading
- `cyper-core` — HyperStream adapter (compio→hyper I/O bridge), CompioExecutor
- `hyper` (client, http2) — HTTP/2 protocol
- `http-body-util` — body utilities for hyper
- `prost` — protobuf encoding/decoding
- `bytes`, `anyhow`, `tracing`

**NOT** dependent on: tokio, tonic, etcd-client, reed-solomon, etc.

## Testing

```bash
# Unit tests (no external deps)
cargo test -p autumn-etcd --lib

# Spawn their own etcd (`etcd` on $PATH, or AUTUMN_TEST_ETCD_BIN); harness in
# tests/common (certificates from rcgen, auth turned on through the client)
cargo test -p autumn-etcd --test concurrent_calls
cargo test -p autumn-etcd --test auth_tls
```

`auth_tls`: mutual TLS + root password — every call the manager makes (put,
get, txn, lease grant/keepalive/revoke), an anonymous client refused; wrong password, foreign CA,
wrong server name fail the connect; with `--auth-token-ttl 1` each kind of call
meets an expired token once.

The test's etcd counts as ready only when the spawned child itself listens on
its client port (`listens_on`): under nextest other test processes can take a
port between `free_port` and etcd binding it, and a child that exits is started
again on fresh ports.

## Runtime compatibility

Compio 0.19.2 pairs with cyper-core 0.9. Connections use
HyperStream::new_plain, or new_tls over compio-tls's rustls (compio features
`rustls` + `ring`; `rustls` 0.23 must stay the version compio-tls links, its
`From<Arc<ClientConfig>>` takes that type). Detached HTTP/2 driver
tasks retain their existing lifetime. Build with Rust 1.95 or newer.
