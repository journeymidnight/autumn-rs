# autumn-client Crate Guide

## Purpose

Client SDK for an autumn-rs cluster. Provides the ergonomic `ClusterClient` API for
KV operations, batching, namespace binding, region routing, zero-copy, and maintenance.

## Namespace binding

Every `ClusterClient` carries a `NamespaceBinding` that maps user keys onto wire keys
BEFORE routing, so a client can only touch its own `{scope}/` keyspace. **There is NO
tenant segment** — a `scope` is a whole namespace (`fs`, `gallery`) or an in-namespace
sub-prefix an app owns (`mem/agent7`). See `docs/key_namespace_split_design.md` §8.

Constructors:
- **`connect(mgr, scope)`** — the entry point EVERY data-plane writer MUST use.
  **Prepend-only**: a scoped client ALWAYS prepends `{scope}/` to the user key and
  strips it back off returned range keys, so it **cannot** touch anything outside its
  keyspace — scope is locked by construction, not merely checked (no assert/validate
  mode). Each `/`-delimited `scope` segment must match `[a-z0-9._-]+`; the FIRST is the
  namespace (Layer-A checks it). Built-in key builders (`fuse/key.rs`, `memory/keys.rs`,
  `kvc/_keys.py`) emit keys RELATIVE to `{scope}/` (the binding owns the prefix) so
  there is no double-prefix.
- **`connect_raw(mgr)`** — admin/unscoped (`Raw` binding, no client prefixing) for
  admin/mgr-only tooling (autumn-op, node registration), cross-namespace
  inspection/migration, and tests. NOT for data writers. The PS still enforces
  Layer-A/B — `raw` only bypasses the CLIENT prefixing.
- **`connect_with_credential(mgr, scope, principal, credential)`** — scoped + authz.
  `principal` = credential owner (from `read_credential_file` → `(principal, secret)`);
  `credential` = raw secret. When authz is on, `validate_credential_scope` verifies at
  connect that `{scope}/` ⊆ one of the credential's granted `allowed_prefixes` — **fail-fast**
  on a mis-scoped OR rejected/invalid credential (only an authz-DISABLED manager is a silent
  skip); `pub` so the PyO3 connect paths run the SAME check. `set_principal_credential` sets
  the identity on an existing client. Account admin: `principal_create` / `principal_delete`;
  `mint_token(principal, cred)`.
- **`raw()` / `rescope(scope)`** return a `NamespaceScope<'_>` borrow-view (shared pools,
  no reconnect) exposing the core ops under a different binding. `raw()` = `Raw`; the PS
  still enforces.

Semantics / invariants:
- **Bind a key exactly once per wire request.** Binding happens at each op's ENTRY
  (`put`/`get`/`delete`/`head`/`put_bulk`/`get_range`/`get_range_into`/`get_direct`; batch
  ops bind each key; `range` binds+clamps+strips). The bound key flows to both routing
  AND the payload closure. `*_bound`/`*_core`/`*_opts` are UNBOUND cores (they receive
  already-bound keys) so a scoped op never double-prefixes.
- **Stream ops call the plain `put`/`get`/`delete`.** A striped chunk key
  `\xff\xfe…++user_key` is just a normal user key to the binding, so a scoped client
  prepends `{scope}/` and the chunk lands INSIDE the scope range (Layer-A / authz /
  presplit cover it); no special chunk path needed under Prepend-only.
- **`range` clamp**: prepend `{scope}/` to the prefix, seed the cursor at the scope
  lower bound, cap the scan at `{scope-last-seg}0` (`0` = 0x30, successor of `/`), strip
  the prefix off returned keys.
- **`AutumnError::NamespaceUnknown` is TERMINAL on the write path** — from
  `StatusCode::NamespaceUnknown` (PS Layer-A reject); `call_ps_for_key` short-circuits it
  like `PermissionDenied` because refreshing routing can't create the namespace. Tests:
  `namespace_binding_tests`.

## Public API — data operations

**`ValueBuf` is the data-plane buffer currency**: a RegPool-backed slab
with a STABLE, recycled address (UCX runtime: `ucp_mem_map`-registered at slab creation
→ rcache/memh zero-copy; TCP runtime: plain recycler, no per-op alloc/zero). Write side:
`alloc_value_buf(len)` → fill `as_mut_slice()` → `truncate(n)` → `freeze() → Bytes` →
`put_bulk`. Read side: `get_pooled` hands the recv'd pool buffer back as a `ValueBuf`
(zero SDK-side copies); read in place or `freeze()` for a framework sink. Dropping
either returns the slab to the pool. Cross-thread drop of a frozen `Bytes` is legal but
frees the slab instead of re-pooling (regpool home-thread guard — foreign-TLS re-pooling
would corrupt per-thread pinning accounts). Fresh per-op allocations are the
anti-pattern: on UCX every send from a fresh address re-registers (~100 µs × rails) and
the rcache entry dies with the free — there is NO explicit registration anywhere;
UCX zero-copy is decided by the value's memory provenance via the implicit rcache.

**What `bulk` names (naming contract):** `MSG_*_BULK` (wire) = value-separable
framing — the value rides in the v28 frame's raw value region, never encoded or
CRC-scanned. `*_bulk`/`*_pooled` (SDK) = the SDK+RPC path itself adds ZERO value
copies. Deliberately named after the STRUCTURE, not an effect: the pre-rename `_zc`
("zero-copy") suffix baked a copies claim into a wire-layout name, and the claim only
holds under conditions the name can't see. TRUE end-to-end zero-copy additionally
depends on the SOURCE: (1) producer
writes directly into pool memory (autumnfs reads file chunks straight into a slab) =
0 copies + stable; (2) producer's own memory is already stable (kvcache aliases pinned
torch pages) = 0 copies + stable; (3) producer hands you its own fresh allocation
(an HTTP body) = pick 0-copies+fresh-address (TCP-optimal; UCX re-registers per op) OR
one staging memcpy into a `ValueBuf` for a stable address (UCX-optimal — the gallery
demo's choice). The copy count is decided by who allocates the source buffer, not by
the API suffix.

- `put(key, value, must_sync)` — write a key-value pair.
- `get(key) → Option<Vec<u8>>` — read, `None` if not found.
- `get_pooled(key) → Option<ValueBuf>` / `get_range_pooled(key, offset, length)` —
  **bulk read, ZERO SDK-side copies** — the CORE every bulk read routes through. The value
  arrives in a read_loop-owned RegPool buffer (`MSG_GET_BULK` + `call_into_pooled`; UCX
  RDMAs into the registered slab, TCP ≥ 64 KiB pays only the kernel copy) and is handed
  straight back. The address-UNCONSTRAINED shape ("I just want the value"): autumnfs
  cat/get, gallery serving, any consumer without a fixed destination. Any value size.
  Honors `rpc_timeout` (pooled recv is cancel-safe).
- `get_into(key, dest: &mut [u8]) → Option<usize>` — **bulk read, one copy**:
  `get_range_pooled` + ONE memcpy into `dest`. For address-CONSTRAINED consumers only
  (sglang pages / torch tensors / python buffers / fuse assembly) — the copy is inherent
  there (bytes must land at THEIR address; recv-into-caller-memory was removed for
  cancel-safety). Returns `Some(value_len)` (`dest[..value_len.min(dest.len())]` filled;
  longer values TRUNCATED to fit — same contract as `get_many_into`/`get_many_direct`)
  or `None`.
- `put_bulk(key, value: Bytes)` — **zero-copy write.** Writes with NO client-side copy AND
  no value crc scan via `MSG_PUT_BULK` + `RpcClient::call_vectored_bulk` (v28: `[meta][key]`
  is the CRC'd ctrl, the value rides after the crc as its own iovec from `value`'s backing
  memory; UCX zero-copy via rcache when that memory is `ucp_mem_map`-registered). Same
  routing + refresh + retry + inline-cap rules as `put`. The frame decoder hands the PS
  ctrl and value pre-split (zero-copy).
- `delete(key)` — delete a key.
- `head(key) → KeyMeta` — metadata (`found`, `value_length`).
- `range(prefix, start, limit) → RangeResult` — prefix scan (resume cursor below).

**Stream operations (large values):**
- `put_stream_begin(key, expires_at) → PutStreamHandle` — streaming writer; the handle
  writes striped chunks. (`expires_at` currently ignored.)
- `get_stream(key, chunk_size_hint) → Option<GetStream>` — streaming reader; auto-detects
  striped vs inline (a plain `put` value yields whole in one `next_chunk()`), `None` if the
  key doesn't exist.
- `delete_stream(key)` — delete a streamed value.

All three route through the plain `put`/`get`/`delete` (see the binding invariant above).

## Public API — batched operations

All batch APIs are thin wrappers over the `fan_out` / `fan_out_collect` primitive; per-op
bulk decisions go through `bulk_worthwhile`. No `concurrency` arg — internal defaults apply.

- `put_many(items: &[(key, Bytes, expires_at)]) → Vec<Result<()>>` — **public batched
  write.** Third tuple field is `expires_at` (Unix-epoch seconds; `0` = no TTL); convert
  a relative TTL with `ClusterClient::ttl_to_expires_at(ttl_secs)`. SDK groups by owning
  partition: values < 64 KiB → one `MSG_BATCH_PUT` per partition (server decodes one
  frame, atomically injects all ops into `partition_loop.pending`); values ≥ 64 KiB →
  per-op `MSG_PUT_BULK` fanned out CONCURRENTLY via
  `fan_out_collect(BATCH_PUT_DEFAULT_CONCURRENCY)`. Result `i` matches `items[i]`.
- `put_many_fenced(items, lease: WriteLease) → Vec<Result<()>>` — lease-fenced
  `put_many`: every item stamped with the SAME `(inode_hint, lease_epoch)` (one inode's
  flush). A fenced item returns `AutumnError::Fenced` in its result slot.
- `get_many(keys: &[&[u8]]) → Vec<Result<Option<Vec<u8>>>>` — **simpler batched read.**
  SDK allocates a `Vec<u8>` per value; one `MSG_BATCH_GET` per partition. Use when you
  don't know value sizes, don't want to pre-alloc dests, or values are < 64 KiB (bulk
  wouldn't engage).
- `get_many_into(items: &mut [GetManyItem]) → Vec<Result<Option<usize>>>` — **bulk batched
  read.** Use when values ≥ 64 KiB AND you have caller-owned dest buffers (sglang pages /
  torch tensors). Each `GetManyItem` = `{key, offset, length, dest}`. The bulk recv lands
  in a read_loop-owned RegPool buffer (UCX RDMAs into the registered slab; TCP owned
  read), then ONE memcpy into `dest` — `dest` needs no registration and no special
  lifetime. Auto-routes: HOMOGENEOUS small whole-value batch (every item `offset==0`,
  `length==0`, `dest.len() < 64 KiB`) → delegates to `get_many` + memcpy into each `dest`;
  MIXED / range / large-bulk → per-op fan-out (`MSG_GET_BULK` pooled recv when `read_len ≥ 64
  KiB`, else `MSG_GET` + memcpy). Result `i` matches `items[i]`.
- `get_many_direct(items: &mut [GetManyItem]) → Vec<Result<Option<usize>>>` — **EN-DIRECT
  batch read.** Same dest shape as `get_many_into`, but each item with length ≥ 64 KiB is
  read STRAIGHT from an extent node (`MSG_GET_REDIRECT` descriptor →
  `read_extent_value_direct`), taking the PS off the large-value DATA path (cross-host
  throughput win). Sub-64 KiB items stay on the proxy `get_range` path, so MIXED batches
  route per item. **Per item, ANY direct-read failure falls back to the proxy** — degrades
  gracefully where ENs aren't client-reachable. The call-vs-`get_many_into` DECISION is
  TOPOLOGY-dependent, so it's a frontend-owned flag (fuse `--direct-read`, python
  `BatchClient(direct=…)` / `autumn.Fs.connect(direct_read=…)` / kvcache / vLLM-loader),
  DEFAULT ON — safe because size-gated AND proxy fallback is authoritative. First fallback
  logs ONE `WARN` (`DIRECT_FALLBACK_WARNED`). Shares the replica-failover loop with
  `get_direct` (`read_redirect_replicas`). Same recv shape as the proxy path (pooled
  recv + one memcpy into `dest`; cancel-safe → the size-scaled timeout + replica
  failover are safe); the win over `get_many_into` is ROUTING (PS NIC egress leaves the
  large-value data path), not copy count.
- `delete_many(keys) → Vec<Result<()>>` / `head_many(keys) → Vec<Result<KeyMeta>>` —
  batched delete / metadata. Client-side fan-out (no server `MSG_BATCH_*`), no bulk (tiny).
  `delete_many` uses `BATCH_PUT_DEFAULT_CONCURRENCY`, `head_many` uses
  `BATCH_GET_DEFAULT_CONCURRENCY`. `head_many` returns `found=false` for a missing key
  (not `Err`).

**Internal-only (`pub(crate)`):** `batch_put` — the server-batched RPC layer for writes,
reached through `put_many`. Kept `pub(crate)` so unit tests + the delegation layer share
one implementation; callers should not invoke directly.

## Batch fan-out primitive — `fan_out` / `fan_out_collect`

Module-level, `pub`. All batch APIs are thin wrappers over ONE streaming primitive:
- `fan_out(futs, concurrency) -> impl Stream<Item=(usize, Fut::Output)>` — drives `futs`
  with a bounded sliding window (`buffer_unordered`) and yields `(input_index, output)`
  in COMPLETION order (the EN/PS SQ/CQ "fire N, reap as they land" shape, lifted to the
  client).
- `fan_out_collect(futs, concurrency) -> Vec<Output>` — collects `fan_out` back into
  INPUT order. `get_many_into` / `put_many` / `delete_many` / `head_many` each build
  per-item futures and call this; only the per-item logic differs.
- **Streaming consumers** drive `fan_out` directly and act per completion (e.g. an
  io_uring daemon, one CQE per finished SQE, no head-of-line wait) — the reason a
  batch-COLLECT primitive like `get_many_into` is the wrong fit there.

## Bulk-value selection rule — `bulk_worthwhile`

The SDK exposes both regular (`get`/`put`) and zero-copy (`get_into`/`put_bulk`) ops; the
SELECTION is encapsulated in `autumn_client::bulk_worthwhile(value_size) -> bool` — **the
ONE source of truth** that perf-check, the python `BatchClient`, and `get_many_into` all
call.

**One symmetric rule — engage bulk iff `value_size >= BULK_MIN_BYTES` (64 KiB), for
BOTH reads and writes AND BOTH transports.** This is the INTENT gate (which msg_type/API
to use, from the size the sender knows/expects); the two RECEIVERS re-decide their recv
strategy on the ACTUAL size with the same 64 KiB — the client read_loop for GET-bulk
responses (`TCP_RECV_INTO_POOLED_MIN_BYTES`), the PS ps-conn loop for PUT_BULK requests
(`AUTUMN_PS_BULK_RECV_MIN_BYTES`) — because a reply/request can legitimately be smaller
than the intent predicted (error/NotFound = 0-length value; bare `put_bulk` doesn't gate).
All four gates (this one + the two recv gates + the PS `handle_get_redirect` 64 KiB) are
deliberately one value; the dispatch table lives in autumn-rpc CLAUDE.md "read_loop
dispatch (4-way)". Below 64 KiB the per-op registered/pooled-recv machinery costs more
than the copy it saves AND the recv side doesn't bulk anyway, so e2e bulk doesn't engage;
at/above it bulk wins on both transports (UCX RDMA into the registered pool slab /
registered-send; TCP pooled recv dropping the rkyv wrap + FrameDecoder accumulation +
owned-`Vec` alloc).

There is no `--bulk` / `bulk=` flag — call `bulk_worthwhile(size)`. The const + helper live in
`crates/client/src/lib.rs` so the CLI and the python extension share one source of truth.

## Public API — maintenance operations

- `split(part_id)` — trigger partition split.
- `compact(part_id)` — trigger compaction.
- `gc(part_id)` — trigger automatic GC.
- `force_gc(part_id, extent_ids)` — force GC on specific extents.
- `flush(part_id)` — trigger memtable flush.
- `merge_partitions(survivor, victim)` — partition merge (CLI orchestration).
- `policy_candidates() → Vec<PolicyCandidate>` — advisory engine output.
- `submit_op(OpSubmitReq) → OpSubmitResp` / `op_query(OpQueryReq) → OpQueryResp` —
  the async op-ledger (MSG_OP_SUBMIT/QUERY): submit a long-running op (split/merge/
  rebalance/compact/gc/forcegc/ec-convert) and get an `op_id` back, then poll for
  its terminal state + failure reason. `autumn-op`'s op triggers + `ops status`/
  `ops list` route through these; the low-level blocking methods above
  (`split`/`compact`/`gc`/`force_gc`/`merge_partitions`/`rebalance_regions`) stay
  as the direct path (tests + internal callers).

## Per-call timeout

- `set_rpc_timeout(Duration)` / `clear_rpc_timeout()` / `rpc_timeout() → Option<Duration>`.

Default is **None (wait forever)**. When set, every `ps_call` (`put`/`get`/`delete`/`head`/
`range`/ stream-op chunks / `merge_partitions`'s FLUSH + downstream PS calls / PutChunk /
etc.) is raced against `compio::time::sleep(timeout)`. Expiry surfaces as
`AutumnError::ConnectionError` so the caller's routing-miss retry path triggers a
`refresh_regions` + one retry.

**Why it exists:** the PS may drop a partition's `req_rx` mid-call (region_sync_loop
reload after merge, split's drain, graceful shutdown). The drop closes the per-request
response oneshot WITHOUT closing the underlying TCP connection (other partitions share
it), and autumn-rpc's closed-state flag fires only on TCP close — so without a timeout
`cluster.put().await` hangs forever. Recommended: 2–5 s for reads, 2 s for split/merge
tests, cleared/30+ s for multi-GiB streamed writes.

**NOT timed out:** `mgr_call` / `mgr_call_retry` — manager calls already have round-robin
failover via `rotate_manager` on connection error.

## Public API — low-level (CLI / benchmarks)

- `mgr_call(msg_type, payload)` — raw manager RPC.
- `mgr_call_retry(msg_type, payload, max_retries)` — with round-robin retry.
- `ps_call(ps_addr, msg_type, payload)` — raw PS RPC.
- `get_ps_client(ps_addr)` — get/create PS connection.
- `resolve_key(key) → (part_id, ps_addr)` — route key to partition.
- `resolve_part_id(part_id) → ps_addr` — resolve partition to PS.
- `all_partitions() → Vec<(part_id, ps_addr)>` — list all partitions.

## Admin-token prefixing

`set_admin_token(token)` sets a per-client admin token. On send, the SDK prefixes it onto
the payload of admin RPCs — manager msgs classified by
`autumn_rpc::manager_rpc::is_admin_mgr_msg`, PS msgs by
`autumn_rpc::partition_rpc::is_admin_ps_msg`, both via `manager_rpc::prefix_admin_token`
(`[u32 len][token][payload]`). Non-admin msgs are sent unmodified. **INVARIANT: do not add
a raw manager/PS call path for admin RPCs** — route them through the classified send so
the token is always prefixed (greppable: `is_admin_mgr_msg` / `is_admin_ps_msg`).

## Error types

- `AutumnError::NotFound` — key not found.
- `AutumnError::InvalidArgument(msg)` — bad request.
- `AutumnError::PreconditionFailed(msg)` — e.g. split with overlap.
- `AutumnError::ServerError(msg)` — internal server error.
- `AutumnError::RoutingError(msg)` — cannot route key.
- `AutumnError::ConnectionError(msg)` — RPC connection failure.
- `AutumnError::NamespaceUnknown` — Layer-A reject (terminal on write path).

## Result types

- `KeyMeta { found, value_length }` — from `head()`.
- `RangeResult { entries: Vec<RangeEntry>, has_more }` — from `range()`.
- `RangeEntry { key, value }` — re-exported from partition_rpc.

## Architecture

- Single-threaded (`Rc`/`RefCell`) — designed for the compio single-thread runtime.
- Manager connections: round-robin failover on error, auto-reconnect.
- PS connections: cached per-address, dropped on error, recreated on next call.
- Routing: `GetRegions` cached at connect, refreshed on routing miss (binary search).

### Per-partition routing (part_addrs resolver)

Each partition binds its own TCP listener at `base_port + ord`. The PS-level address from
`register_ps` (cached in `ps_details[ps_id].address`) only owns the FIRST partition opened
on that PS — a RangeReq / PutReq / GetReq for any other partition sent there gets
`CODE_NOT_FOUND` from the receiving `partition_loop`'s mis-routed-frame fast path.

**INVARIANT: every cross-partition / per-partition call site MUST resolve via
`part_addrs[part_id]` first**, falling back to `ps_details[ps_id].address` only when the
partition is not yet registered (transient post-split state):

| Call site | Resolver |
|-----------|----------|
| `lookup_key` (get/put/del/head + stream ops) | `part_addrs.get(part_id).or_else(ps_details[ps_id])` |
| `resolve_part_id` (split/compact/gc/flush) | same |
| `all_partitions` (CLI listing) | same |
| `range` (cross-partition scan) | same |

`range` additionally surfaces per-partition errors instead of `continue`: silently
dropping one partition's response would return a half-empty `Ok(RangeResult)`
indistinguishable from a true empty result.

### `region_epoch` + resume cursor

The SDK stamps a `region_epoch: u64` (cached from `MgrRegionInfo.region_epoch`, bumped by
the manager on every `rg` rewrite — split / merge) on every hot-path request. The PS
rejects with `FailedPrecondition` when the stamped epoch mismatches; `call_ps_for_key`'s
`Err`-arm refresh path picks it up (drop conn, `refresh_regions`, retry).

Wire surface:
- `MgrRegionInfo` carries `region_epoch: u64`.
- `PutReq` / `GetReq` / `DeleteReq` / `HeadReq` / `RangeReq` / `StreamPutReq` carry
  `region_epoch: u64`. Admin ops (`MaintenanceReq`, `SplitPartReq`, `MergePartReq`) are
  EXEMPT — the operator is the authoritative caller.
- `RangeResp` carries `cur_end_key: Vec<u8>` — the PS's authoritative `rg.end_key`, used as
  the resume cursor.
- `CODE_REGION_EPOCH_STALE = 8` is reserved for future inline use (today staleness surfaces
  via `StatusCode::FailedPrecondition` at the frame level).

SDK plumbing:
- `lookup_epoch_for_part(part_id)` — public helper used by FUSE / ioring / CLI / SDK
  internals to stamp the cached epoch when manually assembling a Req.
- `call_ps_for_key`'s build closure is `Fn(u64, u64) -> Bytes` (`(part_id, region_epoch)`).
  On retry it re-invokes with the freshly-cached epoch so the second attempt reflects
  post-refresh routing.

`range()` resume cursor: each successful `RangeResp` returns `cur_end_key`, used as the
next iteration's `start_key`. A split mid-scan auto-resolves on the next `resolve_key`
against the (possibly refreshed) cache. On epoch-stale error, results from
already-successful partitions are KEPT; only the failing partition is re-resolved +
re-scanned. Bounds: `MAX_RANGE_REFRESHES` (3) refresh cycles, `MAX_RANGE_ITERATIONS`
(10_000) iteration cap against pathological churn.

**Tests / benches stamp `region_epoch: 0`** — the wire-level "skip check" sentinel;
production callers always stamp non-zero from the cache. A `lookup_epoch_for_part` on a
partition not in the cache returns `0` (skip check) rather than failing — the resolver
already handles "no such partition"; epoch is opportunistic.

### Error classification: terminal vs transient (admin ops fail fast)

`ps_call` returns the typed `AutumnError` for a frame-level `RpcError::Status{code,msg}` via
`rpc_status_to_error` (preserved inside `anyhow`, downcastable), so routing-retry loops
branch on error KIND instead of string-matching.

- **`call_ps_for_part`** (admin ops: split/compact/gc/flush) short-circuits
  `PreconditionFailed | InvalidArgument | ValueTooLarge` — these are DETERMINISTIC (e.g.
  "overlapping keys", "needs ≥ 2 keys") and refreshing routing can't fix them, so it
  returns immediately instead of burning `MAX_PS_REFRESHES` (10, ~9 s). Admin ops are
  region_epoch-EXEMPT, so a FailedPrecondition here is never the stale-epoch case.
  Transient errors (NotFound from a not-yet-registered post-split partition /
  ConnectionError / routing miss) still refresh + retry.
- **`call_ps_for_key`** (data ops) is deliberately UNCHANGED: there a FailedPrecondition is
  (often) a stale `region_epoch`, which MUST refresh + retry. **INVARIANT: do not add the
  short-circuit to the data path.**

Two error channels stay distinct: `rpc_status_to_error` maps frame-level `StatusCode`
(handler returned `Err((StatusCode, msg))`); `code_to_error` maps application-level `CODE_*`
carried in a successful response body.

## Constants

- `BULK_MIN_BYTES = 64 KiB` — bulk engage threshold (both directions/transports).
- `BATCH_PUT_DEFAULT_CONCURRENCY = 32` — put/delete fan-out cap.
- `BATCH_GET_DEFAULT_CONCURRENCY = 32` — get/head fan-out cap.
- `MAX_PS_REFRESHES = 10` — routing-refresh retry budget (~9 s), base 100 ms / cap 2000 ms.
- `MAX_RANGE_REFRESHES = 3` — range refresh cycles per call.
- `MAX_RANGE_ITERATIONS = 10_000` — range iteration cap.

## Dependencies

- `autumn-rpc`: RPC client + wire codec (partition_rpc, manager_rpc).
- `compio`: async runtime (time::sleep for retry backoff).
- `anyhow`, `bytes`: error handling + byte buffers.
