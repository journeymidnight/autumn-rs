# autumn-client Crate Guide

## Purpose

Client SDK library for interacting with an autumn-rs cluster. Provides high-level ergonomic API for KV operations, routing, and maintenance.

## Public API

### ClusterClient

Main entry point. Connect via `ClusterClient::connect("addr1,addr2", scope)`.

### F-NS-PRINCIPAL-UNIFIED — namespace binding (Option 3, §8)

Every `ClusterClient` carries a `NamespaceBinding` that maps user keys onto wire
keys BEFORE routing, so a client can only touch its own `{scope}/` keyspace.
**There is NO tenant segment** — a `scope` is a whole namespace (`fs`, `gallery`)
or an in-namespace sub-prefix an app owns (`mem/agent7`). (Historical: tenant-first
`{tenant}/{ns}/` and the earlier `{ns}/{tenant}/` were retired 2026-07-19; wire
bumped to v26 to fence stale images. See docs/key_namespace_split_design.md §8.)
- **`connect(mgr, scope)`** — the entry point EVERY data-plane writer must use.
  **Prepend-only**: a scoped client ALWAYS prepends `{scope}/` to the user key and
  strips it back off returned range keys, so a scoped client **cannot** touch
  anything outside its own keyspace — scope is locked by construction, not merely
  checked (there is no Assert/validate mode). Each `/`-delimited segment of `scope`
  must match `[a-z0-9._-]+`; the FIRST is the namespace (Layer-A checks it). The
  built-in key builders (`fuse/key.rs`, `memory/keys.rs`, `kvc/_keys.py`) emit keys
  RELATIVE to `{scope}/` (the binding owns the prefix) so there is no double-prefix.
- **`connect_raw(mgr)`** — admin/unscoped (`Raw` binding, no client prefixing).
  For admin/mgr-only tooling (autumn-op, node registration), cross-namespace
  inspection/migration, tests. NOT for data writers. The PS still enforces
  Layer-A/B, so `raw` only bypasses the CLIENT prefixing.
- **`connect_with_credential(mgr, scope, principal, credential)`** — scoped +
  authz. `principal` is the credential owner (read from the credential file's name
  line — `read_credential_file` returns `(principal, secret)`); `credential` is the
  raw secret. When authz is on, `validate_credential_scope` verifies at connect that
  `{scope}/` ⊆ one of the credential's granted `allowed_prefixes` (decodes the
  minted token's claims client-side) — **fail-fast** on a mis-scoped credential OR a
  rejected/invalid credential (only an authz-DISABLED manager is a silent skip).
  `pub` so the PyO3 connect paths run the SAME check. Account admin: `principal_create`
  / `principal_delete` (were `tenant_create`/`tenant_delete`); `mint_token(principal,
  cred)`. `set_principal_credential` sets the identity on an existing client.
- **Binding placement**: at each op's ENTRY (`put`/`get`/`delete`/`head`/`put_zc`/
  `get_range`/`get_range_into`/`get_direct`, batch ops bind each key,
  `range` binds+clamps+strips). The bound key flows to both routing AND the
  payload closure. `*_bound`/`*_core`/`*_opts` are UNBOUND cores (receive already-
  bound keys) so a key is bound EXACTLY ONCE per wire request. **Stream ops call
  the plain `put`/`get`/`delete`** — an F186 striped chunk key `\xff\xfe…++user_key`
  is just a normal user key to the binding, so a scoped client prepends
  `{scope}/` and the chunk lands INSIDE the scope range (Layer-A/authz/
  presplit cover it); no special chunk path needed under Prepend-only.
- **`range` clamp**: prepend `{scope}/` to the prefix, seed the cursor at
  the scope lower bound, cap the scan at `{scope-last-seg}0` (`0`=0x30, the
  successor of `/`), strip the prefix off returned keys.
- **`raw()` / `rescope(scope)`** return a `NamespaceScope<'_>` borrow-view
  (shared pools, no reconnect) exposing the core ops (`put`/`get`/`delete`/`head`/
  `range`) under a different binding. `raw()` = `Raw`; the PS still enforces.
- **Errors**: `AutumnError::NamespaceUnknown` (from `StatusCode::NamespaceUnknown`
  = the PS Layer-A reject) is TERMINAL on the write path (`call_ps_for_key`
  short-circuits it like `PermissionDenied` — refreshing routing can't create the
  namespace). Tests: `namespace_binding_tests`.

**Data operations:**
- `put(key, value, must_sync)` — write a key-value pair
- `put_with_ttl(key, value, must_sync, ttl_secs)` — write with TTL (seconds)
- `get(key) → Option<Vec<u8>>` — read, returns None if not found
- `get_into(key, dest: &mut [u8]) → Option<usize>` —
  **zero-copy read.** Reads the value straight into `dest` (no Vec) via
  `MSG_GET_ZC` + `RpcClient::call_into_dest`; returns `Some(value_len)`
  (`dest[..value_len]` filled) or `None` if not found. Caller sizes `dest`
  (e.g. from `head`). Same routing + epoch-stale refresh + RPC-retry shape
  as `call_ps_for_key`. No per-call timeout — `dest` MUST outlive the call
  (cancel-safety, see autumn-rpc CLAUDE `call_into_dest`).
  On UCX, the first call into a fresh `dest` address pays a one-time
  rcache miss (`~100 µs ibv_reg_mr`); subsequent calls into the SAME
  address hit the rcache for free, so any pool / long-lived buffer is
  effectively zero-copy from the second call onward. There is no
  `reg: Option<&RegisteredMem>` argument — the UCX rcache handles
  registration transparently. Power users who want to skip the first-call
  cost on a hot path can call `autumn_transport::register_memory` directly
  to pre-populate the rcache; the SDK finds the registration without any
  SDK-API hook.
- `put_zc(key, value: Bytes)` — **F216-E zero-copy write.** Writes the value with
  NO client-side copy via `MSG_PUT_ZC` + `RpcClient::call_vectored` (value sent as
  its own iovec straight from `value`'s backing memory; on UCX zero-copy via
  rcache when that memory is `ucp_mem_map`-registered — caller holds a
  `RegisteredMem` and passes a `Bytes` aliasing the registered region). `put`
  copies the value 3× (to_vec → clone → rkyv_encode); this copies 0. Same
  routing + epoch-stale refresh + RPC-retry as `call_ps_for_key`; same inline-cap
  rules as `put`. The PS slices key+value zero-copy from the frame.
- `delete(key)` — delete a key
- `head(key) → KeyMeta` — get metadata (found, value_length)
- `range(prefix, start, limit) → RangeResult` — prefix scan
- `stream_put(key, value, must_sync)` — write large values
- `put_many(items: &[(key, Bytes, expires_at)]) → Vec<Result<()>>` —
  **the public batched-write API.** Third tuple field carries
  `expires_at` (Unix-epoch seconds; `0` = no TTL). Use
  `ClusterClient::ttl_to_expires_at(ttl_secs)` to convert a relative
  TTL. The SDK groups items by owning partition and routes them
  internally: values < 64 KiB → one `MSG_BATCH_PUT` per partition
  (server decodes one frame, atomically injects all ops into
  `partition_loop.pending`); values ≥ 64 KiB → per-op `MSG_PUT_ZC`
  **fanned out CONCURRENTLY via `fan_out_collect(BATCH_PUT_DEFAULT_CONCURRENCY)`**
  (value as its own iovec; RDMA when caller-registered) — mirrors the
  concurrent fan-out every other batch API uses. This ZC path used to be a
  serial `for … .await`, which capped a large-value batch (autumnfs / fuse
  8 MiB extents, big kvcache pages) at ONE durable put per round trip
  (extent_size ÷ per-put latency ≈ 40 MB/s when the durable path is
  latency-bound; the PS group-commit — F256 natural batching — then coalesces
  the concurrent puts). No change when the path is bandwidth-bound (single
  fast-local partition/connection saturates at its stream ceiling regardless).
  Result `i` matches `items[i]`. NO `concurrency` arg — partition-by-partition
  issuance + the per-op fan-out cap are the natural pacing.
- `get_many(keys: &[&[u8]]) → Vec<Result<Option<Vec<u8>>>>` —
  **the simpler batched-read API.** SDK allocates a `Vec<u8>` per
  returned value; one `MSG_BATCH_GET` per owning partition. Use this
  when you don't know value sizes ahead of time, don't want to
  pre-alloc dest buffers, or values are small (< 64 KiB) so ZC
  wouldn't engage anyway. For < 64 KiB this has identical perf to
  `get_many_into`'s small path (both pay one rkyv decode-copy).
- `get_many_into(items: &mut [GetManyItem]) → Vec<Result<Option<usize>>>` —
  **the ZC batched-read API.** Use when values are ≥ 64 KiB AND you
  have caller-owned dest buffers (e.g. sglang pages / torch tensors —
  UCX RDMA into the dest is true end-to-end zero-copy from the
  second call onward; the first call into a fresh address pays the
  one-time rcache miss). Each `GetManyItem` carries
  `{key, offset, length, dest}` — no `reg` field; UCX rcache handles
  registration. SDK auto-routes:
  - HOMOGENEOUS small whole-value batch (every item: `offset == 0`,
    `length == 0`, `dest.len() < 64 KiB`) → delegates to `get_many`
    + memcpys each result into its `dest` (read p99 4× lower than
    per-op fan-out on loopback).
  - MIXED / range / large-ZC → per-op fan-out: `MSG_GET_ZC` into
    `dest` (UCX RDMA when the dest is in the rcache) for
    `read_len ≥ 64 KiB`; else `MSG_GET` + memcpy. NO `concurrency`
    arg — sensible internal default applied.
  Result `i` matches `items[i]`. Each `dest` MUST outlive the call.
- `get_many_direct(items: &mut [GetManyItem]) → Vec<Result<Option<usize>>>` —
  **F-DIRECT-MANY: the EN-DIRECT batch read.** Same dest-based shape as
  `get_many_into`, but each item whose requested length is ≥ 64 KiB is read
  STRAIGHT from an extent node (`MSG_GET_REDIRECT` descriptor →
  `read_extent_value_direct`), taking the PS off the large-value DATA path (a
  cross-host throughput win — the PS NIC egress leaves the read path). Sub-64 KiB
  items stay on the plain proxy `get_range` path, so MIXED-SIZE batches route per
  item. Per item, ANY direct-read failure falls back to the proxy — so it
  degrades gracefully where ENs aren't client-reachable (one redirect RTT +
  fallback). Because that reachability is TOPOLOGY-dependent, the DECISION to
  call this vs `get_many_into` is a flag OWNED BY THE FRONTEND (fuse
  `--direct-read`, python `BatchClient(direct=…)` / `autumn.Fs.connect(direct_read=…)` /
  kvcache `direct_read` / vLLM-loader `direct_read`). The frontends now DEFAULT
  it ON (2026-07-09, user directive) — safe because it's size-gated (only
  ≥ 64 KiB reads redirect) AND the proxy fallback is authoritative; the first
  fallback logs ONE `WARN` (`DIRECT_FALLBACK_WARNED`) so a wrong topology (ENs
  on a PS-only subnet) surfaces without per-read spam. Disable per-frontend on
  such a topology to skip the wasted redirect RTT. Shares the replica-failover
  loop with `get_direct` (`read_redirect_replicas`). One extra copy vs `get_many_into`'s
  recv-into-`dest`: the direct read lands in a read_loop pooled buffer
  (`call_into_pooled`) then memcpys into `dest` — the pooled recv (not
  `call_into_dest`) is deliberate, because the direct read carries a 3 s timeout
  + replica failover that `call_into_dest`'s cancel-safety contract forbids. The
  copy is the price of failover-safety on the bypass path. Each `dest` MUST
  outlive the call.
- `delete_many(keys: &[&[u8]]) → Vec<Result<()>>` /
  `head_many(keys: &[&[u8]]) → Vec<Result<KeyMeta>>` — **F237 batched
  delete / metadata.** Client-side fan-out (no server `MSG_BATCH_*`),
  no ZC (delete/head are tiny). Sensible internal concurrency default
  (`BATCH_PUT_DEFAULT_CONCURRENCY` / `BATCH_GET_DEFAULT_CONCURRENCY`).
  `head_many` returns `found=false` for a missing key (not `Err`).

**Internal-only (pub(crate)):**
- `batch_put` — the server-batched RPC layer for writes. Reached
  through `put_many`'s auto-routing; callers should not invoke
  directly. Kept as `pub(crate)` so unit tests + the delegation
  layer share one implementation.

**Removed in the API consolidation (2026-06-08):**
- `put_with_ttl` — TTL is now a tuple field on `put_many`. For a
  single-key TTL put, call `put_many(&[(key, value,
  ClusterClient::ttl_to_expires_at(ttl_secs))])`.
- `concurrency` parameter on `put_many` / `get_many_into` /
  `delete_many` / `head_many` — was a leaky implementation detail.
  SDK now manages partition pacing + per-op fan-out concurrency
  internally.
- `delete_many(keys: &[&[u8]]) → Vec<Result<()>>` / `head_many(keys: &[&[u8]]) →
  Vec<Result<KeyMeta>>` — **F237 batched delete / metadata.** Same client-side
  fan-out as `get_many_into`/`put_many` (no server `MSG_BATCH_*`, `buffered` over
  per-partition conns); NO ZC (delete/head are tiny — `MSG_DELETE`/`MSG_HEAD`).
  `head_many` returns `found=false` for a missing key (not `Err`). delete reuses
  the write concurrency cap, head the read one.

**Batch fan-out foundation (F245) — `fan_out` / `fan_out_collect`.** All four batch
APIs above are thin wrappers over ONE streaming primitive (module-level, pub):
- `fan_out(futs, concurrency) -> impl Stream<Item=(usize, Fut::Output)>` — drives
  `futs` with a bounded sliding window (`buffer_unordered`) and yields
  `(input_index, output)` as each future COMPLETES (completion order). This is the
  same "fire N, reap as they land" SQ/CQ shape the EN/PS server loops use, lifted
  to the client.
- `fan_out_collect(futs, concurrency) -> Vec<Output>` — collects `fan_out` back
  into INPUT order. `get_many_into` / `put_many` / `delete_many` / `head_many` each
  build their per-item futures and call this; the per-item logic (ZC decision, copy
  into dest, etc.) is all that differs.
- **Streaming consumers** drive `fan_out` directly and act per completion — e.g.
  the io_uring daemon would push one CQE per finished SQE with no head-of-line wait
  on the rest of the batch (the reason a batch-COLLECT primitive like
  `get_many_into` is the wrong fit there; see F244-C). This is the seam that lets
  the daemon eventually share the client's fan-out without changing its streaming
  completion model.

**"ucx ⟹ zerocopy" + `UCX_ZC_READ_MIN_BYTES` + `zc_worthwhile` (F216-E/F219/F234/F235).**
The SDK exposes both the regular (`get`/`put`) and zero-copy (`get_into`/`put_zc`)
ops; the SELECTION is encapsulated in the single helper
`autumn_client::zc_worthwhile(value_size) -> bool` — the ONE source of truth that
perf-check, the python `BatchClient`, and `get_many_into` all call (F234 found 3
hand-rolled copies had drifted apart; F235 collapsed them here):

**One symmetric rule — engage ZC iff `value_size >= UCX_ZC_READ_MIN_BYTES` (64 KiB),
for BOTH reads and writes AND BOTH transports.** This mirrors the PS-side recv
gates — `UCX_ZC_READ_MIN_BYTES` (client) + `AUTUMN_PS_ZC_RECV_MIN_BYTES` (PS), both
64 KiB. Below 64 KiB the per-op registered/pooled-recv machinery (`regpool_acquire`
+ `UCP_OP_ATTR_FIELD_MEMH` + 2-stage recv) costs more than the copy it saves (small
UCX read regresses ~18% at 4 KiB) AND the PS recv side doesn't ZC anyway, so
end-to-end ZC simply doesn't engage below 64 KiB. At/above it ZC wins on both
transports — UCX RDMA-into-dest / registered-send (read 2.3× at 8 MiB, the R4
fully-zero-copy path); TCP recv-into-dest / pooled-recv drops the rkyv wrap + the
owned-`Vec` alloc (F219).

**History (do not reintroduce):**
- Pre-F219: "UCX-only ZC; on TCP both ops use the regular path." F219 added
  TCP-large ZC (both directions).
- F234 fixed two drifts but kept an asymmetric WRITE rule `is_ucx || large` (ZC on
  small UCX writes).
- **F235 made WRITE symmetric too** (dropped `is_ucx ||`): a small UCX `put_zc`
  only saved client-side allocs while the PS still FrameDecoder-copied it
  (< `AUTUMN_PS_ZC_RECV_MIN_BYTES`), i.e. NOT real end-to-end ZC. The "4 KiB write
  2.6×" figure was never in any committed bench — treated as unverified and dropped
  in favour of the simple symmetric rule.

There is no `--zc` / `zc=` flag — call `zc_worthwhile(size)`. The const + helper
live in `crates/client/src/lib.rs` so the CLI (`autumn-client`) and the python
extension share one source of truth.

**Maintenance operations:**
- `split(part_id)` — trigger partition split
- `compact(part_id)` — trigger compaction
- `gc(part_id)` — trigger automatic GC
- `force_gc(part_id, extent_ids)` — force GC on specific extents
- `flush(part_id)` — trigger memtable flush
- `merge_partitions(survivor, victim)` — F183 partition merge (CLI orchestration)
- `policy_candidates() → Vec<PolicyCandidate>` — F183 advisory engine output

**Per-call timeout (F184):**
- `set_rpc_timeout(Duration)` — set the per-call timeout for PS-bound RPCs
- `clear_rpc_timeout()` — restore default (wait forever)
- `rpc_timeout() → Option<Duration>` — read current setting

Default is **None (wait forever)**, preserving pre-F184 behavior. When set,
every `ps_call` (i.e. `put`/`get`/`delete`/`head`/`range`/`stream_put`/
`merge_partitions`'s FLUSH and downstream PS calls / F129 PutChunk / etc.)
is raced against `compio::time::sleep(timeout)`. Expiry surfaces as
`AutumnError::ConnectionError` so the caller's existing
retry-on-routing-miss path triggers a `refresh_regions` + one retry.

**Why this exists:** the partition-server may drop a partition's `req_rx`
mid-call — region_sync_loop reload after merge, F140 split's drain,
graceful shutdown. The drop closes the per-request response oneshot
**without** closing the underlying TCP connection (other partitions on
the same PS still use it). autumn-rpc's F121 closed-state flag fires on
TCP close, not on req_rx drop, so without a per-call timeout `cluster.put().await`
hangs forever.

**Recommended values:**
- Production read-heavy loads: 2-5 s (slow-disk fsync coalescer worst case is ~100 ms; 3-replica fanout ~10 ms).
- Tests that drive split/merge: 2 s (the merge-reload window is ~1-2 s).
- Bulk uploads via `stream_put` of multi-GiB blobs: clear the timeout entirely or set 30+ s, since the call itself is long-running.

**NOT timed out**: `mgr_call` and `mgr_call_retry`. Manager calls already have
round-robin failover via `rotate_manager` on connection error.

**Low-level (for CLI/benchmarks):**
- `mgr_call(msg_type, payload)` — raw manager RPC
- `mgr_call_retry(msg_type, payload, max_retries)` — with round-robin retry
- `ps_call(ps_addr, msg_type, payload)` — raw PS RPC
- `get_ps_client(ps_addr)` — get/create PS connection
- `resolve_key(key) → (part_id, ps_addr)` — route key to partition
- `resolve_part_id(part_id) → ps_addr` — resolve partition to PS
- `all_partitions() → Vec<(part_id, ps_addr)>` — list all partitions

### Error Types

- `AutumnError::NotFound` — key not found
- `AutumnError::InvalidArgument(msg)` — bad request
- `AutumnError::PreconditionFailed(msg)` — e.g. split with overlap
- `AutumnError::ServerError(msg)` — internal server error
- `AutumnError::RoutingError(msg)` — cannot route key
- `AutumnError::ConnectionError(msg)` — RPC connection failure

### Result Types

- `KeyMeta { found, value_length }` — from head()
- `RangeResult { entries: Vec<RangeEntry>, has_more }` — from range()
- `RangeEntry { key, value }` — re-exported from partition_rpc

## Architecture

- Single-threaded (Rc/RefCell) — designed for compio single-thread runtime
- Manager connections: round-robin failover on error, auto-reconnect
- PS connections: cached per-address, dropped on error, recreated on next call
- Routing: `GetRegions` cached at connect, refresh on routing miss (binary search)

### F099-K per-partition routing (SDK side)

After F099-K, each partition binds its own TCP listener at `base_port + ord`.
The PS-level address from `register_ps` (cached in `ps_details[ps_id].address`)
only owns the FIRST partition opened on that PS — sending a RangeReq /
PutReq / GetReq for any other partition to that address gets back
`CODE_NOT_FOUND` from the receiving partition_loop's mis-routed-frame
fast path.

Every cross-partition / per-partition call site MUST resolve via
`part_addrs[part_id]` first, falling back to `ps_details[ps_id].address`
only when the partition is not yet registered (transient post-split
state):

| Call site | Resolver |
|-----------|----------|
| `lookup_key` (get/put/del/head/stream_put) | `part_addrs.get(part_id).or_else(ps_details[ps_id])` |
| `resolve_part_id` (split/compact/gc/flush) | same |
| `all_partitions` (CLI listing) | same |
| `range` (cross-partition scan) | same — F112 fixed this; was using ps_details only |

`range` additionally surfaces per-partition errors instead of `continue`:
silently dropping one partition's response would return a half-empty
`Ok(RangeResult)`, which is indistinguishable from a true empty result.

### TiKV-style `region_epoch` + CockroachDB-style resume cursor (2026-05-16)

The SDK stamps a `region_epoch: u64` (cached from `MgrRegionInfo.region_epoch`,
bumped by the manager on every `rg` rewrite — split / merge) on every
hot-path request. The PS rejects with `FailedPrecondition` when the
stamped epoch doesn't match its current epoch. The existing
`call_ps_for_key` `Err`-arm refresh path picks it up: drop conn,
`refresh_regions`, retry.

### F225 — error classification: terminal vs transient (admin ops fail fast)

`ps_call` no longer stringifies the PS error: on a frame-level
`RpcError::Status{code,msg}` it returns the typed `AutumnError` via
`rpc_status_to_error` (preserved inside `anyhow`, downcastable; signature
unchanged so no caller breaks). This lets the routing-retry loops branch on
error kind instead of string-matching.

- **`call_ps_for_part`** (admin ops: split/compact/gc/flush) short-circuits
  `PreconditionFailed | InvalidArgument | ValueTooLarge` — these are
  DETERMINISTIC (e.g. "partition has overlapping keys", "needs >= 2 keys") and
  refreshing routing can't fix them, so it returns immediately instead of
  burning `MAX_PS_REFRESHES` (10, ~9 s). Admin ops are region_epoch-EXEMPT, so a
  FailedPrecondition here is never the stale-epoch case. Transient errors
  (NotFound from a not-yet-registered post-split partition / ConnectionError /
  routing miss) still refresh + retry — that's the F212-fix-2 window this loop
  exists for.
- **`call_ps_for_key`** (data ops) is deliberately UNCHANGED: there a
  FailedPrecondition is (often) a stale `region_epoch`, which MUST refresh +
  retry. Do not add the short-circuit to the data path.

Two error channels remain distinct: `rpc_status_to_error` maps frame-level
`StatusCode` (handler returned `Err((StatusCode, msg))`); `code_to_error` maps
application-level `CODE_*` carried in a successful response body.

**Wire surface** (post-this — backward-incompat with prior etcd
`regions/` blob; `cluster.sh reset` for the migration):

- `MgrRegionInfo` gains `region_epoch: u64`.
- `PutReq` / `GetReq` / `DeleteReq` / `HeadReq` / `RangeReq` /
  `StreamPutReq` gain `region_epoch: u64`. Admin ops
  (`MaintenanceReq`, `SplitPartReq`, `MergePartReq`) are exempt — the
  operator is the authoritative caller.
- `RangeResp` gains `cur_end_key: Vec<u8>` — the PS's authoritative
  `rg.end_key`; the SDK uses it as the resume cursor.
- `CODE_REGION_EPOCH_STALE = 8` is reserved for future inline use
  (today the PS surfaces this via `StatusCode::FailedPrecondition`
  at the frame level).

**SDK plumbing**:

- `lookup_epoch_for_part(part_id)` — public helper. Used by FUSE /
  ioring / CLI / SDK internals to stamp the cached epoch when
  manually assembling a Req struct.
- `call_ps_for_key`'s build closure shape is now `Fn(u64, u64) -> Bytes`
  (`(part_id, region_epoch)`). On retry it re-invokes with the
  freshly-cached epoch so the second attempt reflects the post-
  refresh routing.

**`range()` resume cursor**:

The pre-this snapshot-based scan was replaced with a cursor-driven
loop: each successful `RangeResp` returns `cur_end_key` which the SDK
uses as the start_key for the next iteration. A split that happens
mid-scan auto-resolves on the next `resolve_key` against the
(possibly refreshed) cache. On epoch-stale error, results from
already-successful partitions are KEPT; only the failing partition
gets re-resolved + re-scanned. Up to `MAX_RANGE_REFRESHES` (3) refresh
cycles per call, `MAX_RANGE_ITERATIONS` (10_000) iteration cap as a
defensive bound against pathological churn.

**Tests / benches**: stamp `region_epoch: 0` — `0` is the wire-level
"skip check" sentinel. Production callers always stamp non-zero from
the cache.

`0` reservation has one practical implication: a `lookup_epoch_for_part`
on a partition that's not in the cache returns `0` (skip check) rather
than failing — the resolver path handles the "no such partition" case
already; epoch is opportunistic.

## Dependencies

- `autumn-rpc`: RPC client + wire codec (partition_rpc, manager_rpc)
- `compio`: async runtime (time::sleep for retry backoff)
- `anyhow`, `bytes`: error handling + byte buffers
