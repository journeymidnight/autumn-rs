# autumn-client Crate Guide

## Purpose

Client SDK library for interacting with an autumn-rs cluster. Provides high-level ergonomic API for KV operations, routing, and maintenance.

## Public API

### ClusterClient

Main entry point. Connect via `ClusterClient::connect("addr1,addr2")`.

**Data operations:**
- `put(key, value, must_sync)` — write a key-value pair
- `put_with_ttl(key, value, must_sync, ttl_secs)` — write with TTL (seconds)
- `get(key) → Option<Vec<u8>>` — read, returns None if not found
- `get_into(key, dest: &mut [u8], reg: Option<&autumn_rpc::RegisteredMem>) → Option<usize>` —
  **F216-E zero-copy read.** Reads the value straight into `dest` (no Vec) via
  `MSG_GET_ZC` + `RpcClient::call_into_dest`; returns `Some(value_len)`
  (`dest[..value_len]` filled) or `None` if not found. `reg=Some(&RegisteredMem)`
  covering `dest` → UCX RDMA into the registered dest (zero-copy); `None` → one
  copy off the wire (TCP / unregistered). Same routing + epoch-stale refresh +
  RPC-retry shape as `call_ps_for_key`. Caller sizes `dest` (e.g. from `head`).
  No per-call timeout — `dest` MUST outlive the call (cancel-safety, see
  autumn-rpc CLAUDE `call_into_dest`).
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
- `get_many_into(items: &mut [(key, dest, reg)]) → Vec<Result<Option<usize>>>` —
  **F235 batched zero-copy reads.** Pure client-side fan-out (no server
  `MSG_BATCH_GET`): each item is read concurrently (sliding window of
  `BATCH_GET_DEFAULT_CONCURRENCY` = 32) over the per-partition multiplexed
  connections, amortising per-call await latency + letting the writer_task batch
  syscalls. Per item the ZC choice is `zc_worthwhile(dest.len())`: ≥ 64 KiB →
  `get_into` / `MSG_GET_ZC`, else `get` / `MSG_GET` + one copy into `dest`. Result
  `i` matches `items[i]` (`Ok(Some(n))` = value len, `Ok(None)` = miss, `Err` =
  that item failed; others still ran). Each `dest` MUST outlive the call.
- `put_many(items: &[(key, Bytes)]) → Vec<Result<()>>` — **F236 batched zero-copy
  writes** (write mirror of `get_many_into`). Pure client-side fan-out (no server
  `MSG_BATCH_PUT`), `buffered(BATCH_PUT_DEFAULT_CONCURRENCY` = 32) over the
  per-partition multiplexed connections. Per item `zc_worthwhile(value.len())`:
  ≥ 64 KiB → `put_zc` / `MSG_PUT_ZC` (value sent as its own iovec from the `Bytes`
  backing memory, no copy; RDMA on UCX when registered), else `put` / `MSG_PUT`.
  Result `i` matches `items[i]` (`Ok(())` = stored, `Err` = that item failed).

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
