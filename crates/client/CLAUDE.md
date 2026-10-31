# autumn-client Crate Guide

## Direct-read stale-descriptor heal

`read_descriptor_with_stale_heal` wraps `read_from_descriptor` at all three
descriptor call sites (`get_direct`, `get_range_direct_into`,
`apply_redirect_desc`). The PS's `extent_info_cache` is pull-invalidated — the
manager's EC layout flip writes etcd and notifies nobody — so a PS that served
an extent before its EC conversion hands out a stale REPLICATED descriptor
(3 `.dat` addrs, old eversion) until something makes it refetch; every EN then
refuses the direct read in milliseconds and the read silently takes the slow
proxy path. The heal classifies that shape (`DirectReadOutcome::StaleLayout`:
all replicas refused fast with an eversion-mismatch / payload-not-here text,
no timeout), runs a 1-byte proxy probe (the PS's own read path refetches on
`EversionStale` as a side effect), re-sends `MSG_GET_REDIRECT` once, and reads
direct again. Any failure returns `None` and the caller's proxy fallback runs
as before — the redirect stays an optimization, never a correctness dependency.

## Batched direct-read descriptor concurrency

`get_many_direct` submits partition descriptor requests through `fan_out_collect`
with `--batch-get-concurrency`, instead of waiting for each partition in turn.
The original item indices accompany each result. Per-partition descriptor failures
retain the existing per-item fallback; data fan-out starts after collection.

## Purpose

Client SDK for an autumn-rs cluster. Provides the ergonomic `ClusterClient` API for
KV operations, batching, namespace binding, region routing, zero-copy, and maintenance.

## Namespace binding

Every `ClusterClient` carries a `NamespaceBinding` that maps user keys onto wire keys
BEFORE routing, so a client can only touch its own `{scope}/` keyspace. A `scope` is a
whole namespace (`fs`, `gallery`) or an in-namespace
sub-prefix an app owns (`mem/agent7`). See `docs/key_namespace_split_design.md` §8.

Constructors:
- **`connect(mgr, scope)`** — the entry point EVERY data-plane writer MUST use.
  **Prepend-only**: a scoped client ALWAYS prepends `{scope}/` to the user key and
  strips it back off returned range keys, so it **cannot** touch anything outside its
  keyspace — scope is locked by construction, not merely checked (no assert/validate
  mode). Each `/`-delimited `scope` segment must match `[a-z0-9._-]+`; the FIRST is the
  namespace (Layer-A checks it). Built-in key builders (`fuse/key.rs`,
  `kvc/_keys.py`) emit keys RELATIVE to `{scope}/` (the binding owns the prefix) so
  there is no double-prefix.
- **`connect_raw(mgr)`** — unscoped (`Raw` binding, no client prefixing) for
  cross-namespace inspection/migration, and tests. NOT for data writers. The PS
  still enforces Layer-A/B — `raw` only bypasses the CLIENT prefixing. It
  declares the CLIENT role, so split / compact / gc / flush / merge,
  namespace / principal admin and the op ledger are refused PermissionDenied;
  operator tooling uses **`connect_admin`** (ADMIN role, exact wire), and the
  extent node's startup calls use `connect_peer`.
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
  (`put`/`get`/`delete`/`head`/`compare_put`/`get_range`/`get_range_into`/`get_direct`; batch
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
with a STABLE, recycled address (UCX runtime: `ucp_mem_map`-registered at slab creation;
TCP runtime: plain recycler, no per-op alloc/zero). Write side:
`alloc_value_buf(len)` → fill `as_mut_slice()` → `truncate(n)` → `put` (the
`ValueBuf` itself, or `freeze()`, a `RegBytes`: the bytes plus the slab's memh,
re-exported as `autumn_client::RegBytes`). `IntoValue` produces a `RegBytes`, and
`put_bound` / `put_bulk_opts` and rpc's `call_vectored_bulk` carry it, so over UCX a
value of 64 KiB or more goes out as a frame value with the slab's memh and UCX does
no lookup (a smaller one is copied inline, as before); `Bytes` / `Vec<u8>` / slices
carry none (nor does `compare_put`, which takes `Bytes`; `put_many` takes any
`Clone + Into<RegBytes>`, so lent or pool values keep theirs): over UCX such a
value is copied into a pool buffer at the send (autumn-transport, "UCX is never
handed memory to register itself"). The test
(stream `prepared_append.rs` `a_bulk_call_with_a_pool_buffer_carries_its_memh`)
drives rpc's `call_vectored_bulk`, not `put`; the SDK hops above it are typed. Read side: `get_pooled` hands the recv'd
pool buffer back as a `ValueBuf` (zero SDK-side copies); read in place or
`freeze().into_bytes()` for a framework sink (HTTP body). Dropping
either returns the slab to the pool. A frozen `RegBytes` may be dropped on any
thread: its range goes back to its chunk under the chunk's lock. Writing into a `ValueBuf` is
the zero-copy way: on UCX memory without a memh (a fresh allocation, a caller's
`Bytes`) is copied into a pool buffer at the send, one memcpy per value; an
explicit registration API for long-lived caller memory is data-path redesign
step 3c.

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

**Writes take `WriteOptions { ttl: Option<Duration>, lease: WriteLease }`** (by value;
`Copy`; default = no TTL, unfenced; `WriteOptions::fenced(lease)`, `.with_ttl(d)`,
`.with_lease(l)`). Five write methods, each taking every option that means something
for it: `put`, `put_many`, `compare_put` (TTL + lease), `delete`, `delete_many` (lease
only; a TTL is ignored). The TTL is rounded up to whole seconds and sent as the wire's
absolute `expires_at`. A lease stamps `(inode_hint, lease_epoch)`; the PS refuses a
write below the inode's floor with `AutumnError::Fenced` and raises the floor on a
higher one — the write-lease holders (fuse, the fs core, the S3 gateway through it)
pass theirs, plain KV callers the default. Tests:
`crates/manager/tests/write_options.rs::ttl_and_lease_reach_every_write` (each option
through each method, small and large values; ablating any one hand-off is red).

- `put(key, value: impl IntoValue, opts)` — write a key-value pair. Always durable:
  there is no sync flag (the extent-node fsync coalescer makes every write durable
  before it ACKs). `IntoValue`: `Bytes` / `Vec<u8>` / `&Bytes` move in without a
  copy, `&[u8]` / `&[u8; N]` / `&Vec<u8>` are copied. A value ≥ 64 KiB
  (`bulk_worthwhile`) goes zero-copy as `MSG_PUT_BULK` (below); a smaller one inline
  as `MSG_PUT`.
- `get(key) → Option<Vec<u8>>` / `get_range(key, offset, length)` — read, `None` if not
  found. Served by the same pooled `MSG_GET_BULK` core as `get_pooled`; the returned `Vec`
  is the one application copy. Retry classification is `call_ps_for_key`'s: NotFound is a miss,
  PermissionDenied/NamespaceUnknown are terminal, everything else refreshes and retries.
- `get_pooled(key) → Option<ValueBuf>` / `get_range_pooled(key, offset, length)` —
  **bulk read, ZERO SDK-side copies** — the CORE every bulk read routes through. The value
  arrives in a read_loop-owned RegPool buffer (`MSG_GET_BULK` + `call_into_pooled`; the UCX
  Stream unpack or TCP kernel copy is its only copy) and is handed
  straight back. The address-UNCONSTRAINED shape ("I just want the value"): autumnfs
  cat/get, gallery serving, any consumer without a fixed destination. Any value size.
  Honors `rpc_timeout` (pooled recv is cancel-safe).
- `get_into(key, dest: &mut [u8]) → Option<usize>` — **bulk read, one copy**:
  `get_range_pooled` + ONE memcpy into `dest`. For address-CONSTRAINED consumers only
  (sglang pages / torch tensors / python buffers / fuse assembly). Returns
  `Some(value_len)` (`dest[..value_len.min(dest.len())]` filled; longer values
  TRUNCATED to fit — same contract as `get_many_into`/`get_many_direct`) or `None`.
  A borrowed `&mut [u8]` cannot be lent to a receive (dropping the future would end
  the borrow while the receive writes), hence the copy.
- `get_range_into_raw(key, ptr, len)` / `get_many_into_raw(items: &[RawGetItem])` —
  **unsafe**, whole values received straight into caller memory over
  `RpcClient::call_into_dest` (autumn-rpc CLAUDE.md "Receiving into caller memory"):
  no copy out of a pool buffer. Contract: the memory stays valid and untouched until
  the future completes, and the future runs to completion. For callers that know the
  value's size and own the memory for the call — the python `BatchClient.get_into`
  (kvcache pages); everything else uses the pooled reads. Shares routing, retries and
  refusal classification with `get_range_pooled` (`get_bulk_bound`); each retry lends
  the memory again only after the previous attempt returned it. Over UCX, against a
  cluster at `WIRE_VERSION_WITH_GET_AM` or later, an attempt is `get_am_attempt`:
  register the memory under a fresh AM tag, send `MSG_GET_AM`, and for an OK reply
  wait (bounded) for the value's arrival; UCX moves it by rendezvous with no receive
  copy. A value too long for the memory falls back to `MSG_GET_BULK` in the same
  attempt. Not handled: a PS still at wire 75 in a cluster whose manager is at 76
  refuses `MSG_GET_AM` (PermissionDenied, terminal) — upgrades stop the whole
  cluster; and a PS dying in the middle of the value's rendezvous may leave the
  attempt waiting for UCX (transport CLAUDE.md, "Large values as active messages"). Small batches go
  through `get_many` + copy as in `get_many_into`.
- The large-value `put` is the **zero-copy write**: NO client-side copy AND no value
  crc scan via `MSG_PUT_BULK` + `RpcClient::call_vectored_bulk` (v28: `[meta][key]` is
  the CRC'd ctrl, the value rides after the crc as its own iovec from `value`'s backing
  memory; UCX zero-copy via rcache when that memory is `ucp_mem_map`-registered). The
  frame decoder hands the PS ctrl and value pre-split (zero-copy).
- `delete(key, opts)` — delete a key (`Ok` even if it did not exist).
- `head(key) → KeyMeta` — metadata (`found`, `value_length`).
- `range(prefix, start, limit) → RangeResult` — prefix scan (resume cursor below).
- `range_until(prefix, start, end, limit) → RangeResult` — the same scan stopping
  before `end` (exclusive). `end` is also the cap `range_bound` stops walking
  partitions at, so a page that is not full in its own partition does not read
  the partitions after `end` — which a plain `range` does until it fills the
  page or reaches the namespace cap, and which costs a full tombstone walk when
  those partitions were just bulk-deleted (`autumn-client perf-clean` pages per
  partition this way). Keys at or past `end` are dropped and then `has_more` is
  false.

**Stream operations (large values):**
- `put_stream_begin(key, expires_at) → PutStreamHandle` — streaming writer; the handle
  writes striped chunks. (`expires_at` currently ignored.)
- `get_stream(key, chunk_size_hint) → Option<GetStream>` — streaming reader; auto-detects
  striped vs inline (a plain `put` value yields whole in one `next_chunk()`), `None` if the
  key doesn't exist.
- `delete_stream(key)` — delete a streamed value.

All three route through the plain `put`/`get`/`delete` (see the binding invariant above).

Each upload draws a random 16-byte upload id; its chunk keys and the 45-byte meta
both carry it, so an uncommitted or aborted overwrite never touches the chunks the
current meta names, and two uploaders never share a chunk. Invariant: a published
meta always has all its chunks — `commit` swaps the meta, then deletes the replaced
upload's chunks; `delete_stream` deletes the meta, then the chunks. A reader whose
upload was replaced under it gets `PreconditionFailed` (it re-reads the meta on a
missing chunk), never the new bytes. Two commits racing on one key can both read the
same predecessor; the earlier one's chunks are then orphaned (no meta names them). The v1
meta (no upload id) is refused by `get_stream`; overwrite or `delete` it and its old
chunks are left behind. Test: `system_putstream::a_stream_overwrite_is_invisible_until_commit`.

## Public API — batched operations

All batch APIs are thin wrappers over the `fan_out` / `fan_out_collect` primitive; per-op
bulk decisions go through `bulk_worthwhile`. No `concurrency` arg — internal defaults apply.

- `put_many(items: &[(key, Bytes)], opts) → Vec<Result<()>>` — **public batched
  write.** `opts` (TTL, lease) applies to every item. SDK groups by owning
  partition. An item ≥ 64 KiB leaves the batch entirely and goes per-op
  `MSG_PUT_BULK`, fanned out CONCURRENTLY via
  `fan_out_collect(batch_put_concurrency())`, one serial chain per key; a key
  that also has a < 64 KiB item in the same batch brings that item along, so
  the key's input order holds. What remains is sent as ONE frame
  per partition, and which frame depends on the GROUP's total value bytes, not on any
  single item's: ≥ 64 KiB → `MSG_BATCH_PUT_BULK` (keys and `value_len`s in ctrl, the
  values as their own iovecs in the raw tail — `Bytes::clone` is a refcount bump, so no
  value byte is copied on the way to `writev`); below that → `MSG_BATCH_PUT` with the
  values inline in the rkyv payload, which keeps them under the frame CRC. The
  threshold is on the sum because what earns a transfer its own iovec is how much the
  FRAME carries, and batching is what makes a frame big. Result `i` matches `items[i]`.
- `get_many(keys: &[&[u8]]) → Vec<Result<Option<Vec<u8>>>>` — **simpler batched read.**
  SDK allocates a `Vec<u8>` per value; one `MSG_BATCH_GET_BULK` per partition, whose
  reply carries every value in the frame's raw tail with only per-key status and length
  in ctrl (the SDK slices the tail and copies each value out once, which this API's
  return type requires anyway). Use when you don't know value sizes or don't want to
  pre-alloc dests. The older inline `MSG_BATCH_GET` put the values inside the rkyv
  response, which cost four server-side copies of every value byte plus a CRC pass over
  all of them, and was served through `partition_loop` — so a batched read queued behind
  the single-writer group-commit actor. Measured on a 41-query search workload over a
  5164-chunk corpus: -7.5% end-to-end wall clock, on loopback TCP, where `get_many` is
  only part of the work.
- `get_many_into(items: &mut [GetManyItem]) → Vec<Result<Option<usize>>>` — **bulk batched
  read.** Use when values ≥ 64 KiB AND you have caller-owned dest buffers (sglang pages /
  torch tensors). Each `GetManyItem` = `{key, offset, length, dest}`. The bulk recv lands
  in a read_loop-owned RegPool buffer (UCX Stream unpack; TCP owned read), then ONE
  memcpy into `dest` — `dest` needs no registration and no special
  lifetime. Auto-routes: HOMOGENEOUS small whole-value batch (every item `offset==0`,
  `length==0`, `dest.len() < 64 KiB`) → delegates to `get_many` + memcpy into each `dest`;
  MIXED / range / large-bulk → per-op fan-out through `get_range_into` (`MSG_GET_BULK`
  pooled recv + memcpy) at any size. Result `i` matches `items[i]`.
- `get_many_direct(items: &mut [GetManyItem]) → Vec<Result<Option<usize>>>` — **EN-DIRECT
  batch read.** Same dest shape as `get_many_into`, but each item with length ≥ 64 KiB is
  read STRAIGHT from an extent node (`MSG_GET_REDIRECT` descriptor →
  `read_extent_value_direct`), taking the PS off the large-value DATA path (cross-host
  throughput win). Sub-64 KiB items stay on the proxy `get_range` path, so MIXED batches
  route per item. **Per item, any direct-read failure except an EN refusing the
  client's credential (returned as `PermissionDenied`, see "Direct reads under
  authz") falls back to the proxy** — degrades
  gracefully where ENs aren't client-reachable. That includes a PS DECLINE: the
  batched descriptor call answers `CODE_PRECONDITION` for the individual items
  it cannot serve directly (`redirect_item_action` → `Proxy`), and only those
  items pay a proxy read. Declining the whole batch instead was measured 3.5x
  slower than never trying direct reads, because every key then paid its own
  redirect round trip on top of the proxy read. The call-vs-`get_many_into` DECISION is
  TOPOLOGY-dependent, so it's a frontend-owned flag (fuse `--direct-read`, python
  `BatchClient(direct=…)` / `autumn.Fs.connect(direct_read=…)` / kvcache / vLLM-loader),
  DEFAULT ON — safe because size-gated AND proxy fallback is authoritative. First fallback
  logs ONE `WARN` (`DIRECT_FALLBACK_WARNED`). Shares the replica-failover loop with
  `get_direct` (`read_redirect_replicas`). Same recv shape as the proxy path (pooled
  recv + one memcpy into `dest`; cancel-safe → the size-scaled timeout + replica
  failover are safe); the win over `get_many_into` is ROUTING (PS NIC egress leaves the
  large-value data path), not copy count.
- `delete_many(keys, opts) → Vec<Result<()>>` — one `MSG_BATCH_DELETE` (0x5C) per owning
  partition, same grouping as `put_many`. No bulk variant: a delete carries no value,
  so there is no tail to keep out of the rkyv payload. It was a client-side fan-out of
  single-key deletes until v36; that looked like N concurrent requests and was not,
  because a client holds ONE multiplexed connection per partition and keys that scan
  together land in the same partition. The gain is EXTENT-NODE load, not latency — an
  append is a replicated write, so fewer appends for the same keys is less work asked
  of the extent nodes.
  A fenced key (`opts.lease`) returns `AutumnError::Fenced` in its own slot.
  ⚠️ Its stale-routing fallback must use `delete_opts` on the ALREADY-BOUND key, never
  the public `delete`: `delete` binds again, a scoped binding then targets
  `{prefix}{prefix}{key}`, and deleting a key that does not exist reports `Ok` — the
  batch would claim success having removed nothing.
- `head_many(keys) → Vec<Result<KeyMeta>>` — batched metadata, still client-side
  fan-out at `--batch-get-concurrency`. Returns `found=false` for a missing key
  (not `Err`).

**Internal-only (`pub(crate)`):** `batch_put` / `batch_delete` — the server-batched RPC layer for writes,
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

The SDK picks inline or zero-copy itself on writes (`put`, `put_many`) and offers both
on reads (`get` / `get_into`); the SELECTION is encapsulated in `autumn_client::bulk_worthwhile(value_size) -> bool` — **the
ONE source of truth** that perf-check, the python `BatchClient`, and `get_many_into` all
call.

**One symmetric rule — engage bulk iff `value_size >= BULK_MIN_BYTES` (64 KiB), for
BOTH reads and writes AND BOTH transports.** This is the INTENT gate (which msg_type/API
to use, from the size the sender knows/expects); the two RECEIVERS re-decide their recv
strategy on the ACTUAL size with the same 64 KiB — the client read_loop for GET-bulk
responses (`TCP_RECV_INTO_POOLED_MIN_BYTES`), the PS ps-conn loop for PUT_BULK requests
(`--bulk-recv-min-bytes`) — because a reply/request can legitimately be smaller
than the intent predicted (error/NotFound = 0-length value).
All four gates (this one + the two recv gates + the PS `handle_get_redirect` 64 KiB) are
deliberately one value; the dispatch table lives in autumn-rpc CLAUDE.md "read_loop
dispatch (4-way)". Below 64 KiB the per-op registered/pooled-recv machinery costs more
than the copy it saves AND the recv side doesn't bulk anyway, so e2e bulk doesn't engage;
at/above it bulk wins on both transports (UCX Stream unpack straight into the pool
slab / registered-send; TCP pooled recv dropping the rkyv wrap + FrameDecoder accumulation +
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
- `runtime_config_get()` / `runtime_config_set(key, Option<value>, set_by)` — the
  operator's runtime parameter overrides (`autumn-op config`), leader-routed.
- `cluster_overview()` / `list_node_states()` — the overview and the EN states,
  leader-routed (rotate on NOT_LEADER, like `cluster_df` / `auto_policy_get`);
  `autumn-op info --file-sd` builds the Prometheus target list from them.
- `ps_call(ps_addr, msg_type, payload)` — raw PS RPC.
- `get_ps_client(ps_addr)` — get/create PS connection. Both this cache and
  `mgr_client()`'s skip a client whose `is_closed()` is true and open a fresh one,
  so a connection the rpc keepalive judged silent (autumn-rpc CLAUDE.md "Dead-peer
  detection") is replaced before the next call rather than failing it first.
  One connection per PS address: a miss takes the address's connect turn
  (`ps_connecting`) and checks the cache again, so concurrent first requests
  share the first connect instead of each opening and handshaking their own
  (`connection_tests::concurrent_first_requests_open_one_connection`: 1 accept,
  16 without). A waiter whose turn comes after a
  connect failed while it waited returns that failure instead of dialling
  (`ConnectGate`; `waiters_share_a_failed_connect`: 8 calls to a host that never
  answers fail together after one dial, 7 dials without it). It matters where
  the connect timeout is shorter than the caller's deadline (retries,
  `ps_call`); on a first attempt the two are equal, the holder is cut off
  without recording, and the waiters' own deadlines end them together.
- `resolve_key(key) → (part_id, ps_addr)` — route key to partition.
- `resolve_part_id(part_id) → ps_addr` — resolve partition to PS.
- `all_partitions() → Vec<(part_id, ps_addr)>` — list all partitions.

## Wire-version handshake

Every newly opened manager, PS and direct-read EN connection completes
VERSION_HELLO through RpcClient before business traffic; reconnect repeats it.
Manager and PS target service types are checked. Normal connect/connect_raw
use client role; connect_admin and connect_peer use exact internal wire
matching. Raw namespace binding does not grant admin protocol or auth rights.
On PS, the existing CLIENT_AUTH follows protocol admission.

A client VersionMismatch becomes AutumnError::WireVersionRefused with server
versions and reason. It stops retries within the current operation; a future
operation can try a new connection, and success clears wire_refused. Peer/admin
constructors preserve typed RpcError so startup can distinguish expected
upgrade waiting from identity or authorization failures. Missing/old/malformed
Hello has no unchecked fallback. The obsolete best-effort GetClusterId version
self-check is removed; identity lookup remains.

negotiated_cluster_wire records the last successful response. Routing uses the
existing retained MSG_GET_REGIONS/MSG_GET_CLIENT_REGIONS forms, choosing from
the manager version before sending. ConnPool's role is fixed per instance;
the SDK's EN pool uses client and cannot share internal/admin connections.

## Operator and member connections

`connect_admin` / `connect_peer` declare the Admin / Peer role; every
connection they open runs PEER_AUTH and proves the cluster secret installed in
the process (`autumn_rpc::peer_auth::install`, done by autumn-op from
`--cluster-secret-file` and by the EN binary). Requests carry no token of their
own: operator-only RPCs are gated by the role the connection proved.

## Direct reads under authz

`ensure_token` hands every token it returns to the EN pool
(`ConnPool::set_auth_token`): each new EN connection sends `CLIENT_AUTH` after
VERSION_HELLO, and a changed token drops the pooled EN connections so they
rebind (as `ps_conns` already did). `set_principal_credential` clears it. An EN
of a cluster that runs authz refuses a direct read on a connection without a
valid bound principal. That refusal (`PermissionDenied`, from a refused
CLIENT_AUTH or a refused read; `en_refusal`) is the answer:
`DirectReadOutcome::Denied` returns `AutumnError::PermissionDenied` with no
other replica and no proxy fallback, because every EN judges the same
credential the same way and a fallback would only hide the refusal. An EN that
has not loaded its authz config yet answers `Unavailable`, which falls back as
before. Test: `connection_tests::en_credential_refusal_is_returned_without_fallback`
(replicated and EC descriptors).

## Error types

- `AutumnError::NotFound` — key not found.
- `AutumnError::InvalidArgument(msg)` — bad request.
- `AutumnError::PreconditionFailed(msg)` — e.g. split with overlap.
- `AutumnError::ServerError(msg)` — internal server error.
- `AutumnError::RoutingError(msg)` — cannot route key.
- `AutumnError::ConnectionError(msg)` — RPC connection failure.
- `AutumnError::NamespaceUnknown` — Layer-A reject (terminal on write path).
- `AutumnError::WireVersionRefused` — a server refused this client's wire version at
  `VERSION_HELLO`. TERMINAL, and more sharply than the two authz cases: the version
  is compiled in, so no refresh can change the answer. Carries the SERVER's own message,
  which names which way round the mismatch is.
- `AutumnError::OutcomeUnknown(msg)` — a WRITE may or may not have been applied
  (HBase's "retries exhausted"). Every other write error means "not applied".

### Write outcomes (`CallKind`)

A write attempt may have been applied when the server answered `OutcomeUnknown`
(`StatusCode` 9 / batch `CODE_OUTCOME_UNKNOWN`: the partition sent the append and
gave up; its reopen's seal decides, docs/seal_length_design.md §5) or when the
request was handed to the connection and no answer came back (`SentNoReply`, set by
`ps_call_with_timeout` / `ps_call_bulk_multi` on a transport failure after the
connect; a failed connect sent nothing). `Unavailable` / `CODE_UNAVAILABLE` means
not applied. The retry loops take a `CallKind`:
- `Idempotent` (put, delete, batch put/delete): retry as before. The
  write's error is settled at its single exit (`settle_write_error`): after any
  attempt that may have applied, every error — retries run out, a routing miss on
  a later attempt, a token that expired meanwhile — becomes `OutcomeUnknown`, so
  "not applied" holds only when no attempt may have applied.
- `Conditional` (`compare_put`): stop at the first attempt that may
  have applied and return `OutcomeUnknown`. A retry would compare against the first
  attempt's own write and report a conflict (`Ok(false)`) — what
  `write_outcome_settled::the_sdk_stops_a_conditional_write_at_its_first_unknown_outcome`
  shows when the stop is removed.
- `Read` (reads, admin calls): unchanged.

Lanes: `CallKind::lane` sends `Read` (reads and admin calls through
`call_ps_for_key` / `call_ps_for_part`: HEAD, GET_REDIRECT(_MANY), RANGE, maintenance) on
the RPC client's urgent lane, and every PS connection gets
`set_send_window(ps_send_window)` (3, under the PS's 16 admission slots),
so an `exists` probe overtakes queued large puts and finds a slot at the PS
(rpc CLAUDE.md, "Two lanes and a send window"). The bulk GET requests
(`call_into_pooled`) stay on the normal lane.

Batch writes resend, one by one through `put_opts` / `delete_opts`, the ops whose
status is `CODE_UNAVAILABLE` or `CODE_OUTCOME_UNKNOWN` (HBase's AsyncProcess resubmits
failed actions); a failed resend of a `CODE_OUTCOME_UNKNOWN` op is `OutcomeUnknown`
(`resent_batch_op`). `call_ps_for_part_tracked` / `call_ps_for_part_bulk` also report
whether an earlier attempt of the frame may have applied: a frame answered after a
lost first attempt can say "not applied" for an op the lost attempt wrote, so every
op of such a frame that ends in an error — whichever branch gave it — is settled to
`OutcomeUnknown` (`settle_batch_ops`). A frame that fails after its retries gives
every op the frame's class (`batch_frame_error`). Resends are serial, one
`put_opts` budget each; the first absorbs a reopen, the rest are fast. They go
in input order, and a later op of a resent key is resent after it even when it
was applied (`batch_resend_plan`), so a duplicate key keeps last-input-wins among
the small values. A key given both a small and a ≥64 KiB value in one
`put_many` takes the per-key serial path with ALL its values, because the
bulk-only values are written after every small group (test:
`system_putstream::put_many_mixed_size_duplicate_key_keeps_last_input`). `put_opts` /
`delete_opts` check the answer's body inside the settled region
(`call_ps_for_key_tracked`): a body error after a lost first attempt is
`OutcomeUnknown` too.

## Result types

- `KeyMeta { found, value_length }` — from `head()`.
- `RangeResult { entries: Vec<RangeEntry>, has_more }` — from `range()`.
- `RangeEntry { key, value }` — re-exported from partition_rpc.

## Architecture

- Single-threaded (`Rc`/`RefCell`) — designed for the compio single-thread runtime.
- Manager connections: round-robin failover on error, auto-reconnect.
- PS connections: cached per-address, dropped on error, recreated on next call.
- Routing: fetched at connect and refreshed on a routing miss (binary search).
  Which opcode asks is decided per refresh from the negotiated version, above.

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

The SDK stamps a `region_epoch: u64` (cached from the routing record's
`region_epoch`, bumped by the manager on every `rg` rewrite — split / merge) on
every hot-path request. The PS
rejects with `FailedPrecondition` when the stamped epoch mismatches; `call_ps_for_key`'s
`Err`-arm refresh path picks it up (drop conn, `refresh_regions`, retry).

Wire surface:
- The routing record carries `region_epoch: u64` — `ClientRegion` on
  `MSG_GET_CLIENT_REGIONS`, `MgrRegionInfo` on the older `MSG_GET_REGIONS`.
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

## Constants and parameters

SDK tunables are declared in `crates/params/src/client.rs` (`ClientParams`), one
process-wide instance (`autumn_client::params()`); every binary that embeds the
client offers its argv to it, so `--rpc-timeout-ms`, `--first-attempt-timeout-ms`,
`--batch-get-concurrency` / `--batch-put-concurrency` (32), `--token-renew-margin-secs`
(300) and `--stripe-chunk-bytes` (4 MiB) are flags of each of them. A client copies
the two timeouts when it is built (`set_rpc_timeout` still overrides one client);
the others are read where used.

- `BULK_MIN_BYTES = 64 KiB` — bulk engage threshold (both directions/transports);
  a protocol choice shared with the PS, not a flag.
- `MAX_PS_REFRESHES = 10` — routing-refresh retry budget (~9 s), base 100 ms / cap 2000 ms.
- `MAX_RANGE_REFRESHES = 3` — range refresh cycles per call.
- `MAX_RANGE_ITERATIONS = 10_000` — range iteration cap.

## Dependencies

- `autumn-rpc`: RPC client + wire codec (partition_rpc, manager_rpc).
- `compio`: async runtime (time::sleep for retry backoff).
- `anyhow`, `bytes`: error handling + byte buffers.

## Runtime dependency

Compio 0.19.2 is inherited from the workspace and requires Rust >=1.95. The
standalone Python manifest/lockfile must resolve the same compio family. Client
I/O strategy, pooled receive ownership and caller timeouts remain unchanged.

## Connection reuse after refusals

PS plain, bulk-write, multi-value and pooled-read paths classify RpcError before
mapping it to AutumnError. Status errors retain the cached connection; transport
failures and local timeouts evict it. Routing refresh and retry remain separate:
a stale epoch still refreshes the region map and rebuilds the request with the
new epoch. The SDK's manager connection uses the same classification.
Identity changes and token renewal still clear PS connections so CLIENT_AUTH
binds the new identity/token. Keeping a status-refused connection is not a
substitute for this authentication lifecycle.
Tests in src/connection_tests.rs count TCP accepts and verify retry epochs;
manager's system_status_connection_reuse exercises actual split and merge.

## Conditional writes

`compare_put(key, expected, value: Option<Bytes>, opts) → bool` is the one conditional
write (wire 75, `MSG_COMPARE_WRITE_BULK`): when the key's visible value equals
`expected` (`None` = absent) it writes `value` (`None` = deletes the key) and returns
`true`; otherwise nothing is written and it returns `false` (a body-level conflict,
not retried as stale routing). Decided by the partition's write actor in arrival
order, so concurrent callers from any clients get one winner. The value travels as
a bulk tail through `call_ps_for_key_bulk` (no 64 KiB cap, no copy); `expected` is
capped at 64 KiB. With `expected = None` the PS only checks the key is present.
`opts.ttl` reaches the stored entry; a non-anonymous `opts.lease` is fence-checked
before the comparison and a raised floor persists even when the comparison fails
(a pure floor bump for a recovering owner). Transport outcomes can be ambiguous
(`OutcomeUnknown`): callers publishing UUID generations read back their UUID to
resolve a lost ACK. Refused with `Unsupported` below
`WIRE_VERSION_WITH_COMPARE_WRITE_BULK`. Callers: the fs core (directory entries,
inode CAS, link counts, S3 publication, multipart state). A present key still costs
the value's transfer, so it does not replace an `exists` probe for dedup of
mostly-present pages (350 ms vs 1 ms for 8 x 64 x 16 MiB over UCX). The PS still
serves the older `MSG_COMPARE_PUT` / `MSG_COMPARE_WRITE` / `MSG_COMPARE_PUT_BULK`
for older clients; the SDK no longer sends them.
