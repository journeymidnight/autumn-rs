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
- `delete(key)` — delete a key
- `head(key) → KeyMeta` — get metadata (found, value_length)
- `range(prefix, start, limit) → RangeResult` — prefix scan
- `stream_put(key, value, must_sync)` — write large values

**Maintenance operations:**
- `split(part_id)` — trigger partition split
- `compact(part_id)` — trigger compaction
- `gc(part_id)` — trigger automatic GC
- `force_gc(part_id, extent_ids)` — force GC on specific extents
- `flush(part_id)` — trigger memtable flush

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
`CODE_NOT_FOUND` from the receiving merged_partition_loop's mis-routed-frame
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

## Dependencies

- `autumn-rpc`: RPC client + wire codec (partition_rpc, manager_rpc)
- `compio`: async runtime (time::sleep for retry backoff)
- `anyhow`, `bytes`: error handling + byte buffers
