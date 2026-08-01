# autumn-rpc Crate Guide

## Purpose

Custom binary RPC framework on compio (completion-based I/O, thread-per-core).
Replaces tonic/gRPC to drop HTTP/2 framing and protobuf overhead on the hot path
(extent-node append fanout). Its living surface is the **client + wire** half:
`RpcClient`, the `manager_rpc` / `partition_rpc` / `extent_rpc` wire schemas,
`Frame`/`FrameDecoder`, `StatusCode`. Servers are hand-rolled per component on
`autumn_transport::Conn` (EN, manager, PS), not in this crate.

## Wire Format (v28 — unified CRC)

ONE frame shape, no flag-dependent variants:

```
[req_id: u32 LE][msg_type: u8][flags: u8][payload_len: u32 LE]      header, 10 B
payload = [ctrl_len: u32 LE][ctrl …][crc32c: u32 LE][value …]
```

| Field | Size | Description |
|-------|------|-------------|
| req_id | 4B | Multiplexing ID. Client picks, server echoes. 0 = fire-and-forget. |
| msg_type | 1B | RPC method identifier (0-255 per service) |
| flags | 1B | bit 0 `FLAG_RESPONSE`, bit 1 `FLAG_ERROR`, bit 2 `FLAG_STREAM_END`. Bit 3 reserved (was `FLAG_CRC` pre-v28 — protection is now structural, no bit to flip off) |
| payload_len | 4B | Everything after the header (`HEADER_LEN=10`) |
| ctrl_len | 4B | Length of the CRC-protected control bytes |
| crc32c | 4B | Over `header ++ ctrl_len ++ ctrl` — NEVER over `value` |

`value_len = payload_len − 4 − ctrl_len − 4`, may be 0. Error responses put
`[status_code: u8][message]` in ctrl.

### CRC rule: header+ctrl always protected, bulk value never

The decoder verifies the crc BEFORE exposing anything; mismatch =
`FrameError::CrcMismatch`, structural inconsistency = `FrameError::Malformed`.
Header inclusion closes the pre-v28 holes: a flipped `req_id` delivering a
valid-crc response to the WRONG caller, and a flipped `FLAG_CRC` bit silently
disabling verification. The `value` tail is raw — its integrity is the
transport's (UCX NIC ICRC / TCP kernel checksum) + the storage layer's (WAL
record CRC, SST block CRC); a per-value crc was measured at ~20% of a core
@ 8 MiB. Per-msg_type ctrl/value split:

- normal rkyv/binary RPCs + error envelopes: ctrl = whole body, no value.
- `MSG_GET_BULK` / `MSG_READ_BYTES_BULK` responses: ctrl = `[code:1][message…]`
  (bulk errors carry a human-readable message), value = raw value.
- `MSG_PUT_BULK` requests: ctrl = `[put_bulk meta 44B][key]`, value = raw value —
  the sender never crc-scans the value (`call_vectored_bulk`).
- **`MSG_APPEND` is the ONE deliberate exception** (durability path): its bulk
  payload rides INSIDE ctrl, keeping in-transit CRC on WAL/SST bytes.

Builders: `Frame::encode` / `encode_response_with` (ctrl-only, one buffer),
`encode_vectored_head` + `compute_ctrl_crc` (vectored sends),
`encode_bulk_response_head` (bulk response head; `ps_bulk_head` / `bulk_read_head` are
thin wrappers), `parse_bulk_ctrl`. bulk fast paths use
`FrameDecoder::peek_bulk_prologue` (verify crc without consuming) +
`consume_bulk_prologue`. One protocol — no versions, no encoder toggle, no
back-compat (the cluster restarts together; a pre-v28 peer fails LOUDLY at the
first frame with CrcMismatch instead of reaching the version handshake).

## Modules

- **`frame.rs`** — `Frame` (encode/decode one frame), `FrameDecoder` (streaming
  decode state machine), `HEADER_LEN=10`, `MAX_PAYLOAD_LEN`, flag bits.
  `encode_response_with` builds a framed response in one allocation.
- **`error.rs`** — `StatusCode` (Ok, NotFound, InvalidArgument,
  FailedPrecondition, Internal, Unavailable, AlreadyExists, PermissionDenied),
  `RpcError`, `encode_status`/`decode_status`.
- **`client.rs`** — `RpcClient` (below).
- **`extent_rpc.rs`** — ExtentService wire codec: hot-path binary
  (Append/ReadBytes/CommitLength) + rkyv control-plane (AllocExtent/Df/…). The
  single wire-schema home; autumn-stream re-exports it. `DiskStatus.extent_bytes`
  (EN self-reported per-disk footprint) feeds cluster-df.
- **`manager_rpc.rs`** / **`partition_rpc.rs`** — manager and PS wire schemas
  (rkyv structs + `MSG_*` constants), the most-referenced surface in the crate.
- **`cap_token.rs`** — Ed25519 capability-token codec for data-plane authz: the
  manager (leader) signs short-TTL tokens with a private key, the PS verifies
  with the public key only (asymmetric — a compromised PS can verify, never
  forge), the client forwards opaque bytes. Single source of truth for the claims
  layout, signing bytes, and domain-separation prefix; in the wire fingerprint.

`MSG_TYPE_PING = 0xFF` is reserved; heartbeat lives in each per-component pool.

## RpcClient — SQ/CQ architecture

`RpcClient::connect(addr)` returns `Rc<RpcClient>` and starts two background tasks
over one TCP connection:

- **SQ**: callers push `SubmitMsg { Single | Vectored }` onto a bounded
  `mpsc::channel(SUBMIT_CHANNEL_CAP=1024)`. A single `writer_task` owns
  `WriteHalf` and drains it sequentially — no cross-caller mutex; back-pressure
  comes from the bounded channel.
- **CQ**: the `read_loop` task owns `ReadHalf`, decodes frames, dispatches to the
  matching entry in `Rc<RefCell<HashMap<u32, Pending>>>`.

Calls: `call`, `call_vectored` (vectored ctrl, zero-copy parts),
`call_vectored_bulk` (ctrl parts + raw value after the crc — `MSG_PUT_BULK`),
`call_timeout` / `call_vectored_timeout`, `send_frame` / `send_vectored`
(low-level, return `oneshot::Receiver<Frame>`), `send_oneshot`
(fire-and-forget, req_id=0), `call_into_pooled` (bulk read, below).

**Invariants (correctness rules):**

- Pending-insert happens **before** submit (`register_and_submit`), so the
  read_loop never finds a response with no entry; a failed submit rolls it back.
- `pending.borrow_mut()` is always tightly scoped, never held across an await —
  else a re-entrant call on the same compio thread panics the RefCell.
- `submit_tx` is cloned from a scoped borrow, never borrowed across
  `.send().await` — same RefCell-across-await hazard.
- `next_req_id` skips `0` on wraparound — `0` is fire-and-forget, no response
  routing.
- **`closed: Rc<Cell<bool>>`** is set true when `read_loop` or `writer_task`
  exits, BEFORE `pending` is cleared. Every submit checks it first, in the same
  sync block as `pending.insert` (no await between), so a concurrent close
  resolves to either early-return `ConnectionClosed` or a `pending.clear()`.
  Without it, a stale `Rc<RpcClient>` in a pool would accept submits no live
  read_loop can dispatch. Pools treat `is_closed()` as evict-and-reconnect
  (`stream::conn_pool::get_client`).

### Zero-copy receive-into-pooled

`call_into_pooled(msg_type, payload) -> BulkResp{buf, code, message}` recvs the
response's raw value tail straight into a read_loop-owned RegPool `PooledBuf`
(registered on UCX, plain recycled buffer on TCP), no intermediate Vec. Wire
response = the v28 value-separable frame: ctrl = `[code:1][message…]` (both
CRC-protected together with the header; bulk errors carry a readable message),
value = raw tail (`value_len` derived from `payload_len`).
`RegisteredMem`/`PooledBuf` re-export from autumn-transport (uninhabited/plain
stubs on non-ucx). A recv-into-CALLER-dest sibling (`call_into_dest`) no longer
exists — see "Why pooled-only" below.

**read_loop dispatch (4-way)** — keyed on the req_id's `Pending` variant (which
API the caller used), NEVER on msg_type (the rpc layer stays business-agnostic;
msg_type↔API pairing is the caller's contract, enforced nowhere):

```
Pending::Frame (non-bulk call)
  → try_decode whole frame (verify header+ctrl crc) → oneshot the Frame
Pending::IntoPooled (bulk call), response frame NOT FLAG_ERROR
  ├─ UCX                → fast path: peek_bulk_prologue (verify crc, parse
  │                       code+message), consume prologue, regpool_acquire +
  │                       recv_into(dest, reg) — memh RDMA when the slab is
  │                       registered (0 copies). Unconditional: recv-into is
  │                       never worse than decode on UCX, any size.
  └─ TCP
     ├─ payload ≥ 64 KiB → fast path: verify prologue, drain buffered value
     │  (TCP_RECV_INTO_    prefix into the PooledBuf, then one owned read
     │   POOLED_MIN_       (read_exact_into_pooled). Only the unavoidable
     │   BYTES)            kernel copy; no FrameDecoder accumulation.
     └─ payload < 64 KiB → try_decode (splits ctrl/value, verifies crc) +
                          finish_into_pooled_from_frame (one memcpy — a small
                          value's whole frame is usually already buffered, so
                          recv-into would only add a pool acquire + a syscall).
Pending::IntoPooled, response frame IS FLAG_ERROR
  → excluded from the fast path by the peeked flags → try_decode → the
    IntoPooled arm decodes the `[status_code][message]` envelope into
    `RpcError::Status` (an authz PermissionDenied / mis-route NotFound reaches
    the bulk caller typed, never parsed as a bulk ctrl).
```

The TCP size gate lives at the RECEIVER, not the caller, because its input — the
ACTUAL value_len — only exists once the response header arrives: an error /
NotFound reply is a 0-length bulk frame regardless of what the caller expected.
Intent vs execution: the client-side `bulk_worthwhile` (autumn-client, ≥ 64 KiB on
the EXPECTED size) picks which msg_type/API to use; the receiver picks the recv
strategy from what actually arrived. The four 64 KiB gates are deliberately one
value (see autumn-client CLAUDE.md "Zero-copy selection rule"):

| Gate | Side | Input | Decides |
|------|------|-------|---------|
| `bulk_worthwhile` (autumn-client) | client send | expected size | which msg_type/API (read + write intent) |
| `TCP_RECV_INTO_POOLED_MIN_BYTES` (client.rs) | client recv | actual value_len | GET-bulk response recv strategy (this table) |
| `AUTUMN_PS_BULK_RECV_MIN_BYTES` (partition-server) | PS recv | actual value_len | PUT_BULK request recv strategy (`drain_bulk_writes`) |
| `handle_get_redirect` 64 KiB (partition-server) | PS route | actual clamped read len | EN-direct descriptor vs proxy read |

**Why pooled-only (cancel-safety):** the recv runs in the long-lived
`read_loop`, NOT the caller future, and the read_loop OWNS the `PooledBuf` — a
caller-cancel/timeout just drops the buffer back to the pool, never a leak,
never a NIC writing freed memory. The removed `call_into_dest` variant recv'd
into a caller-owned `*mut u8`, which forced the inverse contract (dest outlives
the call, NO per-call timeout ever) — making it the one SDK RPC that could hang
unboundedly; its explicit `reg` was `None` at every production call site
(implicit rcache registration), and the default-on EN-direct read path had
already chosen pooled-recv + one memcpy deliberately. Callers needing the value
at a specific address copy out of the returned `PooledBuf`
(`ClusterClient::get_range_into` does exactly this, and now honors
`rpc_timeout`).

**Write counterpart (`MSG_PUT_BULK = 0x51`)** uses `call_vectored_bulk`: ctrl =
`[meta][key]` (CRC'd with the header), the value rides after the crc as its own
iovec — zero-copy via rcache when registered, and NEVER crc-scanned by the
sender (v28 removed the per-value crc: pre-v28 `call_vectored` paid a full crc32c pass
over the value). Meta codec lives in `partition_rpc`: `encode_put_bulk_meta` /
`parse_put_bulk_meta`, fixed prefix `PUT_BULK_HEADER_LEN = 44` then the key; the
decoder hands the PS `frame.payload = [meta][key]` + `frame.value` (zero-copy
split). Write bulk is send-side framing only; read bulk needs the
`call_into_pooled` recv primitive because the response value must land outside
the FrameDecoder. Write-side selection is purely
size-based and client-side (the sender KNOWS the exact value size): `put_many`
routes items ≥ 64 KiB to per-op `MSG_PUT_BULK` and smaller ones into
`MSG_BATCH_PUT` via `bulk_worthwhile`; the bare `put_bulk` API does not gate, so the
wire legitimately carries any-size `MSG_PUT_BULK` — the PS recv side re-decides on
the ACTUAL size (`drain_bulk_writes` recv-into-pooled ≥
`AUTUMN_PS_BULK_RECV_MIN_BYTES`, else the normal FrameDecoder path).

## shard_for_extent

`shard_for_extent(extent_id, shard_count) -> u32` is the ONE canonical
extent→shard map, living here (lowest common dep) so the EN (`owns_extent` +
sibling forward), the manager (`shard_addr_for_extent`), and the StreamClient
(`conn_pool::shard_addr_for_extent`) all compute the same shard — a mismatch
black-holes routing. A splitmix64 finalizer decorrelates bootstrap's contiguous
extent ids (7 per partition) from the modulus; a raw `extent_id % shard_count`
aliased every partition's data extents onto shard 0, concentrating client-direct
reads on one EN. `shard_count <= 1` / empty `shard_ports` → shard 0.

**Changing this remaps ownership of existing extents ⇒ STOP-THE-WORLD reshard**
(every EN shard + the manager must agree). It is byte-free (EN shards share the
hashed on-disk data dirs — only logical ownership re-partitions on restart), needs
no wire-struct change (lib.rs is not in the WIRE fingerprint) and no etcd reset.
Tests: `shard_for_extent_tests`.

## Admin-token payload-prefix codec

`is_admin_mgr_msg(msg_type)` is the set of cluster-MUTATING manager ops gated
behind the manager's admin secret (fence/remove/maintenance/create-stream/
upsert-partition/merge/bump-cluster-version/…). Read-only observability ops and
ops carrying their own `admin_token` field (tenant/namespace/principal) are NOT
gated; `MSG_REGISTER_NODE` is deliberately excluded (the EN self-registers with no
admin token — gating it would wedge bring-up). `is_admin_ps_msg` is the PS analog
(`MSG_SPLIT_PART`, `MSG_MAINTENANCE`).

The token rides as an out-of-band prefix stripped before rkyv decode:
`prefix_admin_token(token, payload)` prepends `[u32 LE token_len][token][payload]`
(`ADMIN_TOKEN_LEN_PREFIX = 4`); `strip_admin_token` returns `(token, rest)` or
`None` on a malformed prefix. The manager treats `None` as a FAILED check, never
"run it bare" — a bare unprefixed payload can't be mistaken for a valid strip.

## WIRE fingerprint + wire-version interval

`build.rs` hashes the wire-schema sources (`manager_rpc.rs`, `partition_rpc.rs`,
`frame.rs`, `extent_rpc.rs`, `cap_token.rs`) into `WIRE_FINGERPRINT` (16-hex
compile-time const). Deploys are same-commit (rkyv has no cross-version compat; a
mixed deploy fails SILENTLY with garbage decodes). Hashing the schema source (not
the commit) keeps dev flows sane: unrelated edits don't perturb it, any
wire-struct edit does — even a comment.

- **`WIRE_VERSION_MIN` / `WIRE_VERSION_MAX`** (currently 27/27) declare the
  interval this binary speaks. `wire_compat_check(remote_fp, remote_min,
  remote_max)` accepts iff fingerprints are equal (same-build fast path) OR the
  intervals overlap. A peer reporting `max == 0` (empty/pre-WIRE) is refused.
- **`WIRE_VERSION_FINGERPRINTS`** pins each declared version to the fingerprint it
  was declared against. The `registry_pins_current_schema_to_max_version` test
  fails the test run whenever the schema changes without a version decision — this
  is what makes interval overlap trustworthy. Bump rule: pre-R3 (rkyv has no
  cross-version decode) bump `MAX` and set `MIN = MAX`; post-R3 keep `MIN = MAX-1`
  (frozen V1 + explicit V2 msg_types, N↔N-1 window).
- Runtime cross-check: a peer claiming a version in our registry with a DIFFERENT
  fingerprint is refused as "wire-version fraud" — forgot-to-bump caught at
  runtime, not just CI.
- Exchange: the fingerprint + interval ride on `GetClusterIdResp` (filled by the
  manager in `handle_get_cluster_id`), checked at every long-lived process's
  startup (`ClusterClient::connect`, PS `finish_connect`). `GetClusterIdReq/Resp`
  are FROZEN — they ARE the negotiation channel, decoded before any compat
  decision; additions go in new msg_types. A SUCCESSFUL response failing the check
  is a hard startup refusal; a TRANSPORT failure fetching it is best-effort
  skipped (availability wins while the manager is briefly down — every subsequent
  RPC fails loudly anyway).
- `cluster_version` (manager etcd key `autumn-rs/cluster_version`, ASCII decimal)
  is the operator-bumped feature gate: `MSG_GET_CLUSTER_VERSION` (0x4A, fresh etcd
  read) / `MSG_BUMP_CLUSTER_VERSION` (0x4B, leader-only, +1, capped at
  `WIRE_VERSION_MAX`, value-CAS'd). Bump via `autumn-op upgrade-version` only
  after every member runs the new binary; new wire/persisted formats gate on
  `cluster_version >= N`. Every manager decode of the persisted value fails closed
  (blocks leadership) when it exceeds the binary's own `WIRE_VERSION_MAX`.

## Notes

- The 10-byte header eliminates HTTP/2 frame (9B) + gRPC envelope (5B) + HEADERS
  frame (~50B+): ~58B overhead vs ~200B+ for gRPC.
- `tokio::sync::{Mutex,mpsc,oneshot}` are runtime-agnostic futures — they work on
  compio without a tokio Runtime.
