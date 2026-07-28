# autumn-rpc Crate Guide

## Purpose

Custom binary RPC framework on compio (completion-based I/O, thread-per-core).
Replaces tonic/gRPC to drop HTTP/2 framing and protobuf overhead on the hot path
(extent-node append fanout). Its living surface is the **client + wire** half:
`RpcClient`, the `manager_rpc` / `partition_rpc` / `extent_rpc` wire schemas,
`Frame`/`FrameDecoder`, `StatusCode`. Servers are hand-rolled per component on
`autumn_transport::Conn` (EN, manager, PS), not in this crate.

## Wire Format

10-byte frame header + payload:

```
[req_id: u32 LE][msg_type: u8][flags: u8][payload_len: u32 LE][payload bytes]
```

| Field | Size | Description |
|-------|------|-------------|
| req_id | 4B | Multiplexing ID. Client picks, server echoes. 0 = fire-and-forget. |
| msg_type | 1B | RPC method identifier (0-255 per service) |
| flags | 1B | bit 0 `FLAG_RESPONSE`, bit 1 `FLAG_ERROR`, bit 2 `FLAG_STREAM_END`, bit 3 `FLAG_CRC` |
| payload_len | 4B | Payload size (`HEADER_LEN=10`, `MAX_PAYLOAD_LEN=u32::MAX`); includes the 4-byte CRC trailer when `FLAG_CRC` is set |

Error responses encode status as `[status_code: u8][message bytes]`.

### Per-frame CRC32C

One frame protocol — no versions, no encoder toggle, no back-compat (the cluster
restarts together). Every frame from `Frame::encode` carries a 4-byte CRC32C
trailer over the payload: `FLAG_CRC` set, `payload_len` counts the trailer. The
decoder verifies + strips it; a mismatch is `FrameError::CrcMismatch`. This
guards a flipped `extent_id`/`eversion`/`revision`/`owner_epoch` over TCP — a
silent wrong-extent write or fence bypass that TCP's 16-bit checksum + NIC
offload bugs can let through and on-disk CRC cannot catch in transit. HW CRC32C
(SSE4.2) is negligible on the small control frames it covers.

**The one CRC-less frame** is the zero-copy value response, built by
`Frame::encode_no_crc` (hand-built in production as `partition-server::ps_zc_head`
/ `stream::zc_read_head`): `call_into_dest` / `call_into_pooled` recv the value
straight into a caller dest and cannot strip a trailer, so it omits the CRC
(`FLAG_CRC` unset) and relies on transport integrity (UCX NIC ICRC / TCP kernel
checksum). The decoder's `FLAG_CRC` branch exists to handle this one shape — a ZC
design constraint, not a legacy version.

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

Calls: `call`, `call_vectored` (vectored, zero-copy), `call_timeout` /
`call_vectored_timeout`, `send_frame` / `send_vectored` (low-level, return
`oneshot::Receiver<Frame>`), `send_oneshot` (fire-and-forget, req_id=0).

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

### Zero-copy receive-into-dest

`call_into_dest(msg_type, payload, dest: *mut u8, dest_cap, reg:
Option<&RegisteredMem>) -> DestMeta` reads the response value straight into
`dest`, no intermediate Vec; `call_into_pooled` is the sibling that recvs into a
read_loop-owned `PooledBuf` and hands it back. Wire response = CRC-less frame
with payload `[ZC meta][value]`, `encode_zc_meta(code, value) = [code:1]
[value_len:4 LE][reserved:4 LE]` (`ZC_META_LEN = 9`; reserved carries no value
CRC — integrity is the transport's). No value CRC is verified on either path.
`RegisteredMem`/`PooledBuf` re-export from autumn-transport (uninhabited/plain
stubs on non-ucx, so `reg` is always `None`).

**read_loop dispatch (4-way)** — keyed on the req_id's `Pending` variant (which
API the caller used), NEVER on msg_type (the rpc layer stays business-agnostic;
msg_type↔API pairing is the caller's contract, enforced nowhere):

```
Pending::Frame (non-ZC call)
  → try_decode whole frame (verify+strip frame CRC) → oneshot the Frame
Pending::IntoDest / IntoPooled (ZC call)
  ├─ UCX                → fast path: consume header+meta, drain buffered value
  │                       prefix, recv_into(dest, reg) — memh RDMA when reg=Some
  │                       (0 copies). Unconditional: recv-into is never worse
  │                       than decode on UCX, any size.
  └─ TCP
     ├─ value ≥ 64 KiB  → fast path: drain buffered prefix into dest, then one
     │  (TCP_RECV_INTO_   owned read — read_exact_into_raw (IntoDest) /
     │   POOLED_MIN_      read_exact_into_pooled (IntoPooled). Only the
     │   BYTES)           unavoidable kernel copy; no FrameDecoder accumulation.
     └─ value < 64 KiB  → try_decode + finish_into_{dest,pooled}_from_frame
                          (decode + one memcpy — a small value's whole frame is
                          usually already buffered, so recv-into would only add
                          a pool acquire + an extra syscall).
```

The TCP size gate lives at the RECEIVER, not the caller, because its input — the
ACTUAL value_len — only exists once the response header arrives: an error /
NotFound reply is a 0-length ZC frame regardless of what the caller expected,
and `dest_cap` is only an upper bound. Intent vs execution: the client-side
`zc_worthwhile` (autumn-client, ≥ 64 KiB on the EXPECTED size) picks which
msg_type/API to use; the receiver picks the recv strategy from what actually
arrived. The four 64 KiB gates are deliberately one value (see autumn-client
CLAUDE.md "Zero-copy selection rule"):

| Gate | Side | Input | Decides |
|------|------|-------|---------|
| `zc_worthwhile` (autumn-client) | client send | expected size | which msg_type/API (read + write intent) |
| `TCP_RECV_INTO_POOLED_MIN_BYTES` (client.rs) | client recv | actual value_len | GET-ZC response recv strategy (this table) |
| `AUTUMN_PS_ZC_RECV_MIN_BYTES` (partition-server) | PS recv | actual value_len | PUT_ZC request recv strategy (`drain_zc_writes`) |
| `handle_get_redirect` 64 KiB (partition-server) | PS route | actual clamped read len | EN-direct descriptor vs proxy read |

**Cancel-safety (mandatory):** the recv-into-`dest` runs in the long-lived
`read_loop`, NOT the caller future. It is safe ONLY when `dest` outlives the call
and the call is not dropped mid-recv — so `call_into_dest` has no per-call
timeout (`ClusterClient::get_into` satisfies this). Any path needing a
timeout/failover uses `call_into_pooled` — the read_loop owns the buffer, so a
caller-cancel just returns it to the pool, never a leak.

**Write counterpart (`MSG_PUT_ZC = 0x51`)** needs no new primitive: the client
sends `[meta][value]` via `call_vectored` (value as its own iovec, zero-copy via
rcache when registered; the frame CRC covers `[meta||value]`). Framing lives in
`partition_rpc`: `encode_put_zc_meta` / `parse_put_zc_meta`, fixed prefix
`PUT_ZC_HEADER_LEN = 44` (part_id + region_epoch + expires_at + value_len +
key_len) then the key; the PS slices the value zero-copy from the reassembled
frame. Write ZC is send-side framing only; read ZC needs `call_into_dest` because
the value must land in a specific caller dest. Write-side selection is purely
size-based and client-side (the sender KNOWS the exact value size): `put_many`
routes items ≥ 64 KiB to per-op `MSG_PUT_ZC` and smaller ones into
`MSG_BATCH_PUT` via `zc_worthwhile`; the bare `put_zc` API does not gate, so the
wire legitimately carries any-size `MSG_PUT_ZC` — the PS recv side re-decides on
the ACTUAL size (`drain_zc_writes` recv-into-pooled ≥
`AUTUMN_PS_ZC_RECV_MIN_BYTES`, else the normal FrameDecoder path).

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
