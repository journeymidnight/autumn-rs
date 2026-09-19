# autumn-rpc Crate Guide

## Purpose

Custom binary RPC framework on compio (completion-based I/O, thread-per-core).
Replaces tonic/gRPC to drop HTTP/2 framing and protobuf overhead on the hot path
(extent-node append fanout). Its living surface is the **client + wire** half:
`RpcClient`, the `manager_rpc` / `partition_rpc` / `extent_rpc` wire schemas,
`Frame`/`FrameDecoder`, `StatusCode`. Servers are hand-rolled per component on
`autumn_transport::Conn` (EN, manager, PS), not in this crate.

## Wire Format (unified CRC)

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

### Prepared replica payloads

`PreparedPayload` owns immutable `Bytes` segments and their total length.
`frame_parts` gives each replica connection a frame carrying its own req_id,
and the checksum cannot be paired with different bytes through the public API.
The frame's bytes and full header/control coverage are unchanged; no
wire-version change is needed.

Every star-replicated stream append prepares, whatever its size or replica
count — the wire bytes are identical either way, so the send site has nothing to
choose between. What DOES vary is how each frame finishes its CRC, and
`PreparedPayload` owns that choice because it is the only place it can be
measured:

- at or above `COMBINE_MIN_BYTES` with more than one frame, the payload is
  scanned once and each frame's header CRC is joined on with `crc32c_combine`;
- otherwise each frame re-scans, exactly as a per-frame vectored send did.

The threshold is 1 MiB and it is large on purpose. `crc32c_combine` is a GF(2)
matrix ladder — log(len) growth, but a constant that dwarfs a hardware CRC pass
— so sharing a scan is a LOSS below about 512 KiB, and catastrophically so on
small frames: 63 µs against 2.7 µs at 4 KiB RF=3. A single frame can never win
the shared scan back however large the payload, which is why the frame count is
an input. `replica_crc_cpu_benchmark` is the measurement; rerun it on new
hardware before trusting the constant.

Tests compare exact wire bytes against both the single-buffer and the vectored
encoding, on both checksum arms, at each size and request ID, and reject altered
payloads.

- **`frame.rs`** — `Frame` (encode/decode one frame), `FrameDecoder` (streaming
  decode state machine), `HEADER_LEN=10`, `MAX_PAYLOAD_LEN`, flag bits.
  `encode_response_with` builds a framed response in one allocation. Receive
  loops read into the decoder's own buffer (see "Receiving into the decoder").
- **`error.rs`** — `StatusCode` (Ok, NotFound, InvalidArgument,
  FailedPrecondition, Internal, Unavailable, AlreadyExists, PermissionDenied),
  `RpcError`, `encode_status`/`decode_status`.
- **`client.rs`** — `RpcClient` (below).
- **`extent_rpc.rs`** — ExtentService wire codec: hot-path binary
  (Append/ReadBytes/CommitLength) + rkyv control-plane (AllocExtent/Df/…). The
  single wire-schema home; autumn-stream re-exports it. `DiskStatus.extent_bytes`
  (EN self-reported per-disk footprint) feeds cluster-df. `DfResp.op_progress`
  carries live `ExtentOpProgress` samples for the extent-scoped ops the NODE
  executes (EC conversion, recovery) — keyed by extent_id, because the node
  never learns the manager's op id — so a multi-hour conversion shows a ratio
  instead of a bare RUNNING. A sample, not a queue: overwritten per update,
  dropped when the op ends. `MSG_FENCE_EXTENT` (17,
  `FenceExtentReq`/`FenceExtentResp`) raises the per-extent `owner_epoch` fence
  floor WITHOUT appending — the eager takeover fence (`StreamClient::fence_tail`,
  the G1 zombie-writer fix; see stream CLAUDE.md note 31).
- **`manager_rpc.rs`** / **`partition_rpc.rs`** — manager and PS wire schemas
  (rkyv structs + `MSG_*` constants), the most-referenced surface in the crate.
  The extent-service messages the manager sends are **re-exported** from
  `extent_rpc`, never redefined: `AllocExtentReq/Resp`, `ConvertToEcReq`,
  `DeleteExtentReq`, `DfReq/Resp`, `ReAvaliReq`, `RequireRecoveryReq`,
  `RecoveryTask(Done)`. `CodeResp` is the exception — `manager_rpc` keeps its
  own, for the manager service's own RPCs, so extent-service call sites write
  `extent_rpc::CodeResp`.

  **One definition per message, enforced at compile time.** These used to be
  mirrored (`ExtDfReq`, `ExtDeleteExtentReq`, `MgrRecoveryTask`, …): the
  manager encoded through its copy while the node decoded through
  `extent_rpc`'s, so a field added to one side only was a silent rkyv
  mis-decode, and a version bump could not have caught it — both copies live
  under the same version. `extent_rpc`'s `one_definition_only!` block is an identity
  function per message that compiles only while the two paths name the same
  type, so reintroducing a mirror is a build error. A domain type that
  genuinely needs to differ from its wire form must convert at the encode site;
  a same-shaped copy encoded directly is a mirror, not a separation.
- **`cap_token.rs`** — Ed25519 capability-token codec for data-plane authz: the
  manager (leader) signs short-TTL tokens with a private key, the PS verifies
  with the public key only (asymmetric — a compromised PS can verify, never
  forge), the client forwards opaque bytes. Single source of truth for the claims
  layout, signing bytes, and domain-separation prefix; part of the wire schema.

### Receiving into the decoder (`ReadWindow`)

Receive loops do not read into a scratch `Vec` and `feed` it: that memcpy'd
every received byte once more after the kernel copy (TCP) or Stream unpack
(UCX). `FrameDecoder::read_window(max_len)` lends spare capacity of the
decoder's own `BytesMut` to the read as a `compio::buf::Slice`;
`finish_read(window)` takes it back with the received bytes appended. Frames
then split zero-copy from memory the transport wrote. The lent buffer leaves
the decoder empty until it returns, so nothing may touch the decoder while a
read future owns the window (the EN, PS and `read_loop` only decode after the
read completes).

Window policy — each rule removes a copy that was measured, not guessed:
- A partially buffered frame whose rest fits in the spare capacity keeps that
  allocation; starting a fresh one would copy the partial frame.
- At a frame boundary, reuse spare capacity while it holds a quarter of
  `max_len`, so small frames fill one allocation instead of each reserving a
  new one while earlier frames still share it.
- `try_decode` reserves an incomplete frame's remaining length, so a window of
  `front_frame_remaining()` receives the rest in place.
- The extent node passes `max(512 KiB, front_frame_remaining())`. The part of a
  large frame that arrived in the window before its header was parsed is
  copied once by `try_decode`'s reserve: per replica ~0.45x (TCP) / ~0.19x (UCX)
  for a 1 MiB append, ≤0.06x at 8 MiB. Sizing each boundary window to the previous frame removed
  that copy but was measured SLOWER and is not used: it reserves the next
  frame's buffer while the previous append still shares the old one, which
  raised extent-node page faults (tens → 18–28 K per 2 GiB window) and cost
  ~2% on UCX 8 MiB writes (3 interleaved runs each: 459–464 vs 470–477 MiB/s
  without it). Do not reintroduce it without a better buffer lifetime.

Bulk fast paths keep a fixed window for their frame (`read_loop` for a pending
`call_into_pooled` response, the PS for `MSG_PUT_BULK` / `MSG_BATCH_PUT_BULK`):
only the prologue and a bounded value prefix may enter the decoder, because
the value is received into a pooled buffer. `feed` remains for tests and
control-plane loops that are not on a data path.

Cancellation on UCX: `ucx_recv` deliberately leaks an owned buffer whose read
future is dropped mid-flight (its drain can bail out while UCX still holds the
pointer). The leaked buffer is now the decoder's — up to one frame-sized window
on the extent node (e.g. 8 MiB after 8 MiB appends) instead of the former fixed
512 KiB / 64 KiB scratch `Vec`. Same sites and frequency: only a connection torn
down with a UCX read pending.

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
- **The two task handles are FIELDS, never `detach()`ed.** Dropping the last
  `Rc<RpcClient>` cancels both tasks, which drops the socket halves and closes
  the connection. This is the ONLY teardown there is. `read_loop` is the half
  that always outlives a detached client: it blocks in `read` until EOF, so it
  holds an evicted connection open even when that connection is IDLE and
  healthy — token renewal can evict one with nothing in flight. Before the
  refusal-classification fix, server status errors also did so. `writer_task`
  outlives its client alongside the reader whenever frames are queued: it blocks
  in `write_all` on a peer that stopped reading, pinning every queued frame
  behind that stalled write. An idle writer does end by itself, parked on
  `submit_rx.next()`. Detached, an evicted
  client therefore left the socket open on BOTH sides (no FIN, so the peer keeps
  its own socket and conn task) until the process exited. Measured over 5
  evictions of a never-reading peer: 5 ESTABLISHED sockets and 70 MiB of pinned
  request values, released only when the PEER closed first. Test:
  `tests/client_teardown.rs` — ablation: detaching the READER alone reds both
  cases, detaching the writer alone reds the queued-values case.

  Cancelling a task drops its future mid-op; compio holds the op's buffer until
  the cancel CQE, so nothing is freed under the kernel. Abandoning a half-written
  frame is sound BECAUSE this is a teardown — the peer sees the close right
  behind the truncated bytes, and no caller of ours is left waiting: every
  in-repo path holds its own `Rc` across the await (`&self` for the call family,
  `PinnedRecv` for the pipelined senders). That is a property of the call sites,
  not an API guarantee — an outside `send_frame` user may drop the client
  mid-await, and gets a clean `Canceled` → `ConnectionClosed`.

  On UCX a cancelled transfer LEAKS its buffer by
  design instead of freeing one UCX may still write into (`ucx_recv` /
  `ucx_send` / `ucx_send_vectored` `ManuallyDrop`; the cancel drain can bail out
  at its progress cap while UCX still holds the pointer): at most one in-flight
  recv window plus one in-flight send per closed connection, the send being the
  larger (a whole bulk frame plus its iov array) and the recv window not fixed at
  64 KiB (`read_loop` sizes an in-progress frame's window to
  `front_frame_remaining()`). Bounded per connection, against leaking the
  connection itself. UCX teardown is reasoned from the code, not measured.

  A pipelined caller that holds only the response receiver must PIN its
  connection, or an unrelated task's eviction now cancels its in-flight request:
  `stream::conn_pool::PinnedRecv` carries the `Rc` for `send_vectored` /
  `send_prepared` (replica appends), which is what scopes a connection to the
  work outstanding on it rather than to the pool entry.

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
  │                       recv_into(dest, reg) — one Stream unpack into the
  │                       slab (a memh does not remove it). Unconditional:
  │                       recv-into is never worse than decode on UCX.
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
| `AUTUMN_PS_BULK_RECV_MIN_BYTES` (partition-server) | PS recv | actual tail len | bulk-write request recv strategy — `MSG_PUT_BULK` and `MSG_BATCH_PUT_BULK` alike (`drain_bulk_writes`) |
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
no wire-struct change (`lib.rs` is not part of the wire schema) and no etcd reset.
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

## Frame length ceiling

The header gives the payload length 32 bits, so a frame LARGER than
`MAX_PAYLOAD_LEN` cannot be expressed (a frame exactly that size can — the
encoder accepts `<=` and the decoder rejects only `>`, so the two agree
exactly). Every encoder narrows through
`header_lens`, which `debug_assert!`s that bound rather than letting `as u32`
wrap silently in a debug build. A wrap is not a clean failure; the header disagrees with the
bytes behind it and the peer reports a CRC error, so the operator is told
"corrupt frame" when the truth is "too large to send". That has already cost one
misdiagnosis — an EC rebuild reading a `u32::MAX + 29,421` byte shard read as a
30 s timeout until the log timestamps disproved it.

It bounds on `MAX_PAYLOAD_LEN`, the constant the DECODER already rejects on
(`FrameError::PayloadTooLarge`), not on a second copy of `u32::MAX`: that bound
is documented as one to be lowered to a practical cap, and an encoder comparing
against the type's maximum would then build frames its own peer refuses.

One check covers both length fields: `ctrl_len` is part of `wire_payload_len`,
so an un-narrowable ctrl trips the payload bound first and a second assert would
be unreachable.

**The encoder's own check is DEBUG-ONLY, and that is the design.** Release is
`panic = "abort"`, so a release assert would kill the process rather than
unwind — and every producer of a large frame is reachable from a REMOTE
request. Trading a corrupt frame for a dead node is not an improvement. The
bound that protects production therefore lives at each PRODUCER, where it can
refuse and keep serving:

- the extent node's read path — `read_plan` bounds a read by the FILE and a log
  extent is 16 GiB, so a to-end read asks for four times the ceiling;
  `ReadRefusal::TooLargeForOneFrame` refuses it (see `crates/stream/CLAUDE.md`);
- the partition server's `MSG_BATCH_GET_BULK` — it answers N keys in ONE frame
  and the SDK's `get_many` groups EVERY same-partition key into one request, so
  a few hundred 8 MiB values cross the ceiling. `batch_bulk_budget_exceeded`
  stops the aggregation mid-loop and answers `CODE_PRECONDITION`, which the SDK
  already handles by falling back to per-key reads.

- the PS→EN group-commit append — `MAX_WRITE_BATCH_BYTES` splits the queue at
  the take point (`pending` is bounded by request COUNT, 3072, while one Put
  may be 64 MiB, so 65 large Puts crossed the ceiling on the hot write path).
  The remainder stays queued and launches next, so nothing is failed;
- `MSG_COPY_EXTENT` — `COPY_REPLY_MAX_VALUE_BYTES`; it inlines a whole extent
  when `size == 0`, and the handler answers any peer;
- `MSG_GET_REDIRECT_MANY` — `GET_REDIRECT_MANY_MAX_INLINE_BYTES`; items past
  the budget are DECLINED, which the client already proxies.

Each refusal names the fix and keeps the node serving; the debug assert is what
catches a new path in `cargo test` before it ever ships. **A new
remote-reachable producer needs its own bound — in release, nothing downstream
will catch it for you.**

Two bounds count their reply's CTRL, not just its values, because the ctrl
rides in the same payload and its size comes from the caller: the batch get
counts `keys × BATCH_GET_BULK_CTRL_BYTES_PER_KEY`, and the extent-node read
budgets for the LARGER of its two reply shapes. A value-only budget leaves a
window at large item counts — a million keys of 4 KiB fits the values and
overflows the frame.

## Wire version, and the two checks over it

`WIRE_VERSION` (currently **44**) is the schema this binary speaks.
`MIN_CLIENT_WIRE_VERSION` (**43**) is the oldest CLIENT it serves. Both are maintained
**BY HAND**. There is no schema fingerprint — hashing the sources byte for byte cost
more than it caught (a translated comment once split a rolling cluster, and each false
alarm taught the reflex of refreshing the recorded value without looking).

**Two audiences, two rules, and they are not the same question.**

| Asking | Check | Rule |
|---|---|---|
| a manager / PS / EN | `cluster_peer_compat_check(remote_max)` | `== WIRE_VERSION` |
| a client | `client_compat_check(remote_min, remote_max)` | `remote_min <= WIRE_VERSION <= remote_max` |

Peers compare for EQUALITY, so there is no "oldest cluster peer" constant — a floor
pinned to `WIRE_VERSION` would say nothing. Equality is also what keeps the
stop-the-world discipline enforced: the manager reports `MIN_CLIENT_WIRE_VERSION` in
the `wire_version_min` slot because that is what a client needs, so anything
interval-shaped would admit a stale PS or EN sitting anywhere inside the CLIENT window,
and this handshake is the only thing checking.

A client is refused at BOTH ends. Below the floor the cluster no longer keeps the
behavior it needs; above `remote_max` the cluster cannot speak what it will send, which
is routine rather than exotic — images are built from `main`, so a wheel often runs
ahead of a cluster nobody has upgraded yet. The refusal says which way round it is,
because the fix differs (deploy the cluster vs rebuild the client).

**The window is OPEN: `[43, 44]`, opened by raising the CEILING.**

Lowering the floor to 42 instead was implemented, verified green, and REVERTED — it is
unsafe, and `FIRST_WIRE_VERSION_WITH_PEER_EQUALITY` is the rule that came out of it.
Peer equality is enforced by each peer POLICING ITSELF at startup, so what decides
whether a stale server joins is the check compiled into THAT server. Before 43 that was
an interval OVERLAP against the reported pair (`wire_compat_check`, deleted in
`f17f533`), and those binaries read `wire_version_min` as a PEER floor, because when
they were written it was one. A wire-42 partition server computes
`[42,42] ∩ [43,43] = ∅` and refuses itself today; against a reported `[42,43]` it
computes `{42}` and JOINS. Nothing catches it afterwards — `RegisterPsReq` and
`RegisterNodeReq` carry no version and sit outside the gate. That is a mixed-version
cluster on the INTERNAL plane, the one thing stop-the-world exists to prevent. **The
client floor may never go below 43**, and a `const` assertion now makes it a compile
error.

Raising the ceiling is safe in every direction: a pre-43 peer's overlap misses a window
starting at 43, and a 43-or-later peer demands exact equality and never looks at the
floor.

**Verified on a live cluster, not argued from a diff.** A client built from the wire-43
commit ran put / get / head / 9 MiB bulk put / EN-direct read / range / delete against a
wire-44 cluster, byte-exact both ways. Control: with the floor moved to 44 the same
binary is refused — "this cluster speaks 44 and serves clients [44,44], the client
speaks 43 — that client is older than the window this cluster still serves".

Opening it also turned two tautologies into real assertions. While the constants were
equal, `(min, max)` and `(max, min)` were the same two numbers, so every check of the
ORDER of the reported pair — in `client_wire_admission.rs`, in the partition server's
live hello round trip — passed under a swap. Both now fail under one, which is what
`reported_wire_versions` exists to prevent in the first place.

Serving two forms of one message (`docs/client_wire_compat_design.md` §7) is a DIFFERENT
requirement, needed when a CLIENT-FACING change must keep old clients working. It is not
a precondition for the window; the rule for writing one is enforced, see below.

### `MSG_CLIENT_HELLO` (0x5F) — the client→server half, and server-side admission

`client_hello.rs`. The SDK sends it once per connection it OPENS (from `mgr_client()`
and `get_ps_client()`, not from `connect()` — `rotate_manager` and the `mgr_call` error
arm drop a manager connection and `mgr_client()` silently reopens it). Request
`[magic "AUH1": u32 LE][client_wire_version: u32 LE]`; an admitted reply is
`[server_wire_version][min_client_wire_version]`. A refusal is an ordinary error frame
carrying `FailedPrecondition` and a message that names WHICH WAY ROUND the mismatch is,
because the fix differs. Cost is one round trip per new connection, never per request.

**Hand-coded fixed-layout binary, not rkyv, and the reason is specific.** rkyv's
archived root sits at the END of its buffer, so a decoder reading a longer peer's struct
reads its SUFFIX — a two-`u64` struct decoding a three-`u64` one returns `Ok` with the
fields shifted, and a `u32` added into tail padding round-trips `Ok` in both directions
reading zero. The one message whose job is to detect a version mismatch must not depend
on its own shape to do it. The module is FROZEN for the same reason `GetClusterIdResp`
is; `tests/negotiation_freeze.rs` pins all three encodings byte for byte, and its header
says why refreshing a recorded value is the wrong response to a failure.

**Admission is scoped to msg_types, never to the connection.** `is_client_surface_ps_msg`
/ `is_client_surface_mgr_msg` are the two sets; a frame outside them is never wire-gated.
Without that scoping the first floor move is a cluster outage: the listeners that serve
clients also serve internal peers, that peer traffic is SILENT (PS→manager and EN→manager
go through `ConnPool` straight to `RpcClient::connect`; manager→PS drives split /
maintenance / merge-freeze / roll-tails the same way), and nothing in a frame says which
role sent it. The one peer that does handshake is the extent node's startup identity
check, which is a `ClusterClient` — always at `WIRE_VERSION`, so always admitted, and its
two messages are un-gated regardless. `MSG_GET_CLUSTER_ID` and the hello itself are exempt — they are how a peer
finds out what it is talking to.

**`MSG_GET_REGIONS` is deliberately NOT in the manager set**, and it is the one message
the sets cannot cover: an SDK routes with it and so does every PS's `sync_regions_once`.
Gating it would refuse region sync fleet-wide once the floor rose. Closing it properly
means teaching cluster peers to identify themselves — a different change, and the two
tests pinning this say so.

**The operator surface is deliberately uncovered too.** `MSG_STATUS`, stream/extent info,
`namespace_*`, `tenant_*`, the op-ledger, autopolicy and `MSG_MULTI_MODIFY_*` stay
reachable from a client of any version: they are `autumn-op`'s, and `autumn-op` ships
WITH the cluster at the same commit, so a window buys it nothing. The residue is that a
below-floor caller can still reach routing and the admin surface — both rkyv, so a
stale `autumn-op` gets the same silent misread the data plane is now protected from.
The trade is deliberate, not an oversight.

A connection that sends no hello is treated as `WIRE_VERSION_WITH_CLIENT_HELLO` (43,
frozen at the literal — it is a fact about history, and following `WIRE_VERSION` would
make every silent connection look current). That is what makes the mechanism INERT on
arrival: with the floor at 43, a client built the day before and one built from this
commit are admitted alike.

Where it runs: the PS inside `authz_gate`, **above** its `!gate_active()` early return
(below that line it would never run on an authz-off cluster, which is most of them —
ablated); the manager synchronously in `handle_connection`'s decode loop, before the
per-frame spawn, because a hello and the first request can arrive in one read and a
detached task would let the request be judged before the hello describing it.

**What that leaves uncovered, stated where someone will read it:** *changed the schema
and forgot to bump* is UNCAUGHT. rkyv has no version tag, so two binaries claiming the
same version with different layouts handshake happily and then decode each other's
bytes as garbage. The `compat_no_longer_verifies_the_peers_schema` test exists to keep
that hole visible in the code. The wire schema is `manager_rpc.rs`, `partition_rpc.rs`, `frame.rs`,
`extent_rpc.rs`, `cap_token.rs`; adding, removing, reordering or retyping any field of
an `Archive` type in those files — or changing what an existing field MEANS — is a wire
change.

Bump rule: bump `WIRE_VERSION` on every wire change. Deploying it is stop-the-world for
the manager, PS and EN. Raise `MIN_CLIENT_WIRE_VERSION` **only** when the change breaks
the client-facing surface — it is the one constant answering "does this force every
image carrying an embedded client to be rebuilt", and while the two are equal the
answer is always yes.

**A pure msg_type ADDITION is not a bump.** Until the hello landed the tree treated one
as a bump anyway, which is what made a new opcode expensive; an old peer that never
sends a msg_type cannot be affected by its existence. That rule change is a
precondition, not a convenience: serving two forms of a message means giving the new
form its own opcode, so a window can only be opened if opcodes are cheap. Adding a
CLIENT-facing one still means classifying it in `client_hello.rs` — a data-plane
message with no entry lands outside the window silently, which is the same shape as the
two `extract_part_id` / `authz_check` omissions this tree has already shipped.

### The client surface is frozen to exact bytes

`tests/client_surface_freeze.rs` records the encoding of every request and response form
behind a client-surface msg_type, in BOTH directions, and two more that no msg_type
would have led you to:

- `CapClaims` — no field of any wire struct; it is rkyv-encoded into the opaque token.
  The SDK decodes it out of its OWN minted token to check the namespace scope
  (`crates/client/src/lib.rs`), so its layout is a client contract all the same.
- the extent-node direct read. `--direct-read` is on by default, so a client takes the
  descriptor from `GetRedirectResp` and reads value bytes straight from an EN:
  `ReadBytesReq` (hand-coded 40 bytes) and the bulk response head. The EN has no hello
  and no version concept — §8 closes that edge from the PS side — which is a statement
  about ADMISSION and says nothing about whose bytes those are.

- the error envelope. `[status_code: u8][utf8 message]` (`RpcError::encode_status`), on
  every `FLAG_ERROR` frame from any of the three roles, decoded by every embedded client
  on every failure — including this feature's own wire-version refusal, so a break there
  is a client that cannot read why it was refused. It is keyed by no msg_type, since it
  can answer any of them, which is why a msg_type-shaped inventory does not reach it.

`ReadBytesReq` is also the tree's one message that already serves two forms, and it does
it by LENGTH rather than by opcode: `decode` reads a 32-byte request as the form that
predates the payload selector. Both widths have their own recorded row — the short one
has no encoder left that emits it, which is precisely why it is recorded rather than
derived.

An added, removed or reordered field moves the recorded bytes; for the rkyv forms an
ADDED field also fails to compile there, because those fixtures are struct literals and
Rust makes literals exhaustive, so the first signal names the field rather than a hex
string. The three HAND-CODED forms do not get that second signal — a field added and
filled inside `ReadBytesReq::new` or one of the two encoders compiles — so their layouts
are asserted offset by offset instead. A rename forces an edit there too, to the
fixture, never to a recorded value.

Numbering is frozen beside the layouts: `StatusCode`, the partition and extent-node
`CODE_*`, the payload selector, the lease kinds and invalidation reasons. A constant's
VALUE is as much a client contract as a field's offset, and no encoding catches a
renumbering — the fixtures carry `code: 7` as a literal. Pin them by NAME: asserting
`from_u8(v) as u8 == v` passes any consistent renumbering, which an ablation confirmed.

**This is the thing that makes the two-form rule real.** Before it, "a new form takes a
new msg_type" was a sentence in a design doc, and the cheap path — edit the struct, bump
the version — stayed open with nothing going red. The window mechanism is worth nothing
if the next client-facing change simply walks past it.

**Why a byte freeze here when the schema fingerprint was deleted.** The fingerprint
hashed the SOURCE of every wire module and covered the cluster-internal schema, where
editing a struct in place IS the right answer — so it fired on changes whose correct
response was "yes, I know", and that taught the reflex of refreshing the recorded value
without looking. Here both halves invert: on this surface an in-place edit is never the
right answer, and what is recorded is the ENCODING, so comments, doc edits and reordered
`use` lines move nothing. A diff in a recorded value is the rule speaking, not noise.

The one shape that can still breed that reflex is a MASS red — rkyv's archived format
moving under the whole table, through a dependency bump or a feature another crate in
the graph turns on. The two-form rule cannot express that (there is no per-message fix),
so the file's header states the answer outright rather than leaving it to be improvised
under pressure: it is a `MIN_CLIENT_WIRE_VERSION` raise and a rebuild of every embedded
client, decided deliberately, with the table re-recorded in that same commit.

`every_client_facing_msg_type_has_a_frozen_form_in_both_directions` is what keeps the
freeze from rotting: it walks both client-surface sets plus `MSG_GET_REGIONS` and the EN
read opcode, and fails on any that lacks a recorded request or a recorded response. Per
OPCODE was not enough and the review proved it by deleting `HeadResp`'s fixture and its
row — the suite stayed green, because `HeadReq` went on vouching for `MSG_HEAD`, and a
response is exactly the half an old client decodes. What the guard still cannot check is
whether a form's declared opcodes are the ones its handler actually serves; nothing ties
those lists to the dispatchers.

Exchange: both numbers ride on `GetClusterIdResp` (filled by the manager in
`handle_get_cluster_id`), checked at every long-lived process's startup
(`ClusterClient::connect`, the PS's `finish_connect`, the EN's startup). **The FIELD names outlive
the constants they carry**: `wire_version_max` carries `WIRE_VERSION` and
`wire_version_min` carries `MIN_CLIENT_WIRE_VERSION`, because the struct is frozen and
already-deployed clients read those field names with code that cannot be changed. `GetClusterIdReq/Resp` are FROZEN —
they ARE the negotiation channel, decoded before any compat decision; additions go in
new msg_types. A SUCCESSFUL response failing the check is a hard startup refusal; a
TRANSPORT failure fetching it is best-effort skipped (availability wins while the
manager is briefly down — every subsequent RPC fails loudly anyway).

`cluster_version` (manager etcd key `autumn-rs/cluster_version`, ASCII decimal) is the
operator-bumped ROLLBACK LATCH: `MSG_GET_CLUSTER_VERSION` (0x4A, fresh etcd read) /
`MSG_BUMP_CLUSTER_VERSION` (0x4B, leader-only, +1, capped at `WIRE_VERSION`,
value-CAS'd). Bump via `autumn-op upgrade-version` only after every member runs the new
binary, which is why that command prints that rollback is no longer possible.

**It gates PERSISTED formats only — never a wire format.** A wire change is settled by
the restart itself: after a stop-the-world swap every live peer speaks the same version
and no byte of the old shape exists anywhere, so there is nothing left for a gate to
decide. Stored bytes are still there when the cluster comes back, so what needs a gate
is the moment it becomes safe to START WRITING a shape the previous binary cannot read
— which is exactly "everyone is upgraded and we are not going back". `cluster_version
>= N` is that question and no other.

The cap at `WIRE_VERSION` REUSES the wire numbering so the interlock is one
comparison; it does not make this a wire version. The interlock is the latch's other
side: every manager decode of the persisted value fails closed (blocks leadership) when
it exceeds the binary's own `WIRE_VERSION`, so a rolled-back binary cannot come up
against data written past its own horizon.

Nothing in the tree gates on it yet, by design — the mechanism is in place and carries
no resident evolution code until the first persisted change actually needs it.

## Notes

- The 10-byte header eliminates HTTP/2 frame (9B) + gRPC envelope (5B) + HEADERS
  frame (~50B+): ~58B overhead vs ~200B+ for gRPC.
- `tokio::sync::{Mutex,mpsc,oneshot}` are runtime-agnostic futures — they work on
  compio without a tokio Runtime.

## Optional TCP zerocopy for prepared replicas

set_prepared_zerocopy_min_bytes configures prepared replica frames only; zero
(the default) disables it. Since every star-replicated append is prepared, this
threshold is the only size cut deciding which appends go out zerocopy — a
deployment that wants large frames only sets it there. (It is unrelated to
`COMBINE_MIN_BYTES`, which picks a checksum strategy, not a write path.) The
writer preserves its single sequential owner, IOV_MAX chunking, full CRC and
pending-response handling. Ordinary RPC writes and UCX sends retain their
paths. The threshold counts complete frame bytes.
A timed-out caller never owns the writer's buffers; send completion alone is
insufficient to recycle them. The transport awaits the separate release future.

## Connection errors versus request refusals

RpcError::is_connection_error distinguishes connection failure from Status.
All decoded peer statuses (including Unavailable and Internal) leave a framed
connection usable. Local deadlines use RpcError::Timeout(Duration), never a
synthetic Unavailable status, so pools still evict a peer that stops responding.
The bounded submit queue's local Unavailable refusal also leaves the connection
usable; callers retain their existing retry/backpressure policy. This changes
only local error types, not status codes or wire layout.

A malformed bulk prologue follows ordinary frame-decode teardown: close the
read loop and its pending receivers, yielding ConnectionClosed. It must not
synthesize an Internal Status for local CRC failure, which would make pools
retain a broken connection. The stream pool tests cover a corrupt 64 KiB bulk
response and fail if that synthetic status is restored.
