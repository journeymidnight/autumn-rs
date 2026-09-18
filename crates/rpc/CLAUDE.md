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

## Wire-version interval

`WIRE_VERSION_MIN` / `WIRE_VERSION_MAX` (currently **43/43**) declare the interval this
binary speaks. They are maintained **BY HAND**. `wire_compat_check(remote_min,
remote_max)` is purely "do the intervals overlap"; a peer reporting `max == 0`
(empty/pre-R1) is refused. There is no schema fingerprint — hashing the sources byte
for byte cost more than it caught (a translated comment once split a rolling cluster,
and each false alarm taught the reflex of refreshing the recorded value without
looking).

**What that leaves uncovered, stated where someone will read it:** *changed the schema
and forgot to bump* is UNCAUGHT. rkyv has no version tag, so two binaries claiming the
same version with different layouts handshake happily and then decode each other's
bytes as garbage. The `compat_no_longer_verifies_the_peers_schema` test exists to keep
that hole visible in the code. The wire schema is `manager_rpc.rs`, `partition_rpc.rs`, `frame.rs`,
`extent_rpc.rs`, `cap_token.rs`; adding, removing, reordering or retyping any field of
an `Archive` type in those files — or changing what an existing field MEANS — is a wire
change.

Bump rule, pre-R3 (where this tree is): bump `MAX` **and set `MIN = MAX`**. The new
version is incompatible with everything before it, deploying it is stop-the-world, and
every image carrying an embedded client must be rebuilt at the same commit. Post-R3
(frozen V1 + explicit V2 msg_types) would keep `MIN = MAX - 1`; this tree is not there
— the client runs its compat check once at connect and keeps nothing, so no call site
can gate on the negotiated version.

Exchange: the interval rides on `GetClusterIdResp` (filled by the manager in
`handle_get_cluster_id`), checked at every long-lived process's startup
(`ClusterClient::connect`, PS `finish_connect`). `GetClusterIdReq/Resp` are FROZEN —
they ARE the negotiation channel, decoded before any compat decision; additions go in
new msg_types. A SUCCESSFUL response failing the check is a hard startup refusal; a
TRANSPORT failure fetching it is best-effort skipped (availability wins while the
manager is briefly down — every subsequent RPC fails loudly anyway).

`cluster_version` (manager etcd key `autumn-rs/cluster_version`, ASCII decimal) is the
separate operator-bumped feature gate: `MSG_GET_CLUSTER_VERSION` (0x4A, fresh etcd
read) / `MSG_BUMP_CLUSTER_VERSION` (0x4B, leader-only, +1, capped at
`WIRE_VERSION_MAX`, value-CAS'd). Bump via `autumn-op upgrade-version` only after every
member runs the new binary; new wire/persisted formats gate on `cluster_version >= N`.
Every manager decode of the persisted value fails closed (blocks leadership) when it
exceeds the binary's own `WIRE_VERSION_MAX`.

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
