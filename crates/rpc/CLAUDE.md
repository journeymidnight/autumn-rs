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

  **`PayloadLocation` is the WIRE form of the location byte, and nothing else
  inherits its meaning.** The same byte is persisted in two other carriers, each
  versioned by its own schema: the extent node's `.meta` sidecar at offset 41,
  governed by the `EXTMETA\x02` magic, and the manager's `extentLayout/<id>`
  etcd value. `from_wire_byte` answers `Option` and there is no lenient fold,
  because a peer knowing a location this build does not cannot reach these
  decoders — but NOT for the reason the deleted comment gave. Exact
  `WIRE_VERSION` equality covers manager/PS/EN only. An embedded CLIENT sends
  this byte too, on the default-on direct read to an extent node. What holds
  there is VERSION_HELLO at the EN itself: a client above the cluster's
  ceiling is refused before its first `ReadBytesReq`, as at the manager and the
  PS, and an admitted client may send only `READ_BYTES` / `READ_BYTES_BULK`.
  So a third location must come with a `WIRE_VERSION` bump: every client that
  can send it is then at or above that version, which an older EN refuses. Folding an unreadable byte to `InDat` is not the absence
  of an answer — `InDat` is a positive claim that `extent-{id}.dat` holds the
  payload, so the fold hands shard bytes to a caller asking for a value on
  exactly the extents whose payload has moved. `ReadBytesReq` therefore carries
  the location RESOLVED (it is hand-coded fixed-layout, so only `encode`/`decode`
  touch the byte) and refuses an unreadable one at `decode`; every other reader
  goes through `ExtentInfo::payload`, which states the refusal once per read
  path. An ABSENT selector is still `InDat` and always will be — a 32-byte
  `ReadBytesReq` is a sender that predates the field, which is a different thing
  from a present byte naming a file this build cannot name.
- **`manager_rpc.rs`** / **`partition_rpc.rs`** — manager and PS wire schemas
  (rkyv structs + `MSG_*` constants), the most-referenced surface in the crate.
  `RangeReq.start` is inclusive; callers resume strictly after a returned key
  with `key ++ 0x00`, which remains before every larger user key under the
  internal-key comparator.
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

## Dead-peer detection (`Keepalive`, `MSG_TYPE_PING = 0xFF`)

A peer can be dead while its transport is not: a frozen or wedged process whose
kernel still ACKs, a flow a middlebox dropped without a RST, a UCX endpoint with
peer-failure detection off (`UCP_ERR_HANDLING_MODE_NONE`, see transport). None of
those produce the read/write error `closed` used to wait for. The 2026-09-22
incident was this: fuse reads answered EIO after 30 s for hours over connections
that were ESTABLISHED with empty queues.

Every `RpcClient` runs a third held task, `keepalive_task`, and the rule is about
SILENCE. Signs of life: any byte received (`read_loop` bumps `rx_progress` on every
read, each UCX `recv_into`, each 4 MiB step of a TCP bulk value — `read_value_tcp`);
and on TCP, the peer's kernel ACKing more of our bytes WHILE more are queued unsent
behind them — `AckProbe`, `TCP_INFO` `tcpi_bytes_acked` / `tcpi_notsent_bytes`,
read through a local `repr(C)` prefix because the `libc` crate's glibc `tcp_info`
stops early. After `interval` (2 s)
with no sign the client queues `MSG_TYPE_PING`; once the silence has lasted
`dead_after` (8 s),
`close_silent_peer` sets `closed`, clears `pending`, releases a bulk value receive
already under way (its sender waits in `receiving`, outside `pending`), and logs
`rpc peer stopped answering`. The socket goes when the pool drops the client.
TCP: closed 8–10 s after the last sign of life (the count starts at the tick that
queues the ping); UCX: ~10–12 s (the count starts once the ping is written). Every pool replaces
the client on `is_closed()` — `stream::ConnPool`, the manager's `ConnPool`, the
SDK's `ps_conns` / `mgr_conn`.

- **Why ACKs count only with a send backlog.** Nothing comes back while a
  large frame crosses a slow link, and the ping queued behind it cannot be
  answered until the frame is through. Timing from when the ping was queued — or
  even handed to the socket: this host autotunes up to 32 MiB of send AND of
  receive buffer — closed healthy connections mid-transfer, on every retry alike
  (`a_large_request_on_a_slow_link_is_not_mistaken_for_death`). But a stopped
  process's kernel ACKs every NEW request until its receive window fills —
  thousands of small frames — so an ACK is evidence only while more of our bytes
  wait UNSENT behind it (`tcpi_notsent_bytes > 0`: window- or cwnd-limited, the
  slow-link shape). "Some bytes in flight" (`tcpi_unacked`) is not enough: the
  frozen peer's kernel delays each ACK up to ~40 ms, so under steady small traffic
  a segment is nearly always in flight at sample time
  (`caller_traffic_to_a_frozen_peer_does_not_keep_it_alive`,
  `steady_small_requests_to_a_frozen_peer_do_not_keep_it_alive`: each earlier
  rule kept a frozen peer alive as long as callers kept sending). A heavy sender
  to a frozen peer does keep a backlog while the peer's receive buffer fills, so
  its close waits for that fill (≤ 32 MiB here) plus 8 s. Bytes the peer received
  and has not read are invisible to both signals — which is what a stopped
  process looks like, and is meant to be closed.
- **Never lock `SO_SNDBUF` on an rpc socket.** The slow-link exemption needs
  unsent bytes to exist: autotuning sizes the send buffer at about twice the
  congestion window, so a window-limited transfer always leaves a backlog. A
  fixed buffer smaller than the window would put everything in flight and make a
  slow link read as silence.
- **Why the close does not cancel the reader.** On UCX the bulk receive writes
  into a borrowed pool slab; a cancel that does not complete (the drain bails at
  its progress cap) would hand the slab back to the pool while UCX may still write
  into it. The waiting caller is released from `receiving` instead
  (`a_bulk_receive_frozen_mid_value_is_released_by_the_close`).
- **UCX has no ACK counter**: silent ticks count only once the ping has been
  WRITTEN (`pings_on_wire`, bumped by `writer_task`), so a ping queued behind a
  large frame waits for it; a writer stuck behind a peer that stopped reading
  never gets the ping out, and that case stays with the callers' own deadlines.
  Same without `TCP_INFO` (non-Linux).
- **What it cannot tell apart from death:** a server connection loop back-pressured
  at its in-flight cap (PS 4, EN 64) with every one of those requests stalled
  ≥ `dead_after` — it stops reading, so nothing is answered. The SDK's first
  attempt already evicts a PS connection after 5 s in that state; for PS→EN it
  takes 64 appends stalled ≥ 8 s on one connection, and the close then reaches
  `launch_append` as a connection error (see stream CLAUDE.md).
- **Why not rely on each caller's timeout.** A caller's timeout evicts only if IT
  fires. The SDK's manager call uses the full 30 s `rpc_timeout`, and the fuse
  read that makes it is bounded by a 30 s `REPLY_TIMEOUT` that started earlier —
  so the outer timer always cancelled the call first, the dead connection was
  never evicted, and every later read (stale routing → region refresh → the same
  connection) did the same. `scripts/fuse_dead_peer_chaos.sh` scenario
  `mgr-freeze` reproduces it: every read EIO for the whole run before this,
  worst read 10.2 s then ≤0.2 s after.
- **A connection whose replies arrive at least every `interval` never pings.** A
  peer that is slow on one request but answers others (or the ping) is never
  judged; tests
  `a_slow_request_on_an_answering_peer_is_not_mistaken_for_death`,
  `an_idle_connection_stays_open_whatever_the_peer_answers_the_ping_with`.
- **Servers answer in the decode loop**, never through a dispatch that can queue:
  EN `process_frames_backpressured` (straight into `tx_bufs`), PS
  `push_one_frame_to_inflight` (before the authz gate — the ping names no
  partition and carries no data), manager `handle_connection`. Every one of
  them runs after VERSION_HELLO; no connection reaches its decode loop
  without it.
- **No wire-version bump.** Any reply proves the peer alive, including the
  `unknown msg_type` error an older server sends; the client never inspects it.
- **Cost.** Loaded connections: one `Cell` increment per socket read, no ping.
  Idle connections: an 18-byte frame each way every ~4 s. Every TCP connection:
  one `TCP_INFO` getsockopt per 2 s tick.
- **UCX.** Detection works (verified with SIGSTOP), but the evicted endpoint is
  still leaked by design (`UCP_ERR_HANDLING_MODE_NONE`, see transport); PEER
  mode is a separate decision.
- The task holds a `Weak<RpcClient>` and upgrades only between awaits, so it never
  keeps a connection alive and never drops the last `Rc` (its own `JoinHandle`)
  from inside itself.

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

## Operator-only manager ops

`is_admin_mgr_msg(msg_type)` is the set of manager ops served only on an Admin
connection (fence/remove/maintenance/EC/create-stream/upsert-partition/merge/
op-submit, and the principal/namespace mutations). `check_opcode` refuses them
on a Peer connection; an Admin connection exists only after PEER_AUTH, so this
list is the whole gate — no token rides in any request. `MSG_REGISTER_NODE` is
deliberately not on it (the EN self-registers over its Peer connection), nor is
`MSG_MULTI_MODIFY_SPLIT` (PS-driven). The PS's split/maintenance are outside the
client surface, so only Peer/Admin connections reach them.

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

`WIRE_VERSION` (currently **48**) is the schema this binary speaks.
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

**The window is OPEN: `[43, 48]`.** It was opened by raising the CEILING at
44, and widened again at 45 when `MSG_GET_CLIENT_REGIONS` arrived, at 47 when
`MSG_COMPARE_WRITE` did (46 moved only cluster-internal forms), and at 48 for
the STABLE / REPLACE / EXCLUSIVE lease modes.

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

### Two forms of one message, now live: routing

`docs/client_wire_compat_design.md` §7's rule stopped being theory at wire 45.
An embedded client's routing record dropped the three `*_stream` ids it had
never read — stream-layer identities that leaked across the layer boundary into
every embedded image — while the PARTITION SERVER still needs all seven,
because `sync_regions_once` opens a partition from them.

So both forms are SERVED, each under its own opcode:

| opcode | reply | who asks |
|---|---|---|
| `MSG_GET_REGIONS` (0x2E) | `GetRegionsResp` / `MgrRegionInfo`, 7 fields | the PS, `autumn-op`, any client below 45 |
| `MSG_GET_CLIENT_REGIONS` (0x60) | `ClientRegionsResp` / `ClientRegion`, 4 fields | an SDK at 45 or above |

**A NEW msg_type, not a second shape behind the old one.** msg_type is settled
in the frame header before any decode and a reply carries the msg_type its
request named, so both directions are self-describing. Reusing one opcode and
branching on the connection's negotiated version is what §7 forbids: a frame's
meaning would then depend on connection state, and an rkyv mis-decode is silent,
so a missed hello would read bytes at the wrong version and say nothing.

Choosing WHICH opcode to SEND from the negotiated version is a different act,
and is how the SDK decides — `negotiated_cluster_wire >=
WIRE_VERSION_WITH_CLIENT_REGIONS`. It fails CLOSED: that field is 0 until a
hello succeeds and a silent connection never raises it, so an unknown cluster
gets the old opcode, which every cluster in the window serves. A client at 45
against a cluster at 44 asks the old way and narrows the reply itself
(`From<&MgrRegionInfo> for ClientRegion`) — the one place a stream id reaches a
client and is thrown away.

The new opcode IS inside `is_client_surface_mgr_msg`, which its predecessor
could not be: `MSG_GET_REGIONS` is on both surfaces, so gating it would refuse
region sync fleet-wide, while nothing but an SDK ever sends the narrow one. The
residue that note describes is narrowed, not closed — a client old enough to
still ask with `MSG_GET_REGIONS` remains ungated.

Verified with both forms live on one cluster: a client built at wire 44 read
values a wire-45 client had written, and wrote one the wire-45 client then read.

### `VERSION_HELLO` (0xF0): mandatory connection bootstrap

`version_hello.rs` parses a frozen, bounded framing independently of rkyv and
business `FrameDecoder`. Request control is 16 bytes (`AUPH`, bootstrap version
1, role, reserved zero, wire version, client version); response control is
22+n bytes with verdict, target service, wire, client interval and a reason
of at most 256 bytes. Integers are little-endian; CRC32C covers the frozen
header and control. See `docs/cluster_version_design.md` for the byte layout.

Every `RpcClient` constructor handshakes before starting business tasks; all
manager/PS/EN listeners accept before creating a business decoder. Peer and
Admin connections then run PEER_AUTH (below) before either side starts its
business reader. Peer/admin
require equal WIRE_VERSION; client requires its declared version inside the
server interval. Connect plus Hello has a 5-second bound
(`version_hello::TIMEOUT`); the stream `ConnPool` bounds it separately from a
call's own deadline so a connect failure stays classifiable (stream CLAUDE.md,
"Bounded connect"). Malformed, missing, legacy or mismatched Hello closes the
connection without decoding a business DTO. Reconnect handshakes again; a
failed handshake never enters a pool.

`RpcClient::connect_as` boxes its connect + Hello future. Inline, that state
machine grew every caller's future: autumn-fuse's `prefetch_ahead` overflowed
rustc's layout depth limit, and a flush on the PS's 2 MiB partition thread
overflowed its stack in a debug build (boxed it runs in 1 MiB). One heap
allocation per new connection.

A version refusal (`WireMismatch` / `ClientMismatch`) is logged at WARN by the
refusing server with the peer address, the declared role and versions
(`VERSION_HELLO refused a version mismatch`), so a rollout's stale binary can
be found from either side. A malformed or missing Hello is not logged there:
port scanners and TCP health probes look exactly like that.

`Negotiated::check_opcode` checks the explicit service/role surface before
business decode or batch grouping. Role is a declaration, not a credential:
PEER_AUTH proves a Peer/Admin declaration, CLIENT_AUTH a client's principal;
cluster identity and ownership checks continue. A Client on an EN may send
`READ_BYTES`, `READ_BYTES_BULK` and `CLIENT_AUTH` (0x55, the same message the PS
takes), which binds the principal the EN requires for direct reads when the
cluster runs authz. Unknown and retired opcodes are refused. Duplicate Hello
cannot change the connection's role.

### `PEER_AUTH` (0xF1): cluster-member proof

`peer_auth.rs`, right after a successful VERSION_HELLO, on Peer and Admin
connections only (both sides know the role, so a Client connection exchanges
nothing). Same frozen bootstrap framing as VERSION_HELLO
(`version_hello::{encode_bootstrap, read_bootstrap}`), magic `AUPA`:
challenge (mode, server nonce) → proof (client nonce, HMAC) → result (verdict,
server HMAC), `HMAC-SHA256(secret, domain | side | service | role | nonces)`.
Mutual: the dialer verifies the server's MAC, so a listener impersonating a
member fails too. The secret never crosses the wire, and a recorded proof does
not pass a fresh nonce. The challenge is written back-to-back with the
VERSION_HELLO response, so the step costs one extra round trip per new
connection and nothing per request.

The secret is process-global (`install`, from `--cluster-secret-file`; same
shape as the process-global transport): one process belongs to one cluster.
Every server binary refuses to start without it (`install_for_server`). A server
without a secret (in-process tests only) answers `open`; a dialer holding a
secret refuses an open server, because that is what an impostor would answer.
Refusals log WARN with the caller's address, role and service
(`PEER_AUTH refused a connection holding a different cluster secret`,
`PEER_AUTH: connection gave no cluster-secret proof`). Design:
`docs/cluster_secret_design.md`.

A refused dial (`initiate` reports a refusal, and only a refusal, as
`PermissionDenied`) goes through `on_dial_failure` in `from_conn_as`, which
every Peer/Admin dial passes. The verdict is deterministic, since a secret is
read once and rotation is a full stop, so what happens depends on who refused,
and "who" is the dialed address, never the service the other end declared in
VERSION_HELLO (server-side dials pass `expected = None`, so a stranger on an
EN's address could otherwise declare `Manager` and end every process that
dials it). Refused by one of this process's managers (`designate_managers`,
from `--manager`, called by the PS and EN binaries), a server process (secret
installed by `install_for_server`) logs ERROR and exits 1: it is not a member.
Refused by any other address, the other end is the outsider: ERROR, and the
error is returned, so the caller handles it as an unreachable node. A dialer
that was wrong after all is refused by its manager on its next call and exits
then. The manager designates no managers and never exits on a refusal; with the
secret installed by plain `install` (in-process tests, autumn-op), nothing
exits. Tests: `autumn-server --test cluster_secret`
(`a_member_refused_by_peer_auth_exits`,
`a_member_refused_by_an_extent_node_keeps_running`).

Tests that start in-process servers and also spawn server binaries install one
test secret per process before ANY server starts (manager `tests/support`
installs it from the address pickers): a server that accepted in open mode
would be refused by a dialer that installed the secret a moment later.

### Capability-token helpers shared by the PS and the EN

`cap_token::{keyring, bind_principal, still_valid, BoundPrincipal, now_secs}`:
build the verify keyring from a polled `GetAuthzConfigResp`, verify an
CLIENT_AUTH token (signature, time window, `aud == cluster_id`), and re-check a
bound principal per request (kid still enabled, token not expired). The PS adds
its key-prefix checks on top; the EN uses them as they are (identity only).
`partition_rpc::CLIENT_AUTH_MAX_PAYLOAD` bounds the payload either server decodes.

The old `MSG_CLIENT_HELLO` (0x5F, AUH1) codec remains frozen for historical
fixtures; it no longer admits a live connection. There is no fallback to an
unchecked legacy connection. First deployment updates every caller via
stopworld; later wire changes may roll with cross-wire RPC failures and short
unavailability, subject to the release's persistence and recovery analysis.

Bump WIRE_VERSION for incompatible layouts/semantics. Raise
MIN_CLIENT_WIRE_VERSION only when client compatibility is actually removed.
A matching version cannot detect a forgotten bump. Adding an opcode still
requires updating its role surface and documenting when a client may use it;
retain old forms while they remain inside the supported client interval.

### The client surface is frozen to exact bytes

`tests/client_surface_freeze.rs` records the encoding of every request and response form
behind a client-surface msg_type, in BOTH directions, and two more that no msg_type
would have led you to:

- `CapClaims` — no field of any wire struct; it is rkyv-encoded into the opaque token.
  The SDK decodes it out of its OWN minted token to check the namespace scope
  (`crates/client/src/lib.rs`), so its layout is a client contract all the same.
- the extent-node direct read. `--direct-read` is on by default, so a client takes the
  descriptor from `GetRedirectResp` and reads value bytes straight from an EN:
  `ReadBytesReq` (hand-coded 40 bytes) and the bulk response head. The EN admits a
  client by VERSION_HELLO on the same window as the manager and PS — a statement
  about ADMISSION that says nothing about whose bytes those are.

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

`VERSION_HELLO` carries version admission on every connection. The frozen
`GetClusterIdReq/Resp` still serves identity lookup: wire_version_min reports
MIN_CLIENT_WIRE_VERSION, wire_version_max reports WIRE_VERSION, and
cluster_version is reserved and always zero. It is not a best-effort substitute
for connection admission.

The manager cluster_version latch and query/bump RPCs are removed. Opcode
0x4A/0x4B stay reserved. Persistence changes are rare and analyzed per release;
there is no global latch or mandatory common persistence codec. Preserve
cluster_id and ownership fencing. See docs/ops.md for policy pause, task drain,
replacement and recovery checks.

Manager opcode 0x34 (`MSG_MULTI_MODIFY_MERGE`, the raw merge txn) is retired and
stays reserved. It had no freeze drain, so a merge through it lost a source's
unflushed writes. Only a May 2026 client sent it, long before
MIN_CLIENT_WIRE_VERSION; no peer and no client inside the supported window
does, so retiring it changes nothing between deployed binaries and
WIRE_VERSION was not bumped. An Admin connection that sends it is refused as
an unknown opcode.

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

## Conditional publication (wire 44)

MSG_COMPARE_PUT (0x5D) carries part_id, region_epoch, key, optional expected bytes
and new bytes. PutResp CODE_PRECONDITION means comparison failed; region/ownership
errors retain frame-level status for routing refresh. Both values are capped at
64 KiB; extract_part_id and both PS namespace/authz gates decode the new request.
Every service and embedded client must be rebuilt together for wire 44.

## Cluster status (no bump)

`MSG_GET_CLUSTER_STATUS` (0x65) with the new `ClusterStatusResp`,
`FleetMember` and `FLEET_*` states. A new opcode with new types only: no peer
that predates it can send or receive them, so it is not a bump; an older
manager refuses the opcode.

## PS membership (wire 57)

`PsOverview` gains `joined_at_ms` and `evicted_at_ms` (0 = in the live
registry): the overview now lists every PS member, including evicted ones,
until an operator removes it. `MSG_REMOVE_MEMBER` (0x64, Admin-only,
`RemoveMemberReq {role, id, set_by}`, role `MEMBER_ROLE_PS`; answer
`CodeResp`) and `AUDIT_OP_REMOVE_PS` (16) are appended values. Neither is on
the client surface: the ceiling rises to [43, 57], the floor stays, and
autumn-op / the dashboard are rebuilt with the cluster.
`MEMBER_ROLE_MANAGER` (2) and `AUDIT_OP_REMOVE_MANAGER` (17) were appended
afterwards as values only, with no struct change and no bump: an older
manager answers role 2 with `CODE_INVALID_ARGUMENT`.

## Pending repair requests in the health summary (wire 54)

`ProblemSlot` gains `repair_requested: bool` and `ExtentHealthSummaryResp`
gains `repair_requested_slots: u64`, so `autumn-op health` and the dashboard
show which degraded copies are already queued to move. `MSG_EXTENT_HEALTH_SUMMARY`
is read by autumn-op only (Admin), not on the client surface: the window's
ceiling rises to [43, 54], the floor stays, and autumn-op / the dashboard are
upgraded with the cluster. `OP_KIND_REPAIR_CANCEL` (10) and
`AUDIT_OP_REPAIR_CANCEL` (14) are appended values, no struct change.

## Corrupt slots beside extent info (wire 53)

`ExtentInfoResp` gains `corrupt_slots: u32` — the manager's corrupt-slot
bitmap over `replicates ++ parity`, riding beside `extent` like
`payload_location` (the persisted `MgrExtentInfo` stays as it is). Extent nodes
read it before any whole-extent copy and never copy from a marked slot
(`crates/stream/CLAUDE.md`, "Copy sources"). `MSG_EXTENT_INFO` is internal
(manager, PS, EN), not on the client surface, so the window's ceiling rises to
[43, 53] and the floor stays; every server is upgraded together.

## PS readiness (wire 51)

`HeartbeatPsReq` gains `open_parts: Vec<(u64, u64)>` — `(part_id,
region_epoch)` of every partition the PS has open — and `PsOverview` gains
`open_count: Option<u32>` plus `PsOverview::ready()`, the one definition of a PS
serving everything it was assigned (`crates/manager/CLAUDE.md`, "PS liveness").
Neither is on the client surface: the ceiling rises to [43, 51], the floor
stays, and autumn-op is rebuilt with the cluster.

## PS core slots (wire 50)

`RegisterPsReq` and `HeartbeatPsReq` gain `slot_cap: u32` — the partitions a PS
can pin to cores, `cpuset_len / 2` under an explicit `--cpuset`, `0` without one.
`PsOverview` carries it back to autumn-op and the dashboard. None of the three
is on the client surface, so the window's ceiling rises to [43, 50] and the
floor stays; autumn-op is rebuilt with the cluster as for wire 49. Placement by
these caps is in `crates/manager/CLAUDE.md`, "Rebalance".

## Unsettled deletes in the load report (wire 49)

`PartitionLoad` gains `unsettled_deletes: u64` — deletes no major compaction
has covered yet, the input to the manager's SETTLE compaction reason
(`crates/manager/CLAUDE.md`, "Policy engine"). `PartitionLoad` travels PS →
manager in the load report and manager → autumn-op in `GetPartitionDetailResp`;
neither message is on the client surface, so the window only raises its ceiling
to [43, 49] — but an autumn-op built at wire 48 is still ADMITTED by that window
and would misdecode the shifted struct, so it is rebuilt with the cluster like
every other binary.

## Lease modes (wire 48)

`AcquireLeaseReq.mode` gains `LEASE_MODE_STABLE` (3), `LEASE_MODE_REPLACE` (4)
and `LEASE_MODE_EXCLUSIVE` (5); no struct changed, but what an existing field
may carry did, so it is a bump. `WIRE_VERSION_WITH_LEASE_MODES` = 48 gates them
in the SDK (`lease::acquire`), since a wire-47 manager answers them with
CODE_INVALID_ARGUMENT. Semantics live in `crates/manager/CLAUDE.md`.

## Fenced conditional write (wire 47)

MSG_COMPARE_WRITE (0x5E) is MSG_COMPARE_PUT plus a conditional DELETE (`value:
None`) and a fence identity (`inode_hint`, `lease_epoch`), replied with PutResp:
CODE_OK applied, CODE_PRECONDITION comparison failed, CODE_FENCED stamped epoch
below the partition's floor. A pure opcode addition, but the SDK needs a version
to gate on — an older PS has no handler — so `WIRE_VERSION_WITH_COMPARE_WRITE` =
47 and the client refuses to send it to a cluster negotiated below that
(`AutumnError::Unsupported`). The client window stays [43, 47].

## Recovery attempt protocol (wire 46)

RequireRecoveryReq and RecoveryTaskDone carry RecoveryAttempt: marker revision,
source eversion/slot/sealed length/EC shape/payload location, target node UUID
and disk identities. RecoveryTask itself keeps its encoding because the manager
inflight record persists it. MSG_VALIDATE_RECOVERY (0x61, manager control plane)
accepts RequireRecoveryReq and returns CodeResp; the EN checks it before local
adoption or rebuild, including after restart. Internal peers must all run wire
46. The client floor remains 43; no client-surface request or reply changed.
