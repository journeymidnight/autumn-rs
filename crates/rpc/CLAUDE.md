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
| flags | 1B | bit 0 `FLAG_RESPONSE`, bit 1 `FLAG_ERROR`, bit 2 `FLAG_STREAM_END`, bit 4 `FLAG_AM_VALUE` (below). Bit 3 reserved (was `FLAG_CRC` pre-v28 — protection is now structural, no bit to flip off) |
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
  payload rides INSIDE ctrl, keeping in-transit CRC on WAL/SST bytes. Its
  frame-value form (below) carries the payload's crc32c in ctrl instead.

### Frame values: large value parts as UCX active messages (wire 77)

UCP Stream has no rendezvous, so every byte a stream receive takes costs the
receiving CPU one copy. With `FLAG_AM_VALUE` the frame's large value parts go
beside it as active messages instead (autumn-transport "Frame values"), which
UCX moves by rendezvous: the receiver's NIC reads them straight into its pool
buffer. The frame lists the value's pieces at the end of its ctrl region,
CRC-covered: `[(len: u32 LE, tag: u64 LE) × n][n: u16 LE]`; tag 0 is the next
piece of the stream tail (the parts under `BULK_MIN_BYTES`, one piece per run),
every other tag one part of at least `BULK_MIN_BYTES` (rendezvous moves only
contiguous memory without a copy — UCX 1.20 `rndv/get/zcopy` takes contiguous
datatypes only — so each large part is its own message). `try_decode` strips the
list (`Frame::payload` is the ctrl alone), puts the stream pieces in
`Frame::value` and the list in `Frame::am_pieces`; `peek_bulk_prologue` refuses
such a frame (its value is not the stream tail), so every bulk fast path steps
aside and the normal decode takes it. A list with an empty or a repeated
tagged piece is `Malformed` (its receiver would wait forever). A decoder with
`set_am_values(false)` refuses such frames outright (`Malformed`, the
connection drops): every manager connection (its loop never assembles a value),
an EN Client connection, and a PS client held to control messages (before
CLIENT_AUTH under authz, `ConnGateState::limit_decoder`). Those connections
have no business sending values, and a value declared and never sent would hold
the loop and its buffer, from a peer that is alive, with nothing to end the
wait — reachable from an unauthenticated client with one tiny frame. The connection's payload limit
(`set_max_payload`) covers the whole value, so an unauthenticated client
cannot make a server fetch more than it could have streamed (per frame; runs
are bounded, below).

`am_frame::AmOut` (one per connection, `for_connection(writer, peer_wire)`:
UCX and the peer at `WIRE_VERSION_WITH_AM_VALUES`) encodes such frames
(`encode` / `frame_now` / `reframe` for an already-encoded bulk head) and posts
the values. Who sends them:

| Sender | Frame | Posted |
|---|---|---|
| `RpcClient::call_vectored_bulk(_multi)` (SDK `PUT_BULK` family; values `impl Into<RegBytes>`, so a `ValueBuf`'s memh goes along) | request | by `writer_task`, just before the frame is written (the `SubmitMsg` carries them, and they count toward the send window) |
| `RpcClient::send_prepared` (PS→EN `MSG_APPEND`) | request | same |
| EN `READ_BYTES_BULK` reply (PS reads, client direct reads) | reply | when the reply is built (`frame_now`) |
| PS `GET_BULK` / `BATCH_GET_BULK` reply | reply | same (`reframe`, whose values are `RegBytes`: a value read into a pool buffer keeps its memh) |

Every receive loop decodes the frames it has buffered, starts all their values
(`am_frame::start`, which takes a buffer against the use budget — `Receive` on
the PS and EN server loops, `Reply` in `read_loop` — and registers every piece)
and then takes the frames in order, each once its value is in
(`Incoming::into_parts` / `into_frame`). A read loop never waits for a buffer:
the wait would hold up every other frame and the pings on its connection, and
the frames already started would keep the budget it waits for. With the budget
full, `WhenFull::Refuse` (the PS's loop, `read_loop`) lets the value go
(`abandon_frame_value`, its sender completes) and `IncomingError::refusal` is the
`Unavailable` error frame for its request — the PS writes it back and goes on,
`read_loop` hands it to the caller, the SDK retries; only an i/o error ends the
connection. `WhenFull::Unbudgeted` (the extent node's appends, which the PS
already bounds in flight) takes an unbudgeted pool buffer instead. Refusing also
breaks the cycle the budgets exist for: a PS that cannot take an extent node's
read reply lets it go, so the node's reply buffer is freed, not held. A value
larger than a thread may hold of the budget (up to a pool buffer) is received
unbudgeted; one larger than a pool buffer (`POOL_MAX_BUFFER_BYTES`, 256 MiB: an
SST, a WAL batch of large values) one pool buffer per piece, each charged to the
budget while there is room (so a PS taking one holds that thread's `Receive`
share while it is in flight) (transport "Large values in pieces"):
an extent node's append keeps the parts (`Frame::value_parts`, which its
`AppendReq` takes as `AppendPayload::Parts` and writes one iovec each; the
payload crc runs on across them), any other receiver gets them joined into one
heap buffer. On the way out `encode_am_frame` cuts a value part with no memh
larger than `STAGED_PIECE_BYTES` into pieces, so no piece exceeds a chunk
(`frame::tests::an_unregistered_value_goes_in_staged_pieces`)
(`tests/am_value_budget.rs`, 64 MiB cap: a 16 MiB value received by a refusing
loop; with the thread's share held, a 4 MiB value refused and a second one
taken unbudgeted; red without either fallback). `read_loop` treats a reply that
comes on the stream the same way: `recv_value_pooled` (the bulk fast path)
takes it from `Reply` (`reply_buffer`; an empty value, an error reply's, needs
none); a small reply already decoded is in memory, so refusing it would save
nothing and cost a second read — it is charged when there is room and taken
unbudgeted when not, without counting a refusal (`decoded_reply_buffer`;
asserted in the same test, red when it refuses or counts). With no buffer, a
streamed value is read past (`discard_value`: what the decoder holds, then the
socket or UCX's queued stream data) and the caller gets `Unavailable`, the
connection staying in step (`tests/reply_budget.rs`, 64 MiB cap: this thread's
share held, a 4 MiB reply refused, the next one received whole; red when the
value is not read past — the next call hangs — and when the budget is not
asked). Otherwise the
values of a burst are fetched together and the connection's frames keep their
order. `read_loop` decodes a run up to the next bulk fast-path frame (the
value lands in the pooled buffer a `call_into_pooled` caller gets; a
`call_into_dest` caller gets a copy); the EN starts every frame of a burst; the
PS runs end at a `CLIENT_AUTH`, which changes the next frame's limit
(`push_frames_to_inflight`, then routed like any value-separable request). A
run's buffers are taken before its frames pass the authz gate and back-pressure,
so at the PS and the EN a run is at most the in-flight cap of frames and one
frame limit (`max_payload`) of value bytes: an unauthenticated client of an authz
cluster gets one 1 MiB frame's worth at a time, as when values came on the
stream (a review found the unbounded run let ~2,000 tiny frames claim 2 GiB). A
frame whose value is still arriving blocks its connection's loop, as a stream
value did. A handler that skips `start` would see only the stream pieces in
`Frame::value`; every loop that can receive such frames calls it (the manager
is never sent one).

The frame-value append: ctrl = `[AppendReq header 32 B][crc32c(payload): u32
LE]`, value = the payload. `PreparedPayload` computes that crc once for all
replicas (`am_parts`, a `OnceCell`; the shared scan of the stream form is lazy
too, so a UCX append scans its payload once), and the extent node recomputes it over the
received buffer before appending (`AppendReq::decode_checked`); a mismatch is an
error frame, which the stream client treats as a failed replica (retry, new
extent). That keeps the user's CRC rule for appends — no frame CRC covers a
frame value, and RC's ICRC covers only the wire hop, not PCIe, NIC buffers or a
DMA into the wrong memory. Over TCP, and to a peer below 77, every byte is
unchanged.

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

`PreparedPayload` owns the message head (an append's 32-byte header), the body
segments (`RegBytes`: a segment from a pool buffer keeps its memh, which the
frame-value send hands UCX) and their total length. `Frame.value` is a
`RegBytes` too: `am_frame::into_frame` and `read_loop` freeze the pool buffer a
value was received into, keeping its memh (`tests/am_value_budget.rs` asserts
it; stream `tests/prepared_append.rs`
`an_append_from_a_pool_buffer_carries_its_memh` counts the append sent with
it — each red when the memh is dropped).
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
- That reserve is driven by the header alone, before the body or its CRC has
  arrived, so a connection that may only send small control messages gets a
  limit (`set_max_payload(CLIENT_CONTROL_MAX_PAYLOAD)`, 1 MiB): a Client
  connection to the manager or an extent node, and a PS Client until a
  CLIENT_AUTH succeeds while authz is on (`ConnGateState::frame_limit`). A
  header over the limit is `PayloadTooLarge` (the connection drops) and
  `front_frame_remaining` reports 0, so nothing is reserved for it. Peer/Admin
  connections (cluster secret) and authenticated PS clients are not limited:
  their frames reach gigabytes and keep the in-place receive. Not bounded:
  many authenticated connections together (no global receive budget), and any
  client of a PS with authz off. Tests:
  `frame::tests::a_header_over_the_limit_reserves_nothing`,
  `crates/manager/tests/frame_limit_admission.rs` (manager and EN, client vs
  peer), PS `an_unauthenticated_client_s_large_frame_drops_the_connection`.
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
kernel still ACKs, a flow a middlebox dropped without a RST. UCX endpoints
report a peer that closes or dies (PEER mode, see transport), but a frozen
process looks alive to them too. None of those produce the read/write error `closed` used to wait for. The 2026-09-22
incident was this: fuse reads answered EIO after 30 s for hours over connections
that were ESTABLISHED with empty queues.

Every `RpcClient` runs a third held task, `keepalive_task`, and the rule is about
SILENCE. Signs of life: any byte received (`read_loop` bumps `rx_progress` on every
read, each UCX `recv_pooled`, each 4 MiB step of a TCP bulk value — `read_value_tcp`);
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
  read that makes it is bounded by a 30 s `--reply-timeout-secs` that started earlier —
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
- **UCX.** Detection works (verified with SIGSTOP); the evicted connection's
  endpoint is closed when the client drops (transport, "Peer failure and
  endpoint close"), which completes whatever was still in flight on it.
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

**Two lanes and a send window.** The writer drains two bounded queues and
always takes the `Lane::Urgent` one first (`call_on` / `call_on_timeout`); a
frame already being written is never interrupted, and each queue is FIFO. With
`set_send_window(n)` (0 = off, the default) at most `n` LARGE normal-lane
requests (≥ 64 KiB) are written and unanswered at once; the next large one
waits in the writer (and the normal-lane messages behind it with it — FIFO
holds) while urgent ones still go. Why both: a server that admits a few
requests per connection (the PS, 4) and reads the rest in order leaves
everything else in its transport, where nothing can overtake it — UCX accepts
a stream send whether or not the peer reads. Measured on the kvcache load (one
client putting 128 x 16 MiB pages over 16 partitions while it probes): the
lane alone left the probe at p99 270 ms (it waited after leaving the client,
behind puts that filled the PS's slots); a window of 3 made it p99 8.6 ms, and
put throughput went UP (one partition 0.44–0.57 → 0.82 GB/s, 16 partitions
2.32–2.63 → 2.54–2.74). perf-check A/B against the build without either
(3 disks, 16 threads, depth 8, interleaved): every cell within noise (TCP one
partition 8M write 782/720/680 vs 723/758/703 MB/s; 4K write and read at 8
partitions on both transports), and UCX one-partition writes up (8M 502/471 →
649/653, 256K 560/570 → 760/721). Slots are released by the response, matched by
`req_id` in `read_loop` whether or not the caller still waits; the reader's
exit opens the window. Keepalive pings take the urgent lane, so a full
window cannot keep the UCX dead-peer clock (timed from the ping's write) from
starting. Limits: only large frames are windowed, so a stream of small writes
can still take the PS's spare slot; the SDK's window (3) must stay under
the PS's `--conn-inflight-cap` (16) — a cap at or under the window brings the
wait back; a request the server never answers keeps its slot until the
connection is replaced. Tests: `an_urgent_request_overtakes_queued_ones`
(ablation red: urgent sent to the normal queue),
`the_send_window_holds_large_requests_but_not_urgent_ones` (ablation red: the
ping on the normal lane).

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

  On UCX a cancelled transfer cannot be stopped; the transport keeps its
  buffers on an orphan list until UCX completes it (a pool slab then returns
  to its pool), which for a recv on a connection the peer never writes again
  is for the thread's life: at most one in-flight recv window plus one
  in-flight send per closed connection, the send being the larger (a whole bulk
  frame plus its iov array) and the recv window not fixed at 64 KiB
  (`read_loop` sizes an in-progress frame's window to
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
stubs on non-ucx). A caller that already owns memory of the value's size
receives into it instead with `call_into_dest` (below).

**read_loop dispatch (4-way)** — keyed on the req_id's `Pending` variant (which
API the caller used), NEVER on msg_type (the rpc layer stays business-agnostic;
msg_type↔API pairing is the caller's contract, enforced nowhere):

```
Pending::Frame (non-bulk call)
  → try_decode whole frame (verify header+ctrl crc) → oneshot the Frame
Pending::IntoPooled (bulk call), response frame NOT FLAG_ERROR
  ├─ UCX                → fast path: peek_bulk_prologue (verify crc, parse
  │                       code+message), consume prologue, regpool_acquire +
  │                       recv_pooled(pb, filled) — one Stream unpack into the
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
Pending::IntoDest (call_into_dest) → the same gates; the arms are in
  "Receiving into caller memory" below.
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
| `--bulk-recv-min-bytes` (partition-server) | PS recv | actual tail len | bulk-write request recv strategy — `MSG_PUT_BULK` and `MSG_BATCH_PUT_BULK` alike (`drain_bulk_writes`) |
| `handle_get_redirect` 64 KiB (partition-server) | PS route | actual clamped read len | EN-direct descriptor vs proxy read |
| `BULK_MIN_BYTES` (autumn-transport `am::send`) | UCX AM send | actual value len | rendezvous (at/above) vs eager — not UCX's own `UCX_RNDV_THRESH` |
| `BULK_MIN_BYTES` (`encode_am_frame`) | UCX frame send | each value part's len | frame value by rendezvous (at/above) vs on the stream |

`autumn_transport::BULK_MIN_BYTES` is the one constant behind the client's
`BULK_MIN_BYTES`, `TCP_RECV_INTO_POOLED_MIN_BYTES` and the AM protocol choice.

**Pooled by default (cancel-safety):** the recv runs in the long-lived
`read_loop`, NOT the caller future, and the read_loop OWNS the `PooledBuf` — a
caller-cancel/timeout just drops the buffer back to the pool, never a leak,
never a NIC writing freed memory. Callers needing the value at a specific
address copy out of the returned `PooledBuf` (`ClusterClient::get_range_into`).

### Receiving into caller memory (`call_into_dest`)

For a caller that knows the value's size and already owns the memory (the
kvcache's pages in SGLang's L2 host pool), the copy out of the pool buffer was
the second of the two copies on the read path: on loopback TCP one 16 MiB page
read cost a core 56% in the kernel receive copy and 43% in that memcpy
(bpftrace, W=1 `BatchClient.get_into`, 3.0 GB/s). `call_into_dest(msg, payload,
dest, released, timeout)` lends the memory as a `RecvDest`
(autumn-transport): an owned `IoBufMut` over the caller's pointer whose drop
resolves its `Released`.

The earlier `call_into_dest` was removed because a caller that gave up could
get its memory back while a receive still wrote it, so it had no timeout and
could hang. Ownership fixes that: the `RecvDest` sits in a `DestSlot` in the
pending entry; the read loop takes it when the value starts and drops it when
the value is in; a dropped TCP read keeps it until its completion lands. When
the call ends for any reason it takes the memory back if the receive has not
started, aborts the receive (`AbortHandle`) if it has — the connection goes
with it, its stream position lost — and returns only after `released`. That
abort fails every other request in flight on the connection
(`ConnectionClosed`; the SDK re-routes and retries them), where a pooled
receive kept the connection; it takes a timeout in the middle of a value
(first attempt 5 s, later ones `rpc_timeout`), and the clock includes waiting
behind the other values on the connection. One corner is bounded by the peer,
not by us: if io_uring has no room for the cancel, compio only warns and the
read stays posted until bytes or a close arrive. The caller's future must run
to completion (the SDK API over it is `unsafe`, `get_many_into_raw`, used by
`BatchClient`, which aborts the process if a job panics: an abandoned receive
could otherwise write a page after Python has it back).

On UCX against a wire-76 cluster the SDK sends `MSG_GET_AM` instead and the
value comes as an active message that UCX moves by rendezvous into the memory
(registered by the caller) or a pool buffer it is copied out of (below, and
autumn-client `get_am_attempt`); this call remains the path for older
clusters, for a value longer than the memory, and for unregistered memory over
256 MiB.

On UCX the direct path does not post a receive into the memory: a posted
stream recv cannot be cancelled, so a peer stalled mid-value would hold it with
no bound. It copies each chunk UCX has queued into the memory instead
(`ReadHalf::recv_data_with`, `ucp_stream_recv_data_nb` + release): one copy, the
same one a stream recv's unpack makes, and nothing is left posted when the
receive is aborted. Bytes of a chunk past the value go back to the decoder
(`FrameDecoder::feed`).

What does not take the direct path, and gets the pooled one: a value longer
than the memory (returned in `DestResp::spill`), a reply arriving after the
caller took the memory back, a small TCP reply decoded with its frame (copied
in). The memory stays in the slot until the pooled receive is done. Measured
(16 MiB pages from the PS memory tier, interleaved with the pooled build): TCP
loopback, one worker 3.0 → 5.4 GB/s with user CPU 44% → 1%, eight 18 → 27–30;
UCX (rc_mlx5, HCA loopback), one worker 3.0 → 6.4–6.7 GB/s, eight 14–15 →
20–21. One worker on UCX is at the single-core limit for receiving into memory
that is not in cache: `ucx_perftest -t stream_bw` falls from 13.7 GB/s at
16 MiB messages (its buffer stays cached) to 7.5 GB/s at 1 GiB, and a KV page
batch (128 x 16 MiB) is never in cache. Tests: `call_into_dest_receives_into_the_lent_memory` (exact fit, spill,
small frame; the server writes the prologue and a value prefix in one write —
the first version panicked on a non-zero prefix and the earlier test never sent
one), `the_value_is_received_in_place` (the first half is in the memory while
the server stalls; red with the direct path off),
`after_a_timeout_nothing_writes_the_lent_memory` (before the value and
mid-value; red when the call returns without waiting for `released`),
`a_pooled_receive_does_not_hold_the_lent_memory` (red when the memory leaves
the slot before a pooled receive). The UCX chunk read: transport
`tests/ucx_recv_data.rs` (in order; a dropped call leaves the connection
readable, red with a posted read in its place).

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
`--bulk-recv-min-bytes`, else the normal FrameDecoder path).

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
op-submit, the principal/namespace mutations, the auto-policy GET/SET pair,
read included, and `MSG_SET_RUNTIME_CONFIG`). `check_opcode` refuses them
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

`WIRE_VERSION` (currently **77**) is the schema this binary speaks.
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

## Frame values (wire 77)

perf-check ABAB against the build before it (3 disks, 16 threads, depth 8,
two runs each): UCX writes up — one partition 8M 556–702 → 857–915 MB/s, eight
2234–2237 → 2473–2553; one partition 8M read 3080–3391 → 2813–2861 (p50 253–271
→ 311 ms, p99 unchanged; at 4 threads x depth 4 the same read is 2551 → 6936);
eight-partition reads and every 4K and TCP cell within noise. The first
measurement collapsed that one-partition read to ~150 MB/s: a `.rodata` ctrl
byte in the extent node's reply (transport CLAUDE.md, "Every buffer of a UCX
send must be registrable").

Per hop (a measurement build with each hop switchable; UCX, 3 disks, 16
threads x depth 8, 8 MiB, two interleaved rounds; receiver CPU = whole-process
CPU seconds per GB moved):

| Hop (receiver) | Throughput, stream -> frame values (MB/s) | Receiver CPU s/GB |
|---|---|---|
| client -> PS write (PS) | p1 616/704 -> 708/851, p8 2165/2283 -> 2431/2342 | p1 0.85 -> 0.59, p8 1.28 -> 0.80 |
| PS -> EN append (EN) | p1 616/704 -> 762/817, p8 2165/2283 -> 2220/2168 | p1 2.32 -> 1.91, p8 3.48 -> 2.29 |
| EN -> PS read (PS) | p1 3022/3349 -> 5883/4710, p8 5875/5554 -> 7282/7221 | p1 0.35 -> 0.10, p8 0.76 -> 0.23 |
| PS -> client read (client) | p1 3022/3349 -> 3798/3682, p8 5875/5554 -> 6647/6627 | p1 0.91 -> 0.40, p8 1.38 -> 0.64 |
| EN -> client direct read (client, `BatchClient(direct=True).get_into`, 64 x 16 MiB, six pairs) | 1.30–1.75 -> 2.04–2.62 GB/s | 0.31–0.35 -> 0.14–0.17 |
| every hop | write p1 950/951, p8 2435/2467; read p1 7392/5740, p8 8074/8197 | — |

The sender's CPU drops too (the client sends 8 MiB puts at ~0.01 core instead
of ~0.6). Eight-partition writes gain only on the client hop: they are bound
by the disks' fsync.

`FLAG_AM_VALUE` and its piece list (see "Frame values" above). Sent only to a
peer negotiated at `WIRE_VERSION_WITH_AM_VALUES` (77) or later over UCX, so a
client on either side of 77 keeps working: it neither sends nor receives one
below. Client-facing (the SDK's bulk writes, PS and EN bulk read replies):
ceiling [43, 77], floor unchanged. Tests: `frame::tests::an_am_frame_*`,
`a_prepared_payload_s_am_parts_carry_the_body_crc`,
`extent_rpc::tests::a_frame_value_append_is_checked_against_its_crc`; over UCX,
stream `tests/prepared_append.rs` (appends and the read reply counted as
rendezvous receives; a payload that misses its crc refused — red with the check
off).

## A read whose value comes as a UCX active message (wire 76)

`MSG_GET_AM` (0x62): `GetAmReq` = `GetReq` + `tag` (the AM tag the client
registered for the value) + `max_len` (its memory). Reply `GetAmResp { code,
message, value_len }` as an ordinary frame; for `CODE_OK` the value has been
sent as the AM tagged `tag` (autumn-transport `am`) before the reply, and for
`CODE_VALUE_TOO_LARGE` it was not sent (`value_len` says how long it is). New
client-surface form: ceiling [43, 76], floor unchanged; the SDK sends it only
over UCX to a cluster negotiated at `WIRE_VERSION_WITH_GET_AM` or later.

## One conditional write for the SDK (wire 75)

`MSG_COMPARE_WRITE_BULK` (0x61): ctrl `CompareWriteBulkReq` (part_id,
region_epoch, key, optional expected, `delete`, expires_at, inode_hint,
lease_epoch), the value as the frame's tail (empty for a delete). It is what
`ClusterClient::compare_put` sends for every conditional write, with any mix of
TTL, lease fence and value size. `MSG_COMPARE_PUT`, `MSG_COMPARE_WRITE` and
`MSG_COMPARE_PUT_BULK` stay served for older clients (two-form rule) and can go
when `MIN_CLIENT_WIRE_VERSION` rises past 75. New client-surface form: ceiling
[43, 75], floor unchanged.

## Conditional put with a bulk value (wire 74)

`MSG_COMPARE_PUT_BULK` (0x60): ctrl `ComparePutBulkReq` (part_id, region_epoch,
key, optional expected, expires_at), the value as the frame's tail exactly like
`MSG_PUT_BULK`, so it is not capped at 64 KiB and not copied through rkyv; the
PS receives a large tail into a pooled buffer. Reply `PutResp` as for
`MSG_COMPARE_PUT`. A new client-surface form: the ceiling rises to [43, 74],
the floor stays; the SDK refuses to send it below
`WIRE_VERSION_WITH_COMPARE_PUT_BULK`, since an older PS refuses the opcode on a
client connection.

## Start an auto-policy in one write (wire 73)

`AUTOPOLICY_OP_START` (4) on `MSG_AUTOPOLICY_SET`: select `name` and set the
mode to Armed in one persisted `autoPolicy/config`. `autumn-op auto-policy
start` used to send SET_ACTIVE then SET_MODE, so a failed second call left the
policy selected but off, and another operator could interleave. No struct
changed, but what `op` may carry did, so it is a bump: a wire-72 manager would
answer op 4 InvalidArgument. Admin-only (exact wire equality), not on the client
surface; the ceiling rises to [43, 73], the floor stays. SET_ACTIVE still
selects a policy without touching the mode. In the same version a standby
manager answers `MSG_STREAM_INFO` / `MSG_EXTENT_INFO` / `MSG_NODES_INFO` with
`CODE_NOT_LEADER` instead of its frozen replay state; the PS stream client and
the EN rotate on that code.

## Write outcome unknown (wire 72)

`StatusCode::OutcomeUnknown = 9` and the batch / body code
`partition_rpc::CODE_OUTCOME_UNKNOWN = 10`: a write the partition handed to its log
stream and then gave up — it may or may not have been applied, and the partition's
reopen settles it. `Unavailable` / `CODE_UNAVAILABLE` on a write now means only "not
applied". `code_for_status` maps 9 → 10. A client predating it decodes 9 as `Internal`
and 10 as a generic error, so it retries as it did before; the client floor stays 43,
the ceiling rises to [43, 72].

## Seal a tail on partition open (wire 71)

`MSG_SEAL_TAIL` (0x69, manager, peer, leader-gated): `CheckCommitLengthReq`
→ `CheckCommitLengthResp`, `end` = the sealed length, `last_ex_info` = the
sealed tail. Seals the stream's open tail at the probe-seal length (the min over
the replicas that answer) without allocating a new one; a sealed tail is
returned unchanged. A PS opening a partition calls it for all three streams
after its takeover fence and before replay (docs/seal_length_design.md §2).
No new types. Bumped because a peer sends it: a PS paired with a manager that
predates it could not open any partition. Not on the client surface; the
ceiling rises to [43, 71], the floor stays.

## Runtime parameter overrides (wire 70)

`MSG_GET_RUNTIME_CONFIG` (0x67, leader-gated, peer or admin): empty request →
`GetRuntimeConfigResp { code, message, entries }`, each entry
`RuntimeConfigEntry { key, value, set_by, set_at }` with `key = <role>.<flag>`.
The PS and the EN fetch it in their authz poll loop. `MSG_SET_RUNTIME_CONFIG`
(0x68, leader-gated, admin only): `SetRuntimeConfigReq { key, value: Option,
set_by }` → `CodeResp`; `None` drops the override. Bumped because a peer sends
the GET: a PS or EN paired with a manager that predates it would have every
fetch refused as an unknown opcode. Not on the client surface; the ceiling rises
to [43, 70], the floor stays. `AUDIT_OP_RUNTIME_CONFIG` (18) is an appended value for the
audit rows of a set / unset.

## GC asks which extents are busy (wire 69)

`MSG_EXTENTS_IN_FLIGHT` (0x66, manager, peer, leader-gated): `ExtentsInFlightReq
{ extent_ids }` → `ExtentsInFlightResp { code, message, in_flight }`, the
requested extents with a Recovery / EC / Delete marker. The PS's GC asks it once
per run before relocating, because a punch of such an extent is refused. A pure
opcode addition, cluster-internal, bumped (unlike 0x65, which only `autumn-op`
calls) because a peer sends it: a PS paired with a manager that predates it would
have every ask refused as an unknown opcode — harmless, the punch still refuses,
but every busy extent's relocation would come back. The ceiling rises to
[43, 69], the floor stays.

## EC attempt answer is typed (wire 68)

`MSG_CONVERT_TO_EC` answers `ConvertToEcResp { code, message, state,
failed_target }` instead of `CodeResp`: `state` is `EC_ATTEMPT_STARTED` /
`EC_ATTEMPT_RUNNING` / `EC_ATTEMPT_FAILED`, and a failed attempt names the target
that failed as an index into the request's `target_addrs` (0 = the coordinator).
The manager used to read both from the message text. A failed attempt is not
started again under the same nonce; a request with a NEWER nonce supersedes the
attempt running on the coordinator. Cluster-internal only; the ceiling rises to
[43, 68], the floor stays.

## Node override as one optional field (wire 67)

`NodeStateEntry`'s five flat `override_*` fields become `operator_override:
Option<NodeOverride>` (`kind`, `reason`, `set_by`, `set_at`, `expire_at`); `None` = no
override. `NodeStateEntry::override_kind()` answers `NODE_OVERRIDE_NONE` for `None`.
`autumn-op --json list-nodes` and the dashboard overview JSON keep the flat
`override_*` keys. The ceiling rises to [43, 67], the floor stays.

## A full disk reaches autumn-op; EN reject counters gone (wire 66)

`DiskStatus` gains `full` (the EN marked the disk ENOSPC-full: it serves reads and
existing extents, takes no new ones) and `NodeStateEntry` gains `full_disks` — the
manager's copy, which `autumn-op list-nodes` / overview show. Not `DiskCapWire.full`:
`ClusterDfResp` is client surface and frozen (`client_surface_freeze.rs`). The EN's
`/metrics` keeps only its append counters; disk health, extent counts and the
authentication-refusal counters left it (the refusal counters are gone entirely).
The ceiling rises to [43, 66], the floor stays.

## Node states carry the EN metrics endpoint (wire 65)

`NodeStateEntry` gains `metrics_addr` — the address each EN's latest df answer
carried (`DfResp.metrics_addr`, wire 64). With `PsOverview.metrics_addr` it is what
`autumn-op info --file-sd` turns into the Prometheus target list. The manager's own
`/metrics` and its `/sd` are gone. The ceiling rises to [43, 65], the floor stays.

## EN df answer carries its metrics endpoint (wire 64)

`DfResp` gains `metrics_addr` (the EN's `/metrics` `host:port`, empty when not
registered or serving none); the manager keeps it in memory and lists it, with
each PS's heartbeat `metrics_addr`, in its Prometheus `http_sd` (`/sd`). Wire 63's
relay of PS/EN counters through the manager (`PartitionLoad.counters`,
`ReportPartitionLoadReq.block_cache`, `DfResp.counters`, `DiskStatus.full`,
`MSG_GET_FLEET_METRICS` 0x66) is gone: every PS and EN serves its own
process-level `/metrics` again, and the heartbeat `metrics_addr` of wire 62 is
back. The ceiling rises to [43, 64], the floor stays.

## PS heartbeat carries its metrics endpoint (wire 62)

`HeartbeatPsReq` gains `metrics_addr` (the PS's `/metrics` `host:port`, empty
without `--metrics-port`); `PsOverview` gains `metrics_addr`. The manager keeps
it in memory only, for the dashboard's metrics poller. PS → manager (Peer) and
Admin: the ceiling rises to [43, 62], the floor stays.

## Merge freeze reports its drained log end (wire 61)

`MergeFreezeResp` gains `log_tail_extent_id` / `log_end`: on a freeze OK, the
log position the drain checkpointed at (`(0, 0)` otherwise). The merge refuses
unless its capture measures exactly that — a write acked between the freeze and
the merge's takeover would be captured yet never replayed by the merged
partition. Manager → PS only (Peer): the ceiling rises to [43, 61], the floor
stays.

## No auto-policy observe mode (wire 60)

`AutoPolicyLogEntry` and `AutoPolicyGetResp.log` are removed: the log held
only what an observing (DryRun) policy would run, and that mode is gone.
`MgrAutoPolicyConfig.mode` keeps its byte values (0 off, 2 armed; 1 refused).
`MSG_AUTOPOLICY_GET` / `MSG_AUTOPOLICY_SET` join `is_admin_mgr_msg`: a Peer
(PS/EN) connection could send them before. Admin only: the ceiling rises to
[43, 60], the floor stays.

## Principal admin ops, ownerless namespaces (wire 59)

`MSG_PRINCIPAL_CREATE` / `MSG_PRINCIPAL_DELETE` keep opcodes 0x51 / 0x52 with
`PrincipalCreateReq/Resp` / `PrincipalDeleteReq` (field `principal`).
`MgrNamespace` and `NamespaceCreateReq` have no owner; `GetAuthzConfigResp`
has no protected-prefix list (its key set is public keys + registered namespaces).
All Admin/Peer: the ceiling rises to [43, 59], the floor stays.
`AllocInodesReq.volume` stays on the frozen client surface; the manager refuses
a non-empty one.

## No health verdict (wire 58)

`ExtentHealthSummaryResp.status` and its three verdict constants are gone: the
verdict looked only at extents, so it read OK with every PS down. The counts
it was derived from stay. Read by autumn-op only (Admin), not on the client
surface: the ceiling rises to [43, 58], the floor stays.

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
can give a core each, `cpuset_len` under an explicit `--cpuset` (it was
`cpuset_len / 2` while P-log and P-sst each had a core pinned, changed without
a wire bump — a cluster restarts stop-world, so old and new PS never rank side
by side), `0` without one.
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
