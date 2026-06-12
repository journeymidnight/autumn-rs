# autumn-rpc Crate Guide

## Purpose

Custom binary RPC framework built on compio (completion-based I/O, thread-per-core). Replaces tonic/gRPC to eliminate HTTP/2 framing and protobuf overhead on the hot path (extent node append fanout).

## Wire Format

10-byte frame header + payload:

```
[req_id: u32 LE][msg_type: u8][flags: u8][payload_len: u32 LE][payload bytes]
```

| Field | Size | Description |
|-------|------|-------------|
| req_id | 4B | Multiplexing ID. Client picks, server echoes. 0 = fire-and-forget. |
| msg_type | 1B | RPC method identifier (0-255 per service) |
| flags | 1B | bit 0: is_response, bit 1: is_error, bit 2: stream_end, bit 3: crc |
| payload_len | 4B | Payload size in bytes (max 512MB); includes the 4-byte CRC trailer when bit 3 is set |

Error responses encode status as: `[status_code: u8][message bytes]`.

### Per-frame CRC32C (F165; single frame protocol since F232)

There is exactly **one** frame protocol — no "V0/V1" versions, no encoder toggle,
no back-compat (the whole cluster restarts together). Every frame from
`Frame::encode` carries a 4-byte CRC32C trailer over the payload: `FLAG_CRC`
(bit 3) set, `payload_len` counts the trailer. The decoder verifies + strips it;
mismatch → `FrameError::CrcMismatch`. Rationale (vs Kafka/HDFS/Ceph, which all
ship checksums by default): a flipped `extent_id`/`eversion`/`revision` over TCP
is a silent wrong-extent write or fence bypass that TCP's 16-bit checksum + NIC
offload bugs can let through; on-disk CRC can't catch in-transit corruption. HW
CRC32C (SSE4.2) is negligible on the small control frames it now covers.

**The one CRC-less frame** is the zero-copy value response, built by
`Frame::encode_no_crc` (and hand-built in production: `partition-server::ps_zc_head`
/ `stream::zc_read_head`): `call_into_dest` / `call_into_pooled` recv the value
straight into a caller dest and cannot strip a trailer, so it omits the CRC
(FLAG_CRC unset) and relies on the transport's own integrity (UCX NIC ICRC / TCP
kernel checksum, per F219). The decoder's `FLAG_CRC` dispatch branch exists to
handle this one shape — a ZC design constraint, not a legacy version.

## Modules

### `frame.rs`
- `Frame`: encode/decode a single RPC frame
- `FrameDecoder`: streaming decoder state machine (feed bytes → try_decode frames)
- Constants: `HEADER_LEN=10`, `MAX_PAYLOAD_LEN=512MB`, flag bits

### `error.rs`
- `StatusCode`: Ok, NotFound, InvalidArgument, FailedPrecondition, Internal, Unavailable, AlreadyExists
- `RpcError`: Status, ConnectionClosed, Cancelled, Frame, Io
- `encode_status/decode_status`: wire encoding for error payloads

### `client.rs`
- `RpcClient`: multiplexed client over one TCP connection
  - `connect(addr)` → `Rc<RpcClient>`: connect + start background reader + writer tasks
  - `call(msg_type, payload)` → `Bytes`: send request, await response
  - `call_vectored(msg_type, parts)` → `Bytes`: vectored payload, zero-copy
  - `send_frame(frame)` → `oneshot::Receiver<Frame>`: low-level send
  - `send_vectored(msg_type, parts)` → `oneshot::Receiver<Frame>`: pipelined submit
  - `send_oneshot(msg_type, payload)`: fire-and-forget (req_id=0)
- **SQ/CQ architecture (R4 step 4.1, F098)**:
  - **SQ**: callers push `SubmitMsg { Single | Vectored }` onto a bounded
    `mpsc::channel(SUBMIT_CHANNEL_CAP=1024)`. A single `writer_task` owns
    `WriteHalf` and drains the queue sequentially — no cross-caller mutex.
    Back-pressure comes naturally from the bounded channel.
  - **CQ**: `read_loop` task owns `ReadHalf`, decodes frames, dispatches to
    the matching `oneshot::Sender<Frame>` in
    `Rc<RefCell<HashMap<u32, oneshot::Sender<Frame>>>>`.
- Invariants:
  - pending-insert happens **before** submit_tx.send so the CQ can't race
    in and find no entry.
  - `pending.borrow_mut()` is always scoped tight — never held across await.
  - `submit_tx` is cloned from a `RefCell` borrow (scoped), never borrowed
    across `.send().await` — avoids RefCell-across-await panics.
  - `next_req_id` skips `0` on wraparound (0 reserved for fire-and-forget).
- **F099-I-fix writer_task instrumentation**: on any write error, the
  writer_task logs `iov_count`, `total_bytes`, `errno.raw_os_error()`,
  `kind`, and the error message at WARN before exiting. This makes the
  previously opaque "submit error: connection closed" downstream cascade
  (see `stream::client::launch_append`) self-explanatory — the FIRST
  writer that encountered a kernel-level error in a stress run surfaces
  with the exact shape of the offending SendMsg, eliminating guesswork.
- **2-iov SendMsg shape is stable**: every `call_vectored` /
  `send_vectored` produces a `SubmitMsg::Vectored { bufs: [hdr, part] }`
  with exactly 2 iovecs — well under UIO_MAXIOV=1024. The writer_task
  serialises submits so concurrent callers never combine their iovs in
  one syscall. Stress-tested at 2048 concurrent futures sharing one
  writer_task in `writer_task_handles_2048_concurrent_vectored` — no
  EINVAL, no EAGAIN, all requests complete.
- **F121 closed-state flag (`closed: Rc<Cell<bool>>`)**: set true
  whenever `read_loop` or `writer_task` exits — the read EOF / write
  error / channel-closed paths all set it BEFORE clearing `pending`.
  `send_frame`, `send_vectored`, `send_oneshot` short-circuit with
  `RpcError::ConnectionClosed` when `closed.get()` is true; without
  this, a stale `Rc<RpcClient>` left in any pool would let new
  submits insert pending entries that nobody dispatches (no
  read_loop alive). Single-threaded compio guarantees the check +
  `pending.insert` run in one sync block (no awaits between them),
  so a concurrent close race resolves to either "we early-return"
  or "our entry gets cleared by `pending.clear()`". Pools should
  treat `is_closed()` as a hard "evict and reconnect" signal —
  `crates/stream/src/conn_pool.rs::get_client` does this.

- **F216-E zero-copy receive-into-dest (`call_into_dest`)**: a second
  `Pending` variant `IntoDest` alongside `Frame`. `call_into_dest(msg_type,
  payload, dest: *mut u8, dest_cap, reg: Option<&RegisteredMem>) -> DestMeta`
  reads the response value straight into `dest` with no intermediate Vec:
  - Wire: a **V0** response frame whose payload is `[ZC meta][value]`, where
    `encode_zc_meta(code, value) = [code:1][value_len:4 LE][reserved:4 LE]`
    (`ZC_META_LEN = 9`). The 3rd field was the value crc32c; **F219 removed the
    ZC value crc** (it cost a full crc32c pass per value and duplicated the
    transport's own integrity), so the field is now reserved/0 — the 9-byte
    layout is kept for wire-compat. `DestMeta { code, value_len }` is returned to
    the caller; `value` lands in `dest[..value_len]`.
  - `read_loop` dual-path on the matching `req_id`: **UCX** → `peek_header`
    + `drain_into` the buffered meta prefix out of the `FrameDecoder`, then
    `ReadHalf::recv_into(&mut dest[filled..], reg)` for the value remainder
    (memh RDMA when `reg=Some`); **TCP / non-UCX** `call_into_dest` → normal
    `try_decode` then `finish_into_dest_from_frame` (memcpy `payload[9..]` into
    dest). **`call_into_pooled` on TCP (F219)** instead recvs the value (≥ 64 KiB,
    `TCP_RECV_INTO_POOLED_MIN_BYTES`) straight into the read_loop-owned `PooledBuf`
    via `ReadHalf::read_exact_into_pooled` (a compio owned read) — no FrameDecoder
    accumulation copy. No value crc is verified anywhere (F219); integrity is the
    transport's (UCX NIC ICRC / TCP kernel checksum). Normal (non-ZC) frames keep
    their V1 frame-CRC (F165). All other msg_types go through the untouched
    `Pending::Frame` path → **TCP fully compatible, no regression**.
  - `RegisteredMem` is re-exported from `autumn-transport` (uninhabited stub on
    non-ucx builds, so `reg` is always `None` there and the code compiles
    uniformly).
  - **Cancel-safety:** the recv-into-`dest` happens in the long-lived
    `read_loop` task, NOT the caller future. It is safe ONLY when `dest`
    outlives the call and the call is not dropped mid-recv (no per-call
    timeout). The client←PS GET (`ClusterClient::get_into`) satisfies this
    (no timeout; the sglang page outlives the batch). A path that needs a
    timeout / failover (e.g. PS←EN) must instead use the planned
    read_loop-owns-the-PooledBuf-and-hands-it-back variant — see
    `feature_list.md` F216-E "Remaining".
  - **Write counterpart (`MSG_PUT_ZC`)** needs no new RPC primitive: the client
    sends `[meta][value]` via the existing `call_vectored` (value = its own
    iovec, zero-copy via rcache when its memory is registered; V1 frame CRC
    covers `[meta||value]`). The value-separable framing lives in
    `partition_rpc` (`encode/parse_put_zc_meta`); the PS slices the value
    zero-copy out of the reassembled frame. So WRITE zero-copy is send-side
    framing only; READ zero-copy needs `call_into_dest` because the value must
    land in a specific caller dest.

### `server.rs`
- `RpcServer::new(handler)`: create server with async handler `Fn(u8, Bytes) -> Result<Bytes, (StatusCode, String)>`
- `serve(addr)`: accept loop on dedicated OS thread → dispatch to compio worker threads via `Dispatcher`
- Each connection: read frames → spawn handler per request → write response
- Thread-per-core: `std::net::TcpStream` (Send) accepted on accept thread, dispatched to worker, converted to `compio::net::TcpStream` (!Send) on worker

### `pool.rs`
- `ConnPool`: per-address `Arc<RpcClient>` pool with heartbeat
  - `connect(addr)`: get or create client (no heartbeat)
  - `connect_with_heartbeat(addr)`: get or create + start ping loop
  - `is_healthy(addr)`: check last pong within 8s window
- Heartbeat: periodic `MSG_TYPE_PING` (0xFF) calls every 2s

## Architecture

```
Server side:
  OS thread (accept) → channel → compio Dispatcher → worker threads
  Each worker: compio Runtime → handle_connection → spawn per-request handlers

Client side:
  RpcClient = writer Mutex + background reader task
  Multiplexing: DashMap<req_id, oneshot::Sender>
  ConnPool = DashMap<SocketAddr, Arc<RpcClient>>
```

## Usage Pattern

```rust
// Server
let server = RpcServer::new(|msg_type, payload| async move {
    match msg_type {
        1 => Ok(handle_append(payload)),
        _ => Err((StatusCode::InvalidArgument, "unknown".into())),
    }
});
server.serve(addr).await?;

// Client
let client = RpcClient::connect(addr).await?;
let resp = client.call(1, payload).await?;
```

## Key Design Decisions

1. **10-byte header vs gRPC**: Eliminates HTTP/2 frame (9B) + gRPC envelope (5B) + HEADERS frame (~50B+). ~58B total overhead vs ~200B+ for gRPC.
2. **std::net accept + compio dispatch**: compio's TcpStream is !Send (Rc<Inner>). Accept with std (Send), dispatch raw fd to worker, convert to compio on worker thread.
3. **tokio::sync for locking**: tokio::sync::Mutex/mpsc/oneshot are runtime-agnostic futures. Work correctly on compio without needing tokio Runtime.
4. **req_id=0 for fire-and-forget**: No response routing, handler runs but response is not written.
5. **MSG_TYPE_PING=0xFF reserved**: Health check protocol built into the framework.

## WIRE-1 — wire-schema fingerprint (2026-06-12)

`build.rs` hashes the wire-schema SOURCE files (`manager_rpc.rs`,
`partition_rpc.rs`, `frame.rs`, `../stream/src/extent_rpc.rs`) into
`autumn_rpc::WIRE_FINGERPRINT` (16-hex compile-time const). Rationale:
deploys are SAME-COMMIT (rkyv has no cross-version compatibility) and a
mixed deploy fails SILENTLY with garbage decodes — the F275 stale python
wheel decoded `PutReq` with `part_id=0` and every write failed with
nothing pointing at the cause. Hashing the schema source (not the git
commit) keeps dev flows sane: unrelated code edits don't perturb it; any
wire-struct edit does.

Exchange: `GetClusterIdResp.wire_fingerprint` (filled by the manager in
both arms of `handle_get_cluster_id`). Checks at startup of every
long-lived process via `wire_fingerprint_check`:
- `ClusterClient::connect` (covers autumn-client/op, fuse, ioring, the
  python wheel — the F275 shape — and the EN's cluster_id verify which
  connects through it),
- PS `finish_connect` (own pool path).
Semantics: a SUCCESSFUL response with a different (or empty = pre-WIRE-1)
fingerprint is a HARD startup refusal with an actionable message; a
TRANSPORT failure fetching it is best-effort-skipped (availability wins
while the manager is briefly down — every subsequent RPC fails loudly
anyway). NOTE for a future rolling-upgrade design: this check is the
enforcement point to relax once a real wire-compat story exists.
