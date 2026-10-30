# Transport buffer ownership

One process-wide pool (`ucx/regpool.rs`) serves both transports. It is made of
chunks: anonymous mappings (64 MiB, or a power of two up to 256 MiB for a larger
request; larger still is a plain heap buffer, `out_of_pool_total`), registered
once with `ucp_mem_map` when the runtime transport is UCX and never on TCP,
never unmapped or unregistered while the process lives (overflow chunks,
below, are the exception). A chunk made before
`init_with(Ucx)` is registered on its first use after it
(`tests/ucx_regpool_late_init.rs`, red when only creation registers); a failed
registration is not retried (`register_failed_total`). A chunk is mapped and
registered outside the pool lock, so other threads' slow paths do not wait for
it. Inside a chunk an
`offset_allocator::Allocator` behind a mutex hands out 4 KiB-granular ranges
(page-aligned, so O_DIRECT-aligned); a `PooledBuf` holds its chunk (`Arc`), so
the chunk's memh stays valid for every `Bytes` made from it, and its drop frees
the range under the chunk's lock on whatever thread it runs.

Each thread claims one chunk and allocates from it, so the chunk lock is
uncontended except for frees from other threads. With no room it claims a chunk
nobody holds, or makes one while the pool is under the process cap
(`--ucx-regpool-cap-bytes`, default 16 GiB, floor one 64 MiB chunk — below it
no chunk could ever be made); at the
cap it allocates from any chunk with room, claimed or not. When none has room,
an unbudgeted request (`acquire`) takes an overflow chunk: made and registered
like any chunk but outside the cap and off the chunk list, shared only by other
such requests (the pool keeps a `Weak` to it), and released when its last
buffer returns, on whatever thread that is: `ucp_mem_unmap` (from any thread:
the context is `mt_workers_shared`) then `munmap`, after which UCX's memory
hooks drop the pinned region (`over_cap_total` acquires, `overflow_made_total`
chunks, `overflow_bytes` alive; warned once). Returning an overflow range wakes
no memory waiter: only budgeted requests wait, and they cannot use it. Not
covered by a test: the last buffer dropped inside a UCX completion callback
(an abandoned frame value), which unmaps from inside worker progress. Measured (UCX, 3 disks, 8 partitions, 8 MiB,
20 s, two rounds): at a 256 MiB cap an extent node made 5–9 overflow chunks in
a run, one request each; throughput fell 30–45% against the default cap, from
the budgets (the PS refused thousands of values and replies, the node waited
for reply buffers ~5,000 times), not from overflow. At 1 GiB nothing
overflowed and throughput matched the default.
Budgeted requests never take one: the budgets keep them inside the cap. So the
cap bounds budgeted traffic, and what the unbudgeted callers (an extent node's
appends, send staging, values over a thread's share) take past it is
explicitly registered and temporary, never memory UCX registers on its own
(user decision 2026-10-30; the alternatives were keeping overflow chunks, which
leaves the process above the cap after a spike, and failing at the cap, which
an extent node's append may not). A heap buffer remains only for a request
over 256 MiB and for an overflow chunk whose mapping failed (both counted in
`out_of_pool_total`).
Liveness with the pool full: `crates/manager/tests/pool_full_liveness.rs` — an
in-process cluster (manager, two extent nodes, PS, client) on one 64 MiB pool,
six writers and six readers of 4–8 MiB log values at once; refusals happen on
`Receive` and on `Reply` (each asserted; a reply waited out counts), every request completes byte-exact,
and no gap between completions reaches 10 s (measured: about 1 s). Red when the client does not read
a refused reply past. TCP only (the in-process helpers bind TCP).
`tests/ucx_regpool_cap.rs`: at the cap a registered overflow buffer, the next
over-cap request shares its chunk, the chunk is gone once both buffers return
(one on another thread); red with the heap fallback back, and with the chunk
kept. A thread's claim ends with the thread. So every live thread that
has used the pool holds a chunk (64 MiB, pinned under UCX), whether or not it is
busy; others pass it by until the cap. At the cap every acquire that misses its
own chunk takes the pool lock and walks the chunks — the step that follows
(per-purpose quotas, waiting instead of falling back) replaces that path.

What it replaced: a per-thread free list of power-of-two slabs, each registered
on its own, with a per-thread cap, eviction of idle slabs to make room for
another size, and cross-thread drops that freed the slab while the home thread's
registered count only grew — so a thread that handed buffers to another one
(S3 gateway bodies, kvcache reads) eventually fell back to unregistered memory
for every acquire. Measured (`benches/regpool_alloc.rs`: 8 threads, 64 buffers
in flight each, 4 KiB / 64 KiB / 1 MiB / 8 MiB mix, interleaved with the old
pool): with one buffer in ten dropped on another thread, old 98–200 µs per
acquire+drop, every acquire over the cap, against 1.5–2.1 µs; with none, old
2.0–3.0 µs and 1671 MiB registered against 1.27–1.90 µs and 832–896 MiB.

### Use budgets and waiting (`acquire_for`)

`acquire_for(use, need, wait)` / `try_acquire_for` count a buffer against one of
three shares of the cap — `Receive` 40% (a client's value at the PS, an append
at the extent node), `Reply` 40% (read data, read into a reply, received,
forwarded), `Staging` 20% (bytes copied in to be sent) — and against its
acquiring thread, which may hold at most half of a share. The budgets are what
break the cross-process cycle a single "wait when full" pool would have: a PS
whose pool is full of values waiting for append acks and of read replies cannot
take an extent node's read reply, which keeps the node's reply buffers held, so
the node could take no append, so no ack ever came; with shares, an append
receive never waits on read replies being fetched. Half a share per thread keeps
one slow client (or one partition) from taking a whole use. A full budget makes
`acquire_for` wait and give `PoolError::Full` after `wait`. A wait sits on its
use's list when the budget was short, on the pool's memory list when the budget
had room but no chunk did; a returned charge wakes its use's list, returned
memory the memory list, so returns of one use do not stir waiters of another.
Each list has a generation a wake bumps; a waiter announces itself before its
last try, so a return between that try and the wait is not missed; a new chunk
(or a failed mapping giving its reservation back) wakes the memory list too. The
use budget is taken with a compare-exchange and nothing is counted when the
budget refuses, so that never makes it look full to another thread; when the
budget had room but memory did not, the charge is taken and given back, which
wakes the use's list once for nothing. More than a thread may ever hold, or more
than the largest chunk (256 MiB), is `TooLarge` at once — so a single value is
at most the smaller of 256 MiB and a tenth (`Staging`) or a fifth (`Receive`,
`Reply`) of the cap. A value between 64 and 256 MiB needs a chunk of its own
size; at the cap with only 64 MiB chunks it cannot fit and waits out `wait` to
`Full`. `acquire_many_for(use, needs, wait)` takes a batch's buffers all or none: one
charge for the total split among them, a wait that holds nothing, `TooLarge`
at once when the total is more than a thread may hold. A try that runs short of
memory part way gives back what it took without waking the memory list
(`give_back_quietly`): that is no new room, and the wake let the batch, itself
waiting on that list, wake itself and spin its whole wait (a review found it;
1.0 s of CPU over a 1 s wait). The cost, inferred and accepted: a memory waiter
on another thread that missed its try only because the batch briefly held that
room is not woken when the batch gives it back, and sleeps until the next
return or its deadline (the extent node's 1 s, then the copy path). A batch
counts a hit per buffer (and per buffer given back) against one acquire, so the
logged hit rate can read above 100%. `tests/regpool_batch.rs`: over the share refused
without waiting, a timed-out or waiting batch holds nothing, room for all wakes
it, each buffer gives its share back, a batch short of memory sleeps (thread
CPU < 200 ms over its 1 s wait); red with the total check removed — the batch
then waits its full second — and with the quiet give-back removed. The extent node's
bulk read batch is its caller. `acquire` (no budget; an overflow chunk at the
cap, a heap buffer over 256 MiB) serves the unbudgeted callers; `acquire_charged_if_room`
charges a use when it has room and is `acquire` when not (for a value already
in memory, which refusing would not save). `snapshot().uses` reports each
budget's quota, bytes in use, high water, waits, timeouts, `refused` (`Full`
with no wait: `try_acquire_for`, or `acquire_for` given no wait, as a batch
past its deadline is; a wait that runs out is a timeout) and
`too_large` (the `TooLarge` those two return; `acquire_charged_if_room` counts
neither); `log_snapshot` (`regpool_log`) puts them on one INFO line, which
the PS and the extent node emit every `--regpool-log-interval-secs`, and
`render_prometheus` (`render_regpool_metrics(prefix)`) into each one's
`/metrics`, together with each use's wait time (`wait_ns`, added by a guard
when a wait ends, a dropped one included). Test: `tests/regpool_budget.rs` (64 MiB cap:
per-thread half, uses independent, `TooLarge`, timeout counted with its wait
time, the rendered series, a waiter woken by a return on another thread, a
memory waiter woken by memory coming back; red without the per-thread check,
without the wait-time guard, and without either wake it times out).

The integration test `tcp_recycles_buffers_including_in_a_ucx_capable_binary`
explicitly selects TCP and verifies pool hits, buffer reuse, and zero registered
bytes; run it both with and without `--features ucx`. `tests/ucx_regpool_cap.rs`
(64 MiB cap): a second thread shares the first thread's chunk rather than fall
back, a request no chunk has room for falls back, a range freed on another
thread is reusable, and no chunk is made past the cap.

Current UCX I/O uses UCP Stream. A registered stable buffer benefits send
registration reuse and application buffer ownership; it does not imply
receiver-side end-to-end zero-copy. Upstream UCX 1.16/1.18 Stream receive
unpacks AM data into the destination.

## Compio 0.19 migration

The workspace uses compio 0.19.2 (Rust 1.95 or newer). Owned TCP split halves
are now two TcpStream handles sharing the socket; the public ReadHalf/WriteHalf
enums still constrain each task to its direction. Ordinary vectored forwarding
is unchanged. write_vectored_all_zerocopy is explicit: it waits for the buffer
release future even on send failure, advances only the acknowledged suffix on
partial sends, and falls back for unsupported operations or zerocopy resource
exhaustion. Cancellation leaves ownership with compio through the final CQE.
UCX stages the bytes through the pool like every stream send. Callers must cap
iovec counts.

zerocopy_tcp covers actual owned-buffer cancellation/close and segmented sends.
zerocopy module tests hold the completion notification pending and exercise
partial-send fallback without duplicated bytes. compio_features is an isolated
receive/scheduler benchmark; it does not set production runtime defaults.

The registered-pool UCX tests explicitly call init_with(Ucx); merely
constructing UcxTransport does not enable regpool registration.

## UCX library version and two worked-around defects

`init_with(Ucx)` exits 2 on UCX < 1.17 (`check_library`, `ucp_get_version`); the
Python binding checks first and raises. An active-message value that is not a
`RegBytes` from a pool slab is sent with no memh, so its memory goes through
UCX's registration cache (stream sends leave from the registered pool, below).
In 1.16 the cache's worker fast path (`ucs_rcache_lookup_unsafe`, non-atomic
refcount under the context mt_lock only) raced the memory-event callback, which
invalidated regions on whatever thread called `munmap`/`madvise` (jemalloc purges);
a referenced region reached the GC list and was deregistered under the send →
`Local protection error`, segfault or the `refcount == 0` assertion. 1.17 added
`UCS_RCACHE_FLAG_SYNC_EVENTS` to the ucp cache, so events only queue. A 1.16 build
patched with just that flag passed the repro; vanilla 1.16 failed it.
`tests/ucx_rcache_race.rs` is the regression test: 8 sender threads sending
64 KiB rendezvous values from 64 regions, 2 threads `madvise`ing a page of one
every 2 ms, 60 s. It bypasses `init_with`, so it tests the library itself.
Against a vanilla 1.16 build it segfaulted in 2 of 4 runs; 1.20.1 passes.
Fewer invalidations miss the window; more push every lookup onto the locked slow
path, where the callback queues and the race cannot happen (4 releasers: 0/4).

The second defect is in release builds up to 1.20.1 (the Ubuntu 24.04 package
enables assertions and never shows it). With `UCP_OP_ATTR_FLAG_NO_IMM_CMPL`,
`ucp_stream_recv_nbx` completes a recv whose data is already queued by building
a request and calling the callback with `req->status` inside the call;
`ucp_stream_recv_request_init` sets that field under `UCS_ENABLE_ASSERT` only, so
a recycled request reports a stale value (`UCS_INPROGRESS` on the PS's 8 MiB bulk
receives, arbitrary values on clients). That request is also never marked
completed (`ucp_request_prevent_imm_cmpl`), so `ucp_request_free` only flags it
released and the mpool element is lost. `ucx_recv_guarded` therefore no longer passes
the flag: `ucp_stream_try_recv_inplace` unpacks queued data straight into the
buffer and the call returns NULL with the length (the `Done` arm); a request is
built only when nothing is queued, and then it cannot complete inside the call.
The flag dated from the first `UcxConn` and had no stated reason. Found by
bisecting the `#if UCS_ENABLE_ASSERT` blocks of 1.16 (blocks on, `ucs_assert`
off → no stall; ucp/stream block off → stall), confirmed by a one-line UCX patch
(3/3 clean) and by autumn-side changes on a stock 1.20.1 (3/3 clean each).
`RECV_IN_PLACE` / `RECV_VIA_CALLBACK` count the two completion shapes;
`tests/ucx_immediate_recv.rs` has the sender write 200 messages and the
receiver let its progress loop queue them (100 ms) before posting its first
recv, so every recv finds its data queued; it fails unless all 200 complete in
place and none through the callback (with the flag the receive reports the
stale status and fails). It used to rely on the first recv's progress pulling a
backlog, which queued 11–38 of 200 on the per-thread pool but 0 in some runs,
and 0 in every run once stream sends were staged from the chunk pool. The UCX
protocol tables (`UCX_PROTO_INFO=y`) are the same on both pools — 4 KiB stream
sends are "multi-frag stream zero-copy copy-out" either way — so what moved the
receive-side timing is not known.

PS connection-handler errors are logged at debug, which hid the stale-status
exits.

## A dropped in-flight operation cannot be cancelled

`ucp_request_cancel` handles tag receives only; a stream send or recv stays in
flight until it completes, and a posted stream recv leaves the ep's queue only
by completing or when the ep is destroyed (`ucp_stream_ep_cleanup`). The old
guard called cancel and spun `ucp_worker_progress` 100k times waiting for a
callback that only a completion could bring; on a live ep it bailed out with a
"leaking slot + request" warning (every rpc client teardown: 397 of a
perf-check run's connections), and a `recv_into` into a borrowed pool slab
then let the slab go back to the pool while the recv stayed posted. Measured
with `tests/ucx_dropped_recv.rs` before the change: the slab was handed out
again at once and the peer's next 8184 bytes were unpacked into it, and a
later recv on the connection got the bytes after those, so 8184 bytes left the
stream silently.

Now every operation guards its buffers (`Inflight<P>`): they stay in the
caller's async frame in a `ManuallyDrop` the guard only points at (moving them
through the generic await cost 6% of loopback 64 B RTT; the pointer costs 2%),
and a receive takes its buffer by value (`UcxReadHalf::recv_pooled(pb, filled)
-> BufResult<usize, PooledBuf>`, `ReadHalf::recv_pooled`; `AsyncRead::read`
already did). A future dropped mid-await reads the buffers out of the dying
frame and moves them with request and slot to the thread's orphan list, so a
buffer whose bytes live inside the value (`[u8; N]`, `ArrayVec`, `SmallVec`
inline — compio implements `IoBuf` for them) is refused before posting, since
UCX would keep a pointer into the moved-from frame. `reap_orphans` frees orphans
whose callback has fired and the slab returns to its pool with its
registration. The callback of an orphaned slot counts the completion
(`ORPHANS_DONE`), and the reap's fast path compares that with the reaped count:
an orphan that never completes — every recv dropped on a connection the peer
never writes again, i.e. every rpc client teardown — must not put later
operations on the scan path (which allocates). Reaping runs after the progress
loop's drain and in `regpool::acquire`, which wants the slab back; the `self`
transport and the inline spin complete receives inside another call without
waking the loop, and a done orphan's slab is pushed back before the acquire
pops. An orphan still pending at thread exit stays allocated, as the
never-destroyed worker does. An orphaned recv still takes the connection's next
bytes, so the ep is marked (`recv_abandoned`) and every later recv on it fails
with `ConnectionAborted`. Droppers today: teardowns (the rpc client's held
`JoinHandle`s, the PS connection task's shutdown select) and the handshake
timeouts (`version_hello`, `peer_auth`), all of which drop the connection.
`ucx_orphan_counts()` reports this thread's orphaned / reaped operations and
list scans. Tests (`tests/ucx_dropped_recv.rs`): a dropped registered recv keeps
its slab out of the pool, the slab is not written after the drop, the slab is
reused once the bytes landed; a read after a dropped recv fails at once; a
pending orphan costs 200 later ping-pongs no list scan.

## Receiving into caller memory (`RecvDest`)

`RecvDest::new(ptr, cap)` (unsafe) lends caller memory to one receive as an
owned `IoBufMut`; its drop resolves the paired `Released`. Ownership is the
whole safety argument: compio keeps an owned buffer until a dropped read's
completion lands, so `Released` resolving means nothing writes the memory any
more. `ReadHalf::read_exact_into_dest` is the TCP read; it marks the first
`filled` bytes initialized before slicing (compio's `slice` asserts the start
is within the initialized length). UCX has no such read: a posted stream recv
cannot be cancelled. `UcxReadHalf::recv_data_with(f)` instead takes what UCX
has queued (`ucp_stream_recv_data_nb`), hands each chunk to `f` and releases
it, all inside one poll, so nothing is ever posted and a dropped call leaves
the connection readable. With nothing queued it parks on `STREAM_WAITERS`,
woken after each progress pass (the progress loop and the inline spin in
`await_pending`). Invariant: every path that can queue stream data must end in
`wake_stream_waiters` (a posted recv is woken by its own callback, a waiter here
only by those calls); the `self` transport, which delivers inside a send, is not
selected for these connections today. Tests: `tests/ucx_recv_data.rs`.

## Large values as active messages (`am`)

UCP Stream has no rendezvous, so a stream receive always costs the receiving
CPU one copy (one core into memory not in cache: ~7 GB/s). An active message
does not: `am::Sender::send(tag, value)` is `ucp_am_send_nbx` with the tag as
an 8-byte header; from `BULK_MIN_BYTES` (64 KiB) on it asks for rendezvous,
and the receiver's handler gets a descriptor and fetches the value straight into its memory
(`ucp_am_recv_data_nbx`). One handler per worker (`register_handler`, AM id 1,
`WHOLE_MSG`) looks the tag up in the thread's registry (`am::expect(tag,
dest)`): a registered buffer receives the value (rendezvous, or a copy for an
eager value); an unregistered tag is dropped by returning `UCS_OK`, which also
completes a rendezvous sender (`ucx_am.rs` checks that). This is why it is
AM and not the tag API: a tagged rendezvous nobody receives never completes,
and a caller that gave up leaves exactly that. `Expected` unregisters on drop
while the value has not arrived; `finish` waits out a rendezvous receive
already under way (it cannot be stopped), so the memory is released when it
returns. That wait is bounded by the peer, not by us: with
`UCP_ERR_HANDLING_MODE_NONE` a sender that dies between the rendezvous request
and the end of the RDMA read may never complete it (inferred, not tested), the
same window the stream path avoids by never posting into caller memory. Eager
or rendezvous is ours, not UCX's: rendezvous from `BULK_MIN_BYTES` (64 KiB,
the same constant as the SDK's bulk cut and the TCP pooled receive) on, eager
below, whatever `UCX_RNDV_THRESH` says (`ucx_am.rs` runs with it at 1 MiB and
`ucx_am_eager.rs` at 4 KiB, so each flag is checked). Below the cut the PS's
reply can overtake the value (an eager send completes locally), which is why
the client waits for the arrival after the OK.
Tags come from a per-thread counter (`next_tag`): the registry is
per receiving worker. Measured (kvcache read, 16 MiB pages from PS memory,
rc_mlx5 HCA loopback): one client worker 6.9 → 42 GB/s at ~4% CPU, eight
workers no faster (41). Tests: `tests/ucx_am.rs` (eager and rendezvous land,
counted with `ucx_am_received_counts`; a stranger is dropped and its send
completes; a given-up value writes nothing — red with no unregister on drop;
a too-long value is reported, not received), `tests/ucx_am_eager.rs` (a value
under the cut goes eager under a 4 KiB UCX threshold).

### Frame values (AM id 2)

The value of an RPC frame can travel beside it as active messages
(`am::Sender::post_frame_value`, `am::receive_frame_value`; the frame format is
autumn-rpc's `FLAG_AM_VALUE`). Unlike `MSG_GET_AM`, nobody asked for the value
in advance, so the sender picks the tag (unique per connection, from autumn-rpc's
`AmOut`) and the receiver learns of it from the frame, which may arrive before or
after the value's rendezvous request. The send sets `UCP_AM_SEND_FLAG_REPLY`, so
the handler (`on_frame_value`) gets the receiving endpoint and keys the value by
`(endpoint, tag)`:

- a registered piece is received at once (`ucp_am_recv_data_nbx`);
- one that arrives first is held: the callback answers `UCS_INPROGRESS` for the
  rendezvous descriptor (`EARLY`), and `start_frame_value` starts its receive
  when the frame asks. That receive, started outside a progress pass, then
  spins `SPIN_ITERS` like every other operation. Between two processes the
  completion event would wake the progress loop anyway; with both ends on one
  worker UCX picks the `self` transport, which raises no event, so there the
  receive does not move without the spin (the tests' "value came first" case
  hangs without it under `UCX_TLS=...,self` and passes without `self`);
- only a rendezvous request is held (a descriptor, not the value — the same
  scale as the unexpected data UCP Stream queues for an unread connection); an
  eager frame value nobody registered, or a second value under a tag already
  held, is dropped with a warning: nothing of ours sends either, and copying
  them would let any connection grow the receiver's memory without sending a
  single frame;
- one whose receiver gave up (`Expected` dropped while waiting → `ABANDONED`) is
  dropped on arrival, which completes its sender;
- a piece of another length than the frame said is refused (`InvalidData`) and
  its descriptor released;
- values held for an endpoint that goes away (`UcxEp` drop → `forget_ep`) are
  released, so their senders complete; the endpoint is remembered
  (`FORGOTTEN`) because UCX keeps it and later values still arrive on it — they
  are dropped on arrival (red without it: such a sender never completes).

`start_frame_value(reader, pieces, inline, pb)` assembles the value in `pb`, a
pool buffer the caller took for the whole length (autumn-rpc takes it against a
use budget, see "Use budgets and waiting"); when it can get none,
`abandon_frame_value(reader, pieces)` lets the value go: a piece already held is
released, a later one dropped on arrival, and either sender completes
(`ucx_am_frame.rs`, red when abandoning does nothing). In the buffer, tag-0
pieces are copied from the bytes that came on the stream, every other piece is
registered at once, and `FrameValue::wait` waits for them; a receive loop starts
the values of every frame it has buffered before waiting for the first, so they
are received together. The slab's memh is passed to the receive. The buffer is owned by
an `Rc` each in-flight receive holds, so dropping the call midway cannot free
memory under a receive. Sends ask for rendezvous (pieces are `BULK_MIN_BYTES`
or larger), and `post_frame_value` runs each send on its own task: the writer
must not wait for it, the receiver only fetches once it has read the frame.
Tests: `tests/ucx_am_frame.rs` (value before / after its frame; stream and AM
pieces joined in order; wrong length refused and its sender completes; a
given-up value, a value held for a dropped connection and one sent after it
let their senders complete; values started together waited in either order; a
64 KiB pool buffer at least 3 MiB into its chunk sent with the chunk's memh and
received whole by rendezvous).

Not handled (inferred, not tested): a sender whose spawned send fails only logs
`frame value send failed`, and the receiver's connection loop then waits for
that value until the connection is replaced (keepalive, at the client); a
receiver waiting for a value whose sender died cannot be woken by the stream,
which under `UCP_ERR_HANDLING_MODE_NONE` reports no error either — the same
dead-peer class as a stream read.

### Stream sends leave from the registered pool

The staging buffer is taken unbudgeted (`acquire`): it is taken on a
connection's writer task, where waiting for a budget would hold up that
connection's pings too, and a peer that stops reading would keep its staged
sends — and so a budget shared with other connections — from coming back.
Stream sends carry parts under 64 KiB, bounded per connection by its submit
queue.

`ucx_send` / `ucx_send_vectored` copy the parts into one pool buffer and send it
with its chunk's memh (`ucx_send_staged`). Handed the caller's memory, UCP Stream
sends 478 bytes and up zero-copy and registers every iovec through the
registration cache; memory that turns over misses that cache on every send. The
PS's reply to a 4 KiB GET is a slice of the SST block it read, and on a block
cache miss that block was just allocated: every reply paid `ibv_reg_mr` and,
once jemalloc purged the pages, `ibv_dereg_mr`, and the eight partition threads
queued on the registration lock (88–91% of a partition thread's samples in
`pthread_spin_lock`). The copy is what eager bcopy would cost, and values of
`BULK_MIN_BYTES` and up go beside the stream as active messages, so the stream
carries small parts. Measured on one host (RoCE loopback, 3 EN, 8 partitions,
16 clients x depth 8, alternating builds of the PS, and of the client too for
the memtable and load cases): flushed 4 KiB reads of a set larger
than the PS's 512 MB block cache 4.5–4.9K -> 242–247K ops/s (p99 95–109 ms ->
0.9 ms); a set in the cache 0.73–0.79M -> 1.28–1.29M; fresh keys read from the
memtable 0.96–1.09M -> 1.22–1.26M; loading 80K 4 KiB keys 10.6–15.5K -> 36K
ops/s. perf-check (`--ucx --3disk`, 20 s phases): 4 KiB write 13.2–14.0K ->
31.3–46.1K, 8 MiB write 2442–2449 -> 2656–2742 MB/s, 8 MiB read 7088–7537 ->
9566–9989 MB/s. Its 4 KiB read fell (942–966K -> 419–433K) because it reads
what its write phase wrote, and the faster writes pushed each partition past
the memtable switch into SST reads; on fixed data every 4 KiB read case above is
faster. 32 KiB values (inline on the stream, so a batched reply copies every
part): single writes 9.7–12.1K vs 11.1–12.0K ops/s and single reads 237–270K
vs 243–284K over four alternating rounds each, within each other's spread;
`--bulk 64` writes 47–50K -> 54–57K, `--bulk 64` reads 16.1–16.5K vs
15.7–18.4K.
When no chunk has room at the pool's cap, staging buffers come from an overflow
chunk (registered when it is made, freed when empty), so a send past the cap
pays a chunk registration now and then instead of one per send. The cap (process-wide,
`--ucx-regpool-cap-bytes` on every binary, 16 GiB by default and in the
transport's own default for the Python binding) is sized against: with the old
per-thread pool, 16 MiB bulk reads with the PS, the EN and the client over a
512 MiB cap fell from 8.5 to 3.0–3.7 GB/s (docs/ops.md, "Per-connection
admission and the registered pool").
`RegpoolStats::over_cap_total` counts it; the PS and EN log it with
`--regpool-log-interval-secs`.
Test: `tests/ucx_send_staged.rs` (a send and a vectored send out of `.rodata`,
which cannot be registered, arrive; red without the staging: `ibv_reg_mr`
fails with Bad address).

### A pool buffer's value carries its memh (`RegBytes`)

`PooledBuf::freeze` turns a buffer into a `RegBytes`: the filled bytes (owning
the buffer, whose chunk keeps the registration mapped) and the chunk's memh. `send_am` passes
that memh (`UCP_OP_ATTR_FIELD_MEMH`), so UCX does not look the memory up;
`From<Bytes>` carries none and falls back to the cache. `am::Sender` and
autumn-rpc's `AmOut` / `encode_am_frame` take `impl Into<RegBytes>`.
`RegBytes` derefs to its `Bytes`, `slice` keeps the memh — except an empty
slice, a static `Bytes` that no longer holds the buffer, so a memh never
outlives its chunk (`ucx_am_frame.rs`
`an_empty_slice_of_a_registered_value_keeps_no_memh`, red without) — and
`Default` is empty. `Memh` is a `NonNull`, so `RegBytes` is 40 bytes. The extent node's bulk read reply and the PS's appends of values it
received (`Frame.value`, `PreparedPayload` body) send that way; other
producers still hand plain `Bytes` (the data-path redesign converts them and
then forbids values with no memh). `am::sent_counts()` reports this thread's
sends with and without a memh, `am::sent_bytes()` the process's bytes (on the
regpool log line and `/metrics` as `*_ucx_am_sent_bytes_total{memh}`). Without a memh, UCX takes the context lock the workers share for every
cache lookup (`ucp_memh_get`); with one it skips the lookup and the lock.
Measured (`benches/am_memh.rs`: slabs sent over and over, each sender thread to
its own receiver thread, rc_mlx5, alternating arms): one sender, no difference
(~10.8 GB/s NIC-bound; sender CPU per message 64 KiB 2.5–2.6 µs, 1 MiB
7.3–7.9 µs, 8 MiB ~17.5 µs either way); eight senders, the lock shows: CPU per
message with the memh 3.3–4.6 / 8.9–9.5 / 28–29 µs against 9.4–31.8 /
13.5–90.8 / 32–38 µs through the cache, and one 64 KiB cache round fell from
~30 to 11.4 GB/s. perf-check (`--ucx --3disk`, 8 partitions, 8 MiB, 20 s,
interleaved against the build before): read 12195 / 11264 → 13736 / 13948 MB/s
(p99 528 / 704 → 466 / 482 ms), write 2707 / 2615 vs 2630 / 2597 MB/s. Test:
autumn-stream `tests/prepared_append.rs` over UCX counts the node's 65 read
replies as sent with a memh (red with `RegBytes::from(Bytes::from_owner(pb))`).

### An active-message value must be registrable: never `Bytes::from_static`

An AM value goes to UCX as the caller's memory and is registered through the
cache with write access. A literal behind `Bytes::from_static` lives in
`.rodata`, so its registration fails (`ibv_reg_mr(...) failed: Bad address`),
and under the default protocol stack the connection then stops: later sends
report done and never arrive, until the peer gives up on the connection. The
legacy stack (`UCX_PROTO_ENABLE=n`) falls back to a copy and hides it. Found
when an extent node's reply carried a static byte (one partition's 8 MiB reads
fell from ~3 GB/s to ~150 MB/s with 5–10 s stalls). `inline_storage` refuses
stack memory; nothing refuses `.rodata`, so heap-copy any constant that can
become a frame value. Stream sends copy into the pool and take anything.
