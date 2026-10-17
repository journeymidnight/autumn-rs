# Transport buffer ownership

TCP and UCX share a thread-local size-class pool. Runtime TCP must recycle ordinary
unregistered slabs even when the binary enables the `ucx` feature. Only UCX-runtime
registration fallbacks skip recycling, so registration can be retried later.
Eviction decrements registration counters only for registered slabs. Cross-thread
returns retain the existing home-thread ownership rule.

The integration test `tcp_recycles_buffers_including_in_a_ucx_capable_binary`
explicitly selects TCP and verifies pool hits, buffer reuse, and zero registered
bytes; run it both with and without `--features ucx`.

Current UCX I/O uses UCP Stream. A registered stable slab benefits send registration
reuse and application buffer ownership; it does not imply receiver-side end-to-end
zero-copy. Upstream UCX 1.16/1.18 Stream receive unpacks AM data into the destination.

## Compio 0.19 migration

The workspace uses compio 0.19.2 (Rust 1.95 or newer). Owned TCP split halves
are now two TcpStream handles sharing the socket; the public ReadHalf/WriteHalf
enums still constrain each task to its direction. Ordinary vectored forwarding
is unchanged. write_vectored_all_zerocopy is explicit: it waits for the buffer
release future even on send failure, advances only the acknowledged suffix on
partial sends, and falls back for unsupported operations or zerocopy resource
exhaustion. Cancellation leaves ownership with compio through the final CQE.
UCX uses its existing ordinary send. Callers must cap iovec counts.

zerocopy_tcp covers actual owned-buffer cancellation/close and segmented sends.
zerocopy module tests hold the completion notification pending and exercise
partial-send fallback without duplicated bytes. compio_features is an isolated
receive/scheduler benchmark; it does not set production runtime defaults.

The registered UCX cancellation test explicitly calls init_with(Ucx); merely
constructing UcxTransport does not enable regpool registration.

## UCX library version and two worked-around defects

`init_with(Ucx)` exits 2 on UCX < 1.17 (`check_library`, `ucp_get_version`); the
Python binding checks first and raises. Sends pass no memh, so every zcopy iov
entry (heap `Bytes`, regpool slabs alike) goes through UCX's registration cache.
In 1.16 the cache's worker fast path (`ucs_rcache_lookup_unsafe`, non-atomic
refcount under the context mt_lock only) raced the memory-event callback, which
invalidated regions on whatever thread called `munmap`/`madvise` (jemalloc purges);
a referenced region reached the GC list and was deregistered under the send →
`Local protection error`, segfault or the `refcount == 0` assertion. 1.17 added
`UCS_RCACHE_FLAG_SYNC_EVENTS` to the ucp cache, so events only queue. A 1.16 build
patched with just that flag passed the repro; vanilla 1.16 failed it.
`tests/ucx_rcache_race.rs` is the regression test: 8 sender threads with 32
head+value iov pairs from 256 single-page regions, 2 threads `madvise`ing those
pages every 2 ms, 60 s. It bypasses `init_with`, so it tests the library itself.
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
released and the mpool element is lost. `ucx_recv_raw` therefore no longer passes
the flag: `ucp_stream_try_recv_inplace` unpacks queued data straight into the
buffer and the call returns NULL with the length (the `Done` arm); a request is
built only when nothing is queued, and then it cannot complete inside the call.
The flag dated from the first `UcxConn` and had no stated reason. Found by
bisecting the `#if UCS_ENABLE_ASSERT` blocks of 1.16 (blocks on, `ucs_assert`
off → no stall; ucp/stream block off → stall), confirmed by a one-line UCX patch
(3/3 clean) and by autumn-side changes on a stock 1.20.1 (3/3 clean each).
`RECV_IN_PLACE` / `RECV_VIA_CALLBACK` count the two completion shapes;
`tests/ucx_immediate_recv.rs` has the sender write 200 messages before the
receiver posts its first recv; that recv finds nothing queued (the receiver's
worker has not progressed yet), the progress it triggers pulls a backlog into
rdescs, and the following recvs find data queued. It fails if any completed
through the callback (it did with the flag: 22–33 of 200).

Known, not fixed: `ucp_request_cancel` does nothing for stream operations (it
handles tag receives only), so `InflightSlot::drop`'s cancel path only waits for
the operation to finish on its own; on a live ep that is the `SPIN_CAP` bail-out
and the "leaking slot + request" warning, and the leaked recv stays posted on
the ep. Also PS connection-handler errors are logged at debug, which hid the
stale-status exits.
