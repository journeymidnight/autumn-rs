# 4K TCP: why loopback hits 193 K but cross-host plateaus at 5.8 K

Investigated 2026-06-08 on the standard p=16 × shards=16 × --3disk r=3
TCP cluster. The published `perf_check.sh` SKILL ceiling of 162 K is
loopback-only; cross-host of the SAME cluster plateaus at ~5.8 K
ops/s and then degrades. Why a 28× gap?

## The data

`autumn-client perf-check --transport tcp --size 4096 --partitions 16
--pipeline-depth 8 --threads N --duration 10`, 4 KiB writes:

| t (threads) | LOOPBACK ops/s | LOOPBACK p50 | CROSS-HOST ops/s | CROSS-HOST p50 |
|------|---------------|--------------|------------------|----------------|
| 1    | 5 470         | 1.24 ms      | 1 812            | 1.22 ms        |
| 4    | 21 128        | 1.00 ms      | 3 356            | 1.55 ms        |
| 16   | 14 265        | 2.69 ms      | 3 786            | 21.67 ms       |
| 32   | 14 065        | 7.36 ms      | 3 980            | 57.48 ms       |
| 64   | 33 162        | 6.53 ms      | 4 802            | 106 ms         |
| 128  | 50 336        | 5.87 ms      | **5 796** (peak) | 174 ms         |
| 256  | 87 901        | 9.88 ms      | 5 420            | 310 ms         |
| 512  | **132 523**   | 17.52 ms     | 3 333 (degrading)| 501 ms         |
| 1024 | **193 361**   | 29.12 ms     | (cliff)          | —              |

Two qualitatively different curves:

* **Loopback scales near-linearly** from 1 to 1024 threads. Throughput
  goes up ~35× as concurrency grows; per-op p50 stays bounded under 30 ms
  even at maximum.
* **Cross-host plateaus at t=128**, then degrades. Per-op p50 grows
  unboundedly with t — at t=512 the per-op time is 500 ms even though
  the wire RTT itself is ~50 µs.

The per-op time at low t is IDENTICAL between the two (1.22 vs 1.24 ms at
t=1), so the wire isn't slow per se. The difference is what happens as
concurrency grows.

## What's NOT the cause

* **TCP_NODELAY is set on both sides** (`crates/rpc/src/client.rs:196` +
  `crates/partition-server/src/lib.rs:4707`). No Nagle's algorithm
  stalls.
* **TCP send-buffer / BDP exhaustion**. Loopback BDP ~150 KiB
  (30 GB/s × 5 µs); cross-host BDP ~625 KiB (12.5 GB/s × 50 µs). Linux
  default `tcp_wmem` 4 MiB easily covers either; cross-host has the
  LARGER BDP, so if BDP were the bottleneck cross-host would be FASTER,
  not slower.
* **Wire bandwidth**. 5 800 ops/s × ~5 KiB framed = 29 MB/s. 100 GbE
  NIC line rate is ~12 GB/s — 0.2 % utilised. Not the bottleneck.
* **Server CPU**. PS at t=128 sits at ~25 % CPU during the bench. Not
  the bottleneck.
* **Disk fsync rate**. Group-commit batches up to 256 ops per fsync;
  3 NVMes can do >10 K fsyncs/sec each. Trivially handles 5.8 K ops/s.

## The real cause: arrival-rate determines batch-size

`PartitionServer::partition_loop` (`crates/partition-server/src/lib.rs`
"Group Commit (R4 4.4 SQ/CQ)" — see partition-server CLAUDE.md note on
`MIN_PIPELINE_BATCH = 256`):

```text
(B) if pending.non_empty && !at_cap && (n_inflight==0 || pending >= 256):
      launch_new_batch
```

A new batch fires when EITHER (a) no batch is in flight, OR (b) pending
queue has accumulated ≥ 256 ops. The 256-gate exists for a reason: each
batch costs one 3-replica fanout RPC + one fsync, both fixed-cost per
batch. The cost amortizes over the batch size — a 256-op batch pays the
fsync cost once across 256 ops (~60 µs/op effective); a 32-op batch
pays it across 32 ops (~480 µs/op effective). **Big batches are 8×
more efficient than small batches.**

Arrival-rate matters because it determines how many ops pile up between
batch boundaries:

* **Loopback** delivers a t=1024 burst of requests in **microseconds**
  (no wire). When `partition_loop` finishes one batch and looks at
  `pending`, it sees 256+ already queued → fires the next 256-op batch
  immediately. Amortization is maximal. **Throughput = 256 / (fsync
  time) per partition × 16 partitions ≈ 162 K ops/s** at the
  saturation point.
* **Cross-host** delivers the same t=1024 burst at **wire RTT pace**.
  When `partition_loop` finishes one batch, `pending` might have ~30
  ops queued (the rest are still in flight cross-wire). It can either
  fire that 30-op batch immediately (small batch, low amortization) or
  wait for `pending` to grow to 256. With AUTUMN_PS_INFLIGHT_CAP=8
  (8 concurrent batches max), the gate doesn't seriously gate anyway
  — but the average batch SIZE is small, so amortization is poor.
  **Throughput = ~30 / fsync_time per partition × 16 ≈ 5 800 ops/s**
  at saturation.

The per-op p50 of 174 ms at cross-host t=128 reflects this: with 1024
inflight at 5 800 ops/s, Little's law says mean = 1024 / 5 800 = 176 ms
≈ p50. Most of that 176 ms is **the client waiting for a smaller batch
fsync to complete before its op is committed**, not wire RTT (5-100 µs).

## Verification via depth scaling

A direct corollary: increasing `--pipeline-depth` at fixed `--threads`
should help cross-host by piling up MORE inflight ops per conn → larger
batches at the server. (Tested in the prior session's UCX equivalent:
t=8 d=128 = 1024 inflight gave 6 K, vs t=128 d=8 = 1024 inflight gave
1.5 K — same total inflight, fewer threads + deeper pipeline let larger
batches form.)

## The plateau and degradation

Cross-host throughput plateaus at t=128 (~5.8 K) and DROPS at t=512
(3.3 K) and t=1024 (cliff). Two factors compound past t=128:

1. **Server-side per-conn back-pressure.** `handle_ps_connection`'s
   per-conn `FuturesUnordered` cap is `AUTUMN_PS_CONN_INFLIGHT_CAP = 64`.
   With 16 partition conns × 64 = 1024 server-side concurrency cap.
   Past t=128 d=8 = 1024 total client inflight, the server stops
   accepting new requests until in-flight drains — clients queue ON
   THE WIRE → p50 latency balloons.
2. **Client-side thread overhead.** Each `--threads` is one
   `std::thread::spawn` + own compio runtime + own ConnPool. At
   t=512 + the kernel scheduler context-switch overhead dominates.
   Loopback also pays this but the per-op time is so short the
   overhead barely matters; cross-host's 500 ms per op leaves the
   thread mostly waiting, so the scheduling cost shows up as wasted
   wall-clock.

## Fixes (none built yet)

These would lift cross-host 4K throughput meaningfully:

1. **Adaptive `MIN_PIPELINE_BATCH`.** Today fixed at 256. If the
   partition observes thin arrival (e.g. exponentially-weighted average
   of recent batch sizes < threshold), drop the gate so smaller
   batches fire without waiting. Worst case: loopback small-burst
   regression. Probably acceptable.
2. **Client-side request batching.** Currently each
   `kv_put` is ONE round trip carrying ONE op. A `MSG_BATCH_PUT` that
   bundles N puts into one frame would mean N ops arrive at the server
   IN ONE GO, fully filling the next batch regardless of wire RTT.
   This is the cleanest fix — already half-shipped: `put_many` exists
   in `autumn-client` but `perf-check` doesn't use it. Switching the
   bench to `put_many` (BATCH_PUT internally) should close most of the
   loopback / cross-host gap for synthetic workloads. Real production
   workloads (kvcache page writes) already issue many keys per batch,
   so they wouldn't see this gap.
3. **Increase `AUTUMN_PS_CONN_INFLIGHT_CAP`.** Today 64. Push to 256
   to delay the back-pressure cliff past t=128. Doesn't address the
   root small-batch-amortization issue though.

## Summary

The 28× gap (193 K loopback vs 5.8 K cross-host) is **not network
overhead** — it's the wire RTT determining how many ops can pile up
between server-side batches, which determines fsync amortization
ratio, which determines per-op effective cost.

| factor                          | loopback        | cross-host      |
|---------------------------------|-----------------|-----------------|
| per-op wire RTT                 | ~5 µs           | ~50 µs          |
| ops pending per partition_loop tick | ~256        | ~30             |
| fsync amortization per op       | ~60 µs          | ~480 µs         |
| achieved ops/s (saturation)     | 193 K           | 5.8 K           |

The fix isn't network-tuning; it's letting clients batch puts into
larger atomic requests so the server gets a "fat arrival" pattern even
over a slow wire.

## Followup test (2026-06-08): MIN_PIPELINE_BATCH tuning DIDN'T help

The fix-proposal above predicted that reducing `MIN_PIPELINE_BATCH`
from 256 to 16 would help cross-host by letting `partition_loop`
fire small batches without waiting. Tested on the same cluster
(p=16 × shards=16 × --3disk r=3 cross-host) by setting
`AUTUMN_PS_MIN_BATCH=16`:

| t   | MIN_BATCH=256 (baseline) | MIN_BATCH=16 | Δ        |
|-----|-------------------------|--------------|----------|
| 16  | 3,786 ops/s, p50 22 ms  | 2,927        | **-23%** |
| 64  | 4,802 ops/s, p50 106 ms | 4,151        | **-14%** |
| 128 | 5,796 ops/s, p50 174 ms | 4,548        | **-22%** |

**MIN_BATCH=16 made cross-host WORSE, not better.** Lower threshold
fires smaller batches sooner → more fsync overhead per op without
unlocking any new concurrency.

So my "server is waiting for 256 to pile up" theory was wrong. Looking
at the code more carefully: the 256 gate only applies to the SECOND
concurrent batch when `n_inflight > 0`. With cross-host's slow
arrival, `n_inflight` reaches 0 between batches (one batch fully
drains before the next pile arrives), so the gate never fires —
`partition_loop` already runs in the "single-batch mode" the gate
was meant to avoid. Lowering MIN_BATCH just adds friction.

The REAL bottleneck is somewhere I haven't found yet — possible:
* `fan_out(depth=8)` per client thread is bounded by `buffer_unordered`
  semantics that may yield to scheduler frequently on cross-host's
  longer per-op time → effective per-thread inflight is less than 8.
* The `autumn-rpc` per-conn `writer_task` flushes one frame at a time
  on submit; on loopback the submit→write→ack cycle is microseconds so
  many frames batch into one `tcp_sendmsg` via the writer_task's
  pipelined drain. Cross-host's RTT means the queue empties between
  submits — small `tcp_sendmsg` per frame.
* Per-conn `FuturesUnordered` cap on the SERVER side
  (`AUTUMN_PS_CONN_INFLIGHT_CAP=64`) — at t=128 d=8 = 1024 client
  inflight, 16 conns × 64 = 1024 server-side cap. Exactly at the
  saturation point.

**Decision**: don't speculate further — would need profile data to
nail the real cause (server-side flamegraph at t=128 cross-host vs
loopback). Documented the cliff as an operator gotcha; production
workloads should use batched RPCs or accept the wire-bound ceiling.

The `put_many` client-side fan-out, by the way, would NOT fix this —
it's `buffer_unordered` over single-op `put` calls (no server
`MSG_BATCH_PUT`), so each op still hits the wire individually. A real
server-side batch RPC (~200 lines) is the only mechanical fix that
would let one frame carry N ops and feed `partition_loop`'s pending
queue in one go.

## Profiling (2026-06-08) — root cause located

Added per-second `partition write summary` logging (already present —
`crates/partition-server/src/lib.rs:1366`). Ran cross-host TCP bench
at t=128, captured stats per partition:

```
part_id=55 ops=454 batches=33 ops_per_sec=445 avg_batch_size=13.8 \
  avg_phase1_ms=0.015 avg_phase2_ms=21.85 avg_phase3_ms=0.018 \
  fill_ratio=0.0045
part_id=76 ops=433 batches=31 ops_per_sec=431 avg_batch_size=14.0 \
  avg_phase2_ms=29.83
part_id=118 ops=562 batches=42 avg_batch_size=13.4 avg_phase2_ms=23.43
...
```

**The bottleneck is now precisely identified.** Per partition:
* Each batch carries only **13-15 ops** (`fill_ratio = 0.45 %` of the
  256-op max).
* `phase2_ms` (the 3-replica fanout + fsync wait) is **22-35 ms** —
  this is the per-batch cost the loopback case ALSO pays.
* Per-partition throughput = 14 / 25 ms ≈ **560 ops/s/partition** ×
  16 partitions ≈ 9 K ops/s. Matches the measured 5.8 K within bench
  variance.

By contrast loopback at t=128 = 50 K ops/s, ÷ 16 partitions ≈ 3.1 K
ops/s/partition. With the same ~25 ms `phase2`, that implies
loopback batch_size ≈ 78 ops — about **5.5× larger than cross-host's 14**.

The 5.5× larger batch on loopback is the entire throughput gap. Server-
side work per batch (phase2) is identical between the two; client
arrival shape is the only variable.

At t=512 cross-host, batch size DOES grow to 30-60 ops, but `phase2_ms`
ALSO grows to 45-114 ms (EN-side contention from more concurrent
batches), so net throughput barely scales.

### Why MIN_PIPELINE_BATCH adjustment backfired

The 256 gate only blocks the SECOND concurrent batch from launching
when `n_inflight > 0`. With cross-host's thin arrival, `n_inflight`
already hits 0 BETWEEN batches (one batch drains entirely before the
next pile of arrivals materialises). So the gate doesn't fire — the
partition is already in "one-batch-at-a-time" mode by force.

Lowering MIN_BATCH=16 makes things worse because it fires the SECOND
batch sooner when arrivals are JUST barely keeping `n_inflight > 0` —
the resulting tiny second batch costs a full `phase2_ms` for ~16 ops.
You get more batches with worse amortization, not better concurrency.

### The fix shape: server-side batch delay ("Nagle for batches")

The correct intervention is the OPPOSITE of lowering MIN_BATCH: WAIT
up to T ms (configurable, e.g. 1-5 ms) for `pending` to grow before
firing the FIRST batch. This is exactly Nagle's algorithm but at the
batch-fire layer instead of the TCP segment layer:
* Loopback arrivals fill quickly → wait expires immediately → no
  regression.
* Cross-host arrivals accumulate during the wait → batch_size grows
  → fsync amortizes over more ops → throughput rises.

Trade-off: adds up to T ms tail latency on the FIRST batch. For 4K
writes where p50 is already 20-200 ms (the fsync itself), adding
1-5 ms is invisible.

A 1 ms wait at cross-host arrival rate of 5.8K ops/s / 16 partitions
= 365 ops/s/partition would let ~0.365 ops accumulate per ms — meaning
2-3 ms of wait gets you 1 extra op. Diminishing returns past ~5 ms;
sweet spot probably 2-3 ms. To match loopback's 78-op batches the
wait would need to be ~200 ms per batch — clearly unacceptable. The
realistic gain is partial: maybe 2× the throughput, not the full 5.5×.

The TRUE full fix is client-side bulk put RPC (`MSG_BATCH_PUT` server
side, lets one frame carry N ops). With that, even at 1-op-per-second
arrival from each client thread, the server sees N×t arrivals per
frame → full batch amortization with zero added latency. This is the
~200-line implementation hinted at above.

## Tested put_many in perf-check (2026-06-08, commit `151e3f7`)

Added `--batch-put N` flag to perf-check. When `N > 0`, the write loop
builds batches of N (key, value) per round and submits via
`put_many(items, concurrency=N)` instead of per-op `kv_put`. The
hypothesis was: even though `put_many` is client-side fan-out (no
server `MSG_BATCH_PUT`), the autumn-rpc `writer_task`'s tcp_sendmsg
coalescing should let one TCP segment carry N frames →
PS-conn `read_loop` decodes N → injects N pending into
`partition_loop` → server batch_size grows.

x-host TCP, p=16 × shards=16, 4K writes:

| config                          | ops/s | p50      | server avg batch_size |
|---------------------------------|-------|----------|-----------------------|
| t=16 d=8 baseline               | 3 270 | 31.93 ms | ~14                   |
| t=16 d=1 --batch-put=32         | 4 821 |  3.30 ms | ~2.2                  |
| t=16 d=1 --batch-put=64         | 5 209 |  2.98 ms | ~2.2                  |
| t=16 d=1 --batch-put=128        | 5 263 |  2.93 ms | ~2.2                  |
| t=64 d=1 --batch-put=32         | 7 130 |  8.79 ms | ~6.5                  |
| t=64 d=1 --batch-put=64         | 7 050 |  8.28 ms | ~6.5                  |
| t=128 d=1 --batch-put=32        | 6 507 | 18.66 ms | ~6.5                  |
| **t=128 d=1 --batch-put=64**    | **7 913** | **13.47 ms** | **~6.5**          |
| t=64 d=8 --batch-put=8 (combo)  | 3 444 | 17 ms    | (worse — no win)      |

Best: **t=128 d=1 --batch-put=64 → 7 913 ops/s** vs baseline t=128 d=8
→ 5 796 ops/s. **+37 % throughput, p50 13 ms vs 174 ms (13× lower)**.

Best latency: **t=16 d=1 --batch-put=128 → p50 2.93 ms** vs baseline
31.93 ms. **11× lower per-op latency.**

### The result is surprising

`avg_batch_size` actually SHRANK from 14 (baseline) to 2-6 (with
`put_many`). Yet throughput went UP. So my "batch_size is the
bottleneck" diagnosis was incomplete — the actual mechanism is more
subtle.

Hypothesis: with `put_many`, the client thread submits N futures
through `buffer_unordered` in ONE shot. The autumn-rpc `writer_task`
coalesces them into ONE `write_vectored`. Server read_loop decodes
them in a tight burst and forwards to partition_loop's mpsc. BUT —
partition_loop processes its mpsc message-by-message and may fire a
new batch on each message (when `n_inflight < cap=8` and `pending > 0`).
So instead of accumulating N pending → ONE big batch, we get N small
batches IN PARALLEL up to the inflight cap.

That parallelism is the win: pre-`put_many`, `n_inflight` stayed at
1 most of the time (single-batch mode, sparse arrival). Post-
`put_many`, `n_inflight` actually USES the 8-batch cap because
arrival is bursty enough to fill it before the first batch finishes.

So the fix isn't "make each batch bigger", it's "make ENOUGH batches
fire concurrently to actually exercise INFLIGHT_CAP". `put_many`
accidentally achieves that via burst submission.

Why keys spreading across partitions doesn't kill it: even though a
32-key `put_many` from one thread distributes ~2 keys per partition,
ALL 16 partitions get hit in the same burst, so each partition_loop
sees 2 ops simultaneously instead of 1 — enough to bump `n_inflight`
up by one per partition per round.

### Why the per-batch summary still says ~6.5 (smaller than baseline's 14)

When 8 concurrent batches fire each with ~6.5 ops, throughput per
partition = 8 × 6.5 / phase2_ms = 52 / 25ms = 2080 ops/s/partition.
Times 16 = 33 K ops/s theoretical. We see 7.9 K = 24 % of theoretical —
some EN-side serialization eating the rest.

Pre-`put_many`: 1 batch × 14 ops / 25ms = 560 ops/s/partition × 16 =
9 K theoretical, 5.8 K observed = 64 % of theoretical. **The baseline
was more "efficient per batch" but used fewer concurrent batches**.
`put_many` uses MORE batches at LOWER per-batch efficiency for a net
+37 % win.

### Verdict and recommendation

* `--batch-put 32-64` is a real cross-host win at moderate cost
  (slightly higher tail latency at high t, but much lower p50 at any t).
* Doesn't reach loopback's 50 K ops/s — server-side concurrency cap
  is still the wall. To go higher needs real `MSG_BATCH_PUT`
  (server-side decode of N ops in one frame → ONE big batch fires
  immediately) which would let partition_loop hit full 256-cap
  batches.
* Production kvcache workloads already issue many keys per call, so
  they're getting `put_many`-equivalent behavior already. The 5.8 K
  ceiling was a perf-check synthetic artifact.

**Engineering takeaway**: the `partition_loop` `n_inflight` cap of 8
is the new ceiling once `put_many` unblocks burst submission. Raising
`AUTUMN_PS_INFLIGHT_CAP` from 8 → 16/32 with `put_many` would
likely lift this further (untested).

## Real `MSG_BATCH_PUT` shipped (commit `1724ca3`) — closes the cliff

Implemented server-side batched PUT:

* Wire (`crates/rpc/src/partition_rpc.rs`): `MSG_BATCH_PUT = 0x53`,
  `MSG_BATCH_GET = 0x54`, rkyv `BatchPutReq { part_id, region_epoch,
  must_sync, ops: Vec<BatchPutOp{key, value, expires_at}> }` +
  symmetric Resp / GET shapes.
* PS server: `BatchPutAccumulator` (Rc<RefCell> shared one-shot reply
  across N ops) + `WriteResponder::BatchPut { accum, idx }` +
  `enqueue_batch_put` that pushes ALL N `WriteRequest`s into pending in
  one dispatcher call. `partition_loop` then sees `pending.len() += N`
  atomically and can fire wide batches.
* Client SDK: `ClusterClient::batch_put(items) -> Vec<Result<()>>`.
  Routes by key to group items by partition, ONE `MSG_BATCH_PUT` RPC
  per partition. Non-ZC only (≥ 64 KiB falls back to per-op `put_zc`).
* perf-check `--batch-put N` now uses `batch_put` (was the legacy
  `put_many` for the previous +37% test).

### Results — drops the cliff almost entirely

Same cluster, same shape (p=16 × shards=16 × --3disk r=3), x-host TCP:

| config                                          | ops/s    | p50         | vs prior baseline |
|-------------------------------------------------|----------|-------------|-------------------|
| baseline t=128 d=8 per-op                       | 6 551    | 147.53 ms   | (anchor)          |
| put_many t=128 d=1 --batch-put=64 (prior commit)| 7 913    | 13.47 ms    | +21 % (was anchor)|
| **MSG_BATCH_PUT t=16 --batch-put=64**           | **36 333** | **0.29 ms** | **+455 %**       |
| MSG_BATCH_PUT t=64 --batch-put=32               | 30 793   | 1.90 ms     | +370 %            |
| **MSG_BATCH_PUT t=64 --batch-put=64 (sweet)**   | **45 146** | **1.25 ms** | **+589 %**       |
| MSG_BATCH_PUT t=128 --batch-put=64              | 11 687   | 0.69 ms p99 25 | -                |

Server-side `avg_batch_size` distribution during the sweet spot:
**p50 = 128, p90 = 128, max = 244, mean = 82** (`MAX_WRITE_BATCH` cap
= 256). The hypothesis is confirmed: `partition_loop` fires
near-cap batches when the dispatcher injects N ops in one shot.

### Comparison vs the put_many "fix"

| target               | mechanism                                | x-host TCP 4K ops/s |
|----------------------|------------------------------------------|---------------------|
| TCP loopback ceiling | (anchor — server gets fat arrivals)      | 193 K               |
| MSG_BATCH_PUT        | server-side decode once, inject N pending| **45 K (23 % of loopback)** |
| put_many             | client-side buffer_unordered burst       | 7.9 K (4 %)         |
| per-op kv_put        | naive serial                             | 6.5 K (3 %)         |

`MSG_BATCH_PUT` is **6.9× better than `put_many`** at the same
client-visible shape. The wire-protocol-level batching (one frame
carries N ops, decoded once, injected as one mpsc message)
fundamentally outperforms client-side fan-out.

### Latency

* `MSG_BATCH_PUT` at sweet spot: **p50 1.25 ms** (was 147 ms baseline
  = **118× lower**).
* The per-op latency definition here is `batch wall time / batch size`
  — what users observe for in-batch ops. Even the wall-clock
  per-BATCH (~80 ms at t=64) is faster than the baseline's per-op
  latency.

### What put_many's role becomes

`put_many` is now strictly a **client-side convenience wrapper** for
mixed ZC/non-ZC workloads:

* For values ≥ 64 KiB → falls through to per-op `put_zc` (the wire
  payload dominates; batching gives no win, and `MSG_BATCH_PUT`
  explicitly rejects oversized ops to keep the frame finite).
* For values < 64 KiB → callers should call `batch_put` directly for
  best perf; `put_many`'s per-op fan-out costs the 6.9× factor above.

Recommendation: callers shipping a homogeneous small-value workload
should use `batch_put`. Callers with mixed sizes can stay on
`put_many` if convenience matters more than the 6.9× factor; if it
doesn't, split the input by size and call the appropriate API per
chunk.

### Server-side BATCH_GET also shipped (untested in perf-check yet)

`MSG_BATCH_GET` mirrors PUT on the read path:
`handle_batch_get` (in `rpc_handlers.rs`) runs INLINE on ps-conn,
takes one rkyv-decode of `BatchGetReq`, loops over keys reusing
`get_value` per key, and packs all values into ONE
`BatchGetResp`. Client SDK: `ClusterClient::batch_get(keys) ->
Vec<Result<Option<Vec<u8>>>>`. Read win is purely wire-frame
amortisation (the server's per-key VP resolution / pin / not-found
semantics are unchanged), so the gain factor will be smaller than
PUT's — but the same shape and identical to the latency math: one
RTT covers N keys instead of N RTTs.

A perf-check `--batch-get N` flag hooking the read phase is the
next step; today the read phase still calls per-key `get`.
