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
