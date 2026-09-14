# Single-partition CPU investigation, 2026-09-14

Baseline is the previously published core-path change (45504c3 locally; identical
tree 02ccf07 on H200-1). Same container, RF3 on /data03,/data05,/data08, one client
thread pinned to CPU 40, 8 MiB values, depth 8, one bench partition. UCX remains
1.16 and compio remains 0.18. This is not a cross-host or whole-machine benchmark.

## What consumes CPU

Baseline write: 522.6 MiB/s. Active P-log averaged 0.275 user CPU cores and 0.609
system CPU cores during the measured process window. P-sst was essentially idle.
bpftrace profiles identify repeated send-side CRC in user stacks; kernel stacks
are dominated by TCP receive copy_to_iter, TCP send copy_from_iter, and send page
allocation/clear_page_erms. An io_uring syscall frame above those functions does
not establish that io_uring bookkeeping itself is the expensive work.

## Retained changes

1. Actual P-sst affinity. P-log pins itself, then spawns P-sst. Linux inherits
   that single-CPU mask; compio 0.18 intersects the requested target with the
   current mask, leaving P-sst on its parent's CPU. Apply pin_current before
   runtime construction. Read-back changed P-log/P-sst from 12/12 and 14/14 to
   12/13 and 14/15. The Linux regression fails when the explicit child pin is
   removed. No standalone throughput gain is claimed: the affected thread was
   idle in the baseline, and live repinning measured 455 MiB/s in one noisy run.
2. Reuse immutable payload CRC across star replicas. PreparedPayload owns the
   segments, size and CRC. Each RpcClient combines its distinct frame header CRC
   with that CRC. It saves R-1 complete scans for replicated appends >=64 KiB;
   small/single-replica/chain sends retain their old path. Header and complete
   append transit CRC, wire layout, ordering and all-replica ACK are unchanged.

Release CRC microbenchmark: 200 x 8 MiB payloads, three distinct request IDs per
payload, including prepared-object and iovec construction: original 893.294 ms,
prepared 269.859 ms (69.8% less elapsed CPU-bound work). The separate byte-for-byte
test includes empty/multi-segment payloads and corruption rejection.

Cluster observations, short sequential samples (not confidence intervals):

| Stage | MiB/s | Notes |
|---|---:|---|
| Baseline | 522.6 | P-log user/system 0.275/0.609 cores |
| CRC reuse + fixed P-sst pin, old EN recv | 733.9, 635.3 | First P-log user/system 0.199/0.584 cores |
| Plus candidate pooled EN recv | 810.9, 668.7 | Aggregate system CPU increased; not a demonstrated efficiency win |
| Candidate recv with conn cap 8 | 622.5, 553.7 | More waiting, batch size still about 1–1.17 |
| Restore old EN recv, retain cap 8 | 805.5, 765.3 | Candidate recv not justified |
| Disable prepared CRC, old EN recv, cap 8 | 629.9, 624.0 | Late, shorter samples; useful direction, not exact causal percentage |

Disk fullness and accumulated WAL/SST state changed during the sequence; repeated
restarts paid long replay and samples after writes include normal flush/compaction.
The independent CRC benchmark and byte-equivalence tests are stronger evidence
for the mechanism than a fixed end-to-end speedup claim. Test writes were stopped
as free space decreased; no existing non-test files were removed.

## Rejected/deferred experiments

The pooled EN ctrl-frame receiver preserved full CRC and passed TCP/UCX payload
and slow-successor tests, but its larger pool slabs, registration and receive
scheduling did not yield stable aggregate CPU/throughput gains. It was removed
from the final code. Existing receive and connection cap=4 remain. Raising the
cap alone did not create materially larger natural batches, so no default change
or artificial batch-delay timer was added. The TCP/UCX prepared-append integration
test remains and checks ACK offsets plus actual on-disk bytes.

## Would upgrading compio reduce kernel CPU?

Current lock: compio 0.18.0, driver 0.11.4, runtime 0.11.0. Upstream compio
0.19.1 changelog and driver 0.12.x describe relevant new capabilities:

- Linux send zerocopy and explicit async zerocopy write traits (#754/#898).
- Multishot/managed receive, fixed files and receive/send poll-first.
- Single-issuer/deferred task-run configuration and notification changes.
- IORING_ENTER_NO_IOWAIT on CQ wait (#957); this is not evidence that TCP
  copies disappear or that a lower iowait metric means less CPU work.

Simply bumping the dependency does not convert write_vectored_all into a zerocopy
send or ordinary read into managed/multishot receive. The sampled TCP data copies
and page allocation call for explicitly testing those APIs, particularly PS to
EN large sends. Use both same-host and actual cross-host traffic, include all
kernel/worker CPU in accounting, and retain buffer lifetime until the zerocopy
completion notification. SQPOLL can move CPU to a kernel thread rather than save
it. A separate compio upgrade A/B remains deferred; no dependency was upgraded.

Sources inspected:
- https://github.com/compio-rs/compio/blob/master/compio/CHANGELOG.md
- https://github.com/compio-rs/compio/blob/master/compio-driver/CHANGELOG.md
- https://github.com/compio-rs/compio/blob/master/compio-runtime/CHANGELOG.md

## Reproduce

```sh
cargo test -p autumn-common --lib child_can_move_off_its_parents_single_cpu
cargo test -p autumn-rpc --lib prepared_payload_preserves_full_frame_crc
cargo test --release -p autumn-rpc --lib replica_crc_cpu_benchmark -- --ignored --nocapture
cargo test -p autumn-stream --test prepared_append
AUTUMN_TEST_UCX_BIND='[<RoCE-IP>]:0' UCX_TLS=rc_mlx5,ud_mlx5,tcp,self UCX_NET_DEVICES=mlx5_1:1 \
  cargo test -p autumn-stream --features autumn-rpc/ucx --test prepared_append
```

Raw logs/scripts: H200-1 /data08/dongmao-autumn-perf-20260914/cpu-round2.
Selected samples and test output: perf/core_path_20260914/cpu-round2.json.
