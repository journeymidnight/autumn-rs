# Controlled compio runtime validation, 2026-09-14/15

## Outcome

The missing controlled version/scaling comparisons and kernel accounting were
executed on H200-1. There are 480 low-overhead samples (three rotated repetitions)
and 160 detailed diagnostic samples, plus ten idle trace windows. All 640 planned
successful samples passed operation/byte/partition/window checks. Failed attempts
are retained separately and are not erased by successful repetitions.

This does **not** complete F-COMPIO-UPGRADE acceptance. UCX 64 KiB single-partition
reads regress in this setup, and UCX 1.16 hit intermittent SEND local-protection
errors on both compio 0.18 and 0.19. Shared-host interference prevents claiming
isolated whole-machine efficiency. H200-2 is offline; cross-host coverage is
unavailable. No service code or runtime defaults changed in this validation stage.
TCP prepared-send zerocopy remains disabled by default.

## Controlled setup

- H200-1 dongmao-autumn, Linux 6.1.0-31-amd64, UCX 1.16.0, Rust 1.95.0.
- Baseline 633a483 includes CRC reuse and explicit child CPU affinity. Pure upgrade
  uses compio 0.19.2/cyper 0.9; zerocopy stage enables prepared replica sends at
  65,536 complete-frame bytes. Corresponding-version clients run identical
  controlled_path source (SHA256 recorded), unlike the earlier fixed-old-client run.
- RF3 on separate /data03, /data05 and /data08 NVMe directories; fresh metadata and
  dataset for every trial. Four shards per EN. NUMA memory node 0, EN CPUs 0–11,
  PS 12–19, manager 34, etcd 32–33, client 40 onward. Live affinity is audited.
- One or four partitions, presplit while empty. 64 keys per size per partition,
  with prefix midpoints matching the recorded partition grid. Every thread
  completes exactly the requested count; byte checks happen during load/warmup.
- Four sizes: 4 KiB, 64 KiB, 1 MiB, 8 MiB. Per-partition depth 1/8. Fixed write
  operations: 16,384 / 8,192 / 2,048 / 256; fixed reads: 262,144 / 65,536 /
  8,192 / 1,024. Every version follows the same phase order and warmup count.
- Three rotated version orders; TCP baseline/pure/zerocopy, UCX baseline/pure.
  Separate diagnostic trials repeat the same work with BPF. Normal flush,
  compaction and RF3 persistence remain active. Fresh trials control accumulated
  work; they do not suppress production background behavior or shared host jobs.

## Measurement audit

The benchmark waits at READY/go and DONE/go barriers. Host perf counters begin
disabled and acknowledge enable/disable commands. Fast role-/proc and CPU snapshots
bracket requests; their largest envelope overhead is 1.955 ms. The slow all-process
scan is background audit only. PR_GET_DUMPABLE queries carry BPF markers without
changing process state; maximum marker/request relative difference is 0.001573%.

Hardware counters report 100% running time for every sample. They include user and
kernel cycles, task-clock, instructions and context switches. Per-role CPU splits
client, PS, each EN, manager and etcd. CPU seconds/GiB below use process user+system
CPU; they include io-wq threads charged to those processes.

BPF records independent kernel-thread runtime, selected-CPU unrelated runtime,
IRQ/softirq duration, io_uring_enter/SQE/CQE/opcodes/notification flags,
kmalloc/page allocation bytes and selected TCP copy call sites. These views overlap:
**do not add kernel-thread/IRQ/softirq figures to process CPU or /proc/stat totals**.
Idle and iowait are excluded from busy CPU. No NO_IOWAIT accounting change is
interpreted as saved work.

## Version comparison

8 MiB, depth 8 medians from three untraced repetitions:

| Transport | Partitions | Version | MiB/s | CPU s/GiB | Kernel Gcycles/GiB |
|---|---:|---|---:|---:|---:|
| TCP | 1 | baseline | 840.3 | 4.990 | 11.871 |
| TCP | 1 | pure | 897.7 | 4.845 | 11.566 |
| TCP | 1 | zc | 711.3 | 5.270 | 12.471 |
| TCP | 4 | baseline | 1994.0 | 6.061 | 14.505 |
| TCP | 4 | pure | 1991.4 | 6.036 | 14.486 |
| TCP | 4 | zc | 1926.8 | 6.594 | 15.801 |
| UCX | 1 | baseline | 463.9 | 4.725 | 8.703 |
| UCX | 1 | pure | 487.5 | 4.680 | 8.732 |
| UCX | 4 | baseline | 1274.1 | 5.299 | 9.705 |
| UCX | 4 | pure | 1297.0 | 5.330 | 9.858 |

Single-partition TCP zerocopy is consistently slower in this setup: 8 MiB write
runs are 699–715 MiB/s, versus ordinary-upgrade 882–915 MiB/s. Its process CPU
cost also rises. Four-partition throughput is closer but CPU/GiB is worse. There
is no basis to enable this option by default on loopback.

Pure upgrade does not show a broad CPU reduction. Large TCP single-partition
writes improve, while four-partition throughput/CPU is almost unchanged. UCX
large-value behavior is broadly similar. The complete size/depth/read/write
ranges and latency medians are in summary.json; three repetitions do not justify
confidence intervals or fixed percentage promises.

## Partition scaling

8 MiB, depth 8, aggregate throughput at four partitions divided by one:

| Transport | Version | Read scaling | Write scaling |
|---|---|---:|---:|
| TCP | baseline | 3.01x | 2.37x |
| TCP | pure | 2.98x | 2.22x |
| UCX | baseline | 2.67x | 2.75x |
| UCX | pure | 2.65x | 2.66x |

This is weak scaling: per-partition work/depth stays fixed, so four partitions
perform four times the total work and use four client threads. Replica and CPU
placement are fixed. It is not a strong-scaling claim at fixed total concurrency.

## UCX 64 KiB regression

Single-partition depth-8 reads: baseline median 2,796.7 MiB/s (range 2,670.6–2,904.0),
upgrade 2,572.6 MiB/s (2,381.1–2,578.7), about 8% lower. Median p99 grows from
286.9 to 307.8 us; process CPU/GiB grows 0.9125 to 0.9525. Four-partition medians
do not show the same regression (5,189 versus 5,345 MiB/s). The single-partition
difference remains unresolved; do not mark the small-value/UCX gate passed.

## Detailed kernel observations

For single-partition 8 MiB/depth-8 writes, the diagnostic window transfers 2 GiB
logical data. Baseline and pure upgrade each request about 8 GiB of TCP receive
copies (client to PS plus three replicas). Zerocopy leaves this receive copy
volume unchanged. Pure-upgrade PS uses ordinary sends; the zerocopy trial submits
738 SENDMSG_ZC SQEs (opcode 48) and receives 738 notification CQEs. Four partitions
submit 3,066 such SQEs/notifications for 8 GiB logical writes. The capability is
actually used, but it does not eliminate receiver copies.

Single-partition diagnostic page-allocation bytes are roughly 14.01 GiB baseline,
14.05 GiB pure and 12.70 GiB zerocopy; kmalloc bytes increase with zerocopy. These
are call-site allocation totals (including repeated allocation), not peak memory.
TCP send requests are counted separately; _copy_from_iter is not traceable on this
kernel, so requested send bytes are **not** mislabeled as exact copied bytes.

Independent kernel-thread CPU is now recorded, including ext4 workqueues and
jbd2. For the same single-partition diagnostic window it was 0.888/0.762/1.714 CPU
seconds (baseline/pure/zerocopy), but these raw figures are not isolated gains:
the zerocopy window had 119.7 CPU seconds of unrelated work on the selected CPUs.
IRQ/softirq and all-CPU totals are retained for the same reason. Shared machine
noise and probe overhead make whole-machine efficiency claims invalid here.

99 Hz stack collection had stack-id -17 lookup collisions (2–65 messages per
trial). Typed event/counter maps had no other collector errors. Keep valid stacks
as examples only; this is not a complete stack profile. BPF throughput has clear
probe overhead, especially for small requests, so it is excluded from performance
medians. Diagnostic counts and errors are retained in trace-cells.jsonl and
trace-errors.json; raw maps/stack output are in the compressed archive.

## Reliability findings and failed attempts

1. Upgraded UCX four-partition untraced trial aborted after append timeouts with
   mlx5_1 Local protection error, synd 0x4 vend 0x52, on SEND with a local lkey.
2. The compio 0.18 baseline reproduced the same error during four-partition
   diagnostic sampling. UCX wrapper source is unchanged between versions. This
   establishes a pre-existing reliability defect; it does not identify the root
   cause. Successful repeats completed, but do not erase either failure.
3. Several four-partition trials on both versions logged background flush CAS or
   allocation failures while foreground operations completed. All trial warnings
   are retained. The successful performance dataset is not a reliability pass.
4. A perf ESRCH attach race occurred when an idle io-wq thread exited during
   enumeration. The failed trial was excluded and retained. Bounded retries are
   allowed only for this explicit error before go, when no measured work started.

F-UCX-LOCAL-PROTECTION records the defect with baseline and upgrade crash excerpts.
No speculative retries/timeouts or UCX service changes were added to hide it.

## Artifacts and cleanup

Source: perf/controlled_validation and crates/server/benches/controlled_path.rs.
Results: perf/controlled_validation/results (cells, summary, trace counters,
validation audit, environment/hashes and failure excerpts). Full raw snapshots,
perf CSVs, BPF maps and logs are archived before cleanup under
/data08/autumn-perf-evidence/controlled-validation-20260915.tar.gz.

Successful trial data was reclaimed after results were saved. Failed diagnostics,
source/build directories and temporary environments are removed only after their
archive and report are verified. Container tracefs mounted for this task is
unmounted; host tracefs/security configuration is unchanged. No cross-host test
is claimed. Full feature acceptance remains false for unavailable cross-host,
unresolved UCX read regression/reliability and remaining production capability
integration gates.


Cleanup completed after evidence commit f7c75a3 and archive verification. Removed
/data03/autumn-controlled-validation, /data05/autumn-controlled-validation and
/data08/autumn-controlled-validation, reclaiming 29.05 GiB at final teardown (each
successful trial had already reclaimed its dataset). No benchmark, perf, bpftrace,
compiler or archive process remained. /data08 free space was about 866 GiB.
The archive is 470 MiB; its SHA256 and cleanup audit are committed with the results.
