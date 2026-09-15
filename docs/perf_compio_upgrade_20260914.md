# Compio upgrade evaluation, 2026-09-14

See [the later controlled validation](perf_compio_controlled_20260915.md) for fixed-work
version/scaling comparisons and kernel accounting. This file preserves the earlier
short-sample investigation.

## Delivery and limits

The migration targets compio 0.19.2, driver/net 0.12.5, runtime 0.12.6,
executor 0.1.4 and cyper 0.9.0. Both workspace and standalone Python resolve
one compio family. Rust 1.95 is required: 1.93.1 fails in compio-executor's
cfg_select macro. Baseline and upgraded release binaries use Rust 1.95.0.

Prepared replica TCP zerocopy is explicit and disabled by default. No receive
or scheduler option is enabled in production. No wire or storage format changes
were made. This evaluation does not establish an end-to-end efficiency gain.

The original acceptance is not complete. The user confirmed H200-2 is offline
and no cross-host environment remains. All traffic below is on H200-1. The
complete allocation/copy/SQ/CQ and independent kernel-worker CPU matrix was not
collected. Do not substitute process system time or changed iowait accounting
for whole-machine work. SQPOLL measurements omit its kernel thread and cannot
establish a CPU saving.

## Environment and method

H200-1, dongmao-autumn container, Linux 6.1.0-31-amd64, UCX 1.16.0,
UCX_TLS=rc_mlx5,ud_mlx5,tcp,self and UCX_NET_DEVICES=mlx5_1:1. The baseline is
633a483, containing the prepared CRC and P-sst affinity fixes. TCP uses loopback;
UCX uses the host's RoCE IPv6 address. Neither crosses hosts.

The old /data03 and /data05 NVMe volumes were almost full. Fresh isolated RF3
instances use three directories on /data08 (one NVMe), four shards per EN,
EN CPUs 0-3/4-7/8-11, PS budget 12-31 and client CPU 40. P-log/P-sst read-back
is 12/13 after upgrade. This is not the prior three-NVMe topology. A free-space
floor bounds test writes; existing test data was preserved.

The core_path benchmark loads 256 fixed keys per size, warms each operation
for two seconds, then measures two seconds and drains outstanding requests.
Process user/system snapshots bracket the same timed request/byte window.
Latency samples every 16th operation. The main comparison holds the client at
0.18 while swapping all server binaries, isolating server-runtime changes.
TCP has three repetitions per cell; UCX has two. Sizes are 4 KiB, 64 KiB,
1 MiB and 8 MiB, depths 1/8, read/write separately. Server startup/replay is
excluded. A failed premature readiness sample is retained only in logs.

These are sequential short samples on a shared machine. WAL/SST state grows
between stages, and some writes overlap ordinary flush/compaction. They do not
justify a causal percentage or a claim that every regression is ruled out.
A read-only reversal uses identical final data to check the UCX 64 KiB result.

## TCP observations

Depth 8 medians, MiB/s (three samples each):

| Size | Operation | Baseline | Pure upgrade | Zerocopy opt-in |
|---|---|---:|---:|---:|
| 4 KiB | Read | 435.6 | 439.9 | 439.6 |
| 4 KiB | Write | 51.8 | 49.6 | 50.0 |
| 64 KiB | Read | 1785.3 | 1755.2 | 1559.5 |
| 64 KiB | Write | 162.3 | 113.4 | 138.8 |
| 1 MiB | Read | 2897.2 | 2774.3 | 2867.0 |
| 1 MiB | Write | 538.6 | 508.7 | 503.7 |
| 8 MiB | Read | 2314.6 | 2318.9 | 2333.9 |
| 8 MiB | Write | 557.3 | 565.5 | 572.6 |

8 MiB/depth 8 write process CPU seconds/GiB: baseline 6.38, upgrade 6.33,
zerocopy 6.68. The opt-in did not demonstrate a useful CPU/throughput threshold
on this loopback setup, so it remains off. The weaker 64 KiB samples are
reported rather than discarded; the stages have different accumulated state.

## UCX observations and read-only reversal

Initial depth-8 64 KiB read samples were 2,909–2,913 MiB/s on baseline versus
2,419–2,473 MiB/s on upgrade. The stages contained different WAL/SST state.
After freezing the final data, restarting each version and waiting for replay,
two read-only rounds per version gave these depth-8 medians:

| Size | Baseline MiB/s | Upgrade MiB/s |
|---|---:|---:|
| 4 KiB | 408.9 | 410.2 |
| 64 KiB | 2645.4 | 2526.9 |
| 1 MiB | 4928.8 | 4910.4 |
| 8 MiB | 3532.5 | 3556.8 |

The large initial 64 KiB gap did not reproduce at equal storage state; a 4.5%
residual remains and must not be described as proven regression-free. The
short mixed write sequence is too state-dependent for a causal conclusion.

## Receive and scheduler experiments

compio_features measures one TCP stream, sender CPU 40, receiver CPU 42,
512 MiB warmup followed by 512 MiB measured transfer. The receiver ACK bounds
the transfer; getrusage captures process user/system CPU. There is no storage
or frame decoding, and managed buffers are consumed directly, so gains do not
automatically transfer to autumn's framing/pooled value path.

At 1 MiB sends, ordinary median 0.0928 seconds versus managed 0.0780 seconds;
combined process CPU approximately 0.159 versus 0.149 seconds. Managed receive
is a candidate for a separate bounded framing/backpressure integration.
Forced poll-first, single-issuer and deferred task-run were essentially flat.
SQPOLL increased process CPU even before accounting for its kernel thread.

Multishot initially used a nonzero length (invalid on Linux 6.1); corrected
read_multi(0) still failed because the 32-buffer ring was exhausted. The receiver
error was recorded explicitly. Do not enable it without a ring-exhaustion and
backpressure design. The initial invalid experiment is retained separately.

## Correctness and build evidence

- Workspace all targets with UCX, release servers and standalone Python check.
- 1,052 final library tests passed, two ignored; three new transport tests
  cover delayed completion, partial sends/fallback and connection-error handling.
- Actual TCP zerocopy segmented writes, timeout/cancellation/close and owned
  buffer release passed. Prepared append checks offsets and on-disk bytes on
  TCP and UCX, including a frame with more than 1,024 iovecs.
- Stream integration suites passed, including concurrent replicas and ordering.
- Six FUSE integration cases passed: EOF dirty-size preservation, sticky flush
  error paths, RELEASE handling and variable-length extent reads/truncate.
- UCX Python wheel built and installed in an isolated venv; existing Fs smoke
  passed small/10 MiB/ranged writes, truncation, rename, leases and cross-instance
  byte-exact reads.
- Strict clippy and workspace rustfmt checks fail on existing baseline issues.
  The same failures were reproduced with the baseline and Rust 1.95. Full
  non-strict clippy completed; new zerocopy/benchmark files had no warnings.

## Additional multi-partition check

The upgraded TCP cluster was split into two partitions. The upgraded Python
client verified existing keys at all four sizes, then concurrently wrote/read
sizes 4 KiB/64 KiB/1 MiB/8 MiB at depths 1/8 across both partitions, with exact
byte comparison. This is correctness coverage, not a scaling A/B. A second
split was refused by the existing overlapping-key precondition.

The final TCP/UCX transport suite passed after correcting its existing registered
receive cancellation fixture: creating UcxTransport alone does not select the
runtime transport, and regpool therefore allocated an ordinary TCP slab. The
fixture now calls init_with(Ucx) before testing registered buffer cancellation.

Both isolated clusters were stopped after testing. The user requested cleanup
on 2026-09-14; synthetic data, build trees and temporary environments were deleted.
No subagents were used under the session's delegation restriction.

## Reproduce and rollback

Commands are in docs/ops.md under Compio runtime upgrade verification. Set
--tcp-zerocopy-min-bytes 0 (the default) for ordinary sends. Keep baseline
binaries/lockfiles for a runtime rollback; drain PS before stopping storage.

Selected baseline/upgraded service binaries, lockfiles, logs and runners were
archived and byte-verified before deleting the dated working directories:
/data08/autumn-perf-evidence/20260914-compio-baselines-and-logs.tar.gz.
The archive is about 108 MiB. Restore only needed files into a new bounded test
directory; source is in git. Committed evidence remains under
perf/compio_upgrade_20260914. Full performance acceptance remains unfinished.
