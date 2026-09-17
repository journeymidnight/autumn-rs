# Controlled compio validation

Run this harness only in the dedicated H200-1 `dongmao-autumn` test container.
It uses explicit directories, ports and CPUs; it is not a production launcher.
The host must provide `perf` and tracefs. The privileged test container can use
`nsenter -t 1 -m -- perf` to run the host tool without installing libraries.
If tracefs is absent inside the container, mount it there and unmount it during
final cleanup. Never change host-wide perf security settings for this harness.

## Inputs

- Root: `/data08/autumn-controlled-validation`.
- Binaries: `bin/{baseline,pure,zc}/autumn-*` and a corresponding-version
  `controlled_path` benchmark in each directory.
- Build the exact same `crates/server/benches/controlled_path.rs` source with
  Rust 1.95 against baseline 633a483 and the upgraded tree. Record SHA256 hashes.
- Data: one `autumn-controlled-validation/<trial>` directory on each of
  /data03, /data05 and /data08. Each contains an ownership marker.
- NUMA memory node 0; ENs CPUs 0–3/4–7/8–11; PS 12–19; etcd 32–33;
  manager 34; client threads 40 onward. No SMT sibling is deliberately assigned.
- RF3, four EN shards each; 1 or 4 partitions. Empty keyspace is presplit before
  data is loaded. Topology and live affinity are saved for every trial.

## Measurement contract

The benchmark creates 64 fixed keys per partition for each size. Initial load
and byte verification precede measurements. Every measured phase executes a
fixed warmup count, announces READY, and waits for the controller. The controller
starts disabled perf counters with an acknowledged command, takes a fast role/CPU
snapshot, then sends go. The benchmark marks the BPF window using a harmless
PR_GET_DUMPABLE query and releases all worker threads at a barrier. It closes
the marker only after every fixed-count request has completed, announces DONE,
and waits again. The controller takes the matching fast snapshot and disables
perf before allowing benchmark teardown.

The all-process /proc snapshot is intentionally outside the fast window. It
identifies background workloads and PID reuse; its duration is not the precise
request window. Use `role_before/role_after` for process CPU and CPU totals.
BPF marker timing is the exact diagnostic window. Keep the slight control and
snapshot envelope visible in the report.

Each trial starts with fresh metadata and data and repeats the same phase order.
This controls accumulated WAL/SST state by work rather than elapsed time. It
does not disable normal flush/fsync/compaction or claim to eliminate shared-host
noise. Rotated version order and per-trial idle/background records expose drift.

## CPU accounting

Keep these views separate; they overlap:

- Per-role user/system CPU: fast /proc deltas, including process io-wq workers.
- Process hardware counters: host perf task-clock, cycles:u, cycles:k,
  instructions and context switches, including inherited threads.
- All/selected CPU busy time: /proc/stat, excluding idle and iowait. Never count
  reduced iowait as less work. IRQ/softirq are included in this aggregate.
- BPF kernel-thread runtime and unrelated work on selected CPUs: diagnostic
  attribution, including filesystem journal/workqueue threads. It is not an
  additional summand on top of CPU busy/process system time.
- IRQ/softirq duration: separate diagnostic attribution. Do not add to process
  system time. Entries crossing a marker boundary are excluded.

The trace records SQE opcodes, CQE flags, asynchronous work submissions,
kmalloc/page bytes, selected TCP receive-copy call arguments, send requests,
and 99 Hz kernel stack samples. Diagnostic runs increase BPFTRACE_MAX_MAP_KEYS
to 32,768 so multiple windows do not hit the default 4,096-key limit. Positive CQE result sums include heterogeneous
operations and are not transport byte counts. Kernel send-copy internals marked
`notrace` are unavailable on this kernel: report requested send bytes and stack
evidence without calling them exact copied bytes. TCP checksums and full append
CRC remain enabled.

## Execution and retention

```sh
python3 perf/controlled_validation/matrix.py
python3 perf/controlled_validation/analyze.py /data08/autumn-controlled-validation/results
python3 perf/controlled_validation/analyze_trace.py /data08/autumn-controlled-validation/results
```

The matrix runs three rotated untraced repetitions of TCP baseline/pure/zerocopy
and UCX baseline/pure for 1/4 partitions, four value sizes, reads/writes and
depths 1/8. Separate diagnostic trials repeat the same fixed work with BPF and
include a one-second empty window. Compare diagnostic throughput to matching
untraced runs to quantify instrumentation overhead.

Raw results are saved before each successful trial stops PS, ENs, manager and
etcd, in that order, and deletes only its marked synthetic dataset. Failed trial
data stays for diagnosis and blocks reuse of its trial name. Free-space floor:
150 GiB on every disk. The `completed` file means results were saved and the trial
was reclaimed. It is not the feature's acceptance status.

After the complete matrix and any justified follow-up, commit summaries and
validation evidence, archive compressed raw records, then remove temporary
source/build directories and unmount only the container tracefs mount created
for this task. H200-2 is offline: no cross-host results can be inferred here.


A diagnosed perf attach race is handled only before the go barrier: an idle
io-wq thread may disappear between perf enumerating /proc/PID/task and opening
its events. An explicit ESRCH permits up to three attach retries, saved as
attach-race logs. No measured workload has started then. Any other counter
failure, or a failure after go, aborts the trial and keeps its diagnostics.


Successful request completion does not imply a clean trial. Analyze service logs
for append timeouts, background flush failures and UCX protection errors. Preserve
failed attempts outside the median dataset with their full logs and explicit
failure counts. A later successful repeat does not erase a reliability failure.
