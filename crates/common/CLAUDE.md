# autumn-common Crate Guide

## Purpose

Shared utilities, metadata store, and error types. Used by `autumn-manager` (store + error), `autumn-stream` and `autumn-partition-server` (metrics helpers).

## Modules

### CPU affinity for child runtimes

Every work-unit thread (P-log, P-sst, each EN shard, the single-shard EN's
main thread) calls `pin_current` before building its runtime. Nothing pins
through compio's `RuntimeBuilder::thread_affinity`: compio intersects the
requested CPUs with the mask the thread inherited and binds nothing when they
are disjoint, with no error. A thread spawned by a pinned parent (P-sst under
P-log) inherits one CPU, and a process started as `taskset -c A,B` inherits
A,B — so a whole cluster launched that way ran on two cores while every thread
logged its `--cpuset` core (write throughput 500-560 MiB/s against 2.7 GiB/s
pinned correctly, same box). `pin_current` is `sched_setaffinity`: it moves the
thread anywhere the cgroup/cpuset allows, and a core outside that is a startup
error, never a skip. A Linux unit test spawns from CPU A and checks the child
reaches B; `crates/server/tests/cpuset_pinning.rs` starts the real EN (multi-
and single-shard) and PS binaries under `taskset -c <launcher>` and reads every
thread's `Cpus_allowed_list` back.

**The runtime's io_uring workers go on the same cores** (`confine_io_workers`,
called right after each work-unit runtime is built). io_uring runs what it
cannot complete inline — buffered writes, fsync — on worker threads
(`iou-wrk-*`) that take the whole NUMA node's cores on 6.1 (all allowed cores from about 6.4), not the
creating thread's affinity; only `IORING_REGISTER_IOWQ_AFF` or a cgroup cpuset
(on kernels where io-wq honours it — 6.1 here does) confines them. So
`--cpuset` meant "the work-unit threads" and not "this process": an EN given six
cores ran four more cores of page-cache copy and writeback outside them (write
~2.0 GB/s buffered; confined to its cores by a cgroup cpuset it made ~1.27 GB/s).
The registration applies to the CALLING task's io-wq, which is why it happens
on the work-unit thread itself. Cores registered: the explicit `--cpuset`, or the
detected cores from `--cpu-start` on — the pool the work units are pinned from.
Linux before 5.14 lacks the call, and newer kernels refuse cores outside the
task's cgroup cpuset (a PS `--cpuset` wider than its cgroup): one WARN,
workers unconfined (isolation, not correctness). The polling driver has no io_uring workers. Tests:
`io_workers_follow_the_registered_cores` (unit; red without the registration —
the workers take the node's 96 cores) and the `iou-wrk` check in
`crates/server/tests/cpuset_pinning.rs` (real EN; red with the EN calls removed).
Only the pinned work-unit runtimes register; the PS main runtime and other
unpinned threads are not confined. Not covered either: compio's
`spawn_blocking` pool threads are spawned from the work-unit thread and inherit
its ONE core, so blocking work (EN fallocate, PS SST building) shares that core
instead of the `--cpuset`.

Auto-detection pins only where the OS can bind a thread to a core (Linux,
Android, Windows, FreeBSD). macOS has affinity hints only: `get_core_ids`
lists every core but `set_for_current` always fails, so once a failed pin
became fatal a PS on macOS could open no partition at all. There the detected
set is empty — nothing pinned, one EN shard — and an explicit `--cpuset`
still fails loudly because the operator asked for it.

### `metrics.rs` — Shared Performance Measurement Helpers

Standardized helpers for periodic performance reporting across all crates. All latency fields use milliseconds (`_ms`).

| Function | Purpose |
|----------|---------|
| `duration_to_ns(Duration) -> u64` | Convert Duration to nanoseconds (clamped to u64::MAX) |
| `ns_to_ms(total_ns, count) -> f64` | Accumulated nanoseconds → average milliseconds |
| `unix_time_ms() -> u64` | Current UNIX epoch time in milliseconds |

Used by `StreamAppendMetrics` (stream crate), `WriteLoopMetrics` and `ReadMetrics` (partition-server crate).

### `metrics_http.rs` — Prometheus `/metrics` endpoint (observability batch 1)

`spawn_metrics_http(bind_host, port, render)` — minimal hand-rolled HTTP/1.1
listener on a dedicated OS thread (`std::net::TcpListener`, blocking, 2 s/5 s
read/write timeouts). Deliberately ZERO interaction with the compio runtimes:
the `render: Arc<dyn Fn() -> String + Send + Sync>` closure runs on the
metrics thread and must only read `Arc`-shared state. Binaries whose state is
`Rc`/`RefCell` (manager store, PS partitions map) publish a pre-rendered
snapshot string into an `Arc<RwLock<String>>` from a 2 s task on their own
runtime; the EN renders directly from process-global atomics. Bind failure
returns Err — callers log ERROR and keep serving (metrics are auxiliary,
never kill the data plane). `push_metric` / `push_type` emit the Prometheus
text format with label escaping; emission must be metric-major (all samples
of one metric contiguous after its `# TYPE` line — never interleave).
Endpoint is opt-in per binary via `--metrics-port`; `cluster.sh` wires it
with `AUTUMN_METRICS=1`.

### `error.rs` — Domain Error Types

```rust
pub enum AppError {
    NotLeader,
    NotFound(String),
    Precondition(String),
    InvalidArgument(String),
    Internal(String),
}
```

Uses `thiserror`. These are converted to `tonic::Status` at the gRPC boundary in `autumn-manager`. Mapping:
- `NotFound` → `Status::not_found`
- `Precondition` → `Status::failed_precondition`
- `InvalidArgument` → `Status::invalid_argument`
- `Internal` / `NotLeader` → `Status::internal`

### `store.rs` — the owner-epoch fence tokens and their classifier

**The metadata store MOVED OUT.** `MetadataState` / `MetadataStore` now live in
`crates/manager/src/store.rs`. They left because the manager's persisted records
are `pub(crate)` to that crate (see `crates/manager/CLAUDE.md`, "Persisted
records") and `MetadataState` is what holds them in memory — a state struct in a
shared crate cannot hold a type only the manager may name. Nothing outside the
manager ever referenced it, so no other crate changed.

What stays here is the part `autumn-stream` needs:

**`OWNER_KEY_PREFIX_TOKEN` / `OWNER_EPOCH_MISMATCH_TOKEN` / `OWNER_KEY_MISSING_TOKEN`**
— the wire-stable spellings of an owner-epoch fence rejection.

**`is_owner_epoch_fence_message(msg) -> bool`** — the classifier. Over the wire a
fence is just `CODE_PRECONDITION`, indistinguishable from ordinary preconditions
("stream cannot be empty after punch holes", admin-token checks) without a new
wire code — which would force a stop-world `WIRE_VERSION` bump that nothing
would catch if forgotten. `StreamClient` uses this to route a fence into the
PS's "LockedByOther" poison-and-reopen self-heal.

**Producer and matcher are now in different crates, and that is safe for reasons
that were always the real ones.** The producer
(`autumn_manager::store::MetadataState::ensure_owner_epoch`) builds its message
from the constants above, so a reword cannot detach the matcher; and
`owner_fence_matcher_pairs_with_producer`, which moved to the manager crate with
the producer, runs THIS matcher over errors the REAL producer generated. The old
comment credited "kept ADJACENT" for that property — adjacency was never the
mechanism, and saying so is the point of this note. Renaming or removing a token
is still a wire-visible change for mixed-version clusters: same-commit
stop-world only.

## Important Invariants

1. **The token spellings are a wire contract.** Renaming or removing an
   `OWNER_*_TOKEN` changes text a mixed-version cluster matches on; treat it
   like a wire-schema edit.
2. **Never re-implement the classifier.** One matcher, shared by the stream
   crate and pinned against the real producer by a test in the manager crate.

The invariants that used to be listed here — id uniqueness through `alloc_ids`,
the owner lock bumping on every acquire, `ensure_owner_epoch` before every
stream mutation — moved with `MetadataState` to `crates/manager/CLAUDE.md`.

## Build toolchain

The workspace minimum is Rust 1.95 for compio 0.19.2. Explicit child affinity
still runs before runtime construction; cpu_pin behavior is unchanged.
