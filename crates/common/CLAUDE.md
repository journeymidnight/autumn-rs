# autumn-common Crate Guide

## Purpose

Shared utilities, metadata store, and error types. Used by `autumn-manager` (store + error), `autumn-stream` and `autumn-partition-server` (metrics helpers).

## Modules

### CPU affinity for child runtimes

`pin_current` sets the requested CPU directly before runtime construction. A
P-sst thread spawned by a pinned P-log inherits that single-core mask; compio
0.18 intersects requested CPUs with the inherited mask and otherwise silently
leaves P-sst on P-log's CPU. The direct set still respects OS/cgroup limits. A
Linux regression spawns from CPU A and verifies the child actually runs on B.

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
