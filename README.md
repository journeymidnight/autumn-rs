# autumn-rs

Rust rewrite of `autumn`: a distributed KV storage engine with a stream layer and a partition layer.

## Architecture

```
  Clients (autumn-client CLI / your application)
       │  Put / Get / Delete / Range  (gRPC PartitionKv)
       ▼
  autumn-ps  (Partition Server — one or more)
  ┌─────────────────────────────────────────────┐
  │  LSM-tree per partition                      │
  │  Each partition owns 3 streams:              │
  │    log_stream  — WAL + large values (>4KB)   │
  │    row_stream  — flushed SSTables            │
  │    meta_stream — TableLocations checkpoint   │
  └──────────┬──────────────────────────────────┘
             │ append / read  (gRPC ExtentService)
             ▼
  autumn-extent-node  (one or more, holds raw extent files)

  autumn-manager-server  (control plane, backed by etcd)
  ├── allocates streams and extents
  ├── routes partition → PS assignments
  └── drives extent recovery
```

**Key concept — 3 streams per partition:** The 3 streams are created by `autumn-client bootstrap`,
not by the partition server. The PS receives the stream IDs from the manager on startup and uses
them to store its data.

## Prerequisites

- Rust toolchain (`cargo`, edition 2021)
- `protoc` — `brew install protobuf`
- `etcd` — `brew install etcd`

## Build

```bash
cd autumn-rs
cargo build --workspace
```

---

## Dev Cluster Script (`cluster.sh`)

`cluster.sh` manages the full cluster lifecycle — no extra tools required.

```bash
cd autumn-rs
cargo build --workspace          # build binaries first

./cluster.sh start               # 1-replica cluster (default)
./cluster.sh start 3             # 3-replica cluster

./cluster.sh stop                # kill all processes
./cluster.sh clean               # stop + wipe /tmp/autumn-rs data dirs
./cluster.sh restart             # clean + start (fresh cluster)
./cluster.sh restart 3           # fresh 3-replica cluster

./cluster.sh status              # show which processes are running
./cluster.sh logs                # tail all log files (Ctrl-C to exit)
```

After `start`, the script prints ready-to-use CLI examples:

```
AC="./target/debug/autumn-client --manager 127.0.0.1:9001"
$AC info
echo hello | $AC put mykey /dev/stdin
$AC get mykey
$AC ls
```

Logs go to `/tmp/autumn-rs-logs/{etcd,manager,node1,...,ps}.log`.

---

## Quick Start: 1-replica cluster

A minimal cluster: 1 manager, 1 extent node, 1 partition server.

```bash
# Convenience aliases
MANAGER=./target/debug/autumn-manager-server
NODE=./target/debug/autumn-extent-node
PS=./target/debug/autumn-ps
SC=./target/debug/autumn-stream-cli
AC=./target/debug/autumn-client

# Clean up any previous run
pkill -f autumn-manager-server; pkill -f autumn-extent-node; pkill -f autumn-ps
rm -rf /tmp/autumn-etcd /tmp/d1 /tmp/autumn-ps

# Step 1 — etcd: stores manager metadata across restarts
etcd --data-dir /tmp/autumn-etcd \
     --listen-client-urls http://127.0.0.1:2379 \
     --advertise-client-urls http://127.0.0.1:2379 &
sleep 0.5

# Step 2 — manager: control plane (stream allocation, partition routing)
$MANAGER --port 9001 --etcd 127.0.0.1:2379 &
sleep 0.5

# Step 3 — extent node: data plane (stores raw extent files on disk)
$NODE --port 9101 --disk-id 1 --data /tmp/d1 --manager 127.0.0.1:9001 &
sleep 0.5

# Step 4 — register the extent node with the manager
#   Without this, the manager does not know the node exists and cannot
#   assign extents to it.
$SC register-node --addr 127.0.0.1:9101 --disk disk-1

# Step 5 — partition server: KV API layer
#   Starts up, registers itself with the manager (RegisterPs),
#   then asks "which partitions belong to me?" (GetRegions).
#   Answer: none yet — bootstrap hasn't run.
$PS --psid 1 --port 9201 --manager 127.0.0.1:9001 \
    --data /tmp/autumn-ps --advertise 127.0.0.1:9201 &
sleep 1

# Step 6 — bootstrap: create 3 streams (log/row/meta) + 1 partition
#   This is where the 3 streams are created.
#   After this, the PS polls GetRegions(), finds the new partition,
#   and calls open_partition() to start serving it.
$AC bootstrap --replication 1+0
# Expected: "bootstrap succeeded: 1 partition(s)"

sleep 1   # wait for PS to pick up the new partition

# Step 7 — verify
$AC info
# Expected: 1 node, 3 streams, 1 partition

echo "hello autumn" | $AC put mykey /dev/stdin
$AC get mykey
# Expected: "hello autumn"
```

### What happens in bootstrap

`autumn-client bootstrap --replication 1+0` does:

1. `CreateStream(data_shard=1, parity_shard=0)` → **log_stream** (id=1)
2. `CreateStream(data_shard=1, parity_shard=0)` → **row_stream**  (id=2)
3. `CreateStream(data_shard=1, parity_shard=0)` → **meta_stream** (id=3)
4. `UpsertPartition({ log=1, row=2, meta=3, range=["", "") })` → partition registered in manager

The PS then picks up the partition via its background `sync_regions` loop and opens it.

---

## Quick Start: 3-replica cluster

Same as above, but with 3 extent nodes and `--replication 3+0`.

```bash
MANAGER=./target/debug/autumn-manager-server
NODE=./target/debug/autumn-extent-node
PS=./target/debug/autumn-ps
SC=./target/debug/autumn-stream-cli
AC=./target/debug/autumn-client

pkill -f autumn-manager-server; pkill -f autumn-extent-node; pkill -f autumn-ps
rm -rf /tmp/autumn-etcd /tmp/d1 /tmp/d2 /tmp/d3 /tmp/autumn-ps

etcd --data-dir /tmp/autumn-etcd \
     --listen-client-urls http://127.0.0.1:2379 \
     --advertise-client-urls http://127.0.0.1:2379 &
sleep 0.5

$MANAGER --port 9001 --etcd 127.0.0.1:2379 &
sleep 0.5

$NODE --port 9101 --disk-id 1 --data /tmp/d1 --manager 127.0.0.1:9001 &
$NODE --port 9102 --disk-id 2 --data /tmp/d2 --manager 127.0.0.1:9001 &
$NODE --port 9103 --disk-id 3 --data /tmp/d3 --manager 127.0.0.1:9001 &
sleep 0.5

$SC register-node --addr 127.0.0.1:9101 --disk disk-1
$SC register-node --addr 127.0.0.1:9102 --disk disk-2
$SC register-node --addr 127.0.0.1:9103 --disk disk-3

$PS --psid 1 --port 9201 --manager 127.0.0.1:9001 \
    --data /tmp/autumn-ps --advertise 127.0.0.1:9201 &
sleep 1

$AC bootstrap --replication 3+0
sleep 1

$AC info
echo "hello" | $AC put mykey /dev/stdin
$AC get mykey
```

---

## CLI reference

### autumn-client

```
autumn-client --manager <ADDR> <COMMAND>
```

Default manager address: `127.0.0.1:9001`

| Command | Description |
|---------|-------------|
| `bootstrap [--replication 1+0] [--presplit 1:normal\|N:hexstring]` | Create streams and partition(s). `N:hexstring` splits the hex key space into N partitions. |
| `put <KEY> <FILE>` | Write key with value from file |
| `streamput <KEY> <FILE>` | Stream-put large file in 512KB chunks |
| `get <KEY>` | Read value (writes raw bytes to stdout) |
| `del <KEY>` | Delete key |
| `head <KEY>` | Show key metadata (length only) |
| `ls [--prefix P] [--start S] [--limit N]` | Scan keys |
| `split <PARTID>` | Trigger partition split (server picks split point) |
| `compact <PARTID>` | Trigger major compaction on a partition |
| `gc <PARTID>` | Trigger auto GC on a partition |
| `forcegc <PARTID> <EXTID>...` | Force GC of specific extent IDs |
| `format --listen <ADDR> --advertise <ADDR> <DIR>...` | Format disk dirs and register a new extent node |
| `wbench [--threads 4] [--duration 10] [--size 8192] [--nosync] [--report-interval 1] [--part-id ID] [--reuse-value true|false] [--channels-per-ps 1]` | Write benchmark; `--nosync` skips fsync; `--channels-per-ps` opens multiple independent gRPC channels to the same PS; outputs `write_result.json` with config/summary/ops samples/results |
| `rbench [--threads 40] [--duration 10] <RESULT_FILE>` | Read benchmark using keys from `write_result.json` |
| `info` | Show cluster state (nodes / streams / partitions) |

### autumn-stream-cli

```
autumn-stream-cli --manager <ADDR> <COMMAND>
```

Default manager address: `127.0.0.1:9001`

| Command | Description |
|---------|-------------|
| `register-node --addr <ADDR> --disk <UUID>` | Register an extent node with the manager |
| `create-stream [--data-shard N] [--parity-shard M]` | Create a new stream |
| `stream-info [--stream-id N]` | Show stream and extent metadata (omit `--stream-id` for all streams) |
| `append --stream-id N --data <STR>` | Append string data to a stream |
| `read --stream-id N [--length N]` | Read from a stream |
| `alloc-extent --node <ADDR> --extent-id N` | Pre-create an extent on an extent node |
| `commit-length --node <ADDR> --extent-id N [--revision N]` | Query the current write position of an extent |

---

## Binary reference

| Binary | Default port | Required flags | Purpose |
|--------|-------------|----------------|---------|
| `autumn-manager-server` | 9001 | — | Control plane: stream allocation, partition routing |
| `autumn-extent-node` | 9101 | `--data <DIR>` | Data plane: stores extent files on disk |
| `autumn-ps` | 9201 | `--psid <N>` | KV API: LSM-tree over stream layer |
| `autumn-client` | — | `--manager` | Admin CLI |
| `autumn-stream-cli` | — | `--manager` | Low-level stream layer CLI |

Key flags:

```
autumn-manager-server --port 9001 --etcd 127.0.0.1:2379

autumn-extent-node --port 9101 --disk-id 1 --data /tmp/d1 --manager 127.0.0.1:9001

autumn-ps --psid 1 --port 9201 --manager 127.0.0.1:9001 \
          --data /tmp/ps-wal --advertise 127.0.0.1:9201
```

---

## Operations

### KV operations

```bash
AC=./target/debug/autumn-client

echo "hello" > /tmp/v.txt
$AC put mykey /tmp/v.txt
$AC get mykey
$AC head mykey          # prints length
$AC ls --prefix my      # scan keys with prefix
$AC del mykey
```

### Large value (>4KB uses StreamPut)

```bash
dd if=/dev/urandom of=/tmp/big.bin bs=1024 count=100
$AC streamput bigkey /tmp/big.bin
$AC head bigkey         # expects: length: 102400
```

### Partition operations

```bash
# Get partition IDs from info
$AC info

# Split a partition (server picks mid-key automatically)
$AC split <PARTID>

# Trigger major compaction (clears overlap after split, reclaims space)
$AC compact <PARTID>

# Trigger auto GC (reclaims logStream extents with >40% discard)
$AC gc <PARTID>

# Force GC on specific extents
$AC forcegc <PARTID> <EXTID1> <EXTID2>
```

### Benchmarks

```bash
# Write benchmark: 4 threads, 10 seconds, 8KB values (with fsync)
$AC wbench --threads 4 --duration 10 --size 8192

# Write benchmark without fsync (higher throughput, tests group-commit batching)
$AC wbench --threads 16 --duration 10 --size 8192 --nosync

# Pin the run to one partition and print one sample every 2 seconds
$AC wbench --threads 256 --duration 10 --size 8192 --nosync --part-id <PARTID> --report-interval 2

# Keep the same partition pinned, but fan threads out across 8 independent gRPC channels
$AC wbench --threads 256 --duration 10 --size 8192 --nosync --part-id <PARTID> --channels-per-ps 8

# Disable payload reuse to measure client-side allocation overhead explicitly
$AC wbench --threads 64 --duration 10 --size 8192 --reuse-value false

# Read benchmark: load keys from previous wbench
$AC rbench --threads 40 --duration 10 write_result.json
```

`--nosync` disables `must_sync` on the write request. The partition server will skip the fsync on `log_stream` appends for those writes (unless another write in the same batch requires sync).

`--channels-per-ps` keeps the benchmark semantics unchanged, but pre-creates that many independent `PartitionKvClient<Channel>` connections per partition server and round-robins writer threads across them. This lets you test whether a single unary gRPC/HTTP2 connection is the batching bottleneck.

`write_result.json` now stores benchmark metadata in addition to per-op results:

```json
{
  "version": 1,
  "config": { "...": "..." },
  "summary": { "...": "..." },
  "ops_samples": [{ "second": 1, "ops": 22000, "cumulative_ops": 22000 }],
  "results": [{ "key": "bench_0_0", "start_time": 0.001, "elapsed": 0.011 }]
}
```

`rbench` accepts both the new wrapper format and the legacy top-level result array.

For write-path profiling, run the partition server and client with `RUST_LOG=info`. The partition server emits `partition write summary` once per second with batch fill ratio, `avg_admission_wait_ms` (tonic interceptor admission to `PartitionKv::put()` entry), handler-side pre-enqueue timing, queue wait, phase 1/2/3 timings (both per-batch and amortized per-op), and handler total time; the stream client emits `stream append summary` with mutex wait, extent lookup, fanout append, and retry counts. The write loop now follows Go `doWrites` batching semantics: it keeps absorbing requests until the batch exceeds the Go soft cap (`30 MiB` payload or `3 * write channel capacity` ops) or the single in-flight slot opens, so `avg_batch_size` / `fill_ratio` should be read against that soft cap.

### Add a new extent node to a running cluster

```bash
AC=./target/debug/autumn-client
NODE=./target/debug/autumn-extent-node

# Format the disk and register the node with the manager
$AC format --listen 127.0.0.1:9104 --advertise 127.0.0.1:9104 /tmp/d4
# Prints: node_id=N, disk_id=M, writes /tmp/d4/node_id and /tmp/d4/disk_id

# Start the extent node
$NODE --port 9104 \
      --disk-id $(cat /tmp/d4/disk_id) \
      --data /tmp/d4 \
      --manager 127.0.0.1:9001
```

---

## Tests

```bash
# Unit + fast integration tests
cargo test -p autumn-partition-server -- --nocapture

# Stream layer tests (start etcd first)
cargo test -p autumn-stream --test extent_append_semantics -- --nocapture
cargo test -p autumn-stream --test extent_restart_recovery -- --nocapture

# Manager integration tests (start etcd first)
cargo test -p autumn-manager --test integration -- --nocapture

# All tests
cargo test --workspace
```

---

## UCX / RDMA Mode (F100-UCX)

autumn-rs can carry hot RPC paths over RDMA via UCP/UCX. Default is TCP;
UCX is opt-in at compile time and runtime.

### Build host preconditions
- `libucx-dev` ≥ 1.16 (`pkg-config --modversion ucx`)
- At least one mlx5 (or other RDMA) HCA with a RoCE v2 GID on a routable
  IP (IPv4 or IPv6 GUA/ULA — link-local fe80::/10 doesn't work)
- Verify with `scripts/check_roce.sh` (exit 0 = ready;
  `--listen-candidates` lists valid bind IPs)

### Build with the UCX feature
    cargo build --workspace --features autumn-transport/ucx

The default build has zero UCX dependencies.

### Runtime selection
    AUTUMN_TRANSPORT=auto   # default; pick UCX if RDMA available, else TCP
    AUTUMN_TRANSPORT=tcp    # force TCP
    AUTUMN_TRANSPORT=ucx    # force UCX (panics if no RDMA on this host)

`auto` mode probes `ucp_context_print_info` for any of `rc_mlx5` /
`rc_verbs` / `dc_mlx5` / `ud_mlx5` / `ud_verbs`. Pure-TCP UCX (no RDMA
HCA) is treated as "unavailable" — there's no benefit layering UCX on
top of native TCP.

### Listen-address rule under UCX
The address passed to PartitionServer / ExtentNode / Manager (via the
binaries' `--port` flag, which becomes `0.0.0.0:<port>` or
`[::]:<port>`) must resolve to a netdev with a RoCE GID. Wildcards
(`0.0.0.0`, `[::]`) are fine — UCX will bind all routable interfaces.
For an explicit IP, use `scripts/check_roce.sh --listen-candidates`
to see what's valid.

The opt-in helper `autumn_transport::check_listen_addr(addr, kind)`
returns an `Err` with the candidate list if a binary is misconfigured —
binaries can call it after `init()` for a hard failure on bad addresses.

### Manual smoke (single-host loopback over UCX TCP fallback)

Loopback `127.0.0.1` has no RDMA route; UCX falls back to its own TCP
transport. Useful for proving the env switch + serve_ucx + connect path
end-to-end, but **not** representative of real perf.

    # All three in separate shells; each must export the env so init()
    # picks UCX. Use the autumn-server binary names (autumn-extent-node,
    # autumn-manager-server, autumn-ps).
    AUTUMN_TRANSPORT=ucx cargo run --features autumn-transport/ucx \
        -p autumn-server --bin autumn-extent-node \
        -- --data /tmp/ext0 --port 9101 --manager 127.0.0.1:9001

    AUTUMN_TRANSPORT=ucx cargo run --features autumn-transport/ucx \
        -p autumn-server --bin autumn-manager-server -- --port 9001

    AUTUMN_TRANSPORT=ucx cargo run --features autumn-transport/ucx \
        -p autumn-server --bin autumn-ps \
        -- --psid 1 --port 9201 --manager 127.0.0.1:9001 --data /tmp/ps1

Each binary's startup log must contain
`autumn-transport: init kind=Ucx`. If any prints `Tcp` the env was not
honored; check that the feature flag is on and re-run.

### Perf measurement

Transport-level micro-bench (no cluster needed):

    ./scripts/perf_ucx_baseline.sh transport

Cluster-level A/B (requires `cluster.sh start N` first; honest perf
needs a 2-host setup since loopback bypasses the NIC):

    ./scripts/perf_ucx_baseline.sh cluster

Single-host loopback numbers from this build host (`dc62-p3-t302-n014`,
10× mlx5 HCAs):

    TCP (loopback): ping_pong 64B = 6.88 μs/op | 2839 MB/s @ 1MB
    UCX (rc_mlx5):  ping_pong 64B = 24.17 μs/op | 1133 MB/s @ 1MB

UCX is *slower* in single-host loopback because TCP loopback bypasses
the NIC entirely (kernel memcpy) while UCX rc_mlx5 hits the real HCA
even for loopback (PCIe DMA + transmit + DMA back). The expected perf
win materialises only when network latency dominates — i.e. across
hosts. Cross-host A/B is a separate deploy session.

### Cluster-level perf_check — 2×2×2×2 matrix

`./perf_check.sh` (no flags) runs the full **2×2×2×2 = 16-run matrix**:
transport ∈ {tcp, ucx} × partitions ∈ {1, 8} × pipeline-depth ∈ {1, 8}
× value size ∈ {4K, 8M}. Baselines are per-combo
(`perf_baseline_${transport}_p${parts}_d${depth}_s${size}${_shm?}.json`).
The cluster is restarted per (transport, partitions) but reused across
pipeline-depth and size (both are client-side knobs only).

Narrow the matrix with `--tcp` / `--ucx` / `--partitions N` /
`--pipeline-depth N` / `--size {4k|8m|N}` / `--threads N`. Storage
defaults to `/tmp/autumn-rs`; pass `--shm` for `/dev/shm/autumn-rs`
(RAM tmpfs; fsync is a no-op).

The script sets two environment defaults (overridable by the caller)
to keep UCX healthy at non-trivial message sizes:
- `ulimit -l unlimited` — RDMA pins memory via `ibv_reg_mr`;
  the common distro default (8 MB) is exhausted by 8 MB payload runs.
- `UCX_TLS=^sysv,^posix` — this environment blocks both shared-memory
  transports for >eager messages:
    - sysv: `mm_sysv.c:59 shmat(...) failed: Invalid argument`
            (IPC namespace denies shmat)
    - posix: `mm_posix.c:233 open(/proc/<peer_pid>/fd/<N>) failed: No
             such file or directory` (peer-fd visibility restricted)
  Either one being chosen by UCX for an 8 MB rendezvous wedges the
  send for tens of seconds. Excluding both lets UCX fall back to
  `cma` (Cross-Memory-Attach, ~17 GB/s in ucx_perftest) for intra-host
  bulk and `tcp` for control.

**Same-host UCX caveat**: 127.0.0.1 / ::1 isn't on a RoCE-attached
NIC, so UCX cannot use rc_mlx5 for our cluster — verified by
`ucx_perftest` directly: even with two physical mlx5 HCAs and strict
`UCX_TLS=rc_mlx5,self`, the local rc_mlx5 interfaces report `no
connect to iface` (HCA driver doesn't bridge two cards on the same
host). With `UCX_TLS=^sysv,^posix` UCX falls back to cma — fast but
not actual RDMA. Real RDMA numbers require cross-host deployment
(F100-UCX gate c).

**Scaling rule (thread-per-core):** total in-flight ops = threads ×
pipeline-depth. Prefer fewer threads with deeper pipeline over many
threads with shallow pipeline — better cache locality and, on UCX,
avoids rdma_cm saturation at > ~100 concurrent connects.
`perf_check.sh` defaults to `--threads 16` for this reason; overshoot
to `--threads 256` will make UCX runs hang (rdma_cm `Destination
unreachable` at connect time) and TCP runs waste CPU on no perf win.

Measured default matrix on this host (Xeon 8457C, 192 CPU, mlx5_0
RoCEv2, disk, 3-replica, --nosync, threads=16):

4 KB values — small-op ceiling:

| transport | partitions | pipeline-depth | write ops/s | read ops/s |
|---|---|---|---|---|
| TCP | 8 | 8 | **141,988** | **1,112,102** |
| UCX | 8 | 8 | 129,199 | 763,697 |
| TCP | 8 | 1 | 69,371 | 466,833 |
| UCX | 8 | 1 | 61,060 | 276,573 |

8 MB values — bandwidth ceiling (reads are VP-resolved via
`read_bytes_from_extent`, so they hit the log_stream path too):

| transport | partitions | depth | write ops/s | write MB/s | read ops/s | read MB/s |
|---|---|---|---|---|---|---|
| TCP | 8 | 8 | 199.5 | **1,596** | 91.3 | 730 |
| TCP | 8 | 1 | 164.6 | 1,317 | 90.4 | 723 |
| UCX | 8 | 8 | 35.0 | 280 | 34.3 | 275 |
| TCP | 1 | 8 | 70.0 | 560 | 63.2 | 506 |
| TCP | 1 | 1 | 71.6 | 573 | 59.2 | 474 |

(TCP loopback wins decisively at 8 MB on this host — kernel memcpy
runs at PCIe bandwidth while UCX-over-TCP has to traverse multiple
userspace/kernel hops. On a real cross-host deploy, UCX RDMA would
decouple from the CPU and typically match or beat this ceiling.)

Off-matrix sweet spots confirmed at 4 KB: UCX p=32 × 16t × d=16 →
1.71 M reads; TCP p=32 × 16t × d=16 → 1.81 M reads.

---

## Notes

- `IoMode::IoUring` is not yet implemented; extent nodes use `IoMode::Standard`.
- Without `--etcd`, manager runs in-memory only (metadata lost on restart).
- Erasure coding (`parity_shard > 0`) is not yet implemented; use `parity_shard=0`.
- There is currently no automatic partition server failover; if a PS crashes, restart it
  with the same `--psid` and it will re-register and reload its partitions.
