# autumn-server Crate Guide

## Purpose

Binary-only crate. Contains the executable entry points that wire together the library crates, including one-off repair tooling. No library code lives here — all logic is in the other crates.

## Binaries

### `autumn-manager-server` (`src/bin/manager.rs`)

**Default port**: 9001

```
autumn-manager-server [--port 9001] [--etcd 127.0.0.1:2379,...]
```

- Without `--etcd`: in-memory only (metadata lost on restart, no leader election)
- With `--etcd`: persistent mode — connects to etcd, replays state on start, runs leader election loop
- Serves both `StreamManagerService` and `PartitionManagerService` on the same port
- Also registers gRPC reflection (uses `FILE_DESCRIPTOR_SET` from `autumn-proto`)

### `autumn-extent-node` (`src/bin/extent_node.rs`)

**Default port**: 9101

```
autumn-extent-node --data /path/to/data [,/path/to/data2,...] [--port 9101] [--manager 127.0.0.1:9001]
```

- `--data`: directory where extent files are stored (`extent-{id}.dat` + `extent-{id}.meta`). Comma-separated or repeated `--data` flags for multi-disk EN.
- `--manager`: manager address. F214-D: also used at startup for the cluster_id cross-check.
- **F214-D startup requirements:** each `--data` dir MUST be pre-formatted via `autumn-op format` — the EN refuses to start without the `cluster_id` + `disk_id` sentinel files. Pre-flight checks:
  1. `read_and_verify_cluster_id` (sync, before shard threads): reads `cluster_id` file from each dir, verifies they all agree.
  2. `verify_manager_cluster_id` (async, shard 0 only): fetches the manager's `cluster_id` via `MSG_GET_CLUSTER_ID` and refuses on mismatch.
- The pre-F214 `--disk-id N` flag was removed; the EN reads disk_id from each dir's `disk_id` sentinel file.

### `autumn-ps` (`src/bin/partition_server.rs`)

**Default port**: 9201

```
autumn-ps --psid <ID> --manager 127.0.0.1:9001 [--port 9201] [--data /tmp] [--advertise <ADDR>]
```

- `--psid`: **required** — unique partition server ID (must be unique in the cluster)
- `--data`: directory for local WAL files (`part-{id}.wal`)
- `--advertise`: address announced to the manager (useful when listening on 0.0.0.0 but manager needs a routable address)
- Startup sequence:
  1. `PartitionServer::connect_with_advertise()`: connects to manager
  2. `RegisterPs(ps_id, advertise_addr)`: announces itself
  3. `GetRegions()`: finds assigned partitions
  4. `open_partition()` for each assigned partition (replay from streams)
  5. Serves `PartitionKv` gRPC

### `autumn-client` (`src/bin/autumn_client.rs`)

Data-plane CLI: KV ops + benchmarks. **F213 split:** all admin / observability subcommands moved to the sibling binary `autumn-op` (see below). `autumn-client op <anything>` and the legacy spellings (`autumn-client split 7`, `autumn-client info`, ...) all print a navigation hint and exit non-zero.

```
autumn-client --manager 127.0.0.1:9001 <COMMAND>
```

| Command | Description |
|---------|-------------|
| `put <KEY> <FILE>` | Write key with value from file |
| `put-stream [--chunk-size N] <KEY> <FILE-or->>` | F186 ceph-style client-side stripe-put (4 MiB chunk default). The modern path for large values; F205 removed the legacy `streamput` single-RPC alias. |
| `get-stream [--chunk-size N] [--out FILE] <KEY>` | Chunked stream get. |
| `get <KEY>` | Read value, write to stdout |
| `del <KEY>` | Delete key |
| `head <KEY>` | Show key metadata (length) |
| `ls [--prefix P] [--start S] [--limit N]` | List/scan keys |
| `wbench [--threads 4] [--duration 10] [--size 8192]` | Concurrent write benchmark; outputs write_result.json |
| `rbench [--threads 40] [--duration 10] <RESULT_FILE>` | Concurrent read benchmark using keys from write_result.json |
| `perf-check [--threads N] [--baseline FILE] [--threshold T] [--update-baseline] [--partitions N] [--pipeline-depth K] [--group-commit-cap N]` | Regression-gated bench: compares against a JSON baseline, fails if throughput < threshold. |
| `op <subcmd> ...` | Stub: prints a hint pointing at `autumn-op <subcmd>` and exits 1. No subprocess fork. |

**Architectural rule (F213):** `autumn-client` MUST NOT call `mgr_call(MSG_*)` for admin / observability RPCs. Greppable invariant — `grep -cE 'mgr_call\(MSG_' crates/server/src/bin/autumn_client.rs` must be 0. If future autumn-client functionality requires op data, do not add direct manager calls — open a separate proposal for shared-library extraction or subprocess delegation.

**Key routing**: `resolve_key(key)` calls `GetRegions()` on the manager, binary-searches sorted partitions by `start_key`, returns `(part_id, ps_addr)`. Connects lazily to PS via `PartitionKvClient`.

### `autumn-op` (`src/bin/autumn_op.rs`)

F211-G + F213 admin / observability CLI. The canonical interface to the manager control plane. The Python policy script (`python/node_policy.py`) shells out to this binary for all RPC traffic — autumn-op is its rkyv codec.

```
autumn-op [--manager 127.0.0.1:9001] [--json] <COMMAND>
```

| Category | Commands |
|----------|----------|
| Read / observability | `list-nodes`, `extent-health [--node N] [--all]`, `list-ec-markers`, `recovery-stats`, `audit-log [--op N --node N --since/--until --limit L]`, `info [--part PID] [--detail]`, `policy-candidates` |
| Node lifecycle (F211) | `fence-node <id> --reason ... --by ... [--force]`, `maintenance <id> --reason ... --by ... [--expire UNIX_TS]`, `unfence <id> --by ...`, `remove <id> --by ...` |
| Cluster / partition admin (F213 + F214) | `bootstrap [--replication 3+0] [--log-ec K+M] [--row-ec K+M] [--presplit 1:normal\|N:hexstring]`, `set-stream-ec --stream <ID> --ec K+M`, `force-ec-convert --extent <EXTID>`, `split <PARTID>`, `merge <SURVIVOR_PARTID> <VICTIM_PARTID>`, `compact <PARTID>`, `gc [--ratio R --max-size B --stream-debt B --empty-only] <PARTID>`, `forcegc <PARTID> <EXTID>...`, `format --listen <ADDR> --advertise <ADDR> <DIR>...` |

**F214-C**: `register-node` subcommand removed; merged into `format`. The legacy spelling routes to a migration stub that prints + exits 1 BEFORE connecting to the manager. `MSG_REGISTER_NODE` wire RPC unchanged — `format` calls it internally.

All commands accept `--json` for Python policy consumption. The JSON schema for `info` mirrors the F205 layout (top-level `nodes / extents / streams / partitions` arrays); legacy `jq` filters and `python3 -c "import json; ..."` snippets in the README continue to work.

**Why two binaries (HDFS-style sibling pattern, not umbrella):** data-plane CLI churn shouldn't drag operator tooling along; rkyv schema stays in exactly one place (`crates/rpc/src/manager_rpc.rs`); Python automation has a stable wire format to depend on.

### `autumn-stream-cli` (`src/bin/stream_cli.rs`)

Low-level stream layer CLI for debugging and manual testing. Bypasses the partition layer entirely.

```
autumn-stream-cli --manager 127.0.0.1:9001 <COMMAND>
```

| Command | Description |
|---------|-------------|
| `register-node --addr <ADDR> --disk <UUID>` | Register an extent node |
| `create-stream [--data-shard N] [--parity-shard M]` | Create a new stream |
| `stream-info [--stream-id N]` | Show stream/extent metadata (omit for all streams) |
| `append --stream-id <ID> --data <STR>` | Append string data to a stream |
| `read --stream-id <ID> [--length N]` | Read from stream |
| `alloc-extent --node <ADDR> --extent-id N` | Pre-create an extent on an extent node |
| `commit-length --node <ADDR> --extent-id N [--revision N]` | Query current write position of an extent |

### `repair-metastream` (`src/bin/repair_metastream.rs`)

One-off repair CLI for historical partition checkpoint corruption.

```
repair-metastream --manager 127.0.0.1:9001 --meta-stream <ID> \
  --vp-extent <ID> --vp-offset <OFF> --sst <extent:offset:len> [--sst ...]
```

- Connects as a normal `StreamClient` owner, reads and prints the current last
  `TableLocations` record from the target `meta_stream`, prints the requested
  replacement, then appends the new checkpoint.
- `--dry-run` prints current + target state without writing.
- Intended for offline/preserved-data repair only; normal PS recovery should
  not rely on it.

## Startup Ordering

For a fresh cluster (F214):
1. Start `autumn-manager-server` first — it CAS-imprints the cluster_id on first leader-promotion.
2. For each EN: run `autumn-op format --advertise HOST:PORT <DIR>...` BEFORE launching `autumn-extent-node`. `format` fetches the cluster_id, allocates disk_uuid(s), calls `MSG_REGISTER_NODE`, and stamps the per-dir sentinel files (`cluster_id`, `disk_uuid`, `node_id`, `disk_id`).
3. Launch `autumn-extent-node` for each formatted EN. It refuses to start without the sentinel files; on startup it cross-checks the stamped cluster_id against the manager's.
4. Run `autumn-op bootstrap` to create streams and initial partition.
5. Start `autumn-ps` with a unique `--psid`.

Newly-registered nodes start in `NodeAutoState::Suspend`. The manager's 10-s `disk_status_update_loop` flips them to `Online` on the first successful `df` response. `select_nodes` gates allocation on `Online` state but falls back to the full node set when none are Online (cold-leader / fresh-bootstrap path).

## Common CLI Patterns

```bash
# Start a minimal 1-node cluster (no replication, testing only)
autumn-manager-server --port 9001 &
autumn-op --manager 127.0.0.1:9001 format \
    --listen :9101 --advertise 127.0.0.1:9101 /tmp/extent0
autumn-extent-node --data /tmp/extent0 --port 9101 --manager 127.0.0.1:9001 &
autumn-op --manager 127.0.0.1:9001 bootstrap --replication 1+0
autumn-ps --psid 1 --port 9201 --manager 127.0.0.1:9001 --data /tmp/ps1 &

# Write and read (data plane = autumn-client)
echo "hello world" > /tmp/val.txt
autumn-client --manager 127.0.0.1:9001 put mykey /tmp/val.txt
autumn-client --manager 127.0.0.1:9001 get mykey

# Inspect cluster (op plane = autumn-op)
autumn-op --manager 127.0.0.1:9001 info
```
