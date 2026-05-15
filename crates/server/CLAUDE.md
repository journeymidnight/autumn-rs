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
autumn-extent-node --data /path/to/data [--port 9101] [--disk-id <UUID>] [--manager 127.0.0.1:9001]
```

- `--data`: directory where extent files are stored (`extent-{id}.dat` + `extent-{id}.meta`)
- `--disk-id`: identifies this disk to the manager (used for replica placement); auto-generated UUID if not provided
- `--manager`: manager address for self-registration (`RegisterNode`) and for fetching ExtentInfo during recovery/re-avali
- On startup: registers itself with the manager, then serves `ExtentService` gRPC

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

Admin CLI for the full cluster. Connects to the manager for region routing, then connects directly to partition servers.

```
autumn-client --manager 127.0.0.1:9001 <COMMAND>
```

| Command | Description |
|---------|-------------|
| `bootstrap [--replication 3+0] [--log-ec K+M] [--row-ec K+M] [--presplit 1:normal\|N:hexstring]` | Create streams + partition(s). `--log-ec`/`--row-ec` set EC shape for log/row streams (replicates=K+M); meta always plain replication. `cluster.sh` auto-sets EC based on replica count. |
| `set-stream-ec --stream <ID> --ec K+M` | Change an existing stream's EC policy. F203: this only mutates `MgrStreamInfo.ec_data_shard / ec_parity_shard`. The post-F203 `ec_conversion_dispatch_loop` is **drain-only** — actual conversion happens per-extent via `force-ec-convert` (or an external Python controller iterating over advisory candidates). Set-stream-ec is the prerequisite for force-ec-convert: `handle_force_ec_convert` rejects extents on streams with `ec_parity_shard == 0`. |
| `force-ec-convert --extent <EXTID>` | F203: per-extent EC trigger. Persists a rich `pending_ec_dispatch` marker; the next `ec_conversion_dispatch_loop` tick (~5 s) drains it via the F198 replay path. Requires the extent's parent stream to have EC policy (`ec_parity_shard > 0`); use `set-stream-ec` first if needed. |
| `put <KEY> <FILE>` | Write key with value from file |
| `put-stream [--chunk-size N] <KEY> <FILE-or->>` | F186 ceph-style client-side stripe-put (4 MiB chunk default). The modern path for large values; F205 removed the legacy `streamput` single-RPC alias. |
| `get-stream [--chunk-size N] [--out FILE] <KEY>` | Chunked stream get. |
| `get <KEY>` | Read value, write to stdout |
| `del <KEY>` | Delete key |
| `head <KEY>` | Show key metadata (length) |
| `ls [--prefix P] [--start S] [--limit N]` | List/scan keys |
| `split <PARTID>` | Trigger partition split |
| `compact <PARTID>` | Trigger major compaction on a partition |
| `gc <PARTID>` | Trigger automatic GC on a partition |
| `forcegc <PARTID> <EXTID>...` | Force GC of specific extent IDs on a partition |
| `format --listen <ADDR> --advertise <ADDR> <DIR>...` | Format disk dirs, register extent node with manager |
| `wbench [--threads 4] [--duration 10] [--size 8192]` | Concurrent write benchmark; outputs write_result.json |
| `rbench [--threads 40] [--duration 10] <RESULT_FILE>` | Concurrent read benchmark using keys from write_result.json |
| `info` | Show cluster state (nodes, streams, partitions) |

**Key routing**: `resolve_key(key)` calls `GetRegions()` on the manager, binary-searches sorted partitions by `start_key`, returns `(part_id, ps_addr)`. Connects lazily to PS via `PartitionKvClient`.

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

For a fresh cluster:
1. Start `autumn-extent-node` instances (at least as many as `data_shard + parity_shard`)
2. Start `autumn-manager-server`
3. Run `autumn-client bootstrap` to create streams and initial partition
4. Start `autumn-ps` with a unique `--psid`

## Common CLI Patterns

```bash
# Start a minimal 1-node cluster (no replication, testing only)
autumn-extent-node --data /tmp/extent0 --port 9101 --manager 127.0.0.1:9001 &
autumn-manager-server --port 9001 &
autumn-client --manager 127.0.0.1:9001 bootstrap --replication 1+0
autumn-ps --psid 1 --port 9201 --manager 127.0.0.1:9001 --data /tmp/ps1 &

# Write and read
echo "hello world" > /tmp/val.txt
autumn-client --manager 127.0.0.1:9001 put mykey /tmp/val.txt
autumn-client --manager 127.0.0.1:9001 get mykey

# Inspect cluster
autumn-client --manager 127.0.0.1:9001 info
```
