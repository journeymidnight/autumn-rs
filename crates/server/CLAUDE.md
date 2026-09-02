# autumn-server Crate Guide

## Purpose

Binary-only crate: the executable entry points that wire the library crates together, plus one-off repair tooling. No library logic lives here.

## Binaries

### `autumn-manager-server` (`src/bin/manager.rs`)

**Default port**: 9001

```
autumn-manager-server [--port 9001] [--listen 0.0.0.0] [--transport tcp|ucx] [--etcd 127.0.0.1:2379,...]
```

- Without `--etcd`: in-memory only (metadata lost on restart, no leader election). With `--etcd`: persistent — connects, replays state, runs the leader-election loop.
- Serves `StreamManagerService` + `PartitionManagerService` on the same port, plus gRPC reflection.
- `--metrics-port <P>` / `--metrics-listen <H>`: opt-in Prometheus `/metrics` (unauthenticated; pin to 127.0.0.1 when the RPC plane is on 0.0.0.0).
- The leader-fenced **auto-policy controller** runs in-process (leader only). `--auto-policy-default <NAME>` seeds an Armed policy on a fresh cluster; arming is per-policy (`autumn-op auto-policy activate --arm`). The **web dashboard is no longer served by the manager** — it is a standalone app (`examples/dashboard`) that talks to the manager only through `autumn-op`. Runbook: `docs/ops.md`.
- Authz (opt-in): `--auth-signing-key-file <FILE>` enables data-plane authz (keys from `autumn-op gen-signing-key`). `--admin-token` / `--admin-token-file` gate the tenancy/authz admin RPCs (refused without one). `--auth-protected-prefix <P>` (repeatable) marks default-DENY prefixes. `--auth-token-ttl-secs` / `--auth-clock-skew-secs` tune minted tokens.

### `autumn-extent-node` (`src/bin/extent_node.rs`)

**Default port**: 9101

```
autumn-extent-node --data DIR[,DIR2,...] [--port 9101] [--manager 127.0.0.1:9001] --advertise HOST:PORT
```

- `--data`: directory holding extent files (`extent-{id}.dat` + `extent-{id}.meta`); comma-separated or repeated for a multi-disk EN.
- `--advertise HOST:PORT` is **REQUIRED whenever `--manager` is given** (`main()` bails otherwise) — a `--manager` run that self-registered nothing would sit at an empty location forever. HOST must be an IP (DNS-free); PORT must equal `--port`. `--manager`-less offline/test runs are exempt.
- **Self-registration**: at startup (after cluster-id verification, before serving) the EN registers its live address + shard ports with the manager, keyed by its stable `node_uuid`. The manager updates the location IN PLACE, so a reshard or fresh pod IP is picked up on the next boot — the **EN, not `format`, is the sole source of location**. `handle_df` echoes the identity so `node_health_loop` self-heals drift.
- **Static shard ports**: shard count = the `--cpuset` / `--cpu-start` core count; sibling shard *i* listens on `port + i * shard_stride` (`--shard-stride`, default 10). Control ports default to `port + 1000` (override `--control-port`).
- **Requires pre-formatting**: each `--data` dir MUST be formatted by `autumn-op format` first — the EN refuses to start without the sentinel files (`cluster_id`, `disk_uuid`, `node_id`, `disk_id`, `node_uuid`). It cross-checks each dir's `cluster_id`, then fetches the manager's via `MSG_GET_CLUSTER_ID` and refuses on mismatch. `disk_id` comes from the sentinel; `--disk-id` and `--shards` are migration-error stubs (exit 2).

### `autumn-ps` (`src/bin/partition_server.rs`)

**Default port**: 9201

```
autumn-ps --psid <ID> --manager 127.0.0.1:9001 [--port 9201] [--data /tmp] [--advertise <ADDR>]
```

- `--psid`: **required**, unique partition-server ID across the cluster. `--data`: directory for local WAL files (`part-{id}.wal`). `--advertise`: address announced to the manager (when listening on 0.0.0.0 but the manager needs a routable address).
- Startup: connect to manager → `RegisterPs(ps_id, advertise_addr)` → `GetRegions()` for assigned partitions → `open_partition()` (replay from streams) each → serve `PartitionKv` gRPC.

### `autumn-client` (`src/bin/autumn_client/`)

Data-plane CLI: KV ops + benchmarks. Directory bin (Cargo target `src/bin/autumn_client/main.rs`). All admin / observability subcommands live in the sibling binary `autumn-op`; `autumn-client op <anything>` and legacy admin spellings print a hint and exit non-zero.

```
autumn-client --manager 127.0.0.1:9001 <COMMAND>
```

| Command | Description |
|---------|-------------|
| `put <KEY> <FILE>` | Write key with value from file |
| `put-stream [--chunk-size N] <KEY> <FILE-or->>` | Client-side stripe-put for large values (4 MiB chunk default) |
| `get-stream [--chunk-size N] [--out FILE] <KEY>` | Chunked stream get |
| `get <KEY>` | Read value to stdout |
| `del <KEY>` | Delete key |
| `head <KEY>` | Show key metadata (length) |
| `ls [--prefix P] [--start S] [--limit N]` | List/scan keys |
| `perf-check [--threads N] [--size B] [--bulk N] [--baseline FILE] [--threshold T] [--update-baseline] [--partitions N] [--pipeline-depth K] [--group-commit-cap N]` | Regression-gated bench: pure write phase then pure read phase vs a JSON baseline; fails if throughput < threshold. `--size` accepts a byte suffix (`4k`/`8m`, same grammar as `gc --max-size`). `--bulk N` (>0) drives each round through one `put_many`/`get_many_into(N items)` — a live batch-size knob (replaces the removed `--batch-put`/`--batch-get`/`--put-many`). |
| `ycsb [--threads 32] [--duration 30] [--size 1024] [--partitions N] [--pipeline-depth 16] [--read-ratio 0.5] [--key-dist zipfian\|uniform] [--records 100000] [--rmw]` | YCSB-equivalent MIXED workload: LOAD then one mixed R/W run; keys partition-local per thread; our driver, not reference Java YCSB |

**Rule:** `autumn-client` MUST NOT call `mgr_call(MSG_*)` for admin / observability RPCs — that keeps data-plane CLI churn from dragging operator tooling along. Greppable invariant: `grep -rcE 'mgr_call\(MSG_' crates/server/src/bin/autumn_client/` must be 0. New op-data needs go through shared-library extraction or subprocess delegation, not direct manager calls.

**Key routing**: `resolve_key(key)` calls `GetRegions()`, binary-searches sorted partitions by `start_key`, returns `(part_id, ps_addr)`, connects lazily via `PartitionKvClient`.

### `autumn-op` (`src/bin/autumn_op/`)

Admin / observability CLI — the canonical interface to the manager control plane. Directory bin (Cargo target `src/bin/autumn_op/main.rs`). The Python ops tooling shells out to this binary for all RPC traffic, so the wire schema stays in exactly one place (`crates/rpc/src/manager_rpc.rs`).

```
autumn-op [--manager 127.0.0.1:9001] [--json] [--transport tcp|ucx] [--admin-token TOK | --admin-token-file FILE] <COMMAND>
```

Global `--admin-token` / `--admin-token-file`: attached as a signed payload prefix to mutating RPCs; read-only commands ignore it. `--json` on every command (the `info` schema is top-level `nodes / extents / streams / partitions` arrays). Global `--wait [--timeout SECS]` (default 600) applies to the async op triggers below.

**Async ops.** The seven long-running ops — `split` / `merge` / `rebalance` / `compact` / `gc` / `forcegc` / `force-ec-convert` — are **submitted through the leader's op-ledger** and return an `op_id` immediately (non-blocking). Query with `ops status <OP_ID>` or `ops list [--active] [--kind K] [--limit N]`; each op's state (pending/running/succeeded/failed/unknown) + the **failure reason** is retained (compact/gc/forcegc outcomes ride back on the PS load heartbeat). Pass global `--wait` to block until terminal and exit on the real outcome (non-zero on failure) — scripts/`presplit` that need the blocking error use it. A leader change answers an old id `unknown` (terminal history is in `audit-log`).

| Category | Commands |
|----------|----------|
| Read / observability | `list-nodes`, `df`, `cluster-version`, `extent-health [--node N] [--all]`, `list-ec-markers`, `recovery-stats`, `audit-log [--op N --node N --since/--until --limit L]`, `info [--part PID] [--detail]`, `policy-candidates` |
| Node lifecycle | `fence-node <id> --reason ... --by ... [--force]`, `maintenance <id> --reason ... --by ... [--expire TS]`, `unfence <id> --by ...`, `remove <id> --by ...` |
| Cluster / partition admin | `bootstrap [--replication 3+0] [--log-ec K+M] [--row-ec K+M] [--presplit 1:normal\|N:hex]`, `set-stream-ec --stream <ID> --ec K+M`, `force-ec-convert --extent <EXTID>`, `split <PARTID>`, `presplit <ns> <tenant> <rule>`, `merge <SURVIVOR> <VICTIM> [--force]`, `rebalance`, `compact <PARTID>`, `gc [--ratio R --max-size B --stream-debt B --empty-only] <PARTID>`, `forcegc <PARTID> <EXTID>...`, `format <DIR>...`, `upgrade-version --to <V>` |
| Auth / tenancy | `gen-signing-key [--kid K]`, `principal-create --principal P --grant P... [--admin-token]`, `principal-delete --principal P`, `principal-list`, `mint-token --principal P --credential ...`, `namespace-create --name N [--tenant T] [--presplit hex,…] [--admin-token]`, `namespace-delete --name N`, `namespace-list` |
| Auto-policy controller | `auto-policy status`, `auto-policy activate <NAME> [--arm]` (`--arm` = Armed, else DryRun), `auto-policy deactivate`, `auto-policy upsert <NAME> --switches split,gc,… [--interval N --cooldown N --max N --desc "…"]` (create/replace a custom policy), `auto-policy delete <NAME>`. Leader-routed |
| Async op-ledger | `ops status <OP_ID>` (one op, `unknown` if this leader doesn't know it), `ops list [--active] [--kind split\|merge\|rebalance\|compact\|gc\|forcegc\|ec\|recovery] [--limit N]`. The seven op triggers above submit here + print an `op_id`; global `--wait` blocks to terminal. **`recovery` is auto-dispatched** (never submitted — submit refuses it): it appears on its own with `attempts=N` and, while still `running`, the last failure `ERROR[code]: reason`. Leader-routed |

`format` is IDENTITY-ONLY: no location flags — it stamps the sentinels and registers an EMPTY location; the EN self-registers its real location. `register-node` is a migration stub that hints and exits 1 before connecting.

**CLI conventions (canonical + accepted aliases).** Both binaries hand-parse args (no clap; `autumn_op/args.rs`, `autumn_client/args.rs`). Canonical subcommands are kebab-case; the old snake_case / no-separator spellings stay as accepted aliases (`policy-candidates`←`policy_candidates`/`policy`, `auto-policy`←`auto_policy`, `put-stream`←`putstream`, `get-stream`←`getstream`). Canonical flag names per concept, with the older spelling kept as an alias: `--namespace` (←`--scope`, client KV scope), `--tenant` (←`--with-tenant`, `namespace-create`), `--principal` (←`--tenant`, `mint-token`). Byte-size flags accept an optional binary suffix (`4k`/`8m`/`1gib`) across both binaries (`gc --max-size`/`--stream-debt`, `perf-check`/`ycsb --size`). Three `autumn-client` subcommands are INTERNAL zero-copy verification paths, deliberately omitted from `usage()`: `put-bulk`, `direct-get`, `bulk-get` (they mirror `put`/`get` through the ZC codepaths). NOT YET unified (follow-ups): the verb-noun vs noun-verb split (`list-nodes`/`fence-node` vs `namespace-create`), the per-command `--admin-token` duplicating the global one, and `split`'s three targeting flags (`--at`/`--at-hex`/`--at-raw-hex`).

### `autumn-s3` (`src/bin/autumn_s3/`)

Read-only, unauthenticated S3 endpoint over the `fs/` tree, in its own process.
It exists so inference engines with no loader plugin seam (SGLang, FreeToken)
can stream weights through their built-in `--load-format runai_streamer`, which
speaks S3 and nothing else; every other S3 tool reads autumn through it as a
side effect. Serves only what that streamer issues — `ListObjectsV2`, ranged
`GetObject`, whole `GetObject` — and answers every mutating verb with a
parseable S3 `NotImplemented`. Requests are served whatever their
`Authorization` header says, including none.

```
autumn-s3 --manager <host:port> [--listen 0.0.0.0] [--port 9000] [--workers N]
          [--host <daemon-identity>] [--credential-file <path>]
          [--direct-read true|false]
```

- Reads go through `autumn-fuse`'s `core` layer — the same one the PyO3
  `autumn.Fs` binding uses — so lane striping, EN-direct reads and authz apply
  unchanged. An adapter over the partition layer, not a second data plane.
- `--workers` (default `min(cores, 8)`) accept threads, each with its own compio
  runtime, its own `FsState` and an SO_REUSEPORT listener on the same port. One
  thread caps an AWS-CRT client at ~40% of the read path; the knee is at 4.
- `--host` names the daemon identity each worker registers under (the entrypoint
  passes `s3-$HOSTNAME`); workers append their index.
- Being a binary of this package rather than an example also means a plain
  `cargo build --release` produces it — examples were never in
  `default-members`, so it used to be skipped, which is the shape of the
  stale-release-binary trap the chaos and perf runbooks warn about.
- **Its own process, on purpose.** Hosting it inside `autumn-ps` behind a flag
  was implemented, verified end to end, and reverted: the release profile sets
  `panic = "abort"` so a gateway panic would abort the partition server, the
  transport is a process-wide `OnceLock` the PS initialises (a `--transport ucx`
  server would hand the gateway UCX connections, which nothing has exercised),
  and `--cpuset` cannot confine threads that pin nothing. See
  `claude-progress.txt` before proposing the move again.

### `autumnfs` (`src/bin/autumnfs.rs`)

Offline POSIX-ish CLI over the fuse on-disk schema, **without** mounting — `ls / mkdir / cp` from any shell against a running cluster, for inspection, scripted setup, CI seeding.

```
autumnfs [--manager 127.0.0.1:9001] [--transport tcp|ucx] [--credential-file FILE] <SUBCMD>
```

| Subcommand | Description |
|------------|-------------|
| `ls <PATH> [--long]` | List directory entries (default `/`) |
| `stat <PATH>` | Show inode metadata (size, ino, type, parent) |
| `mkdir <PATH>` | Create a directory (parents must exist) |
| `touch <PATH>` | Create empty file (no-op if it exists) |
| `cat <PATH>` | Read file to stdout |
| `put <LOCAL> <REMOTE>` | Upload local file |
| `get <REMOTE> <LOCAL>` | Download to local file (`-` = stdout) |
| `rm <PATH>` | Remove a file or empty directory |

- **Namespace-first binding**: connects via `ClusterClient::connect(mgr, "fs")`, so the binding prepends `fs/` to every relative fuse key (and strips it off range results) — the same single global keyspace a fuse mount uses, so writes here are visible to a mount. No `--tenant`; this CLI sees the whole `fs/` namespace.
- **Authz**: `--credential-file` (`<principal>\n<hex>`, from `autumn-op principal-create`) is REQUIRED when the cluster protects `fs/` (connects via `connect_with_credential`, fails fast if the credential doesn't cover `fs/`); omit on an authz-off cluster.
- **Inodes** come from the MANAGER's global counter (`alloc_inodes`) — the same crash-safe source the fuse mount and PyO3 `autumn.Fs` use, so no colliding inodes.
- **ls / cat**: PS `handle_range` returns key-only entries, so both do a per-key `cluster.get` after the range scan (fine for one-shot CLI use). **Sizes**: files ≤4 KiB inline in the `InodeMeta`; larger go through the extent path (8 MiB chunks, `extent_key([0x03][ino BE][off BE])`).

### `autumn-stream-cli` (`src/bin/stream_cli.rs`)

Low-level stream-layer CLI for debugging; bypasses the partition layer entirely.

```
autumn-stream-cli --manager 127.0.0.1:9001 <COMMAND>
```

| Command | Description |
|---------|-------------|
| `register-node --addr <ADDR> --disk <UUID>` | Register an extent node |
| `create-stream [--data-shard N] [--parity-shard M]` | Create a new stream |
| `stream-info [--stream-id N]` | Show stream/extent metadata (omit for all) |
| `append --stream-id <ID> --data <STR>` | Append string data to a stream |
| `read --stream-id <ID> [--length N]` | Read from a stream |
| `alloc-extent --node <ADDR> --extent-id N` | Pre-create an extent on a node |
| `commit-length --node <ADDR> --extent-id N [--revision N]` | Query current write position |

### `repair-metastream` (`src/bin/repair_metastream.rs`)

One-off repair CLI for partition checkpoint corruption. Offline / preserved-data repair only; normal PS recovery does not rely on it.

```
repair-metastream --manager 127.0.0.1:9001 --meta-stream <ID> \
  --vp-extent <ID> --vp-offset <OFF> --sst <extent:offset:len> [--sst ...]
```

Connects as a normal `StreamClient` owner, reads and prints the current last `TableLocations` record from the target `meta_stream`, prints the replacement, then appends the new checkpoint. `--dry-run` prints current + target without writing.

## Startup Ordering

For a fresh cluster:
1. Start `autumn-manager-server` first — it CAS-imprints the cluster_id on first leader-promotion.
2. For each EN, run `autumn-op format <DIR>...` BEFORE launching the EN. `format` is IDENTITY-ONLY: it fetches the cluster_id, allocates disk_uuid(s), mints (or reuses) a stable `node_uuid`, registers an EMPTY location, and stamps the sentinel files.
3. Launch `autumn-extent-node` for each formatted EN with its own REQUIRED `--advertise`. It refuses to start without the sentinels, cross-checks its cluster_id against the manager's, then self-registers its live address + shard ports.
4. Run `autumn-op bootstrap` to create streams and the initial partition.
5. Start `autumn-ps` with a unique `--psid`.

Newly-registered nodes start `Suspend`; the manager's 2 s `node_health_loop` flips a node to `Online` on its first successful `df`. `select_nodes` gates allocation on `Online` but falls back to the full node set when none are Online (cold-leader / fresh-bootstrap path).

## Common CLI Patterns

```bash
# Start a minimal 1-node cluster (no replication, testing only)
autumn-manager-server --port 9001 &
# format is identity-only; the EN self-registers its location
autumn-op --manager 127.0.0.1:9001 format /tmp/extent0
autumn-extent-node --data /tmp/extent0 --port 9101 --manager 127.0.0.1:9001 \
    --advertise 127.0.0.1:9101 &
autumn-op --manager 127.0.0.1:9001 bootstrap --replication 1+0
autumn-ps --psid 1 --port 9201 --manager 127.0.0.1:9001 --data /tmp/ps1 &

# Write and read (data plane = autumn-client)
echo "hello world" > /tmp/val.txt
autumn-client --manager 127.0.0.1:9001 put mykey /tmp/val.txt
autumn-client --manager 127.0.0.1:9001 get mykey

# Inspect cluster (op plane = autumn-op)
autumn-op --manager 127.0.0.1:9001 info
```
