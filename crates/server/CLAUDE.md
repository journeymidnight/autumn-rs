# autumn-server Crate Guide

## Fixed-key core path benchmark

`cargo bench -p autumn-server --features ucx --bench core_path -- <manager>
<tcp|ucx> <bytes> <seconds> <depth> <load|write|read|direct>` uses 256 fixed keys
under `bench/core-path`, warms each measured operation for two seconds, and fails
on RPC errors or missing/short values. Load once before a comparison. Run both
versions against the same data and layout: `perf-check` derives its read set from
the preceding write phase, so its read results can change with write throughput.
The benchmark is single-threaded and reports sampled operation latency (1/16).

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
| Read / observability | `list-nodes`, `df`, `cluster-version` (prints the cluster's wire version AND the CLIENT WINDOW — `oldest client served`, annotated open or shut; an open window is what says an embedded client image does not have to be rebuilt at this commit), `extent-health [--node N] [--all]`, `list-ec-markers`, `recovery-stats`, `audit-log [--op N --node N --since/--until --limit L]`, `info [--part PID] [--detail]`, `policy-candidates` |
| Node lifecycle | `fence-node <id> --reason ... --by ... [--force]`, `maintenance <id> --reason ... --by ... [--expire TS]`, `unfence <id> --by ...`, `remove <id> --by ...` |
| Cluster / partition admin | `bootstrap [--replication 3+0] [--log-ec K+M] [--row-ec K+M] [--presplit 1:normal\|N:hex]`, `set-stream-ec --stream <ID> --ec K+M`, `force-ec-convert --extent <EXTID>`, `split <PARTID>`, `presplit <ns> <tenant> <rule>`, `merge <SURVIVOR> <VICTIM> [--force]`, `rebalance`, `compact <PARTID>`, `gc [--ratio R --max-size B --stream-debt B --empty-only] <PARTID>`, `forcegc <PARTID> <EXTID>...`, `format <DIR>...`, `upgrade-version --to <V>` |
| Auth / tenancy | `gen-signing-key [--kid K]`, `principal-create --principal P --grant P... [--admin-token]`, `principal-delete --principal P`, `principal-list`, `mint-token --principal P --credential ...`, `namespace-create --name N [--tenant T] [--presplit hex,…] [--admin-token]`, `namespace-delete --name N`, `namespace-list` |
| Auto-policy controller | `auto-policy status`, `auto-policy activate <NAME> [--arm]` (`--arm` = Armed, else DryRun), `auto-policy deactivate`, `auto-policy upsert <NAME> --switches split,gc,… [--interval N --cooldown N --max N --desc "…"]` (create/replace a custom policy), `auto-policy delete <NAME>`. Leader-routed |
| Async op-ledger | `ops status <OP_ID>` (one op, `unknown` if this leader doesn't know it), `ops list [--active] [--kind split\|merge\|rebalance\|compact\|gc\|forcegc\|ec\|recovery] [--limit N]`. The seven op triggers above submit here + print an `op_id`; global `--wait` blocks to terminal. **`recovery` is auto-dispatched** (never submitted — submit refuses it): it appears on its own and, while still `running`, carries the last failure as `ERROR[code]: reason` — including the executing node's own reason, which arrives on the `df` heartbeat rather than waiting for the next re-dispatch. Leader-routed |

**Extent sharing (`info`).** Both extent views name the partitions holding a
reference (`shared by parts [13, 19]`, JSON `shared_by_parts`), not just how
many there are — `extent_sharers()` maps extent → holders by walking the
regions in partition-id order, so each list is ascending BY CONSTRUCTION (no
second sort step to forget) and does not shuffle between runs. A CoW split is
what creates sharing, and the file is unlinked only at `refs == 0`, so the
identities are the actionable half: one holder releasing returns no space until
the others do. For a LOG extent it also explains a number operators misread —
each holder counts the extent's dead bytes in its OWN `gc_debt`, so summed
per-partition debt over-states physical bytes. That over-count is deliberate
(de-duplicating it strands the extent below `refs == 0` forever); the fix is
what the view SAYS. `df` is unaffected — it walks extents, not partitions.

COST, because the guard is easy to misread as free: the unscoped view gets the
mapping from data it already fetched. `run_partition_info` does not have other
partitions' stream membership, so it spends a second `MSG_STREAM_INFO` naming
all `3N` streams — whose response clones every membered extent in the cluster,
the full-cluster pull that view otherwise avoids. It is guarded on some extent
in THIS partition reporting `refs > 1`, and it is one pull per drawer open
rather than per refresh, but `refs > 1` is common on a split-grown cluster, so
budget for it rather than assuming the guard rarely fires. Cheap would require
a manager-side reverse lookup, i.e. a wire change. A failed lookup degrades to
`refs` alone rather than failing the panel.

`format` is IDENTITY-ONLY: no location flags — it stamps the sentinels and registers an EMPTY location; the EN self-registers its real location. `register-node` is a migration stub that hints and exits 1 before connecting.

**CLI conventions (canonical + accepted aliases).** Both binaries hand-parse args (no clap; `autumn_op/args.rs`, `autumn_client/args.rs`). Canonical subcommands are kebab-case; the old snake_case / no-separator spellings stay as accepted aliases (`policy-candidates`←`policy_candidates`/`policy`, `auto-policy`←`auto_policy`, `put-stream`←`putstream`, `get-stream`←`getstream`). Canonical flag names per concept, with the older spelling kept as an alias: `--namespace` (←`--scope`, client KV scope), `--tenant` (←`--with-tenant`, `namespace-create`), `--principal` (←`--tenant`, `mint-token`). Byte-size flags accept an optional binary suffix (`4k`/`8m`/`1gib`) across both binaries (`gc --max-size`/`--stream-debt`, `perf-check`/`ycsb --size`). Three `autumn-client` subcommands are INTERNAL zero-copy verification paths, deliberately omitted from `usage()`: `put-bulk`, `direct-get`, `bulk-get` (they mirror `put`/`get` through the ZC codepaths). NOT YET unified (follow-ups): the verb-noun vs noun-verb split (`list-nodes`/`fence-node` vs `namespace-create`), the per-command `--admin-token` duplicating the global one, and `split`'s three targeting flags (`--at`/`--at-hex`/`--at-raw-hex`).

### `autumn-s3` (`src/bin/autumn_s3/`)

Unauthenticated S3 endpoint over the `fs/` tree, in its own process. It began
read-only, so inference engines with no loader plugin seam (SGLang, FreeToken)
could stream weights through their built-in `--load-format runai_streamer`; it
now also writes, so a stock S3 client (LanceDB's `object_store`) can create and
update data on autumn. Serves `ListObjectsV2`, `HeadBucket`, ranged/whole and
conditional `GetObject`/`HeadObject`, `PutObject` (`If-None-Match: *`,
`If-Match`), `CopyObject`, `DeleteObject` (`If-Match`), `DeleteObjects`, and
Create/UploadPart/Complete/AbortMultipartUpload. Everything else answers a
parseable S3 `NotImplemented` (UploadPartCopy, ListParts, ListMultipartUploads,
versioning, ACLs). Object metadata (`Content-Type`, `x-amz-meta-*`) is not
stored. Requests are served whatever their `Authorization` header says,
including none.

- **Writes are `autumn-fs`, not a second implementation** (`write.rs`). A PUT
  or Copy is a `publish::NewFile` published with one fenced compare-and-write
  on its dirent, so `If-None-Match: *` and `If-Match` are decided by the
  partition server; multipart is `autumn_fs::multipart` (Complete touches no
  part body). An object written here is a file to the mount and to
  `autumn.Fs`, and vice versa. The ETag is inode + content generation
  (`publish::etag`), so it changes on a same-size same-second rewrite and an
  `If-Match` names exactly one version; a tag this gateway did not issue names
  no version (412, or 404 when there is no object).
- **The worker's state lock is held only for metadata steps; bytes never
  move under it.** Begin, the inode put, publish and namespace lookups take
  it. A request body streams into its data object through the client alone
  (`NewFile::start_object` + `write_streamed`, then `flush_streamed` for the
  tail before the lock is taken; UploadPart needs the lock only for its
  begin). Deleting data is handed to the reclaimer thread (below): a DELETE,
  an overwrite, an Abort and a Complete's leftovers write their durable intent
  (a tombstone, the terminal upload state) under the lock and return. A
  cancelled request's own unpublished objects are deleted through the client.
  Measured with `--workers 1` on a local 3-EN cluster: a 1000 MiB DELETE
  answers in 4 ms and a 200-part Abort in 3 ms, and a small HEAD on the same
  worker stays under 3 ms meanwhile (idle 1 ms). With the deletes under the
  lock that HEAD waited out the whole delete, 30 ms and 58 ms, and it grows
  with the object. A body of declared length at most the inline
  threshold is written under the lock (no I/O: it is buffered into the
  inode).
- **Nothing unfinished outlives its request.** An unpublished file and an
  unfinished part sit in a guard whose `Drop` undoes them, which also covers a
  client disconnect dropping the request future mid-await. Without it the
  pending record would stay under the worker's session, which is alive, so no
  recovery would ever look at it.
- **A GET pins what it streams** (`objects::open_pinned`). It takes a STABLE
  lease before reading the metadata, so the headers and every byte are one
  version, and no other client can write it in place or reclaim its data
  until the body is done or the client goes away. A delete or overwrite still
  succeeds at once — only the data waits (verified: a 40 MiB GET survives a
  delete and a replace mid-body, 4/4; with the manager lease released right
  after the pin it broke at 17.5 MB). The pin lives in `held_leases`
  (`reader_refs` counts this worker's requests), which gives it three things:
  the shared heartbeat renews it; `remove_unreachable_inode` on the SAME worker
  defers to `unlinked_open` instead of reclaiming under it, which matters
  because a client's own EXCLUSIVE is never stopped by its own pin; and the
  release reclaims those deferred inodes. The last request's release waits
  2 s (`PIN_LINGER`) so the sequential ranged GETs of the streamer and of Lance
  scans share one manager acquire; a mount writer is refused EBUSY during that
  linger. A writer holding the file makes the GET a 503 `SlowDown`, which SDKs
  retry. A pin the heartbeat finds gone ends the body with an error rather than
  risk a changed file.
- **Conditional reads** follow RFC 9110 order (If-Match, else
  If-Unmodified-Since; If-None-Match, else If-Modified-Since), which is what S3
  does; `Last-Modified` is an IMF-fixdate (it was ISO 8601, which is not an
  HTTP date).
- **Bodies**: `aws-chunked` (current AWS SDKs' default, to carry a trailing
  checksum) is decoded without copying; chunk signatures and trailing
  checksums are not verified, like SigV4. `Content-MD5` is checked only when
  sent, so an ordinary PUT pays no single-core MD5 over its bytes.
- **Keys** map to paths: parent directories are created on write; a key
  ending in `/` is a directory (an empty PUT of it creates the directory);
  empty, `.` and `..` components are `InvalidArgument`. Deleting an object
  leaves its directories (there is no multi-key transaction between removing
  a directory and a concurrent create under it), so an emptied directory still
  lists as a common prefix.
- **The reclaimer thread** (`reclaim_worker`) has its own `FsState` and its
  own client identity, and does two things. It takes what the workers hand it
  over a channel (`FsState::reclaim_later`: files left unreachable, terminal
  uploads) and reclaims it at once. Every `--sweep-interval-secs` (default 30;
  0 turns off only the periodic sweeps) it takes over, fences and finishes or
  undoes dead publishing sessions, and reclaims terminal multipart uploads,
  unlinked files and segment garbage. A separate identity is the point: a
  worker's own GET pin would not stop its own EXCLUSIVE, but it does stop the
  reclaimer's. For the same reason a worker hands a file over only after
  releasing its own REPLACE on it, and a file deleted while this worker streams
  it is handed over when the pin is released (~2 s after the GET, measured).
  A hand-off lost to a crash is found again by the sweeps, because the worker
  made it durable first. All idempotent, safe in several gateways at once.
  Every worker also runs the lease heartbeat. Without it the publishing
  session lapses after 30 s, and a sweeper would then recover it and fence out
  its writes. Verified by `kill -9` of the gateway mid-PUT and mid-UploadPart
  with 29 data keys already written: after a restart the sweeper reclaimed
  every one of them and the pending records, and the upload stayed usable.
- **A Complete retried after it succeeded** (the SDK lost the reply) gets the
  same 200 and ETag, as from S3, for an hour after the upload finished. The
  answer holds even if the object has since been replaced or deleted
  (`multipart::complete` keeps it in `[0x04]mpc/`, see `crates/fs/CLAUDE.md`).
  The key is checked inside `complete`, so another key with the same upload id
  is `NoSuchUpload`.
- **Any gateway thread stopping ends the process** (`spawn_role`). A worker
  returns only on error, and the reclaimer returns only when every worker is
  gone. A dead reclaimer in a live process would go on deleting names while
  nothing reclaimed their bytes.
- **Known limits.** DeleteObjects deletes its keys one at a time (each is
  metadata only). What a hand-off cannot reclaim yet waits for a periodic
  sweep: a file another worker, gateway or mount still holds; an aborted or
  completed upload with a part still in flight; a file parked for a pin that
  the heartbeat found lost. With `--sweep-interval-secs 0` this gateway never
  reclaims those; only another gateway that runs sweeps does. A publish whose outcome is unknown is settled only when the worker's
  session is recovered, i.e. after a gateway restart; until then a retry of that
  Complete on the same worker is `ConditionalRequestConflict`.
- SDK check: `scripts/s3_write_check.py` (boto3; every API above, the error
  codes the SDK parses, a racing-creators round that must leave exactly one
  winner — ablated by ignoring `If-None-Match`, which made all eight win). See
  `docs/ops.md`.

- **Listing (`listing.rs`) walks in S3 key order and resumes at the token.**
  Keys are raw-byte ordered and a directory `d` owns every `d/...` key, while
  dirents are stored in NAME order; they disagree about names that extend `d`
  with a byte below `/` (`d.txt` sorts before `d/`), so the walk holds a
  directory back until the scan passes `d/`. A page starts from its
  continuation token and skips subtrees that sort before it. Inside a
  directory the scan starts at the token's own name, and the only earlier
  names that can still sort after the token — directories named by a proper
  prefix of it followed by a byte below `/` (`part` for `part-00500`) — are
  fetched by name in one batched lookup. Starting the scan at that stem
  instead made page k of a `part-NNNNN` directory scan about k pages. So a page
  costs about a page of entries plus one scan and one lookup per token path
  level. An earlier version walked the whole subtree on every page and then
  sorted, and capped the walk at 100k entries, first silently truncating and
  then failing with a retryable 500; neither remains. Objects and common
  prefixes count together toward `max-keys`; the token is the last key on the
  page. Scans resume from the last name SCANNED, so entries deleted between a
  scan and its value fetch cannot end a directory early. Names that are not
  UTF-8 are left out: they cannot be S3 keys, and a lossy token would stop
  comparing with its own name and repeat a page forever. Stats for a page are
  one batched get of the listed inodes.

```
autumn-s3 --manager <host:port> [--listen 0.0.0.0] [--port 9000] [--workers N]
          [--host <daemon-identity>] [--credential-file <path>]
          [--direct-read true|false] [--sweep-interval-secs 30]
```

- Reads go through `autumn-fs` — the same crate the fuse mount and the PyO3
  `autumn.Fs` binding use — so lane striping, EN-direct reads and authz apply
  unchanged. An adapter over the partition layer, not a second data plane.
- `--workers` (default `min(cores, 8)`) accept threads, each with its own compio
  runtime, its own `FsState` and an SO_REUSEPORT listener on the same port. One
  thread caps an AWS-CRT client at ~40% of the read path; the knee is at 4.
- `--host` names the daemon identity each worker registers under (the entrypoint
  passes `s3-$HOSTNAME`); workers append their index and the sweeper `-sweep`.
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

- **Built on an `FsState`** since v4: `ensure_root` is the core's, so the CLI
  verifies the schema stamp (its own copy never did, and a v3 CLI misreads v4
  inodes), and `rm` is the core's `unlink`/`rmdir` (tombstoned removal, segment
  reclaim, deferral while another client holds the file) instead of a third copy
  that deleted extents before the name. `cat`/`get` of a segmented file plan
  from its map and fail on a missing data extent.
- **Namespace-first binding**: connects via `ClusterClient::connect(mgr, "fs")`, so the binding prepends `fs/` to every relative fuse key (and strips it off range results) — the same single global keyspace a fuse mount uses, so writes here are visible to a mount. No `--tenant`; this CLI sees the whole `fs/` namespace.
- **Authz**: `--credential-file` (`<principal>\n<hex>`, from `autumn-op principal-create`) is REQUIRED when the cluster protects `fs/` (connects via `connect_with_credential`, fails fast if the credential doesn't cover `fs/`); omit on an authz-off cluster.
- **Inodes** come from the MANAGER's global counter (`alloc_inodes`) — the same crash-safe source the fuse mount and PyO3 `autumn.Fs` use, so no colliding inodes.
- **ls / cat**: PS `handle_range` returns key-only entries, so both do a per-key `cluster.get` after the range scan (fine for one-shot CLI use). **Sizes**: files ≤4 KiB inline in the `InodeMeta`; larger go through the extent path (8 MiB chunks, `extent_key([0x03][ino BE][off BE])`).

### `migratev3_v4` — the fs schema v3 → v4 converter (run once, then delete)

Rewrites every `[0x01][ino]` of the `fs/` tree from the v3 `InodeMeta` (vendored
in the tool) to v4 (`generation = 1`, `segments = None`); dirents, extents and
inode numbers are untouched, and the `[0x04]schema_version` stamp moves to 4
LAST. Resumable: the last converted key is kept in `[0x04]migrate_v4_cursor`,
so an interrupted run continues instead of decoding converted values as v3.
`--dry-run` decodes everything and writes nothing; `--unstamped-is-v3` accepts
a populated tree with no stamp (built by autumnfs or the S3 gateway, which did
not stamp before v4). Stop every fs client first — an old binary does not check
the stamp and misreads v4 inodes. Verified on a local cluster: 6715 inodes,
listings and file bytes identical before and after, rerun a no-op. Runbook in
`docs/ops.md`.

### `migratev0_v1` — RAN AND DELETED (2026-09-20)

The one-shot converter that wrapped the manager's persisted etcd records in
their `persist` envelope. It was run against the single production cluster on
2026-09-20 (455 records across the nine split prefixes) and then deleted with
the `autumn-etcd` dependency it had added here, which is the whole point of a
converter: it leaves no residue. `git show fb47730e:crates/server/src/bin/migratev0_v1.rs`
has it if a later migration wants the shape.

Two things from it are worth carrying forward, because the next converter will
need them and will NOT get the first one for free:

- **Naming is `migratev<from>_v<to>` over the PERSIST format generation**, not
  the wire version. A persist change need not move `WIRE_VERSION` and usually
  should not.
- **That one decoded nothing.** Splitting a record out of the wire schema is a
  rename, and rkyv's layout does not depend on the type name (measured:
  `MgrExtentInfo` and an identically shaped `ExtentRecord` both encode to the
  same 152 bytes), so it was a pure prefix insertion. A later conversion that
  changes a record's FIELDS has to vendor the old definition itself, because by
  then the tree only has the new one.

The rule it served stands: a persisted-format change is delivered by a
converter, never by compatibility code in the servers (`crates/manager/CLAUDE.md`,
"Upgrade safety").

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

## Runtime upgrade and experiments

Build with Rust >=1.95 and compio 0.19.2; the Docker builder matches this minimum.
autumn-ps --tcp-zerocopy-min-bytes N enables replica append TCP sends at that
complete-frame size. Every star-replicated append is prepared, so N is the only
size cut. Default 0 leaves ordinary sends active. UCX is unchanged.
Measure on the deployment kernel/link before choosing a threshold. The optional
AUTUMN_PERF_PIDS JSON file maps process names to PIDs for core_path: CPU snapshots
are taken after warmup and after draining timed requests, around the same byte
window. These are process counters, not independent kernel-worker accounting.


## Controlled runtime performance validation

The controlled_path bench runs a fixed operation count on 64 keys per partition,
with 1 or 4 runtime threads pinned to CPUs 40 onward. Prefixes match the uniform
hex presplit grid under bench/controlled. Load/warmup verifies bytes before timing.
READY/go and DONE/go barriers let an external controller start/stop counters while
workers retain their runtime and buffers. PR_GET_DUMPABLE markers carry only trace
metadata and make no process-state changes. Panics fail the trial; the external
controller enforces a deadline and retains failure diagnostics.

perf/controlled_validation/run.py recreates RF3 on three NVMe directories for each
trial. Counter snapshots for benchmark/server roles bracket the request window;
the slower all-process snapshot is background audit only. Host perf uses disabled
counters with acknowledged enable/disable. BPF separately counts kernel threads,
softirqs, per-ring SQE/CQE, allocations and selected TCP copy call sites. These
metrics overlap: never add softirq time to process time or double-count io-wq
threads. Diagnose throughput with untraced runs; trace repetitions quantify probe
overhead. matrix.py rotates version order across repetitions and stops on failure.
