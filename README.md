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

**Key concept — 3 streams per partition:** The 3 streams are created by `autumn-op bootstrap`,
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
./cluster.sh start 3             # 3-replica cluster (EC 2+1 for log/row, meta 3+0)
./cluster.sh start 4             # 4-replica cluster (EC 3+1 for log/row, meta 3+0)

./cluster.sh stop                # kill all processes
./cluster.sh clean               # stop + wipe /tmp/autumn-rs data dirs
./cluster.sh restart             # clean + start (fresh cluster)
./cluster.sh restart 3           # fresh 3-replica cluster

./cluster.sh status              # show which processes are running
./cluster.sh logs                # tail all log files (Ctrl-C to exit)
```

**Auto-EC bootstrap** (FOPS-02): when `replicas ≥ 3`, `cluster.sh` automatically sets EC on log/row streams:

| replicas | log/row streams | meta stream |
|----------|----------------|-------------|
| 1, 2 | `N+0` pure replication | `N+0` |
| 3 | EC `2+1` (replicates=3) | `3+0` |
| ≥4 | EC `3+1` (replicates=4) | `3+0` |

Override with env vars (before `cluster.sh start N`):
```bash
AUTUMN_EC_LOG=off AUTUMN_EC_ROW=off ./cluster.sh start 4   # force all-replication
AUTUMN_EC_ROW=5+2 ./cluster.sh start 7                     # custom row EC (needs K+M ≤ N)
```

### Per-process control (recovery testing)

After at least one `cluster.sh start` (which snapshots the launch params to
`$DATA_ROOT/cluster_config`), individual extent-nodes and the partition
server can be killed and restarted without touching the rest of the cluster
— useful for exercising manager recovery dispatch loops, PS region
failover, etc.

```bash
./cluster.sh stop-node 2         # kill extent-node #2 (replicas are 1-indexed)
./cluster.sh status              # node2 will show NOT STARTED; rest still running
./cluster.sh start-node 2        # relaunch — re-registers with manager (idempotent on same addr)
./cluster.sh restart-node 2      # = stop-node 2 + start-node 2

./cluster.sh stop-ps             # kill the partition server
./cluster.sh start-ps            # relaunch the partition server
./cluster.sh restart-ps          # = stop-ps + start-ps
```

`start-node N` refuses to launch if node N is already running, and refuses
indices outside the snapshot's `REPLICAS`. To extend the cluster size,
re-run `cluster.sh start <new-N>`.

### F120 — graceful shutdown + write back-pressure (2026-04-27)

`cluster.sh stop` now sends SIGTERM and waits up to **60 s** for autumn-ps
to drain its in-memory state to row_stream before falling back to SIGKILL.
On a clean shutdown the partition server:

1. Stops accepting new client requests on every per-partition listener.
2. Drains all in-flight Phase-2 batches.
3. Rotates `active` memtable → imm.
4. Calls `flush_one_imm` repeatedly until imm is empty (each flush ships
   an SST to row_stream + writes a `TableLocations` checkpoint to
   meta_stream).
5. Replies on the per-partition oneshot, threads exit, process returns 0.

**Result:** on the next `cluster.sh start`, `open_partition` finds an
up-to-date `vp_offset` checkpoint and the logStream replay window is
empty (or close to it). Pre-F120, `cluster.sh stop` killed the process
after 5 s and any imm queued behind a slow P-bulk got replayed —
witnessed at 1.96 GB on partition 15 of a 4-disk EC cluster.

Tunables:

| env var | default | range | role |
|---------|---------|-------|------|
| `AUTUMN_PS_MAX_IMM_DEPTH` | `4` | `[1, 64]` | imm queue cap; partition_loop stalls req intake when reached (RocksDB analogue: `max_write_buffer_number`) |
| `AUTUMN_PS_MAX_WAL_GAP` | `2 GiB` | `[128 MiB, 64 GiB]` | force-rotate active when `active.bytes + Σ imm.bytes` exceeds this (RocksDB analogue: `max_total_wal_size`) |
| `AUTUMN_PS_SHUTDOWN_TIMEOUT_MS` | `60_000` | `[1_000, 600_000]` | per-partition drain deadline; SIGKILL fallback after this |

Manual verification (live cluster):

```bash
# 4-replica EC cluster.
bash cluster.sh reset 4

# Drive ~30 s of writes (any wbench / app load).
target/release/autumn-client --manager 127.0.0.1:9001 wbench --threads 8 --duration 30

# Graceful stop. SIGTERM is sent first; cluster.sh waits up to 60 s.
time bash cluster.sh stop      # should return in well under 60 s

# Restart and inspect /tmp/autumn-rs-logs/ps.log "open_partition: ready"
# lines: `vp_offset` should be ≈ logStream `commit_length end=` for each
# partition (no tail to replay).
bash cluster.sh start 4
grep -E 'logStream commit_length OK|open_partition: ready' /tmp/autumn-rs-logs/ps.log
```

### F121 — node-failure write recovery (2026-04-28)

When you `cluster.sh stop-node N` while a partition's open extents
include node `N`, the next write seals the current extent and
allocates a new 3-replica extent on the surviving nodes — within
~one append-fanout-timeout window (default **5 s**). Pre-F121 the
write blocked indefinitely because the PS-side stream `ConnPool` kept
returning a dead `Rc<RpcClient>` whose `read_loop` had exited; new
submits inserted into `pending` with no reader to dispatch them.

```bash
bash cluster.sh reset 4
echo hello > /tmp/v.txt
target/release/autumn-client --manager 127.0.0.1:9001 put k1 /tmp/v.txt   # ok
bash cluster.sh stop-node 1
target/release/autumn-client --manager 127.0.0.1:9001 put k2 /tmp/v.txt   # ok in <6 s
target/release/autumn-client --manager 127.0.0.1:9001 info                # node 1 disk online=false; new log_stream extent on the live nodes
```

Tunables:

| env var | default | range | role |
|---------|---------|-------|------|
| `AUTUMN_STREAM_APPEND_TIMEOUT_MS` | `5000` | `[200, 60_000]` | per-replica deadline inside `launch_append`'s 3-replica fanout. `Elapsed` becomes a soft error so the existing retry loop in `append_payload_segments` escalates to `alloc_new_extent`. |

Operator notes:
- The manager's `disk_status_update_loop` runs every 10 s — `info` may
  briefly show `online=true` for a node you just stopped; the next
  sweep flips it. A recovered node flips back automatically on the
  following sweep.
- `select_nodes` prefers nodes with at least one online disk; when too
  few healthy candidates appear (e.g. a cold leader before the first
  df sweep), it falls back to the full set and the per-RPC fall-back
  inside `handle_stream_alloc_extent` walks alternates on failure.
  Since F144 both the primary pick and the fall-back walk are
  shuffled — see below.

### F144 — uniform allocator across all extent-nodes (2026-05-05)

Pre-F144 the manager picked the lowest-`node_id` `count` nodes for every
new extent. On a 4-node cluster (`node_ids 1, 3, 5, 7`) every extent
landed on `[1, 3, 5]`; node 7 only ever appeared when one of the first
three failed the F121 online-disk check. Post-F144 the pick is a uniform
random `count`-subset, so all four nodes share replica + EC-parity load.

```bash
bash cluster.sh reset 4
AC="./target/debug/autumn-client --manager 127.0.0.1:9001"   # data plane
AO="./target/debug/autumn-op     --manager 127.0.0.1:9001"   # op plane (F213)
for i in $(seq 1 20); do echo data$i | $AC put key$i /dev/stdin; done
$AO info | grep -E 'extent .*replicas=|extent .*data='
# Expected: across ~20 extents node 7 should appear in roughly 75% of
# the replica/data sets (same as nodes 1, 3, 5). Pre-F144 node 7 would
# show up in 0 of them.
```

The same shuffle covers EC-parity allocation (parity slots no longer
land exclusively on whichever node `HashMap` iteration visits last) and
the per-RPC fall-back path inside `handle_stream_alloc_extent`.

### F140 — split vs concurrent compact/GC race (2026-05-05)

`handle_split_part` read `commit_length(row_stream_id)` and sealed via
`multi_modify_split` while two background tasks could have appends in flight:
(A) compaction's `RowAppendReq` on P-bulk writing to row_stream; (B) GC's
`run_gc` writing live VP records to log_stream. Whichever replicas had not
yet received the manager's eversion-bump push would accept the in-flight
append past the sealed point → replica file-size divergence → `MetaBlock CRC
mismatch` on restart.

Fix: dual-gate acquisition. `handle_split_part` acquires `compact_gate`
(PS-wide, same gate held by `background_compact_loop` for the duration of
`do_compact`) followed by `gc_gate` (per-partition, new in F140, acquired by
`background_gc_loop` around the `for eid in holes` block). Both gates are
held through `multi_modify_split` so commit_length is read with P-bulk idle
and GC idle.

Manual repro (verifying the gates prevent divergence):

```bash
bash cluster.sh reset 4
AC="./target/debug/autumn-client --manager 127.0.0.1:9001"   # data plane
AO="./target/debug/autumn-op     --manager 127.0.0.1:9001"   # op plane (F213)
# fill data so compaction and GC have work to do
bash cluster.sh wbench 2G
PARTID=$($AC ls | awk 'NR==1{print $1}')
# kick off concurrent compact + GC, then split
$AO compact "$PARTID" &
$AO gc "$PARTID" &
$AO split "$PARTID"
bash cluster.sh restart-ps
# Post-restart: no MetaBlock CRC mismatch in the ps log
grep -i "crc mismatch\|meta_len" /tmp/autumn-rs-logs/ps.log
# Verify replica sizes converge for the row_stream tail extent
SID=$($AO info --json "$PARTID" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['row_stream_id'])")
for d in /tmp/autumn-rs/d{1..4}/$SID; do stat -c '%s %n' "$d"/extent-*.dat 2>/dev/null; done
# All four replicas should report the same size for the sealed tail extent
```

---

### F146 — Three manager-side data-corruption races (2026-05-06)

Three lost-update races closed in one pass:

**HIGH-1**: `handle_stream_alloc_extent` snapshotted the tail extent then
awaited `commit_length_on_node` / `alloc_extent_on_node` / etcd mirror
before writing back. Concurrent `recovery_done` or `ec_conversion_done`
running during those awaits had their eversion bump and replica rewrite
silently overwritten. Fix: refuse-at-start (check `ec_conversion_inflight`
and `recovery_tasks` before first await) + verify-at-apply (re-check
`s.extents[tail].eversion` before the writeback; refuse if it changed).

**HIGH-2**: `handle_multi_modify_split` Phase-1 had an F138 EC guard but
no `recovery_tasks` check. `apply_recovery_done` running during Phase-2's
etcd await would have its replica slot replacement overwritten by Phase-3's
`apply_split_mutations`. Fix: symmetric `recovery_tasks` refuse-at-start +
verify-at-apply checking `pre_bump_eversion` snapshot.

**HIGH-3**: `build_append_future` in `extent_node.rs` did not re-check
seal state after the `truncate_to_commit_ref` await. A concurrent
`apply_extent_meta_durable` (from `handle_re_avali` or another append's
pre-truncate confirm path) could seal the extent during the truncate I/O;
the pwritev then landed bytes past the new `sealed_length`. Fix: re-check
`sealed_length / avali` atomics before computing offsets.

```bash
bash cluster.sh reset 4
AC="./target/debug/autumn-client --manager 127.0.0.1:9001"   # data plane
AO="./target/debug/autumn-op     --manager 127.0.0.1:9001"   # op plane (F213)
bash cluster.sh wbench 4G
# Trigger concurrent alloc_extent + EC conversion + recovery to exercise HIGH-1/2.
LOG_STREAM=$($AO info --json | python3 -c "import sys,json; p=json.load(sys.stdin)['partitions'][0]; print(p['log_stream_id'])")
$AC update-stream-ec "$LOG_STREAM" 2 1 &
bash cluster.sh stop-node 2   # triggers recovery on replicas hosted by node 2
bash cluster.sh start-node 2  # recovery starts; manager.log will show
                               # "defer alloc_extent until recovery completes"
                               # or "eversion changed during alloc_extent" retries.
# For HIGH-3: concurrent re_avali + in-flight append (needs two connections
# racing on the same extent):
bash cluster.sh stop-node 3 && bash cluster.sh start-node 3
# Post-F146: all three paths retry safely with Precondition/CODE_PRECONDITION
# until the competing operation completes.
$AO info | grep -E 'extent.*eversion|extent.*replicates'
```

---

### F147 — Three snapshot-await-writeback races (2026-05-06)

Three data-corruption races with the same snapshot-capture-then-await-then-apply
shape, missed by F146:

**F147-A**: `handle_sync_partition_vp_refs` applied a VP-ref diff after
awaiting the etcd mirror, without verifying that the touched extents had not
been mutated (eversion bumped, replica rewritten) during the await. On
leader-failover, replaying the etcd entry would produce a `vp_table_refs`
count inconsistent with in-memory state. Fix: refuse-at-start (check
`ec_conversion_inflight` + `recovery_tasks` for all extents in the new
snapshot; return `Precondition` before any await) + verify-at-apply (re-read
each touched extent's `eversion` under a fresh `borrow_mut` after the etcd
write; return `Precondition` if any eversion changed).

**F147-B**: `handle_append` (the non-batched code path, line ~2437) lacked
the post-truncate seal recheck that F146 added to `build_append_future` (the
batched path). A concurrent `apply_extent_meta_durable` sealing the extent
during the `truncate_to_commit` await would allow the subsequent `pwritev` to
land bytes past the new `sealed_length`, producing "logStream value short" or
out-of-bounds slice panics on EC reads. Fix: identical post-truncate recheck
in `handle_append` (check `sealed_length / avali` atomics before computing
pwritev offsets; return `CODE_PRECONDITION` if sealed).

**F147-C**: `run_recovery_task` performed no verification after fetching the
full extent from a peer and calling `sync_all`. A concurrent seal (from the
manager marking the extent sealed while recovery was in progress) could arrive
during the multi-second fetch I/O; without a check, recovery would write back
stale eversion/sealed_length metadata and log an incorrect `fetch_max` value.
Fix: after `sync_all`, re-read the local extent's `eversion` atomics; retry if
it advanced. Gate the `fetch_max` writeback on the fetched length matching the
manager-reported `sealed_length`.

```
# Scenario F147-A: concurrent flush + leader-failover with etcd-regressed eversion.
# Run wbench to accumulate VP deps, trigger a leader failover (kill + restart manager)
# exactly while a PS is calling sync_partition_vp_refs, and simultaneously trigger
# recovery on an affected extent. Pre-F147-A: the replayed vp_table_refs count on
# the new leader diverged from the PS's in-memory snapshot, risking premature
# extent deletion. Post-F147-A: the handler returns Precondition; PS retries on the
# next flush cycle with a fresh snapshot.

# Scenario F147-B: concurrent seal during truncate on the non-batched append path.
# Restart an extent-node mid-write so its replica is lagging. The next append
# triggers truncate_to_commit on the non-batched path (payload below the batch
# threshold). Simultaneously send re_avali to seal the extent. Pre-F147-B: pwritev
# landed bytes past sealed_length. Post-F147-B: CODE_PRECONDITION, client retries.

# Scenario F147-C: long recovery + concurrent seal.
# Stop a node hosting a large (>128 MB) sealed extent. Manager dispatches recovery.
# While the CopyExtent fetch is in-flight (seconds on loopback), the manager seals
# a second extent on the same node (eversion bump). Pre-F147-C: recovery wrote back
# stale metadata. Post-F147-C: recovery detects eversion advance and retries.
```

---

### F148 — Race-hunt audit after F147 (2026-05-06)

Continuation of the race-hunt that produced F146/F147. Three parallel layer-scoped
audits (manager / extent-node / partition-server) flagged 12 candidates;
verification against the actual code + crate CLAUDE.md context **confirmed zero
new HIGH-severity unfixed races** — every candidate is either already covered
by F119–F147, closed by F140's dual-gate, theoretical and not exercised by the
production call-graph, or precluded by single-threaded compio + the synchronous
path between `borrow_mut` drop and mpsc-send. F148 ships only:

**F148-A** — Inline-comment + regression test for the metadata-publish ordering
invariant. The conclusion that PS-side `flush_one_imm` and `do_compact`
concurrent metadata publishes cannot produce a stale meta_stream checkpoint
rests on three load-bearing properties: (P1) compio P-log runtime is
single-threaded, (P2) `borrow_mut` blocks contain no `.await`, (P3) the path
`borrow_mut` drop → `rkyv_encode` → `stream_client.append` → mpsc-send is
purely synchronous (first await is `ack_rx`, after FIFO mpsc). Together
(P1)–(P3) guarantee `borrow_mut` order = mpsc-send order = meta_stream record
order; the LATEST persisted record's `tables_snapshot` therefore necessarily
reflects all prior `borrow_mut` mutations. A future refactor that introduces
an `.await` between the `borrow_mut` drop and the mpsc send would silently
re-open a stale-snapshot race that could persist tables compaction has
already removed. Inline `// F148-A invariant` comments at all four call sites
(`flush_one_imm`, `flush_one_imm_local` in lib.rs; both branches of
`do_compact` in background.rs) state the rule next to the code.

**F148-B** — Defensive guard in `handle_copy_extent`. After the manager-fetch
+ `apply_extent_meta_durable` step, refuse with `CODE_PRECONDITION` when
`entry.sealed_length == 0`. Production callers (`run_recovery_task`,
`handle_re_avali`) only target sealed extents by design — the manager
dispatches both only after seal. Without the guard, a stray caller hitting
an unsealed extent could race a concurrent in-flight `handle_append`'s
`truncate_to_commit` await window and observe a mix of pre- and
post-truncate bytes via `file_pread_chunked`. On a sealed extent the append
protocol step 3 rejects concurrent appends, so the race only exists for
unsealed extents. Belt-and-braces.

```
# F148-A regression test: simulates two concurrent publishers (flush + compact)
# on a single compio runtime and asserts the LATER snapshot extends the EARLIER
# one — locks in the borrow_mut-order = mpsc-send-order invariant.
cargo test -p autumn-partition-server \
  f148_concurrent_publisher_ordering_invariant -- --nocapture

# F148-B guard tests: copy_extent must refuse with FailedPrecondition on
# unsealed extents, must succeed on sealed extents.
cargo test -p autumn-stream \
  copy_extent_unsealed_refused_with_precondition -- --nocapture
cargo test -p autumn-stream copy_extent_sealed_succeeds -- --nocapture
```

The full audit and per-candidate verdicts are in `claude-progress.txt` and
`feature_list.md`. Deferred: MED-2 (`handle_get → resolve_value` vs background
GC `punch_holes` on log_stream extent — needs a per-extent reader-pin
protocol, separate structural feature).

---

### F149 — Leader-fence on every manager etcd write (2026-05-06)

F005's lease-based leader election guarantees at most one manager **holds** the
leader-key at any time, but the deposed leader's in-process `self.leader` flag
can lag the etcd ground truth indefinitely under runtime starvation, GC pauses,
or syscall hangs. During that lag the deposed leader's mirror_* writes overwrite
the new leader's state with last-writer-wins, reverting freshly-applied recovery
slot replacements / EC conversion bumps / split snapshots. F149 closes the
window by making **every** manager → etcd write txn fenced on the value of the
leader-key:

```
compare prepended:
  Cmp::value("autumn-rs/stream-manager/leader") == self.instance_id
```

If the fence holds, the txn applies as before. If the fence fails (someone
else's `instance_id` is now in the leader-key), the helper flips the in-process
`leader` Cell to `false` and returns `AppError::NotLeader`, which bubbles up to
the client as `CODE_NOT_LEADER` so the client retries against whoever etcd
currently lists as leader. Routes covered: all 9 mirror_* helpers,
`persist_extent`, `acquire_owner_revision`, `dispatch_recovery_task`, and
`handle_multi_modify_split`'s consolidated Phase-2 txn. The only paths
intentionally NOT fenced are `try_become_leader` (the operation that
establishes ownership) and `replay_from_etcd` (read-only).

```
# Unit-level smoke (in-memory mode — fence is no-op since etcd is None).
cargo test -p autumn-manager --lib

# Integration test — requires Go toolchain for the embedded etcd helper.
# Spins up etcd, starts a manager, registers a node baseline, then externally
# overwrites the leader-key value to simulate a clean failover. Asserts the
# next mirror_register_node returns CODE_NOT_LEADER, that subsequent writes
# stay sticky NotLeader (proving the in-process leader Cell flipped without
# re-hitting etcd), and that only the pre-deposition state survived in etcd.
cargo test -p autumn-manager --test f149_leader_fence -- --ignored --nocapture
```

Live-cluster repro: run two `autumn-manager-server` instances against a 3-node
etcd cluster. Confirm M1 wins leadership (`etcdctl get
autumn-rs/stream-manager/leader` returns M1's instance_id). Pause M1 with
`SIGSTOP`, wait 12 s for M1's lease to expire, observe M2 winning election
(value becomes M2's instance_id). `SIGCONT` M1 — its `leader_keepalive_loop`
will eventually detect deposition, but ANY mirror_* call M1 issues in the gap
must immediately bubble up `CODE_NOT_LEADER` rather than overwriting M2's
state. Pre-F149: M1 would silently overwrite. Post-F149: M1's first attempted
write fence-fails, flips `leader=false`, surfaces NotLeader; client retries
hit M2.

---

### F145 — punch_holes/truncate vs in-flight EC conversion (2026-05-06)

F138 made `ec_conversion_inflight` an eversion-bump lock: while an extent is
mid-EC, no other mutator may bump its `eversion`. F138 covered
`apply_recovery_done`, `mark_extent_available`, and `handle_multi_modify_split`.
`handle_stream_punch_holes` and `handle_truncate` were missed: both would fall
into the "keep alive with eversion+1" else-branch for any ec-inflight extent,
producing a lost-update when `apply_ec_conversion_done` later overwrote
eversion with the pre-captured `new_eversion`.

Fix: two symmetric `Err(Precondition)` guards in `rpc_handlers.rs`, immediately
after the F139 recovery guards, refusing the entire RPC if any to-be-removed
extent is in `ec_conversion_inflight`. The PS GC retry loop already handles
`Precondition` from the F139 recovery path; the same retry covers this.

```bash
bash cluster.sh reset 4
AC="./target/debug/autumn-client --manager 127.0.0.1:9001"   # data plane
AO="./target/debug/autumn-op     --manager 127.0.0.1:9001"   # op plane (F213)
bash cluster.sh wbench 4G
# Trigger EC conversion on the log_stream while GC is running
LOG_STREAM=$($AO info --json | python3 -c "import sys,json; p=json.load(sys.stdin)['partitions'][0]; print(p['log_stream_id'])")
$AC update-stream-ec "$LOG_STREAM" 2 1 &
PARTID=$($AC ls | awk 'NR==1{print $1}')
$AO gc "$PARTID"
# Post-F145: gc retries with "in-flight EC conversion" until EC completes,
# then succeeds. Pre-F145: manager etcd had refs=0 records with eversion
# equal to the EC new_eversion; on leader-failover the replayed state was
# internally inconsistent (replicates referred to nodes that no longer
# owned the extent).
$AO info | grep -E 'extent.*refs=0|extent.*ec_converted'
```

---

### F139 — extent-node delete vs in-flight recovery (2026-05-05)

`handle_delete_extent` would race with `run_recovery_task` on the same
extent_id. Two failure modes: (a) recovery's `ensure_extent` auto-creates
a file after delete unlinked it → silent orphan with no manager record;
(b) recovery writes to an open fd whose path is already unlinked → data
evaporates on fd drop.

Fix: five symmetric guards using the same in-flight-set pattern as F138.
Manager: `dispatch_recovery_task` skips when the extent is in
`pending_extent_deletes`; `punch_holes` / `truncate` return Precondition
when a refs→0 extent is in `recovery_tasks`; `apply_recovery_done`'s
None-branch enqueues a targeted `PendingDelete` for immediate cleanup.
Extent-node: `handle_delete_extent` returns `CODE_PRECONDITION` when
`recovery_inflight.contains_key(&extent_id)`; `extent_delete_loop`
retries up to 60 × 2 s.

Manual repro (verifying the guard fires under concurrent GC + recovery):

```bash
bash cluster.sh reset 4          # 4-node cluster, EC enabled
AC="./target/debug/autumn-client --manager 127.0.0.1:9001"   # data plane
AO="./target/debug/autumn-op     --manager 127.0.0.1:9001"   # op plane (F213)
# Sustained writes to fill at least one extent rotation
for i in $(seq 1 200); do dd if=/dev/urandom bs=4k count=1 2>/dev/null | $AC put k$i /dev/stdin; done
bash cluster.sh stop-node 2      # triggers recovery dispatch on ext-node-2 slots
# Concurrently issue GC on a partition whose log_stream tail extent is recovering
PARTID=$($AC ls | awk 'NR==1{print $1}')
$AO gc "$PARTID" &
sleep 30
# Verify: no orphan .dat files and no "recovery_done apply" for a concurrently-deleted extent
grep -i "extent.*recovery.*precondition\|pending.*delete.*queued" /tmp/autumn-rs-logs/manager.log
# Expected: at most a few CODE_PRECONDITION retries, then clean convergence
$AO info --json | python3 -c "import sys,json; d=json.load(sys.stdin); print('orphans:', [e for e in d.get('extents',[]) if not e.get('in_stream',True)])"
```

---

### F138 — eversion-bump lock during EC conversion (2026-05-05)

`ec_conversion_dispatch_loop` captures `new_eversion = ex.eversion + 1`
before its long await, and `apply_ec_conversion_done` wrote that value
back unconditionally. A recovery completion or `mark_extent_available`
firing during the await would have its eversion bump silently overwritten.
Worse: `apply_ec_conversion_done`'s `ex.replicates = target_nodes[..]`
would revert the recovery's slot replacement.

Fix: `ec_conversion_inflight` now covers the full dispatch-to-apply
window. `apply_recovery_done`, `mark_extent_available`, and
`handle_multi_modify_split` check the lock and defer; they retry on the
next 2 s tick or client-side backoff. Symmetric to the existing F136
guard (EC checks `recovery_tasks` before dispatch).

Manual repro (verifying the deferral path is reachable):

```bash
bash cluster.sh reset 4
# enable EC on the first stream; write some data
AC="./target/debug/autumn-client --manager 127.0.0.1:9001"   # data plane
AO="./target/debug/autumn-op     --manager 127.0.0.1:9001"   # op plane (F213)
bash cluster.sh stop-node 1          # kill node 1 mid-cluster
for i in $(seq 1 20); do echo data$i | $AC put k$i /dev/stdin; done
# node 1 outage triggers recovery AND EC conversion on the same extents.
# Manager log shows: "ec conversion in flight on extent N; deferring recovery apply"
# After EC clears, recovery completes and eversion is bumped twice (EC + recovery).
$AO info | grep 'extent .* eversion'
```

After `start`, the script prints ready-to-use CLI examples:

```
AC="./target/debug/autumn-client --manager 127.0.0.1:9001"   # data plane
AO="./target/debug/autumn-op     --manager 127.0.0.1:9001"   # op plane (F213)
$AO info
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
AC=./target/debug/autumn-client   # data plane
AO=./target/debug/autumn-op       # op plane (F213)

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

# Step 3 — F214: format the data dir BEFORE launching the EN. Format
# fetches the manager's cluster_id, allocates a disk_uuid, calls
# MSG_REGISTER_NODE, and stamps cluster_id/disk_uuid/node_id/disk_id
# sentinel files in /tmp/d1. The EN refuses to start without these.
$AO format --listen :9101 --advertise 127.0.0.1:9101 /tmp/d1

# Step 4 — extent node: data plane (stores raw extent files on disk).
# Reads cluster_id + disk_id from /tmp/d1, cross-checks cluster_id
# against the manager before binding the listener.
$NODE --port 9101 --data /tmp/d1 --manager 127.0.0.1:9001 &
sleep 0.5

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
$AO bootstrap --replication 1+0
# Expected: "bootstrap succeeded: 1 partition(s)"

sleep 1   # wait for PS to pick up the new partition

# Step 7 — verify
$AO info
# Expected: 1 node, 3 streams, 1 partition

echo "hello autumn" | $AC put mykey /dev/stdin
$AC get mykey
# Expected: "hello autumn"
```

### What happens in bootstrap

`autumn-op bootstrap --replication 1+0` does:

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
AC=./target/debug/autumn-client   # data plane
AO=./target/debug/autumn-op       # op plane (F213)

pkill -f autumn-manager-server; pkill -f autumn-extent-node; pkill -f autumn-ps
rm -rf /tmp/autumn-etcd /tmp/d1 /tmp/d2 /tmp/d3 /tmp/autumn-ps

etcd --data-dir /tmp/autumn-etcd \
     --listen-client-urls http://127.0.0.1:2379 \
     --advertise-client-urls http://127.0.0.1:2379 &
sleep 0.5

$MANAGER --port 9001 --etcd 127.0.0.1:2379 &
sleep 0.5

# F214: format each dir BEFORE launching the EN. Each format call
# fetches cluster_id, allocates the disk_uuid, registers the node,
# and stamps the per-dir sentinel files.
$AO format --listen :9101 --advertise 127.0.0.1:9101 /tmp/d1
$AO format --listen :9102 --advertise 127.0.0.1:9102 /tmp/d2
$AO format --listen :9103 --advertise 127.0.0.1:9103 /tmp/d3

$NODE --port 9101 --data /tmp/d1 --manager 127.0.0.1:9001 &
$NODE --port 9102 --data /tmp/d2 --manager 127.0.0.1:9001 &
$NODE --port 9103 --data /tmp/d3 --manager 127.0.0.1:9001 &
sleep 0.5

$PS --psid 1 --port 9201 --manager 127.0.0.1:9001 \
    --data /tmp/autumn-ps --advertise 127.0.0.1:9201 &
sleep 1

$AO bootstrap --replication 3+0
sleep 1

$AO info
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
| `put <KEY> <FILE>` | Write key with value from file (≤ 64 MiB inline) |
| `streamput <KEY> <FILE>` | Single-frame StreamPut (legacy; prefer `putstream` for >64 MiB) |
| `putstream <KEY> <FILE> [--chunk-size N]` | F129 multipart upload — splits FILE into chunks (default 4 MiB), each one `MSG_PUT_CHUNK` to log_stream; final commit installs a multi-fragment ValuePointer. The only path for values > 64 MiB. |
| `getstream <KEY> [--chunk-size N] [--out FILE]` | F129 streaming read — pulls chunks via offset/length GetReqs; writes to FILE or stdout. Use for large values to avoid buffering the full payload in client memory. |
| `put-zc <KEY> <FILE>` | F216-E zero-copy write: reads `FILE` into a `Bytes`, registers it for UCX zero-copy send (`ucx` build), writes via `ClusterClient::put_zc` (`MSG_PUT_ZC`) — the value is sent as its own iovec straight from its (registered) backing memory with no client-side copy, and sliced zero-copy on the PS. Same routing/retry as `put`; interoperable with `get`/`zc-get`. |
| `get <KEY>` | Read value (writes raw bytes to stdout) |
| `zc-get <KEY>` | F216-E zero-copy read: heads the key, reads the value straight into a dest buffer via `ClusterClient::get_into` (`MSG_GET_ZC`); on a `ucx`-feature build the dest is `ucp_mem_map`-registered so the value lands by RDMA (memh) with no intermediate copy. Writes raw bytes to stdout — byte-identical to `get`; used to verify the client←PS zero-copy path. |
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
| `info [--json] [--top N \| --part PID]` | Show cluster state (nodes / streams / partitions). `--json` emits a machine-readable dump; `--top N` lists the N largest partitions by live size; `--part PID` shows detail for one partition including pending GC discards. |

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

# F214: format the dir BEFORE launching the EN. autumn-extent-node
# reads cluster_id + disk_id from sentinel files in /tmp/d1 and
# refuses to start if they're missing.
autumn-op --manager 127.0.0.1:9001 format \
          --listen :9101 --advertise 127.0.0.1:9101 /tmp/d1
autumn-extent-node --port 9101 --data /tmp/d1 --manager 127.0.0.1:9001

autumn-ps --psid 1 --port 9201 --manager 127.0.0.1:9001 \
          --data /tmp/ps-wal --advertise 127.0.0.1:9201
```

### F196 — static cpuset pre-allocation (ScyllaDB-style)

Both `autumn-extent-node` and `autumn-ps` accept `--cpuset <SPEC>` using
`taskset` syntax. When supplied, it overrides the auto-detected core
list, disables `--cpu-start`, and enables per-binary static budgets:

```
autumn-extent-node --cpuset 0-3 --data /tmp/d1 ...
autumn-ps          --cpuset 8-15 --psid 1 ...
```

- **EN** sets `shards = cpuset_len` (one shard per core). `--shards`
  was removed in F196 — the binary refuses to start if the flag is
  passed, with an error pointing at `--cpuset`. `cpuset_len == 1`
  warns about no parallelism.
- **PS** sets `max_partitions = cpuset_len / 2` (each partition reserves
  P-log + P-bulk). When the budget is exhausted, `split` returns
  `FailedPrecondition: PS core budget exhausted (N / M partitions)` and
  newly-assigned partitions from the manager are skipped at
  `sync_regions` time with a WARN so the operator can grow `--cpuset`
  or migrate the partition elsewhere.

For `cluster.sh`, point `AUTUMN_PS_CPUSET` / `AUTUMN_EN{i}_CPUSET` at
disjoint ranges:

```bash
AUTUMN_EN1_CPUSET=0-1 AUTUMN_EN2_CPUSET=2-3 AUTUMN_EN3_CPUSET=4-5 \
AUTUMN_PS_CPUSET=6-15 \
  ./cluster.sh up
```

The manager also emits a hot/cold imbalance advisory: a single WARN
line per PS, at most every 5 minutes, when a partition's `req_per_sec`
runs ≥ 10× another partition on the same PS for at least 5 consecutive
1-minute buckets and the hottest is above `SPLIT_QPS_HIGH/2`. The
advisory is informational — operators can use it to plan splits before
the core budget gates further growth, or to plan merges of cold pairs
to free a slot.

---

## Operations

### KV operations

```bash
AC=./target/debug/autumn-client   # data plane
AO=./target/debug/autumn-op       # op plane (F213)

echo "hello" > /tmp/v.txt
$AC put mykey /tmp/v.txt
$AC get mykey
$AC head mykey          # prints length
$AC ls --prefix my      # scan keys with prefix
$AC del mykey
```

### Large value (>4KB inline VP, >64MB client-side striperados — F186)

```bash
# 100 KiB — inline `Put` is fine; if value > 4 KiB the PS stores the
# bytes in log_stream and the memtable holds a ValuePointer.
dd if=/dev/urandom of=/tmp/big.bin bs=1024 count=100
$AC put bigkey /tmp/big.bin
$AC head bigkey         # expects: length: 102400

# 200 MiB — inline cap (AUTUMN_PS_MAX_INLINE_BYTES_DEFAULT = 64 MiB) is
# exceeded. F186 (replaces F129/F130): pure client-side striping. The
# SDK splits the file into 4 MiB chunks and writes each as a normal
# `Put` to a reserved-namespace key (\xff\xfeacv1\xff...), then writes
# a 29-byte StripeMeta blob to the user key as the atomic commit point.
# No server-side multipart RPCs, no multi-fragment VP, no GC active
# rewrite — just normal Puts under the hood.
dd if=/dev/urandom of=/tmp/huge.bin bs=1M count=200
$AC putstream hugekey /tmp/huge.bin
# `head hugekey` returns 29 (the meta blob length), not 209715200.
# Use getstream + diff to verify content.

# Read back via streaming so the daemon doesn't buffer 200 MiB in RAM.
$AC getstream hugekey --out /tmp/huge.copy
diff /tmp/huge.bin /tmp/huge.copy
```

### Partition operations

```bash
# Get partition IDs from info
$AO info

# Split a partition (server picks mid-key automatically)
$AO split <PARTID>

# Trigger major compaction (clears overlap after split, reclaims space)
$AO compact <PARTID>

# Repeated rightmost split + compact should preserve existing keys while
# clearing overlap on each descendant.
$AO split <PARTID>
$AO compact <RIGHT_CHILD_PARTID>
$AO split <RIGHT_CHILD_PARTID>

# Trigger auto GC (reclaims logStream extents with >40% discard)
$AO gc <PARTID>

# Force GC on specific extents
$AO forcegc <PARTID> <EXTID1> <EXTID2>
```

### Cluster info

```bash
# Full text report (nodes / extents / streams / partitions)
$AO info

# Machine-readable JSON dump (pipeable to jq)
$AO info --json | jq '.partitions | length'

# Top 3 partitions by live size — jq filter (F205 removed --top)
$AO info --json | jq '.partitions | sort_by(.live_size) | reverse | .[:3]'

# Detail for partition 0 (3 streams + pending GC discards)
$AO info --part 0

# Same but JSON
$AO info --json --part 0
```

Each partition's `log` stream line shows pending GC discard when non-zero:

```
  part 0: ps=127.0.0.1:9201, range=[..∞)
    log: stream 1, extents=[1, 2], size=128.0 MB, discard: 2 ext / 54.3 MB pending
    row: stream 2, extents=[3], size=64.0 MB
    meta: stream 3, extents=[4], size=4.0 KB
    total: 4 extents, 192.0 MB
```

`--top` and `--part` are mutually exclusive. `--json --top N` returns an array of the top-N partition objects. `--json --part PID` returns a single partition object.

#### F109: verifying extent files are physically reclaimed after GC

When `gc` succeeds, the manager removes the extent's metadata and
fans out a `MSG_DELETE_EXTENT` to every replica. The physical
`{disk}/{hash:02x}/extent-{id}.dat` + `.meta` files should be
unlinked within ~2 s (one sweep of `extent_delete_loop`).

```bash
# Pre-GC: capture the extent dir size on each extent-node
du -sh /tmp/autumn-rs/d1/  /tmp/autumn-rs/d2/  /tmp/autumn-rs/d3/

# Trigger GC for the partition you want to reclaim from
$AO gc <PARTID>

# Wait ~5s for the manager's extent_delete_loop sweep + per-replica unlink
sleep 5

# Post-GC: dir size should drop by the size of the punched extents
du -sh /tmp/autumn-rs/d1/  /tmp/autumn-rs/d2/  /tmp/autumn-rs/d3/
```

If a node was offline during the delete fanout, the orphan files
remain until the next `autumn-extent-node` startup, where
`reconcile_orphans_with_manager` queries the manager for unknown
extents and unlinks the corresponding files. To exercise the
reconcile path manually: stop a node before running `gc`, run the
GC, then restart that node and observe its data dir shrink as the
reconcile completes during boot.

#### F201 — multi-tier `client gc` flags (2026-05-15)

The `gc` command accepts optional filter flags so operators (and
external policy controllers) can target specific tiers of reclaimable
extents without rebuilding the PS:

```bash
# Default (matches pre-F201): discard_ratio > 0.4 AND F201 empty-extent
# pick — punches both garbage-heavy AND empty sealed slots.
$AO gc <PARTID>

# Only sealed-length=0 non-tail extents (cheapest possible — no rewrite).
# Use this when `info` shows empty `(open), 0 B` sealed slots.
$AO gc <PARTID> --empty-only

# Aggressive: 10% dead is enough.
$AO gc <PARTID> --ratio 0.1

# Mixed: relax ratio for small extents (any extent < 16 MiB that is at
# least 10% dead becomes eligible).
$AO gc <PARTID> --max-size 16MiB --ratio 0.1

# Stream-debt-aware: when the partition's total reclaimable bytes
# exceed 1 GiB, the PS halves the ratio internally for this dispatch.
$AO gc <PARTID> --stream-debt 1GiB --ratio 0.4
```

Byte-size flags (`--max-size`, `--stream-debt`) accept K/M/G/T(i)B
suffixes case-insensitively (e.g. `512K`, `16MiB`, `1G`, `2T`).

The F201 user-reported case (`(open), 0 B` sealed extents stuck in
`extent_ids` forever):

```bash
cluster.sh reset 4 && cluster.sh start
$AO bootstrap --replication 3+0 --log-ec 3+1 --row-ec 3+1
$AC perf-check --duration 60        # produces 0-byte sealed log_stream extents

$AO info                            # observe `(open), 0 B` on some sealed-position extents
$AO gc <PARTID> --empty-only        # cheapest cleanup path
$AO info                            # 0-byte sealed extents are gone
```

If you run `client gc` concurrently with an EC convert on the same
extent, the manager refuses the punch with `CODE_PRECONDITION`. F201's
cooldown classifier puts that extent on a 30 s soft cooldown (instead
of 5 min) so a manual retry, or the periodic GC tick, picks it up
again as soon as the EC convert completes.

#### F202 — advisory unified across 6 actionable kinds (2026-05-15)

`client policy` now prints up to 7 kinds in one query:

| kind | source | when it fires |
|---|---|---|
| `split` | `PolicyEngine::compute_candidates` | sustained size > 50 GiB, qps > 15k, or imm_full > 10/s |
| `merge` | same | adjacent cold pair (size < 1 GiB AND qps < 1.5k each) |
| `gc` | `compute_maintenance_advisory` | `gc_debt_bytes > 1 GiB` sustained |
| `major` | same | `pending_compaction_bytes > 4 GiB` sustained |
| `minor` | same (F202) | `minor_compact_pending_bytes > 512 MiB` sustained AND pickup-tables non-empty |
| `ec` | `compute_ec_advisory` (F202) | sealed-unconverted extent on EC-policy stream, `sealed_length ≥ 64 MiB` |
| `hotcold` | `compute_hot_cold_advisory` | ≥10× qps or size ratio between hottest/coldest partition on a PS |

Common-sense filters built into the advisory layer:
- EC: extents < 64 MiB are NOT surfaced (encode + 3-replica fanout
  overhead would exceed the 3 → K/(K+M) replication savings).
- Minor compact: requires `pickup_tables` to actually have work to do
  in the most recent bucket — avoids spamming "minor compact this
  partition" when there's nothing to compact.

An external policy controller can act on this in a one-liner. Example
cron + bash for an MVP controller:

```bash
# /etc/cron.d/autumn-policy: */5 * * * * /usr/local/bin/autumn-policy.sh
$AO policy --json | jq -c '.[]' | while read -r cand; do
  kind=$(echo "$cand" | jq -r .kind)
  pid=$(echo "$cand" | jq -r .primary_part_id)
  sec=$(echo "$cand" | jq -r .secondary_part_id)
  size=$(echo "$cand" | jq -r .size_bytes)
  case "$kind" in
    ec)    [ "$size" -ge 67108864 ] && $AO set-stream-ec --stream "<resolve from $sec>" --ec 3+1 ;;
    gc)    qps=$(curl -s prom/api/v1/query?query=fg_qps_partition\{p=\"$pid\"\} | jq -r .data.result[0].value[1])
           [ "$qps" -lt 1000 ] && $AO gc "$pid" ;;
    major) $AO compact "$pid" ;;
    # split / merge / minor / hotcold left as exercise
  esac
done
```

Note: per-extent EC convert (`set-stream-ec --extent <EXTID>`) is a
Stage 3 deliverable; today's controller resolves extent_id → stream_id
via `info --json` and triggers stream-level conversion, which the
existing manager dispatch loop will then apply to sealed-unconverted
extents on that stream.

#### F203 — External-policy controller surface (2026-05-15)

End of the mechanism/policy separation refactor. The manager no
longer auto-dispatches anything based on advisory candidates; it
just publishes `advisory_cache` via `MSG_GET_POLICY_CANDIDATES`. To
make use of those advisories, run an external policy controller (a
bash script, a cron job, a Python daemon, a custom binary — the
contract is the same).

**OP toolkit (every command idempotent, every command safe to retry):**

| Read | Action |
|---|---|
| `client policy [--json]` | inspect all advisory candidates |
| `client info [--json]` | full cluster state snapshot |
| `client info --part PID --detail [--json]` (**F203**) | latest F202 metrics for one partition |
| `client streams [--json]` (**F203**) | stream + extent map |
| `client gc PID [--ratio R | --max-size B | --empty-only | --stream-debt B]` | trigger GC (F201) |
| `client compact PID` | trigger major compact |
| `client forcegc PID EXTID...` | force GC specific extents |
| `client split PID` | trigger split |
| `client merge SURVIVOR VICTIM` | trigger merge |
| `client set-stream-ec --stream SID --ec K+M` | change stream EC policy |
| `client force-ec-convert --extent EXTID` (**F203**) | convert one extent now |

**Why no `--auto-*` flags remain on `autumn-manager-server`**: in
prior versions `--auto-split` and `--auto-merge` opt'd the manager
into auto-dispatching the corresponding candidates. F203 removed
both. Passing them now exits with a migration message pointing here.
Reasoning: policy is environment-dependent (cluster quiet hours,
business-tier preferences, SLO targets, capacity planning windows),
and embedding even simple "fire when threshold > X" decisions inside
the manager forces every operator to rebuild + redeploy when those
inputs change. The advisory output is plenty for a 30-line bash
controller to do what production needs.

**MVP controller example (cron + bash, every 5 minutes):**

```bash
# /etc/cron.d/autumn-policy: */5 * * * * /usr/local/bin/autumn-policy.sh
#!/usr/bin/env bash
set -euo pipefail
AC="autumn-client --manager 10.0.0.1:9001"
PROM="http://prometheus.internal/api/v1/query"

# Business hours? Skip all heavy ops between 09:00 and 18:00.
hour=$(date +%H)
if [ "$hour" -ge 9 ] && [ "$hour" -lt 18 ]; then
  exit 0
fi

# Cluster-wide foreground QPS gate. Defer ALL dispatch if user
# traffic is above 50K — leave the kernel's must-cleanup paths
# (expiry-major / has_overlap-major / minor-compact) to handle
# the steady-state.
fg_qps=$(curl -fsS "$PROM?query=sum(rate(fg_put_qps_total[1m]))" | jq -r '.data.result[0].value[1] // "0"')
if (( $(echo "$fg_qps > 50000" | bc -l) )); then
  exit 0
fi

$AO policy --json | jq -c '.[]' | while read -r cand; do
  kind=$(echo "$cand" | jq -r .kind)
  pid=$(echo "$cand"  | jq -r .primary_part_id)
  sec=$(echo "$cand"  | jq -r .secondary_part_id)
  size=$(echo "$cand" | jq -r .size_bytes)
  case "$kind" in
    ec)
      # F202 generator already filtered extents < 64 MiB.
      $AO force-ec-convert --extent "$sec"
      ;;
    gc)
      # Per-partition QPS gate — only GC partitions that aren't
      # actively serving heavy reads. Read F202 detail first.
      p_qps=$(curl -fsS "$PROM?query=fg_put_qps_total{p=\"$pid\"}" | jq -r '.data.result[0].value[1] // "0"')
      if (( $(echo "$p_qps < 1000" | bc -l) )); then
        $AO gc "$pid"
      fi
      ;;
    major)
      $AO compact "$pid"
      ;;
    minor)
      # No-op: minor compact is already kernel-driven (LSM hygiene).
      # We just log here for visibility.
      echo "$(date -u +%FT%TZ) minor advisory for part $pid (kernel handles)" >&2
      ;;
    split|merge|hotcold)
      # Range reassignment — page a human instead of auto-acting.
      echo "$(date -u +%FT%TZ) policy advisory: kind=$kind part=$pid secondary=$sec" \
        | mailx -s "autumn policy alert" oncall@example.com
      ;;
  esac
done
```

**Failure model**: if the controller crashes mid-loop, nothing is
left dangling — every action is idempotent at the manager (re-issuing
`split` against an already-split partition fails fast with
`PRECONDITION`; `force-ec-convert` against a pending or converted
extent returns OK; etc.). Worst case the next cron tick re-tries.
The cluster degrades to the kernel's must-cleanup behaviour
(expiry-major, has_overlap-major, size-tiered minor) which is the
intended floor.

**Want fancier? upgrade the controller, not the kernel**: ML-based
load prediction, predictive split based on hot-key trajectory,
SLO-aware compact windowing — all of it is pure addition outside the
binary. The kernel's job is to expose accurate signals and execute
RPCs reliably; deciding when to call them is a separate concern with
its own iteration cadence.

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
AC=./target/debug/autumn-client   # data plane
AO=./target/debug/autumn-op       # op plane (F213)
NODE=./target/debug/autumn-extent-node

# Format the disk and register the node with the manager. F214: writes
# cluster_id / disk_uuid / node_id / disk_id sentinel files in /tmp/d4.
$AO format --listen :9104 --advertise 127.0.0.1:9104 /tmp/d4

# Start the extent node. It reads the sentinel files on startup and
# cross-checks the stamped cluster_id against the manager (F214-D).
$NODE --port 9104 --data /tmp/d4 --manager 127.0.0.1:9001
```

### F206 — post-EC `avali` regression check (2026-05-15)

Quick sanity check that the post-EC `avali` bitmap covers all K+M
slots. Pre-F206 the bitmap kept its pre-EC value (`all_bits(K)`),
which caused the manager's `recovery_dispatch_loop` to fire
`EXT_MSG_RE_AVALI` on the parity holder every 2 s; each dispatch
allocated `sealed_length`-sized buffers on the extent-node. Visible
symptom: multi-GB RSS swings + `df RPC failed; marked node disks
offline` flap on an idle cluster after `cluster.sh restart`.

```bash
# 1) Find a fully EC-converted extent.
AC=./target/release/autumn-client
$AC --manager 127.0.0.1:9001 info --json \
    | jq '.streams[].extents[] | select(.ec_converted == true)'

# 2) Capture its replicates + parity counts.
EID=10
TOTAL=$($AC --manager 127.0.0.1:9001 info --json \
        | jq --argjson e $EID \
          '[.streams[].extents[] | select(.extent_id == $e)
             | (.replicates | length) + (.parity | length)] | first')
EXPECTED=$(( (1 << TOTAL) - 1 ))

# 3) etcd-side `avali` MUST equal (1 << (K+M)) - 1.
#    Pre-F206 it would have been (1 << K) - 1 (e.g. 0b0111 for K=3+M=1).
#    Decoding rkyv is non-trivial from the shell; the simplest check
#    is to watch the manager log for a steady-state cluster:
#       NO `df RPC failed; marked node disks offline` lines after
#       the first 30 s post-restart.
tail -F /tmp/autumn-rs-logs/manager.log | grep -E "df RPC failed"

# 4) Per-node RSS should stabilise under ~100 MB on an idle cluster.
pgrep -f autumn-extent-node | xargs -n1 ps -o pid,rss,command -p
```

A cluster running an EC-policy stream that has already been EC-converted
on a pre-F206 binary will **self-heal** within ~2 s of restarting both
manager and extent-nodes against an F206-or-later binary: the manager's
RE_AVALI to the parity holder now returns OK immediately (see Bug B
fix in `handle_re_avali`), which causes `mark_extent_available` to OR
in the missing slot bit and persist the corrected `avali` to etcd.

---

## Python bindings (asyncio)

The `python/` crate exposes an asyncio-native client. All RPCs run on a
dedicated compio worker thread; each Python method returns an
`asyncio.Future` that the worker resolves via `loop.call_soon_threadsafe`.

Build into a virtualenv:

```bash
python3 -m venv /tmp/autumn-py-venv
/tmp/autumn-py-venv/bin/pip install maturin
cd python && VIRTUAL_ENV=/tmp/autumn-py-venv /tmp/autumn-py-venv/bin/maturin develop --release
```

Use:

```python
import asyncio, autumn

async def main():
    client = await autumn.Client.connect("127.0.0.1:9001")
    await client.put(b"k", b"v")
    print(await client.get(b"k"))                        # b"v"
    rows = await client.range(b"k", b"", 100)            # keys only; values empty
    await asyncio.gather(*[client.put(f"x/{i}".encode(), b"v") for i in range(50)])
    n = await client.batch_delete(b"x/")
    await client.close()

asyncio.run(main())
```

API: `Client.connect(addr)`, `put(k, v)`, `get(k)`, `delete(k)`,
`range(prefix, start=b"", limit=100)`, `batch_delete(prefix)`, `close()`,
plus zero-copy `put_from(k, buf)` and `get_into(k, buf)` for buffer-protocol
arguments (numpy / torch tensor / memoryview). All methods are awaitable.
`range` returns a list of `(key, value)` tuples — the partition server fills
only the key slot, so call `get` for values you actually need.

## autumn-kvcache (sglang HiCache L3 backend)

`python/autumn_kvcache/` is a thin Python adapter that plugs autumn into
sglang as a [HiCache L3 storage backend][hicache] via the `dynamic` plugin
mechanism — no sglang source patch required. Architecture: pure Python
adapter over the `autumn` PyO3 client; **no sidecar daemon, no local DRAM
LRU**. partition layer's memtable + block cache serves as the implicit DRAM
tier (see `docs/autumn_kvcache_plan.md` for rationale).

[hicache]: docs/hicache_l3_interface.md

### Install

```bash
# Build + install the autumn PyO3 client (one-time, plus on Rust changes).
cd python
python3 -m venv /tmp/autumn-py-venv
/tmp/autumn-py-venv/bin/pip install maturin numpy
VIRTUAL_ENV=/tmp/autumn-py-venv /tmp/autumn-py-venv/bin/maturin develop --release

# Install the sglang adapter package (pure Python).
/tmp/autumn-py-venv/bin/pip install -e autumn_kvcache
```

### Run with sglang

```bash
sglang ... \
  --enable-hierarchical-cache \
  --hicache-storage-backend dynamic \
  --hicache-storage-backend-extra-config '{
    "backend_name":"autumn",
    "module_path":"autumn_kvcache.sglang_backend",
    "class_name":"AutumnKVCacheStorage",
    "interface_v1":1,
    "endpoint":"127.0.0.1:9001"
  }'
```

`interface_v1: 1` is **required** — it routes sglang's cache controller
through the zero-copy v1 path (`batch_get_v1` / `batch_set_v1`); without it
v0 batch methods are used and lose zero-copy.

`endpoint` is the autumn manager address (or comma-separated list for
multi-manager). The adapter holds one connection per process and uses the
same routing / failover as `autumn-client`.

### Tenant isolation

The adapter stores partition keys as
`f"kvc/{tenant_suffix}/kv/{sha256_hex}"`. `tenant_suffix` is built per
sglang's `HiCacheFile._get_suffixed_key` from the `HiCacheStorageConfig`:

| Field | Effect |
|-------|--------|
| `model_name` | First segment of `tenant_suffix` |
| `tp_rank`, `tp_size` | `_{tp_rank}_{tp_size}` appended (skipped for MLA models) |
| `pp_rank`, `pp_size` | `_pp{pp_rank}_{pp_size}` appended if `pp_size > 1` |
| `is_mla_model` | Drops the tp segment; all TP ranks share one bundle key |

`kv/` is a reserved pool name slot — MVP only supports the KV pool, but the
key format reserves it so future v2 multi-pool support (Mamba / SWA /
hybrid models) doesn't require a key migration.

### Smoke test (no sglang required)

After `cluster.sh reset 1`:

```bash
AUTUMN_KVCACHE_ENDPOINT=127.0.0.1:9001 \
  /tmp/autumn-py-venv/bin/python python/autumn_kvcache/tests/test_smoke.py
```

Validates: tenant key format, `batch_set_v1` / `batch_get_v1` zero-copy
round-trip on a numpy-backed fake pinned-host pool, `batch_exists`
contiguous-prefix semantics, v0 method fallbacks, and `clear()`.

### F216-E zero-copy data path verification (`put-zc` / `zc-get`)

The client↔PS hops move the value with no intermediate copy: `put-zc` sends the
value as its own iovec from its (registered) backing memory; `zc-get` reads it
straight into a registered dest (RDMA on UCX). To verify both are byte-identical
to the normal `put`/`get` (and interoperate):

```bash
# TCP cluster
./cluster.sh reset 1
head -c 262144 /dev/urandom > /tmp/v.bin                 # 256 KiB (>4 KiB -> VP/log_stream path)
./target/release/autumn-client put-zc k /tmp/v.bin       # zero-copy write
./target/release/autumn-client zc-get k > /tmp/zc.bin    # zero-copy read
cmp /tmp/v.bin /tmp/zc.bin && echo "zero-copy round-trip == original"

# UCX cluster (RoCE) — build the ucx binaries first, then point at the RoCE addr.
# put-zc registers the source + zc-get registers the dest so the value moves by
# RDMA; rc_mlx5 should appear in ps.log (and node1.log for the write).
cargo build --release -p autumn-server --features ucx
export AUTUMN_TRANSPORT=ucx AUTUMN_BIND_HOST="[<roce-ip>]" UCX_TLS="^sysv,posix"
./cluster.sh reset 1
AC="./target/release/autumn-client --manager [<roce-ip>]:9001 --transport ucx"
$AC put-zc k /tmp/v.bin && $AC zc-get k > /tmp/zc.bin && cmp /tmp/v.bin /tmp/zc.bin
```

Status: the **READ path is now fully zero-copy EN → PS → client** over UCX
(R3 + R4, done + verified). A 256 KiB / 8 MiB value is a VP in `log_stream`, so
`zc-get` exercises the EN→PS hop (`MSG_READ_BYTES_ZC` + `read_value_into_pooled`,
value recv'd into a registered RegPool buffer) and the PS→client hop
(`handle_get_zc` emits `[V0 header][zc_meta]` + the value as a SEPARATE iovec
aliasing that buffer — `write_vectored_all`, no concat copy). E2E (1-node UCX
rc_mlx5): byte-identical at 1000 B (inline) / 256 KiB / 8 MiB, both interop
directions (`put-zc`↔`get`, `put`↔`zc-get`). The client↔PS write hop
(`put_zc`/`put-zc`) is also done. Remaining (lower priority, see `feature_list.md`
F216-E): PS→EN explicit send registration (rcache already zero-copy) and
registering the sglang host pool once for memh reads.

**Zero-copy is the DEFAULT on the UCX transport** (F216-E — the old `--zc` flag
was removed). With `--transport ucx`, `perf-check` (and the python `BatchClient`
/ kvcache adapter) automatically use the ZC data path: **writes always**
(MSG_PUT_ZC — cheaper at every size), **reads when the value ≥ 64 KiB**
(`UCX_ZC_READ_MIN_BYTES`; below that the registered-recv per-op overhead exceeds
the small copy it saves, so small reads stay on the regular path). On
`--transport tcp` the regular `MSG_PUT`/`MSG_GET` path runs. So the A/B is now at
the **transport level** — run once per transport:

```bash
AC="./target/release/autumn-client --manager [<roce-ip>]:9001"
$AC --transport tcp perf-check --partitions 8 --pipeline-depth 8 --threads 16 --size 8388608  # regular
$AC --transport ucx perf-check --partitions 8 --pipeline-depth 8 --threads 16 --size 8388608  # auto zero-copy
```

Measured (1-replica P8 d8 t16 UCX rc_mlx5, ZC vs regular on the same UCX cluster):

| size | write ZC/reg | read ZC/reg |
|------|-------------|-------------|
| 4 KiB | **2.6×** (32.5K vs 12.4K ops/s) | 0.82× (−18%) → read stays **regular** below 64 KiB |
| 8 MiB | **1.96×** (1518 vs 775 MB/s) | **2.34×** (2981 vs 1275 MB/s) |

Writes win at every size (drop 3 client copies + rkyv encode). Reads win big at
large sizes (the R4 fully-zero-copy read path) but regress at 4 KiB, hence the
size guard. (`--zc` is kept as a warn-once no-op so old scripts don't break.)

### F219 — TCP recv-side single-copy (read + write) + ZC-CRC removal

F216-E made the **UCX** recv paths zero-copy. F219 does the analogous thing on
**TCP**: the two server recv loops recv a large value straight into a pooled
buffer via a compio *owned* read, skipping the `FrameDecoder` accumulation copy.
TCP can't be true zero-copy (the kernel socket copy is mandatory) — the goal is
**single-copy** (kill the extra app-level copy):

- **Read (PS←EN VP value):** regular `get` of a value > 4 KiB (a VP in
  `log_stream`) now routes through `MSG_READ_BYTES_ZC` on TCP too —
  `read_value_into_pooled` → `call_into_pooled` recvs the value into a
  `PooledBuf` (`ReadHalf::read_exact_into_pooled`). The EN response is already
  pooled + value-separable, so the EN send side also drops its per-op alloc /
  zeroing / encode copy (this subsumes the old F216-F item).
- **Write (PS←client large `MSG_PUT_ZC`):** `drain_zc_writes` recvs values
  ≥ 64 KiB into a `PooledBuf` on TCP (no decoder copy). `perf-check` and the
  `put-zc` CLI send `MSG_PUT_ZC` on TCP for values ≥ 64 KiB; **small writes
  (4 KiB) stay regular `MSG_PUT`** — a vectored put_zc gives no copy win at that
  size and the 4 KiB path is QPS-critical.
- **ZC value CRC removed (no toggle):** the per-value crc32c on the ZC path
  (compute on send + verify on recv = two full passes over every value) is gone.
  Value integrity is left to the transport (UCX NIC ICRC / TCP kernel segment
  checksum). **Normal (non-ZC) RPC frames keep their V1 frame-CRC** — only the
  ZC-read/write value crc is removed.

Manual verification (TCP, single-copy recv + CRC-off round-trip is byte-exact):

```bash
./cluster.sh reset 1
head -c 8388608 /dev/urandom > /tmp/v.bin                  # 8 MiB -> VP/log_stream
AC="./target/release/autumn-client --manager 127.0.0.1:9001 --transport tcp"
$AC put-zc k /tmp/v.bin                                     # TCP write -> drain_zc_writes recv-into-pooled
$AC get   k > /tmp/g.bin                                    # TCP read  -> read_value_into_pooled (PS<-EN)
$AC zc-get k > /tmp/z.bin                                   # client<-PS ZC read framing
cmp /tmp/v.bin /tmp/g.bin && cmp /tmp/v.bin /tmp/z.bin && echo "F219 TCP round-trip == original"
grep "write-recv ZC engaged" /tmp/autumn-rs-logs/ps.log     # expect transport="tcp(pooled)"
```

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
- `libucx-dev` ≥ 1.16 (`pkg-config --modversion ucx`) — only needed if
  you opt into `--features ucx`. Default builds work without it; the
  `ucx-sys-mini` build script gracefully degrades to an empty stub
  (one `cargo:warning`) when pkg-config can't find ucx.
- At least one mlx5 (or other RDMA) HCA with a RoCE v2 GID on a routable
  IP (IPv4 or IPv6 GUA/ULA — link-local fe80::/10 doesn't work)
- Verify with `scripts/check_roce.sh` (exit 0 = ready;
  `--listen-candidates` lists valid bind IPs)

### Build with the UCX feature
    cargo build --workspace --features autumn-transport/ucx

The default build has zero UCX dependencies — `cargo build --workspace`
on a host without libucx will compile `ucx-sys-mini` as an empty stub
(prints a `cargo:warning`) and skip linking `libucp`. Opting into
`--features ucx` without libucx fails at link time with unresolved
`ucp_*` symbols — that's the signal to `apt install libucx-dev`.

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
fewer EPs landing on each partition's single-threaded UCX progress
worker. `perf_check.sh` defaults to `--threads 16` for this reason.
For the UCX cliff at higher thread counts, see "UCX scaling and
limits" below.

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

### UCX scaling and limits

The PS architecture binds **one UCX listener per partition**, hosted on
that partition's P-log thread (one OS thread). Each listener has its
own `UCS_THREAD_MODE_SINGLE` UCX worker driving `ucp_worker_progress`,
so a partition's per-second work is bounded by what one user-space
thread can drive. Adding workers per partition is intentionally not
supported (per-partition fan-out conflicts with the rest of the
thread-per-core design); **scale by adding partitions, not threads**.

**Per-partition load — two axes.** Two different things land on each
partition's single UCX worker, and they grow with the client config in
different ways.

*Concurrent in-flight UCX ops per partition:*

```
in_flight_per_partition = (client_threads × pipeline_depth) ÷ partitions
```

This is symmetric for read and write — both perf-check phases cap each
client thread's `FuturesUnordered` at `pipeline_depth`, so the
aggregate `client_threads × pipeline_depth` total in-flight is spread
across all partitions roughly uniformly (perf-check write shards by
partition affinity in `tid % partitions`; perf-check read sweeps the
keys it just wrote, which were similarly sharded). At
`--threads 256 --pipeline-depth 16 --partitions 8` that's
`256×16÷8 = 512` concurrent ops per partition for *both* phases.

*Open EPs (UCX endpoints) per partition:*

```
eps_per_partition_read  = client_threads
eps_per_partition_write = client_threads ÷ partitions
```

`perf-check` read keeps a per-thread `HashMap<ps_addr, RpcClient>`
pool, so every client thread eventually has one EP to *every*
partition that owns one of its keys (`autumn_client.rs:2005-2034`).
Write pins each thread to one partition (line 1863), so each thread
opens one EP total. At `--threads 256 --partitions 8` the read phase
holds **256 EPs per partition's worker** while the write phase holds
**32**.

**The cliff is the EP-count axis, not the in-flight axis.** Each EP
has per-EP state in the `ucp_worker` (queue pair, internal buffers,
progress callbacks); even when the aggregate in-flight count is the
same, a worker that has to walk 256 EPs per `ucp_worker_progress`
iteration finishes far less work per second than one walking 32. So
`perf-check` read collapses well before write at the same thread
count.

**Empirical bands** at `--partitions 8 --pipeline-depth 16 --size 4k`,
RoCEv2 cross-host on this cluster, post-`fix(ucx): drop UcxEp
close-on-Drop`:

| `--threads` | EPs / partition (read) | in-flight / partition | write ops/s | read ops/s | read p99 |
|---|---|---|---|---|---|
| 16  | 16  | 32  | 104 k | 970 k | 0.46 ms ← supported |
| 32  | 32  | 64  | 80 k  | 610 k | 1.16 ms ← degrades, p99 ~2.5× |
| 64  | 64  | 128 | 14 k  | 105 k | 18 ms ← cliff, p99 ~40× |
| 256 | 256 | 512 | ~0    | 0     | — ← hard fail (read collapses, log spams `Connection reset by remote peer`) |

The cliff lands between 32 and 64 EPs per partition's worker — beyond
that, rc_mlx5's RNR / endpoint-timeout fires because the single-threaded
progress task can't keep up with all the EPs it's responsible for. RDMA
device caps and FD limits both have ~500× headroom on this host
(`max_qp = 131 072`, `ulimit -n = 1 048 576`), so this is purely a
user-space single-thread CPU ceiling, not a resource exhaustion.

**Recommended client config band.** Read fan-out is the binding
constraint, since EP count for reads = `client_threads`. Keep:

```
client_threads ÷ partitions ≲ 32          # read EPs per partition
```

Examples:

| config | EPs/partition (read) | OK? |
|---|---|---|
| `--threads 16 --partitions 8`   | 2   | ✓ comfortable |
| `--threads 32 --partitions 8`   | 4   | ✓ |
| `--threads 64 --partitions 8`   | 8   | ✓ workable but not measured here |
| `--threads 256 --partitions 8`  | 32  | borderline — sit at the cliff |
| `--threads 256 --partitions 32` | 8   | ✓ — same total client load, more partitions |

(The empirical sweep used the same 256-key-space-per-thread that
write produced, so the read EP count grows linearly with client thread
count regardless of partition count; the recommendation is to scale by
partitions any time you need more client threads.)

If you need more total client concurrency, add partitions
(`AUTUMN_BOOTSTRAP_PRESPLIT=N:hexstring` at bootstrap). Each partition
gets its own listener / worker, so total per-PS UCX progress capacity
scales linearly with partition count.

> **cluster.sh note (F221):** each partition needs 2 PS cores (P-log +
> P-bulk) and the PS refuses partitions past `cpuset_len/2`. When you launch
> via `cluster.sh`, the PS core budget is **auto-derived from the presplit
> count** — `AUTUMN_BOOTSTRAP_PRESPLIT=16:hexstring` makes cluster.sh give the
> PS 32 cores (`cpuset 16-47`) so all 16 partitions open. Set
> `AUTUMN_PS_PARTS_HINT=N` to override. Without this (pre-F221) a 16-partition
> presplit on the default 8-partition budget stranded partitions 8–15 with
> `F196: core budget exhausted`.

**TCP transport has no equivalent ceiling** at this scale — kernel
sockets fan accept/recv I/O across cores. If your workload genuinely
needs more concurrent inbound EPs per PS-side worker than UCX supports
(`client_threads ÷ partitions ≳ 64`), pick TCP. UCX wins on p99 inside
its supported region (0.46 ms vs ~0.5 ms TCP at 16t in the table at
the top of this section); the win is gone outside that region.

**The same applies in production.** A workload doing N concurrent
point-reads on UCX against M partitions per PS should plan for
N ÷ M ≲ ~32. Otherwise, either add partitions or use TCP for that
RPC path.

---

## F183 / F185 — Partition merge + advisory policy

### Manual partition merge

Merges two adjacent partitions on the same PS. The survivor keeps its `part_id`;
the victim is deleted from the manager. The merged partition's range becomes
`[SURVIVOR.start, VICTIM.end)`.

```bash
autumn-client --manager 127.0.0.1:9001 merge <SURVIVOR_PART_ID> <VICTIM_PART_ID>
```

**No need to stop writes** — F185 closes the previous Stage-1 ~5 % loss window
by orchestrating freeze + commit on the manager. In-flight writes during the
merge window receive `CODE_UNAVAILABLE` and are retried by the standard SDK
`refresh_regions` path.

The CLI is a thin wrapper around one new manager RPC, `MSG_MERGE_PARTITIONS`.
The manager (which is leader-fenced + crash-recoverable via etcd) drives the
sequence:

1. Acquire admin owner-lock keyed on the partition pair
2. `MSG_MERGE_FREEZE { freeze: true }` to victim PS — drains pending+inflight,
   flushes every imm, halts new writes; returns OK only after the post-freeze
   checkpoint is durable
3. Same to survivor PS
4. Capture `commit_length` × 6 (3 streams × 2 partitions) — race-free now
5. `MultiModifyMerge` — single atomic etcd txn (F124-style):
   - splices victim's stream extents into survivor's (refs++ CoW; same as
     split's `compute_duplicate_stream` but inverted)
   - allocates a fresh log_stream tail extent (`E_new`) on K replicas
   - merges victim's `partition_vp_refs` snapshot into survivor's
   - widens survivor's `rg.end_key` to victim's `rg.end_key`
   - deletes victim's `partitions/`, three `streams/`, `regions/`,
     `partitionVpRefs/`, `partitionLastOp/` etcd keys
6. On success: leave both PSes frozen — `region_sync_loop` (~2 s tick) drops
   the frozen `PartitionData` and the survivor reopens with the merged state
   and `frozen_for_merge = None`. On failure: `MSG_MERGE_FREEZE { freeze:
   false }` rollback, plus the PS-side `FREEZE_TTL = 30 s` backstop for the
   orchestrator-crash case.

Crash safety:
- **CLI crash** at any point: benign — manager continues to completion.
- **Manager crash before commit**: failover; new leader sees no half-state in
  etcd; PSes auto-unfreeze via `FREEZE_TTL`. Merge can be retried.
- **Manager crash after commit**: merge is durable; `region_sync_loop` drives
  the reload normally.
- **PS crash mid-flow**: in-memory freeze flag lost on restart; partition
  reopens with whichever state the etcd txn settled on.

Preconditions enforced by the manager:
- `survivor.end_key == victim.start_key` (adjacent in keyspace)
- Neither side's source extents in `ec_conversion_inflight` /
  `recovery_tasks` / `pending_extent_deletes`
- F146-style verify-at-apply on `pre_bump_eversion` snapshot
- F149 leader-fence on the etcd txn

### Policy candidates

```bash
autumn-client --manager 127.0.0.1:9001 policy-candidates
```

Shows the manager's advisory engine output (recomputed every 60 s from
the last 30 min of per-partition `(size_bytes, req_per_sec, imm_full_per_sec)`
samples reported by each PS via `MSG_REPORT_PARTITION_LOAD` every 5 s).

Thresholds (hard-coded in Stage 1):

| Trigger | Threshold |
|---|---|
| SPLIT — size_hard | > 50 GiB |
| SPLIT — qps + size | qps > 50 K AND size > 1 GiB |
| SPLIT — imm_full saturation | imm_full_per_sec > 10 |
| SPLIT — cooldown | 1 h since last op on this partition |
| MERGE — both small | each side < 1 GiB |
| MERGE — both cold | sum req_per_sec < 5 K, both imm_full == 0 |
| MERGE — cooldown | 6 h since last op on either side |
| MERGE — feasibility | both on same PS (cross-PS marked `same_ps=false`) |

The 10× hysteresis between split (50 K qps) and merge (5 K qps) prevents
oscillation. Stage 1 emits cross-PS candidates with `feas=no` so operators
can plan co-location manually before merging.

### Auto-trigger (Stage 2/3, deferred)

Per `feedback_auto_split_before_merge.md` (auto-memory): autumn-rs is
thread-per-core; **merge concentrates load onto a single P-log core**, so
the staging order is `auto-split first` (Stage 2), `auto-merge second`
(Stage 3). Both gated behind feature flags on the manager. See
`docs/superpowers/specs/2026-05-09-partition-merge-and-split-merge-policy-design.md`
§6 for burn-in criteria.

## F211 — Operator-Driven Node Lifecycle (2026-05-17)

autumn-rs no longer auto-recovers extents on a transient node failure.
Manager tracks `Online ↔ Suspected` per EN automatically, but only a
human operator (via `mgr_fence_node`) flips a node to `Fenced` —
which is what kicks `recovery_dispatch_loop` and the EC abandon path.
Modelled on HDFS decommission + Ceph `noout`.

### New manager admin RPCs

| MSG | Wire id | Purpose |
|---|---|---|
| `MSG_LIST_NODE_STATES` | 0x3C | Read-only — every EN's auto state + override |
| `MSG_EXTENT_HEALTH_REPORT` | 0x3D | Read-only — per-slot health for filtered or unhealthy extents |
| `MSG_LIST_EC_INFLIGHT_MARKERS` | 0x3E | Read-only — EC convert markers + coord state |
| `MSG_FENCE_NODE` | 0x3F | Operator confirms node dead; triggers cleanup |
| `MSG_SET_NODE_MAINTENANCE` | 0x40 | Soft-pause without recovery |
| `MSG_CLEAR_NODE_OVERRIDE` | 0x41 | Undo a fence/maintenance |
| `MSG_REMOVE_NODE` | 0x42 | Hard delete after fence + drain |
| `MSG_RECOVERY_STATS` | 0x43 | Inflight + per-source/target counters |
| `MSG_QUERY_AUDIT_LOG` | 0x44 | All operator-action history (90 d retention) |

### Operator runbook (manual smoke)

```bash
# Identify suspected nodes (auto state). Use the F211-G Python script
# once it lands; for now this is a manager-RPC-direct path.

# Confirm a specific node is permanently dead (ssh, k8s, monitoring).
# Then explicitly fence via the manager RPC. Effects:
#   - Writes node_override/{node_id} to etcd (persistent)
#   - Bumps owner-lock revisions on every extent the node touched
#   - Auto-abandons EC convert markers whose target_nodes[0] == node
#     (writes ec_convert_advisory/<extent_id> for OP follow-up)
#   - recovery_dispatch_loop now proceeds to recover all the slots

# After recovery has drained, remove the node. Preconditions:
#   - Node must already be Fenced
#   - No extent.replicates / .parity list still references it
#   - No inflight marker's target_nodes contains it
# Effect: hard delete + writes decommissioned/{node_id} tombstone.
```

### Backwards-incompat behaviour switches

The dispatch gate is configurable so legacy ops can opt out for the
first deployment window:

```bash
# Default (recommended). Recovery fires ONLY when operator has
# explicitly fenced the failing node.
AUTUMN_MGR_RECOVERY_GATE=fenced_only autumn-manager-server ...

# Legacy: trigger on disk.online == false (pre-F211 behaviour).
AUTUMN_MGR_RECOVERY_GATE=auto_disk autumn-manager-server ...
```

Recovery throttling (F211-H, on by default):

```bash
AUTUMN_MGR_RECOVERY_MAX_GLOBAL=64       # cluster-wide concurrent recoveries
AUTUMN_MGR_RECOVERY_MAX_PER_SOURCE=4    # per-source-EN concurrent reads
AUTUMN_MGR_RECOVERY_MAX_PER_TARGET=2    # per-target-EN concurrent writes
```

Other tunables:

```bash
AUTUMN_MGR_NODE_SUSPECTED_TIMEOUT_SECS=10   # auto Online → Suspected
AUTUMN_MGR_AUDIT_RETENTION_DAYS=90           # audit log GC
```


---

## F227 — the seal must be lenient (an EN can be unreachable at seal time) (2026-05-29)

**Design principle (load-bearing — do NOT make the seal strict):** when the
manager seals/commits a stream extent, **it is entirely possible that some
extent-node cannot be reached** — in fact you usually seal *because* a node
just went down. The manager seal therefore **must NOT require every committed
replica to respond.** It seals at the `min` over the *reachable* committed
members (≥ a small durability `floor`), and leaves an unreachable member's
`avali` bit unset to be reconciled later by recovery / `re_avali`.

This is safe because **the guarantee comes from the append path, not from a
strict seal**: appends are all-replica-ACK (`apply_completion` acks only when
every replica wrote), so the acked prefix is present on *every* committed
member. Hence `min` over the reachable members is always ≥ the acked length and
**never drops acked data**, no matter which nodes are down. Requiring all
committed members to respond would block the seal forever whenever a node is
down — that was the bug #3 seal-wedge.

The flip side — seal-over-reachable can promote an un-acked-but-replicated tail
byte to committed (data *gain*, never *loss*) — is **acceptable and
intentional**: it matches the system's existing uncertain-write semantics (a
PUT that timed out on the client may still land; treat such keys as uncertain).
Do **not** add a strict mode or acked-watermark threading to remove it — that
would trade a benign data-gain for a real data-*loss* risk. See
`crates/manager/CLAUDE.md` note 28 and `crates/stream/CLAUDE.md` (F227).

### Bug #3 — kill+restart partition wedge (RESOLVED 2026-05-29)

Two independent layers, both fixed:

1. **Poison-wedge** — `commit_length` on the extent-node is now **check-only**
   (rejects a stale owner, but never performs the old fence-*handover* bump).
   Write-ownership is established **exclusively by the append path**. Before the
   fix, the manager's merge orchestration acquired an `admin-merge:<v>:<s>`
   owner-lock whose revision is the global monotonic owner-revision counter,
   then probed `commit_length` with it — the handover bumped the EN fence and
   stole write-ownership from the live partition server (which holds its lower
   acquire-time revision), poisoning the partition on its next append.
2. **Routing-wedge** — per-partition listeners now fall back to an OS-assigned
   port (`bind :0`) when the deterministic `base_port + ord` port hits
   `EADDRINUSE`, and register the **actual** bound port. The deterministic port
   can collide with an OS *ephemeral* local port held by an outbound socket when
   `base_port` falls inside `ip_local_port_range`; the split-child then never
   binds a listener → no `part_addr` → unroutable. Clients route via the
   registered address, so the port need not be deterministic.

**Manual verification (chaos):**
```bash
# Harsh regime that reproduced bug #3 (4-data/2-parity EC, 90s, kill+fence+split+merge).
AUTUMN_CHAOS_SEED=6 AUTUMN_CHAOS_DURATION_SECS=90 \
AUTUMN_CHAOS_EC_K=4 AUTUMN_CHAOS_EC_M=2 AUTUMN_CHAOS_NUM_ENS=8 \
AUTUMN_CHAOS_ACTIONS=split,merge,ec,fence,flush,compact,gc,kill,killfence,partition,latency \
  cargo test -p autumn-manager --test system_chaos -- --ignored --nocapture
# Expect: "per-key verify: total=N mismatches=0 not_found=0" and "test result: ok".
# Before the fixes this failed ~3/8 runs (poison wedge) then ~1/8 (routing wedge);
# after, seed=6 passes 10/10. The PS log shows "bound OS-assigned fallback port"
# 2-3x per run — the ephemeral-collision fallback engaging as designed.
```

## Notes

- `IoMode::IoUring` is not yet implemented; extent nodes use `IoMode::Standard`.
- Without `--etcd`, manager runs in-memory only (metadata lost on restart).
- Erasure coding (`parity_shard > 0`) is not yet implemented; use `parity_shard=0`.
- There is currently no automatic partition server failover; if a PS crashes, restart it
  with the same `--psid` and it will re-register and reload its partitions.
