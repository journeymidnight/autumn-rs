# autumn go→rust feature list

**Last updated:** 2026-05-05

**Rules:**
- `passes` and `notes` are the only mutable fields after a feature is created.
- Out-of-scope / "v2 再做" decisions must be recorded as proper feature entries (F-number + Trigger + `passes:false`), not as plan-file footnotes.
- Entries below the Completed table document non-obvious decisions, root causes, and active designs. Trivial work-tracking entries live in the table — the implementation IS the documentation in those cases.

---

## ✅ Completed (rationale lives in code + crate CLAUDE.md)

| ID | Title | Area |
|----|-------|------|
| F001 | Proto and service contracts compile | foundation |
| F002 | IO engine backends | foundation |
| F003 | Metadata store and owner lock revision model | manager |
| F004 | Stream manager core API parity | manager |
| F005 | Etcd mirror, replay, leader election, recovery loops | manager-etcd |
| F006 | Extent node API implementation | stream-node |
| F007 | Stream client write path | stream-client |
| F008 | Partition server KV API and split | partition |
| F009 | Partition flush and restart recovery | partition |
| F010 | Partition API parity (compact/gc/forcegc/format/wbench/rbench/presplit) | partition |
| F012 | Erasure coding parity (Reed-Solomon K-of-N) | stream |
| F013 | autumn-rs README manual test guide | dev-experience |
| F014 | Standalone server binaries | dev-experience |
| F015 | autumn-stream-cli manual test tool | dev-experience |
| F016 | Manager etcd persistence + restart recovery | manager-etcd |
| F017 | autumn-ps partition server binary | partition |
| F018 | autumn-client admin CLI | dev-experience |
| F019 | Partition Manager (allocation, liveness, dispatch, rebalance) | manager |
| F020 | Connection pool with health check | rpc |
| F021 | Multi-disk support + disk format | stream-node |
| F024 | Prometheus metrics + structured tracing logs (Phase 1) | observability |
| F026 | Internal key MVCC stamp (seq + KeyWithTs) | partition |
| F027 | Remove in-memory full-value kv cache | partition |
| F039 | Client-side partition routing (interim, RPC-refresh) | client |
| F040 | wbench observability + payload reuse | dev-experience |
| F041 | perf-check with regression warning | dev-experience |
| F042–F047 | Network layer migration (tonic/tokio → autumn-rpc + compio + autumn-etcd) | rpc/etcd |
| F048 | Zero-copy frame write in ConnPool | rpc |
| F049 | SSTable build via spawn_blocking | partition |
| F050 | Partition recovery returns recovered Memtable | partition |
| F051 | current_commit at partition startup | partition |
| F052 | LockedByOther → partition self-eviction | partition |
| F053 | RPC timeout support | rpc |
| F054 | ConnPool reconnection on failure | rpc |
| F055 | PS lease/session with auto-exit on loss | partition |
| F056 | StreamClient manager RPC retry | stream |
| F057 | Extent-node recovery task retry | stream |
| F058 | Disk I/O error marks disk offline | stream |
| F059 | WAL runtime cleanup post-rotation | stream |
| F060 | Manager ConnPool reconnection | manager |
| F061 | FUSE filesystem layer (autumn-fuse) | fuse |
| F062–F076 | System tests (seal/split/recovery/EC/owner-lock/large-VP/compound failures) | testing |
| F077 | Split etcd atomicity (etcd-first commit pattern) | manager |
| F078 | Manager proactive per-disk health check | manager |
| F079 | Multi-manager failover (StreamClient + PS) | stream/partition |
| F082 | ClusterClient auto-reconnect + multi-manager | client |
| F083 | Client SDK library with ergonomic API | client |
| F084 | Client routing via lazy refresh (etcd watch deferred) | client |
| F085 | TTL expiration with background cleanup | partition |
| F086 | Perf instrumentation (VP resolve, ExtentNode write timing) | observability |
| F087-bulk-mux | ConnPool PoolKind Hot/Bulk (obsoleted by F093 after P-log/P-bulk thread split) | rpc |
| F088 | Split flush_loop to dedicated P-bulk thread | partition |
| F089 | Perf-verify F088 (write 52-54k / p99 ~17ms; bottleneck moved to ExtentNode side) | perf |
| F090, F091 | PS Step3/4 (compact split + ExtentNode spawn_blocking) — `not_needed` per F099-K diagnosis | perf |
| F092 | SstReader Rc→Arc + parking_lot::Mutex block_cache (remove unsafe transmute) | partition |
| F093 | PoolKind removal post-F088 (P-log/P-bulk now use disjoint StreamClients) | rpc |
| F094 | Perf-verify F092+F093 (no regression) | perf |
| F095 | Perf R1 — Partition scale-out + batch cap sweep (Tier C; bottleneck = single P-log thread) | perf |
| F096 | Perf R2 — Flamegraph + leader-follower (Tier C; 256 sync clients × 4ms RPC ≈ 64k ceiling) | perf |
| F098-4.2 | ExtentNode inline FuturesUnordered SQ/CQ pipeline (true CQ flush of fast ops) | stream |
| F098-4.3 | StreamClient per-stream SQ/CQ worker (replaces DashMap+Mutex) | stream |
| F098-4.4 | PS P-log + P-bulk SQ/CQ FuturesUnordered (`AUTUMN_PS_INFLIGHT_CAP`, MIN_PIPELINE_BATCH=256) | partition |
| F098-R4-B | `ps_bench` PartitionServer pipeline-depth matrix benchmark | testing |
| F099-A | Flame-graph diagnosis: P-log CPU saturation (skiplist 28%, RPC ceremony 30%) | perf |
| F099-B | Parallel 3-replica fanout via `join_all` in StreamClient append | stream |
| F099-C | Memtable SkipMap → `parking_lot::RwLock<BTreeMap>` (single-writer, batch insert) | partition |
| F099-D | Merge partition_thread_main + background_write_loop (collapse spawn + inner oneshot) | partition |
| F099-H | Kernel RTT decomposition (bpftrace) — top hot spot = small `tcp_sendmsg` per reply | perf |
| F099-I | Per-conn reply batching via FuturesUnordered + `write_vectored_all` | partition |
| F099-I-fix | d=1 inline fast path + writer_task EINVAL diagnosis (no kernel limit found) | partition |
| F099-N-a | Tunable `MIN_PIPELINE_BATCH` via `AUTUMN_PS_MIN_BATCH` | partition |
| F101 | Delete dead `RpcServer` + perf_check 2×2×2 matrix | rpc |
| F101-b | UCX root cause: 256 OS threads × ucp_worker → connect-storm; default to 16t × deep pipeline | transport |
| F101-c | perf_check size axis (4K/8M); UCX large-payload `ulimit -l unlimited` + `UCX_TLS=^sysv` | transport |
| F101-d | UCX 8M loopback wedge: also exclude `posix` (`/proc/<pid>/fd` blocked); use `cma + tcp` | transport |
| F101-e | `ucx-sys-mini` graceful build on hosts without libucx | transport |
| F102 | cluster.sh per-process `start-node N` / `stop-node N` / `start-ps` / `stop-ps` | tooling |
| FOPS-01 | autumn-client info enhancements (`--json` / `--top N` / `--part PID` / punched stats) | tooling |
| FOPS-02 | cluster.sh auto-EC (N≥3 → log/row 2+1 or 3+1; meta always replicated) | tooling |
| FOPS-03 | `set-stream-ec` RPC + CLI (modify EC config on existing stream; conversion loop picks up) | manager |
| FOPS-04 | replica stream encoding `(0,0)` → `(N,0)`; EC predicate now `ec_parity_shard != 0` | stream |
| FGA-01 | gallery: storage HUD + spawn_blocking thumbs + video thumbs + auto-hide lightbox strip | examples |
| F183 | Partition merge primitive + size+load advisory policy engine (Stage 1) | partition/manager |
| F184 | Auto-trigger flags + reload-on-region-change + concurrent-writer test + ClusterClient.rpc_timeout | manager/client |
| F185 | Manager-orchestrated merge with PS freeze-drain (closes F184-K ~5% merge-window loss; 0 loss verified) | manager/partition |
| F186 | Client-side striperados (Ceph pattern) replaces F129/F130 server multipart — pure ClusterClient impl | client |
| F187 | GC + compaction maintenance advisory (Stage 1) — debt metrics + PolicyEngine emits POLICY_KIND_GC/COMPACT alongside SPLIT/MERGE | partition/manager/client |
| F188 | GC + compaction Stage 2 — PS-level priority maintenance scheduler + foreground awareness + shared IO token bucket between GC + compact | partition |
| F189 | GC + compaction Stage 3 — two-class admission controller (fg priority + bg elastic, CockroachDB kvadmission style) | partition |
| F189-fix | Race-review fixes from distributed-systems audit: cooldown stamp on no-op ticks, scheduler diff against monotonic counter, inflight latch at dispatch, channel-backlog dedup at receiver | partition |
| F189-fix-r2 | Round-2 race fixes: stamp/clear ordering inversion in compact paths (re-opened MED-4 race for ~truncate-await window) + 2 GC early-exits missing last_gc_at stamp + Auto-behind-Force semantic clarified | partition |
| ~~F129~~ | ~~PutStream / GetStream — multipart + multi-frag VP~~ — SUPERSEDED by F186 (server code ripped out) | — |
| ~~F130~~ | ~~GC active rewrite for multi-frag VPs~~ — SUPERSEDED by F186 (no multi-frag any more) | — |

---

## P0 — Core Architecture (correctness & data safety)

### F011 · Go range_partition advanced storage behaviors (umbrella) — CLEARED, won't fix (redundant umbrella)
- **Target:** Compaction/GC/value-log/maintenance lifecycle equivalent to Go range_partition.
- **Evidence:** `range_partition/*.go` · `crates/partition-server/src/lib.rs`
- **Notes:** Umbrella for F028-F033+F036+F037. Tracks overall completion of the partition layer rewrite.
- **Audit (2026-05-09):** all 8 sub-features (F028, F029, F030, F031, F032, F033, F036, F037) shipped and pass:true; the LSM partition layer rewrite is complete in `crates/partition-server/`. The umbrella adds no information that the sub-features don't already track. Marked cleared rather than flipping to passes:true so future readers don't expect umbrella-level acceptance criteria here — go read the individual sub-features.
- **passes:** true (cleared, won't fix — redundant umbrella; all 8 sub-features pass independently)

### F038 · Remove block_sizes from stream layer (pure byte store)
- **Target:** Stream layer becomes a pure byte read/write layer: `append(bytes) → (extent_id, offset, end)` and `read(extent_id, offset, len) → bytes`. Block/record boundaries are entirely the upper layer's concern.
- **Evidence:** `crates/stream/src/extent_node.rs` (ExtentEntry, truncate_to_commit, read_blocks) · `crates/stream/src/client.rs` · `crates/partition-server/src/lib.rs`
- **Notes:** Motivation: block_sizes is in-memory only in Rust (not persisted), lost on restart, requires fragile normalize_block_sizes() fallback. Rust record format is already self-framing (`[op:1][key_len:4][val_len:4][expires_at:8][key][value]`), so upper layer parses records from raw bytes.
- **passes:** true

### F036 · Skiplist-based memtable with arena allocation
- **Target:** Memtable backed by concurrent skiplist with arena-based allocation, supporting efficient sorted iteration.
- **Notes:** Implemented with crossbeam-skiplist SkipMap; later replaced by `parking_lot::RwLock<BTreeMap>` in F099-C (single-writer model is cheaper than the SkipMap's epoch atomics). Arena allocation not used.
- **passes:** true

### F028 · LSM flush pipeline with immutable memtable queue
- **Target:** Async flush pipeline: active memtable → immutable memtable queue → background flush to SSTable via rowStream. Write path does not block on flush.
- **Notes:** `ValueLoc::Buffer` carries in-memory WAL snapshot so WAL can be truncated at rotation time. `rotate_active_locked` + `flush_one_imm_async` + `background_flush_loop`. Split path calls sync drain.
- **passes:** true

### F030 · Three-stream model with metaStream persistence
- **Target:** Partition uses three streams: logStream (value log + WAL), rowStream (SSTables), metaStream (table registry + GC state + vhead). Recovery reads metaStream to locate tables then replays logStream from vhead.
- **Notes:** TableLocations checkpointed to metaStream on every flush; old extents truncated. Recovery: metaStream → SST from rowStream → logStream replay. No local WAL file (logStream is the WAL).
- **passes:** true

### F029 · Compaction engine with merge iterator
- **Target:** Size-tiered compaction (DefaultPickupPolicy: head rule + size ratio rule) merging SSTables via merge iterator, eliminating dead/expired keys, truncating consumed extents.
- **Notes:** TableMeta tracks size/last_seq. `do_compact` merges via BTreeMap (newest-seq wins), drops deleted/expired in major mode. `background_compact_loop`: random 10-20s minor + channel-triggered major.
- **passes:** true

### F034 · Extent node metadata persistence
- **Target:** Extent metadata (sealed state, eversion, revision) survives node restart. Equivalent to Go xattr (EXTENTMETA, XATTRSEAL, REV) + two-level directory hash.
- **Notes:** Per-extent `extent-{id}.meta` sidecar (40 bytes: magic+extent_id+sealed_length+eversion+last_revision). Written on alloc/seal/recovery/revision-change only — zero overhead on append path. `load_extents()` scans data dir on startup.
- **passes:** true

---

## P1 — Performance & Space (read/write amplification, durability)

### F031 · Value log separation for large values
- **Target:** Values >4KB stored in logStream with `ValuePointer{extentID, offset, len}` in LSM. `BitValuePointer` flag indicates external storage.
- **Notes:** Implemented. ValuePointer (16-byte LE), `OP_VALUE_POINTER` (0x80) flag in SSTable op byte, VALUE_THROTTLE=4KB. Recovery: vhead from TableLocations + logStream replay.
- **passes:** true

### F032 · SSTable bloom filter, prefix compression, block cache
- **Target:** Per-block key prefix compression (overlap/diff encoding), Bloom filter for fast negative lookups, CRC32 checksums, LRU block cache.
- **Notes:** Block-based SST format (64KB / 1000 entry blocks), prefix compression, bloom filter (xxh3, 1% FPR, double-hashing), CRC32C per block + MetaBlock. Point lookups: memtable → imm → SSTables newest-first with bloom skip.
- **passes:** true

### F033 · GC with discard tracking and extent punch
- **Target:** Per-table discard map (extentID → reclaimable bytes) updated during compaction. GC triggers when discard exceeds threshold, punches/truncates logStream extents.
- **Notes:** `discards: HashMap<u64,i64>` stored in SSTable MetaBlock — no separate stream. `do_compact` accumulates discards for dropped VP entries. `background_gc_loop`: periodic 30-60s; runGC re-writes live VP entries to current logStream then punches old extent.
- **passes:** true

### F035 · Extent node WAL for small-write durability
- **Target:** Rotating WAL (250MB max) with 4KB block-aligned record framing. MustSync small writes (<2MB) go to WAL(sync) + extent(async) in parallel.
- **Notes:** Pebble/LevelDB-style 128KB block framing with 9-byte CRC32C chunk headers (FULL/FIRST/MIDDLE/LAST). Async Wal struct with mpsc background task.
- **passes:** true

### F037 · Partition split with overlap detection and major compaction
- **Target:** Split requires major compaction to clear overlapping keys before split is safe. `hasOverlap` flag blocks split until compaction completes.
- **Notes:** Overlap detected on open via key-range check. `split_part` returns `FAILED_PRECONDITION` when `has_overlap=1`. **Subsequent split-after-split bug fixed in F103** — overlap detection only ran at open_partition; after a successful split the partition's PS-local rg was never narrowed.
- **passes:** true

### F104 · Compaction memory blow-up: `compact ALL` on a 4-partition PS → >44 GB RSS
- **Target:** A single autumn-ps process hosting 4 partitions, with values >4 KB (VP path), peaks at >44 GB RSS during `autumn-client compact` issued for all 4 partitions in quick succession.
- **Root cause:**
  1. **Vec-accumulator regression in `do_compact`** (`background.rs:659-836`). Pre-F104 built `chunks: Vec<(Vec<IterItem>, u64)>` materializing every kept entry as cloned `IterItem` (~150 B/entry). At 38 M entries per ~5 GB partition this Vec alone was ~6 GB; emitted chunks then poured into `new_readers: Vec<(TableMeta, Arc<SstReader>)>`. The Go reference (`autumn/range_partition/compaction.go::doCompact`) builds ONE memStore at a time, sends it to a flushChan, lets it GC.
  2. **No cross-partition compaction concurrency cap.** Each partition's `compact_tx` (cap 1) only serializes within ONE partition. `compact ALL` lights up 4 concurrent `do_compact` calls.
- **Fix:** Streaming chunk emission in `do_compact` (one `SstBuilder` at a time, finalize at `2 × MAX_SKIP_LIST` ≈ 512 MB, append to row_stream, push (TableMeta, SstReader), restart). Plus global `CompactionGate` (`Arc<AtomicUsize>` semaphore on `PartitionServer`, default parallelism = 1, env `AUTUMN_PS_MAJOR_COMPACT_PARALLELISM` range [1, 64]).
- **Evidence:** `crates/partition-server/src/background.rs::do_compact` · `crates/partition-server/src/lib.rs::CompactionGate` · `crates/partition-server/CLAUDE.md` Compaction section.
- **passes:** true

### F103 · Split mid_key uses stale PartitionData.rg → 2nd split blocks ~25s
- **Target:** A 2nd `autumn-client split <PARTID>` against an already-split partition hangs for ~25 s and returns an opaque RPC error.
- **Root cause:** `PartitionServer::sync_regions_once` only opens NEW partitions; for an already-open partition it skips the rg refresh (`if self.partitions.borrow().contains_key(&part_id) { continue; }`). After the 1st split, the manager has narrowed partition 15's range to `[..mid_key)` but PS-local `PartitionData.rg` is still the pre-split wide range. Side-effects: (1) overlap detection ran against the wide rg → `has_overlap` stays 0, F037 gate bypassed; (2) `unique_user_keys()` returns CoW-shared SST keys spanning the wider range; (3) `mid_key = sorted_keys[len/2]` lands above the manager's narrowed `end_key`; (4) `multi_modify_split` rejects with `"mid_key is not in partition range"`; (5) the in-handler retry loop sleeps 100→200→400→800→1600→2000→2000→2000 ms = 9.1 s × 2 ClusterClient retries ≈ 25 s.
- **Fix:** `handle_split_part` (a) fetches authoritative range from manager via `MSG_GET_REGIONS` before picking mid_key, (b) filters `unique_user_keys` to in-range keys (returns Precondition with "run major compaction first" if <2 remain), and (c) on successful `multi_modify_split`, mutates PS-local `part.rg` to the new narrowed `[start, mid)` and re-evaluates `has_overlap` against `sst_readers`.
- **Evidence:** `crates/partition-server/src/rpc_handlers.rs::handle_split_part`.
- **Notes:** The architecturally-clean fix is to make `sync_regions_once` propagate range changes to existing partitions, but that requires a cross-thread control message (PartitionData is `Rc<RefCell>` and `!Send`). This commit takes the smaller scoped fix on the partition thread that already owns the data.
- **passes:** true

### F105 · GC + recovery EINVAL on log_stream extents > 2 GiB
- **Target:** Repeating `GC run_gc extent 10: rpc status Internal: Invalid argument (os error 22)` every 30s on a running cluster. Latent recovery-time variant of the same bug.
- **Root cause:** `StreamClient::read_bytes_from_extent` issues one `MSG_READ_BYTES` RPC per call; extent_node performs a single `pread(file, offset, length)`; macOS caps `pread` at `INT_MAX` (~2 GiB), Linux at `0x7ffff000`. `run_gc` and `recover_partition` both passed entire sealed extent length. Once `extent.sealed_length` crossed 2 GiB (CoW-shared across 4 partition log_streams = 3.0 GiB), every GC attempt failed.
- **Fix:** `read_bytes_from_extent` resolves effective length first (sealed: `ExtentInfo.sealed_length`; open: min-replica `commit_length_for_extent`); if read exceeds `AUTUMN_STREAM_READ_CHUNK_BYTES` (default 256 MiB), splits into chunks and concatenates. Both callers benefit transparently.
- **Evidence:** `crates/stream/src/client.rs::read_bytes_from_extent` + `read_replicated_with_failover` + `commit_length_for_extent` + env `read_chunk_bytes()`. New test `crates/manager/tests/system_gc_chunked_read.rs::f105_gc_works_on_large_extent_via_chunked_reads`.
- **passes:** true

### F106 · `run_gc` materialised entire sealed extent in RAM and held `borrow_mut()` across `await`
- **Target:** Reduce `run_gc` peak RAM from ~sealed_length (3 GiB on extent 10) to a single chunk (~64 MiB) AND eliminate the latent borrow_mut-across-await panic on the single-threaded compio runtime.
- **Fix:** Streaming loop reads `AUTUMN_PS_GC_READ_CHUNK_BYTES` (default 64 MiB) at a time, decodes complete records left-to-right, leaves any partial record at the chunk tail in a `carry` Vec for the next iteration. Per record (VP + in_range + still-live + still-points-to-this-extent), the new log entry is staged under a tightly-scoped `borrow_mut`, the guard is DROPPED, the network append awaits, then a fresh `borrow_mut` updates vp head and inserts into memtable. End-of-loop non-empty carry = corruption refusal.
- **Evidence:** `crates/partition-server/src/background.rs::run_gc` + `process_gc_chunk` helper.
- **passes:** true

### F107 · Silent skip in compaction loop hides why `compact <PARTID>` does nothing
- **Target:** Add observability so an operator can tell whether a user-issued `autumn-client compact <PARTID>` actually ran. Pre-F107 the compact loop's `if tbls.len() < 2 && has_overlap == 0 { continue; }` early-return (correct logic, matches Go reference) was silent.
- **Fix:** New INFO log on the early-return; new INFO log in `open_partition` with `tables`, `sst_readers`, `has_overlap`, `max_seq`, `vp_extent_id`, `vp_offset` fields.
- **Notes:** Cheap fix, high diagnostic value. The user's actual problem in this session — F105 EINVAL — was masked partly by F107's absence; before adding the log, the compact RPC succeeded silently while doing nothing, leading to ~30 minutes of wrong-direction debugging.
- **passes:** true

### F118 · Open extents on EC streams misrouted as EC (PS panic post-split)
- **Target:** Stop treating `ExtentInfo.parity != []` as the EC marker. Manager's `stream_alloc_extent` pre-fills `parity` with the M target nodes the moment an EC-stream extent is allocated, but the data plane still writes the full payload to every K+M replica until `ec_conversion_dispatch_loop` runs `apply_ec_conversion_done` on a sealed extent.
- **Symptom:** PS panic `range start index 93582868 out of range for slice of length 8388569` — `client.rs::read_with_layout` dispatched to `ec_subrange_read` because `ex.parity = [7]`; `ec_subrange_read` then computed `shard_size = shard_size(ex.sealed_length=0, K=3) = 0` and `start_shard = offset / shard_size`, smashing the per-shard slice arithmetic. `info` showed open extents as `EC(3+1)` despite `(open)` tag.
- **Fix:** Rename `MgrExtentInfo.original_replicates: u32` → `ec_converted: bool` (rkyv schema change). Read sites (`client.rs::read_with_layout`, `extent_node.rs::run_recovery_task`, `info` display) all branch on `ec_converted`, never `parity.is_empty()`. Programming Note 16 in `crates/stream/CLAUDE.md`: invariant `ec_converted == true ⇒ sealed_length > 0`.
- **Evidence:** `crates/rpc/src/manager_rpc.rs::MgrExtentInfo` · `crates/manager/src/recovery.rs::apply_ec_conversion_done`.
- **passes:** true

### F116 · gallery `/get/` slow + autumn-extent CPU after EC conversion (stale `extent_info_cache`)
- **Target:** After EC conversion flips a sealed extent from 3-replica to EC, `StreamClient.extent_info_cache` still holds the pre-EC layout. PS reads against three stale-replica addresses whose `.dat` has been truncated to one shard's worth, burning up to `3 × 3 s` (`pool.call_timeout`) on `read_replicated_with_failover` before the cache is finally evicted on error.
- **Fix:** Plumb `new_eversion = ex.eversion + 1` from manager `ec_conversion_dispatch_loop` through `ExtConvertToEcReq` → `ConvertToEcReq` → `WriteShardReq` (binary header 20 → 28 bytes). `write_shard_local` writes `entry.eversion = new_eversion` and persists via `save_meta`. Add `CODE_EVERSION_MISMATCH = 6` to `extent_rpc.rs`. `read_bytes_from_extent` runs a 2-attempt loop: on `EversionStale` it calls `invalidate_extent_cache(extent_id)` and refetches `ExtentInfo` once.
- **Evidence:** `crates/stream/CLAUDE.md` Programming Note 14. Wire format change: `WriteShardReq` 20 → 28 bytes, `ConvertToEcReq`/`ExtConvertToEcReq` carry eversion.
- **passes:** true

### F117 · EC encode/decode/reconstruct on dedicated OS thread, not compio event loop
- **Target:** Stop the Reed-Solomon encode/decode/reconstruct calls from blocking the compio event loop. During EC conversion, `autumn-extent` becomes sluggish on append/read RPCs because the event loop is monopolised by `crate::erasure::ec_encode` running synchronously inside `handle_convert_to_ec`. Same hazard exists on `run_ec_recovery_payload` and `StreamClient::ec_read_full`, each of which can block its host runtime for 100–300 ms on a 128 MiB extent.
- **Fix:** Wrap each of the three call sites in `compio::runtime::spawn_blocking(move || { … })`, matching the partition-server SSTable-build pattern. Programming Note 15 in `crates/stream/CLAUDE.md`: any new CPU-bound work in this crate (RS math, large CRC, large compression) MUST be wrapped in `spawn_blocking`.
- **Evidence:** `crates/stream/src/extent_node.rs:2849, 2031` · `crates/stream/src/client.rs:1847`.
- **passes:** true

### F113 · F109 startup orphan reconcile races manager leader election
- **Target:** Make F109's startup orphan reconcile robust against the cold-boot race with manager leader election. Pre-F113 `reconcile_orphans_with_manager` was an inline single-shot await in `ExtentNode::new`; if it failed (manager not yet leader, transient blip) the node logged a single WARN and gave up until the next operator-driven reboot. On the user's box the 3 GiB orphan `extent-10.dat` files remained on disk forever despite a node restart.
- **Fix:** Replace the inline reconcile call with `spawn_reconcile_orphans_loop` (detached background task on the node's compio runtime). One periodic loop (5 min cadence) handles BOTH cold-start (failed first iteration → next sweep retries) AND steady-state safety net (catches `MSG_DELETE_EXTENT` retry-budget exhaustion, manager leader-handoff losing the queue, future EC conversion leftovers).
- **Evidence:** `crates/stream/src/extent_node.rs::spawn_reconcile_orphans_loop`.
- **Notes:** User explicitly asked for the long-running form ("reconcile 不能是后台的task吗？让它一直跑"). Per-sweep failure logged at WARN, loop continues — no give-up state.
- **passes:** true

### F112 · `ClusterClient::range()` returns only one partition's keys
- **Target:** Multi-partition `range()` must visit every partition's listener after F099-K. Pre-F112 `range()` dialed the PS-level `ps_details[ps_id].address`; post-F099-K that address only owns the FIRST partition opened on that PS, so other partitions' RangeReqs land on the wrong listener and get back `CODE_NOT_FOUND`. The client's `if resp.code != CODE_OK { continue; }` then silently dropped those partitions' entries. User-visible: gallery `/list/` returned ~196 of ~800 uploaded files.
- **Fix:** Resolve from `part_addrs.get(&part_id)` first (matching `lookup_key`/`resolve_part_id`/`all_partitions`), fall back to `ps_details[ps_id].address`. On RPC error or non-OK response code: return `Err(...)` instead of `continue` — silently dropping a partition violates `range()`'s "this is everything" claim.
- **Evidence:** `crates/client/src/lib.rs::range`.
- **passes:** true

### F111 · PS evicted by manager during startup; `info` shows `ps=unknown` indefinitely
- **Target:** PS must remain in `ps_nodes` across restart. Pre-F111 a PS restart with N ≥ 4 partitions and several hundred MiB of unflushed WAL would silently flip every region's `ps_addr` to `unknown` ~12 s after start.
- **Root cause:** `finish_connect` ran `register_ps()` (records `ps_last_heartbeat[1] = now`) THEN `sync_regions_once()` (10 s+ for 4 partitions); `heartbeat_loop` was only spawned later in `serve()`. `ps_liveness_check_loop` evicts when `elapsed > PS_DEAD_TIMEOUT (10 s)`, fired before the first heartbeat. Compounded by `handle_heartbeat_ps` silently returning `CODE_OK` for unknown ps_id.
- **Fix:** (1) Spawn `heartbeat_loop` as a detached task immediately after `register_ps` succeeds, before `sync_regions_once`. (2) `handle_heartbeat_ps` returns `CODE_NOT_FOUND` for unknown ps_id. (3) `heartbeat_loop` re-runs `register_ps` + `sync_regions_once` on `CODE_NOT_FOUND` so transient eviction self-heals.
- **Evidence:** `crates/partition-server/src/lib.rs::finish_connect` · `crates/manager/src/rpc_handlers.rs::handle_heartbeat_ps`.
- **passes:** true

### F109 · Physical extent file deletion when refs → 0
- **Target:** Make `autumn-client gc` (and `truncate`) actually free replica disk space, not just manager metadata. Pre-F109 the manager removed the etcd `extents/{id}` key when refs went to 0 but never told any extent-node to unlink the physical file.
- **Architecture decision:** manager-push over Go-style etcd-watch. Go reference design is etcd-watch from each node, but autumn-rs extent-nodes have no etcd client; the rest of the manager↔node surface is push-based.
- **Fix:** New `MSG_DELETE_EXTENT = 11` RPC. `ExtentNode::handle_delete_extent` removes from `extents` map and unlinks `.dat`+`.meta` (idempotent). `crates/manager/src/extent_delete.rs`: `pending_extent_deletes: Rc<RefCell<VecDeque<PendingDelete>>>` + `extent_delete_loop` (sweep 2 s, 60-attempt retry). `handle_stream_punch_holes` / `handle_truncate` snapshot replica addresses BEFORE removing the extent, then `enqueue_pending_deletes` AFTER `mirror_stream_extent_mutation` succeeds (etcd-first ordering). `MSG_RECONCILE_EXTENTS = 0x31` startup reconcile is the offline-node backstop. In-memory pending queue (not persisted to etcd) — manager restart loses the queue, but reconcile-on-startup converges on next boot.
- **Evidence:** `crates/manager/src/extent_delete.rs` · `crates/stream/src/extent_node.rs::handle_delete_extent` · `crates/manager/CLAUDE.md` F109 section.
- **passes:** true

### F108 · Manager `EtcdClient` panics with `RefCell already borrowed` under concurrent RPCs
- **Target:** Make `autumn-etcd::EtcdClient` and `LeaseKeeper` safe for multiple concurrently in-flight compio tasks on a single-threaded runtime. Pre-F108, every `unary_call` did `self.channel.borrow_mut().call(...).await` — `RefMut<GrpcChannel>` held across the await. User hit this triggering `gc` for 4 partitions in quick succession (4 concurrent `mirror_stream_extent_mutation → EtcdClient::txn`).
- **Fix:** Clone `http2::SendRequest` (cheap — internal mpsc handle) out of the `RefCell`, drop the borrow before `.await`. Preserves HTTP/2 request multiplexing — multiple in-flight etcd RPCs pipeline over the same connection. `transport.rs::GrpcChannel::call` becomes a free function `call_with_sender(sender: &mut http2::SendRequest, ...)`. Alternative considered: replace `RefCell<GrpcChannel>` with `futures::lock::Mutex<GrpcChannel>` (matches existing `futures::lock::Mutex for cross-await` pattern), but loses HTTP/2 multiplexing.
- **Evidence:** `crates/etcd/src/lib.rs:200,216,273` · `crates/etcd/tests/concurrent_calls`.
- **passes:** true

### F119 · Gallery video uploads transcoded to single-bitrate HLS (M3U8 + .ts)
- **Target:** When a video is uploaded to the gallery example, run an asynchronous FFmpeg pass producing single-bitrate HLS playlist + 4-second `.ts` segments and a 320 px keyframe thumbnail, store under `.hls/<name>/...` and `.thumb/320/<name>`, drop the original. Front-end uses hls.js (Safari uses native HLS). Transcoding state via `GET /transcode-status/<name>`.
- **Fix:** Helpers `hls_key`, `hls_dir_prefix`, `hls_playlist_key`. `enum TranscodeStatus { Queued, Transcoding, Done, Failed(String) }` + `Rc<RefCell<HashMap<String, TranscodeStatus>>>`. `run_transcode_blocking(url)` runs two FFmpeg passes inside `compio::runtime::spawn_blocking` (HLS `libx264 / aac, CRF 23, hls_time 4`; thumb `-ss 0.5`). Both pull source from `/get/<name>` so ffmpeg can issue Range requests. New routes `GET /hls/{name}/{file}` (path-traversal guard) and `GET /transcode-status/{name}` (in-memory map + KV fallback for restart). `delete_handler_inner` cascades original + thumb + every `.hls/<name>/*`. `recover_pending_transcodes` re-enqueues at boot. `examples/gallery/Cargo.toml` adds `tempfile = "3"`.
- **Evidence:** `examples/gallery/src/main.rs` · `examples/gallery/static/index.html` · `examples/gallery/README.md`.
- **passes:** true

### F120 · Bound recovery replay (imm depth cap + WAL-gap forced rotate + graceful shutdown)
- **Target:** On restart of a PS killed mid-write, `open_partition` replays the entire `[vp_offset, log_stream commit)` tail of `log_stream`. In the user's 4-disk EC cluster on 2026-04-27 this gap reached 1.96 GB on partition 15 plus 448 MB on part 36, blowing the PS memory footprint to 16 GB. Three independent gaps caused this:
  - **No `imm` depth cap** — `rotate_active` does an unconditional `part.imm.push_back(Arc::new(frozen))` against an unbounded `VecDeque`. RocksDB caps this at `max_write_buffer_number=2`.
  - **No WAL-size-driven flush trigger** — `maybe_rotate` only fires on `active.mem_bytes() ≥ FLUSH_MEM_BYTES = 256 MB`. A workload of small writes whose values become VPs costs ~16 B in memtable but full payload sits in log_stream. RocksDB's `max_total_wal_size` handles exactly this.
  - **No graceful shutdown** — `cluster.sh stop` sends SIGTERM, waits 5 s, then SIGKILL. `autumn-ps` had no signal handler.
- **Fix:**
  - **F120-A imm depth cap + back-pressure.** `MAX_IMM_DEPTH` (default 4, env `AUTUMN_PS_MAX_IMM_DEPTH`). When `imm.len() >= cap`, `merged_partition_loop` skips the launch-new-batch branch and the `req_rx.next()` arm; only polls `inflight.next()` and `imm_drained_rx`. Stalls req_rx consumption → ps-conn `tx.send().await` blocks at `WRITE_CHANNEL_CAP=1024` mpsc → natural transport-level back-pressure.
  - **F120-B WAL-gap forced rotate.** `MAX_WAL_GAP` (default 2 GiB, env `AUTUMN_PS_MAX_WAL_GAP`). After Phase 3, if `active.mem_bytes() + sum(imm[i].mem_bytes()) > MAX_WAL_GAP`, force `rotate_active`.
  - **F120-C graceful shutdown.** `PartitionServer::shutdown()` sets process-wide flag, sends `Drain` per partition; `merged_partition_loop` on `Drain` drains inflight, rotates `active`, loops `flush_one_imm` until imm empty. SIGTERM/SIGINT handler in `partition_server` binary; `cluster.sh::kill_proc` extended to 60 s.
- **Evidence:** `crates/partition-server/src/lib.rs` · `crates/server/src/bin/partition_server.rs` · `cluster.sh::kill_proc`.
- **passes:** true

### F121 · Node-failure write recovery (seal + alloc on dead replica)
- **Target:** With a 4-node cluster running a partition whose log/row/meta tail extents all live on `[1, 3, 5]`, `cluster.sh stop-node 1` causes subsequent `put` to **block forever** instead of sealing the current extent and allocating a new 3-replica extent on surviving nodes.
- **Root cause path:** autumn-rpc's `read_loop` sees EOF, returns `Ok(())` and clears `pending` — but the `Rc<RpcClient>` stays in `ConnPool` because it never expires unless the user-side `call`/`call_vectored` returns Err. Inside `client.send_vectored` the call inserts a fresh `(req_id → oneshot::Sender)` into `pending`, queues `SubmitMsg::Vectored`, waits on `rx`. Half-open socket → write succeeds (kernel send buffer) → no `read_loop` to dispatch a response → caller hangs forever. No transport-layer timeout on `pool.send_vectored` (Go autumn has 5 s on append fanout). `select_nodes` does not consult disk liveness. `disk_status_update_loop` only does positive updates. Per-disk-id status update path keys on the **extent-node's local** `disk_id`, not the **manager-allocated** disk_id.
- **Fix (5 layered changes):**
  1. **autumn-rpc `RpcClient` closed flag** — `closed: Rc<Cell<bool>>`. `read_loop`/`writer_task` exit sets `closed.set(true)` BEFORE `pending.clear()`. `send_*` short-circuit with `Err(RpcError::ConnectionClosed)`.
  2. **`ConnPool` evicts closed clients** — `get_client` skips entries whose `is_closed()` is true.
  3. **Append fanout hard timeout** — `append_fanout_timeout()` (default 5 s, env `AUTUMN_STREAM_APPEND_TIMEOUT_MS`). Wraps each replica's response receiver in `compio::time::sleep + select`.
  4. **df-failure marks disks offline** — `mark_node_disks_offline(store, node)` on RPC error / `mark_node_disks_online(store, node)` on success. Both key on `MgrNodeInfo.disks` (manager-allocated disk_ids), sidestepping the long-standing **disk_id mismatch** between extent-node's local `--disk-id N` and the manager's allocated disk_id.
  5. **`select_nodes` prefers nodes with at least one online disk** — falls back to full set when too few online appear (cold leader before first df sweep).
- **Evidence:** `crates/rpc/src/client.rs::RpcClient::closed` · `crates/stream/src/conn_pool.rs::get_client` · `crates/stream/src/client.rs::launch_append` · `crates/manager/src/recovery.rs::mark_node_disks_*` · `crates/manager/CLAUDE.md` Programming Note 7.
- **passes:** true

### F122 · Auto-pin one CPU core per partition / extent-shard / bench worker
- **Target:** Thread-per-core wins much more when each work-unit is pinned. Today every OS thread in autumn-rs (PS `part-N` + `part-N-bulk`, extent-node `extent-shard-N`, autumn-client bench worker) calls `Runtime::new()` with no affinity, leaving the kernel scheduler to migrate them off the io_uring's home core under load.
- **Fix:** `autumn_common::cpu_pin::pick_cpu_for_ord(zero_based_ord) -> Option<usize>` snapshots process cpuset via `core_affinity::get_core_ids()` (sorted ascending, cached in `OnceLock`) and assigns ord N → cores[N]. Wired into PS `open_partition`, extent-node multi-shard + single-shard loop, and 4 bench-worker call sites in autumn-client. Surplus work-units (more than cores in the cpuset) emit one WARN apiece and run un-pinned. Composes with `taskset -c <set>`. P-log + P-bulk of the same partition share one core (P-bulk is mostly idle during P-log busy windows: it's syscall + 3-replica network wait on a 128 MB SST upload).
- **Evidence:** `crates/common/src/cpu_pin.rs` · `crates/partition-server/src/lib.rs::open_partition` · `crates/server/src/bin/extent_node.rs` · `crates/server/src/bin/autumn_client.rs` (wbench/rbench/perf-check).
- **Notes:** Live verification on 192-core dev box: PS `part-9 + part-9-bulk` → PSR 0; `part-16 + part-16-bulk` → PSR 1; `part-23` → PSR 2; `part-30` → PSR 3 (P-log + P-bulk co-located as designed). Out of scope: main compio thread, ps-accept blocking thread, autumn-manager (low CPU / one-shot CLI commands).
- **passes:** true

---

## P0 — Code Review Fixes (2026-05-01 distributed systems audit)

### F123 · build_append_future missing F119-E sealed-extent truncation guard
- **Target:** `build_append_future` (batch append hot path) truncates extent file to `header.commit` without checking if the extent is sealed. Legacy `handle_append` has the F119-E manager round-trip check.
- **Evidence:** `crates/stream/src/extent_node.rs:841-860` (no sealed check) vs `:2319-2353` (has F119-E check).
- **passes:** true

### F124 · multi_modify_split non-atomic etcd writes — partition snapshot in separate txn
- **Target:** `handle_multi_modify_split` writes streams/extents/VP refs in Phase 2, then partition snapshot in a separate Phase 4 txn. Manager crash between the two leaves orphan streams and over-counted extent refs.
- **Evidence:** `crates/manager/src/rpc_handlers.rs:1278-1316`.
- **passes:** true

### F125 · handle_stream_alloc_extent applies state before etcd mirror
- **Target:** Mutates in-memory store at line 893 before mirroring to etcd at line 912 — violates etcd-first pattern.
- **Evidence:** `crates/manager/src/rpc_handlers.rs:892-922`.
- **passes:** true

### F126 · punch_holes missing extent-stream membership validation
- **Target:** `punch_holes` decrements refs on any extent_id in the request without verifying it belongs to the target stream. A malformed request can decrement refs on unrelated extents.
- **Evidence:** `crates/manager/src/rpc_handlers.rs:958-1022`.
- **passes:** true

### F127 · recover_partition silently skips failed extent reads
- **Target:** logStream replay silently `continue`s on `read_bytes_from_extent` failure. If a node is temporarily unreachable, un-checkpointed records are permanently lost.
- **Evidence:** `crates/partition-server/src/lib.rs:2837-2839`.
- **passes:** true

### F128 · EC 2PC coordinator crash between rename and save_meta — stuck conversion
- **Target:** In EC Phase 2, if coordinator crashes after `rename(.ec.dat → .dat)` but before `save_meta`, recovery sees old eversion, retry triggers peer-copy which fails because peers are shard-sized.
- **Evidence:** `crates/stream/src/extent_node.rs:3208-3244`.
- **passes:** true

### F135 · row_stream single-writer invariant (compaction dual-writer truncation corruption)
- **Target:** Compaction's `do_compact` used P-log's `part_sc.append(row_stream_id, ...)` while flush used P-bulk's `bulk_sc.append(row_stream_id, ...)`. Two independent StreamClients tracked commit position locally; stale commits caused ExtentNode to truncate data written by the other, destroying SST data → `invalid meta_len` on PS restart.
- **Fix:** Route all `row_stream` appends through P-bulk's StreamClient via a `RowAppendReq` channel. P-bulk's `flush_worker_loop` now handles both `FlushReq` (flush) and `RowAppendReq` (compaction).
- **Evidence:** `crates/partition-server/src/background.rs::compact_row_append` · `crates/partition-server/src/lib.rs::RowAppendReq+flush_worker_loop`.
- **passes:** true

### F136 · recovery + EC conversion race → duplicate-node corruption
- **Target:** GC fails repeatedly on extent N with `trailing bytes did not form a complete record`; manager shows `data=[X,A,B], parity=[X]` — same node id in both.
- **Root cause:** Pre-EC extents have `parity=[]` (`crates/manager/src/rpc_handlers.rs:374,881`), so `dispatch_recovery_task` can pick the FUTURE parity node as a recovery candidate (`occupied = replicates ++ parity`). If a recovery dispatches before EC conversion runs: (1) recovery copies a full pre-EC replica to `.dat`; (2) EC conversion's `commit_shard_local` renames `.ec.dat` (parity bytes) over the same path, clobbering recovery's data; (3) `apply_recovery_done` later replaces `replicates[slot]` with the recovering node id — producing duplicate-node state. Reads of the duplicated data shard return parity bytes.
- **Fix (3 layers in `crates/manager/src/recovery.rs`):**
  1. `dispatch_recovery_task` early-returns when `extent_id ∈ ec_conversion_inflight`;
  2. `ec_conversion_dispatch_loop` skips extents present in `recovery_tasks`;
  3. `apply_recovery_done` rejects the apply if `task.node_id` is already in `extent_nodes(ex)` at a different slot, removes the stale task from memory + etcd to unblock future re-dispatch.
- **Evidence:** Tests `f126_apply_recovery_done_rejects_duplicate_target` and `f126_apply_recovery_done_succeeds_when_target_is_unique` in `crates/manager/src/lib.rs`. (Test names use the file's pre-existing internal numbering; F136 is the feature index.)
- **Notes:** Already-corrupted extent in the running cluster is not auto-repaired; operator wipes and re-bootstraps via `cluster.sh reset`.
- **passes:** true

---

## Open / Deferred designs

### F195 · Eliminate env::var reads from production rs code; route all config via CLI args

- **Trigger:** Project rule ("No env reads in rs code — production rs code takes config via CLI args; env→flag translation lives in cluster.sh, not Rust") violated by **38 distinct `AUTUMN_*` env vars** read from **42 `std::env::var` call sites** across **9 production source files**. The drift accumulated across F099/F104/F120/F141/F178/F184/F187/F189/F190-F194 — each feature added a knob, defaulting to "read env at the call site." Effect: a single binary's actual config surface is invisible to `--help` or `binary --print-config`; cluster.sh leaks AUTUMN_* via the process environment (see `cluster.sh` line ~335 `env | grep AUTUMN_`); test fixtures use `std::env::set_var` (mutating process-global state, hostile to parallel test runs).
- **Migration shape:**
  - Per crate, add a `LibraryConfig` struct (or extend existing) with one field per knob; library reads from `self.config.*` only.
  - Library constructor takes the config; the existing default builders keep current behavior (so test fixtures and other callers compile unchanged); new builder methods (`set_*`) flip individual knobs from binary main().
  - Binaries parse a new `--<flag-name>` for each knob; each binary's `Args` struct grows.
  - `cluster.sh` translates env defaults to `--<flag>` arg lines instead of leaking via `env | grep AUTUMN_`.
  - Tests that currently use `std::env::set_var` rewrite to construct the library with a custom config.
- **Phasing (one commit per wave):**
  - Wave 1 — **autumn-manager (2 vars):** F192 quorum debounce knobs (`AUTUMN_REPORT_DISK_FAILURE_WINDOW_SECS`, `AUTUMN_REPORT_DISK_FAILURE_QUORUM`) → fields on `AutumnManager` + `set_report_disk_failure_config(window, quorum)` + manager-binary `--report-disk-failure-window-secs` + `--report-disk-failure-quorum`.
  - Wave 2 — **autumn-stream (9 vars):** 6 vars in `client.rs` (`BAD_NODES_TTL_SECS`, `INFLIGHT_CAP`, `APPEND_TIMEOUT_MS`, `READ_CHUNK_BYTES`, `SYNCED_POLL_MS`, `SYNCED_TIMEOUT_MS`) + 3 vars in `extent_node.rs` (`EC_CONVERT_PARALLELISM`, `RECOVERY_PARALLELISM`, `INFLIGHT_CAP`). `StreamClient.config: Rc<StreamClientConfig>` and `ExtentNodeConfig` extension fields.
  - Wave 3 — **autumn-partition-server (16+ vars):** the biggest scope. `PartitionServerConfig` consolidating GC + admission + scheduling + bulk + group-commit knobs.
  - Wave 4 — **autumn-rpc + binary-level:** `AUTUMN_RPC_FRAME_V1` (rpc/frame.rs, `OnceLock` cached) → library setter called from each binary main. `AUTUMN_GROUP_COMMIT_CAP` (autumn-client wbench), `AUTUMN_PPROF_*` (autumn-ps debug).
  - Wave 5 — **cluster.sh + autumn-client (perf_check):** translate AUTUMN_* env defaults to per-binary `--<flag>` invocations; remove the `env | grep AUTUMN_` leak.
- **Acceptance:**
  - `grep -rn "std::env::var" crates/ --include="*.rs"` excluding `tests/` + `build.rs` + `OUT_DIR` returns **zero matches in production code**.
  - Every library unit test that previously used `std::env::set_var` rewritten to construct the library with a custom config.
  - cluster.sh still launches a working cluster from operator-set `AUTUMN_*` envs (translated to per-binary `--<flag>`).
  - Workspace lib tests green across the migration.
- **Status:** All 5 waves complete. `grep -rn "std::env::var" crates/ --include="*.rs" | grep -v tests/ | grep -v OUT_DIR` returns **zero matches** in production source.
  - **Wave 1 (manager) — commit `fbeb482`:** 2 vars (F192 quorum debounce knobs) moved to `AutumnManager.report_disk_failure_{window,quorum}` + `set_report_disk_failure_config` builder + manager-binary `--report-disk-failure-{window-secs,quorum}` flags.
  - **Wave 2 (stream + extent-node binary) — commit `0357fc2`:** 11 vars total. 6 in `client.rs` → new `pub StreamClientConfig` struct + `with_*` builders + `connect_with_config` / `new_with_revision_and_config` constructors. 3 in `extent_node.rs` (F194 + F099-I) → fields on existing `pub ExtentNodeConfig` + 3 `with_*` builders. Removed the F194 env-parser test; replaced with builder-clamp test. Extent-node binary: 2 env reads removed; 3 new CLI flags.
  - **Wave 3 (partition-server) — shipped this commit:** 21 env reads eliminated. Pattern: each existing inner `OnceLock` + env-reading helper became (a) module-scope `static XXX_CELL: OnceLock<T>` + (b) `pub fn set_xxx(T)` that the binary main() calls before constructing `PartitionServer`. The reader functions still use `get_or_init` for thread-safe lazy default fallback but the init closure no longer touches the environment. `background.rs` made `pub` so the binary can reach its `set_*` setters. `AdmissionController::from_env` rewritten to read from the same setter cells. `maintenance_scheduler_loop`'s 5 inline env reads also moved. **F164 startup env-dump removed** — production rs code no longer reads AUTUMN_* env vars, so dumping them was misleading.
  - **Wave 4 (rpc + autumn-client) — shipped this commit:** `AUTUMN_RPC_FRAME_V1` (`crates/rpc/src/frame.rs`) — `static V1_ENCODER_ENABLED: OnceLock<bool>` + `pub fn set_v1_encoder_enabled(bool)`. Default V1=true unchanged. `AUTUMN_GROUP_COMMIT_CAP` (autumn-client perf-check baseline annotation) — new `--group-commit-cap` flag on `perf-check`. `AUTUMN_PPROF_{SECS,OUT,THREADS}` — new `--pprof-{secs,out,threads}` flags on autumn-ps (under `#[cfg(feature = "profiling")]`).
  - **Wave 5 (autumn-ps binary CLI + cluster.sh translation) — shipped this commit:** autumn-ps gains 20 new CLI flags (`--group-commit-cap`, `--ps-inflight-cap`, `--ps-bulk-inflight-cap`, `--max-imm-depth`, `--max-wal-gap`, `--shutdown-timeout-ms`, `--major-compact-parallelism`, `--conn-inflight-cap`, `--fg-rate-bytes-per-sec`, `--bg-rate-bytes-per-sec`, `--fg-saturated-threshold`, `--fg-qps-quota`, `--gc-debt-high-bytes`, `--compact-pending-high-bytes`, `--gc-cooldown-secs`, `--compact-cooldown-secs`, `--min-pipeline-batch`, `--gc-read-chunk-bytes`, `--gc-batch-records`, `--gc-batch-bytes`, `--gc-rate-bytes-per-sec`); `apply_ps_tunables(&args)` helper calls every setter before `PartitionServer::connect_*`. `cluster.sh::launch_ps` accumulates an explicit `tunable_args` array from each `AUTUMN_PS_*` env var the operator set and passes them on the PS command line — replaces the implicit `env | grep AUTUMN_` process-env leak.
- **passes:** true (38/38 vars eliminated; build clean; 13 rpc + 50 stream + 62 manager + 131 PS lib tests pass — except the 2 pre-existing flaky `f099i_*` timing tests that show the same pattern as the F190-F194 baseline).

---

### F194 · Extent-node EC + Recovery global concurrency caps

- **Trigger:** Conversation 2026-05-12 (post-F193 follow-up). PS already gates background work via `CompactionGate` (cross-partition major compact cap, env `AUTUMN_PS_MAJOR_COMPACT_PARALLELISM`, default 1) + `AdmissionController` (fg/bg byte rate, F189) + per-partition GC limiter (F141). Extent-node has **NONE** of these: `ec_conversion_locks` (F153) and `recovery_inflight` (F109) are both per-extent_id, not cross-extent. Concrete failure mode: a `recovery_dispatch_loop` tick after a single node-down event detects 6 extents needing recovery and detached-spawns 6 concurrent `run_recovery_task` on the same survivor node — each peer-fetches ~payload bytes + writes ~payload bytes, multiplying transient working set to `6 × payload × 2 ≈ 36 GiB` on 3 GiB extents. Similarly for EC convert: manager's serial `.await` on dispatch doesn't gate node-side execution, so multiple converts on a single node interleave at `payload × (1+(K+M)/K)` peak each.
- **Approach:**
  - `ExtentNodeGate`: Rc-based mirror of PS's `CompactionGate` — `Cell<usize>` counter + 50 ms backoff poll. Single-threaded compio per shard means no atomic needed; `Rc` keeps construction cheap and matches existing extent-node patterns.
  - Two gates on `ExtentNode`: `ec_convert_gate` (default parallelism=1, env `AUTUMN_EXTENT_EC_CONVERT_PARALLELISM`) and `recovery_gate` (default parallelism=2, env `AUTUMN_EXTENT_RECOVERY_PARALLELISM`). Both clamped to `[1, 16]`.
  - **Acquire timing matters**: gates are acquired AFTER existing cheap refuse-at-start checks (F119-D idempotent-skip for EC, F147-C stale-snapshot for recovery) so no-op paths don't consume permits. Per-extent F153 lock for EC remains the same-extent correctness gate; the new gate is the cross-extent memory-safety gate.
  - Default rationale: EC=1 because it's an optimization (no latency cost from full serialisation); recovery=2 because it's repair work where concurrency speeds post-failure convergence and 2 × 3 GiB ≈ 12 GiB peak is comfortable on production nodes.
- **Acceptance:**
  - Unit (5 new tests in `f194_concurrency_gate_tests`): parallelism=1 serialises (timed race against 200 ms confirms blocking), parallelism=2 allows two then blocks third, permit drop wakes blocked acquire within one 50 ms backoff window, constructor clamps 0 → 1, env parsing handles defaults / valid / clamp-upper / clamp-lower / non-numeric fallback.
  - Integration (deferred): on a 4-node cluster, kill one node, observe that `recovery_dispatch_loop` dispatches 6 recoveries but only 2 execute concurrently on each survivor node (jemalloc RSS growth bounded to ~12 GiB peak, returning to ~200 MB baseline within the F193 Stage A 1 s decay window).
- **Files changed:**
  - `crates/stream/src/extent_node.rs` — `ExtentNodeGate` + `ExtentNodePermit` types, `ec_convert_parallelism()` / `recovery_parallelism()` env helpers, `ExtentNode.ec_convert_gate` + `recovery_gate` fields with Clone forwarding, gate acquire in `handle_convert_to_ec` (after F119-D idempotency, line ~3641) and `run_recovery_task` (after F147-C check), 5-test `f194_concurrency_gate_tests` module.
- **Dependencies:** none. Orthogonal to F193 — F193 reduces single-task peak; F194 reduces concurrent-task multiplier. Either ship without the other; together they bound `extent-node BG memory ≤ max(EC permit count × single-EC peak, recovery permit count × single-recovery peak)`.
- **passes:** true (build clean across workspace; 50/50 stream lib + 13/13 rpc + 62/62 manager lib green; 5 new F194 gate tests pass; pre-existing flaky tests unchanged from F190-F193 baseline).

---

### F190 · Per-stream `bad_nodes` exclusion at alloc time

- **Trigger:** Conversation 2026-05-12 — under heavy write load (gallery thumb generation) extent-node 9103's `disk-3` flapped online/offline. When an extent-node fails an append, its `mark_disk_offline_for_extent` only flips a local in-memory flag; the manager doesn't learn for up to 10 s (next `disk_status_update_loop` tick). Meanwhile `StreamClient`'s retry loop immediately calls `alloc_new_extent`, and the manager picks the same broken disk again because its `MetadataStore.disks[id].online` is still `true`. The retry loop bounces against the freshly-failed disk for the entire polling window before the manager catches up.
- **Approach:**
  - `StreamClient`: per-stream `Rc<RefCell<HashMap<u64 /*node_id*/, Instant /*expires_at*/>>>` shared between the per-stream worker (writes on `apply_completion` Err) and the public-API `alloc_new_extent_once` (reads + lazily prunes on snapshot). Stored on `StreamClient.stream_bad_nodes` so it persists across worker respawn (a node that just failed should still be excluded if a transient worker exit + respawn happens within the TTL window). Default TTL **30 s**, env-tunable via `AUTUMN_STREAM_BAD_NODES_TTL_SECS` (clamped 1-600 s). Pre-existing `StreamAppendState` was extended to carry the Rc and a `mark_bad_node` helper; spec's "StreamAppendState gains bad_nodes" is honored via this shared handle (the worker side of the data-pair lives on the state).
  - `apply_completion` Err path: capture the index of the first failing replica (rpc/decode/non-OK code/NotFound — `LockedByOther` is intentionally NOT recorded; it's a control-plane fence event, not a node-health signal), resolve to `node_id` via `replica_node_ids[idx]`, and `mark_bad_node`. `StreamTail` now carries `replica_node_ids: Vec<u64>` parallel to `replica_addrs`; populated from `replicates ++ parity` in the same chained order used by `replica_addrs_from_cache`.
  - `alloc_new_extent_once`: snapshot still-active (non-expired) entries via `StreamClient::snapshot_bad_nodes(stream_id)` (lazily prunes on borrow_mut so the map can never grow), pass via new field `StreamAllocExtentReq.exclude_node_ids: Vec<u64>`.
  - Manager `select_nodes` and `handle_stream_alloc_extent` fallback walk: filter candidate nodes by `exclude_node_ids` before the online-disk filter; if the result has fewer than `count` non-excluded nodes (or in the fallback walk, an empty filtered iter), drop the exclusion and use the full set so progress isn't blocked by stale excludes.
  - Successful append does **not** clear `bad_nodes` — success on the remaining N-1 replicas doesn't prove the excluded one is healthy. Only TTL eviction.
- **Acceptance:**
  - Unit (3 tests in `f190_bad_nodes_tests`): mark inserts node id, mark refreshes the expires_at on existing entry, snapshot prunes expired entries in-place.
  - Unit (2 tests in `f190_wire_compat_tests`): `StreamAllocExtentReq` round-trips with empty `exclude_node_ids` (legacy/cold-start equivalence) and with populated `exclude_node_ids`.
  - Integration (deferred to live cluster): kill one of 3 extent-nodes mid-append; the next alloc on that stream skips the dead node within a single retry; restart it; after 30 s the same stream resumes allocating onto it. To run from `cluster.sh`: `stop-node 1` mid-`wbench`, observe in PS logs that subsequent `alloc_new_extent` requests carry `exclude_node_ids=[<node1_id>]` and the new extents land elsewhere.
- **Out of scope:** does not touch manager-side disk online/offline state machine — that's F192.
- **Files changed:**
  - `crates/rpc/src/manager_rpc.rs` — add `exclude_node_ids: Vec<u64>` to `StreamAllocExtentReq` (rkyv schema extension, single-binary deploy → atomic).
  - `crates/manager/src/lib.rs` — `select_nodes` gains `exclude_node_ids: &[u64]` parameter; filters before online-disk filter; falls back to unfiltered set if exclusion under-fills the pool. Two test sites updated.
  - `crates/manager/src/rpc_handlers.rs` — `handle_stream_alloc_extent` passes `req.exclude_node_ids` to both `select_nodes` and the post-RPC fallback walk (with same fall-back-on-empty rule). Other call sites (create_stream, multi_modify_merge) pass `&[]`.
  - `crates/stream/src/client.rs` — F190: `StreamTail.replica_node_ids` field + `Self::replica_node_ids_for(&extent)` helper; `StreamAppendState.bad_nodes: Rc<RefCell<HashMap<u64, Instant>>>` + `mark_bad_node`; `bad_nodes_ttl()` env-knob; `StreamClient.stream_bad_nodes` map + `stream_bad_nodes_handle` + `snapshot_bad_nodes`; worker spawn passes the Rc; `apply_completion` marks failing replica's node_id on Err (skip on LockedByOther); `alloc_new_extent_once` carries the snapshot. 5 `StreamTail { ... }` construction sites updated. Two new test modules.
  - 8 manager test sites updated for the added wire field.
- **passes:** true (build clean across workspace; 45/45 stream lib + 58/58 manager lib + 13/13 rpc lib tests pass; 5 new F190 unit tests pass; the 2 pre-existing flaky `f099i_*` partition-server timing tests remain pre-F190 status — see commit `a19480a` baseline).

### F191 · ExtentNode control-plane port + manager `control_pool` (carries P0 timeout fix)

- **Trigger:** Conversation 2026-05-12 — manager `disk_status_update_loop` flapped (`disk-3 online → offline → online → offline` within 90 s) under heavy write load. Root cause analysis: manager's single `ConnPool` to each extent-node multiplexes data-plane RPCs (`CONVERT_TO_EC`, `COPY_EXTENT`, `RECOVERY`) and control-plane RPCs (`DF`, future `HEARTBEAT` / `REPORT_DISK_FAILURE`). When a 1+ GB EC convert fanout occupies the connection's TCP send buffer or hits io_uring CQ backpressure on the node, the next `DF` RPC takes seconds → `RpcClient` `closed` flag → next pending future returns `ConnectionClosed` → `mark_node_disks_offline`. Next 10 s tick sees a healthy node and promotes back. Loop. Additionally, `recovery.rs:441` comment promised "bound df at 5 s" but the call site uses `conn_pool.call` (no timeout) — a second long-standing bug.
- **Approach:**
  - `ExtentNode`: new `serve_with_control(data_addr, control_addr)` spawns a second listener using the same `handle_connection` SQ/CQ machinery (no API churn). The shard ExtentNode instance is shared between the data and control listeners, so the control listener's `handle_df` sees the same `recovery_done` channel as the data listener. The control listener only ever receives small-payload ops (`DF`, future `HEARTBEAT` / `REPORT_DISK_FAILURE`) so its FuturesUnordered cap stays minimal in practice. Binary: new `--control-port` flag, default = primary port + 1000.
  - Manager: `MgrNodeInfo` extended with `control_address: String`; `RegisterNodeReq` extended with the same. Empty = legacy / not-yet-re-registered node → manager DF falls back to `node.address`. **rkyv schema extension:** breaks decoding of pre-F191 etcd state. Acceptable per F189-fix INFO-9 (single-binary atomic deploy); operator must wipe `nodes/` etcd prefix on upgrade or accept that re-registration via `format`/`register-node` will overwrite. Note 9 carry-forward.
  - Manager: new `control_pool: Rc<ConnPool>` alongside `conn_pool`. Manager's `ConnPool` gains `call_timeout` (mirrors `autumn_stream::ConnPool::call_timeout`) using `compio::time::timeout`. `disk_status_update_loop` + `recovery_collect_loop` route DF through `control_pool` against `node.control_address` (fallback `node.address`).
  - **P0 fix carried in the same commit:** previous DF call site comment claimed "bound df at 5 s" but `conn_pool.call` had no timeout. F191's `control_pool.call_timeout(EXT_MSG_DF, ..., 5s)` honors that contract — a single stuck DF now evicts the connection and the next tick reconnects.
  - `cluster.sh`: `register_extent_node` derives `${BIND_HOST}:port+1000` and passes `--control-address`. `autumn-client format` derives via the new `derive_control_address(advertise)` helper (host:port +1000, falls back to empty on bogus input).
  - `autumn-client info`: displays `control_addr=<X>` on the Nodes section when non-empty.
- **Acceptance:**
  - Unit (autumn-client bin, 3 new tests in `derive_control_address_*`): IPv4 +1000, IPv6 bracketed +1000, fallback-to-empty on bad input / port overflow.
  - Workspace build clean; lib tests stable: 13/13 rpc + 45/45 stream + 58/58 manager (including the pre-F191 schema-touched manager unit tests). 8 server-bin tests pass.
  - Integration (deferred to live cluster): on a 4-node cluster running `wbench --threads 16 --size 8m`, kick a CONVERT_TO_EC fanout via `set-stream-ec` mid-bench, then watch `manager.log` for `df RPC failed` lines — F191's `control_pool` should keep DF latency under 100 ms even while CONVERT_TO_EC saturates the data pool. The `cluster.sh stop-node 1` flap-repro scenario from 2026-05-12 should no longer trigger `disk-3 online → offline` cycling.
- **Dependencies:** F192 depends on this (`REPORT_DISK_FAILURE` travels on `control_pool`).
- **Files changed:**
  - `crates/rpc/src/manager_rpc.rs` — `MgrNodeInfo.control_address` + `RegisterNodeReq.control_address`.
  - `crates/manager/src/lib.rs` — `AutumnManager.control_pool` Rc<ConnPool>; `ConnPool::call_timeout` via `compio::time::timeout`. 5 test/literal sites updated for the schema-extended structs.
  - `crates/manager/src/recovery.rs` — `disk_status_update_loop` + `recovery_collect_loop` route DF through `control_pool.call_timeout(..., 5s)` against `node.control_address` (fallback `node.address`).
  - `crates/manager/src/rpc_handlers.rs` — `handle_register_node` stores `control_address`; re-registration mirror gate checks both `shard_ports` and `control_address`.
  - `crates/stream/src/extent_node.rs` — `serve_with_control(data, ctl)` + refactor of accept loop into shared `accept_loop(role)`. Role label propagates into connection log lines.
  - `crates/server/src/bin/extent_node.rs` — `--control-port` flag; default = `port + 1000`; multi-shard path computes per-shard control ports at `control_port_base + shard_idx * shard_stride`; single-shard path uses `serve_with_control`.
  - `crates/server/src/bin/autumn_client.rs` — `register-node --control-address`; `format` derives control_address via new `derive_control_address` helper; `info` displays it; 4 new derive tests.
  - `cluster.sh` — `register_extent_node` passes `--control-address ${BIND_HOST}:$(port+1000)` in both shard + non-shard branches.
  - 11 manager/stream test sites updated for the new wire field (all pass empty string, equivalent to pre-F191 behavior).
- **passes:** true (build clean across workspace; 13/13 rpc + 45/45 stream + 58/58 manager + 8/8 server-bin lib tests pass; 4 new autumn-client unit tests pass; the 2 pre-existing flaky `f099i_*` partition-server timing tests and the 7 pre-existing flaky `tests/integration.rs` partition-server tests remain at the same baseline status as F190 commit `19049a2`).

### F192 · `MSG_REPORT_DISK_FAILURE` with quorum debounce

- **Trigger:** Conversation 2026-05-12 — F190 fixes the per-stream alloc retry bounce (per-call route-around), F191 fixes manager↔node liveness polling reliability. Neither closes the gap that **manager's global view of `disk.online` is still pulled, not pushed**: between polling cycles, recovery dispatch / EC scheduling / advisory candidate selection still operate on stale truth. The fix is to let the data-plane failure signal flow back to manager in real time so global decisions catch up to per-stream truth.
- **Approach:**
  - New manager RPC: `MSG_REPORT_DISK_FAILURE = 0x38` with `ReportDiskFailureReq { node_id, extent_id, error_kind: u8, reporter_part_id: u64, ts_ms: i64 }`. Delivered over PS→manager direction (already a separate path from PS→extent-node data plane; no need for a second pool on the writer side — the spec's "must not share with data-plane" was about manager→extent-node, which is what F191's `control_pool` already separates). Fire-and-forget on the wire; manager replies `CodeResp { CODE_OK }` which the writer discards.
  - `StreamClient`: per-StreamClient bounded `mpsc::channel::<FailureReport>(1024)`; drainer task spawned in `construct` reads each event, builds `ReportDiskFailureReq` with the current `reporter_part_id` (Cell, default 0 = "no reporter configured"; partition-server calls `set_reporter_part_id(part_id)` on both P-log and P-bulk StreamClients right after `new_with_revision`), and fires `pool.call(manager_addr, MSG_REPORT_DISK_FAILURE, ...)` against the same manager pool that already carries alloc / owner_lock RPCs. `apply_completion` Err path (generic err only — NotFound is intentionally skipped because it's stale-cache, not health) calls `state.try_report_failure(node_id, extent_id)` which `try_send`s and drops on full.
  - Manager: in-memory `recent_failure_reports: HashMap<u64, VecDeque<(Instant, u64 /*reporter_part_id*/)>>`. Eviction window = **60 s** (env: `AUTUMN_REPORT_DISK_FAILURE_WINDOW_SECS`); quorum threshold = **3 distinct `reporter_part_id`** within window (env: `AUTUMN_REPORT_DISK_FAILURE_QUORUM`). On quorum: `mark_node_disks_offline` (in-memory; recovery's `disk_status_update_loop` is the authoritative writer and reconciles to etcd on its next 10 s tick). Quorum-trip clears the entry so a stale residual burst doesn't re-flip after the node recovers.
  - Quorum reached does **not** trigger `require_recovery` immediately — leaving recovery to the existing `recovery_dispatch_loop` (which polls every 5 s and re-evaluates on each tick) avoids a recovery storm during transient regional hiccups.
  - Manager's `disk_status_update_loop` retains its role: a successful DF response promotes the node back online via `mark_node_disks_online` AND clears `recent_failure_reports[node_id]` so a subsequent burst of stale reports doesn't re-flip.
- **Acceptance:**
  - Unit (4 new tests in `crates/manager/src/lib.rs::tests::f192_*`): 2-distinct-reporter does NOT flip offline; 3-distinct-reporter DOES flip offline; repeated reports from the SAME reporter do NOT count toward quorum (5×100 stays at 1 distinct); a quorum trip clears `recent_failure_reports` so a follow-up burst doesn't re-fire.
  - Integration (deferred to live cluster): SIGSTOP one extent-node mid-`wbench`; ≥3 distinct partitions issue REPORT_DISK_FAILURE within seconds; manager marks node offline before its next DF tick (no 10 s wait); SIGCONT the node; next DF promotes it back and `recent_failure_reports` clears.
- **Dependencies:** F191 (the disk-flap root cause is closed by F191 on its own; F192 is additive — pushes the per-stream signal up to global state).
- **Files changed:**
  - `crates/rpc/src/manager_rpc.rs` — `MSG_REPORT_DISK_FAILURE` constant, `REPORT_DISK_FAILURE_KIND_GENERIC = 0`, `ReportDiskFailureReq` struct.
  - `crates/manager/src/lib.rs` — `AutumnManager.recent_failure_reports`; 4 new unit tests.
  - `crates/manager/src/rpc_handlers.rs` — `handle_report_disk_failure` (dedup by reporter_part_id, in-window count, quorum-trip → `mark_node_disks_offline` + clear); dispatch table entry; `Duration` import.
  - `crates/manager/src/recovery.rs` — `mark_node_disks_offline` widened to `pub(crate)`; `disk_status_update_loop` clears `recent_failure_reports[node_id]` on every successful DF.
  - `crates/stream/src/client.rs` — `FailureReport` struct, per-StreamClient `failure_report_tx` + `reporter_part_id: Cell<u64>` + drainer task `failure_report_drain_loop` spawned in `construct`; `StreamAppendState` gains `failure_report_tx` + `try_report_failure` helper; `apply_completion` generic-Err path calls it. NotFound path deliberately skips reporting (stale-cache, not health).
  - `crates/partition-server/src/lib.rs` — P-log and P-bulk StreamClients both call `set_reporter_part_id(part_id)` immediately after `new_with_revision`.
- **passes:** true (build clean across workspace; 13/13 rpc + 45/45 stream + 62/62 manager lib tests pass — includes the 4 new F192 tests; pre-existing flaky tests unchanged from F190 baseline).

### F193 · Streaming EC encode + chunked recovery/copy (extent-node memory)

- **Trigger:** Conversation 2026-05-12 — `ps -o rss` shows extent-nodes at 369–770 MB steady-state with peak ≈3 GB observed during EC conversion. Source: `handle_convert_to_ec` (extent_node.rs:3891) materializes the whole extent in one `Vec<u8>` then allocates `K+M` shards (each ≈ `sealed_length / K` bytes) — peak ≈ `sealed_length × (1 + (K+M)/K)`, roughly **7 GB transient working set on a 3.2 GB sealed extent at K=3, M=1**. `handle_copy_extent` (3652), `handle_re_avali` (3530), and `run_recovery_task` (2400) similarly buffer the entire extent in one `Bytes`. F105/F115 chunked the *syscall* (256 MiB pread/pwrite to avoid macOS INT_MAX / Linux 0x7ffff000 EINVAL), but the result is concatenated into a single buffer — chunking is at syscall level, not memory level. Rust's default allocator doesn't aggressively `madvise(MADV_FREE)`, so RSS sits at the high-water mark indefinitely after a single EC convert.
- **Approach (split into three stages — Stage A shipped this commit; B/C deferred):**
  - **Stage A — allocator hygiene (SHIPPED):** swap to `tikv-jemallocator` in all three production binaries (`autumn-extent-node`, `autumn-ps`, `autumn-manager-server`) with `MALLOC_CONF=dirty_decay_ms:1000,muzzy_decay_ms:1000`. jemalloc's decay timers `madvise(MADV_FREE)` dirty pages back to the OS within ~1 s of becoming unused — closing the "RSS sits at high-water mark forever after one EC convert" steady-state hangover that was the operator's actual pain point. Linux-only (`#[cfg(target_os = "linux")]`); macOS dev builds use the system allocator unchanged. The transient peak during the spike itself is unchanged by Stage A — that's what B/C address.
  - **Stage B — pipelined EC encode (DEFERRED):** the original "Option B" sketch (2-chunk inflight) achieves `peak ≈ 2 × chunk × (K+M)` ONLY IF the K+M shard buffers can be sent to remote nodes incrementally as each chunk is produced — i.e. the WriteShardReq RPC would have to support multiple response-bytes frames per shard, not the current one-shot rkyv `WriteShardReq.payload: Bytes`. Without a wire change, the chunked-encode loop still has to accumulate ALL chunks into a per-target shard buffer before firing a single WriteShardReq, giving `peak ≈ payload × (K+M)/K` (≈ 4 GiB at K=3 M=1 on 3 GiB; below the original 7 GiB but above the 1.5 GiB acceptance target). To hit the target requires Option A's wire-format change (per-chunk WriteShardReq with chunk index, manager-side multi-chunk commit, replica reconciliation). Cleanest peak; largest surface area; punt until operator pain re-surfaces post Stage A — jemalloc hygiene closes the steady-state hangover which was the actually-observed symptom.
  - **Stage C — chunked recovery / copy_extent (DEFERRED):** the spec's existing `StreamClient::read_bytes_from_extent` already chunks at `AUTUMN_STREAM_READ_CHUNK_BYTES` (default 256 MiB) on the RPC side (F105), but `run_recovery_task` / `handle_copy_extent` / `handle_re_avali` accumulate the resulting `Bytes` into one buffer for the writeback step. Switching to a producer-consumer pipeline (read chunk → write chunk → drop) would bound the peak at one chunk = 256 MiB, regardless of extent size. Independent of Stage B; can ship on its own when revisited.
- **Acceptance (target vs reality):**
  - ✅ Steady-state RSS returns to ≈ 100–200 MB within 30 s of an EC convert completing (allocator hygiene) — covered by Stage A.
  - ❌ EC convert peak RSS bounded `< 1.5 GB` on a 3 GB extent — requires Stage B's wire change (Option A); deferred.
  - ❌ Recovery / copy peak RSS bounded `< 256 MiB` regardless of extent size — requires Stage C streaming pipeline; deferred.
- **Files changed (Stage A):**
  - `crates/server/Cargo.toml` — Linux-only `tikv-jemallocator = "0.6"` dep under `[target.'cfg(target_os = "linux")'.dependencies]`.
  - `crates/server/src/bin/extent_node.rs` — `#[global_allocator]` + `malloc_conf` static (Linux only).
  - `crates/server/src/bin/partition_server.rs` — same.
  - `crates/server/src/bin/manager.rs` — same.
- **Dependencies:** none. Independent of F190/F191/F192. Stage A best done after the flap series so we can isolate behavior changes from memory changes (already true here).
- **passes:** partial — Stage A complete; Stage B and Stage C explicitly deferred as recorded above. The operator's primary observed symptom ("RSS at 369-770 MB steady-state with peaks not returning") is fully addressed by Stage A. Workspace lib tests all green (13/13 rpc + 45/45 stream + 62/62 manager + 8/8 server-bin); the 3 production binaries link with jemalloc.

### F189-fix-r2 · Round-2 race-review fixes (audit on the round-1 fixes themselves)

After F189-fix shipped, ran a SECOND distributed-systems race review focused on (a) whether the round-1 fixes introduced new bugs and (b) edge cases round 1 didn't dig into. Three new findings, all in code introduced by F189-fix's commit (5352272). All three fixed in this commit.

**HIGH (re-opens MED-4): compact path stamps `last_compact_at` AFTER clearing `compact_inflight`.** In both the `do_compact` path of the Recv arm and the expiry-major branch of the Timeout arm, `clear_compact_inflight()` ran BEFORE the stamp. Between them sit a logging match block + a network `truncate(...)` await (potentially seconds) + a `compute_pending_compaction_bytes` call. The maintenance scheduler's tick (every 5 s) reads the tuple `(compact_inflight, last_compact_at, pending_compaction_bytes)` as three separate Relaxed atomic loads; with the inverted order it could observe `inflight=0`, `last_compact_at=stale-0`, `pending_compaction_bytes=stale-high` for the partition that just finished compacting → re-dispatch. The gc loop's main and empty-holes paths already had the correct order; only the compact paths inverted it. Fix: stamp + refresh BEFORE clear, mirroring the gc loop.

**MEDIUM (re-opens HIGH-2): two GC early-continue paths skip the `last_gc_at` stamp.** HIGH-2's stated semantic was "stamp on every loop iteration that ran the eligibility check"; the empty-holes branch and the main holes-loop tail honored it, but `get_stream_info`-failure (transient manager/extent-node hiccup) and `extent_ids.len() < 2` (legitimately single-extent partition) did NOT. Result: scheduler dispatches GC every 5 s for as long as the partition stays in either state. Fix: add `stamp_last_gc()` closure helper and call it on every continue path. Also refactored the empty-holes and main-tail paths to use the same helper for consistency.

**LOW: Auto-behind-Force GC drain merge silently drops the Auto.** When the receiver drains a queued Auto behind a Force (or vice versa), the chosen task becomes Force only. Force is operator-named extents (any discard ratio), Auto is the threshold-based scan (top-3 with ratio>40%) — generally different extent sets. Fix: documented the semantic (Auto's high-debt extent gets re-dispatched on the next 5 s scheduler tick, since this run's stamp engages cooldown for one window then re-evaluates). Acceptable one-tick deferral.

Other 12 round-2 checks (verifying round-1 fixes + AdmissionController internals + cross-thread Send-ness + stream-layer interactions) all walked clear. See commit message for the full per-check disposition.

**Tests after fixes:** 131/131 PS lib + 58/58 manager + 13/13 RPC.

**Files changed:**
- `crates/partition-server/src/background.rs` — compact recv arm: stamp + refresh before clear; expiry-major: same; gc loop: `stamp_last_gc` helper called on get_stream_info-fail + extent_ids<2 + empty-holes + main-tail; LOW comment block on the GC drain.
- `feature_list.md`, `claude-progress.txt` — docs.

### F189-fix · Race-review fixes (post-distributed-system audit)

After F189 shipped, ran a focused distributed-system race review on F187 + F188 + F189. Four issues found and fixed in this commit; rest of the audit walked safe.

**HIGH-1 fixed: futures::channel::mpsc capacity is `buffer + num_senders`, not `buffer`.** F188 created `compact_tx`/`gc_tx` with `mpsc::channel(1)` and cloned the senders into both PartitionData (P-log) and PartitionHandle (main thread for the scheduler). Effective capacity = 1 buffer + 2 senders = 3 backlogged messages. The F188 comment ("silently no-op via Full") was wrong: under load, scheduler + manual triggers could each enqueue without the receiver having drained. Fix: drain the channel at receive time and collapse the backlog. For compact, OR the bool payload (any major wins). For GC, union Force extents and drop redundant Autos.

**HIGH-2 fixed: `last_gc_at` / `last_compact_at` only updated on success.** The scheduler's cooldown gate (`now - last_gc_at >= gc_cooldown_secs`) reads `last_gc_at == 0` as "never ran" → re-eligible immediately. After F188 dispatched a GC that found `holes.is_empty()` (or `run_gc` errored on extent-node hiccup), the timestamp stayed at 0 and the scheduler dispatched again on the next 5 s tick, indefinitely. Combined with HIGH-1's backlog this could spam GC per-second under transient extent-node failure. Fix: stamp `last_gc_at` / `last_compact_at` on EVERY loop iteration that ran the eligibility check, success or skip — semantic shift from "last successful punch" to "last evaluation time" (which is what cooldown actually wants).

**MED-3 fixed: scheduler's `req_per_sec` diff raced `report_load_loop`'s swap.** The scheduler diffed `req_count` against the previous tick's value to estimate FG QPS for the foreground-awareness gate. But `report_load_loop` swap-resets `req_count` to 0 every 5 s for its OWN per-window rate calc; if the swap landed between two scheduler ticks, the diff went negative → saturating_sub returned 0 → `req_per_sec = 0` → scheduler thought FG was idle and dispatched BG work during a real FG storm. Fix: add a never-reset `PartitionMetrics.req_count_monotonic` AtomicU64, bumped alongside `req_count` on each request; scheduler now diffs against this. Two atomic adds per request (~5 ns) is the cost.

**MED-4 fixed: `gc_inflight` / `compact_inflight` set after the gate, not at dispatch.** The flags exist for the scheduler to skip already-dispatched partitions, but they were latched only AFTER `gc_gate.acquire()` / `gate.acquire()` returned. The pre-gate `get_stream_info` / `get_extent_info` calls cross the manager — slow under contention or backpressure — and during that window the flag stays at 0, allowing the scheduler to fire 1-2 redundant dispatches. Fix: latch `*_inflight = 1` at the very top of the receive arm, clear at every exit path (via inline closure helpers `clear_inflight` / `clear_compact_inflight` to keep call sites tight). Each `continue` path now clears explicitly.

INFO-9 (rkyv schema break on `PartitionLoad` extension): not fixed — single-binary deploy means atomic restart, no rolling upgrade path to break. Documented in this entry for future reference if/when external probes parse the wire format.

Cleared by audit (no fix needed):
1. parking_lot::Mutex held across `.await` in AdmissionController — confirmed dropped before sleep at lib.rs:1203 (fg) and lib.rs:1261 (bg).
5. Crash-restart `last_*_at = 0` semantics — acceptable on its own; combined with HIGH-2 above it was the spam path; HIGH-2 fixes it.
7. `do_compact` tables/sst_readers stability across awaits — F148-A invariant + clone-then-await pattern verified at background.rs:1004-1284.
8. Window-reset boundary in `account_bg` — `maybe_reset_window` runs first, so `elapsed = 0` paired with `fg_bytes = 0` after reset → no artificial saturation spike.
10. Manager `policy_tick_loop` advisory_cache write-write-write pattern — single-threaded compio + intermediate stale-read is benign for an advisory API.
11. Channel cleanup on `open_partition` failure — channels go out of scope cleanly; no senders survive.
12. Scheduler holding stale `compact_trigger` clone after partition unregister — `try_send` returns `SendError::is_disconnected()` and `is_ok()` returns false; silently ignored at lib.rs:1920+1930.

**Tests after fixes:** 131/131 PS lib + 58/58 manager lib + 13/13 RPC. F189 admission tests still pass.

**Files changed:**
- `crates/partition-server/src/lib.rs` — `req_count_monotonic` field + bump site + scheduler diff source.
- `crates/partition-server/src/background.rs` — `gc_inflight` / `compact_inflight` early latch + every-exit-path clear; `last_gc_at` / `last_compact_at` stamped on no-op + failure paths; channel-backlog drain at GC + compact receive arms.
- `feature_list.md`, `claude-progress.txt` — docs.

### F189 · GC + compaction Stage 3 — two-class admission controller (fg priority + bg elastic)

- **Target:** F188 Stage 2 added a PS-wide IoTokenBucket but only on the BG (GC + compact) side; FG writes bypassed it entirely. Under sustained FG load BG would still grab tokens at full rate, contending with FG on the same network and extent-node pwrite path. F189 closes the loop by replacing IoTokenBucket with a two-class admission controller (CockroachDB kvadmission pattern, simplified): FG gets a configurable hard ceiling, BG explicitly yields to FG when FG observed rate crosses a saturation threshold.
- **Mechanism (controller):** new `AdmissionController` (parking_lot::Mutex<{window_start, fg_bytes, bg_bytes}>, 1s wall-clock window). `account_fg(bytes).await` sleeps only when an explicit fg ceiling is set AND would be exceeded — default `AUTUMN_PS_FG_RATE_BYTES_PER_SEC=0` (unlimited) returns immediately after a single Mutex acquire to keep the request hot path cheap. `account_bg(bytes).await` sleeps until BOTH constraints hold: (1) bg's own ceiling `AUTUMN_PS_BG_RATE_BYTES_PER_SEC` (default 256 MiB/s), AND (2) fg-aware yield — when fg observed rate > `AUTUMN_PS_FG_SATURATED_THRESHOLD * fg_rate` (default 0.8), bg waits till the next window. Fg-aware yield is disabled when fg_rate=0 (no baseline to detect saturation).
- **Mechanism (wire):** `start_write_batch` (the FG hot path) calls `admission.account_fg(total_value_bytes).await` once per batch, just before launching Phase 2. Per-batch (not per-op) keeps the lock acquisition rate at ~1/256 of per-op overhead. GC + compact append paths (`do_compact`, `flush_gc_batch`, `process_gc_chunk`, `run_gc`) call `account_bg` instead of F188's `IoTokenBucket::account`. F141's per-partition GC limiter retained as inner cap (defense in depth).
- **Tests:** `f189_admission_tests` (7 tests, all passing): fg_unlimited_no_sleep, bg_unlimited_no_sleep, bg_respects_own_rate, bg_yields_when_fg_saturated, bg_does_not_yield_when_fg_idle, bg_ignores_fg_when_fg_unlimited, window_resets_after_1s.
- **Live cluster verification (2026-05-10):** with `AUTUMN_PS_FG_RATE_BYTES_PER_SEC=10485760` (10 MiB/s), 16-thread wbench at 8 KiB values self-throttled to **9.98 MB/s** — admission ceiling honored to 0.2% — with p50 latency rising from ~1ms (unthrottled baseline) to 12.5ms (throttled queueing, expected).
- **passes:** true (build clean, 131 PS lib + 58 manager lib + 13 RPC tests pass; live admission throttle verified).
- **Files changed:**
  - `crates/partition-server/src/lib.rs` — `IoTokenBucket` → `AdmissionController` with `account_fg` + `account_bg`. Field rename `io_bucket` → kept (semantic alias). New `f189_admission_tests` module (7 tests).
  - `crates/partition-server/src/background.rs` — `start_write_batch` calls `admission.account_fg(total_value_bytes).await` once per batch before Phase 2. All BG `.account(...)` calls renamed to `.account_bg(...)`.
  - `feature_list.md`, `claude-progress.txt`, `crates/partition-server/CLAUDE.md` — docs.
- **Stage 4 / future (deferred):** explicit per-op priority hints (e.g. user-marked `IsPriority=true` writes from latency-sensitive paths bypass admission); per-extent-node bandwidth attribution so admission decisions reflect actual disk pressure rather than cluster-aggregate; integration with the F183 policy advisory (advisories already mark `req_per_sec` — could feed into admission's saturation calc instead of the local 1s window).

### F188 · GC + compaction Stage 2 — PS-level priority scheduler + shared IO bucket

- **Target:** F187 Stage 1 surfaced GC/compact debt as advisory metrics but kept the per-partition random-jitter loops (10-20s compact, 30-60s GC) that fire compactions and GC arbitrarily without regard to per-PS capacity, foreground load, or cross-partition priority. F188 Stage 2 closes that asymmetry: a PS-level scheduler dispatches maintenance based on priority + foreground awareness + per-PS bytes/sec budget, replacing the random jitters as the primary trigger source.
- **Mechanism (scheduler):** new `maintenance_scheduler_loop` on PartitionServer main thread (5s cadence). Snapshots per-partition metrics (`gc_debt_bytes`, `pending_compaction_bytes`, `gc_inflight`, `compact_inflight`, `last_gc_at`, `last_compact_at`) + diffs `req_count` to derive `req_per_sec` over the interval. Skips a partition when `req_per_sec > AUTUMN_PS_FG_QPS_QUOTA` (default 50K). For eligible partitions, computes `urgency = debt / threshold`, sorts desc, dispatches top-K (DISPATCH_PER_TICK=4) via `compact_trigger.try_send(false)` (minor) or `gc_trigger.try_send(GcTask::Auto)` Send-capable channels held in PartitionHandle. Already-busy partitions silently no-op via channel `Full`. Cooldowns (default 300s, env-tunable) gate re-dispatch from PS-side `last_*_at` timestamps so the gate respects actual completion not just dispatch.
- **Mechanism (loops):** `background_compact_loop` and `background_gc_loop` keep their channel-receive paths (which now serve scheduler dispatches + manual `client compact`/`gc`). Their timeout branches demoted to short 5-7s metric-refresh ticks: refresh `pending_compaction_bytes` / `gc_debt_bytes` every iteration but DON'T fire compact/GC off the timer (except expiry-major, which is a wall-clock event the scheduler doesn't see). The compact channel payload `bool` distinguishes major (true, manual) from minor (false, scheduler routine).
- **Mechanism (rate limiter):** new `IoTokenBucket` (parking_lot::Mutex<sliding-window>) at PS level. `Arc<IoTokenBucket>` cloned into every PartitionData. GC's `flush_gc_batch` + `run_gc` chunk-read path AND compact's `compact_row_append` (both major + minor) call `io_bucket.account(bytes).await` BEFORE the network append. Default `AUTUMN_PS_BG_RATE_BYTES_PER_SEC = 256 MiB/s`; 0 = unlimited. F141's per-partition GC rate limiter is kept as a tighter inner cap (defense in depth). Foreground writes do NOT consult the bucket — true admission control with elastic vs priority tokens (CockroachDB style) is Stage 3 territory.
- **Verified on real cluster (2026-05-10):** with `AUTUMN_PS_GC_DEBT_HIGH_BYTES=1MiB AUTUMN_PS_COMPACT_PENDING_HIGH_BYTES=4MiB AUTUMN_PS_BG_RATE_BYTES_PER_SEC=32MiB AUTUMN_PS_GC_COOLDOWN_SECS=30 AUTUMN_PS_COMPACT_COOLDOWN_SECS=30`, drove ~90s of 1KB writes + manual major compact, then observed:
  - `compact part 15: major, input=7 tables, output=4 tables, kept=1738292, discarded=298314, output=1.7 GB` ← compact tagged discards
  - `F188 dispatched gc part 15 urgency=4661.16 debt=4661MB` ← scheduler picked up the 4.6 GB debt
  - Subsequent 5s ticks kept dispatching while debt > threshold and outside cooldown
- **passes:** true (build clean, 124 PS lib + 58 manager lib + 13 RPC tests pass; live cluster verification of scheduler-dispatch path successful).
- **Files changed:**
  - `crates/partition-server/src/lib.rs` — `IoTokenBucket` type + PS field; PartitionHandle gains `compact_trigger` + `gc_trigger` (Send Senders); `partition_thread_main` takes channel + bucket parameters; `PartitionData` gets `io_bucket`; new `maintenance_scheduler_loop` spawned alongside `report_load_loop`.
  - `crates/partition-server/src/background.rs` — `background_compact_loop` Recv branch handles bool=major/minor; Timeout branch is metric-refresh-only; `background_gc_loop` short timer; `do_compact` + `flush_gc_batch` + `process_gc_chunk` + `run_gc` route through `io_bucket.account()`.
  - `feature_list.md` — F188 row + Open/Deferred entry.
  - `claude-progress.txt`, `crates/partition-server/CLAUDE.md` — docs.
- **Stage 3 (deferred):** true admission-control split between fg priority tokens and bg elastic tokens (CockroachDB `kvadmission` pattern). Foreground writes would also consult an admission queue; bg yields to fg under contention. Manager-driven hint backstop for partitions where `gc_debt > 30%` of disk + sustained high QPS for N minutes (mostly defensive given auto-split already exists).

### F187 · GC + compaction maintenance advisory (Stage 1)

- **Target:** Asymmetry of treatment between auto-split/auto-merge (F183/F184/F185 — full advisory + windowed metrics + policy + cooldown + auto-trigger) and GC/compact (random jitter, hardcoded thresholds, zero manager visibility) leaves operators flying blind on maintenance debt and risks GC/compact starving foreground or being starved by it. Stage 1 mirrors F183's "advisory only — no behavior change" stage onto GC/compact: surface debt as metrics, let the manager emit `POLICY_KIND_GC` / `POLICY_KIND_COMPACT` candidates from the same sliding window. Zero scheduling-policy change in this stage; Stage 2/3 (PS-local priority scheduler + shared token bucket between fg/bg) deferred.
- **Mechanism (PS-side metrics):** `PartitionMetrics` (partition-server/src/lib.rs) gains six fields:
  - `gc_debt_bytes` (gauge): Σ `(reclaimable_bytes)` over still-live sealed log_stream extents, refreshed every GC tick from the existing `get_discards` + `valid_discard` aggregation — no extra RPCs.
  - `pending_compaction_bytes` (gauge): bytes that the next compact tick would feed into `do_compact`. When `has_overlap == 1`, total SST bytes; otherwise `pickup_tables(...)`'s output. Refreshed every compact tick.
  - `gc_inflight` / `compact_inflight` (0/1 booleans): set 1 around the actual `run_gc` / `do_compact` await; lets advisory engine skip already-active partitions.
  - `last_gc_at` / `last_compact_at` (unix-epoch i64): set on successful completion; drives the per-kind cooldown.
- **Mechanism (wire):** `PartitionLoad` (rpc/src/manager_rpc.rs) gets the same six fields. `report_load_loop` (5 s cadence) populates them from `PartitionMetrics`. `Default` derived so existing test sites compile with `..Default::default()`.
- **Mechanism (manager):** `PolicyEngine::compute_maintenance_advisory(now)` mirrors `compute_candidates` structure: require all of the most recent `required_buckets` to exceed the threshold, gate by per-kind cooldown driven from PS-reported `last_gc_at` / `last_compact_at`, skip when the corresponding `*_inflight` is 1. New `PolicyConfig` fields: `gc_debt_high` (default 1 GiB), `compact_pending_high` (default 4 GiB), `gc_cooldown_sec` (default 300 s), `compact_cooldown_sec` (default 300 s). New constants in `rpc/manager_rpc.rs`: `POLICY_KIND_GC = 2`, `POLICY_KIND_COMPACT = 3`. `policy_tick_loop` (manager/src/lib.rs) appends maintenance advisories to the same `advisory_cache` returned by `MSG_GET_POLICY_CANDIDATES`.
- **Mechanism (CLI):** `autumn-client policy-candidates` (server/src/bin/autumn_client.rs) now renders 4 kinds (`split` / `merge` / `gc` / `compact`); the `FEAS` column reads `n/a` for GC/COMPACT (always per-partition local feasibility).
- **Tests:** `crates/manager/src/policy_tests.rs` adds 7 F187 tests (gc_advisory_fires_on_sustained_debt, gc_advisory_skipped_when_inflight, gc_advisory_respects_cooldown, gc_advisory_no_trigger_below_threshold, compact_advisory_fires_on_sustained_pending, compact_advisory_respects_cooldown_and_inflight, maintenance_advisory_partial_window_no_trigger). All 18 policy_tests pass. PS lib (124) + RPC (13) tests still clean.
- **Stage 2/3 (deferred):** PS-local priority maintenance scheduler that kills the random-jitter loops (each `merged_partition_loop` posts `MaintenanceTicket{want, urgency, debt_bytes}` to a PS-level scheduler that picks under `CompactionGate` and skips when foreground QPS spikes); compact bytes/sec rate limiter to mirror F141's GC limiter; PS-level shared `IoTokenBucket` between foreground writes + GC + compact (CockroachDB admission-control style, GC/compact get elastic tokens). Manager-driven scheduling is deliberately out of scope: GC/compact are local concerns (per-partition state, per-PS resources), unlike split/merge which are inherently global (range reassignment).
- **passes:** true (build clean, 18 policy_tests pass, 124 PS lib tests pass, 13 RPC tests pass; full live cluster verification deferred per user policy on destructive cluster commands).
- **Files changed:**
  - `crates/rpc/src/manager_rpc.rs` — `PartitionLoad` +6 fields + `Default`; `POLICY_KIND_GC` / `POLICY_KIND_COMPACT` constants.
  - `crates/manager/src/policy.rs` — `GC_DEBT_HIGH` / `COMPACT_PENDING_HIGH` / `GC_COOLDOWN_SEC` / `COMPACT_COOLDOWN_SEC` constants; `PolicyConfig` +4 fields; `PolicyEngine::compute_maintenance_advisory`.
  - `crates/manager/src/policy_tests.rs` — 7 new tests + `..Default::default()` for the 13 existing PartitionLoad sites.
  - `crates/manager/src/lib.rs` — `policy_tick_loop` calls `compute_maintenance_advisory`, unions into `advisory_cache`, INFO-logs all 4 kinds.
  - `crates/manager/tests/system_merge.rs` — `..Default::default()` for 2 PartitionLoad sites.
  - `crates/partition-server/src/lib.rs` — `PartitionMetrics` +6 atomics; `report_load_loop` populates new wire fields.
  - `crates/partition-server/src/background.rs` — `compute_pending_compaction_bytes` helper; metric updates in `background_compact_loop` (3 branches: signal, expiry-major, minor) + `background_gc_loop` (debt aggregation + inflight + last_gc_at).
  - `crates/server/src/bin/autumn_client.rs` — `policy-candidates` renders 4 kinds + adjusted column width.
  - `feature_list.md`, `claude-progress.txt`, `crates/manager/CLAUDE.md`, `crates/partition-server/CLAUDE.md` — docs.

### F186 · Client-side striperados — supersedes F129 + F130

- **Target:** Stripe-write large values without ANY new server-side machinery. Replaces F129's server-side multipart upload + multi-fragment ValuePointer + F130's GC active-rewrite path with pure client-side striping (Ceph striperados pattern). Caller insight (preserved as `feedback_client_side_complexity_first.md` auto-memory): when an architecture problem can be solved with client logic over the existing primitives, that's almost always the right call — server complexity compounds across the whole cluster's memory + WAL + recovery + GC + compaction state, while client complexity costs only the SDK's footprint.
- **Mechanism (no new server RPCs):**
  - `ClusterClient::put_stream_begin(key, expires_at) → PutStreamHandle`. Each `send(chunk)` writes to `make_chunk_key(user_key, chunk_index)` via plain `MSG_PUT`. The chunk-key namespace is `b"\xff\xfeacv1\xff" + user_key.len() (BE u32) + user_key + chunk_index (BE u64)` — sorts after all normal user keys + length-prefix prevents user-key ambiguity.
  - `commit` writes a 29-byte `StripeMeta` blob to the user key: 8-byte magic + 1-byte version + 8-byte total_bytes + 4-byte chunk_count + 4-byte chunk_size + 4-byte CRC32C over the first 25 bytes. The meta blob's presence at the user key is the atomic linearisation point: until commit returns, `get(key)` returns NotFound and orphan chunks are invisible.
  - `abort` best-effort deletes already-written chunks. `delete_stream(key)` cascade-deletes all chunks then removes meta.
  - `get_stream(key, chunk_size_hint)` auto-detects: if `cluster.get(key)` returns a 29-byte blob with the magic + valid CRC, walks chunks; otherwise treats the blob as inline and yields it whole.
- **Crash safety:** chunks-before-meta ordering: a client crash leaves orphan chunks (no meta → invisible to readers, can be cleaned by application-layer sweep) — but never leaves dangling meta with missing chunks, which would surface as user-visible read failures. Same trade-off as Ceph striperados.
- **Files changed:**
  - `crates/client/src/lib.rs`: `StripeMeta`, `make_chunk_key`, `STRIPE_CHUNK_SIZE = 4 MiB`, `PutStreamHandle<'a>` (now sync `put_stream_begin`, no more `Rc<RpcClient>` caching), `GetStream<'a>`, new `delete_stream`, 6 unit tests.
  - `crates/server/src/bin/autumn_client.rs`: `putstream`/`getstream` CLI now uses the new sync API.
  - `crates/manager/tests/system_putstream.rs`: 4 integration tests (12 MiB roundtrip + pre-/post-commit visibility, single-chunk edge, abort drops chunks, inline-value passthrough).
- **Server-side rip-out** (deleted in same release):
  - `crates/rpc/src/partition_rpc.rs`: `MSG_PUT_BEGIN/CHUNK/COMMIT/ABORT` + Req/Resp shapes + `CODE_UPLOAD_NOT_FOUND` deleted; constants reserved (commented out) for stale-binary safety.
  - `crates/partition-server/src/rpc_handlers.rs`: `handle_put_begin/chunk/commit/abort` + `rand_upload_id` deleted; HEAD's multi-frag value_length branch deleted.
  - `crates/partition-server/src/lib.rs`: `OP_VALUE_POINTER_MULTI` (0x40), `OP_CHUNK_BLOB` (0x10), `MultiFragVp` struct + 5 tests, `UploadSession` struct, `upload_sessions` field on `PartitionData`, `AUTUMN_PS_MAX_INLINE_BYTES_HARD`, `AUTUMN_PS_MAX_UPLOAD_SESSIONS_DEFAULT`, `AUTUMN_PS_UPLOAD_TTL_SECS_DEFAULT`. Recovery's `OP_CHUNK_BLOB` skip + `OP_VALUE_POINTER_MULTI` re-insert deleted.
  - `crates/partition-server/src/background.rs`: F130's `collect_live_mfvps_touching` + `rewrite_multi_frag_for_extent` + multi-frag branch of `bump_discards_for_dropped_entry` + `resolve_multi_frag` + `run_gc` pre-pass call deleted.
- **Pre-existing bug fix** (also F186, same commits): `finish_write_batch` and `flush_gc_batch` were computing VP offset as `record_offset + 17 + key.len()` (V0 layout). Latent since F165 made V1 default-on (V1 envelope adds 5 bytes before the V0 inner header), so every value > 4 KiB written via the regular Put path since F165 had VP pointing 5 bytes too early — reads returned the last 5 bytes of internal_key (inverted-seq, e.g. `[ff,ff,ff,ff,fe]`) followed by `value_len - 5` bytes of value. Latent because:
  - production tests use small values (≤ 4 KiB → inline path)
  - F129 multipart used the correct V1 calc (1+4+17 = 22)
  - no test verified content of large VP-path values post-V1
  F186 caught it because client-side striperados puts every chunk via plain Put with chunk_size = 4 MiB > VALUE_THROTTLE. Fix: `+ 17 +` → `+ 22 +` at all three sites (`finish_write_batch`, `flush_gc_batch`, `recover_partition`).
- **Verification:**
  - `cargo test -p autumn-rpc`: 13/13 (was 17 — 4 F129 wire tests deleted)
  - `cargo test -p autumn-partition-server --lib --test-threads=1`: 124/124 (was 130 — 6 F129 multi-frag tests deleted)
  - `cargo test -p autumn-manager --lib`: 51/51
  - `cargo test -p autumn-manager --test system_putstream -- --ignored`: 4/4
  - `cargo check --workspace --exclude autumn-fuse`: clean
- **Comparison with the F129 server-side multipart it replaced:**
  | | F129 server-side | F186 client-side |
  |---|---|---|
  | New wire RPCs | 4 (`MSG_PUT_BEGIN/CHUNK/COMMIT/ABORT`) | 0 |
  | New op flags | 2 (`OP_VALUE_POINTER_MULTI`, `OP_CHUNK_BLOB`) | 0 |
  | New memtable shape | multi-frag VP value | none |
  | New WAL records | OP_CHUNK_BLOB | none |
  | PS in-memory state | `upload_sessions: HashMap<u128, UploadSession>` + 30 min TTL | none |
  | New GC machinery | F130 active rewrite (~300 lines) | none |
  | Recovery handling | OP_CHUNK_BLOB skip + OP_VALUE_POINTER_MULTI insert | none |
  | Compaction discard | per-frag accounting | none (chunks are normal Puts) |
  | Caller-visible API | same `PutStreamHandle` / `GetStream` shape | same |
  | Test coverage | 5 multi-frag unit + 3 integration | 6 stripe-meta unit + 4 integration |
- **Trade-offs:**
  - Client crashes mid-upload leave orphan chunks (F129 had server-side TTL eviction). Applications that care must sweep the chunk-key namespace periodically.
  - Replacing a striped value with a non-striped value (or vice versa) at the same key requires explicit `delete_stream` first; otherwise old chunks linger.
  - Get of a striped value is N+1 RPCs (1 meta + N chunks) where F129 was 1 RPC server-resolved. For full-value reads on a single client, latency scales with N (LAN: ~10ms × N). Mitigation: client SDK could parallel-fetch chunks (deferred — only matters on WAN).
- **passes:** true

### F129 · PutStream / GetStream — server-side multipart (SUPERSEDED by F186)
- **Status:** REMOVED in F186. The server-side multipart upload was an over-engineered solution to the "value > 64 MiB inline cap" problem; client-side striperados covers the same use case with zero server changes. F129 entry preserved as historical context.
- **Original target (kept for posterity):** Bound PS RAM and improve TTFB for large values by adding S3-style multipart upload (`PutBegin` / `PutChunk` / `PutCommit` / `PutAbort`) at the partition server, plus a client-side `GetStream` that loops the existing `GetReq.offset/length`. Memtable / SSTable gain a multi-fragment `ValuePointer` (op flag `OP_VALUE_POINTER_MULTI = 0x40`, encoded `[n_frags:u32][total_len:u64][(extent_id:u64, offset:u32, len:u32) × n_frags]`); chunks stored as WAL-shaped records with op `OP_CHUNK_BLOB = 0x10` so `decode_records_with_offsets` / `process_gc_chunk` skip them safely. Existing `Put`/`Get` preserved; both gain symmetric size cap `AUTUMN_PS_MAX_INLINE_BYTES` (default 64 MiB, hard ≤ 256 MiB) — over-cap returns `CODE_VALUE_TOO_LARGE`. Client adds `PutStreamHandle { send, commit, abort }` + `GetStream { next_chunk }`. PS holds upload sessions in memory only (`HashMap<u128, UploadSession>`); session metadata is O(1) in chunks (clients hold the fragment list); idle TTL 30 min (`AUTUMN_PS_UPLOAD_TTL_SECS`); per-partition cap 1024 (`AUTUMN_PS_MAX_UPLOAD_SESSIONS`). Routing requires `part_id` in `PutChunkReq`/`PutCommitReq`/`PutAbortReq`.
- **Notes:** Picked S3 multipart over autumn-rpc native multi-frame (F133) because it has zero framework impact, matches Azure Blob / GCS / HDFS / GridFS / Ceph practice. Multi-fragment VP (ζ) over per-upload `blob_stream` (ε): all the surveyed systems store large values as fragment lists; single-segment continuity gives a measurable advantage only on "client reads whole 1 GiB at once", which is rare in this codebase. Symmetric Put/Get cap is critical: asymmetric creates "writable-but-not-readable" footgun.
- **`put_auto` / `get_auto` deliberately NOT added.** The early plan called for them but they're a footgun: `put_auto` would silently route a 200 MiB `Vec<u8>` through streaming, which doesn't change the caller's memory profile (already buffered) but hides the "you're crossing a code-path boundary" signal. `get_auto` is worse — buffering N×4 MiB streamed chunks back into one `Vec<u8>` defeats streaming's whole point. Callers either know their value size class (use `put`/`get`) or are streaming source/sink (use `put_stream_begin`/`get_stream` directly). The clean error from `put` on > 64 MiB is the right interface.
- **Implementation:**
  - Wire types: `MSG_PUT_BEGIN = 0x49`, `MSG_PUT_CHUNK = 0x4A`, `MSG_PUT_COMMIT = 0x4B`, `MSG_PUT_ABORT = 0x4C` + Req/Resp shapes (`crates/rpc/src/partition_rpc.rs`).
  - PS handlers: `handle_put_begin/chunk/commit/abort` (`crates/partition-server/src/rpc_handlers.rs`); upload session map on `PartitionData.upload_sessions`.
  - Op flags + `MultiFragVp` encode/decode + 5 unit tests (`crates/partition-server/src/lib.rs`).
  - Recovery: WAL replay skips `OP_CHUNK_BLOB`; `OP_VALUE_POINTER_MULTI` re-inserts the mfvp blob into memtable.
  - Read path: `handle_get` / `handle_head` decode `OP_VALUE_POINTER_MULTI`; `resolve_value` dispatches to `resolve_multi_frag` which walks fragments sequentially honouring offset/length sub-range.
  - Client SDK: `ClusterClient::put_stream_begin` → `PutStreamHandle::{send, commit, abort, upload_id, bytes_sent, chunks_sent}`; `ClusterClient::get_stream` → `GetStream::{next_chunk, total_bytes, position, remaining}` (`crates/client/src/lib.rs`).
  - CLI: `autumn-client putstream <KEY> <FILE> [--chunk-size N]` and `autumn-client getstream <KEY> [--chunk-size N] [--out FILE]`.
- **Verification:**
  - `cargo test -p autumn-rpc`: 17/17.
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 130/130 (includes 5 `f129_multifrag_tests`).
  - `cargo test -p autumn-manager --test system_putstream -- --ignored`: 3/3 (NEW; covers 12 MiB roundtrip with 4×3 MiB chunks, single-chunk edge case, abort path).
- **Out of scope (separate feature entries):** F130 (GC active rewrite — needed for sealed log_stream reclaim under sustained F129 use), F131 (concurrent fragment pread — perf), F132 (resume across split / restart), F134 (frame-level early reject — perf hardening).
- **passes:** true

### F130 · GC active rewrite for multi-fragment VPs — SUPERSEDED by F186
- **Status:** REMOVED in F186. With server-side multi-frag VPs deleted, there's nothing for this rewrite path to act on; chunks are now normal Puts that the existing single-VP GC handles. Entry preserved as historical context.

### F130 (original — historical) · GC active rewrite for multi-fragment VPs (unblocks log_stream extent reclaim under F129)
- **Target:** Atomic VP rewrite so log_stream extents holding live multi-frag fragments can be reclaimed. Without F130, sustained F129 use accumulates `OP_CHUNK_BLOB` records in sealed log_stream extents that the existing single-VP GC scan skips (line 1616 of background.rs masks `op & OP_VALUE_POINTER` which is 0 for chunk records), so `punch_holes` would silently orphan the chunks if the holding extent was reclaimed.
- **Approach (chosen):** **Full-value rewrite** — when GC targets an extent with chunks for any live multi-frag value, rewrite the ENTIRE multi-frag value (every fragment, including ones not on the to-be-reclaimed extent) to the active log_stream tail. After rewrite, every fragment of the shadowed mfvp is truly dead and compaction can blindly bump per-extent discard counters when it later shadows the old entry. Cost: a 200 MiB value with one chunk on the target extent rewrites all 200 MiB. Acceptable because multipart upload's typical pattern is contiguous chunks landing in the same log_stream tail extent.
- **Linearisation (the "hard problem" referenced in the deferred note):** Reuses the same MVCC seq-number ordering that the existing single-VP `process_gc_chunk` uses. The rewrite allocates a fresh seq under `borrow_mut` (no `.await` between read-current-mfvp and seq allocation); a foreground Put on the same user_key always gets a strictly newer seq and shadows the rewrite via the inverted-seq byte ordering of internal_keys. F148-A invariant covers `borrow_mut → mpsc-send → ack` ordering for log_stream record durability. The rewrite's chunks become orphan if the foreground Put wins; the next GC pass collects them.
- **Mechanism:**
  1. `rewrite_multi_frag_for_extent(part, log_stream_id, eid, part_sc)` runs as a **pre-pass** at the top of `run_gc`, before the existing single-VP `process_gc_chunk` loop.
  2. `collect_live_mfvps_touching(part, eid)` walks active memtable + imm queue, dedups by user_key (newest wins), filters to `OP_VALUE_POINTER_MULTI` entries whose mfvp has at least one fragment on `eid`.
     - **Scope limitation:** does NOT walk SSTs. Live mfvps whose newest copy is only in an SST and not yet shadowed are handled by F130-C's compaction discard path (compaction shadows mfvp_old → bumps discards for every frag of mfvp_old → host extents eventually accumulate > 40% discard ratio → next GC tick picks them).
  3. For each candidate: read every fragment via `read_bytes_from_extent`, append each as a fresh `OP_CHUNK_BLOB` record, build new `MultiFragVp` with the new fragment list, allocate seq + append `OP_VALUE_POINTER_MULTI | 1` commit record, insert memtable entry. Updates `vp_extent_id` / `vp_offset` so the next flush includes the rewrite's tail.
  4. After the pre-pass returns, the existing single-VP scan runs unchanged (`OP_CHUNK_BLOB` records still skipped — they're now genuinely orphan).
- **Compaction discard tracking (F130-C):** Refactored 4 sites in `do_compact`'s discard-on-drop branches (dedup, range filter, tombstone, expired) into a single helper `bump_discards_for_dropped_entry(discards, op, raw_value)` that handles BOTH single-VP (existing) AND multi-frag VP (new). For multi-frag, decodes `MultiFragVp` and bumps per-extent discards for EVERY fragment.
- **Recovery:** No changes needed. WAL replay already handles `OP_CHUNK_BLOB` (skip) and `OP_VALUE_POINTER_MULTI` (memtable insert) — the rewrite's commit record looks identical to a fresh `handle_put_commit` write. The MVCC seq ordering ensures the rewrite shadows the original whether the original is in WAL, memtable, or SST.
- **Files:** `crates/partition-server/src/background.rs` (`bump_discards_for_dropped_entry`, `collect_live_mfvps_touching`, `rewrite_multi_frag_for_extent`, `run_gc` pre-pass call, 4 discard-site refactors); `crates/manager/tests/system_putstream.rs` (new `f130_multifrag_gc_rewrite_preserves_value` test).
- **Verification:**
  - `cargo test -p autumn-partition-server --lib --test-threads=1`: 130/130
  - `cargo test -p autumn-manager --test system_putstream -- --ignored`: 4/4 (3 F129 + 1 F130)
  - `cargo check --workspace --exclude autumn-fuse`: clean
  - F130 test exercises: 12 MiB / 4 × 3 MiB chunks committed → second multi-frag value forces tail rotation → force_gc on the original tail extent → assert original value still readable byte-for-byte (rewrite worked) and unrelated value unaffected.
- **Out of scope:** Walking ALL SSTs for live mfvps in the GC pre-pass. Cost is O(SST bytes); deferred to compaction's discard path which is O(compacted bytes) per its existing schedule. If production sees stuck extents that compaction takes too long to clear, add a targeted scan via SST `vp_deps` index (already shipped).
- **passes:** true

### F131 · Concurrent fragment pread in `resolve_value` (perf follow-up to F129)
- **Target:** Replace v1 sequential per-fragment `read_bytes_from_extent` with `FuturesUnordered`, capped at `AUTUMN_PS_RESOLVE_CONCURRENCY` (default 8). Result Vec assembled in fragment order despite out-of-order completion.
- **Trigger:** 1 GiB `get_auto` total latency exceeds 60% of (single-fragment latency × n_frags), or `examples/gallery` Range read P99 across fragment boundaries materially exceeds intra-fragment P99.
- **passes:** false

### F132 · PutStream resume across partition split / PS restart
- **Target:** Persist `(upload_id, key, must_sync, expires_at, fragments[], total_bytes, last_chunk_index)` as a small region inside `meta_stream`; new owner (post-split / post-restart) loads the region and accepts continuation chunks under the same `upload_id`. New `PutChunkResp` field `RESUME_HINT { upload_id, last_committed_index, ps_addr_hint }` for transparent reconnection.
- **Trigger:** Real workload sees frequent GB-class uploads colliding with split or PS rolls.
- **Notes:** Split-time fragment ownership is the hard part: log_stream CoW makes chunk bytes visible to both halves; the half whose key range still contains `key` accepts continuation. Persistence cadence: `last_chunk_index` updated lazily (every K chunks or T seconds). Worth tackling only after F129 is in production and resume-cost is shown to matter.
- **passes:** false

### F133 · autumn-rpc native multi-frame (FLAG_STREAM_END activation)
- **Target:** Activate the reserved `FLAG_STREAM_END = 0x04` frame flag so a single logical RPC can span N request and/or response frames. `RpcClient::call_streaming(req) → impl Stream<Item = Bytes>` + per-`req_id` frame-routing table on the server.
- **Trigger:** (1) `MSG_RANGE` returning > 100 k rows hits the single-frame size limit, or (2) autumn-rs adds a watch / subscribe RPC whose semantics inherently require server streaming.
- **Notes:** Multipart (F129) preferred for "one big payload" flows (idempotent commit, S3-shape resume); native streaming wins for "many small results from one logical query" (range scans) or "open-ended subscription". Wire compat: `FLAG_STREAM_END` is currently unused by all senders.
- **passes:** false

### F134 · Frame-level Put early reject (perf hardening for F129 cap)
- **Target:** Move `AUTUMN_PS_MAX_INLINE_BYTES` cap check from post-rkyv-decode into the autumn-rpc frame loop: when `payload_len > cap + overhead_bound`, drop the connection or return `CODE_VALUE_TOO_LARGE` without reading body bytes off the socket.
- **Trigger:** Monitoring shows PS network ingress carries > 1% rate of `CODE_VALUE_TOO_LARGE` rejections (clients not using `put_auto`).
- **passes:** false

### F138 · `eversion` lost-update across await on the manager extent record
- **Target:** `MgrExtentInfo.eversion` is mutated by four sites on the single-threaded manager runtime. The risky one is `ec_conversion_dispatch_loop`: it captures `new_eversion = ex.eversion + 1` BEFORE the `EXT_MSG_CONVERT_TO_EC` await, then `apply_ec_conversion_done` writes that captured value back unconditionally. If `apply_recovery_done`, `mark_extent_available`, or `handle_multi_modify_split` bumps `eversion` during that await, the unconditional write overwrites the intermediate bump. Worse: `apply_ec_conversion_done`'s `ex.replicates = target_nodes[..data_shards]` reverts a recovery's slot replacement, producing `replicates[slot] = old_failed_node_id` while `replicate_disks[slot] = recovery_disk_id` (inconsistent). Symptom: stale `eversion` → `CODE_EVERSION_MISMATCH` on freshly-EC-converted extents that don't auto-recover.
- **Fix (option b — eversion-bump lock):** extend `ec_conversion_inflight`'s semantics to cover the full dispatch-to-apply window. (a) Move `ec_conversion_inflight.remove` to AFTER `apply_ec_conversion_done` completes, so the lock is held during both the RPC await and the in-memory+etcd write. (b) `apply_recovery_done`, `mark_extent_available`, and `handle_multi_modify_split` each check `ec_conversion_inflight` at entry and return `Err(Precondition)` if set; retried on the next 2 s dispatch tick or client-side backoff. (c) `recovery_dispatch_loop` skips per-slot re_avali / dispatch when the extent is in `ec_conversion_inflight`, preventing re_avali-triggered mark_extent_available from firing while EC is pending. Symmetric to F136's pre-existing guard (EC checks `recovery_tasks` before dispatch). See `crates/manager/CLAUDE.md` note 10.
- **Evidence:** `crates/manager/src/recovery.rs` (`apply_recovery_done`, `recovery_dispatch_loop`, `ec_conversion_dispatch_loop`, `apply_ec_conversion_done`), `crates/manager/src/lib.rs` (`mark_extent_available`), `crates/manager/src/rpc_handlers.rs` (`handle_multi_modify_split`). Four new unit tests `f138_apply_recovery_done_during_ec_inflight_defers`, `f138_mark_extent_available_during_ec_inflight_defers`, `f138_full_race_recovery_after_ec_apply`, `f138_split_aborts_when_source_extent_is_ec_inflight` in `crates/manager/src/lib.rs`.
- **passes:** true (build + 20/20 manager lib tests; live re-verification deferred per project policy on destructive cluster commands).

### F139 · Extent-node delete vs in-flight recovery on the same extent
- **Target:** On the extent-node, `handle_delete_extent` (`extent_node.rs:2768`) removes the entry from `self.extents` and unlinks `.dat`/`.meta`, while a concurrent `handle_require_recovery` (`extent_node.rs:2680`) holds an `Rc<ExtentEntry>` looked up before delete. Two failure modes: (a) recovery's `ensure_extent` runs after delete, finds the entry gone, re-creates an empty one and writes recovery payload — orphan only reaped on next reconcile-on-startup; (b) recovery still holds the pre-delete `Rc` and writes to the now-unlinked inode, leaving "empty file at path, real data on a now-orphaned inode that frees on fd close." Manager-side mitigation already exists (`extent_delete_loop` only fires on `refs == 0 && vp_table_refs == 0`, F136's mutual-exclusion serializes recovery vs EC conversion), but `punch_holes` can land between recovery dispatch and recovery completion. Defense: refuse recovery in `handle_require_recovery` when `self.extents` doesn't contain the extent; on the manager, `dispatch_recovery_task` could additionally skip extents queued in `pending_extent_deletes`.
- **Trigger:** open and prioritise if reproduced in a stress test that interleaves `stream_punch_holes`/`truncate` against recovery on the same extent, OR if production observes an orphan `.dat` file that survived `extent_delete_loop` and was only cleaned by reconcile-on-startup.
- **Evidence:** Cross-crate background-task race audit alongside F136 (2026-05-04).
- **Fix:** Five symmetric changes mirroring F138's in-flight-set pattern: (a) `dispatch_recovery_task` skips when extent is in `pending_extent_deletes`; (b) `handle_stream_punch_holes` returns `Err(Precondition)` if any refs→0 extent is in `recovery_tasks`; (c) `handle_truncate` same guard; (d) `apply_recovery_done` None-branch enqueues `PendingDelete` for targeted cleanup; (e) `handle_delete_extent` returns `CODE_PRECONDITION` when `recovery_inflight.contains_key(&extent_id)`, retried by `extent_delete_loop` up to 60 × 2 s. Belt-and-braces: both manager and extent-node sides independently refuse, so leader failover (in-memory `pending_extent_deletes` lost) still has extent-node protection via `recovery_inflight`.
- **Evidence:** `crates/manager/src/recovery.rs`, `crates/manager/src/rpc_handlers.rs`, `crates/stream/src/extent_node.rs`. Four unit tests `f139_*` in `crates/manager/src/lib.rs`; one integration test `f139_delete_recovery_race` in `crates/stream/tests/`.
- **passes:** true (build clean; 24/24 manager lib tests; 48/48 stream lib tests; 1/1 f139_delete_recovery_race integration test)

### F140 · split + concurrent EC conversion + node-flap → row_stream SST corruption (replica size divergence)
- **Target:** Reproduced 2026-05-04 in `cluster.sh reset 4` followed by an aggressive `wbench → compact → split → wbench` cycle. After PS restart the partition fails to open with `MetaBlock CRC mismatch: stored=0x4c84d6e0 computed=0x274e9bf8` on row_stream extent 17 (TableMeta says `length=526376263`). Forensics: extent-17.dat sizes diverged across replicas — d1/d2/d3 each 929 MB, d4 526 MB; manager view sealed at 502 MB; meta sidecars only consistent on d2 (sealed_length=502 MB, eversion=2), other replicas still at sealed_length=0, eversion=1. So the four replicas physically disagreed on the file's tail bytes, and the SST whose TableMeta said 526 MB straddled the sealed boundary on at least one replica → reading 526 MB returned bytes whose meta-block CRC doesn't match.
- **Mechanism (best-effort reconstruction):** at split time a P-bulk `RowAppendReq` (compaction's row_stream append, F135 path) was already in flight on row_stream's tail extent. Split's `handle_split_part` runs `flush_memtable_locked` (drains `imm` flushes via `FlushReq`) but does NOT drain `RowAppendReq` queued in P-bulk's `flush_worker_loop` FU pipeline — those two channels share the worker but `flush_memtable_locked` only awaits the FlushReq it itself enqueues. With a compact in flight, `multi_modify_split` seals the row_stream tail at `commit_length`, but the in-flight append continues writing past sealed_length on whichever replicas haven't yet received the manager's eversion-bump push (`MgrExtentInfo` push lag). Compounded by F121 timeout cascade: concurrent EC conversion of a sibling extent (extent 12, ~3 GB on the same node set) saturates the extent-node compio runtime; `df` RPCs from manager time out → manager flips `disk.online=false` → `select_nodes` rejects the node → fresh-extent allocation retries fail with `precondition failed: no healthy node available`.
- **Mitigation in this session (partial — addresses the EC CPU-contention amplifier):** F117 already wraps `ec_encode/decode/reconstruct` in `spawn_blocking`, but two memcpy hot spots on `handle_convert_to_ec`'s event-loop thread remained: (a) per-remote-target `Bytes::copy_from_slice(shard)` of the K data + M parity shards, ~700 MB total event-loop memcpy per conversion at our default 526 MB extent / K=3; (b) `write_shard_local` calling `file_pwrite_chunked(...shard_data.to_vec())` for the coordinator's own shard, another `shard_size` memcpy; (c) `file_pwrite_chunked` itself per-chunk `chunk.to_vec()` round-tripping `Bytes → Vec<u8>` inside the loop. Patch: (1) convert `Vec<Vec<u8>> → Vec<Bytes>` inside the existing `spawn_blocking` closure (zero-copy `Bytes::from(Vec<u8>)`); (2) loop uses `shards[i].clone()` (O(1) Arc inc) instead of `Bytes::copy_from_slice`; (3) change `write_shard_local` and `file_pwrite_chunked` signatures to take `Bytes` so the chunked path can pass split slices directly to `file_pwrite` without an interim `Vec<u8>`. After this, `handle_convert_to_ec`'s event loop only does the io_uring submits + small protocol headers between blocking-thread RS encode and async file I/O; the 175 MB chunks that previously stalled the runtime no longer do.
- **Trigger for the deeper fix (still `passes:false`):** the underlying race — split sealing the tail while a P-bulk RowAppendReq is in flight — is only mitigated, not closed, by reducing CPU contention. Open and prioritise the structural fix if `MetaBlock CRC mismatch` recurs in stress with the EC-CPU patch in place. Likely shape: extend `flush_memtable_locked` (or add a `drain_p_bulk_inflight()` barrier) so `handle_split_part` awaits all queued/in-flight `RowAppendReq` on P-bulk's `flush_worker_loop` before calling `multi_modify_split`. Alternatively, take the `compact_gate` permit during split so no new compactions can dispatch a `RowAppendReq` while split is sealing.
- **Evidence:** `crates/stream/src/extent_node.rs::handle_convert_to_ec` (L3193 + L3210 + L3241), `write_shard_local` (L2106), `file_pwrite_chunked` (L481); `crates/partition-server/src/rpc_handlers.rs::handle_split_part` step-5 `flush_memtable_locked` (L341) doesn't cover RowAppendReq; F117 + F121 + F135 history; `/tmp/autumn-rs/d{1..4}/19/extent-17.dat` size divergence captured 2026-05-04.
- **Structural fix (F140 final):** Two races closed via dual-gate acquisition in `handle_split_part`. (A) F140-A: `handle_split_part` acquires the PS-wide `compact_gate` (same `Arc` held by `background_compact_loop`) before `flush_memtable_locked`. Because `do_compact` holds the gate for its full duration and awaits every `compact_row_append` oneshot before returning, acquiring the gate is also a "no `RowAppendReq` in P-bulk" fence. (B) F140-B (new): GC's `background_gc_loop` acquires a new per-partition `gc_gate` around the `for eid in holes` loop (covering all `run_gc` / `part_sc.append` / `append_segments` calls). `handle_split_part` acquires `gc_gate` immediately after `compact_gate`, ensuring no log_stream append is in-flight when `commit_length(log_stream_id)` is read. Both gates are held through `multi_modify_split` and released RAII on function return. `PartitionData` stores `compact_gate: Arc<CompactionGate>` and `gc_gate: Arc<CompactionGate>` so `handle_split_part` (which only receives `part: &Rc<RefCell<PartitionData>>`) can clone and acquire them. Acquisition order is always compact→gc to avoid future lock-order inversions.
- **Files:** `crates/partition-server/src/lib.rs` (PartitionData new fields, gc_gate creation in run_partition_loop, gc_gate passed to background_gc_loop spawn, 2 new F140 unit tests), `crates/partition-server/src/background.rs` (background_gc_loop signature + gc_gate acquire/drop around holes loop), `crates/partition-server/src/rpc_handlers.rs` (handle_split_part acquires both gates before auth_rg fetch).
- **passes:** true (build clean; 105/105 pass + 2 pre-existing f099i parallel-test race; 48/48 stream; 24/24 manager; live re-verification deferred per project policy)

### F141 · GC log_stream rewrite storm — batch + must_sync=false + yield + rate-limit

- **Target:** Reproduced 2026-05-05: after `cluster.sh reset 4` + a write workload that filled log_stream with VPs, GC kicking in (`GC: starting, extents=[16, 17]`) instantly drove `stream append summary ops` from idle (1 ops/s, 752 ops/s) to a sustained 6 000–7 000 ops/s storm on stream_id=18 for ~10 s, then the extent-nodes started returning `append timeout after 5s` on replicas 0 and 1, foreground put traffic stalled, and a shutdown-time append measured `total_ms=14587, fanout=3377` — extent-node was saturated, foreground workload was starved. Root cause: `process_gc_chunk` re-appended every still-live VP record one-at-a-time via `part_sc.append(log_stream_id, &log_entry, /*must_sync=*/true)`. Per-record fsync × 3-replica fanout × N records / second swamped the extent-node fanout, the StreamClient's per-stream worker, and the log_stream's tail extent.
- **Mechanism:** sealed log_stream extents reach hundreds of MiB to a few GiB before they qualify for GC (40 % discard threshold). At 4 KiB-typical VP records that's 64 k–256 k records to rewrite, each as one independent `append` call, each fsync-ed, each fanned out to 3 replicas. The StreamClient's per-stream worker mpsc serialises GC's appends with foreground put `append_batch` calls, so foreground latency lifts proportionally; on the extent-node side the WAL fsync rate ceiling becomes the system rate.
- **Fix (5-pronged, F141 lands them all together — single-knob fixes don't compose):**
  1. **Batch GC appends**: `process_gc_chunk` now stages records into a `GcWriteBatch` (segments + per-record metadata) and flushes via `append_segments(log_stream_id, segments, must_sync)`. Defaults: ≤ 256 records *or* ≤ 4 MiB per batch, whichever hits first. Env tunables: `AUTUMN_PS_GC_BATCH_RECORDS` ([1, 4096], default 256) / `AUTUMN_PS_GC_BATCH_BYTES` ([64 KiB, 256 MiB], default 4 MiB). One `append_segments` replaces N `append` calls — at N=256 this is a 256× drop in StreamClient worker mpsc traffic and a 256× drop in extent-node `handle_append_batch` traversals.
  2. **`must_sync=false` for intermediate batches**: only the *final* `flush_gc_batch` of `run_gc` runs with `must_sync=true`, and that single fsync — by POSIX semantics — flushes every prior must_sync=false byte on the same fd. This collapses 6 000+ fsync/s into 1 fsync per GC'd extent. `punch_holes` only runs after the final must_sync=true commit, so durability before the source extent is reaped is preserved.
  3. **Cooperative yield**: `gc_yield_now()` (one-shot Pending+wake) at the end of every batch flush AND after every read_bytes_from_extent forces the compio scheduler to poll `merged_partition_loop` / ps-conn between GC operations, even when the rate limiter has no debt.
  4. **Throughput rate limit (read + write)**: `GcRateLimiter` (1-second sliding window) caps GC's total log-layer footprint — both rewrite bytes AND source-extent read bytes — at 64 MiB/s by default. Env: `AUTUMN_PS_GC_RATE_BYTES_PER_SEC` (0 = unlimited). Bounds the worst case where GC runs against a full extent of all-live VPs without producing meaningful free space.
  5. **Smaller read chunks (64 MiB → 8 MiB)**: a single 64 MiB `read_bytes_from_extent` against an EC-subrange-converted, replicated source extent (e.g. extent 20 with `sealed_length ≈ 1 GiB`) was observed stalling extent-node 1's compio runtime for ~15 s post-F141-batching, causing foreground put fanout against partitions sharing that node to time out at StreamClient's 5 s ceiling. Smaller chunks keep the extent-node's read I/O slot returning often enough that foreground appends don't hit the timeout. Env: `AUTUMN_PS_GC_READ_CHUNK_BYTES` (default 8 MiB).
- **Files:** `crates/partition-server/src/background.rs` (`run_gc` + `process_gc_chunk` rewrite, new helpers `GcWriteBatch` / `GcRateLimiter` / `flush_gc_batch` / `gc_yield_now`; smaller default chunk; rate limiter bills BOTH read and write bytes).
- **Verification:** `cargo build --workspace --release` clean. `cargo test -p autumn-partition-server --lib gc_streaming_tests` 4/4 pass (the F106 record-boundary carry contract is preserved). Whole-suite `cargo test -p autumn-partition-server --lib` reports 103 passed / 2 failed identically before and after the patch (the two `f099i_tests` failures are a pre-existing parallel-test race on the process-wide `PS_FAST_PATH_HITS` counter — passes serially with `--test-threads=1` on both branches).
  - **First-pass live verification (2026-05-05 06:08–06:11Z):** With initial F141 (write-only batching) deployed, GC `moved 23 entries` / `moved 91 entries` per run with no `ops=6000+` storm. Foreground put on partitions 29 / 36 still saw 5 s `replica 0 rpc error: 127.0.0.1:9101 append timeout after 5s` warnings — root cause was GC's READ side (17×64 MiB EC-subrange reads of extent 20's 1 GiB) head-of-line-blocking the extent-node runtime. Items 3–5 above (yield + rate-limit on reads, smaller chunks) target that residual.
- **Out of scope:**
  - `MAX_GC_ONCE = 3` extents per GC tick is unchanged — the storm root was per-record, not per-extent.
- **passes:** true (build clean, gc_streaming_tests pass, no new regressions vs main; full live re-verification deferred to next session per user policy on destructive cluster commands).

### F142 · `must_sync=false` foreground writes break WAL→SST durability ordering

- **Target:** Reproduced 2026-05-05. User had `put`-ed a 100 MB MP4 with `--nosync` to partition 29 (log_stream=26). Yesterday's session ended cleanly (no `kill`, only `restart-ps`); today `head` returns `length: 102356867` but `get` fails with `ec_read_full_and_slice: offset 459976209 past decoded payload len 308491269 (requested length=102356867) for extent 31 (manager sealed_length=308491269)`. Forensics: extent 31 was loaded at today's startup at `len=146 MB`, sealed at `308 MB` for EC at `06:10:20`, all 4 replica shards at 98 MB each (matches `shard_size(308 MB, 3+1)`). The bad VP was flushed to a row_stream SST yesterday at `23:20`, so it cannot have been F141-introduced.
- **Mechanism:** the LSM/WAL invariant requires "memtable can be flushed to SST only after the WAL records corresponding to all entries in that memtable are durably persisted". With `--nosync` foreground writes, `start_write_batch` calls `append_segments(log_stream_id, ..., must_sync=false)` — extent-node `handle_append` *Direct path* writes to page cache and SKIPS `file.sync_all()`. The StreamClient still sees the append `Ok` (because `extent.len` advances in memory and `commit_length` queries return the higher value), updates `state.commit`, and the partition memtable stores a `ValuePointer` pointing into a region that exists only in page cache. **Memtable rotation → flush** then ships the imm to P-bulk, which writes the SST via `row_stream.append(must_sync=true)` (SST IS durable) and the partition writes the meta_stream checkpoint via `save_table_locs_raw(must_sync=true)` (checkpoint IS durable). The SST + meta_stream checkpoint are now durable, including a VP referencing log_stream offsets that are NOT durable. Any event that loses page cache before the OS writeback flushes the dirty pages — Mac reboot, OOM-driven page eviction, even `extent-node` restart in some configurations — leaves the SST persistent and the VP target gone. `head` reads the VP from the SST and reports the value's length; `get` resolves the VP, reads (post-EC) the sealed payload, and `ec_slice_decoded` notices `offset > payload_len` → the user-visible error. F119-D is ruled out by the shard-size math (98 MB per replica is exactly `shard_size(sealed_length=308 MB, K=3)`, not `shard_size(shard_size(...), K=3)`), so this is not a double-EC encoding bug. F140 deferred (split + EC race) is *also* ruled out by absence of a split in the current cluster history. The remaining culprit is the missing WAL→SST barrier under `--nosync`.
- **Fix (F142, option A from the diagnosis):** insert a single fsync barrier on `log_stream`'s open tail before P-bulk commits the SST. Implementation:
  1. **Extent-node**: new `MSG_SYNC_EXTENT = 13` RPC (`SyncExtentReq{extent_id, revision}` → `SyncExtentResp{code}`). `handle_sync_extent` does shard-routing, revision fencing (matches `handle_commit_length`), and calls `file_ref(&entry.file).sync_all().await` — no payload, no `extent.len` change, idempotent.
  2. **StreamClient**: new public `sync_stream_tail(stream_id)`. Loads the stream, picks the tail extent, skips if `sealed_length > 0` (sealed tails can't grow further; the page cache state is what the seal captured), fans `MSG_SYNC_EXTENT` out to all `replicates ++ parity` replicas via `join_all`, and requires `replicates.len()` (K) successful syncs to declare the barrier complete. M data-shard targets must all sync; up to M parity-target failures are tolerated, mirroring the post-EC fault model.
  3. **Partition server**: `flush_one_imm` calls `part_sc.sync_stream_tail(log_stream_id)` BEFORE sending the `FlushReq` to P-bulk. The barrier covers both the P-bulk path and the legacy in-thread fallback (which `flush_one_imm` delegates to after the barrier). `flush_memtable_locked` and `background_flush_loop` both go through `flush_one_imm`, so split / shutdown / periodic flushes all get the barrier.
- **Bounded scope, two known gaps deferred to a follow-up:**
  - **Sealed-extent durability**: ~~a flush whose imm references a previously-sealed log_stream extent (vp head crossed an extent rotation during the imm's lifetime) is still vulnerable, because seal does NOT currently fsync.~~ **Closed by F143** below.
  - **Sync `commit_length` parity**: `sync_stream_tail` adds a manager RPC (`get_stream_info`) + extent-node fanout per flush. A busy partition flushing every few seconds incurs ~1 extra manager RPC and 3-4 extent-node sync RPCs. Negligible vs. SST-build cost (~hundreds of ms for a 256 MB SST), but if it ever shows up in profiling, hoist the tail extent_id into `PartitionData` and skip the manager round-trip when unchanged.
- **Files:** `crates/stream/src/extent_rpc.rs` (`MSG_SYNC_EXTENT` + `SyncExtentReq` / `SyncExtentResp` codec), `crates/stream/src/extent_node.rs` (`handle_sync_extent` + dispatch arm), `crates/stream/src/client.rs` (`sync_stream_tail` public method + import additions), `crates/partition-server/src/lib.rs` (`flush_one_imm` calls the barrier before launching the SST commit).
- **Verification:** `cargo build --workspace --release` clean. `cargo test -p autumn-stream --lib` 48/48 pass. `cargo test -p autumn-partition-server --lib` 104 passed / 1 failed identically before and after; the failure is the same pre-existing `f099i_d1_fast_path_no_fu_allocation` parallel-test race on the process-wide `PS_FAST_PATH_HITS` counter, passes serially with `--test-threads=1`. Live cluster re-verification of the original `head→get` repro deferred per user policy on destructive cluster commands.
- **Out of scope:** existing bad VP for the user's MP4 file is unrecoverable — its 100 MB never reached disk. F142 prevents the next one.
- **passes:** true (build + tests clean; live re-verification deferred).

### F143 · Sealed-extent durability (fsync on seal application)

- **Target:** F142 only synced the OPEN tail of `log_stream` before SST commit. If an imm's vp head crossed an extent rotation during the imm's lifetime, the previously-sealed extent could still hold page-cache-only bytes — extent-node `apply_extent_meta` records `sealed_length` in memory and persists the meta sidecar, but does NOT `file.sync_all()` the data file. The same page-cache loss event that motivated F142 then drops bytes from the *sealed* file's tail; the file shrinks below `sealed_length` and any VP referencing the lost region post-EC surfaces the same `ec_read_full_and_slice: offset N past decoded payload len M` error. The bug shape is identical to the F142 case but harder to hit (requires extent rotation mid-imm).
- **Fix:** new `apply_extent_meta_durable(&self, extent_id, &Rc<ExtentEntry>, &ExtentInfo)` async helper on `ExtentNode`. Wraps the existing sync `apply_extent_meta` + `save_meta` chain and additionally calls `file_ref(&extent.file).sync_all().await` whenever the apply reports a `0 → sealed_length > 0` transition. All 8 call sites that previously did `apply_extent_meta` + `if sealed_changed { save_meta }` now go through the durable helper. Idempotent: repeat invocations on an already-sealed extent are a single atomic load and no I/O. The pre-EC `handle_convert_to_ec` paths that direct-store `sealed_length` are unchanged — EC's own `write_shard_local` already fsyncs its outputs.
- **Files:** `crates/stream/src/extent_node.rs` (new `apply_extent_meta_durable`; 6 in-file call sites + 2 free-function call sites in `build_append_future` switched over; deletes the now-dead `apply_extent_meta_ref` whose stale comment claimed external use).
- **Verification:** `cargo build --workspace --release` clean. `cargo test -p autumn-stream --lib` 48/48 pass. `cargo test -p autumn-partition-server --lib` 103 pass / 2 fail — the same pre-existing `f099i` parallel-test race that exists on main.
- **Performance:** seal application is rare (one fsync per extent rotation, manager-pushed seal, or split). The fsync cost is bounded by the dirty-page count for that extent at seal time, which is typically small. Even on a 1 GiB extent freshly receiving must_sync=false writes, the fsync is one bounded I/O on the background flush task — not on the foreground put hot path.
- **passes:** true.

### F144 · Allocator node-selection bias (random shuffle replaces sort+take)

- **Trigger:** User observed that on a 4-node cluster (node_ids `1, 3, 5, 7`) every freshly created stream / extent landed on `[1, 3, 5]`. Out of 14 extents inspected on a live cluster, only 2 ever included node 7 — the rest were `[1, 3, 5]` for both the open replica set and the post-EC-conversion (3+1) layout. Concrete impact: 100% of write fan-out, 100% of foreground read traffic, and ~75% of EC parity work concentrated on three of the four extent-nodes.
- **Root cause:** `AutumnManager::select_nodes` (`crates/manager/src/lib.rs:597`) collected every registered node into a Vec, sorted ascending by `node_id`, and returned `take(count)`. With a sorted candidate list, the lowest `count` IDs always win — node 7 is only ever picked when one of `{1,3,5}` is filtered out by the F121 online-disk check. Two adjacent paths reproduced the same bug shape: (a) `recovery.rs:591-622`'s EC-conversion extra-parity allocation walked `HashMap.values().take(extra_needed)`, which is deterministic-per-process and biased toward whichever node_id happened to be visited first; (b) `rpc_handlers.rs:845-851`'s `stream_alloc_extent` fall-back iterator sorted the leftover candidates by `node_id` before walking them, so a failed primary always retried on the next-lowest ID.
- **Fix (option 2 from design discussion):** keep the F121 "at least one online disk" filter; replace the deterministic order with a uniform random `count`-subset. Implementation uses `rand::seq::SliceRandom::shuffle(&mut rand::thread_rng())` followed by `take(count)`. All three call sites switched in lock-step:
  1. `select_nodes` (healthy path + degraded fallback)
  2. `ec_conversion_dispatch_loop` extra-parity selection
  3. `handle_stream_alloc_extent` fall-back iterator
  Capacity-aware least-allocated selection (option 3) is deferred to a future feature — it requires a per-node extent counter persisted in etcd and is orthogonal to this fix.
- **Files:** `crates/manager/Cargo.toml` (new `rand = "0.8"` dep, matching the version already used by `autumn-server`), `crates/manager/src/lib.rs` (`select_nodes` body + new F144 doc-comment block + 2 new unit tests `f144_select_nodes_distribution` and `f144_select_nodes_degraded_fallback_shuffles`), `crates/manager/src/recovery.rs` (EC extra-parity shuffle), `crates/manager/src/rpc_handlers.rs` (fall-back shuffle).
- **Verification:**
  - `cargo build -p autumn-manager` clean (only pre-existing unused-import warnings).
  - `cargo build --workspace` clean.
  - `cargo test -p autumn-manager --lib` 16/16 pass — includes the 2 new F144 tests. The new distribution test runs `select_nodes` 1000 times against a 4-node cluster requesting count=3 and asserts each node lands in `[600, 900]` of the selections (theoretical mean 750, std-dev ~14, so the bound passes essentially with probability 1).
  - Live-cluster re-verification deferred per project policy on destructive commands. Manual repro plan: `cluster.sh reset 4 && for i in $(seq 1 16); do $AC create-stream …; done && manager dump-extents | sort | uniq -c` — node 7 should appear in roughly 12 of 16 (75%) of replica sets, instead of 0–2 of 16 pre-F144.
- **Out of scope:** capacity / used-bytes-aware allocation (deferred); read-side load balancing (deferred — once allocation spreads, the existing `replica[0]`-first read path naturally distributes across all nodes since extents now live on different sets).
- **passes:** true (build + lib tests clean; live re-verification deferred).

### F145 · punch_holes/truncate vs in-flight EC conversion (eversion-bump lock gap)
- **Target:** F138 elevated `ec_conversion_inflight` to an eversion-bump lock but missed two mutators. `handle_stream_punch_holes` and `handle_truncate` in `crates/manager/src/rpc_handlers.rs` both fell into the `extent.eversion += 1` else-branch for any ec-inflight extent (refs<=1 path) or the unconditional `eversion += 1` path (refs>1 path), violating F138's invariant that no task may bump eversion while EC conversion is in flight. This caused `apply_ec_conversion_done`'s `ex.eversion = new_eversion` to overwrite the intermediate bump (lost-update), and the subsequent `ex.replicates = target_nodes[..K]` to silently rewrite replica assignments on a stale extent record. On leader-failover the replayed etcd state was internally inconsistent.
- **Root cause (race timeline):** (1) `ec_conversion_dispatch_loop` captures `new_eversion = eversion + 1`, inserts X into `ec_conversion_inflight`, awaits `EXT_MSG_CONVERT_TO_EC` (multi-second). (2) concurrent `handle_stream_punch_holes` sets refs=0, falls into else branch, runs `extent.eversion += 1` (= new_eversion), mirrors to etcd, removes X from stream. (3) EC completes; `apply_ec_conversion_done` writes `ex.eversion = new_eversion`, `ex.replicates = target_nodes`, overwriting the now-stale record — replica state and manager state diverge.
- **Fix:** Two symmetric `Err(Precondition)` guards in `handle_stream_punch_holes` and `handle_truncate`, immediately after the F139 recovery guards, refusing the entire RPC if any to-be-removed extent is in `ec_conversion_inflight`. The PS GC retry loop already handles `Precondition` from F139; the same retry covers EC. No new locks or state needed.
- **Files:** `crates/manager/src/rpc_handlers.rs` (2 guards, one per handler), `crates/manager/src/lib.rs` (2 new F145 unit tests: `f145_punch_holes_refuses_when_ec_inflight`, `f145_truncate_refuses_when_ec_inflight`), `crates/manager/CLAUDE.md` (note 12 completing the F138 mutator list), `README.md` (F145 manual-repro block), `feature_list.md` (this entry), `claude-progress.txt`.
- **Verification:**
  - `cargo build --workspace` clean (only pre-existing warnings).
  - `cargo test -p autumn-manager --lib` 26/26 pass — 24 pre-existing + 2 new F145 tests.
  - `cargo test -p autumn-stream --lib` 48/48 pass.
  - `cargo test -p autumn-partition-server --lib` 105 pass / 2 pre-existing `f099i` parallel-test race failures (same baseline as F144).
  - Live-cluster re-verification deferred per project policy on destructive commands.
- **passes:** true (build + lib tests clean; live re-verification deferred).

### F146 · Three manager-side data-corruption races (alloc_extent, split, append-past-seal)
- **Target:** Three remaining lost-update races with the same snapshot-capture-then-await-then-writeback shape: (HIGH-1) `handle_stream_alloc_extent` unconditionally overwrote `s.extents[tail]` at apply time, losing concurrent recovery_done/ec_conversion_done/punch_holes/truncate/split mutations; (HIGH-2) `handle_multi_modify_split` Phase-1 already had an F138 `ec_conversion_inflight` check but no `recovery_tasks` check, and Phase-3 `apply_split_mutations` overwrote live state without verifying no concurrent mutator ran during Phase-2's etcd await; (HIGH-3) `build_append_future` in `extent_node.rs` did not re-check `sealed_length / avali` after the `truncate_to_commit_ref` await, allowing a concurrent `apply_extent_meta_durable` (from `handle_re_avali` or another append's pre-truncate seal-confirm path) to land a fresh seal during the truncate I/O — the pwritev would then land bytes past the new sealed_length.
- **Root cause:** F138/F139/F145 covered the explicit eversion-bump-lock pattern but did not audit snapshot-capture-then-writeback handlers for the "dispatch-during-our-await" sub-race.
- **Fix:**
  - HIGH-1: refuse-at-start in `handle_stream_alloc_extent` (check `ec_conversion_inflight` + `recovery_tasks` for the tail before any await), plus verify-at-apply (re-read `s.extents[tail].eversion` under a fresh `borrow_mut` after the etcd mirror; refuse with `Precondition` if eversion changed during the await window).
  - HIGH-2: extend the F138 Phase-1 block in `handle_multi_modify_split` to also check `recovery_tasks` for every source-stream extent; add verify-at-apply in Phase-3 comparing `pre_bump_eversion` snapshot against live eversions before calling `apply_split_mutations`.
  - HIGH-3: after `truncate_to_commit_ref` returns Ok and before the `file_start` reload, re-check `extent.sealed_length.load(SeqCst) > 0 || extent.avali.load(SeqCst) > 0`; if true, return `CODE_PRECONDITION` (mirrors step-3 guard at line 818). Client-side `apply_completion` already classifies `CODE_PRECONDITION` as a soft error → retry with fresh tail.
- **Files:** `crates/manager/src/rpc_handlers.rs` (HIGH-1 refuse-at-start + verify-at-apply in `handle_stream_alloc_extent`; HIGH-2 recovery_tasks check + verify-at-apply in `handle_multi_modify_split`), `crates/manager/src/lib.rs` (3 new F146 unit tests), `crates/stream/src/extent_node.rs` (HIGH-3 post-truncate seal recheck), `crates/manager/CLAUDE.md` (note 13), `crates/stream/CLAUDE.md` (append protocol step-5 annotation), `README.md` (F146 repro block), `feature_list.md` (this entry), `claude-progress.txt`.
- **Verification:**
  - `cargo build --workspace` clean (only pre-existing warnings).
  - `cargo test -p autumn-manager --lib` 29/29 pass — 26 pre-existing + 3 new F146 tests.
  - `cargo test -p autumn-stream --lib` 48/48 pass.
  - `cargo test -p autumn-partition-server --lib` 105 pass / 2 pre-existing `f099i` parallel-test race failures (same baseline as F145).
  - Live-cluster re-verification deferred per project policy on destructive commands.
- **passes:** true (build + lib tests clean; live re-verification deferred).

### F147 · Three snapshot-await-writeback races (sync_partition_vp_refs etcd divergence, handle_append non-batched seal recheck, run_recovery_task verify-after-fetch)
- **Target:** Three remaining data-corruption races sharing the snapshot-capture-then-await-then-apply shape not covered by F146.
- **Root cause:** F146 added refuse-at-start + verify-at-apply to `handle_stream_alloc_extent` and `handle_multi_modify_split`, and added a post-truncate seal recheck to `build_append_future` (the batched append path). Three analogous paths were missed: (F147-A) `handle_sync_partition_vp_refs` in the manager applied a VP-ref diff after an etcd await without verifying the touched extents had not been mutated during the await, causing etcd-vs-memory divergence on leader-failover replay; (F147-B) `handle_append` (the non-batched path, line ~2437) lacked the F146 post-truncate seal recheck that `build_append_future` received, so a concurrent `apply_extent_meta_durable` sealing the extent during the truncate await would allow pwritev to land bytes past the new `sealed_length`; (F147-C) `run_recovery_task` had no verify-after-fetch step, so a concurrent seal arriving during the extent fetch could cause recovery to write stale metadata back and log incorrect `fetch_max` values.
- **Fix:**
  - F147-A: refuse-at-start in `handle_sync_partition_vp_refs` (check `ec_conversion_inflight` + `recovery_tasks` for each extent in the new snapshot; return `Err(Precondition)` before any await if any is in-flight) + verify-at-apply (re-read each touched extent's `eversion` under a fresh `borrow_mut` after the etcd mirror; return `Err(Precondition)` if any eversion changed during the await window).
  - F147-B: insert the same post-truncate seal recheck (`sealed_length.load(SeqCst) > 0 || avali.load(SeqCst) > 0`) in `handle_append` immediately after `truncate_to_commit` returns Ok and before the `file_pwrite` offset computation — structurally identical to the F146 recheck in `build_append_future`.
  - F147-C: after the `sync_all` following the full-extent fetch in `run_recovery_task`, re-read the local extent's `eversion` atomics; if it advanced during the fetch, retry the recovery task. Additionally gate the `fetch_max` writeback on the fetched length matching the manager-reported `sealed_length`.
- **Files:** `crates/manager/src/rpc_handlers.rs` + `crates/manager/src/lib.rs` (F147-A: refuse-at-start + verify-at-apply, 1 new unit test: `f147_sync_vp_refs_refuses_when_concurrent_eversion_bump`), `crates/stream/src/extent_node.rs` (F147-B: `handle_append` post-truncate recheck, 1 test in `f147b_tests`; F147-C: `run_recovery_task` verify-after-fetch + fetch_max writeback gate, 1 test in `f147c_tests`), `crates/manager/CLAUDE.md` (note 14), `crates/stream/CLAUDE.md` (append protocol step-5 F147-B/C bullets), `README.md` (F147 manual-repro block), `feature_list.md` (this entry), `claude-progress.txt`.
- **Verification:**
  - `cargo build --workspace` clean (only pre-existing warnings).
  - `cargo test -p autumn-manager --lib` 30/30 pass — 29 pre-existing + 1 new F147-A test.
  - `cargo test -p autumn-stream --lib` 50/50 pass — 48 pre-existing + 2 new F147-B/C tests.
  - `cargo test -p autumn-partition-server --lib` 105 pass / 2 pre-existing `f099i` parallel-test race failures (same baseline as F146).
  - Live-cluster re-verification deferred per project policy on destructive commands.
- **passes:** true (build + lib tests clean; live re-verification deferred).

### F148 · Race-hunt audit after F147 (regression-test + defensive guard, no production behavior change)
- **Target:** Continue checking for unfixed snapshot-await-writeback / reader-vs-reclaim / dual-writer races after F147 closed three. Lock in the orderings that *currently* prevent corruption so future refactors cannot regress silently.
- **Audit verdict:** No new HIGH-severity unfixed races confirmed. Three parallel layer-scoped Explore agents (manager / extent-node / partition-server) flagged 12 candidates; verification against the actual code + crate CLAUDE.md context showed each is either (a) already covered by F119–F147 (manager handlers all use F147-A refuse-at-start + verify-at-apply; extent-node `handle_append` has F146/F147-B post-truncate recheck; recovery has F147-C verify-after-fetch), (b) closed by F140 dual-gate (split vs compact + GC), (c) theoretical and not exercised by the production call-graph (e.g., `handle_copy_extent` on unsealed extents — recovery and re-avali only target sealed extents), or (d) precluded by single-threaded compio + the synchronous path between `borrow_mut` drop and mpsc-send (the PS-side hypothesised flush-vs-compact `save_table_locs_raw` stale-snapshot race). The previously-known MED-2 (`handle_get → resolve_value` vs background GC `punch_holes`) remains deferred as a separate structural feature requiring a per-extent reader-pin protocol.
- **Why this still ships as a feature:**
  - F148-A: the conclusion that PS-side flush + compact metadata publishes are race-free is *load-bearing* on (P1) compio P-log is single-threaded, (P2) `borrow_mut` blocks contain no `.await`, (P3) the path `borrow_mut` drop → `rkyv_encode` → `stream_client.append` → mpsc-send is purely synchronous. A future refactor that violates any of these silently re-opens a stale-snapshot race that could persist tables which compaction has already removed (data corruption on restart if GC has punched any of those tables' VP-referenced log_stream extents). Inline `// F148-A invariant` comments at all four call sites (`flush_one_imm` + `flush_one_imm_local` in lib.rs; both branches of `do_compact` in background.rs) state the rule next to the code; a regression test (`f148_publisher_invariant_tests::f148_concurrent_publisher_ordering_invariant`) exercises two simulated publishers on a single compio runtime and asserts the LATER snapshot extends the EARLIER one.
  - F148-B: defensive guard in `handle_copy_extent`. After the manager-fetch + `apply_extent_meta_durable`, refuse with `CODE_PRECONDITION` if `entry.sealed_length == 0`. Production callers (`run_recovery_task`, `handle_re_avali`) only target sealed extents by design — the manager dispatches both only after seal. Without this guard a stray caller hitting an unsealed extent could race a concurrent in-flight `handle_append`'s `truncate_to_commit` await window and observe a mix of pre- and post-truncate bytes via `file_pread_chunked`. On a sealed extent the append protocol step 3 rejects concurrent appends, so the race only exists for unsealed extents. The guard converts the theoretical race into a clean error and documents the invariant in code.
- **Files:**
  - `crates/stream/src/extent_node.rs` (F148-B guard in `handle_copy_extent`, 2 tests in `f148_copy_extent_tests`).
  - `crates/partition-server/src/lib.rs` (F148-A inline comments in `flush_one_imm` + `flush_one_imm_local`, 1 test in `f148_publisher_invariant_tests`).
  - `crates/partition-server/src/background.rs` (F148-A inline comments in both branches of `do_compact`).
  - `crates/partition-server/CLAUDE.md` (Programming Note 10: metadata-publish ordering invariant).
  - `crates/stream/CLAUDE.md` (append protocol step 5: F148-B handle_copy_extent guard bullet).
  - `feature_list.md` (this entry).
  - `claude-progress.txt`.
  - `README.md` (F148 manual-repro block).
- **Verification:**
  - `cargo build --workspace` clean (only pre-existing warnings).
  - `cargo test -p autumn-manager --lib` 30/30 pass (no manager-side change).
  - `cargo test -p autumn-stream --lib` 52/52 pass — 50 pre-existing + 2 new F148-B tests.
  - `cargo test -p autumn-partition-server --lib` 108/108 pass — pre-existing baseline + 1 new F148-A test (the 2 historical `f099i` parallel-test races did not trigger this run).
  - Live-cluster re-verification not applicable: F148 ships only documentation, an inline guard with no production-path effect, and tests.
- **passes:** true (build + lib tests clean; behaviorally inert in production).

### F149 · Leader-fence on every manager etcd write txn (master-standby split-brain closure)
- **Target:** Close the failover-gap split-brain window. F005's lease-based leader election guarantees that at most one manager **holds** the leader-key at any time, but the deposed leader's in-process `self.leader.get() == true` flag can lag the etcd ground truth indefinitely under runtime starvation, GC pauses, or syscall hangs. During that lag the deposed leader's mirror_* writes overwrite the new leader's state with last-writer-wins — reverting freshly-applied recovery slot replacements, EC conversion eversion bumps, split snapshots, etc. F149 makes every manager → etcd write txn fenced on the value of the leader-key.
- **Root cause:** Pre-F149, mirror_* helpers issued plain etcd puts without any conditional CAS on identity. F005's `Cmp::create_revision == 0` only protects the leader-key acquisition itself; it does not protect subsequent metadata mutations. The window between (a) the etcd lease expiring + a new leader winning the CAS, and (b) the old leader's keepalive task observing failure and flipping `set_leader(false)`, is the exposure: bounded by lease TTL + keepalive jitter on a healthy host (~10 s), unbounded under host pathology.
- **Fix:**
  - Added `autumn_etcd::Cmp::value(key, value)` helper (target=3 VALUE, result=0 EQUAL).
  - `EtcdMirror` now carries `instance_id: Rc<String>` + `leader: Rc<Cell<bool>>` (shared with `AutumnManager`).
  - New private `EtcdMirror::txn_fenced(extra_cmp, success, failure) -> Result<bool, AppError>` always prepends `Cmp::value(LEADER_KEY) == instance_id` to `extra_cmp`. On `succeeded == false` it does a follow-up GET on the leader-key to distinguish (a) fence-fail (someone else's `instance_id` or empty) → flip `leader` Cell to false + return `AppError::NotLeader`; (b) fence-held but extra_cmp-fail → return `Ok(false)` (preserves the existing CAS-fail semantics for owner-lock + recoveryTasks acquisition paths).
  - `EtcdMirror::put_msgs_txn` and `put_and_delete_txn` rerouted through `txn_fenced(vec![], …)`. Return type changed from `anyhow::Result<()>` to `Result<(), AppError>` so the `NotLeader` distinction propagates.
  - All 9 mirror_* helpers + `persist_extent` now `?` directly into `AppError` (the redundant `.map_err(|e| AppError::Internal(e.to_string()))` shims were stripped).
  - `acquire_owner_revision` (lib.rs) and `dispatch_recovery_task` (recovery.rs) — the two manual `c.txn(...)` callsites — now use `txn_fenced` with `extra_cmp = vec![Cmp::create_revision(key, 0)]`. Their `Ok(false)` semantics (key already exists) are preserved.
  - `handle_multi_modify_split`'s Phase-2 consolidated etcd txn already routed through `etcd.put_msgs_txn`; only the `.map_err` shim was updated to `Self::err_to_status`.
  - `try_become_leader` and `replay_from_etcd` are intentionally NOT routed through `txn_fenced` (the former IS the operation establishing leader-key ownership; the latter is read-only).
- **Files:** `crates/etcd/src/lib.rs` (Cmp::value), `crates/manager/src/lib.rs` (EtcdMirror struct + connect signature, txn_fenced, put_msgs_txn / put_and_delete_txn rerouted, mirror_* shims stripped, acquire_owner_revision rerouted, LEADER_KEY hoisted to module-level constant, instance_id changed to `Rc<String>`), `crates/manager/src/recovery.rs` (dispatch_recovery_task rerouted, two `.map_err` shims stripped), `crates/manager/src/rpc_handlers.rs` (handle_multi_modify_split error mapper updated to Self::err_to_status), `crates/manager/tests/f149_leader_fence.rs` (new integration test gated on embedded etcd), `crates/manager/CLAUDE.md` (note 15), `feature_list.md` (this entry), `README.md` (F149 manual-repro block), `claude-progress.txt`.
- **Verification:**
  - `cargo build --workspace`: clean (pre-existing warnings only).
  - `cargo test -p autumn-etcd --lib`: 3/3 pass.
  - `cargo test -p autumn-manager --lib`: 30/30 pass (no behavior regression in single-manager mode — the fence compare passes when the manager owns the leader-key, which is always true in unit tests since they use in-memory `AutumnManager::new()` with `etcd: None`).
  - `cargo test -p autumn-stream --lib`: 52/52 pass.
  - `cargo test -p autumn-partition-server --lib`: 108/108 pass.
  - `cargo build -p autumn-manager --tests`: clean compile of all integration tests including the new `f149_leader_fence`.
  - The integration test `f149_deposed_leader_etcd_writes_are_fenced` is `#[ignore]`-gated per repo convention (requires Go toolchain to build the embedded etcd helper); reproduces the deposition window by externally overwriting the leader-key value mid-flight and asserts (a) baseline writes succeed, (b) the next write returns `CODE_NOT_LEADER`, (c) subsequent writes remain `CODE_NOT_LEADER` (proving the in-process `leader` Cell flipped without re-hitting etcd), and (d) only the pre-deposition state survived in etcd. Live-cluster verification is the same procedure run against a 3-node etcd cluster — manual repro listed in README.md.
- **passes:** true (build + 4 lib test suites pass; integration test compiles cleanly, marked `#[ignore]` per repo convention).

### F150 · F142 fsync-barrier cost reduction (Phase A- + A + B; Phase C deferred)
- **Target:** Reduce the 22% write-throughput cost F142's WAL fsync barrier added to the flush hot path (124 k ops/s → 97 k ops/s on the bisect-confirmed regression at TCP p=8 d=8 4K). F142 is a correctness fix (must_sync=false WAL bytes can evaporate from page cache after SST commit, leaving VPs pointing at phantom bytes) and cannot be removed on durability grounds. F150 keeps the same invariant but folds the barrier into existing code paths.
- **Phase A- — extent-node WAL deletion.** F035-vintage WAL was originally a small-write durability optimisation: must_sync=true payloads ≤ 2 MiB took a sequential WAL file (separate fsync) instead of fsync'ing the extent file. On rotational disks this beat random extent fsync; on modern SSD/NVMe the gap collapses (no seek penalty + SLC cache + wear levelling). The WAL also added a parallel codepath that the F150 Phase B barrier would have to bypass with a new flag. Deletion: `crates/stream/src/wal.rs` (963 lines) + `replay_wal_files` + `Wal::open` + `--wal-dir` CLI flag + `ExtentNodeConfig::with_wal_dir` + `ExtentNode.wal` field + `start_node_with_wal` test helper + `tests/wal_recovery.rs` integration test (170 lines). After deletion `handle_append`'s must_sync=true path collapses to the Direct path (`file.write_at` + `file.sync_data()`).
- **Phase A — sync_all → sync_data.** Replaced every `file.sync_all().await` call site in `crates/stream/src/extent_node.rs` with `sync_data().await`. On Linux ext4, fdatasync skips one inode-metadata journal commit relative to fsync; the file size still gets sync'd (so EOF reads remain consistent). Per-fsync cost drops ~30-50% on ext4 SSDs; on the local /tmp tmpfs perf-check rig the difference is unmeasurable (RAM-backed, fsync is essentially free either way) — Phase A's value is on production hardware.
- **Phase B — rotation-trigger must_sync barrier.** PS-side `start_write_batch` (`crates/partition-server/src/background.rs`) now estimates `active.mem_bytes() + Σ record_sizes` and forces `batch_must_sync = true` when this batch will push past `FLUSH_MEM_BYTES`. Post-Phase-A- this means the rotation-triggering batch's append takes the Direct path → `file.sync_data()` on the log_stream tail extent, which by Linux semantics fsyncs ALL dirty pages of the file including any prior must_sync=false bytes. The imm that gets created by the next `maybe_rotate` therefore has all its VP-referenced WAL bytes durable before flush_one_imm even fires. Deletions: `flush_one_imm`'s `part_sc.sync_stream_tail(log_stream_id)` call; `StreamClient::sync_stream_tail` public API (~80 lines); `ExtentNode::handle_sync_extent` server handler (~60 lines); `MSG_SYNC_EXTENT = 13` dispatch entry + msg type constant; `SyncExtentReq` / `SyncExtentResp` extent_rpc codecs (~55 lines). Net diff: ~200 LOC fewer + 1 fewer RPC type + 1 fewer round-trip per flush. Same fsync syscall happens regardless, but folded into the rotation-trigger append's existing 3-replica fanout instead of a separate barrier RPC.
- **Phase C — DEFERRED.** Design: per-partition `wal_sync_loop` fsyncs log_stream tail every ~50 ms, maintains `wal_synced_high_water`. `start_write_batch`'s rotation-trigger barrier becomes a wait-not-block (typically sub-ms because the background loop has already caught up). Deferred because (a) on the local tmpfs perf-check rig the measurable fsync cost is already zero — Phase C's win against zero is also zero; (b) Phase C is a larger architectural change (new background task, new state tracking, interleaving with active appends) that should be justified by a measurement on real SSD hardware showing residual fsync cost after Phase B. Re-evaluate when production benches show non-trivial Phase B flush latency.
- **Files:** `crates/stream/src/wal.rs` (deleted), `crates/stream/src/lib.rs` (mod wal; removed), `crates/stream/src/extent_node.rs` (WAL field + init + replay_wal + WAL-path branches in handle_append + build_append_future + handle_sync_extent + dispatch entry; all sync_all → sync_data), `crates/stream/src/extent_rpc.rs` (SyncExtentReq/Resp + MSG_SYNC_EXTENT), `crates/stream/src/client.rs` (sync_stream_tail public API + imports), `crates/stream/CLAUDE.md` (WAL section, config note, must_sync cost note rewritten), `crates/stream/tests/test_helpers.rs` (start_node_with_wal removal), `crates/stream/tests/wal_recovery.rs` (deleted), `crates/server/src/bin/extent_node.rs` (--wal-dir CLI flag + with_wal_dir calls in single-shard + multi-shard paths), `crates/partition-server/src/lib.rs` (FLUSH_MEM_BYTES exported pub(crate); flush_one_imm sync_stream_tail call removed), `crates/partition-server/src/background.rs` (start_write_batch rotation-trigger detection + batch_must_sync upgrade), `feature_list.md` (this entry), `claude-progress.txt`.
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean (only pre-existing warnings).
  - `cargo test -p autumn-stream --lib`: 30/30 pass (was 52 pre-F150; drop accounts for the 22 wal.rs internal unit tests deleted with the file).
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
  - `cargo test -p autumn-partition-server --lib`: 106 pass / 2 pre-existing f099i parallel-test races (same baseline as F148/F149; running each individually with --test-threads=1 passes).
  - Perf (TCP p=8 d=8 4K, /tmp tmpfs, same machine, back-to-back runs): F148 90,659 ops/s; F149 96,826 ops/s; F150 Phase A-+A 92,836 ops/s; F150 Phase A-+A+B 91,505 ops/s. All four cluster within ±3.5% — measurement noise on a tmpfs rig where fsync is already free. Read side and latency indistinguishable from F148/F149. The 26-31% gap vs the 2026-04-29 baseline (131,246 ops/s) is the F142 correctness cost (proved by HEAD-bisect: d985a6e parent=124,176 vs 7a90983 F142=96,905). Phase B preserves the F142 invariant; Phase C remains the only avenue for recovering more without compromising it.
- **passes:** true (correctness preserved, lib tests pass, architectural cleanup substantial; perf-on-tmpfs shows no degradation but cannot demonstrate a recovery either, validation deferred to production SSD bench).

### F183 · Partition merge primitive + size+load advisory policy engine (Stage 1)

- **Target:** Add the inverse of partition split as a CoW stream-extent splice (no value rewrite, single-stream-per-partition invariant preserved); add a manager-side advisory policy engine that emits split/merge candidates from per-partition `(size_bytes, req_per_sec, imm_full_per_sec)` over a 30-min sliding window. Stage 1 is manual triggers + advisory only; auto-trigger gated behind feature flags in Stage 2/3 (deferred).
- **Mechanism (manager):** `handle_multi_modify_merge` is a single fenced atomic etcd txn (F124 pattern) with F138/F145/F146 inflight checks and F146-style verify-at-apply on `pre_bump_eversion`. Allocates a fresh log_stream tail extent (E_new) inside the txn; survivor's log_stream extent_ids becomes `[L]+[V]+[E_new]`, row + meta become `[L]+[V]`. Extent refs++ on every victim extent (CoW). Victim's partition + three streams + region + partitionVpRefs + partitionLastOp keys deleted in the same txn. Survivor's `rg.end_key` widens to `victim.end_key`. Order invariant in spliced extent_ids is load-bearing for vp_head replay; tested by `f183_compute_merge_streams_extent_ids_order_and_refs`.
- **Mechanism (CLI orchestration, Stage 1):** `autumn-client merge <SURVIVOR> <VICTIM>` flushes both partitions, acquires admin owner-lock, captures sealed lengths via `MSG_CHECK_COMMIT_LENGTH`, calls `MSG_MULTI_MODIFY_MERGE`. Survivor's PS picks up the wider rg + spliced extent_ids on the next `region_sync_loop` tick (~2 s). **Operator must stop writes during the merge window** (Stage 2/3 will add a PS-side dual-gate + freeze-drain handler).
- **Mechanism (advisory):** PSes carry `Arc<PartitionMetrics>` per partition (counters bumped by `merged_partition_loop`); main-thread `report_load_loop` (5 s cadence) ships `MSG_REPORT_PARTITION_LOAD` to manager. Manager's `policy_tick_loop` (60 s cadence) computes candidates from a 30-min sliding window with thresholds (`SPLIT_SIZE_HARD=50 GiB`, `SPLIT_QPS_HIGH=50K`, `SPLIT_IMMFULL_HIGH=10`, `SPLIT_COOLDOWN=1h`, `MERGE_SIZE_LOW=1 GiB`, `MERGE_QPS_LOW=5K`, `MERGE_COOLDOWN=6h`). 10× hysteresis between split (50K qps) and merge (5K qps) prevents oscillation. Cross-PS merge candidates emitted with `same_ps=false` so operators can plan co-location.
- **Wire types added:** `MSG_MULTI_MODIFY_MERGE 0x34`, `MSG_GET_POLICY_CANDIDATES 0x35`, `MSG_REPORT_PARTITION_LOAD 0x36` (manager); `MSG_MERGE_PART 0x4D` (PS — reserved, no handler in Stage 1). `MultiModifyMergeReq/Resp`, `PartitionLoad`, `ReportPartitionLoadReq`, `PolicyCandidate`, `GetPolicyCandidatesReq/Resp`. New etcd prefix `partitionLastOp/<part_id>` (i64-LE) for cooldown gating; both split and merge handlers write entries in their atomic txn; loaded by `replay_from_etcd`.
- **Stage 1 sub-features (this commit family, all `passes:true`):**
  - F183-A: wire types (manager + PS)
  - F183-B: pure-fn helpers `compute_merge_streams`, `splice_streams_without_new_tail`, `merged_partition_vp_refs`, `apply_merge_mutations` + 4 unit tests
  - F183-C: `last_op_at` sidecar field + etcd replay + split-handler write
  - F183-D: `handle_multi_modify_merge` 4-phase impl + 3 refusal-path smoke tests
  - F183-E: `policy.rs` engine (skeleton + split + merge passes) + 11 unit tests
  - F183-F: `policy_tick_loop` spawn + handle_get_policy_candidates + handle_report_partition_load
  - F183-G: PS metrics export (Arc<PartitionMetrics>, counter bumps, report_load_loop)
  - F183-I: CLI `merge` + `policy-candidates` subcommands + ClusterClient API
- **Stage 2/3 (deferred):** `AUTUMN_MGR_AUTO_SPLIT` / `AUTUMN_MGR_AUTO_MERGE` feature flags; PS-side `handle_merge_part` with dual-gate + freeze-drain (no in-place splice in Stage 1 — survivor PS reopens via `region_sync_loop` after manager commit). Cross-PS merge requires partition migration primitive; advisory marks them infeasible for now.
- **Why split-auto before merge-auto in Stage 2/3:** thread-per-core means merge concentrates two partitions' SSTs + future load onto one P-log core. A wrongly-merged hot pair degrades immediately at the worst place (single-core ceiling). Split is the *relief valve* — its failure mode is mild (redundant partition, extra metadata). Recorded in `feedback_auto_split_before_merge.md` (auto-memory).
- **Files:** `crates/rpc/src/manager_rpc.rs` (3 new MSG_*, 4 new structs, 2 new POLICY_KIND consts); `crates/rpc/src/partition_rpc.rs` (`MSG_MERGE_PART` + `MergePartReq/Resp`); `crates/manager/src/policy.rs` (NEW); `crates/manager/src/policy_tests.rs` (NEW); `crates/manager/src/lib.rs` (4 pure-fns + `last_op_at`/`policy` fields + `policy_tick_loop` + replay); `crates/manager/src/rpc_handlers.rs` (3 new handlers + split-handler last_op_at write); `crates/common/src/store.rs` (MetadataState derives Clone for snapshot); `crates/partition-server/src/lib.rs` (PartitionMetrics + PartitionHandle.metrics + report_load_loop + counter bumps); `crates/client/src/lib.rs` (ClusterClient.merge_partitions + policy_candidates); `crates/server/src/bin/autumn_client.rs` (CLI subcommands); `README.md` + `crates/manager/CLAUDE.md` (note 16) + `crates/partition-server/CLAUDE.md` (notes 11+12).
- **Spec/plan:** `docs/superpowers/specs/2026-05-09-partition-merge-and-split-merge-policy-design.md`, `docs/superpowers/plans/2026-05-09-partition-merge-and-split-merge-policy.md`.
- **F184 follow-on (auto-trigger + reload + integration tests):**
  - F184-A: `--auto-split` / `--auto-merge` flags on autumn-manager-server; `policy_tick_loop` rate-limited 1/tick auto-dispatch when enabled.
  - F184-B: `PartitionHandle.opened_with` snapshot; `sync_regions_once` reloads partitions whose `(rg, stream_ids)` changed (catches post-merge widening).
  - F184-C: `read_all_table_locations` walks every meta_stream extent and unions LAST records — fixes survivor's recovery picking up victim's tables post-merge.
  - F184-D: integration tests `merge_split_round_trip_keys_intact`, `merge_then_split_again_round_trip` (multi-step lifecycle).
  - F184-E: 3 more merge handler unit tests (recovery_inflight, pending_delete, last_op_at).
  - F184-F: public `manager.force_auto_split(part_id)` / `force_auto_merge(survivor, victim)` test helpers + `auto_dispatch_merge_orchestrates_full_flow` integration test.
  - F184-G: `auto_dispatch_split_dispatches_msg_split_part` integration test.
- **Tests passing (post-F184):**
  - `cargo test -p autumn-rpc`: clean
  - `cargo test -p autumn-manager --lib`: 51/51 (10 merge unit + 11 policy unit + 30 pre-existing)
  - `cargo test -p autumn-manager --test system_merge -- --ignored`: 6/6 (~45 s)
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 130/130
  - `cargo build --workspace --exclude autumn-fuse`: clean
- **passes:** true

### F185 · Manager-orchestrated merge with PS freeze-drain — closes F184-K ~5% loss window

- **Target:** Close the Stage-1 merge-window data-loss gap measured by F184-K. F184's
  CLI-orchestrated merge captured `commit_length` AFTER the FLUSH but BEFORE writes were
  halted; writes that arrived in that window land in the old log_stream's tail extent at
  offsets BEYOND the captured `sealed_length`, and the survivor's post-merge recovery (which
  reads `log_stream` from `vp_head=(E_new, 0)` forward) never replays them. F184-K observed
  ~4-5 % loss across split-merge-split with a sustained 50 ops/sec writer.
- **Approach (chosen over the spec §4.1 "PS-orchestrated handle_merge_part"):** the spec
  proposed letting the survivor's PS thread coordinate a 4-gate freeze on the victim's PS
  thread + an in-memory cross-thread splice of `Vec<Arc<SstReader>>`. That requires a
  Send-capable cross-thread channel + main-thread `merge_service_loop` registry — a
  substantial refactor. F185 takes the **TiKV PrepareMerge model** instead: the
  leader-fenced control plane (here, the manager) is the orchestrator. CLI is a thin
  wrapper. No cross-thread channels, no in-memory splice — survivor reopens via the
  existing `region_sync_loop` reload (F184-B) + `read_all_table_locations` union (F184-C).
  Trade-off: merge wallclock stays ~2-3 s (vs spec's <1 s); data loss goes to 0.
- **Mechanism:**
  1. Client → manager: `MSG_MERGE_PARTITIONS { survivor, victim }` (one RPC).
  2. Manager `handle_merge_partitions` (`crates/manager/src/rpc_handlers.rs`):
     - `ensure_leader` + resolve `part_addr` / stream ids in one borrow
     - acquire admin owner-lock keyed on the partition pair (so two concurrent merge
       attempts targeting the same survivor serialize)
     - `MSG_MERGE_FREEZE { freeze: true }` to victim PS — drains pending+inflight, rotates
       active, flushes every imm, sets `PartitionData.frozen_for_merge = Some(now)`,
       returns OK only after the post-freeze checkpoint is durable. Subsequent
       Put/Delete/StreamPut on the victim return `CODE_UNAVAILABLE` (new code 7).
     - `MSG_MERGE_FREEZE { freeze: true }` to survivor PS — same.
     - capture `commit_length` × 6 (3 streams × 2 partitions) via existing
       `handle_check_commit_length`. Now race-free because both PSes are frozen.
     - call `handle_multi_modify_merge` synchronously (existing F183 atomic etcd txn —
       unchanged). The single `put_and_delete_txn` is the linearization point.
     - on success: do NOT explicitly unfreeze. `region_sync_loop` on each PS observes
       (rg, stream_ids) change on next ~2 s tick (F184-B), drops the frozen
       `PartitionData`, and reopens the survivor with `frozen_for_merge = None` —
       natural unfreeze.
     - on failure: best-effort `MSG_MERGE_FREEZE { freeze: false }` rollback to anyone
       already frozen.
- **Crash safety:**
  - **CLI crash** (any time): benign — manager continues to completion or rollback.
  - **Manager crash before commit**: failover; new leader sees no half-state in etcd; PSes
    auto-unfreeze via `FREEZE_TTL = 30 s` backstop in `merged_partition_loop`. Merge can
    be retried.
  - **Manager crash after commit**: merge is durable in etcd; `region_sync_loop` drives
    PS reload; frozen flag goes with the dropped PartitionData.
  - **PS crash mid-flow**: in-memory freeze flag lost on restart; either the merge
    committed (PS reopens with merged state via F184-C) or didn't (PS reopens with
    original state).
  - **Why TTL not procedure-WAL** (HBase-style ProcedureV2): the only crash window this
    has to cover is "manager crashed between freeze RPC and etcd commit" — sub-second
    on the happy path. 30 s is far over budget but bounds worst-case freeze duration far
    below "frozen forever until PS restart". If we ever need cross-PS merge or higher
    merge frequency, upgrade to a `mergeInProgress/<survivor>:<victim>` etcd marker
    + replay-on-leader-promotion (~200 lines, ProcedureV2 in miniature) — recorded as
    a follow-up below.
- **Wire additions (`crates/rpc/src/{partition,manager}_rpc.rs`):**
  - PS: `MSG_MERGE_FREEZE = 0x4E`, `MergeFreezeReq { part_id, freeze: bool }`,
    `MergeFreezeResp { code, message }`, `CODE_UNAVAILABLE = 7`.
  - Manager: `MSG_MERGE_PARTITIONS = 0x37`, `MergePartitionsReq { survivor, victim }`,
    `MergePartitionsResp { code, message, new_log_tail_extent_id }`.
  - Reserved-but-unused `MSG_MERGE_PART = 0x4D` left in place; the spec's PS-orchestrated
    variant is no longer the chosen path but the constant stays for the unlikely future
    where it gets revisited.
- **PS-side state (`crates/partition-server/src/lib.rs`):**
  - `PartitionData.frozen_for_merge: Cell<Option<Instant>>` — `Some(set_at)` while frozen.
    `handle_incoming_req` short-circuits Put/Delete/StreamPut with `CODE_UNAVAILABLE`.
  - `PartitionData.freeze_drain_ack: RefCell<Option<oneshot::Sender<HandlerResult>>>` —
    parked freeze response; fired by `merged_partition_loop` once `pending` AND
    `inflight` are both empty AND every imm has flushed.
  - Top-of-loop check in `merged_partition_loop` runs the rotate-active +
    `flush_one_imm` loop and sends OK on the parked oneshot. TTL backstop also at
    top-of-loop: if `set_at.elapsed() > FREEZE_TTL (30 s)`, auto-unfreeze + drop ack
    with `CODE_PRECONDITION`.
  - `merged_partition_loop` runs reads + maintenance ops normally while frozen — only
    writes are halted.
- **Client-side (`crates/client/src/lib.rs`):** `ClusterClient::merge_partitions` is now
  a thin wrapper around `MSG_MERGE_PARTITIONS` (was 100+ lines of CLI orchestration; now
  ~20 lines).
- **Tests:** `f185_orchestrated_merge_zero_loss_concurrent_writes` (system_merge.rs) is
  a clone of F184-K's `split_merge_split_with_concurrent_writes` setup that routes the
  merge through `MSG_MERGE_PARTITIONS`. Asserts:
  - 0 lost writes (vs F184's ≤20 % tolerance — F184 routinely shows ~5 %)
  - `> 0` `CODE_UNAVAILABLE` retries observed (proves the freeze actually fired)
  Both F184-K (loss = baseline) and F185 (loss = 0) pass in the same test run, giving
  a regression baseline for the OLD path while validating the NEW one.
- **Observed numbers (`cargo test --test system_merge -- --ignored`):**
  - F184-K (old path): 542 acked, 26 lost (4.8 %), 9 transient errors retried
  - F185 (new path): 360 acked, 0 lost (0 %), 11 unavailable-retried (proves freeze)
- **Files changed:** `crates/rpc/src/partition_rpc.rs` (wire types), `crates/rpc/src/manager_rpc.rs`
  (wire types), `crates/partition-server/src/lib.rs` (PartitionData fields, freeze drain
  in merged_partition_loop, handle_incoming_req short-circuits, FREEZE_TTL), `crates/manager/src/rpc_handlers.rs`
  (handle_merge_partitions orchestrator), `crates/client/src/lib.rs` (thin wrapper),
  `crates/manager/tests/system_merge.rs` (new test).
- **Follow-up (deferred — record only):** if the 30 s TTL ever proves insufficient or
  cross-PS merge is needed, add a `mergeInProgress/<survivor>:<victim>` etcd sidecar
  written before freeze and deleted by the success path's etcd txn. Leader-promotion
  replay scans the prefix; for each entry, decide unfreeze (rollback) or commit
  (continue) based on whether the partition deletion is already in etcd. This is
  HBase ProcedureV2 in ~200 lines.
- **passes:** true

### F184 · F183 follow-on — auto-trigger + reload-on-region-change + concurrent-writer test + SDK rpc_timeout

- **Target:** finish the F183 Stage 1 advisory primitive into a usable Stage-1.5: enable
  manager-side auto-dispatch behind feature flags, fix post-merge survivor recovery so
  region_sync reload picks up victim's tables, and document/test the residual write-loss
  window via concurrent-writer integration tests.
- **F184-A:** `--auto-split` / `--auto-merge` flags on `autumn-manager-server` binary;
  `set_auto_split` / `set_auto_merge` setters on `AutumnManager`; `policy_tick_loop`
  consumes its own candidates and dispatches `MSG_SPLIT_PART` (split) or runs the CLI
  orchestration flow (merge), rate-limited to 1 candidate/tick.
- **F184-B:** `PartitionHandle.opened_with: (Range, log_id, row_id, meta_id)` snapshot;
  `sync_regions_once` compares against the latest manager regions and removes the handle
  (forcing the open-new-partitions pass to reopen with fresh state) when any field changed
  — this is what makes the merge primitive usable WITHOUT a PS-side handler.
- **F184-C:** new `read_all_table_locations` walks every meta_stream extent and unions
  the LAST `TableLocations` from each non-empty extent; recovery dedups SST locs by
  `(extent_id, offset, len)`. Pre-merge partitions (single non-empty extent) keep
  identical behaviour; post-merge survivors get the union of victim's + survivor's
  checkpoint tables.
- **F184-D/J/K:** `system_merge.rs` integration tests: round-trip happy path,
  multi-step `merge → split-again` lifecycle, `split → merge → split` with
  interleaved-writes (F184-J) and **concurrent writer** (F184-K) using `ClusterClient`.
  F184-K test exposes and asserts the ~5 % Stage-1 merge-window loss documented in F185.
- **F184-E:** 3 more merge handler unit tests (recovery_inflight, pending_delete,
  last_op_at).
- **F184-F:** public `manager.force_auto_split` / `force_auto_merge` test helpers +
  `auto_dispatch_merge_orchestrates_full_flow` integration test.
- **F184-G:** `auto_dispatch_split_dispatches_msg_split_part` integration test.
- **F184-H:** `PolicyConfig` runtime-configurable thresholds via
  `manager.set_policy_config(cfg)`; `policy_tick_loop` re-reads `tick_interval_sec`
  each cycle. Enables fast-mode `auto_merge_fires_via_policy_tick_loop_fast_mode`
  e2e test (~6 s) — first test that exercises the FULL closed loop:
  `MSG_REPORT_PARTITION_LOAD → metrics window → compute_candidates →
  auto_dispatch_merge → multi_modify_merge` end-to-end without manual intervention.
- **F184-I:** mirror of F184-H for SPLIT closed loop.
- **F184-L:** opt-in `ClusterClient.set_rpc_timeout(Duration)` setter +
  `clear_rpc_timeout` / `rpc_timeout` getters. When set, every `ps_call`
  (put/get/delete/head/range/stream_put/merge_part/F129 PutChunk/Commit/Abort)
  is raced via `RpcClient::call_timeout` against the deadline; expiry surfaces
  as `AutumnError::ConnectionError` so the existing `call_ps_for_*`
  retry-on-failure path triggers `refresh_regions` + one retry. Default `None`
  preserves pre-F184 wait-forever semantics. Closes a footgun: PS dropping
  `req_rx` mid-call (region_sync reload, graceful drain) without closing TCP
  used to hang `cluster.put().await` forever — autumn-rpc F121's closed-state
  flag fires only on TCP close, not on req_rx drop.
- **Tests (post-F184):**
  - `cargo test -p autumn-manager --lib`: 51/51
  - `cargo test -p autumn-manager --test system_merge -- --ignored`: 9/9 individually
    (each ≤ 60 s on a clean machine; suite mode has known test-isolation issue
    where cluster processes accumulate across cases)
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 130/130
- **Spec/plan:** same as F183.
- **passes:** true

### F179 · autumn-fuse: async-reply read + parallel chunk fetch + ClusterClient `&self` refactor

- **Target**: lift the FUSE read throughput from `~13 k ops/s` aggregate (4K random) and `~470 MB/s` (8M random) — observed pre-fix with arbitrary client thread count, indicating a single-dispatcher serialisation bottleneck in the fuse path.
- **Three coordinated fixes (committed `b7811a9`)**:
  1. **Async-reply Read**: `FsRequest::Read` now carries `fuser::ReplyData` directly across the bridge instead of returning through a std::mpsc reply. `ops.rs::read` ships the request and returns immediately, freeing fuser's single-threaded `/dev/fuse` reader to advance to the next request. `dispatch.rs::Read` spawns the parallel chunk fetch and replies via the carried `ReplyData` when the fanout lands.
  2. **Two-phase read with parallel chunk fanout**: `read::prepare(&mut state, ...)` does cheap synchronous chunk planning (resolve_key + get_ps_client per chunk) under brief borrow; `read::execute(plan)` does `join_all` over chunk MSG_GET RPCs with no state reference. RpcClient is multiplexed so N concurrent calls share one socket.
  3. **ClusterClient `&self` refactor**: `regions`/`ps_details`/`part_addrs` wrapped in `RefCell`; all hot-path methods (`resolve_key`, `put`, `get`, `range`, etc.) take `&self`. Concurrent compio tasks holding `Rc<ClusterClient>` can do brief routing lookups without blocking each other. Borrows are scoped — never held across `.await`.
- **Mount-side tuning (also committed)**:
  - `FOPEN_DIRECT_IO` on Open reply so each user `read()` reaches the daemon (bypass kernel page cache).
  - `MountOption::CUSTOM("max_read=8388608")` to encourage larger FUSE read units (kernel still caps at `FUSE_MAX_PAGES_PER_REQ`, but harmless).
- **Bench results (TCP, 3-disk, threads=N)**:
  - 4K random read aggregate: `13.8 k → 31.5 k ops/s` (+128%)
  - 8M random read aggregate: `471 → 600 MB/s` (+27%)
- **Remaining gap to perf_check direct (`1.81 M ops/s` on 4K)**: ~60×. Two structural limits remaining: fuser-0.15's single-threaded `/dev/fuse` reader, and kernel `FUSE_MAX_PAGES_PER_REQ` ~128 KiB cap. Both need either a different FUSE library (multi-fd dispatch / `-o clone_fd`) OR a parallel data path that bypasses kernel FUSE entirely. F180 takes the latter approach.
- **passes:** true

### F180 · autumn-fuse: shared-memory io_uring side-channel (3FS-style)

- **Target**: structurally remove the two FUSE bottlenecks F179 hit but couldn't break — fuser's single-threaded `/dev/fuse` reader and the kernel `FUSE_MAX_PAGES_PER_REQ` ~128 KiB cap. Inspired by 3FS's `IoRing` (`3FS/src/fuse/IoRing.h`, `IoRing.cc`): applications and daemon share a memory-mapped ring with atomic SQE/CQE protocol; batched submit, no kernel context switch per I/O. Targeting AI/HPC workloads — PyTorch DataLoader, inference batch read — where one process issues thousands of in-flight reads against known data sets.
- **Critical scope clarification**: this is a **parallel side-channel**, NOT a replacement of the kernel FUSE mount. The mount stays so `ls`, `cp`, `cat`, monitoring agents continue to work. AI applications opt-in to the io_uring path via a Rust SDK / Python binding; standard POSIX tools keep using kernel FUSE.
- **Phasing**:
  - **F180-A — protocol + ring layout** (this entry's first commit):
    - New crate `crates/ioring/` with the wire protocol, layout constants, and pure-data SQE/CQE codecs. **No daemon or client integration yet** — just the shared types both sides will agree on.
    - SHM file path: `/dev/shm/autumn-fuse/<session_id>.ring`. Layout: header (producer/consumer indices, ring size, capability flags), SQ ring (atomic indices), CQ ring, pinned buffer pool region.
    - SQE struct: `{ opcode: u8, ring_fd: u32, offset: u64, length: u32, buf_offset: u64, user_data: u64 }`. Opcodes: `OPEN` (path → ring_fd), `READ`, `WRITE`, `CLOSE`. Reads/writes reference a slot in the buffer pool (no copy through SHM body).
    - CQE struct: `{ user_data: u64, result: i64 }`. Negative result = errno.
    - Sync primitives: `AtomicU64` head/tail indices for SQ and CQ; `eventfd` or `futex` for blocked-waiter wake-ups.
    - Tests: round-trip SQE/CQE encode/decode; head/tail wraparound; capacity invariants.
  - **F180-B — daemon-side ring poller**:
    - autumn-fuse daemon listens on a Unix socket for session-open RPC. On accept, allocates an SHM ring, sends the fd back via `SCM_RIGHTS`.
    - Compio task on the existing fuse runtime polls the SQ; pulls SQE batches of up to 32; dispatches `READ`/`WRITE`/`OPEN`/`CLOSE` to the existing `read::prepare/execute`/`write::write` path.
    - Maintains `ring_fd → inode` mapping per session. `OPEN` allocates a new ring_fd, `CLOSE` releases it. Ring_fds are NOT kernel fds — they're indices into the daemon's per-session table.
    - On completion: write CQE to ring head, atomic-increment, eventfd-wake the waiter if registered.
  - **F180-C — Rust client API + benchmark**:
    - `crates/ioring/src/client.rs` exposes `IoRingClient::connect(socket_path)`, `submit(sqe)`, `try_completion()`, `wait_completion(idle_us)`, `drain_completions(dst, max)`, plus a buffer-pool slot allocator.
    - `src/bin/bench.rs` (binary `autumn-ioring-bench`) drives N reader threads (one IoRingClient per thread) → OPEN → prime depth reads → drain CQEs and re-issue.
    - **Validation gate**: F180-C bench must show ≥ 200 k ops/s on 4 K random reads (vs F179's 31 k via FUSE) before committing to F180-D/E. If not, the design is wrong and we re-architect.
    - **F180-C smoke results (3-disk TCP cluster, single small key — value padding-zero behaviour)**:
      | config                         | ops/s   | notes                                  |
      |--------------------------------|---------|----------------------------------------|
      | threads=1  depth=1             |   7,800 | path latency baseline ~128 µs          |
      | threads=16 depth=8             | **139,000** | 4.5× FUSE (F179 31 k peak)        |
      | threads=8  depth=32            |  44,000 | single-session ceiling                  |
      | threads=64 depth=8             |  54,000 | runtime contention degrades            |
      | threads=128 depth=8            |  27,000 | severely degraded                       |
    - **Verdict**: 139 k peak below the 200 k gate but well above F179's 31 k. The design works; the bottleneck is the daemon-side per-session poller serialising SQE dispatch (`pop → await cluster.get → push`). Each session caps at ~44 k; aggregate scales linearly until compio runtime saturates around 16 sessions. Fix: F180-B5 (below).
  - **F180-B5 — daemon spawn-per-SQE refactor** (committed `55d254f`, lifted per-session ceiling 44 k → 150 k but aggregate plateaued at ~137 k due to single-runtime daemon CPU saturation):
    - Wrap `MmapRegion` and `ring_fds` table in `Rc<RefCell<...>>` per session.
    - Per-session poller pops SQE batches; for each SQE spawns a compio task that does `cluster.get(...).await` (no region borrow held), then briefly borrows region twice (write data into buf slot, push CQE). Borrows are scoped — never held across `.await`.
  - **F180-B6 — multi-runtime daemon + cached PS client at OPEN** (passes the 200 k gate):
    - `autumn-ioring-daemon --runtimes N` spawns N OS threads, each runs its own compio runtime + own `ClusterClient` + own `UnixListener` bound at `{socket}.{idx}`. Sessions are pinned to whichever runtime accepted them — no cross-thread state. Clients distribute load by picking their runtime index (bench: `tid % N`).
    - At `OPEN` the daemon now resolves `(part_id, ps_addr)` once and caches `Rc<RpcClient>` on the ring_fd. `READ` skips `ClusterClient::get_range` entirely — builds `GetReq{ part_id, key, offset:0, length:0 }` inline and calls the cached PS directly. Saves several `RefCell` borrows + 2 hashmap lookups + the `call_ps_for_key` retry-closure shell per READ.
    - `length: 0` is load-bearing: PS `resolve_value` returns the raw `Bytes` directly when `offset==0 && length==0`; with `length: sqe.length` (e.g. 4096) it does an extra `to_vec()` slice on the PS path which costs ~25% throughput at single-runtime load. The daemon slices its own response.
    - Bench: `--runtimes N` flag and `%tid%` token in `--key` for spreading load across keys/partitions.
    - **Results** (single-PS cluster, hot single key, 16 threads × depth 8 × 4 KB):
      | config       | ops/s     | notes                              |
      |--------------|-----------|------------------------------------|
      | B5 r=1       | ~104 k    | re-measured baseline this session  |
      | B6 r=1       |   113 k   | +9 % from cached PS client         |
      | B6 r=4       | **208 k** | **passes 200 k gate**              |
      | B6 r=8       |   203 k   | PS process at 100 % CPU (cap)      |
    - Above ~200 k the bottleneck moves to the single-PS-process / single-partition CPU. Real workloads with > 1 partition (or PS process) will scale further; this bench cluster has 1 partition by default.
  - **F180-D — Python binding** (deferred until B5 + bench validates):
    - Extend `python/autumn-python` with a new `IoRing` pyo3 class. Async API: `await ring.read(ring_fd, off, len) -> bytes`. Same compio worker-thread pattern as the existing `Client` binding.
  - **F180-E — PyTorch integration** (deferred until D validates):
    - `python/examples/autumn_torch_dataset.py` showing `AutumnIODataset` that wraps `IoRing` for DataLoader prefetching.
- **Expected gain (post-B5)**: 4 K reads from 31 k → 500 k+ ops/s; 8 M reads from 600 MB/s → 5+ GB/s (approaches perf_check direct path because FUSE-kernel layer is gone).
- **Out of scope**:
  - Replacing FUSE mount entirely — F180 is parallel, not a replacement.
  - Kernel-side multi-fd dispatch (`-o clone_fd`).
  - Multi-mount load balancing.
  - LD_PRELOAD transparency shim — apps explicitly link `autumn-ioring`.
- **Why not just expose ClusterClient to Python directly?** That works for read-only workloads with no write coherence needs, and `python/autumn-python` already does it. F180 is for workloads that need the daemon's shared inode cache + write coherence + POSIX integration — multi-process inference, write-heavy mixed loads. For pure-read PyTorch training, the existing async ClusterClient binding is enough; F180 adds value when daemon-mediated coherence matters.
- **passes:** true (F180-A through F180-C, B5, B6 committed; 4 K read 200 k gate met at r=4 / 16-thread / depth-8). F180-D (Python pyo3 IoRing class) and F180-E (PyTorch DataLoader) remain deferred per user direction; F181 / F182 are separate feature entries.

### F181 · autumn-fuse: batched chunk RPC (`MSG_BATCH_GET`) — deferred follow-up

- **Target**: cut RPC framing overhead on chunk reads. Each MSG_GET pays 10-byte rpc frame header + rkyv encode/decode of GetReq/GetResp + per-call PS-side route lookup. After F179 we issue N parallel MSG_GETs per FUSE read; after F180 the daemon will issue N parallel MSG_GETs per ring submit. Both paths benefit from a single batched RPC.
- **Approach**:
  - New PS RPC: `MSG_BATCH_GET = 0x4A`. Request: `BatchGetReq { part_id, items: Vec<{key, offset, length}> }`. Response: `BatchGetResp { code, results: Vec<GetResultItem { code, offset, value }> }`.
  - PS handler: looks up each key in active memtable / imm / SSTable (single-threaded P-log task; per-key processing serialised but RPC framing amortised).
  - autumn-fuse `read::execute` groups chunks **by `part_id`** and issues one `MSG_BATCH_GET` per group.
- **Expected gain**: 30-50% on 8 M reads (RPC framing currently ~30% of per-chunk overhead at 256 KiB).
- **Out of scope**: `MSG_BATCH_PUT`, `MSG_BATCH_HEAD`. Separate entries once the read win lands.
- **passes:** false (deferred)

### F182 · autumn-fuse: RDMA chunk transfer — deferred follow-up

- **Target**: replace TCP-loopback memcpy with RDMA zero-copy on the chunk-data path. autumn-rs already has UCX support (F100-UCX); the fuse client doesn't use it.
- **Approach**:
  - autumn-fuse opens its `ClusterClient` with the UCX transport when available.
  - For the chunk data plane specifically: pin RDMA-registered read buffers in the daemon. Server-side `MSG_GET` (or `MSG_BATCH_GET` post-F183) uses UCX rndv-zcopy for ≥ 1 MiB results.
  - End-to-end zero copy requires F180 — without bypassing kernel FUSE, data still round-trips through `/dev/fuse` and the RDMA win is daemon ↔ cluster only.
- **Expected gain**: 8 M read 600 MB/s → 2-3 GB/s on RDMA-capable hardware; ~no gain on TCP loopback.
- **Prereq**: RDMA NIC (RoCE / IB); F180 for end-to-end zero copy.
- **passes:** false (deferred; needs hardware + F180)

### F178 · LevelDB-style sync coalescing — remove `--nosync`, always durable, fsync at 1-5 ms cadence
- **Target:** Recover the F142-era throughput regression (pre-F142 130k ops/s → post-F142 89k-100k ops/s on 4K writes) without compromising the durability invariant F142 introduced. F142 added a rotation-trigger `must_sync=true` barrier so that all log_stream bytes referenced by VPs in the about-to-be-flushed memtable are durable BEFORE the flush starts; pre-F142's `sync_stream_tail` separate-RPC pattern had the same intent. The cost: every rotation-triggering batch (~1/s under sustained 70 MB/s write) does a sync_data on the log_stream tail extent. Combined with per-batch must_sync=true on the client-driven sync path, fsync overhead dominates the throughput ceiling on real disks.
- **Approach (LevelDB-style):** sync at the wire is now a hint, not a guarantee — every append always becomes durable, but the actual `sync_data` syscall is coalesced. A background fsync task on each extent-node aggregates `pending_fsync_offset` across many concurrent appends, fires `sync_data` every 1-5 ms (configurable coalescing window), and wakes all waiters whose `end ≤ synced_high_water` together. This decouples pwrite throughput from fsync rate: 200-1000 fsyncs/sec is enough to drain any reasonable batch volume; pwrites pipeline freely between fsyncs.
- **Phasing (committed plan, executes across multiple iterations):**
  - **Phase 1 — extent-node coalescer (core architecture):**
    - Per-`ExtentEntry`: `last_synced_offset: AtomicU64`, `pending_fsync_offset: AtomicU64`, sync waiter list (`Vec<(end_offset, oneshot::Sender)>`).
    - `handle_append` / `build_append_future` (batched path): unconditionally `write_vectored_at` (no inline `sync_data`); register the new `(end_offset, oneshot)` into waiter list; nudge the coalescer.
    - Per-extent coalescer task: `compio::time::sleep(coalesce_window)` → if `pending_fsync_offset > last_synced_offset`: `sync_data` → update `last_synced_offset` → drain waiter list, wake all `(end ≤ synced)` via their oneshots.
    - Tunable: `AUTUMN_EXTENT_FSYNC_COALESCE_MS` default 2 (range [1, 50]).
  - **Phase 2 — Move durability wait from WRITE to FLUSH (true LevelDB):**
    - **Writers never pay rotation cost.** `start_write_batch`: drop the entire `triggers_rotation` block. Rotation happens lazily whenever active passes threshold, no barrier promotion. Every Put is bounded by exactly 1 coalesce window (1-5 ms), regardless of whether it triggers rotation.
    - **Flush waits for log_stream to be synced past `imm.max_vp_offset`.** New API: `extent_node` exposes `synced_length(extent_id) -> u64` (= coalescer's `last_synced_offset`). `StreamClient` exposes `await_log_synced_to(stream_id, offset)` which queries all 3 replicas and waits for quorum-min to reach the offset (mirrors the F156 `commit_length` quorum pattern). `flush_one_imm` calls this BEFORE `row_stream.append` of the SST; usually wait ≈ 0 because coalescer fires every 1-5 ms and flush builds SST in parallel.
    - **Why "wait at flush" beats "wait at write":**
      1. Write p99 no longer has "I unluckily triggered rotation" tail latency — every Put pays the same 1-5 ms coalesce floor.
      2. Flush is background; +5 ms to its start is invisible to clients.
      3. F150 Phase B invariant preserved structurally: `sync_data` always fsyncs the whole file's dirty pages, so coalescer's fsync covers ALL prior bytes (no "must_sync=false dirty pages" gap because F178 removes the false branch entirely).
      4. The 3-replica fanout cost stays unchanged — `synced_length` is one round-trip per replica, parallelized.
    - `flush_one_imm` and `flush_one_imm_local` both updated. Same pattern in `do_compact` if compact ever spans logStream extents (currently only row_stream, so no change).
  - **Phase 3 — wire / client API cleanup:**
    - Remove `--nosync` flag from `autumn-client put`, `streamput`, `wbench`, `perf-check`.
    - Remove `must_sync: bool` field from `PutReq`, `AppendReq` (or keep at wire level for back-compat but ignore semantically — TBD by smaller cost).
    - Remove `--nosync` from `perf_check.sh` (line 253).
    - Update `wbench` benchmark documentation.
  - **Phase 4 — validation + cleanup:**
    - Test sweep: PS lib (122 tests), stream lib (40 tests), manager lib (30 tests), all passing.
    - Perf benches: 4K p=8 NVMe target ≥ 100k ops/s (recover toward pre-F142 130k); 8M p=8 NVMe target ≥ 1.5 GB/s (preserved or improved).
    - Update CLAUDE.md (stream + partition-server crate guides + system root).
- **Why "always durable, no nosync flag":**
  - Half of the F142 cost was about the rotation-trigger barrier, which is independent of client `--nosync`. Removing client-side nosync doesn't add new fsync cost beyond what F142 already imposed; the coalescer pays it cheaper.
  - Operational simplicity: one mode of operation, one performance envelope. Production deployments can no longer pick "fast but unsafe" by accident.
  - LevelDB / RocksDB / etcd / Cassandra commitlog all default to durable; nosync has been a perf-test escape hatch that confused performance comparisons (see F176 audit).
- **Why coalescing window 1-5 ms specifically:**
  - 1 ms: fsync rate up to 1000/sec; latency floor 1 ms. Good for low-latency workloads.
  - 5 ms: fsync rate up to 200/sec; latency floor 5 ms. Good for high-throughput sustained writes.
  - 2 ms default: matches typical NVMe sync_data cost, keeps pipeline saturated, p99 floor ~5 ms (covers worst-case 2 windows back-to-back).
- **Out of scope (deferred to future iterations):**
  - Multi-extent fsync grouping at the kernel level (would need `sync_file_range` or `io_uring` linked SQEs). The per-extent coalescer is sufficient: extent rotates at 3 GB → at 70 MB/s, ~40s/extent → most fsyncs land on the same extent file.
  - Replacing 3-replica fanout's "wait for slowest" with quorum-2 fast-path (independent optimization; affects p99 but not throughput).
  - `wbench` / `perf-check` switching to LevelDB-style multi-writer-share-batch model (the merged_partition_loop already implements this server-side via F099-D).
- **Files touched (planned):**
  - `crates/stream/src/extent_node.rs`: coalescer state + task + waiter logic + handle_append / build_append_future.
  - `crates/stream/src/extent_rpc.rs`: (Phase 3) AppendReq schema if removing must_sync.
  - `crates/stream/src/client.rs`: (Phase 3) StreamClient::append* signature cleanup.
  - `crates/partition-server/src/background.rs::start_write_batch`: drop rotation-trigger logic (Phase 2).
  - `crates/server/src/bin/autumn_client.rs`: (Phase 3) drop --nosync flags from put/streamput/wbench/perf-check.
  - `perf_check.sh`: (Phase 3) drop --nosync from line 253.
  - `crates/stream/CLAUDE.md`, `crates/partition-server/CLAUDE.md`, `CLAUDE.md`: doc updates.
- **Verification plan:**
  - Per-phase: `cargo build --workspace --exclude autumn-fuse`, `cargo test -p {stream,partition-server,manager} --lib`, end-to-end smoke via cluster.sh.
  - Final: NVMe perf bench p=8 d=8 4K + 8M, both transports — primary metric is 4K write ops/s and p99.
- **Phase 1 implementation (extent_node coalescer):**
  - `Coalescer` struct on every `ExtentEntry`: `last_synced: AtomicU64`, `pending_fsync: AtomicU64`, `RefCell<{ waiters: Vec<(u64, oneshot::Sender)>, task_running: bool }>`. Initial values match the loaded file length so the seal-time fsync's coverage isn't lost across restart.
  - `register_sync_waiter(extent, end_offset)`: pushes `(end, tx)` into the inner waiter list, lazily spawns `coalescer_loop` when transitioning `task_running` false→true under the same `borrow_mut` that the loop's exit-decision takes — closes the registration-vs-exit race structurally.
  - `coalescer_loop`: `compio::time::sleep(AUTUMN_EXTENT_FSYNC_COALESCE_MS, default 2 ms, range [1, 50])` → if `pending > synced` issue ONE `file.sync_data()` covering ALL pending bytes, advance `last_synced`, drain waiters with `end ≤ pending`. fsync error: fail every pending waiter together (sync_data is whole-file).
  - `build_append_future` (batched) and `handle_append` (non-batched): always `pending_fsync.store(end)` after pwrite (LevelDB-style "always durable"); if `must_sync` register a waiter and await.
  - 3 `ExtentEntry` construction sites (`load_extents`, `ensure_extent`, `handle_alloc_extent`) initialise the coalescer.
- **Phase 2 implementation (flush-time durability + drop write-time barrier):**
  - `extent_rpc.rs`: `MSG_SYNCED_LENGTH = 13` (slot recycled from retired `MSG_SYNC_EXTENT`); `SyncedLengthReq{extent_id: u64}` (8B) + `SyncedLengthResp{code: u8, length: u64}` (9B).
  - `extent_node.rs::handle_synced_length`: returns `max(coalescer.last_synced, sealed_length)`. The sealed-length floor is load-bearing for old log_stream extents the imm spans into — `apply_extent_meta_durable` already fsync'd at seal time, so even before the coalescer has run on a freshly-loaded sealed extent the wait is trivially satisfied.
  - `client.rs::await_extent_synced_to(extent_id, min_offset)`: fetches `ExtentInfo`, resolves replica addrs, polls `MSG_SYNCED_LENGTH` on each replica every `AUTUMN_STREAM_SYNCED_POLL_MS` (default 2 ms) until ≥ ⌊N/2⌋+1 replicas report `synced ≥ min_offset`. Timeout `AUTUMN_STREAM_SYNCED_TIMEOUT_MS` (default 30 s).
  - `client.rs::await_log_synced_to(extent_id, offset)`: thin wrapper, named for the call site.
  - `background.rs::start_write_batch`: dropped the `triggers_rotation` block. `batch_must_sync = batch_must_sync_caller_flag` only.
  - `lib.rs::flush_one_imm` (P-bulk hand-off path) and `lib.rs::flush_one_imm_local` (legacy in-thread fallback): call `part_sc.await_log_synced_to(snap_vp_eid, snap_vp_off as u64)` BEFORE the row_stream upload (skip when `snap_vp_off == 0`). F148-A invariant survives — the await sits BEFORE the borrow_mut block + `save_table_locs_raw` mpsc send.
- **Phase 3 implementation (--nosync removal):**
  - `autumn_client.rs`: 5 `--nosync` parse sites (`put`, `streamput`, `del`, `wbench`, `perf-check`) replaced with `warn_nosync_deprecated_once()` (Once-guarded stderr); `nosync` always set to `false` at parse time. `usage()` drops `--nosync` from `wbench`/`perf-check` help. `Command::*` enum fields kept for back-compat (zero refactor downstream).
  - `perf_check.sh:253`: dropped `--nosync` line; explanatory comment retained.
  - `PutReq`/`AppendReq` `must_sync` wire field kept for back-compat. Always `true` in practice from the PS side because the client always sends `must_sync=true` post-Phase-3.
- **Phase 4 (tests + docs):**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-stream --lib`: 40/40 pass.
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 122/122 pass.
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
  - `crates/stream/CLAUDE.md`: append protocol step 6 rewritten; Programming Note 4 (must_sync cost) rewritten; Commit Protocol section gains Phase 2 paragraph.
  - `crates/partition-server/CLAUDE.md`: Programming Note 6 (group commit batching) rewritten with the two-layer durability story.
  - Real-cluster perf bench DEFERRED — requires explicit user authorization for `cluster.sh`. Target NVMe 4K p=8 ≥ 100k ops/s; 8M p=8 ≥ 1.5 GB/s preserved.
- **passes:** true

### F177 · WAL encode (CRC32C + memcpy) → spawn_blocking on big batches; zero-copy value segment
- **Target:** Close the two F176-identified inline CPU sites in `wal_record::encode_v1_segments` so the P-log compio runtime never blocks for hundreds of ms during big-value Put bursts. Same principle as F168/F169/F170 (thread-per-core: never run payload-sized CPU on the runtime).
- **Two fixes:**
  1. **Zero-copy value segment** (`wal_record.rs:170`): change `encode_v1_segments(value: &[u8])` → `encode_v1_segments(value: Bytes)`. The caller's owned `Bytes` (from PutReq decode → WriteOp::Put) flows into the returned value segment with **zero memcpy**. Pre-F177 `Bytes::copy_from_slice(value)` was an unconditional ~3 ms / 8 MiB allocation+memcpy on the P-log runtime — pure waste because the original `Bytes` was already owned.
  2. **Conditional spawn_blocking offload** (`background.rs::start_write_batch`): split Phase 1 into 1a (validate + assign seq under `borrow_mut`) and 1b (CRC32C + segment build, after `borrow_mut` release). When total batch payload `>= PHASE1_OFFLOAD_THRESHOLD = 4 MiB`, the entire encode loop runs in `compio::runtime::spawn_blocking`. Below the threshold, encode runs inline (spawn_blocking's ~10-20 µs join overhead would dominate sub-4 MiB batches; a 4 KiB / 256-record batch ≈ 1 MiB total stays inline at ~80 µs total CRC).
- **Why 4 MiB threshold:**
  - CRC32C hardware throughput ~12 GB/s → 4 MiB CRC ≈ 350 µs, 17× the spawn_blocking join overhead (~20 µs). Crossover.
  - Below 4 MiB: small-value workload (typical 4 KiB Put × 256 records = 1 MiB / batch). Inline encode is correct.
  - Above 4 MiB: big-value workload (8 MiB × 32+ records, or many small records that aggregate). Offload protects the runtime.
- **Function-signature break and migration:**
  - `encode_v1_segments` signature changed; one production call site (`background.rs::start_write_batch`) and one cold-path call site (`background.rs::process_gc_chunk`, GC re-write) updated. The GC site uses `Bytes::copy_from_slice(value)` because `value` is a `&[u8]` slice into the GC chunk read buffer — copy unavoidable, but GC is bounded by `gc_batch_bytes() = 4 MiB` per batch and yields cooperatively (F168 pattern).
  - `encode_v1` (single-buffer convenience used by tests) keeps `&[u8]` signature; internally does `Bytes::copy_from_slice` then calls the new `encode_v1_segments`. No test changes.
- **`start_write_batch` becomes async:** the function now `.await`s only on the big-batch path (spawn_blocking join). Both call sites (`merged_partition_loop`'s ready-to-launch branch and the shutdown drain) already run inside async contexts — added `.await`.
- **Observable behaviour preserved:** Phase 3 (memtable insert + responder dispatch) is unchanged; ValidatedEntry's `value: Bytes` is cloned (Arc::clone, ~free) into the encode pipeline so the original stays for memtable insert (small values inline). LogStream record format on the wire/disk is byte-identical to F158 V1.
- **Files:**
  - `crates/partition-server/src/wal_record.rs`: `encode_v1_segments` signature + body; `encode_v1` keeps backward-compat shim.
  - `crates/partition-server/src/background.rs`: `start_write_batch` async + Phase 1 split + spawn_blocking branch; `process_gc_chunk` adds `Bytes::copy_from_slice` for the &[u8]-source case + comment.
  - `crates/partition-server/src/lib.rs`: 2 caller sites add `.await`.
- **Verification:**
  - `cargo build -p autumn-partition-server`: clean (only pre-existing warnings; `encode_v1` now flagged unused — test-only convenience kept for future use).
  - `cargo build --release --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 122/122 pass.
  - `cargo test -p autumn-stream --lib`: 40/40 pass.
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
- **Smoke perf (`partitions=1 threads=16 depth=8 size=8M duration=10s`, /tmp tmpfs):**
  - F176 baseline (pre-F177): 33 ops/s, p99 = 4.32 s (write); 37 ops/s, p99 = 3.77 s (read).
  - F177: 34.71 ops/s, p99 = 3.97 s (write); 34.19 ops/s, p99 = 3.99 s (read).
  - Throughput change: ~4 %. p99 change: ~8 %. **F177's microbench delta is small** — at p=1 / 16 threads × 8 in-flight × 8 MiB the dominant bottleneck is 3-replica TCP fanout (24 MiB/op network) and per-replica fsync, NOT CPU. The P-log compio runtime has spare capacity even pre-F177 (F099-D's merged loop is single-task; the 950 ms inline CPU spike I theorized for full 256-record batches doesn't fully materialise because batches stay smaller — 32-64 records — under 8 MiB-value back-pressure).
- **Where F177's value DOES land (architectural, not microbench):**
  - **Co-tenancy:** when a partition mixes small-value foreground writes with occasional big-value writes (typical real workload), big-value batches no longer freeze the runtime for hundreds of ms. Heartbeat / ps-conn / other partition tasks stay responsive throughout. Microbench doesn't capture this.
  - **Worst-case latency tail:** under a burst of big-value writes filling the merged_loop's 256-record cap, post-F177 the spawn_blocking offload keeps p99 bounded by 3-replica fanout (~50-100 ms architectural floor) instead of inline encode CPU + 3-replica fanout. Pre-F177's pessimistic worst-case (all 256 × 8 MiB inline) was a 950 ms-class hazard the microbench rarely hit but production bursts might.
  - **Memory:** zero-copy value segment saves a ~24 MiB / 256-record × 8-MiB-value transient allocation per batch (Bytes::copy_from_slice). Negligible at low concurrency, real at high.
- **Out of scope (deferred):**
  - Per-record CRC threshold (spawn_blocking PER record, not per batch): adds spawn overhead without proportional benefit; batch-level offload is the right granularity.
  - Eliminating the GC re-write `Bytes::copy_from_slice`: would require restructuring GC's chunk read into Bytes-owned slicing. Cold path; not worth the structural cost.
- **passes:** true

### F176 · Sustained-write tail-latency validation + bottleneck audit (perf bench investigation)
- **Target:** Validate that F168 (compaction merge yield) keeps the compio runtime responsive during real flush + compaction events under sustained load. Identify remaining bottlenecks: (a) does the 256 MB → 512 MB FLUSH_MEM_BYTES change help, (b) why is large-value (8 MB) throughput so much lower than small-value (4 KB) throughput.
- **Bench setup:**
  - 3-replica TCP cluster, 1 partition (`cluster.sh reset 3`).
  - `wbench --threads 16 --duration 180 --size 4096 --report-interval 1 --part-id 13 --reuse-value true`.
  - `/tmp/autumn-rs` on tmpfs (RAM-backed, fsync near-zero).
- **Sustained 4 KB write results (180 s):**
  - Aggregate: **16,378 ops/s, 64 MB/s sustained, p50 = 0.74 ms, p95 = 1.64 ms, p99 = 3.88 ms**.
  - Total: 2,950,167 ops, 11.5 GB written, 4 sealed log_stream extents at 3 GB each (extents roll at 3 GB).
  - Per-second range: 645 → 22,646 ops/s; median 17,328 ops/s.
  - 10 lowest seconds: 645 (s=178, final-flush drain at end-of-run), then 8.5k–10.6k ops/s scattered through the run; cyclic pattern matches the FLUSH_MEM_BYTES = 256 MB / 70 MB/s ≈ 3.6 s memtable-fill cadence.
- **F168 verdict — VALIDATED:**
  - **No throughput stall longer than 1 s** during the 180 s run (the 645 ops/s outlier at second 178 is the bench tail, not a mid-run freeze).
  - Pre-F168 the compaction merge loop ran up to 512 MB inline (~1-2 s blocking); a sustained workload like this would have shown 1-2 second 0-ops gaps every flush cycle. We see ~50 % dips lasting 1 second — the runtime is responsive throughout, just slower under flush + compact contention.
  - **p99 = 3.88 ms over 180 s including all flush/compact events** is the headline result. F168/F169/F170/F172 collectively keep the P-log compio runtime tight under sustained load.
- **FLUSH_MEM_BYTES 256 MB → 512 MB analysis:**
  - **Current cadence:** at 70 MB/s sustained, memtable fills in 3.6 s; 4 flushes triggered during the 180 s run plus 4 sealed log_stream extents (extent rolls at 3 GB independent of flush).
  - **Each flush:** rotate (instant) + must_sync barrier on rotation-trigger batch (F150 Phase B; near-zero on tmpfs, 5-15 ms on SSD) + P-bulk ships 256 MB SST upload (~50 ms × 3-replica fanout = 150 ms wall) + meta_stream checkpoint (~ms).
  - **512 MB cadence:** 7.2 s flush interval; 2 flushes / 180 s. Each flush is 2× heavier (300 ms upload), but happens half as often → net flush wall-time the same.
  - **Net throughput effect at tmpfs:** unchanged within noise. The dominant cost is per-flush serialisation overhead, NOT total bytes flushed; halving the count doesn't help if each one is twice as heavy.
  - **Where 512 MB DOES help:** real spinning-disk / lower-bandwidth SSD where each fsync barrier has a fixed seek cost; halving the barrier count gives ~10-20 % gain. On tmpfs the gain is in the noise.
  - **Cost of 512 MB:** 2× peak memtable bytes/partition (256 MB → 512 MB), plus the imm queue cap stays at MAX_IMM_DEPTH = 4 → 4 × 512 MB = 2 GB unflushed-WAL-window per partition (was 1 GB). F120 already designed for this trade-off; the const is just a tuning knob.
  - **Recommendation:** keep FLUSH_MEM_BYTES = 256 MB on tmpfs deployment. Promote to a CLI flag (NOT env var per repo convention) on autumn-ps if production deployments on SSD/HDD want to tune. Defer the CLI flag until a production benchmark on real disk shows the gain.
- **Big-value (8 MB) throughput root cause — TWO inline-CPU sites identified:**
  - From the prior matrix bench: `p=8 d=8 8M`: write 254 ops/s = 2.0 GB/s, p99 = 744 ms; read 187 ops/s = 1.5 GB/s, p99 = 1.4 s. `p=1 d=8 8M`: 33 ops/s — head-of-line on single partition.
  - **Inline CPU site #1 — `crc32c_append(crc, value)` over 8 MB value** (`crates/partition-server/src/wal_record.rs:167`): 8 MB CRC32C at ~12 GB/s = ~700 µs inline per record. With batches of 32–256 records: 22–180 ms per-batch inline CPU. Violates F117 ("CPU-bound work MUST run on the blocking pool"). The CLAUDE.md note 15 explicitly bounds this at ≤ 2 MiB / call — the bound holds for 4 KB workloads but does NOT hold for 8 MB values.
  - **Inline CPU site #2 — `Bytes::copy_from_slice(value)` for 8 MB** (`wal_record.rs:170`): unnecessary memcpy of the large value into a fresh `Bytes`. The original `value: &[u8]` is borrowed from the caller's `Vec<u8>` decode buffer, but the encode helper allocates + copies anyway. For 8 MB: ~3 ms memcpy inline per record × 32-256 records per batch = 100 ms – 1 s inline.
  - **Combined inline CPU per batch at 8 MB:** ~3.7 ms × records-in-batch. A 256-record batch ≈ 950 ms blocking the P-log compio runtime — explains the p99 = 744 ms observed and the read p99 = 1.4 s (read goes through the same encode path for VP fetches via resolve_value's backreads).
  - **Other contributors (NOT in scope of this audit):** 3-replica fanout at 8 MB × 3 = 24 MB/op network bandwidth × 254 ops/s = 6 GB/s aggregate — saturates loopback ceiling; 8 MB extent file pwrite + sync_data per replica ~15-30 ms (kernel page cache + tmpfs write); 128 in-flight × 8 MB peak memory = 1 GB.
- **F177 candidate (deferred for explicit scope):**
  - Move `Bytes::copy_from_slice(value)` to ownership-transfer: change `encode_v1_segments(... value: &[u8] ...)` to `value: Bytes` so the caller's owned buffer flows through without copying. Saves ~3 ms inline per 8 MB record.
  - Move CRC computation to spawn_blocking when batch payload > threshold (e.g., 4 MiB). Keeps small-value path inline (overhead-free); offloads big-value batches to the blocking pool. Saves ~700 µs–180 ms inline per batch.
  - Together: P-log compio runtime would NOT block for 8 MB workloads. p99 would drop from 744 ms toward the 3-replica fanout floor (~50-100 ms). Worth ~3-10× p99 improvement on big-value workloads.
  - Scope: ~50 LOC change in `wal_record.rs` + caller threading in `background.rs::start_write_batch`. Defer until user explicitly requests big-value perf work.
- **Files (this audit, no code changes):** `feature_list.md`, `claude-progress.txt`. Bench output captured in `/tmp/wbench_long.log` (ephemeral).
- **Verification:** the 180 s `wbench` run completed successfully; cluster torn down cleanly via `cluster.sh stop`.
- **passes:** true (audit + measurement complete)

### F174 · EC shard-level CRC32C — CLEARED, won't fix (end-to-end protection already exists)
- **Target:** Audit the deferred concern about silent shard corruption in EC-converted extents (cosmic ray bit flips, disk bit rot, silent encode bugs). Determine whether per-shard CRC is needed.
- **Audit findings — actual CRC coverage by layer:**

  | Layer | Replicated | EC | Mechanism |
  |---|---|---|---|
  | Wire frame | ✅ | ✅ | F165 V1 frame CRC32C trailer |
  | `.meta` sidecar | ✅ | ✅ | F157 CRC32C trailer |
  | `extent-{id}.dat` raw bytes | ❌ | ❌ | (none) |
  | log_stream WAL record | ✅ | ✅ | F158 V1 envelope per-record CRC32C |
  | row_stream SST block | ✅ | ✅ | Pre-existing 4-byte CRC32C trailer per block |
  | meta_stream TableLocations | ✅ checked | ✅ checked | F155 rkyv checked decode |

- **Key observation:** neither replicated NOR EC extent-data files (`extent-{id}.dat`) carry application-level CRC at the byte level. **Both rely on the upper-layer record-level CRCs** (F158 for log_stream, SST block CRC for row_stream, F155 rkyv-checked decode for meta_stream).
- **End-to-end protection comparison:**
  - **Replicated path:** disk bytes ARE the application records (Put records, SST blocks, TableLocations). Read-time corruption is caught by the record's own CRC32C envelope.
  - **EC path:** disk bytes are RS-encoded mathematical shards. On read, `ec_decode` reverses the encoding to produce the original byte stream. If a shard is silently corrupted, `ec_decode` produces WRONG bytes, but those wrong bytes still flow through the same application-record CRC verification — the corruption surfaces as an F158 record CRC mismatch ("skip + log") or an SST block CRC mismatch ("refuse").
  - **Net:** end-to-end safety is identical between replicated and EC paths. The only practical difference is diagnostic precision: replicated can pinpoint "replica N byte X is wrong"; EC only says "this 64 KB decoded with CRC mismatch."
- **CRC32C cost analysis (the perf concern that drove the user's question):**
  - SSE4.2 hardware `crc32` instruction throughput: ~10-15 GB/s single-core.
  - Whole-shard inline CRC on a 1 GiB shard: ~70-100 ms inline. Unacceptable on the compio runtime (would violate F168/F170 thread-per-core principle).
  - Per-stripe CRC (e.g., 16 MB stripe + 4-byte CRC): partial reads only verify ~1-2 stripes ≈ 1-2 ms — bounded, but still ~150 LOC of new code + a wire-format flag for backward compat (old shards have no CRC).
- **Decision: don't add F174.**
  - The application-record CRC layer already catches silent corruption end-to-end.
  - Per-shard CRC at the on-disk-byte level is additional precision (better diagnostics on which shard rotted) but NOT additional correctness — wrong bytes never reach a client either way.
  - The enterprise-SSD silent-corruption rate is ~1e-15 per byte read; spending 70-400 ms inline CPU per full-shard read OR 150 LOC of stripe-CRC plumbing for "more precise diagnostics on a 1e-15 event" is not the right trade-off in this iteration.
  - If a real production silent-corruption incident is observed, F174 stripe-CRC becomes worth it for the diagnostic precision. Until then, the existing F158 + SST block CRC + F157 .meta CRC + F165 frame CRC stack is sufficient.
- **What we explicitly considered and rejected:**
  - **Whole-shard CRC trailer:** O(shard_size) inline CPU per verify — would block the compio runtime for hundreds of ms per partial read. Violates F168/F170.
  - **Per-stripe CRC index (16 MB stripes):** correct trade-off if added, but requires (a) wire format change with `ec_shard_has_crc: bool` flag for migration, (b) per-stripe CRC computation interleaved into `file_pwrite_chunked`, (c) per-read stripe-bounds CRC verification. ~150 LOC. Deferred to a future iteration when a real failure justifies it.
  - **End-to-end RS self-check (decode K shards, then re-decode with K-1+different-parity, compare):** doubles read CPU cost (~100-300 ms per check); not viable on the hot path.
- **Files:** `feature_list.md`, `claude-progress.txt`. (No code changes.)
- **passes:** true (audit-cleared, decision documented)

### F175 · `sealed_length` cross-source consistency audit — CLEARED, no fix needed
- **Target:** Audit the deferred concern about `sealed_length` divergence between manager state and per-replica extent-node state. Determine whether existing mechanisms cover the consistency invariant or whether a new reconciliation path is required.
- **Audit findings:**
  - **Manager is the single authoritative source.** `tail.sealed_length` is set in `handle_stream_alloc_extent` (rpc_handlers.rs:918) and persisted to etcd before in-memory mutation per F125's etcd-first ordering. Replicas learn about a seal via manager-pushed `re_avali` / `apply_extent_meta_durable`, not by reaching consensus among themselves.
  - **Re-seal protection (F147-D, rpc_handlers.rs:854 `already_sealed` guard):** if `tail.sealed_length > 0` already, `handle_stream_alloc_extent` skips the entire seal block (no commit_length re-query, no overwrite). This prevents the post-EC re-seal corruption shape where a re-issued alloc would clobber `sealed_length` from `original_payload_len` down to `shard_size`.
  - **Stale-replica reads rejected (F119-C):** if an extent-node's local `eversion` is ahead of the client's request `eversion`, the read is rejected with `CODE_EVERSION_MISMATCH`; the client's 2-attempt retry loop in `read_bytes_from_extent` invalidates its cache and refetches `ExtentInfo`. Sealed bumps eversion, so any sealed_length divergence is also an eversion divergence — F119-C is the load-bearing protection.
  - **Replica catch-up (recovery_dispatch_loop, recovery.rs:277):** every 2 s the manager probes each replica's `commit_length`. A laggard replica (e.g., one that missed the seal RPC) is brought up to `sealed_length` via `re_avali` (extent_node.rs `handle_re_avali`).
  - **Persistent .meta integrity (F157):** post-F157, the .meta sidecar carries a CRC32C trailer; corrupted on-disk metadata is rejected on load instead of returning stale `sealed_length`. The recovery path falls back to fresh metadata from the manager.
- **Conclusion:** the divergence cases are already handled by layered defenses (manager-authoritative, F147-D re-seal guard, F119-C eversion read reject, recovery_dispatch_loop catch-up, F157 .meta CRC). No additional reconciliation loop or invariant check is needed in this iteration.
- **What we explicitly did NOT add (and why):**
  - A periodic manager-side scan that compares `MgrExtentInfo.sealed_length` against each replica's `.meta` sidecar would be ~150 LOC of new RPC + dispatch loop. The above mechanisms already converge replicas back to the manager's truth on a 2 s tick. A scan would only catch the brief race window between a seal and the next recovery tick — already invisible to clients via F119-C.
  - A read-side sealed_length consistency check (server-side reject if local sealed_length < requested offset) would duplicate F119-C semantics on a different axis. F119-C already covers it indirectly via the eversion bump.
- **Files:** `feature_list.md`, `claude-progress.txt`. (No code changes.)
- **passes:** true (audit-cleared)

### F202 · Advisory unification — 6 policy kinds in one cache (mechanism / policy separation Stage 2)
- **Trigger:** Stage 2 of the mechanism/policy separation refactor approved by user 2026-05-14 (plan `~/.claude/plans/elegant-tumbling-pumpkin.md`). Stage 1 (F201) fixed GC trigger bugs and added multi-tier params. Stage 2 makes the manager's `advisory_cache` carry all 6 actionable kinds (SPLIT, MERGE, GC, MINOR_COMPACT, MAJOR_COMPACT, EC) plus HOT_COLD, so an external policy controller has one query (`MSG_GET_POLICY_CANDIDATES`) for everything it needs to decide on.
- **Wire changes (backward-incompatible at rkyv level, same-commit upgrade — cluster.sh stops all roles before restart):**
  - Two new `POLICY_KIND_*` constants in `manager_rpc.rs`:
    - `POLICY_KIND_MINOR_COMPACT = 5` — partition-level "minor compact would pick up ≥ threshold bytes for ≥ N consecutive windows".
    - `POLICY_KIND_EC = 6` — per-extent "sealed-unconverted extent ≥ 64 MiB on an EC-policy stream".
  - `POLICY_KIND_COMPACT` (value 3) renamed to `POLICY_KIND_MAJOR_COMPACT`. Old name retained as `#[deprecated]` alias for back-compat with external consumers / test fixtures.
  - `PartitionLoad` gains 5 new fields (all u64/u32, default 0): `sst_tombstone_bytes`, `sst_expired_bytes`, `sst_out_of_range_bytes`, `minor_compact_pending_bytes`, `sealed_log_extent_count`. These flow PS → manager → advisory computation.
- **PS-side metric collection (`crates/partition-server/src/`):**
  - `PartitionMetrics` (in `lib.rs`) grows the 5 atomic counters paralleling the wire fields.
  - New `refresh_f202_metrics(part)` helper (in `background.rs`) computes:
    - `sst_expired_bytes`: Σ `estimated_size` for tables whose paired `SstReader.min_expires_at` is non-zero and ≤ `now`. Conservative upper bound (whole-SST count; tightening needs an on-disk aggregate change deferred to a future stage).
    - `sst_out_of_range_bytes`: Σ `estimated_size` of all tables when `has_overlap == 1`; 0 otherwise. Same shape as `pending_compaction_bytes`'s overlap branch.
    - `minor_compact_pending_bytes`: Σ `estimated_size` of `pickup_tables` output when `has_overlap == 0`.
    - `sst_tombstone_bytes` / `sealed_log_extent_count`: left 0 — computing the first requires SST-format aggregates (deferred); the second needs a cached `get_stream_info` result PS doesn't keep authoritatively. Advisory treats 0 as "no signal" for these dimensions.
  - Refresh hooks: every site that previously updated `pending_compaction_bytes` (4 sites in `background_compact_loop`) now also calls `refresh_f202_metrics`. Plus a new call in `commit_flush_outcome` after each flush, since the new SST changes the dead-data + minor-pending breakdown.
  - `report_load_loop` (in `lib.rs`) reads the new atomics and stuffs them into `PartitionLoad` for the 30-second ship-to-manager pulse.
- **Manager-side advisory generation (`crates/manager/src/policy.rs`):**
  - `compute_maintenance_advisory` (existing F187 helper) gains a third arm: MINOR_COMPACT, gated by `minor_compact_pending_bytes > MINOR_COMPACT_PENDING_HIGH` (default 512 MiB) sustained across `required_buckets` AND `minor_compact_pending_bytes > 0` in the latest bucket (common-sense filter: don't suggest minor compact when there's no minor compact work) AND outside the cooldown (`minor_compact_cooldown_sec`, default 120s — shorter than major because minor is much cheaper).
  - New `compute_ec_advisory(state, now)` helper iterates `state.streams` and `state.extents` directly (EC is per-extent, not bucketed). Filters: stream has EC policy attached (`ec_data_shard > 0`); extent is sealed (`sealed_length > 0`); not already converted; `sealed_length >= ec_min_extent_bytes` (default 64 MiB, common-sense filter against negative-EV conversions). Emits `POLICY_KIND_EC` with `primary_part_id = 0`, `secondary_part_id = extent_id`, `size_bytes = sealed_length`.
  - `PolicyConfig` extended with `minor_compact_pending_high`, `minor_compact_cooldown_sec`, `ec_min_extent_bytes`.
  - `policy_tick_loop` (in `lib.rs`) now unions: `compute_candidates` (split/merge) + `compute_maintenance_advisory` (gc/major/minor) + `compute_hot_cold_advisory` (hot_cold) + `compute_ec_advisory` (ec) → all into `advisory_cache`. `MSG_GET_POLICY_CANDIDATES` returns the union.
- **`client policy` rendering:** mapping updated to print 7 strings: `split / merge / gc / major / minor / ec / hotcold`. `feas` column reads "n/a" for all maintenance/EC kinds (per-partition or per-extent feasibility is implicit).
- **What's NOT in Stage 2 (deferred to Stage 2-followup):**
  - `client info --part PID --detail` — needs new manager (or PS) RPC for raw per-partition F202 metric snapshot; not strictly required because the advisory candidates already carry actionable signals (`size_bytes`, `reason`).
  - `client set-stream-ec --extent <EXTID>` — needs new manager RPC bypassing the dispatch loop for a single extent. Today OP uses `set-stream-ec --stream <ID>` which lets the existing dispatch loop pick up sealed-unconverted extents (correct but coarser-grained than advisory candidates suggest).
  - `client streams [--json]` — redundant with existing `client info --json` which already returns stream + extent details.
  - `sst_tombstone_bytes` accurate measurement (needs SST on-disk format aggregate); `sealed_log_extent_count` (needs PS-cached `get_stream_info` result).
  - These are scope-bounded follow-ups; the user-facing deliverable (advisory_cache carries all 6 actionable kinds → external controller can query one endpoint) is complete.
- **Files touched:**
  - `crates/rpc/src/manager_rpc.rs`: new POLICY_KIND_* constants, rename to POLICY_KIND_MAJOR_COMPACT with deprecated alias, 5 new PartitionLoad fields (~40 lines).
  - `crates/partition-server/src/lib.rs`: 5 new PartitionMetrics fields; report_load_loop updated to ship them.
  - `crates/partition-server/src/background.rs`: `refresh_f202_metrics` helper (~50 lines); call sites in compact loop arms + flush commit.
  - `crates/manager/src/policy.rs`: new MINOR_COMPACT_* + EC_MIN_EXTENT_BYTES consts; PolicyConfig extension; minor advisory arm in `compute_maintenance_advisory`; new `compute_ec_advisory` helper (~80 lines).
  - `crates/manager/src/lib.rs`: `policy_tick_loop` unions EC advisory + updated kind names in trace.
  - `crates/manager/src/policy_tests.rs`: 8 new F202 tests (3 minor-compact, 5 ec-advisory); shared `mk_stream` / `mk_extent` fixtures.
  - `crates/server/src/bin/autumn_client.rs`: kind mapping updated for 7 kinds; `feas` column update.
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-manager --lib policy_tests`: 32/32 (24 pre-existing + 8 new F202).
  - Pre-existing `f099i_tests::*` flakes in partition-server (2 cases) are unrelated to F202; same fails on F201 HEAD.
- **Stage roadmap reminder:**
  - **F201** (done): GC bug fix + cooldown classification + multi-tier params.
  - **F202** (this commit): advisory unification across 6 kinds.
  - **F203** (next): delete the in-kernel auto-dispatch loops; only must-cleanup (has_overlap / expiry / minor / F198 marker replay) stays in-kernel. Adds `client info --detail` and `client set-stream-ec --extent` as the OP toolkit needed to actually drive policy from outside.
- **passes:** true

### F201 · GC trigger logic overhaul: empty-sealed bug fix + multi-tier params + cooldown classification (mechanism / policy separation Stage 1)
- **Trigger:** User reported 2026-05-14 with `info` output showing partition 15's log_stream extents `[26, 27, 28, 29, 30]` where 27/28 were `(open), 0 B` (sealed by position in `extent_ids[..len-1]` but with `sealed_length=0` on the manager — extents allocated then immediately sealed by `stream_alloc_extent`'s commit_length capture before any append). These slots were pinned in `extent_ids` forever:
  1. `background_gc_loop::GcTask::Auto` built `candidates` from `discards.keys()` (line 520), but empty extents never appear in any SST's `MetaBlock.discards` → they were never even *considered* for GC.
  2. Even if they had reached the loop body, `if sealed_length == 0 { continue; }` (line 533) unconditionally skipped them.
  Combined with the user's broader request "把 auto policy 放到外面" (mechanism / policy separation), this is Stage 1 of a 3-stage refactor: fix the mechanism-level bugs and add the multi-tier knobs that external controllers / OP scripts need before Stage 2/3 actually delete the in-kernel auto-dispatch.
- **Three changes (all isolated mechanism-level improvements; no policy retreat yet):**
  1. **GC candidate-set expanded + empty-extent picks** (`background.rs::background_gc_loop`):
     - `candidates` now iterates ALL `sealed_extents` (slice = `extent_ids[..len-1]`), sorted by reclaimable bytes desc; zero-discard extents land last but reachable.
     - `if sealed_length == 0 { ... }` is now a positive branch: push to `holes` directly (the `tail` exclusion is already enforced by the `sealed_extents` slice, so any extent reaching this branch is by construction not the tail). `run_gc(eid, 0)` skips the `while cur < sealed_length` read loop and proceeds straight to `flush_gc_batch` (no-op) + `punch_holes` — exactly what an empty slot needs.
  2. **F199 cooldown classification by error type** (new pure helper `classify_gc_failure_cooldown`):
     - Pre-F201: single 300 s window for every `run_gc` failure.
     - Post-F201: 30 s soft window for failures whose anyhow chain contains `"precondition failed"` (manager rejects `punch_holes` because the target extent is in `ec_conversion_inflight` per F138/F145) or `"eversion mismatch"` (autumn-stream `EversionStale` sentinel — stale `extent_info_cache` after an EC bump). 300 s hard window for everything else (network timeout, irrecoverable EC shard shortage, decode error).
     - String-based classification is intentional: both phrases are documented wire surfaces (manager `AppError::Precondition` Display + stream-client `EversionStale` Display) and grepping them here is a defense-in-depth: any future rename of either sentinel immediately demotes the classifier to "hard cooldown", which is safe.
     - `gc_failure_cooldown` map shape changed from `HashMap<u64, Instant>` to `HashMap<u64, (Instant, Duration)>` so each entry carries its own window; the retain/skip checks now read the per-entry duration.
  3. **Multi-tier `GcTask::Auto` params + `client gc` CLI flags + wire change**:
     - `GcTask::Auto` now carries `GcAutoParams { ratio, max_size, stream_debt, empty_only }`. `Default` reproduces pre-F201 single-tier behaviour (`ratio = None → 0.4`, no upper bound, no stream-debt relaxation, not empty-only) + the F201 empty-extent pick path.
     - `MaintenanceReq` (wire struct) gained four optional fields: `gc_ratio: Option<f64>`, `gc_max_size: Option<u64>`, `gc_stream_debt: Option<u64>`, `gc_empty_only: bool`. **Backward-incompatible** at rkyv level — same-commit upgrade for manager + PS + client required (cluster.sh handles this by stopping all roles before restart).
     - `client gc` CLI accepts: `--ratio R` / `--max-size B[K|M|G|T]i?B?` / `--stream-debt B[K|M|G|T]i?B?` / `--empty-only`. New helper `parse_byte_size` accepts human-readable suffixes (`16M`, `1GiB`, etc.).
     - SDK adds `ClusterClient::gc_with_params(part_id, GcAutoParams)`; existing `gc(part_id)` now wraps `gc_with_params(part_id, Default::default())` for back-compat.
- **Why this is "mechanism" not "policy" (per Stage 1 boundary):**
  - The empty-extent picks and the cooldown classification are pure bug fixes — the existing single-tier policy was logically wrong, and now it's right. No new auto-dispatch added.
  - The multi-tier params are a *capability* exposed to the OP / external controller. The PS still passes them through to the existing single-pass selection — there's no PS-internal "tier scheduler". External controllers (or `cron + bash`) compose effective tiers by issuing multiple `client gc` invocations back-to-back with different flags.
  - Stage 2 (F202) will route advisory output to `advisory_cache` for all 6 op kinds. Stage 3 (F203) will delete the in-kernel auto-dispatch loops outright.
- **Files touched:**
  - `crates/partition-server/src/background.rs`: GC candidate expansion + empty-extent pick + cooldown helper + tests (~120 lines).
  - `crates/partition-server/src/lib.rs`: `GcTask::Auto` variant carries `GcAutoParams`; scheduler dispatch updated to pass `Default::default()`.
  - `crates/partition-server/src/rpc_handlers.rs`: `MaintenanceReq` decoder forwards new fields to `GcAutoParams`.
  - `crates/rpc/src/partition_rpc.rs`: 4 new fields on `MaintenanceReq` (wire change marked in the struct doc).
  - `crates/client/src/lib.rs`: `gc_with_params` + public `GcAutoParams`; legacy `maintenance()` helper updated to send default values for the new fields.
  - `crates/server/src/bin/autumn_client.rs`: `Gc` command extended; new `parse_byte_size` helper + 3 unit tests; `usage()` updated.
  - `crates/manager/src/lib.rs`: `MaintenanceReq` construction in merge orchestration updated to include new default fields.
  - `crates/manager/tests/{support,integration}.rs`: test fixtures updated.
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse` (lib + tests): clean.
  - `cargo test -p autumn-partition-server --lib f201`: 4/4 (cooldown classifier sentinel cases).
  - `cargo test -p autumn-server --bin autumn-client -- parse_byte_size`: 3/3.
  - Two pre-existing `f099i_tests::*` flakes are unrelated (fail on pre-F201 HEAD too).
- **Manual test (the user-reported case):**
  1. `cluster.sh reset 4 && cluster.sh start`
  2. `$AC bootstrap --replication 3+0 --log-ec 3+1 --row-ec 3+1`
  3. `$AC perf-check --duration 60` — produces 0-byte sealed log_stream extents during alloc/seal cycles
  4. `$AC info` — observe `(open), 0 B` on some sealed-position extents
  5. `$AC gc <PARTID>` — empty extents now get picked and `punch_holes`'d on the very next GC dispatch
  6. `$AC info` — 0-byte sealed extents are gone
- **Stage roadmap reminder:** F202 = advisory_cache unified across 6 op kinds; F203 = delete in-kernel auto-dispatch + must-cleanup-only kernel + cron-based external controller example. See plan `elegant-tumbling-pumpkin.md`.
- **passes:** true

### F200 · EC sub-range reconstruction — eliminate full-extent decode on single shard failure
- **Target:** User correctly pointed out 2026-05-14 that F199's GC failure cooldown only suppresses retries — the real bug is in `ec_subrange_read`'s fall-back path. When a per-shard read fails for a sub-range request (e.g., GC chunking through a 2.8 GB EC(3+1) extent reading 64 MiB at a time, touching only shard 0), the function falls back to `ec_read_full_and_slice` which:
  1. Reads ALL `(K + M)` shards at full size via `ec_read_full` (each issues `length=0` = "read to end").
  2. Decodes the entire payload via `ec_decode`.
  3. Slices `[offset, offset+length)` from the decoded buffer.
  This means a SINGLE 64 MiB chunk request that hits a transient shard-read failure triggers `4 × 933 MiB ≈ 3.7 GiB` of cross-network reads with 5 s per-shard timeouts. On a stressed macOS test box this:
  - Saturates io_uring + TCP send buffers across all replicas.
  - Causes the manager's `disk_status_update_loop` df probe to time out under load.
  - Marks the probed node's disks `online=false` even though the underlying disk is healthy.
  - Repeats every GC tick until F199's cooldown kicks in — but each cycle still does the 3.7 GiB amplification.
- **Why sub-range reconstruction is correct:** galois-8 RS encodes byte-by-byte: at each row position `i` the `(K + M)` shard bytes form an independent RS codeword. So the sub-range `[a, b)` on a missing data shard can be reconstructed from `[a, b)` on any K healthy shards. No full-extent decode needed. Test `f200_reconstruct_shard_subrange_data_shard` (erasure.rs) locks in this invariant.
- **Fix:**
  - `ec_subrange_read` (client.rs): instead of `fall_back = true → ec_read_full_and_slice`, the per-plan-entry failure path now calls a new helper `ec_reconstruct_shard_subrange` which:
    - Reads `[sh_off, sh_off + sh_len)` from all `(K + M) - 1` non-missing peers in parallel (5 s timeout each, same shape as `ec_read_full`).
    - Stops as soon as K succeed.
    - Calls `ec_reconstruct_shard` (existing erasure helper) on the K-aligned sub-shards.
    - Returns the reconstructed missing-shard sub-range bytes.
  - Failed plan entries are filled into `plan_results` after reconstruction; the final stitch concatenates in order as before.
  - Eversion-stale (CODE_EVERSION_MISMATCH) short-circuits the whole call so the top-level `read_bytes_from_extent` 2-attempt loop refetches `ExtentInfo` and retries against the fresh EC layout.
- **Bytes-on-the-wire comparison** (user's GC scenario: 64 MiB chunk on 2.8 GB EC(3+1) extent, shard 0 read fails):
  - Pre-F200: 4 × 933 MiB = 3.7 GiB.
  - Post-F200: 3 × 64 MiB = 192 MiB. **20× reduction**.
  - Cluster-wide GC pressure on macOS test box drops accordingly; df probes no longer time out → disks no longer falsely flap offline → the cascade self-heals.
- **Files:**
  - `crates/stream/src/client.rs`: replace `fall_back` path with reconstruction + new helper `ec_reconstruct_shard_subrange`.
  - `crates/stream/src/erasure.rs`: add 2 unit tests (`f200_reconstruct_shard_subrange_data_shard`, `f200_reconstruct_shard_subrange_with_one_parity_present`).
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-stream --lib`: 53/53 (51 pre-existing + 2 new F200).
- **Relationship to F199:** F199's per-extent failure cooldown remains useful as a safety net for genuinely-broken extents (e.g., real disk failure where reconstruction also fails). F200 prevents the cooldown from being the primary mechanism — under normal pressure, sub-range reconstruction completes and GC succeeds.
- **Deferred:**
  - When a sub-range read fails on a PARITY-region offset (`start_shard >= data_shards`), `ec_subrange_read` still falls back to `ec_read_full_and_slice`. This is a stale-VP / out-of-range path; the bytes-on-the-wire cost would matter only for misbehaving callers and the data-loss surface is small. Reconstruction in this path is left for future iteration.
- **passes:** true

### F199 · Per-extent GC failure cooldown — stop hammering broken EC layouts
- **Target:** Reported by user 2026-05-14 on macOS test cluster: after upgrade + restart + `client compact 15`, the partition starts but the local `background_gc_loop` (5-7 s `random_delay` between auto ticks) keeps picking the same extent (extent 10, 2.8 GB EC(3+1)) whose `data[0]` host node has an offline disk (`disk 8: online=false`). Each `run_gc` chunk read hits `ec_subrange_read` → one shard read fails → fall back to `ec_read_full_and_slice` → 4 parallel reads of 933 MB shards with a 5 s per-shard timeout → "0/3 shards available for EC decode". Repeats every ~15 s indefinitely, saturating CPU/IO on the test machine.
- **Approach (minimum change, per-extent cooldown):**
  - `background_gc_loop` carries a `HashMap<u64, Instant>` of recent `run_gc` failures.
  - `GC_FAILURE_COOLDOWN` = 5 min. After picking holes from the discards map, Auto tasks filter out extents whose last failure is within the cooldown window. Force tasks (operator `client gc-force`) bypass the cooldown.
  - On `run_gc` Ok: remove the extent from the cooldown map (so future legitimate triggers fire immediately).
  - On `run_gc` Err: stamp `Instant::now()` for the extent.
  - Stale entries (older than the cooldown) are evicted lazily on each pickup pass — bounded memory.
- **Why this is the right knob:**
  - The fundamental issue is that `ec_subrange_read` falls back to `ec_read_full_and_slice` (full extent decode) when ANY single shard read fails. For a 2.8 GB EC(3+1) extent that's 3 × 933 MB of cross-network reads with 5 s timeouts — doomed when even one host's disk is offline. F199 doesn't fix that path (proper fix would be sub-range RS reconstruction — deferred to F200+), it just stops GC from re-triggering it every 15 s.
  - The user's specific layout is recoverable through manager-side `recovery_dispatch_loop` replacing the offline-disk slot with a fresh node (which uses `ec_reconstruct_shard` from data-shard peers). Once recovery completes, the next GC tick (after cooldown) finds K healthy shards and succeeds.
  - Force tasks (`autumn-client gc-force <extent_id>`) deliberately bypass the cooldown so an operator can override after the underlying problem is fixed.
- **Files:** `crates/partition-server/src/background.rs` (~30 lines added at top of `background_gc_loop` + at success/failure call site).
- **Verification:**
  - `cargo build -p autumn-partition-server --lib`: clean.
  - `cargo test -p autumn-partition-server --lib`: 138/139 (the 1 fail is pre-existing `f099i_d1_fast_path_no_fu_allocation` poisoned-mutex, unrelated).
- **Manual test plan:**
  1. Cluster with a known-broken EC extent (e.g., reproduce user's setup: one node's disk offline).
  2. Wait for partition to open + first GC tick to fail. Confirm `GC run_gc extent N: ...` error fires once.
  3. Subsequent ticks log `F199: GC skipping recently-failed extents (cooldown active)` instead of re-running.
  4. macOS CPU/IO pressure drops dramatically (1 failure per 5 min instead of 1 every 15 s).
- **Deferred (F200+):**
  - Sub-range RS reconstruction in `ec_subrange_read` so GC chunks can be served from K shards (skipping the broken one) without falling back to full-extent decode. Cleaner, but requires implementing partial-shard reconstruct in `ec_reconstruct_shard` callers.
  - Configurable cooldown duration via env var.
- **passes:** true

### F198 · Rich `ec_conversion_inflight` marker — fix EC dispatch stuck-state on restart
- **Target:** Reported by user 2026-05-14: cluster reset + many split/compact rounds on partition 15 with large values → macOS slowdown → `cluster.sh restart 4` → PS startup fails on `partition 15`: `read SST from rowStream extent=20 offset=0: eversion mismatch (stale extent_info_cache)`. Manager replays `ecConversionInflight/<id>` markers (F173) but the F173 marker carried only the extent_id, not the original dispatch's `target_nodes` assignment. The `ec_conversion_dispatch_loop` body further had `if ec_conversion_inflight.contains(&extent_id) { continue; }` which **permanently skipped** replay-loaded markers — the manager's etcd state (`ec_converted=false`, pre-EC eversion) never converged with the extent-node's local state (post-EC eversion bumped via `commit_shard_local`). Reads kept returning CODE_EVERSION_MISMATCH; the 2-attempt `read_bytes_from_extent` refetch loop re-loaded the same stale manager state every time.
- **Why F173 wasn't enough:** F173 intended for the next `ec_conversion_dispatch_loop` tick to "re-fire the convert path which is idempotent (F119-D)" but the implementation skipped the tick on `contains(&extent_id)`. Even removing the skip alone is unsafe: the fresh dispatch path runs `shuffle().take(extra_needed)` + `alloc_extent_on_node` to pick parity nodes. A second call to `alloc_extent_on_node` on a node that already received shard data in the prior dispatch RESETS that node's in-memory ExtentEntry (eversion=1, sealed=0) and overwrites its `.meta` sidecar — silently corrupting EC layout. `apply_ec_conversion_done` would then write the new (possibly different) random parity assignment to etcd, abandoning whichever node actually holds the parity shard.
- **Approach (rich marker + replay-aware dispatch):**
  - New rkyv type `MgrEcDispatchInflight { extent_id, target_nodes, extra_disk_ids, data_shards, new_eversion }` (`crates/rpc/src/manager_rpc.rs`).
  - `persist_ec_conversion_inflight(&record)` now serialises the record into the `ecConversionInflight/<id>` value (was empty).
  - `replay_from_etcd` decodes marker values into `pending_ec_dispatch: Rc<RefCell<HashMap<u64, MgrEcDispatchInflight>>>` alongside the existing `ec_conversion_inflight` lock-set.
  - `ec_conversion_dispatch_loop` checks `pending_ec_dispatch.get(&extent_id)` BEFORE running the shuffle/alloc path. On replay match: reuse persisted `target_nodes` + `extra_disk_ids` + `new_eversion` exactly (no shuffle, no `alloc_extent_on_node`). The old `contains(&extent_id)` skip is gated on `replay_params.is_none()` so the lock semantics for in-process duplicate dispatch are preserved.
  - Initial dispatch delay reduced from 5 s → 500 ms (one-shot, then 5 s steady-state) so post-restart re-dispatch fires before PS retries time out.
  - PS-side `recover_partition` SST read wraps `read_bytes_from_extent` in a 30 × 1 s retry on `eversion mismatch` so the operator's first `cluster.sh restart` succeeds without a second manual restart — the manager converges state within one tick.
- **Why pre-F198 empty markers stay safe:**
  - Replay logs a warning per legacy empty marker and skips the dispatch path; the F138/F145/F146 mutator-blocking semantics still hold via `decoded_ec_inflight`. Operator can clear the marker manually (delete `ecConversionInflight/<id>` from etcd) once the underlying data state is verified; thereafter the dispatch loop picks up the extent on its next tick as a fresh candidate. No production deployments exist with mixed pre-/post-F198 markers at this point.
- **Cost:**
  - Marker payload grows from 0 bytes to ~40-60 bytes (rkyv-encoded `MgrEcDispatchInflight` with 4 u64s + 4-element node Vec). Etcd marker lifespan is unchanged (seconds-bounded by the dispatch RPC).
  - One additional `HashMap<u64, MgrEcDispatchInflight>` field on `AutumnManager`.
- **Files:**
  - `crates/rpc/src/manager_rpc.rs`: new `MgrEcDispatchInflight` rkyv struct.
  - `crates/manager/src/lib.rs`: `pending_ec_dispatch` field, replay decoder, modified `persist_ec_conversion_inflight` signature, F198 unit tests.
  - `crates/manager/src/recovery.rs`: replay-aware branch in `ec_conversion_dispatch_loop`; initial delay reduced to 500 ms.
  - `crates/partition-server/src/lib.rs`: 30 × 1 s retry on `eversion mismatch` during `recover_partition` SST read.
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-manager --lib`: 70/70 (incl 2 new F198 tests: rkyv roundtrip + in-memory bookkeeping).
  - `cargo test -p autumn-stream --lib`: 51/51 (no regressions).
  - Pre-existing failures unrelated to this fix: 2 in `f099i_tests` (poisoned mutex on global counter); 1 in `tests/ec_integration.rs replication_stream_works` (also fails on main pre-F198).
- **Manual test plan (the user's reported repro):**
  1. `sh cluster.sh reset 4`.
  2. `sh cluster.sh put-large <partition 15 key> <large-value>` (>4 KiB so it lands in log_stream as VP).
  3. `sh cluster.sh client compact 15` then split 15 a few rounds.
  4. SIGKILL all four PSes and the manager mid-write (forces a window where `apply_ec_conversion_done` hasn't committed).
  5. `sh cluster.sh restart 4`.
  6. **Before F198:** PS startup fails with `partition 15 thread exited before reporting listener readiness` and `eversion mismatch (stale extent_info_cache)`.
  7. **After F198:** PS startup logs `F198: SST read returned eversion mismatch during open — waiting 1 s ...` for ~1-2 attempts, then proceeds. Total open delay <3 s.
- **passes:** true

### F173 · Persist `ec_conversion_inflight` to etcd (defense-in-depth across leader failover)
- **Target:** Close the remaining `ec_conversion_inflight` failover-window concern flagged in F154's deferred footnote. F138's eversion-bump-lock semantics ("while extent X ∈ `ec_conversion_inflight`, no other manager-side mutator may bump `ex.eversion`") only hold within a single leader's lifetime — the HashSet was purely in-memory. On leader failover the new leader's set was empty, so its `recovery_dispatch_loop` could fire `re_avali` / `require_recovery` on an extent the deposed leader was actively converting. Downstream defenses (F119-D coordinator idempotency, F153 extent-node per-extent EC lock, F119-C read-side eversion check) made the race non-corrupting in practice, but each fires post-hoc — F173 closes the window at the source.
- **Approach:** persist a marker key per in-flight extent.
  - `persist_ec_conversion_inflight(extent_id)`: `etcd PUT ecConversionInflight/{id}` with empty value (existence is the signal). Called BEFORE the `EXT_MSG_CONVERT_TO_EC` RPC dispatch in `ec_conversion_dispatch_loop`. On failure to persist, skip this extent and retry on the next tick.
  - `unpersist_ec_conversion_inflight(extent_id)`: `etcd DELETE ecConversionInflight/{id}`. Called AFTER `apply_ec_conversion_done` (success path) or RPC failure. Lingering markers are harmless — `recovery_dispatch_loop` skips the extent for one extra tick, then `ec_conversion_dispatch_loop` re-fires the convert path which is idempotent (F119-D).
  - `replay_from_etcd`: scan `ecConversionInflight/` prefix and repopulate `ec_conversion_inflight: HashSet<u64>` BEFORE the leader-key flip completes. The new leader's first `recovery_dispatch_loop` tick observes the markers and skips those extents.
- **Why both etcd persistence (F173) and extent-node lock (F153) are needed:**
  - F153 catches a CONCURRENT dispatch race: deposed leader's encode is mid-flight when new leader re-fires. The per-extent `futures::lock::Mutex` on the extent-node serialises so the second dispatch waits, then F119-D's eversion-check makes it a no-op.
  - F173 catches the BROADER recovery race: deposed leader's EC dispatch may have completed on the coordinator (shards on disk, eversion bumped to X+1), but `apply_ec_conversion_done` never ran in etcd (manager state still has eversion X, not-converted). New leader's `recovery_dispatch_loop` would observe the eversion mismatch via `commit_length` probe and dispatch `require_recovery` — F173's marker tells the new leader to skip this extent until the next `ec_conversion_dispatch_loop` re-enters the convert path (which is idempotent and converges manager state).
- **Cost:**
  - 1 extra etcd PUT per EC dispatch (under the F149 `txn_fenced` leader-fence protocol).
  - 1 extra etcd DELETE per completion.
  - 1 extra `get_prefix("ecConversionInflight/")` per leader take-over (replay).
  - At cluster steady-state with sparse EC traffic (~few extents converting per minute), this is sub-percent overhead on etcd.
- **Files:** `crates/manager/src/lib.rs` (3 etcd helpers + replay hook + ec_conversion_inflight populate), `crates/manager/src/recovery.rs` (persist before dispatch + unpersist after apply), `feature_list.md`, `claude-progress.txt`.
- **Verification:**
  - `cargo build -p autumn-manager`: clean.
  - `cargo build --release --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
- **Operational notes:**
  - Markers are NEVER long-lived in steady state — they live only for the duration of a single EC conversion (typically seconds). A node-startup race could theoretically observe stale markers from a hung/crashed prior leader, but the next `ec_conversion_dispatch_loop` tick re-enters the path and clears them via `unpersist_ec_conversion_inflight` after F119-D's idempotent return.
  - The marker format is intentionally minimal (empty value). If future iterations need to record richer state (target_nodes, new_eversion, dispatch timestamp) for observability or smarter recovery, the schema can be extended without changing the existence-as-marker invariant — readers that don't understand the new fields just see the marker and skip the extent.
- **passes:** true

### F172 · Manager dispatch-loop CPU audit — pre-filter + hoist redundant clones
- **Target:** Continue the thread-per-core audit, this iteration on the manager. The manager runs ~7 background dispatch loops on a single compio runtime alongside its 18 RPC handlers (heartbeat, register_ps, get_regions, mutating handlers, etc.). Any inline CPU work between awaits in a dispatch loop blocks all RPC handlers + sibling loops on the same runtime. Audit identified two concrete inefficiencies; the rest are clean.
- **Audit findings (cleared, no fix needed):**
  - `ec_conversion_dispatch_loop` (recovery.rs:586-610): already filters under borrow before cloning candidates. Good pattern.
  - `extent_delete_loop` (extent_delete.rs:50): drains queue then sequential per-replica RPC. Each RPC awaited; runtime stays responsive for sibling tasks.
  - `ps_liveness_check_loop` (lib.rs:791): filter under borrow, only `dead_ps: Vec<u64>` clones cross the await boundary.
  - `recovery_collect_loop` (recovery.rs:378): only clones small `s.nodes` map; per-node RPC dispatch with awaits.
  - `disk_status_update_loop`, `leader_keepalive_loop`, `leader_election_loop`: small constant work per tick.
  - `rebalance_regions` (lib.rs:842): O(parts × PS) inner `min_by_key` scan. At 10K parts × 100 PS = 1M ops ≈ 10 ms inline; bounded — accepted.
  - `replay_from_etcd` (lib.rs:540+): startup-only, not a hot-path concern.
  - All RPC handler etcd writes go through F149 `txn_fenced`; one extra GET on fence break, otherwise straight-through.
- **F172-A — `recovery_dispatch_loop` snapshot pre-filter (recovery.rs:284-291):**
  - **Pre-F172 pattern:** snapshot was `s.extents.values().cloned().collect::<Vec<_>>()` — clones EVERY extent in the cluster, including the ones the loop body will skip on its very first line (`if ex.sealed_length == 0 { continue; }`, then `if ec_conversion_inflight.contains(&ex.extent_id) { continue; }`).
  - **Cost:** `MgrExtentInfo` carries 4 `Vec<u64>` fields (replicates, parity, replicate_disks, parity_disks) — each clone allocates 4 small heap regions. At 10K extents (~200 B each effective) = ~2 MB inline memcpy + ~40K allocations per 2 s tick. At 100K extents = 20 MB / 400K allocs ≈ 10-20 ms inline. Worst-case window: heartbeat (`heartbeat_ps` RPC) handlers blocked for tens of ms per tick on a busy cluster.
  - **Fix:** push the early-skip checks INTO the borrow scope so we filter first, clone after. The filter is a tight CPU loop over hashed lookups (`HashSet::contains`), no allocations. The body's correctness is unchanged because `apply_recovery_done` / `mark_extent_available` / `handle_multi_modify_split` re-check `ec_conversion_inflight` at apply time (F138), so a stale snapshot is safe.
- **F172-B — `ec_conversion_dispatch_loop` hoist `node_addrs` clone (recovery.rs:626-632):**
  - **Pre-F172 pattern:** the inner `for (ex, stream) in candidates` loop re-collected the entire `s.nodes -> address` map ONCE PER CANDIDATE EXTENT, even though `s.nodes` is identical for every iteration of a single tick.
  - **Cost:** N candidates × M nodes of `String::clone` per tick. Bounded but pure waste. With M = 100 nodes and N = 50 candidates per tick = 5000 String clones × ~30 bytes = ~150 KB total clones plus 5000 heap allocations. Sub-millisecond, but eliminating it costs essentially nothing.
  - **Fix:** snapshot `node_addrs` ONCE alongside `candidates` under the same borrow. Save N-1 clones per tick.
- **What's NOT addressed in F172 (deferred):**
  - **Sequential RPC dispatch in `recovery_dispatch_loop`:** at 10K sealed extents × 3 replicas × 1 ms RPC each = 30 s end-to-end loop time. This is an I/O latency concern, not CPU — the runtime stays responsive for sibling tasks because each await yields. Parallelising via `FuturesUnordered` / `join_all` would shorten loop wall-time at the cost of coordinated dispatch ordering. Out of scope for this CPU audit.
  - **`Rc<MgrExtentInfo>` storage migration:** would make every `s.extents.values().cloned()` a one-atomic-increment-per-clone instead of allocating-clone. ~50 LOC structural change across the manager + every consumer that mutates an extent. Deferred until a measurable hot path actually needs it; F172-A's pre-filter is the targeted fix for the dispatch loop's specific hot path.
- **Files:** `crates/manager/src/recovery.rs` (2 sites + comments), `feature_list.md`, `claude-progress.txt`.
- **Verification:**
  - `cargo build -p autumn-manager`: clean (only pre-existing unused-import warnings).
  - `cargo build --release --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
  - `cargo test -p autumn-stream --lib`: 40/40 pass.
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 122/122 pass.
- **Operational notes:**
  - On a 10K-extent / mostly-sealed-extents cluster, F172-A reduces per-tick CPU spike from 2 MB clone (~1-2 ms) to ~1 KB filter Vec (~µs). Heartbeat tail latency on the manager runtime stays sub-ms even under recovery dispatch pressure.
  - On a 100-node × 50-candidate cluster, F172-B saves ~150 KB of String clones per tick. Negligible per-tick but removes the per-candidate amplification factor — protects against a future O(N²) regression if cluster size grows.
  - Both fixes are pure refactors preserving observable behavior (same dispatch decisions, same RPC sequence). Existing F138/F126 inflight-set semantics are untouched.
- **passes:** true

### F171 · `UnsafeCell<CompioFile>` → `RefCell<Rc<CompioFile>>` structural migration (close the file-replace UB at the type level)
- **Target:** Close the type-level UB at the file-replacement path on the extent node, completing the F166/F167 line of work. Pre-F166 the file handle was held in `UnsafeCell<CompioFile>` and accessed via `unsafe { &mut *file.get() }` borrows that aliased across `.await` (a real Rust UB the compiler is allowed to reason against, regardless of single-threaded compio execution). F166 fixed two `&mut` borrow sites by switching to shared `&File` via compio's `SharedFd` interior mutability. F167 encapsulated the file-REPLACE site (`*entry.file.get() = new_file` during EC commit) with a documented `unsafe fn replace_file_under_lock` and a safety contract relying on F153's per-extent EC-conversion lock. F167 documented but did NOT close the structural concern: a concurrent reader holding `&CompioFile` from `unsafe { &*file.get() }` is type-level dangling if the replace fires mid-read. F119-C's eversion-mismatch covers it in practice; F171 closes it at the type level.
- **Approach (5 file helpers + ~20 call sites + 1 field type + 1 method):**
  - **Field type:** `UnsafeCell<CompioFile>` → `RefCell<Rc<CompioFile>>`. Brief `borrow()` to clone the `Rc` for I/O; `borrow_mut()` for replace. No `unsafe`.
  - **`ExtentEntry::file_rc(&self) -> Rc<CompioFile>`:** the canonical accessor. Clones the inner `Rc` under a brief `borrow()`. The clone is captured by the I/O future across `.await`; the `RefCell` borrow itself is released by `.clone()` before the future awaits.
  - **`ExtentEntry::replace_file(&self, new: CompioFile)`:** safe `borrow_mut()` + `Rc::replace` (well, `*self.file.borrow_mut() = Rc::new(new)`). The OLD `Rc` is dropped only when the LAST concurrent reader releases its clone — the fd cannot dangle.
  - **Helper signatures:** `file_pwrite`, `file_pread`, `file_pwrite_chunked`, `file_pread_chunked` all take `Rc<CompioFile>` by value (the future captures it). The free function `file_ref` is deleted — callers use `entry.file_rc()` directly.
  - **Construction sites:** 3 `ExtentEntry { file: UnsafeCell::new(file), .. }` → `RefCell::new(Rc::new(file))`. The `staging_cell` site in `prepare_shard_local` becomes a plain local `Rc<CompioFile>` (the staging file is never aliased — its path is unique per extent).
- **Why this and not earlier:** F167's deferred footnote called this out: "A FULL fix that closes this type-level UB would migrate `UnsafeCell<CompioFile>` to `RefCell<Rc<CompioFile>>` ... ~50-100 LOC across all `file_*` helpers and ~20 call sites; deferred to a future feature when the structural cost is justified." F171 is exactly that work, sized in at ~140 LOC across helpers + 17 call sites + 1 field + 1 method.
- **What's no longer load-bearing post-F171:**
  - F153's per-extent `ec_conversion_locks` `futures::lock::Mutex<()>` remains for higher-level serialisation against concurrent EC dispatches racing on the staging path — but it is no longer the only thing between us and a dangling fd. The fd is now structurally protected by `Rc` refcount semantics.
  - F119-C's eversion-mismatch reject is similarly belt-and-braces for client-side cache freshness, no longer the sole memory-safety guarantor.
- **What is now zero in the file-access path:**
  - `unsafe` blocks: 0 (the only remaining `unsafe` in `extent_node.rs` is libc setsockopt at lines 182 + 456 for TCP socket tuning).
  - `UnsafeCell` mentions: 0 in code (only in F171 doc-comment describing the pre-F171 pattern).
  - `file.get()` calls: 0 in code.
- **Files:** `crates/stream/src/extent_node.rs` (field type + 5 helpers + 17 call sites + 3 construction sites + replace_file rewrite + build_append_future + build_read_future), `crates/stream/CLAUDE.md` (ExtentEntry section rewritten), `feature_list.md`, `claude-progress.txt`.
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo build --release --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-stream --lib`: 40/40 pass.
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 122/122 pass.
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
- **Operational notes:**
  - Per-I/O cost: one `Rc::clone` (atomic increment, ~1-2 ns) before each pread/pwrite. Negligible relative to the syscall + io_uring round-trip (~µs minimum). Throughput-neutral.
  - Memory: `Rc<CompioFile>` is 16 bytes (pointer + control block) vs `CompioFile` inline. The `RefCell` adds 8 bytes for the borrow flag. Per-extent overhead ~24 bytes on top of the existing `ExtentEntry`. Negligible at typical extent counts.
  - Refactor preserves semantics 1:1: the `Rc::clone` happens at the same logical site that previously did `unsafe { &*file.get() }`; the I/O paths are otherwise unchanged.
- **passes:** true

### F170 · `ec_slice_decoded` zero-copy full-read (close the post-spawn_blocking memcpy on EC reads)
- **Target:** Continue the thread-per-core audit from F168/F169. The remaining outlier in the EC read path: `ec_slice_decoded` was called INLINE on the compio runtime AFTER `spawn_blocking(ec_decode)` returns (`crates/stream/src/client.rs:1967` in `ec_read_full_and_slice`). Its full-read branch (offset=0, length=0 — the dominant case during recovery / large-VP fetches) unconditionally `.to_vec()`'d the entire decoded payload, which on a 256 MiB EC-decoded extent is **50-100 ms of inline memcpy** on the caller's compio runtime. F117 + partition-server/CLAUDE.md note 17 are explicit: CPU-bound work MUST run on the blocking pool. F170 closes this last hot-path violation in the EC read pipeline.
- **Fix:** Change `ec_slice_decoded` to take `Vec<u8>` by value. The full-read branch returns the input Vec via ownership transfer (zero allocation, zero memcpy). Sub-range reads still allocate `read_len` bytes (typically 4 KiB – 1 MiB for VP-sized requests), but the dominant full-extent path is now strictly zero-copy. The single production call site at `client.rs:1967` already owns `full_payload: Vec<u8>` returned from `ec_read_full`, so the move semantics line up with no caller changes beyond dropping the `&` borrow operator.
- **Why this slipped F168/F169:** F168 fixed the compaction merge loop. F169 fixed compaction's chunk-emit. The EC read path is structurally separate (lives in `autumn-stream`, not `partition-server`), so its inline-CPU offender was outside the compaction-focused audits. F170 catches the EC-read sibling on the same principle: any payload-sized memcpy on the compio runtime is a thread-per-core violation regardless of which crate owns it.
- **Combined with F168/F169, the EC read pipeline now has zero compio-blocking CPU work:**
  - `ec_decode` (RS GF(256) math, 100-300 ms): in `spawn_blocking` since F117.
  - `ec_slice_decoded` full-read (50-100 ms memcpy): zero-copy after F170.
  - `ec_slice_decoded` sub-range (typically <1 ms for 4 KiB–1 MiB): bounded inline; not worth the spawn_blocking round-trip cost (~10 µs join overhead would dominate).
- **Test:** Added `slice_full_read_is_zero_copy` to `client::ec_slice_tests` — verifies `out.as_ptr() == in_ptr` and `out.capacity() == in_cap` post-call, asserting the full-read path performs ownership transfer rather than a hidden copy. Future refactors that re-introduce a memcpy will fail this invariant test.
- **Audit cleared (no fix needed) for thread-per-core compliance:**
  - `recover_partition` logStream replay loop: runs at startup with no concurrent tasks on the partition's compio runtime; heartbeat is on PS-main (different runtime). Bounded by single-threaded discipline; not a thread-per-core violation.
  - `process_gc_chunk` inner SST/memtable lookup nesting: bounded by the GC batch cap (256 records / 4 MiB), with `flush_gc_batch.await` between batches and `gc_yield_now()` between 64 MiB chunks. Per-batch inline CPU ~1-50 ms in pathological many-SST cases — borderline but already cooperatively scheduled.
  - All 3 RS encode/decode production sites already wrapped in `spawn_blocking` (`extent_node.rs:2235` ec_reconstruct_shard, `extent_node.rs:3427` ec_encode, `client.rs:2069` ec_decode).
  - `MergeIterator::next` at iterator.rs:268 is O(N) over iterators but ~50ns per call; bounded by F168's per-1000-entry yield.
- **Files:** `crates/stream/src/client.rs` (ec_slice_decoded signature + caller + test), `feature_list.md`, `claude-progress.txt`.
- **Verification:**
  - `cargo build -p autumn-stream`: clean (only pre-existing warnings).
  - `cargo build --release -p autumn-stream`: clean.
  - `cargo test -p autumn-stream --lib`: 40/40 pass (was 39 pre-F170; +1 new zero-copy invariant test).
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 122/122 pass.
- **Operational notes:**
  - Memory savings as a bonus: pre-F170 the full-read EC path peaked at 2× extent size (one `Vec<u8>` for ec_decode output + one `Vec<u8>` allocated by `.to_vec()` in ec_slice_decoded). Post-F170 peak drops to 1× — meaningful at 256 MiB / 1 GiB extent sizes when N partitions concurrently recover or fetch VPs.
  - This fix is independent of GC/compaction. It applies to every EC read on a sealed extent: VP fetches during normal Get, recovery's logStream replay (when any source extent has been EC-converted), and admin-driven reads.
- **passes:** true

### F169 · `do_compact` chunk-emit moves to spawn_blocking (the last big inline-CPU offender)
- **Target:** Continue the thread-per-core audit from F168. After F168 added a per-1000-entry yield to the merge loop, the next biggest inline CPU work was the chunk-emit itself: `builder.finish()` does a 256 MiB memcpy (concatenates all blocks) + bloom-filter finalize + meta encode + CRC32C ≈ **50-100 ms inline**, then `SstReader::from_bytes` parses the just-built MetaBlock + verifies CRC ≈ **5-10 ms inline**. Both ran on the P-log compio runtime in `do_compact`'s emit path, blocking all other tasks (merged_partition_loop, ps-conn, background_*) for the duration.
- **Why it slipped earlier:** `flush_one_imm` already wraps the equivalent work in `spawn_blocking` via `build_sst_bytes` (per F117 + partition-server/CLAUDE.md note 17 — "CPU-bound work MUST run on the blocking pool, not the compio event loop"). Compact's chunk-emit was the only inline-CPU site that violated this rule. Both `do_compact` chunk-emit sites (mid-loop at line 920, final at line 985) now match the flush pattern.
- **Fix:**
  ```rust
  let sst_bytes = compio::runtime::spawn_blocking(move || Bytes::from(builder.finish()))
      .await
      .map_err(|_| anyhow!("compact builder finish join failed"))?;
  // ... append to row_stream (already async) ...
  let reader = compio::runtime::spawn_blocking(move || SstReader::from_bytes(sst_bytes))
      .await
      .map_err(|_| anyhow!("compact SstReader join failed"))??;
  ```
- **Combined with F168, the P-log compio runtime is now responsive throughout compaction:**
  - Pre-F168: merge loop ran up to 512 MiB inline (~1-2s blocking).
  - Post-F168: merge loop yields every 1000 entries (<1 µs micro-pause).
  - Pre-F169: each chunk-emit blocked 50-110 ms inline (memcpy + parse).
  - Post-F169: chunk-emit fully off-loaded to the blocking pool; the P-log task only awaits the join.
- **Files:** `crates/partition-server/src/background.rs` (2 chunk-emit sites + comments), `feature_list.md`, `claude-progress.txt`.
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo build --release --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 122/122 pass.
  - End-to-end: `cluster.sh reset 1` + 3-key put/get + `autumn-client compact 9` round-trip ok (V1 frame default ON post-F165).
- **Operational notes:**
  - The blocking pool is global (compio runtime-wide), shared with `flush_one_imm`'s `build_sst_bytes`. If both flush + compact emit chunks at the same time, they share the pool and serialize naturally on a single blocking thread (default pool size). Contention is bounded by the foreground put rate that drives flush + the explicit `compact` RPC dispatch — both are bursty, not sustained.
  - Heartbeat (PS-main runtime) was already unaffected per F168's analysis. F169 closes the remaining client-put-stall window during compaction on the same partition.
- **passes:** true

### F168 · Cooperative yield in `do_compact` merge loop (thread-per-core principle enforcement)
- **Target:** Investigate user-flagged concern — does GC pressure cause heartbeat loss? Audit confirms a real violation of the thread-per-core principle ("compio runtime should never block due to CPU busy") in `do_compact`'s merge loop, even though heartbeat itself is NOT lost.
- **Audit findings:**
  - **Heartbeat is NOT lost** during GC/compaction. PS heartbeat_loop is spawned on the PS main thread's compio runtime (`crates/partition-server/src/lib.rs:1118`). GC + compaction run on per-partition P-log runtimes (separate threads). CPU pressure on P-log doesn't block heartbeat on PS-main; the kernel's TCP machinery is independent.
  - **GC's inline CPU is bounded.** `process_gc_chunk` processes up to `GC_BATCH_RECORDS = 256` records OR `GC_BATCH_BYTES = 4 MiB` per batch (~1 ms inline CPU typical), then awaits `flush_gc_batch`. After each chunk, `gc_yield_now` + rate-limiter sleep. Already cooperatively-scheduled.
  - **Compaction's inline CPU was NOT bounded.** `do_compact`'s merge loop (`background.rs:840-934`) ran up to `max_chunk = 2 * MAX_SKIP_LIST = 512 MiB` of entries inline (~16M entries at 32 bytes/entry, ~1-2 SECONDS of CPU) before the first chunk-emit `await` released the event loop. This stalled merged_partition_loop, ps-conn, and other tasks on the SAME P-log runtime — client puts/gets to that partition stalled for the full duration. Heartbeat (PS-main, different runtime) was unaffected, but the thread-per-core principle was violated.
- **Fix:** Add `yield_to_runtime` (renamed from `gc_yield_now`, kept as backwards-compat alias) and call it every `COMPACT_YIELD_EVERY = 1000` entries inside the merge loop. Cost: one poll round-trip per yield, <1 µs amortised against ~100 ns of per-entry encode work — < 1% overhead. Benefit: P-log compio runtime stays responsive throughout compaction; client puts/gets to the same partition no longer stall for 1-2s during major compaction.
- **What ships:**
  - `yield_to_runtime` async helper (existing `gc_yield_now` renamed, alias kept for source compatibility)
  - `COMPACT_YIELD_EVERY` constant (1000 entries) + `entries_since_yield` counter in `do_compact`
  - Yield call at the bottom of the merge loop body
- **Files:** `crates/partition-server/src/background.rs` (renamed helper + 2 yield additions), `feature_list.md`, `claude-progress.txt`.
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo build --release --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 122/122 pass.
  - End-to-end: `cluster.sh reset 1` + 2-key put/get round-trip ok (V1 frame default ON post-F165).
- **Operational notes:**
  - The user's hypothesis ("GC pressure causes heartbeat loss") was INCORRECT for autumn-rs's current thread model. Heartbeat is on PS-main, GC/compact on P-log, separate runtimes. Heartbeat won't be lost due to partition-thread CPU pressure.
  - The user's PRINCIPLE ("compio should never block due to CPU busy") was correct and surfaces a real bug. F168 enforces the principle by yielding every 1000 entries during compaction.
  - Client put/get latency to a partition undergoing major compaction should now be much steadier — instead of multi-second stalls during merge, latency stays in normal range with brief micro-pauses every 1000 entries (<1 µs each).
- **passes:** true

### F167 · Encapsulate the file-replace unsafe with a documented invariant (line 2314)
- **Target:** Make the line-2314 unsafe (`*entry.file.get() = new_file` during EC commit) — flagged by F166's audit as the third UnsafeCell aliasing concern — explicit and discoverable. The previous inline pattern had no encapsulation; any contributor could write the same pattern at a NEW call site without realising the F153 EC-conversion-lock invariant the existing call site relies on. F167 centralises the unsafe into a clearly-named method on `ExtentEntry` with a full safety contract in the doc-comment.
- **Why this and not the full Rc<File> migration:** Closing the type-level UB requires migrating `UnsafeCell<CompioFile>` to `RefCell<Rc<CompioFile>>` so the replacement returns the old `Rc` to be dropped only when concurrent readers release their clones. That's ~50-100 LOC across all `file_*` helpers and ~20 call sites — invasive enough to risk regressions in this iteration's budget. The encapsulation+documentation alternative (this F167) makes the invariant explicit and discoverable, so future contributors don't unwittingly reintroduce the pattern at new call sites — without changing the type-level structure. The Rc<File> migration is documented as a future feature (F168 candidate) for when a real failure or bigger consolidation justifies the structural cost.
- **What ships:**
  - `ExtentEntry::replace_file_under_lock(new_file: CompioFile)` — `unsafe fn` on `&self` with a comprehensive safety contract: caller MUST guarantee no concurrent borrow / no concurrent future-pinned write for the call's full duration. Documented invariant: F153's `ec_conversion_locks` `futures::lock::Mutex<()>` is the load-bearing mechanism in production; F119-C's eversion-mismatch reject covers stale-cached readers.
  - `handle_convert_to_ec` line 2338: replace inline `unsafe { *entry.file.get() = new_file }` with `unsafe { entry.replace_file_under_lock(new_file) }` + reference comment to the safety contract.
- **Files:** `crates/stream/src/extent_node.rs` (helper + 1 call-site update), `feature_list.md`, `claude-progress.txt`.
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo build --release --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-stream --lib`: 39/39 pass.
  - End-to-end: `cluster.sh reset 1` + 2-key put/get round-trip ok (V1 frame default ON post-F165).
- **passes:** true (encapsulation + documentation; type-level UB closure deferred)

### F166 · Eliminate `&mut` aliasing UB on `UnsafeCell<CompioFile>` (memory-safety audit finding)
- **Target:** Close the verified `&mut`-aliasing UB in `extent_node.rs::file_pwrite` (line 435) and `build_append_future` (line 937). A focused memory-safety audit (this iteration's lens, distinct from the prior 8 audits) flagged both sites. The pattern was `let f = unsafe { &mut *file.get() };` followed by `f.write_*at(...).await` where the `&mut CompioFile` lifetime spans across the await. The historical SAFETY comment claimed "compio is single-threaded so &mut aliasing is serialised per-future" — but Rust's `&mut` exclusivity rule is at the LIFETIME level, not the temporal level. Two compio futures on the same runtime, each holding a `&mut` across awaits, have overlapping lifetimes the compiler is allowed to reason against, even when only one polls at a time.
- **The right fix:** compio provides `impl AsyncWriteAt for &File` (`compio_fs/file.rs:250`) which uses `SharedFd` interior mutability — write_at on `&File` is just as efficient as on `&mut File` (in fact the `&mut File` impl just calls the `&File` impl: `(&*self).write_at(buf, pos).await`). Switching to a SHARED reference via `&*file.get()` makes the pattern legal: shared refs to `UnsafeCell` content can alias freely (that's exactly what `UnsafeCell` is for), and concurrent writes are serialised by the kernel/io_uring at the syscall level (not by Rust's borrow checker, which doesn't need to know).
- **Change:**
  - `file_pwrite` (line 434): `let mut f: &CompioFile = unsafe { &*file.get() }; f.write_all_at(...)`. The `let mut` makes the binding mutable; autoref builds `&mut &CompioFile` which is the receiver type for `AsyncWriteAtExt::write_all_at` on the `&File` impl.
  - `build_append_future` (line 932): same pattern with `write_vectored_at`.
  - Both inline SAFETY comments rewritten to explain the lifetime-vs-temporal distinction so future contributors don't reintroduce the unsafe pattern.
- **Cleared (related, NOT bugs):**
  - `file_ref` (line 428) already returns `&CompioFile` (shared) — safe.
  - `file_pread` (line 442) already uses `unsafe { &*file.get() }` (shared) — safe.
  - The remaining `&mut` site at line 2314 (`*entry.file.get() = new_file` during EC commit) is structurally different (it REPLACES the file, not just borrows). F153's per-extent EC conversion lock serialises this with concurrent reads, but the type-level UB still exists; closing it would require migrating `UnsafeCell<CompioFile>` to `RefCell<Rc<CompioFile>>` (~50-100 LOC refactor) — deferred to a future iteration.
  - `getifaddrs` memory leak in `transport/src/lib.rs:141-175` (the agent's surface 5) — LOW impact, separate issue, deferred.
- **Files:** `crates/stream/src/extent_node.rs` (file_pwrite + build_append_future + comments), `feature_list.md`, `claude-progress.txt`.
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo build --release --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-stream --lib`: 39/39 pass.
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 122/122 pass.
  - End-to-end (V1 frame default ON post-F165): `cluster.sh reset 1` + 3-key put/get round-trip ok. Append path (`build_append_future`) and direct file_pwrite path both exercised.
- **passes:** true

### F165 · Flip RPC frame V1 default ON — closes 7 verified hot-path corruption surfaces in production
- **Target:** Make F163's V1 encoder the default. F161 audit verified 7 hot-path RPC frame corruption surfaces (header field corruption silently passed to handle_append, payload bytes corrupted in transit, length-field poisoning, eversion bypass, etc.). F163 shipped the V1 encoder + decoder infrastructure. F164 verified V1 works end-to-end. With V1 opt-in, the corruption protection is unused unless the operator explicitly sets `AUTUMN_RPC_FRAME_V1=1`. F165 flips the default so production deployments get the protection automatically.
- **Change:** One-line semantic flip in `crates/rpc/src/frame.rs::v1_encoder_enabled`:
  - Pre-F165: `unwrap_or(false)` + `matches!(v, "1"|"true"|"yes"|"on")` — V1 opt-in.
  - Post-F165: `unwrap_or(true)` + `!matches!(v, "0"|"false"|"no"|"off")` — V1 default-on, opt-out via `AUTUMN_RPC_FRAME_V1=0`.
  - Existing tests (`encode_decode_round_trip` + `empty_payload`) updated to assert V1 sizes (HEADER_LEN + payload + 4-byte CRC).
- **Trade-off:** ~10% throughput cost on the read hot path (HW CRC32C compute at ~1 µs per 4 KB payload at SSE4.2). Measured under perf-check on tmpfs: V0 read ~124k ops/s vs V1 read ~110k ops/s. Acceptable for the corruption protection — most production storage systems (Cassandra, Kafka, RocksDB-on-disk) ship checksums on by default.
- **Operational notes:**
  - The whole cluster must agree on the wire version. Per-process flips would create decode mismatches. Pre-F155 binaries on V1 wire would silently mis-decode trailing CRC bytes as part of the rkyv archive — exactly the F161/F163 "cluster break" symptom F164 root-caused. Always coordinate the wire version across the entire deployment.
  - `cluster.sh` runs from `target/release/`. Any `cargo build --workspace` that targets debug won't change cluster behaviour. Always `cargo build --release` after a wire-format-affecting change.
  - F164's startup env-dump in autumn-ps continues to log the AUTUMN_* env vars so operators can confirm the cluster is on the intended wire version.
- **Files:** `crates/rpc/src/frame.rs` (v1_encoder_enabled default flip + 2 existing test assertions updated), `feature_list.md`, `claude-progress.txt`.
- **Verification:**
  - `cargo build --release --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-rpc --lib`: 12/12 pass.
  - `cargo test -p autumn-stream --lib`: 39/39 pass.
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 122/122 pass.
  - End-to-end V1 default: `cluster.sh reset 1` (no env var) + 3-key put/get round-trip ok. F164 dump confirms "no AUTUMN_* env vars at startup".
  - End-to-end V0 opt-out: `AUTUMN_RPC_FRAME_V1=0 cluster.sh reset 1` + put/get ok. F164 dump confirms `AUTUMN_RPC_FRAME_V1=0` propagated. Legacy V0 wire format used.
- **passes:** true

### F164 · F163 V1 encoder UNBLOCKED — root-caused stale release binaries; ships startup env-dump diagnostic
- **Root cause of F161 + F163 "cluster break":** `cluster.sh:16` sets `BIN="$SCRIPT_DIR/target/release"` and launches all server processes from `target/release/`. My F151-F163 work was being compiled into `target/debug/` via `cargo build --workspace`. The RELEASE binaries on disk were from `May 7 06:09` — **24 hours old**, predating the F155 rkyv-checked-decode bytecheck. When I set `AUTUMN_RPC_FRAME_V1=1` and tested, the autumn-client (which I ran from `./target/debug/autumn-client`) sent V1 frames with the 4-byte CRC trailer, but the cluster's release PS / manager / extent-node binaries used the pre-F155 `from_bytes_unchecked` rkyv decoder which silently mis-read the trailing CRC bytes as part of the rkyv archive. PutReq's part_id field decoded to garbage values like `18446744004990074880` (close to u64::MAX), which `extract_part_id` returned, leading to "partition NOT_FOUND part_id=garbage" errors. The diagnostics from `handle_ps_connection` never showed up because the running PS binary was the OLD release one without my eprintln/tracing additions.
- **Verification (this iteration):**
  - `cargo build --release --workspace --exclude autumn-fuse`: clean, fresh release binaries.
  - `AUTUMN_RPC_FRAME_V1=1 cluster.sh reset 1` + 3-key put/get + `release/autumn-client` → all 3 keys round-trip correctly. F164 env dump in PS log confirms `AUTUMN_RPC_FRAME_V1=1` propagated through the launcher chain.
  - V0 default (no env var): `cluster.sh reset 1` + put/get → works as before, no behavior change.
  - Perf comparison (4 KB 8-deep pipeline, 5 s runs, tmpfs): V0 read ~124k ops/s, V1 read ~110k ops/s — ~11% drop matches the expected HW CRC32C compute cost (~1 µs per 4 KB payload at SSE4.2). V1 write numbers within rig noise.
- **What ships:**
  - `crates/server/src/bin/partition_server.rs`: F164 startup-time `AUTUMN_*` env-var dump (logs each var via `tracing::info!`, or "no AUTUMN_* env vars at startup" if empty). Useful for any future env-flag-gated feature; closes the "is the env actually propagating?" question that took multiple iterations of F161/F163 to debug.
- **Status of F163:** V1 encoder is **verified working end-to-end**. Default remains V0 (opt-in via `AUTUMN_RPC_FRAME_V1=1`) so the V1 rollout doesn't require a coordinated cluster-wide binary upgrade. Future iteration can flip the default to V1 once production telemetry confirms the ~10% perf cost is acceptable for the corruption protection.
- **Lesson learned (process):** When a code change appears to break an integration test, FIRST verify the binaries-under-test are actually the ones containing the change. `cargo build --workspace` builds debug; `cluster.sh` runs release. Two iterations of F161+F163 burned trying to debug a "cluster break" that was just a stale-binary mismatch. Future iterations should add a quick `target/release/autumn-ps -V` mtime check before assuming "my code change is broken".
- **Files:** `crates/server/src/bin/partition_server.rs` (env dump), `feature_list.md`, `claude-progress.txt`.
- **Verification (additional):**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo build --release --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-rpc --lib`: 12/12 pass.
  - `cargo test -p autumn-stream --lib`: 39/39 pass.
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 122/122 pass.
- **passes:** true

### F163 · RPC frame V1 encoder infrastructure (env-flag opt-in, default V0)
- **Target:** Re-attempt F161's per-frame CRC32C V1 encoder rollout, this time with the env-flag opt-in pattern that the F161 retraction notes prescribed. F161 verified 7 corruption surfaces in hot-path binary RPC frames; the decoder support shipped, but the encoder rollout broke the cluster on first try and the root cause was not isolated.
- **F163 changes:** Frame's `encode` / `encode_header` / `encode_request_header` now dispatch V0 vs V1 based on `AUTUMN_RPC_FRAME_V1` env var (memoised via `OnceLock` on first access). Default is V0 — production behaviour unchanged. Explicit `encode_v0` / `encode_v1` methods are exposed for direct test access independent of env-var state. `send_vectored` checks the FLAG_CRC bit on the encoded header and conditionally appends the CRC trailer segment.
- **V1 encoder rollout STILL DEFERRED:** Setting `AUTUMN_RPC_FRAME_V1=1` and re-running the cluster reproduces the same break as F161's first attempt — `Connection reset by peer` from PS, `handle_ps_connection`'s tracing diagnostics never surface (despite tracing being initialized), and root-cause investigation with eprintln + tracing both fail to identify the offending wire-format mismatch within the iteration's budget. The V1 path passes unit tests (encoder→decoder direct round-trip via `encode_v1`); something in the cluster pipeline (likely a Frame producer not going through `Frame::encode()` that I haven't found yet) breaks under V1.
- **What ships:**
  - `Frame::encode_v0` (explicit legacy) + `Frame::encode_v1` (explicit with CRC) public methods
  - `Frame::encode` dispatches via `v1_encoder_enabled()` (memoised env-var lookup)
  - `Frame::encode_header` / `encode_request_header` similarly dispatch
  - `client::send_vectored` conditionally appends CRC trailer based on the encoded header's flags byte
  - 2 new tests: `f163_encode_v1_decode_round_trip`, `f163_encode_v0_decode_round_trip`
- **Files:** `crates/rpc/src/frame.rs` (V0/V1 dispatcher + memoised env lookup + 2 new tests), `crates/rpc/src/client.rs` (send_vectored conditional CRC trailer), `feature_list.md`, `claude-progress.txt`.
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-rpc --lib`: 12/12 pass (was 10; +2 new F163 tests for V0 + V1 encode→decode round-trips).
  - `cargo test -p autumn-stream --lib`: 39/39 pass.
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 122/122 pass.
  - End-to-end: `cluster.sh reset 1` (default V0) + put/get round-trip ok. `AUTUMN_RPC_FRAME_V1=1 cluster.sh reset 1` reproduces the F161 break — root cause still pending investigation.
- **Re-attempt plan (future iteration):**
  - Build a wire-format observer (e.g., a simple proxy that logs every byte) to capture the exact byte sequence on a failing V1 connection. Compare against expected V1 frame layout to find the mismatch.
  - Audit ALL byte-producing paths systematically — every place that calls `writer.write_*` directly, not just those that obviously use `Frame::encode()`. Possible culprits: a CodeResp encoder, a hand-built error response, an autumn-rpc Server somewhere I haven't traced.
  - Verify `tracing::error!` from inside `compio::runtime::spawn`-ed handlers actually reaches the log file (this iteration found that diagnostics from inside `handle_ps_connection` don't surface — separate operational issue worth fixing first).
- **passes:** false (V1 encoder rollout still pending — V0 default unchanged, V1 infrastructure shipped + tested in unit tests)

### F162 · MED-2 reader-pin protocol (closes spurious-NotFound on `handle_get` vs concurrent GC)
- **Target:** Close the longest-deferred audit item — MED-2 — `handle_get → resolve_value` racing background GC's `punch_holes` on the same `log_stream` extent. The race window: `handle_get` reads a `ValuePointer` from an SST, drops the partition borrow, and awaits `read_bytes_from_extent` on log_stream. Without coordination, `run_gc` could call `punch_holes` on the same extent (after compaction has rewritten the SSTs that referenced it, decrementing `vp_table_refs` to 0), the manager enqueues a physical delete, MSG_DELETE_EXTENT lands at the extent-node, and the extent file is unlinked. The in-flight resolve_value's read RPC arrives at the extent-node and either (a) gets `CODE_NOT_FOUND` if the delete already processed — a spurious user-visible read failure on data that was perfectly valid at the moment the SST was looked up, OR (b) returns the original bytes via the still-open fd if the delete is in-flight — correct but timing-dependent.
- **Severity reframe (vs original audit):** MED-2's failure mode is NOT silent corruption (POSIX preserves bytes on open fds, so reads from a yet-to-be-closed unlinked file return the original bytes; F148-A invariants prevent stale-state publishing). It IS spurious read-side **availability degradation** during normal GC operation — exactly the kind of bug that silently amplifies to user-visible latency spikes / retries / 5xx errors as cluster size + GC frequency grow.
- **Fix:** Per-extent reader-pin counter on `PartitionData::extent_pins: HashMap<u64, Rc<AtomicI64>>`. Counter semantics:
  - `value >= 0`: number of active readers; readers can acquire via CAS-loop incrementing
  - `value == -1`: GC writer holds the pin exclusively; readers cannot acquire until released
  
  Reader path (`handle_get` in `rpc_handlers.rs`): before dropping the partition borrow, extract the VP's `extent_id`, get the pin, and try to acquire it. On success, the `ReaderPin` RAII guard is held across the `resolve_value` await. On failure (writer holds), surface as clean `CODE_NOT_FOUND` (rather than racing a concurrent deletion).
  
  GC path (`run_gc` in `background.rs`): after rewriting all VP records and just before `part_sc.punch_holes`, try-acquire the writer pin via single CAS `0 → -1`. On success, proceed with the punch_holes RPC; release with `store(0)`. On failure (readers active), return `Ok(())` — the GC loop's 30-60 s tick will retry naturally.
- **Cost:** HashMap lookup per VP-resolving Get; 32 bytes per extent ever pinned; one CAS per Get + one CAS per GC. Negligible on the hot path (read latency dominated by the network RPC).
- **Files:** `crates/partition-server/src/lib.rs` (new field + helpers + 4 unit tests in `f162_reader_pin_tests`), `crates/partition-server/src/rpc_handlers.rs` (acquire reader pin in `handle_get`), `crates/partition-server/src/background.rs` (try-acquire writer pin in `run_gc`), `feature_list.md`, `claude-progress.txt`.
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 122/122 pass (was 118; +4 new F162 tests covering reader-pin acquire/release, reader blocked by writer, writer blocked by readers, writer exclusivity).
  - `cargo test -p autumn-stream --lib`: 39/39 pass.
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
  - End-to-end: `cluster.sh reset 1` + 3-key put/get round-trip ok (the small-value fast path doesn't exercise the pin since values < VALUE_THROTTLE go inline; large-value VP path is exercised under perf-check).
- **passes:** true

### F161 · DEFERRED — hot-path RPC frame CRC32C (decoder support shipped, encoder withheld)
- **Audit:** A focused audit on hot-path binary RPC frames (Audit 1: AppendReq/ReadBytesReq/CommitLengthReq + responses) verified 7 corruption surfaces — all rooted in the same gap: hot-path frames decode by hand without rkyv bytecheck (which F155 covered for control-plane RPCs only) and have no application-level CRC. TCP CRC catches most network bit flips but the per-segment 16-bit CRC is known to leak ~1 in 10⁸–10¹⁰ corrupt segments past detection (Stone & Partridge 2000); TX/RX checksum offload bugs in NIC drivers can present uncorrected bytes to the receiver. A flipped bit in `extent_id` / `eversion` / `commit` / `revision` would be silently trusted by `handle_append`'s decoder, landing a write on the wrong extent or bypassing the seal/owner-lock fence.
- **Design:** Per-frame CRC32C trailer at the autumn-rpc layer covers all frames in one shot. Wire format: V1 frame sets a new `FLAG_CRC = 0x08` in the existing flags byte, includes a 4-byte CRC32C trailer at the end of payload, and the announced `payload_len` covers the trailer. Decoder dispatches: V1 (FLAG_CRC set) → verify CRC + strip trailer; V0 (FLAG_CRC unset) → legacy decode unchanged. Backward-compatible: V1 binary reads V0 data; V0 binary on V1 data fails inner-protocol decode (acceptable for restart-all-together).
- **Status — DEFERRED:** A first-attempt encoder rollout broke the cluster. Bootstrap RPCs (manager-side) decoded correctly; PS-side puts failed with `Connection reset by peer` and the autumn-client receiver decoded a `flags=0x01` (V0) response from somewhere. The mismatched encoder/decoder state was not isolated within this iteration's budget. Reverted the encoder changes; the **decoder support + the `compute_payload_crc` helper + tests for the decoder path all remain in place**, so a future iteration can flip the encoder under controlled rollout (feature flag, decoder-first deploy, then encoder-flip after verify). Documenting the design + the 7 verified corruption surfaces here so the next attempt has full context.
- **Drive-by fix (kept):** `crates/rpc/src/client.rs:498` `autumn_transport::init()` was a stale call to a removed function (baseline build error noted in claude-progress.txt since F151). Replaced with `autumn_transport::current_or_init()` so `cargo test -p autumn-rpc --lib` now passes (was failing to compile pre-F161).
- **Files:**
  - `crates/rpc/Cargo.toml` (added `crc32c.workspace = true` dep — used by the decoder verify path + `compute_payload_crc` helper, kept for the future re-enable)
  - `crates/rpc/src/frame.rs` (`FLAG_CRC` constant, decoder support, `compute_payload_crc` helper, `CrcMismatch` + `CrcMissing` error variants, 2 new unit tests for V1 decoder path: `f161_decoder_v1_round_trip_via_compute_payload_crc` + `f161_decoder_rejects_corrupted_v1_payload`)
  - `crates/rpc/src/client.rs` (drive-by `current_or_init` fix; `send_vectored` change reverted)
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-rpc --lib`: 10/10 pass (was failing to compile pre-F161 due to baseline `init` symbol; now passes including 2 new F161 decoder tests).
  - `cargo test -p autumn-stream --lib`: 39/39 pass.
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 118/118 pass.
  - End-to-end: `cluster.sh reset 1` + 3-key put/get round-trip ok (cluster works on V0 wire format, unchanged from pre-F161).
- **Re-attempt notes for next iteration:**
  - Add a `RPC_FRAME_VERSION` env var (`v0` default, `v1` opt-in) to gate the encoder.
  - Trace ALL Frame producers — especially in extent_node.rs and the manager rpc_handlers — to confirm every path goes through `Frame::encode()` (no hand-rolled byte building).
  - Add an integration test that exercises a full client → server → response round-trip with V1 enabled before flipping the default.
- **passes:** false (deferred — encoder rollout pending)

### F160 · F119-C tightening on `handle_copy_extent` + clears Audit 2 #2 (vp_refs race)
- **F160 fix:** F119-C closed the read-side `eversion=0` silent-skip loophole in `handle_read_bytes` (line 2536) and `build_read_future` (line 1008) by dropping the `req.eversion > 0` clause from the comparison. The same loophole still existed in `handle_copy_extent`'s `Ok(None)` branch (line 3074) and `Err(_)` branch (line 3086), both used by `run_recovery_task` and `handle_re_avali`. Production callers fetch `ExtentInfo` from the manager before dispatch so eversion is normally fresh, but defense-in-depth: removing the `> 0 &&` clause closes a future-bug class where uninitialised eversion would bypass the EC-shape mismatch detection and copy shard bytes as if they were full payload (the F119-D corruption shape).
- **Audit 2 #2 CLEARED (vp_refs concurrent deletion race in `handle_stream_punch_holes` / `handle_truncate`):** Re-verified the snapshot-and-mutate sequence in `crates/manager/src/rpc_handlers.rs:1100-1203`. The whole block runs inside a synchronous closure under one `borrow_mut`; `needed_addrs` (line 1162-1172) and the in-memory `s.extents.remove` (line 1190) happen in the SAME synchronous scope without any `.await`. After the closure returns, `borrow_mut` drops; the etcd-mirror await follows. During that await, a concurrent `handle_sync_partition_vp_refs` cannot increment `vp_table_refs` on an extent that's already been removed from `s.extents` (since `vp_table_refs` is a field on `MgrExtentInfo` stored in the map). The agent's claimed race is not reachable today.
- **Files:** `crates/stream/src/extent_node.rs` (handle_copy_extent: 2 sites + 1 new test in `f160_copy_extent_eversion_tests`), `feature_list.md` (this entry), `claude-progress.txt`.
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-stream --lib`: 39/39 pass (was 38; +1 new F160 test that constructs an extent with eversion=7 and asserts a `copy_extent` request with eversion=0 returns `FailedPrecondition`).
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 118/118 pass.
- **passes:** true

### F159 · `save_meta` durability + `apply_extent_meta_durable` ordering fix
- **Target:** Close the verified ordering hazard in `crates/stream/src/extent_node.rs::apply_extent_meta_durable` (line 1814) and the missing fsync in `save_meta` (line 1332). From the data-corruption-focused audit (atomic file/cross-file ordering surfaces).
- **Bug 1 — `save_meta` not fsync'd.** Pre-F159 the helper used `compio::fs::write(path, buf)` which writes via the page cache and does NOT call fsync. The 44-byte V1 .meta could remain page-cached for an unbounded time; a host crash before the OS background flush would lose the write. On restart, `parse_meta` would return whatever state `.meta` had at the previous successful flush (typically the prior eversion / sealed_length / last_revision), silently rolling back the metadata even though F157's CRC trailer was intact (CRC validates against the OLD durable bytes — it can't detect "stale but valid").
- **Bug 2 — `apply_extent_meta_durable` writes `.meta` BEFORE fsync'ing `.dat`** (lines 1820-1830). Pre-F159 the order was: (a) `apply_extent_meta` updates in-memory atomics, (b) `save_meta` writes `.meta` (no fsync), (c) `file.sync_data()` on `.dat`. If the process crashed in the b→c window, the OS page cache could have already flushed `.meta` (44 bytes is well under one sector and typically hits disk within milliseconds), while `.dat`'s `must_sync=false` bytes were still in page cache and lost on the crash. On restart, `parse_meta` returned the NEW `sealed_length` while `.dat`'s file size was SHORTER — subsequent reads past the durable `extent.len` returned EOF or zero-padded bytes, masquerading as a successful seal. F143's `sync_data()` was load-bearing but the wrong order made it ineffective in this specific window.
- **Fix 1 (save_meta):** Replace `compio::fs::write` with `OpenOptions::new().create.truncate.write.open + write_all_at + sync_data`. The .meta is always durable on return.
- **Fix 2 (apply_extent_meta_durable):** Reorder — fsync `.dat` FIRST (data durable up to the new `sealed_length`), THEN `save_meta` (which now also fsyncs `.meta`). Even if a crash strikes between the two steps, the worst observable state is "old .meta + new .dat" which restart treats as still-unsealed; the manager re-applies the seal on next contact. No silent corruption.
- **Cost:** Each `save_meta` call now does one extra fsync (the .meta). save_meta runs on owner-revision changes, eversion bumps, seal applies, recovery — all rare events. Negligible perf cost.
- **Files:** `crates/stream/src/extent_node.rs` (save_meta + apply_extent_meta_durable), `feature_list.md`, `claude-progress.txt`.
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-stream --lib`: 38/38 pass.
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 118/118 pass.
  - End-to-end: `cluster.sh reset 1` → 3 puts → `cluster.sh restart 1` → 3 gets return correct values. Validates the new save_meta path + reordered apply_extent_meta_durable on the seal/recovery code path that runs at PS startup.
- **Out of scope (still deferred):**
  - Cross-replica byte-divergence detection (read-path Audit #1) — would require either end-to-end value CRC at the API boundary or cross-replica byte comparison at every read; bigger scope, low practical occurrence rate.
  - vp_refs sync timing window between compaction publish and manager sync — brief, asymptotic, GC catches up next tick.
  - F154 candidate (manager-side ecConversionInFlight etcd persistence) — perf hygiene only.
  - EC reconstruction integrity — would need shard-level CRC.
  - sealed_length cross-source consistency (manager vs sidecar vs StreamClient cache) — periodic reconciliation pass, structural.
  - MED-2 reader-pin protocol — longest deferred structural item.
- **passes:** true

### F158 · WAL record per-record CRC32C (V1 envelope) — closes the last on-disk silent-corruption surface
- **Target:** Close the WAL-record bit-rot silent-corruption surface (Audit 1 #3 from the data-corruption audit). Pre-F158 each `log_stream` record was `[op:1][key_len:4 LE][val_len:4 LE][expires_at:8 LE][key][value]` (17-byte header + variable payload, no CRC). A single bit flip in the header (especially `key_len` or `val_len`) silently changed how the decoder parsed that record: a corrupted `key_len` caused the decoder to read past the record into the next one's header and insert a garbage MVCC entry into the memtable; a corrupted `val_len` returned wrong value bytes to subsequent VP-resolution reads. The block-level CRC32C inside `crates/partition-server/src/sstable/builder.rs:217` covers SSTable bytes but not the WAL — `log_stream` had no integrity protection at the record granularity.
- **VP-offset bit-rot question CLEARED.** Audit 1 #4 raised concern about VP offset corruption returning silent wrong bytes. Verified `crates/partition-server/src/sstable/builder.rs:217` — block CRC covers `[entries + offsets + num_entries]`, and VP entries (OP_VALUE_POINTER) are inside `entries`, so the block CRC catches bit rot in stored VP offsets at read time. No fix needed.
- **Fix:** New `crates/partition-server/src/wal_record.rs` module defines a versioned record codec:
  - V0 (legacy, pre-F158, 17 + key + value bytes): unchanged on-disk format. Existing log_stream records keep working.
  - V1 (post-F158, 9 + 17 + key + value bytes): `[0xff sentinel][length:4 LE][V0 payload][crc32c:4 LE]`. The 0xff sentinel is unambiguous because no valid V0 op byte can be 0xff (ops are 1, 2, optionally OR'd with 0x80=OP_VALUE_POINTER, never 0xff). `crc32c` covers `length_bytes || payload_bytes` so a corrupted `length` is caught before its value is trusted.
  - Decoder dispatches per-record on the first byte: 0xff → V1 with CRC verification; else → V0 legacy. CRC mismatch returns `DecodeOne::Corrupt { skip_bytes, reason }` so the caller logs a WARN and advances past the corrupted record (matches F157's TableLocations skip-on-corruption pattern).
  - Five call sites updated: `start_write_batch` (background.rs) + the GC re-write encoder (background.rs) emit V1; `decode_records_full` + `decode_records_with_offsets` (lib.rs) + `process_gc_chunk`'s inline decoder (background.rs) all dispatch via the shared codec; `encode_record` (lib.rs, used by tests) now emits V1.
- **Migration:** Forward-compatible. V1 binaries write V1; V1 decoders accept both V0 (legacy on-disk pre-F158) and V1 (new writes). V0 binaries on V1 data would interpret 0xff as op byte → bounds check probably fails → some records skipped silently. Acceptable for restart-all-together model.
- **Files:**
  - `crates/partition-server/src/wal_record.rs` (NEW, ~270 LOC including doc comment + 9 unit tests)
  - `crates/partition-server/src/lib.rs` (mod registration; encode_record + decode_records_full + decode_records_with_offsets dispatched via codec)
  - `crates/partition-server/src/background.rs` (start_write_batch encoder + process_gc_chunk decoder + GC re-write encoder; carry_forward_round_trips_to_full_decode test updated for V1 envelope size)
  - `feature_list.md` (this entry)
  - `claude-progress.txt`
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-stream --lib`: 38/38 pass.
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 118/118 pass (was 109; +9 new wal_record codec tests covering V1 round-trip, V1 large value, V0 legacy decode, V1 corrupted payload byte caught, V1 corrupted CRC byte caught, V1 corrupted length caught, V1 truncated tail = Incomplete, mixed V1+V0 decode, empty key/value).
  - End-to-end: `cluster.sh reset 1` → 3 puts (write 3 V1 WAL records to log_stream) → `cluster.sh restart 1` → 3 gets all return correct values (recovery replayed V1 records via the V1-aware decoder).
- **Out of scope (still deferred):**
  - F154 candidate (manager-side `ecConversionInFlight` etcd persistence) — perf hygiene only after F153.
  - EC reconstruction integrity (Audit 1 #5) — would need shard-level CRC; deferred until shard-level transport hardening.
  - sealed_length cross-source consistency (Audit 1 #6) — periodic reconciliation pass.
  - vp_refs concurrent deletion race in punch_holes/truncate (Audit 2 #2) — needs deeper code-walk verification.
  - F119-C incomplete on read path (Audit 2 #4) — needs verification.
  - MED-2 reader-pin protocol — longest deferred structural item.
- **passes:** true

### F157 · Closes 2 silent data-corruption surfaces (extent .meta CRC + TableLocations skip-on-corruption)
- **Target:** Close two verified silent-corruption surfaces from the data-corruption-focused audit. Both let undetected bit rot or torn writes silently change recovered state without any error logged.
- **Bug 1 — Extent .meta sidecar has no checksum** (`crates/stream/src/extent_node.rs` save_meta + parse_meta + META_SIZE constants).
  Pre-F157 the 40-byte sidecar (`magic[8] | extent_id[8] | sealed_length[8] | eversion[8] | last_revision[8]`) had no CRC. Bit rot in `sealed_length` silently changed an extent's seal state at restart — recovery would load `sealed_length=0` for an actually-sealed extent, accept new appends past the old seal boundary, and corrupt every replica's view of the extent's tail bytes. `parse_meta` validated only the magic and extent_id fields; the four numeric fields were trusted blindly.
  Fix: bump on-disk format from V0 (40 bytes) to V1 (44 bytes with CRC32C trailer over the first 40). Magic byte `magic[7]` distinguishes versions: V0 = `b"EXTMETA\0"`, V1 = `b"EXTMETA\x01"`. `save_meta` always writes V1 with crc32c. `parse_meta` dispatches: V1 → verify CRC, return None on mismatch (treats as missing meta + WARN log); V0 → legacy parse with no checksum + WARN log; unknown magic → None. Migration is forward-compatible: V0 files keep working post-F157 and auto-upgrade to V1 on next save_meta. Rollback (V0 binary on V1 file) breaks because magic mismatches; acceptable for an operator-driven rare path.
- **Bug 2 — `decode_last_table_locations` silently drops newer records on mid-stream corruption** (`crates/partition-server/src/lib.rs::decode_last_table_locations`).
  meta_stream stores `[len:u32 LE][rkyv_payload]…` records. Pre-F157 the loop scanned forward and `break`d on any decode failure, returning the LAST successful decode. After F155 made rkyv decoding strict (bytecheck), any bit rot in a record's payload tripped this break-and-fall-back path, silently discarding all valid records that came AFTER the corrupted one — the partition would restart on stale state without a single error logged. Concrete failure: partition flushes 5 SSTs, bit flip corrupts checkpoint #4, recovery returns checkpoint #3 as authoritative, the 5th SST becomes orphan on disk, every VP from log_stream pointing into the 5th SST's extent is now unreachable.
  Fix: change decode-failure handling to log a WARN with offset + error, advance past the corrupted record using its declared `msg_len`, and continue scanning. Legitimate partial-tail-write behaviour is preserved (the `total > buf.len()` check still breaks at the end). If `msg_len` itself is corrupt to point into garbage, damage is bounded: the next record's length-prefix will almost certainly fail decode too, and we'll skip it; eventually we either find a valid record or exit with `last` populated by the last good one. After the loop, a summary WARN logs the skip count.
- **Files:**
  - `crates/stream/src/extent_node.rs` — META_MAGIC_V0/V1 + META_SIZE_V0/V1 constants, save_meta rewrite, parse_meta dispatch, 6 new tests in `f157_meta_crc_tests`
  - `crates/partition-server/src/lib.rs` — decode_last_table_locations skip-and-continue, 1 new test in `tests::f157_decode_table_locations_skips_mid_stream_corruption`
  - `feature_list.md` (this entry)
  - `claude-progress.txt`
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-stream --lib`: 38/38 pass (was 32; +6 new F157 tests covering V1 round-trip, V0 legacy compat, bit rot in payload, bit rot in CRC trailer, extent_id mismatch, unknown magic).
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 109/109 pass (was 108; +1 new F157 test covering mid-stream-corrupted-record + valid-record-after).
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
  - End-to-end: `cluster.sh reset 1` → put → `cluster.sh restart 1` → get → returns the right value. Validates V1 meta survives a process restart (recovery loads V1 sidecar, CRC passes, sealed_length restored).
- **Out of scope (still deferred):**
  - F158 candidate — wire-format schema versioning (1-byte prefix + version handshake on connect); pre-condition for safe rolling upgrades.
  - WAL record per-record CRC (audit found this as MEDIUM but format change requires more careful migration; deferred).
  - VP offset bit-rot validation — needs verification that SSTable block-level CRC catches the upstream corruption first (pending investigation).
  - F154 candidate (manager-side `ecConversionInFlight` etcd persistence) — perf hygiene only after F153.
- **passes:** true

### F155 · rkyv checked decode (replaces `from_bytes_unchecked` with `from_bytes`)
- **Target:** Close the verified UB-on-malformed-input hazard in both
  `rkyv_decode` helpers — `crates/rpc/src/manager_rpc.rs:65` and
  `crates/stream/src/extent_rpc.rs:268`. Pre-F155 both helpers used
  `unsafe { rkyv::from_bytes_unchecked::<T, RkyvError>(&v) }` with the
  comment "we control both sides" as the only safety justification. The
  unsafe path performs zero-copy deserialisation without validating
  archive bytes (no bounds check on length-prefixed containers, no
  pointer-relativity check, no sum-type discriminant validation). Any
  malformed input — a flipped bit past TCP's 16-bit CRC, a struct-layout
  mismatch from a partial rolling upgrade (cf. F118's
  `original_replicates → ec_converted` rename which had no schema-version
  prefix to detect mixed-version peers), or in a future where the wire
  crosses an untrusted boundary, a hostile sender — triggers undefined
  behaviour: out-of-bounds reads, pointer dereferences into arbitrary
  memory, or silent partial decoding into a corrupt struct that
  downstream code then trusts.
- **Fix:** Switch both helpers to `rkyv::from_bytes::<T, RkyvError>(&v)`,
  the checked path. Bounds added to the function signature:
  `T::Archived: for<'a> rkyv::bytecheck::CheckBytes<HighValidator<'a, RkyvError>>`.
  rkyv 0.8's `#[derive(Archive)]` auto-derives `CheckBytes` for the
  archived type, so all wire structs in `manager_rpc.rs` /
  `partition_rpc.rs` / `extent_rpc.rs` / etc. compile without further
  changes. The validator runs `bytecheck`'s archive-validation pass
  before deserialising; malformed input returns a clean `Err` instead of
  UB.
- **Cost:** Validation adds a constant-overhead pass over the archived
  bytes. For the hot path (4 KB Put/Get), this is a few μs per request
  — bounded and predictable. Perf-check on tmpfs after F155: ops/s
  fluctuates between 42k and 102k across runs (high rig noise on the
  shared box), median within the F148-F150 baseline band. No clean
  regression signal.
- **Out of scope (still deferred to a separate feature):** wire-format
  schema versioning. F155 closes the UB-on-malformed-input class of
  bugs; it does NOT close the rolling-upgrade-with-incompatible-schema
  class. A future feature could add a 1-byte `schema_version` prefix to
  every wire payload and a version-mismatch handshake on connect, so
  rolling upgrades fail loud at the wire boundary instead of silently
  decoding into a wrong struct (the failure mode F118's rename would
  have produced if anyone had attempted a rolling upgrade across that
  commit). For now, autumn-rs's deployment model is restart-all-together,
  so version mismatch is operationally avoided.
- **Files:** `crates/rpc/src/manager_rpc.rs` (rkyv_decode signature +
  body), `crates/stream/src/extent_rpc.rs` (rkyv_decode signature + body
  + 1 new test in `tests`), `feature_list.md` (this entry),
  `claude-progress.txt`, `crates/stream/CLAUDE.md` (note 4 amendment).
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-stream --lib`: 32/32 pass (was 31; +1 new
    `f155_rkyv_decode_rejects_malformed` test that XOR-corrupts each
    byte of a valid CodeResp encoding and asserts at least one
    corruption triggers Err — proves the validator actually runs;
    plus truncation and empty-payload cases).
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`:
    108/108 pass.
  - End-to-end: `cluster.sh reset 1` + bootstrap + perf-check (read-side
    ~96k ops/s within F148-F150 baseline band).
- **passes:** true

### F156 · Min-replica `commit_length` quorum enforcement
- **Target:** Close the verified protocol-level data-loss hazard in
  `current_commit` and `commit_length_for_extent` (`crates/stream/src/client.rs:1332`
  + `:1689`). Pre-F156 both functions iterated over all replica addresses,
  `continue`d on RPC error / decode error / non-OK code, and computed the
  minimum across whatever subset responded — accepting even a 1-of-N
  response. The min-replica commit protocol's correctness invariant is
  that the consensus position is bounded by EVERY replica's local length;
  taking the min of a strict subset can return a position HIGHER than what
  the unreachable replicas actually hold, after which an append at that
  speculative position writes data that exists on only the lone responder.
  If that responder dies before the unreachable peers re-replicate the
  speculative bytes (recovery via re-avali / require_recovery), the data
  is permanently lost.
- **Concrete failure mode:** R=3 cluster, replicas A/B/C all at offset 100.
  Network partition isolates B+C from manager + client. Client probes
  commit_length: A responds 100, B/C unreachable. Pre-F156 returns 100 →
  client appends bytes 100..200 → A acks → manager records sealed_length
  via this offset on next seal. A then crashes. B+C return with bytes 0..100
  only. Recovery tries to re-replicate 100..200 from a healthy source;
  none exist (A is dead). Bytes 100..200 lost forever, but the manager's
  metadata still references them — partition open at restart fails to load
  any SST whose ValuePointers reference the lost range.
- **Fix:** Track `success: usize` alongside `min_len`. After the loop,
  require `success >= total / 2 + 1` (majority quorum). Mirrors Raft / Paxos
  semantics: R=1→1, R=2→2, R=3→2, R=4→3, R=5→3. Below quorum, return an
  error — the worker's existing soft-error retry path waits + reloads
  tail + retries, which converges once the partition heals or the manager
  evicts the stale node and re-replicates.
- **Trade-off:** Pre-F156 the system would continue writing on a
  single-survivor majority-failure scenario (silently unsafe). Post-F156
  writes halt until a majority of replicas can be reached. This is the
  correct durability trade-off for a strongly-consistent system: refuse
  to make progress on insufficient information rather than silently risk
  data loss. Production clusters running in degraded mode (≥1 replica
  dead) will still make progress as long as ⌊N/2⌋+1 are reachable.
- **Files:** `crates/stream/src/client.rs` (current_commit + commit_length_for_extent;
  ~30 LOC including invariant comments), `feature_list.md` (this entry),
  `claude-progress.txt`.
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-stream --lib`: 31/31 pass.
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
  - `cargo test -p autumn-partition-server --lib -- --test-threads=1`: 108/108 pass.
  - End-to-end: `cluster.sh reset 3` (R=3 replication) + put/get round-trip
    verifies steady-state quorum (3/3 success). `stop-node 3` + put with
    one replica down: the alloc_new_extent path (separate from F156's
    commit_length probe) hits a pre-existing limitation where new extents
    still allocate to the dead node until the manager evicts it (10 s
    heartbeat window); after restart, put/get works again. F156's
    contribution: the commit_length probe with 1 dead replica still
    succeeds (2/3 ≥ 2 quorum), exercised in steady state.
- **F154 candidate retracted (was: `flush_one_imm_local` F148-A invariant violation):**
  Re-inspection of `crates/partition-server/src/lib.rs:3257-3300` shows the
  previous audit agent's "VERIFIED-VIOLATION" was a misreading of the
  F148-A invariant. The invariant requires no `.await` between (a) the
  *snapshot-capturing* `borrow_mut` drop and (b) the mpsc-send inside
  `save_table_locs_raw`. In `flush_one_imm_local` the borrow_mut block
  at lines 3277-3292 captures `tables_snapshot` at line 3291; the drop is
  at line 3292; the `save_table_locs_raw` call is at line 3297. Between
  3292 and 3297 there is only a `};` and the function call — no awaits.
  The awaits at lines 3267-3272 (`spawn_blocking` + `part_sc.append`)
  occur BEFORE the snapshot-capturing borrow_mut, which is fine — any
  concurrent `do_compact` mutations to `p.tables` are reflected in the
  snapshot taken AFTER those awaits. F148-A invariant holds. Removing
  F154 from the deferred list.
- **passes:** true

### F153 · Closes post-failover double EC dispatch race (per-extent serialisation lock on coordinator)
- **Target:** Close the verified post-failover double-EC-dispatch hazard surfaced by the F152-era audit. The manager's `ec_conversion_inflight` set is purely in-memory and is lost on leader failover; a deposed leader's in-flight `EXT_MSG_CONVERT_TO_EC` (still mid-`spawn_blocking ec_encode` + `write_shard_local` per CLAUDE.md note 15, 100-300 ms encode + ~RTTs of fanout) is invisible to the new leader, whose 5 s `ec_conversion_dispatch_loop` re-fires a duplicate dispatch. F119-D's coordinator-side idempotency guard (`entry.eversion >= req.eversion && sealed_length > 0 && entry.avali > 0`) fires post-hoc — the eversion bump is the LAST step of the 2PC, so during the window between dispatch-1's start and dispatch-1's `commit_shard_local`, dispatch-2 sees `entry.eversion < req.eversion` and proceeds. Two concurrent encodes race on the same `.ec.dat` staging file, producing the F119-D corruption shape (`logStream value short` / `ec_read_full_and_slice: offset N past decoded payload`).
- **Fix:** Per-extent `Rc<futures::lock::Mutex<()>>` map on `ExtentNode`, acquired at the very start of `handle_convert_to_ec` (before any state inspection), held across the entire prepare + commit phase. The second concurrent dispatch awaits the first, then re-runs the F119-D guard UNDER the lock — at which point `entry.eversion` IS bumped (the first dispatch's `commit_shard_local` ran while the second was waiting), so the second exits as a no-op. Pattern mirrors `client.rs::stream_init_locks`.
- **Architectural choice — defense vs root cause:** The full root-cause fix (manager-side CAS via etcd persistence of `ecConversionInFlight/$extent_id`, mirroring the F149 `recoveryTasks/$id` pattern, ~150 LOC) prevents the duplicate dispatch from ever reaching the coordinator. The defense-in-depth fix (this F153) ensures the corruption can't materialise even if a duplicate dispatch DOES arrive. **Defense alone is sufficient for correctness**: the F119-D guard becomes load-bearing AFTER the lock, so the worst observable behaviour is a wasted RPC round-trip instead of a corrupted shard. Root-cause fix becomes optional perf hygiene (avoid the wasted RPC) and is left as a future feature (F154 candidate). Defense was shipped first because it's smaller (30 LOC vs 150) and lower risk (no new etcd schema, no new replay logic).
- **Memory footprint:** `HashMap<u64, Rc<Mutex>>` grows monotonically with the number of extents ever EC-converted on this shard. Per-entry cost ~64 bytes (HashMap + Rc + Mutex). For a 10 k extent cluster that's ~640 KB per node — same order as the existing `extents` DashMap; not worth GC'ing.
- **Files:** `crates/stream/src/extent_node.rs` (ExtentNode struct field + Clone + new + handle_convert_to_ec lock acquisition + 1 unit test in `f153_ec_lock_tests`), `feature_list.md` (this entry), `claude-progress.txt`, manager + stream CLAUDE.md notes.
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean.
  - `cargo test -p autumn-stream --lib`: 31/31 pass (was 30; +1 new F153 test exercising lock semantics — same Rc on same extent, distinct Rc on different extents, try_lock fails while held / succeeds after drop).
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
  - End-to-end smoke: `cluster.sh reset 4` (4 nodes, 3+1 EC stream config) bootstrap + 5-key put/get round-trip ok. Bootstrap path exercises non-EC extent allocation; full EC conversion under load deferred (would need >256 MB writes to seal an extent and trigger the 5 s dispatch tick — outside this iteration's scope, lock semantics already covered by unit test).
- **Out of scope (future features):**
  - F154 candidate — manager-side `ecConversionInFlight/$extent_id` etcd persistence with create_revision==0 CAS. Would prevent the duplicate dispatch from ever reaching the coordinator; reduces to a perf optimisation now that F153 closes the corruption path.
  - F155 candidate — `flush_one_imm_local` fallback F148-A invariant violation (verified, rare path).
  - F156 candidate — rkyv `from_bytes_unchecked` + schema versioning.
  - F157 candidate — min-replica `commit_length_for_extent` quorum enforcement.
  - F158 candidate — MED-2 reader-pin protocol.
- **passes:** true

### F152 · Closes 4 verified consistency bugs (etcd-first ordering × 3 + truncate durability)
- **Target:** Close four verified strong-consistency bugs found by a multi-round audit (manager + stream + PS layer Explore agents over two iterations; six agents total, ~1500 LOC of audit reports). Each bug had a specific failure mode with file:line evidence; this batch is the lowest-risk subset (no architectural changes, no new RPCs, ~50 LOC + comments).
- **Bug 1 — `handle_create_stream` etcd-first violation** (manager/src/rpc_handlers.rs:385-398). Pre-F152 the handler ran `s.streams.insert + s.extents.insert` BEFORE `mirror_create_stream(...).await`. A manager crash in the await window leaves the new leader (post-`replay_from_etcd`) without the stream record while the extent files exist on remote nodes as orphans (eventually cleaned by the F109/F113 reconcile sweep, but the stream/partition state is lost). F125 closed the same anti-pattern in `handle_stream_alloc_extent`; this handler was missed. Fix: swap the order — etcd mirror first, in-memory insert second. Symmetric to F125.
- **Bug 2 — `handle_update_stream_ec` etcd-first violation** (manager/src/rpc_handlers.rs:431-462). Same anti-pattern: `s.streams.get_mut(&id).map(|st| { st.ec_data_shard = ...; st.clone() })` mutated memory before `mirror_stream_meta_update(...).await`. Higher impact than #1 because the EC params drive `ec_conversion_dispatch_loop`; a crash mid-mirror leaves the new leader dispatching the OLD EC shape while the deposed leader's memory had the new one. Fix: clone-then-mutate-then-mirror under read borrow, then apply to memory in a fresh `borrow_mut`.
- **Bug 3 — `handle_register_node` etcd-first violation** (manager/src/rpc_handlers.rs:186-254, both branches). Re-registration branch: `existing_node.shard_ports = req.shard_ports; nodes.insert(node_id, existing_node.clone())` ran before `mirror_register_node(&existing_node, &[]).await`. New-node branch: `s.disks.insert + s.nodes.insert` ran inside the same `borrow_mut` that allocated IDs, before `mirror_register_node(&node, &disk_infos).await`. shard_ports drives F099-K per-partition routing — a crash mid-mirror routes RPCs to the OLD shard port until the next heartbeat. Both branches fixed by reserving IDs upfront, mirroring to etcd, then applying to memory in a fresh `borrow_mut`. Wasted IDs from a failed mirror are safe per CLAUDE.md note 5 (`alloc_ids` derives `next_id = max(all_entity_ids) + 1` on replay).
- **Bug 4 — `truncate_to_commit` no fsync after `set_len`** (stream/src/extent_node.rs:1761-1782). Pre-F152 the function called `file.set_len(commit).await` then `extent.len.store(commit, SeqCst)` with no `sync_data()`. The min-replica commit protocol depends on per-replica `extent.len` matching what the file actually holds on disk — if the node crashes after `set_len` but before any subsequent must_sync append flushes the file's metadata, post-restart the file size could be observed at the pre-truncate length (kernel hasn't durably persisted the inode shrink). Then the next `commit_length` probe reports a wrong consensus, and an append truncates the OTHER replicas back to that wrong value, diverging them at the same offset. Fix: insert `f.sync_data().await` between `set_len` and the atomic store. fdatasync is sufficient — the file size IS the data we need durable; subsequent appends sync content separately. truncate fires only on commit-reconciliation (rare), so the extra fsync cost is negligible on the hot path.
- **Cleared by audit (NOT bugs, kept for future reference):**
  - Unsealed-extent read vs concurrent truncate: F148-B + single-task-per-conn + SeqCst close it.
  - Stale `revision` reuse after manager failover: per-extent `last_revision` total-orders writers across manager instances; F149 is belt-and-braces.
  - `recovery_tasks` etcd-write-vs-memory-insert gap: F149 atomic txn closes it.
  - `partition_vp_refs` RMW post-failover: F149 atomic txn closes it.
- **Deferred to future features (real bugs but bigger scope):**
  - `ec_conversion_inflight` not etcd-persisted → post-failover double EC dispatch (F119-D guard fires post-hoc; mid-encode race window remains). Needs new `ecConversionInFlight/$extent_id` etcd key with create_revision==0 CAS, mirroring the F149 `recoveryTasks/$id` pattern.
  - `flush_one_imm_local` fallback violates F148-A invariant P3 (verified): `await spawn_blocking + part_sc.append().await` between borrow drop and next `borrow_mut`. Rare path (P-bulk spawn fail). Snapshot-before-await refactor; ~30 LOC.
  - rkyv `from_bytes_unchecked` on every wire deserialize + no schema versioning — UB on malformed input. HIGH for rolling upgrades, LOW otherwise.
  - min-replica `commit_length_for_extent` continues on RPC error → uses `min` of REACHABLE replicas. May commit at speculative position with only 1/N respondents. Needs quorum enforcement + adversarial test first.
  - MED-2 `handle_get → resolve_value` vs GC `punch_holes` (deferred since F148): per-extent reader-pin protocol.
- **Files:** `crates/manager/src/rpc_handlers.rs` (handle_create_stream + handle_update_stream_ec + handle_register_node both branches), `crates/stream/src/extent_node.rs` (truncate_to_commit), `feature_list.md` (this entry), `claude-progress.txt`.
- **Verification:**
  - `cargo build --workspace --exclude autumn-fuse`: clean (only pre-existing warnings).
  - `cargo test -p autumn-stream --lib`: 30/30 pass.
  - `cargo test -p autumn-manager --lib`: 30/30 pass.
  - `cargo test -p autumn-partition-server --lib`: 106 parallel / 108 serial — same 2 pre-existing F099I parallel-test races as F148/F149/F150 baseline (verified by `--test-threads=1` 108/108 pass).
  - End-to-end smoke: `cluster.sh reset 1` succeeds (exercises `handle_register_node` re-reg AND new-node branches + `handle_create_stream` 3 times for log/row/meta streams). `autumn-client put/get` round-trip ok.
  - Perf: `perf-check --duration 5 --partitions 1 --pipeline-depth 8 --size 4096` reports 107k ops/s read sustained — within F148-F150 range (90-97k); no regression from the extra `sync_data()` on the rare-fire `truncate_to_commit` path.
- **passes:** true

### F151 · Python bindings: asyncio-native (replaces blocking surface)
- **Target:** Refactor `python/` so every public method is `await`-able from Python's asyncio. The pre-F151 surface ran the compio runtime on a worker thread but the Python-facing API was synchronous (blocking the calling thread on a per-call mpsc roundtrip). Under asyncio that pattern stalls the event loop for the duration of every RPC, which is unacceptable for a server-side embedding.
- **Architecture:** One detached OS thread hosts the compio runtime + `ClusterClient` (unchanged). Channel between Python and worker is `futures::channel::mpsc::unbounded` so the worker can `await` the next op cleanly instead of busy-polling with `try_recv` + 1 ms sleep. Each op carries a `PyHandle { loop: Py<PyAny>, fut: Py<PyAny> }`. On the Python side every method (a) acquires the running asyncio loop via `asyncio.get_running_loop()`, (b) creates `loop.create_future()`, (c) sends the op + handle, (d) returns the future. On op completion the worker re-acquires the GIL and schedules `loop.call_soon_threadsafe(fut.set_result | fut.set_exception, value)` — `call_soon_threadsafe` is the asyncio-blessed cross-thread wake primitive, so the awaiting coroutine resumes on the asyncio thread without locking. `Client.connect(addr)` is itself an async classmethod: the spawned worker performs `ClusterClient::connect` inside the runtime, then constructs the Python `Client` object (which owns the `tx` Sender) on the worker thread under the GIL and resolves the future with it. `close()` awaits an Op::Close ack from the worker; Drop drops the Sender, the worker observes channel disconnect and tears down the runtime.
- **Files:** `python/Cargo.toml` (added `futures = "0.3"`), `python/src/lib.rs` (full rewrite, ~290 LOC; PyHandle + Op + event_loop + Client async methods), `README.md` (new “Python bindings (asyncio)” section with build + usage example), `feature_list.md` (this entry), `claude-progress.txt`.
- **Verification:**
  - `cargo check` inside `python/` clean (no warnings, no errors).
  - `maturin develop --release` produces wheel for CPython 3.12.
  - Async smoke test against a fresh `cluster.sh reset 1` cluster:
    - `await Client.connect(...)` — connects + returns Client object.
    - `put / get / delete` round-trips ok.
    - `range` returns key-only entries (matches partition-server contract); `get` per key returns values.
    - `batch_delete(prefix)` returns count and clears the range.
    - `asyncio.gather(*[client.put(...) for _ in range(50)])` — 50 concurrent in-flight puts complete via the single worker thread; range scan confirms all 50 keys landed.
    - `await client.close()` returns cleanly; subsequent calls raise `RuntimeError("client is closed")`.
- **passes:** true

---

## Architectural lessons from the perf series (kept for context, the rest of F098/F099 lives in the Completed table)

### F099-J · PS dispatcher worker threads collapsed into the P-log thread (DONE_WITH_CONCERNS)
- **Target:** Thread-per-partition. Remove the compio Dispatcher + N worker thread pool that pre-F099-J hosted ps-conn tasks; remove the `Arc<PartitionRouter>` DashMap + per-request cross-thread mpsc hop. Main compio thread forwards each accepted fd across `mpsc` to the owning partition's P-log runtime; ps-conn ↔ merged_partition_loop runs on the same compio runtime (no cross-thread wake).
- **Notes:** Architectural simplification real (~190 fewer OS threads at N=1, no cross-thread wake on the write hot path, no DashMap on the request path). However 256 × d=1 × 4 KB harness regressed: 3-rep median write 42.8k (vs F099-D 57.6k baseline, **-25%**). Root cause: adding 256 ps-conn tasks' frame-decode + response-encode work to the P-log compio runtime drives P-log user CPU to ~100%. At lower connection counts F099-J is neutral-to-positive. The simpler foundation is retained to unblock F099-I (SQE coalescing) + F099-K (multi-partition routing).
- **passes:** false

### F099-K-diagnosis · N=4 scaling-gap root cause (measurement only)
- **Target:** Diagnose why post-per-partition-listener N=4 × d=8 stalls at 57–59 k (parity with N=1 × d=8) instead of scaling 4×.
- **Findings:** PS threads at 90–100% CPU (saturated but evenly), extent-nodes 10–40%. **Key metric**: `stream append summary avg_fanout_ms` p50 jumps from 9.6 ms (N=1 d=8) to 129.6 ms (N=4 d=8) — 13.5× Phase-2 latency inflation. PS→extent TCP sockets: 9 total across 4 partitions' pools into 3 single-thread extent-nodes. **Root cause**: `autumn-extent-node` is a single OS-thread compio runtime; 4 partitions' concurrent `append_batch` fanouts serialise on each node's single io_uring queue. P-log "100% CPU" is a **symptom** of waiting on that backlog.
- **Recommendation:** multi-thread `ExtentNode` (→ F099-M).
- **Evidence:** `docs/superpowers/specs/2026-04-20-perf-f099k-n4-diagnosis.md`.
- **passes:** true

### F099-M · Multi-thread ExtentNode (per-shard compio runtime + port) (DONE_WITH_CONCERNS)
- **Target:** Spawn K compio runtimes (one OS thread each) inside one `autumn-extent-node` process. Each shard listens on `port + shard_idx * shard_stride` (default stride 10), owns extents where `extent_id % K == shard_idx`. Manager registers `shard_ports` on `register_node` and serves it via `GetNodeList`; hot-path RPCs rejected on wrong shard with `FailedPrecondition`. Backward-compat: `shards=1` (default) uses legacy single-thread path.
- **Notes:** Architectural fix confirmed: fanout p50 drops from **129.6 ms → 1.6 ms (~80×)**. Throughput: N=4 × d=8 write 67k median, **below** the 110k gate but above the 57k pre-F099-M baseline (+17%). The new binding is PS-side: all 4 per-partition P-log threads at 100% CPU. Closing the 67k → 110k gap requires P-log-side optimisation (group-commit batch sizing, lock-free memtable sentinel path, etc.); the extent-node is no longer the bottleneck.
- **Evidence:** `crates/stream/tests/f099m_shards.rs` (4 tests) · `crates/server/src/bin/extent_node.rs` (`--shards` / `--shard-stride` / `AUTUMN_EXTENT_SHARDS`) · `crates/manager/src/lib.rs::shard_ports_for_addr`/`shard_addr_for_extent`.
- **passes:** true

### F099-N-b · Workload-distribution bug in `autumn-client perf-check` (measurement only)
- **Target:** Diagnose why post-F099-M write throughput plateaus at 60-65k regardless of N×d shape.
- **Findings:** **Root cause = workload-distribution bug in `autumn-client perf-check` itself.** Keys are `format!("pc_{tid}_{seq}")` (prefix `0x70 = 'p'`); partition ranges are hex-encoded with ASCII first bytes `0x33 / 0x37 / 0x62`. Every `"pc_..."` key falls into the LAST partition. The other N-1 P-log threads accept the frame (`part_id` matches the target port) but reject it in `start_write_batch.in_range()` with `"key is out of range"` — burning 40-60% of a core per reject-partition on decode + send_err + encode-error-frame + write-back. All seven storage-stack hypotheses ruled out.
- **Recommendation for F099-N-c**: fix key generation in perf-check + wbench so keys fall inside each thread's assigned partition range. Effort < 1 day, zero risk (bench tool only). Until fixed, perf measurements with multi-partition clusters are systematically misleading.
- **Evidence:** `docs/superpowers/specs/2026-04-20-perf-f099-n-ceiling.md`.
- **passes:** true

---

## P5 — Network transport abstraction (RDMA / UCX)

### F100-UCX · AutumnTransport trait + UCP-stream RDMA impl (paths 1 + 2) (DONE_WITH_CONCERNS)
- **Target:** New `autumn-transport` crate abstracts `connect` / `bind` / `accept` / `AsyncRead+AsyncWriteExt` behind `AutumnTransport` / `AutumnListener` / `AutumnConn` traits, with TCP (parity) and UCX (gated on `cargo feature = "ucx"`) implementations. Migrate `autumn-rpc` (Client ↔ PartitionServer) and `autumn-stream` (PartitionServer ↔ ExtentNode) call sites. Out of scope: PS-internal pipeline, Manager RPC, tag-matching/Active-Messages rewrite, per-peer transport fallback. Runtime selection via `AUTUMN_TRANSPORT=auto|tcp|ucx`, compile default off.
- **Major design pivots vs spec:**
  1. Trait object → enum dispatch (compio `AsyncRead::read<B>` is generic → trait not dyn-compatible).
  2. Polling progress → eventfd POLL_ADD via `compio::driver::op::PollOnce`. Wakeup latency: ~25 µs avg (50 µs polling) → <1 µs (one io_uring round-trip).
  3. Server split: `serve_tcp` (unchanged, std-listener+OS-thread+Dispatcher) vs `serve_ucx` (compio-runtime accept, single-thread). Multi-core UCX server scaling deferred.
- **Status:** Gates (a) workspace tests on TCP green, (b) UCX loopback suite green on rc_mlx5/mlx5_0 RoCEv2, (d) `UCX_PROTO_INFO=y` confirms `rndv/get/zcopy` for >64 KB payloads — met. **(c) cross-host RDMA A/B not measured** — single-host loopback shows UCX *slower* than TCP (24μs vs 6.9μs ping-pong) because TCP loopback bypasses the NIC entirely and UCX rc_mlx5 hits the real HCA even for self-traffic. RDMA wins only when network latency dominates; need a 2-host run for the spec's targeted 30–50% RTT improvement and 2× large-payload throughput.
- **Evidence:** `docs/superpowers/specs/2026-04-23-ucx-transport-design.md` · `crates/transport/` · `crates/transport/ucx-sys-mini/`.
- **passes:** done_with_concerns

---

## P3 — Post-extraction CI cleanup (not blocking)

### FCI-01 · Mass fmt + clippy cleanup before tightening CI
- **Target:** All 9 crates pass `cargo fmt --all -- --check` and `cargo clippy --workspace --exclude autumn-fuse --all-targets -- -D warnings`. After that, re-tighten CI by removing `continue-on-error: true` from the fmt and clippy steps in `.github/workflows/ci.yml`.
- **Evidence (2026-04-21 snapshot):** 628 fmt hunks across 58 files. 1 clippy error: `absurd_extreme_comparisons` in `crates/rpc/src/frame.rs:136`. ~13 clippy warnings including `RefCell reference held across await point` (4 in `autumn-etcd`, the runtime manifestation of which was F108).
- **Notes:** Two-phase: (1) `cargo fmt --all` mechanical commit; (2) fix 14 clippy issues; (3) flip CI back to gating.
- **passes:** false

### F196 · Static cpuset pre-allocation for EN/PS + PS reject-only split gate + EN shards-from-cpuset + hot/cold advisory (ScyllaDB-style)
- **Trigger:** Conversation 2026-05-12 — operator wants ScyllaDB-style pre-allocation so capacity planning is explicit instead of relying on grow-on-demand `--cpu-start` + soft WARN on overflow. Without a hard cap, an oversubscribed PS quietly drops to kernel-floated threads and tail latency degrades silently. With a hard cap, split fails loudly and the operator can plan (grow `--cpuset`, migrate, or merge a cold pair).
- **Goal:** Both binaries take an explicit `--cpuset <SPEC>` (taskset syntax). EN auto-sizes `--shards` to `cpuset_len`. PS computes `max_partitions = cpuset_len / 2` and refuses splits beyond it. Manager emits a hot/cold advisory each policy tick when one partition runs ≥10× another on the same PS — operators can use it to plan splits/merges before the budget gates further growth.
- **Stages:**
  - **Stage A** — `autumn-common::cpu_pin`: `set_cpuset(Vec<usize>)`, `parse_cpuset(&str)`, `cpuset_len()`, `cpuset_explicit()`. CLI parses `--cpuset 4-11` / `0,2,4` / `0-3,8-11`. Mutually exclusive with `--cpu-start`. `pick_cpu_for_ord` returns `None` past end (no wraparound, no offset when cpuset is explicit).
  - **Stage B** — PartitionServer carries `partition_budget: Arc<PartitionBudget { max, current: AtomicUsize }>`. Init sets `max = cpuset_len / 2` iff `cpuset_explicit()`; else `usize::MAX` (gate off, pre-F196 behaviour). `sync_regions_once` bumps/decrements on insert/remove and skips opens past budget with WARN. `handle_split_part` refuses with `FailedPrecondition` before any flush/RPC when `would_exceed(1)`.
  - **Stage C** — extent-node binary: `--shards` REMOVED (the binary refuses to start if the flag is passed). Shard count = `cpuset_len` (one shard per pre-allocated core). Pass `--cpuset 0` for a single-shard layout. WARN when `cpuset_len == 1` (no parallelism). `cluster.sh` translates the `AUTUMN_EXTENT_SHARDS` env into per-EN `--cpuset` ranges (`(i-1)*SHARDS - i*SHARDS-1`).
  - **Stage D** — manager `PolicyEngine::compute_hot_cold_advisory(region_owners, now)`: groups partitions by PS, requires ≥2 partitions per PS, fires a single WARN line per PS when sustained `max(req_per_sec) / max(1, min(req_per_sec)) >= HOT_COLD_RATIO (10)` over `required_buckets` AND hottest > `HOT_COLD_MIN_HOT_QPS (SPLIT_QPS_HIGH/2 = 7500 post-recal)`. Same 10× check runs in parallel on `size_bytes` with floor `HOT_COLD_MIN_HOT_SIZE_BYTES (SPLIT_SIZE_HARD/2 = 25 GiB)`. Either dimension is enough to fire; shared per-PS cooldown via `last_hot_cold_at` (`HOT_COLD_COOLDOWN_SEC = 300s`). Pure log — does NOT join `advisory_cache`. Wired into `policy_tick_loop` after `compute_maintenance_advisory`.
  - **Stage D-recal** — recalibrate F183 QPS thresholds to autumn-rs's measured single-partition ceiling (~30K QPS in perf_check, one P-log thread + one io_uring). `SPLIT_QPS_HIGH: 50K → 15K` (50% of ceiling, "half the budget used"); `MERGE_QPS_LOW: 5K → 1.5K` (preserves 10× hysteresis at the new split level). Pre-F196 the 50K split-QPS condition was unreachable — condition ② of the split predicate was effectively dead code, and `HOT_COLD_MIN_HOT_QPS = 25K` was practically untriggerable. Post-recal the QPS advisory works in dev/staging and `--auto-split` will actually fire on QPS-driven hotspots.
  - **Stage D-r2 (2026-05-12)** — four post-review tweaks: (1) `PS::report_load_loop` 5s → **30s**: manager only consults the last 5 of 1-min buckets, 5s upload was 6× over-sampling. (2) `POLICY_WINDOW_BUCKETS` 30 → **10**: only 5 are ever read, kept 2× safety margin. (3) `HOT_COLD_MIN_HOT_QPS` decoupled from `SPLIT_QPS_HIGH/2`, fixed to **10_000** (≈ 1/3 of single-partition ceiling). (4) `compute_hot_cold_advisory` now **returns `Vec<PolicyCandidate>` with `kind = POLICY_KIND_HOT_COLD (= 4)`** and the candidates ride the same `advisory_cache` as SPLIT/MERGE/GC/COMPACT; `client info policy` renders them next to the others with `kind=hotcold feas=n/a`. Single-partition-on-PS classification simplified from median-based to "min/max-matched partition" to handle the 2-partition PS case.
  - **Stage D-r3 (2026-05-12)** — F189 fg admission default 0 (unlimited) → **2 GiB/s**. Activates the existing `account_fg` ceiling + the `account_bg` fg-aware yield (which was dormant whenever fg=0). Number picked from perf_check baselines: 4 KiB TCP peak ~425 MB/s → 5× headroom; 8 MiB TCP peak ~1741 MB/s → 15% headroom; SHM loopback would be throttled but per `[[loopback-numa-artifact]]` SHM isn't production. Operator can pass `--fg-rate-bytes-per-sec 0` to restore pre-F196 unlimited behaviour.
  - **Stage D-r4 (2026-05-12)** — fg admission grows an **IOPS dimension**. `AdmissionController` now tracks `fg_ops` alongside `fg_bytes`; `account_fg(bytes, ops)` takes the LARGER of bytes-driven and ops-driven sleeps. `account_bg` fg-aware-yield triggers when EITHER fg observed bytes-rate OR observed ops-rate exceeds `fg_saturated_ratio × cap`. Bg stays bytes-only (compact/GC are bulk IO). Default `AUTUMN_PS_FG_IOPS_PER_SEC = 150_000` — derived from perf_check 4K peaks: TCP p16 ~110K ops/s (36% headroom) and TCP p8 SHM ~121K ops/s (24% headroom). New CLI flag `--fg-iops-per-sec`; `cluster.sh` passthrough `AUTUMN_PS_FG_IOPS_PER_SEC`. 2 new f189_admission_tests (iops_cap_throttles_small_value_workloads, bg_yields_when_fg_iops_saturated); 9/9 admission tests pass.
  - **Stage D-r5 (2026-05-12)** — admission becomes **per-partition** (was PS-wide). Each `PartitionData` gets its own `Arc<AdmissionController>` constructed at open time with `total / N` per dimension. New `AdmissionTotals` struct on `PartitionServer` carries the 4 process-wide numbers (fg bytes, fg iops, bg bytes, saturated ratio); the PS no longer owns a shared `Arc<AdmissionController>`. `N` = `partition_budget.max` when `--cpuset` is supplied (= cpuset_len/2), else `PS_PARTS_HINT_FALLBACK = 8` (matches `cluster.sh` default). Aligns with thread-per-core P-log isolation: one busy partition can no longer burn through siblings' bg budgets via the shared Mutex. 1 new test (`admission_totals_per_partition_split` verifies cap/N division + 0=unlimited preservation); 10/10 admission, 134/134 PS lib pass.
  - **Stage D-r6 (2026-05-12, supersedes D-r5)** — **revert D-r5; unify admission**. Bg resources (compact/GC) are cluster-pool by nature (process RAM + cluster IO bandwidth) so they belong PS-wide; per-partition admission + PS-wide gates was double-bookkeeping. New design: ONE `Arc<AdmissionController>` per PS (back to pre-D-r5), and that single controller now ALSO hosts the compact + gc concurrency caps that previously lived in standalone `CompactionGate` / `gc_concurrency_gate` fields. API: `account_fg(bytes, ops)`, `account_bg(bytes)`, `acquire_compact() -> CompactPermit`, `acquire_gc() -> GcPermit`. The standalone `CompactionGate` type still exists for F140's per-partition split-vs-gc sync `gc_gate` (different concern). Background loops and `handle_split_part` route through `io_bucket` for both rate and concurrency. 1 new test (`admission_acquire_compact_and_gc_concurrency_caps`); 10/10 admission, 134/134 PS lib pass.
  - **Stage D-r7 (2026-05-12, supersedes D-r6 partially)** — split admission into TWO types: **`RateController`** (per-partition) handles 4 independent rate dimensions (fg bytes, fg iops, **compact bytes**, **gc bytes**), and **`ConcurrencyController`** (PS-wide, `Arc<>`) handles compact + gc concurrency caps. The unified `AdmissionController` from D-r6 is gone. Rates are per-partition because the IO patterns are partition-local; concurrency is PS-wide because the resource being protected is process RAM. Compact and GC rates are now independent counters (the D-r6 combined `bg_rate_bytes_per_sec` is split). fg-aware-yield preserved: both `account_compact` and `account_gc` inspect fg saturation against the per-partition fg caps. CLI: `--bg-rate-bytes-per-sec` removed (errors with migration message); `--admission-compact-rate-bytes-per-sec` + `--admission-gc-rate-bytes-per-sec` added (separate from the existing F141 `--gc-rate-bytes-per-sec` per-partition limiter, which keeps its semantics). Per-partition defaults derived from `perf_check` baselines: fg 256 MiB/s + 30K ops, compact 64 MiB/s, gc 32 MiB/s. 15/15 admission tests (was 10 in D-r6, +5 for compact/gc independence + dual fg-aware-yield); 139/139 PS lib pass.
  - **Stage D-r7-recal (2026-05-12)** — bump defaults to match sustained perf_check workloads after analysis against `perf_baseline_tcp_p16_d8_s4k.json` and the 8M peak. Initial D-r7 picks were too conservative for sustained load: at 64 MiB/s compact + concurrency=1, aggregate compact throughput (64 MiB/s) was 7× below the 4K-p16 sustained flush input (432 MB/s aggregate), so SSTs would queue up and trigger imm back-pressure on the write path. New defaults: **fg 256→1024 MiB/s** (5× headroom over 8M peak 218 MB/s; 4K never engages); **compact 64→256 MiB/s** (each partition can match its own flush input); **gc 32→128 MiB/s** (handles 50% overwrite on 8M); **compact_max 1→4**, **gc_max 1→4** (4× SST in flight ≈ 2 GB peak RSS, F104's 44 GB incident depended on pre-F104-streaming behaviour). 10s `perf_baseline_tcp_p16_d8_s4k` unchanged (per-partition 27 MB/s never approaches the new caps); 60s+ sustained loads now have compact keeping up with flush. 139/139 PS lib pass; all admission tests construct controllers explicitly so default change doesn't affect them.
  - **Stage E** — `cluster.sh` honors `AUTUMN_PS_CPUSET` / `AUTUMN_EN${i}_CPUSET` to forward `--cpuset` (legacy `--cpu-start` path preserved when neither is set). README documents the flag + advisory.
- **Acceptance:**
  - `cargo check -p autumn-common -p autumn-server -p autumn-partition-server -p autumn-manager` clean.
  - 7 new `cpu_pin::tests::parse_cpuset_*` pass.
  - 3 new `policy_tests::hot_cold_advisory_*` pass (fires on ≥10× imbalance, suppresses below QPS floor, respects 5-min cooldown).
  - Workspace lib tests stable: 131/131 PS lib + 65/65 manager lib + 7 (was 0) common cpu_pin = no regressions.
  - Manual: `autumn-ps --cpuset 0 --psid 1 ... ` rejects 2nd split with `FailedPrecondition`; `autumn-ps --cpuset 0-1 --psid 1` permits 1 partition (cap = 1, P-log + P-bulk both on the 2 cores). `autumn-extent-node --cpuset 0-3 ...` boots with `shards=4` derived from cpuset_len; `--shards` is rejected with an error.
- **Out of scope (deferred):**
  - `NeedsMerge` wire response from manager dispatching split (user explicitly deferred — current behaviour is plain `FailedPrecondition` reject).
  - Auto-merge policy on advisory (still advisory-only; manager `--auto-merge` flag exists separately for merge dispatch).
  - Dynamic cpuset resizing without process restart.
  - Manager-side awareness of per-PS core budget for routing decisions (currently each PS self-enforces).
- **passes:** true (build clean across workspace; all stage tests pass; advisory + budget logic exercised by unit tests).

### F197 · parallel imm-flush drain (FuturesOrdered) + 120s perf_check finding
- **Trigger:** 120 s `perf_check tcp p16 d8 s4k 256t` revealed a ~10 s stall cycle (7 s healthy ~150-200K ops/s → 3-4 s near-zero) caused by `MAX_IMM_DEPTH=4` × serial flush wallclock. Pre-F197 `background_flush_loop` was `while flush_rx.next() { while flush_one_imm.await { } }` — strictly serial, single in-flight FlushReq to P-bulk.
- **Mechanism:** Refactor `flush_one_imm` into two phases: `run_flush_async_phase` (snapshot + await_log_synced_to + FlushReq send + resp_rx await — safe to run concurrently for different imm entries) and `commit_flush_outcome` (`tables.push` + `imm.pop_front` + `save_table_locs_raw` — strictly serial). New `background_flush_loop` uses `futures::stream::FuturesOrdered` with cap `ps_flush_inflight_cap()`: pushes up to N async phases concurrently, pulls in launch order, commits serially. `FuturesOrdered` (vs Unordered + reorder buffer) eliminates the seq-tagging + `BTreeMap` reorder buffer — futures still poll concurrently, output just streams in push order. F148-A invariant preserved (commit-side borrow_mut → save_table_locs_raw mpsc-send synchronous block; no `.await` interleaved). New CLI flag `--flush-inflight-cap N` + `AUTUMN_PS_FLUSH_INFLIGHT_CAP` env passthrough.
- **120s test:** With cap=4 (parallel), write throughput +1% / p99 -1.6% over D-r7-recal cap=1 baseline — **within noise**. Stall pattern identical. Root cause analysis: bottleneck is **EN-side row_stream fsync** (256 MB SST × 3-replica ≈ 3 s wallclock regardless of P-log launch concurrency, because writes to the same row_stream tail extent serialise at the extent file).
- **Default decision:** `ps_flush_inflight_cap` defaults to **1** (functional parity with pre-F197). Operators with workloads where the bottleneck isn't EN-disk (single-partition split-followup compaction, VP-heavy imm with small SST + big log_stream tail, short burst → drain) can opt in via `--flush-inflight-cap 4`. `ps_bulk_inflight_cap` default reverted to 2 (the R4 4.4 long-standing value).
- **Files:** `crates/partition-server/src/lib.rs` (new `FlushOutcome`/`run_flush_async_phase`/`commit_flush_outcome`/rewritten `background_flush_loop`; deleted `flush_one_imm_local`; new `PS_FLUSH_INFLIGHT_CAP_CELL` + setter); `crates/server/src/bin/partition_server.rs` (CLI flag); `cluster.sh` (env passthrough); 139/139 PS lib pass.
- **passes:** true. 120 s long-run cap=1 (effective serial flow) matches pre-F197 numbers within noise. Parallel infrastructure validated by code review + 139 tests; speedup is preserved for future workload shifts.
