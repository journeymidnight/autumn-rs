# autumn go→rust feature list

**Last updated:** 2026-05-04

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

---

## P0 — Core Architecture (correctness & data safety)

### F011 · Go range_partition advanced storage behaviors (umbrella)
- **Target:** Compaction/GC/value-log/maintenance lifecycle equivalent to Go range_partition.
- **Evidence:** `range_partition/*.go` · `crates/partition-server/src/lib.rs`
- **Notes:** Umbrella for F028-F033+F036+F037. Tracks overall completion of the partition layer rewrite.
- **passes:** false

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

### F129 · PutStream / GetStream — PS multipart upload + multi-fragment ValuePointer
- **Target:** Bound PS RAM and improve TTFB for large values by adding S3-style multipart upload (`PutBegin` / `PutChunk` / `PutCommit` / `PutAbort`) at the partition server, plus a client-side `GetStream` that loops the existing `GetReq.offset/length`. Memtable / SSTable gain a multi-fragment `ValuePointer` (op flag `OP_VALUE_POINTER_MULTI = 0x40`, encoded `[n_frags:u32][total_len:u64][(extent_id:u64, offset:u32, len:u32) × n_frags]`); chunks stored as WAL-shaped records with op `OP_CHUNK_BLOB = 0x10` so `decode_records_with_offsets` / `process_gc_chunk` skip them safely. Existing `Put`/`Get` preserved; both gain symmetric size cap `AUTUMN_PS_MAX_INLINE_BYTES` (default 64 MiB, hard ≤ 256 MiB) — over-cap returns `CODE_VALUE_TOO_LARGE`. Client adds `PutStreamHandle { send, commit, abort }` + `put_stream(Stream<Bytes>)` / `get_stream() -> Stream<Bytes>` / `put_auto` / `get_auto`. PS holds upload sessions in memory only (`HashMap<[u8;16], UploadSession>`); session metadata is O(1) in chunks (clients hold the fragment list); idle TTL 30 min (`AUTUMN_PS_UPLOAD_TTL_SECS`); per-partition cap 1024 (`AUTUMN_PS_MAX_UPLOAD_SESSIONS`). Routing requires `part_id` in `PutChunkReq`/`PutCommitReq`/`PutAbortReq`.
- **Acceptance:** see plan `/Users/zhangdongmao/.claude/plans/resilient-greeting-ember.md`.
- **Notes:** Picked S3 multipart over autumn-rpc native multi-frame (F133) because it has zero framework impact, matches Azure Blob / GCS / HDFS / GridFS / Ceph practice. Multi-fragment VP (ζ) over per-upload `blob_stream` (ε): all the surveyed systems store large values as fragment lists; single-segment continuity gives a measurable advantage only on "client reads whole 1 GiB at once", which is rare in this codebase. Symmetric Put/Get cap is critical: asymmetric creates "writable-but-not-readable" footgun.
- **passes:** false

### F130 · GC active rewrite for multi-fragment VPs (unblocks log_stream extent reclaim under F129)
- **Target:** Atomic VP rewrite so log_stream extents holding live multi-frag fragments can be reclaimed. Background task scans sealed log_stream extents; for each live multi-frag VP, append a fresh contiguous copy to active log_stream extent, atomically swap the memtable / SSTable VP, bump discard counters on source extents.
- **Trigger:** v1 monitoring shows log_stream sealed extent count > 16 OR partition disk usage > 80%.
- **Notes:** Hard problem — linearisation between rewrite, foreground writes, compaction, and GC needs a clean version-stamp story. Likely candidate: piggyback on memtable seq-number to make VP rewrites look like a normal Put with a higher seq, leveraging existing MVCC machinery.
- **passes:** false

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
- **Target:** `MgrExtentInfo.eversion` is mutated by four sites on the single-threaded manager runtime. The risky one is `ec_conversion_dispatch_loop`: it captures `new_eversion = ex.eversion + 1` at `crates/manager/src/recovery.rs:661` BEFORE the `EXT_MSG_CONVERT_TO_EC` await, then `apply_ec_conversion_done` writes that captured value back at line 736. If `apply_recovery_done` (`recovery.rs:205`), `compute_duplicate_stream` for split (`lib.rs:881`), or `mark_extent_available` (`lib.rs:1144`) bumps `eversion` during that await, EC conversion silently overwrites that bump. Symptom: stale `eversion` propagating to extent-node `.meta` files, surfacing as F119-D-style stale-cache `CODE_EVERSION_MISMATCH`. Fix is non-trivial: a clean fix likely requires either (a) re-sending `CommitEcShard` with a recomputed eversion if it differs at apply time, or (b) holding an "eversion-bump lock" between dispatch and apply. F136's mutual-exclusion partly mitigates: it serializes recovery vs EC conversion, but does NOT block split or `mark_extent_available` from racing EC conversion's await.
- **Trigger:** open and prioritise if production logs show repeated `CODE_EVERSION_MISMATCH` on freshly EC-converted extents that don't auto-recover, OR if a partition split during an in-flight EC conversion is observed to leave the extent's `eversion` stale.
- **Evidence:** Cross-crate background-task race audit alongside F136 (2026-05-04).
- **passes:** false

### F139 · Extent-node delete vs in-flight recovery on the same extent
- **Target:** On the extent-node, `handle_delete_extent` (`extent_node.rs:2768`) removes the entry from `self.extents` and unlinks `.dat`/`.meta`, while a concurrent `handle_require_recovery` (`extent_node.rs:2680`) holds an `Rc<ExtentEntry>` looked up before delete. Two failure modes: (a) recovery's `ensure_extent` runs after delete, finds the entry gone, re-creates an empty one and writes recovery payload — orphan only reaped on next reconcile-on-startup; (b) recovery still holds the pre-delete `Rc` and writes to the now-unlinked inode, leaving "empty file at path, real data on a now-orphaned inode that frees on fd close." Manager-side mitigation already exists (`extent_delete_loop` only fires on `refs == 0 && vp_table_refs == 0`, F136's mutual-exclusion serializes recovery vs EC conversion), but `punch_holes` can land between recovery dispatch and recovery completion. Defense: refuse recovery in `handle_require_recovery` when `self.extents` doesn't contain the extent; on the manager, `dispatch_recovery_task` could additionally skip extents queued in `pending_extent_deletes`.
- **Trigger:** open and prioritise if reproduced in a stress test that interleaves `stream_punch_holes`/`truncate` against recovery on the same extent, OR if production observes an orphan `.dat` file that survived `extent_delete_loop` and was only cleaned by reconcile-on-startup.
- **Evidence:** Cross-crate background-task race audit alongside F136 (2026-05-04).
- **passes:** false

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
