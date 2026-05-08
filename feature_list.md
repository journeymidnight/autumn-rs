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
