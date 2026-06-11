# autumn go→rust feature list

**Last updated:** 2026-06-11

**Rules:**
- `passes` and `notes` are the only mutable fields after a feature is created.
- Out-of-scope / "v2 再做" decisions must be recorded as proper feature entries (F-number + Trigger + `passes:false`), not as plan-file footnotes.
- Entries below the Completed table document non-obvious decisions, root causes, and active designs. Trivial work-tracking entries live in the table — the implementation IS the documentation in those cases.

---

---

## 已归档（2026-06-11 清理）

已完成 feature 的详细记录（Trigger / 设计 / 验收 / 实测）**原文搬迁**至
`docs/feature_list_archive.md`（不可改写规则同样适用）。本文件保留：规则、
Completed 索引表、全部未完成（passes/Status = not_completed）条目。
新 feature 仍在本文件新增；完成后的归档随下次清理批量进行。

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
| F254 | row_stream single-writer = type-level: P-bulk spawn failure is fatal-for-this-partition; in-thread flush/compact fallback removed (was the 2026-05-03 invalid_meta_len corruption foothold) | partition |
| F255 | P-bulk hardening — close two F254 audit findings: (P0) split → P-bulk row_stream invalidate race fixed via SYNCHRONOUS pre-seal barrier (RowInvalidateBarrierReq) + per-partition compact_gate; (P2) P-bulk readiness handshake (spawn_bulk_thread returns ready oneshot; open_partition awaits) | partition |
| F-ioring-lease-1 | JuiceFS-style inode-level lease + close-to-open coherence — manager state machine (`crates/manager/src/inode_lease.rs`), 4 RPCs (MSG_ACQUIRE_LEASE / RELEASE_LEASE / HEARTBEAT_LEASE / POLL_INVALIDATIONS, 0x46–0x49), writer-lease etcd persistence under `inode_leases/<ino>` (F149-fenced), TTL revoke loop (`inode_lease_revoke_loop`), invalidation push queue per client. **Phase 1 ground floor** for ioring-daemon + autumn-fuse cross-mount coherence (plan: `docs/autumn_fs_lease_plan.md`). Daemons not yet wired — F-ioring-lease-2 next. | manager |
| F-ioring-lease-2 | autumn-ioring-daemon Open/Close acquire+release lease — RING_VERSION 1→2 (Open's SQE flags byte now carries `LEASE_MODE_READ/WRITE`, 0 = safe default WRITE); per-session `DaemonClientId` (UUID); per-inode refcounted `held_leases` so a 2nd Open within a session shares the lease, only the LAST Close releases; per-session 5 s `session_heartbeat_loop` renews held leases (NotHeld → invalidate session ring_fds with EBADF). New `autumn-ioring::lease` module wraps the 4 RPCs into typed `acquire/release/heartbeat/poll_invalidations`. **Phase 1, step 2 of 4** — F-ioring-lease-3/4 still pending. | ioring/manager |
| F-ioring-lease-3 | Long-poll invalidation channel — manager-side `ClientInbox.waker` parks the `MSG_POLL_INVALIDATIONS` handler when the queue is empty (`drain_or_park`); push fires the waker so the next event resolves the poll within ~ms (vs the 10 s timeout). Daemon-side per-session `session_invalidation_poll_loop` drains events in a tight loop; on transport error OR overflow sentinel it wholesale-invalidates the session's `ring_fds` + `held_leases` + best-effort releases (plan §6.4). **Phase 1, step 3 of 4** — F-ioring-lease-4 still pending. | manager/ioring |
| F-ioring-lease-4 | `OpenedExtents` version-tagged + cache invalidation e2e — `OpenedExtents.lease_version: u64` populated at Open from the AcquireLease response; per-session `InvalidationMap: HashMap<u64, u64>` (ino → min-valid-version) bumped by `session_invalidation_poll_loop` via the pure-fn `apply_invalidation`. Read SQE arm uses `cache_is_stale` to compare; on stale → `fuse_read::reload_extents` re-fetches the inode meta + extent map, updates `lease_version` to the new floor. Multi-daemon close-to-open coherence end-to-end (writer-close pushes WriterClosed → reader's next Read on the same ring_fd reloads → sees new bytes). **Phase 1 COMPLETE.** | ioring/manager |
| ~~F129~~ | ~~PutStream / GetStream — multipart + multi-frag VP~~ — SUPERSEDED by F186 (server code ripped out) | — |
| ~~F130~~ | ~~GC active rewrite for multi-frag VPs~~ — SUPERSEDED by F186 (no multi-frag any more) | — |

---

## P12 — autumn-kvcache vLLM connector (F250)

### F250 · autumn-kvcache vLLM `KVConnectorV1` adapter (Phase 3a — CPU-offload path)
- **Trigger:** F216 shipped the sglang HiCache L3 backend; the same partition data
  plane (`autumn.BatchClient` + `_bridge`) can serve vLLM, but vLLM's offload
  contract is `KVConnectorBase_V1` (scheduler/worker split, per-layer load/save),
  not a synchronous storage backend. Design route A selected + detailed in
  `docs/autumn_kvcache_plan.md §13` (2026-06-01). User: "开始实现".
- **Goal:** A native `AutumnKVConnector(KVConnectorBase_V1)` in
  `python/autumn_kvcache/autumn_kvcache/vllm_connector.py`, daemon-less, reusing
  the F216 data plane. Phase 3a target = CPU-offload KV path (no GPU staging) +
  connector lifecycle correct + cross-instance prefix-cache hit. Pattern mirrors
  vLLM's `SharedStorageConnector` (per-request-prefix, per-layer entries), swapping
  safetensors files for autumn keys.
  Sub-features:

  **F250-A · Shared util extraction**
  - Factor `_build_tenant_suffix` + key-namespace (`kvc/{tenant}/{pool}/...`) out of
    `sglang_backend.py` into `_keys.py`; both adapters import it. No behavior change
    to the sglang path (its existing smoke test must still pass).

  **F250-B · `AutumnKVConnector(KVConnectorBase_V1)` + `AutumnConnectorMetadata`**
  - Defensive vLLM import (module importable without vLLM, like `sglang_backend`).
  - Scheduler role: `get_num_new_matched_tokens` (block-aligned prefix hash →
    `batch_head` existence → matched token count), `update_state_after_alloc`,
    `build_connector_meta` (per-req load/save `ReqMeta{token_ids, slot_mapping,
    is_store}`), `request_finished`.
  - Worker role: `register_kv_caches`, `start_load_kv` (per load-req, per layer:
    `get_into` autumn → inject into layer via slot_mapping), `wait_for_layer_load`,
    `save_kv_layer` (extract per slot_mapping → `put_from`), `wait_for_save`,
    `get_finished`.
  - Key: `kvc/{tenant}/vllm/{prefix_hash}/{layer_name}`; pool segment `vllm` (separate
    keyspace from sglang's `kv`). tenant suffix from vllm_config TP/PP + model.
  - Reuse `autumn.BatchClient`/`Client.batch_head`/`_bridge`. No daemon, no local LRU,
    persistence only via partition ([[feedback_no_parallel_data_plane]]).

  **F250-C · Data-plane smoke test (NO vLLM dependency)**
  - `tests/test_vllm_dataplane.py`: against a real 1-node cluster, exercise the
    autumn-facing core the connector relies on — key format, byte-buffer store/load
    round-trip via the connector's `_AutumnKVStore` helper (`put_from`/`get_into`),
    and `batch_head` prefix existence. Validates the half that doesn't need a model.

  **F250-D · vLLM e2e (isolated venv) + README**
  - Isolated venv (do NOT disturb system torch 2.9.1 / sglang 0.5.10): install a
    vLLM version, run two `vllm serve` instances with
    `--kv-transfer-config '{"kv_connector":"AutumnKVConnector",...}'`, verify the 2nd
    instance gets a cross-instance prefix-cache hit (cached tokens > 0) served from
    autumn. README "Using autumn-kvcache as vLLM L3" section.

- **Architectural invariants:** same as F216 — stateless adapter, content-addressed
  keys (no invalidation), partition is the only persistence path, return fast / never
  block past the engine's budget.
- **Acceptance:**
  - `pip install -e python/autumn_kvcache` still imports; `python -c "from
    autumn_kvcache.vllm_connector import AutumnKVConnector"` works WITHOUT vLLM installed.
  - F250-A: existing sglang smoke test (`test_smoke.py`) still passes (no regression).
  - F250-C data-plane smoke test passes against a real 1-node autumn cluster.
  - F250-D: vLLM e2e cross-instance prefix hit demonstrated in the isolated venv.
- **Out of scope (Phase 3b+):** GPU-resident KV staging buffers + `cudaMemcpyAsync`
  overlap (Phase 3b); per-(block,layer) key merge / RPC coalescing (Phase 3c);
  hybrid-attention multi-pool. Tracked in `docs/autumn_kvcache_plan.md §13.9`.
- **passes:** not_completed (in progress 2026-06-01)

---

## P15 — Lease subsystem audit (coco arch review 2026-06-05)

coco GPT-5.5 arch review surfaced 8 findings (3 P0, 2 P1, 3 P2)
across the full lease subsystem (manager + client + fuse + ioring).
Each is tracked as a BUG-LEASE-N entry, reproduce-first, with the
fix gated on a failing test. Cluster chaos run with seed=42 / 60s
showed 0 data loss + 0 wedged partitions on the pre-fix tree, so
none of these is a flaming-house emergency, but they're all real
gaps in the lease protocol's correctness story.

### BUG-LEASE-2 (P0 #2) — storage-layer fencing token

- **Source:** coco arch review 2026-06-05, P0 #2 +
  recommendation #1.
- **Symptom:** PS write requests didn't carry the lease epoch;
  partition-server had no way to reject a write from a writer
  the manager had already revoked. So during the
  `LeaseRevoked` push window — or after any TTL-revoke /
  force-revoke — the previous writer's in-flight RPCs could
  still land at the PS, mingling with the new writer's first
  writes.

#### Phase 1 (completed 2026-06-06): wire + in-memory fence floor

- **Wire change** (`crates/rpc/src/partition_rpc.rs`):
  `PutReq` gains `inode_hint: u64` and `lease_epoch: u64`.
  New response code `CODE_FENCED = 9`. Defaults are `0/0` so
  every existing caller compiles after a mechanical field
  addition; `inode_hint == 0` is the explicit
  "anonymous write — skip fencing" opt-out for KV CLI and
  non-lease-aware paths.
- **PS state** (`crates/partition-server/src/lib.rs`):
  `PartitionData.fence_floors: RefCell<HashMap<u64, u64>>`
  per partition. Floor = the highest `lease_epoch` accepted
  for that ino on this PS instance.
- **Pure-fn** `check_and_bump_fence(inode_hint, stamped_epoch,
  &mut floors) -> Result<(), String>` — unit-testable; PS
  invokes via `enqueue_put` before the value-too-large check.
- **Semantics:** `inode_hint == 0` ⇒ pass-through (no
  mutation). Else: `floor = floors[ino].unwrap_or(0)`;
  `stamped < floor` ⇒ `CODE_FENCED`; else accept + bump floor
  to `max(floor, stamped)`.
- **Phase 1 known gap (deliberate):** IN-MEMORY ONLY. A PS
  restart wipes the floors → during the post-restart warm-up
  window, an old stale-epoch RPC from a previously-revoked
  writer would slip through. Phase 2 will persist via WAL.
  Also: `MSG_PUT_ZC` ignores fencing (the ZC framing
  `parse_put_zc_meta` doesn't yet carry the new fields);
  Phase 2 extends the meta header.
- **Reproduction:**
  - Unit (default CI):
    `crates/partition-server/src/lib.rs::bug_lease_2_fence_tests`
    7 tests covering the floor semantics (anonymous bypass,
    seed/bump/reject, multi-ino independence, monotonic-
    under-reorder).
  - Wire e2e (`#[ignore]`):
    `crates/manager/tests/bug_lease_2_storage_fencing.rs`
    boots manager + 2 EN + 1 PS, sends seq of Puts with
    epoch 1 → 5 → 1 (stale) → asserts the stale write is
    rejected with `CODE_FENCED`. Also covers `inode_hint=0`
    bypass and per-ino independence.
- **passes:** completed (2026-06-06, Phase 1 only)

#### Phase 2 (completed 2026-06-11) — WAL/checkpoint persistence + ZC framing + fuse/ioring stamping

- **WAL persistence**: new WAL op `OP_FENCE_BUMP` (0x08; key = ino
  BE8, value = epoch LE8). `check_and_bump_fence` 返回 bumped 标志；
  floor 真正抬升时 enqueue_put / enqueue_put_zc / enqueue_batch_put
  把一条 `WriteOp::FenceBump`（`WriteResponder::Fence` 无客户端应答）
  排在被接受写**之前**进同一 group-commit —— floor 持久化先于/同于该
  写的 ACK。Phase 3 与重放都不入 memtable；重放 max-merge（幂等，
  绕过 ts-dedup）；GC 的 VP 扫描天然跳过（无 VP 位）。
- **Checkpoint 快照**: `TableLocations.fence_floors: Vec<(u64,u64)>`，
  三个发布点（flush / compact×2）在同一 borrow 下
  `snapshot_fence_floors`（F148-A 保持）。恢复时从所有 meta 记录
  max-merge 种子（merge 后两来源 floors 并集）+ 重放补增量 ——
  重放窗口之前的旧 floor 也存活。
- **ZC framing**: `PUT_ZC_HEADER_LEN` 28 → 44（inode_hint+lease_epoch
  追加在 key_len 之后、key 前移位）；`enqueue_put_zc` 与 enqueue_put
  同语义检查（value-too-large 先于 fence，coco R2-P1 #5 顺序保持）；
  `drain_zc_writes` 的 part_id/key_len 偏移不变仅 const 更新。
  `BatchPutOp` 增 per-op 字段；被 fence 的 op 在 statuses 记
  CODE_FENCED 不拖累整批。
- **客户端**: `WriteLease{inode_hint, lease_epoch}` + `AutumnError::
  Fenced` + `put_fenced / put_zc_fenced / put_many_fenced`；匿名路径
  （put/put_many/kvcache/perf）全部 `WriteLease::ANON` 零行为变化。
- **fuse**: `FsState::write_lease_for(ino)`（held_leases 的 WRITE
  lease version；revoked 条目仍 stamp 旧 version → PS 拒绝 = 协议
  的存储侧半边）；write_region 数据 extent（append 批 + RMW）、
  truncate 跨界 put、put_inode 全部 fence-stamp。
- **ioring**: `write_lease_of(OpenedExtents)`（lease_version==0 =
  pre-lease 兼容 → ANON）；execute_step 数据 put/put_zc、
  maybe_persist_size_growth、daemon 的 dirty-meta 延迟 flush
  （map 值改 `(InodeMeta, lease_version)`）+ close 同步 flush 全部
  fence-stamp。
- **验证**: e2e `bug_lease_2_phase2_persistence.rs` ×2 PASS —
  ① kill -9 重启（无 flush）→ stale epoch 仍 CODE_FENCED（WAL 重放
  路径）+ ZC stale 重启前后均被 fence + live epoch 正常；
  ② Maintenance FLUSH 后重启（vp 越过 bump 记录）→ floor 来自
  checkpoint 快照。Phase 1 e2e ×2 回归 PASS。单测 162+72+27+68+19
  全绿（rpc 新增 ZC meta roundtrip ×3）。4K 写 A/B 对照：A 52.5-56.4K
  vs B(改前) 49.5-62.8K 完全重叠 = 环境噪声，无回归（匿名写仅多一次
  RefCell borrow + 早退分支）。
- **残留（记录不阻塞）**: StreamPut 未 fence（非 fuse/ioring 路径）；
  归 BUG-LEASE-8 原子性家族一并考虑。
- **coco review（GPT-5.5; 1 P1 修 + 1 P1 论证接受 + 1 P1 升级修掉 +
  1 P1 按仓库惯例 + 1 P2 修 + 1 P3 修）**：
  ① P1 #1 修——fence 检查曾在 in_range 之前：误路由写会抬升（Phase 2
  后还持久化）floor。修为三个 enqueue（put/zc/batch per-op）+ delete
  都先 range admission 再 fence（value-too-large 同序），
  start_write_batch 的 range 检查保留为纵深。
  ② P1 #2 论证后接受（snapshot_fence_floors 文档化）——checkpoint 可能
  含 WAL 尚未落盘的 bump，但 floor=E 只编码"见过 epoch E 的请求"，
  manager 单调发号下这一事实已证明 <E 的 writer 全部被撤销，fence 它们
  永远正确；危险方向（floor 丢失）才是 WAL-before-ACK 防的。双态
  pending/durable 机制对正确客户端零收益，不建。
  ③ P1 #3 修（从"残留"升级）——DeleteReq 增 inode_hint/lease_epoch，
  enqueue_delete 同 fencing（range→fence 序）；客户端 delete_fenced；
  fuse truncate 整 extent 删除 + unlink delete_all_extents 全部
  fence-stamp。撤销 writer 的迟到 truncate/unlink 不再能删新 writer
  的数据。
  ④ P1 #4 按仓库惯例——TableLocations 为持久化 rkyv 结构，旧 meta
  checkpoint 不可解码：与 F207-E 同例，旧数据集群需 `cluster.sh
  reset`（本仓库无滚动升级/数据保留升级承诺）。
  ⑤ P2 #5 修——daemon dirty-meta flush 对 `Fenced` 终止性错误不再
  requeue（旧 lease 永远不可能成功），丢弃 + WARN；close 同步 flush
  同理且不再阻塞 close。
  ⑥ P3 #6 修——e2e 重启等待循环加 60s 超时。
- **MSG_STREAM_PUT 删除（用户指示，随本 commit）**：`ClusterClient::
  stream_put` 全仓库零调用方（F186 客户端分条后即死代码；CLI 的
  put-stream 走 put_stream_begin 与此无关），而 PS 侧 `enqueue_stream_put`
  ≡ enqueue_put 减去 inline 上限检查与 fence 检查 = 绕过防护的无界写
  后门。已删除：客户端方法、StreamPutReq、enqueue_stream_put、dispatch/
  frozen/mis-route 三处 arm；0x46 常量按 F129 惯例保留 RESERVED 注释。
- **命名收敛（用户指示）**：fencing epoch 在客户端侧统一为
  `lease_epoch`——`FuseLease.version` → `lease_epoch`、
  `OpenedExtents.lease_version` → `lease_epoch`（与
  `WriteLease.lease_epoch` 一致）；manager 线协议字段
  `MgrInodeLeaseInfo.version` 保留原名（发号方自身的状态机字段），
  等价关系记录在 FuseLease 字段文档。
- **passes:** completed (2026-06-11)

### BUG-LEASE-8 (P2 #9) — multi-key write has no atomic commit boundary

- **Source:** coco arch review 2026-06-05, P2 #9.
- **Symptom:** fuse `write::write` writes multiple extent KVs
  then updates inode meta size/mtime. Crash between extent
  writes and the inode-meta update leaves: extents written but
  size unchanged; or partial extents present; or size advanced
  but missing extents (read as sparse zeros). Lease only
  handles inter-client coherence; this is INTRA-client crash
  consistency.
- **Status:** ARCHITECTURAL, deferred. Pre-existing fuse-layer
  limitation (predates the lease work). Requires per-inode
  generation manifest / commit record; recovery needs to GC
  orphan extents under uncommitted generations.
- **passes:** not_completed (deferred — design recorded)

---

### F263 · fencing 三层命名收敛（`*_epoch` 词族）
- **Trigger:** 用户指示（2026-06-11）。三层 fencing token 命名各异
  （owner_revision / region_epoch / lease_epoch + 客户端侧
  FuseLease.version / OpenedExtents.lease_version），同概念五个名字，
  且 stream 层 "revision" 与 etcd 自身的 mod_revision/create_revision
  撞词。
- **改动（纯改名，rkyv 字段改名不改 wire 字节序——positional 编码）：**
  - `owner_revision`/裸 `revision`（owner-lock 语义）→ `owner_epoch`，
    覆盖 stream/manager/rpc/partition-server/common + 测试 + bench：
    `ExtentEntry.owner_epoch`、`durable_owner_epoch`、
    `StreamClient::new_with_owner_epoch[_and_config]`、
    `acquire_owner_epoch`/`ensure_owner_epoch`、extent 头
    `[owner_epoch: i64 LE]`、manager_rpc 10 个 req/resp 字段。
    etcd 术语（mod_revision/create_revision/EtcdMirror 内部）保留。
  - `lease_version`（ioring/fuse 客户端缓存）→ `lease_epoch`（上一
    commit 已做主体，本次补测试 fixtures）。
  - root CLAUDE.md "Owner Lock Fencing" 重写为三层 fencing 表
    （owner_epoch / region_epoch / lease_epoch：grantor→checker、
    fence 对象、拒绝码）；各 crate CLAUDE.md 同步。
    feature_list 归档文件按不可改写规则保留历史命名。
- **验收:** 全 8 crate 549 单测绿；BUG-LEASE-2 Phase 2 e2e（全集群
  启动含 owner-lock 流程）重跑 2/2 PASS。
- **passes:** completed (2026-06-11)

---

### F264 · chaos 补充：PS-failover + transport 层（用户 /loop 指令 2026-06-11）
- **目标:** ① 2 PS 集群 kill 一个 → partition 必须迁移到幸存者且零数据
  丢失（system_chaos 只杀 EN、单 PS 永不死——PS 驱逐→rebalance→
  survivor open_partition 全恢复路径无 chaos 覆盖）；② transport 层
  （tcp/ucx）chaos；发现 bug 修复 bug。
- **已实现 (迭代 1):** `system_ps_failover_chaos.rs` ×2（子进程 PS 可
  kill -9）：`ps_kill_migrates_partitions`（预置 100 键含 VP 大值 →
  kill 持有者 → 45s 内重指派 → 全键字节校验 + 双分区写活性 → 原 psid
  重启后一致性复查）；`ps_kill_during_write_storm`（40s 连续写中途
  kill；ACK 集合必须全存活，超时写按 uncertain 规则剔除）。
- **迭代 1 结果:** 2/2 PASS 首跑——110,799 ACK 写 0 丢失，kill 后两
  partition 均迁移（211→212），SDK 重试完整扛过驱逐窗口（10s 驱逐 +
  region_sync + survivor 恢复）。未发现 bug。
- **passes:** not_completed (迭代进行中——transport ucx/tcp chaos 待做)
