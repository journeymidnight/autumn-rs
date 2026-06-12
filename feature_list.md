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

## ✅ 2026-06-11 chaos /loop 战役 — Completed（详情在 docs/feature_list_archive.md）

| 条目 | 状态 |
|---|---|
| BUG-LEASE-2 (P0 #2) — storage-layer fencing token | completed (2026-06-06, Phase 1 only) |
| F263 · fencing 三层命名收敛（`*_epoch` 词族） | completed (2026-06-11) |
| F264 · chaos 补充：PS-failover + transport 层（用户 /loop 指令 2026-06-11） | completed (2026-06-11) |
| F265 · chaos 迭代 3：manager 控制面 chaos（E4/E5）——发现并修复 3 个 bug（/loop 2026-06-11） | completed (2026-06-11) |
| F266 · chaos 迭代 4：E6 随机化重复 kill 轮（/loop 2026-06-11） | completed (2026-06-11) |
| F267 · chaos 迭代 5：multi-manager HA chaos——发现并修复 4 个 bug（/loop 2026-06-11） | completed (2026-06-11) |
| F268 · chaos 迭代 6：split/merge 与 kill 竞争（E7，/loop 2026-06-11） | completed (2026-06-11) |
| F269 · chaos 迭代 7：20 轮混合 soak（kill × split/merge 交错，/loop 2026-06-11） | completed (2026-06-11) |
| F270 · F227 open-tail write-wedge family 三层根因全闭合（fence 自愈 + harness 端口 + part_addr 实际地址；seed=13 15/15） | completed (2026-06-11) |
| F271 · chaos 迭代 11：20-seed 严酷扫描认证（/loop 2026-06-11） | completed (2026-06-11) |

---

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
---

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

---

### F272 · chaos 迭代 12：跨主机 chaos（真实网络，::14 + ::15）
- **目标:** 真 RoCE 网络上的 kill/迁移/failback/manager 重启 chaos。新
  harness `scripts/crosshost_chaos.sh tcp|ucx`：本地 etcd+manager+
  EN×2+PS1，远端（ssh）EN×1+PS2，replication=3 跨网；X1 杀远端 EN、
  X2 跨主机迁移、X3 跨主机 failback、X4 manager 重启。
- **tcp 轮: 全 PASS** —— X2 跨主机迁移 12s、X3 failback 19s、X4 恢复
  3s，734 ACK 写 0 丢失。两个 harness 坑已修：ssh 启动远端 daemon 会
  挂住通道（本地后台化）；首跑 25 min 卡死即此。
- **ucx 首轮异常已全部定性为 harness 时序（验尸闭环）:** manager 的
  UCX listener 在跨网 TIME_WAIT（前一 tcp 轮同端口）上按 F264 设计重
  试 ~90s，而脚本各阶段是固定 sleep——format/bootstrap/全部 seed 打到
  未监听的 manager 上连环 FAIL，EN/PS1 注册 fail-fast 退出，X2 假阳
  性、X4 后 manager 当选于"空 etcd"（revision=7 = 只有 cluster_id
  imprint）全部由此派生。**无产品 bug。**
- **修复（harness）:** 阶段就绪门取代固定 sleep——`wait_mgr`（180s，
  盖过 ucx bind 重试预算）+ `wait_nodes 3`（bootstrap 前节点注册门）。
- **最终认证（带门双轨）:** ucx 全 PASS（X1-X4，跨主机迁移 12s/
  failback 21s，320 ACK 0 丢失）；tcp 复跑全 PASS（768 ACK 0 丢失）。
  运维注记：EN/PS 启动期对未监听 manager fail-fast 属设计内
  （supervisor 重启吸收），编排側必须设 readiness 门。
- **passes:** completed (2026-06-12)

---

### F273 · chaos 迭代 14：数据面接口 chaos——autumn-fuse 在 failover 下（2026-06-12）
- **目标:** 三接口（fuse/kvcache/client）此前无 failover chaos 覆盖。
  新 harness `scripts/fuse_chaos.sh`：2-PS 集群 + fuse 挂载，文件负载
  流经 fuse→SDK→PS→EN 全链（含 inode-lease），sha256 清单仅记录
  成功 close 的文件；F1 杀持有 PS、F2 杀 manager+respawn、F3 杀
  fuse 守护+重挂；每阶段清单全验。
- **验收: 首跑全 PASS** —— F1 迁移后文件 I/O 12s 恢复、F2 11s、F3 重
  挂即恢复；83 个 synced 文件三轮故障后 sha256 全部完好，零损坏。
- **passes:** completed (2026-06-12)
