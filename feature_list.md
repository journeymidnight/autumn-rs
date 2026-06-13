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

---

### F274 · chaos 迭代 15：过夜 90 轮 soak 认证（2026-06-12）
- **目标:** 四层覆盖全绿后的最终耐久认证。seed=99 混合 soak（kill ×
  split/merge 交错）：tcp 60 轮 + ucx 30 轮连跑（~2.5h，深夜空闲机）。
- **验收: 双双全 PASS** —— tcp 8671 ACK + ucx 1363 ACK 全部完好，90
  轮随机故障零丢失零 wedge。
- **passes:** completed (2026-06-12)

---

### F275 · chaos 迭代 16：kvcache python 接口 chaos（2026-06-12）
- **目标:** 三接口最后一块——python L3 backend（sglang HiCache 路径，
  无需 sglang）在 failover 下。新 harness `scripts/kvcache_chaos.sh` +
  `python/autumn_kvcache/tests/chaos_workload.py`：batch_set_v1 + 随机
  历史页读回校验持续流经 python 桥；K1 杀持有 PS、K2 杀 manager；进度
  门判活 + 结束后新进程全清单复验。
- **过程发现（非产品 bug，运维坑）:** 已安装的 maturin wheel 是 5/20
  旧构建——其后 PutReq 加了 fence 字段（rkyv 线协议变更，仓库约定
  same-commit 部署），旧编码被新 PS 解出 `part_id=0` → batch_set 全
  False、官方 smoke test 同样失败。重建 wheel 即愈。**注记：rust 侧
  wire 变更后必须 `maturin build --release` + 重装**（README 已加）。
- **验收: 重建后全 PASS** —— K1 后 12s、K2 后 8s 恢复进度；158 轮全
  部 readback 校验 + 新进程复验 158/158，零 mismatch。
- **passes:** completed (2026-06-12)

---

### P0-D · 生产急修批次 1：EN durability——清零被吞的 `.meta` persist 失败（2026-06-12）
- **目标:** `/loop 生产视角` 首项（用户拍板 P0 durability batch 先行）。
  P0-A/B/C（fsync fail-closed / fence durable-before-ACK / sealed-empty
  V2）此前已闭；本批清零 extent_node.rs 最后 3 处 `let _ = save_meta`：
  ① `run_recovery_task` 末尾 persist——失败曾被吞，恢复任务谎报完成而
  sidecar 仍是恢复前 eversion/seal；② `handle_convert_to_ec` prepare 路
  seal persist——非持久 seal 门控 EC 编码，crash 中途重启回 OPEN 而
  shard 已分发；③ 同函数 post-convert eversion/seal persist——stale
  sidecar 盖 shard 数据（F119-C/D 防的同族损坏）。三处统一 fail-closed
  （mark_disk_offline + 报错，由调度/manager 重试）。
- **coco（GPT-5.5）2 P1 全采纳:** ① F119-D 幂等跳过路径内存原子满足≠
  sidecar 持久——skip 前 ensure save_meta（幂等）并 fail-closed；②
  recovery persist 失败后残留的 partial entry 会让本地重试复用已下线
  盘、并以 "extent already exists" 卡死 manager 重派——mark offline 后
  remove entry（孤儿 .dat 走 F109/F113 reconcile 回收）。
- **验收:** autumn-stream --lib 72 单测绿；seed=13/60s 隔离 system_chaos
  ×2（含 P1 折入后）全 "test result: ok" 零丢失；transport_chaos tcp
  全 PASS。stream CLAUDE.md note 25 固化不变量：**任何
  seal/convert/recovery RPC 返回 OK 即断言 sidecar 已持久，而非仅内存
  一致**。
- **passes:** completed (2026-06-12)

---

### OBS-1 · 生产急修批次 2：observability——Prometheus /metrics 薄层（2026-06-12）
- **目标:** `/loop 生产视角` 第二项。三个 server binary 此前零可观测端点
  （QPS/延迟/盘健康/分区分布只能翻日志）。新增 opt-in `--metrics-port`
  （+`--metrics-listen` 独立绑定）暴露 Prometheus 文本 /metrics：
  - 公共层 `autumn-common::metrics_http`：手写 HTTP/1.1，**独立 std 线程**
    （零 compio/io_uring 交互，慢 scraper 永不碰数据面）+ 文本格式 helpers
    + `MetricsSnapshot`（Mutex<Arc<String>> O(1) 指针交换信箱）。
  - manager：leader/serving、streams/extents/nodes/partitions/ps/regions/
    part_addrs 计数、per-disk online、F207 inflight 数（Rc 状态 → 运行时上
    2s publisher task 渲染快照）。
  - PS：per-partition requests_total（用 req_count_monotonic，swap-reset 的
    req_count 会锯齿）+ size/gc-debt/pending-compaction/inflight/sealed-log
    gauges；metric-major 分组（text format 要求同名样本连续）。
  - EN：append batches/bytes/ns 全局单调计数（record() 每 BATCH 3 个
    relaxed fetch_add）+ per-shard gauge 槽位（extents、disk online）。
- **过程发现（实测抓出）:** 首版 EN gauge 刷新挂在 handle_df ——但 manager
  的 df 只探注册的 control_address（shard 0），其余 shard 槽位永久陈旧
  （磁盘 6 extents、metrics 报 3）。改为每 shard 在自己 runtime 上跑 2s
  刷新循环，多 shard 复验 3+3=6。
- **coco（GPT-5.5 deep）3P2+3P3，采纳 5 拒 1:** ① RwLock 快照可能让
  publisher 阻塞 compio runtime → MetricsSnapshot O(1) Arc 交换+毒锁安全；
  ② EN 槽位/刷新任务无生命周期 → 注册表存 Weak、render 剪枝、刷新任务持
  Weak（节点 drop 即退出，不钉死 extent 状态）；③ 注册早于可失败 init →
  Weak 语义自动覆盖；④ 首抓空 200 → listener 前同步发布首个快照；⑤
  0.0.0.0 暴露 → `--metrics-listen` 独立绑定 + README 无鉴权注记。拒：
  ⑥ CLI 缺值 panic——与全部既有 flag 的手写解析习惯一致，不单独特判。
- **验收:** workspace 构建绿；4 crate 395 单测绿（含 metrics_http 2 个新
  单测）；live 验证 tcp 3EN×2shard 集群三端点全通（manager 9591/PS 9701/
  EN 9601，404 路径、计数随写入增长、multi-shard 聚合、manager-kill 后
  gauge 行为）；seed=13 隔离 chaos 回归 ok 零丢失。cluster.sh
  `AUTUMN_METRICS=1` 一键接线；README 新节 + 3 crate CLAUDE.md 注记。
- **passes:** completed (2026-06-12)

---

### ENOSPC-1 · 生产急修批次 3：写满盘行为定义 + 实测——并揪出 ACK 短写静默损坏（2026-06-12）
- **目标:** `/loop 生产视角` 第三项。写满盘行为此前未定义未测试：任何写
  错误（含暂时性 ENOSPC）都把盘永久标 offline 直到进程重启；EC 写路径
  根本不标；分配从不看剩余空间。
- **设计:** ① EN `DiskHealth` 三态机（Online/Full/Faulted，按
  state-machine-not-bool 约定）取代 `DiskFS.online` bool：ENOSPC/EDQUOT
  （`is_disk_full_error`，按 os-error 后缀匹配——多处只有字符串化错误）
  归类 **Full**——盘停收新 extent（choose_disk 要求 allocatable）但继续
  服务读+既有 extent，每 shard 2s sweep 在 free≥总量5% 时自愈回 Online；
  其余归 **Faulted**（保留历史永久语义，绝不被降级）。全部写/persist
  错误站点换 `mark_disk_error_for_extent`（append pwrite/fsync、fence
  persist、save_meta fail-closed、recovery、EC staging/commit——EC 族
  此前零标记）。② metrics 新增 `autumn_en_disk_full`。③ manager：
  node_health_loop 存每节点最大单盘 free（内存态），`select_nodes` 软避
  低于 `--min-alloc-free-bytes`（默认 256MiB，0=关）的节点，不足时回退
  全 healthy 集（容量紧张集群仍尝试分配，EN 侧 Full 快速失败+逐 RPC
  fallback 兜底）。
- **headline BUG（E2E 首跑抓获，真实数据损坏）:** `build_append_future`
  用裸 `write_vectored_at` 只查 Err——POSIX pwritev 在将满盘上写入能容
  纳的部分并返回**短计数**，partial append 被 fsync + **ACK**，预留区
  未写尾部读回全零（实测 1MB 值只有 3.5KB 完好+后续全零，重试 3 次仍
  错）。修复：`write_vectored_all_at`。不变量：**本地文件写必须用
  `*_all` 形式或校验写入计数——裸定位写的 Ok(n) 不是成功**
  （file_pwrite 早已用 write_all_at，批量 vectored 路径是唯一裸调用）。
- **E2E:** `scripts/enospc_chaos.sh`——EN1 数据目录放 512MB loopback
  ext4，压舱后 1MB 值持续写至 ENOSPC：E1 Full≠Faulted（metrics 断言）、
  E2 写可用性 20/20（新 extent 落其他 EN）、E3 释放空间后 ~2s 自愈、
  E4 全部 ACK key 字节级回读（修复后 145/162 key 两轮零损）。harness
  自身教训：键须带随机 hex 前缀散到 8 分区（首版 "ek-*" 全落一个分区，
  其 log tail 恰好不含 EN1）；AUTUMN_DATA_ROOT 必须传给 cluster.sh。
- **coco（GPT-5.5 deep）2P1+4P2+2P3，采纳 6 拒 2:**
  - **P1-甲（第二个真 bug，acked 数据被 seal 砍掉）:** apply_completion
    在 batch 全副本完成时无条件给调用方发 Ok——但同 extent 更低 offset
    的 lease 已失败（永久 hole，正是 ENOSPC 中段失败的形态）时，writer
    `commit` 停在 hole 之下（设计如此：roll 的 seal 按 contiguous
    commit 排除"hole 及其后全部"），于是 seal 砍掉已 ACK 的区间。修复：
    caller-ack 延迟到 contiguous prefix 覆盖该区间才发（pending_acks 携
    带 oneshot）；poison 时 `failure_floor` 以上的 pending/迟到完成全部
    回 Err（副本上的字节成为无害的未 ACK 重复）。不变量：**对调用方可
    见的 append ack ⟹ 区间在 contiguous 全副本 ACK 前缀内**。单测 ×2
    （双时序 + hole 之下仍正常 ack）。
  - **P1-乙:** 多 shard 各持同一物理目录的独立 DiskFS——shard A 标 Full
    后 shard B 照常分配。修复：health 改为按 canonical base_dir 共享的
    进程级 Arc<AtomicU8>（shared_disk_health 注册表）+ 跨实例共享单测。
  - **P2:** alloc_extent 的 save_meta 失败补 mark+remove entry（P0-D 同
    族）；cluster.sh 环境变量正则校验防参数注入。**P3:** gauge 槽初值
    1 在新语义下= Full（启动 2s 假阳性）→ 0。
  - **拒 2:** chaos 脚本全局 kill/rm-rf（全部既有 harness 的固定模式）；
    CLI 缺值 panic（手写解析器的统一习惯）。
- **验收:** workspace 0 error；stream 77 + ps 162 + manager 150 单测
  （新增 DiskHealth ×3、select_nodes spacious ×1、deferred-ack ×2）；
  enospc_chaos ×3 全 PASS；seed=13 隔离 chaos ok 零丢失；
  transport_chaos tcp 全 PASS。文档：stream CLAUDE.md note 25a（含两
  P1）、manager note 37、README "Disk-full (ENOSPC) behavior" 节。
- **passes:** completed (2026-06-12)

---

### LEASE8-LITE · 生产急修批次 4：fuse truncate 崩溃一致性 + 两个连带真 bug（2026-06-12）
- **目标:** `/loop 生产视角` 第四项 = BUG-LEASE-8 的可自治子集（完整修复=
  per-inode generation manifest，维持 deferred）。盘点结论：grow 路径
  本就安全（extent 全 ACK 后才 put meta——Explore agent 的"CRITICAL"判
  断经查证为误报），孤儿 extent 自愈（prefix-scan 删除 + 同 key 重写）；
  真正的窗口在 **truncate-shrink**：先毁 extent 后 put meta，中间 crash
  → durable size=旧值但尾部数据已毁 → 读出**文件内零**（本层唯一"伪造
  数据"的窗口）。
- **修复:** shrink 改 meta-first（meta put = 提交点，extent 清理后置）。
  不变量：**content[0..size] 恒等于最后成功写入的内容；crash 只能选择
  哪个 size 存活**。coco（GPT-5.5）2P1 全采纳：① meta-first 的残留
  extent 在后续 GROW（truncate-up/稀疏写）下会以旧数据复活（POSIX 要求
  零）→ `clean_beyond_eof`（raw prefix 扫描删 ≥eof 整 key + 按**真实**
  KV value 长度截 straddler）挂全部 grow 冷路径（truncate-grow 前置 +
  write_region 入口检测 leftover key/稀疏 grow）；连续 append 自界定，
  热路径零开销。② 提交后清理失败不再报错（调用方重试会落进 same-size
  早退 no-op 永不补清）→ WARN+invalidate。
- **T2 钓出第二个真 bug（读路径，partition-server）:** 完全越过 VP value
  末尾的子区间 GET 被 clamp 成 0 长读，`read_value_from_log` 的 pooled
  快路径把**回收的 RegPool 脏缓冲**整个当 value 返回——fuse 读
  截短/稀疏 extent 窗口得到**逐次变化的垃圾**而非零（manual repro:
  15744/1312/984 非零字节逐次不同；autumnfs 旁证 KV 本身干净）。修复：
  read_len==0 显式短路返回空。
- **harness（fuse_chaos.sh）:** T1 truncate 突发+kill -9+remount 前缀级
  校验（40 文件）+窗口命中证据；T2 shrink→grow 零值校验（含 remount 区
  分内核缓存）；coco 3 项采纳：burst timeout、kill 后 lazy umount、窗口
  断言。两个 harness 真相：本机无 fusermount3——历史所有 umount 都是静默
  no-op（守护进程在坏挂载上叠挂）→ unmount_all 循环 umount -l；
  工作负载与 unmount 窗口竞态会把文件写进**裸目录**并记入 manifest →
  fs-type 守卫 + T 阶段前停负载。
- **coco 终轮（1P0+2P1+1P2+1P3，采纳全部）:** ① P0——write buffer 的
  in-memory size 提前抬升会抹掉 pre-grow EOF：稀疏写经 buffer 后
  write_region 只见增长后 size，残留 straddler 被当合法数据 RMW 合并
  → 修为 `write::write` 入口在 size 抬升前 `offset > size` 即 sweep；
  ② clean_beyond_eof 是 grow 屏障——straddler 读硬错误必须传播中止
  grow（新增 kv_get_opt 区分 NotFound）；③ 读侧调用方契约——ioring
  read_into 的 dest 是复用 ring buffer，空/短读 slice 未写尾部回填零
  （fuse 路径预清零故安全）；④ T2 先断言 grow 后 size==1M（防 grow
  失败被"空尾=全零"漏判）+ setup 显式查错；⑤ unmount_all 失败返回非零。
- **验收:** fuse_chaos ×3 全 PASS（T1 40/40 前缀级精确、T2 全零+size
  断言、76/77 文件 manifest 零损）；fuse 43 + ioring 68 + ps 162 +
  stream 77 单测绿；seed=13 隔离 chaos ok 零丢失；workspace 0 error。
  文档：fuse CLAUDE.md "Crash-consistency contract" 全节。
- **passes:** completed (2026-06-12)

---

### ETCD-1 · 生产急修批次 5：etcd 故障 chaos——leaderless 黑洞 + 全队自杀 + 审计日志泄漏（2026-06-12）
- **目标:** etcd 是控制面唯一外部依赖，此前零 chaos 覆盖（历史 harness
  杀过 manager/PS/EN，从未杀过 etcd）。生产 etcd 维护（升级/compaction/
  快照）分钟级常态。新 harness `scripts/etcd_chaos.sh`：D1 杀 etcd 数据
  面须继续；D2 停 150s（越过 PS 退出预算）全队必须存活；D3 重启 etcd
  控制面恢复；终局零丢失校验。
- **pre-fix 复现（reproduce-first）:** D1 FAIL——新客户端进程在断电期
  无法解析路由（get_regions 严格 leader 门控，而失去 lease 的 ex-leader
  内存路由其实是全网最新）；D2 FAIL——PS 1→0 全队自杀（NOT_LEADER 与
  transport 失败共享 90s 预算）；下游连锁出 31 个假"丢失"。
- **修复 ①（stale-while-leaderless）:** `displaced` 标志（初始 true；当
  选清零；选举 CAS / F149 fence 诊断观察到**不同** instance 持锁时置
  位——key 消失=lease 过期非替位）。`ensure_routable()` = leader ||
  !displaced，只放行两个只读 RPC（get_regions/heartbeat_ps）；全部
  mutating handler 维持严格 ensure_leader。H3 rejoined-follower 黑洞
  保持关闭（displaced=true）。
- **修复 ②（PS 退出预算分离）:** `MAX_CONSECUTIVE_NOT_LEADER=450`
  （15min）独立于 90s transport 预算——NOT_LEADER 证明 manager 可达
  （非网络分区），leaderless 控制面无法驱逐任何人；数据安全从不依赖
  该退出（owner_epoch/region_epoch fencing），它只约束多 manager
  follower-pinned 的陈旧读窗口。
- **修复 ③（审计日志泄漏）:** `audit_retention_gc` 自 F211-I 起零调用
  方——`mgr_audit_log/` 在 etcd 无限增长。新 `audit_gc_loop`（日频、
  leader-only）+ `--audit-retention-days`（默认 90，0=关；顺手把该
  helper 的 env 读取改为 CLI flag——F195 规则）。
- **coco（GPT-5.5）1P1+1P3，采纳 1 拒 1:** P1——非对称分区（仅本
  manager 失联 etcd，B 接任）下 keepalive 失败不置 displaced，A 无限
  期供陈旧路由并把 PS 钉离真 leader → stale-while-leaderless 加
  `ROUTABLE_STALE_TTL`（15min，自 leaderless_since 起；displaced 在本
  机 etcd 链路恢复时由选举 CAS 检出）；窗口内的钉扎自愈（PS 撞 TTL →
  NOT_LEADER → 轮转 → NOT_FOUND 重注册）。拒 P3 = CLI 缺值 panic
  （手写解析器统一习惯，第三次一致拒绝）。
- **验收:** post-fix etcd_chaos 全 PASS ×2（TTL 折入后复跑：断电全程
  数据面满速、D2 150s 全队存活、D3 秒级恢复、650 ACK 零丢失）；
  manager-HA chaos (H1-H3) 回归 PASS 6369 ACK（H3 门控未回退）；
  manager 150 单测绿。
- **passes:** completed (2026-06-12)

---

### ETCD-2 · 生产急修批次 6：etcd 多副本部署 + 成员级故障验证（2026-06-12）
- **目标:** autumn-etcd 客户端早有多 endpoint 轮转重连
  （reconnect_shared round-robin）但部署面只起单 etcd——生产 3 副本
  etcd 集群从未被接线或验证过 failover 真的生效。
- **交付:** ① cluster.sh `AUTUMN_ETCD_CLUSTER=N`（默认 1）——N 成员
  etcd 集群（client 2379/2389/2399…，peer +1），manager 拿全 endpoint
  列表；stop 路径补 etcd*.pid 清理（coco P2 采纳：node*.pid 循环不匹
  配多成员，pkill 兜底只覆盖 autumn-rs 路径）。② etcd_chaos.sh
  `cluster` 模式 + D0 事件：杀 1/3 成员 → manager 须**保持 leader**
  （lease keepalive 经 reconnect 续期，metrics 断言 leader=1）+ 控制
  面全可用；D1-D3 随后杀余下成员走全断电退化路径。
- **验收:** cluster 模式首跑全 PASS——D0 成员杀后 manager 保持
  leadership、写持续推进、autumn-op 正常；D1-D3 复用退化路径（2/3 成
  员重启即恢复 quorum）；614 ACK 零丢失。single 模式回归 PASS
  （556 ACK 零丢失）。
- **passes:** completed (2026-06-12)

---

### SOAK-2 · 当日 6 批变更认证 soak + 3 个 harness 根修（2026-06-12）
- **soak 结果:** transport tcp 15 轮 + ucx 10 轮（全部故障事件 PASS，唯
  一失败 = E7 前置条件 artifact）；修后复跑 etcd cluster（614 ACK）+
  enospc（135 ACK）+ tcp 8 轮全 PASS，零丢失。
- **harness 根修 ×3:** ① E7 自供给——高 CHAOS_ROUNDS 的 E6 merge 可把
  集群并到 1 个分区，E7 现在先 split 自建前置而非 fail；② etcd_chaos /
  enospc_chaos 补端口排水——ucx→tcp 相邻阶段在 9001 TIME_WAIT 内启动
  时 tcp manager 直接 EADDRINUSE 退出（只有 ucx listener 会重试，
  F264）；③（ETCD-2 已含）stop 路径 etcd*.pid 清理。
- **passes:** completed (2026-06-12)

---

### WIRE-1 · 生产急修批次 7：wire-schema 指纹——混版本部署从静默损坏变响亮拒绝（2026-06-12）
- **目标:** rolling upgrade 的可自治子集。same-commit 约定下混版本部署
  **静默**失败（rkyv 解出垃圾——F275 stale wheel: part_id=0 全写失败且
  无任何指向性报错）。完整 rolling upgrade（线协议兼容策略）仍待用户
  决策；本批先把"混了"这件事变成启动时的硬拒绝。
- **实现:** autumn-rpc build.rs 对 wire schema **源文件**取哈希
  （manager_rpc/partition_rpc/frame/extent_rpc）→ 编译期常量
  `WIRE_FINGERPRINT`（哈希 schema 源而非 git commit：无关代码改动不扰
  动 dev 流，改 wire 结构必变）。`GetClusterIdResp.wire_fingerprint`
  携带；启动校验点：ClusterClient::connect（覆盖 client/op/fuse/
  ioring/python wheel——F275 形态直接拦截——及 EN 的 cluster_id 校验链）
  + PS finish_connect + EN verify_manager_cluster_id 显式。语义：成功
  响应但指纹不同（或解码失败/空指纹=旧端）= 硬拒绝并给出可操作信息；
  transport 失败 = best-effort 跳过（可用性优先，后续 RPC 自然响亮失
  败）。
- **coco（GPT-5.5 fast）1P1+1P2+1P3 全采纳:** ① 成功响应但解码失败曾
  被静默跳过——这恰是该机制要抓的形态（旧 manager 的 resp 缺字段）→
  解码失败=硬拒绝；② EN 站点显式校验（不依赖 ClusterClient 耦合）；
  ③ format! 多行字面量缩进混入错误信息 → 行继续符修正。
- **验收:** rpc 21 单测（新增指纹 ×2）+ client/ps/manager/stream 全
  绿；live smoke ×2（折入前后）全栈 put/get 正常（即全部校验点
  happy-path 实测）；workspace 0 error。文档：rpc CLAUDE.md WIRE-1 节
  （注明未来 rolling-upgrade 设计的 enforcement point 就在此处放宽）。
- **passes:** completed (2026-06-12)

---

### LAT-1 · 生产急修批次 8：PS 延迟直方图（OBS-1 收尾项）（2026-06-12）
- **目标:** /metrics 缺延迟维度（OBS-1 当时为热路径成本而缓）。
- **实现:** `LatHist`（9 桶 0.5ms..250ms + sum/count，存储非累积、渲染
  时累积 le）。**PUT 零新增热路径计时**——复用 WriteLoopMetrics 已测的
  per-batch `end_to_end_ns`，按 `n=ops` 观测（组提交批内每 op 经历的
  即批延迟）；GET 在 serve_get_local 加一对 Instant + 一次 borrow（与
  F183 req_count 同量级）。Prometheus histogram 规范导出
  （`autumn_ps_write/get_duration_seconds_{bucket,sum,count}`）。
- **coco（GPT-5.5 fast）零 P0-P2，1 P3 采纳:** 直方图实际覆盖批内全部
  写操作（Put/Delete/FenceBump）而非仅 PUT → 改名
  `autumn_ps_write_duration_seconds`（语义对齐看板/SLO）。
- **验收:** A/B perf-check（4K, p8, d8, 64 线程 12s）：写 999,864 →
  1,069,685 ops 无回归；live 直方图分布合理（p50≈1-2ms，GET 17 万次观
  测）；ps 162 单测绿。
- **passes:** completed (2026-06-12)

---

### UNLINK-1 · 生产急修批次 9：fuse unlink/rename 数据回收——含一个无条件泄漏 bug（2026-06-12）
- **目标:** BUG-LEASE-8 家族剩余的真实泄漏窗口（crash-mid-unlink 孤儿
  extent 永久不可达），以窄版意图日志关闭，无需完整 generation
  manifest。
- **盘点时发现第二个【无条件】bug:** rename 覆盖已存在文件时只删目标
  INODE、**从不删其 EXTENTS**——POSIX 原子保存模式（write tmp; mv tmp
  file）每次保存泄漏前一版全部内容，无需 crash。
- **实现:** `remove_unreachable_inode`（意图 tombstone
  `[0x04]rmtomb/[ino]` → 删 extents → 删 inode → 删 tombstone）统一
  unlink 与 rename-over 路径；`sweep_unlink_tombstones` 每次挂载
  （Init）重放幸存 tombstone。不变量：**tombstone 只为不可达 inode 写
  入**（sweep 无条件删数据）——rename-over 中强制把移除放到 dirent 覆盖
  之后。残余窗口=不可达点→tombstone 的单 RPC 间隙（修前=整个扫描+N 次
  删除）。
- **coco（GPT-5.5 fast）1P1+1P2 全采纳:** ① **第三个 bug**——
  `rename("a","a")`（或同 inode 两个硬链接间 rename）把源自身当
  "被覆盖目标"清理：递减 nlink + 删 inode（修前既有 POSIX 违反，
  UNLINK-1 后还会删数据）→ POSIX same-file no-op 早退；② sweep 单页
  4096 封顶 → 分页推进。
- **验收:** fuse_chaos 全 PASS（T1/T2 回归 + 新 T3：unlink 突发+kill+
  remount sweep、rename-over 内容校验、same-path rename no-op）；
  fuse 44 单测（新增 tombstone round-trip + 前缀不碰撞）。窗口命中说
  明：loopback 上单 unlink ~1.3ms，难以稳定命中 kill 窗口——sweep 在
  每次挂载执行（Ok(0) 路径全 harness 实测），机制由不变量+单测+T3 流
  程覆盖。
- **passes:** completed (2026-06-12)

---

### F227-CLOSE · F227 open-tail write-wedge 家族闭环认证（2026-06-12）
- **背景:** 用户选定方向。该家族 = 开放尾部 extent 某副本永久不可达时
  `current_commit`（全副本探测）永远失败 → 写入/flush 永久卡死（不丢
  数据但分区写冻结）。历史 seed=15 稳定触发、seed=13 多模式（1/10 通
  过率时代）。
- **复现尝试（reproduce-first）:** 当前 HEAD（0e1eb4f）上 seed=15 ×3 +
  全量 20-seed 严酷扫描（1-20，含历史热点 8/13/15）= **23/23 全 PASS
  零 wedge**——不再复现。
- **判定:** 已被既有修复组合关闭——F270 时代的 ① `ensure_tail_
  initialised` BUG#1 逃生通道（current_commit 持续失败 → `alloc_new_
  extent(stream_id, None)` 走 manager seal-over-reachable 收口换尾；
  禁传 Some(0)）+ ② manager note 31（卡死 recovery 标记不再阻塞已
  seal 尾上的 alloc），再叠加本日战役的 deferred-ack/fence/ENOSPC 修复。
  **seal-lenient 原则（manager note 28）维持为法律**：append 全副本
  ACK 是安全性来源，seal 对可达副本取 min 永不切已 ACK 数据。
- **passes:** completed (2026-06-12) — 状态由 OPEN → RESOLVED-certified

---

### ROLL-R0 · 滚动重启程序化（rolling upgrade 设计 R0 阶段）（2026-06-12）
- **目标:** docs/rolling_upgrade_design.md §3-R0 — 把 chaos 已证明的
  per-role kill+restart 零丢失能力固化为运维程序：逐进程滚动重启 + 每步
  收敛门 + 失败即停。同 commit 配置变更/换机/内核升级即刻可滚动；亦是
  R1+ 真正升级编排的骨架。
- **实现:** `scripts/rolling_restart.sh`（顺序按设计 §6：EN 逐个 → PS →
  manager）。收敛门：EN = list-nodes Online + 心跳 ≤10s + recovery-stats
  全静默（0 inflight / 0 backoff）；PS = info 全分区 ps= 路由非 unknown；
  manager = info 可答（etcd replay + leader 重选）+ 全节点 Online。每步
  之后逐分区写活性探针（per-partition 区间内派生 key，put+读回比对，
  非可打印边界跳过并警告）。滚动前 per-partition seed + 12MiB
  put-stream 大值，滚动后内容校验。cluster.sh 补齐 manager 缺失的
  per-process 子命令（start-manager/stop-manager/restart-manager，
  launch_manager 从 do_start 提取 + compute_etcd_endpoints 推导端点）。
- **coco（GPT-5.5）1P1+3P2 全采纳:** ① P1 探针 key 不保证落在目标分区
  （空 start 分区的探针字典序路由进末分区——首分区假覆盖）→
  derive_probe_prefix 按 [start,end) 推导可证明在区间内的前缀（start
  非 end 前缀 → start 本身；是前缀 → start+低于 end 下一字节的可打印
  字符），不可推导则响亮 SKIP + 诚实计数，LC_ALL=C 保字节序；②
  partitions_routed 的 ps= 检查有 base-addr 回退假阳性 → 注释明确
  liveness 探针才是权威门；③ 固定探针 key 会覆盖业务数据 + 遗留垃圾 →
  per-run namespace `__autumn-roll-<runid>` + EXIT trap 删除；④ 无并发
  互斥 → flock $DATA_ROOT/rolling_restart.lock 失败即停。
- **验收:** 3-EN/4 分区（hexstring presplit）集群、外部持续写负载下全
  序列通过 ×2（修复前后）：5 进程逐个滚动、全部收敛门 PASS、seed 校验
  PASS、负载期 191/191 ACKed key 零丢失；修复版 4 分区探针含首分区真
  覆盖（prefix="0"），探针 key 零遗留，并发第二实例被锁拒绝。
  derive_probe_prefix 8 边界用例（含 [ab..ab0) SKIP、[..0) SKIP）人工
  断言通过。README "Rolling restart" 节手动步骤可复执行。
- **passes:** completed (2026-06-12)

---

### ROLL-R1 · cluster_version 门 + wire 区间握手（rolling upgrade 设计 R1 阶段）（2026-06-12）
- **目标:** docs/rolling_upgrade_design.md §3-R1 — 滚动升级地基：持久
  cluster_version（operator 显式 bump 的特性门）+ 二进制自带
  `[min_wire, max_wire]` 区间、WIRE-1 单点指纹等值检查放宽为区间交集。
- **实现:**
  - rpc: `WIRE_VERSION_MIN/MAX` 编译期常量 + `wire_compat_check`（同指
    纹快路径 ∨ 区间交集）替换 `wire_fingerprint_check`，三处检查点
    （client connect / PS finish_connect / EN verify）全部切换。
    **防忘 bump 双保险**：`WIRE_VERSION_FINGERPRINTS` 注册表 + 单测
    （wire schema 源文件任何改动 → 指纹变 → 测试 fail，强制显式
    MIN/MAX 决策，pre-R3 规则 = bump MAX 且 MIN=MAX）；运行时 fraud
    交叉校验（对端声明我方注册表已有的版本但指纹不符 → 拒绝）。
  - manager: etcd `autumn-rs/cluster_version`（ASCII decimal，跨序列化
    时代永远可读）+ CAS-imprint（首 leader 种到自身 max）+ replay 安装
    + `bump_cluster_version`（leader-only、严格 current+1、≤自身 max、
    value-CAS 防并发双 bump）。`GetClusterIdResp` 携带区间+
    cluster_version（该结构从 R1 起冻结=协商通道）。
  - autumn-op: `cluster-version`（GET 走 etcd 新读防 follower 陈旧）+
    `upgrade-version [--to N]`。
  - 顺手修复 cluster.sh 两个预存在 bug：① restart 竞态（旧 leader 10s
    租约残留 + format 需 leader → set -e 中途夭折；launch_manager 增加
    leadership 等待门）；② save_cluster_config 在零 AUTUMN_* 环境变量
    时 grep 空匹配 + pipefail 中止 do_start；以及 rolling_restart.sh
    的 bash IFS=TAB 吞前导空字段 bug（空 start 分区探针仍错路由——
    1 分区回归跑暴露；改 '|' 分隔 + 打印派生前缀审计行）。
- **coco（GPT-5.5）3P1+3P2，4 采纳 + 2 文档化:** ① P1 新端 decode 旧
  GetClusterIdResp 失败先于兼容检查 → 文档化为冻结契约（R1 本身是
  same-commit 部署，握手从 R1 之后的版本对开始生效，前提=该结构冻结）;
  ② P1 异指纹同区间放行 → 运行时注册表 fraud 交叉校验（采纳）; ③ P1
  持久值超 max 未拒 → parse_cluster_version 单点 fail-closed（采纳，
  回滚安全从约定变机制）; ④ P2 imprint 失败仍当 leader → 维持
  best-effort 并补安全论证（无 gate 消费者 + bump 对缺失 key 天然 CAS
  拒绝 + 每轮选举重试）; ⑤ P2 follower 陈旧读 → GET 走 etcd 新读
  （采纳）; ⑥ P2 --to 缺参 panic → 越界检查（采纳）。
- **验收:** 单测全绿（rpc 25 含 5 新 wire-version 测试、manager 152 含
  bump 校验+回滚拒绝 2 新测、ps 162、stream 77、client 27）。live：
  fresh 集群 cluster_version=1/[1,1]；upgrade-version 正确拒绝（超
  max，给出"先升二进制"指引）；restart-manager 后 etcd replay 读回；
  **真混版本实测**：R0 时代旧 autumn-op 连新 manager → 响亮拒绝
  （decode 失败 + 指引 rebuild）；--to 缺参走 usage 不 panic；4 分区
  rolling_restart 全序列零丢失回归 PASS（4 个探针前缀含 p0='0' 全部
  可证明在区间内）。
- **passes:** completed (2026-06-12)

---

### ROLL-R2-A · 控制面 prost 迁移第一阶段：etcd 核心元数据 → prost（rolling upgrade 设计 R2）（2026-06-13）
- **目标:** docs/rolling_upgrade_design.md §2/§R2 的硬前提 —— etcd 持久值
  跨版本可重放（rkyv 内存布局做不到；prost tag 编码天然前后兼容）。用户
  拍板：全量迁移 + 切换方式 = cluster.sh reset。
- **技术决策:** rkyv Archive + prost::Message 可共存于同一 struct（实测，
  prost 提供 Debug+Default 故从 derive 去掉这两个，保留 Archive/Serialize/
  Deserialize/Clone + PartialEq/Eq）。**持久类型必须用 named-pair
  `repeated`（U64U32Pair）而非 prost `map<>`** —— map 编码非确定性会破坏
  manager note 33 的 etcd value-CAS（baseline 字节比对）。u8/u16/[u8;N] 非
  prost-native → u32/Vec<u8>。
- **实现（本阶段=7 个核心前缀）:** nodes/disks/streams/extents/partitions/
  partitionVpRefs/regions 的 7 个 Mgr* 类型双 derive + prost 字段 attr +
  MgrRange(嵌套)；helper prost_encode/prost_decode；mirror_*/persist_extent/
  split/merge/recovery 的 etcd 写 + replay 解码 + F33 CAS baseline 全切
  prost；类型变更 shard_ports u16→u32（仅 manager 边界，热路径留 u16，coco
  P2 用 try_from 非 as 截断）、VpRefs.refs tuple→U64U32Pair（含
  SyncPartitionVpRefsReq）。WIRE_VERSION 1→2（MIN=MAX=2，与 v1 无交集；v2=
  prost 格式，回滚到 v1 被 fail-closed 拦截）。
- **coco（GPT-5.5）2P1+1P2:** 2 个 P1（旧 rkyv etcd 无法被 prost 读 / CAS
  baseline 不匹配旧值）= 混合格式迁移顾虑，用户已拍板 cluster.sh reset 切换
  （etcd 清空，仓库标准 same-commit 惯例 + v2 wire bump 强制旧二进制拒绝），
  按设计接受并文档化；1 个 P2（as u16 静默截断）采纳 → u16::try_from +
  越界丢弃告警。
- **验收:** 单测全绿（rpc 27 含 prost round-trip+determinism+registry、
  manager 152、ps 162、stream 77、client 27）。**live 端到端**：fresh
  cluster.sh reset（cluster_version=2）put/get → manager 重启强制 prost
  replay 全元数据，数据存活 + 4 分区路由正确 + 零 decode error；split（4→5
  分区，prost split-write 块）+ 再次 manager 重启 replay，split 元数据+数据
  存活；全序列 rolling_restart 零丢失。
- **passes:** completed (2026-06-13) — R2-B（RPC 结构 + 剩余持久前缀）待续
