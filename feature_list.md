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
| F-ioring-lease-2 | autumn-ioring-daemon Open/Close acquire+release lease — RING_VERSION 1→2 (Open's SQE flags byte now carries `LEASE_MODE_READ/WRITE`, 0 = safe default WRITE); per-session `DaemonClientId` (UUID); per-inode refcounted `held_leases` so a 2nd Open within a session shares the lease, only the LAST Close releases; per-session 5 s `session_heartbeat_loop` renews held leases (NotHeld → invalidate session ring_fds with EBADF). New `autumn-ioring::lease` module wraps the 4 RPCs into typed `acquire/release/heartbeat/poll_invalidations`. **Phase 1, step 2 of 4** — Phase 1 COMPLETE (see F-ioring-lease-4). | ioring/manager |
| F-ioring-lease-3 | Long-poll invalidation channel — manager-side `ClientInbox.waker` parks the `MSG_POLL_INVALIDATIONS` handler when the queue is empty (`drain_or_park`); push fires the waker so the next event resolves the poll within ~ms (vs the 10 s timeout). Daemon-side per-session `session_invalidation_poll_loop` drains events in a tight loop; on transport error OR overflow sentinel it wholesale-invalidates the session's `ring_fds` + `held_leases` + best-effort releases (plan §6.4). **Phase 1, step 3 of 4** — Phase 1 COMPLETE (see F-ioring-lease-4). | manager/ioring |
| F-ioring-lease-4 | `OpenedExtents` version-tagged + cache invalidation e2e — `OpenedExtents.lease_version: u64` populated at Open from the AcquireLease response; per-session `InvalidationMap: HashMap<u64, u64>` (ino → min-valid-version) bumped by `session_invalidation_poll_loop` via the pure-fn `apply_invalidation`. Read SQE arm uses `cache_is_stale` to compare; on stale → `fuse_read::reload_extents` re-fetches the inode meta + extent map, updates `lease_version` to the new floor. Multi-daemon close-to-open coherence end-to-end (writer-close pushes WriterClosed → reader's next Read on the same ring_fd reloads → sees new bytes). **Phase 1 COMPLETE.** 收尾认证 (2026-06-15): byte-level full-cluster e2e `f_ioring_lease_phase1_e2e.rs::phase1_close_to_open_coherence_e2e` (manager+2EN+1PS, seed v1 → writer overwrites v2+release → reader poll→reload→读到 v2 + 跨 extent 校验) CERTIFIED GREEN 3/3 under `--ignored`. | ioring/manager |
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

### ROLL-R2-REVERT · prost 迁移回退,升级安全收敛为 rkyv fail-loud（2026-06-13）
- **背景:** 用户两条约束定调 —— 不需要 rolling upgrade(混版本同时在线);
  需要"全停全启升级安全"且**生产绝不 cluster.sh reset**。
- **关键认知:** 全停全启的"安全"(不静默损坏) plain rkyv 本身即保证 ——
  校验式 `from_bytes` 在持久结构布局不符时响亮失败(manager 重放报错 →
  当不上 leader),绝不静默把旧字节解成错值。prost(R2-A)多给的只是"不
  reset 也能原地改 etcd schema"这个独立能力,可按需用一次性迁移补,不必
  常驻。故 R2-A 的 prost 是纯复杂度(且对"忘改 reset"反而静默风险更高)。
- **动作:** `git revert` ROLL-R2-A(b587471)—— etcd 7 前缀回 rkyv、
  WIRE_VERSION 退回 1、移除 prost dep/helper/U64U32Pair/双 derive/etcd
  codec 切换。保留 R0(运维滚动重启)+ R1(wire 门 + cluster_version 格式
  戳 + 回滚 fail-closed)。新增 `AutumnManager::replay_decode_err`:replay
  解码失败时给可操作提示(schema 变了需匹配二进制/一次性迁移,勿 reset)。
- **最终升级安全模型:** plain rkyv fail-loud(安全)+ R1 格式戳(回滚拦截)
  + 按需迁移(演进能力,零常驻代码,第一次真改 etcd 结构时再写)。砍掉
  R2 全量 prost / R2-B / R3 协商 / upgrade_chaos.sh。
- **验收:** revert 后 build 全绿、rpc 25 单测过(registry 指纹回到 R1 值、
  MAX=1);live:fresh reset(cluster_version=1)put/get → manager 重启
  rkyv replay 全元数据,数据存活 + 4 分区路由 + 零 decode error。
- **passes:** completed (2026-06-13)

---

### PROD-FIX-1 · META-FAILCLOSED + EC-PREPARE-DURABLE(生产就绪审计 P0,2026-06-13)
- **背景:** coco arch 生产就绪审计找出两处崩溃一致性 fail-open。按用户"先复
  现再修"(尤其易错区)纪律处理。
- **META-FAILCLOSED(先复现):** `load_extents` 把损坏 `.meta`(CRC 不符)
  当缺失 → 默认 open/owner_epoch=0 → fence 绕过 ghost-append。复现测试
  `corrupt_meta_quarantines_extent_and_rejects_stale_append`(node1 fence 到
  10 → 损坏 .meta → reload → stale epoch=5 append;修前 CODE_OK=绕过,修后
  拒绝)。修复:`ExtentEntry.corrupt_meta` flag;load 区分 present-but-corrupt
  / 非-NotFound 读错(coco P1,都 quarantine)/ 真 NotFound(默认 open);
  append(handle_append + build_append_future)/read(handle_read_bytes +
  build_read_future 批量热路径,coco P1)/commit_length 拒绝;成功持久 .meta
  清 flag。
- **EC-PREPARE-DURABLE:** EC 2PC prepare 写 `.ec.dat` 只 sync_data 内容,未
  fsync 父目录 → 断电丢 dirent,"durable prepare record"不成立 → commit 卡
  死。修复:`fsync_staging_dir` helper,prepare 新写+幂等早退(coco P2)+
  commit rename 后都 fsync 父目录。套用 `write_meta_locked` P0-B 模式。
- **coco（GPT-5.5 findbugs）3 条全采纳:** P1 非-NotFound 读错也 quarantine;
  P1 批量读热路径 build_read_future 也要拒绝;P2 幂等早退也要 dir-fsync。
- **验收:** stream 78 单测全绿(含复现测试 + EC f119-d/f153);workspace
  build 绿;live smoke(3-node reset + put/get + 2MiB put-stream + restart
  node1 + restart-manager)数据存活、正常 .meta 重载无误判 quarantine。
- **passes:** completed (2026-06-13)

---

### PROD-FIX-2 · WAL-FAILSTOP:log_stream 中间损坏 → fail recovery(生产就绪审计 P0 #2,2026-06-13)
- **背景:** coco arch P0 #2。recover_partition replay 与 GC 遇完整记录 CRC/
  长度不符(bit rot/torn write)时 skip 继续 → 静默丢已 ACK 写 + replay 顺序洞。
- **复现(先):** `wal_mid_record_corruption_fails_recovery_not_silently_skipped`
  —— [A][CRC 损坏的 B][C],修前 decode_records_chunk 返回 [A,C](B 静默跳过,
  len==2=复现);修后 .is_err()。
- **修复:** decode_records_chunk 改返回 Result,Corrupt→Err(recover_partition
  `?` 传播→open 失败响亮→走副本重建);Incomplete(尾部截断=crash-tail)仍
  干净 break。process_gc_chunk 同样 Corrupt→Err(abort GC,不越过损坏记录
  punch holes)。
- **coco 复审 1 P1 采纳(同轮闭环):** length 字段被 bit-flip 增大时 decode_one
  在 CRC 校验前因 `len<total` 返回 Incomplete,逃过 Corrupt → 在 recover_partition
  的"committed-end 非空 carry"处兜住:F261 committed-clamp 下 committed 边界
  必在记录边界、未提交尾部被 clamp 掉,故 committed-end 残留 carry 只可能是
  length 损坏/滞后副本截断 → fail(撤销 pre-F261 的"trailing partial=crash-tail"
  陈旧容忍)。回归断言 inflated-length → B 全留 carry。
- **验收:** ps 163 单测(含复现+inflated-length 断言);live(40 key + 3MiB
  put-stream,PS 重启 → 0 丢失、大值 OK、无误触 WAL-FAILSTOP)。test-only 的
  decode_records_full/with_offsets 保留旧 skip(非生产路径)。
- **passes:** completed (2026-06-13)

---

### SELFHEAL-A1 · 读路径按 avali 隔离副本(WAL replay 自愈环 增量 A 基石)(2026-06-14)
- **背景:** WAL-FAILSTOP 后 replay 遇坏副本 loud-but-stuck。自愈环(docs/
  wal_selfheal_design.md,coco arch 两阶段评审)的前提:**读路径必须能把坏
  副本从 serving set 移除**,否则"读干净副本开服"后用户 VP 读仍命中坏副本
  返回坏数据(coco P0#2)。
- **实现:** `eligible_replica_slots(ex)` 纯 helper —— sealed replicated extent
  跳过 avali 位为 0 的 slot(隔离 recovering/坏副本);**open extent 不过滤**
  (avali=0 是"未封"常态,一致性靠 commit-min);EC 不过此 helper(走
  ec_subrange_read,缺 shard 重建不跳过);全 0 防御回退全读。
  `read_replicated_with_failover` 接入:无隔离时(eligible==n)热路径不变;
  有 slot 隔离时跳 hedge + 跳坏 slot。
- **验收:** 3 单测(sealed 排除 avali=0 slot / open 不过滤 / 全清回退);
  stream 81 单测全绿(热路径未回归);workspace build 绿。
- **passes:** completed (2026-06-14) — 自愈环增量 A 后续:A2 per-replica 读 /
  A3 replay 跨副本 decode-check / A4 seal-and-roll+isolate / A5 manager 上报
  RPC(fencing+etcd-first) / A6 EN quarantine;增量 B forced-repair。

### SELFHEAL-A2/A3/A5 · WAL replay 跨副本自愈 + manager 隔离 RPC(增量 A 主体)(2026-06-14)
- **背景:** A1 装好读路径过滤。本次补齐"replay 发现坏副本→换干净副本继续→
  隔离坏副本"闭环(docs/wal_selfheal_design.md 增量 A)。
- **A2(stream client):** `read_committed_from_replica(eid, idx, off, len) ->
  (bytes, committed_end, node_id)` —— 指定副本、不 failover、committed-clamp;
  `replicated_replica_count`。EC extent → Err。(commit 8ab48d9)
- **A3(recover_partition):** replay 每 chunk 用 `read_committed_bytes_from_extent`
  返回的 `committed_end` 算 `expected`;**corrupt 信号 = 短读(serving 副本截断)
  ‖ decode Err(CRC/长度) ‖ committed-end 残留**。命中→`self_heal_replay_chunk`:
  只对 **sealed replicated** extent,跳过 avali=0 已隔离 slot(I2,coco P1),
  逐 ELIGIBLE 副本重读同一 committed 窗口、decode-check,选第一个干净的继续
  replay,坏 node_id 累积;读错误的副本不记坏(I7);全坏/open/EC → fail loud。
  纯选择核 `select_clean_replica_chunk` 单测化。修复后 cursor 按 `want_full`
  前进(不被短读 `got` 缩窄)。
- **A5(manager `MSG_REPORT_CORRUPT_REPLICA`=0x4C,wire v2):** fencing =
  partition owner_epoch CAS + extent eversion CAS + **partition→log_stream→extent
  归属链校验**(coco P1:防跨 partition 隔离);etcd-first persist + **verify-at-
  apply**(F146 式:await 后 eversion 变了就拒,不覆盖并发);拒 open(A4 待做)/
  EC/最后一个副本/全非副本(stale layout,coco P2:不假成功);清坏 slot avali 位
  + bump eversion(逼各 PS refetch,A1 过滤生效);slot<32 shift 守卫。PS 上报后
  `invalidate_extent_cache`。A4(open-tail seal-and-roll)与 A6(EN 本地 quarantine)
  仍 deferred。
- **验收:** A3 选择核单测(干净选择/读错不记坏/短读记坏/全坏 None/非终块残留 OK);
  A5 manager 单测 10 条(清位+bump / stale owner / stale eversion / EC 拒 / open 拒 /
  最后副本拒 / 幂等 / 非副本 stale-layout 拒 / 跨 partition 拒 / 越界 extent 拒);
  4 crate lib 全绿(432);workspace build 绿;coco deep 两轮,P0/P1 全处置。
- **已知残留(登记,非阻塞):** (1)初始 serving read 不返回 node_id,若坏副本在
  cross-read 时恰好超时则该次无法归因(bit-rot 实际场景重读仍坏→可归因;I7 不隔离
  不可达副本);(2)A5 etcd 为 blind-put 非 CAS —— 与 apply_recovery_done 等同类,
  按 manager note 33 deferred(reproduce-first 再上 CAS);(3)cluster_version 不门控
  (全停全启 + wire-fingerprint 启动拒绝已防混部)。
- **passes:** completed (2026-06-14)

### SELFHEAL-A4 · open-tail 内容损坏 seal-and-roll(自愈环增量 A 收尾)(2026-06-14)
- **背景:** A3/A5 只处理 sealed extent;open tail(当前写入 extent)损坏时
  self_heal 返回 None → fail loud。open extent 没有 avali 可原地隔离,且 commit-min
  随读集变 → 设计 I3 规定 open-tail = seal-and-roll。
- **实现:** `StreamClient::seal_and_roll_tail(stream_id)` = `alloc_new_extent(None)`
  (复用现有 F227 manager probe seal-over-reachable + roll 新 tail,**不改 seal 协议**,
  纯 manager RPC + cache 更新,recovery 期 owner_epoch fence,worker 未起亦可调)。
  `self_heal_replay_chunk` 改为循环:open + **内容损坏(!truncated)** → seal-and-roll
  → invalidate cache → 重取(现 sealed)→ 同一 pass 跑 sealed cross-read 隔离坏副本
  (corrupt_acc 累积 → 复用 A5 上报循环,不依赖重开);最多封一次(sealed_once 守卫)。
- **coco 三轮(数据安全关键,F227 相邻区):** R1 2 条(P0 截断副本被纳入 seal min →
  可封到 acked 以下丢数据 / P1 重开靠 rotated read 撞坏副本不可靠)→ 改为
  **!truncated 才封**(截断 open tail 仍 fail loud,避免封到 acked 下)+ **一 pass
  同步隔离**(不依赖重开);R3 P3 文档对齐。
- **安全论证:** open-tail 实际触发是内容 bit-rot —— 坏副本文件长度完好,probe
  commit_length 报满 → seal min 不变 = 正确长度,acked 前缀(all-replica-ACK 在每个
  committed 成员)≤ min,零丢失。截断到 acked 以下属另一条 pre-existing F227 edge,
  本次不碰(明确 fail loud)。
- **验收:** 不引入新 wire 结构(复用 StreamAllocExtentReq);stream+ps 245 lib 单测绿;
  workspace build 绿;coco R2/R3 P0-P2 clean。
- **已知残留:** open-tail **截断**(非内容损坏)仍 fail loud —— "seal-over-HEALTHY
  排除坏副本" 需改 seal-probe 排除机制(动 F227 协议,revert-prone),deferred 到真复现。
- **passes:** completed (2026-06-14)

### ADMIN-AUTH · 管理操作鉴权(DESIGN ONLY,2026-06-14,未实现)
- **背景:** 全代码库零鉴权/授权/TLS。可信内网威胁模型 = 挡误连别集群 / 流氓客户端
  跑破坏性管理命令(非防 MITM/多租户)。
- **决定 Option A:** 单一共享 admin secret(bearer capability),只 gate manager 的
  10 个操作员专属变更 op(fence/remove/maintenance/clear-override/bump-version/
  update-EC/force-EC/create-stream/upsert-partition/register-node —— 已核实 PS/EN
  内部不发)。split/merge/punch/truncate/alloc 靠 owner_epoch 已挡;数据面 + 只读
  管理留开。否决 Option B(HBase 式同 gate PS Maintenance split/merge/gc/compact)。
- **实现路径(留待将来):** `--auth-token-file`(rs 不读 env,cluster.sh 生成分发);
  `is_admin_msg(msg_type)` 共享判定;admin payload 前缀 32B token + manager
  constant-time compare(零 wire-struct 改动);opt-in(无 token=放行,零回归)。
  入口 3 个(manager + autumn-op + autumn-client),PS/EN/python/fuse/ioring 不动,
  热路径零开销。详见 `docs/admin_auth_design.md`(含 HBase 两层对照 + 连接面调研)。
- **passes:** not_completed (design only, 用户 2026-06-14 决定记录留后做)

### SELFHEAL-E2E · 自愈环端到端复现 + 读路径隔离漏洞修复(2026-06-14)
- **scripts/selfheal_chaos.sh:** 3-EN/1-partition TCP 集群,写 12×1MiB 大值(→
  log_stream VP),kill -9 PS(不优雅刷盘→未刷 tail 重放),**直接翻 slot[0] 副本
  .dat 一个字节**(真 bit-rot;open tail 读 replica[0]-first → 确定命中)。
  S1(正向):重启→A4 seal-and-roll→A3 跨副本→A5 隔离;断言 PS 开成功 + **全部
  键 byte-exact(含被损值的键)** + extent 封 + slot[0] avali=0、其余=1。
  S2(负向,毁数据):同 extent 另两副本也翻→无干净副本→open fail loud + 分区不可用。
  node_id→dir 经 per-dir node_id sentinel 动态映射(已验证 slot[0]=node1/3 随机均通过)。
- **读路径隔离漏洞(e2e 现场抓到,真数据正确性 BUG):** A1 只把 avali 过滤接进了
  copy 路径 `read_replicated_with_failover`,但**两条 VP 值读快路径绕过**:
  (1)`read_value_into_pooled`(PS 代理 ZC 读)、(2)`extent_read_descriptor`
  (F259 客户端直读描述符)—— 都按 slot 顺序读 addrs[0],隔离后(avali=0)仍把
  坏副本当首选,而 VP 读不校验 CRC → 自愈后 GET 被损值的键**仍返回坏字节**
  (正是设计 P0#2/I2)。修复:两处都按 `eligible_replica_slots(ex)` 过滤(健康
  extent eligible==全部、顺序不变 → 热路径零变化;仅隔离时跳坏 slot)。
- **验收:** 修复前 e2e 现场复现(sh-key-6 5/5 返回坏 md5)→ 修复后 12/12 byte-exact;
  stream 81 lib 单测绿;coco deep 评审 Rust 改动 0 issue。
- **passes:** completed (2026-06-14)

### SELFHEAL-E2E-CERT · 自愈环增量 A 端到端认证(reproduce-first,2026-06-15)
- **目的:** 用户 /loop 选"自愈环端到端验证"。把增量 A(A1 读路径隔离 / A3 replay
  跨副本 decode-check / A4 open-tail seal-and-roll / A5 manager 隔离上报)从"单测+单跑"
  升级为活集群 reproduce-first 认证。**无代码改动**——纯运行 `scripts/selfheal_chaos.sh`。
- **结果:** 3 次连跑 **3/3 PASS,确定性一致**(每次 sealed=12583431、slot[0]=corrupt-node
  avali=false、slots1/2=true、12/12 键 byte-exact 含被损值键)。
  - S1(正向自愈):翻 slot[0] .dat 一字节 → kill -9 PS → 重启 open 命中损坏 OPEN tail →
    A4 seal-and-roll → A3 跨读干净副本 → A5 隔离坏副本(avali=0 + eversion bump);
    PS 开成功 + 零丢失 + extent 封 + 仅坏 slot 隔离。
  - S2(负向 fail-loud):另两副本同 extent 也翻 → 无干净副本 → open **响亮失败**、
    分区不可用(WAL-FAILSTOP / not-self-healable)。
- **环境:** /data05(非 /tmp overlayfs)、TCP、3-EN/1-partition、release 二进制。
- **passes:** completed (2026-06-15)

### EC-COMMIT-ATOMIC (#5) · EC commit rename↔save_meta 崩溃窗 — intent marker(2026-06-15)
- **崩溃窗(reproduce-first 确认):** `commit_shard_local` 先 `rename(.ec.dat→.dat)`+dir-fsync
  (durable),再改 eversion/sealed + `save_meta`。两步间崩溃(kill -9 即可,rename 已 fsync)
  → `.dat`=shard 但 `.meta`=pre-EC(stale eversion / sealed_length=原长),且现有幂等
  (staging 缺失 + eversion stale)直接 Err → commit **永久卡死** + 读 shard 当完整值(损坏)。
- **修复 intent marker:** `extent-{id}.ec.commit` = `[new_eversion][sealed_length]`,
  rename 前 durable 写(tmp→sync→rename→dir-fsync),save_meta 后删。`finish_ec_commit`
  共享 helper(rename-if-staging + 总是 reopen .dat + 单调 fetch_max eversion + save_meta)。
  `load_extents` 启动重放:Valid+eversion<marker → finish 补齐;eversion≥marker → 仅清
  marker(不回退);Corrupt(present 但损坏)→ quarantine 失败-关闭;Absent → 跳过。
  同进程 retry 分支同样用 **marker payload**(非 RPC 参数)+ eversion 门控。corrupt_meta
  的 extent 绝不 replay(marker 无 owner_epoch,会 fence 旁路)。
- **coco 4 轮:** P1×2(corrupt_meta 旁路 quarantine / 重放失败仍服务)→ 修;
  P1×2(retry 用 stale fd 不 reopen / RPC 参数覆盖 marker)→ always-reopen + marker 为准;
  P2×3(eversion 回滚 / marker 三态 NotFound-vs-corrupt / commit retry 缺 eversion 门控)→
  fetch_max + 三态 + 对称门控;P3(display() 非 UTF8)→ OsString。
- **验收:** 4 单测(无 marker 状态损坏复现 / marker 重放修复+幂等 / corrupt-meta 跳过 /
  corrupt-marker quarantine);stream 85 lib 绿;ec_integration/ec_failover/extent_recovery 全绿。
- **passes:** completed (2026-06-15)

### EC-ABANDON-DEADCODE (#3) · 删除 fence-handover 死代码 + 诚实化残留(2026-06-15)
- **死代码确认:** `ec_abandon::push_fence_handover_to_targets` 用 `commit_length_on_node`
  发更高 owner_epoch,指望 EN `handle_commit_length` 做 handover bump。但 commit_length
  自 2026-05-29 三概念规则起 **CHECK-ONLY-NEVER-HANDOVER**(更高 owner_epoch 走
  `>=→no-op` 分支,不 store)→ push **零作用**,ghost ex-coord 从未被隔离;WARN 日志却
  宣称有保护(假安全)。单测 `ec3_fence_handover_tests::commit_length_is_check_only_never_handover`
  证明(高 owner_epoch→no-op 不 bump,低→LOCKED)。
- **处理:删除**(死代码 + 误导注释;删一个 no-op 不改运行时行为,只去掉假安全 + 无用 RPC)。
  保留 advisory(真 OP 信号)。
- **残留(登记,非本次改动引入,non-reproduced):** fenced coordinator 进程仍活可继续
  向 targets 发 EC 2PC;EC 写/提交只 CHECK 不 raise owner_epoch,sealed extent 无 append
  → fence 不会被顶。但**有界**:ghost-alone = eversion fence 挡成响亮 wedge(非静默损坏);
  reissue-race = 读路由跟 manager 最终 layout(orphan shard 不被路由,reconcile 回收)。
  coco P1 要求加码级 gate;判断为**理论窄窗 + 删除不恶化 + reproduce-first**,deferred(若复现:
  force_ec_convert 见 advisory 拒绝 + OP 确认清除 / 专用 fence-bump RPC,**绝不用 commit_length**)。
- **验收:** manager lib 162 / F211 lifecycle 9+8 / ec3 单测全绿;coco P1 评估后 deferred(理由记档)。
- **passes:** completed (2026-06-15) — 删除死代码 + 残留诚实化;下一步 #6 split FREEZE_TTL。

### SPLIT-FREEZE-BUDGET (#6) · 把 split 临界区限制在 FREEZE_TTL 内(2026-06-15)
- **窗口:** split 冻结写后抓 commit_length 再调 multi_modify_split(原 `for 0..8` × 每调 30s
  超时 → 可达 ~4 分钟 >> FREEZE_TTL=30s)。若临界区超过 TTL,`check_freeze_ttls` 自动解冻
  → 写恢复越过抓到的 commit_length → split 用 stale length 封 log_stream → 这些写落在
  sealed_length 之上 → 恢复时丢失。F255 只 bound 了 row-invalidate barrier(15s),
  multi_modify_split 循环没 bound。
- **修复(收窄,非完全闭合):** multi_modify_split 加 per-call timeout 参数;split 路径用短
  超时(SPLIT_CALL_TIMEOUT=8s)+ deadline-bounded 循环(`split_freeze_deadline = FREEZE_TTL
  - call_timeout - 2s`):freeze 持有超过 deadline 就**干净 abort**(解冻 + Err,客户端重试),
  绝不让 split 在 freeze 可能已失效后**成功**。成功后再校验 frozen_for_split 仍持有(belt-and-
  braces)。把窗口从"几分钟重试"缩到"一个在途调用的 8s"。
- **已知残留(coco P1,登记,deferred):** 客户端 RPC 超时**不能**取消 manager 侧已提交的事务
  —— 单个 multi_modify_split 到了 manager、manager 卡 >8s(PS 记超时+解冻)、随后 etcd txn
  才落地,仍会用 stale sealed_length 提交。**这是分布式提交不确定性,客户端侧无法闭合**。真正
  的修复在 manager 侧:handle_multi_modify_split 内**重新 probe commit_length 按当前 all-replica
  值封**(解冻后的写要么落在封点下=保留、要么之上=被已封 tail 拒绝重路由=不丢;F227 已有 probe),
  或带 freeze token/deadline 让 manager 提交前校验。**non-reproduced + revert-prone F227/split 区,
  按 reproduce-first deferred**。
- **验收:** budget 不变量单测(deadline+call_timeout < FREEZE_TTL);split/vp-after-split 集成测试绿;
  workspace build 绿。
- **passes:** completed (2026-06-15) — 收窄 + 残留登记;manager 侧完全闭合 deferred(待复现)。

### SPLIT-CASCADE-GUARD (#6 后续, reproduce-first) · split 重试级联 + 静默丢写复现(2026-06-15)
- **复现(临时在 handle_multi_modify_split 注入 40s 延迟):** 原以为 #6 是"freeze 解冻后
  stale-seal 静默丢写"。实际复现出**更清楚的真 bug**:PS 对慢 manager 重试 multi_modify_split
  时,**每个重试各自提交一个 split → 1→6 分区级联**(TEMP 触发 5 次)。而且我的 #6 fix(更快的
  8s 重试)**放大**了级联。
- **修复(manager 侧 split-inflight guard):** `AutumnManager.split_inflight: HashSet<part_id>`
  (内存,单线程,RAII `SplitInflightGuard` 全出口清理);`handle_multi_modify_split` 入口对同
  partition 并发 split 拒(CODE_PRECONDITION)。复现验证:级联从 1→6 收敛到 **1→2**,TEMP 触发
  **1 次**。不持久(级联在单 manager 慢窗内;跨 failover 重复是更窄残留)。
- **静默丢写没复现:** 级联修掉后,解冻后的写**一致地被响亮拒绝**(owner_epoch/seal fence,
  NEWOK=0,两轮都是),从不"ack 了再静默丢"。所以 manager 侧 commit_length re-probe(理论上闭合
  client-timeout-vs-server-commit 残留的完整修复)**复现不支持 → deferred**。freeze-held 复检留作
  belt-and-braces。
- **验收:** split-inflight guard 单测(同 part 并发 split 被拒);manager 163 / ps 166 / stream 86
  lib 全绿;split 集成测试绿;coco deep 0 issue。
- **passes:** completed (2026-06-15) — 级联(真 bug)已修;静默丢写复现不出来 → re-probe deferred。

### MERGE-EC-REPLAY · merge survivor 重启永久 WAL-FAILSTOP(EC log extent committed_end 透传 bug,2026-06-16)
- **现象:** `autumn-op merge <survivor> <victim>` 后,survivor 分区重启**永久打不开**,无限重试,每次同一处
  `WAL-FAILSTOP: 1183349 leftover byte(s) at the committed end of log_stream extent 40`(40 = victim 的 3GB EC(3+1)
  log extent,被 splice 进 survivor)。下游症状:gallery / autumn-op 路由到该分区 NotFound。
- **根因(实锤,debug 埋点定位):** `StreamClient::read_committed_bytes_from_extent` 正确算出 `committed_end =
  sealed_length`(3GB),但末尾 `Ok(r) => return Ok(r)` **把 `read_with_layout` 的返回元组整个透传**,而 EC 路径
  `ec_subrange_read` 的第二元素是 **shard 相对长度(≈ sealed_length/K ≈ 1GB)**。于是 PS WAL replay(用该值作停止
  边界)只读了 1 个 shard,跨 shard 边界的记录被截断 → WAL-FAILSTOP。merge 的作用只是**把 survivor 的 VP-head replay
  窗口移到了这个 EC log extent**(victim 自己 VP head 在更后的 extent,从不 replay 它),从而触发这条早已存在的透传 bug。
  **数据完好(4 shard 都在),纯恢复读长度错误。**
- **修复:** `read_committed_bytes_from_extent` 的成功分支返回 `(bytes, committed_end)` —— 用本函数算出的权威
  `committed_end`,而非 `read_with_layout` 的 shard 相对 end。`want` 已 clamp 到 `committed_end - offset`,字节本就正确,
  错的只是上报的 end。
- **验收:** stream lib 86 全绿(无回归);**端到端实证**:对真实 wedged 的 part 25 重启,extent 40 replay 读满 3GB
  成功打开、绑定 listener(9302)、`autumn-op info` 4 分区无 warning、`autumn-client ls` 对 part 25 范围
  (`.thumb/320/C*`/`F*`)正常返回(数据零丢失)。
- **passes:** completed (2026-06-16) — 真因(return 值透传)已修 + 端到端救活 part 25。
- **登记 follow-up(deferred,reproduce-first):**
  1. **回归测试:** ✅ **DONE (2026-06-16)** — `client::merge_ec_replay_tests::ec_committed_end_is_sealed_length_not_shard_length`
     (`crates/stream/src/client.rs`)。无需起 manager:`StreamClient::construct`(跳过 acquire_owner_lock)+ 预填
     `nodes_cache`/`extent_info_cache`,起 4 个进程内 EN 持 RS shard(K=3,M=1),走真实 `read_committed_bytes_from_extent`
     全量读,断言 `committed_end == sealed_length`(6144,非 shard 长 ~2050)且 bytes 解码回原 payload。已验证:revert 修复
     (`Ok(r) => return Ok(r)`)后该测试红(committed_end=2050),修复后绿;stream lib 87 全绿。
  2. **merge eversion desync(本次发现但非本 bug 病因,已 revert 不修):** `compute_merge_streams` /
     `splice_streams_without_new_tail` 对每个 victim extent 无条件 `ex.eversion += 1`,只改 manager 不下推 EN
     (实测 extent 40 manager=4 / EN=3)。本次确认它**不导致** WAL-FAILSTOP(读路径 `req.eversion(4) > local(3)` 不触发
     mismatch),危害未证,merge 区 revert-prone → 按 reproduce-first deferred。修法:已 sealed 的 CoW victim extent 只
     refs++、不 bump eversion。
  3. **EC extent manager `sealed` bool:** 本次确认 extent 40 manager 侧 `sealed=true`(我一度误判为 false),不变量正常。

### CLUSTER-DF · 集群容量汇总(Ceph 模型 df)+ extent_rpc 收口 autumn-rpc(2026-06-16)
- **动机:** 三处「空间」展示不准/缺失:(a)fuse statfs 硬编码 1TiB/512GiB 假值;(b)集群级根本无容量汇总接口;
  (c)EC 让「可写逻辑空间」本质是区间(冷 EC 1.25–1.33× / 热 3 副本)。
- **表达模型(用户选 Ceph):** 集群级报 RAW + 经验放大系数(physical_used/logical_stored 摊开自显形),usable 只在
  方案确定的粒度回答,标量接口(statfs)取保守值。
- **架构(用户主导「收口到 EN」):** physical_used 不用放大公式 —— EN 自己是数据 owner,`handle_df` 按 disk_id 求和
  `ExtentEntry.len`(真实文件字节:副本 3× / EC shard / open tail 全含),报进 `DiskStatus.extent_bytes`;manager
  `node_health_loop` 只把各 EN 的 extent_bytes/total/free 累加进内存快照 `cluster_cap`,**零热路径手术、零散落计数器**。
  logical_stored(去放大)是 manager 知识 → 后台 ~30s 只读扫 `Σ 去重 sealed_length`(跳过 refs==0&&vp_table_refs==0);
  只读、不碰任何 mutation 点。`MSG_CLUSTER_DF=0x4D` leader-only 读快照 O(per_node)。区间/系数在 display 端算
  (autumn-op df / fuse statfs),wire 只载裸 u64。正是 Ceph(OSD per-PG)/HDFS(DataNode block report)的 owner 自报 + 控制面求和。
- **Phase 0 前置重构:** `extent_rpc` 从 autumn-stream 搬进 autumn-rpc(与 manager_rpc/partition_rpc 看齐,唯一 wire 家),
  删纯 wire 镜像 `ExtDiskStatus`(无域等价物),`ExtDfResp` 内嵌 canonical `extent_rpc::DiskStatus`;保留 `MgrRecoveryTask`
  域类型(持久化 etcd recoveryTasks/ + F207 ledger,domain/wire 分离不是镜像)。WIRE bump v2→v3(unify)→v4(extent_bytes+RPC)。
- **fuse statfs:** 实调 cluster_df(bounded 2s,失败 fallback),保守 ÷3 映射(CephFS 式把 EC 区间塌成最坏 3 副本 → 绝不乐观
  over-report → 不诱发乐观 ENOSPC)。
- **info open=0:** 经核 `part.is_some() && !probe_set.contains` 按 De Morgan 全局 info(无 --part)本就探测所有 open extent
  → 无 bug,不改(早期 Explore 误读)。
- **验收:** wire 6 / stream lib 82 / manager lib 163 / client 27 全绿;新增集成测试
  `cluster_df_extent_bytes::df_reports_summed_extent_len_per_disk`(EN df extent_bytes = Σ 真实 extent 长,绿)。
  端到端(起集群跑 `autumn-op df` 核对 RAW vs 各 EN statvfs、PHYS_USED vs `du`)待真实集群验证。
- **live e2e (2026-06-16, 隔离 5-EN memory-mode 集群):** RAW==Σ statvfs(逐字节); physical_used==Σ EN 真实字节(对 du);
  3 副本 amp=3.00x → force-ec-convert 转 EC 3+1 后 phys 3.5→1.6MB / amp 3.00x→1.34x(per-node ~400KB=shard_size×4),
  不同配置算出不同正确结果; CoW refs=2 stored 计一次; --json 全字段算术核对; 混合 EC 正确。
- **passes:** completed (2026-06-16, 代码 + 单测/集成 + live e2e 全绿)。

### EC-TRAILER-REMOVE · 删除 EC shard 的 4 字节长度 trailer(2026-06-16, 无部署前提)
- **动机:** `shard_size` 旧公式 `ceil((L+4)/K)` 多留 4 字节,`ec_encode` 把 payload 长度写进最后一个 data shard 末 4 字节,
  `ec_decode` 读它来截断。但这个长度与 manager 权威的 `sealed_length` **完全冗余**(Go reed-solomon 自描述格式的遗留)。
- **改动:** `shard_size = ceil(L/K).max(1)`(去 +4);`ec_encode` 不再写 trailer;`ec_decode(shards,K,M, original_size)`
  新增 `original_size` 参数(由调用方传 `sealed_length`)截断。唯一生产调用方 `ec_read_full` 传 `ex.sealed_length`。
  `ec_subrange_read` / EN crash-detection 等用 `shard_size` 处自动跟新公式(encode 与 read 同源,一致)。
- **格式影响:** 这是**on-disk EC shard 格式变更**(shard 边界移位 + 无 trailer)。安全前提=**当前无任何部署**(无存量 EC extent
  需迁移),用户明确「不需要管兼容」。非 wire schema,WIRE_FINGERPRINT 不变。
- **验收:** erasure 单测 16(含各尺寸 roundtrip + 缺片重建)/ ec_slice 6 / merge_ec_replay 1 / ec_2pc 5 全绿;stream lib 81。
  **live e2e**:300KB value 写入 → seal → force-ec-convert 3+1 → GET 回读 sha256 **MATCH**(经 ec_subrange_read 新无-trailer 格式)。
- **passes:** completed (2026-06-16, 代码 + 单测 + live e2e 全绿)。

### U64-OFFSET-WIDEN · extent 字节偏移 u32→u64 + max_extent_size 16 GiB flag(2026-06-17, 无部署前提)
- **动机:** extent 受 `ReadBytesReq.offset: u32` 限死 4 GiB;`max_extent_size` 写死 3 GiB(lib.rs)。
  拓到 u64 + 提到 16–32 GiB → 单 extent 更大 → extent 数更少 → 大规模下 manager/etcd 元数据压力更小。
  用户明确「现在没有任何部署,不需要管兼容」。
- **改动(编译器驱动级联):**
  - **wire 热路径(extent_rpc 二进制):** AppendReq.commit / AppendResp.offset+end /
    ReadBytesReq.offset+length / ReadBytesResp.end / CommitLengthResp.length 全 u32→u64
    (APPEND_HEADER 28→32, AppendResp 9→17, ReadBytesReq 24→32, ReadBytesResp 5→9, CommitLength 5→9);
    manager 手镜像 `ExtCommitLengthResp.length` 同步(逐字节 wire-compat,EN↔manager)。
  - **持久格式:** `ValuePointer` 16→24B(offset/len u64);`partition_rpc::SstLocation.offset/len` +
    `TableLocations.vp_offset`;SST `MetaBlock.vp_offset`(read_u32→read_u64);`TableMeta.offset/len`;
    `StreamAllocExtentReq.seal_commit: Option<u64>`;`MultiModifySplitReq.{log,row,meta}_stream_sealed_length`;
    `GetRedirectResp.value_offset/value_len`。**WAL 记录 `val_len` 保持 u32**(单值 ≤ 4GB 不变量)。
  - **计数仍 u32:** in_flight / shard 数 / 块内偏移(≤64KB)/ 单值长(≤4GB)。
  - **EC 读分块修复(coco P1 #4):** `read_with_layout` 对 EC extent 直接走 `ec_subrange_read` 绕过
    256MiB 分块;16 GiB extent K=3 → shard ~5.33 GiB > frame `payload_len: u32`(≤4GiB)上限。在唯一收口
    `read_shard_from_addr` 内部按 `read_chunk_bytes`(256MiB)分块,所有 EC 路径自动受界。旧 3GiB 默认
    shard ~1GiB 不触发,16GiB 默认才暴露 → 本次新增能力的真 bug,已修。
  - **max_extent_size flag:** `set_max_extent_size_bytes`(OnceLock 默认 16GiB,clamp [1,64]GiB)+
    `max_extent_size_bytes()`,两处 StreamClient 建点(P-log + P-bulk)都读它;`autumn-ps --max-extent-size-bytes`。
  - **WIRE fingerprint v4→v5**(MIN=MAX=5,pre-R3 同 commit 部署);registry 注册 `5254fafce73f6ffe`。
- **兼容范围:** #1/#2/#3/#7(coco 提的旧持久格式/滚动升级)出范围 —— 用户明确不管兼容 + 全停全启
  rkyv-fail-loud + cluster_version 戳 + WIRE v5 MIN=MAX 强制无 v4/v5 混跑。非渐进迁移。
- **验收:** `cargo build --workspace` 绿;rpc 30 / manager lib 163 / stream + PS 全单测绿。manager 集成
  3 个 recovery 测试并发下偶发失败 = 进程级 e2e 资源竞争,git stash 对照 HEAD baseline **相同**偶发
  (非本次回归;各自单跑全绿)。coco deep 7 项已逐条处置(#4 真 bug 已修,余为兼容/已知区)。
  **live e2e(隔离 memory-mode 4-EN 集群, /tmp, 端口 19xxx, --cpuset 130-135, 不碰 /data02):**
  put-stream 4.5 GiB → 单 log_stream extent 实测 **4,831,907,398 字节 > u32::MAX(4,294,967,295)** →
  get-stream sha256 **MATCH**(后段 chunk VP offset > 4 GiB 走 u64 read);kill -9 PS 重启 → recovery
  重放 SST(paged u64 vp_offset)+ WAL(u64 record offset)→ 再读 **MATCH**;`--max-extent-size-bytes` flag 生效。
- **passes:** completed (2026-06-17, 代码 + 单测 + coco + live e2e 全绿)。

### EC-CHUNKED-CONVERT · 分块 EC encode + 流式 peer-copy(2026-06-17, F193 Stage B 落地)
- **动机:** 16 GiB extent 下 EC convert 两处爆掉:(1)旧 `ec_encode` 整 extent 编码,transient ≈ 2× payload(16G→~32G);
  (2)更致命——whole-shard `WriteShard` payload = shard ≈ sealed/K(16G/K=3 ≈ 5.33G)> frame `payload_len: u32`(≤4G)上限。
  F193 Stage B 当年 "won't-do",16 GiB 默认正是它说的 "real EC-convert OOM signal" 触发条件。
- **改动:**
  - **`erasure::ec_encode_stripe`:** RS over GF(256) 按 offset 逐字节 → 一个 stripe 可独立编码。给 K 个等长 data stripe 切片
    返回 M 个 parity stripe。**字节级等价**于整 extent `ec_encode` 的对应切片(`ec_encode_stripe_matches_whole` 单测证明:
    各尺寸 × 各 stripe 大小,reassemble == whole)→ on-disk shard 布局不变,读路径(ec_subrange_read/ec_read_full)零改。
  - **`handle_convert_to_ec` stripe loop:** 逐 stripe 从本地 `.dat` 读 K 个 data 子区间(尾部零填充)→ 编码 M parity →
    远端 shard 先发、coordinator 自己 shard 0 最后写。峰值 RAM = (K+M)×stripe(默认 64 MiB → ~256 MiB),
    与 extent 大小无关;每个 `WriteShard` ≤ frame 上限 → >4 GiB shard 也能转。
  - **stripe 大小做成 flag(2026-06-17 followup):** `autumn-extent-node --ec-stripe-bytes N`(process-global OnceLock
    setter `set_ec_encode_stripe_bytes`,main 里 first-call-wins;clamp [1 MiB, 1 GiB])。优先级 **flag > 环境变量
    `AUTUMN_EXTENT_EC_STRIPE_BYTES`(测试覆盖)> 64 MiB 默认**。大 stripe = WriteShard/fsync 更少(转换更快)但峰值 RAM 更高;
    无 wire 改动(指纹不变)。live e2e:`--ec-stripe-bytes 16M` → 1 GiB extent 走 ~22 stripe → get-stream sha256 MATCH。
  - **`WriteShardReq.shard_offset: u64`(WIRE v5→v6):** 远端按 offset pwrite 进 `.ec.dat` + per-stripe `sync_data`;
    顺序 await-ack 让 durable 前缀单调增长;coordinator-自己-最后 保留 "coord staging 满 ⇒ 所有 participant 已 durable staged"
    不变量(coordinator_prepared skip + 2PC commit 排序依赖它)。2PC + EC-COMMIT-ATOMIC #5 marker 不变。
  - **流式 peer-copy(`peer_copy_full_extent_to_dat`):** coordinator 本地 short 时,流式拉进**临时文件**,拿到完整
    sealed_length + fsync 后才原子 rename 覆盖 `.dat`(coco P1 — 绝不像 `stream_extent_from_sources` 那样先 set_len(0)
    live replica;那个 dest 是正在重建的节点才行)。删掉整 Vec 缓冲的 `fetch_full_extent_from_sources`。
  - **coco P1 bounds guard:** `write_shard_stripe_local` 拒绝 `shard_offset + stripe_len > sealed_length`(防 malformed/stale
    WriteShard 把 `.ec.dat` 撑成超大 sparse → commit 污染 entry.len/sealed_length)。
- **coco deep:** 2 项 P1 全修(#1 peer-copy 不再毁活副本=改临时文件原子替换;#2 加边界校验)。
- **验收:** `cargo build --workspace` 绿;rpc 30 / stream 全单测(含 erasure 17 / f119-d / f153 / ec_2pc / merge_ec_replay)
  / manager lib 163 全绿;新增 `ec_encode_stripe_matches_whole`(stripe==whole 字节级)。
  **live e2e(隔离 4-EN, `AUTUMN_EXTENT_EC_STRIPE_BYTES=2MiB` 强制多 stripe):** put-stream 1.25 GiB → log_stream extent
  在 1 GiB 滚动 seal → `force-ec-convert` 走**分块** prepare(EN 日志确认 "(chunked)")→ 单 shard 实测 357,919,062 字节
  (≈sealed/3,~170 stripes)→ get-stream 经 ec_subrange_read 回读 sha256 **MATCH**(1,342,177,280 字节);open tail 仍复制态。
- **passes:** completed (2026-06-17, 代码 + 单测 + coco + live e2e 全绿)。

### MERGE-REFS-LEAK · 分区 merge 泄漏 extent refs → 不可见、不可回收的孤儿 extent(2026-06-17, 无部署前提)
- **现场触发:** 排查 "extent 10 一直删不掉、`autumn-op info` 里看不到"。etcd `extents/10`:`refs=2`、
  `vp_table_refs=5`,但扫遍全部 12 个 live stream 的 `extent_ids` **没有任何一个引用 extent 10**。
  `python/audit_extent_refs.py` 进一步发现这是系统性问题:孤儿 extent 10 与 47;extent 12/20/33/46 全部
  `refs` 比真实成员数多 1;stream 29 把 extent 20 列了两次。
- **根因:** `compute_merge_streams` / `splice_streams_without_new_tail`(`manager/src/lib.rs`)对每个 victim extent
  无条件 `ex.refs += 1`(为"拼进 survivor"计数),而 `apply_merge_mutations` 随后整体删除 victim 的 3 个 stream
  (`streams.remove`)**却没有为 victim 的旧成员关系做对应 `refs -= 1`**。净效果:**每次 merge,每个 victim extent
  refs 泄漏 +1**;CoW 共享 extent(split→merge-back)还会在 survivor 的 `extent_ids` 里被列两次。多轮
  split→merge→GC 后,extent 被 GC 从 stream 中 punch 干净、membership 归 0,refs 却卡在正数 → `extent_can_delete`
  (`refs==0 && vp_table_refs==0`)永不成立 → 永久磁盘泄漏;且 `handle_stream_info` 只渲染 stream 成员 extent →
  `info` 完全看不到(EC 3+1 时每节点一 shard,4 份 `.dat` 各 ~sealed/K)。
- **改动:** merge 对 victim extent 的 refs 改为**成员关系中性**:
  - CoW 共享(已在 survivor):`refs -= 1`(两份 {survivor,victim} 合并成一份 {survivor}),且 `extent_ids` **去重不重列**
    (重列会让后续 GC `punch_holes` 的 `retain` 一次删光所有重复项却只减 1 次 refs,永远对不平)。
  - victim 独有:成员关系 victim→survivor **转移**,refs 不变(survivor 拼入 +1 抵消 victim stream 删除 -1)。
  - splice 顺序不变:共享 extent 留在 survivor(前段)位置,保住 vp_head replay 顺序不变量;tail 是 split 后各自独有
    extent,永不在 `survivor_set` 里,故 seal 块与去重不冲突。
- **测试:** 修正 2 个把 bug 当预期的旧单测(`f181_compute_merge_streams_extent_ids_order_and_refs` /
  `f181_splice_streams_without_new_tail_no_e_new`:victim-only extent merge 后只属一个 stream → refs 应为 1,原断言 2);
  新增 2 个 CoW 共享回归(`f_merge_refs_leak_cow_shared_extent_dedup_and_refs` 及 splice 版):断言共享 extent
  refs 2→1、`extent_ids` 不重复、victim 独有 extent refs 不变。
- **可观测性修复(孤儿 extent 在 `autumn-op info` 中不可见):** `handle_stream_info` 原先只遍历
  `s.streams[*].extent_ids`,refs>0 但 0 stream 成员的孤儿(如 extent 10,被 vp_table_refs 正确保留、
  仍在服务读)对 `info` 完全隐身 —— 隐藏了活数据与不可回收泄漏。修复:全量 dump(`stream_ids` 空)时
  追加所有非成员 extent;定向 stream_ids 查询(热路径 client.rs)保持仅成员行为。`autumn-op info`
  现额外打印 `refs=N (streams=M)`、`vp_table_refs`,并对孤儿/泄漏打标签
  (`(ORPHAN: …retained by vp_table_refs — live data)` / `(…vp_table_refs=0 — reclaimable leak)` / `(refs-leak)`)。
- **审计工具:** `python/audit_extent_refs.py`(shell out etcdctl + `autumn-op --json info`,不加新 RPC,符合 ops-in-Python 约定):
  交叉核对 etcd 全量 extent 与 stream 成员关系,用 info 的 vp_table_refs 把孤儿分为 DEAD(可回收泄漏)与
  RETAINED(活数据,正常);报告 DEAD 孤儿 / refs 不匹配 / 单 stream 内重复列举;有问题退出码 1。
- **修复工具:** `python/patch_extent_refs.py`(etcd v3 REST gateway,base64 二进制安全;`ArchivedMgrExtentInfo`
  定长 88B,root=len-88、refs@root+32,校验 extent_id+当前 refs 后原位改标量 + 备份 + readback;默认 dry-run):
  把泄漏的 refs 改回真实 stream 成员数。**必须先停 manager**(否则内存会覆盖 etcd patch),patch 后重启 replay 生效。
- **现网处置(已执行 + 验证):** compact 15/31/42(vp_table_refs(10) 5→3)→ 回收 5 个 DEAD 孤儿(12/20/26/46/47,
  删 etcd + unlink 30 文件)→ 部署可观测性修复(restart-manager)→ patch extent 10 refs 2→0、33 refs 2→1。
  终态审计 exit=0,所有 extent refs == stream 成员数;extent 10 = retained orphan(streams=0, vp_table_refs=3,
  refs=0),info 可见并标注 "live data"。
- **验收:** `cargo build --workspace` 绿;manager lib 165 全单测绿(163 + 2 新增);审计脚本在现网准确复现 2 孤儿 + 4 不匹配 + 1 重复。
  此修复只阻止**新增**泄漏;现网已泄漏的孤儿(extent 10/47)需单独处置(先 major-compact 引用方把 `vp_table_refs` 清 0,
  再由 operator 清 etcd 里泄漏的 `refs`)。
- **passes:** completed (2026-06-17, 代码 + 单测 + 现网审计复现;现网孤儿清理为后续 OP 步骤)。

### VALUE-STREAM · 大 value 从 log_stream 分离(对齐 WAS,DESIGN ONLY,2026-06-17,未实现)
- **触发(Trigger):** MERGE-REFS-LEAK 排查暴露的结构问题 —— log_stream 同时兼任 WAL + 大 value 存储,
  叠加 CoW split 多主,使 GC 结构上无法"拥有"extent 删除(refs/vp_table_refs 双生命周期 + 删除触发只挂 refs 一侧
  → extent 10 类不可见孤儿);且大 value 留在 WAL 撑大 replay(F120/F261 根因)。
- **方向(对齐 Azure WAS):** log_stream 退化为纯 WAL(只存小提交记录,truncate-from-head);大 value 移入独立
  **value_stream**(写一次,WAL 只存 VP,不双写);value extent 唯一生命周期 = vp_table_refs(跨 split 用已有
  partition_vp_refs 聚合);回收折进 compaction(BlobDB 式碎片迁移)+ vp 侧删除触发,**退役独立 GC loop / punch /
  双计数赛跑**。避开"value 内联 SST(写放大)"与"split 拷贝 value(split 变重)"两个已否方案。
- **验收标准(待实现时填):** 写路径不双写大 value;recovery replay 不再流过 value 字节;CoW split 后共享 value
  extent 在两子分区都 compact 后能自动删除(无 extent 10 类孤儿);compaction 折叠迁移按存活率阈值触发且受 admission
  限速;旧 log_stream VP 数据迁移(方案 A 一次性 / B 双读过渡)。
- **设计文档:** `docs/value_stream_design.md`(分阶段:Phase 0 止血 = vp 侧删除触发 + 兜底 sweep,与本重构正交且为终态删除原语;Phase 1 value_stream 写路径;Phase 2 回收折进 compaction;Phase 3 迁移老数据)。
- **passes:** not_completed(DESIGN ONLY;经 2026-06-18 评估**否决落地** —— 见 GC-VP-IDENTITY:
  正确性改走"正确不变量 + 出 bug 修 bug",保留 vp_table_refs(它把 membership-bug 从丢数据降为可恢复孤儿,
  extent 10 为实证),不做 value_stream;replay 膨胀由 F120/F261 已界定,EC 策略分叉另议)。

### GC-VP-IDENTITY · GC 判活按完整 VP identity(修同-extent 多版本静默丢新值,2026-06-18)
- **触发(Trigger):** 评估"删 vp_table_refs、删除门改 refs==0"是否安全时,coco /arch 对抗审计发现的独立 P0
  (与 vp_table_refs 无关)。审计结论:删 vp_table_refs 当前不安全(它依赖的 relocate-then-punch 不变量正被此 P0 破坏);
  且 extent 10(membership-bug 把活数据弄到 refs=0)证明 vp_table_refs 是必需的网 —— 故决定**保留 vp_table_refs**,先修此 P0。
- **根因:** `run_gc`/`process_gc_chunk` 判活只比 `vp.extent_id`,relocate 的是被扫描 record 的 value。同一 key 在同一
  sealed log extent 内有新旧两个大 value(A 旧 off_a / B 新 off_b)时:GC 先扫 A → 当前 live=B、extent_id 匹配 →
  误判 A live → 以新 seq relocate A 的旧值;A ≥ GC batch cap(或 record_cap=1)→ flush 把 relocate-A 写进 memtable
  (在扫 B 前)→ 扫 B 时 live 已是 relocate-A(指新 extent)→ skip B → punch X → 新值 B 丢失、旧值 A 复活。
  静默 lost-update,热路径(大 value overwrite),8MB 同 extent 可靠复现。
- **改动:**
  - `wal_record.rs`:`value_offset_in_record(first_byte, key_len)` 作 value 在 record 内偏移的**单一真源**
    (V1=22+key_len / V0=17+key_len);`DecodedRecord` 增 `val_off`,`decode_one` 两分支用 helper 填。
  - `background.rs`:`process_gc_chunk` 增参 `buf_base_offset`,判活改**完整 VP identity**
    (`extent_id && offset==buf_base+record_start+val_off && len==val_len` 才 relocate);`run_gc` 计算
    `buf_base_offset = cur - carry.len()` 并传入。
  - `lib.rs` recovery:重建 VP.offset 改用 `value_offset_in_record`(原硬编码 V1 `+22`,对 V0 legacy 错 5 字节,
    与新 GC identity 不一致会漏 relocate V0 live → 丢数据;coco P1)。
- **测试:**
  - reproduce-first 集成测试 `crates/manager/tests/system_gc_multiversion_same_extent.rs`:同 key 两大值同 extent
    → split 封 X → 小写滚新 tail → compact → GC;**修前 FAIL(读回旧值 'A')、修后 PASS('B')**;并断言 GC 实际
    punch 了 X(refs<2,coco P2 防假阴性)。
  - `wal_record` 单测:`value_offset_matches_envelope` + round-trip `val_off` 定位断言(V0/V1)。
- **验收:** PS lib 167 单测绿;`system_gc_chunked_read` / `system_vp_after_split_gc` 回归绿(genuinely-live 值仍正确
  relocate,无 over-skip);reproduce-first 修前红/修后绿;coco /findbugs 复审 2 findings 均已处理。
- **passes:** completed(2026-06-18,代码 + reproduce-first 测试 + 回归 + coco 复审)。

### EXTENT10-AUTORECLAIM · both-zero 孤儿自动回收(终结 extent-10 类不可回收孤儿,2026-06-18)
- **触发:** extent 10 残留问题 —— 一个 `refs==0 && vp_table_refs==0` 但**不在任何 stream** 的 extent
  永远不会被自动删除:refs 侧触发器在 `punch_holes`/`truncate`(只作用于 stream 成员),vp 侧
  `sync_partition_vp_refs` 只递减 `vp_table_refs` 从不触发删除。当最后归 0 的是 vp_table_refs
  (GC 已把它 punch 出所有 log_stream 后,compaction 丢掉最后一个 SST VP),无人删 → 磁盘永久泄漏,
  靠手动 python/reclaim。
- **改动:** `extent_delete.rs` 新增 leader-only `extent_both_zero_sweep_once` / `_loop`:扫 `s.extents`,
  候选 = `extent_can_delete`(both-zero)且 **不在任何 stream 的 extent_ids**(coco P1 #2:不靠 refs==0 当
  "不在 stream" 的代理;refs 欠计仍在 stream 则 log error 跳过)且不在 F207 ledger / delete_progress;
  **value-CAS** 删 `extents/<id>`(snapshot 基线,coco P1:挡并发 sync bump vp_table_refs 0→1)→ 内存移除 →
  `enqueue_pending_deletes`(获 F207 Delete marker;`extent_delete_loop` 发物理 unlink)。
  `start_runtime_tasks` spawn_supervised 注册(60s tick)。
- **安全性:** both-zero 删除健全 —— `refs==0`(且经 #2 确认确实不在 stream)⇒ relocate-then-punch
  (经 GC-VP-IDENTITY 修正)已搬走 live 值 ⇒ 无 live VP;`vp_table_refs==0` ⇒ 无活 SST 物理引用。
  无 bug 世界里 refs==0 已足够,both-zero 门把 vp_table_refs 当 membership-bug 的网保留;经 `extent_can_delete`,
  将来删 vp_table_refs 此门自动退化 refs==0,sweep 不变。
- **接受的残留(coco,与现有 extent-state 路径同一档):**
  - #1 resurrection:sweep CAS 挡住危险方向(sync 先提交 → CAS 失败 → 跳过);反向(我删后 in-flight sync
    经未 CAS 的 `mirror_partition_vp_refs` blind-put 复活 etcd 记录)需 DEFERRED 的 note-33 extent-state-CAS
    (revert-prone、未复现);在 #2 + relocate-then-punch 下该残留 **非丢数据**(不在 stream 的 extent 其 SST 引用是死的),
    下次 compaction/sweep 自愈。
  - #3 atomicity:`extents/<id>` 删除与 Delete marker 非同一 txn(同 `punch_holes`);崩溃窗由 F109 node-startup
    reconcile 兜底 —— 空间泄漏、非丢数据。
- **测试:** manager lib `extent10_both_zero_orphan_is_auto_reclaimed_referenced_kept`:both-zero 非成员孤儿被回收
  (移除 + 入 delete_progress);`vp_table_refs>0`、`refs>0`、以及 **both-zero 但仍在 stream**(#2)的 extent 均保留。
- **验收:** manager lib 166 单测绿;cargo build --workspace(除 fuse)绿;coco /findbugs 三轮(P1 CAS / P1 #2 已修;
  P1 #1、P2 #3 为文档化接受残留)。注:端到端(GC→compact→retained-orphan→自动删)后续强化
  `vp_freed_after_both_children_compact_and_gc` 集成测试覆盖(慢)。
- **passes:** completed(2026-06-18,代码 + reproduce-first 单测 + 回归 + coco 复审)。

### MERGE-REFS-RECOMPUTE · merge refs 改"从成员关系重算"(消 saturating_sub 脆性)
- **触发:** coco /findbugs P1 + GC-VP-IDENTITY 审计 —— merge 的 `refs.saturating_sub(1)` 在 refs 已欠计时静默到 0,
  理论上若 refs 欠计则物理删除风险。
- **方向(若做):** `compute_merge_streams` / `splice_streams_without_new_tail` 的 refs 从结果成员关系**重算**(而非
  增量 saturating_sub);recovery 后对 `refs == count(stream.extent_ids 含 eid)` 做一次校验。
- **复现尝试(2026-06-18,reproduce-first):** `merge_refs_invariant_holds_across_split_merge_cycles`(manager lib)
  用真实 compute+apply fns 跑 5 轮 2-way split→merge-back + 一次 3-way split→merge,每步断言
  `refs == #streams listing extent`。**全程不变量成立 → saturating_sub 在真实操作下从不在欠计值上触发 → 复现失败**。
  且其后果(refs 欠计 → 误删)已被 **EXTENT10 #2 成员检查**(both-zero 但仍在 stream → ERROR 跳过,extent 13 单测)
  + punch 的成员感知删除 网住。
- **结论:DEFERRED(防御性,非复现真害)。** 按 memory(reproduce-first / 不凭 coco-arch 在 revert-prone merge 区动刀)
  缓做;真复现出 saturating_sub 引发实害再落。复现尝试测试保留作 refs/membership 漂移的**永久回归守卫**。
- **passes:** not_completed(DEFERRED;复现失败 + 后果已网住;回归守卫已加)。

### VP-TABLE-REFS-REMOVAL · 删除 vp_table_refs 双计数,extent 留存只看 refs(2026-06-18)
- **决策来源:** 用户明确指示"Vp table refs 删除就行,简化代码" + 选项 B(全删 + 迁移)。推翻 2026-06-18 早先
  "保留 vp_table_refs 当网"的结论 —— 改走 memory「0 bug:出 bug 修 bug,不靠 vp_table_refs 网住」+
  「Client-side complexity first / server complexity compounds」:GC-VP-IDENTITY 已修好 relocate-then-punch
  不变量(`refs==0 ⇒ 无 live VP`),merge refs 会计正确(MERGE-REFS-LEAK 修 + RECOMPUTE 复现失败),
  故 `refs`(stream 成员)单计数已足够,第二计数纯属冗余复杂度。
- **目标/边界:** 删 vp_table_refs 全部机制;终态 `extent_can_delete` 退化为 `refs == 0`;保持全停全启升级安全
  (绝不 reset)。分两阶段:**Stage 1 删机制 + 不破坏持久格式 + 删除门保留 `&& vp_table_refs==0` 作升级守卫**;
  Stage 2 迁移后才把门塌缩到 `refs==0` 并删字段。
- **coco P0 修正(关键):** 初版 Stage 1 把删除门直接塌成 `refs==0`,coco /findbugs(GPT-5.5)指出这是**升级丢数据**:
  全停全启在既有 etcd 上重启,旧版集群可能遗留 `refs==0 && vp_table_refs>0 && 不在任何 stream` 的 retained-by-VP
  extent(老 net 合法保留的活数据,如 extent 10,本仓 MERGE-REFS-LEAK 实证存在过);`refs==0 ⇒ 无 live VP` 只对
  **post-GC-VP-IDENTITY 路径达成 refs==0 的 extent**成立,不覆盖老 buggy GC 冻结在 etcd 的状态。违反 memory#1
  「全停全启安全:持久态改动要么 rkyv 同布局要么带迁移」。修正:Stage 1 只删机制、删除门保留 `&& vp_table_refs==0`
  作冻结升级守卫(无 maintainer → 新管 extent 恒 0 → 等价 refs==0;老 vp>0 extent 不删,空间泄漏非丢数据,Stage 2 迁移清)。
- **Stage 1(本次,无格式破坏):**
  - 删(机制):`MSG_SYNC_PARTITION_VP_REFS`(0x33)/`MSG_PULL_VP_REFS`(0x4F) + req/resp(`SyncPartitionVpRefs*`/
    `PullVpRefs*`/`MgrPartitionVpRefs`);manager `partition_vp_refs` state + `partitionVpRefs/` etcd 加载;
    维护 fns(`preview/apply/merged/split_snapshot/merge_extent_updates/mirror_partition_vp_refs/
    partition_vp_ref_deltas/vp_refs_to_map`);`handle_sync_partition_vp_refs` + `pull_and_apply_vp_refs` +
    split/merge 的 pull;PS `vp_refs_dirty` + `vp_refs_retry_loop` + GC dirty-gate + `sync_partition_vp_refs*` +
    `collect_partition_vp_refs` + `handle_pull_vp_refs`;PS `PartitionData.manager_addr/pool`(随之死代码)。WIRE bump v6→v7
    (MIN=MAX=7,registry 记 `4263787e9cedcca8`)。
  - **不改删除语义(升级守卫):**`extent_can_delete` 保持 `refs==0 && vp_table_refs==0`;punch/truncate 门、recovery
    logical_stored、EXTENT10 sweep、autumn-op 展示/分类**全部不动**(字段变冻结只读,语义对既有数据零变化)。
  - 保留(Stage 2 再删):`MgrExtentInfo.vp_table_refs` 字段(etcd `extents/<id>` rkyv 布局不变,仍被 `extent_can_delete` 读)、
    SST `MetaBlock.vp_deps`(+`SstReader.vp_deps`,`#[allow(dead_code)]`)。
- **验收标准:** (1) 全 workspace + tests 编译绿;(2) rpc/common/PS/manager 单测绿(EXTENT10 测试维持原断言:both-zero
  非成员孤儿回收、vp_table_refs>0 / refs>0 / 仍在 stream 者保留);(3) **chaos/e2e 验证(机制删除后)删除不丢数据** ——
  `system_vp_after_split_gc`(split+一子 GC,两子仍解析 VP)、`system_split_ref_counting`(共享 extent 两子 GC 后
  refs→0 freed 且数据正确)、`system_gc_multiversion_same_extent`、`system_merge` round-trip/zero-loss 全绿。
  注:`split_merge_split_with_concurrent_writes`(#[ignore] long-running)本分支与 baseline 均 ~7min 不完成
  [EXIT 124],reproduce-first 确认 PRE-EXISTING、非本次回归(同 split/merge 机制已由 f185 + system_split_* 覆盖)。
- **Stage 2(未做):** 迁移(PS 重扫 SST vp_deps / 强制 major-compact 确认无 live VP → 清 `vp_table_refs`)后,
  `extent_can_delete` 塌缩 `refs==0`、版本化解码物理删 `MgrExtentInfo.vp_table_refs` + SST `MetaBlock.vp_deps`
  (MetaBlock 有 VERSION → dual-decode)+ 清残留 `partitionVpRefs/` etcd key。
- **passes:** completed(Stage 1,含 coco P0 修正;代码 + 单测 + e2e 验证 + 文档)。Stage 2 = not_completed(待排期)。

## BUG-GC-STALE-CACHE — GC 不得基于陈旧 extent_info 做破坏性 punch(2026-06-18,chaos seed=583)
- **目标/边界:** 修复 chaos 复现的真实 data-loss:GC 把一个**已封存满载 live VP** 的 extent 当成空 extent 删掉。
- **根因:** GC 的 F201 fast-punch(`sealed_length==0` → punch)读 `StreamClient::get_extent_info`(带缓存)。
  `alloc_new_extent` 封旧 tail 后只缓存新 tail、**不 evict 旧 tail** → 旧 tail 残留 pre-seal OPEN 快照
  (`sealed=false, sealed_length=0`)→ GC 信陈旧 0 → punch 已封 extent → refs→0 删除 → VP 读 NotFound。
  seed 583:extent 12 已封 7.8MB,GC 读陈旧 0 删之,q000056 丢。
- **修法(保守跳过,no-refetch):** `authoritative_sealed_length` helper(Auto + Force GC 共用,validated
  `(eid, sealed_length)` 带到 run_gc,无 check/use 再读分裂):`sealed` 置位后不可变 ⇒ 缓存 `sealed=true` 可信;
  读到 `!sealed` 一律 **SKIP**(`None`)——绝不删非权威已封 extent(OPEN extent 永远 `sealed_length=0` 但非空)。
  **刻意不 invalidate+refetch:** 每候选多一次 GC→manager RPC 会移时序、(seed=603)照出另一个老 split-child-open
  wedge。不用 vp_deps(尊重"vp_deps 会永久阻断 GC")。
- **验收标准:** (1) 编译 + PS 167 单测绿;(2) seed 583 修复前必丢、修复后 ×多次全过;(3) 不回归:
  no-refetch 版本 11 seed(58/581/582/13/600/601/602/604/605/583/603)加压 chaos 全绿;(4) coco 复审。
- **已知限制(deferred,与 seed=603 wedge 配对):** 真已封但卡在 stale-cached-as-open 的 extent 会被跳过、
  直到缓存刷新(读/EC-invalidate/重启)才回收 —— GC 回收不及时(coco P1),**非丢数据**。干净回收修法
  (封存时更新旧 tail 缓存为 `sealed=true,len=seal_commit`,GC 见新态且零新增 RPC)待 seed=603 wedge 根因后再上。
- **passes:** completed(no-refetch 保守跳过;代码 + 单测 + 11-seed 加压 chaos + coco 复审;用户批准"先保守跳过")。
  GC-回收-leak 修法 + seed=603 split-child-open wedge = not_completed(后续两件)。

## BUG-IDEMPOTENT-ROLL — seal-and-roll 重试幂等(2026-06-18,chaos seed=603,WIRE v8)
- **目标/边界:** 修复 chaos 复现的 split-child-open wedge:split 子分区永远起不来 → key 不可达
  (mismatches=0;数据没物理丢,在 CoW 共享 stream;是 seal 长度虚高)。
- **根因:** `alloc_new_extent`(封存+滚动)重试不幂等。写入端为 tail T 抓 `seal_commit`(SealCommit 握手),
  调 `alloc_new_extent(Some(commit))` 封 T + 滚新 tail T'。若该 alloc 在 manager 成功(封 T、滚 T')但响应丢了
  (chaos 延迟),写入端用同一 `seal_commit` 重试 → manager 当前 tail 已是全新 OPEN 的 T' → 把 T' 按 stale commit
  封过头 → T' 永不可恢复 → 任何回放它的分区(CoW 共享 log stream 的 split 子)撞 WAL-FAILSTOP 永不 open。
- **修法(把封存绑定到具体 extent,幂等):** SealCommit 握手返回 `(commit, tail_extent_id)`;
  `seal_commit_watermark → alloc_new_extent(stream, Some(commit), seal_extent_id)`(`Some(result.end)` 预滚传
  `result.extent_id`;probe/`None` 传 0)。manager **仅当当前 tail == `seal_extent_id` 且 OPEN 才封**;否则
  (重试/已滚走)幂等空操作返回当前 tail。**`!tail.sealed` 闸(coco P1):** 当前 tail 已封则 fall through 到
  `already_sealed`(保 seal + 分配新 open tail),不把已封 extent 当 fresh tail 返回。不动 F227 lenient-seal 语义。
  WIRE bump v8(`StreamAllocExtentReq.seal_extent_id: u64`,同版本部署)。
- **验收标准:** (1) 编译 + stream 82 / PS 167 / rpc 30 单测绿 + WIRE registry test 绿;(2) 修复前(带 refetch
  触发器)seed 603 3/3 挂;修复后 seed 603 ×5 + 回归(583/58/581/600/601/602/13)= 0 挂(14 次加压 chaos);
  (3) coco 复审(P1 已修 + 重验)。
- **passes:** completed(WIRE v8;代码 + 单测 + 14-run 加压 chaos + coco;用户已看 diff 批准提交)。

## GC-RECLAIM-EVICT — close the no-refetch reclamation leak (2026-06-19,coco P1 from BUG-GC-STALE-CACHE)
- **目标/边界:** 关闭 BUG-GC-STALE-CACHE 的 no-refetch 保守跳过留下的 GC 回收 leak(coco P1):真已封但卡在
  stale-cached-as-open 的 extent 永远不被 GC 回收(浪费空间,非丢数据)。BUG#2 已修 → 此项解锁。
- **修法(零合成,evict):** `StreamClient::alloc_new_extent`(seal-and-roll)在 authoritative 路径
  (`seal_commit=Some(_)` 且 `seal_extent_id != 0`)封旧 tail 后,**evict 旧 tail 的 extent_info_cache**,
  让 GC 下次 get_extent_info 从 manager 拉权威 sealed 状态 → 正常回收。**不 synthesize `sealed_length=seal_commit`**
  (coco P1 丢数据:manager already_sealed 分支忽略 req.seal_commit、保留已有 L≥acked≥seal_commit;本地写 c<L 会让 GC
  只搬前 c 字节却 punch 整个 extent → 丢 [c,L))。多出的 cache-miss RPC 现在安全(它本会照出的 seed=603 wedge 已由
  BUG2-IDEMPOTENT-ROLL 修掉)。no-refetch 跳过保留作其它 stale-open 来源(probe/split/merge)的数据安全兜底。
- **验收标准:** (1) 编译 + stream 82 单测;(2) 不回归 BUG#1:seed 583 ×3 加压 chaos 全过(GC 见正确 7.8MB 而非陈旧 0
  → 不误删 live VP);(3) seed 603 ×2 + 回归(760-763)= 0 挂(共 9 次);(4) coco "未发现问题"。
- **passes:** completed(evict 版;代码 + 单测 + 9-run 加压 chaos + coco 两轮[v1 synthesize 被 coco P1 否,改 evict]
  ;用户曾提议做此项)。

## CHAOS-ACCOUNTING-CHECKER — storage-accounting invariants in system_chaos (2026-06-19)
- **目标/边界:** system_chaos 此前只校验用户数据(per-key/range/write-liveness),不校验 storage 会计。
  新增收尾断言:每 extent `refs == 列它的 stream 数`、`vp_table_refs==0`、无 dangling membership(stream 列了
  无 ExtentInfo 的 extent)。一次抓 extent-10 orphan 类 + CoW double-free + GC leak 回归。纯 test-harness
  改动(无生产代码),直接读 manager etcd(extents//streams/)为真相源,不加新 RPC。来源:用户提议
  extent-10 场景 + coco arch gap 分析(T1a 最高杠杆项)。
- **设计:** verify 阶段调 `verify_extent_accounting(&etcd_endpoint)`,折进既有 panic 条件。读用
  **revision-pinned 一致快照**(读 extents/ 取 header.revision,再按同 revision 读 streams/;否则两次独立
  latest-revision Range 跨 commit 会拼出 phantom 状态误报 — coco P2)。convergence loop(6×2s)兜底任何残留瞬态。
  非空快照守卫:clean 但 extents/memberships=0 视为失败(测试必有 log/row/meta 流),防 vacuous pass(coco P2)。
  比较逻辑抽成纯函数 `check_accounting_invariants` + 6 个单测(clean 含 CoW refs=2+pending-delete;
  orphan/under/over/vp_table_refs/dangling 各触发)。
- **验收标准:** (1) 编译 + 6 单测绿(证明 checker 对每类违规会 fire,非空跑);(2) e2e:多 seed + EC 几何 +
  深 split/merge/gc/ec churn,accounting errors=0 且 checked N extents>0 / M memberships>N(证明在校验 CoW 共享
  extent,refs≥2);(3) coco findbugs(2 个 P2 已修:一致快照 + 非空守卫)。
- **passes:** completed(代码 + 6 单测 + e2e 多 seed/几何/深 churn 全绿 + coco 2 轮[P2×2 已修];用户选定此方向)。

## BUG2-EC-APPLY-FAIL — EC convert apply 失败被吞 → 半永久 wedge (2026-06-20, coco arch 发现 + 复现)
- **目标/边界:** 修 coco arch gap 分析核实的真 bug:EC convert dispatch loop `let _ = apply_ec_conversion_done(...)`
  吞错 + 无条件 release marker。
- **根因:** `recovery.rs:1300` RPC OK 后 `let _ = apply(...).await`(吞错)再 `if rpc_ok { commit_extent_inflight_release }`
  (无条件)。若 apply 的 etcd txn 失败但**未丢 leadership**(etcd 瞬断/非-fence 错),etcd 留 marker + pre-EC layout,
  本 leader 却清了内存 marker;ec_conversion_dispatch_loop 是 drain-only(F203 枚举内存 shadow 非重扫 etcd)→ 永不重派
  → 该 extent manager-pre-EC / EN-post-EC,读永远 EVERSION_MISMATCH 直到 leader failover 重放 etcd marker。
- **修法:** 抽出 `finalize_ec_dispatch_after_convert`,**仅 apply 返回 Ok 才 release marker**;Err(含 NotLeader/瞬断/
  NotFound)保留 marker → 下 tick 重派(F119-D/F153 幂等使重发为 no-op)。体现 [[feedback_no_let_underscore_swallow]]。
- **验收标准:** (1) reproduce-first:对抽出方法写单测,先用旧 buggy 体跑出 RED(apply 失败 marker 被误清),再换 fixed 体 GREEN;
  (2) 164 lib 单测 0 回归;(3) EC-heavy chaos(ec,split,merge,gc,kill,fence)≥2 seed 全过 + accounting errors=0;(4) coco findbugs。
- **passes:** completed(代码 + 2 单测[fail-keeps / success-releases,red→green 已验] + 164 lib + 2 EC-heavy chaos + coco "未发现问题")。
