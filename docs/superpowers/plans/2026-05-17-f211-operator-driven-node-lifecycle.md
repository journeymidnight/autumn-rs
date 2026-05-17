# Operator-Driven Node Lifecycle — autumn-rs

## Context

### 起因
EC convert flow 当 coordinator EN 永久死亡时 marker 卡在 etcd，导致源 extent 退化但 recovery 被 F138 互斥锁死，EC convert 永远完不成，需运维 `etcdctl del` 手工介入。

### 深层问题
这是 autumn-rs 整体节点死亡判定模型的问题，不只 EC convert：

- 现在 manager 自动判定: 心跳 10s 超时 → 当死处理 → 立即 dispatch recovery（2s tick）
- 一次网络抖动就触发跨节点重建，partition 期间扰动放大
- 同时 EC convert 这个 case 又过于保守（F209-C WARN-only，怕错判），导致死局

### 设计原则（采纳用户思路）

**Manager 提供事实，OP policy script 做判断。**

- Manager 自动跟踪 `Online ↔ Suspected`（基于心跳），**不**自动进 Down/Fenced
- Manager 暴露事实给 OP：哪些节点 suspected、每个 extent 副本健康度、哪些 EC convert marker 关联了 suspected coord
- OP policy script 周期性读取事实 + 综合外部证据（ssh / k8s / 监控）做决定
- OP 决定后通过 admin RPC 触发"动作"（fence 节点、abandon EC marker、re-issue convert）
- Manager 只在收到 OP 的 `Fenced` 信号后才进自动 cleanup（recovery / EC abandon）
- **EC convert reissue 不自动**——由 OP policy script 自己发现后调 `force_ec_convert`

### 类比业界

最接近 **HDFS decommission 模式**：
- 自动检测 stale datanode（soft）→ 停止路由读
- 完全清理需运维显式 decommission → 才 replicate blocks

也类似 Ceph 的 `noout` + 手动 `osd out`：自动不做，等运维确认。

---

## 设计

### Manager 内部状态

#### 1. NodeStateTracker（自动，纯内存）
```rust
enum NodeAutoState {
    Online,
    Suspected { since: Instant },
}
```
- `Online → Suspected`：心跳/df 失败超过 `soft_timeout`（默认 10s，跟现状一致）
- `Suspected → Online`：心跳恢复
- **没有 Down 自动转换**

#### 2. NodeFenceFlag（OP 手动，etcd 持久化）
```rust
enum NodeOverride {
    Fenced { set_at: u64, set_by: String, reason: String },
    Maintenance { set_at: u64, set_by: String, reason: String },
}
```
- Etcd prefix: `node_override/{node_id}`
- 只能通过 admin RPC 设置/清除
- `Fenced`：触发 recovery + EC abandon
- `Maintenance`：节点暂时不可用但不要重建（运维知道它会回来）

### 节点视角下的副本健康

每个 extent 的 replica slot 视角：
- Slot 在 `Online` 节点：参与读写
- Slot 在 `Suspected` 节点：让开路由，但**不**重建（等 OP 决定）
- Slot 在 `Fenced` 节点：触发自动 recovery + 计入 "lost replica"
- Slot 在 `Maintenance` 节点：让开路由，**不**重建

### 触发链（OP-driven）

```
节点 N 心跳失败 ≥ 10s
   ↓
[自动] manager: N 状态 Online → Suspected
   ↓
[自动] manager: 暴露事实 (list_suspected_nodes / extent_health_report)
   ↓
[人类/外部脚本] OP Policy Script 周期跑:
   - 拉 list_suspected_nodes (manager view 哪些 suspected、多久了)
   - 拉 extent_health_report (哪些 extent 副本不全 / 哪些 marker coord 是 suspected)
   - 综合外部证据 (ssh / k8s / 监控) 判断
   - 如确认 N 死: 调 admin RPC mgr_fence_node(N, reason)
   ↓
[自动, 由 fence 触发] manager:
   1. 写 node_override/{N} = Fenced 到 etcd (持久化)
   2. 内存 NodeStateTracker 应用 fence 状态
   3. 对 N 持有 ownership 的所有 extent: bump owner-lock revision (fencing)
   4. 扫描 inflight markers 中 target_nodes[0] == N 的:
        原子 etcd txn:
          - delete extent_inflight/{id}
          - put ec_convert_advisory/{id} (auto-abandoned, 审计留痕)
          - 不再 dispatch (marker 没了 + node fenced)
   5. recovery_dispatch_loop 看到 avali slot 在 fenced 节点 → 触发副本重建
   ↓
[自动] recovery 跑完源副本回到 3R
   ↓
[人类/外部脚本] OP Policy Script:
   - 周期扫描 ec_convert_advisory + extent 状态
   - 看到 advisory 对应的 extent 已 3R 健康 → 调 force_ec_convert(extent_id)
   - manager 用全新 new_eversion + 全新 target_nodes 重发
   ↓
EC convert 完成, advisory 清除
```

### Fencing 机制（复用 owner-lock revision）

- autumn-rs 已有 owner-lock revision 协议（CLAUDE.md "Owner Lock Fencing" 段）
- append 操作已检 `header.revision >= last_revision`
- **fence_node 时 manager bump 该节点持有 ownership 的所有 extent 的 revision**
- 假死 coord 复活后所有写带旧 revision → ExtentNode 拒绝
- **新增**：shard write/commit handler 加 revision fence（当前缺，append 已有）

### 两种 EC 失败场景

本 plan 覆盖**两种独立**的 EC 故障场景，两者走**同一套** F211 机制但路径不同：

#### 场景 1：EC convert 进行中 coord 死
- 状态：`extent_inflight/{id}` marker 在 etcd 中
- F138 互斥阻塞 recovery
- fence_node 触发 `ec_abandon` 路径 (F211-F)：删 marker + 写 advisory → F138 解锁 → recovery 接管
- 重发 EC：OP script 周期扫 advisory 后调 `force_ec_convert`

#### 场景 2：**Sealed EC 副本 down** (用户提到的 case)
- 状态：extent 已 EC 完成，无 marker，K+M shards 分布在 K+M 节点
- 某个 shard 持有节点 NX 死掉 → extent.avali 位图显示 slot[i] = 0
- 没有 F138 互斥（无 marker）
- 流程：

```
NX 死 → NodeStateTracker: Online → Suspected (自动)
   ↓
OP policy script: extent_health_report 看到 extent E.slot[i] 在 Suspected 节点
   ↓
OP 确认死透后调 mgr_fence_node(NX)
   ↓
[自动, F211-D]: 对 NX 持有 slot 的所有 extent (包括 sealed EC) bump owner-lock revision
   ↓
[自动, F211-F 的扫描器]: 没找到关联 inflight markers (sealed extent 没 marker)，跳过 abandon
   ↓
[自动, F211-E]: recovery_dispatch_loop 看到 slot[i] 节点 Fenced
   → 走现有 EC recovery 路径 (run_ec_recovery_payload)
   → 候选 EN NY 从 K 个健康 shards RS-decode → 重建 slot[i]
   ↓
NY 写完 → manager apply_recovery_done 更新 extent.replicates/parity 列表
   ↓
如 NX 复活: 任何写带旧 revision → ExtentNode 拒绝 (F211-D fence)
            读请求由 manager 重定向到 NY (extent meta 已更新)
```

**关键点**：
- F211 复用 recovery loop 同时处理 sealed EC 和 EC convert in-flight 两种 case
- 不需要额外 RPC 或新逻辑——已现有 EC recovery 路径 `run_ec_recovery_payload`
- Fence revision bump 顺带保护读路径未来加 fence（本 plan 范围内仅做 write/commit fence；read fence 标记为后续 feature）
- 极端情况（同时 > M 个 slot 故障）超出 EC 容错能力，manager 报错，需运维补救

**与 EC convert in-flight 的区别**：
| 维度 | EC convert 中途 | Sealed EC 副本 down |
|---|---|---|
| marker | 存在 | 不存在 |
| F138 互斥 | 阻塞 recovery | 不影响 |
| OP fence 后 cleanup | delete marker + bump revision + recovery + 等 OP reissue | bump revision + recovery (单走 recovery loop) |
| 重新跑 EC 编码 | 是（重发 force_ec_convert） | 否（只补单 shard） |

---

## 子 Feature 拆分

按 CLAUDE.md "每个 feature 必须按固定流程推进" 规则。

### F211-A：NodeStateTracker (Online/Suspected only, 自动)
**改动**：
- 新文件 `crates/manager/src/node_state.rs`
  - `enum NodeAutoState { Online, Suspected { since: Instant } }`
  - `struct NodeStateTracker { auto_states, ... }`
  - `on_heartbeat_ok(node_id)` / `on_heartbeat_fail(node_id)` / `on_df_*(node_id)` / `tick()`
  - **故意没有自动 Down/Fenced 转换**
- `lib.rs:1305-1350` PS liveness 接入（保留现有 evict，并存观察一段时间）
- `recovery.rs:571-641` `disk_status_update_loop` 喂 df 结果给 tracker

**验收**：
- 单元测试：Online ↔ Suspected 8 种转换分支
- 集成测试：模拟 EN 不响应 10s → Suspected；30 min 后仍 Suspected（不自动 Down）

### F211-B：Health reporting RPCs（manager 暴露事实，只读）
**改动**：
- 3 个新 RPC：
  - `mgr_list_node_states() -> Vec<NodeStateEntry>` 含 auto state + override + last_heartbeat_secs_ago
  - `mgr_extent_health_report(filter?) -> Vec<ExtentHealth>` 每个 extent 的 slot 健康
    - 不带 filter 返回所有 unhealthy 的（avali 不满 OR slot 在 Suspected/Fenced/Maintenance 节点）
    - 可按 node_id filter
  - `mgr_list_ec_inflight_markers() -> Vec<InflightWithCoordState>` marker 列表 + coord 当前节点状态
- `crates/rpc/src/manager_rpc.rs`：新 MSG_ 常量 + rkyv Req/Resp 结构

**验收**：
- 单元测试：聚合逻辑
- 集成测试：模拟一个节点 Suspected，调 RPC 看返回 last_heartbeat_secs_ago 准确

### F211-C：Operator fence / maintenance / remove admin RPCs (etcd-persisted)
**改动**：
- 新 etcd prefix `node_override/{node_id}` → rkyv `MgrNodeOverride { kind, set_at, set_by, reason, expire_at?: Option<u64> }`
  - `expire_at` 可选字段用于 Maintenance TTL（#6 漏洞补丁）
- 4 个 admin RPC：
  - `mgr_fence_node(node_id, reason) -> CodeResp`（含容量预检，见下）
  - `mgr_set_node_maintenance(node_id, reason, expire_at?: u64) -> CodeResp`
  - `mgr_clear_node_override(node_id) -> CodeResp`
  - `mgr_remove_node(node_id) -> CodeResp`（hard delete + 写入 decommissioned history，见下）
- `NodeStateTracker` 读 override 优先合并显示
- F149 leader failover 时从 etcd replay overrides
- `mgr_fence_node` 触发 hook：F211-D + F211-F 内部逻辑（fence 是 cleanup 的同步点）

`mgr_fence_node` 的容量预检（#5 漏洞补丁）：
1. F149 leader fence
2. 计算节点持有的总数据量（遍历 extents 含此 node_id 的 slot）
3. 统计剩余可用节点的空闲容量（df 上报或 manager 维护的 used/total）
4. 如剩余容量 < 待迁移数据 × 安全系数（默认 1.2）→ 返回 `CODE_PRECONDITION_FAILED` 附详情
5. 运维可加 `--force` 跳过此检查（紧急场景，已知风险）

`mgr_remove_node` 的安全前置检查（全部通过才执行）：
1. F149 leader fence
2. 节点必须当前为 `Fenced` 状态（不允许 Online / Suspected / Maintenance 直接 remove）
   - 强制走 fence → 等 recovery → remove 三步流程，避免误删活节点
3. 扫所有 extent metadata：**无任何 extent 在 replicates / parity 列表里仍引用此 node_id**
   - 包括: 3R extent / sealed EC extent / 正在 dispatch 中的 recovery target
   - 任一引用 → 返回 `CODE_PRECONDITION_FAILED` 附详细 extent_id 列表
4. 扫所有 inflight markers：**无任何 marker.target_nodes 含此 node_id**
   - 即使是非 coord 位置也不允许 remove（怕 advisory 重发 EC convert 时引用幽灵节点）
5. 原子 etcd txn (fenced)：
   - delete `node_override/{node_id}`
   - delete `nodes/{node_id}`（节点注册项，如果有）
   - delete 任何 ephemeral 节点元数据
   - put `decommissioned/{node_id}` = `{ removed_at, removed_by, reason }` (永久墓碑，#2 zombie 防护)
6. 同步清理内存：NodeStateTracker 删除该 node_id entry
7. 返回 OK

**Zombie 防护（#2 漏洞补丁）**：
- 节点注册路径（`lib.rs` 接收新节点 register 请求处）：
  - 检查 etcd `decommissioned/{node_id}` 是否存在 → 存在则拒绝注册
  - 检查 etcd `node_override/{node_id}` 是否为 Fenced → 是则拒绝注册（要求运维先 clear_override / decommission）
- 防止已 remove 的 node_id 复用 + 防止 Fenced 节点自己复活后偷偷重新加入

**Maintenance 自动过期（#6 漏洞补丁）**：
- `NodeStateTracker.tick()` 每周期检查所有 Maintenance entry
- 如 `expire_at` 已过 → 自动 clear_override，节点退回 auto state（Online 或 Suspected）
- 写日志 + 触发 audit log（F211-I）

**核心安全语义**：remove 是 hard delete，不可逆。失败时返回明确原因（哪个 extent / 哪个 marker 还引用），运维可针对性处理。

**验收**：
- 单元测试：
  - override 优先于 auto state
  - remove 在节点非 Fenced 时返回 PRECONDITION
  - remove 在仍有 extent 引用时返回 PRECONDITION + extent 列表
  - remove 在仍有 marker 引用时返回 PRECONDITION + marker 列表
- 集成测试：fence → 等 recovery → remove 完整 lifecycle

### F211-D：Owner-lock revision bump on fence + shard read/write/commit fence
**改动**：
- `crates/manager/src/node_state.rs`：`mgr_fence_node` 处理流程内部：
  - 找出该节点持有 ownership 的所有 extent
  - 对每个 extent: bump owner-lock revision（复用现有 `acquire_owner_lock` 或新建 batch 函数）
  - 原子写 etcd
- `crates/stream/src/extent_node.rs` **三个 handler 都加 revision fence**：
  - `handle_write_shard` (lines 4372-4395)：拒绝 `req.revision < last_revision`
  - `commit_shard_local` (lines 2954)：同样加 fence
  - **`handle_read_shard` / 读 extent 路径**（#4 漏洞补丁）：拒绝 `req.revision < last_revision`
    - 读 path 加 fence 防止 client 通过 stale cache 读到老 EN 的过期 shard
    - 注意 fence 失败时返回明确错误 → 客户端知道要回 manager 取最新 location
  - 参考 append fence 实现模式
- `crates/rpc/src/manager_rpc.rs`：`WriteShardReq` / `CommitEcShardReq` / `ReadShardReq` 加 `revision: u64`
  - **wire-compat 警告**（per `feedback_warn_on_backward_incompat`）：rkyv 结构变化，需 V2 message id 或字段末尾追加 + 兼容回退
  - 读路径需要 client 也升级（partition server 用最新 revision 调）

**验收**：
- 单元测试：write_shard / commit_shard / read_shard 在 revision < last_revision 时返回 PRECONDITION
- 注入测试：fence 后老 client 携带旧 revision 的所有操作（读 + 写 + commit）100% 被拒
- 集成测试：模拟假死 coord 复活，所有请求被拒；客户端缓存的旧 location 也无法读到 stale 数据

### F211-E：Recovery loop gated by Fenced state + 退避（#7 漏洞补丁）
**改动**：
- `recovery.rs:160-168` F138 互斥**完全不动**（marker 在仍阻塞）
- `recovery.rs:355-484` `recovery_dispatch_loop`：
  - 当前判定：`disk.online == false`
  - 改为：node 状态为 `Fenced`（不再依赖 disk 自动判定）
  - **行为变化**：单纯 Suspected 不再触发 recovery
- **每 (extent_id, slot) 维护 retry 状态**（in-memory）：
  - `last_attempt_at: Instant`
  - `consecutive_failures: u32`
  - 失败 → 指数退避（2^N 秒，上限 5 min）
  - 成功 → 清空状态
  - 防止持续刷日志 + 浪费资源

**向后兼容警告**（per `feedback_warn_on_backward_incompat`）：
- 这是 backward-incompat 行为变化：单节点故障不再自动重建，必须 OP 显式 fence
- 必须先让 F211-G policy script 上线后才能切此改动
- 提供 env var 临时回滚：`AUTUMN_MGR_RECOVERY_GATE = auto_disk | fenced_only`（default `fenced_only`）

**验收**：
- 集成测试：节点故障但未 fence → recovery 不触发；fence 后才触发
- 集成测试：注入持续失败的 recovery 任务，验证指数退避生效（日志频率随时间衰减）
- 性能验证：transient 故障不再引发误重建

### F211-F：EC convert auto-abandon on coord fenced + Suspected-window 跳过（#3 漏洞补丁）
**改动**：
- 新文件 `crates/manager/src/ec_abandon.rs`
- Hook：监听 `mgr_fence_node` 操作完成 → 扫 inflight markers 找 target_nodes[0] == node_id 的
- 对每个 marker 执行原子 abandon：
  - revision 已在 F211-D bump（同一 fence_node 操作内）
  - 原子 etcd txn：delete marker + put advisory entry（审计）
  - 同步清理内存 ledger
- F208 `extent_inflight_stale_sweep_loop`（`extent_inflight.rs:426`）行为：
  - 不再 WARN-only
  - 检测 marker 老于阈值时**只写 advisory entry**（visibility），**不删 marker**
  - 这样 OP policy script 周期扫 advisory 就能发现"该考虑 fence 这个 coord 了"
- `ec_conversion_dispatch_loop`（`recovery.rs:700`）加 **Suspected 跳过逻辑**：
  - 每次 dispatch 前查 NodeStateTracker 判断 coord 当前状态
  - 如 coord 为 `Suspected` 或 `Fenced` 或 `Maintenance` → 本 tick 跳过 dispatch（不报错、不刷日志）
  - 等节点状态恢复 Online 或 OP 介入 fence 后再继续
  - 避免 Suspected 窗口期持续刷无效 dispatch 日志
- **EC convert 不自动重发**（按用户原则）：reissue 由 OP policy script 自己发现后调 `force_ec_convert`

**验收**：
- 集成测试：fence coord 节点 → inflight marker 自动消失 + advisory entry 出现
- 集成测试：调 force_ec_convert 重发后 advisory 自动清除
- 集成测试：coord Suspected 窗口期 ec_conversion_dispatch_loop 不刷错误日志

### F211-H：Recovery 节流 + 优先级（#1 漏洞补丁）
**改动**：
- `crates/manager/src/recovery.rs`：
  - 引入 `RecoveryRateLimiter`：
    - `max_concurrent_per_source: u32` (从源节点同时拉数据的最大并发，默认 4)
    - `max_concurrent_per_target: u32` (target 节点同时写入的最大并发，默认 2)
    - `max_global_concurrent: u32` (全局上限，默认 64)
  - `recovery_dispatch_loop` 内每次选取 candidate 时跑限流检查
  - 引入优先级队列：
    - 副本数最少的 extent 优先（接近不可恢复阈值的优先）
    - 同优先级内按 extent_id 排序保证公平
- 配置 env vars：
  - `AUTUMN_MGR_RECOVERY_MAX_PER_SOURCE = 4`
  - `AUTUMN_MGR_RECOVERY_MAX_PER_TARGET = 2`
  - `AUTUMN_MGR_RECOVERY_MAX_GLOBAL = 64`
- 暴露监控接口 `mgr_recovery_stats() -> RecoveryStatsResp`（in-flight count、queue depth、per-node IO 估算）

**验收**：
- 单元测试：限流器在达到阈值时阻塞新 dispatch
- 集成测试：fence 一个大节点（持有 100+ extents），验证 dispatch 并发不超过 max_global
- 性能测试：限流下 client 读写延迟不被 recovery IO 显著拖慢

### F211-I：Operator action audit log（#8 漏洞补丁）
**改动**：
- 新 etcd prefix `mgr_audit_log/{timestamp_ns}_{op_id}` → rkyv `MgrAuditEntry { op, node_id?, extent_id?, by, reason, result }`
- 所有 admin RPC 内部 wrap 一层：成功/失败后 append audit entry
  - 覆盖：fence_node / set_node_maintenance / clear_node_override / remove_node / force_ec_convert / force_abandon_ec_marker
- 新 RPC：`mgr_query_audit_log(filter, limit, since?, until?) -> Vec<MgrAuditEntry>`
- 简单 GC 策略：定期删除老于 90 天的 entry（环境变量 `AUTUMN_MGR_AUDIT_RETENTION_DAYS`）

**验收**：
- 单元测试：每个 admin RPC 后写入 audit entry
- 集成测试：query_audit_log 按时间 / op type / node_id filter 正确
- 持久性测试：leader failover 后 audit log 完整

### F211-G：Python OP policy script
**改动**：
- `python/node_policy.py`（新文件，遵循 `feedback_ops_tools_in_python.md`）
- 周期模式 / one-shot 模式两种运行方式
- 主要功能：
  ```
  # 只读视图
  list                          # list_node_states + extent_health 综合视图
  inspect <node>                # 单节点详情 (suspected 时长、持有副本、关联 marker)
  inspect-extent <id>           # 单 extent slot 健康 + 关联 marker
  list-stale-markers            # ec_convert_advisory 列表
  
  # 决策 (admin)
  fence <node>                  # 调 mgr_fence_node
  maintenance <node>            # 调 mgr_set_node_maintenance
  unfence <node>                # 调 mgr_clear_node_override
  remove <node>                 # 调 mgr_remove_node (Fenced + 无引用时才允许)
  reissue-ec <extent_id>        # 调现有 force_ec_convert
  
  # 整合 lifecycle (HDFS decommission 等价物)
  decommission <node>           # fence → 轮询等 recovery 完成 + advisory 处理 → remove
                                # 安全交互式 confirm 每步；--yes 跳过确认
  
  # 半自动 (谨慎)
  auto-reissue --dry-run        # 扫 advisory + 验证 extent 3R 健康 + 调 force_ec_convert
  ```
- 内置安全检查：fence 前显示 manager view + 等待人工 confirm（除非 `--yes`）
- **不**新增 autumn-client subcommand

**验收**：
- 手动验证完整 runbook
- dry-run 模式产生合理输出

---

## 关键不变量（不改动）

- ✅ F138：marker 在时 recovery 仍阻塞（marker 删除是同步点）
- ✅ F149：leader fence
- ✅ F119-D：EC convert 幂等 guard
- ✅ F153：per-extent ec_conversion_locks
- ✅ F207：marker rich state + replay
- ✅ Reed-Solomon 编码确定性
- ✅ owner-lock revision 协议（只是新增更多使用点）

---

## 修改文件清单

### Rust 改动
1. `crates/manager/src/node_state.rs`（新）
2. `crates/manager/src/lib.rs`：PS liveness 接入 tracker
3. `crates/manager/src/recovery.rs`：
   - `recovery_dispatch_loop`：gate on Fenced
   - `disk_status_update_loop`：喂 tracker
4. `crates/manager/src/ec_abandon.rs`（新）：fence hook + auto-abandon
5. `crates/manager/src/extent_inflight.rs`：
   - `extent_inflight_stale_sweep_loop`：写 advisory 而非 WARN-only
   - 新增 `MgrEcAdvisoryEntry` rkyv 结构
6. `crates/manager/src/rpc_handlers.rs`：9 个新 RPC
   - list_node_states / extent_health_report / list_ec_inflight_markers
   - fence_node / set_node_maintenance / clear_node_override / remove_node
   - recovery_stats / query_audit_log
7. `crates/rpc/src/manager_rpc.rs`：新 MSG_ 常量 + Req/Resp 结构
8. `crates/stream/src/extent_node.rs`：shard write/commit revision fence (F211-D)
9. `crates/common/src/config.rs`（或等价）：
   - `AUTUMN_MGR_NODE_SUSPECTED_TIMEOUT_SECS = 10`
   - `AUTUMN_MGR_RECOVERY_GATE = fenced_only`（回滚 env: `auto_disk`）

### Python 工具
10. `python/node_policy.py`（新，F211-G）

### 维护文件
11. `feature_list.md`：F211-A 到 F211-I 9 个条目，passes:false
12. `claude-progress.txt`：分阶段更新
13. `autumn-rs/README.md`：补 "节点生命周期 OP 流程" 章节 + "Recovery 节流配置" 章节
14. `crates/manager/CLAUDE.md`：补节点状态机 + EC abandon 设计 + 限流器架构
15. 新文件 `crates/manager/src/recovery_rate_limiter.rs`（F211-H 限流器）
16. 新文件 `crates/manager/src/audit.rs`（F211-I audit log 写入/查询）

---

## 落地顺序

按依赖关系：

1. **F211-A** Node tracker（pure addition）
2. **F211-B** Health reporting RPCs（暴露事实，纯只读）
3. **F211-I** Audit log（基础设施，越早接入越好；后续 admin RPC 都会写它）
4. **F211-C** Operator fence/maintenance/remove RPCs（开始有 OP 接口，含容量预检 + zombie 防护 + maintenance TTL）
5. **F211-D** Owner-lock revision bump + shard read/write/commit fence（fencing 基建，必须在 E/F 之前）
6. **F211-H** Recovery 节流 + 优先级（基建，给 E 用）
7. **F211-G** Python policy script（运维工具，必须在 E 之前上线）
8. **F211-E** Recovery loop gated by Fenced + 退避（**行为变化**，需 A-D + G + H 全部就绪 + 监控渠道准备好）
9. **F211-F** EC convert auto-abandon on fence + Suspected 跳过（依赖 C/D/E，行为变化但只针对 EC 路径）

阶段化：1-7 是基建（无行为变化或低风险），可一起合到 main；8-9 是语义切换，分开上线 + 监控观察。

---

## 验证步骤

### 单元测试
- `node_state.rs`：Online ↔ Suspected 转换、override 合并
- `ec_abandon.rs`：on fence 触发 auto-abandon
- shard write/commit revision fence

### 集成测试
- `tests/test_node_state_machine.rs`：节点故障 → Suspected；不自动进任何其他状态
- `tests/test_fence_node.rs`：fence → revision bump → recovery 触发 → 完整流程
- `tests/test_ec_convert_coord_fence.rs`：
  - 起 6-EN 集群
  - 触发 force_ec_convert
  - SIGKILL coord EN（不重启）
  - 等 10s 后 list_node_states 看到 coord Suspected
  - 调 fence_node(coord)
  - 验证 marker 自动消失 + advisory 出现
  - 等 recovery 跑完源 extent 回 3R
  - 调 force_ec_convert 重发
  - 验证 EC 完成 + advisory 清除

### 手动验证
- `cluster.sh reset 6` + 跑完整 runbook
- 测 maintenance：模拟运维场景，标 maintenance 不触发任何 recovery
- 测假死复活：fence 后再启动老 coord，所有请求被 revision fence 拒绝

### 性能验证
- transient 故障（< soft_timeout 内恢复）不再触发任何 recovery（对比旧行为）
- NodeStateTracker tick O(N_nodes)，不在 hot path

---

## 向后兼容性 ⚠️

按 `feedback_warn_on_backward_incompat` 显式标记：

- ⚠️ **行为变化（核心）**：节点故障默认不再自动重建。需 OP 显式 fence 才触发。提供 env `AUTUMN_MGR_RECOVERY_GATE = auto_disk` 临时回滚
- ⚠️ **行为变化**：EC convert 不再 stuck 等运维 etcdctl，fence 后自动 abandon；但 reissue 仍需 OP 主动
- ⚠️ **行为变化**：Recovery 现在有并发上限（F211-H 限流），默认 max_global=64；老行为是无限并发。env var 可调
- ⚠️ **新 etcd prefix**：`node_override/`、`ec_convert_advisory/`、`decommissioned/`、`mgr_audit_log/`（回滚需 etcdctl 清空）
- ⚠️ **新 MSG_ 常量**：9 个新 RPC，旧 client 不能调，gracefully fallback
- ⚠️ **rkyv 结构扩展**：`WriteShardReq` / `CommitEcShardReq` / `ReadShardReq` 加 revision，需 V2 message id（**读路径变化是新的，partition server 也要升级**）
- ⚠️ **节点注册路径变化**：Fenced / Decommissioned 节点 register 会被拒，需要先 clear_override / 改用新 node_id
- ⚠️ **新增 Python 工具**：要求新 manager；老脚本继续工作
- ✅ rkyv 现有结构（marker、extent metadata）不变
- ✅ owner-lock revision 协议不变
- ✅ F138 / F149 / F207 / F119-D / F153 完全不变

---

## 关键文件路径速查

| 文件 | 用途 |
|---|---|
| `crates/manager/src/lib.rs:1305` | F069 PS liveness loop（接入点） |
| `crates/manager/src/recovery.rs:160` | F138 互斥点（**不改**） |
| `crates/manager/src/recovery.rs:355` | `recovery_dispatch_loop`（gate on Fenced） |
| `crates/manager/src/recovery.rs:571` | `disk_status_update_loop`（喂 tracker） |
| `crates/manager/src/recovery.rs:700` | `ec_conversion_dispatch_loop`（看 abandon hook） |
| `crates/manager/src/extent_inflight.rs:213` | `acquire_extent_inflight` |
| `crates/manager/src/extent_inflight.rs:397` | F209-C 决策点（替换为写 advisory） |
| `crates/manager/src/extent_inflight.rs:426` | `extent_inflight_stale_sweep_loop` |
| `crates/stream/src/extent_node.rs:2862` | shard staging path |
| `crates/stream/src/extent_node.rs:2954` | `commit_shard_local`（F211-D 加 fence） |
| `crates/stream/src/extent_node.rs:4052` | `handle_convert_to_ec`（不改） |
| `crates/stream/src/extent_node.rs:4372` | `handle_write_shard`（F211-D 加 fence） |
| `python/d.py` | Python RPC client 参考 |
