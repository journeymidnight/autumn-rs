# Rolling Upgrade 设计（v1.2，2026-06-12）

> 状态：**R0 + R1 已实现并实测通过**（R0: scripts/rolling_restart.sh，
> 3-EN/4 分区持续写负载下全序列零丢失；R1: cluster_version 门 +
> `[min_wire,max_wire]` 区间握手 + 指纹注册表防忘 bump + 回滚
> fail-closed）。R2/R3 待用户拍板（§9 决策点 1/2）。WIRE-1（981c3ef）
> 已把混版本部署从静默损坏变为启动硬拒绝——本文档定义如何把这个硬拒绝
> **有计划地放宽**成真正的滚动升级能力。

## 1. 目标与非目标

**目标**
- 逐节点升级一个在线集群（EN / PS / manager / 客户端），全程数据面可用、
  零已确认数据丢失。
- etcd 持久化状态跨版本可读（新 leader 重放旧值）。
- 支持回滚到前一版本（同样滚动）。

**非目标**
- 跨多个大版本跳跃升级（兼容窗口 = 相邻版本，见 §5）。
- 热路径序列化格式更换（rkyv / 手写编码保留——性能是既定约束）。
- 客户端长期多版本并存（python wheel 等随集群版本走，窗口内兼容即可）。

## 2. 现状：为什么今天做不到

| 层 | 现状 | 升级障碍 |
|---|---|---|
| RPC 帧 | 10 字节头 `[req_id:4][msg_type:1][flags:1][len:4]`，单一协议 | 无版本协商通道（但 `flags` 有空闲位，`msg_type` 0x00-0xFF 有空段） |
| 控制面 payload | rkyv（manager_rpc ~50+ 结构） | rkyv archive = 内存布局，任何字段增删改 = 不兼容，**无演进能力** |
| 数据面 payload | 手写二进制（Append/ReadBytes/CommitLength）+ rkyv（partition_rpc） | 同上；热路径性能敏感，不可换格式 |
| etcd 持久值 | rkyv（manager mirror_* 共 54 处 encode 站点） | 新 leader replay 旧值 = 解码失败/垃圾 |
| 部署 | cluster.sh 全停全起；WIRE-1 指纹硬拒绝混版本 | 不存在滚动路径 |
| 升级窗口语义 | 无 cluster version 概念 | 新功能/新字段何时"安全启用"无判定依据 |

## 3. 总体方案：四阶段（每阶段独立可交付、可验证）

### R0 — 滚动重启程序化（同 commit 内，零 wire 改动，~1 天）✅ 已实现 2026-06-12

chaos 战役已证明每个角色 kill+restart 零丢失（transport/etcd/HA 全家
harness）。把这个能力固化为运维程序：

- `scripts/rolling_restart.sh`：按序逐个重启（顺序见 §6），每步等待收敛
  门（EN: 重新上线 + recovery 静默；PS: 分区迁回/心跳恢复；manager:
  leader 稳定），失败即停。
- 价值：同 commit 的配置变更/换机/内核升级即刻可滚动；同时它就是 R1+
  之后真正升级编排的骨架。

### R1 — cluster_version 门 + 连接握手（地基，~3 天）✅ 已实现 2026-06-12

> 实现注记：① 区间握手经由 `GetClusterIdResp`（该结构从 R1 起**冻结**——
> 它是协商通道本身，再改布局会让混版本握手不可达）；② 防忘 bump =
> `WIRE_VERSION_FINGERPRINTS` 注册表 + 单测（schema 源文件任何改动都使
> 指纹变化并 fail 测试，强制显式版本决策）+ 运行时 fraud 交叉校验（对端
> 声明我方已知版本但指纹不符 → 拒绝）；③ 回滚 fail-closed：manager 读到
> 持久 cluster_version 超出自身 max_wire 时拒绝（经 replay 阻断当选）。

参照 TiKV/CockroachDB 的 cluster version 模型：

- **持久化 `cluster_version`**（etcd key，operator 通过 `autumn-op
  upgrade-version` bump；manager 校验单调 + 只允许 +1）。
- **二进制自带 `[min_wire, max_wire]` 区间**（编译期常量，代替 WIRE-1
  的单点指纹；WIRE-1 检查放宽为"区间有交集"）。
- **语义**：所有成员二进制升到 N 之后，operator 才 bump
  cluster_version → N；**新 wire 形态/新持久值格式只有在
  cluster_version ≥ N 时才允许发出**。在此之前新二进制以 N-1 模式运行。
  这把"什么时候所有人都懂新格式"从猜测变成显式状态。
- 回滚规则：cluster_version 未 bump 前可滚回 N-1 二进制；bump 后不可
  （新格式可能已持久化）。

### R2 — 控制面 schema 演进：manager_rpc + etcd 值迁移 prost（核心投资，~1-2 周）

**这是本设计最大的一次性成本，换取长期免演进负担。**

- 范围：manager_rpc 全部请求/响应结构 + 54 处 etcd mirror 值。
- 理由：
  - 控制面低频（心跳 2s、df 2s、alloc 按 roll），序列化成本无关紧要；
  - prost（仓库已有依赖，etcd 客户端在用）的 tag-based 编码天然
    前后兼容：新增 optional 字段旧端忽略、旧值缺字段新端取默认——
    **此后控制面加字段不再产生升级事件**；
  - etcd 重放跨版本可读是滚动升级的硬前提，rkyv 做不到。
- 迁移方式：一次性切换（本身仍是一次 same-commit 部署 + `cluster.sh
  reset` 或一次性 etcd 值转换工具），切换后进入演进时代。
- 热路径**不在**此范围：partition_rpc 的 Put/Get 与 extent_rpc 手写
  编码保持 rkyv/手写。

### R3 — 数据面版本协商（按需，~1 周）

热路径结构极少变（PutReq 上次变更是 BUG-LEASE-2 加 fence 字段），用
"冻结 + 显式 V2"模型而非格式更换：

- **连接级 hello**：RPC connect 后首帧交换 `[min_wire, max_wire]`
  （新 msg_type；旧端不识别 → 视为 N-1，由 R1 的区间检查保证只可能差
  一版）。连接缓存协商结果 `eff = min(self.max, peer.max)`。
- **冻结纪律**：已发布的 wire 结构永不修改；变更 = 新建 `FooReqV2` +
  新 msg_type（msg_type 空间足够），服务端两者都接，发送端按 `eff`
  选择编码。N-2 的 V1 处理代码在窗口滑过后删除。
- 帧头不变（10 字节布局稳定），版本信息全部在 msg_type 维度表达——
  对热路径零额外字节、零额外分支（msg_type dispatch 本来就有）。

## 4. etcd 持久状态演进规则（R2 之后）

1. 新字段一律 optional + 有语义化默认值；
2. **写新格式 gate 在 cluster_version bump 之后**（R1 语义）；
3. 删除字段需两个版本：N 停读、N+1 停写；
4. `replay_from_etcd` 是唯一解码入口，天然单点可审计。

## 5. 兼容窗口：仅 N ↔ N-1

- 任意时刻集群中至多两个相邻版本并存（升级中）；
- 跳版本升级 = 逐版本滚动多轮；
- 收益：每个版本只需维护对前一版的兼容代码，V1 处理代码生命周期 = 一个
  版本窗口。

## 6. 升级编排顺序与论证

```
1. EN（逐个）   ← 最被依赖端（PS/manager/client 都调它）；服务端先升，
                  新 EN 必须接受 N-1 请求（向后兼容服务端优先）
2. PS（逐个）   ← 依赖 EN（已是新）+ manager（仍旧，PS 以 N-1 模式发）
3. manager（standby 先、leader 后）
4. bump cluster_version
5. 客户端/wheel/fuse（随时，连接级协商自动适配）
```

- 每步之间跑收敛门（R0 的骨架）+ 写活性探针（chaos harness 现成）。
- 回滚 = 逆序滚回，前提 cluster_version 未 bump。

## 7. 测试策略：mixed-version chaos

新 harness `scripts/upgrade_chaos.sh`：

1. 构建两份二进制（git worktree 上一 tag vs HEAD）；
2. 旧版本起集群 + 持续写负载（etcd_chaos 的 workload 骨架）；
3. 按 §6 顺序滚动到新版本，每步断言：写持续推进、零丢失清单校验、
   无 wedge（写活性检查）；
4. 中途注入既有 chaos 事件（kill PS / kill etcd member）验证"升级中
   叠加故障"；
5. 反向滚回（cluster_version 未 bump 分支）再验证一轮。

CI 形态：每次发版 tag 前手动跑（双构建成本高，不进常规循环）。

## 8. 备选方案与否决理由

| 备选 | 否决理由 |
|---|---|
| 全栈换 protobuf | 热路径回归（rkyv 零拷贝是实测选型）；R2 已把需要演进的面覆盖 |
| 全栈版本化 rkyv（每结构手写 V1/V2 + 转换） | 54 处持久值 + 50+ RPC 结构的 churn 不可持续；控制面用 prost 一劳永逸 |
| 蓝绿双集群 + 迁移 | 数据量级与成本不成比例；本系统无跨集群复制设施 |
| 不做（维持 same-commit） | kvcache/推理在线业务无法接受全停窗口（这是本设计的动因） |

## 9. 需要拍板的决策点

1. **R2 prost 迁移范围**：manager_rpc 全量 + etcd 值（推荐），还是仅
   etcd 值（RPC 留 rkyv + R3 式冻结）？后者省一半迁移但控制面每次加
   字段仍是升级事件。
2. **R2 的切换方式**：`cluster.sh reset`（丢 etcd 元数据重 bootstrap，
   开发期可接受）vs 一次性离线转换工具（保留现网数据）。
3. **cluster_version bump**：纯 operator 手动（推荐，显式可控）vs
   manager 检测全员就绪后自动。
4. **阶段顺序**：按 R0→R1→R2→R3 推进（推荐），还是先 R0+R1 停下来
   观望（R2 是大头，可以等真正需要第一次在线升级前再做）？

## 10. 工作量汇总

| 阶段 | 估算 | 交付物 |
|---|---|---|
| R0 | ~1 天 | rolling_restart.sh + 收敛门 + 文档 |
| R1 | ~3 天 | cluster_version etcd 门 + [min,max] 区间握手 + autumn-op 子命令 |
| R2 | ~1-2 周 | manager_rpc/etcd 值 prost 化 + 转换工具 + 回归 |
| R3 | ~1 周（按需触发） | 连接 hello + 冻结纪律 + 首个 V2 样例 |
| 测试 | ~3 天 | upgrade_chaos.sh 双版本滚动 harness |
