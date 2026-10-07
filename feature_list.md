# autumn-rs feature list — OPEN backlog

**Last updated:** 2026-10-07

**Rules:**
- This file tracks the **OPEN backlog only**. A feature that reaches `passes: true`
  is **DELETED** from here — git history is the record, there is no archive file
  (CLAUDE.md rule 13: 定期清理删除，保持整洁).
- `passes` and `notes` are the only mutable fields after a feature is created.
- Out-of-scope / "v2 再做" decisions must be recorded as proper feature entries
  (F-name + Trigger + Scope + Acceptance + `passes: false`), never as plan-file footnotes.

---

## Active

### F-CLUSTER-STATUS-SUMMARY — 首页状态改为可核对的明细摘要，分母是期望数
- **Trigger** (2026-10-07 用户): `HEALTH_OK` 只看 extent，PS 全挂、无 standby 时照样 OK；作为首页状态不合适。要一份可核对的明细：`Manager leader 1 / standby 1`、`PS Ready 3/3`、`EN Online 6/6`、`Extent clean/degraded/unavailable`、`Recovery inflight`、采样时间。**分母必须是期望数**，不能用当前发现的节点数凑成 5/5。
- **设计定案（用户确认）**:
  1. 期望数 = etcd 持久成员表。新 id 首次注册自动加入；重启/掉线/驱逐不改成员表，只影响分子；只有显式 remove 删除，且在线成员拒绝删除。
  2. `--psid` 与新加的 `--manager-id` 都手填。psid 唯一性由运维保证（manager 不判重）；manager 启动时对 `manager_alive/<id>` 做 etcd lease 占位（`create_revision==0`），占不到每秒重试并打出持有者，lease 丢失则退出——这把 key 同时是 standby 计数的来源。
  3. 不显示读/写可用推导。
  4. `HEALTH_OK/WARN/ERR` 彻底移除（常量、wire 字段、页面、测试、文档）。
- **Scope（分步，每步独立提交）**:
  1. PS 成员表 `psMembers/`；驱逐保留成员并记离开时间；`autumn-op ps-remove`（在线拒绝）；overview 列出全部成员。
  2. `--manager-id` + lease 占位 + `managerMembers/` + `autumn-op manager-remove`（在线拒绝）；cluster.sh 与脚本启动命令同步。
  3. `GET_CLUSTER_STATUS` RPC（leader 上计算，带采样时间与各来源数据年龄）+ `autumn-op status`。
  4. dashboard 首页顶栏改为该摘要；移除 `HEALTH_*`；docs/README/CLAUDE.md。
- **Acceptance**:
  - 3 PS 杀 1 台，等过驱逐窗口，分母仍为 3；跨 leader 切换不变；在线成员 remove 被拒；消融（驱逐删成员 / replay 不读成员 / remove 不查在线）各自变红。
  - 2 manager 杀 standby 显示 `standby 0/1`；同 id 第二个 manager 进程占不到 id、日志报出持有者；消融变红。
  - `autumn-op status` 与 dashboard 顶栏显示同一份数据，含采样时间；leader 无应答时显示未知而非旧值。
  - 代码与文档中不再出现 `HEALTH_OK/WARN/ERR`。
- `passes: false`
- **notes** (2026-10-07): 第 1 步完成（wire 57，record type 11 `MemberRecord`）。第 2 步完成：`--manager-id` + `managerAlive/` lease 占位（丢 lease 重占，被他人占走才退出）+ `managerMembers/` + `manager-remove`。已知：manager 记录的是 `--listen` 地址（k8s 下为 0.0.0.0）。第 3 步完成：`MSG_GET_CLUSTER_STATUS` + `autumn-op status [--json]`。

### F-ETCD-AUTH — manager 连接带认证/TLS 的 etcd
- **Trigger** (2026-10-07 用户): 生产 etcd 要开 auth。现 manager 只有 `--etcd <endpoints>`，`crates/etcd` 是 h2c 明文 gRPC，无 `Auth/Authenticate`、无 token 头、无 TLS；开 auth 的 etcd 连不上。
- **Scope**: 用户名+密码（`Auth/Authenticate` 换 token，每请求带 `token` 头，过期重认证一次再发）；TLS（rustls，CA 校验，可选双向证书）。CLI flag：`--etcd-user`、`--etcd-password-file`、`--etcd-cacert`、`--etcd-cert`、`--etcd-key`（rs 不读 env）。cluster.sh / docs/ops.md 同步。
- **Acceptance**: 对开了 auth+TLS 的真实 etcd，manager 选主、replay、写入、token 过期后自动重认证均正常；错误口令/证书启动即响亮失败；不带新 flag 时对明文 etcd 行为不变。
- `passes: false`
- **notes** (2026-10-07): 用户定：后做，排在 F-CLUSTER-STATUS-SUMMARY 之后。

### F-PS-CORE-CAPACITY — 分区放置按 PS 核容量；允许超卖，manager 感知并按策略消解
- **Trigger** (2026-09-29 用户讨论): `--cpuset` 下每个分区占 2 核（P-log + P-sst），PS 容量 = `cpuset_len/2`，但 manager 放置分区只看各 PS 的 region 数（`compute_region_for_partition`、`rebalance_regions`、`compute_rebalance_moves` 三处），完全不知道核容量。PS 侧预算门是硬拒：`sync_regions_once` 满了拒开（分区一直 `ps=unknown`），`handle_split_part` 满了拒 split，且检查的是父分区所在 PS，而右孩子由 manager 派到最少 region 的 PS，可能不是本机。超出核数的线程 `pick_cpu_for_ord` 返回 `None` 不绑核，继承进程掩码，可能跑出 cpuset 抢 EN/其他租户的核。
- **设计定案（用户确认）**:
  1. `--cpuset` 即该 PS 全部核预算；不带 `--cpuset` 的 PS 容量未知、不参与核容量管理。
  2. 放置顺序：有空 slot 的 cpuset PS（空闲 slot 多者优先）→ 无 cpuset 的 PS → 全满时超卖到 `used/cap` 最低的 PS。容量是软门，不做预留/2PC。rebalance 同样改按 `used/cap`。
  3. 允许超卖：PS 不再因核预算拒开分区或拒 split。超卖分区的线程亲和到整个 cpuset 掩码（浮动但不出 cpuset）。端口序号与核槽位解耦；绑核分区关闭腾出槽位时，把一个浮动分区晋升绑核（线程自行 re-pin，不重开分区）。
  4. manager 能看到超卖（每 PS `used/cap`、浮动分区数，经 `client info`/dashboard 可见），按顺序处理：迁移（集群尚有空 slot）→ 超卖 merge（集群整体满）→ 告警加 PS（无足够冷的相邻对）。动作走 op ledger，在 leader-fenced manager 内执行。
  5. 防跷跷板（用户硬要求：split/merge 绝不能来回切换）：
     - 超卖 merge 可放宽 merge 阈值，但合并后指标必须不超过 split 阈值的 **1/4**：QPS 和 ≤ 3.75K（`SPLIT_QPS_HIGH/4`）、带宽和 ≤ `SPLIT_BW_HIGH/4`（≈44 MiB/s）、imm_full 仍须为 0、每侧大小仍 < `MERGE_SIZE_LOW`（不放宽）。
     - 同一 tick 由一个 planner 统一决策；集群有待执行 split 时不做超卖 merge。
     - split 产生的孩子在 merge 冷却期内不可作超卖 merge 候选；merge 产物在冷却期内不可因超卖被迁移。
     - 为凑同 PS 而做的迁移与 rebalance 共用同一容量评分，只有评分严格下降才做。
- **Scope（分阶段，每阶段独立提交）**:
  - 阶段 1：PS 上报核容量（cpuset 是否显式、`slot_cap`）；manager 在三处放置/rebalance 路径按上述顺序与 `used/cap` 决策；容量可见于 `client info`。manager 换主后容量信息须能恢复（注册或心跳重新带上，不能依赖旧 leader 内存）。
  - 阶段 2：PS 去掉两处硬拒；超卖线程亲和整个 cpuset 掩码；端口序号与核槽位解耦，槽位空出时晋升浮动分区。
  - 阶段 3：manager 超卖处理（迁移 → 超卖 merge → 告警）及防跷跷板规则。
- **Acceptance**:
  - 阶段 1：2 个 cpuset PS（容量不同）+ 1 个无 cpuset PS 的集群，新建/split 出的分区按放置顺序落位；manager 重启/换主后放置仍正确；消融（改回按 region 数）测试变红。
  - 阶段 2：分区数超过 `cpuset_len/2` 时全部可服务；浮动线程的亲和掩码等于 cpuset（读 `/proc/<pid>/task/*/status` 的 `Cpus_allowed_list`）；关闭一个绑核分区后，一个浮动分区在有限时间内变为单核绑定；split 在 PS 满时不再被拒。
  - 阶段 3：超卖时产生迁移或满足 1/4 上限的 merge；构造在阈值边界抖动的负载，断言 N 个 tick 内同一 key range 的 split+merge 次数 ≤ 1；无冷对时只告警不动作；各规则消融能变红。
- `passes: false`
- **notes** (2026-09-29): 阶段 1 完成——PS 注册/心跳上报 `slot_cap`（wire 50），manager 内存保存、换主清空待心跳补回；`ps_placement.rs` 统一排序，放置/split 右孩子/驱逐重放/rebalance/告警共用；rebalance 只做严格改进的移动且不移到已满 cpuset PS（PS 硬拒仍在，移过去会关掉一个在服务的分区）；`info`/dashboard 显示 used/cap。真实集群（etcd+manager+EN+3 PS，含 manager kill -9 重启）4 项放置检查全过；单测消融变红。阶段 3 待处理：告警的滞回带（`rebalance_gap_threshold`）让超卖少于阈值的 cpuset PS 永远当不了源——手动 `rebalance` 能修，自动策略不会（旧按数量的告警同样如此，非回归）；超卖处理 planner 要让 Over 源绕过这个带。

### BUG-POLICY-ACTIVATE-ATOMIC — policy 名称与模式切换跨两次 RPC
- **Trigger** (2026-09-27 dashboard review): `autumn-op auto-policy activate` 先 SET_ACTIVE 后 SET_MODE；manager 的 SET_ACTIVE 保留旧 mode。旧模式为 Armed 时，选择本应 DryRun 的新 policy 会先继承 Armed；第二次请求失败会留下部分更新，其他操作者也可在两次调用之间交错。
- **Scope**: 在 manager 提供一次持久化事务中的 name+mode 更新，让 CLI/dashboard 共用；明确已有 wire 的兼容/升级要求。不能仅换两次 RPC 的顺序或靠 dashboard 本地锁掩盖。
- **Acceptance**: Armed→选择新 policy 的 DryRun 更新只发布一份完整配置；注入持久化失败、leader 切换与并发操作者，不出现部分配置或误 arm 其他 policy；消融测试失败。
- `passes: false`
- **notes**: 两次 RPC 与 manager 保留旧 mode 已按代码核实；窗口内真实误派发尚未复现。本次 dashboard 迁移不改这个跨层契约。

### F-REVIEW-T3-REAL-CRASH — P2 crash 测试真正停止旧 runtime
- **Trigger**: review.md T3；drop RpcClient 不等于杀 PS/EN。
- **Scope**: 改用可终止 runtime 或 SIGKILL 子进程并等待退出；compact/flush 用 durable/checkpoint barrier。
- **Acceptance**: 证明旧服务退出、故障发生于指定窗口；重启验证 ACK 数据和 tombstone。
- `passes: false`

### F-REVIEW-T4-CHAOS-GATES — P2 chaos 覆盖与历史验收
- **Trigger**: review.md T4；动作零成功、ignored 未运行、未知结果抹掉已 ACK 历史。
- **Scope**: 区分 smoke/定向/长跑；必需动作非零门槛；修正 full-action 脚本；专用 CI；保留 invocation/response 历史及并发允许结果。
- **Acceptance**: 必需动作未进入/完成则失败；包含 manager/PS 故障与在线 Remove、DELETE/TTL/多 writer 的定向覆盖，CI 实际执行。
- `passes: false`
- **notes** (2026-09-20): 普通 manager 测试已解除对系统 FUSE 的无条件依赖；8 个 FUSE 专属目标通过 fuse-tests feature 显式启用，CI 的 clippy/integration 命令保持启用。完整 ignored chaos、动作覆盖门槛和历史 checker 尚未完成。本轮定向验证入口见 scripts/README.md。
- **notes** (2026-09-27): `scripts/transport_chaos.sh` 在 HEAD 上整体跑不起来——自 `67728e4`
  起 `autumn-client` 的 KV 命令必须带 `--namespace`，脚本没带，seed 全部失败，后续 E1–E7 的
  断言因此没有意义。同日 merge 加了分离闸门，E7b 前补了"两侧 compact + 等 `has_overlap=0`"
  （否则 freeze 直接拒、manager kill 落空），**这一步未实跑**。另：该脚本开机时 `kill -9` 本机
  所有 `autumn-*`/`etcd` 进程，多租户机器上会杀掉别的工作树的进程。

### F-COMPIO-UPGRADE — 升级运行时并分阶段验证 TCP 内核 CPU 降耗
- **Trigger** (2026-09-14，用户要求记录升级计划): 当前 compio 0.18.0 /
  compio-driver 0.11.4 / compio-runtime 0.11.0；H200-1 单 partition 写入采样的
  内核热点是 TCP 收发复制、发送页分配与清零。新版有 send zerocopy、multishot
  和 deferred task-run 等能力，但单纯升级不会自动让现有收发路径使用这些接口。
  依据：[CPU 分析](docs/perf_partition_cpu_20260914.md)。
- **Scope / 执行顺序**:
  1. **建立基线并升级依赖**：以已合入 CRC 复用和绑核修复的版本为基线，目标
     compio 0.19.x（已调研 0.19.1；实施时核对稳定补丁、MSRV 与子 crate 版本）。
     在独立分支适配 breaking API、feature、buffer ownership、取消和任务生命周期；
     同步 workspace 及独立的 `python/Cargo.toml` / `python/Cargo.lock`，检查重复
     compio 版本。先保持现有普通收发方式、并发参数和协议，单独测纯升级效果。
  2. **显式接入大值 send zerocopy**：优先 PS→EN 的复制发送，再评估 client→PS
     和读响应。保留 control/value 分片和完整 CRC；buffer 必须持有到内核零拷贝
     完成通知，不能仅凭发送完成就回池。覆盖部分发送、失败、超时、取消、连接关闭
     和不支持时的普通发送回退。按实际内核、链路、value 大小实测启用门槛。
  3. **分别评估接收与调度能力**：multishot/managed receive、poll-first、
     single-issuer/deferred task-run 逐项接入、逐项 A/B；验证与 UCX eventfd
     progress、线程 affinity、背压和完成通知的兼容。SQPOLL 仅作独立实验；
     不能把 CPU 转移到内核线程，或 NO_IOWAIT 导致的记账变化，当作工作量减少。
  4. **收敛交付**：只启用有稳定收益且通过正确性验证的能力；记录无收益/回退的
     实验，保留回退到基线的路径，更新 crate 指南、`docs/ops.md` 与性能报告。
- **Acceptance**:
  - 固定 key 集合、独立预热、相同 RF3 / NVMe / 分区 / cpuset / NUMA / 内核 /
    UCX 配置，分别比较「当前版本」「纯升级」「每项新能力」；单 partition 为主，
    多 partition 验证扩展性，覆盖 4 KiB、64 KiB、1 MiB、8 MiB，读写与不同深度。
  - TCP loopback、真实跨机 TCP、UCX 分开测；每组重复并记录吞吐、p50/p99、
    客户端/PS/EN 用户态与内核态 CPU、独立内核工作线程 CPU、CPU 秒/GiB、
    分配/复制及 io_uring 提交/完成指标。计数窗口与传输字节窗口必须一致；排除
    启动重放、磁盘满和非受控后台负载，收益须超出重复测试波动，无固定百分比承诺。
  - 回归验证帧/CRC、乱序完成与写入顺序、buffer 回池时机、取消/关闭、部分 I/O、
    故障回退、三副本持久化、TCP/UCX 字节一致性；构建并验证 FUSE、Python 等
    调用方。协议及存储格式保持不变；小值与 UCX 不得出现未经处理的显著回退。
  - 纯升级和新能力的效果分别报告；未测真实跨机或未通过上述验证时，不标记完成。
- **Status**: 2026-09-14 已升级到 0.19.2（compio fork 分支修 CreateSocket 双持有；阶段 2 的副本 TCP zerocopy 为默认关闭的 opt-in）。2026-09-24 被误判为 4K 写回退的根因而整体回退（`3279694`），2026-09-25 隔离核 A/B 证明 0.18≈0.19 后撤销回退（reset + force push）。阶段 3/4 未做。
- `passes: false`
- **notes** (2026-09-15 controlled validation): Compio 0.19.2/cyper 0.9 migration
  implemented with 1,052 library tests and TCP/UCX/Python/FUSE correctness checks.
  Completed 480 fixed-work performance samples plus 160 typed kernel diagnostic
  samples; all 640 window/count/affinity checks pass. See [controlled report](docs/perf_compio_controlled_20260915.md).
  Default-off zerocopy is slower on loopback; no production receive/scheduler
  defaults changed. UCX 64KiB p1 reads still regress ~8%; UCX local protection
  failure reproduced on both 0.18 and 0.19, tracked separately below. Shared-host
  noise limits whole-machine efficiency claims; sampled stacks have ID collisions.
  H200-2 offline, cross-host unavailable. Full acceptance remains passes:false.


### F-UCX-LOCAL-PROTECTION — 四分区 UCX SEND 间歇性本地保护错误
- **Trigger** (2026-09-14): compio 0.19 受控四分区 UCX 写入在 mlx5_1 上出现 Local protection error (synd 0x4 vend 0x52) 并 abort；compio 0.18 基线在相同拓扑的诊断采样中也复现，说明问题早于运行时升级。失败 SEND 包含无效/失效 lkey，根因尚未确定。
- **Scope**: 定位 UCX 1.16 stream vectored send 的 buffer ownership、注册缓存和取消完成生命周期，复现后只保留有证据的修复；不得通过加超时、重试或丢弃失败样本掩盖。
- **Acceptance**: 可重复触发的最小用例；修复消融失败/修复后通过；四分区 TCP/UCX 字节一致性、注册 buffer 取消/复用及重复压力验证；记录吞吐和 CPU 代价。
- **notes**: 受控验证保留了 0.19 普通跑分和 0.18 诊断采样两份崩溃证据。后续重复成功不关闭该问题。
- `passes: false`

> **这个账本只记 autumn-rs 自己的东西。** 下游怎么被 autumn 的改动影响（例如一次 wire
> 版本变更要求哪些内嵌客户端重建）算 autumn 的后果，该记；下游自己的缺陷、进展和上线
> 状态不算，记在它们各自的仓库里。

### F-DISK-REBALANCE — 机内盘间倾斜没有任何东西会纠正
- **Trigger** (2026-09-09): `choose_disk` 现在按负载选盘(见 F-EXTENT-PLACEMENT Scope 1 的
  第二个 commit),但那只决定**新** extent 去哪。一块盘上已有的存量倾斜不会自己消失,
  而**没有任何机制**会把 extent 从满盘搬到空盘。
- **为什么值得单独做**: 跨节点搬迁要过网络、要占恢复配额、要和真恢复抢优先级;
  **机内盘间搬迁是本地拷贝** —— 不过网络、不动副本数、不需要 manager 参与、失败只是白拷一次。
  风险比跨节点低一个量级。HDFS 正是因此把 `hdfs diskbalancer` 和集群级 `hdfs balancer`
  做成两个独立工具。
- **但先量再做**: 改动前 `choose_disk` 是 `HashMap` 随机序的 first-fit,**每个 shard 落在
  各自随机一块盘**,所以多 shard 节点本来就是散开的 —— 真实倾斜有多大**未测**。
  先量:每节点逐盘的 `extent_bytes` 极差(df 里已有),看它是否值得一个搬运机制。
- **Scope(量完确认值得再做)**: EN 本地的一个后台任务,把 extent 从最满的盘拷到最空的盘,
  限速、可中断;`ExtentEntry.disk_id` 与 `.meta` 同步更新;整个过程不改变 manager 视图
  (副本数、成员、eversion 都不动)。
- **Acceptance**: 人为造一个盘间倾斜的节点 ⇒ 收敛;搬迁过程中该 extent 始终可读;
  中途杀 EN ⇒ 重启后不留半拉文件、账目一致。
- `passes: false`

### F-EN-SHARD-AUTO — default EN shard count to CPU cores (format-side), not a hand-set env
- **Trigger** (2026-07-13, user: "EN 分片确实是核数导向,但目前是手动 env,不是自动...对于集群配置有好处,记下来,以后做"): EN sharding IS core-oriented — `AUTUMN_EXTENT_SHARDS` should track io_uring cores (one shard = `extent_id % shard_count`), but it's a MANUAL env (default 1). Operators must hand-count cores AND keep three things in lockstep. It is NOT a simple "read `available_parallelism()` in the EN" because shard_count is coupled through a chain: **(a)** EN ports are static/registered-once — `autumn-op format --shard-ports <csv>` stamps the N ports into etcd and the manager routes by that list forever (stream CLAUDE.md "EN ports are FUNDAMENTALLY static"); a runtime-auto shard count would desync from etcd → manager black-holes shards 1..N. **(b)** the k8s overlay Service must enumerate exactly `shard_count` data+control ports (`9101+i*10` / `10101+i*10`); auto-shard needs the Service port list generated too. **(c)** `AUTUMN_EXPECT_NODES` / presplit sizing are tuned against the shard fan-out.
- **Scope (when triggered)**: make the CORRECT layer (deploy/format, NOT the Rust EN process) default the shard count to cores when unset — entrypoint.sh: `AUTUMN_EXTENT_SHARDS` unset → `nproc` (clamped to a sane max); `autumn-op format` auto-derives `--shard-ports` from it; the k8s overlay generates the per-pod Service port list from the same value (kustomize can't loop → a small generator or documented N-port template). Keep the manual env as an explicit override. Rust EN stays config-driven (no `available_parallelism()` read in-process — the ports must match etcd, which only `format` knows). Cross-ref stream CLAUDE.md "serve_with_control is fail-stop … EN ports are FUNDAMENTALLY static".
- **Acceptance**: a fresh deploy with no `AUTUMN_EXTENT_SHARDS` set brings up one shard per core, `format` registers the matching ports, the Service exposes them, and the manager routes to all shards; the manual env still overrides.
- **Status**: `passes: false` (2026-07-13) — recorded for later per user. Deploy/format-layer change (entrypoint + format + overlay), NOT an EN-process change; the coupling chain above is the reason it's "manual by design" today, not a bug.

### BUG-KVC-POOLNAME-STR — `str(PoolName.KV)` 在 py≥3.11 得到 `'PoolName.KV'` 而非 `'kv'`
- **Trigger** (2026-09-04, fable 评审 L3 接口解析改动时顺带发现，**在本次改动之外**):
  `PoolName` 是 `(str, Enum)`。py≤3.10 的 `str()` 返回值 `'kv'`，**py≥3.11 返回限定名
  `'PoolName.KV'`**（`enum` 的 `__str__` 在 3.11 改过）。`sglang_backend.py` 有三处
  假设了前者：~398 的注释（"the KV pool's segment is 'kv', which is what
  `PoolName.KV` stringifies to"）、413 的 `str(pool_name) == DEFAULT_POOL_NAME`
  比较、447 的 `hit_count = {str(PoolName.KV): kv_pages}` 键名。评审在 3.9.6 与 3.13.8
  两个版本上实测确认。
- **影响范围（未查证的那半）**: 取决于调用方往 `transfer.name` 里放什么。已知的调用方
  都传**普通字符串**，那条路不触发。sglang 是否会送 enum 成员进来 **未查证** ——
  评审只能确认 controller 是从上游调用者转发 `transfer.name` 的。若会，后果是 v2 的
  KV key 落到 `"PoolName.KV"` 这个段下（与 v1 不再字节一致，跨版本读不到），
  且 `batch_exists_v2` 的结果字典键名对不上。
- **Scope**: 三处统一改成取 `.value`（或 `PoolName.KV.value`），并加一条断言/单测把
  "v2 的 KV 段必须与 v1 字节一致"钉住 —— 那是 v2 设计时明确写下的性质
  （"v2 keys for the KV pool are byte-identical to v1's — this is additive, not a migration"）。
- **Acceptance**: 先查证 sglang 是否真的传 enum 成员（传字符串则本条降级为整洁性修补）；
  修后在 py≥3.11 上 v1/v2 的 KV key 逐字节相同。
- **Status**: `passes: false` (2026-09-04) — 既有缺陷，非本轮引入。已知调用方传字符串
  而非 enum 成员，所以现在不触发；严重度取决于上面 sglang 那半的查证结果。

### F-CHAOS-DISK-FAULT — 多盘形态有了，但"一块盘坏了"本身还没有 nemesis
- **Trigger** (2026-09-10，fable 评审确认): chaos 的 EN 现在是多盘的
  (`AUTUMN_CHAOS_DISKS_PER_EN`,默认 2),既有 nemesis 因此第一次跑在多盘形态上。
  但**没有任何 nemesis 会弄坏一块盘**,而且两块"盘"是同一个 tempdir 下的子目录、
  同一个文件系统 ⇒ `disk_stats()` 返回相同数字、全程都是 Online。
  所以多盘真正覆盖到的只有:多目录 format/注册、重启时跨盘 `load_extents`、
  以及 `choose_disk` 的挑选与打平(open/held/last_picked)。
  `Full` / `Faulted` 和"只重建那块盘上的 sealed slot"**依然零 chaos 覆盖**。
  (我第一版把这三样都写进了 docs/ops.md 和结构体注释的"已覆盖"里,是**夸大**,
  评审指出后已改成如实描述 —— 记在这里因为这正是账本存在的意义。)
- **两条已核过的约束,写下来免得下一轮重新踩**:
  1. **不能用"杀掉 EN → chmod 000 该盘 → 重启"**。`read_and_verify_cluster_id`
     (crates/server/src/bin/extent_node.rs)在启动时读每个盘的 `cluster_id`,读不到就
     `bail!` —— 整个进程起不来。那是杀节点,不是坏一块盘。
  2. **Faulted 粘到进程重启为止**(docs/ops.md 的失败盘 runbook)。所以这个 nemesis
     **不可逆**,不能当成每 tick 都能挑中的普通动作 —— 要么做成一轮一次的 one-shot
     (像 decommission),要么给它自己的预算门,保证任何时刻活着的盘数仍能满足 RF。
- **Scope**: 对一个活着的 EN,把它**某一块**盘的 hash 子目录改成不可写(`chmod 0500`),
  让下一次在该盘上建 extent 得到 EACCES —— 走 `classify_disk_error` 的 Media 臂
  (EACCES 既不在 Capacity 也不在 Process 名单里),EN 把该盘置 Faulted 并在 df 里
  上报 `online:false`。注入前先快照该盘上的 extent id(每个 disk 目录下的 `extent-*.dat`)。
- **Acceptance**: 注入后 (a) 该节点**仍是集群成员**、没有被 fence;(b) 快照里那些
  extent 的该 slot 在限流预算内被重建到别处(manager 的 extent 布局不再指向这台的这块盘);
  (c) 该节点**另一块**盘上的 extent 一个都没被搬;(d) 轮末的逐键校验零丢失。
  没有 `apply_df_disk_health` + 恢复门那条 per-disk 臂时,(b) 必须是红的。
- **Status**: `passes: false` (2026-09-10) — 本轮只交付了多盘形态本身;盘级故障注入
  是独立的一件事,且因为不可逆需要自己的预算设计,不适合顺手塞进同一个改动。

### F-EXTENT-PLACEMENT — extent 分片放到哪台，两条路径两套策略，且都不看均衡
- **Trigger** (2026-09-05，AZ 迁移实测暴露): 迁移中发现"逐台下线"会让数据**回流到还没下线的
  机器上**。查证后发现根因不是迁移顺序，而是**同一个问题在代码里有两套互不相干的答案**。
- **事实（已核对代码，非推断）**:
  - **分配路径** `select_nodes`（`manager/src/lib.rs:3752`）: 三层回退
    spacious（healthy 且不在 `space_low` 里）→ healthy（Online 且至少一块 online 盘）→ all，
    **每层都 `pool.shuffle(&mut rng)` 后 `take(count)`** ⇒ 均匀随机。
  - **恢复路径** `dispatch_recovery_task`（`manager/src/recovery.rs:406`）: 过滤掉
    `occupied`（该 extent 现有成员）与 `hard_excluded`（fenced/maintenance/suspected）之后
    **`all.sort_by_key(|n| n.node_id)`**，然后 `for candidate in &candidates` **顺序取第一个
    过限流的** ⇒ **小 node_id 优先、首个命中即用，没有 shuffle**。
  - 两者都**不看已用容量、不看分片数**。`space_low` 只是一个二值的"快满了"信号，
    不是负载度量。
  - **没有 extent 级的再平衡**。`autumn-op rebalance` 搬的是 **partition 在 PS 之间**的分布，
    与 extent 分片在 EN 之间的分布无关。所以一旦倾斜，只有 fence 才会重新洗牌。
- **实测代价（本次迁移，数字真实）**:
  - 稳态倾斜: 全是同规格 i3s.3xlarge（3.5T），却是 node 3=45 / node 83=12，**3.75 倍**。
  - 排空 node 5 的 27 个分片时，**12 个搬到了 node 1 和 3 上——那正是接下来要下线的两台**；
    而 4 台全新的空节点（102/104/106/108）**一个都没接到**。
    机制: 要下线的旧节点 ID 最小（1/3/5/7）＜ 保留节点（9/83/85/102+），
    首个命中即用 ⇒ 旧节点永远优先中签；`per_target<=2` 的限流是唯一让它溢出到高 ID 的力量，
    这也解释了 83/85 为什么能分到一些而 102+ 完全分不到。
  - 把四台**一起 fence**（fenced 进 `hard_excluded`，从候选里彻底剔除）之后立刻改观:
    新四台 0 → 37 个分片，并发 4 → 12（7 个目标各自吃到 `per_target<=2` 的额度）。
- **Scope**:
  1. **两条路径统一到一个放置策略上**。至少要看"该节点已持有多少分片 / 已用多少字节"，
     让选择偏向轻载节点。随机能避免系统性偏置但**不收敛**（balls-in-bins 的方差是固有的）；
     升序 ID 则是**主动的系统性偏置**，比随机更糟。
  2. **手动放置**: 允许运维指定某个 extent 的某个 slot 落到哪个节点，
     形如 `autumn-op place-shard <extent_id> --slot N --node M`（走 admin token）。
     用途是迁移、腾机器、绕开坏节点——现在这些都只能靠 fence 间接影响，粒度太粗。
  3. **extent 级 rebalance**: `autumn-op rebalance-extents [--max-moves N]`，
     把分片从重载节点搬到轻载节点。缺了它，扩容之后新机器只能靠"等别人 fence"才会被用起来。
- **⚠️ 设计约束（不要绕过）**: 放置必须继续尊重 `occupied`（同一 extent 的两个 slot 不能落在
  同一节点上，否则 K+M 的容错度直接下降）、`hard_excluded`，以及 EC 的
  `K+M` 个不同节点的下限。手动放置尤其要在**服务端**校验这些，不能只靠调用方自觉。
- **Acceptance**:
  - 造一个倾斜集群（N 台空 + M 台满），触发一批 recovery，断言分片流向轻载节点，
    且最终各节点分片数极差 ≤ 某个阈值；**消融: 换回 `sort_by_key(node_id)` 该断言变红**。
  - `place-shard` 指定一个合法目标 → 分片确实落在该节点；指定一个已持有该 extent 其它 slot
    的节点 → **服务端拒绝**并说明原因。
  - `rebalance-extents` 在一个人为倾斜的集群上收敛，且过程中 `extent-health` 始终干净。
- **Status**: `passes: false` (2026-09-09) — **Scope 1 已实现,Scope 2/3 未动**。
  Scope 1 落地形态(与用户 2026-09-08 讨论定稿,选 B 档:manager 只选 node,盘由 EN 自己选):
  - 新纯模块 `crates/manager/src/placement.rs`。分数是**分层带宽比较**而非加权和 ——
    加权和需要一个"十个 open tail 值几个百分点的盘"的跨单位常数,那个数只会被调到测试通过
    为止;分层每层可单独测,以后插一级不用重调其它。层次:利用率(5 个百分点一带)→
    open extent 数 → 分片数。
  - **分配**(`select_nodes`)用 d=2 抽样,**恢复**(`recovery_candidate_order`)用全排序。
    分开的理由是评审逼出来的:恢复路径上 `RecoveryRateLimiter` 的 `max_per_target=2`
    加"被限流就跳下一个"本来就在分散突发,抽样只是白付准确度(7 台 3 满时约 1/7 的首选
    仍落在满节点)。抽样只在没有限流器的分配路径上有价值。
  - 负载来源**零新增采集**:`cluster_cap.per_node`(逐盘求和,每 df tick)+ per-node
    open/shard 计数(搭在 `logical_stored` 那趟 30s 分块游标扫描上,整圈完成才发布)。
  - EN 侧 `choose_disk` 从 first-fit 改成看负载(单独 commit)。
  - 消融三条各自变红:恢复换回 `sort_by_key(node_id)`;盘排序里把 open 计数降到 held 之下;
    去掉 `last_picked` 盖戳。
  验收里"各节点分片数极差收敛"这一条**未做** —— 它需要 Scope 3 的再平衡器才能成立,
  Scope 1 只决定新数据去哪,存量倾斜不会自己消失。已做的是"分片流向轻载节点"+ 消融。
  当前可用的替代手段是
  **把要腾空的节点全部一次性 fence**，靠 `hard_excluded` 把它们从候选里剔除，
  从而避免数据回流；这次迁移就是这么做的，有效但粒度粗，且解决不了稳态倾斜。

### F-EC-STARVES-FOREGROUND-APPEND — 一个 EC 转换就能把前台写入饿到超时
- **Trigger** (2026-09-11，追一次分区卡死时量到): 单个 16 GiB extent 的 EC 转换
  把协调节点推到 **952% CPU**(9.5 核)，同一时刻该 EN 的 append fanout 从
  **亚毫秒涨到 300-2454 ms**，六秒内越过 size-scaled deadline 触发软错误。
  节点盘只有 39% util、队列不深 —— 不是带宽饱和，是 **每 stripe 在每个目标节点
  `pwrite 64 MiB + sync_data`**，与 append 路径的 per-burst `sync_data` 抢
  fsync 串行点。
- **现有限流为什么不够**: `ec_convert_parallelism` 默认 1 —— 已经是"每节点一个"了。
  限的是并发数，不是这一个转换消耗的资源。`crates/stream/CLAUDE.md` 里
  "No bytes/s rate cap on EN(deliberate)"那段的论据是"并发上限就够"，
  这次的数据是对那个论断的反例。
- **严重性**: 写路径的三个 bug 修好之后它**不再致命**(不会再把分区卡死)，
  但仍然让写入变慢，并且是那三个 bug 当初能被触发的压力来源。
- **Scope**(未实现): 给 EC 转换一个 **bytes/s 节流**或让它的 stripe fsync
  与前台 append 分离(例如降低 stripe 大小、或让转换走独立的 fsync 节奏)，
  使前台 append 的 p99 在转换期间不越过 deadline。
- **Acceptance**: 在一个持续写入的分区上跑一次 16 GiB EC 转换，
  append fanout 的 p99 不超过 size-scaled deadline 的一半；转换本身允许变慢。
- **Status**: `passes: false` (2026-09-11) — 仅立账。

### F-MERGE-REPLAY-OUT-OF-RANGE — 重放不按 range 过滤，可能让已分离的孩子重新带上兄弟的 key（未复现）
- **Trigger** (2026-09-27，merge 分离闸门的独立评审提出，推断): merge 现在要求两侧
  `has_overlap == 0`（`MSG_MERGE_FREEZE` 拒绝），这个标志只从 SST 算（split 时与 open 时）。
  但 reopen 的 WAL 重放**不做 `in_range` 过滤**，唯一的跳过是 `ts <= extent_dedup`
  （`partition-server/src/lib.rs` `recover_partition` 重放段）；而每个来源的 dedup 取该
  checkpoint 记录里**仍能解析**的 SST 的 `last_seq` 最大值，解析不到时为 0，major compaction
  丢掉最新条目又会压低输出表的 `last_seq`。推断链：祖先有 ≥2 个非空 meta extent（或 meta
  尾被 roll 过）⇒ 最早的 checkpoint vp 在后来的父记录之前 ⇒ 孩子 major compaction 之后那些
  区间按 0 或偏低的值去重 ⇒ 任何一次 reopen 都把兄弟那半的父记录重新插进孩子的 memtable
  ⇒ `has_overlap` 仍是 0，下一次 flush（包括 merge freeze 自己的 drain flush）写出带越界 key
  的 SST ⇒ merge 放行后与本次修掉的现象相同。
- **已做的复现尝试（阴性）**: split → 两侧写/删 → 两侧 major compact → PS 优雅重启 / SIGKILL
  重启 → merge → 读，D/K/J/R 全部正确（一次父 flush、一个 meta extent 的形状，最早 vp 在所有
  父记录之后，构造不出推断链的前提）。另观察到：一侧的删除若是它 checkpoint 前的最后一条
  记录，未设闸门的 merge 之后该 key 读 NOT_FOUND——与"被 compaction 丢掉的 tombstone 在
  reopen 时被重放"一致，是同一机制的无害一面。
- **Scope（复现之后才谈）**: 造出 ≥2 个 meta extent 的祖先（强制 meta roll，或多级 split），
  走完上面的链；若坐实，根因修法是重放时对每条记录做一次 `in_range(rg)` 过滤（同时去掉被
  重放的 tombstone），不是再加一个标志。
- **Acceptance**: 一个确定性复现（或一份说明为什么前提不可达的分析），据此决定修不修；修则
  消融能变红。
- `passes: false`

### BUG-MERGE-FREEZE-PS-RESTART — freeze 应答后、merge 提交前 PS 重启，重启后接受的写会被 merge 丢掉（推断，未复现）
- **Trigger** (2026-10-06，F-REVIEW-V1-MERGE-REPLAY 的 fable 评审): freeze 只存在 PS 内存。某一侧回了 freeze OK 之后崩溃并重开（`frozen_for_merge = None`），manager 还在抓 6 个 commit_length 或提交事务；重开的分区接受客户端写。merge 提交后，合并打开从最新的源 cursor（victim 的）开始重放，survivor 这段写不会被重放，即使被读到也会被并集 max_seq 跳过。`admin-merge:S:V` owner key 只是 epoch bump，不 fence 这个 PS。
- **Scope**: 先复现（PS 子进程：freeze OK 后 SIGKILL，重开，写，再让 manager 提交）。坐实后从根因修：让 merge 提交能发现某一侧已不在它冻结时的状态（例如 fence 源分区的 owner epoch，或在事务里校验源分区自 freeze 以来没有重开），不是加超时。
- **Acceptance**: 确定性复现，或说明前提不可达的代码证据；修则 ACK 数据全部可读，消融变红。
- `passes: false`

### BUG-MERGE-STALE-ROLLBACK-UNFREEZE — 一次 merge 的回滚可能解冻另一次并发 merge 以为冻住的那一侧（推断，未复现）
- **Trigger** (2026-10-06，同上评审): `MSG_MERGE_FREEZE{freeze:false}` 无条件清 `frozen_for_merge`；`acquire_owner_epoch` 只 bump epoch，不串行化同一对分区的并发 merge（`handle_merge_partitions` 里"two concurrent merge attempts ... serialize on the manager"的注释不准确）。A、B 两次同对 merge：B 的 freeze 命中"already drained-frozen"拿到 OK，A 失败回滚把这一侧解冻，B 继续抓 commit_length 并提交，期间该侧已在接受写。
- **Scope**: 先复现（两个并发 `MSG_MERGE_PARTITIONS`，让 A 在 freeze 后失败）。坐实后让解冻只作用于发出它的那次 freeze（freeze 带 attempt 身份），或在 manager 侧真正串行化同对 merge；同时改正注释。
- **Acceptance**: 确定性复现或不可达证据；修则 B 提交时 ACK 数据全部可读，消融变红。
- `passes: false`

### BUG-MERGE-COMMIT-DEADLINE-BEFORE-TXN — 提交截止时间在 etcd 事务前检查，事务本身无上限（推断，未复现）
- **Trigger** (2026-10-06，同上评审): `MERGE_FREEZE_COMMIT_DEADLINE`（15 s）在 `handle_multi_modify_merge` 之前检查；etcd 事务本身没有截止时间。事务若超过 `FREEZE_TTL`（30 s）才落地，PS 已自动解冻并在旧尾部继续 ACK 写，merge 按抓到的长度 seal，这些写在 sealed length 之后，丢失（与重放去重无关）。
- **Scope**: 先复现（在事务前后注入 etcd 延迟，或 manager 侧暂停点放到事务内）。坐实后从根因修：让提交在 PS 解冻后不可能成立（例如 PS 解冻时 fence 掉 merge 的 owner epoch），而不是再加一个超时。
- **Acceptance**: 确定性复现或不可达证据；修则 ACK 数据全部可读，消融变红。
- `passes: false`

### BUG-MERGE-FREEZE-REPLY-LOST — freeze OK 在网络上丢失时，那一侧冻到 FREEZE_TTL（推断，不丢数据）
- **Trigger** (2026-10-06，同上评审): PS 已把 OK 交给连接（`succeeded && delivered`），但回复没到 manager（30 s `call_timeout` 恰等于 `FREEZE_TTL`，或回复写出时连接断）。manager 视为失败，回滚列表不含这一侧；PS 保持冻结直到 TTL，期间拒写。不丢数据：已排空、全程拒写，重试命中"already drained"是合法的。
- **Scope**: 量一下实际影响（一次 30 s 拒写）再决定是否修；修法候选：manager 回滚时也给失败的一侧发 `freeze=false`（它可能已冻住）。
- **Acceptance**: 复现一次回复丢失后该侧在有界时间内恢复可写；消融变红。
- `passes: false`

### F-SPLIT-CARRIED-BYTES-UNBOUNDED — 大 value 分区可以无限长大，而现在没有任何判据会说话
- **Trigger** (2026-09-11，本次 split 判据重设计的直接后果): 硬 size 触发器改读
  **LSM 常驻字节**(`size_bytes`)之后，一个大 value 分区的携带字节(log_stream 里的
  payload)**不再是任何 split 的触发条件**。这是故意的 —— split 切的是 key range，
  CoW 之后两个孩子仍然指着同一批 log extent，不 major compact 就分不开 ——
  但它留下一个没人回答的问题:一个 0 iops、0 B/s、LSM 0 MiB、携带 73 GiB 的分区
  **可以一直长下去**，而三个速率维度全都是静的。
- **需要先量，再决定要不要做**: 携带字节真正影响的是**分区级操作的时间** ——
  reopen/replay、rebalance 搬迁、它三条 stream 的恢复。没有测量说明这些在 73 GiB
  会变差到什么程度，也没有说明拐点在哪。**在量出来之前不要加判据**
  ([[feedback_no_defensive_fixes_for_imaginary_bugs]]:"未来可能挂"不算 bug)。
- **而且即使量出来变差了，split 也未必是解药**: 切一刀不减少携带字节(共享 extent
  照旧)，要等 major compaction 重写 + GC 搬走活值才真的分开。真正的缓解手段是
  compaction/GC/EC，或者把分区**搬到另一台 PS**，这两条都不是 split。
- **Scope(量完确认值得再做)**: 测 reopen/replay 与 rebalance 搬迁时间对携带字节的
  曲线;若确有拐点，触发的应当是那条真正有缓解作用的动作(rebalance / 强制 GC)，
  并给它自己的 `POLICY_KIND_*`，而不是把携带字节塞回 split。
- **Acceptance**: 一条测量曲线(携带字节 × reopen/搬迁时间)，以及据此做出的
  "做/不做"决定被写进本条。
- **Status**: `passes: false` (2026-09-11) — 仅立账，未实现，且**先量再做**。
- `passes: false`

### F-FUSE-READ-LATENCY-NOT-BANDWIDTH — 每次读约 4ms 固定成本；挂载能到 1.3 GB/s，慢的是加载器的读粒度
- **Trigger** (2026-09-12，用户): 「为什么autumnfs读这么慢？」。一个 vLLM-Omni 扩散服务
  从挂载读 127 GB 权重，冷启动 19 分钟。
- **实测**（Wan2.2 I2V pod，`--direct-read true`、`--direct-io`(硬编码) ON）:

  | 读者 | 守护进程侧每次读 | 吞吐 |
  |---|---|---|
  | `dd bs=8M count=384`（pread） | **930.3 KiB**（3072 次读 / 2791 MiB） | **106 MB/s** |
  | 模型加载（整轮） | 74.4 KiB（1,705,984 次 / 126,993 MiB） | 171 MB/s |
  | 模型加载（前段小文件） | 31.3 KiB | ~40 MB/s |

- **一个已被证伪的诊断，记在这里以免重走**: 曾推断 "`FOPEN_DIRECT_IO` 关掉页缓存 →
  readahead 失效 → 每次读只有 31 KiB → 落在 `BULK_MIN_BYTES`(64 KiB) 之下 → 每读还多走
  一次 PS 代理"，并据此加了 `--direct-io` 开关。**930 KiB 那一行否定了它**：同一挂载、
  同样开着 direct-io，`pread` 读者拿到的远在门槛之上，吞吐仍只有 106 MB/s。加载时的
  31 KiB 是**加载器自身的访问模式**（大量小张量读），不是页错误。改动已整体回退
  (2026-09-12)。两处本仓库既有记录当时就该读到：`docs/ops.md` 的 "`O_DIRECT` 探测成功
  → 加载器留在 `preadv` 路径"，以及 `ops.rs` 里"内核把每个请求钳到 1 MiB、
  `set_max_readahead` is inert either way"的实测注释——930 KiB 正是那条钳制的确认。
- **已做完那两组对照（2026-09-12），结论是挂载不慢，按标题改写如下**:

  并发扫描（8 MiB 读，每流不同文件）:

  | 并发 | 聚合 | 每流 |
  |---|---|---|
  | 1 | 85.4 MB/s | 85.4 |
  | 2 | 234.6 MB/s | 117.3 |
  | 4 | 524.4 MB/s | 131.1 |
  | 8 | **1089.7 MB/s** | 136.2 |

  固定并发 8、变读大小:

  | 读大小 | 聚合 | **每次读延迟** |
  |---|---|---|
  | 64 KiB | 122.4 MB/s | 4.28 ms |
  | 256 KiB | 471.7 MB/s | 4.45 ms |
  | 1 MiB | **1316.2 MB/s** | 6.37 ms |
  | 8 MiB | 1354.1 MB/s | 49.56 ms |

  单流同尺寸: 64 KiB → 16.1 MB/s (4.07 ms)、1 MiB → 113.7 MB/s (9.22 ms)、
  8 MiB → 126.7 MB/s (66.23 ms)。

- **结论**: 每次读有一个约 **4 ms 的固定成本，与字节数几乎无关**（64 KiB 4.28 ms
  vs 1 MiB 6.37 ms）。所以 `吞吐 ≈ 读大小 × 并发 ÷ 延迟`。挂载本身能到 **1.3 GB/s**；
  单流 85–127 MB/s 只是"没有并发去掩盖那 4 ms"。**挂载不是瓶颈，不要再去调它。**
- **于是问题转移了**: 模型加载在 ~8 并发下只有 171 MB/s，因为它的读是 74 KiB ——
  把 4 ms 摊在了太少的字节上。**该查的是加载器为什么发 74 KiB 的读**
  （safetensors 按张量切分？`get_many_*` 把一次大读拆成按 extent 的小读？），
  而不是 FUSE 侧。
- **Scope**（未实现）: 在加载期抓一次读大小直方图（守护进程侧按 size 分桶计数），
  确认 74 KiB 是加载器发出的还是我们内部拆的。若是后者，合并相邻 extent 的读是直接收益。

- **notes** (2026-09-27): 对 **mmap 加载器** 上面"不是页错误"的结论不成立：safetensors
  `load_file` 是 `MAP_PRIVATE`，缺页按预读窗口（当时 128 KiB）同步读，每次缺页一个往返；
  netns+veth netem 复现 pod 的 4 ms/读后，冷加载 108 MiB/s，窗口 4 MiB 时 823 MiB/s。
  已由挂载走页缓存 + `--readahead-kb`（commit 见 git log）处理；`pread` 小块读者那部分结论不变。

### F-FUSE-DAEMON-PREFETCH — 守护进程对顺序读自己并行预取，内核的 READ 从内存答
- **Trigger** (2026-09-27，用户："按上面的设计做守护进程预取"): 单流读者只靠内核预读，同一
  时间只有一个窗口在飞——单向 1 ms（≈ pod 每读 4 ms）单流 `dd` ~1.0–1.1 GB/s、mmap 冷加载
  单核 ~740 / 9 核 ~1300 MiB/s；而同一挂载 8 线程并行 `pread` 能到 1.6–2.7 GB/s、并行预取 +
  加载总共约 1.0 s（~1.9 GB/s）。内核窗口是脆弱的杠杆：多线程缺页时 4 MiB 窗口塌到
  124–188 MiB/s。
- **Scope**: (a) 按 inode 检测顺序读前沿（允许几个交错前沿，连续 ≥2 块才启动；小文件、随机读
  不触发）；(b) 前沿之前并行预取若干块（初值 8 × 8 MiB 在飞），READ 落在已取回的块里就直接
  从内存答；(c) 内存：全局预算（`--prefetch-mem-mb`）+ 每流窗口上限；预算不够时新流不预取、
  退回直读，**读永不因预算阻塞**；块被内核读完即释放，前沿越过、流空闲超时、最后一个 fd 关闭、
  失效（inode 的 generation 变了：`meta_invalidated` 重读、本挂载写、truncate）时丢弃，
  空闲超时释放；预取块是普通堆内存（传输层的注册缓冲池只管接收缓冲，数据会拷进调用方的
  `&mut [u8]`），预算独立计——UCX 下守护进程总内存 = 预取预算 + 每读线程注册池；
  (d) 块记下规划时的 generation，读请求带着当前 generation，不一致即丢，陈旧窗口不大于页缓存
  本身；(e) 按 (ino, 块号) 把块和落在块里的读请求分给固定的读线程，由它用 `ReplyData` 直接
  应答，不在派发线程上拷贝，单个文件也能用上全部读线程；(f) 计数：命中、等待、未命中、
  浪费字节，周期性写日志。
- **Acceptance**: 单向 0/1/2 ms × 单核/9 核加载器 × 内核窗口 1–2 MiB，mmap 冷加载与单流 `dd`
  明显高于没有预取时（目标接近并行预取 + 加载的 ~1.9 GB/s），交替 A/B；4K 随机读延迟与写吞吐
  不回退；N 个大文件同时顺序读时守护进程 RSS 不超预算、预算耗尽时读照常；真挂载脚本 7 项仍过，
  新增"预取进行中另一个挂载改写同一文件"必须读到新字节，关掉失效丢块时变红；
  `fuse_inval_deadlock.sh` 仍过。
- `passes: false`
- **notes** (2026-09-27): 已实现并按用户决定**默认开**合入（`--prefetch-mem-mb 1024`）。验收对照：
  单流 `dd` 0.69 → 1.7 GB/s、vLLM Qwen 7.9 → 4.8 s、真实 MiniMax-H3（vLLM-Omni TP4）243 → 203 s
  （配 `--disable-multithread-weight-load` 197 s）、4K 随机读 p99 不变、真挂载 PREFETCH 在两层
  generation 检查一起消融时变红、`fuse_inval_deadlock.sh` 过。**未达成的两项**：(1) mmap 冷加载
  并非处处更高——零延迟单核 −12%/多核 −37%，合成的多线程 CPU 拷贝 1/2 ms −25%/−38%（开着时内核
  READ 多一倍多、更碎，原因未在内核核实）；(2) "RSS 不超预算"只对预取块成立（预算 128 MiB 时 RSS
  峰值 287 MiB）。写路径不经过预读，未单独复测。

### BUG-FUSE-CACHED-META-AFTER-LEASE-LOSS — lease 丢了之后缓存的 meta / 页没有人再失效（未复现）
- **Trigger** (2026-09-27，挂载改走页缓存那次的独立评审推断，读代码得出): 挂载走页缓存之后
  两个既有窗口的暴露面变大。(a) 心跳 `NotHeld`（manager 在 poll 仍正常时忘了这个 lease，如
  TTL 过期）只删 `held_leases` 条目，不调内核 invalidator、不标 `meta_invalidated`：仍开着
  的 fd 继续信任旧页，之后的写者关闭也不会再推给本挂载。(b) manager 重启后 lease version
  从 1 重来，`inode_cache_needs_reload` 的 `cached < acquired` 比较失效；dentry 仍在（
  `InodeState` 被 lookup 计数钉住）的已关闭 inode 再次打开时，Open 已经现读了 meta
  （`fresh_meta`）却只在未缓存时才用，GETATTR 继续答旧 size。poll 失败路径（manager 重启时
  通常先走到）已经会标记全部 held ino，所以 (b) 只剩"重启期间本挂载未持 lease"的 inode。
  (c) PyO3 `acquire("r")` 在已持写的 inode 上无条件把 `mode` 改成 READ（`python/src/fs.rs`
  acquire 分支）：之后 `write_lease_for` 发 ANON 写，`get_inode` 的"本会话是写者"豁免也失效。
  既有问题，挂载侧读 open 只 `add_ref` 不改 mode。
- **Scope（复现之后才谈）**: (a) 让心跳在 manager 仍在线时收到 NotHeld（停心跳超过 TTL 再
  恢复）；(b) 挂载 A 读后关闭（dentry 保留）→ 重启 manager → B 追加 → A 重开后 `stat`。
  坐实再修：(a) NotHeld 与 poll 失败同样处理；(b) Open 手里的 `fresh_meta` 在 inode 不脏时
  直接替换缓存的 meta；(c) 绑定写 → 读 acquire 只加引用、不降 mode（与挂载一致）。
- **Acceptance**: 各自确定性复现（或证伪），修则消融能变红。
- `passes: false`

### BUG-FUSE-UMOUNT-JOIN-HANG — 普通卸载后守护进程可能永不退出（未复现）
- **Trigger** (2026-09-27，SIGTERM 优雅退出的独立评审读代码发现，既有问题): `umount` /
  `fusermount -u` 之后 `Session::run` 因 ENODEV 返回，`main` 接着 `compio_handle.join()`；
  派发循环只在收到 `FsRequest::Destroy` 或 `rx.next() == None` 时退出。此刻 bridge 的 sender
  还活着（`main` 里的 `bridge`、`Session` 内 `AutumnFs` 的那份），而 `Session::drop`（会发
  Destroy）要到 join 之后才跑。推断（内核知识，未在本机核实）：非 fuseblk 挂载内核不发
  FUSE_DESTROY ⇒ join 永远等下去，进程在没有挂载的情况下每 30 s 跑一次 `periodic_sync`。
  SIGTERM 现在能把它救出来（信号线程发 Destroy）。
- **Scope（复现之后才谈）**: 挂载 → `umount` → 看守护进程是否退出；若坐实，修法是 join 之前
  `drop(session)`（一行），并在脚本里加一条卸载后进程必须退出的断言。
- **Acceptance**: 确定性复现（或证伪），修则消融能变红。
- `passes: false`

### F-FUSE-BIG-IO-TUNING — writeback cache + splice 零拷贝，把大 IO 的 FUSE 开销压进 5%
- **Trigger** (2026-09-16，用户在 FUSE 性能讨论后确认的三件套之一): 实测
  4K 随机读延迟 0.44ms 中 FUSE 跨用户态开销仅占 2–10%（~10–50μs），顺序大 IO 上
  该比例更低；max_read 已 8MB。剩余可白拿的内核侧开关是 writeback cache 与
  splice_read/write（fuser 支持）。
- **Scope**: (a) 挂载选项开 writeback cache（内核页缓存回写），评估与现有
  dirty-inode flush / lease 撤销失效语义 (`evict_revoked_held_leases`、
  `notify_inval_inode`) 的交互——writeback 会把脏数据驻留内核，lease revoke 时的
  内核失效路径必须保证一致性，这是主要风险点；(b) `splice_read`/`splice_write`
  零拷贝路径接入 read_pool/write 管道；(c) 逐项 A/B，无收益即回退（同账本惯例）。
- **Acceptance**: 固定基线对照——大文件顺序读写、8 并发读、写后 fsync 延迟，
  每项开关单独 A/B，收益须超出重复测试波动；lease revoke 期间内核页缓存无脏数据
  丢失（注入 revoke + 读回校验）；与 BUG-FUSE-INVAL-ON-DISPATCHER-THREAD 的
  复现步骤联测不引入新死锁。
- **Status**: `passes: false` (2026-09-16) — 未开工。先于 F-FUSE-IORING-PASSTHROUGH。
- `passes: false`
- **notes** (2026-09-26): BUG-FUSE-INVAL-ON-DISPATCHER-THREAD 已修并关账，它的复现步骤就是
  `scripts/fuse_inval_deadlock.sh`（docs/ops.md「Kernel cache invalidation must not wedge a
  mount」）。writeback cache 会让页缓存多出脏页，联测时这条脚本必须仍然通过。
- **notes** (2026-09-27): 挂载已不用 `FOPEN_DIRECT_IO`，读写都走页缓存
  （写是无 writeback cache 的直写，吞吐与 direct-io 持平 272.8 vs 272.8 MB/s）；`max_background`
  已是 64。本条剩 writeback cache 与 splice；writeback 联测还要加 `scripts/fuse_page_cache.sh`。

### F-FUSE-IORING-PASSTHROUGH — FUSE io_uring 提交 + passthrough 读直达
- **Trigger** (2026-09-16，用户确认): Linux 6.x FUSE 支持 io_uring 提交路径与
  passthrough 模式（读直达底层文件、跳过用户态拷贝），是"用户态实现复杂度 +
  接近内核态大 IO 性能"的折中。fuser crate 已有实验性支持。前置依赖：
  F-FUSE-BIG-IO-TUNING 先行（更小代价先拿大头）。
- **Scope**: (a) 确认目标内核版本支持项（fuse.io_uring 需 6.14+ 档位，passthrough
  需 6.x + 挂载 `-o allow_passthrough` 类选项，先在 H200-1 的实际内核上核对）；
  (b) fuser 实验特性接入评估——读路径 passthrough 要求底层文件句柄与 FUSE inode
  的映射，autumnfs 的"文件"是 KV/extent 聚合而非本地文件，**passthrough 只可能
  作用于本地缓存层或 page-cache 命中路径**，先做可行性 spike 再定形态；
  (c) io_uring 提交替换 /dev/fuse 同步 read/write 循环，与 compio runtime 的
  事件循环共存性（F-COMPIO-UPGRADE 同族问题，共享调研结论）。
- **Acceptance**: spike 阶段——在真实内核上证明 passthrough 对"非本地文件"形态
  可行或不可行，结论写回本条；若可行，A/B 实测大 IO 吞吐与 4K 并发延迟，
  收益须超波动；正确性回归同 F-FUSE-BIG-IO-TUNING。
- **Status**: `passes: false` (2026-09-16) — 未开工；可行性未证，passthrough 与
  非 POSIX 后端的适配形态是最大未知。
- `passes: false`
