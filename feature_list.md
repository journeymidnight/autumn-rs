# autumn-rs feature list — OPEN backlog

**Last updated:** 2026-09-18

**Rules:**
- This file tracks the **OPEN backlog only**. A feature that reaches `passes: true`
  is **DELETED** from here — git history is the record, there is no archive file
  (CLAUDE.md rule 13: 定期清理删除，保持整洁).
- `passes` and `notes` are the only mutable fields after a feature is created.
- Out-of-scope / "v2 再做" decisions must be recorded as proper feature entries
  (F-name + Trigger + Scope + Acceptance + `passes: false`), never as plan-file footnotes.

---

## Active

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
- **Status**: 仅完成计划记录，尚未升级依赖或实测新版。
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

### F-DISK-FAULT-CLASSIFIER — 瞬时错误被判成永久磁盘故障,而代价已经变成整盘搬迁
- **Trigger** (2026-09-10, fable 评审在 F-DISK-FAULTED-REBUILD 里指出): 分类器
  `mark_disk_error_for_extent`(`extent_node.rs`)把**任何**非 ENOSPC/EDQUOT 的错误
  一律置成 `Faulted`,粘住到进程重启为止。十九个调用点,全在持久化/追加路径上。
- **为什么现在才要紧**: 这个分类器当初是为"停止在这块盘上分配"设计的 —— 误判的代价是本地的、
  可逆的。F-DISK-FAULTED-REBUILD 之后,同一个判断会**触发整块盘的数据搬迁**,代价变成
  集群级且不可逆。而它**没有任何去抖**:对照 PS 上报那条路径,需要 60 秒内 3 个上报者
  才肯做一次**仅仅是建议性**的翻转。
- **Scope**:
  1. EN 侧在上报 `online:false` 之前**自证一次** —— 往该目录做一次小写 + fsync,过了就不报;
  2. 给运维一个清除 `Faulted` 的手段(现在只能重启 EN 进程)。
- **⚠️ 与用户 2026-09-09 决定的关系**: 用户明确"Online/Full/Faulted 先做成自动的",
  所以第 2 条(运维动词)属于**以后**;第 1 条不引入新状态,只是让自动判断更可信,不冲突。
- **Acceptance**: 造一次瞬时 I/O 错误(非介质) ⇒ 盘**不**被置成 Faulted、不触发重建;
  造一次持续错误 ⇒ 仍然置 Faulted 并触发。
- **Status**: `passes: false` (2026-09-10) — **只做了一半,且不是 Scope 里的那一半**。
  已做:`classify_disk_error` 拆成三类,`Process`(EMFILE/ENFILE/ENOMEM)完全不碰盘的健康。
  依据是读代码核实的可达路径 —— `write_meta_locked` 的 open 失败会带着
  `Too many open files (os error 24)` 经 `handle_fence_extent` 直接进分类器。
  **未做**:Scope 1 的"上报前自证"。一次瞬时的 EIO 或 `.tmp` 被清掉导致的 ENOENT
  **仍然会永久判死并触发整盘搬迁**,而账本的 Acceptance 说的正是这一类。
  同轮评审还抓到我在这次改动里**新引入**的一个反向 bug:`msg.contains("os error 12")`
  子串命中 errno 120~129(含 EREMOTEIO —— 块层 `BLK_STS_TARGET` 的映射,正是要抓的设备错误),
  会让真设备故障**静默留在 Online**。改成带右括号匹配,并加了遍历 errno 1..=133 的回归测试
  (白名单的价值全在边界上,只测想到的那三个值不算数)。
- **Status** (2026-09-18): Scope 1 已交付。`Media` 类不再直接判死,先做一次
  `probe_write`(小写 + fsync,探失败 extent 自己的 hash 目录,先 unlink 再写,
  open/unlink/write/file-fsync/dir-fsync 整体 2s 上界);探针通过 ⇒ 保留盘健康(原操作照常失败),
  探针失败或超时 ⇒ 才置 `Faulted`。验收两半都有测试:瞬时 EIO ⇒ Online **且 `df` 仍报 online**
  (即不触发重建),注入 fsync EIO ⇒ Faulted;消融(去掉探针)两条测试变红。
  Scope 2(运维清除 `Faulted` 的动词)仍按 2026-09-09 的决定押后,不在本轮。
  **待测假设**(评审提出,本轮判定不改行为):探针超时算作确认故障,理论上一块"健康但饱和"的盘
  fsync 超过 2s 就会被判死。但这不是回归 —— 改动前 Media 错误**不经任何探针直接判死**,
  新判死集合是旧集合的子集,探针只会减少判死。
  空载基线实测(2026-09-18,真实 NVMe /data05,300 次同序列 open→unlink→write4K→fsync→dir-fsync):
  p50 0.14ms / p99 0.43ms / max 1.83ms —— 2s 预算是最差观测值的 1000 倍以上,
  要误判需要 fsync 劣化三个数量级。饱和态尾延迟未测(本机无 fio 且多租户,压测会影响其他人),
  且它属于"能否比旧行为更宽容"的增强,不是本次引入的缺陷。
- `passes: true`

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

### F-WIRE-VERSION-BY-HAND — 指纹已删除，wire 版本号改为纯手工维护
- **决定** (2026-09-05，用户): 删掉 schema 指纹与 registry 测试，`WIRE_VERSION_MIN/MAX`
  由人维护。已实施：`crates/rpc/build.rs` 删除、`syn` 构建依赖移除、
  `WIRE_FINGERPRINT` / `WIRE_VERSION_FINGERPRINTS` 清空、
  `GetClusterIdResp.wire_fingerprint` 字段摘除（并进未部署的 v36）、
  `wire_compat_check` 改为纯版本区间。五个 schema 文件顶部各加了警示横幅。
- **放弃了什么，说清楚**: 指纹独占的能力只有一个——抓「改了 schema 却没抬版本号」。
  其余全部由区间覆盖（例如那次陈旧 python wheel，它的 MAX 更低，区间检查本就会拒）。
  这个能力现在**没有任何东西替代**，`compat_no_longer_verifies_the_peers_schema`
  这条测试就是为了把这个洞留在代码里可见，而不是只存在于某个 commit 说明里。
- **为什么仍然合理**: 字节哈希造成过真实停机（翻译一条中文注释劈开了滚动中的集群），
  而误报会训练出「刷新记录值继续」的反射——那正是真实改动被放行的路径。
  且 `MIN=MAX` 的停机纪律意味着**不会存在混版本集群**，而混版本正是静默损坏的发生条件。
- **调研结论（2026-09-05，回答"要不要换掉 rkyv"）**:
  - **rkyv 官方不做 schema 演进**。docs.rs 自陈 "lacks a full schema system…
    isn't well equipped for data migration and schema upgrades"；
    issue #164 "Schema evolution" 2021-07 开，至今 open，标签是 "new crate"。
  - **作者自己的 protoss（rkyv 的 schema 演进 crate）已于 2024-11-11 归档**，
    共 6 个 commit、无 release。生态里没有可用方案。
  - 没有搜到任何用 rkyv 做**网络协议**并公开版本管理方案的项目。协议层主流是
    Cap'n Proto / FlatBuffers / protobuf，**三者都把演进做进格式本身**（字段编号 /
    可选字段），所以不需要外挂版本号。rkyv 没有 tag，解码就是按当前 Rust 布局读，
    **忘记抬版本 = 静默读错**，而 protobuf 里最多是丢个字段。
  - ⇒ 这套 `WIRE_VERSION` 不是过度设计，是在补 rkyv 缺的那一层，且无先例可抄。
- **若将来要迁移（实测规模，非估计）**: 206 个 `Archive` 类型
  （manager 144 / partition 45 / extent 16 / cap_token 1）、
  `rkyv_encode` 852 处 + `rkyv_decode` 533 处。
  **FlatBuffers 是错的方向**：它的全部价值在零拷贝访问器，而本树几乎不用——
  `Archived*` 直接读只有 1 处，`rkyv_decode` 是 memcpy 到 `AlignedVec` 再完整反序列化成
  owned，大数据则走帧的裸 tail 完全绕开 rkyv。为一个用不上的能力付 1,385 个调用点的改造。
  **prost 才贴合现状**（解码产物就是 owned struct，多数调用点只换函数名），但迁移的触发
  条件应该是「需要滚动升级、不能再停机」，而不是现在——停机纪律已经挡住了实际风险。
- **`rkyv_decode` 改零拷贝的阻断点（2026-09-05 查清，未做）**: `HEADER_LEN=10` +
  `CTRL_PREFIX_LEN=4` ⇒ rkyv 载荷从帧内偏移 **14** 开始，既非 16 也非 8 对齐。
  那次 memcpy 到 `AlignedVec<16>` **正是让 `rkyv::access` 合法的前提，不是浪费**。
  要零拷贝必须先把帧头补齐到 16 对齐（wire 改动）。且 533 个解码点的类型全变
  （`String`→`ArchivedString` 等），而许多点拿到数据后立刻 clone 进 owned 结构、
  零拷贝买不到东西。**并且没有任何测量指向解码是瓶颈**——今天测到的是 append 0.185 ms /
  端到端 1.15 ms，PS 处理那 ~0.97 ms 的构成完全空白。要做应先量。

### BUG-WIRE36-UNDEPLOYED — main 上有一个未部署的破坏性 wire 版本
- **状态** (2026-09-05 起；2026-09-11 更新版本号): `d6a8b73` 把 `WIRE_VERSION` 抬到
  **36 且 `MIN = MAX`**
  （`MSG_BATCH_DELETE` 是纯加法的 opcode，但本树握手不保存协商结果供调用点门控，
  所以只能 MIN=MAX）。**代码已入库、线上仍是 35。** 此后 main 继续往上走，现在是
  **39**（38→39 = `PartitionLoad.has_overlap` + `NodeCapWire.disks` +
  `GetClusterOverviewResp.ps_servers`）。**这不改变本条的性质** —— 差距变成 35→39，
  那一次停机升级要跨的版本更多，但仍然是同一次停机。
- **⚠️ 这是个陷阱，不是待办**：CP 流水线是从 **main 分支**构建的
  （`sources: branch: main`，`cloneDepth: 1`）。所以在这个 commit 之后，**任何一次
  例行构建产出的镜像都无法加入正在运行的 v35 集群**——握手会以
  "wire-version mismatch" 拒绝。这不会悄悄坏掉（拒绝是响亮的），但会让一次
  本以为无关的构建变成一次意外的停机。
- **部署它需要**: 全部组件同 commit 停机升级 + **重建每一个内嵌 autumn 客户端的镜像**
  （freetoken-l3、hermes-webui、memory-mcp）。
- **在部署之前**，若需要用 CP 构建任何镜像，必须先确认构建的是 `d6a8b73` 之前的 commit，
  或接受这次构建就是那次停机升级。
- **原 Status**（保留）: `passes: false` (2026-09-02；2026-09-04 补机制假说；2026-09-04 机制已实测) —— 仍只立账，不阻塞任何东西：
  需要吞吐的消费者（perf-check / ycsb）本来就多线程，单进程 30K 只影响一次性批量作业的墙钟。
  **本轮没有测量**：验收后半（"改动后吞吐提升可复现"）是延迟敏感的，而本机当时有 2600 个
  sglang/ray 进程、load 8+、热核散布在高低两段，按 `feedback_perf_check_cpu_gate` 隔离不出
  干净的核段 ⇒ 测出来的每操作延迟只会是噪声。等安静的机器或用 cpuset 隔离后再测。

### F-CLIENT-WIRE-COMPAT — 一个版本号同时管内部协议和客户端协议，逼着所有内嵌客户端的镜像跟着重建
- **Trigger** (2026-09-18，用户: 「关于老 stopworld 升级，然后所有依赖都需要 rebuild+升级，似乎不对」):
  `WIRE_VERSION_MIN/MAX`（`crates/rpc/src/lib.rs:104`）把三件本该分开的事焊成了一个常量：
  (1) **持久态**（etcd / SST / WAL）—— 全停全启 + rkyv fail-loud，跟客户端无关，这条是对的；
  (2) **集群内部 wire**（manager↔PS↔EN）—— `MIN=MAX` 代价为零，这三个角色的二进制本来就在
  同一次部署里一起换；(3) **客户端 wire**（SDK / python wheel / fuse daemon / kvcache /
  s3 网关 / vLLM·ComfyUI 镜像）—— **被强行绑到 (2) 上**，这是要修的。
- **证据，不是推演**:
  - 最近一次 bump 42→43（`2b76620`）删的是 `MSG_APPEND_CHAIN`，一个 **EN↔EN 的消息类型，
    客户端从来不发**。但 `MIN=MAX=43` 让每个旧 wheel 在 `ClusterClient::connect`
    （`crates/client/src/lib.rs:1527`）硬失败。为一个客户端看不见的改动，要求所有推理镜像
    重 build + 重 roll。同型代价 `BUG-WIRE36-UNDEPLOYED` 已经记过一次
    （「重建每一个内嵌 autumn 客户端的镜像」）。
  - **「全停全启」这个窗口盖不住客户端进程**。存储那三个角色是我们的；vLLM / sglang /
    ComfyUI pod / 用户挂着的 fuse mount 不是 —— 既停不了，也不该要求跟着存储版本走。
  - **现在两头都不对**：`client.rs:1515` 是 `if let Ok(...)`，取 `GetClusterIdResp` 的
    transport 失败就跳过检查；而服务端**从不校验客户端版本**（`MSG_AUTH_HELLO` 不带版本
    且是 optional，`partition-server/src/lib.rs:3482` 那处 `wire_compat_check` 是 PS 作为
    *客户端*连 manager 时的自查）。所以 manager 短暂不可用时，版本不匹配的旧 wheel 照样连得上
    然后静默乱解码 —— 就是 `part_id = 0` 那个事故的形状（`crates/rpc/src/lib.rs:91`）。
    既太严（无谓的全量重建），又太松（真不匹配时可能漏过）。
- **Scope**:
  1. **枚举客户端面向的消息集**，并把它和内部消息集在代码里分开（不是靠注释约定）。已知它
     不只是 `partition_rpc`：`--direct-read` 默认开，客户端直连 EN 读，所以 `extent_rpc` 的
     **读子集**（`MSG_READ_BYTES_BULK` + `MSG_GET_REDIRECT`/`MANY` 的描述符结构）也在客户端面上；
     再加 `frame.rs`、`cap_token.rs`、`manager_rpc` 的路由/命名空间/authz 子集
     （`MSG_GET_CLUSTER_ID`、`MSG_GET_REGIONS`、namespace/principal/mint-token）。
     **先量代价**：这批结构冻结（只许加 msg_type、不许改字段含义）到底拦掉多少将来的改动，
     在拆常量之前要有个数，否则拆完只是把痛点换了个地方。
  2. **拆成两个区间**：`CLUSTER_WIRE_*` 保持 `MIN=MAX`，内部随便 bump；`CLIENT_WIRE_*` 走
     `MIN = MAX-N` 的兼容窗口。两个区间都仍是手工维护（`F-WIRE-VERSION-BY-HAND` 的定调不变，
     指纹不回来）。
  3. **校验改成服务端强制**：客户端区间随握手/第一帧上报，PS 与 manager 在**服务端**判定不重叠
     即拒；客户端自查降级为提前给出好错误信息，而不是唯一的门。
  4. 演进规则与升级流程分别写进 `crates/rpc/CLAUDE.md` 和 `docs/ops.md`：内部 bump = 只停存储
     三角色；客户端 bump = 需要兼容窗口公告 + 重建内嵌客户端的镜像。
- **Non-goals**: 不做存储集群自身的 rolling upgrade（`project_rolling_upgrade_paused` 的定调不变）；
  持久态仍然是全停全启 + rkyv fail-loud；不换序列化格式。
- **Acceptance**:
  - **内部 bump 不再波及客户端**：把 `CLUSTER_WIRE_*` 抬一级而 `CLIENT_WIRE_*` 不动，用改动前
     构建的客户端对新集群跑 put/get/range/batch/direct-read，字节精确、无拒连。
  - **客户端面改动必须被服务端拦住**：构造一个把客户端区间伪造成窗口外的连接，断言 **PS 侧**
     在任何 Put 落盘之前拒绝（不是客户端自己拒），并且事后读回确认没有写入。
  - **绕过客户端自查的路径不再能写**：模拟取 `GetClusterIdResp` 的 transport 失败（今天会
     `if let Ok` 跳过），断言写入仍被服务端拒绝。这条是今天那个洞的直接回归。
  - **窗口内的旧客户端真的能用**：用窗口内旧 commit 构建的 python wheel（不是伪造区间）对新集群
     跑一遍数据面，读写字节精确。
  - Ablation，逐条确认变红后还原：合回单常量；去掉服务端校验；把 `CLIENT_WIRE_MIN` 拉回等于 MAX。
  - `docs/ops.md` 的升级步骤可执行地区分两类 bump；`crates/rpc/CLAUDE.md` 写清哪些文件/结构
     属于客户端面以及它们的演进规则。
- **Status**: Scope 1（枚举 + 代价测量）已做完，**结论改变了 2/3 的做法顺序**，见 notes。
  **Scope 2 第一步已落地**（2026-09-19，未 push）：常量拆成 `WIRE_VERSION` /
  `MIN_CLIENT_WIRE_VERSION`（删掉 `WIRE_VERSION_MIN`），区间重叠判据拆成 peer 精确相等 +
  客户端两端包含，三个调用点各归各位，manager 用 `MIN_CLIENT_WIRE_VERSION` 填冻结的
  `wire_version_min` 槽位。两常量相等 ⇒ **窗口关着，行为与改动前逐对相同**（评审独立推导
  过旧判据恰好归约为新的客户端判据）。
  **Scope 3（服务端强制）已落地**（2026-09-19，未 push）：`MSG_CLIENT_HELLO`（0x5F，
  手写定长二进制，`client_hello.rs`，与 `GetClusterIdReq/Resp` 一同被 golden-bytes
  冻结）+ manager `client_wire_gate`（解码循环内同步执行，在 per-frame spawn 之前）
  + PS 端并入 `authz_gate`（在 `!gate_active()` 早退**之上**）+ SDK 在
  `mgr_client()`/`get_ps_client()` 每开一条连接发一次 hello 并留住协商版本
  (`negotiated_cluster_wire`)。准入按 msg_type 划界；沉默连接按引入版本 43 对待，
  故对改动前构建的每个客户端与每个内部 peer 都**惰性**。
  **设计没预料到的一条**：`MSG_GET_REGIONS` 同时在两个面上（SDK 路由用它，PS 的
  `sync_regions_once` 也用它），PS 不发 hello，所以它必须留在门外，否则地板一抬就是
  全队 region sync 停摆 —— 正是 msg_type 划界要避免的那个停摆，只是换了条路进来。
  代价：地板以下的客户端仍能拉路由，但它之后发的每个数据面消息都会被拒。要真正堵上
  需要让集群 peer 能自报身份，那是另一件事。两个测试钉住了这条。
  **服务端拒绝在客户端侧必须是终止性的**（评审逼出，新增 `AutumnError::WireVersionRefused`）：
  拒绝来自**开连接**而非调用，所以它落在七个重试循环各自的 connect-error 臂上；
  原先 `connect` 把它吞成「cannot connect to any manager」，数据面则烧满
  `MAX_PS_REFRESHES`（实测 11 次 accept / 28 秒一个 get）再贴上 ConnectionError 标签，
  恰好把拒绝唯一携带的信息（往哪个方向修）丢掉。改成一个 choke point：
  `wire_refused` 由 `say_hello` 写，`refresh_and_backoff` 与新的 `routing_exhausted`
  读——后者替换掉七段复制粘贴的循环尾巴。**不是永久闩**：握手成功即清除，
  且每次调用仍会真的试一次，所以跑在集群前面的客户端在集群部署后自行恢复。
  运维面（autumn-op 的消息）**有意不纳入窗口**：它与集群同 commit 发布；代价是
  陈旧 autumn-op 仍会跨版本解 rkyv，已写入文档而非隐含。
  余下未做：调用点服务两种形式（设计 §7）—— 在那之前 `MIN_CLIENT_WIRE_VERSION`
  抬不起来，窗口仍然是关着的。
- `passes: false`
- **notes** (2026-09-18, Scope 1 完成 — 枚举与代价测量):
  - **客户端面是可枚举的**：236 个 wire 类型里约 61 个在上面。`partition_rpc` 数据面、
    `manager_rpc` 的路由/lease/inode/authz 子集（挂载中的 fuse daemon 就是内嵌客户端，
    `fuse/src/dispatch.rs:1100` 的 statfs 真的会去问 manager 要 cluster-df）、`frame.rs`、
    `cap_token.rs`，加上 `extent_rpc` 的**读子集**（`--direct-read` 默认开，SDK 经
    `read_extent_value_direct` 直连 EN）。EC 转换 / recovery / WriteShard / df / reconcile /
    split-merge / op-ledger / dashboard 都不在。
  - **代价（45 个 wire 版本区间逐个分类）**：三种口径分别得到 16/46、15/45、18/45，
    **稳定结论是约 60% 的 bump 没有改动任何内嵌客户端会解码的东西** —— 删 chained
    replication、EC 分条转换、at-rest 腐化扫描、EN UUID 身份、fence-drain、WAL 自愈、
    GC 可观测性、dashboard，全都逼着每个内嵌客户端镜像重建。精确计数不是承重部分，
    不同口径都指向同一个数量级。
  - **⚠️ 计数是下界，检测器有已证实的盲区**：它看不见"结构没变、含义变了"。`a857084`
    (v34→35) 的提交说明自己写着「no struct change, but a v34 client reads a declined item
    as a per-item error」—— `GetRedirectResp.code` 的语义改动，正落在 SDK 直读热路径上，
    被我判成了"内部"。同类还有 `4a9b336`：它从 `GetClusterIdResp` 删掉了
    `wire_fingerprint`，而 `WIRE_VERSION_MAX` 前后都是 36（改动发生在非 bump 提交里）。
  - **推翻了原条目的一个假设 —— 换编码救不了这件事，还会更糟。** 原本以为"客户端面只许
    加字段"配一个带 tag 的编码就能覆盖大部分。实测 16 次打断里 9 次只是加字段，看起来
    支持这个想法；但逐条核对后，其中 `1de0005` 给 `GetRedirectResp` 加的
    `ec_data_shards` 是个**判别位**，`replica_addrs` 的含义随它改变。rkyv 因为布局移位而
    **响亮失败**；换成 prost，老客户端会忽略这个未知字段、解码"成功"，然后把 EC 分片地址
    当副本地址、**静默**把分片字节当 value 读回去。9 次里只有 1 次（`a8c6afb`
    `AllocInodesReq.volume`）是干净的"prost 本可以救"。
  - **rkyv 加字段的失败是"有时响亮"，不是"总是响亮"**（实测，rkyv 0.8.15，走本树同款
    `AlignedVec<16>` + checked `from_bytes`）：root 在 buffer **末尾**，所以老解码器读的是
    新结构的后缀。`{u64,u64}` 读 `{u64,u64,u64}` 得到 `Ok` 且字段**整体错位**；
    `{u64,u32}` 加一个 `u32` 落进尾部 padding，两个方向都 `Ok`、新字段读成 0;
    只有带 `Vec`/`String` 的形状才因相对指针越界而报错。**"plain rkyv fail-loud 本身就给了
    安全"这句话的适用范围比 [[project_rolling_upgrade_paused]] 记的窄** —— 那条讲的是
    持久态重放，这里讲的是 RPC 跨版本解码，后者的响亮与否取决于结构形状。当前唯一真正的
    围栏是版本握手本身。
  - **⇒ 真正缺的机制不是编码，是客户端把协商结果扔了。** `crates/rpc/src/lib.rs:99` 已经
    写明："the client runs its compatibility check once at connect and keeps nothing, so no
    call site can gate on the negotiated version"。剩下那 ~40% 里真正无法靠编码解决的是
    **行为/语义**改动（v26 key 布局、v34 EC 判别位、v35 decline 语义）和 v28 的帧重塑 ——
    它们需要**调用点按协商版本分支**。没有这个能力，任何兼容窗口都开不出来，换什么编码都
    一样。**所以 Scope 2/3 的前置不是拆常量，是先让客户端留住协商到的版本并使调用点可以
    据此分支**；这一步小且自足，应当先做。encoding 迁移（含 prost）在这条路径上**不做**。
- **notes** (2026-09-18, Scope 2/3 设计已写 — `docs/client_wire_compat_design.md`，代码未动):
  形状 = 一个整数**两个**常量 `WIRE_VERSION` / `MIN_CLIENT_WIRE_VERSION`（后者是唯一因
  "打断客户端"而动的数）+ 连接级
  `MSG_CLIENT_HELLO`（手写定长二进制，非 rkyv）+ 沉默连接按引入版本对待 + EN 那条边靠
  PS 拒发 descriptor 转 proxy（EN 不获得版本概念）。评审逼出的两条决定性修正，别再踩:
  - **`wire_compat_check` 的区间重叠判据对这两个问题是错的，而且错在危险方向。**
    集群 `[45,45]`、窗口内客户端自己 `[44,44]` ⇒ `lo=45 > hi=44` ⇒ **客户端自己拒绝自己**，
    窗口根本开不出来。唯一可行解是让冻结的 `GetClusterIdResp.wire_version_min` 槽位改载
    `MIN_CLIENT_WIRE_VERSION`（已部署的老客户端读这个字段的代码改不了）；但这样一来陈旧 PS/EN
    也会因重叠而被放行 —— **握手本来就是 stop-the-world 的唯一执行者**。所以集群 peer 的
    判据必须改成 `resp.wire_version_max == WIRE_VERSION_MAX` 精确相等，客户端才用区间包含。
  - **拒绝必须按 msg_type 划界，不能按连接划界，否则第一次抬地板就是集群停摆。**
    PS→manager / EN→manager 走 `autumn_stream::ConnPool` 裸连、无任何握手；manager→PS 的
    split/maintenance/merge-freeze/roll-tails 同理。帧里没有任何东西标明角色。连接级拒绝会
    在地板一动时拒掉 `register_ps`、心跳、`register_node`、reconcile。
  - 另外三条已写进文档：`frame.rs` **不在窗口内而是冻结**（帧变了先挂 CRC，拒绝消息都送不
    出去，hello 根本够不着）；PS 侧检查必须放在 `authz_gate` 的 `!gate_active()` 早退**之上**
    （否则 authz-off 集群上永不执行）；新客户端碰到老 PS 时 hello 会因 `extract_part_id`
    的 `_ => 0` 被当成 misroute 返回 `NotFound`，客户端要把它读作"服务端早于 hello"。
  - **常量改名**（用户 2026-09-19，本条 Acceptance 文字按规则 8 不动，此处记映射）：
    上面 Acceptance 里的 `CLIENT_WIRE_MIN` 即现在的 `MIN_CLIENT_WIRE_VERSION`，
    `CLUSTER_WIRE_*` / `WIRE_VERSION_MAX` 即现在的 `WIRE_VERSION`；**只改名，判据不变**。
    原来的三常量拼法里 `WIRE_VERSION_MIN`（集群下限，钉死 == MAX）在"peer 必须精确相等"
    这条定下来之后就是同义反复，已删 —— MIN/MAX 这对名字读起来别扭的根因就是它。
  - **按组件归因的测量**（2026-09-19，用户问"只改一个 API 参数为什么要全停"）: 44 个版本区间里
    真正需要 MGR+PS+EN 三者一起动的只有 **8 (18%)**，EN 本可不重启的占 **30 (68%)**；
    只牵连 PS 的 8 次全是 client↔PS 数据面（本条窗口覆盖），只牵连 MGR 的 9 次另立
    [[F-SCHEMA-HOMES]]。**按"边"发版本已评估并否决**：边不是代码里存在的属性 ——
    `ReadBytesReq` 同时服务 PS→EN、client→EN、EN→EN 三条边，按边拆会塌缩成按消息拆
    （Kafka per-API 模型），而 45 次 bump 里真正打断客户端的只有 7 次，养不起。且它只缩短窗口
    （EN 免 `load_extents`），不消除停机 —— PS 全体重启本身就是不可用。
    口径同上：源码引用是代理指标，且看不见语义改动，数量级可用、单行不可引。
  - **判别器进字节，不进连接状态**（2026-09-19 用户指出 `one_definition_only!` 在 rolling /
    兼容窗口面前不成立后定的）: 客户端面消息的**新版本用新 msg_type**，旧形式保留自己的
    opcode 和结构直到地板越过它。msg_type 在帧头、解码之前就确定，响应沿用请求的 msg_type，
    两个方向都自描述。**不能**复用同一 opcode 按连接协商版本分支 —— 那让一帧的含义取决于
    连接状态，而 rkyv 误解码是静默的，hello 漏了就是静默按错版本解码。新 opcode 在设计 §4
    的新规则下不算 bump，所以这条几乎免费。§3 的"版本属于连接"随之收窄：连接版本只管**准入**
    和**服务端主动推送**（如推给 fuse 的 invalidation），不管收到的字节怎么解释。
  - **`one_definition_only!` 分成两半**: 它抓的是"两份定义而**没有东西决定读哪一份**"
    （当年 `ExtDfReq` 镜像），不是"有两份定义"。集群内部不做 rolling ⇒ 同一消息永不会有两个
    版本同时在线 ⇒ 第二份定义必是镜像 ⇒ **守卫保持原样全力有效**。客户端面按构造就有两个
    版本在线 ⇒ 重述为 **一个 (消息, 版本) 一份定义，且每份只能经由它的 msg_type 到达**。
    我一度说"防镜像机制要延伸到新边界"，不加限定是错的 —— 延伸到客户端面会把窗口锁死。

### F-SCHEMA-HOMES — 五种 schema 应各有明确的家；今天 manager 的持久值寄居在 wire 文件里
- **原则**（用户 2026-09-19 定）: **etcd、wire、客户端三类类型全部拆开，在明确的地方分别
  定义。** 不按"生命周期绑定就不用拆"这种个案判断 —— 那是个每次改动都要重做、并且会烂掉的
  判断，而 `manager_rpc.rs` 里攒下 17 个非消息类型，正是因为从来没有规则说东西该放哪。
  按位置分家之后，"这次改动会不会逼所有内嵌镜像重建"从一道推理题变成"这个文件在不在客户端
  schema 里"。
- **全树盘点：每种 schema 是"谁写给谁"，载体无关**:
  | schema | 写给谁 | 载体 | 自版本 |
  |---|---|---|---|
  | SST / WAL record / checkpoint | 未来的 PS（或接管的 PS） | **EN 的 extent，EN 视为不透明字节** | `MAGIC "AU7B"` + `FORMAT_VERSION` ✓ |
  | `.meta` / `.ck` | 未来的 EN（+ recovery 的另一台 EN） | EN 本地盘 | `EXTMETA\0/\x01/\x02`，三版都还在解析 ✓ |
  | manager 记录 | 未来的 manager | etcd | **无 —— 借 `WIRE_VERSION`** ← 唯一的窟窿 |
  | 集群内部 wire | 活着的 peer | 网络 | `WIRE_VERSION` 精确相等 |
  | 客户端 wire | 活着的客户端 | 网络 | 窗口（[[F-CLIENT-WIRE-COMPAT]]） |
  **PS 对本地文件系统的引用是 0** —— 它的持久态 = manager 记录 + stream 内容。所以改 SST 格式
  是 PS↔未来 PS 的事，EN 根本看不见，`FORMAT_VERSION` 独立于 `WIRE_VERSION` 是对的。
  **五格里三格已经健康**，要动的只有 etcd 一格：它之所以借 wire 版本号，仅仅因为被定义成
  rkyv 结构、住在 wire schema 文件里。这不是要发明新纪律，是去抄 `.meta` 已经在用的那套。
- **代价（2026-09-19 实测）**: 44 个 wire 版本区间里，真正需要 MGR+PS+EN 三者一起动的只有
  **8 (18%)**；**EN 本可不重启的占 30 (68%)**。etcd 借版本号的后果就是这个差额。
- **持久类型权威清单**（来自 `replay_from_etcd`，manager 重放必须解码每一个）: rkyv 的 8 个 ——
  `MgrExtentInfo`(extents/)、`MgrRegionInfo`(regions/)、`MgrStreamInfo`(streams/)、
  `MgrNodeInfo`(nodes/)、`MgrDiskInfo`(disks/)、`MgrPartitionMeta`(partitions/)、
  `MgrNamespace`、`MgrTenantAccount`；另有三个非 rkyv 的 key 无 schema 问题
  （`ownerLocks/` 裸 revision、`psNodes/` UTF-8 地址、`partitionLastOp/` i64 LE）。
  其中 `MgrRegionInfo` 和 `MgrNamespace` 同时被内嵌客户端解码。
- **Scope**:
  1. 8 个持久类型各自在 `crates/manager/src/persist/` 有独立定义 + 自己的魔数/版本字节，
     只有 manager 能引用；与 wire 类型之间是**显式转换**。
  2. **转换必须穷尽**：用结构体解构（`let Mgr… { a, b, c } = x;`，不许 `..`），加字段不写
     转换就编译失败。当年 `ExtDfReq` 镜像的教训是"两份定义而没有东西决定读哪份"，不是
     "有两份定义"；这里判别器是显式转换函数本身。
  3. `MgrRegionInfo` 拆三份：持久（7 字段）、PS 线上（7 字段）、**客户端路由记录（4 字段：
     `rg` / `part_id` / `ps_id` / `region_epoch`）**。客户端生产路径从不读三个 `*_stream`
     id —— `client/src/lib.rs` 里那 3 处只有两条注释加一个被迫编造
     `log_stream: 1, row_stream: 2, meta_stream: 3` 的测试 fixture。拆完顺带止住 stream 层
     id 跨层泄漏到 SDK。`MgrNamespace` 同样处理（它只有 manager + 客户端两个身份，更干净）。
  4. `PayloadLocation` 这类跨 wire/盘 的类型各归各家：盘上那个字节的含义由 EN 的持久 schema
     定义、由 `.meta` 魔数管，不从 wire enum 继承。今天 `from_byte` 的"unknown → InDat,
     never an error"是条**持久化决策长在 wire 类型上**；全停全启 + 不回滚兜着，但回滚会
     静默把分片字节当 value 服务出去。
  5. **补上被搬走的守卫。** 今天"改它就要 bump wire 版本"是**意外**生效的护栏；搬出去不给
     替代品就是拿过宽的守卫换成没有守卫。且 fail-loud 不得假设 —— 见
     [[project_rkyv_add_field_not_always_loud]]，rkyv 加字段是否响亮取决于结构形状。
- **Acceptance**:
  - 改一个已分家的持久结构（例如给 `MgrAuditEntry` 加字段）**不需要**动 wire 版本号，且
    PS/EN 二进制不重新编译即可继续互操作：真集群验，只换 manager 二进制，PS/EN 保持原进程，
    读写与 split/recovery 正常。
  - 给任一持久结构加一个字段而**不改**对应的转换函数 ⇒ **编译失败**（穷尽解构的 ablation）。
  - 旧 manager 写的 etcd 数据被新 manager 就地重放成功；反向按持久侧纪律明确是拒绝还是兼容，
    有测试钉住是哪一种。
  - 每个持久结构有测试证明"加字段后旧二进制重放响亮失败"，或记录它靠什么别的机制兜底。
  - 客户端拿到的路由记录不再包含任何 `*_stream` id（编译期不可达，非运行时断言）。
  - Ablation：把某个分家出去的类型搬回 wire 文件 → 第一条验收转红。
- **Status**: 仅立账，未动工。与 [[F-CLIENT-WIRE-COMPAT]] 的第 3 条 Scope 有交集
  （`MgrRegionInfo` / `MgrNamespace` 的客户端那一份），其余可并行。
- `passes: false`

### F-STREAM-ATREST-CKSUM — stream 层大 value 的 at-rest 内容校验 + scrub（静默腐化 G12）
- **Trigger** (2026-08-04, chaos 缺口 loop 的 G12，已 reproduce-first 复现 harness `crates/manager/tests/silent_corruption_rot.rs`): sealed extent 的 **value 数据字节**在单副本上被静默翻位后，**全链无检测**：(a) 客户端读回坏字节仍返回 `CODE_OK`（frame CRC 明确排除 bulk value 段；`.meta` CRC 只覆盖 40B 元数据；WAL/SST CRC 是 partition 层、不覆盖 stream extent 的原始 value）；(b) recovery 从坏副本重填时 `verify` 只校 `length==sealed_length` + eversion、**不校内容** → 把腐化洗成权威；(c) EC 转换对坏字节直接编 parity → 固化成 canonical。stream 层**既无 per-extent/block content checksum、也无 scrubber**；确定性副本轮转让坏副本被一致选中（harness 里 25/64 子区间读命中）。这是**设计缺口**（数据完整性面），不是坏代码——today 的裸机盘不会自发翻位、且需要单副本静默腐化才触发，故不是"今天可复现的线上危害"，属于中期加固。
- **Scope（真要做时）**: (1) 写侧对 sealed extent 落 **per-extent/block content checksum**（`.meta` 里加一段覆盖 `.dat` 内容的 CRC/xxhash；注意不能进 append 热路径的每帧 CRC，只在 seal 时对最终内容算一次）；(2) EN 读时（至少 sealed 全值读 + recovery 重填读）验内容 checksum，错则走**现有副本轮转/failover 绕开**坏副本（隔离路径已存在，缺的是检测触发器）；(3) recovery/EC 转换前加内容校验，**拒绝**把校验失败的副本洗成权威/编进 parity；(4) 后台 **scrub loop**：低速重哈希 sealed extent，mismatch 则清该副本 `avali` 位交给 recovery 重建。
- **Acceptance**: 用 `silent_corruption_rot.rs` 的注入点——翻转单副本 sealed `.dat` 字节后：客户端读返回错误（非 `CODE_OK` 坏字节）或自动从好副本服务正确字节；recovery 不再从坏副本洗白（重填结果字节精确）；EC 转换对坏副本报错而非编坏 parity；scrub 能在无外部读的情况下自行发现并清 `avali`。harness 从"记录暴露"翻成 fail-until-fixed 正确性断言。
- **Status**: `passes: false` (2026-08-04；2026-09-04 开工) — **增量 1 已落地**：`.ck` sidecar 格式
  （`crates/stream/src/extent_cksum.rs`）、seal 时写、读时验（在 `build_read_future` 上，
  即生产读真正走的那条路），设计见 `docs/autumn_integrity_plan.md`。
  **三条 harness 腿仍绿**，原因写在设计文档里：EN 上没有 seal 事件，那条流程不会写 sidecar，
  要靠增量 2 的 scrub 回填才够得着。剩余：scrub（探测 + 回填 + 经 `DfResp` 上报）、
  EC 转换前置校验、EC 分片的 at-rest 覆盖。
  原始定调保留 — **backlog（用户定调 2026-08-04「g12 放到 backlog 里面」）**：已 reproduce（harness 未提交/已提交见 chaos 套件 `b15168c`），本轮**不实现**，留账本记录。cross-ref memory `project_chaos_gap_loop_findings`（G12 条）。真要动之前先确认触发条件（单副本静默腐化）是否已在真实硬件/线上出现过。

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

### BUG-RECOVERY-MARKER-ORPHAN — unfence 不释放 marker，僵尸占着限流名额且完全不可见
- **Trigger** (2026-09-04，一次 fence→unfence→再 fence 的排干中实测): fence node 5 创建了
  4 个 recovery marker（`replace_id=5`）；**unfence 之后它们全部留存**，既完不成也不释放，
  并且把限流器的名额占死（`per_source: node 5 → 4`、`per_target: 83→2 / 85→2`，
  `global 4/64`）。同期一个**新**派的重建（extent 73）正常跑通、完成、释放——证明机制本身没坏，
  坏的是这四个的生命周期。
- **根因一：释放条件只看「执行者」，从不看「创建它的理由」。** 设计里 marker 的释放是
  事件驱动的两点：`apply_recovery_done`（干完）与
  `release_recovery_markers_for_dead_executors`（**钉定的执行者**不在/非 Online）。
  fence 是**创建**它的理由，但 **unfence 不在任何一条释放路径上**，也没有 wall-clock TTL
  （刻意如此，见 crates/manager/CLAUDE.md 的 Recovery 节，那个决定本身是对的）。
  于是形成僵尸：`replace_id` 指的 slot 已经健康（实测 extent 69 的
  `slot[0] node=5 avali=true auto=Online`），活没意义所以完不成；执行者 83/85 活着，
  所以也不释放。
- **根因二：空转完全不可见。** `redispatch_pinned_recovery`（recovery.rs:305-364）每 2 秒
  重发一次，而它的**三条非成功路径全是 `tracing::debug!`**（refused / decode / unreachable），
  `CODE_OK`（"已启动"和"已在跑"共用）则什么都不打印。manager 跑在 INFO ⇒
  **一个每 2 秒空转的 marker 在日志上一行都没有**。排查时 manager 日志里"没有任何 recovery 派发"
  被我读成了"没在派发"，实际是"发了但看不见"。
- **Scope**: (a) 给 `release_recovery_markers_for_dead_executors` 加一个同形的**电平触发**
  伙伴：每 tick 检查 marker 的 `replace_id`——若该节点 Online、无 override、且它在该 extent 的
  `avali` 位已置——则释放 marker。保持"事件驱动、无 TTL"的既有形状，不引入超时语义。
  (b) 把重发的 refused/unreachable 从 `debug!` 提到 `warn!`，或至少加一个"同一 marker 连续
  N 次未推进"的计数并在 INFO 上说一次——一个永远重发、永不进展的 marker 必须能被看见。
  ⚠️ 注意不要把 (a) 写成"只要不 fenced 就释放"：磁盘故障、`auto_disk` 门控、以及
  corrupt-slot 强制派发都是不看 override 的合法来源，判据必须是**那个 slot 是否真的还需要重建**。
- **Acceptance**: fence 一个节点 → 出现 marker → unfence → **下一个 tick 内 marker 被释放**，
  `recovery-stats` 的 `global`/`per_source`/`per_target` 归零；重复 fence/unfence 十次不残留。
  另：让重发对一个必然失败的目标空转，`autumn-op ops list` 或 manager 日志能在 INFO 上看出
  它没有进展。
- **Status**: `passes: false` (2026-09-04) — 不丢数据（僵尸描述的活是"把一个健康 slot 搬走"，
  完不成反而是安全的），但它**吃掉恢复容量**且**完全不可观测**，两者叠加正是本次排查里
  最误导人的一段：4 个名额被占、日志全静默、而我据此得出过"根本没在 recovery"的错误结论。
  与"重建不上报进度"是同一处观测缺口的两个面（那条已于 2026-09-09 在真实重建上验收关闭，
  所以今天只剩本条这一面）。
- **Status** (2026-09-18): Scope (a)(b) 均已交付。(a) `release_recovery_markers_for_healthy_slots`
  每 tick 电平触发、无 TTL，判据是 dispatcher 自己的 `slot_verdict`（只有 `Withhold` 才释放），
  而不是"没被 fence 就放"。(b) 重发的 refused/decode/unreachable 从 `debug!` 提到 `warn!`。
  验收：单测含 10×fence/unfence 且限流器 global/per_source/per_target 全部归零 + 5 条"仍需重建"
  的否定分支；另有一条测试驱动**真实的 dispatch tick**（为此把 loop 体抽成
  `recovery_dispatch_tick`），因为只测谓词无法证明 loop 真的调用了它 —— 消融掉那一行调用即变红。
  真集群实证（system_chaos、子进程 EN + 真 etcd、`AUTUMN_CHAOS_ACTIONS=fence`、11 轮 fence/unfence）：
  INFO 日志可见 `released recovery marker: source slot is healthy again`（extent 20/22），
  quiesce 后无 still-ACTIVE op，400 keys 全部校验一致。
  **两处刻意保留 marker**（评审发现，均已加回归测试/文档）：`auto_disk` 门控下探测失败派出的重建
  与"本就健康"不可区分，释放会导致每 tick 重启一次整 extent 拷贝；leader 刚接管、源节点首个 `df`
  尚未到达时 `faulted_disks` 为空，此时释放会丢弃真实的故障盘重建，故以 `has_first_hand_df` 把门。
- `passes: true`

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

### BUG-ROT-BLOCKS-ITS-OWN-REPAIR — 腐化的 extent 若正在转 EC，修它的 recovery 派不出去
- **Trigger** (2026-09-10，多盘 chaos 的 seed 603，**三次独立复现**): CorruptReplica 往
  某个 extent 注了 64 字节腐化，EcConvert 随后选中**同一个** extent。
  转换的前置内容校验（正确地）拒绝了它 ——
  op-ledger 上写着 `last_error="extent 14 block 0 fails its content checksum
  (expected 0xaa010c0c, found 0xf9209ac6)"` —— 然后这一轮以 `EC marker on extent 14 still
  pinned after quiesce (age 79s)` 失败。
- **两条路互相挡住,都能在代码里看到**:
  (a) `recovery_dispatch_loop`（recovery.rs:1250）**跳过**任何带 ConvertToEc marker 的 extent；
  (b) EC 转换本身在前置校验上必然失败,因为那份副本就是腐化的 —— 而能修好它的
  只有 recovery。scrub 已经隔离了那个 slot,隔离没有用武之地。
  解开死结的唯一出口是重复失败给弃:`EC_ABANDON_AFTER_CONSECUTIVE_FAILURES = 24`
  (recovery.rs:39)。所以它**会**自愈,代价是 24 次派发往返 —— 而 at-rest 校验这套东西
  存在的理由正是"腐化要尽快修掉"。
- **已经稳定的部分**: 三轮各自独立地停在 `age 78~79s`(extent 14 一次、extent 20 两次),
  也就是说 45 秒的 nemesis + 10 秒 settle + quiesce 走完,marker 一次都没走掉。
  这是 seed 603 不稳定的**主因**,不是偶发。
- **仍未验证（当作假设）**: 78 秒时 marker 还在,不等于它永远不走 —— 我没有测到它真正
  弃给的时刻,也没有确认失败计数在 marker 释放/重取之间是否会归零(若会,给弃可能永远
  凑不满 24)。定级前先补这一条测量。
- **Scope**: (a) 让"参与者内容校验失败"这一类失败**立即**弃给 marker,而不是并入
  24 次的通用预算 —— 它不是暂态的,重试不会让腐化的字节变好;
  (b) 或者反过来,让 corrupt-slot 的强制派发绕过 (a) 那条 ConvertToEc 跳过。
  两者选一,不要都做:同时放开会让 recovery 和 EC 在同一个 extent 上并发改布局。
- **Acceptance**: 一条确定性测试 —— 给一个 extent 打上 corrupt slot 标记并置 ConvertToEc
  marker,断言 recovery 在**一个** tick 内被派出(或 marker 在一次失败后即被弃给),
  且没有该修复时同一断言是红的。chaos 侧:`corrupt` 与 `ec` 同轮启用时,
  quiesce 后不再出现 pinned 的 EC marker。
- **Status**: `passes: false` (2026-09-10) — 不丢数据,会自愈,但把"尽快修腐化"变成
  "先等 EC 放弃"。同时它是 chaos seed 603 不稳定的已知来源之一。
- **Status** (2026-09-18): 选了 Scope (a),没做 (b)（两者只能选一）。内容校验失败新增
  `CODE_CONTENT_CORRUPT`(=8,因此 WIRE_VERSION 41→42)：EN 在同一 attempt nonce 的再次派发上
  直接回该码且**不再起第二个 encoder**，manager `release_corrupt_ec_attempt` 首次失败即弃 marker，
  交给副本 recovery；新 nonce 会重新校验内容，修好后转换自行恢复。顺带补了
  `abandon_ec_marker` 的正确性：etcd `Cmp::value` CAS + await 后重核 nonce（否则一条迟到的回复
  可能释放**后继** attempt 的 marker），CAS 失败不再静默返回、而是 warn 说明该 marker 要等换主才动。
  **评审抓到的关键缺口（已修）**：只弃 marker 并不够 —— 默认门控下 recovery 只重建"被标记过"的 slot，
  而 EC 前置校验发现的腐化此前**没有任何地方记录**，于是要等 scrub 自己按 8MiB/s 的节奏重新发现，
  其间 EC 被反复提议、每次重读整个 extent 再拒绝。现在 EN 通过 scrub 同一条 `df` 通道上报
  (`note_scrub_rot`)，且在每次拒绝时**重新入队**（manager 会丢弃"有在途 op 的 extent"的上报，
  而这次拒绝正是释放该 op 的动作）；消融掉重新入队即变红。
  验收：EN 侧 + manager 侧各一条确定性测试，四条消融全部验证变红；chaos 侧
  `AUTUMN_CHAOS_ACTIONS=corrupt,ec AUTUMN_CHAOS_SEED=603` 两次（修复前后各一次）均通过，
  日志给出完整碰撞链 —— extent 20 被注入 64 字节腐化 → 同一 extent 被选中 EC 转换 →
  `recovery ops driven this round: 1 [extent 20 state=2]`，quiesce 后无 pinned EC marker。
- **待验证假设**（第三轮评审提出，按"先复现再修"未动代码）：`isolate_rotted_slot` 的
  verify-at-apply 只在 `persist_extent` 的 await 之后重核 eversion，**不重核 ledger**。
  构造：extent X 上 coordinator 腐化、另一 slot 被 fence；abandon → 隔离 → persist await
  期间 recovery tick 恰好为那个 fenced slot 取到 Recovery marker（不 bump eversion）→
  verify 通过 → 在有在途 op 的情况下落盘。对 Recovery marker 判断是良性的
  （`apply_recovery_done` 在 apply 时读实时状态）；对 EC marker 才是"op 下改 eversion"的老危害，
  而 EC 由 60s policy tick 驱动、与此不相关。该代码在 df 路径上早已如此，本次只是新增了一个
  时序上正好落在"recovery 刚被解锁"那一刻的调用方。要修就先复现。
- `passes: true`

### BUG-BULK-READ-FLATTENS-REFUSAL — bulk 读把"分片不归我"压成"extent 不可用"
- **Trigger** (2026-09-04，评审发现，**潜伏未触发**): 非 bulk 的 `MSG_READ_BYTES` 走
  `get_extent`，先查 `owns_extent`，不归本分片则回 `wrong_shard_err` →
  `FailedPrecondition` 的**错误帧**，消息里点名该找哪个分片。而 `MSG_READ_BYTES_BULK`
  在 `extent_node.rs` 的对应处只 `match Err((_code, _msg))`，发出
  `bulk_read_head(…, CODE_ERROR, "extent unavailable", 0)` ——**把 code 和消息一起丢了**。
  在 bulk 这条路上，路由错误与真正的不可用**无法区分**。
- **为什么现在不咬人**: 已修好的三个 peer-copy 调用点走的是非 bulk 的
  `read_bytes_chunk`，不经过这条路。所以是潜伏项，不是线上故障。
- **为什么仍要记**: 这正是 CLAUDE.md 第 15 条记录的那类事故的形状——上层靠错误**类型**
  触发 refresh/回退，而下层把类型抹平成一个笼统的失败码，于是回退逻辑变成死代码，
  且全量单测与逐字节 e2e 都是绿的。分片路由的守卫测试目前只覆盖了非 bulk 那条臂。
- **Scope**: bulk 臂透传 `(code, msg)`，而不是改写成 `CODE_ERROR "extent unavailable"`；
  并把守卫测试补到 bulk 路径上。
- **Status**: `passes: false` (2026-09-04) — 未修，已核对代码确认存在。
- **Status** (2026-09-18): 已修。bulk 臂改为发**带类型的错误帧**（`err_bytes`，保留
  `(StatusCode, message)`），与非 bulk 的 `MSG_READ_BYTES` 同形，不再压成
  `CODE_ERROR "extent unavailable"`；另抽出 `ReadRefusal` 承载
  `EversionStale | PayloadNotHere` 两种拒绝。守卫测试补到 bulk 路径：
  `shards.rs` 的 `a_read_addressed_to_the_wrong_shard_is_refused` 现在同时驱动
  plain/bulk/direct 三条臂并断言 bulk 拒绝仍是 `FailedPrecondition` 且消息里点名
  `belongs to shard`；client 侧新增 `wrong_shard_bulk_refusal_falls_back_to_proxy`。
  消融（还原成扁平化）两侧均变红。
  行为差异一处（刻意，已写入 stream CLAUDE.md）：这些拒绝过去以 `Ok(非 OK 码)` 到达
  `read_value_into_pooled` 并直接回落到 copy 路径，现在进入 `Err` 臂，会先走完其余副本再回落 ——
  一个节点的拒绝本就不能代表其他节点；`is_connect_failure`/`is_liveness_timeout` 都不匹配这段文本，
  故 note-29 的抑制与 note-34 的地址遗忘不受影响。
- `passes: true`

### BUG-FRAME-LEN-U32-WRAP — ≥4 GiB 的帧静默编出一个损坏的头
- **Trigger** (2026-09-04，实测过一次真实故障，此处补记): `frame.rs` 的
  `Frame::encode` 与 `encode_response_with` 都把 `usize` 的 `wire_payload_len`
  直接 `as u32` 写进头部，**没有任何上界检查**。载荷一旦 ≥ 4 GiB，长度回绕，
  头部与实际字节数不符，对端 `FrameDecoder` 立刻 CRC 失败。
- **实测**: EC 重建读一个 `u32::MAX + 29,421` 字节的分片时就是这样炸的——当时被误判成
  30 秒超时，日志时间戳（两次尝试相隔 10.75 秒，而非 4×30 秒）本身就否证了超时那个说法。
- **为什么现在不咬人**: EC 重建已改成按 stripe（默认 64 MiB）流式重建，不再发出
  单个 >4 GiB 的读。**但编码器本身仍然无防护**——任何一条新路径只要产生大响应就会中招，
  而且症状是"损坏的帧"，不是"清晰的错误"。
- **对比**: 同一个函数里对 `write_payload` 写入字节数不符的检查是 **release 强制的
  `assert_eq!`**，理由写在注释里："Fail loud rather than ship a silently-bad frame"。
  长度上界该用同一个标准，现在却没有。
- **Scope**: 编码前检查 `wire_payload_len > u32::MAX` 则返回错误（或按同样理由 assert），
  让调用方分块；补一条构造超限载荷的回归测试。
- **Status**: `passes: false` (2026-09-04) — 未修，已核对 `crates/rpc/src/frame.rs` 确认。
- **Status** (2026-09-18): 已修。四个编码入口（`Frame::encode`、`encode_response_with`、
  `encode_vectored_head`、bulk 响应头）现在统一经 `header_lens` 收窄长度，越界 **assert**
  而不是让 `as u32` 静默回绕。三点比账本原文更进一步：
  ① 账本只说了 `wire_payload_len`，实际 `ctrl_len as u32` 也是裸截断——现由同一处覆盖
  （ctrl 是 payload 的一部分，所以 payload 的界先触发；单独给 ctrl 加 assert 会是死代码，
  写了又删，改由 `header_lens` 自己计算 `wire_payload_len` 来保证这层包含关系）；
  ② 界取 **`MAX_PAYLOAD_LEN`** 而非 `u32::MAX` —— 解码侧本来就用它拒绝
  （`FrameError::PayloadTooLarge`），且其注释明写将来会下调为实际上限；编码侧若对着类型
  最大值比较，那天就会编出自己对端拒收的帧；
  ③ `encode_response_with` 的检查放在 `write_payload` 闭包**之前**，越界的调用方不必先
  被要求产出那些字节。
  选 assert 而非 `Result`：同文件既有先例（`write_payload` 字节数不符是 release 强制
  `assert_eq!`，注释理由 "Fail loud rather than ship a silently-bad frame"），且改成
  `Result` 会波及 `encode()` 的每一个调用方。
- **只加编码侧 assert 是不够的，而且更糟（自查发现，本条最重要的一点）**：
  `read_plan` 只把读长度夹到**文件**大小，而 log extent 固定 16 GiB、`ReadBytesReq.length`
  是 u64 且 `length=0` 表示读到尾 —— 所以**一个远端请求就能让响应帧超过 4 GiB**。
  改动前它回绕成损坏帧（EN 活着，对端报 CRC 错）；只加 assert 的话就变成
  **远端可触发的 EN panic** —— 把"可远程触发的坏帧"换成"可远程触发的宕机"，净亏。
  因此在**请求层**加了带类型的拒绝：`ReadRefusal::TooLargeForOneFrame`（复用
  `CODE_PRECONDITION`，不新增 wire code），bulk 与非 bulk 两条路共用 `read_refusal_resp`
  发出，调用方收到的是"超出单帧上限，请分块"而不是连接被切断。编码侧的 assert 因此
  退化为**本节点自造帧的最后一道**不变量，而不是远端请求会撞上的那道。
- **第二条远端可达路径（独立评审发现，比我自查那条更严重）**：PS 的
  `MSG_BATCH_GET_BULK` 把 N 个 key 的值**聚合进一个帧**，而上游对 N **没有任何上限** ——
  SDK 的 `get_many` 会把同分区的 key 全部塞进一个请求，单值上限 64 MiB，于是
  几百个 8 MiB 值（model load 的典型形状）就越过 4 GiB。更要命的是
  release profile 是 **`panic = "abort"`**，所以 assert 不是可被 `spawn_supervised`
  接住的任务 panic，而是**整个 PS 进程中止** —— 一个 `get_many` 打掉一个节点。
  代码库其实已经在同一文件 20 行之外为 `get_redirect_many` 写下过这个危害
  （"model load 期间批量回退会把几百个 8 MiB 值聚成 GB 级帧"），只是 batch get 这条没设防。
  修法：在**循环内**累计并越限即返回 —— 放在循环后会先在 PS 上物化几个 GB 的 `Bytes`；
  返回 `CODE_PRECONDITION`，因为 SDK 对它**已有**逐 key `get_bound` 回退（原本给 stale-epoch 用的），
  于是超限批次自动降级成 N 次小读，而不是把错误抛给调用方。
- **`MSG_RANGE` 查过，不需要设防（核实，非推断）**：它形状相似（`RangeResp.entries` 内联、
  只被远端 `limit: u32` 限条数、无字节上限），但 `RangeEntry.value` **恒为空**
  （`rpc_handlers.rs` 里 `value: vec![]`）——range 只返回 key。让 batch-get 危险的那个放大器
  （把 64 MiB 级的值内联进来）在这条路上不存在，要凑够 4 GiB 需要单次返回上亿个 key，
  而那会先在 `out` 上 OOM，且四个在仓消费者都用小 limit 分页。按"不为复现不了的 bug 加防御"
  不动它，把判据记在这里，免得下次有人重新推一遍。
- **另一处收口（评审建议，已采纳）**：`header_lens` 改成自己**计算** `wire_payload_len`
  而不是接收它。原先"ctrl 的检查不可达"只是**当前四个调用方的巧合**，第五个编码器若用别的方式
  算长度，就会拿到一个被静默回绕的 `ctrl_len` —— 正是本条要修的 bug 上移一层。现在由构造保证。
- **Acceptance 达成**: `frame_length_ceiling_tests` 四条 —— 越界的 vectored head 被拒、
  越界响应在写 payload 前就被拒（闭包里放 `unreachable!`）、**恰好等于上限**的帧仍能编出
  （边界，off-by-one 会藏在这里）、越界 ctrl 由 payload 界拒掉。消融：去掉那条 assert，
  三条立刻变红。构造 4 GiB 载荷不需要真分配内存——这两个入口收的是**长度**而非 buffer。
  服务端那半另有 `read_frame_ceiling_tests` 四条：16 GiB extent 的到尾读被拒、显式超限长度
  被拒、**恰好等于上限**的读仍被服务、以及 256 MiB 常规分块不受影响（同样只设长度原子量，
  不占磁盘）；消融去掉那个界，前两条变红。
  PS 那半有 `batch_bulk_ceiling_tests` 三条（上限低于帧天花板且给 ctrl 留足余量、
  恰好等于上限可服务而多一字节不可、512 个 8 MiB 值确实越界）。
  **坦白覆盖缺口**：这三条钉的是判据与常量，**没有钉住调用点** —— 驱动真 handler 需要一个活的
  partition 外加几 GB 的值，单测够不着。要真正钉住得写一条带可注入上限的集成测试；
  现在靠的是那段说明"为什么必须在循环内 bail"的注释，不是测试。
- **性能**: 每次编码多一次 `usize` 比较（`#[inline]`，与常量比，分支恒不取），相对同一函数里
  的 memcpy + CRC 是噪声；未新增分配或拷贝。没有跑 bench —— 这台机器多租户、本地基线已过期，
  为这种量级的改动跑出的数字不可信，理由记在此处而不是假装测过。
- **最终形状（用户 2026-09-18 定调："所有地方都应当有 bound，能加就加，然后把 assert 删了"）**：
  编码侧的 `assert!` 改成 `debug_assert!` —— release 是 `panic = "abort"`，留着它等于把
  "远端可触发的坏帧"换成"远端可触发的宕机"，净亏；debug 构建（含 `cargo test`）仍会响，
  所以回归测试照旧有效、新写的路径在开发期就被抓住。真正保护生产的界放在**每个生产者**上，
  因为只有它们能"拒绝并继续服务"。五处全部加上，每处都是降级而非报错：
  1. **EN 读** `READ_REPLY_MAX_VALUE_BYTES` → `ReadRefusal::TooLargeForOneFrame`，提示分块。
     注意扣的是**两种回复形状里较大的那个**（bulk 是 `[code][空 message]`，非 bulk 的
     `ReadBytesResp` 另占 9 字节）——我第一版只扣了帧自身的 `CTRL_OVERHEAD`，于是留下一段
     能过检查却在编码时越界的尺寸，而当时的边界测试恰好把那个值钉成"合法"。
  2. **PS batch get** `batch_bulk_budget_exceeded(value_bytes, keys)` → `CODE_PRECONDITION`，
     SDK 已有的逐 key 回退接住。**把 ctrl 按 key 数算进预算**：ctrl 与值同在一个 payload，
     只留固定余量的话，100 万个 4 KiB 的 key 值能过而 ctrl 把帧顶出去。
     同样的错我在第 5 条上又犯了一次（只算值不算每项开销），第四轮评审抓出来，
     现在两处都按"值 + 项数×每项开销"算。
  3. **PS→EN group-commit append** `MAX_WRITE_BATCH_BYTES`，在取 batch 处按字节切前缀，
     余下的留在队列里下一批发 —— 不丢不失败。这条最要紧，走的是热写路径：`pending` 只按
     **条数**限（3072），而单个 Put 可到 64 MiB。顺带接上了早就写好却挂着
     `#[allow(dead_code)]` 的 `WriteRequest::encoded_size()`，并修掉了那句
     "每个在途 batch 最多 30 MB"的**假注释**（代码里从来没有这个常量）。
  4. **`MSG_COPY_EXTENT`** `COPY_REPLY_MAX_VALUE_BYTES` → `FailedPrecondition`，
     `size == 0` 会内联整个 extent，而 handler 对任何 peer 应答。
  5. **`MSG_GET_REDIRECT_MANY`** `GET_REDIRECT_MANY_MAX_INLINE_BYTES` → 超预算的项改发
     **declined**，客户端本来就会对 declined 走 proxy，于是退化成"慢一点的读"而不是失败。
- **测试**: 编码侧 4 条（debug 下仍 should_panic）、EN 读 4 条（含**在天花板附近扫一圈、
  真的驱动 `bulk_read_head`** 的性质测试——消融回我那版 off-by-one 即变红）、PS batch 4 条
  （含"百万小 key 的 ctrl 必须被算进去"）、写批 3 条（超限切分、常规整批不受影响、
  单个超限请求仍能前进）。
- **第四轮评审（fable）的结论已全部处理**：① 第 5 处（redirect-many）的界只算了值、
  没算**每项的 rkyv 开销**（≥69 B/项），6.5 万项 × 65,535 B 就能值合法而帧越界 —— 与第 2 处
  同一个错，我连犯两次；现已改成"值 + 项数×每项开销"并补了消融过的测试。
  ② `WriteRequest::encoded_size()` 比真实 WAL 记录**每条少算 17 字节**（漏了 envelope 与
  internal key 的 8 字节时间戳），而记录条数并不受 `max_write_batch()` 限（一次 BatchPut
  会把它所有 op 一起 push）；现按 `V1_ENVELOPE_OVERHEAD + PAYLOAD_HEADER + 8` 算准。
  ③ 三条 `should_panic` 断的是 `debug_assert!`，`cargo test --release` 会判它们失败；
  已加 `#[cfg(debug_assertions)]`，debug/release 两种构建都验过。
  ④ 另修 6 处文档假话（"release-enforced"与 `debug_assert!` 自相矛盾、两个不存在的常量名、
  "所有 in-tree 客户端都分块"、已过期的 `panic="abort"` 说法、"没有一个会让调用方直接失败"）。
  它同时核实了**切批不会造成 wedge 或乱序**（seq 在 `start_write_batch` 内按批序分配、
  `FuturesOrdered` 保持 Phase 3 顺序、剩余项在下一轮或关机路径必被取走），以及 manager
  的三个 `limit` 字段都只能截断不能放大 —— 没有漏掉的生产者。
- **切批的一个语义变化（已写进代码注释）**：跨切点的 BatchPut 不再在**单个 append 内原子持久**，
  崩溃可能只落一半；但没有任何已 ACK 的东西会丢 —— 响应在最后一个 op 才发，半写的 BatchPut
  还没回答过任何人。
- **仍未钉住的**: PS 三处的**调用点**都没有测试驱动真 handler（需要活 partition + GB 级数据）；
  钉住它们要写带可注入上限的集成测试。`MSG_RANGE` 查过不需设防（`RangeEntry.value` 恒空）。
  一个内存向的更紧的 batch 上限（注释原本想要的 30 MB）是另一个带实测的决定，不在本条。
- `passes: true`

### BUG-REBUILD-FSYNC-UNCOUNTED — 重建成功但 fsync 失败，整个分片不记账
- **Trigger** (2026-09-04，评审发现，已核对代码): EC 重建的成功分支上，
  `f.sync_data().await…?` 与 `fsync_staging_dir(…)?` 都在 `extent.note_shard_file(…)`
  **之前**早退。任一个失败，盘上留下一个**完整长度**的分片文件，而条目里没有任何记录。
- **后果**: 条目与磁盘不一致的另一半——字节在盘上却不记账，`df` 少算，
  `holds_payload` 为假。要到重启后 `discover_shard_files` 补登才对上。
  比它的对偶（失败重建 unlink 了分片却留着账上的记录，那半已于 2026-09-04 修掉）轻，
  不会把读降级成 Internal，但同样是条目与磁盘不一致。
- **Scope**: 要么把 `note_shard_file` 提到 fsync 之前（记录"文件存在"本就不依赖它是否已持久），
  要么在这两个 `?` 上改成先记账再返回错误。注意别和失败重建臂的 discard 语义打架。
- **Status**: `passes: false` (2026-09-04) — 既有缺陷，未修。

### F-SPLITMERGE-PROGRESS — split/merge 全程冻结分区却不报进度，"在切"和"卡住"看起来一样
- **Trigger** (2026-09-09，用户在 dashboard 上切了 part 44 之后问"什么算 split 完了"):
  op 表上只有一行终态 `split succeeded ... dispatched`，中间什么都没有。而 split **全程
  持有 `frozen_for_split`**——这段时间该分区的写是停的。运维在此期间真正要回答的问题是
  "我的写被冻了多久、它是不是卡住了"，而控制面对此**一个字都没说**。同一次操作里还夹着
  几次 `cannot split: partition has overlapping keys` 的失败（判据是 `has_overlap != 0`，
  要等 compaction 消化），成功与失败交替出现，更需要能看见它走到哪一步。
- **机制现状（已核对代码，非推测）**:
  - 通道**已经存在**: `MaintenanceProgress { op_id, kind, done, total }` 搭在
    `PartitionLoad.active_maintenance` 上随负载心跳走；PS 侧有
    `maintenance_progress: Mutex<Option<..>>`（`partition-server/src/lib.rs:1416`）、setter
    与 `snapshot_maintenance_progress()`；manager 侧 `update_progress()`
    （`rpc_handlers.rs:4899`）**在终态之前**应用，避免样本给已关闭的 op 上色。
  - 但只有 **compact / gc / forcegc** 在填它。split/merge 从不填。
  - split 处理里有 **13 个 `.await`**（`rpc_handlers.rs` 1470–2120），会让出线程，
    所以冻结期间心跳照常发得出去——采样不会堵到最后才到。
- **Scope**:
  1. PS 在 split 的既有阶段边界上报阶段，**六个**: 受理（等 maintenance gate + 扫描分裂点，
     此时还没冻）→ 冻结 → 排空（compaction、GC，含 flush）→ commit_length →
     `multi_modify_split`（元数据）→ 解冻。第一个阶段是评审补上的:gate 等待和中位数扫描
     才是真正会等几十秒的地方，而 `has_overlap` 重试恰好落在这一段。
     merge 四个，顺序是 **owner-lock → 冻结双方 → 六个 commit_length → `multi_modify_merge`**
     （owner lock 在最前，且它是冻结+排空，不是 flush）。
  2. **报阶段序号，不报字节**。各阶段代价极不均匀（flush 占大头），字节数会在 flush 上
     长时间不动、反而像卡住；`done/total` 用阶段序号是这里唯一诚实的单位。
     （这一条与仓库既有约定"进度是原始计数不是百分比"不冲突：消费者仍自己算比例。）
  3. manager 加 `update_progress_by_part(kind, part_id, done, total)`，匹配该分区上
     RUNNING 的 split/merge 条目。
- **⚠️ 不需要动 wire，这是本条与 extent 重建进度那条路的关键分野**: `SplitPartReq` 里没有
  `op_id`，加一个就是 rkyv 结构变更 → 版本 bump → `MIN=MAX` 全停。**不用加**——样本装在
  `PartitionLoad` 里上来，manager 本来就知道是哪个分区，PS 报 `op_id: 0` 并由 manager 按
  (kind, part_id) 匹配即可。这正是 `update_progress_by_extent` 已有的先例，它存在的原因
  就是"extent node 永远不知道 manager 的 op id"。重建进度走 EN 的 `DfResp`，那条路才必须
  动 wire（已实现并关闭）；两条不要混为一谈。
- **Acceptance**:
  - 对一个足够大的分区发起 split，`autumn-op ops status <id>` 的 `progress_done` 在操作
    进行中**非递减**（心跳 5 s 一次，一个阶段会横跨多个样本，所以"严格单调"不是可达的标准），
    并在终态等于 `progress_total`——后者靠 `finish` 对 SUCCEEDED 快照到满格，因为最后两个
    阶段与 RPC 返回之间没有 `.await`，没有任何一次心跳能落在那里。
    **消融: 去掉 PS 侧的上报，该断言变红**。
  - 人为让某一阶段挂住（如注入一个不返回的 flush），进度**停在该阶段不动**，
    且停住的位置在 `ops status` 上可见 —— 即"慢"和"卡"可区分，这是本条存在的全部理由。
  - merge 同样两条。
  - 单测层面: `update_progress_by_part` 只触碰 RUNNING 条目，终态条目不被样本复活
    （与 `update_progress_by_extent` 同型，直接复用其测试形状）。
- **Status**: `passes: false` (2026-09-09) — 已实现、单测与消融通过、docs 已更新；
  **验收未做**（要在真集群上切一个够大的分区看进度推进与注入挂起）。
  评审补掉的两个真缺陷:(1) split 有九条退出路径不清 slot，残留阶段会被**下一次** split
  继承——已改为 RAII 守卫，drop 时清，覆盖所有 `?` 与 early return；
  (2) 阶段 5/6 与 RPC 返回之间没有 `.await`，心跳采不到，SUCCEEDED 却显示中间阶段——
  已让 `finish` 对成功的 op 快照到满格（`reconcile_outcome` 早有同款逻辑，注释里记的正是
  EC 转换停在 75% 那次）。

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

### BUG-GC-ADVISORY-VS-SELECTION — 建议按绝对死字节，回收按死亡比例，两套判据对不上
- **Trigger** (2026-09-09，用户看运维面板): 「一直在gc」「我怀疑是punch的洞的那个数字算错了？
  导致老让启动gc」。方向对了——不是算错，是**两个判据不是同一个**。
- **实测到的机制**(数字全部读自 `info --full` 与 PS 日志):
  - 建议触发按**绝对值**：`gc_debt_bytes > gc_debt_high`(默认 1 GiB，`policy.rs:735`)。
  - GC 选取按**比例**：`discard_bytes / sealed_length > effective_ratio`(默认 **0.4**，
    `background.rs:1105`)。
  - part 168/204 上真实的两个 extent：

    | extent | sealed_length | dead | ratio |
    |---|---|---|---|
    | 190 | 16.00 GiB | 3.12 GiB | **0.195** |
    | 169 | 15.25 GiB | 0.43 GiB | **0.028** |

    两者都远低于 0.4 → 永远挑不中 → GC 每次都报 `no eligible extents to reclaim`；
    而每分区 3.6 GiB 的绝对债务远超 1 GiB → 建议每 5 分钟重发。**一个永不收敛的循环。**
  - 不是 replay-floor 保护：三台 PS 的日志里 `protected extent` **零条**，`holes` 在到达
    那道保护之前就已经是空的。
  - GC 本身正常：`GC: punched extent 156, moved 1072 entries`、`punched extent 187,
    moved 0 entries`，没有失败也没有冷却跳过。
- **关于"双重计数"的订正**(2026-09-10): 168 与 204 是一次 CoW split 的父子，共享 6 个
  extent(22,169,172,188,190,200)，两个分区确实各把同一份 3.6 GiB 报了一遍。我最初把这
  记成缺陷并打算 dedup —— **那是错的，实现了会造成真损害**。
  extent 是**引用计数**的，物理删除发生在 `refs → 0`(`extent_delete.rs:1`)，实测这 5 个
  非空 extent 的 `refs` 全是 **2**。两个 stream 各持一个引用，**必须各自 punch** 才可能归零。
  所以：
  - "一组只发一次建议"会让另一个 stream 永远不放手，refs 卡在 1，那 16 GiB **永远不会被
    释放** —— 比现在的噪声坏得多。
  - "把字节 dedup 掉"会少报每个分区自己真实要做的工作。
  两份账各自成立，不是重复计数。**唯一真实的问题是呈现**：集群层面把它们相加得到的不是
  物理字节数，而运维面板没有任何地方说明这一点。值得做的是在 extent 视图上显示 `refs`
  与共享它的分区，而不是去动会计口径。
- **Scope**(未实现):
  1. 两套判据要对齐。合理的方向是让选取也认绝对值——一个 16 GiB extent 上的 3 GiB 死字节
     值得回收，即使只占 19%；或者反过来让建议也认比例，但那会让真实的大块垃圾无人问津。
     **倾向前者**：加一条 "dead_bytes > X 也算合格"的或条件，X 与 `gc_debt_high` 同源。
  2. ~~共享 extent 的父子对上 `gc_debt_bytes` 要 dedup，或至少一组只发一次建议。~~
     **撤销**——见上面的订正：那样会让共享 extent 的 refs 永远归不了零。改为呈现层的事：
     extent 视图应显示 `refs` 与共享它的分区，让相加得到的数字不被误读成物理字节。
- **Acceptance**: 一个 dead 3 GiB / size 16 GiB 的 extent 在默认配置下会被 GC 选中并回收；
  共享 extent 的两个分区报告的债务之和不超过实际死字节。
- **Status**: `passes: true` (2026-09-18 真集群复验 + 呈现层落地；2026-09-09 立账；2026-09-10 修了第一半；2026-09-11 修了 Scope 1) —
  **验收前半已实现，后半按下面的订正不做，但仍未在真集群复验，所以不关。**
  **2026-09-11**：选取侧加了绝对字节的或条件 —— `dead >= dead_bytes_high` 无论比例都合格，
  `X` 由 manager 从 policy 自己的 `gc_debt_high` 送下来(wire 39→40 加
  `MaintenanceReq.gc_dead_bytes_high`)，建议按哪个数触发、回收就按哪个数选取。
  **关键是两端现在是同一个函数**:抽出 `gc_selects(dead, len, refs, ratio, abs)`，
  选取循环与 `collectable_debt`(建议读的那个 gauge)共用它 —— 这两处曾是各写一遍的同一条
  判断，而它们不一致正是本条 bug 本身。
  **评审抓到我第一版把这句话说过头了**:`gc_debt_bytes` 有**两个**写入点,我只改了选取那个;
  另一个是 5-7 秒一次的空闲刷新 tick(`Sel::GcTimeout`),它写的是"所有死字节"的裸和,
  并且几秒内就把选取写的值覆盖掉 ⇒ 建议绝大多数时候读的仍是裸和,
  对"死字节摊薄在多个 extent 上、每个都够不着门槛"的分区,原来的死循环**原封不动**。
  已修:选取把它解析出来的 `(sealed_length, refs)` 与阈值发布到 `PartitionData.gc_debt_basis`,
  空闲 tick 用同一条谓词算 —— 零额外 RPC;首次派发之前退化为裸和(高报,安全方向)。
  另按用户 2026-09-11 的提议加了第三条:`refs > 1` 的共享 extent 把比例门槛再减半
  (文件只有 `refs → 0` 才真删，收掉本侧引用正是让 extent 变成独占的那一步)；
  `refs` 搭在选取本来就做的 `get_extent_info` 上，零额外 RPC；**确定性而非概率**,
  否则运维无法回答"这个为什么没被收"。`--empty-only` 只打空洞,绝对阈值对它无意义(第四轮已删掉那个多余的抑制分支)。
  **第二轮评审又抓到同一条被我说过头的话的另一半**(2026-09-11):basis 被**无条件**发布,
  而 `dead_bytes_high` 是**每次派发带下来的参数**——`autumn-op gc PART`、dashboard 的 GC
  按钮、测试 helper 都不带 floor,`--empty-only` 还带一个够不着的 INFINITY gate。于是
  **一次手动 GC 就把 floor 从 gauge 上永久摘掉**:空闲 tick 每 5-7 秒都从这个 basis 重
  答,那 3 GiB 永远算 0 → 建议再也不发 → manager 再也不下发 floor → 直到分区重开才恢复。
  这是本 bug **沉默的那一半**,比它替换掉的裸和更坏(裸和至少几秒后就把错值覆盖掉)。
  根因不是那行赋值,而是我把**单次派发的覆盖参数**当成了**分区的常驻策略**存下来。
  修法:basis 拆成 FACTS(sealed_length/refs,任何一次 pass 都可刷新)与 POLICY(ratio/floor,
  **只有带真实 floor 的 pass 才可定义**),`next_gc_debt_basis` 一个纯函数承载全部判断,
  调用点退化成一次无条件赋值;从未收到过 policy 时 basis 保持 None → 退回裸和(高报,安全方向)。
  消融:3 条新单测在去掉粘性规则后全红(12 passed / 3 failed),恢复后 15 全绿。
  **第三轮评审抓到同一形状的第三次**(2026-09-12):我用"带了真实 floor"去**推断**"来自控制面",
  而 CLI 能伪造这个特征——`autumn-op gc --ratio 0.9 --dead-bytes 100G PART` 带着一个货真价实的
  floor,于是把 (0.9, 100 GiB) 钉成常驻策略,gauge 再次永久归零。同时 basis 里存的
  `effective_ratio` 根本不是策略:它把 `stream_debt_hit`(一个关于**此刻**有多少死字节的事实)
  烤了进去,于是 gauge 会继续报告一个早已关掉的减半,或错过一个刚打开的。
  根因一句话:**basis 存的必须是"问题",不是某一次派发对这个问题的"答案"**。
  修法三件:①basis 存 `(ratio_base, stream_debt, floor)`,减半在每次读取时从 live discards
  重新推导;②"可定义策略"从推断改成**显式信号** `MaintenanceReq.gc_policy_is_standing`
  (wire 39→40,与本轮同一次抬),只有 manager 置位;③manager 对**没带任何 knob** 的 submitted GC 用自己的
  config 补齐并标记 standing —— 于是 dashboard 上那个紧挨着 GC 建议的按钮,第一次真的能收掉
  建议所指的那堆字节(此前它发的是无 floor 派发,按比例门槛根本够不着)。
  消融两条,各自只红自己那条(16 passed / 1 failed ×2),恢复后 17 全绿。
  11 条单测(含线上真实的 3.12 GiB/16.00 GiB 与 0.43 GiB/15.25 GiB 两组)，
  **两条消融各自变红**:去掉绝对臂 ⇒ 验收用例 + 债务一致性用例红;去掉 refs 减半 ⇒ 两条共享用例红。
  **~~仍未做/未验~~ 两条均已了结(2026-09-18)**:
  ① 验收原文要求"默认配置下被选中并**回收**"——**已在真集群复验**,见下面 2026-09-18 一段;
  ② 验收后半"共享 extent 两个分区报告的债务之和不超过实际死字节"**不实现** ——
     见本条 2026-09-10 的订正:dedup 会让 refs 永远归不了零。按订正落到呈现层
     (extent 视图显示 `refs` 与共享它的分区)，**2026-09-18 已实现**。
  **已修**：自动 GC 派发从来不带 `gc_stream_debt`(`manager/src/lib.rs` 的
  `actuate_maintenance` 写死 `None`)，于是"stream 级死字节超过高水位就把 per-extent 比例
  减半"这条**本就为这种场景设计的**机制，只在运维手工敲 `--stream-debt` 时才生效。现在按
  GC 与 FORCE_GC 两种 op 传入策略自己的 `gc_debt_high`——建议按哪个数触发，回收就按哪个数
  放宽，两端不再各说各话。
  **也已修**(2026-09-10)：`gc_debt_bytes` 现在只统计**策略真会来收**的字节。抽出
  `collectable_debt()` 复用选取过程已经取到的 `sealed_length`(零额外网络调用)，对每个
  extent 施加同一条比例判定；**扫描没走到的 extent 保留原始死字节**——它的可回收性未知，
  报 0 会把真实存在的工作也一并静音,那是更坏的错误。建议于是不再承诺做不到的事:
  0.195 比例的那 3.12 GiB 不再计入债务，循环自然停止。
  4 条单测(含线上真实数字 3.12 GiB / 16 GiB 那一组)，消融验证:去掉比例判定 → 该条变红。
  **不修**：共享 extent 的"双重计数"经核实不是缺陷，见上文订正。

  **2026-09-18 结项：真集群复验(验收前半)**。3 EN + 真 etcd,**全默认配置**
  (含默认 16 GiB extent seal size,`gc_debt_high` 1 GiB,ratio 0.4)。造出
  extent 8 = 16.0 GB sealed / **2,576,980,376 B dead** ⇒ **ratio 0.15**。
  **关键是这个 0.15 而不是随手取的数**:standing 派发会把 `gc_stream_debt` 也按
  `gc_debt_high` 填上,于是 stream 死字节过 1 GiB 时比例门槛**减半到 0.2** ——
  我第一次的复验用的是 4 GiB extent / 1.2 GiB dead = 0.30,**0.30 > 0.2,
  是比例臂选中的,绝对臂就算整条拆掉结果也一样**,那次复验什么都没证明。
  评审(fable)抓到了这一点。0.15 同时低于 0.4 与减半后的 0.2,高于 1 GiB 地板 ⇒
  **只有绝对臂能选中它**,这才是线上 0.195 的真实形状。
  对照两跑只差"带不带 floor":
  - `gc 13 --ratio 0.4 --stream-debt 1073741824`(减半生效、**无 floor**)
    → `no eligible extents to reclaim`;事后 `df` 仍读 2.4 GB(一次手工 override
    不得改写常驻 gauge — 本条 2026-09-11 那半"沉默的 bug"也就此第一次拿到真集群证据)。
  - `gc 13`(不带任何 knob ⇒ manager 用 policy 的 `gc_debt_high` 补 floor 并标 standing)
    → `GC: punched extent 8, moved 3491 entries`,搬 ~14.6 GiB 存活值,耗时 7m22s,
    extent 从 `info --full` 消失,`df` 2.4 GB → **0 B**。
  另外**无人值守**那条环路也复验过(4 GiB extent 那轮):`auto-policy activate gc-only --arm`
  → 08:36:22 manager 自己发建议 `gc_debt_bytes>1073741824 (1228 MiB) sustained 5m`
  → 08:38:30 punch → 08:38:36 gauge 归零。即本条描述的"建议每 5 分钟重发、GC 每次拒绝"
  的死循环,现在会自己收敛。
  **呈现层(验收后半的订正版)已实现**:`autumn-op info --full` 与
  `info --part`(dashboard 抽屉的数据源)的 extent 视图现在**点名持有者**
  `shared by parts [13, 19] — freed only once ALL of them drop it`,JSON 加
  `shared_by_parts`;dashboard extent chip 同样,并且只在 `role == "log"` 时才附
  "各自记一份债"那句(gc_debt 是 log stream 的账,row/meta 共享 extent 靠 compaction
  的 head truncate 释放,不是 GC)。真 CoW split 上渲染验证过。
  **代价写进文档而不是含糊过去**:`run_partition_info` 没有别的分区的 stream 成员关系,
  所以在本分区有 `refs > 1` 时多发一次 `MSG_STREAM_INFO`(全部 3N 条 stream),
  而 manager 对这个请求会把**集群里每个有归属的 extent** 克隆回来 —— 正是该视图平时
  避开的那种全量拉取。评审指出"常见情况不花钱"是错的:split 长出来的集群里
  `refs > 1` 很常见。要做便宜只能在 manager 侧加反查(动 wire),暂不做,**把代价写明**。
  **不做**:跨 crate 的"标准派发→PS 选中"端到端单测。manager 侧
  (`maintenance_req_for_submitted_op` standing 填 `gc_dead_bytes_high`/`gc_stream_debt`)
  与 PS 侧判据(含"减半门槛也够不着、但够得着 1 GiB 地板"那组断言)各自都已有单测钉住,
  中间只剩一次字段拷贝;真正没被单测覆盖的是整条线,而那正是上面这次真集群跑验的东西。

  **订正**(2026-09-09)：本条最初记作 `BUG-SPLIT-STUCK-RETRY`，把机制写成"父子共用同一对
  stream"。**那是错的**——我当时的脚本把字段列表截断在前 8 个，误把 `discards` 里的
  extent id 当成了 stream id。实际每个分区的 stream 都是独立的(168 是 165/166/167，
  204 是 201/202/203)，共享的是 **extent**。split 被拒是另一回事 —— CoW 孩子的 `has_overlap`
  只有 major compaction 能清，策略现在会**改发这条 compact** 而不是发一个必被拒的
  split(2026-09-11)。

### F-SPLIT-ADVICE-COUNTS-UNRECLAIMED — split 建议把"还没回收的垃圾"当成体积，按幻影切分区
- **Trigger** (2026-09-11，用户把 split+EC 打开做验证时观察到): 三个模型刚被删干净，
  集群里真实数据只剩 9.2 MB 的 buda 文档，而 auto-policy 立刻连发四条 split：
  `split 17 (est_live 91 GiB)` / `split 164 (88 GiB)` / `split 28 (114 GiB)` /
  `split 32 (120 GiB)`。这些分区的 LSM 只有 1-12 MB(`lsm 1 MiB` 就写在同一行里)。
- **成因**: `effective_size_bytes = max(size_bytes, est_live_bytes)`，而
  `est_live = sealed_sum + open_tail - gc_debt - open_tail_dead`。数据被删之后
  `sealed_sum` 仍然是满的(extent 还在)，`gc_debt` 本应把它抵消掉 ——
  但 `gc_debt` 是**按 0.4 比例门槛算出来的 collectable 部分**
  (见 `BUG-GC-ADVISORY-VS-SELECTION`)，16 GiB 的 extent 里死 1-3 GiB 时它算 0。
  于是"已删但没回收"的字节在 `est_live` 里**全额计入**，分区看起来永远是满的。
- **后果不是浪费而已**: split 会持有 `frozen_for_split`、真的产出两个孩子、
  两个孩子 CoW 共享 extent 并各自带上 `has_overlap`，然后需要 major compaction
  才能分开 —— 也就是说，**一次基于幻影体积的 split 会给集群留下真实的清理债**，
  而这债正是"切不动"的来源 —— 策略现在会改发解锁用的 major compact，
  而不是发一个必被 `has_overlap` 拒掉的 split(2026-09-11 已实现)。
- **与 GC 门槛的关系**: 这两条是同一个根的两个症状。GC 那条修好(门槛不再让大 extent
  的垃圾算 0)之后，`est_live` 会自动跟着变准，这一条大概率随之消失。**所以不要
  独立地给 split 加补丁**；先修 `BUG-GC-ADVISORY-VS-SELECTION`，再回来验证这一条。
- **Scope**(未实现):
  1. 先修 GC 门槛，然后**复验**：在一个刚删空的集群上，`est_live` 应当跟着掉下来，
     split 建议不再出现。
  2. 若仍出现，才考虑让 split 的判据不采信 `sealed_sum - gc_debt` 这条路径，
     改用一个不依赖回收进度的量(例如 SST + 实际被引用的 VP 字节)。
- **Acceptance**: 一个刚删空全部数据的集群，在 auto-policy armed 的情况下，
  一个 policy 周期内**不产生任何 split 建议**；`autumn-op info` 里该分区的
  `est_live` 与 `size_bytes` 处在同一量级。
- **Status**: `passes: false` (2026-09-11) — 仅立账。观察到的现场在
  `claude-progress.txt` 2026-09-11 (4) 那条里。
  **补充** (2026-09-11，split 判据重设计之后): 幻影体积**不再是 split 的触发器** ——
  硬 size 触发器改读 LSM 常驻字节，携带字节(含未回收垃圾)只剩下"速率触发器的地板"
  与"merge 的否决"两个用途，所以本条描述的那四条 split 建议按设计不会再出现
  (单测 `carried_payload_alone_does_not_advise_a_split`，消融验证)。**但本条不关**:
  ①验收要求的"刚删空的集群一个周期内零 split 建议"**没有在真集群上复验**;
  ②根因没动 —— `est_live` 仍然把未回收的垃圾全额计入，于是**merge 的否决**和
  速率触发器的地板仍然按幻影体积判断(一个删空的分区会因为"看起来很大"而不被 merge);
  ③因此本条说的"先修 `BUG-GC-ADVISORY-VS-SELECTION`"仍然成立。
  **前置已解除**(2026-09-18)：`BUG-GC-ADVISORY-VS-SELECTION` 已结项(真集群复验:
  16 GiB extent / 2.4 GiB dead / ratio 0.15 被绝对臂选中并回收,`gc_debt` 归零)。
  于是本条 Scope 1 说的"复验 `est_live` 会不会跟着掉下来"**现在可以做了** ——
  仍未做,它需要的是一个刚删空的集群 + armed auto-policy 跑满一个周期。

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

### F-MERGE-COW-STALE-SEQ — 合并两个未分离的 CoW 孩子是否会让旧值压过新值（未复现）
- **Trigger** (2026-09-11，做 compact-before-merge 时顺带查清的事): 实测确认
  **merge 根本不检查 `has_overlap`** —— 两侧都是 1 的一对照样合并成功，60 个 key
  全部读回，幸存者 reopen 之后 `has_overlap` 自己重算成 0。全树唯一的 `has_overlap`
  闸门是 `handle_split_part`。（`crates/manager/tests/system_merge.rs` 里
  "Without this, merge would refuse with the has_overlap gate" 那句注释是错的，已改。）
- **于是剩下一个没人回答的问题**: split 之后两个孩子的 seq 计数器各自从父亲的
  `max_seq` 独立往上走。幸存者的 SST 里有父亲留下的 key K 的旧版本（seq 高，因为
  split 前父亲的 seq 已经走到那里），受害者在 split 之后给 K 写了新值（seq 低，
  因为它从同一个起点独立计数）。合并把两边的 SST 并到一起、range 变宽、
  `has_overlap` 重算成 0 之后，**MVCC 按 seq 定胜负，旧值可能赢**。
- **这是假设，不是事实**: 上面那次实测没有构造这个形状（没有在 split 之后对
  受害者范围内的 key 重写）。按 [[feedback_reproduce_before_fixing_mechanism_bugs]]，
  先复现再谈修。
- **Scope(复现之后才谈)**: 写一个确定性 harness —— 建分区 → 写 K → split →
  只对受害者侧写 K 的新值 → 不 compact 直接 merge → 读 K。读回旧值即坐实。
  若坐实，修法在 merge 的 seq 处理上（重编号或取全局 max），不是在策略层加闸门。
- **今天的策略层做了什么、没做什么**: `unblocking_compact` 会在**幸存者**还
  overlap 时先发一次 major compact —— 那是卫生（幸存者不该扛着未分离的 CoW 表
  跨过 range 变宽），**不是**为了堵这个洞，注释和 reason 文案都如实这么写。
  受害者被**故意排除**：它马上就要被 merge 删掉，compact 它是白干。
- **Acceptance**: 一个确定性复现（或一份说明为什么构造不出来的分析），据此决定修不修。
- **Status**: `passes: false` (2026-09-11) — 仅立账，**先复现再修**。
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

### F-FUSE-WRITE-INFLIGHT-DEPTH — fuse 写侧多槽流水线，收益未实测
- **Trigger** (2026-09-13，用户指派"把连续流水线搬进 fuse 写侧"): 立论起点是
  `e58c735` 记的"mount 写 208 vs CLI 345"。**这个起点是错的** —— 208 是
  2026-09-03 00:05 的测量，而 `2949372` 在同日 04:20 就做了单槽异步流水化，把写从
  249 抬到 338 = CLI 345 的 98%。查出来时代码已经写完，用户指示保留代码、修掉错误论证。
- **实际做了什么**: `InodeState.pending_flush: Option<_>` → `pending_flushes:
  VecDeque`；规划下一批前不再无条件 drain 上一批，只在队列满时等最老的一个
  (`write::make_room`)；`drain_pending` 改为排空**全部**槽 —— `drain_all_pending` 与
  `flush_inode` 都依赖"排空后无在飞"，漏一个槽 = fsync 对未落地字节报成功 = 丢数据。
  `APPEND_INFLIGHT_DEPTH = 2`。
- **剩余空间有多大，先说清楚**: 单槽一批已经是 **8 个 extent 并发**，恰好等于
  autumnfs 非 striped 的 `depth`=8 —— 338≈345 正是这个对等造成的。多槽把在飞 extent
  从 8 抬到 16，找的是剩下那 ~2%。而 `934d4ee` 实测单机剩余的墙是 RF3 全副本 fsync，
  不在客户端，所以这 2% 未必拿得到。
- **安全性(两条都有依据)**: ① 落地顺序无关 —— `extent::upsert` 按 start 有序插入替换，
  连续 append 批次区间互不相交；单测 `disjoint_batches_apply_the_same_in_any_order`
  钉住，消融(把 upsert 改成 push 不排序)会红，已验。② 出错不提前返回 —— postcondition 是"队列空"，
  提前返回会把槽留下而调用方以为已静默。**反过来也别清空队列**：`JoinHandle` 即
  `async_task::Task`，`Drop` 调 `set_canceled()`，drop 会**取消**那次 flush。
  (我最初引的 compio "drop 不取消"是 `spawn_blocking` 的契约，不是 `spawn` 的，已订正。)
- **测试覆盖的边界**: 多槽本体**无**自动化覆盖 —— `ClusterClient` 三条构造路径全要连
  manager、`FsState::from_client` 私有 ⇒ 造不出 `FsState`，而 `make_room` /
  `drain_pending` / `settle_oldest` 都要 `&mut FsState`。只钉住了它依赖的纯函数前提；
  没有为了凑数写 `fn should_make_room(len)->bool` 那种测 `>=` 的同义反复。
- **Acceptance**: 同机对照实测 —— mount 写与 `autumnfs put` 同一个大文件，给出
  `APPEND_INFLIGHT_DEPTH` = 1 与 2 的吞吐对照。若无可辨差异，改回 1（**精确回退**：
  队列非空就等 = 原来的无条件 drain），并把结论写回本条。
- **Status**: `passes: false` (2026-09-13) — 代码与单测完成，**收益未实测**。
- `passes: false`

### BUG-FUSE-EOF-READ-CLOBBERS-DIRTY-META — 一次 EOF 之外的读会抹掉未发布的写，然后写侧自己把已落地的 extent 删掉
- **Trigger** (2026-09-13，fuse 多槽评审顺带追出;**既有缺陷，非多槽引入**，单槽时代同样可达):
  `read.rs:129-175` 在 `offset >= 缓存 size` 时会 `get_inode_uncached` 然后 `meta = fresh`;
  `meta.rs:200-210` 的 `if is.meta.size != fresh.size { is.meta = fresh; is.extents = None }`
  **既不看 `is.dirty` 也不看方向**。原 commit(`01e0ad7`)针对的是"缓存偏小"，而"缓存比 KV 大"
  恰恰是 mount 正在写一个文件时的**常态**。
- **两种结局(都是静默数据损坏)**: ① 下次写在旧偏移 → `write.rs:266-271` 看到被抹小的
  `cur_size < offset` → `clean_beyond_eof` → `extent.rs:635-641` 删掉**每一个** `s >= eof` 的
  extent key，即所有已落地但未发布的 extent;② 不再写直接 Release → `flush_inode` 时
  `is.dirty` 仍为 true(`get_inode_uncached` 从不清它) → `put_inode` 发布被抹小的 size →
  **文件以 size 0 关闭**。
- **为什么内核没挡住**: `ops.rs:264` 用 `FOPEN_DIRECT_IO`，内核不在 `i_size` 处截断读，
  所以本机一个 EOF 处的 read 真的会走到 `read::prepare`。`tail -f` 正是这个形状
  (SEEK_END 然后在 EOF 读)，写满 64 MiB 整数倍后的任何读者同样。
- **Scope(先复现，再修)**: 按 [[feedback_reproduce_before_fixing_mechanism_bugs]]，
  先要一个真复现再动刀。e2e 配方(评审给的，今日未跑): 开 `f` 写恰好 64 MiB 不 fsync 不关;
  另一进程 `dd if=f bs=1M skip=64 count=1`(读到 0 字节，正确);关 writer;`stat f` 期望
  64 MiB，实测应为 0。修法方向是决策(脏 inode 跳过 refresh / 只接受更大的 fresh size)，
  不在本条预先选定。
- **Acceptance**: 上述 e2e 在修前红、修后绿;且"缓存偏小"那个原始场景(`01e0ad7` 的目标)
  仍然被覆盖。
- **Status**: `passes: true` (2026-09-13) — 已复现、已修、已回归。
  **手段与当初写的不同，记下来**:本条 Acceptance 原文写的是 shell e2e(起集群+挂载+dd)，
  实际做成了 in-process 集成测试 `crates/manager/tests/system_fuse_eof_clobber.rs` ——
  clobber 发生在 daemon 自己的缓存里、在 FUSE 边界之下，**根本不需要内核挂载**，所以它能
  进 CI 常驻，比原计划的一次性脚本强。(我先照原文写了一个 shell 版 repro 脚本，在手搓集群上
  连栽 wire 版本/format/--advertise/--cpuset/pgrep 五个坑，才想起去找 `system_fuse_*` 这套
  现成的 in-process 脚手架 —— **那个脚本已废弃删除，不在树里**,别照这段去找它。)
  实证:pre-fix 红在 `:128`("the EOF probe clobbered the unpublished size")，post-fix 绿。
  验收第二半("缓存偏小仍被纠正")做成同一测试里的回归护栏，它**前后都绿** —— 那是护栏不是
  复现，如实标注。
  修法:`get_inode_uncached` 的 `!=` 收窄成 `fresh.size > is.meta.size`(只接受"文件其实更大"
  这一个方向)，`read.rs` 的局部 rebind 一并收进 `changed` 分支。没拿 `is.dirty` 当闸门 ——
  size 可能在 inode 短暂 clean 时仍未发布，方向本身才是判据。
- `passes: true`

### BUG-FUSE-FLUSH-ERROR-EATEN-BY-LOGGERS — 粘性回写错误被只打日志的路径吃掉，fsync 随后对着洞报成功
- **Trigger** (2026-09-13，同上评审;**既有缺陷**，来自 `2949372`): `flush_error` 的设计是
  errseq_t 式"一次报告消费一次"，由 `flush_inode` 消费并上报。但 `main.rs:385-395`
  (periodic_sync，每 30 s)、`dispatch.rs:264-273`(Destroy)、`dispatch.rs:841-857`
  (revoked Release) 都调 `flush_inode`，拿到错误后**只 `tracing::warn!`**。
- **后果**: 粘性记录就此消失;应用下一次 fsync 通过 `take_flush_error`、继续 `put_inode`、
  **对着空洞返回成功** —— 正是 `write.rs` 与 `schema.rs` 里说这个机制存在所要防的那件事。
  讽刺的是那两处注释还把"periodic sync"列进了"MUST see it"的消费者名单。
- **Scope**: 让只打日志的消费者不要消费(改成 peek 而非 take)，或让它们负责上报。
  多槽不改变暴露面(单槽时同样)。
- **Acceptance**: 一次失败的 flush 之后，periodic_sync 先跑一轮，应用的 fsync 仍然报错。
- **Status**: `passes: true` (2026-09-13) — 已修、已回归，但**验收口径被替换过，必须说明**。
  原文验收是"一次失败的 flush 之后，periodic_sync 先跑一轮，应用的 fsync 仍然报错"。
  实测那条**在 PS 保持死亡的前提下不具鉴别力**:那时 pre-fix 的那次 fsync 也会红 —— 只是红
  在它自己的 put 失败上。两边都红，测了等于没测。**这个限定是必须说明的**:它不是说原文验收
  写错了，而是说我这次只在"PS 一直死着"这一种造法下试过，而那种造法恰好把两边都染红。
  所以断言换成**修复真正改变的那个性质**:粘性记录在"只打日志"的调用者之后是否存活
  (`crates/manager/tests/system_fuse_flush_error_sticky.rs`)，外加对称的另一半(会上报的
  调用者确实消费掉它,否则一次失败会永远报下去)。
  实证:pre-fix 红在 `:107`("a logging-only flush consumed the sticky record")，post-fix 绿。
  **原文验收的危害级变体是可行的，未做,另立后续项**:把 PS 重启回来再 fsync —— 那时 pre-fix
  的 fsync 会返回 Ok 并把 size 发布到洞上面(`put_inode` 打的是 inode key,可能落在另一个
  分区,所以 extent 那边失败不蕴含它也失败),post-fix 则仍然报错。那才是直接钉住"对着洞报
  成功"的那条。
  修法:`flush_inode` 加 `FlushReport{ToApplication,BestEffort}`，只有前者 take。
  ⚠️ **`dispatch.rs` 的 Release 一开始复用 `propagate_flush_err`(= `!revoked`)当判据,那是错的**,
  评审抓出后改成**恒传 `BestEffort`**:fuser 的契约明说 release 的错误值"不会返回给触发它的
  close()/munmap()",所以非撤销的 Release 同样谁都没告诉,没资格消费。连带把退化成常量的
  `flush_report_for` 和它那两条映射单测一并删掉(其中 `normal_release_consumes_because_it_reports`
  钉的正是这个假前提)。
  ⚠️ **第三扇门,opus 评审抓出(本条 Scope 原文没点到它)**:`read.rs` 的 read-after-write 屏障
  同样在消费记录,而"read-after-write barrier"正是我自己写进 `ToApplication` 注释里的。它的
  `?` 确实把 EIO 交给了调用者,但**读不是回写错误的退休处** —— Linux errseq 只在
  fsync/close/msync 退休一次。危害链:回读拿 EIO → 记录被吃 → 重试读成功 → close 的 fsync
  无事待办 → size 发布到洞上面。改成 `BestEffort`;测试
  `the_read_after_write_barrier_must_not_eat_the_sticky_flush_error`,消融红在 `:226`。
  连带修:Release 的 `return Err(e)` 跳过 open_count/驱逐/租约释放(fuser 每 open 只发一次
  release ⇒ 永久漏),改为延迟到块尾返回,消融红在 `:158`。
  ⚠️ **第四、五扇门(二轮评审抓出)**:写路径自己的 **gap flush**(`write_inner` 的非连续分支)
  与 **truncate** 同样在消费记录。它们的 `?` 确实把 EIO 交给调用者,但 buffered `write()` 与
  `ftruncate` 都不是 Linux 的回写错误退休点。至此分类收敛成一条可陈述的规则:
  **`ToApplication` 只有三处 = FUSE_FLUSH(close)/FSYNC/PyO3 显式 flush**,其余六处全 `BestEffort`;
  判据是"这里是不是退休点",**不是**"这个调用者会不会把错误交给谁"——后者正是连错五处的原因。
  消融:gap 红在 `:327`、truncate 红在 `:350`(两者红点不同,证明各自非空洞)。
  代价如实记:记录存活期间 inode 恒脏、periodic_sync 每 30 s 告警、缓冲尾巴不落盘,直到
  ToApplication 调用者清掉 —— 故意取舍(响亮卡住 > 静默的洞),写在 `FlushReport` doc 里。
- `passes: true`
### F-SPLIT-NEEDS-COMPACT — split 前需要先 compact，而策略与面板都不知道这件事
- **Trigger** (2026-09-09，用户): 「split前要compact，dashboard或者policy要知道」。
- **实测到的现象**: `auto-policy` 的 recent actions 里排着一列相同的拒绝：
  `[refused] autumn-op split 168: rpc error (FailedPrecondition): cannot split: partition
  has overlapping keys`。策略每个周期发一次、每次被拒。part 168 已 50 GiB、超过
  `size>53687091200` 的阈值，切不开就只会继续长。
- **为什么它不会自己好**: 拒绝来自 PS 侧的 `part.borrow().has_overlap.get() != 0`
  (`rpc_handlers.rs:1490`)。重叠是一次 CoW split 的遗留——父子共享 extent
  (168 与 204 共享 22,169,172,188,190,200 六个)，要靠 compaction 把各自的数据重写进
  自己的 extent 才会消失。**而策略的冷却 `last_op_at` 只在真的产出了两个孩子之后才盖章**
  (`rpc_handlers.rs:3686`)，被拒的 split 根本不进冷却，所以每周期照发。
- **Scope**(未实现，三条按性价比排序):
  1. **策略在发 split 前先发 compact**。它已经会发 major-compact 建议，缺的是
     "这个分区想切但重叠着 → 先 compact"这条因果。
  2. ~~**被拒的 split 要进冷却**~~ —— **已修** (2026-09-10)。根因比预想的更简单也更普遍：
     auto-policy 的执行循环只在 `Ok(())` 分支写 `st.cooldowns.insert(key, now)`，`Err` 分支
     只记一条 refused 就走 —— **任何**被拒的动作都会在下一跳原样重发，不只是 split。
     现在拒绝也起冷却，并且 `cooldowns_changed` 让这类 tick 也持久化冷却(否则 manager
     一重启又立刻重试)。这是限流不是封禁：条件清了下个窗口自然会再拿起它。
     ⚠️ **没有单测**：这条路径在一个带 I/O 的 async 循环里，要造一次真实的 actuation 失败
     才测得到，靠读代码核对。manager 346 个单测仍全绿。
  3. **面板要说人话**。现在只显示一条 `FailedPrecondition: overlapping keys`，读者无从知道
     该做什么。应当显示"等待物理分离；先 compact"并给出那条命令。
- **Acceptance**: 一个重叠未消的分区上，`auto-policy` 的 recent actions 不再出现连续的
  `split ... refused`；面板对该分区显示的是"需要先 compact"而不是一条裸错误。
- **Status**: `passes: false` (2026-09-09) — 仅立账，未实现。手工推动的办法是对两个分区
  各发一次 `autumn-op compact`(最近 17、164 都 compact 成功过)。

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

### BUG-FUSE-INVAL-ON-DISPATCHER-THREAD — 失效通知在 dispatcher 线程上做阻塞写，可能死锁
- **Trigger** (2026-09-12): 独立评审在审 `--direct-io` 时发现，**与该改动无关，是既有隐患**。
- **代码事实**: `main.rs` 的失效闭包调用 `notifier.inval_inode(ino, 0, 0)`，fuser 侧是对
  session fd 的同步 `write(2)`。它从 lease 轮询任务发起，跑在**同一个 compio runtime**上：
  每个事件一次；溢出哨兵与**任何轮询传输错误**时对**每一个持有的 inode** 各一次。
  而一个 FUSE 读要被应答，必须先由 dispatcher 跑 `read::prepare`（需要 `&mut state`）。
- **推断的后果**（未复现）: `FUSE_NOTIFY_INVAL_INODE` 走 `invalidate_inode_pages2_range`，
  要拿 folio 锁，而预读持有该锁直到对应 FUSE_READ 被应答。若那条读还排在桥接通道里等
  同一个被阻塞的线程，双方互等；FUSE 无超时，读者永久 D 状态。
  **触发场景是只读挂载也有的**：加载途中 manager 重启或连接抖动。
- **Scope**（未实现）: 先按评审给的判据复现——`cat bigfile` 循环 + 中途重启 manager，
  看 `autumn-fuse-compio` 线程是否停在 `folio_wait_bit`/`__lock_page`（`/proc/<pid>/task/*/wchan`）、
  读者是否 D 状态。确认后的修法形状：把通知移到专用线程发（`Notifier` 是 `Send + Clone`）。

### BUG-FUSE-WRITE-READ-DUAL-OPEN-EBUSY — 同挂载对同一文件"写打开时只读打开"返回 EBUSY，ffmpeg faststart 全挂
- **Trigger** (2026-09-16，ComfyUI 线上故障定位 + 在 comfyui-autumn pod 实测复现):
  SaveVideo 保存 mp4 必然失败 (`av.error.OSError: [Errno 16] Device or resource busy`)。
  根因链：ffmpeg 的 MP4 faststart (`shift_data()`) 在写 trailer 时**保持写句柄、再以 O_RDONLY
  重开同一文件**做 moov 搬移；而 FUSE 客户端 `FuseLease` 每个 ino 只有一个 `mode` 槽。
- **实测证据** (pod `comfyui-autumn-57d759f7f-65zvm`)：PyAV 走 SaveVideo 同一代码路径，
  5 种尺寸 100% EBUSY；去掉 faststart 全部成功；纯 Python 三种双开中
  read+read OK、write+write OK、**write 时 read-open FAIL**——manager 侧
  (`inode_lease.rs` 的 `acquire(READ)` 无条件插入 readers、只对"别的" writer 报
  WriteConflict) 本来允许读写共存，拦截纯发生在 FUSE 客户端。
- **代码事实**: `crates/fuse/src/dispatch.rs:576` Open arm
  `if slot.mode != req_mode { return Err("lease mode mismatch ...") }` →
  `err_to_errno` (ops.rs:458) 把 "lease mode mismatch" 映射成 EBUSY。
- **Scope**（用户 2026-09-16 确认的形态）: `FuseLease` 按角色拆分：
  ```rust
  pub struct FuseLease {
      pub writer_refs: u32,   // O_WRONLY/O_RDWR 的 fd 数
      pub reader_refs: u32,   // O_RDONLY 的 fd 数
      pub mode: u8,           // manager 侧当前持有的最强 lease (WRITE > READ)
      pub lease_epoch: u64,
      pub revoked: bool,      // 语义不变
  }
  ```
  - Open: req=READ 且 writer_refs>0 ⇒ 只 bump reader_refs、零 RPC（本挂载写路径
    自持缓存一致性，manager 不需要知道）；req=WRITE 且 reader_refs>0（升级）⇒
    manager `acquire(WRITE)`（同 client 幂等），更新 mode/epoch；同角色 bump 不发 RPC。
  - Release: 对应角色 refcount 减 1；writer_refs 1→0 且 reader_refs>0（降级）⇒
    `lease::release` + `lease::acquire(READ)` 重注册为读者，避免残留读 fd 把 writer
    槽占死挡住其他挂载的写者（tail -f 场景）；双双归零走现有 drop + release。
  - 连带: `check_write_allowed` (writer_refs>0)、`compute_release_action`
    （按角色 + 总 refcount 判定，签名加 role 参数）、`write_lease_for`
    (state.rs:281, fencing 戳看 writer_refs) 三处同改；"lease mode mismatch"
    的 EBUSY 映射成死代码按规范删除；测试 helpers (dispatch.rs:1133/1237) 适配。
  - **先做（同改必踩）**: Lance writer 的"写 manifest 时读旧 manifest"模式与 faststart
    同型，本条不修则 F-LANCEDB-OBJECT-STORE 的 FUSE demo 路径不可用。
- **Acceptance**:
  - 单测: open(W)→open(R)→close(R)→close(W) 全序列的 refcount/mode/release 决策
    状态机；降级路径；降级中途 revoked；三个连带函数的新语义；
    消融——恢复 mode mismatch 检查必须变红。
  - e2e: 挂载真实 autumn-fuse，跑 PyAV faststart 复现脚本（当前 100% EBUSY）应成功；
    回归——双挂载 reader+writer、`echo >> f` + `tail -f` 并存且写者不被挡、
    单挂载写后读同 fd 字节一致。
  - 端到端: ComfyUI SaveVideo (mp4 + faststart) 在 autumnfs output 上成功。
- **Status**: `passes: true` (2026-09-18) — 已按确认方案实现并验证。最后一条
  ComfyUI SaveVideo 端到端**由用户自己在 comfyui-autumn pod 上跑**（本机没有 ComfyUI），
  用户据此判定通过；等价的 `ffmpeg -movflags +faststart` 路径已在真实挂载上验过。
  已完成：`FuseLease` 拆 `writer_refs`/`reader_refs`（`mode` = 当前在
  manager 持有的最强租约）；Open 的 READ-on-writer 零 RPC、readers-then-writer 走
  `acquire(WRITE)` 升级、Granted 用 `entry()` 合并不丢已开读 fd；RELEASE 经
  `bridge`/`ops` 拿到内核回传的 open flags 按角色减计数，最后一个写 fd 关闭且仍有读者时
  先 flush 再 `release`+`acquire(READ)` 降级；`check_write_allowed` /
  `write_lease_for` / `compute_release_action` 改看角色计数；"lease mode mismatch" 的
  EBUSY 臂按规范删除；PyO3 `Fs.acquire/release` 同步按角色记。
  验证：fuse lib 63（新增 dual-open/降级/revoked 不降级/faststart 全序列纯函数状态机）；
  真集群 `fuse_lease_1` 11（新增同挂载 dual open、降级让出 writer 槽、**跨挂载只读者共存
  且收到 WriterClosed**）、`fuse_lease_2` 6、`bug_lease_3` 4、`system_fuse_read` 5
  （新增第二个挂载线性读看得到追加数据）、其余 fuse system 测试全过；真实挂载 e2e
  （release 二进制 + 3 EN + fusermount3）：dual open、`ffmpeg -movflags +faststart`
  产出可被 ffprobe 解析的 mp4、reader 活过 writer 后追加、3 MiB 往返字节一致。
  消融两级：把 `slot.mode != req_mode` 加回去，两条集群测试变红；用带该检查重编的
  release 守护进程跑 e2e，T1/T2/T3 全失败且日志 3 次 "lease mode mismatch"。
  **独立评审（fable subagent）抓到一条自引入的高危洞并已修**：降级原先不看
  flush 结果，而 `flush_inode` 在 `write_region` 前就清零了 `wb.len`，flush 失败会留下
  `dirty=true` + 覆盖这些字节的 `meta.size`；把 writer 槽还回去之后，别的挂载拿到写者、
  追加，本挂载后续的 periodic sync / 读 fd FLUSH/RELEASE 会把陈旧 size 盖上去（且
  `write_lease_for` 当时按 `writer_refs` 判、stamp 成 ANON = 不围栏），再一次 grow 的
  `clean_beyond_eof` 就会删掉对方的 extent。修法：降级增加
  `deferred_flush_err.is_none()` 前提，flush 失败则保持写租约（等同改动前）；
  `write_lease_for` 改回按 `mode` 判（"还持不持有写租约"与"有没有开着的写 fd"是两个
  问题），release 成功即把 `mode` 置 READ。回归测试
  `a_failed_last_writer_flush_keeps_the_write_lease`（杀 PS 造真实 flush 失败，断言第二
  个 client 的 `acquire(WRITE)` 仍是 Conflict），消融去掉该前提即变红。评审的其余项
  （ops.md 第 3 步不可执行且走的是升级路径而非降级、真实挂载没有证据表明 RELEASE 角色
  接对了、"TTL backstop" 措辞错误、`last_writer` 用 `<=1`、`mode` 沦为只写字段、
  绑定无降级）均已处理：ops.md 改成两个挂载点的可执行步骤，e2e 增加 T5（第二个挂载拿到
  writer 槽 + 守护进程日志确认降级真的发生），其余按上述改正或写进 crate 指南。
  已知自愈状态：升级路径会让本 client 同时在 manager 的 `readers` 与 `writer` 里，
  `release` 的 writer 分支提前返回不摘 reader，留下的幽灵 reader 由
  `inode_lease.rs` 的 `tick_reader_expiry` 按 TTL 回收（本地 `held_leases` 条目已删，
  不再续租），空 inode 条目随后一并丢弃。
- `passes: true`

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

### F-LANCEDB-OBJECT-STORE — autumn 作为 LanceDB 的原生 object_store 后端
- **Trigger** (2026-09-16，用户定调走原生 object_store 路径而非 FUSE 直跑；
  memory 现有实现定位为"简化的 lancedb"，二者长期不竞争，memory 未来可选
  Lance 做大向量索引后端)。立论：lance/lancedb Rust 侧底层是 Apache `object_store`
  crate，支持自定义 ObjectStore 注入 (`ObjectStoreParams`)；lance 的存储需求
  （不可变列式文件 + 前缀 list + manifest 原子提交）与 ordered KV + MVCC 同构，
  CAS 语义比 S3 后端（需外挂 DynamoDB 锁）更原生。避开 FUSE 用户态开销与
  list/stat 密集元数据路径。
- **前置**: BUG-FUSE-WRITE-READ-DUAL-OPEN-EBUSY 先修——仅影响 FUSE demo 路径，
  但 demo 是本条的第一步验收。
- **Scope**: (1) FUSE demo 路径跑通 `lancedb.connect("file:///mnt/autumn/lancedb")`
  建/写/查一张向量表（验证 rename/原子替换语义在 KV 上的表现）；
  (2) `autumn-object-store` 适配层（新 crate，预估 1–2k 行）：`put/get/delete/list`
  映射 KV streaming 原语，manifest 提交映射 **put-if-absent / CAS**（基于 MVCC，
  这是整个集成唯一需要认真设计的语义点）；(3) list 性能——KV 前缀 range scan
  在元数据文件增多后的分页延迟基准（query planning 每次 scan 目录，决定小查询
  P99）；(4) 读写 batch 布局核对——lance 按 batch（几百 KB–几 MB）读向量，
  与 1MB 随机读 355MB/s 的实测对上即可，4K 碎片读偏弱但 lance 文件布局规避。
- **Acceptance**: demo 路径——表创建、追加、向量检索、删除全通过，双开修复后
  writer+reader 并发不 EBUSY；object_store 路径——并发双 writer 提交不互相覆盖
  （CAS 生效，消融：改成 last-writer-wins 测试变红）；list 1000+ fragment 的
  scan 延迟有基准数字；与 S3 后端跑同一基准集对比吞吐/P99。
- **Status**: `passes: false` (2026-09-16) — 定调与范围已确认，未开工。
