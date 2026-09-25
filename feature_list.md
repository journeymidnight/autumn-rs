# autumn-rs feature list — OPEN backlog

**Last updated:** 2026-09-25

**Rules:**
- This file tracks the **OPEN backlog only**. A feature that reaches `passes: true`
  is **DELETED** from here — git history is the record, there is no archive file
  (CLAUDE.md rule 13: 定期清理删除，保持整洁).
- `passes` and `notes` are the only mutable fields after a feature is created.
- Out-of-scope / "v2 再做" decisions must be recorded as proper feature entries
  (F-name + Trigger + Scope + Acceptance + `passes: false`), never as plan-file footnotes.

---

## Active

### F-REVIEW-R1-GC-COMPLETE-SCAN — P1 GC 完整扫描证明
- **Trigger**: review.md R1；提前 EOF 或 record 边界短读可绕过 carry 检查并误 punch。
- **Scope**: 每次读取必须满足 want，punch 前检查 sealed_length 和 carry。
- **Acceptance**: 截短到 0/record 边界、有无 checksum 均拒绝 punch 或完整搬迁；重启逐字节验证 live VP。
- `passes: false`
- **notes** (2026-09-20): 已实现逐次 want 精确长度校验和 punch 前 sealed_length/carry 双重校验；5 条 GC streaming 单测通过，新增完整 record 边界及 offset=0 提前 EOF 回归。尚未完成真实双副本截短、checksum 两种状态及 PS 硬重启组合验收，不能按完整 R1 验收关闭。

### F-REVIEW-R6-FENCE-CAPACITY — P2 Fence placement 与容量预检
- **Trigger**: review.md R6；non-force 在没有合法 spare 时仍成功，1.2 倍容量承诺未实现。
- **Scope**: 按 Recovery placement 排除 occupied/不可用节点；按副本或 shard 字节与实际 headroom 预留容量，缺失容量信号保守失败。
- **Acceptance**: 无 spare、fenced/maintenance/suspected spare、容量不足均拒绝；合法目标可通过；force 保持显式覆盖。
- `passes: false`

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

### F-REVIEW-V1-MERGE-REPLAY — 待验证：merge replay cursor 可达性
- **Trigger**: review.md 4.1；数值模型不足以证明正常 merge 丢失数据。
- **Scope**: 复现 raw merge、checkpoint 失败、旧状态和 sealed-empty cursor 回收；按可达性决定修复。
- **Acceptance**: 真实调用链固定时序及 ACK 数据验证，记录可达或不可达的证据。
- `passes: false`

### F-REVIEW-V3-COMPACT-CHECKPOINT — 待验证：checkpoint 失败后 GC 恢复链
- **Trigger**: review.md 4.3；compact 在 checkpoint 成功前替换内存表。
- **Scope**: 验证 checkpoint 失败、GC relocation、row truncate 与前台写入交错；证明 durable 集合完整或修复发布顺序。
- **Acceptance**: 固定失败窗口后硬重启，所有 ACK 数据与 tombstone 正确，修复消融能变红。
- `passes: false`

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

### F-LANCEDB-S3-GATEWAY — 补齐 LanceDB 所需的 S3 API
- **Trigger** (2026-09-23，用户要求): `autumn-s3` 当前只提供 ListObjectsV2、GET 和 HEAD，
  LanceDB 无法通过 `s3://` endpoint 建表和写表。目标是让现成的 LanceDB S3 客户端直接
  读写 autumn，无须修改 LanceDB 或使用 FUSE。
- **Scope**: 保留现有 `s3://<bucket>/<key>` 到 `fs/<bucket>/<key>` 的映射，补齐
  LanceDB 实际使用的对象操作：PutObject（含 `If-None-Match: *`、`If-Match` 条件）、
  DeleteObject、DeleteObjects 批量删除、CopyObject，以及大对象的
  CreateMultipartUpload / UploadPart / CompleteMultipartUpload / AbortMultipartUpload；
  完善现有 HeadObject、GetObject（Range 与条件读取）、ListObjectsV2 的兼容性，
  并支持客户端实际发出的 bucket 存在性检查。以固定版本的 LanceDB/Lance 和其
  S3 `object_store` 请求轨迹核对上述清单，发现缺项时补入同一 feature。
  Multipart 分片完成前不可作为最终 key 可见；条件写入必须由底层原子比较操作保证，
  不能用先 HEAD 后 PUT。错误响应、ETag、分页和 XML 需能被客户端解析。
- **Boundary**: bucket 由 `fs/` 一级目录预先创建；本条不承诺 CreateBucket、ACL、
  versioning、虚拟主机寻址或 SigV4 验签。网关维持现有鉴权模型；这些能力若实际被
  固定版本的 LanceDB 调用，再按请求轨迹纳入范围，不以“理论上 S3 支持”扩张协议面。
- **Acceptance**: 使用固定版本的 Python LanceDB 指向网关 endpoint，在预建 bucket 上
  完成建表、追加、重开读取、向量检索、删除和 vacuum；小对象与超过单次 PUT 阈值的
  数据文件均字节一致。两个独立写者并发提交同一张表时不得静默丢提交，冲突必须
  通过条件写入反馈给 LanceDB；取消或失败的 multipart 不得发布不完整对象。
  验证 DeleteObjects、CopyObject、分页 list、Range/条件 GET、缺失 key 与条件失败
  的 SDK 可解析响应；网关重启后已完成对象仍可读，现有模型加载只读用例回归通过。
- **用户补充要求与计划** (2026-09-24): 详见
  [LanceDB S3 gateway 实施计划](docs/lancedb_s3_gateway_plan.md)。
  **硬性验收：CompleteMultipartUpload 不读取、不复制、不重写任何分片正文**；
  通过共享 FS 分段映射完成元数据拼接，禁止后台全量合并或首次访问／修改时全量转换。
  使用数据路径计数、正文 I/O 故障注入及冷启动测试验证。支持 S3、FUSE、Python Fs
  混合访问；已有写者占用立即报冲突，S3 GET 期间禁止原地修改；允许统一协议和格式升级。
  Abort 必须阻止迟到分片及发布，并支持重启后继续清理；不得误删已完成对象的组成数据。
- `passes: false`
