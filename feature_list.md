# autumn-rs feature list — OPEN backlog

**Last updated:** 2026-09-11

**Rules:**
- This file tracks the **OPEN backlog only**. A feature that reaches `passes: true`
  is **DELETED** from here — git history is the record, there is no archive file
  (CLAUDE.md rule 13: 定期清理删除，保持整洁).
- `passes` and `notes` are the only mutable fields after a feature is created.
- Out-of-scope / "v2 再做" decisions must be recorded as proper feature entries
  (F-name + Trigger + Scope + Acceptance + `passes: false`), never as plan-file footnotes.

---

## Active

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
- `passes: false`

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
- **Status**: `passes: false` (2026-09-09；2026-09-10 修了第一半；2026-09-11 修了 Scope 1) —
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
  **仍未做/未验**:
  ① 验收原文要求"默认配置下被选中并**回收**"——单测证明了判据会选中它，
     但**没有在一个真集群上复验**回收确实发生、`gc_debt` 随之下降;
  ② 验收后半"共享 extent 两个分区报告的债务之和不超过实际死字节"**不实现** ——
     见本条 2026-09-10 的订正:dedup 会让 refs 永远归不了零。按订正落到呈现层
     (extent 视图显示 `refs` 与共享它的分区)，面板已有位置，未做。
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
- **Status**: `passes: false` (2026-09-13) — 仅立账，未复现未修。
- `passes: false`

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
- **Status**: `passes: false` (2026-09-13) — 仅立账，未修。
- `passes: false`
