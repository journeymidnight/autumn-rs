# autumn-rs feature list — OPEN backlog

**Last updated:** 2026-09-08

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

### F-KV-CLIENT-30K — 单个客户端进程的 KV 写吞吐卡在 ~30K ops/s，与分区数/并发/批量都无关
- **Trigger** (2026-09-02, 从 F-MEM-WIPE-COST 的残余里分离出来): 同一个单线程 memory-mcp
  进程，无论怎么配都拿不到超过 ~30K key/s 的写（delete 或 put）：
  | 变量 | 取值 | 删除 2M key 耗时 |
  |---|---|---|
  | 分区数 | 1 → 4（数据实测分成 46/21/13/20%） | 70.2 s → 76.2 s |
  | 删除并发 | 32 → 256 | 67.8 s → 68.9 s |
  | 页大小 | 512 → 4096（页数 3883→486） | 67.8 s → 68.7 s |
  | 4 分区 × 256 并发（补的那格） | — | 73.9 s |
  ingest 走批量 put 也是同样的 ~30K key/s（2M key / 66 s）。
- **已排除**: 客户端 CPU（全程 16% 单核，不是 CPU 绑定）；磁盘（`fsync_isolated` 实测
  p50 56~60 µs、group-commit K=256 摊薄 237K ops/s，比观察值高一个数量级）；
  扫描（`wipe breakdown` 显示只占 2.8 s / 4%）。
- **Scope（真要做时）**: 先用 `perf-check --threads N` 对照——它多线程能到 98K~162K，
  说明不是集群侧的绝对上限，那么"单进程 30K"要么在 SDK 的某条串行路径上，要么在
  单条 PS 连接 / 单个 compio 事件循环上。**先定位到具体的串行点再改**，不要先猜。
- **Acceptance**: 有一个能解释 30K 的具体机制（火焰图或分段计时指到某一处），
  且改动后单进程写吞吐提升可复现。
- **2026-09-04 机制假说（纯读代码得出，未测量）——它一次解释掉上表全部四个变量**：
  wipe 的 scan 是 `range(prefix, start, 512)`，返回的是 **512 个连续 key**；连续 key 落在
  **同一个分区**。而 client 用的是**每分区一条多路复用连接**（`crates/client/src/lib.rs`
  多处注释："per-partition multiplexed connections"）。于是每一页的 `delete_many`
  实际上只打**一个分区、一条连接**，与集群有几个分区无关。
  - 分区 1→4 无效：每页仍然只落一个分区，多出来的连接这一页根本用不到；
  - 并发 32→256 无效：全部排在同一条连接上，只是队列更深；
  - 页大小无效：还是单分区批次，只是更大；
  - CPU/盘无效：本来就不是资源绑定。
  而 perf-check 多线程能到 98~162K，是因为 key 随机散布**且**16 线程 ⇒ 多条连接同时在飞。
  **可证伪的预测**：把"扫描/删除"改成跨页流水（不同页打不同分区）应该能线性scale，
  而在**同一页内**加并发不会。先测这个，别再动上表那四个变量。
- **⚠️ 上表"删除并发 32→256"那一行今天无法复现**：`delete_many` 的扇出宽度是**编译期常量**
  `BATCH_PUT_DEFAULT_CONCURRENCY = 32`（`crates/client/src/lib.rs:639`），memory-mcp 与
  autumn-memory 里**没有任何** env/flag 能改它（已全量 grep）。所以那一行要么是改了常量重编，
  要么当时旋的是别的东西。重测前先确认旋钮真的接到了扇出宽度上。
- **2026-09-04 机制已实测确认，上面那条假说对了一半**：串行点确实在"单分区单连接"上，
  但真正的上限是那条连接的**在飞数**。**不变量是每分区连接 ~950 次 append/s**
  （cap 4 ÷ 每次 append 1.4~2.2 ms），key/s = 它 × 每 append 的 key 数：
  | 负载 | append/s | key/append | key/s | 出处 |
  |---|---|---|---|---|
  | delete（`MSG_DELETE`，每 key 一个请求） | 893 | **3.10** | 2,768 | PS `partition write summary` |
  | batched put（`MSG_BATCH_PUT`，每分区一帧装 N 个） | ~950 | **~31** | **29.5K** | 同上，~30 个连续采样 29,700~30,200 |
  put 的 30K 在单分区、单连接、cap 确认为 4 的条件下**精确复现**；它与 delete 的 2.7K
  是**同一个事实**，11 倍差距纯粹是 key/append 之比。机制是 `MSG_BATCH_PUT` 每请求装
  ~31 个 key，**不是** PS 侧 group-commit 合并请求 —— 两种负载下 PS 看到的在飞请求都只有 ~1.2 个。
  （本条先前两版都写错过：先并排写成"吻合"，后又归因为"单位可疑"。单位一直相同，都是 key/s。）
- **那 1.39 ms 的 ~95% 是跨 AZ 复制，不是磁盘**：日志流三副本分处 cn-beijing-b/d/e，
  而 `apply_completion` 要求**每个副本都 ack、没有 quorum** ⇒ 延迟 = max-of-3 次往返。
  同 AZ RTT 23 µs，跨 AZ 388–399 µs（17 倍，两组独立数据）。EN 侧 pwrite+fsync 实测
  0.059–0.062 ms，只占 4% —— 本条最初"已排除磁盘"是**对的**，错的是由此推出的"往上层找"：
  写路径受**复制网络**约束，不在 SDK 里。副本放置是 `crates/manager/src/lib.rs:3775` 的
  `pool.shuffle(&mut rng).take(count)`，**manager 全无 zone/rack 概念**（已 grep），
  三区分布是随机抽样结果、不是持久性设计。
  另有一项**每字节成本**实测：(2.1 − 1.39 ms) / ~28 个额外 key ≈ **25 µs/key**，叠在
  ~1.3 ms 的固定跨 AZ 成本之上；批 31 时固定成本仍占 ~20 倍，EN 侧仍是每 append 一次 fsync。
- **⚠️ 但标题里 delete 的 30K 仍然无法调和**（2026-09-04）：三次独立 wipe 一致在
  ~2.7K key/s（109K key / 40 s、939K key / 350 s、PS 侧 2,769）。要达到 28.5K
  （2M key / 68~70 s，单分区那一行）需要 ~40 个 `MSG_DELETE` 在飞或 ~0.14 ms 的 append，
  两者在代码和集群里都不存在。**最后一个候选解释也已排除**：cap 4→16 的 bump 与回退
  （`d6aa298` / `3f5d3a9`）都在 2026-05-21，比那次测量早几个月，当时 cap 就是 4。
  ⇒ **该数字的出处需要查证**；在此之前标题的 "~30K" 只对 put 成立。
- **2026-09-05 杠杆 (a) 已实测验证，倍数比预测更好**：EN 全部收拢到可用区 C 之后，
  用同口径对照（两边 `avg_batch_size` 都是 1.0，同集群同代码，只差 PS 与 EN 是否同区）：
  | PS | 可用区 | 样本 | `avg_phase2_ms`（WAL 复制） | end-to-end |
  |---|---|---|---|---|
  | ps-0 → part 17 | b（跨区） | n=1×2 | **1.603** | 1.65 |
  | ps-1 → part 44 | **c（与 EN 同区）** | **n=39** | **0.190** | 0.193 |
  **8.4×**，且 0.190 ms 与预测的 ~0.11 ms 同量级。（首个样本 0.545 ms 是冷启动，
  被 39 个样本推翻——n=1 的数不要用。）
  按实测有效在飞数 ~1.2 推算：append/s 从 ~750 升到 ~6,300，配 ~31 key/append 的批量 put
  ⇒ 单进程 key/s 由 ~23K 升到 ~195K 量级。**注意这是外推，不是端到端实测吞吐。**
- **2026-09-05 PS 也已迁到 zone c，7/7 分区走同区路径**：PS **没有任何本地卷**
  （无 PVC / hostPath / emptyDir，状态全在 stream 层与 etcd），所以迁移是干净的——
  给 StatefulSet 加 `topology.kubernetes.io/zone: cn-beijing-c` 的 nodeSelector 钉住，
  再**并行删除**三个 pod（不用滚动：滚动会把每台的分区推给幸存者且不推回，最后挤在一台）。
  分区接管约 3.5 分钟。同一个分区 part 17 迁移前后：**1.603 ms → 0.300 ms**；
  暖机后稳定在 **0.185 ms**（n=40）。
  ⚠️ **教训**：PS 重启后的第一个样本是 **1.16 ms**（`ops=1`），看上去像"迁移没用甚至更慢"。
  与上面 0.545 那次同一形状——**n=1 是冷启动读数**。灌了 100 次写暖机后才是真值。
  这个会话里同型错误已出现 6 次，共同点是**仪器/样本对目标现象不成立时，读数和真值长得一样**。
- **⚠️ perf-check / ycsb 在开了 authz 的集群上不可用（2026-09-05 实测）**：
  两者都硬编码 `ClusterClient::connect(&mgr, BENCH_SCOPE)`——**匿名连接，忽略
  `--credential-file`**（`crates/server/src/bin/autumn_client/main.rs:301/352/530`）。
  匿名连不上 PS，而每次写的错误又在 `.is_ok()` 处被丢掉，于是唯一的症状是
  `write phase produced no keys — is the cluster running?`——一个把**认证失败**
  伪装成**集群故障**的错误信息，PS 侧连一条拒绝日志都没有（连接根本没建立）。
  **已修（`6999ed1`）**：加 `bench_connect`，四个 worker 线程改走
  `connect_with_credential`；凭据在所有网络 I/O 之前读一次，并补上空 principal 守卫。
  评审纠正了我一处**说反的因果**：worker 的 `connect` **只联系 manager、连接是成功的**，
  失败发生在 PS 侧——连接上没有 AUTH_HELLO，`authz_check` 以**错误帧**拒绝且**不打日志**，
  所以"PS 无日志"不是"请求没到达"的证据。注意 bench principal 的 grant 必须覆盖
  `bench/perf/`——perf-check/ycsb **不看 `--namespace`**。
  本轮的延迟测量因此改用 `autumn-client put` 逐条写 + 读 PS 的 `partition write summary`。
- **⚠️ 逐条 put 测不出 `ps_conn_inflight_cap` 的效果**：cap 管的是**在飞深度**，
  而每次 put 是一个新进程、一个操作，在飞深度恒为 1 ⇒ cap 4 还是 8 都一样。
  用它测只会得到"没变化"，那是**方法的产物，不是结论**。cap 的 A/B 必须等修好的
  perf-check 进镜像。cap=8 的配置链已逐段验证：`AUTUMN_PS_CONN_INFLIGHT_CAP=8` →
  `entrypoint.sh:241` → `--conn-inflight-cap 8`（已在 `/proc/1/cmdline` 确认）→
  `set_ps_conn_inflight_cap`。
- **三条杠杆，按该做的顺序**：
  **(a) AZ 收拢 —— 已做，见上。** append 1.39 ms → ~0.11 ms 会把 append/s 从 950 抬约一个
  量级，delete 不改协议就能追平 put 今天的水平，**一行 wire 都不用动**。
  **(b) `ps_conn_inflight_cap` 4→8。** `AUTUMN_PS_CONN_INFLIGHT_CAP` 已在
  `deploy/docker/entrypoint.sh` 的 `PS_TUNABLES` 表里，**不用改代码也不用换镜像**。
  线性那一半是稳的（cap 翻倍 = 在飞深度翻倍 = 吞吐翻倍）：4→8 预期干净 2x，EN 到 ~78%
  仍在余量内。**4→32 不要一步到位**：那是 8 倍需求、EN 会到 312%，只有 EN 侧 group
  commit 真的兑现才成立 —— 而"每 fsync 的 append 数 ≈ 1"是**推导非读数**
  （`avg_write_ms` 是每请求的，60 µs/请求 恰等于裸设备 fsync 的 59 µs），未验证。
  注：`ps_conn_inflight_cap` 的文档注释记着一次 4→16 的 bump 被 revert（`d6aa298`），
  但那次量的是**读**（8 MiB 读在 cap4 就已 NIC-bound），不构成对写侧的反对。
  **(c) `MSG_BATCH_DELETE` —— 值得做，但最后做。** 现有 `MSG_DELETE=0x42` /
  `MSG_BATCH_PUT=0x53` / `MSG_BATCH_PUT_BULK=0x5A`，**没有批量删**；`delete_many`
  的注释自陈 "pure client-side fan-out (no server MSG_BATCH_*)"（`client/src/lib.rs:4003`）。
  加它是**纯加法的 opcode**，按 `rpc/src/lib.rs:85` 的约定 post-R3 本该 `MIN=MAX-1` 滚动升级，
  **但本树尚未 post-R3**：客户端握手只做一次 `wire_compat_check`，协商结果没有存下来供调用点
  门控（`client/src/lib.rs:1524`）⇒ 只能 `MIN=MAX`，即 **stop-the-world + 重建每个内嵌
  客户端的镜像**。它的长期价值不在延迟而在 **EN 负载**：同样的 key 吞吐下 delete 的 append
  数是 put 的 ~10 倍（3.1 vs 31 key/append），而 EN 的 `req_count` 实测就等于 append/s。
- **2026-09-05 杠杆 (b) `ps_conn_inflight_cap` 4→8：实测无效，本条预测是错的。**
  32 线程 / 1 KiB / 25 s，PS 与 EN 同区：
  | cap | 写 ops/s | p50 | p99 |
  |---|---|---|---|
  | 8 | 21,242 | 1.36 ms | 4.39 |
  | 4 | 18,991 / 20,868 / **21,574** | 1.34–1.37 ms | 2.35 / 6.01 / 3.12 |
  cap=4 的**跑间方差 13%，已经完全覆盖 cap=8 的读数**。上面写的"线性那一半是稳的，
  4→8 预期干净 2x"**不成立**——而本条自己早就记过原因，只是没连起来：
  "两种负载下 PS 看到的**在飞请求都只有 ~1.2 个**"。**cap 是上限不是目标**；
  在飞深度从未接近 4，抬到 8 自然什么都不会发生。
  p99 反向佐证：cap=8 的 p99 更差（4.39 vs 2.35）而 p50 完全相同——
  典型排队行为，队列更深、吞吐不涨、尾巴变坏。
- **2026-09-05 单进程实测（`perf-check --threads 1`，AZ 收拢后）**：**862 ops/s，p50 1.15 ms**。
  `1 / 1.15 ms ≈ 870` ⇒ 单线程是**纯串行、在飞深度恒为 1**，所以 cap 对它天然无关。
  关键的新事实：**append 现在只占端到端的 16%**（0.185 / 1.15 ms）。
  剩下的 ~0.97 ms 在 client↔PS 往返与 PS 自身处理上，**不在复制路径里**——
  所以继续放宽复制方向的任何旋钮都不会有收益，后续工作必须往那 84% 去找。
- **Status**: `closed / passes: true` (2026-09-05) —— 验收两半都达成，关闭。
  **机制**：单分区单连接 + 在飞深度受限，而 append 的 ~95% 是跨 AZ 复制
  （all-replica ack 无 quorum ⇒ max-of-3 往返）。**改进可复现**：把 EN 与 PS 全部收拢到
  可用区 C 后，append **1.603 ms → 0.185 ms（8.4×，n=40）**，同一分区前后对照。
  ⚠️ **诚实标注**：我**没有**用同一工具做过"迁移前 vs 迁移后"的单进程端到端吞吐对照——
  迁移前的 30K 出处是 memory-mcp 的批量 put（31 key/append），与 perf-check 的单键 put
  不同负载。改进是在**机制的支配量**（append 延迟）上测到的，端到端的那一半是推论。
  标题里 delete 的 30K 仍然无法调和（见上），关闭时它依然是个**出处存疑的数字**。
  剩下的两条杠杆：(b) 已证否；(c) `MSG_BATCH_DELETE` **已实现（`d6a8b73`），但未部署**——
  见下面的 BUG-WIRE36-UNDEPLOYED。它的价值在 EN 负载（delete 的 append 数是 put 的
  ~10 倍），不能再指望 cap 这条线。

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
- **状态** (2026-09-05): `d6a8b73` 把 `WIRE_VERSION` 抬到 **36 且 `MIN = MAX`**
  （`MSG_BATCH_DELETE` 是纯加法的 opcode，但本树握手不保存协商结果供调用点门控，
  所以只能 MIN=MAX）。**代码已入库、线上仍是 35。**
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

### F-RECOVERY-PROGRESS — extent recovery 不上报进度，卡死与缓慢无法区分
- **Trigger** (2026-09-04，一次 fence 排干中发现，代价是整个诊断过程): 4 个 recovery
  marker 卡了约 4 小时、**一个字节没搬**，而 `autumn-op ops list` 全程显示
  `recovery running`，`recovery-stats` 显示 `4/64 在飞`，EN 进程健康、心跳正常。
  从控制面看不出"在拷"和"拷不动"的区别。真相是靠 `kubectl exec` 到目标 EN 上
  `df -B1 /data` 采样两次、发现 5.5 分钟零增长才暴露的，之后才去翻 EN 日志、读源码、
  查 etcd（`only 0/4 shards available` 的错误本身也没说明原因，见 329fa75）。
- **根因（已定位，非猜测）**: `update_progress` 全仓库只有一个真实调用点
  （`rpc_handlers.rs:4901`），数据来自 **PS 负载心跳的 `active_maintenance`**，
  所以只有 **PS 执行的 kind（gc / compact / forcegc）** 有进度。**recovery 是 EN 执行的**，
  完成经 `DfResp.done_tasks` 回报，**中途不报任何东西**，于是 `OpRecord.progress_done/total`
  恒为 0，`ops list` 无百分比可显示。
  次要的一条：`seed_replay`（op_ledger.rs:486）重建 RUNNING 条目时只填 5 个字段、
  其余 `..Default::default()`，所以 **manager 一重启，`attempts` 和 "rebuilding slot N
  on node X" 就没了** —— 那些是 leader 本地活状态，etcd 的 marker 只存派工不存进度。
- **Scope**: EN 侧的 `stream_ec_recovery_payload` 条带循环里**已经天然持有 `(offset, want)`**，
  副本恢复的 `stream_extent_from_sources` 同理持有 `(copied, sealed_length)` —— 顺着现有的
  `DfResp` 捎回去即可，**不需要新机制、不需要新 RPC 往返**（`node_health_loop` 是唯一的
  df 调用者，2 s 一次，已有 `done_tasks` 这条通道）。manager 侧接到后调
  `update_progress_by_extent(OP_KIND_RECOVERY, extent_id, done, total)`（函数已存在，
  目前只有 EC-convert 的测试在用）。⚠️ 加字段到 `DfResp` 是 **wire 改动**（指纹变更 +
  `MIN=MAX` 全停），所以要么攒到下一次 wire 升版一起做，要么想办法塞进现有字段。
  按仓库既有约定，进度是**原始计数不是百分比**（消费者自己算比例），单位用字节。
  `seed_replay` 那半可选：failover 后进度可以从下一次 df 自然回填，`attempts` 则确实丢了。
- **Acceptance**: 一次真实的 EC 分片重建（分片 >1 GiB）中，`autumn-op ops list` 显示的
  `progress_done` 随时间单调增长；把源端人为掐断后，进度**停止增长**且该状态在
  `ops list` 上可见 —— 即"卡死"和"缓慢"在控制面上可区分，不必再 exec 到节点上量 `df`。
- **Status**: `passes: false` (2026-09-08) — **代码已实现、待真集群验收**。EN 侧
  `stream_ec_recovery_payload` 每条带（64 MiB）在写盘之后报一次
  `(OP_KIND_RECOVERY, extent_id, 已写字节, 分片长度)`，走既有的 `DfResp.op_progress`
  （**未动 wire**——该字段早已存在，本条当初担心的 wire 改动已由 EC convert 那条先做掉）；
  每次尝试开始归零、任务结束（成功/放弃）由 `OpProgressGuard` 的 Drop 清 slot。
  单测 4 条 + 4 项消融验证变红（`cargo test -p autumn-stream`）。验收里"分片 >1 GiB
  的真实重建、掐断源端后进度停止增长"两条要在线上集群做，尚未做。
  （2026-09-09 续）Scope 里剩下的两半也做了，代价是 **wire 37 → 38**（全停）：
  - **失败原因走心跳**：`DfResp` 新增 `op_failures`，EN 每次尝试失败即上报
    `(extent_id, kind, error_code, reason)`，manager 用 `record_node_op_failure`
    更新 RUNNING 条目。此前 EN 侧的失败只到自己的日志，manager 要等**下一次重派的
    响应**才知道原因，而重派受指数退避控制 —— "在失败"和"只是慢"在控制面上到那时
    才分得开。该函数**只更新不创建**（理由同 `update_progress_by_extent`）。
  - **`seed_replay` 读 marker 的 `started_at`**：此前盖成"现在"，manager 一重启，
    跑了四小时的 op 显示成刚开始。marker 里本来就存着（`MgrExtentInflightRecord`）。
  - **`OpRecord.attempts` 删除**：它想代理的问题（"在重试吗？为什么？"）已被实时
    reason 直接回答；而它在 manager 重启后从 0 重数，本身就会误导。
  各配 1 条单测 + 消融验证变红（`cargo test -p autumn-manager --lib`，330 passed）。
- **Status**: `closed / passes: true` (2026-09-09) — **验收两条都在真实重建上观察到了**，
  在一个隔离的 6 节点实验集群上（做法见 docs/ops.md「在一个 pod 里跑一次性集群」）：
  - 分片 1,207,977,696 字节（1.125 GiB，> 1 GiB）的 EC 重建，`ops list` 上
    `progress_done` **单调增长**：`0 → 201326592 (17%) → 268435456 (22%) → …`，
    每步都是 64 MiB 的整数倍，并显示 `rebuilding slot 0 on node 9`。
  - 在比例首次非零的那一刻 `SIGSTOP` 掉一个源端（4+1 布局下少一个源就凑不齐 k=4）：
    又落地一条在途的条带后，**比例在 268435456 上冻结 19 秒不动，而 op 始终 RUNNING**，
    停在条带边界上——正是 docs/ops.md 描述的卡死形态。`SIGCONT` 之后立刻跑完，
    证明冻结由源端造成而非崩溃。
  - 用 `SIGSTOP` 而不是杀进程是有意的：杀掉源端会让读**立刻报错**，这次尝试随即失败、
    进度按设计回退成"未上报"；而真正贵的那个故障（4 小时零字节）是源端**不应答**，
    只有停住进程才复现得出来。
  - 同轮还验证了 wire 38 那条：一个失败的重建把执行节点自己的原因实时挂在 `ops list` 上
    （`ERROR[4]: reopen sealed extent 14: No such file or directory`），op 仍是 RUNNING。
    此前这句话只会留在那台 EN 的日志里。
  原状态：`passes: false` (2026-09-04) — 不阻塞任何功能，但它是本次排查里最贵的一个
  缺口：有它的话，"4 小时零字节"在第一分钟就摆在眼前，而不是要靠 `df` 采样才发现。

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
  与 F-RECOVERY-PROGRESS 是同一处观测缺口的两个面。

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

### F-EC-RECOVERY-RESUME — EC 分片重建中断后从零重来，而进度本可以直接读出来
- **Trigger** (2026-09-04，用户在排查 EC 重建卡死时提出): 一个 4.25 GiB 的分片在 90% 处失败，
  当前行为是**丢弃全部、从 0 重来**（失败路径删残片 + 清条目，非 EC 的 `Incomplete` 分支同样是
  "Drop the stub and rebuild"）。extent 满容量是 17 GiB，K=4 ⇒ 分片 4.25 GiB，重来一次的
  代价随 extent 大小线性增长。
- **为什么现在做得到**: 流式重建（cf1ce53）把分片**按偏移顺序写**，所以**文件长度本身就是进度**，
  恢复点 = `len` 向下取整到 `ec_recovery_stripe_bytes()` 边界（最后一个条带可能被写到一半，重做它）。
  权威总长是 `ec_shard_read_len` = `erasure::shard_size(sealed_length, K)`，已经存在。
- **缺的是"定年"，不是进度**: `try_adopt_completed_recovery` 对 EC 一律返回 `Unknown`，而它的
  注释自陈原因 —— "EC-shard adopt needs a `shard_size` comparison"。非 EC 路径**已经**在比
  `local_ev`/`local_len` 判 Complete/Incomplete；EC 只差这个比较。补上之后：
  `len == shard_size && eversion 相符` → Complete（直接上报完成）；
  `len < shard_size && eversion 相符` → **Incomplete → resume**；eversion 不符 → 丢弃。
- **分层的边界（必须写清，否则会做错）**: `run_recovery_task` **最后才写 `.meta`**（刻意如此：
  崩溃后残片重载成 open extent，一眼可辨不完整）。所以
  **同进程内重试**（EN 的 10 次 × 10 秒循环，覆盖绝大多数情况）条目还在内存、带着 eversion ⇒ 可 resume；
  **跨 EN 重启**残片没有 `.meta`、无从定年 ⇒ 只能丢弃重来。要让跨重启也能续，需要一份持久化的
  进度记录（eversion + shard_index + 已完成字节）—— 与 F-RECOVERY-PROGRESS 是**同一份状态**，
  两个需求应当一起设计，不要各做一份。
- **⚠️ 一个不能忽略的风险**: 没有 per-shard 内容校验和（见 F-STREAM-ATREST-CKSUM），所以
  resume 无法验证已完成的前缀是否完好 —— 早先某个 peer 返回的坏字节会被继续沿用。
  每条带的精确长度检查（0434135）只挡长度错，不挡内容错。跨重启的 resume 尤其应当等
  校验和落地后再做；同进程 resume 风险低得多（那些字节是本进程刚写的）。
- **Acceptance**: 人为在第 N 个条带打断一次 EC 重建（同进程），下一次尝试从第 N 个条带继续、
  **不重读已完成的部分**（以 peer 侧 `read_bytes` 的请求偏移为证），最终分片与未打断时逐字节相同。
  eversion 在打断期间被 bump 时，必须丢弃重来而不是续。
- **⚠️ 2026-09-05 复查发现:真正的障碍不是"进度会丢"，是"代次无从判断"**（这条比下面的
  取舍理由更硬，单独记）。一度打算用"内存记已完成条带"来续传，但那份状态重启即丢；
  改用"文件长度即进度"也不够——长度可以靠**每条带 fsync** 变可信（4.25 GiB 分片 = 68 次
  fdatasync，相对于从 K 个 peer 拉同样多的字节可忽略），但**长度不携带代次**。
  而 `run_recovery_task` 是**故意最后才写 `.meta`** 的（崩溃后残留能一眼认出不完整），
  所以重启后 `load_extents` 拿到 `DEFAULT_META`，eversion=1，
  `discover_shard_files` 把半截分片挂到这个条目上，`classify_ec_shard` 一比代次不符 →
  重建。**结论：即使长度可信，跨重启也永远不会真的续传**——正确，但没用。
  要跨重启续传，必须落一份持久化的 `{eversion, shard_index, done_bytes}`
  （与 F-RECOVERY-PROGRESS 是同一份状态，要一起设计），或者提前写 `.meta` 把代次定下来——
  后者推翻的正是"`.meta` 写在最后"的刻意设计，不可轻动。
- **Status**: `closed / wont-do` (2026-09-05，用户决定) — 不做。理由:续传不是常见情况，
  且 extent 大小本就可控，把单元切小比在大单元内部做续传更对路。
  这与业界做法一致：主流系统的选择是**把修复单元切小到重来很便宜**，而不是在单元内部
  做字节级续传——Ceph 的单元是 4 MiB object，HDFS EC 是 block，失败都整体重来；
  会 resume 的（Kafka 的 fetch offset、Raft 的 `nextIndex`）之所以能续，是因为它们的
  续传点落在**天然可验证的完整单元边界**上，而 EC 分片的字节流中间没有这种边界。
  Cassandra 是个印证：它默认也是重来，后来才加了 resumable bootstrap，粒度是**文件**不是字节。
  上面那些分析（长度即进度、重做最后一条 stripe）留档，若将来 extent 尺寸策略变了可以直接取用。

### BUG-EC-RECOVERY-WEDGE — 一次失败的 EC 重建把 (节点, extent) 永久毒死
- **Trigger** (2026-09-04，实测): EC 重建失败后，`ensure_extent` 建的本地条目留在原地；
  下一次派发走到 `require_recovery` → `try_adopt_completed_recovery` → 对 `ec_converted`
  **一律返回 `Unknown`** → `CODE_PRECONDITION "extent N already exists"` → 永久拒绝。
  manager 侧 marker 是常驻指令、每 2 秒重发一次，于是**双方都永不放弃**，而 marker 还占着
  限流名额（实测 4 个僵尸把 `recovery-stats` 的名额占满，`every candidate rate-limited`
  挡住了一长串真正该重建的 extent）。
- **代码作者预见到了这个楔子，但 EC 落在唯一没有防护的分支**：非 EC 的 `Incomplete` 分支
  明写 "Refusing here poisons this (node, extent) pair forever … Drop the stub and rebuild"；
  而 `try_adopt_completed_recovery` 的注释自陈无法判断 EC —— "EC-shard adopt needs a
  `shard_size` comparison"。
- **⚠️ "失败时把条目删掉"是错的解法（已试过并撤回，见评审）**: (a) `handle_write_shard`
  与重建**共用同一个 entry**，EC 转换可能在重建期间把本节点指派为 parity（manager 在
  `recovery.rs:805-840` 把这个状态记为真实生产情形，而 `redispatch_pinned_recovery`
  不重查 occupancy），删条目会让刚写好的 parity 分片从 `holds_payload`/df 账目里消失；
  (b) `ec_stage_nonce` 是拒绝过期协调者 `write_shard` 的守卫，一并删掉会重新打开那扇门；
  (c) `ensure_extent` 建的 0 字节 `.dat` 若不一并清掉，`scan_extents` 无长度过滤，
  **重启后条目重新注册、楔子复活**；(d) 而用 `remove_extent_files` 清又会删掉该节点持有的
  **其它** shard，正是 (a) 的危害。
- **Scope（正确的方向）**: 补上 `try_adopt_completed_recovery` 的 EC 分支——权威长度是
  `erasure::shard_size(sealed_length, K)`，与 `eversion` 一起就能判：
  `len == shard_size` → Complete（上报完成）；`len < shard_size` → Incomplete
  （按 F-EC-RECOVERY-RESUME 续传，或至少安全地重来）；eversion 不符 → 丢弃。
  这同时解掉楔子和 resume，且不必碰共享的 entry。
- **Acceptance**: 人为让一次 EC 重建失败 → 下一次派发**不返回 "already exists"**；
  重复失败十次后 marker 仍能被正常执行；全程 `holds_payload`/df 对该节点其它 shard 的
  记账不变；EN 重启后不复活楔子。
- **Status**: `passes: true` (2026-09-05) — 已实现**并在生产验证**。
  线上实测（EN 滚到 15995c1 之后）:`already exists` 按 extent 逐个消失——en-6 起来后
  63/66 停，en-5 起来后 48/69 停，之后 60 秒内**零拒绝**（此前每 2 秒四条、持续三小时）。
  EN 侧同时打出新分支的日志 `require_recovery: local EC shard is missing, short or
  stale — rebuilding over the existing entry`。五个 recovery op 全部
  `succeeded`（63/66→node 83，48/69/67→node 85），`ops list --active` 清空，
  `extent-health` 无不健康 extent。顺带验证了分片路由修复:日志里读的是
  `192.168.2.65:9131`（分片端口）而非基础端口 9101。
  也顺带确认了 BUG-FRAME-LEN-U32-WRAP 的数字:extent 69 的分片是 4,294,996,716 字节
  = `u32::MAX + 29,421`，与当初推断完全一致。
- **实现要点**。
  `ExtentNode::classify_ec_shard(info, entry, replace_id)` 是个无 `&self` 的纯函数：
  用 `ec_shard_read_len(sealed_length, replicates.len())`（即 `erasure::shard_size`，
  编码器实际写入的长度）当权威值，**长度精确相等且 eversion 相符** → Complete（上报完成）；
  缺失／偏短／偏长／eversion 不符 → 新的 `IncompleteEcShard`；
  非成员／`want == 0`（manager 记录自相矛盾）→ 仍然 Unknown（重建必失败，谎称 incomplete
  只会派发一次注定的失败）。
- **为什么 `IncompleteEcShard` 不能复用既有的 `Incomplete`**: 后者的处理动作是
  `extents.remove` + `remove_extent_files`，两半对 EC 都不安全——见上面的 (a) 与 (d)。
  新分支**什么都不重置**，直接派发：`ensure_extent` 幂等，重建自己用 `truncate(true)`
  开目标文件，而 `ensure_extent` 留下的 0 字节 `.dat` 由 reconcile sweep 在分片到手后回收。
- **线上那四个（63/66/69/48）为什么会被解开（已核对代码，非推断）**:
  `ensure_extent` 给新建条目的 eversion 是**硬编码的 1**；已封存并 EC 转换的 extent
  其 `info.eversion` >1 ⇒ 走 eversion 分支 ⇒ `IncompleteEcShard`。即便 eversion 恰为 1，
  `shard_file_len` 也返回 `None`，同样结论。且 `replace_id` 是**被替换的失败节点**，
  在 `apply_recovery_done` 之前一直留在 slot 里 ⇒ `ec_shard_index` 必然查得到 ⇒
  不会落进 Unknown。（已 apply 后 marker 又重发确实返回 Unknown，但那不是楔子：
  manager 的 `layout_changed.is_none()` 分支会释放 marker 并停止重发。）
- **消融**: 把分类的兜底臂改回 `Unknown`（即修复前行为），7 个新测试**红 4**，
  含 `a_missing_shard_is_rebuildable_not_a_permanent_refusal`。autumn-stream 156 全绿。
- **⚠️ 不含 resume**: 判为 incomplete 后是**整个分片重来**，不是从已完成字节续传。
  见 F-EC-RECOVERY-RESUME —— 该条已按用户决定 `closed / wont-do`（续传非常见情况，
  且 extent 大小可控），所以"重来"就是最终行为，不是欠账。

### BUG-SHARD-RECORD-GHOST — 失败的重建 unlink 了分片，却留着账上的记录
- **Trigger** (2026-09-04，facd61e 评审发现；由 8f96626 引入): EC 重建失败的 `Err` 臂
  删掉了写了一半的分片文件，但没有配对调用 `forget_shard_file`。而该方法的定义处白纸黑字
  写着契约——"Call AFTER the unlink, so the entry never advertises a file that is gone"，
  reconcile sweep 也一直是这么配对的，唯独这条路径只做了前一半。
- **后果**: 若 `entry.shard_files` 里本就有这个下标（重启发现补登的，或并发的
  `write_shard_stripe_local` 写的——它与重建**共用同一个 entry**），那么记录比字节活得久：
  `holds_payload` 继续为真、`df` 继续计一个已经不存在的文件的字节；而路由到本节点的读
  **通过了 ownership 门**，然后在 `payload_file` 里以 `Internal` 失败——而不是干净地
  拒绝为 `PayloadNotHere` 让调用方刷新布局。下一次 reconcile sweep 会自愈，所以是有界的。
- **修复**: 把这对操作提成 `ExtentEntry::discard_shard_file(path, shard_index) -> bool`，
  紧挨 `forget_shard_file` 放。两个调用点（失败重建的 `Err` 臂 + reconcile sweep 的
  stale-shard 循环）共用它，语义就再也不会只落实一半。`NotFound` 算作"已经没了"（重试，
  或 unlink 与更新记录之间崩溃，都会走到这里；若把它当失败，记录将永远滞留，因为此后
  任何 unlink 都不可能成功）。**unlink 真失败则保留记录并返回 false**——这是镜像另一半：
  字节还在盘上却不再记账，`df` 会少算，且 `InShardFile` 情形下会挡住后续的 `.dat` 回收。
- **⚠️ 不涉及**: 条目本身（entry）**仍然不删**，理由见 BUG-EC-RECOVERY-WEDGE 的 (a)-(d)。
  本条只修"文件没了但记录还在"，不碰楔子。
- **Acceptance / 消融**: 3 个 `#[compio::test]`。把 `discard_shard_file` 里的
  `forget_shard_file` 一行摘掉后，`discard_stops_advertising_the_file` 与
  `discard_treats_not_found_as_gone` **变红**（报的正是那条诊断信息），
  `failed_unlink_keeps_the_record` 保持绿（它守的是另一半不变量）——已实测。
- **Status**: `passes: true` (2026-09-04) — autumn-stream 149 lib + 全部集成测试通过；
  `autumn-rpc` 61 通过，含 `registry_pins_current_schema_to_max_version`，WIRE 指纹未变。

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
- **后果**: 与 BUG-SHARD-RECORD-GHOST 相反的一半——字节在盘上却不记账，`df` 少算，
  `holds_payload` 为假。要到重启后 `discover_shard_files` 补登才对上。
  比 ghost 那半轻（不会把读降级成 Internal），但同样是条目与磁盘不一致。
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
- **⚠️ 不需要动 wire，这是本条与 `F-RECOVERY-PROGRESS` 的关键分野**: `SplitPartReq` 里没有
  `op_id`，加一个就是 rkyv 结构变更 → 版本 bump → `MIN=MAX` 全停。**不用加**——样本装在
  `PartitionLoad` 里上来，manager 本来就知道是哪个分区，PS 报 `op_id: 0` 并由 manager 按
  (kind, part_id) 匹配即可。这正是 `update_progress_by_extent` 已有的先例，它存在的原因
  就是"extent node 永远不知道 manager 的 op id"。`F-RECOVERY-PROGRESS` 走 EN 的 `DfResp`，
  那条才必须动 wire；两条不要混为一谈。
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

### F-EN-WORKLOAD-IDENTITY — EN 的 k8s 身份与集群身份对不上，退役只能靠 `scale`
- **Trigger** (2026-09-09，一次 7→5 缩容前的检查): 集群里 EN 是一个 `replicas=11` 的
  StatefulSet，序号 0-10，其中活着的是 0,5,6,7,8,9,10，另外 1,2,3,4 是没有节点可调度的
  Pending 空壳（早期拓扑的残留）。要摘掉的是 en-7 和 en-8（分片最少：30 和 21）。
  而 StatefulSet 只有一个删除动词 `scale`，**只能从序号尾部砍** —— 缩到 5 会砍掉
  5,6,7,8,9,10 这六台**有数据的**，留下 0 和四个**空的**。序号顺序和退役顺序毫无关系，
  所以这个动词根本表达不了"摘掉这一台"。
- **根因**: EN 的身份是 **PVC 上持久化的 `node_uuid`**（地址每次启动自注册，序号只是
  volumeClaimTemplate 的下标）。`deploy/k8s/extent-node.yaml` 的注释自己就写着
  "StatefulSet 留着只是为了 STORAGE identity"——但它同时把序号伪装成了身份，
  而 k8s 提供的唯一缩容动词恰好绑在序号上。
- **Scope**: 照 Rook 管 OSD 的路子，**每个 EN 一个 Deployment**，按名字挂已有的
  `data-autumn-en-<n>` PVC（身份不变，因为身份在 PVC 上）。`strategy: Recreate` 是必须的
  而非口味：PVC 是 RWO 且绑在某个节点的本地盘上，RollingUpdate 的 surge pod 会永远等一块
  前任还攥着的卷。配两个脚本：`deploy/scripts/en-workload.sh`（渲染/应用，基座 manifest
  由它生成以免漂移）与 `deploy/scripts/en-decommission.sh`（fence → 等排干 → remove →
  删 workload，每一步都门控）。**不写 controller**——这是"便宜的那档"。
- **Acceptance**:
  - `en-decommission.sh --dry-run <n>` 能通过 pod IP 正确解析出 node_id 与分片数；
    剩余节点数低于副本数时**拒绝执行**。
  - 在真集群上用它摘掉两台 EN，落到 5 台：期间无数据丢失，`autumn-op info` 显示
    分片重新铺开、`extent-health` 干净、所有分区照常服务。
  - 摘完后 `deploy/k8s` 与 vke overlay 都能 `kustomize build`，且 EN 不再有任何
    以 `scale` 为运维入口的路径。
- **Status**: `closed / passes: true` (2026-09-09) — **已在生产完成**，与 wire v38 的
  全停合并做（每台 EN 本来也要重启）。实测：
  - `delete sts --cascade=orphan` 后 7 个 pod 照常运行；全停期间删掉，再按序号建
    Deployment，**7 个 node_id 一个不差、分片数与基线逐一相同**（41/42/38/33/30/36/21），
    而 pod IP 全变了（如 node 9 从 .145 变成 .204）——身份确实在 PVC 上，不在序号或地址上。
  - 退役脚本摘掉 en-8（node 108，21 分片）与 en-7（node 104，30 分片）：fence 后
    **90 秒**排干到 0，`remove` 服务端放行，workload 才删。落到 5 台，分片自动铺平
    （57/54/53/52/53），全程无数据丢失、无卡住的 op。
  - 副作用一条（已修）：全停后 PS 缓存着**迁移前的 EN 地址**，打不开分区
    （`connect 192.168.3.169:9131 timed out`）。等 EN 全部 Online 后重启 PS 即可，
    已写进 docs/ops.md 的迁移步骤。
  评审挖出三条会在真集群上出事的，均已修：
  1. **迁移会让两个 EN 进程开同一块盘**：EN 对 data-dir **不加锁**（`flock|fs2|fd-lock`
     全仓库为空），而 RWO 是**节点级**语义 —— 同一节点上两个 pod 可以同时挂同一个 claim，
     PV 又是 nodeAffinity 钉在那台。`apply` 不 prune，旧 STS 不会自己消失。已在
     docs/ops.md 写死顺序：`delete sts --cascade=orphan` → **逐台**先删 pod 等它消失、
     再 apply 该序号的 Deployment；并写明为什么不能一次性 apply 整组。
  2. **退役脚本解析不到 Deployment 的 pod**（名字是 `autumn-en-7-<rs>-<pod>`）：改为按
     `autumn.dev/en-ordinal` 标签找；匹配到多于一个直接拒绝（那正是两进程共盘的信号）；
     仍是 StatefulSet 时给出明确拒绝而不是误删。另修：健康节点计数原来用 `/Online/`
     松匹配，会把**已 fence 正在排空**的节点算成剩余容量（auto 列仍是 Online），
     改为 `$7=="Online" && $8=="-"`。
  3. **vke overlay 的 patch 目标还是 StatefulSet**：kustomize 对匹配不到的 target
     **静默跳过**，所以 build 照样成功，却产出没有 nodeSelector（会调度到 kernel-5.4
     节点）、cpu 仍是 1 的 EN。根因是"EN 数量"根本不该由 manifest 声明——已把 EN 整个
     移出 kustomize，由 `deploy.sh` 调渲染器创建，且**在有 EN PVC 但没有 EN Deployment
     时拒绝猜序号**（否则会在本集群上复活退役的 1-4）。
  附带修掉 `deploy/validate.sh`：它的 EN 检查被 `if en_ss:` 包着，EN 不再是 StatefulSet
  后整段空转却照打 `VALIDATION OK`；改为验渲染器输出，并做了消融（把 `Recreate` 改成
  `RollingUpdate` → `VALIDATION FAILED`）。

### F-MEM-EXTERNAL-EMBED — 向量腿只有 hash 词袋可用，等于没有
- **Trigger** (2026-09-09，用户): "支持的 hashemb 一点用都没有"。属实，而且代码自己早就写明了：
  `is_semantic()` 对 `HashEmbedder` 返回 false，注释说"两段同主题的文本不会比两段无关的更近，
  向量与 hybrid 检索返回的是噪声"。`examples/memory-mcp/src/main.rs:119` 也写着
  "With the default HashEmbedder the vector leg is noise, and RRF fusion …"。
  也就是说向量腿一直是通电但没接信号的状态；唯一的真语义路径是 `static-embed`
  的离线 int8 查表，要自带模型文件和 tokenizer。
- **Scope**: 加一个 OpenAI 兼容的外部 embedding 客户端 `OpenAiEmbedder`，feature
  `openai-embed`（沿用 `static-embed` 的可选依赖惯例，默认构建仍零额外依赖）。
  HTTP 用 `cyper`——compio 原生，跑在调用方的运行时上，不会在旁边再拖一个 runtime 进来；
  TLS 用 rustls 而非默认的 native-tls，否则发布镜像没有 OpenSSL 头文件会直接编译失败。
  `Embedder::embed` 改为 **async**（三个调用点本来就在 async fn 里），并加 `embed_batch`
  ——端点本身是批量形状，而索引是个循环。**不做**混用保护：向量库本来就不管向量是谁产的，
  那是调用方的契约（用户拍板）。
- **Acceptance**:
  - 解析层：响应按 `index` 归位而非按数组顺序；重复 index、数量不符、非数字、
    错误体各自被拒绝并说清楚；每个向量 L2 归一化。
  - 线路层：对真实 socket 发出的确实是 `POST /v1/embeddings`，body 带 model 与 input，
    `dim()` 报的是服务端真实返回的宽度；**服务端接受连接后不应答时会超时而不是挂死**。
  - 默认构建与 `--features openai-embed` 两种都要能编过，且默认构建不引入新依赖。
- **Status**: `passes: true` (2026-09-09) — 已实现并通过。8 条单测（6 条解析 + 2 条走真实
  socket 的端到端），`cargo test -p autumn-memory --features openai-embed` 41 passed，
  默认特性 33 passed，clippy 在新代码上零告警。两条消融各自变红并在还原后复绿：
  (A) 改成信任数组顺序 → 按 index 归位那条红；(B) 去掉重复 index 检查 → 该条红。
  （另试过去掉数量校验，**没有变红**——短响应还有第二道守卫按槽位回填时接住，
  那条校验只是让报错更准确。如实记下，不算作一条有效消融。）
  `examples/memory-mcp` 加了 `--embed-url` / `--embed-api-key-file`（读文件而非命令行，
  argv 里的密钥全机器可见），并把外部 embedder 排在 hash 之前——运维指定了 URL 却因为
  拼错回落到 hash，会得到一个每次向量检索都返回噪声、而只在启动日志里说过一句的服务。

  **评审挖出的四条,均已修**：
  1. **超时只包了 `send()`**，`resp.text()` 在外面——"发完 header 再停住"是与"接受连接后
     不说话"不同的一种挂死，原来的写法两种都防不住第二种。已把整个交换(send + 读 body)
     一起包进 timeout，并补了"发 header 不发 body"的测试。消融变红的形态本身就是证据：
     超时形同虚设时客户端**整整等了 30 秒**直到服务端断开，最后报的是 hyper 的 body
     读取错误而不是超时。
  2. **`embed_batch` 没有任何调用方**，而索引是每个符号一次往返——正是注释里警告的用法。
     索引循环改成**按文件批量**(天然的分块边界，内存有界)。
  3. **api key 文件读失败只 warn 然后无 key 继续**，与"URL 拼错不能静默回落"自相矛盾。
     改为 fail-fast。同时加了启动探针：起服务前先要一个向量，URL/模型/密钥错在**当场**
     退出，而不是等读者第一次搜索；顺带让 `/config` 的 `dim` 有真值可报(它是启动快照，
     否则 `--no-index` 时会永远显示 0)。
  4. `docs/ops.md` 与 `examples/memory-mcp/README.md` 未同步——已补，并写明"换 embedder
     必须重新索引"。
  评审确认的、不改的两条：解析用 `serde_json::Value` 中转在大批量时有分配开销(可日后改
  typed struct，非阻塞)；`rustls` 特性组合独立可用，Cargo.lock 里确实没有 openssl-sys。


### F-MEM-DROP-HASH-EMBED — 删掉那个假 embedder，连同为绕开它而存在的防御机制
- **Trigger** (2026-09-09，用户): 「一个纯粹为了绕开假数据而存在的防御机制，应该和假数据
  一起走，都删了，要不就是 BM25，要不就是纯语义，要不就是 hybrid」。
- **根因**: `HashEmbedder` 本身不坏——签名 FNV 词袋，确定可复现，是条真管道。**坏在它是
  默认**：向量与 hybrid 检索不会失败，而是自信地把噪声排进前列。实测(docs/ops.md 的
  eval 表)vector 的 hit@1 = 0.146，hybrid 被它从 lexical 的 0.976 拖到 0.610。为绕开它，
  代码里长出了 `is_semantic()` 和 `auto_mode()`——一个专门用来问"我自己的 embedder 是不是
  在撒谎"的机制。
- **Scope**: 删 `HashEmbedder`、`Embedder::Hash`、只它用的 `fnv1a`/`tokenize`、`is_semantic()`。
  `embed` 模块整体按 feature 门控——没启用 `static-embed` 或 `openai-embed` 就没有这个模块，
  因为一个总是存在的模块必须提供点什么。memory-mcp 里 `Embedder` 变成 `Option`：没配就只有
  BM25，`/config` 如实报 `"modes": ["lexical"]`(此前是硬编码三元组，等于骗人)。
- **Acceptance**: 四种 feature 组合零告警零错误；没配 embedder 时 `mode=vector|hybrid`
  明确报错而非返回空结果；`--eval` 默认不中止。
- **Status**: `passes: true` (2026-09-09) — 已实现并通过。四种组合(无/static/openai/两者)
  各 0 warning 0 error，`--features openai-embed` 42 passed、默认 33 passed、workspace 零错误。
  **评审挖出三条，均已修**：
  1. **[高] 没配 embedder 时 `--eval` 整跑中止**——默认 modes 是硬编码三元组，第一条 vector
     查询直接 Err 退出：无报告、无基线对比、非零退出，而 README 与 ops.md 的两条 runbook
     命令照抄就挂。改为默认按 `emb.is_some()` 取；显式 `--eval-modes vector` 仍然报错(那是
     调用方点名要的)。
  2. **[中] 新增 `unreachable pattern` 告警**(embed.rs 的 `embed_batch` 兜底臂)——`Hash`
     变体没了之后 `_` 不再可达，而这正是 memory-mcp 的默认特性集。改成具名的 cfg 分支。
     我此前说"clippy 无新增告警"是错的：只查了 memory-mcp 这个包，没查它默认特性下的**库**。
  3. **[中] `eval/baseline.json` 仍写着 `"embedder": "hash"`**，且 vector/hybrid 两段是那个
     已删 embedder 产生的、永远无法复现的数字。改成 `"embedder": "none"` 并只保留 lexical 段
     ——BM25 与 embedder 无关，那些数字仍然描述这个语料。`compare` 对基线里没有的模式会打印
     "not in baseline"并跳过，不会误红。
  另修：MCP 侧 `mode=vector` 无 embedder 时改为 `isError:true` 的工具结果而非 JSON-RPC
  -32603(后者客户端会当成"工具挂了"，agent 学不到可以改用 lexical)；索引批量加长度校验
  (短批次会静默让文件尾部的符号没有向量——词法搜得到、向量搜不到，且无人报告)；
  漏改的三处文档(根 README、fetch_model.py、plan.md 把 hit@1 误写成 nDCG@10)已补。

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
- **Status**: `passes: false` (2026-09-09；2026-09-10 修了第一半) — 逐个修复中。
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
  204 是 201/202/203)，共享的是 **extent**。split 被拒是另一回事，见
  `F-SPLIT-NEEDS-COMPACT`。

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
