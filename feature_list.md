# autumn-rs feature list — OPEN backlog

**Last updated:** 2026-09-03

**Rules:**
- This file tracks the **OPEN backlog only**. A feature that reaches `passes: true`
  is **DELETED** from here — git history is the record, there is no archive file
  (CLAUDE.md rule 13: 定期清理删除，保持整洁).
- `passes` and `notes` are the only mutable fields after a feature is created.
- Out-of-scope / "v2 再做" decisions must be recorded as proper feature entries
  (F-name + Trigger + Scope + Acceptance + `passes: false`), never as plan-file footnotes.

---

## Active

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
- **Status**: `passes: false` (2026-09-02；2026-09-04 补机制假说；2026-09-04 机制已实测) —— 仍只立账，不阻塞任何东西：
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
- **影响范围（未查证的那半）**: FreeToken 侧传的是普通字符串（`"dsv4_full"`/`"dsv4_window"`）
  且只读 `kv_hit_pages`，**不受影响**。sglang 侧是否会把 enum 成员送进 `transfer.name`
  **未查证** —— 评审只能确认 controller 是从上游调用者转发 `transfer.name` 的。若会，
  后果是 v2 的 KV key 落到 `"PoolName.KV"` 这个段下（与 v1 不再字节一致，跨版本读不到），
  且 `batch_exists_v2` 的结果字典键名对不上。
- **Scope**: 三处统一改成取 `.value`（或 `PoolName.KV.value`），并加一条断言/单测把
  "v2 的 KV 段必须与 v1 字节一致"钉住 —— 那是 v2 设计时明确写下的性质
  （"v2 keys for the KV pool are byte-identical to v1's — this is additive, not a migration"）。
- **Acceptance**: 先查证 sglang 是否真的传 enum 成员（传字符串则本条降级为整洁性修补）；
  修后在 py≥3.11 上 v1/v2 的 KV key 逐字节相同。
- **Status**: `passes: false` (2026-09-04) — 既有缺陷，非本轮引入；当前唯一的消费者
  （FreeToken）不触发，故不阻塞 L3 上线。

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
- **Status**: `passes: false` (2026-09-04) — 不阻塞任何功能，但它是本次排查里最贵的一个
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
- **Status**: `passes: false` (2026-09-05) — 未实现。当前可用的替代手段是
  **把要腾空的节点全部一次性 fence**，靠 `hard_excluded` 把它们从候选里剔除，
  从而避免数据回流；这次迁移就是这么做的，有效但粒度粗，且解决不了稳态倾斜。
