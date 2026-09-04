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
- **三条杠杆，按该做的顺序**：
  **(a) AZ 收拢 —— 先做这个。** append 1.39 ms → ~0.11 ms 会把 append/s 从 950 抬约一个
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
