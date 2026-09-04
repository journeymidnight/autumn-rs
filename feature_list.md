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
- **Status**: `passes: false` (2026-09-02；2026-09-04 补机制假说) —— 仍只立账，不阻塞任何东西：
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

### F-FT-DSV4-KV-SCOPE — DeepSeek-V4 在 FreeToken 上要不要接 autumn KV
- **Trigger** (2026-09-01, 用户问"FreeToken 现在的 hicache 支持 DSV4 了吗"后核实上游):
  **FreeToken 至今没有任何 HiCache**。上游 `a2538a4`(2026-09-01，仅落后本地 2 个无关 commit)
  全树搜 `HiCache` 只命中 `kvcache/base.py:197` 那行 `# TODO: support HiCache`；
  `hicache`/`HiCacheStorage`/`host_pool`/`L3`/`storage_backend` 全部 0 命中，
  `kvcache/` 目录里没有任何分层缓存文件。
- **所以 DSV4 的 KV 现状**: 能工作，但**纯显存**（`dsv4_paged_pool.py` 614 行 +
  `swa_radix_cache.py`，四组 buffer: window/cmp/idx/state_ring）。autumn 接不进去不是
  因为不兼容，而是**没有可接的地方**。
- **完整代价（若要接）**: 把 sglang 的 unified 分层缓存移植进一个**完全没有分层缓存**的引擎，
  并为 DSV4 那四组 buffer 写 sidecar 映射 + autumn 侧 v2（已完成，efeea3c）。
  这是 FreeToken 侧的活，远大于最初估的"移植 HiRadixCache 1427 行"。
- **便宜的替代**: DSV4 就用 FreeToken 自带的显存 KV，不接 autumn；autumn 的 v2 留给
  **真正的 sglang** 用（它已经在调 v2）。单副本部署下损失有限 —— L3 的主要价值是
  **跨副本**前缀复用，而当前规划就是 1 副本。
- **Acceptance**: 明确记录选哪条；若选替代方案，FreeToken 的 `--cache-type` 保持默认
  （DSV4 会被强制成 swa_radix），不做 HiCache 移植，autumn v2 的验证改用真 sglang。
- **Status**: `passes: false` (2026-09-01) — 待用户定夺。**注意**: 本条的前身结论
  （"sglang 上游没有 SWA+HiCache，那一档是原创设计"）是错的——那个判断出自一个落后上游
  6232 个 commit 的 sglang 克隆，上游实际有 `UnifiedRadixCache`(3120 行) + `unified_cache/`
  子包，HiRadixCache 已是死代码。真实约束是 DSV4 需要 sidecar 池 ⇒ v2 接口，而非上游未解。
  移植工作本体已随 da91d38 移到 ../buda，本条只留"autumn 这边要不要为它做事"的取舍。
