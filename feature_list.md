# autumn-rs feature list — OPEN backlog

**Last updated:** 2026-10-29 (data-path redesign: F-DP-* entries)

**Rules:**
- This file tracks the **OPEN backlog only**. A feature that reaches `passes: true`
  is **DELETED** from here — git history is the record, there is no archive file
  (CLAUDE.md rule 13: 定期清理删除，保持整洁).
- `passes` and `notes` are the only mutable fields after a feature is created.
- Out-of-scope / "v2 再做" decisions must be recorded as proper feature entries
  (F-name + Trigger + Scope + Acceptance + `passes: false`), never as plan-file footnotes.

---

## Active

> 数据通路重设计（2026-10-29 用户定稿）：设计页 https://claude.ai/artifact/E18oSqzHKbg2Vx1HqKKyo1 。
> 三条规则：一种操作一种消息；数据永远在 value 区（ctrl 只放元数据）；前台数据在注册池里，UCX 不允许隐式注册。
> 小于 64 KiB 允许拷贝一次，大于等于 64 KiB 全程零拷贝。以下 F-DP-* 按顺序实施。

### F-DP-CALLER-REGISTER — 调用方内存显式注册，禁止隐式注册（第 3 步）
- **Trigger**: UCX 用调用方内存（Python buffer、kvcache 页）今天靠 rcache 隐式注册；`RecvDest` 只有指针和长度。
- **Scope**: Python `register(buf) -> handle` / `unregister`（TCP 空操作，只读内存注册时报错）；地址落在已注册区域的 put/get 自动带 memh，否则 UCX 下拷进 slab；UCX 收发接口只收带 memh 的切片（类型保证）；TCP 直接读写调用方内存；调用返回后传输层不再引用调用方内存（一并关闭 BUG-PY-PUT-BORROWED-MEMORY）。
- **Acceptance**: 传裸指针或普通 `Bytes` 给 UCX 收发编译不过；只读 buffer 注册报错、不卡连接；超时返回后重用 buffer 不会被晚到的发送读到（去掉修复变红）；perf-check 不降。
- `passes: false`
- **notes** (2026-10-30，3a 链 1): 用户定：不用按地址二分查表找回 memh，而是让值一路带着 memh 走（`RegBytes`，即设计页"带注册信息的 Bytes"）。链 1（PS 收值 → WAL → 发给 EN）已改：`Frame.value`、`PartitionRequest.bulk_value`、`WriteOp::Put.value`、`Conditional.value`、组提交段、stream client 追加、`PreparedPayload` 都是 `RegBytes`；`RegBytes` 加了 Deref/slice/Default。加了进程级 AM 发送字节计数（带/不带 memh，日志与 `/metrics`）。UCX 8M 写实测：PS 以 AM 发出约 180 GB，全部带 memh，不带的为 0。测试：`am_value_budget` 断言收进池的帧值带 memh、`prepared_append` 断言池缓冲的追加带 memh 发出（各自去掉即红）。读走 EN 直读，PS 的 GET 回复（链 2）只在代理读时出现。评审：空切片（静态空 Bytes，不持有缓冲）不再带 memh（测试去掉即红）；`Memh` 改 `NonNull`，`RegBytes` 48→40 字节。perf-check（ABAB 换序）：TCP/UCX 8M 与 UCX 4K 持平；TCP 4K 写 patch 九次 25.3/41.0/44.2/25.5/42.3/42.3/28.7/42.7/46.6K（中位 42.3K），基线八次 38.0–44.3K（中位 39.9K）；patch 三次落在低态（p50 不变、p95 26–28 ms 对 18–21 ms，即延后排查的 4K 写长尾），基线这次没有，但 2b-1 时基线也测到过 28.9K；TCP 4K 路径上只改了类型，没有新增等待。
- **notes** (2026-10-31，3a 链 2): PS 回 GET 带 memh：`read_value_from_log` 返回 `pb.freeze()`，`resolve_value`、`GetOutcome::Value`、`handle_get_bulk` / `handle_batch_get_bulk` / `serve_get_local`、`AmOut::reframe` 都传 `RegBytes`；`handle_get_am` 的 `am.send` 同样带上；TCP 连接发普通 `Bytes`。UCX 8M 实测：读阶段 PS 发出约 325 GB 回复，全部带 memh、不带的为 0（之前以为读走 EN 直读是看错了：上次抓日志在读阶段之前就停了）；读 14.9 GB/s。PS 侧这一跳没有单独的自动化测试（需要 UCX 下的进程内 PS），靠 `ucx_am_sent_bytes_total{memh}` 计数可见；3b 强制带 memh 后回退会被计数和类型挡住。perf-check（ABAB 换序）：读四格持平或略好（TCP 4K 读 276/279K 对 271/262K）；写路径两边代码相同，TCP 4K 写仍出现 29.3K 对 40.0K 这样的差，印证链 1 那几次低态是环境。
- **notes** (2026-10-31，3a 链 3): SDK 写带 memh：`IntoValue::into_value` 返回 `RegBytes`（新增 `ValueBuf`、`RegBytes` 实现），`ValueBuf::freeze` 返回 `RegBytes`（`autumn_client::RegBytes` 重导出；HTTP 等用 `.into_bytes()`），`put_bound` / `put_bulk_opts` / `put_bulk_attempts` 与 rpc `call_vectored_bulk(_multi)` / `value_frame` 接受 `Into<RegBytes>`。autumnfs 的 `alloc_value_buf` → `put` 就此带 memh。`put_many` 的公开 `Bytes` 项不变（调用方自己的内存，3b 在 UCX 边界处理）。测试：`prepared_append::a_bulk_call_with_a_pool_buffer_carries_its_memh`（UCX，池缓冲的 bulk 调用带 memh 计数、普通 Bytes 不带；`value_frame` 去掉 memh 即红）。perf-check 的客户端写用的是堆 `Bytes`，不经这条链。
- **notes** (2026-10-31，3b): UCX 发送只收带 memh 的值：`send_am` 收 `Staged`，只能由 `Staged::of` 构造——带 memh（或空）的直接过，否则拷进池缓冲（`acquire_charged_if_room(Staging)`，发送不会因缓冲失败）再带那块的 memh 发；拷贝计数 `am::staged_bytes()`（日志、`/metrics`）。超过 256 MiB 的值落堆、仍不带 memh，等第 5 步分段。GC、SST flush、compaction 的追加就靠这一条拷贝（设计页"后台大块 UCX 拷进池中转"），链 4 不再单独做。UCX 8M 实测：客户端（perf-check 用堆 Bytes）拷了约 59 GB，PS、EN 拷 0、发出的全部带 memh。测试：`prepared_append` 两个用例改为普通 Bytes 也带 memh 发出且计入拷贝（去掉拷贝即红）；`ucx_am_frame` 三个发送方生命周期用例改看本线程持有的 Staging 字节（原先看堆值何时被丢，拷贝后堆值立即释放，发送方持有的是拷贝）。新增 `regpool_thread_held`。评审：三处"发送方释放"检查原来是空的（发送是 spawn 出去的，拷贝前 Staging 就是 0），改为同时要求本线程发送计数已动（让发送方永不释放即三个都红）；超过 256 MiB 的值不再白拷一份到堆。perf-check（ABAB 换序）：UCX 8M 写 2566/2650 对 2686/2638、TCP 各格持平；UCX 4K 读 281K/277K 对 289K/288K（约 −3.5%），原因：rc_mlx5 上 515 B 起的 AM 就走零拷贝（即注册），PS 小值 GET_AM 回复（内联值）每条都要拷进池——这是"不允许隐式注册"的代价，记录在案。
- **notes** (2026-10-31，3c): 用户定按句柄传（不按地址查表）。传输层 `Region::register` / `lend` / `Returned`（`lend.rs`）：注册一次调用方内存；出借的值在区域内带它的 memh 零拷贝，区域外在 UCX 发送处拷进池；`Returned` 在最后一个克隆放下后才完成。SDK `put_many` 接受 `Clone + Into<RegBytes>`，`batch_put` 内部全用 `RegBytes`。Python：`autumn.register(buf) -> Region`（只读缓冲拒绝，`unregister()`），`BatchClient.put_from(keys, bufs, ttl_secs=0, region=None)` 与 `Client.batch_put_from` 的 TTL 路径改为出借并在全部归还后才返回（删掉 `Bytes::from_static`），`autumn.ucx_am_counters()` 给发送/拷贝计数。实测（UCX 集群、Python）：16 页区域内写入 staged 不变、带 memh 32 MiB；4 页普通写入 staged +8 MiB；写完立即覆盖缓冲，读回仍是原值；只读 numpy 标志的缓冲 register 被拒。测试：`a_value_in_read_only_memory_arrives`（`mprotect` 只读映射：拷贝后能到；不拷贝则 rendezvous 永不完成——Python 侧 `np.memmap(mode='r')` 同样复现：无拷贝的 wheel 上 put 卡死 90 s 被杀）、`a_lent_value_is_sent_from_its_region_and_returned_after`、`lend::tests`。未覆盖：`put_from` 在归还前不返回这一组合没有"去掉即红"的测试（要造出调用超时后发送仍在途，现有测试只证明 `Returned` 本身）。收方向（GET_AM 收进调用方内存）是 3d。
- **notes** (2026-10-31，3d): 读也不再隐式注册：`am::expect(tag, dest, region)`——落在调用方注册的区域里就带区域的 memh 直接收；否则收进同样大小的池缓冲（`Reply`，有空才计额度），到齐后拷进调用方内存（`landed_bytes`，Python `ucx_am_counters()["landed"]`）；超过 256 MiB 没有这么大的池缓冲，`Unsupported`，SDK 改走 stream 读。SDK `get_range_into_raw` / `get_many_into_raw` 加 `region` 参数，Python `BatchClient.get_into(keys, bufs, region=None)`。实测（UCX、PS 内存层、64×16 MiB、单 worker、四轮）：区域内 7.1–9.1 GB/s、拷贝 0；普通内存 5.0–5.6 GB/s（一次拷贝）；旧的隐式注册在普通内存上 7.6–9.1（与区域内相同，3d 不是回退）。kvcache 注册 L2（第 4 步）后走第一种速度。测试 `ucx_am.rs::a_value_reaches_caller_memory_directly_only_through_its_registration`（忽略区域、或把调用方内存不带 memh 交给 UCX，均变红）。验收"不带注册的内存到不了 UCX"仍差超过 256 MiB 的值（发送直接放行、帧值收进堆），等第 5 步分段，故 `passes` 仍为 false。

### F-KVC-REGISTERED-L2 — autumn_kvcache 注册 L2，vLLM 换可复用缓冲（第 4 步，隔壁仓库）
- **Trigger**: sglang 的 L2 host 池长期存在却每次靠 rcache；vLLM 每次 `.cpu()` / `np.empty` 新内存，UCX 上每次现注册。
- **Scope**: sglang 后端在 `register_mem_pool_host` 注册整块 L2，`copy` 布局 scratch 也注册，关闭时注销；vLLM 连接器改用可复用 staging（注册一次）或 SDK 池缓冲；`AUTUMN_REV` 跟进。
- **Acceptance**: UCX 上 sglang、vLLM e2e 输出正确（命中与不命中生成一致）；L3 读写不再拷进 slab；TCP 不变；L3 读写带宽与改前对比。
- `passes: false`
- **notes** (2026-10-31): autumn_kvcache 1836e03（AUTUMN_REV aec2c23e）。sglang：页直接进出池的布局在 `register_mem_pool_host` 时注册池的整块存储（失败即启动失败），`copy` 布局注册每线程 scratch；vLLM：每线程一块复用的注册缓冲（存时 GPU 张量直接拷进去，字节与原 `.cpu()` 相同，不用升存储格式）；所有 put_from / get_into 带 region。sglang e2e（Qwen3-VL-4B TP1、UCX、PS 内存层、B 实例读 A 写的页）：答案全同，69120/69280 token 来自 L3；layer_first 拷贝 10.2 GB 进 + 10.2 GB 出 → 0，时间不变；page_first_direct 20.4 GB → 0，128 页读 362/269 → 201/239 ms，写不变；首次注册 32 GB 池 +160 ms。顺带修了 PS bug（c4b3240c）：内存层大值在批量重定向里被内联进 rkyv 回复（vLLM 一次 1.2 GB、7 s、全部 miss）。未完成：vLLM e2e——修复后重跑时整机 8 张卡被他租占满（各 138 GB、100%），等 GPU；vLLM 默认直读 EN，读仍经池拷贝（第 6 步统一 READ 落点后才零拷贝）。


### F-DP-ONE-READ — 读统一成一种消息（第 6 步）
- **Scope**: PS 读 EN、client 直读、后台读共用 READ（长度模式：精确 / 按本地截短 / 读到尾；可选落点标签；可选附带块 CRC）；GET / BATCH_GET 回复"value 或位置"；接收合并为"落点"一种机制；删除 GET_BULK、GET_AM、GET_REDIRECT*、BATCH_GET_BULK、READ_BYTES*。
- **Acceptance**: 读路径拷贝次数符合设计页的表；perf-check 读四格不降；直读失败回退经 PS 的测试；wire 升级按全停全启写进 ops.md。
- `passes: false`

### F-DP-ONE-WRITE — 写统一成一种消息（第 7 步）
- **Scope**: PUT / BATCH_PUT / COMPARE_* 去掉 _BULK，数据一律在 value 区；rkyv 结构里的 `value: Vec<u8>` 字段全部删除；APPEND 统一为 payload CRC 在 ctrl；手写固定头（PUT_BULK 44 B、APPEND 32 B、WRITE_SHARD 52 B）改 rkyv，4K 写有可测回退则留作例外。
- **Acceptance**: 写路径拷贝次数符合设计页的表；perf-check 写四格不降；wire 升级写进 ops.md，Python wheel 重建。
- `passes: false`

### F-DP-BACKGROUND — 后台数据传输统一（第 8 步）
- **Scope**: 所有后台读改用 READ（恢复、peer copy、EC 重建、回放、checkpoint 读附带块 CRC，恢复出的副本直接写 `.ck`）；UCX 上写盘类与窗口类直接用 slab（0 拷贝），回放拷进堆；flush / compaction APPEND 在 UCX 上分段中转、EN 按段 pwritev；WRITE_SHARD 改 value 区 + CRC 在 ctrl；EN 之间复用连接、扇出并行；后台读与前台分开连接；删除 `MSG_COPY_EXTENT`。
- **Acceptance**: 各后台任务拷贝次数符合设计页；恢复副本的块 CRC 核对能发现源端腐坏（测试）；前台 4K 写 p99 在 compaction / flush 进行中不变差；perf-check 不降。
- `passes: false`

### F-DP-CRC — CRC 整理（第 9 步）
- **Scope**: payload CRC 在编码循环里顺带算、每批一次（不用 `crc32c_combine`）；EN 写入时记下写满的块（开放形态随 fsync 落盘），scrub 按 sealed_length 定稿、只重读尾块，首次记录只留作兜底；WRITE_SHARD 带 stripe CRC，分片持有者生成分片 `.ck`。
- **Acceptance**: 写入后不经 scrub 首次记录即有 `.ck` 的测试；`truncate_to_commit` / fsync 失败后块 CRC 正确回退的测试；scrub 能用写入时的基准发现腐坏；perf-check 写不降。
- `passes: false`

### BUG-PY-PUT-BORROWED-MEMORY — Python 写路径把调用方内存当 'static 交给传输层
- **Trigger** (2026-10-24 评审): `python/src/lib.rs` 的 `BatchPutFrom`（带 TTL）与 `BatchClient` 的 Put 把调用方 buffer 包成 `Bytes::from_static` 交给 `put_many`。UCX 上 ≥64 KiB 的值走帧值 rendezvous（PS 取走前一直引用这块内存），小值拼进 ≥478 B 的流发送（零拷贝，逐段注册）。
- **Scope**: (1) 只读映射（`np.memmap(mode='r')`、只读 mmap）注册失败 → 该连接的流静默停住（同 `.rodata` 那次）；(2) 写超时/重试后 Python 已返回并重用这页，PS 之后取到的是新内容并存在旧 key 下（KV 缓存静默错数据）。两者 AM 之前就存在（流的零拷贝同样注册、同样延后读取），帧值把窗口拉长到 PS 处理到该帧为止。sglang 的 KV 页是可写堆/锁页内存，不触发 (1)。
- **Acceptance**: 调用返回之后传输层不再引用调用方内存（像 `get_into` 的 `Released` 那样等释放，或在边界拷贝）；只读 buffer 不会让连接停住；各有一个去掉修复就变红的测试。
- `passes: false`
- **notes** (2026-10-31): 随 3c 修。(1) 只读映射：UCX 发送处一律拷进池（3b），`a_value_in_read_only_memory_arrives` 复现并守住（不拷贝即 rendezvous 永不完成；Python `np.memmap(mode='r')` 同样卡死）；`autumn.register` 拒绝只读缓冲。(2) 借用：`BatchClient.put_from`、`Client.batch_put_from` 的 TTL 路径不再 `Bytes::from_static`，改为 `lend` 并等全部 `Returned` 才返回；Python 实测写完立即覆盖缓冲、读回仍是原值。(2) 缺一个"去掉等待即红"的测试（要造出调用超时后发送仍在途），故仍为 false，由用户判断是否关。

### F-KVC-ADAPTER-WRITE-READ — KV-cache adapter 读写跑满（autumn_kvcache 仓库）
- **Trigger** (2026-10-23 实测): autumn 单 rank 写 2.39 GB/s、读 1.79 GB/s（微基准），但 SGLang 里取 128 页（2 GiB）要 2–6 s，超过 timeout 预算，跨实例命中 0%；读实例预取失败后重算、把已在 L3 的页又写一遍（adapter 写前不查 exists），和读抢带宽。
- **Scope**: (1) 写前去重（批量 exists，只写不存在的页）；(2) 查清引擎内取数比微基准慢 2–5 倍的原因（拷贝？GIL？和写争用？）并修。
- **Acceptance**: 同一负载（Qwen3-VL-32B、8K prompt、逐个请求、timeout 策略）跨实例命中接近 100%，每请求耗时低于重算；读实例不再重写已有页；去掉修复变差。
- `passes: false`
- **notes** (2026-10-23): 根因找到一个更重的：adapter 读到 `get_data_page(flat=True)` 上，而 sglang 的 MHA 三种布局、MLA layer_first、Mamba 的页都不是池的视图，flatten/cat 出来的是拷贝——读报命中、池没被写（静默错 KV），每批还白分配 2 GiB。SST 不是原因（大 value 在 memtable/SST 读速相同，已核实命中 SST）。修法在 autumn_kvcache：按池判定 整页视图 / K·V 两段视图（零拷贝）/ 拷贝（scratch + set_from_flat_data_page），非 layer_first 布局的 key 带布局名；写前批量 exists 去重。实测 128×16 MiB：读 0.79–0.98 s（原始微基准 1.2 s）、重复写 3–5 ms。引擎端到端待 GPU 空出来复测。

- **notes** (2026-10-23 晚，引擎端到端): 正确性已证：B 实例经 autumn 从 L3 读回的 KV 与 sglang 自己 L2 装回的逐 token 一致（对照 E）；与 GPU 前缀缓存的差异是 sglang L2 装回路径本身的数值差（不接 autumn 也有）。引擎内取数慢的根因是 GIL 护航：每页多个 torch 调用各自放/抢 GIL，调度线程忙时每次等 ~5 ms（单测加一个忙线程即复现 0.9→3.9 s）。修：池按"页轴"做成一个 numpy 跨步视图，零拷贝页取 numpy 切片，拷贝页整批 torch.index_select 收集 / numpy 花式赋值散布（sglang 设 torch 单线程，按此选型）。结果（Qwen3-VL-32B、逐个请求、timeout）：跨实例命中 0–31% → 93–96%，引擎内取 2 GiB 2.5–4 s → 0.87–1.31 s，读实例不再重写；写入 L3 page_first_direct 2.08 GB/s ≈ 产出。未达标项：每请求 p50 1463–1910 ms 仍慢于重算 976 ms（exists 在引擎里 200–250 ms，也是护航；TCP 单 worker）。
- **notes** (2026-10-23 夜): exists 在引擎里 200–250 ms = 异步桥接多次线程交接、每次在忙线程后抢 GIL（单测 3 个忙线程复现 215 ms）。修：PyO3 `BatchClient.exists`（同步、整批放一次 GIL；探测在 worker 运行时上与读写并发，否则排在 2 GiB 写后面 p50 ~0.8 s）→ 忙线程下 12.8 ms，写入进行中 p50 0.6–1.9 ms。剩余长尾（max 377–794 ms）推断为 PS 每连接只放行 4 个请求（`conn_inflight_cap`），探测排在同连接的大写后面，未验证（验证：`--conn-inflight-cap 64`）。client_workers：单 rank 4 个读 2 GiB 0.80→0.38 s，但 8 rank 交替对比读合计 6.78/7.29 → 5.18/5.74 GB/s（-22%），默认保持 1。引擎内 4 workers 时 page_first_direct 每请求 p50 1079 ms（重算 976），取数瓶颈是 EN 冷读（direct I/O 写后不在 page cache：单测冷读 0.59–0.75 s / 热读 0.34 s）。
- **notes** (2026-10-23 夜，已推 autumn-rs fd6ccf89 / autumn_kvcache aa715f1，默认 1 worker): 引擎内逐请求、timeout：命中 99%；page_first_direct 每请求 p50 1370 ms（取 2 GiB 950 ms、exists 52 ms），layer_first 1701 ms；重算 976 ms。写入 L3 page_first_direct 2.08 GB/s。验收"每请求低于重算"未达成，瓶颈在 EN 冷读带宽。
- **notes** (2026-10-24): exists 长尾（写入进行中 p99 500–800 ms）的根因不是 PS `conn_inflight_cap`（调到 64 不变），也不是 GIL、连接队列：io_uring 在提交系统调用里就地完成 socket 发送，16 MiB 页在回环上是内核里的 CPU 活，worker 线程 93% CPU 卡在 `submit_with_args`（gdb 25/25 采样），同一运行时上的探测只能等。对照：另一线程上的 client p99 12 ms；写 1 MiB 页长尾缩到 35 ms。修：BatchClient 多一个专门跑 exists 的线程（自己的运行时和连接），读写 worker 恢复串行 → 同 client 写入进行中 exists p99 12–13 ms、max 13 ms；消融（探测走读写 worker）p50 927 ms。
- **notes** (2026-10-24，exists 长尾根因修正): 前面"专门 probe 线程"只对 TCP 的 io_uring 就地发送有效；UCX 上同 client 探测 p99 270 ms 的真因是 PS 每连接 4 个准入槽读写共用，同连接上 2–3 个 16 MiB put 占满后 HEAD 要等一次提交。修：RPC 客户端加 urgent 通道（读类请求先发）+ PS 连接的发送窗口（≥64 KiB 的未应答请求最多 3 个，`--ps-send-window`），probe 线程删掉，exists 回到同一 worker/同一 ClusterClient。同 client 写入进行中 exists p99 270 → 8.6 ms；perf-check 交替对照无一格变慢，UCX 单分区写 +30%。

### F-KVC-PARTITION-LAYOUT — PS 规划 8–16 分区，kvc 默认按哈希预切
- **Trigger** (2026-10-23 用户 + 实测): 默认部署下一个模型的 KV 全在一个分区（实测写 0.36 GB/s）；kvc key 的模型标识带大写，`presplit --namespace` 拒收，只能 `presplit --namespace kvc --hash-prefix <model_scope>/kv/` 且只能切空键空间。
- **Scope**: PS 分区数按 8–16 规划；kvc 按哈希预切做成部署的默认动作（模型标识事先不知道时怎么切要设计）。
- **Acceptance**: 新部署的 kvc 写入无需人工就分散到 8–16 个分区；ops.md 写明。
- `passes: false`

### F-SST-SMALLER-BLOCKS — 块缓存未命中时 4K 读只有 memtable 的 15–20%（方案 B，未做）
- **Trigger** (2026-10-28，BUG-READ-SLOWER-AFTER-FLUSH 验收拆出): 读集合超出 512 MiB 块缓存时，每次未命中为一个 4 KiB 值从 EN 取回约 62 KiB 的块并校验整块 CRC；固定 key 集刷盘后读 TCP 0.21M / UCX 0.24M ops/s，memtable 1.33M / 1.23M（ycsb `--before-run`，docs/ops.md "Core-path performance validation"）。
- **Scope**: 量块大小（如 16 KiB）对未命中读、命中读、写放大、SST 索引大小与 compaction 吞吐的影响，再定是否改默认块大小。
- **Acceptance**: 同一测法下未命中读提升且命中读、写、compaction 不降（交替对照）；结论与数字写进 ops.md。
- `passes: false`
### BUG-MEM-TIER-EMPTY-GROUPS-LINGER — 条目被删后其组不回收（评审推断，未复现）
- **Trigger** (2026-10-27 内存层批量写的第三轮评审): `MemShard::remove`（删除、批量写撤回、落盘覆盖后的 `applied`）只删条目，组里的 key 副本和 CLOCK 槽位留着；同一批 key 跨批次反复重写或删除时，空组只在预算满触发淘汰时才被清掉，这部分内存不计入预算、槽位数组持续增长。
- **Scope**: 先复现（反复重写/删除同一批 key，看 groups 槽位数与 RSS），坐实后让空组随最后一个条目离开而释放。
- **Acceptance**: 复现用例修复前槽位数随轮次增长、修复后有界；消融变红；内存层其余测试全过。
- `passes: false`

### F-KVC-CACHE-TIER — PS 之上的 kvc 专用薄缓存层（记录，暂缓）
- **Trigger** (2026-10-23 用户): 从性能看需要给 kvc 一层很薄的缓存：write-through、超大 CLOCK、选择性落盘，把所有节点的内存池化；放在 PS 层之上，PS 层以下保持原样（不在 PS 里加读缓存——存储不该带重读缓存，3FS/HDFS 都不做）。
- **Scope**: 等 F-KVC-ADAPTER-WRITE-READ 读写跑满之后再定要不要做、怎么做（对标 Mooncake：内存先确认、eager 或淘汰时落盘）。
- **Acceptance**: 先给设计，用户定了再立实现条目。
- `passes: false`
- **notes** (2026-10-24 设计定稿，用户选): 研究结论——sglang 每页进 L2 后都写 L3，`write_through_selective` 只是 L2 两次命中门槛；`batch_exists` 按连续前缀计；exists 后 get 失败会截断重算、不重试写 → L3 丢数据只损命中率。Mooncake：DRAM 即确认、默认 1 副本、近似 LRU、exists 后租约防淘汰、SSD 落盘可选（默认 eager，另有 offload_on_evict + 有界队列）。用户定：① 淘汰时只把被读过的条目落盘（队列满就丢）；② 按一次写批成组淘汰，exists 置引用位；③ 预算整个 PS 共享（按分区分片）；④ 直读命中内存先在重定向里回值，量过再定。先做不落盘的最小版量收益。
- **notes** (2026-10-24，最小版已提交，不落盘): `--mem-tier-bytes` / `--mem-tier-prefix`，bulk put 进内存即确认、读先查内存、成组 CLOCK、整个 PS 共享预算。微基准（UCX，8 rank×128×16 MiB）：写 2.76→8.65 GB/s，读 5.34→10.54 GB/s。引擎端到端（Qwen3-VL-32B、逐个请求、timeout、TCP、page_first_direct）：每请求 p50 1344→980 ms（重算 976），取 2 GiB 999→699 ms，命中 99%。还没低于重算：单 rank TCP 读内存约 3 GB/s。下一步候选：落盘（淘汰时落盘被读过的条目）；UCX/零拷贝回包（缓存值放进注册内存）；跨分区让出预算（闲置分区不让）；被改写 key 的旧组副本到组淘汰才释放；千万级条目的 HashMap 扩容卡写 actor；过期条目到组淘汰才释放。

### BUG-MEM-TIER-LONG-BURST-LOSES-HEAD — 长批次/写入被拒时批次开头的组仍被淘汰（用户定不修）
- **Trigger** (2026-10-26 评审复现): 只保护当前组时，超过 256 条的批次开头的组仍会被淘汰（400 条批次复现）；写入被拒不更新上次写入时间，连续被拒超过 50 ms 后批次视为结束，开头的组失去保护。
- **Scope**: 腾地方时同时跳过本批次第一个组和当前组；被拒的插入也延续批次。
- **Acceptance**: 回归测试两项各自变红再变绿；写入不停时分区仍接收新写入。
- `passes: false`
- **notes** (2026-10-26): 实现后评审发现高危：批次按时间切，写入不停时永不结束，分区只剩"第一个组+当前组"时每次插入都被拒、且被拒又延续批次 → 冻结（单测：最后 1000 次写入 0 次接收；已提交版本全接收）。用户定撤销两处改动，保留 `nonstop_writes_keep_being_accepted`。若要做，需客户端给出批次边界（一次 put_from 一组），属协议改动。

### BUG-FS-DIR-NLINK-LOST-UNDER-CONTENTION — 同目录高并发 mkdir 父目录 nlink 少记（已复现，未修）
- **Trigger** (2026-10-23 父目录类型检查的基准): 4 会话 × 300 次 mkdir 进同一目录，结束后父目录 nlink 比 2+1200 少 0–18（改前改后都有）。`update_dir` 对父 inode 的 CAS 连续冲突 16 次就放弃、只记 WARN（名字已发布，不报错）。
- **Scope**: 先定位放弃次数与冲突分布；修法候选：退避重试直到落地（不能卡住 FUSE 单请求循环）、或把子目录计数从父 inode 拆出去（比如读时由目录项数推出 nlink）。性能不能降（同一基准 mkdir ~4.2K/s）。
- **Acceptance**: 同一基准多轮 nlink 一个不少；去掉修复变红；mkdir 吞吐不降。
- `passes: false`

### BUG-ROW-PREFIX-CUT-DEFERRED-ON-IDLE — 行流前缀裁剪被推迟后，空闲分区再也不裁（推断，未复现）
- **Trigger** (2026-10-28，测试集体检遗留项拆出): `truncate_unreferenced_row_prefix` 遇到在途 op（Recovery/EC）时推迟到"下一次 compaction"；分区此后空闲就没有任何东西再裁，被替换下来的 row extent 一直留在流里（占空间，不影响正确性）。tick 上的 reclaim 探针只在有活表引用头 extent 时才动，不管无表引用的前缀。
- **Scope**: 先复现（compaction 替换掉的 row extent 上有在途 op，使裁剪被推迟，然后分区空闲），坐实后让推迟的裁剪在 op 结束后由已有的周期机制补做（在维护闸门下，与 compaction 串行）。
- **Acceptance**: 复现用例修复前 extent 留在流里、修复后被裁；消融变红。
- `passes: false`
- **notes** (2026-10-28): 用 `system_sst_block_rot::a_compaction_reads_around_a_rotted_replica` 去掉重复 compact、只 compact 一次后空闲 40 s，6/6 次都在 200 ms 内裁掉，未出现推迟；未修。
### BUG-LEASE-HEARTBEAT-STALE-NOTHELD — 心跳快照期间重新 acquire 的 lease 会被迟到的 NotHeld 删掉（推断，未复现）
- **Trigger** (2026-10-17 fuse_lease_loss 修复的 opus 评审): `lease_tasks.rs` 心跳循环先快照 `held_leases` 的 ino 再逐个 await 心跳 RPC；期间最后一次 release + 新 Open 可以重新 acquire 同一 ino，迟到的 `NotHeld` 把新条目删掉（现在还会多做一次失效）：后续写 EIO "no lease"，manager 那份新 lease 漏到 TTL 过期。
- **Scope**: 先复现（心跳 RPC 前挂暂停点，期间 release+acquire）。坐实后：删条目前比较快照时的 `lease_epoch`，不同则不删。
- **Acceptance**: 复现用例修复前新 lease 被删、修复后保留；消融变红。
- `passes: false`
- **notes** (2026-10-17): 分析后未复现、未修。挂载对 manager 只有一条 FIFO 连接：心跳的回复总在之后发出的 release/acquire 的回复之前到达，新条目只可能在迟到的 NotHeld 落地之后插入——单连接上不可达。可达的前提是心跳与重新 acquire 走不同连接（心跳期间 manager 换主、`mgr_call_retry` 轮换到新 leader）。若要修，删/置 revoked 前比较快照时的 `lease_epoch` 即可；等有换主中的复现再做。
- **notes** (2026-10-28 证伪): 不可达。manager 回 NotHeld 的分支在回包前没有 await（`handle_heartbeat_lease`，rpc_handlers.rs:7744），同一连接上它的回包排在之后任何请求的回包前面；换主时 release/acquire 要先在旧连接上拿到 NOT_LEADER 才会转到新 leader，那个回包同样排在心跳之后，所以迟到的 NotHeld 总在新 lease 插入前落地。建议关闭本条（验收的"复现用例"无法构造）。

### F-ETCD-AUTH — manager 连接带认证/TLS 的 etcd
- **Trigger** (2026-10-07 用户): 生产 etcd 要开 auth。现 manager 只有 `--etcd <endpoints>`，`crates/etcd` 是 h2c 明文 gRPC，无 `Auth/Authenticate`、无 token 头、无 TLS；开 auth 的 etcd 连不上。
- **Scope**: 用户名+密码（`Auth/Authenticate` 换 token，每请求带 `token` 头，过期重认证一次再发）；TLS（rustls，CA 校验，可选双向证书）。CLI flag：`--etcd-user`、`--etcd-password-file`、`--etcd-cacert`、`--etcd-cert`、`--etcd-key`（rs 不读 env）。cluster.sh / docs/ops.md 同步。
- **Acceptance**: 对开了 auth+TLS 的真实 etcd，manager 选主、replay、写入、token 过期后自动重认证均正常；错误口令/证书启动即响亮失败；不带新 flag 时对明文 etcd 行为不变。
- `passes: false`
- **notes** (2026-10-07): 用户定：后做，排在 F-CLUSTER-STATUS-SUMMARY 之后。

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
