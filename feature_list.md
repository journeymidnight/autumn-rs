# autumn-rs feature list — OPEN backlog

**Last updated:** 2026-09-02

**Rules:**
- This file tracks the **OPEN backlog only**. A feature that reaches `passes: true`
  is **DELETED** from here — git history is the record, there is no archive file
  (CLAUDE.md rule 13: 定期清理删除，保持整洁).
- `passes` and `notes` are the only mutable fields after a feature is created.
- Out-of-scope / "v2 再做" decisions must be recorded as proper feature entries
  (F-name + Trigger + Scope + Acceptance + `passes: false`), never as plan-file footnotes.

---

## Active

### F-EC-REPLAY-SELFHEAL — EC 重放路径拿不到 R2/R3 的自愈，且它唯一处理的哨兵是服务端不会发的那个
- **Trigger** (2026-09-02, fable 对 R1-R4 的评审): `read_committed_bytes_from_extent`（`crates/stream/src/client.rs` 的第二个 2-attempt 循环）**只处理 `EversionStale`**；`PayloadNotHere` 与 `EcGatherOneShort` 作为硬错误直接冒泡，没有 invalidate/refetch。而这条路正是 PS 打开分区时 `recover_partition` 按 64 MiB 分块**重放 log stream** 走的路。
  **为什么重放会遇到 EC**（没有"EC WAL"这种东西，链条是四步）：PS 的 WAL 记录写进 log stream → `--log-ec K+M` 是 bootstrap 参数，设的是 log stream **封口之后**的 EC 编码形状（4 EN 默认 3+1、≥5 默认 4+1，即默认开启）→ 该 stream 已封口的 extent 被转成 EC → 重放读到它们。
  **暴露面是负载/时机相关的**：EC 只作用于已封口 extent，若 replay floor 很新则那些 extent 可能还开着（未封口即不会 EC）；真正会撞上的是**距上次 checkpoint 很久、或长时间宕机后恢复**的分区。
- **更糟的一半**: 服务端 `read_plan` 是**先查 `holds_payload` 再查 eversion**（`extent_node.rs:2503-2512`），所以缓存里的 `payload_location` 陈旧时，EN 回的是 `PAYLOAD_NOT_HERE` 而**不是** `EversionStale` ⇒ 这个循环唯一会处理的哨兵，恰恰是服务端在该场景下不会发的那个。后果是重放响亮失败（可用性问题，非损坏），且这条路上没有任何东西会驱逐缓存 ⇒ **单个 StreamClient 生命周期内不收敛**。
- **Scope**: 把 `is_payload_not_here` / `is_ec_gather_one_short` 两条臂接进该循环，语义与主读路径一致（invalidate → 重取 → 重新规划；one-short 带抖动，payload-not-here 不睡）。注意两个循环的重试预算要各自独立，别叠成 4 次。
- **Acceptance**: 构造"客户端缓存 payload_location 陈旧 + extent 已 EC 转换"的重放，验证第一次 `PAYLOAD_NOT_HERE` 后自动刷新布局并成功；同一 StreamClient 内重复重放不再持续失败；两个循环各自最多重试一次（用注入计数断言，别只断言最终成功）。
- **Status**: `passes: false` (2026-09-02)

### F-EC-RETRY-TESTS — R2 的"边界"回归测试实际上测不到边界
- **Trigger** (2026-09-02, fable 对 R1-R4 的评审): 账本称"3 个回归测试锁住边界"，但那几个测试只钉了**哨兵的管道**：`multi_short_is_not_retryable` 自己手搓一个字符串错误再断言"它不是类型化哨兵" —— **即使生产代码在 `success == K-2` 时挂上哨兵，这个测试照样通过**。真正要守的三件事一件没测：`ec_reconstruct_shard_subrange` 的计数逻辑（何时才算 one-short）、`attempt == 0` 的只重试一次、以及 R1 的"缺失分片答上来就逐字返回"。同文件的 `manager_retry_tests` 是反例，它确实驱动了自己的重试循环。
- **Scope**: 让测试驱动真实函数而不是构造错误对象 —— 注入可控的 per-shard 结果（成功/短读/错误/超时），断言：K-1 成功才挂哨兵、K-2 不挂；重试恰好发生一次；R1 那条路返回的字节与分片内容逐字相同且**跳过** RS。
- **Acceptance**: 把生产代码的判据从 `success + 1 == data_shards` 改成 `success + 2 == data_shards`，测试必须变红（消融验证）；把 `attempt == 0` 去掉，测试必须变红。
- **Status**: `passes: false` (2026-09-02)

### BUG-EC-PARTICIPANT-UNSEALED — EC 转换的参与者从不落盘封存状态，重启后那个纯分片 extent 在它眼里可写
- **Trigger** (2026-09-02, 用户在排查一起 EC 读故障时顺手挖出，非该故障的原因): 同一个 extent，协调者与参与者的持久状态不一致 —— en-0（协调者）`sealed=1 / sealed_length=17183737326 / eversion=2 / avali=7`，en-1..4（参与者）全是 `sealed=0 / sealed_length=0 / eversion=1 / avali=0`。
- **已核实（我自己读的代码，不是转述）**: `write_shard_stripe_local`（`crates/stream/src/extent_node.rs`，全长 153 行）里 **`save_meta` 出现 0 次**，从不写 `entry.sealed`、不写 `avali`，`sealed_length` 只作越界校验用；`apply_placements` 后来只补写了 `payload_location`。而 append 的封存判据是 **`sealed_length > 0 || avali > 0`**（见 stream/CLAUDE.md 附录的 append 协议第 3 步），参与者持久化的这两个值**都是 0** ⇒ 参与者重启后，该 extent 在它眼里既未封存也无 avali 位，**这道闸门不会拒绝写入**。
- **未追到底的部分（不要跳过这一步）**: 闸门有洞是确定的，**危害链条还没走完**。参与者的 `.dat` 通常是空的（分片在独立文件里），所以一次被放行的 append 落在 `.dat` 上、而读按 `payload_location=InShardFile` 去读分片文件——**直接的字节损坏未被证明**。真正要查的是：这样的 extent 在参与者眼里"未封存"之后，recovery / seal / avali 重建这些路径会不会把它当成开着的尾巴处理。按仓库规矩（`feedback_reproduce_before_fixing_mechanism_bugs`）**先复现再修**。
- **Scope（复现后）**: 参与者在写完自己的分片后必须落盘封存状态（`sealed=1` + `sealed_length` + `eversion` + 自己的 `avali` 位），也就是 `write_shard_stripe_local` 或 `apply_placements` 尾部补一次 `save_meta`。注意这是**持久态布局改动**，按升级纪律要么同布局要么带迁移。
- **Acceptance**: 构造"EC 转换完成 → 杀掉参与者 → 重启"，断言重启后的参与者认为该 extent 已封存（`sealed` 与 `sealed_length` 非零），且对它的 append 被 `CODE_PRECONDITION` 拒绝；转换后再读，字节精确。
- **Status**: `passes: false` (2026-09-02)

### F-MEM-WIPE-COST — `memory-mcp --reset` 在真实语料上要 10 分钟（扫描绑定，非写绑定）
- **Trigger** (2026-09-02, 建 F-MEM-EVAL 时实测撞上): `wipe_agent` 按页 `range(512)` + 逐 key 删除，清一个 5164 chunk 的文档语料要删 **1,987,843 个 key**，耗时 **9 分 48 秒**（3380 key/s）。文档语料的 key 绝大部分是 BM25 posting（一个中文 chunk 几百个不同 term），所以 key 数是 chunk 数的约 385 倍。
- **⚠️ 已排除的解法（负结果，别重做）**: 把删除循环换成 `delete_many` 并发扇出**实测无收益**（3380 → 3418 key/s）。原因：`ClusterClient::delete_many` 不是批量 RPC，而是并发上限 32 的客户端扇出；而同集群 `perf-check` 显示单分区写 1 线程 9.2K ops/s、8 线程即达 30K ops/s 天花板 ⇒ 删除本身只该占约 12 s，104 s 里的绝大部分是 **694 次 range 扫描**（每页 512 键约 133 ms）。改动已回滚（按 `feedback_no_defensive_fixes_for_imaginary_bugs`：没有实测收益的优化不留）。**真正的瓶颈是前缀扫描，不是删除。**
- **Scope（真要做时）**: 先测准扫描为何这么慢（一页 512 键 133 ms 与 30K ops/s 的写路径不相称；怀疑 tombstone 累积后 iterator seek 变贵，需要在 PS 侧计时确认），再决定是加服务端前缀删除（一次 RPC 删一个前缀，PS 内部直接下 range tombstone），还是仅优化扫描。**在测准之前不要动客户端。**
- **Acceptance**: 清一个 5164 chunk 语料（约 200 万 key）从 ~10 分钟降到分钟以内，且有 PS 侧分段计时证明瓶颈确实被移走；`reconcile` 在清空后 `is_clean`。
- **Status**: `passes: false` (2026-09-02) — **瓶颈已测准并移除，但没打到"分钟以内"这条线**。
  实测（同一份 1,987,843 key 的语料，本机 3 节点 --3disk）：**588 s → 70.2 s**（8.4×，
  29,190 key/s）。完整性直接验过：wipe 后前缀扫描返回空，且三次独立运行的删除计数都精确等于
  1,987,843（扫描若提前结束，计数会变少）。
  第一步是扫描：`handle_range` 每次请求都把**整个 memtable + 全部 imm** 克隆成 Vec 再排序
  （`collect_mem_items` → `snapshot_sorted`），而且连 value 一起克隆——range 返回的
  `RangeEntry.value` 恒为空，那些 value 是纯浪费。这笔开销是 O(memtable) 且与 page limit
  无关：release 实测 50 万条 64B value = 66.5 ms、512B value = 124 ms、100 万条 = 212 ms，
  和现场观察到的 133 ms/页对得上。改成从 seek 点起的**窗口**快照（cap = max(4×limit, 4096)、
  不克隆 value），每页固定 ~215 µs，与 memtable 大小无关。
  第二步是删除：把逐 key 删除换成 `delete_many`，只修扫描时是 253.5 s，加上批量删除后
  70.2 s = **29,190 key/s**，正好压在单分区 ~30K ops/s 的写天花板上。**上面那条"负结果"
  由此被推翻**——当初测不出收益是因为扫描占了全程的绝大部分，删除侧再快也显示不出来；
  瓶颈换了以后同一个改动值 3.6×。教训：负结果必须连同"当时的瓶颈在哪"一起记。
  剩下的 70 s 是单分区写天花板，客户端侧再优化不动了；要进一分钟内需要 `mem` 命名空间多分区。

### BUG-KVC-MM-ALIAS — 多模态请求取不到 mm hash 时仍然缓存（跨图片 KV 串读）
- **Trigger** (2026-07-22, coco deep inspect `vllm_connector.py:193`；**已复核代码为真**): `_request_extra_keys()` 已经识别出"有 `mm_features` 但取不到任何 mm hash"这一情况并打 warning，注释甚至写明 "its prefix hash would collide across DIFFERENT images sharing the same placeholder token ids (false-alias → wrong output)" —— 然后**照样返回不含 disambiguator 的 keys 并继续缓存**（"the connector still caches (best-effort)"）。VLM 场景下同尺寸不同图片的 placeholder token 序列可以完全相同 ⇒ 用户 B 可能读到用户 A 的视觉 KV：错误输出 + **跨请求信息泄漏**。
- **Scope**: `_request_extra_keys()` 改返回 `Optional[List[str]]`，无法区分多模态内容时返回 `None`；load 路径 `get_num_new_matched_tokens()` 见 `None` 直接 `return 0, False`，store 路径 `build_connector_meta()` 见 `None` 直接跳过保存（= 该请求不participate external KV，纯文本请求不受影响）。优先复用 vLLM 自身的 BlockHash / extra keys，而不是在 connector 里 best-effort 猜字段名。
- **Acceptance**: 单测 —— (a) 有 `mm_features.identifier` 时不同图片得到不同 hash；(b) `mm_features` 存在但字段取不到 hash 时**既不 load 也不 save**（当前会 save）；(c) 无多模态的纯文本请求行为不变。
- **Status**: `passes: false` (2026-07-22) — cross-ref memory `project_kvcache_vlm_mmhash_unverified`（"mm_hash 漏 key"曾是已修 bug，本条是它的残留 fail-open 面）。
  **用户定调 2026-07-22**（在 "fix BUG-KVC-TENANT" 之后说「剩下的 2 个你看着办，一般不需要」）: 本条**不做**，留在 backlog 只作记录。真要动之前，先复核它的触发条件是否已经在线上出现过。
### BUG-KVC-PARTIAL-PREFIX — chunked prefill 下可能为"部分 KV"发布完整 prefix marker【假说，未复现】
- **Trigger** (2026-07-22, coco deep inspect `vllm_connector.py:675`): store 路径按**完整 prompt** 的 block-aligned prefix 算 `content_hash`，但 slot 来自当前调度步的 `block_ids`；代码只检查 `_slot_len(slots) == 0`，**没有**检查 `== num_tokens`。coco 推断：chunked prefill / token budget 不足时当前 step 只覆盖部分 prefix，保存的 KV 少于 `num_tokens`，而 `wait_for_save()` 只校验 layer 数齐全就 `mark_present(content_hash)` → 把部分 KV 伪装成完整 prefix 命中。
- **⚠️ 未验证**: 这条依赖"vLLM 会在 prefix 未全部计算完时就调用 save"这一假设，**没有复现**。按 house rule（`feedback_reproduce_before_fixing_mechanism_bugs` / `feedback_no_defensive_fixes_for_imaginary_bugs`）：**先复现再改**，不许凭推断在这条热路径上动刀。
- **Scope（复现后才做）**: 先写复现 —— 开 chunked prefill + 长 prompt，断言 `mark_present` 的 prefix 与真实已计算 token 数一致。若确认成立：`build_connector_meta()` 加 `_slot_len(slots) != num_tokens → skip + warning`；marker 里写入 `num_tokens` 供加载前校验。
- **Acceptance**: 先有一个能稳定复现"marker 声称的 prefix > 实际保存 token 数"的测试；修后该测试转绿且正常（非 chunked）路径命中率不变。
- **Status**: `passes: false` (2026-07-22, 假说待复现)。
  **用户定调 2026-07-22**（在 "fix BUG-KVC-TENANT" 之后说「剩下的 2 个你看着办，一般不需要」）: 本条**不做**，留在 backlog 只作记录。真要动之前，先复核它的触发条件是否已经在线上出现过。
### BUG-KVC-SGLANG-ZC-CANCEL — sglang 零拷贝传输无超时/取消，host page 被回收后可能仍被读写【假说，未复现】
- **Trigger** (2026-07-22, coco deep inspect `sglang_backend.py:274`): `_batch_v1()` 把 `host_indices` 解析成 pinned host page 的 view 后直接 `transfer(full_keys, views)`（get 走 `batch.get_into` 写入这些页，set 走 `batch.put_from` 从这些页读），**不带 deadline/timeout/cancel**。coco 推断：sglang HiCache prefetch 有超时语义，上层可能放弃并复用这些 `host_indices`，而 native 侧传输仍持有原指针 → get 把旧数据写进已复用页（KV 静默污染）/ set 把新页内容当旧 KV 持久化。
- **⚠️ 未验证**: 依赖"sglang 在 prefetch timeout 后会回收/复用 host_indices"这一对上游行为的假设，**未查证、未复现**。同上，按 house rule 先复现再改。
- **Scope（复现后才做）**: `batch_get_v1`/`batch_set_v1` 处理 `extra_info` 里的 deadline，或给 client 配 bounded RPC timeout；超时返回 `[False]*len(keys)` 且保证 native worker 不再触碰 caller-owned buffer。若 native 层无法取消进行中的零拷贝 I/O，get 路径需先落 backend-owned 临时 buffer、确认请求仍有效再拷回。
- **Acceptance**: 先复现（人为拖慢 autumn 侧、触发 sglang prefetch timeout、检测页内容是否被事后覆写）；修后该场景下页内容不被污染，且正常路径零拷贝性能不退。
- **Status**: `passes: false` (2026-07-22, 假说待复现) — cross-ref memory `feedback_zc_recv_no_leak_on_cancel`（本项目对"取消时 ZC buffer 归属"已有既定立场，本条是 Python 侧同类问题）。
  **用户定调 2026-07-22**（在 "fix BUG-KVC-TENANT" 之后说「剩下的 2 个你看着办，一般不需要」）: 本条**不做**，留在 backlog 只作记录。真要动之前，先复核它的触发条件是否已经在线上出现过。
### F-KVC-CONNECTOR-HARDENING — kvcache 两个 adapter 的 P2/P3 加固批
- **Trigger** (2026-07-22, coco deep inspect 的 P2/P3 余项，逐条已复核):
  1. **【真，已复核】per-client event loop 建了却从不使用**（`vllm_connector.py:295-297`、`sglang_backend.py:205-206`）: 两处都 `self._loop = new_loop()` + `run_on(self._loop, connect)`，但**此后所有操作都走模块级全局 loop 的 `run(...)`**（`_bridge.run` → `get_loop()`），`_loop0`/`_loop` 再无第二个引用点。而 `new_loop()` 的 docstring 明写它的存在理由是"给每个 client 自己的 loop，避免 result marshaling 串行化 —— 正是让 in-process 多 client fan-out 不 scale 的瓶颈"。⇒ **该优化对这两个 adapter 实际从未生效**，还每实例泄漏一个常驻 daemon 线程。非正确性问题（PyO3 在调用时绑定当前 running loop，所以走全局 loop 是对的），是性能/资源缺陷。
  2. 后台 save job 无 timeout/watchdog（`vllm_connector.py:883`）: `_inflight_saves` 的归还全靠 `finally`，但 `gather_event.synchronize()` / `save_layers` / `mark_present` 都无超时；`_MAX_INFLIGHT_SAVES = 2`，两个卡死任务即让后续保存长期被 drop，而 drop 日志是 debug ⇒ 线上只表现为命中率下降。
  3. 无 `close()`/`shutdown()`（`vllm_connector.py:295`）: event loop 线程、Rust BatchClient worker、`ThreadPoolExecutor` 全靠进程退出兜底；engine reload / 测试反复构造会泄漏。
  4. save quota 在 GPU gather **之后**才判（`vllm_connector.py:827`）: 超限的请求已经先 `_extract_layer()` 分配了 GPU staging，到 `wait_for_save()` 才丢弃 ⇒ 限流挡不住瞬时显存峰值。
  5. `mark_present()` 返回值被忽略（`vllm_connector.py:895`）: marker 写失败只有 debug 日志 ⇒ layer 数据已占存储却永不可命中，重复保存同一 prefix（写放大），线上无信号。
  6. `_stats` 无锁（`sglang_backend.py:277`）: `dict[k] += n` 是复合读改写，多 daemon 线程并发下丢计数；仅影响监控口径。
- **⚠️ coco 第 7 条被否决（与既有决策冲突，不做）**: coco 建议 `sglang_backend.py:129` 的 transport 配置改 fail-fast（非法 transport 直接抛错、UCX 初始化失败不回落 TCP）。这**直接违反** memory `feedback_ucx_warn_not_block` 的用户明令："回落非 RDMA 打警告；绝不 force UCX_TLS/硬失败"。**唯一可取的窄化版本**：对**拼写错误的 transport 名**（如 `"ucxx"`/`"rdma"`）做白名单校验并报错 —— 这是配置笔误，不是 RDMA 不可用回落，与该 feedback 不冲突。是否做由用户定。
- **Scope**: 上述 1–6 逐条修（1 = 要么全程用 `run_on(self._loop, ...)`、要么删掉 `_loop` 别建；2 = store 操作加 timeout + future done-callback/watchdog + drop 日志提到限频 warning；3 = 加 `close()` 并在 save 路径检查 `_closed`；4 = quota 前移到 gather 前按 req_id 预留；5 = 检查返回值并 warning；6 = 加 `threading.Lock` 或声明 best-effort）。
- **Acceptance**: 1 有单测/日志证明每 client 用自己的 loop（或线程数不再随实例增长）；2 注入一个永久阻塞的 store，断言 watchdog 报警且 `_inflight_saves` 不永久占满；3 `close()` 后线程全部 join；5 marker 失败有 warning。
- **Status**: `passes: false` (2026-07-22, coco 发现 + 主 agent 逐条复核；第 7 条已否决)。
  **用户定调 2026-07-22**（在 "fix BUG-KVC-TENANT" 之后说「剩下的 2 个你看着办，一般不需要」）: 本条**不做**，留在 backlog 只作记录。真要动之前，先复核它的触发条件是否已经在线上出现过。
### F-STREAM-ATREST-CKSUM — stream 层大 value 的 at-rest 内容校验 + scrub（静默腐化 G12）
- **Trigger** (2026-08-04, chaos 缺口 loop 的 G12，已 reproduce-first 复现 harness `crates/manager/tests/silent_corruption_rot.rs`): sealed extent 的 **value 数据字节**在单副本上被静默翻位后，**全链无检测**：(a) 客户端读回坏字节仍返回 `CODE_OK`（frame CRC 明确排除 bulk value 段；`.meta` CRC 只覆盖 40B 元数据；WAL/SST CRC 是 partition 层、不覆盖 stream extent 的原始 value）；(b) recovery 从坏副本重填时 `verify` 只校 `length==sealed_length` + eversion、**不校内容** → 把腐化洗成权威；(c) EC 转换对坏字节直接编 parity → 固化成 canonical。stream 层**既无 per-extent/block content checksum、也无 scrubber**；确定性副本轮转让坏副本被一致选中（harness 里 25/64 子区间读命中）。这是**设计缺口**（数据完整性面），不是坏代码——today 的裸机盘不会自发翻位、且需要单副本静默腐化才触发，故不是"今天可复现的线上危害"，属于中期加固。
- **Scope（真要做时）**: (1) 写侧对 sealed extent 落 **per-extent/block content checksum**（`.meta` 里加一段覆盖 `.dat` 内容的 CRC/xxhash；注意不能进 append 热路径的每帧 CRC，只在 seal 时对最终内容算一次）；(2) EN 读时（至少 sealed 全值读 + recovery 重填读）验内容 checksum，错则走**现有副本轮转/failover 绕开**坏副本（隔离路径已存在，缺的是检测触发器）；(3) recovery/EC 转换前加内容校验，**拒绝**把校验失败的副本洗成权威/编进 parity；(4) 后台 **scrub loop**：低速重哈希 sealed extent，mismatch 则清该副本 `avali` 位交给 recovery 重建。
- **Acceptance**: 用 `silent_corruption_rot.rs` 的注入点——翻转单副本 sealed `.dat` 字节后：客户端读返回错误（非 `CODE_OK` 坏字节）或自动从好副本服务正确字节；recovery 不再从坏副本洗白（重填结果字节精确）；EC 转换对坏副本报错而非编坏 parity；scrub 能在无外部读的情况下自行发现并清 `avali`。harness 从"记录暴露"翻成 fail-until-fixed 正确性断言。
- **Status**: `passes: false` (2026-08-04) — **backlog（用户定调 2026-08-04「g12 放到 backlog 里面」）**：已 reproduce（harness 未提交/已提交见 chaos 套件 `b15168c`），本轮**不实现**，留账本记录。cross-ref memory `project_chaos_gap_loop_findings`（G12 条）。真要动之前先确认触发条件（单副本静默腐化）是否已在真实硬件/线上出现过。

### F-SEALED-EMPTY-SWEEP — manager backstop sweep for leaked sealed-empty non-tail stream members
- **Trigger** (2026-07-14, BUG-FLUSH-TIMEOUT-LEAK follow-up): the live 5-node wedge (10.4 TB leaked / 222 GB logical, 47×) was fixed two ways in `crates/stream/src/client.rs` — size-scaled append deadlines (`effective_append_timeout`) and writer-side reclaim of a tail sealed at commit=0 on roll-away (`reclaim_abandoned_empty_tail`, best-effort punch). The reclaim is CLIENT-side and best-effort: if the punch (or the authoritative `extent_info` re-fetch) fails — manager briefly unreachable, extent momentarily in a Recovery/EC ledger op — or the writer process dies right after the roll, that one sealed-empty extent still leaks forever (same unreclaimable shape: `sealed=true, sealed_length=0`, refs≥1, non-tail stream member, referenced by no VP/SST/checkpoint, invisible to accounting, skipped by GC/truncate/orphan-reconcile). A cluster already poisoned by the pre-fix bug also holds ~40k such extents that the writer-side fix will never revisit.
- **Scope (when triggered)**: leader-only manager sweep (mirror `extent_both_zero_sweep_loop`, extent_delete.rs): for each stream, any member extent that is (a) NOT the tail (`extent_ids.last()`), (b) `sealed == true && sealed_length == 0` (authoritative empty seal — manager state note 32), and (c) not in the F207 inflight ledger → remove from `streams/<id>` membership (value-CAS per note 33) + refs-- (extent CAS) → existing pending-delete queue unlinks the physical files. Safety = the same argument as the writer-side reclaim: under caller-ack ⊆ commit (stream note 25a) + seal ≥ acked (notes 20/22), a sealed-AT-0 extent has no acked byte ⇒ nothing can reference it. CoW: refs-- only; delete at refs 0. Also drains the pre-fix backlog on an upgraded cluster (the 40k-extent case) — rate-limit the sweep (N extents/tick).
- **Acceptance**: unit test — a sealed-empty non-tail member is swept, a sealed-empty TAIL and a sealed-nonzero member are NOT, an inflight-ledger extent is deferred; integration — kill the writer between seal and punch (or force the punch to fail once), assert the sweep reclaims the extent within one sweep interval; chaos regression (seed 603 + 769351064 class) green.
- **Status**: `passes: false` (2026-07-14) — deferred: the writer-side reclaim closes the death-spiral producer; this is the backstop for writer-death/punch-failure residuals + pre-fix backlog cleanup. Cross-ref stream CLAUDE.md note 28; manager CLAUDE.md notes 32/33/41.

### F-EN-SHARD-AUTO — default EN shard count to CPU cores (format-side), not a hand-set env
- **Trigger** (2026-07-13, user: "EN 分片确实是核数导向,但目前是手动 env,不是自动...对于集群配置有好处,记下来,以后做"): EN sharding IS core-oriented — `AUTUMN_EXTENT_SHARDS` should track io_uring cores (one shard = `extent_id % shard_count`), but it's a MANUAL env (default 1). Operators must hand-count cores AND keep three things in lockstep. It is NOT a simple "read `available_parallelism()` in the EN" because shard_count is coupled through a chain: **(a)** EN ports are static/registered-once — `autumn-op format --shard-ports <csv>` stamps the N ports into etcd and the manager routes by that list forever (stream CLAUDE.md "EN ports are FUNDAMENTALLY static"); a runtime-auto shard count would desync from etcd → manager black-holes shards 1..N. **(b)** the k8s overlay Service must enumerate exactly `shard_count` data+control ports (`9101+i*10` / `10101+i*10`); auto-shard needs the Service port list generated too. **(c)** `AUTUMN_EXPECT_NODES` / presplit sizing are tuned against the shard fan-out.
- **Scope (when triggered)**: make the CORRECT layer (deploy/format, NOT the Rust EN process) default the shard count to cores when unset — entrypoint.sh: `AUTUMN_EXTENT_SHARDS` unset → `nproc` (clamped to a sane max); `autumn-op format` auto-derives `--shard-ports` from it; the k8s overlay generates the per-pod Service port list from the same value (kustomize can't loop → a small generator or documented N-port template). Keep the manual env as an explicit override. Rust EN stays config-driven (no `available_parallelism()` read in-process — the ports must match etcd, which only `format` knows). Cross-ref stream CLAUDE.md "serve_with_control is fail-stop … EN ports are FUNDAMENTALLY static".
- **Acceptance**: a fresh deploy with no `AUTUMN_EXTENT_SHARDS` set brings up one shard per core, `format` registers the matching ports, the Service exposes them, and the manager routes to all shards; the manual env still overrides.
- **Status**: `passes: false` (2026-07-13) — recorded for later per user. Deploy/format-layer change (entrypoint + format + overlay), NOT an EN-process change; the coupling chain above is the reason it's "manual by design" today, not a bug.

### F-FENCE-SEAL — manager-unilateral open-tail seal (EN-side seal RPC) [deferred]
- **Trigger** (2026-07-04, recorded per house rule "v2 再做 must be a feature entry"): F-FENCE-DRAIN's PS-driven roll requires the partition to have a serving PS. A partition with NO PS (all PS down / unassigned for a long period) cannot drain its open tails; the sweep WARNs per cooldown. The WAS-native alternative — manager seals unilaterally — needs an EN-side seal RPC that persists the sealed flag in `.meta` and REJECTS subsequent appends (fencing the writer first), because EN `commit_length` is deliberately check-only (the "Layer-C poison" lesson): probe-sealing under a live writer would truncate acked data.
- **Scope (when triggered)**: EN `MSG_SEAL_EXTENT` (persist sealed in `.meta`, reject appends with a distinct code the writer's roll path maps to seal-and-roll), manager seal-over-reachable orchestration (seal at ENs FIRST, then min over responses, then persist), writer-side append-rejection → existing roll machinery.
- **Acceptance**: fence a node while its partition has no serving PS → tails still drain; live-writer race test proves no acked-data truncation.
- **Status**: `passes: false` (deferred — trigger condition not yet observed in practice; rebalancer reassigns partitions within seconds in all runs so far).

### F-FS-WRITE-STRIPE — fuse MOUNT write-path large-file striping (B2 of F-FS-STRIPE)
- **Trigger** (2026-07-19, 从已完成的 F-FS-STRIPE 抽出剩余项): F-FS-STRIPE 的 autumnfs 路径已 done（write/read/rm striped、字节精确、突破单分区、连续 pipeline），但 **fuse MOUNT 的写路径 striping 延后**——现状 fuse 读/删 striped 正确（stripe-aware），但 mount 内的 write/truncate 对 striped 文件是 fail-loud 拒绝（不是自己写 striped）。
- **Scope**: 让 fuse mount 的写路径（`crates/fuse` write/extent）也能对大文件按 lane 分裂写（stripe key `[0x03][lane][ino][off]`），复用 autumnfs 已验证的 auto-detect lane 逻辑；truncate/删 striped 已支持。streaming 写（不像 autumnfs 上传时已知文件大小）要在写超过 STRIPE_THRESHOLD 时决定 stripe 布局并写 `InodeMeta.stripe`。
- **Acceptance**: fuse mount 写一个 >64 MiB 文件 → 落成 N-lane striped；autumnfs / 另一 mount cold-read 回字节精确；小文件（≤阈值）仍单分区；与 autumnfs 互通。
- **Status**: `passes: false` (2026-07-19, 从 F-FS-STRIPE 完成条目抽出) — 独立 deferred；破 ~450 单机聚合墙需真跨机集群（非代码），MAX_EXTENT 最优在满资源硬件另测（本 rig：≤8 MiB 越小越好）。cross-ref F-FS-STRIPE（autumnfs 路径已 done，git 历史）。
- **⛔ WON'T-DO (2026-07-22, 用户拍板「Fuse mount 可以不做 lanes」)**: 条带写是 **autumnfs 独有能力**，fuse mount 不实现。mount 现有行为保持：对条带文件的 write/truncate **fail-loud 拒绝**，读/rm 仍 stripe-aware 正确。理由：条带针对的是大文件灌入（模型权重），那条路径本来就走 autumnfs；mount 的价值是 POSIX 兼容而非峰值吞吐。注意 F-FS-GEOM-DECLARED 删掉 64 MiB 阈值后本条**已无技术障碍**（lane 函数是增量的，不需要预先知道大小）—— 这是产品取舍，不是做不了。要复活的话工作量是机械的。
### F-IMG-FUSE-MCP — 把 autumn-fuse + memory-mcp 打进容器镜像
- **Trigger** (2026-09-01, FreeToken rollout 规划时发现): 镜像只 build `-p autumn-server -p autumn-dashboard`，`autumn-fuse`（挂载守护进程）和 `memory-mcp` 都不在里面。前者导致"从 autumn fuse 读模型权重"在 k8s 上无法实现（`docs/ops.md` 只有一句"应做成 privileged DaemonSet"的说明，仓库里没有任何 FUSE manifest）；后者导致 hermes 无法以 stdio 子进程方式接 MCP —— 而 stdio 是 `memory-mcp` 唯一的传输方式，`docs/autumn_memory_plan.md:378` 明确不建议常驻 HTTP/SSE MCP。
- **Scope**: `deploy/docker/Dockerfile` builder 段加 `libfuse3-dev`+`pkg-config`（fuser 0.15 链接 libfuse3），构建改 `-p autumn-server -p autumn-dashboard -p autumn-fuse -p memory-mcp`；runtime 段加 `fuse3`（libfuse3 + fusermount3 setuid helper）并拷出两个二进制。`entrypoint.sh` 加 `fuse` role（env→flag：`AUTUMN_MANAGER`/`AUTUMN_FUSE_MOUNTPOINT`/`AUTUMN_CREDENTIAL_FILE`/`AUTUMN_FUSE_DIRECT_READ`/`AUTUMN_FUSE_ALLOW_OTHER`），挂载前 `fusermount3 -u` 清理崩溃残留。`docs/ops.md` 补 sidecar manifest + mountPropagation 配对 + FOPEN_DIRECT_IO 的 mmap 后果。
- **Acceptance**: (a) `deploy/validate.sh` 通过且 role 分派表含 `fuse`；(b) Linux 上镜像构建成功、`autumn-fuse --help` / `memory-mcp --help` 在镜像里可执行；(c) sidecar 挂载后主容器能在 `/mnt/autumn` 看到 `fs/` 内容（mountPropagation 配对生效）；(d) 杀掉 fuse 容器再拉起，不因残留挂载点 EBUSY。
- **Status**: `passes: false` (2026-09-01；2026-09-02 收窄) — (a) 通过；(b) 镜像里有 `autumn-fuse` 且在 v29 集群上真挂载成功（`dd iflag=direct` 4K/8M 两档都过），但 `memory-mcp --help` 未在镜像里单独跑过。**(c) 的形态已被推翻**：本集群 kubelet 未配 rshared，mountPropagation 不兑现（特权 sidecar 内 `grep -c "shared:" /proc/self/mountinfo` = 0），sidecar 主容器看不到挂载；可行形态是**单容器**（autumn-fuse 后台进程与应用同 mount namespace，整容器 privileged）。⇒ 本条剩余工作 = `docs/ops.md` 的 sidecar manifest 改写成单容器形态 + (b) 的 memory-mcp 检查 + (d) 杀容器重拉不因残留挂载点 EBUSY。

### F-S3-RUNAI-PLUGIN — 用 runai 后端插件 ABI 承载 autumn 原生传输【WON'T-DO，判据实测未触发】
- **Trigger** (2026-09-01): `libstreamer.so`（未 strip）里存在一套可插拔后端 C ABI，按**裸 soname**
  `dlopen`，因此 `LD_LIBRARY_PATH` 即可接管，无需 patch 上游。反出的签名：
  `obj_open_backend(void**)` / `obj_close_backend(void*)` /
  `obj_create_client(void*, const ObjectClientConfig_t*, void**)` / `obj_remove_client(void*)` /
  `obj_remove_all_clients(void*)` /
  `obj_request_read(void*, const char* path, ObjectRange_t, char* dst, size_t)` /
  `obj_wait_for_completions(void*, ObjectCompletionEvent_t*, unsigned, unsigned*, ObjectWaitMode_t)` /
  `obj_cancel_all_reads(void*)` / `obj_get_backend_shutdown_policy()`。
  `obj_request_read` 收 `char* dst`，形状与 `read_into` 一致 ⇒ 可 RDMA 零拷贝直落 runai 的
  pinned buffer，同时覆盖 vLLM 与 SGLang。
- **Scope（若启动）**: 实现上述 ABI 的 `.so` + 配套的 `runai_model_streamer_s3` Python 替身
  （`s3_glob` / `pull_files` 在 Python 侧，不在 .so 里，**两个产物缺一不可**）。
- **已知代价（决定为何设为条件性）**:
  ① 插件名是三个 static const（`obj_plugin_s3_name` / `_gcs_` / `_azure_`），
     `get_libstreamers_plugin_type()` 硬编码三选一、无 env 覆盖 ⇒ 只能**冒充
     `libstreamers3.so`**，同进程内 autumn 与真 S3 二选一，且 URI 仍须写成 `s3://`；
  ② C ABI 无版本号、结构体布局需从上游头文件抄，上游改字段 ⇒ **内存损坏而非干净报错**。
- **启动判据（2026-09-01 修正后：未触发）**: 判据原文是"网关达原生 ~85% 则不做，掉到 60%
  以下才启动"。初测 runai→网关 = 45% 看似触发，但 MinIO 对照推翻了归因：**CRT 客户端本身
  不慢**（同机同文件从 MinIO 读 2.8 GB/s），慢的是本网关的单线程 accept。
  ⇒ **判据应在 F-S3-GW-MULTIWORKER 修完后重测**，在那之前本 feature **不启动**。
  用一个"冒充 libstreamers3.so + 无版本 ABI"的方案去补一个自家单线程造成的缺口，是错的修法。
- **Acceptance（若启动）**: vLLM 与 SGLang 均可经 `--load-format runai_streamer` 走 autumn 原生传输，
  字节精确；吞吐 ≥ 现有 `--load-format autumn` 原生 loader。
- **⛔ WON'T-DO (2026-09-01，判据已测且未触发)**: F-S3-GW-MULTIWORKER 修完后 runai→网关
  = native `read_into` 的 **98%**，远在"85% 以上则不做"这条线之上。原先看到的 45% 完全是
  自家单线程 accept 造成的，不是传输层损失。⇒ **不做**。用一个"冒充 `libstreamers3.so`
  + 无版本 C ABI + 还要再造一个 Python 替身包"的方案，去换那 2%，是明显的负收益。
  本条保留是因为 ABI 反解的结果本身有价值（万一将来需要真正绕开 HTTP 时可直接接手）。
- **Status**: `passes: false` (2026-09-01) — WON'T-DO，判据实测未触发。

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
