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
  70.2 s = **29,190 key/s**。**上面那条"负结果"由此被推翻**——当初测不出收益是因为扫描占了
  全程的绝大部分，删除侧再快也显示不出来；瓶颈换了以后同一个改动值 3.6×。教训：负结果必须
  连同"当时的瓶颈在哪"一起记。
  分段计时（`memory-mcp` 现在每次 wipe 打一行 `wipe breakdown`）：3883 页、**扫描 2.8 s、
  删除 67.8 s** —— 扫描从改动前占全程 ~88% 降到 4%，本 feature 认定的瓶颈已经移走并有数据佐证。
- **剩余 70 s 的归因：测了 4 组变量，全部无效，已另立 F-KV-CLIENT-30K**（别再在本条下猜）：
  分区 1→4（切点实测把数据分成 46/21/13/20%，落盘 63/34/25/71 MB，确实分开了）、
  删除并发 32→256、页大小 512→4096（页数 3883→486）、以及 1 分区/4 分区 × 32/256 并发的
  四格全跑过 —— 删除耗时始终 68~74 s。客户端只吃 16% 单核，盘的 fsync p50 56~60 µs、
  K=256 摊薄 237K ops/s，都不是瓶颈。而 ingest（走批量 put）也是同样的 ~30K key/s。
  ⚠️ 我一度写过"这是单分区写天花板、多分区就能解决"，**那是错的**，已被上面这组实验推翻；
  也不要反过来断言"多分区无用"——4 分区那轮先用了并发 32，两个实验各把对方的变量钉在了
  限制值上，补跑的 4×256 那格才是有效否证。

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
- **Status**: `passes: false` (2026-09-02) —— 只立账。这不阻塞任何东西：需要吞吐的
  消费者（perf-check / ycsb）本来就多线程，单进程 30K 只影响一次性批量作业的墙钟。

### F-FUSE-READ-GAP — fuse mount 读只有 autumnfs CLI 的一半
- **Trigger** (2026-09-03): 同集群（3 块真盘、EC 2+1）、同一个 4 GiB 未条带化文件、
  全部读到 `/dev/null`、direct-read 都开：
  | 路径 | direct=true | direct=false |
  |---|---|---|
  | autumnfs CLI (`cat`) | **1552~2053 MiB/s** | 769 |
  | fuse mount (`dd bs=8M`) | **936 MiB/s** | 498 |
  | 裸 KV（`perf-check` 1 线程 depth 8，8 MiB 值） | 2522 MB/s | — |
  | 本地 NVMe 单流 O_DIRECT bs=8M | 4512 MiB/s | — |
- **两条结论**:
  1. **CLI 已经接近裸 KV 的天花板**（2053 vs 2522），所以 fuse 那 936 是 fuse 自己丢的，
     不是集群丢的。
  2. **"8 MiB extent 太小"这个假设被否证**：同样 8 MiB、同样 depth 8，裸 KV 就有 2522 MB/s。
     extent 尺寸不是限制项（至少对 CLI 这条路不是）。
- **Scope**: 找 fuse 读路径丢掉的那 2×。已知嫌疑：单线程 dispatcher 上的
  内核回复拷贝、`read::execute` 的每请求粒度（内核按 `max_read` 下发，不是按 extent），
  以及每次 `read` 都重新 `prepare` 一遍 ChunkSpec。**先分段计时再动手**。
- **Acceptance**: fuse 单流读进到 CLI 的 80% 以内，且 `fuse_chaos.sh` 全绿。
- **Status**: `passes: false` (2026-09-03) — `fuse_chaos.sh` 全绿（78 文件，跑在默认的 4 线程池上；
  日志确认 `threads=4`），但单流那一半到不了，理由见下。
- **进展 (2026-09-03)**: **`fuser` 的 feature 从 `abi-7-12` 提到 `abi-7-28`，读 +81%**
  （交替 A/B：898 → 1621 MiB/s，三对全部同向）。根因：`FUSE_MAX_PAGES` 和 INIT 应答的
  `max_pages` 字段在 fuser 里都在 `#[cfg(feature = "abi-7-28")]` 后面，缺了它内核把**每个**
  请求夹在 32 页 = 128 KiB —— `ops.rs` 里 `set_max_write(1 MiB)` 从写下来那天起就是空转的
  （4 GiB 的写产生 32768 次 128 KiB 调用；开了之后是 4096 次 1 MiB）。1 MiB 是内核 256 页
  夹逼后的真实上限，设 4/8 MiB 请求数不变。
  ⚠️ **这次升级本身带进一个静默数据丢失**（fable 评审抓到，已修+守卫）：`FUSE_RENAME2`
  从 7-23 起会被解析并派发给同一个 `rename`，而我们忽略 flags、底下是 POSIX 覆盖语义。
  实测 `RENAME_EXCHANGE` **返回成功**却做单向 rename，目标内容直接没了。现在 `flags != 0`
  一律 EINVAL，守卫在 `scripts/fuse_chaos.sh` T3。教训：**抬 ABI 地板时要看的是"落到已实现
  方法上"的新 opcode，不是拿 ENOSYS 的那些**。
  **分段计时已加**（`fuse read breakdown`，每 1024 次读一行）。4 GiB 单流读 2.5 s 的拆分：
  prepare 8 ms / alloc 97 ms / **fill 1840 ms** / 约 550 ms 在这些之外。
- **五个旋钮全部实测无效，别再试**（每条都是真跑的，不是推断）：
  | 试的 | 结果 |
  |---|---|
  | 请求大小 | 内核 `max_pages` 夹在 256 页，1 MiB 已是上限（见 `ops.rs`） |
  | `max_background` 16→64→128（在途 FUSE 请求数） | 1658 → 1704 → 1743，+3~5%，噪声级，已回退 |
  | bdi `read_ahead_kb` 128→1024→8192→32768 | 1723 / 1722 / 1737 / 1721 —— **完全无效**（已确认设置生效） |
  | 守护进程 CPU | 22%、2 线程，**不是 CPU 绑定** |
  | `--direct-read` | 已经开着，关掉更慢（1621 vs 898） |
- **真因（已定量）**: 单流顺序读是**每请求延迟绑定**的 —— 1840 ms / 4096 次 = **449 µs 一次
  1 MiB 读**，1 MiB / 449 µs = 2230 MiB/s，和实测吻合。内核对一个**同步**读者不会并发下发
  FUSE 请求，而 CLI 单流之所以快，是它在用户态一次发 8 个整 extent（64 MiB 在途 vs 1 MiB）。
- **⚠️ 我一度记过"并行读线性扩展到 5341 MiB/s"——那是测坏的**：8 个 dd 用 `skip` 读同一个
  4 GiB 文件的不同段，skip ≥ 4 GiB 的那 4 个读到文件外、立刻返回 0 字节，我却按 8 GiB 算了。
  重测（8 个**独立** 1 GiB 文件、每轮核对字节数）：
  | 并发 | fuse mount | autumnfs CLI（同一批文件） |
  |---|---|---|
  | 1 | 1264 / 1525 | **3010** |
  | 4 | 2546 / 2654 | **5148** |
  | 8 | 2187 / 1961 | **5466** |
  **5 GB/s 是 CLI 的数，不是 mount 的。** mount 在 ~2600 到顶，8 流反而降到 ~2070。
- **真因（已定量，取代先前的"延迟绑定 + 预读"结论）**: **fuse 守护进程只有 2 个线程，
  compio 那一个被打满**。8 并发读时 daemon CPU = 90~110%（一个核），per-thread CPU
  jiffies = 内核通道读线程 **3** vs compio 线程 **394** —— 活全在 compio 上。
  这解释了全部三个点：单流 1400~1650 是线程没满时的延迟绑定；4 流 2600 是接近饱和；
  8 流 2070 是**饱和后加并发只剩争用**。CLI 能到 5466 是因为 8 个进程 = 8 个 compio 线程。
- **不是 TCP**：同一条 loopback、同一集群、同一批文件，CLI 跑到 5466 MiB/s。
- **⚠️ "守护进程侧预读是唯一杠杆"这条结论作废**：预读省的是延迟，而 4 流以上该线程已经
  CPU 饱和，预读只会让它更忙。
- **✅ 已做：读 I/O 线程池**（`crates/fuse/src/read_pool.rs`，`--read-io-threads`，默认 4）。
  N 个独立单线程 compio runtime，各自持 `ClusterClient`；`prepare` 仍独占派发线程上的
  `FsState`，只有 `execute` 跨线程（`ReadJob` 天然 Send —— `fuser::ReplyData` 因
  `ReplySender: Send + Sync` 而 Send）。round-robin 派发；派发不出去就把 job 还回来在本地跑。
  实测（8 个独立 1 GiB 文件，每轮核对字节数，两轮同向）：
  | `--read-io-threads` | p=1 | p=4 | p=8 |
  |---|---|---|---|
  | 0（旧） | 885 / 878 | 2006 / 2066 | 2211 / 2213 |
  | 2 | 882 / 805 | 2965 / 2445 | 3705 / 3081 |
  | 4（默认） | 710 / 739 | 2937 / 2776 | **4763 / 4488**（另一轮 5186~5396）|
  机制也验过（p=8 per-thread jiffies）：派发线程 435 → 7，总量 435/427/412 **几乎守恒**
  —— 同一份活摊开到多核，不是加开销侥幸变快。t=4 的 p=8 = CLI 5466 的 95%~99%。
- **验收状态：`passes: false`，而且这条验收线量的是错的东西**。"80% of CLI 单流" 到不了
  （mount p=1 ≈ 880 vs CLI 3010 = 29%），因为**单流是每请求延迟绑定**：内核对一个同步
  读者不并发下发 FUSE 请求（bdi `read_ahead_kb` 128→32768 实测完全无效），线程池按定义
  改不了它。**聚合吞吐已经追平 CLI**。要动单流只剩守护进程侧预读，而那要和 lease /
  `cached_version` 失效那套对齐，属于设计决策。验收口径按规则不改写，只记状态。
- **剩下的第二杠杆（未做）**：减少每字节拷贝 —— `execute` 先 `vec![0u8; n]`（分配+清零）
  再填，可以直接填进回复缓冲。当前每字节约 3 次（网络收进 pool → memcpy 到 Vec → 写回
  `/dev/fuse`）。
- **⚠️ UCX 下未测**：所有数字是 loopback TCP，且默认二进制不带 `ucx` feature；本机只有
  `lo` 有 IPv4（无 RoCE IP），跨机不可用，所以这条路本会话够不着。UCX worker 创建走宿主级
  devx 自旋锁 + fuse 在 UCX 上崩过 daemon，真跑 UCX mount 先拿 `--read-io-threads 0` 对照。
- **测量方法论（本条是踩出来的）**: 比较读吞吐时**两边都必须读到 `/dev/null`**。
  `autumnfs get <file>` 自带一次本地盘写，会把结果钉在 ~800 MiB/s 并**反转**结论。

### BUG-KVC-MM-ALIAS — 多模态请求取不到 mm hash 时仍然缓存（跨图片 KV 串读）
- **Trigger** (2026-07-22, coco deep inspect `vllm_connector.py:193`；**已复核代码为真**): `_request_extra_keys()` 已经识别出"有 `mm_features` 但取不到任何 mm hash"这一情况并打 warning，注释甚至写明 "its prefix hash would collide across DIFFERENT images sharing the same placeholder token ids (false-alias → wrong output)" —— 然后**照样返回不含 disambiguator 的 keys 并继续缓存**（"the connector still caches (best-effort)"）。VLM 场景下同尺寸不同图片的 placeholder token 序列可以完全相同 ⇒ 用户 B 可能读到用户 A 的视觉 KV：错误输出 + **跨请求信息泄漏**。
- **Scope**: `_request_extra_keys()` 改返回 `Optional[List[str]]`，无法区分多模态内容时返回 `None`；load 路径 `get_num_new_matched_tokens()` 见 `None` 直接 `return 0, False`，store 路径 `build_connector_meta()` 见 `None` 直接跳过保存（= 该请求不participate external KV，纯文本请求不受影响）。优先复用 vLLM 自身的 BlockHash / extra keys，而不是在 connector 里 best-effort 猜字段名。
- **Acceptance**: 单测 —— (a) 有 `mm_features.identifier` 时不同图片得到不同 hash；(b) `mm_features` 存在但字段取不到 hash 时**既不 load 也不 save**（当前会 save）；(c) 无多模态的纯文本请求行为不变。
- **Status**: `passes: false` (2026-07-22) — cross-ref memory `project_kvcache_vlm_mmhash_unverified`（"mm_hash 漏 key"曾是已修 bug，本条是它的残留 fail-open 面）。
  **用户定调 2026-07-22**（在 "fix BUG-KVC-TENANT" 之后说「剩下的 2 个你看着办，一般不需要」）: 本条**不做**，留在 backlog 只作记录。真要动之前，先复核它的触发条件是否已经在线上出现过。
  **复核结果 (2026-09-03)：门槛未过，维持不做。** 触发条件等价于"部署在用的 vLLM 不再通过
  connector 读的那两个名字暴露 mm hash"。实测本机 `vllm-env` = **vLLM 0.28.0**（也就是
  S3 网关真引擎验证用的那个版本），`MultiModalFeatureSpec` 的字段是
  `['data','modality','identifier','mm_position','mm_hash']` —— `identifier` 与 `mm_hash`
  **两个都在**，而 `_request_extra_keys` 第 177 行取的正是这两个（`identifier` or `mm_hash`）。
  所以 fail-open 那条分支在当前部署上够不着。fail-open 本体仍在（`vllm_connector.py` ~193：
  打 warning 然后照样返回不含 disambiguator 的 keys），代码没动。
  **下次复核只需重跑这一条**：`python -c "import dataclasses; from vllm.multimodal.inputs
  import MultiModalFeatureSpec as S; print([f.name for f in dataclasses.fields(S)])"` ——
  升级后这两个名字若消失或改名，门槛即刻成立。
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

### BUG-EC-CONVERT-STALL-HEALTHY-COORD — EC 转换在协调者健康的情况下永不完成，卡住该 extent 的 GC【已复现，未修】
- **Trigger** (2026-09-03, 给 F-SEALED-EMPTY-SWEEP 补 chaos 验收时撞上)：chaos verify 的
  in-flight 阶段报 `op ... kind=7 target=0/12 still ACTIVE (state=1) after quiesce —
  attempts=0 last_error=""; EC marker on extent 12 still pinned after quiesce (age 44s) with a
  HEALTHY coordinator (node N) — nothing is stopping this conversion from progressing, and the
  extent's GC is blocked until it drains`。chaos 自己的判词是 **"NO fail-loud marker in any EN log
  —— the invariant broke SILENTLY"**。
- **复现配方**（本机可复现，非推断）：
  `RUST_LOG=autumn_manager=info AUTUMN_CHAOS_SEED=603 AUTUMN_CHAOS_DURATION_SECS=45 \
   AUTUMN_CHAOS_NEMESIS_INTERVAL_MS=1500 cargo test -p autumn-manager --test system_chaos \
   chaos_real_kill_split_merge_ec_fence_no_data_loss -- --nocapture --ignored`
  **时序敏感**：同一 seed 不加 `RUST_LOG` 时曾通过两次，加上之后两次都失败（日志开销改变了时序）。
  所以判定它是否修好，必须**多次**跑带 `RUST_LOG` 的这一条，不能只跑一次。
- **归因已做一半**：同样的失败在 **sweep 关闭时逐字复现**（同 kind、同 extent、同措辞），
  所以与 F-SEALED-EMPTY-SWEEP 无关。**未做**：没有单独对照今天的重放修复（84545c9）之前的版本，
  尽管机制上不相干（重放只改 `recover_partition` 的窗口，这里卡的是 EC 转换派发）。
  cross-ref memory `project_ec_frozen_owner_epoch_wedge`（曾修的 owner_epoch 永久 fence 两半）与
  `project_chaos_coverage_gaps_20260619`（"EC apply-fail 吞→wedge" 早被列为覆盖缺口）。
- **Scope（复现之后）**: 查 attempts=0 且 last_error 为空的 ACTIVE EC op 为何不再被派发——
  重点看 `ec_conversion_dispatch_loop` 的候选过滤与 ledger attach 语义（历史上"第二次提交发生在
  第一次转换已关闭之后 ⇒ 不 attach ⇒ 新建条目"是同族的坑）。
- **Acceptance**: 上面那条复现配方连跑 5 次全绿（in-flight verify errors=0）；且能说明是哪一处
  让它停止派发的（不是"加了重试就好了"）。
- **Status**: `passes: false` (2026-09-03) — 已复现，未修，未归因到具体代码点。

### F-SEALED-EMPTY-SWEEP — manager backstop sweep for leaked sealed-empty non-tail stream members【代码已就绪，未上线】
- **Trigger** (2026-07-14, BUG-FLUSH-TIMEOUT-LEAK follow-up): 线上 5 节点 wedge 泄漏 10.4 TB / 222 GB
  逻辑数据（47×）。客户端侧已修两处（size-scaled append deadline、roll-away 时
  `reclaim_abandoned_empty_tail` best-effort punch），但那是客户端且 best-effort：punch 或权威 re-fetch
  失败、或 writer 在 seal 与 punch 之间死掉，那个 extent 就永远泄漏（`sealed=true, sealed_length=0`、
  refs≥1、非尾巴的 stream 成员、无人引用、GC/truncate/orphan-reconcile 各自因"它没有那个字段"全部跳过）。
  修复前被污染的集群另有约 4 万个这种 extent，客户端修复永远不会再访问它们。
- **Scope**: leader-only manager sweep，谓词 = 非尾巴 + `sealed && sealed_length == 0` + 不在 inflight
  ledger；复用 punch-holes 的变更路径（`compute_extent_ref_drops` → value-CAS 的
  `mirror_stream_extent_mutation` → pending-delete 队列）。限速 N extent/tick。
- **Acceptance**: 单测 —— sealed-empty 非尾巴要扫、sealed-empty **尾巴**与 sealed 非空**不扫**、
  inflight 的推迟；集成 —— 在 seal 与 punch 之间杀掉 writer（或让 punch 失败一次），断言 sweep 在一个
  周期内回收；chaos 回归（seed 603 + 769351064 类）绿。
- **Status**: `passes: false` (2026-07-14；2026-09-03 更新) — **实现完成、测试通过、评审过，但故意不上线**。
  代码在 `crates/manager/src/extent_delete.rs`（`sealed_empty_sweep_candidates` 纯谓词 + 6 单测；
  `sealed_empty_sweep_once`/`_loop`），`start_runtime_tasks` 里**没有** spawn，附了不 spawn 的理由；
  三条谓词消融各自咬住不同断言。曾阻塞在重放游标类上（现已修）：manager 看不见 PS 的
  vp_head，所以 sweep 无法过滤掉"被 checkpoint 游标指着的空 extent"，而那一类会静默丢失已 ACK 的写。
  **2026-09-03：已上线（spawn 了）。** 阻塞解除（BUG-EMPTY-VP-CURSOR-PUNCH 已复现并修复：
  恢复侧游标全解析不到时改为全量重放，所以回收游标指向的空 extent 现在的代价是"开得慢"而非丢数据）。
  验收里"杀 writer 在 seal 与 punch 之间"的集成半**已做**：`crates/manager/tests/sealed_empty_sweep_etcd.rs`
  用真 etcd + aux 客户端直查，消融掉持久化那步会精确变红（`etcd still holds extents/N`）。
  **chaos 那半仍不算数,而且理由要记住**：实测 grep 两轮 chaos 日志，`sealed-empty sweep` 出现
  **0 次** —— chaos 从没造出这个形态，所以它只证明"开着这个循环不打扰既有不变量"
  （同 seed 在 sweep 开/关下结果逐字相同），**不证明 sweep 本身在 chaos 下正确**。
  要真覆盖，得在 harness 里显式造一个 sealed-empty 非尾巴成员再跑。
  另注：seed 603 目前**本来就红**（见 BUG-EC-CONVERT-STALL-HEALTHY-COORD），与本条无关。
  旧文（仍适用）：验收里"杀 writer / punch 失败"的集成半与 chaos 半**仍未做**：`AutumnManager::new()` 没有 etcd，
  `mirror_stream_extent_mutation` 在内存模式下是**彻底 no-op**，所以现有两条集成测试覆盖的是选择逻辑与
  内存态 apply，**不是** plan 基线与 txn 之间的 CAS 耦合；那需要 etcd 后端的集群。
  另：inflight ledger 的"计划后、mutation 前"窗口已改成每个 plan 重读一次，但该重检查**没有测试覆盖**
  （计划期谓词已排除 inflight，内存模式下那些 await 不挂起，进程内开不出窗口），代码里已如实标注。

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

### F-IMG-FUSE-MCP — 把 autumn-fuse + memory-mcp 打进容器镜像
- **Trigger** (2026-09-01, FreeToken rollout 规划时发现): 镜像只 build `-p autumn-server -p autumn-dashboard`，`autumn-fuse`（挂载守护进程）和 `memory-mcp` 都不在里面。前者导致"从 autumn fuse 读模型权重"在 k8s 上无法实现（`docs/ops.md` 只有一句"应做成 privileged DaemonSet"的说明，仓库里没有任何 FUSE manifest）；后者导致 hermes 无法以 stdio 子进程方式接 MCP —— 而 stdio 是 `memory-mcp` 唯一的传输方式，`docs/autumn_memory_plan.md:378` 明确不建议常驻 HTTP/SSE MCP。
- **Scope**: `deploy/docker/Dockerfile` builder 段加 `libfuse3-dev`+`pkg-config`（fuser 0.15 链接 libfuse3），构建改 `-p autumn-server -p autumn-dashboard -p autumn-fuse -p memory-mcp`；runtime 段加 `fuse3`（libfuse3 + fusermount3 setuid helper）并拷出两个二进制。`entrypoint.sh` 加 `fuse` role（env→flag：`AUTUMN_MANAGER`/`AUTUMN_FUSE_MOUNTPOINT`/`AUTUMN_CREDENTIAL_FILE`/`AUTUMN_FUSE_DIRECT_READ`/`AUTUMN_FUSE_ALLOW_OTHER`），挂载前 `fusermount3 -u` 清理崩溃残留。`docs/ops.md` 补 sidecar manifest + mountPropagation 配对 + FOPEN_DIRECT_IO 的 mmap 后果。
- **Acceptance**: (a) `deploy/validate.sh` 通过且 role 分派表含 `fuse`；(b) Linux 上镜像构建成功、`autumn-fuse --help` / `memory-mcp --help` 在镜像里可执行；(c) sidecar 挂载后主容器能在 `/mnt/autumn` 看到 `fs/` 内容（mountPropagation 配对生效）；(d) 杀掉 fuse 容器再拉起，不因残留挂载点 EBUSY。
- **Status**: `passes: false` (2026-09-01；2026-09-02 收窄) — (a) 通过；(b) 镜像里有 `autumn-fuse` 且在 v29 集群上真挂载成功（`dd iflag=direct` 4K/8M 两档都过），但 `memory-mcp --help` 未在镜像里单独跑过。**(c) 的形态已被推翻**：本集群 kubelet 未配 rshared，mountPropagation 不兑现（特权 sidecar 内 `grep -c "shared:" /proc/self/mountinfo` = 0），sidecar 主容器看不到挂载；可行形态是**单容器**（autumn-fuse 后台进程与应用同 mount namespace，整容器 privileged）。⇒ 本条剩余工作 = `docs/ops.md` 的 sidecar manifest 改写成单容器形态 + (b) 的 memory-mcp 检查 + (d) 杀容器重拉不因残留挂载点 EBUSY。
  **2026-09-03 进展**：文档那半**已做** —— `docs/ops.md` 现在把单容器形态放在前面并给了完整
  manifest，sidecar 降为可选并在前面加了节点前置检查（特权 pod 内
  `grep -c "shared:" /proc/self/mountinfo`，本集群返回 0 = 该形态在此不兑现）；单容器 manifest
  的就绪探测用 `/proc/mounts` 而非 `mountpoint -q`（后者对尸体挂载会永久阻塞，是本仓库已知坑）。
  **(b) 与 (d) 仍未做，且本机做不了**：开发机上没有 docker/podman/nerdctl，两条都要构建主机
  或集群。下次在有容器运行时的机器上补：`docker run --rm <img> memory-mcp --help`，以及杀掉
  fuse 容器再拉起验证不因残留挂载点 EBUSY（docs/ops.md 里已有那份 acceptance 脚本）。

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
