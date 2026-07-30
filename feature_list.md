# autumn-rs feature list — OPEN backlog

**Last updated:** 2026-07-30

**Rules:**
- This file tracks the **OPEN backlog only**. A feature that reaches `passes: true`
  is **DELETED** from here — git history is the record, there is no archive file
  (CLAUDE.md rule 13: 定期清理删除，保持整洁).
- `passes` and `notes` are the only mutable fields after a feature is created.
- Out-of-scope / "v2 再做" decisions must be recorded as proper feature entries
  (F-name + Trigger + Scope + Acceptance + `passes: false`), never as plan-file footnotes.

---

## Active

### F-WIRE-CRC-UNIFY — 统一帧结构：header+ctrl 必有 CRC、bulk value 交给传输层、FLAG_CRC 位退役
- **Trigger** (2026-07-29, 用户审阅 CRC 全景表后拍板「CRC 在 wire 上就很乱，改为 header+meta 必有 CRC、大 value 交给传输层」+「不需要 FLAG_CRC 了，默认都有 meta crc」): 现状三档不一致
  —— 普通帧 CRC 只盖 payload 不盖 header（req_id 翻转 = 响应静默投递给错误 caller 且 CRC 仍通过；FLAG_CRC 位被翻掉 = 验证被静默关闭）；ZC value 响应完全无保护；`MSG_PUT_ZC` 发送侧仍全量扫 value 算 CRC（F219 残留）而 PS 快路径只消费不验证。且 ZC meta codec (`encode_zc_meta`) 住在 `rpc/src/client.rs`，不在 WIRE 指纹内 —— 改布局不会触发版本决策。
- **Scope（设计已钉死）**: 唯一一种帧形状，无标志位区分：
  `[req_id:4][msg_type:1][flags:1][payload_len:4] [ctrl_len:4][ctrl…][crc32c:4][value…]`
  crc32c 覆盖 header ++ ctrl_len ++ ctrl；value 裸（传输层完整性）；value_len = payload_len−4−ctrl_len−4；flags 只剩 RESPONSE/ERROR/STREAM_END（FLAG_CRC 位退役保留）。per-msg_type 的 ctrl/value 切分：
  * 普通 rkyv 帧 / 错误帧: ctrl = 整个 body，value 空（每帧 wire +4B）。
  * `MSG_GET_ZC` / `MSG_READ_BYTES_ZC` 响应: ctrl = `[code:1][message…]`（旧 9B zc_meta 死亡；ZC 错误终于有 message），value = 裸值。
  * `MSG_PUT_ZC` 请求: ctrl = `[44B put_meta][key]`，value = 裸值 —— **发送侧全量 value CRC pass 消灭**；PS `drain_zc_writes` 改为验 ctrl-crc（廉价）+ 无 trailer 消费。
  * **`MSG_APPEND` 是唯一例外（用户拍板 payload 需保护）**: ctrl = `[29B meta][payload]`，value 空 —— 零新 msg_type，F165 的 control 字段保护 + payload 在途完整性全保留。不做 EN 收侧 recv-into-pooled（批处理架构 + 4KiB 主导负载 + 保 CRC 后反正要全量扫；未来 profiling 说话再议）。
  * codec 迁入 `frame.rs`（纳入 WIRE 指纹）；`encode_no_crc`/`ps_zc_head`/`zc_read_head` 手搓头统一为 frame.rs helper；`call_into_pooled` 返回类型携带 message。
  * WIRE 27→28（MIN=MAX），指纹重 pin。混部失败模式变化要写文档：帧层变更使 GetClusterId 协商通道对旧 binary 也解不开 —— 混部第一帧 CRC mismatch 响亮断连（劣于优雅拒绝、优于 rkyv 静默乱码；same-commit 部署策略下无实际影响）。
- **Acceptance**: 单测 —— header 任一字节翻转 → CrcMismatch（普通帧 + ZC 帧都验）；ZC 错误响应 message 端到端可读；put_zc 大/小 value 双路径字节精确；`registry_pins_current_schema_to_max_version` 过（v28 pin）。集成 —— rpc/client/PS --lib/stream 全绿 + manager f235（--ignored 真集群）绿 + e2e put/get/append 冒烟。性能 —— put_zc 8MiB 发送侧少一趟全量 crc（perf-check 或微基准佐证方向即可）。
- **Status**: `passes: false` (2026-07-29, **实现完成、验收基本达成，仅欠 coco 评审**——Trae
  token 过期待用户重登)。
  **Notes (2026-07-30, 术语定版)**: 全部 zc 词汇改名 **bulk**（用户裁定: zc 是 wire 结构却拿
  效果命名——拷贝数由源内存出身×传输层决定，TCP 下 put 照样有内核拷贝；调研 brpc attachment /
  Ceph data segment / NVMe in-capsule / TOAST out-of-line 后定 bulk）。腾词: P-bulk flush 线程
  改名 **P-sst**（更准——它就是 SST build+upload 线程），env `AUTUMN_PS_BULK_INFLIGHT_CAP` →
  `AUTUMN_PS_SST_INFLIGHT_CAP`（entrypoint.sh/autumn-deploy/ops.md 已同步）。CLI 动词
  put-zc/zc-get → put-bulk/bulk-get；python `BatchClient.zc()` → `.bulk()`；bench_zc.py →
  bench_bulk.py。wire 字节零变化，v28 指纹就地二刷 `7e04e6c6cbf5a759`。已落地: frame.rs 全重写（统一帧 + peek_zc_prologue/
  consume_zc_prologue + encode_zc_response_head/encode_vectored_head/compute_ctrl_crc +
  Malformed 错误变体）；rpc client（call_vectored_zc 新增、call_into_pooled 返回
  ZcResp{buf,code,message}、read_loop 快路径先验 crc 再 recv）；PS（ps_zc_head 瘦包装、
  drain_zc_writes 验 ctrl-crc + 无 trailer、frame.value 经 zc_value 线到 enqueue_put_zc、
  authz 零改动——parse_put_zc_meta 在 ctrl 上原样工作）；EN（zc_read_head 瘦包装 + 全部
  错误点带 message）；SDK（put_zc→call_vectored_zc、get_range_into 消费 message）。
  验收: header/ctrl 任意字节翻转→CrcMismatch（新单测）、ZC error message 端到端（新单测
  ×2 + rpc 54 全绿）、put_zc 大/小值 + get_zc 混合大小真集群 e2e（system_putstream 7/7
  绿）、v28 pin `db105c702b8ff770` 过、transport 9 / client --lib 40 / PS --lib 204 /
  stream 114 全绿；layer_a 与 system_chaos 的 2 处失败经 stash 对照确认为预存在（admin
  token 环境 + 缺 force 字段），与本 feature 无关。ops.md 已加 v28 混部失败模式说明。
  put_zc 发送侧 crc pass 消灭为结构性保证（value 不再进 compute_ctrl_crc）。

### BUG-KVC-LOAD-ATOMIC — external KV load 是 fail-open：部分 layer 加载失败仍继续推理
- **Trigger** (2026-07-22, coco deep inspect `vllm_connector.py:773`；**已复核代码为真**): scheduler 见 `__present__` marker 后就告诉 vLLM 这些 token 不用再 prefill。worker 侧 `start_load_kv()` 拿 `oks = load_layers(...)` 后是**边检查边注入**：`if not ok: log.warning(); continue`。于是任一 layer 缺失时，前面的 layer 已写进 paged cache、失败的 layer 保持**未初始化**内容，请求进入"部分新 KV + 部分旧/未初始化 KV"的混合态且无法回滚，继续推理 → 静默错误输出。**这不是假想**：BUG-KVC-TENANT 那次线上事故的表现就正是 `external KV load miss after positive presence`（layer 0..3）+ garbage output。
- **现状是有意为之，不是疏忽**: 代码注释明写"The position keeps its (uninitialised) paged KV; surface it loudly rather than swallowing at debug" —— 当时的选择是**响亮告警但继续服务**。本条要重新定的是这个姿态：正确性优先 ⇒ 应 fail-closed。
- **Scope**: `start_load_kv()` 改 all-or-nothing —— 先 `len(oks) == len(layer_names) and all(oks)` 再注入任何 layer；任一失败则一个都不注入并走 vLLM 的 external-load-failure 回退（若该 vLLM 版本支持 `get_block_ids_with_load_errors()` 之类上报路径就接上，否则至少保证该请求退回正常 prefill 而不是带着半截 KV 跑）。可选加固：marker value 带 `num_layers`/每层 byte length，加载前先校验。
- **Acceptance**: 单测 —— 构造 `load_layers` 返回 `[True, False, True]`，断言**零** layer 被注入且该请求走回退；线上/离线注入一次故意的 layer 缺失，确认输出不再是 garbage 而是正常（重算）结果。
- **Status**: `passes: false` (2026-07-22, coco 发现 + 主 agent 复核) — cross-ref `BUG-KVC-TENANT`（那次事故的下游放大器就是本条的 fail-open）。
  **用户定调 2026-07-22**（在 "fix BUG-KVC-TENANT" 之后说「剩下的 2 个你看着办，一般不需要」）: 本条**不做**，留在 backlog 只作记录。真要动之前，先复核它的触发条件是否已经在线上出现过。
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

### F-DIRECT-MANY — batched client direct-read (fuse / kvcache / fsspec opt-in)
- **Trigger**: user 2026-07-07, continuing the "autumn-fuse 是否该默认 --direct-read" discussion. F259 `get_direct` only bypasses the PS for a SINGLE whole-value read; fuse/kvcache/fsspec read in BATCHES via `get_many_into` (PS-proxied `MSG_GET_ZC`), so large-file (model) serving never took the PS off the data path. User asked for a batched direct-read shared by all three frontends, and set two constraints: (1) direct-read is TOPOLOGY-dependent (needs the client to reach EN data ports) so each frontend needs its OWN opt-in flag, default OFF — never a hardcoded SDK default; (2) it must handle mixed-size batches (some values large, some small) correctly.
- **Scope**:
  - **PS sub-range redirect** (`partition-server/src/rpc_handlers.rs::get_value_inner`): `MSG_GET_REDIRECT` was whole-value-only (`req.offset==0 && req.length==0`); fuse whole-extent reads carry an explicit `length` so they never redirected. Now redirects any VP sub-range whose CLAMPED requested length `r_len = (length==0 ? vp.len-off : min(length, vp.len-off))` is ≥ 64 KiB, returning `value_offset = vp.offset + req.offset`, `value_len = r_len`. Single-key `get_direct` (0,0) is the `r_off==0, r_len==vp.len` special case — byte-identical. Sub-ranges past the value end (`req.offset > vp.len`) and sub-64 KiB requests fall through to the inline proxy resolve unchanged. No wire-struct change (only PS handler logic) → NO WIRE bump.
  - **Client `get_many_direct`** (`client/src/lib.rs`): dest-based batched direct-read mirroring `get_many_into`. Per item, `read_len ≥ 64 KiB` → `MSG_GET_REDIRECT` → descriptor → EN direct read (`read_extent_value_direct`, pooled `Bytes`) copied into `dest`; inline (`extent_id==0`, PS declined) copied straight in; ALL-replica failure → proxy `get_range_into` fallback. `read_len < 64 KiB` → plain proxy `get_range` + copy (mixed-size batches route per item). Extracted `read_redirect_replicas` (shared with `get_direct`) + `get_range_direct_into` (per-item core). The one extra copy vs `get_many_into`'s recv-into-dest is deliberate: the direct read carries a 3 s timeout + replica failover, which `call_into_dest`'s cancel-safety contract forbids, so it uses the pooled recv (`call_into_pooled`) then memcpys — the price of failover-safety on the bypass path.
  - **Frontend flags (now DEFAULT ON everywhere — 2026-07-09 user directive; topology-dependent, size-gated, fallback+warn make it safe)**: fuse `--direct-read` (default `true`; `--direct-read false` to disable) → `FsState.direct_read` → threaded into each `ReadPlan` → `read::execute` picks `get_many_direct` vs `get_many_into`; python `BatchClient(direct=True)` (PyO3 default stays `false`, but every consumer passes `True`) → Get arm chooses the primitive; `autumn.Fs.connect(..., direct_read=True)` → `FsState.direct_read` (fsspec `AutumnFileSystem(direct_read=True)`); kvcache `AutumnKVConnector` (`kv_connector_extra_config.direct_read`, default True → `_AutumnKVStore` → `BatchClient(direct=…)`); `autumn_vllm_loader` (`model_loader_extra_config.direct_read`, default True). **Requirement 2 (size-gated) already holds**: `get_many_direct` routes per item via `zc_worthwhile` — only ≥ 64 KiB reads issue `MSG_GET_REDIRECT`, < 64 KiB stay on the proxy. Safe even when ENs are unreachable — each item falls back to the PS proxy (authoritative); the shared client (`read_redirect_replicas`) logs ONE `WARN` (`DIRECT_FALLBACK_WARNED`) on first fallback so a wrong topology surfaces without per-read spam.
- **Acceptance**: build + clippy green (autumn-client 0 warnings, autumn-partition-server 187 `--lib` tests green); fuse `core` feature builds (the fuse binary + python wheel are Linux/nightly-only build envs — `main.rs`/PyO3 changes are trivial clap/`#[pyo3(signature)]` threading). Manual verification in `docs/ops.md` (fuse `--direct-read` mount: reads return byte-identical; cross-host large-file read offloads PS NIC egress). LIVE cluster perf verification (cross-host PS-egress win, mirroring F259's 145→46 ms loopback latency figure) DEFERRED to a cluster run.
- **Status**: `passes: not_completed` — code complete + builds/clippy/PS-tests green. coco findbugs DONE 2026-07-07 (GPT-5.5, deep, on `HEAD~1..HEAD`) → **未发现问题** (0 findings): it traced the sub-range offset/length clamp vs `get_direct`'s whole-value contract, FUSE's explicit-range reads, `BatchClient` whole-value reads, and the EN direct short-read/all-replica-fail → PS-proxy fallback; the two things it weighed (`vp.offset + r_off` u64 overflow — identical to the existing `resolve_value`, benign; small-dest/large-value truncation — pre-existing documented behavior) were cleared. (The coco-review.sh wrapper itself needed a fix first — traecli 0.200.16 repurposed the old headless `-p` flag to `-p/--profile`; corrected to the `coco exec` subcommand, verified working.) STILL not_completed: the fuse binary + python wheel are Linux/nightly-only build envs (unbuilt locally — trivial flag threading only), and the cross-host functional + PS-egress perf verification is DEFERRED to a real cluster run.
  **2026-07-22 audit** — 两条残留里一条已清、一条部分清:
  - ✅ **fuse 二进制本地构建绿**：`cargo build --release -p autumn-fuse` 通过（本机就是 Linux；`main.rs:180` 的 `--direct-read` 线接完整）。PyO3 侧 `autumn.Fs.connect(..., direct_read=...)` 在 `python/src/fs.rs:144`。wheel 本地仍编不出（系统 Python 3.14 > pyo3 max 3.13，环境问题非代码）。
  - ⚠️ **跨机功能路径已被线上跑过**：VKE 2026-07-20 vLLM autumn loader（`loader.py:153` `direct_read` 默认 True）流式读 39.56 GiB / 22.8 s ≈ 1.78 GiB/s，字节正确（72B 正常出答案）→ 直读路径跨机功能 OK。**仍缺**：`--direct-read on/off` 的 A/B **PS-egress 卸载量测**（F259 那种 145→46 ms 的对照数字），这才是本条 Acceptance 里 DEFERRED 的那项。

### F-FS-WRITE-STRIPE — fuse MOUNT write-path large-file striping (B2 of F-FS-STRIPE)
- **Trigger** (2026-07-19, 从已完成的 F-FS-STRIPE 抽出剩余项): F-FS-STRIPE 的 autumnfs 路径已 done（write/read/rm striped、字节精确、突破单分区、连续 pipeline），但 **fuse MOUNT 的写路径 striping 延后**——现状 fuse 读/删 striped 正确（stripe-aware），但 mount 内的 write/truncate 对 striped 文件是 fail-loud 拒绝（不是自己写 striped）。
- **Scope**: 让 fuse mount 的写路径（`crates/fuse` write/extent）也能对大文件按 lane 分裂写（stripe key `[0x03][lane][ino][off]`），复用 autumnfs 已验证的 auto-detect lane 逻辑；truncate/删 striped 已支持。streaming 写（不像 autumnfs 上传时已知文件大小）要在写超过 STRIPE_THRESHOLD 时决定 stripe 布局并写 `InodeMeta.stripe`。
- **Acceptance**: fuse mount 写一个 >64 MiB 文件 → 落成 N-lane striped；autumnfs / 另一 mount cold-read 回字节精确；小文件（≤阈值）仍单分区；与 autumnfs 互通。
- **Status**: `passes: false` (2026-07-19, 从 F-FS-STRIPE 完成条目抽出) — 独立 deferred；破 ~450 单机聚合墙需真跨机集群（非代码），MAX_EXTENT 最优在满资源硬件另测（本 rig：≤8 MiB 越小越好）。cross-ref F-FS-STRIPE（autumnfs 路径已 done，git 历史）。
- **⛔ WON'T-DO (2026-07-22, 用户拍板「Fuse mount 可以不做 lanes」)**: 条带写是 **autumnfs 独有能力**，fuse mount 不实现。mount 现有行为保持：对条带文件的 write/truncate **fail-loud 拒绝**，读/rm 仍 stripe-aware 正确。理由：条带针对的是大文件灌入（模型权重），那条路径本来就走 autumnfs；mount 的价值是 POSIX 兼容而非峰值吞吐。注意 F-FS-GEOM-DECLARED 删掉 64 MiB 阈值后本条**已无技术障碍**（lane 函数是增量的，不需要预先知道大小）—— 这是产品取舍，不是做不了。要复活的话工作量是机械的。