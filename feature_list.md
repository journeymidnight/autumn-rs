# autumn-rs feature list — OPEN backlog

**Last updated:** 2026-08-05

**Rules:**
- This file tracks the **OPEN backlog only**. A feature that reaches `passes: true`
  is **DELETED** from here — git history is the record, there is no archive file
  (CLAUDE.md rule 13: 定期清理删除，保持整洁).
- `passes` and `notes` are the only mutable fields after a feature is created.
- Out-of-scope / "v2 再做" decisions must be recorded as proper feature entries
  (F-name + Trigger + Scope + Acceptance + `passes: false`), never as plan-file footnotes.

---

## Active

### BUG-KVC-LOAD-ATOMIC — external KV load 是 fail-open：部分 layer 加载失败仍继续推理
- **Trigger** (2026-07-22, coco deep inspect `vllm_connector.py:773`；**已复核代码为真**): scheduler 见 `__present__` marker 后就告诉 vLLM 这些 token 不用再 prefill。worker 侧 `start_load_kv()` 拿 `oks = load_layers(...)` 后是**边检查边注入**：`if not ok: log.warning(); continue`。于是任一 layer 缺失时，前面的 layer 已写进 paged cache、失败的 layer 保持**未初始化**内容，请求进入"部分新 KV + 部分旧/未初始化 KV"的混合态且无法回滚，继续推理 → 静默错误输出。**这不是假想**：BUG-KVC-TENANT 那次线上事故的表现就正是 `external KV load miss after positive presence`（layer 0..3）+ garbage output。
- **现状是有意为之，不是疏忽**: 代码注释明写"The position keeps its (uninitialised) paged KV; surface it loudly rather than swallowing at debug" —— 当时的选择是**响亮告警但继续服务**。本条要重新定的是这个姿态：正确性优先 ⇒ 应 fail-closed。
- **Scope**: `start_load_kv()` 改 all-or-nothing —— 先 `len(oks) == len(layer_names) and all(oks)` 再注入任何 layer；任一失败则一个都不注入并走 vLLM 的 external-load-failure 回退（若该 vLLM 版本支持 `get_block_ids_with_load_errors()` 之类上报路径就接上，否则至少保证该请求退回正常 prefill 而不是带着半截 KV 跑）。可选加固：marker value 带 `num_layers`/每层 byte length，加载前先校验。
- **Acceptance**: 单测 —— 构造 `load_layers` 返回 `[True, False, True]`，断言**零** layer 被注入且该请求走回退；线上/离线注入一次故意的 layer 缺失，确认输出不再是 garbage 而是正常（重算）结果。
- **Status**: `passes: true` (2026-08-11) — 曾 `passes: false` (2026-07-22, coco 发现 + 主 agent 复核) — cross-ref `BUG-KVC-TENANT`（那次事故的下游放大器就是本条的 fail-open）。
  **用户定调 2026-07-22**（在 "fix BUG-KVC-TENANT" 之后说「剩下的 2 个你看着办，一般不需要」）: 本条**不做**，留在 backlog 只作记录。真要动之前，先复核它的触发条件是否已经在线上出现过。
  **2026-08-11 触发条件已在线上复核出现 + 用户明确要求修复**: 活集群 7B 对 hermes 固定长 prefix 输出满屏 garbage，vllm 日志每层刷 `external KV load miss after positive presence`。按既定 Scope 修复：`start_load_kv` 改 all-or-nothing（`len(oks)==len(layer_names) and all(oks)` 才注入，否则一层不注入）+ 记录失败请求的 block_ids，新增 override `get_block_ids_with_load_errors()` 上报给 vLLM 走重算回退。单测 `tests/test_load_atomic.py`（[True,False,True]→零注入+上报 blocks；短 oks→fail-closed；全 True→注入全部）。同批把 tenant key 改简洁+有效：`tenant_cfg_from_vllm` 用 autumn weights-path basename（`qwen7b`）替代恒定无效的 `/model-cfg` 段（`_identity.py`），key 从 `model-cfg_<fp>_...` 变 `qwen7b_<fp>_...`；`tests/test_tenant_identity.py` 同步。
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
- **Status**: `passes: false` (2026-09-01) — 本地已改完并通过 `bash -n` + `deploy/validate.sh` + `cargo build -p memory-mcp` + `cargo check -p autumn-fuse`；(b)(c)(d) 需 Linux 镜像构建与活集群验证。

### F-FT-FUSE-ODIRECT — 确认 FreeToken 能否从 autumn-fuse 挂载点加载权重【先决条件】
- **Trigger** (2026-09-01): FreeToken 的 FTW reader 开局探测一次 O_DIRECT（`checkpoint/ftw.py`），成功走 8MiB chunk 多线程 `preadv`，**失败则回退到整片 `mmap.mmap(fd, 0, prot=PROT_READ)`**。Python `mmap.mmap` 不传 `flags` 默认 **MAP_SHARED**，而 `autumn-fuse` 对每个 open 无条件返回 **FOPEN_DIRECT_IO**（`crates/fuse/src/ops.rs`），内核对 direct_io 的 FUSE 文件拒绝 MAP_SHARED（`ENODEV`）。⇒ 若 O_DIRECT 探测失败，**两条加载路径同时死**。另：raw-safetensors 的 `--expert-load parallel` 用无回退的 `O_RDONLY|O_DIRECT` 且只捕获 `NotImplementedError`，在不支持的挂载上会直接崩。
- **Scope**: 在挂了 autumn-fuse 的 Linux pod 里对挂载点上一个 ≥4096 字节的文件执行 `os.open(p, os.O_RDONLY|os.O_DIRECT)`。若失败，给 `autumn-fuse` 加 `--direct-io <bool>` 开关（`ops.rs` 目前硬编码 `reply.opened(fh, 1)`），让 mmap 回退路径可用；或改为本地盘暂存权重。
- **Acceptance**: 探测结果被记录到 `docs/ops.md`；FreeToken 能从挂载点完成一次权重加载，且日志中未出现 `O_DIRECT unsupported ... using mmap fallback` 之后的 ENODEV 失败。
- **Status**: `passes: true` (2026-09-01) — 活集群实测通过。3.2 节点、v29 镜像、autumn-fuse sidecar 挂 `fs/`（authz 开，带 fs.cred）：buffered 读 OK；`dd iflag=direct bs=4096` **OK**；`bs=8M iflag=direct`（= FTW 的分块尺寸）**OK**。⇒ FTW reader 的 O_DIRECT 探测会成功、走多线程 `preadv` 快路，那条会因 MAP_SHARED 撞 ENODEV 的整片 mmap 回退不会被触发。同轮顺带验证 `autumnfs put` 12 MiB → 自动 striped ×24 lanes → FUSE 挂载读回，链路通。仍保留的约束：**FUSE 上禁用 `--expert-load parallel`**（它用无回退的 O_DIRECT 且只捕获 NotImplementedError）。

### BUG-PRESPLIT-NO-CRED — `autumn-op presplit --namespace fs` 在开了 authz 的集群上无法执行
- **Trigger** (2026-09-01, 线上 v29 全量重建时实测): 新集群 bootstrap 后执行
  `autumn-op --admin-token-file F presplit --namespace fs --lanes 24 --parts 6`，报
  `presplit: read existing stripe geometry: permission denied: protected key requires a
  capability token (no AUTH_HELLO on this connection)`。
  根因：`main.rs:2076-2088` 的"禁止静默收窄 lane 宽度"守卫（注释标为 UX-fix M5，较新迁入）
  会做一次**裸数据面读** `client.get(b"fs/" ++ stripe_geom_key())`，但 autumn-op 走的是
  `connect_raw`（无 AUTH_HELLO），而 PS 端是 PROTECT-EVERYTHING
  （`crates/partition-server/src/authz.rs:221-226`：开了 authz 就每个 key 都要 token，
  `protected_prefixes` 列表已退休）。而 **`presplit` 子命令没有任何 `--credential-file` 参数**
  —— 全仓只有 `mint-token` 接受它（`args.rs:813`）。admin token 是控制面凭据，不满足数据面检查。
- **为什么以前能跑**: 那个 read-before-write 守卫是后加的；在它出现之前 presplit 不碰数据面，
  所以 2026-07-20 那次 reset 的 `presplit --namespace fs --lanes 6` 能成功。属回归。
- **影响**: 开了 authz 的集群**无法给 fs 做 lane presplit**。而 presplit 必须在写入任何数据
  之前做（数据落盘后 CoW 重叠会让多数切点 `has_overlap` 失败），所以这挡住的是新集群 bring-up
  的关键一步 —— 只能退回单分区，大文件上传/读取拿不到跨分区并行。
- **Scope**: 给 autumn-op 加**全局** `--credential-file`（与 `--admin-token-file` 同一层），
  在需要数据面访问的子命令上用 `connect_with_credential` 取代 `connect_raw`。
  presplit 是当前唯一已知的受害者，但任何未来做数据面读写的 admin 子命令都同此。
  次选（不推荐）：把守卫里的 PermissionDenied 当作"无既有声明"处理 —— 会让守卫在开了 authz
  的集群上静默失效，正是它要防的那个 harm。
- **Acceptance**: 在 authz 开启的集群上 `autumn-op --credential-file fs.cred presplit
  --namespace fs --lanes 24 --parts 6` 成功切分并写入声明；`autumn-op info` 显示 6 个分区
  跨 3 个 PS；收窄守卫仍生效（不带 `--force` 的 `--lanes 2` 被拒）。
- **Status**: `passes: true` (2026-09-01) — 活集群验证通过。`autumn-op --admin-token-file T --credential-file fs.cred presplit --namespace fs --lanes 24 --parts 6` → `declared fs stripe geometry: 24 lanes × 8 MiB units` + `4/5 cut points applied`，`fs` 得到 6 个分区跨 3 个 PS 均摊 2/2/2。
  **踩到的次生坑（值得记）**: 第一次重试只切成 1/5，因为 FUSE O_DIRECT 探测时往 `fs/` 写了个 12 MiB 测试文件 —— 正是本条 Scope 里写明的"presplit 必须在写入任何数据之前"。恢复路径按 `docs/ops.md` 有效：删文件 → `autumn-op compact <part>`（两个分区都要）→ 重跑 presplit。删除只留 tombstone，不 compact 的话 `has_overlap` 依旧。

### F-S3-GATEWAY — 只读 S3 兼容网关（把 SGLang / FreeToken 接上 runai_streamer 快路）
- **Trigger** (2026-09-01): `autumn_vllm_loader`（`--load-format autumn`）**只对 vLLM 有效** ——
  46ed345 已修正文档中"一套实现同时服务 vLLM 和 SGLang"的错误说法。实测确认 SGLang 无插件位：
  `srt/configs/load_config.py:15` 是封闭 `LoadFormat` 枚举，`srt/model_loader/loader.py:3106`
  `get_model_loader` 是硬编码 if 链，全树无 `register_model_loader`。因此 SGLang 与 FreeToken
  目前只能走 FUSE 挂载（Recipe B，非流式快路）。
  但 **SGLang 原生支持 `--load-format runai_streamer`**（`load_config.py:34`，
  `utils/runai_utils.py:12` `SUPPORTED_SCHEMES = ["s3://","gs://","az://"]`，
  `model_loader/weight_utils.py:1144-1147` 把 `AWS_ENDPOINT_URL` 转成 `RUNAI_STREAMER_S3_ENDPOINT`），
  vLLM 同样支持。⇒ 一个 S3 端点可同时把两个引擎抬到并发流式读，且不需要改任何引擎代码。
- **Scope**: 只读、只实现 runai 真正调用的 3 个操作（调用链已从本机
  `runai-model-streamer 0.15.6` 逐层核实）：
  - `ListObjectsV2` ← `list_safetensors()` → `s3_glob(path, ["*.safetensors"])`
  - `GetObject` + `Range` ← `SafetensorsMetadata.from_files()` 先取 8 字节头长再取 header JSON，
    随后按 chunk 取张量。**所有 tensor 的 offset/size 来自 safetensors header 自解析
    （`safetensors_pytorch.py:206`），不依赖对象大小查询。**
  - `GetObject`（整取）← 引擎侧 `pull_files` 拉 config.json / tokenizer。
  必须满足 AWS SDK C++ 的严格解析：`ListBucketResult` XML（`Contents/Key/Size/LastModified/ETag`
  + `prefix`/`delimiter`/`continuation-token`）、Range 请求回 **206** + 精确
  `Content-Range: bytes a-b/total` 与 `Content-Length`、**path-style 寻址**、HTTP/1.1 keep-alive。
  **不验签**：忽略 `Authorization` 头（客户端仍需假的 `AWS_ACCESS_KEY_ID/SECRET_ACCESS_KEY`，
  因为 AWS SDK 的凭据链在发请求前就会自检）。读穿 `autumn.Fs`，属 adapter，不是并行数据面。
- **明确不做**: PUT / DELETE / multipart / versioning / ACL / CORS / 虚拟主机寻址 / SigV4 校验。
- **部署形态**: 每 GPU 节点 sidecar（长跳 EN→sidecar 走 RDMA，只在 localhost 付一段 HTTP），
  避免单点网关成为带宽瓶颈并省掉 HA 设计。
- **Acceptance**:
  (a) `aws --endpoint-url ... s3 ls` / `s3 cp` 能列出并取回 `fs/` 下的对象，字节精确；
  (b) SGLang `--load-format runai_streamer --model-path s3://<bucket>/<model>` 加载成功，
      推理输出与同模型本地盘加载一致；
  (c) vLLM 同上；
  (d) A/B：与现有 `--load-format autumn` 原生 loader 对比吞吐，比值记入 `docs/model_loading.md`
      —— 这个数字是 F-S3-RUNAI-PLUGIN 是否值得做的判据。
- **Status**: `passes: false` (2026-09-01) — 网关已实现（`examples/s3-gateway`，二进制 `autumn-s3`，
  ~600 行 + 5 单测），本机 3 节点集群验证到位；(b)(c) 引擎级端到端未跑，故不置 true。
  **已过的**：
  - boto3（真 botocore/AWS SDK）：`list_buckets` / `list_objects_v2`（prefix+delimiter+
    `encoding-type=url`）/ `head_object` / `get_object` / ranged `get_object` / 官方 paginator
    翻页 / typed `NoSuchKey` + `NotImplemented`，全过。
  - **真 `runai-model-streamer` 0.15.6 端到端字节精确**：`list_safetensors` → 逐 shard 读
    `bytes=0-7` 头长 → header JSON → 各张量 chunk，3 个张量 bytes_exact=True。这正是 SGLang
    runai loader 内部跑的那段代码。
  - 边界：206 + 精确 `Content-Range`、末段越界钳位、`bytes=-N` 后缀、416 带 `bytes＊/size`、
    404 `NoSuchKey`、写操作回 `NotImplemented`、64 并发 ranged GET 全 206 无 panic。
  - 幂等实现细节：`FsState` 用**异步** mutex 而非 `RefCell`（`RefCell` 在第二个并发请求就
    `already borrowed` panic，而流式读天然并发）；读路径按 fuse dispatcher 的分法拆成
    `prepare`（持锁、只做路由）+ `execute`（无锁、真 I/O），所以并发 GET 仍然重叠扇出。
  - **(d) A/B 已测**（2 GiB shard，loopback）：native `read_into` 8 线程 **1327 MB/s** /
    裸 HTTP 过网关 **1290 MB/s（native 的 97%）** / runai→网关 **~600 MB/s（45%）** /
    runai→本地页缓存文件 20–33 GB/s。⇒ **HTTP 这一跳几乎免费，网关不是瓶颈，runai 的张量
    流水线也不是**；瓶颈在 `libstreamers3.so` 包的 **aws-c-s3 CRT 客户端**——它每个 8 MiB
    chunk 开一条新 TCP（2 GiB 用了 241 连接 / 259 请求），每块都吃冷拥塞窗口；
    `TARGET_GBPS` / `MAX_CONNECTIONS` / `CHUNK_BYTESIZE` 全扫过，548–633 MB/s 纹丝不动。
    网关自身 keep-alive 正常（curl 复用连接）。
  **踩到的坑（已写进 ops.md）**：aws-c-s3 认 `HTTP_PROXY` 但**不认 `NO_PROXY`** —— 环境里有
  代理时，boto3 侧的 list 正常（boto3 认 NO_PROXY），每个权重读却全挂在
  `AWS_ERROR_S3_INTERNAL_ERROR` 且**不开任何 socket**。"lists fine, every read fails" 是指纹。
  **仍缺**：(b) SGLang 真起服务加载一个真模型；(c) vLLM 同（本机无 vLLM）。

### F-S3-RUNAI-PLUGIN — 用 runai 后端插件 ABI 承载 autumn 原生传输【条件性，等 F-S3-GATEWAY 的数字】
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
- **启动判据（已有数字，2026-09-01）**: 判据原文是"网关达原生 ~85% 则不做，掉到 60% 以下才启动"。
  实测 runai→网关 = 原生的 **45%** ⇒ **判据已触发**。而且测量把损失定位得很准：裸 HTTP 过网关
  是原生的 97%，runai 的张量流水线本地跑 33 GB/s，**丢的那一段恰好就是 aws-c-s3 CRT 客户端**
  —— 也正是本 feature 要替换掉的那个组件（`libstreamers3.so` 就是 CRT 的包装）。所以
  "插件会继承同样的天花板"这个反对意见不成立。
  **但先做便宜的事**：CRT 每 8 MiB 开一条新连接是首要嫌疑，先查有没有让它复用连接的办法
  （上游 issue / `aws-c-s3` 版本 / 别的 env），能修好就不必写插件。
- **Acceptance（若启动）**: vLLM 与 SGLang 均可经 `--load-format runai_streamer` 走 autumn 原生传输，
  字节精确；吞吐 ≥ 现有 `--load-format autumn` 原生 loader。
- **Status**: `passes: false` (2026-09-01) — 条件性，未启动，等 F-S3-GATEWAY 的 A/B 数字。
