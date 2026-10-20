# Compact 安全推进 checkpoint / GC floor 修复计划

状态：待实现。2026-10-08。

本计划不修改实现、不部署、不发起新的线上维护操作。已授权的 bench forcegc 队列独立继续。

## 1. 目标与问题边界

目标：当一个 WAL 前缀的数据已经全部被持久化 SST 集合覆盖时，即使没有后续业务写入，也能由 compact 发布新的 checkpoint、推进 replay/GC floor，让旧 sealed log extent 最终进入正常 GC。

保留之前修复的“不回退”保证；不能为了回收空间跳过未 flush 的已确认写入，也不能直接取消 replay 保护。

已确认的当前行为：

- `background.rs::compaction_checkpoint_cursor`：`durable_ckpt_vp != (0,0)` 时原样沿用，包括真实 compact 和无操作 compact。
- `do_compact` 成功发布 checkpoint 后只更新 SST 集合，没有针对新的安全边界推进 `durable_ckpt_vp`。
- flush、freeze、merged checkpoint 有成功持久化后更新该游标的路径。
- GC 只允许删除严格位于 floor 之前的非空 extent；即使 floor offset 已到 extent 末尾，当前按 extent 位置判断仍保护该 extent。
- `gc_debt` 的比例/字节资格判断与最终 replay 保护不是完整的同一套判断，可能触发“有 debt → GC → 无 eligible extent”的空跑。

不把“compact 遍历完所有 SST”本身当作 WAL 尾部安全的证明。pending/inflight 写入、active memtable、immutable 队列、正在构建或尚未发布的 flush，以及 GC 搬迁写入都必须纳入证明。

旧版 inline WAL 没有 discard 记账是独立历史问题：本修复不会从 inline SST 反算旧 WAL 位置。旧 extent 的受控 forcegc 清理不等同于自动补账。

## 2. 必须保持的不变量

1. **覆盖性**：checkpoint 的游标 C 之前，所有需要恢复的 WAL 记录，均已被该 checkpoint 引用的 SST 覆盖，或已被持久化的新版本/删除记录取代。VP 指向的存活 payload 仍由 GC 的存活检查保护。
2. **ACK 后推进**：只有 checkpoint 持久化成功后，才能发布新表集合并更新 `durable_ckpt_vp`；失败或结果不确定时不推进、不放宽 GC。
3. **同一快照**：表集合、游标、fence floors 在 `publish_lock` 下形成一致 checkpoint；不能把旧 SST 集合与后完成的 flush 游标拼在一起。
4. **不回退**：同一 log stream 中按实际成员位置及 offset 比较新旧边界，不按 extent ID 数值比较；已有 durable 游标不能被旧 SST header 拉回。
5. **写入隔离**：边界 C 之后的新写不要求进入本次 compact，但不得被误认作 C 之前已经落 SST。
6. **拓扑隔离**：split/merge、owner fencing、tail rotation、GC 等改变 log 成员关系时，旧证明必须被重新验证或废弃；不能跨 stream/owner epoch 套用游标比较。
7. **恢复可达性**：新 checkpoint 的 anchor 必须能被重启后的 recovery 正常定位，不能先删除 anchor 再依赖全量 replay 兜底。

## 3. 阶段 A：先补失败用例与时序证明

涉及：`crates/manager/tests/`、partition-server 内部测试。

先建立可控回归：

1. 写入并 flush 数据，随后产生只有已被 SST 覆盖记录的 sealed WAL 前缀；没有后续业务写入。
2. 将 durable checkpoint 留在旧 sealed extent，运行 major compact 和 no-op compact。
3. 断言 checkpoint/floor 安全推进，旧前缀可 GC；重启不依赖全量 replay，所有已确认存活 key/删除状态正确。

增加 failpoint，将以下步骤分别停住：写入 ACK、memtable 插入、flush 上传、flush 发布、compact 发布、checkpoint ACK。先画出实际 partition loop 与 flush/compact 的锁和 await 顺序，证明 proposed barrier 不会与维护 gate、flush drain 或 GC 搬迁互相等待。

验收：关闭新行为时用例失败；只把游标改成 live tail 的错误实现必须在未 flush 写入/崩溃用例中失败。测试不能仅断言某个字段增加。

## 4. 阶段 B：建立“已落 SST 边界”的显式证明

涉及：`partition-server/src/lib.rs`、`rpc_handlers.rs`、partition loop / write / flush 路径。

实现一个内部 checkpoint barrier，不直接复用对外 split/merge freeze 状态：

1. 在串行写入调度路径插入边界标记，排空标记之前的 pending/inflight 写入，覆盖所有 WAL 写入来源，包括 GC relocation。
2. 在这个有序点捕获 committed WAL 边界 C，必要时旋转 active；确认不能有“WAL 已提交、memtable 尚未可见”的写入落在 C 之前。
3. 将 C 之前的 immutable 按既有顺序 flush 并发布；如果对应数据已经全部在 SST 中，则无需制造新的非空 SST，也无需等待新的业务写入。
4. 短暂屏障期间后来的请求排队；建立隔离后允许 C 之后写入继续，不在整个大 compact 期间冻结业务。队列必须有界。
5. 得到包含 stream、owner epoch、边界 C 和覆盖状态的内部证明；验证失败、超时、拓扑变化则不推进，安全退回已有 durable 游标。

优先复用现有 freeze/drain 的顺序保证与 flush ACK 路径，但不能只检查 `active.is_empty()`、不能只持有 `publish_lock`，更不能在无法证明覆盖时读取 live tail 当 checkpoint。

具体屏障实现由阶段 A 的时序审计确定；覆盖证明无法成立时保持旧边界，而不是使用启发式跳跃。

## 5. 阶段 C：compact 发布新 checkpoint

涉及：`background.rs::do_compact`、`checkpoint_and_truncate_row_prefix`、`compaction_checkpoint_cursor`、checkpoint 发布 helper。

- 在选择 compact 输入前建立阶段 B 的已落 SST 边界。major compact 使用该边界；no-op compact 也能发布边界，不能因没有 SST 重写就提前跳过。
- minor compact 不强制引入完整 drain；继续沿用最新安全 durable 边界，除非已有有效的新覆盖证明。
- 在 `publish_lock` 下重新读取最新 durable 游标和当前 SST 集合；把 compact 输出替换输入，保留过程中已成功发布的新 flush SST。
- 仅比较同一有效 log 成员顺序中的已证明边界与当前 durable 边界，选不回退的安全值。并发 flush 已经前进时不能被本次 compact 的旧 C 覆盖。
- 表集合、边界、fence floors 一起调用 `save_table_locs_raw`。
- checkpoint 成功后，在同一发布临界区更新 SST 集合和 `durable_ckpt_vp`；no-op 发布成功后也更新该 cell。
- `(0,0)` fallback 保留保守恢复路径；若 fallback 新产生了有效 checkpoint，运行时 cell 也应与它一致。
- 不为推进全局 checkpoint 而重写所有 SST header；旧 SST header 可以保留原内容边界。

不能采用的捷径：从 SST 最大 seq 推算 WAL byte cursor；取最大的 extent ID；从尚未 flush 的 `p.vp_*` 推进；仅对游标做 `max` 而不验证表集合覆盖。

## 6. 阶段 D：解决 extent 末尾 anchor 的保守滞留

checkpoint 不回退还不够：`C=(sealed_extent, sealed_length)` 在当前严格 before-floor 规则下仍会保护整段 extent。

- 若 C 的 extent 已权威 sealed，offset 恰好覆盖 sealed_length，且当前同一 stream 有经过验证的后继 extent，允许将等价重放起点规范化成 `(successor,0)`。
- 规范化后的 anchor 与表集合一起持久化，ACK 后才能让 GC 移除旧 extent。
- 比较和规范化均按 stream 实际顺序；必须处理 split/merge 的非单调 ID、重复成员的既有恢复语义和并发成员变更。
- 若无后继、旧 tail 仍 open、C 未覆盖到末尾，保留原 anchor。本计划不为释放最后一个 open tail 强制新增 seal/roll 操作。
- 明确验证 recovery 对新 anchor 的选择；不能让旧 SST header 重新拉低已经确认的 durable 边界。

## 7. 阶段 E：统一 GC debt / 候选 / 实际执行

涉及：`background.rs` 的 debt basis、GC 候选选择；`rpc_handlers.rs` 的 forcegc advisory / diagnostics；manager policy。

- 使用相同 stream 成员、authoritative seal 状态、有效 durable floor 和比例/绝对字节门槛判断“当前可选 extent”。
- `gc_debt` 不再把已知被 replay floor 保护的字节宣称为当前可回收；如需显示垃圾总量，单独保留 raw dead / replay-protected dead，不能静默混成同一含义。
- floor 或 stream membership 变化时同步刷新/失效 debt basis；idle refresh 不能恢复成包含受保护字节的旧值。
- diagnostics、forcegc advisory 与执行使用同一 effective floor，包括 durable checkpoint 提升；修正当前文案将实际“at/after”写成“at/before”的歧义。
- 当 floor 阻挡了足量垃圾时，提供可观测的原因与 checkpoint 推进需求。自动动作以边界变化和冷却控制，避免把 GC 空跑换成 checkpoint 空跑。
- split 继承同一份既有 discard 账目是正确行为；各分区的 raw debt 不可直接相加为物理可释放空间。增加继承用例，不在此计划中改成按 refs 平分。

阶段 E 不得取消执行端 replay guard；即使策略口径出错，执行端也继续 fail closed。

## 8. 独立后续：共享引用 CAS 冲突有限重试

这是并发完成率问题，不是推进 floor 的前置条件，单独提交。

- 在元数据 mutation 层对明确的 CAS rejection 重建最新 stream/extent 快照，重新执行 owner、membership、EC/recovery/topology guard，再提交事务。
- 有限重试和退避，不能重复使用旧 refs/eversion；不重跑整个大 extent 的扫描来解决一次 refs 更新冲突。
- owner fencing、未知提交结果、CRC 错误、普通 timeout 不作为 CAS 冲突盲目重试。
- 对已不属于 stream 的 extent保持幂等；保留引用计数 CAS 保护。
- 并发多个共享持有者 GC，断言最终 `refs == in_streams`，无 ghost refs，无提前物理删除。

## 9. 回归与验收矩阵

| 场景 | 必须验证 |
|---|---|
| 无新业务写入，全部已落 SST | compact/no-op 可推进 checkpoint；eligible sealed 前缀最终回收 |
| active / queued imm / flush inflight | 新 floor 不跨过未被 checkpoint 覆盖的数据 |
| WAL ACK 后、memtable 可见前暂停 | barrier 等待该写入，不能错误判空 |
| compact 中并发 flush | 表集合包含新 flush；游标不回退、不超前 |
| checkpoint 发布失败或 ACK 不确定 | 不提前更新内存 floor、不执行越界 GC；重启数据完整 |
| checkpoint ACK 后立即 crash | recovery 使用新边界，数据/删除状态正确 |
| durable 位于 sealed 末尾 | 后继 anchor 规范化成功后才可移除旧 extent |
| boundary 后仍有业务写入 | 重启完整 replay 后缀，不遗漏已确认写入 |
| split / merge / fence / manager 切换 | 旧证明不跨拓扑/owner使用；游标比较不依赖 ID 大小 |
| 删除全部 key、discard-only SST | discard/seq/checkpoint 保留，重启不复活 key |
| inline 与大 VP 混合 | inline 旧账不虚构；存活 VP 在 GC 释放前完成安全迁移 |
| replay-protected 垃圾 | debt 不驱动反复 GC 空跑，diagnostic 清楚说明阻挡原因 |
| 50% discard 后连续 split | 子分区继承50%既有账目，物理空间不按子分区 debt简单求和 |
| 共享 refs 并发修改 | CAS 重试有界，refs与实际stream数一致，最后引用后才删除 |

必须跑现有相关回归：`system_compact_ckpt_cursor`、`system_restart_replay_cursor`、`system_compact_unflushed_vp_head`、`system_compact_checkpoint_fail`、`system_crash_mid_compact`、`system_compact_settles_deletes`、`system_gc_multiversion_same_extent`、`system_gc_inline_wal`、`system_merge_single_checkpoint`、`system_merge_replay_reachability`、split/fence/VP 系列。

新增测试先复现再修复，做错误实现的 ablation。系统测试按现有仓库要求使用 release 与合适的 `RUST_MIN_STACK`；不以 fmt/clippy 通过代替一致性验证。

## 10. 提交顺序与线上验证

建议独立提交：A（失败用例与时序证明）→ B/C/D（安全边界与 checkpoint 推进）→ E（debt/候选一致性）→ CAS 重试。

线上验证需要另行获得实现、build/rollout及测试操作授权。本计划不是上线授权。

1. 先在隔离测试集群验证，在 SST 稳定、无新写入的分区上记录：log membership、旧/新 checkpoint、raw/protected/eligible debt、refs、磁盘实际占用。
2. 再测持续读写 + compact + GC + PS crash / manager 切换；分别验证 ACK key、删除状态、恢复 replay 起点、业务可用性。
3. 对共享 bench extent 等待所有持有者解除引用及 EN 删除队列完成，确认物理空间下降；不能把 `reclaimed N extent(s)` 直接等同于磁盘已释放。
4. 对包含 fs/kvc 的边界分区验证存活 VP；不要用 bench key 为0推断整个分区/extent为空。
5. 新写入场景无需人工 forcegc 即可收敛；旧未记账 extent 用既定清理流程处理，单列历史欠账。

遇到数据缺失、游标超前/回退、无法定位 anchor 或 refs 不一致，立即停止新增维护动作并保留元数据/日志。不能假定回滚旧二进制可以安全读取已被新 GC 删除的 WAL；回滚必须先证明旧 recovery 接受新 checkpoint，优先暂停回收并前向修复。
