# GC / Compact / Recovery / Fence / Remove 代码审查

- 审查日期：2026-09-19。
- 基线：main，commit c6a4a8239dbfa39a05667194d907897d2ea2a3fb；开始时工作区干净。
- “Remoe” 按 Remove 理解，覆盖节点退役及 extent 物理删除。
- 本次交付为审查报告，不修改产品实现。范围是当前代码的正确性与测试有效性，不是单个 commit 的 diff review。

**结论：现有保护和回归测试已经覆盖不少历史故障，但不能据此认定极端情况已覆盖。当前仍有 GC 提前 EOF 后误 punch、Recovery 跨 attempt 接受旧完成、takeover fence 失败后继续服务、Remove 漏查 Recovery 目标等高风险缺口。主 chaos 还有确定性的空验证：写入存活探测遗漏 namespace，全部跳过；“物理回收”只检查 etcd metadata 消失。**

以下区分三种证据：**运行确认**指实际执行了公开 API、原始测试或源码函数；**代码确认**指调用链和分支可直接证实，但未跑完整故障时序；**待验证**指仍有额外前提，不能作为已复现的数据损失结论。

## 1. 优先修复的问题

### R1 — [P1] GC 接受提前 EOF，未读满 sealed length 也会 punch

**位置：** [background.rs:3394](crates/partition-server/src/background.rs#L3394)、[background.rs:3447](crates/partition-server/src/background.rs#L3447)、[background.rs:3486](crates/partition-server/src/background.rs#L3486)；读侧 [extent_node.rs:2665](crates/stream/src/extent_node.rs#L2665)、[client.rs:5032](crates/stream/src/client.rs#L5032)。

run_gc 已有 CRC 错误即终止、尾部不完整 record 即拒绝 punch 的保护，但没有检查扫描总长度：

1. 请求扫描权威 sealed_length=L 的 extent。
2. 普通 MSG_READ_BYTES 按 EN 本地文件长度截取；短副本可以返回 CODE_OK 和空 payload。read_replicated_with_failover 对 Ok 直接返回，不继续尝试好副本。
3. run_gc 看到 chunk.is_empty() 就 break；如果停在 record 边界或 offset=0，carry 为空，后面的校验也通过。
4. flush_gc_batch 后直接 punch_holes，扫描遗漏的 live VP 没有搬迁。最后一个 stream 引用被移除时，好副本也会被删除。

**影响：** 单副本截短原本可以由其他副本恢复，却可能经 GC 变成不可恢复的数据损失。refs/VP identity/replay floor 均不能替代“完整扫描”的证明。

**验证：** 已用真实 EN 的 append → test_seal_durable → 截短 .dat → 新 EN 加载同目录 → RPC read 验证前提：请求 8192 字节，返回 code=0、bytes=0、end=0；测试保留了真实 seal 生成的 checksum sidecar。空读不覆盖完整 checksum block，不会被内容校验挡住。尚未运行完整 PS GC → 物理删除 → GET 丢失的集成复现。

**建议：** 对 sealed extent 的每个 GC read 要求返回量等于本次 want，提前 EOF/短读必须换副本或中止；punch 前再要求 cur==sealed_length 且 carry 为空。复用 PS WAL replay 已有的 committed window/short-read 判断思路（[lib.rs:9056](crates/partition-server/src/lib.rs#L9056)）。

**应补测试：** extent 含 live VP，截短一个副本到 0 及完整 record 边界；固定读路由命中短副本；ForceGC 后断言完整搬迁或拒绝 punch，再杀 PS、重开并逐字节验证。故障要在已有 checksum 和没有 checksum 两种状态各测一次。

### R2 — [P1] Recovery 的“attempt identity”实际只比较节点组合，存在 ABA

**位置：** [recovery.rs:200](crates/manager/src/recovery.rs#L200)、[recovery.rs:892](crates/manager/src/recovery.rs#L892)、[extent_rpc.rs:839](crates/rpc/src/extent_rpc.rs#L839)、[extent_node.rs:9130](crates/stream/src/extent_node.rs#L9130)。

classify_recovery_completion 的 Apply 条件仅为 live marker 的 (node_id, replace_id) 与 done 相同。RecoveryTask.start_time 未参与比较；done 没有 nonce、复制时的 eversion 或 payload layout。相比之下，EC 完成会校验 coordinator、eversion 和 attempt_nonce。

可达的危险交错是：旧任务 A 从复制布局重建；其 marker 因 executor Suspected 或 source unfence 被释放；extent 转 EC；再次为同一 replace_id 派到同一目标形成任务 B；A 的完成消息晚到。只要目标当前不是其他 slot 的成员，节点组合与 duplicate-node guard 都通过，旧的完整 .dat 可以被当成当前布局要求的 shard。

EN 侧已经在跑某个 extent 的恢复时也仅凭 extent_id 返回“already running”，没有证明正在执行的任务等于本次 assignment/attempt；这进一步弱化了重派发的身份契约。

**验证：** 从当前源码原样提取 classify_recovery_completion 并用 rustc 运行，相同节点组合的旧完成对新 marker 返回 Apply。完整“释放 → EC → 同目标重派 → 旧 done”时序未运行；数据损失后果属于代码推导。

**建议：** 将现有 inflight 创建 revision/nonce 贯穿 RequireRecoveryReq、EN 去重、RecoveryTaskDone 和 apply。marker 同时固定源 eversion、slot 和 payload location；应用时比较这些条件。仅比较秒级 start_time 不足以解决同秒重派和进程重启的 ABA。

**应补测试：** A、B 保持相同 extent/target/replace，nonce 不同；延迟 A 的 done 到 B 已创建后，断言 A 被拒、B marker 保留。再加入复制→EC 及相同目标重启两种布局/生命周期变化。

### R3 — [P1] takeover fence 全部失败后仍继续打开分区

**位置：** [lib.rs:6036](crates/partition-server/src/lib.rs#L6036)、[lib.rs:6088](crates/partition-server/src/lib.rs#L6088)、[client.rs:4253](crates/stream/src/client.rs#L4253)。

fence_tail 在没有任何副本确认 fence 时正确返回 Err，但调用方重试三次后只 warn，接着 recover_partition 并进入正常服务。没有一个持久化 fence 成功时，“all-replica ACK 至少碰到一个已 fenced 副本”的安全论证不成立。

**触发：** 新 PS 获取 epoch；旧 PS 暂停；故障只挡住 takeover 的 FENCE_EXTENT 请求，或者恰好在整个 fence 重试窗口断开并在 replay 前恢复。新 PS 完成 replay 后，旧 PS 恢复并用旧 epoch 向未被 fenced 的 EN 写入。在旧 PS 自我驱逐前，写可能获 ACK，但新 PS 已越过 replay 窗口。

**证据：** 失败后继续 open 的代码已确认；未运行 selective-fence-drop 的端到端复现。现有 system_sigstop_zombie_writer 测了正常 eager fence，但不能证明“fence 失败”分支安全。

**建议：** 没有取得必要的持久化 fence 证明时，保持分区不可服务并重试打开。错误码与监控应明确表示 takeover 尚未完成。对于已封存 tail 等可安全豁免的情况，单独给出可验证条件。

**应补测试：** 只丢弃新 PS 的 FENCE_EXTENT，允许 commit probe、replay read 和旧 PS append；新 PS 不得宣布打开成功。恢复网络后再验证它能完成 takeover，旧 epoch 始终不能得到新的成功写 ACK。

### R4 — [P1] Remove 漏查 Recovery 目标；晚到完成可重新引用已删除节点

**位置：** [rpc_handlers.rs:6487](crates/manager/src/rpc_handlers.rs#L6487)、[rpc_handlers.rs:6511](crates/manager/src/rpc_handlers.rs#L6511)、[recovery.rs:251](crates/manager/src/recovery.rs#L251)、[recovery.rs:465](crates/manager/src/recovery.rs#L465)、[recovery.rs:1159](crates/manager/src/recovery.rs#L1159)。

Remove 的 blocker 只有当前 extent 成员及 ConvertToEc.target_nodes。Recovery 目标在 apply 前本来就不是 extent 成员，所以不会被这两项捕获。

同时，release_recovery_markers_for_dead_executors 只看节点不存在/Suspected，没有看 Fenced；redispatch_pinned_recovery 只看 Online。因此，一个仍正常响应 df 的恢复目标被 Fence 后，已有任务仍保留并可以继续报告完成。

**运行确认：** 构造 extent 42、Recovery(source→target)，Fence target 后调用真实 handle_remove_node：返回 CODE_OK，blocker 两个列表均为空，target 已从节点表删除；随后 acquire 相同 extent 的 marker 仍返回“already has an in-flight op”。证明 Remove 成功时确实留下了指向已移除节点的 Recovery marker。

**后果边界：** 如果 node_health_loop 已缓存该节点/df，而 done 在 Remove 后、下一次 executor-release 前应用，apply_recovery_done 没有重新校验 target 仍注册、未 decommissioned、磁盘仍属于它，会把 slot 写向已删除节点。这个最终交错未运行；若下一 tick 先清掉 marker，完成会被拒，不是每次 Remove 都会损坏布局。

**建议：** Remove 检查所有会新增成员的 marker，至少 Recovery.node_id；Fence 对已有目标任务明确取消或排空语义；apply 在同一个提交条件里检查目标身份和节点状态。单靠预先扫描不能解决 Remove 与新 dispatch/apply 的并发窗口，应使用共享串行化或 etcd compare。

**应补测试：** Recovery 进行中 Fence 并 Remove 其目标；让 Remove、已接收 df、done apply 以两种顺序执行。断言拒绝 Remove 或原子结束任务，任何时候 extent 不得引用已 tombstone 的 node/disk。

### R5 — [P1] Recovery apply 的事务只做 leader fence，没有锁定所校验的 marker/extent 版本

**位置：** [recovery.rs:1247](crates/manager/src/recovery.rs#L1247)、[lib.rs:376](crates/manager/src/lib.rs#L376)、[extent_inflight.rs:210](crates/manager/src/extent_inflight.rs#L210)。

apply_recovery_done 在内存中检查 marker、克隆 extent，然后执行 put_and_delete_txn。该 helper 的额外 compare 为空，仅验证 leader 身份；事务无条件覆盖 extents/<id>、删除 extent_inflight/<id>，await 返回后也无条件安装内存副本并释放 marker。

**危险窗口：** 同一个 leader 上，apply 在发送事务前被延迟；recovery_dispatch 先释放旧 marker，并取得另一个 assignment 或 EC marker；旧 apply 随后提交，就能删除后继 marker。leader fence 在这个场景始终通过。即使旧事务已提交、只是响应延迟，之后的无条件内存安装/释放也需要验证所持 attempt 没有变更。

**证据：** 缺少 compare 和 after-await identity 检查已由代码确认；本轮未完成真实 etcd 响应重排复现。此项与 R2 独立：R2 是处理前误认旧 done，R5 是校验通过以后被异步窗口改变状态。

**建议：** 在同一 etcd 事务比较 marker 的创建 revision/nonce 和 extent 的 baseline，成功后按相同 nonce 安装内存状态。可参考 abandon_ec_marker 已有的 record compare + after-await nonce 检查，而不是增加无条件重试。

**应补测试：** 用 barrier 卡住 apply 的 etcd 请求/响应，释放 A 并创建 B，再恢复 A；B 的 marker 和 extent 状态必须保持。现有 apply_done_atomicity 测的是 EC 成功/leader 被罢免后的原子性，没有覆盖同 leader 的这一窗口。

### R6 — [P2] 非 force 的 Fence 检查没有证明存在可用恢复目标

**位置：** [rpc_handlers.rs:6441](crates/manager/src/rpc_handlers.rs#L6441)、[recovery.rs:550](crates/manager/src/recovery.rs#L550)。

check_capacity_for_fence 累加了 data_to_migrate，却只检查“其他节点有 online disk”。它不排除该 extent 的现有成员，不检查其他节点是否 Fenced/Maintenance/Suspected，也不比较所需空间与 free bytes。函数注释所说的默认 1.2 倍空间检查并未实现。

**运行确认：** RF2 extent 已占用仅有的两个节点，调用 force=false Fence 一个成员仍返回 CODE_OK；Recovery 的目标选择排除全部现有成员，实际无 spare，无法完成退役。system_recovery_loop_drives 的 no-spare 测试也明确说明这种恢复不会前进。

**建议：** 至少按相同 placement/occupied 条件验证每个受影响 extent 有合法目标；空间估算要考虑 per-shard size 和实际 headroom。若产品本意仅提供极弱检查，应修正接口语义/说明，不能让 non-force 给出容量已检查的印象。

### R7 — [P2] Remove 成功后重试返回“必须先 Fence”，不满足 lost-reply 幂等语义

**位置：** [rpc_handlers.rs:6492](crates/manager/src/rpc_handlers.rs#L6492)、[rpc_handlers.rs:6594](crates/manager/src/rpc_handlers.rs#L6594)。

第一次 Remove 成功会删除 override 并写 decommissioned tombstone；第二次请求首先检查 override，直接返回 CODE_PRECONDITION，没有读取“同一节点已经移除”的终态。

**运行确认：** 相同 RemoveNodeReq 第一次 code=0；重试 code=3，message="node 3 must be Fenced before remove"。如果第一次响应丢失，调用方会把已完成退役看成未完成；不能靠 clear override 补救，否则会解除 tombstone。

**建议：** 对匹配的 decommissioned 终态返回幂等成功/明确 AlreadyRemoved，不改变 tombstone。补一条“事务落地但响应丢失，重发相同 Remove”的测试；现有 system_lost_reply_idempotency 覆盖 alloc/split/punch，没有 Remove。

## 2. Chaos / 回归测试本身的问题

### T1 — [P1，测试可信度] 写入存活探测遗漏 mem/，当前拓扑下全部跳过

**位置：** [system_chaos.rs:579](crates/manager/tests/system_chaos.rs#L579)、[system_chaos.rs:2783](crates/manager/tests/system_chaos.rs#L2783)、[system_chaos.rs:3836](crates/manager/tests/system_chaos.rs#L3836)。

writer 使用 mem/b000000、mem/q000000，初始分区为 [mem/a, mem/z)。verify_write_liveness 却生成 b000000/q000000；前者小于 mem/a，后者大于 mem/z，没有任何候选落在范围内，直接 continue。split/merge 后的子范围仍位于该范围内，问题不会自行消失。最终函数返回空 errors，被打印为“write-liveness errors=0”。

**运行确认：** 按源码候选格式枚举，在完整范围及 [mem/a,mem/m)、[mem/m,mem/z) 中候选数均为 0。

**修复与验收：** 与 writer 共用 namespaced key builder，并断言实际 probe 的分区数和 ACK 数。再让 PS 保持 GET 可用、拒绝所有 PUT；checker 必须报错。还应修正“50 次 8 KiB 会触发 rotation/flush”的注释：即使前缀修好，当前 make_value 对未识别 key 产出 256 B；修好后 50×8 KiB 也不等于证明触发 flush，应显式等待 flush/append 计数。

### T2 — [P1，测试可信度] “物理回收成功”只检查 etcd extent 消失

**位置：** [system_chaos.rs:3140](crates/manager/tests/system_chaos.rs#L3140)、[system_chaos.rs:3285](crates/manager/tests/system_chaos.rs#L3285)、[extent_delete.rs:163](crates/manager/src/extent_delete.rs#L163)。

read_extent_id_set 只读取 extents/；before/mid/after 的差集只能证明 manager metadata 删除。真实 unlink 是之后由 Delete marker/primary retry/persisted retry 驱动的。即使 EN 的 DELETE 永远失败，metadata 仍可以消失。

此外，该 helper 将 etcd 连接/读取错误转为空集合：after 快照读取失败可能被解释为“全部回收”。随后独立 accounting 的一次成功快照并不能反证这个回收数量是假的。gc_reclaimed 使用时间窗口差集，也不能区分 ForceGC 与同窗口的 sealed-empty sweep、row/meta truncate。

**修复与验收：** 快照失败返回 Err；记录明确的候选 extent 与各副本文件路径，等待这些 .dat/.meta/.shard/.cksum 实际消失，并检查 Delete/persisted-retry 状态。注入“DELETE RPC 永久拒绝”时，回收检查必须失败，即使 extents/<id> 已不存在。磁盘占用总量只能作为辅助，不代替逐 extent 判定。

### T3 — [P2] 多个 crash/restart 测试没有停止旧进程/运行时

**位置：** [system_crash_mid_compact.rs:58](crates/manager/tests/system_crash_mid_compact.rs#L58)、[system_crash_mid_flush.rs:46](crates/manager/tests/system_crash_mid_flush.rs#L46)、[system_ps_recovery.rs:60](crates/manager/tests/system_ps_recovery.rs#L60)、[support/mod.rs:162](crates/manager/tests/support/mod.rs#L162)。

这些测试 drop(ps1) 丢弃的是 RpcClient，start_partition_server 启动的 detached thread 继续服务/心跳。再次启动相同 ps_id 得到的是两个活 PS 的竞争，不是 crash 后恢复。compact 测试还只发 maintenance、sleep 100 ms，没有同步点证明 kill 位于输出落盘与 checkpoint 提交之间。

stream/tests/extent_restart_recovery 也使用 detached serve；结束一个局部作用域不等于结束同一个 compio runtime 上的服务 task。它能验证从文件重新 load 的部分行为，但不能作为进程退出/未完成 I/O 丢失的证据。

**修复与验收：** SIGKILL 子进程或使用真正丢弃 runtime 的 killable helper，并等待退出。设置“输出 SST 已 durable、checkpoint 未提交”等 barrier 后 kill。stoppable helper 的 graceful drain 对正常停机有价值，但不能覆盖断电/硬崩溃窗口。

### T4 — [P2] 全动作、极端故障和持续一致性没有形成验收门槛

**位置：** [system_chaos.rs:1890](crates/manager/tests/system_chaos.rs#L1890)、[system_chaos.rs:4055](crates/manager/tests/system_chaos.rs#L4055)、[ci.yml:70](.github/workflows/ci.yml#L70)、[decommission_chaos.sh:55](scripts/decommission_chaos.sh#L55)。

- 主测试默认 30 s、动作间隔 3 s，共 13 个动作；即便每个动作瞬时完成，也跑不完一轮。shuffled round-robin 保证的是有足够时间时尝试，不保证每轮成功执行。
- 动作失败/超时计为 skipped；启用动作整轮成功次数为零只输出 NOTE，没有通用失败条件。某些 rot/recovery 检查有额外非空保护，但不是所有动作都有。
- decommission_chaos.sh 显式覆盖 AUTUMN_CHAOS_ACTIONS，却没有列入 corrupt，因此其“FULL action set”已不准确。
- 主 chaos 的 manager 与单个 PS 是 in-process，nemesis 杀的是 EN；Remove 是停止 workload 后的可选终态阶段。不能覆盖 manager failover × Recovery apply、PS crash × Compact/GC commit、在线 Remove × Recovery 目标。
- reader_loop 只校验 value 形状，不检查历史顺序；writer 对 timeout 将整个 key 从 expected 删除。它能做停止写入后的最终值校验，不能证明中途无 stale read、无 lost update、无非法回滚。未知结果应记录为允许分支，而不抹掉此前已确认的历史。
- 主 workload 只有 256 B/8 KiB PUT/GET；不含 delete、TTL、同 key 多 writer，也无法替代多 GiB extent、EC stripe、GC carry 的边界矩阵。
- CI 没有 --ignored 或专用 chaos job，也没有安装 toxiproxy；主 chaos、leader_fence、部分大 GC/故障用例被 ignore，编译到不等于运行过。

**建议：** 明确区分 smoke、定向故障和长跑；关键动作记录 attempted/accepted/entered/completed，并对每轮指定的必需路径做非零断言。为两故障重叠设计可控 barrier，不依赖更长 sleep。保留 invocation/response 历史以校验允许的并发结果。

## 3. 已有的有效保护与覆盖边界

下面是源码/断言层面的覆盖判断；除验证记录中列明的 10 个既有用例，不能理解为本轮已跑绿。

| 场景 / 不变量 | 已有实现或测试证据 | 本次判断与缺口 |
| --- | --- | --- |
| GC 只处理已 seal extent，避免 stale open cache 当空 extent | authoritative_sealed；system_split_forcegc_stale_cache | 有针对性保护，R1 是另一条短读窗口 |
| GC 不覆盖同 extent 内同 key 的新版本 | 完整 VP identity；system_gc_multiversion_same_extent | 有回归覆盖 |
| GC vs seq 已分配但未入 memtable 的 PUT | inflight_write_keys；system_gc_inflight_wal_put | 有 barrier，覆盖精确窗口 |
| GC replay floor 与 durable checkpoint | gc_replay_floor / gc_floor_raise_to_durable_ckpt；system_gc_floor_durable_ckpt | 有专门验收，重用例被 ignore；需硬崩溃交叉验证 |
| Compact 使用输入 SST vp_head，不跨过未 flush WAL | compaction_output_vp_head；system_compact_unflushed_vp_head | 关键保护存在；已实现 imm rotation 时保存 vp_head，不应把旧注释中的已修复问题重新报成缺陷 |
| Compact 输出发布和并发 flush 顺序 | 输出插回旧表位置；snapshot→send 无 await；对应 publisher/order 测试 | 正常发布顺序有保护；不能替代 checkpoint 写失败/硬崩溃测试 |
| 腐化 WAL / SST lookup 错误 | process_gc_chunk fail-stop；paged SST lookup Err 中止 GC | 防止解析失败误删；不覆盖合法空响应 |
| Recovery 成功、重复派发、丢 completion、partial .dat | system_extent_recovery 的对应测试 | 覆盖复制路径；R2 的同 assignment 新 attempt 未覆盖 |
| 真正驱动 recovery loop / 无 spare 的对照 | system_recovery_loop_drives | 有非空触发断言，比只测 predicate 更强 |
| Fence/unfence 后 marker 与 limiter 释放 | extent_inflight/recovery 的单测与 tick 测试 | 已有 source unfence 回归；目标节点被 Fence/Remove 不等价 |
| EN Delete 与当前 Recovery 互斥 | delete_recovery_race | 本轮运行通过；注入 marker 测 handler，不代表验证复制中途 kill/delete 全流程 |
| leader fence 与 metadata 原子提交 | leader_fence、apply_done_atomicity | 专项存在，部分 ignore；未证明 R5 同 leader 时序 |
| PS SIGKILL / SIGSTOP zombie | system_ps_failover_chaos、system_sigstop_zombie_writer | 独立专项存在；主 chaos 无该组合，fence 全失败缺口仍在 |
| 2/3 副本同时丢失、EC 超容错 | system_correlated_2of3_loss | 独立专项存在；主 nemesis 有 healthy budget，不能声称主 chaos 覆盖 |
| 非对称网络分区 | system_asym_partition_grayfail | 专项存在；需要核对是否验证修复与持续可用性，文件中的 reproduce 注释不是通过证明 |
| ENOSPC / 单盘 media fault | enospc_chaos.sh；主 chaos 每 EN 多目录 | 独立 ENOSPC 场景存在；主 chaos 多目录共用文件系统，不等于单盘 Full/Faulted。feature_list 明确盘级注入未完成 |
| Remove 与身份 tombstone | node_lifecycle、terminal decommission | 9 个 smoke 本轮通过；并发目标、lost reply、真正 tombstone 后再启动的组合不足 |
| GC 空间回收 | verify_gc_reclaim、physical_deletion 专项 | 专项与主 checker 要分开看；主 checker 当前不能证明物理 unlink |

## 4. 仍需定向验证的边界，不算已确认生产事故

### 4.1 merge 后 replay cursor 全部不可解析时，global max_seq fallback 的前提

[lib.rs:8824](crates/partition-server/src/lib.rs#L8824) 只为 vp_extent_id 仍在 log 的 checkpoint 建 source region；全失效时退到 global max_seq（8851），replay 在 9196 丢弃 ts<=extent_dedup 的记录。数值模型很清楚：survivor SST max=100、victim SST max=1000，survivor 尚未 flush 的 seq=101 会被 global=1000 跳过。

**没有将它列成已确认 P1 的原因：** 正常 handle_merge_partitions 会 freeze/drain/flush 两边；上述“pre-merge source 存在未 checkpoint 记录”的形状不能直接假定仍可从正常流程形成。代码注释承认该 residual，但注释本身不是可达性证明。应针对 raw multi_modify_merge、checkpoint 失败、旧状态迁移、sealed-empty cursor 回收建立真实时序，再决定修复方案；不要仅凭数值模型宣称日常 merge 必丢数据。

### 4.2 Recovery marker 清理失败后的内存 / etcd 分裂

[recovery.rs:850](crates/manager/src/recovery.rs#L850) 的 release_recovery_marker_best_effort 在 etcd 删除失败后仍清内存，并声称 stale sweep 会清掉 etcd marker；但 [extent_inflight.rs:575](crates/manager/src/extent_inflight.rs#L575) 已明确排除 Recovery，且扫描的是内存 marker。若这些 stale-layout/extent-removed 分支可达，再遇到一次 etcd 错误，重派发的 create_revision==0 CAS 会一直失败，直到 leader replay。需要把可达分支与一次性 etcd 故障连起来复现，不能将它当成有定时自愈保证。

### 4.3 Compact checkpoint 失败与后续 GC 的 durable 集合

do_compact 在 save_table_locs_raw 前已经换掉 p.tables/p.sst_readers；失败后 caller 只记录 FAILED，未回滚或停止分区。旧 WAL 通常仍能恢复，因此“内存先换表”本身不等于数据损失。应验证失败后下一次 GC 使用新 SST replay floor 时，搬迁 WAL、旧 durable checkpoint 与后续 row truncate 是否始终构成完整恢复链；尤其是 GC relocation 和前台写入的 seq/append/flush 顺序。当前假 crash 测试不能回答这一点。

## 5. 建议的 chaos 验收矩阵

修复验证应先用可控 barrier 得到固定失败，再加入随机组合；每项都要求关掉对应修复后测试确实变红。

| 优先级 | 场景 | 必须观测的结果 |
| --- | --- | --- |
| 第一批 | GC 读到 0 / record 边界短读，其他副本完整 | 完整搬迁或不 punch；重启后 live key 全部正确 |
| 第一批 | Recovery A 释放，B 同节点组合重新创建，延迟 A done | A 不 apply、不删除 B marker；B 成功后校验 payload 类型与内容 |
| 第一批 | 新 owner 的所有 FENCE_EXTENT 被阻断，旧 owner 随后恢复 | fence 未完成不 serving；任何 stale write 都无成功 ACK |
| 第一批 | 正在恢复的 target 被 Fence/Remove，df done 同时到达 | 节点删除和完成应用没有 dangling node/disk 引用 |
| 第一批 | 同 leader 延迟 etcd apply，期间 release/reacquire marker | marker nonce、extent 版本 CAS 生效；内存与 etcd 一致 |
| 第一批 | T1 checker 对一个 GET 正常、PUT 全失败的 PS | 必须实际探测且报错，不得 0 probe 通过 |
| 第一批 | EN DELETE 永远拒绝，manager metadata 已删 | 物理回收 checker 必须失败，并指出残留文件/重试项 |
| 第二批 | Remove 事务成功但响应丢失；manager 随后换 leader | 重试得到终态成功，tombstone 保留，旧 UUID 重注册拒绝 |
| 第二批 | Compact 输出一半 / SST durable / checkpoint 前后分别 SIGKILL | 重启后所有 ACK 值及 tombstone 正确，未发布输出最终回收 |
| 第二批 | GC relocation durable、punch 前后分别杀 PS/manager | 旧新 VP 至少一条可恢复链，refs 与 memberships 正确 |
| 第二批 | 同 EN 一块盘 EIO/EACCES，其他盘继续写 | 仅坏盘 slot 重建；节点不被误判整机退出；健康盘不迁移 |
| 第二批 | 恢复/压缩目标盘 ENOSPC，释放空间后重试 | 不假成功、不永久占 marker；重试收敛，无残留半成品被 adopt |
| 第二批 | 2/3 replica loss、EC 剩 K 与 K-1、最后一份腐化 | 容错内恢复；超容错明确报错，不能伪装 NotFound/健康 |
| 第二批 | PUT/DELETE/TTL、同 key 多 writer、timeout 后再写 | 历史允许未知结果，但任何已 ACK 的顺序约束不能被抹掉 |
| 第三批 | 0/1 byte、VP 阈值两侧、WAL 跨 chunk、EC stripe 边界、16 GiB extent | 精确长度与 checksum，内存有界；不以调小 chunk 替代全部大尺寸验证 |
| 第三批 | 真跨主机 TCP/UCX，网络单向故障 × PS failover × Recovery | 固定时序与随机 seed 都留完整历史，明确实际走到的传输路径 |

## 6. 本轮实际验证与限制

### 已执行

1. 读取当前产品调用链、相关测试、CI、chaos 脚本及仓库故障记录；引用均以本次 HEAD 为准。
2. 源码 classify_recovery_completion 的原样提取探针：旧 done + 新 marker 同 assignment → Apply。
3. namespaced range 与 liveness 候选枚举：三个范围均为 0 候选。
4. 独立 Cargo 探针直接依赖当前 manager/stream/rpc 源码，绕开 manager 测试的 FUSE dev-dependency：
   - Remove 带活 Recovery target 返回 CODE_OK，marker 留存。
   - 同 Remove 重试返回 CODE_PRECONDITION。
   - 无 spare 的 force=false Fence 返回 CODE_OK。
5. 原始测试文件通过独立 manifest 运行：node_lifecycle **9/9 通过**；delete_recovery_race **1/1 通过**。没有改写原测试断言。
6. 真实 EN 短读探针 **1/1 通过**，此处“通过”表示确认当前缺陷前提：durable sealed 8192 B，截短并重新 load 后，普通 read 回 CODE_OK/0 B。

关键原始输出：

~~~text
OLD_DONE_SAME_ASSIGNMENT_NEW_ATTEMPT: Apply
LIVENESS_PROBE_CANDIDATES b'mem/a' b'mem/z' 0
REMOVE_WITH_LIVE_RECOVERY: code=0 ext_blockers=[] marker_blockers=[] target_exists=false
RECOVERY_MARKER_AFTER_REMOVE: Err(Precondition("extent 42 already has an in-flight op (in-memory)"))
REMOVE_RETRY_AFTER_SUCCESS: code=3 message=node 3 must be Fenced before remove
FENCE_NO_SPARE_FORCE_FALSE: code=0 message=
SEALED_TRUNCATED_READ: code=0 requested=8192 bytes=0 end=0
~~~

探针、独立 manifest 与提取源码保留在本机 /private/tmp/autumn-review-deuwr8gx；临时目录不是长期测试交付物。可复查：

~~~sh
cargo run --manifest-path /private/tmp/autumn-review-deuwr8gx/Cargo.toml --offline --target-dir /Users/dongmao.zhang/upstream/autumn-rs/target
cargo test --manifest-path /private/tmp/autumn-review-deuwr8gx/Cargo.toml --offline --target-dir /Users/dongmao.zhang/upstream/autumn-rs/target --test review_node_lifecycle --test review_delete_recovery_race --test review_short_read -- --nocapture --test-threads=1
/private/tmp/autumn-review-deuwr8gx/identity
~~~

### 未完成的验证

- 原始 cargo test -p autumn-manager --lib recovery_completion --offline 编译失败：fuser 需要 pkg-config/FUSE，本机缺失；不是测试断言失败。
- H200-1 只读连接尝试在跳板机遭到 Permission denied (gssapi-with-mic)，未进入 dongmao-autumn，也未在远端运行测试或修改集群。
- 本机没有 toxiproxy-server，本轮未运行完整 system_chaos，也没有运行有 loop mount/清集群行为的 ENOSPC 脚本。
- 独立 manifest 引用的是当前产品源码，首次复制 workspace lock 后为较小依赖图重新解析，部分传递依赖版本有变化；它用于验证具体代码分支，不能冒充完全相同 workspace lock 的 CI 运行。
- 没有做掉电、真实 media EIO、UCX 或跨主机故障验证；没有声称已用完整 e2e 复现 R1–R5 的最终数据损失。

**验收建议：先关闭 R1–R5 和 T1–T2，再以实际故障进入次数、重启后的 ACK 数据、slot/refs/membership 一致性及逐副本物理回收为门槛评估 chaos。单纯增加 seed 数或延长运行时间不能修复 checker 的空验证。**
