# autumn-rs 全仓代码审查

审查日期：2026-10-19 至 2026-10-20（America/Los_Angeles）。基线：`346dc802729a348b8e10d463b38c5f662d2b96a5`，分支 `main`。

**结论：发现 7 项当前实现问题，其中 5 项 P1、2 项 P2。全部取得了运行证据，但证据强度不同：3 项通过正常公开 API 复现，1 项用现有事务暂停点复现，1 项重建崩溃边界的持久状态后调用真实恢复逻辑，1 项设置 standby 状态后验证真实 RPC 响应，1 项直接验证解码器分配行为。应优先修复流式覆盖、租约旧事务和文件命名/恢复问题。**

已有 9 月报告中的 GC 完整扫描、Recovery nonce/CAS、takeover fence 和 Remove 保护已经补齐，不能继续将旧 R1–R7 作为本基线的未修复缺陷。本报告替代旧的当前结论；旧报告全文可用 `git show 346dc802:review.md` 查看。旧编号对照保留在第 5 节。

本次交付是审查报告，未修复产品实现。审查通过 CLI 完成，测试只启动本地临时集群和 etcd，没有操作线上集群。

## 1. 问题总表

P1：数据完整性、持久状态正确性或正常操作造成命名空间丢失，优先修复。P2：条件触发的可用性/资源风险或控制面契约错误。严重性不等于“已发生线上事故”。

| 编号 | 等级 | 问题 | 本轮证据 | 状态 |
|---|---|---|---|---|
| N1 | P1 | SDK 流式覆盖复用旧 chunk key，未 commit 即改变旧值，abort 删除旧值 | 两个客户端、真实 PS/EN，公开 API 复现 | 当前缺陷 |
| N2 | P1 | 同一 leader 任期内，迟到的 AcquireLease 事务覆盖后续 writer | 真实 etcd，既有暂停点，直接调用真实 handler | 当前缺陷 |
| N3 | P1 | create/mkdir 先检查后无条件写，同名并发创建出不同 inode | 两个 FsState 并发 create，均成功且 inode 不同 | 当前缺陷 |
| N4 | P1 | rename 不检查目标目录非空，成功移除整个目标子树的入口 | 真实目录和文件，经共享 FS API 复现 | 当前缺陷 |
| N5 | P1 | Retire 恢复重复递减 nlink，删除仍被另一个硬链接引用的 inode | 重建精确崩溃边界状态，执行真实恢复 | 文档已有记录，本轮坐实 |
| N6 | P2 | CheckCommitLength 没有 leader gate，standby 返回成功元数据 | 同一实例普通查询拒绝、该查询成功 | 从 backlog 推断升级为行为确认 |
| N7 | P2 | RPC 仅收到 10 字节头即按声明长度预留内存，实际上限约 4 GiB | 64 MiB 长度头使容量达到 67,108,874 字节 | 分配行为已确认，未制造 OOM |

## 2. 详细发现

### N1 — [P1] 流式覆盖在 commit 前修改旧版本，abort 破坏已提交数据

**位置：** [client send](crates/client/src/lib.rs#L6607)、[chunk key](crates/client/src/lib.rs#L5911)、[abort](crates/client/src/lib.rs#L6671)、[put_stream_begin](crates/client/src/lib.rs#L5521)。

`make_chunk_key` 只包含 user key 和 chunk index，没有 upload/generation 身份。第二次对同一 key 开始流式写时，`send` 直接覆盖第一次已发布的 chunk。旧 manifest 仍指向这些位置，因此 manifest 的最终 put 不能构成整值原子发布。`abort` 随后删除的也是同一组 key。

**运行结果：**

1. 对 `review-stream` 发送 `AAAA` 并成功 commit。
2. 再次 begin，发送 `BBBB`，尚未 commit。
3. 另一个客户端 `get_stream().next_chunk()` 已返回 `BBBB`。
4. 第二次上传 abort 后，旧 manifest 仍存在，读取报 `striped chunk 0 missing`。

全程没有故障注入、并发调度要求或进程崩溃；顺序操作就能复现。多 chunk 的覆盖还可能暴露新旧混合内容。影响的是 `ClusterClient::put_stream_begin`/`get_stream` 和使用它们的入口；S3 的 `autumn_fs::multipart` 是另一套实现，本项不套用到它。

**建议：** 每次上传使用独立 generation/object ID，chunk key 与 manifest 同时携带该身份；新 manifest 发布之前不触碰旧 generation；abort 只清理本次上传。旧 generation 回收还需考虑已开始的 reader，不能在换 manifest 后立即删除其仍在读的内容。

**回归验收：** 在旧值已提交时开始覆盖，逐 chunk 暂停，另一个 reader 始终读到完整旧值；abort 后旧值仍完整；commit 后新 reader 读到完整新值；两个 uploader 交错不能混合 chunk。既有 `system_putstream` 的新 key roundtrip/abort 场景不覆盖本例。

### N2 — [P1] AcquireLease 只有 leader fence，没有 inode 版本 CAS

**位置：** [handle_acquire_lease](crates/manager/src/rpc_handlers.rs#L7386)、[无条件 put helper](crates/manager/src/lib.rs#L593)、[fenced_txn](crates/manager/src/lib.rs#L398)。

handler 在内存中先获得 writer 并捕获 record，再通过 `put_msgs_txn` 持久化。事务只检查 leader key；同一任期内其他请求对该 inode 的更新不会让旧事务失效。事务返回成功后，handler 也不重新检查 inode 的 writer/version。

**使用真实 etcd 和现有 `FENCED_TXN_TEST_HOLD_*` 暂停点复现的时序：**

1. W 的第一次 acquire 已更新内存，在发送 etcd put 前暂停。
2. W 重试 acquire，成功持久化并收到成功响应。
3. W release 成功。
4. X acquire 成功，etcd 中 writer 为 X，版本 `1099511627777`。
5. 恢复 W 的第一次事务：仍返回成功，etcd 被写回 W，版本降为 `1099511627776`。

本例不需要换主、force-preempt 或直接修改 etcd 记录。暂停点只控制真实事务的发送顺序。已确认的是持久记录被旧请求覆盖；本轮没有继续运行“换主重放 → 文件数据损失”的端到端链路。根据 replay 安装持久 writer 的路径，后续换主会恢复错误持有者，现有 PS fence 可能表现为旧 writer 被拒写，而不能据此说这个持久记录错误无害。

**建议：** 对每个 inode 的 lease grant/release/expiry 引入一致的条件提交协议：比较操作所属 writer/version/记录 revision；事务后内存提交或回滚同样验证身份。也可以选择只串行化同 inode 的生命周期操作，但不应全局阻塞所有 inode。仅加 leader 任期检查不能解决本例。

**相邻未独立复现的窗口：** `handle_release_lease` 的 delete、`inode_lease_revoke_loop` 的批量 delete 也缺少 writer-record compare；`revert_writer_acquire` 在同任期内仍可能恢复已过时的快照。它们属于同一修复范围的待验证分支，不另计已复现问题。近期 `refresh_writer_record` 修复仅保护 heartbeat，未覆盖 grant。

**回归验收：** 原样运行上述时序，X 的 durable record 与内存 holder 始终一致；旧事务不得成功复活 W；再换主确认 X 仍有效。同时覆盖响应延迟、提交前延迟、失败回滚、release 与 revoke 的交错。

### N3 — [P1] 同名 create/mkdir 缺少条件发布

**位置：** [create](crates/fs/src/dir.rs#L298)、[mkdir](crates/fs/src/dir.rs#L252)；入口：[FUSE Create](crates/fuse/src/dispatch.rs#L569)、[Python create](python/src/fs.rs#L356)。

两个客户端均可在 `kv_exists` 时看到名字不存在，分别分配 inode，再用无条件 `kv_put(dirent)` 互相覆盖。新 inode 的文件租约无法保护此竞争，因为竞争发生在父目录的同一个名字上，而两次创建的 inode 不同。此外，`kv_exists(...).await.unwrap_or(false)` 把检查失败当作不存在，遇到读失败也可能覆盖既有名字。

**运行结果：** 两个独立 `FsState` 用 `futures::join!` 对同一 parent/name 执行 `dir::create`，分别返回 `Ok(2)` 与 `Ok(1002)`；两者都以为创建成功，目录只能保留最后一个 inode。后续对失去名字的 inode 写入可以成功，但无法通过预期路径找到。复现覆盖 create；mkdir 的相同先读后写模式由代码确认。

**建议：** 用 create-only CAS 发布 dirent，并对结果未知的情况 read-back 判定；拒绝把存储错误转换成不存在。竞争失败的 inode 需要可靠清理，父目录 nlink/mtime 更新也需避免丢失更新。`publish::ensure_dirs` 已有条件创建，可作为局部参考，但不等于解决父目录全部一致性问题。

**回归验收：** 多会话竞争同名 create/mkdir 只能有一个新 inode 发布成功；其他调用按 API 语义返回存在冲突或复用同一 inode；注入 existence-read 错误不得覆盖已有目录项；同时检查落败 inode/数据是否泄漏。

### N4 — [P1] rename 覆盖非空目录时不拒绝

**位置：** [rename 目标检查及替换](crates/fs/src/dir.rs#L465)。

目标检查只处理“与源为同一个 inode”和“目标是非目录时需回收”。目标是目录时，没有检查其是否为空，也没有完整校验源/目标类型。之后仍删除源名并无条件覆盖目标名。

**运行结果：** 创建 `/srcdir`、`/dstdir/precious` 后，`rename(srcdir, dstdir)` 返回 `Ok(())`。`dstdir` 此后指向源 inode 3，原目标 inode 4 及 `precious` 失去从根目录到达的路径。这是命名空间内容丢失；本轮未声称这些旧 inode 的物理字节已被删除。

此问题在无并发、无故障时即可发生，独立于 crate 文档承认的“rename 非原子”限制。文件/目录类型不匹配的覆盖也应同步检查；本轮运行复现的是非空目录覆盖。

**建议：** 修改任何 key 前完成目标类型、目录非空等语义检查，并让检查结果在实际提交时仍有效。跨客户端情况下，单纯增加一次 `is_empty` 查询仍有 TOCTOU，需要目录级协调或版本条件。

**回归验收：** 非空目录目标、文件覆盖目录、目录覆盖文件均拒绝且源/目标数据不变；空目录合法替换正确维护父目录引用；同 inode rename 仍保持 no-op。

### N5 — [P1，已知未修] Retire 恢复重复减 nlink

**位置：** [retire](crates/fs/src/publish.rs#L433)、[drop_name_of](crates/fs/src/publish.rs#L506)、[恢复 Retire](crates/fs/src/publish.rs#L712)。已有说明：[FS 已知限制](crates/fs/CLAUDE.md#L605)。

正常 retire 先递减旧 inode 的 nlink，之后才删除 pending record。若在两步之间崩溃，或 pending delete 失败后会话结束，恢复看到目录项指向 successor，又调用 `drop_name_of`，把同一次名字移除重复计算。

**运行验证方式及边界：**

- 用真实 publish 和 hardlink API 创建 old inode 的 `object`、`alias` 两个名字。
- publish 新版本到 `object` 后，读取持久旧 inode，确认 nlink 已从 2 变为 1。
- 放回那条本应在崩溃窗口内仍存在的 `PendingOp::Retire`，释放 session lease；这只重建“已减引用、未删 pending”的状态，没有伪造 nlink 或删除 inode。
- 另一个 `FsState` 执行真实 `recover_dead_sessions`：`recovered=1`，`alias_exists=true`，`inode_exists=false`。

本轮是崩溃边界状态复现，没有实际在机器断电/SIGKILL 窗口里抓取状态。结果证明恢复逻辑对该真实可达状态不幂等。现有 `a_retire_whose_swap_lost_leaves_a_linked_inode_alone` 检查的是 swap 未获胜，不是 swap 获胜且引用已减的情况。

**建议：** 将 retire operation ID/应用标记与 inode 引用变更放到同一个条件更新里，让 replay 能判断该次递减是否已应用。仅根据当前 dirent 是否等于 successor 不能判断引用是否已经减过；简单调整两次写的顺序也会产生相反的泄漏窗口。

**回归验收：** 在递减前、递减后、pending 删除前后分别恢复，重复恢复任意次数，`alias` 仍可逐字节读到旧版本；单链接的旧 inode 最终可回收。

### N6 — [P2] standby 可以成功回答 CheckCommitLength

**位置：** [handle_check_commit_length](crates/manager/src/rpc_handlers.rs#L2197)、[dispatch](crates/manager/src/rpc_handlers.rs#L320)。

该 handler 解码后直接使用本地 owner lock、stream、extent 和节点表，没有 `ensure_leader`/适用的路由服务检查。外层 dispatch 也不补这个检查。owner epoch 比较只对冻结的本地 store 有效，不能证明当前元数据仍权威。

**运行结果：** 构建 stream、owner lock 和 sealed-tail 元数据，在同一个 manager 实例 `set_leader(false)` 后通过真实 RPC 查询：

```text
StreamInfo:       code=5 (NOT_LEADER)
CheckCommitLength: code=0 (OK), end=4096
```

这验证 handler 的拒绝契约缺口；sealed length 是测试显式设定的状态，本轮未模拟真实两 manager 换主、roll tail 后的写入损坏。可推导的影响是调用方把 standby 的旧 tail/长度当成功结果，无法触发按 NOT_LEADER 轮换。尤其 sealed 分支不访问 EN 就立即成功。

**建议：** 在读本地 owner/stream 状态前加入与该权威查询语义一致的 leader gate；若允许 leaderless 缓存服务，需要明确其期限与位点安全条件，不能直接无条件放行。

**回归验收：** 真实 leader/standby 集群，在 leader 上 roll tail/推进 owner epoch，standby 对该 RPC 返回 NOT_LEADER，客户端轮换后取得新 tail。修正旧 `etcd_replay_owner_lock_allows_check_commit_length_without_reacquire`：`drop(mgr1)` 只关闭客户端连接，并未停止旧 manager，不能证明 failover replay。

### N7 — [P2] RPC 声明长度在鉴权和 CRC 检查前驱动大块预分配

**位置：** [FrameDecoder::try_decode](crates/rpc/src/frame.rs#L510)、[MAX_PAYLOAD_LEN](crates/rpc/src/frame.rs#L61)、[manager decode 循环](crates/manager/src/rpc_handlers.rs#L250)。

`payload_len` 是 u32，`MAX_PAYLOAD_LEN` 也是 `u32::MAX`，所以当前长度上限检查不会拒绝任何声明。只要拿到 10 字节头，未收到 body、未校验 ctrl 长度/CRC，`reserve(total - len)` 就会按整帧大小申请容量。业务 opcode 和 CLIENT_AUTH 检查在解码之后，不能保护这次分配。

**运行结果：** 给真实 decoder 喂 10 字节、声明 64 MiB payload，`try_decode` 返回“等待更多数据”，buffer capacity 已为 `67,108,874`。本轮没有申请 4 GiB 或主动触发 OOM；reserve 的容量不等于立即驻留同量物理内存，实际是否被 OOM kill 取决于 allocator/OS/cgroup 和后续数据。

业务帧路径需要先完成版本握手；不是说握手本身可以用这个头绕过。完成 Client 角色握手不等于完成业务 token 认证，因此在开放客户端连接的服务上，凭证校验也不能拦住此资源消耗。多连接和持续发送 body 会放大风险。

**建议：** 在 reserve 前按 service/opcode/连接阶段实施实际长度和并发内存预算；未完成业务鉴权的连接使用更小的合法消息上限；分段接收避免单凭未验证头一次申请整帧。上限需容纳现有 recovery 分块等合法大请求，不能随意统一降到一个很小常量。

**回归验收：** 大声明、短 body、非法 ctrl 长度、无 token、并发慢连接均受预算约束；合法 bulk/append/recovery 帧仍通过。断言内存/容量边界，不以“没有 OOM”作为唯一成功标准。

## 3. 覆盖范围及已有保护

这是围绕正确性、崩溃恢复、并发、鉴权和资源边界的跨模块审查，不是逐行形式证明。以下写明实查内容和没有执行的平台验证。

| 模块 | 实查重点 | 结论/边界 |
|---|---|---|
| manager / etcd | leader term、事务不确定结果、lease 生命周期、Recovery/EC apply、Fence/Remove、元数据查询 gate | N2/N6；Recovery 独立 nonce、marker revision 和 extent baseline CAS 已存在；etcd 不盲目重发不确定 txn |
| partition-server | takeover、seal/replay、fence floor、GC 完整扫描、checkpoint/replay floor、SST iterator/compaction 入口、authz gate | 已修旧 R1/R3；定向跑截短副本 GC；本轮未确认新的 KV 层 ACK 数据丢失 |
| stream / EN | append/commit/fence、复制/EC 恢复、checksum、分块复制、直读鉴权、后台循环 | 核心库测试和 Recovery 集成覆盖；未执行断电、真实多盘坏盘和跨机 RDMA |
| client | 写入结果分类、conditional retry、batch 重发、region epoch、直读回退、stripe streaming | N1；条件写遇 OutcomeUnknown 停止重试的保护已存在并有定向测试 |
| fs / fuse / Python FS | create/rename/link、lease slot、publish/recover、segment/multipart、前端调用关系 | N3–N5；共享 FS core 已实跑；本机未执行真实 FUSE mount、Python 扩展构建/模型服务 |
| RPC / transport | 握手/role、frame 长度与 CRC、TCP zerocopy buffer 生命周期、UCX pending/orphan/注册内存路径 | N7；TCP/通用库测试通过；UCX 仅静态检查，不能用 macOS TCP 测试认证其内存安全 |
| S3 / dashboard | S3 发布条件、multipart 状态与清理、listing 结构、管理入口和鉴权边界 | S3 底层 publish/multipart 实跑；未跑 HTTP 全接口和浏览器 UI；边界见下文 |
| params / 启动 / 部署 / CI | 参数范围、PS/EN cpuset 初始化、EN shard 数、Kubernetes requests、测试执行命令与故障模型 | 参数库测试通过；不以源码推断现网 CPU 亲和性或吞吐瓶颈 |

### 已知设计限制，仍需在使用范围中体现

1. **EN 直读只验身份，不验 key grant 范围。** [实现](crates/stream/src/extent_node.rs#L1732) 与 [设计说明](docs/data_plane_authz_design.md#L208) 一致：合法 principal 若知道/猜中 extent 坐标，仍可读取 grant 外数据。这里按明确的现有安全边界记录，不伪装成新发现。若需求是不可信租户隔离，需要带范围和有效期的签名 descriptor 或等价服务端授权。
2. **S3 网关无 SigV4 验证。** [入口说明](crates/server/src/bin/autumn_s3/main.rs#L1) 明确写出忽略 Authorization；不能把后端已启用 authz 等同于每个 S3 caller 各有身份。etcd auth/TLS 也是 backlog 中尚未实现的功能。
3. **rename 仍是跨 key 非原子操作。** [实现](crates/fs/src/dir.rs#L481) 先删旧名再写新名。独立于 N4，崩溃/写失败可使源暂时或永久失去名字。发布 pending 恢复只检查原位置，发布后 rename 走再恢复存在误回收风险；crate guide 已记录，本轮未额外复现该链路。
4. **租约缓存的迟到 NotHeld 风险未消除。** `lease_tasks` 快照只带 ino，处理回复时未核对 lease epoch；backlog 已指出通常 FIFO 连接下不可直接构造，换主/连接轮换才可能形成必要重排。本轮不把静态窗口写成已确认生产缺陷。
5. **UCX 取消后缓冲区留在 orphan 列表。** 代码为避免提前释放而保留 buffer，等 UCX completion 后回收。长期不完成请求的资源上限及真实 RDMA 错误恢复需要实机验证；没有依据宣称出现 UAF，也没有运行证明其长时间 RSS 有界。

### 性能审查结论

本轮没有做 perf benchmark，不给出“CPU 已到极限”或“能达到 270K IOPS”的推断。源码能确认的约束是：PS 默认前台限流是每 partition `30,000 IOPS`，`ps_inflight_cap=8`、`conn_inflight_cap=4`；SDK 的 bulk 选择阈值为 64 KiB。压测结果需要连同运行时 override、连接数和 partition 分布解读。

当前 PS `--cpuset` 是先限制启动线程 CPU mask、子线程继承的进程运行范围，代码没有为每个 partition 固定唯一 core；EN shard 数则直接由 cpuset 大小决定，并对 shard 线程 pin core。这与 Kubernetes `requests.cpu` 的数值是不同配置层。本轮只核对源码，未读取 pod 的实际 affinity/cgroup。

## 4. 测试可信度及验证记录

### 4.1 实际运行

环境：本机 macOS，Rust `1.98.1` / Cargo `1.98.1`，本地 etcd，TCP，`--offline --locked`。初次基础库执行因沙箱拒绝 bind 出现 12 个 `Operation not permitted`；使用允许本地端口的执行环境重跑后全部通过。这些不计为代码失败。

| 基础库 | passed | ignored |
|---|---:|---:|
| client | 63 | 0 |
| common | 15 | 0 |
| etcd | 3 | 0 |
| fs | 27 | 0 |
| manager | 468 | 0 |
| params | 9 | 0 |
| partition-server | 230 | 1 |
| rpc | 88 | 1 |
| stream | 199 | 0 |
| transport | 8 | 0 |
| 合计 | **1,110** | **2** |

命令：

```bash
cargo test --offline --locked --lib --no-fail-fast \
  -p autumn-common -p autumn-params -p autumn-rpc -p autumn-etcd \
  -p autumn-client -p autumn-fs -p autumn-manager \
  -p autumn-partition-server -p autumn-stream -p autumn-transport
```

另运行 11 个既有集成目标，**60 passed、0 failed、0 ignored**；数字包括各目标重复编入的公共 support 测试和 child 入口，不能解释为 60 个独立故障场景。

| 集成目标 | passed |
|---|---:|
| apply_done_atomicity | 6 |
| fence_same_process_reelection | 4 |
| leader_step_down_stale_term | 5 |
| recovery_attempt | 5 |
| system_gc_truncated_replica | 7 |
| system_multipart | 4 |
| system_publish | 7 |
| system_review_remove_retry | 4 |
| system_review_takeover_fence | 4 |
| system_status_connection_reuse | 4 |
| write_outcome_settled | 10 |

命令：

```bash
cargo test --offline --locked --no-fail-fast -p autumn-manager \
  --test apply_done_atomicity --test recovery_attempt \
  --test system_review_takeover_fence --test system_review_remove_retry \
  --test system_gc_truncated_replica --test system_publish --test system_multipart \
  --test system_status_connection_reuse --test write_outcome_settled \
  --test leader_step_down_stale_term --test fence_same_process_reelection \
  -- --test-threads=1
```

审查临时 probe 共运行 7 个 harness test：4 个审查 test（其中一个包含 4 个 FS/SDK 场景）和共享 support 模块附带的 3 个测试。**probe 使用断言确认缺陷存在，所以其绿色结果不是“实现正确”。** 复现源代码保留在附录，未把“断言错误行为”的测试加入正常 CI。

关键原始输出：

```text
PROBE stream: before_commit=[66, 66, 66, 66], after_abort=Err(ServerError("striped chunk 0 missing for key — value may have been partially deleted; call delete_stream + re-upload"))
PROBE create: a=Ok(2), b=Ok(1002)
PROBE rename: result=Ok(()), current=3, source=3, old_target=4
PROBE retire: recovered=1, alias_exists=true, inode_exists=false
PROBE follower: stream_info_code=5, commit_length_code=0, end=4096
PROBE lease: retry=0, successor=0, late=0, stored_old=true, old_epoch=1099511627776, successor_epoch=1099511627777
PROBE frame: received=10, announced=67108864, allocated_capacity=67108874
```

本地完整日志：`/tmp/autumn-review-tests-unrestricted.log`、`/tmp/autumn-review-probes.log`、`/tmp/autumn-review-integration.log`。它们是本机临时证据，关键结果已纳入本文件。

### 4.2 仍需补齐的测试

- `system_gc_chunked_read` 的负载只有约 64 KiB 日志，不 roll tail，随后只是 compact/gc/读回。源码显示它没有建立“存在可 GC 的 sealed log”前置条件，也不检查实际搬迁/punch，不能证明 GC chunk carry 被执行。这一项已在 backlog 中；需要在目标 chunk 路径加可观察门槛，并让移除 carry 逻辑时失败。
- `etcd_replay_owner_lock_allows_check_commit_length_without_reacquire` 用 `drop(RpcClient)` 模拟 kill；旧 manager 仍运行，结合 N6 会形成假阳性。需要真实停止旧 runtime，等待新 leader，明确验证重放后的 owner 状态。
- **T3（沿用旧编号）：** crash/restart 测试应分别记录旧进程/线程退出、checkpoint 位点、重启 pid/runtime 身份。不能把断开客户端连接当作服务崩溃。本轮核实了上述具体反例，没有断言所有历史 T3 用例仍有缺陷。
- **T4（沿用旧编号）：** CI 有普通 workspace unit/integration 门槛，但本轮没有运行全动作长时间 chaos、硬盘故障、断电、跨机 UCX、真实 FUSE mount；不能把本轮 1,110 个基础测试和有限集成目标等价成这些验证。动作应有非零成功/完成计数，历史 checker 应保留已 ACK 写入。

## 5. 旧报告 R/T 编号复核

| 旧编号 | 原问题 | 本基线复核 |
|---|---|---|
| R1 | GC 提前 EOF 后 punch | 已有 `ensure_gc_scan_complete` 及逐块精确长度检查；本轮跑截短副本集成 |
| R2 | Recovery 相同 node/replace 的 ABA | 已校验 `RecoveryAttempt` 和非零 nonce；本轮跑 `recovery_attempt` |
| R3 | takeover fence 全失败后仍 open | 现在三次失败直接返回 Err；本轮跑 selective fence 失败测试 |
| R4 | Remove 忽略 Recovery 目标 | lifecycle 锁、marker/target 身份检查及异常节点取消已存在；相关库测试和 Recovery 集成通过 |
| R5 | Recovery apply 没有 marker/extent CAS | 走 `commit_inflight_extent`，比较 marker revision、extent baseline、目标身份；本轮跑 `apply_done_atomicity` |
| R6 | non-force Fence 不检查合法 spare/容量 | 当前已有合法目标、占用与容量检查；相关 manager unit 通过；未做现网容量模拟 |
| R7 | Remove 丢回复后重试不幂等 | 已有终态处理；本轮跑 manager runtime 重启后的 Remove 重试 |
| T1 | chaos liveness key 不在 namespace，全部跳过 | 已使用 `mem/` 范围内 probe 并检查实际探测/ACK 数；静态复核，未跑完整 chaos |
| T2 | “物理回收”只看 etcd | 已有 `wait_for_physical_reclaim`，检查副本和 sidecar、删除重试状态；静态复核，未跑完整 chaos |
| T3/T4 | 假 crash / chaos 验收不足 | 仍需逐项处理，见第 4.2 节；不机械继承旧报告所有旧行号/判断 |

## 6. 修复顺序与验收要求

1. **先处理 N1/N2：** N1 用 generation 隔离每次上传；N2 为同 inode 的完整租约生命周期建立条件提交。两者修完都需用本报告时序验证修复前失败、修复后通过。
2. **处理 N3/N4：** 共享 FS 命名操作加条件发布和目录语义验证，多会话测试优先；不能只在 FUSE callback 增加本地锁，因为 Python/S3/另一挂载同样会改后端。
3. **处理 N5：** 修复恢复幂等性与持久状态设计，覆盖 hardlink 和多次恢复。把限制写在文档里不能代替保护仍可达的数据。
4. **处理 N6/N7：** 补权威查询 gate 与实际接收内存预算；同时补失效的测试前置条件。
5. **最后补平台验证：** Linux FUSE、真实 etcd failover、UCX/RDMA 和磁盘故障按各自条件执行。任何吞吐结论需记录二进制 revision、cpuset、运行时参数、part 分布和传输模式。

没有在本轮做自动修复、提交或部署。未跟踪的已有恢复工具、examples 和临时浏览器目录未纳入基线发布判断。

## 附录：本轮复现代码

下面的代码在本基线编译运行。保存为临时 `crates/manager/tests/review_probe.rs` 后执行：

```bash
cargo test --offline --locked -p autumn-manager --test review_probe \
  -- --nocapture --test-threads=1
```

需要允许本地 loopback 监听，并在 PATH 中有 etcd。只使用临时目录和动态本地端口。此文件依赖现有 `tests/support`，因此应放在上述位置；全局事务暂停点要求串行执行。**这些断言用于重现现存错误，修复后的回归测试必须改成断言正确行为。** 报告完成后临时测试已移出工作区；本机另留副本 `/tmp/autumn-review-probes.rs`。

<details>
<summary>展开完整 Rust 复现源代码</summary>

```rust
//! Temporary review probes: assertions deliberately describe the observed defects.
mod support;
use support::*;
use std::{ffi::OsStr, time::{Duration, Instant}, sync::atomic::Ordering};
use autumn_rpc::{client::RpcClient, manager_rpc::*};
use autumn_client::lease::{self, DaemonClientId};
use autumn_fs::{dir, key, meta, publish::{self, NewFile, Condition}, schema, state::FsState};

#[test]
fn stale_grant_overwrites_the_successor_in_same_term() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_guard, endpoint) = start_etcd().await;
        let manager = autumn_manager::AutumnManager::new_with_etcd(vec![endpoint.clone()], manager_identity()).await.unwrap();
        let aux = autumn_etcd::EtcdClient::connect(&endpoint).await.unwrap();
        let w = DaemonClientId::new_fuse("old").as_wire().clone();
        let x = DaemonClientId::new_fuse("successor").as_wire().clone();
        let ino = 76543;
        let req = |client: MgrClientId| rkyv_encode(&AcquireLeaseReq {client, ino, mode: LEASE_MODE_WRITE, force: false});
        use autumn_manager::{FENCED_TXN_TEST_HOLD_ARMED as ARMED, FENCED_TXN_TEST_HOLD_PREFIX as PREFIX, FENCED_TXN_TEST_PARKED as PARKED};
        *PREFIX.lock().unwrap() = format!("inode_leases/{ino}");
        let before = PARKED.load(Ordering::Relaxed);
        ARMED.store(true, Ordering::Relaxed);
        let a = manager.clone(); let payload = req(w.clone());
        let old = compio::runtime::spawn(async move { a.handle_acquire_lease(payload).await });
        let deadline = Instant::now() + Duration::from_secs(10);
        while PARKED.load(Ordering::Relaxed) == before {
            assert!(Instant::now() < deadline);
            compio::time::sleep(Duration::from_millis(5)).await;
        }
        let retried: AcquireLeaseResp = rkyv_decode(&manager.handle_acquire_lease(req(w.clone())).await.unwrap()).unwrap();
        assert_eq!(retried.code, CODE_OK);
        let release: ReleaseLeaseResp = rkyv_decode(&manager.handle_release_lease(rkyv_encode(&ReleaseLeaseReq {client: w.clone(), ino})).await.unwrap()).unwrap();
        assert_eq!(release.code, CODE_OK);
        let next: AcquireLeaseResp = rkyv_decode(&manager.handle_acquire_lease(req(x.clone())).await.unwrap()).unwrap();
        assert_eq!(next.code, CODE_OK);
        let k = format!("inode_leases/{ino}");
        let record: MgrInodeLeaseRecord = rkyv_decode(&aux.get(k.as_bytes()).await.unwrap().kvs[0].value).unwrap();
        assert_eq!(record.writer.uuid, x.uuid);
        ARMED.store(false, Ordering::Relaxed);
        let late: AcquireLeaseResp = rkyv_decode(&old.await.unwrap().unwrap()).unwrap();
        let record: MgrInodeLeaseRecord = rkyv_decode(&aux.get(k.as_bytes()).await.unwrap().kvs[0].value).unwrap();
        eprintln!("PROBE lease: retry={}, successor={}, late={}, stored_old={}, old_epoch={}, successor_epoch={}", retried.code, next.code, late.code, record.writer.uuid == w.uuid, record.version, next.lease.unwrap().version);
        assert_eq!(late.code, CODE_OK);
        assert_eq!(record.writer.uuid, w.uuid, "expected to reproduce stale overwrite");
    });
}

async fn publish_bytes(s: &mut FsState, name: &[u8], bytes: &[u8]) -> u64 {
    let mut f = NewFile::begin(s, 1, name).await.unwrap();
    f.write(s, bytes).await.unwrap();
    f.finish(s).await.unwrap();
    let ino = f.ino;
    f.publish(s, Condition::None).await.unwrap();
    ino
}

#[test]
fn filesystem_namespace_and_recovery_probes() {
    let (mgr_addr, n1, n2, _d1, _d2) = setup_two_node_infra(231);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1, n2, 231).await;
        let (log, row, meta_stream) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 23101, log, row, meta_stream, b"", b"\xff\xff\xff\xff").await;
        let ps = pick_addr();
        start_partition_server(231, mgr_addr, ps);
        let mut a = FsState::new(&mgr_addr.to_string()).await.unwrap();
        meta::ensure_root(&mut a).await.unwrap();
        let mut b = FsState::new(&mgr_addr.to_string()).await.unwrap();
        let mut committed = a.client.put_stream_begin(b"review-stream", 0);
        committed.send(b"AAAA").await.unwrap();
        committed.commit().await.unwrap();
        let mut replacement = a.client.put_stream_begin(b"review-stream", 0);
        replacement.send(b"BBBB").await.unwrap();
        let mut reader = b.client.get_stream(b"review-stream", 4096).await.unwrap().unwrap();
        let before_commit = reader.next_chunk().await.unwrap().unwrap();
        assert_eq!(before_commit, b"BBBB");
        replacement.abort().await.unwrap();
        let mut reader = b.client.get_stream(b"review-stream", 4096).await.unwrap().unwrap();
        let after_abort = reader.next_chunk().await;
        eprintln!("PROBE stream: before_commit={before_commit:?}, after_abort={after_abort:?}");
        assert!(after_abort.is_err());
        let (one, two) = futures::join!(dir::create(&mut a, 1, OsStr::new("raced"), 0o644), dir::create(&mut b, 1, OsStr::new("raced"), 0o644));
        eprintln!("PROBE create: a={:?}, b={:?}", one.as_ref().map(|v| v.0), two.as_ref().map(|v| v.0));
        assert!(one.is_ok() && two.is_ok(), "expected two successful creates");
        assert_ne!(one.unwrap().0, two.unwrap().0);
        let (src, _) = dir::mkdir(&mut a, 1, OsStr::new("srcdir"), 0o755).await.unwrap();
        let (dst, _) = dir::mkdir(&mut a, 1, OsStr::new("dstdir"), 0o755).await.unwrap();
        dir::create(&mut a, dst, OsStr::new("precious"), 0o644).await.unwrap();
        let renamed = dir::rename(&mut a, 1, OsStr::new("srcdir"), 1, OsStr::new("dstdir")).await;
        let named = dir::lookup(&mut b, 1, OsStr::new("dstdir")).await.unwrap().0;
        eprintln!("PROBE rename: result={renamed:?}, current={named}, source={src}, old_target={dst}");
        assert!(renamed.is_ok()); assert_eq!(named, src);

        let old = publish_bytes(&mut a, b"object", b"old data").await;
        dir::link(&mut a, old, 1, OsStr::new("alias")).await.unwrap();
        let new = publish_bytes(&mut a, b"object", b"new data").await;
        let old_meta = meta::get_inode_uncached(&mut a, old).await.unwrap().0;
        assert_eq!(old_meta.nlink, 1);
        let session = a.session.unwrap();
        // Exact durable state at the crash boundary: nlink already reduced,
        // retirement record not yet deleted. No fabricated inode contents.
        a.kv_put(&key::pending_key(session, old), &schema::encode_pending(&schema::PendingOp::Retire {parent: 1, name: b"object".to_vec(), ino: old, successor: Some(new)})).await.unwrap();
        lease::release(&a.client, &a.client_id, session).await.unwrap();
        let recovered = publish::recover_dead_sessions(&mut b).await.unwrap();
        let alias = b.kv_get_opt(&key::dirent_key(1, b"alias")).await.unwrap();
        let inode = b.kv_get_opt(&key::inode_key(old)).await.unwrap();
        eprintln!("PROBE retire: recovered={recovered}, alias_exists={}, inode_exists={}", alias.is_some(), inode.is_some());
        assert!(alias.is_some() && inode.is_none());
    });
}

#[test]
fn unvalidated_frame_header_reserves_announced_body() {
    use compio::buf::IntoInner;
    let mut decoder = autumn_rpc::frame::FrameDecoder::new();
    let mut head = [0u8; 10]; head[0] = 1;
    head[6..10].copy_from_slice(&(64u32 * 1024 * 1024).to_le_bytes());
    decoder.feed(&head);
    assert!(decoder.try_decode().unwrap().is_none());
    let cap = decoder.read_window(1).into_inner().capacity();
    eprintln!("PROBE frame: received=10, announced=67108864, allocated_capacity={cap}");
    assert!(cap >= 64 * 1024 * 1024);
}

#[test]
fn follower_returns_success_for_commit_length() {
    let n1 = pick_addr(); let n2 = pick_addr();
    let d1 = tempfile::tempdir().unwrap(); let d2 = tempfile::tempdir().unwrap();
    start_extent_node(n1, d1.path().into(), 241);
    start_extent_node(n2, d2.path().into(), 242);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let addr = pick_addr();
        let m = autumn_manager::AutumnManager::new();
        m._test_disable_background_tasks();
        let serving = m.clone();
        let server = compio::runtime::spawn(async move { serving.serve(addr).await });
        wait_accepting_async(addr, "review manager").await;
        let c = RpcClient::connect_as(addr, autumn_rpc::version_hello::Role::Admin, None).await.unwrap();
        register_two_nodes(&c, n1, n2, 241).await;
        let sid = create_stream(&c, 2).await;
        let info: StreamInfoResp = rkyv_decode(&c.call(MSG_STREAM_INFO, rkyv_encode(&StreamInfoReq {stream_ids: vec![sid]})).await.unwrap()).unwrap();
        let (_, mut extent) = info.extents[0].clone();
        extent.sealed = true; extent.sealed_length = 4096;
        m._test_seed_extent(extent.extent_id, extent);
        let lock: AcquireOwnerLockResp = rkyv_decode(&c.call(MSG_ACQUIRE_OWNER_LOCK, rkyv_encode(&AcquireOwnerLockReq {owner_key: "review-owner".into()})).await.unwrap()).unwrap();
        m.set_leader(false);
        let regular: StreamInfoResp = rkyv_decode(&c.call(MSG_STREAM_INFO, rkyv_encode(&StreamInfoReq {stream_ids: vec![sid]})).await.unwrap()).unwrap();
        let stale: CheckCommitLengthResp = rkyv_decode(&c.call(MSG_CHECK_COMMIT_LENGTH, rkyv_encode(&CheckCommitLengthReq {stream_id: sid, owner_key: "review-owner".into(), owner_epoch: lock.owner_epoch})).await.unwrap()).unwrap();
        eprintln!("PROBE follower: stream_info_code={}, commit_length_code={}, end={}", regular.code, stale.code, stale.end);
        assert_eq!(regular.code, CODE_NOT_LEADER);
        assert_eq!(stale.code, CODE_OK);
        assert_eq!(stale.end, 4096);
        drop(server);
    });
}
```

</details>
