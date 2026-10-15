# autumn-fs 多客户端一致性：inode lease — 设计

**代码**: `crates/manager/src/inode_lease.rs`（lease 注册表、TTL revoke）·
`crates/manager/src/rpc_handlers.rs`（4 个 handler + long-poll）·
`crates/client/src/lease.rs`（SDK）·
`crates/fs/src/lease_tasks.rs`（per-session 心跳 / poll / 驱逐）·
`crates/partition-server/src/lib.rs`（`FenceFloors`，PS 侧写围栏）

## 1. 语义

同一棵 fs 树可被多个前端同时打开：内核挂载 `autumn-fuse`、CLI `autumnfs`、
PyO3 `autumn.Fs`、`autumn-s3` 网关。它们都是 manager 的 lease 客户端，manager
不区分客户端家族（`kind` 只是诊断标签），所以多种前端并跑同一 ino 由同一套
lease / invalidation 覆盖。

| 场景 | 语义 |
|------|------|
| 多客户端并发读同一文件 | 安全：读 lease 可多持有者共存 |
| 多客户端并发写同一文件 | 单写者：另一客户端的写请求返回 Conflict（mount 映射为 EBUSY） |
| writer 写 → 别处 reader 读 | close-to-open：writer release 后立即可见 |
| 删除仍被别处打开的文件 | 数据回收等到没有其他持有者（EXCLUSIVE） |
| S3 GET 期间被覆盖 | GET 读完旧内容（STABLE + REPLACE） |

非目标：POSIX byte-range lock（`fcntl(F_SETLK)`）；同文件多写者合并；原子
rename（`dir::rename` 是多次无事务 KV 操作，lease 不改变这一点）。

## 2. 为什么在 manager 里

manager + etcd 已是集群元数据服务，inode lease 复用它（与 stream 层
`acquire_owner_lock` 同源），不引入第二个元数据系统。lease 状态只在 manager
内存与 etcd `inode_leases/<ino>`，不往数据 KV 加键，fs key schema 不因 lease 改变。

## 3. 状态

每个 ino 一个 `InodeLeaseState`：至多一个**写者槽**（`writer` + `writer_kind`）、
`readers`、`stable`（钉住内容的读者）、单调的 `version`、force-acquire 的
`pending_revoke_at`。

- **身份** `MgrClientId { kind, uuid, host }`：lease 键是 `(kind, uuid)`；`uuid`
  每个 `FsState` 生成一次（`DaemonClientId::new_fuse`），会话内不变。一个 fuse
  挂载、一个 PyO3 `autumn.Fs` 各是一个客户端；autumn-s3 每个工作线程一个
  `FsState`，一个网关进程就是多个客户端。所有前端上报的 kind 都是 FUSE；`host`
  只用于诊断，manager 从不比较。
- **version** 从 1 开始，写者 release 或被 revoke 时 +1。`last_version` 影子值跨
  条目删除 / 重建保住 high-water mark，重新 acquire 绝不重发陈旧缓存正持有的
  `(ino, version)`。
- **持久化**：只有 `WRITE` 写者写 etcd（leader-fenced）。release 与 TTL revoke
  先删 etcd 再改内存；acquire 先改内存再写 etcd，etcd 失败则
  `revert_writer_acquire` 回滚。读者、
  STABLE、REPLACE、EXCLUSIVE 只在内存——failover 后读者集合丢失是良性的（客户端
  重连即整体失效缓存）；回放的写者记录按 `WRITE` 恢复。
- **时间**：TTL `--inode-lease-ttl-secs`（默认 30 s）；客户端每
  `--lease-heartbeat-secs`（默认 5 s）续约所有持有的 lease（fuse、PyO3、
  autumn-s3 起这个后台任务；`autumnfs` CLI 是一次性命令，不续约）；force-acquire 宽限
  `--inode-revoke-grace-ms`（默认 5 s）；revoke 循环每 1 s 一次；每客户端 inbox 上限
  1024 事件。

## 4. 模式

| 模式 | 用途 | 与**其他客户端**的持有共存规则 |
|---|---|---|
| `READ` (1) | 读 | 只被 EXCLUSIVE 拒绝 |
| `WRITE` (2) | 原地修改 | 占写者槽；拒绝于他人写者或 STABLE |
| `STABLE` (3) | S3 GET 钉住内容 | 拒绝于他人 WRITE / EXCLUSIVE；与 REPLACE 共存 |
| `REPLACE` (4) | S3 覆盖：把 inode 从名字上换下 | 占写者槽；只排斥其他写者，读者与 STABLE 继续 |
| `EXCLUSIVE` (5) | 回收 inode 数据（unlink 后） | 占写者槽；任何他人持有都拒绝，持有期间拒绝一切新 lease |

同一客户端可同时持读与写（ffmpeg faststart 写 fd + 读 fd），冲突只看**其他**客户端。

回收路径：`reclaim_unreachable` / `reclaim_live` 取 EXCLUSIVE，拿不到就把墓碑留给
周期扫描；拿到才删，删除以该 lease 的 epoch 围栏。`reclaim_unreachable` 在各阶段
之间重新 acquire 以再确认独占。

## 5. 协议（manager RPC `0x46`–`0x49`）

**Acquire（`MSG_ACQUIRE_LEASE`）** `{ client, ino, mode, force }` →
`{ code, lease: { version, writer_present, ttl_secs } }`。冲突（内部 `WriteConflict`
/ `HolderConflict`）在线上都是 `CODE_PRECONDITION`。`force` 只对「WRITE 抢 WRITE」生效：首次推 `WillRevokeIn` 给
现持有者并开宽限窗口，返回 `CODE_REVOKE_PENDING`（`eta_ms` 放在 `lease.ttl_secs`）；窗口内写者自愿 release 则
下一次 acquire 直接授予；过期后的 force-acquire 强收（version+1，推 `LeaseRevoked`）
并授予。REPLACE / EXCLUSIVE 持有者从不被抢；STABLE 读者从不被抢。抢占的推送先暂存，
etcd 提交后才投递；提交失败则丢弃暂存并 `revert_writer_acquire` 回滚内存——客户端
看不到未发生的 revoke。

**Release（`MSG_RELEASE_LEASE`）** 写者 release：清写者槽、version+1、向该 ino 所有
readers 推 `WriterClosed`（close-to-open 触发点）。读者 / STABLE release 不改 version；
未持有返回 NotHeld（幂等）。

**Heartbeat（`MSG_HEARTBEAT_LEASE`）** 续约返回 `Renewed { version, … }`；NotHeld
表示已被 revoke 或过期，客户端丢弃本地 lease 与缓存。

**PollInvalidations（`MSG_POLL_INVALIDATIONS`）** RPC 是 req/resp，推送做成服务端
long-poll：先排空 inbox，空则挂起至多 10 s；`push` 唤醒挂起的 waker，所以「writer
close → reader 看见」是毫秒级。每 inbox 至多一个 waker，被顶掉的 poll 以无事件返回。

事件 `MgrInvalidation { ino, version, kind }`：

| kind | 值 | 含义 |
|---|---|---|
| `WRITER_CLOSED` | 1 | 写者 release，version 已自增 |
| `LEASE_REVOKED` | 2 | manager 强收（TTL 过期 / 被抢占） |
| `META_CHANGED` | 3 | `ino == 0` 为 inbox 溢出哨兵：客户端整体失效 |
| `WILL_REVOKE_IN` | 4 | 推给现写者：有人 force-acquire，`version` 字段为宽限毫秒数 |

## 6. 异常与恢复

| 情况 | 处理 |
|------|------|
| 写者 TTL 过期 | 两阶段：先删 etcd 记录，再内存 revoke（version+1）并推给 readers |
| 读者 / STABLE TTL 过期 | 静默移除 |
| manager failover | 新 leader 从 etcd 重装写者（`install_persisted_writer`，deadline 按 TTL 夹紧） |
| poll 失败 / 溢出哨兵 | 客户端丢弃全部持有的 lease 与缓存 |

`tick` 先处理写者 revoke 再驱逐过期 readers，所以卡在写者 TTL 边界过期的 reader 仍能
收到推送。

## 7. 不变量

1. **manager 是唯一的 lease 决策者**；写路径先 acquire，冲突同步返回。
2. **写者先 flush 脏缓冲再 release**；manager 先自增 version 再推送——reader 收到
   事件时新数据已在 KV，事件与新 version 配对。
3. **客户端缓存按 version 标记**；invalidation 一票否决，cache-stale 的读先重载
   extent 再服务，否则返回 EIO，绝不用 close 前的字节应付。
4. **poll 通道断开 = 失效全部缓存**，溢出哨兵走同一路径。
5. **续约失败 = 自我 revoke**；被 revoke 的 lease 上的写快失败。
6. **写 lease 在第一次 Put 之前拿到**。每次写带 `(inode_hint = ino, lease_epoch =
   version)`；PS 按 ino 维护 fence floor：`lease_epoch < floor` 拒绝为 `CODE_FENCED`，
   更高的 epoch 抬高 floor 并在 ACK 前写 `OP_FENCE_BUMP` WAL 记录（checkpoint +
   回放恢复）。无 lease 的匿名写（`WriteLease::ANON`，`lease_epoch = 0`）不参与围栏，
   单前端照常工作。
