# autumn-fs 多客户端一致性：inode lease

**关联代码**: `crates/manager/src/inode_lease.rs`（lease 注册表 + revoke loop）·
`crates/manager/src/rpc_handlers.rs`（4 个 handler + long-poll）·
`crates/client/src/lease.rs`（SDK 客户端）·
`crates/fuse/src/lease_tasks.rs`（per-session 心跳 / poll / 驱逐后台任务）·
`crates/fuse/src/key.rs`（fs key schema）
**关联记忆**: [[project_three_interfaces]]、[[project_fs_unify_complete]]

---

## 1. 一致性语义

同一棵 fs 树可以被多个前端同时打开：内核挂载 `autumn-fuse`、CLI `autumnfs`、
PyO3 `autumn.Fs`（`autumn_vllm_loader` 走这条）。manager 的 inode lease 给这些
前端下列语义：

| 场景 | 语义 |
|------|------|
| 多客户端并发读同一文件 | 安全：读 lease 可多持有者共存 |
| 多客户端并发写同一文件 | 单写者：第二个 `acquire(WRITE)` 返回 Conflict |
| writer 写 → reader 读（不同进程 / 不同主机） | close-to-open：writer release 后立即可见 |
| 同一 mount 内部并发 | 安全（单线程 compio） |
| 不同前端并跑同一 ino | 同一套 lease/invalidation 覆盖——manager 不区分客户端家族 |

非目标（明确不做）：
- POSIX byte-range locking（`fcntl(F_SETLK)`）
- 同文件并发**写者**的 chunk-slice 合并（JuiceFS 用 slice 模型；这里走 single-writer-lease）
- 跨集群联邦
- 原子 rename：`dir::rename` 是多次无事务 KV 操作，lease 不改变这一点

## 2. 为什么 lease 长在 manager 里

manager + etcd 已经承担 JuiceFS 里那个**外置** metadata 服务的角色，所以
inode lease 直接复用 manager 的 etcd lease 原语（与 stream 层 `acquire_owner_lock`
同源），不引入第二个元数据系统、也不引入 Redis。所有前端都只是该 manager 的
lease 客户端；manager 对客户端家族一视同仁（`kind` 只是诊断标签），这正是
"多种前端并跑同一文件"能成立的原因。

lease 状态全部在 **manager 进程内 + etcd**，**不在数据 KV 里加键**——fs key
schema（`fs/` namespace 下 `0x01` InodeMeta / `0x02` DirentValue / `0x03` extent /
`0x04` superblock）不因 lease 而改动。

## 3. 数据模型

### 3.1 InodeLeaseState

```rust
pub struct InodeLeaseState {
    ino: u64,
    writer: Option<ClientKey>,               // 至多一个写者
    writer_diag_host: String,                // 诊断用
    writer_expires_at: Option<Instant>,
    readers: BTreeMap<ClientKey, Instant>,   // 读者可与写者共存
    version: u64,                            // writer close / revoke 时 +1
    pending_revoke_at: Option<Instant>,      // force-acquire 的 grace 窗口
}
```

- **写者 lease 持久化**在 etcd `inode_leases/{ino}`（leader-fenced）；**读者 lease
  只在内存**——failover 后读者集合丢失是良性的（客户端重连即整体失效缓存）。
- TTL = `DEFAULT_LEASE_TTL_SECS` 30s；客户端每 5s 心跳续约（`lease_tasks.rs`）。
- force-revoke 的宽限期 `DEFAULT_REVOKE_GRACE` = 5s；revoke 扫描周期
  `REVOKE_TICK` = 1s；每客户端 inbox 上限 `MAX_INBOX_EVENTS` = 1024。
- `version` 在 inode 条目的**整个生命周期**内单调（`last_version` 影子值跨
  remove/re-create 保住 high-water mark），所以重新 acquire 绝不会把某个陈旧
  reader 缓存正持有的 `(ino, version)` 再发一次。

### 3.2 MgrClientId

```rust
pub struct MgrClientId { kind: u8, uuid: [u8; 16], host: String }
```

`kind` ∈ {`LEASE_CLIENT_KIND_FUSE` = 1, `LEASE_CLIENT_KIND_IORING` = 2}，**仅供
`autumn-op` 标注哪个前端持有 lease**。lease 身份是 `(kind, uuid)`；`host` 是纯诊断
字段，manager 从不比较它。`uuid` 在客户端进程启动时生成一次，之后每个 lease RPC
复用（断线重连不换）。

## 4. 协议（4 个 manager RPC，`0x46`–`0x49`）

### 4.1 Acquire（`MSG_ACQUIRE_LEASE` = 0x46）

```
Client → Manager: AcquireLeaseReq { client, ino, mode: READ|WRITE, force }
Manager:
  mode == WRITE 且 writer 被别人持有:
      force == false → WriteConflict（客户端映射成 EBUSY / EAGAIN）
      force == true  → 首次推 WillRevokeIn 给现持有者并起 grace 窗口，
                       返回 RevokePending { eta_ms }；grace 到期后的
                       force-acquire 强收（version+1，推 LeaseRevoked）再授予
  mode == READ  → 加入 readers 集合
→ AcquireLeaseResp { code, lease: MgrInodeLeaseInfo { version, writer_present, ttl_secs } }
```

客户端把返回的 `version` 当作缓存的"代"（`InodeState.cached_version`）：不符
即丢弃缓存的 `InodeMeta` / extent map 重建。

### 4.2 读路径

客户端**不是**每次 read 都问 manager 要 version。它依赖 §4.5 的 invalidation
long-poll；`cache_is_stale(ino, lease_epoch, inv)` 用 poll 收到的事件判定缓存是否
被超越，没有事件就认为缓存有效。

### 4.3 Release（`MSG_RELEASE_LEASE` = 0x47）

```
Manager:
  writer == 调用者 → writer = None; version += 1;
                     推 WriterClosed 给该 ino 的所有 readers   ← close-to-open 触发点
  否则            → readers.remove(调用者)（幂等：没持有返回 NotHeld）
```

写者在 ReleaseLease **之前**排干脏写缓冲（`flush_inode`）落 KV，所以 reader 收到
invalidation 时新数据已经在 KV 里。

### 4.4 Heartbeat（`MSG_HEARTBEAT_LEASE` = 0x48）

续约成功返回 `Renewed{version, writer_present, ttl_secs}`；`NotHeld` 表示
manager 已 revoke / 过期该 lease，客户端必须丢缓存并重新 acquire。

### 4.5 Invalidation 通道（`MSG_POLL_INVALIDATIONS` = 0x49）

autumn-rpc 是 req/resp 模型（无 streaming RPC），所以推送做成**服务端 long-poll**：
handler 先排空该客户端 inbox，空则挂起最多 `LONG_POLL_WAIT` = 10s 等事件；
`ClientInbox::push` 在返回前唤醒挂起的 waker，所以"writer close → reader 看见新
字节"是毫秒级而不是一个 poll 周期。每个 inbox 至多挂一个 waker，被顶掉的那个
long-poll 以 `Canceled`（= 无事件，重试）收场。

事件 `MgrInvalidation { ino, version, kind }`，`kind`：

| 常量 | 值 | 含义 |
|---|---|---|
| `LEASE_INVAL_WRITER_CLOSED` | 1 | 写者释放，version 已自增 |
| `LEASE_INVAL_LEASE_REVOKED` | 2 | manager 强收 lease（TTL 过期 / 被抢占） |
| `LEASE_INVAL_META_CHANGED` | 3 | **溢出哨兵**：`ino == 0` 表示 inbox 溢出，客户端须整体失效 |
| `LEASE_INVAL_WILL_REVOKE_IN` | 4 | 推给当前写者：有人 force-acquire，`version` 字段复用为宽限毫秒数 |

## 5. 异常与恢复

| 情况 | 处理 |
|------|------|
| Writer 没续约 → TTL 过期 | manager 自动 revoke writer，version +1，推给 readers |
| Reader 没续约 → TTL 过期 | 静默从 readers 集合移除 |
| Manager failover | 新 leader 从 etcd 重装写者 lease（`install_persisted_writer`，deadline 按 TTL 夹紧防时钟漂移）；读者集合丢失是良性的 |
| 客户端 poll 通道断开 / 收到溢出哨兵 | 客户端**保守**：丢弃全部持有的 lease + 缓存，下次读重新拉 |
| force-revoke 的 etcd 持久化失败 | `acquire_with_force_deferred` 把推送暂存，持久化失败即丢弃暂存并 `revert_writer_acquire` 回滚内存态——客户端绝不会看到"没真正发生的 revoke" |

`tick(now)` 的顺序是**先捕获 readers 集合再驱逐过期 readers**（writer revoke →
reader expiry → drop-empty-inode → push），所以卡在写者 TTL 边界过期的 reader 仍
能收到推送。

## 6. 不变量与代码约束

1. **manager 是唯一 lease 决策者**。客户端不本地决定"我先写"——写路径先 Acquire，
   `acquire` 在别人持写者槽时同步返回 `WriteConflict`。
2. **写者的 ReleaseLease 排在脏缓冲 flush 之后**。反过来 readers 会看到 version+1
   却读不到新数据。同理 manager 侧 `version` 自增排在推送**之前**，reader 拿到的
   事件与新代号总是配对的。
3. **客户端缓存一律按 version 标记**。extent map / write buffer / inode meta 的每份
   缓存都带 `cached_version`；invalidation 到达一票否决。
4. **poll 通道断线 = 失效全部 cache**。不做"乐观保留"——部分失效是脚枪；溢出哨兵
   （`MetaChanged` + `ino == 0`）走同一条整体失效路径。
5. **续约失败 = 自我 revoke**。`heartbeat` 返回 `NotHeld` 的客户端不得假装还持有，
   否则会与新写者对撞。被 revoke 的租约上的写操作快失败（`autumn.Fs` 与 mount 都
   有这道门），PS 侧另有 `(inode_hint, lease_epoch)` 的 fence floor 兜底。
6. **写租约在第一次 Put 之前拿到**。没有"先写后申请"的乐观协议——ValuePointer /
   extent 结构没有 CAS 可供事后回退。无租约的匿名写不参与围栏（`WriteLease::ANON`，
   `lease_epoch = 0`），单前端场景照常工作。

消费端还有两条派生规则：cache-stale 的读**必须先重载 extent 再服务**，否则返回
EIO——绝不能拿 pre-close 的字节应付。
