# autumn-fs 多客户端 / 多 daemon 一致性计划

**日期**: 2026-06-05
**状态**: 设计草案（待立项实施）
**关联代码**: `crates/fuse/`、`crates/ioring/`、`crates/manager/`、`crates/fuse/src/key.rs`
**关联记忆**: [[project_fuse_extents_ioring_ucx]]、[[project_three_interfaces]]

---

## 1. 目标

让 autumn-rs 在「**同时跑** `autumn-fuse` + `autumn-ioring-daemon`」以及「**多主机同时挂载** `autumn-fuse`」这两类场景下，保持读写一致性，并且**不引入第二个元数据系统**。

具体一致性语义：

| 场景 | 当前 | 目标 |
|------|------|------|
| 多 mount 并发读同一文件 | ✅ 安全（只读） | ✅ 安全 |
| 多 mount 并发写同一文件 | ⚠️ Last-write-wins，缓存不失效 | ✅ 单写者 + reader cache 失效推送 |
| writer 写 → reader 读（不同进程 / 不同主机） | ⚠️ 缓存最长 30s 才看到 | ✅ close-to-open coherence（writer close 立即对 reader 可见）|
| 同一 mount 内部并发 | ✅ 已安全（单线程 compio） | ✅ 不变 |
| autumn-fuse + autumn-ioring-daemon 并跑同一文件 | ⚠️ 各自 cache 不互通 | ✅ 同一 lease/invalidation 机制覆盖 |

非目标（**Non-goals，明确不做**）：
- POSIX byte-range locking（`fcntl(F_SETLK)`）
- 同文件并发**写者**的 chunk-slice 合并（JuiceFS 用 slice 模型；我们走 single-writer-lease）
- 跨集群联邦
- 强一致 directory rename（保持现有"非原子 rename"限制，独立工作）

---

## 2. 与 JuiceFS 的关键架构差异

| | JuiceFS | autumn-rs |
|---|---------|-----------|
| 元数据后端 | **外置**（Redis / TiKV / 自家 metasrv） | **内置**（manager + etcd，已存在） |
| 数据后端 | 对象存储（S3 兼容） | autumn partition layer |
| Lease 服务 | metadata 服务自己提供 | **复用 manager 的 etcd lease 原语**（`acquire_owner_lock` 同款） |
| Invalidation 通道 | metadata 服务 → 客户端 push | **新加**（manager → fuse/ioring 客户端） |

**核心结论：autumn-rs 已经具备 JuiceFS 那个"metadata 服务"角色（manager + etcd），不需要额外引入 Redis。**所有协调放进 manager，fuse 和 ioring daemon 都作为该 manager 的客户端订阅 invalidation 事件。

---

## 3. 数据模型与不变量

复用已有的 fuse key schema（`crates/fuse/src/key.rs:1-20`），**不动**：
```
0x01 → InodeMeta
0x02 → DirentValue
0x03 → File extent
0x04 → Superblock
```

新增的状态全部在 **manager 进程内 + etcd 持久化**（不在数据 KV 里加键）。

### 3.1 InodeLease — 新增 manager 状态

```rust
struct InodeLease {
    ino: u64,
    writer: Option<ClientId>,        // Some = 单写者；None = 无 writer
    readers: HashSet<ClientId>,      // 当前持读 lease 的客户端集合
    version: u64,                    // 单调，每次 writer close +1（C2O coherence 标记）
    expires_at: SystemTime,          // TTL，writer 异常未续约 → 自动释放
}
```

- 持久化在 etcd `inode_leases/{ino}`，由 manager 序列化（与现有 `regions/` 同形态）
- TTL 默认 30s，客户端持锁期间 5s 心跳续约（同 stream 的 owner_lock）

### 3.2 ClientId

```rust
struct ClientId {
    daemon: DaemonKind,   // Fuse | IoRing
    instance: Uuid,       // 进程启动时生成，断线重连保持
    host: String,         // 诊断用
}
```

manager 不区分客户端来自 fuse 还是 ioring —— 同一套 lease 协议，**这是"两 daemon 并跑"的关键**。

---

## 4. 协议

### 4.1 Open

```
Client → Manager: AcquireLease { ino, mode: Read|Write }
Manager:
  if mode == Write:
    if writer != Some(self): wait (或返回 Conflict 让 client poll)
    else: writer = Some(self), version unchanged
  if mode == Read:
    readers.insert(self)
  return InodeLease { version, writer_present: bool }

Client: 缓存 (ino, version)；用 version 标记后续 cache 项的"代"
```

### 4.2 Read 路径

```
Client.read(ino, off, len):
  if cached_version(ino) == server_version(ino): use cache
  else: invalidate cache, re-scan extents from KV
  fetch missing extents via SDK
```

注意：客户端**不每次 read 都 ping manager 检查 version**。它依赖 invalidation push（4.4）；只在没收到 push 时假定 cache 有效。

### 4.3 Close

```
Client → Manager: ReleaseLease { ino }
Manager:
  if writer == Some(self):
    writer = None
    version += 1    ← close-to-open coherence trigger
    push InvalidateInode { ino, new_version } to all readers in readers set
  else:
    readers.remove(self)
```

Writer close 之前，client 先把所有 dirty buffer 排干（已有的 `flush_inode` 调用）→ 落 KV → 然后才 ReleaseLease。这样 reader 收到 invalidation 时，新数据已在 KV。

### 4.4 Invalidation Push 通道

新增 manager RPC：**`SubscribeInvalidations`**（长连接 / streaming）。

```
Client → Manager: SubscribeInvalidations { client_id }
Manager → Client: stream of InvalidateInode { ino, version, kind }
```

`kind`:
- `WriterClosed` — writer 释放，version 自增
- `LeaseRevoked` — manager 强收 lease（writer 异常 / 抢占）
- `MetaChanged` — inode meta（size / mode）变了，dirent 变了

autumn-rpc 现状是 req/resp 模型，**没有 streaming RPC**。要么扩 autumn-rpc 加 streaming，要么用一个常驻 long-poll TCP 连接（manager 累积事件，客户端 poll 一次拿一批）。**倾向后者**（小改动，autumn-rpc 不动）。

### 4.5 异常处理

| 情况 | 处理 |
|------|------|
| Writer 没续约 → TTL 过期 | manager 自动 revoke writer，version +1，push 给 readers |
| Reader 没续约 → TTL 过期 | manager 静默从 readers 集合移除 |
| Manager failover (leader 切换) | 新 leader 从 etcd 读 lease 状态恢复；客户端 subscribe 重连，根据返回的 current version 决定是否 invalidate 本地 cache |
| 客户端 invalidation 通道断开 | 客户端**保守**：所有 cached inode 失效，下次 read 重新拉 |

---

## 5. 阶段划分

按用户指定优先级：**phase 1 = autumn-ioring-daemon + inode-level lease + close-to-open coherence**，其他阶段排队。

### Phase 1（优先）— ioring daemon 接入 lease

**范围**:
- manager 加 InodeLease 状态 + 4 个 RPC：`AcquireLease`、`ReleaseLease`、`HeartbeatLease`、`SubscribeInvalidations`
- autumn-ioring-daemon 的 `Opcode::Open` 申请 lease（mode 来自 `O_RDONLY` / `O_RDWR`，flag 加到 OpenReq）
- `Opcode::Close` 释放 lease
- daemon 内 `OpenedExtents` 缓存按 (ino, version) 标记；invalidation 到达时丢弃
- daemon 启动开一个常驻 subscribe 连接，事件 → 同一 compio runtime 的 invalidator task

**验收**:
- 两个 ioring daemon 同时跑（不同 host），同时写同一 ino → 后者 AcquireLease 返回 Conflict，应用收到错误
- 一个 daemon 写完 close → 另一个 daemon 同 ino 的 Open 立即看到新 version → cache 失效 → 读到新数据
- daemon 异常 kill → 30s 后 lease 自动 revoke → 第二个能 acquire

**不做**:
- autumn-fuse 接入（Phase 2）
- 跨 daemon 类型混用（fuse + ioring 同 ino 并发）—— Phase 2 完成后自然就能用
- 抢占（先返回 Conflict，让客户端自己决定 retry）

### Phase 2 — autumn-fuse 接入同一套 lease

**范围**:
- fuse 的 `open` / `release` 回调对接 lease（带上 mode）
- fuse 的 `InodeState.extents` 缓存按 version 标记
- fuse kernel attribute cache（`attr_timeout=30s`）失效：收到 push → 调用 `fuser::notify_inval_inode()` 让内核也丢
- 处理 fuse 特有问题：write buffer 在 invalidation 时怎么办（drop 还是 force-flush？ → drop + 错误返给上层，writer 应已先 release）

**验收**:
- 一台 host fuse mount 写文件，另一台 host fuse mount 读 → 写者 close 后立即可见
- fuse + ioring daemon 同时跑同 ino —— writer 切换时正确 invalidate

### Phase 3 — 抢占 / Revoke / 多 writer 协调

**范围**:
- `AcquireLease` 支持 `force=true`：manager 主动 revoke 当前 writer，等其 flush + release，再给新申请者
- writer revoke 协议：manager push `WillRevokeIn { 5s }` → writer flush → push `Revoked`
- 类似 NFSv4 delegations 的回收

**优先级**: 低。Phase 1+2 已经能跑 sglang 多副本场景（writer 是 cp、reader 是 sglang，writer 短任务自然 close）。

---

## 6. 不变量与代码约束（写代码时必须遵守）

1. **manager 是唯一 lease 决策者**。客户端绝不能本地决定"我先写"——必须先 Acquire。
2. **writer 的 ReleaseLease 必须在 dirty buffer flush 之后**。否则 readers 看到 version+1 但 KV 没新数据 → 读到旧值或空。
3. **客户端 cache 一定要按 version 标记**。任何"我缓存了 ino X 的 extent map / write buffer / inode meta"都必须带 version；invalidation 到达时一票否决。
4. **subscribe 断线 = 失效全部 cache**。不要"乐观保留"。
5. **lease 续约失败 = 自我 revoke**。client 不能假装还持有 —— 否则可能跟新 writer 撞写。
6. **AcquireLease Write 必须在第一次 Put 之前**。不允许"先写后申请"的乐观协议（autumn-rs 的 ValuePointer / extent 结构没有 CAS 来事后回退）。

---

## 7. 风险

| 风险 | 影响 | 缓解 |
|------|------|------|
| invalidation 通道延迟 / 丢失 → reader 看不到 close-to-open | 读到旧数据 | (a) 客户端定期主动 poll version 兜底（如 1s 心跳同时报告 known versions）；(b) Phase 1 内置抖动测试 |
| writer crash → 30s TTL 内 reader 看不到任何新数据 | 30s 的不可读窗口 | TTL 可调；运维场景接受 |
| lease 数量爆炸（百万 inode 同时打开） | manager 内存 + etcd 压力 | (a) lease 粒度按"打开"而非"存在"（关闭即释放）；(b) etcd 只持久化 writer-lease，reader-lease 内存即可（客户端断线由 subscribe 心跳兜底）|
| 与现有 `acquire_owner_lock(stream)` 的命名/语义混淆 | 维护难度 | 命名分开（`InodeLease` vs `OwnerLock`），manager 内部数据结构也分开 |
| autumn-rpc 没有 streaming → invalidation 通道改造大 | 实现周期长 | 用常驻 TCP + manager-buffer + client-poll 替代，autumn-rpc 不动 |

---

## 8. 实施前置

立项 Phase 1 之前需要确认：

1. **autumn-rpc 长连接 / streaming 决定**：用常驻 poll 还是真 streaming？倾向 poll（小动作）。
2. **manager etcd schema**：InodeLease 序列化用 rkyv 还是 prost？跟现有 `regions/` 对齐用 prost。
3. **OpenReq 加 mode 字段**：wire 不兼容，需要 ring 协议 bump `RING_VERSION`。
4. **fuse Phase 2 时是否同时升级 fuser 依赖**：`notify_inval_inode` 需要 fuser ≥ 某版本，确认现版本支持。

---

## 9. 立项后的 feature 拆分（待 commit 进 feature_list.md）

| Feature | 范围 | 验收 |
|---------|------|------|
| F-ioring-lease-1 | manager 加 InodeLease + 4 个 RPC + etcd 持久化 | 单元测试 + 多客户端 acquire/release |
| F-ioring-lease-2 | ioring daemon Open/Close 接 lease | 两 daemon 并跑互斥写测试 |
| F-ioring-lease-3 | invalidation push 通道（常驻 poll） | writer close → reader 1s 内 invalidate |
| F-ioring-lease-4 | OpenedExtents version 标记 + cache 失效 | e2e：两个 daemon 读写同 ino，cache 正确失效 |
| F-fuse-lease-1 | fuse open/release 接 lease | 与 ioring 互操作 |
| F-fuse-lease-2 | fuse kernel cache invalidate (`notify_inval_inode`) | multi-host fuse mount 验证 |
| F-lease-preempt | force-revoke + writer revoke 协议 | 抢占测试 |
