# autumn-fuse Architecture Guide

## Purpose

内核挂载：把 `fs/` 文件树挂成 POSIX 文件系统。文件系统本身（布局、命名空间、读写路径、
会话）在 `autumn-fs`，见 `crates/fs/CLAUDE.md`；本 crate 只有挂载才需要的部分，是整个
仓库唯一依赖 fuser / libfuse 的 crate。

设计借鉴 3FS (DeepSeek/3FS) 的高性能 FUSE 模式：**每 inode 1MB 级写缓冲 + 延迟刷写、
周期异步 sync、内核 attr/entry 缓存、元数据/数据路径分离**。3FS 的共享内存 I/O Ring 与
三级优先级 worker 未采纳（autumn 用 channel 桥接足够）。

## 模块职责

| 文件 | 职责 |
|------|------|
| `main.rs` | 二进制入口 + 30s 周期脏 inode sync + CLI |
| `attr.rs` | **唯一的 core→fuser 转换点**：`inode_to_attr` / `dt_to_filetype` |
| `bridge.rs` | `FsRequest` enum、FUSE↔compio channel 桥接 |
| `ops.rs` | `fuser::Filesystem` trait 实现（readdir 在 reply 边界做 DT_*→FileType）|
| `dispatch.rs` | compio 侧派发循环（lookup/mkdir 在此转 FileAttr；lease 三方法 `pub use` 自 `autumn_fs::lease_tasks`）|
| `read_pool.rs` | 读 I/O 线程池（N 个独立 compio runtime + 各自的 ClusterClient；`ReadJob` 载 `fuser::ReplyData` 跨线程）|
| `inval.rs` | 内核缓存失效线程（`autumn-fuse-inval`：`inval_inode` 在这里发，结果回派发 runtime 记 sticky 集）|

不变量：新的 core→fuser 转换一律进 `attr.rs`；文件系统逻辑一律进 `autumn-fs`，不在
这里另写一份（挂载、S3 网关、Python 绑定必须读写同一份格式）。


## 架构

```
┌─────────────────────────────────────────────────┐
│              应用程序 (ls, cat, cp, ...)          │
└────────────────────┬────────────────────────────┘
                     │ POSIX syscalls
┌────────────────────▼────────────────────────────┐
│              Linux FUSE (kernel)                 │
│   attr_timeout=30s, entry_timeout=30s            │
└────────────────────┬────────────────────────────┘
                     │ /dev/fuse
┌────────────────────▼────────────────────────────┐
│           autumn-fuse daemon                      │
│  ┌─────────┐   crossbeam     ┌───────────────┐   │
│  │ fuser   │──channel──────>│ compio thread  │   │
│  │ threads │<─oneshot───────│ + ClusterClient│   │
│  └─────────┘                └───────────────┘   │
│  写缓冲 (64MiB/inode) | inode 缓存 | 周期 sync    │
└────────────────────┬────────────────────────────┘
                     │ autumn-rpc (binary RPC)
┌────────────────────▼────────────────────────────┐
│         PartitionServer (KV 层)                   │
│         Put / Get / Delete / Range               │
└──────────────────────────────────────────────────┘
```

### FUSE 线程 ↔ compio 桥接

`fuser` 在自己的线程中调用回调，`ClusterClient`（`Rc<RpcClient>`）是 `!Send`。
桥接：fuser 回调线程 → `crossbeam::channel::send(FsRequest)` → compio 线程 recv +
处理 → `oneshot` 回复。`FsRequest` 是 typed enum（Lookup / GetAttr / Read / Write /
…），参考 `crates/rpc/src/server.rs` 的 Dispatcher 模式。

## 挂载侧的操作路径

### 读 I/O 线程池（`read_pool.rs`，`--read-io-threads`，默认 4）

compio 没有 work-stealing 调度器 —— 一个 runtime 就是一个线程，它发出的句柄全是
`!Send`。所以这里的"多线程"和仓库里别处一个意思（PS 的 P-log / P-sst 就是这样）：**一组
各自独立的单线程 runtime，各自持有自己的连接，靠 channel 喂只含 Send 数据的活。**

读路径能这么切，是因为 `prepare` / `execute` 早就分好了：`prepare` 那一半才需要
`&mut FsState`（inode 缓存、写缓冲 flush、extent map），`execute` 一点都不需要 ——
它只拿着一份 `(key, 子区间, dest 偏移)` 的计划和一个 client。派发线程仍是**唯一**读写
`FsState` 的人，所以 lease 一致性、extent map 的语义一个字都没变。

跨线程送的是 `ReadJob`，**不需要 unsafe impl Send**：`ChunkSpec` 全是自有数据，
`fuser::ReplyData` 因为 `ReplySender` 带 `Send + Sync` 超 trait 而天然 Send。client
**不跨线程** —— 每个 worker 自己连（同样 scope 到整个 `fs/`，所以计划里的相对 key
解析结果一致），job 里没有任何 `Rc`。

- **round-robin 派发，不按 inode 哈希**。读之间没有任何顺序要求（每个读回自己的内核
  请求，`prepare` 已经把依赖状态的部分都解完了），而哈希会把"多个线程读同一个大文件"
  这种模型加载形状全压回一个线程。两种路由实测吞吐**没有可分辨的差别**（差值落在同一
  配置重复跑的波动里），所以取简单、且不牺牲那个形状的那个。
- **worker 里必须 spawn，不能 inline await**：inline 会把并发上限压到线程数，比单
  runtime 还差。池子买的是"同样并发、更多 CPU"，不是更少并发。
- **job 不能丢**。派发失败（没配池子、或所有 worker 的 channel 都关了 = 线程死了）时
  `submit` 把 job **原样还回来**，调用方在自己的 runtime 上跑。丢掉不是静默的
  ——`fuser::ReplyRaw::Drop` 会替没发出的回复答 EIO —— 但把"集群本来能服务的读"变成 EIO
  仍然是用户可见的失败，而且靠那个 Drop 等于把正确性押在 fuser 版本细节上。
  `submit_round_robin` 有单测钉这四种情况（空池 / 轮转 / 跳过死 worker / 全死回退）。
- **就绪等待有上限**（`READY_TIMEOUT` 20s）。建池时 fuse session **已经挂载**，请求正在
  桥里排队，所以一个连不上的 worker 会卡住整个挂载点的系统调用；而裸 TCP connect 到黑洞
  manager 只受 SYN 重试约束（分钟级）。放弃一个 worker 只损吞吐，不损正确性。
- **代价**：每线程一个 client ⇒ 每个 mount N 套连接池 + N 条 manager 连接。**更大的一项是
  每线程的注册缓冲池**——`REGPOOL_CAP_BYTES` 是 **per-thread** 上限（默认 512 MiB），规则是
  `cap × threads < RLIMIT_MEMLOCK`。默认 4 把带池线程从 1 抬到 5，最坏情况下 mount 常驻的
  缓冲内存跟着抬（TCP 上是普通堆内存，UCX 上是 **pin 住的页**，计入 RLIMIT_MEMLOCK）。
- ⚠️ **UCX 下未测**。上面全部数字是 loopback TCP。`--transport ucx` 时池子会按线程数多开
  UCX worker，而本仓库记录过 UCX worker 创建走**宿主级 devx 自旋锁**、以及 fuse 在 UCX 上
  的 daemon 崩溃（TCP 不崩）。默认二进制不带 `ucx` feature（`--transport ucx` 直接 panic），
  所以这条路要专门构建才够得着。真跑 UCX mount 时先用 `--read-io-threads 0` 对照。

**为什么要做**：8 并发读时 mount 在 ~2600 MiB/s 到顶，8 流反而掉到 ~2070，而 8 个
`autumnfs` CLI 进程读同一批文件、走同一条 loopback TCP 能到 5466。守护进程的 compio
线程被钉在一个核上（per-thread CPU 时间：compio 394 jiffies vs 内核通道读线程 3）。
每个字节要在那一个线程上过约三次（网络收进 pooled buffer → memcpy 进结果 → 回复写
`/dev/fuse`）。实测（8 个独立 1 GiB 文件，每轮核对字节数，两轮同向）：

| `--read-io-threads` | p=1 | p=4 | p=8 |
|---|---|---|---|
| 0（旧行为） | 885 / 878 | 2006 / 2066 | 2211 / 2213 |
| 2 | 882 / 805 | 2965 / 2445 | 3705 / 3081 |
| 4（默认） | 710 / 739 | 2937 / 2776 | 4763 / 4488 |

p=8 在 t=4 上到 5186~5396（第二轮实验）= CLI 那 5466 的 95%~99% —— mount 不再是瓶颈。
p=1 在 700~940 之间来回，跨配置看不出趋势（是噪声，不是回归）—— 单流是**每请求延迟
绑定**的，内核对一个同步读者不并发下发 FUSE 请求，池子改不了这一点。

机制本身也验过（p=8，per-thread CPU jiffies）：派发线程从**打满变空闲**，总量几乎守恒
—— 是同一份活摊开到多个核，不是加了开销侥幸变快：

| `--read-io-threads` | MiB/s | `-com`（派发） | worker 各线程 | 合计 |
|---|---|---|---|---|
| 0 | 1874 | **435** | — | 435 |
| 2 | 3318 | 7 | 220 / 207 | 427 |
| 4 | 4542 | 7 | 118 / 96 / 102 / 96 | 412 |

### 内核协商（`ops.rs::init`）—— `abi-7-28` 是性能地板，也是新风险面

`fuser` 必须开 `abi-7-28`：`FUSE_MAX_PAGES` 与 INIT 应答的 `max_pages` 字段都在这个 feature
后面，缺了它内核把**每个**请求夹在 32 页 = 128 KiB，`set_max_write` 形同虚设。实测把它打开，
读 **898 → 1621 MiB/s（+81%）**，写不变（写不受请求数限制）。1 MiB 是内核 256 页夹逼后的
真实上限，设更大无效。

**抬 ABI 地板会让新 opcode 变得可达，要看的是"落到我们已实现的方法上"的那些，不是拿 ENOSYS
的那些。** `FUSE_RENAME2` 就是：7-23 起 fuser 会解析它并派发给同一个 `Filesystem::rename`
并带上 flags，而我们的实现忽略 flags、底下是 POSIX 覆盖语义 —— 实测 `RENAME_EXCHANGE`
**返回成功**却做了单向 rename，目标那份内容直接没了。现在 `flags != 0` 一律 EINVAL；
守卫在 `scripts/fuse_chaos.sh` 的 T3。（`RENAME_NOREPLACE` 到不了我们这儿，VFS 先返回
EEXIST，但同样被拒。）

### `FuseLease` 按角色计数（writer_refs / reader_refs）

**同一个文件可以同时被本挂载的一个写 fd 和一个读 fd 打开**，所以 `held_leases[ino]`
两个 refcount 分开记：`O_WRONLY`/`O_RDWR` 记 `writer_refs`，`O_RDONLY` 记
`reader_refs`；`mode` 记的是**本挂载当前在 manager 那边持有的最强租约**。

这是 ffmpeg 的 MP4 faststart 形状：`shift_data()` 写 trailer 时**保持写句柄**、再以
`O_RDONLY` 重开同一文件搬 moov。以前每个 ino 只有一个 `mode` 槽，Open arm 的
`slot.mode != req_mode` 把第二次 open 拒成 EBUSY（`err_to_errno` 的
"lease mode mismatch" 臂），ComfyUI SaveVideo 保存 mp4 **必然**失败。manager 侧从来
允许这个组合（`acquire(READ)` 无条件插 readers，只有**别的 client** 的 writer 才
WriteConflict）——拦截完全发生在挂载侧的簿记里。

- **Open**：`req=READ` 且本挂载已持租约 ⇒ 只 bump `reader_refs`，**零 RPC**（本挂载
  自己的写路径维护缓存一致性，manager 不需要知道）；`req=WRITE` 且 `writer_refs==0`
  （读 fd 已开、第一个写者到来）⇒ `acquire(WRITE)` **升级**（同 client 幂等，
  granted 则更新 mode/epoch；别的 client 持写者才 Conflict）；同角色再开只 bump。
  Granted 一律走 `entry()` 合并而不是 `insert`——upgrade 时槽里已经有读 fd 的计数，
  覆盖会把它们忘掉，之后那些 fd 的 RELEASE 会把计数减到 0、在 fd 还开着时把租约还掉。
- **Release**：内核在 RELEASE 回传该 fd 的 open flags（`ops.rs` 以前丢掉了），据此
  决定减哪个角色。`writer_refs` 1→0 而 `reader_refs>0` ⇒ **降级**：先 flush，再
  `lease::release` + `lease::acquire(READ)` 重注册为读者。不降级的话，一个活过写进程的
  `tail -f` 会一直占着该 inode 唯一的 writer 槽，把别的挂载的写者挡在 EBUSY 外面。
  revoked 的槽整个丢弃、**不降级**（manager 已经把 inode 给了别人，再 acquire 等于复活
  已经不存在的状态）。
- **⚠️ flush 失败就不降级**（`deferred_flush_err.is_none()` 是降级的前提条件之一）。
  写租约正是让本挂载"剩下的脏状态"能安全重试的东西，而 flush 失败会剩下不少：
  `flush_inode` 在 `write_region` **之前**就把 `wb.len` 清零，所以字节没了而 `dirty`
  还在、`meta.size` 仍然覆盖着它们；之后 periodic sync、读 fd 自己的 FLUSH 与 RELEASE
  每一次都会再 put 一遍这个 size。带着这些把 writer 槽还回去，另一个挂载就能拿到写者、
  追加、然后被本挂载那个陈旧的小 size 盖在上面——再往后一次 grow 的 `clean_beyond_eof`
  会把超出该 size 的 extent 删掉。所以 flush 失败时**保持写租约**（等同改动前行为）：
  槽在最后一个读 fd 关闭时归还（那次 RELEASE 会再 flush 一遍），在那之前 `mode` 保持
  WRITE，重试的 put 照样带围栏而不是 ANON。消融：去掉这个条件，
  `system_fuse_release_best_effort.rs` 的 `a_failed_last_writer_flush_keeps_the_write_lease`
  变红（第二个 client 的 `acquire(WRITE)` 从 Conflict 变成 Granted）。
- **两个字段答两个不同的问题，别混**：`check_write_allowed` 问"本挂载还有没有打开的写
  fd"⇒ 看 `writer_refs>0`；`write_lease_for` 问"本挂载在 manager 那边**还持不持有写
  租约**"⇒ 看 `mode == WRITE`。二者恰恰在要紧的时候分叉（flush 失败后 fd 没了但租约
  故意留着）。`mode` 因此是**有承载力的状态**，不是 refcount 的复述：release 成功的那
  一刻立即改成 READ（在重新 acquire 之前，免得 acquire 失败还留着一个已经不属于自己的
  声明）。`compute_release_action` 多返回一个 `downgrade_to_read`，`must_flush` 增加
  "最后一个写 fd"这一条；`last_writer` 用 `writer_refs == 1`（不是 `<=1`，否则一次
  漂移会换来两次白跑的 RTT）。
- **降级中间那个窗口的已知良性竞态**：`release` 与 `acquire(READ)` 之间若心跳恰好落在
  中间，且本 client **不在** manager 的 readers 里（Create 或 Open(W) 起手、从未 acquire
  过 READ 的历史），心跳会拿到 `NotHeld` 并把本地条目删掉，随后 Granted 分支的
  `get_mut` 找不到条目 ⇒ manager 侧留一个读租约而本地无记录，按 TTL 自己过期。升级
  历史（先 READ 后 WRITE）下本 client 仍在 readers 里，心跳返回 Renewed，碰不到这条。
  只丢一次 RTT 与一个 TTL 内的幽灵读者，不影响正确性，故不加防御代码。
- **PyO3 `autumn.Fs`**：`acquire(ino, mode)` / `release(ino, mode="w")` 也按角色记
  （`release` 因此多了 mode 参数），但**绑定没有降级逻辑**：`release("w")` 在还有读者
  时只减计数、不通知 manager，写者槽留到最后一次 release。绑定是 headless 的显式
  lease API，没有内核 fd 生命周期，也没有 in-tree 调用方。
- 测试：`dispatch.rs` 的纯函数状态机（dual-open / 降级 / revoked 不降级 /
  faststart 全序列）+ `crates/manager/tests/fuse_lease_1.rs` 的真集群
  `fuse_write_open_then_read_open_in_same_mount_coexist`、
  `fuse_last_writer_close_downgrades_and_frees_the_writer_slot`、
  `fuse_remote_reader_coexists_with_a_local_dual_open_and_sees_writer_closed`（消融：把
  `slot.mode != req_mode` 拒绝加回去，前两条都红，报的正是 "lease mode mismatch"）；
  flush 失败不降级由 `system_fuse_release_best_effort.rs` 的
  `a_failed_last_writer_flush_keeps_the_write_lease` 钉住（杀 PS 造真实 flush 失败）。
  真实挂载侧：两个挂载点跑 `ffmpeg -movflags +faststart` + 降级观察（守护进程要
  `RUST_LOG=info` 才看得到 `lease downgrade: writer slot released` 那行；T1–T3 区分不了
  "角色恒为 READ"，只有第二个挂载拿到 writer 槽才证明 RELEASE 的角色真的接对了）。

### 内核缓存失效只在 `autumn-fuse-inval` 线程上发（`inval.rs`）

`Notifier::inval_inode` 是对 `/dev/fuse` 的**同步** write(2)，内核处理它时要锁住该
inode 的每一个缓存页（`invalidate_inode_pages2_range`）。预读中的页一直锁着，直到
对应的 FUSE_READ 被应答——而**每个** FUSE_READ 都要先经派发线程 `prepare`（读池只
接 `execute`）。所以在派发线程上发这个 write，就是自己等自己：读者 D 状态、挂载点
从此不再应答任何请求（FUSE 没有超时）。以前就是这么发的（lease 轮询任务与 Open 臂
都在派发 runtime 上直接调闭包）。

实测（`scripts/fuse_inval_deadlock.sh`，修前二进制）：第 1～2 个 WriterClosed 事件就
卡死，`autumn-fuse-com` 线程停在 `folio_wait_bit_common`（D），读者同样；
`--read-io-threads 0` 和默认 4 都一样。修后同一脚本 90 s 各跑过 ~23 万个事件、
~250 轮整文件校验，零卡顿、字节全对。

- **怎么才会有被锁的缓存页**：Open 应答带 `FOPEN_DIRECT_IO`，普通 `read(2)` 不进页缓存，
  所以 `cat` 循环**触发不了**。进页缓存的是：`MAP_PRIVATE` mmap（6.1 内核对 direct-io
  文件允许私有映射，缺页走 filemap 预读）、以及 `create` 返回的 fd（应答 flags 为 0）。
  脚本的读者就是私有映射反复缺页。
- **形状**：派发 runtime 只把 ino 塞进 std channel；专用线程调 `inval_inode`，在那里阻塞
  无害（派发线程空着，能去应答持锁的那条读）；每个结果按序经 futures channel 回到派发
  runtime，由 `record_results` 写 `notify_inval_failed`（失败置 sticky、成功清除）。
- **不许等失效落地再应答请求**：在 handler 里 await 结果，等于把派发循环挂住——跟阻塞
  write 把线程挂住是同一个死锁。所以 Open 臂对 sticky ino 的重试是**只入队**，结果
  以后再清 sticky；以前那句"retry succeeded on Open"的同步判断因此移到 `record_result`。
- 线程不 join：所有 sender 丢掉（compio runtime 退出）就自然结束；卸载之后也没有值得
  送达的失效。
- **⚠️ 剩下的一个坑：notify 正等着预读页时被 SIGKILL**。能应答那条读的线程都死了，等待的
  线程不可中断，而 `/dev/fuse` fd 要等**所有**线程退出才释放（释放才会 abort 连接、才会
  放开那页）——守护进程永远是僵尸，挂载点背后没有服务（就是 AutoUnmount 注释里那五台
  节点的形状）。实测：脚本早期版本用 `kill -9` 收尾，修前修后每跑一次都留下一个，线程停在
  `fuse_reverse_inval_inode → invalidate_inode_pages2_range → folio_wait_bit_common`；
  `echo 1 > /sys/fs/fuse/connections/<minor>/abort` 后立即退出。前提是被杀那一刻有预读中
  的页缓存页，也就是原来会直接死锁的那种 mmap-private 负载，所以修后严格更好。根治要让
  SIGTERM 走优雅卸载，记在账本 BUG-FUSE-SIGKILL-DURING-NOTIFY。
- 测试：`inval.rs` 单测钉"invalidator 不等 notify 就返回"（notify 只在调用返回后才被放行，
  内联就会等满超时并报错）、结果保序、失败/成功对 sticky 集的作用；端到端是那个脚本
  （需要真挂载，不能进 cargo test）。

## 配置（CLI）

`autumn-fuse` 参数：`--manager`（default `127.0.0.1:9001`）、`--mountpoint`、
`--credential-file`（authz 保护 `fs/` 时必需；`<principal>\n<hex>`，覆盖不到 `fs/`
则 fail-fast）、`--allow-other`（default false）、`--transport`（`tcp`/`ucx`，须与
cluster 一致）、`--direct-read`（default true）、`--read-io-threads`（default 4；
`0` = 全部读留在派发线程，即池子之前的行为）。内核缓存 `attr_timeout` /
`entry_timeout` = 30s、`negative_timeout` = 5s；周期脏 inode sync 间隔 30s（`main.rs`）。

## 关键依赖文件

| 文件 | 用途 |
|------|------|
| `crates/fs/` | 文件系统本身（`autumn-fs`）|
| `crates/rpc/src/server.rs` | Dispatcher 模式参考（bridge 设计）|

## statfs — 保守 3 副本映射

`df -h <mountpoint>` 的 `Statfs` 调 `state.client.cluster_df()`（`MSG_CLUSTER_DF`
聚合快照，每 EN 的 RAW + autumn physical_used 求和），**按 3 副本因子保守映射**：
`blocks = raw_total/3/4096`，`bavail = bfree = raw_free/3/4096`。EC 下可用逻辑容量是
个区间（cold EC 1.25–1.33× vs hot 3×），statfs 是单标量 → 收敛到 WORST 因子（CephFS
式），使 `df` 绝不高报空闲、不会诱使 writer 乐观 ENOSPC（低报是安全侧）。调用有界
（`compio::time::timeout` 2s），超时/错回退到良性大默认值。文件 `size` 保持逻辑大小
（副本/EC 放大对 FS 层透明）；inode 计数为常量。

## Restart 行为 —— EN vs PS

- **PS kill+restart**（`scripts/fuse_chaos.sh` 的 PS-kill 阶段）：分区 MIGRATE 到另一 PS，region
  重收敛后 I/O 恢复，已 sync 文件字节精确。RMW-GET-SWALLOW 窗口由
  `scripts/fuse_rmw_chaos.sh` 覆盖。
- **manager / fuse-daemon kill+restart**：`fuse_chaos.sh` 的 MGR-kill / FUSE-kill 阶段。
- **EN kill+restart**（`scripts/fuse_en_restart_chaos.sh`）：EN kill **不迁移**分区，
  stream 层把读写 failover 到存活副本。**INTEGRITY 完好** —— 4 轮 kill+restart（全 3
  EN）+ remount 验证 6 个 durable 文件（4 KiB..10 MiB 含多 extent）+ 4 个反复 RMW
  文件对 lockstep mirror 字节精确。

  **WRITE 可用性 caveat = CAPACITY 非 failover-latency bug**：EN kill 只在 cluster
  恰好 = RF（=3）EN 时 stall 写。
  - **3 EN / RF=3**：每 extent 在全 3 EN，killing 1 剩 2 healthy `< RF`，
    `select_nodes` 组不出新 3 副本 extent → all-replica-ACK append 不完成、new-extent
    alloc 反复退回死节点 → 单次写 WEDGE 到 EN 回来（实测一次 put 撞 90s CLI 超时，
    实际无界）。这是 RF=N-on-N 的真相（Ceph/HDFS 同样在 3 节点 RF=3 一个 down 时停写），
    非 autumn 缺陷。
  - **5 EN / RF=3**：killing 1 剩 4 healthy `≥ RF` → 新 extent 在 healthy 节点 alloc，
    append 透明滚过死副本 extent → **写永不 stall**（实测每 put/get < 0.1s，PS
    retries=0）。
  - READS 在任意 cluster 大小容忍一个 down 副本（min-quorum read），这就是上面 3 EN
    下 integrity 校验总过的原因。

  CONSEQUENCE：`fuse_chaos.sh` / `fuse_en_restart_chaos.sh` 跑 3 EN，验的是 EN-restart
  INTEGRITY（EN 很快重生、读全程可用），**不**测 EN-down 期间持续写（那只会撞 RF=3
  capacity wedge）。要测 EN 丢失下的写可用性须配 >RF 台 EN。单线程 fuse dispatcher +
  30s bridge `REPLY_TIMEOUT` 只把 stall 放大成 EIO，非成因；无需改 stream 层超时。
