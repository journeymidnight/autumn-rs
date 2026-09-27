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
| `prefetch.rs` | 守护进程预读：派发线程上的顺序检测（`Detector`）+ 读线程共享的预取块缓存（`PrefetchCache`）|
| `inval.rs` | 内核缓存失效线程（`autumn-fuse-inval`：`inval_inode` 在这里发，结果回派发 runtime 记 sticky 集）|
| `readahead.rs` | 挂载的预读窗口：INIT 时打开 `/sys/class/bdi/<dev>/read_ahead_kb`，第一个 open 时写入 |

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

### 页缓存：所有读都走内核页缓存，Open 决定是否跨 open 保留（`open_keeps_page_cache`）

Open 不再回 `FOPEN_DIRECT_IO`。以前回它是为了"测出来的数字不被页缓存命中虚高"——那是测试
便利，不是一致性需要；代价是 `MAP_SHARED` mmap 报 `ENODEV`、6.1 内核每次 mmap 都把整个文件
的缓存清掉、`pread` 读者一次一个请求、第二次加载一样从远端重读。

**是否保留（回 `FOPEN_KEEP_CACHE`）**，`dispatch.rs::open_keeps_page_cache`：
- **打开前本挂载已持有该 inode 的 lease**（且未被撤销）→ 保留。持 lease 期间别的客户端的
  写都会以 WriterClosed/LeaseRevoked 推过来，`inval.rs` 把缓存丢掉。
- **没持 lease** → 在 AcquireLease **之后**从 PS 现读 inode（`meta::fetch_inode`，不走
  `state.inodes` 缓存——那份缓存在没有 lease 时不可信），`generation` 等于
  `FsState.page_cache_generation[ino]`（上次同样这样读到的）才保留，然后记下新值。
  理由：最后一个 fd 关闭时 lease 就还了，manager 只给**当时的**读者推 WriterClosed
  （`inode_lease.rs` release 路径），所以之后的改写本挂载一无所知，而 KEEP_CACHE 让旧页一直留着。
  先 acquire 再读：之后关闭的写者会推失效给我们，之前关闭的写者已经把 generation 落了盘。
  读失败 → 不保留（只损失缓存，不让 open 失败——acquire 已经成功，失败会漏掉 lease 引用）。
- **失效通知失败过（sticky）** → 一律不保留，内核在 open 里自己清（在打开者线程上，不在
  派发线程上，不会重演 `inval.rs` 那个自己等自己）。
- 内核 FORGET 掉 inode（lookup 计数归零）时删记录——页跟着 inode 一起没了。

**为什么用 `generation` 不用 mtime / lease version**：`generation` 由每次改字节的路径抬高
（flush 的写、truncate、segment 换图）；mtime 可被 `setattr` 指定成旧值；lease version 只在
manager 内存里，写者关闭后 etcd 记录即删，manager 重启后从 1 重来。S3 覆盖写、`autumnfs put`
都换新 ino，是另一个内核 inode，不涉及这里。
**已知窗口（与以前相同，不是本改动引入）**：写者崩在"extent 已落、meta 未 flush"之间时，字节
变了而 generation 没变；未 fsync 的写本来就不保证。

**同挂载写**：写 fd 也走页缓存（直写，无 writeback cache），内核在同一份页缓存里自己保证
一致；可写 `MAP_SHARED` 的脏页由内核回写（close 前的 flush 写出）。写吞吐与 direct-io
持平（4 GiB `dd bs=8M conv=fsync`，交替 4 对：272.8 vs 272.8 MB/s）。写 fd 关闭且 flush
干净时，Release 把刚发布的 `generation` 记进 `page_cache_generation`：这些页就是本挂载在
自己的写 lease 下写进去的，所以 `cp` 进挂载后紧接着加载不必整份重读（以前每次都丢）。

**大小只从 GETATTR 来，所以缓存的 `InodeState.meta` 必须跟着失效走。** direct-io 时每个
`read` 都到守护进程，读路径的 EOF 确认（`get_inode_uncached`）会收下 KV 里更大的 size；
走页缓存后内核在自己的 `i_size` 处就截断，**超出部分根本不发 READ**，而是问 GETATTR——
那里答的是缓存的旧 meta。实测（真挂载 TAIL：A 持 fd 读到 EOF，B 追加并关闭）：A 永远停在
旧 EOF，`fstat` 也是旧值；HEAD 上同一步通过。修法：invalidation 轮询循环把每个事件的 ino
（以及 poll 失败 / overflow 时丢掉的全部 held ino）放进 `FsState.meta_invalidated`，
`meta::get_inode`（GetAttr、Lookup 都经过它）遇到标记时从 KV 重读一次、丢掉按 size 算的
extent map、清标记。不用 `invalidations` 的版本下限做比较：lease version 在 manager 重启后
从 1 重来，比较会漏。**本挂载持写 lease（未撤销）或 inode 脏时不重读**：那时别人改不了文件，
标记（发给我们的 WillRevokeIn）不说明内容变了，而缓存的 size 可能领先 KV（flush 在 put
落地前清 `dirty`，put 失败就停在那里）——收下 KV 的更小 size 就是 `get_inode_uncached`
注释里那条删数据的路（`clean_beyond_eof`）；最初版本只看 `dirty`，评审指出后改。poll 失败
/ overflow 先清空 `held_leases` 再标记，所以那时不再是写者、标记照常生效——没有 lease 的 fd
写会被拒（`check_write_allowed`），重开会重新 acquire。标记在读 KV **之前**取走（读的
await 期间到来的新事件会重新标记，不会被随后的清除抹掉），读失败放回并报错（GETATTR 以前
答缓存，现在报错直到读成功）；FORGET 时也清掉。

守卫：`crates/manager/tests/fuse_page_cache.rs`（派发层；消融各红一条：不校验 generation /
忽略 sticky / FORGET 不删记录 / `get_inode` 无视标记 / Release 不记 generation / 持写 lease
时仍重读）+
`dispatch.rs::page_cache_tests`（判定表，默认 `cargo test` 就跑）+ `scripts/fuse_page_cache.sh`
（真挂载，SHARED/KEEP/REOPEN/HELD/TAIL/LOCAL/MMAPW；不校验 generation 的构建上 REOPEN 与
MMAPW 变红，`get_inode` 无视标记的构建上 TAIL 变红）。

### 预读窗口（`readahead.rs`，`--readahead-kb`，默认 2048）与 `max_background`

mmap 缺页按页缓存预读窗口（bdi `read_ahead_kb`）以缺页处为中心读一窗，外加异步预读标记
提前取下一窗；每个缺页线程在飞的量由窗口决定，高延迟下它就是加载器能拿到的大部分吞吐（不是
简单的 窗口 ÷ 延迟：单核 2 MiB、每读 4 ms 实测 735，高于 2 MiB ÷ 4 ms ≈ 512）。**INIT 只能把它调小**（内核给出 128 KiB，取 min），所以
抬高只能写 sysfs——**而且必须在 INIT 应答之后**：内核处理 INIT 应答时把 bdi 设成
`min(当前, 协商值)`，挂载后立刻写的 16384 实测读回 128。所以 `init()` 里打开 sysfs 文件
（打不开 → 挂载失败，`--readahead-kb 0` 可关），第一个 open/create 时写入（任何读都在某次
open 之后）。

`max_background` 64 / 拥塞阈值 48：一窗按 ≤1 MiB 的后台 READ 发出，fuser 默认 16/12 时
超过 12 个在途内核就不再发异步预读。同为 16 MiB 窗口，HEAD（16/12）655–659 vs 本构建
735–763 MiB/s；2 MiB 窗口下没有单独 A/B 过这一项。

**实测**（2026-09-27，集群在 netns 里、挂载在宿主、只在 veth 上 netem；单向 1 ms 时 64 KiB
读 4.1 ms，与 pod 实测 4.07 ms 一致；1930 MiB 分片冷加载 `load_file` + 逐张量拷贝，MiB/s）。
**加载器用几个核决定了哪个窗口好**，两组都要看：

加载器绑单核（缺页只来自一个线程）：

| 单向延迟 | 128 KiB | 1 MiB | 2 MiB | 4 MiB | 16 MiB | 64 MiB |
|---|---|---|---|---|---|---|
| 0 | 952 | 955–958 | 942–948 | 916 | 835 | 650 |
| 1 ms（≈ pod） | 108 | 515–523 | 720–740 | 823 | 749 | 627 |
| 2 ms | 58 | 331–333 | 515–517 | 682 | 705 | 607 |

加载器 9 核（torch 的 `copy_` 多线程，几个线程同时在一个大张量的不同切片上缺页）：

| 单向延迟 | 128 KiB | 1 MiB | 2 MiB | 4 MiB | 16 MiB | 64 MiB |
|---|---|---|---|---|---|---|
| 0 | 2213 | 1979–2010 | 1727–1762 | 1263–1506 | 1089–1107 | 1052 |
| 1 ms（≈ pod） | 763 | 1140–1375 | 1095–1334 | **124–188** | 165–241 | 994 |
| 2 ms | — | 981–999 | 1013–1038 | — | — | — |

多核下 4 MiB 塌了：守护进程收到的字节一样（1930 MiB），请求却从约 5 千个（1 MiB 窗口、9 核）
变成 5–6 万个（平均约 36 KiB），每个都付一次往返。**实测的只有请求数和大小**；为什么会碎是
推测、未在内核核实：各线程的切片相距几 MiB，以缺页处为中心的预读窗口互相重叠，已在读的页把
批次切碎，mmap 缺页计数还可能把预读整个关掉；1 MiB 时窗口基本不重叠，64 MiB 时一个窗口罩住
所有线程。"9 核"是 `taskset` 给的核数，没有单独控制 torch 的线程数，所以"多线程 `copy_`
在不同切片上缺页"同样是推测。

最初按单核选了 4 MiB 并上线，多核复测后改成 **2 MiB**，代价明说：单核 1 ms 720–740（4 MiB
是 823，低约 11%），2 ms 515–517（最好的 16 MiB 是 705，低约 27%）；零延迟多核 1727–1762
（1 MiB 1979–2010、128 KiB 2213，低 12–22%）。换来的是多核下不塌（1 ms 1095–1334、2 ms
1013–1038）。**不保证换一个模型（张量大小、线程数不同）不塌**——调内核窗口本身是脆弱的
杠杆，根本办法是守护进程自己预取，内核窗口的影响就小了。
HEAD（direct-io、128 KiB）单核 1 ms 108 MiB/s；8 进程同时加载同一分片（TP rank）：最慢一个
32.45 s → 4.34–4.70 s（4 MiB 时测）；第二次加载：HEAD 17.9 s（全部重读）→ 1.1 s（守护进程读
0 字节）。注意 8 进程**同时**起步时 HEAD 也只从远端读一遍（私有映射共用页缓存），收益来自
窗口，不是字节数。

### 守护进程预读（`prefetch.rs`，`--prefetch-mem-mb`，默认 1024，默认开）

内核预读每个缺页线程只有约一个窗口在飞，单流读者每个窗口付一次往返；同一挂载 8 线程并行
`pread` 在每读 4 ms 下能到 1.6–2.7 GB/s，挂载本身不是瓶颈。这一层给任何读者那个深度：派发线程
在 Read 臂**先把读请求发出去**，再看它是不是某条顺序前沿的延续、规划前方的块
（`dispatch.rs::prefetch_ahead` → `read::prepare` 规划每块）；块由读线程取进共享的
`PrefetchCache`；读请求在读线程上先查缓存，命中就从内存答，块还在取就等它（`Lookup::Wait`，
整个应答仍在 `REPLY_TIMEOUT` 里）。

**判定与窗口**（`Detector`）——每条都是实测逼出来的：
- 前沿按**实际读到的字节**判定：≥3 MiB 且读过的字节不少于所跨范围的一半才预取。按"连续几次"
  判定时，4K 随机读偶尔几次落在一起就被当成顺序，p99 从 2.2 ms 变成 24.7 ms（它们去等 4 MiB 块）。
- 容差 2 MiB（一个内核窗口）：内核同一窗口的几个 READ 并发发出、到达有乱序。
- 窗口**随已读字节放大**，至少 1 块（4 MiB）最多 64 MiB。safetensors 加载先碰每个张量的头
  （每处 2–4 MiB、跳 32–86 MiB）再逐个拷贝：一上来就预取满窗口时取了 1.7 倍文件（1.4 GB 没人读）。
- 每文件最多 16 条前沿（9 个线程缺页时 8 条会被反复挤掉）；同一代里一个块只要一次。
- **没有"读者都走过就释放"**：那条规则把第二遍（拷贝阶段）要回来读的块全放掉了。

**内存**：块只活在"取回"到"内核读走"之间（之后页缓存里有），守护进程占的约等于每个活跃读者
前方的窗口。预算是上限不是预留：放不下的块不取，读请求照常去集群，**读永不因预算阻塞**。
释放：读满（前沿起点所在的块，前沿自己已从集群读过的那段在接纳时就记为已读，否则永远读不满——
实测这种块两秒内把 1 GiB 预算占满、拒绝了 168 块）、5 s 没人读、文件关闭（`open_count` 归零或
FORGET）、generation 变了。块是普通堆内存：传输层注册池只管接收缓冲，UCX 下总内存 = 预算 + 每
读线程注册池。

**一致性（两层 generation 检查）**：块带规划时的 generation，读请求带 `read::prepare` 刚看到的
generation（`get_inode` 在失效标记后会重读；本挂载写会抬 generation）。`lookup` 发现不一致就丢掉
这个文件的块；`admit` 接纳新一代的块时也清掉旧一代。实测只消融 `lookup` 那层时真挂载 PREFETCH
仍过——改写后第一个读请求一发出，派发线程就为新一代 `admit`，旧块在读线程查缓存前已被清掉；两层
一起消融才变红（A 读到改写前预取的字节）。陈旧窗口与页缓存相同：别处写者关闭的失效到达之前。

**实测**（netns + veth，单向 1 ms ≈ pod 每读 4 ms；开 vs `--prefetch-mem-mb 0`，交替）：

| 负载 | 关 | 开 |
|---|---|---|
| 单流 `dd` | 0.69 GB/s | 1.7 GB/s（2 ms：0.41 → 1.6） |
| 串行 1 MiB `O_DIRECT` | 211–214 MB/s | 2.1 GB/s |
| vLLM `eager`（`read()` 整个读） | 368–375 | 482–495（2 ms：267 → 488） |
| vLLM 默认加载 Qwen3-VL-4B（bf16，8.27 GiB） | 7.85–7.90 s | 4.77–4.78 s |
| vLLM-Omni 加载路径，bf16 DiT 替身（26.6 GiB） | 1051–1055 MiB/s | 1434–1449 |
| 同上 + `--disable-multithread-weight-load` | 1126–1127 | **1961–1969** |
| vLLM-Omni 路径，Wan fp32→bf16（53.2 GiB） | 323–340 | 372–438 |
| 4K 随机读 p99 | 2.16–2.20 ms | 2.16–2.24 ms |

**真实 MiniMax-H3**（vLLM-Omni main、4×H200 TP4、`--task-type fl2va`，数据用 `autumnfs put -P8`
写进 24 lane / 24 分区，1 ms，每格一次）："Model loading took"：页缓存改动之前 364 s；关预读 243 s；
开预读 203 s；开预读 + `--disable-multithread-weight-load` **197 s**；本地 NVMe 参照 132 s。
关预读时 `--disable-multithread-weight-load` 反而 286 s——两者要一起用。开预读省的时间大半在
"读权重之前"那段（VAE 用 `safe_open` 逐张量读，137 → 100 s）。早先按单进程替身等比推算的
"改动前约 19 分钟"被这次实测推翻（TP4 下各 rank 并行、只读自己的切片）。

**内存**：6 个 5 GB 文件并行顺序读 3263 MiB/s，RSS 峰值 578 MiB（预算 1024）；预算压到 128 MiB：
拒绝 4914 块、读照常（2932 MiB/s），RSS 峰值 287 MiB——预算只管预取块，走集群的读自己的结果
缓冲不在其内。

**生命周期**：被接纳的块由 `Admitted` 持有，`complete` 交出字节；没完成就被丢弃（worker 死了
带走队列里的任务、没有 worker 接）时析构里释放块、唤醒等待者——否则这块永远"取数中"，每个
碰到它的读都要等满 30 s `REPLY_TIMEOUT` 再 EIO（评审发现；单测去掉析构里的释放即红）。被预算
拒绝或规划失败的块从 `issued` 撤回（`Detector::unissue`），预算空出后还能再要。

**会变差的**（默认开是用户的决定，按 JuiceFS / mountpoint-s3 的惯例；低延迟或这类负载用
`--prefetch-mem-mb 0`）：零延迟下 mmap 加载单核 −12%、多核 −37%、`dd` −5%；合成的"9 核 CPU
`copy_`"mmap 加载 1/2 ms 下 −25%/−38%（开着时内核发来的 READ 多一倍多、更碎：13.7k 对 ~5k，
为什么会碎未在内核核实）；Wan fp32 配 vLLM `prefetch` 策略 1893–1927 → 1721–1790。
**mmap 单线程加载的上限在内核缺页路径**：零延迟不预取单核也只有约 945，页缓存全热约 1740——
守护进程预读省掉的只是每次 READ 的网络往返，不是缺页本身。

守卫：`prefetch.rs` 单测 20 条（判定、随机读不触发、窗口放大、已读记账、跨块拼接、等待与唤醒、
失败、`Admitted` 丢弃释放、`unissue`、两层 generation、预算、短尾块、空闲清理）；`scripts/fuse_page_cache.sh` 的 PREFETCH（
两层 generation 检查一起消融时变红）。

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

- **怎么才会有被锁的缓存页**：所有读都走页缓存（见「页缓存」一节），`cat`、`pread`、
  mmap 缺页的预读页都会被锁。脚本的读者用私有映射反复缺页并在每轮之间丢缓存，让同一时刻
  处于预读中的页尽量多。
- **形状**：派发 runtime 只把 ino 塞进 std channel；专用线程调 `inval_inode`，在那里阻塞
  无害（派发线程空着，能去应答持锁的那条读）；每个结果按序经 futures channel 回到派发
  runtime，由 `record_results` 写 `notify_inval_failed`（失败置 sticky、成功清除）。
- **不许等失效落地再应答请求**：在 handler 里 await 结果，等于把派发循环挂住——跟阻塞
  write 把线程挂住是同一个死锁。所以 Open 臂对 sticky ino 的重试是**只入队**，结果
  以后再清 sticky；以前那句"retry succeeded on Open"的同步判断因此移到 `record_result`。
- 线程不 join：所有 sender 丢掉、结果流丢掉或 `InvalGate` 关闭时自然结束。
- **退出前必须先关 `InvalGate`（SIGTERM / SIGINT，`main.rs::shutdown_on_signal`）**。
  notify 正等着预读页时整个进程被杀（默认动作一次杀光所有线程），能应答那条读的线程都死了，
  等待的线程不可中断，而 `/dev/fuse` fd 要等**所有**线程退出才释放（释放才会 abort 连接、
  才会放开那页）——守护进程成僵尸、挂载点背后没有服务（AutoUnmount 注释里那五台节点的
  形状），线程停在 `fuse_reverse_inval_inode → invalidate_inode_pages2_range →
  folio_wait_bit_common`。实测：不带处理的构建，负载中 SIGTERM 10 轮留 4 个僵尸。
  所以两个信号在任何线程创建之前被屏蔽（线程继承掩码；挂载本身不建线程，fusermount3
  帮手进程自己屏蔽全部信号），由 `autumn-fuse-signal` 线程 `sigwait` 独占，顺序是：
  ① 关 gate——失效线程在每次 notify 期间持锁，拿到锁就等于在途 notify 已被应答
  （session 循环和派发线程这时都还在服务），之后不再发新的；② 给派发线程发 Destroy
  （flush 脏 inode）并等它结束；③ lazy 卸载；④ `process::exit`，关掉 `/dev/fuse`，
  仍开着文件的进程随连接 abort。同类负载的单集群多轮复验：不带处理 10 轮 4 僵尸，带处理
  20 轮 0；提交的脚本 8 轮全过，不带处理的构建在第 1、2 轮即失败。
  代价：启动阶段（连集群 + 等 ready，最长约 60 s）收到的 SIGTERM 要等启动走完才退出——
  Destroy 只在派发循环里被消费；这期间还没有 lease 任务、不会有 notify 在途，宽限期到了
  被 SIGKILL 也是干净的。第二个 SIGTERM 不起作用（信号一直屏蔽，只 `sigwait` 一次）。
  **SIGKILL 跳过这一步，照样会留僵尸**（被杀时刻有预读中的页缓存页才会，任何读都可能），只能 `echo 1 > /sys/fs/fuse/connections/<minor>/abort` 清。
- 测试：`inval.rs` 单测钉"invalidator 不等 notify 就返回"（notify 只在调用返回后才被放行，
  内联就会等满超时并报错）、结果保序、失败/成功对 sticky 集的作用、"关 gate 会等在途
  notify 且之后不再发"（把放锁挪到 notify 之前即变红）；端到端是那个脚本（需要真挂载，
  不能进 cargo test），它的 TERM 段在竞争进行中连发 `TERM_ROUNDS` 轮 SIGTERM。

## 配置（CLI）

`autumn-fuse` 参数：`--manager`（default `127.0.0.1:9001`）、`--mountpoint`、
`--credential-file`（authz 保护 `fs/` 时必需；`<principal>\n<hex>`，覆盖不到 `fs/`
则 fail-fast）、`--allow-other`（default false）、`--transport`（`tcp`/`ucx`，须与
cluster 一致）、`--direct-read`（default true）、`--read-io-threads`（default 4；
`0` = 全部读留在派发线程，即池子之前的行为）、`--readahead-kb`（default 2048；`0` = 内核默认
128 KiB）、`--prefetch-mem-mb`（default 1024；`0` = 关掉守护进程预读；需要读线程池）。内核缓存 `attr_timeout` /
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
