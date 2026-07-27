# autumn-fuse Architecture Guide

## Purpose

FUSE 文件系统层，将 autumn-rs KV 存储挂载为 POSIX 文件系统。设计借鉴 3FS
(DeepSeek/3FS) 的高性能 FUSE 模式：**每 inode 1MB 级写缓冲 + 延迟刷写、周期异步
sync、内核 attr/entry 缓存、元数据/数据路径分离**。3FS 的共享内存 I/O Ring 与三级
优先级 worker 未采纳（autumn 用 channel 桥接足够）。

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

### Inode-based 路径映射

采用 inode 方案（非扁平 path=key）：rename O(1)（只改目录项）、hardlink（多目录项
指向同一 inode）、根 inode = 1 (`ROOT_INO` / FUSE_ROOT_ID)。

**inode 分配 = manager 发号**：`ClusterClient::alloc_inodes` → `MSG_ALLOC_INODES`
（leader-fenced etcd CAS），每批预分配 `INODE_ALLOC_BATCH`=1000 个，全局计数器保证
并发分配者（双 mount，或 mount + Python `autumn.Fs`）不重号。`[0x04]next_inode` KV
仅作**迁移 floor**（首批把旧值传给 manager）+ 每批 best-effort 回写（advisory-only）。

## KV Key 编码

所有文件系统数据存在同一 KV namespace，靠 key 第一个字节区分类型。Big Endian 保证
自然排序：同父目录项聚集、同文件 extent 按逻辑偏移连续有序。

| 前缀 | 用途 | Key 格式 | Value |
|------|------|---------|-------|
| `0x01` | Inode 元数据 | `[0x01][ino: u64 BE]` | InodeMeta (rkyv) |
| `0x02` | 目录项 | `[0x02][parent: u64 BE][name]` | DirentValue (rkyv) |
| `0x03` | 文件数据 extent | `[0x03][ino: u64 BE][logical_off: u64 BE]` | raw bytes ≤ 8 MiB (`MAX_EXTENT`) |
| `0x03` | 条带 extent (striped) | `[0x03][lane: u8][ino BE][logical_off BE]` | raw bytes ≤ `MAX_EXTENT` |
| `0x04` | FS 超级块 | `[0x04][field]` | varies（`next_inode` / `schema_version` / `stripe_geom` / `rmtomb/[ino]`）|

**Namespace-first 绑定（Option 3）**：wire key = `fs/[type][fields]`（一棵全局树，
无 tenant 段、无 volume 段）。`autumn-fuse` / `autumnfs` / PyO3 `autumn.Fs` 都无
`--tenant`；`FsState` 用 `connect(mgr, "fs")` / `scoped("fs")`。多棵互隔离的树用不同
namespace（`fsA`/`fsB`）。上表是 RELATIVE key —— **client 负责整个 `fs/` 前缀**
（prepend + 把返回 range key 剥回、按 namespace 边界 clamp）。`state.rs` 的 8 个
`kv_*` choke point、`key::*` builder、全部 `parse_*` 都交裸 RELATIVE key 给 client，
零 wire 拼接。两处 batch 数据路径（`read::prepare` 的 `ChunkSpec.key`、
`extent::flush_appends` 的 append keys，为性能直调 `get_many_*`/`put_many_fenced`
绕过 `kv_*`）同样交裸 `key::*`——client 一处 prepend，与元数据路径一致。授权 =
`principal-create --grant fs/` + `--credential-file`（principal 名在文件里，authz
开了就整个 `fs/` 受保护）。详见 docs/key_namespace_split_design.md §8。

### 变长 extent（F247）

文件数据是**按逻辑字节偏移寻址的变长 extent**（key = `[0x03][ino][logical_off BE]`，
value ≤ 8 MiB = `MAX_EXTENT`）：顺序写合并成接近 8 MiB 的 extent，末尾/部分 extent
较短（"像 Linux extent 一样变长"）。相比固定 256 KiB chunk，大文件从几十万个小块变成
数量级更少、每个 ≥ 64 KiB 的 extent，每个整 extent 读走 `get_many_into` 的 ZC 路径
（`MSG_GET_ZC`，F243 RDMA 零拷贝的目标尺寸）。

- **持久真相 = extent KV key 本身**（隐式 key 设计，InodeMeta 里**不**存 extent 列表）。
- **运行时缓存** `InodeState.extents: Option<Vec<(start, len)>>`：冷启动 range-scan
  `[0x03][ino]` 前缀拿起始偏移 + 由相邻起始/文件大小推断长度；写时增量维护，
  truncate 时失效（置 `None`）。
- **不变量：extent 互不重叠**。读按 `[start, start+len)` 请求每个重叠 extent 的精确
  子区间，PS get 按真实 value 长度裁剪、dest 余下补零 → 短 extent / 稀疏空洞都正确。
- **小文件** ≤ `INLINE_THRESHOLD`=4KB：inline 在 `InodeMeta.inline_data`（无 extent，
  读写各省一次 KV 操作；增长超过阈值迁移到 extent 存储）。
- 全部寻址/读/写/截断/删除逻辑在 `crate::extent`。

### Lane striping（F-FS-STRIPE，大文件跨分区条带化）

大文件的 extent 跨 `lanes` 个分区分布，使单文件读写并行超过单 partition/log_stream
天花板。

- **每文件 stamp**：`InodeMeta.stripe: Option<StripeLayout>`。`Some` = 条带化
  （extent 走 `[0x03][lane][ino][off]`），`None` = 单分区 legacy 布局
  （`[0x03][ino][off]`）。**create 时定，之后不可变**；读侧 branch 于此选 key 布局，
  老文件无迁移仍正确。
- **striped key**（18 B）：`[0x03][lane][ino BE][off BE]`，`lane =
  stripe_lane(off, lanes, unit_bytes) = (off / unit_bytes) % lanes`。lane 字节在
  HIGH 位（紧跟 0x03）主导分区路由；lane 边界 `[0x03][lane]` 是 STATIC（ino 无关），
  故 fs 可在 bootstrap 预切成 lane 分区而无需任何 ino。
- **declared 几何** `[0x04]stripe_geom` → rkyv `StripeLayout { lanes: u8,
  unit_bytes: u32 }`。`geom::read_stripe_geom` 每 session 读一次并缓存：key 存在=fs
  自声明；key 缺失=默认 `DEFAULT_STRIPE_LANES`=24 lanes、unit=`MAX_EXTENT`；**硬 KV
  错 PROPAGATE**（不吞成 lanes=1，否则一次瞬时 blip 造出永久单分区大文件，症状只有
  "吞吐莫名差"，最难诊断）。由 `autumn-op presplit --namespace fs --lanes N` 在切
  lane 边界的**同一命令**里写声明，声明与放置不会脱节。
- **24 lanes 过量供给**：任何整除 24 的分区数（1,2,3,4,6,8,12,24）都能均匀分布每个
  文件；lane 数是永久布局常量而非 cluster 形状的函数 → 一个 1-分区 fs 写的文件已按
  lane 排序，日后在 lane 边界 split 可 RETROACTIVELY 拿到并行度，无数据重写。
- **`striped_extent_offsets(size, unit)`** 枚举 `(0, u, 2u, …)` 的对齐偏移供
  reader/rm/delete 计算 lane key —— **`unit` 必来自文件 PERSISTED 的
  `StripeLayout.unit_bytes`，不是 `MAX_EXTENT` 常量**：`MAX_EXTENT` 会在满配硬件上
  retune，若按当下常量步进，缩小后老条带文件会枚举出不匹配的偏移 → 半个文件读成零而
  无错（稀疏语义）。`StripeLayout::checked()` 校验 `lanes ≥ 1 && unit_bytes ≥ 1`
  （防 key builder div-by-zero），每条读 `meta.stripe` 的路径必经它。
- **fuse mount 拒绝条带写**：`write::write` / `write::truncate` 对 `meta.stripe`
  非空的 inode 返回 "not supported yet; use autumnfs"。fuse **能读**条带文件，
  **条带写只由 `autumnfs` 做**（大文件 create 时按声明 stamp 成条带）。

## KV 数据模型

### 完整示例

```
/                          (ino=1, 目录)
└── docs/                  (ino=2, 目录)
    └── readme.txt         (ino=3, 文件, 600KB)
```

```
  [0x01][ino=1]  →  InodeMeta{ mode=S_IFDIR|0755, nlink=3, size=0, ... }
  [0x01][ino=3]  →  InodeMeta{ mode=S_IFREG|0644, nlink=1, size=614400, inline_data=None, stripe=None }
  [0x02][parent=1]["docs"]        →  { child_inode=2, file_type=DT_DIR }
  [0x02][parent=2]["readme.txt"]  →  { child_inode=3, file_type=DT_REG }   (文件名在 key，不在 value)
  [0x03][ino=3][off=0]            →  [≤ 8 MiB 原始字节]
  [0x04]["next_inode"] / ["schema_version"]=3 / ["stripe_geom"]=StripeLayout(rkyv)
```

`DirentValue.child_inode` 指向 `InodeMeta`；extent key 里的 ino 就是该 InodeMeta
的 inode 号；`InodeMeta.size` 界住可见 extent 范围。

### InodeMeta — "这个东西是什么"

key `[0x01][ino BE]`，描述文件/目录**自身属性**（对应 Linux `struct stat`）：
`mode`（类型+权限）、`uid`/`gid`、`size`（目录为 0）、`nlink`、`atime`/`mtime`/
`ctime`、`inline_data`（≤4KB 小文件数据）、`symlink_target`、`stripe`（条带几何或
None）。**不含文件名和父目录** —— 一个 inode 不知道自己叫什么、在哪，硬链接才能工作。

### DirentValue — "谁在哪个目录下叫什么名字"

key `[0x02][parent_ino BE][name]`，两个字段：`child_inode`、`file_type`
（DT_REG=8 / DT_DIR=4 / DT_LNK=10）。文件名在 key 里不在 value。`file_type` 与
`InodeMeta.mode` 冗余是**空间换时间**：`readdir` 直接返回每个条目类型，无需为每个条目
再查一次 InodeMeta（同 ext4 `ext4_dir_entry_2.file_type`）。

### 为什么 InodeMeta 与 DirentValue 分开

| 操作 | 只改 DirentValue | 只改 InodeMeta | 两者都改 |
|------|:---:|:---:|:---:|
| `rename` | ✓ | | |
| `chmod`/`chown` | | ✓ | |
| `write` | | ✓ (size/mtime) | |
| `link` | ✓ (新目录项) | ✓ (nlink++) | ✓ |
| `mkdir`/`unlink` | ✓ | ✓ | ✓ |

内嵌会使硬链接无法实现（多名共享属性）、rename 变重、chmod 要找到所有目录项。

### 各操作的 KV 访问模式

| FUSE 操作 | KV 操作 |
|-----------|---------|
| `lookup(parent, name)` | 1× Get dirent + 1× Get inode |
| `readdir(ino)` | 1× Range(prefix=[0x02][ino BE]) |
| `getattr(ino)` | 1× Get inode |
| `mkdir`/`create(parent, name)` | 1× Put inode + 1× Put dirent + 1× Put parent inode (nlink) |
| `unlink(parent, name)` | 1× Get dirent + 1× Delete dirent + tombstone + N× Delete extent + 1× Delete inode |
| `rename(old, new)` | 1× Get old dirent + 1× Delete old dirent + 1× Put new dirent |
| `read(ino, off, size)` | 每个重叠 extent 1× Get（sub-range，批量并发） |
| `write(ino, off, data)` | 缓冲后：对齐 1× Put / 非对齐 1× Get + 1× Put（RMW） |
| `truncate(ino, 0)` | meta Put（commit）+ N× Delete extent |

ino → inode 数据是 **O(log N) KV Get**（ino 编码在 key 里，LSM-tree 查找，非 ext4
的 O(1) 数组下标）；ino → 数据靠 extent key 隐式关联，物理位置由 KV 层透明管理。性能
差距主要在**网络 RTT**（每 Get 一次 RPC），FUSE 内核缓存（entry_timeout=30s）抵消
大部分重复 lookup。

## 常量

| 常量 | 值 | 说明 |
|------|-----|------|
| `MAX_EXTENT` | 8 MiB | extent value 上限；写缓冲按此粒度刷；≥64 KiB 整 extent 读走 ZC |
| `INLINE_THRESHOLD` | 4 KiB | 小文件 inline 阈值（匹配 VALUE_THROTTLE）|
| `WRITE_BUF_EXTENTS` | 8 | 每 inode 写缓冲容量（extent 数）|
| `WRITE_BUF_CAP` | 64 MiB | = `WRITE_BUF_EXTENTS × MAX_EXTENT`；>1 时 `write_region` 拆多 extent 由 `put_many` 按 `APPEND_PIPELINE_DEPTH` 流水 |
| `INODE_ALLOC_BATCH` | 1000 | 每批向 manager 领的 inode 数 |
| `DEFAULT_STRIPE_LANES` | 24 | fs 未声明几何时的默认 lane 数 |
| `ROOT_INO` | 1 | 根 inode（FUSE_ROOT_ID）|
| `SCHEMA_VERSION` | 3 | 见下 fail-loud |
| `DT_REG`/`DT_DIR`/`DT_LNK` | 8/4/10 | 目录项类型 |

## 核心数据结构

```rust
struct InodeMeta {          // rkyv, key [0x01][ino BE]
    mode: u32, uid: u32, gid: u32, size: u64, nlink: u32,
    atime_secs: i64, atime_nsecs: u32,
    mtime_secs: i64, mtime_nsecs: u32,
    ctime_secs: i64, ctime_nsecs: u32,
    inline_data: Option<Vec<u8>>,     // ≤4KB 小文件
    symlink_target: Option<Vec<u8>>,
    stripe: Option<StripeLayout>,     // Some = 条带化, None = 单分区 legacy
}

struct DirentValue { child_inode: u64, file_type: u8 }  // key [0x02][parent BE][name]

struct StripeLayout { lanes: u8, unit_bytes: u32 }       // [0x04]stripe_geom + per-inode

// 运行时状态（compio 线程本地，不持久化）
struct InodeState {
    meta: InodeMeta,
    write_buf: Option<WriteBuffer>,        // buf 容量 WRITE_BUF_CAP
    dirty: bool,
    open_count: u32,
    extents: Option<Vec<(u64, u32)>>,      // F247 运行时 extent map（truncate 置 None）
    cached_version: u64,                   // 上次从 KV 刷 meta/extents 时的 lease 版本
}
```

`cached_version`：Open 时与 AcquireLease 返回的版本比对，不符则丢弃缓存 InodeState 从
`get_inode` 重建（+ 下次 rescan extent），维持 close-to-open 一致性（第二 mount
Open 已被首 mount 写关的 inode 不会读到陈旧 `meta`）。

## 操作路径

### Read
1. 脏写缓冲与读范围重叠 → 先 flush（read-after-write 一致性）。
2. 小文件 `inline_data` → 直接返回。
3. 加载 extent map（F247 运行时缓存，冷启动 range-scan），条带文件先 `checked()` 校验
   几何，为每个重叠 extent 生成 `ChunkSpec`（striped → lane key，否则 `[0x03][ino][off]`），
   sub-range = 精确重叠区间，extent 间空洞补零（稀疏语义）。
4. 一次批量读所有 extent slice，多 extent 并发（compio spawn，spawned `execute`
   不持 `&FsState`）。

**`--direct-read`（默认 ON）**：`read::execute` 用 `get_many_direct` 取代
`get_many_into`，≥ 64 KiB 整 extent 读**绕过 PS 直读 EN**（PS 网卡出流量离开数据
路径，大文件/模型服务跨机吞吐更高）；< 64 KiB 仍走 PS proxy（逐项按大小 gate）。
安全：每项直读失败**逐项回退 PS proxy**（authoritative），首次回退 client 打一次 WARN
（EN 不可达）。若 EN 数据口在 PS-only 子网，用 `--direct-read false` 省掉每 extent 一个
redirect RTT。落点：`FsState.direct_read` → `ReadPlan.direct_read` → `execute` 选原语。

### Write（带缓冲）
1. 懒分配 `WriteBuffer`（容量 `WRITE_BUF_CAP`=64 MiB）。
2. gap 检测（写偏移不连续）→ flush 当前缓冲。
3. 拷贝到 buffer；满一个 buffer → `extent::write_region` 刷（拆成 `MAX_EXTENT` 封顶、
   互不重叠的 extent，`put_many` 流水）。
4. 标记 dirty。

### Flush
`extent::write_region`：对齐区间直接 Put；非对齐落在已有 extent 内 → RMW（读旧值、
覆盖子区间、回写）。之后更新 `InodeMeta.size`。

### 目录操作
- **lookup**：Get dirent → Get inode。
- **readdir**：dirent 前缀 Range scan。
- **mkdir**：alloc inode + Put meta + Put dirent + parent nlink。
- **rename**：Delete old dirent + Put new dirent（非原子，v1 限制；rename-over 见
  UNLINK-1）。

## 模块职责（core / fuse 两层）

**`core` feature —— fuser-free 文件系统核心**（`--no-default-features --features
core` 可独立编译，唯一额外依赖 libc；返回裸 `InodeMeta`/`DT_*`，供 PyO3 `autumn.Fs`
绑定与 fsspec facade 复用）：

| 文件 | 职责 |
|------|------|
| `key.rs` | KV key 编码/解码（含 striped key builder；恒编译）|
| `schema.rs` | InodeMeta / DirentValue / StripeLayout / ReaddirEntry / WriteBuffer + 常量（恒编译）|
| `geom.rs` | declared stripe 几何 read/write（`[0x04]stripe_geom`；恒编译）|
| `meta.rs` | inode 元数据 get/put、`alloc_inode`（manager 取号）、`ensure_root`/`ensure_schema_version`、S_IF* mode 常量 |
| `dir.rs` | lookup/readdir/mkdir/rmdir/rename/create/unlink/resolve —— 返回 `(ino, InodeMeta)` / DT_* 条目 |
| `extent.rs` | 变长 extent 寻址/写/RMW/截断/删除/`clean_beyond_eof`/`remove_unreachable_inode` |
| `read.rs` / `write.rs` | 分块读组装 / 写缓冲 + flush |
| `lease_tasks.rs` | per-session lease 后台任务（heartbeat + invalidation poll + revoked 驱逐；fuser-free）|
| `state.rs` | `FsState`（ClusterClient、inode 批次游标、lease 簿记、`direct_read`）|

**`fuse` feature（default，含 `core`）—— 内核挂载胶水**：

| 文件 | 职责 |
|------|------|
| `main.rs` | 二进制入口 + 30s 周期脏 inode sync + CLI |
| `attr.rs` | **唯一的 core→fuser 转换点**：`inode_to_attr` / `dt_to_filetype` |
| `bridge.rs` | `FsRequest` enum、FUSE↔compio channel 桥接 |
| `ops.rs` | `fuser::Filesystem` trait 实现（readdir 在 reply 边界做 DT_*→FileType）|
| `dispatch.rs` | compio 侧派发循环（lookup/mkdir 在此转 FileAttr；lease 三方法 `pub use` 自 `lease_tasks`）|

不变量：core 文件**禁止 import fuser**（`cargo tree --features core` 中 fuser 计数
为 0）；新的 core→fuser 转换一律进 `attr.rs`。PyO3 `autumn.Fs`（`python/src/fs.rs`）
与 fsspec facade 调用的就是这份 Rust core，一处实现两个前端不 drift；绑定用一个专属
compio worker 线程独占 `!Send` 的 `FsState`（Python 同步方法 ship job 阻塞取结果）。

## Per-session lease + 跨前端围栏

per-session lease 后台任务（5s heartbeat 续所有 held lease + 持久 invalidation
long-poll + `LeaseRevoked` 驱逐）在 `lease_tasks.rs`（core）；mount 传真 kernel
invalidator，binding 传 None（headless，无内核页缓存驱逐）。

- **写写围栏**：写路径 `acquire(WRITE)` 环绕；冲突时 fsspec facade 抛
  `BlockingIOError`，被抢占租约标 revoked、`write` 对 revoked 租约快失败（无租约的
  匿名写仍放行）。写租约在长写期间被续。
- **读一致性**：靠 fresh-read + Q1 只写租约（binding 只在写时缓存，release 时
  `forget` 驱逐 inode 缓存）。

## Schema 版本戳（fail-loud）

`schema::SCHEMA_VERSION` = **3**，存于 `[0x04]schema_version`（相对 key，即
`fs/[0x04]schema_version`）。`meta::ensure_schema_version` 在 mount（`ensure_root`
入口）缺则戳、有则核对、**不符则 fail-loud 拒挂**（防未来不兼容布局静默读写坏数据）。
- v1 = pre-namespace 裸 key（从不戳）。
- v2 = namespaced 相对布局 + 全局 inode 计数器。
- v3 = F-FS-STRIPE：`InodeMeta` 加 `stripe` 字段（rkyv 布局变，v2 inode 字节解不出），
  大文件走 lane-striped key。v2→v3 stop-world reset，无 in-place 迁移；小/legacy 文件
  仍 `stripe=None` + `[0x03][ino][off]`。BUMP whenever 布局/编码不兼容变更。

## 配置（CLI）

`autumn-fuse` 参数：`--manager`（default `127.0.0.1:9001`）、`--mountpoint`、
`--credential-file`（authz 保护 `fs/` 时必需；`<principal>\n<hex>`，覆盖不到 `fs/`
则 fail-fast）、`--allow-other`（default false）、`--transport`（`tcp`/`ucx`，须与
cluster 一致）、`--direct-read`（default true）。内核缓存 `attr_timeout` /
`entry_timeout` = 30s、`negative_timeout` = 5s；周期脏 inode sync 间隔 30s（`main.rs`）。

## 关键依赖文件

| 文件 | 用途 |
|------|------|
| `crates/client/src/lib.rs` | ClusterClient — 所有 KV 操作入口 |
| `crates/rpc/src/partition_rpc.rs` | PutReq/GetReq/RangeReq/DeleteReq |
| `crates/rpc/src/server.rs` | Dispatcher 模式参考（bridge 设计）|

## Crash-consistency contract

fuse 层无多 key 原子提交（完整方案 per-inode generation manifest 仍 deferred）。保证
的是严格的顺序纪律：崩溃**永不伪造数据**，最多丢失最近未 fsync 的写。规则（present-tense
不变量 + 一句原因）：

- **Grow / write**：extent KV put 全副本 ACK（`write_region(..).await?`）**后**才推进
  并持久化 inode-meta size。原因：崩溃只让 durable size 落后于已写 extent（文件看起来
  更旧，未 fsync 数据 POSIX 可接受）；beyond-size extent 是良性孤儿（读被 size 界住不
  可见、regrow 用同 `[0x03][ino][off]` key 覆盖、unlink/truncate 前缀扫描回收）。
- **Read-after-write barrier**：read 前若脏写缓冲与读范围重叠**必先 flush**。原因：
  否则读到未落盘的旧内容，破坏 read-after-write 一致性。
- **In-place overwrite (RMW) read barrier**：部分写落在已有 extent 内必须
  read-modify-write，读旧值**必用 `kv_get_opt` 屏障并 PROPAGATE 硬错**（同
  `clean_beyond_eof`）。原因：把瞬时 RPC/routing/storage 错吞成空值会零填未触及前缀
  `[start, offset)`、截断未触及后缀、再 put → 在成功写里伪造零/丢字节（`get`/`put`
  各有独立 ~13s 重试预算，PS 短暂不可用时 get 耗尽而 put 后成功 → 确定性损坏）。只有
  真正 `Ok(None)`（已映射 extent 不该出现）当稀疏空值。守卫 `scripts/fuse_rmw_chaos.sh`
  （单 PS，partial overwrite + PS kill + restart-at-16s 使 get 预算耗尽而 put 落地，
  断言未触及前缀永不被零）。
- **Shrink / truncate**：inode-meta put 是 **COMMIT POINT**，先落，再删/缩 extent。
  原因：反序会在 durable size=old 但尾数据已删时读到文件内部零。**不变量：
  content[0..size] 永远等于最后成功写入的内容；崩溃只能选 old/new 哪个 size 存活。**
  守卫 `fuse_chaos.sh` T1（truncate burst + kill -9 mid-burst + remount + 前缀精确校验）。
- **Grow 上的 leftover reaping**（`clean_beyond_eof(ino, eof)`）：每条 grow 路径先做
  raw 前缀扫描，删 ≥ eof 的整 key、按 straddler 的**真实 KV value 长度**缩它。原因：
  meta-first shrink 后崩溃残留的 beyond-size extent，日后 grow 会当作复活的旧数据
  （POSIX 要求零）。必须在内存 size bump **之前**跑（bump 抹掉 pre-grow EOF，否则
  flush 时 `write_region` 看到已 grow 的 size、陈旧 straddler 尾看似合法 in-file
  数据）。跑在 `write::write` 入口（`offset > size`）、`write::truncate` grow 分支
  （meta put 前）、`write_region` 入口（防御）。硬 kv 错传播中止 grow，只有真正缺失
  key 可跳过（`kv_get_opt`）。连续 append 自界（old-EOF 的新 key 封住 straddler 推断
  长度），热路径只付一次 size 比较。
- **Post-commit cleanup 错误不失败 truncate**：meta 落地即已提交；上报清理错误会让
  caller 重试进 `new_size == old_size` 早返回（no-op，永不重清）。WARN + invalidate，
  残留由下次 grow/unlink 回收。
- **UNLINK-1 tombstone**：unlink 与 rename-over 通过 `extent::remove_unreachable_inode`
  删目标数据 —— inode 变 UNREACHABLE 的**瞬间**写 intent tombstone
  （`[0x04]rmtomb/[ino]`），再删 extent + inode key + tombstone；
  `sweep_unlink_tombstones` 每次 mount（Init）重放幸存者。**不变量：tombstone 只为
  不可达 inode 写**（sweep 无条件删）—— rename-over 里这强制删除发生在 dirent 覆盖
  之后。原因：残留泄漏窗口从"整次扫描 + N 删"缩到单次 unreachability→tombstone RPC
  gap。rename-over 必须同时删目标 **extent**（否则 POSIX atomic-save「写 tmp；mv tmp
  file」每次泄漏整份旧内容）。守卫 `fuse_chaos` T3。
- **Read-path 边界短路**（partition-server + ioring）：完全越过 VP value 末尾的
  sub-range GET 短路成空（`read_value_from_log` 不返回复用 RegPool buffer 的陈旧
  内容）；caller 侧 ioring `read_into` 对短/空 extent slice 的未写尾清零（dest 是复用
  ring buffer，非新零 Vec）。原因：读缩短/稀疏 extent 窗口应得零而非 garbage。

## statfs — 保守 3 副本映射

`df -h <mountpoint>` 的 `Statfs` 调 `state.client.cluster_df()`（`MSG_CLUSTER_DF`
聚合快照，每 EN 的 RAW + autumn physical_used 求和），**按 3 副本因子保守映射**：
`blocks = raw_total/3/4096`，`bavail = bfree = raw_free/3/4096`。EC 下可用逻辑容量是
个区间（cold EC 1.25–1.33× vs hot 3×），statfs 是单标量 → 收敛到 WORST 因子（CephFS
式），使 `df` 绝不高报空闲、不会诱使 writer 乐观 ENOSPC（低报是安全侧）。调用有界
（`compio::time::timeout` 2s），超时/错回退到良性大默认值。文件 `size` 保持逻辑大小
（副本/EC 放大对 FS 层透明）；inode 计数为常量。

## Restart 行为 —— EN vs PS

- **PS kill+restart**（`scripts/fuse_chaos.sh` F1）：分区 MIGRATE 到另一 PS，region
  重收敛后 I/O 恢复，已 sync 文件字节精确。RMW-GET-SWALLOW 窗口由
  `scripts/fuse_rmw_chaos.sh` 覆盖。
- **manager / fuse-daemon kill+restart**：`fuse_chaos.sh` F2/F3。
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
