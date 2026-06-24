# autumn-fuse Architecture Guide

## Purpose

FUSE 文件系统层，将 autumn-rs KV 存储挂载为 POSIX 文件系统。设计借鉴 3FS (DeepSeek/3FS) 的高性能 FUSE 架构。

## 3FS FUSE 性能架构分析

### 3FS 采用的关键性能模式

3FS 的 FUSE 实现（`3FS/src/fuse/`）通过以下手段实现极高性能：

#### 1. 写缓冲 (InodeWriteBuf)
**来源**: `3FS/src/fuse/FuseOps.cc` lines 1552-1680

每个 inode 一个 1MB 写缓冲区，延迟刷写：
- 顺序写入累积到缓冲区，满时才刷到存储层
- Gap 检测：如果写入偏移不连续，立即 flush 当前缓冲
- O_DIRECT 绕过缓冲直接写入
- RDMA 注册内存，避免额外拷贝

```cpp
// 3FS 写逻辑核心
if (wb->len && wb->off + wb->len != off) {
    flushBuf(req, pi, wb->off, *wb->memh, wb->len, true);  // gap → flush
}
memcpy(wb->buf.data() + wb->len, buf, size);
wb->len += size;
if (wb->len == wb->buf.size()) {
    flushBuf(...);  // buffer full → flush
}
```

#### 2. 周期异步 Sync
**来源**: `3FS/src/fuse/FuseClients.cc` lines 159-164

- 30 秒间隔，±30% 抖动（防止惊群）
- 脏 inode 集合 (`dirtyInodes`) 在写完成时标记
- 后台扫描刷写，不阻塞应用写操作
- 每轮最多处理 1000 个脏 inode

#### 3. 内核级元数据缓存
**来源**: `3FS/src/fuse/FuseConfig.h` lines 24-28

```
attr_timeout   = 30s   // getattr 结果缓存
entry_timeout  = 30s   // lookup 结果缓存
negative_timeout = 5s  // ENOENT 缓存
```

FUSE 内核模块直接缓存这些结果，30 秒内重复 stat/lookup 完全不到用户态。

#### 4. 自定义 I/O Ring（共享内存）
**来源**: `3FS/src/fuse/IoRing.h` lines 53-215

通过共享内存实现 lock-free 的提交/完成队列：
- 原子操作 + 信号量协调
- 批量提交多个 I/O 再唤醒 worker
- 3 级优先级（hi/normal/lo）
- **autumn-fuse v1 不实现**，用 channel 桥接代替

#### 5. 批量 I/O 处理
**来源**: `3FS/src/fuse/IoRing.cc` lines 67-284, `PioV.cc` lines 132-183

- Ring 级别批量：一次取最多 32 个 I/O 请求
- 文件查找去重：同批次中同一文件只查找一次
- Chunk 级别批量：所有 chunk 的 storage I/O 打包成一个 batchRead/batchWrite

#### 6. 元数据/数据路径分离
- 元数据操作（lookup, getattr, mkdir）：同步 RPC，结果缓存
- 数据操作（read, write）：异步缓冲，批量提交
- 每个 inode 的 DynamicAttr 独立跟踪长度/时间戳 hint

### autumn-fuse 采纳决策

| 3FS 模式 | autumn-fuse | 原因 |
|----------|-------------|------|
| 写缓冲 1MB/inode | ✅ 采用 | 关键性能优化，直接移植 |
| 周期 sync 30s+jitter | ✅ 采用 | 防止数据丢失窗口 |
| 内核缓存 30s timeout | ✅ 采用 | 零成本，效果显著 |
| 元数据/数据分离 | ✅ 采用 | 自然匹配 |
| I/O Ring 共享内存 | ❌ 跳过 v1 | 复杂度极高，channel 足够 |
| 3 级优先级 worker | ❌ 跳过 v1 | 依赖 I/O Ring |
| 批量 chunk I/O | ⚠️ 部分 | 多 chunk 读并发化 |

---

## 架构设计

### 整体架构

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
│                                                   │
│  ┌─────────┐   crossbeam     ┌───────────────┐  │
│  │ fuser   │──channel───>│ compio thread  │  │
│  │ threads │<─oneshot────│ + ClusterClient│  │
│  └─────────┘              └───────────────┘  │
│                                                   │
│  写缓冲 (1MB/inode) | inode 缓存 | 周期 sync     │
└────────────────────┬────────────────────────────┘
                     │ autumn-rpc (binary RPC)
┌────────────────────▼────────────────────────────┐
│         PartitionServer (KV 层)                   │
│         Put / Get / Delete / Range               │
└──────────────────────────────────────────────────┘
```

### FUSE 线程 ↔ compio 桥接

核心挑战：`fuser` 在自己的线程中调用回调，`ClusterClient` 使用 `Rc<RpcClient>` 是 `!Send`。

方案参考 `crates/rpc/src/server.rs` 的 Dispatcher 模式：

```rust
// bridge.rs
enum FsRequest {
    Lookup { parent: u64, name: OsString, reply: oneshot::Sender<Result<(FileAttr, u64)>> },
    GetAttr { ino: u64, reply: oneshot::Sender<Result<FileAttr>> },
    Read { ino: u64, offset: i64, size: u32, reply: oneshot::Sender<Result<Vec<u8>>> },
    Write { ino: u64, offset: i64, data: Vec<u8>, reply: oneshot::Sender<Result<u32>> },
    // ... 其他操作
}
```

fuser 回调线程 → crossbeam::channel::send(FsRequest) → compio 线程 recv + 处理 → oneshot 回复

### Inode-based 路径映射

采用 inode 方案（非扁平 path=key）：
- rename O(1)：只改目录项
- hardlink：多目录项指向同一 inode
- 根 inode = 1 (FUSE_ROOT_ID)
- inode 分配器存在 KV (`[0x04]next_inode`)，批量预分配 1000 个

### KV Key 编码

| 前缀 | 用途 | Key 格式 | Value |
|------|------|---------|-------|
| `0x01` | Inode 元数据 | `[0x01][ino: u64 BE]` | InodeMeta (rkyv) |
| `0x02` | 目录项 | `[0x02][parent: u64 BE][name]` | DirentValue (rkyv) |
| `0x03` | 文件数据 extent | `[0x03][ino: u64 BE][logical_off: u64 BE]` | raw bytes ≤ 8 MiB (`MAX_EXTENT`) |
| `0x04` | FS 超级块 | `[0x04][field]` | varies |

Big Endian 保证自然排序，同父目录项聚集、同文件 extent 按逻辑偏移连续有序。

> **F247 — 变长 extent（取代固定 256 KiB chunk）。** 文件数据不再是固定 256 KiB
> 块，而是**按逻辑字节偏移寻址的变长 extent**（key = `[0x03][ino][logical_off BE]`，
> value ≤ 8 MiB = `MAX_EXTENT`）。顺序写（write-once）合并成接近 8 MiB 的 extent，
> 末尾/部分 extent 较短 → "像 Linux extent 一样变长"。动机：模型文件等大文件以前会
> 散成几十万个 256 KiB chunk（LSM key 基数爆炸 + 每读散成大量小 RPC），现在变成数量级
> 更少、每个 ≥ 64 KiB 的 extent —— **每个整 extent 读都走 `get_many_into` 的 ZC 路径
> （`MSG_GET_ZC`）**，正是 F243 RDMA 零拷贝要利用的尺寸。
>
> - **持久真相 = extent KV key 本身**（隐式 key 设计，InodeMeta 里**不**存 extent 列表）。
> - **运行时缓存**：`InodeState.extents: Option<Vec<(start, len)>>` —— 冷启动用
>   range-scan `[0x03][ino]` 前缀拿到起始偏移 + 由相邻起始/文件大小推断长度；写时增量
>   维护，truncate 时失效。
> - **不变量：extent 互不重叠**。读按 `[start, start+len)` 请求每个重叠 extent 的精确
>   子区间，PS get 会按真实 value 长度裁剪、dest 余下补零 —— 短 extent / 稀疏空洞都正确。
> - 全部寻址/读/写/截断/删除逻辑在 `crate::extent`。

### KV 数据模型详解

所有文件系统数据存在同一个 autumn-rs KV namespace 中，靠 key 的第一个字节区分类型。

#### 完整示例

假设文件系统内容：
```
/                          (ino=1, 目录)
└── docs/                  (ino=2, 目录)
    └── readme.txt         (ino=3, 文件, 600KB)
```

KV 存储的全部内容：

```
─── 0x01: InodeMeta (每个文件/目录一条) ───────────────────────

  [0x01][ino=1]  →  { mode=S_IFDIR|0755, nlink=3, uid=501, gid=20,
                       size=0, atime, mtime, ctime,
                       inline_data=None, symlink_target=None }

  [0x01][ino=2]  →  { mode=S_IFDIR|0755, nlink=2, ... }

  [0x01][ino=3]  →  { mode=S_IFREG|0644, nlink=1, size=614400,
                       inline_data=None, ... }

─── 0x02: DirentValue (每个"父→子"关系一条) ──────────────────

  [0x02][parent=1]["docs"]        →  { child_inode=2, file_type=DT_DIR }
  [0x02][parent=2]["readme.txt"]  →  { child_inode=3, file_type=DT_REG }

  注意: 文件名编码在 key 里 (第 9 字节之后), 不在 value 里。

─── 0x03: Chunk (文件数据, 每块最大 256KB) ────────────────────

  [0x03][ino=3][chunk=0]  →  [256KB 原始字节]   ← 文件 0-256KB
  [0x03][ino=3][chunk=1]  →  [256KB 原始字节]   ← 文件 256-512KB
  [0x03][ino=3][chunk=2]  →  [88KB 原始字节]    ← 文件 512-600KB

  目录没有 chunk。chunk 数量 = ceil(size / 256KB)。

─── 0x04: Superblock (全局状态) ──────────────────────────────

  [0x04]["next_inode"]  →  [u64 BE: 1001]   ← 下一批 inode 分配起点
```

#### 三者的关系

```
     DirentValue                 InodeMeta                Chunk Data
  (父子关系 + 名字)           (文件/目录属性)             (文件内容)

[0x02][parent=1]["docs"]      [0x01][ino=2]
{ child_inode: 2 ──────────→ { mode: DIR               (目录没有 chunk)
  file_type: DIR }              nlink: 2, ... }

[0x02][parent=2]["readme.txt"] [0x01][ino=3]            [0x03][ino=3][0] → 256KB
{ child_inode: 3 ──────────→ { mode: REG               [0x03][ino=3][1] → 256KB
  file_type: REG }              size: 614400            [0x03][ino=3][2] → 88KB
                                nlink: 1, ... }
```

DirentValue.child_inode 指向 InodeMeta。InodeMeta.size 隐含了 chunk 数量。
chunk key 中的 ino 就是 InodeMeta 的 inode 号。

#### InodeMeta — "这个东西是什么"

存储在 key `[0x01][ino BE]`，描述一个文件或目录**自身的全部属性**。
对应 Linux `struct stat`，`ls -l` 显示的所有信息都来自这里。

| 字段 | 说明 |
|------|------|
| `mode` | 文件类型 + 权限。如 `S_IFREG\|0644` = 普通文件 owner 读写 |
| `uid` / `gid` | 所属用户和组 |
| `size` | 文件逻辑大小（字节），目录为 0 |
| `nlink` | 硬链接计数。文件默认 1，目录默认 2（`. ` 和父目录的指向） |
| `atime` | 最后访问时间 |
| `mtime` | 最后数据修改时间 |
| `ctime` | 最后元数据变更时间（chmod、chown 等） |
| `inline_data` | ≤4KB 小文件的数据直接存在这里，省掉 chunk KV 操作 |
| `symlink_target` | 符号链接的目标路径 |

**不包含**：文件名、父目录。一个 inode 不知道自己叫什么名字，也不知道在哪个目录下。
这样硬链接才能工作——同一个 inode 可以有多个名字。

#### DirentValue — "谁在哪个目录下叫什么名字"

存储在 key `[0x02][parent_ino BE][name]`，只有两个字段：

| 字段 | 说明 |
|------|------|
| `child_inode` | 指向的 inode 号 |
| `file_type` | DT_REG(8)=文件, DT_DIR(4)=目录, DT_LNK(10)=符号链接 |

文件名不在 value 里，而是编码在 **key 本身**的第 9 字节之后。

`file_type` 和 InodeMeta.mode 中的信息是冗余的，但 `readdir` 需要返回每个条目的类型。
如果不冗余存储，readdir 就要为每个条目额外查一次 InodeMeta，N 个文件就是 N 次 KV Get。
这是用空间换时间——和 Linux ext4 的 `struct ext4_dir_entry_2.file_type` 设计一致。

#### Chunk — 文件的原始字节

存储在 key `[0x03][ino BE][chunk_idx BE]`，value 是原始文件字节，最大 256KB。

对一个 600KB 的文件：
- chunk 0: 字节 0-262143 (256KB)
- chunk 1: 字节 262144-524287 (256KB)
- chunk 2: 字节 524288-614399 (88KB，最后一块不满)

目录没有 chunk。小文件 (≤4KB) 也没有 chunk，数据 inline 在 InodeMeta 中。

#### 为什么 InodeMeta 和 DirentValue 分开存储

类比 Linux 文件系统，inode 和目录项是分离的两种数据结构：

| 操作 | 只改 DirentValue | 只改 InodeMeta | 两者都改 |
|------|:---:|:---:|:---:|
| `rename` | ✓ | | |
| `chmod` / `chown` | | ✓ | |
| `write` (改内容) | | ✓ (size/mtime) | |
| `link` (硬链接) | ✓ (新目录项) | ✓ (nlink++) | ✓ |
| `mkdir` | ✓ | ✓ | ✓ |
| `unlink` | ✓ (删目录项) | ✓ (nlink--) | ✓ |

如果把 InodeMeta 嵌入 DirentValue：
- **硬链接无法实现**：同一文件两个名字需要共享同一份属性
- **rename 变重**：要读写更大的 value
- **chmod 要找到所有目录项**：不知道文件有几个名字、在哪些目录下

#### 小文件特例 (≤4KB)

小文件不产生 chunk，数据直接存在 InodeMeta.inline_data 中：

```
[0x01][ino=5]  →  InodeMeta{
    size: 18,
    inline_data: Some(b"hello autumn-fuse\n"),  ← 数据在这里
    ...
}
[0x02][parent=1]["hello.txt"]  →  { child_inode=5, file_type=DT_REG }
```

只有 2 条 KV 记录（1 InodeMeta + 1 DirentValue），没有 `[0x03]` chunk。
读写各省一次 KV 操作。当文件增长超过 4KB 时，迁移到 chunk 存储。

#### 各操作的 KV 访问模式

| FUSE 操作 | KV 操作 |
|-----------|---------|
| `lookup(parent, name)` | 1× Get dirent + 1× Get inode |
| `readdir(ino)` | 1× Range(prefix=[0x02][ino BE]) |
| `getattr(ino)` | 1× Get inode |
| `mkdir(parent, name)` | 1× Put inode + 1× Put dirent + 1× Put parent inode (nlink) |
| `create(parent, name)` | 同 mkdir |
| `unlink(parent, name)` | 1× Get dirent + 1× Delete dirent + N× Delete chunks + 1× Delete inode |
| `rename(old, new)` | 1× Get old dirent + 1× Delete old dirent + 1× Put new dirent |
| `read(ino, off, size)` | ceil(size/256KB)× Get chunk |
| `write(ino, off, data)` | 缓冲后: per-chunk 1× Put (对齐) 或 1× Get + 1× Put (非对齐) |
| `truncate(ino, 0)` | N× Delete chunk + 1× Put inode |

#### 与 Linux ext4 的架构对比

从 dirent 到文件数据的完整查找链路：

```
Linux ext4:                              autumn-fuse:

dirent("readme.txt") → ino=42          [0x02][parent]["readme.txt"] → {ino=42}
         │                                        │
         ▼                                        ▼
inode_table[42]       O(1) 算术         KV Get [0x01][ino=42]     O(log N)
  { size, mode,                           { size, mode,
    extents → disk blocks }                 inline_data }
         │                                        │
         ▼                                        ▼
disk block 1001,1002,1003               KV Get [0x03][ino=42][chunk=0,1,2]
```

ext4 的 inode table 是 mkfs 时预分配的**固定大小数组**，inode 号直接当下标算磁盘偏移：

```
ext4:    &inode_table + ino * 256B      → 一次算术，O(1)
autumn:  KV Get([0x01][ino BE])         → LSM-tree 查找，O(log N)
         (memtable SkipMap.seek → bloom filter → SSTable 二分查找)
```

| | ext4 | autumn-fuse |
|---|---|---|
| ino → inode 数据 | **O(1) 算术**（ino 是数组下标） | **O(log N) KV Get**（ino 编码在 key 里） |
| inode → file data | inode 里存 block 指针/extent tree | ino 编码在 chunk key 里，隐式关联 |
| 数据位置管理 | inode 自己管（extent tree） | KV 存储层管（LSM-tree + ValuePointer） |

ext4 的 inode 里存了 `i_block[15]` 数组或 extent tree，直接指向数据所在的磁盘 block 号。
autumn-fuse 不需要显式的 block 指针——chunk key `[0x03][ino][chunk_idx]` 本身就是寻址方式，
数据的物理位置由 KV 存储层（LSM-tree → stream layer → extent node）透明管理。

实际性能差距主要在**网络 RTT**（每次 KV Get 是一次 RPC 到 PartitionServer），
而非查找算法本身。FUSE 内核缓存（entry_timeout=30s）抵消了大部分重复 lookup 开销。

### 数据存储 (F247 变长 extent)

- **Extent 上限**: 8 MiB (`MAX_EXTENT`)，变长（顺序写合并到上限，末尾较短）
  - 远大于 4KB VALUE_THROTTLE → 走 ValuePointer（高效）
  - 写缓冲也按 8 MiB 刷写（每满一个 extent 落一条 KV）
  - ≥ 64 KiB → 整 extent 读走 ZC (`MSG_GET_ZC`)
- **小文件优化**: ≤4KB inline 在 InodeMeta.inline_data 中（无 extent）
- **部分 extent 读**: 利用 `GetReq.offset + length` 做 in-extent sub-range 读；读结果按
  绝对 dest 偏移拼装，extent 间空洞补零（稀疏文件语义）

### 核心数据结构

```rust
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct InodeMeta {
    mode: u32,              // S_IFREG | 0644, S_IFDIR | 0755
    uid: u32,
    gid: u32,
    size: u64,
    nlink: u32,
    atime_secs: i64,
    atime_nsecs: u32,
    mtime_secs: i64,
    mtime_nsecs: u32,
    ctime_secs: i64,
    ctime_nsecs: u32,
    inline_data: Option<Vec<u8>>,     // ≤4KB 小文件
    symlink_target: Option<Vec<u8>>,  // 符号链接
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct DirentValue {
    child_inode: u64,
    file_type: u8,  // DT_REG=8, DT_DIR=4, DT_LNK=10
}

// 运行时状态 (compio 线程本地)
struct InodeState {
    meta: InodeMeta,
    write_buf: Option<WriteBuffer>,
    dirty: bool,
    open_count: u32,
}

struct WriteBuffer {
    buf: Vec<u8>,    // capacity = 1MB
    offset: i64,
    len: usize,
}
```

---

## 操作路径

### Read 路径
```
read(ino, offset, size):
  1. 脏写缓冲与读范围重叠 → 先 flush
  2. 小文件 inline_data → 直接返回
  3. 计算 chunk 范围，per-chunk Get RPC（利用 sub-range）
  4. 多 chunk 并发读 (compio spawn)
```

### Write 路径 (带缓冲)
```
write(ino, offset, data):
  1. 懒分配 1MB WriteBuffer
  2. gap 检测 → flush
  3. 拷贝到 buffer
  4. buffer ≥ CHUNK_SIZE → flush 一个 chunk
  5. 标记 dirty
```

### Flush 路径
```
flush_buffer(ino):
  对每个 chunk:
    - 部分写 → read-modify-write
    - 整块写 → 直接 Put
  更新 InodeMeta.size
```

### 目录操作
- **lookup**: Get dirent key → Get inode meta
- **readdir**: Range scan on dirent prefix
- **mkdir**: Allocate inode + Put meta + Put dirent + Update parent nlink
- **rename**: Delete old dirent + Put new dirent (非原子, v1 限制)

---

## 模块职责

| 文件 | 职责 |
|------|------|
| `main.rs` | 二进制入口，解析参数，启动 mount |
| `lib.rs` | FuseConfig, mount() |
| `schema.rs` | InodeMeta, DirentValue, WriteBuffer 类型定义 |
| `key.rs` | KV key 编码/解码工具函数 |
| `bridge.rs` | FsRequest enum, FUSE↔compio channel 桥接 |
| `ops.rs` | fuser::Filesystem trait 实现 |
| `dir.rs` | lookup, readdir, mkdir, rmdir, rename |
| `meta.rs` | getattr, setattr, create, unlink, mknod |
| `read.rs` | 分块读取 + 组装 |
| `write.rs` | 1MB 写缓冲 + flush 逻辑 |
| `cache.rs` | inode 缓存管理 |
| `sync_task.rs` | 30s 周期脏 inode sync |

---

## 配置

```rust
struct FuseConfig {
    manager_addr: String,
    mountpoint: String,

    // 缓存 (来自 3FS)
    attr_timeout_secs: f64,      // 默认 30
    entry_timeout_secs: f64,     // 默认 30
    negative_timeout_secs: f64,  // 默认 5

    // 写缓冲 (来自 3FS)
    write_buf_size: usize,       // 默认 1MB
    chunk_size: usize,           // 默认 256KB

    // 周期 sync (来自 3FS)
    sync_interval_secs: u64,     // 默认 30
    sync_max_dirty: usize,       // 默认 1000

    // FUSE
    allow_other: bool,
    max_readahead: usize,        // 默认 16MB
}
```

---

## 实现分阶段

### Phase 1 — MVP
init, destroy, lookup, forget, getattr, setattr, mkdir, rmdir, unlink, rename,
create, open, read, write, flush, release, fsync, opendir, readdir, releasedir, statfs

### Phase 2 — 完善
symlink, readlink, link, readdirplus, xattr

### Phase 3 — 高级性能
I/O Ring, copy_file_range, fallocate

---

## 关键依赖文件

| 文件 | 用途 |
|------|------|
| `crates/client/src/lib.rs` | ClusterClient — 所有 KV 操作入口 |
| `crates/rpc/src/partition_rpc.rs` | PutReq/GetReq/RangeReq/DeleteReq |
| `crates/rpc/src/server.rs` | Dispatcher 模式参考（bridge 设计） |

## Crash-consistency contract (BUG-LEASE-8, 2026-06-12)

The fuse layer has NO multi-key atomic commit (the full fix — a per-inode
generation manifest — stays deferred in feature_list BUG-LEASE-8). What IS
guaranteed is a strict ordering discipline so that a crash anywhere never
FABRICATES data; it can only lose a recent un-fsynced write:

- **Grow / write path**: extent KV puts are all-replica ACKed
  (`write_region(..).await?`) BEFORE the inode-meta size advances and is
  persisted. Crash between ⇒ durable size is SHORT of the written extents:
  the file just looks older (POSIX-acceptable for un-fsynced data). The
  beyond-size extent KVs are benign orphans: invisible to reads (bounded
  by size), overwritten by a regrowing append (same `[0x03][ino][off]`
  keys), and reaped by unlink / truncate (both PREFIX-scan, not
  size-bounded).
- **In-place overwrite (RMW) read barrier (RMW-GET-SWALLOW, 2026-06-23)**:
  a partial write whose offset lands INSIDE an existing extent must
  read-modify-write that extent's value (`extent::write_region` RMW
  branch). The existing-value read MUST use the `kv_get_opt` barrier and
  PROPAGATE a hard error — exactly like `clean_beyond_eof`. Pre-fix it was
  `kv_get(&ck).await.unwrap_or_default()`, which collapsed a transient
  RPC/routing/storage error into an EMPTY value; the code then zero-filled
  the untouched prefix `[start, offset)`, TRUNCATED the untouched suffix,
  and `put` the result — fabricating zeros / dropping bytes on a
  *successful* write (the cp-only append workload never hits RMW, so
  `fuse_chaos` T1–T3 missed it). The corruption is deterministic when the
  RMW's `get` hard-errors but the following `put` lands: `get`/`put` have
  SEPARATE 10-refresh (~13 s) retry budgets, so a PS that is briefly
  unavailable (kill, migration > budget, a single RPC timeout under load)
  exhausts the `get` while the `put` later succeeds. Fix propagates → fuse
  EIO (the app retries); only a genuinely-absent key (`Ok(None)`, which a
  mapped extent should never be) is treated as the safe sparse-empty value.
  Guarded by the dedicated `scripts/fuse_rmw_chaos.sh` (partial overwrite + PS
  kill + restart-at-16s so the get's retry budget exhausts while the put lands;
  asserts the untouched prefix is NEVER zeroed). It is SINGLE-PS on purpose —
  a kill+respawn in the 2-PS `fuse_chaos.sh` migrates the partition and the
  post-restart verify read can wedge on part_addr reconvergence, which is
  unrelated to this bug; single-PS has no migration so the read returns.
- **Shrink / truncate path (the fixed bug)**: the inode-meta put is the
  COMMIT POINT — it lands BEFORE extents are deleted/shortened. Pre-fix
  the order was inverted: a crash between extent destruction and the meta
  put left durable size = old_size with the tail data already gone, so
  reads returned ZEROS INSIDE the file — the one crash window in this
  layer that fabricated data. Invariant: **content[0..size] always equals
  what was last successfully written there; a crash may only choose WHICH
  size (old or new) survives.** Covered by `scripts/fuse_chaos.sh` T1
  (truncate burst + kill -9 mid-burst + remount + prefix-exact verify).
- **Leftover reaping on GROW (coco P1)**: with meta-first shrink, a crash
  in the post-commit cleanup window leaves extents beyond the durable
  size. They are invisible while size stays put, but a later GROW would
  re-expose them as resurrected old data where POSIX requires zeros.
  `extent::clean_beyond_eof(ino, eof)` (raw prefix scan; deletes whole
  keys ≥ eof, shortens the straddler by its ACTUAL KV value length)
  runs on every grow path: `write::write` ENTRY when `offset > size`
  (coco P0: the sweep must fire BEFORE the in-memory size bump — the
  bump erases the pre-grow EOF, so by flush time `write_region` sees
  the grown size and a stale straddler tail looks like legitimate
  in-file data whose RMW would merge the write into pre-shrink bytes),
  `write::truncate` grow branch (before the meta put), and
  `write_region` entry as defense (leftover keys visible in the prefix
  scan / sparse grow vs the passed file_size). Contiguous appends
  self-bound (the new key at old-EOF caps the straddler's inferred
  length) — the hot path pays one size compare. The sweep is a
  pre-grow BARRIER: a hard kv error on the straddler read PROPAGATES
  (aborts the grow) — only a genuinely absent key may be skipped
  (`kv_get_opt`, coco P1).
- **Post-commit cleanup errors do NOT fail the truncate** (coco P1): the
  shrink is committed once the meta lands; surfacing a cleanup error
  would make the caller retry into the `new_size == old_size` early
  return (a no-op that never re-cleans). WARN + invalidate instead;
  leftovers are reaped by the next grow/unlink.
- **UNLINK-1 (closed the former "deferred gap")**: unlink and
  rename-over-existing remove the target's data through
  `extent::remove_unreachable_inode` — an intent TOMBSTONE
  (`[0x04]rmtomb/[ino]`) written the moment the inode becomes
  UNREACHABLE, then extents + inode key + tombstone deleted;
  `sweep_unlink_tombstones` replays survivors at every mount (Init).
  INVARIANT: a tombstone is only ever written for an unreachable inode
  (the sweep deletes unconditionally) — in rename-over this forces the
  removal AFTER the dirent overwrite. The residual leak window is the
  single unreachability→tombstone RPC gap (pre-fix: the whole scan + N
  deletes). Bonus unconditional bug fixed en route: rename-over deleted
  the target's INODE but never its EXTENTS — the POSIX atomic-save
  pattern (write tmp; mv tmp file) leaked the entire previous content
  on EVERY save. Covered by fuse_chaos T3 (unlink burst + kill +
  remount sweep; rename-over content check).
- **Read-path bug found by the T2 harness check (fixed in
  partition-server)**: a sub-range GET fully past a VP value's end
  clamped to a zero-length read, and `read_value_from_log`'s pooled
  fast path returned the recycled RegPool buffer's STALE contents as
  the value — fuse reads of a shortened/sparse extent window got
  varying garbage instead of zeros. Now short-circuits to empty
  (`crates/partition-server/src/background.rs::read_value_from_log`).
  Caller-side counterpart (coco P1): the ioring daemon's `read_into`
  zeroes the unwritten tail of every short/empty extent slice — its
  dest is a REUSED ring buffer, not a fresh zeroed Vec (the fuse path
  pre-zeros its whole buffer; ioring zeroed only the gaps BETWEEN
  extents). `crates/ioring/src/fuse_read.rs`.

## statfs — real backend capacity (CLUSTER-DF, 2026-06-16)

`df -h <mountpoint>` was a hardcoded 1 TiB / 512 GiB placeholder. The `Statfs`
arm in `dispatch.rs` now calls `state.client.cluster_df()` (the `MSG_CLUSTER_DF`
aggregate snapshot — RAW + autumn physical_used summed from every EN's df) and
maps it **conservatively at the 3-replica factor**: `blocks = raw_total/3/4096`,
`bavail = bfree = raw_free/3/4096`. Usable LOGICAL capacity is a RANGE under EC
(cold EC 1.25–1.33× vs hot 3×); statfs is a single scalar, so — CephFS-style —
we collapse the range to the WORST factor so `df` never over-reports free and
can't lull a writer into an optimistic ENOSPC (already-EC'd cold data means real
free is higher; under-reporting is the safe side). The call is BOUNDED
(`compio::time::timeout` 2 s) so a slow/down manager can't hang the syscall —
on timeout/error it falls back to the benign large default. statfs is rare
(a `df` invocation) so an inline call is fine; no background cache needed.
File `size` stays the logical size (replica/EC amplification is transparent to
the FS layer, matching Ceph/HDFS). inode counts (files/ffree) stay a constant.

## Restart behaviour under chaos — EN vs PS (2026-06-23)

Three restart classes, each with its own harness:

- **PS (partition-server) kill+restart** — `scripts/fuse_chaos.sh` F1. The
  partition MIGRATES to another PS; file I/O resumes after region
  reconvergence; synced files stay byte-exact. The RMW-GET-SWALLOW guard
  (`scripts/fuse_rmw_chaos.sh`) covers the PS-down RMW corruption window.
- **manager / fuse-daemon kill+restart** — `fuse_chaos.sh` F2/F3.
- **EN (extent-node, data-plane) kill+restart** —
  `scripts/fuse_en_restart_chaos.sh`. An EN kill does NOT migrate partitions;
  the stream layer fails reads/writes over to the surviving replicas.
  **INTEGRITY is intact** — verified byte-exact across 4 kill+restart rounds
  (all 3 ENs) + a remount, for 6 durable files (4 KiB..10 MiB incl.
  multi-extent) + 4 repeatedly-RMW'd files vs a lockstep mirror.

  **WRITE-availability caveat = CAPACITY, not a failover-latency bug
  (localized 2026-06-23, reproduce-first via `autumn-client`, no fuse bridge).**
  An EN kill stalls WRITES only when the cluster has exactly RF (=3) ENs:
  - **3 ENs, RF=3:** every extent lives on all 3 ENs, and killing 1 leaves 2
    healthy `< RF` — `select_nodes` cannot form a new 3-replica extent without
    the dead node, so the F227 all-replica-ACK append never completes and a
    new-extent alloc keeps falling back to the dead node → a single write
    WEDGES until the EN returns (measured: one `put` hit the 90 s CLI timeout;
    effectively unbounded). This is the RF=N-on-N-nodes truth (Ceph/HDFS halt
    writes too at RF=3 on 3 nodes with one down), NOT an autumn defect.
  - **5 ENs, RF=3:** killing 1 leaves 4 healthy `≥ RF` → new extents alloc on
    healthy nodes, the append rolls off the dead-replica extent transparently
    → **writes never stall** (measured: every put/get < 0.1 s with 1 EN down,
    PS `retries=0`, no manager Suspected-mark even needed).
  READS tolerate a down replica at ANY cluster size (min-quorum read), which is
  why the integrity check above always passed even at 3 ENs.

  CONSEQUENCE for these harnesses: `fuse_chaos.sh` / `fuse_en_restart_chaos.sh`
  run 3 ENs, so they verify EN-restart INTEGRITY (the EN is respawned quickly,
  reads work throughout) but DO NOT exercise sustained writes-during-EN-down
  (that would just hit the RF=3 capacity wedge). To test write availability
  under EN loss, provision >RF ENs. The single-threaded fuse dispatcher + 30 s
  bridge `REPLY_TIMEOUT` only AMPLIFY a stall into an EIO; they are not the
  cause. No stream-layer timeout change is warranted (the "fast-fail the stale
  EN conn" idea was the wrong diagnosis).
