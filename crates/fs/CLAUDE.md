# autumn-fs Architecture Guide

## Purpose

`fs/` 文件树本身：key/value 布局、命名空间操作、数据读写路径、分段文件、整文件发布与
发布会话，全部建在 partition 层的 KV 之上。**不含任何内核挂载代码**，不依赖 libfuse。

前端都是它的薄壳，一处实现、不会 drift：

| 前端 | 位置 |
|------|------|
| 内核挂载 | `autumn-fuse`（唯一依赖 fuser / libfuse 的 crate，见它的 CLAUDE.md）|
| S3 网关 | `crates/server/src/bin/autumn_s3/` |
| `autumnfs` CLI、`migratev3_v4` | `crates/server/src/bin/` |
| PyO3 `autumn.Fs` + fsspec facade | `python/src/fs.rs` |

以前这些代码是 `autumn-fuse` 的 `core` feature；拆成独立 crate 是因为 S3 网关、Python
绑定等使用方与内核挂载毫无关系，却要依赖一个叫 fuse 的 crate 并关掉它的默认 feature。

## 模块职责

| 文件 | 职责 |
|------|------|
| `key.rs` | KV key 编码/解码（含 striped key builder）|
| `schema.rs` | InodeMeta / DirentValue / StripeLayout / ReaddirEntry / WriteBuffer + 常量 |
| `geom.rs` | declared stripe 几何 read/write（`[0x04]stripe_geom`）|
| `meta.rs` | inode 元数据 get/put、`alloc_inode`（manager 取号）、`ensure_root`/`ensure_schema_version`、S_IF* mode 常量 |
| `dir.rs` | lookup/readdir/mkdir/rmdir/rename/create/unlink/resolve —— 返回 `(ino, InodeMeta)` / DT_* 条目 |
| `extent.rs` | 变长 extent 寻址/写/RMW/截断/删除/`clean_beyond_eof`/`remove_unreachable_inode` |
| `read.rs` / `write.rs` | 分块读组装 / 写缓冲 + flush |
| `segment.rs` | 分段文件：映射拼接/裁剪/读计划（纯函数）、数据对象与映射页读写、`reclaim` |
| `publish.rs` | 新 inode 整文件发布（条件 CAS）、删除、会话与死会话恢复、`fence_all` |
| `lease_tasks.rs` | per-session lease 后台任务（heartbeat + invalidation poll + revoked 驱逐）|
| `state.rs` | `FsState`（ClusterClient、inode 批次游标、lease 簿记、`direct_read`）|

不变量：本 crate **不依赖 fuser**，也不认识任何内核回复类型；core→fuser 的转换只在
`autumn-fuse` 的 `attr.rs`。PyO3 绑定用一个专属 compio worker 线程独占 `!Send` 的
`FsState`（Python 同步方法 ship job 阻塞取结果）。

manager 的 `system_publish`、`system_segmented`、`system_fuse_*` 等系统测试只依赖本
crate，不需要 `fuse-tests` feature 也不需要 libfuse；只有驱动挂载派发循环的三个测试
（`fuse_lease_1`、`fuse_lease_2`、`system_fuse_release_best_effort`）需要。


## Range-local read planning

`read::prepare` calls `extent::read_extents`: striped files calculate only the
requested units from the inode's persisted geometry; the same global extent-count
validation remains in `schema::striped_extent_count`. Legacy cached maps are binary
searched and only their intersecting entries are copied. Cold legacy reads still
scan once and retain the map when an inode cache entry exists. No read semantics or
on-disk format changes. The `read_plan` benchmark measures cached planning across
1/64/1024 GiB logical files separately from network/disk I/O.

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
开了就整个 `fs/` 受保护）。详见 docs/key_namespace_split_design.md。

### 变长 extent

文件数据是**按逻辑字节偏移寻址的变长 extent**（key = `[0x03][ino][logical_off BE]`，
value ≤ 8 MiB = `MAX_EXTENT`）：顺序写合并成接近 8 MiB 的 extent，末尾/部分 extent
较短（"像 Linux extent 一样变长"）。相比固定 256 KiB chunk，大文件从几十万个小块变成
数量级更少、每个 ≥ 64 KiB 的 extent，每个整 extent 读走 `get_many_into` 的 bulk 路径
（`MSG_GET_BULK`，RDMA 零拷贝的目标尺寸）。

- **持久真相 = extent KV key 本身**（隐式 key 设计，InodeMeta 里**不**存 extent 列表）。
- **运行时缓存** `InodeState.extents: Option<Vec<(start, len)>>`：冷启动 range-scan
  `[0x03][ino]` 前缀拿起始偏移 + 由相邻起始/文件大小推断长度；写时增量维护，
  truncate 时失效（置 `None`）。
- **不变量：extent 互不重叠**。读按 `[start, start+len)` 请求每个重叠 extent 的精确
  子区间，PS get 按真实 value 长度裁剪、dest 余下补零 → 短 extent / 稀疏空洞都正确。
- **小文件** ≤ `INLINE_THRESHOLD`=4KB：inline 在 `InodeMeta.inline_data`（无 extent，
  读写各省一次 KV 操作；增长超过阈值迁移到 extent 存储）。
- 全部寻址/读/写/截断/删除逻辑在 `crate::extent`。

### Lane striping（大文件跨分区条带化）

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
| `readdir(ino)` | paginated Range(prefix=[0x02][ino BE]) + one batched get per page |
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
| `MAX_EXTENT` | 8 MiB | extent value 上限；写缓冲按此粒度刷；≥64 KiB 整 extent 读走 bulk |
| `INLINE_THRESHOLD` | 4 KiB | 小文件 inline 阈值（匹配 VALUE_THROTTLE）|
| `WRITE_BUF_EXTENTS` | 8 | 每 inode 写缓冲容量（extent 数）|
| `WRITE_BUF_CAP` | 64 MiB | = `WRITE_BUF_EXTENTS × MAX_EXTENT`；>1 时 `write_region` 拆多 extent，`put_many` 按 wire key 分组后并发 fan out（`BATCH_PUT_DEFAULT_CONCURRENCY`）；extent key 互异故满并发 |
| `APPEND_INFLIGHT_DEPTH` | 2 | 单 inode 同时在飞的 append flush 批数；1 批 = 8 个 extent 并发。未实测，见写流水化一节 |
| `INODE_ALLOC_BATCH` | 1000 | 每批向 manager 领的 inode 数 |
| `DEFAULT_STRIPE_LANES` | 24 | fs 未声明几何时的默认 lane 数 |
| `ROOT_INO` | 1 | 根 inode（FUSE_ROOT_ID）|
| `SCHEMA_VERSION` | 4 | 见下 fail-loud |
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
    pending_flushes: VecDeque<PendingFlush>, // 在飞的 append flush（≤ APPEND_INFLIGHT_DEPTH）
    flush_error: Option<String>,           // 粘性回写失败，只由会上报的 flush_inode 消费
    dirty: bool,
    open_count: u32,
    extents: Option<Vec<(u64, u32)>>,      // 运行时 extent map（truncate 置 None）
    cached_version: u64,                   // 上次从 KV 刷 meta/extents 时的 lease 版本
}
```

`cached_version`：Open 时与 AcquireLease 返回的版本比对，不符则丢弃缓存 InodeState 从
`get_inode` 重建（+ 下次 rescan extent），维持 close-to-open 一致性（第二 mount
Open 已被首 mount 写关的 inode 不会读到陈旧 `meta`）。

## 操作路径

### Read
1. 脏写缓冲与读范围重叠 → 先 flush（read-after-write 一致性）。这次 flush 传
   **`FlushReport::BestEffort`**：它的 `?` 确实把 EIO 交给了调用者，但**读不是回写错误的
   退休处**。Linux errseq 只在 fsync/close/msync 退休一次回写错误，理由就在这里——应用回读
   自己刚写的数据、拿到 EIO、重试读成功、然后 close，如果这道屏障把粘性记录吃掉，那次 close
   的 fsync 就会发现无事待办、把 size 发布到洞上面。读照样失败，区别只在记录是否存活。
2. **偏移 ≥ 缓存 size 时，先向 KV 确认 EOF 再上报**（`read::prepare` 唯一一条能返回"比请求
   少"的路径，所以偏小的 size 不是把答案变旧而是把文件**截断**，顺序读者会把空回复当成
   文件结束、静默停下）。确认后的采纳是**单向的：只接受"其实更大"**。
   - 判据钉在**这次读要用的 size** 上（`fresh.size > file_size`），不是钉在"缓存是否被采纳"
     上：采纳只可能发生在 inode 已在 `state.inodes` 里的情况，而 S3 网关与 PyO3 读路径只在
     **写**时填这张表 ⇒ 若按采纳判，这道确认对所有只读前端就是纯浪费的一次往返，还会把它们
     原本能捡到的增长丢掉。
   - 为什么拒绝"变小"这个方向：它**未必**不是陈旧——另一个 mount 截断文件就会留下同样的形状。
     不对称在**后果**不在证据。变小也正是本 mount 正在写文件的常态（extent 早落地、size 未
     发布），两者在这里无法区分,所以只能倒向不会毁东西的那边：拒绝变小只让 `getattr` 旧一会儿
     （直到租约版本重载或 FORGET），**改变不了任何读的答案**（进到这条分支就已经
     `offset >= size`，钳位照样返回空）；而采纳变小会让下次写看到 `cur_size < offset` →
     `clean_beyond_eof` → **删掉自己已落地的每一个 extent**。
3. 小文件 `inline_data` → 直接返回。
4. 加载 extent map（运行时缓存，冷启动 range-scan），条带文件先 `checked()` 校验
   几何，为每个重叠 extent 生成 `ChunkSpec`（striped → lane key，否则 `[0x03][ino][off]`），
   sub-range = 精确重叠区间，extent 间空洞补零（稀疏语义）。
5. 一次批量读所有 extent slice，多 extent 并发（compio spawn，spawned `execute`
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

**缓冲是 MOVE 给 flush 的，不是拷给它的。** `write_region` 取 `&mut FsState`，而缓冲就住在
里面，所以数据没法借着跨这个调用——原来的答案是 `wb.buf[..wb.len].to_vec()`，每次 flush
新分配并拷贝整块 64 MiB，实测占一次 4 GiB 写的 12%。现在用 `mem::take` 搬走，用完由
`reclaim_buffer` 把 Vec **原样**还回去（不 clear——`mem::take` 留下的是 len-0 的 Vec，
clear 过的缓冲会让 fill 每轮都跑 `resize(.., 0)`，把下一行就要覆盖的字节先清零一遍，
等于每次 flush 一次 64 MiB memset）。`wb.len` 是长度的唯一真源，三个消费者都按它切片，
所以 `wb.len` 之后的陈旧字节永远读不到。实测 197 → 234 MiB/s。

**`WRITE_BUF_EXTENTS` 从 8 加到 16/32 实测更慢（248→234/229），别再走**——写路径的瓶颈不在
一次 flush 的深度。

**`max_write` 也不是旋钮，但理由和上面不同**：内核把 `max_pages` 夹在 `FUSE_MAX_MAX_PAGES`
(256)，所以 1 MiB 已经是每请求上限，设 4/8 MiB 请求数恒为 4096（4 GiB）。
⚠️ 我一度记过"1→8 MiB 更慢（248→219）"——**那是错的**：当时 `fuser` 还是 `abi-7-12`，
`FUSE_MAX_PAGES` 被编译掉，两侧都在发 128 KiB，测到的是纯噪声。

**分段计时留在代码里**（`fuse write breakdown`，每 16 次 flush 一行）：上面这两个假设和
"to_vec 无所谓"都是靠它否掉的。

### 写流水化（`pipelined_writes`，只有 mount 开）

**纯追加**的 flush 拆成两半：`extent::plan_append_only` 在 `&mut FsState` 下规划（要读
extent map），`extent::execute_append` 只需要 `Rc<ClusterClient>`，spawn 出去。于是上一批
64 MiB 在网络上飞的时候，dispatcher 回去继续从内核收下一批。读路径一直是这个形状
（`read::prepare` + spawn `read::execute`），这也正是读能扇出而写不能的原因。
实测 **249 → 338 MiB/s（+36%）**，达到 CLI 345 的 98%。

**多槽（`APPEND_INFLIGHT_DEPTH`=2）**：规划下一批前不再无条件 drain 上一批，只有队列
满了才等最老的一个（`write::make_room`）。`InodeState.pending_flushes` 因此是队列不是
单槽。⚠️ **未实测**，而且要清楚剩余空间有多小：单槽本身就已经让**一批 8 个 extent 并发**
在飞，这恰好等于 `autumnfs` 非 striped 的 `depth`=8 —— 338 ≈ 345 正是这个对等造成的。
多槽把在飞 extent 从 8 抬到 16，找的是剩下那 ~2%，**不是**去补什么 137 MiB/s 的差距
（`e58c735` 里的 208 早于本节的流水化四小时，是陈旧数字，别再引用）。这点空间能不能
拿到还是未知：`934d4ee` 实测单机剩余的墙是 RF3 全副本 fsync，不在客户端。

多槽安全性依赖两条，都验过：① `extent::upsert` 是按 start 的有序插入替换，而连续 append
批次区间互不相交，所以**落地顺序无关**（`disjoint_batches_apply_the_same_in_any_order`
钉住，消融会红）；② 出错不提前返回——postcondition 是"队列空"，提前返回会把槽留下而调用方以为已静默。
**别"简化"成出错时清空队列**：compio 0.19 的 `JoinHandle::drop` 仍然取消任务，
drop 一个 `PendingFlush` 会**取消**那次 flush（compio 那句"drop 不取消"
是 `spawn_blocking` 的契约，不是 `spawn` 的——本轮我一开始就引错了这句）。

不需要第二块缓冲：`plan` 里的 values 本来就是 `Bytes::copy_from_slice` 拷出来的自有数据，
所以规划一结束缓冲就自由了。

**只有纯追加走这条路。** RMW / 空洞 / EOF 之后的残留一律回落到 inline `write_region`——
它们要读 extent map，而在途 flush 的 extent 还没进图。`plan_append_only` 的判据
（`offset > file_size`、`s >= file_size`、`s + l > offset` 任一成立就返回 None）就是这个边界。

**drain 放在 dispatcher 一个点上**（非 Write 请求一律先 drain），而不是逐调用点审计——
那种清单只在下一个 handler 加进来之前是完整的。**这也是 `pipelined_writes` 默认关、只由
mount 打开的原因**：PyO3 的 `autumn.Fs` worker 直接调核心 op，根本不过这个 dispatcher，
开了流水化它的 `read` 会读到在途 flush 那段的零。

**失败的 flush 要粘在 inode 上**（`InodeState.flush_error`），不能只靠返回值：dispatcher 的
drain 跑在 fsync handler **之前**且只打日志，会把唯一一份错误消费掉；`flush_inode` 随后看到
没有 pending，就会持久化一个覆盖了丢失区域的 size —— **fsync 回报成功，文件里留着零洞**。
现在 `flush_inode` 先取这个粘性错误并返回，才轮到持久化 size。

**但"取"的资格按调用者分**（`flush_inode(state, ino, report)`）：传 `FlushReport::ToApplication`
的才 take，其余传 `BestEffort` 只 peek。**判据不是"这个调用者会不会把错误交给谁"，而是"这里是不是
Linux 意义上回写错误的退休点"** —— 只有三处合格：FUSE_FLUSH（内核在每次 `close()` 都发）、FSYNC、
PyO3 绑定的显式 flush。其余六处全传 `BestEffort`，分两类：
- **谁都没告诉**：periodic_sync、Destroy 只打日志；RELEASE 回的错误被内核丢掉。
- **告诉了，但不是退休点**：read-after-write 屏障、写路径自己的 gap flush、truncate。它们的 `?`
  确实把 EIO 交给调用者，但 Linux errseq 只在 fsync/close/msync 退休 —— 应用读/写拿到 EIO、重试
  成功、再 close，那次 close 必须仍然失败；在这里消费，close 就找不到记录，对着洞报成功。

RELEASE 在这份名单里容易看反：它确实会
回一个错误，但 fuser 的契约明说"错误值不会返回给触发 release 的 close()/munmap()"——内核把
它丢掉，所以它和只打日志的那两个等价。`dispatch.rs` 的 `propagate_flush_err`(= `!revoked`)
只决定**要不要回 EIO**，**不能**拿来决定能不能消费粘性记录（本轮一开始就是这么错的）。
也别退回到"FLUSH 反正总在 RELEASE 之前"那条假设上——fuser 同一份文档明说
"filesystems shouldn't assume that flush will always be called ... or that it will be
called at all"，那正是这份记录存在要堵的洞。
⚠️ 这条的可达形状是 **extent key 和 inode key 落在不同分区**（`[0x03][ino][off]` 与
`[0x01][ino]` 排序相隔很远，split 后必然分开）而 extent 那边不可用；租约撤销**不是**——
`put_inode` 同样带围栏，两边一起失败。`scripts/fuse_chaos.sh` 的 fsync 守卫杀整个集群，
**隔离不了这一条**（meta put 也会失败），要真正隔离需要冻结单个分区。

### Flush
`extent::write_region`：对齐区间直接 Put；非对齐落在已有 extent 内 → RMW（读旧值、
覆盖子区间、回写）。之后更新 `InodeMeta.size`。

### 目录操作
- **lookup**：Get dirent → Get inode。
- **readdir**：dirent 前缀 Range scan，按 key 分页直到读完；单页 4096 项不是
  目录大小上限。每页从上一页末 key 的后继（`key + 0x00`）继续，offset 是名字序
  位置。`readdir_bounded` 限制单次返回条数：内核每次只取一个 reply buffer（约百项）
  再从末 offset 续读，若每次都读完剩余目录，大目录就是平方级的 get。offset 之前的
  名字只扫 key、不取 value；返回的条目用 `list_children` 一次 `get_many` 取 dirent。
- **mkdir**：alloc inode + Put meta + Put dirent + parent nlink。
- **rename**：Delete old dirent + Put new dirent（非原子，v1 限制；rename-over 见
  UNLINK-1）。

## Per-session lease + 跨前端围栏

per-session lease 后台任务（5s heartbeat 续所有 held lease + 持久 invalidation
long-poll + `LeaseRevoked` 驱逐）在 `lease_tasks.rs`（core）；mount 传真 kernel
invalidator，binding 传 None（headless，无内核页缓存驱逐）。

- **写写围栏**：写路径 `acquire(WRITE)` 环绕；冲突时 fsspec facade 抛
  `BlockingIOError`，被抢占租约标 revoked、`write` 对 revoked 租约快失败（无租约的
  匿名写仍放行）。写租约在长写期间被续。
- **读一致性**：靠 fresh-read + Q1 只写租约（binding 只在写时缓存，release 时
  `forget` 驱逐 inode 缓存）。

## 分段文件（segmented files，v4）

S3 multipart 的 Complete 必须**不读、不拷、不重写任何分片正文**，所以文件内容可以是
一张"逻辑区间 → 数据对象"的映射（`InodeMeta.segments`，`segment.rs`）：

- **数据对象**：一次写入、不可变、不挂目录。按 `unit`（= `MAX_EXTENT`）**稠密**写在
  `[0x03][lane][data_ino][off]`，`lane = (off/unit + data_ino) % lanes`——按对象错开起始
  lane，否则每个 5 MiB 的分片都只有一个 extent，全压在 lane 0。稠密意味着映射覆盖的区间
  每个 key 都必须存在：读到缺失或短值是**丢数据**，`ChunkSpec.strict` 让读报错而不是按稀疏
  语义补零（消融：关掉 strict，删一个 extent 后读回全零）。
- **映射**：≤ `INLINE_SEGMENTS`(64) 段内联在 inode 里（单 PUT 对象、一般 Lance 数据文件
  读时零额外 RPC）；更大的分页存 `[0x05][map_id][page]`，页不可变，按 `(map_id, page)`
  缓存在 `FsState.segment_pages`，读只取所涉页。
- **写**：`extent::write_region` 对分段 inode 转到 `segment::write_file_range`——缓冲区
  `[off, off+n)` 写成一个新数据对象，映射里该区间被替换（`splice`，被切开的旧段只调整
  `off/len/data_off`），新页写在新 `map_id` 下，flush 末尾的 inode put 是唯一提交点。
  **不读旧数据、无 RMW、不整体物化**。流水化 append 不走分段文件。truncate 只 `clip`
  映射，之后扩展的区域是洞、读零，不会重新暴露被截掉的字节。
- **generation**：每次内容变化加一（write 调用、truncate、映射替换），与 inode 号一起
  构成 S3 ETag，同大小同秒重写也会变。
- **回收**：每个数据对象 / 分页映射在写入**之前**先记 `[0x04]segc/[file][id]`（带删除它
  所需的长度与几何），所以崩溃留下的东西都找得到。`segment::reclaim(current)` 以文件
  **当前**映射为唯一真相：记录里当前映射不再引用的对象、不是当前 `map_id` 的页一律删掉，
  仍被引用的记录保留给以后的修改。这对任意次修改、任意崩溃点都成立，不需要逐次对账。
  调用方必须独占该文件（EXCLUSIVE 租约），否则持有旧映射的读者会被抽掉数据。
- **活文件的回收**：每次修改在写任何对象/分页**之前**先写 `[0x04]segg/[ino]` 标记（每会话
  每文件一次 put，记进 `FsState.segment_garbage`）。不只在"确知丢了引用"时写：写了但没发布
  的对象（崩溃、inode put 失败）也是垃圾，而别的路径永远不会去找它——`sweep_garbage` 只看
  标记。本会话最后一次 close 时（FUSE RELEASE、Python `release`）和扫描（`periodic_sync` 每
  30 s，每次一页 1024 个标记，游标 `garbage_sweep_from` 轮转，被别处持有的标记挡不住后面的）
  用 `segment::reclaim_live`：取 EXCLUSIVE，**从 KV 重读**当前映射（绝不用缓存——别的会话
  可能已发布更新的映射，旧映射会把新对象判成垃圾），回收，删标记。有别的持有者就留着标记。
  `reclaim_live` 无论结果都把 ino 从本会话集合里去掉：之后可能是别的会话回收并删了标记，
  留着的集合项会让本会话下一次修改跳过打标记。
- **修改分段文件必须持有 WRITE 租约**（`held_write_lease`，否则 EBUSY）：租约挡住回收，
  新对象已写而引用它的映射尚未发布的窗口里，回收会把它当垃圾删掉。FUSE 打开即持有；Python
  的裸 `write` 对分段文件因此要先 `acquire`（普通文件不变），且 `write()` 本身会成功、到
  flush 才 EBUSY，缓冲的字节随该错误丢弃。
- **路径 truncate 自己取租约**：没协商 `ATOMIC_O_TRUNC`，内核把 `open(O_TRUNC)` 变成 OPEN
  之前的 SETATTR(size)，`truncate(2)` 则根本不 open。本会话对该文件什么都没持有时取一次性
  WRITE（`segment::hold_transient_write`，被别人持有 = EBUSY），改完即还并就地回收。FUSE
  SETATTR 把**整个请求**包在这个租约里（末尾那次 inode put 也要带围栏）；核心
  `write::truncate` 自己也会取，供 Python / autumnfs。否则 `cp`/`echo >`/编辑器覆盖 S3 写出
  的文件全都 EBUSY（`a_path_truncate_takes_its_own_lease`，消融即红）。
  **取到租约后丢掉缓存的 inode、从 KV 重读**：没打开的文件缓存可能早于别的会话的改写
  （只有 Open 会按租约版本判陈旧），从陈旧映射裁剪再发布，随后的回收会把别人的对象当垃圾
  删掉——从"EBUSY"变成了"删数据"（`a_path_truncate_reads_the_map_under_its_lease`，消融：
  不重读则读到已删对象）。本挂载以只读打开着该文件时（`tail -f`）仍是 EBUSY。
- **flush 失败的最后一次 close 丢掉分段 inode 的缓存**（无论租约是否已被 revoke）：租约此刻
  已还，缓存里没发布的映射所指的对象马上会被回收；留着它，重开读到的是已删对象，periodic
  sync 还会以 ANON 把它 put 回去。下次 open 从 KV 读映射。
- **代价（有意的取舍，未实测）**：每个就地修改分段文件的会话多一次标记 put；最后一次 close
  的回收是 EXCLUSIVE acquire + 读映射 + 扫该文件全部 `segc/` 记录（仍被引用的记录永不删，
  扫描 O(对象数)）。S3 写出的 Lance 文件是写一次、走 `publish` 不走这里，所以可接受；若出现
  "对多分片大对象反复小改"的负载，改成：标记用 `compare_put(None)` 知道是不是自己建的、
  本地记"是否丢过引用 / 有失败的 put"，干净时 close 只删标记。
- 读到的段先 `Segment::checked()`：损坏的 0 lanes/unit 或溢出报错，不在除法里 panic 掉挂载。

### 整文件发布（`publish.rs`，S3 PUT / Copy / Complete / Delete 的核心）

新文件写进**新 inode**（`NewFile`），对谁都不可见，然后一次带围栏的 `compare_write` 把
dirent 换过去——`If-None-Match: *`（期望不存在）和 `If-Match`（期望指向那个 inode）由 PS
判定，不是先读后写。读者只会看到旧文件或新文件的全部。ETag = hex(ino) ++ hex(generation)。

- **会话**：一切写入带会话 inode 上 WRITE 租约的 epoch；每个操作写任何东西之前先记
  `[0x04]pend/[S][obj]`。会话死后 `recover_dead_sessions` 夺取其租约（epoch 升高）、
  `fence_all` 把每个 fs 分区对该会话的围栏抬到新 epoch（先 `refresh_regions`，并重复到
  一轮找不到新分区为止：长寿客户端的缓存可能早于一次 split，split 之后才抬的地板子分区
  没有），再按 dirent 指向谁完成或撤销每个记录。**调用方是 S3 网关的周期任务**（尚未接入；
  在那之前只有测试调用它，死会话的记录与会话 key 会一直留着）。活会话自己留下的记录（结果
  未知的交换）只在它死后才被处理。
- **CAS 是提交点**，之后的一切都不能让发布失败：替换/删除之前先记
  `PendingOp::Retire{parent,name,ino,successor}`（`successor` = 要换上的新 inode，删除为
  `None`），CAS 落地后退掉旧 inode（`drop_name_of`），成功才删记录；失败留给恢复（以前退旧
  inode 出错会让 `NewFile::publish` 撤销一个**已经发布**的文件，dirent 悬空——
  `system_publish` 里损坏旧 inode 的那段，消融即红）。
- **恢复只在 dirent 恰好是 `successor` 时退旧 inode**：别的会话把名字换走时已经自己退过
  一次，再退一次对 nlink=2 的 inode（Lance manifest 经 FUSE 硬链接）就是删掉另一个名字还指着
  的文件（`a_retire_whose_swap_lost_leaves_a_linked_inode_alone`，消融：按"dirent 不再指向
  它"判定即红）。记录用 `compare_write(None)` 只建不覆盖：结果未知的上一次交换留下的记录
  原样保留，`end_swap` 只删本次建的——否则后一次失败的重试会删掉前一次仍可能落地的记录。
- **结果未知 ≠ 没落地**：CAS 的 RPC 出错时回读 dirent，是新值就算成功；否则返回
  `PublishError::Other`，调用方**不撤销**（在途请求之后仍可能落地），记录留给恢复。只有
  PreconditionFailed / NoSuchKey / Busy / NotAFile 这些确定结果才撤销。
- **REPLACE 持有期间才核对 `If-Match` 的 generation**：先读后取租约，中间别的写者可以改完
  并释放。旧 inode 此时已不存在（别的发布者抢先换掉）= PreconditionFailed。本 client 自己对旧 inode 持有写租约也算冲突（manager 允许同 client 在自己的
  WRITE 上取 REPLACE）。
- 已知限制：FUSE 的 unlink/rename 是无条件 KV 操作，与网关同名并发时后者赢；恢复按"dirent
  是否指向它"判定，发布者在 CAS 与删记录之间崩溃、且恢复前有人把该名 rename 走，会被误判为
  未发布而撤销；`Retire` 在"已减 nlink、未删记录"时崩溃，恢复会再减一次，nlink=2 时即删掉
  另一个链接还指着的 inode。`flush_inode` 在 `put_inode` 之前就清掉 `dirty`，FUSE_FLUSH 的
  put 失败后 RELEASE 看不到失败，分段 inode 的缓存不会被丢——下次 Open 按版本重建能兜住，
  PyO3 会读到 strict 错误（不是零）。

### 不可达 inode 的回收要等持有者

`remove_unreachable_inode`（unlink / rename 覆盖）先写 `rmtomb`，然后：本会话仍打开
着 → 记进 `FsState.unlinked_open`，最后一次 close（FUSE RELEASE）时再回收；否则
`reclaim_unreachable` 取 **EXCLUSIVE** 租约——别的客户端还持有任何租约（另一个挂载打开
着、S3 GET 在读）就 Conflict，墓碑留给扫描（挂载时 + `periodic_sync` 每 30 s 一次）。
拿到才删数据，删除以该租约 epoch 围栏；各删除阶段之间以同一客户端重新 acquire
EXCLUSIVE，再确认一次独占（manager failover 会把它重放成普通 WRITE，放进读者）。已知不是
分段文件的 inode 跳过 segc 记录扫描。以前 unlink 立即删数据，别的挂载上打开着的文件会读到
洞。REPLACE/EXCLUSIVE 不持久化，所以每次 unlink 多两次 manager 内存 RTT、没有 etcd 写。
测试 `system_segmented.rs::reclaim_waits_for_other_holders`（消融：跳过租约，持有期间数据
被删）、`unlinked_while_open_here_is_reclaimed_at_last_close`。

legacy extent 前缀扫描（`scan_extents`、`clean_beyond_eof`、`delete_all_extents_with`）
从**最后返回的 key** 的后继续扫，而不是从最后解析出的 offset：18 字节的条带 / 数据对象 key
（lane 0、inode 号 = `ino << 8 | x`）会落在 17 字节的 legacy 前缀里，一整页都是这种 key 时
旧游标永远不前进。

## Schema 版本戳（fail-loud）

`schema::SCHEMA_VERSION` = **4**，存于 `[0x04]schema_version`（相对 key，即
`fs/[0x04]schema_version`）。`meta::ensure_schema_version` 在 mount（`ensure_root`
入口）缺则戳、有则核对、**不符则 fail-loud 拒挂**（防未来不兼容布局静默读写坏数据）。
- v1 = pre-namespace 裸 key（从不戳）。
- v2 = namespaced 相对布局 + 全局 inode 计数器。
- v3 = lane striping：`InodeMeta` 加 `stripe` 字段（rkyv 布局变，v2 inode 字节解不出），
  大文件走 lane-striped key。v2→v3 stop-world reset，无 in-place 迁移；小/legacy 文件
  仍 `stripe=None` + `[0x03][ino][off]`。BUMP whenever 布局/编码不兼容变更。
- v4 = 分段文件 + 内容代数：`InodeMeta` 加 `generation`、`segments`。用一次性离线工具
  `migratev3_v4` 原地转换（每个 inode 保留编号/链接/字节，得到 `generation=1`、
  `segments=None`，最后才改戳），不 reset。
- **缺戳 ≠ 新树**：只有一个 inode 都没有才算新树并盖当前版本；有 inode 却无戳，是 v4 以前
  不盖戳的工具（`autumnfs`、S3 网关）建的，inode 是 v3——拒绝挂载并指向
  `migratev3_v4 --unstamped-is-v3`。以前会直接盖 v4，在本地实测转换时把一棵 v3 树变得
  整棵不可读（`an_unstamped_populated_tree_is_refused_not_stamped`）。

## 关键依赖文件

| 文件 | 用途 |
|------|------|
| `crates/client/src/lib.rs` | ClusterClient — 所有 KV 操作入口 |
| `crates/rpc/src/partition_rpc.rs` | PutReq/GetReq/RangeReq/DeleteReq |

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

## Manifest hard links

Lance's local ConditionalRenameCommitHandler uses linkat(source, destination),
then unlinks its temporary source. The default fuser callback returned EPERM,
so table creation failed although ordinary rename worked (strace confirmed).
Link now dispatches to dir::link: regular files only, persist the increased nlink,
then create the destination dirent using compare_put(None). Existing destinations
return EEXIST; a lost-ACK retry recognizing the same inode succeeds. Source unlink
preserves the linked inode. Data is not copied. A pre-existing-target check avoids
unnecessary inode metadata writes in the usual conflict case.

This retains the filesystem's existing nontransactional namespace limitation:
inode reference counts and directory entries are separate KV writes, and mutation
serialization is per mount. A crash after the count update can leak a reference.
It is suitable for the one-mount Lance demo; it does not establish crash-atomic
or multi-mount namespace transactions.
