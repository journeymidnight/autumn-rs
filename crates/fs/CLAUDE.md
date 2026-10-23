# autumn-fs Architecture Guide

## Purpose

`fs/` 文件树本身：key/value 布局、命名空间操作、数据读写路径、分段文件、整文件发布与
发布会话，全部建在 partition 层的 KV 之上。**不含任何内核挂载代码**，不依赖 libfuse。

前端都是它的薄壳，一处实现、不会 drift：

| 前端 | 位置 |
|------|------|
| 内核挂载 | `autumn-fuse`（唯一依赖 fuser / libfuse 的 crate，见它的 CLAUDE.md）|
| S3 网关 | `crates/server/src/bin/autumn_s3/` |
| `autumnfs` CLI | `crates/server/src/bin/` (mkdir / rm through `dir`, touch of an existing file through `meta::put_inode`; the parent must be a directory, judged from its dirent (`dir::resolve_typed`) for mkdir / touch / rm and from its inode for put; a new file (put / touch) is published create-only via `dir::compare_put_settled`, its keys deleted when the publish is refused and kept when the outcome stays unknown — the same cross-session windows as rename apply) |
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
| `multipart.rs` | S3 multipart：上传状态机、分片写入、元数据级 Complete、Abort、清理与死会话恢复 |
| `lease_tasks.rs` | per-session lease 后台任务（heartbeat + invalidation poll + revoked 驱逐）|
| `state.rs` | `FsState`（ClusterClient、inode 批次游标、lease 簿记、`direct_read`、`reclaim_later` 回收交接）|

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
（leader-fenced etcd CAS），每批预分配 `inode_alloc_batch`=1000 个，全局计数器保证
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
| `0x04` | FS 超级块 | `[0x04][field]` | varies（`next_inode` / `schema_version` / `stripe_geom` / `rmtomb/[ino]` / `sess/` / `pend/` / `rbase/[S][ino]`）|

**Namespace 绑定**：wire key = `fs/[type][fields]`（一棵全局树）。`autumn-fuse` /
`autumnfs` / PyO3 `autumn.Fs` 都没有 scope 参数；`FsState` 用 `connect(mgr, "fs")` / `scoped("fs")`。多棵互隔离的树用不同
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
| `mkdir`/`create(parent, name)` | 1× Put inode + 1× CAS dirent (create-only) + 1× Get parent + 1× CAS parent (create: at most once a second per directory); the parent's Get moves before the publish the first time this session meets that directory (`check_parent`) |
| `link`, `unlink` (file count) | 1× Get + 1× CAS inode (`change_links`); whole-inode writes (flush, truncate, setattr) also Get + CAS (one more round trip per dirty flush than the old put; an unknown outcome is settled by re-reading) |
| `unlink(parent, name)` | 1× Get dirent + 1× CAS delete dirent + tombstone + N× Delete extent + 1× Delete inode + 1× Get + 1× CAS parent |
| `rename(old, new)` | 3× Get dirent (+1 Range per check if the new name is a dir) + 1× CAS delete old + 1× CAS new + Get + CAS of each parent inode |
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
| `DEFAULT_STRIPE_LANES` | 24 | fs 未声明几何时的默认 lane 数 |
| `ROOT_INO` | 1 | 根 inode（FUSE_ROOT_ID）|
| `SCHEMA_VERSION` | 4 | 见下 fail-loud |
| `DT_REG`/`DT_DIR`/`DT_LNK` | 8/4/10 | 目录项类型 |

参数（`crates/params/src/fs.rs` 的 `FsParams`，进程级 `autumn_fs::params()`，由嵌入 fs 层的
binary 解析为 flag）：

| flag | 默认 | 说明 |
|------|-----|------|
| `--write-buf-extents` | 8 | 每 inode 写缓冲容量（extent 数）；`write_buf_cap()` = 它 × `MAX_EXTENT` = 64 MiB。>1 时 `write_region` 拆多 extent，`put_many` 按 wire key 分组后并发 fan out（客户端 `--batch-put-concurrency`）|
| `--append-inflight-depth` | 2 | 单 inode 同时在飞的 append flush 批数；1 批 = 8 个 extent 并发。实测比 1 快 ~6%，见写流水化一节 |
| `--segment-stream-inflight` | 8 | 流式数据对象（S3 PUT / UploadPart）同时在飞的 unit put 数，见"数据对象是连续流水线" |
| `--inode-alloc-batch` | 1000 | 每批向 manager 领的 inode 数 |
| `--segment-page-cache-cap` | 256 | 段表页缓存上限 |
| `--lease-heartbeat-secs` | 5 | 写租约心跳周期（manager 租约 TTL 默认 30 s）|
| `--multipart-completed-retention-secs` | 3600 | 已完成 multipart 的应答保留时长 |

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
    write_buf: Option<WriteBuffer>,        // buf 容量 write_buf_cap()
    pending_flushes: VecDeque<PendingFlush>, // 在飞的 append flush（≤ append_inflight_depth）
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
1. 懒分配 `WriteBuffer`（容量 `write_buf_cap()`=64 MiB）。
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

**`write_buf_extents` 从 8 加到 16/32 实测更慢（248→234/229），别再走**——写路径的瓶颈不在
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

**多槽（`append_inflight_depth`=2）**：规划下一批前不再无条件 drain 上一批，只有队列
满了才等最老的一个（`write::make_room`）。`InodeState.pending_flushes` 因此是队列不是
单槽。剩余空间本来就小：单槽已经让**一批 8 个 extent 并发**在飞，恰好等于 `autumnfs`
非 striped 的 `depth`=8；多槽去掉的是批与批之间的空泡，**不是**去补什么 137 MiB/s 的差距
（`e58c735` 里的 208 早于本节的流水化四小时，是陈旧数字，别再引用）。
**实测（2026-09-27）**：绑核（NUMA node1：每个 EN 一核 48/49/50、PS 52-59、fuse 60-67、
写者 68）的本机 3-EN RF3 集群，4 GiB `dd bs=8M conv=fsync` 写进挂载，depth 2 与 1 交替 6 对、
两种先后顺序各 3 对：**6 对全是 depth 2 快**，六对均值 284.7 vs 266.3 MiB/s（+6.9%），与同轮
`autumnfs put` 同一文件持平（283.5）。这台绑核装置比上面 338/345 那组慢，单槽在它上面只到
CLI 的 ~94%，缺的 ~6% 就是批间空泡。收益小但可辨，所以保持 2。

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
- **mkdir / create**：alloc inode + Put meta + 只在名字不存在时发布 dirent
  （`compare_put(None)`，不先查：内核建之前已 lookup 过）+ 父目录 inode。两个会话
  同名创建只有一个赢，另一个 EEXIST 并删掉自己写的 inode。
- **父目录必须是目录**（`check_parent`，mkdir / create / link / rename 的新父目录）：
  文件当父目录返回 ENOTDIR，什么都不写。挂载上内核已保证；直接传 inode 号的调用方
  （PyO3 `autumn.Fs`）以前能在文件下建目录项，mkdir 还把文件 nlink 抬到 2、永不回收。
  不另花一次读：本会话第一次碰到某目录时读它的 inode 并检查，这份字节就是随后父目录
  CAS 的基线；之后记在 `FsState::known_dirs`（类型创建后不变、inode 号不复用，所以不会
  过期；超过 65536 项清空），直接走原来的路径。只预读不缓存时，基线早于发布、同目录
  并发下 CAS 更易失败：4 会话同目录 mkdir −5%；加缓存后 create 11.1K/s、mkdir 4.2K/s
  与改前持平（release，ABAB 各 6 次）。测试 `system_fs_namespace::a_file_is_refused_as_a_parent`
  （去掉检查则 mkdir 成功、变红）。
- **路径上的类型取自目录项**：`DirentValue.file_type` 在每个写目录项的地方都设，
  rename 原样拷贝，类型又不可变，所以和 inode 一致。`dir::resolve_typed` 返回最后一级
  的类型；S3 的 `bucket_dir`、`publish::ensure_dirs` 和 autumnfs CLI 用它判断目录，
  不读 inode：ensure_dirs 4 级已有路径 13.1K → 27.2K/s。
- **父目录 inode** 的 nlink、mtime 对读到的字节 CAS（`cas_dir`）：改 nlink 的（mkdir、
  rmdir、rename）冲突就重读重试，最多 16 次不睡（`update_dir`）；只改 mtime 的（create、
  unlink、link）只试一次，输了就算——赢的那次改动晚于它开始；目录 mtime 已在当前这一
  整秒内就不写（用户 2026-10-21 定：目录时间精度 1 秒，同 1 秒时间戳的文件系统：落在
  新一秒的改动一定推进 mtime/ctime；存的 mtime 在别的秒——包括未来——就写，所以时钟
  不齐的客户端之间会退化成每次都写，只慢不错）。每次都写父目录时，那一个 key 上的
  条件写在 PS 上串行（必须先等前一次提交才能比较），4 会话同目录 create 6.2K/s 对改前
  6.9K（同轮交替）；按秒跳过后 9.3K/8.4K 对改前 6.5K/6.3K。两者都在名字改完（提交点）
  之后，失败只记 WARN、不报错：报错会让已经发生的操作看起来失败（FUSE 一次只处理一个
  请求，退避还会卡住整个挂载），代价是 nlink 计数偏差（stat 看得到），不会让东西不可达。
  以前是读-改-写，两个会话同一目录下 mkdir、或 mkdir 旁边 unlink，会丢 nlink。
- **inode 的链接计数只由 `meta::change_links` 改**（对存储字节 CAS，同时推进 ctime）：
  link +1、unlink / rename 覆盖文件 / link 回滚 −1，把计数减到 0 的那一次负责回收；计数
  已为 0 或 inode 已不存在时拒绝加链（ENOENT）。retire 的最后一个名字也先 CAS 到 0 再写
  墓碑。整份写 inode（`meta::put_inode`：写入 flush、truncate、setattr）保留存储里的
  nlink、对读到的字节 CAS，inode 已不存在就 ENOENT，不再把它写回来；新 inode 用
  `put_new_inode`。条件写结果不明（`OutcomeUnknown`，例如 PS 换主时超时）不报错，重读后
  判定：读回自己写的字节就算成功（`change_links` 每次写都带新 ctime，值唯一）；读回它
  当时期望的字节说明没落地，再试；`change_links` 读到别的值就报错（计数已是 0 且本次是 −1
  时例外：没有名字也不会再有，由它回收）——那次可能已落地又被
  别的写覆盖，再减一次会提前减到 0 回收有名字的文件，计数只能宁高勿低
  （`system_fs_links_lost_reply`，failpoint `CHANGE_LINKS_TEST_LOST_REPLY`）。`put_inode`
  不会重复生效（nlink 每次取当前值），读到别的值就重试；它也保留更新的 ctime。以前的
  无条件 put 会被客户端重发，换成条件写后不重读就会让 flush / unlink 在一次抖动上失败。
  `change_links` 不带写租约：链接计数是名字空间状态，挂载上已失效的写租约不能让已删了
  名字的 unlink 卡在计数上（泄漏）。以前三处都是读-改-写：一个会话 unlink 唯一的名字、另一个会话同时
  link 它，link 拿着旧计数把 inode 写回或挂上新名字，数据已被回收（40 轮里 29 轮新名字
  悬空或读不出）；写入方 flush 缓存里的 meta 会把别的会话刚加的链接写回旧值，之后删掉
  原名就回收了新名字的数据；目录 chmod 也会写回别的会话 mkdir 前的 nlink
  （`crates/manager/tests/system_fs_links.rs`，各自去掉对应机制即红）。
- **unlink / rmdir** 只删读到的那个目录项（`remove_name`，条件删除）：被别人删了就
  ENOENT，被 rename 或替换了就重读重来，不给别人的 inode 减 nlink，rmdir 也不删别人
  的 inode。
- **rename**：动 key 之前先查（`rename_onto`）：目标是文件则源须是文件，目标是目录则
  源须是目录且目标为空（ENOTEMPTY / EISDIR / ENOTDIR），同 inode 是 no-op。以前目标
  是目录时什么都不查，`/srcdir` 覆盖非空 `/dstdir` 返回 Ok，原目录子树从根不可达。
  先条件删旧名（崩溃只丢名字，不会出现两个名字一个 nlink），再对检查时读到的目标值
  条件发布新名；新名被别人改了就重读重查再发布，查不过或重试用完就放回旧名。发布新名
  和放回旧名结果不明时重读判定（`compare_put_settled`）：读回自己的值算落地，读回期望
  的旧值就再发一次（两份期望同一字节的写最多落地一个），读到别的值说明没落地也不可能
  再落地。一次结果不明之后，后面那次的"比较失败"也要重读判定：可能正是被前一份迟到落地
  的副本比掉的，当成没落地会把旧名放回，一个 nlink 两个名字（删其一即回收、另一名悬空）。
  以前读到别的值就报错、源 inode 没有名字，迟到的发布再落地时被替换的目标没减 nlink
  （`system_fs_links_lost_reply::a_rename_publish_of_unknown_outcome_is_settled` /
  `a_late_landing_publish_is_not_taken_for_a_refusal`，failpoint `SETTLE_TEST_FAULT`）。删旧名结果不明仍报错（读到"没有"分不清是
  自己删的还是别人 unlink 的），源 inode 可能没名字（泄漏，不丢数据），错误里写明。错误
  信息里不带用户的名字：FUSE 按子串映射 errno。被替换的空目录删 inode，父目录
  nlink 跟着改。仍非原子（v1 限制；rename-over 见 UNLINK-1）。
  已知限制（用户 2026-10-22 定为接受）：rmdir 或 rename 覆盖空目录时，"为空"的检查与
  删除之间另一个会话往那个目录里建的名字会失去入口——create 返回成功、文件却不可见
  （泄漏，数据留在 KV 里）。只在跨会话并发对同一目录 rmdir 与建名字时出现；要关闭需要
  目录级协调（先标记目录"正在删除"再查空，建名字的路径发布后再查父目录）。读回恰好是
  自己的值也可能是第三个会话把同一 inode 链接到同一名字所致，此时 rename 会按落地处理
  （需三方以上并发，接受）。同理，副本落地后、重读前别的会话改了这个名字，重读会误导：
  被 unlink 回到期望值就再发一次（名字可能指向已回收的 inode），被替换成别的值就按没落地
  放回旧名（源多一个名字没计数）。要关闭需要 dirent 带版本或每次尝试的 nonce；以前这里
  报错泄漏。
  测试：`crates/manager/tests/system_fs_namespace.rs`。

## Per-session lease + 跨前端围栏

per-session lease 后台任务（5s heartbeat 续所有 held lease + 持久 invalidation
long-poll + `LeaseRevoked` 驱逐）在 `lease_tasks.rs`（core）；mount 传真 kernel
invalidator，binding 传 None（headless，无内核页缓存驱逐）。
`FsState.page_cache_generation` 只有挂载用：挂载跨 open 保留内核页缓存，没持 lease 的 Open
用 `meta::fetch_inode`（直读 KV、不碰 `state.inodes`）拿 `generation` 与这张表比，决定保不保留
（见 `crates/fuse/CLAUDE.md`「页缓存」）。
`FsState.meta_invalidated`：轮询循环收到某 ino 的失效事件（或 poll 失败 / overflow，此时标记全部
held ino）就标记它，`meta::get_inode` 遇到标记时从 KV 重读一次——缓存的 meta 不再活过
别处的写者关闭。心跳对某 ino 拿到 `NotHeld` 时同样处理：manager 已忘掉这个 lease，之后别处的
写者关闭不会再推给本会话，所以忘 invalidation 版本、标记、调内核 invalidator，READ slot 删掉；
WRITE slot 留下并置 `revoked`（与 manager 主动 revoke 同形）：之后的写 EIO，flush 仍带原 epoch、
被围栏拒掉。删掉的话 `write_lease_for` 退成 ANON，脏数据会不带围栏地盖掉现在持 lease 的写者
（`crates/manager/tests/fuse_lease_loss.rs` 两个用例）。这段处理在 `lease_tasks::lease_not_held`，
后台心跳与 binding 的手动 `heartbeat()` 共用。本会话持写 lease 或 inode 脏时不重读（缓存的 size 可能领先 KV，收下更小的
就是 `get_inode_uncached` 说的删数据）。挂载与 binding 都经过这里。

poll 失败或 overflow（`lease_tasks::drop_after_lost_poll`）：可能漏了失效事件，所以全部缓存失效、
READ slot 删掉并 release；WRITE slot 留着、不 release——poll 失败说明不了写 lease 丢没丢，心跳照常续，
真丢了就 `NotHeld` → revoked。以前一并删掉并 release，写 lease 就交给了下一个来要的挂载，本挂载开着的
写 fd 之后每次写都 EIO（`crates/manager/tests/fuse_lease_drain.rs`，toxiproxy 切断 manager 连接）。

WRITE slot 的 epoch 只来自它的授予，心跳不改它（只刷新非写 slot 的版本）：持有中的写者版本不会变，而
先读后写的会话在 manager 那里同时是 reader，被强制抢占后心跳按 reader 续约、带回的是新写者的版本，抄进
写 slot 就能穿过 PS 围栏盖掉新写者已确认的字节（`fuse_lease_deposed_writer.rs`：断开期间 inbox 溢出
丢了 `LeaseRevoked`）。`LeaseRevoked` 可能在 inbox 溢出（>1024 条）时被丢，也可能在 manager 已取走、
回复还没到时连接断开而丢。漏掉后旧写者照用原 epoch 写；它若先读过，心跳一直按 reader 续约、拿不到
`NotHeld`，挡住它的只有 PS 的 inode 围栏：按分区、在新写者第一次写该分区时抬高，所以旧写者的写进不了
新写者已写过的分区，盖不掉它已确认的字节。

`InodeState.write_epoch`：脏状态（`write_buf`、`dirty`）在哪个 WRITE lease epoch 下产生，写入时盖上
（`write.rs` 的 write 路径、`segment.rs` 的 splice），flush 成功后清零。`write_lease_for` 没有 WRITE
条目时退到它：丢了 lease 的脏数据以原 epoch 写出、被围栏拒掉，而不是退成 ANON 盖掉现在持 lease
的写者——条目被删的路径不止一条（Release 丢 revoked 条目、Open 重取前先删、poll 失败 drain），
靠条目在不在来决定 flush 的身份是靶子。`flush_inode` 把围栏拒绝当终局：丢掉未发布状态、记
sticky `flush_error`（下一次 fsync/close 报 EIO）；其他失败也记 sticky、保留 `dirty`——记录在时
flush 只返回它不再 put，报掉之后的那次 flush 才重试（绑定因此要两次 `flush()`）。`dirty` 只在 put
落地后才清（以前先清再 put，put 失败就既没有可重试的、也没有可报告的）。
用例：`crates/manager/tests/fuse_lease_loss.rs` 第二个（A 丢 lease 后 release 的 flush 和之后的
periodic sync 都不能盖掉 B 的 100 字节；fsync 报错）。

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
  没有），再按 dirent 指向谁完成或撤销每个记录。**调用方是 S3 网关的清扫线程**（每
  `--sweep-interval-secs` 一次）。它翻完 `sess/` 的每一页：网关每台 9 个活会话（8 个 worker +
  回收线程），只读第一页（1024）时，约 113 台以上的网关集群会让排在后面的死会话永远没人恢复
  （`system_publish::a_dead_session_behind_a_page_of_live_ones_is_recovered`，只读一页即红）。
  - **Retire 只减一次 nlink**：被替换（或 S3 删除）的旧 inode 若还有别的名字，`retire`
    先把它当前的字节记到 `[0x04]rbase/[S][ino]`，再从这份字节 CAS 出 nlink−1（两次写都带
    会话租约，会话被别人恢复后迟到的写被围栏挡掉）；然后先删 `pend/` 记录、再删 `rbase/`。
    恢复看到 Retire 记录且名字已归 successor 时：有 `rbase/` → inode 正是"它减一"说明已减，
    正是"它"就补减，都不是（崩溃后别人改过这个 inode）就不动（泄漏，不会删掉别的名字还指着
    的 inode）；没有 `rbase/` → 还没开始减，照常减。恢复结束删掉该会话剩下的 `rbase/`。以前
    "已减、未删记录"时崩溃会再减一次，nlink=2 时删掉另一个名字指着的 inode
    （`system_publish::a_retire_is_recovered_once_from_every_crash_point`，恢复不看 `rbase/`
    即红）。只剩一个名字时直接写墓碑（幂等），不写 `rbase/`——除非同一次 retire 先前的
    尝试已写过（CAS 输给了别的改动），那份 `rbase/` 也会被删；崩溃留下它时恢复按它判定，
    结果最多是泄漏。会话租约挡掉的是"被恢复后才醒来"的会话：它的 CAS 重试会读到恢复已减
    过的 nlink=1 去写墓碑，正是这一下删掉别名还指着的数据（`system_publish::
    a_stalled_session_s_retire_is_fenced_after_its_recovery`，failpoint
    `RETIRE_TEST_HOLD`；墓碑与减计数不带租约即红）。
  每个活会话每轮一次 manager acquire（被拒即跳过）。活会话自己留下的记录（结果未知的交换）只在它死后才被
  处理。持有会话的一方必须跑租约心跳（`spawn_lease_background_tasks`），否则会话租约
  30 s 后过期，清扫方会接管并围栏掉它还在写的一切。
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
  并释放。旧 inode 此时已不存在（别的发布者抢先换掉）= PreconditionFailed。本 client 自己对旧
  inode 的**写者**也算冲突（manager 允许同 client 在自己的 WRITE 上取 REPLACE）：只看
  `writer_refs > 0` 或 `mode == WRITE`。以前是 `held_leases` 里有这个 inode 就算冲突，于是同一
  网关 worker 上一个正在进行的 GET（它的 STABLE pin 也记在 `held_leases`）会让覆盖它的 PUT
  得到 409，而换一个 worker 就能成功——别的客户端的读者和 STABLE 本来就不挡 REPLACE。
- **流式写入不需要 `FsState`**：`ObjectStream` 在 `new` 时从 `FsState` 拿一份
  `Rc<ClusterClient>`，之后 `write/finish` 不取任何参数。`NewFile::start_object` 让新文件一开始
  就有数据对象，之后 `write_streamed(..)` 不碰 `FsState`，`flush_streamed()` 把缓冲的尾巴写出去
  并等所有在飞的 put，`finish` 就只剩 inode 一次 put；`PartWriter::write/finish/abort` 与
  `NewFile::abort`（撤销未发布文件，`segment::reclaim_except`）同样不要 `FsState`。共享一个
  `FsState` 的调用方（S3 网关每个 worker）因此只在 begin、inode put、publish 时持锁，字节从不在
  锁内移动。
- **数据对象是连续流水线**（`ObjectStream`）：每个 unit（= `MAX_EXTENT`）一攒满就 spawn 一个
  put，最多 `--segment-stream-inflight`=8 个在飞，满了只等最老的那个，**没有批间屏障**，所以 body 还在到达时
  前面的 unit 已经在写。以前是攒满 8 个 unit（64 MiB）才发一批、整批落地才回去收 body，body 接收
  与写入完全串行。本机 3-EN、4 lane 分区、urllib 客户端、交替 A/B：16 MiB 单流 228 → 259 MiB/s
  （p50 68 → 58 ms），128 MiB 单流 256 → 408 MiB/s；8 并发 413 → 434，那里瓶颈在后端（1 分区时
  291，4 分区 433，同一份代码）。在飞的 put 是 spawn 出去的任务，不 poll 也会前进；
  compio 0.19 drop `JoinHandle` 会取消任务。
- **记录永远领先于数据**：`segc/`（或上传的 `mpa/`）记录说这个对象最多多长，撤销/回收按它删。
  不变量是**任何数据 put 发出之前，记录已持久地覆盖它的末尾**（`ensure_recorded`）。记录最多领先
  已写末尾 `--segment-stream-inflight` 个 unit（64 MiB），用掉一半余量就提前再抬（约每 5 个 unit 一次），同一
  时刻只有一个抬高在飞，所以落地有序。调用方知道长度时（`start_object(state, Some(n))`、
  `PartWriter::begin(.., Some(n))`，网关传 Content-Length / `x-amz-decoded-content-length`），
  `declare` 在后台先把记录写到 `min(n, 64 MiB)`，和第一个 unit 的接收重叠——≤64 MiB 的对象因此
  只写一次记录。**声明的长度只能封顶、不能撑大记录**：回收要按记录枚举每个 key
  （`object_extents` 整表物化），网关不验签，一个 `Content-Length: 2^64-1` 加断开就会让撤销去建
  2^41 个 key、把整个网关进程拖垮（评审发现，测试里 `u64::MAX` 那段，去掉封顶即红）。记录比数据长
  只会让删除多碰几个不存在的 key；`finish` 等完所有 put 之后把它修到精确长度。
  测试 `a_streamed_object_records_ahead_of_its_writes` 每写一块就读持久记录、断言它不落后于已发出的
  put（让 put 不等记录即红）。
- **撤销先 settle**：`write` 返回时它的 put 可能还在飞，所以 `NewFile::abort` / `PartWriter::abort`
  先 `ObjectStream::settle`（等在飞的 put 与记录抬高），再按记录删；失败路径（`fail`）同样先 settle。
  它堵的是 **put 任务自己的重试**：client 对每个 PS 一条连接、PS 按到达顺序解码写，所以先发的 put
  总在后发的 delete 之前落地（这也是去掉 settle 的消融**没有变红**的原因）；但 put 遇到超时或
  PreconditionFailed（分区刚 split）会 refresh 后重发（`put_bulk_opts` / `batch_put` 的回退），那次
  重发可以落在删除之后。旧代码 `write` 等完 put 才返回，这条按构造成立；流水线打破了它，settle 把它
  恢复，只在失败路径有代价。
  **句柄就地 await、落定后才移出**（`settle_oldest` / `settle_raise`）：compio 0.19 drop
  `JoinHandle` 会取消任务；先 `pop` 再 await，请求在那个 await 上被取消（客户端断开）时任务被取消、
  `settle` 也看不见它，已经上线的 put / 记录可能落在回收之后。
- **网关 PUT 的分段计时**留在代码里：`RUST_LOG=autumn_s3::write=debug` 每个 PUT 打一行
  `PUT breakdown`（begin/body/flush/lock/finish/publish 毫秒）。16 MiB、单流实测：begin 0.4、
  body 19.6、写数据 50、finish 0.2、publish 0.4——元数据合计不到 1 ms，所以"合并 inode put 与删
  pending"没有做：两者分处提交点（dirent CAS）两侧，本来也无法合并。
- **回收可以交给后台**（`FsState.reclaim_later: Option<Box<dyn Fn(Reclaim)>>`，只有网关 worker
  设置）：`Reclaim::Inode(ino)` / `Reclaim::Upload(id)`。设置后 `retire`（覆盖、删除、Complete
  覆盖旧对象）只写墓碑（`extent::tombstone_unreachable`），**释放 REPLACE 之后**才经
  `extent::reclaim_now_or_later` 交出；`multipart::abort` 与 Complete 的 `settle` 交出上传
  清理（`cleanup_now_or_later`）。交出之前持久状态已经写好（墓碑、终态上传记录），交接丢失
  只是等下一次扫描。接收方必须是**另一个 client 身份**：manager 不拿本 client 的 STABLE 挡
  本 client 自己的 EXCLUSIVE，同一身份在锁外回收，会删掉本 worker 在名字消失后才 pin 住的
  GET 正在读的数据；换身份后 pin 按普通租约冲突挡住它。也正因如此，交出必须在本 client 放掉
  它对该 inode 的租约之后——否则自己的 REPLACE 让接收方的 EXCLUSIVE 冲突，回收落到扫描。
  未设置（FUSE、Python、系统测试）时一律当场回收，`remove_unreachable_inode` 行为不变；
  死会话恢复的 Retire 分支也当场回收。`retire` 的当场回收失败只记 WARN、照常删掉 Retire
  记录：墓碑已经写好，扫描会找到它。
  交接只覆盖"现在就能回收"的情况，下面几种仍要靠周期扫描（`--sweep-interval-secs 0` 时本
  网关永远不回收，只能等别的开着扫描的网关）：别的 worker / 网关 / 挂载还持有的文件（交出后
  EXCLUSIVE 冲突，墓碑留着）；终态上传里仍有活会话 pending 的对象（`cleanup` 留下上传记录）；
  心跳发现 pin 已丢（`NotHeld`）时直接从 `held_leases` 删掉、不经释放路径，`unlinked_open`
  里的该 inode 不会被交出。
- 已知限制：恢复按"dirent 是否指向它"
  判定，发布者在 CAS 与删记录之间崩溃、且恢复前有人把该名 rename 走，会被误判为未发布而
  撤销；Retire 恢复遇到崩溃后被别人改过的 inode 不减 nlink（泄漏）。`flush_inode` 在 `put_inode` 之前就清掉 `dirty`，FUSE_FLUSH 的
  put 失败后 RELEASE 看不到失败，分段 inode 的缓存不会被丢——下次 Open 按版本重建能兜住，
  PyO3 会读到 strict 错误（不是零）。

### S3 multipart（`multipart.rs`）

**Complete 不读、不拷、不重写任何分片正文**：每个分片写成自己的不可变数据对象
（`ObjectStream`，和分段文件同一种对象），Complete 只读分片记录，写新文件的 `segc/` 记录、
映射和 inode，再经 `publish_inode` 发布名字——工作量随分片数增长，与字节数无关。测试先
删光所有分片正文再 Complete，并比对 Complete 前后的全部 `[0x03]` key，证明它一个 data key
都没碰。

- **状态机只经 CAS 迁移**：`[0x04]mpu/[id]` → `UploadRecord{parent,name,key,state}`，
  `Open → Completing{new_ino,session,frozen} → Completed{new_ino,frozen}`，或
  `Open → Aborted`，每一步都是对该记录的 `compare_write`，所以 Complete 与 Abort 只有一个
  赢家。`frozen` 是 Complete 冻结的数据对象列表：之后迟到或失败的 UploadPart 写出的对象
  不在其中，既混不进文件，也不会被当成文件的一部分删掉。
- **数据对象的归属**：分片记录 `[0x04]mpp/[id][part]` 指向该分片**当前**的对象；上传创建
  过的每个对象都先记在 `[0x04]mpa/[id][session][obj]`（写对象之前、随对象增长重写），
  包括被同一分片号重试取代的。Open 时上传拥有全部；Complete 另在文件下记 `segc/`，文件
  接管 `frozen` 里的对象。终态上传的 `cleanup` 删掉所有**不在** `frozen` 里的对象——
  被取代的重试、Complete 列表没列的分片、Abort 后的一切——但跳过仍有 pending 记录的
  （某个活会话还在写，由它自己看到上传已非 Open 后清理）；只要还有这种对象，上传记录就
  留着给下一次 `sweep_uploads`。`mpa/` key 里带会话，就是为了能问"这个会话还在写它吗"。
- **ETag = hex(对象号) ++ hex(CRC32C)**：每次尝试的对象号都不同，Complete 列表只能点名
  客户端亲眼看到成功的那一次。
- **会话与恢复**：每个 UploadPart 和 Complete 写任何东西之前都在会话下记
  `PendingOp::Part{upload,part,data_ino}` / `PendingOp::Complete{upload}`（后者按新文件
  inode 为 key）。`recover_session` 的两个分支：Part —— 上传拥有该对象（Open 且分片记录
  指向它，或在 `frozen` 里）就留，否则删；Complete —— 状态是本会话、本 inode 的
  Completing 时，名字指向该 inode 就推进到 Completed 并清理，否则撤销并重开上传。Abort
  遇到别人的 Completing：那个会话活着就 Busy，死了就当场恢复它再看一次。
- **发布是提交点，与 `publish.rs` 同一条规矩**：名字换过去之后的任何步骤（改 Completed、
  删 pending、清理）都不能让 Complete 失败，只记 WARN 留给重试 / 恢复 / 扫描。发布返回
  `PublishError::Other`（结果未知：在途请求之后仍可能落地）时**不撤销**，上传留在本会话
  的 Completing、pending 记录保留；否则一个之后落地的名字会指向已删的 inode
  （消融：对 Other 也撤销，`system_multipart` 断言上传仍是 Completing 处变红）。确定没
  发布的错误（条件不成立、Busy、目录挡路、发布前的写失败）才撤销。
- **本会话的重试能收尾**：上传停在本会话的 Completing 时，重试 Complete 先看名字——已指向
  那个文件就推进到 Completed 并返回它（消融：一律 Busy，`system_multipart` 变红）；否则
  分不清是在途未落地还是撤销失败，返回 Busy，等会话恢复。
- **成功之后的重试得到同一个答案**（S3 也是这样；SDK 丢了回复会重试）：`complete(state,
  upload, s3_key, ..)` 返回 `Completed{ino, generation}`。上传是 Completed 就直接答，不再看名字
  是否仍指向该文件（之后被覆盖、删除也一样答原 ETag）。`cleanup` 删上传记录之前先写
  `[0x04]mpx/[deadline BE][id]`（到期索引）再写 `[0x04]mpc/[id]` = `CompletedUpload{key,
  new_ino}`，记录没了时 `completed_earlier` 按它回答，并核对 S3 key。保留
  `--multipart-completed-retention-secs` = 1 h（SDK 重试几分钟内放弃，`object_store` 默认 3 min）；
  `sweep_uploads` 里的 `expire_completed` 从 `mpx/` 头开始扫、遇到第一条未到期即停，**只读到期
  的条目**，所以保留的记录再多也不增加每轮扫描成本（放在 `mpu/` 里会让每 30 s 的扫描逐条读
  它们）。generation 取 `meta::FIRST_GENERATION`：Complete 发布的永远是全新 inode。两个清理者
  同时收尾，或清理在写完答案、删上传记录之前崩溃后重来，都会多写一条到期索引：较早的那条
  到期时删掉答案，所以这时保留期从第一次写答案算起，可能短于记录真正消失后的 1 h（网关停了
  50 min 再起，重试窗口只剩约 10 min）。不会答错，也碰不到活数据。
  消融：跳过 `remember_completed`，`system_multipart` 在重试处变红。
- **清理的代价**：`frozen` 转成 `HashSet` 再逐条比对 `mpa/` 记录（最多 10000 分片 ×
  若干重试，线性）；`sweep_uploads` 翻完所有上传页，不只第一页（前 256 个 Open 上传挡不住
  后面的终态上传）。
- **已知限制**：
  - 网关的回收线程调 `sweep_uploads`、`recover_dead_sessions`，并接收 worker 交出的
    `Reclaim`；HTTP 路由见 `crates/server/CLAUDE.md` 的 `autumn-s3` 一节。
  - 保留期过后才到的重试仍是 NoSuchUpload。
  - `create` 不记 pending：会话在建完记录后死掉，留下的 Open 上传没有分片也不会被自动
    清理（S3 用 lifecycle 的 AbortIncompleteMultipartUpload 处理这种情况，这里还没有）。
  - Complete 进行中收到的 UploadPart 回 Busy；若该 Complete 随后失败并重开上传，这个分片
    已被删掉，客户端需要重传。
  - `PendingOp` 在末尾新增了两个变体，已有变体的判别值不变。v4 树只在本地开发集群上用过，
    生产集群仍是 v3，没有旧的 pending 记录需要转换。

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
- v4 = 分段文件 + 内容代数：`InodeMeta` 加 `generation`、`segments`。历史迁移使用一次性
  离线工具原地转换，每个 inode 保留编号/链接/字节，得到 `generation=1`、
  `segments=None`，最后才改戳，不 reset；工具现已移除。
- **缺戳 ≠ 新树**：只有一个 inode 都没有才算新树并盖当前版本；有 inode 却无戳，是 v4 以前
  不盖戳的工具（`autumnfs`、S3 网关）建的，inode 是 v3——拒绝挂载并指向
  对应历史发布的独立转换流程。以前会直接盖 v4，在本地实测转换时把一棵 v3 树变得
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
