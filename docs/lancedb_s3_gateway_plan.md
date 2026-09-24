# LanceDB S3 gateway 实施计划

日期：2026-09-24。关联需求：`feature_list.md` 的 F-LANCEDB-S3-GATEWAY。
状态：计划已记录，功能尚未实现。本计划替代讨论中的流式合并方案。

## 1. 目标、现状与已确认约定

让官方 Python LanceDB 通过 `s3://<bucket>/<key>` 直接读写 autumn，并支持
S3、FUSE、Python Fs 对同一路径并发操作。

已实现：独立 `autumn-s3` 进程、现有 FS 路径映射、基础 GET／HEAD／ListObjectsV2、
流式读取及 EN 直接读取路径。

未实现：S3 写入 API、共享原子发布、分段文件布局、multipart 生命周期及本计划验收。

用户确认的约定：

- 保留现有 FS 路径映射，bucket 为预先创建的一级目录。
- 保留独立网关进程及现有鉴权模型；不承诺 CreateBucket、ACL、versioning、
  虚拟主机寻址或 SigV4 验签。固定客户端实际调用的额外能力按请求轨迹纳入。
- 已有写者占用目标时立即返回冲突，不等待、不抢占。
- S3 GET 期间固定文件内容；FUSE／Python Fs 原地修改立即报忙。
- 允许统一升级协议、文件系统格式、manager、PS 和全部前端。
- 本次只记录计划，不执行实现或部署。

## 2. 硬性验收：Complete 不访问分片正文

**`CompleteMultipartUpload` 不读取、不复制、不重写任何分片正文。**

- Complete 只读取分片描述，校验清单，计算逻辑偏移，持久化分段映射并原子发布。
- 正文校验和在 UploadPart 时计算并持久化，Complete 不重新扫描正文。
- Complete 的工作量取决于分片数量和映射元数据大小，不随正文总字节数增长。
- 禁止通过后台全量合并、首次读取转换或首次修改时全量复制转移合并成本。
- 分片描述与正文分开存储；不能将正文内嵌到描述中，再把正文读取计为元数据读取。

## 3. Autumnfs 分段文件

现有 FS extent key 包含 inode 和 offset，文件内容由这些数据 key 隐式描述，
没有可直接复用的跨 inode 分段映射。因此这部分实现进入共享 FS core，
不能只在 S3 gateway 中增加特殊读路径。

- 增加分段文件布局：文件逻辑区间映射到内部数据 inode 及其局部偏移。
  原有普通文件布局继续支持。
- 每个 part 使用独立内部数据 inode；不挂到用户目录，不出现在普通目录遍历或
  S3 listing 中。分片全部持久化后，才提交可用的分片描述。
- 最终文件引用不可变、可分页的分段映射，以小型根指针发布，避免完整映射超过
  单次 CAS 大小限制。映射引用稳定的逻辑数据身份，不保存裸磁盘位置。
- 三个前端统一解析映射；跨 part Range 拆成现有 KV／EN 读取请求，保留条带和
  EN 直接读取。缺失的已声明分片必须报错，不能当作稀疏洞返回零。
- 修改分段文件时，仅为受影响区间写新数据并更新映射。truncate 裁剪映射，
  随后扩展的区域读零，不得重新暴露已截断的旧数据。禁止整体物化文件。
- 成功发布后，分片数据归最终文件持有；删除上传记录不得删除组成数据。
  回收检查映射、硬链接、打开句柄和进行中的读取，并可在崩溃后恢复。

示例：

```text
最终文件 inode → 分段映射
  [0, 64 MiB)      → part A 的数据
  [64, 128 MiB)    → part B 的数据
  [128, 150 MiB)   → part C 的数据
```

Complete 发布映射，数据留在原处，不生成一份合并后的正文副本。

## 4. 共享发布与并发控制

- 三个前端统一使用共享 FS core 的命名空间保护、内容租约和发布接口，覆盖
  create、mkdir、link、rename、unlink、write、truncate，关闭无租约修改旁路。
- 增加与内容写入互斥的稳定读租约，GET／Copy 源读取持有到读取结束或取消。
  已有普通文件读取语义保持兼容；按请求计数，避免同一 worker 的请求互相释放保护。
- 按固定顺序获取目录和 inode 保护，获取后重新核对路径与对象身份；
  元数据保护不覆盖整个上传过程。
- PUT、Copy 的数据及 multipart 的分段映射先持久化，再检查目标条件，
  通过带租约围栏的底层 CAS 发布。增加条件更新／删除原语，禁止先 HEAD 再普通 PUT。
- inode 增加持久内容 generation，所有入口修改后更新，作为 ETag 身份的一部分，
  避免同大小、同秒重写不改变 ETag。
- PS 校验围栏；授予稳定读或发布权限前排空旧写入，覆盖跨进程、租约失效、
  分区迁移和重启后的迟到请求，不能只依赖客户端判断。
- 持久操作记录用于恢复发布、替换和删除；CAS 响应丢失时按唯一操作标识确认结果。
  仍被打开或读取的旧 inode 延迟回收。

## 5. Multipart 生命周期与取消

- 一个 upload 对应持久任务记录，保存目标、状态和分片描述引用；
  分片描述保存编号、大小、ETag、数据 inode 和上传尝试身份。
- 任务跨 worker、跨网关共享，网关重启后可继续上传或 Abort。
- 支持乱序和并行上传。同一 partNumber 重传时先持久化新分片，原子替换引用后
  回收旧分片，失败或迟到的请求不能覆盖已冻结的完成清单。
- Complete 固定清单，验证分片编号、顺序、大小和 ETag，生成分段映射，
  持久化后执行条件发布；整个过程只访问元数据。
- Abort 使用 `DELETE /bucket/key?uploadId=...`，先持久化取消状态、禁止分片提交
  和发布，再撤销写入并回收数据。成功响应表示取消生效，空间回收允许随后完成。
- Complete 与 Abort 共用持久状态机，由原子提交决定胜者；Abort 成功则不能发布，
  Complete 已发布则 Abort 返回 `NoSuchUpload`，不能删除完成对象。
- 普通 DeleteObject 只删除已发布对象，不隐式取消该 key 的 multipart。
- 清理必须可重试、可恢复，迟到 UploadPart 不能复活任务或遗留无法追踪的数据。

## 6. S3 接口与兼容性

- 补齐 PutObject、DeleteObject、DeleteObjects、CopyObject、HeadBucket 和
  CreateMultipartUpload／UploadPart／CompleteMultipartUpload／AbortMultipartUpload。
- PUT／Complete 支持 `If-None-Match: *` 和 `If-Match`；条件失败返回
  `412 PreconditionFailed`，并发占用返回 `409 ConditionalRequestConflict`。
- 完善条件 GET／HEAD、Range、HTTP 日期、ETag、XML、URL 编码及 SDK 错误解析。
  删除缺失对象保持幂等，批量删除报告逐项结果。
- ListObjectsV2 将对象与 CommonPrefixes 一起计入页大小，支持 `max-keys=0`，
  消除目录 4096 项和递归上限导致的静默漏项。
- 固定官方 `lancedb==0.39.0`（Lance 12.0.0），使用 uv 独立环境和依赖锁，
  不使用本机修改版。保存该发行版依赖版本及真实 S3 请求轨迹。
- 实际出现的 UploadPartCopy、bucket 检查和校验和等请求纳入同一实现。

## 7. 验证计划

### Complete 正文零 I/O

1. UploadPart 全部完成后记录数据路径计数，执行 Complete，断言分片正文读取字节数
   和新增正文写入字节数均为零；映射元数据独立计数。
2. 分片上传成功后，让 Complete 执行上下文中的正文读取／写入接口调用直接失败；
   Complete 仍须成功。恢复正文读取后验证完整文件字节。
3. 重启网关、清空缓存后执行 Complete，重复零 I/O 验证，不能依赖缓存掩盖正文访问。
4. 固定分片数、增加正文总量；另固定总量、增加分片数。记录 Complete 延迟、
   元数据操作数和内存，验证成本来源，不以单次计时替代正文零 I/O 断言。

### 数据、并发与故障

- 验证三个入口完整读取、跨 part Range、边界覆盖写、truncate 后扩展、硬链接、
  重传及清理；校验文件字节一致，修改未触及区间不发生整体复制。
- 验证写句柄占用与 S3 覆盖、GET 与原地写入、同名 create、rename／unlink 与发布、
  快速同大小重写及缓存失效。
- 两个独立进程经不同网关竞争同一表的提交：成功提交全部保留，无法完成的提交
  显式报错。用屏障确定性触发同版本竞争，禁用 CAS 后对应测试必须失败。
- 故障覆盖上传中断、Abort／Complete 竞争、迟到分片、发布成功但响应丢失、
  网关／PS／manager 重启、租约过期及分区迁移；无半对象、无活跃数据误删，
  清理最终完成。
- SDK 验证 Copy、批量删除、超过 4096 项的目录、分页前缀、Range、条件读写和错误解析。

### LanceDB 与性能回归

- 官方固定版本完成建表、追加、重开读取、向量检索、删除和 vacuum。
- 大文件必须实际触发 multipart，并校验完整字节及重启后读取。
- 回归现有模型加载用例；测量读取吞吐、内存、元数据延迟和 Complete 成本。

## 8. 实施顺序、升级与交付

1. 固定客户端环境，捕获请求轨迹，建立 SDK 与数据路径计数验收工具。
2. 实现共享分段布局、读取、局部修改和生命周期回收；验证三个入口一致性。
3. 实现共享租约、围栏及条件发布，验证竞争、失败恢复与 CAS 消融。
4. 实现 multipart 状态机、元数据 Complete 和 Abort，先通过正文零 I/O 硬性验收。
5. 补齐其余 S3 API 和兼容性，完成 LanceDB 端到端、故障及性能回归。
6. 完成升级工具、文档、独立审查和提交；全部验收通过后才标记 feature 完成。

协议及 FS 格式按仓库约定统一升级，提供停写、备份、可恢复离线转换和全组件升级步骤。
转换保留现有文件内容、inode 和硬链接，仅补充新元数据；拒绝旧客户端修改新格式。
部署到现有集群另行执行。

更新相关 crate 架构说明、`docs/ops.md`、用户接入示例和进度记录；增加冲突、围栏失败、
活跃上传、取消、清理结果及 Complete 元数据／正文 I/O 的可观测性。

原 feature 验收项全部保留；本次用户新增的正文零 I/O 和混合访问要求作为补充记录。

## 9. 实现设计（2026-09-24 细化）

本节把第 3–5 节落到具体的 key、状态机与原语上，是后续各步实现与评审的共同依据。
固定客户端 `lancedb==0.39.0` 的真实请求轨迹见 `scripts/lancedb_s3/`：它只用到
Range GET、ListObjectsV2（带/不带 delimiter）、PutObject、`If-None-Match: *`
的 PutObject（manifest 提交，冲突方收到 412 后用 HEAD/GET 核对）、HeadObject、
DeleteObjects（带 Content-MD5）和 5 MiB 分片的 multipart。CopyObject、
DeleteObject、`If-Match`、Abort 与 HeadBucket 没有出现，但仍按原验收实现。

### 9.1 已落地的原语

- **`MSG_COMPARE_WRITE`（wire 47）**：带围栏的条件 put/delete。先查围栏、再比较、
  再写；请求抬高的 floor 无论比较成败都持久化，失败的比较在 floor 落盘后才返回。
  因此"期望值不可能成立"的一次调用就是对该 key 所在分区的纯 floor 抬升——恢复方接管
  死会话的租约后，用它把死会话的迟到写挡在外面。
- **有序、可续的 ListObjectsV2** 与分页 `readdir`：按 S3 字节序遍历、从 token 续读，
  每页代价约为一页条目加 token 路径每层一次扫描；无上限、无整树重走。

### 9.2 FS 格式 v4（`SCHEMA_VERSION` 3 → 4，离线转换）

`InodeMeta` 增加两个字段（rkyv 布局改变，按仓库惯例用一次性 `migratev3_v4`
转换器改写全部 `[0x01]` inode，转换完删除，不留兼容代码）：

- `generation: u64`：内容代数。任何改变内容的操作（flush 写入数据、truncate、
  分段映射替换）都加一。S3 ETag = `ino` 与 `generation` 的十六进制拼接，同大小、
  同秒重写也会变。
- `segments: Option<SegmentRoot>`：`Some` 表示文件内容由分段映射定义，此时文件自身
  不再有 `[0x03]` extent、`inline_data` 与 `stripe` 均为 `None`。

分段映射：`SegmentRoot { map_id, count, page_starts: Vec<u64> }`；页不可变，存于
`[0x05][map_id BE][page u32 BE]`，每页至多 1024 个
`Segment { off, len, data_ino, data_off, lanes, unit }`（逻辑区间 → 数据对象的局部
偏移）。`map_id` 由 inode 分配器发号，全局唯一。映射引用数据对象的逻辑身份
（`data_ino` + 几何），不引用磁盘位置。区间之间的空隙是洞，读零。

**数据对象**：一个 part（或一次分段修改写入的新数据）独占一个 `data_ino`，不挂任何
目录、不出现在 listing。数据按 `unit`（= `MAX_EXTENT`）稠密写在
`[0x03][lane][data_ino][off]`，`lane = (off/unit + data_ino) % lanes`——按
`data_ino` 错开起始 lane，否则 5 MiB 的 Lance 分片全部落在 lane 0。因为稠密，读映射
覆盖的区间时每个 key 必须存在：**缺 key 报错，不当稀疏洞读零**（`ChunkSpec.strict`）。

### 9.3 分段文件的读与改

- 读：`read::prepare` 按页加载与请求区间相交的段（页不可变，按 `(map_id, page)`
  缓存），为每段生成指向数据对象的 strict chunk；跨 part 的 Range 自然拆开，条带与 EN
  直读不变。
- 改（FUSE / Python / 其它核心写入方共用 `write::flush_inode` →
  `extent::write_region`）：被写区间 `[off, off+n)` 写成一个新数据对象 D；新映射 =
  旧映射在该区间内被 D 替换（`splice`）；新页写在新 `map_id` 下；最后带围栏
  `put_inode` 发布新 root、size 与 generation。**不读旧数据、不做 RMW、不整体物化**。
  truncate 只裁剪映射；随后扩展的区域没有段，读零，旧数据不会重新暴露。
- 回收：写入方在创建新数据前先记 `[0x04]segc/[file_ino][map_id]`（本次新增的
  `map_id` 与数据对象）。回收者持有 EXCLUSIVE 租约（见 9.5，证明此刻没有其它读写者）
  并对 inode key 所在分区做一次围栏抬升后，以文件**当前**映射为唯一真相：记录里不等于
  当前 `map_id` 的页、不被当前映射引用的数据对象都是垃圾，删掉后删记录。这对任意
  次数的修改与任意崩溃点都成立，不需要逐步对账。

### 9.4 命名空间发布与会话

- **会话**：每个发布方（每个网关 worker）持有一个会话 inode `S` 的 WRITE 租约并由
  既有心跳续期，登记 `[0x04]sess/[S]`。它发起的全部数据写与发布 CAS 都以 `(S, epoch)`
  为围栏。
- **意图记录**：每个未完成的操作在开始写数据前登记 `[0x04]pend/[S]/[obj]`（PUT/Copy：
  目标父目录、名字、新 inode N、期望的旧 dirent；UploadPart：upload id、分片号、数据
  对象；Complete：upload id）。操作完成后删除。
- **PUT / Copy**：分配新 inode N，写数据与 inode meta，然后对 dirent 做
  `compare_write(expected=旧 dirent 或 None, value=N)`（`If-None-Match: *` 即
  expected=None；`If-Match` 先核对旧 inode 的 ETag，CAS 本身保证核对之后 dirent 未被
  换过）。条件不成立返回 412 并回收 N。CAS 在应答丢失后重试时可能把自己已成功的发布
  报成冲突：dirent 值里的 N 对这次尝试唯一，所以返回 412 前先回读 dirent，指向 N 即
  视为成功。父目录按需逐级 put-if-absent 创建，已存在的同名目录直接复用。被替换的旧
  inode 走退役回收（9.5）。
- **死会话恢复**：扫描 `sess/` 的任一方对 `S` 做非强制 `acquire(WRITE)`：Conflict 表示
  会话还活着；Granted 表示原持有者的租约已过期、manager 已把版本抬高，此时恢复方对 fs
  命名空间的每个分区做一次纯 floor 抬升（`fence_all(S, epoch)`），从此死会话的任何迟到
  写都会被拒，然后逐条按"dirent 是否已指向 N"决定补完或回收，最后删 `sess/[S]`。
- **目录删除**：S3 的空"目录"不应出现在 delimiter listing 里；目录不随对象删除而
  删除（删目录与并发创建之间没有多 key 事务），由 listing 在生成 CommonPrefix 前确认
  该子树仍含对象。

### 9.5 租约模式（manager，wire 48）

在 READ / WRITE 之外新增：

| 模式 | 用途 | 与其它客户端已持有者的冲突 |
|------|------|------|
| STABLE | S3 GET / Copy 源读取期间固定内容 | 与 WRITE、REPLACE、EXCLUSIVE 冲突 |
| REPLACE | PUT/Copy/Complete 替换 dirent 期间 | 与 WRITE、REPLACE、EXCLUSIVE 冲突；不挡 READ/STABLE |
| EXCLUSIVE | 回收已不可达或分段修改遗留的数据 | 与任何持有者冲突 |

WRITE 与 STABLE 互斥，因此 GET 期间 FUSE/Python 的写打开立即 EBUSY，反之亦然；
PUT 在替换期间持有旧 inode 的 REPLACE，已有写者时立即 409。REPLACE 与 EXCLUSIVE
复用 writer 槽（持久化为普通 writer 记录；failover 后按 WRITE 恢复，只会更保守）。
STABLE 与 READ 一样不持久化，由 30 s TTL 与心跳维持。网关按请求计数持有 STABLE，
同一 worker 的并发 GET 共享一次 acquire，最后一个请求结束后释放。

不可达 inode 的回收（unlink、rename 覆盖、S3 覆盖/删除）先写 `rmtomb`，再尝试
EXCLUSIVE：拿到才删数据，拿不到留给扫描方重试——打开中的文件与进行中的读取因此不会
被抽掉数据。本挂载自己仍打开着的 inode 在最后一次 close 时再回收。Python `autumn.Fs`
的写入必须持有 WRITE 租约，关闭无租约修改旁路。

### 9.6 Multipart 状态机

- 记录：`[0x04]mpu/[id]` = `{target, state, ...}`，`state ∈ Open | Completing{frozen} |
  Completed | Aborted`；分片 `[0x04]mpu/[id]/p/[part BE]` = `{data_ino, size, etag,
  crc32c, lanes, unit, attempt}`；分配 `[0x04]mpu/[id]/a/[data_ino]`。
- UploadPart：确认 Open → 登记 pend 与 alloc → 写数据对象（边写边算 CRC32C；ETag 由
  CRC32C、大小与 `data_ino` 组成，因此同时标识这一次上传尝试；只有请求带 Content-MD5
  时才另算 MD5 并校验，不让每个分片都付 MD5 的单核代价）→ CAS 替换分片记录 → 再读
  状态：仍 Open 则删 pend（数据归 upload），
  否则自清。被同号重传替换下来的旧分片不在此处删除，留给终态清理——这样迟到或失败的
  请求永远删不到冻结清单里的数据。
- Complete：读分片记录、按请求清单核对编号顺序/大小/ETag → CAS `Open → Completing
  {frozen}` → 写文件 inode（`segments` 指向由冻结清单直接生成的映射页）→ 按条件对
  dirent 做 `compare_write` 发布 → CAS `Completing → Completed` → 清理。整个过程只
  读写元数据：**分片正文零读、零写**，数据对象原地成为最终文件的段。条件失败回到 Open。
- Abort：CAS `Open → Aborted` 后回收；遇到 `Completing` 且 Complete 方会话仍活着则
  409，会话已死则先做死会话恢复再判定；遇到 `Completed` 返回 `NoSuchUpload`。
- 终态清理：alloc 中不在冻结清单里的数据对象回收（冻结清单中的只删 alloc 记录，
  所有权已转给文件）；其 pend 仍属活会话的暂留，由该会话自清。全部处理完才删分片记录与
  upload 记录。
- Complete 的正文零 I/O 由数据路径计数器（extent put/get 字节数）与"正文 I/O 失败注入"
  两种方式验证（第 7 节）。
