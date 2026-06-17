# Value Stream 设计：大 value 从 log_stream 分离（DESIGN，2026-06-17）

> **状态：设计提案，未实现。** 起因是排查"EC extent 10 删不掉、`autumn-op
> info` 看不到"的现场问题(见 feature_list.md `MERGE-REFS-LEAK`)。根因不是
> 单个 bug,而是 **log_stream 同时兼任 WAL 和大 value 存储** 这个结构选择,
> 叠加 CoW split 多主,导致 GC 结构上无法"拥有"extent 的删除。本文给出对齐
> Azure WAS 的重构方向。**先落的止血修复(vp 侧删除触发 + 兜底 sweep,见
> §7 Phase 0)与本重构正交,且是本重构终态的删除原语。**

## 1. 问题

当前(WiscKey value-log 模式):大 value(>4KB)写入 **log_stream**,SST 里只
存 VP(`extent_id, offset, len`)。log_stream 同时是 WAL。于是一个 log extent
背负**两条独立生命周期**:

| 生命周期 | 含义 | 谁驱动 | 计数 |
|---|---|---|---|
| WAL 截断 | "已 checkpoint,可截断" | flush 进度 | `refs`(log_stream 成员) |
| value 保留 | "还被活 SST 的 VP 引用" | compaction / 覆写 | `vp_table_refs` |

物理删除要求 `refs==0 && vp_table_refs==0`(manager CLAUDE.md note 2/3)。但
删除触发器(`extent_can_delete` → `enqueue_pending_deletes`)**只挂在 refs
一侧**(`handle_stream_punch_holes` / `handle_truncate`);vp 侧
(`handle_sync_partition_vp_refs`)只递减 `vp_table_refs`,**从不触发删除**。

后果:GC 按 WAL 节奏 punch 掉一个还被 SST 引用的 value extent → `refs=0` 但
`vp_table_refs>0` → 被保留 → 之后 compaction 把 `vp_table_refs` 减到 0 → **没
有任何代码再回头删它** → 永久不可见的 dead orphan(extent 10)。CoW split 把
单个 log extent 变成多主(多个子分区的 log_stream + SST 都引用),使该状态几乎
必然发生。

附带代价:大 value 留在 WAL 里 → WAL 巨大 → recovery replay 要流过全部 value
字节(只为抽出 VP),正是 F120 / F261 一直在压的 replay 膨胀根因。

## 2. 参考:Azure WAS 怎么做(SOSP 2011)

每个 RangePartition 有职责严格分离的多条 stream:
- **Commit Log Stream** —— 纯 WAL,只记小提交记录;checkpoint 后 truncate-from-head。
- **Row Data Stream** —— checkpoint(行 + 索引)。
- **Blob Data Stream** —— **大对象字节**,blob 写入直接落这条(它本身复制/持久),
  行里只存指针(键值分离,等价 VP)。
- **Metadata Stream** —— 指向上述 + checkpoint。

关键:
1. **Stream Layer 只认一种引用 = stream 成员**;extent 不被任何 stream 引用 →
   Stream Manager 删除。单一引用、单一删除权。
2. **GC 由 partition layer 驱动**:partition 算出 live extent 集合,把 stream
   成员重写成该集合;SM 据成员引用计数删除。
3. **split/merge 用 MultiModify 共享 extent 指针(CoW)**,成员引用计数 = N;
   各子分区独立重写 → 计数归零时 SM 删。
4. **大 value 不待在 WAL**:commit log 纯瞬态;value extent 的生命周期只有
   "被行引用"一条 → "在 value stream 里 ⟺ 还被引用"由 GC 维护 → 删除 = 成员
   归零,GC 真正拥有删除。

autumn-rs 抄错的点:把 WAL 和大 value 存储合并进 log_stream,凭空给 value
extent 多了一条 WAL 生命周期,让 GC 按 WAL 节奏 punch value extent。

## 3. 提案:独立 value_stream

每个 partition 的 stream 角色调整:

| stream | 现状 | 提案后 |
|---|---|---|
| log_stream | WAL + 大 value | **纯 WAL**(小提交记录),truncate-from-head |
| value_stream(新) | — | **大 value 唯一的家**,VP 指进它 |
| row_stream | SST(VP 指针) | 不变(VP 改指 value_stream) |
| meta_stream | checkpoint | 不变 |

**value extent 的唯一生命周期 = `vp_table_refs`**(被活 SST 引用);跨 split 用
**已有的 `partition_vp_refs` 聚合**处理多主。删除 = `vp_table_refs→0`。

### 3.1 写路径(关键:大 value 不双写)

```
小写(≤4KB):  不变 —— 值内联进 WAL 记录 + memtable。不碰 value_stream。
大写(>4KB):
  1. value 字节 → append 到 value_stream(all-replica-ack,持久)→ 得 VP
  2. 小提交记录 [key][seq][VP] → append 到 log_stream(WAL,all-replica-ack)← commit 点
  3. 插 memtable(存 VP)
```
- **大字节只落一次(value_stream)。** WAL 只多一条几十字节指针记录;**总 WAL 量
  比现状还小**(现状 WAL 含整个 value)。
- **崩溃语义**:步骤 1 后、2 前崩 → value_stream 有孤儿字节,无人引用 → 回收;
  步骤 2 落了即一致。**value 先持久、再写 WAL commit** 是不变量。
- **延迟**:朴素实现大写 +1 RTT(WAL 记录要引用 value append 返回的 VP,顺序
  依赖)。优化:StreamClient 预分配 value_stream 偏移 → 两个 append 并行发、都等
  ack → 延迟 ≈ max 而非 sum。小写无变化。

### 3.2 读路径

VP 改指 value_stream;`resolve_value` / `read_value_from_log` 从 value_stream
读。F259 直读、F216-E ZC 读路径照旧(只换目标 stream)。EC value extent 仍走
ec_subrange_read。

### 3.3 回收模型:只用 compaction,不要独立 GC loop

value extent 的回收分两类:

- **全死 extent**(里面 value 全无人引用):compaction 丢死 key → 最后一个引用它
  的 SST 被 compact 掉 → `sync_partition_vp_refs` 把 `vp_table_refs` 减到 0 →
  **vp 侧删除触发**删之。**无需迁移,纯 compaction + 触发。**
- **碎片化 extent**(少量 live value 钉住一个大部分死的 extent):compaction 默认
  不搬 value 字节 → 死空间收不回。需把 live value 迁到新 value extent。**做法:
  把迁移折进 compaction(RocksDB BlobDB 式)** —— compaction 时发现某输入 value
  extent 存活率低于阈值(复用 `GC_DISCARD_RATIO`),顺手拷其 live value 进新
  value extent、更新 VP。

于是**砍掉独立 GC loop / punch / refs-vp 赛跑**,回收全部变成:
```
compaction(丢死 key + 碎片高时折叠迁移 live value → 新 value extent)
  → sync_partition_vp_refs 递减老 value extent 的 vp_table_refs
  → vp_table_refs→0 → vp 侧删除触发 → SM 删
```
- 迁移**工作量**不消失(碎片化时存在),只是从独立 loop 折进 compaction;按存活率
  阈值触发,成本 profile 同现状 GC。**纯 write-once 负载**(图片只上传不覆写)只有
  "全死"一类 → compaction + 触发彻底够用,迁移几乎不跑。
- CoW split 多主:partition C 迁移 C 的 live value 后 C 的新 SST 改指新 extent;
  D 仍引用老 extent;两边都迁移后 `vp_table_refs`(经 `partition_vp_refs` 聚合)
  归零 → 删。**不再产生 extent 10。**

### 3.4 refs / vp_table_refs 收敛

- value extent:生命周期收敛为**单一** `vp_table_refs`。是否保留 `refs`(value_stream
  成员)作冗余校验,实现期定;关键是删除只由 `vp_table_refs→0` 触发。
- log_stream(WAL)extent:只有 `refs`(WAL 成员),truncate-from-head 递减,
  归零即删 —— 纯净,无 value 牵连。

## 4. durability / recovery

- value_stream all-replica-ack 后才写 WAL commit(§3.1)。
- recovery replay **只读 WAL 的小指针记录**(不再流过大 value 字节)→ replay 轻一
  个量级,直接缓解 F120 / F261 的 replay 膨胀。value 在 Get 时按 VP 懒读。
- 孤儿 value 字节(commit 前崩)由回收路径(`vp_table_refs==0`)清理。

## 5. EC / 策略分叉

WAL(热、小、写密集)→ 复制 + 高频 fsync;value_stream(冷、大、只读)→ 激进
EC。现状两者绑在 log_stream 一条 stream 上策略冲突;分离后各自独立(autumn-rs 已
支持 per-stream EC)。

## 6. 迁移(老 log_stream VP 数据)

无 rolling、全停全启(memory note `feedback_stopworld_restart_primary`)。两种:
- **A. 一次性迁移**:升级版本带迁移步骤 —— 扫每个 partition 的活 SST,把仍指向
  log_stream 的 VP 值拷进新 value_stream,改 VP,checkpoint;旧 log extent 转纯
  WAL 语义后由 truncate 回收。幂等 + 格式戳收尾(R1 `cluster_version`)。
- **B. 双读过渡**:读路径同时认"VP 指 log_stream(旧)/ 指 value_stream(新)";新
  写只进 value_stream;旧值随 compaction 自然迁移(碎片折叠)到 value_stream,最终
  log_stream 只剩 WAL。无显式迁移步骤,收敛较慢。

倾向 B(无停机迁移窗、靠既有 compaction 收敛),A 作为加速兜底。实现期定。

## 7. 分阶段

- **Phase 0(止血,已就绪/建议先落)**:vp 侧删除触发 + both-zero 兜底 sweep。
  让 `vp_table_refs→0`(及 refs→0)时删除可靠触发。**它是本重构终态的删除原语**,
  与 value_stream 正交,先落不浪费;立即消除 extent 10 类孤儿的"不可回收"。
- **Phase 1**:引入 value_stream + 写路径(大 value 落 value_stream,WAL 只存 VP)。
- **Phase 2**:回收折进 compaction(BlobDB 式碎片迁移),退役独立 GC loop / punch。
- **Phase 3**:迁移老 log_stream VP 数据(§6),log_stream 收敛为纯 WAL。

## 8. 风险 / 待定

- 大写 +1 RTT:靠偏移预分配并行抹平(§3.1),需验证 StreamClient 改造复杂度。
- compaction 折叠迁移让 compaction I/O 含 value 读写(碎片时),需 admission 限速
  纳入(复用 F196 D-r7 的 compact/gc 通道)。
- 迁移期双读(方案 B)的 VP 来源判别(log vs value stream),需 wire/格式标注。
- value_stream 的 stream 数量/分裂策略(单条超大 vs 多条),与 16GiB max_extent
  及 EC 分块(stream CLAUDE.md note 12)的交互。

## 参考

- feature_list.md `MERGE-REFS-LEAK`(本设计的触发现场 + Phase 0 止血)。
- manager CLAUDE.md note 2/3(refs vs vp_table_refs 双计数),partition-server
  CLAUDE.md GC 节(run_gc / discard / vp_deps)。
- WAS: Calder et al., "Windows Azure Storage", SOSP 2011。
- 对比方案(已否):value 内联进 SST(compaction 写放大);split 时拷贝 value
  (split 变重)。本方案两者皆避(value 不随 SST 重写;value_stream CoW 共享)。
