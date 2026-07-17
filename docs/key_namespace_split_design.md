# 内置应用 Key 命名空间 + 分裂规则 — Design Doc

Status: PROPOSAL（未实现；本文档只做设计与论证）
Date: 2026-07-16
相关 feature 条目：F-KEY-NS-FUSE / F-POLICY-SIZE-EST-LIVE / F-SPLIT-AT-KEY / F-PRESPLIT-DEPLOY-MODE（feature_list.md）

用户提出两条主张：
1. **内置的应用必须有 prefix**；
2. **有内置的应用对应的 split 规则**。

本文的结论（先亮立场，论证在后）：

- **主张 1 完全成立，且不是新约定** — `crates/autumn-memory/src/keys.rs`
  开头已经把"共享 KV namespace + 各接口保留前缀"写成了明文契约，
  kvc/、mem/ 都遵守，**fuse 是唯一的违约者**（裸 `0x01`–`0x04` 字节）。
  这不只是热点问题，是一个真实的隔离/损坏漏洞（§2.1）。
- **主张 2 的目标成立，但实现位置要改**：per-app 规则不应该进 partition
  layer（PS/manager 机制层），应该进 **policy/controller 层**（F203 的
  mechanism/policy 分离是本仓库自己立下的架构原则）。partition 层已有的
  split 机制（median-user-key，任意字节切点）对所有应用**天然够用**；
  真正坏掉的是**触发器的度量口径**（`size_bytes` = LSM 常驻字节，对
  大 value 负载失明，§2.2）和 presplit 模式被部署层硬编码埋掉（§2.3）。
  修好这两个，"per-app split 规则"就退化成 policy 层的一小段配置——
  而 policy 层做 per-app 识别的前提恰恰是主张 1 的 prefix。

---

## 0. 背景：三个 "partition 大小" 口径到底是什么（先把事实钉死）

线上对 partition/17 同时存在三个互相矛盾的数字。**三个都是真的，
只是量的不是同一个东西**：

| 口径 | 数值(part 17) | 定义 | 代码位置 |
|---|---|---|---|
| ① `PartitionLoad.size_bytes`（`info --part 17 --detail`、policy split/merge、hot/cold size 维度、Prometheus gauge） | 741 MB | **LSM 常驻字节** = Σ SST `len` + active/imm memtable 字节。>4 KiB 的 value 走 ValuePointer，SST/memtable 里只有 ~24–40 B 的指针，**value 本体（在 log_stream）完全不计** | `partition-server/src/lib.rs::lsm_resident_bytes`（1228）；writer 在 `background.rs:309-317`（30 s 刷新） |
| ② `autumn-op info` 每 partition 的 `live_size` | 45.4 GB | 该 partition 三条 stream（log/row/meta）**去重后所有 extent 的字节和**：sealed 取 `sealed_length`，open tail 现场 probe EN。**包含尚未 GC 的死字节**（被覆盖/删除的大 value）与 CoW split 后与兄弟共享的 extent（双计） | `autumn_op/main.rs::run_info`（~2106-2134） |
| ③ dashboard/overview 的 `live_size` | — | ②的周期性 rollup 版：manager sealed 和 + PS 上报的 `open_tail_bytes`（5 s 心跳携带，30 s 刷新） | manager `compute_cluster_overview_resp`（F-OVERVIEW-OPENTAIL） |

推论（这是 §2.2 的全部根源）：

- **真实活数据量介于 ① 和 ② 之间**：② − gc_debt − open_tail_dead ≈ 活数据。
  45.4 GB 里有相当部分是 fuse 覆盖写/删除产生的 WAL debt；741 MB 则几乎
  只是索引。**两个数都不是"该 partition 承载了多少用户数据"。**
- policy（split/merge/hot-cold size 维度）消费的是 ①。对 fuse/kvcache
  这类几乎全部字节都在 VP 里的负载，policy 看到的是一个缩小 ~60× 的影子。

**订正一处文档陈旧**：manager CLAUDE.md:234（F-OVERVIEW-OPENTAIL 段）仍写着
"`PartitionLoad.size_bytes` has no writer (always 0) … F-PS-SIZE-BYTES-DEAD
deferred"。该 gauge 已于 2026-07-05 复活（`background.rs:309` 的
F-PS-SIZE-BYTES-DEAD 注释就是复活现场），写入的就是 LSM 常驻字节。
结论没变（对 VP 负载失明），但"恒 0"的描述已过期，随本设计落地时应同步改。

---

## 1. 现状

### 1.1 四个内置应用与它们的 key 空间

| 应用 | 前缀 | key 形态 | 位置 |
|---|---|---|---|
| kvcache | `kvc/` | `kvc/{tenant}/vllm/v1/{sha256-hex}/{layer}`（vLLM pool；sglang 为 `kvc/{tenant}/kv/...`） | `python/autumn_kvcache/autumn_kvcache/_keys.py`（`KEY_NAMESPACE="kvc"`）、`vllm_connector.py:281` |
| memory | `mem/` | `mem/{tenant}/{agent}/ep\|fact\|doc\|idx\|ivf\|node\|edge/...`，动态组件百分号编码 | `crates/autumn-memory/src/keys.rs`（`NS="mem"`） |
| fuse | **无** | 裸类型字节：`[0x01][ino]`（inode）、`[0x02][parent][name]`（dirent）、`[0x03][ino][off]`（extent 数据）、`[0x04][field]`（superblock，含 `rmtomb/` unlink 墓碑） | `crates/fuse/src/key.rs:17-20` |
| client | 无（裸 key） | 用户任意字节；bench/perf-check 写 ASCII hex key | — |

约定本身已经成文：`keys.rs` 头注释 —— "Memory records share the autumn KV
namespace with the other interfaces (fuse / kvcache / client), under a
**reserved `mem/` prefix**"；`_keys.py` 同样声明 `kvc/` 为 reserved
namespace。**fuse 违约，client 天然无法约束（它就是裸 KV 面）。**

### 1.2 实测 key→partition 映射（线上 32 partition，hexstring presplit）

线上边界 `[, 07ffffff) [07ffffff, 0ffffffe) …` 与
`hex_split_ranges(32)`（`autumn_op/args.rs:930`：均分 `format!("{:08x}")`
的 ASCII 空间，边界字节 0x30–0x66）逐字节一致 —— 部署即 hexstring 模式。
后果：

- fuse 的 `0x01`–`0x04` < `'0'`(0x30) → **全部落第一个 range partition**
  （线上 = part 17，承载 ~19 GB 模型权重的全部 extent）。
- `'k'`(0x6b) 和 `'m'`(0x6d) 都 > `'f'`(0x66) → **kvc/ 与 mem/ 一起
  塌缩进最后一个 range partition**（线上 kvc = part 108）。
- 只有 bench 的 hex 字符串 key 真正分散。

### 1.3 presplit 现状

- `autumn-op bootstrap --presplit N:normal|N:hexstring|N:fuse`
  （`autumn_op/main.rs:1813-1822`；`normal`/未知 = 单 partition）。
- `fuse_split_ranges(n)`（args.rs:972）：切点 `[0x03,0,0,0,0,0,0,0,byte]`
  —— 按 inode 低字节把 **文件数据** 摊到 ≤256 个桶；`[0x01][0x02]` 全进
  第 0 桶，`[0x04]` + 一切 ASCII key（kvc/mem/bench）全进最后一桶。
- **两条部署路径都硬编码 hexstring**：`deploy/docker/entrypoint.sh:338`
  （`--presplit "${n_parts}:hexstring"`）与
  `deploy/baremetal/autumn-deploy:580`（`"${PRESPLIT}:hexstring"`）。
  fuse 模式在部署里**选不到**，现成能力被埋掉。
- 注意 fuse 模式的固有上限：**一个文件的所有 extent 共享一个 inode →
  永远在同一个 partition**。fuse presplit 摊的是"文件之间"，摊不了
  "一个 19 GB 权重文件内部"。后者只有运行时 split 能切（§3.4）。

### 1.4 运行时 split 机制（现状能力，别低估它）

`handle_split_part`（partition-server）：flush memtable → 取 LSM 内
**去重 user key 的中位数**作 `mid_key` → `multi_modify_split(mid_key,…)`
CoW 复制三条 stream。要点：

- `mid_key` 是**完整的任意字节串 user key**，没有"只能切到第 K 字节"的
  限制 —— 切点想多深就多深（回答 §3.4 的 kvcache 问题）。
- 但选点是 **key 数量的中位数，不是字节量的中位数**；且要求 LSM 里
  ≥2 个去重 key、`has_overlap==0`。
- 触发它的自动策略读的是口径 ①（§2.2）。

---

## 2. 问题陈述（按严重性排序）

### 2.1 P0 — fuse 无前缀 = 隔离/损坏漏洞（不是风格问题）

任何 client（SDK、bench、误配置的应用）写一个以 `\x01`–`\x04` 开头的
裸 key，就直接落进 fuse 的元数据/数据空间。没有任何机制拦截，目前靠
"没人会这么用"的默契。具体后果分级：

- **静默文件系统损坏**：写 `[0x01][ino]` 覆盖 InodeMeta、写
  `[0x03][ino][off]` 篡改文件内容 —— fuse 无从察觉。
- **可触发的数据删除**：`[0x04]rmtomb/[ino]`（`key.rs:120`）是 unlink
  墓碑，**mount 时的 sweep 对墓碑指向的 inode 无条件删数据**
  （"the sweep deletes data unconditionally"）。一个格式碰巧匹配的裸
  key = 下次 mount 时删掉对应 inode 的全部 extent。这把"误写"升级成
  "误删且延迟引爆"。
- 反向污染：fuse 假设 `0x01`–`0x04` 空间归它独有（prefix scan、
  next_inode 计数器），client 的杂 key 混进去会让 readdir/sweep
  扫出无法解析的记录。

对比：mem/ 有百分号编码防止组件伪造分隔符，且 F-AUTHZ-1 的数据面
capability 机制（`docs/data_plane_authz_design.md`，PS 侧
`protected_prefixes` + `allowed_prefixes` 前缀检查）已经为 "按字节前缀
做服务端隔离" 修好了路 —— **fuse 的裸字节空间连被这个机制保护的资格
都没有**（它没有可声明的前缀）。因此 P0 的完整修复 = **D1（前缀迁移，
获得可保护性）+ D6（authz enforcement，把可保护变成已保护）**，两者
是强依赖不是并列 —— 单独 D1 只把伪造成本从"碰巧撞进"提高到"多打
几个字节"，删数据的 INVARIANT 仍然只靠默契（论证见 §3.6）。

### 2.2 P1 — 自动分裂/合并对大 value 负载整体失明（独立严重问题，确认成立）

policy（`crates/manager/src/policy.rs`）的 size 维度全部消费口径 ①：

- **split 的 size 触发永远不会响**：`size_bytes > SPLIT_SIZE_HARD(50 GiB)`
  要求 *LSM 常驻* 50 GiB —— VP 负载下对应 ~3 TB 级的真实数据；实际上
  part 17 扛着 45 GB stream 数据时 ① 只有 741 MB。QPS 触发
  （15 K sustained）对大文件低 QPS 高带宽的 fuse/kvcache 同样够不着。
  **对这两类负载，自动分裂这个安全网是死的。**
- **merge 候选方向性错误**：`merge_candidates` 要求左右两侧 `size_bytes`
  都 < 1 GiB → 线上把扛着 45 GB stream 数据的 part 17 判成 merge 候选
  （`policy-candidates` 实测输出；注意谓词是**两侧各自 < 1 GiB**，
  reason 字符串里的 `size_sum<…` 只是展示格式）。
- **hot/cold 的 size 维度同样失明**（`HOT_COLD_MIN_HOT_SIZE_BYTES =
  25 GiB` 的 LSM 常驻字节，同一口径）。

**当前爆炸半径评估**（重要，别夸大也别轻描淡写）：

- auto-policy 默认 Off；部署层 dashboard 默认开但
  `AUTUMN_DASHBOARD_ALLOW_MUTATIONS` 默认 0（entrypoint.sh:120 /
  autumn-deploy:509）；内置 preset 里只有 `aggressive` 打开 merge/split
  开关。所以**今天不会有自动误合并发生** —— 它是 advisory 毒药：
  操作员看着 `policy-candidates` 的 MERGE 建议手动执行、或某天 arm 了
  aggressive preset，就会把一个 45 GB 承载的 partition 冻结-合并掉。
- 误 merge **不丢数据**（stream splice，extent 全保留），但代价真实：
  survivor 背上全部 extent、合并需 freeze-drain 大 partition、想再拆回
  去要先 major compaction 消 overlap —— 而且合并的"理由"本身就是错的。
- 更隐蔽的是反向：**该分裂的永远等不到分裂**，热点 partition
  （§2.3）只能靠人肉 `autumn-op split`。

### 2.3 P2 — 热点：内置应用各自塌缩在单 partition

§1.2 的实测：fuse 全量在一个 partition、kvc+mem 共挤另一个 partition。
写放大/恢复时长/单 P-log 线程吞吐都被钉在单分区上。且 §2.2 说明自动
分裂救不了它。presplit 的 fuse 模式存在但部署选不到（§1.3），kvc 则
**根本无法 presplit**（tenant 名在 bootstrap 时未知，hash 又在
`kvc/{tenant}/vllm/v1/` 之后的可变偏移处）。

### 2.4 P3 — 口径混乱 + 文档陈旧（观测性问题）

§0 的三个数字没有任何一处文档并排解释；`info` 的 45.4 GB 被当成
"真实数据量"（其实含死字节）、`--detail` 的 707 MB 被当成矛盾（其实是
索引大小）、manager CLAUDE.md 还说 size_bytes 恒 0（已复活）。这直接
造成了本次调查的三次误判，值得单独修。

---

## 3. 设计

### 3.1 D1（P0 修复）：fuse key 迁入保留前缀 `fs/{tenant}/{volume}/`【前缀字面值已拍板】

**方案**（2026-07-16 用户拍板：`fs/{tenant}/{volume}/`，与
`kvc/{tenant}/...`、`mem/{tenant}/{agent}/...` 的 tenant+instance
两层模式对称 —— tenant=归属，volume=独立文件系统实例，一个 tenant 可有
dev/prod 多个。每 key ~16 B 的代价已知悉并接受；决策不对称性是理由：
多两层猜错"要"只赔字节，少两层猜错"不要"要再停机重灌一次）：

```
fs/{tenant}/{volume}/\x01[ino BE]                inode meta
fs/{tenant}/{volume}/\x02[parent BE][name]       dirent
fs/{tenant}/{volume}/\x03[ino BE][off BE]        file extent
fs/{tenant}/{volume}/\x04[field]                 superblock / next_inode floor / rmtomb
```

- **tenant/volume 组件字符集限制** `[a-z0-9._-]+`（mkfs/mount 时
  fail-loud 校验），**不做百分号编码** —— 它们是运维配置名，不是运行时
  任意数据（mem/ 需要 q() 编码的原因在此不成立）。
- 选 ASCII 前缀而不是另一个保留裸字节（如 `\x05`）：与 kvc/、mem/
  同一 convention，可 grep、可在 `autumn-op ls` 里辨认、可被 F-AUTHZ-1
  的 `protected_prefixes` 直接声明。两层结构额外送一个能力：authz 既可
  按 `fs/` 整族保护，也可按 `fs/{tenant}/` 分租户授权。
- 前缀之后的内部布局**不变**（保持 BE 排序性质、positional 解析，
  dirent 名里的 `/` 不需要转义 —— fuse 解析是定长偏移不是 split-on-/）。
- 排序影响：同一 volume 内 `\x01 < \x02 < \x03 < \x04` 保持；整个 fuse
  空间落在 `'f'` 开头 —— 与 kvc(`k`)/mem(`m`) 相邻但不重叠。
- **superblock 加 schema 版本戳**：`fs/{t}/{v}/\x04schema_version = 2`
  （per volume），mount/`autumnfs` 启动时读取，遇到旧版（找不到新前缀
  但扫到裸 `\x01`）**响亮拒绝** 并指向迁移工具 —— 符合仓库 "rkyv
  fail-loud、持久结构改动必须带迁移" 的既有纪律（manager CLAUDE.md
  note 39）。

**多 volume 的连带设计（每 volume 必须自持全部状态）**：

不变量：**一个 volume 的全部持久状态 = 它自己的 key range
`fs/{t}/{v}/` + 一个 per-volume 的 manager inode 计数器，别无其他。**
这使 create/delete/backup/restore 一个 volume = 对一个前缀区间（+ 一个
etcd key）的操作，天然正确。逐项：

- **inode 编号是 volume 作用域的**：`fs/a/x/\x01[5]` 与
  `fs/a/y/\x01[5]` 是不同 key，跨 volume 无碰撞可能。因此
  **`ROOT_INO=1` per volume 直接成立** —— FUSE 内核协议要求的
  root ino=1 本来就是 per-mount 概念，volume 作用域的 ino 命名空间让它
  无需任何全局协调。
- **`next_inode` 计数器每 volume 一份（必须）**：manager 侧 etcd key
  由全局单个 `autumn-rs/fs/next_inode`（F-FS-UNIFY M0）改为
  `autumn-rs/fs/{tenant}/{volume}/next_inode`；KV 侧迁移 floor 同步变
  `fs/{t}/{v}/\x04next_inode`。注意：共享全局计数器本身不会造成 ino
  碰撞（key 空间已隔离），但它给每个 volume 埋一条隐蔽的跨 volume
  元数据依赖 —— 单 volume 的灾备重建无法只凭自己的 KV floor 安全 seed
  全局计数器（必须 max-merge 所有 volume 的 floor），volume 删除也留下
  无法归还的全局余额。per-volume 计数器消灭这条依赖，让上面的不变量
  成立。
- **wire 影响**：`MSG_ALLOC_INODES`（`AllocInodesReq`）增加 volume
  标识字段（tenant+volume，或直接传规范化前缀）→ **WIRE bump**
  （照例 MIN=MAX、same-commit 停机部署）。这是 D1 唯一的 wire 变更
  （§4 表已更新）。
- **rmtomb 墓碑随 superblock 进 per-volume 空间**：mount 时的 unlink
  sweep 只扫本 volume 的 `fs/{t}/{v}/\x04rmtomb/` 前缀 —— P0 的
  "伪造墓碑删数据"通道被前缀+authz 修复之后，即使未来出现 volume 内
  误写，爆炸半径也被锁死在单 volume。
- **`autumnfs` 的非 CAS inode 分配 race（README 已知问题）——多 volume
  不放大它**：该 race 的域是"同一个计数器 key 的并发写者"；分 volume
  之后不同 volume 的写者根本不再相遇（各自的 `\x04next_inode`），
  同 volume 内与今天完全相同。根治（autumnfs 改走 manager
  `alloc_inodes`）是正交的既有事项，不在本设计范围。

**迁移选项**（现有唯一线上集群持 ~19 GB fuse 数据；迁移目标统一为
`fs/default/default/`）。**已拍板（2026-07-16）：走 A（重灌），不写
迁移工具** —— 依据是线上实际盘点（`autumnfs ls` 全树）：整个 fuse
文件系统只有模型权重，无配置/无状态/无不可再生数据：

```
/models/qwen32b/model-0000{1..5}-of-00005.safetensors   19 GB   Qwen2.5-32B-AWQ（在用）
/models/qwen/model-0000{1,2}-of-00002.safetensors       5.5 GB  旧 7B（已无消费者）
```

全部可从 hf-mirror 重新下载（现成上载 job，实测 download ~13 min +
upload 分钟级）。**附带收益**：`/models/qwen`（旧 7B，vLLM 已只读
qwen32b）重灌时直接不灌 = 顺带回收 5.5 GB × RF3 ≈ 16.5 GB EN 空间 ——
EN 池总量仅 100 GiB（5 × 20 GiB），不是小数。选项表保留作方案记录
（B/C 的否决理由仍然成立）：

| 选项 | 做法 | 代价 | 失败模式 |
|---|---|---|---|
| A. 重灌（**已拍板**） | 新代码 mkfs 到 `fs/default/default/`；模型权重从来源重新 `autumnfs put`；旧裸 key 批量 delete（或直接 `cluster.sh` 级重建） | 一次停机 + 19 GB 重传（分钟级）；要求数据可再生（模型权重可以） | 来源不可得的数据会丢 —— 迁移前必须盘点 fuse 里是否只有可再生数据 |
| B. 离线迁移工具 | `autumnfs migrate-ns`：停 mount → 扫 `\x01..\x05` 范围 → 逐 key 读出重写到 `fs/default/default/` → 删旧 key → 戳版本 | LSM 无 rename，重写 = 全量数据过一遍集群（19 GB 读+写+GC）；工具一次性代码 | 中断后半迁移状态 —— 用版本戳 + "新前缀存在即以新为准" 的幂等重跑解决 |
| C. 双读兼容期 | 新 fuse 读新前缀 miss 后回退旧前缀，写只写新；后台搬迁 | fuse core 每条路径双份（readdir/rename/unlink 的 prefix scan 要合并两个空间，正确性面大）；观察期内隔离漏洞仍在（旧空间还得可写/可删） | 复杂度换来的只是 "不停机"，而当前唯一集群完全可以停机 |

**推荐 A（数据可再生时）/ B（不可再生时），拒绝 C** —— C 的复杂度
服务于一个当前不存在的约束（不可停机的多集群），且延长漏洞窗口。
仓库的升级纪律本来就是全停全启。

**配套 → 已升级为独立设计项 D6（§3.6，已拍板"彻底启用"）**：authz 把
`fs/`、`kvc/`、`mem/` 全部登记为 protected prefix 并启用 enforcement。
注意因果关系：**前缀迁移（本节 D1）是 authz-for-fs 的前置条件** ——
authz 保护的是前缀，裸 `\x01`–`\x04` 没有可登记的前缀（登记单字节
`\x01` 会把整个低位 key 空间锁死，殃及 client 裸 key 面）。P0 的
rmtomb 通道要 D1+D6 一起才算闭环，单独 D1 只是缓解（伪造从"碰巧
撞进"变成"多打几个字节"，INVARIANT 仍靠默契）。详见 §3.6。

### 3.2 D2：命名空间注册表（一处成文，消灭下一个 fuse）

新增 `docs/` 内一节（可并入本文档维护）+ 根 CLAUDE.md 一行指针：

| 前缀 | 所有者 | 分隔/编码规则 |
|---|---|---|
| `fs/{tenant}/{volume}/` | autumn-fuse | tenant/volume 限 `[a-z0-9._-]+`；其后为二进制定长布局 |
| `kvc/` | autumn_kvcache | `/` 分隔，组件不含 `/` |
| `mem/` | autumn-memory | `/` 分隔 + 百分号编码 |
| （其余全部） | client 裸 key 面 | 无约束；**新内置应用必须先在此登记前缀** |

规则：新内置应用不登记前缀不得合入 —— 这是主张 1 的制度化，也是
§3.5 里 policy 层做 per-app 识别的数据基础。

### 3.3 D3（P1 修复）：给 policy 一个看得见 VP 字节的 size 口径

**关键观察：不需要任何新的 PS 侧计量，四个组件都已经在船上**：

```
est_live_bytes(part) =
    Σ sealed_length(该 partition 三条 stream 的去重 extent)   # manager 状态，已有
  + PartitionLoad.open_tail_bytes                              # PS 已上报（F-OVERVIEW-OPENTAIL）
  − PartitionLoad.gc_debt_bytes                                # PS 已上报（F187，sealed 死字节）
  − PartitionLoad.open_tail_dead_bytes                         # PS 已上报（F-DF-WALDEBT，open tail 死字节）
```

- **纯 manager 侧改动**（policy_tick 里计算，喂给 split/merge/hot-cold
  的 size 谓词），**零 wire 变更、零 PS 热路径成本**。
- `size_bytes`（LSM 常驻）保留，作为第二信号（它对小 value 负载仍是
  准的；两个口径取 max 喂 split、取 est_live 喂 merge 是最保守组合：
  **merge 必须用 est_live** —— 这是防止"45 GB 判成冷分区"的那一半；
  split 用 `max(size_bytes, est_live)`）。
- 已知误差（可接受，须写进代码注释）：
  - CoW split 之后共享 extent 在两个 child 各计一次 → 刚分裂的
    partition 短期高估，major compaction 后收敛。高估只会**推迟 merge、
    提早 split** —— 方向安全。
  - open_tail_bytes 30 s 刷新 + probe 失败保持旧值 → 分钟级陈旧，
    与 policy 的 5 分钟窗口匹配。
  - 阈值语义变化：`SPLIT_SIZE_HARD=50 GiB` 现在量的是真实承载字节，
    对 fuse/kvcache 第一次变得可达 —— 需要跑一轮线上校准（part 17
    在新口径下 ≈ 45 GB − debt，很可能立即成为 split 候选，这是
    期望行为，但 arm 之前要人工确认一次）。

### 3.4 D4：split 机制维持 app-agnostic；补一个 `split --at KEY`

回答 kvcache 问题（问题 4）：**现有机制已经支持"切到第 N 个字节"** ——
`mid_key` 是完整 user key（§1.4），对 `kvc/{tenant}/vllm/v1/{hash}/…`
取中位数得到的切点就是某个真实 key，天然落在 hash 空间中间；sha256-hex
均匀 → 按 key 数量的中位数 ≈ 按 hash 空间的中位数。fuse 大文件同理：
`[0x03][ino][off]` 的中位数可以落在一个文件内部 —— **运行时 split 能
切开单个 19 GB 文件，这是任何 presplit 都做不到的**（§1.3）。

两个补强：

- **`autumn-op split <PART> --at <KEY>`**（新增，小改动）：操作员/
  controller 指定切点，PS 校验 in-range 后跳过 median 选点直接走既有
  `multi_modify_split`。用途：(a) 对**空或数据極少**的 partition 做
  事后 presplit（tenant 出现后再按 `kvc/{tenant}/vllm/v1/{00→ff}` 切 ——
  bootstrap 时切不了的现在能切了）；(b) policy 层 per-app 规则的执行
  原语。需要 wire 变更（SPLIT_PART req 加 optional key 字段，WIRE bump，
  照例 same-commit 部署）。注意放宽 `user_keys.len() >= 2` 检查仅限
  显式 `--at` 路径（median 路径保持原判定）。
- **字节量中位数（不做，记录理由）**：median-by-key-count 对 fuse 是
  近似字节均衡的（extent 定长 ≤8 MiB）；对混合负载会偏，但修它需要
  SST 侧按 key 累计 VP 字节直方图 —— 在 D3 + `--at` 之后没有剩余的
  真实场景，违反 reproduce-first。

### 3.5 D5（回应主张 2）：per-app 规则放 policy 层，prefix 是它的前提

**正面回应"presplit 只是更准的猜测"这一质疑：同意，且加码** ——
presplit 连"更准"都很有限：kvc 的 tenant 维度 bootstrap 时不存在，
fuse 的单大文件 presplit 切不开。**任何 bootstrap 时刻的静态规则都
无法消除猜测**；能消除猜测的只有"看着真实数据分裂"的运行时机制。
所以优先级是：

1. **D3（修口径）让现有自动分裂对所有负载活过来** —— 这是安全网本体；
2. **D4（--at）给策略层一个精确执行原语**；
3. **presplit 降级为冷启动优化**：D5a 把部署层的模式露出来
   （`AUTUMN_PRESPLIT` 已有数量，加 `AUTUMN_PRESPLIT_MODE`，默认保持
   hexstring 不破坏现网脚本；fuse 集群配 fuse 模式），不再追求
   per-app 的 presplit 规则库。

**那么"partition 层运行时识别 key 属于哪个应用、按应用声明的规则
分裂"要不要做？—— 不要，理由三条：**

- 机制上不需要：split 选点已经是 data-driven 的（median 跟着真实 key
  分布走），app 身份不改变"往哪切"的答案；
- 架构上有先例约束：F203 花了真实成本把 manager 清成纯 mechanism、
  把策略赶进 controller（auto_policy.rs），PS 里塞 app 规则是开倒车；
- 演化上更脆：partition 层认识 `kvc/` 的那天起，`_keys.py` 改版就要
  同步改 Rust —— 跨语言耦合换不来任何 median+--at 给不了的能力。

**但用户直觉里对的部分要保留**：policy/controller 层（以及人）确实
需要"这个 partition 在承载哪个应用"来选阈值/解释 advisory——
`kvc/` TTL 型缓存分区或许允许更激进的 merge，`fs/` 分区的 merge 应该
更保守。这个识别只需要拿 partition 的 `[start_key, end_key)` 与
§3.2 的前缀注册表求交 —— **manager 侧十几行的纯函数**，且它成立的
前提恰恰是主张 1（fuse 不迁前缀，`[0x01…, 0x04…]` 这个 range 永远
"匿名"）。这就是两条主张真正的依赖关系：**prefix 是 policy 层
per-app 规则的前提，而不是 partition 层 split 代码的前提。**

落点（当触发实现时）：`auto_policy.rs` 的 advisory 描述与阈值挑选
按前缀注册表分档；`policy-candidates`/dashboard 输出里标注
`app=fs|kvc|mem|client`。

### 3.6 D6：authz 彻底启用，覆盖全部内置前缀【已拍板】

**决定（2026-07-16，用户："authz 彻底都加上吧"）**：`fs/`、`kvc/`、
`mem/` 三个内置前缀全部登记为 protected prefix 并**启用 enforcement**
（不是"只登记不强制"）。

**为什么这才是 P0 的真正闭环（因果链，逐环）**：

1. rmtomb 的 INVARIANT（`key.rs:114` — "a tombstone is only ever
   written for an unreachable inode; the sweep deletes data
   unconditionally"）成立的前提是**只有 fuse 自己能写墓碑 key**。
2. 只做 D1（前缀迁移）：伪造 key 变成
   `fs/{t}/{v}/\x04rmtomb/…` —— 任何 client 仍然写得出来，只是从
   "碰巧撞进"变成"照着格式多打几个字节"。INVARIANT 仍靠默契。
3. D1+D6：`fs/` 是 protected prefix → PS 在每帧派发顶端
   （`authz_gate`）default-DENY 无 token 连接对 protected key 的读写
   → 伪造被机制挡住 → **INVARIANT 第一次由机制而非默契保证**。
4. 依赖方向：authz 保护的是**前缀**，裸 `\x01`–`\x04` 没有可登记的
   前缀 —— **D1 是 D6-for-fs 的硬前置**（登记单字节 `\x01` 为
   protected prefix 会把整个低位 key 空间锁死，殃及 client 裸 key
   面，不可取）。`mem/`/`kvc/` 无此依赖，可先行（见灰度顺序）。

即：**前缀给了"可保护性"，authz 才把它变成"已保护"。** 据此本文把
P0 修复的定义从"D1"修正为"D1+D6"；§4 的实施顺序同步更新。

**机制现状盘点（以代码为准；订正一处外部判断）**：

| 层 | 现状 | 位置 |
|---|---|---|
| Token | Ed25519 capability（CapClaims/sign/verify、kid 多密钥、aud=cluster_id、短 TTL、domain separation） | `crates/rpc/src/cap_token.rs` |
| KDC | manager leader 签发；etcd `tenantAccount/` 账户库；`MSG_MINT_TOKEN` / `MSG_GET_AUTHZ_CONFIG` | `crates/manager/src/authz.rs` |
| 强制 | PS `authz_gate` 每帧派发顶端；`check_key`/`check_range`（整扫区间 ⊆ 单 allowed prefix）；enabled=单 AtomicBool，关时零成本 | `crates/partition-server/src/authz.rs` + lib.rs |
| 开关 | manager 二进制 `--auth-signing-key-file`（无 key = 全关）+ `--auth-protected-prefix`（可重复；**缺省 = 只有 `mem/`**）；PS 5 s poll 生效 | `crates/server/src/bin/manager.rs:310-322` |
| 运维入口 | **已存在**（外部盘点说 "autumn-op 没有 authz 子命令" 不属实）：`autumn-op gen-signing-key` / `tenant-create --tenant --prefix`（admin-token gated）/ `tenant-delete` / `mint-token` | `autumn_op/args.rs:203-225` |
| SDK | `connect_with_credential`/`set_tenant_credential`；懒 mint + **exp 前 300 s 自动续期**（续期驱逐旧 PS 连接）；`PermissionDenied` 为 terminal 错误（不重试） | `crates/client/src/lib.rs` |
| memory | `MemoryStore::connect_with_credential` ✅ 已接好（authz e2e 覆盖） | `crates/autumn-memory` |

**真正缺的东西（"彻底启用"的实施清单，不是机制而是接线）**：

| 客户端 | 缺口 | 令牌获取/续期路径（设计） |
|---|---|---|
| fuse mount daemon | `FsState`/mount 无 credential 参数 | mount flag `--tenant --credential-file`（或 env）→ `connect_with_credential`；SDK 自动续期已覆盖长挂载；k8s 用 Secret 投递 credential 文件 |
| kvcache（Python vLLM/sglang） | PyO3 `BatchClient`/`autumn.Fs` 未暴露 credential | binding 加 `credential=` 参数 → 透传 `set_tenant_credential`；vLLM 侧走 `kv_connector_extra_config`，k8s Secret → env |
| `autumnfs` / `autumn-client` CLI | 无 credential flag | `--credential-file`（复用 args.rs:70 的 secret-file 读取 helper） |
| 部署层 | manager 未配 signing key / protected prefixes | entrypoint/autumn-deploy 加 `AUTUMN_AUTH_SIGNING_KEY_FILE`（k8s Secret 挂载）+ `AUTUMN_AUTH_PROTECTED_PREFIXES=fs/,kvc/,mem/` |
| 运行时改前缀列表 | `--auth-protected-prefix` 是启动 flag，**改动需重启 manager**（PS 5 s poll 收敛）；无热更 RPC | 可接受（改动低频 + 全停全启是仓库惯例）；若嫌重，后续补一个 leader-gated set RPC，不阻塞本设计 |

**灰度路径（机制没有 audit-only 模式 —— 用两个既有属性替代）**：

审计模式（先记违规不拒绝）目前不存在，补它要动 PS 强制路径 ——
**不建议**为灰度专门加，因为已有两个天然灰度轴：

1. **按前缀灰度**：protected_prefixes 是清单，逐个加。顺序 =
   `mem/`（SDK 已全接好，今天就能开）→ `kvc/`（等 Python credential
   接线）→ `fs/`（等 D1 前缀迁移落地）。每一步只影响一个应用。
2. **凭据先行**：AUTH_HELLO 对未启用 authz 的集群是 no-op OK、对
   非 protected 前缀无影响 —— 所以**先给全部客户端发凭据并上线带
   credential 的配置，观察 mint/hello 正常，再把前缀加进 protected
   清单**。翻车回滚 = 从清单移除该前缀 + 重启 manager（5 s 收敛）；
   注意设计规定生产**清空**整张清单需显式 `--allow-disable-authz`，
   移除单个前缀不受此限。

**失败模式（正面回答，不粉饰）**：

- **配错凭据/漏发凭据 = 该应用全线写失败**：fuse mount 挂不上或
  EIO、kvcache save 全 miss、memory 写拒绝。`PermissionDenied` 是
  terminal（SDK 不重试，正确 —— 盲重试只会放大）。灰度路径 2
  （凭据先行）就是为把这个风险压到"加前缀那一刻已验证过"而设。
- **manager（leader）不可达 > TTL−300 s → 续期失败 → token 过期 →
  PS 关连接/拒写**：启用 enforcement 后，数据面对控制面产生一条
  **宽限期 = TTL 的可用性依赖**（强制本身不回调 manager，但续签
  leader-only）。这是 authz 的固有代价，必须写进运维认知：TTL 是
  "撤销窗口 vs manager 故障容忍"的权衡旋钮 —— 按设计默认小时级，
  比 F265/etcd-chaos 实测的 manager 故障恢复时间（秒~分钟级）有
  充足余量，但"manager 挂一整天"从此不再是数据面无感事件。
- **令牌轮换**：kid 多密钥 + disabled 位 + PS 5 s poll，机制已支持
  （加新 kid → 新 token 用新 kid → 旧 token 自然过期 → 禁旧 kid）；
  租户撤销 = 删账户停续期，窗口 = TTL。时钟偏移有 30–120 s leeway。
- **既有旁路（继承设计的 WON'T-DO，不因 D6 改变）**：rogue client
  直连 EN 猜 `(extent_id, offset)` 可**只读**原始字节 —— 可信内网 +
  只读 + 需猜中坐标，2026-07-01 已明确接受；D6 不重启这个决定。

### 3.7 D7：写入强制归属 namespace（"类 table"，取消裸 key 写面）【已拍板】

**决定（2026-07-16，用户）**："要写入的话，都要写到某一个 namespace
下面（类似于 table），这可能涉及到 authz，修改 clusterclient。"
即：**取消"client 裸 key"这个写入面** —— 任何写都必须先指明
namespace，如同 HBase 必须指明 table。范围按用户原话锚定在
**写入**（put/delete/batch/stream-put）；读/scan 不设限（运维
`autumn-op ls`、调试、跨 namespace 巡检都需要裸读，见下"立场"）。

**分层原则（本设计的骨架，先立后破）**：namespace 是
**SDK + manager 元数据层**的概念，**绝不下渗到 partition/stream 层**
—— PS/manager 的 split/merge/路由继续只看字节区间，不存在
per-namespace 的 META 表或独立 region 空间（HBase 类比到 API 为止，
存储层仍是单一全局 range 空间）。这与 §3.5 拒绝"partition 层识别
app"是同一条原则的两面。

**① ClusterClient API 形态 —— namespace 句柄（HBase `Table` 模式）**：

```rust
let ns = client.namespace("bench").await?;   // 校验存在 + 绑定（可携带该 ns 的 credential）
ns.put(key, val).await?;                     // 实际写 key = b"bench/" ++ key
ns.range(start, limit).await?;               // 扫描区间自动钳在 [bench/, bench0)
```

- **为什么是句柄而不是连接绑定或每调用传参**：连接绑定（一个
  client 一个 ns）对单应用（fuse/kvcache）够用但堵死多 ns 工具；
  每调用传参污染全部签名且易漏传。句柄两者兼得，且是 credential 的
  天然挂载点（D6：一个 namespace 的 token 就绑在它的句柄上）。
- **裸 key API 的去向**：`ClusterClient` 的裸 `put/delete` 改为
  crate-private + `raw_` 前缀，仅供内置应用 crate（fuse/memory 经
  自己的 key builder）与 admin 工具链使用；对外二进制入口
  （`autumn-client put` 等）打仓库标准的 migration-error stub。
- **改动面清单**（全部随 D7 一次改完）：

| 面 | 改动 |
|---|---|
| `crates/client/src/lib.rs` | `Namespace` 句柄类型（prefix 前置 + range 钳制 + per-ns credential）；裸写降级 `raw_*` |
| PyO3（`python/src/lib.rs`） | `BatchClient(namespace=…)` / `Autumn.namespace("x")` 句柄 |
| `autumn.Fs` / fuse mount / `autumnfs` | 无额外改动 —— D1 之后它们的 namespace 就是 `fs/{tenant}/{volume}/`，本来就经 key builder 全前缀化 |
| kvcache（`_keys.py`）/ memory（`keys.rs`） | 已全前缀化（kvc/、mem/）；内部改走句柄属实现细节 |
| `autumn-client` CLI | `--namespace`（put/get/del/head/ls） |
| **perf-check / ycsb（必须重设计，见下）** | `--namespace`（默认 `bench/`）+ key 策略修正 |
| manager | etcd `namespace/<name>` 注册表 + `MSG_GET_AUTHZ_CONFIG` 响应扩字段带 namespace 清单（wire bump）+ `namespace-create/delete` RPC |
| PS | 写路径 membership 检查（复用 authz_gate 的前缀匹配基建，见 ②） |

- **perf-check 的 key 策略在 D7 下是违规的，必须重设计**（复核
  `autumn_client/main.rs:25` 发现）：`key_for_partition` 生成的
  bench key = **该 partition 的 `start_key` ++ `"!"` ++ tag** ——
  它按当前分区边界构造 partition-local key。D7 之后一个起点在
  `fs/…` 里的 partition 会收到 bench 写 → 落进受保护的 fs
  namespace → 被拒。修正 = bench 只在自己 namespace 的分区集内做
  partition-local 构造。这不是附带损伤而是**修正了一个测量缺陷**，
  见 §3.8 的 perf 悖论。

**② 与 authz 的关系 —— 合并"身份"，分离"强制"（正面回答"能不能
合并成一个概念"）**：

**能合并的（应该合并）**：namespace = **注册单位 = authz 授权范围
单位（allowed_prefix）= presplit 锚点（§3.8）= 未来配额单位**。
一张 etcd 注册表（`namespace/<name>`：前缀、owner tenant（可选）、
presplit 规格、created_at），PS 一条 poll 通道（现成的
`MSG_GET_AUTHZ_CONFIG` 扩字段），一族 CLI（`namespace-create` 可
选 `--with-tenant` 顺手建 owner 账户 = 包装现有 `tenant-create`）。
D6 的 protected_prefixes 手工清单**随之消亡** —— 被"注册表里
`owner != None` 的 namespace 自动 protected"取代。

**不该合并的（强制拆两层，这是我坚持的边界）**：

- **Layer A（存在性，D7 本体）**：写 key 必须落在**某个已注册
  namespace** 的前缀区间内，否则拒（新错误码，语义=NotFound 类）。
  这个检查**不需要 token** —— 纯前缀匹配已注册清单，匿名连接也受检。
  开销与 authz_check 同级（短清单前缀匹配 + enabled 快门）。
- **Layer B（所有权，D6 既有）**：**protected** namespace 还要求
  capability token 覆盖其前缀。依旧以"集群配了 signing key"为
  开关。
- **为什么分层**：合并成"namespace 必然带 token"会把 dev/测试 UX
  杀死（每个 cluster.sh 单测集群都要先造密钥/发凭据），而存在性
  检查（Layer A）不需要任何密钥基建就能成立。namespace 是**身份**
  概念，authz 是**强制**开关 —— 一个 dev 集群可以有 namespace 而
  没有 token；一个 prod 集群两层全开。

**③ 生命周期**：

- **创建**：`autumn-op namespace-create --name bench
  [--presplit 8:hexstring] [--with-tenant]`（admin-token gated，
  与 tenant-create 同门禁）→ etcd `namespace/<name>`（F149 fenced）
  → PS 5 s poll 生效。命名规则与 D1 组件一致：`[a-z0-9._-]+`，
  注册表内以 `name + "/"` 存前缀。**内置三族（fs/、kvc/、mem/）由
  bootstrap 预注册**，注册表粒度 = 顶层 family（`fs/` 一行，不是
  每个 volume 一行）——族内的多级结构（tenant/volume、tenant/agent）
  归应用自己管，Layer A 只查顶层归属（清单短、检查快）。
- **删除**：`namespace-delete` = 前缀 batch_delete（range 扫 +
  批删，慢但低频）+ 摘注册表；**默认拒绝非空删除**（`--force`
  覆盖）。分区不需要同步 merge —— D3 修好后冷分区自然进 merge
  advisory。
- **传播窗口（新失败模式，明说）**：namespace-create 后 PS 最长
  5 s 才见到 → 立刻写会被 Layer A 误拒。缓解：SDK 对"unknown
  namespace"错误做有限重试 + PS 收到未知前缀时触发一次即时
  config refresh（on-miss refresh，与 region_epoch 失配的
  refresh+retry 同型）。

**④ 迁移与兼容**：

- **订正外部盘点一处**：`meta/stats`、`idx/` **不是**存量裸
  namespace —— 它们是 `mem/{tenant}/{agent}/…` 之下的 family
  子段（`autumn-memory/src/keys.rs:174-204`，全部经
  `agent_prefix` 前缀化），不在裸 key 面里，无需迁移。
- 现网真实的裸 key 只有 bench/perf-check/ycsb 的一次性数据
  （§1.2 的实测映射也只见这四类）——**可弃**。因此**不设
  `default/` 兜底 namespace**：兜底会把"忘了指定 namespace"从
  错误变成静默行为，恰好复活 D7 要消灭的模糊性。存量 bench key
  随 D1 的停机批次批删；裸 CLI 入口打 migration stub（仓库惯例）。
- **与 D1 同一个停机批次落地**：两者都是 client 可见的 key 面
  破坏性变更，一次停机吃掉两个 break，不分两次。

**⑤ 代价与我的反对意见（正面回答）**：

- **方向同意**：D7 把 D2（注册表）/D6（authz）/D8（presplit）
  钉在同一个概念上，是这份 doc 从"打补丁"走向"有对象模型"的
  那一步；且它让"下一个 fuse"在 API 层就不可能发生（写不进
  未注册空间）。
- **反对 1 —— namespace 不得下渗存储层**（上文分层原则）。如果
  哪天需要真 per-namespace placement（独立副本策略/独立 EC 档），
  那是另一个系统设计，不要从这里滑进去。
- **反对 2 —— 只管写、不管读**。把读也 namespace 化会破坏运维
  巡检与调试（跨 ns 的 `ls`/info/修复工具），而隔离目标由
  Layer B（protected 读也拒，D6 已做）覆盖 —— 存在性层管读没有
  增益只有摩擦。用户原话也只说了"写入"。
- **反对 3 —— 不与 token 强绑**（②的分层论证）。
- **代价照实记**：≥7 个面的一次性破坏改动 + 一个 wire bump +
  新错误码 + 5 s 传播窗口的新失败模式 + perf-check 重设计。全部
  绑进 D1 的停机批次后，边际运维成本 ≈ 0，但代码量是本 doc 各项
  里最大的（估计与 F-AUTHZ-1 Stage 3 同量级）。

### 3.8 D8：presplit 按 namespace —— 从"集群级一次性模式"到"namespace-create 时的 SPLITS"【已拍板】

**决定（2026-07-16，用户认可 HBase 类比）**：HBase presplit 有效
because 它是 **per-table** 的；autumn 的 presplit 是
**per-cluster 单一模式**，这是根子。

**代码复核（外部断言属实）**：`autumn_op/main.rs:1813-1822` ——
`--presplit N:kind` 整集群一个 `kind`，`hexstring`/`fuse`/`normal`
互斥。§1.2 的实测后果不再赘述；结论成立：**没有任何一个单一模式
对多应用集群是正确的**（hexstring 摊 bench 挤 fs/kvc/mem；fuse
摊 fs 挤其余）。

**设计 —— presplit 的锚点从 bootstrap 时刻移到 namespace-create
时刻**（D7 给了它生命周期，这正是 HBase `create 'table', SPLITS`
的形状）：

- `namespace-create --presplit <SPEC>`：在**该 namespace 自己的
  前缀区间** `[ns/, ns0)` 内按 SPEC 生成切点，逐点调 D4 的
  `split --at KEY`（对空/新 namespace 就是切空分区 —— D4 已为此
  放宽 `≥2 keys` 检查）。SPEC 形态：
  - `N:hexstring` —— 均分 hex 字符串空间（bench 类）；
  - `N:inode` —— fuse 族：volume mkfs 时在
    `fs/{t}/{v}/\x03[ino 低字节]` 上切（**mkfs 时 tenant/volume
    已知**，presplit 天然可用 —— 对比 bootstrap 时刻它们还不存在）；
  - 显式切点列表 —— 任意负载的逃生门。
- `bootstrap --presplit N:kind` **整个退役**：bootstrap 只建单
  partition + 预注册内置三族；初始 namespace 及其 SPLITS 来自
  部署清单（entrypoint/autumn-deploy 的 manifest），每个
  namespace 自带自己的规则 —— **D5a（部署层外露 presplit 模式）
  被本项取代（superseded）**，不再单独做。
- **kvc 的边界照实承认**：tenant 含模型指纹，namespace-create
  （kvc/ 族注册）时也不知道 —— per-namespace presplit 对 kvc
  依旧够不着 tenant 维度。kvc 的摊开 = tenant 首次出现后
  `split --at`（D4，可由 controller 在观察到新 tenant 前缀时自动
  执行），加 D3 修好的自动分裂兜底。**presplit 与 split --at 是
  互补而非竞争；两者都以 D3 为前提**（否则糟糕的初始切分永远等
  不到纠正）。

**perf 悖论（写进问题面，但先订正两处再用）**：

- **订正 A**：复核 `key_for_partition`（autumn_client/main.rs:25）
  —— bench key 不是"恰好摊开的 hex key"，而是
  **`partition.start_key ++ "!" ++ tag` 的 partition-local 构造**：
  perf-check 按当前分区表逐分区造 key。所以它不止"在 hexstring
  模式下碰巧摊开"，而是**无论怎么切分都按构造均匀** —— 它测的
  永远是"理想分布"，**结构性地看不见任何 key 分布热点**。这比
  "碰巧摊开"更糟也更可修：D7 之后该策略还会写进别人的受保护
  namespace（§3.7 ①），必须重设计为"只在自己 namespace 的分区集
  内构造"。
- **订正 B**：今天 "`perf-check` 207k ops/s 全绿 vs `autumnfs put`
  45 s" **不能**当热点证据引用 —— 那次 45 s 已被 root-cause 为
  BUG-MGR-RETRY-CLASS（stale owner_epoch → 每次 alloc 烧 20×500 ms
  重试；见 feature_list 同名条目），与 key 分布无关。
- 订正之后，悖论的诚实形态依然成立且值得写：**perf-check 的分布
  假设（构造均匀）≠ 真实负载的分布（fs/kvc 单分区塌缩）**，所以
  绿色的 bench 数字对真实负载没有预测力 —— 与"presplit 方便测试"
  的预期相反。D7+D8 之后 bench 在自己 namespace 里、真实负载在
  各自 namespace 里，各自按各自的 SPLITS 分布，**数字第一次可比**。

---

## 4. 兼容性与迁移汇总

| 改动 | 持久结构 | wire | 部署形态 |
|---|---|---|---|
| D1 fuse→`fs/{tenant}/{volume}/` | fuse KV key（破坏性）+ manager etcd `fs/next_inode` 改 per-volume | `MSG_ALLOC_INODES` +volume 字段（WIRE bump，MIN=MAX） | 停机 + **重灌（已拍板，盘点确认全部可再生）** 到 `fs/default/default/`；schema 版本戳 fail-loud 防旧 mount 读新盘/新 mount 读旧盘 |
| D2 注册表 | 无 | 无 | 文档 |
| D3 est_live | 无 | 无 | manager 二进制升级即可（全停全启惯例） |
| D4 split --at | 无 | SPLIT_PART req +1 字段（WIRE bump，MIN=MAX） | same-commit 停机部署（仓库惯例） |
| D5a presplit 模式外露 | 无 | 无 | entrypoint.sh / autumn-deploy 各一行 + 文档 |
| D6 authz 彻底启用（fs/、kvc/、mem/ 全部 enforce，已拍板） | etcd `tenantAccount/` 账户 + k8s Secret（签名私钥、各端 credential） | 无（RPC 全部已有） | manager 加 `--auth-signing-key-file` + `--auth-protected-prefix`×3（部署层 env 化）；fuse mount / PyO3 / autumnfs 补 credential 接线；按 mem/→kvc/→fs/ 分前缀灰度，凭据先行（§3.6） |

推荐实施顺序：**D3 →（校准观察）→ D6-mem（今天就能开）→ D1+D2 →
D6-fs/kvc → D4 → D5a**。D3 无破坏且 independently valuable；D6 按
§3.6 的灰度轴拆三步 —— `mem/` 无依赖先开，`kvc/` 等 Python credential
接线，`fs/` 硬依赖 D1（前缀先于保护）；D1 需要停机窗口，单独排
（数据盘点已完成，见 §3.1 —— 全部可再生，重灌路径已定）。

---

## 5. 未解决 / 需要用户拍板的问题

**已拍板（2026-07-16）— fuse 前缀字面值 = `fs/{tenant}/{volume}/`**：
与 kvc/、mem/ 的 tenant+instance 两层模式对称（tenant=归属，volume=
独立文件系统实例）；现网迁移用 `fs/default/default/`；每 key ~16 B
代价接受（猜错"要"只赔字节，猜错"不要"要二次停机重灌）。连带设计
（per-volume `next_inode` / `ROOT_INO=1` / superblock+rmtomb 布局、
`MSG_ALLOC_INODES` wire 变更）已并入 §3.1。

**已拍板（2026-07-16）— 线上 19 GB fuse 数据走 A（重灌），不写迁移
工具**：线上 `autumnfs ls` 全树盘点确认 fuse 里只有模型权重（19 GB
qwen32b 在用 + 5.5 GB 旧 qwen 7B 已无消费者），全部可从 hf-mirror
再生；旧 7B 重灌时不再灌，顺带回收 ≈16.5 GB EN 空间（RF3，EN 池仅
100 GiB）。细节与依据见 §3.1 迁移选项段。

**已拍板（2026-07-16）— authz 彻底启用，覆盖全部内置前缀**（用户：
"authz 彻底都加上吧"）：`fs/`、`kvc/`、`mem/` 全部登记 protected
prefix 并启用 enforcement。完整设计（机制盘点、缺口清单、灰度路径、
失败模式、与 D1 的强依赖）= §3.6 D6；实施顺序按 mem/→kvc/→fs/
分前缀灰度（§4）。**连带消解原待拍板项"client 裸 key 面要不要劝阻
保留前缀"** —— enforcement 之后三个内置前缀由机制保护，无 token 的
client 根本写不进去，"劝阻"问题不复存在（SDK 文档仍应说明这三个
前缀受保护）。

仍待拍板：

1. **D3 之后 SPLIT_SIZE_HARD/MERGE_SIZE_LOW 是否需要重标定**：新口径
   下 50 GiB/1 GiB 的物理含义变了；建议先 DryRun 观察一轮
   `policy-candidates` 再定，但阈值最终值需要运维判断。
2. **kvc 的 TTL 分区是否允许更激进的 merge/GC 策略档**（§3.5 的
   per-app 阈值分档做到什么粒度）—— 建议 reproduce-first，等 D3 上线
   后的真实 advisory 流再定。
3. **D6 的 token TTL 取值与凭据投递细节**（小时级 vs 天级；k8s
   Secret 的命名/挂载约定）—— 属实施细节，可在 D6-mem 灰度第一步时
   顺手定。

---

## 6. 对前期调查结论的核对（哪些对、哪些要修正）

- ✅ "policy 的 size 建立在看不见 VP 数据的指标上，大 value 负载下
  自动分裂/合并失效"——**成立**，机制细节见 §0/§2.2。
- ✅ kvc key 形态 `kvc/{tenant}/vllm/v1/{hash}/{layer}`、hexstring/fuse
  两个 presplit 的切法、部署硬编码 hexstring、fuse key 无前缀、线上
  key→partition 映射——**全部与代码/实测一致**。
- ✏️ "`size_bytes` 无 writer / 恒 0"（manager CLAUDE.md 原文）——
  **已过期**：F-PS-SIZE-BYTES-DEAD 于 2026-07-05 复活了该 gauge
  （`background.rs:309`），现在写入 LSM 常驻字节。所以线上 policy
  才会有 741 MB 这个非零读数。失明结论不变，但机理是"量错了东西"，
  不是"没在量"。
- ✏️ merge 候选谓词是**两侧各自 < 1 GiB**（`policy.rs:462-468`），
  不是 reason 字符串字面的 `size_sum < 1 GiB`。
- ✏️ `info` 的 45.4 GB 也**不是**"真实数据量"——含未 GC 的死字节与
  CoW 双计；真实活数据 = 45.4 GB − gc_debt − open_tail_dead（§0）。
- ✏️ "自动合并可能把扛着 19 GB 的 partition 合并掉"——advisory 层面
  成立；**自动执行**还需要三重人工条件（allow-mutations env + arm +
  merge 开关只在 aggressive preset 打开），今天不会自发发生。风险
  实际形态是"错误 advisory 诱导人工误操作 + 该分裂的永远不分裂"。
