# 内置应用 Key 命名空间 + 分裂规则

两条规则支撑本文：

1. **每一次写入都必须归属某个已注册的 namespace**，且必须**显式**归属 ——
   忘指定 = 报错（fail-loud），没有 default 兜底。
2. **per-app 的分裂规则住在 policy / 部署层，不下渗 partition 层** ——
   PS / manager 的 split / merge / 路由只看字节区间，永远不认识哪个前缀属于
   哪个应用。

---

## 1. Key 布局

### 1.1 统一 keyspace，按 namespace 组织

部署前提是**可信内网**（sglang/vLLM 推理集群，RoCE 内网）。这里没有互不信任
的租户，就是**一份统一资源**，只是不同组件有不同访问权限。所以键空间里
**没有 tenant 段**：

```
wire key = {ns}/[relative]
```

`{ns}` 是一个单路径段（`[a-z0-9._-]+`），Layer-A 认 key 的**第 1 段**。
「归属」这层由 namespace 本身承担，「权限」这层由 principal 的 grant 承担
（§4）。

| namespace | 相对 key 形态 | 位置 |
|---|---|---|
| `fs/` | 裸类型字节：`[0x01][ino BE]`（inode meta）、`[0x02][parent BE][name]`（dirent）、`[0x03][lane][ino BE][off BE]`（条带数据）、`[0x04][field]`（superblock，含 `stripe_geom` 与 `rmtomb/` unlink 墓碑） | `crates/fuse/src/key.rs` |
| `kvc/` | `{model}[_{fingerprint}][_{tp}][_pp{pp}]/…/{sha256-hex}/{layer}` | `python/autumn_kvcache/autumn_kvcache/_keys.py` |
| `mem/` | `{agent}/ep\|fact\|doc\|idx\|…/…`，动态组件百分号编码（`keys::q`） | `crates/autumn-memory/src/keys.rs` |
| 其余（`bench/`、`gallery/`、用户自建） | 任意字节 | `autumn-op namespace-create` |

内置三族（`fs` / `kvc` / `mem`）由首任 leader 的 `seed_builtin_namespaces`
CAS 预注册，且**不可删除**。`bench` / `gallery` 这类由部署层
（`cluster.sh`）显式 `namespace-create`。

### 1.2 路径段型 vs 二进制型 namespace

grant 的粒度取决于该 ns 的相对 key 形态，实现与运维时**必须区别对待**：

| | 路径段型 ns | 二进制型 ns |
|---|---|---|
| 例 | `mem/{agent}/…`、`kvc/{model}/…`、`gallery/…` | fuse `fs/[0x01][ino]…` |
| 相对 key 开头 | ASCII 段（`agent7/…`） | 定长二进制记录 |
| ns 内子前缀 grant | **有意义**（`mem/agent7/` 真能切开一片子空间） | **无意义**（`fs/models/` 里的 `/models/` 是 `[0x02]` 记录内的 dirent 名字，不是 key 前缀 → 匹配零个 key，连接期 `validate_credential_scope` 还会拒） |
| 隔离手段 | 授不同子前缀 | **只能整 ns 授（`fs/`）；要多棵互隔离的树 → 开多个 ns**（`fsA`/`fsB` 各自注册） |

**二进制型 ns 的取舍（明写）**：`fs/` 是一棵全局树、唯一的授权单位。任何持
`fs/` grant 的 writer 都能伪造 `fs/[0x04]rmtomb/<任意 ino>`（mount 时的 sweep
对墓碑指向的 inode **无条件删数据**）。**可信内网前提下接受**：一棵树内的多个
writer 本就该互信；要硬隔离就开多个 namespace。

### 1.3 client 侧绑定（`NamespaceBinding`）

`crates/client/src/lib.rs`。每个 `ClusterClient` 实例携带一个 binding，
「无归属的写」在类型层面表达不出来：

```rust
pub enum NamespaceBinding {
    Scoped { prefix: Vec<u8> },  // 永远以 `/` 结尾：`fs/`、`mem/agent7/`
    Raw,                         // admin / 跨 ns 工具
}

ClusterClient::connect(mgr, "fs")                    // scope 必填
ClusterClient::connect_with_credential(mgr, "mem/agent7", cred)
ClusterClient::connect_raw(mgr)                      // 显式 Raw
client.rescope("kvc") / client.raw()                 // 换作用域视图，共享连接池
```

- **Scoped 永远 Prepend**：`bind_key` = `prefix ++ user_key`，作用域**由构造锁定**
  而不是「事后校验」—— scoped client **拿不到**自己 keyspace 之外的东西。
  内置 key builder（fuse / memory / kvcache）吐的是**相对 key**（不含 ns 段），
  前缀归 binding。
- **range 三重钳制**：`bind_prefix` 拼前缀、cursor seed 不低于下界、
  `upper_cap()` 给上界 = 把 prefix 尾字节 `/`(0x2f) 换成后继 `0`(0x30)，
  所以 limit 驱动的扫描永远走不出本 ns；返回时 `strip()` 剥掉前缀，
  调用方看到的还是自己的相对 key。
- **`Raw` 只绕开 client 侧的钳制，不绕开 PS 侧的检查** —— Layer-A / Layer-B
  在服务端照常强制。逃生舱只解「工具要操作多个 namespace」，不解「绕过保护」。
- **scope 段校验**：`is_valid_scope_segment` 在 connect 时强制
  `[a-z0-9._-]+` 且非空 —— 否则一个含 `/` 或空的段能伪造出嵌套 / 别名作用域
  （`acme/sub` + `mem` → `acme/sub/mem/`，或 `//mem`）。

**哪一段归 binding、哪一段归 builder，每个 app 声明一次**（写进各自的
CLAUDE.md）。反例：grant = `mem/agent7/` 而 memory builder 又自吐 `agent7/…`，
会拼出 `mem/agent7/agent7/…` 的双段 key。

### 1.4 大值条带的 chunk key

`make_chunk_key(user_key, idx)` = `CHUNK_KEY_PREFIX ++ len(user_key) ++
user_key ++ idx`，它产出的是一个**相对 key**，再经 binding 前缀化 →
`{ns}/ ++ \xff\xfe… ++ user_key ++ idx`。这样条带 body 落在本 ns 区间内：
Layer-A 放行、Layer-B 覆盖、presplit 区间涵盖。代价是本 ns 内的 range 扫描会
看到自己的 chunk key（`\xff\xfe` 排在本 ns 尾部），需要 caller 侧过滤。

---

## 2. namespace 注册表

### 2.1 数据模型

etcd `namespace/<name>` → rkyv 的 `MgrNamespace`（`crates/rpc/src/manager_rpc.rs`）：

| 字段 | 含义 |
|---|---|
| `name` | 单路径段，`[a-z0-9._-]+` |
| `prefix` | `name + "/"` —— Layer-A / authz / presplit 匹配的字节前缀 |
| `owner_tenant: Option<String>` | `Some(_)` 标记该 ns **protected**，其前缀被桥接进 `GetAuthzConfigResp.protected_prefixes`。内置三族种子值是 `None`（只登记、不标 owner） |
| `presplit: Vec<Vec<u8>>` | 运维声明过的切点（§6.3 的 sacred boundary） |
| `created_at` | 诊断用 |

manager 侧内存影子 `namespaces: HashMap<String, MgrNamespace>`，leader 上任时
从 `get_prefix("namespace/")` replay，**解码失败 fail-loud**。

### 2.2 生命周期

全部 leader-only + admin-token gated（`admin_auth_design.md` §4.2），etcd-first，
并由 `namespace_admin_lock` 串行化整个临界区（存在性 + 不相交校验 → etcd 写 →
内存 apply），防两个并发 create 都通过校验后以冲突顺序提交。

```
autumn-op namespace-create --name <NS> [--tenant <T>] [--presplit <hex,…>] --admin-token…
autumn-op namespace-delete --name <NS> [--force] --admin-token…
autumn-op namespace-list [--json]        # 只读，不需要 admin token
```

- **保留名**：`fs` / `kvc` / `mem` / `default` 一律拒绝创建
  （`RESERVED_NAMESPACE_NAMES`）。`default` 被保留纯为防混淆 —— 它是约定俗成的
  **tenant 名**，不是 namespace。
- **前缀不相交规则**：新名 `X` 使 `X/` 与任何既有 namespace 前缀互为
  `starts_with` 时拒绝创建，保证所有 namespace 区间两两不交 —— Layer-A / 授权 /
  presplit 的前缀匹配才无歧义。这也天然免疫「捕获脚枪」：`default/abc/…`
  永远不会被后来创建的 `abc/` 静默易主。
- **删除**：内置三族拒绝删除。非空检查由 `autumn-op` **客户端侧**做
  （manager 没有 KV 数据面 client），`--force` 跳过；handler 只摘 etcd 注册表行。

### 2.3 传播窗口

PS 通过 5 s 的 `MSG_GET_AUTHZ_CONFIG` poll 拿到全量 namespace 前缀清单
（`GetAuthzConfigResp.namespaces`）。所以 `namespace-create` 之后最长 5 s 内，
往新 ns 的写可能被 Layer-A 拒 —— 这是已知且接受的失败模式（create 低频）。

---

## 3. 两层检查

### 3.1 Layer-A —— 存在性（不需要 token）

`partition_server::authz::check_layer_a`。**put 类**（`MSG_PUT` /
`MSG_PUT_BULK` / `MSG_BATCH_PUT`）的 key **必须**落在某个已注册 namespace 的
前缀下，否则拒 `StatusCode::NamespaceUnknown`(=8)。

- **纯字节 `starts_with` 已注册清单，免 token，匿名连接也受检**。
- **开关独立于 signing key**：`layer_a_enabled = !namespaces.is_empty()`，
  一个没开 authz 的 dev 集群照样有 Layer-A；一个还没 poll 到配置的冷 PS
  则不误拒。
- **只管写、不管删/读**：delete 制造不出「无归属数据」（Layer-A 的目的是防
  污染），读 / range 的隔离归 Layer-B。这个收窄顺带让清扫工具可以直接批删
  legacy 裸 key，无需给 Layer-A 开洞。
- `NamespaceUnknown` 在 client 侧是 **terminal** 错误（刷新路由创造不出一个
  namespace），与 `PermissionDenied` 同类，不重试。

### 3.2 Layer-B —— 授权（需要 capability token）

设计见 `data_plane_authz_design.md`。要点：manager 当 KDC 用 Ed25519 私钥签短
TTL token，PS 持公钥本地验签并按 token 的 `allowed_prefixes` 前缀 gate；
**authz 一旦开启就是 protect-everything**，每个 key、每个 range 都要 token。

### 3.3 为什么分两层

合并成「namespace 必然带 token」会把 dev / 测试 UX 杀死（每个 cluster.sh 单测
集群都得先造密钥、发凭据），而存在性检查不需要任何密钥基建就成立。
**namespace 是身份概念，authz 是强制开关**：dev 集群可以有 namespace 而没有
token；prod 集群两层全开。

---

## 4. principal 与 grant

- **principal** 是唯一的身份：
  `autumn-op principal-create <name> --grant <prefix> [--grant …]` →
  manager 存 `{name, credential_hash, allowed_prefixes}`，返回长期凭据。
  `autumn-op principal-list` 只读列举（凭据哈希不出网）。
- grant 通常就是一个 ns（`fs/`）；路径段型 ns 可以再细分到子前缀
  （`mem/agent7/`），二进制型 ns 不行（§1.2）。
- **最小权限是默认，不建 all-ns 万能钥匙**：一把 master 会让 Layer-B 恒真，
  一个有 bug 的 kvcache 进程就能删 fs 数据。turnkey 部署建**每族一把**：

```bash
autumn-op principal-create fs   --grant fs/    # → fs.cred   （fuse / autumnfs）
autumn-op principal-create kvc  --grant kvc/   # → kvc.cred  （kvcache loader）
autumn-op principal-create mem  --grant mem/   # → mem.cred  （codebase-memory）
```

- **数据面只出示凭据**，没有 `--tenant` / `--principal` flag：principal 名字随
  凭据文件携带（两行格式 `<name>\n<hex>`），作用域来自 `--namespace <ns>`
  （`autumn-client` 的 `--namespace` / `--scope`）或 app 自带的 ns。
- **Layer-A 与 grant 正交**：一个 grant 再宽，写也必须落在**已注册 ns** 下。
  「全权限」不等于能往未注册前缀乱写。

---

## 5. Partition 大小的三个口径

同一个 partition 会同时有三个互相矛盾的数字。**三个都是真的，只是量的不是
同一个东西**：

| 口径 | 定义 | 代码位置 |
|---|---|---|
| ① `PartitionLoad.size_bytes` | **LSM 常驻字节** = Σ SST `len` + active/imm memtable 字节。> 4 KiB 的 value 走 ValuePointer，SST/memtable 里只有 ~24–40 B 的指针，**value 本体（在 log_stream）完全不计** | `partition-server/src/lib.rs::lsm_resident_bytes`；writer 在 `background.rs`（30 s 刷新） |
| ② `autumn-op info` 的每 partition `live_size` | 该 partition 三条 stream（log/row/meta）**去重后所有 extent 的字节和**：sealed 取 `sealed_length`，open tail 现场 probe EN。**包含尚未 GC 的死字节**与 CoW split 后与兄弟共享的 extent（双计） | `autumn_op/main.rs::run_info` |
| ③ overview 的 `live_size` | ② 的周期性 rollup：manager sealed 和 + PS 心跳上报的 `open_tail_bytes` | manager `compute_cluster_overview_resp` |

**真实活数据量介于 ① 和 ② 之间**。对 fuse / kvcache 这类几乎全部字节都在
ValuePointer 里的负载，① 是一个缩小几十倍的影子 —— 直接拿 ① 做 split/merge
判据会让「该分裂的永远等不到分裂、扛着几十 GB 的分区被判成 merge 候选」。

policy 因此消费的是 `manager::policy::effective_size_bytes`：

```
est_live = Σ sealed_length(三条 stream, 去重)   # manager 状态
         + open_tail_bytes                      # PS 上报
         − gc_debt_bytes                        # PS 上报（sealed 死字节）
         − open_tail_dead_bytes                 # PS 上报（open tail 死字节）

effective = max(size_bytes, est_live)
```

- 纯 manager 侧计算，**零 wire 变更、零 PS 热路径成本**（四个分量都已在船上）。
- 算术全程 saturating：三个 PS gauge 各有各的刷新节奏，刚 punch 完的 extent 会
  短暂让 debt > sealed + open_tail，钳到 0 而不是回绕，退化值经 `max` 落回 ①。
- **两侧都取 max** 比「merge 用 est_live、split 用 max」更保守：对 split 是
  `old ∨ new`（只增加候选），对 merge 是 `old ∧ new`（只减少候选）——
  任一口径说「大」都能否决一次 merge、成就一次 split。
- 已知误差（方向安全，全是高估）：CoW 共享 extent 在两个 child 各计一次；
  row_stream 的 compact 垃圾不被扣除；`open_tail_bytes` 在 PS 首次 probe 前是 0
  （低估侧由 `max` 兜住）。
- **已知缺口**：`sealed_sum` 是单次当前快照，被套用到滑动窗口的每一个 bucket，
  所以 size 维度**不参与**「N 个 bucket 全触发」的去抖 —— sealed 的一次阶跃会
  在一个 tick 内就够到 size 触发器。今天可接受（auto-exec 默认关，只影响
  `policy-candidates` 的 DryRun advisory；sealed 是慢变量；split/merge 都有
  cooldown）。**arm 基于 size 的 auto-exec 之前必须先修**：要么把 sealed 挪出
  热路径做 per-bucket 历史，要么把 size 维度整个移出去抖。

阈值（`crates/manager/src/policy.rs`）：`SPLIT_SIZE_HARD = 50 GiB`、
`MERGE_SIZE_LOW = 1 GiB`、`SPLIT_QPS_HIGH = 15K`、`MERGE_QPS_LOW = 1.5K`、
`HOT_COLD_MIN_HOT_SIZE_BYTES = SPLIT_SIZE_HARD / 2 = 25 GiB`。

---

## 6. 分裂

### 6.1 运行时 split：app-agnostic

`handle_split_part`（`partition-server/src/rpc_handlers.rs`）：flush memtable →
定切点 → `multi_modify_split` CoW 复制三条 stream。两种定点方式：

- **median（`at_key = None`）**：取 LSM 内**去重 user key 的中位数**。要求
  ≥ 2 个去重 key 且 `has_overlap == 0`。
- **显式（`at_key = Some(k)`）**：PS 校验 `k` 严格落在 partition 的
  `(start_key, end_key)` 内后**逐字节采用**，跳过 median 选点与 `>= 2 keys` 门
  —— 所以**空分区也能切**，这正是事后 presplit 需要的。

`mid_key` 是**完整的任意字节串 user key**，没有「只能切到第 K 字节」的限制：
切点想多深就多深。sha256-hex 均匀 → 按 key 数量的中位数 ≈ 按 hash 空间的中位数；
`[0x03][lane][ino][off]` 的中位数可以落在一个文件内部 —— **运行时 split 能切开
单个大文件，这是任何 presplit 都做不到的**。

**按字节量取中位数（不做）**：median-by-key-count 对 fuse 是近似字节均衡的
（extent ≤ 8 MiB）；修它要在 SST 侧按 key 累计 VP 字节直方图，而在
`effective_size_bytes` + 显式切点之后没有剩余的真实场景。

CLI（用户面只讲 namespace，不手拼前缀字节）：

```
autumn-op split <PARTID>                                        # median
autumn-op split <PARTID> --namespace <NS> [--tenant <T>] [--at <SUFFIX> | --at-hex <HEX>]
autumn-op split <PARTID> --at-raw-hex <HEX>                     # admin 逃生舱
```

`SplitPoint::resolve_at_key` 把 CLI 意图降到裸 wire key
= `{ns}/` (+ `{tenant}/` 作为 ns 内子段，`fs` 场景为空) `++ suffix`；
**空 suffix = 恰好切在前缀边界上**。wire 层（`MSG_SPLIT_PART`）**只认裸字节
key**，partition 层继续不认识 namespace。

### 6.2 presplit：per-namespace，bootstrap 之后做

`bootstrap --presplit` **已退役**（它在 bootstrap 时刻按绝对切点切，而那时
真实 key 还不存在，切点落在无人区，等于什么都没做；现在传它会 fail-loud 报错
并给出替代命令）。presplit 的锚点是**namespace + 它自己的维度**，在
keyspace 还空的时候执行：

```
autumn-op presplit --namespace fs     --lanes <N> [--parts <P>]      # 条带 lane 边界
autumn-op presplit --namespace fs     --fs-inos <i,j,…> | --count <N> # 按 inode 切（一文件一分区）
autumn-op presplit --namespace kvc    --count <N> --hash-prefix '<model>/vllm/v1/'
autumn-op presplit --namespace mem    --agents <a,b,…>
autumn-op presplit --namespace <其它> --count <N>                     # 均分 hex 空间
```

`--tenant <T>` 是**可选**的 ns 内子段（`mem` / `kvc` 用得上，`fs` 没有 →
默认空串 → 切在 `{ns}/` 上）；给了就必须是单个小写路径段，与 `split` 同一守卫。

`presplit_suffixes(rule)` 产出**相对**切点后缀，`cmd_presplit` 拼上
`{ns}/`(+`{tenant}/`) 得到绝对切点，逐点调 §6.1 的显式 split。
规则见 `PresplitRule`：`FsLanes` / `Fs` / `Kvc` / `Mem` / `Hex`。

`kvc` 的 `--hash-prefix` **没有默认值**：content-hash 不在一个固定段之下
（vLLM 存 `kvc/{model}/vllm/v1/{hash}/{layer}`，hash 坐在 per-model 前缀之下），
硬编码一个 `vllm/` 会切在离真实 key 十万八千里的地方并**静默**让 presplit 失效。
必须先看一条真实 key 再传精确的相对前缀。

`FsLanes` 的两个参数是两个独立决策：**lanes = key 布局**（一个文件被切成几条
stripe），**parts = 真的建几个 partition**。切点是 `[0x03][k · lanes/parts]`，
所以每个 partition 拥有**恰好 `lanes/parts` 条连续 lane**；`parts` 必须**整除**
`lanes`，否则各分区拥有的 lane 数不均、每个文件都会落得偏斜，直接报错而不是
四舍五入。

`--lanes N` **在切边界的同一条命令里声明 stripe 几何**，且**先声明后切**：
两种失败顺序不对称 —— 先声明后切失败 = 几何声称 N 条 lane 而边界还没建，
新文件仍按 lane 排序、只是暂时没有并行度，重跑即痊愈；先切后声明失败 =
边界建好却没有声明，文件永远静默不条带化。

### 6.3 sacred boundary：运维声明过的切点不可被合并掉

`autumn-op presplit` 带 admin token 时会把实际切点通过
`MSG_NAMESPACE_SET_PRESPLIT` 记回 `MgrNamespace.presplit`。manager 据此提供三个
**故意 generic** 的谓词（manager 永远不知道什么叫「lane」）：

- `sacred_boundary_owner(key)` —— 这个 key 是不是某个 ns 声明过的边界。
  merge 不得跨越它（`--force` 才能穿）。
- `declared_split_point_within(part_id)` —— 一次 split 应当吸附到哪个
  **已声明但还没切开**的点：取落在该分区区间内、**最靠中间**的那个声明点，
  让分裂对半切 lane 跨度而不是从一端削掉一条。
- `sacred_boundaries()` —— 全集群的声明边界集合，policy 引擎用它在 merge 候选
  被 advertise 之前就跳过。

「运维声明过的边界是神圣的」这一条规则，同时覆盖 fs lane 边界、kvc hash 桶和
mem agent 切点。

### 6.4 presplit 与运行时 split 是互补的

- presplit 摊的是「文件之间 / 桶之间」，它做不到的：kvc 的模型指纹在部署时才
  出现、fs 的单个大文件内部。
- 运行时 split 是安全网本体，它靠 `effective_size_bytes`（§5）才对大 value
  负载睁眼。
- 二者都以 §5 的口径为前提 —— 否则一个糟糕的初始切分永远等不到纠正。

---

## 7. 分层原则

**namespace 是 SDK + manager 元数据层的概念，绝不下渗到 partition / stream
层。** PS / manager 的 split / merge / 路由继续只看字节区间；不存在
per-namespace 的 META 表或独立 region 空间（HBase 类比到 API 为止，存储层仍是
单一全局 range 空间）。

因此**不做**「partition 层运行时识别 key 属于哪个应用、按应用声明的规则分裂」：

- 机制上不需要：split 选点已经是 data-driven 的（median 跟着真实 key 分布走），
  app 身份不改变「往哪切」的答案；
- 架构上有约束：manager 是纯 mechanism，策略在 controller
  （`auto_policy.rs`），往 PS 里塞 app 规则是开倒车；
- 演化上更脆：partition 层认识 `kvc/` 的那天起，`_keys.py` 改版就要同步改
  Rust —— 跨语言耦合换不来任何 median + 显式切点给不了的能力。

policy / controller 层（以及人）确实需要「这个 partition 在承载哪个应用」来选
阈值、解释 advisory（`kvc/` 这种 TTL 型缓存或许允许更激进的 merge，`fs/` 应该
更保守）。这个识别只需要拿 partition 的 `[start_key, end_key)` 与 namespace
注册表求交 —— manager 侧十几行的纯函数。**这才是两条主张真正的依赖关系：
prefix 是 policy 层 per-app 规则的前提，而不是 partition 层 split 代码的前提。**

如果哪天需要真正的 per-namespace placement（独立副本策略 / 独立 EC 档），
那是另一个系统设计，不要从这里滑进去。
