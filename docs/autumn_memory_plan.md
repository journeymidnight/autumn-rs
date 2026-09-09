# autumn-memory 设计

**关联文档**: [autumn_kvcache_plan.md](./autumn_kvcache_plan.md) ·
[data_plane_authz_design.md](./data_plane_authz_design.md)
**关联代码**: `crates/autumn-memory/`（`keys.rs` / `recall.rs` / `vector.rs` /
`graph.rs` / `embed.rs`）· `examples/memory-mcp`（Rust 消费者 + MCP）
**关联记忆**: [[project_agent_memory_backend_fit]] · [[project_three_interfaces]] ·
[[feedback_no_parallel_data_plane]] · [[feedback_client_side_complexity_first]]

---

## 0. TL;DR(一页纸结论)

autumn-rs 做 **AI agent memory 的耐久分布式后端**,形态 = **纯客户端库(无 daemon)**,
各 agent 内嵌、直连 autumn。

- **接入面**(决定能连哪些 agent)与 **检索后端**(决定怎么找、多大多准)是**两条独立的轴**,别混。
- 接入:**原生适配器**(自动生命周期)+ **MCP(stdio)**(通用触达)。
- 检索:**两条腿都是 posting-on-KV** —— 向量 = SPFresh 式 **IVF-on-KV**,词法 =
  **BM25-on-KV**;hybrid = 两腿 + RRF 客户端融合。另有 **graph**(adjacency-on-KV)。
- **embedding** 默认走远程端点(复用已有 sglang);另备一个不需要模型服务的内置
  CPU embedder(§11)。
- **共享 memory** 由 autumn 这层提供(多 agent 读写同 key),**不是靠 daemon**;
  企业治理用**可选的无状态 gateway**。
- **默认 no-daemon**;只在 measured 需要热内存 HNSW 的特定 namespace 才局部升级 sidecar。
- 定位:≈「turbopuffer 的索引哲学 + sqlite-vec/FTS5 的无-daemon 接入 + FoundationDB
  的 layer 基质」,但**没有完全对应的单品**。

---

## 1. 目标

让运行在 vLLM/SGLang 上的 agent 把长期记忆(情景/语义/过程)落到 autumn partition 层。

**核心论点**:agent memory 在存储层分两半——**system-of-record**(autumn 强契合)+
**检索智能**(向量 ANN/倒排/图,主流系统本就外置成「指回 KV 的索引」,正确分层,非缺陷)。
autumn 不去「变成」Qdrant/turbopuffer(并行数据面 + 匹配不完的工程)。

**闭环卖点**:同一批 vLLM/SGLang 推理机已把 autumn 当 KV-cache 的 L3
([[project_autumn_kvcache_architecture]])。再接长期记忆 = **一套 autumn 同时装
「激活记忆(KV cache)」+「明文记忆(episodic/semantic)」**(MemOS/MemCube「统一基质」
方向)。本地嵌入式方案(sqlite-vec / 本地 tantivy)给不了的:分布式持久、多租户自动
split、跨集群共享、原生 TTL。

## 2. 部署假设(**这是设计前提,不要绕开**)

- **autumn ↔ 一个 sglang/vLLM**(配对部署,可同机可不同机);autumn partition 集群独立部署。
- **一个 sglang 之上挂多个 agent**(多用户 / 多框架 / 多会话)→ **多客户端**。
- **多租户真实规模形状**:海量「各自很小」的 per-agent 命名空间(典型单 agent
  10^3–10^5 条记忆),少数中规模(10^5–10^7)。autumn 的 split/merge 按 key range 自动把
  热 agent 切到独立核——正打这个形状。

直接推论:
- **多 agent = 多客户端**,但**不需要共享 daemon**:共享数据由 autumn 这层提供(§9);
  各 agent 直连。
- embedding 是个**推理服务**,但它是 agent 推理**本来就有的** sglang,不是新 memory
  daemon(§11)。
- 区别于 kvcache:kvcache 是「一节点一个 sglang = 单客户端」,memory 是「一 sglang 多
  agent = 多客户端」——但结论仍是无 daemon(§10)。

## 3. 架构:纯客户端库 + 可选无状态 gateway

核心 = Rust crate `crates/autumn-memory`,直接基于 `autumn-client::ClusterClient`。
`MemoryStore` 是 `!Send`(单线程 compio,与整个 client 面一致),async 方法跑在 compio
runtime 上。key schema 与召回打分(BM25 / IVF / RRF)都在 Rust 里,一处实现。

消费者两类:
- **Rust 直接用 crate**(`examples/memory-mcp` = web UI + MCP server 一个二进制)。
- **非 Rust 适配器按 `mem/` key schema 复刻**——在任意语言的客户端上按 §6 的 key
  schema 重建情景/事实/词法搜索即可。**契约是 key schema(§6),不是某个语言绑定**
  ——这是 adapter 能长在核心库之外的原因。

```
┌─ 多个 agent(同一 sglang 之上)─────────────────────────────┐
│  agent 进程 = 框架 + 内嵌适配器(薄)                          │
│   ├─ Hermes MemoryProvider / 自研 loop                        │
│   └─ autumn-memory 核心库(命名空间 / 情景 / 事实 / 检索)     │
│        ├─ 要向量? → RPC 调 sglang 的 embed 端点拿向量         │
│        └─ 读写 posting / 记录 → ClusterClient 直连 autumn      │
└───────────────┬──────────────────────────────┬───────────────┘
                │                              │
         (通用触达,可选)                 (无中间服务)
   stdio MCP 子进程 / 可选无状态 gateway        │
   = 同一核心库的 server 外壳(治理用)          │
                └──────────────┬───────────────┘
                               │ partition RPC (UCX/TCP)
                ┌──────────────▼───────────────┐
                │  autumn partition layer       │
                │  (唯一持久 + 唯一共享层)       │
                │  → stream layer                │
                └───────────────────────────────┘
```

**两条独立的轴(整个对接策略的核心认知):**

| 轴 | 决定 | 有哪些 | 触发方式 / 局限 |
|---|---|---|---|
| **A 接入面** | 能**连**哪些 agent | MCP / REST / 原生适配器 | MCP=通用但 model-invoked(opt-in);原生=自动生命周期但每框架一份 |
| **B 检索后端** | prefetch/search **怎么找** | 暴力 / IVF-on-KV / BM25-on-KV / sidecar | 藏在 A 接口后面,与接入无关,可后插不动接口 |

**MCP+REST 买广度,原生适配器买生命周期;检索后端是 A 之下可独立升级的内部实现。**

## 4. 不做哪些事(design rationale)

| 否定项 | 否决理由 |
|---|---|
| **默认起一个共享 memory daemon** | agent 召回 LLM-bound(低 QPS、延迟被秒级 LLM 淹没),daemon 热缓存白送;autumn 又非慢对象存储,缓存层税收不回(§10) |
| autumn 内置向量 ANN / 倒排引擎(变 Qdrant/tantivy) | 并行数据面 [[feedback_no_parallel_data_plane]] + 匹配不完;检索 = posting-on-KV(中小)或 sidecar(重) |
| **turbopuffer 当 autumn 的检索层 / 后端** | turbopuffer 闭源托管 + 建在 S3 + 无换后端接口,autumn 非 S3 兼容→插不进;并列用 = daemon + 并行数据面 + 第三方托管 + autumn 被边缘化(目标没了) |
| 客户端进程内加载**神经** embedding 模型 | N agent 各加载 N 份炸显存;走远程端点或零 GPU 的静态表 embedder(§11) |
| 依赖引擎 MVCC 做「时点/历史」记忆 | 客户端读不到历史、旧版 compaction 丢弃(§13);双时态在应用层 key schema 显式做 |
| 服务端二级索引 / 谓词扫描 / 聚合 / hybrid 融合 | partition 只保证 key 有序;过滤/打分/RRF 在客户端做 |
| 等「记忆专用协议标准」 | 不存在:A2A/AGNTCY=agent-to-agent 非记忆;OpenMemory MCP 只是跑在 MCP 上的产品;**MCP=事实唯一通用答案** |

## 5. 记忆分类 → 存储操作映射

| 记忆类型 | 主存储操作 | autumn 契合 |
|---|---|---|
| 工作/短期(激活态) | point get/put | ✅ 已有 = autumn-kvcache |
| 情景 episodic | append + 时间序前缀扫 | ✅ 极契合(LSM 本行) |
| 语义 semantic | 向量ANN 或 point-get+前缀list;supersede | ✅ 存储契合 / 检索见 §7 |
| 过程 procedural | point-get by name | ✅ 普通命名 KV |
| 图 | 邻接表前缀扫 | ✅ `graph.rs`(§6) |
| 双时态 | 有效期查询 | ❌ 不原生(有效期编 key,§13) |

## 6. Key 格式

wire key = `mem/{tenant}/{agent}/…`,固定 `mem/` namespace 与 `kvc/`(kvcache)/
`fs/`(文件系统)分开。

**`keys.rs` emit 的是 RELATIVE key(从 `{agent}/` 起)**;`MemoryStore::connect` 用
`ClusterClient::connect(mgr, "mem/{tenant}")` 绑定 scope,由 client 负责 prepend
`mem/{tenant}/` 并把返回的 range key 剥回。`{tenant}` 是 memory app 自己管的
namespace 内子前缀,不是 SDK 的 tenant 概念。

```text
情景日志:   mem/{tenant}/{agent}/ep/{session}/{12B 后缀}   → 事件 blob
事实 KV:    mem/{tenant}/{agent}/fact/{namespace}/{key}    → 事实 blob
共享数据:   mem/{tenant}/shared/{namespace}/{key}          → 跨 agent 共享(§9)

词法(BM25):
  权威文档:  mem/{tenant}/{agent}/doc/{doc_id}             → IndexedDoc{doc_len,terms→tf,text,meta}
  倒排 posting: mem/{tenant}/{agent}/idx/{term}/{doc_id}   → 空值(存在性 marker)
  语料统计:  mem/{tenant}/{agent}/meta/stats               → {n_docs, sum_doc_len} 16B LE

向量(IVF):
  posting:   mem/{tenant}/{agent}/ivf/{centroid BE4}{vec_id} → 向量
  质心清单:  mem/{tenant}/{agent}/ivf_meta/centroids        → {epoch,dim,n,f32…}
  反向指针:  mem/{tenant}/{agent}/ivf_meta/vptr/{vec_id}    → centroid BE4

图(adjacency):
  节点:      mem/{tenant}/{agent}/node/{id}                 → 权威节点记录
  按 kind 索引: mem/{tenant}/{agent}/nidx/{kind}/{id}       → marker
  正向边:    mem/{tenant}/{agent}/edge/{src}/{type}/{dst}   → 权威,带 attrs
  反向边索引: mem/{tenant}/{agent}/redge/{dst}/{type}/{src} → marker/hint
```

约定:

- 结构分隔符是字面 `/`;每个**动态**组件(tenant/agent/session/namespace/key/term/
  doc_id/节点 id/边类型)都 **percent-encode**(`q`/`unq`),组件里的 `/` 伪造不出分隔符、
  也钻不进别的 agent 前缀(单测 `agent_prefix_isolation` 钉住)。
- 情景 `{后缀}` = `BE(u64::MAX - ts_ns) ++ BE(u32::MAX - counter)` → 升序 range 扫描
  即 **newest-first**;per-store counter 打破同纳秒并列。
- IVF 桶 id 是**定长 4 字节 BE**,所以 `ivf/{4B}{vec_id}` 按 offset 解析(那 4 字节可能
  含 `0x2F`,绝不当分隔符)。`ivf_meta/` 与 `ivf/` 不互为前缀(`ivf_` ≠ `ivf/`),所以
  vptr / 质心对桶扫描不可见。
- 每 key 可带 **TTL**(`expires_at`,§13)。
- **双时态**用 immutable `fact/{entity}/{valid_from}/{txid}`,「关闭旧有效期」不原地覆盖
  而是 append-only correction/interval record;一个 bitemporal update 用
  `commit_marker/txid` 标完整提交;重建历史只读 committed txid 并按 txid 去重
  (**不靠引擎 MVCC**;详见 §8.5)。

## 7. 检索层:两条腿,都 posting-on-KV

**先厘清三个易混概念(不在一个层面,非三选一):**
- **FTS** = 全文检索**能力**(analyzer 分词 + 倒排索引 + 打分函数)。
- **BM25** = FTS 内的**打分函数**(词法腿)。
- **SPFresh** = **向量 ANN 索引**(centroid/IVF/SPANN 族 + LIRE 增量更新,turbopuffer
  向量用的;语义腿)。
- 关系:**BM25 ⊂ FTS = 词法腿;SPFresh = 向量腿;hybrid = 两腿 + RRF**。

**核心洞察:倒排索引 与 IVF 向量索引在有序 KV 上同形状 = 「posting-list 按前缀存 +
客户端打分」。** turbopuffer 生产把 posting 块化存做 BM25(MAXSCORE);Faiss
`rocksdb_ivf` 把 centroid→posting 存 KV;Bleve 在 RocksDB 做 FTS。两者唯一不白送:
决定「扫哪些 posting」的小结构(IVF=质心表 / HNSW=导航图),客户端建、可重建。

| 腿 | 形态 | 不用 |
|---|---|---|
| **词法** | **BM25-on-KV**:`idx/{term}/{doc}` 存在性 marker → keys-only 扫出候选 → 取 `doc/{id}` → Okapi BM25(k1=1.2 b=0.75)客户端算。`df` ≈ posting 计数(陈旧 orphan 的高估有界,idf 稳健) | — |
| **向量** | **SPFresh 式 IVF-on-KV**:`ivf/{centroid}{vec}`,质心客户端缓存,`nearest_centroids` 选 nprobe 个桶扫 + cosine | HNSW/DiskANN(图非 KV 友好) |
| **融合** | **hybrid = 两腿 + RRF**(`recall::rrf_fuse`,ordinal 融合,客户端三行) | 服务端融合 |
| **图** | adjacency-on-KV:每次遍历都是前缀 range-scan(`out_edges` 扫 `edge/{src}/`,`in_edges` 扫 `redge/{dst}/`,`bfs` 串起来) | 图数据库 |

分词器:小写 + 最大字母数字连续段 + 小停用词表 + 保守复数折叠;**CJK**(Han / kana /
Hangul)按码点切 unigram,这样单字查询能命中(中文单字常是整词)且不会跟拉丁文的
length-norm 打架。bigram 精度是后续细化。值是 opaque bytes——编码由调用方定,核心库
不强加。

**按规模选(每查询成本独立于 fleet 大小,因 key 前缀隔离):**

| 单 agent 规模 | 每查询 |
|---|---|
| ≤ ~10^5(常见) | 几 ms,精确 |
| 10^5–10^7 / 多中规模 agent | 几十 ms,~90% recall(只 probe 几桶) |
| > 10^7 单 agent | IVF-on-KV(LIRE)或 sidecar |

**SPFresh 是借「设计」非跑其代码**(原版 SPDK/裸 NVMe);autumn 借其逻辑模型
(centroid posting list + LIRE),铺在 autumn KV 上。

## 8. 多租户容量模型 + IVF 共享质心

**结构优势:每 agent 检索成本被 key 前缀隔离**,agent A 查询只扫
`mem/{tenant}/A/...`,与 fleet 其它 agent 数无关(单机 sqlite-vec 做不到)。两条独立的轴:

**轴一 单 agent 检索**:见 §7。对「多中规模 agent」**必须 IVF-on-KV**(posting 在
autumn 磁盘,每查询只读探中几桶);**全局共享粗量化器**(一套质心)→ 质心 RAM 一份
(几 MB),**per-agent RAM≈0,agent 数无上限**。暴力/sidecar-HNSW 要 per-agent RAM,
撑不住多中规模 agent。

**轴二 聚合容量** = Σ 各 agent,横向扩。单 agent 存储(float32,每条=维度×4B;int8 ÷4,
binary ÷32):10^5≈0.3–1.2GB,10^6≈3–12GB,10^7≈30–120GB(加正文 ~0.5–2KB/条)。实用
甜点 **10^6·768维·int8≈~1.8GB/agent**。

| agent 数(各 ~2GB) | 原始 | ×RF3 | 约 EN(~10TB/EN) |
|---|---|---|---|
| 1,000 | 2 TB | 6 TB | 几台 |
| 10,000 | 20 TB | 60 TB | ~6–8 |
| 100,000 | 200 TB | 600 TB | ~60–80 |

QPS:单 partition ~30K [[project_partition_qps_ceiling]],扩=加 partition
[[feedback_no_multiworker_per_partition]],热 agent 自动 split
[[feedback_auto_split_before_merge]];单个超大 agent 的连续 range 也被 split 切到多
partition。

## 8.5 索引一致性契约(correctness contract)

一次逻辑 memory 写 = 主记录 + 多 IVF posting + 多 BM25 posting + 统计 + TTL/删除
marker = **多 key、常跨分区、无事务、无快照、无 CAS** 的派生状态。autumn 保证单 key
字节级 durable,但**不保证这些 key 之间逻辑一致**。风险非字节损坏,而是**索引↔主记录
持久逻辑不一致** → 漏召回 / 错召回 / 过期可见 / 并发丢写 / 旧质心污染。

契约:

1. **posting 只是候选提示,权威记录才是正确性边界。** 写序 = 先写 posting、**最后**写
   权威记录(那就是提交点);查询必反查权威记录并以它为准。
   - 词法腿:`doc/{id}` 的当前 `terms` map 是判据——re-index 删掉的词,其陈旧 posting
     自然不在 map 里,查询时被忽略。**因此词法腿不需要 generation 戳**。
   - 图:`edge/*` 是权威,`redge/*` 是读时对着正向边验证的反向索引提示
     (`add_edge` 先写 `redge` 再写 `edge`;`delete_edge` 先删 `edge` 再删 `redge`)。
2. **每个 id 在 IVF 里只有一份 posting。** `index_vector` 移桶时反手回收旧桶,并同步
   `ivf_meta/vptr/{id}`;`delete_vector` 靠 vptr 在 O(1) 定位并回收。
   `train_centroids` 重新分桶时**不检查主记录是否存在**,所以它从不回收已删向量——
   **vptr 是唯一回收者**。解析器仍应丢掉 `doc/{id}` 已消失的命中(MCP 的 `_resolve`
   就这么做)以覆盖 in-flight/过期竞态。
3. **`meta/stats` 的多写者 RMW 竞态被容忍,不被序列化。** 危害低(只歪 idf / avgdl 的
   打分,绝不改变"哪些 doc 被找到"——posting 与 doc 记录是 per-doc 的,无跨 doc 竞态),
   且需要"多进程写同一 agent"的非主流拓扑。所以不给热路径加 per-writer 分片或服务端
   原子自增,而是**容忍漂移 + 用 §15 的 `reconcile` 检出 + `repair_stats` 在热路径之外
   修**。
4. **一次逻辑写只算一个 `expires_at` 复用到所有 key**(避免客户端 clock skew 让同一批
   key 错位过期);TTL 对象**禁用 `put_stream_begin`**——它当前忽略 `expires_at`。
5. **检索语义是 near-real-time / eventually consistent,不是 snapshot search。**
   `get_values` 会跳过在"扫 key"与"取值"之间消失(删除/过期)的 key。要更强得引入
   snapshot/transaction/CAS。分页用最后一个 key 的后继(`last_key ++ 0x00`)独占续扫。
6. **所有 IVF/BM25/redge/nidx posting 都是 derived index = 可丢弃、可重建。**
   split/freeze 当**正常可重试错误**处理,不当一致性保证。

## 9. 共享 memory:autumn 提供,不靠 daemon

「共享 memory」两义且**正交**:

| | 指什么 | 谁提供 |
|---|---|---|
| **共享数据** | 多 agent 读写同一记忆(org 知识库 / 跨 agent A 写 B 读 / 多 agent 改同一 state) | **autumn 本身**(同 key,全副本提交/read-your-writes;同 kvcache 跨 sglang 共享靠 partition) |
| **共享服务** | 一个受管入口大家都连 | **可选的无状态 gateway**(治理用,非共享数据) |

→ **纯客户端完全支持共享 memory**(数据共享在 autumn 这层,daemon 从来不是 sharing
来源)。企业要的「共享受管入口」是**治理**(集中鉴权/审计/脱敏/限流/集中 reconciliation)
= 可选 MCP/REST gateway = **同库包成无状态 server**;**部署与否数据共享语义不变**。

**并发写边界**:autumn 给的是**单 key 持久 + read-your-writes**,**不给并发 RMW 防丢
更新**(plain `put` = `WriteLease::ANON` 无 fence/CAS,`PutReq` 无 expected revision)。
多 agent 并发改同一 fact/state → 后写覆盖先写 = **丢更新**。共享 fact **不用覆盖式
RMW**:① append-only **event log + reducer**(事件溯源重建 shared state)② 或 `head`
指针 + 单写者 ③ 强一致需 coordinator/gateway 或扩展 autumn 条件写。**private
per-agent 记忆单写者,无此问题**。

**安全点:鉴权 / 租户隔离在 autumn 服务端强制**(client/gateway 可绕过,非安全边界)。
**autumn=安全边界,gateway=策略 UX,client 库=默认数据通路**。

## 9.5 多租户隔离

隔离是五层,**每层强度与谁强制不同**——别只看一层:

| 维度 | 机制 | 强度 | 谁强制 |
|---|---|---|---|
| **逻辑/数据** | key 前缀 `mem/{tenant}/{agent}/` | **只是组织,不是安全** | 客户端约定 |
| **安全/授权** | 调用方鉴权 + key-range 授权 | **真隔离** | **autumn server 端** |
| **检索** | 召回只 range-scan 本 agent 前缀;共享质心是模型非数据 | A 物理上扫不到 B 的数据 | key 前缀 + 召回逻辑 |
| **性能/噪声邻居** | auto-split 把热 agent 切独立核 | 部分(EN 磁盘/网络仍共享) | autumn split + (待补)配额/限流 |
| **崩溃爆炸半径** | RF=3 + 每 partition 一段 key-range | 故障域=partition 非 agent | autumn 复制/recovery |

**两级命名**:`{tenant}` = 硬边界(不同客户绝不互通);`{agent}` = 租户内软边界(私有
`mem/{tenant}/{agent}/` vs 共享 `mem/{tenant}/shared/`,§9)。隔离与共享是同一 key
schema 的两个区。

**最关键、最易踩的诚实点:key 前缀只是组织,不是安全。** 客户端能伪造别的 agent 的
key 读写。**真隔离由 autumn 服务端的 data-plane authz 提供**:PS 的 `authz_gate` 在每
个 frame dispatch 顶端做唯一 choke point,按凭据把调用方绑定到被授予的 key 前缀并拒绝
越界 read/range/write;开权限用
`autumn-op principal-create --principal <name> --grant mem/{tenant}/`,客户端带
`--credential-file`。详见 [data_plane_authz_design.md](./data_plane_authz_design.md)。
- **可信单组织内**:可以只用前缀约定。
- **多客户/多租户 SaaS**:**必须开 authz 并按 `mem/{tenant}/` 授权**,否则有的是
  「组织」不是「隔离」,不可对外宣称多租户隔离。

**检索隔离细节**:全局共享质心(§8)是**模型不是数据**(只是粗聚类中心,posting 仍
per-agent 前缀,不泄任何一条向量);但**训练质心别跨租户读数据** → 用公共/采样语料或
per-tenant 质心。

**性能隔离**:auto-split 把热 agent 切独立核([[feedback_auto_split_before_merge]]),
缓解噪声邻居;但 EN 磁盘带宽/网络共享 → 硬 QoS 需 per-tenant 限流/配额(待补)。

**爆炸半径**:海量小 agent 共置同一 partition(一段 key-range)→ 故障域=partition 非
per-agent;RF=3 兜底;大/热 agent 经 split 获独立 partition+独立故障域。

**(可选,企业级)per-tenant 信封加密**:每租户一把密钥,at-rest 也隔离。

## 10. daemon vs no-daemon 定论:默认 no-daemon

「要不要 daemon」=「要不要常驻热索引层(本地 SSD/RAM cache)」。**agent memory 选
no-daemon**,三理由:

1. **workload 不匹配**:agent 召回每轮一次、QPS 被 LLM 卡死、延迟被秒级 LLM 生成淹没;
   daemon 把召回 40ms→5ms 端到端无感。turbopuffer 的 SSD cache 是为高 QPS 交互搜索
   (Cursor/Notion),不是 LLM-bound 对话。
2. **autumn 不是慢对象存储**:turbopuffer 需 SSD cache 因后端=S3(冷读几百 ms);
   autumn=带 memtable+block cache 的快 KV(个位数 ms),客户端读 autumn 已享 PS 端缓存,
   再加客户端 SSD cache 赚得远不如 turbopuffer。
3. **简单**:无状态 → 无 LB、无亲和路由、无并行数据面,合仓库哲学。

**澄清**:no-daemon ≠ 无缓存——客户端进程自缓存热质心/桶,长寿进程预热(只是
per-process 不共享、重启冷启,对低 QPS 召回无所谓)。

**何时才上 daemon**(三条同时):单 agent 10^6–10^7 + 召回真延迟/QPS 敏感(非 LLM-bound)
+ 要 HNSW 级召回 → **仅给那些 namespace** 加 per-node sidecar daemon(引入 per-process
状态 → 一致性哈希 namespace 亲和路由)。**别预先为它设计**
([[feedback_reproduce_before_fixing_mechanism_bugs]] 不为想象性能问题预加复杂度)。

**决策规则:默认 no-daemon;只在 measured 需要时对特定 namespace 局部升级 sidecar。**

## 11. embedding

要不要看做不做向量召回:**纯词法(BM25)= 不要 embedder**;**向量召回 = 要**(写入
embed 每条、查询 embed query,两端都要)。turbopuffer/Qdrant/sqlite-vec 全不自带
embedding(都「自带向量」)。

核心库的 `index_vector` / `search_vector` 收的是**调用方给的 `&[f32]`**,自己不做
embedding。两条供给路径:

- **生产路径 = 远程端点**:复用已在跑的 sglang/vLLM 服务一个 embed 模型,客户端发 RPC
  拿向量 → client 始终薄。**它是已有推理基础设施,不是新 memory daemon**。
- **无模型服务时的内置 embedder**(`embed.rs`,可选):`HashEmbedder`(零依赖、
  signed-FNV bag-of-words,确定可复现,真管道弱语义)与 `StaticTableEmbedder`
  (`static-embed` feature,Model2Vec 式静态 int8 查表:分词 → int8 行查 → 反量化 →
  mean-pool;**有真语义、无网络、无 GPU**)。`Embedder` 枚举分派,所有变体都吐
  `EMBED_DIM` = 256 维 **L2 归一化**向量。这条不违反 §4 的否决项——被否的是把**神经**
  模型加载进每个 agent 进程炸显存。

彻底不要 embedding 依赖 → 走纯词法腿(代价:只有字面召回)。

## 12. 通用对接:MCP / REST / 原生适配器

- **(a) stdio MCP server**:host 按 session 派生子进程 = 核心库外壳(非常驻 daemon);
  暴露 `search`/`fetch`/`add`/`update`/`delete`。ChatGPT 普通模式只认 `search`+`fetch`
  (写工具要 Developer Mode)→ 命名给 search+fetch 对。通用触达但 model-invoked。避免
  长驻 HTTP/SSE MCP + REST daemon(除非企业要治理 gateway,那时无状态可多副本)。
  `examples/memory-mcp` 的同一个二进制加 `--mcp` 就是 stdio MCP server。
- **(b) REST API**(仅企业治理 gateway 形态):`add/search/get_all/get/update/delete`。
- **(c) 原生适配器**(零额外进程,纯 client):在宿主框架的语言里直接照 §6 的 key
  schema 实现;其它框架
  (LangGraph `BaseStore`、Mem0 `VectorStoreBase`、OpenAI Agents SDK Session)按同一
  key schema 复刻即可。

**满足 Mem0 后端契约**:实现 `VectorStoreBase`(11 方法);`search` 不要求 ANN,暴力/IVF
都合规;无独立关键词后端接口,hybrid Mem0 自融合,可 override `keyword_search()`。

## 13. autumn 客户端面约束

**能力**:point CRUD;每-key **TTL**(`expires_at`,经 `put_many` +
`ClusterClient::ttl_to_expires_at`);**前缀 `range(prefix, start, limit)`**(有序/分页/
跨分区去重)→ 情景/向量桶/倒排 posting 扫纯客户端可建;批量;大值(≥4KiB→VP,
striping)。

**约束**:
- ⚠️ `range` **只返 key 不返 value**(PS 的 `range_scan_sst_merge` 填 `value: vec![]`)
  → 每次 list/replay 都是"扫 key + 逐 key point-get"两跳。若 prefetch P99 痛,**受控
  改**:让 `range_scan_sst_merge` 顺带 `resolve_value`(先穷尽客户端再上服务端)。
- ⚠️ **无快照/时点读**,latest-wins,旧版 GC 丢弃 → 双时态走 key schema(§6)。
- ⚠️ `put_stream_begin` **忽略 `expires_at`** → 带 TTL 的对象不能走它(§8.5 契约 4)。
- key 编码 `user_key ++ BE(u64::MAX - seq)`(排序用 user-key 优先的比较器,非裸字节序),MVCC seq 仅内部。

## 14. 竞品定位 / 性能

**定位**:≈「**turbopuffer 的索引哲学**(SPFresh + BM25 posting-on-storage)+
**sqlite-vec/FTS5 的无-daemon 接入** + **FoundationDB 的 layer 基质**」。市面无完全对应
单品。**不像** Qdrant/Milvus/Pinecone(daemon + RAM 索引 + ANN-first);**不像**
Mem0/Zep/Letta 作为托管服务(autumn-memory 是它们底下可建的后端);作为 agent-memory
风格更近 **Memori/memweave**(嵌入式、向量可选)。

**性能(vs turbopuffer)——分 workload**:
- **autumn 结构性赢**:冷/尾延迟(autumn 后端快 KV 无 S3 悬崖,无双峰);多租户海量小
  namespace 冷访问平尾;RDMA/UCX 数据面([[project_ucx_crosshost_wins]])。
- **turbopuffer 仍赢**:热点 warm 延迟(本地 NVMe 索引零网络);索引成熟度
  (SPFresh+bitpack+MAXSCORE);$/GB(S3 比 RF3 快存储便宜)。
- **结论**:**针对 agent memory(LLM-bound、低 QPS、延迟不敏感、多小租户)能平甚至超**
  ——turbopuffer 的核心优化(NVMe cache 掩盖 S3)在此既派不上用场、又正好是 autumn 的
  结构强项;**全面超过不能也不必**(不同 workload)。

## 15. 完整性检查与验收

派生索引是可丢弃、可重建的(§8.5 契约 6),所以正确性靠**热路径之外的审计 + 修复**,
不靠热路径加锁:

- **`reconcile() -> ReconcileReport`**(只读):重数活的 `doc/{id}` 与 `meta/stats` 对账;
  拿 IVF posting 与 vptr 交叉核对,数**实际** posting(绝不按 id 折叠),因此能报出
  `duplicate_ivf`(同 id 在两个桶——train 崩溃残留)、`orphan_ivf`(posting 的 id 没
  vptr)、`dangling_vptr`(vptr 的桶里没 posting)、`malformed_vptr`(值 ≠ 4 字节)。
  同一趟还查图:`nodes`/`edges`/`redges` 计数 + `dangling_edge`(端点节点记录缺失)、
  `orphan_redge`(反向 marker 无正向边)、`missing_redge`(正向边无反向 marker)。
  `is_clean()` = 统计对账一致且以上全为 0。
  **SCOPE:只查结构完整性,不查质心分配的最优性**——train 中途崩可能把 posting 留在
  次优桶里(那是 ANN 召回质量,不是损坏;重跑 `train_centroids` 即愈)。
- **`repair_stats()`**:重扫 `doc/` 后重写 `meta/stats`。它自己就是无 CAS 的
  read-then-write,**必须在写者静默的维护窗口跑**,否则会踩掉并发写。

**验收口径**:每次功能推进都要在 **split / compaction / TTL nemesis** 下跑
index-reconcile 检查(主记录↔posting↔统计一致性、orphan/stale posting、TTL 级联),
合仓库 chaos 文化([[project_chaos_writeliveness_check]])。

**Non-goals(直到外部需求)**:autumn 内置 ANN/倒排引擎、图遍历下推、服务端 hybrid
融合、记忆专用 wire 协议、默认 daemon。

## 16. 开放问题

① `prefetch` 的两跳(§13)是否逼出「value 携带 range」的服务端改 —— 实测决定;
② 全局共享质心的训练/再训练触发(冷启 vs 周期;SPFresh LIRE 增量);
③ 双时态 key schema 的 valid-time 编码;
④ 量化档(int8/binary)——当前 posting 存 f32(`[dim u32 LE][f32 LE]*dim`),换档看召回
质量基准;
⑤ sidecar 在 no-daemon 下的取舍边界(per-process 状态 vs 重建成本)。
