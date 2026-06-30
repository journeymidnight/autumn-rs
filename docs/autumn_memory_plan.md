# autumn-memory 设计与实施计划

**日期**: 2026-06-30（经一轮完整设计讨论收敛定稿）
**状态**: 设计已收敛,待 feature 立项实施
**关联文档**: [autumn_kvcache_plan.md](./autumn_kvcache_plan.md)
**关联记忆**: [[project_agent_memory_backend_fit]] · [[project_three_interfaces]] · [[feedback_no_parallel_data_plane]] · [[feedback_client_side_complexity_first]]

---

## 0. TL;DR(一页纸结论)

把 autumn-rs 做成 **AI agent memory 的耐久分布式后端**,形态 = **纯客户端库(无 daemon)**,各 agent 内嵌、直连 autumn。

- **接入面**(决定能连哪些 agent)与 **检索后端**(决定怎么找、多大多准)是**两条独立的轴**,别混。
- 接入:**原生适配器**(Hermes / LangGraph / Mem0,自动生命周期)+ **MCP(stdio)/REST**(通用触达)。
- 检索:**两条腿都用 posting-on-KV** —— 向量 = SPFresh 式 **IVF-on-KV**,词法 = **BM25-on-KV**;MVP 用 **暴力 / sidecar FTS5** 单腿垫;hybrid = 两腿 + RRF 客户端融合。
- **embedding** 走远程端点(复用已有 sglang),不进客户端进程。
- **共享 memory** 由 autumn 这层提供(多 agent 读写同 key),**不是靠 daemon**;企业治理用**可选的无状态 gateway**。
- **默认 no-daemon**;只在 measured 需要热内存 HNSW 的特定 namespace 才局部升级 sidecar。
- 定位:≈「turbopuffer 的索引哲学 + sqlite-vec/FTS5 的无-daemon 接入 + FoundationDB 的 layer 基质」,但**没有完全对应的单品**。

---

## 1. 目标

让运行在 vLLM/SGLang 上的 agent(Hermes、LangGraph、Mem0、自研 loop)把长期记忆(情景/语义/过程)落到 autumn partition 层。

**核心论点**:agent memory 在存储层分两半——**system-of-record**(autumn 强契合)+ **检索智能**(向量 ANN/倒排/图,主流系统本就外置成「指回 KV 的索引」,正确分层,非缺陷)。autumn 不去「变成」Qdrant/turbopuffer(并行数据面 + 匹配不完的工程)。

**闭环卖点**:同一批 vLLM/SGLang 推理机已把 autumn 当 KV-cache 的 L3([[project_autumn_kvcache_architecture]])。再接长期记忆 = **一套 autumn 同时装「激活记忆(KV cache)」+「明文记忆(episodic/semantic)」**(MemOS/MemCube「统一基质」方向)。本地嵌入式方案(sqlite-vec / 本地 tantivy)给不了的:分布式持久、多租户自动 split、跨集群共享、原生 TTL。

## 2. 部署假设(**这是设计前提,不要绕开**)

- **autumn ↔ 一个 sglang/vLLM**(配对部署,可同机可不同机);autumn partition 集群独立部署。
- **一个 sglang 之上挂多个 agent**(多用户 / 多框架 / 多会话)→ **多客户端**。
- **多租户真实规模形状**:海量「各自很小」的 per-agent 命名空间(典型单 agent 10^3–10^5 条记忆),少数中规模(10^5–10^7)。autumn 的 split/merge 按 key range 自动把热 agent 切到独立核——正打这个形状。

直接推论:
- **多 agent = 多客户端**,但**不需要共享 daemon**:共享数据由 autumn 这层提供(§9);各 agent 直连。
- embedding 是个**推理服务**,但它是 agent 推理**本来就有的** sglang,不是新 memory daemon(§11)。
- 区别于 kvcache:kvcache 是「一节点一个 sglang = 单客户端」,memory 是「一 sglang 多 agent = 多客户端」——但结论仍是无 daemon(§10)。

## 3. 架构:纯客户端库 + 可选无状态 gateway

```
┌─ 多个 agent(同一 sglang 之上)─────────────────────────────┐
│  agent 进程 = 框架 + 内嵌适配器(薄)                          │
│   ├─ Hermes MemoryProvider / LangGraph BaseStore / Mem0 后端  │
│   └─ autumn-memory 核心库(命名空间 / 情景 / 事实 / 检索)     │
│        ├─ 要向量? → RPC 调 sglang 的 embed 端点拿向量         │
│        └─ 读写 posting / 记录 → ClusterClient 直连 autumn      │
└───────────────┬──────────────────────────────┬───────────────┘
                │                              │
         (通用触达,可选)                 (无中间服务)
   stdio MCP 子进程 / 可选无状态 gateway        │
   = 同一核心库的 server 外壳(治理用)          │
                └──────────────┬───────────────┘
                               │ partition gRPC (UCX/TCP)
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
| 客户端进程内加载 embedding 模型 | N agent 各加载 N 份炸显存;embedding 走远程端点(§11) |
| 依赖引擎 MVCC 做「时点/历史」记忆 | 客户端读不到历史、旧版 compaction 丢弃(§14);双时态在应用层 key schema 显式做 |
| 服务端二级索引 / 谓词扫描 / 聚合 / hybrid 融合 | partition 只保证 key 有序;过滤/打分/RRF 在客户端做 |
| 等「记忆专用协议标准」 | 不存在:A2A/AGNTCY=agent-to-agent 非记忆;OpenMemory MCP 只是跑在 MCP 上的产品;**MCP=事实唯一通用答案** |

## 5. 记忆分类 → 存储操作映射

| 记忆类型 | 主存储操作 | autumn 契合 |
|---|---|---|
| 工作/短期(激活态) | point get/put | ✅ 已有 = autumn-kvcache |
| 情景 episodic | append + 时间序前缀扫 | ✅ 极契合(LSM 本行) |
| 语义 semantic | 向量ANN 或 point-get+前缀list;supersede | ✅ 存储契合 / 检索见 §7 |
| 过程 procedural | point-get by name | ✅ 普通命名 KV |
| 图/双时态 | 图遍历 + 有效期查询 | ❌ 不原生(外置;有效期编 key) |

## 6. Key 格式

固定前缀 `mem/`,与 `kvc/`(kvcache)/ fuse / 普通 client 分命名空间。

```
情景日志:  mem/{tenant}/{agent}/ep/{session}/{ts_be}{seq}      → 事件 blob
事实 KV:   mem/{tenant}/{agent}/fact/{namespace}/{key}         → JSON  (= LangGraph BaseStore 模型)
向量(暴力):mem/{tenant}/{agent}/vec/{vec_id}                   → 向量+payload
向量(IVF): mem/{tenant}/{agent}/ivf/{centroid_be}/{vec_id}     → 向量(残差)
倒排(BM25):mem/{tenant}/{agent}/idx/{term}/{doc_id}            → posting(块化)
共享数据:  mem/{tenant}/shared/...                             → 跨 agent 共享(§9)
```

约定:`ts_be` 大端时间戳倒置 = newest-first;每 key 可带 **TTL**(`expires_at`,§14);质心表 / df-avgdl 等小状态也存成 autumn key。**双时态(coco P1)**= immutable `fact/{entity}/{valid_from}/{txid}`,「关闭旧有效期」**不原地覆盖**而是写 append-only correction/interval record;一个 bitemporal update 用 `commit_marker/txid` 标完整提交;重建历史只读 committed txid 并按 txid 去重(**不靠引擎 MVCC**;详见 §8.5)。

## 7. 检索层:两条腿,都 posting-on-KV

**先厘清三个易混概念(不在一个层面,非三选一):**
- **FTS** = 全文检索**能力**(analyzer 分词 + 倒排索引 + 打分函数)。
- **BM25** = FTS 内的**打分函数**(词法腿)。
- **SPFresh** = **向量 ANN 索引**(centroid/IVF/SPANN 族 + LIRE 增量更新,turbopuffer 向量用的;语义腿)。
- 关系:**BM25 ⊂ FTS = 词法腿;SPFresh = 向量腿;hybrid = 两腿 + RRF**。

**核心洞察:倒排索引 与 IVF 向量索引在有序 KV 上同形状 = 「posting-list 按前缀存 + 客户端打分」。** turbopuffer 生产把 posting 块化存做 BM25(MAXSCORE);Faiss `rocksdb_ivf` 把 centroid→posting 存 KV;Bleve 在 RocksDB 做 FTS。两者唯一不白送:决定「扫哪些 posting」的小结构(IVF=质心表 / HNSW=导航图),客户端建、可重建。

| 腿 | 稳态(无状态,合 no-daemon) | MVP 起步 | 不用 |
|---|---|---|---|
| **向量** | **SPFresh 式 IVF-on-KV**(`ivf/{centroid}/{vec}`,质心客户端缓存,探针扫桶+cosine,更新照 LIRE 分裂/合并/重分配) | **暴力**(`vec/` range-scan + cosine,≤10^5,要 embedder) | HNSW/DiskANN(图非 KV 友好) |
| **词法** | **BM25-on-KV**(`idx/{term}/{doc}` 块化倒排,BM25 客户端算) | **sidecar SQLite FTS5**(白嫖 BM25,零 embedder) | — |
| **融合** | **hybrid = 两腿 + RRF**(客户端三行,ordinal 融合) | 先单腿 | 服务端融合 |

**按规模选(每查询成本独立于 fleet 大小,因 key 前缀隔离):**

| 单 agent 规模 | 向量 | 每查询 |
|---|---|---|
| ≤ ~10^5(常见) | 暴力(= sqlite-vec 同款) | 几 ms,精确 |
| 10^5–10^7 / 多中规模 agent | IVF-on-KV | 几十 ms,~90% recall(只 probe 几桶) |
| > 10^7 单 agent | IVF-on-KV(LIRE)或 sidecar | — |

**满足 Mem0 后端契约**:实现 `VectorStoreBase`(11 方法);`search` 不要求 ANN,暴力/IVF 都合规;无独立关键词后端接口,hybrid Mem0 自融合,可 override `keyword_search()`。

**SPFresh 是借「设计」非跑其代码**(原版 SPDK/裸 NVMe);autumn 借其逻辑模型(centroid posting list + LIRE),铺在 autumn KV 上。

## 8. 多租户容量模型 + IVF 共享质心

**结构优势:每 agent 检索成本被 key 前缀隔离**,agent A 查询只扫 `mem/{tenant}/A/...`,与 fleet 其它 agent 数无关(单机 sqlite-vec 做不到)。两条独立的轴:

**轴一 单 agent 检索**:见 §7。对「多中规模 agent」**必须 IVF-on-KV**(posting 在 autumn 磁盘,每查询只读探中几桶);**全局共享粗量化器**(一套质心,key=`[agent][centroid][vec]`)→ 质心 RAM 一份(几 MB),**per-agent RAM≈0,agent 数无上限**。暴力/sidecar-HNSW 要 per-agent RAM,撑不住多中规模 agent。

**轴二 聚合容量** = Σ 各 agent,横向扩。单 agent 存储(float32,每条=维度×4B;int8 ÷4,binary ÷32):10^5≈0.3–1.2GB,10^6≈3–12GB,10^7≈30–120GB(加正文 ~0.5–2KB/条)。实用甜点 **10^6·768维·int8≈~1.8GB/agent**。

| agent 数(各 ~2GB) | 原始 | ×RF3 | 约 EN(~10TB/EN) |
|---|---|---|---|
| 1,000 | 2 TB | 6 TB | 几台 |
| 10,000 | 20 TB | 60 TB | ~6–8 |
| 100,000 | 200 TB | 600 TB | ~60–80 |

QPS:单 partition ~30K [[project_partition_qps_ceiling]],扩=加 partition [[feedback_no_multiworker_per_partition]],热 agent 自动 split [[feedback_auto_split_before_merge]];单个超大 agent 的连续 range 也被 split 切到多 partition。

## 8.5 索引一致性契约(correctness contract)

**核心问题(coco arch 评审 2026-06-30 抓出,plan 此前欠详)**:一次逻辑 memory 写 = 主记录 + 多 IVF posting + 多 BM25 posting + df/avgdl 统计 + TTL/删除 marker = **多 key、常跨分区、无事务、无快照、无 CAS** 的派生状态。autumn 保证单 key 字节级 durable,但**不保证这些 key 之间逻辑一致**。风险非字节损坏,而是**索引↔主记录持久逻辑不一致** → 漏召回 / 错召回 / 过期可见 / 并发丢写 / 旧质心污染。

**8 条契约(立项前定为 correctness contract,逐条标 phase):**

| # | 契约 | 何时落 |
|---|---|---|
| 1 | **主记录不可变版本化** `doc/{doc_id}/{generation}`;`doc_head/{doc_id}` 只指当前 generation | Phase 3(MVP 单 key 记录无扇出) |
| 2 | **posting 携带 `generation/expires_at/index_epoch/centroid_epoch`** | Phase 3 |
| 3 | **查询必反查主记录,校验 generation/TTL/delete marker**,不匹配丢弃;**posting 仅候选,主记录是正确性边界** | Phase 3(暴力 MVP 直读主记录天然满足) |
| 4 | **逻辑写用 commit marker**(主记录 pending_index → 写 posting → index_done);写**幂等**(deterministic posting key,重试不重复) | Phase 3 |
| 5 | **统计(df/avgdl/centroid count)走 append-only delta-log + 周期重算**,绝不覆盖式 RMW(多客户端丢增量) | Phase 3 |
| 6 | **质心 epoch manifest** `centroids/{epoch}`+`centroids/current` 指针;切换窗口双写 old/new 或双 probe;LIRE 作可恢复作业 prepare→migrate→publish→cleanup;客户端缓存带 TTL+epoch check | Phase 3(IVF) |
| 7 | **一次逻辑写只算一个 `expires_at` 复用到所有 key**(避免客户端 clock skew 错位);TTL 对象**禁用 `put_stream`**(当前忽略 expires_at,lib.rs:2231) | **立即**(trivial guardrail) |
| 8 | **声明检索语义 = near-real-time / eventually consistent**(非 snapshot search);要更强需引入 snapshot/transaction/CAS | **立即**(文档声明) |

**right-size(CLAUDE.md item12,非反驳 coco)**:MVP(单 key 记录 + 暴力召回)几乎无多 key 扇出 → 契约 1–6 **随 Phase 3 posting-on-KV 落地**,不必为 MVP 建全套 commit-marker;契约 7–8 立即采纳。所有 IVF/BM25 posting 视作 **derived index = 可丢弃重建**;补 `index_manifest` + 周期 reconcile;split/freeze 当**正常可重试错误**,不当一致性保证。

## 9. 共享 memory:autumn 提供,不靠 daemon

「共享 memory」两义且**正交**:

| | 指什么 | 谁提供 |
|---|---|---|
| **共享数据** | 多 agent 读写同一记忆(org 知识库 / 跨 agent A 写 B 读 / 多 agent 改同一 state) | **autumn 本身**(同 key,MVCC/全副本提交/read-your-writes;同 kvcache 跨 sglang 共享靠 partition) |
| **共享服务** | 一个受管入口大家都连 | **可选的无状态 gateway**(治理用,非共享数据) |

→ **纯客户端完全支持共享 memory**(数据共享在 autumn 这层,daemon 从来不是 sharing 来源)。企业要的「共享受管入口」是**治理**(集中鉴权/审计/脱敏/限流/集中 reconciliation)= 可选 MCP/REST gateway = **同库包成无状态 server**;**部署与否数据共享语义不变**。

**并发写边界(coco P0,修正本节「autumn 提供共享」的过满表述)**:autumn 给的是**单 key 持久 + read-your-writes**,**不给并发 RMW 防丢更新**(plain `put`=`WriteLease::ANON` 无 fence/CAS,`PutReq` 无 expected revision)。多 agent 并发改同一 fact/state → 后写覆盖先写 = **丢更新**。共享 fact **不用覆盖式 RMW**:① append-only **event log + reducer**(事件溯源重建 shared state)② 或 `head` 指针 + 单写者 ③ 强一致需 coordinator/gateway 或扩展 autumn 条件写。**private per-agent 记忆单写者,无此问题**(主题 B 只在 shared 跨 agent 写是 P0)。

**安全点:鉴权 / 租户隔离必须在 autumn(server 端)强制**(client/gateway 可绕过,非安全边界;autumn 已有 tenant/lease fencing,见 [[project_production_readiness_audit]] 待补「鉴权/TLS+授权」)。**autumn=安全边界,gateway=策略 UX,client 库=默认数据通路**。

## 9.5 多租户隔离

隔离是五层,**每层强度与谁强制不同**——别只看一层:

| 维度 | 机制 | 强度 | 谁强制 |
|---|---|---|---|
| **逻辑/数据** | key 前缀 `mem/{tenant}/{agent}/` | **只是组织,不是安全** | 客户端约定 |
| **安全/授权** | 调用方鉴权 + key-range 授权 | **真隔离** | **autumn server 端(必须)** |
| **检索** | 召回只 range-scan 本 agent 前缀;共享质心是模型非数据 | A 物理上扫不到 B 的数据 | key 前缀 + 召回逻辑 |
| **性能/噪声邻居** | auto-split 把热 agent 切独立核 | 部分(EN 磁盘/网络仍共享) | autumn split + (待补)配额/限流 |
| **崩溃爆炸半径** | RF=3 + 每 partition 一段 key-range | 故障域=partition 非 agent | autumn 复制/recovery |

**两级命名**:`{tenant}` = 硬边界(不同客户绝不互通);`{agent}` = 租户内软边界(私有 `mem/{tenant}/{agent}/` vs 共享 `mem/{tenant}/shared/`,§9)。隔离与共享是同一 key schema 的两个区。

**最关键、最易踩的诚实点:key 前缀只是组织,不是安全。** 客户端能伪造别的 agent 的 key 读写 → 纯前缀给不了隔离。**真隔离必须 autumn server 端**:①鉴权调用方(mTLS/token)②绑定到允许的 prefix ③拒绝越界 read/range/write。这是 [[project_production_readiness_audit]]「鉴权/TLS+授权」待补项,且是**多客户隔离的前置条件**:
- **可信单组织内**:可先只用前缀约定,够用。
- **多客户/多租户 SaaS**:**必须先补 server 端 authz**,否则有的是「组织」不是「隔离」——不可对外宣称多租户隔离(见 §16 Phase 0)。

**检索隔离细节**:全局共享质心(§8)是**模型不是数据**(只是粗聚类中心,posting 仍 per-agent 前缀,不泄任何一条向量);但**训练质心别跨租户读数据** → 用公共/采样语料或 per-tenant 质心。

**性能隔离**:auto-split 把热 agent 切独立核([[feedback_auto_split_before_merge]]),缓解噪声邻居;但 EN 磁盘带宽/网络共享 → 硬 QoS 需 per-tenant 限流/配额(待补)。

**爆炸半径**:海量小 agent 共置同一 partition(一段 key-range)→ 故障域=partition 非 per-agent;RF=3 兜底;大/热 agent 经 split 获独立 partition+独立故障域。

**(可选,企业级)per-tenant 信封加密**:每租户一把密钥,at-rest 也隔离;Phase 3+ 硬化项。

## 10. daemon vs no-daemon 定论:默认 no-daemon

「要不要 daemon」=「要不要常驻热索引层(本地 SSD/RAM cache)」。**agent memory 选 no-daemon**,三理由:

1. **workload 不匹配**:agent 召回每轮一次、QPS 被 LLM 卡死、延迟被秒级 LLM 生成淹没;daemon 把召回 40ms→5ms 端到端无感。turbopuffer 的 SSD cache 是为高 QPS 交互搜索(Cursor/Notion),不是 LLM-bound 对话。
2. **autumn 不是慢对象存储**:turbopuffer 需 SSD cache 因后端=S3(冷读几百 ms);autumn=带 memtable+block cache 的快 KV(个位数 ms),客户端读 autumn 已享 PS 端缓存,再加客户端 SSD cache 赚得远不如 turbopuffer。
3. **简单**:无状态 → 无 LB、无亲和路由、无并行数据面,合仓库哲学。

**澄清**:no-daemon ≠ 无缓存——客户端进程自缓存热质心/桶,长寿进程预热(只是 per-process 不共享、重启冷启,对低 QPS 召回无所谓)。

**何时才上 daemon**(三条同时):单 agent 10^6–10^7 + 召回真延迟/QPS 敏感(非 LLM-bound)+ 要 HNSW 级召回 → **仅给那些 namespace** 加 per-node sidecar daemon(引入 per-process 状态 → 一致性哈希 namespace 亲和路由)。**别预先为它设计**([[feedback_reproduce_before_fixing_mechanism_bugs]] 不为想象性能问题预加复杂度)。

**决策规则:默认 no-daemon;只在 measured 需要时对特定 namespace 局部升级 sidecar。**

## 11. embedding

要不要看做不做向量召回:**纯词法(BM25/FTS)= 不要 embedder**(Memori/memweave/Hermes 自带 FTS5 都零 embedding);**向量召回 = 要**(写入 embed 每条、查询 embed query,两端都要)。

turbopuffer/Qdrant/sqlite-vec 全不自带 embedding(都「自带向量」)。在 no-daemon 设计里:**embedding = 远程端点**,优先**复用已在跑的 sglang/vLLM 服务一个 embed 模型**(两者都支持),客户端发 RPC 拿向量 → client 始终薄。**它是已有推理基础设施,不是新 memory daemon**。彻底不要 embedding 依赖 → 走纯词法腿(代价:只有字面召回)。

## 12. 通用对接:MCP / REST / 原生适配器

- **(a) stdio MCP server**:host 按 session 派生子进程 = 核心库外壳(非常驻 daemon);暴露 `search`/`fetch`/`add`/`update`/`delete`。ChatGPT 普通模式只认 `search`+`fetch`(写工具要 Developer Mode)→ 命名给 search+fetch 对。通用触达但 model-invoked。避免长驻 HTTP/SSE MCP + REST daemon(除非企业要治理 gateway,那时无状态可多副本)。
- **(b) REST API**(仅企业治理 gateway 形态):`add/search/get_all/get/update/delete`。
- **(c) 原生适配器**(零额外进程,纯 client):首批 **Hermes MemoryProvider**(§13) + **LangGraph BaseStore**;按需 Mem0 `VectorStoreBase` / OpenAI Agents SDK Session。

## 13. Hermes Agent 适配器(首批原生皮)

「hermes」= Nous Research **Hermes Agent**(2026-02 开源 Python runtime,跑 vLLM/SGLang/OpenAI-compat)。接口 `MemoryProvider` ABC(`agent/memory_provider.py`):

| 方法 | 约束 | autumn 侧 |
|---|---|---|
| `initialize(session_id, **kwargs)` | 拿 hermes_home/user_id/agent_identity | 开 ClusterClient + 推命名空间 |
| `sync_turn(user_content, assistant_content, *, session_id, messages)` | **必须非阻塞**(daemon 线程) | append 情景日志 + 可选抽事实→embed→写 vec |
| `prefetch(query, *, session_id) -> str` | **必须快** | 召回(近期情景前缀扫 + 向量/词法召回)拼 context |
| `get_tool_schemas()` / `handle_tool_call(...)` | 可选 | 给模型 `memory_search`/`memory_store` 工具 |
| `on_session_end` / `on_pre_compress` | flush/压缩前 | 摘要落盘 |
| `on_memory_write(action, target, content)` | 镜像内置 MEMORY.md/USER.md | 持久化进 autumn |
| `register(ctx)` | 入口 | `ctx.register_memory_provider(...)` |

打包 `plugins/memory/autumn/`;**Hermes in-tree 封闭 → 独立 plugin repo `hermes-memory-autumn` 发**(pip 装 `~/.hermes/plugins/`)。
**要验证**:`prefetch` 延迟撞 §14 range-keys-only → 以 point-get + 有界近期情景扫 + 单agent暴力召回(≤10^5)为主,先测 P99。**边界**:内置 MEMORY.md/USER.md 故意本地小,不替换,只 `on_memory_write` 镜像。

## 14. autumn 客户端面约束

**今天能、零服务端改**:point CRUD;每-key **TTL**(`expires_at`,经 `put_many`+`ttl_to_expires_at`);**前缀 `range(prefix,start,limit)`**(有序/分页/跨分区去重)→ 情景/向量桶/倒排 posting 扫纯客户端可建;批量;大值(≥4KiB→VP,striping 到 256MiB)。

**约束 / 可能服务端小改**:
- ⚠️ `range` **只返 key 不返 value**(rpc_handlers.rs:944)→ 扫描两跳(扫 key + `get_many`)。若 prefetch P99 痛,**受控改**:`range_scan_sst_merge` 顺带 `resolve_value`(先穷尽客户端再上)。
- ⚠️ **无快照/时点读**,latest-wins,旧版 GC 丢弃 → 双时态走 key schema(§6)。
- key 编码 `user_key ++ 0x00 ++ BE(u64::MAX - seq)`,MVCC seq 仅内部。

## 15. 竞品定位 / 性能

**定位**:≈「**turbopuffer 的索引哲学**(SPFresh + BM25 posting-on-storage)+ **sqlite-vec/FTS5 的无-daemon 接入** + **FoundationDB 的 layer 基质**」。市面无完全对应单品。**不像** Qdrant/Milvus/Pinecone(daemon + RAM 索引 + ANN-first);**不像** Mem0/Zep/Letta 作为托管服务(autumn-memory 是它们底下可建的后端);作为 agent-memory 风格更近 **Memori/memweave**(嵌入式、向量可选)。

**性能(vs turbopuffer)——分 workload**:
- **autumn 结构性赢**:冷/尾延迟(autumn 后端快 KV 无 S3 悬崖,无双峰);多租户海量小 namespace 冷访问平尾;RDMA/UCX 数据面([[project_ucx_crosshost_wins]])。
- **turbopuffer 仍赢**:热点 warm 延迟(本地 NVMe 索引零网络);索引成熟度(SPFresh+bitpack+MAXSCORE);$/GB(S3 比 RF3 快存储便宜)。
- **结论**:**针对 agent memory(LLM-bound、低 QPS、延迟不敏感、多小租户)能平甚至超**——turbopuffer 的核心优化(NVMe cache 掩盖 S3)在此既派不上用场、又正好是 autumn 的结构强项;**全面超过不能也不必**(不同 workload)。

## 16. Phase / 里程碑

**Phase 0 / 前置(多客户场景 non-negotiable,§9.5)**:**autumn server 端鉴权 + key-range 授权**(把调用方绑定到允许的 `mem/{tenant}/...` 前缀,拒绝越界 read/range/write)。
- **可信单组织**:可跳过,先用前缀约定起 Phase 1。
- **多客户/多租户 SaaS**:**必须先做**——否则只是「组织」非「隔离」,不可对外宣称多租户。属 [[project_production_readiness_audit]]「鉴权/TLS+授权」。

**Phase 1(MVP,单 feature)**:框架无关 `autumn-memory` 核心库
- 情景日志读写(append + 前缀扫回放)+ 事实 KV(point-get + 前缀 list + TTL)
- 召回 **单腿起步**:纯词法(sidecar FTS5,零 embedder)**或** 纯向量(暴力 ≤10^5,sglang embedder)
- 验收:单 agent 写入→回放→事实读写→单腿召回 e2e 绿

**Phase 2(验两端)**:
- **Hermes MemoryProvider** 插件(initialize/sync_turn/prefetch)→ Hermes + vLLM/SGLang e2e(顺带 kvcache L3 闭环)
- **stdio MCP server**(search/fetch/add/update/delete)→ 接一个 MCP host 验通用触达
- LangGraph BaseStore 适配器

**Phase 3(按规模/质量)**:
- **hybrid**(两腿 + RRF);**BM25-on-KV**(块化倒排,脱离 sidecar 本地状态)
- **SPFresh 式 IVF-on-KV + 全局共享质心**(多中规模 agent)
- Mem0 `VectorStoreBase` 适配器;可选无状态治理 gateway(REST + 集中鉴权审计)

**Phase 4(仅 measured 需要)**:特定 namespace 的 per-node sidecar daemon(热内存 HNSW + 一致性哈希亲和)。

**验收(贯穿 Phase 1–3,coco P0–P3)**:每 Phase 加 **split / compaction / TTL nemesis** 下的 **index-reconcile 检查**(主记录↔posting↔统计一致性、orphan/stale posting、TTL 级联、generation 校验),合仓库 chaos 文化([[project_chaos_writeliveness_check]])。

**Non-goals(直到外部需求)**:autumn 内置 ANN/倒排引擎、图遍历、服务端 hybrid 融合、记忆专用 wire 协议、默认 daemon。

## 17. 现状盘点 / 开放问题

- 核心库未立项;仓库无 hermes/agent-memory 代码(grep 确认)。
- 开放:① `prefetch` 两跳延迟是否逼出「value 携带 range」服务端改 —— Phase 1/2 实测 ② 全局共享质心训练/再训练触发(冷启 vs 周期;SPFresh LIRE 增量) ③ 双时态 key schema 的 valid-time 编码 ④ 量化档(int8/binary)默认 —— 看召回质量基准 ⑤ sidecar 在 no-daemon 下的取舍边界(per-process 状态 vs 重建成本)。
- 立项后:补 `feature_list.md` 验收账本 + `claude-progress.txt`,按 CLAUDE.md 长任务规则推进;Phase 1 起每 feature 走「定义→实现→测试→更新 README→commit」。
