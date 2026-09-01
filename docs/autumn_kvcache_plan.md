# autumn-kvcache 设计

**关联文档**: [hicache_l3_interface.md](./hicache_l3_interface.md)
**关联代码**: `python/autumn_kvcache/autumn_kvcache/`（`sglang_backend.py` /
`vllm_connector.py` / `_keys.py` / `_identity.py` / `_bridge.py`）·
`python/src/lib.rs`（`Client` / `BatchClient` 的 zero-copy 入口）

---

## 1. 目标

给 sglang 的 HiCache L3 storage backend 和 vLLM 的 `KVConnectorV1` 各提供一个
autumn-rs 接入实现，让推理集群把"溢出的 KV cache"持久化到 autumn partition 层。

**autumn-kvcache 是 [[project_three_interfaces]] 里"partition 之上三个接口之一"**，
与 autumn-fuse、autumn-client 并列，本质是一个推理框架 friendly 的 façade。

## 2. 部署假设

部署形态（**这是设计的前提，不要绕开这一条做设计**）：

- **多节点**集群
- **每节点一个推理实例**（一卡一进程也罢、TP 内部多卡也罢，外部只暴露一个进程）
- **单个实例可以跨多节点**（TP/PP/EP 跨机），每个节点上是一个 worker rank
- autumn partition 集群独立部署，跟推理节点可能同机也可能不同机

直接推论：
- 同节点没有多推理进程共享需求 → **不需要 daemon + 本地 DRAM cache 这一层**
- 跨节点共享靠 partition（partition memtable + block cache 已经在 RAM）
- 跨实例 / 重启复用同样靠 partition

## 3. 架构

### 3.1 形态

```
┌────────────────────────────────────────────────┐
│  推理 worker (一个 tp_rank, 一个节点)            │
│  ├─ L1 GPU HBM                                 │
│  ├─ L2 HostKVCache (pinned host DRAM)          │
│  └─ HiCacheController / KVConnector             │
│     ├─ prefetch / backup / forward hooks        │
│     └─ autumn adapter  (Python class)          │
│        └─ autumn.BatchClient (PyO3)            │
└────────────────┬───────────────────────────────┘
                 │ partition RPC (UCX/TCP)
┌────────────────▼───────────────────────────────┐
│  autumn partition layer (autumn-ps cluster)     │
│  ├─ memtable + block cache  ← 隐式 DRAM cache   │
│  └─ stream layer (持久化)                       │
└────────────────────────────────────────────────┘
```

**autumn-kvcache 全部代码 = 一个 Python 包 + autumn PyO3 binding 的 zero-copy
入口**。没有 Rust daemon，没有 sidecar 进程，没有独立 LRU。

### 3.2 不做哪些事（design rationale）

| 否定项 | 否决理由 |
|---|---|
| sidecar Rust daemon | 单推理实例 per node，daemon 服务的客户端只有一个，跟框架自己的 L2 平级，没有跨进程共享需求 |
| 本地 DRAM LRU (L3a) | 跟 L2 重复；要 DRAM 不如直接配给 sglang `hicache_ratio` |
| daemon-to-daemon peer mesh / RDMA cache mesh | partition 已经是分布式 KV；再做 peer mesh 违反 [[feedback_no_parallel_data_plane]] |
| 本地 mmap slot file (3FS 风格) | partition 已经是持久层；再加本地 NVMe slot 等于自建并行数据面 |
| 参与 sglang PD-disaggregation 传输 | 那是 sglang transfer engine 的职责（mooncake transfer engine / NIXL），HiCache L3 接口不覆盖 |

## 4. sglang 接口契约

`AutumnKVCacheStorage` 实现 sglang `HiCacheStorage` 的 **v1 子集**
（zero-copy single-pool）。构造签名 `(storage_config, extra_kwargs)` 就是
`--hicache-storage-backend dynamic` 的 `StorageBackendFactory` 传进来的形状；
`storage_config.extra_config` 是 `--hicache-storage-backend-extra-config` 的
JSON dict（sglang 不把它拆成 kwargs，全部挂在 dataclass 上）。

### 4.1 v1 热路径

```python
class AutumnKVCacheStorage(HiCacheStorage):
    def __init__(self, storage_config=None, extra_kwargs=None): ...
    def register_mem_pool_host(self, mem_pool_host): ...
    def batch_get_v1(self, keys, host_indices, extra_info=None) -> List[bool]: ...
    def batch_set_v1(self, keys, host_indices, extra_info=None) -> List[bool]: ...
    def batch_exists(self, keys, extra_info=None) -> int:  # contiguous-prefix int!
    def get_stats(self): ...
```

`extra_config["interface_v1"] = 1` 后 sglang 的 cache_controller 走 v1 路径
（docs:142–151）。

### 4.2 v0 abstract methods

`get` / `batch_get` / `set` / `batch_set` / `exists` 是薄 wrapper（走普通
`Client`，不走 `BatchClient` 热路径），只在 debug / 边角场景命中。

### 4.3 v2 multi-pool 不做

只支持 KV pool。hybrid attention 模型（Mamba / SWA）的多 pool 支持是 non-goal，
直到外部需求出现。

### 4.4 Optional

`clear()` 走 `batch_delete(pool_prefix)`——**pool-scoped**，绝不跨进同租户的
`vllm` pool。仅作 debug 用。

## 5. Key 格式

```
wire key = kvc/{tenant_suffix}/{pool_name}/{content_hash}
```

**客户端绑定 `scope="kvc"` 并自己 prepend `kvc/`**，所以 `_keys.full_key()` 只
emit 相对部分 `{tenant_suffix}/{pool_name}/{content_hash}`——scope 由构造锁死，
adapter 不拼 wire 前缀。

| 字段 | 来源 | 备注 |
|---|---|---|
| `kvc/` | client scope | 跟其它 autumn 接口（`fs/` / `mem/`）分命名空间 |
| `tenant_suffix` | `build_tenant_suffix(cfg, fingerprint)` | `{model}` + 可选 `_{fingerprint}` + `_{tp_rank}_{tp_size}`（MLA 模型跳过，其 KV 与 rank 无关）+ `_pp{pp_rank}_{pp_size}`（仅 `pp_size > 1`）。`model` 里的 `/` 折成 `_` |
| `pool_name` | sglang = `"kv"`，vLLM = `"vllm"` | 两个框架的 keyspace 天然分流 |
| `content_hash` | 内容寻址摘要 | sglang 直接用 controller 算好的 chain hash（backend 不重算）；vLLM 见 §13.3 |

`tenant_suffix` 的 `model` 段在 sglang 路径上就是 `--model-path`（本身是真实
身份），所以默认不带指纹；路径**不唯一**的部署（同一路径下两个微调、容器把不同
权重挂到同一挂载点）用 `extra_config["model_id"]` 显式区分，它会被折成指纹。
vLLM 路径**必须**带指纹（§13.3）。

内容寻址 ⇒ **永远不需要 invalidation**；`ttl_secs`（默认 0 = 不过期）是唯一的
回收旋钮。

## 6. 数据流

### 6.1 读路径 (`batch_get_v1`)

`host_indices` → `mem_pool_host.get_data_page(idx, flat=True)` 拿到 pinned host
pool 的 1-D tensor 视图 → `.view(torch.uint8).numpy()` 做**零拷贝字节重解释**
（numpy 没有原生 bfloat16 dtype，直接 `.numpy()` 会 TypeError）→ 整批交给
`BatchClient.get_into(full_keys, views)`。

Rust 侧一次 PyO3 调用在 GIL 下取出全部 dest 指针，然后 `allow_threads` 让
worker 流水传输并 memcpy 进 pinned page——**partition value 直接落进 sglang 的
host pool，不经 Python bytes**，且整个批期间 GIL 是放开的（sglang 其它线程照跑）。

未命中 → 该项返回 `False`，sglang 按"没找到这一段 prefix"处理。

### 6.2 写路径 (`batch_set_v1`)

同样的 page view，整批交给 `BatchClient.put_from(full_keys, views, ttl_secs)`；
write-through，ACK 后才返回 True。

不在 backend 内做异步 buffer——sglang `write_through` 默认就期望 ACK 之后才算
落盘（[[feedback_two_phase_commit]]）。如果 `set` 因为 partition 慢顶到 prefetch
budget（2s + 0.1s/Kitok，docs:224），那是 partition 性能问题，不该靠"本地 ACK
先回"掩盖。

### 6.3 存在性查询 (`batch_exists`)

返回 **contiguous-prefix int**（最长连续存在前缀长度），不是 per-key list
（docs:62–64）。

这条跑在 sglang 的**准入路径**（每请求一次，prefill 之前），所以形状是
**先探 key[0]，再决定要不要 fan-out**：

- `head(keys[0])` 为假 → 直接返回 0。冷/无命中是压倒性常见情形，一次 `head`
  就结案。
- key[0] 命中才 `batch_head(all)` 数出前缀能延伸多远——那是能省掉整个 prefill
  的情形，值得这些 RPC。

per-key fan-out 版本实测约 4.8 ms/call，正好是 L3 在 TTFT 上的全部开销。

## 7. 同步 / 异步桥接

sglang 的 prefetch/backup 线程与 vLLM 的 model-runner 线程都是**同步阻塞**调用
adapter，而 `autumn` PyO3 binding 的 `Client` 是 async。`_bridge.py` 持有一个独立
线程上的 asyncio loop，同步方法用 `run_coroutine_threadsafe(...).result()` 提交
并等待。

热路径不走这条：`BatchClient` 是 Rust 侧 GIL-releasing 的同步接口，没有 event
loop 一跳。`_bridge.py` 只服务低频路径（v0 方法、`batch_exists`、`clear`、
vLLM 的 marker 读写）。

## 8. Zero-copy buffer API

`python/src/lib.rs` 的 buffer-protocol 入口：

```rust
// Client（单 key）
fn put_from(&self, py, key: &[u8], buf: PyBuffer<u8>) -> ...
fn get_into(&self, py, key: &[u8], buf: PyBuffer<u8>) -> ...     // buf 必须可写
fn batch_put_from(...) / fn batch_get_into(...)
// BatchClient（热路径，多 key 多 buf，GIL-releasing）
fn get_into(&self, py, keys: Vec<Vec<u8>>, bufs: Vec<PyBuffer<u8>>) -> Vec<bool>
fn put_from(&self, py, keys: Vec<Vec<u8>>, bufs: Vec<PyBuffer<u8>>, ttl_secs: u64) -> Vec<bool>
```

- `PyBuffer<u8>` 接收任何 buffer protocol 对象（numpy / torch tensor / mmap）。
- `buf.as_ptr()` + `buf.len_bytes()` 直接喂 `ClusterClient` 的 raw 落点。
- 调用方保证 buffer 生命周期覆盖整个调用（同步桥接天然满足）。
- transport 是 UCX 时 **zero-copy 数据面是默认**（大 page 走 `MSG_PUT_BULK` /
  `MSG_GET_BULK`），没有 opt-in flag；TCP 走常规路径。
  `max_inflight` 默认 UCX 16 / TCP 64——UCX 单 worker 的 rendezvous 悬崖在 16。

## 9. Crash 语义

| 谁崩 | 后果 |
|---|---|
| 推理 worker | 控制器重启会丢 in-flight host_indices；autumn 这边无状态，无影响 |
| autumn-kvcache adapter | 它没有独立状态——它就是推理进程的一部分 |
| autumn-client (PyO3) | 同上 |
| autumn partition / extent-node | 走现有 stream layer 恢复（owner-lock fencing）；adapter 收到错误 → 该项返回 `False`，框架按 cache miss 处理 |

由于 key 是内容寻址（docs:190–200），**永远不需要 invalidation**。partition 是
唯一真相源。

## 10. 启动配置（sglang）

```bash
sglang ... \
  --enable-hierarchical-cache \
  --hicache-storage-backend dynamic \
  --hicache-storage-backend-extra-config '{
    "backend_name":"autumn",
    "module_path":"autumn_kvcache.sglang_backend",
    "class_name":"AutumnKVCacheStorage",
    "interface_v1":1,
    "endpoint":"manager:9001"
  }'
```

`extra_config` 认的键：`endpoint`/`manager`（必需）、`transport`（`tcp` 默认 /
`ucx`）、`max_inflight`、`client_workers`、`ttl_secs`（默认 0 = 不过期，负值
fail-fast）、`model_id`（§5）、`auth_credential_file`/`auth_principal`（§12）。

`client_workers` 默认 1：进程内多 worker 目前被共享的 process-global
`ucp_context` 卡住（worker 会在它上面串行），>1 是给 per-thread context 落地后
预留的。

## 11. 性能口径

- **prefix cache hit rate**：跟 baseline（`--hicache-storage-backend file`）比。
- **L3 lookup latency P50/P99**：必须 << prefetch budget（2s + 0.1s/Kitok）。
- **L3 write latency**：write_through 模式下的 worst-case bound。
- **partition QPS**：单 partition 不能超 [[project_partition_qps_ceiling]] 的
  30K QPS；超了说明该 split 或该按 tenant_suffix 分库。

按 [[feedback_perf_check_matrix]]：transport × partitions{8+} × depth{8} ×
size{4K, 8M}。**page size 由模型决定**，不在 perf matrix 里，但 benchmark 要覆盖
典型 page 大小（64KB、256KB、1MB）。

## 12. 命名空间与 authz 接线

两个 adapter 用同一套接线，都通过 `extra_config` / `kv_connector_extra_config`：

- **`scope="kvc"`** 恒定传给 `Client` / `BatchClient`；key builder 只 emit 相对
  key（§5）。
- **`auth_credential_file`** 是唯一必需的鉴权键——凭据文件自带 principal 名
  （`principal: <name>` 行，或 `<name>\n<hex>`）。文件读失败在启动时 fail loud。
- **`auth_principal`** 覆盖文件里的名字；单独给它而不给凭据文件是配置错误，直接
  报错。
- 凭据必须同时接到**两个** client：authz 对受保护前缀的**读**也设卡，一个没凭据
  的探测 client 会把每次命中静默变成 miss。

## 13. vLLM `KVConnectorV1` 适配器

### 13.0 形态：原生 connector，daemon-less

`AutumnKVConnector(KVConnectorBase_V1)` 直接坐在 `autumn.BatchClient` 上，与
`sglang_backend.py` 平级、共用同一条数据通路。

不经 LMCache 中转的理由：LMCache **自带本地 DRAM/GPU cache**，违反
[[project_autumn_kvcache_architecture]] 的"无本地 DRAM cache"，且多一跳。代价是
要自己实现 connector 生命周期（scheduler/worker 双角色），并把 vLLM 版本钉死
——`KVConnectorBase_V1` 的签名随版本漂移。

**铁律**：持久化只走 partition（[[feedback_no_parallel_data_plane]]）；adapter 无
daemon、无本地 LRU、无并行数据面。

### 13.1 与 sglang HiCache L3 的本质差异

sglang HiCache L3 是个**同步 storage backend**（§4）。vLLM 的 `KVConnectorV1` 是
**嵌进 forward pass 的连接器**，两点不同决定了 adapter 比 sglang 重：

1. **scheduler / worker 双角色**：connector 在 scheduler 进程和每个 worker 进程
   **都实例化**（`__init__(vllm_config, role)`）。scheduler 侧做前缀匹配 + 决定
   load/save 集合，worker 侧做实际 I/O，中间靠可序列化的
   `AutumnConnectorMetadata` 传递。
2. **paged KV block 布局**：KV 页要从 paged 张量里按 `slot_mapping` gather 出来
   （`_extract_layer`）再存，读回来 `_inject_layer` 散射回去。

### 13.2 方法映射（`KVConnectorBase_V1` → autumn 操作）

| 角色 | vLLM 方法 | autumn 落点 |
|---|---|---|
| scheduler | `get_num_new_matched_tokens` | 本地 prefix cache 已覆盖块对齐前缀就直接返回 `(0, False)`（不发远程探测）；否则算 `prefix_hash` 并探 `__present__` marker，命中返回 `(num_check - num_computed, False)` |
| scheduler | `update_state_after_alloc` | 记下该 req 分配到的 block id（权威 slot 来源，胜过从 `scheduled_new_reqs` 猜） |
| scheduler | `build_connector_meta` | 打包 `AutumnConnectorMetadata`；用 per-batch `scheduled_stores` 集合去重，否则同一冷前缀的并发请求会各存一份 |
| scheduler | `request_finished` | 有 in-flight save 就延迟释放 block，等 `get_finished` 报完成 |
| worker | `register_kv_caches` | 缓存每层张量引用 + 层序 |
| worker | `bind_connector_metadata` / `clear_connector_metadata` | 绑定本步元数据；**必须同时调 `super().bind_connector_metadata`**，否则 `has_connector_metadata()` 为假 → attention decorator 静默跳过每一次 `save_kv_layer`，而 `wait_for_save` 照样发 marker → marker 有、层没有 |
| worker | `start_load_kv` | 每层建一个 host staging buffer，**一次批量 `load_layers`**（`BatchClient.get_into`，UCX 上走 bulk）拉全部层，再逐层 `_inject_layer` 散射回 paged slot |
| worker | `wait_for_layer_load` | no-op——load 在 `start_load_kv` 里同步做完（逐层 overlap 未接） |
| worker | `get_block_ids_with_load_errors` | 返回并清空本步 load 失败的 block，让 vLLM 重跑 prefill |
| worker | `save_kv_layer` | **只做 GPU 侧 gather**（`_extract_layer` → 独立 GPU 张量，不同步 CPU），攒进 `_pending_saves` |
| worker | `wait_for_save` | **非阻塞**：把攒齐的 req 交给一个后台 job（CUDA event 等 gather 完 → D2H → dedup 探测 → `put_from` → 发 marker），prefill 关键路径上不付 D2H |
| worker | `get_finished` | 轮询后台 job 状态，返回已完成 save / load 的 req_id |

**load fail-closed**：请求是因为看到 `__present__` marker 才被准入的，所以任何一
层拉不到都**一个字节都不注入**，把它的 block 记进 `get_block_ids_with_load_errors`
让 vLLM 重算。只注入拉到的那部分 = 剩下的层是未初始化 paged KV = 静默输出错 token。

### 13.3 Key 格式（沿用 §5 命名空间）

```
wire key = kvc/{tenant_suffix}/vllm/{VLLM_KV_STORAGE_FORMAT}/{content_hash}/{layer_name}
```

- `vllm` 段就是 §5 的 `pool_name`（sglang 用 `kv`），两个框架的 keyspace 天然分流。
- **`content_hash` = `prefix_hash(token_ids, num_tokens, extra_keys)`**：connector
  自己对**整段块对齐 prompt 前缀**的 token id 做 sha256（每 token 4 字节 LE +
  `|{num_tokens}` + extra keys），不是逐 block 的 key。`extra_keys` **必须**包含
  一切"token id 之外会改变 KV 的上下文"（LoRA id、多模态输入哈希），否则同 token
  不同 LoRA/mm 的两个请求会 alias 到同一份 KV——这是正确性 bug。
- **`layer_name`** 是 vLLM 的层名；另有保留哨兵 `__present__`，标记"该前缀所有层
  都已存完"。scheduler 探的是**它**而不是某个真实层名——scheduler 不知道 worker
  侧的模型层命名（`register_kv_caches` 只在 worker 侧）。marker 在后台 save ACK
  之后才发布，所以它天然是 all-layers-durable 的提交点。
- **`model_fingerprint`（写进 `tenant_suffix`）**：`_identity.vllm_identity_sources`
  从 `VllmConfig` 提取模型真实身份做 12-hex sha256——架构形状（layers / hidden /
  kv_heads / head / vocab / model_type / dtype / quant / MLA）+ 权重来源
  （`load_format`，autumn loader 时再加 `model_loader_extra_config["path"]`）+
  可选 `kv_connector_extra_config["model_id"]`。vLLM 路径上这个指纹是**必需**的：
  共享一个本地 config 目录的部署把 config/tokenizer 钉死在固定路径，`model_config.model`
  对所有这样 serve 的模型都是同一个字符串，没有指纹就会两个模型共用一个 tenant
  互相串读 KV（层数不同时是可见告警，同形状模型则是**无声**错误）。指纹跨进程
  确定（同部署 → 同 tenant，否则缓存永 miss）。
  **残留撞车场景**：同架构 + 同权重身份源（同本地路径的两个微调、或在同一 autumn
  path 原地覆写权重）→ 用 `model_id` 显式区分 / 新权重放新 path。
- **`VLLM_KV_STORAGE_FORMAT`（`_keys.py`）**：connector 自己的 KV 布局版本，既烧进
  key 路径又折进 tenant 指纹（双重失配）。指纹里另折入**运行中的 vLLM 完整版本号**
  ——KV page 布局是 vLLM 内部实现细节、无稳定性契约，patch 版也可能改（取不到时
  loud warning + 降级）。
  **INVARIANT：改 `_extract_layer` / `_inject_layer` / `_byte_view` / key 组成方式
  必须 bump `VLLM_KV_STORAGE_FORMAT`**；不 bump = 形状可能仍对得上、什么都不报错、
  输出静默是垃圾。`test_tenant_identity.py` 钉住当前值，让 bump 永远是一次自觉的、
  被 review 的动作（它会冷失效整个 vLLM pool）。
  运维后果：**任何 vLLM 版本变动都冷失效整个 vLLM pool**（见 ops.md）。粒度取完整
  版本是有意的——升级本就重启 pod（GPU cache 必丢），重暖是一次性可预期代价；漏挡
  一次布局变化 = 无声垃圾。
- 内容寻址 ⇒ **永不需要 invalidation**（同 §9）；`ttl_secs=0` 的旧 key 要回收空间
  得手动 `batch_delete`。

### 13.4 buffer / GPU staging

- **CPU-offload KV**：paged block 直接是 host buffer-protocol 对象 → 直接走 §8 的
  `get_into` / `put_from`。
- **GPU 驻留 KV**：autumn client 在 host 侧，必须过 host staging。
  - load：每层 `np.empty(nbytes, uint8)` staging → 一次批量 `load_layers` →
    `torch.from_numpy(staging).view(dtype).reshape(shape)` → `_inject_layer`。
  - save：`save_kv_layer` 只在 GPU 上 gather 出**独立**张量（与 paged block 解耦，
    block 可以立刻复用），D2H `.cpu()` 推迟到后台 job——那才是会 stall prefill 的
    那一步。这些临时 GPU 张量的峰值被 vLLM 自己的 token budget 界住
    （一步内的 store req ≤ `max_num_batched_tokens` × per-token KV），不随 prompt
    长度或 QPS 增长。

### 13.5 同步 / 异步桥接

vLLM worker 从 model-runner 线程**同步**调 connector 方法，`get_finished` 是轮询。
数据面走 `BatchClient`（同步、GIL-releasing）；`_bridge.py` 的 asyncio loop 只用于
marker 的 `head`/`put` 这类低频单 key 操作（§7）。

### 13.6 启动方式

```bash
vllm serve <model> \
  --kv-transfer-config '{
    "kv_connector":"AutumnKVConnector",
    "kv_connector_module_path":"autumn_kvcache.vllm_connector",
    "kv_role":"kv_both",
    "kv_connector_extra_config":{"endpoint":"manager:9001"}
  }'
```

### 13.7 性能口径

- **prefix cache hit rate**：跟 vLLM 内置 `--enable-prefix-caching`（仅本地）对比
  跨实例 / 重启命中。
- **GPU staging 拷贝开销**：GPU-KV 情形量化 device↔host 拷贝占比。
- partition QPS 仍受 [[project_partition_qps_ceiling]] 约束；page/block size 覆盖
  64KB/256KB/1MB（[[feedback_perf_check_matrix]]）。
