# autumn-kvcache 设计与实施计划

**日期**: 2026-05-19（2026-06-01 增补 §13 vLLM `KVConnectorV1` Phase 3 细化设计）
**状态**: 设计已收敛，待 feature 立项实施（sglang Phase 1 / vLLM Phase 3 均已细化）
**关联文档**: [hicache_l3_interface.md](./hicache_l3_interface.md)

---

## 1. 目标

为 sglang 的 HiCache L3 storage backend 提供 autumn-rs 接入实现，让 sglang 推理集群把它的"溢出 KV cache"持久化到 autumn partition 层。

**autumn-kvcache 是 [[project_three_interfaces]] 里"partition 之上三个接口之一"**，与 autumn-fuse、autumn-client 并列，本质是一个 sglang-friendly façade。

## 2. 部署假设

部署形态（**这是设计的前提，不要绕开这一条做设计**）：

- **多节点**集群
- **每节点一个 sglang 实例**（一卡一进程也罢、TP 内部多卡也罢，外部只暴露一个进程）
- **单个 sglang 实例可以跨多节点**（TP/PP/EP 跨机），每个节点上是一个 worker rank
- autumn partition 集群独立部署，跟 sglang 节点可能同机也可能不同机

直接推论：
- 同节点没有多 sglang 进程共享需求 → **不需要 daemon + 本地 DRAM cache 这一层**
- 跨节点 sglang 共享靠 partition（partition memtable + block cache 已经在 RAM）
- 跨实例 / 重启复用同样靠 partition

## 3. 架构

### 3.1 最终形态

```
┌────────────────────────────────────────────────┐
│  sglang worker (一个 tp_rank, 一个节点)          │
│  ├─ L1 GPU HBM                                 │
│  ├─ L2 HostKVCache (pinned host DRAM)          │
│  └─ HiCacheController                          │
│     ├─ prefetch_thread / backup_thread          │
│     └─ AutumnKVCacheStorage  (Python class)    │
│        └─ autumn (PyO3 binding)                │
└────────────────┬───────────────────────────────┘
                 │ partition gRPC (UCX/TCP)
┌────────────────▼───────────────────────────────┐
│  autumn partition layer (autumn-ps cluster)     │
│  ├─ memtable + block cache  ← 隐式 DRAM cache   │
│  └─ stream layer (持久化)                       │
└────────────────────────────────────────────────┘
```

**autumn-kvcache 全部代码 = 一个 Python 包 + autumn PyO3 binding 的补丁**。没有 Rust daemon，没有 sidecar 进程，没有独立 LRU。

### 3.2 不做哪些事（design rationale）

| 否定项 | 否决理由 |
|---|---|
| sidecar Rust daemon | 单 sglang per node，daemon 服务的客户端只有一个，跟 sglang 自己的 L2 平级，没有跨进程共享需求 |
| 本地 DRAM LRU (L3a) | 跟 L2 重复；要 DRAM 不如直接配给 sglang `hicache_ratio` |
| daemon-to-daemon peer mesh / RDMA cache mesh | partition 已经是分布式 KV；再做 peer mesh 违反 [[feedback_no_parallel_data_plane]] |
| 本地 mmap slot file (3FS 风格) | partition 已经是持久层；再加本地 NVMe slot 等于自建并行数据面 |
| 参与 sglang PD-disaggregation 传输 | 那是 sglang transfer engine 的职责（mooncake transfer engine / NIXL），HiCache L3 接口不覆盖 |

## 4. 接口契约

实现 sglang `HiCacheStorage` 的 **v1 子集**（zero-copy single-pool）。

### 4.1 必须实现

```python
class AutumnKVCacheStorage(HiCacheStorage):
    def __init__(self, storage_config: HiCacheStorageConfig, extra_config: dict): ...
    def register_mem_pool_host(self, mem_pool_host: HostKVCache): ...
    def batch_get_v1(self, keys, host_indices, extra_info=None) -> List[bool]: ...
    def batch_set_v1(self, keys, host_indices, extra_info=None) -> List[bool]: ...
    def batch_exists(self, keys, extra_info=None) -> int:  # contiguous-prefix int!
    def get_stats(self): ...
```

### 4.2 v0 abstract methods（必须实现，但不在热路径）

```python
def get(...): ...    # thin wrapper, 单次走 v1
def batch_get(...): ...
def set(...): ...
def batch_set(...): ...
def exists(...): ...
```

`extra_config["interface_v1"] = 1` 后 sglang 的 cache_controller 走 v1 路径（docs:142–151），v0 方法只在 debug / 边角场景命中。

### 4.3 v2 multi-pool 不做

MVP 阶段只支持 KV pool。hybrid attention 模型（Mamba / SWA / DeepSeek-V4）支持留 **Phase 2+**。

### 4.4 Optional

`clear()` 路由到 `autumn-client` 的 range delete (按 tenant suffix 前缀清理)，**仅作 debug 用**。

## 5. Key 格式

```
autumn-kvcache key = f"kvc/{tenant_suffix}/{pool_name}/{sha256_hex}"
```

| 字段 | 来源 | 备注 |
|---|---|---|
| `kvc/` | 固定前缀 | 跟其它 autumn 接口（fuse / 普通 client）分命名空间 |
| `tenant_suffix` | `(model_name, tp_rank, tp_size, pp_rank, pp_size, is_mla_model)` | 抄 `HiCacheFile._get_suffixed_key`；MLA 不带 tp 后缀 |
| `pool_name` | 当前固定 `"kv"` | **为 v2 预留位置，避免未来 key 迁移** |
| `sha256_hex` | sglang controller 已计算好的 chain hash | docs:190–200；backend 不重新 hash |

留 `pool_name` 即使 v1 阶段不用，是 [[feedback_state_machine_not_bool]] 的同源原则：keyspace 结构一旦定型很难改，宁可现在多 13 字节也别将来做迁移。

## 6. 数据流

### 6.1 读路径 (`batch_get_v1`)

```python
def batch_get_v1(self, keys, host_indices, extra_info=None):
    results = []
    for k, idx in zip(keys, host_indices):
        ptr, size = self.mem_pool_host.get_data_page(idx, flat=True)
        # PyO3 binding 把 partition value 直接写到 pinned host pool, 不经 Python bytes
        ok = self.client.get_into(self._full_key(k), ptr, size)
        results.append(ok)
    return results
```

partition `get` 命中 → 直接 memcpy 到 sglang pinned host page，**零中间拷贝**。

未命中 → 返回 `False`，sglang 按"没找到这一段 prefix"处理，跳过此 page 起重新计算。

### 6.2 写路径 (`batch_set_v1`)

write-through 给 partition；ACK 后才返回 True。

```python
def batch_set_v1(self, keys, host_indices, extra_info=None):
    results = []
    for k, idx in zip(keys, host_indices):
        ptr, size = self.mem_pool_host.get_data_page(idx, flat=True)
        ok = self.client.put_from(self._full_key(k), ptr, size)
        results.append(ok)
    return results
```

不在 backend 内做异步 buffer——sglang `write_through` 默认就期望 ACK 之后才算落盘（[[feedback_two_phase_commit]]）。如果 `set` 因为 partition 慢顶到 prefetch budget（2s + 0.1s/Kitok，docs:224），那是 partition 性能问题，不该靠"本地 ACK 先回"掩盖。

### 6.3 存在性查询 (`batch_exists`)

返回 **contiguous-prefix int**（最长连续存在前缀长度），不是 per-key list（docs:62–64）。

```python
def batch_exists(self, keys, extra_info=None) -> int:
    # 顺序检查每个 key，第一个不存在的位置就是返回值
    for i, k in enumerate(keys):
        if not self.client.head(self._full_key(k)):
            return i
    return len(keys)
```

后续若 autumn-client 支持 `batch_head`，把 N 次 RPC 合成一次。

## 7. 同步 / 异步桥接

sglang `HiCacheController` 的 prefetch_thread / backup_thread 是**同步阻塞调用** backend（docs:220）。现有 `autumn` Python binding 是 async（返回 future）。

**Phase 1**: 在 `AutumnKVCacheStorage` 内部维护一个专用 asyncio event loop（独立线程），同步方法把 future 提交进去等结果：

```python
class AutumnKVCacheStorage:
    def __init__(self, ...):
        self._loop = asyncio.new_event_loop()
        threading.Thread(target=self._loop.run_forever, daemon=True).start()
        self.client = asyncio.run_coroutine_threadsafe(
            autumn.Client.connect(endpoint), self._loop
        ).result()

    def _await(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()
```

**Phase 2**: 给 `autumn` binding 加同步入口（封 `block_on` 在 PyO3 层），省掉 event loop 一跳。能进 partition I/O 的延迟省一点。

## 8. Zero-copy buffer API（autumn Python binding 改动）

现有 binding（`python/src/lib.rs`）：

```rust
fn put<'py>(&self, py, key: &[u8], value: &[u8]) -> ...    // 拷贝
fn get<'py>(&self, py, key: &[u8]) -> ...                   // 返回 Python bytes
```

要支持 sglang pinned host pool 的 zero-copy，需要新增 buffer-protocol API：

```rust
fn put_from<'py>(&self, py, key: &[u8], buf: PyBuffer<u8>) -> ...
fn get_into<'py>(&self, py, key: &[u8], buf: PyBuffer<u8>) -> ...  // PyBuffer 必须可写
```

- `PyBuffer<u8>` 接收 PyO3 buffer protocol 对象（numpy / torch tensor / mmap）
- `buf.as_ptr()` 直接拿到 `*mut u8`，加 `buf.len_bytes()` 调底层 `ClusterClient.put_raw / get_raw`
- 调用方保证 buffer 生命周期覆盖 await（同步桥接已经满足）

**这是这个 plan 里唯一实质性的 Rust 工作**，估计 ~150 行（包括 ClusterClient 侧的 zero-copy 落点支持，如果当前是 Vec<u8> 进 Vec<u8> 出，可能要顺手加一层 `into_buf` / `from_buf` 路径）。

## 9. Crash 语义

| 谁崩 | 后果 |
|---|---|
| sglang worker | `HiCacheController` 重启会丢 in-flight host_indices；autumn 这边无状态，无影响 |
| autumn-kvcache (Python adapter) | 它没有独立状态——它就是 sglang 进程的一部分。sglang 崩 = adapter 崩。 |
| autumn-client (PyO3) | 同上 |
| autumn partition / extent-node | 走现有 stream layer 恢复（owner-lock fencing）；adapter 收到错误 → `batch_get_v1` 返回 `[False]`，sglang 按 cache miss 处理 |

由于 key 是 SHA-256 chain hash 内容寻址（docs:190–200），**永远不需要 invalidation**。partition 是唯一真相源。

## 10. Phase / 里程碑

### Phase 1 (MVP, 单 feature)
- [ ] `python/autumn_kvcache/` 包骨架（`pyproject.toml` extra package 或单独安装）
- [ ] `class AutumnKVCacheStorage(HiCacheStorage)` v1 实现（batch_get_v1 / batch_set_v1 / batch_exists / register_mem_pool_host）
- [ ] v0 abstract methods 作为 thin wrapper
- [ ] tenant key suffix（照搬 `HiCacheFile._get_suffixed_key`）
- [ ] 同步桥接（独立 asyncio loop in thread）
- [ ] `autumn` binding 新增 `put_from(buf)` / `get_into(buf)` zero-copy 入口
- [ ] sglang `extra_config` 文档（README 更新）
- [ ] 集成测试：启动 manager + extent-node + ps，跑 sglang `--enable-hierarchical-cache --hicache-storage-backend dynamic --hicache-storage-backend-extra-config '{...}'`，验证 prefix cache hit

### Phase 2（按需）
- [ ] 同步 PyO3 入口（去掉 event loop 一跳）
- [ ] `batch_head` / `batch_put` autumn-client RPC（减少 N 次 RPC 到 1 次）
- [ ] `get_stats()` 暴露指标
- [ ] 接 [[feedback_perf_check_matrix]]：sglang prefix-heavy workload benchmark

### Phase 3+（明确 non-goals 直到外部需求出现）
- [ ] vLLM `KVConnectorV1` 适配器（同 daemon-less 形态，复用 autumn-client）—— **route A 选定，细化设计见 §13**
- [ ] HiCache v2 multi-pool 支持（mamba / swa pool，hybrid 模型）
- [ ] 本地 DRAM cache / sidecar daemon（仅在 benchmark 证明 partition 延迟成为瓶颈时回头加）

## 11. 性能验证

启动命令：
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

性能指标：
- **prefix cache hit rate**：跟 baseline (`--hicache-storage-backend file`) 对比 hit rate
- **L3 lookup latency P50/P99**：必须 << prefetch budget (2s + 0.1s/Kitok)
- **L3 write latency**：在 write_through 模式下 worst-case bound
- **partition QPS**：单 partition 不能超 [[project_partition_qps_ceiling]] 30K QPS；超了说明该 split 或该按 tenant_suffix 分库

按 [[feedback_perf_check_matrix]]：transport × partitions{8+} × depth{8} × size{4K, 8M}。**page size 由模型决定**，不在 perf matrix 里，但要在 benchmark 里覆盖典型 page 大小（64KB、256KB、1MB）。

## 12. 现状盘点

- [x] `python/autumn` PyO3 binding 已存在（async `Client.connect/put/get/delete/range`）
- [ ] zero-copy buffer protocol API
- [ ] 同步入口
- [ ] `autumn_kvcache` 子包
- [ ] sglang 集成测试 harness

按 CLAUDE.md "长任务执行规则"，Phase 1 立项时进 `feature_list.md`，开发期间维护 `claude-progress.txt`，完成后更新 `autumn-rs/README.md` 人工验证步骤。

---

## 13. vLLM `KVConnectorV1` 适配器（Phase 3 细化设计）

**日期补充**: 2026-06-01 · **路线**: route A（原生 connector，daemon-less，复用 autumn-client）

### 13.0 选型结论：为什么 route A

| 路线 | 形态 | 取舍 |
|---|---|---|
| **A 原生 `KVConnectorBase_V1`**（选定） | vLLM → `AutumnKVConnector` → `autumn.BatchClient` | daemon-less、零额外依赖、守住"无本地 cache"原则、数据面与 sglang 路线零改动。代价：自己实现 connector 生命周期（scheduler/worker 双角色 + 逐 layer overlap）。 |
| B 经 LMCache 中转 | vLLM → `LMCacheConnectorV1` → autumn（LMCache remote backend） | 白嫖 LMCache 成熟集成，但引入 LMCache **自带本地 DRAM/GPU cache**，违反 [[project_autumn_kvcache_architecture]] 的"无本地 DRAM cache"，且多一跳。仅当原生 connector 跟 vLLM 版本演进的维护成本过高时回退。 |

**铁律不变**：持久化只走 partition（[[feedback_no_parallel_data_plane]]）；adapter 无 daemon、无本地 LRU、无并行数据面。connector 只是 vLLM 的"插座"，下面那条数据通路（`autumn.BatchClient`）和 sglang 路线完全是同一条。

### 13.1 与 sglang HiCache L3 的本质差异

sglang HiCache L3 是个**同步 storage backend**（`batch_get_v1/batch_set_v1/batch_exists`，§4）。vLLM 的 `KVConnectorV1` 是**嵌进 forward pass 的连接器**，三点不同决定了 adapter 比 sglang 重：

1. **scheduler / worker 双角色**：connector 在 scheduler 进程和每个 worker 进程**都实例化**（`__init__(vllm_config, role)`，role ∈ {`SCHEDULER`,`WORKER`}）。scheduler 侧做前缀匹配 + 决定 load/save 集合，worker 侧做实际 I/O，中间靠一份可序列化的 `KVConnectorMetadata` 传递。
2. **layer 粒度 + 与计算 overlap**：不是"一批 page 一次走完"，而是 `start_load_kv` 起异步加载、逐层 `wait_for_layer_load(layer)` 同步，把 autumn RPC 藏在前面层的 attention 计算后。
3. **paged KV block 布局 + vLLM 自己的 block hash**：key 方案要对齐 vLLM 的 prefix-block 哈希（内容寻址），不自创（[[project_autumn_kvcache_targets_hicache]] 原则在 vLLM 同样适用）。

### 13.2 方法映射（`KVConnectorBase_V1` → autumn 操作）

| 角色 | vLLM 方法 | 语义 | autumn 落点 |
|---|---|---|---|
| scheduler | `get_num_new_matched_tokens(req, num_computed)` → `(int, bool)` | 报告该 req 前缀有多少 token 已在外部 cache（决定跳过多少 prefill） | 由 token ids 算 block hash → `batch_exists`（contiguous-prefix）→ `matched_blocks × block_size`；第二元 `bool` = 是否异步加载（autumn 走异步 → `True`） |
| scheduler | `update_state_after_alloc(req, blocks, num_external)` | KV block 分配后，记录哪些 block 要从外部加载 | 记 `(req_id, block_hash[], block_id[])` 进 connector 内部待 load 表 |
| scheduler | `build_connector_meta(scheduler_output)` → `KVConnectorMetadata` | 把本步要 load/save 的 (req, block) 集合打包给 worker | 序列化 `AutumnConnectorMeta{loads:[(req,hash,block_id)], saves:[...]}` |
| scheduler | `request_finished(req, block_ids)` → `(bool, dict?)` | req 结束时，是否延迟释放 block（等异步 save 落盘） | 若该 req 有 in-flight save → 返回 `(True, None)`，`get_finished` 报完成后再放 |
| worker | `register_kv_caches(kv_caches: dict[layer→Tensor])` | 给 connector worker 侧的 paged KV cache 张量 | 缓存每层张量引用；若 GPU 驻留则准备 pinned host staging buffer（ZC 落点，§13.4） |
| worker | `bind_connector_metadata(meta)` / `clear_connector_metadata()` | 收/清本步元数据 | 反序列化 `AutumnConnectorMeta`，驱动本步 load/save |
| worker | `start_load_kv(forward_context)` | 起异步加载本步所有待 load block | 对每 (block,layer) 发批量 `get_into(key, host_buf)`；future 按 layer 归桶 |
| worker | `wait_for_layer_load(layer_name)` | 阻塞到该层加载完（overlap 点） | join 该层 future 桶；命中后 host→GPU 拷回 paged slot（GPU 情形） |
| worker | `save_kv_layer(layer, kv_layer, attn_meta)` | 异步保存该层新 block | GPU→host stage（GPU 情形）→ `put_from(key, host_buf)`；future 入 save 集合 |
| worker | `wait_for_save()` | 阻塞到本步所有 save 落盘 | join 全部 save future（write-through，ACK 才算落盘，[[feedback_two_phase_commit]]） |
| worker | `get_finished(finished_req_ids)` → `(sent, recv)` | 上报哪些异步 send/recv 完成 | 轮询 future 状态，返回已完成 save / load 的 req_id 集合 |

### 13.3 Key 格式（沿用 §5 命名空间，换 v1 子段）

```
autumn-kvcache (vllm) key = f"kvc/{tenant_suffix}/vllm/{block_hash_hex}/{layer_id}"
```

- `kvc/` 固定前缀、`tenant_suffix` 由 `(model, tp_rank, tp_size, pp_rank, pp_size)` 拼（与 sglang 同源，§5）。
- `vllm` 段对应 §5 的 `pool_name` 预留位（sglang 用 `kv`），天然分流两个框架的 keyspace。
- `block_hash_hex`：**取 vLLM 自己算好的 block hash**（`BlockHash`，token-ids + LoRA/mm extra key 内容寻址），backend 不重算。
- `/{layer_id}`：因为 `save_kv_layer` 是逐层的，按 (block,layer) 拆 key 最贴合 overlap 模型（代价是 num_layers× key 数；若 RPC 数成瓶颈，Phase B 再合并为整 block 单 key + 内部 layer offset，先留结构不优化）。
- 内容寻址 ⇒ **永不需要 invalidation**（同 §9）。

### 13.4 buffer / zero-copy（复用 §8，多一段 GPU staging）

- **CPU-offload KV**（vLLM `--kv-cache-dtype` 在 host）：paged block 直接是 host buffer-protocol 对象 → 直接 `get_into/put_from`（§8 的 `PyBuffer<u8>` 入口），≥64K 走 UCX `MSG_GET_ZC/PUT_ZC`，零中间拷贝。
- **GPU 驻留 KV**：autumn client 在 host 侧，必须经 **pinned host staging buffer**：load = `get_into(pinned)` → `cudaMemcpyAsync` host→device；save = device→host `cudaMemcpyAsync` → `put_from(pinned)`。staging buffer 在 `register_kv_caches` 时按 (block_bytes × 并发) 预分配并 `ibv_reg_mr`（ZC 复用 [[feedback_zc_recv_no_leak_on_cancel]] 的 regpool 思路）。这一段 GPU↔host 拷贝是 vLLM 路线相对 sglang 多出来的唯一数据搬运。

### 13.5 同步 / 异步桥接（直接复用 `_bridge.py`）

vLLM worker 从 model-runner 线程**同步**调 connector 方法；`get_finished` 是轮询。现有 `_bridge.py`（独立 asyncio loop 线程 + `run_coroutine_threadsafe`）原样复用：
- `start_load_kv` / `save_kv_layer` 把批量 coroutine 提交进 loop，**不阻塞**（返回 future 句柄存桶里）。
- `wait_for_layer_load` / `wait_for_save` 才 `.result()` 阻塞。
- 这样 RPC 真正与 GPU 计算 overlap（提交即返回，等待点延后到层边界）。

### 13.6 复用 vs 新增（工作量盘点）

**直接复用（零改动）**：`autumn.BatchClient`（数据面）、`_bridge.py`（同步↔异步）、tenant key suffix 逻辑、§8 的 `put_from/get_into` ZC 入口、crash 语义（§9，adapter 无状态、内容寻址）。

**新增（route A 的实际工作）**：
- [ ] `python/autumn_kvcache/autumn_kvcache/vllm_connector.py` —— `class AutumnKVConnector(KVConnectorBase_V1)`，与 `sglang_backend.py` 平级。
- [ ] scheduler/worker 双角色 + `AutumnConnectorMeta` 序列化。
- [ ] 逐 layer future 归桶（`{layer → [future]}`）+ `wait_for_layer_load` 同步。
- [ ] GPU staging buffer 池（仅 GPU-KV 情形）+ `cudaMemcpyAsync` 双向。
- [ ] `get_num_new_matched_tokens` 的 block-hash 计算对齐 vLLM（读 vLLM `BlockHash` 而非自算）。
- [ ] vLLM 版本钉死（`KVConnectorBase_V1` 签名随版本漂移；CI 锁一个 vLLM 版本，是 route A 主要维护负担——也是 route B 存在的唯一理由）。

### 13.7 启动方式（目标形态）

```bash
vllm serve <model> \
  --kv-transfer-config '{
    "kv_connector":"AutumnKVConnector",
    "kv_connector_module_path":"autumn_kvcache.vllm_connector",
    "kv_role":"kv_both",
    "kv_connector_extra_config":{"endpoint":"manager:9001"}
  }'
```

### 13.8 性能 / 验证（沿用 §11 口径）

- **prefix cache hit rate**：跟 vLLM 内置 `--enable-prefix-caching`（仅本地）对比跨实例/重启命中。
- **per-layer load latency P99 << 层间计算时间**：否则 overlap 失效、`wait_for_layer_load` 成为串行墙。
- **GPU staging 拷贝开销**：GPU-KV 情形量化 device↔host 拷贝占比；若显著，评估 GPUDirect-Storage 风格直读（Phase 3 之后再议，先不做）。
- partition QPS 仍受 [[project_partition_qps_ceiling]] 约束；page/block size 覆盖 64KB/256KB/1MB（[[feedback_perf_check_matrix]]）。

### 13.9 Phase 划分（vLLM 子线）

- **Phase 3a**：CPU-offload KV 路径（无 GPU staging）跑通 —— 最小可用，验证 connector 生命周期 + hit rate。
- **Phase 3b**：GPU-KV staging buffer 池 + `cudaMemcpyAsync` 双向 overlap。
- **Phase 3c**：(block,layer) → 整 block 单 key 合并（若 RPC 数成瓶颈）；`get_stats` 指标。

立项时同 §10 进 `feature_list.md`，维护 `claude-progress.txt`。
