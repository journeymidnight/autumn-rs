# sglang HiCache L3 Storage Backend — autumn-kvcache 集成参考

**调研日期**: 2026-05-19
**目标**: 设计 `autumn-kvcache` 作为 sglang HiCache L3 storage backend 的实现输入
**sglang snapshot**: `sgl-project/sglang` `main` 分支 (commit `66ef97c` 之后)

---

## 1. HiCache 在 sglang 源码中的位置

| 关注点 | 路径 |
|---|---|
| 抽象基类 + 内置 `HiCacheFile` backend | `python/sglang/srt/mem_cache/hicache_storage.py` |
| Backend 注册/工厂 | `python/sglang/srt/mem_cache/storage/backend_factory.py` |
| 内置 backend 实现目录 | `python/sglang/srt/mem_cache/storage/{lmcache, mooncake_store, hf3fs, nixl, eic, simm, aibrix_kvcache}` |
| Controller (驱动 prefetch/backup) | `python/sglang/srt/managers/cache_controller.py` |
| L1/L2/L3 集成的 radix tree | `python/sglang/srt/mem_cache/hiradix_cache.py` |
| key hash 函数 | `python/sglang/srt/mem_cache/utils.py` (`get_hash_str`) |
| CLI flags | `python/sglang/srt/server_args.py` (`hicache_*` 字段) |

---

## 2. L3 Backend 接口 — `HiCacheStorage`

三套 API 并存（v0 / v1 / v2），backend 实现哪个子集决定它走哪条 dispatch 路径。

### 2.1 Config dataclass

```python
@dataclass
class HiCacheStorageConfig:
    tp_rank: int
    tp_size: int
    pp_rank: int
    pp_size: int
    attn_cp_rank: int
    attn_cp_size: int
    is_mla_model: bool
    enable_storage_metrics: bool
    is_page_first_layout: bool
    model_name: Optional[str]
    tp_lcm_size: Optional[int] = None
    should_split_heads: bool = False
    extra_config: Optional[dict] = None
```

### 2.2 v0 (legacy, abstract methods 必须实现)

```python
class HiCacheStorage(ABC):
    @abstractmethod
    def get(self, key: str, target_location=None, target_sizes=None) -> torch.Tensor | None: ...
    @abstractmethod
    def batch_get(self, keys, target_locations=None, target_sizes=None) -> List[torch.Tensor | None] | int: ...
    @abstractmethod
    def set(self, key: str, value=None, target_location=None, target_sizes=None) -> bool: ...
    @abstractmethod
    def batch_set(self, keys, values=None, target_locations=None, target_sizes=None) -> bool: ...
    @abstractmethod
    def exists(self, key: str) -> bool: ...

    def batch_exists(self, keys, extra_info=None) -> int:
        """返回最长连续存在前缀长度 (不是 per-key 列表！)"""
```

### 2.3 v1 (zero-copy single-pool，**autumn-kvcache 主路径**)

```python
def batch_get_v1(self, keys: List[str],
                 host_indices: torch.Tensor,
                 extra_info: Optional[HiCacheStorageExtraInfo] = None) -> List[bool]: ...

def batch_set_v1(self, keys: List[str],
                 host_indices: torch.Tensor,
                 extra_info: Optional[HiCacheStorageExtraInfo] = None) -> List[bool]: ...
```

`host_indices` = int64 `torch.Tensor` of page indices into 已注册的 host KV pool。Backend 启动时通过 `register_mem_pool_host(mem_pool_host)` 拿到 pinned-host pool 的基址和 stride，每次调用就用 index 算出 `(ptr, size)`。

### 2.4 v2 (multi-pool，hybrid 模型用 — Mamba / SWA / DeepSeek-V4)

```python
@dataclass
class PoolTransfer:
    name: PoolName               # "kv" | "mamba" | "swa" | "indexer" | ...
    host_indices: Optional[torch.Tensor]
    device_indices: Optional[torch.Tensor]
    keys: Optional[List[str]]
    hit_policy: PoolHitPolicy = PoolHitPolicy.ALL_PAGES   # 或 TRAILING_PAGES
    nodes_to_load: Optional[List[Any]] = None
    indices_from_pool: Optional[PoolName] = None

def batch_exists_v2(self, keys, pool_transfers=None, extra_info=None) -> PoolTransferResult: ...
def batch_get_v2(self, transfers: List[PoolTransfer], extra_info=None) -> dict[str, List[bool]]: ...
def batch_set_v2(self, transfers: List[PoolTransfer], extra_info=None) -> dict[str, List[bool]]: ...
```

MVP 阶段**只支持 `KV` pool**，hybrid 模型支持留 Phase 2+。

### 2.5 Optional

```python
def clear(self) -> None: ...
def get_stats(self): return None
def register_mem_pool_host(self, mem_pool_host: HostKVCache): ...
```

---

## 3. Backend 注册与加载

`backend_factory.py` 内置注册：

```python
StorageBackendFactory.register_backend("file", "...hicache_storage", "HiCacheFile")
StorageBackendFactory.register_backend("nixl", "...nixl.hicache_nixl", "HiCacheNixl")
StorageBackendFactory.register_backend("mooncake", "...mooncake_store.mooncake_store", "MooncakeStore")
StorageBackendFactory.register_backend("hf3fs", "...hf3fs.storage_hf3fs", "HiCacheHF3FS")
StorageBackendFactory.register_backend("aibrix", "...aibrix_kvcache.aibrix_kvcache_storage", "AibrixKVCacheStorage")
StorageBackendFactory.register_backend("eic", "...eic.eic_storage", "EICStorage")
StorageBackendFactory.register_backend("simm", "...simm.hicache_simm", "HiCacheSiMM")
```

### 推荐方案：`dynamic` backend（**无需改 sglang 源码**）

```bash
sglang ... \
  --enable-hierarchical-cache \
  --hicache-storage-backend dynamic \
  --hicache-storage-backend-extra-config '{
    "backend_name": "autumn",
    "module_path":  "autumn_kvcache.sglang_backend",
    "class_name":   "AutumnKVCacheStorage",
    "interface_v1": 1,
    "endpoint":     "manager_addr:9001"
  }'
```

工厂会 `backend_class(storage_config, extra_kwargs)`，所以 `__init__` 必须接受这个签名。

### `interface_v1` 开关的关键作用

`cache_controller.py` 里：

```python
if (self.storage_backend_type in ["hf3fs","mooncake","eic","nixl","simm"]) or (
    self.storage_backend_type == "dynamic"
    and bool(self.storage_config.extra_config.get("interface_v1", 0))):
    self.page_get_func = self._page_get_zero_copy   # → batch_get_v1
    self.page_set_func = self._page_set_zero_copy   # → batch_set_v1
```

**autumn-kvcache 必须在 extra_config 设 `"interface_v1": 1`**，否则会走 v0 batch_get/batch_set 慢路径。

### Server args 相关 flag

```python
enable_hierarchical_cache: bool = False
hicache_ratio: float = 2.0
hicache_size: int = 0
hicache_write_policy: str = "write_through"      # 或 write_back, write_through_selective
hicache_io_backend: str = "kernel"
hicache_mem_layout: str = "layer_first"           # 或 page_first / page_first_direct
hicache_storage_backend: Optional[str] = None
hicache_storage_prefetch_policy: str = "timeout"
hicache_storage_backend_extra_config: Optional[str] = None
```

---

## 4. Block 布局

- **粒度** = 一个 page = `page_size` 个 token 的 KV state
- **Backend 永远看 CPU host memory** —— 不直接接触 GPU memory。v1/v2 拿 `host_indices`，通过 `mem_pool_host.get_data_page(offset, flat=True)` 解析为 pinned-host `(ptr, size)`
- **dtype/shape** 跟模型 KV cache dtype 一致（fp16/bf16/fp8）
- **bytes_per_page** 计算（工厂代码）：
  ```python
  if layout in ["page_first", "page_first_direct"]:
      bytes_per_page = mem_pool_host.get_ksize_per_token() * page_size
  elif layout == "layer_first":
      bytes_per_page = mem_pool_host.get_size_per_token() * page_size
  ```
- **`is_page_first_layout`** 在 `storage_config` 里告诉 backend 当前布局
- **MLA vs MHA**：MLA 用单一 bundle key；MHA Mooncake 把 K/V 分成 `_k`/`_v` 后缀两个对象

---

## 5. Key 格式

**SHA-256 hex digest (64 字符 ASCII)**，按 page 链式 hash：

```python
def get_hash_str(token_ids: List[int], prior_hash: Optional[str] = None) -> str:
    hasher = hashlib.sha256()
    if prior_hash:
        hasher.update(bytes.fromhex(prior_hash))
    for t in token_ids:
        hasher.update(t.to_bytes(4, byteorder="little", signed=False))
    return hasher.hexdigest()
```

- **Backend 不 hash**，sglang controller 已经算好传过来
- Merkle 链式 → 所以 `batch_exists` 返回"最长连续存在前缀长度"语义自然成立
- **Backend 必须再加 tenant suffix** 区分模型/TP/PP/MLA（照搬 `HiCacheFile._get_suffixed_key`）:
  ```python
  config_suffix = f"_{model_name}"
  if not is_mla_model: config_suffix += f"_{tp_rank}_{tp_size}"
  if enable_pp:        config_suffix += f"_{pp_size}_{pp_rank}"
  ```
- 线上 key 类型 = `str`

---

## 6. 并发模型

- 全部调用在 **HiCacheController 的 daemon 线程** 中发生：
  - `prefetch_thread`
  - `backup_thread`
  - `prefetch_io_aux_thread`
- **同步 Python 调用** — 不是 async；并发来自 sglang 多个 in-flight operation + backend 内部 worker pool
- **每个 op 都是 batched** —— `_page_transfer` 把 `hash_value` 按 `storage_batch_size` 切片，每片一次 `batch_get_v1`/`batch_set_v1`
- **prefetch 和 backup 线程独立** —— 读写可并发
- **TP 协调**：sglang 在 attention TP/CP group 内 all-reduce hit 数；backend 不需要处理分布式，只接收自己的 `tp_rank/tp_size` 用于 keyspace 隔离
- **Prefetch timeout 是 token 数线性**（`PrefetchTimeoutConfig`：2s base + 0.1s/Ki tokens, 上限 30s）—— **慢 backend 会被 sglang 直接放弃**，host_indices 会被回收，backend 应快速返回 `False` 而不是阻塞

---

## 7. 驱逐语义

- **L3 自管容量** —— sglang **永远不会调 delete/evict**，只有 `clear()` (全清，debug only)
- Write policy: `write_through` (默认) / `write_back` / `write_through_selective`
- 同 key 重复 `set` 必须幂等（`HiCacheFile.set` 用 `self.exists(key)` 短路）
- **autumn-kvcache 含义**：必须自己实现 LRU/TTL + 固定容量，`get_stats()` 暴露指标。GC 由 partition 层的 TTL 配合 autumn-kvcache 自身的容量驱动驱逐共同完成

---

## 8. 参考 backend — 哪些抄、哪些不抄

### `HiCacheFile`（基线，全 v0+v1+v2 实现）
- Storage key = `f"{hash}.{pool}.bin"` 平铺在目录里
- `set` 是 `tensor.numpy().tofile(path)`, `get` 是 `readinto(memoryview)`
- **只参考 API 形态，性能 model 不学**

### **`MooncakeStore`（autumn-kvcache 最该照抄的对象）**
```python
class MooncakeStore(HiCacheStorage, MooncakeBaseStore):
```
- 薄 Python facade 包一个 native 分布式 store
- `_batch_preprocess(host_indices)` 把 page index 转 `(ptr, size)` list，交给 native 侧做真 zero-copy
- MHA 把 K/V 拆 `_k`/`_v` 两个 key；MLA 单 key
- **autumn-kvcache 的代码骨架基本可以照搬这个文件，把 native 部分换成 ClusterClient + 本地 DRAM cache**

### `HiCacheHF3FS`
- `from_env_config(bytes_per_page, dtype, storage_config)` 构造
- 把存储当成固定 stride 的 slot 数组，key → slot id
- **fixed-page-size 模式参考**

### `LMCRadixCache` —— **不要学**
```python
class LMCRadixCache(RadixCache):  # 注意不是 HiCacheStorage
```
- LMCache **绕开了 L3 抽象**，直接继承 `RadixCache` hook `match_prefix`/`cache_finished_req`/`evict`
- 自己跑 layer-wise async + 2 个专用 CUDA stream
- 是另一种集成方式，autumn-kvcache 不走这条

### 关键非显然 pattern

- 所有 zero-copy backend 在 `__init__` 就 `register_mem_pool_host`，缓存 pinned-host 基址，per-call 只做 index → offset 算术
- Mooncake 的 `batch_exists` 返回 contiguous-prefix int（不是 per-key list）
- Pool name `KV` 在 `HiCacheFile._get_component_key` 里被当 `__default__` —— bare hash 不加后缀

---

## 9. 接口稳定性

最近 8 个改动 `hicache_storage.py` 的 commit：

| SHA | 日期 | 主题 |
|---|---|---|
| `66ef97c` | 2026-05-15 | Fix `Optional[X] = (None,)` 默认值 typo |
| `d9fa84b` | 2026-05-15 | DeepSeek_V4 HiCache 支持 |
| `5495026` | 2026-05-12 | 默认 storage prefetch timeout |
| `eb5f0fb` | 2026-05-06 | SWA HiCache for unified radix cache |
| `c0f5950` | 2026-05-03 | UnifiedRadixTree HiCache Framework |
| `90c76d6` | 2026-04-17 | HiCacheFile component key suffixing fix |
| `e9d6b9e` | 2026-04-14 | Mooncake DSA & mamba 支持 |
| `1c76f32` | 2026-04-10 | CP support |

**评估**：
- **v0 稳定**（但批量方法已标 `# TODO: Deprecate`）
- **v1 是当前推荐**（zero-copy），稳定
- **v2 还在演化**（2026-04→05 新加，PoolName 据 TODO 评论将改名）
- **autumn-kvcache 策略**：先实现 v1（Mooncake 对齐），v2 仅 KV pool，hybrid 模型支持留 Phase 2+

---

## 10. autumn-kvcache 实现清单

1. Python 包 `autumn_kvcache.sglang_backend`，导出 `class AutumnKVCacheStorage(HiCacheStorage)`
2. `__init__(self, storage_config: HiCacheStorageConfig, extra_kwargs: dict)` — 从 extra_kwargs / extra_config 读取 manager endpoint 和 cluster 配置
3. Override `register_mem_pool_host` 缓存 pinned-host 基址 + stride
4. 实现 `batch_get_v1` / `batch_set_v1` / `batch_exists` (返回 contiguous-prefix int)
5. v0 abstract 方法实现成 thin wrapper（开 `interface_v1: 1` 后不走热路径）
6. Tenant key suffix 用 `(model_name, tp_rank, tp_size, pp_rank, pp_size, is_mla_model)` 拼，照搬 `HiCacheFile._get_suffixed_key`
7. **内部容量管理是 backend 自己的事**（sglang 永不调 delete）—— LRU + `get_stats()` 暴露指标
8. **必须快** —— prefetch budget = `2s + 0.1s/Ki-tok`，慢 backend 直接被放弃

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

---

## 11. 对 autumn 设计的传导

- **本地 DRAM cache** = backend 内部 LRU，命中即 `host_indices` 写满返回 True
- **节点间 peer 共享 (one-sided RDMA)** = LRU miss 时先问 peer，再 fall through partition
- **Partition 持久化** = LRU miss + peer miss 时 `ClusterClient.get`；writeback evict 时 `ClusterClient.put`
- **Block 大小** = sglang `bytes_per_page` 决定，不固定（不同模型不同 page_size），但**一次启动期间是常量** —— autumn-kvcache 启动时锁定 slab slot 大小
- **Key 命名** = `f"autumn-kvcache/{tenant_suffix}/{sha256_hash}"`，partition key 直接用这个字符串
- **持久化失败语义** = `batch_set_v1` 返回 `[False]` 就行，sglang 会按 write policy 决定是否重试。**绝对不能阻塞** prefetch budget
