# 升级安全（全停全启）

autumn-rs **不做 rolling upgrade**（混版本同时在线服务）。生产升级形态是
**全停 → 换二进制 → 全起**，且 **etcd 永不清**（生产绝不 `cluster.sh reset`）。
本文记录这个形态下"不静默损坏"的保证来源与配套机制。

---

## 1. 安全来源：rkyv 校验式解码 fail-loud

热路径与控制面持久值都是 rkyv。rkyv 的校验式 `from_bytes` 在布局不符时**响亮失败**，
绝不把旧字节静默解成错值：

- 新二进制读旧 etcd，代码改而 schema 不变的升级直接读通。
- 任何人改了持久结构而 etcd 未迁移，`replay_from_etcd` 报错 → 该 manager **当不上
  leader**（fail-loud replay，见 `crates/manager/CLAUDE.md`）。

**INVARIANT**：任何持久结构改动（etcd value / SST / `.meta` / WAL）要么保持同 rkyv
布局，要么随该版本带一个一次性、幂等的迁移 —— 绝不靠 reset 兜底。

## 2. `cluster_version` = 格式戳 + 回滚 fail-closed

etcd key `autumn-rs/cluster_version`（ASCII 十进制，刻意不用 rkyv，好让它活过序列化
时代更替）。`bump_cluster_version` 只允许 leader、只允许 current+1、上限是本二进制的
`WIRE_VERSION_MAX`、value-CAS。唯一解码点 `parse_cluster_version` **回滚 fail-closed**：
持久值超过本二进制的 `WIRE_VERSION_MAX` 时拒绝，于是 bump 之后旧二进制经 replay 当不上
leader。

语义：**所有成员都换成新二进制之后**，operator 才 `autumn-op upgrade-version`；新的
wire 形态 / 新持久值格式只在 `cluster_version >= N` 时才允许发出。bump 前可以换回旧
二进制，bump 后不可。运维步骤见 `docs/ops.md`。

## 3. wire 区间 + 指纹注册表

- 每个二进制自带编译期区间 `[WIRE_VERSION_MIN, WIRE_VERSION_MAX]`
  （`crates/rpc/src/lib.rs`）；启动检查要求区间有交集。当前 rkyv 无跨版本解码能力，
  所以 **MIN = MAX**，部署保持 same-commit。
- 区间与指纹经 `GetClusterIdResp` 交换。该结构**冻结** —— 它本身就是协商通道，在任何
  兼容性判断之前被解码，再改布局会让混版本握手不可达；新增字段走新 msg_type。
- 防忘 bump：`WIRE_VERSION_FINGERPRINTS` 注册表 + `wire_version_registry_tests`。任何
  wire-schema 源文件改动都会改变 `WIRE_FINGERPRINT` 并让测试失败，强制一次显式的版本
  决策。运行时还做 fraud 交叉校验：对端声明一个我方已知的版本但指纹不符 → 拒绝。

## 4. 同版本滚动重启（运维工具，不是版本升级）

`scripts/rolling_restart.sh`：**同一份二进制**的逐进程重启（改配置 / 换机 / 内核升级），
顺序为最被依赖的一端优先 —— extent-node 逐个 → partition-server → manager，每步之间跑
收敛门 + 每分区写活性探针，第一个不收敛的门即 fail-stop。`cluster.sh` 的
`start-manager` / `stop-manager` / `restart-manager` 提供 manager 侧的单进程操作
（etcd replay 让 manager bounce 成为安全的一步）。用法与人工验证步骤见 `docs/ops.md`。

## 5. 否决的备选

| 备选 | 否决理由 |
|---|---|
| 控制面全量迁 prost（换取免演进负担） | 它多给的只是"不 reset 也能原地改 etcd schema"这一项独立能力，可在真正需要时用一次性迁移补，不必常驻编解码复杂度 |
| 全栈换 protobuf | 热路径回归（rkyv 零拷贝是实测选型） |
| 全栈版本化 rkyv（每结构手写 V1/V2 + 转换） | 几十处持久值 + 50+ RPC 结构的 churn 不可持续 |
| 蓝绿双集群 + 迁移 | 数据量级与成本不成比例；本系统无跨集群复制设施 |
