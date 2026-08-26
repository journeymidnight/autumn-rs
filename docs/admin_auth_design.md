# 管理操作鉴权（admin token）

一个**集群共享的 admin secret**（bearer capability），gate 那些**破坏性、
操作员专属**的控制面 op。数据面零改动，热路径（append fanout）一帧不动。
数据面的 key-range 授权是另一套机制，见 `data_plane_authz_design.md`。

## 1. 威胁模型

**部署前提 = 可信内网**（sglang/vLLM 推理集群，RoCE 内网）。

- **目标**：挡「**误连别的集群**」与「**流氓或测试客户端跑破坏性管理命令**」——
  fence-node / remove-node / 改 EC / 改 cluster_version / 乱建 stream / 改路由 /
  提交 op / 触发 split·merge·gc·compact。
- **不做**：不防 MITM / 真实网络攻击者（不上 TLS/mTLS）；不做多租户 per-user
  角色 / ACL（不上 RBAC）。

## 2. 与「像权限」的既有机制的区别

这三层机制经常被误当成鉴权，它们解决的是别的问题：

| 机制 | 真实作用 | 是不是鉴权 |
|---|---|---|
| `owner_epoch` / `region_epoch` / `lease_epoch` fencing | 防可信组件间脑裂 / 串写 | ❌ |
| wire-fingerprint / `WIRE_VERSION` | 防版本混部 rkyv 解码错乱 | ❌ |
| autumn-rpc 帧 CRC32C | 防传输位翻转 | ❌ |

## 3. secret 分发

- **manager**：`--admin-token <TOK>` 或 `--admin-token-file <path>`。**首选文件
  形式**——裸 flag 会把 secret 泄进 `ps` / `/proc/<pid>/cmdline`。
  遵守仓库铁律「rs 代码不读 env」：env→flag 翻译在 `cluster.sh`，由它生成 token
  并分发到全集群。
- **`autumn-op`**：全局 `--admin-token` / `--admin-token-file`（全局形式必须写在
  **子命令之前**）；`principal-create` / `namespace-*` / `presplit` 等子命令也各自
  接受同名 flag。
- **PS**：不配置，**从 manager 学** —— `GetAuthzConfigResp.admin_token` 随 5 s 的
  authz config poll 下发。明文传输，与签名公钥同一通道，可信内网前提允许。
- **EN / python / fuse / ioring**：完全不参与。

admin token 与数据面 authz **解耦**：一个没开 authz（没配 signing key）的集群
照样可以、也应该配 admin token。

## 4. 三个执行点

### 4.1 manager 的集群变更 op —— payload 前缀，opt-in

`autumn_rpc::manager_rpc::is_admin_mgr_msg(msg_type)` 是**唯一一份清单**，
client 与 manager 共用（杜绝两边漂移）：

`MSG_FENCE_NODE` / `MSG_REMOVE_NODE` / `MSG_SET_NODE_MAINTENANCE` /
`MSG_CLEAR_NODE_OVERRIDE` / `MSG_BUMP_CLUSTER_VERSION` / `MSG_UPDATE_STREAM_EC` /
`MSG_FORCE_EC_CONVERT` / `MSG_CREATE_STREAM` / `MSG_UPSERT_PARTITION` /
`MSG_MERGE_PARTITIONS` / `MSG_MULTI_MODIFY_MERGE` / `MSG_OP_SUBMIT`

**wire 成本为零**：不给十几个 req struct 各加 `admin_token` 字段，而是把 token 做成
payload 前缀 `[u32 LE token_len][token][原 payload]`（`prefix_admin_token` /
`strip_admin_token`，`crates/rpc/src/manager_rpc.rs`），在 manager dispatch 一处
剥离 + 常量时间比较（`manager::authz::ct_eq_secret`）。前缀畸形（长度头截断 /
长度跑出 buffer）**判定为校验失败，绝不当作「裸跑」**。

**opt-in**：manager 没配 token → 这些 op 裸跑（dev / test / bench / chaos 不受
影响）；配了 → 缺 token / token 错一律拒。

清单里两处刻意的例外，改动时不要「顺手补齐」：

- **`MSG_REGISTER_NODE` 不 gate**：EN 启动时以及 manager 重启后靠它自注册
  （`extent_node.rs::register_with_manager`），EN 没有 admin token 概念 ——
  gate 它会把集群 bring-up 卡死。`MSG_CREATE_STREAM` / `MSG_UPSERT_PARTITION`
  仍然 gate：只有 `autumn-op bootstrap` 发它们。
- **`MSG_MULTI_MODIFY_SPLIT` 不 gate**：它是 PS 驱动的。同族的
  `MSG_MULTI_MODIFY_MERGE` 没有 in-tree wire 调用方（manager 进程内直接调），
  所以 gate 它只挡外部流氓客户端绕过 `MSG_MERGE_PARTITIONS` 的 freeze /
  sacred-boundary 守卫去直发底层原语。

### 4.2 manager 的账户 / 注册表 admin RPC —— struct 字段，fail-closed

principal（`tenantAccount/`）与 namespace 注册表的 create / delete /
set-presplit 各自在 req struct 里带 `admin_token: String` 字段，handler 里
leader-gate + 常量时间比较。

与 4.1 **相反：没配 token = 拒绝**（`admin RPCs disabled`）。这些 op 只在鉴权
语境下有意义，所以 fail-closed 而不是 opt-in。

### 4.3 PS 的集群变更 op —— 同一份前缀编解码

`autumn_rpc::partition_rpc::is_admin_ps_msg` = `MSG_SPLIT_PART | MSG_MAINTENANCE`
（maintenance = gc / compact / forcegc / flush）。PS 用 poll 来的 admin secret 做
`partition_server::authz::ct_eq_bytes` 常量时间比较；secret 为空（manager 没配）
= 这些 op 裸跑，与 manager 侧同一 opt-in 姿态。

**manager 自己也是调用方**：auto-policy controller 的 split、merge 前的 flush 都是
manager→PS 调用，所以 manager 发往 PS 的这类 payload 走
`AutumnManager::admin_prefix_ps`，与操作员的 `autumn-op` 走**同一条**认证路径。
client 侧同样在 `call_ps_for_part` 里按 `is_admin_ps_msg` 前缀一次（在重试循环
**之外**，避免每次重试重复拼装）。

## 5. 不 gate 的面

- **数据面**（put / get / delete / range / head / lease）：可信内网高频路径，
  一帧不动。key 级授权由 `data_plane_authz_design.md` 的 capability token 负责。
- **只读 / 观测类**（`info`、`df`、node 列表、recovery-stats、extent-health、
  audit-query、partition-detail、get-regions、probe-extent、get-discards、
  cluster-id / cluster-version、`namespace-list`）：不破坏性，留开。
- **已被 `owner_epoch` fencing 挡住的**（`punch_holes` / `truncate` /
  `stream_alloc_extent` 等）：流氓客户端拿不到 partition owner 锁，发了也被
  EN / manager 拒。

## 6. 与 HBase 的对照

HBase 安全 = 认证（Kerberos + SASL 确定 principal）+ 授权（`AccessController`
按 `R/W/X/C/A` 权限与作用域查 `hbase:acl` 表），admin 类 op 要 `ADMIN`/`CREATE`
权限。

本机制是它的**合理降级**：不做身份 / 角色 / ACL 表，用**一个共享 admin
capability** 代替「这个 principal 有 ADMIN 权限」。作用域上与 HBase 一致 ——
连 PS 侧的 split / merge / flush / compact 也算 ADMIN 类（§4.3），而不是只
gate manager。对可信内网这是对的尺寸；要多租户 / 不可信网络才需要再上 mTLS +
身份 + per-op ACL。

## 7. 性能

**零**。每个 admin op 低频；开销 = 4+N 字节前缀 + 一次常量时间比较；数据面与
连接建立完全不碰。
