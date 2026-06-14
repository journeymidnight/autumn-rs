# 管理操作鉴权 设计（DESIGN ONLY，2026-06-14，未实现）

> 状态：**仅设计，未实现，留作后续**。本文记录决定（Option A）+ 实现路径，
> 供将来直接动手，不必重新调研。

## 背景 / 威胁模型

autumn-rs 当前**完全没有鉴权 / 授权 / TLS**（全代码库零 `auth/token/tls/mtls/
x509/rbac`；出现的 "TLS" 都是 thread-local storage）。现有三层"像权限"的机制
解决的都是别的问题，**都不是鉴权**：

| 机制 | 真实作用 | 是不是鉴权 |
|---|---|---|
| `owner_epoch` / `region_epoch` / `lease_epoch` fencing | 防可信组件间脑裂/串写 | ❌ |
| wire-fingerprint / `WIRE_VERSION` | 防版本混部 rkyv 解码错乱 | ❌ |
| autumn-rpc 帧 CRC | 防传输位翻转 | ❌ |

**部署前提（用户确认）= 可信内网**（sglang/vllm 推理集群，RoCE 内网）。
- **目标**：挡"**误连别的集群** / **流氓或测试客户端跑破坏性管理命令**"
  （fence-node / remove-node / 改 EC / 改 cluster_version / 乱建 stream / 改路由）。
- **明确不做**：不防 MITM / 真实网络攻击者（→ 不上 TLS/mTLS）；不做多租户
  per-user 角色 / ACL（→ 不上 RBAC）。

## 决定：Option A —— 单一共享 admin secret，只 gate manager 的操作员专属变更 op

**核心**：用**一个集群共享的 admin secret（bearer capability）**，只对
**纯操作员、破坏性、且只发往 manager** 的控制面 op 校验。数据面零改动，热路径
（append fanout）一帧不动。

### 要 token 的集合（已核实 PS/EN 内部一个都不发，纯 CLI 专属）

`MSG_FENCE_NODE` / `MSG_REMOVE_NODE` / `MSG_SET_NODE_MAINTENANCE` /
`MSG_CLEAR_NODE_OVERRIDE` / `MSG_BUMP_CLUSTER_VERSION` /
`MSG_UPDATE_STREAM_EC` / `MSG_FORCE_EC_CONVERT` / `MSG_CREATE_STREAM` /
`MSG_UPSERT_PARTITION` / `MSG_REGISTER_NODE`

（核实方法：`grep` 这些 MSG 在 partition-server/stream 源码里**无内部发送方**，
只有 autumn-op / autumn-client 发。）

### 不 gate 的（已有别的保护或不破坏性）

- **`split` / `merge` / `punch_holes` / `truncate` / `stream_alloc_extent`**：
  已被 **`owner_epoch` fencing** 挡——流氓客户端拿不到 partition owner 锁，
  发了也被 EN/manager 拒。这是 Option A 不额外 gate PS 的依据。
- **数据面**（put/get/del/lease/range/head）：可信内网高频可信，不碰。
- **只读管理**（list-node-states / recovery-stats / extent-health / audit-query /
  partition-detail / nodes-info / get-regions / probe-extent / get-discards /
  get-cluster-id|version）：不破坏性，**留开**（gating 收益低）。

### 被否的 Option B（HBase 对齐，记录备选）

HBase 把 `split / merge / flush / compact` 归类为 **ADMIN 操作**（要 `A` 权限）。
照 HBase 语义应同时 gate **PS 的 Maintenance RPC**（操作员触发的 split/merge/
gc/compact，PS 也读 token）。**否决理由**：这些在 autumn-rs 已有 `owner_epoch`
兜底，且会把 PS 拉进鉴权面（多一个入口 + 改 PS）。Option A 已覆盖真正裸奔的
非-owner-fenced 变更 op。若将来要 HBase 式"按操作语义一刀切"，再升 B。

## 实现路径（落地时照此做）

### secret 分发
- `--auth-token-file <path>`：读文件（**不用裸 flag**，避免 secret 进
  `/proc/cmdline`）。遵守仓库铁律"**rs 代码不读 env**"——env→flag 翻译在
  `cluster.sh`，由它**生成一份 token 并分发到全集群**（manager + 两个 CLI）。
- 入口 **3 个**：`autumn-manager-server`（校验）、`autumn-op`、`autumn-client`
  的 admin 子命令（带 token）。**PS / EN / python / fuse / ioring 完全不动**。

### token 怎么走（推荐：payload 前缀，零 wire-struct 改动）
- 共享判定 `autumn_rpc::is_admin_msg(msg_type) -> bool`（列上面 10 个 MSG，
  与 manager + CLI 共用一份，杜绝两边漂移）。
- **CLI 侧**：发 admin op 且配了 token 时，在 rkyv payload **前缀 32 字节 token**。
- **manager 侧**：dispatch `match msg_type` 前，若 `is_admin_msg(mt) &&
  self.admin_token.is_some()`：切掉前 32 字节做 **constant-time compare**
  （`subtle`/手写恒等时间），不符回 `CODE_PRECONDITION` + 拒绝；符则把剩余
  payload 交原 handler。
- 备选（更"正"但 wire 改动大）：给每个 admin Req 加 `token` 字段（~10 个 struct
  改动 + 各自 fingerprint bump）。**不推荐**——前缀法一处 codec、零 struct 改动。

### opt-in / 向后兼容
- manager **没配** `--auth-token-file` → 校验**完全跳过**，CLI 也不前缀 →
  现有 dev/test/bench/单测**零影响**。
- 配了 token 的 manager **拒绝**没 token / token 错的 admin 请求，报清晰错误。
- 同版本全停全启部署（仓库惯例），无混部窗口。

### 热路径 / 性能
- **零**。每个 admin op 低频，前缀 32 字节 + 一次比较；数据面与连接建立**不碰**。

## 连接面调研结论（落地时直接用，2026-06-14 Explore 核实）

- TCP 与 UCX **统一在 `autumn_transport::Conn`(ReadHalf/WriteHalf) + 同一
  `FrameDecoder`**，msg 分发层透明 → **UCX 不是单独的鉴权窟窿**（若将来升级到
  连接级握手，一处 frame 层即覆盖两种传输）。
- 客户端出站**唯一咽喉** = `RpcClient::connect`（client.rs）；两个 ConnPool
  (`autumn_rpc` + `autumn_stream`) 都包它。
- 服务端 accept 面有 **3 处**（manager `serve` / PS 每分区 `accept_loop_on` /
  EN `serve_with_control`），但 **Option A 用不到**——只在 manager dispatch 里
  按 msg_type 校验即可，不碰 accept 面。
- 当前**没有**任何 per-connection 握手帧；`MSG_GET_CLUSTER_ID` / wire-fingerprint
  是**客户端启动期主动校验 server**，非 server 认证 client、非 per-connection。

## 对照 HBase（为什么这是合理降级）

HBase 安全 = 两层：
1. **认证（你是谁）**= Kerberos + SASL/GSSAPI（含 daemon 间、跟 HDFS/ZK；
   批作业用 delegation token）。**任何授权决定的前提是先知道 principal。**
2. **授权（你能干啥）**= `AccessController` 协处理器拦每个 op 的 `preXxx`，按
   `R/W/X/C/A` 权限 + 作用域（global→ns→table→cf→qualifier→cell）查 `hbase:acl`
   表；`hbase.superuser` bypass。**admin op = 要 `ADMIN`/`CREATE` 权限。**

本设计 = HBase 这套的**合理降级**：不做身份 / 角色 / ACL 表，用**一个共享
admin capability** 代替"这个 principal 有 ADMIN 权限"——等价于 HBase 不开
AccessController、但给危险操作单加一道"admin 口令"。对可信内网是对的尺寸；
将来若要多租户 / 不可信网络，再上 mTLS（autumn-rpc 是自研裸 TCP + UCX，套
传输加密是另一个量级的工程）+ 身份 + per-op ACL。

## 落地 checklist（将来照做）

1. `autumn_rpc::is_admin_msg` + 10 个 MSG 常量复用。
2. manager：`--auth-token-file` 读取 → `admin_token: Option<[u8;32]>`；dispatch
   前缀校验（constant-time）。
3. autumn-op / autumn-client：`--auth-token-file` → admin 调用前缀 token。
4. cluster.sh：生成 token（如 `openssl rand -hex 32` → 文件，权限 600）+ 分发 +
   env→flag。
5. 测试：无 token=放行（回归）；配 token 后 admin op 缺/错 token=拒、对=过；
   constant-time compare 单测。coco review。
6. README：admin 鉴权开启步骤 + `cluster.sh` 用法。
