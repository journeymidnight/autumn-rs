# 数据面 key-range 授权（服务端 authz）设计

> 状态：**设计定稿（2026-07-01）**，实现待动手。经与用户 + coco 多轮讨论收敛。
> 满足 plan §9.5 / §16 Phase 0 的多租户隔离。区别于（从未落地的）
> `admin_auth_design.md` 里的「管理操作鉴权」。

## 一句话架构

**manager(leader) 当 KDC 签发短期 capability token；PS 在 KV 层本地验签 + 强制。**
非对称 Ed25519：manager 持私钥签、PS 只持公钥验（PS 造不了假）。

```
 签发/续期(KDC)          验证 + 强制(数据面)
┌──────────────┐        ┌──────────────────────────┐
│ manager      │  公钥   │ partition-server (KV层)   │
│ leader:      │ ─────▶ │  AUTH_HELLO 验签(公钥)     │
│  · 私钥签token│        │  每请求: key前缀 + exp     │
│  · 租户账户库 │        │  ★ 从不回调 manager 做强制 │
└──────┬───────┘        └──────────────────────────┘
       │ MINT_TOKEN(短TTL)         ▲ AUTH_HELLO(token)
       ▼                           │
   ┌────────────────────────────────┐
   │ client (autumn-memory / SDK)    │
   │  持永久租户凭据 → 自动续 token   │
   └────────────────────────────────┘
```

**核心区分**：**签发**（可放 manager）和**强制**（必须在 KV 层）是两个角色。
「auth 在 KV 层」= 强制在 PS 本地、每请求、**不回调 manager**。manager 当 KDC
只管签发，不碰强制 —— 两者不冲突。

## 威胁模型（已定）

**可信内网**（RoCE 推理集群，整集群明文）。防：租户的代码/流氓-测试-client
误读/串读别租户的 `mem/{tenant}/` 数据。**不防**：网络 MITM/抓包（→ 不上 TLS）、
被攻破的 manager（manager 沦陷 = 全盘沦陷，控制分区/路由，本就出局）。

`mem/` 各段 percent-encode（`keys::q`）→ `mem/<q(tenant)>/` 是**不可伪造**的边界。

## 为什么是非对称（不是对称）

- 对称（Ceph cephx / 共享 secret + HMAC）：验证方也能签发 → **一个 PS 被攻破就能
  伪造任意租户 token**。否决。
- 非对称（FDB / Ed25519）：**签名私钥只在 manager（可信中心权威）**，PS 只持公钥，
  **只能验、造不了假**。这是把 Ceph 的 KDC 便利 + FDB 的「数据面造不了假」合起来。

## Token —— Ed25519 签名的 capability

```
claims = {
  ver: 1,                        // 格式版本(加字段兼容)
  typ: "autumn.cap.v1",          // 类型(防与其它 token 混用)
  kid: u32,                      // 用哪把公钥验 → 支持多密钥/轮换
  iss: "autumn-mgr",             // 签发方
  aud: <cluster_id>,             // 只给这个集群(防 dev token 打 prod)
  iat, nbf, exp,                 // 签发/生效/过期(短 TTL)
  allowed_prefixes: [b"mem/acme/"], // 授权范围, 每个必须以 `/` 结尾
}
sig = Ed25519_Sign(mgr_priv, DOMAIN ‖ canonical_bytes(claims))
token(wire) = rkyv{claims, kid} ‖ sig[64]
```

- **canonical serialization**：签名输入是确定字节序（rkyv 确定性布局），跨版本稳定。
- **domain separation**：`DOMAIN = b"autumn-rs data-plane cap v1"`，前缀进签名输入，
  杜绝「别的场景的合法签名被拿来冒用」。
- 每个字段堵一个洞：无 `exp`→泄露永久沦陷；无 `aud`→跨环境重放；无 `kid`→不能轮换；
  无 `allowed_prefixes`→无授权范围。

## 流程

### (1) 开租户（admin，低频，需 admin token）
`autumn-op tenant-create --tenant acme --prefix mem/acme/` →
manager 存 `{tenant, credential_hash=SHA-256(cred), allowed_prefixes}` 到 etcd，
返回该租户的**永久凭据 cred**（交给租户，当「refresh token」）。
admin 面用 `admin_auth_design.md` 的 Option A（共享 admin token 前缀 + 恒定时间比较）保护。

### (2) 签发 / 续期 token（client，高频，需租户凭据）
client → **leader manager** `MSG_MINT_TOKEN{tenant, cred}` →
manager 验 `SHA-256(cred)==credential_hash` → 用**私钥**签一个
`exp=now+TTL` 的 capability token（`allowed_prefixes` 取自账户库）→ 返回。
client 库**后台在 exp 前自动续**（Ceph 式无缝；permanent cred = 长效低频、
token = 短效高频）。

### (3) 连接 + 强制（PS）
client 连 PS → 首帧 `MSG_AUTH_HELLO{token}` → PS 按 `kid` 取公钥**本地验签** →
绑该连接 `{allowed_prefixes, exp}` → **每个 KV 请求**：key 在受保护前缀内则必须
`starts_with` 某 allowed_prefix，且 `now ≤ exp + skew`；否则拒。**PS 全程不调 manager。**

### (4) PS 取公钥 + 受保护前缀（poll，缓存）
PS 轮询 manager `MSG_GET_AUTHZ_CONFIG` → `{public_keys:[{kid, ed25519_pub, disabled}],
protected_prefixes:[mem/]}` → 本地缓存。manager 宕机 → 用缓存继续强制（只有轮换/
新公钥要 manager 在线）。

## 密钥轮换 + 撤销

- **轮换**：manager 持多把签名私钥 + 对应公钥，token 带 `kid`。加新 kid → 用新 kid 签
  新 token → 等旧 token 过期 → 禁旧 kid（`disabled`，PS 下次 poll 生效）。
- **撤销**（manager-as-KDC 比纯离线**强**）：
  1. **停续期**：从账户库删/禁租户 → 它续不到新 token → **当前 token 过期即失访**（窗口=TTL）。
  2. **应急全撤**：禁掉某 `kid` → 该 kid 签的**所有** token 立即失效 + 关已认证连接。
  - **不做** per-token deny-list（要下发黑名单 = 回控制面）。
- **TTL**：prod 租户 token 默认**小时级**（自动续期，无感）；dev 可天级。**不要 30d**
  （bearer 泄露窗口）。PS 验 exp 留 30–120s clock-skew leeway；auth 失败 metric 区分
  expired / not-yet-valid / sig-invalid。

## 强制细节（coco 收紧项）

- **choke points**：写 = `admit_region_range`（`enqueue_put/_zc/_delete`）+ `enqueue_batch_put`
  逐 op；读 = `handle_get/head/range/get_redirect/batch_get/get_zc` 旁 `check_region_epoch` 处。
- **Range 必须整区间 ⊆ 某 allowed_prefix**（不只查首 key，否则 `prefix=mem/,start=mem/acme/`
  会扫进 `mem/other/`）；分页 resume cursor 也锁在授权子区间内。
- **Batch 跨前缀**：逐 op 检查，denied 返回明确 `PermissionDenied`（**不折成 NotFound**，
  否则盲重试放大）；或预扫整批、任一 denied 整批拒不入队。
- **EN 大值直读旁路（对 autumn-memory 本就不存在，零成本）**：`MSG_GET`（`get`/`get_many`）
  **恒走 PS**（`handle_get` 传 `redirect=false`）；只有 `get_direct` / `MSG_GET_REDIRECT` 走
  EN 直读，且它是**显式 opt-in + ≥64 KiB 才给 descriptor**（失败自动 fallback 到 proxy get）。
  **autumn-memory 只用 `get`/`get_many` → `mem/` 的读全部过 PS**，天然没有旁路、无 PS-proxy
  额外成本（不是新增开销，是现状）。当 authz 开启时，`handle_get_redirect` 顺带做同样的前缀
  检查、并对 protected 前缀直接拒发 descriptor（近零开销，因为默认关 + autumn-memory 不用）。
  **残留旁路 = 明确接受、不做（WON'T-DO，见「非目标」）**：rogue client 可**绕过 PS 直连 EN 发
  `MSG_READ_BYTES`**、靠枚举/猜 `(extent_id, offset)` 读原始字节（EN 只认坐标、不认 tenant/key，
  且读路径只校验 `eversion` 不做授权；owner_epoch fence 只挡写不挡读）。**决策（用户 2026-07-01）**：
  可信内网 + **只读**（EN 只吐字节、改不了别租户数据）+ 需先猜中有效 extent 坐标 → 风险可接受，
  **不实现 EN-read-capability**。运维上 EN 数据端口本就只在数据面子网、只对 PS 开放。
- **allowed_prefixes 规范化**：必须以 `/` 结尾（防 `mem/ac` 误匹配 `mem/acme/`），mint 用
  `keys::q` 生成。
- **opt-in + default-deny**：PS 没配到任何公钥 → 不强制（fuse/kvcache/dev 零影响）；配了 →
  受保护前缀内 default-DENY（无有效 token 的连接=匿名，读不到 protected key），非受保护
  前缀不受影响。生产默认拒绝清空 protected_prefixes，除非显式 `--allow-disable-authz`。

## 连接层规则（防串租户）

- **一条连接 = 一个 principal**：PS 首帧 AUTH_HELLO 后绑定，**不允许有 inflight 时
  静默 rebind**（要 re-auth 则 drain inflight + auth epoch，或直接关连接重连）。
- **client 连接池按 principal 分区**：`ps_conns` key = `(ps_addr, principal_id)`；
  换 token 强制 drop 该 principal 的连接。**不建议一个 ClusterClient 服务多租户**——
  autumn-memory 的 `MemoryStore` 本就 per-(tenant,agent) 各自 client，天然满足。
- token 到 exp：PS 主动关连接；client 在 exp 前续好并重连。

## 非目标（本设计不做）

TLS/mTLS；per-user RBAC/角色/ACL 表；抗被攻破的 manager；抗 MITM/抓包重放；
per-token 撤销黑名单；非 `mem/` 命名空间的强制（除非配置）。

**EN 直连绕过 PS 的大值直读旁路 —— 明确接受、不做（WON'T-DO，用户 2026-07-01）。**
理由：威胁模型是可信内网，且该旁路**只读**（EN 只吐字节、无法写/改别租户数据），
攻击者还得先猜中有效 `(extent_id, offset, length)` 坐标。风险与「可信内网」前提一致，
**不上 EN-read-capability**（那会给 EN 加验签逻辑 + 引出「谁签」的对称困境，成本不匹配收益）。
若某天威胁模型升级（不可信读者接入数据面），再把它作为独立 feature 重启。

## 实现阶段

- **Stage 1 — KDC（manager）✅ 完成（2026-07-01，commit c56acf4 + 22debd7）**：
  `crates/rpc/cap_token.rs` = Ed25519 token 编解码（`CapClaims` + `sign_claims`/
  `verify_token`，`verify_strict`、验携带的 claims 字节非重编码、untrusted 走 checked-rkyv、
  `AuthReject` 分类、`DOMAIN` 分离；进 wire 指纹 → v9，MIN=MAX=9；10 单测）。manager 侧：
  `authz.rs` keyring（`--auth-signing-key-file`，多 kid，fail-loud 解析，active=最高 enabled kid，
  published 从种子派生公钥无 drift）+ SHA-256 凭据哈希 + 常量时间比较；etcd `tenantAccount/` 账户库
  （F149-fenced，fail-loud replay）；`MSG_MINT_TOKEN`（leader-only，常量时间验 cred，unknown-tenant/
  wrong-cred 同 opaque 错）/ `MSG_GET_AUTHZ_CONFIG`（不 leader-gate，静态本地配置）/ `tenant-create/delete`
  admin RPC（leader + admin token gated，etcd-first）；binary CLI（opt-in，无 signing key 则关）。
  验收单测 `authz_kdc_tests`：mint→publish config→验签→过期失败→改字节失败→delete 停续期 + disabled-when-no-key。
  manager lib 169/169、rpc lib +10、workspace 全绿。
- **Stage 2 — PS 强制**：poll `GET_AUTHZ_CONFIG` 缓存公钥/protected；`MSG_AUTH_HELLO`
  首帧验签绑 principal；两 choke point + Range 区间 + batch 逐 op + exp 每请求；
  EN 大值走 PS proxy。测：跨租户拒、匿名拒、非 protected 放行。
- **Stage 3 — client + 工具**：SDK 持 token + 自动续期（后台）+ 连接池按 principal；
  autumn-memory 透传租户凭据；`autumn-op gen-signing-key / tenant-create / mint-token`；
  cluster.sh 分发。e2e：两租户互隔离 + 非 `mem/` 不受影响 + 续期无感 + kid 轮换。

## 参照
FoundationDB tenant authorization（非对称 JWT，storage 层验，不可撤销靠短 TTL）；
Ceph cephx（MON 当 KDC + 自动续 ticket，对称——我们改非对称）；etcd RBAC（中心策略，
被否的 per-request 中心查形态）；OAuth（access+refresh = 我们的 token+permanent-cred）。
