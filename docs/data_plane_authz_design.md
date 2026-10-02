# 数据面 key-range 授权（服务端 authz）

## 1. 一句话架构

**manager(leader) 当 KDC 签发短期 capability token；PS 在 KV 层本地验签 + 强制。**
非对称 Ed25519：manager 持私钥签、PS 只持公钥验（PS 造不了假）。

```
 签发/续期(KDC)          验证 + 强制(数据面)
┌──────────────┐        ┌──────────────────────────┐
│ manager      │  公钥   │ partition-server (KV层)   │
│ leader:      │ ─────▶ │  CLIENT_AUTH 验签(公钥)    │
│  · 私钥签token│        │  每请求: key前缀 + exp     │
│  · principal │        │  ★ 从不回调 manager 做强制 │
│    账户库    │        └──────────────────────────┘
└──────┬───────┘
       │ MINT_TOKEN(短TTL)         ▲ CLIENT_AUTH(token)
       ▼                           │
   ┌────────────────────────────────┐
   │ client (SDK / fuse / kvcache)   │
   │  持长期 principal 凭据 → 自动续  │
   └────────────────────────────────┘
```

**核心区分**：**签发**（可放 manager）和**强制**（必须在 KV 层）是两个角色。
「auth 在 KV 层」= 强制在 PS 本地、每请求、**不回调 manager**。manager 当 KDC
只管签发，不碰强制 —— 两者不冲突。

身份单位是 **principal**（持凭据的组件），授权单位是它的 **grant**（左锚字节
前缀，通常就是一个 namespace，如 `fs/`）。key 布局与 namespace 注册表见
`key_namespace_split_design.md`。

## 2. 威胁模型

**可信内网**（RoCE 推理集群，整集群明文）。防：某个组件的代码 / 流氓 - 测试
client 误读 / 串写别的 namespace 的数据。**不防**：网络 MITM / 抓包（不上 TLS）、
被攻破的 manager（manager 沦陷 = 全盘沦陷，它控制分区与路由，本就出局）。

namespace 段是 `[a-z0-9._-]+` 的单路径段（连接时 `is_valid_scope_segment`
校验），所以 `{ns}/` 是一个**不可伪造**的边界；应用若在 namespace 内继续
分层，仍须对动态路径段做无歧义编码。

## 3. 为什么是非对称（不是对称）

- 对称（Ceph cephx / 共享 secret + HMAC）：验证方也能签发 → **一个 PS 被攻破
  就能伪造任意 principal 的 token**。否决。
- 非对称（FDB / Ed25519）：**签名私钥只在 manager（可信中心权威）**，PS 只持
  公钥，**只能验、造不了假**。这是把 Ceph 的 KDC 便利与 FDB 的「数据面造不了假」
  合起来。

## 4. Token —— Ed25519 签名的 capability

`crates/rpc/src/cap_token.rs`：

```
claims = CapClaims {
  ver: 1,                        // CAP_VER，格式版本(加字段兼容)
  typ: "autumn.cap.v1",          // CAP_TYP，类型(防与其它 token 混用)
  kid: u32,                      // 用哪把公钥验 → 支持多密钥/轮换
  iss: "autumn-mgr",             // 签发方
  aud: <cluster_id>,             // 只给这个集群(防 dev token 打 prod)
  iat, nbf, exp,                 // 签发/生效/过期(短 TTL)
  allowed_prefixes: [b"fs/"],    // 授权范围, 每个必须以 `/` 结尾
}
sig = Ed25519_Sign(mgr_priv, CAP_DOMAIN ‖ canonical_bytes(claims))
token(wire) = canonical_bytes(claims) ‖ sig[64]     // SIG_LEN = 64
```

- **canonical serialization**：签名输入是确定字节序（rkyv 确定性布局）。验签用
  **token 携带的 claims 字节本身**，不重新编码后再比对；untrusted 输入走
  checked-rkyv 解码。
- **domain separation**：`CAP_DOMAIN = b"autumn-rs data-plane cap v1"` 前缀进
  签名输入，杜绝「别的场景的合法签名被拿来冒用」。
- 每个字段堵一个洞：无 `exp` → 泄露即永久沦陷；无 `aud` → 跨环境重放；
  无 `kid` → 不能轮换；无 `allowed_prefixes` → 无授权范围。
- 拒绝原因分类为 `AuthReject`（malformed / 类型不符 / 未知或禁用 kid /
  not-yet-valid / expired / bad-signature），PS 按类上报 metric。

## 5. 流程

### 5.1 建 principal（admin，低频）

`autumn-op principal-create <name> --grant <prefix> [--grant …]` →
manager 存 `{name, credential_hash = SHA-256(cred), allowed_prefixes}` 到 etcd
`tenantAccount/<name>`（leader-fenced，replay fail-loud），返回该 principal 的
**长期凭据**（≈ refresh token，交给那个组件）。

grant 串语义：**非空、左锚字节前缀、强制补尾 `/`**。

凭据文件格式是**两行**：`<principal-name>\n<hex-secret>`。名字随文件携带，
所以数据面不需要 `--principal` / `--tenant` flag。

这条 RPC 只在 Admin 连接上受理，Admin 连接必须持有集群密钥
（`cluster_secret_design.md` §4）。

### 5.2 签发 / 续期 token（client，高频）

client → **leader manager** `MSG_MINT_TOKEN{principal, credential}` →
manager 常量时间验 `SHA-256(cred) == credential_hash` → 用**私钥**签一个
`exp = now + TTL` 的 capability token（`allowed_prefixes` 取自账户库）→ 返回
`{token, exp}`。unknown-principal 与 wrong-credential 返回**同一个 opaque 错误**。

client 库懒 mint，并在 `exp` 前 `TOKEN_RENEW_MARGIN_SECS = 300` 秒自动续；
续期会驱逐用旧 token 认证过的 PS 连接。长期 cred = 低频长效，token = 高频短效。

### 5.3 连接 + 强制（PS）

client 连 PS → 首帧 `MSG_CLIENT_AUTH{token}`（`0x55`）→ PS 按 `kid` 取公钥
**本地验签**、校验 `aud == cluster_id` → 绑该连接
`BoundPrincipal{allowed_prefixes, exp, kid}` → **每个 KV 请求**做前缀 + 有效期
检查，不符回 `StatusCode::PermissionDenied`(=7)。**PS 全程不调 manager。**

### 5.4 PS 取公钥与配置（poll，缓存）

PS 轮询 manager `MSG_GET_AUTHZ_CONFIG` → `GetAuthzConfigResp{enabled,
public_keys:[{kid, ed25519_pub, disabled}], namespaces, token_ttl_secs,
clock_skew_secs, cluster_id, …}` → 本地缓存
（`AuthzState::install`，`RwLock<Arc<AuthzInner>>` 整体换页）。
首次同步取在 `finish_connect`（强制在第一条连接前就已武装），此后 5 s 一次
（`authz_config_poll_loop`）。manager 宕机 → 用缓存继续强制（只有轮换 / 新公钥
需要 manager 在线）。

## 6. 强制细节

- **choke point 只有一个**：`authz_gate` 在**每帧派发的顶端（路由之前）**，
  接在 `push_one_frame_to_inflight` 与 `d1_fast_path_round_trip` 两条路径上。
  `AuthzState::is_enabled()` 是单个 `AtomicBool`，关时一次 relaxed load，零成本。
- **protect-everything**：authz 一旦开启，**每个 key、每个 range 都要 token**。
  没有「非受保护区间」的概念 —— 匿名连接（没发过 CLIENT_AUTH）对任何 key 都被拒。
  `GetAuthzConfigResp.protected_prefixes` 字段仍在 wire 上（manager 从 namespace
  注册表里 owner 非空的行桥接过来），但**不参与 PS 的强制判定**。
- **`authz_check` 的 INVARIANT（load-bearing）**：每个携带 user key 的数据面
  msg_type **必须**在 `authz_check` 里有一条 arm 去取 key 并调
  `check_key` / `check_range`。catch-all `_ => None`（放行）只对非 key 作用域的
  op（maintenance / split / merge / discards / diag）与 `CLIENT_AUTH` 正确。
  新增一个带 key 的读写 RPC 而忘了加 arm = 一个 authz 旁路。
  当前有 arm 的：`MSG_GET_BULK` / `MSG_GET_REDIRECT` /
  `MSG_GET_REDIRECT_MANY` / `MSG_HEAD` / `MSG_DELETE` / `MSG_PUT` /
  `MSG_PUT_BULK` / `MSG_RANGE` / `MSG_BATCH_PUT` / `MSG_BATCH_PUT_BULK` /
  `MSG_BATCH_GET_BULK`。
- **解不开的帧放行**：gate 与真正的 handler 用**同一次** decode
  （rkyv 的走 `rkyv_decode`，`MSG_PUT_BULK` 走 `parse_put_bulk_meta`），
  这里解不开的 handler 那里也解不开 → 会被 `InvalidArgument` 拒，永远不会吐字节。
- **`MSG_PUT_BULK` 零 value 拷贝**：key 是二进制 meta 头里的一个 slice。
  `MSG_PUT` 会被 rkyv decode 拷贝 value，但它只用于 < 64 KiB 的值（大值走
  `MSG_PUT_BULK`），拷贝有界，且只在 authz 开启时发生。
- **Range 必须整区间 ⊆ 某个 allowed_prefix**（不只查首 key，否则
  `prefix=mem/, start=mem/acme/` 会扫进 `mem/other/`）。可返回的 key 都
  `starts_with(prefix)`，所以判据等价于 `prefix.starts_with(某 AP)`。
  **空 prefix（无界全扫）永远拒**——空区间不可能 ⊆ 一个非空 grant。
- **Batch 逐 op 检查**，任一 denied 整批拒；返回明确的 `PermissionDenied`，
  **不折成 NotFound**（否则盲重试放大）。client 侧 `PermissionDenied` 是
  **terminal** 错误，不重试。
- **kid 撤销对活连接生效**：`check_key`/`check_range` 每次都查
  `inner.keys.contains_key(&p.kid)`，禁掉的 kid 在下一次 poll 装载后立刻让
  已绑定的连接失效。
- **allowed_prefixes 规范化**：必须以 `/` 结尾（防 `mem/ac` 误匹配 `mem/acme/`）。
- **连接期 fail-fast**：`ClusterClient::validate_credential_scope` 在 connect 时
  就 mint 一次并校验「本 client 的 scope 落在某条 grant 之下」，配错凭据在
  connect 报错，而不是拖到第一次写才 `PermissionDenied`。只有「manager 没开
  authz」才跳过。
- **opt-in**：manager 没配 `--auth-signing-key-file` → `enabled=false` → PS 不
  强制（fuse / kvcache / dev / chaos 零影响）。

## 7. 密钥轮换 + 撤销

- **轮换**：manager 持多把签名私钥 + 对应公钥（keyfile 每行
  `<kid> <hex-32-byte-seed> [disabled]`，`autumn-op gen-signing-key` 生成），
  token 带 `kid`。加新 kid → 用新 kid 签新 token → 等旧 token 过期 → 把旧 kid 标
  `disabled`（仍然 publish，PS 下次 poll 后即拒该 kid）。active = 最高的 enabled kid。
- **撤销**（manager-as-KDC 比纯离线**强**）：
  1. **停续期**：从账户库删 / 禁 principal → 它续不到新 token → **当前 token
     过期即失访**（窗口 = TTL）。
  2. **应急全撤**：禁掉某 `kid` → 该 kid 签的**所有** token 立即失效，含已认证的
     活连接。
  - **不做** per-token deny-list（下发黑名单 = 把强制拉回控制面）。
- **TTL**：`--auth-token-ttl-secs`，默认 3600。prod 小时级（自动续期，无感）；
  dev 可放宽。**不要 30d**（bearer 泄露窗口）。`--auth-clock-skew-secs` 默认 60，
  用于 `nbf`/`exp` 的 leeway。

**运维认知**：开了 enforcement，数据面对控制面产生一条**宽限期 = TTL** 的可用性
依赖 —— 强制本身不回调 manager，但**续签是 leader-only**。manager 不可达超过
`TTL − 300 s` → 续期失败 → token 过期 → PS 拒。TTL 是「撤销窗口 vs manager 故障
容忍」的权衡旋钮。

## 8. 连接层规则（防串 principal）

- **一条连接 = 一个 principal**：PS 首帧 CLIENT_AUTH 后绑定，**不允许有 inflight
  时静默 rebind**（要 re-auth 就 drain inflight + auth epoch，或直接关连接重连）。
- **client 连接池按 principal 分区**：换 token 强制 drop 该 principal 的 PS 连接
  （`ensure_token` 续期路径做这件事）。**不建议一个 `ClusterClient` 服务多个
  principal**。
- token 到 `exp`：PS 拒并要求 re-authenticate；client 在 exp 前 300 s 续好并重连。

## 9. 非目标

TLS / mTLS；per-user RBAC / 角色 / ACL 表；抗被攻破的 manager；抗 MITM / 抓包
重放；per-token 撤销黑名单；EN 按 key 范围授权直读（见 §10）。

## 10. EN 直读：只验身份

`get_direct` / `MSG_GET_REDIRECT(_MANY)` 让客户端拿着 PS 发的 descriptor
（extent、偏移、副本地址）直接向 EN 发 `MSG_READ_BYTES(_BULK)`。PS 在发
descriptor 前做 `check_key`，但 EN 收到的只是坐标，不知道它属于哪个 key。

- **开启 authz 时**，EN 只在绑定了 principal 的 Client 连接上服务直读：客户端
  先发同一个 `MSG_CLIENT_AUTH{token}`，EN 用同一份公钥验签、校验 `aud`、绑定
  `BoundPrincipal`，此后每个读请求检查 kid 仍启用、token 未过期（与 PS 的
  `still_valid` 是同一个函数，`cap_token.rs`）。没有绑定、token 过期、kid 被禁，
  一律 `PermissionDenied`。
- **被拒就是结果**：EN 拒绝这个客户端（CLIENT_AUTH 被拒，或读请求因凭证被拒）时，
  SDK 不换副本、不回落 PS proxy，直接返回 `AutumnError::PermissionDenied`，由调用方
  决定怎么办：同一份凭证在每个 EN 上的判定都相同，换地方重试只会把拒绝藏起来。
  其他直读失败（节点不可达、超时、布局过期）照旧换副本再回落 proxy。
  “每个 EN 判定相同”有一个窗口：EN 与 PS 每 5 s 才取一次配置，签名密钥增删后的
  几秒内，用新 kid 签的 token 可能在某个 EN 上被拒，这个拒绝同样直接返回调用方。
- **EN 不验范围**：一个合法 principal 仍可凭猜中的坐标读到别的 tenant 的 extent。
  要堵这一点需要 PS 为每个 descriptor 签名、EN 验签，未做。
- **EN 取配置**：每个 shard 每 5 s 轮询 `MSG_GET_AUTHZ_CONFIG`（`ClientAuthz`）。
  配置到手之前（`Unknown`）拒绝直读和 CLIENT_AUTH，都回 `Unavailable`（暂时的，
  不是对凭证的判定，SDK 照常回落 proxy），不在不知道是否开启 authz 的状态下放行；
  manager 不可达时沿用上一次的结果。
- **SDK**：`ensure_token` 每次拿到 token 都交给直读连接池
  （`ConnPool::set_auth_token`），新连接在 VERSION_HELLO 后自动 `CLIENT_AUTH`；
  token 换了，池里的旧连接全部丢弃重连。
- **集群成员（Peer）**：EN 之间、PS 到 EN 的连接不发 CLIENT_AUTH，它们由集群密钥
  认证（`cluster_secret_design.md`），可以调用 EN 的全部接口。

## 11. 参照

FoundationDB tenant authorization（非对称 JWT，storage 层验，不可撤销靠短 TTL）；
Ceph cephx（MON 当 KDC + 自动续 ticket，对称 —— 我们改非对称）；etcd RBAC
（中心策略，被否的 per-request 中心查形态）；OAuth（access + refresh = 我们的
token + 长期凭据）。
