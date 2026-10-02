# 集群成员认证（cluster secret）

整个集群共享一把密钥。manager、PS、EN 启动时必须给出它；声明为集群成员（Peer）
或运维工具（Admin）的连接，建连时必须证明自己持有它。客户端不持有它：客户端的
身份由数据面 capability token 证明，见 `data_plane_authz_design.md`。

## 1. 威胁模型

- **防**：任何能连上集群端口、但不持有密钥的进程，冒充集群成员或运维工具——
  向 EN 发 `APPEND` / `DELETE_EXTENT` / `FENCE_EXTENT`，向 manager 发
  `TRUNCATE` / `ACQUIRE_OWNER_LOCK` / 读 `GET_AUTHZ_CONFIG`，或执行运维操作
  （fence / remove / merge / namespace / principal / bootstrap / split / gc …）。
- **不防**：能读或改写流量本身的攻击者（不加密、无 channel binding，不上 TLS）；
  被攻破、因而持有密钥的集群成员（PS / EN 与运维工具持有同一把密钥，任何一方
  沦陷都等同运维权限）。

## 2. 三层各管什么

| 层 | 回答 | 在哪 |
|---|---|---|
| VERSION_HELLO（`version_hello.rs`） | 双方版本能否互通；连接说的是哪套接口（Client / Peer / Admin） | 每条连接的第一帧 |
| PEER_AUTH（`peer_auth.rs`） | 声明 Peer / Admin 的连接是否持有集群密钥 | 紧跟 VERSION_HELLO，仅 Peer / Admin |
| AUTH_HELLO（capability token） | 客户端是哪个 principal、能读写哪些 key | 客户端连接上的业务帧（PS；开启 authz 时也在 EN） |

VERSION_HELLO 声明的身份只决定版本规则和接口范围，本身不是凭证；PEER_AUTH 让
Peer / Admin 这两种声明需要拿出证明。

## 3. 握手

VERSION_HELLO 成功后，双方仍在原始字节流上（业务 reader 尚未启动），用与
VERSION_HELLO 相同的定界帧格式（opcode `0xF1`，magic `AUPA`）交换：

```text
server → challenge  "AUPA" | mode u8 (0 open, 1 required) | server_nonce[32]
client → proof      "AUPA" | client_nonce[32] | client_mac[32]
server → result     "AUPA" | verdict u8 (0 ok, 1 refused) | server_mac[32]
mac = HMAC-SHA256(secret, "autumn-rs peer-auth v1" | side | service | role | server_nonce | client_nonce)
```

- **双向**：服务端验客户端的 mac，客户端也验服务端的 mac。冒充成员地址的监听者
  过不了客户端这一关。
- **不传密钥**：线上只有 nonce 和 mac。每条连接的 server_nonce 都是新的，录下的
  proof 无法重放到另一条连接。
- **只在建连时做一次**：连接池里的连接长期复用，数据路径上每个请求没有任何新增开销。
- **Client 连接跳过**：双方从 VERSION_HELLO 的结果知道身份，Client 连接不交换任何字节。
- **open 模式**：没有安装密钥的服务端回 `mode = 0`，交换到此结束。只有进程内测试
  会这样（服务端二进制没有密钥就拒绝启动）。持有密钥的客户端拒绝 open 服务端，
  因为冒充者恰好会这样回答。
- **拒绝要留痕**：服务端拒绝时打 WARN，带对端地址、声明的身份和服务：
  `PEER_AUTH refused a connection holding a different cluster secret`，以及
  `PEER_AUTH: connection gave no cluster-secret proof`（对端没有密钥，直接断开）。

## 4. 运维操作的门

manager 上只有 Admin 连接能发的 opcode 是 `manager_rpc::is_admin_mgr_msg` 这张表
（fence / remove / maintenance / EC / create-stream / upsert-partition / merge /
op-submit / principal / namespace / set-presplit）；Peer 连接发这些会被
`check_opcode` 拒绝。Admin 连接必须先过 PEER_AUTH，所以这张表就是全部的门，
请求体里不再携带任何 token。PS 上的 split / maintenance 不在客户端接口内，只有
Peer / Admin 连接能发，同样由 PEER_AUTH 把关。

manager 自己驱动的 split / flush / gc（auto-policy、merge 前的 flush）是它以
Peer 身份连 PS 发出的，与运维工具走同一条认证路径。

## 5. 密钥分发

- **manager / PS / EN**：`--cluster-secret-file <PATH>`，必填，缺失即退出（exit 2）。
  文件内容去掉首尾空白后至少 32 字节。`autumn-op gen-cluster-secret` 打印 64 个
  十六进制字符。用文件而不是命令行参数，密钥不会出现在 `ps` / `/proc/<pid>/cmdline`。
- **autumn-op**：全局 `--cluster-secret-file`（子命令前后均可）。它以 Admin 身份连接，
  所以只读命令也需要；不连集群的命令（`gen-signing-key`、`gen-cluster-secret`）不需要。
- **autumn-dashboard**：必填 `--cluster-secret-file`，把路径转给每次 autumn-op 调用。
- **客户端（SDK / fuse / kvcache / S3 网关 / autumn-client）**：不持有。
- rs 代码不读环境变量：env → flag 的翻译在 `cluster.sh` / 部署层（`cluster.sh`
  把密钥放在 `$DATA_ROOT/cluster.secret`，首次启动生成）。

进程内的密钥是全局的（`peer_auth::install`，与 transport 选择同一形态）：一个进程
只属于一个集群，所有 Peer / Admin 连接证明同一把密钥。

## 6. 轮换

一个进程只认一把密钥，轮换需要整集群停机：生成新文件，分发到所有节点和运维机器，
全部重启。没有双密钥过渡期。

## 7. 性能

每条 Peer / Admin 连接建立时多 1 个 RTT（challenge 与 VERSION_HELLO 的响应背靠背
发送，proof / result 再一个往返）和两次 HMAC-SHA256。数据路径为零。
