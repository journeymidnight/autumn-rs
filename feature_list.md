# autumn-rs feature list — OPEN backlog

**Last updated:** 2026-09-29

**Rules:**
- This file tracks the **OPEN backlog only**. A feature that reaches `passes: true`
  is **DELETED** from here — git history is the record, there is no archive file
  (CLAUDE.md rule 13: 定期清理删除，保持整洁).
- `passes` and `notes` are the only mutable fields after a feature is created.
- Out-of-scope / "v2 再做" decisions must be recorded as proper feature entries
  (F-name + Trigger + Scope + Acceptance + `passes: false`), never as plan-file footnotes.

---

## Active

### BUG-EN-CONTROL-PORT-REGISTER — EN 注册控制地址忽略 --control-port
- **Trigger** (2026-10-02 用户): "忽略 --control-port是什么意思？" → 解释后 "4. 修"。
- **Scope**: EN 向 manager 注册的控制地址用 `--control-port`（给了时），否则 advertise 端口 + 1000；多 shard 与单 shard 两条注册路径一致。
- **Acceptance**: 真进程：EN 以 ≠ 数据端口 + 1000 的 `--control-port` 启动，数秒后 manager 记录的最近心跳 ≤ 3 s；去掉修复即红。
- `passes: true`
- **notes** (2026-10-02): `advertise_control_port` / `local_control_port` 供两条路径共用，默认值只在没给 flag 时计算（原 `unwrap_or(port + 1000)` 在 `--port` > 64535 时即使给了 flag 也会溢出 panic，测试约 1.8% 随机失败），没 flag 且溢出时启动报错。单测 `registered_control_port_follows_the_flag`、`an_explicit_control_port_works_with_any_data_port`；真进程 `tests/en_control_port.rs` 单 shard 与两 shard 两条注册路径（7 s 后 last_heartbeat_secs_ago ≤ 3；修复前该值随时间增长、磁盘被标离线）；消融（单 shard 忽略 flag、只多 shard 路径忽略 flag）各自变红。cluster_secret、direct_io_default 真进程测试仍绿。评审（opus）已采纳：溢出、多 shard 覆盖、ops.md 代理说明（显式 flag 同时是监听端口与注册端口）、过时注释（EN 代码、cluster_secret 测试、server CLAUDE.md 的 --advertise 端口说明）。部署脚本都用默认值，生产未触发。

### BUG-SYNC-BARRIER-REDIAL — flush 屏障对连不上的副本每 2 ms 重连 30 s
- **Trigger** (2026-10-02 用户): "EN的话，retry几次后还不行，因为不会出现PEER_AUTH的情况，所以就上报flush失败，上面sst会重新allocate 健康的extent重新flush"；"如果PS在drain的阶段，等待flush 失败，那就直接退出就行"。附带："Python 注释过时 → 修复"。
- **Scope**: `StreamClient::await_extent_synced_to` 对查询失败（连不上 / 超时 / 拒绝）的副本连续失败若干次即返回错误（flush 失败），不再按 2 ms 轮询到 30 s；“已应答但未同步到位”的等待不变。drain 时 flush 失败即退出（数据留在 WAL）。修正 Python `Client.connect` / `Fs.connect` 注释。
- **Acceptance**: 单测：副本只接受即关闭时屏障在 3 次拨号内报错、远小于 30 s；去掉修复即红。真进程：PS 收到 SIGTERM、EN 已停时 drain 立刻报 flush 失败并退出。flush / EN 故障相关集成测试绿。
- `passes: true`
- **notes** (2026-10-02): `await_extent_synced_to` 每个副本连续 3 次查询失败（`SYNCED_QUERY_ATTEMPTS`）即带上下文返回错误；`Ok(None)` / `Ok(Some)` 清零计数；“应答但未同步到位”仍按 2 ms / 30 s。屏障是 flush 的第一步，提前失败只会让 flush 多失败、不会发布 VP 未落盘的 SST；所有失败路径都释放 claim（评审核过）。重试仍查缓存的副本集（`ExtentInfo` 缓存不在此路径失效），死副本在恢复或 recovery 替换并失效缓存前一直被要求。顺带：`manager_retry_tests` 补 `#[cfg(test)]`（正式构建的 dead_code 警告）；Python `Client.connect` / `Fs.connect` 与 `crates/fs/src/read.rs` 注释更正；`python/Cargo.lock` 补上集群密钥提交漏掉的依赖。验证：单测恰 3 次拨号、< 5 s，消融（不放弃）红；真进程 SIGTERM + EN 已停：drain 约 4 ms 报 flush 失败并正常退出（原先在屏障里 30 s）；stream lib 223；manager 集成 bug_flush_timeout_leak、fence_flush_invariant、system_crash_mid_flush、system_extent_failover、system_extent_recovery、system_flush_race_vp_head、system_compact_unflushed_vp_head、system_row_truncate_queued_flush、system_recovery_vp_seed、system_restart_replay_cursor、system_ps_recovery 全绿。评审（fable）无高危；已采纳：文档不再声称重试会重新拉取副本集、Python 异常措辞、fs 注释。未做（待用户定）：运行中 EN 死后屏障失败导致的 flush 卡住（需 EN 恢复或运维 fence）；屏障 / 读失败不上报 `REPORT_DISK_FAILURE`；EN 注册控制地址忽略 `--control-port`。

### REN-CLIENT-AUTH — 客户端凭证 opcode 改名 AUTH_HELLO → CLIENT_AUTH
- **Trigger** (2026-10-02 用户): "所以现在内部rpc是先protocol_hello + peer_auth, 外部rpc是protocol_hello +auth_hello? 那么这么看auth_hello应该改明成client_auth，区分外部还是内部请求" → "是AUTH_CLIENT吗？没改吗？"
- **Scope**: 只改代码 / 测试 / 文档 / CLAUDE.md 中的名字（`MSG_CLIENT_AUTH`、`ClientAuthReq` / `ClientAuthResp`、`CLIENT_AUTH_MAX_PAYLOAD`）；opcode 数值与消息结构不变，不升 WIRE_VERSION；历史账本不改。
- **Acceptance**: workspace 全目标编译；rpc / stream / client / PS / manager lib 与 `client_surface_freeze`（字节冻结不变）绿。
- `passes: true`
- **notes** (2026-10-02): sed 改名 24 个文件 + 手改动词形式；`MSG_CLIENT_AUTH = 0x55`、`ClientAuthReq { token }`、`ClientAuthResp { code, message }` 与改名前逐字节一致，冻结测试的金标十六进制不变（只换键名）。人可读的拒绝消息文本随之变为 CLIENT_AUTH，无代码解析它（匹配的是 "capability token"）。验证：workspace 全目标编译；rpc 97 / stream 222 / client 66 / PS 275 / manager 435 lib、client_surface_freeze 8/8、cluster_secret 4/4 全绿。评审（opus）无阻塞；已采纳：两处 "an" → "a"、两行超 100 列重折、两个 use 列表恢复字母序、三处手改行重折、设计文档框线对齐、`hello` 局部变量改名 `auth_req`。

### F-AUTH-FAILURE-POLICY — 内部 PEER_AUTH 被拒与客户端凭证被拒的处理
- **Trigger** (2026-10-02 用户): "所有的内部RPC都有可能出现PEER_AUTH失败的情况，说明远端不允许访问，直接fatal都可以"；"如果是HELLO_AUTH失败，说明client连接了不允许的服务，也正常返回失败就行，由autumn用户自己判断" → 细化（用户确认）："被 manager 拒绝：退出……被 EN 或 PS 拒绝：把对方当作不可达，打 ERROR 日志，按正常的退避重试，不退出。如果其实是自己配错了，下一次心跳就会被 manager 拒绝，然后退出。"
- **Scope**: (1) 服务端进程（manager / PS / EN）拨号时 PEER_AUTH 失败：对端是 manager → ERROR 并退出；对端是 EN / PS → ERROR，错误照常返回给调用方（按不可达处理），进程不退出。进程内测试与 autumn-op 不退出。(2) SDK 直读时 EN 拒绝客户端凭证（AUTH_HELLO 被拒或读被拒，`PermissionDenied`）→ 不换副本、不回落 PS proxy，返回 `AutumnError::PermissionDenied`。EN 在 authz 配置未知时对 AUTH_HELLO 回 `Unavailable`（暂时状态，不是对凭证的判定）。
- **Acceptance**: 真进程：EN 的 manager 在同地址换密钥重启后 EN 以状态 1 退出并打出 ERROR；manager 被占了 EN 控制地址、持另一把密钥的监听者拒绝后继续运行并打出 ERROR。SDK：EN 以 `PermissionDenied` 拒绝时，副本与 EC 两种 descriptor 都返回 `PermissionDenied`，只访问一个 EN、零次 proxy。EN：配置未知时带 token 的连接得到 `Unavailable`。每项去掉修复后对应测试变红。
- `passes: true`
- **notes** (2026-10-02): `peer_auth::on_dial_failure`（在 `RpcClient::from_conn_as`）：拒绝（`PermissionDenied`）且拨的是本进程 `--manager` 地址（PS / EN 启动时 `designate_managers` 登记）→ ERROR + exit 1；其他地址 → ERROR，错误原样返回（按不可达处理）。按拨号地址判断而不是对端在 VERSION_HELLO 里自称的服务（评审高危：服务端拨号 `expected = None`，占 EN 地址的外人自称 manager 就能让 manager 退出）。SDK `DirectReadOutcome::Denied` → `AutumnError::PermissionDenied`，副本 / EC / stale-heal 三处；`ConnPool` AUTH_HELLO 拒绝带类型；EN `bind` 在 Unknown 时回 `Unavailable`。验证：cluster_secret 4/4（真二进制，含外人分别自称 ExtentNode / Manager 两轮）；client 66、stream 222、rpc 97、PS 275、manager 435 lib 全绿；rpc 集成与 client_wire_admission 绿。消融变红：不退出、对任何地址都退出、按自称服务判断、SDK 去掉拒绝检查、Unknown 回 PermissionDenied。评审（fable）：高危 1 已修并补回归；低：每次被拒都打 ERROR 无限流（按用户要求，未限流）；签名密钥增删后约 5 s 内新 kid 的 token 可能被某个 EN 拒并直接报给调用方（已写入文档）。顺带发现（未修）：EN 注册控制地址恒为 advertise 端口 + 1000，忽略 `--control-port`。

### F-CLUSTER-SECRET — 集群密钥替代 admin token；EN 直读校验客户端 cred
- **Trigger** (2026-10-02 用户): "现在EN有auth检查吗？" → 没有：任何能连 EN 的进程声明 Peer 即可 APPEND / DELETE_EXTENT / FENCE_EXTENT，声明 Client 可按猜的坐标读任意 extent；声明 Peer 还能从 manager `GET_AUTHZ_CONFIG` 读到 admin token。用户："一把密钥替代现在的 admin token，这样EN，PS，MANAGER启动的时候都要这个token，但是对于client来说，需要一个cred"；"是不是强制cred是cluster的配置决定的"；"EN只看身份，并且只有direct-read这一个API"；升级 stop world。
- **Scope**: (1) manager / PS / EN 启动必须给集群密钥文件，缺则拒绝启动；非 Client 连接在 VERSION_HELLO 之后做独立的双向 HMAC 挑战应答，失败即断开；VERSION_HELLO 字节不变。(2) admin token 全部删除（manager / autumn-op / dashboard 的 flag、payload 前缀、principal/namespace 请求字段、`GET_AUTHZ_CONFIG` 下发）；管理操作改由“经集群密钥认证的 Admin 连接”把关；autumn-op / dashboard 用同一密钥文件。(3) 集群开启 authz（manager 配 signing key）时，EN 对 Client 连接要求先 `AUTH_HELLO` 绑定有效 principal（只验身份与有效期 / kid，不验 extent 归属）才服务 `READ_BYTES` / `READ_BYTES_BULK`；未开启时不查。SDK 直读连接在 authz 开启时自动 AUTH_HELLO，token 续期时重连。
- **Acceptance**: 真进程集群（manager / PS / EN 二进制）上：无密钥或错误密钥的 Peer / Admin 连接被拒且 EN 日志记录拒绝；正确密钥的集群读写、split、autumn-op 管理操作正常；无 `--cluster-secret-file` 的服务端拒绝启动。authz 开启时：未 AUTH_HELLO 的 Client 连接 READ_BYTES 被拒，带有效 token 的 SDK 大值直读成功；authz 关闭时直读不需 token。各拒绝路径的回归测试在去掉对应检查后变红。`docs/ops.md`、部署脚本（cluster.sh / docker / baremetal / k8s）更新。数据路径每请求无新增密码学开销。
- `passes: true`
- **notes** (2026-10-02): 实现：`autumn_rpc::peer_auth`（0xF1，与 VERSION_HELLO 同一冻结帧，HMAC-SHA256 双向挑战应答，进程级密钥）；manager / PS / EN 三处 accept 与 `RpcClient::from_conn_as` 接入；WIRE_VERSION 51→52（MIN_CLIENT 仍 43）。admin token 全删（前缀编解码、`is_admin_ps_msg`、5 个请求字段、`GetAuthzConfigResp.admin_token`——原先任何声明 Peer 的连接都能读到它），租户 / namespace 变更并入 `is_admin_mgr_msg`。EN：`ClientAuthz`（每 shard 5 s 轮询，Unknown 拒 Unavailable）+ `client_gate`；`cap_token::{keyring,bind_principal,still_valid}` 与 PS 共用；SDK `ConnPool::set_auth_token`。autumn-op `--cluster-secret-file` / `gen-cluster-secret`，dashboard 必填该参数。验证：单元 rpc 97 / stream 222 / client 65 / PS 275 / manager 435 / fs 27 / fuse 63；新真进程测试 `cluster_secret`（manager 与 EN × Peer/Admin × 无/错/对密钥，WARN 留痕，autumn-op 无密钥报错）；全部集成测试仅剩 BUG-RECOVERY-PINNED-TARGET-TESTS 的 5 个旧失败；`client_window_verify.sh` 完整路径首次可跑并 ALL CHECKS PASSED（wire 51 客户端对 52 集群）。真进程 authz-ON 集群：1 MiB 值 direct-get 字节一致、无回退警告；消融（SDK 不把 token 交给直读池）→ 出现 "direct-read fell back to PS proxy"。消融变红：发起方不验服务端 MAC、服务端接受任意证明、manager 以 open 模式接受、EN 跳过 client_gate、连接池不发 AUTH_HELLO。独立评审（fable）无高危；已采纳：连接池换 token 的代计数检查对“无→有”也生效、EN 不再把 keepalive PING 当读拦截、EN 配置未知时轮询失败打 WARN、SDK 回退警告提到 authz、client_window_verify.sh 补密钥、若干文档漂移。未改：EN 只有一个 manager 地址、不随 leader 切换（REGISTER_NODE / RECONCILE 同样受限，已有限制）；每 shard 各自轮询；已运行的 PS 遇到同地址换了密钥的 EN 时按原有重连节奏约 2 ms 重连、EN 每次打 WARN；perf/ 下两个历史 harness 仍用 `--admin-token`（回放旧二进制，待用户定）。

### REN-VERSION-HELLO — 版本握手改名 PROTOCOL_HELLO → VERSION_HELLO
- **Trigger** (2026-10-02 用户): "PROTOCOL_HELLO是检查version的，和这些没关系" → "改成VERSION_HELLO我同意"。
- **Scope**: 只改代码 / 文档 / 脚本中的名字（模块 `version_hello`、`MSG_VERSION_HELLO`）；wire 字节（0xF0、AUPH）不变；历史账本不改。顺带改正两处“EN 校验 direct-read capability”的错误文档。
- **Acceptance**: workspace 全目标编译；rpc / client / stream lib 与版本准入测试绿；`client_window_verify.sh` 同时认新旧名字。
- `passes: true`
- **notes** (2026-10-02): `cargo build --workspace --all-targets` 通过；autumn-rpc、autumn-client lib 65、autumn-stream lib 218、manager client_wire_admission、PS lib refused 过滤全绿。独立评审（opus）无阻塞；已采纳：ops.md 的 grep 同时匹配旧名、文档注明“开启 authz 时”、重排长行，另改正 cluster_version_design.md 两处同类表述。

### BUG-PROTOCOL-HELLO-REVIEW — 统一 Hello 提交（e8e6be2）评审出的回归
- **Trigger** (2026-10-01 用户): "review 这个新commit" → "fix"。评审发现：Peer 角色的测试调管理类 opcode 被拒，manager/stream 测试大面积红；`integration compaction_merges_small_tables` 在 partition 线程栈溢出（上一提交通过）；`ConnPool::call_timeout` / `call_into_pooled` 把建连放进调用方超时，黑洞地址不再被识别为 `NodeAddrStale`；autumn-op 用 PS 连接探 EN，声明的目标服务不对，开放 extent 长度静默拿不到；PS 等 manager 时认不出建连超时；服务端拒绝版本不符只打 debug；PS 被拒分支的测试断言不可能失败；`connect_raw` 文档称管理入口实为 Client；`client_window_verify.sh` 前提不可能成立且依赖 ruby。
- **Scope**: 只修上述问题，不改 Hello 协议、版本号与客户端区间设计。
- **Acceptance**: 受影响测试套件全绿；栈溢出测试在默认 2 MiB 栈通过；真实 ConnPool 对不回应握手的地址，读超时短于建连上限时仍归类为建连失败，回退修复即红；PS 测试覆盖“握手与写请求同一次发送、写不被投递”和“Client 角色发 SPLIT_PART 被拒”，两项各自消融变红；autumn-op `info` 能拿到开放 extent 的实时长度；`docs/ops.md` 更新。
- `passes: true`
- **notes** (2026-10-01): 已修并验证。另修评审中途发现的两处同源回归：e8e6be2 让 `autumn-fuse` 编译失败（`prefetch_ahead` 布局超出 rustc 深度上限），以及 `ConnPool` 两个等长 5 s 建连计时器竞争导致错误形状随机、`is_liveness_timeout`（只读 `to_string()`）多数情况认不出建连超时。根因：connect+Hello 的状态机内联进每个调用者 future → `RpcClient::connect_as` 内 Box；建连只留一个计时器，两个分类器都读 `{:#}`。测量：栈溢出用例 1 MiB 栈通过（只恢复超时结构时 1.25 MiB 仍溢出）；新 ConnPool 用例对“建连放进调用超时”和“`is_liveness_timeout` 读 `to_string()`”两项消融都变红；PS 窗口用例两项消融变红。全量：rpc/stream/client/fs/fuse/PS lib 绿；manager 111 个目标仅剩 5 个在 e8e6be2^ 上同样失败的恢复用例（见 BUG-RECOVERY-PINNED-TARGET-TESTS）。`extent_pipeline::cq_flushes_fast_ops_while_slow_op_runs` 本机负载下 ratio 0.52–0.56 间歇失败（计时窗口不含建连，HEAD 上评审也见过），未动。本地真集群：`autumn-op info` / `info --part` 显示开放 extent 实时长度 500195 B；wire-52 Peer Hello 触发 manager WARN。`client_window_verify.sh` 只验证了“无可用旧客户端 → 退出 0”分支，完整分支要等出现低于上限的 Hello 版本客户端。独立评审（fable）发现的 `is_liveness_timeout` 与 doc 粘连已修。

### BUG-RECOVERY-PINNED-TARGET-TESTS — 5 个副本恢复集成测试在 main 上失败
- **Trigger** (2026-10-01，BUG-PROTOCOL-HELLO-REVIEW 跑全量 manager 测试时发现): `e2e_lifecycle::e2e_fence_triggers_recovery_dispatch`、`system_correlated_2of3_loss::leg1_correlated_2of3_loss_survives_and_recovery_refills_from_survivor`、`system_corrupt_replica_rebuild::a_replica_reported_corrupt_is_eventually_rebuilt`、`system_recovery_loop_drives::fencing_a_member_rebuilds_the_slot_when_a_spare_node_exists`、`system_wiped_rejoin_truncation::fencing_a_wiped_rejoined_node_triggers_recovery_refill` 恢复从不完成；在 e8e6be2^ 上同样失败，与统一 Hello 无关。EN 日志反复 `recovery task failed ... recovery destination disk is outside the pinned target`（检查来自 0efc2aa）。
- **Scope**: 先查清是测试夹具（手工注册的节点/磁盘身份与 EN 真实 disk_id 不一致）还是 0efc2aa 的生产缺陷，再按根因修。
- **Acceptance**: 5 个用例全绿；若是生产缺陷，加能在修复前变红的回归测试。
- `passes: false`

### BUG-MERGE-STALE-SOURCE-DEDUP — stale checkpoint extent counts can assign replay to the wrong source max_seq
- **Trigger** (2026-09-29 external review of BUG-MERGE-SOURCE-REPLAY-OFFSET): existing `dedup_at` derives post-merge source regions from cumulative checkpoint-time `log_extent_count`. If one source grows while another truncates, stale counts can misattribute an extent to the other source; independent source sequence spaces then make `ts <= wrong_src_max` capable of dropping an unflushed record.
- **Scope**: establish a durable source-boundary representation at merge time or remove count-derived source attribution without reverting to unsafe global sequence dedup. Keep the cursor-offset replay optimization independent from this work.
- **Acceptance**: construct a reachable source-growth plus prefix-truncation merge shape with overlapping independent sequence numbers; crash-reopen preserves every ACKed record; the pre-fix implementation fails the test; no WAL/checkpoint format change unless explicitly approved.
- `passes: false`

### BUG-PS-SHUTDOWN-CLONE — background region sync can reopen a drained partition
- **Trigger** (2026-09-29 major-row-reclaim verification): `system_restart_replay_cursor::graceful_restart_replays_only_past_the_checkpoint` hit its 30 s shutdown deadline; trace showed "graceful drain complete" followed by region sync reopening the same partition.
- **Cause**: `PartitionServer` derives Clone, but `shutting_down: Cell<bool>` is copied by value; the supervised region-sync/heartbeat clones never observe shutdown's flag. The entry-only check in `sync_regions_once` also needs review across awaited manager/open calls.
- **Scope**: share the shutdown state across PS clones; prevent in-flight region sync from publishing/reopening a partition during shutdown, without losing drain coverage.
- **Acceptance**: deterministic clone/shutdown and in-flight-sync regression; graceful shutdown finishes without reopening any partition; restart replays only the tail; ablation fails.
- `passes: false`

### F-CHAOS-PS-RESTART — chaos 覆盖 PS 真进程重启与 checkpoint / row stream 结构检查
- **Trigger** (2026-09-29 用户): "chaos test为什么之前没有覆盖1a，1b的问题，需要加chaos测试" → "先不管perf check，chaos还是要增加 1. checkpoint 与 row stream的结构检查 2. PS 真进程和两种重启， 然后开始跑chaos测试"。之前 system_chaos 的 PS 在测试进程内、从不重启，读的都是反复覆盖的最新值，没有任何检查核对 checkpoint 引用的 SST 是否还在 row stream 里。
- **Scope**: system_chaos 的 PS 改为 `autumn-ps` 子进程（二进制可替换，便于拿旧版本 A/B）；新增 nemesis 动作 `psterm`（SIGTERM → drain 退出 → 重启 → 等 ready）与 `pskill`（SIGKILL → 重启 → 等 ready）；每个 nemesis 动作之后与 verify 之前，对每个分区检查：恢复会读的 checkpoint（每个 meta extent 的最后一条）列出的 SST 所在 extent 都还在 row stream 里；verify 前再做一次崩溃重启，全部分区必须重新打开；传输出错的写按结果不确定处理。
- **Acceptance**: HEAD 上多个种子全绿，两种重启都实际执行；重启后 ready 超时、drain 超时、结构违例都判失败；用修复前的 `autumn-ps`（35d0baf）跑，结构检查或重开能报出 1b 的问题，打不出则记录原因；`docs/ops.md` chaos 章节写明。
- `passes: true`
- **notes** (2026-09-29): 已实现并提交。HEAD 上 PS 重启/结构检查在全部 10 轮里零违例、两种重启都执行；另加 `rollrow`、`flushburst` 两个动作塑造 row stream。**未达成**：用 35d0baf 的 `autumn-ps` 跑 8 轮（多种动作配比，row stream 到 15 个 extent）零违例——1b 的形状要 size-tiered 跳过 ≥128 MiB 的大 SST（先大批写入再零星写），chaos 的 256 B / 8 KiB 值造不出来；1b 由 system_row_truncate_live_refs 钉住，检查器本身由 `checkpoint_check_reports_an_sst_outside_the_row_stream` 证明会报（消融变红）。全动作集轮次 7 轮里 4 轮挂在既有的 "physical reclaim incomplete"（见 BUG-CHAOS-RECLAIM-RESIDUE），与本改动无关（旧新 PS 都出现）。是否加"大批写入"脚本化阶段待用户定。
- **notes** (2026-09-29, 用户 "Check replay volume after graceful restarts"): 已加。drain 干净（PS 日志无 flush failed / drain channel cancelled / drain timed out / thread join deadline）的 `psterm` 之后，新进程每个分区的 "log replay done ... bytes" 必须 ≤ 1 MiB，且每个分区都要有这行（缺行即失败）。同种子同配比：HEAD 12 次优雅重启最多 0 字节（绿）；8a4b12a（1a 修复前，只补了这行日志）最多 59–69 MB（红）。仍带多条 checkpoint 记录的分区（merge 后未再 flush——drain 在 memtable 为空时不写 checkpoint）豁免、单独报告：HEAD 的 merge 密集轮实测 926 KB，这个已知缺口是真的。
- **notes** (2026-09-29, 用户 "bulk-load phase 的确需要，但是sst的最大参数要改，可以从理论上出1b"): 已达成。PS 新增 `--flush-mem-bytes`，删掉 `MAX_SKIP_LIST`（已无 skiplist），所有 compaction 尺寸都由 flush 大小推出；chaos 的 PS 用 256 KiB，负载前加 bulk 阶段（6000 个冷 key、6 批各约一个 memtable、每批 flush、每 2 批滚 row tail）。同配比 seeds 1-3：35d0baf（只补了该 flag）三轮全报 CHECKPOINT VIOLATION，seed 3 随后有分区再也打不开；HEAD 三轮零违例、优雅重启回放 0 字节、6400 key 全对；全动作集 seeds 21/22 全绿。另修：最后一次崩溃重启若有分区打不开立即判失败（原来 verify 会重试到 30 分钟超时）；liveness 探针覆盖只含冷 key 的分区。

### BUG-MERGE-MULTI-CHECKPOINT — merge 后 survivor 留着多条 checkpoint，每次重启都重放 victim 的 WAL
- **Trigger** (2026-09-29 用户): "所以merge以后，应该生成新的TableLocations啊！，所以就应该只有一个"。merge 把两边的 meta stream 拼进 survivor，恢复读每个 meta extent 的最后一条，得到两条 checkpoint，从较早的游标（survivor 的）一路重放拼在后面的全部 victim log extent；直到 survivor 下一次 flush 才合成一条，而 memtable 为空时 drain 不 flush，于是每次重启都重放（chaos 实测 926 KB）。
- **Scope**: survivor 打开时（恢复之后、开始服务之前）若发现多于一条 checkpoint，立即发布一条合并后的：memtable 非空就 flush，空则直接写一条列出全部 SST、游标为 log 尾的记录，meta stream 截到一条。chaos 断言 PS ready 时每个分区只有一条。
- **Acceptance**: 确定性测试：merge 后 survivor 服务时 meta stream 只有一条记录，优雅重启回放 < 64 KiB，消融变红；chaos merge 密集配比 HEAD 绿、消融二进制红；merge/split/恢复相关集成测试全过。
- `passes: true`

### F-SST-DELETION-COUNT — SST 记删除条目数，按 TiKV 规则自动 major compaction
- **Trigger** (2026-09-29 用户): 线上删除约 1317 万 key 后重启删除进程，第一次从头 range 扫过这些 tombstone，首页 4096 key 用了 185.4 秒。本地复现：扫描耗时正比于未被 compaction 清掉的 tombstone 及其遮住的旧值；一次 major 后同一页 2.9 s → 29 ms。持续删除时 SETTLE（要求窗口内无新删除）不触发，`unsettled_deletes` 只在内存、重启清零。用户定："在 SST 里记 tombstone 数，改SST格式，这个是必要的"；"DeleteRange还是太困难，不做"；"做删除计数，和RocksDB 和 TiKV 一个规则"；"写一个临时的convert_sst的binary，stopworld->convert_sst->start-new-version"。
- **Scope**: SST MetaBlock 格式 v2，记 `num_entries` / `num_deletions`，服务端只认 v2（v1 报错并提示跑 convert_sst）；PS 周期 tick 按 TiKV 规则（tombstone ≥ 10000 且 ≥ 全部条目 30%，检查间隔 5 分钟）自动发起 major compaction；分区打开时 `unsettled_deletes` 计入 SST 里的 tombstone；临时二进制 `convert_sst`：全停 PS 后把每个分区 checkpoint 引用的 SST 重写成 v2、发布新 checkpoint、截断 row stream，可中断重跑。
- **Acceptance**: v2 编解码单测，v1 被拒；集成测试：删除 ≥ 1 万 key 并 flush 后，无外部触发，PS 自己做 major，tombstone 被清、range 首页变快，消融变红；重启后删除数不丢；convert_sst 在真实进程集群上把 v1 数据转成 v2，新 PS 打开后全部 key 可读（含 split 后 CoW 共享 SST、merge 后分区），重跑幂等；`docs/ops.md` 写明升级步骤。
- `passes: true`
- **notes** (2026-09-29): 已实现，本地提交、未 push（用户"最后也先别push"）。system_deletion_triggered_compaction 三项消融均变红（规则关掉、打开时不计 SST tombstone、跳过的 major 不结算）；真实进程端到端：旧 PS 经 merge + split 写 v1 数据，新 PS 拒开并提示 convert_sst，转换 38 个 SST，重跑为空操作，11000 个 key 逐字节正确、range 恰好列出 8200 个存活 key。2026-09-30：按用户要求删除一次性 SST/FS 转换工具、辅助模块及镜像配置，保留格式拒绝检查。

### BUG-MERGE-MINOR-ORDER — merge 之后的 minor compaction 让旧 SST 遮住新值
- **Trigger** (2026-09-30 用户): "part 反复split, merge， minor compact以后， sst 不是依赖sst的顺序找最新的吗？" → "按 last_seq 排序（HBase 的思路）" → "复现测试" / "同意"。
- **Scope**: 点查从表列表尾部往前找、命中即返回；minor compaction 按 last_seq 连续选输入，却把输出插在第一个输入的列表位置。merge 后列表是 survivor 的表再接 victim 的表（两边 seq 独立），于是输出落在 victim 更旧的表前面。改为表列表始终按 last_seq 排序（打开时、compaction 替换后、merge 恢复写 checkpoint 前），与 HBase 按 sequence id 排 store file 相同。
- **Acceptance**: 确定性测试复现（修前读到旧值）并断言场景形状（各表 seq 顺序、trim 选中的表）；修后变绿；恢复旧放置逻辑的消融变红；相关 compaction/merge/split 集成测试全过。
- `passes: true`
- **notes** (2026-09-30): system_merge_minor_compaction_order；另修 compaction 分块的 last_seq 差一（把下一块第一条的 seq 记到上一块）。未修（非本条范围，已报告）：merge 恢复按字节切块会把同一 key 的版本切进两块（ffb4e8a），排序救不了，要按 key 边界切；GC relocation 可能打破"每次 flush 覆盖更晚的 seq 区间"（reviewer 推断，未复现）；MSG_DIAG_TRACE_KEY 的 fullscan 对 paged SST 一律返回 0。

### BUG-MERGE-REPLAY-NOT-NEEDED — split/merge 之后恢复不该回放源分区的 log
- **Trigger** (2026-09-30 用户): "merge 还是有问题！， merge先stop，然后flush，所有merge就完全不需要从log stream恢复啊" → "不兼容" → "都放行"（撤掉 8c9d267 的 covered-prefix 标记）。
- **Scope**: split/merge 的冻结 drain 一律在 log 末尾写 checkpoint（含已接受的 fence floor）；merge 后第一次打开从最新的源游标开始回放，解析不到的游标跳过；去重统一用回放前的全局 max seq。撤掉 etcd 标记、split 冻结期内分配新 tail、`MSG_STREAM_REPLAY_INFO`、按源分区偏移与去重、merge 恢复分块 flush；`log_extent_count` 不再计算（写 0，每次 flush 省一次 manager RPC）；`convert_sst` 去掉 NOT_INTACT 帧。不兼容旧 build 做的、survivor 尚未打开过的 merge。
- **Acceptance**: merge 后第一次打开回放 < 64 KiB（从最早游标开始的消融回放 1,065,000 字节变红）；split 前只写在 WAL 的 fence bump 在两个子分区重启后仍然生效（checkpoint 不带 floor 的消融变红）；merge/split/恢复/崩溃/lease 集成测试全过。
- `passes: true`
- **notes** (2026-09-30): 两轮 opus 评审。第一轮高危：merge 时游标解析不到就拒绝打开，会因空 tail 被回收而永久打不开 → 改为跳过。第二轮高危：跳过 + 去重门槛 0 会让被覆盖的旧记录遮住 SST 里的新值 → 去重统一用全局 max seq（这条路径没有确定性测试：需要源 tail 被回收且覆盖写所在 extent 被 GC 回收，靠推理覆盖）。

### BUG-CHAOS-RECLAIM-RESIDUE — chaos 的物理回收检查在 fence 后重建的节点上留有旧副本
- **Trigger** (2026-09-29，加 PS 重启 chaos 时发现): 全动作集 system_chaos 7 轮里 4 轮 `verify_gc_reclaim` 报 `physical reclaim incomplete`，残留文件都在被 KillThenFence/fence 过、随后由 recovery 在别处重建了副本的节点上（例：extent 22 在 node 1）。旧版与新版 PS 都出现。
- **Scope**: 查清是检查过严（删除只发给当前成员，非成员残留靠 EN reconcile：3 轮 × 5 min 才收）还是产品缺陷（被替换下来的副本该在重建完成时就删），据此修检查或修产品。
- **Acceptance**: 结论有证据；修后全动作集多种子不再因此失败，且真实泄漏仍会被检查抓到。
- `passes: false`

### F-PS-CORE-CAPACITY — 分区放置按 PS 核容量；允许超卖，manager 感知并按策略消解
- **Trigger** (2026-09-29 用户讨论): `--cpuset` 下每个分区占 2 核（P-log + P-sst），PS 容量 = `cpuset_len/2`，但 manager 放置分区只看各 PS 的 region 数（`compute_region_for_partition`、`rebalance_regions`、`compute_rebalance_moves` 三处），完全不知道核容量。PS 侧预算门是硬拒：`sync_regions_once` 满了拒开（分区一直 `ps=unknown`），`handle_split_part` 满了拒 split，且检查的是父分区所在 PS，而右孩子由 manager 派到最少 region 的 PS，可能不是本机。超出核数的线程 `pick_cpu_for_ord` 返回 `None` 不绑核，继承进程掩码，可能跑出 cpuset 抢 EN/其他租户的核。
- **设计定案（用户确认）**:
  1. `--cpuset` 即该 PS 全部核预算；不带 `--cpuset` 的 PS 容量未知、不参与核容量管理。
  2. 放置顺序：有空 slot 的 cpuset PS（空闲 slot 多者优先）→ 无 cpuset 的 PS → 全满时超卖到 `used/cap` 最低的 PS。容量是软门，不做预留/2PC。rebalance 同样改按 `used/cap`。
  3. 允许超卖：PS 不再因核预算拒开分区或拒 split。超卖分区的线程亲和到整个 cpuset 掩码（浮动但不出 cpuset）。端口序号与核槽位解耦；绑核分区关闭腾出槽位时，把一个浮动分区晋升绑核（线程自行 re-pin，不重开分区）。
  4. manager 能看到超卖（每 PS `used/cap`、浮动分区数，经 `client info`/dashboard 可见），按顺序处理：迁移（集群尚有空 slot）→ 超卖 merge（集群整体满）→ 告警加 PS（无足够冷的相邻对）。动作走 op ledger，在 leader-fenced manager 内执行。
  5. 防跷跷板（用户硬要求：split/merge 绝不能来回切换）：
     - 超卖 merge 可放宽 merge 阈值，但合并后指标必须不超过 split 阈值的 **1/4**：QPS 和 ≤ 3.75K（`SPLIT_QPS_HIGH/4`）、带宽和 ≤ `SPLIT_BW_HIGH/4`（≈44 MiB/s）、imm_full 仍须为 0、每侧大小仍 < `MERGE_SIZE_LOW`（不放宽）。
     - 同一 tick 由一个 planner 统一决策；集群有待执行 split 时不做超卖 merge。
     - split 产生的孩子在 merge 冷却期内不可作超卖 merge 候选；merge 产物在冷却期内不可因超卖被迁移。
     - 为凑同 PS 而做的迁移与 rebalance 共用同一容量评分，只有评分严格下降才做。
- **Scope（分阶段，每阶段独立提交）**:
  - 阶段 1：PS 上报核容量（cpuset 是否显式、`slot_cap`）；manager 在三处放置/rebalance 路径按上述顺序与 `used/cap` 决策；容量可见于 `client info`。manager 换主后容量信息须能恢复（注册或心跳重新带上，不能依赖旧 leader 内存）。
  - 阶段 2：PS 去掉两处硬拒；超卖线程亲和整个 cpuset 掩码；端口序号与核槽位解耦，槽位空出时晋升浮动分区。
  - 阶段 3：manager 超卖处理（迁移 → 超卖 merge → 告警）及防跷跷板规则。
- **Acceptance**:
  - 阶段 1：2 个 cpuset PS（容量不同）+ 1 个无 cpuset PS 的集群，新建/split 出的分区按放置顺序落位；manager 重启/换主后放置仍正确；消融（改回按 region 数）测试变红。
  - 阶段 2：分区数超过 `cpuset_len/2` 时全部可服务；浮动线程的亲和掩码等于 cpuset（读 `/proc/<pid>/task/*/status` 的 `Cpus_allowed_list`）；关闭一个绑核分区后，一个浮动分区在有限时间内变为单核绑定；split 在 PS 满时不再被拒。
  - 阶段 3：超卖时产生迁移或满足 1/4 上限的 merge；构造在阈值边界抖动的负载，断言 N 个 tick 内同一 key range 的 split+merge 次数 ≤ 1；无冷对时只告警不动作；各规则消融能变红。
- `passes: false`
- **notes** (2026-09-29): 阶段 1 完成——PS 注册/心跳上报 `slot_cap`（wire 50），manager 内存保存、换主清空待心跳补回；`ps_placement.rs` 统一排序，放置/split 右孩子/驱逐重放/rebalance/告警共用；rebalance 只做严格改进的移动且不移到已满 cpuset PS（PS 硬拒仍在，移过去会关掉一个在服务的分区）；`info`/dashboard 显示 used/cap。真实集群（etcd+manager+EN+3 PS，含 manager kill -9 重启）4 项放置检查全过；单测消融变红。阶段 3 待处理：告警的滞回带（`rebalance_gap_threshold`）让超卖少于阈值的 cpuset PS 永远当不了源——手动 `rebalance` 能修，自动策略不会（旧按数量的告警同样如此，非回归）；超卖处理 planner 要让 Over 源绕过这个带。

### BUG-POLICY-ACTIVATE-ATOMIC — policy 名称与模式切换跨两次 RPC
- **Trigger** (2026-09-27 dashboard review): `autumn-op auto-policy activate` 先 SET_ACTIVE 后 SET_MODE；manager 的 SET_ACTIVE 保留旧 mode。旧模式为 Armed 时，选择本应 DryRun 的新 policy 会先继承 Armed；第二次请求失败会留下部分更新，其他操作者也可在两次调用之间交错。
- **Scope**: 在 manager 提供一次持久化事务中的 name+mode 更新，让 CLI/dashboard 共用；明确已有 wire 的兼容/升级要求。不能仅换两次 RPC 的顺序或靠 dashboard 本地锁掩盖。
- **Acceptance**: Armed→选择新 policy 的 DryRun 更新只发布一份完整配置；注入持久化失败、leader 切换与并发操作者，不出现部分配置或误 arm 其他 policy；消融测试失败。
- `passes: false`
- **notes**: 两次 RPC 与 manager 保留旧 mode 已按代码核实；窗口内真实误派发尚未复现。本次 dashboard 迁移不改这个跨层契约。

### F-REVIEW-R1-GC-COMPLETE-SCAN — P1 GC 完整扫描证明
- **Trigger**: review.md R1；提前 EOF 或 record 边界短读可绕过 carry 检查并误 punch。
- **Scope**: 每次读取必须满足 want，punch 前检查 sealed_length 和 carry。
- **Acceptance**: 截短到 0/record 边界、有无 checksum 均拒绝 punch 或完整搬迁；重启逐字节验证 live VP。
- `passes: false`
- **notes** (2026-09-20): 已实现逐次 want 精确长度校验和 punch 前 sealed_length/carry 双重校验；5 条 GC streaming 单测通过，新增完整 record 边界及 offset=0 提前 EOF 回归。尚未完成真实双副本截短、checksum 两种状态及 PS 硬重启组合验收，不能按完整 R1 验收关闭。

### F-REVIEW-T3-REAL-CRASH — P2 crash 测试真正停止旧 runtime
- **Trigger**: review.md T3；drop RpcClient 不等于杀 PS/EN。
- **Scope**: 改用可终止 runtime 或 SIGKILL 子进程并等待退出；compact/flush 用 durable/checkpoint barrier。
- **Acceptance**: 证明旧服务退出、故障发生于指定窗口；重启验证 ACK 数据和 tombstone。
- `passes: false`

### F-REVIEW-T4-CHAOS-GATES — P2 chaos 覆盖与历史验收
- **Trigger**: review.md T4；动作零成功、ignored 未运行、未知结果抹掉已 ACK 历史。
- **Scope**: 区分 smoke/定向/长跑；必需动作非零门槛；修正 full-action 脚本；专用 CI；保留 invocation/response 历史及并发允许结果。
- **Acceptance**: 必需动作未进入/完成则失败；包含 manager/PS 故障与在线 Remove、DELETE/TTL/多 writer 的定向覆盖，CI 实际执行。
- `passes: false`
- **notes** (2026-09-20): 普通 manager 测试已解除对系统 FUSE 的无条件依赖；8 个 FUSE 专属目标通过 fuse-tests feature 显式启用，CI 的 clippy/integration 命令保持启用。完整 ignored chaos、动作覆盖门槛和历史 checker 尚未完成。本轮定向验证入口见 scripts/README.md。
- **notes** (2026-09-27): `scripts/transport_chaos.sh` 在 HEAD 上整体跑不起来——自 `67728e4`
  起 `autumn-client` 的 KV 命令必须带 `--namespace`，脚本没带，seed 全部失败，后续 E1–E7 的
  断言因此没有意义。同日 merge 加了分离闸门，E7b 前补了"两侧 compact + 等 `has_overlap=0`"
  （否则 freeze 直接拒、manager kill 落空），**这一步未实跑**。另：该脚本开机时 `kill -9` 本机
  所有 `autumn-*`/`etcd` 进程，多租户机器上会杀掉别的工作树的进程。

### BUG-E2E-FENCE-RECOVERY-STALLS — `e2e_fence_triggers_recovery_dispatch` 在 HEAD 上失败：fence 之后恢复 60 s 内没换掉副本
- **Trigger** (2026-09-27，做 fence 预检时跑到): `cargo test -p autumn-manager --test e2e_lifecycle
  e2e_fence_triggers_recovery_dispatch`（未标 ignore）在 `cd8a956` 上连续失败：3 个真 EN、RF2
  extent 追加并 seal 后 fence 一个成员（此时 fence 返回 OK），轮询 60 s，
  `recovery did not replace fenced victim N with healthy_target M`。与 fence 预检的改动无关
  （改动前的 HEAD 同样失败；改动后测试多了一步等 df 上报）。
- **未查**: 是恢复派发没发生、EN 执行失败，还是完成没被收回来。测试设了
  `AUTUMN_MGR_RECOVERY_GATE=fenced_only`，代码仍读这个 env。
- **Scope**: 先定位卡在哪一段（manager 日志的 dispatch / EN 的 recovery_done / apply），
  再按根因修。
- **Acceptance**: 该测试稳定通过；若根因在生产路径，修复要有消融。
- `passes: false`

### F-REVIEW-V1-MERGE-REPLAY — 待验证：merge replay cursor 可达性
- **Trigger**: review.md 4.1；数值模型不足以证明正常 merge 丢失数据。
- **Scope**: 复现 raw merge、checkpoint 失败、旧状态和 sealed-empty cursor 回收；按可达性决定修复。
- **Acceptance**: 真实调用链固定时序及 ACK 数据验证，记录可达或不可达的证据。
- `passes: false`

### F-REVIEW-V3-COMPACT-CHECKPOINT — 待验证：checkpoint 失败后 GC 恢复链
- **Trigger**: review.md 4.3；compact 在 checkpoint 成功前替换内存表。
- **Scope**: 验证 checkpoint 失败、GC relocation、row truncate 与前台写入交错；证明 durable 集合完整或修复发布顺序。
- **Acceptance**: 固定失败窗口后硬重启，所有 ACK 数据与 tombstone 正确，修复消融能变红。
- `passes: false`

### F-COMPIO-UPGRADE — 升级运行时并分阶段验证 TCP 内核 CPU 降耗
- **Trigger** (2026-09-14，用户要求记录升级计划): 当前 compio 0.18.0 /
  compio-driver 0.11.4 / compio-runtime 0.11.0；H200-1 单 partition 写入采样的
  内核热点是 TCP 收发复制、发送页分配与清零。新版有 send zerocopy、multishot
  和 deferred task-run 等能力，但单纯升级不会自动让现有收发路径使用这些接口。
  依据：[CPU 分析](docs/perf_partition_cpu_20260914.md)。
- **Scope / 执行顺序**:
  1. **建立基线并升级依赖**：以已合入 CRC 复用和绑核修复的版本为基线，目标
     compio 0.19.x（已调研 0.19.1；实施时核对稳定补丁、MSRV 与子 crate 版本）。
     在独立分支适配 breaking API、feature、buffer ownership、取消和任务生命周期；
     同步 workspace 及独立的 `python/Cargo.toml` / `python/Cargo.lock`，检查重复
     compio 版本。先保持现有普通收发方式、并发参数和协议，单独测纯升级效果。
  2. **显式接入大值 send zerocopy**：优先 PS→EN 的复制发送，再评估 client→PS
     和读响应。保留 control/value 分片和完整 CRC；buffer 必须持有到内核零拷贝
     完成通知，不能仅凭发送完成就回池。覆盖部分发送、失败、超时、取消、连接关闭
     和不支持时的普通发送回退。按实际内核、链路、value 大小实测启用门槛。
  3. **分别评估接收与调度能力**：multishot/managed receive、poll-first、
     single-issuer/deferred task-run 逐项接入、逐项 A/B；验证与 UCX eventfd
     progress、线程 affinity、背压和完成通知的兼容。SQPOLL 仅作独立实验；
     不能把 CPU 转移到内核线程，或 NO_IOWAIT 导致的记账变化，当作工作量减少。
  4. **收敛交付**：只启用有稳定收益且通过正确性验证的能力；记录无收益/回退的
     实验，保留回退到基线的路径，更新 crate 指南、`docs/ops.md` 与性能报告。
- **Acceptance**:
  - 固定 key 集合、独立预热、相同 RF3 / NVMe / 分区 / cpuset / NUMA / 内核 /
    UCX 配置，分别比较「当前版本」「纯升级」「每项新能力」；单 partition 为主，
    多 partition 验证扩展性，覆盖 4 KiB、64 KiB、1 MiB、8 MiB，读写与不同深度。
  - TCP loopback、真实跨机 TCP、UCX 分开测；每组重复并记录吞吐、p50/p99、
    客户端/PS/EN 用户态与内核态 CPU、独立内核工作线程 CPU、CPU 秒/GiB、
    分配/复制及 io_uring 提交/完成指标。计数窗口与传输字节窗口必须一致；排除
    启动重放、磁盘满和非受控后台负载，收益须超出重复测试波动，无固定百分比承诺。
  - 回归验证帧/CRC、乱序完成与写入顺序、buffer 回池时机、取消/关闭、部分 I/O、
    故障回退、三副本持久化、TCP/UCX 字节一致性；构建并验证 FUSE、Python 等
    调用方。协议及存储格式保持不变；小值与 UCX 不得出现未经处理的显著回退。
  - 纯升级和新能力的效果分别报告；未测真实跨机或未通过上述验证时，不标记完成。
- **Status**: 2026-09-14 已升级到 0.19.2（compio fork 分支修 CreateSocket 双持有；阶段 2 的副本 TCP zerocopy 为默认关闭的 opt-in）。2026-09-24 被误判为 4K 写回退的根因而整体回退（`3279694`），2026-09-25 隔离核 A/B 证明 0.18≈0.19 后撤销回退（reset + force push）。阶段 3/4 未做。
- `passes: false`
- **notes** (2026-09-15 controlled validation): Compio 0.19.2/cyper 0.9 migration
  implemented with 1,052 library tests and TCP/UCX/Python/FUSE correctness checks.
  Completed 480 fixed-work performance samples plus 160 typed kernel diagnostic
  samples; all 640 window/count/affinity checks pass. See [controlled report](docs/perf_compio_controlled_20260915.md).
  Default-off zerocopy is slower on loopback; no production receive/scheduler
  defaults changed. UCX 64KiB p1 reads still regress ~8%; UCX local protection
  failure reproduced on both 0.18 and 0.19, tracked separately below. Shared-host
  noise limits whole-machine efficiency claims; sampled stacks have ID collisions.
  H200-2 offline, cross-host unavailable. Full acceptance remains passes:false.


### F-UCX-LOCAL-PROTECTION — 四分区 UCX SEND 间歇性本地保护错误
- **Trigger** (2026-09-14): compio 0.19 受控四分区 UCX 写入在 mlx5_1 上出现 Local protection error (synd 0x4 vend 0x52) 并 abort；compio 0.18 基线在相同拓扑的诊断采样中也复现，说明问题早于运行时升级。失败 SEND 包含无效/失效 lkey，根因尚未确定。
- **Scope**: 定位 UCX 1.16 stream vectored send 的 buffer ownership、注册缓存和取消完成生命周期，复现后只保留有证据的修复；不得通过加超时、重试或丢弃失败样本掩盖。
- **Acceptance**: 可重复触发的最小用例；修复消融失败/修复后通过；四分区 TCP/UCX 字节一致性、注册 buffer 取消/复用及重复压力验证；记录吞吐和 CPU 代价。
- **notes**: 受控验证保留了 0.19 普通跑分和 0.18 诊断采样两份崩溃证据。后续重复成功不关闭该问题。
- `passes: false`

> **这个账本只记 autumn-rs 自己的东西。** 下游怎么被 autumn 的改动影响（例如一次 wire
> 版本变更要求哪些内嵌客户端重建）算 autumn 的后果，该记；下游自己的缺陷、进展和上线
> 状态不算，记在它们各自的仓库里。

### F-DISK-REBALANCE — 机内盘间倾斜没有任何东西会纠正
- **Trigger** (2026-09-09): `choose_disk` 现在按负载选盘(见 F-EXTENT-PLACEMENT Scope 1 的
  第二个 commit),但那只决定**新** extent 去哪。一块盘上已有的存量倾斜不会自己消失,
  而**没有任何机制**会把 extent 从满盘搬到空盘。
- **为什么值得单独做**: 跨节点搬迁要过网络、要占恢复配额、要和真恢复抢优先级;
  **机内盘间搬迁是本地拷贝** —— 不过网络、不动副本数、不需要 manager 参与、失败只是白拷一次。
  风险比跨节点低一个量级。HDFS 正是因此把 `hdfs diskbalancer` 和集群级 `hdfs balancer`
  做成两个独立工具。
- **但先量再做**: 改动前 `choose_disk` 是 `HashMap` 随机序的 first-fit,**每个 shard 落在
  各自随机一块盘**,所以多 shard 节点本来就是散开的 —— 真实倾斜有多大**未测**。
  先量:每节点逐盘的 `extent_bytes` 极差(df 里已有),看它是否值得一个搬运机制。
- **Scope(量完确认值得再做)**: EN 本地的一个后台任务,把 extent 从最满的盘拷到最空的盘,
  限速、可中断;`ExtentEntry.disk_id` 与 `.meta` 同步更新;整个过程不改变 manager 视图
  (副本数、成员、eversion 都不动)。
- **Acceptance**: 人为造一个盘间倾斜的节点 ⇒ 收敛;搬迁过程中该 extent 始终可读;
  中途杀 EN ⇒ 重启后不留半拉文件、账目一致。
- `passes: false`

### F-WIRE-VERSION-BY-HAND — 指纹已删除，wire 版本号改为纯手工维护
- **决定** (2026-09-05，用户): 删掉 schema 指纹与 registry 测试，`WIRE_VERSION_MIN/MAX`
  由人维护。已实施：`crates/rpc/build.rs` 删除、`syn` 构建依赖移除、
  `WIRE_FINGERPRINT` / `WIRE_VERSION_FINGERPRINTS` 清空、
  `GetClusterIdResp.wire_fingerprint` 字段摘除（并进未部署的 v36）、
  `wire_compat_check` 改为纯版本区间。五个 schema 文件顶部各加了警示横幅。
- **放弃了什么，说清楚**: 指纹独占的能力只有一个——抓「改了 schema 却没抬版本号」。
  其余全部由区间覆盖（例如那次陈旧 python wheel，它的 MAX 更低，区间检查本就会拒）。
  这个能力现在**没有任何东西替代**，`compat_no_longer_verifies_the_peers_schema`
  这条测试就是为了把这个洞留在代码里可见，而不是只存在于某个 commit 说明里。
- **为什么仍然合理**: 字节哈希造成过真实停机（翻译一条中文注释劈开了滚动中的集群），
  而误报会训练出「刷新记录值继续」的反射——那正是真实改动被放行的路径。
  且 `MIN=MAX` 的停机纪律意味着**不会存在混版本集群**，而混版本正是静默损坏的发生条件。
- **调研结论（2026-09-05，回答"要不要换掉 rkyv"）**:
  - **rkyv 官方不做 schema 演进**。docs.rs 自陈 "lacks a full schema system…
    isn't well equipped for data migration and schema upgrades"；
    issue #164 "Schema evolution" 2021-07 开，至今 open，标签是 "new crate"。
  - **作者自己的 protoss（rkyv 的 schema 演进 crate）已于 2024-11-11 归档**，
    共 6 个 commit、无 release。生态里没有可用方案。
  - 没有搜到任何用 rkyv 做**网络协议**并公开版本管理方案的项目。协议层主流是
    Cap'n Proto / FlatBuffers / protobuf，**三者都把演进做进格式本身**（字段编号 /
    可选字段），所以不需要外挂版本号。rkyv 没有 tag，解码就是按当前 Rust 布局读，
    **忘记抬版本 = 静默读错**，而 protobuf 里最多是丢个字段。
  - ⇒ 这套 `WIRE_VERSION` 不是过度设计，是在补 rkyv 缺的那一层，且无先例可抄。
- **若将来要迁移（实测规模，非估计）**: 206 个 `Archive` 类型
  （manager 144 / partition 45 / extent 16 / cap_token 1）、
  `rkyv_encode` 852 处 + `rkyv_decode` 533 处。
  **FlatBuffers 是错的方向**：它的全部价值在零拷贝访问器，而本树几乎不用——
  `Archived*` 直接读只有 1 处，`rkyv_decode` 是 memcpy 到 `AlignedVec` 再完整反序列化成
  owned，大数据则走帧的裸 tail 完全绕开 rkyv。为一个用不上的能力付 1,385 个调用点的改造。
  **prost 才贴合现状**（解码产物就是 owned struct，多数调用点只换函数名），但迁移的触发
  条件应该是「需要滚动升级、不能再停机」，而不是现在——停机纪律已经挡住了实际风险。
- **`rkyv_decode` 改零拷贝的阻断点（2026-09-05 查清，未做）**: `HEADER_LEN=10` +
  `CTRL_PREFIX_LEN=4` ⇒ rkyv 载荷从帧内偏移 **14** 开始，既非 16 也非 8 对齐。
  那次 memcpy 到 `AlignedVec<16>` **正是让 `rkyv::access` 合法的前提，不是浪费**。
  要零拷贝必须先把帧头补齐到 16 对齐（wire 改动）。且 533 个解码点的类型全变
  （`String`→`ArchivedString` 等），而许多点拿到数据后立刻 clone 进 owned 结构、
  零拷贝买不到东西。**并且没有任何测量指向解码是瓶颈**——今天测到的是 append 0.185 ms /
  端到端 1.15 ms，PS 处理那 ~0.97 ms 的构成完全空白。要做应先量。

### F-STREAM-ATREST-CKSUM — stream 层大 value 的 at-rest 内容校验 + scrub（静默腐化 G12）
- **Trigger** (2026-08-04, chaos 缺口 loop 的 G12，已 reproduce-first 复现 harness `crates/manager/tests/silent_corruption_rot.rs`): sealed extent 的 **value 数据字节**在单副本上被静默翻位后，**全链无检测**：(a) 客户端读回坏字节仍返回 `CODE_OK`（frame CRC 明确排除 bulk value 段；`.meta` CRC 只覆盖 40B 元数据；WAL/SST CRC 是 partition 层、不覆盖 stream extent 的原始 value）；(b) recovery 从坏副本重填时 `verify` 只校 `length==sealed_length` + eversion、**不校内容** → 把腐化洗成权威；(c) EC 转换对坏字节直接编 parity → 固化成 canonical。stream 层**既无 per-extent/block content checksum、也无 scrubber**；确定性副本轮转让坏副本被一致选中（harness 里 25/64 子区间读命中）。这是**设计缺口**（数据完整性面），不是坏代码——today 的裸机盘不会自发翻位、且需要单副本静默腐化才触发，故不是"今天可复现的线上危害"，属于中期加固。
- **Scope（真要做时）**: (1) 写侧对 sealed extent 落 **per-extent/block content checksum**（`.meta` 里加一段覆盖 `.dat` 内容的 CRC/xxhash；注意不能进 append 热路径的每帧 CRC，只在 seal 时对最终内容算一次）；(2) EN 读时（至少 sealed 全值读 + recovery 重填读）验内容 checksum，错则走**现有副本轮转/failover 绕开**坏副本（隔离路径已存在，缺的是检测触发器）；(3) recovery/EC 转换前加内容校验，**拒绝**把校验失败的副本洗成权威/编进 parity；(4) 后台 **scrub loop**：低速重哈希 sealed extent，mismatch 则清该副本 `avali` 位交给 recovery 重建。
- **Acceptance**: 用 `silent_corruption_rot.rs` 的注入点——翻转单副本 sealed `.dat` 字节后：客户端读返回错误（非 `CODE_OK` 坏字节）或自动从好副本服务正确字节；recovery 不再从坏副本洗白（重填结果字节精确）；EC 转换对坏副本报错而非编坏 parity；scrub 能在无外部读的情况下自行发现并清 `avali`。harness 从"记录暴露"翻成 fail-until-fixed 正确性断言。
- **Status**: `passes: false` (2026-08-04；2026-09-04 开工) — **增量 1 已落地**：`.ck` sidecar 格式
  （`crates/stream/src/extent_cksum.rs`）、seal 时写、读时验（在 `build_read_future` 上，
  即生产读真正走的那条路），设计见 `docs/autumn_integrity_plan.md`。
  **三条 harness 腿仍绿**，原因写在设计文档里：EN 上没有 seal 事件，那条流程不会写 sidecar，
  要靠增量 2 的 scrub 回填才够得着。剩余：scrub（探测 + 回填 + 经 `DfResp` 上报）、
  EC 转换前置校验、EC 分片的 at-rest 覆盖。
  原始定调保留 — **backlog（用户定调 2026-08-04「g12 放到 backlog 里面」）**：已 reproduce（harness 未提交/已提交见 chaos 套件 `b15168c`），本轮**不实现**，留账本记录。cross-ref memory `project_chaos_gap_loop_findings`（G12 条）。真要动之前先确认触发条件（单副本静默腐化）是否已在真实硬件/线上出现过。
- **notes** (2026-09-27): 上面的 Status 已过期 —— 增量 2 已在 main:scrub(`extent_scrub.rs`,
  含 sidecar 回填、经 `DfResp.scrub_rot` 上报、manager 隔离该副本)与 EC 转换前置校验
  (`e0b8861`,后续 `1ffaa93` / `5720408` 修过)。剩余:EC `.shard{i}` 的 at-rest 内容校验
  (`docs/autumn_integrity_plan.md` 表中仍为 none),以及本条 Acceptance 的逐条复核。

### F-EN-SHARD-AUTO — default EN shard count to CPU cores (format-side), not a hand-set env
- **Trigger** (2026-07-13, user: "EN 分片确实是核数导向,但目前是手动 env,不是自动...对于集群配置有好处,记下来,以后做"): EN sharding IS core-oriented — `AUTUMN_EXTENT_SHARDS` should track io_uring cores (one shard = `extent_id % shard_count`), but it's a MANUAL env (default 1). Operators must hand-count cores AND keep three things in lockstep. It is NOT a simple "read `available_parallelism()` in the EN" because shard_count is coupled through a chain: **(a)** EN ports are static/registered-once — `autumn-op format --shard-ports <csv>` stamps the N ports into etcd and the manager routes by that list forever (stream CLAUDE.md "EN ports are FUNDAMENTALLY static"); a runtime-auto shard count would desync from etcd → manager black-holes shards 1..N. **(b)** the k8s overlay Service must enumerate exactly `shard_count` data+control ports (`9101+i*10` / `10101+i*10`); auto-shard needs the Service port list generated too. **(c)** `AUTUMN_EXPECT_NODES` / presplit sizing are tuned against the shard fan-out.
- **Scope (when triggered)**: make the CORRECT layer (deploy/format, NOT the Rust EN process) default the shard count to cores when unset — entrypoint.sh: `AUTUMN_EXTENT_SHARDS` unset → `nproc` (clamped to a sane max); `autumn-op format` auto-derives `--shard-ports` from it; the k8s overlay generates the per-pod Service port list from the same value (kustomize can't loop → a small generator or documented N-port template). Keep the manual env as an explicit override. Rust EN stays config-driven (no `available_parallelism()` read in-process — the ports must match etcd, which only `format` knows). Cross-ref stream CLAUDE.md "serve_with_control is fail-stop … EN ports are FUNDAMENTALLY static".
- **Acceptance**: a fresh deploy with no `AUTUMN_EXTENT_SHARDS` set brings up one shard per core, `format` registers the matching ports, the Service exposes them, and the manager routes to all shards; the manual env still overrides.
- **Status**: `passes: false` (2026-07-13) — recorded for later per user. Deploy/format-layer change (entrypoint + format + overlay), NOT an EN-process change; the coupling chain above is the reason it's "manual by design" today, not a bug.

### F-EN-WIRE-AUTH — EN wire 面无鉴权：破坏性 op（APPEND/DELETE/FENCE/…）对任何内网对端开放
- **Trigger** (2026-09-27, 设计讨论: "安全的话，EN 最好也有 auth"): `data_plane_authz_design.md`
  §9 只把 **client 直读旁路**（大值 `MSG_READ_BYTES` 直连 EN）记为明确接受（WON'T-DO），
  但读旁路与破坏面来自同一事实：EN 不区分对端。client 通过一次 `GetRedirectResp`
  descriptor 就合法拿到 `(en_addr, extent_id, eversion)`，之后在同一个裸连接上不仅能读，
  还能发 `MSG_APPEND`（往别人的 extent 追加垃圾）、`MSG_COMMIT_LENGTH`（谎报长度）、
  `MSG_DELETE_EXTENT` / `MSG_FENCE_EXTENT` / `MSG_ALLOC_EXTENT` / `MSG_COPY_EXTENT` /
  `MSG_CONVERT_TO_EC` / `MSG_WRITE_SHARD`——一个流氓 client 能毁掉**所有租户**的数据完整性。
  这是未在威胁模型里讨论过的面，比已接受的读旁路更值得先修。
- **形状（已讨论）**: 不照搬 PS 的 tenant-prefix 模型——EN 的操作单位是 extent_id，
  不租户可判定，且 extent 随 split/merge/GC 高频生灭，per-extent capability 会让 mint
  频率与 token 尺寸崩掉。改为**按操作等级分两层，复用现有 Ed25519 keyring**：
  (1) 破坏性/管理 op 要求"节点 token"——manager 在 PS/EN 注册时签发
  `typ: "autumn.node.v1"` 的同族 token（复用 `cap_token.rs` 全套 codec，domain 分开），
  EN 轮询现成 `MSG_GET_AUTHZ_CONFIG` 拿公钥，连接级验一次绑 principal，之后每请求零开销
  （与 PS 的 AUTH_HELLO 同构；opt-in 同 PS：manager 不配 key 文件则全关）；
  (2) 数据读保持开放（维持 WON'T-DO），最多做到"持任意有效 cap token 即可读"挡匿名
  actor；**不做** per-extent 租户隔离（需 PS 持签名权或逐读 mint，改变信任模型，
  可信内网前提下不值）。性能账：验签只在建连时一次（~几十 µs），数据面零新增。
- **Scope**: (a) 先把"破坏性 op 无鉴权可达"补进 `data_plane_authz_design.md` 的威胁模型
  节（读旁路已有记录，写/删面没有）；(b) 节点 token 的签发（manager）、分发（PS/EN
  注册路径）、EN 侧连接级验证与 op 分级 gate；(c) EN 侧拒绝指标（复用 `AuthReject`
  分类）；(d) 消融：去掉 gate 后一个未认证 client 能 APPEND/DELETE 成功。
- **Acceptance**: 开启后未持节点 token 的连接发 `MSG_DELETE_EXTENT` / `MSG_APPEND` /
  `MSG_FENCE_EXTENT` 等被拒且按类上报 metric；持 token 的 PS/manager 数据路径行为
  逐字节不变（建连多一次验签，吞吐回归不劣化）；authz 未配置时行为与现状完全一致；
  消融测试在无 gate 时变红。
- **Status**: `passes: false` (2026-09-27) — 仅记录，未开始。

### BUG-KVC-POOLNAME-STR — `str(PoolName.KV)` 在 py≥3.11 得到 `'PoolName.KV'` 而非 `'kv'`
- **Trigger** (2026-09-04, fable 评审 L3 接口解析改动时顺带发现，**在本次改动之外**):
  `PoolName` 是 `(str, Enum)`。py≤3.10 的 `str()` 返回值 `'kv'`，**py≥3.11 返回限定名
  `'PoolName.KV'`**（`enum` 的 `__str__` 在 3.11 改过）。`sglang_backend.py` 有三处
  假设了前者：~398 的注释（"the KV pool's segment is 'kv', which is what
  `PoolName.KV` stringifies to"）、413 的 `str(pool_name) == DEFAULT_POOL_NAME`
  比较、447 的 `hit_count = {str(PoolName.KV): kv_pages}` 键名。评审在 3.9.6 与 3.13.8
  两个版本上实测确认。
- **影响范围（未查证的那半）**: 取决于调用方往 `transfer.name` 里放什么。已知的调用方
  都传**普通字符串**，那条路不触发。sglang 是否会送 enum 成员进来 **未查证** ——
  评审只能确认 controller 是从上游调用者转发 `transfer.name` 的。若会，后果是 v2 的
  KV key 落到 `"PoolName.KV"` 这个段下（与 v1 不再字节一致，跨版本读不到），
  且 `batch_exists_v2` 的结果字典键名对不上。
- **Scope**: 三处统一改成取 `.value`（或 `PoolName.KV.value`），并加一条断言/单测把
  "v2 的 KV 段必须与 v1 字节一致"钉住 —— 那是 v2 设计时明确写下的性质
  （"v2 keys for the KV pool are byte-identical to v1's — this is additive, not a migration"）。
- **Acceptance**: 先查证 sglang 是否真的传 enum 成员（传字符串则本条降级为整洁性修补）；
  修后在 py≥3.11 上 v1/v2 的 KV key 逐字节相同。
- **Status**: `passes: false` (2026-09-04) — 既有缺陷，非本轮引入。已知调用方传字符串
  而非 enum 成员，所以现在不触发；严重度取决于上面 sglang 那半的查证结果。

### F-CHAOS-DISK-FAULT — 多盘形态有了，但"一块盘坏了"本身还没有 nemesis
- **Trigger** (2026-09-10，fable 评审确认): chaos 的 EN 现在是多盘的
  (`AUTUMN_CHAOS_DISKS_PER_EN`,默认 2),既有 nemesis 因此第一次跑在多盘形态上。
  但**没有任何 nemesis 会弄坏一块盘**,而且两块"盘"是同一个 tempdir 下的子目录、
  同一个文件系统 ⇒ `disk_stats()` 返回相同数字、全程都是 Online。
  所以多盘真正覆盖到的只有:多目录 format/注册、重启时跨盘 `load_extents`、
  以及 `choose_disk` 的挑选与打平(open/held/last_picked)。
  `Full` / `Faulted` 和"只重建那块盘上的 sealed slot"**依然零 chaos 覆盖**。
  (我第一版把这三样都写进了 docs/ops.md 和结构体注释的"已覆盖"里,是**夸大**,
  评审指出后已改成如实描述 —— 记在这里因为这正是账本存在的意义。)
- **两条已核过的约束,写下来免得下一轮重新踩**:
  1. **不能用"杀掉 EN → chmod 000 该盘 → 重启"**。`read_and_verify_cluster_id`
     (crates/server/src/bin/extent_node.rs)在启动时读每个盘的 `cluster_id`,读不到就
     `bail!` —— 整个进程起不来。那是杀节点,不是坏一块盘。
  2. **Faulted 粘到进程重启为止**(docs/ops.md 的失败盘 runbook)。所以这个 nemesis
     **不可逆**,不能当成每 tick 都能挑中的普通动作 —— 要么做成一轮一次的 one-shot
     (像 decommission),要么给它自己的预算门,保证任何时刻活着的盘数仍能满足 RF。
- **Scope**: 对一个活着的 EN,把它**某一块**盘的 hash 子目录改成不可写(`chmod 0500`),
  让下一次在该盘上建 extent 得到 EACCES —— 走 `classify_disk_error` 的 Media 臂
  (EACCES 既不在 Capacity 也不在 Process 名单里),EN 把该盘置 Faulted 并在 df 里
  上报 `online:false`。注入前先快照该盘上的 extent id(每个 disk 目录下的 `extent-*.dat`)。
- **Acceptance**: 注入后 (a) 该节点**仍是集群成员**、没有被 fence;(b) 快照里那些
  extent 的该 slot 在限流预算内被重建到别处(manager 的 extent 布局不再指向这台的这块盘);
  (c) 该节点**另一块**盘上的 extent 一个都没被搬;(d) 轮末的逐键校验零丢失。
  没有 `apply_df_disk_health` + 恢复门那条 per-disk 臂时,(b) 必须是红的。
- **Status**: `passes: false` (2026-09-10) — 本轮只交付了多盘形态本身;盘级故障注入
  是独立的一件事,且因为不可逆需要自己的预算设计,不适合顺手塞进同一个改动。

### F-EXTENT-PLACEMENT — extent 分片放到哪台，两条路径两套策略，且都不看均衡
- **Trigger** (2026-09-05，AZ 迁移实测暴露): 迁移中发现"逐台下线"会让数据**回流到还没下线的
  机器上**。查证后发现根因不是迁移顺序，而是**同一个问题在代码里有两套互不相干的答案**。
- **事实（已核对代码，非推断）**:
  - **分配路径** `select_nodes`（`manager/src/lib.rs:3752`）: 三层回退
    spacious（healthy 且不在 `space_low` 里）→ healthy（Online 且至少一块 online 盘）→ all，
    **每层都 `pool.shuffle(&mut rng)` 后 `take(count)`** ⇒ 均匀随机。
  - **恢复路径** `dispatch_recovery_task`（`manager/src/recovery.rs:406`）: 过滤掉
    `occupied`（该 extent 现有成员）与 `hard_excluded`（fenced/maintenance/suspected）之后
    **`all.sort_by_key(|n| n.node_id)`**，然后 `for candidate in &candidates` **顺序取第一个
    过限流的** ⇒ **小 node_id 优先、首个命中即用，没有 shuffle**。
  - 两者都**不看已用容量、不看分片数**。`space_low` 只是一个二值的"快满了"信号，
    不是负载度量。
  - **没有 extent 级的再平衡**。`autumn-op rebalance` 搬的是 **partition 在 PS 之间**的分布，
    与 extent 分片在 EN 之间的分布无关。所以一旦倾斜，只有 fence 才会重新洗牌。
- **实测代价（本次迁移，数字真实）**:
  - 稳态倾斜: 全是同规格 i3s.3xlarge（3.5T），却是 node 3=45 / node 83=12，**3.75 倍**。
  - 排空 node 5 的 27 个分片时，**12 个搬到了 node 1 和 3 上——那正是接下来要下线的两台**；
    而 4 台全新的空节点（102/104/106/108）**一个都没接到**。
    机制: 要下线的旧节点 ID 最小（1/3/5/7）＜ 保留节点（9/83/85/102+），
    首个命中即用 ⇒ 旧节点永远优先中签；`per_target<=2` 的限流是唯一让它溢出到高 ID 的力量，
    这也解释了 83/85 为什么能分到一些而 102+ 完全分不到。
  - 把四台**一起 fence**（fenced 进 `hard_excluded`，从候选里彻底剔除）之后立刻改观:
    新四台 0 → 37 个分片，并发 4 → 12（7 个目标各自吃到 `per_target<=2` 的额度）。
- **Scope**:
  1. **两条路径统一到一个放置策略上**。至少要看"该节点已持有多少分片 / 已用多少字节"，
     让选择偏向轻载节点。随机能避免系统性偏置但**不收敛**（balls-in-bins 的方差是固有的）；
     升序 ID 则是**主动的系统性偏置**，比随机更糟。
  2. **手动放置**: 允许运维指定某个 extent 的某个 slot 落到哪个节点，
     形如 `autumn-op place-shard <extent_id> --slot N --node M`（走 admin token）。
     用途是迁移、腾机器、绕开坏节点——现在这些都只能靠 fence 间接影响，粒度太粗。
  3. **extent 级 rebalance**: `autumn-op rebalance-extents [--max-moves N]`，
     把分片从重载节点搬到轻载节点。缺了它，扩容之后新机器只能靠"等别人 fence"才会被用起来。
- **⚠️ 设计约束（不要绕过）**: 放置必须继续尊重 `occupied`（同一 extent 的两个 slot 不能落在
  同一节点上，否则 K+M 的容错度直接下降）、`hard_excluded`，以及 EC 的
  `K+M` 个不同节点的下限。手动放置尤其要在**服务端**校验这些，不能只靠调用方自觉。
- **Acceptance**:
  - 造一个倾斜集群（N 台空 + M 台满），触发一批 recovery，断言分片流向轻载节点，
    且最终各节点分片数极差 ≤ 某个阈值；**消融: 换回 `sort_by_key(node_id)` 该断言变红**。
  - `place-shard` 指定一个合法目标 → 分片确实落在该节点；指定一个已持有该 extent 其它 slot
    的节点 → **服务端拒绝**并说明原因。
  - `rebalance-extents` 在一个人为倾斜的集群上收敛，且过程中 `extent-health` 始终干净。
- **Status**: `passes: false` (2026-09-09) — **Scope 1 已实现,Scope 2/3 未动**。
  Scope 1 落地形态(与用户 2026-09-08 讨论定稿,选 B 档:manager 只选 node,盘由 EN 自己选):
  - 新纯模块 `crates/manager/src/placement.rs`。分数是**分层带宽比较**而非加权和 ——
    加权和需要一个"十个 open tail 值几个百分点的盘"的跨单位常数,那个数只会被调到测试通过
    为止;分层每层可单独测,以后插一级不用重调其它。层次:利用率(5 个百分点一带)→
    open extent 数 → 分片数。
  - **分配**(`select_nodes`)用 d=2 抽样,**恢复**(`recovery_candidate_order`)用全排序。
    分开的理由是评审逼出来的:恢复路径上 `RecoveryRateLimiter` 的 `max_per_target=2`
    加"被限流就跳下一个"本来就在分散突发,抽样只是白付准确度(7 台 3 满时约 1/7 的首选
    仍落在满节点)。抽样只在没有限流器的分配路径上有价值。
  - 负载来源**零新增采集**:`cluster_cap.per_node`(逐盘求和,每 df tick)+ per-node
    open/shard 计数(搭在 `logical_stored` 那趟 30s 分块游标扫描上,整圈完成才发布)。
  - EN 侧 `choose_disk` 从 first-fit 改成看负载(单独 commit)。
  - 消融三条各自变红:恢复换回 `sort_by_key(node_id)`;盘排序里把 open 计数降到 held 之下;
    去掉 `last_picked` 盖戳。
  验收里"各节点分片数极差收敛"这一条**未做** —— 它需要 Scope 3 的再平衡器才能成立,
  Scope 1 只决定新数据去哪,存量倾斜不会自己消失。已做的是"分片流向轻载节点"+ 消融。
  当前可用的替代手段是
  **把要腾空的节点全部一次性 fence**，靠 `hard_excluded` 把它们从候选里剔除，
  从而避免数据回流；这次迁移就是这么做的，有效但粒度粗，且解决不了稳态倾斜。

### F-EC-STARVES-FOREGROUND-APPEND — 一个 EC 转换就能把前台写入饿到超时
- **Trigger** (2026-09-11，追一次分区卡死时量到): 单个 16 GiB extent 的 EC 转换
  把协调节点推到 **952% CPU**(9.5 核)，同一时刻该 EN 的 append fanout 从
  **亚毫秒涨到 300-2454 ms**，六秒内越过 size-scaled deadline 触发软错误。
  节点盘只有 39% util、队列不深 —— 不是带宽饱和，是 **每 stripe 在每个目标节点
  `pwrite 64 MiB + sync_data`**，与 append 路径的 per-burst `sync_data` 抢
  fsync 串行点。
- **现有限流为什么不够**: `ec_convert_parallelism` 默认 1 —— 已经是"每节点一个"了。
  限的是并发数，不是这一个转换消耗的资源。`crates/stream/CLAUDE.md` 里
  "No bytes/s rate cap on EN(deliberate)"那段的论据是"并发上限就够"，
  这次的数据是对那个论断的反例。
- **严重性**: 写路径的三个 bug 修好之后它**不再致命**(不会再把分区卡死)，
  但仍然让写入变慢，并且是那三个 bug 当初能被触发的压力来源。
- **Scope**(未实现): 给 EC 转换一个 **bytes/s 节流**或让它的 stripe fsync
  与前台 append 分离(例如降低 stripe 大小、或让转换走独立的 fsync 节奏)，
  使前台 append 的 p99 在转换期间不越过 deadline。
- **Acceptance**: 在一个持续写入的分区上跑一次 16 GiB EC 转换，
  append fanout 的 p99 不超过 size-scaled deadline 的一半；转换本身允许变慢。
- **Status**: `passes: false` (2026-09-11) — 仅立账。

### F-MERGE-REPLAY-OUT-OF-RANGE — 重放不按 range 过滤，可能让已分离的孩子重新带上兄弟的 key（未复现）
- **Trigger** (2026-09-27，merge 分离闸门的独立评审提出，推断): merge 现在要求两侧
  `has_overlap == 0`（`MSG_MERGE_FREEZE` 拒绝），这个标志只从 SST 算（split 时与 open 时）。
  但 reopen 的 WAL 重放**不做 `in_range` 过滤**，唯一的跳过是 `ts <= extent_dedup`
  （`partition-server/src/lib.rs` `recover_partition` 重放段）；而每个来源的 dedup 取该
  checkpoint 记录里**仍能解析**的 SST 的 `last_seq` 最大值，解析不到时为 0，major compaction
  丢掉最新条目又会压低输出表的 `last_seq`。推断链：祖先有 ≥2 个非空 meta extent（或 meta
  尾被 roll 过）⇒ 最早的 checkpoint vp 在后来的父记录之前 ⇒ 孩子 major compaction 之后那些
  区间按 0 或偏低的值去重 ⇒ 任何一次 reopen 都把兄弟那半的父记录重新插进孩子的 memtable
  ⇒ `has_overlap` 仍是 0，下一次 flush（包括 merge freeze 自己的 drain flush）写出带越界 key
  的 SST ⇒ merge 放行后与本次修掉的现象相同。
- **已做的复现尝试（阴性）**: split → 两侧写/删 → 两侧 major compact → PS 优雅重启 / SIGKILL
  重启 → merge → 读，D/K/J/R 全部正确（一次父 flush、一个 meta extent 的形状，最早 vp 在所有
  父记录之后，构造不出推断链的前提）。另观察到：一侧的删除若是它 checkpoint 前的最后一条
  记录，未设闸门的 merge 之后该 key 读 NOT_FOUND——与"被 compaction 丢掉的 tombstone 在
  reopen 时被重放"一致，是同一机制的无害一面。
- **Scope（复现之后才谈）**: 造出 ≥2 个 meta extent 的祖先（强制 meta roll，或多级 split），
  走完上面的链；若坐实，根因修法是重放时对每条记录做一次 `in_range(rg)` 过滤（同时去掉被
  重放的 tombstone），不是再加一个标志。
- **Acceptance**: 一个确定性复现（或一份说明为什么前提不可达的分析），据此决定修不修；修则
  消融能变红。
- `passes: false`

### F-SPLIT-CARRIED-BYTES-UNBOUNDED — 大 value 分区可以无限长大，而现在没有任何判据会说话
- **Trigger** (2026-09-11，本次 split 判据重设计的直接后果): 硬 size 触发器改读
  **LSM 常驻字节**(`size_bytes`)之后，一个大 value 分区的携带字节(log_stream 里的
  payload)**不再是任何 split 的触发条件**。这是故意的 —— split 切的是 key range，
  CoW 之后两个孩子仍然指着同一批 log extent，不 major compact 就分不开 ——
  但它留下一个没人回答的问题:一个 0 iops、0 B/s、LSM 0 MiB、携带 73 GiB 的分区
  **可以一直长下去**，而三个速率维度全都是静的。
- **需要先量，再决定要不要做**: 携带字节真正影响的是**分区级操作的时间** ——
  reopen/replay、rebalance 搬迁、它三条 stream 的恢复。没有测量说明这些在 73 GiB
  会变差到什么程度，也没有说明拐点在哪。**在量出来之前不要加判据**
  ([[feedback_no_defensive_fixes_for_imaginary_bugs]]:"未来可能挂"不算 bug)。
- **而且即使量出来变差了，split 也未必是解药**: 切一刀不减少携带字节(共享 extent
  照旧)，要等 major compaction 重写 + GC 搬走活值才真的分开。真正的缓解手段是
  compaction/GC/EC，或者把分区**搬到另一台 PS**，这两条都不是 split。
- **Scope(量完确认值得再做)**: 测 reopen/replay 与 rebalance 搬迁时间对携带字节的
  曲线;若确有拐点，触发的应当是那条真正有缓解作用的动作(rebalance / 强制 GC)，
  并给它自己的 `POLICY_KIND_*`，而不是把携带字节塞回 split。
- **Acceptance**: 一条测量曲线(携带字节 × reopen/搬迁时间)，以及据此做出的
  "做/不做"决定被写进本条。
- **Status**: `passes: false` (2026-09-11) — 仅立账，未实现，且**先量再做**。
- `passes: false`

### F-FUSE-READ-LATENCY-NOT-BANDWIDTH — 每次读约 4ms 固定成本；挂载能到 1.3 GB/s，慢的是加载器的读粒度
- **Trigger** (2026-09-12，用户): 「为什么autumnfs读这么慢？」。一个 vLLM-Omni 扩散服务
  从挂载读 127 GB 权重，冷启动 19 分钟。
- **实测**（Wan2.2 I2V pod，`--direct-read true`、`--direct-io`(硬编码) ON）:

  | 读者 | 守护进程侧每次读 | 吞吐 |
  |---|---|---|
  | `dd bs=8M count=384`（pread） | **930.3 KiB**（3072 次读 / 2791 MiB） | **106 MB/s** |
  | 模型加载（整轮） | 74.4 KiB（1,705,984 次 / 126,993 MiB） | 171 MB/s |
  | 模型加载（前段小文件） | 31.3 KiB | ~40 MB/s |

- **一个已被证伪的诊断，记在这里以免重走**: 曾推断 "`FOPEN_DIRECT_IO` 关掉页缓存 →
  readahead 失效 → 每次读只有 31 KiB → 落在 `BULK_MIN_BYTES`(64 KiB) 之下 → 每读还多走
  一次 PS 代理"，并据此加了 `--direct-io` 开关。**930 KiB 那一行否定了它**：同一挂载、
  同样开着 direct-io，`pread` 读者拿到的远在门槛之上，吞吐仍只有 106 MB/s。加载时的
  31 KiB 是**加载器自身的访问模式**（大量小张量读），不是页错误。改动已整体回退
  (2026-09-12)。两处本仓库既有记录当时就该读到：`docs/ops.md` 的 "`O_DIRECT` 探测成功
  → 加载器留在 `preadv` 路径"，以及 `ops.rs` 里"内核把每个请求钳到 1 MiB、
  `set_max_readahead` is inert either way"的实测注释——930 KiB 正是那条钳制的确认。
- **已做完那两组对照（2026-09-12），结论是挂载不慢，按标题改写如下**:

  并发扫描（8 MiB 读，每流不同文件）:

  | 并发 | 聚合 | 每流 |
  |---|---|---|
  | 1 | 85.4 MB/s | 85.4 |
  | 2 | 234.6 MB/s | 117.3 |
  | 4 | 524.4 MB/s | 131.1 |
  | 8 | **1089.7 MB/s** | 136.2 |

  固定并发 8、变读大小:

  | 读大小 | 聚合 | **每次读延迟** |
  |---|---|---|
  | 64 KiB | 122.4 MB/s | 4.28 ms |
  | 256 KiB | 471.7 MB/s | 4.45 ms |
  | 1 MiB | **1316.2 MB/s** | 6.37 ms |
  | 8 MiB | 1354.1 MB/s | 49.56 ms |

  单流同尺寸: 64 KiB → 16.1 MB/s (4.07 ms)、1 MiB → 113.7 MB/s (9.22 ms)、
  8 MiB → 126.7 MB/s (66.23 ms)。

- **结论**: 每次读有一个约 **4 ms 的固定成本，与字节数几乎无关**（64 KiB 4.28 ms
  vs 1 MiB 6.37 ms）。所以 `吞吐 ≈ 读大小 × 并发 ÷ 延迟`。挂载本身能到 **1.3 GB/s**；
  单流 85–127 MB/s 只是"没有并发去掩盖那 4 ms"。**挂载不是瓶颈，不要再去调它。**
- **于是问题转移了**: 模型加载在 ~8 并发下只有 171 MB/s，因为它的读是 74 KiB ——
  把 4 ms 摊在了太少的字节上。**该查的是加载器为什么发 74 KiB 的读**
  （safetensors 按张量切分？`get_many_*` 把一次大读拆成按 extent 的小读？），
  而不是 FUSE 侧。
- **Scope**（未实现）: 在加载期抓一次读大小直方图（守护进程侧按 size 分桶计数），
  确认 74 KiB 是加载器发出的还是我们内部拆的。若是后者，合并相邻 extent 的读是直接收益。

- **notes** (2026-09-27): 对 **mmap 加载器** 上面"不是页错误"的结论不成立：safetensors
  `load_file` 是 `MAP_PRIVATE`，缺页按预读窗口（当时 128 KiB）同步读，每次缺页一个往返；
  netns+veth netem 复现 pod 的 4 ms/读后，冷加载 108 MiB/s，窗口 4 MiB 时 823 MiB/s。
  已由挂载走页缓存 + `--readahead-kb`（commit 见 git log）处理；`pread` 小块读者那部分结论不变。

### F-FUSE-DAEMON-PREFETCH — 守护进程对顺序读自己并行预取，内核的 READ 从内存答
- **Trigger** (2026-09-27，用户："按上面的设计做守护进程预取"): 单流读者只靠内核预读，同一
  时间只有一个窗口在飞——单向 1 ms（≈ pod 每读 4 ms）单流 `dd` ~1.0–1.1 GB/s、mmap 冷加载
  单核 ~740 / 9 核 ~1300 MiB/s；而同一挂载 8 线程并行 `pread` 能到 1.6–2.7 GB/s、并行预取 +
  加载总共约 1.0 s（~1.9 GB/s）。内核窗口是脆弱的杠杆：多线程缺页时 4 MiB 窗口塌到
  124–188 MiB/s。
- **Scope**: (a) 按 inode 检测顺序读前沿（允许几个交错前沿，连续 ≥2 块才启动；小文件、随机读
  不触发）；(b) 前沿之前并行预取若干块（初值 8 × 8 MiB 在飞），READ 落在已取回的块里就直接
  从内存答；(c) 内存：全局预算（`--prefetch-mem-mb`）+ 每流窗口上限；预算不够时新流不预取、
  退回直读，**读永不因预算阻塞**；块被内核读完即释放，前沿越过、流空闲超时、最后一个 fd 关闭、
  失效（inode 的 generation 变了：`meta_invalidated` 重读、本挂载写、truncate）时丢弃，
  空闲超时释放；预取块是普通堆内存（传输层的注册缓冲池只管接收缓冲，数据会拷进调用方的
  `&mut [u8]`），预算独立计——UCX 下守护进程总内存 = 预取预算 + 每读线程注册池；
  (d) 块记下规划时的 generation，读请求带着当前 generation，不一致即丢，陈旧窗口不大于页缓存
  本身；(e) 按 (ino, 块号) 把块和落在块里的读请求分给固定的读线程，由它用 `ReplyData` 直接
  应答，不在派发线程上拷贝，单个文件也能用上全部读线程；(f) 计数：命中、等待、未命中、
  浪费字节，周期性写日志。
- **Acceptance**: 单向 0/1/2 ms × 单核/9 核加载器 × 内核窗口 1–2 MiB，mmap 冷加载与单流 `dd`
  明显高于没有预取时（目标接近并行预取 + 加载的 ~1.9 GB/s），交替 A/B；4K 随机读延迟与写吞吐
  不回退；N 个大文件同时顺序读时守护进程 RSS 不超预算、预算耗尽时读照常；真挂载脚本 7 项仍过，
  新增"预取进行中另一个挂载改写同一文件"必须读到新字节，关掉失效丢块时变红；
  `fuse_inval_deadlock.sh` 仍过。
- `passes: false`
- **notes** (2026-09-27): 已实现并按用户决定**默认开**合入（`--prefetch-mem-mb 1024`）。验收对照：
  单流 `dd` 0.69 → 1.7 GB/s、vLLM Qwen 7.9 → 4.8 s、真实 MiniMax-H3（vLLM-Omni TP4）243 → 203 s
  （配 `--disable-multithread-weight-load` 197 s）、4K 随机读 p99 不变、真挂载 PREFETCH 在两层
  generation 检查一起消融时变红、`fuse_inval_deadlock.sh` 过。**未达成的两项**：(1) mmap 冷加载
  并非处处更高——零延迟单核 −12%/多核 −37%，合成的多线程 CPU 拷贝 1/2 ms −25%/−38%（开着时内核
  READ 多一倍多、更碎，原因未在内核核实）；(2) "RSS 不超预算"只对预取块成立（预算 128 MiB 时 RSS
  峰值 287 MiB）。写路径不经过预读，未单独复测。

### BUG-FUSE-CACHED-META-AFTER-LEASE-LOSS — lease 丢了之后缓存的 meta / 页没有人再失效（未复现）
- **Trigger** (2026-09-27，挂载改走页缓存那次的独立评审推断，读代码得出): 挂载走页缓存之后
  两个既有窗口的暴露面变大。(a) 心跳 `NotHeld`（manager 在 poll 仍正常时忘了这个 lease，如
  TTL 过期）只删 `held_leases` 条目，不调内核 invalidator、不标 `meta_invalidated`：仍开着
  的 fd 继续信任旧页，之后的写者关闭也不会再推给本挂载。(b) manager 重启后 lease version
  从 1 重来，`inode_cache_needs_reload` 的 `cached < acquired` 比较失效；dentry 仍在（
  `InodeState` 被 lookup 计数钉住）的已关闭 inode 再次打开时，Open 已经现读了 meta
  （`fresh_meta`）却只在未缓存时才用，GETATTR 继续答旧 size。poll 失败路径（manager 重启时
  通常先走到）已经会标记全部 held ino，所以 (b) 只剩"重启期间本挂载未持 lease"的 inode。
  (c) PyO3 `acquire("r")` 在已持写的 inode 上无条件把 `mode` 改成 READ（`python/src/fs.rs`
  acquire 分支）：之后 `write_lease_for` 发 ANON 写，`get_inode` 的"本会话是写者"豁免也失效。
  既有问题，挂载侧读 open 只 `add_ref` 不改 mode。
- **Scope（复现之后才谈）**: (a) 让心跳在 manager 仍在线时收到 NotHeld（停心跳超过 TTL 再
  恢复）；(b) 挂载 A 读后关闭（dentry 保留）→ 重启 manager → B 追加 → A 重开后 `stat`。
  坐实再修：(a) NotHeld 与 poll 失败同样处理；(b) Open 手里的 `fresh_meta` 在 inode 不脏时
  直接替换缓存的 meta；(c) 绑定写 → 读 acquire 只加引用、不降 mode（与挂载一致）。
- **Acceptance**: 各自确定性复现（或证伪），修则消融能变红。
- `passes: false`

### BUG-FUSE-UMOUNT-JOIN-HANG — 普通卸载后守护进程可能永不退出（未复现）
- **Trigger** (2026-09-27，SIGTERM 优雅退出的独立评审读代码发现，既有问题): `umount` /
  `fusermount -u` 之后 `Session::run` 因 ENODEV 返回，`main` 接着 `compio_handle.join()`；
  派发循环只在收到 `FsRequest::Destroy` 或 `rx.next() == None` 时退出。此刻 bridge 的 sender
  还活着（`main` 里的 `bridge`、`Session` 内 `AutumnFs` 的那份），而 `Session::drop`（会发
  Destroy）要到 join 之后才跑。推断（内核知识，未在本机核实）：非 fuseblk 挂载内核不发
  FUSE_DESTROY ⇒ join 永远等下去，进程在没有挂载的情况下每 30 s 跑一次 `periodic_sync`。
  SIGTERM 现在能把它救出来（信号线程发 Destroy）。
- **Scope（复现之后才谈）**: 挂载 → `umount` → 看守护进程是否退出；若坐实，修法是 join 之前
  `drop(session)`（一行），并在脚本里加一条卸载后进程必须退出的断言。
- **Acceptance**: 确定性复现（或证伪），修则消融能变红。
- `passes: false`

### F-FUSE-BIG-IO-TUNING — writeback cache + splice 零拷贝，把大 IO 的 FUSE 开销压进 5%
- **Trigger** (2026-09-16，用户在 FUSE 性能讨论后确认的三件套之一): 实测
  4K 随机读延迟 0.44ms 中 FUSE 跨用户态开销仅占 2–10%（~10–50μs），顺序大 IO 上
  该比例更低；max_read 已 8MB。剩余可白拿的内核侧开关是 writeback cache 与
  splice_read/write（fuser 支持）。
- **Scope**: (a) 挂载选项开 writeback cache（内核页缓存回写），评估与现有
  dirty-inode flush / lease 撤销失效语义 (`evict_revoked_held_leases`、
  `notify_inval_inode`) 的交互——writeback 会把脏数据驻留内核，lease revoke 时的
  内核失效路径必须保证一致性，这是主要风险点；(b) `splice_read`/`splice_write`
  零拷贝路径接入 read_pool/write 管道；(c) 逐项 A/B，无收益即回退（同账本惯例）。
- **Acceptance**: 固定基线对照——大文件顺序读写、8 并发读、写后 fsync 延迟，
  每项开关单独 A/B，收益须超出重复测试波动；lease revoke 期间内核页缓存无脏数据
  丢失（注入 revoke + 读回校验）；与 BUG-FUSE-INVAL-ON-DISPATCHER-THREAD 的
  复现步骤联测不引入新死锁。
- **Status**: `passes: false` (2026-09-16) — 未开工。先于 F-FUSE-IORING-PASSTHROUGH。
- `passes: false`
- **notes** (2026-09-26): BUG-FUSE-INVAL-ON-DISPATCHER-THREAD 已修并关账，它的复现步骤就是
  `scripts/fuse_inval_deadlock.sh`（docs/ops.md「Kernel cache invalidation must not wedge a
  mount」）。writeback cache 会让页缓存多出脏页，联测时这条脚本必须仍然通过。
- **notes** (2026-09-27): 挂载已不用 `FOPEN_DIRECT_IO`，读写都走页缓存
  （写是无 writeback cache 的直写，吞吐与 direct-io 持平 272.8 vs 272.8 MB/s）；`max_background`
  已是 64。本条剩 writeback cache 与 splice；writeback 联测还要加 `scripts/fuse_page_cache.sh`。

### F-FUSE-IORING-PASSTHROUGH — FUSE io_uring 提交 + passthrough 读直达
- **Trigger** (2026-09-16，用户确认): Linux 6.x FUSE 支持 io_uring 提交路径与
  passthrough 模式（读直达底层文件、跳过用户态拷贝），是"用户态实现复杂度 +
  接近内核态大 IO 性能"的折中。fuser crate 已有实验性支持。前置依赖：
  F-FUSE-BIG-IO-TUNING 先行（更小代价先拿大头）。
- **Scope**: (a) 确认目标内核版本支持项（fuse.io_uring 需 6.14+ 档位，passthrough
  需 6.x + 挂载 `-o allow_passthrough` 类选项，先在 H200-1 的实际内核上核对）；
  (b) fuser 实验特性接入评估——读路径 passthrough 要求底层文件句柄与 FUSE inode
  的映射，autumnfs 的"文件"是 KV/extent 聚合而非本地文件，**passthrough 只可能
  作用于本地缓存层或 page-cache 命中路径**，先做可行性 spike 再定形态；
  (c) io_uring 提交替换 /dev/fuse 同步 read/write 循环，与 compio runtime 的
  事件循环共存性（F-COMPIO-UPGRADE 同族问题，共享调研结论）。
- **Acceptance**: spike 阶段——在真实内核上证明 passthrough 对"非本地文件"形态
  可行或不可行，结论写回本条；若可行，A/B 实测大 IO 吞吐与 4K 并发延迟，
  收益须超波动；正确性回归同 F-FUSE-BIG-IO-TUNING。
- **Status**: `passes: false` (2026-09-16) — 未开工；可行性未证，passthrough 与
  非 POSIX 后端的适配形态是最大未知。
- `passes: false`
