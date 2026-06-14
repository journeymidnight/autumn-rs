# WAL replay 自愈环 设计（v2，2026-06-14，coco-reviewed）

## 目标
WAL-FAILSTOP（5873b71）把 log_stream replay 遇损坏记录从静默丢数据改为响亮 fail。
但端到端是 **loud-but-stuck**：读用确定性副本选择、坏字节不是 stream 读错误，故
反复读同一坏副本 → 分区永久不可用，靠人工 fence。数据通常只一个盘 bit-rot，另两个副本好。
**目标：replay 发现坏副本时自动隔离 + failover 到健康副本（可用且正确）+ 后台修复坏副本。**

## coco 评审结论（已采纳）
草稿「找干净副本继续 open + best-effort 上报」**不安全**，3 个 P0：
1. 现有读路径不按 avali 过滤副本 → 清 avali 隔离不了坏副本，普通读仍命中。
2. VP 读不校验 WAL CRC → 自愈后用户 GET 大值仍可能从坏副本读到坏字节（比 fail-loud 更糟）。
3. 上报/forced-overwrite 无 fencing → stale PS 可能覆盖新 owner 正在写的 open extent。
**正确架构 = 两阶段：先同步隔离（成功才 serving），再异步修复（staging+校验+原子 rename）。**

## 不变量（load-bearing）
- **I1 隔离先于 serving**：replay 确认副本坏 → 必须先持久隔离（坏 slot 从读集移除）成功，
  partition 才 serving。隔离失败 → 维持 fail-loud，绝不在坏副本仍可读时开服。
- **I2 读路径按 avali 隔离（仅 sealed replicated）**：sealed replicated extent 的 failover
  读跳过 avali 位为 0 的 slot。**open extent 不过滤**（avali=0 是"未封"常态，非"坏"；
  open 一致性由 commit-min 协议管）。**EC extent 不过滤 addr 列表**（破坏 shard↔addr 索引
  对齐；缺 shard 由 EC 重建处理）。防御：过滤后 0 个可用 → 回退全读 + WARN（避免回归）。
- **I3 open tail 坏副本 = seal-and-roll**：open tail 没固定长度，直接 failover 读含糊
  （commit-min 随读集变）。改为 **F227 seal-over-reachable（排除坏副本）封当前 tail +
  roll 新 tail（健康节点）**。已 ACK 前缀在每个 committed 成员上 → min over 健康 ≥ ACK，
  不丢已确认数据；超出健康 committed 的字节是未 ACK 投机，封掉正确。新写落新 tail。
  已 sealed 的 extent → 不 seal，直接 isolate + 固定 sealed_length 修复。
- **I4 fencing**：隔离上报带 `owner_epoch + partition_id + extent_id + eversion + sealed`；
  manager CAS 校验当前 owner/eversion，不匹配丢弃（stale PS 不能改布局）。forced-repair
  RPC 带 fencing token，EN 存储层拒绝 stale。
- **I5 etcd-first**：清 avali/bump eversion/标 suspect 都 etcd-first（clone→persist→改内存），
  与 apply_recovery_done 一致（mark_extent_available 现为 memory-first，需改）。
- **I6 修复 crash-safe**：forced-repair 写 staging `.repair.dat` → 从已校验健康源拉 →
  WAL-CRC 扫描校验 → fsync + dir-fsync → 原子 rename 覆盖 → reopen → save_meta（清
  corrupt_meta quarantine）→ etcd-first 恢复 avali。**不 in-place truncate**。
- **I7 修复源排除**：排除 target node + 所有已报坏 node + avali=0 slot + eversion 不符 +
  open-tail。源 copy 按 WAL V1 CRC 校验，不只验长度。全副本无 clean → fail loud，不"最长 wins"。
- **I8 重读窗口对齐 carry**：跨副本重读从 `buf_base = cur_off - carry.len()` 起读
  `carry.len()+got`，换副本后重算 consumed/carry/record_extent_off（否则坏 carry 误判健康副本 +
  VP offset 错位）。
- **I9 保障边界**：强完整性只对 V1 WAL（有 CRC）。V0 legacy 无 CRC，bit-flip 若过长度边界
  decode_one 直接 Ok → 自愈报告标 `legacy_unchecked`，不宣称 2^-32。

## 增量交付
### 增量 A — 同步隔离（先做，拿 80% 价值：不再读坏数据 + 自动 failover）
- **A1 读路径按 avali 隔离**（I2）：read_replicated_with_failover 跳过 avali=0 slot（sealed
  replicated；open/EC 豁免；全 0 回退）。纯 helper `eligible_replica_slots(ex)` 单测。
  —— 独立正确（不读 recovering slot），是隔离的前提。**【本次起步】**
- **A2 per-replica committed 读**（stream client）：`read_committed_from_replica(eid, idx, off,
  len) -> (bytes, end, node_id)`，不 failover，返回服务 node_id；供 replay 逐副本 decode 校验。
- **A3 replay 跨副本 decode-check**（recover_partition）：decode-Err/carry-fail → 从 buf_base
  起逐其余副本重读 decode（I8）；第一个干净 → 用之 + 记坏 node_id 集；全坏 → fail loud。
- **A4 隔离动作**：open tail → seal-and-roll（I3）；sealed → isolate。
- **A5 manager MSG_REPORT_CORRUPT_REPLICA**（I4 fencing + I5 etcd-first）：CAS 校验 → 清坏
  slot avali + bump eversion（持久化后返回）→ 读路径据 avali 隔离 + eversion 淘汰 cache。
- **A6 EN 本地 quarantine**：manager 通知坏 node 本地标 corrupt_data，handle_read_bytes 拒读
  直到修复（与 META-FAILCLOSED corrupt_meta 同形态）。
- 隔离持久成功后 replay 用干净副本继续、partition serving。

### 增量 B — 异步强制修复（后做：把隔离的坏副本复活）
- forced-repair（I6 staging+rename+CRC 校验 + I7 源排除），manager dispatch（F207 ledger
  防并发），etcd-first 恢复 avali，清 EN quarantine + corrupt_meta。

## reproduce-first 测试
- A1：`eligible_replica_slots` 单测（sealed+一位清→排除该 slot；open→全；全清→回退全 + 标记）。
- A3：注入坏 buf + 好 buf，验证选干净副本；全坏→Err。
- A5：stale owner_epoch 上报→manager 拒绝；etcd-first persist 失败→不对外可见。
- 端到端（A 全）：3-node，写 log_stream 大值占 sealed extent，**直接翻盘** 某 EN .dat 一个
  CRC 字节 → PS open replay → 断言 open 成功(读健康副本) + 坏副本被隔离(avali=0, 读不命中) +
  数据零丢失 + 用户 GET 大值不返回坏字节。全 3 副本翻坏 → open fail loud。
- B：forced-repair 5 crash point（quarantine/truncate→staging/fetch 半途/fsync 后 meta 前/
  rename 后 meta 前）；多坏副本源排除；与 EC convert/delete/re_avali 并发走 op_lock+ledger。

## 备选与否决
- stream 层加 per-record CRC + 读自动 failover：侵入热路径、跨层、EC 不适用 → 否决，放 partition 层。
- 直接 re_avali 修复：本地长度够即判最新 → 对 bit-rot 无效 → 必须 forced-overwrite。
- 只 fail-loud 靠人工 fence：现状，用户要自愈 → 否决。
- 草稿的 best-effort 上报 + 不隔离就 serving：coco P0，会"可用但返回坏数据" → 否决，改两阶段。
