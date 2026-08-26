# WAL replay 自愈环

## 目标

log_stream replay 遇到 mid-stream 损坏记录 **fail loud**（WAL-FAILSTOP 不变量，见
`crates/partition-server/CLAUDE.md`），不静默跳过丢数据。但只 fail loud 是
**loud-but-stuck**：读用确定性副本选择、坏字节不是 stream 层的读错误，于是反复读到同
一个坏副本 → 分区永久打不开。实际损坏通常只在一个盘上 bit-rot，另两个副本是好的。

自愈环把 loud-but-stuck 变成：**replay 发现坏副本 → 跨副本重读修好这段窗口 → 同步隔离
坏副本（成功才 serving）**。

## 链路

全部在 `recover_partition`（`crates/partition-server/src/lib.rs`）+
`StreamClient`（`crates/stream/src/client.rs`）+ manager
`handle_report_corrupt_replica`（`crates/manager/src/rpc_handlers.rs`）：

1. 每个 committed 窗口跑 `decode_records_chunk`。三个损坏信号任一命中即触发自愈：
   短读（serving 副本截断）、complete 记录 CRC/内长不符（`Err`）、final chunk 解完仍
   有残留（`consumed < buf.len()`）。
2. `self_heal_replay_chunk`：对 **sealed replicated** extent，从每个 eligible 副本用
   `StreamClient::read_committed_from_replica(eid, idx, off, len)`（不 failover、返回
   服务它的 node_id）重读**同一 committed 窗口**，纯函数 `select_clean_replica_chunk`
   挑第一个 decode 干净的，并收集坏 node_id。
3. OPEN tail 的**内容损坏** → `StreamClient::seal_and_roll_tail`（seal-over-reachable
   封当前 tail + roll 新 tail 到健康节点），同一 pass 重取已 sealed 的 ExtentInfo，再走
   第 2 步的 sealed 跨副本隔离（不依赖重开）。
4. EC extent / 全副本都坏 / **截断**（short）的 open tail / seal-and-roll 失败 →
   不自愈，fail loud。
5. chunk 循环结束、**partition serving 之前**：对每个坏 extent
   `invalidate_extent_cache` → 重取 eversion → `report_corrupt_replica`
   (`MSG_REPORT_CORRUPT_REPLICA`)。manager 清坏 slot 的 `avali` 位 + bump eversion
   （etcd-first）。被拒（stale owner/eversion、会隔离掉最后一个副本、extent 仍 open、
   已 EC）→ `Err` → partition open fail loud → region_sync 重试 → 重新检测重新上报。
6. 读路径 `eligible_replica_slots` 按 `avali` 过滤（`replicated_read_order` 消费它）。

## 不变量（load-bearing）

> 编号被代码注释直接引用（`I1`/`I2`/`I4`/`I5`/`I7`），改动请勿重编号。

- **I1 隔离先于 serving**：replay 确认副本坏 → 必须先持久隔离（坏 slot 从读集移除）
  成功，partition 才 serving。隔离失败 → 维持 fail-loud，绝不在坏副本仍可读时开服。
- **I2 读路径按 avali 隔离（仅 sealed replicated）**：sealed replicated extent 的
  failover 读跳过 `avali` 位为 0 的 slot。**open extent 不过滤**（avali=0 是"未封"
  常态，不是"坏"；open 一致性由 commit-min 协议管）。**EC extent 不过滤 addr 列表**
  （会破坏 shard↔addr 索引对齐；缺 shard 由 EC 重建处理）。防御：过滤后 0 个可用 →
  回退全读 + 调用方记日志（不回归可用性）。
- **I3 open tail 坏副本 = seal-and-roll**：open tail 没有固定长度，直接 failover 读
  含糊（commit-min 随读集变）。改为 seal-over-reachable 封当前 tail + roll 新 tail。
  已 ACK 前缀存在于每个 committed 成员上 → min over 健康副本 ≥ ACK，不丢已确认数据；
  超出健康 committed 的字节是未 ACK 的投机写，封掉是正确的。新写落新 tail。已经
  sealed 的 extent 不再 seal，直接隔离 + 按固定 `sealed_length` 处理。
- **I4 fencing**：上报带 `owner_epoch + partition_id + log_stream_id + extent_id +
  eversion`；manager 四层校验后才动布局 —— ① 上报者必须是该 partition 的当前
  owner_epoch；② 该 log_stream 必须属于这个 partition；③ 该 extent 必须是这个
  log_stream 的成员；④ eversion 必须与当前一致。任一不符 → `CODE_PRECONDITION`，
  stale PS 改不了布局，也不能跨 partition 隔离别人的副本。
- **I5 etcd-first**：清 avali / bump eversion 先 `persist_extent` 落 etcd，成功后才写
  内存，并在写内存前做 verify-at-apply（live eversion 仍等于上报快照才 insert，否则
  拒绝，不用陈旧 clone 覆盖并发 mutator）。
- **I7 干净源选择**：跨副本重读时，`avali=0` 的 slot 不作为干净源候选（已被隔离，
  即使这次 decode 干净也不可信）；**读错误 ≠ 损坏**（节点不可达/超时是 transient，
  不进坏副本集，绝不隔离不可达节点）；判定按 WAL V1 CRC decode，不只验长度；全部
  可达副本都不干净 → fail loud，绝不"最长 wins"。
- **I8 重读窗口对齐 carry**：跨副本重读从 `buf_base = cur_off - carry.len()` 起读
  `carry.len() + got` 字节，换副本后重算 consumed / carry / record_extent_off（否则
  坏 carry 会误判健康副本，且 VP offset 错位）。请求的是 **full 窗口**
  （`want_full`），不是可能被截短的 `buf.len()` —— 截断的 serving 副本不能缩小向干净
  副本索要的窗口。
- **I9 保障边界**：强完整性只对 V1 WAL（带 CRC）。V0 legacy 无 CRC，bit-flip 若不过
  长度边界 `decode_one` 会直接 `Ok`，自愈报告只能标 `legacy_unchecked`，不宣称
  2^-32。

## 边界（当前实现的已知缺口）

- **隔离不是永久的**：manager `recovery_dispatch_loop` 把 `avali & bit == 0` 当成
  "该副本需要修"，会向该 node 发 `EXT_MSG_RE_AVALI`；而 bit-rot 副本长度是完整的，
  EN 的 `handle_re_avali` 在 `local_len >= sealed_length` 时直接返回 `CODE_OK`，
  manager 随即 `mark_extent_available` 把 `avali` 位重新置上。所以对**内容损坏**
  （非长度缺失）的副本，隔离只在这次 open 的读窗口内成立，随后会被 recovery 环撤销。
  真正复活坏副本需要 forced-overwrite（staging + WAL-CRC 校验 + 原子 rename）——
  未实现；`re_avali` 按长度判"最新"，对 bit-rot 无效。
- **EN 侧没有本地 quarantine**：坏 node 不会本地标记数据损坏、也不会拒绝
  `handle_read_bytes`（与 META-FAILCLOSED 的 `corrupt_meta` 不同形态）。隔离完全靠
  manager 侧 `avali` + eversion 淘汰 client 缓存。
- **VP 读不校验 WAL CRC**：`resolve_value` 不做 per-record CRC，所以 I1 的
  "隔离先于 serving" 是用户 GET 大值不读到坏字节的唯一保障。
- **A5 的 etcd 写是 blind put**（`extents/<id>`，不是 value-CAS），与
  `apply_recovery_done` / `apply_ec_conversion_done` / split-seal 同一类残留
  （manager CLAUDE.md note 33，reproduce-first 挂起）；靠 verify-at-apply 收口。
- 初始 serving read 不返回 node_id：bit-rot 重读仍坏时可归因，超时不可归因
  （按 I7 不隔离不可达副本）。

## 否决的备选

- stream 层加 per-record CRC + 读自动 failover：侵入热路径、跨层、EC 不适用 →
  放在 partition 层。
- 直接 `re_avali` 修复：本地长度够就判"最新" → 对 bit-rot 无效。
- 只 fail-loud 靠人工 fence：可用性不可接受。
- best-effort 上报 + 不隔离就 serving：会变成"可用但返回坏数据"，比 fail-loud 更糟。
