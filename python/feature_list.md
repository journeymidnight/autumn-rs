
---

### F275 · chaos 迭代 16：kvcache python 接口 chaos（2026-06-12）
- **目标:** 三接口最后一块——python L3 backend（sglang HiCache 路径，
  无需 sglang）在 failover 下。新 harness `scripts/kvcache_chaos.sh` +
  `python/autumn_kvcache/tests/chaos_workload.py`：batch_set_v1 + 随机
  历史页读回校验持续流经 python 桥；K1 杀持有 PS、K2 杀 manager；进度
  门判活 + 结束后新进程全清单复验。
- **过程发现（非产品 bug，运维坑）:** 已安装的 maturin wheel 是 5/20
  旧构建——其后 PutReq 加了 fence 字段（rkyv 线协议变更，仓库约定
  same-commit 部署），旧编码被新 PS 解出 `part_id=0` → batch_set 全
  False、官方 smoke test 同样失败。重建 wheel 即愈。**注记：rust 侧
  wire 变更后必须 `maturin build --release` + 重装**（README 已加）。
- **验收: 重建后全 PASS** —— K1 后 12s、K2 后 8s 恢复进度；158 轮全
  部 readback 校验 + 新进程复验 158/158，零 mismatch。
- **passes:** completed (2026-06-12)
