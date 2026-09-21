# Manual Stream Test

## 1) Smoke test (no external etcd dependency)

```bash
cd "$(git rev-parse --show-toplevel)"   # repo root
./scripts/manual_stream_test.sh smoke
```

This runs:
- create stream
- append
- commit length
- truncate
- punchhole

## 2) With embedded etcd

```bash
cd "$(git rev-parse --show-toplevel)"   # repo root
./scripts/manual_stream_test.sh etcd
```

Requires:
- `go` in PATH (used to start embedded etcd helper)

## 3) Run both

```bash
cd "$(git rev-parse --show-toplevel)"   # repo root
./scripts/manual_stream_test.sh all
```

## Review 回归测试

普通 manager 回归不需要 FUSE 系统库：

```sh
cargo test -p autumn-manager --test node_lifecycle --test system_chaos --test system_review_takeover_fence --test system_review_remove_retry -- --test-threads=1
cargo test -p autumn-partition-server --lib gc_streaming_tests
```

需要本机回环监听权限及 PATH 上的 `etcd`（或设置 `AUTUMN_TEST_ETCD_BIN`）。测试的临时数据由各用例创建和清理，源码和配置均在当前仓库。

2026-09-20 的定向验证覆盖：R3 全部 fence RPC 被拒后分区不发布地址、恢复后重新打开及旧 epoch append 被拒；R7 Remove 响应丢失后销毁旧 manager runtime，新 manager 从 etcd 重放后重试成功且 tombstone/UUID 拒绝不变；T1 完整及 split 范围、空 topology、GET 正常但 150 次 PUT 全失败；T2 真实 EN 拒绝 DELETE、metadata 已不存在时残留文件仍判失败，并分别检查 inflight 与 persisted retry 阻塞。R1 完整扫描守卫已补，双副本截短与硬重启矩阵仍在 feature list。

该命令运行非 ignored 的 checker 和定向测试，不等于运行完整 chaos。FUSE 专属测试需要系统 FUSE 开发库，并显式加 `--features fuse-tests`；CI 保留此 feature 的编译和运行覆盖。
