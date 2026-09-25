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
cargo test -p autumn-manager --test apply_done_atomicity -- --ignored --test-threads=1
```

需要本机回环监听权限及 PATH 上的 `etcd`（或设置 `AUTUMN_TEST_ETCD_BIN`）。测试的临时数据由各用例创建和清理，源码和配置均在当前仓库。

2026-09-20 的定向验证覆盖：R3 全部 fence RPC 被拒后分区不发布地址、恢复后重新打开及旧 epoch append 被拒；R7 Remove 响应丢失后销毁旧 manager runtime，新 manager 从 etcd 重放后重试成功且 tombstone/UUID 拒绝不变；T1 完整及 split 范围、空 topology、GET 正常但 150 次 PUT 全失败；T2 真实 EN 拒绝 DELETE、metadata 已不存在时残留文件仍判失败，并分别检查 inflight 与 persisted retry 阻塞。R1 完整扫描守卫已补，双副本截短与硬重启矩阵仍在 feature list。

2026-09-21 的 Recovery/EC 提交回归覆盖：请求提交前及事务响应后创建字节相同的后继 marker，旧 apply 均不得改写内存或删除后继；真实 etcd 删除并重建相同 marker 后 revision CAS 拒绝旧 EC apply；Recovery marker 清理失败时保留内存状态，下一 dispatch tick 无需 leader 切换即可重试成功。

该命令运行非 ignored 的 checker 和定向测试，不等于运行完整 chaos。驱动挂载派发循环的三个测试（`fuse_lease_1`、`fuse_lease_2`、`system_fuse_release_best_effort`）需要系统 FUSE 开发库，并显式加 `--features fuse-tests`；其余文件系统测试只依赖 `autumn-fs`，不需要；CI 保留此 feature 的编译和运行覆盖。

Recovery attempt / Fence / Remove 的 wire-46 验证入口：

~~~sh
cargo test -p autumn-manager --test recovery_attempt --test system_extent_recovery --test node_lifecycle --test apply_done_atomicity -- --include-ignored --test-threads=1
~~~

包含真实 etcd 的 marker/snapshot 同事务、重放及相同 assignment 重派，以及目标 runtime 真正退出并重启后拒绝旧请求。manager 库内的 recovery_attempt 测试覆盖 Fence/Remove 两种提交顺序和取消失败 blocker。Linux 全目标/FUSE 检查仍需对应系统依赖。

macOS 本地编译需要 macFUSE 的库/头文件和 pkg-config 工具。安装 macFUSE 本身
不提供 pkg-config；缺少该命令时可安装 Homebrew 的 pkgconf。验证及编译入口：

~~~sh
pkg-config --modversion fuse
pkg-config --libs --cflags fuse
cargo check --workspace --lib --bins --tests --features autumn-manager/fuse-tests
cargo test -p autumn-fs -p autumn-fuse --lib
~~~

macFUSE 的 fuse.pc 默认位于 /usr/local/lib/pkgconfig，Homebrew pkgconf 的默认
搜索路径包含该目录。以上命令不运行真实内核挂载，也不包含 Linux 专用 benchmarks。
