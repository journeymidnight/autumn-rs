# autumn-rs：UCP（UCX）transport 接入设计

Date: 2026-04-23
Feature ID: F100-UCX
Branch: main（repo extraction 已完成；本 feature 在 main 上推进）
Status: brainstorm-done, Q1/Q2/Q4 resolved（见 §12）；pending writing-plans

## 0. 目标与范围

把 autumn-rs 的关键热点流量从 TCP 切到 UCX / RDMA，上层代码通过一个 `AutumnTransport` trait 解耦：

- **path 1**：PartitionServer ↔ ExtentNode 的三副本 append 复制（`autumn-stream`）。
- **path 2**：Client ↔ PartitionServer 用户 RPC（`autumn-rpc`）。

**不在本版**：PS 内部 pipeline（path 3）、Manager RPC（path 4）、异构集群、per-peer 回退、tag/AM/单边 PUT 通道。

## 1. 架构决策（已定稿）

| 维度 | 决策 | 备注 |
|---|---|---|
| 抽象层 | 新建 `autumn-transport` crate，`AutumnTransport` + `AutumnConn` trait | compio 本身不改 |
| 覆盖路径 | path 1 + path 2 | path 3/4 仍 TCP |
| UCX API | **UCP Stream**（`ucp_stream_send_nb` / `ucp_stream_recv_nb`） | drop-in `AsyncRead + AsyncWrite`；UCX 1.16 支持 rndv |
| 线程模型 | **per-thread `ucp_worker_h`**（`UCS_THREAD_MODE_SINGLE`） | 贴 compio per-thread runtime；endpoint 池 thread-local |
| 探测粒度 | **进程启动一次性 probe** | 同质集群假设 |
| 编译开关 | `cargo feature = "ucx"`（默认 off） | 不启用时零 UCX 依赖 |
| 运行开关 | `AUTUMN_TRANSPORT=auto\|tcp\|ucx`（默认 `auto`） | `auto`=探测；`tcp`/`ucx` 强制 |
| Listener | 单 transport 监听（随探测结果） | UCX 时只开 `ucp_listener`（必须 bind 在 RoCE-attached netdev 的 IP，不能是 `127.0.0.1`），TCP 时只开 TCP listener |

**非目标**（有意识地没选）：

- 不 fork compio 去加 UCX driver（维护成本过高）。
- 不走 tag-matching / Active Messages 重写 RPC 层（违背"上层改个参数"的约束）。
- 不搞 per-peer UCX↔TCP 回退（决策简单、性能可预测优先）。

## 2. 架构概览

```
autumn-rs/crates/transport/               [新 crate, feature = "ucx"]
├── src/lib.rs
│   ├── trait AutumnTransport   — connect / bind
│   ├── trait AutumnListener    — accept
│   ├── trait AutumnConn        — 继承 compio::io::{AsyncRead, AsyncWriteExt}
│   ├── probe()                 — 启动期探测 + env 解析
│   └── fn init() -> &'static DynTransport
├── src/tcp.rs                  — TcpTransport / TcpConn（包 compio::net）
├── src/ucx/mod.rs              — UcxTransport（feature = "ucx"）
├── src/ucx/worker.rs           — per-thread UcpWorker + progress task
├── src/ucx/endpoint.rs         — UcxConn，包 ucp_stream_*_nb
├── src/ucx/listener.rs         — UcxListener，包 ucp_listener
└── src/ucx/ffi.rs              — ucx-sys / 手写 bindgen 绑定

使用方改造：
- autumn-rpc      (path 2)  rpc/src/{server,client,pool}.rs
- autumn-stream   (path 1)  stream/src/client.rs
```

**不改动**：

- compio crate 本体。
- `autumn-rpc` 的 10-byte frame 协议（仍是上层字节流协议）。
- Manager 注册协议 / etcd 服务发现数据模型（`host:port` 格式不变，UCX 只换 port 语义）。
- path 3（PS 内 P-log pipeline）、path 4（Manager RPC）。

## 3. Transport trait 契约

```rust
pub trait AutumnTransport: Send + Sync + 'static {
    async fn connect(&self, addr: SocketAddr) -> io::Result<Box<dyn AutumnConn>>;
    async fn bind(&self, addr: SocketAddr) -> io::Result<Box<dyn AutumnListener>>;
    fn kind(&self) -> TransportKind;  // Tcp | Ucx，给日志/bench 用
}

pub trait AutumnListener: Send + 'static {
    async fn accept(&mut self) -> io::Result<(Box<dyn AutumnConn>, SocketAddr)>;
    fn local_addr(&self) -> io::Result<SocketAddr>;
}

pub trait AutumnConn:
    compio::io::AsyncRead + compio::io::AsyncWriteExt + 'static
{
    fn peer_addr(&self) -> io::Result<SocketAddr>;
    fn into_split(self: Box<Self>)
        -> (Box<dyn AutumnReadHalf>, Box<dyn AutumnWriteHalf>);
}
```

- 使用 compio 自带 I/O trait（owned buffer / `BufResult`），避免再套一层包装。
- `into_split` 两端半独立，沿用现有 `compio::net::OwnedReadHalf / OwnedWriteHalf` 语义。
- `Box<dyn ...>` 的动态分发放在连接建立期；热路径 I/O 经 vtable 一次，相对一个 `read()` 系统调用成本可以忽略。想完全避开 vtable 可以后续引入泛型版本（`enum TransportImpl { Tcp(...), Ucx(...) }`），v1 先走 trait object 简化调用方。

## 4. 探测 & 选择流程

```rust
// 进程启动时，在 main 的 runtime 起来前调用一次
pub fn init() -> Arc<dyn AutumnTransport> { ... }

fn decide() -> Decision {
    match env::var("AUTUMN_TRANSPORT").as_deref() {
        Ok("tcp") => Decision::Tcp,
        Ok("ucx") => {
            probe_ucx().expect("AUTUMN_TRANSPORT=ucx but UCX unavailable")
        }
        Ok("auto") | Err(_) => probe_ucx().unwrap_or(Decision::Tcp),
        Ok(other) => panic!("invalid AUTUMN_TRANSPORT={other}"),
    }
}

#[cfg(feature = "ucx")]
fn probe_ucx() -> Option<Decision> {
    // 1. link-time 依赖 libucp（通过 ucx-sys），启动时符号自然解析；
    //    缺 libucp.so 时二进制在 main 之前就加载失败——这已经是 clear 的错误。
    //    本函数只负责"可以 init，且有 RDMA transport"这一层判断。
    // 2. ucp_config_read + ucp_init
    // 3. ucp_worker_create (probe worker)
    // 4. 查询 transport list，要求存在以下任一 RDMA transport：
    //    rc_mlx5 / rc_verbs / dc_mlx5 / ud_mlx5 / ud_verbs
    //    （纯 tcp / self / sm 不算可用）
    // 5. 尝试 bind 一个 ucp_listener 在 127.0.0.1:0（仅验证 listener API 可建；
    //    实际数据路径绑定见 §7.3，需要走 RoCE-attached netdev 的 IP）
    // 6. 全部成功 → Some(Decision::Ucx)；否则 drop + None
}

#[cfg(not(feature = "ucx"))]
fn probe_ucx() -> Option<Decision> { None }
```

**启动日志格式**（bench / 排障必须 grep 到）：

```
autumn-transport: init decision=UCX (reason=auto,probe_ok)
                  devices=[mlx5_0..mlx5_9] transport=rc_mlx5
autumn-transport: init decision=TCP (reason=auto,probe_fail:libucp.so missing)
autumn-transport: init decision=TCP (reason=forced_by_env)
autumn-transport: init decision=UCX (reason=forced_by_env,probe_ok)
```

**不可用时的行为**：

- `AUTUMN_TRANSPORT=ucx` 且探测失败 → **进程 panic**（显式配置不该静默降级）。
- `AUTUMN_TRANSPORT=auto` 且探测失败 → `warn!` + TCP 继续。
- feature `ucx` 未启用 → 不管 env 怎么写，探测直接返回 TCP（`ucx` 强制时 panic）。

## 5. UCX 运行时细节

### 5.1 Per-thread worker bootstrap

```rust
thread_local! {
    static UCX_THREAD: RefCell<Option<UcxThreadCtx>> = const { RefCell::new(None) };
}

struct UcxThreadCtx {
    worker: *mut ucp_worker,
    progress_eventfd: RawFd,                 // ucp_worker_get_efd
    ep_cache: HashMap<SocketAddr, *mut ucp_ep>,
    progress_task: compio::task::JoinHandle<()>,
}
```

- `ucp_context_h` **进程全局**（一个），由 `probe_ucx()` 创建后 leak 成 `'static`。
- `ucp_worker_h` **每个 compio 工作线程一个**，惰性初始化：第一次在该线程上 `connect`/`bind` 时创建 worker + spawn progress task。
- progress task 流程：
  ```rust
  loop {
      ucp_worker_progress(worker);              // drain ready events
      if ucp_worker_arm(worker) == OK {         // 没 event 了，arm 后等
          async_fd.readable().await?;          // compio::AsyncFd 包 eventfd
      }
      // 否则直接再 progress 一轮（busy，中间挂 yield_now）
  }
  ```
- `AsyncFd` 封装 eventfd，把 UCX progress 集成进 compio 的 io_uring poll 循环，不开第二个 epoll/poll。

### 5.2 Connect

```rust
async fn connect(addr: SocketAddr) -> io::Result<UcxConn> {
    let ctx = UCX_THREAD.with(bootstrap);
    if let Some(&ep) = ctx.ep_cache.get(&addr) {
        return Ok(UcxConn { ep, ... });        // 复用
    }
    let ep = ucp_ep_create_nbx(ctx.worker, &params {
        field_mask: UCP_EP_PARAM_FIELD_SOCK_ADDR | UCP_EP_PARAM_FIELD_FLAGS,
        flags: UCP_EP_PARAMS_FLAGS_CLIENT_SERVER,
        sockaddr: addr,
        err_handler: Some(on_ep_error),
    });
    await_ep_ready(ep).await?;
    ctx.ep_cache.insert(addr, ep);
    Ok(UcxConn { ep, ... })
}
```

### 5.3 Accept

- `ucp_listener_create` 在 bind 地址上监听，给一个 `conn_handler` 回调（UCX 在 progress 时触发）。
- 回调里拿到 `ucp_conn_request_h`，push 到一个 mpsc::UnboundedSender → `AutumnListener::accept` 的 `.await` 从 receiver 取 → `ucp_ep_create_nbx({conn_request})` 建立 ep → 返回 `(UcxConn, peer_addr)`。
- peer_addr 从 `ucp_conn_request_query` 的 `client_address` 取。

### 5.4 Read / Write

```rust
impl compio::io::AsyncRead for UcxConn {
    async fn read<B: IoBufMut>(&mut self, mut buf: B) -> BufResult<usize, B> {
        let ptr = buf.as_buf_mut_ptr();
        let cap = buf.buf_capacity();
        let (tx, rx) = oneshot::channel();
        let req = ucp_stream_recv_nb(self.ep, ptr, cap, cb_recv(tx), ...);
        match rx.await { Ok(len) => { buf.set_buf_init(len); BufResult(Ok(len), buf) } ... }
    }
}
```

- 同步完成（`UCS_OK`）时直接返回；`UCS_INPROGRESS` 时把 `tx` 挂进 request user data，完成回调唤醒。
- `write` 对称：`ucp_stream_send_nb(ep, ptr, len, cb_send(tx))`。
- compio 的 owned-buffer 约定天然匹配 UCX 要求 buffer 在 request 完成前活着的约束，不需要额外复制。

### 5.5 错误 & 重连

- EP 错误（`UCS_ERR_CONNECTION_RESET` 等）→ `ucp_ep_close_nb` + 从 `ep_cache` 删除 → 下次 `connect` 重建。
- Worker fatal（罕见）→ 日志 + 该线程上所有 pending I/O `Err` 返回，上层 RPC 客户端自重试（现有逻辑）。
- 与 TCP 保持同构错误码（`io::ErrorKind::ConnectionReset` / `BrokenPipe`），调用方不区分 transport。

## 6. 编译 & 依赖（`ucx-sys` 选型仍为 P0 调研项）

**新 crate**：`autumn-rs/crates/transport/Cargo.toml`

```toml
[package]
name = "autumn-transport"

[features]
default = []
ucx = ["dep:ucx-sys"]           # 或手写 bindgen 子 crate

[dependencies]
compio = { version = "0.18", features = ["net", "io", "macros"] }
bytes.workspace = true
tracing = "0.1"
thiserror.workspace = true

ucx-sys = { version = "0.2", optional = true }   # 调研：有没有维护良好的 crate
# 备选：自己在 crates/transport/ucx-sys-mini/ 里 bindgen 核心 40 个符号
```

**调研任务**（P0，在 P3 之前做）：

- `ucx-sys` crate 的成熟度（crates.io 上有几个同名实现）。
- 若不合适 → 用 `bindgen` 生成最小绑定（只需要 `ucp_*` 的 context/worker/ep/listener/stream/request 相关 40 个符号）。
- 运行时要求 `libucx-dev` 装在构建机；目标机只要 `libucx0`。本机已装 `libucx-dev=1.16.0+ds-5ubuntu1` + `ucx-utils`（用于 §12 Q1 探针），可直接进入 P3。

## 7. 改造点清单

### 7.1 autumn-rpc（path 2）

- `rpc/src/server.rs`：
  - accept 线程的 `std::net::TcpListener` → `AutumnTransport::bind` + `AutumnListener::accept`（同步包成 Dispatcher 可分发对象）。
  - UCX 模式下不再有 "std TcpStream → compio TcpStream" 转换；直接 `AutumnConn`。
  - Dispatcher 分发载荷从 `(std::net::TcpStream, SocketAddr)` 换成 `(Box<dyn AutumnConn>, SocketAddr)`。
- `rpc/src/client.rs`：
  - `TcpStream::connect` → `transport.connect(addr)`。
  - `OwnedReadHalf / OwnedWriteHalf` → `AutumnReadHalf / AutumnWriteHalf`。
  - reader task / writer task 逻辑不变（frame 解码沿用）。
- `rpc/src/pool.rs`：连接池 value 类型换 `Box<dyn AutumnConn>`；key 仍是 `SocketAddr`。

### 7.2 autumn-stream（path 1）

- `stream/src/client.rs` 的 StreamClient 连接管理：TcpStream → `AutumnConn`。
- `stream/src/conn_pool.rs`：同上。
- R4 系列的 per-stream SQ/CQ worker 线程继续存在，只是底层 conn 变了。

### 7.3 全局初始化

- `autumn-rs/crates/server/src/bin/partition_server.rs`、`manager.rs`、`extent_node.rs`：
  - 每个 binary 在创建 compio runtime 之后、起 service 之前调 `autumn_transport::init()`。
  - `partition_server.rs` 把 `AUTUMN_PS_LISTEN` 地址交给 `transport.bind`。
  - **UCX 模式下**：`AUTUMN_*_LISTEN` 必须是 RoCE-attached netdev 的 IP（IPv4 或 IPv6 GUA/ULA），不能是 `127.0.0.1`。读取 `AUTUMN_TRANSPORT=ucx` 时，启动期校验该 IP 在 `getifaddrs()` 中且对应 netdev 的 RoCE GID 表非空，否则 panic 并打印可选 IP 列表。
- `autumn-client`：同样 `init()`。

### 7.4 测试

- `rpc/tests/round_trip.rs`、`stream/tests/*`：改成受环境变量参数化，CI 里跑两遍（TCP + 如果 feature 开 UCX）。
- 新增 `transport/tests/loopback.rs`：同一套 ping-pong / 半关闭 / 并发 / 大 payload 测试套件在 Tcp 和 Ucx 两个实现下运行。

## 8. 测试策略

### 8.1 Unit

- `transport/tests/loopback.rs`：ping-pong、2MB payload、半关闭、1000 并发连接。两个实现都过。
- `transport/tests/probe.rs`：模拟各种 env + feature 组合，验证 `decide()` 输出。

### 8.2 Integration（已有套件 + UCX 参数化）

- `rpc/tests/round_trip.rs` + `manager/tests/integration.rs` + `stream/tests/*` 都加 `AUTUMN_TRANSPORT=ucx` 的 CI 路径。
- 接受 baseline：`cargo test`（默认 TCP）全绿 + `AUTUMN_TRANSPORT=ucx cargo test --features ucx` 全绿。

### 8.3 Perf

- `partition-server/benches/ps_bench.rs` 加 `--transport tcp|ucx` 开关。
- A/B 目标（机器：10×mlx5 HCA，本机 loopback 测）：
  - path 2（小 RPC，64–512 B 请求）：UCX 相对 TCP 预期 **RTT 降 30–50%**（eager_bcopy vs 内核协议栈）。
  - path 1（大 payload，64 KB–1 MB append）：UCX 相对 TCP 预期 **吞吐翻倍以上**（rndv_get 零拷贝 vs 多次 memcpy + 协议栈）。
  - 未达预期 → 检查 `UCX_PROTO_INFO=y` 日志，确认走了 rndv 而非 eager 分片。
- F099 系列的 `autumn-client perf-check` 在 UCX 下重跑一次，对比单分区 / 多分区的 QPS 上限。

### 8.4 验证工具链

- `ib_write_bw` / `ib_send_bw` 做裸 RDMA baseline，作为 UCX 路径的理论上限参照。
- `UCX_LOG_LEVEL=info` + `UCX_PROTO_INFO=y` 在 bench 时开，落盘一份，确认协议路径。

## 9. 分阶段推进（对应 git commit 节点）

对应项目 CLAUDE.md 的"每个 feature 必须按固定流程推进"，拆成 5 个阶段，每阶段一个 commit + `claude-progress.txt` 更新：

| 阶段 | 目标 | 验收 |
|---|---|---|
| **P1** | 建 `autumn-transport` crate，TCP impl，不改任何 call site | `cargo build -p autumn-transport` + loopback 单测通过 |
| **P2** | 把 path 1 + path 2 call sites 全部换成 trait（仍 TCP 实现） | 现有 `cargo test` 全绿，零行为变化 |
| **P3** | 加 `UcxTransport`（feature = `ucx`），loopback 单测过 | `cargo test -p autumn-transport --features ucx` 全绿 |
| **P4** | 接 probe + env，三值开关打通 | `AUTUMN_TRANSPORT=ucx cargo test --features ucx` 全绿 |
| **P5** | perf A/B + `autumn-rs/README.md` 手工验证步骤 | bench 数据落进 `perf_baseline_ucx.json` |

每阶段完成 → `claude-progress.txt` `TaskStatus: completed` → git commit → 下一阶段。中断/阻塞 → `not_completed`。

## 10. 风险与未决项

- ~~**`ucp_stream_*` 在 UCX 1.16 的 rndv 激活情况未实测**~~ → **已验证 2026-04-23（见 §12 Q1）**。本机 ucx_perftest stream_bw 256 KB × 200 iter 走 `rc_mlx5/mlx5_0:1/path0` 实测 13.4 GB/s；UCX 1.16 新 proto 框架对 ≥478 B 的 ucp_stream send 自动选 `multi-frag stream zero-copy copy-out`（即旧 `rndv_get_zcopy`）。
- **MR cache 命中率**：path 1 的 WAL append buffer 如果不是长期存活的 pool，每次要注册会吃到毫秒级 stall。需要审视 `autumn-stream` 的 buffer 生命周期；必要时在 P3 之前加一层 `BufferPool`。这一项在 P5 perf 验证时重点看。
- **compio I/O trait 和 UCX callback 的集成边界**：compio 的 owned buffer 契约要求 future drop 时 cancel 安全。UCX 的 `ucp_request_cancel` 语义要求 cancel 后仍要等完成回调才能释放请求，两边配合要仔细写（P3 内部问题，单测覆盖）。
- **`ucx-sys` crate 质量**：若不成熟，自己 bindgen，多 1–2 天工作量。P0 先调研。
- **cancel 时 pending `ucp_stream_send_nb` 的 buffer 归还**：需要确认 UCX 是否在 cancel 完成回调前"借用"着 buffer。这决定 compio-style owned-buffer 返回时机。
- **部署期 RoCE 配置前置条件**：UCX rdmacm wireup 要求 RoCE NIC 上有可路由的 IP（IPv4 或 IPv6 GUA/ULA）。本机 10×mlx5 已配 IPv6 ULA（`fdbb:dc62:3::/48`），生产部署需保留同等配置；纯无 IP 的 RDMA-only 机器需要走 OOB worker-address exchange，不在本版范围内。

## 11. 下一步

本文档通过后：

1. 用 superpowers:writing-plans skill 产出实现 plan（拆 P1–P5 的每一步 task）。
2. P0 调研 `ucx-sys` crate 选型。
3. ~~登记 feature~~ → 已登记为 `feature_list.md` 的 **F100-UCX**（P5 — Network transport abstraction）。

## 12. 已解决的 brainstorm 问题（2026-04-23）

### Q1 · `ucp_stream` rndv 在 UCX 1.16 + RoCEv2 的实际行为

**结论：rndv 自动激活，设计前提成立。**

实测命令（本机 dc62-p3-t302-n014, 10×mlx5_0..9 RoCEv2 over IPv6 ULA）：

```bash
# server
UCX_PROTO_ENABLE=y UCX_PROTO_INFO=y UCX_TLS=rc_mlx5,self,tcp \
  UCX_NET_DEVICES=mlx5_0:1,eth2 ucx_perftest -6 -p 13339

# client (data path forced through rc_mlx5; tcp 仅作 OOB wireup)
UCX_PROTO_ENABLE=y UCX_PROTO_INFO=y UCX_TLS=rc_mlx5,self,tcp \
  UCX_NET_DEVICES=mlx5_0:1,eth2 ucx_perftest -6 -p 13339 \
  "fdbb:dc62:3:3::16" -t stream_bw -s 262144 -n 200 -m host
```

UCX 选出的协议表（`UCX_PROTO_INFO=y` 输出）：

```
| 0..477   B | multi-frag stream copy-in copy-out   | rc_mlx5/mlx5_0:1/path0 |
| 478..inf B | multi-frag stream zero-copy copy-out | rc_mlx5/mlx5_0:1/path0 |
```

性能：256 KB × 200 iter, **13422 MB/s, 53.7k msg/s, 18.6 μs/op**。

含义：
- path 1 大 payload（log_stream append 通常 ≥4 KB，flush 一次 128 MB）落在 478 B 之上 → 全程 zero-copy（rndv get zcopy）。
- path 2 小 RPC（64–512 B 用户请求 + 10 B frame header）跨在阈值两侧；UCX 自动按 size 选 eager copy-in 或 zcopy，无需上层手动切换。
- 13.4 GB/s 是 200 Gb/s ConnectX RoCEv2 的合理水线（理论 25 GB/s，proto + 单 QP loopback overhead 后这个数字符合预期）。

### Q2 · `Box<dyn AutumnConn>` vs. enum dispatch

**结论：维持 §3 的 trait object 设计。**

理由（量化）：

| Path | per-op cost | vtable hop | 占比 |
|---|---|---|---|
| Path 2 RDMA 小 RPC | ~3 μs | ~1 ns | 0.03 % |
| Path 1 RDMA 1 MB append | ~50 μs | ~1 ns | 0.002 % |
| Path 2 kernel TCP 小 RPC | ~15 μs | ~1 ns | 0.007 % |

vtable 跳转在工作负载层面完全消失在噪声里。enum 唯一可能的胜场是 hot decode loop 的可内联性，但 RPC frame decode 是先读到 buffer 再原地解析，内层循环不再回到 conn → 这部分增益不存在。trait object 的优点：贴近 compio 自身 `AsyncRead/AsyncWriteExt` 的形状、加入第三种 transport（如 SHM）零调用方改动、接口清晰。如果未来 flame graph 出现 `vtable_dispatch` 热点（高度不确定），可以局部把最热的 1–2 个方法改成 enum 单态化，无需推翻整个 API。

### Q4 · Branch / 与 repo extraction 的时序

**结论：repo extraction 已完成，本 feature 直接在 `main` 上推进。**

原 `Branch: compio` 已在 header 修正为 `main`。F100-UCX 不依赖 extraction 之外的任何前置条件。
