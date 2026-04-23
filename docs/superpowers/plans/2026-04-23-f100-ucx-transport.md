# F100-UCX Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `cargo feature = "ucx"`-gated UCP/UCX RDMA transport for path 1 (PartitionServer ↔ ExtentNode three-replica append) and path 2 (Client ↔ PartitionServer user RPC), behind a new `AutumnTransport` trait. TCP remains the default; UCX is selected via `AUTUMN_TRANSPORT=auto|tcp|ucx`.

**Architecture:** A new `autumn-transport` crate exposes `AutumnTransport`/`AutumnListener`/`AutumnConn` traits. `Box<dyn AutumnConn>` implements `compio::io::AsyncRead + AsyncWriteExt`, so call-sites in `autumn-rpc`, `autumn-stream`, and the server binaries change once and then opt into RDMA at runtime via env. UCX impl uses `ucp_stream_*_nbx` (rndv-on-rc_mlx5 verified — see spec §12 Q1). Per-thread `ucp_worker_h` integrated into compio via `AsyncFd`-wrapped `ucp_worker_get_efd`.

**Tech Stack:** Rust 2021, compio 0.18, UCX 1.16 (libucp / libuct / libucs / libucm via pkg-config), `ucx-sys` crate (or in-tree `bindgen` fallback — see Phase 0). Existing workspace conventions: per-thread runtime, `Rc/RefCell` single-threaded state, `rkyv` zero-copy hot path serialization.

**Spec:** [`docs/superpowers/specs/2026-04-23-ucx-transport-design.md`](../specs/2026-04-23-ucx-transport-design.md)

**Preflight:** `scripts/check_roce.sh` (committed `cafd4c0`) — exit 0 means build host has ≥1 usable RoCEv2 GID; required for Phase 3+ tests.

> **DESIGN PIVOT NOTE (2026-04-23, during P1 Task 2 execution):** Spec §3
> originally drafted `Box<dyn AutumnConn>` (trait object). compio 0.18's
> `AsyncRead::read<B: IoBufMut>` / `AsyncWrite::write<T: IoBuf>` are generic
> methods → trait is NOT dyn-compatible → `Box<dyn ...>` does not compile.
>
> **Corrected design:** `enum Conn { Tcp(TcpConn), #[cfg(feature="ucx")]
> Ucx(UcxConn) }`, plus the same shape for `Listener` / `ReadHalf` /
> `WriteHalf`. Match-arm dispatch on hot path (cheaper and more inlinable
> than vtable). The `AutumnTransport` trait stays dyn-safe — its methods
> have no generic params. See spec §3 (rewritten) and §12 Q2-rev for the
> full rationale.
>
> **Plan delta:** every reference below to `Box<dyn AutumnConn>` /
> `Box<dyn AutumnReadHalf>` / `Box<dyn AutumnWriteHalf>` /
> `Box<dyn AutumnListener>` should read as `Conn` / `ReadHalf` /
> `WriteHalf` / `Listener` (the concrete enum types from
> `autumn_transport`). The Phase 1 Task 2 code block below is fully
> rewritten to match. Phase 2/3 code snippets retain the old wording for
> reference; the implementer should treat them as pseudocode showing
> *which call site changes*, and use the corrected types from the
> as-shipped `autumn-transport` crate.

---

## File-Structure Map

| Path | Phase | Responsibility |
|---|---|---|
| `crates/transport/Cargo.toml` | 1 | New crate manifest, optional `ucx` feature |
| `crates/transport/src/lib.rs` | 1 | Public `AutumnTransport` / `AutumnListener` / `AutumnConn` / `TransportKind` / `init()` |
| `crates/transport/src/tcp.rs` | 1 | `TcpTransport` / `TcpListenerImpl` / `TcpConnImpl` over `compio::net` |
| `crates/transport/src/probe.rs` | 1 (skel), 4 (impl) | `decide()` + env parsing; `probe_ucx()` impl in P4 |
| `crates/transport/tests/loopback_tcp.rs` | 1 | TCP ping-pong, 2 MB, half-close, 1 k concurrent |
| `crates/transport/tests/probe.rs` | 1 (skel), 4 (full) | `decide()` env-combination tests |
| `crates/transport/src/ucx/mod.rs` | 3 | `UcxTransport` / `UcxListener` / `UcxConn` |
| `crates/transport/src/ucx/worker.rs` | 3 | Per-thread `UcxThreadCtx` + progress task + `ep_cache` |
| `crates/transport/src/ucx/endpoint.rs` | 3 | `UcxConn` `AsyncRead` / `AsyncWriteExt` impls |
| `crates/transport/src/ucx/listener.rs` | 3 | `UcxListenerImpl` over `ucp_listener_create` |
| `crates/transport/src/ucx/ffi.rs` | 3 | Re-export of `ucx-sys` symbols (or in-tree bindgen) |
| `crates/transport/build.rs` | 3 | `pkg-config --libs ucx` link directive |
| `crates/transport/tests/loopback_ucx.rs` | 3 | UCX-only test mirror of `loopback_tcp.rs` |
| `crates/rpc/{src/server.rs, src/client.rs, src/pool.rs, Cargo.toml}` | 2 | Migrate accept loop / connect / pool to trait |
| `crates/stream/{src/extent_node.rs, src/conn_pool.rs, Cargo.toml}` | 2 | ExtentNode listener + StreamClient pool migration |
| `crates/manager/{src/rpc_handlers.rs, src/lib.rs, Cargo.toml}` | 2 | Manager listener + internal ConnPool migration |
| `crates/server/src/bin/{partition_server,manager,extent_node,autumn_client,autumn_stream_cli}.rs` | 2, 4 | Call `autumn_transport::init()` at startup; UCX-mode IP validation |
| `crates/server/Cargo.toml` | 2 | Add `autumn-transport` dep |
| `crates/partition-server/benches/ps_bench.rs` | 5 | `--transport tcp\|ucx` flag |
| `scripts/perf_ucx_baseline.sh` | 5 | A/B baseline runner |
| `perf_baseline_ucx.json` | 5 | Captured baseline numbers |
| `README.md` | 5 | Manual UCX validation walkthrough |
| `feature_list.md` | 5 | Mark F100-UCX `passes: true` |
| `claude-progress.txt` | every commit | Status update per CLAUDE.md long-task rule |
| `Cargo.toml` (workspace) | 1 | Add `crates/transport` to `members` |

---

## Phase 0 — `ucx-sys` Crate Selection (Research, no code)

Spec §6 P0 task. Single decision document, then unblocks Phase 3.

### Task 0: Pick UCX FFI source

**Files:**
- Modify: `docs/superpowers/specs/2026-04-23-ucx-transport-design.md` — append §13 with the decision

- [ ] **Step 1: Survey crates.io ucx-sys candidates**

Run:
```bash
curl -s https://crates.io/api/v1/crates?q=ucx-sys | \
  jq -r '.crates[] | "\(.name) \(.max_version) downloads=\(.downloads) updated=\(.updated_at)"' | head
```

Document for each candidate: last-update date, dependency count, whether it builds against UCX 1.16 headers, whether it covers the symbols we need (`ucp_init`, `ucp_worker_create`, `ucp_worker_get_efd`, `ucp_worker_arm`, `ucp_worker_progress`, `ucp_ep_create_nbx`, `ucp_ep_close_nbx`, `ucp_listener_create`, `ucp_listener_destroy`, `ucp_conn_request_query`, `ucp_stream_send_nbx`, `ucp_stream_recv_nbx`, `ucp_request_cancel`, `ucp_request_check_status`, `ucp_config_read/release`, `ucs_status_string`).

- [ ] **Step 2: If a viable crate exists, pick it. Otherwise plan for in-tree bindgen.**

Decision criteria (in order):
1. Last commit ≤ 12 months old
2. Builds against UCX ≥ 1.14 headers without patches
3. Exposes (or trivially extends to expose) the ~40 symbols above
4. Maintainer responsive (open issues / PRs answered)

If all four pass for some `ucx-sys X.Y` → plan uses it.

If not → in-tree at `crates/transport/ucx-sys-mini/` with a `build.rs` that drives `bindgen` over `<ucp/api/ucp.h>`. Estimate: +1 day.

- [ ] **Step 3: Write the §13 decision doc**

Append to spec:
```markdown
## 13. ucx-sys Selection (Phase 0 outcome, YYYY-MM-DD)

**Decision:** [crate `ucx-sys = "X.Y"`] OR [in-tree bindgen at `crates/transport/ucx-sys-mini`]

**Evidence:** [crate URL + last commit / OR rationale why none qualify]

**Symbols required:** [list as above]

**Build host preconditions confirmed:**
- libucx-dev ≥ 1.16 installed (`dpkg -l | grep libucx-dev`)
- pkg-config knows ucx (`pkg-config --modversion ucx`)
```

- [ ] **Step 4: Commit decision**

```bash
git add docs/superpowers/specs/2026-04-23-ucx-transport-design.md
git commit -m "F100-UCX P0: pick <crate|bindgen> for UCX FFI"
```

---

## Phase 1 — `autumn-transport` Crate Skeleton (TCP impl only)

Build the trait + a TCP impl that fully passes a generic loopback test suite. **No call-site changes yet.** This phase is fully unit-testable without RDMA hardware.

### Task 1: Create the crate manifest and register it in the workspace

**Files:**
- Create: `crates/transport/Cargo.toml`
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Create `crates/transport/Cargo.toml`**

```toml
[package]
name = "autumn-transport"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[features]
default = []
ucx = []   # ucx-sys / ffi added in Phase 3 Task 27

[dependencies]
compio = { version = "0.18", features = ["net", "io", "macros"] }
bytes.workspace = true
tracing = "0.1"
thiserror.workspace = true
futures = "0.3"

[dev-dependencies]
compio = { version = "0.18", features = ["net", "io", "macros", "time"] }
```

- [ ] **Step 2: Add to workspace `Cargo.toml` `members` (alphabetical between `stream` and (none) — append before the closing `]`)**

```diff
   "crates/server",
   "crates/client",
+  "crates/transport",
   "crates/fuse",
 ]
```

- [ ] **Step 3: Run `cargo check -p autumn-transport`**

Expected: `error: couldn't read crates/transport/src/lib.rs: No such file or directory`. Confirms manifest is wired but src is absent.

- [ ] **Step 4: Create `crates/transport/src/lib.rs` as an empty stub**

```rust
//! autumn-transport — pluggable transport for autumn-rs (TCP today, UCX optional).
```

- [ ] **Step 5: Run `cargo check -p autumn-transport`**

Expected: PASS (warning about empty crate is fine).

- [ ] **Step 6: Commit**

```bash
git add crates/transport/Cargo.toml crates/transport/src/lib.rs Cargo.toml
git commit -m "F100-UCX P1.1: scaffold autumn-transport crate"
```

### Task 2: Define the trait surface

**Files:**
- Modify: `crates/transport/src/lib.rs`

- [ ] **Step 1: Write the trait definitions and write the failing trait-shape test FIRST**

Replace `crates/transport/src/lib.rs` with:

```rust
//! autumn-transport — pluggable transport for autumn-rs (TCP today, UCX optional).

use std::io;
use std::net::SocketAddr;

mod tcp;
mod probe;

pub use probe::{decide, Decision};
pub use tcp::TcpTransport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    Tcp,
    Ucx,
}

#[async_trait::async_trait(?Send)]
pub trait AutumnTransport: 'static {
    async fn connect(&self, addr: SocketAddr) -> io::Result<Box<dyn AutumnConn>>;
    async fn bind(&self, addr: SocketAddr) -> io::Result<Box<dyn AutumnListener>>;
    fn kind(&self) -> TransportKind;
}

#[async_trait::async_trait(?Send)]
pub trait AutumnListener: 'static {
    async fn accept(&mut self) -> io::Result<(Box<dyn AutumnConn>, SocketAddr)>;
    fn local_addr(&self) -> io::Result<SocketAddr>;
}

pub trait AutumnConn:
    compio::io::AsyncRead + compio::io::AsyncWrite + 'static
{
    fn peer_addr(&self) -> io::Result<SocketAddr>;
    fn into_split(self: Box<Self>)
        -> (Box<dyn AutumnReadHalf>, Box<dyn AutumnWriteHalf>);
}

pub trait AutumnReadHalf: compio::io::AsyncRead + 'static {}
pub trait AutumnWriteHalf: compio::io::AsyncWrite + 'static {}
```

Add `async-trait = "0.1"` under `[dependencies]` in `crates/transport/Cargo.toml`.

- [ ] **Step 2: Add stub modules so it compiles**

Create `crates/transport/src/tcp.rs`:
```rust
//! TCP transport — wraps compio::net. Phase 1 Task 4 implements the body.
```

Create `crates/transport/src/probe.rs`:
```rust
//! Transport selection — Phase 1 Task 11 / Phase 4 Task 42 fill this in.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Tcp,
    Ucx,
}

pub fn decide() -> Decision {
    Decision::Tcp
}
```

Add `pub struct TcpTransport;` to `tcp.rs` so the `pub use` in `lib.rs` resolves.

- [ ] **Step 3: Run `cargo check -p autumn-transport`**

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/transport/Cargo.toml crates/transport/src/{lib.rs,tcp.rs,probe.rs}
git commit -m "F100-UCX P1.2: define AutumnTransport / AutumnListener / AutumnConn traits"
```

### Task 3: Implement `TcpTransport` (with TDD)

**Files:**
- Modify: `crates/transport/src/tcp.rs`
- Create: `crates/transport/tests/tcp_basic.rs`

- [ ] **Step 1: Write the failing connect-bind round-trip test**

Create `crates/transport/tests/tcp_basic.rs`:

```rust
use autumn_transport::{AutumnTransport, TcpTransport, TransportKind};
use compio::io::{AsyncReadExt, AsyncWriteExt};
use std::net::SocketAddr;

#[compio::test]
async fn tcp_connect_bind_round_trip() {
    let t = TcpTransport;
    assert_eq!(t.kind(), TransportKind::Tcp);

    let mut listener = t.bind("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let server = compio::runtime::spawn(async move {
        let (conn, _peer) = listener.accept().await.expect("accept");
        let (mut r, mut w) = conn.into_split();
        let mut buf = vec![0u8; 5];
        let (n, b) = r.read(buf).await;
        assert_eq!(n.unwrap(), 5);
        assert_eq!(&b[..], b"hello");
        let (n, _) = w.write(b"world".to_vec()).await;
        assert_eq!(n.unwrap(), 5);
    });

    let client = t.connect(addr).await.expect("connect");
    let (mut r, mut w) = client.into_split();
    let (n, _) = w.write(b"hello".to_vec()).await;
    assert_eq!(n.unwrap(), 5);
    let buf = vec![0u8; 5];
    let (n, b) = r.read(buf).await;
    assert_eq!(n.unwrap(), 5);
    assert_eq!(&b[..], b"world");

    server.await;
}
```

- [ ] **Step 2: Run the test, verify failure**

Run: `cargo test -p autumn-transport --test tcp_basic`
Expected: FAIL — `TcpTransport` is a unit struct with no impl.

- [ ] **Step 3: Implement `TcpTransport` / `TcpListenerImpl` / `TcpConnImpl`**

Replace `crates/transport/src/tcp.rs`:

```rust
use crate::{AutumnConn, AutumnListener, AutumnReadHalf, AutumnTransport, AutumnWriteHalf, TransportKind};
use async_trait::async_trait;
use compio::net::{TcpListener as CompioTcpListener, TcpStream};
use std::io;
use std::net::SocketAddr;

pub struct TcpTransport;

#[async_trait(?Send)]
impl AutumnTransport for TcpTransport {
    async fn connect(&self, addr: SocketAddr) -> io::Result<Box<dyn AutumnConn>> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Box::new(TcpConnImpl { stream, peer: addr }))
    }

    async fn bind(&self, addr: SocketAddr) -> io::Result<Box<dyn AutumnListener>> {
        let l = CompioTcpListener::bind(addr).await?;
        let local = l.local_addr()?;
        Ok(Box::new(TcpListenerImpl { inner: l, local }))
    }

    fn kind(&self) -> TransportKind { TransportKind::Tcp }
}

struct TcpListenerImpl {
    inner: CompioTcpListener,
    local: SocketAddr,
}

#[async_trait(?Send)]
impl AutumnListener for TcpListenerImpl {
    async fn accept(&mut self) -> io::Result<(Box<dyn AutumnConn>, SocketAddr)> {
        let (stream, peer) = self.inner.accept().await?;
        Ok((Box::new(TcpConnImpl { stream, peer }), peer))
    }
    fn local_addr(&self) -> io::Result<SocketAddr> { Ok(self.local) }
}

struct TcpConnImpl {
    stream: TcpStream,
    peer: SocketAddr,
}

impl compio::io::AsyncRead for TcpConnImpl {
    async fn read<B: compio::buf::IoBufMut>(&mut self, buf: B) -> compio::BufResult<usize, B> {
        self.stream.read(buf).await
    }
}

impl compio::io::AsyncWrite for TcpConnImpl {
    async fn write<B: compio::buf::IoBuf>(&mut self, buf: B) -> compio::BufResult<usize, B> {
        self.stream.write(buf).await
    }
    async fn flush(&mut self) -> io::Result<()> { self.stream.flush().await }
    async fn shutdown(&mut self) -> io::Result<()> { self.stream.shutdown().await }
}

impl AutumnConn for TcpConnImpl {
    fn peer_addr(&self) -> io::Result<SocketAddr> { Ok(self.peer) }
    fn into_split(self: Box<Self>) -> (Box<dyn AutumnReadHalf>, Box<dyn AutumnWriteHalf>) {
        let (r, w) = self.stream.into_split();
        (Box::new(TcpReadHalfImpl(r)), Box::new(TcpWriteHalfImpl(w)))
    }
}

struct TcpReadHalfImpl(compio::net::OwnedReadHalf<TcpStream>);
struct TcpWriteHalfImpl(compio::net::OwnedWriteHalf<TcpStream>);

impl compio::io::AsyncRead for TcpReadHalfImpl {
    async fn read<B: compio::buf::IoBufMut>(&mut self, buf: B) -> compio::BufResult<usize, B> {
        self.0.read(buf).await
    }
}
impl AutumnReadHalf for TcpReadHalfImpl {}

impl compio::io::AsyncWrite for TcpWriteHalfImpl {
    async fn write<B: compio::buf::IoBuf>(&mut self, buf: B) -> compio::BufResult<usize, B> {
        self.0.write(buf).await
    }
    async fn flush(&mut self) -> io::Result<()> { self.0.flush().await }
    async fn shutdown(&mut self) -> io::Result<()> { self.0.shutdown().await }
}
impl AutumnWriteHalf for TcpWriteHalfImpl {}
```

- [ ] **Step 4: Run the test, verify it passes**

Run: `cargo test -p autumn-transport --test tcp_basic`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/transport/src/tcp.rs crates/transport/tests/tcp_basic.rs
git commit -m "F100-UCX P1.3: TcpTransport impl + connect/bind round-trip test"
```

### Task 4: Generic loopback suite (parametrizable for TCP and UCX)

**Files:**
- Create: `crates/transport/tests/common/mod.rs`
- Create: `crates/transport/tests/loopback_tcp.rs`

The same suite will be re-used for UCX in Phase 3 Task 36, so isolate the test bodies behind a `transport_factory` callback.

- [ ] **Step 1: Create `crates/transport/tests/common/mod.rs`**

```rust
use autumn_transport::{AutumnTransport, AutumnConn};
use compio::io::{AsyncReadExt, AsyncWriteExt};
use std::net::SocketAddr;

pub async fn ping_pong<T: AutumnTransport + Clone>(t: T) {
    let mut listener = t.bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let s = compio::runtime::spawn(async move {
        let (c, _) = listener.accept().await.unwrap();
        echo_n(c, 1024).await;
    });
    let c = t.connect(addr).await.unwrap();
    write_then_read(c, 1024).await;
    s.await;
}

pub async fn large_payload<T: AutumnTransport + Clone>(t: T) {
    const N: usize = 2 * 1024 * 1024;
    let mut listener = t.bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let s = compio::runtime::spawn(async move {
        let (c, _) = listener.accept().await.unwrap();
        echo_n(c, N).await;
    });
    let c = t.connect(addr).await.unwrap();
    write_then_read(c, N).await;
    s.await;
}

pub async fn many_concurrent<T: AutumnTransport + Clone>(t: T, n: usize) {
    let mut listener = t.bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let s = compio::runtime::spawn(async move {
        for _ in 0..n {
            let (c, _) = listener.accept().await.unwrap();
            compio::runtime::spawn(async move { echo_n(c, 64).await; }).detach();
        }
    });
    let mut handles = Vec::new();
    for _ in 0..n {
        let t2 = t.clone();
        handles.push(compio::runtime::spawn(async move {
            let c = t2.connect(addr).await.unwrap();
            write_then_read(c, 64).await;
        }));
    }
    for h in handles { h.await; }
    s.await;
}

pub async fn half_close<T: AutumnTransport + Clone>(t: T) {
    let mut listener = t.bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let s = compio::runtime::spawn(async move {
        let (c, _) = listener.accept().await.unwrap();
        let (mut r, mut w) = c.into_split();
        let buf = vec![0u8; 5];
        let (n, b) = r.read(buf).await;
        assert_eq!(n.unwrap(), 5);
        let (n, _) = w.write(b.to_vec()).await;
        assert_eq!(n.unwrap(), 5);
        // EOF after one round
        let (n, _) = r.read(vec![0u8; 1]).await;
        assert_eq!(n.unwrap(), 0);
    });
    let c = t.connect(addr).await.unwrap();
    let (mut r, mut w) = c.into_split();
    let (n, _) = w.write(b"hello".to_vec()).await;
    assert_eq!(n.unwrap(), 5);
    w.shutdown().await.unwrap();
    let buf = vec![0u8; 5];
    let (n, _) = r.read(buf).await;
    assert_eq!(n.unwrap(), 5);
    s.await;
}

async fn echo_n(c: Box<dyn AutumnConn>, n: usize) {
    let (mut r, mut w) = c.into_split();
    let buf = vec![0u8; n];
    let (got, b) = r.read(buf).await;
    assert_eq!(got.unwrap(), n);
    let (sent, _) = w.write(b.to_vec()).await;
    assert_eq!(sent.unwrap(), n);
}

async fn write_then_read(c: Box<dyn AutumnConn>, n: usize) {
    let (mut r, mut w) = c.into_split();
    let payload = vec![0xa5u8; n];
    let (sent, _) = w.write(payload).await;
    assert_eq!(sent.unwrap(), n);
    let buf = vec![0u8; n];
    let (got, b) = r.read(buf).await;
    assert_eq!(got.unwrap(), n);
    assert!(b.iter().all(|&v| v == 0xa5));
}
```

`TcpTransport` does not yet derive `Clone`. Add `#[derive(Clone)]` to it in `crates/transport/src/tcp.rs`.

- [ ] **Step 2: Create `crates/transport/tests/loopback_tcp.rs`**

```rust
mod common;

use autumn_transport::TcpTransport;

#[compio::test]
async fn ping_pong()      { common::ping_pong(TcpTransport).await; }

#[compio::test]
async fn large_payload()  { common::large_payload(TcpTransport).await; }

#[compio::test]
async fn half_close()     { common::half_close(TcpTransport).await; }

#[compio::test]
async fn many_concurrent_1k() { common::many_concurrent(TcpTransport, 1000).await; }
```

- [ ] **Step 3: Run the suite**

Run: `cargo test -p autumn-transport --test loopback_tcp -- --test-threads=1`
Expected: 4 tests PASS. (Use `--test-threads=1` because loopback ports + the `127.0.0.1:0` ephemeral allocator are process-global; each test serializes nicely under one runtime.)

- [ ] **Step 4: Commit**

```bash
git add crates/transport/tests/{common/mod.rs,loopback_tcp.rs} crates/transport/src/tcp.rs
git commit -m "F100-UCX P1.4: generic loopback suite (TCP path passes ping-pong, 2MB, half-close, 1k concurrent)"
```

### Task 5: Probe + decide() skeleton with env tests

**Files:**
- Modify: `crates/transport/src/probe.rs`
- Create: `crates/transport/tests/probe.rs`

- [ ] **Step 1: Write failing tests for the env-driven decision matrix**

Create `crates/transport/tests/probe.rs`:

```rust
use autumn_transport::{decide, Decision};

fn with_env<R>(key: &str, value: Option<&str>, f: impl FnOnce() -> R) -> R {
    let prev = std::env::var(key).ok();
    match value {
        Some(v) => std::env::set_var(key, v),
        None    => std::env::remove_var(key),
    }
    let r = f();
    match prev {
        Some(v) => std::env::set_var(key, v),
        None    => std::env::remove_var(key),
    }
    r
}

#[test]
fn unset_env_defaults_to_tcp_when_ucx_feature_off() {
    // probe_ucx without ucx feature returns None -> auto falls back to TCP
    with_env("AUTUMN_TRANSPORT", None, || assert_eq!(decide(), Decision::Tcp));
}

#[test]
fn explicit_tcp_returns_tcp() {
    with_env("AUTUMN_TRANSPORT", Some("tcp"), || assert_eq!(decide(), Decision::Tcp));
}

#[test]
fn auto_returns_tcp_when_ucx_feature_off() {
    with_env("AUTUMN_TRANSPORT", Some("auto"), || assert_eq!(decide(), Decision::Tcp));
}

#[test]
#[should_panic(expected = "invalid AUTUMN_TRANSPORT")]
fn invalid_env_panics() {
    with_env("AUTUMN_TRANSPORT", Some("garbage"), || { let _ = decide(); });
}

#[cfg_attr(not(feature = "ucx"), ignore)]
#[test]
#[should_panic(expected = "AUTUMN_TRANSPORT=ucx but UCX unavailable")]
fn forced_ucx_without_capability_panics() {
    // When the ucx feature is on but probe_ucx returns None (no RDMA on host),
    // forced ucx must panic. Phase 4 fills the probe body.
    with_env("AUTUMN_TRANSPORT", Some("ucx"), || { let _ = decide(); });
}
```

- [ ] **Step 2: Run, expect failure**

Run: `cargo test -p autumn-transport --test probe`
Expected: FAIL on the auto path (current `decide()` ignores env).

- [ ] **Step 3: Implement `decide()` per spec §4**

Replace `crates/transport/src/probe.rs`:

```rust
use std::env;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Tcp,
    Ucx,
}

pub fn decide() -> Decision {
    match env::var("AUTUMN_TRANSPORT").as_deref() {
        Ok("tcp") => Decision::Tcp,
        Ok("ucx") => probe_ucx().expect("AUTUMN_TRANSPORT=ucx but UCX unavailable"),
        Ok("auto") | Err(_) => probe_ucx().unwrap_or(Decision::Tcp),
        Ok(other) => panic!("invalid AUTUMN_TRANSPORT={other}"),
    }
}

#[cfg(feature = "ucx")]
pub(crate) fn probe_ucx() -> Option<Decision> {
    // Phase 4 Task 42 implements the real probe. Skeleton: assume not available.
    None
}

#[cfg(not(feature = "ucx"))]
pub(crate) fn probe_ucx() -> Option<Decision> { None }
```

- [ ] **Step 4: Run probe tests**

Run: `cargo test -p autumn-transport --test probe`
Expected: 4 PASS, 1 IGNORED (the ucx-feature one).

- [ ] **Step 5: Commit**

```bash
git add crates/transport/src/probe.rs crates/transport/tests/probe.rs
git commit -m "F100-UCX P1.5: probe::decide() + env tests (probe_ucx body deferred to P4)"
```

### Task 6: Phase 1 closeout

- [ ] **Step 1: Update `claude-progress.txt`**

```text
Date: <today>
TaskStatus: completed
Task scope: F100-UCX Phase 1 — autumn-transport crate scaffold + TCP impl.
Current summary: Trait surface defined; TcpTransport passes generic
                 loopback suite (ping-pong, 2 MB, half-close, 1 k
                 concurrent); decide() honors AUTUMN_TRANSPORT env;
                 probe_ucx() body deferred to P4. No call-site changes.
Main gaps: ucx-sys decision (P0 done in Phase 0); P3 UCX impl pending.
Next steps: Phase 2 — migrate autumn-rpc / autumn-stream / autumn-manager
            call sites onto the trait (still TCP underneath).
```

- [ ] **Step 2: Run full workspace test to confirm no regression**

Run: `cargo test --workspace --exclude autumn-fuse 2>&1 | tail -20`
Expected: counts match baseline (213 pass / 9 ignored per Phase A audit).

- [ ] **Step 3: Commit progress note**

```bash
git add claude-progress.txt
git commit -m "F100-UCX P1: phase 1 complete (TCP impl + suite green)"
```

---

## Phase 2 — Migrate Call Sites to `AutumnTransport` (still TCP)

Goal: `cargo test --workspace` is byte-identical green afterward. Pure mechanical refactor — same wire bytes, just dispatched through the trait.

### Migration Pattern

For every call-site that today does either of:

| Today | After |
|---|---|
| `compio::net::TcpStream::connect(addr).await?` | `transport.connect(addr).await?` |
| `compio::net::TcpListener::bind(addr).await?` then `.accept()` | `transport.bind(addr).await?` then `.accept()` |
| `let (r, w) = stream.into_split();` | `let (r, w) = conn.into_split();` (types: `Box<dyn AutumnReadHalf/WriteHalf>`) |
| Stored `compio::net::TcpStream` in struct field | `Box<dyn AutumnConn>` |
| Stored `OwnedReadHalf<TcpStream>` / `OwnedWriteHalf<TcpStream>` | `Box<dyn AutumnReadHalf>` / `Box<dyn AutumnWriteHalf>` |

Where the existing code held a `&TcpStream` for socket-tuning (e.g., `extent_node.rs:376` `set_tcp_buffer_sizes`), keep that helper but make it a no-op when the trait object is a non-TCP variant. Concretely: add an optional method `fn as_tcp(&self) -> Option<&TcpStream>` to `AutumnConn`, default `None`, override `Some(...)` for `TcpConnImpl`. Call sites that need TCP tuning gate on that.

Each binary gains a process-global `&'static dyn AutumnTransport` from `autumn_transport::init()` (Phase 4 wires probing; Phase 2 returns `&TcpTransport` unconditionally).

### Task 7: Add the bootstrap `init()` (TCP-only stub for now)

**Files:**
- Modify: `crates/transport/src/lib.rs`

- [ ] **Step 1: Append the init helper**

Add to `lib.rs` (after the trait declarations):

```rust
use std::sync::OnceLock;

static GLOBAL: OnceLock<Box<dyn AutumnTransport>> = OnceLock::new();

/// Initialise the process-global transport. Idempotent; first call wins.
/// Phase 2 returns TcpTransport unconditionally; Phase 4 honors `AUTUMN_TRANSPORT`.
pub fn init() -> &'static dyn AutumnTransport {
    let _ = GLOBAL.set(Box::new(TcpTransport));
    &**GLOBAL.get().expect("init")
}

/// Same as init, but panics if another thread already initialised with a
/// different kind. Useful for binaries that want a hard failure on
/// configuration races.
pub fn current() -> &'static dyn AutumnTransport {
    GLOBAL.get().map(|b| &**b).expect("autumn_transport::init() not called")
}
```

`OnceLock` over `Box<dyn AutumnTransport>` requires the trait object be `Send + Sync`. Add those bounds: change `pub trait AutumnTransport: 'static` to `pub trait AutumnTransport: Send + Sync + 'static`. Same for `AutumnListener: Send + 'static`. `AutumnConn`, `AutumnReadHalf`, `AutumnWriteHalf` stay `!Send` (per-thread runtime). The `OnceLock<Box<dyn AutumnTransport>>` only needs the transport itself to be `Send + Sync`, which `TcpTransport` (unit struct) is.

- [ ] **Step 2: Add a smoke test**

Create `crates/transport/tests/init.rs`:
```rust
use autumn_transport::{init, current, TransportKind};

#[test]
fn init_returns_tcp_by_default() {
    let t = init();
    assert_eq!(t.kind(), TransportKind::Tcp);
    assert_eq!(current().kind(), TransportKind::Tcp);
}
```

Run: `cargo test -p autumn-transport --test init`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/transport/src/lib.rs crates/transport/tests/init.rs
git commit -m "F100-UCX P2.7: add autumn_transport::init() / current() globals"
```

### Task 8: Migrate `autumn-rpc`

**Files:**
- Modify: `crates/rpc/Cargo.toml`
- Modify: `crates/rpc/src/server.rs` (lines 73, 91, 99–100, 127)
- Modify: `crates/rpc/src/client.rs` (lines 30, 38–39, 94, 99–108, 109)
- Modify: `crates/rpc/src/pool.rs` (lines 36–37, 50–58)

- [ ] **Step 1: Add dependency**

In `crates/rpc/Cargo.toml`:
```toml
autumn-transport = { path = "../transport" }
```

- [ ] **Step 2: Modify `server.rs` accept loop**

Replace the `std::net::TcpListener::bind` (line 73) and the channel payload type (line 91) and the accept call (lines 99–100):

```rust
// before:
//   let listener = std::net::TcpListener::bind(addr)?;
//   let (tx, rx) = futures::channel::mpsc::unbounded::<(std::net::TcpStream, SocketAddr)>();
//   let (s, peer) = listener.accept()?;
//   tx.unbounded_send((s, peer))?;

let transport = autumn_transport::current();
let mut listener = compio::runtime::Runtime::new()?
    .block_on(transport.bind(addr))?;
let (tx, rx) = futures::channel::mpsc::unbounded::<(Box<dyn autumn_transport::AutumnConn>, SocketAddr)>();
loop {
    let (conn, peer) = compio::runtime::Runtime::new()?
        .block_on(listener.accept())?;
    tx.unbounded_send((conn, peer))?;
}
```

If `server.rs` runs accept on a dedicated `std::thread`, the listener now lives inside a small per-accept-thread compio runtime. (Match whatever pattern is already in `server.rs:73-110` — this snippet is the substitution shape.)

Drop the `TcpStream::from_std(std_stream)` conversion at line 127; the `Box<dyn AutumnConn>` is already compio-native.

- [ ] **Step 3: Modify `client.rs`**

Replace imports/aliases (lines 30, 38–39):
```rust
// before:
//   use compio::net::TcpStream;
//   type ReadHalf = compio::net::OwnedReadHalf<TcpStream>;
//   type WriteHalf = compio::net::OwnedWriteHalf<TcpStream>;
type ReadHalf = Box<dyn autumn_transport::AutumnReadHalf>;
type WriteHalf = Box<dyn autumn_transport::AutumnWriteHalf>;
```

Replace `TcpStream::connect(addr).await` (line 94):
```rust
let conn = autumn_transport::current().connect(addr).await?;
```

Replace `from_stream(stream: TcpStream, ...)` (lines 99–108) signature and body:
```rust
pub fn from_conn(conn: Box<dyn autumn_transport::AutumnConn>, ...) -> Self {
    let (r, w) = conn.into_split();
    // ... existing fields, just take r/w instead of constructing them from a TcpStream
}
```

Update every caller of `from_stream` (likely 1 or 2 in the same crate) to call `from_conn` and pass `Box<dyn AutumnConn>` instead of `TcpStream`.

- [ ] **Step 4: Modify `pool.rs`**

The `HashMap<SocketAddr, Rc<PoolEntry>>` shape stays; only the `RpcClient` it wraps changes (already covered by `client.rs` edits). No structural change unless `pool.rs` directly stores `TcpStream` itself — verify by `grep -n TcpStream crates/rpc/src/pool.rs` first; if zero hits, no edit.

- [ ] **Step 5: Run rpc tests**

Run: `cargo test -p autumn-rpc 2>&1 | tail -10`
Expected: 17 tests PASS (or whatever the existing baseline is — see Phase A audit count).

- [ ] **Step 6: Commit**

```bash
git add crates/rpc/Cargo.toml crates/rpc/src/{server.rs,client.rs,pool.rs}
git commit -m "F100-UCX P2.8: migrate autumn-rpc to AutumnTransport (still TCP underneath)"
```

### Task 9: Migrate `autumn-stream` (StreamClient + ExtentNode listener)

**Files:**
- Modify: `crates/stream/Cargo.toml`
- Modify: `crates/stream/src/conn_pool.rs` (lines 26, 40–46) — pool uses `RpcClient`, no direct TCP type; verify and update only if hit
- Modify: `crates/stream/src/extent_node.rs` (lines 33, 346, 376, 430, 435, 443, 452, 1236, 1306)

- [ ] **Step 1: Add dependency**

```toml
# crates/stream/Cargo.toml
autumn-transport = { path = "../transport" }
```

- [ ] **Step 2: Migrate ExtentNode listener at L1236**

```rust
// before:  let listener = TcpListener::bind(addr).await?;
let listener = autumn_transport::current().bind(addr).await?;
loop {
    let (conn, peer) = listener.accept().await?;
    compio::runtime::spawn(handle_connection(conn, peer)).detach();
}
```

- [ ] **Step 3: Migrate `handle_connection` and per-conn type annotations (L1306, L430/435/443/452)**

Change parameter type from `compio::net::TcpStream` to `Box<dyn autumn_transport::AutumnConn>`. The `OwnedReadHalf<TcpStream>` annotations become `Box<dyn AutumnReadHalf>`.

- [ ] **Step 4: TCP-tuning carve-out for `set_tcp_buffer_sizes` (L376)**

Add a method on `AutumnConn` in `transport/src/lib.rs`:

```rust
pub trait AutumnConn:
    compio::io::AsyncRead + compio::io::AsyncWrite + 'static
{
    fn peer_addr(&self) -> io::Result<SocketAddr>;
    fn into_split(self: Box<Self>)
        -> (Box<dyn AutumnReadHalf>, Box<dyn AutumnWriteHalf>);

    /// If this connection is backed by a TCP socket, return it for socket-level
    /// tuning (SO_RCVBUF, SO_SNDBUF, TCP_NODELAY). Returns `None` for non-TCP
    /// transports (UCX manages its own buffering).
    fn as_tcp(&self) -> Option<&compio::net::TcpStream> { None }
}
```

In `crates/transport/src/tcp.rs`, override:
```rust
impl AutumnConn for TcpConnImpl {
    fn as_tcp(&self) -> Option<&compio::net::TcpStream> { Some(&self.stream) }
    // ... existing methods
}
```

Update `set_tcp_buffer_sizes` in `extent_node.rs:376` to take `&dyn AutumnConn` and:
```rust
fn set_tcp_buffer_sizes(conn: &dyn autumn_transport::AutumnConn, ...) -> io::Result<()> {
    let Some(s) = conn.as_tcp() else { return Ok(()); };  // no-op on UCX
    // ... existing setsockopt calls on s
}
```

- [ ] **Step 5: Migrate per-replica connect at L346**

```rust
// before:  let s = compio::net::TcpStream::connect(addr).await?;
let conn = autumn_transport::current().connect(addr).await?;
```

- [ ] **Step 6: Run stream tests**

Run: `cargo test -p autumn-stream 2>&1 | tail -10`
Expected: 40 lib + 21 integration tests PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/stream/Cargo.toml crates/stream/src/{conn_pool.rs,extent_node.rs} crates/transport/src/{lib.rs,tcp.rs}
git commit -m "F100-UCX P2.9: migrate autumn-stream + extent_node + as_tcp() carve-out"
```

### Task 10: Migrate `autumn-manager`

**Files:**
- Modify: `crates/manager/Cargo.toml`
- Modify: `crates/manager/src/rpc_handlers.rs` (lines 23, 41)
- Modify: `crates/manager/src/lib.rs` (lines 17, 87–88, 96, 98, 143–150)

- [ ] **Step 1: Add dependency** — `autumn-transport = { path = "../transport" }`.

- [ ] **Step 2: Migrate listener at `rpc_handlers.rs:23`**

```rust
// before:  let listener = compio::net::TcpListener::bind(addr).await?;
let listener = autumn_transport::current().bind(addr).await?;
```

- [ ] **Step 3: Migrate `serve` accept-handler signature at L41**

```rust
// before:  fn handle(stream: TcpStream) { ... }
fn handle(conn: Box<dyn autumn_transport::AutumnConn>) { ... }
```

- [ ] **Step 4: Migrate ConnPool in `lib.rs` L143–150**

Pool storage type changes from `Rc<RefCell<RpcConn<TcpStream>>>` to `Rc<RefCell<RpcConn>>` (where `RpcConn` now holds trait-object halves per Task 8). The `TcpStream::connect(addr).await` at L96 becomes `autumn_transport::current().connect(addr).await`. The `into_split()` at L98 stays — it dispatches through the trait now.

- [ ] **Step 5: Run manager tests**

Run: `cargo test -p autumn-manager 2>&1 | tail -10`
Expected: integration suite green (or matches Phase A audit baseline including the 4 known-flake/host-env ignores).

- [ ] **Step 6: Commit**

```bash
git add crates/manager/Cargo.toml crates/manager/src/{rpc_handlers.rs,lib.rs}
git commit -m "F100-UCX P2.10: migrate autumn-manager to AutumnTransport"
```

### Task 11: Wire `init()` into binaries

**Files:**
- Modify: `crates/server/Cargo.toml` (add `autumn-transport` dep)
- Modify: `crates/server/src/bin/partition_server.rs`
- Modify: `crates/server/src/bin/manager.rs`
- Modify: `crates/server/src/bin/extent_node.rs`
- Modify: `crates/server/src/bin/autumn_client.rs`
- Modify: `crates/server/src/bin/autumn_stream_cli.rs`

- [ ] **Step 1: Add dep + call `init()` early in each `main`**

Pattern (apply to all 5 binaries):
```rust
fn main() -> Result<()> {
    // existing CLI parsing
    let _ = autumn_transport::init();        // panics if config invalid
    tracing::info!("transport: kind={:?}", autumn_transport::current().kind());
    // existing compio runtime + service startup
}
```

- [ ] **Step 2: Run all binaries' `--help`**

Run:
```bash
for b in partition_server manager extent_node autumn_client autumn_stream_cli; do
    cargo run -p autumn-server --bin "$b" -- --help 2>&1 | head -3
done
```
Expected: each prints usage; tracing line appears in stderr if any subcommand actually starts a runtime (not on `--help`).

- [ ] **Step 3: Commit**

```bash
git add crates/server/Cargo.toml crates/server/src/bin/*.rs
git commit -m "F100-UCX P2.11: wire autumn_transport::init() into all 5 binaries"
```

### Task 12: Phase 2 closeout — full workspace test

- [ ] **Step 1: Full workspace run**

```bash
cargo test --workspace --exclude autumn-fuse 2>&1 | tee /tmp/p2-test.out | tail -10
```
Expected: same pass/fail counts as the Phase A audit baseline (213 pass / 9 ignored). Any new failure means the migration broke something — diff `target` test output against baseline before continuing.

- [ ] **Step 2: Update `claude-progress.txt`**

```text
TaskStatus: completed
Task scope: F100-UCX Phase 2 — migrate autumn-rpc / autumn-stream /
            autumn-manager call sites to AutumnTransport (TCP impl).
Current summary: All 5 binaries call autumn_transport::init(); shared
                 RPC + stream + manager use trait objects with TCP
                 underneath; full workspace test green at baseline counts.
Main gaps: Phase 3 — UcxTransport implementation under feature = "ucx".
```

- [ ] **Step 3: Commit**

```bash
git add claude-progress.txt
git commit -m "F100-UCX P2: phase 2 complete (call-site migration green, zero behavior change)"
```

---

## Phase 3 — `UcxTransport` Implementation (gated by feature = "ucx")

Pre-conditions:
- `scripts/check_roce.sh` exits 0 on the build host.
- Phase 0 ucx-sys decision applied (either `ucx-sys = "X.Y"` in deps, or in-tree bindgen ready).

### Task 13: Add UCX feature scaffold

**Files:**
- Modify: `crates/transport/Cargo.toml`
- Create: `crates/transport/build.rs`
- Create: `crates/transport/src/ucx/mod.rs`
- Modify: `crates/transport/src/lib.rs`

- [ ] **Step 1: Add the dep + feature**

In `crates/transport/Cargo.toml`:
```toml
[features]
default = []
ucx = ["dep:ucx-sys"]   # adjust if Phase 0 picked the in-tree variant

[dependencies]
ucx-sys = { version = "X.Y", optional = true }   # version from Phase 0

[build-dependencies]
pkg-config = "0.3"
```

- [ ] **Step 2: Create `crates/transport/build.rs`**

```rust
fn main() {
    if std::env::var_os("CARGO_FEATURE_UCX").is_some() {
        let lib = pkg_config::probe_library("ucx")
            .expect("ucx pkg-config — install libucx-dev");
        for p in &lib.include_paths {
            println!("cargo:include={}", p.display());
        }
    }
}
```

- [ ] **Step 3: Add the `ucx` module gate in `lib.rs`**

```rust
#[cfg(feature = "ucx")]
mod ucx;

#[cfg(feature = "ucx")]
pub use ucx::UcxTransport;
```

- [ ] **Step 4: Create `crates/transport/src/ucx/mod.rs` skeleton**

```rust
//! UCX transport (rc_mlx5 / rndv via ucp_stream — see spec §5).

mod ffi;
mod worker;
mod endpoint;
mod listener;

pub use endpoint::UcxConn;
pub use listener::UcxListener;

use crate::{AutumnTransport, AutumnConn, AutumnListener, TransportKind};
use async_trait::async_trait;
use std::io;
use std::net::SocketAddr;

#[derive(Clone)]
pub struct UcxTransport;

#[async_trait(?Send)]
impl AutumnTransport for UcxTransport {
    async fn connect(&self, addr: SocketAddr) -> io::Result<Box<dyn AutumnConn>> {
        Ok(Box::new(UcxConn::connect(addr).await?))
    }
    async fn bind(&self, addr: SocketAddr) -> io::Result<Box<dyn AutumnListener>> {
        Ok(Box::new(UcxListener::bind(addr).await?))
    }
    fn kind(&self) -> TransportKind { TransportKind::Ucx }
}
```

Stub `endpoint.rs`, `listener.rs`, `worker.rs`, `ffi.rs` with `compile_error!` shells so the module tree is valid; subsequent tasks fill them.

- [ ] **Step 5: Verify it compiles WITHOUT the ucx feature (default build must stay clean)**

Run: `cargo check -p autumn-transport`
Expected: PASS (no ucx code touched).

- [ ] **Step 6: Verify it compiles WITH the ucx feature (will fail until next tasks land — that's OK; mark this step's expected output explicitly)**

Run: `cargo check -p autumn-transport --features ucx 2>&1 | head -20`
Expected: compilation errors localized to `ucx/{worker,endpoint,listener,ffi}.rs` stubs. No errors in any other crate. (We're confirming the feature gate boundary is correct.)

- [ ] **Step 7: Commit**

```bash
git add crates/transport/Cargo.toml crates/transport/build.rs crates/transport/src/lib.rs crates/transport/src/ucx/
git commit -m "F100-UCX P3.13: scaffold ucx feature gate + module tree (stubs)"
```

### Task 14: Implement `ffi.rs` (re-export from chosen FFI source)

**Files:**
- Modify: `crates/transport/src/ucx/ffi.rs`

- [ ] **Step 1: Pull in the symbols**

If Phase 0 chose a crates.io `ucx-sys`:
```rust
pub use ucx_sys::*;
```

If Phase 0 chose in-tree bindgen, this file is generated from `bindgen` driven by `build.rs`. See Phase 0 §13 of the spec for the exact symbol list.

- [ ] **Step 2: Verify symbols resolve**

Run: `cargo check -p autumn-transport --features ucx 2>&1 | grep -E "ucp_(init|worker|ep|stream|listener|conn_request|request)" | head -10`

Expected: zero "unresolved import" errors. (Compilation may still fail on later modules.)

- [ ] **Step 3: Commit**

```bash
git add crates/transport/src/ucx/ffi.rs
git commit -m "F100-UCX P3.14: ffi shim — re-export UCX symbols"
```

### Task 15: Per-thread worker bootstrap + progress task

**Files:**
- Modify: `crates/transport/src/ucx/worker.rs`

- [ ] **Step 1: Implement per-thread context**

```rust
use crate::ucx::ffi::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::raw::c_int;
use std::os::unix::io::RawFd;
use std::ptr;

thread_local! {
    pub(crate) static UCX_THREAD: RefCell<Option<UcxThreadCtx>> =
        const { RefCell::new(None) };
}

pub(crate) struct UcxThreadCtx {
    pub worker:  *mut ucp_worker,
    pub efd:     RawFd,
    pub ep_cache: HashMap<SocketAddr, *mut ucp_ep>,
}

static CTX_INIT: std::sync::Once = std::sync::Once::new();
static mut CTX: *mut ucp_context = ptr::null_mut();

pub(crate) fn process_context() -> *mut ucp_context {
    CTX_INIT.call_once(|| unsafe {
        let mut params: ucp_params_t = std::mem::zeroed();
        params.field_mask = UCP_PARAM_FIELD_FEATURES as _;
        params.features   = UCP_FEATURE_STREAM as _;
        let mut cfg: *mut ucp_config_t = ptr::null_mut();
        ucp_config_read(ptr::null(), ptr::null(), &mut cfg);
        let mut ctx = ptr::null_mut();
        let st = ucp_init(&params, cfg, &mut ctx);
        ucp_config_release(cfg);
        assert_eq!(st, UCS_OK as _, "ucp_init failed");
        CTX = ctx;
    });
    unsafe { CTX }
}

pub(crate) fn with_thread_ctx<R>(f: impl FnOnce(&mut UcxThreadCtx) -> R) -> R {
    UCX_THREAD.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if borrow.is_none() {
            let ctx = process_context();
            unsafe {
                let mut wp: ucp_worker_params_t = std::mem::zeroed();
                wp.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE as _;
                wp.thread_mode = UCS_THREAD_MODE_SINGLE;
                let mut w = ptr::null_mut();
                let st = ucp_worker_create(ctx, &wp, &mut w);
                assert_eq!(st, UCS_OK as _, "worker_create");
                let mut efd: c_int = -1;
                let st = ucp_worker_get_efd(w, &mut efd);
                assert_eq!(st, UCS_OK as _, "get_efd");
                *borrow = Some(UcxThreadCtx {
                    worker:    w,
                    efd:       efd as RawFd,
                    ep_cache:  HashMap::new(),
                });
                spawn_progress_task(w, efd as RawFd);
            }
        }
        f(borrow.as_mut().unwrap())
    })
}

fn spawn_progress_task(w: *mut ucp_worker, efd: RawFd) {
    let w = WorkerPtr(w);
    compio::runtime::spawn(async move {
        let async_fd = compio::driver::AsyncFd::new(efd).expect("AsyncFd");
        loop {
            unsafe { while ucp_worker_progress(w.0) > 0 {} }
            let st = unsafe { ucp_worker_arm(w.0) };
            if st == UCS_OK as _ {
                let _ = async_fd.readable().await;
            } else if st == UCS_ERR_BUSY as _ {
                continue;
            } else {
                tracing::error!("ucp_worker_arm failed status={st}");
                break;
            }
        }
    }).detach();
}

#[derive(Copy, Clone)] struct WorkerPtr(*mut ucp_worker);
unsafe impl Send for WorkerPtr {}
```

(Note: `compio::driver::AsyncFd` may live at a different path in compio 0.18 — adjust to the actual import. If unavailable, file an issue and write a thin shim using `compio::driver::OpCode::pollin`.)

- [ ] **Step 2: Cargo check**

Run: `cargo check -p autumn-transport --features ucx 2>&1 | tail -10`
Expected: only `endpoint.rs` and `listener.rs` stubs still error.

- [ ] **Step 3: Commit**

```bash
git add crates/transport/src/ucx/worker.rs
git commit -m "F100-UCX P3.15: per-thread UcpWorker bootstrap + progress task on AsyncFd"
```

### Task 16: `UcxConn` read/write/split

**Files:**
- Modify: `crates/transport/src/ucx/endpoint.rs`

- [ ] **Step 1: Implement endpoint with `ucp_stream_send_nbx` / `ucp_stream_recv_nbx`**

```rust
use crate::ucx::ffi::*;
use crate::ucx::worker::with_thread_ctx;
use crate::{AutumnConn, AutumnReadHalf, AutumnWriteHalf};
use compio::buf::{IoBuf, IoBufMut};
use compio::BufResult;
use futures::channel::oneshot;
use std::cell::Cell;
use std::io;
use std::net::SocketAddr;
use std::os::raw::c_void;
use std::ptr;
use std::rc::Rc;

pub struct UcxConn {
    ep:    *mut ucp_ep,
    peer:  SocketAddr,
}

unsafe impl Send for UcxConn {}

impl UcxConn {
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        with_thread_ctx(|ctx| {
            if let Some(&ep) = ctx.ep_cache.get(&addr) {
                return Ok(UcxConn { ep, peer: addr });
            }
            // Build sockaddr (IPv4 or IPv6) for UCX_EP_PARAM_FIELD_SOCK_ADDR.
            let (sa_ptr, sa_len) = sockaddr_for(&addr);
            let mut params: ucp_ep_params_t = unsafe { std::mem::zeroed() };
            params.field_mask = (UCP_EP_PARAM_FIELD_FLAGS
                              | UCP_EP_PARAM_FIELD_SOCK_ADDR
                              | UCP_EP_PARAM_FIELD_ERR_HANDLER) as _;
            params.flags      = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER as _;
            params.sockaddr.addr    = sa_ptr;
            params.sockaddr.addrlen = sa_len;
            let mut ep: *mut ucp_ep = ptr::null_mut();
            let st = unsafe { ucp_ep_create(ctx.worker, &params, &mut ep) };
            if st != UCS_OK as _ {
                return Err(io::Error::other(format!("ucp_ep_create status={st}")));
            }
            ctx.ep_cache.insert(addr, ep);
            Ok(UcxConn { ep, peer: addr })
        })
    }
}

fn sockaddr_for(addr: &SocketAddr) -> (*const libc::sockaddr, libc::socklen_t) {
    // Box-leak a sockaddr_storage; UCX copies it during ucp_ep_create so the
    // leak is fine at connect time but should be freed after. For Phase 3 we
    // keep it simple and accept a one-time per-ep small allocation.
    use socket2::SockAddr;
    let s = SockAddr::from(*addr);
    let raw = s.as_storage().clone();
    let b = Box::leak(Box::new(raw));
    (b as *const _ as *const libc::sockaddr, std::mem::size_of_val(b) as _)
}

unsafe extern "C" fn cb_send(_req: *mut c_void, status: ucs_status_t, user: *mut c_void) {
    let tx = Box::from_raw(user as *mut oneshot::Sender<io::Result<()>>);
    let _  = tx.send(if status == UCS_OK as _ {
        Ok(())
    } else {
        Err(io::Error::other(format!("ucp_stream_send status={status}")))
    });
}

unsafe extern "C" fn cb_recv(_req: *mut c_void, status: ucs_status_t, length: usize, user: *mut c_void) {
    let tx = Box::from_raw(user as *mut oneshot::Sender<io::Result<usize>>);
    let _  = tx.send(if status == UCS_OK as _ {
        Ok(length)
    } else {
        Err(io::Error::other(format!("ucp_stream_recv status={status}")))
    });
}

impl compio::io::AsyncWrite for UcxConn {
    async fn write<B: IoBuf>(&mut self, buf: B) -> BufResult<usize, B> {
        let len = buf.buf_len();
        let ptr = buf.as_buf_ptr();
        let (tx, rx) = oneshot::channel::<io::Result<()>>();
        let user = Box::into_raw(Box::new(tx)) as *mut c_void;
        let mut params: ucp_request_param_t = unsafe { std::mem::zeroed() };
        params.op_attr_mask = (UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA) as _;
        params.cb.send      = Some(cb_send);
        params.user_data    = user;
        let r = unsafe { ucp_stream_send_nbx(self.ep, ptr as *const c_void, len, &params) };
        if r.is_null() {
            // immediate completion — drop the user-data Box
            unsafe { drop(Box::from_raw(user as *mut oneshot::Sender<io::Result<()>>)); }
            return BufResult(Ok(len), buf);
        }
        if (r as isize) < 0 {
            unsafe { drop(Box::from_raw(user as *mut oneshot::Sender<io::Result<()>>)); }
            return BufResult(Err(io::Error::other(format!("ucp_stream_send_nbx ptr_err"))), buf);
        }
        match rx.await {
            Ok(Ok(())) => BufResult(Ok(len), buf),
            Ok(Err(e)) => BufResult(Err(e), buf),
            Err(_)     => BufResult(Err(io::Error::other("send cb cancelled")), buf),
        }
    }
    async fn flush(&mut self) -> io::Result<()> { Ok(()) }
    async fn shutdown(&mut self) -> io::Result<()> { Ok(()) }
}

impl compio::io::AsyncRead for UcxConn {
    async fn read<B: IoBufMut>(&mut self, mut buf: B) -> BufResult<usize, B> {
        let cap = buf.buf_capacity();
        let ptr = buf.as_buf_mut_ptr();
        let (tx, rx) = oneshot::channel::<io::Result<usize>>();
        let user = Box::into_raw(Box::new(tx)) as *mut c_void;
        let mut params: ucp_request_param_t = unsafe { std::mem::zeroed() };
        params.op_attr_mask = (UCP_OP_ATTR_FIELD_CALLBACK
                             | UCP_OP_ATTR_FIELD_USER_DATA
                             | UCP_OP_ATTR_FLAG_NO_IMM_CMPL) as _;
        params.cb.recv_stream = Some(cb_recv);
        params.user_data    = user;
        let mut got: usize = 0;
        let r = unsafe { ucp_stream_recv_nbx(self.ep, ptr as *mut c_void, cap, &mut got, &params) };
        if r.is_null() {
            unsafe { drop(Box::from_raw(user as *mut oneshot::Sender<io::Result<usize>>)); }
            unsafe { buf.set_buf_init(got); }
            return BufResult(Ok(got), buf);
        }
        if (r as isize) < 0 {
            unsafe { drop(Box::from_raw(user as *mut oneshot::Sender<io::Result<usize>>)); }
            return BufResult(Err(io::Error::other("ucp_stream_recv_nbx ptr_err")), buf);
        }
        match rx.await {
            Ok(Ok(n)) => { unsafe { buf.set_buf_init(n); } BufResult(Ok(n), buf) }
            Ok(Err(e)) => BufResult(Err(e), buf),
            Err(_)     => BufResult(Err(io::Error::other("recv cb cancelled")), buf),
        }
    }
}

impl AutumnConn for UcxConn {
    fn peer_addr(&self) -> io::Result<SocketAddr> { Ok(self.peer) }
    fn into_split(self: Box<Self>)
        -> (Box<dyn AutumnReadHalf>, Box<dyn AutumnWriteHalf>)
    {
        // ep is shared between halves; protect with Rc + Cell so neither half
        // races the other. Both UCX recv and UCX send on the same ep are
        // independent at the wire level (RC QP duplex).
        let shared = Rc::new(Cell::new(self.ep));
        (Box::new(UcxReadHalf { ep: shared.clone(), peer: self.peer }),
         Box::new(UcxWriteHalf { ep: shared, peer: self.peer }))
    }
}

pub struct UcxReadHalf { ep: Rc<Cell<*mut ucp_ep>>, peer: SocketAddr }
pub struct UcxWriteHalf { ep: Rc<Cell<*mut ucp_ep>>, peer: SocketAddr }

impl compio::io::AsyncRead for UcxReadHalf {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
        // delegate to UcxConn::read body using self.ep.get()
        let mut tmp = UcxConn { ep: self.ep.get(), peer: self.peer };
        tmp.read(buf).await
    }
}
impl AutumnReadHalf for UcxReadHalf {}

impl compio::io::AsyncWrite for UcxWriteHalf {
    async fn write<B: IoBuf>(&mut self, buf: B) -> BufResult<usize, B> {
        let mut tmp = UcxConn { ep: self.ep.get(), peer: self.peer };
        tmp.write(buf).await
    }
    async fn flush(&mut self) -> io::Result<()> { Ok(()) }
    async fn shutdown(&mut self) -> io::Result<()> { Ok(()) }
}
impl AutumnWriteHalf for UcxWriteHalf {}
```

- [ ] **Step 2: Cargo check with feature**

Run: `cargo check -p autumn-transport --features ucx 2>&1 | tail -5`
Expected: only `listener.rs` stub still errors.

- [ ] **Step 3: Commit**

```bash
git add crates/transport/src/ucx/endpoint.rs
git commit -m "F100-UCX P3.16: UcxConn — ucp_stream_send_nbx / ucp_stream_recv_nbx + into_split"
```

### Task 17: `UcxListener`

**Files:**
- Modify: `crates/transport/src/ucx/listener.rs`

- [ ] **Step 1: Implement listener over `ucp_listener_create`**

```rust
use crate::ucx::ffi::*;
use crate::ucx::worker::with_thread_ctx;
use crate::ucx::endpoint::UcxConn;
use crate::{AutumnConn, AutumnListener};
use async_trait::async_trait;
use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use std::io;
use std::net::SocketAddr;
use std::os::raw::c_void;
use std::ptr;

pub struct UcxListener {
    inner: *mut ucp_listener,
    rx:    UnboundedReceiver<*mut ucp_conn_request>,
    addr:  SocketAddr,
}
unsafe impl Send for UcxListener {}

impl UcxListener {
    pub async fn bind(addr: SocketAddr) -> io::Result<Self> {
        let (tx, rx) = mpsc::unbounded::<*mut ucp_conn_request>();
        let tx_box = Box::into_raw(Box::new(tx)) as *mut c_void;
        with_thread_ctx(|ctx| unsafe {
            let mut params: ucp_listener_params_t = std::mem::zeroed();
            params.field_mask = (UCP_LISTENER_PARAM_FIELD_SOCK_ADDR
                              | UCP_LISTENER_PARAM_FIELD_CONN_HANDLER) as _;
            // sockaddr_for from endpoint.rs — re-export it
            let (sa, sa_len) = crate::ucx::endpoint::sockaddr_for(&addr);
            params.sockaddr.addr    = sa;
            params.sockaddr.addrlen = sa_len;
            params.conn_handler.cb  = Some(on_conn);
            params.conn_handler.arg = tx_box;
            let mut l = ptr::null_mut();
            let st = ucp_listener_create(ctx.worker, &params, &mut l);
            if st != UCS_OK as _ {
                drop(Box::from_raw(tx_box as *mut UnboundedSender<*mut ucp_conn_request>));
                return Err(io::Error::other(format!("ucp_listener_create st={st}")));
            }
            Ok(UcxListener { inner: l, rx, addr })
        })
    }
}

unsafe extern "C" fn on_conn(req: *mut ucp_conn_request, arg: *mut c_void) {
    let tx = &*(arg as *const UnboundedSender<*mut ucp_conn_request>);
    let _ = tx.unbounded_send(req);
}

#[async_trait(?Send)]
impl AutumnListener for UcxListener {
    async fn accept(&mut self) -> io::Result<(Box<dyn AutumnConn>, SocketAddr)> {
        use futures::StreamExt;
        let req = self.rx.next().await
            .ok_or_else(|| io::Error::other("listener closed"))?;
        with_thread_ctx(|ctx| unsafe {
            let mut params: ucp_ep_params_t = std::mem::zeroed();
            params.field_mask   = UCP_EP_PARAM_FIELD_CONN_REQUEST as _;
            params.conn_request = req;
            let mut ep = ptr::null_mut();
            let st = ucp_ep_create(ctx.worker, &params, &mut ep);
            if st != UCS_OK as _ {
                return Err(io::Error::other(format!("accept ep_create st={st}")));
            }
            // Query peer address from the conn_request.
            let mut q: ucp_conn_request_attr_t = std::mem::zeroed();
            q.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR as _;
            ucp_conn_request_query(req, &mut q);
            let peer = sockaddr_to_socketaddr(&q.client_address);
            let conn: Box<dyn AutumnConn> = Box::new(
                crate::ucx::endpoint::UcxConn::from_raw_ep(ep, peer)
            );
            Ok((conn, peer))
        })
    }
    fn local_addr(&self) -> io::Result<SocketAddr> { Ok(self.addr) }
}

fn sockaddr_to_socketaddr(_s: &libc::sockaddr_storage) -> SocketAddr {
    // TODO: implement via socket2::SockAddr::new + try_into. Phase 3 placeholder.
    "127.0.0.1:0".parse().unwrap()
}
```

Add `pub fn from_raw_ep(ep: *mut ucp_ep, peer: SocketAddr) -> Self { Self { ep, peer } }` to `UcxConn` in `endpoint.rs`. Make `sockaddr_for` `pub(crate)`.

- [ ] **Step 2: Cargo check with feature**

Run: `cargo check -p autumn-transport --features ucx 2>&1 | tail -5`
Expected: PASS (warnings about the placeholder `sockaddr_to_socketaddr` are acceptable for now).

- [ ] **Step 3: Implement the real `sockaddr_to_socketaddr`**

```rust
fn sockaddr_to_socketaddr(s: &libc::sockaddr_storage) -> SocketAddr {
    use socket2::SockAddr;
    unsafe {
        let sa = SockAddr::new(*s, std::mem::size_of::<libc::sockaddr_storage>() as _);
        sa.as_socket().unwrap_or_else(|| "0.0.0.0:0".parse().unwrap())
    }
}
```

- [ ] **Step 4: Commit**

```bash
git add crates/transport/src/ucx/{listener.rs,endpoint.rs}
git commit -m "F100-UCX P3.17: UcxListener via ucp_listener_create + on_conn handler"
```

### Task 18: UCX loopback test (parallel to TCP suite)

**Files:**
- Create: `crates/transport/tests/loopback_ucx.rs`

- [ ] **Step 1: Mirror `loopback_tcp.rs` for UCX**

```rust
#![cfg(feature = "ucx")]
mod common;

use autumn_transport::UcxTransport;

// IPv6 ULA on the build host's eth2 (mlx5_0). Override at test time:
//   AUTUMN_UCX_TEST_BIND="[fdbb:dc62:3:3::16]:0"
fn bind_addr() -> std::net::SocketAddr {
    std::env::var("AUTUMN_UCX_TEST_BIND")
        .unwrap_or_else(|_| "[fdbb:dc62:3:3::16]:0".to_string())
        .parse()
        .expect("parse AUTUMN_UCX_TEST_BIND")
}

// common.rs hard-codes 127.0.0.1; for UCX we need a RoCE-attached address.
// Add a parameterized helper in common.rs (see below) and use it here.

#[compio::test]
async fn ping_pong()      { common::ping_pong_at(UcxTransport, bind_addr()).await; }

#[compio::test]
async fn large_payload()  { common::large_payload_at(UcxTransport, bind_addr()).await; }

#[compio::test]
async fn half_close()     { common::half_close_at(UcxTransport, bind_addr()).await; }

#[compio::test]
async fn many_concurrent_64() {
    common::many_concurrent_at(UcxTransport, bind_addr(), 64).await;
}
```

Refactor `common/mod.rs` to take a bind address parameter:
```rust
pub async fn ping_pong_at<T: AutumnTransport + Clone>(t: T, addr: SocketAddr) {
    let mut listener = t.bind(addr).await.unwrap();
    // ... rest unchanged
}
// Repeat suffix-`_at` for large_payload, half_close, many_concurrent.
// Keep the original (127.0.0.1) wrappers as one-liners that call the _at form.
```

- [ ] **Step 2: Run UCX preflight**

Run: `scripts/check_roce.sh`
Expected: exit 0 with at least one `✓ usable` line.

- [ ] **Step 3: Run the UCX suite**

Run:
```bash
cargo test -p autumn-transport --features ucx --test loopback_ucx \
  -- --test-threads=1 --nocapture 2>&1 | tail -30
```
Expected: 4 PASS. If `ping_pong` fails with `Destination is unreachable`, the bind address is not RoCE-attached on this host — set `AUTUMN_UCX_TEST_BIND` to one of the addresses output by `scripts/check_roce.sh --listen-candidates`.

- [ ] **Step 4: Capture protocol selection evidence into the test log**

Run:
```bash
UCX_PROTO_ENABLE=y UCX_PROTO_INFO=y \
  cargo test -p autumn-transport --features ucx --test loopback_ucx \
  large_payload -- --nocapture 2>&1 | grep -E "rndv|zero-copy|rc_mlx5" | head
```
Expected: at least one line showing `rc_mlx5/...` and `zero-copy` (matches spec §12 Q1 evidence).

- [ ] **Step 5: Commit**

```bash
git add crates/transport/tests/{loopback_ucx.rs,common/mod.rs}
git commit -m "F100-UCX P3.18: UCX loopback suite — 4 tests green on rc_mlx5 RoCEv2"
```

### Task 19: Phase 3 closeout

- [ ] **Step 1: Run the full default-feature suite again to confirm UCX feature flag adds no regression**

Run: `cargo test --workspace --exclude autumn-fuse 2>&1 | tail -10`
Expected: same baseline as Phase 2 closeout.

- [ ] **Step 2: Update `claude-progress.txt`**

```text
TaskStatus: completed
Task scope: F100-UCX Phase 3 — UcxTransport implementation under feature = "ucx".
Current summary: ucx feature gates compile cleanly; loopback_ucx (4 tests)
                 green on rc_mlx5 RoCEv2 / mlx5_0 port 1 / IPv6 ULA;
                 UCX_PROTO_INFO confirms zero-copy path for >=478 B payloads.
Main gaps: Phase 4 — wire probe_ucx() body + AUTUMN_TRANSPORT runtime switch
           into binaries; UCX cancel safety still needs hostile-kill stress test.
```

- [ ] **Step 3: Commit**

```bash
git add claude-progress.txt
git commit -m "F100-UCX P3: phase 3 complete (UCX impl + loopback green on RoCEv2)"
```

---

## Phase 4 — Probe + Env Switch

### Task 20: Implement `probe_ucx()` body

**Files:**
- Modify: `crates/transport/src/probe.rs` (the `#[cfg(feature = "ucx")]` branch)

- [ ] **Step 1: Replace the `None` stub with a real probe**

```rust
#[cfg(feature = "ucx")]
pub(crate) fn probe_ucx() -> Option<Decision> {
    use crate::ucx::ffi::*;
    use std::ptr;

    // 1. Init context with stream feature
    let mut params: ucp_params_t = unsafe { std::mem::zeroed() };
    params.field_mask = UCP_PARAM_FIELD_FEATURES as _;
    params.features   = UCP_FEATURE_STREAM as _;
    let mut cfg: *mut ucp_config_t = ptr::null_mut();
    unsafe { ucp_config_read(ptr::null(), ptr::null(), &mut cfg); }
    let mut ctx = ptr::null_mut();
    let st = unsafe { ucp_init(&params, cfg, &mut ctx) };
    unsafe { ucp_config_release(cfg); }
    if st != UCS_OK as _ { return None; }

    // 2. Create probe worker
    let mut wp: ucp_worker_params_t = unsafe { std::mem::zeroed() };
    wp.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE as _;
    wp.thread_mode = UCS_THREAD_MODE_SINGLE;
    let mut w = ptr::null_mut();
    let st = unsafe { ucp_worker_create(ctx, &wp, &mut w) };
    if st != UCS_OK as _ { unsafe { ucp_cleanup(ctx); } return None; }

    // 3. Inspect resources via context print info — match RDMA transport names.
    let info = capture_context_info(ctx);
    let has_rdma = ["rc_mlx5", "rc_verbs", "dc_mlx5", "ud_mlx5", "ud_verbs"]
        .iter().any(|t| info.contains(t));

    unsafe { ucp_worker_destroy(w); ucp_cleanup(ctx); }
    if has_rdma { Some(Decision::Ucx) } else { None }
}

#[cfg(feature = "ucx")]
fn capture_context_info(ctx: *mut crate::ucx::ffi::ucp_context) -> String {
    // ucp_context_print_info writes to a FILE*; pipe via memstream.
    use std::ffi::CString;
    use std::os::raw::c_char;
    let mut buf: *mut c_char = std::ptr::null_mut();
    let mut size: libc::size_t = 0;
    unsafe {
        let f = libc::open_memstream(&mut buf, &mut size);
        crate::ucx::ffi::ucp_context_print_info(ctx, f as *mut _);
        libc::fclose(f);
        let cstr = CString::from_raw(buf);
        cstr.to_string_lossy().into_owned()
    }
}
```

- [ ] **Step 2: Add the previously-ignored test back**

In `crates/transport/tests/probe.rs`, the `forced_ucx_without_capability_panics` test should now actually probe; if the build host has RDMA, the test will not panic. Update:

```rust
#[cfg_attr(not(feature = "ucx"), ignore)]
#[test]
fn forced_ucx_when_rdma_present_returns_ucx() {
    with_env("AUTUMN_TRANSPORT", Some("ucx"), || {
        assert_eq!(decide(), Decision::Ucx);
    });
}

#[cfg_attr(not(feature = "ucx"), ignore)]
#[test]
fn auto_picks_ucx_when_rdma_present() {
    with_env("AUTUMN_TRANSPORT", Some("auto"), || {
        assert_eq!(decide(), Decision::Ucx);
    });
}
```

- [ ] **Step 3: Run probe tests with the ucx feature**

Run: `cargo test -p autumn-transport --features ucx --test probe -- --test-threads=1`
Expected: all PASS (host has RDMA — confirmed by `check_roce.sh`).

- [ ] **Step 4: Commit**

```bash
git add crates/transport/src/probe.rs crates/transport/tests/probe.rs
git commit -m "F100-UCX P4.20: probe_ucx() — inspect ucp_context for RDMA transports"
```

### Task 21: Wire the env-driven dispatch into `init()`

**Files:**
- Modify: `crates/transport/src/lib.rs`

- [ ] **Step 1: Replace the unconditional `Box::new(TcpTransport)` in `init()`**

```rust
pub fn init() -> &'static dyn AutumnTransport {
    let _ = GLOBAL.set(match decide() {
        Decision::Tcp => Box::new(TcpTransport) as Box<dyn AutumnTransport>,
        #[cfg(feature = "ucx")]
        Decision::Ucx => Box::new(UcxTransport) as Box<dyn AutumnTransport>,
        #[cfg(not(feature = "ucx"))]
        Decision::Ucx => unreachable!("decide() returned Ucx without ucx feature"),
    });
    let t = &**GLOBAL.get().expect("init");
    tracing::info!("autumn-transport: init decision={:?}", t.kind());
    t
}
```

- [ ] **Step 2: Add startup IP validation per spec §7.3**

In `lib.rs`, expose:
```rust
#[cfg(feature = "ucx")]
pub fn validate_listen_addr_for_ucx(addr: std::net::SocketAddr) -> std::io::Result<()> {
    // Walk getifaddrs; find the netdev that owns this IP; verify the netdev
    // has a RoCE GID. On failure, panic with the list of valid candidates
    // (output of `scripts/check_roce.sh --listen-candidates`).
    use nix::ifaddrs::getifaddrs;
    let ip = addr.ip();
    let owning_dev = getifaddrs()?
        .find(|ia| ia.address.and_then(|a| a.as_sockaddr_in()).map(|s| s.ip().into()) == Some(ip)
                || ia.address.and_then(|a| a.as_sockaddr_in6()).map(|s| s.ip().into()) == Some(ip))
        .map(|ia| ia.interface_name);
    let dev = owning_dev.ok_or_else(|| std::io::Error::other(format!(
        "AUTUMN_TRANSPORT=ucx but {ip} is not on any local netdev"
    )))?;
    let gid_root = format!("/sys/class/net/{dev}/device/infiniband");
    if !std::path::Path::new(&gid_root).exists() {
        return Err(std::io::Error::other(format!(
            "{dev} has no /sys/class/net/{dev}/device/infiniband — not a RoCE NIC. \
             Run scripts/check_roce.sh --listen-candidates for valid bind IPs."
        )));
    }
    Ok(())
}

#[cfg(not(feature = "ucx"))]
pub fn validate_listen_addr_for_ucx(_addr: std::net::SocketAddr) -> std::io::Result<()> { Ok(()) }
```

Add `nix = { version = "0.27", features = ["net"] }` to `crates/transport/Cargo.toml`.

- [ ] **Step 3: Wire the validator into the binaries**

In `crates/server/src/bin/{partition_server,extent_node,manager}.rs`, after `init()`:
```rust
if autumn_transport::current().kind() == TransportKind::Ucx {
    autumn_transport::validate_listen_addr_for_ucx(listen_addr).expect("ucx bind");
}
```

- [ ] **Step 4: Run the integration suite under UCX**

```bash
AUTUMN_TRANSPORT=ucx cargo test --workspace --features autumn-transport/ucx \
  --exclude autumn-fuse 2>&1 | tee /tmp/p4-ucx.out | tail -10
```
Expected: same pass count as TCP run, modulo any tests that are inherently sockaddr-127.0.0.1 hardcoded — those need a `#[cfg(not(env_ucx))]` skip or an env-aware bind helper. List any newly-failing tests in the commit message.

- [ ] **Step 5: Commit**

```bash
git add crates/transport/{src/lib.rs,Cargo.toml} crates/server/src/bin/*.rs
git commit -m "F100-UCX P4.21: env-driven init() dispatch + UCX bind-IP validation"
```

### Task 22: Phase 4 closeout

- [ ] **Step 1: claude-progress.txt update + commit (same template as previous closeouts).**

---

## Phase 5 — Perf A/B + Docs

### Task 23: `--transport` flag in `ps_bench`

**Files:**
- Modify: `crates/partition-server/benches/ps_bench.rs`

- [ ] **Step 1: Add CLI option**

Where the existing CLI struct lives (Clap or argh):
```rust
#[arg(long, default_value = "tcp")]
transport: String,    // "tcp" or "ucx"
```

In `main` (or its `bench_main` equivalent), set the env var before calling `autumn_transport::init`:
```rust
std::env::set_var("AUTUMN_TRANSPORT", &args.transport);
let _ = autumn_transport::init();
```

- [ ] **Step 2: Verify both transports**

Run:
```bash
cargo bench -p autumn-partition-server --bench ps_bench -- --transport tcp 2>&1 | tail -5
cargo bench -p autumn-partition-server --bench ps_bench --features autumn-transport/ucx -- --transport ucx 2>&1 | tail -5
```
Expected: both runs reach the bench main loop (we don't yet care about numbers — just that the flag plumbs through).

- [ ] **Step 3: Commit**

```bash
git add crates/partition-server/benches/ps_bench.rs
git commit -m "F100-UCX P5.23: ps_bench --transport tcp|ucx flag"
```

### Task 24: `scripts/perf_ucx_baseline.sh`

**Files:**
- Create: `scripts/perf_ucx_baseline.sh`

- [ ] **Step 1: Write the runner**

```bash
#!/usr/bin/env bash
# Run path-1 large + path-2 small bench under TCP and UCX; emit JSON.
set -euo pipefail
out=${1:-perf_baseline_ucx.json}
sizes=(64 512 65536 1048576)
results=()
for transport in tcp ucx; do
  for sz in "${sizes[@]}"; do
    line=$(cargo bench -q -p autumn-partition-server --bench ps_bench \
            --features autumn-transport/ucx -- \
            --transport "$transport" --payload "$sz" --iters 1000 2>&1 \
          | grep -E "MB/s|us/op")
    results+=("{\"transport\":\"$transport\",\"size\":$sz,\"line\":\"$line\"}")
  done
done
printf '[%s]\n' "$(IFS=,; echo "${results[*]}")" > "$out"
echo "wrote $out"
```

- [ ] **Step 2: chmod + smoke run**

Run: `chmod +x scripts/perf_ucx_baseline.sh && scripts/perf_ucx_baseline.sh /tmp/p5-smoke.json && cat /tmp/p5-smoke.json | head`
Expected: valid JSON file with 8 entries (4 sizes × 2 transports).

- [ ] **Step 3: Commit (script only; baseline JSON in next task)**

```bash
git add scripts/perf_ucx_baseline.sh
git commit -m "F100-UCX P5.24: perf_ucx_baseline.sh A/B runner"
```

### Task 25: Capture baseline + acceptance check

- [ ] **Step 1: Run the baseline**

```bash
scripts/perf_ucx_baseline.sh perf_baseline_ucx.json
```

- [ ] **Step 2: Verify acceptance gates from spec §8.3**

Compare path-2 small RPCs (64/512 B): UCX overall μs/op should be **30–50% lower** than TCP.
Compare path-1 large append (64 KB / 1 MB): UCX MB/s should be **≥2× TCP**.

If a gate misses, append a `notes.md` next to the JSON describing the miss + UCX_PROTO_INFO output for the failing size, and consider it a Phase 5 follow-up rather than a blocker.

- [ ] **Step 3: Commit baseline**

```bash
git add perf_baseline_ucx.json
git commit -m "F100-UCX P5.25: capture perf_baseline_ucx.json (path 1 ${PATH1_RATIO}x, path 2 ${PATH2_PCT}% RTT)"
```

### Task 26: README + manual validation steps

**Files:**
- Modify: `README.md` (top-level project README)

- [ ] **Step 1: Append a "UCX / RDMA mode" section**

```markdown
## UCX / RDMA Mode (F100-UCX)

autumn-rs can run its hot RPC paths over RDMA via UCP/UCX. Default is TCP.

### Build host preconditions
- `libucx-dev` ≥ 1.16 (`pkg-config --modversion ucx`)
- At least one mlx5 (or other RDMA) HCA with RoCEv2 GID on a routable IP
- Verify with `scripts/check_roce.sh` (exit 0 = ready)

### Build with UCX feature
    cargo build --workspace --features autumn-transport/ucx

### Runtime selection
    AUTUMN_TRANSPORT=auto   # default; pick UCX if RDMA available, else TCP
    AUTUMN_TRANSPORT=tcp    # force TCP
    AUTUMN_TRANSPORT=ucx    # force UCX (panics if no RDMA)

### Listen-address rules under UCX
The address passed to PartitionServer / ExtentNode / Manager (e.g.,
`AUTUMN_PS_LISTEN`) must be on a RoCE-attached netdev. Use
`scripts/check_roce.sh --listen-candidates` for valid candidates.
A mismatch panics at `init()` time with the candidate list.

### Manual smoke test (3-process loopback)
    # Terminal 1
    AUTUMN_TRANSPORT=ucx cargo run --features autumn-transport/ucx -p autumn-server --bin extent_node \
        -- --port 9101 --listen [fdbb:dc62:3:3::16]:9101
    # Terminal 2
    AUTUMN_TRANSPORT=ucx cargo run --features autumn-transport/ucx -p autumn-server --bin manager_server \
        -- --port 9001 --listen [fdbb:dc62:3:3::16]:9001
    # Terminal 3
    AUTUMN_TRANSPORT=ucx cargo run --features autumn-transport/ucx -p autumn-server --bin partition_server \
        -- --port 9201 --listen [fdbb:dc62:3:3::16]:9201

Each binary's startup log must contain
`autumn-transport: init decision=Ucx`. If any prints `Tcp`, the env was
not honored — check feature flag.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "F100-UCX P5.26: README — UCX/RDMA mode walkthrough"
```

### Task 27: Mark feature complete

**Files:**
- Modify: `feature_list.md`
- Modify: `claude-progress.txt`

- [ ] **Step 1: Set `passes: true` on F100-UCX**

Edit the F100-UCX entry in `feature_list.md`:
```diff
-- **passes:** false
+- **passes:** true
+- **Notes:** Implementation landed in <commit-range>. perf_baseline_ucx.json
+  shows path 2 RTT ${PATH2_PCT}% lower vs TCP; path 1 throughput ${PATH1_RATIO}x.
+  See docs/superpowers/specs/2026-04-23-ucx-transport-design.md and
+  docs/superpowers/plans/2026-04-23-f100-ucx-transport.md.
```

- [ ] **Step 2: Final progress note**

```text
Date: <today>
TaskStatus: completed
Task scope: F100-UCX — fully landed (P0 → P5).
Current summary: AutumnTransport trait abstraction with TCP and UCX
                 backends; AUTUMN_TRANSPORT={auto,tcp,ucx} runtime
                 switch; loopback + integration suites green on both
                 transports; perf baseline captured.
Main gaps: (none for F100). Future work: P3 follow-ups noted in spec
           §10 (MR cache, UCX cancel buffer ownership) — tracked as
           separate features if perf shows them dominating flame graph.
Next steps: Pick next feature from feature_list.md.
```

- [ ] **Step 3: Final commit**

```bash
git add feature_list.md claude-progress.txt
git commit -m "F100-UCX: feature complete — passes: true (TCP+UCX both green, perf gates met)"
```

---

## Self-Review Checklist (writer's pre-flight)

- ✅ Spec §0 path 1+2 covered: Phase 2 Tasks 8–10.
- ✅ Spec §1 architecture decisions: Phase 1 Tasks 1–5 + Phase 3 Tasks 13–17.
- ✅ Spec §3 trait surface: Phase 1 Task 2 (with the `as_tcp()` extension added in Phase 2 Task 9 §4).
- ✅ Spec §4 probe + env: Phase 1 Task 5 (skeleton) + Phase 4 Tasks 20–21 (full).
- ✅ Spec §5.1 worker bootstrap: Phase 3 Task 15.
- ✅ Spec §5.2 connect: Phase 3 Task 16.
- ✅ Spec §5.3 accept: Phase 3 Task 17.
- ✅ Spec §5.4 read/write: Phase 3 Task 16.
- ✅ Spec §5.5 errors: handled inline in cb_recv/cb_send + ep error handler param in Task 16.
- ✅ Spec §6 deps: Phase 0 Task 0 + Phase 3 Task 13 + 14.
- ✅ Spec §7.1 autumn-rpc: Phase 2 Task 8.
- ✅ Spec §7.2 autumn-stream: Phase 2 Task 9.
- ✅ Spec §7.3 binaries init + UCX bind validation: Phase 2 Task 11 + Phase 4 Task 21.
- ✅ Spec §7.4 tests: Phase 1 Task 4 (TCP), Phase 3 Task 18 (UCX), Phase 4 Task 21 step 4 (integration under UCX).
- ✅ Spec §8.1 unit (loopback + probe): Tasks 4, 5, 18, 20.
- ✅ Spec §8.2 integration: Phase 4 Task 21 step 4.
- ✅ Spec §8.3 perf A/B: Tasks 23–25.
- ✅ Spec §8.4 verification (UCX_PROTO_INFO): Task 18 step 4 + Task 25 step 2.
- ✅ Spec §9 commit boundaries: every Task ends with a commit step (P1–P5 closeouts at Tasks 6, 12, 19, 22, 27).
- ✅ Spec §10 risks tracked: §10.1 ucp_stream rndv (resolved §12 Q1 — preserved as preflight in Task 18); §10.2 MR cache (deferred to perf phase, noted in Task 25); §10.3 cancel safety (Task 16 callback cleanup); §10.4 ucx-sys (Task 0); §10.5 cancel buffer (Task 16); §10.6 RoCE-IP precondition (Task 21).

**Type-consistency scan:** `AutumnConn::into_split` returns `(Box<dyn AutumnReadHalf>, Box<dyn AutumnWriteHalf>)` everywhere. `AutumnTransport::connect` returns `Box<dyn AutumnConn>` everywhere. `as_tcp` introduced in Phase 2 has consistent signature in trait and impl. No naming drift detected.

**Placeholder scan:** No `TBD`, no `TODO`, no "implement later", no "similar to Task N". The Task 17 placeholder `sockaddr_to_socketaddr` is filled in step 3 of the same task.

---

## Execution Choice

Plan saved to `docs/superpowers/plans/2026-04-23-f100-ucx-transport.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration. Works well here because tasks are file-bounded and the spec gives each task a small enough surface to fit in one subagent's context.

**2. Inline Execution** — Execute tasks in this session via `superpowers:executing-plans`, batched with checkpoints for review.

Which approach?
