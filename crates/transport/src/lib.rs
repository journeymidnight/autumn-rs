//! autumn-transport — pluggable transport for autumn-rs (TCP today, UCX optional; F100-UCX).
//!
//! ## Why enum dispatch instead of trait objects
//!
//! Spec §3 originally drafted `Box<dyn AutumnConn>` for runtime polymorphism.
//! That doesn't compile against compio 0.18: `compio::io::AsyncRead::read` and
//! `AsyncWrite::write` are generic over the buffer type (`B: IoBufMut`,
//! `T: IoBuf`), and a trait with generic methods is not `dyn`-compatible.
//!
//! Instead we expose concrete `enum Conn { Tcp(...), Ucx(...) }` /
//! `enum Listener` / `enum ReadHalf` / `enum WriteHalf`. The `AutumnTransport`
//! trait stays `dyn`-safe (its methods are non-generic, returning the concrete
//! enums) so `&'static dyn AutumnTransport` from `init()` still gives runtime
//! transport selection. Hot-path I/O dispatches via enum match — strictly
//! cheaper than the vtable hop §12 Q2 originally analysed.
//!
//! TCP variants carry `compio::net` types directly; UCX variants (Phase 3)
//! will carry the custom `UcxConn` / `UcxListener` types from `crate::ucx`.

use std::io;
use std::net::SocketAddr;
use std::sync::OnceLock;

mod probe;
mod tcp;
#[cfg(feature = "ucx")]
mod ucx;

// F216-E: the registered-buffer pool is transport-agnostic (compiles with and
// without `ucx`); only the `ibv_reg_mr` call inside is `cfg(ucx)`. Declared at
// top level (file lives under ucx/ for history) so autumn-rpc/stream can use
// `PooledBuf` with no `cfg` — preserving the "only the transport leaf is
// cfg-gated" pattern.
#[path = "ucx/regpool.rs"]
mod regpool;

pub use probe::parse_transport_flag;
pub use regpool::{
    acquire as regpool_acquire, set_regpool_cap_bytes,
    snapshot as regpool_snapshot, PooledBuf, RegpoolStats,
};
pub use tcp::TcpTransport;
#[cfg(feature = "ucx")]
pub use ucx::UcxTransport;
#[cfg(feature = "ucx")]
pub use ucx::{register_memory, RegisteredMem};

/// Placeholder when built without the `ucx` feature — never constructed (TCP
/// has no memory registration). Lets the recv-into seam
/// (`ReadHalf::recv_into(reg: Option<&RegisteredMem>)`) compile uniformly
/// across features; `reg` is always `None` on TCP-only builds.
#[cfg(not(feature = "ucx"))]
pub enum RegisteredMem {}

static GLOBAL: OnceLock<Box<dyn AutumnTransport>> = OnceLock::new();

/// Initialise the process-global transport with an explicit `TransportKind`.
/// Idempotent — the first call wins; subsequent calls are no-ops.
///
/// Panics if `TransportKind::Ucx` is requested but the binary was built
/// without the `ucx` feature.
pub fn init_with(kind: TransportKind) -> &'static dyn AutumnTransport {
    let _ = GLOBAL.set(match kind {
        TransportKind::Tcp => Box::new(TcpTransport) as Box<dyn AutumnTransport>,
        #[cfg(feature = "ucx")]
        TransportKind::Ucx => Box::new(UcxTransport) as Box<dyn AutumnTransport>,
        #[cfg(not(feature = "ucx"))]
        TransportKind::Ucx => {
            panic!("binary requested --transport ucx but was built without the `ucx` feature")
        }
    });
    let t = &**GLOBAL.get().expect("init_with");
    tracing::info!("autumn-transport: init kind={:?}", t.kind());
    t
}

/// True iff the process-global transport is initialised AND is UCX. The
/// regpool uses this to decide whether registering a fresh slab pays off — a
/// ucx-FEATURE binary running the TCP transport must NOT `ucp_mem_map` (memh
/// is never used on TCP; registering would only initialise a UCX context +
/// pin memlock for nothing). Deliberately does NOT lazy-init: an
/// uninitialised transport reads as "not UCX" (every regpool recv path runs
/// behind a live connection, which implies the transport is initialised).
#[cfg(feature = "ucx")]
pub(crate) fn runtime_transport_is_ucx() -> bool {
    GLOBAL
        .get()
        .map(|t| t.kind() == TransportKind::Ucx)
        .unwrap_or(false)
}

/// Read the process-global transport. Panics if `init()` was never called.
///
/// Use this from binaries that call `init()` explicitly at startup so a
/// missing initialisation surfaces as a panic rather than silent fallback.
pub fn current() -> &'static dyn AutumnTransport {
    &**GLOBAL
        .get()
        .expect("autumn_transport::init() must be called once at startup")
}

/// Read the process-global transport, lazily initialising it with TCP if
/// `init_with()` was never called. Use this from library code
/// (`autumn-rpc`, `autumn-stream`, …) so tests and library users don't
/// have to call `init_with()` explicitly.
pub fn current_or_init() -> &'static dyn AutumnTransport {
    if let Some(t) = GLOBAL.get() {
        return &**t;
    }
    init_with(TransportKind::Tcp)
}

/// Format `host` (an IPv4 or IPv6 literal, with or without brackets) and
/// `port` into a parsed `SocketAddr`. Brackets are stripped and re-applied
/// as needed, so `"::1"`, `"[::1]"`, and `"fdbd:dc62:3:302::14"` all work.
pub fn format_listen_addr(host: &str, port: u16) -> io::Result<SocketAddr> {
    let host = host.trim_matches(['[', ']']);
    let s = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    s.parse()
        .map_err(|e| io::Error::other(format!("parse listen addr {s:?}: {e}")))
}

/// Deployment preflight: log a warning when a non-wildcard listen address is
/// on a netdev without a RoCE GID table and `kind == Ucx`. Always returns
/// `Ok(())` — UCX may still fall back to TCP/shm at runtime; the caller
/// decides whether to abort. No-op under TCP.
///
/// Call this after `init_with()` (passing `current().kind()`) in server binaries
/// so operators see a clear diagnostic when binding on a non-RDMA interface.
pub fn check_listen_addr(addr: SocketAddr, kind: TransportKind) -> io::Result<()> {
    if kind != TransportKind::Ucx {
        return Ok(());
    }
    let ip = addr.ip();
    // Wildcards (0.0.0.0, [::]) are acceptable — UCX binds all interfaces.
    if ip.is_unspecified() {
        return Ok(());
    }
    let dev = match find_netdev_owning_ip(&ip) {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(
                "autumn-transport: UCX listen on {ip}: not found on any local netdev ({e}); \
                 UCX may fall back to TCP/shm"
            );
            return Ok(());
        }
    };
    let gid_dir = format!("/sys/class/net/{dev}/device/infiniband");
    if std::path::Path::new(&gid_dir).exists() {
        tracing::info!("autumn-transport: UCX listen on {ip} via {dev} (RoCE-attached)");
    } else {
        let candidates = roce_candidates();
        tracing::warn!(
            ?candidates,
            "autumn-transport: UCX listen on {ip} (netdev {dev}) has no RoCE GID table; \
             UCX will likely fall back to TCP/shm. \
             Run scripts/check_roce.sh --listen-candidates to see valid bind IPs."
        );
    }
    Ok(())
}

/// All `(netdev name, IP)` pairs on this host, via one getifaddrs walk.
/// (`/sys/class/net/<dev>` doesn't carry IP info; getifaddrs via libc is
/// simpler than parsing /proc/net/{fib_trie,if_inet6} and we already pull
/// libc.) Cold path — deployment diagnostics only.
fn all_ifaddrs() -> io::Result<Vec<(String, std::net::IpAddr)>> {
    let mut out = Vec::new();
    unsafe {
        let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
        if libc::getifaddrs(&mut head) != 0 {
            return Err(io::Error::last_os_error());
        }
        let mut cur = head;
        while !cur.is_null() {
            let ifa = &*cur;
            if !ifa.ifa_addr.is_null() {
                let ip = match (*ifa.ifa_addr).sa_family as i32 {
                    libc::AF_INET => {
                        let sa = ifa.ifa_addr as *const libc::sockaddr_in;
                        let raw = u32::from_be((*sa).sin_addr.s_addr);
                        Some(std::net::IpAddr::V4(std::net::Ipv4Addr::from(raw)))
                    }
                    libc::AF_INET6 => {
                        let sa = ifa.ifa_addr as *const libc::sockaddr_in6;
                        Some(std::net::IpAddr::V6(std::net::Ipv6Addr::from(
                            (*sa).sin6_addr.s6_addr,
                        )))
                    }
                    _ => None,
                };
                if let Some(ip) = ip {
                    let name = std::ffi::CStr::from_ptr(ifa.ifa_name)
                        .to_string_lossy()
                        .into_owned();
                    out.push((name, ip));
                }
            }
            cur = ifa.ifa_next;
        }
        libc::freeifaddrs(head);
    }
    Ok(out)
}

fn find_netdev_owning_ip(ip: &std::net::IpAddr) -> io::Result<String> {
    all_ifaddrs()?
        .into_iter()
        .find(|(_, a)| a == ip)
        .map(|(name, _)| name)
        .ok_or_else(|| io::Error::other(format!("ip {ip} not found on any local netdev")))
}

fn roce_candidates() -> Vec<(String, std::net::IpAddr)> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir("/sys/class/net") else {
        return out;
    };
    for entry in entries.flatten() {
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        let ib = format!("/sys/class/net/{name}/device/infiniband");
        if !std::path::Path::new(&ib).exists() {
            continue;
        }
        // Collect non-link-local IPs on this netdev via getifaddrs.
        let ips = netdev_ips(&name);
        for ip in ips {
            if !ip.is_loopback() && !is_link_local(&ip) {
                out.push((name.clone(), ip));
            }
        }
    }
    out
}

fn netdev_ips(target_dev: &str) -> Vec<std::net::IpAddr> {
    all_ifaddrs()
        .map(|addrs| {
            addrs
                .into_iter()
                .filter(|(name, _)| name == target_dev)
                .map(|(_, ip)| ip)
                .collect()
        })
        .unwrap_or_default()
}

fn is_link_local(ip: &std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => v4.is_link_local(),
        std::net::IpAddr::V6(v6) => (v6.octets()[0] == 0xfe) && (v6.octets()[1] & 0xc0 == 0x80),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    Tcp,
    Ucx,
}

#[async_trait::async_trait(?Send)]
pub trait AutumnTransport: Send + Sync + 'static {
    async fn connect(&self, addr: SocketAddr) -> io::Result<Conn>;
    async fn bind(&self, addr: SocketAddr) -> io::Result<Listener>;
    fn kind(&self) -> TransportKind;
}

pub enum Conn {
    Tcp(compio::net::TcpStream),
    #[cfg(feature = "ucx")]
    Ucx(crate::ucx::endpoint::UcxConn),
}

pub enum Listener {
    Tcp(compio::net::TcpListener),
    #[cfg(feature = "ucx")]
    Ucx(crate::ucx::listener::UcxListener),
}

pub enum ReadHalf {
    Tcp(compio::net::OwnedReadHalf<compio::net::TcpStream>),
    #[cfg(feature = "ucx")]
    Ucx(crate::ucx::endpoint::UcxReadHalf),
}

pub enum WriteHalf {
    Tcp(compio::net::OwnedWriteHalf<compio::net::TcpStream>),
    #[cfg(feature = "ucx")]
    Ucx(crate::ucx::endpoint::UcxWriteHalf),
}

// ---- Conn API ----

impl Conn {
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Conn::Tcp(s) => s.peer_addr(),
            #[cfg(feature = "ucx")]
            Conn::Ucx(c) => c.peer_addr(),
        }
    }

    pub fn into_split(self) -> (ReadHalf, WriteHalf) {
        match self {
            Conn::Tcp(s) => {
                let (r, w) = s.into_split();
                (ReadHalf::Tcp(r), WriteHalf::Tcp(w))
            }
            #[cfg(feature = "ucx")]
            Conn::Ucx(c) => {
                let (r, w) = c.into_split();
                (ReadHalf::Ucx(r), WriteHalf::Ucx(w))
            }
        }
    }

    /// `Some(_)` only for the `Tcp` variant — call sites that need TCP-only
    /// socket tuning (`SO_RCVBUF`, `TCP_NODELAY`) gate on this.
    pub fn as_tcp(&self) -> Option<&compio::net::TcpStream> {
        match self {
            Conn::Tcp(s) => Some(s),
            #[cfg(feature = "ucx")]
            Conn::Ucx(_) => None,
        }
    }
}

impl compio::io::AsyncRead for Conn {
    async fn read<B: compio::buf::IoBufMut>(&mut self, buf: B) -> compio::BufResult<usize, B> {
        match self {
            Conn::Tcp(s) => s.read(buf).await,
            #[cfg(feature = "ucx")]
            Conn::Ucx(c) => c.read(buf).await,
        }
    }
}

impl compio::io::AsyncWrite for Conn {
    async fn write<B: compio::buf::IoBuf>(&mut self, buf: B) -> compio::BufResult<usize, B> {
        match self {
            Conn::Tcp(s) => s.write(buf).await,
            #[cfg(feature = "ucx")]
            Conn::Ucx(c) => c.write(buf).await,
        }
    }
    /// Forward to the inner type's `write_vectored` so the TcpStream's
    /// native sendmsg-with-N-iovecs path is reachable. The default trait
    /// impl loops calling `write` per buffer (= N syscalls), which costs
    /// ~2× write throughput on the rpc client's 2-iov header+payload path.
    async fn write_vectored<T: compio::buf::IoVectoredBuf>(
        &mut self,
        buf: T,
    ) -> compio::BufResult<usize, T> {
        match self {
            Conn::Tcp(s) => s.write_vectored(buf).await,
            #[cfg(feature = "ucx")]
            Conn::Ucx(c) => c.write_vectored(buf).await,
        }
    }
    async fn flush(&mut self) -> io::Result<()> {
        match self {
            Conn::Tcp(s) => s.flush().await,
            #[cfg(feature = "ucx")]
            Conn::Ucx(c) => c.flush().await,
        }
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        match self {
            Conn::Tcp(s) => s.shutdown().await,
            #[cfg(feature = "ucx")]
            Conn::Ucx(c) => c.shutdown().await,
        }
    }
}

// ---- Listener API ----

impl Listener {
    pub async fn accept(&mut self) -> io::Result<(Conn, SocketAddr)> {
        match self {
            Listener::Tcp(l) => {
                let (s, peer) = l.accept().await?;
                Ok((Conn::Tcp(s), peer))
            }
            #[cfg(feature = "ucx")]
            Listener::Ucx(l) => {
                let (c, peer) = l.accept().await?;
                Ok((Conn::Ucx(c), peer))
            }
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Listener::Tcp(l) => l.local_addr(),
            #[cfg(feature = "ucx")]
            Listener::Ucx(l) => l.local_addr(),
        }
    }
}

// ---- Half APIs ----

impl ReadHalf {
    /// True for the UCX variant. The autumn-rpc read_loop uses this to choose
    /// the recv-into-registered-buffer path (UCX) vs owned-read/decode+memcpy
    /// (TCP) for `call_into_pooled`.
    pub fn is_ucx(&self) -> bool {
        #[cfg(feature = "ucx")]
        {
            matches!(self, ReadHalf::Ucx(_))
        }
        #[cfg(not(feature = "ucx"))]
        {
            false
        }
    }
}

impl compio::io::AsyncRead for ReadHalf {
    async fn read<B: compio::buf::IoBufMut>(&mut self, buf: B) -> compio::BufResult<usize, B> {
        match self {
            ReadHalf::Tcp(r) => r.read(buf).await,
            #[cfg(feature = "ucx")]
            ReadHalf::Ucx(r) => r.read(buf).await,
        }
    }
}

impl ReadHalf {
    /// F216 zero-copy recv into a pre-registered buffer (UCX only). Single
    /// recv (may be partial). See `UcxReadHalf::recv_registered`.
    #[cfg(feature = "ucx")]
    pub async fn recv_registered(
        &mut self,
        buf: &mut [u8],
        reg: &RegisteredMem,
    ) -> io::Result<usize> {
        match self {
            ReadHalf::Ucx(r) => r.recv_registered(buf, reg).await,
            ReadHalf::Tcp(_) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "recv_registered is UCX-only",
            )),
        }
    }

    /// F216 recv-into seam (UCX). `Some(reg)` → zero-copy receive via memh;
    /// `None` (regpool over-cap fallback) → UCX recv into the slice (copy-out).
    /// Single recv (may be partial); caller loops for read_exact semantics.
    /// TCP errors — the autumn-rpc read_loop handles TCP ZC recvs via
    /// `read_exact_into_pooled` / the normal decode + memcpy path, never this
    /// seam. Defined for all builds so autumn-rpc compiles uniformly; on
    /// TCP-only builds it always errors.
    pub async fn recv_into(
        &mut self,
        buf: &mut [u8],
        reg: Option<&RegisteredMem>,
    ) -> io::Result<usize> {
        match self {
            #[cfg(feature = "ucx")]
            ReadHalf::Ucx(r) => match reg {
                Some(reg) => r.recv_registered(buf, reg).await,
                None => r.recv_unregistered(buf).await,
            },
            ReadHalf::Tcp(_) => {
                let _ = (buf, reg);
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "recv_into is UCX-only; TCP uses read_exact_into_pooled / decode+memcpy",
                ))
            }
        }
    }

    /// F219 — TCP recv the remaining `[filled..target]` bytes of a value
    /// straight into a `PooledBuf` via compio owned reads, eliminating the
    /// FrameDecoder accumulation copy (only the unavoidable kernel→userspace
    /// copy remains). The counterpart to UCX's `recv_into` on the recv-side
    /// zero-copy paths (PS←EN read = `call_into_pooled`, PS←client write =
    /// `drain_zc_writes`). Reads exactly `target - filled` bytes; returns the
    /// filled buffer.
    ///
    /// Cancel-safe: the caller (a read_loop / ps-conn task) OWNS `pb` across the
    /// await, and compio retains the owned buffer until the in-flight read CQE
    /// lands even if the task is dropped — so `pb` returns to the pool on cancel,
    /// never a freed buffer the kernel is still writing into. This is the same
    /// rule as the UCX recv path, just via compio's owned-buffer semantics
    /// instead of UCX's `InflightSlot` drain.
    ///
    /// UCX errors (callers gate on `!is_ucx()` and use `recv_into` there).
    pub async fn read_exact_into_pooled(
        &mut self,
        pb: crate::PooledBuf,
        filled: usize,
        target: usize,
    ) -> io::Result<crate::PooledBuf> {
        debug_assert!(filled <= target && target <= pb.cap());
        if self.is_ucx() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "read_exact_into_pooled is TCP-only; UCX uses recv_into",
            ));
        }
        if filled >= target {
            return Ok(pb);
        }
        use compio::buf::{IntoInner, IoBuf};
        use compio::io::AsyncReadExt;
        // `read_exact` fills the slice's `buf_capacity()` (= target - filled)
        // bytes, writing into the underlying pool memory at `[filled..target]`.
        let slice = pb.slice(filled..target);
        let compio::BufResult(r, slice) = self.read_exact(slice).await;
        r?;
        Ok(slice.into_inner())
    }

}

impl compio::io::AsyncWrite for WriteHalf {
    async fn write<B: compio::buf::IoBuf>(&mut self, buf: B) -> compio::BufResult<usize, B> {
        match self {
            WriteHalf::Tcp(w) => w.write(buf).await,
            #[cfg(feature = "ucx")]
            WriteHalf::Ucx(w) => w.write(buf).await,
        }
    }
    /// Same as `Conn::write_vectored` — forward to inner; default trait impl
    /// loops + ~2× syscalls.
    async fn write_vectored<T: compio::buf::IoVectoredBuf>(
        &mut self,
        buf: T,
    ) -> compio::BufResult<usize, T> {
        match self {
            WriteHalf::Tcp(w) => w.write_vectored(buf).await,
            #[cfg(feature = "ucx")]
            WriteHalf::Ucx(w) => w.write_vectored(buf).await,
        }
    }
    async fn flush(&mut self) -> io::Result<()> {
        match self {
            WriteHalf::Tcp(w) => w.flush().await,
            #[cfg(feature = "ucx")]
            WriteHalf::Ucx(w) => w.flush().await,
        }
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        match self {
            WriteHalf::Tcp(w) => w.shutdown().await,
            #[cfg(feature = "ucx")]
            WriteHalf::Ucx(w) => w.shutdown().await,
        }
    }
}
