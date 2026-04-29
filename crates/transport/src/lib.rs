//! autumn-transport — pluggable transport for autumn-rs (TCP today, UCX optional).
//!
//! See `docs/superpowers/specs/2026-04-23-ucx-transport-design.md` (F100-UCX).
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

pub use probe::parse_transport_flag;
pub use tcp::TcpTransport;
#[cfg(feature = "ucx")]
pub use ucx::UcxTransport;

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
        TransportKind::Ucx => panic!(
            "binary requested --transport ucx but was built without the `ucx` feature"
        ),
    });
    let t = &**GLOBAL.get().expect("init_with");
    tracing::info!("autumn-transport: init kind={:?}", t.kind());
    t
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
        tracing::info!(
            "autumn-transport: UCX listen on {ip} via {dev} (RoCE-attached)"
        );
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

fn find_netdev_owning_ip(ip: &std::net::IpAddr) -> io::Result<String> {
    // /sys/class/net/<dev> doesn't carry IP info; walk via iproute's
    // documented pattern of reading each netdev's IPs via getifaddrs.
    // We can avoid the nix dep by parsing /proc/net/{fib_trie,if_inet6}
    // but getifaddrs via libc is simpler and we already pull libc.
    unsafe {
        let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
        if libc::getifaddrs(&mut head) != 0 {
            return Err(io::Error::last_os_error());
        }
        let mut cur = head;
        let result = loop {
            if cur.is_null() {
                break None;
            }
            let ifa = &*cur;
            if !ifa.ifa_addr.is_null() {
                let family = (*ifa.ifa_addr).sa_family as i32;
                let matched = match (ip, family) {
                    (std::net::IpAddr::V4(v4), libc::AF_INET) => {
                        let sa = ifa.ifa_addr as *const libc::sockaddr_in;
                        let raw = u32::from_be((*sa).sin_addr.s_addr);
                        raw == u32::from(*v4)
                    }
                    (std::net::IpAddr::V6(v6), libc::AF_INET6) => {
                        let sa = ifa.ifa_addr as *const libc::sockaddr_in6;
                        (*sa).sin6_addr.s6_addr == v6.octets()
                    }
                    _ => false,
                };
                if matched {
                    let name = std::ffi::CStr::from_ptr(ifa.ifa_name)
                        .to_string_lossy()
                        .into_owned();
                    break Some(name);
                }
            }
            cur = ifa.ifa_next;
        };
        libc::freeifaddrs(head);
        result.ok_or_else(|| {
            io::Error::other(format!("ip {ip} not found on any local netdev"))
        })
    }
}

fn roce_candidates() -> Vec<(String, std::net::IpAddr)> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir("/sys/class/net") else {
        return out;
    };
    for entry in entries.flatten() {
        let Ok(name) = entry.file_name().into_string() else { continue };
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
    let mut out = Vec::new();
    unsafe {
        let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
        if libc::getifaddrs(&mut head) != 0 {
            return out;
        }
        let mut cur = head;
        while !cur.is_null() {
            let ifa = &*cur;
            if !ifa.ifa_addr.is_null() {
                let name = std::ffi::CStr::from_ptr(ifa.ifa_name).to_string_lossy();
                if name == target_dev {
                    let family = (*ifa.ifa_addr).sa_family as i32;
                    if family == libc::AF_INET {
                        let sa = ifa.ifa_addr as *const libc::sockaddr_in;
                        let raw = u32::from_be((*sa).sin_addr.s_addr);
                        out.push(std::net::IpAddr::V4(std::net::Ipv4Addr::from(raw)));
                    } else if family == libc::AF_INET6 {
                        let sa = ifa.ifa_addr as *const libc::sockaddr_in6;
                        out.push(std::net::IpAddr::V6(std::net::Ipv6Addr::from(
                            (*sa).sin6_addr.s6_addr,
                        )));
                    }
                }
            }
            cur = ifa.ifa_next;
        }
        libc::freeifaddrs(head);
    }
    out
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
    async fn read<B: compio::buf::IoBufMut>(
        &mut self,
        buf: B,
    ) -> compio::BufResult<usize, B> {
        match self {
            Conn::Tcp(s) => s.read(buf).await,
            #[cfg(feature = "ucx")]
            Conn::Ucx(c) => c.read(buf).await,
        }
    }
}

impl compio::io::AsyncWrite for Conn {
    async fn write<B: compio::buf::IoBuf>(
        &mut self,
        buf: B,
    ) -> compio::BufResult<usize, B> {
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

impl compio::io::AsyncRead for ReadHalf {
    async fn read<B: compio::buf::IoBufMut>(
        &mut self,
        buf: B,
    ) -> compio::BufResult<usize, B> {
        match self {
            ReadHalf::Tcp(r) => r.read(buf).await,
            #[cfg(feature = "ucx")]
            ReadHalf::Ucx(r) => r.read(buf).await,
        }
    }
}

impl compio::io::AsyncWrite for WriteHalf {
    async fn write<B: compio::buf::IoBuf>(
        &mut self,
        buf: B,
    ) -> compio::BufResult<usize, B> {
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
