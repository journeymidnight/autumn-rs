//! `autumn-ioring-daemon` — standalone daemon that exposes an autumn-rs
//! cluster as a shared-memory io_uring service.
//!
//! Architecture (F180-B6 scope):
//!
//! ```text
//!   client                                    daemon (this binary)
//!   ──────                                    ──────────────────
//!   connect AF_UNIX(socket_path[.idx])        accept (per-runtime)
//!   send HelloRequest                         recv → negotiate
//!                                             memfd_create + ftruncate
//!                                             mmap PROT_RW; init RingHeader
//!                                             send HelloResponse + fd
//!   recv → mmap fd                            spawn poller task
//!                                             loop: pop SQE → spawn task
//!                                                       (OPEN/READ/CLOSE)
//!                                                  → push CQE
//!   submit SQE_OPEN("dir/model.bin")          dispatch (F248): walk fuse path
//!     wait CQE                                  → inode → load extent map;
//!                                              cache on ring_fd; reply CQE(fd)
//!   submit SQE_READ(ring_fd, off, len, buf)   dispatch (F248): fan out across
//!     wait CQE                                  the covering F247 extents'
//!                                                sub-ranges via get_many_into,
//!                                                recv/RDMA (F243) into the ring
//!                                                slot directly → CQE(n)
//! ```
//!
//! Multi-runtime mode (`--runtimes N` > 1):
//! - N OS threads, each runs its own compio runtime.
//! - Each runtime owns: 1 `ClusterClient`, 1 `UnixListener` on
//!   `{socket}.{idx}`, the session pool for that listener.
//! - Sessions are pinned to whichever runtime accepted them; no cross-
//!   thread state. Clients distribute load by picking their runtime
//!   index (e.g. `tid % N`) when connecting.
//!
//! Multi-runtime mode breaks past the single-core ~150 k ceiling that a single
//! runtime hits on threads=1 d=32 / 16-thread aggregate.
//!
//! F248: a ring_fd maps to a FUSE FILE (inode + F247 variable-length extents),
//! not a flat KV key. OPEN walks the fuse path → inode and loads the extent map
//! (cached on the ring_fd); READ passes `sqe.offset/length` through and fans out
//! across only the overlapping extents via `get_many_into` (= the client
//! `fan_out`), so a large-model-file read moves only the bytes asked for instead
//! of pulling the whole value per SQE. The F244-C `FuturesUnordered` SQ/CQ loop
//! gives a second fan-out level across concurrent SQEs. (Pre-F248 cached one PS
//! `RpcClient` per ring_fd for a flat-key whole-value fetch.)

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use anyhow::{Context, Result};
use autumn_client::ClusterClient;
use autumn_rpc::RegisteredMem;
use clap::Parser;

use autumn_ioring::cqe::Cqe;
use autumn_ioring::handshake::{self, DaemonLimits, HelloStatus};
use autumn_ioring::header::{RingHeader, HEADER_SIZE};
use autumn_ioring::lease::{self, AcquireResult, DaemonClientId, HeartbeatResult};
use autumn_ioring::mmap::{prot, MmapRegion};
use autumn_ioring::opcode::Opcode;
use autumn_ioring::ring::{CqProducer, SqConsumer};
use autumn_ioring::socket;
use autumn_ioring::sqe::{
    Sqe, SQE_LEASE_MODE_READ, SQE_LEASE_MODE_UNSET, SQE_LEASE_MODE_WRITE,
};
use autumn_rpc::manager_rpc::{
    LEASE_INVAL_LEASE_REVOKED, LEASE_INVAL_META_CHANGED, LEASE_INVAL_WRITER_CLOSED,
    LEASE_MODE_READ, LEASE_MODE_WRITE,
};

#[derive(Parser, Debug, Clone)]
#[command(
    name = "autumn-ioring-daemon",
    about = "SHM io_uring daemon for autumn-rs (F180-B6)"
)]
struct Args {
    /// Manager address (comma-separated for HA).
    #[arg(long, default_value = "127.0.0.1:9001")]
    manager: String,

    /// Unix domain socket path the daemon listens on. Clients connect
    /// here for the handshake. With `--runtimes > 1` the path is
    /// suffixed `.0`, `.1`, … per runtime.
    #[arg(long, default_value = "/run/autumn-ioring/ring.sock")]
    socket: PathBuf,

    /// Number of independent compio runtimes (= OS threads + listeners).
    /// Default 1 keeps backward compatibility. Set to N to break past
    /// the single-core ~150 k ops/s daemon ceiling on hot 4 K-read
    /// workloads. Clients distribute themselves across the N sockets.
    #[arg(long, default_value_t = 1)]
    runtimes: usize,

    /// Idle backoff between SQ polls when the queue is empty.
    /// Microseconds. Default 100 µs.
    #[arg(long, default_value_t = 100)]
    idle_poll_us: u64,

    /// Transport backend: `tcp` (default) or `ucx`. F243: with `ucx` the daemon
    /// registers each ring region (`ucp_mem_map`) so ≥64 KiB extent reads land
    /// in the ring via RDMA zero-copy. Requires a binary built `--features ucx`.
    #[arg(long, default_value = "tcp")]
    transport: String,
}

/// Compute the per-runtime socket path. With N=1 returns `base`
/// unchanged (legacy behaviour). With N>1 appends `.{idx}` so each
/// runtime gets a distinct path.
pub fn runtime_socket_path(base: &std::path::Path, idx: usize, n: usize) -> PathBuf {
    if n <= 1 {
        return base.to_path_buf();
    }
    let mut s = base.as_os_str().to_owned();
    s.push(format!(".{idx}"));
    PathBuf::from(s)
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();

    // F243: select the transport once, process-wide, BEFORE spawning runtimes
    // (the ClusterClient + ring registration use the process-global transport).
    let tk = autumn_transport::parse_transport_flag(&args.transport).unwrap_or_else(|bad| {
        eprintln!("--transport must be `tcp` or `ucx`, got {bad:?}");
        std::process::exit(2);
    });
    autumn_transport::init_with(tk);

    if let Some(parent) = args.socket.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).ok();
        }
    }

    let n = args.runtimes.max(1);
    tracing::info!(runtimes = n, socket = %args.socket.display(), "starting daemon");

    let mut handles = Vec::with_capacity(n);
    for idx in 0..n {
        let args_c = args.clone();
        let path = runtime_socket_path(&args_c.socket, idx, n);
        // Remove stale socket file before bind.
        let _ = std::fs::remove_file(&path);
        let h = std::thread::Builder::new()
            .name(format!("ioring-rt-{idx}"))
            .spawn(move || -> Result<()> {
                compio::runtime::Runtime::new()
                    .context("create compio runtime")?
                    .block_on(async move { run_runtime(idx, path, args_c).await })
            })
            .with_context(|| format!("spawn runtime {idx}"))?;
        handles.push(h);
    }

    // Single-runtime mode: just join the only thread and propagate its
    // error. Multi-runtime mode: join all and surface the first error.
    let mut first_err: Option<anyhow::Error> = None;
    for h in handles {
        match h.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!(error = %e, "runtime exited with error");
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(_) => {
                tracing::error!("runtime thread panicked");
                if first_err.is_none() {
                    first_err = Some(anyhow::anyhow!("runtime thread panicked"));
                }
            }
        }
    }
    if let Some(e) = first_err {
        return Err(e);
    }
    Ok(())
}

async fn run_runtime(idx: usize, socket: PathBuf, args: Args) -> Result<()> {
    tracing::info!(
        idx,
        manager = %args.manager,
        socket = %socket.display(),
        "runtime initialising"
    );
    let cluster = ClusterClient::connect(&args.manager)
        .await
        .with_context(|| format!("runtime {idx}: connect ClusterClient"))?;
    let cluster = Rc::new(cluster);

    let listener = compio::net::UnixListener::bind(&socket)
        .await
        .with_context(|| format!("runtime {idx}: bind {}", socket.display()))?;
    tracing::info!(idx, "listening");

    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!(idx, error = %e, "accept failed, retrying");
                compio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let cluster_c = cluster.clone();
        let idle_us = args.idle_poll_us;
        compio::runtime::spawn(async move {
            if let Err(e) = handle_session(stream, cluster_c, idle_us).await {
                tracing::warn!(error = %e, "session ended with error");
            }
        })
        .detach();
    }
}

/// Convert a compio `UnixStream` into a raw fd usable by the
/// blocking-style libc::sendmsg/recvmsg helpers in autumn_ioring::socket.
fn unix_stream_raw_fd(s: &compio::net::UnixStream) -> std::os::unix::io::RawFd {
    s.as_raw_fd()
}

async fn handle_session(
    stream: compio::net::UnixStream,
    cluster: Rc<ClusterClient>,
    idle_us: u64,
) -> Result<()> {
    let fd = unix_stream_raw_fd(&stream);

    // Handshake (synchronous; small fixed-size messages).
    let req = socket::recv_request(fd).context("recv HelloRequest")?;
    let session_id: u64 = rand_session_id();
    let resp = handshake::negotiate(&req, &DaemonLimits::defaults(), session_id);
    if resp.status != HelloStatus::Ok {
        let _ = socket::send_response_with_fd(fd, &resp, fd);
        anyhow::bail!("handshake rejected: {:?}", resp.status);
    }

    let mut header = RingHeader::new(session_id);
    header.sq_entries = resp.sq_entries;
    header.cq_entries = resp.cq_entries;
    header.buf_pool_size = resp.buf_pool_size;
    header.buf_slot_size = resp.buf_slot_size;
    header.buf_pool_offset = header.cq_array_offset()
        + (header.cq_entries as u64) * (autumn_ioring::cqe::CQE_SIZE as u64);

    let shm_size = header.total_size();
    let memfd = socket::create_memfd(&format!("autumn-ioring-{:016x}", session_id), shm_size)
        .context("create memfd")?;

    let mut region =
        MmapRegion::map(&memfd, shm_size as usize, prot::READ_WRITE).context("mmap memfd")?;
    {
        let mut hbuf = [0u8; HEADER_SIZE as usize];
        header.encode(&mut hbuf);
        region.as_mut_slice()[..HEADER_SIZE as usize].copy_from_slice(&hbuf);
    }

    socket::send_response_with_fd(fd, &resp, memfd.as_raw_fd())
        .context("send HelloResponse + fd")?;

    // F-ioring-lease-2: per-session daemon client identity. Each
    // session gets a fresh UUID so multiple sessions on the same
    // runtime cannot share a lease (a "second writer in the same
    // session" must still conflict). `host` is diagnostic only.
    let host = format!(
        "{}#{:016x}",
        std::env::var("HOSTNAME").unwrap_or_else(|_| "ioring".to_string()),
        session_id,
    );
    let client_id = Rc::new(DaemonClientId::new(host));

    tracing::info!(
        session_id = session_id,
        sq = resp.sq_entries,
        cq = resp.cq_entries,
        buf_pool_mib = resp.buf_pool_size / (1024 * 1024),
        "session established"
    );

    poller_loop(region, header, cluster, idle_us, stream, memfd, client_id).await
}

/// Per ring_fd, cached state set up at OPEN. The cached PS RpcClient
/// lets READs skip the entire `ClusterClient::get_range` shell —
/// `resolve_key` + `get_ps_client` already happened at OPEN, and the
/// `call_ps_for_key` retry closure adds nothing for a stable cluster.
// F248: a ring_fd maps to an opened FUSE FILE (inode + F247 variable-length
// extent map), resolved once at Open. Was a single flat KV key + cached PS.
// F242 adds the write side (`fuse_write`) — same module for daemon-only,
// ungated key+schema code path.
use autumn_ioring::fuse_read::{self, OpenedExtents};
use autumn_ioring::fuse_write;

/// F243: register the ring region with the process-global UCX context so reads
/// can RDMA-land directly into it. Returns `None` on a non-UCX build or if
/// registration fails (e.g. RLIMIT_MEMLOCK) — the daemon then falls back to the
/// transport's recv-into-dest (TCP) with no extra copy beyond the kernel one.
#[cfg(feature = "ucx")]
fn register_ring(base: *mut u8, len: usize) -> Option<RegisteredMem> {
    match autumn_transport::register_memory(base as *mut std::ffi::c_void, len) {
        Ok(r) => {
            tracing::info!(
                len,
                "F243: ring region registered for UCX RDMA zero-copy reads"
            );
            Some(r)
        }
        Err(e) => {
            tracing::warn!(error = %e, "F243: ring UCX registration failed; falling back to copy-recv");
            None
        }
    }
}

#[cfg(not(feature = "ucx"))]
fn register_ring(_base: *mut u8, _len: usize) -> Option<RegisteredMem> {
    None
}

async fn poller_loop(
    region: MmapRegion,
    header: RingHeader,
    cluster: Rc<ClusterClient>,
    idle_us: u64,
    _stream: compio::net::UnixStream,
    _memfd: std::os::unix::io::OwnedFd,
    client_id: Rc<DaemonClientId>,
) -> Result<()> {
    let region: Rc<RefCell<MmapRegion>> = Rc::new(RefCell::new(region));
    // F243: capture the mmap's STABLE base ptr + register the region for UCX. The
    // base never moves (mmap is fixed for the region's life, kept alive by the
    // `region` Rc), so Read can build a raw `&mut` into a buffer-pool slot — held
    // across the get await — WITHOUT taking the RefCell (which serves only the
    // SQ/CQ ring header). Slot disjointness is guaranteed by `validate_slice`
    // (slot-aligned + length ≤ slot_size) + the client owning slot allocation.
    let (data_base, region_len) = {
        let mut r = region.borrow_mut();
        let len = r.len();
        (r.as_mut_slice().as_mut_ptr(), len)
    };
    let ring_reg = register_ring(data_base, region_len);
    let reg_ref = ring_reg.as_ref();
    let ring_fds: Rc<RefCell<HashMap<u32, OpenedExtents>>> = Rc::new(RefCell::new(HashMap::new()));
    let next_fd: Rc<Cell<u32>> = Rc::new(Cell::new(1));
    let backoff = Duration::from_micros(idle_us);

    // F-ioring-lease-2: per-session lease bookkeeping. `held_leases`
    // is INODE-keyed (one entry per ino currently leased by this
    // session, with the mode + refcount of open ring_fds backing it).
    // Multiple Open calls in the same session targeting the same
    // inode are refcounted so we don't AcquireLease again (the
    // manager would grant idempotently to the same client, but then
    // a single Close would bump version while the other ring_fd is
    // still in use — corruption). Release fires only on the 1→0
    // transition.
    let held_leases: Rc<RefCell<HashMap<u64, SessionLease>>> =
        Rc::new(RefCell::new(HashMap::new()));

    // Spawn a per-session heartbeat task — keeps held leases alive
    // past their TTL (30 s default; we tick at TTL/6 = 5 s). On
    // HeartbeatResult::NotHeld we drop the held entry + every
    // ring_fd that backed it (subsequent reads/writes get EBADF —
    // the cache was invalidated externally, e.g. the writer was
    // revoked by the manager).
    {
        let cluster_h = cluster.clone();
        let client_h = client_id.clone();
        let held_h = held_leases.clone();
        let ring_fds_h = ring_fds.clone();
        compio::runtime::spawn(async move {
            session_heartbeat_loop(cluster_h, client_h, held_h, ring_fds_h).await
        })
        .detach();
    }

    // F-ioring-lease-3: spawn a per-session invalidation poll loop.
    // Drains MgrInvalidation events the manager pushes when other
    // daemons' writers close or have their leases revoked. The loop
    // is a tight call/await because the manager-side handler
    // long-polls (LONG_POLL_WAIT = 10 s) — we burn at most one
    // round-trip per 10 s when idle. On transport error or overflow
    // sentinel we drop every cached ring_fd + held lease
    // (plan §6.4 "subscribe disconnect = invalidate everything")
    // so a connection blip never leaves us serving stale state.
    {
        let cluster_i = cluster.clone();
        let client_i = client_id.clone();
        let held_i = held_leases.clone();
        let ring_fds_i = ring_fds.clone();
        compio::runtime::spawn(async move {
            session_invalidation_poll_loop(cluster_i, client_i, held_i, ring_fds_i).await
        })
        .detach();
    }

    // F244-C: bounded streaming SQ/CQ loop — mirrors the EN/PS
    // `handle_connection` shape (FuturesUnordered + drain-as-they-land), the
    // server-side form of the client's `fan_out` streaming primitive. A
    // persistent `inflight` holds the in-flight `service_sqe` futures (bounded by
    // the ring's SQ depth); each completion's CQE is pushed AS IT LANDS — no
    // batch-boundary stall and no unbounded per-SQE `spawn`.
    use futures::future::FutureExt;
    use futures::stream::{FuturesUnordered, StreamExt};
    let mut inflight = FuturesUnordered::new();
    loop {
        // SQ side: pop whatever the ring offers, launch one service future each.
        let mut sqes: Vec<Sqe> = Vec::new();
        {
            let r = region.borrow();
            let cons = SqConsumer::new(r.as_slice(), header);
            cons.try_pop_batch(&mut sqes, 32);
        }
        for sqe in sqes {
            let region_c = region.clone();
            let cluster_c = cluster.clone();
            let ring_fds_c = ring_fds.clone();
            let next_fd_c = next_fd.clone();
            let client_c = client_id.clone();
            let held_c = held_leases.clone();
            // `data_base` (Copy raw ptr) + `reg_ref` (Copy ref into `ring_reg`,
            // which outlives `inflight`) are captured by the future; the Read arm
            // builds its dest slice from them.
            inflight.push(async move {
                service_sqe(
                    sqe,
                    &region_c,
                    &header,
                    &cluster_c,
                    &ring_fds_c,
                    &next_fd_c,
                    data_base,
                    reg_ref,
                    &client_c,
                    &held_c,
                )
                .await
            });
        }
        // CQ side: idle → back off; else await ONE completion (this polls all
        // in-flight futures + yields the runtime) then drain any others ready.
        if inflight.is_empty() {
            compio::time::sleep(backoff).await;
        } else {
            if let Some(cqe) = inflight.next().await {
                push_cqe(&region, header, cqe);
            }
            while let Some(cqe) = inflight.next().now_or_never().flatten() {
                push_cqe(&region, header, cqe);
            }
        }
    }
}

/// Push one completion onto the CQ ring (brief `borrow_mut`, no await held).
fn push_cqe(region: &Rc<RefCell<MmapRegion>>, header: RingHeader, cqe: Cqe) {
    let mut r = region.borrow_mut();
    let mut prod = CqProducer::new(r.as_mut_slice(), header);
    if prod.try_push(cqe).is_err() {
        tracing::warn!("CQ full; dropping completion");
    }
}

#[allow(clippy::too_many_arguments)]
async fn service_sqe(
    sqe: Sqe,
    region: &Rc<RefCell<MmapRegion>>,
    header: &RingHeader,
    cluster: &ClusterClient,
    ring_fds: &Rc<RefCell<HashMap<u32, OpenedExtents>>>,
    next_fd: &Rc<Cell<u32>>,
    // F243: stable mmap base + (optional) UCX registration for ZC-into-ring reads.
    data_base: *mut u8,
    reg: Option<&RegisteredMem>,
    // F-ioring-lease-2: per-session daemon identity + per-inode lease refcount.
    client_id: &Rc<DaemonClientId>,
    held_leases: &Rc<RefCell<HashMap<u64, SessionLease>>>,
) -> Cqe {
    match sqe.opcode {
        Opcode::Nop => Cqe::ok(sqe.user_data, 0),

        Opcode::Open => {
            let layout = autumn_ioring::buffer_pool::BufferPoolLayout::from_header(header);
            if layout.validate_slice(sqe.buf_offset, sqe.length).is_err() {
                return Cqe::err(sqe.user_data, libc::EINVAL);
            }
            let path = {
                let r = region.borrow();
                r.as_slice()[sqe.buf_offset as usize..sqe.buf_offset as usize + sqe.length as usize]
                    .to_vec()
            };
            // F248: resolve the FUSE PATH → inode + variable-length extent map
            // (was: treat `path` as a flat KV key + cache one PS). Reading the
            // actual chunked file the fuse mount writes; per-extent routing now
            // happens inside `get_many_into` on each Read.
            let opened = match fuse_read::open(cluster, &path).await {
                Ok(o) => o,
                Err(e) => {
                    let msg = e.to_string();
                    let errno = if msg.contains("ENOENT") {
                        libc::ENOENT
                    } else {
                        libc::EIO
                    };
                    return Cqe::err(sqe.user_data, errno);
                }
            };
            // F-ioring-lease-2: AcquireLease BEFORE publishing the
            // ring_fd. The lease mode is encoded in the SQE flags
            // byte; `SQE_LEASE_MODE_UNSET` (legacy v1 clients)
            // defaults to WRITE — the safe upper bound that never
            // silently downgrades a writer to a read-only session.
            let req_mode = match sqe.lease_mode {
                SQE_LEASE_MODE_READ => LEASE_MODE_READ,
                SQE_LEASE_MODE_WRITE | SQE_LEASE_MODE_UNSET => LEASE_MODE_WRITE,
                other => {
                    tracing::warn!(other, "unknown SQE lease_mode; rejecting Open");
                    return Cqe::err(sqe.user_data, libc::EINVAL);
                }
            };
            let ino = opened.ino;
            // Refcount path: if we already hold this inode's lease
            // in this session, just bump the count. The mode is
            // pinned to the FIRST opener's choice (a READ-then-
            // WRITE within the same session would otherwise
            // require us to upgrade by re-Acquiring as WRITE, which
            // we deliberately do NOT support — second Open returns
            // EBUSY so the client sees a clear mismatch instead of
            // silently sharing a read-only lease).
            let needs_acquire = {
                let mut m = held_leases.borrow_mut();
                if let Some(slot) = m.get_mut(&ino) {
                    if slot.mode != req_mode {
                        tracing::warn!(
                            ino,
                            existing_mode = slot.mode,
                            new_mode = req_mode,
                            "Open mode mismatch against existing per-session lease"
                        );
                        return Cqe::err(sqe.user_data, libc::EBUSY);
                    }
                    slot.refcount = slot.refcount.saturating_add(1);
                    false
                } else {
                    // Insert a placeholder so a concurrent Open on
                    // the same ino in this session blocks behind us
                    // — service_sqe futures run on the same compio
                    // runtime, so the next Open's `borrow_mut` here
                    // sees `refcount > 0` and joins us. (compio
                    // tasks are cooperatively single-threaded; no
                    // cross-thread race.)
                    m.insert(
                        ino,
                        SessionLease {
                            mode: req_mode,
                            refcount: 1,
                            version: 0,
                        },
                    );
                    true
                }
            };
            if needs_acquire {
                match lease::acquire(cluster, client_id, ino, req_mode).await {
                    Ok(AcquireResult::Granted(info)) => {
                        if let Some(slot) = held_leases.borrow_mut().get_mut(&ino) {
                            slot.version = info.version;
                        }
                    }
                    Ok(AcquireResult::Conflict { manager_message }) => {
                        held_leases.borrow_mut().remove(&ino);
                        tracing::info!(
                            ino,
                            mgr = %manager_message,
                            "lease conflict; returning EBUSY"
                        );
                        return Cqe::err(sqe.user_data, libc::EBUSY);
                    }
                    Err(e) => {
                        held_leases.borrow_mut().remove(&ino);
                        tracing::warn!(ino, error = %e, "AcquireLease failed; EIO");
                        return Cqe::err(sqe.user_data, libc::EIO);
                    }
                }
            }
            let fd = next_fd.get();
            next_fd.set(fd.checked_add(1).unwrap_or(1));
            ring_fds.borrow_mut().insert(fd, opened);
            Cqe::ok(sqe.user_data, fd as u64)
        }

        Opcode::Read => {
            let layout = autumn_ioring::buffer_pool::BufferPoolLayout::from_header(header);
            if layout.validate_slice(sqe.buf_offset, sqe.length).is_err() {
                return Cqe::err(sqe.user_data, libc::EINVAL);
            }
            // F248: snapshot the opened file's extent map under a brief borrow,
            // then (F243) fan out across the OVERLAPPING extents' exact sub-ranges
            // DIRECTLY into the ring buffer slot — no whole-value amplification,
            // no intermediate buffer + copy. On UCX (reg=Some) ≥64 KiB extents
            // RDMA-land in the ring; on TCP the transport recvs into it.
            let opened = {
                let fds = ring_fds.borrow();
                match fds.get(&sqe.ring_fd) {
                    Some(o) => OpenedExtents {
                        ino: o.ino,
                        size: o.size,
                        extents: o.extents.clone(),
                    },
                    None => return Cqe::err(sqe.user_data, libc::EBADF),
                }
            };
            // SAFETY: `validate_slice` (above) guarantees the slot is in-bounds,
            // slot-aligned, and length ≤ slot_size → contained in ONE buffer-pool
            // slot. The client owns slot allocation and never reuses a slot with a
            // Read in flight, so this `&mut` is disjoint from every other in-flight
            // Read's. `data_base` is the stable mmap base (kept alive by the
            // `region` Rc); the SQ/CQ ring header is a disjoint area accessed only
            // via the RefCell, so this raw write never aliases it.
            let dest: &mut [u8] = unsafe {
                std::slice::from_raw_parts_mut(
                    data_base.add(sqe.buf_offset as usize),
                    sqe.length as usize,
                )
            };
            let n = match fuse_read::read_into(cluster, &opened, sqe.offset, sqe.length, dest, reg)
                .await
            {
                Ok(n) => n,
                Err(_) => return Cqe::err(sqe.user_data, libc::EIO),
            };
            Cqe::ok(sqe.user_data, n as u64)
        }

        Opcode::Write => {
            // F242 — write SQE: ring slot at `buf_offset[..length]` is the
            // source, `sqe.offset` is the file offset. Same layout/safety
            // story as Read (validate_slice + single-slot disjointness +
            // stable mmap base + client-owned slot allocation).
            let layout = autumn_ioring::buffer_pool::BufferPoolLayout::from_header(header);
            if layout.validate_slice(sqe.buf_offset, sqe.length).is_err() {
                return Cqe::err(sqe.user_data, libc::EINVAL);
            }
            // Snapshot the opened file's mutable state (extents + size) under a
            // brief borrow; apply updates back under another brief borrow after
            // the writes finish. Concurrent writes on the same ring_fd would
            // race here (POSIX-style: no per-fd concurrency guarantee); the
            // model-file write-once workload is single-writer per fd.
            let mut opened = {
                let fds = ring_fds.borrow();
                match fds.get(&sqe.ring_fd) {
                    Some(o) => OpenedExtents {
                        ino: o.ino,
                        size: o.size,
                        extents: o.extents.clone(),
                    },
                    None => return Cqe::err(sqe.user_data, libc::EBADF),
                }
            };
            // SAFETY: same argument as Read. validate_slice keeps the slice
            // inside one buffer-pool slot; the client owns slot allocation and
            // doesn't reuse an in-flight slot's region, so this `&[u8]` is
            // disjoint from every other in-flight SQE's slot AND from the SQ/CQ
            // ring header (a separate area accessed only via the RefCell).
            // We copy out of the slot inside `write_into` (one memcpy into a
            // heap `Bytes`) before any await that could see the client free the
            // slot, so the borrow doesn't have to outlive the await chain.
            let src: &[u8] = unsafe {
                std::slice::from_raw_parts(
                    data_base.add(sqe.buf_offset as usize),
                    sqe.length as usize,
                )
            };
            let n = match fuse_write::write_into(cluster, &mut opened, sqe.offset, src).await {
                Ok(n) => n,
                Err(_) => return Cqe::err(sqe.user_data, libc::EIO),
            };
            // Commit the updated extent map + size back to the cached state.
            if let Some(o) = ring_fds.borrow_mut().get_mut(&sqe.ring_fd) {
                o.size = opened.size;
                o.extents = opened.extents;
            }
            Cqe::ok(sqe.user_data, n as u64)
        }

        Opcode::Close => {
            let opened = match ring_fds.borrow_mut().remove(&sqe.ring_fd) {
                Some(o) => o,
                None => return Cqe::err(sqe.user_data, libc::EBADF),
            };
            // F-ioring-lease-2: refcount the per-session lease;
            // ReleaseLease fires only when the LAST ring_fd backing
            // this inode closes (so a second Open in the same
            // session keeps the lease alive).
            let release_now = {
                let mut m = held_leases.borrow_mut();
                match m.get_mut(&opened.ino) {
                    Some(slot) => {
                        slot.refcount = slot.refcount.saturating_sub(1);
                        if slot.refcount == 0 {
                            m.remove(&opened.ino);
                            true
                        } else {
                            false
                        }
                    }
                    None => false,
                }
            };
            if release_now {
                if let Err(e) = lease::release(cluster, client_id, opened.ino).await {
                    // Per plan §4.3: writer's release happens AFTER
                    // flush — by this point the cluster has the new
                    // bytes. A failed release means the manager's
                    // TTL revoke loop will eventually reclaim the
                    // lease, so the worst case is a 30s "writer
                    // present" window for new readers; no data
                    // corruption.
                    tracing::warn!(
                        ino = opened.ino,
                        error = %e,
                        "ReleaseLease failed; manager TTL will revoke"
                    );
                }
            }
            Cqe::ok(sqe.user_data, 0)
        }
    }
}

/// Per-inode state inside a session's `held_leases` map.
#[derive(Clone, Debug)]
struct SessionLease {
    /// `LEASE_MODE_READ` or `LEASE_MODE_WRITE` — pinned at first Open.
    mode: u8,
    /// Number of currently-open ring_fds backing this inode in this
    /// session. ReleaseLease fires on the 1→0 transition.
    refcount: u32,
    /// Latest `version` the manager handed back on Acquire / Heartbeat.
    /// F-ioring-lease-4 will use this to tag the per-fd OpenedExtents
    /// cache so an externally-pushed invalidation can drop stale
    /// entries.
    #[allow(dead_code)]
    version: u64,
}

/// Per-session heartbeat task. Walks every held lease every
/// `HEARTBEAT_INTERVAL` (~5s under the 30s default TTL — 6× safety
/// factor) and renews. On `HeartbeatResult::NotHeld` the entry +
/// every ring_fd backing it is dropped (subsequent reads/writes
/// surface as EBADF so the client knows to reopen — F-ioring-lease-3
/// will surface the same state via PollInvalidations).
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// F-ioring-lease-3: per-session invalidation poll loop. Persistent
/// long-poll consumer of `MSG_POLL_INVALIDATIONS`. The manager-side
/// handler waits up to `LONG_POLL_WAIT` (10 s) for an event before
/// returning empty, so this loop's effective round-trip rate when
/// idle is ~1/10s.
///
/// Event handling (F-3 scope — per-ino cache eviction lands in F-4):
/// - `LEASE_INVAL_META_CHANGED { ino: 0 }`: manager's overflow
///   sentinel — wholesale invalidate (per plan §6.4).
/// - `LEASE_INVAL_WRITER_CLOSED` / `LEASE_INVAL_LEASE_REVOKED` for a
///   specific ino: log only; F-ioring-lease-4 will drop the
///   matching `OpenedExtents` so the next Read re-resolves the
///   extent map.
///
/// On transport error (manager unreachable, connection dropped): also
/// wholesale invalidate before retrying. This honours plan §6.4 —
/// "subscribe disconnect = invalidate everything" — so a poll-loop
/// blip never leaves the session serving stale cache.
async fn session_invalidation_poll_loop(
    cluster: Rc<ClusterClient>,
    client_id: Rc<DaemonClientId>,
    held_leases: Rc<RefCell<HashMap<u64, SessionLease>>>,
    ring_fds: Rc<RefCell<HashMap<u32, OpenedExtents>>>,
) {
    loop {
        match lease::poll_invalidations(&cluster, &client_id).await {
            Ok(events) => {
                let mut wholesale = false;
                for ev in &events {
                    match ev.kind {
                        LEASE_INVAL_META_CHANGED if ev.ino == 0 => {
                            wholesale = true;
                            tracing::warn!(
                                "F-ioring-lease-3: overflow sentinel; invalidating session cache"
                            );
                        }
                        LEASE_INVAL_WRITER_CLOSED => {
                            tracing::info!(
                                ino = ev.ino,
                                version = ev.version,
                                "invalidation: writer closed (F-4 will drop the cached entry)"
                            );
                        }
                        LEASE_INVAL_LEASE_REVOKED => {
                            tracing::info!(
                                ino = ev.ino,
                                version = ev.version,
                                "invalidation: lease revoked"
                            );
                        }
                        LEASE_INVAL_META_CHANGED => {
                            tracing::info!(
                                ino = ev.ino,
                                version = ev.version,
                                "invalidation: meta changed"
                            );
                        }
                        other => {
                            tracing::warn!(kind = other, ino = ev.ino, "unknown invalidation kind");
                        }
                    }
                }
                if wholesale {
                    let drained: Vec<u64> =
                        held_leases.borrow().keys().copied().collect();
                    held_leases.borrow_mut().clear();
                    ring_fds.borrow_mut().clear();
                    // Best-effort: tell the manager we're letting go
                    // of every held lease so its TTL revoke loop
                    // doesn't sit on them until expiry. Errors here
                    // are non-fatal (TTL backstop covers it).
                    for ino in drained {
                        if let Err(e) = lease::release(&cluster, &client_id, ino).await {
                            tracing::warn!(ino, error = %e, "best-effort release after overflow");
                        }
                    }
                }
            }
            Err(e) => {
                // Plan §6.4: subscribe-disconnect = invalidate every
                // cached inode. Drop everything, sleep briefly, and
                // re-issue the poll (manager auto-reconnect via
                // ClusterClient::mgr_call_retry covers reconnection).
                tracing::warn!(
                    error = %e,
                    "F-ioring-lease-3: poll failed; invalidating session cache + retrying"
                );
                held_leases.borrow_mut().clear();
                ring_fds.borrow_mut().clear();
                compio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

async fn session_heartbeat_loop(
    cluster: Rc<ClusterClient>,
    client_id: Rc<DaemonClientId>,
    held_leases: Rc<RefCell<HashMap<u64, SessionLease>>>,
    ring_fds: Rc<RefCell<HashMap<u32, OpenedExtents>>>,
) {
    loop {
        compio::time::sleep(HEARTBEAT_INTERVAL).await;
        // Snapshot the inode set under a brief borrow so we don't
        // hold the RefCell across the await.
        let inos: Vec<u64> = held_leases.borrow().keys().copied().collect();
        if inos.is_empty() {
            continue;
        }
        for ino in inos {
            match lease::heartbeat(&cluster, &client_id, ino).await {
                Ok(HeartbeatResult::Renewed(info)) => {
                    if let Some(slot) = held_leases.borrow_mut().get_mut(&ino) {
                        slot.version = info.version;
                    }
                }
                Ok(HeartbeatResult::NotHeld) => {
                    tracing::warn!(
                        ino,
                        "heartbeat: lease was revoked externally; invalidating session ring_fds"
                    );
                    held_leases.borrow_mut().remove(&ino);
                    // Evict every ring_fd backed by this inode so
                    // subsequent ops fail clearly (EBADF) instead of
                    // silently reading stale state.
                    ring_fds.borrow_mut().retain(|_, o| o.ino != ino);
                }
                Err(e) => {
                    // Transport / NotLeader is transient — leave the
                    // entry in place and try again on the next tick.
                    tracing::warn!(ino, error = %e, "heartbeat transient failure");
                }
            }
        }
    }
}

fn rand_session_id() -> u64 {
    let mut buf = [0u8; 8];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        if std::io::Read::read_exact(&mut f, &mut buf).is_ok() {
            return u64::from_le_bytes(buf);
        }
    }
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0xdeadbeef)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socket_path_n1_unchanged() {
        let base = PathBuf::from("/run/autumn-ioring/ring.sock");
        assert_eq!(runtime_socket_path(&base, 0, 1), base);
    }

    #[test]
    fn socket_path_multi_appends_idx() {
        let base = PathBuf::from("/run/autumn-ioring/ring.sock");
        assert_eq!(
            runtime_socket_path(&base, 0, 4),
            PathBuf::from("/run/autumn-ioring/ring.sock.0")
        );
        assert_eq!(
            runtime_socket_path(&base, 3, 4),
            PathBuf::from("/run/autumn-ioring/ring.sock.3")
        );
    }
}
