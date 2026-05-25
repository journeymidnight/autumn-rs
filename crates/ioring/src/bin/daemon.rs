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
use autumn_ioring::mmap::{prot, MmapRegion};
use autumn_ioring::opcode::Opcode;
use autumn_ioring::ring::{CqProducer, SqConsumer};
use autumn_ioring::socket;
use autumn_ioring::sqe::Sqe;

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
    tracing::info!(
        session_id = session_id,
        sq = resp.sq_entries,
        cq = resp.cq_entries,
        buf_pool_mib = resp.buf_pool_size / (1024 * 1024),
        "session established"
    );

    poller_loop(region, header, cluster, idle_us, stream, memfd).await
}

/// Per ring_fd, cached state set up at OPEN. The cached PS RpcClient
/// lets READs skip the entire `ClusterClient::get_range` shell —
/// `resolve_key` + `get_ps_client` already happened at OPEN, and the
/// `call_ps_for_key` retry closure adds nothing for a stable cluster.
// F248: a ring_fd maps to an opened FUSE FILE (inode + F247 variable-length
// extent map), resolved once at Open. Was a single flat KV key + cached PS.
use autumn_ioring::fuse_read::{self, OpenedExtents};

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

        Opcode::Write => Cqe::err(sqe.user_data, libc::ENOSYS),

        Opcode::Close => {
            if ring_fds.borrow_mut().remove(&sqe.ring_fd).is_some() {
                Cqe::ok(sqe.user_data, 0)
            } else {
                Cqe::err(sqe.user_data, libc::EBADF)
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
