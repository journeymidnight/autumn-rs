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
//!   submit SQE_OPEN("dataset/x.bin")          dispatch: register key →
//!     wait CQE                                  resolve_key + cache PS
//!                                              ring_fd; reply CQE(fd)
//!   submit SQE_READ(ring_fd, off, len, buf)   dispatch: ps.call(MSG_GET,
//!     wait CQE                                  GetReq{part_id, key, ...})
//!                                                → memcpy resp.value
//!                                                → CQE(bytes_read)
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
//! F180-B6 changes vs B5:
//! - Multi-runtime mode breaks past the single-core ~150 k ceiling
//!   that B5 hit on threads=1 d=32 / 16-thread aggregate.
//! - At OPEN we now resolve_key + get_ps_client *once* and cache
//!   `Rc<RpcClient>` + `part_id` on the ring_fd. READ skips
//!   `ClusterClient::get` entirely, builds GetReq inline, calls the
//!   cached PS directly. Saves ~3-4 RefCell borrows + 2 hashmap
//!   lookups + the `call_ps_for_key` retry-closure shell per READ.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use anyhow::{Context, Result};
use autumn_client::{AutumnError, ClusterClient, DEFAULT_RPC_TIMEOUT};
use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc::{
    rkyv_decode, rkyv_encode, GetReq, GetResp, CODE_NOT_FOUND, CODE_OK, MSG_GET,
};
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
}

/// Compute the per-runtime socket path. With N=1 returns `base`
/// unchanged (legacy behaviour). With N>1 appends `.{idx}` so each
/// runtime gets a distinct path.
pub fn runtime_socket_path(base: &PathBuf, idx: usize, n: usize) -> PathBuf {
    if n <= 1 {
        return base.clone();
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
struct OpenedFile {
    key: Vec<u8>,
    part_id: u64,
    ps: Rc<RpcClient>,
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
    let ring_fds: Rc<RefCell<HashMap<u32, OpenedFile>>> = Rc::new(RefCell::new(HashMap::new()));
    let next_fd: Rc<Cell<u32>> = Rc::new(Cell::new(1));
    let backoff = Duration::from_micros(idle_us);

    loop {
        let mut sqes: Vec<Sqe> = Vec::new();
        {
            let r = region.borrow();
            let cons = SqConsumer::new(r.as_slice(), header);
            cons.try_pop_batch(&mut sqes, 32);
        }
        if sqes.is_empty() {
            compio::time::sleep(backoff).await;
            continue;
        }

        for sqe in sqes {
            let region_c = region.clone();
            let cluster_c = cluster.clone();
            let ring_fds_c = ring_fds.clone();
            let next_fd_c = next_fd.clone();
            compio::runtime::spawn(async move {
                let cqe =
                    service_sqe(sqe, &region_c, &header, &cluster_c, &ring_fds_c, &next_fd_c).await;
                let mut r = region_c.borrow_mut();
                let mut prod = CqProducer::new(r.as_mut_slice(), header);
                if prod.try_push(cqe).is_err() {
                    tracing::warn!("CQ full; dropping completion");
                }
            })
            .detach();
        }
    }
}

async fn service_sqe(
    sqe: Sqe,
    region: &Rc<RefCell<MmapRegion>>,
    header: &RingHeader,
    cluster: &ClusterClient,
    ring_fds: &Rc<RefCell<HashMap<u32, OpenedFile>>>,
    next_fd: &Rc<Cell<u32>>,
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
            // Existence check — same semantics as before.
            match cluster.head(&path).await {
                Ok(meta) if meta.found => {}
                Ok(_) => return Cqe::err(sqe.user_data, libc::ENOENT),
                Err(AutumnError::NotFound) => return Cqe::err(sqe.user_data, libc::ENOENT),
                Err(_) => return Cqe::err(sqe.user_data, libc::EIO),
            }
            // Resolve once — cache the PS RpcClient on the ring_fd so
            // every READ skips routing and connection-pool lookups.
            let (part_id, ps_addr) = match cluster.resolve_key(&path).await {
                Ok(p) => p,
                Err(_) => return Cqe::err(sqe.user_data, libc::EIO),
            };
            let ps = match cluster.get_ps_client(&ps_addr).await {
                Ok(c) => c,
                Err(_) => return Cqe::err(sqe.user_data, libc::EIO),
            };
            let fd = next_fd.get();
            next_fd.set(fd.checked_add(1).unwrap_or(1));
            ring_fds.borrow_mut().insert(
                fd,
                OpenedFile {
                    key: path,
                    part_id,
                    ps,
                },
            );
            Cqe::ok(sqe.user_data, fd as u64)
        }

        Opcode::Read => {
            let layout = autumn_ioring::buffer_pool::BufferPoolLayout::from_header(header);
            if layout.validate_slice(sqe.buf_offset, sqe.length).is_err() {
                return Cqe::err(sqe.user_data, libc::EINVAL);
            }
            // Snapshot key + part_id + ps under a brief borrow. The
            // Rc<RpcClient> clone is the per-call cost we cared about
            // amortising — one atomic increment vs the multi-RefCell
            // borrows + hashmap lookup of the pre-F180-B6 path.
            let (key, part_id, ps) = {
                let fds = ring_fds.borrow();
                match fds.get(&sqe.ring_fd) {
                    Some(o) => (o.key.clone(), o.part_id, o.ps.clone()),
                    None => return Cqe::err(sqe.user_data, libc::EBADF),
                }
            };
            // Pass offset=0 length=0 ("whole value") to PS — this hits
            // the cheap path in `resolve_value` (one to_vec instead of
            // two). The daemon slices [sqe.offset, sqe.offset+sqe.length)
            // from the response. For 4 KB hot reads this saves an
            // extra 4 KB memcpy per op on the PS side, which under
            // single-runtime daemon load was the difference between
            // 137 k (length=0) and 103 k (length=4096) ops/s.
            let region_epoch = cluster.lookup_epoch_for_part(part_id);
            let req = GetReq {
                part_id,
                key,
                offset: 0,
                length: 0,
                region_epoch,
            };
            let payload = rkyv_encode(&req);
            // Bounded so a paged-out / hung PS surfaces as EIO to the
            // ioring user instead of wedging this Read SQE forever.
            let resp_bytes = match ps.call_timeout(MSG_GET, payload, DEFAULT_RPC_TIMEOUT).await {
                Ok(b) => b,
                Err(_) => {
                    let _ = cluster; // unused warning suppression
                    return Cqe::err(sqe.user_data, libc::EIO);
                }
            };
            let resp: GetResp = match rkyv_decode(&resp_bytes) {
                Ok(r) => r,
                Err(_) => return Cqe::err(sqe.user_data, libc::EIO),
            };
            if resp.code == CODE_NOT_FOUND {
                return Cqe::err(sqe.user_data, libc::ENOENT);
            }
            if resp.code != CODE_OK {
                return Cqe::err(sqe.user_data, libc::EIO);
            }
            let start = sqe.offset as usize;
            if start >= resp.value.len() {
                return Cqe::ok(sqe.user_data, 0);
            }
            let end = (start + sqe.length as usize).min(resp.value.len());
            let n = end - start;
            {
                let mut r = region.borrow_mut();
                let dst =
                    &mut r.as_mut_slice()[sqe.buf_offset as usize..sqe.buf_offset as usize + n];
                dst.copy_from_slice(&resp.value[start..end]);
            }
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
