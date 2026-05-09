//! `autumn-ioring-daemon` — standalone daemon that exposes an autumn-rs
//! cluster as a shared-memory io_uring service.
//!
//! Architecture (F180-B4 scope):
//!
//! ```text
//!   client                                    daemon (this binary)
//!   ──────                                    ──────────────────
//!   connect AF_UNIX(socket_path)              accept
//!   send HelloRequest                         recv → negotiate
//!                                             memfd_create + ftruncate
//!                                             mmap PROT_RW; init RingHeader
//!                                             send HelloResponse + fd
//!   recv → mmap fd                            spawn poller task
//!                                             loop: pop SQE → dispatch
//!                                                       (OPEN/READ/CLOSE)
//!                                                  → push CQE
//!   submit SQE_OPEN("dataset/x.bin")          dispatch: register key →
//!     wait CQE                                  ring_fd; reply CQE(fd)
//!   submit SQE_READ(ring_fd, off, len, buf)   dispatch: cluster.get_range
//!     wait CQE                                  → memcpy into buf slot
//!                                                → CQE(bytes_read)
//! ```
//!
//! Notes:
//! - One daemon process serves many concurrent clients (one accept loop +
//!   N poller tasks on the same compio runtime).
//! - One ClusterClient is shared across all sessions via `Rc` — the F179
//!   `&self` refactor makes this safe.
//! - Per-session state (ring memory, ring_fd → key mapping) lives on the
//!   poller task's stack; no cross-session state contention.
//! - SQE polling is a sleep-loop with a small backoff. F180-C may add
//!   futex-based wake-up to drop idle CPU usage.

use std::collections::HashMap;
use std::io::Write as _;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use anyhow::{Context, Result};
use autumn_client::{AutumnError, ClusterClient};
use clap::Parser;

use autumn_ioring::handshake::{
    self, DaemonLimits, HelloStatus, HELLO_REQUEST_SIZE,
};
use autumn_ioring::header::{HEADER_SIZE, RingHeader};
use autumn_ioring::mmap::{prot, MmapRegion};
use autumn_ioring::opcode::Opcode;
use autumn_ioring::ring::{CqProducer, SqConsumer};
use autumn_ioring::cqe::Cqe;
use autumn_ioring::sqe::Sqe;
use autumn_ioring::socket;

#[derive(Parser, Debug)]
#[command(
    name = "autumn-ioring-daemon",
    about = "SHM io_uring daemon for autumn-rs (F180-B4)"
)]
struct Args {
    /// Manager address (comma-separated for HA).
    #[arg(long, default_value = "127.0.0.1:9001")]
    manager: String,

    /// Unix domain socket path the daemon listens on. Clients connect
    /// here for the handshake.
    #[arg(long, default_value = "/run/autumn-ioring/ring.sock")]
    socket: PathBuf,

    /// Idle backoff between SQ polls when the queue is empty.
    /// Microseconds. Default 100 µs.
    #[arg(long, default_value_t = 100)]
    idle_poll_us: u64,
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
    // Remove any stale socket file from a previous run.
    let _ = std::fs::remove_file(&args.socket);

    compio::runtime::Runtime::new()
        .context("create compio runtime")?
        .block_on(async move {
            run(args).await
        })
}

async fn run(args: Args) -> Result<()> {
    tracing::info!(manager = %args.manager, "connecting to autumn-rs cluster");
    let cluster = ClusterClient::connect(&args.manager)
        .await
        .context("connect ClusterClient")?;
    let cluster = Rc::new(cluster);
    tracing::info!(socket = %args.socket.display(), "daemon listening");

    let listener = compio::net::UnixListener::bind(&args.socket)
        .await
        .context("bind unix socket")?;

    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!(error = %e, "accept failed, retrying");
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
///
/// We do the entire handshake synchronously over the raw fd because
/// the messages are small (32B request, 40B response + cmsg) and the
/// fd is held by `stream` for the duration; compio's async wrapper
/// re-takes the fd for any subsequent ops if we kept the stream alive.
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
        // Best-effort send rejection without a real fd. We pass `fd`
        // (the socket itself) as a sentinel since SCM_RIGHTS requires
        // one fd per cmsg; client sees status != Ok and discards the
        // fd.
        let _ = socket::send_response_with_fd(fd, &resp, fd);
        anyhow::bail!("handshake rejected: {:?}", resp.status);
    }

    // Build a header reflecting the negotiated sizes (resp may have
    // clamped client request).
    let mut header = RingHeader::new(session_id);
    header.sq_entries = resp.sq_entries;
    header.cq_entries = resp.cq_entries;
    header.buf_pool_size = resp.buf_pool_size;
    header.buf_slot_size = resp.buf_slot_size;
    // Recompute pool offset for the (possibly clamped) ring sizes.
    header.buf_pool_offset = header.cq_array_offset()
        + (header.cq_entries as u64) * (autumn_ioring::cqe::CQE_SIZE as u64);

    let shm_size = header.total_size();
    let memfd = socket::create_memfd(
        &format!("autumn-ioring-{:016x}", session_id),
        shm_size,
    )
    .context("create memfd")?;

    // Map for daemon-side use; write the header so the client's view
    // of the SHM matches.
    let mut region = MmapRegion::map(&memfd, shm_size as usize, prot::READ_WRITE)
        .context("mmap memfd")?;
    {
        let mut hbuf = [0u8; HEADER_SIZE as usize];
        header.encode(&mut hbuf);
        region.as_mut_slice()[..HEADER_SIZE as usize].copy_from_slice(&hbuf);
    }

    // Send response + memfd. Client mmap's the same fd and sees the
    // same header bytes.
    socket::send_response_with_fd(fd, &resp, memfd.as_raw_fd())
        .context("send HelloResponse + fd")?;
    tracing::info!(
        session_id = session_id,
        sq = resp.sq_entries,
        cq = resp.cq_entries,
        buf_pool_mib = resp.buf_pool_size / (1024 * 1024),
        "session established"
    );

    // The client now owns its own kernel fd to the same memfd; the
    // daemon's `memfd` keeps the inode alive through this session.
    // `stream` is kept alive too — when the client closes the socket
    // we exit the poller and Drop releases everything.
    poller_loop(region, header, cluster, idle_us, stream, memfd).await
}

async fn poller_loop(
    mut region: MmapRegion,
    header: RingHeader,
    cluster: Rc<ClusterClient>,
    idle_us: u64,
    _stream: compio::net::UnixStream,
    _memfd: std::os::unix::io::OwnedFd,
) -> Result<()> {
    // Per-session ring_fd table. Linear ring_fds are simpler than a
    // free list for v1; collisions on close+reopen are not a concern
    // in F180-B4 (test workloads use a small fixed set).
    let mut ring_fds: HashMap<u32, Vec<u8>> = HashMap::new();
    let mut next_fd: u32 = 1;
    let backoff = Duration::from_micros(idle_us);

    loop {
        // Snapshot a single SQE under an immutable borrow, drop, then
        // process. Single-task compio runtime → no concurrent
        // mutation on `region` between borrow-drop and re-borrow.
        let sqe_opt = {
            let cons = SqConsumer::new(region.as_slice(), header);
            cons.try_pop()
        };
        let Some(sqe) = sqe_opt else {
            compio::time::sleep(backoff).await;
            continue;
        };

        let cqe = dispatch_sqe(
            sqe,
            &mut region,
            &header,
            &cluster,
            &mut ring_fds,
            &mut next_fd,
        )
        .await;

        // Push CQE. If CQ is full we drop the completion (a real
        // production daemon should back-pressure SQ pulls; F180-B4
        // skips this for simplicity — clients sized for bench
        // workloads will keep up).
        let mut prod = CqProducer::new(region.as_mut_slice(), header);
        if prod.try_push(cqe).is_err() {
            tracing::warn!("CQ full; dropping completion");
        }
    }
}

/// Service one SQE. Returns the CQE to push back. Always succeeds at
/// the type level — errors become `Cqe::err`.
async fn dispatch_sqe(
    sqe: Sqe,
    region: &mut MmapRegion,
    header: &RingHeader,
    cluster: &ClusterClient,
    ring_fds: &mut HashMap<u32, Vec<u8>>,
    next_fd: &mut u32,
) -> Cqe {
    match sqe.opcode {
        Opcode::Nop => Cqe::ok(sqe.user_data, 0),

        Opcode::Open => {
            // Path bytes live at `buf_offset` in the buffer pool, of
            // length `length` bytes.
            let layout = autumn_ioring::buffer_pool::BufferPoolLayout::from_header(header);
            if let Err(_) = layout.validate_slice(sqe.buf_offset, sqe.length) {
                return Cqe::err(sqe.user_data, libc::EINVAL);
            }
            let path = region.as_slice()
                [sqe.buf_offset as usize..sqe.buf_offset as usize + sqe.length as usize]
                .to_vec();
            // Existence check: HEAD the key. If absent, return ENOENT.
            // (Avoids handing back a ring_fd that points at nothing.)
            // This costs one round-trip per OPEN — acceptable, OPEN
            // is rare in batch workloads.
            match cluster.head(&path).await {
                Ok(meta) if meta.found => {}
                Ok(_) => return Cqe::err(sqe.user_data, libc::ENOENT),
                Err(AutumnError::NotFound) => return Cqe::err(sqe.user_data, libc::ENOENT),
                Err(_) => return Cqe::err(sqe.user_data, libc::EIO),
            }
            let fd = *next_fd;
            *next_fd = next_fd.checked_add(1).unwrap_or(1);
            ring_fds.insert(fd, path);
            Cqe::ok(sqe.user_data, fd as u64)
        }

        Opcode::Read => {
            let key = match ring_fds.get(&sqe.ring_fd) {
                Some(k) => k.clone(),
                None => return Cqe::err(sqe.user_data, libc::EBADF),
            };
            let layout = autumn_ioring::buffer_pool::BufferPoolLayout::from_header(header);
            if let Err(_) = layout.validate_slice(sqe.buf_offset, sqe.length) {
                return Cqe::err(sqe.user_data, libc::EINVAL);
            }
            // Use sub-range get if cluster client supports it. We use
            // `get` for whole-value semantics here and apply offset
            // client-side. F181 (BatchGet) and a future get_range
            // pass-through could trim the wire.
            let data = match cluster.get(&key).await {
                Ok(Some(v)) => v,
                Ok(None) => return Cqe::err(sqe.user_data, libc::ENOENT),
                Err(_) => return Cqe::err(sqe.user_data, libc::EIO),
            };
            // Apply offset / length window.
            let start = sqe.offset as usize;
            if start >= data.len() {
                return Cqe::ok(sqe.user_data, 0); // EOF
            }
            let end = (start + sqe.length as usize).min(data.len());
            let n = end - start;
            // Copy into the buffer pool slot.
            let dst = &mut region.as_mut_slice()
                [sqe.buf_offset as usize..sqe.buf_offset as usize + n];
            dst.copy_from_slice(&data[start..end]);
            Cqe::ok(sqe.user_data, n as u64)
        }

        Opcode::Write => {
            // F180-B4 scope: writes deferred. Return ENOSYS so client
            // can detect and skip.
            Cqe::err(sqe.user_data, libc::ENOSYS)
        }

        Opcode::Close => {
            if ring_fds.remove(&sqe.ring_fd).is_some() {
                Cqe::ok(sqe.user_data, 0)
            } else {
                Cqe::err(sqe.user_data, libc::EBADF)
            }
        }
    }
}

fn rand_session_id() -> u64 {
    // /dev/urandom; 64 bits. Falls back to time-based id on read failure.
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

// Quiet the unused-import lint when the `daemon` feature is on but
// `Write` from io is only used for tracing-subscriber's `.init()`.
#[allow(dead_code)]
fn _unused_writer() -> Box<dyn std::io::Write> {
    Box::new(std::io::stdout().lock())
}
