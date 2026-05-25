//! `autumn-ioring-bench` — read throughput bench through the SHM
//! io_uring side-channel.
//!
//! Drives N concurrent reader threads, each with its own
//! `IoRingClient` (one Unix socket / one mmap region per thread, so
//! threads don't contend on a single SQ). The daemon serves all
//! sessions on its single compio runtime; concurrency on the daemon
//! side comes from per-session poller tasks running cooperatively.
//!
//! Usage:
//!
//! ```ignore
//! autumn-ioring-bench \
//!   --socket /run/autumn-ioring/ring.sock \
//!   --key dataset/sample.bin \
//!   --threads 16 \
//!   --depth 8 \
//!   --duration 10 \
//!   --read-size 4096
//! ```
//!
//! For the validation gate (≥ 200 k 4K read ops/s) on
//! threads=16 depth=8 size=4K, with the daemon running.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;

use autumn_ioring::client::IoRingClient;
use autumn_ioring::cqe::Cqe;
use autumn_ioring::opcode::Opcode;
use autumn_ioring::sqe::Sqe;

#[derive(Parser, Debug, Clone)]
#[command(name = "autumn-ioring-bench", about = "F180 read perf bench")]
struct Args {
    /// Daemon Unix socket path. With `--runtimes > 1` the bench
    /// connects to `{socket}.{tid % runtimes}` so worker threads
    /// distribute themselves across daemon runtimes.
    #[arg(long, default_value = "/run/autumn-ioring/ring.sock")]
    socket: PathBuf,

    /// Number of daemon runtimes. Workers tid % runtimes pick the
    /// listener path. Default 1 = single-runtime daemon (legacy).
    #[arg(long, default_value_t = 1)]
    runtimes: usize,

    /// Key to read (must already exist in the cluster). The token
    /// `%tid%` (if present) is replaced with the worker thread id —
    /// pass e.g. `--key 'f180/key_%tid%'` to spread load across many
    /// keys / partitions instead of hammering a single hot key.
    #[arg(long)]
    key: String,

    /// Reader thread count.
    #[arg(long, default_value_t = 16)]
    threads: usize,

    /// In-flight reads per thread.
    #[arg(long, default_value_t = 8)]
    depth: usize,

    /// Read size per op.
    #[arg(long, default_value_t = 4096)]
    read_size: u32,

    /// Bench duration (seconds).
    #[arg(long, default_value_t = 10)]
    duration: u64,

    /// CQE wait idle backoff in microseconds.
    #[arg(long, default_value_t = 1)]
    idle_us: u64,

    /// Per-slot buffer size (bytes). MUST be >= read_size. Default 1 MiB; bump
    /// to e.g. 8388608 for whole-extent (8 MiB) reads (F243 perf compare).
    #[arg(long, default_value_t = 1024 * 1024)]
    slot_size: u32,

    /// Per-client buffer-pool size (bytes). Holds num_slots = pool/slot buffers;
    /// must fit depth+1 slots. Default 64 MiB.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    pool_size: u64,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();

    tracing::info!(
        threads = args.threads,
        depth = args.depth,
        read_size = args.read_size,
        duration = args.duration,
        "bench starting"
    );

    let stop = Arc::new(AtomicBool::new(false));
    let total_ops = Arc::new(AtomicU64::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for tid in 0..args.threads {
        let args = args.clone();
        let stop = stop.clone();
        let total_ops = total_ops.clone();
        let total_bytes = total_bytes.clone();
        let h = thread::Builder::new()
            .name(format!("ioring-bench-{tid}"))
            .spawn(move || worker(tid, args, stop, total_ops, total_bytes))
            .expect("spawn worker");
        handles.push(h);
    }

    let started = Instant::now();
    thread::sleep(Duration::from_secs(args.duration));
    stop.store(true, Ordering::Release);

    let mut had_err = false;
    for h in handles {
        if let Err(e) = h.join().map_err(|_| "panic") {
            tracing::warn!(error = %e, "worker panicked");
            had_err = true;
        }
    }
    let elapsed = started.elapsed();

    let ops = total_ops.load(Ordering::Acquire);
    let bytes = total_bytes.load(Ordering::Acquire);
    let secs = elapsed.as_secs_f64();
    let ops_per_sec = ops as f64 / secs;
    let mb_per_sec = bytes as f64 / secs / (1024.0 * 1024.0);
    println!();
    println!("=== F180-C bench results ===");
    println!("threads      : {}", args.threads);
    println!("depth/thread : {}", args.depth);
    println!("read_size    : {} bytes", args.read_size);
    println!("duration     : {:.2}s", secs);
    println!("total_ops    : {}", ops);
    println!("ops/s        : {:.0}", ops_per_sec);
    println!("MB/s         : {:.1}", mb_per_sec);
    println!();
    println!("Validation gate: ≥ 200,000 ops/s on 4K random reads");
    println!(
        "  current    : {:.0} ops/s — {}",
        ops_per_sec,
        if ops_per_sec >= 200_000.0 {
            "PASS"
        } else {
            "below gate"
        }
    );

    if had_err {
        std::process::exit(1);
    }
    Ok(())
}

fn worker(
    tid: usize,
    args: Args,
    stop: Arc<AtomicBool>,
    total_ops: Arc<AtomicU64>,
    total_bytes: Arc<AtomicU64>,
) {
    let socket_path = if args.runtimes > 1 {
        let mut s = args.socket.as_os_str().to_owned();
        s.push(format!(".{}", tid % args.runtimes));
        PathBuf::from(s)
    } else {
        args.socket.clone()
    };
    // F243: negotiate slot/pool sizes so whole-extent (8 MiB) reads fit a slot
    // (validate_slice requires read_size <= slot_size).
    let mut hello = autumn_ioring::handshake::HelloRequest::defaults();
    hello.buf_slot_size = args.slot_size;
    hello.buf_pool_size = args.pool_size;
    let mut client = match IoRingClient::connect_with(&socket_path, hello) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(tid, socket = %socket_path.display(), error = %e, "client connect failed");
            return;
        }
    };

    // OPEN the key. Slot 0 holds the path bytes during this call;
    // we'll re-use it for read buffers afterward. Substitute %tid%
    // so multi-key workloads can spread across partitions.
    let resolved_key = args.key.replace("%tid%", &format!("{:02}", tid));
    let path_bytes = resolved_key.as_bytes();
    if path_bytes.len() > client.header().buf_slot_size as usize {
        tracing::error!(tid, "key path too long for buffer slot");
        return;
    }
    let path_slot = match client.alloc_buf_slot() {
        Some(s) => s,
        None => {
            tracing::error!(tid, "no free buffer slot for OPEN");
            return;
        }
    };
    client.write_buf(path_slot, path_bytes);
    let _ = client.submit(Sqe {
        opcode: Opcode::Open,
        ring_fd: 0,
        offset: 0,
        length: path_bytes.len() as u32,
        buf_offset: path_slot,
        user_data: u64::MAX, // sentinel for OPEN
    });
    let cqe = client.wait_completion(args.idle_us);
    if cqe.result < 0 {
        tracing::error!(tid, errno = -cqe.result, "OPEN failed");
        return;
    }
    let ring_fd = cqe.result as u32;
    client.free_buf_slot(path_slot);

    // Pre-allocate `depth` buffer slots.
    let mut slots = Vec::with_capacity(args.depth);
    for _ in 0..args.depth {
        match client.alloc_buf_slot() {
            Some(s) => slots.push(s),
            None => {
                tracing::error!(tid, "out of buffer slots before priming");
                return;
            }
        }
    }

    // Prime: submit `depth` reads.
    for (i, &slot) in slots.iter().enumerate() {
        let _ = client.submit(Sqe {
            opcode: Opcode::Read,
            ring_fd,
            offset: 0, // hot key, same data
            length: args.read_size,
            buf_offset: slot,
            user_data: i as u64,
        });
    }

    // Hot loop: drain CQEs, resubmit. user_data carries slot index so
    // we can re-issue the SQE pointing at the same slot.
    let mut local_ops = 0u64;
    let mut local_bytes = 0u64;
    let mut completions: Vec<Cqe> = Vec::with_capacity(args.depth);
    while !stop.load(Ordering::Acquire) {
        // Drain available completions; if none, spin-wait briefly.
        completions.clear();
        let n = client.drain_completions(&mut completions, args.depth);
        if n == 0 {
            // Tight wait; with a busy daemon we usually get something.
            for _ in 0..256 {
                if let Some(c) = client.try_completion() {
                    completions.push(c);
                    break;
                }
                std::hint::spin_loop();
            }
            if completions.is_empty() {
                continue;
            }
        }
        for cqe in completions.drain(..) {
            if cqe.result < 0 {
                tracing::warn!(tid, errno = -cqe.result, "read errno");
                continue;
            }
            local_ops += 1;
            local_bytes += cqe.result as u64;
            let slot_idx = cqe.user_data as usize;
            if slot_idx < slots.len() {
                let slot = slots[slot_idx];
                // Re-issue.
                let _ = client.submit(Sqe {
                    opcode: Opcode::Read,
                    ring_fd,
                    offset: 0,
                    length: args.read_size,
                    buf_offset: slot,
                    user_data: slot_idx as u64,
                });
            }
        }
    }

    total_ops.fetch_add(local_ops, Ordering::Relaxed);
    total_bytes.fetch_add(local_bytes, Ordering::Relaxed);

    // Drain remaining inflight (best-effort) so the daemon doesn't
    // see a backlog when this client disconnects.
    for _ in 0..args.depth {
        let _ = client.try_completion();
    }
}
