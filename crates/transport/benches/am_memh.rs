//! Rendezvous frame values sent from pool slabs, with the slab's memh
//! (`PooledBuf::freeze`) against without it (UCX looks the registered memory up
//! in its cache, under the context lock the workers share). `AM_MEMH_SENDERS`
//! sender threads (default 1) each send to their own receiver thread at once,
//! so the lock is contended when there is more than one; every value crosses
//! the NIC rather than UCX's `self` transport.
//!
//! ```text
//! cargo bench -p autumn-transport --features ucx --bench am_memh --no-run
//! AUTUMN_UCX_TEST_BIND='[<roce-ip>]:0' UCX_TLS=rc_mlx5,ud_mlx5,tcp,self UCX_NET_DEVICES=<dev>:1 \
//!   AM_MEMH_SENDERS=8 taskset -c <idle cores> cargo bench -p autumn-transport --features ucx --bench am_memh
//! ```
//! `AM_MEMH_COUNT` values per sender and run (default 4000; an eighth at
//! 8 MiB), `AM_MEMH_ROUNDS` alternating rounds per arm (default 3).

#[cfg(not(feature = "ucx"))]
fn main() {
    eprintln!("am_memh needs --features ucx");
}

#[cfg(feature = "ucx")]
fn main() {
    imp::main()
}

#[cfg(feature = "ucx")]
mod imp {
    use autumn_transport::{am, AutumnTransport, RegBytes, TransportKind, UcxTransport};
    use std::net::SocketAddr;
    use std::sync::{mpsc, Arc, Barrier};
    use std::time::{Duration, Instant};

    const SIZES: [usize; 3] = [64 << 10, 1 << 20, 8 << 20];
    /// Values received at once on the receiving side.
    const WINDOW: usize = 32;

    fn env_or(name: &str, default: usize) -> usize {
        std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
    }

    fn thread_cpu() -> Duration {
        let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
        Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
    }

    /// Runs as `(size, count, with_memh, warm_up)`, in the order every thread
    /// walks them; each size starts with an unreported warm-up run.
    fn plan(count: usize, rounds: usize) -> Vec<(usize, usize, bool, bool)> {
        let mut runs = Vec::new();
        for &size in &SIZES {
            let n = if size >= 8 << 20 { count / 8 } else { count };
            runs.push((size, n, true, true));
            for _ in 0..rounds {
                // Alternate the arms so drift hits both.
                runs.push((size, n, true, false));
                runs.push((size, n, false, false));
            }
        }
        runs
    }

    /// One sender's run: (receiver's elapsed time, sender thread CPU).
    type RunResult = (Duration, Duration);

    fn receiver(bind: SocketAddr, runs: Vec<(usize, usize, bool, bool)>, addr_tx: mpsc::Sender<SocketAddr>, done_tx: mpsc::Sender<Duration>) {
        compio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut listener = UcxTransport.bind(bind).await.unwrap();
            addr_tx.send(listener.local_addr().unwrap()).unwrap();
            let (conn, _) = listener.accept().await.unwrap();
            let (reader, _w) = conn.into_split();
            let mut tag = 1u64;
            for (size, n, _, _) in runs {
                let started = Instant::now();
                let mut pending = std::collections::VecDeque::new();
                for _ in 0..n {
                    pending.push_back(am::start_frame_value(&reader, &[(size, tag)], &[]).unwrap());
                    tag += 1;
                    if pending.len() == WINDOW {
                        pending.pop_front().unwrap().wait().await.unwrap();
                    }
                }
                for v in pending {
                    v.wait().await.unwrap();
                }
                done_tx.send(started.elapsed()).unwrap();
            }
        })
    }

    fn sender(
        addr: SocketAddr,
        runs: Vec<(usize, usize, bool, bool)>,
        start: Arc<Barrier>,
        done_rx: mpsc::Receiver<Duration>,
        out: mpsc::Sender<RunResult>,
    ) {
        compio::runtime::Runtime::new().unwrap().block_on(async move {
            let conn = UcxTransport.connect(addr).await.unwrap();
            let (_r, w) = conn.into_split();
            let tx = w.am_sender().expect("a UCX connection");
            let slabs: Vec<RegBytes> = SIZES
                .iter()
                .map(|&size| {
                    let mut pb = autumn_transport::regpool_acquire(size);
                    pb.dest_mut().fill(0xa5);
                    pb.freeze()
                })
                .collect();
            assert!(slabs.iter().all(RegBytes::is_registered), "pool slabs must be registered");
            let mut tag = 1u64;
            for (size, n, with_memh, _) in runs {
                let slab = &slabs[SIZES.iter().position(|&s| s == size).unwrap()];
                start.wait();
                let cpu = thread_cpu();
                let sent_before = am::sent_counts();
                for _ in 0..n {
                    let value = if with_memh { slab.clone() } else { RegBytes::from(slab.bytes().clone()) };
                    tx.post_frame_value(tag, value);
                    tag += 1;
                }
                // Keep this runtime turning until the receiver has every value.
                let elapsed = loop {
                    if let Ok(d) = done_rx.try_recv() {
                        break d;
                    }
                    compio::time::sleep(Duration::from_micros(200)).await;
                };
                let cpu = thread_cpu() - cpu;
                let with = am::sent_counts().0 - sent_before.0;
                assert_eq!(with, if with_memh { n as u64 } else { 0 }, "memh not handed to UCX as asked");
                out.send((elapsed, cpu)).unwrap();
            }
        })
    }

    pub fn main() {
        let bind: SocketAddr = std::env::var("AUTUMN_UCX_TEST_BIND")
            .expect("set AUTUMN_UCX_TEST_BIND=\"[<roce-ip>]:0\"")
            .parse()
            .expect("parse AUTUMN_UCX_TEST_BIND");
        let senders = env_or("AM_MEMH_SENDERS", 1).max(1);
        let runs = plan(env_or("AM_MEMH_COUNT", 4000), env_or("AM_MEMH_ROUNDS", 3));
        autumn_transport::init_with(TransportKind::Ucx);

        let start = Arc::new(Barrier::new(senders));
        let mut results = Vec::new();
        let mut threads = Vec::new();
        for _ in 0..senders {
            let (addr_tx, addr_rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();
            let (out_tx, out_rx) = mpsc::channel();
            let r = runs.clone();
            threads.push(std::thread::spawn(move || receiver(bind, r, addr_tx, done_tx)));
            let addr = addr_rx.recv().unwrap();
            let (r, start) = (runs.clone(), start.clone());
            threads.push(std::thread::spawn(move || sender(addr, r, start, done_rx, out_tx)));
            results.push(out_rx);
        }

        println!("senders={senders}");
        println!("{:>8} | {:>7} | {:>10} | {:>8} | {:>14}", "size", "memh", "msgs/s", "GB/s", "sender cpu/msg");
        for (size, n, with_memh, warm_up) in runs {
            let per: Vec<RunResult> = results.iter().map(|r| r.recv().unwrap()).collect();
            if warm_up {
                continue;
            }
            let total = (n * senders) as f64;
            let wall = per.iter().map(|p| p.0).max().unwrap().as_secs_f64();
            let cpu: f64 = per.iter().map(|p| p.1.as_secs_f64()).sum();
            println!(
                "{:>8} | {:>7} | {:>10.0} | {:>8.2} | {:>11.2} µs",
                format!("{}K", size >> 10),
                if with_memh { "slab" } else { "rcache" },
                total / wall,
                total * size as f64 / wall / 1e9,
                cpu * 1e6 / total,
            );
        }
        for t in threads {
            t.join().unwrap();
        }
    }
}
