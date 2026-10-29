//! Pool allocation under a mixed in-flight load: each thread keeps `WINDOW`
//! buffers of mixed sizes outstanding, acquiring a new one and dropping the
//! oldest; one in `XTHREAD` buffers is dropped on another thread instead.
//! Reports ns per acquire+drop and the registered bytes the pool holds at the
//! end. Only the public pool API is used, so the same file runs against
//! another build of the pool.
//!
//! ```text
//! UCX_TLS=rc_mlx5,ud_mlx5,tcp,self UCX_NET_DEVICES=<dev>:1 \
//!   taskset -c <idle cores> cargo bench -p autumn-transport --features ucx --bench regpool_alloc
//! ```
//! `REGPOOL_THREADS` (default "1,8"), `REGPOOL_OPS` per thread (default 200000),
//! `REGPOOL_XTHREAD` (default 10; 0 drops every buffer on its own thread).

#[cfg(not(feature = "ucx"))]
fn main() {
    eprintln!("regpool_alloc needs --features ucx");
}

#[cfg(feature = "ucx")]
fn main() {
    use autumn_transport::{regpool_acquire, regpool_snapshot, PooledBuf, TransportKind};
    use std::collections::VecDeque;
    use std::sync::mpsc;
    use std::time::Instant;

    const WINDOW: usize = 64;
    /// Sizes by share: mostly small values, some bulk, a few large.
    fn size(i: u64) -> usize {
        let r = (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) % 100;
        let jitter = ((i * 7919) % 4000) as usize;
        match r {
            0..=59 => 4096 + jitter,
            60..=79 => (64 << 10) + jitter,
            80..=94 => (1 << 20) + jitter,
            _ => (8 << 20) + jitter,
        }
    }

    autumn_transport::init_with(TransportKind::Ucx);
    let xthread: u64 = std::env::var("REGPOOL_XTHREAD").ok().and_then(|v| v.parse().ok()).unwrap_or(10);
    let ops: u64 = std::env::var("REGPOOL_OPS").ok().and_then(|v| v.parse().ok()).unwrap_or(200_000);
    let threads: Vec<usize> = std::env::var("REGPOOL_THREADS")
        .unwrap_or_else(|_| "1,8".into())
        .split(',')
        .map(|t| t.parse().unwrap())
        .collect();
    println!("{:>7} | {:>12} | {:>16} | {:>12}", "threads", "ns/op", "registered MiB", "over cap");
    for t in threads {
        // A drop thread for the buffers that leave their acquirer.
        let (drop_tx, drop_rx) = mpsc::channel::<PooledBuf>();
        let dropper = std::thread::spawn(move || for pb in drop_rx { drop(pb) });
        let start = Instant::now();
        let workers: Vec<_> = (0..t)
            .map(|w| {
                let drop_tx = drop_tx.clone();
                std::thread::spawn(move || {
                    let mut window = VecDeque::with_capacity(WINDOW);
                    for i in 0..ops {
                        let mut pb = regpool_acquire(size(i + (w as u64) * ops));
                        pb.dest_mut()[0] = 1;
                        window.push_back(pb);
                        if window.len() > WINDOW {
                            let old = window.pop_front().unwrap();
                            if xthread != 0 && i % xthread == 0 {
                                drop_tx.send(old).unwrap();
                            } else {
                                drop(old);
                            }
                        }
                    }
                })
            })
            .collect();
        for w in workers {
            w.join().unwrap();
        }
        let elapsed = start.elapsed();
        drop(drop_tx);
        dropper.join().unwrap();
        let s = regpool_snapshot();
        println!(
            "{:>7} | {:>12.0} | {:>16} | {:>12}",
            t,
            elapsed.as_nanos() as f64 / ops as f64,
            s.registered_bytes >> 20,
            s.over_cap_total
        );
    }
}
