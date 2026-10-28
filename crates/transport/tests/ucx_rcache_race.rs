//! Rendezvous sends from many worker threads while other threads release pages
//! the registration cache covers.
//!
//! A UCX send of unregistered memory looks the buffer up in the context's
//! registration cache; a `munmap` / `madvise(MADV_DONTNEED)` on any thread
//! invalidates the cached regions it overlaps. UCX 1.16 let that invalidation
//! run on the releasing thread while a worker thread's lock-free lookup was
//! taking a reference to the same region, so a region could be deregistered
//! under an in-flight send: the NIC then fails the WQE with `Local protection
//! error` and UCX aborts the process. Active-message values are what still go
//! to UCX as the caller's memory (stream sends leave from the registered pool),
//! so each send here is a 64 KiB rendezvous from a region the releasers keep
//! invalidating. Nobody expects the tags, so the receiver drops each value,
//! which completes the send.
//!
//! Needs a RoCE address: `AUTUMN_UCX_TEST_BIND="[<roce-ip>]:0"`, plus the
//! usual `UCX_TLS` / `UCX_NET_DEVICES`.

#![cfg(feature = "ucx")]

use autumn_transport::{AutumnTransport, UcxTransport};
use bytes::Bytes;
use compio::io::AsyncRead;
use compio::BufResult;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const SENDERS: usize = 8;
const RELEASERS: usize = 2;
const PAGE: usize = 4096;
/// One rendezvous value: a whole region, so a release of any of its pages
/// drops the region's cached registration.
const REGION: usize = autumn_transport::BULK_MIN_BYTES;
const HOT_REGIONS: usize = 64;
/// Regions a region apart, so cached registrations do not merge.
const STRIDE: usize = 2;
const ARENA_BYTES: usize = HOT_REGIONS * STRIDE * REGION;
const RELEASE_EVERY: Duration = Duration::from_millis(2);
const RUN_FOR: Duration = Duration::from_secs(60);

fn bind_addr() -> SocketAddr {
    std::env::var("AUTUMN_UCX_TEST_BIND")
        .expect("set AUTUMN_UCX_TEST_BIND=\"[<roce-ip>]:0\" to a RoCE-attached address")
        .parse()
        .expect("parse AUTUMN_UCX_TEST_BIND")
}

struct Arena(*mut u8);
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

/// xorshift; the test only needs scattered offsets.
fn next(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

#[test]
fn rendezvous_sends_survive_concurrent_page_release() {
    let arena = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            ARENA_BYTES,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    assert_ne!(arena, libc::MAP_FAILED);
    let arena = Arc::new(Arena(arena as *mut u8));
    unsafe { std::ptr::write_bytes(arena.0, 0x5a, ARENA_BYTES) };

    let stop = Arc::new(AtomicBool::new(false));
    let sent = Arc::new(AtomicU64::new(0));
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();

    // One receiving thread drains every connection; Stream has no EOF, so it
    // is left running when the test returns.
    std::thread::spawn(move || {
        compio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                let mut listener = UcxTransport.bind(bind_addr()).await.unwrap();
                addr_tx.send(listener.local_addr().unwrap()).unwrap();
                let mut drains = Vec::new();
                for _ in 0..SENDERS {
                    let (c, _) = listener.accept().await.unwrap();
                    drains.push(compio::runtime::spawn(async move {
                        let (mut r, _w) = c.into_split();
                        let mut buf = vec![0u8; 1 << 20];
                        loop {
                            let BufResult(n, b) = r.read(buf).await;
                            buf = b;
                            match n {
                                Ok(0) | Err(_) => break,
                                Ok(_) => {}
                            }
                        }
                    }));
                }
                for d in drains {
                    d.await.expect("drain task");
                }
            })
    });
    let addr = addr_rx.recv().unwrap();

    let senders: Vec<_> = (0..SENDERS)
        .map(|t| {
            let (arena, stop, sent) = (arena.clone(), stop.clone(), sent.clone());
            std::thread::spawn(move || {
                compio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        let c = UcxTransport.connect(addr).await.unwrap();
                        let (_r, w) = c.into_split();
                        let am = w.am_sender().expect("a UCX connection");
                        let mut seed = 0x9e37_79b9_7f4a_7c15 ^ (t as u64 + 1);
                        let mut tag = 0u64;
                        while !stop.load(Ordering::Relaxed) {
                            let region = (next(&mut seed) as usize) % HOT_REGIONS;
                            let s = unsafe {
                                std::slice::from_raw_parts(arena.0.add(region * REGION * STRIDE) as *const u8, REGION)
                            };
                            tag += 1;
                            am.send(tag, Bytes::from_static(s)).await.unwrap();
                            sent.fetch_add(1, Ordering::Relaxed);
                        }
                    })
            })
        })
        .collect();

    // Page release on threads that own no UCX worker, as an allocator's purge
    // does. The bytes stay mapped; only the cached registrations are dropped.
    let releasers: Vec<_> = (0..RELEASERS)
        .map(|t| {
            let (arena, stop) = (arena.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut seed = 0x2545_f491_4f6c_dd1d ^ (t as u64 + 1);
                while !stop.load(Ordering::Relaxed) {
                    let region = (next(&mut seed) as usize) % HOT_REGIONS;
                    let page = (next(&mut seed) as usize) % (REGION / PAGE);
                    unsafe {
                        libc::madvise(
                            arena.0.add(region * REGION * STRIDE + page * PAGE) as *mut libc::c_void,
                            PAGE,
                            libc::MADV_DONTNEED,
                        );
                    }
                    std::thread::sleep(RELEASE_EVERY);
                }
            })
        })
        .collect();

    std::thread::sleep(RUN_FOR);
    stop.store(true, Ordering::Relaxed);
    for h in releasers {
        h.join().unwrap();
    }
    for h in senders {
        h.join().unwrap();
    }
    let sends = sent.load(Ordering::Relaxed);
    eprintln!("rendezvous sends: {sends}");
    assert!(sends > 0);
}
