//! Each use has its own share of the pool, one thread holds at most half of a
//! use's share, and `acquire_for` waits for room instead of falling back —
//! for its use's budget, or for pool memory when that is what ran short.
//! A 64 MiB cap: 40% for `Receive` (~25.6 MiB, ~12.8 MiB per thread), 40% for
//! `Reply`.

use autumn_transport::{regpool_acquire_for, regpool_try_acquire_for, set_regpool_cap_bytes, PoolError, PoolUse};
use std::time::{Duration, Instant};

const MIB: usize = 1 << 20;

/// Spin until `f` holds; fail instead of hanging.
fn until(what: &str, f: impl Fn() -> bool) {
    let started = Instant::now();
    while !f() {
        assert!(started.elapsed() < Duration::from_secs(5), "never: {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn receive_stats() -> autumn_transport::PoolUseStats {
    autumn_transport::regpool_snapshot().uses[PoolUse::Receive as usize]
}

#[test]
fn uses_and_threads_are_budgeted_and_a_full_budget_waits() {
    assert!(set_regpool_cap_bytes(64 * MIB), "this process sets the cap first");
    let a = regpool_try_acquire_for(PoolUse::Receive, 8 * MIB).expect("room for one");
    assert_eq!(
        regpool_try_acquire_for(PoolUse::Receive, 8 * MIB).err(),
        Some(PoolError::Full),
        "one thread holds at most half of the use"
    );
    assert_eq!(
        regpool_try_acquire_for(PoolUse::Receive, 16 * MIB).err(),
        Some(PoolError::TooLarge),
        "more than a thread may ever hold"
    );
    // Two more threads fill the use; the next is refused.
    let b = std::thread::spawn(|| regpool_try_acquire_for(PoolUse::Receive, 8 * MIB)).join().unwrap().expect("b");
    let c = std::thread::spawn(|| regpool_try_acquire_for(PoolUse::Receive, 8 * MIB)).join().unwrap().expect("c");
    let refused = std::thread::spawn(|| regpool_try_acquire_for(PoolUse::Receive, 8 * MIB).err()).join().unwrap();
    assert_eq!(refused, Some(PoolError::Full), "the use's share is spent");
    assert!(receive_stats().high_water_bytes >= 24 * MIB as u64);

    // Another use is not touched by it.
    let reply = regpool_try_acquire_for(PoolUse::Reply, 8 * MIB).expect("Reply has its own share");

    // A full use times out without a buffer.
    let timeouts = receive_stats().timeouts;
    let started = Instant::now();
    let got = std::thread::spawn(|| {
        compio::runtime::Runtime::new().unwrap().block_on(regpool_acquire_for(PoolUse::Receive, 8 * MIB, Duration::from_millis(100)))
    })
    .join()
    .unwrap();
    assert_eq!(got.err(), Some(PoolError::Full));
    assert!(started.elapsed() >= Duration::from_millis(100));
    assert_eq!(receive_stats().timeouts, timeouts + 1);

    // A waiter gets the room a buffer gives back, here from another thread.
    let waits = receive_stats().waits;
    let waiter = std::thread::spawn(|| {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let started = Instant::now();
            let pb = regpool_acquire_for(PoolUse::Receive, 8 * MIB, Duration::from_secs(10)).await;
            (pb.map(|p| p.cap()), started.elapsed())
        })
    });
    until("the waiter waits", || receive_stats().waits > waits);
    drop(b);
    let (got, waited) = waiter.join().unwrap();
    assert_eq!(got, Ok(8 * MIB), "woken by the return");
    assert!(waited < Duration::from_secs(5), "woken, not timed out: {waited:?}");
    drop((a, c, reply));
    assert_eq!(receive_stats().in_use_bytes, 0, "every charge given back");

    // Budget to spare but no memory (an unbudgeted buffer fills the chunk):
    // the wait is on the pool's memory, and memory coming back wakes it.
    let hog = autumn_transport::regpool_acquire(60 * MIB);
    assert!(autumn_transport::regpool_snapshot().in_use_bytes >= 60 * MIB as u64, "the hog is pool memory");
    let reply_waits = autumn_transport::regpool_snapshot().uses[PoolUse::Reply as usize].waits;
    let waiter = std::thread::spawn(|| {
        compio::runtime::Runtime::new()
            .unwrap()
            .block_on(regpool_acquire_for(PoolUse::Reply, 8 * MIB, Duration::from_secs(10)))
            .map(|p| p.cap())
    });
    until("the memory waiter waits", || autumn_transport::regpool_snapshot().uses[PoolUse::Reply as usize].waits > reply_waits);
    drop(hog);
    assert_eq!(waiter.join().unwrap(), Ok(8 * MIB), "woken by memory coming back");
}
