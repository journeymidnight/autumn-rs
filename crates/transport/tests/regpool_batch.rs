//! A batch takes its buffers all or none (`acquire_many_for`): it never holds
//! some while it waits for room for the rest. A 64 MiB cap gives `Reply` about
//! 25.6 MiB, a thread half of that.

use std::time::{Duration, Instant};

use autumn_transport::{regpool_acquire_many_for, regpool_try_acquire_for, set_regpool_cap_bytes, PoolError, PoolUse};

const MIB: usize = 1 << 20;

fn reply_in_use() -> u64 {
    autumn_transport::regpool_snapshot().uses[PoolUse::Reply as usize].in_use_bytes
}

fn block_on<T>(f: impl std::future::Future<Output = T>) -> T {
    compio::runtime::Runtime::new().unwrap().block_on(f)
}

#[test]
fn a_batch_takes_all_its_buffers_or_none() {
    assert!(set_regpool_cap_bytes(64 * MIB), "this process sets the cap first");

    // More than a thread may hold in total, though each would fit: refused at
    // once, nothing taken.
    let started = Instant::now();
    let got = block_on(regpool_acquire_many_for(PoolUse::Reply, &[4 * MIB; 4], Duration::from_secs(1)));
    assert_eq!(got.err(), Some(PoolError::TooLarge));
    assert!(started.elapsed() < Duration::from_millis(500), "no wait for what can never fit");
    assert_eq!(reply_in_use(), 0);

    // Another thread holds 20 MiB: 8 MiB in two buffers does not fit.
    let held: Vec<_> = (0..2)
        .map(|_| std::thread::spawn(|| regpool_try_acquire_for(PoolUse::Reply, 10 * MIB).expect("hold")).join().unwrap())
        .collect();
    let before = reply_in_use();
    let got = block_on(regpool_acquire_many_for(PoolUse::Reply, &[4 * MIB; 2], Duration::from_millis(100)));
    assert_eq!(got.err(), Some(PoolError::Full));
    assert_eq!(reply_in_use(), before, "a batch that timed out holds nothing");

    // While it waits it holds nothing either, and room for all of it wakes it.
    let waiter = std::thread::spawn(|| block_on(regpool_acquire_many_for(PoolUse::Reply, &[4 * MIB; 2], Duration::from_secs(10))));
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(reply_in_use(), before, "a waiting batch holds none of its buffers");
    let mut held = held;
    drop(held.pop());
    let bufs = waiter.join().unwrap().expect("room for the whole batch");
    assert_eq!(bufs.iter().map(|b| b.cap()).collect::<Vec<_>>(), vec![4 * MIB; 2]);
    assert_eq!(reply_in_use(), (10 * MIB + 8 * MIB) as u64);
    drop(bufs);
    assert_eq!(reply_in_use(), (10 * MIB) as u64, "each buffer gives its share back");
    drop(held);

    // Short of memory, not of budget: the cap's one chunk holds 60 MiB of
    // unbudgeted buffers, so of two 4 MiB buffers only the first fits. Giving
    // that one back is no new room, so the batch must sleep out its wait, not
    // wake itself and spin.
    let filler = autumn_transport::regpool_acquire(60 * MIB);
    let (got, cpu) = std::thread::spawn(|| {
        let cpu = || {
            let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
            // SAFETY: a valid timespec out-parameter.
            unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
            Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
        };
        let before = cpu();
        let got = block_on(regpool_acquire_many_for(PoolUse::Reply, &[4 * MIB; 2], Duration::from_secs(1)));
        (got, cpu() - before)
    })
    .join()
    .unwrap();
    assert_eq!(got.err(), Some(PoolError::Full));
    assert!(cpu < Duration::from_millis(200), "the batch spun for {cpu:?} of CPU while it waited");
    assert_eq!(reply_in_use(), 0);
    drop(filler);
}
