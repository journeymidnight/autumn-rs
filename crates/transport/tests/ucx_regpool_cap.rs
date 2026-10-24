//! The registered pool's budget is not held hostage by idle slabs: once a
//! burst of large buffers went back to the pool, a buffer of another size is
//! still registered (the idle ones give their registration back), not a fresh
//! unregistered allocation.

#![cfg(feature = "ucx")]

use autumn_transport::{regpool_acquire, set_regpool_cap_bytes, TransportKind};

#[test]
fn idle_slabs_make_room_for_another_size() {
    autumn_transport::init_with(TransportKind::Ucx);
    assert!(set_regpool_cap_bytes(64 << 20), "this process sets the cap first");
    let big: Vec<_> = (0..2).map(|_| regpool_acquire(32 << 20)).collect();
    assert!(big.iter().all(|b| b.reg().is_some()), "the cap fits both");
    drop(big); // idle in the pool, still registered: the budget is full
    let small = regpool_acquire(8 << 20);
    assert!(small.reg().is_some(), "an idle slab should have made room");
    drop(small);
    let evicted = autumn_transport::regpool_snapshot().evicted_total;
    assert_eq!(evicted, 1, "one 32 MiB slab covers the shortfall");
    // The big size again, past the idle 8 MiB slab: 32 in use + 8 idle + 32.
    let held = regpool_acquire(32 << 20);
    let second = regpool_acquire(32 << 20);
    assert!(held.reg().is_some() && second.reg().is_some(), "the idle 8 MiB slab should have made room");

    // In use alone, the slabs fill the cap: nothing idle is thrown away for
    // a request that cannot fit anyway.
    drop(second);
    let before = autumn_transport::regpool_snapshot().evicted_total;
    let over = regpool_acquire(64 << 20);
    assert!(over.reg().is_none());
    assert_eq!(autumn_transport::regpool_snapshot().evicted_total, before, "evicted for nothing");
    drop(held);
    drop(over);

    // Idle 32 and 8 MiB, 16 MiB in use: another 16 MiB is 8 MiB short, and
    // the idle 8 MiB slab covers it — the 32 MiB one stays.
    drop(regpool_acquire(8 << 20)); // evicts a 32: idle 32 + 8, 40 registered
    let in_use = regpool_acquire(16 << 20);
    let bytes = autumn_transport::regpool_snapshot().evicted_bytes;
    let another = regpool_acquire(16 << 20);
    assert!(in_use.reg().is_some() && another.reg().is_some());
    assert_eq!(autumn_transport::regpool_snapshot().evicted_bytes - bytes, 8 << 20, "smallest idle class that covers it");
}
