//! The pool's chunks are a process budget: at the cap a thread shares a chunk
//! another thread claimed; a request no chunk has room for takes a registered
//! overflow chunk, shared by the next such request and freed when its last
//! buffer returns — never memory UCX would register on its own.

#![cfg(feature = "ucx")]

use autumn_transport::{regpool_acquire, regpool_snapshot, set_regpool_cap_bytes, TransportKind};

const MIB: usize = 1 << 20;

#[test]
fn at_the_cap_a_request_takes_a_registered_overflow_chunk_freed_when_empty() {
    autumn_transport::init_with(TransportKind::Ucx);
    assert!(set_regpool_cap_bytes(64 * MIB), "this process sets the cap first");
    // One 64 MiB chunk: the whole budget, claimed by this thread.
    let held = regpool_acquire(32 * MIB);
    assert!(held.reg().is_some(), "the first chunk is registered");
    assert_eq!(regpool_snapshot().registered_bytes, 64 << 20);

    // Another thread can make no chunk of its own; it shares this one.
    let shared = std::thread::spawn(|| regpool_acquire(16 * MIB)).join().unwrap();
    assert!(shared.reg().is_some(), "a claimed chunk with room is shared at the cap");

    // 16 MiB left, 32 asked: an overflow chunk, registered.
    let before = regpool_snapshot();
    let over = regpool_acquire(32 * MIB);
    assert!(over.reg().is_some(), "an overflow buffer is registered");
    let s = regpool_snapshot();
    assert_eq!(s.over_cap_total, before.over_cap_total + 1);
    assert_eq!(s.overflow_made_total, before.overflow_made_total + 1);
    assert_eq!(s.overflow_bytes, 64 << 20);
    assert_eq!(s.registered_bytes, 128 << 20);

    // The next over-cap request (16 MiB left in the cap's chunk) shares it.
    let over2 = regpool_acquire(24 * MIB);
    assert!(over2.reg().is_some());
    assert_eq!(regpool_snapshot().overflow_made_total, before.overflow_made_total + 1, "shared, not made again");

    // Its last buffer back: the overflow chunk is gone.
    drop(over);
    assert_eq!(regpool_snapshot().overflow_bytes, 64 << 20, "one buffer still holds it");
    // Returned on another thread, which then unregisters it.
    std::thread::spawn(move || drop(over2)).join().unwrap();
    let s = regpool_snapshot();
    assert_eq!(s.overflow_bytes, 0, "freed when empty");
    assert_eq!(s.registered_bytes, 64 << 20, "back to the cap");

    // The shared buffer comes back from the other thread: room again.
    drop(shared);
    let again = regpool_acquire(32 * MIB);
    assert!(again.reg().is_some(), "a range freed on another thread is reusable");
    assert_eq!(regpool_snapshot().overflow_made_total, before.overflow_made_total + 1, "no overflow while the cap has room");
    drop((held, again));
}
