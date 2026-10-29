//! The pool's chunks are a process budget: at the cap a thread shares a chunk
//! another thread claimed rather than fall back, and only a request no chunk
//! has room for gets an unregistered heap buffer.

#![cfg(feature = "ucx")]

use autumn_transport::{regpool_acquire, set_regpool_cap_bytes, TransportKind};

#[test]
fn at_the_cap_threads_share_chunks_before_falling_back() {
    autumn_transport::init_with(TransportKind::Ucx);
    assert!(set_regpool_cap_bytes(64 << 20), "this process sets the cap first");
    // One 64 MiB chunk: the whole budget, claimed by this thread.
    let held = regpool_acquire(32 << 20);
    assert!(held.reg().is_some(), "the first chunk is registered");
    assert_eq!(autumn_transport::regpool_snapshot().registered_bytes, 64 << 20);

    // Another thread can make no chunk of its own; it shares this one.
    let shared = std::thread::spawn(|| regpool_acquire(16 << 20)).join().unwrap();
    assert!(shared.reg().is_some(), "a claimed chunk with room is shared at the cap");

    // 16 MiB left, 32 asked: nothing fits, the buffer is unregistered.
    let over = autumn_transport::regpool_snapshot().over_cap_total;
    let heap = regpool_acquire(32 << 20);
    assert!(heap.reg().is_none());
    assert_eq!(autumn_transport::regpool_snapshot().over_cap_total, over + 1);

    // The shared buffer comes back from the other thread: room again.
    drop(shared);
    let again = regpool_acquire(32 << 20);
    assert!(again.reg().is_some(), "a range freed on another thread is reusable");
    assert_eq!(autumn_transport::regpool_snapshot().registered_bytes, 64 << 20, "no chunk past the cap");
    drop((held, heap, again));
}
