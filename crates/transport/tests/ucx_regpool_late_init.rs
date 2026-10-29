//! A chunk made before the process transport became UCX (a buffer acquired
//! before `init_with`) is registered on its first use after it, instead of
//! leaving every later buffer carved from it unregistered.

#![cfg(feature = "ucx")]

use autumn_transport::{regpool_acquire, TransportKind};

#[test]
fn a_chunk_made_before_ucx_init_is_registered_on_first_use_after() {
    let early = regpool_acquire(1 << 20);
    assert!(early.reg().is_none(), "no transport yet: nothing to register with");
    assert_eq!(autumn_transport::regpool_snapshot().registered_bytes, 0);

    autumn_transport::init_with(TransportKind::Ucx);
    let late = regpool_acquire(1 << 20);
    assert!(late.reg().is_some(), "the same chunk, registered now");
    assert!(early.reg().is_some(), "the registration covers the earlier buffer too");
    assert!(autumn_transport::regpool_snapshot().registered_bytes > 0);
}
