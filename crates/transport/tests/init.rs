use autumn_transport::{current, init_with, TransportKind};

#[test]
fn init_with_tcp_is_idempotent() {
    let t = init_with(TransportKind::Tcp);
    assert_eq!(t.kind(), TransportKind::Tcp);
    // current() must return the same kind once init_with() has run.
    assert_eq!(current().kind(), TransportKind::Tcp);
    // Calling init_with() again is a no-op — first call wins.
    let t2 = init_with(TransportKind::Tcp);
    assert_eq!(t2.kind(), TransportKind::Tcp);
}

#[test]
fn tcp_recycles_buffers_including_in_a_ucx_capable_binary() {
    init_with(TransportKind::Tcp);
    // Pool state is thread-local, so other tests cannot supply the hit.
    std::thread::spawn(|| {
        let before = autumn_transport::regpool_snapshot();
        let mut first = autumn_transport::regpool_acquire(200_000);
        first.dest_mut().fill(0xa5);
        assert!(first.reg().is_none(), "TCP must not register memory");
        let addr = first.filled().as_ptr();
        drop(first);
        let next = autumn_transport::regpool_acquire(250_000);
        assert_eq!(next.filled().as_ptr(), addr);
        assert!(next.filled()[..200_000].iter().all(|&b| b == 0xa5));
        let after = autumn_transport::regpool_snapshot();
        assert!(
            after.hit_total > before.hit_total,
            "must reuse, not reallocate at the same address"
        );
        assert_eq!(after.registered_bytes, 0);
    })
    .join()
    .unwrap();
}
