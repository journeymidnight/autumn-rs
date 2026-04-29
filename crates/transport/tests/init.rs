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
