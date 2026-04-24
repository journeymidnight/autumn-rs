use autumn_transport::{current, init, TransportKind};

#[test]
fn init_returns_tcp_by_default() {
    let t = init();
    assert_eq!(t.kind(), TransportKind::Tcp);
    // current() must return the same kind once init() has run.
    assert_eq!(current().kind(), TransportKind::Tcp);
    // Calling init() again is idempotent — same kind.
    let t2 = init();
    assert_eq!(t2.kind(), TransportKind::Tcp);
}
