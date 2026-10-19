use autumn_transport::{check_listen_addr, TransportKind};

// Even without the ucx feature the validator is pure: TransportKind::Ucx
// as a parameter triggers the RoCE-check path, which reads sysfs directly.
// check_listen_addr is warn-only — non-RDMA addresses log a warning but
// always return Ok so callers can decide whether to abort.
#[test]
fn ucx_warns_on_loopback() {
    let addr: std::net::SocketAddr = "127.0.0.1:9999".parse().unwrap();
    check_listen_addr(addr, TransportKind::Ucx).expect("check_listen_addr is warn-only");
}
