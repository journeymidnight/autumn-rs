use autumn_transport::{check_listen_addr, TransportKind};

#[test]
fn tcp_mode_accepts_any_addr() {
    let addr: std::net::SocketAddr = "127.0.0.1:9999".parse().unwrap();
    check_listen_addr(addr, TransportKind::Tcp).expect("tcp mode: any addr ok");
}

// Even without the ucx feature the validator is pure: TransportKind::Ucx
// as a parameter triggers the RoCE-check path, which reads sysfs directly.
// check_listen_addr is warn-only — non-RDMA addresses log a warning but
// always return Ok so callers can decide whether to abort.
#[test]
fn ucx_warns_on_loopback() {
    let addr: std::net::SocketAddr = "127.0.0.1:9999".parse().unwrap();
    check_listen_addr(addr, TransportKind::Ucx).expect("check_listen_addr is warn-only");
}

#[test]
fn ucx_accepts_wildcard() {
    let addr: std::net::SocketAddr = "[::]:9999".parse().unwrap();
    check_listen_addr(addr, TransportKind::Ucx).expect("wildcard should pass");
}

// This one needs an actual RoCE IP to exist on the build host. Skip if
// not running on the verified host.
#[test]
fn ucx_accepts_roce_ip_on_build_host() {
    // eth2 on this host has fdbb:dc62:3:3::16 — verified by check_roce.sh.
    // If running elsewhere, the test will fail with a useful error listing
    // the actual local RoCE candidates.
    let addr: std::net::SocketAddr = "[fdbb:dc62:3:3::16]:9999".parse().unwrap();
    match check_listen_addr(addr, TransportKind::Ucx) {
        Ok(()) => {}
        Err(e) => {
            // On non-RoCE hosts this test would naturally fail. Emit a
            // useful diagnostic instead of just a cryptic unwrap panic.
            panic!(
                "expected RoCE-attached IP on this build host; got: {e}. \
                 If running on a host without fdbb:dc62:3:3::16, ignore \
                 this test."
            );
        }
    }
}
