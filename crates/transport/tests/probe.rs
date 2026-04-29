use autumn_transport::{parse_transport_flag, TransportKind};

#[test]
fn parse_transport_flag_accepts_tcp_and_ucx() {
    assert_eq!(parse_transport_flag("tcp"), Ok(TransportKind::Tcp));
    assert_eq!(parse_transport_flag("ucx"), Ok(TransportKind::Ucx));
}

#[test]
fn parse_transport_flag_rejects_other_strings() {
    assert!(parse_transport_flag("auto").is_err());
    assert!(parse_transport_flag("").is_err());
    assert!(parse_transport_flag("TCP").is_err());
}
