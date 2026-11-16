//! Startup resolution of server address flags.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use autumn_transport::{resolve_advertise, resolve_host, resolve_host_port, resolve_host_port_list};

#[test]
fn literals_are_taken_as_written() {
    let v4: SocketAddr = "10.1.2.3:9001".parse().unwrap();
    let v6: SocketAddr = "[fd00::7]:9001".parse().unwrap();
    assert_eq!(resolve_host_port("10.1.2.3:9001").unwrap(), vec![v4]);
    assert_eq!(resolve_host_port("http://10.1.2.3:9001").unwrap(), vec![v4]);
    assert_eq!(resolve_host_port("[fd00::7]:9001").unwrap(), vec![v6]);
    assert_eq!(resolve_host("fd00::7").unwrap(), v6.ip());
    assert_eq!(resolve_host("[fd00::7]").unwrap(), v6.ip());
    assert_eq!(resolve_host("0.0.0.0").unwrap(), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
}

#[test]
fn a_name_resolves_to_its_addresses() {
    let addrs = resolve_host_port("localhost:9001").unwrap();
    assert!(!addrs.is_empty());
    assert!(addrs.iter().all(|a| a.ip().is_loopback() && a.port() == 9001), "{addrs:?}");
    assert!(resolve_host("localhost").unwrap().is_loopback());
}

#[test]
fn a_list_expands_names_and_drops_duplicates() {
    let list = resolve_host_port_list("127.0.0.1:9001,localhost:9001,127.0.0.1:9002").unwrap();
    let local = resolve_host_port("localhost:9001").unwrap();
    assert_eq!(list[0], "127.0.0.1:9001".parse::<SocketAddr>().unwrap());
    assert_eq!(*list.last().unwrap(), "127.0.0.1:9002".parse::<SocketAddr>().unwrap());
    for a in &local {
        assert_eq!(list.iter().filter(|x| *x == a).count(), 1, "{list:?}");
    }
}

#[test]
fn an_advertised_name_takes_the_listen_family() {
    let v4 = resolve_advertise("localhost:9201", IpAddr::V4(Ipv4Addr::UNSPECIFIED)).unwrap();
    assert_eq!(v4, "127.0.0.1:9201".parse::<SocketAddr>().unwrap());
    let lit = resolve_advertise("[fd00::7]:9201", IpAddr::V4(Ipv4Addr::UNSPECIFIED)).unwrap();
    assert_eq!(lit, "[fd00::7]:9201".parse::<SocketAddr>().unwrap());
}

#[test]
fn an_unknown_name_is_an_error() {
    let e = resolve_host_port("no-such-host.invalid:9001").unwrap_err();
    assert!(e.to_string().contains("no-such-host.invalid"), "{e}");
    assert!(resolve_host("no-such-host.invalid").is_err());
    assert!(resolve_host_port("10.1.2.3").is_err());
}
