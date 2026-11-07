//! Ports for tests that start servers by address.
//!
//! A port picked by binding `:0` and dropping the listener is free only at
//! that instant: until the server binds it, another test process can be handed
//! the same port and bind it first (32 processes picking and binding in
//! parallel lost 0.11% of their binds), and the test then talks to the other
//! test's server. A port here comes from below the kernel's ephemeral range,
//! which neither `bind(:0)` nor an outbound connection hands out, and is held
//! by a lock on `/tmp/autumn-test-ports/<port>` until the process exits, so no
//! other test process takes it.
//!
//! Each pick holds `BLOCK` consecutive ports: a partition server listens on
//! its base port plus one per partition. `port_with_control` also holds the
//! block 1000 above, where an extent node binary puts its control listener.

use std::fs::{File, OpenOptions};
use std::hash::{BuildHasher, Hasher};
use std::net::{SocketAddr, TcpListener};
use std::sync::Mutex;

/// Ports held from each picked port up.
pub const BLOCK: u16 = 8;
/// The offset of an extent node's control listener.
const CONTROL: u16 = 1000;
const LOW: u16 = 2000;
const DIR: &str = "/tmp/autumn-test-ports";

static HELD: Mutex<Vec<File>> = Mutex::new(Vec::new());

/// A loopback address on a port this process holds.
pub fn addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port()))
}

/// A port this process holds, with the `BLOCK - 1` above it.
pub fn port() -> u16 {
    reserve(&(0..BLOCK).collect::<Vec<_>>())
}

/// `port`, also holding the same block 1000 higher.
pub fn port_with_control() -> u16 {
    reserve(&(0..BLOCK).chain(CONTROL..CONTROL + BLOCK).collect::<Vec<_>>())
}

/// A base port this process holds together with `base + o` for every offset
/// in `offsets` (which should include 0).
pub fn reserve(offsets: &[u16]) -> u16 {
    let span = offsets.iter().copied().max().unwrap_or(0);
    let high = ephemeral_floor().saturating_sub(span + 1);
    assert!(high > LOW, "no room below the ephemeral range for a span of {span}");
    std::fs::create_dir_all(DIR).expect("create the test port lock dir");
    for _ in 0..10_000 {
        let base = LOW + (random() % u64::from(high - LOW)) as u16;
        if let Some(locks) = hold(base, offsets) {
            HELD.lock().unwrap().extend(locks);
            return base;
        }
    }
    panic!("no free test ports for offsets {offsets:?} in {LOW}..{high}");
}

/// The locks of `base + o` for every offset, if each is free; none held
/// otherwise.
fn hold(base: u16, offsets: &[u16]) -> Option<Vec<File>> {
    let mut locks = Vec::with_capacity(offsets.len());
    for p in offsets.iter().map(|o| base + o) {
        let f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(format!("{DIR}/{p}"))
            .expect("open a test port lock");
        // Held by another test process (or another pick of this one).
        f.try_lock().ok()?;
        // In use by something that is not a test.
        TcpListener::bind(("127.0.0.1", p)).ok()?;
        locks.push(f);
    }
    Some(locks)
}

/// The first port the kernel hands out for `bind(:0)` and outbound
/// connections.
fn ephemeral_floor() -> u16 {
    std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range")
        .ok()
        .and_then(|s| s.split_whitespace().next()?.parse().ok())
        .unwrap_or(32768)
}

fn random() -> u64 {
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos());
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn picks_in_one_process_never_overlap() {
        let mut taken = std::collections::HashSet::new();
        for i in 0..50 {
            let (p, control) = if i % 2 == 0 { (port(), false) } else { (port_with_control(), true) };
            let above = if control { p + CONTROL..p + CONTROL + BLOCK } else { 0..0 };
            for q in (p..p + BLOCK).chain(above) {
                assert!(taken.insert(q), "port {q} handed out twice");
            }
        }
    }

    #[test]
    fn a_held_port_is_refused_to_another_lock() {
        let p = port();
        let f = OpenOptions::new().write(true).open(format!("{DIR}/{p}")).unwrap();
        assert!(f.try_lock().is_err());
        assert!(TcpListener::bind(("127.0.0.1", p)).is_ok(), "held, not bound: the server binds it");
    }
}
