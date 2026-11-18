//! `autumn-s3` at the HTTP level, against the real binary
//! (`target/debug/autumn-s3`) and an in-process cluster:
//! - a bucket addressed as `/{bucket}/`, the form minio-go (warp, mc) sends,
//!   is the bucket;
//! - small responses are not held back by Nagle: sequential GETs on one
//!   keep-alive connection do not each wait for the client's delayed ACK;
//! - a GET of an inline object reads it whole and takes no lease, while a
//!   larger object is pinned for its body.

mod support;

use std::ffi::OsStr;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use autumn_fs::state::FsState;
use autumn_client::lease::{self, AcquireResult, DaemonClientId};
use autumn_fs::{dir, meta};
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::LEASE_MODE_EXCLUSIVE;

use support::*;

struct Gateway(std::process::Child);

impl Drop for Gateway {
    fn drop(&mut self) {
        let _killed = self.0.kill();
        let _reaped = self.0.wait();
    }
}

/// A cluster with an empty bucket `b`, and the gateway serving it.
fn gateway(base: u16) -> (Gateway, SocketAddr, SocketAddr, (tempfile::TempDir, tempfile::TempDir)) {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (n1, n2) = (pick_addr(), pick_addr());
    start_extent_node(n1, dirs.0.path().to_path_buf(), 1);
    start_extent_node(n2, dirs.1.path().to_path_buf(), 2);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1, n2, base).await;
        let (log, row, meta_stream) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, base as u64 * 100 + 1, log, row, meta_stream, b"", b"\xff\xff\xff\xff").await;
        start_partition_server(base as u64, mgr_addr, pick_addr());
        compio::time::sleep(Duration::from_millis(1500)).await;
        create_namespaces(mgr_addr, &["fs"]).await;
        let mut st = FsState::new(&mgr_addr.to_string()).await.unwrap();
        meta::ensure_root(&mut st).await.unwrap();
        dir::mkdir(&mut st, 1, OsStr::new("b"), 0o755).await.unwrap();
    });

    let addr = pick_addr();
    let gw = Gateway(
        std::process::Command::new(support_binary_path("autumn-s3"))
            .args(["--manager", &mgr_addr.to_string(), "--listen", "127.0.0.1", "--port", &addr.port().to_string(), "--workers", "1"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn autumn-s3"),
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    while TcpStream::connect(addr).is_err() {
        assert!(Instant::now() < deadline, "autumn-s3 never listened");
        std::thread::sleep(Duration::from_millis(200));
    }
    (gw, addr, mgr_addr, dirs)
}

/// One request on a fresh connection; the status code and the raw response.
fn http(addr: SocketAddr, method: &str, path: &str) -> (u16, String) {
    let mut c = TcpStream::connect(addr).unwrap();
    write!(c, "{method} {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").unwrap();
    let mut raw = Vec::new();
    c.read_to_end(&mut raw).unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    let code = text.split_whitespace().nth(1).unwrap().parse().unwrap();
    (code, text)
}

/// One request on a kept-alive connection; the status code and the body
/// (read by its `Content-Length`).
fn keep_alive(c: &mut TcpStream, head: &str, body: &[u8]) -> (u16, Vec<u8>) {
    c.write_all(head.as_bytes()).unwrap();
    c.write_all(body).unwrap();
    let mut raw = Vec::new();
    let mut byte = [0u8; 1];
    while !raw.ends_with(b"\r\n\r\n") {
        c.read_exact(&mut byte).unwrap();
        raw.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&raw).to_ascii_lowercase();
    let code = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    let len = head
        .lines()
        .find_map(|l| l.strip_prefix("content-length:"))
        .map_or(0, |v| v.trim().parse().unwrap());
    let mut out = vec![0u8; len];
    c.read_exact(&mut out).unwrap();
    (code, out)
}

#[test]
fn a_bucket_with_a_trailing_slash_is_the_bucket() {
    let (_gw, addr, _mgr, _dirs) = gateway(266);
    for path in ["/b", "/b/"] {
        assert_eq!(http(addr, "HEAD", path).0, 200, "HEAD {path}");
        let (code, text) = http(addr, "GET", &format!("{path}?list-type=2"));
        assert_eq!(code, 200, "GET {path}: {text}");
        assert!(text.contains("<ListBucketResult"), "GET {path}: {text}");
    }
    assert_eq!(http(addr, "HEAD", "/missing/").0, 404);
    // A bucket subresource is refused, not answered with a listing.
    for q in ["location=", "versioning=", "uploads=", "marker=a", "list-type=1"] {
        let (code, text) = http(addr, "GET", &format!("/b/?{q}"));
        assert_eq!(code, 501, "GET /b/?{q}: {text}");
    }
}

/// With Nagle on, each small GET's response waited ~40 ms for the client's
/// delayed ACK. The median of 20 stays far below that floor.
#[test]
fn small_gets_on_one_connection_are_not_delayed() {
    let (_gw, addr, _mgr, _dirs) = gateway(267);
    let mut c = TcpStream::connect(addr).unwrap();
    let body: Vec<u8> = (0..4096u32).map(|i| i as u8).collect();
    let put = format!("PUT /b/small HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n", body.len());
    assert_eq!(keep_alive(&mut c, &put, &body).0, 200);

    let get = "GET /b/small HTTP/1.1\r\nHost: x\r\n\r\n";
    assert_eq!(keep_alive(&mut c, get, b""), (200, body.clone()));
    let mut took: Vec<Duration> = (0..20)
        .map(|_| {
            let started = Instant::now();
            assert_eq!(keep_alive(&mut c, get, b""), (200, body.clone()));
            started.elapsed()
        })
        .collect();
    took.sort();
    assert!(took[10] < Duration::from_millis(25), "median small GET took {:?} ({took:?})", took[10]);
}

/// An object of at most 4096 bytes lives in its inode: the GET reads it whole
/// in one get and pins nothing, so another client can take the file at once.
/// One byte larger, the body is streamed under a STABLE pin that lingers
/// after the GET (`--pin-linger-ms`, 2 s), and the same acquire is refused.
#[test]
fn an_inline_get_takes_no_lease() {
    let (_gw, addr, mgr, _dirs) = gateway(268);
    let mut c = TcpStream::connect(addr).unwrap();
    for (name, n) in [("small", 4096usize), ("big", 4097)] {
        let body: Vec<u8> = (0..n).map(|i| (i * 7) as u8).collect();
        let put = format!("PUT /b/{name} HTTP/1.1\r\nHost: x\r\nContent-Length: {n}\r\n\r\n");
        assert_eq!(keep_alive(&mut c, &put, &body).0, 200, "PUT {name}");
        let get = format!("GET /b/{name} HTTP/1.1\r\nHost: x\r\n\r\n");
        assert_eq!(keep_alive(&mut c, &get, b""), (200, body.clone()), "GET {name}");
        let ranged = format!("GET /b/{name} HTTP/1.1\r\nHost: x\r\nRange: bytes=4090-\r\n\r\n");
        assert_eq!(keep_alive(&mut c, &ranged, b""), (206, body[4090..].to_vec()), "ranged GET {name}");
        let suffix = format!("GET /b/{name} HTTP/1.1\r\nHost: x\r\nRange: bytes=-5\r\n\r\n");
        assert_eq!(keep_alive(&mut c, &suffix, b""), (206, body[n - 5..].to_vec()), "suffix GET {name}");
        let past = format!("GET /b/{name} HTTP/1.1\r\nHost: x\r\nRange: bytes={n}-\r\n\r\n");
        assert_eq!(keep_alive(&mut c, &past, b"").0, 416, "unsatisfiable GET {name}");
    }

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut st = FsState::new(&mgr.to_string()).await.unwrap();
        let other = DaemonClientId::new("other");
        for (name, free) in [("small", true), ("big", false)] {
            let get = format!("GET /b/{name} HTTP/1.1\r\nHost: x\r\n\r\n");
            assert_eq!(keep_alive(&mut c, &get, b"").0, 200);
            let ino = dir::resolve(&mut st, &format!("/b/{name}")).await.unwrap().unwrap();
            let granted = match lease::acquire(&st.client, &other, ino, LEASE_MODE_EXCLUSIVE).await.unwrap() {
                AcquireResult::Granted(_) => true,
                AcquireResult::Conflict { .. } | AcquireResult::RevokePending { .. } => false,
            };
            assert_eq!(granted, free, "EXCLUSIVE on {name} right after its GET");
            if granted {
                lease::release(&st.client, &other, ino).await.unwrap();
            }
        }
    });
}

/// The worker caches the directories a GET walks through and never
/// invalidates them; a lookup under a cached directory that finds nothing is
/// redone reading every entry. A prefix directory removed and created again
/// behind the gateway (a new inode under the same name) is therefore found
/// on the first GET after it.
#[test]
fn a_directory_replaced_behind_the_gateway_is_found_again() {
    let (_gw, addr, mgr, _dirs) = gateway(269);
    let mut c = TcpStream::connect(addr).unwrap();
    let put = |c: &mut TcpStream, key: &str, body: &[u8]| {
        let head = format!("PUT /b/{key} HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n", body.len());
        assert_eq!(keep_alive(c, &head, body).0, 200, "PUT {key}");
    };
    let get = |c: &mut TcpStream, key: &str| keep_alive(c, &format!("GET /b/{key} HTTP/1.1\r\nHost: x\r\n\r\n"), b"");

    put(&mut c, "d/x", b"old");
    assert_eq!(get(&mut c, "d/x"), (200, b"old".to_vec()), "caches b/ and d/");
    assert_eq!(keep_alive(&mut c, "DELETE /b/d/x HTTP/1.1\r\nHost: x\r\n\r\n", b"").0, 204);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut st = FsState::new(&mgr.to_string()).await.unwrap();
        let b = dir::resolve(&mut st, "/b").await.unwrap().unwrap();
        dir::rmdir(&mut st, b, OsStr::new("d")).await.unwrap();
    });
    put(&mut c, "d/y", b"new");
    assert_eq!(get(&mut c, "d/y"), (200, b"new".to_vec()), "found under the new d/");
    assert_eq!(http(addr, "HEAD", "/b/d/y").0, 200, "HEAD too");
}

/// A cached directory's name taken over by a file: the directory renamed
/// away behind the gateway (it still exists), then a file put under the old
/// name. The last component is never taken from the cache, so the key is the
/// file — it used to resolve to the renamed directory and answer 404 for
/// good, since a directory is no object and nothing ever refreshed it.
#[test]
fn a_cached_directory_name_taken_by_a_file_is_the_file() {
    let (_gw, addr, mgr, _dirs) = gateway(270);
    let mut c = TcpStream::connect(addr).unwrap();
    let put = |c: &mut TcpStream, key: &str, body: &[u8]| {
        let head = format!("PUT /b/{key} HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n", body.len());
        assert_eq!(keep_alive(c, &head, body).0, 200, "PUT {key}");
    };
    let get = |c: &mut TcpStream, key: &str| keep_alive(c, &format!("GET /b/{key} HTTP/1.1\r\nHost: x\r\n\r\n"), b"");

    put(&mut c, "d/x", b"in d");
    assert_eq!(get(&mut c, "d/x").0, 200, "caches b/ and d/");
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut st = FsState::new(&mgr.to_string()).await.unwrap();
        let b = dir::resolve(&mut st, "/b").await.unwrap().unwrap();
        dir::rename(&mut st, b, OsStr::new("d"), b, OsStr::new("old")).await.unwrap();
    });
    put(&mut c, "d", b"a file");
    for _ in 0..2 {
        assert_eq!(get(&mut c, "d"), (200, b"a file".to_vec()));
    }
    assert_eq!(http(addr, "HEAD", "/b/d").0, 200);
    assert_eq!(get(&mut c, "old/x"), (200, b"in d".to_vec()), "the renamed directory");
}
