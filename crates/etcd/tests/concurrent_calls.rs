//! regression: `EtcdClient` must be safe to use from multiple
//! concurrently in-flight `compio` tasks on a single-threaded runtime.
//!
//! Before the fix, `unary_call` did `self.channel.borrow_mut().call(...).await`,
//! holding a `RefMut<GrpcChannel>` across the await point. The second
//! concurrent caller on the same runtime would then panic with
//! `already borrowed: BorrowMutError` when its `borrow_mut()` fired.
//!
//! Repro: 4 partitions racing on
//! `autumn-op gc <part>` → 4 concurrent `handle_stream_punch_holes`
//! invocations on the manager → 4 concurrent `EtcdClient::txn` → panic on
//! the second one. Workaround was "GC one partition at a time"; the fix
//! removes the workaround.
//!
//! Each test spawns its own `etcd` (on `$PATH`, or `AUTUMN_TEST_ETCD_BIN`)
//! and fails if it cannot.

use std::rc::Rc;
use std::time::{Duration, Instant};

use autumn_etcd::EtcdClient;

struct Etcd {
    child: std::process::Child,
    _dir: tempfile::TempDir,
}

impl Drop for Etcd {
    fn drop(&mut self) {
        if let Err(e) = self.child.kill() {
            eprintln!("kill etcd: {e}");
        }
        if let Err(e) = self.child.wait() {
            eprintln!("wait etcd: {e}");
        }
    }
}

fn free_port() -> u16 {
    autumn_test_ports::port()
}

/// Whether process `pid` holds the socket listening on 127.0.0.1:`port`:
/// tells our etcd from another test process's server that took the port
/// after `free_port` released it.
fn listens_on(pid: u32, port: u16) -> bool {
    let tcp = std::fs::read_to_string("/proc/net/tcp").unwrap_or_default();
    let want = format!("0100007F:{port:04X}");
    let inodes: std::collections::HashSet<String> = tcp
        .lines()
        .skip(1)
        .filter_map(|l| {
            let f: Vec<&str> = l.split_whitespace().collect();
            (f.len() > 9 && f[1] == want && f[3] == "0A").then(|| format!("socket:[{}]", f[9]))
        })
        .collect();
    std::fs::read_dir(format!("/proc/{pid}/fd")).into_iter().flatten().flatten().any(|fd| {
        std::fs::read_link(fd.path()).is_ok_and(|t| inodes.contains(t.to_string_lossy().as_ref()))
    })
}

/// Ready when the child itself listens on the client port and answers; a
/// child that exits first lost a port and is started again on fresh ones.
async fn start_etcd() -> (Etcd, EtcdClient) {
    let bin = std::env::var("AUTUMN_TEST_ETCD_BIN").unwrap_or_else(|_| "etcd".to_string());
    for _ in 0..5 {
        let client_port = free_port();
        let client_url = format!("http://127.0.0.1:{client_port}");
        let peer_url = format!("http://127.0.0.1:{}", free_port());
        let dir = tempfile::tempdir().expect("etcd tempdir");
        let child = std::process::Command::new(&bin)
            .args(["--name", "n1", "--log-level", "error"])
            .arg("--data-dir")
            .arg(dir.path().join("data"))
            .args(["--listen-client-urls", &client_url, "--advertise-client-urls", &client_url])
            .args(["--listen-peer-urls", &peer_url, "--initial-advertise-peer-urls", &peer_url])
            .arg("--initial-cluster")
            .arg(format!("n1={peer_url}"))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn `{bin}`: {e} — install etcd or set AUTUMN_TEST_ETCD_BIN"));
        let mut etcd = Etcd { child, _dir: dir };
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if etcd.child.try_wait().expect("etcd child status").is_some() {
                break; // lost a port: try fresh ones
            }
            if listens_on(etcd.child.id(), client_port) {
                if let Ok(c) = EtcdClient::connect(&client_url).await {
                    if c.get("health-check").await.is_ok() {
                        return (etcd, c);
                    }
                }
            }
            assert!(Instant::now() < deadline, "etcd at {client_url} not ready after 30 s");
            compio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    panic!("etcd lost its ports to other processes five times");
}

/// core regression: 8 concurrent `put` tasks on one EtcdClient must
/// all succeed without panicking on `RefCell::borrow_mut`.
#[compio::test]
async fn concurrent_puts_no_borrow_panic() {
    let (_etcd, client) = start_etcd().await;
    let client = Rc::new(client);

    let mut handles = Vec::new();
    for i in 0..8 {
        let c = client.clone();
        handles.push(compio::runtime::spawn(async move {
            c.put(format!("__concurrent__/put/{i}"), format!("value-{i}"))
                .await
        }));
    }

    for (i, h) in handles.into_iter().enumerate() {
        let resp = h
            .await
            .unwrap_or_else(|_| panic!("task {i} panicked (likely RefCell borrow)"));
        resp.unwrap_or_else(|e| panic!("put {i} failed: {e}"));
    }

    // Verify all writes landed.
    for i in 0..8 {
        let r = client.get(format!("__concurrent__/put/{i}")).await.unwrap();
        assert_eq!(r.kvs.len(), 1, "key {i} missing");
        assert_eq!(r.kvs[0].value, format!("value-{i}").into_bytes());
    }
}

/// mixed workload: concurrent puts + gets on one client.
#[compio::test]
async fn concurrent_mixed_no_borrow_panic() {
    let (_etcd, client) = start_etcd().await;
    let client = Rc::new(client);

    // Pre-populate so reads have something to find.
    for i in 0..4 {
        client
            .put(format!("__concurrent__/mix/{i}"), format!("seed-{i}"))
            .await
            .expect("seed put");
    }

    let mut handles = Vec::new();
    for i in 0..4 {
        let c = client.clone();
        handles.push(compio::runtime::spawn(async move {
            c.put(format!("__concurrent__/mix/{i}"), format!("upd-{i}"))
                .await
                .map(|_| ())
        }));
    }
    for i in 0..4 {
        let c = client.clone();
        handles.push(compio::runtime::spawn(async move {
            c.get(format!("__concurrent__/mix/{i}")).await.map(|_| ())
        }));
    }

    for (i, h) in handles.into_iter().enumerate() {
        h.await
            .unwrap_or_else(|_| panic!("task {i} panicked (likely RefCell borrow)"))
            .unwrap_or_else(|e| panic!("op {i} failed: {e}"));
    }
}

/// `listens_on` names the process holding the listening socket, and only it:
/// what tells our etcd from another process on a port it lost.
#[test]
fn listens_on_names_only_the_listening_process() {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = l.local_addr().expect("local_addr").port();
    let mut other = std::process::Command::new("sleep").arg("30").spawn().expect("spawn sleep");
    assert!(listens_on(std::process::id(), port), "this process listens");
    assert!(!listens_on(other.id(), port), "another process does not");
    drop(l);
    assert!(!listens_on(std::process::id(), port), "nobody listens once it is closed");
    other.kill().expect("kill sleep");
    other.wait().expect("wait sleep");
}
