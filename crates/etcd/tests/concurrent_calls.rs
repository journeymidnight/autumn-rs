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
//! Each test spawns its own `etcd` (`common::start_etcd`).

mod common;

use std::rc::Rc;

async fn start_etcd() -> (common::Etcd, autumn_etcd::EtcdClient) {
    let (etcd, _addr, client) =
        common::start_etcd(&[], "http", |addr| autumn_etcd::EtcdConfig::plain(&[addr])).await;
    (etcd, client)
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
    assert!(common::listens_on(std::process::id(), port), "this process listens");
    assert!(!common::listens_on(other.id(), port), "another process does not");
    drop(l);
    assert!(!common::listens_on(std::process::id(), port), "nobody listens once it is closed");
    other.kill().expect("kill sleep");
    other.wait().expect("wait sleep");
}
