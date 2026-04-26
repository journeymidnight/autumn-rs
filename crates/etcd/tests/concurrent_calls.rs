//! F108 regression: `EtcdClient` must be safe to use from multiple
//! concurrently in-flight `compio` tasks on a single-threaded runtime.
//!
//! Pre-F108, `unary_call` did `self.channel.borrow_mut().call(...).await`,
//! holding a `RefMut<GrpcChannel>` across the await point. The second
//! concurrent caller on the same runtime would then panic with
//! `already borrowed: BorrowMutError` when its `borrow_mut()` fired.
//!
//! Repro on the production cluster: 4 partitions racing on
//! `autumn-client gc <part>` → 4 concurrent `handle_stream_punch_holes`
//! invocations on the manager → 4 concurrent `EtcdClient::txn` → panic on
//! the second one. Workaround was "GC one partition at a time"; F108
//! removes the workaround.
//!
//! Requires etcd running at `127.0.0.1:2379` (matches the convention used
//! by `crates/manager/tests/`). Test prints a skip notice and returns
//! success if etcd is unreachable.

use std::rc::Rc;
use std::time::Duration;

use autumn_etcd::EtcdClient;

const ETCD_ENDPOINT: &str = "127.0.0.1:2379";

async fn etcd_or_skip(test_name: &str) -> Option<EtcdClient> {
    match EtcdClient::connect(ETCD_ENDPOINT).await {
        Ok(c) => match c.get("__f108_health_check__").await {
            Ok(_) => Some(c),
            Err(e) => {
                eprintln!("[{test_name}] skipped — etcd reachable but get failed: {e}");
                None
            }
        },
        Err(e) => {
            eprintln!("[{test_name}] skipped — etcd not reachable at {ETCD_ENDPOINT}: {e}");
            None
        }
    }
}

/// F108 core regression: 8 concurrent `put` tasks on one EtcdClient must
/// all succeed without panicking on `RefCell::borrow_mut`.
#[compio::test]
async fn concurrent_puts_no_borrow_panic() {
    let Some(client) = etcd_or_skip("concurrent_puts_no_borrow_panic").await else {
        return;
    };
    let client = Rc::new(client);

    // Clean up any previous run.
    for i in 0..8 {
        let _ = client.delete(format!("__f108__/put/{i}")).await;
    }

    let mut handles = Vec::new();
    for i in 0..8 {
        let c = client.clone();
        handles.push(compio::runtime::spawn(async move {
            c.put(format!("__f108__/put/{i}"), format!("value-{i}")).await
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
        let r = client.get(format!("__f108__/put/{i}")).await.unwrap();
        assert_eq!(r.kvs.len(), 1, "key {i} missing");
        assert_eq!(r.kvs[0].value, format!("value-{i}").into_bytes());
    }

    // Cleanup.
    for i in 0..8 {
        let _ = client.delete(format!("__f108__/put/{i}")).await;
    }
}

/// F108 mixed workload: concurrent puts + gets on one client.
#[compio::test]
async fn concurrent_mixed_no_borrow_panic() {
    let Some(client) = etcd_or_skip("concurrent_mixed_no_borrow_panic").await else {
        return;
    };
    let client = Rc::new(client);

    // Pre-populate so reads have something to find.
    for i in 0..4 {
        client
            .put(format!("__f108__/mix/{i}"), format!("seed-{i}"))
            .await
            .expect("seed put");
    }

    let mut handles = Vec::new();
    for i in 0..4 {
        let c = client.clone();
        handles.push(compio::runtime::spawn(async move {
            c.put(format!("__f108__/mix/{i}"), format!("upd-{i}"))
                .await
                .map(|_| ())
        }));
    }
    for i in 0..4 {
        let c = client.clone();
        handles.push(compio::runtime::spawn(async move {
            c.get(format!("__f108__/mix/{i}")).await.map(|_| ())
        }));
    }

    for (i, h) in handles.into_iter().enumerate() {
        h.await
            .unwrap_or_else(|_| panic!("task {i} panicked (likely RefCell borrow)"))
            .unwrap_or_else(|e| panic!("op {i} failed: {e}"));
    }

    for i in 0..4 {
        let _ = client.delete(format!("__f108__/mix/{i}")).await;
    }
}

/// F108 stress: 16 tasks all firing simultaneously hammer the borrow path.
#[compio::test]
async fn concurrent_stress_no_borrow_panic() {
    let Some(client) = etcd_or_skip("concurrent_stress_no_borrow_panic").await else {
        return;
    };
    let client = Rc::new(client);

    let mut handles = Vec::new();
    for i in 0..16 {
        let c = client.clone();
        handles.push(compio::runtime::spawn(async move {
            // Small sleep so the runtime has a chance to interleave the
            // borrow_mut sites if F108 regresses.
            compio::time::sleep(Duration::from_millis(1)).await;
            c.put(
                format!("__f108__/stress/{i}"),
                vec![0xAB; 256], // 256-byte value
            )
            .await
            .map(|_| ())
        }));
    }

    for (i, h) in handles.into_iter().enumerate() {
        h.await
            .unwrap_or_else(|_| panic!("task {i} panicked (likely RefCell borrow)"))
            .unwrap_or_else(|e| panic!("stress put {i} failed: {e}"));
    }

    for i in 0..16 {
        let _ = client.delete(format!("__f108__/stress/{i}")).await;
    }
}
