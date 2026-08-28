//! D7 (SD-2) — wire-level integration tests for the two subtle
//! correctness points the unit tests only cover in isolation:
//!
//! 1. **Layer-A over the wire** (`layer_a_rejects_unregistered_namespace_put`):
//!    with a namespace registered, a put under it is admitted, a put under an
//!    UNregistered prefix is rejected with `NamespaceUnknown` (anonymous
//!    connection checked too), and a `delete` under an unregistered prefix is
//!    NOT Layer-A gated.
//! 2. **put-stream chunk-in-tenant-range** (`putstream_chunks_land_in_tenant_range`):
//!    a Prepend-bound client's striped chunk keys land INSIDE
//!    `[{tenant}/{ns}/, {tenant}/{ns}0)`, not in a global `\xff\xfe…` space.
//!
//! Both need EN binaries + a real PS, so they are `#[ignore]` (run explicitly),
//! mirroring `system_putstream.rs`.

mod support;

use std::net::SocketAddr;
use std::time::Duration;

use autumn_client::{AutumnError, ClusterClient};
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, NamespaceCreateReq, NamespaceCreateResp, CODE_OK,
    MSG_NAMESPACE_CREATE,
};

use support::*;

const ADMIN: &str = "layer-a-admin";

/// A memory-mode manager WITH an admin token (namespace-create is admin-gated).
fn start_manager_with_admin(mgr_addr: SocketAddr) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = autumn_manager::AutumnManager::new();
            manager.set_admin_token(ADMIN.to_string());
            let _ = manager.serve(mgr_addr).await;
        });
    });
    std::thread::sleep(Duration::from_millis(200));
}

async fn ns_create(mgr: &RpcClient, name: &str) {
    let payload = rkyv_encode(&NamespaceCreateReq {
        admin_token: ADMIN.to_string(),
        name: name.to_string(),
        owner_tenant: None,
        presplit: Vec::new(),
    });
    let resp = mgr
        .call(MSG_NAMESPACE_CREATE, payload)
        .await
        .expect("namespace-create rpc");
    let r: NamespaceCreateResp = rkyv_decode(&resp).expect("decode");
    assert_eq!(r.code, CODE_OK, "namespace-create failed: {}", r.message);
}

#[test]
#[ignore] // needs EN binaries + a real PS
fn layer_a_rejects_unregistered_namespace_put() {
    let mgr_addr = pick_addr();
    start_manager_with_admin(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 130);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 131);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 130).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 13001, log, row, meta, b"", b"\xff\xff\xff\xff").await;

        // Register `bench` BEFORE the PS starts so its initial authz-config fetch
        // sees a non-empty registry → Layer-A enabled (independent of any signing
        // key). Then bring up the PS.
        ns_create(&mgr, "bench").await;

        let ps_addr = pick_addr();
        start_partition_server(130, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(1800)).await;

        // scoped client `"bench/perf"` prepends `bench/perf/`.
        // A put of `k1` → wire `bench/perf/k1` → 1st segment `bench/` registered →
        // ADMITTED.
        let scoped = ClusterClient::connect(&mgr_addr.to_string(), "bench/perf")
            .await
            .expect("scoped connect");
        scoped.set_rpc_timeout(Duration::from_secs(15));
        scoped.put(b"k1", b"v1").await.expect("put in registered ns");
        assert_eq!(scoped.get(b"k1").await.unwrap().as_deref(), Some(&b"v1"[..]));

        // RAW (anonymous, no client clamp) put of an ABSOLUTE key whose 1st
        // segment is an UNregistered namespace → Layer-A rejects with
        // NamespaceUnknown. Anonymous connection is checked too (token-free).
        let raw = ClusterClient::connect_raw(&mgr_addr.to_string())
            .await
            .expect("raw connect");
        raw.set_rpc_timeout(Duration::from_secs(15));
        let err = raw.put(b"unreg/x", b"nope").await.unwrap_err();
        assert!(
            matches!(err, AutumnError::NamespaceUnknown(_)),
            "put to unregistered namespace must be NamespaceUnknown, got {err:?}"
        );

        // A put whose 1st segment IS registered (`bench/…`) via the raw
        // client is admitted.
        raw.put(b"bench/raw-ok", b"ok")
            .await
            .expect("raw put under registered ns");

        // DELETE is NOT Layer-A gated (Layer-A is put-class only) → succeeds
        // (delete of an absent key is OK).
        raw.delete(b"unreg/x")
            .await
            .expect("delete under unregistered ns is not Layer-A gated");
    });
}

#[test]
#[ignore] // needs EN binaries + a real PS
fn putstream_chunks_land_in_tenant_range() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr); // no namespace registered → Layer-A OFF (not under test)
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 140);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 141);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 140).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 14001, log, row, meta, b"", b"\xff\xff\xff\xff").await;
        let ps_addr = pick_addr();
        start_partition_server(140, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(1800)).await;

        // Prepend-bound client on `bench/perf/`.
        let scoped = ClusterClient::connect(&mgr_addr.to_string(), "bench/perf")
            .await
            .expect("scoped connect");
        scoped.set_rpc_timeout(Duration::from_secs(20));

        // Stripe-put a large value → chunks go through the leaf `put`, so each
        // chunk key `\xff\xfe…++bigfile` is prefixed ONCE with `bench/perf/`.
        let value = vec![7u8; 3 * 1024 * 1024]; // 3 MiB → multiple chunks
        let mut h = scoped.put_stream_begin(b"bigfile", 0).with_chunk_size(1024 * 1024);
        h.send(&value).await.expect("send");
        h.commit().await.expect("commit");

        // A tenant-scoped range scan (empty prefix → the whole `bench/perf/`)
        // sees BOTH the meta key and the striped chunk keys — proving the chunks
        // landed inside the tenant range. The binding strips `bench/perf/`, so
        // the chunk keys come back starting with the `\xff\xfe` chunk prefix.
        let scan = scoped.range(b"", b"", 10_000).await.expect("scoped range");
        let has_meta = scan.entries.iter().any(|e| e.key == b"bigfile");
        let has_chunk = scan
            .entries
            .iter()
            .any(|e| e.key.starts_with(b"\xff\xfe"));
        assert!(has_meta, "meta key `bigfile` must be in the tenant range");
        assert!(
            has_chunk,
            "striped chunk keys (\\xff\\xfe…) must be in the tenant range"
        );

        // On the WIRE the chunk keys are `bench/perf/\xff\xfe…` — verify via a raw
        // client that scanning the GLOBAL `\xff\xfe` space finds NOTHING (the
        // chunks are NOT in a global stripe namespace), while scanning the
        // `bench/perf/` prefix DOES find them.
        let raw = ClusterClient::connect_raw(&mgr_addr.to_string())
            .await
            .expect("raw connect");
        raw.set_rpc_timeout(Duration::from_secs(20));
        let global = raw.range(b"\xff\xfe", b"\xff\xfe", 100).await.expect("raw global range");
        assert!(
            global.entries.is_empty(),
            "no chunk key may live in the GLOBAL \\xff\\xfe space (found {})",
            global.entries.len()
        );
        let in_tenant = raw
            .range(b"bench/perf/", b"bench/perf/", 10_000)
            .await
            .expect("raw tenant range");
        assert!(
            in_tenant
                .entries
                .iter()
                .any(|e| e.key.starts_with(b"bench/perf/\xff\xfe")),
            "chunk keys must be under `bench/perf/\\xff\\xfe…` on the wire"
        );
    });
}
