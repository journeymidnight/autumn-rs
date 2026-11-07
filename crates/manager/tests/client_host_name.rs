//! The client dials a host name, not only an IP literal: the manager address
//! it is given and the partition addresses the manager hands back. Only the
//! client dials the name here: the manager's own dial of a PS advertise
//! address still takes an IP literal (nothing in this test makes it dial).

mod support;

use std::net::SocketAddr;
use std::time::Duration;

use autumn_client::{ClusterClient, WriteOptions};
use autumn_partition_server::PartitionServer;
use autumn_rpc::client::RpcClient;

use support::*;

/// A PS that advertises `localhost:<port>`, so its partitions are routed to
/// a host name.
fn start_ps_advertising_localhost(ps_id: u64, mgr_addr: SocketAddr, ps_addr: SocketAddr) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let advertise = format!("localhost:{}", ps_addr.port());
            let ps = PartitionServer::connect_with_advertise_and_port(
                ps_id,
                &mgr_addr.to_string(),
                Some(advertise),
                ps_addr,
            )
            .await
            .expect("connect partition server");
            ps.sync_regions_once().await.expect("sync regions");
            if let Err(e) = ps.serve(ps_addr).await {
                panic!("partition server serve: {e:#}");
            }
        });
    });
    wait_accepting(ps_addr, "partition server");
}

#[test]
fn a_client_reaches_the_cluster_by_host_name() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 150);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 151);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None)
            .await
            .unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 150).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 15001, log, row, meta, b"", b"\xff\xff\xff\xff").await;
        create_namespaces(mgr_addr, &["t"]).await;

        let ps_addr = pick_addr();
        start_ps_advertising_localhost(150, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(1500)).await;

        let client = ClusterClient::connect(&format!("localhost:{}", mgr_addr.port()), "t")
            .await
            .expect("connect to the manager by host name");
        client.set_rpc_timeout(Duration::from_secs(15));
        let parts = client.all_partitions().await.expect("partitions");
        assert!(
            parts.iter().all(|(_, a)| a.starts_with("localhost:")),
            "routing should name the PS by host name: {parts:?}"
        );
        client.put(b"k", b"v", WriteOptions::default()).await.expect("put through a host-named PS");
        assert_eq!(client.get(b"k").await.unwrap().as_deref(), Some(&b"v"[..]));
    });
}
