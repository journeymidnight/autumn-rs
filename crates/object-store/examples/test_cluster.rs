//! Standalone disposable RF2 cluster for integration demos. No fixed ports and
//! no existing data directories. Terminate this process to stop its services.
#[path = "../../manager/tests/support/mod.rs"]
mod support;

use support::*;

fn main() {
    let info = std::env::args()
        .nth(1)
        .expect("usage: test_cluster OUTPUT_JSON");
    let mgr = pick_addr();
    start_manager(mgr);
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let en1 = pick_addr();
    let en2 = pick_addr();
    start_extent_node(en1, dirs[0].path().to_path_buf(), 1);
    start_extent_node(en2, dirs[1].path().to_path_buf(), 2);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let client = autumn_rpc::client::RpcClient::connect(mgr).await.unwrap();
        register_two_nodes(&client, en1, en2, 410).await;
        let (log, row, meta) = create_three_streams(&client).await;
        upsert_partition(&client, 901, log, row, meta, b"", b"").await;
    });
    let ps = pick_addr();
    start_partition_server(415, mgr, ps);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        PsRouter::new(mgr, ps).client_for(901).await;
    });
    std::fs::write(
        info,
        serde_json::to_vec(&serde_json::json!({
            "manager": mgr.to_string(), "pid": std::process::id(),
            "scope": "fs/objects/lancedb-demo"
        }))
        .unwrap(),
    )
    .unwrap();
    loop {
        std::thread::park();
    }
}
