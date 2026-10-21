//! A failed invalidation poll drops the mount's caches, but not a WRITE lease
//! the mount still holds: dropping and releasing the slot handed the lease to
//! the next mount that asked, and the writer's open fd failed every write from
//! then on. Mount A reaches the manager through toxiproxy; the
//! proxy is cut long enough for a poll to fail, then restored. Drives
//! `dispatch::handle_request` on two `FsState`s, no kernel. Spawns
//! `toxiproxy-server` from `$PATH`.

mod support;

use std::time::{Duration, Instant};

use autumn_fs::schema::{self, DirentValue, DT_REG, ROOT_INO};
use autumn_fs::state::FsState;
use autumn_fs::write::{flush_inode, FlushReport};
use autumn_fs::{key, meta};
use autumn_fuse::{bridge, dispatch};
use autumn_rpc::client::RpcClient;

use support::*;

const INO: u64 = 79;
const O_WRONLY: i32 = 1;

async fn seed_file(state: &mut FsState, name: &[u8], ino: u64) {
    let m = meta::new_file_meta(0o644, 0, 0);
    meta::put_new_inode(state, ino, &m).await.expect("put_inode");
    let dk = key::dirent_key(ROOT_INO, name);
    let dv = schema::encode_dirent(&DirentValue { child_inode: ino, file_type: DT_REG });
    state.kv_put(&dk, &dv).await.expect("put dirent");
}

async fn try_open(state: &mut FsState, ino: u64, flags: i32) -> anyhow::Result<(u64, u32)> {
    let (tx, rx) = bridge::reply_channel::<(u64, u32)>();
    dispatch::handle_request(state, bridge::FsRequest::Open { ino, flags, reply: tx }, None).await;
    rx.recv_timeout(Duration::from_secs(30)).expect("open reply")
}

async fn try_write(state: &mut FsState, ino: u64, data: Vec<u8>) -> anyhow::Result<u32> {
    let (tx, rx) = bridge::reply_channel::<u32>();
    dispatch::handle_request(state, bridge::FsRequest::Write { ino, offset: 0, data, reply: tx }, None).await;
    rx.recv_timeout(Duration::from_secs(10)).expect("write reply")
}

#[test]
fn a_failed_poll_keeps_the_write_lease() {
    let _ = tracing_subscriber::fmt().with_env_filter("autumn_fs=warn").try_init();
    let params = autumn_params::manager::ManagerParams::default();
    params.inode_lease_ttl_secs.set(15);
    autumn_fs::params().lease_heartbeat_secs.set(3);
    let mgr_addr = pick_addr();
    start_manager_with(mgr_addr, params);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let (n1_addr, n2_addr) = (pick_addr(), pick_addr());
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 128).await;
        let (log, row, meta_stream) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 12801, log, row, meta_stream, b"", b"\xff\xff\xff\xff").await;
        let ps_addr = pick_addr();
        start_partition_server(128, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(1500)).await;

        let (_toxi_guard, toxi_admin) = start_toxiproxy().await;
        let toxi = ToxiproxyCli::new(toxi_admin);
        let proxy_addr = pick_addr().to_string();
        toxi.create("mgr", &proxy_addr, &mgr_addr.to_string()).expect("create proxy");

        let mut a = FsState::new(&proxy_addr).await.expect("mount a");
        dispatch::spawn_lease_background_tasks(&a, None);
        let mut b = FsState::new(&mgr_addr.to_string()).await.expect("mount b");
        seed_file(&mut a, b"drained.bin", INO).await;

        // A holds the WRITE lease; its first 4096 bytes are durable.
        try_open(&mut a, INO, O_WRONLY).await.expect("a open");
        assert_eq!(try_write(&mut a, INO, vec![0xAAu8; 4096]).await.expect("a write"), 4096);
        flush_inode(&mut a, INO, FlushReport::ToApplication).await.expect("a fsync");
        a.meta_invalidated.borrow_mut().clear();

        // Cut A off the manager until its poll fails and the drain runs.
        toxi.set_enabled("mgr", false).expect("cut");
        let deadline = Instant::now() + Duration::from_secs(40);
        while !a.meta_invalidated.borrow().contains(&INO) {
            assert!(Instant::now() < deadline, "the poll never failed");
            compio::time::sleep(Duration::from_millis(100)).await;
        }
        toxi.set_enabled("mgr", true).expect("restore");

        // Past the lease TTL, B asks for the lease: A still holds it.
        compio::time::sleep(Duration::from_secs(20)).await;
        let b_open = try_open(&mut b, INO, O_WRONLY).await;
        assert!(
            b_open.as_ref().is_err_and(|e| format!("{e:#}").contains("lease conflict")),
            "A's write lease was given up when its poll failed: {b_open:?}"
        );

        // A, still the writer, writes on its open fd and syncs.
        assert_eq!(try_write(&mut a, INO, vec![0xCCu8; 10]).await.expect("a late write"), 10);
        flush_inode(&mut a, INO, FlushReport::ToApplication).await.expect("a fsync");
        let mut c = FsState::new(&mgr_addr.to_string()).await.expect("mount c");
        let head = autumn_fs::read::read(&mut c, INO, 0, 16).await.expect("c read");
        assert_eq!(&head[..10], &[0xCCu8; 10]);
        assert_eq!(&head[10..], &[0xAAu8; 6]);
    });
    drop((n1_dir, n2_dir));
}
