//! A mount that held a file open for reading and then for writing is still in
//! the manager's reader set as the writer. When a forced acquire deposes it and
//! the `LeaseRevoked` push is lost (its inbox overflowed while it was cut off),
//! its heartbeat is renewed as a reader and answers the NEW writer's version.
//! A write slot's epoch comes only from a write grant: the heartbeat must not
//! copy that version into it, or the deposed writer's writes pass the storage
//! fence and replace the new writer's acknowledged bytes. Spawns
//! `toxiproxy-server` from `$PATH`.

mod support;
use std::time::{Duration, Instant};
use autumn_fs::schema::{self, DirentValue, DT_REG, ROOT_INO};
use autumn_fs::state::FsState;
use autumn_fs::write::{flush_inode, FlushReport};
use autumn_fs::{key, meta};
use autumn_fuse::{bridge, dispatch};
use autumn_rpc::client::RpcClient;
use autumn_client::lease::{self, AcquireResult};
use support::*;
const INO: u64 = 80;
const O_RDONLY: i32 = 0;
const O_WRONLY: i32 = 1;
async fn seed_file(state: &mut FsState, name: &[u8], ino: u64) {
    let m = meta::new_file_meta(0o644, 0, 0);
    meta::put_inode(state, ino, &m).await.expect("put_inode");
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
fn a_deposed_writer_keeps_its_epoch() {
    let _ = tracing_subscriber::fmt().with_env_filter("autumn_fs=warn").try_init();
    let params = autumn_params::manager::ManagerParams::default();
    params.inode_lease_ttl_secs.set(60);
    params.inode_revoke_grace_ms.set(0);
    autumn_fs::params().lease_heartbeat_secs.set(2);
    let mgr_addr = pick_addr();
    start_manager_with(mgr_addr, params);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let (n1_addr, n2_addr) = (pick_addr(), pick_addr());
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 129).await;
        let (log, row, meta_stream) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 12901, log, row, meta_stream, b"", b"\xff\xff\xff\xff").await;
        let ps_addr = pick_addr();
        start_partition_server(129, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(1500)).await;
        let (_toxi_guard, toxi_admin) = start_toxiproxy().await;
        let toxi = ToxiproxyCli::new(toxi_admin);
        let proxy_addr = pick_addr().to_string();
        toxi.create("mgr", &proxy_addr, &mgr_addr.to_string()).expect("create proxy");
        let mut a = FsState::new(&proxy_addr).await.expect("mount a");
        dispatch::spawn_lease_background_tasks(&a, None);
        let mut b = FsState::new(&mgr_addr.to_string()).await.expect("mount b");
        seed_file(&mut a, b"deposed.bin", INO).await;
        // A also reads 1100 other inodes (manager-side READ leases only).
        for i in 0..1100u64 {
            let r = lease::acquire(&a.client, &a.client_id, 100_000 + i, autumn_rpc::manager_rpc::LEASE_MODE_READ).await.expect("a read lease");
            assert!(matches!(r, AcquireResult::Granted(_)));
        }
        // A: read fd first (reader entry at the manager), then the write upgrade.
        try_open(&mut a, INO, O_RDONLY).await.expect("a open r");
        try_open(&mut a, INO, O_WRONLY).await.expect("a open w");
        assert_eq!(try_write(&mut a, INO, vec![0xAAu8; 4096]).await.expect("a write"), 4096);
        flush_inode(&mut a, INO, FlushReport::ToApplication).await.expect("a fsync");
        let va = a.held_leases.borrow().get(&INO).unwrap().lease_epoch;
        a.meta_invalidated.borrow_mut().clear();
        toxi.set_enabled("mgr", false).expect("cut");
        let deadline = Instant::now() + Duration::from_secs(40);
        while !a.meta_invalidated.borrow().contains(&INO) {
            assert!(Instant::now() < deadline, "the poll never failed");
            compio::time::sleep(Duration::from_millis(100)).await;
        }
        // B force-preempts while A is cut off; the LeaseRevoked push is lost.
        let (cl, id) = (b.client.clone(), b.client_id.clone());
        let r1 = lease::acquire_force(&cl, &id, INO, autumn_rpc::manager_rpc::LEASE_MODE_WRITE).await.expect("force 1");
        eprintln!("force 1: {r1:?}");
        compio::time::sleep(Duration::from_millis(50)).await;
        let vb = match lease::acquire_force(&cl, &id, INO, autumn_rpc::manager_rpc::LEASE_MODE_WRITE).await.expect("force 2") {
            AcquireResult::Granted(info) => info.version,
            o => panic!("force 2: {o:?}"),
        };
        eprintln!("A epoch {va}, B epoch {vb}");
        // While A is cut, a writer closes those 1100 inodes: A's inbox
        // overflows (cap 1024) and the LeaseRevoked queued first is dropped.
        let d = FsState::new(&mgr_addr.to_string()).await.expect("mount d");
        for i in 0..1100u64 {
            let ino = 100_000 + i;
            let r = lease::acquire(&d.client, &d.client_id, ino, autumn_rpc::manager_rpc::LEASE_MODE_WRITE).await.expect("d write lease");
            assert!(matches!(r, AcquireResult::Granted(_)), "{r:?}");
            lease::release(&d.client, &d.client_id, ino).await.expect("d release");
        }
        compio::time::sleep(Duration::from_millis(500)).await;
        toxi.set_enabled("mgr", true).expect("restore");
        compio::time::sleep(Duration::from_secs(6)).await;
        let a_slot = a.held_leases.borrow().get(&INO).map(|l| (l.mode, l.lease_epoch, l.revoked));
        eprintln!("A slot after heartbeats: {a_slot:?}");
        // The push was lost and the heartbeats renewed A as a reader: its write
        // slot is live and must still carry its own epoch, not B's.
        assert!(vb > va);
        assert_eq!(a_slot, Some((autumn_rpc::manager_rpc::LEASE_MODE_WRITE, va, false)));
        // B writes and syncs under its lease.
        try_open(&mut b, INO, O_WRONLY).await.expect("b open");
        assert_eq!(try_write(&mut b, INO, vec![0xBBu8; 4096]).await.expect("b write"), 4096);
        flush_inode(&mut b, INO, FlushReport::ToApplication).await.expect("b fsync");
        // A, deposed, writes over B's acknowledged bytes.
        let aw = try_write(&mut a, INO, vec![0xCCu8; 4096]).await;
        eprintln!("A late write: {aw:?}");
        let af = flush_inode(&mut a, INO, FlushReport::ToApplication).await;
        eprintln!("A late flush: {af:?}");
        let mut c = FsState::new(&mgr_addr.to_string()).await.expect("mount c");
        let head = autumn_fs::read::read(&mut c, INO, 0, 16).await.expect("c read");
        eprintln!("file head: {:02x?}", &head[..]);
        assert_eq!(&head[..], &[0xBBu8; 16], "deposed writer A overwrote B's acknowledged bytes");
    });
    drop((n1_dir, n2_dir));
}
