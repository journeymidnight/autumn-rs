//! A manager restart starts lease versions over. A mount that still caches an
//! inode it has closed (the dentry keeps it) then re-opens it with a version
//! no higher than the one it cached, so the "newer version → reload" rule
//! cannot fire; the Open has read the inode's current meta anyway and must
//! use it. Needs `etcd` on `PATH`.

mod support;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use autumn_fs::schema::{self, DirentValue, DT_REG, ROOT_INO};
use autumn_fs::state::FsState;
use autumn_fs::{key, meta};
use autumn_fuse::{bridge, dispatch};
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_rpc::version_hello::Role;

use support::*;

const INO: u64 = 78;
const O_RDONLY: i32 = 0;
const O_WRONLY: i32 = 1;

fn start_manager(mgr_addr: SocketAddr, etcd_url: String) -> Arc<AtomicBool> {
    let flag = Arc::new(AtomicBool::new(false));
    let flag_thread = flag.clone();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = autumn_manager::AutumnManager::new_with_etcd(vec![etcd_url], manager_identity())
                .await
                .expect("new manager with etcd");
            compio::runtime::spawn(async move {
                let _ = manager.serve(mgr_addr).await;
            })
            .detach();
            while !flag_thread.load(Ordering::Acquire) {
                compio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    });
    flag
}

async fn wait_leader(addr: SocketAddr) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(mgr) = RpcClient::connect_as(addr, Role::Admin, None).await {
            if let Ok(resp) = mgr.call(MSG_GET_CLUSTER_OVERVIEW, bytes::Bytes::new()).await {
                let ov: GetClusterOverviewResp = rkyv_decode(&resp).expect("decode");
                if ov.code == CODE_OK {
                    return;
                }
            }
        }
        assert!(std::time::Instant::now() < deadline, "manager at {addr} never led");
        compio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn seed_file(state: &mut FsState, name: &[u8], ino: u64) {
    let m = meta::new_file_meta(0o644, 0, 0);
    meta::put_inode(state, ino, &m).await.expect("put_inode");
    let dk = key::dirent_key(ROOT_INO, name);
    let dv = schema::encode_dirent(&DirentValue { child_inode: ino, file_type: DT_REG });
    state.kv_put(&dk, &dv).await.expect("put dirent");
}

/// The kernel's dentry pin: a looked-up inode stays cached across its closes.
async fn lookup(state: &mut FsState, name: &str) -> u64 {
    let (tx, rx) = bridge::reply_channel::<(autumn_fuse::fuser::FileAttr, u64)>();
    let req = bridge::FsRequest::Lookup { parent: ROOT_INO, name: std::ffi::OsString::from(name), reply: tx };
    dispatch::handle_request(state, req, None).await;
    rx.recv_timeout(Duration::from_secs(30)).expect("lookup reply").expect("lookup").0.ino
}

async fn open(state: &mut FsState, ino: u64, flags: i32) {
    let (tx, rx) = bridge::reply_channel::<(u64, u32)>();
    dispatch::handle_request(state, bridge::FsRequest::Open { ino, flags, reply: tx }, None).await;
    rx.recv_timeout(Duration::from_secs(30)).expect("open reply").expect("open");
}

async fn getattr_size(state: &mut FsState, ino: u64) -> u64 {
    let (tx, rx) = bridge::reply_channel::<autumn_fuse::fuser::FileAttr>();
    dispatch::handle_request(state, bridge::FsRequest::GetAttr { ino, reply: tx }, None).await;
    rx.recv_timeout(Duration::from_secs(30)).expect("getattr reply").expect("getattr").size
}

async fn write(state: &mut FsState, ino: u64, data: Vec<u8>) {
    let (tx, rx) = bridge::reply_channel::<u32>();
    let n = data.len() as u32;
    dispatch::handle_request(state, bridge::FsRequest::Write { ino, offset: 0, data, reply: tx }, None).await;
    assert_eq!(rx.recv_timeout(Duration::from_secs(30)).expect("write reply").expect("write"), n);
}

async fn release(state: &mut FsState, ino: u64, flags: i32) {
    let (tx, rx) = bridge::reply_channel::<()>();
    dispatch::handle_request(state, bridge::FsRequest::Release { ino, flags, flush: true, reply: tx }, None).await;
    rx.recv_timeout(Duration::from_secs(30)).expect("release reply").expect("release");
}

#[test]
fn a_reopened_inode_takes_the_current_meta_after_a_manager_restart() {
    let _ = tracing_subscriber::fmt().with_env_filter("autumn_fs=warn,autumn_fuse=info").try_init();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_url) = start_etcd().await;
        let mgr_addr = pick_addr();
        let mgr1 = start_manager(mgr_addr, etcd_url.clone());
        wait_leader(mgr_addr).await;

        let n1_dir = tempfile::tempdir().expect("n1");
        let n2_dir = tempfile::tempdir().expect("n2");
        let n1_addr = pick_addr();
        let n2_addr = pick_addr();
        start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
        start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 128).await;
        let (log, row, meta_stream) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 12801, log, row, meta_stream, b"", b"\xff\xff\xff\xff").await;
        let ps_addr = pick_addr();
        start_partition_server(128, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(1500)).await;
        let _ = RpcClient::connect(ps_addr).await.unwrap();

        let mut a = FsState::new(&mgr_addr.to_string()).await.expect("mount a");
        dispatch::spawn_lease_background_tasks(&a, None);
        let mut b = FsState::new(&mgr_addr.to_string()).await.expect("mount b");
        seed_file(&mut a, b"restart.bin", INO).await;

        // A looks the file up (the dentry keeps the inode cached across its
        // closes), then opens and closes it for writing a few times: each
        // writer close moves the lease version up, and the cached inode
        // remembers the last one.
        assert_eq!(lookup(&mut a, "restart.bin").await, INO);
        for _ in 0..5 {
            open(&mut a, INO, O_WRONLY).await;
            release(&mut a, INO, O_WRONLY).await;
        }
        assert_eq!(getattr_size(&mut a, INO).await, 0);
        let cached_version = a.inodes.get(&INO).expect("cached after close").cached_version;
        eprintln!("A cached version {cached_version}");

        // Manager restart on the same address; the PS and the mounts reconnect.
        mgr1.store(true, Ordering::Release);
        compio::time::sleep(Duration::from_secs(1)).await;
        let _mgr2 = start_manager(mgr_addr, etcd_url.clone());
        wait_leader(mgr_addr).await;
        compio::time::sleep(Duration::from_secs(5)).await;

        // B appends under the new manager (lease versions start over).
        open(&mut b, INO, O_WRONLY).await;
        write(&mut b, INO, vec![0xBBu8; 4096]).await;
        release(&mut b, INO, O_WRONLY).await;
        assert_eq!(getattr_size(&mut b, INO).await, 4096);

        // A re-opens: its new lease version is no higher than the one it
        // cached, but the Open has read the current meta and must use it.
        open(&mut a, INO, O_RDONLY).await;
        let acquired = a.held_leases.borrow().get(&INO).map(|l| l.lease_epoch).unwrap_or(0);
        eprintln!("A re-acquired version {acquired}");
        assert!(acquired <= cached_version, "harness: the version did not start over ({acquired} > {cached_version})");
        assert_eq!(getattr_size(&mut a, INO).await, 4096, "GETATTR answers the meta cached before the restart");
    });
}
