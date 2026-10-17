//! A mount whose lease the manager forgot (TTL ran out while its heartbeats
//! were late) must stop trusting what it cached: the heartbeat's `NotHeld`
//! has to invalidate the inode like a lost poll does, or the mount answers
//! GETATTR from meta no writer-close can refresh any more (it is no longer a
//! reader the manager pushes to). Drives `dispatch::handle_request` on two
//! `FsState`s (two mounts), no kernel.

mod support;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use autumn_fs::schema::{self, DirentValue, DT_REG, ROOT_INO};
use autumn_fs::state::FsState;
use autumn_fs::{key, meta};
use autumn_fuse::dispatch::{self, InodeInvalidator};
use autumn_fuse::bridge;
use autumn_rpc::client::RpcClient;

use support::*;

const INO: u64 = 77;
const O_RDONLY: i32 = 0;
const O_WRONLY: i32 = 1;

async fn seed_file(state: &mut FsState, name: &[u8], ino: u64) {
    let m = meta::new_file_meta(0o644, 0, 0);
    meta::put_inode(state, ino, &m).await.expect("put_inode");
    let dk = key::dirent_key(ROOT_INO, name);
    let dv = schema::encode_dirent(&DirentValue { child_inode: ino, file_type: DT_REG });
    state.kv_put(&dk, &dv).await.expect("put dirent");
}

async fn open(state: &mut FsState, ino: u64, flags: i32) {
    let (tx, rx) = bridge::reply_channel::<(u64, u32)>();
    dispatch::handle_request(state, bridge::FsRequest::Open { ino, flags, reply: tx }, None).await;
    rx.recv_timeout(Duration::from_secs(10)).expect("open reply").expect("open");
}

async fn getattr_size(state: &mut FsState, ino: u64) -> u64 {
    let (tx, rx) = bridge::reply_channel::<autumn_fuse::fuser::FileAttr>();
    dispatch::handle_request(state, bridge::FsRequest::GetAttr { ino, reply: tx }, None).await;
    rx.recv_timeout(Duration::from_secs(10)).expect("getattr reply").expect("getattr").size
}

async fn write(state: &mut FsState, ino: u64, data: Vec<u8>) {
    let (tx, rx) = bridge::reply_channel::<u32>();
    let n = data.len() as u32;
    dispatch::handle_request(state, bridge::FsRequest::Write { ino, offset: 0, data, reply: tx }, None).await;
    assert_eq!(rx.recv_timeout(Duration::from_secs(10)).expect("write reply").expect("write"), n);
}

async fn release(state: &mut FsState, ino: u64, flags: i32) {
    let (tx, rx) = bridge::reply_channel::<()>();
    dispatch::handle_request(state, bridge::FsRequest::Release { ino, flags, flush: true, reply: tx }, None).await;
    rx.recv_timeout(Duration::from_secs(10)).expect("release reply").expect("release");
}

fn recording_invalidator() -> (InodeInvalidator, Arc<Mutex<Vec<u64>>>) {
    let log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let log_c = log.clone();
    (std::rc::Rc::new(move |ino: u64| log_c.lock().unwrap().push(ino)), log)
}

#[test]
fn a_lease_the_manager_forgot_invalidates_the_cached_inode() {
    let _ = tracing_subscriber::fmt().with_env_filter("autumn_fs=warn").try_init();
    // The manager forgets a lease 15 s after its last heartbeat (swept once a
    // second); this mount heartbeats every 25 s, so its first heartbeat finds
    // the lease gone.
    let params = autumn_params::manager::ManagerParams::default();
    params.inode_lease_ttl_secs.set(15);
    autumn_fs::params().lease_heartbeat_secs.set(25);

    let mgr_addr = pick_addr();
    start_manager_with(mgr_addr, params);
    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1_addr, n2_addr, 126).await;
        let (log, row, meta_stream) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 12601, log, row, meta_stream, b"", b"\xff\xff\xff\xff").await;
        let ps_addr = pick_addr();
        start_partition_server(126, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(1500)).await;
        let _ = RpcClient::connect(ps_addr).await.unwrap();

        let mut a = FsState::new(&mgr_addr.to_string()).await.expect("mount a");
        let (inv, invalidated) = recording_invalidator();
        dispatch::spawn_lease_background_tasks(&a, Some(inv));
        let mut b = FsState::new(&mgr_addr.to_string()).await.expect("mount b");
        seed_file(&mut a, b"shared.bin", INO).await;

        // A reads: holds a READ lease and caches the inode (size 0).
        open(&mut a, INO, O_RDONLY).await;
        assert_eq!(getattr_size(&mut a, INO).await, 0);
        assert!(a.held_leases.borrow().contains_key(&INO));
        let t0 = Instant::now();

        // The manager's TTL runs out and its sweep drops A; A has not
        // heartbeated yet.
        compio::time::sleep(Duration::from_secs(18)).await;
        // B writes and closes. A is no longer a reader, so nothing is pushed
        // to it.
        open(&mut b, INO, O_WRONLY).await;
        write(&mut b, INO, vec![7u8; 4096]).await;
        release(&mut b, INO, O_WRONLY).await;
        assert_eq!(getattr_size(&mut b, INO).await, 4096);
        // Shown, not assumed: had the close been pushed to A (its lease not
        // yet swept), A would already be current and the end of this test
        // would prove nothing about NotHeld.
        compio::time::sleep(Duration::from_secs(1)).await;
        assert!(a.held_leases.borrow().contains_key(&INO), "harness timing: the heartbeat ran early");
        assert_eq!(getattr_size(&mut a, INO).await, 0, "harness timing: B's close was pushed to A");
        assert!(invalidated.lock().unwrap().is_empty(), "harness timing: B's close was pushed to A");

        // A's heartbeat (t = 25 s) is answered NotHeld.
        let deadline = Instant::now() + Duration::from_secs(40);
        while a.held_leases.borrow().contains_key(&INO) {
            assert!(Instant::now() < deadline, "the heartbeat never dropped the lease");
            compio::time::sleep(Duration::from_millis(200)).await;
        }
        eprintln!("lease dropped at {:?}", t0.elapsed());

        // Nothing A cached about the inode may be trusted any more.
        assert_eq!(getattr_size(&mut a, INO).await, 4096, "GETATTR still answers the size cached before the lease was lost");
        assert!(
            invalidated.lock().unwrap().contains(&INO),
            "the kernel cache was not invalidated after NotHeld: {:?}",
            invalidated.lock().unwrap()
        );
    });
}
