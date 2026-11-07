//! Two acquires of one owner key whose etcd replies arrive out of commit
//! order: the manager's in-memory epoch must still end at the newest one.
//!
//! The etcd client re-sends a request after a timeout on a fresh connection,
//! and the old connection's pending replies are still delivered. So acquire A
//! (held, times out, re-sent, commits LAST) can be answered before acquire B
//! (held, released, committed in between). Spawns `etcd` and
//! `toxiproxy-server` from `$PATH`.

mod support;

use std::net::SocketAddr;
use std::time::Duration;

use autumn_manager::AutumnManager;
use autumn_params::manager::ManagerParams;
use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;

use support::{pick_addr, start_etcd, start_toxiproxy, ToxiproxyCli};

const OWNER_KEY: &str = "partition/777";

fn start_manager(mgr_addr: SocketAddr, endpoints: Vec<String>) {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let params = ManagerParams::default();
            // The smallest allowed; the held reply must outlive it.
            params.etcd_request_timeout_ms.set(1000);
            let manager = AutumnManager::new_with_etcd_params(endpoints, support::manager_identity(), params)
                .await
                .expect("new manager with etcd");
            let _ = manager.serve(mgr_addr).await;
        });
    });
}

/// Retries until the manager thread is listening.
async fn admin(mgr_addr: SocketAddr) -> std::rc::Rc<RpcClient> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match RpcClient::connect_as(mgr_addr, autumn_rpc::version_hello::Role::Admin, None).await {
            Ok(c) => return c,
            Err(_) if std::time::Instant::now() < deadline => {
                compio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("connect mgr: {e}"),
        }
    }
}

async fn acquire(mgr: &RpcClient, owner_key: &str) -> AcquireOwnerLockResp {
    let resp = mgr
        .call(
            MSG_ACQUIRE_OWNER_LOCK,
            rkyv_encode(&AcquireOwnerLockReq { owner_key: owner_key.to_string() }),
        )
        .await
        .expect("acquire owner lock");
    rkyv_decode(&resp).expect("decode")
}

/// The manager's in-memory epoch for `partition/777`, read off the fence
/// message of a corrupt-replica report with an epoch that cannot match.
async fn memory_epoch(mgr: &RpcClient) -> i64 {
    let resp = mgr
        .call(
            MSG_REPORT_CORRUPT_REPLICA,
            rkyv_encode(&ReportCorruptReplicaReq {
                partition_id: 777,
                owner_epoch: -1,
                stream_id: 0,
                extent_id: 0,
                eversion: 0,
                corrupt_node_ids: vec![],
            }),
        )
        .await
        .expect("report corrupt replica");
    let resp: ReportCorruptReplicaResp = rkyv_decode(&resp).expect("decode");
    assert_eq!(resp.code, CODE_PRECONDITION, "{}", resp.message);
    let (_, tail) = resp.message.split_once("current Some(").expect(&resp.message);
    tail.split(')').next().unwrap().parse().expect(&resp.message)
}

#[test]
fn a_late_reply_from_the_old_etcd_connection_does_not_lower_the_owner_epoch() {
    let _ = tracing_subscriber::fmt().with_env_filter("autumn_etcd=warn,autumn_manager=warn").try_init();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (_etcd_guard, etcd_url) = start_etcd().await;
        let etcd_addr = etcd_url.trim_start_matches("http://").to_string();
        let (_toxi_guard, toxi_admin) = start_toxiproxy().await;
        let toxi = ToxiproxyCli::new(toxi_admin);
        let proxy_addr = pick_addr().to_string();
        toxi.create("etcd", &proxy_addr, &etcd_addr).expect("create proxy");

        // The manager connects to the first endpoint (the proxy); a timeout
        // reconnects it to the next one (etcd directly).
        let mgr_addr = pick_addr();
        start_manager(mgr_addr, vec![format!("http://{proxy_addr}"), etcd_url.clone()]);
        let mgr = admin(mgr_addr).await;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if acquire(&mgr, "partition/warmup").await.code == CODE_OK {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "manager never led");
            compio::time::sleep(Duration::from_millis(100)).await;
        }
        let first = acquire(&mgr, OWNER_KEY).await;
        assert_eq!(first.code, CODE_OK, "{}", first.message);

        // Hold every etcd reply (a latency longer than the test; removing the
        // toxic releases what it holds — toxiproxy's `timeout` toxic would
        // discard the bytes instead). Requests still reach etcd and commit.
        toxi.add_toxic("etcd", "latency", "hold", false, &[("latency", "5000"), ("jitter", "0")])
            .expect("toxic add");

        let mgr_a = admin(mgr_addr).await;
        let mgr_b = admin(mgr_addr).await;
        let t0 = std::time::Instant::now();
        let a_answered = std::rc::Rc::new(std::cell::Cell::new(false));
        let a_flag = a_answered.clone();
        let a = compio::runtime::spawn(async move {
            let r = acquire(&mgr_a, OWNER_KEY).await;
            eprintln!("A answered at {:?}: epoch {}", t0.elapsed(), r.owner_epoch);
            a_flag.set(true);
            r
        });
        compio::time::sleep(Duration::from_millis(600)).await;
        let b = compio::runtime::spawn(async move {
            let r = acquire(&mgr_b, OWNER_KEY).await;
            eprintln!("B answered at {:?}: epoch {}", t0.elapsed(), r.owner_epoch);
            r
        });
        // t=1.0 s: A times out, reconnects past the proxy, re-sends and is
        // answered (the newest commit). B's deadline is t=1.6 s.
        compio::time::sleep(Duration::from_millis(600)).await;
        eprintln!("removing the hold at {:?}", t0.elapsed());
        // Without this the held reply could land first and memory would end
        // at A's revision even unfixed.
        assert!(a_answered.get(), "harness timing: A's re-send was not answered before the release");
        toxi.remove_toxic("etcd", "hold").expect("toxic remove");
        // B's held reply arrives now, carrying the older revision.
        let a = a.await.expect("a");
        let b = b.await.expect("b");
        assert_eq!(a.code, CODE_OK, "{}", a.message);
        assert_eq!(b.code, CODE_OK, "{}", b.message);
        assert!(
            a.owner_epoch > b.owner_epoch,
            "the reorder did not happen (a={} b={}); the timing assumptions are off",
            a.owner_epoch,
            b.owner_epoch
        );

        // etcd holds A's re-sent commit as the key's latest...
        let etcd = autumn_etcd::EtcdClient::connect(&etcd_url).await.expect("etcd");
        let kv = etcd.get(format!("ownerLocks/{OWNER_KEY}")).await.expect("get");
        eprintln!(
            "first={} a={} b={} etcd: header_rev={} mod_rev={} version={} create_rev={} memory={}",
            first.owner_epoch, a.owner_epoch, b.owner_epoch,
            kv.header.as_ref().map(|h| h.revision).unwrap_or(-1),
            kv.kvs[0].mod_revision, kv.kvs[0].version, kv.kvs[0].create_revision,
            memory_epoch(&mgr).await
        );
        assert_eq!(kv.kvs[0].mod_revision, a.owner_epoch);
        // ...and the manager must agree, or it fences the real owner.
        assert_eq!(memory_epoch(&mgr).await, a.owner_epoch);
    });
}
