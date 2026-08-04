//! System test (REPRODUCE-FIRST) — G3: asymmetric partition gray failure.
//!
//! Hypothesis under test: an extent-node's health is judged SOLELY by the
//! manager's `node_health_loop` `df` probe on the EN's CONTROL port
//! (`recovery.rs`: it dials `control_address` via `control_pool`). If we cut
//! the PS/client -> EN DATA path but leave the manager -> EN control (`df`)
//! path reachable, the node stays `Online`, so:
//!   * `select_nodes` (manager `lib.rs`) — which has NO data-plane-reachability
//!     input, only `online_node_ids` (df-verified) / disk-online / `hard_excluded`
//!     = Fenced|Maintenance|Suspected — keeps the node in the selectable pool and
//!     keeps placing new extents' replicas on it;
//!   * `place_extents_with_fallback` (rpc_handlers.rs) fails to `alloc_extent_on_node`
//!     over the (cut) data plane and just walks fallbacks — it NEVER marks the node
//!     offline/suspected or reports disk failure, so the data-plane failure is
//!     invisible to node health;
//!   * the all-replica-ACK append (`client.rs`) to any extent hosting the cut node
//!     can never complete -> soft error -> `alloc_new_extent` -> the manager re-selects
//!     the still-`Online` node -> `MAX_ALLOC_PER_APPEND` (= 3) -> hard error.
//! Net: an indefinite WRITE OUTAGE while every component reports healthy = the
//! definitional gray failure.
//!
//! Contrast (the design TOLERATES this): a SYMMETRIC partition (data + control
//! cut together, like the existing `NetworkPartition` chaos action) fails the df
//! probe -> the node transitions `Online -> Suspected` -> `placement_excluded` ->
//! `select_nodes` refuses it. That path is exercised at the end so the difference
//! is isolated to the asymmetry.
//!
//! Topology: memory-mode manager + 3 in-process ENs (A, B, C), replication
//! factor 3 (so EVERY extent hosts a replica on A — RF == #ENs is the scenario
//! where alloc cannot route around A and the outage PERSISTS). EN-A sits behind
//! two self-contained in-process TCP proxies fronting its single listener:
//!   * `P_data` (registered as the node `address`)      -> data plane (append + alloc)
//!   * `P_ctl`  (registered as the node `control_address`) -> df / control plane
//! Cutting only `P_data` reproduces the asymmetric partition. No toxiproxy /
//! subprocess needed; the cut is a plain `AtomicBool` we flip.
//!
//! DO NOT COMMIT — this is a reproduction harness (per task).

mod support;

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use autumn_rpc::client::RpcClient;
use autumn_rpc::manager_rpc::*;
use autumn_stream::client::StreamClientConfig;
use autumn_stream::{ConnPool, StreamClient};

use support::*;

/// A trivial in-process TCP proxy fronting `upstream`. Returns the listen port
/// and a `cut` flag. While `cut` is false it byte-forwards both directions;
/// once `cut` is set, existing connections are torn down (within ~100 ms) and
/// every new connection is accepted-then-reset (fast, connection-refused-style
/// failure — the same shape toxiproxy `disable` produces). A true SYN-drop
/// blackhole would only make failures SLOWER, not change the outcome.
fn spawn_cut_proxy(upstream: SocketAddr) -> (u16, Arc<AtomicBool>) {
    let cut = Arc::new(AtomicBool::new(false));
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind proxy listener");
    let port = listener.local_addr().expect("proxy local_addr").port();
    let cut_accept = cut.clone();
    thread::spawn(move || {
        for conn in listener.incoming() {
            let client = match conn {
                Ok(c) => c,
                Err(_) => continue,
            };
            if cut_accept.load(Ordering::SeqCst) {
                // Data plane is cut: reset the new connection immediately.
                let _ = client.shutdown(Shutdown::Both);
                continue;
            }
            let up = match TcpStream::connect(upstream) {
                Ok(u) => u,
                Err(_) => {
                    let _ = client.shutdown(Shutdown::Both);
                    continue;
                }
            };
            let _ = client.set_nodelay(true);
            let _ = up.set_nodelay(true);
            let (c1, u1) = (
                client.try_clone().expect("clone client"),
                up.try_clone().expect("clone up"),
            );
            let cut_a = cut_accept.clone();
            let cut_b = cut_accept.clone();
            thread::spawn(move || pump(c1, u1, cut_a)); // client -> upstream
            thread::spawn(move || pump(up, client, cut_b)); // upstream -> client
        }
    });
    (port, cut)
}

fn pump(mut from: TcpStream, mut to: TcpStream, cut: Arc<AtomicBool>) {
    from.set_read_timeout(Some(Duration::from_millis(100))).ok();
    let mut buf = [0u8; 32 * 1024];
    loop {
        if cut.load(Ordering::SeqCst) {
            let _ = from.shutdown(Shutdown::Both);
            let _ = to.shutdown(Shutdown::Both);
            return;
        }
        match from.read(&mut buf) {
            Ok(0) => {
                let _ = to.shutdown(Shutdown::Both);
                return;
            }
            Ok(n) => {
                if to.write_all(&buf[..n]).is_err() {
                    let _ = from.shutdown(Shutdown::Both);
                    return;
                }
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue
            }
            Err(_) => {
                let _ = to.shutdown(Shutdown::Both);
                return;
            }
        }
    }
}

async fn register_with_control(mgr: &RpcClient, addr: &str, control_address: &str, disk_uuid: &str) -> u64 {
    let req = RegisterNodeReq {
        addr: addr.to_string(),
        disk_uuids: vec![disk_uuid.to_string()],
        shard_ports: vec![],
        control_address: control_address.to_string(),
        node_uuid: String::new(),
    };
    let resp = mgr
        .call(MSG_REGISTER_NODE, rkyv_encode(&req))
        .await
        .expect("register node call");
    let r: RegisterNodeResp = rkyv_decode(&resp).expect("decode RegisterNodeResp");
    assert_eq!(r.code, CODE_OK, "register {addr}: {}", r.message);
    r.node_id
}

async fn node_states(mgr: &RpcClient) -> Vec<NodeStateEntry> {
    let resp = mgr
        .call(MSG_LIST_NODE_STATES, rkyv_encode(&ListNodeStatesReq {}))
        .await
        .expect("list_node_states call");
    let r: ListNodeStatesResp = rkyv_decode(&resp).expect("decode ListNodeStatesResp");
    assert_eq!(r.code, CODE_OK, "list_node_states: {}", r.message);
    r.nodes
}

fn state_of(states: &[NodeStateEntry], id: u64) -> u8 {
    states
        .iter()
        .find(|n| n.node_id == id)
        .unwrap_or_else(|| panic!("node {id} missing from list_node_states"))
        .auto_state
}

fn state_name(s: u8) -> &'static str {
    match s {
        NODE_AUTO_STATE_ONLINE => "Online",
        NODE_AUTO_STATE_SUSPECTED => "Suspected",
        NODE_AUTO_STATE_SUSPEND => "Suspend",
        _ => "?",
    }
}

fn dump_states(tag: &str, states: &[NodeStateEntry], a: u64, b: u64, c: u64) {
    eprintln!(
        "  [{tag}] A={} B={} C={}",
        state_name(state_of(states, a)),
        state_name(state_of(states, b)),
        state_name(state_of(states, c)),
    );
}

/// One bounded acked-append probe. Returns a human string describing the
/// outcome; `succeeded` is true ONLY if the write actually acked.
async fn probe_append(sc: &StreamClient, stream_id: u64, label: &str) -> (bool, String) {
    let payload = format!("probe-{label}").into_bytes();
    let started = Instant::now();
    match compio::time::timeout(Duration::from_secs(8), sc.append(stream_id, &payload)).await {
        Ok(Ok(res)) => (
            true,
            format!("ACKED (extent={}, end={}) in {:?}", res.extent_id, res.end, started.elapsed()),
        ),
        Ok(Err(e)) => (false, format!("FAILED after {:?}: {e}", started.elapsed())),
        Err(_) => (false, format!("STALLED (still retrying at 8s cap)")),
    }
}

#[test]
fn asym_partition_data_cut_control_healthy_is_gray_failure() {
    // Fast Online->Suspected transition for the symmetric-partition contrast
    // (the data-only-cut case is unaffected: A's df keeps succeeding on the
    // control port, so on_heartbeat_ok refreshes last_ok every tick).
    std::env::set_var("AUTUMN_MGR_NODE_SUSPECTED_TIMEOUT_SECS", "3");

    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    // 3 in-process extent nodes.
    let a_dir = tempfile::tempdir().expect("a tmpdir");
    let b_dir = tempfile::tempdir().expect("b tmpdir");
    let c_dir = tempfile::tempdir().expect("c tmpdir");
    let a_real = pick_addr();
    let b_addr = pick_addr();
    let c_addr = pick_addr();
    start_extent_node(a_real, a_dir.path().to_path_buf(), 1);
    start_extent_node(b_addr, b_dir.path().to_path_buf(), 2);
    start_extent_node(c_addr, c_dir.path().to_path_buf(), 3);

    // EN-A behind two proxies fronting its single listener: data + control.
    let (a_data_port, cut_data) = spawn_cut_proxy(a_real);
    let (a_ctl_port, cut_ctl) = spawn_cut_proxy(a_real);
    let a_data = format!("127.0.0.1:{a_data_port}");
    let a_ctl = format!("127.0.0.1:{a_ctl_port}");
    eprintln!("EN-A real={a_real}  data-proxy={a_data}  ctl-proxy={a_ctl}");

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");

        // Register: A with SEPARATE data(address) + control(control_address);
        // B/C direct (empty control_address -> df falls back to data addr).
        let node_a = register_with_control(&mgr, &a_data, &a_ctl, "uuid-a").await;
        let node_b = register_node(&mgr, &b_addr.to_string(), "uuid-b").await.node_id;
        let node_c = register_node(&mgr, &c_addr.to_string(), "uuid-c").await.node_id;
        eprintln!("registered A={node_a} B={node_b} C={node_c}");

        // Wait for the first df sweep to bring all three Online.
        let all_online = poll_bool(Duration::from_secs(20), Duration::from_millis(500), || async {
            let s = node_states(&mgr).await;
            state_of(&s, node_a) == NODE_AUTO_STATE_ONLINE
                && state_of(&s, node_b) == NODE_AUTO_STATE_ONLINE
                && state_of(&s, node_c) == NODE_AUTO_STATE_ONLINE
        })
        .await;
        assert!(all_online, "all three ENs must reach Online before the cut");
        dump_states("pre-cut", &node_states(&mgr).await, node_a, node_b, node_c);

        // RF == #ENs == 3: every extent hosts a replica on A.
        let stream_id = create_stream(&mgr, 3).await;

        let pool = Rc::new(ConnPool::new());
        let cfg = StreamClientConfig::default()
            .with_append_fanout_timeout(Duration::from_millis(400));
        let sc = StreamClient::connect_with_config(
            &mgr_addr.to_string(),
            "grayfail-writer".to_string(),
            64 * 1024 * 1024,
            pool.clone(),
            cfg,
        )
        .await
        .expect("connect stream client");

        // Baseline: all reachable -> acked write succeeds.
        let (ok, msg) = probe_append(&sc, stream_id, "baseline").await;
        eprintln!("baseline append: {msg}");
        assert!(ok, "baseline acked write must succeed with all ENs reachable");

        // ---- THE CUT: data plane only. Control (df) stays reachable. ----
        eprintln!("\n>>> CUTTING EN-A DATA PLANE ONLY (control/df left healthy) <<<\n");
        cut_data.store(true, Ordering::SeqCst);

        // Give the manager a few df ticks (2 s each) to re-probe A's control port.
        compio::time::sleep(Duration::from_secs(5)).await;

        // (a) Manager still reports A Online — df on the control port succeeds,
        //     so node health is blind to the data-plane cut.
        let s = node_states(&mgr).await;
        dump_states("post-data-cut", &s, node_a, node_b, node_c);
        assert_eq!(
            state_of(&s, node_a),
            NODE_AUTO_STATE_ONLINE,
            "GRAY-FAILURE PREMISE: EN-A must stay Online after a data-only cut (df on control port healthy)"
        );
        assert_eq!(state_of(&s, node_b), NODE_AUTO_STATE_ONLINE);
        assert_eq!(state_of(&s, node_c), NODE_AUTO_STATE_ONLINE);

        // (b) Acked writes now stall/fail: all-replica-ACK needs A's data port,
        //     and alloc keeps re-selecting the still-Online-but-unreachable A.
        let (ok1, msg1) = probe_append(&sc, stream_id, "post-cut-1").await;
        eprintln!("post-cut append #1: {msg1}");
        assert!(!ok1, "WRITE must NOT ack while A's data plane is cut");

        // (c) No self-heal: after more df ticks, A is STILL Online and writes
        //     STILL do not ack — the outage persists (no data-plane signal ever
        //     removes A from the selectable pool).
        compio::time::sleep(Duration::from_secs(6)).await;
        let s = node_states(&mgr).await;
        dump_states("persist", &s, node_a, node_b, node_c);
        assert_eq!(
            state_of(&s, node_a),
            NODE_AUTO_STATE_ONLINE,
            "outage must persist with A still reported Online (no self-heal)"
        );
        let (ok2, msg2) = probe_append(&sc, stream_id, "post-cut-2").await;
        eprintln!("post-cut append #2 (persistence): {msg2}");
        assert!(!ok2, "WRITE still must NOT ack after settle — outage persists");

        eprintln!(
            "\n=== GRAY FAILURE REPRODUCED ===\n\
             EN-A reported Online the entire time (df on control port healthy), yet\n\
             acked writes never completed (data plane unreachable, select_nodes keeps\n\
             re-placing on A). MAX_ALLOC_PER_APPEND = 3 (stream/src/client.rs).\n"
        );

        // ---- CONTRAST: symmetric partition (cut control too). ----
        // The design DOES detect this: df fails -> Online -> Suspected ->
        // placement_excluded. Isolates the gap to the asymmetry.
        eprintln!(">>> CONTRAST: now cutting EN-A CONTROL plane too (symmetric partition) <<<");
        cut_ctl.store(true, Ordering::SeqCst);
        let suspected = poll_bool(Duration::from_secs(20), Duration::from_millis(500), || async {
            state_of(&node_states(&mgr).await, node_a) == NODE_AUTO_STATE_SUSPECTED
        })
        .await;
        let s = node_states(&mgr).await;
        dump_states("post-symmetric-cut", &s, node_a, node_b, node_c);
        assert!(
            suspected,
            "CONTRAST: with BOTH planes cut, df fails and the manager DOES mark A Suspected \
             (this is the tolerated symmetric-partition path the asymmetric cut evades)"
        );
        eprintln!(
            "\nCONTRAST confirmed: the manager's ONLY liveness signal is the df probe on the\n\
             control port; a symmetric cut is detected (Suspected), an asymmetric data-only cut\n\
             is NOT. => the case for a data-plane reachability signal + failure domains in select_nodes.\n"
        );
    });
}

/// Minimal async poll helper (avoids depending on a specific support signature).
async fn poll_bool<F, Fut>(timeout: Duration, interval: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if f().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        compio::time::sleep(interval).await;
    }
}
