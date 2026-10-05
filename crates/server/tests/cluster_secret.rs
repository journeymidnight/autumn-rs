//! The cluster secret, against the real binaries.
//!
//! Every server refuses to start without `--cluster-secret-file`, and refuses a
//! Peer or Admin connection that cannot prove the secret (PEER_AUTH, right after
//! VERSION_HELLO). A Client connection proves nothing here. autumn-op connects
//! as Admin, so it needs the secret for every command. This test process installs
//! no secret of its own: each connection names the secret it proves, if any.

use std::net::{SocketAddr, TcpListener};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use autumn_rpc::extent_rpc::MSG_DELETE_EXTENT;
use autumn_rpc::manager_rpc::{MSG_GET_CLUSTER_ID, MSG_NAMESPACE_CREATE, MSG_NAMESPACE_LIST};
use autumn_rpc::peer_auth::ClusterSecret;
use autumn_rpc::version_hello::{Hello, Role, Service};
use autumn_rpc::{Frame, FrameDecoder, RpcError, StatusCode};
use autumn_transport::{Conn, ReadHalf, WriteHalf};
use bytes::Bytes;
use compio::io::{AsyncRead, AsyncWriteExt};

const MANAGER_BIN: &str = env!("CARGO_BIN_EXE_autumn-manager-server");
const EXTENT_NODE_BIN: &str = env!("CARGO_BIN_EXE_autumn-extent-node");
const AUTUMN_OP_BIN: &str = env!("CARGO_BIN_EXE_autumn-op");

const SECRET: &str = "cluster-secret-test-the-right-one-0123456789";
const WRONG: &str = "cluster-secret-test-a-different-one-0123456";

fn pick_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
    l.local_addr().expect("local_addr").port()
}

fn wait_port_open(port: u16) {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(20) {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("port {port} never opened");
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let p = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn secret(s: &str) -> ClusterSecret {
    ClusterSecret::new(s).unwrap()
}

/// VERSION_HELLO as `role`, then PEER_AUTH proving `proof` (if any).
async fn open(
    addr: SocketAddr,
    service: Service,
    role: Role,
    proof: Option<&ClusterSecret>,
) -> Result<(ReadHalf, WriteHalf), RpcError> {
    let socket = compio::net::TcpStream::connect(addr).await?;
    let (mut rd, mut wr) = Conn::Tcp(socket).into_split();
    let negotiated =
        autumn_rpc::version_hello::initiate(&mut rd, &mut wr, Hello::current(role), Some(service))
            .await?;
    autumn_rpc::peer_auth::initiate(&mut rd, &mut wr, &negotiated, proof).await?;
    Ok((rd, wr))
}

async fn call(rd: &mut ReadHalf, wr: &mut WriteHalf, op: u8, payload: Bytes) -> Frame {
    wr.write_all(Frame::request(2, op, payload).encode())
        .await
        .0
        .unwrap();
    let mut decoder = FrameDecoder::new();
    loop {
        let compio::BufResult(n, buf) = rd.read(vec![0; 4096]).await;
        let n = n.unwrap();
        assert_ne!(n, 0, "connection closed before a reply");
        decoder.feed(&buf[..n]);
        if let Some(frame) = decoder.try_decode().unwrap() {
            return frame;
        }
    }
}

fn refused(r: &Result<(ReadHalf, WriteHalf), RpcError>, needle: &str) {
    match r {
        Err(RpcError::Status {
            code: StatusCode::PermissionDenied,
            message,
        }) => assert!(message.contains(needle), "{message}"),
        Err(e) => panic!("refused with the wrong error: {e}"),
        Ok(_) => panic!("admitted; expected a refusal mentioning {needle:?}"),
    }
}

/// One class of `autumn_en_auth_rejects_total` from an extent node's /metrics.
fn en_rejects(metrics_port: u16, class: &str) -> u64 {
    use std::io::{Read, Write};
    wait_port_open(metrics_port);
    let mut conn = std::net::TcpStream::connect(("127.0.0.1", metrics_port)).unwrap();
    conn.write_all(b"GET /metrics HTTP/1.0\r\n\r\n").unwrap();
    let mut body = String::new();
    conn.read_to_string(&mut body).unwrap();
    let line = format!("autumn_en_auth_rejects_total{{class=\"{class}\"}} ");
    body.lines()
        .find_map(|l| l.strip_prefix(&line))
        .unwrap_or_else(|| panic!("no {class} counter in:\n{body}"))
        .trim()
        .parse::<f64>()
        .unwrap() as u64
}

fn run_op(args: &[&str]) -> std::process::Output {
    Command::new(AUTUMN_OP_BIN)
        .args(args)
        .output()
        .expect("run autumn-op")
}

#[test]
fn servers_refuse_to_start_without_the_secret() {
    for bin in [MANAGER_BIN, EXTENT_NODE_BIN] {
        let out = Command::new(bin)
            .args(["--port", &pick_port().to_string(), "--listen", "127.0.0.1"])
            .output()
            .expect("run server");
        assert_eq!(out.status.code(), Some(2), "{bin} started without a secret");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains("--cluster-secret-file is required"), "{bin}: {err}");
    }
    // The removed admin token is named, not ignored.
    let out = Command::new(MANAGER_BIN)
        .args(["--admin-token-file", "/nonexistent"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("was removed"));
}

#[test]
fn members_and_operators_must_prove_the_secret() {
    let tmp = TempDir::new("cluster-secret");
    let secret_file = tmp.0.join("secret");
    std::fs::write(&secret_file, format!("{SECRET}\n")).unwrap();
    let secret_arg = secret_file.to_str().unwrap();

    let mgr_port = pick_port();
    let mgr_addr = format!("127.0.0.1:{mgr_port}");
    let mgr_log = tmp.0.join("manager.log");
    let _manager = ChildGuard(
        Command::new(MANAGER_BIN)
            .args(["--cluster-secret-file", secret_arg])
            .args(["--port", &mgr_port.to_string(), "--listen", "127.0.0.1"])
            .stdout(std::fs::File::create(&mgr_log).unwrap())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn manager"),
    );
    wait_port_open(mgr_port);

    // An extent node of the same cluster.
    let en_dir = tmp.0.join("en");
    std::fs::create_dir_all(&en_dir).unwrap();
    let out = run_op(&[
        "--cluster-secret-file",
        secret_arg,
        "--manager",
        &mgr_addr,
        "format",
        en_dir.to_str().unwrap(),
    ]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let en_port = pick_port();
    let metrics_port = pick_port();
    let en_log = tmp.0.join("en.log");
    let _en = ChildGuard(
        Command::new(EXTENT_NODE_BIN)
            .args(["--cluster-secret-file", secret_arg])
            .args(["--port", &en_port.to_string(), "--listen", "127.0.0.1"])
            .args(["--metrics-port", &metrics_port.to_string()])
            .args(["--control-port", &pick_port().to_string()])
            .args(["--data", en_dir.to_str().unwrap()])
            .args(["--manager", &mgr_addr])
            .args(["--advertise", &format!("127.0.0.1:{en_port}")])
            .args(["--cpuset", &first_allowed_core()])
            .stdout(std::fs::File::create(&en_log).unwrap())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn extent node"),
    );
    wait_port_open(en_port);

    let mgr: SocketAddr = mgr_addr.parse().unwrap();
    let en: SocketAddr = format!("127.0.0.1:{en_port}").parse().unwrap();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let (right, wrong) = (secret(SECRET), secret(WRONG));
        for (addr, service) in [(mgr, Service::Manager), (en, Service::ExtentNode)] {
            for role in [Role::Peer, Role::Admin] {
                // No secret, or someone else's: refused before any business frame.
                refused(
                    &open(addr, service, role, None).await,
                    "requires the cluster secret",
                );
                refused(
                    &open(addr, service, role, Some(&wrong)).await,
                    "different secrets",
                );
                // The cluster's own: admitted.
                assert!(
                    open(addr, service, role, Some(&right)).await.is_ok(),
                    "{service:?} refused {role:?} holding the right secret"
                );
            }
        }

        // An admitted operator runs an operator-only op; a member may not.
        let (mut rd, mut wr) = open(mgr, Service::Manager, Role::Admin, Some(&right))
            .await
            .unwrap();
        let reply = call(&mut rd, &mut wr, MSG_NAMESPACE_LIST, Bytes::new()).await;
        assert!(!reply.is_error(), "Admin namespace-list refused");
        let (mut rd, mut wr) = open(mgr, Service::Manager, Role::Peer, Some(&right))
            .await
            .unwrap();
        let reply = call(&mut rd, &mut wr, MSG_NAMESPACE_CREATE, Bytes::new()).await;
        assert!(reply.is_error());
        assert_eq!(
            RpcError::decode_status(&reply.payload).0,
            StatusCode::PermissionDenied
        );

        // A client proves nothing here.
        let (mut rd, mut wr) = open(mgr, Service::Manager, Role::Client, None)
            .await
            .unwrap();
        let reply = call(&mut rd, &mut wr, MSG_GET_CLUSTER_ID, Bytes::new()).await;
        assert!(!reply.is_error(), "Client get-cluster-id refused");

        // A Client reaches an extent node without any proof, but only its
        // read ops: a destructive one is refused and counted.
        let before = (en_rejects(metrics_port, "opcode_denied"), en_rejects(metrics_port, "peer_auth"));
        let (mut rd, mut wr) = open(en, Service::ExtentNode, Role::Client, None)
            .await
            .unwrap();
        let reply = call(&mut rd, &mut wr, MSG_DELETE_EXTENT, Bytes::new()).await;
        assert_eq!(
            RpcError::decode_status(&reply.payload).0,
            StatusCode::PermissionDenied,
            "a Client ran DELETE_EXTENT"
        );
        assert_eq!(en_rejects(metrics_port, "opcode_denied"), before.0 + 1);
        // A Peer holding the wrong secret never reaches a frame: counted too.
        refused(
            &open(en, Service::ExtentNode, Role::Peer, Some(&secret(WRONG))).await,
            "different secrets",
        );
        assert_eq!(en_rejects(metrics_port, "peer_auth"), before.1 + 1);
    });

    // The refusing side names the refused peer, so a misconfigured process can
    // be found from the server's log.
    for log in [&mgr_log, &en_log] {
        let text = std::fs::read_to_string(log).unwrap();
        assert!(
            text.contains("PEER_AUTH refused a connection holding a different cluster secret"),
            "{}: {text}",
            log.display()
        );
    }

    // autumn-op connects as an operator: without the secret it says which flag
    // is missing; with it, it works. The removed admin token is named.
    let out = run_op(&["--manager", &mgr_addr, "list-nodes"]);
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("--cluster-secret-file"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = run_op(&["--cluster-secret-file", secret_arg, "--manager", &mgr_addr, "list-nodes"]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let out = run_op(&["--admin-token-file", secret_arg, "--manager", &mgr_addr, "list-nodes"]);
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("was removed"));
}

/// A running member whose dial is refused by PEER_AUTH exits: the extent node
/// keeps polling its manager, and when the manager comes back on the same
/// address holding a different secret, the extent node's next dial is refused
/// and the process ends with status 1, naming the peer.
#[test]
fn a_member_refused_by_peer_auth_exits() {
    let tmp = TempDir::new("cluster-secret-fatal");
    let (right_file, other_file) = (tmp.0.join("secret"), tmp.0.join("other"));
    std::fs::write(&right_file, SECRET).unwrap();
    std::fs::write(&other_file, WRONG).unwrap();
    let (right_arg, other_arg) = (right_file.to_str().unwrap(), other_file.to_str().unwrap());

    let mgr_port = pick_port();
    let mgr_addr = format!("127.0.0.1:{mgr_port}");
    let manager = |secret: &str, log: &str| {
        ChildGuard(
            Command::new(MANAGER_BIN)
                .args(["--cluster-secret-file", secret])
                .args(["--port", &mgr_port.to_string(), "--listen", "127.0.0.1"])
                .stdout(std::fs::File::create(tmp.0.join(log)).unwrap())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn manager"),
        )
    };
    let first = manager(right_arg, "manager.log");
    wait_port_open(mgr_port);

    let en_dir = tmp.0.join("en");
    std::fs::create_dir_all(&en_dir).unwrap();
    let out = run_op(&[
        "--cluster-secret-file",
        right_arg,
        "--manager",
        &mgr_addr,
        "format",
        en_dir.to_str().unwrap(),
    ]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let en_port = pick_port();
    let en_log = tmp.0.join("en.log");
    let mut en = ChildGuard(
        Command::new(EXTENT_NODE_BIN)
            .args(["--cluster-secret-file", right_arg])
            .args(["--port", &en_port.to_string(), "--listen", "127.0.0.1"])
            .args(["--control-port", &pick_port().to_string()])
            .args(["--data", en_dir.to_str().unwrap()])
            .args(["--manager", &mgr_addr])
            .args(["--advertise", &format!("127.0.0.1:{en_port}")])
            .args(["--cpuset", &first_allowed_core()])
            .stdout(std::fs::File::create(&en_log).unwrap())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn extent node"),
    );
    wait_port_open(en_port);

    drop(first);
    let _second = manager(other_arg, "manager2.log");
    wait_port_open(mgr_port);

    // The extent node polls its manager every few seconds.
    let start = Instant::now();
    let status = loop {
        if let Some(status) = en.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "the extent node kept running after its manager refused it: {}",
            std::fs::read_to_string(&en_log).unwrap()
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    assert_eq!(status.code(), Some(1));
    let text = std::fs::read_to_string(&en_log).unwrap();
    assert!(
        logged(&text, "PEER_AUTH failed against this process's manager", &mgr_addr),
        "{text}"
    );
}

/// The reverse: a member refused by anything but its manager is not the one at
/// fault, so it keeps running and treats that end as unreachable. The manager
/// polls its extent nodes; here one of them is replaced, on its registered
/// address, by a stranger holding a different secret, which first answers
/// VERSION_HELLO as an extent node and then as a manager. What the stranger
/// claims to be must not matter: only the address the dialer chose does.
#[test]
fn a_member_refused_by_an_extent_node_keeps_running() {
    let tmp = TempDir::new("cluster-secret-en-refuses");
    let secret_file = tmp.0.join("secret");
    std::fs::write(&secret_file, SECRET).unwrap();
    let secret_arg = secret_file.to_str().unwrap();

    let mgr_port = pick_port();
    let mgr_addr = format!("127.0.0.1:{mgr_port}");
    let mgr_log = tmp.0.join("manager.log");
    let mut manager = ChildGuard(
        Command::new(MANAGER_BIN)
            .args(["--cluster-secret-file", secret_arg])
            .args(["--port", &mgr_port.to_string(), "--listen", "127.0.0.1"])
            .stdout(std::fs::File::create(&mgr_log).unwrap())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn manager"),
    );
    wait_port_open(mgr_port);

    let en_dir = tmp.0.join("en");
    std::fs::create_dir_all(&en_dir).unwrap();
    let out = run_op(&[
        "--cluster-secret-file",
        secret_arg,
        "--manager",
        &mgr_addr,
        "format",
        en_dir.to_str().unwrap(),
    ]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    // No `--control-port`: the node listens on, and registers, its port +
    // 1000, which is where the manager polls.
    let en_port = std::iter::repeat_with(pick_port)
        .find(|p| *p < u16::MAX - 1000)
        .unwrap();
    let control_port = en_port + 1000;
    let en = ChildGuard(
        Command::new(EXTENT_NODE_BIN)
            .args(["--cluster-secret-file", secret_arg])
            .args(["--port", &en_port.to_string(), "--listen", "127.0.0.1"])
            .args(["--data", en_dir.to_str().unwrap()])
            .args(["--manager", &mgr_addr])
            .args(["--advertise", &format!("127.0.0.1:{en_port}")])
            .args(["--cpuset", &first_allowed_core()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn extent node"),
    );
    // Registered: the manager polls it from now on.
    let start = Instant::now();
    loop {
        let out = run_op(&["--cluster-secret-file", secret_arg, "--manager", &mgr_addr, "list-nodes"]);
        if String::from_utf8_lossy(&out.stdout).contains(&format!("127.0.0.1:{en_port}")) {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(20), "the extent node never registered");
        std::thread::sleep(Duration::from_millis(200));
    }
    drop(en);

    for declared in [Service::ExtentNode, Service::Manager] {
        let refused = stranger(control_port, declared);
        assert!(refused > 0, "the manager never dialed the extent node's address");
        assert!(
            manager.0.try_wait().unwrap().is_none(),
            "the manager exited after a stranger declaring {declared:?} refused it"
        );
    }
    let text = std::fs::read_to_string(&mgr_log).unwrap();
    assert!(
        logged(
            &text,
            "PEER_AUTH failed: the peer holds a different cluster secret",
            &format!("127.0.0.1:{control_port}"),
        ),
        "{text}"
    );
}

/// A log line carrying `message` and naming `peer`.
fn logged(text: &str, message: &str, peer: &str) -> bool {
    text.lines().any(|l| l.contains(message) && l.contains(peer))
}

/// Listens on `port` for 8 s as a process holding another cluster secret that
/// answers VERSION_HELLO as `declared`; returns how many dials it refused.
fn stranger(port: u16, declared: Service) -> u32 {
    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async move {
            // The port can stay busy for a moment after the node is killed.
            let start = Instant::now();
            let listener = loop {
                match compio::net::TcpListener::bind(format!("127.0.0.1:{port}")).await {
                    Ok(l) => break l,
                    Err(_) if start.elapsed() < Duration::from_secs(10) => {
                        compio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(e) => panic!("bind 127.0.0.1:{port}: {e}"),
                }
            };
            let wrong = secret(WRONG);
            let mut refused = 0u32;
            // One deadline around the whole loop: cancelling a pending accept
            // can drop the connection it was about to return.
            let _ = compio::time::timeout(Duration::from_secs(8), async {
                loop {
                    let Ok((socket, _)) = listener.accept().await else { continue };
                    let (mut rd, mut wr) = Conn::Tcp(socket).into_split();
                    let Ok(n) =
                        autumn_rpc::version_hello::accept(&mut rd, &mut wr, declared, "stranger")
                            .await
                    else {
                        continue;
                    };
                    if autumn_rpc::peer_auth::accept(&mut rd, &mut wr, &n, Some(&wrong), "stranger")
                        .await
                        .is_err()
                    {
                        refused += 1;
                    }
                }
            })
            .await;
            refused
        })
    })
    .join()
    .unwrap()
}

/// One core, so the EN runs one shard.
fn first_allowed_core() -> String {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let list = status
        .lines()
        .find_map(|l| l.strip_prefix("Cpus_allowed_list:"))
        .unwrap()
        .trim();
    autumn_common::parse_cpuset(list).unwrap()[0].to_string()
}
