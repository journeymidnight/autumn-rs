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
    let en_log = tmp.0.join("en.log");
    let _en = ChildGuard(
        Command::new(EXTENT_NODE_BIN)
            .args(["--cluster-secret-file", secret_arg])
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
