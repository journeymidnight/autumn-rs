//! A throwaway etcd per test (`etcd` on `$PATH`, or `AUTUMN_TEST_ETCD_BIN`),
//! TLS material for it, and the admin calls that turn its auth on.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use autumn_etcd::{EtcdClient, EtcdConfig};

pub struct Etcd {
    child: std::process::Child,
    _dir: tempfile::TempDir,
}

impl Drop for Etcd {
    fn drop(&mut self) {
        if let Err(e) = self.child.kill() {
            eprintln!("kill etcd: {e}");
        }
        if let Err(e) = self.child.wait() {
            eprintln!("wait etcd: {e}");
        }
    }
}

fn free_port() -> u16 {
    autumn_test_ports::port()
}

/// Whether process `pid` holds the socket listening on 127.0.0.1:`port`:
/// tells our etcd from another test process's server that took the port
/// after `free_port` released it.
pub fn listens_on(pid: u32, port: u16) -> bool {
    let tcp = std::fs::read_to_string("/proc/net/tcp").unwrap_or_default();
    let want = format!("0100007F:{port:04X}");
    let inodes: std::collections::HashSet<String> = tcp
        .lines()
        .skip(1)
        .filter_map(|l| {
            let f: Vec<&str> = l.split_whitespace().collect();
            (f.len() > 9 && f[1] == want && f[3] == "0A").then(|| format!("socket:[{}]", f[9]))
        })
        .collect();
    std::fs::read_dir(format!("/proc/{pid}/fd")).into_iter().flatten().flatten().any(|fd| {
        std::fs::read_link(fd.path()).is_ok_and(|t| inodes.contains(t.to_string_lossy().as_ref()))
    })
}

/// Start etcd with `args` added, serving clients on `scheme` (`http` /
/// `https`). Ready when the child itself listens on the client port and a
/// client built by `config(addr)` answers; a child that exits first lost a
/// port and is started again on fresh ones. Returns the client address.
pub async fn start_etcd(
    args: &[String],
    scheme: &str,
    config: impl Fn(String) -> EtcdConfig,
) -> (Etcd, String, EtcdClient) {
    let bin = std::env::var("AUTUMN_TEST_ETCD_BIN").unwrap_or_else(|_| "etcd".to_string());
    for _ in 0..5 {
        let client_port = free_port();
        let addr = format!("127.0.0.1:{client_port}");
        let client_url = format!("{scheme}://{addr}");
        let peer_url = format!("http://127.0.0.1:{}", free_port());
        let dir = tempfile::tempdir().expect("etcd tempdir");
        let child = std::process::Command::new(&bin)
            .args(["--name", "n1", "--log-level", "error"])
            .arg("--data-dir")
            .arg(dir.path().join("data"))
            .args(["--listen-client-urls", &client_url, "--advertise-client-urls", &client_url])
            .args(["--listen-peer-urls", &peer_url, "--initial-advertise-peer-urls", &peer_url])
            .arg("--initial-cluster")
            .arg(format!("n1={peer_url}"))
            .args(args)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn `{bin}`: {e} — install etcd or set AUTUMN_TEST_ETCD_BIN"));
        let mut etcd = Etcd { child, _dir: dir };
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if etcd.child.try_wait().expect("etcd child status").is_some() {
                break; // lost a port: try fresh ones
            }
            if listens_on(etcd.child.id(), client_port) {
                if let Ok(c) = EtcdClient::connect_with(config(addr.clone())).await {
                    if c.get("health-check").await.is_ok() {
                        return (etcd, addr, c);
                    }
                }
            }
            assert!(Instant::now() < deadline, "etcd at {client_url} not ready after 30 s");
            compio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    panic!("etcd lost its ports to other processes five times");
}

/// PEM files of a test CA, an etcd server certificate it signs (for
/// `localhost` and 127.0.0.1), a client certificate it signs (CN
/// `autumn-test`, no such etcd user), and a second, unrelated CA.
pub struct Certs {
    pub dir: tempfile::TempDir,
    pub ca: PathBuf,
    pub server_cert: PathBuf,
    pub server_key: PathBuf,
    pub client_cert: PathBuf,
    pub client_key: PathBuf,
    pub other_ca: PathBuf,
}

pub fn make_certs() -> Certs {
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
        SanType,
    };
    let dir = tempfile::tempdir().expect("cert tempdir");
    let write = |name: &str, pem: String| -> PathBuf {
        let p = dir.path().join(name);
        std::fs::write(&p, pem).expect("write pem");
        p
    };
    let ca_of = |cn: &str| {
        let key = KeyPair::generate().expect("ca key");
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name.push(DnType::CommonName, cn);
        let cert = params.self_signed(&key).expect("ca cert");
        (cert, key)
    };
    let (ca, ca_key) = ca_of("autumn test ca");
    let (other_ca, _) = ca_of("unrelated ca");

    let server_key = KeyPair::generate().expect("server key");
    let mut params = CertificateParams::new(vec!["localhost".to_string()]).expect("server params");
    params.subject_alt_names.push(SanType::IpAddress("127.0.0.1".parse().unwrap()));
    // etcd also dials itself (its gRPC gateway) with this certificate.
    params.extended_key_usages =
        vec![ExtendedKeyUsagePurpose::ServerAuth, ExtendedKeyUsagePurpose::ClientAuth];
    params.distinguished_name.push(DnType::CommonName, "etcd");
    let server = params.signed_by(&server_key, &ca, &ca_key).expect("server cert");

    let client_key = KeyPair::generate().expect("client key");
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("client params");
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    params.distinguished_name.push(DnType::CommonName, "autumn-test");
    let client = params.signed_by(&client_key, &ca, &ca_key).expect("client cert");

    Certs {
        ca: write("ca.pem", ca.pem()),
        server_cert: write("server.pem", server.pem()),
        server_key: write("server-key.pem", server_key.serialize_pem()),
        client_cert: write("client.pem", client.pem()),
        client_key: write("client-key.pem", client_key.serialize_pem()),
        other_ca: write("other-ca.pem", other_ca.pem()),
        dir,
    }
}

/// etcd flags serving clients over TLS with `certs`, client certificates
/// required.
pub fn tls_args(certs: &Certs) -> Vec<String> {
    let p = |p: &Path| p.display().to_string();
    vec![
        "--cert-file".into(),
        p(&certs.server_cert),
        "--key-file".into(),
        p(&certs.server_key),
        "--trusted-ca-file".into(),
        p(&certs.ca),
        "--client-cert-auth".into(),
    ]
}

// etcdserverpb admin messages, for turning auth on.
#[derive(Clone, prost::Message)]
struct UserAddRequest {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    password: String,
}
#[derive(Clone, prost::Message)]
struct UserGrantRoleRequest {
    #[prost(string, tag = "1")]
    user: String,
    #[prost(string, tag = "2")]
    role: String,
}
#[derive(Clone, prost::Message)]
struct AuthEnableRequest {}
#[derive(Clone, prost::Message)]
struct AdminResponse {}

/// Create `root` with `password` and turn auth on.
pub async fn enable_auth(client: &EtcdClient, password: &str) {
    let _: AdminResponse = client
        .unary(
            "/etcdserverpb.Auth/UserAdd",
            &UserAddRequest { name: "root".into(), password: password.into() },
        )
        .await
        .expect("add root");
    let _: AdminResponse = client
        .unary(
            "/etcdserverpb.Auth/UserGrantRole",
            &UserGrantRoleRequest { user: "root".into(), role: "root".into() },
        )
        .await
        .expect("grant root");
    let _: AdminResponse = client
        .unary("/etcdserverpb.Auth/AuthEnable", &AuthEnableRequest {})
        .await
        .expect("enable auth");
}
