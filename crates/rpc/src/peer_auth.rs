//! Cluster membership proof.
//!
//! VERSION_HELLO settles which version rule a connection is held to and
//! nothing else. This step settles whether a connection that declared itself
//! a cluster member (`Peer`) or an operator tool (`Admin`) holds the cluster
//! secret. A `Client` connection skips it: clients prove who they are with a
//! capability token (`AUTH_HELLO`), never with the cluster secret.
//!
//! It runs on the raw stream right after VERSION_HELLO succeeds, before either
//! side starts its business reader. Both sides prove the secret with an
//! HMAC-SHA256 over both nonces, so neither a stranger dialing in nor a
//! stranger listening on a member's address gets through, and the secret never
//! crosses the wire. It does not defend against an attacker who can read or
//! rewrite the traffic (no encryption, no channel binding): the threat is
//! anyone on the network who can open a connection.
//!
//! ```text
//! server -> challenge  "AUPA" | mode u8 (0 open, 1 required) | server_nonce[32]
//! client -> proof      "AUPA" | client_nonce[32] | client_mac[32]
//! server -> result     "AUPA" | verdict u8 (0 ok, 1 refused) | server_mac[32]
//! mac = HMAC-SHA256(secret, DOMAIN | side | service | role | server_nonce | client_nonce)
//! ```
//!
//! A server without a secret answers `open` and the exchange ends there. That
//! exists for in-process tests only: every server binary refuses to start
//! without `--cluster-secret-file`. A client holding a secret refuses an open
//! server, because answering `open` is exactly what an impostor would do.
use crate::version_hello::{encode_bootstrap, read_bootstrap, Negotiated, Role, Service, TIMEOUT};
use crate::{RpcError, StatusCode};
use compio::io::AsyncWriteExt;
use compio::BufResult;
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha256;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

pub const MSG_PEER_AUTH: u8 = 0xF1;
const NAME: &str = "PEER_AUTH";
const MAGIC: [u8; 4] = *b"AUPA";
const DOMAIN: &[u8] = b"autumn-rs peer-auth v1";
const NONCE_LEN: usize = 32;
const MAC_LEN: usize = 32;
const CHALLENGE_LEN: usize = 4 + 1 + NONCE_LEN;
const PROOF_LEN: usize = 4 + NONCE_LEN + MAC_LEN;
const RESULT_LEN: usize = 4 + 1 + MAC_LEN;
const MODE_OPEN: u8 = 0;
const MODE_REQUIRED: u8 = 1;
const VERDICT_OK: u8 = 0;
const VERDICT_REFUSED: u8 = 1;
const CLIENT_SIDE: u8 = b'C';
const SERVER_SIDE: u8 = b'S';
/// Shortest secret accepted. `autumn-op gen-cluster-secret` writes 64 hex
/// characters.
pub const MIN_SECRET_LEN: usize = 32;

pub struct ClusterSecret(Box<[u8]>);

impl std::fmt::Debug for ClusterSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClusterSecret(..)")
    }
}

impl ClusterSecret {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, String> {
        let bytes = bytes.into();
        if bytes.len() < MIN_SECRET_LEN {
            return Err(format!(
                "cluster secret is {} bytes; at least {MIN_SECRET_LEN} required \
                 (generate one with `autumn-op gen-cluster-secret`)",
                bytes.len()
            ));
        }
        Ok(Self(bytes.into_boxed_slice()))
    }

    /// Surrounding whitespace (the file's trailing newline) is not part of the
    /// secret.
    pub fn from_file(path: &std::path::Path) -> Result<Self, String> {
        let raw = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
        Self::new(raw.trim_ascii().to_vec()).map_err(|e| format!("{}: {e}", path.display()))
    }

    /// A fresh secret as 64 hex characters.
    pub fn generate() -> String {
        let mut b = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut b);
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    fn mac(
        &self,
        side: u8,
        n: &Negotiated,
        server_nonce: &[u8],
        client_nonce: &[u8],
    ) -> Hmac<Sha256> {
        let mut m = Hmac::<Sha256>::new_from_slice(&self.0).expect("HMAC accepts any key length");
        m.update(DOMAIN);
        m.update(&[side, n.service as u8, n.role as u8]);
        m.update(server_nonce);
        m.update(client_nonce);
        m
    }
}

static INSTALLED: OnceLock<ClusterSecret> = OnceLock::new();

/// Sets the secret this process proves when it dials and demands when it
/// accepts, on every Peer and Admin connection. Called once at startup from
/// `--cluster-secret-file`. Installing the same secret again is a no-op; a
/// different one is an error.
pub fn install(secret: ClusterSecret) -> Result<(), String> {
    let installed = INSTALLED.get_or_init(|| ClusterSecret(secret.0.clone()));
    if installed.0 == secret.0 {
        Ok(())
    } else {
        Err("a different cluster secret is already installed in this process".to_string())
    }
}

/// Set by `install_for_server`: in a server process, the manager refusing a
/// dial ends the process (`on_dial_failure`).
static REFUSAL_IS_FATAL: AtomicBool = AtomicBool::new(false);

/// This process's managers, as its `--manager` flag names them
/// (`designate_managers`).
static MANAGERS: OnceLock<Vec<std::net::SocketAddr>> = OnceLock::new();

/// What every server binary runs before it serves: install the secret named by
/// `--cluster-secret-file`, or refuse to start. From then on, failing PEER_AUTH
/// against one of its managers is fatal.
pub fn install_for_server(path: Option<&std::path::Path>) -> Result<(), String> {
    let path = path.ok_or_else(|| {
        "--cluster-secret-file is required (generate one with `autumn-op gen-cluster-secret`)"
            .to_string()
    })?;
    install(ClusterSecret::from_file(path)?)?;
    REFUSAL_IS_FATAL.store(true, Ordering::Relaxed);
    Ok(())
}

/// Records which addresses are this process's managers: `list` is its
/// `--manager` flag, comma-separated `IP:PORT`s (an `http://` prefix is
/// ignored, as everywhere else). Called once at startup by the partition server
/// and the extent node; the manager dials no manager.
pub fn designate_managers(list: &str) -> Result<(), String> {
    let addrs = list
        .split(',')
        .map(|a| {
            let a = a.trim();
            let bare = a
                .trim_start_matches("http://")
                .trim_start_matches("https://");
            bare.parse()
                .map_err(|e| format!("--manager {a:?} is not IP:PORT: {e}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    MANAGERS
        .set(addrs)
        .map_err(|_| "the managers are already designated in this process".to_string())
}

/// What a dialer does when `initiate` failed against `peer`.
///
/// Only a refusal counts (`initiate` reports one, and only one, as
/// `PermissionDenied`); a timeout or a broken connection is not a refusal and
/// passes through. A refusal means the two ends hold different cluster
/// secrets, and since a secret is read once at startup and rotating it is a
/// full stop, retrying never changes it. Who refused decides what happens, and
/// "who" is the address this process dialed, never what the other end says it
/// is: a stranger answering VERSION_HELLO as the manager must not be able to
/// end a process.
///
/// - One of this process's managers (`designate_managers`), the authority on
///   membership: this process is not a member of the cluster it serves. A
///   server process logs and exits.
/// - Any other address: that end is the outsider (a misconfigured process, or
///   a stranger on a member's address). It is logged at ERROR and the error is
///   returned, so the caller sees an unreachable node and goes on as it would
///   for one. Should this process be the outsider instead, its next call to a
///   manager is refused and it exits then.
pub fn on_dial_failure(e: &RpcError, peer: std::net::SocketAddr, declared: Service) {
    if !matches!(
        e,
        RpcError::Status {
            code: StatusCode::PermissionDenied,
            ..
        }
    ) {
        return;
    }
    let is_manager = MANAGERS.get().is_some_and(|m| m.contains(&peer));
    if is_manager && REFUSAL_IS_FATAL.load(Ordering::Relaxed) {
        tracing::error!(
            %peer,
            error = %e,
            "PEER_AUTH failed against this process's manager: this process holds a different \
             cluster secret and is not a member of this cluster; exiting",
        );
        std::process::exit(1);
    }
    tracing::error!(
        %peer,
        ?declared,
        error = %e,
        "PEER_AUTH failed: the peer holds a different cluster secret; treating it as unreachable",
    );
}

pub fn installed() -> Option<&'static ClusterSecret> {
    INSTALLED.get()
}

fn nonce() -> [u8; NONCE_LEN] {
    let mut b = [0u8; NONCE_LEN];
    rand::rngs::OsRng.fill_bytes(&mut b);
    b
}

fn refused(message: String) -> RpcError {
    RpcError::status(StatusCode::PermissionDenied, format!("{NAME}: {message}"))
}

fn malformed(message: &str) -> RpcError {
    RpcError::status(StatusCode::InvalidArgument, format!("{NAME}: {message}"))
}

/// `ctrl` minus the magic, which must be there and must be `len` long.
fn body(ctrl: &[u8], len: usize) -> Result<&[u8], RpcError> {
    if ctrl.len() != len || ctrl[..4] != MAGIC {
        return Err(malformed("unknown control layout"));
    }
    Ok(&ctrl[4..])
}

async fn send(
    writer: &mut autumn_transport::WriteHalf,
    parts: &[&[u8]],
    response: bool,
) -> Result<(), RpcError> {
    let mut ctrl = MAGIC.to_vec();
    for p in parts {
        ctrl.extend_from_slice(p);
    }
    let BufResult(result, _) = writer
        .write_all(encode_bootstrap(MSG_PEER_AUTH, &ctrl, response))
        .await;
    result?;
    Ok(())
}

/// The dialing side. `n` is what VERSION_HELLO negotiated on this connection.
pub async fn initiate(
    reader: &mut autumn_transport::ReadHalf,
    writer: &mut autumn_transport::WriteHalf,
    n: &Negotiated,
    secret: Option<&ClusterSecret>,
) -> Result<(), RpcError> {
    if n.role == Role::Client {
        return Ok(());
    }
    compio::time::timeout(TIMEOUT, async {
        let challenge = read_bootstrap(reader, NAME, MSG_PEER_AUTH, true, CHALLENGE_LEN).await?;
        let challenge = body(&challenge, CHALLENGE_LEN)?;
        let (mode, server_nonce) = (challenge[0], &challenge[1..]);
        let secret = match (mode, secret) {
            (MODE_OPEN, None) => return Ok(()),
            (MODE_OPEN, Some(_)) => {
                return Err(refused(format!(
                    "{:?} does not authenticate cluster members, but this process holds \
                     a cluster secret",
                    n.service
                )))
            }
            (MODE_REQUIRED, None) => {
                return Err(refused(format!(
                    "{:?} requires the cluster secret; start this process with \
                     --cluster-secret-file",
                    n.service
                )))
            }
            (MODE_REQUIRED, Some(s)) => s,
            _ => return Err(malformed("unknown challenge mode")),
        };
        let client_nonce = nonce();
        let proof = secret
            .mac(CLIENT_SIDE, n, server_nonce, &client_nonce)
            .finalize()
            .into_bytes();
        send(writer, &[&client_nonce, &proof], false).await?;
        let result = read_bootstrap(reader, NAME, MSG_PEER_AUTH, true, RESULT_LEN).await?;
        let result = body(&result, RESULT_LEN)?;
        match result[0] {
            VERDICT_OK => {}
            VERDICT_REFUSED => {
                return Err(refused(format!(
                    "{:?} refused this process's cluster secret: the two hold different \
                     secrets",
                    n.service
                )))
            }
            _ => return Err(malformed("unknown verdict")),
        }
        secret
            .mac(SERVER_SIDE, n, server_nonce, &client_nonce)
            .verify_slice(&result[1..])
            .map_err(|_| {
                refused(format!(
                    "{:?} answered with a wrong cluster-secret proof",
                    n.service
                ))
            })
    })
    .await
    .map_err(|_| RpcError::Timeout(TIMEOUT))?
}

/// The accepting side. `peer` only labels the WARN a refusal logs, so an
/// operator can find the misconfigured process.
pub async fn accept(
    reader: &mut autumn_transport::ReadHalf,
    writer: &mut autumn_transport::WriteHalf,
    n: &Negotiated,
    secret: Option<&ClusterSecret>,
    peer: &str,
) -> Result<(), RpcError> {
    if n.role == Role::Client {
        return Ok(());
    }
    compio::time::timeout(TIMEOUT, async {
        let Some(secret) = secret else {
            return send(writer, &[&[MODE_OPEN], &[0; NONCE_LEN]], true).await;
        };
        let server_nonce = nonce();
        send(writer, &[&[MODE_REQUIRED], &server_nonce], true).await?;
        let proof = match read_bootstrap(reader, NAME, MSG_PEER_AUTH, false, PROOF_LEN).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    peer,
                    role = ?n.role,
                    service = ?n.service,
                    error = %e,
                    "PEER_AUTH: connection gave no cluster-secret proof"
                );
                return Err(e);
            }
        };
        let proof = body(&proof, PROOF_LEN)?;
        let (client_nonce, client_mac) = proof.split_at(NONCE_LEN);
        if secret
            .mac(CLIENT_SIDE, n, &server_nonce, client_nonce)
            .verify_slice(client_mac)
            .is_err()
        {
            send(writer, &[&[VERDICT_REFUSED], &[0; MAC_LEN]], true).await?;
            tracing::warn!(
                peer,
                role = ?n.role,
                service = ?n.service,
                "PEER_AUTH refused a connection holding a different cluster secret"
            );
            return Err(refused("cluster secret mismatch".to_string()));
        }
        let server_mac = secret
            .mac(SERVER_SIDE, n, &server_nonce, client_nonce)
            .finalize()
            .into_bytes();
        send(writer, &[&[VERDICT_OK], &server_mac], true).await
    })
    .await
    .map_err(|_| RpcError::Timeout(TIMEOUT))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::version_hello::Service;
    use compio::io::AsyncRead;

    fn secret(fill: u8) -> ClusterSecret {
        ClusterSecret::new(vec![fill; MIN_SECRET_LEN]).unwrap()
    }

    fn negotiated(role: Role) -> Negotiated {
        Negotiated {
            role,
            service: Service::ExtentNode,
            remote_wire: crate::WIRE_VERSION,
            min_client: crate::MIN_CLIENT_WIRE_VERSION,
            max_client: crate::WIRE_VERSION,
            declared_version: crate::WIRE_VERSION,
        }
    }

    async fn socket_pair() -> (autumn_transport::Conn, autumn_transport::Conn) {
        let listener = compio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = compio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (
            autumn_transport::Conn::Tcp(client),
            autumn_transport::Conn::Tcp(server),
        )
    }

    /// Runs one exchange; returns (dialer result, acceptor result).
    async fn exchange(
        role: Role,
        dialer: Option<u8>,
        acceptor: Option<u8>,
    ) -> (Result<(), RpcError>, Result<(), RpcError>) {
        let (client, server) = socket_pair().await;
        let task = compio::runtime::spawn(async move {
            let (mut rd, mut wr) = server.into_split();
            let s = acceptor.map(secret);
            let r = accept(&mut rd, &mut wr, &negotiated(role), s.as_ref(), "test").await;
            drop((rd, wr));
            r
        });
        let (mut rd, mut wr) = client.into_split();
        let s = dialer.map(secret);
        let dialed = initiate(&mut rd, &mut wr, &negotiated(role), s.as_ref()).await;
        drop((rd, wr));
        (dialed, task.await.unwrap())
    }

    #[compio::test]
    async fn members_holding_the_same_secret_are_admitted() {
        for role in [Role::Peer, Role::Admin] {
            let (d, a) = exchange(role, Some(1), Some(1)).await;
            assert!(d.is_ok(), "{d:?}");
            assert!(a.is_ok(), "{a:?}");
        }
    }

    #[compio::test]
    async fn a_different_or_missing_secret_is_refused_by_both_sides() {
        for (dialer, acceptor) in [(Some(2), Some(1)), (None, Some(1)), (Some(1), None)] {
            for role in [Role::Peer, Role::Admin] {
                let (d, a) = exchange(role, dialer, acceptor).await;
                assert!(
                    matches!(&d, Err(RpcError::Status { code: StatusCode::PermissionDenied, .. })),
                    "dialer {dialer:?} acceptor {acceptor:?}: {d:?}"
                );
                // An open acceptor cannot tell; the dialer refuses it.
                if acceptor.is_some() {
                    assert!(a.is_err(), "acceptor admitted dialer {dialer:?}");
                }
            }
        }
    }

    #[compio::test]
    async fn without_secrets_on_either_side_the_exchange_is_open() {
        let (d, a) = exchange(Role::Peer, None, None).await;
        assert!(d.is_ok() && a.is_ok(), "{d:?} {a:?}");
    }

    #[compio::test]
    async fn a_client_connection_exchanges_nothing() {
        let (client, server) = socket_pair().await;
        let (mut srd, mut swr) = server.into_split();
        let s = secret(1);
        accept(&mut srd, &mut swr, &negotiated(Role::Client), Some(&s), "test")
            .await
            .unwrap();
        let (mut rd, mut wr) = client.into_split();
        initiate(&mut rd, &mut wr, &negotiated(Role::Client), None)
            .await
            .unwrap();
        drop((srd, swr));
        let BufResult(n, _) = rd.read(vec![0; 1]).await;
        assert_eq!(n.unwrap(), 0, "nothing was written for a Client connection");
    }

    #[compio::test]
    async fn a_replayed_proof_does_not_pass_a_fresh_challenge() {
        // Record a member's proof from one exchange, replay it on another.
        let (client, server) = socket_pair().await;
        let task = compio::runtime::spawn(async move {
            let (mut rd, mut wr) = server.into_split();
            let s = secret(1);
            accept(&mut rd, &mut wr, &negotiated(Role::Peer), Some(&s), "test").await
        });
        let (mut rd, mut wr) = client.into_split();
        let n = negotiated(Role::Peer);
        let challenge = read_bootstrap(&mut rd, NAME, MSG_PEER_AUTH, true, CHALLENGE_LEN)
            .await
            .unwrap();
        let server_nonce = body(&challenge, CHALLENGE_LEN).unwrap()[1..].to_vec();
        let client_nonce = [7u8; NONCE_LEN];
        let recorded = secret(1)
            .mac(CLIENT_SIDE, &n, &server_nonce, &client_nonce)
            .finalize()
            .into_bytes();
        send(&mut wr, &[&client_nonce, &recorded], false).await.unwrap();
        task.await.unwrap().unwrap();

        let (client, server) = socket_pair().await;
        let task = compio::runtime::spawn(async move {
            let (mut rd, mut wr) = server.into_split();
            let s = secret(1);
            accept(&mut rd, &mut wr, &negotiated(Role::Peer), Some(&s), "test").await
        });
        let (mut rd, mut wr) = client.into_split();
        read_bootstrap(&mut rd, NAME, MSG_PEER_AUTH, true, CHALLENGE_LEN)
            .await
            .unwrap();
        send(&mut wr, &[&client_nonce, &recorded], false).await.unwrap();
        assert!(task.await.unwrap().is_err(), "a replayed proof was admitted");
    }

    #[compio::test]
    async fn a_listener_that_cannot_prove_the_secret_is_refused() {
        // An impostor on a member's address: it demands a proof, ignores it and
        // answers "ok" without a valid MAC of its own.
        let (client, server) = socket_pair().await;
        let task = compio::runtime::spawn(async move {
            let (mut rd, mut wr) = server.into_split();
            send(&mut wr, &[&[MODE_REQUIRED], &[3; NONCE_LEN]], true).await.unwrap();
            read_bootstrap(&mut rd, NAME, MSG_PEER_AUTH, false, PROOF_LEN)
                .await
                .unwrap();
            send(&mut wr, &[&[VERDICT_OK], &[0; MAC_LEN]], true).await.unwrap();
        });
        let (mut rd, mut wr) = client.into_split();
        let s = secret(1);
        let r = initiate(&mut rd, &mut wr, &negotiated(Role::Peer), Some(&s)).await;
        task.await.unwrap();
        assert!(
            matches!(&r, Err(RpcError::Status { message, .. }) if message.contains("wrong cluster-secret proof")),
            "{r:?}"
        );
    }

    #[test]
    fn short_secrets_are_rejected_and_a_file_is_trimmed() {
        assert!(ClusterSecret::new(vec![1; MIN_SECRET_LEN - 1]).is_err());
        let dir = std::env::temp_dir().join(format!("peer_auth_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("secret");
        let generated = ClusterSecret::generate();
        assert_eq!(generated.len(), 64);
        std::fs::write(&path, format!("{generated}\n")).unwrap();
        let read = ClusterSecret::from_file(&path).unwrap();
        assert_eq!(&*read.0, generated.as_bytes());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
