//! The client against an etcd with TLS (client certificates required) and
//! password auth: every call the manager makes works, a refused token is
//! replaced, and a wrong password or an untrusted certificate fails the
//! connect.

mod common;

use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(10);

use autumn_etcd::proto::{TxnRequest, TxnResponse};
use autumn_etcd::{Cmp, Credentials, EtcdClient, EtcdConfig, EtcdEndpoint, Op, TlsConfig};

const PASSWORD: &str = "s3cret";

fn root() -> Option<Credentials> {
    Some(Credentials { user: "root".into(), password: PASSWORD.into() })
}

/// TLS with the client certificate; `server_name` replaces the address host.
fn tls_config(certs: &common::Certs, addr: &str, server_name: &str, credentials: Option<Credentials>) -> EtcdConfig {
    let tls = TlsConfig::from_pem_files(&certs.ca, Some((&certs.client_cert, &certs.client_key)))
        .expect("tls config");
    EtcdConfig {
        endpoints: vec![EtcdEndpoint { addr: addr.to_string(), server_name: server_name.to_string() }],
        tls: Some(tls),
        credentials,
    }
}

fn create(key: &str, value: &str) -> TxnRequest {
    TxnRequest {
        compare: vec![Cmp::create_revision(key, 0)],
        success: vec![Op::put(key, value)],
        failure: vec![],
    }
}

async fn assert_created(client: &EtcdClient, key: &str, value: &str, resp: TxnResponse) {
    assert!(resp.succeeded, "create txn on {key} lost its compare: sent twice?");
    let got = client.get(key).await.expect("read back");
    assert_eq!(got.kvs.len(), 1, "{key} missing");
    assert_eq!(got.kvs[0].value, value.as_bytes());
}

#[compio::test]
async fn tls_and_password_auth_carry_every_call() {
    let certs = common::make_certs();
    let (_etcd, addr, admin) = common::start_etcd(&common::tls_args(&certs), "https", |addr| {
        tls_config(&certs, &addr, "localhost", None)
    })
    .await;
    common::enable_auth(&admin, PASSWORD).await;

    // Auth is on: without a token the request is refused.
    let anon = EtcdClient::connect_with(tls_config(&certs, &addr, "localhost", None), TIMEOUT)
        .await
        .expect("TLS connect without a user");
    assert!(anon.put("k", "v").await.is_err(), "auth is not enforced");

    // Certificate checked against the IP SAN this time.
    let client = EtcdClient::connect_with(tls_config(&certs, &addr, "127.0.0.1", root()), TIMEOUT)
        .await
        .expect("connect as root");
    client.put("k", "v").await.expect("put");
    assert_eq!(client.get("k").await.expect("get").kvs[0].value, b"v");
    let resp = client.txn(create("t", "1")).await.expect("txn");
    assert_created(&client, "t", "1", resp).await;

    let lease = client.lease_grant(10).await.expect("lease grant").id;
    client.put_with_lease("leased", "x", lease).await.expect("put with lease");
    let keeper = client.lease_keep_alive(lease, Duration::from_secs(2)).await.expect("keeper");
    assert!(keeper.keep_alive().await.expect("keep alive").ttl > 0);

    client.lease_revoke(lease).await.expect("lease revoke");
    assert!(client.get("leased").await.expect("get").kvs.is_empty(), "revoke left the key");
}

#[compio::test]
async fn wrong_password_or_certificate_fails_the_connect() {
    let certs = common::make_certs();
    let (_etcd, addr, admin) = common::start_etcd(&common::tls_args(&certs), "https", |addr| {
        tls_config(&certs, &addr, "localhost", None)
    })
    .await;
    common::enable_auth(&admin, PASSWORD).await;

    let wrong = Some(Credentials { user: "root".into(), password: "nope".into() });
    let err = EtcdClient::connect_with(tls_config(&certs, &addr, "localhost", wrong), TIMEOUT)
        .await
        .err()
        .expect("wrong password connected");
    assert!(format!("{err:#}").contains("authentication failed"), "{err:#}");

    let mut other = tls_config(&certs, &addr, "localhost", root());
    other.tls = Some(TlsConfig::from_pem_files(&certs.other_ca, None).expect("other ca"));
    let err = EtcdClient::connect_with(other, TIMEOUT).await.err().expect("untrusted server connected");
    assert!(format!("{err:#}").contains("tls handshake"), "{err:#}");

    let err = EtcdClient::connect_with(tls_config(&certs, &addr, "etcd.example", root()), TIMEOUT)
        .await
        .err()
        .expect("certificate accepted for another name");
    assert!(format!("{err:#}").contains("tls handshake"), "{err:#}");
}

/// Tokens expire after a second idle. Each kind of call meets an expired
/// token once: the shared connection on a read and on a txn (sent again: etcd
/// refused it unapplied), the keeper on its own connection.
#[compio::test]
async fn expired_token_is_replaced() {
    let args = ["--auth-token-ttl".to_string(), "1".to_string()];
    let (_etcd, addr, admin) =
        common::start_etcd(&args, "http", |addr| EtcdConfig::plain(&[addr])).await;
    common::enable_auth(&admin, PASSWORD).await;

    let mut config = EtcdConfig::plain(&[addr]);
    config.credentials = root();
    let client = EtcdClient::connect_with(config, TIMEOUT).await.expect("connect as root");
    client.put("k", "v").await.expect("put");
    let lease = client.lease_grant(30).await.expect("lease grant").id;
    let keeper = client.lease_keep_alive(lease, Duration::from_secs(2)).await.expect("keeper");

    let idle = || compio::time::sleep(Duration::from_secs(3));
    idle().await;
    assert_eq!(client.get("k").await.expect("get after expiry").kvs[0].value, b"v");
    idle().await;
    let resp = client.txn(create("t", "1")).await.expect("txn after expiry");
    assert_created(&client, "t", "1", resp).await;
    idle().await;
    assert!(keeper.keep_alive().await.expect("keep alive after expiry").ttl > 0);
}
