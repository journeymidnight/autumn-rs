//! F-AUTHZ-1 cross-tenant end-to-end test against a LIVE authz-enabled cluster.
//!
//! `#[ignore]` — driven by `tests/run_authz_e2e.sh`, which brings up an isolated
//! authz-enabled cluster (manager with a signing key + protected `mem/`), creates
//! two tenants, and passes their credentials in via env:
//!
//! ```bash
//! bash crates/autumn-memory/tests/run_authz_e2e.sh
//! ```
//!
//! Verifies the FULL wire path — manager mints → client AUTH_HELLOs → PS enforces:
//!   * a tenant's client reads/writes its own `mem/{tenant}/` prefix,
//!   * is DENIED (PermissionDenied) on another tenant's prefix,
//!   * an anonymous client is denied on any protected `mem/` key,
//!   * a non-protected key is ungated,
//!   * the MemoryStore credential pass-through works end to end.

use autumn_client::{AutumnError, ClusterClient};
use autumn_memory::MemoryStore;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set by run_authz_e2e.sh"))
}

fn hex_decode(s: &str) -> Vec<u8> {
    assert!(s.is_ascii() && s.len() % 2 == 0, "bad hex credential");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn is_denied(e: &AutumnError) -> bool {
    matches!(e, AutumnError::PermissionDenied(_))
}

#[test]
#[ignore = "needs a live authz-enabled cluster (run tests/run_authz_e2e.sh)"]
fn cross_tenant_isolation() {
    let mgr = env("AUTUMN_AUTHZ_E2E_MANAGER");
    let acme_cred = hex_decode(&env("AUTUMN_AUTHZ_E2E_ACME_CRED"));
    let other_cred = hex_decode(&env("AUTUMN_AUTHZ_E2E_OTHER_CRED"));

    compio::runtime::Runtime::new()
        .expect("compio runtime")
        .block_on(async move {
            // ── tenant "acme" (granted mem/acme/) ─────────────────────────
            let acme = ClusterClient::connect_with_credential(&mgr, "acme", acme_cred)
                .await
                .expect("connect acme");

            // own prefix: write + read back
            acme.put(b"mem/acme/authz-e2e/k1", b"v1")
                .await
                .expect("acme writes its own prefix");
            assert_eq!(
                acme.get(b"mem/acme/authz-e2e/k1").await.expect("acme get"),
                Some(b"v1".to_vec()),
            );

            // cross-tenant prefix: DENIED (read + write)
            let err = acme.get(b"mem/other/authz-e2e/k1").await.unwrap_err();
            assert!(is_denied(&err), "cross-tenant GET must be denied, got {err:?}");
            let err = acme.put(b"mem/other/authz-e2e/k1", b"x").await.unwrap_err();
            assert!(is_denied(&err), "cross-tenant PUT must be denied, got {err:?}");

            // non-protected namespace (outside mem/): ungated → allowed
            acme.put(b"scratch/authz-e2e/k", b"ok")
                .await
                .expect("non-protected key is ungated");

            // ── tenant "other" (granted mem/other/) ───────────────────────
            let other = ClusterClient::connect_with_credential(&mgr, "other", other_cred)
                .await
                .expect("connect other");
            other
                .put(b"mem/other/authz-e2e/k1", b"w1")
                .await
                .expect("other writes its own prefix");
            let err = other.get(b"mem/acme/authz-e2e/k1").await.unwrap_err();
            assert!(is_denied(&err), "other reading acme must be denied, got {err:?}");
            // acme's key is intact + isolated (other couldn't touch it)
            assert_eq!(
                acme.get(b"mem/acme/authz-e2e/k1").await.expect("acme get 2"),
                Some(b"v1".to_vec()),
            );

            // ── anonymous client (no credential) → denied on protected ────
            let anon = ClusterClient::connect(&mgr).await.expect("connect anon");
            let err = anon.get(b"mem/acme/authz-e2e/k1").await.unwrap_err();
            assert!(
                is_denied(&err),
                "anonymous GET on protected prefix must be denied, got {err:?}"
            );

            // ── MemoryStore credential pass-through (full memory path) ────
            let store = MemoryStore::connect_with_credential(
                &mgr,
                "acme",
                "authz-e2e-agent",
                hex_decode(&env("AUTUMN_AUTHZ_E2E_ACME_CRED")),
            )
            .await
            .expect("MemoryStore connect_with_credential");
            store
                .put_fact("ns", "greeting", b"hello", None)
                .await
                .expect("put_fact under mem/acme/");
            assert_eq!(
                store.get_fact("ns", "greeting").await.expect("get_fact"),
                Some(b"hello".to_vec()),
            );

            println!("AUTHZ E2E OK: cross-tenant isolation + anon deny + ungated + MemoryStore pass-through");
        });
}
