use anyhow::{Context, Result};
use autumn_manager::AutumnManager;
use autumn_params::manager::ManagerParams;
use autumn_params::Positional;
use autumn_etcd::{Credentials, EtcdConfig, EtcdEndpoint, TlsConfig};

// allocator hygiene — see crates/server/src/bin/extent_node.rs for
// the rationale and the MALLOC_CONF tuning explanation. Manager's peak
// RSS during etcd replay can also benefit, though the dominant case is
// the extent-node EC path.
#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// `_rjem_malloc_conf` (NOT `malloc_conf`): tikv-jemallocator 0.6 is `_rjem_`-
// prefixed, so the old unprefixed symbol was a silent no-op. `oversize_threshold:0`
// keeps large allocations in normal arenas (warm reuse) — see
// crates/server/src/bin/extent_node.rs for the full rationale.
// Override at runtime via `_RJEM_MALLOC_CONF` (cluster.sh / prod launcher).
#[cfg(target_os = "linux")]
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static malloc_conf: &[u8] = b"oversize_threshold:0\0";

const USAGE: &str = "usage: autumn-manager-server --cluster-secret-file <PATH> [options]";

/// Fast mode is a set of parameter defaults, applied first so an explicit
/// flag still wins.
fn parse_args() -> ManagerParams {
    let params = ManagerParams::default();
    if std::env::args().any(|a| a.replace('_', "-") == "--policy-fast-mode") {
        params.apply_policy_fast_mode();
    }
    autumn_params::parse_or_exit(USAGE, &[&params], Positional::Refuse);
    params
}

/// `--listen` with its name resolved, and the `--etcd` endpoints: every name
/// replaced by its addresses, each keeping the name for TLS to check.
fn resolve_address_flags(params: &ManagerParams) -> Result<Vec<EtcdEndpoint>, String> {
    let listen = params.listen.get();
    let ip = autumn_transport::resolve_host(&listen).map_err(|e| format!("--listen: {e}"))?;
    params.listen.set(ip.to_string());
    let mut endpoints: Vec<EtcdEndpoint> = Vec::new();
    for item in params.etcd.get().join(",").split(',').filter(|s| !s.trim().is_empty()) {
        let name = EtcdEndpoint::new(item).server_name;
        for sa in autumn_transport::resolve_host_port(item).map_err(|e| format!("--etcd: {e}"))? {
            let addr = sa.to_string();
            if !endpoints.iter().any(|e| e.addr == addr) {
                endpoints.push(EtcdEndpoint { addr, server_name: name.clone() });
            }
        }
    }
    params.etcd.set(endpoints.iter().map(|e| e.addr.clone()).collect());
    Ok(endpoints)
}

/// TLS and the etcd user from their flags; a file that does not load stops
/// the start.
fn etcd_config(params: &ManagerParams, endpoints: Vec<EtcdEndpoint>) -> Result<EtcdConfig> {
    let tls = match params.etcd_cacert.get() {
        None => None,
        Some(ca) => {
            let cert = params.etcd_cert.get();
            let key = params.etcd_key.get();
            let client = cert.as_deref().zip(key.as_deref());
            Some(TlsConfig::from_pem_files(&ca, client).context("etcd TLS")?)
        }
    };
    let credentials = match (params.etcd_user.get(), params.etcd_password_file.get()) {
        (Some(user), Some(path)) => {
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("read --etcd-password-file {}", path.display()))?;
            let password = text.trim_end_matches(['\n', '\r']).to_string();
            anyhow::ensure!(!password.is_empty(), "--etcd-password-file {} is empty", path.display());
            Some(Credentials { user, password })
        }
        _ => None,
    };
    Ok(EtcdConfig { endpoints, tls, credentials })
}

#[compio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let params = parse_args();
    // Names in the address flags become IP literals here, once.
    let etcd_endpoints = match resolve_address_flags(&params) {
        Ok(endpoints) => endpoints,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    };
    // `params` moves into the manager; what main needs is read first.
    let etcd = params.etcd.get();
    let fast_mode = params.policy_fast_mode.get();
    let signing_key_file = params.auth_signing_key_file.get();
    let auto_policy_default = params.auto_policy_default.get();
    if let Err(e) = autumn_rpc::peer_auth::install_for_server(params.cluster_secret_file.get().as_deref()) {
        eprintln!("error: {e}");
        std::process::exit(2);
    }
    let _ = autumn_transport::init_with(params.transport.get().into());
    let addr = autumn_transport::format_listen_addr(&params.listen.get(), params.port.get())
        .context("parse listen address")?;
    autumn_transport::check_listen_addr(addr, autumn_transport::current().kind()).ok();

    let manager = if etcd.is_empty() {
        tracing::warn!(
            "no --etcd endpoints given; running in-memory only (metadata will be lost on restart)"
        );
        AutumnManager::new_with_params(params)
    } else {
        tracing::info!("connecting to etcd: {:?}", etcd);
        let identity = autumn_manager::ManagerIdentity {
            id: params.manager_id.get(),
            address: addr.to_string(),
        };
        let config = etcd_config(&params, etcd_endpoints)?;
        tracing::info!(
            tls = config.tls.is_some(),
            user = config.credentials.as_ref().map(|c| c.user.as_str()),
            "etcd connection"
        );
        AutumnManager::new_with_etcd_config(config, identity, params)
            .await
            .context("connect to etcd")?
    };
    if fast_mode {
        tracing::warn!("--policy-fast-mode enabled: short policy windows, NOT FOR PRODUCTION");
    }

    // data-plane authz (opt-in). Loading a signing-key file ENABLES
    // it; without the flag the manager is not a KDC and PSes don't enforce.
    if let Some(path) = &signing_key_file {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read --auth-signing-key-file {path}"))?;
        let keyring = autumn_manager::authz::AuthzKeyring::from_file_contents(&text)
            .map_err(|e| anyhow::anyhow!("parse --auth-signing-key-file {path}: {e}"))?;
        manager.set_authz_keyring(keyring);
        tracing::info!("data-plane authz ENABLED (manager is a KDC)");
    }

    // Leader-fenced auto-policy controller (the web dashboard is now a
    // standalone app — see crates/server/src/bin/autumn_dashboard).
    // Seed the deploy-configured default active policy on a fresh cluster.
    // Validate the preset name up front — a typo must fail loud at startup, not
    // silently leave the controller Off. A seeded policy is Armed and actuates
    // (arming is per-policy via `autumn-op auto-policy start <NAME>`).
    if let Some(preset) = &auto_policy_default {
        if !AutumnManager::is_known_auto_policy_preset(preset) {
            anyhow::bail!(
                "--auto-policy-default {preset:?} is not a known preset \
                 (gc-only / maintenance / space-reclaim / balanced / aggressive)"
            );
        }
        manager.set_auto_policy_default(preset.clone());
    }
    // `new_with_etcd` already ran the first replay + election in the constructor
    // (which recorded whether a config was persisted), so this seeds a fresh
    // cluster and no-ops when a config already exists or no default was requested.
    manager.apply_auto_policy_default();

    tracing::info!("autumn-manager-server listening on {addr}");
    manager.serve(addr).await?;

    Ok(())
}
