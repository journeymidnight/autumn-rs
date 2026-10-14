use anyhow::{Context, Result};
use autumn_manager::AutumnManager;
use autumn_params::manager::ManagerParams;
use autumn_transport::TransportKind;

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

struct Args {
    port: u16,
    etcd: Vec<String>,
    /// `--manager-id`: this manager's identity in the membership. Required
    /// with `--etcd`; the operator keeps it unique, and a second process
    /// with the same id waits until the first is gone.
    manager_id: u64,
    bind_host: String,
    transport: TransportKind,
    /// path to the Ed25519 signing-key file (KDC private material).
    /// `None` = data-plane authz DISABLED (opt-in). Format: one key per line,
    /// `<kid> <hex-32-byte-seed> [disabled]`. Generate via
    /// `autumn-op gen-signing-key`.
    auth_signing_key_file: Option<String>,
    /// `--cluster-secret-file`: required. Peer and Admin connections prove it.
    cluster_secret_file: Option<std::path::PathBuf>,
    /// seed this preset as the active policy (Armed) on a FRESH cluster. Deploy
    /// layer passes `balanced`; unset = controller stays Off (cluster.sh /
    /// tests).
    auto_policy_default: Option<String>,
    policy_fast_mode: bool,
    /// Every tunable; see `autumn_params::manager`.
    params: ManagerParams,
}

fn usage() -> String {
    format!(
        "usage: autumn-manager-server [options]\n\
         \n\
         \x20 --port <n>                    listen port (default 9001)\n\
         \x20 --listen <host>               bind host (default 0.0.0.0)\n\
         \x20 --etcd <a,b,...>              etcd endpoints (none = in memory only)\n\
         \x20 --manager-id <n>              non-zero id, required with --etcd\n\
         \x20 --transport tcp|ucx\n\
         \x20 --cluster-secret-file <path>  required\n\
         \x20 --auth-signing-key-file <path> turns on data-plane authz\n\
         \x20 --auto-policy-default <preset> armed policy for a fresh cluster\n\
         \x20 --policy-fast-mode            short policy windows for dev/test clusters\n\
         \n\
         parameters:\n{}",
        autumn_params::help::<ManagerParams>()
    )
}

fn parse_args() -> Args {
    let mut port: u16 = 9001;
    let mut etcd: Vec<String> = Vec::new();
    let mut manager_id: u64 = 0;
    let mut bind_host = String::from("0.0.0.0");
    let mut transport = TransportKind::Tcp;
    let mut auth_signing_key_file: Option<String> = None;
    let mut cluster_secret_file: Option<std::path::PathBuf> = None;
    let mut auto_policy_default: Option<String> = None;
    let params = ManagerParams::default();

    let raw: Vec<String> = std::env::args().collect();
    // Fast mode is a set of defaults; applied first so an explicit flag wins.
    let policy_fast_mode = raw.iter().any(|a| a == "--policy-fast-mode");
    if policy_fast_mode {
        params.apply_policy_fast_mode();
    }
    let mut i = 1;
    while i < raw.len() {
        match raw[i].as_str() {
            "--help" | "-h" => {
                print!("{}", usage());
                std::process::exit(0);
            }
            "--port" => {
                i += 1;
                port = raw[i].parse().expect("--port must be a number");
            }
            "--etcd" => {
                i += 1;
                for ep in raw[i].split(',') {
                    etcd.push(ep.trim().to_string());
                }
            }
            "--manager-id" => {
                i += 1;
                manager_id = raw[i].parse().expect("--manager-id must be a number");
            }
            "--listen" => {
                i += 1;
                bind_host = raw[i].clone();
            }
            "--transport" => {
                i += 1;
                transport = autumn_transport::parse_transport_flag(&raw[i]).unwrap_or_else(|bad| {
                    eprintln!("--transport must be `tcp` or `ucx`, got {bad:?}");
                    std::process::exit(2);
                });
            }
            // --auto-split / --auto-merge removed. Mechanism /
            // policy separation puts dispatch decisions in an external
            // controller. Read `client policy` + call `client split` /
            // `client merge` to act.
            "--auto-split" | "--auto-merge" => {
                eprintln!(
                    "{}: removed. Use `client policy` + `client {}` to drive policy externally.",
                    raw[i],
                    if raw[i] == "--auto-split" { "split" } else { "merge" },
                );
                std::process::exit(2);
            }
            "--policy-fast-mode" => {}
            // The manager exports no metrics; Prometheus finds the PS and EN
            // endpoints through `autumn-op info --file-sd`.
            "--metrics-port" | "--metrics-listen" => {
                eprintln!(
                    "error: {} was removed: the manager serves no /metrics \
                     (Prometheus targets: autumn-op info --file-sd)",
                    raw[i]
                );
                std::process::exit(2);
            }
            // ── manager-as-KDC (data-plane authz) ────────────
            "--auth-signing-key-file" => {
                i += 1;
                auth_signing_key_file = Some(raw[i].clone());
            }
            "--cluster-secret-file" => {
                i += 1;
                cluster_secret_file = Some(raw[i].clone().into());
            }
            "--admin-token" | "--admin-token-file" => {
                eprintln!(
                    "error: {} was removed: admin operations are authorized by the \
                     cluster secret (--cluster-secret-file)",
                    raw[i]
                );
                std::process::exit(2);
            }
            "--auto-policy-default" => {
                i += 1;
                auto_policy_default = Some(raw[i].clone());
            }
            other => match autumn_params::take_flag(&params, other, raw.get(i + 1).map(|s| s.as_str())) {
                Ok(autumn_params::Flag::Taken) => i += 1,
                Ok(autumn_params::Flag::Unknown) => {
                    eprintln!("error: unknown argument {other:?}\n\n{}", usage());
                    std::process::exit(2);
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(2);
                }
            },
        }
        i += 1;
    }

    if !etcd.is_empty() && manager_id == 0 {
        eprintln!("error: --manager-id <N> (non-zero) is required with --etcd");
        std::process::exit(2);
    }
    if let Err(e) = autumn_params::CrossCheck::cross_check(&params) {
        eprintln!("error: {e}");
        std::process::exit(2);
    }

    Args {
        port,
        etcd,
        manager_id,
        bind_host,
        transport,
        auth_signing_key_file,
        cluster_secret_file,
        auto_policy_default,
        policy_fast_mode,
        params,
    }
}

#[compio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = parse_args();
    if let Err(e) = autumn_rpc::peer_auth::install_for_server(args.cluster_secret_file.as_deref()) {
        eprintln!("error: {e}");
        std::process::exit(2);
    }
    let _ = autumn_transport::init_with(args.transport);
    let addr = autumn_transport::format_listen_addr(&args.bind_host, args.port)
        .context("parse listen address")?;
    autumn_transport::check_listen_addr(addr, autumn_transport::current().kind()).ok();

    let manager = if args.etcd.is_empty() {
        tracing::warn!(
            "no --etcd endpoints given; running in-memory only (metadata will be lost on restart)"
        );
        AutumnManager::new_with_params(args.params)
    } else {
        tracing::info!("connecting to etcd: {:?}", args.etcd);
        let identity = autumn_manager::ManagerIdentity {
            id: args.manager_id,
            address: addr.to_string(),
        };
        AutumnManager::new_with_etcd_params(args.etcd, identity, args.params)
            .await
            .context("connect to etcd")?
    };
    if args.policy_fast_mode {
        tracing::warn!("--policy-fast-mode enabled: short policy windows, NOT FOR PRODUCTION");
    }

    // data-plane authz (opt-in). Loading a signing-key file ENABLES
    // it; without the flag the manager is not a KDC and PSes don't enforce.
    if let Some(path) = &args.auth_signing_key_file {
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
    if let Some(preset) = &args.auto_policy_default {
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
