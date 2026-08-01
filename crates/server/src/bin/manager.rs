use anyhow::{Context, Result};
use autumn_manager::AutumnManager;
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
    bind_host: String,
    transport: TransportKind,
    /// enable fast-mode policy thresholds for load testing —
    /// 1-bucket / 5 s tick / 1 MiB GC debt / 4 MiB compact pending /
    /// 30 s cooldowns. Production should never use this; the default
    /// is `false` (production thresholds = 1 GiB / 4 GiB / 5-bucket /
    /// 60 s tick / 5-min cooldown).
    policy_fast_mode: bool,
    /// (was env): MSG_REPORT_DISK_FAILURE sliding window
    /// length in seconds. `None` = library default (60 s).
    report_disk_failure_window_secs: Option<u64>,
    /// (was env): MSG_REPORT_DISK_FAILURE distinct-reporter
    /// quorum threshold. `None` = library default (3).
    report_disk_failure_quorum: Option<usize>,
    /// Observability batch 1: Prometheus `/metrics` HTTP port.
    /// `None` = endpoint disabled (zero cost).
    metrics_port: Option<u16>,
    /// Bind host for /metrics only. `None` = follow `--listen`. The
    /// endpoint is unauthenticated — operators exposing the RPC plane
    /// on 0.0.0.0 can pin metrics to 127.0.0.1 with this.
    metrics_listen: Option<String>,
    /// ENOSPC-1: allocation free-space floor (bytes). Nodes whose best
    /// disk has less free are soft-avoided by extent allocation.
    /// `None` = library default (256 MiB); 0 = disabled.
    min_alloc_free_bytes: Option<u64>,
    /// audit-log retention (days). `None` = default 90; 0 = off.
    audit_retention_days: Option<u64>,
    /// path to the Ed25519 signing-key file (KDC private material).
    /// `None` = data-plane authz DISABLED (opt-in). Format: one key per line,
    /// `<kid> <hex-32-byte-seed> [disabled]`. Generate via
    /// `autumn-op gen-signing-key`.
    auth_signing_key_file: Option<String>,
    /// admin token gating `tenant-create` / `tenant-delete`
    /// (admin_auth_design.md Option A). `None` = those admin RPCs are refused.
    admin_token: Option<String>,
    /// protected (default-DENY) key prefixes, repeatable. `mem/` is
    /// the default when authz is enabled and none is given.
    auth_protected_prefixes: Vec<String>,
    /// minted-token TTL in seconds. `None` = library default 3600.
    auth_token_ttl_secs: Option<u64>,
    /// clock-skew leeway in seconds. `None` = library default 60.
    auth_clock_skew_secs: Option<u64>,
    /// embedded web dashboard HTTP port. `None` = disabled
    /// (no listener). Deploy layer defaults this on (cluster.sh / entrypoint /
    /// autumn-deploy translate AUTUMN_DASHBOARD=1 → --dashboard-port 8799).
    dashboard_port: Option<u16>,
    /// Bind host for the dashboard. `None` = follow `--listen` (reachable
    /// cluster-wide by default, per the on-by-default rollout decision). Pin to
    /// 127.0.0.1 to keep the unauthenticated surface loopback-only.
    dashboard_listen: Option<String>,
    /// ARM cluster mutations — manual dashboard actions AND the
    /// auto-policy controller leaving DryRun. Default OFF = read-only viewer.
    dashboard_allow_mutations: bool,
    /// seed this preset as the active policy (Armed)
    /// on a FRESH cluster. Deploy layer passes `balanced`; unset = controller
    /// stays Off (cluster.sh / tests). Armed is honored only with
    /// `--dashboard-allow-mutations`.
    auto_policy_default: Option<String>,
}

fn parse_args() -> Args {
    let mut port: u16 = 9001;
    let mut etcd: Vec<String> = Vec::new();
    let mut bind_host = String::from("0.0.0.0");
    let mut transport = TransportKind::Tcp;
    let mut policy_fast_mode = false;
    let mut report_disk_failure_window_secs: Option<u64> = None;
    let mut report_disk_failure_quorum: Option<usize> = None;
    let mut metrics_port: Option<u16> = None;
    let mut metrics_listen: Option<String> = None;
    let mut min_alloc_free_bytes: Option<u64> = None;
    let mut audit_retention_days: Option<u64> = None;
    let mut auth_signing_key_file: Option<String> = None;
    let mut admin_token: Option<String> = None;
    let mut auth_protected_prefixes: Vec<String> = Vec::new();
    let mut auth_token_ttl_secs: Option<u64> = None;
    let mut auth_clock_skew_secs: Option<u64> = None;
    let mut dashboard_port: Option<u16> = None;
    let mut dashboard_listen: Option<String> = None;
    let mut dashboard_allow_mutations = false;
    let mut auto_policy_default: Option<String> = None;

    let raw: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < raw.len() {
        match raw[i].as_str() {
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
            "--policy-fast-mode" => policy_fast_mode = true,
            "--report-disk-failure-window-secs" => {
                i += 1;
                report_disk_failure_window_secs = Some(
                    raw[i]
                        .parse()
                        .expect("--report-disk-failure-window-secs must be a number"),
                );
            }
            "--report-disk-failure-quorum" => {
                i += 1;
                report_disk_failure_quorum = Some(
                    raw[i]
                        .parse()
                        .expect("--report-disk-failure-quorum must be a number"),
                );
            }
            "--metrics-port" => {
                i += 1;
                metrics_port = Some(raw[i].parse().expect("--metrics-port must be a port"));
            }
            "--metrics-listen" => {
                i += 1;
                metrics_listen = Some(raw[i].clone());
            }
            "--min-alloc-free-bytes" => {
                i += 1;
                min_alloc_free_bytes =
                    Some(raw[i].parse().expect("--min-alloc-free-bytes must be a number"));
            }
            "--audit-retention-days" => {
                i += 1;
                audit_retention_days =
                    Some(raw[i].parse().expect("--audit-retention-days must be a number"));
            }
            // ── manager-as-KDC (data-plane authz) ────────────
            "--auth-signing-key-file" => {
                i += 1;
                auth_signing_key_file = Some(raw[i].clone());
            }
            "--admin-token" => {
                i += 1;
                admin_token = Some(raw[i].clone());
            }
            // Read the admin token from a FILE — preferred over --admin-token,
            // which leaks the secret via ps / /proc/<pid>/cmdline on a
            // long-lived daemon. Trailing newline trimmed. cluster.sh passes
            // this form (AUTUMN_ADMIN_TOKEN_FILE).
            "--admin-token-file" => {
                i += 1;
                let path = raw[i].clone();
                let text = std::fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("read --admin-token-file {path}: {e}"));
                let tok = text.trim_end_matches(['\n', '\r']).to_string();
                if tok.is_empty() {
                    panic!("--admin-token-file {path} is empty");
                }
                admin_token = Some(tok);
            }
            "--auth-protected-prefix" => {
                i += 1;
                auth_protected_prefixes.push(raw[i].clone());
            }
            "--auth-token-ttl-secs" => {
                i += 1;
                auth_token_ttl_secs =
                    Some(raw[i].parse().expect("--auth-token-ttl-secs must be a number"));
            }
            "--auth-clock-skew-secs" => {
                i += 1;
                auth_clock_skew_secs =
                    Some(raw[i].parse().expect("--auth-clock-skew-secs must be a number"));
            }
            // ── embedded web dashboard ───────────────────
            "--dashboard-port" => {
                i += 1;
                dashboard_port = Some(raw[i].parse().expect("--dashboard-port must be a port"));
            }
            "--dashboard-listen" => {
                i += 1;
                dashboard_listen = Some(raw[i].clone());
            }
            "--dashboard-allow-mutations" => {
                dashboard_allow_mutations = true;
            }
            "--auto-policy-default" => {
                i += 1;
                auto_policy_default = Some(raw[i].clone());
            }
            other => eprintln!("unknown arg: {other}"),
        }
        i += 1;
    }

    Args {
        port,
        etcd,
        bind_host,
        transport,
        policy_fast_mode,
        report_disk_failure_window_secs,
        report_disk_failure_quorum,
        metrics_port,
        metrics_listen,
        min_alloc_free_bytes,
        audit_retention_days,
        auth_signing_key_file,
        admin_token,
        auth_protected_prefixes,
        auth_token_ttl_secs,
        auth_clock_skew_secs,
        dashboard_port,
        dashboard_listen,
        dashboard_allow_mutations,
        auto_policy_default,
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
    let _ = autumn_transport::init_with(args.transport);
    let addr = autumn_transport::format_listen_addr(&args.bind_host, args.port)
        .context("parse listen address")?;
    autumn_transport::check_listen_addr(addr, autumn_transport::current().kind()).ok();

    let manager = if args.etcd.is_empty() {
        tracing::warn!(
            "no --etcd endpoints given; running in-memory only (metadata will be lost on restart)"
        );
        AutumnManager::new()
    } else {
        tracing::info!("connecting to etcd: {:?}", args.etcd);
        AutumnManager::new_with_etcd(args.etcd)
            .await
            .context("connect to etcd")?
    };

    // in-kernel auto-dispatch deleted. The manager's policy_tick_loop
    // produces an advisory_cache via `MSG_GET_POLICY_CANDIDATES`; external
    // operators / controllers act on it.
    // quorum debounce config — applied if either flag was
    // set. The library defaults (60 s / 3) match the earlier env defaults.
    if args.report_disk_failure_window_secs.is_some() || args.report_disk_failure_quorum.is_some() {
        let window =
            std::time::Duration::from_secs(args.report_disk_failure_window_secs.unwrap_or(60));
        let quorum = args.report_disk_failure_quorum.unwrap_or(3);
        manager.set_report_disk_failure_config(window, quorum);
        tracing::info!(
            window_secs = window.as_secs(),
            quorum,
            "quorum debounce configured"
        );
    }

    if let Some(v) = args.min_alloc_free_bytes {
        manager.set_min_alloc_free_bytes(v);
        tracing::info!(min_alloc_free_bytes = v, "ENOSPC-1 allocation floor configured");
    }
    if let Some(v) = args.audit_retention_days {
        manager.set_audit_retention_days(v);
        tracing::info!(audit_retention_days = v, "audit retention configured");
    }

    // The ADMIN TOKEN gates CONTROL-plane admin RPCs (namespace-create,
    // principal-create, the merge/set-presplit ops, …) and is
    // INDEPENDENT of data-plane authz. Apply it UNCONDITIONALLY: it used to live
    // only inside the signing-key branch below, so a manager launched with
    // `--admin-token-file` but no `--auth-signing-key-file` silently discarded
    // the token and answered every admin RPC with "admin RPCs disabled". That is
    // exactly the shape a default (authz-off) cluster needs in order to register
    // the bench namespace (BUG-BENCH-NS-UNREGISTERED). Configuring the token can
    // only ENABLE RPCs that were refused outright before.
    if let Some(tok) = &args.admin_token {
        manager.set_admin_token(tok.clone());
    }

    // data-plane authz (opt-in). Loading a signing-key file ENABLES
    // it; without the flag the manager is not a KDC and PSes don't enforce.
    if let Some(path) = &args.auth_signing_key_file {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read --auth-signing-key-file {path}"))?;
        let keyring = autumn_manager::authz::AuthzKeyring::from_file_contents(&text)
            .map_err(|e| anyhow::anyhow!("parse --auth-signing-key-file {path}: {e}"))?;
        manager.set_authz_keyring(keyring);
        // Protected prefixes: use the given ones, else default to `mem/`.
        let prefixes: Vec<Vec<u8>> = if args.auth_protected_prefixes.is_empty() {
            vec![b"mem/".to_vec()]
        } else {
            args.auth_protected_prefixes
                .iter()
                .map(|p| p.as_bytes().to_vec())
                .collect()
        };
        manager.set_protected_prefixes(prefixes.clone());
        if let Some(v) = args.auth_token_ttl_secs {
            manager.set_token_ttl_secs(v);
        }
        if let Some(v) = args.auth_clock_skew_secs {
            manager.set_clock_skew_secs(v);
        }
        // (admin token is applied unconditionally above — it is not part of
        // data-plane authz.)
        tracing::info!(
            protected_prefixes = ?prefixes
                .iter()
                .map(|p| String::from_utf8_lossy(p).into_owned())
                .collect::<Vec<_>>(),
            admin_token_set = args.admin_token.is_some(),
            "data-plane authz ENABLED (manager is a KDC)"
        );
    } else if !args.auth_protected_prefixes.is_empty() {
        // --admin-token WITHOUT a signing key is now valid (control-plane admin,
        // applied above). Only protected-prefixes is meaningless without one —
        // it configures data-plane enforcement that isn't running.
        tracing::warn!(
            "--auth-protected-prefix given without --auth-signing-key-file; \
             data-plane authz stays DISABLED (no signing key)"
        );
    }

    if args.policy_fast_mode {
        let cfg = autumn_manager::policy::PolicyConfig {
            required_buckets: 1,
            tick_interval_sec: 5,
            bucket_sec: 5,
            gc_debt_high: 1024 * 1024,
            compact_pending_high: 4 * 1024 * 1024,
            gc_cooldown_sec: 30,
            compact_cooldown_sec: 30,
            split_cooldown_sec: 30,
            merge_cooldown_sec: 30,
            ..Default::default()
        };
        manager.set_policy_config(cfg);
        tracing::warn!(
            "--policy-fast-mode enabled; thresholds={{gc_debt=1MiB, compact=4MiB, bucket=5s, tick=5s, required=1, cooldown=30s}}. NOT FOR PRODUCTION."
        );
    }

    // Observability batch 1: /metrics endpoint. The store is Rc/!Send, so
    // a 2 s publisher task on THIS runtime renders the snapshot string;
    // the HTTP listener (own OS thread, std::net) serves the latest copy.
    if let Some(mport) = args.metrics_port {
        let snap = autumn_common::metrics_http::MetricsSnapshot::new();
        // Initial snapshot BEFORE the listener — a scrape that races
        // startup gets real data, never an empty 200 (coco P3).
        snap.publish(manager.metrics_text());
        let snap_http = snap.clone();
        let mhost = args.metrics_listen.as_deref().unwrap_or(&args.bind_host);
        match autumn_common::metrics_http::spawn_metrics_http(
            mhost,
            mport,
            std::sync::Arc::new(move || snap_http.get().as_ref().clone()),
        ) {
            Ok(()) => {
                let mgr = manager.clone();
                compio::runtime::spawn(async move {
                    loop {
                        compio::time::sleep(std::time::Duration::from_secs(2)).await;
                        // Render OUTSIDE any lock; publish is an O(1)
                        // Arc swap — never blocks this runtime behind a
                        // scraper (coco P2).
                        snap.publish(mgr.metrics_text());
                    }
                })
                .detach();
                tracing::info!(port = mport, host = mhost, "metrics endpoint up at /metrics");
            }
            // Metrics are auxiliary — a taken port must not kill the
            // control plane. Loud log, keep serving.
            Err(e) => tracing::error!(port = mport, "metrics endpoint bind failed: {e}"),
        }
    }

    // embedded web dashboard + (M2+) auto-policy controller.
    // Spawns its own compio TcpListener task; must be started BEFORE the
    // blocking serve() below. Default bind follows --listen (on-by-default
    // rollout); mutations are OFF unless --dashboard-allow-mutations.
    // M2: one flag gates BOTH the dashboard's manual actions AND
    // the auto-policy controller leaving DryRun. Set unconditionally — the
    // controller loop runs even without --dashboard-port.
    manager.set_dashboard_allow_mutations(args.dashboard_allow_mutations);
    // seed the deploy-configured default active policy
    // on a fresh cluster. Validate the preset name up front — a typo must fail
    // loud at startup, not silently leave the controller Off.
    if let Some(preset) = &args.auto_policy_default {
        if !AutumnManager::is_known_auto_policy_preset(preset) {
            anyhow::bail!(
                "--auto-policy-default {preset:?} is not a known preset \
                 (gc-only / maintenance / space-reclaim / balanced / aggressive)"
            );
        }
        manager.set_auto_policy_default(preset.clone());
        if !args.dashboard_allow_mutations {
            tracing::warn!(
                preset,
                "--auto-policy-default set WITHOUT --dashboard-allow-mutations: the seeded \
                 policy will run in DryRun (advisory only) until mutations are armed"
            );
        }
    }
    // seed now — AFTER the flag is set. `new_with_etcd`
    // already ran the first replay + election in the constructor (which recorded
    // whether a config was persisted), so this seeds a fresh cluster and no-ops
    // when a config already exists or no default was requested.
    manager.apply_auto_policy_default();
    if let Some(dport) = args.dashboard_port {
        let dhost = args
            .dashboard_listen
            .clone()
            .unwrap_or_else(|| args.bind_host.clone());
        manager.start_dashboard(dhost, dport, args.dashboard_allow_mutations);
    }

    tracing::info!("autumn-manager-server listening on {addr}");
    manager.serve(addr).await?;

    Ok(())
}
