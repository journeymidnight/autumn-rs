use anyhow::{Context, Result};
use autumn_manager::AutumnManager;
use autumn_transport::TransportKind;

// F193 allocator hygiene — see crates/server/src/bin/extent_node.rs for
// the rationale and the MALLOC_CONF tuning explanation. Manager's peak
// RSS during etcd replay can also benefit, though the dominant case is
// the extent-node EC path.
#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// `_rjem_malloc_conf` (NOT `malloc_conf`): tikv-jemallocator 0.6 is `_rjem_`-
// prefixed, so the old unprefixed symbol was a silent no-op. `oversize_threshold:0`
// keeps large allocations in normal arenas (warm reuse) — see
// crates/server/src/bin/extent_node.rs for the full F193/F216-E rationale.
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
    /// F187: enable fast-mode policy thresholds for load testing —
    /// 1-bucket / 5 s tick / 1 MiB GC debt / 4 MiB compact pending /
    /// 30 s cooldowns. Production should never use this; the default
    /// is `false` (production thresholds = 1 GiB / 4 GiB / 5-bucket /
    /// 60 s tick / 5-min cooldown).
    policy_fast_mode: bool,
    /// F195 (was F192 env): MSG_REPORT_DISK_FAILURE sliding window
    /// length in seconds. `None` = library default (60 s).
    report_disk_failure_window_secs: Option<u64>,
    /// F195 (was F192 env): MSG_REPORT_DISK_FAILURE distinct-reporter
    /// quorum threshold. `None` = library default (3).
    report_disk_failure_quorum: Option<usize>,
}

fn parse_args() -> Args {
    let mut port: u16 = 9001;
    let mut etcd: Vec<String> = Vec::new();
    let mut bind_host = String::from("0.0.0.0");
    let mut transport = TransportKind::Tcp;
    let mut policy_fast_mode = false;
    let mut report_disk_failure_window_secs: Option<u64> = None;
    let mut report_disk_failure_quorum: Option<usize> = None;

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
            // F203: --auto-split / --auto-merge removed. Mechanism /
            // policy separation puts dispatch decisions in an external
            // controller. Read `client policy` + call `client split` /
            // `client merge` to act.
            "--auto-split" | "--auto-merge" => {
                eprintln!(
                    "{}: removed in F203. Use `client policy` + `client {}` to drive policy externally.",
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

    // F203: in-kernel auto-dispatch deleted. The manager's policy_tick_loop
    // produces an advisory_cache via `MSG_GET_POLICY_CANDIDATES`; external
    // operators / controllers act on it.
    // F195: F192 quorum debounce config — applied if either flag was
    // set. The library defaults (60 s / 3) match pre-F195 env defaults.
    if args.report_disk_failure_window_secs.is_some() || args.report_disk_failure_quorum.is_some() {
        let window =
            std::time::Duration::from_secs(args.report_disk_failure_window_secs.unwrap_or(60));
        let quorum = args.report_disk_failure_quorum.unwrap_or(3);
        manager.set_report_disk_failure_config(window, quorum);
        tracing::info!(
            window_secs = window.as_secs(),
            quorum,
            "F192 quorum debounce configured"
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
            "F187: --policy-fast-mode enabled; thresholds={{gc_debt=1MiB, compact=4MiB, bucket=5s, tick=5s, required=1, cooldown=30s}}. NOT FOR PRODUCTION."
        );
    }

    tracing::info!("autumn-manager-server listening on {addr}");
    manager.serve(addr).await?;

    Ok(())
}
