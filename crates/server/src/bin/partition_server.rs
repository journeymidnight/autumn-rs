use anyhow::{Context, Result};
#[cfg(unix)]
extern crate libc;
use autumn_params::ps::PsParams;
use autumn_partition_server::PartitionServer;
use autumn_transport::TransportKind;

// allocator hygiene + read-perf fix — see
// crates/server/src/bin/extent_node.rs for the full rationale.
#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// The config symbol jemalloc actually reads is `_rjem_malloc_conf`:
// tikv-jemallocator 0.6 builds jemalloc with the `_rjem_` prefix, so the old
// `#[export_name="malloc_conf"]` was a SILENT NO-OP (jemalloc never saw it; its
// `dirty_decay_ms:1000` never applied). `oversize_threshold:0` is the load-
// bearing setting: jemalloc 5.x's default oversize_threshold is 8 MiB, so 8 MiB
// VP value reads landed in the dedicated oversize arena that PURGES pages on
// free — every read then page-faulted cold pages (~2× slower; a TCP 8 MiB read
// regression). Threshold 0 routes them through normal arenas
// with warm dirty-page reuse (~3.1 GB/s vs ~1.6), while jemalloc's default
// decay still returns idle pages (the desired RSS hygiene). RUNTIME-
// CONFIGURABLE: `_RJEM_MALLOC_CONF` (set by cluster.sh / the prod launcher,
// like UCX_TLS) is read after this symbol and overrides/extends it per deploy.
#[cfg(target_os = "linux")]
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static malloc_conf: &[u8] = b"oversize_threshold:0\0";

struct Args {
    /// `--cluster-secret-file`: required. Peer and Admin connections prove it.
    cluster_secret_file: Option<std::path::PathBuf>,
    port: u16,
    psid: u64,
    manager: String,
    advertise: Option<String>,
    bind_host: String,
    transport: TransportKind,
    /// the cores the whole PS runs on, taskset syntax (e.g. `4-11`,
    /// `0,2,4`, `0-3,8-11`). None = the mask it was started with.
    cpuset: Option<Vec<usize>>,
    /// Prometheus `/metrics` port. `None` = `--port` + 500; 0 = no endpoint.
    metrics_port: Option<u16>,
    /// Bind host for /metrics only. `None` = follow `--listen`. The
    /// endpoint is unauthenticated — operators exposing the RPC plane
    /// on 0.0.0.0 can pin metrics to 127.0.0.1 with this.
    metrics_listen: Option<String>,
    #[cfg(feature = "profiling")]
    pprof_secs: Option<u64>,
    #[cfg(feature = "profiling")]
    pprof_out: Option<String>,
    #[cfg(feature = "profiling")]
    pprof_threads: Option<String>,
}

fn usage() -> String {
    format!(
        "usage: autumn-ps --psid <ID> --cluster-secret-file <PATH> [options]\n\
         \n\
         \x20 --psid <id>                 partition server id (required, non-zero)\n\
         \x20 --port <n>                  first partition's port; partition N binds port+N-1 (default 9201)\n\
         \x20 --manager <a,b,...>         manager endpoints (default 127.0.0.1:9001)\n\
         \x20 --listen <host>             bind host (default 0.0.0.0)\n\
         \x20 --advertise <host:port>     address announced to the manager\n\
         \x20 --transport tcp|ucx\n\
         \x20 --cpuset <spec>             cores the whole PS runs on, like taskset; len = slot_cap\n\
         \x20 --metrics-port <n>          /metrics port (default port+500, 0 = off)\n\
         \x20 --metrics-listen <host>     /metrics bind host (default --listen)\n\
         \n\
         parameters:\n{}",
        autumn_params::help::<PsParams>()
    )
}

fn parse_args() -> Args {
    let mut cluster_secret_file: Option<std::path::PathBuf> = None;
    let mut port: u16 = 9201;
    let mut psid: u64 = 0;
    let mut manager = String::from("127.0.0.1:9001");
    let mut advertise: Option<String> = None;
    let mut bind_host = String::from("0.0.0.0");
    let mut transport = TransportKind::Tcp;
    let mut cpuset: Option<Vec<usize>> = None;
    let mut metrics_port: Option<u16> = None;
    let mut metrics_listen: Option<String> = None;
    #[cfg(feature = "profiling")]
    let mut pprof_secs: Option<u64> = None;
    #[cfg(feature = "profiling")]
    let mut pprof_out: Option<String> = None;
    #[cfg(feature = "profiling")]
    let mut pprof_threads: Option<String> = None;
    let params = autumn_partition_server::params();

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--help" | "-h" => {
                print!("{}", usage());
                std::process::exit(0);
            }
            "--port" => {
                i += 1;
                port = args[i].parse().expect("--port must be a number");
            }
            "--cluster-secret-file" => {
                i += 1;
                cluster_secret_file = Some(args[i].clone().into());
            }
            "--psid" => {
                i += 1;
                psid = args[i].parse().expect("--psid must be a number");
            }
            "--manager" => {
                i += 1;
                manager = args[i].clone();
            }
            "--advertise" => {
                i += 1;
                advertise = Some(args[i].clone());
            }
            "--listen" => {
                i += 1;
                bind_host = args[i].clone();
            }
            "--transport" => {
                i += 1;
                transport =
                    autumn_transport::parse_transport_flag(&args[i]).unwrap_or_else(|bad| {
                        eprintln!("--transport must be `tcp` or `ucx`, got {bad:?}");
                        std::process::exit(2);
                    });
            }
            "--cpu-start" => {
                eprintln!("error: --cpu-start was removed; pass --cpuset <SPEC> with the cores to use");
                std::process::exit(2);
            }
            "--cpuset" => {
                i += 1;
                cpuset = Some(autumn_common::parse_cpuset(&args[i]).unwrap_or_else(|e| {
                    eprintln!("--cpuset parse error: {e}");
                    std::process::exit(2);
                }));
            }
            "--metrics-port" => {
                i += 1;
                metrics_port = Some(args[i].parse().expect("--metrics-port must be a port"));
            }
            "--metrics-listen" => {
                i += 1;
                metrics_listen = Some(args[i].clone());
            }
            #[cfg(feature = "profiling")]
            "--pprof-secs" => {
                i += 1;
                pprof_secs = Some(args[i].parse().expect("--pprof-secs u64"));
            }
            #[cfg(feature = "profiling")]
            "--pprof-out" => {
                i += 1;
                pprof_out = Some(args[i].clone());
            }
            #[cfg(feature = "profiling")]
            "--pprof-threads" => {
                i += 1;
                pprof_threads = Some(args[i].clone());
            }
            "--bg-rate-bytes-per-sec" => {
                eprintln!(
                    "error: --bg-rate-bytes-per-sec was removed. Use \
                     --admission-compact-rate-bytes-per-sec + \
                     --admission-gc-rate-bytes-per-sec instead."
                );
                std::process::exit(2);
            }
            // Never read by anything; the GC / compaction advisory thresholds
            // and cooldowns are the manager's (`--gc-debt-high-bytes` etc.).
            "--fg-qps-quota" | "--gc-debt-high-bytes" | "--compact-pending-high-bytes"
            | "--gc-cooldown-secs" | "--compact-cooldown-secs" | "--min-pipeline-batch"
            | "--conn-threads" => {
                eprintln!("error: {} was removed: it had no effect on the PS", args[i]);
                std::process::exit(2);
            }
            other => match autumn_params::take_flag(params, other, args.get(i + 1).map(|s| s.as_str())) {
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

    if psid == 0 {
        eprintln!("error: --psid is required and must be non-zero");
        std::process::exit(1);
    }
    if let Err(e) = autumn_params::CrossCheck::cross_check(params) {
        eprintln!("error: {e}");
        std::process::exit(2);
    }

    Args {
        cluster_secret_file,
        port,
        psid,
        manager,
        advertise,
        bind_host,
        transport,
        cpuset,
        metrics_port,
        metrics_listen,
        #[cfg(feature = "profiling")]
        pprof_secs,
        #[cfg(feature = "profiling")]
        pprof_out,
        #[cfg(feature = "profiling")]
        pprof_threads,
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

    // env-dump removed — production rs code no longer reads
    // AUTUMN_* env vars, so dumping them at startup was misleading. The
    // remaining AUTUMN_* in the operator's shell (e.g. for cluster.sh's
    // own use) are no longer the source of truth for binary config.

    let args = parse_args();

    let params = autumn_partition_server::params();
    // Apply the regpool cap BEFORE any compio runtime / TLS pool init, so
    // the first thread's `POOL` reads the operator-supplied value.
    if !autumn_transport::set_regpool_cap_bytes(params.ucx_regpool_cap_bytes.get()) {
        tracing::warn!("regpool cap already set (ignored — first-call-wins)");
    }

    // ---- pprof CLI flags (replaces AUTUMN_PPROF_* env reads) ----
    #[cfg(feature = "profiling")]
    {
        if let Some(secs) = args.pprof_secs {
            if secs > 0 {
                let out_path = args
                    .pprof_out
                    .clone()
                    .unwrap_or_else(|| "/tmp/autumn_ps_pprof.svg".to_string());
                let thread_filter = args.pprof_threads.clone();
                std::thread::spawn(move || {
                    let guard = pprof::ProfilerGuardBuilder::default()
                        .frequency(99)
                        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                        .build()
                        .expect("pprof guard");
                    std::thread::sleep(std::time::Duration::from_secs(secs));
                    let report = guard.report().build().expect("pprof report");
                    let mut file = std::fs::File::create(&out_path).expect("pprof outfile");
                    report.flamegraph(&mut file).expect("flamegraph write");
                    if let Some(prefix) = thread_filter {
                        let txt_path = format!("{}.threads.txt", out_path);
                        if let Ok(mut txt) = std::fs::File::create(&txt_path) {
                            use std::io::Write;
                            for (frames, count) in &report.data {
                                if frames.thread_name.starts_with(&prefix) {
                                    writeln!(txt, "thread={} count={}", frames.thread_name, count)
                                        .ok();
                                }
                            }
                        }
                    }
                    eprintln!("[R2] pprof flamegraph written: {}", out_path);
                });
            }
        }
    }
    // ---- end pprof hook ----

    autumn_stream::set_read_hedge_ms(params.read_hedge_ms.get());

    if let Err(e) = autumn_rpc::peer_auth::install_for_server(args.cluster_secret_file.as_deref())
        .and_then(|()| autumn_rpc::peer_auth::designate_managers(&args.manager))
    {
        eprintln!("error: {e}");
        std::process::exit(2);
    }
    let _ = autumn_transport::init_with(args.transport);
    autumn_rpc::client::set_prepared_zerocopy_min_bytes(params.tcp_zerocopy_min_bytes.get());
    // --cpuset (if given) is installed BEFORE any cpu_pin reader
    // fires, so the cached core list reflects the override.
    if let Some(cs) = args.cpuset.clone() {
        let _ = autumn_common::set_cpuset(cs);
    }
    // Before any thread is spawned, so every thread inherits the cpuset. The
    // PS pins nothing: the cpuset confines it like taskset.
    if let Err(e) = autumn_common::cpu_pin::float_current() {
        eprintln!("error: cannot confine the PS to its cores: {e}");
        std::process::exit(2);
    }

    #[cfg(unix)]
    unsafe {
        let mut rl = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) == 0 && rl.rlim_cur < 65535 {
            rl.rlim_cur = rl.rlim_max.min(65535);
            libc::setrlimit(libc::RLIMIT_NOFILE, &rl);
        }
        // RDMA (UCX rc_mlx5) registers every send/recv buffer with the
        // NIC via ibv_reg_mr, pinning pages against RLIMIT_MEMLOCK. The default
        // soft limit (often 8 MiB) is far too small for large values — a single
        // 8 MiB value send exceeds it and ibv_reg_mr FAULTS inside libibverbs
        // (observed: PS SIGSEGV in ucp_stream_send_nbx -> rcache ->
        // ibv_reg_mr_iova2 under concurrent 8 MiB reads; 4 KiB reads were fine).
        // Raise to INFINITY — standard for RDMA daemons, sibling to the
        // RLIMIT_NOFILE raise above. Harmless on TCP (nothing pins). Falls back
        // to raising the soft limit up to the hard limit if INFINITY is refused.
        let inf = libc::rlimit {
            rlim_cur: libc::RLIM_INFINITY,
            rlim_max: libc::RLIM_INFINITY,
        };
        if libc::setrlimit(libc::RLIMIT_MEMLOCK, &inf) != 0 {
            let mut ml = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            if libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut ml) == 0 && ml.rlim_cur < ml.rlim_max {
                ml.rlim_cur = ml.rlim_max;
                libc::setrlimit(libc::RLIMIT_MEMLOCK, &ml);
            }
        }
    }
    let addr = autumn_transport::format_listen_addr(&args.bind_host, args.port)
        .context("parse listen address")?;
    autumn_transport::check_listen_addr(addr, autumn_transport::current().kind()).ok();

    let advertise = args.advertise.unwrap_or_else(|| {
        autumn_transport::format_listen_addr(&args.bind_host, args.port)
            .map(|sa| sa.to_string())
            .unwrap_or_else(|_| format!("{}:{}", args.bind_host, args.port))
    });

    tracing::info!(
        "autumn-ps starting: psid={}, first_part_port={}, manager={}, advertise={}",
        args.psid,
        addr,
        args.manager,
        advertise,
    );
    tracing::info!(
        "per-partition listener — partition N binds port={}+N-1",
        args.port,
    );

    // fix: use `_and_port` so `base_port` is set BEFORE `finish_connect`'s
    // implicit `sync_regions_once()` runs `open_partition`. On restart, partitions
    // already exist in the manager — without this, `open_partition` reads
    // `base_port = 0` and binds the first partition to port `0 + 1 = 1`.
    let advertise_host = advertise
        .rsplit_once(':')
        .map_or(advertise.as_str(), |(h, _)| h)
        .to_string();
    let ps = PartitionServer::connect_with_advertise_and_port(
        args.psid,
        &args.manager,
        Some(advertise),
        addr,
    )
    .await
    .context("connect partition server")?;

    tracing::info!("autumn-ps ready (per-partition listeners; first partition on {addr})");

    // Periodic regpool snapshot. Single per-process task on the PS main
    // compio runtime — `regpool_snapshot()` aggregates atomic counters
    // across every thread's TLS pool, so one logger is sufficient.
    // Detach (not supervised) because this is observability-only; a panic
    // here loses the counters, not data — and compio's runtime already
    // catches the panic so the main thread stays alive.
    {
        let secs = params.regpool_log_interval_secs.get();
        if secs > 0 {
            let period = std::time::Duration::from_secs(secs);
            compio::runtime::spawn(async move {
                let mut ticker = compio::time::interval(period);
                ticker.tick().await; // first tick immediate; skip
                loop {
                    ticker.tick().await;
                    let s = autumn_transport::regpool_snapshot();
                    tracing::info!(
                        acquire = s.acquire_total,
                        hit = s.hit_total,
                        hit_rate_pct = (s.hit_rate() * 100.0) as u32,
                        out_of_pool = s.out_of_pool_total,
                        over_cap = s.over_cap_total,
                        register_failed = s.register_failed_total,
                        registered_bytes = s.registered_bytes,
                        "regpool"
                    );
                }
            })
            .detach();
            tracing::info!(period_secs = secs, "regpool periodic log armed");
        }
    }

    // /metrics on its own thread and runtime, reading the PS's atomics.
    if let Some(mport) = autumn_common::metrics_http::metrics_port(args.metrics_port, args.port) {
        let mhost = args.metrics_listen.as_deref().unwrap_or(&args.bind_host);
        let m = ps.metrics();
        match autumn_common::metrics_http::spawn_metrics_http(mhost, mport, std::sync::Arc::new(move || m.render())) {
            Ok(()) => {
                let reach = autumn_common::metrics_http::metrics_reach_addr(mhost, &advertise_host, mport);
                ps.set_metrics_addr(reach.clone());
                tracing::info!(port = mport, host = mhost, reach, "metrics endpoint up at /metrics");
            }
            // Auxiliary — a taken metrics port must not kill the data plane.
            Err(e) => tracing::error!(port = mport, "metrics endpoint bind failed: {e}"),
        }
    }

    // install a SIGTERM/SIGINT handler. The handler sets an
    // atomic flag (only async-signal-safe ops allowed); a sidecar future
    // polls the flag every 100 ms and resolves once tripped, asking
    // `serve_until_shutdown` to drain partitions and exit gracefully.
    #[cfg(unix)]
    install_term_handler();

    let shutdown_fut = async {
        #[cfg(unix)]
        {
            use std::sync::atomic::Ordering;
            use std::time::Duration;
            loop {
                if SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
                    return;
                }
                compio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        #[cfg(not(unix))]
        {
            std::future::pending::<()>().await
        }
    };

    ps.serve_until_shutdown(addr, shutdown_fut).await?;
    tracing::info!("autumn-ps exited cleanly");
    Ok(())
}

#[cfg(unix)]
static SHUTDOWN_REQUESTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(unix)]
extern "C" fn handle_term_signal(_sig: libc::c_int) {
    SHUTDOWN_REQUESTED.store(true, std::sync::atomic::Ordering::Release);
}

#[cfg(unix)]
fn install_term_handler() {
    unsafe {
        libc::signal(
            libc::SIGTERM,
            handle_term_signal as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGINT,
            handle_term_signal as *const () as libc::sighandler_t,
        );
    }
}

