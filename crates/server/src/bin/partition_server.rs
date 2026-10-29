use anyhow::{Context, Result};
#[cfg(unix)]
extern crate libc;
use autumn_params::ps::PprofParams;
use autumn_params::{Positional, Section};
use autumn_partition_server::PartitionServer;

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

const USAGE: &str = "usage: autumn-ps --psid <ID> --cluster-secret-file <PATH> [options]";

/// Every flag lands in the process-wide `PsParams` or, in a `profiling`
/// build, `PprofParams`.
fn parse_args() -> PprofParams {
    let ps: &dyn Section = autumn_partition_server::params();
    let pprof = PprofParams::default();
    let sections: &[&dyn Section] = if cfg!(feature = "profiling") { &[ps, &pprof] } else { &[ps] };
    autumn_params::parse_or_exit(USAGE, sections, Positional::Refuse);
    pprof
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

    let prof = parse_args();

    let params = autumn_partition_server::params();
    // Apply the regpool cap BEFORE any compio runtime / TLS pool init, so
    // the first thread's `POOL` reads the operator-supplied value.
    if !autumn_transport::set_regpool_cap_bytes(params.ucx_regpool_cap_bytes.get()) {
        tracing::warn!("regpool cap already set (ignored — first-call-wins)");
    }

    start_pprof(&prof);

    autumn_stream::set_read_hedge_ms(params.read_hedge_ms.get());

    if let Err(e) = autumn_rpc::peer_auth::install_for_server(params.cluster_secret_file.get().as_deref())
        .and_then(|()| autumn_rpc::peer_auth::designate_managers(&params.manager.get()))
    {
        eprintln!("error: {e}");
        std::process::exit(2);
    }
    let _ = autumn_transport::init_with(params.transport.get().into());
    if autumn_transport::current().kind() == autumn_transport::TransportKind::Ucx {
        if let Some(w) = autumn_partition_server::regpool_admission_mismatch(
            params.ucx_regpool_cap_bytes.get(),
            params.conn_inflight_cap.get(),
        ) {
            tracing::warn!("{w}");
        }
    }
    autumn_rpc::client::set_prepared_zerocopy_min_bytes(params.tcp_zerocopy_min_bytes.get());
    // --cpuset (if given) is installed BEFORE any cpu_pin reader
    // fires, so the cached core list reflects the override.
    if let Some(cs) = params.cpuset.get().map(|c| c.0) {
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
    let (psid, port, listen, manager) =
        (params.psid.get(), params.port.get(), params.listen.get(), params.manager.get());
    let addr = autumn_transport::format_listen_addr(&listen, port)
        .context("parse listen address")?;
    autumn_transport::check_listen_addr(addr, autumn_transport::current().kind()).ok();

    let advertise = params.advertise.get().unwrap_or_else(|| {
        autumn_transport::format_listen_addr(&listen, port)
            .map(|sa| sa.to_string())
            .unwrap_or_else(|_| format!("{listen}:{port}"))
    });

    tracing::info!(
        "autumn-ps starting: psid={}, first_part_port={}, manager={}, advertise={}",
        psid,
        addr,
        manager,
        advertise,
    );
    tracing::info!(
        "per-partition listener — partition N binds port={}+N-1",
        port,
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
        psid,
        &manager,
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
                        pool_bytes = s.pool_bytes,
                        registered_bytes = s.registered_bytes,
                        in_use_bytes = s.in_use_bytes,
                        "regpool"
                    );
                }
            })
            .detach();
            tracing::info!(period_secs = secs, "regpool periodic log armed");
        }
    }

    // /metrics on its own thread and runtime, reading the PS's atomics.
    if let Some(mport) = autumn_common::metrics_http::metrics_port(params.metrics_port.get(), port) {
        let metrics_listen = params.metrics_listen.get();
        let mhost = metrics_listen.as_deref().unwrap_or(&listen);
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

/// Writes a flamegraph `--pprof-secs` after start (`profiling` builds only).
#[cfg(feature = "profiling")]
fn start_pprof(prof: &PprofParams) {
    let secs = prof.pprof_secs.get();
    if secs > 0 {
        let out_path = prof.pprof_out.get();
        let thread_filter = prof.pprof_threads.get();
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

#[cfg(not(feature = "profiling"))]
fn start_pprof(_: &PprofParams) {}
