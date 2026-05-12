use anyhow::{Context, Result};
#[cfg(unix)]
extern crate libc;
use autumn_partition_server::PartitionServer;
use autumn_transport::TransportKind;

// F193 allocator hygiene — see crates/server/src/bin/extent_node.rs for
// the rationale and the MALLOC_CONF tuning explanation.
#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(target_os = "linux")]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"dirty_decay_ms:1000,muzzy_decay_ms:1000\0";

struct Args {
    port: u16,
    psid: u64,
    manager: String,
    advertise: Option<String>,
    bind_host: String,
    transport: TransportKind,
    cpu_start: usize,
    // F195: PS tunables previously env::var-gated, now CLI flags.
    // `None` = library default. Defaults match pre-F195 env defaults.
    group_commit_cap: Option<usize>,
    ps_inflight_cap: Option<usize>,
    ps_bulk_inflight_cap: Option<usize>,
    max_imm_depth: Option<usize>,
    max_wal_gap: Option<u64>,
    shutdown_timeout_ms: Option<u64>,
    major_compact_parallelism: Option<usize>,
    conn_inflight_cap: Option<usize>,
    fg_rate_bytes_per_sec: Option<u64>,
    bg_rate_bytes_per_sec: Option<u64>,
    fg_saturated_threshold: Option<f64>,
    fg_qps_quota: Option<u32>,
    gc_debt_high_bytes: Option<u64>,
    compact_pending_high_bytes: Option<u64>,
    gc_cooldown_secs: Option<i64>,
    compact_cooldown_secs: Option<i64>,
    min_pipeline_batch: Option<usize>,
    gc_read_chunk_bytes: Option<u32>,
    gc_batch_records: Option<usize>,
    gc_batch_bytes: Option<usize>,
    gc_rate_bytes_per_sec: Option<u64>,
    // F195: pprof CLI flags (replaces AUTUMN_PPROF_* env reads).
    #[cfg(feature = "profiling")]
    pprof_secs: Option<u64>,
    #[cfg(feature = "profiling")]
    pprof_out: Option<String>,
    #[cfg(feature = "profiling")]
    pprof_threads: Option<String>,
}

fn parse_args() -> Args {
    let mut port: u16 = 9201;
    let mut psid: u64 = 0;
    let mut manager = String::from("127.0.0.1:9001");
    let mut advertise: Option<String> = None;
    let mut bind_host = String::from("0.0.0.0");
    let mut transport = TransportKind::Tcp;
    let mut cpu_start: usize = 0;
    // F195 tunables — None = library default.
    let mut group_commit_cap: Option<usize> = None;
    let mut ps_inflight_cap: Option<usize> = None;
    let mut ps_bulk_inflight_cap: Option<usize> = None;
    let mut max_imm_depth: Option<usize> = None;
    let mut max_wal_gap: Option<u64> = None;
    let mut shutdown_timeout_ms: Option<u64> = None;
    let mut major_compact_parallelism: Option<usize> = None;
    let mut conn_inflight_cap: Option<usize> = None;
    let mut fg_rate_bytes_per_sec: Option<u64> = None;
    let mut bg_rate_bytes_per_sec: Option<u64> = None;
    let mut fg_saturated_threshold: Option<f64> = None;
    let mut fg_qps_quota: Option<u32> = None;
    let mut gc_debt_high_bytes: Option<u64> = None;
    let mut compact_pending_high_bytes: Option<u64> = None;
    let mut gc_cooldown_secs: Option<i64> = None;
    let mut compact_cooldown_secs: Option<i64> = None;
    let mut min_pipeline_batch: Option<usize> = None;
    let mut gc_read_chunk_bytes: Option<u32> = None;
    let mut gc_batch_records: Option<usize> = None;
    let mut gc_batch_bytes: Option<usize> = None;
    let mut gc_rate_bytes_per_sec: Option<u64> = None;
    #[cfg(feature = "profiling")]
    let mut pprof_secs: Option<u64> = None;
    #[cfg(feature = "profiling")]
    let mut pprof_out: Option<String> = None;
    #[cfg(feature = "profiling")]
    let mut pprof_threads: Option<String> = None;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                i += 1;
                port = args[i].parse().expect("--port must be a number");
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
                transport = autumn_transport::parse_transport_flag(&args[i])
                    .unwrap_or_else(|bad| {
                        eprintln!("--transport must be `tcp` or `ucx`, got {bad:?}");
                        std::process::exit(2);
                    });
            }
            "--cpu-start" => {
                i += 1;
                cpu_start = args[i].parse().expect("--cpu-start must be a number");
            }
            // F099-J: `--conn-threads` is a no-op. Pre-F099-J it sized the
            // compio Dispatcher worker pool that ran ps-conn tasks; after
            // F099-J every ps-conn task runs on the owning partition's
            // P-log runtime and there is no worker pool. The flag is
            // accepted and ignored to preserve CLI compatibility with
            // existing deployment scripts.
            "--conn-threads" => {
                i += 1;
                let _ = args[i].clone();
                tracing::warn!(
                    "--conn-threads is a no-op post F099-J; worker pool removed"
                );
            }
            // F195 PS tunables. Each flag mirrors the pre-F195 env var
            // of the same suffix (lowercased + kebab-cased).
            "--group-commit-cap" => {
                i += 1;
                group_commit_cap = Some(args[i].parse().expect("--group-commit-cap u64"));
            }
            "--ps-inflight-cap" => {
                i += 1;
                ps_inflight_cap = Some(args[i].parse().expect("--ps-inflight-cap usize"));
            }
            "--ps-bulk-inflight-cap" => {
                i += 1;
                ps_bulk_inflight_cap = Some(args[i].parse().expect("--ps-bulk-inflight-cap usize"));
            }
            "--max-imm-depth" => {
                i += 1;
                max_imm_depth = Some(args[i].parse().expect("--max-imm-depth usize"));
            }
            "--max-wal-gap" => {
                i += 1;
                max_wal_gap = Some(args[i].parse().expect("--max-wal-gap u64 bytes"));
            }
            "--shutdown-timeout-ms" => {
                i += 1;
                shutdown_timeout_ms = Some(args[i].parse().expect("--shutdown-timeout-ms u64"));
            }
            "--major-compact-parallelism" => {
                i += 1;
                major_compact_parallelism = Some(
                    args[i].parse().expect("--major-compact-parallelism usize"),
                );
            }
            "--conn-inflight-cap" => {
                i += 1;
                conn_inflight_cap = Some(args[i].parse().expect("--conn-inflight-cap usize"));
            }
            "--fg-rate-bytes-per-sec" => {
                i += 1;
                fg_rate_bytes_per_sec = Some(args[i].parse().expect("--fg-rate-bytes-per-sec u64"));
            }
            "--bg-rate-bytes-per-sec" => {
                i += 1;
                bg_rate_bytes_per_sec = Some(args[i].parse().expect("--bg-rate-bytes-per-sec u64"));
            }
            "--fg-saturated-threshold" => {
                i += 1;
                fg_saturated_threshold = Some(args[i].parse().expect("--fg-saturated-threshold f64"));
            }
            "--fg-qps-quota" => {
                i += 1;
                fg_qps_quota = Some(args[i].parse().expect("--fg-qps-quota u32"));
            }
            "--gc-debt-high-bytes" => {
                i += 1;
                gc_debt_high_bytes = Some(args[i].parse().expect("--gc-debt-high-bytes u64"));
            }
            "--compact-pending-high-bytes" => {
                i += 1;
                compact_pending_high_bytes = Some(
                    args[i].parse().expect("--compact-pending-high-bytes u64"),
                );
            }
            "--gc-cooldown-secs" => {
                i += 1;
                gc_cooldown_secs = Some(args[i].parse().expect("--gc-cooldown-secs i64"));
            }
            "--compact-cooldown-secs" => {
                i += 1;
                compact_cooldown_secs = Some(args[i].parse().expect("--compact-cooldown-secs i64"));
            }
            "--min-pipeline-batch" => {
                i += 1;
                min_pipeline_batch = Some(args[i].parse().expect("--min-pipeline-batch usize"));
            }
            "--gc-read-chunk-bytes" => {
                i += 1;
                gc_read_chunk_bytes = Some(args[i].parse().expect("--gc-read-chunk-bytes u32"));
            }
            "--gc-batch-records" => {
                i += 1;
                gc_batch_records = Some(args[i].parse().expect("--gc-batch-records usize"));
            }
            "--gc-batch-bytes" => {
                i += 1;
                gc_batch_bytes = Some(args[i].parse().expect("--gc-batch-bytes usize"));
            }
            "--gc-rate-bytes-per-sec" => {
                i += 1;
                gc_rate_bytes_per_sec = Some(args[i].parse().expect("--gc-rate-bytes-per-sec u64"));
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
            "--help" | "-h" => {
                eprintln!("Usage: autumn-ps --psid <ID> [OPTIONS]");
                eprintln!();
                eprintln!("Options:");
                eprintln!("  --psid <ID>          Partition server ID (required, non-zero)");
                eprintln!("  --port <PORT>        First partition's listener port [default: 9201]");
                eprintln!("                       (F099-K: subsequent partitions bind PORT+1, PORT+2, ...)");
                eprintln!("  --manager <ADDR>     Manager endpoint [default: 127.0.0.1:9001]");
                eprintln!("  --listen <HOST>      Bind host (IPv4 or bare/bracketed IPv6) [default: 0.0.0.0]");
                eprintln!("  --advertise <ADDR>   Advertise host for cluster discovery");
                eprintln!("                       (F099-K: the `host:port` base — port comes from --port)");
                eprintln!("  --transport <MODE>   Transport backend: tcp (default) or ucx");
                eprintln!("  --cpu-start <N>      First core to pin partition threads to [default: 0]");
                eprintln!("                       Multi-process clusters on one host need disjoint values");
                eprintln!("                       so PS partitions don't share cores with extent-nodes.");
                eprintln!("  --conn-threads <N>   [DEPRECATED, F099-J] accepted but ignored");
                std::process::exit(0);
            }
            other => eprintln!("unknown arg: {other}"),
        }
        i += 1;
    }

    if psid == 0 {
        eprintln!("error: --psid is required and must be non-zero");
        std::process::exit(1);
    }

    Args {
        port,
        psid,
        manager,
        advertise,
        bind_host,
        transport,
        cpu_start,
        group_commit_cap,
        ps_inflight_cap,
        ps_bulk_inflight_cap,
        max_imm_depth,
        max_wal_gap,
        shutdown_timeout_ms,
        major_compact_parallelism,
        conn_inflight_cap,
        fg_rate_bytes_per_sec,
        bg_rate_bytes_per_sec,
        fg_saturated_threshold,
        fg_qps_quota,
        gc_debt_high_bytes,
        compact_pending_high_bytes,
        gc_cooldown_secs,
        compact_cooldown_secs,
        min_pipeline_batch,
        gc_read_chunk_bytes,
        gc_batch_records,
        gc_batch_bytes,
        gc_rate_bytes_per_sec,
        #[cfg(feature = "profiling")]
        pprof_secs,
        #[cfg(feature = "profiling")]
        pprof_out,
        #[cfg(feature = "profiling")]
        pprof_threads,
    }
}

/// F195: apply CLI-derived tunables BEFORE the first PartitionServer
/// construction. Each setter is first-call-wins, so calling here
/// guarantees the binary's values land before any library reader fires.
fn apply_ps_tunables(args: &Args) {
    use autumn_partition_server as ps;
    if let Some(n) = args.group_commit_cap {
        ps::set_max_write_batch(n);
    }
    if let Some(n) = args.ps_inflight_cap {
        ps::set_ps_inflight_cap(n);
    }
    if let Some(n) = args.ps_bulk_inflight_cap {
        ps::set_ps_bulk_inflight_cap(n);
    }
    if let Some(n) = args.max_imm_depth {
        ps::set_max_imm_depth(n);
    }
    if let Some(n) = args.max_wal_gap {
        ps::set_max_wal_gap(n);
    }
    if let Some(n) = args.shutdown_timeout_ms {
        ps::set_shutdown_timeout_ms(n);
    }
    if let Some(n) = args.major_compact_parallelism {
        ps::set_ps_major_compact_parallelism(n);
    }
    if let Some(n) = args.conn_inflight_cap {
        ps::set_ps_conn_inflight_cap(n);
    }
    if let Some(n) = args.fg_rate_bytes_per_sec {
        ps::set_admission_fg_rate(n);
    }
    if let Some(n) = args.bg_rate_bytes_per_sec {
        ps::set_admission_bg_rate(n);
    }
    if let Some(n) = args.fg_saturated_threshold {
        ps::set_admission_fg_saturated_threshold(n);
    }
    if let Some(n) = args.fg_qps_quota {
        ps::set_fg_qps_quota(n);
    }
    if let Some(n) = args.gc_debt_high_bytes {
        ps::set_gc_debt_high(n);
    }
    if let Some(n) = args.compact_pending_high_bytes {
        ps::set_compact_pending_high(n);
    }
    if let Some(n) = args.gc_cooldown_secs {
        ps::set_gc_cooldown_secs(n);
    }
    if let Some(n) = args.compact_cooldown_secs {
        ps::set_compact_cooldown_secs(n);
    }
    if let Some(n) = args.min_pipeline_batch {
        ps::background::set_min_pipeline_batch(n);
    }
    if let Some(n) = args.gc_read_chunk_bytes {
        ps::background::set_gc_read_chunk_bytes(n);
    }
    if let Some(n) = args.gc_batch_records {
        ps::background::set_gc_batch_records(n);
    }
    if let Some(n) = args.gc_batch_bytes {
        ps::background::set_gc_batch_bytes(n);
    }
    if let Some(n) = args.gc_rate_bytes_per_sec {
        ps::background::set_gc_rate_bytes_per_sec(n);
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

    // F195: F164 env-dump removed — production rs code no longer reads
    // AUTUMN_* env vars, so dumping them at startup was misleading. The
    // remaining AUTUMN_* in the operator's shell (e.g. for cluster.sh's
    // own use) are no longer the source of truth for binary config.

    let args = parse_args();

    // ---- F195: pprof CLI flags (replaces AUTUMN_PPROF_* env reads) ----
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
                                    writeln!(
                                        txt,
                                        "thread={} count={}",
                                        frames.thread_name, count
                                    )
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

    // F195: apply PS-library tunables before any library reader fires.
    apply_ps_tunables(&args);

    let _ = autumn_transport::init_with(args.transport);
    autumn_common::set_cpu_offset(args.cpu_start);

    #[cfg(unix)]
    unsafe {
        let mut rl = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) == 0 && rl.rlim_cur < 65535 {
            rl.rlim_cur = rl.rlim_max.min(65535);
            libc::setrlimit(libc::RLIMIT_NOFILE, &rl);
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
        "F099-K: per-partition listener — partition N binds port={}+N-1",
        args.port,
    );

    // F099-K fix: use `_and_port` so `base_port` is set BEFORE `finish_connect`'s
    // implicit `sync_regions_once()` runs `open_partition`. On restart, partitions
    // already exist in the manager — without this, `open_partition` reads
    // `base_port = 0` and binds the first partition to port `0 + 1 = 1`.
    let ps = PartitionServer::connect_with_advertise_and_port(
        args.psid,
        &args.manager,
        Some(advertise),
        addr,
    )
    .await
    .context("connect partition server")?;

    tracing::info!("autumn-ps ready (F099-K: per-partition listeners; first partition on {addr})");

    // F120-C — install a SIGTERM/SIGINT handler. The handler sets an
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
        libc::signal(libc::SIGTERM, handle_term_signal as libc::sighandler_t);
        libc::signal(libc::SIGINT, handle_term_signal as libc::sighandler_t);
    }
}
