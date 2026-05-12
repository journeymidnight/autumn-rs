use std::path::PathBuf;

use anyhow::{Context, Result};
use autumn_stream::{ExtentNode, ExtentNodeConfig};
use autumn_transport::TransportKind;

// F193 allocator hygiene. tikv-jemallocator's `dirty_decay_ms` /
// `muzzy_decay_ms` knobs make the allocator MADV_FREE / MADV_DONTNEED
// dirty pages back to the OS within ~1 s of becoming unused. Without
// this, the peak working set during EC convert + copy_extent (~3 GiB
// on a 3 GiB sealed extent at K=3, M=1) stays mapped indefinitely
// after the spike, leaving the process RSS at the high-water mark
// even when the live heap is back to a few hundred MB.
//
// MALLOC_CONF can be overridden at process launch time; the static
// here is the default. Linux-only — the binary is a Linux-only
// production target; macOS dev builds use the system allocator.
#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(target_os = "linux")]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"dirty_decay_ms:1000,muzzy_decay_ms:1000\0";

struct Args {
    /// Primary (shard 0) listen port. Sibling shards use
    /// `port + shard_idx * shard_stride` (default stride 10).
    port: u16,
    /// One or more data directories. Comma-separated or repeated --data flags.
    data_dirs: Vec<PathBuf>,
    /// Optional explicit disk_id for single-disk backward-compat mode.
    disk_id: Option<u64>,
    manager: Option<String>,
    /// F099-M: port stride between sibling shards. Shard count itself
    /// (F196) is always `cpuset_len` — supply `--cpuset` to control it.
    shard_stride: u16,
    /// Bind host for the listener (IPv4 or bare/bracketed IPv6). Default 0.0.0.0.
    bind_host: String,
    transport: TransportKind,
    /// First core to pin shard threads to. Multi-process clusters on one host
    /// need disjoint values across processes so they don't share cores.
    /// Mutually exclusive with `--cpuset`.
    cpu_start: usize,
    /// F196: explicit list of cores this binary may pin to, taskset
    /// syntax (e.g. `4-11`, `0,2,4`, `0-3,8-11`). Overrides
    /// `core_affinity::get_core_ids()` snapshot and disables
    /// `--cpu-start`. When unset, behaviour matches pre-F196.
    cpuset: Option<Vec<usize>>,
    /// F191: control-plane listener port. None → derive as `port + 1000`.
    /// Each shard binds its own control listener at
    /// `control_port + shard_idx * shard_stride`. Operators only need
    /// to override this when 1000 collides on a non-default deployment.
    control_port: Option<u16>,
    /// F195 (was F194 env `AUTUMN_EXTENT_EC_CONVERT_PARALLELISM`).
    /// Default 1; clamped to [1, 16] by library.
    ec_convert_parallelism: Option<usize>,
    /// F195 (was F194 env `AUTUMN_EXTENT_RECOVERY_PARALLELISM`).
    /// Default 2; clamped to [1, 16] by library.
    recovery_parallelism: Option<usize>,
    /// F195 (was env `AUTUMN_EXTENT_INFLIGHT_CAP`, F099-I). Default 64.
    inflight_cap: Option<usize>,
}

fn parse_args() -> Args {
    let mut port: u16 = 9101;
    let mut data_dirs: Vec<PathBuf> = Vec::new();
    let mut disk_id: Option<u64> = None;
    let mut manager: Option<String> = None;
    // F196: `--shards` was removed. Shard count = cpuset_len; supply
    // `--cpuset` to control it. `--shard-stride` survives as the
    // port stride between sibling shards.
    let mut shard_stride: u16 = 10;
    let mut bind_host = String::from("0.0.0.0");
    let mut transport = TransportKind::Tcp;
    let mut cpu_start: usize = 0;
    let mut cpuset: Option<Vec<usize>> = None;
    // F191: optional override; default = port + 1000.
    let mut control_port: Option<u16> = None;
    // F195: F194 + F099-I knobs as Option<usize>; library defaults when None.
    let mut ec_convert_parallelism: Option<usize> = None;
    let mut recovery_parallelism: Option<usize> = None;
    let mut inflight_cap: Option<usize> = None;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                i += 1;
                port = args[i].parse().expect("--port must be a number");
            }
            "--data" => {
                i += 1;
                for part in args[i].split(',') {
                    let p = part.trim();
                    if !p.is_empty() {
                        data_dirs.push(PathBuf::from(p));
                    }
                }
            }
            "--disk-id" => {
                i += 1;
                disk_id = Some(args[i].parse().expect("--disk-id must be a number"));
            }
            "--manager" => {
                i += 1;
                manager = Some(args[i].clone());
            }
            "--shards" => {
                i += 1;
                let _ = args[i].clone();
                eprintln!(
                    "error: --shards was removed in F196; pass --cpuset <SPEC> to size the EN (shard count = cpuset_len)"
                );
                std::process::exit(2);
            }
            "--shard-stride" => {
                i += 1;
                shard_stride = args[i].parse().expect("--shard-stride must be a number");
                assert!(shard_stride >= 1, "--shard-stride must be >= 1");
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
            "--cpuset" => {
                i += 1;
                cpuset = Some(autumn_common::parse_cpuset(&args[i]).unwrap_or_else(|e| {
                    eprintln!("--cpuset parse error: {e}");
                    std::process::exit(2);
                }));
            }
            "--control-port" => {
                i += 1;
                control_port = Some(args[i].parse().expect("--control-port must be a number"));
            }
            "--ec-convert-parallelism" => {
                i += 1;
                ec_convert_parallelism = Some(
                    args[i].parse().expect("--ec-convert-parallelism must be a number"),
                );
            }
            "--recovery-parallelism" => {
                i += 1;
                recovery_parallelism = Some(
                    args[i].parse().expect("--recovery-parallelism must be a number"),
                );
            }
            "--inflight-cap" => {
                i += 1;
                inflight_cap = Some(args[i].parse().expect("--inflight-cap must be a number"));
            }
            other => eprintln!("unknown arg: {other}"),
        }
        i += 1;
    }

    if data_dirs.is_empty() {
        data_dirs.push(PathBuf::from("/tmp/autumn-extent"));
    }

    // F196: --cpuset and --cpu-start are mutually exclusive at the CLI
    // layer. cpuset is the final list; offset has no meaning on top of
    // an explicit list.
    if cpuset.is_some() && cpu_start != 0 {
        eprintln!("error: --cpuset and --cpu-start are mutually exclusive");
        std::process::exit(2);
    }

    Args {
        port,
        data_dirs,
        disk_id,
        manager,
        shard_stride,
        bind_host,
        transport,
        cpu_start,
        cpuset,
        control_port,
        ec_convert_parallelism,
        recovery_parallelism,
        inflight_cap,
    }
}

/// F195: helper — apply the F194 / F099-I CLI flags to an ExtentNodeConfig.
/// `None` means "library default" and skips the builder call.
fn apply_extent_tunables(
    mut cfg: autumn_stream::ExtentNodeConfig,
    args: &Args,
) -> autumn_stream::ExtentNodeConfig {
    if let Some(n) = args.ec_convert_parallelism {
        cfg = cfg.with_ec_convert_parallelism(n);
    }
    if let Some(n) = args.recovery_parallelism {
        cfg = cfg.with_recovery_parallelism(n);
    }
    if let Some(n) = args.inflight_cap {
        cfg = cfg.with_inflight_cap(n);
    }
    cfg
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = parse_args();
    let _ = autumn_transport::init_with(args.transport);
    // F196: --cpuset (if given) is installed BEFORE any cpu_pin reader
    // fires, so the cached core list reflects the override. Without
    // --cpuset we fall back to the legacy --cpu-start offset over
    // `core_affinity::get_core_ids()`.
    let cpuset_given = args.cpuset.is_some();
    if let Some(cs) = args.cpuset.clone() {
        let _ = autumn_common::set_cpuset(cs);
    } else {
        autumn_common::set_cpu_offset(args.cpu_start);
    }
    // F196: `--cpuset` is the sole sizing surface. Shard count = cpuset_len
    // (one shard per pre-allocated core). `--shards` was removed; CLI
    // parsing rejects it. Default with no --cpuset gives `cpuset_len`
    // shards over the auto-detected core set — pass `--cpuset 0` (or a
    // 1-core spec) to force a single-shard layout for legacy single-disk
    // deployments.
    let cpuset_n = autumn_common::cpuset_len();
    let shards: u32 = cpuset_n.max(1) as u32;
    tracing::info!(
        cpuset_len = cpuset_n,
        cpuset_given,
        shards,
        "F196: EN sized from cpuset"
    );
    if cpuset_n <= 1 {
        tracing::warn!(
            "F196: EN started with a single-core cpuset; no parallelism across extent shards. \
             Consider growing --cpuset for production loads."
        );
    }

    // F099-M: each shard i listens on port + i * shard_stride.
    let shard_ports: Vec<u16> = (0..shards)
        .map(|i| args.port + (i as u16) * args.shard_stride)
        .collect();
    // F191: per-shard control port. Operator can override the shard-0
    // base via --control-port; per-shard stride matches data plane so
    // shard-N has its own control listener too.
    let control_port_base = args.control_port.unwrap_or(args.port + 1000);
    let control_ports: Vec<u16> = (0..shards)
        .map(|i| control_port_base + (i as u16) * args.shard_stride)
        .collect();

    // Sibling addresses — used by each shard to forward control-plane RPCs
    // to the owning sibling when a mismatched extent_id arrives. Must use
    // the same bind host so UCX/RoCE connections reach the right address.
    let sibling_addrs: Vec<String> = shard_ports
        .iter()
        .map(|p| {
            autumn_transport::format_listen_addr(&args.bind_host, *p)
                .map(|sa| sa.to_string())
                .unwrap_or_else(|_| format!("{}:{}", args.bind_host, p))
        })
        .collect();

    tracing::info!(
        shards,
        ports = ?shard_ports,
        "autumn-extent-node starting"
    );

    if shards == 1 {
        // Single-shard fast path — preserve exact pre-F196 behaviour.
        return run_single_shard(args);
    }

    // Multi-shard: spawn one OS thread per shard, each with its own compio
    // runtime + io_uring + TcpListener + ExtentNode instance. Each shard
    // pins to one core via the shared `pick_cpu_for_ord` helper.
    let mut joins = Vec::with_capacity(shards as usize);
    for shard_idx in 0..shards {
        let data_dirs = args.data_dirs.clone();
        let disk_id = args.disk_id;
        let manager = args.manager.clone();
        let siblings = sibling_addrs.clone();
        let shards_for_thread = shards;
        let listen_port = shard_ports[shard_idx as usize];
        let bind_host = args.bind_host.clone();
        let cpu = autumn_common::pick_cpu_for_ord(shard_idx as usize);

        let control_listen_port = control_ports[shard_idx as usize];
        // F195: capture F194 / F099-I tunable overrides for the thread.
        let ec_par = args.ec_convert_parallelism;
        let rec_par = args.recovery_parallelism;
        let inflight = args.inflight_cap;
        let join = std::thread::Builder::new()
            .name(format!("extent-shard-{shard_idx}"))
            .spawn(move || -> Result<()> {
                let rt = compio::runtime::RuntimeBuilder::new()
                    .thread_affinity(autumn_common::affinity_set(cpu))
                    .build()
                    .context("create compio runtime")?;
                tracing::info!(shard_idx, ?cpu, "extent-shard runtime ready");
                rt.block_on(async move {
                    let addr = autumn_transport::format_listen_addr(&bind_host, listen_port)
                        .context("parse listen address")?;
                    autumn_transport::check_listen_addr(addr, autumn_transport::current().kind())
                        .ok();
                    // F191: per-shard control listener — same SQ/CQ
                    // machinery, no API churn.
                    let ctl_addr = autumn_transport::format_listen_addr(
                        &bind_host,
                        control_listen_port,
                    )
                    .context("parse control listen address")?;

                    let mut cfg = if data_dirs.len() == 1 && disk_id.is_some() {
                        let data = data_dirs.into_iter().next().unwrap();
                        ExtentNodeConfig::new(data, disk_id.unwrap())
                    } else {
                        ExtentNodeConfig::new_multi(data_dirs)
                    };
                    if let Some(mgr) = manager {
                        cfg = cfg.with_manager_endpoint(mgr);
                    }
                    cfg = cfg.with_shard(shard_idx, shards_for_thread, siblings);
                    // F195: per-shard tunables.
                    if let Some(n) = ec_par {
                        cfg = cfg.with_ec_convert_parallelism(n);
                    }
                    if let Some(n) = rec_par {
                        cfg = cfg.with_recovery_parallelism(n);
                    }
                    if let Some(n) = inflight {
                        cfg = cfg.with_inflight_cap(n);
                    }

                    tracing::info!(
                        shard_idx,
                        addr = %addr,
                        ctl_addr = %ctl_addr,
                        "extent-node shard listening"
                    );

                    let node = ExtentNode::new(cfg).await
                        .with_context(|| format!("create ExtentNode shard {shard_idx}"))?;
                    node.serve_with_control(addr, ctl_addr).await
                })
            })
            .with_context(|| format!("spawn extent-shard-{shard_idx}"))?;
        joins.push(join);
    }

    // Wait forever (or until one thread exits). If any shard thread exits
    // with an error, bubble it up.
    for (idx, j) in joins.into_iter().enumerate() {
        match j.join() {
            Ok(Ok(())) => tracing::info!(shard_idx = idx, "extent-node shard exited cleanly"),
            Ok(Err(e)) => tracing::error!(shard_idx = idx, error = ?e, "extent-node shard error"),
            Err(panic) => tracing::error!(shard_idx = idx, ?panic, "extent-node shard panicked"),
        }
    }
    Ok(())
}

fn run_single_shard(args: Args) -> Result<()> {
    let cpu = autumn_common::pick_cpu_for_ord(0);
    let rt = compio::runtime::RuntimeBuilder::new()
        .thread_affinity(autumn_common::affinity_set(cpu))
        .build()
        .context("create compio runtime")?;
    tracing::info!(?cpu, "extent-node (single-shard) runtime ready");
    // F191: control port defaults to port + 1000.
    let ctl_port = args.control_port.unwrap_or(args.port + 1000);
    rt.block_on(async move {
        let addr = autumn_transport::format_listen_addr(&args.bind_host, args.port)
            .context("parse listen address")?;
        autumn_transport::check_listen_addr(addr, autumn_transport::current().kind()).ok();
        let ctl_addr = autumn_transport::format_listen_addr(&args.bind_host, ctl_port)
            .context("parse control listen address")?;

        let config = if args.data_dirs.len() == 1 && args.disk_id.is_some() {
            let data = args.data_dirs.iter().next().unwrap().clone();
            let mut c = ExtentNodeConfig::new(data, args.disk_id.unwrap());
            if let Some(mgr) = args.manager.clone() {
                c = c.with_manager_endpoint(mgr);
            }
            c
        } else {
            let mut c = ExtentNodeConfig::new_multi(args.data_dirs.clone());
            if let Some(mgr) = args.manager.clone() {
                c = c.with_manager_endpoint(mgr);
            }
            c
        };
        // F195: F194 / F099-I tunables.
        let config = apply_extent_tunables(config, &args);

        tracing::info!(
            data_addr = %addr,
            ctl_addr = %ctl_addr,
            "autumn-extent-node listening"
        );

        let node = ExtentNode::new(config).await.context("create ExtentNode")?;
        node.serve_with_control(addr, ctl_addr).await?;
        Ok(())
    })
}
