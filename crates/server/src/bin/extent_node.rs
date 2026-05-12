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
    /// F099-M: number of compio runtimes (shards) to spawn in this process.
    /// Each shard owns extents where `extent_id % shards == shard_idx` and
    /// listens on `port + shard_idx * shard_stride`. Default 1 (legacy).
    shards: u32,
    /// F099-M: port stride between sibling shards.
    shard_stride: u16,
    /// Bind host for the listener (IPv4 or bare/bracketed IPv6). Default 0.0.0.0.
    bind_host: String,
    transport: TransportKind,
    /// First core to pin shard threads to. Multi-process clusters on one host
    /// need disjoint values across processes so they don't share cores.
    cpu_start: usize,
    /// F191: control-plane listener port. None → derive as `port + 1000`.
    /// Each shard binds its own control listener at
    /// `control_port + shard_idx * shard_stride`. Operators only need
    /// to override this when 1000 collides on a non-default deployment.
    control_port: Option<u16>,
}

fn parse_args() -> Args {
    let mut port: u16 = 9101;
    let mut data_dirs: Vec<PathBuf> = Vec::new();
    let mut disk_id: Option<u64> = None;
    let mut manager: Option<String> = None;
    // Default shard count from AUTUMN_EXTENT_SHARDS env, else 1.
    let mut shards: u32 = std::env::var("AUTUMN_EXTENT_SHARDS")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|v| *v >= 1)
        .unwrap_or(1);
    let mut shard_stride: u16 = std::env::var("AUTUMN_EXTENT_SHARD_STRIDE")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .filter(|v| *v >= 1)
        .unwrap_or(10);
    let mut bind_host = String::from("0.0.0.0");
    let mut transport = TransportKind::Tcp;
    let mut cpu_start: usize = 0;
    // F191: optional override; default = port + 1000.
    let mut control_port: Option<u16> = None;

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
                shards = args[i].parse().expect("--shards must be a number");
                assert!(shards >= 1, "--shards must be >= 1");
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
            "--control-port" => {
                i += 1;
                control_port = Some(args[i].parse().expect("--control-port must be a number"));
            }
            other => eprintln!("unknown arg: {other}"),
        }
        i += 1;
    }

    if data_dirs.is_empty() {
        data_dirs.push(PathBuf::from("/tmp/autumn-extent"));
    }

    Args {
        port,
        data_dirs,
        disk_id,
        manager,
        shards,
        shard_stride,
        bind_host,
        transport,
        cpu_start,
        control_port,
    }
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
    autumn_common::set_cpu_offset(args.cpu_start);

    // F099-M: each shard i listens on port + i * shard_stride.
    let shard_ports: Vec<u16> = (0..args.shards)
        .map(|i| args.port + (i as u16) * args.shard_stride)
        .collect();
    // F191: per-shard control port. Operator can override the shard-0
    // base via --control-port; per-shard stride matches data plane so
    // shard-N has its own control listener too.
    let control_port_base = args.control_port.unwrap_or(args.port + 1000);
    let control_ports: Vec<u16> = (0..args.shards)
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
        shards = args.shards,
        ports = ?shard_ports,
        "autumn-extent-node starting"
    );

    if args.shards == 1 {
        // Legacy single-thread path — preserve exact behaviour.
        return run_single_shard(args);
    }

    // Multi-shard: spawn one OS thread per shard, each with its own compio
    // runtime + io_uring + TcpListener + ExtentNode instance. Each shard
    // pins to one core via the shared `pick_cpu_for_ord` helper (cpuset
    // honored via `taskset -c <set>`; surplus shards log a WARN and float).
    let mut joins = Vec::with_capacity(args.shards as usize);
    for shard_idx in 0..args.shards {
        let data_dirs = args.data_dirs.clone();
        let disk_id = args.disk_id;
        let manager = args.manager.clone();
        let siblings = sibling_addrs.clone();
        let shards = args.shards;
        let listen_port = shard_ports[shard_idx as usize];
        let bind_host = args.bind_host.clone();
        let cpu = autumn_common::pick_cpu_for_ord(shard_idx as usize);

        let control_listen_port = control_ports[shard_idx as usize];
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
                    cfg = cfg.with_shard(shard_idx, shards, siblings);

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
            let data = args.data_dirs.into_iter().next().unwrap();
            let mut c = ExtentNodeConfig::new(data, args.disk_id.unwrap());
            if let Some(mgr) = args.manager {
                c = c.with_manager_endpoint(mgr);
            }
            c
        } else {
            let mut c = ExtentNodeConfig::new_multi(args.data_dirs);
            if let Some(mgr) = args.manager {
                c = c.with_manager_endpoint(mgr);
            }
            c
        };

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
