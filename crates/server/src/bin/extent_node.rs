use std::path::PathBuf;

use anyhow::{Context, Result};
use autumn_client::ClusterClient;
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, GetClusterIdReq, GetClusterIdResp, CODE_OK, MSG_GET_CLUSTER_ID,
};
use autumn_stream::{ExtentNode, ExtentNodeConfig};
use autumn_transport::TransportKind;

// F193 allocator hygiene + F216-E read-perf fix. jemalloc (vs glibc) keeps the
// EC-convert / copy_extent peak working set (~3 GiB on a 3 GiB sealed extent at
// K=3, M=1) from sitting at the high-water RSS mark indefinitely — its decay
// timers MADV_FREE idle pages back to the OS within seconds.
//
// TWO load-bearing details:
//   1. The config symbol jemalloc reads is `_rjem_malloc_conf`, NOT
//      `malloc_conf`: tikv-jemallocator 0.6 builds jemalloc with the `_rjem_`
//      prefix, so the earlier `#[export_name="malloc_conf"]` was a SILENT NO-OP
//      (jemalloc never saw it; the `dirty_decay_ms:1000` it set never applied —
//      the allocator ran on jemalloc's defaults the whole time).
//   2. `oversize_threshold:0`: jemalloc 5.x's default oversize_threshold is
//      8 MiB, so 8 MiB read/append buffers landed in the dedicated oversize
//      arena that PURGES pages on free — every large read page-faulted cold
//      pages (~2× slower; a TCP 8 MiB read regression bisected to F193).
//      Threshold 0 routes them through normal arenas (warm dirty-page reuse,
//      ~3.1 GB/s vs ~1.6) while decay still returns idle pages.
//
// RUNTIME-CONFIGURABLE: `_RJEM_MALLOC_CONF` (set by cluster.sh / the prod
// launcher, like UCX_TLS) is read after this symbol and overrides/extends it
// per deployment — e.g. `_RJEM_MALLOC_CONF=oversize_threshold:0,dirty_decay_ms:1000`
// to also tighten the EC-spike decay. Linux-only (production target).
#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(target_os = "linux")]
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static malloc_conf: &[u8] = b"oversize_threshold:0\0";

struct Args {
    /// Primary (shard 0) listen port. Sibling shards use
    /// `port + shard_idx * shard_stride` (default stride 10).
    port: u16,
    /// One or more data directories. Comma-separated or repeated --data flags.
    /// F214-D: every dir must be formatted via `autumn-op format` first;
    /// the EN reads `disk_id` + `cluster_id` from each dir's sentinel
    /// files. The old `--disk-id` CLI bypass is gone.
    data_dirs: Vec<PathBuf>,
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
    /// Per-thread regpool cap (pinned/registered bytes). `None` = library
    /// default (512 MiB/thread). Clamped to [16 MiB, 64 GiB].
    ucx_regpool_cap_bytes: Option<usize>,
    /// Observability batch 1: Prometheus `/metrics` HTTP port.
    /// `None` = endpoint disabled (zero cost). One endpoint per PROCESS
    /// (shard gauges are aggregated by the renderer).
    metrics_port: Option<u16>,
    /// Bind host for /metrics only. `None` = follow `--listen`. The
    /// endpoint is unauthenticated — operators exposing the RPC plane
    /// on 0.0.0.0 can pin metrics to 127.0.0.1 with this.
    metrics_listen: Option<String>,
}

fn parse_args() -> Args {
    let mut port: u16 = 9101;
    let mut data_dirs: Vec<PathBuf> = Vec::new();
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
    let mut metrics_port: Option<u16> = None;
    let mut metrics_listen: Option<String> = None;
    // F195: F194 + F099-I knobs as Option<usize>; library defaults when None.
    let mut ec_convert_parallelism: Option<usize> = None;
    let mut recovery_parallelism: Option<usize> = None;
    let mut inflight_cap: Option<usize> = None;
    let mut ucx_regpool_cap_bytes: Option<usize> = None;

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
                // F214-D: --disk-id removed. Every data dir must be
                // formatted via `autumn-op format` first; the EN reads
                // `disk_id` from the dir's sentinel file. Print a
                // migration error and exit 2, matching the --shards
                // pattern below.
                i += 1;
                let _ = args[i].clone();
                eprintln!(
                    "error: --disk-id was removed in F214-D. \
                     Run `autumn-op format --advertise <ADDR> <DIR>...` \
                     first; the EN reads disk_id from each dir's \
                     `disk_id` sentinel file."
                );
                std::process::exit(2);
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
                transport =
                    autumn_transport::parse_transport_flag(&args[i]).unwrap_or_else(|bad| {
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
                    args[i]
                        .parse()
                        .expect("--ec-convert-parallelism must be a number"),
                );
            }
            "--recovery-parallelism" => {
                i += 1;
                recovery_parallelism = Some(
                    args[i]
                        .parse()
                        .expect("--recovery-parallelism must be a number"),
                );
            }
            "--inflight-cap" => {
                i += 1;
                inflight_cap = Some(args[i].parse().expect("--inflight-cap must be a number"));
            }
            "--ucx-regpool-cap-bytes" => {
                i += 1;
                ucx_regpool_cap_bytes = Some(
                    args[i]
                        .parse()
                        .expect("--ucx-regpool-cap-bytes usize"),
                );
            }
            "--metrics-port" => {
                i += 1;
                metrics_port = Some(args[i].parse().expect("--metrics-port must be a port"));
            }
            "--metrics-listen" => {
                i += 1;
                metrics_listen = Some(args[i].clone());
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
        ucx_regpool_cap_bytes,
        metrics_port,
        metrics_listen,
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

/// F214-D: async manager cross-check. Connects to the manager once,
/// fetches its cluster_id, and verifies it matches the value stamped
/// in our data dirs. Catches the "EN pointed at the wrong manager"
/// misconfiguration that the on-disk consistency check alone cannot
/// see. No retry — if the manager isn't reachable at startup we want
/// to bubble up the error fast.
async fn verify_manager_cluster_id(manager: &str, stamped: &str) -> Result<()> {
    let client = ClusterClient::connect(manager)
        .await
        .with_context(|| format!("connect to manager {manager} for cluster_id verify"))?;
    let resp_bytes = client
        .mgr_call(MSG_GET_CLUSTER_ID, rkyv_encode(&GetClusterIdReq {}))
        .await
        .context("get cluster_id from manager")?;
    let resp: GetClusterIdResp =
        rkyv_decode(&resp_bytes).map_err(|e| anyhow::anyhow!("decode GetClusterIdResp: {e}"))?;
    if resp.code != CODE_OK {
        anyhow::bail!(
            "manager replied error to GetClusterId: code={} msg={}",
            resp.code,
            resp.message
        );
    }
    // WIRE-1 (coco P2): explicit compat check here too — the transitive
    // check inside ClusterClient::connect covers today's path, but this
    // site decodes its own resp and must not depend on that coupling.
    // R1: relaxed from fingerprint equality to interval overlap.
    if let Err(msg) = autumn_rpc::wire_compat_check(
        &resp.wire_fingerprint,
        resp.wire_version_min,
        resp.wire_version_max,
    ) {
        anyhow::bail!(msg);
    }
    if resp.cluster_id != stamped {
        anyhow::bail!(
            "cluster_id mismatch: data dirs stamped for cluster {} but manager {} reports {}. \
             Point at the correct manager, or re-run `autumn-op format` against this cluster.",
            stamped,
            manager,
            resp.cluster_id
        );
    }
    Ok(())
}

/// F214-D: read the `cluster_id` sentinel file from each data dir and
/// verify they all agree. Returns the shared cluster_id string. Panics
/// (via `Result::Err` → main returns) with an actionable message if any
/// dir is unformatted, partially-formatted, or formatted for a different
/// cluster than its siblings. Run synchronously in main()'s prelude so
/// shard threads never start against a misconfigured dir set.
fn read_and_verify_cluster_id(data_dirs: &[PathBuf]) -> Result<String> {
    if data_dirs.is_empty() {
        anyhow::bail!("no --data dirs supplied");
    }
    let mut shared: Option<String> = None;
    for dir in data_dirs {
        let cid_path = dir.join("cluster_id");
        if !cid_path.exists() {
            anyhow::bail!(
                "data dir {} is not formatted (no cluster_id sentinel). \
                 Run `autumn-op format --advertise <HOST:PORT> {}` first.",
                dir.display(),
                dir.display()
            );
        }
        let cid = std::fs::read_to_string(&cid_path)
            .with_context(|| format!("read cluster_id in {}", dir.display()))?
            .trim()
            .to_string();
        if cid.is_empty() {
            anyhow::bail!(
                "data dir {} has an empty cluster_id file — re-run \
                 `autumn-op format` against this dir",
                dir.display()
            );
        }
        match &shared {
            None => shared = Some(cid),
            Some(prev) if prev == &cid => {}
            Some(prev) => {
                anyhow::bail!(
                    "data dirs disagree on cluster_id: {} reports {} but a prior \
                     dir reports {}. All --data dirs for one EN must belong to \
                     the same cluster.",
                    dir.display(),
                    cid,
                    prev
                );
            }
        }
    }
    Ok(shared.unwrap())
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = parse_args();
    // Apply regpool cap BEFORE init_with so the first transport-touch (and
    // thus first TLS pool init) reads the operator's setting.
    if let Some(cap) = args.ucx_regpool_cap_bytes {
        if !autumn_transport::set_regpool_cap_bytes(cap) {
            tracing::warn!(cap, "regpool cap already set (ignored — first-call-wins)");
        }
    }
    let _ = autumn_transport::init_with(args.transport);

    // F216-E: RDMA (UCX rc_mlx5) pins every registered send/recv buffer against
    // RLIMIT_MEMLOCK via ibv_reg_mr. The default soft limit (often 8 MiB) faults
    // libibverbs on large (e.g. 8 MiB) value transfers — raise to INFINITY,
    // same as the PS. Harmless on TCP. Falls back to soft-up-to-hard.
    #[cfg(unix)]
    unsafe {
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

    // F214-D: verify every --data dir has a matching cluster_id
    // sentinel file before launching shard threads. The manager
    // cross-check happens inside the per-shard async block below
    // (needs a compio runtime to do an RPC).
    let stamped_cluster_id = read_and_verify_cluster_id(&args.data_dirs)?;
    tracing::info!(
        cluster_id = %stamped_cluster_id,
        "F214-D: data dirs verified consistent"
    );
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

    // Observability batch 1: ONE /metrics endpoint per process, spawned
    // before the shard threads (covers both single- and multi-shard
    // paths). The renderer reads process-global atomics + the per-shard
    // gauge slots each ExtentNode registers at construction — no compio
    // involvement, safe on its own OS thread.
    if let Some(mport) = args.metrics_port {
        let mhost = args.metrics_listen.as_deref().unwrap_or(&args.bind_host);
        match autumn_common::metrics_http::spawn_metrics_http(
            mhost,
            mport,
            std::sync::Arc::new(autumn_stream::render_en_metrics),
        ) {
            Ok(()) => tracing::info!(port = mport, host = mhost, "metrics endpoint up at /metrics"),
            // Auxiliary — a taken metrics port must not kill the data plane.
            Err(e) => tracing::error!(port = mport, "metrics endpoint bind failed: {e}"),
        }
    }

    if shards == 1 {
        // Single-shard fast path — preserve exact pre-F196 behaviour.
        return run_single_shard(args, stamped_cluster_id);
    }

    // Multi-shard: spawn one OS thread per shard, each with its own compio
    // runtime + io_uring + TcpListener + ExtentNode instance. Each shard
    // pins to one core via the shared `pick_cpu_for_ord` helper.
    let mut joins = Vec::with_capacity(shards as usize);
    for shard_idx in 0..shards {
        let data_dirs = args.data_dirs.clone();
        let manager = args.manager.clone();
        let stamped_cluster_id = stamped_cluster_id.clone();
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
        // Fail-stop: any shard exit (Err / panic / unexpected clean return)
        // calls `std::process::exit(1)` directly. The join loop below is
        // therefore unreachable in steady state — its only role is to park
        // the main thread alive so the OS keeps the shard threads scheduled.
        // Rationale: a half-online EN (one shard's bind failed → its port
        // silently doesn't serve) is hard to spot from the manager — df
        // succeeds on the surviving shards and the node still looks
        // "Online". Better to die loudly so the operator notices.
        let join = std::thread::Builder::new()
            .name(format!("extent-shard-{shard_idx}"))
            .spawn(move || {
                let shard_main = std::panic::AssertUnwindSafe(move || -> Result<()> {
                    let rt = compio::runtime::RuntimeBuilder::new()
                        .thread_affinity(autumn_common::affinity_set(cpu))
                        .build()
                        .context("create compio runtime")?;
                    tracing::info!(shard_idx, ?cpu, "extent-shard runtime ready");
                    rt.block_on(async move {
                        let addr = autumn_transport::format_listen_addr(&bind_host, listen_port)
                            .context("parse listen address")?;
                        autumn_transport::check_listen_addr(
                            addr,
                            autumn_transport::current().kind(),
                        )
                        .ok();
                        // F191: per-shard control listener — same SQ/CQ
                        // machinery, no API churn.
                        let ctl_addr = autumn_transport::format_listen_addr(
                            &bind_host,
                            control_listen_port,
                        )
                        .context("parse control listen address")?;

                        // F214-D: only shard 0 runs the manager cross-check;
                        // it's the same check for every shard, so doing it
                        // once is sufficient. Skipped when no manager is
                        // configured (test deployments).
                        if shard_idx == 0 {
                            if let Some(mgr) = manager.as_ref() {
                                verify_manager_cluster_id(mgr, &stamped_cluster_id).await?;
                            }
                        }

                        let mut cfg = ExtentNodeConfig::new_multi(data_dirs);
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

                        let node = ExtentNode::new(cfg)
                            .await
                            .with_context(|| format!("create ExtentNode shard {shard_idx}"))?;
                        node.serve_with_control(addr, ctl_addr).await
                    })
                });
                match std::panic::catch_unwind(shard_main) {
                    Ok(Ok(())) => {
                        tracing::error!(
                            shard_idx,
                            "extent-node shard exited cleanly — fail-stop \
                             (accept_loop should never return Ok)"
                        );
                        std::process::exit(1);
                    }
                    Ok(Err(e)) => {
                        tracing::error!(
                            shard_idx,
                            error = ?e,
                            "extent-node shard error — fail-stop"
                        );
                        std::process::exit(1);
                    }
                    Err(panic) => {
                        tracing::error!(
                            shard_idx,
                            ?panic,
                            "extent-node shard panicked — fail-stop"
                        );
                        std::process::exit(1);
                    }
                }
            })
            .with_context(|| format!("spawn extent-shard-{shard_idx}"))?;
        joins.push(join);
    }

    // Park forever. Any shard failure has already called process::exit(1)
    // from inside the shard thread; reaching the end of this loop would
    // mean every shard returned Ok cleanly (impossible in steady state —
    // accept_loop runs an infinite loop).
    for (idx, j) in joins.into_iter().enumerate() {
        let _ = j.join();
        tracing::error!(
            shard_idx = idx,
            "extent-node shard join returned — fail-stop \
             (shard should have called process::exit on its own)"
        );
        std::process::exit(1);
    }
    Ok(())
}

fn run_single_shard(args: Args, stamped_cluster_id: String) -> Result<()> {
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

        // F214-D: manager cross-check (skipped when --manager is omitted).
        if let Some(mgr) = args.manager.as_ref() {
            verify_manager_cluster_id(mgr, &stamped_cluster_id).await?;
        }

        let mut config = ExtentNodeConfig::new_multi(args.data_dirs.clone());
        if let Some(mgr) = args.manager.clone() {
            config = config.with_manager_endpoint(mgr);
        }
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
