use std::path::PathBuf;

use anyhow::{Context, Result};
use autumn_client::ClusterClient;
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, GetClusterIdReq, GetClusterIdResp, RegisterNodeReq, RegisterNodeResp,
    CODE_NOT_LEADER, CODE_OK, MSG_GET_CLUSTER_ID, MSG_REGISTER_NODE,
};
use autumn_stream::{ExtentNode, ExtentNodeConfig};
use autumn_transport::TransportKind;

// allocator hygiene + read-perf fix. jemalloc (vs glibc) keeps the
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
//      pages (~2× slower; a TCP 8 MiB read regression).
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
    /// every dir must be formatted via `autumn-op format` first;
    /// the EN reads `disk_id` + `cluster_id` from each dir's sentinel
    /// files. The old `--disk-id` CLI bypass is gone.
    data_dirs: Vec<PathBuf>,
    manager: Option<String>,
    /// port stride between sibling shards. Shard count itself
    /// is always `cpuset_len` — supply `--cpuset` to control it.
    shard_stride: u16,
    /// Bind host for the listener (IPv4 or bare/bracketed IPv6). Default 0.0.0.0.
    bind_host: String,
    transport: TransportKind,
    /// First core to pin shard threads to. Multi-process clusters on one host
    /// need disjoint values across processes so they don't share cores.
    /// Mutually exclusive with `--cpuset`.
    cpu_start: usize,
    /// explicit list of cores this binary may pin to, taskset
    /// syntax (e.g. `4-11`, `0,2,4`, `0-3,8-11`). Overrides
    /// `core_affinity::get_core_ids()` snapshot and disables
    /// `--cpu-start`. When unset, behaviour matches the earlier default.
    cpuset: Option<Vec<usize>>,
    /// control-plane listener port. None → derive as `port + 1000`.
    /// Each shard binds its own control listener at
    /// `control_port + shard_idx * shard_stride`. Operators only need
    /// to override this when 1000 collides on a non-default deployment.
    control_port: Option<u16>,
    /// (was env `AUTUMN_EXTENT_EC_CONVERT_PARALLELISM`).
    /// Default 1; clamped to [1, 16] by library.
    ec_convert_parallelism: Option<usize>,
    /// (was env `AUTUMN_EXTENT_RECOVERY_PARALLELISM`).
    /// Default 2; clamped to [1, 16] by library.
    recovery_parallelism: Option<usize>,
    /// (was env `AUTUMN_EXTENT_INFLIGHT_CAP`). Default 64.
    inflight_cap: Option<usize>,
    /// Chunked EC-convert stripe size (bytes). `None` = library default
    /// (64 MiB). Peak EC-convert RAM = `(K+M) × stripe`. Clamped to
    /// [1 MiB, 1 GiB] by the library.
    ec_stripe_bytes: Option<usize>,
    /// max resident SEALED-extent fds cached per shard. `None` =
    /// library default (4096). Bounds open fds on a node with many extents.
    fd_cache_cap: Option<usize>,
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
    /// M1: `HOST:PORT` this EN announces to the manager at
    /// startup (HOST must be an IP — the binary stays DNS-free; the shell
    /// resolves names). PORT is the shard-0 data port (== `--port`). When set
    /// (and `--manager` is given), the EN self-registers its live location +
    /// shard ports on every boot, so a changed shard-port layout (a reshard) or
    /// a fresh pod IP is picked up automatically. When unset, the EN relies on
    /// the location `autumn-op format` stamped (the pre-M1 behavior).
    advertise: Option<String>,
}

fn parse_args() -> Args {
    let mut port: u16 = 9101;
    let mut data_dirs: Vec<PathBuf> = Vec::new();
    let mut manager: Option<String> = None;
    // `--shards` was removed. Shard count = cpuset_len; supply
    // `--cpuset` to control it. `--shard-stride` survives as the
    // port stride between sibling shards.
    let mut shard_stride: u16 = 10;
    let mut bind_host = String::from("0.0.0.0");
    let mut transport = TransportKind::Tcp;
    let mut cpu_start: usize = 0;
    let mut cpuset: Option<Vec<usize>> = None;
    // optional override; default = port + 1000.
    let mut control_port: Option<u16> = None;
    let mut metrics_port: Option<u16> = None;
    let mut metrics_listen: Option<String> = None;
    // parallelism + inflight-cap knobs as Option<usize>; library defaults when None.
    let mut ec_convert_parallelism: Option<usize> = None;
    let mut recovery_parallelism: Option<usize> = None;
    let mut inflight_cap: Option<usize> = None;
    let mut ec_stripe_bytes: Option<usize> = None;
    let mut fd_cache_cap: Option<usize> = None;
    let mut ucx_regpool_cap_bytes: Option<usize> = None;
    let mut advertise: Option<String> = None;

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
                // --disk-id removed. Every data dir must be
                // formatted via `autumn-op format` first; the EN reads
                // `disk_id` from the dir's sentinel file. Print a
                // migration error and exit 2, matching the --shards
                // pattern below.
                i += 1;
                let _ = args[i].clone();
                eprintln!(
                    "error: --disk-id was removed. \
                     Run `autumn-op format <DIR>...` \
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
                    "error: --shards was removed; pass --cpuset <SPEC> to size the EN (shard count = cpuset_len)"
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
            "--ec-stripe-bytes" => {
                i += 1;
                ec_stripe_bytes =
                    Some(args[i].parse().expect("--ec-stripe-bytes must be a number"));
            }
            "--fd-cache-cap" => {
                i += 1;
                fd_cache_cap =
                    Some(args[i].parse().expect("--fd-cache-cap must be a number"));
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
            "--advertise" => {
                i += 1;
                advertise = Some(args[i].clone());
            }
            other => eprintln!("unknown arg: {other}"),
        }
        i += 1;
    }

    if data_dirs.is_empty() {
        data_dirs.push(PathBuf::from("/tmp/autumn-extent"));
    }

    // --cpuset and --cpu-start are mutually exclusive at the CLI
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
        ec_stripe_bytes,
        fd_cache_cap,
        ucx_regpool_cap_bytes,
        metrics_port,
        metrics_listen,
        advertise,
    }
}

/// helper — apply the parallelism / inflight-cap CLI flags to an ExtentNodeConfig.
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

/// async manager cross-check. Connects to the manager once,
/// fetches its cluster_id, and verifies it matches the value stamped
/// in our data dirs. Catches the "EN pointed at the wrong manager"
/// misconfiguration that the on-disk consistency check alone cannot
/// see. No retry — if the manager isn't reachable at startup we want
/// to bubble up the error fast.
async fn verify_manager_cluster_id(manager: &str, stamped: &str) -> Result<()> {
    let client = ClusterClient::connect_raw(manager)
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

/// read the `cluster_id` sentinel file from each data dir and
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
                 Run `autumn-op format {}` first.",
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

/// M1: read the identity sentinels `autumn-op format` stamped —
/// the single `node_uuid` (must agree across every dir) and each dir's
/// `disk_uuid` (in `--data` order). The EN re-reports these to the manager at
/// startup (self-registration), so a missing / empty / disagreeing sentinel is
/// fail-loud (the dir was never formatted, or a mixed / cloned data set).
fn read_node_identity(data_dirs: &[PathBuf]) -> Result<(String, Vec<String>)> {
    let mut node_uuid: Option<String> = None;
    let mut disk_uuids: Vec<String> = Vec::with_capacity(data_dirs.len());
    for dir in data_dirs {
        let du = std::fs::read_to_string(dir.join("disk_uuid"))
            .with_context(|| {
                format!("read disk_uuid in {} (dir not formatted?)", dir.display())
            })?
            .trim()
            .to_string();
        if du.is_empty() {
            anyhow::bail!(
                "empty disk_uuid in {} — re-run `autumn-op format`",
                dir.display()
            );
        }
        disk_uuids.push(du);

        let nu = std::fs::read_to_string(dir.join("node_uuid"))
            .with_context(|| {
                format!("read node_uuid in {} (dir not formatted?)", dir.display())
            })?
            .trim()
            .to_string();
        if nu.is_empty() {
            anyhow::bail!(
                "empty node_uuid in {} — re-run `autumn-op format`",
                dir.display()
            );
        }
        match &node_uuid {
            None => node_uuid = Some(nu),
            Some(prev) if prev == &nu => {}
            Some(prev) => anyhow::bail!(
                "data dirs disagree on node_uuid: {} reports {} but a prior dir reports \
                 {} — a mixed or cloned data set must not be served by one EN",
                dir.display(),
                nu,
                prev
            ),
        }
    }
    Ok((node_uuid.unwrap(), disk_uuids))
}

/// M1: build the self-registration request (pure — no I/O, so the
/// control_address derivation is unit-testable). UCX serves control RPCs on the
/// data listener (a second `ucp_listener` on the same RoCE device can't bind),
/// so it registers an EMPTY control_address → the manager's df falls back to the
/// data addr. TCP keeps a separate control port for HoL isolation. (This
/// transport-conditional logic moved here from `autumn-op format`, §2.2.)
fn build_register_req(
    advertise: &str,
    transport: TransportKind,
    control_port_base: u16,
    node_uuid: &str,
    disk_uuids: &[String],
    shard_ports: &[u16],
) -> RegisterNodeReq {
    let control_address = match transport {
        TransportKind::Ucx => String::new(),
        _ => {
            let host = advertise.rsplit_once(':').map_or(advertise, |(h, _)| h);
            format!("{host}:{control_port_base}")
        }
    };
    RegisterNodeReq {
        addr: advertise.to_string(),
        disk_uuids: disk_uuids.to_vec(),
        shard_ports: shard_ports.to_vec(),
        control_address,
        node_uuid: node_uuid.to_string(),
    }
}

/// M1: self-register the EN's LIVE location (advertise address +
/// the shard ports this process actually binds) with the manager at startup.
/// The manager keys the node by `node_uuid` (M0) and updates the location IN
/// PLACE — so a changed shard-port layout (a reshard) or a fresh pod IP is
/// picked up on the next boot without re-running `format`. Retries through a
/// manager mid-election (30 × 1 s, like PS `register_ps`); fail-stops on a hard
/// refusal (fenced / decommissioned / cluster mismatch) or on exhaustion — an
/// EN the manager can't route to must not serve (same rationale as the
/// multi-shard bind fail-stop).
async fn register_with_manager(
    manager: &str,
    req: &RegisterNodeReq,
) -> Result<()> {
    let mut last_err = String::new();
    for attempt in 1..=30u32 {
        let step = async {
            let client = ClusterClient::connect_raw(manager)
                .await
                .with_context(|| format!("connect to manager {manager}"))?;
            let bytes = client
                .mgr_call(MSG_REGISTER_NODE, rkyv_encode(req))
                .await
                .context("register-node RPC")?;
            let resp: RegisterNodeResp = rkyv_decode(&bytes)
                .map_err(|e| anyhow::anyhow!("decode RegisterNodeResp: {e}"))?;
            Ok::<RegisterNodeResp, anyhow::Error>(resp)
        }
        .await;
        match step {
            Ok(resp) if resp.code == CODE_OK => {
                tracing::info!(
                    node_id = resp.node_id,
                    advertise = %req.addr,
                    ports = ?req.shard_ports,
                    "M1: EN self-registered its location with the manager"
                );
                return Ok(());
            }
            // NOT_LEADER is transient (mid-election) → retry. Any other refusal
            // (fenced / decommissioned uuid, address conflict, cluster mismatch)
            // is terminal → fail-stop with the manager's message verbatim.
            Ok(resp) if resp.code == CODE_NOT_LEADER => {
                last_err = format!("manager not leader: {}", resp.message);
            }
            Ok(resp) => anyhow::bail!(
                "manager refused EN self-registration: code={} {}",
                resp.code,
                resp.message
            ),
            Err(e) => last_err = format!("{e:#}"),
        }
        tracing::warn!(attempt, error = %last_err, "EN self-register retry");
        compio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    anyhow::bail!("EN self-registration failed after 30 attempts: {last_err}")
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = parse_args();
    // Apply the EC-convert stripe size (process-global, first-call-wins) before
    // any shard runs an EC convert. Flag > env > 64 MiB default.
    if let Some(n) = args.ec_stripe_bytes {
        if !autumn_stream::set_ec_encode_stripe_bytes(n) {
            tracing::warn!(n, "ec-stripe-bytes already set (ignored — first-call-wins)");
        }
    }
    // apply the sealed-extent fd-cache cap (process-global,
    // first-call-wins) before ExtentNode::new opens extents.
    if let Some(n) = args.fd_cache_cap {
        if !autumn_stream::set_fd_cache_cap(n) {
            tracing::warn!(n, "fd-cache-cap already set (ignored — first-call-wins)");
        }
    }
    // Apply regpool cap BEFORE init_with so the first transport-touch (and
    // thus first TLS pool init) reads the operator's setting.
    if let Some(cap) = args.ucx_regpool_cap_bytes {
        if !autumn_transport::set_regpool_cap_bytes(cap) {
            tracing::warn!(cap, "regpool cap already set (ignored — first-call-wins)");
        }
    }
    let _ = autumn_transport::init_with(args.transport);

    // the EN is the fd大户 — `ExtentNode::load_extents` opens every
    // owned extent's data file at startup and the fd-LRU cache keeps up to
    // `--fd-cache-cap` of them open. The default RLIMIT_NOFILE soft limit (often
    // 1024) is far too small — a multi-disk node with 16 GiB extents already
    // approaches it, and smaller extents blow past → EMFILE in the load_extents
    // open loop / on alloc_new_extent. Raise the soft limit to 65535 (same as
    // the PS, `partition_server.rs`), clamped to the hard limit. Harmless if
    // already high. Set BEFORE the shard threads run `load_extents`.
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
    }

    // RDMA (UCX rc_mlx5) pins every registered send/recv buffer against
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

    // verify every --data dir has a matching cluster_id
    // sentinel file before launching shard threads. The manager
    // cross-check happens inside the per-shard async block below
    // (needs a compio runtime to do an RPC).
    let stamped_cluster_id = read_and_verify_cluster_id(&args.data_dirs)?;
    tracing::info!(
        cluster_id = %stamped_cluster_id,
        "data dirs verified consistent"
    );
    // --cpuset (if given) is installed BEFORE any cpu_pin reader
    // fires, so the cached core list reflects the override. Without
    // --cpuset we fall back to the legacy --cpu-start offset over
    // `core_affinity::get_core_ids()`.
    let cpuset_given = args.cpuset.is_some();
    if let Some(cs) = args.cpuset.clone() {
        let _ = autumn_common::set_cpuset(cs);
    } else {
        autumn_common::set_cpu_offset(args.cpu_start);
    }
    // `--cpuset` is the sole sizing surface. Shard count = cpuset_len
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
        "EN sized from cpuset"
    );
    if cpuset_n <= 1 {
        tracing::warn!(
            "EN started with a single-core cpuset; no parallelism across extent shards. \
             Consider growing --cpuset for production loads."
        );
    }

    // each shard i listens on port + i * shard_stride.
    let shard_ports: Vec<u16> = (0..shards)
        .map(|i| args.port + (i as u16) * args.shard_stride)
        .collect();
    // per-shard control port. Operator can override the shard-0
    // base via --control-port; per-shard stride matches data plane so
    // shard-N has its own control listener too.
    let control_port_base = args.control_port.unwrap_or(args.port + 1000);
    let control_ports: Vec<u16> = (0..shards)
        .map(|i| control_port_base + (i as u16) * args.shard_stride)
        .collect();

    // M1c: --advertise is now REQUIRED whenever --manager is
    // given. `autumn-op format` (M1c) is identity-only — it no longer stamps
    // a location — so an EN started without --advertise would self-register
    // NOTHING and sit at an empty location forever (df can never reach it,
    // stays Suspend, never selected for allocation). `--manager`-less runs
    // (offline / unit-test invocations) are exempt — there's no registrar to
    // report to. HOST must be an IP (DNS-free per the repo rule — the shell
    // resolves names). The advertise PORT is the shard-0 port AS SEEN BY PEERS
    // — normally == --port, but it MAY differ behind NAT / a proxy (the manager
    // routes to advertise_host:advertise_port + i*stride while the EN binds
    // --port + i*stride locally). We warn (not fail) on a mismatch so a bare
    // typo surfaces without forbidding the legitimate proxy case. `advertise_port`
    // (Some when --advertise given) drives the REGISTERED shard_ports below.
    let advertise_port: Option<u16> = match (args.manager.as_ref(), args.advertise.as_ref()) {
        (Some(_), None) => {
            eprintln!(
                "error: --advertise HOST:PORT is required when --manager is given \
                 (M1c — `autumn-op format` no longer stamps a location; \
                 the EN self-registers its own address + shard ports at every startup)."
            );
            std::process::exit(2);
        }
        (_, Some(adv)) => match adv.parse::<std::net::SocketAddr>() {
            Ok(sa) => {
                if sa.port() != args.port {
                    tracing::warn!(
                        advertise_port = sa.port(),
                        listen_port = args.port,
                        "advertise port differs from --port — assuming NAT/proxy; \
                         registered shard_ports derive from the advertise port"
                    );
                }
                Some(sa.port())
            }
            Err(e) => {
                eprintln!("error: --advertise must be IP:PORT (DNS-free), got {adv:?}: {e}");
                std::process::exit(2);
            }
        },
        (None, None) => None,
    };
    // Peer-reachable (advertise-side) shard ports + control base — what the
    // manager routes to, distinct from the local bind ports (`shard_ports` /
    // `control_port_base`, derived from --port). Equal in the common case
    // (advertise_port == --port), different behind a proxy.
    let advertise_shard_ports: Vec<u16> = advertise_port
        .map(|ap| (0..shards).map(|i| ap + (i as u16) * args.shard_stride).collect())
        .unwrap_or_default();
    let advertise_control_base: u16 = advertise_port.map_or(0, |ap| ap.saturating_add(1000));

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
        // Single-shard fast path — preserve exact single-core behaviour.
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
        // M1: shard 0 self-registers the live location. Capture
        // the full port vector + advertise + control base + transport (unused
        // by shards > 0).
        let reg_advertise = args.advertise.clone();
        // Register the ADVERTISE-side (peer-reachable) ports, NOT the local
        // bind ports — they differ behind a proxy/NAT (see advertise_port above).
        let reg_shard_ports = advertise_shard_ports.clone();
        let reg_control_base = advertise_control_base;
        let reg_transport = args.transport;
        // capture parallelism / inflight-cap tunable overrides for the thread.
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
                        // per-shard control listener — same SQ/CQ
                        // machinery, no API churn.
                        let ctl_addr = autumn_transport::format_listen_addr(
                            &bind_host,
                            control_listen_port,
                        )
                        .context("parse control listen address")?;

                        // only shard 0 runs the manager cross-check;
                        // it's the same check for every shard, so doing it
                        // once is sufficient. Skipped when no manager is
                        // configured (test deployments).
                        // M1b: identity to echo in this shard's
                        // `handle_df` (only shard 0 is dialed by the manager df).
                        let mut reg_for_cfg: Option<(String, String, Vec<u16>)> = None;
                        if shard_idx == 0 {
                            if let Some(mgr) = manager.as_ref() {
                                verify_manager_cluster_id(mgr, &stamped_cluster_id).await?;
                                // M1: self-register live location +
                                // shard ports BEFORE any shard serves. Only when
                                // --advertise is given (else keep the format-stamped
                                // location, the pre-M1 behavior).
                                if let Some(adv) = reg_advertise.as_ref() {
                                    let (node_uuid, disk_uuids) =
                                        read_node_identity(&data_dirs)?;
                                    let req = build_register_req(
                                        adv,
                                        reg_transport,
                                        reg_control_base,
                                        &node_uuid,
                                        &disk_uuids,
                                        &reg_shard_ports,
                                    );
                                    register_with_manager(mgr, &req).await?;
                                    reg_for_cfg =
                                        Some((node_uuid, adv.clone(), reg_shard_ports.clone()));
                                }
                            }
                        } else if manager.is_some() {
                            // Sibling shards need the node's IDENTITY even though
                            // they don't register a LOCATION (only shard 0 is
                            // dialed by the manager's df, so only it echoes an
                            // advertise address).
                            //
                            // Every shard runs its own reconcile, for its own
                            // disjoint set of extents, and the manager answers
                            // nothing to a reporter it cannot identify. Without
                            // the uuid here, shards 1..N got NO verdict — so on
                            // any multi-core EN the orphan backstop and the
                            // post-conversion `.dat` reclaim were dead for
                            // (N-1)/N of the disk, silently.
                            if let Ok((node_uuid, _)) = read_node_identity(&data_dirs) {
                                reg_for_cfg = Some((node_uuid, String::new(), Vec::new()));
                            }
                        }

                        let mut cfg = ExtentNodeConfig::new_multi(data_dirs);
                        if let Some(mgr) = manager {
                            cfg = cfg.with_manager_endpoint(mgr);
                        }
                        cfg = cfg.with_shard(shard_idx, shards_for_thread, siblings);
                        if let Some((nu, adv, ports)) = reg_for_cfg {
                            cfg = cfg.with_registration(nu, adv, ports);
                        }
                        // per-shard tunables.
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

    // Park on shard 0's join handle. Any shard failure has already called
    // process::exit(1) from inside its own thread, so a join RETURNING (even
    // Ok) is itself anomalous — accept_loop runs an infinite loop — and we
    // fail-stop on it. (Only shard 0 is awaited: another shard exiting
    // cleanly without process::exit would go unnoticed, same as the previous
    // loop form, which also never reached index 1.)
    if let Some(j) = joins.into_iter().next() {
        let _ = j.join();
        tracing::error!(
            shard_idx = 0,
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
    // control port defaults to port + 1000.
    let ctl_port = args.control_port.unwrap_or(args.port + 1000);
    rt.block_on(async move {
        let addr = autumn_transport::format_listen_addr(&args.bind_host, args.port)
            .context("parse listen address")?;
        autumn_transport::check_listen_addr(addr, autumn_transport::current().kind()).ok();
        let ctl_addr = autumn_transport::format_listen_addr(&args.bind_host, ctl_port)
            .context("parse control listen address")?;

        // manager cross-check (skipped when --manager is omitted).
        let mut reg_for_cfg: Option<(String, String, Vec<u16>)> = None;
        if let Some(mgr) = args.manager.as_ref() {
            verify_manager_cluster_id(mgr, &stamped_cluster_id).await?;
            // M1: self-register live location before serving. A
            // single-shard node registers `[advertise_port]` — the PEER-reachable
            // port (== --port normally, but the proxy/NAT port when they differ),
            // and control = advertise_port + 1000. main() already required
            // --advertise here, so the parse cannot fail.
            if let Some(adv) = args.advertise.as_ref() {
                let advertise_port = adv
                    .parse::<std::net::SocketAddr>()
                    .expect("--advertise validated in main()")
                    .port();
                let (node_uuid, disk_uuids) = read_node_identity(&args.data_dirs)?;
                let req = build_register_req(
                    adv,
                    args.transport,
                    advertise_port.saturating_add(1000),
                    &node_uuid,
                    &disk_uuids,
                    &[advertise_port],
                );
                register_with_manager(mgr, &req).await?;
                reg_for_cfg = Some((node_uuid, adv.clone(), vec![advertise_port]));
            }
        }

        let mut config = ExtentNodeConfig::new_multi(args.data_dirs.clone());
        if let Some(mgr) = args.manager.clone() {
            config = config.with_manager_endpoint(mgr);
        }
        // M1b: echo identity in handle_df for the manager's
        // drift-heal / imposter check.
        if let Some((nu, adv, ports)) = reg_for_cfg {
            config = config.with_registration(nu, adv, ports);
        }
        // parallelism / inflight-cap tunables.
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

#[cfg(test)]
mod tests {
    use super::*;

    // M1: the control_address derivation is transport-conditional
    // (TCP → separate control port; UCX → empty = manager df falls back to the
    // data addr), and the request must echo the live location + shard ports.
    #[test]
    fn build_register_req_tcp_derives_control_address() {
        let req = build_register_req(
            "10.0.0.5:9101",
            TransportKind::Tcp,
            10101,
            "uuid-A",
            &["disk-A".to_string(), "disk-B".to_string()],
            &[9101, 9111],
        );
        assert_eq!(req.addr, "10.0.0.5:9101");
        assert_eq!(req.control_address, "10.0.0.5:10101");
        assert_eq!(req.shard_ports, vec![9101u16, 9111]);
        assert_eq!(req.node_uuid, "uuid-A");
        assert_eq!(req.disk_uuids, vec!["disk-A".to_string(), "disk-B".to_string()]);
    }

    #[test]
    fn build_register_req_ucx_has_empty_control_address() {
        let req = build_register_req(
            "10.0.0.5:9101",
            TransportKind::Ucx,
            10101,
            "uuid-A",
            &["disk-A".to_string()],
            &[9101],
        );
        // UCX can't bind a second listener on the RoCE device → empty control
        // address → the manager df dials the data addr.
        assert!(req.control_address.is_empty());
        assert_eq!(req.addr, "10.0.0.5:9101");
        assert_eq!(req.shard_ports, vec![9101u16]);
    }
}
