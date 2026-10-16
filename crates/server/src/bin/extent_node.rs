use std::path::PathBuf;

use anyhow::{Context, Result};
use autumn_client::ClusterClient;
use autumn_rpc::manager_rpc::{
    rkyv_decode, rkyv_encode, GetClusterIdReq, GetClusterIdResp, RegisterNodeReq, RegisterNodeResp,
    CODE_NOT_LEADER, CODE_OK, MSG_GET_CLUSTER_ID, MSG_REGISTER_NODE,
};
use autumn_stream::{ExtentNode, ExtentNodeConfig};
use autumn_params::{Positional, Section};
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

const USAGE: &str =
    "usage: autumn-extent-node --cluster-secret-file <PATH> --data <DIR[,DIR...]> [options]";

/// async manager cross-check. Connects to the manager once,
/// fetches its cluster_id, and verifies it matches the value stamped
/// in our data dirs. Catches the "EN pointed at the wrong manager"
/// misconfiguration that the on-disk consistency check alone cannot
/// see. A manager that is unreachable or refuses our wire version
/// (`transient_manager_error`) is waited for, with a WARN per attempt —
/// during a wire rollout the manager may be replaced after this node. A
/// cluster_id mismatch is terminal.
async fn verify_manager_cluster_id(manager: &str, stamped: &str) -> Result<()> {
    let mut delay = 1u64;
    loop {
        match verify_manager_cluster_id_once(manager, stamped).await {
            Ok(()) => return Ok(()),
            Err(e) if transient_manager_error(&e) => {
                tracing::warn!(manager, wire = autumn_rpc::WIRE_VERSION, error = %format!("{e:#}"), delay, "waiting for compatible manager; dependencies not ready");
                compio::time::sleep(std::time::Duration::from_secs(delay)).await;
                delay = (delay + 1).min(5);
            }
            Err(e) => return Err(e),
        }
    }
}

fn transient_manager_error(e: &anyhow::Error) -> bool {
    e.downcast_ref::<autumn_rpc::RpcError>().is_some_and(|e| {
        matches!(
            e,
            autumn_rpc::RpcError::VersionMismatch { .. }
                | autumn_rpc::RpcError::Io(_)
                | autumn_rpc::RpcError::ConnectionClosed
                | autumn_rpc::RpcError::Timeout(_)
        )
    })
}

async fn verify_manager_cluster_id_once(manager: &str, stamped: &str) -> Result<()> {
    let client = ClusterClient::connect_peer(manager)
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
    // An extent node is a CLUSTER peer, so it needs the manager's exact
    // version. `wire_version_min` is the CLIENT floor and says nothing about
    // whether this node belongs in this cluster.
    if let Err(msg) = autumn_rpc::cluster_peer_compat_check(resp.wire_version_max) {
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
            .with_context(|| format!("read disk_uuid in {} (dir not formatted?)", dir.display()))?
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
            .with_context(|| format!("read node_uuid in {} (dir not formatted?)", dir.display()))?
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

/// The control listener's port: `--control-port`, else `--port` + 1000. The
/// default is computed only when the flag is absent, so an explicit control
/// port works with any data port; without one, a data port above 64535 has no
/// default and is refused.
fn local_control_port(control_port: Option<u16>, port: u16) -> Result<u16> {
    match control_port {
        Some(p) => Ok(p),
        None => port.checked_add(1000).ok_or_else(|| {
            anyhow::anyhow!("--port {port} + 1000 exceeds 65535; pass --control-port")
        }),
    }
}

/// The control port the manager is told to dial: `--control-port` when given
/// (the port the control listener binds, so behind a proxy it must also be the
/// proxy's), else the advertised data port + 1000, matching the listener's own
/// default of `--port` + 1000.
fn advertise_control_port(control_port: Option<u16>, advertise_port: u16) -> Result<u16> {
    match control_port {
        Some(p) => Ok(p),
        None => advertise_port.checked_add(1000).ok_or_else(|| {
            anyhow::anyhow!(
                "advertised port {advertise_port} + 1000 exceeds 65535; pass --control-port"
            )
        }),
    }
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
/// picked up on the next boot without re-running `format`. An unreachable or
/// wire-mismatched manager (`transient_manager_error`) is waited for without
/// limit, 5 s apart; a manager mid-election (NOT_LEADER) is retried 30 × 1 s;
/// a hard refusal (fenced / decommissioned / cluster mismatch) or exhausted
/// NOT_LEADER retries fail-stop — an EN the manager can't route to must not
/// serve (same rationale as the multi-shard bind fail-stop).
async fn register_with_manager(manager: &str, req: &RegisterNodeReq) -> Result<()> {
    let mut last_err: String;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let step = async {
            let client = ClusterClient::connect_peer(manager)
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
        if let Err(e) = &step {
            if transient_manager_error(e) {
                tracing::warn!(manager, wire = autumn_rpc::WIRE_VERSION, error = %e, "waiting for compatible manager registration");
                compio::time::sleep(std::time::Duration::from_secs(5)).await;
                attempt = 0;
                continue;
            }
        }
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
        if attempt >= 30 {
            break;
        }
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

    autumn_params::parse_or_exit(USAGE, &[autumn_stream::en_params() as &dyn Section], Positional::Refuse);
    let p = autumn_stream::en_params();
    let (manager, advertise, data, listen) =
        (p.manager.get(), p.advertise.get(), p.data.get(), p.listen.get());
    let (port, shard_stride, control_port) = (p.port.get(), p.shard_stride.get(), p.control_port.get());
    let transport: TransportKind = p.transport.get().into();
    let direct_io = p.direct_io();
    if let Err(e) = autumn_rpc::peer_auth::install_for_server(p.cluster_secret_file.get().as_deref())
        .and_then(|()| match manager.as_deref() {
            Some(m) => autumn_rpc::peer_auth::designate_managers(m),
            None => Ok(()),
        })
    {
        eprintln!("error: {e}");
        std::process::exit(2);
    }
    // Apply regpool cap BEFORE init_with so the first transport-touch (and
    // thus first TLS pool init) reads the operator's setting.
    let regpool_cap = autumn_stream::en_params().ucx_regpool_cap_bytes.get();
    if !autumn_transport::set_regpool_cap_bytes(regpool_cap) {
        tracing::warn!(regpool_cap, "regpool cap already set (ignored — first-call-wins)");
    }
    let _ = autumn_transport::init_with(transport);

    // the EN is by far the biggest fd consumer — `ExtentNode::load_extents` opens every
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
    let stamped_cluster_id = read_and_verify_cluster_id(&data)?;
    tracing::info!(
        cluster_id = %stamped_cluster_id,
        "data dirs verified consistent"
    );
    // --cpuset (if given) is installed BEFORE any cpu_pin reader
    // fires, so the cached core list reflects the override. Without
    // it the pool is `core_affinity::get_core_ids()`.
    let cpuset_given = p.cpuset.get().is_some();
    if let Some(cs) = p.cpuset.get().map(|c| c.0) {
        let _ = autumn_common::set_cpuset(cs);
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
        .map(|i| port + (i as u16) * shard_stride)
        .collect();
    // per-shard control port. Operator can override the shard-0
    // base via --control-port; per-shard stride matches data plane so
    // shard-N has its own control listener too.
    let control_port_base = local_control_port(control_port, port)?;
    let control_ports: Vec<u16> = (0..shards)
        .map(|i| control_port_base + (i as u16) * shard_stride)
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
    let advertise_port: Option<u16> = match (manager.as_ref(), advertise.as_ref()) {
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
                if sa.port() != port {
                    tracing::warn!(
                        advertise_port = sa.port(),
                        listen_port = port,
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
        .map(|ap| {
            (0..shards)
                .map(|i| ap + (i as u16) * shard_stride)
                .collect()
        })
        .unwrap_or_default();
    let advertise_control_base: u16 = match advertise_port {
        Some(ap) => advertise_control_port(control_port, ap)?,
        None => 0,
    };

    // Sibling addresses — used by each shard to forward control-plane RPCs
    // to the owning sibling when a mismatched extent_id arrives. Must use
    // the same bind host so UCX/RoCE connections reach the right address.
    let sibling_addrs: Vec<String> = shard_ports
        .iter()
        .map(|p| {
            autumn_transport::format_listen_addr(&listen, *p)
                .map(|sa| sa.to_string())
                .unwrap_or_else(|_| format!("{}:{}", listen, p))
        })
        .collect();

    tracing::info!(
        shards,
        ports = ?shard_ports,
        "autumn-extent-node starting"
    );

    // ONE /metrics endpoint per process, on its own thread and runtime,
    // spawned before any shard thread pins itself. The renderer reads
    // process-global atomics + the per-shard gauge slots each ExtentNode
    // registers at construction.
    let mut metrics_reach = String::new();
    if let Some(mport) = autumn_common::metrics_http::metrics_port(p.metrics_port.get(), port) {
        let metrics_listen = p.metrics_listen.get();
        let mhost = metrics_listen.as_deref().unwrap_or(&listen);
        let render = std::sync::Arc::new(autumn_stream::render_en_metrics);
        match autumn_common::metrics_http::spawn_metrics_http(mhost, mport, render) {
            Ok(()) => {
                // The advertised host when bound to every address; without
                // --advertise the node is not registered and reports nothing.
                let adv_host = advertise.as_deref().and_then(|a| a.rsplit_once(':')).map_or("", |(h, _)| h);
                metrics_reach = autumn_common::metrics_http::metrics_reach_addr(mhost, adv_host, mport);
                tracing::info!(port = mport, host = mhost, reach = %metrics_reach, "metrics endpoint up at /metrics");
            }
            // Auxiliary — a taken metrics port must not kill the data plane.
            Err(e) => tracing::error!(port = mport, "metrics endpoint bind failed: {e}"),
        }
    }

    if shards == 1 {
        // Single-shard fast path — preserve exact single-core behaviour.
        return run_single_shard(stamped_cluster_id, metrics_reach);
    }

    // Multi-shard: spawn one OS thread per shard, each with its own compio
    // runtime + io_uring + TcpListener + ExtentNode instance. Each shard
    // pins to one core via the shared `pick_cpu_for_ord` helper.
    let mut joins = Vec::with_capacity(shards as usize);
    // Each shard reports once its listeners are bound (after `ExtentNode::new`
    // opened its disks and passed the O_DIRECT check). The node is ready only
    // when all of them did; a shard that fails first exits the process.
    let (bound_tx, bound_rx) = std::sync::mpsc::channel::<()>();
    // One authz config for the whole process, fetched by shard 0.
    let shared_authz = autumn_stream::SharedAuthz::new(manager.is_some());
    for shard_idx in 0..shards {
        let data_dirs = data.clone();
        let manager = manager.clone();
        let stamped_cluster_id = stamped_cluster_id.clone();
        let siblings = sibling_addrs.clone();
        let shards_for_thread = shards;
        let listen_port = shard_ports[shard_idx as usize];
        let bind_host = listen.clone();
        let cpu = autumn_common::pick_cpu_for_ord(shard_idx as usize);

        let control_listen_port = control_ports[shard_idx as usize];
        // M1: shard 0 self-registers the live location. Capture
        // the full port vector + advertise + control base + transport (unused
        // by shards > 0).
        let reg_advertise = advertise.clone();
        // Register the ADVERTISE-side (peer-reachable) ports, NOT the local
        // bind ports — they differ behind a proxy/NAT (see advertise_port above).
        let reg_shard_ports = advertise_shard_ports.clone();
        let reg_control_base = advertise_control_base;
        let reg_transport = transport;
        let reg_metrics_addr = metrics_reach.clone();
        let authz = shared_authz.clone();
        let bound_tx = bound_tx.clone();
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
                    autumn_common::cpu_pin::pin_current(cpu).context("pin extent shard")?;
                    let rt = compio::runtime::RuntimeBuilder::new()
                        .build()
                        .context("create compio runtime")?;
                    autumn_common::cpu_pin::confine_io_workers(&rt);
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
                        let ctl_addr =
                            autumn_transport::format_listen_addr(&bind_host, control_listen_port)
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
                        cfg = cfg
                            .with_shard(shard_idx, shards_for_thread, siblings)
                            .with_shared_authz(authz);
                        if let Some((nu, adv, ports)) = reg_for_cfg {
                            cfg = cfg.with_registration(nu, adv, ports);
                        }
                        if shard_idx == 0 {
                            cfg = cfg.with_metrics_addr(reg_metrics_addr);
                        }
                        cfg = cfg.with_direct_io(direct_io);

                        let node = ExtentNode::new(cfg)
                            .await
                            .with_context(|| format!("create ExtentNode shard {shard_idx}"))?;
                        node.serve_with_control(addr, ctl_addr, move || {
                            bound_tx
                                .send(())
                                .expect("main holds bound_rx for the process lifetime");
                        })
                        .await
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
    drop(bound_tx);
    if (0..shards).all(|_| bound_rx.recv().is_ok()) {
        tracing::info!(
            shards,
            first_port = shard_ports[0],
            "autumn-extent-node ready: every shard's listeners are bound"
        );
    }
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

fn run_single_shard(stamped_cluster_id: String, metrics_addr: String) -> Result<()> {
    let p = autumn_stream::en_params();
    let (manager, advertise, data, listen) =
        (p.manager.get(), p.advertise.get(), p.data.get(), p.listen.get());
    let (port, control_port) = (p.port.get(), p.control_port.get());
    let transport: TransportKind = p.transport.get().into();
    let direct_io = p.direct_io();
    let cpu = autumn_common::pick_cpu_for_ord(0);
    autumn_common::cpu_pin::pin_current(cpu).context("pin extent node")?;
    let rt = compio::runtime::RuntimeBuilder::new()
        .build()
        .context("create compio runtime")?;
    autumn_common::cpu_pin::confine_io_workers(&rt);
    tracing::info!(?cpu, "extent-node (single-shard) runtime ready");
    // control port defaults to port + 1000.
    let ctl_port = local_control_port(control_port, port)?;
    rt.block_on(async move {
        let addr = autumn_transport::format_listen_addr(&listen, port)
            .context("parse listen address")?;
        autumn_transport::check_listen_addr(addr, autumn_transport::current().kind()).ok();
        let ctl_addr = autumn_transport::format_listen_addr(&listen, ctl_port)
            .context("parse control listen address")?;

        // manager cross-check (skipped when --manager is omitted).
        let mut reg_for_cfg: Option<(String, String, Vec<u16>)> = None;
        if let Some(mgr) = manager.as_ref() {
            verify_manager_cluster_id(mgr, &stamped_cluster_id).await?;
            // M1: self-register live location before serving. A
            // single-shard node registers `[advertise_port]` — the PEER-reachable
            // port (== --port normally, but the proxy/NAT port when they differ),
            // and control = --control-port, else advertise_port + 1000
            // (`advertise_control_port`). main() already required
            // --advertise here, so the parse cannot fail.
            if let Some(adv) = advertise.as_ref() {
                let advertise_port = adv
                    .parse::<std::net::SocketAddr>()
                    .expect("--advertise validated in main()")
                    .port();
                let (node_uuid, disk_uuids) = read_node_identity(&data)?;
                let req = build_register_req(
                    adv,
                    transport,
                    advertise_control_port(control_port, advertise_port)?,
                    &node_uuid,
                    &disk_uuids,
                    &[advertise_port],
                );
                register_with_manager(mgr, &req).await?;
                reg_for_cfg = Some((node_uuid, adv.clone(), vec![advertise_port]));
            }
        }

        let mut config = ExtentNodeConfig::new_multi(data.clone());
        if let Some(mgr) = manager.clone() {
            config = config.with_manager_endpoint(mgr);
        }
        // M1b: echo identity in handle_df for the manager's
        // drift-heal / imposter check.
        if let Some((nu, adv, ports)) = reg_for_cfg {
            config = config.with_registration(nu, adv, ports).with_metrics_addr(metrics_addr);
        }
        let config = config.with_direct_io(direct_io);

        let node = ExtentNode::new(config).await.context("create ExtentNode")?;
        node.serve_with_control(addr, ctl_addr, || {
            tracing::info!(
                data_addr = %addr,
                ctl_addr = %ctl_addr,
                "autumn-extent-node ready: listeners are bound"
            );
        })
        .await?;
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

    /// The registered control port is the one the listener binds: an explicit
    /// `--control-port` is used as is, not replaced by the data port + 1000.
    #[test]
    fn registered_control_port_follows_the_flag() {
        assert_eq!(advertise_control_port(Some(29421), 29420).unwrap(), 29421);
        assert_eq!(advertise_control_port(None, 29420).unwrap(), 30420);
        assert!(advertise_control_port(None, 65000).is_err());
    }

    /// An explicit control port never touches the data port + 1000 default,
    /// which does not exist for a data port above 64535.
    #[test]
    fn an_explicit_control_port_works_with_any_data_port() {
        assert_eq!(local_control_port(Some(21878), 65000).unwrap(), 21878);
        assert_eq!(local_control_port(None, 9101).unwrap(), 10101);
        assert!(local_control_port(None, 65000).is_err());
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
