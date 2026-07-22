//! F211-G + F213: companion CLI for manager control plane.
//!
//! `autumn-op` is the canonical operator interface. It speaks rkyv over
//! the manager RPC framing and prints either human-readable or
//! `--json` output (the Python ops tooling in `python/` consumes the
//! JSON form).
//!
//! Two command families:
//!   * F211 node-lifecycle: list-nodes / extent-health / list-ec-markers
//!     / recovery-stats / audit-log + fence-node / maintenance / unfence
//!     / remove.
//!   * F213 (moved from autumn-client): bootstrap / set-stream-ec /
//!     force-ec-convert / split / merge / policy-candidates / compact /
//!     gc / forcegc / register-node / format / info.
//!
//! The data-plane CLI `autumn-client` MUST NOT regrow direct manager
//! admin RPCs — if it needs op data in the future, route through this
//! binary.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use autumn_client::{decode_err, ClusterClient, DEFAULT_RPC_TIMEOUT};
use autumn_rpc::manager_rpc::*;
use autumn_rpc::partition_rpc::{
    DiagPartitionVpReq, DiagPartitionVpResp, GetDiscardsReq, GetDiscardsResp,
    MSG_DIAG_PARTITION_VP, MSG_GET_DISCARDS,
};
use bytes::Bytes;
use serde::Serialize;


mod args;
use args::{
    fuse_split_ranges, hex_split_ranges, parse, parse_replication, presplit_suffixes, Args, Command,
    PresplitRule, SplitPoint,
};

fn format_disk(dir: &str) -> Result<String> {
    for byte in 0u8..=255 {
        let subdir = format!("{}/{:02x}", dir, byte);
        std::fs::create_dir_all(&subdir).with_context(|| format!("create hash subdir {subdir}"))?;
    }
    let disk_uuid = uuid::Uuid::new_v4().to_string();
    let marker_path = format!("{}/{}", dir, disk_uuid);
    std::fs::File::create(&marker_path)
        .with_context(|| format!("create UUID marker {marker_path}"))?;
    Ok(disk_uuid)
}

/// F214-C: fetch the manager's cluster_id. Retries on `CODE_NOT_LEADER`
/// (same pattern as other admin RPCs in this binary). Returns an error
/// if the cluster has never been imprinted (manager replied with
/// `CODE_ERROR` "not yet bootstrapped"), which only happens before the
/// first leader election against a fresh etcd.
async fn fetch_cluster_id(client: &ClusterClient) -> Result<String> {
    let req_bytes = rkyv_encode(&GetClusterIdReq {});
    let mut attempt = 0u32;
    loop {
        let resp_bytes = client
            .mgr_call(MSG_GET_CLUSTER_ID, req_bytes.clone())
            .await
            .context("get cluster_id")?;
        let resp: GetClusterIdResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
        if resp.code == CODE_OK {
            if resp.cluster_id.is_empty() {
                bail!("manager replied OK with empty cluster_id");
            }
            return Ok(resp.cluster_id);
        }
        if resp.code == CODE_NOT_LEADER && attempt < 60 {
            attempt += 1;
            compio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
        bail!("get cluster_id failed: code={} {}", resp.code, resp.message);
    }
}

/// F214-C: probe a data dir for prior `format` state. The dedicated
/// `cluster_id` + `disk_uuid` sentinel files (added in F214-C) are the
/// canonical "already formatted" signal. Returns `(cluster_id, disk_uuid)`
/// when both files are present and readable; `None` for a fresh dir.
fn read_existing_format(dir: &str) -> Result<Option<(String, String)>> {
    let cluster_path = format!("{dir}/cluster_id");
    if !std::path::Path::new(&cluster_path).exists() {
        return Ok(None);
    }
    let cid = std::fs::read_to_string(&cluster_path)
        .with_context(|| format!("read cluster_id in {dir}"))?
        .trim()
        .to_string();
    let uuid_path = format!("{dir}/disk_uuid");
    if !std::path::Path::new(&uuid_path).exists() {
        // Defensive: cluster_id present but disk_uuid missing means a
        // partially-formatted dir (interrupted previous run, or a
        // pre-F214 dir that's been hand-patched). Treat as fresh and
        // let the new format overwrite — the operator hit this path
        // intentionally by running format on this dir.
        return Ok(None);
    }
    let did = std::fs::read_to_string(&uuid_path)
        .with_context(|| format!("read disk_uuid in {dir}"))?
        .trim()
        .to_string();
    Ok(Some((cid, did)))
}

/// Atomically write a sentinel file: write a temp sibling then rename over the
/// target, so a concurrent / crash-interrupted reader never observes a torn
/// value. Used for the F-EN-DYNSHARD `node_uuid` identity sentinel, which is
/// read back verbatim to keep a node's `node_id` stable across re-formats — a
/// half-written value there would mint a duplicate identity.
fn write_sentinel_atomic(path: &str, content: &str) -> std::io::Result<()> {
    let tmp = format!("{path}.tmp");
    std::fs::write(&tmp, content)?;
    std::fs::rename(&tmp, path)
}

fn human_size(bytes: u64) -> String {
    if bytes >= 1 << 30 {
        format!("{:.1} GB", bytes as f64 / (1u64 << 30) as f64)
    } else if bytes >= 1 << 20 {
        format!("{:.1} MB", bytes as f64 / (1u64 << 20) as f64)
    } else if bytes >= 1 << 10 {
        format!("{:.1} KB", bytes as f64 / (1u64 << 10) as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn stream_total(s: &MgrStreamInfo, extent_map: &HashMap<u64, MgrExtentInfo>) -> u64 {
    s.extent_ids
        .iter()
        .filter_map(|eid| extent_map.get(eid))
        .map(|e| e.sealed_length)
        .sum()
}

// ---------------------------------------------------------------------------
// JSON view types
// ---------------------------------------------------------------------------

fn auto_state_str(b: u8) -> &'static str {
    match b {
        NODE_AUTO_STATE_ONLINE => "Online",
        NODE_AUTO_STATE_SUSPECTED => "Suspected",
        // F214-B: registered, never verified alive. Distinct from
        // Suspected — Suspended means "was alive, now flaky".
        NODE_AUTO_STATE_SUSPEND => "Suspend",
        _ => "Unknown",
    }
}

fn override_str(b: u8) -> &'static str {
    match b {
        NODE_OVERRIDE_NONE => "-",
        NODE_OVERRIDE_FENCED => "Fenced",
        NODE_OVERRIDE_MAINTENANCE => "Maintenance",
        _ => "Unknown",
    }
}

fn op_name(b: u8) -> &'static str {
    match b {
        AUDIT_OP_FENCE_NODE => "fence_node",
        AUDIT_OP_SET_NODE_MAINTENANCE => "maintenance",
        AUDIT_OP_CLEAR_NODE_OVERRIDE => "clear_override",
        AUDIT_OP_REMOVE_NODE => "remove_node",
        AUDIT_OP_FORCE_EC_CONVERT => "force_ec_convert",
        AUDIT_OP_FORCE_ABANDON_EC_MARKER => "force_abandon_ec_marker",
        _ => "unknown",
    }
}

#[derive(Serialize)]
struct JsonNode {
    node_id: u64,
    address: String,
    auto_state: String,
    last_heartbeat_secs_ago: u64,
    suspected_age_secs: u64,
    override_kind: String,
    override_reason: String,
    override_set_by: String,
    override_set_at: i64,
    override_expire_at: u64,
    // F-EN-DYNSHARD M1c
    node_uuid: String,
    shard_count: usize,
    shard_ports: Vec<u16>,
}

#[derive(Serialize)]
struct JsonAudit {
    op: String,
    node_id: u64,
    extent_id: u64,
    by: String,
    reason: String,
    result_code: u8,
    result_message: String,
    ts_ns: u64,
}

// ---------------------------------------------------------------------------
// F213 `info` JSON output types (migrated from autumn_client.rs)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct InfoDiskView {
    disk_id: u64,
    uuid: String,
    online: bool,
}

#[derive(Serialize)]
struct InfoNodeView {
    node_id: u64,
    address: String,
    disks: Vec<InfoDiskView>,
}

#[derive(Serialize)]
struct InfoExtentView {
    extent_id: u64,
    size: u64,
    open: bool,
    replicas: Vec<u64>,
    parity: Vec<u64>,
    refs: u64,
    /// vp_table_refs-removal note: the maintenance machinery is gone, so for
    /// extents managed under this build this is always 0. A non-zero value is a
    /// LEGACY extent carried over from a pre-removal build — retained by the
    /// upgrade-safety guard (`refs==0 && vp_table_refs>0` is NOT reclaimed) and
    /// a Stage-2 migration target. Removed from the schema in Stage 2.
    vp_table_refs: u64,
    eversion: u64,
    /// How many live streams list this extent in `extent_ids`. 0 = orphan
    /// (not in any stream): either retained-by-vp_table_refs legacy live data,
    /// or a both-zero reclaimable extent (the EXTENT10-AUTORECLAIM sweep reaps
    /// it). `refs` should equal this; a gap is a refcount leak (MERGE-REFS-LEAK).
    in_streams: usize,
}

#[derive(Serialize)]
struct InfoStreamView {
    stream_id: u64,
    replicates: u32,
    ec_data: u32,
    ec_parity: u32,
    extent_ids: Vec<u64>,
    total_size: u64,
}

#[derive(Serialize)]
struct InfoDiscardEntry {
    extent_id: u64,
    bytes: i64,
}

#[derive(Serialize)]
struct InfoPartitionView {
    part_id: u64,
    ps_addr: String,
    range_start: String,
    range_end: String,
    live_size: u64,
    total_extents: usize,
    log_stream_id: u64,
    row_stream_id: u64,
    meta_stream_id: u64,
    discards: Vec<InfoDiscardEntry>,
}

#[derive(Serialize)]
struct InfoSnapshot {
    nodes: Vec<InfoNodeView>,
    extents: Vec<InfoExtentView>,
    streams: Vec<InfoStreamView>,
    partitions: Vec<InfoPartitionView>,
}

// ---------------------------------------------------------------------------
// run()
// ---------------------------------------------------------------------------

async fn run(args: Args) -> Result<()> {
    // F214-C: the register-node migration stub must print BEFORE we
    // try to connect to the manager — otherwise users hit "manager
    // unreachable" instead of the actionable migration message.
    if matches!(args.cmd, Command::RegisterNode) {
        eprintln!(
            "register-node has merged into 'autumn-op format'.\n\
             Run: autumn-op --manager <ADDR> format <DIR> [<DIR>...]\n\
             'format' fetches the cluster_id, registers the node IDENTITY \
             (F-EN-DYNSHARD M1c — no location), and stamps every data dir. \
             Start `autumn-extent-node --manager <ADDR> --data <DIR>... \
             --advertise <HOST:PORT>` to self-register the live location."
        );
        std::process::exit(1);
    }
    // F-AUTHZ-1: gen-signing-key is LOCAL (no manager) — handle it BEFORE
    // connecting so key generation works offline (and never needs a cluster).
    if let Command::GenSigningKey { kid } = &args.cmd {
        return cmd_gen_signing_key(*kid);
    }
    // Select the process-global transport before connecting. Without this an
    // autumn-op invoked against a UCX manager would default to TCP and hang.
    let _ = autumn_transport::init_with(args.transport);
    let client = ClusterClient::connect_raw(&args.manager).await?;
    match args.cmd {
        // ---------------- F211 read ----------------
        Command::ClusterVersion => cmd_cluster_version(&client, args.json).await?,
        Command::UpgradeVersion { to } => cmd_upgrade_version(&client, args.json, to).await?,
        Command::ListNodes => cmd_list_nodes(&client, args.json).await?,
        Command::Df => cmd_df(&client, args.json).await?,
        Command::ExtentHealth { node_filter, include_healthy } => cmd_extent_health(&client, args.json, node_filter, include_healthy).await?,
        Command::ListEcMarkers => cmd_list_ec_markers(&client, args.json).await?,
        Command::RecoveryStats => cmd_recovery_stats(&client, args.json).await?,
        Command::AuditLog { op, node_id, since, until, limit } => cmd_audit_log(&client, args.json, op, node_id, since, until, limit).await?,
        // ---------------- F211 admin ----------------
        Command::Fence { node_id, reason, by, force } => cmd_fence(&client, args.json, node_id, reason, by, force).await?,
        Command::Maintenance { node_id, reason, by, expire } => cmd_maintenance(&client, args.json, node_id, reason, by, expire).await?,
        Command::Unfence { node_id, by } => cmd_unfence(&client, args.json, node_id, by).await?,
        Command::Remove { node_id, by } => cmd_remove(&client, args.json, node_id, by).await?,
        // ---------------- F213 read ----------------
        Command::PolicyCandidates => cmd_policy_candidates(&client, args.json).await?,
        Command::AutoPolicy { action, name, arm } => {
            cmd_auto_policy(&client, args.json, &action, &name, arm).await?
        }
        Command::Info { part, detail, full } => {
            run_info(&client, args.json, part, detail, full).await?;
        }
        // ---------------- F213 admin ----------------
        Command::Bootstrap {
            replication,
            presplit,
            log_ec,
            row_ec,
        } => {
            run_bootstrap(&client, args.json, &replication, &presplit, log_ec, row_ec).await?;
        }
        Command::SetStreamEc { stream_id, ec_data, ec_parity } => cmd_set_stream_ec(&client, args.json, stream_id, ec_data, ec_parity).await?,
        Command::ForceEcConvert { extent_id } => cmd_force_ec_convert(&client, args.json, extent_id).await?,
        Command::Split { part_id, point } => cmd_split(&client, args.json, part_id, point).await?,
        Command::Presplit { namespace, tenant, rule } => {
            cmd_presplit(&client, args.json, &namespace, &tenant, &rule).await?
        }
        Command::Merge { survivor_part_id, victim_part_id } => cmd_merge(&client, args.json, survivor_part_id, victim_part_id).await?,
        Command::Rebalance { max_moves } => cmd_rebalance(&client, args.json, max_moves).await?,
        Command::Compact { part_id } => cmd_compact(&client, args.json, part_id).await?,
        Command::Gc { part_id, ratio, max_size, stream_debt, empty_only } => cmd_gc(&client, args.json, part_id, ratio, max_size, stream_debt, empty_only).await?,
        Command::ForceGc { part_id, extent_ids } => cmd_force_gc(&client, args.json, part_id, extent_ids).await?,
        Command::RegisterNode => {
            // Already handled by the pre-connect stub above.
            unreachable!("Command::RegisterNode handled before connect");
        }
        Command::Format { dirs } => cmd_format(&client, args.json, dirs, &args.manager).await?,
        // ---------------- F-AUTHZ-1 tooling ----------------
        Command::GenSigningKey { .. } => unreachable!("gen-signing-key handled before connect"),
        Command::PrincipalCreate { principal, grants, admin_token } => cmd_principal_create(&client, args.json, principal, grants, admin_token).await?,
        Command::PrincipalDelete { principal, admin_token } => cmd_principal_delete(&client, args.json, principal, admin_token).await?,
        Command::MintToken { principal, credential } => cmd_mint_token(&client, args.json, principal, credential).await?,
        // ---------------- F-KEY-NS D2 namespace registry ----------------
        Command::NamespaceCreate { name, owner_tenant, presplit, admin_token } => cmd_namespace_create(&client, args.json, name, owner_tenant, presplit, admin_token).await?,
        Command::NamespaceDelete { name, force, admin_token } => cmd_namespace_delete(&client, args.json, name, force, admin_token).await?,
        Command::NamespaceList => cmd_namespace_list(&client, args.json).await?,
        Command::PrincipalList => cmd_principal_list(&client, args.json).await?,
    }
    let _ = std::io::stdout().flush();
    Ok(())
}

/// Lowercase hex encode.
fn hex_encode(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

/// Decode an ASCII hex string into bytes.
fn hex_decode(s: &str) -> Result<Vec<u8>> {
    if !s.is_ascii() || !s.len().is_multiple_of(2) {
        bail!("expected an even-length ASCII hex string");
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| anyhow!("bad hex: {e}")))
        .collect()
}

/// F-AUTHZ-1: generate an Ed25519 signing key locally. Prints the keyfile line
/// (`<kid> <hex-seed>`) to STDOUT (redirect into `--auth-signing-key-file`); the
/// derived public key + guidance go to STDERR so they don't pollute the file.
fn cmd_gen_signing_key(kid: u32) -> Result<()> {
    use rand::RngCore;
    let mut seed = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut seed);
    let public = autumn_rpc::cap_token::public_key_from_seed(&seed);
    println!("{} {}", kid, hex_encode(&seed));
    eprintln!("# F-AUTHZ-1 signing key kid={kid}");
    eprintln!("# public_key = {}", hex_encode(&public));
    eprintln!("# The stdout line (kid + hex seed) is the SECRET keyfile line — give");
    eprintln!("# it to the manager via --auth-signing-key-file and keep it protected.");
    let _ = std::io::stdout().flush();
    Ok(())
}

/// F-NS-PRINCIPAL-UNIFIED: create/rotate a principal account. Returns the
/// permanent credential in the `principal:`/`credential:` two-line form, so
/// `autumn-op principal-create ... > loader.cred` produces a ready credential file
/// (the SDK's `read_credential_file` parses that form, carrying the name).
async fn cmd_principal_create(
    client: &ClusterClient,
    json: bool,
    principal: String,
    grants: Vec<String>,
    admin_token: String,
) -> Result<()> {
    let allowed_prefixes: Vec<Vec<u8>> = grants.iter().map(|p| p.as_bytes().to_vec()).collect();
    // Leader-only RPC with manager rotation on CODE_NOT_LEADER lives in the SDK.
    let credential = client
        .principal_create(&principal, allowed_prefixes, &admin_token)
        .await?;
    let cred = hex_encode(&credential);
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "principal": principal,
                "credential": cred,
            }))?
        );
    } else {
        // Two-line credential-file form (principal name travels with the secret).
        println!("principal: {principal}");
        println!("credential: {cred}");
        eprintln!("# ^ save both lines as the principal's credential file; shown ONCE (manager stores only the SHA-256 hash).");
    }
    Ok(())
}

/// F-NS-PRINCIPAL-UNIFIED: remove a principal account (stops renewal; current
/// token still works until it expires).
async fn cmd_principal_delete(
    client: &ClusterClient,
    json: bool,
    principal: String,
    admin_token: String,
) -> Result<()> {
    client.principal_delete(&principal, &admin_token).await?;
    if json {
        println!(
            "{}",
            serde_json::json!({ "principal": principal, "deleted": true })
        );
    } else {
        println!("principal '{principal}' deleted");
    }
    Ok(())
}

/// F-NS-PRINCIPAL-UNIFIED: mint a short-TTL capability token from a principal
/// credential.
async fn cmd_mint_token(
    client: &ClusterClient,
    json: bool,
    principal: String,
    credential_hex: String,
) -> Result<()> {
    let credential = hex_decode(&credential_hex).context("--credential")?;
    let (token_bytes, exp) = client.mint_token(&principal, credential).await?;
    let token = hex_encode(&token_bytes);
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "principal": principal,
                "token": token,
                "exp": exp,
            }))?
        );
    } else {
        println!("{token}");
        eprintln!("# token for '{principal}', exp={exp} (unix seconds)");
    }
    Ok(())
}

/// F-KEY-NS D2: register a namespace. `--with-tenant` sets the owner (protected);
/// `--presplit` freezes D8 split points (stored, not acted upon in SD-1).
async fn cmd_namespace_create(
    client: &ClusterClient,
    json: bool,
    name: String,
    owner_tenant: Option<String>,
    presplit: Vec<Vec<u8>>,
    admin_token: String,
) -> Result<()> {
    client
        .namespace_create(&name, owner_tenant.clone(), presplit.clone(), &admin_token)
        .await?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "namespace": name,
                "owner_tenant": owner_tenant,
                "presplit_points": presplit.len(),
            }))?
        );
    } else {
        println!("namespace '{name}' created");
        match &owner_tenant {
            Some(t) => println!("  owner tenant: {t} (protected)"),
            None => println!("  owner tenant: none (registered, not protected)"),
        }
        if !presplit.is_empty() {
            println!("  presplit points: {} (stored; applied by D8)", presplit.len());
        }
    }
    Ok(())
}

/// F-KEY-NS D2: delete a namespace registry row. The non-empty guard is enforced
/// HERE (client-side): scan the `<name>/` prefix and refuse unless `--force`, since
/// the manager has no KV data-plane client. `--force` only skips the guard — it
/// does NOT batch-delete data (deploy is cluster-reset, §7.4; batch-delete tooling
/// is a later delivery).
async fn cmd_namespace_delete(
    client: &ClusterClient,
    json: bool,
    name: String,
    force: bool,
    admin_token: String,
) -> Result<()> {
    let prefix = format!("{name}/").into_bytes();
    if !force {
        // One key is enough to prove the namespace is non-empty.
        let scan = client
            .range(&prefix, &prefix, 1)
            .await
            .with_context(|| format!("scanning namespace '{name}/' for emptiness"))?;
        if !scan.entries.is_empty() {
            bail!(
                "namespace '{name}' is not empty (holds data under '{name}/'); \
                 re-run with --force to remove the registry row anyway \
                 (this does NOT delete the data)"
            );
        }
    }
    client.namespace_delete(&name, &admin_token).await?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "namespace": name,
                "deleted": true,
                "forced": force,
            }))?
        );
    } else {
        println!("namespace '{name}' deleted (registry row removed)");
        if force {
            eprintln!("# --force: emptiness check skipped; any data under '{name}/' is untouched.");
        }
    }
    Ok(())
}

/// F-KEY-NS D2: list the registered namespaces (rich rows). Read-only.
async fn cmd_namespace_list(client: &ClusterClient, json: bool) -> Result<()> {
    let namespaces = client.namespace_list().await?;
    if json {
        let rows: Vec<_> = namespaces
            .iter()
            .map(|n| {
                serde_json::json!({
                    "name": n.name,
                    "prefix": String::from_utf8_lossy(&n.prefix),
                    "owner_tenant": n.owner_tenant,
                    "protected": n.owner_tenant.is_some(),
                    "presplit_points": n.presplit.len(),
                    "created_at": n.created_at,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&rows)?);
    } else {
        println!(
            "{:<14} {:<16} {:<14} {:>8} {:>12}",
            "NAME", "PREFIX", "OWNER", "PRESPLIT", "CREATED_AT"
        );
        for n in &namespaces {
            let owner = match &n.owner_tenant {
                Some(t) => format!("{t} (protected)"),
                None => "-".to_string(),
            };
            println!(
                "{:<14} {:<16} {:<14} {:>8} {:>12}",
                n.name,
                String::from_utf8_lossy(&n.prefix),
                owner,
                n.presplit.len(),
                n.created_at,
            );
        }
    }
    Ok(())
}

/// F-NS-PRINCIPAL-LIST: `principal-list [--json]`. Read-only inspection —
/// answers "who exists and what may they touch", which until now needed either
/// `ls $DATA_ROOT/authz/*.cred` (only what turnkey wrote) or an etcd key scan
/// (names only; grants live inside an rkyv value). Never prints credentials.
async fn cmd_principal_list(client: &ClusterClient, json: bool) -> Result<()> {
    let principals = client.principal_list().await?;
    let grants_of = |p: &autumn_rpc::manager_rpc::PrincipalRow| -> Vec<String> {
        p.grants
            .iter()
            .map(|g| String::from_utf8_lossy(g).into_owned())
            .collect()
    };
    if json {
        let rows: Vec<_> = principals
            .iter()
            .map(|p| serde_json::json!({ "name": p.name, "grants": grants_of(p) }))
            .collect();
        println!("{}", serde_json::to_string_pretty(&rows)?);
    } else {
        println!("{:<20} {}", "NAME", "GRANTS");
        for p in &principals {
            let g = grants_of(p);
            println!(
                "{:<20} {}",
                p.name,
                if g.is_empty() { "-".to_string() } else { g.join(",") }
            );
        }
    }
    Ok(())
}

async fn cmd_cluster_version(client: &ClusterClient, json: bool) -> Result<()> {
                let bytes = client
                    .mgr_call(
                        MSG_GET_CLUSTER_VERSION,
                        rkyv_encode(&GetClusterVersionReq {}),
                    )
                    .await?;
                let resp: GetClusterVersionResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
                if resp.code != CODE_OK {
                    bail!("cluster-version: {}", resp.message);
                }
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "cluster_version": resp.cluster_version,
                            "manager_wire_version_min": resp.wire_version_min,
                            "manager_wire_version_max": resp.wire_version_max,
                            "op_wire_version_min": autumn_rpc::WIRE_VERSION_MIN,
                            "op_wire_version_max": autumn_rpc::WIRE_VERSION_MAX,
                        }))?
                    );
                } else {
                    println!("cluster_version: {}", resp.cluster_version);
                    println!(
                        "manager wire interval: [{}, {}]",
                        resp.wire_version_min, resp.wire_version_max
                    );
                    println!(
                        "this autumn-op binary:  [{}, {}]",
                        autumn_rpc::WIRE_VERSION_MIN,
                        autumn_rpc::WIRE_VERSION_MAX
                    );
                    if resp.cluster_version < resp.wire_version_max {
                        println!(
                            "NOTE: manager binaries support up to v{} — `upgrade-version` can bump \
    once EVERY member runs the new binary",
                            resp.wire_version_max
                        );
                    }
                }
    Ok(())
}

async fn cmd_list_nodes(client: &ClusterClient, json: bool) -> Result<()> {
    let bytes = client
        .mgr_call(MSG_LIST_NODE_STATES, rkyv_encode(&ListNodeStatesReq {}))
        .await?;
    let resp: ListNodeStatesResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
    if resp.code != CODE_OK {
        bail!("list-nodes: {}", resp.message);
    }
    if json {
        let out: Vec<JsonNode> = resp
            .nodes
            .into_iter()
            .map(|n| JsonNode {
                node_id: n.node_id,
                address: n.address,
                auto_state: auto_state_str(n.auto_state).to_string(),
                last_heartbeat_secs_ago: n.last_heartbeat_secs_ago,
                suspected_age_secs: n.suspected_age_secs,
                override_kind: override_str(n.override_kind).to_string(),
                override_reason: n.override_reason,
                override_set_by: n.override_set_by,
                override_set_at: n.override_set_at,
                override_expire_at: n.override_expire_at,
                node_uuid: n.node_uuid,
                shard_count: n.shard_ports.len(),
                shard_ports: n.shard_ports,
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!(
            "{:<6} {:<10} {:<24} {:<7} {:<10} {:<8} {:<8} {:<12} REASON",
            "ID", "UUID", "ADDRESS", "SHARDS", "AUTO", "HB_AGO", "SUSP_AGE", "OVERRIDE"
        );
        for n in resp.nodes {
            let hb = if n.last_heartbeat_secs_ago == u64::MAX {
                "never".to_string()
            } else {
                format!("{}s", n.last_heartbeat_secs_ago)
            };
            // F-EN-DYNSHARD M1c: short uuid + shard count (verify a reshard took).
            let uuid_short = if n.node_uuid.is_empty() {
                "-".to_string()
            } else {
                n.node_uuid.chars().take(8).collect::<String>()
            };
            println!(
                "{:<6} {:<10} {:<24} {:<7} {:<10} {:<8} {:<8} {:<12} {}",
                n.node_id,
                uuid_short,
                n.address,
                n.shard_ports.len(),
                auto_state_str(n.auto_state),
                hb,
                n.suspected_age_secs,
                override_str(n.override_kind),
                n.override_reason,
            );
        }
    }
    Ok(())
}

async fn cmd_df(client: &ClusterClient, json: bool) -> Result<()> {
    let r = client.cluster_df().await?;
    let raw_used = r.raw_total.saturating_sub(r.raw_free);
    // Logical FOOTPRINT = sealed data + open-tail committed bytes (one copy).
    // physical_used counts open-tail bytes (largely live VP/log data), so amp
    // MUST divide by the footprint, not sealed-only — else an all-open-tail
    // (VP/log-heavy) cluster shows a ~15× inflated amp (F-DF-OPENTAIL).
    let logical_footprint = r.logical_stored.saturating_add(r.logical_open_tail);
    // Empirical replication/EC amplification (physical_used / logical
    // footprint): the REAL current cold/hot mix. ~3× for 3-replica, lower with
    // EC. n/a when nothing is stored yet.
    let amp = if logical_footprint > 0 {
        r.physical_used as f64 / logical_footprint as f64
    } else {
        0.0
    };
    // Writable logical estimate is a RANGE under EC: best EC shape is
    // K = min(4, node_count-1) data shards + 1 parity → factor
    // (K+1)/K; worst is 3-replica. Point estimate uses the empirical
    // amplification (falls back to the conservative /3).
    let k = if r.node_count >= 2 {
        (r.node_count - 1).min(4)
    } else {
        0
    };
    let best_factor = if k >= 1 {
        (k as f64 + 1.0) / k as f64
    } else {
        3.0
    };
    let writable_low = r.raw_free / 3; // conservative: 3-replica
    let writable_high = (r.raw_free as f64 / best_factor) as u64;
    let writable_est = if amp > 0.0 {
        (r.raw_free as f64 / amp) as u64
    } else {
        writable_low
    };
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let snap_age = now_ms.saturating_sub(r.last_update_ms) / 1000;
    let logical_age = now_ms.saturating_sub(r.logical_last_update_ms) / 1000;
    // F-DF-WALDEBT: dead (reclaimable) fraction of the footprint = sealed
    // gc_debt + open-tail dead bytes. Pre-F-DF-WALDEBT a log-heavy /
    // all-open-tail partition's open-tail debt was invisible (df only had
    // sealed gc_debt). physical_used carries these bytes until GC punches them.
    let wal_debt_ratio = if logical_footprint > 0 {
        r.logical_wal_debt as f64 / logical_footprint as f64
    } else {
        0.0
    };

    if json {
        let per_node: Vec<serde_json::Value> = r
            .per_node
            .iter()
            .map(|n| {
                serde_json::json!({
                    "node_id": n.node_id,
                    "total": n.total,
                    "free": n.free,
                    "extent_bytes": n.extent_bytes,
                    "online": n.online,
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "raw_total": r.raw_total,
                "raw_used": raw_used,
                "raw_free": r.raw_free,
                "physical_used": r.physical_used,
                "logical_stored_sealed": r.logical_stored,
                "logical_open_tail": r.logical_open_tail,
                "logical_footprint": logical_footprint,
                "logical_wal_debt": r.logical_wal_debt,
                "wal_debt_ratio": wal_debt_ratio,
                "amplification": amp,
                "writable_est": writable_est,
                "writable_low_3x": writable_low,
                "writable_high_ec": writable_high,
                "best_ec_data_shards": k,
                "node_count_online": r.node_count,
                "snapshot_age_secs": snap_age,
                "logical_scan_age_secs": logical_age,
                "per_node": per_node,
            }))?
        );
    } else {
        let amp_str = if amp > 0.0 {
            format!("{amp:.2}x")
        } else {
            "n/a".to_string()
        };
        println!("=== Cluster df ===");
        println!(
            "RAW:     total={:<10} used={:<10} avail={}",
            human_size(r.raw_total),
            human_size(raw_used),
            human_size(r.raw_free),
        );
        println!(
            "AUTUMN:  phys_used={:<10} footprint={:<10} amplification={}",
            human_size(r.physical_used),
            human_size(logical_footprint),
            amp_str,
        );
        println!(
            "         logical: sealed={} + open_tail={} = footprint {}",
            human_size(r.logical_stored),
            human_size(r.logical_open_tail),
            human_size(logical_footprint),
        );
        println!(
            "         WAL debt: {} dead ({:.1}% of footprint, GC-reclaimable; incl. open-tail)",
            human_size(r.logical_wal_debt),
            wal_debt_ratio * 100.0,
        );
        println!(
            "WRITABLE(est): {}   range [{} .. {}]  (3-replica .. EC {}+1)",
            human_size(writable_est),
            human_size(writable_low),
            human_size(writable_high),
            k,
        );
        println!(
            "NODES: {} online   (snapshot {}s ago; logical scan {}s ago)",
            r.node_count, snap_age, logical_age,
        );
        println!(
            "  {:<6} {:<10} {:<10} {:<10} ONLINE",
            "ID", "TOTAL", "FREE", "PHYS_USED"
        );
        for n in &r.per_node {
            println!(
                "  {:<6} {:<10} {:<10} {:<10} {}",
                n.node_id,
                human_size(n.total),
                human_size(n.free),
                human_size(n.extent_bytes),
                if n.online { "yes" } else { "no" },
            );
        }
    }
    Ok(())
}

async fn cmd_extent_health(client: &ClusterClient, json: bool, node_filter: Vec<u64>, include_healthy: bool) -> Result<()> {
    let req = ExtentHealthReq {
        node_id_filter: node_filter,
        include_healthy,
    };
    let bytes = client
        .mgr_call(MSG_EXTENT_HEALTH_REPORT, rkyv_encode(&req))
        .await?;
    let resp: ExtentHealthResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
    if resp.code != CODE_OK {
        bail!("extent-health: {}", resp.message);
    }
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(
                &resp
                    .extents
                    .into_iter()
                    .map(|e| {
                        serde_json::json!({
                            "extent_id": e.extent_id,
                            "eversion": e.eversion,
                            "sealed_length": e.sealed_length,
                            "ec_converted": e.ec_converted,
                            "unhealthy": e.unhealthy,
                            "slots": e.slots.into_iter().map(|s| serde_json::json!({
                                "slot": s.slot_index,
                                "node_id": s.node_id,
                                "avali": s.avali,
                                "auto_state": auto_state_str(s.auto_state),
                                "override": override_str(s.override_kind),
                            })).collect::<Vec<_>>(),
                        })
                    })
                    .collect::<Vec<_>>()
            )?
        );
    } else {
        if resp.extents.is_empty() {
            println!("(no extents match filter)");
        }
        for e in resp.extents {
            println!(
                "extent {}  eversion={} sealed={} ec={} unhealthy={}",
                e.extent_id, e.eversion, e.sealed_length, e.ec_converted, e.unhealthy
            );
            for s in e.slots {
                println!(
                    "  slot[{}] node={:<4} avali={:<5} auto={} override={}",
                    s.slot_index,
                    s.node_id,
                    s.avali,
                    auto_state_str(s.auto_state),
                    override_str(s.override_kind),
                );
            }
        }
    }
    Ok(())
}

async fn cmd_list_ec_markers(client: &ClusterClient, json: bool) -> Result<()> {
    let bytes = client
        .mgr_call(
            MSG_LIST_EC_INFLIGHT_MARKERS,
            rkyv_encode(&ListEcInflightMarkersReq {}),
        )
        .await?;
    let resp: ListEcInflightMarkersResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
    if resp.code != CODE_OK {
        bail!("list-ec-markers: {}", resp.message);
    }
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(
                &resp
                    .markers
                    .into_iter()
                    .map(|m| {
                        serde_json::json!({
                            "extent_id": m.extent_id,
                            "coord_node_id": m.coord_node_id,
                            "coord_auto_state": auto_state_str(m.coord_auto_state),
                            "coord_override": override_str(m.coord_override_kind),
                            "target_nodes": m.target_nodes,
                            "data_shards": m.data_shards,
                            "new_eversion": m.new_eversion,
                            "started_at": m.started_at,
                            "age_secs": m.age_secs,
                        })
                    })
                    .collect::<Vec<_>>()
            )?
        );
    } else {
        if resp.markers.is_empty() {
            println!("(no inflight EC markers)");
        }
        for m in resp.markers {
            println!(
                "ext={} coord={} ({}/{}) targets={:?} K={} new_ev={} age={}s",
                m.extent_id,
                m.coord_node_id,
                auto_state_str(m.coord_auto_state),
                override_str(m.coord_override_kind),
                m.target_nodes,
                m.data_shards,
                m.new_eversion,
                m.age_secs,
            );
        }
    }
    Ok(())
}

async fn cmd_recovery_stats(client: &ClusterClient, json: bool) -> Result<()> {
    let bytes = client
        .mgr_call(MSG_RECOVERY_STATS, rkyv_encode(&RecoveryStatsReq {}))
        .await?;
    let resp: RecoveryStatsResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
    if resp.code != CODE_OK {
        bail!("recovery-stats: {}", resp.message);
    }
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "global_inflight": resp.global_inflight,
                "max_global": resp.max_global,
                "max_per_source": resp.max_per_source,
                "max_per_target": resp.max_per_target,
                "per_source": resp.per_source,
                "per_target": resp.per_target,
                "backoff_entries": resp.backoff_entries,
                "backoff": resp.backoff.iter().map(|b| serde_json::json!({
                    "extent_id": b.extent_id,
                    "slot": b.slot,
                    "consecutive_failures": b.consecutive_failures,
                    "last_attempt_at": b.last_attempt_at,
                    "next_retry_at": b.next_retry_at,
                    "reason": b.reason,
                })).collect::<Vec<_>>(),
            }))?
        );
    } else {
        println!(
            "global: {}/{}  per_source<={}  per_target<={}  backoff_entries={}",
            resp.global_inflight,
            resp.max_global,
            resp.max_per_source,
            resp.max_per_target,
            resp.backoff_entries,
        );
        if !resp.per_source.is_empty() {
            println!("per-source:");
            for (id, c) in resp.per_source {
                println!("  node {:<4} {}", id, c);
            }
        }
        if !resp.per_target.is_empty() {
            println!("per-target:");
            for (id, c) in resp.per_target {
                println!("  node {:<4} {}", id, c);
            }
        }
        if !resp.backoff.is_empty() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            println!("backoff:");
            println!(
                "  {:<10} {:<4} {:<6} {:<10} reason",
                "extent", "slot", "fails", "retry_in"
            );
            for b in &resp.backoff {
                let retry_in = b.next_retry_at - now;
                let retry_str = if retry_in <= 0 {
                    "now".to_string()
                } else {
                    format!("{retry_in}s")
                };
                println!(
                    "  {:<10} {:<4} {:<6} {:<10} {}",
                    b.extent_id, b.slot, b.consecutive_failures, retry_str, b.reason
                );
            }
        }
    }
    Ok(())
}

async fn cmd_audit_log(client: &ClusterClient, json: bool, op: u8, node_id: u64, since: i64, until: i64, limit: u32) -> Result<()> {
    let req = QueryAuditLogReq {
        op_filter: op,
        node_id_filter: node_id,
        since_ts_s: since,
        until_ts_s: until,
        limit,
    };
    let bytes = client
        .mgr_call(MSG_QUERY_AUDIT_LOG, rkyv_encode(&req))
        .await?;
    let resp: QueryAuditLogResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
    if resp.code != CODE_OK {
        bail!("audit-log: {}", resp.message);
    }
    if json {
        let out: Vec<JsonAudit> = resp
            .entries
            .into_iter()
            .map(|e| JsonAudit {
                op: op_name(e.op).to_string(),
                node_id: e.node_id,
                extent_id: e.extent_id,
                by: e.by,
                reason: e.reason,
                result_code: e.result_code,
                result_message: e.result_message,
                ts_ns: e.ts_ns,
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        if resp.entries.is_empty() {
            println!("(no audit entries match filter)");
        }
        for e in resp.entries {
            println!(
                "{}  op={:<22} node={:<4} ext={:<8} by={:<12} code={} reason={}",
                e.ts_ns,
                op_name(e.op),
                e.node_id,
                e.extent_id,
                e.by,
                e.result_code,
                e.reason,
            );
            if !e.result_message.is_empty() {
                println!("    => {}", e.result_message);
            }
        }
    }
    Ok(())
}

/// F-DASH-IN-MGR M2: headless control of the in-manager auto-policy controller.
///   auto-policy status
///   auto-policy activate <name> [--arm]   (select policy; --arm = actuate)
///   auto-policy deactivate                (mode → Off)
async fn cmd_auto_policy(
    client: &ClusterClient,
    json: bool,
    action: &str,
    name: &str,
    arm: bool,
) -> Result<()> {
    match action {
        "status" => {}
        "activate" => {
            if name.is_empty() {
                anyhow::bail!("auto-policy activate requires a policy name");
            }
            client
                .auto_policy_set(AutoPolicySetReq {
                    op: AUTOPOLICY_OP_SET_ACTIVE,
                    mode: 0,
                    name: name.to_string(),
                    entry: None,
                })
                .await?;
            // ALWAYS set the mode explicitly so `activate` never silently
            // inherits a prior Armed/Off (coco P2): --arm → Armed (actuates),
            // else DryRun (runs + logs "would: …" but never actuates).
            let mode = if arm { 2 } else { 1 };
            client
                .auto_policy_set(AutoPolicySetReq {
                    op: AUTOPOLICY_OP_SET_MODE,
                    mode,
                    name: String::new(),
                    entry: None,
                })
                .await?;
        }
        "deactivate" => {
            client
                .auto_policy_set(AutoPolicySetReq {
                    op: AUTOPOLICY_OP_SET_MODE,
                    mode: 0, // Off
                    name: String::new(),
                    entry: None,
                })
                .await?;
        }
        other => anyhow::bail!("unknown auto-policy action '{other}' (status|activate|deactivate)"),
    }

    // Always print the resulting state.
    let st = client
        .auto_policy_get()
        .await
        .map_err(|e| anyhow!("auto-policy status: {e}"))?;
    let mode_str = match st.mode {
        2 => "armed",
        1 => "dry_run",
        _ => "off",
    };
    if json {
        let policies: Vec<_> = st
            .policies
            .iter()
            .map(|p| {
                serde_json::json!({
                    "name": p.name,
                    "desc": p.desc,
                    "builtin": p.builtin,
                    "switches": p.switches,
                    "interval_sec": p.interval_sec,
                    "cooldown_sec": p.cooldown_sec,
                    "max_actions": p.max_actions,
                })
            })
            .collect();
        let log: Vec<_> = st
            .log
            .iter()
            .map(|l| serde_json::json!({ "ts": l.ts, "level": l.level, "msg": l.msg }))
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "mode": mode_str,
                "active": st.active,
                "allow_mutations": st.allow_mutations,
                "policies": policies,
                "log": log,
            }))?
        );
    } else {
        println!(
            "auto-policy: mode={mode_str}  active={}  allow_mutations={}",
            if st.active.is_empty() { "(none)" } else { &st.active },
            st.allow_mutations
        );
        println!("  policies:");
        for p in &st.policies {
            println!(
                "    {:<14} {} — {}",
                p.name,
                if p.builtin { "preset" } else { "custom" },
                p.desc
            );
        }
        if !st.log.is_empty() {
            println!("  recent actions:");
            for l in st.log.iter().take(10) {
                println!("    [{}] {}", l.level, l.msg);
            }
        }
    }
    Ok(())
}

async fn cmd_policy_candidates(client: &ClusterClient, json: bool) -> Result<()> {
    let cands = client
        .policy_candidates()
        .await
        .map_err(|e| anyhow!("policy_candidates: {e}"))?;
    if json {
        let out: Vec<_> = cands
            .iter()
            .map(|c| {
                let kind = match c.kind {
                    POLICY_KIND_SPLIT => "split",
                    POLICY_KIND_MERGE => "merge",
                    POLICY_KIND_GC => "gc",
                    POLICY_KIND_MAJOR_COMPACT => "major",
                    POLICY_KIND_HOT_COLD => "hotcold",
                    POLICY_KIND_MINOR_COMPACT => "minor",
                    POLICY_KIND_EC => "ec",
                    POLICY_KIND_REBALANCE => "rebalance",
                    _ => "?",
                };
                serde_json::json!({
                    "kind": kind,
                    "primary_part_id": c.primary_part_id,
                    "secondary_part_id": c.secondary_part_id,
                    "reason": c.reason,
                    "size_bytes": c.size_bytes,
                    "req_per_sec": c.req_per_sec,
                    "imm_full_per_sec": c.imm_full_per_sec,
                    "same_ps": c.same_ps,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else if cands.is_empty() {
        println!("(no candidates)");
    } else {
        println!(
            "{:<7} {:<10} {:<10} {:<46} {:<10} {:<8} {:<6} {:<5}",
            "KIND", "PRIMARY", "SECONDARY", "REASON", "SIZE", "QPS", "IMM/s", "FEAS"
        );
        for c in cands {
            let kind = match c.kind {
                POLICY_KIND_SPLIT => "split",
                POLICY_KIND_MERGE => "merge",
                POLICY_KIND_GC => "gc",
                POLICY_KIND_MAJOR_COMPACT => "major",
                POLICY_KIND_HOT_COLD => "hotcold",
                POLICY_KIND_MINOR_COMPACT => "minor",
                POLICY_KIND_EC => "ec",
                POLICY_KIND_REBALANCE => "rebalance",
                _ => "?",
            };
            let feas = match c.kind {
                POLICY_KIND_GC
                | POLICY_KIND_MAJOR_COMPACT
                | POLICY_KIND_MINOR_COMPACT
                | POLICY_KIND_EC
                | POLICY_KIND_HOT_COLD
                | POLICY_KIND_REBALANCE => "n/a", // rebalance is cluster-scoped, no same-PS feasibility
                _ if c.same_ps => "yes",
                _ => "no",
            };
            let secondary = if c.secondary_part_id == 0 {
                "-".to_string()
            } else {
                c.secondary_part_id.to_string()
            };
            println!(
                "{:<7} {:<10} {:<10} {:<46} {:<10} {:<8} {:<6} {:<5}",
                kind,
                c.primary_part_id,
                secondary,
                c.reason,
                format!("{} MB", c.size_bytes / (1024 * 1024)),
                c.req_per_sec,
                c.imm_full_per_sec,
                feas,
            );
        }
    }
    Ok(())
}

async fn cmd_upgrade_version(client: &ClusterClient, json: bool, to: Option<u32>) -> Result<()> {
                // Resolve the default target (current+1) from a fresh read so
                // the printed intent matches what the manager will validate.
                let bytes = client
                    .mgr_call(
                        MSG_GET_CLUSTER_VERSION,
                        rkyv_encode(&GetClusterVersionReq {}),
                    )
                    .await?;
                let cur: GetClusterVersionResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
                if cur.code != CODE_OK {
                    bail!("upgrade-version: read current failed: {}", cur.message);
                }
                let target = to.unwrap_or(cur.cluster_version + 1);
                let bytes = client
                    .mgr_call(
                        MSG_BUMP_CLUSTER_VERSION,
                        rkyv_encode(&BumpClusterVersionReq { to: target }),
                    )
                    .await?;
                let resp: BumpClusterVersionResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
                if resp.code != CODE_OK {
                    bail!(
                        "upgrade-version to {} REFUSED (cluster_version stays {}): {}",
                        target,
                        resp.cluster_version,
                        resp.message
                    );
                }
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "cluster_version": resp.cluster_version,
                        }))?
                    );
                } else {
                    println!(
                        "cluster_version bumped: {} -> {} — rollback to older binaries is no \
    longer safe (new formats may now be emitted/persisted)",
                        cur.cluster_version, resp.cluster_version
                    );
                }
    Ok(())
}

async fn cmd_fence(client: &ClusterClient, json: bool, node_id: u64, reason: String, by: String, force: bool) -> Result<()> {
    if reason.is_empty() || by.is_empty() {
        bail!("--reason and --by are required");
    }
    let req = FenceNodeReq {
        node_id,
        reason,
        set_by: by,
        force,
    };
    let bytes = client.mgr_call(MSG_FENCE_NODE, rkyv_encode(&req)).await?;
    let resp: CodeResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
    print_code(json, "fence-node", &resp);
    Ok(())
}

async fn cmd_maintenance(client: &ClusterClient, json: bool, node_id: u64, reason: String, by: String, expire: u64) -> Result<()> {
    if by.is_empty() {
        bail!("--by is required");
    }
    let req = SetNodeMaintenanceReq {
        node_id,
        reason,
        set_by: by,
        expire_at: expire,
    };
    let bytes = client
        .mgr_call(MSG_SET_NODE_MAINTENANCE, rkyv_encode(&req))
        .await?;
    let resp: CodeResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
    print_code(json, "maintenance", &resp);
    Ok(())
}

async fn cmd_unfence(client: &ClusterClient, json: bool, node_id: u64, by: String) -> Result<()> {
    if by.is_empty() {
        bail!("--by is required");
    }
    let req = ClearNodeOverrideReq {
        node_id,
        set_by: by,
    };
    let bytes = client
        .mgr_call(MSG_CLEAR_NODE_OVERRIDE, rkyv_encode(&req))
        .await?;
    let resp: CodeResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
    print_code(json, "unfence", &resp);
    Ok(())
}

async fn cmd_remove(client: &ClusterClient, json: bool, node_id: u64, by: String) -> Result<()> {
    if by.is_empty() {
        bail!("--by is required");
    }
    let req = RemoveNodeReq {
        node_id,
        set_by: by,
    };
    let bytes = client.mgr_call(MSG_REMOVE_NODE, rkyv_encode(&req)).await?;
    let resp: RemoveNodeResp = rkyv_decode(&bytes).map_err(|e| anyhow!(e))?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "code": resp.code,
                "message": resp.message,
                "blocking_extent_ids": resp.blocking_extent_ids,
                "blocking_marker_extent_ids": resp.blocking_marker_extent_ids,
            }))?
        );
    } else if resp.code == CODE_OK {
        println!("remove: ok");
    } else {
        println!("remove: code={} {}", resp.code, resp.message);
        if !resp.blocking_extent_ids.is_empty() {
            println!("  blocking extents: {:?}", resp.blocking_extent_ids);
        }
        if !resp.blocking_marker_extent_ids.is_empty() {
            println!("  blocking markers: {:?}", resp.blocking_marker_extent_ids);
        }
    }
    if resp.code != CODE_OK {
        std::process::exit(2);
    }
    Ok(())
}

async fn cmd_set_stream_ec(client: &ClusterClient, json: bool, stream_id: u64, ec_data: u32, ec_parity: u32) -> Result<()> {
    let req_bytes = rkyv_encode(&UpdateStreamEcReq {
        stream_id,
        ec_data_shard: ec_data,
        ec_parity_shard: ec_parity,
    });
    let mut attempt = 0u32;
    loop {
        let resp_bytes = client
            .mgr_call(MSG_UPDATE_STREAM_EC, req_bytes.clone())
            .await
            .context("update stream EC")?;
        let resp: UpdateStreamEcResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
        if resp.code == CODE_OK {
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "code": resp.code,
                        "stream_id": stream_id,
                        "ec_data": ec_data,
                        "ec_parity": ec_parity,
                    }))?
                );
            } else {
                println!(
                    "stream {} EC updated to {}+{}; conversion will run on next manager tick (~5s)",
                    stream_id, ec_data, ec_parity
                );
            }
            break;
        }
        if resp.code == CODE_NOT_LEADER && attempt < 60 {
            attempt += 1;
            compio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
        bail!("set-stream-ec failed: code={} {}", resp.code, resp.message);
    }
    Ok(())
}

async fn cmd_force_ec_convert(client: &ClusterClient, json: bool, extent_id: u64) -> Result<()> {
    let req = rkyv_encode(&ForceEcConvertReq { extent_id });
    let resp_bytes = client
        .mgr_call(MSG_FORCE_EC_CONVERT, req)
        .await
        .context("force-ec-convert")?;
    let resp: ForceEcConvertResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
    if resp.code != CODE_OK {
        bail!("force-ec-convert: code={} {}", resp.code, resp.message);
    }
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "code": resp.code,
                "extent_id": extent_id,
                "message": resp.message,
            }))?
        );
    } else {
        println!("{}", resp.message);
    }
    Ok(())
}

async fn cmd_split(
    client: &ClusterClient,
    json: bool,
    part_id: u64,
    point: SplitPoint,
) -> Result<()> {
    // Lower the CLI intent (ns/tenant/suffix or raw) to the raw wire key.
    let at_key = point.resolve_at_key();

    // Friendly CLI precheck (UX only — the PS is the authoritative validator).
    // When an explicit point is given, verify it lands strictly inside the
    // target partition's [start, end). This turns the PS's hex-only rejection
    // into a message that names the namespace/tenant/suffix the operator typed.
    if let Some(key) = &at_key {
        if let Ok(parts) = client.all_partitions_with_range().await {
            if let Some((_, _, start, end)) = parts.iter().find(|(pid, _, _, _)| *pid == part_id) {
                let below_start = key.as_slice() <= start.as_slice();
                let at_or_above_end = !end.is_empty() && key.as_slice() >= end.as_slice();
                if below_start || at_or_above_end {
                    bail!(
                        "split point {} resolves to key 0x{} which is not strictly inside \
                         partition {}'s range [0x{}, 0x{}); pick a namespace/tenant/suffix \
                         that falls inside this partition",
                        point.describe(),
                        hex_encode(key),
                        part_id,
                        hex_encode(start),
                        hex_encode(end),
                    );
                }
            }
            // Partition not found in the map → skip precheck; the PS call below
            // surfaces the authoritative NotFound.
        }
        // GetRegions failed → skip precheck; let the authoritative PS call run.
    }

    client
        .split_at(part_id, at_key.clone())
        .await
        .map_err(|e| anyhow!("split: {e}"))?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "ok": true,
                "part_id": part_id,
                "split_point": point.describe(),
                // Echo the resolved raw wire key (hex); null = PS-median.
                "at_key_hex": at_key.as_ref().map(|k| hex_encode(k)),
            }))?
        );
    } else if at_key.is_some() {
        println!("split ok (at {})", point.describe());
    } else {
        println!("split ok");
    }
    Ok(())
}

/// F-PRESPLIT-NS-RULES: presplit a `{tenant}/{namespace}/` keyspace by its
/// natural dimension (fs=ino, kvc=content-hash, mem=agent). Computes the cut
/// points, then splits the owning partition at each in ASCENDING order —
/// re-resolving the owner each time, since a prior split creates the child that
/// owns the next point.
async fn cmd_presplit(
    client: &ClusterClient,
    json: bool,
    namespace: &str,
    tenant: &str,
    rule: &PresplitRule,
) -> Result<()> {
    let suffixes = presplit_suffixes(rule)?;
    // F-NS-PRINCIPAL-UNIFIED: cut prefix = `{namespace}/` (+ `{tenant}/` as an
    // in-namespace sub-segment for mem/kvc; empty for fs), matching the binding.
    let prefix = if tenant.is_empty() {
        format!("{namespace}/").into_bytes()
    } else {
        format!("{namespace}/{tenant}/").into_bytes()
    };
    let points: Vec<Vec<u8>> = suffixes
        .into_iter()
        .map(|s| {
            let mut k = prefix.clone();
            k.extend_from_slice(&s);
            k
        })
        .collect();
    if points.is_empty() {
        bail!("presplit: rule produced no cut points (count < 2 / empty list) — nothing to do");
    }

    // F-FS-GEOM-DECLARED: `--lanes N` DECLARES the fs stripe geometry, in the
    // same command that cuts the boundaries — the declaration and the placement
    // can never be created out of step. Writers read this key; they no longer
    // reverse-engineer the lane count from partition boundaries.
    //
    // Declared BEFORE cutting, deliberately. The two failure orders are not
    // symmetric: declare-then-fail-to-cut leaves geometry claiming N lanes whose
    // boundaries don't exist yet, so new files stripe across lanes that all land
    // in one partition — CORRECT (files are self-describing), just no parallelism
    // yet, and it heals completely when the cuts land on a re-run. Cut-then-
    // fail-to-declare leaves boundaries with no declaration, i.e. silently
    // unstriped files forever — the exact failure class this feature removes.
    if let PresplitRule::FsLanes { lanes } = rule {
        let layout = autumn_fuse::schema::StripeLayout {
            lanes: *lanes,
            unit_bytes: autumn_fuse::schema::MAX_EXTENT as u32,
        };
        // `client` here is connect_raw (no binding), so the wire key carries the
        // `fs/` namespace prefix explicitly — same bytes a scoped fs client
        // produces from the relative `[0x04]stripe_geom`.
        let mut k = b"fs/".to_vec();
        k.extend_from_slice(&autumn_fuse::key::stripe_geom_key());
        client
            .put(&k, &autumn_fuse::schema::encode_stripe_geom(&layout))
            .await
            .map_err(|e| anyhow!("presplit: declare fs stripe geometry: {e}"))?;
        if !json {
            println!("declared fs stripe geometry: {lanes} lanes × {} MiB units", layout.unit_bytes / (1 << 20));
        }
    }

    let mut applied = 0usize;
    let mut skipped: Vec<String> = Vec::new();
    for point in &points {
        // Re-fetch the region map each iteration — a prior split created a new
        // child that may own this point. `all_partitions_with_range` reads the
        // client's REGION CACHE (it only auto-refreshes when empty), so force a
        // refresh first: without it the 2nd+ cut of a multi-point presplit
        // resolves against the PRE-split map and tries to split the wrong (now
        // narrowed) partition → "at or above partition end" (hit live on
        // `presplit --namespace fs --lanes N` where all cuts land in one small
        // range so every later cut needs the fresh child).
        // coco P2: do NOT swallow the refresh error — a failed refresh falls back
        // to the STALE cache and mis-resolves the owner, and since the run reports
        // ok when applied>0 the operator could load into a partially-presplit
        // keyspace. Fail the whole presplit instead.
        client
            .refresh_regions()
            .await
            .map_err(|e| anyhow!("presplit: refresh regions before cut: {e}"))?;
        let parts = client
            .all_partitions_with_range()
            .await
            .map_err(|e| anyhow!("presplit: list partitions: {e}"))?;
        let owner = parts.iter().find(|(_, _, start, end)| {
            point.as_slice() >= start.as_slice()
                && (end.is_empty() || point.as_slice() < end.as_slice())
        });
        let Some((pid, _, _, _)) = owner else {
            skipped.push(format!("0x{} (no owning partition)", hex_encode(point)));
            continue;
        };
        match client.split_at(*pid, Some(point.clone())).await {
            Ok(_) => applied += 1,
            // A data-bearing partition can't be split repeatedly until major
            // compaction clears the CoW out-of-range keys (has_overlap). The PS
            // auto-major-compacts, so a later re-run succeeds — surface the hint
            // rather than failing the whole run.
            Err(e) => skipped.push(format!("part {pid} at 0x{}: {e}", hex_encode(point))),
        }
    }

    // `ok` (and the exit code below) track whether we ACCOMPLISHED anything, not
    // whether every point applied. A partial run (some has_overlap skips on a live
    // data-bearing cluster) is expected progress → success; but `applied == 0`
    // means the run split nothing (wrong --hash-prefix so no owner found, authz
    // denial, stale map, …) and MUST fail loudly — else automation reads exit 0
    // and starts loading into an un-presplit keyspace.
    let ok = applied > 0;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "ok": ok,
                "namespace": namespace,
                "tenant": tenant,
                "points": points.len(),
                "applied": applied,
                "skipped": skipped,
            }))?
        );
    } else {
        println!("presplit {tenant}/{namespace}: {applied}/{} cut points applied", points.len());
        for s in &skipped {
            println!("  skipped: {s}");
        }
        if !skipped.is_empty() {
            println!(
                "note: a data-bearing partition can't be split repeatedly until major \
                 compaction clears CoW out-of-range keys (has_overlap). Re-run `presplit` \
                 after compaction, or presplit the EMPTY keyspace BEFORE loading data."
            );
        }
    }
    if !ok {
        bail!(
            "presplit {tenant}/{namespace}: 0/{} cut points applied — nothing was split. \
             Check --hash-prefix (a wrong prefix owns no partition), authz, and the partition map.",
            points.len()
        );
    }
    Ok(())
}

async fn cmd_merge(client: &ClusterClient, json: bool, survivor_part_id: u64, victim_part_id: u64) -> Result<()> {
    eprintln!(
        "F183: stop writes to partitions {survivor_part_id} and {victim_part_id} \
         before continuing. The CLI will FLUSH both, then issue the manager merge. \
         The survivor's PS picks up the wider range on the next region_sync (~2 s)."
    );
    client
        .merge_partitions(survivor_part_id, victim_part_id)
        .await
        .map_err(|e| anyhow!("merge: {e}"))?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "ok": true,
                "survivor": survivor_part_id,
                "victim": victim_part_id,
            }))?
        );
    } else {
        println!("merge ok: partition {victim_part_id} merged into {survivor_part_id}");
    }
    Ok(())
}

async fn cmd_rebalance(client: &ClusterClient, json: bool, max_moves: u32) -> Result<()> {
    let moves = client
        .rebalance_regions(max_moves)
        .await
        .map_err(|e| anyhow!("rebalance: {e}"))?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "ok": true,
                "moved": moves.len(),
                "moves": moves
                    .iter()
                    .map(|m| serde_json::json!({
                        "part_id": m.part_id,
                        "from_ps": m.from_ps,
                        "to_ps": m.to_ps,
                    }))
                    .collect::<Vec<_>>(),
            }))?
        );
    } else if moves.is_empty() {
        println!("rebalance: already balanced (0 moves)");
    } else {
        println!("rebalance: moved {} partition(s):", moves.len());
        for m in &moves {
            println!("  part {} : ps {} -> ps {}", m.part_id, m.from_ps, m.to_ps);
        }
    }
    Ok(())
}

async fn cmd_compact(client: &ClusterClient, json: bool, part_id: u64) -> Result<()> {
    client
        .compact(part_id)
        .await
        .map_err(|e| anyhow!("compact: {e}"))?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "ok": true,
                "part_id": part_id,
            }))?
        );
    } else {
        println!("compact triggered for partition {part_id}");
    }
    Ok(())
}

async fn cmd_gc(client: &ClusterClient, json: bool, part_id: u64, ratio: Option<f64>, max_size: Option<u64>, stream_debt: Option<u64>, empty_only: bool) -> Result<()> {
    let params = autumn_client::GcAutoParams {
        ratio,
        max_size,
        stream_debt,
        empty_only,
    };
    client
        .gc_with_params(part_id, params.clone())
        .await
        .map_err(|e| anyhow!("gc: {e}"))?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "ok": true,
                "part_id": part_id,
                "ratio": params.ratio,
                "max_size": params.max_size,
                "stream_debt": params.stream_debt,
                "empty_only": params.empty_only,
            }))?
        );
    } else {
        println!(
            "gc triggered for partition {part_id} (ratio={:?} max_size={:?} stream_debt={:?} empty_only={})",
            params.ratio, params.max_size, params.stream_debt, params.empty_only
        );
    }
    Ok(())
}

async fn cmd_force_gc(client: &ClusterClient, json: bool, part_id: u64, extent_ids: Vec<u64>) -> Result<()> {
    let advisory = client
        .force_gc(part_id, extent_ids.clone())
        .await
        .map_err(|e| anyhow!("forcegc: {e}"))?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "ok": true,
                "part_id": part_id,
                "extents": extent_ids,
                "advisory": advisory,
            }))?
        );
    } else {
        println!("forcegc triggered for partition {part_id}, extents={extent_ids:?}");
        if !advisory.is_empty() {
            println!("  advisory: {advisory}");
        }
    }
    Ok(())
}

/// F-EN-DYNSHARD M1c: `format` mints/reuses IDENTITY only (`node_uuid` +
/// per-dir `disk_uuid`) and registers with an EMPTY location — no
/// `addr`/`shard_ports`/`control_address`. The EN binary's own `--advertise`
/// self-registers the live address + shard ports at every startup (M1a/M1b),
/// so format never needs `--listen`/`--advertise`/`--shard-ports` again.
/// Mirrors Kafka `kafka-storage.sh format` (writes only clusterId/nodeId to
/// meta.properties, zero network info) and Ceph `ceph-volume prepare` (writes
/// only the OSD fsid) — mkfs mints identity offline; the daemon reports its
/// own network location every boot, not the formatting step.
async fn cmd_format(client: &ClusterClient, json: bool, dirs: Vec<String>, manager: &str) -> Result<()> {
    // F214-C: fetch the manager's cluster_id BEFORE touching
    // any disk. Failure here means the manager is not yet
    // leader (retries internally) or has never bootstrapped
    // (fatal — operator must start the manager first).
    let cluster_id = fetch_cluster_id(client).await?;

    // For each dir, decide whether to fresh-format or reuse
    // existing. Refuse on cluster_id mismatch — that's the
    // "wrong cluster" diagnostic.
    let mut disk_uuids = Vec::new();
    let mut freshly_formatted: Vec<bool> = Vec::with_capacity(dirs.len());
    for dir in &dirs {
        std::fs::create_dir_all(dir).with_context(|| format!("create dir {dir}"))?;
        match read_existing_format(dir)? {
            Some((existing_cid, existing_did)) if existing_cid == cluster_id => {
                // Idempotent path — already formatted for this
                // cluster. Reuse the disk_uuid so the manager's
                // re-register branch returns the existing
                // disk_id without allocating a fresh one.
                if !json {
                    println!(
                        "{dir}: already formatted (cluster_id matches), reusing disk_uuid={existing_did}"
                    );
                }
                disk_uuids.push(existing_did);
                freshly_formatted.push(false);
            }
            Some((existing_cid, _)) => {
                // Different cluster — refuse rather than risk
                // joining a disk to the wrong cluster.
                bail!(
                    "{dir} is already formatted for cluster {existing_cid}, \
                     but the manager at {} reports cluster {}. \
                     Wipe the dir or point at the original cluster.",
                    manager,
                    cluster_id
                );
            }
            None => {
                // Fresh dir — full format: 256 hash subdirs +
                // fresh disk_uuid.
                let uuid = format_disk(dir)?;
                if !json {
                    println!("formatted {dir}: disk_uuid={uuid}");
                }
                disk_uuids.push(uuid);
                freshly_formatted.push(true);
            }
        }
    }

    // F-EN-DYNSHARD M0: resolve the node's stable identity (one node_uuid per
    // NODE, not per disk) BEFORE registering. Reuse an existing sentinel so a
    // re-format keeps the same node_id; mint a fresh v4 only when NO dir carries
    // one. The read is FAIL-LOUD: a NotFound means "to mint", but a
    // permission/IO/corrupt read must NOT be swallowed as "absent" — that would
    // mint a NEW identity and create a duplicate node at the same address on the
    // next register. Disagreeing dirs (a cloned / mixed data set) are refused,
    // never silently unified.
    let mut seen_uuids: Vec<String> = Vec::new();
    for dir in &dirs {
        let path = format!("{dir}/node_uuid");
        match std::fs::read_to_string(&path) {
            Ok(s) => {
                let s = s.trim().to_string();
                if !s.is_empty() && !seen_uuids.contains(&s) {
                    seen_uuids.push(s);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(anyhow!(e)).with_context(|| {
                    format!(
                        "read node_uuid sentinel in {dir}; refusing to mint a new \
                         identity over an unreadable one"
                    )
                });
            }
        }
    }
    if seen_uuids.len() > 1 {
        bail!(
            "conflicting node_uuid across --data dirs ({seen_uuids:?}); a mixed or \
             cloned data set must not be formatted together"
        );
    }
    let node_uuid = seen_uuids
        .into_iter()
        .next()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    // Persist the identity to EVERY dir (atomic tmp+rename) BEFORE registering.
    // A crash between a successful register and the sentinel write would
    // otherwise lose it → the next format mints a fresh uuid → a duplicate node.
    // Writing it first is safe even if registration then fails: the value is
    // reused verbatim on the retry (idempotent identity).
    for dir in &dirs {
        write_sentinel_atomic(&format!("{dir}/node_uuid"), &node_uuid)
            .with_context(|| format!("write node_uuid in {dir}"))?;
    }

    // F214-C + F-EN-DYNSHARD M1c: register against the manager, IDENTITY
    // ONLY. `addr`/`shard_ports`/`control_address` are all empty — an
    // identity-only registration is the M0 `handle_register_node`
    // "identity-only" branch: the manager allocates node_id + disk_ids
    // for a fresh node, or (re-format on an existing uuid) returns the
    // existing node_id + disk map WITHOUT touching any live location the
    // EN has already self-registered. A never-booted node therefore has
    // NO location, so df can't reach it — it stays Suspend (F214-B) and
    // is never selected for allocation until the EN actually starts and
    // self-registers (M1a/M1b).
    let resp_bytes = client
        .mgr_call(
            MSG_REGISTER_NODE,
            rkyv_encode(&RegisterNodeReq {
                addr: String::new(),
                disk_uuids: disk_uuids.clone(),
                shard_ports: vec![],
                control_address: String::new(),
                node_uuid: node_uuid.clone(),
            }),
        )
        .await
        .context("register node")?;
    let resp: RegisterNodeResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
    // A FAILED registration (follower / non-leader, fenced or
    // decommissioned address, same-addr-different-disk) returns
    // code != CODE_OK with node_id=0 and no/partial disk_uuids.
    // Writing the local format sentinels on that response leaves a
    // dir that LOOKS formatted (cluster_id present) but the manager
    // never accepted — the EN then starts on a node/disk the manager
    // doesn't know about.
    if resp.code != CODE_OK {
        bail!("register node failed: code={} {}", resp.code, resp.message);
    }

    let node_id = resp.node_id;
    // Resolve every requested disk_uuid -> disk_id BEFORE writing any
    // sentinel: a missing mapping (the manager returned only the
    // disks it actually accepted) must FAIL, not silently write
    // disk_id=0 (a pseudo-formatted dir the manager can't route to).
    // Two passes so a partial failure leaves NO half-written dirs.
    let uuid_to_id: HashMap<&str, u64> = resp
        .disk_uuids
        .iter()
        .map(|(u, id)| (u.as_str(), *id))
        .collect();
    let mut disk_assignments: Vec<(String, String, u64)> = Vec::new();
    for (dir, disk_uuid) in dirs.iter().zip(disk_uuids.iter()) {
        let disk_id = *uuid_to_id.get(disk_uuid.as_str()).ok_or_else(|| {
            anyhow!(
                "manager did not assign a disk_id for {dir} (uuid {disk_uuid}); \
                 not writing format sentinels"
            )
        })?;
        disk_assignments.push((dir.clone(), disk_uuid.clone(), disk_id));
    }
    for (dir, disk_uuid, disk_id) in &disk_assignments {
        // F214-C: cluster_id + disk_uuid sentinel files. The
        // extent-node binary's startup check reads cluster_id
        // and cross-checks against the manager; disk_uuid is
        // used by re-formats to preserve idempotency.
        std::fs::write(format!("{dir}/cluster_id"), &cluster_id)
            .with_context(|| format!("write cluster_id in {dir}"))?;
        std::fs::write(format!("{dir}/disk_uuid"), disk_uuid)
            .with_context(|| format!("write disk_uuid in {dir}"))?;
        std::fs::write(format!("{dir}/node_id"), node_id.to_string())
            .with_context(|| format!("write node_id in {dir}"))?;
        std::fs::write(format!("{dir}/disk_id"), disk_id.to_string())
            .with_context(|| format!("write disk_id in {dir}"))?;
        // F-EN-DYNSHARD M0: the node_uuid sentinel was already persisted (atomic
        // tmp+rename) to every dir BEFORE registration — nothing to write here.
    }

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "node_id": node_id,
                "node_uuid": node_uuid,
                "cluster_id": cluster_id,
                "disks": disk_assignments.iter()
                    .map(|(d, u, id)| serde_json::json!({
                        "dir": d, "uuid": u, "disk_id": id,
                    }))
                    .collect::<Vec<_>>(),
            }))?
        );
    } else {
        println!("node registered: node_id={node_id}, node_uuid={node_uuid}");
        println!("cluster_id={cluster_id}");
        for (dir, _u, disk_id) in &disk_assignments {
            println!("  {dir}: node_id={node_id}, disk_id={disk_id}");
        }
        // Don't claim "Format complete" when every dir was just REUSED — the
        // idempotent re-register path formats nothing. Report accurately.
        let n_fresh = freshly_formatted.iter().filter(|&&f| f).count();
        let n_reused = freshly_formatted.len() - n_fresh;
        if n_fresh == 0 {
            println!("\nReuse complete ({n_reused} dir(s) already formatted; nothing to format).");
        } else if n_reused == 0 {
            println!("\nFormat complete ({n_fresh} dir(s) formatted).");
        } else {
            println!("\nFormat complete ({n_fresh} formatted, {n_reused} reused).");
        }
        // F-EN-DYNSHARD M1c: format registered IDENTITY ONLY — the node has
        // NO location until the EN process itself self-registers via its own
        // `--advertise` (required whenever `--manager` is given).
        println!("Start the extent node with:");
        println!(
            "  autumn-extent-node --manager {} --data {} --advertise <HOST:PORT> [--cpuset <SPEC>]",
            manager,
            dirs.join(",")
        );
    }
    Ok(())
}

fn print_code(json: bool, op: &str, resp: &CodeResp) {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "code": resp.code,
                "message": resp.message,
            }))
            .unwrap()
        );
    } else if resp.code == CODE_OK {
        println!("{}: ok", op);
    } else {
        println!("{}: code={} {}", op, resp.code, resp.message);
    }
    if resp.code != CODE_OK {
        std::process::exit(2);
    }
}

// ---------------------------------------------------------------------------
// F213 bootstrap (migrated from autumn_client.rs)
// ---------------------------------------------------------------------------

async fn run_bootstrap(
    client: &ClusterClient,
    json_out: bool,
    replication: &str,
    presplit: &str,
    log_ec: Option<(u32, u32)>,
    row_ec: Option<(u32, u32)>,
) -> Result<()> {
    let meta_replicates = parse_replication(replication)?;

    // Per-stream (replicates, ec_data, ec_parity):
    //   - Replica streams: replicates=N, ec_data=N, ec_parity=0.
    //   - EC streams: replicates is the open-extent replica count (=
    //     meta_replicates), and (ec_data, ec_parity) describes the
    //     post-seal EC encoding (e.g. 4+1, 7+1). The two are independent.
    let log_params = log_ec.map(|(k, m)| (meta_replicates, k, m)).unwrap_or((
        meta_replicates,
        meta_replicates,
        0,
    ));
    let row_params = row_ec.map(|(k, m)| (meta_replicates, k, m)).unwrap_or((
        meta_replicates,
        meta_replicates,
        0,
    ));
    let meta_params = (meta_replicates, meta_replicates, 0u32);

    let ranges: Vec<(Vec<u8>, Vec<u8>)> = {
        let parts: Vec<&str> = presplit.splitn(2, ':').collect();
        let n: usize = parts[0].parse().unwrap_or(1);
        let kind = parts.get(1).copied().unwrap_or("normal");
        match kind {
            "hexstring" => hex_split_ranges(n),
            "fuse" => fuse_split_ranges(n),
            _ => vec![(vec![], vec![])],
        }
    };

    let create_stream_once =
        |label: &'static str, replicates: u32, ec_data: u32, ec_parity: u32| async move {
            let req_bytes = rkyv_encode(&CreateStreamReq {
                replicates,
                ec_data_shard: ec_data,
                ec_parity_shard: ec_parity,
            });
            let mut attempt = 0u32;
            loop {
                let resp_bytes = client
                    .mgr_call(MSG_CREATE_STREAM, req_bytes.clone())
                    .await
                    .with_context(|| format!("create {label} stream"))?;
                let resp: CreateStreamResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
                if resp.code == CODE_OK {
                    return Ok::<u64, anyhow::Error>(resp.stream.map(|s| s.stream_id).unwrap_or(0));
                }
                if resp.code == CODE_NOT_LEADER && attempt < 60 {
                    attempt += 1;
                    compio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
                bail!(
                    "create {label} stream failed: code={} {}",
                    resp.code,
                    resp.message
                );
            }
        };

    let mut created: Vec<serde_json::Value> = Vec::new();
    for (idx, (start_key, end_key)) in ranges.iter().enumerate() {
        let (log_repl, log_k, log_m) = log_params;
        let (row_repl, row_k, row_m) = row_params;
        let (meta_repl, meta_k, meta_m) = meta_params;
        let log_stream_id = create_stream_once("log", log_repl, log_k, log_m).await?;
        let row_stream_id = create_stream_once("row", row_repl, row_k, row_m).await?;
        let meta_stream_id = create_stream_once("meta", meta_repl, meta_k, meta_m).await?;

        let meta = MgrPartitionMeta {
            log_stream: log_stream_id,
            row_stream: row_stream_id,
            meta_stream: meta_stream_id,
            part_id: 0,
            rg: Some(MgrRange {
                start_key: start_key.clone(),
                end_key: end_key.clone(),
            }),
        };

        let req_bytes = rkyv_encode(&UpsertPartitionReq { meta });
        let mut attempt = 0u32;
        let resp = loop {
            let resp_bytes = client
                .mgr_call(MSG_UPSERT_PARTITION, req_bytes.clone())
                .await
                .context("upsert partition")?;
            let resp: UpsertPartitionResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
            if resp.code == CODE_OK {
                break resp;
            }
            if resp.code == CODE_NOT_LEADER && attempt < 60 {
                attempt += 1;
                compio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            bail!(
                "bootstrap partition {} failed: code={} {}",
                idx,
                resp.code,
                resp.message
            );
        };

        let start_s = if start_key.is_empty() {
            String::from("\"\"")
        } else {
            String::from_utf8_lossy(start_key).to_string()
        };
        let end_s = if end_key.is_empty() {
            String::from("\"\"")
        } else {
            String::from_utf8_lossy(end_key).to_string()
        };
        if json_out {
            created.push(serde_json::json!({
                "index": idx,
                "part_id": resp.part_id,
                "log_stream_id": log_stream_id,
                "row_stream_id": row_stream_id,
                "meta_stream_id": meta_stream_id,
                "log_ec": [log_k, log_m],
                "row_ec": [row_k, row_m],
                "meta_ec": [meta_k, meta_m],
                "range_start": start_s,
                "range_end": end_s,
            }));
        } else {
            println!(
                "partition {} created: id={} log={} ({}+{}) row={} ({}+{}) meta={} ({}+{}) range=[{}..{})",
                idx,
                resp.part_id,
                log_stream_id,
                log_k,
                log_m,
                row_stream_id,
                row_k,
                row_m,
                meta_stream_id,
                meta_k,
                meta_m,
                start_s,
                end_s,
            );
        }
    }
    if json_out {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "partitions": created,
            }))?
        );
    } else {
        println!("bootstrap succeeded: {} partition(s)", ranges.len());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// F213 info (migrated from autumn_client.rs)
// ---------------------------------------------------------------------------

/// Scalable compact cluster overview (the DEFAULT `info`). One
/// `MSG_GET_CLUSTER_OVERVIEW` RPC: per-partition rollup (range / ps /
/// live_size / extent count, range-sorted) + per-node extent-shard count —
/// NO per-extent array, NO per-PS discard probe. Bounded by partition + node
/// count, so it scales to 数千 partition / 数万 extent. Per-partition extents
/// are fetched lazily via `info --part P` (scoped).
async fn run_overview(client: &ClusterClient, json_out: bool) -> Result<()> {
    let bytes = client
        .mgr_call(MSG_GET_CLUSTER_OVERVIEW, rkyv_encode(&GetClusterOverviewReq {}))
        .await
        .context("cluster overview")?;
    let resp: GetClusterOverviewResp = rkyv_decode(&bytes).map_err(decode_err)?;
    if resp.code != CODE_OK {
        bail!("cluster overview: code={} {}", resp.code, resp.message);
    }
    let mut parts = resp.partitions;
    // Global key-range order: "" (−∞) first, then by raw start bytes.
    parts.sort_by(|a, b| {
        (!a.range_start.is_empty(), &a.range_start).cmp(&(!b.range_start.is_empty(), &b.range_start))
    });
    // PS-instance count comes from the manager (distinct ps_id). NOT distinct
    // ps_addr — under F099-K each partition has its OWN listener (base_port+ord),
    // so one PS process serving N partitions exposes N addresses.
    let ps_count = resp.ps_count;
    let key_s = |k: &[u8]| String::from_utf8_lossy(k).into_owned();

    if json_out {
        let pj: Vec<_> = parts
            .iter()
            .map(|p| {
                serde_json::json!({
                    "part_id": p.part_id,
                    "ps_id": p.ps_id,
                    "ps_addr": p.ps_addr,
                    "range_start": key_s(&p.range_start),
                    "range_end": key_s(&p.range_end),
                    "live_size": p.live_size,
                    "total_extents": p.total_extents,
                    "req_per_sec": p.req_per_sec,
                    "write_bytes_per_sec": p.write_bytes_per_sec,
                    "read_bytes_per_sec": p.read_bytes_per_sec,
                    "log_stream_id": p.log_stream,
                    "row_stream_id": p.row_stream,
                    "meta_stream_id": p.meta_stream,
                })
            })
            .collect();
        let nj: Vec<_> = resp
            .nodes
            .iter()
            .map(|n| serde_json::json!({
                "node_id": n.node_id, "address": n.address, "extent_count": n.extent_count,
            }))
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "part_count": parts.len(),
                "ps_count": ps_count,
                "total_req_per_sec": resp.total_req_per_sec,
                "total_write_bytes_per_sec": resp.total_write_bytes_per_sec,
                "total_read_bytes_per_sec": resp.total_read_bytes_per_sec,
                "partitions": pj,
                "nodes": nj,
            }))?
        );
    } else {
        println!("cluster overview: {} partitions across {} PS", parts.len(), ps_count);
        for p in &parts {
            println!(
                "  part {:>6}  ps {:<20}  {:>10}  {:>3} ext  [{}, {})",
                p.part_id,
                p.ps_addr,
                human_size(p.live_size),
                p.total_extents,
                key_s(&p.range_start),
                if p.range_end.is_empty() { "+∞".into() } else { key_s(&p.range_end) },
            );
        }
        println!("nodes:");
        for n in &resp.nodes {
            println!("  node {:>4} {:<20} {} extent shards", n.node_id, n.address, n.extent_count);
        }
    }
    Ok(())
}

/// Scoped per-partition view (`info --part P`, no `--detail`): one
/// `MSG_GET_REGIONS` (cheap) to resolve the partition's 3 streams + ps + range,
/// then a SCOPED `MSG_STREAM_INFO` (only those streams) so the extent list is
/// bounded by THIS partition — never a full-cluster extent pull. Emits the
/// partition's `extents` array (the dashboard's lazy drawer source).
async fn run_partition_info(client: &ClusterClient, json_out: bool, pid: u64) -> Result<()> {
    let regions_bytes = client.mgr_call(MSG_GET_REGIONS, Bytes::new()).await.context("get regions")?;
    let regions_resp: GetRegionsResp = rkyv_decode(&regions_bytes).map_err(decode_err)?;
    if regions_resp.code != CODE_OK {
        bail!("get regions: code={} {}", regions_resp.code, regions_resp.message);
    }
    let regions: std::collections::HashMap<u64, MgrRegionInfo> =
        regions_resp.regions.into_iter().collect();
    let part_addrs: std::collections::HashMap<u64, String> =
        regions_resp.part_addrs.into_iter().collect();
    let ps_details: std::collections::HashMap<u64, MgrPsDetail> =
        regions_resp.ps_details.into_iter().collect();
    let r = regions
        .get(&pid)
        .ok_or_else(|| anyhow!("partition {pid} not found"))?;

    let si_bytes = client
        .mgr_call(
            MSG_STREAM_INFO,
            rkyv_encode(&StreamInfoReq {
                stream_ids: vec![r.log_stream, r.row_stream, r.meta_stream],
            }),
        )
        .await
        .context("stream info (scoped)")?;
    let si: StreamInfoResp = rkyv_decode(&si_bytes).map_err(decode_err)?;
    if si.code != CODE_OK {
        bail!("stream info: code={} {}", si.code, si.message);
    }
    let extent_map: std::collections::HashMap<u64, MgrExtentInfo> = si.extents.into_iter().collect();
    let stream_map: std::collections::HashMap<u64, MgrStreamInfo> = si.streams.into_iter().collect();

    // Resolve EN node addresses (cheap, one RPC) so we can PROBE open extents
    // for their live length. An OPEN (unsealed) extent's manager `sealed_length`
    // is 0, so without a probe the tail log/row/meta extents render as 0B — which
    // is wrong (the log tail holds the live WAL). Sealed/EC extents need no probe.
    let node_map: std::collections::HashMap<u64, String> =
        match client.mgr_call(MSG_NODES_INFO, Bytes::new()).await {
            Ok(nb) => match rkyv_decode::<NodesInfoResp>(&nb) {
                Ok(nr) => nr.nodes.into_iter().map(|(id, n)| (id, n.address)).collect(),
                Err(_) => Default::default(),
            },
            Err(_) => Default::default(),
        };

    let ps_addr = part_addrs
        .get(&pid)
        .or_else(|| ps_details.get(&r.ps_id).map(|d| &d.address))
        .cloned()
        .unwrap_or_default();
    let key_s = |k: &[u8]| String::from_utf8_lossy(k).into_owned();
    let (rs, re) = r
        .rg
        .as_ref()
        .map(|g| (key_s(&g.start_key), key_s(&g.end_key)))
        .unwrap_or_default();

    let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut extents_json = Vec::new();
    let mut live_size = 0u64;
    for (role, sid) in [("log", r.log_stream), ("row", r.row_stream), ("meta", r.meta_stream)] {
        if let Some(st) = stream_map.get(&sid) {
            for eid in &st.extent_ids {
                if !seen.insert(*eid) {
                    continue;
                }
                if let Some(e) = extent_map.get(eid) {
                    // Sealed → authoritative sealed_length. Open → probe the EN
                    // for the live length (else it shows 0B, see node_map note).
                    let mut sz = e.sealed_length;
                    if !e.sealed {
                        if let Some(addr) = e.replicates.first().and_then(|nid| node_map.get(nid)) {
                            if let Ok(enc) = client.get_ps_client(addr).await {
                                let req = ExtProbeExtentReq { extent_id: *eid };
                                if let Ok(rb) = enc
                                    .call_timeout(EXT_MSG_PROBE_EXTENT, req.encode(), DEFAULT_RPC_TIMEOUT)
                                    .await
                                {
                                    if let Ok(pr) = ExtProbeExtentResp::decode(rb) {
                                        sz = pr.length;
                                    }
                                }
                            }
                        }
                    }
                    live_size = live_size.saturating_add(sz);
                    let replicas: Vec<u64> = e.replicates.iter().chain(e.parity.iter()).copied().collect();
                    extents_json.push(serde_json::json!({
                        "extent_id": *eid,
                        "role": role,
                        "size": sz,
                        "open": !e.sealed,
                        "ec": e.ec_converted,
                        "ec_shape": if e.ec_converted { Some(format!("{}+{}", e.replicates.len(), e.parity.len())) } else { None },
                        "replicas": replicas,
                        "refs": e.refs,
                        "eversion": e.eversion,
                    }));
                } else {
                    extents_json.push(serde_json::json!({
                        "extent_id": *eid, "role": role, "missing": true,
                    }));
                }
            }
        }
    }

    // F-GC-FLOOR-OBS #2: ask the PS for the GC replay floor + per-SST vp_heads,
    // so the operator can see WHY a `forcegc P E` on a given extent is protected
    // (extent E at/before the floor → correct, not a bug). Best-effort: a PS that
    // predates this RPC, or is briefly unreachable, just omits the section.
    let floor: Option<DiagPartitionVpResp> = if ps_addr.is_empty() {
        None
    } else {
        match client.get_ps_client(&ps_addr).await {
            Ok(psc) => match psc
                .call_timeout(
                    MSG_DIAG_PARTITION_VP,
                    rkyv_encode(&DiagPartitionVpReq { part_id: pid }),
                    DEFAULT_RPC_TIMEOUT,
                )
                .await
            {
                Ok(bytes) => rkyv_decode::<DiagPartitionVpResp>(&bytes)
                    .ok()
                    .filter(|d| d.code == CODE_OK),
                Err(_) => None,
            },
            Err(_) => None,
        }
    };

    if json_out {
        let floor_json = floor.as_ref().map(|f| {
            serde_json::json!({
                "floor_extent_id": f.floor_extent_id,
                "floor_pos": f.floor_pos,
                "vp_seed_extent_id": f.vp_seed_extent_id,
                "vp_seed_offset": f.vp_seed_offset,
                "log_extent_ids": f.log_extent_ids,
                "sst_vp_heads": f.sst_vp_heads
                    .iter()
                    .map(|(e, o)| serde_json::json!({"vp_extent_id": e, "vp_offset": o}))
                    .collect::<Vec<_>>(),
            })
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "part_id": pid,
                "ps_addr": ps_addr,
                "range_start": rs,
                "range_end": re,
                "live_size": live_size,
                "total_extents": seen.len(),
                "log_stream_id": r.log_stream,
                "row_stream_id": r.row_stream,
                "meta_stream_id": r.meta_stream,
                "replay_floor": floor_json,
                "extents": extents_json,
            }))?
        );
    } else {
        println!("partition {pid} on {ps_addr}  [{rs}, {re})  {} ({} ext)", human_size(live_size), seen.len());
        if let Some(f) = &floor {
            println!(
                "  replay_floor = extent {} (pos {})   vp_seed(tail) = extent {} @ {}",
                f.floor_extent_id, f.floor_pos, f.vp_seed_extent_id, f.vp_seed_offset
            );
            for (i, (eid, off)) in f.sst_vp_heads.iter().enumerate() {
                let pin = if *eid == f.floor_extent_id && f.floor_extent_id != 0 {
                    "  ← pins floor (forcegc at/before this extent is protected)"
                } else {
                    ""
                };
                println!("  sst[{i}] vp_head = extent {eid} @ {off}{pin}");
            }
        }
        for e in &extents_json {
            println!("  {}", e);
        }
    }
    Ok(())
}

async fn run_info(
    client: &ClusterClient,
    json_out: bool,
    part: Option<u64>,
    detail: bool,
    full: bool,
) -> Result<()> {
    // F203: `--detail` prints PartitionLoad snapshot for `part`.
    if detail {
        let pid = part.expect("--detail requires --part PID; checked at parse time");
        let req = rkyv_encode(&GetPartitionDetailReq { part_id: pid });
        let resp_bytes = client
            .mgr_call(MSG_GET_PARTITION_DETAIL, req)
            .await
            .context("get partition detail")?;
        let resp: GetPartitionDetailResp = rkyv_decode(&resp_bytes).map_err(decode_err)?;
        if resp.code != CODE_OK {
            bail!("get_partition_detail: code={} {}", resp.code, resp.message);
        }
        let l = &resp.load;
        if json_out {
            let v = serde_json::json!({
                "part_id": pid,
                "bucket_ts": resp.bucket_ts,
                "size_bytes": l.size_bytes,
                "req_per_sec": l.req_per_sec,
                "write_bytes_per_sec": l.write_bytes_per_sec,
                "read_bytes_per_sec": l.read_bytes_per_sec,
                "imm_full_per_sec": l.imm_full_per_sec,
                "p99_us": l.p99_us,
                "gc_debt_bytes": l.gc_debt_bytes,
                "pending_compaction_bytes": l.pending_compaction_bytes,
                "minor_compact_pending_bytes": l.minor_compact_pending_bytes,
                "sst_tombstone_bytes": l.sst_tombstone_bytes,
                "sst_expired_bytes": l.sst_expired_bytes,
                "sst_out_of_range_bytes": l.sst_out_of_range_bytes,
                "gc_inflight": l.gc_inflight,
                "compact_inflight": l.compact_inflight,
                "last_gc_at": l.last_gc_at,
                "last_compact_at": l.last_compact_at,
                "sealed_log_extent_count": l.sealed_log_extent_count,
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&v).context("serialize detail")?
            );
        } else {
            println!("=== Partition {pid} (bucket_ts={}) ===", resp.bucket_ts);
            println!("  size_bytes={}", l.size_bytes);
            println!(
                "  req_per_sec={}  imm_full_per_sec={}",
                l.req_per_sec, l.imm_full_per_sec
            );
            println!(
                "  gc_debt_bytes={} ({} MiB)",
                l.gc_debt_bytes,
                l.gc_debt_bytes / (1024 * 1024)
            );
            println!(
                "  pending_compaction_bytes={} ({} MiB)  [major]",
                l.pending_compaction_bytes,
                l.pending_compaction_bytes / (1024 * 1024)
            );
            println!(
                "  minor_compact_pending_bytes={} ({} MiB)",
                l.minor_compact_pending_bytes,
                l.minor_compact_pending_bytes / (1024 * 1024)
            );
            println!(
                "  sst_tombstone_bytes={}  sst_expired_bytes={}  sst_out_of_range_bytes={}",
                l.sst_tombstone_bytes, l.sst_expired_bytes, l.sst_out_of_range_bytes
            );
            println!(
                "  gc_inflight={}  compact_inflight={}",
                l.gc_inflight, l.compact_inflight
            );
            println!(
                "  last_gc_at={}  last_compact_at={}",
                l.last_gc_at, l.last_compact_at
            );
            println!("  sealed_log_extent_count={}", l.sealed_log_extent_count);
        }
        return Ok(());
    }

    // Big-cluster routing: default `info` = scalable compact overview;
    // `info --part P` = scoped single-partition view (+extents). Only the
    // explicit `--full` falls through to the legacy complete dump below.
    if !full {
        if let Some(pid) = part {
            return run_partition_info(client, json_out, pid).await;
        }
        return run_overview(client, json_out).await;
    }

    // === Fetch manager data (legacy full dump, `--full` only) ===
    let stream_resp_bytes = client
        .mgr_call(
            MSG_STREAM_INFO,
            rkyv_encode(&StreamInfoReq {
                stream_ids: Vec::new(),
            }),
        )
        .await
        .context("stream info")?;
    let stream_resp: StreamInfoResp = rkyv_decode(&stream_resp_bytes).map_err(decode_err)?;
    // Check the manager's response code BEFORE building the snapshot: a
    // follower / non-routable manager returns code != CODE_OK with EMPTY
    // collections, which the renderer would otherwise present as a real,
    // empty cluster (silent in `--json`, and the exit code stays 0).
    if stream_resp.code != CODE_OK {
        bail!("stream info: code={} {}", stream_resp.code, stream_resp.message);
    }

    let nodes_resp_bytes = client
        .mgr_call(MSG_NODES_INFO, Bytes::new())
        .await
        .context("nodes info")?;
    let nodes_resp: NodesInfoResp = rkyv_decode(&nodes_resp_bytes).map_err(decode_err)?;
    if nodes_resp.code != CODE_OK {
        bail!("nodes info: code={} {}", nodes_resp.code, nodes_resp.message);
    }

    let regions_resp_bytes = client
        .mgr_call(MSG_GET_REGIONS, Bytes::new())
        .await
        .context("get regions")?;
    let regions_resp: GetRegionsResp = rkyv_decode(&regions_resp_bytes).map_err(decode_err)?;
    if regions_resp.code != CODE_OK {
        bail!("get regions: code={} {}", regions_resp.code, regions_resp.message);
    }

    // === Build lookup maps ===
    let mut extent_map: HashMap<u64, MgrExtentInfo> = stream_resp.extents.into_iter().collect();
    let disk_map: HashMap<u64, MgrDiskInfo> = nodes_resp.disks_info.into_iter().collect();

    let mut nodes_sorted: Vec<(u64, MgrNodeInfo)> = nodes_resp.nodes.into_iter().collect();
    nodes_sorted.sort_by_key(|(id, _)| *id);
    let node_map: HashMap<u64, String> = nodes_sorted
        .iter()
        .map(|(id, n)| (*id, n.address.clone()))
        .collect();

    let mut streams_sorted: Vec<(u64, MgrStreamInfo)> = stream_resp.streams.into_iter().collect();
    streams_sorted.sort_by_key(|(id, _)| *id);
    let stream_map: HashMap<u64, MgrStreamInfo> = streams_sorted.iter().cloned().collect();

    let regions: HashMap<u64, MgrRegionInfo> = regions_resp.regions.into_iter().collect();
    let ps_details: HashMap<u64, MgrPsDetail> = regions_resp.ps_details.into_iter().collect();
    let part_addr_map: HashMap<u64, String> = regions_resp.part_addrs.into_iter().collect();

    let mut part_ids: Vec<u64> = regions.keys().copied().collect();
    part_ids.sort();

    // === Query commit_length for open extents ===
    let probe_set: HashSet<u64> = if let Some(pid) = part {
        regions
            .get(&pid)
            .into_iter()
            .flat_map(|r| [r.log_stream, r.row_stream, r.meta_stream])
            .flat_map(|sid| {
                stream_map
                    .get(&sid)
                    .into_iter()
                    .flat_map(|s| s.extent_ids.iter().copied())
            })
            .collect()
    } else {
        HashSet::new()
    };

    let mut open_extents: HashSet<u64> = HashSet::new();
    for (eid, ext) in extent_map.iter_mut() {
        // Authoritative seal STATE is the explicit `sealed` flag, NOT
        // `sealed_length == 0` (manager note 32): a failover can seal an
        // EMPTY tail (`sealed = true, sealed_length = 0`). Treating that as
        // open mislabels it `(open)` and pointlessly probes a sealed extent.
        if !ext.sealed {
            open_extents.insert(*eid);
            if part.is_some() && !probe_set.contains(eid) {
                continue;
            }
            if let Some(node_id) = ext.replicates.first() {
                if let Some(addr) = node_map.get(node_id) {
                    if let Ok(en_client) = client.get_ps_client(addr).await {
                        // F210-H3 Tier 2: probe RPC has no PS-owner context;
                        // must NOT use the fence-gated commit_length RPC.
                        let req = ExtProbeExtentReq { extent_id: *eid };
                        if let Ok(resp_bytes) = en_client
                            .call_timeout(EXT_MSG_PROBE_EXTENT, req.encode(), DEFAULT_RPC_TIMEOUT)
                            .await
                        {
                            if let Ok(resp) = ExtProbeExtentResp::decode(resp_bytes) {
                                ext.sealed_length = resp.length;
                            }
                        }
                    }
                }
            }
        }
    }

    // === Fetch pending discard snapshots from each PS ===
    let pids_to_query: Vec<u64> = if let Some(pid) = part {
        vec![pid]
    } else {
        part_ids.clone()
    };
    let mut part_discards: HashMap<u64, Vec<(u64, i64)>> = HashMap::new();
    for pid in &pids_to_query {
        let r = match regions.get(pid) {
            Some(r) => r,
            None => continue,
        };
        let ps_addr = part_addr_map
            .get(pid)
            .or_else(|| ps_details.get(&r.ps_id).map(|d| &d.address))
            .map(|s| s.as_str())
            .unwrap_or("");
        if ps_addr.is_empty() {
            continue;
        }
        let req_bytes = rkyv_encode(&GetDiscardsReq { part_id: *pid });
        match client.get_ps_client(ps_addr).await {
            Ok(ps_client) => match ps_client
                .call_timeout(MSG_GET_DISCARDS, req_bytes, DEFAULT_RPC_TIMEOUT)
                .await
            {
                Ok(resp_bytes) => match rkyv_decode::<GetDiscardsResp>(&resp_bytes) {
                    Ok(resp) if resp.code == autumn_rpc::partition_rpc::CODE_OK => {
                        part_discards.insert(*pid, resp.discards);
                    }
                    Ok(resp) => eprintln!("[warning] discard fetch part {pid}: {}", resp.message),
                    Err(e) => eprintln!("[warning] discard decode part {pid}: {e}"),
                },
                Err(e) => eprintln!("[warning] discard fetch failed for part {pid}: {e}"),
            },
            Err(e) => eprintln!("[warning] connect PS for part {pid}: {e}"),
        }
    }

    // Per-extent live stream-membership count (how many streams list it in
    // extent_ids, duplicates counted). 0 = orphan (not in any stream). `refs`
    // should equal this; a gap is a leaked refcount (MERGE-REFS-LEAK). Computed
    // before the json/text split so both renderings can flag orphans.
    let mut stream_member_count: HashMap<u64, usize> = HashMap::new();
    for (_sid, s) in &streams_sorted {
        for eid in &s.extent_ids {
            *stream_member_count.entry(*eid).or_insert(0) += 1;
        }
    }

    if json_out {
        let nodes_view: Vec<InfoNodeView> = nodes_sorted
            .iter()
            .map(|(nid, n)| InfoNodeView {
                node_id: *nid,
                address: n.address.clone(),
                disks: n
                    .disks
                    .iter()
                    .map(|did| InfoDiskView {
                        disk_id: *did,
                        uuid: disk_map
                            .get(did)
                            .map(|d| d.uuid.clone())
                            .unwrap_or_default(),
                        online: disk_map.get(did).map(|d| d.online).unwrap_or(false),
                    })
                    .collect(),
            })
            .collect();

        let extents_view: Vec<InfoExtentView> = {
            let mut v: Vec<_> = extent_map
                .iter()
                .map(|(eid, e)| InfoExtentView {
                    extent_id: *eid,
                    size: e.sealed_length,
                    open: open_extents.contains(eid),
                    replicas: e.replicates.clone(),
                    parity: e.parity.clone(),
                    refs: e.refs,
                    vp_table_refs: e.vp_table_refs,
                    eversion: e.eversion,
                    in_streams: stream_member_count.get(eid).copied().unwrap_or(0),
                })
                .collect();
            v.sort_by_key(|e| e.extent_id);
            v
        };

        let streams_view: Vec<InfoStreamView> = streams_sorted
            .iter()
            .map(|(sid, s)| {
                let r = if s.replicates > 0 {
                    s.replicates
                } else {
                    s.extent_ids
                        .iter()
                        .find_map(|eid| {
                            extent_map
                                .get(eid)
                                .filter(|e| !e.ec_converted)
                                .map(|e| e.replicates.len() as u32)
                        })
                        .unwrap_or(0)
                };
                InfoStreamView {
                    stream_id: *sid,
                    replicates: r,
                    ec_data: s.ec_data_shard,
                    ec_parity: s.ec_parity_shard,
                    extent_ids: s.extent_ids.clone(),
                    total_size: stream_total(s, &extent_map),
                }
            })
            .collect();

        let mut partitions_view: Vec<InfoPartitionView> = part_ids
            .iter()
            .filter_map(|pid| {
                let r = regions.get(pid)?;
                let rg = r.rg.as_ref()?;
                let ps_addr = part_addr_map
                    .get(pid)
                    .or_else(|| ps_details.get(&r.ps_id).map(|d| &d.address))
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                let mut live_size = 0u64;
                let mut total_extents = 0usize;
                for sid in [r.log_stream, r.row_stream, r.meta_stream] {
                    if let Some(s) = stream_map.get(&sid) {
                        live_size += stream_total(s, &extent_map);
                        total_extents += s.extent_ids.len();
                    }
                }
                let discards = part_discards
                    .get(pid)
                    .map(|v| {
                        v.iter()
                            .map(|&(eid, bytes)| InfoDiscardEntry {
                                extent_id: eid,
                                bytes,
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                Some(InfoPartitionView {
                    part_id: *pid,
                    ps_addr,
                    range_start: String::from_utf8_lossy(&rg.start_key).into_owned(),
                    range_end: if rg.end_key.is_empty() {
                        String::new()
                    } else {
                        String::from_utf8_lossy(&rg.end_key).into_owned()
                    },
                    live_size,
                    total_extents,
                    log_stream_id: r.log_stream,
                    row_stream_id: r.row_stream,
                    meta_stream_id: r.meta_stream,
                    discards,
                })
            })
            .collect();

        // F205: keep sort_by live_size desc for consistent ordering.
        partitions_view.sort_by_key(|p| std::cmp::Reverse(p.live_size));

        if let Some(pid) = part {
            match partitions_view.into_iter().find(|p| p.part_id == pid) {
                Some(pv) => println!("{}", serde_json::to_string_pretty(&pv)?),
                None => eprintln!("partition {pid} not found"),
            }
        } else {
            let snapshot = InfoSnapshot {
                nodes: nodes_view,
                extents: extents_view,
                streams: streams_view,
                partitions: partitions_view,
            };
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        }
    } else {
        // === Text output ===
        let show_pids: Vec<u64> = if let Some(pid) = part {
            vec![pid]
        } else {
            part_ids.clone()
        };

        if part.is_none() {
            println!("=== Nodes ===");
            for (nid, n) in &nodes_sorted {
                if n.control_address.is_empty() {
                    println!("  node {}: addr={}", nid, n.address);
                } else {
                    println!(
                        "  node {}: addr={}, control_addr={}",
                        nid, n.address, n.control_address
                    );
                }
                for did in &n.disks {
                    if let Some(d) = disk_map.get(did) {
                        println!("    disk {}: uuid={}, online={}", did, d.uuid, d.online);
                    } else {
                        println!("    disk {}: (no info)", did);
                    }
                }
            }

            println!("\n=== Extents ===");
            let mut extents: Vec<(&u64, &MgrExtentInfo)> = extent_map.iter().collect();
            extents.sort_by_key(|(id, _)| **id);
            for (eid, e) in &extents {
                let in_streams = stream_member_count.get(*eid).copied().unwrap_or(0);
                let mut tag = String::new();
                if open_extents.contains(eid) {
                    tag.push_str(" (open)");
                }
                // Orphan = in no live stream. Distinguish a LEGACY extent still
                // retained by the upgrade-safety guard (vp_table_refs>0 — live
                // VPs from a pre-removal build, a Stage-2 migration target) from
                // a both-zero reclaimable extent (the EXTENT10-AUTORECLAIM sweep
                // reaps it). Also flag a refs vs membership gap (MERGE-REFS-LEAK).
                if in_streams == 0 {
                    tag.push_str(if e.vp_table_refs > 0 {
                        " (ORPHAN: no stream, retained by vp_table_refs — legacy live data, Stage-2 migrate)"
                    } else {
                        " (ORPHAN: no stream, vp_table_refs=0 — reclaimable, pending sweep)"
                    });
                } else if e.refs as usize != in_streams {
                    tag.push_str(" (refs-leak)");
                }
                let layout = if e.ec_converted {
                    format!(
                        "EC({}+{}), data={:?}, parity={:?}",
                        e.replicates.len(),
                        e.parity.len(),
                        e.replicates,
                        e.parity
                    )
                } else if e.parity.is_empty() {
                    format!("replicas={:?}", e.replicates)
                } else {
                    let mut all = e.replicates.clone();
                    all.extend(e.parity.iter().copied());
                    format!("replicas={:?}", all)
                };
                println!(
                    "  extent {}: size={}{}, {}, refs={} (streams={}), vp_table_refs={}, eversion={}",
                    eid,
                    human_size(e.sealed_length),
                    tag,
                    layout,
                    e.refs,
                    in_streams,
                    e.vp_table_refs,
                    e.eversion
                );
            }

            println!("\n=== Streams ===");
            for (sid, s) in &streams_sorted {
                let total = stream_total(s, &extent_map);
                let r = if s.replicates > 0 {
                    s.replicates
                } else {
                    s.extent_ids
                        .iter()
                        .find_map(|eid| {
                            extent_map
                                .get(eid)
                                .filter(|e| !e.ec_converted)
                                .map(|e| e.replicates.len() as u32)
                        })
                        .unwrap_or(0)
                };
                let layout = if s.ec_parity_shard == 0 {
                    format!("repl={}", r)
                } else {
                    format!("repl={}, EC={}+{}", r, s.ec_data_shard, s.ec_parity_shard)
                };
                println!(
                    "  stream {} ({}): extents={:?}, total={}",
                    sid,
                    layout,
                    s.extent_ids,
                    human_size(total)
                );
            }
        }

        let section_header = if let Some(pid) = part {
            format!("\n=== Partition {pid} ===")
        } else {
            "\n=== Partitions ===".to_string()
        };
        println!("{section_header}");

        for pid in &show_pids {
            let r = match regions.get(pid) {
                Some(r) => r,
                None => {
                    println!("  part {pid}: not found");
                    continue;
                }
            };
            let rg = match r.rg.as_ref() {
                Some(r) => r,
                None => continue,
            };
            let ps_addr = part_addr_map
                .get(pid)
                .or_else(|| ps_details.get(&r.ps_id).map(|d| &d.address))
                .map(|s| s.as_str())
                .unwrap_or("unknown");
            println!(
                "  part {}: ps={}, range=[{}..{})",
                pid,
                ps_addr,
                String::from_utf8_lossy(&rg.start_key),
                if rg.end_key.is_empty() {
                    "\u{221e}".to_string()
                } else {
                    String::from_utf8_lossy(&rg.end_key).to_string()
                }
            );
            let discards = part_discards.get(pid);
            let mut part_total = 0u64;
            let mut part_extents = 0usize;
            for (label, sid) in [
                ("log", r.log_stream),
                ("row", r.row_stream),
                ("meta", r.meta_stream),
            ] {
                if let Some(s) = stream_map.get(&sid) {
                    let total = stream_total(s, &extent_map);
                    part_total += total;
                    part_extents += s.extent_ids.len();
                    let mut line = format!(
                        "    {}: stream {}, extents={:?}, size={}",
                        label,
                        sid,
                        s.extent_ids,
                        human_size(total)
                    );
                    if label == "log" {
                        if let Some(d) = discards {
                            if !d.is_empty() {
                                let total_discard: i64 = d.iter().map(|(_, b)| b).sum();
                                line.push_str(&format!(
                                    ", discard: {} ext / {} pending",
                                    d.len(),
                                    human_size(total_discard as u64)
                                ));
                            }
                        }
                    }
                    println!("{line}");
                }
            }
            println!(
                "    total: {} extents, {}",
                part_extents,
                human_size(part_total)
            );
        }
    }
    Ok(())
}

fn main() {
    let args = parse();
    let rt = compio::runtime::Runtime::new().expect("compio runtime");
    if let Err(e) = rt.block_on(run(args)) {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

