//! `autumn-convert` — the persisted-format conversions a release needs, one
//! subcommand each, run once against a stopped cluster. A subcommand is
//! deleted in a later release. The conversions themselves live beside the
//! formats they produce (`autumn_manager::upgrade`,
//! `autumn_partition_server::sst_convert`).
//!
//! ```text
//! autumn-convert extent-records --etcd URL[,URL] [--dry-run]
//!     every manager stopped: `extents/<id>` format 1 → 2 (vp_table_refs gone)
//! autumn-convert sst --manager HOST:PORT --cluster-secret-file F [--dry-run]
//!                    [--part ID]... [--parallel N] [--max-extent-size-bytes N]
//!     every PS stopped, manager and extent nodes up: SST MetaBlock v2 → v3
//! ```

use std::process::ExitCode;
use std::rc::Rc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use futures::stream::{self, StreamExt};

use autumn_etcd::{Cmp, Op};
use autumn_manager::upgrade::{extent_records, meta_streams};
use autumn_params::convert::ConvertParams;
use autumn_rpc::manager_rpc::{self, rkyv_decode, rkyv_encode};
use autumn_stream::{normalize_endpoint, ConnPool};

const USAGE: &str = "usage: autumn-convert <extent-records|sst> [flags]";

/// Every running manager, follower included, holds `managerAlive/<id>`; the
/// leader also holds the leader key (`crates/manager/src/lib.rs`).
const LEADER_KEY: &str = "autumn-rs/stream-manager/leader";
const MANAGER_ALIVE_PREFIX: &str = "managerAlive/";
/// Below etcd's default `--max-txn-ops` (128).
const TXN_KEYS: usize = 64;
/// The manager evicts a PS it has not heard from in 10 s.
const LIVE_PS_HEARTBEAT_SECS: u64 = 10;

fn main() -> ExitCode {
    let p = ConvertParams::default();
    let rest = autumn_params::parse_or_exit(USAGE, &[&p], autumn_params::Positional::Anywhere);
    if rest.iter().any(|w| w == "-h" || w == "--help") {
        print!("{}", autumn_params::help_text(USAGE, &[&p]));
        return ExitCode::SUCCESS;
    }
    let cmd = match rest.as_slice() {
        [c] => c.as_str(),
        [] => {
            eprintln!("error: no subcommand\n{USAGE}");
            return ExitCode::from(2);
        }
        _ => {
            eprintln!("error: one subcommand, got {rest:?}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let rt = match compio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("autumn-convert: runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let r = match cmd {
        "extent-records" => rt.block_on(extent_records_cmd(&p)),
        "sst" => rt.block_on(sst_cmd(&p)),
        other => {
            eprintln!("error: unknown subcommand {other:?}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    match r {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("autumn-convert {cmd}: {e:#}");
            ExitCode::FAILURE
        }
    }
}

// ---------------------------------------------------------------------------
// extent-records
// ---------------------------------------------------------------------------

async fn extent_records_cmd(p: &ConvertParams) -> Result<()> {
    let list = p.etcd.get();
    if list.is_empty() {
        bail!("--etcd is required");
    }
    let dry_run = p.dry_run.get();
    let client = autumn_etcd::EtcdClient::connect_many(&list)
        .await
        .with_context(|| format!("connect etcd {}", list.join(",")))?;

    // A running manager would race these writes (and a new one refuses to
    // lead until they are done).
    let mut live = Vec::new();
    for prefix in [LEADER_KEY, MANAGER_ALIVE_PREFIX] {
        let resp = client.get_prefix(prefix).await.with_context(|| format!("read {prefix}"))?;
        live.extend(resp.kvs.iter().map(|kv| String::from_utf8_lossy(&kv.key).into_owned()));
    }
    if !live.is_empty() {
        bail!(
            "a manager is still running ({}). Stop every manager, wait up to 10 s for \
             its etcd lease to expire, then rerun",
            live.join(", ")
        );
    }

    let pairs = |r: autumn_etcd::proto::RangeResponse| -> Vec<(String, Vec<u8>)> {
        r.kvs.into_iter().map(|kv| (String::from_utf8_lossy(&kv.key).into_owned(), kv.value)).collect()
    };
    let resp = client
        .get_prefix(extent_records::PREFIX)
        .await
        .with_context(|| format!("read {}", extent_records::PREFIX))?;
    // `sst` converts a partition holding one checkpoint record; one that may
    // hold more is refused now, while the old release can still fix it.
    let regions = pairs(client.get_prefix(meta_streams::REGIONS).await.context("read regions/")?);
    let streams = pairs(client.get_prefix(meta_streams::STREAMS).await.context("read streams/")?);
    let extents = pairs(resp.clone());
    // Once every record is format 2 there is nothing left to protect (and a
    // stopped `sst` run may legitimately have left two records).
    let all_converted = resp
        .kvs
        .iter()
        .all(|kv| matches!(extent_records::convert(&String::from_utf8_lossy(&kv.key), &kv.value), Ok(extent_records::Step::Already)));
    let multi = if all_converted {
        Vec::new()
    } else {
        meta_streams::multi_record(&regions, &streams, &extents).map_err(|e| anyhow!(e))?
    };
    if !multi.is_empty() {
        for (part, meta, ids) in &multi {
            eprintln!("partition {part}: meta stream {meta} has {} extents that may each hold a checkpoint: {ids:?}", ids.len());
        }
        bail!(
            "{} partitions may hold more than one checkpoint record; nothing was written. On \
             the old release, start the PS and let it open them (an open replaces several \
             records with one and truncates the rest), stop it, then rerun",
            multi.len()
        );
    }
    let mut todo: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = Vec::new();
    let mut already = 0usize;
    for kv in &resp.kvs {
        let key = String::from_utf8_lossy(&kv.key).into_owned();
        match extent_records::convert(&key, &kv.value).map_err(|e| anyhow!(e))? {
            extent_records::Step::Already => already += 1,
            extent_records::Step::Convert(v) => todo.push((kv.key.clone(), kv.value.clone(), v)),
        }
    }
    println!(
        "{}: {} to convert, {already} already format 2{}",
        extent_records::PREFIX,
        todo.len(),
        if dry_run { " (dry run: nothing written)" } else { "" }
    );
    if dry_run {
        return Ok(());
    }
    for batch in todo.chunks(TXN_KEYS) {
        // No manager may have won leadership since the check above.
        let mut compare = vec![Cmp::create_revision(LEADER_KEY, 0)];
        compare.extend(batch.iter().map(|(k, old, _)| Cmp::value(k, old)));
        let txn = client
            .txn(autumn_etcd::proto::TxnRequest {
                compare,
                success: batch.iter().map(|(k, _, new)| Op::put(k, new)).collect(),
                failure: vec![],
            })
            .await
            .context("txn")?;
        if !txn.succeeded {
            bail!("a manager started, or an extent record changed, while converting; stop every manager and rerun");
        }
    }
    println!("{}: converted {}", extent_records::PREFIX, todo.len());
    Ok(())
}

// ---------------------------------------------------------------------------
// sst
// ---------------------------------------------------------------------------

struct Mgr {
    pool: Rc<ConnPool>,
    addrs: Vec<String>,
}

impl Mgr {
    /// Each manager in turn until one answers other than NotLeader.
    async fn call<T>(&self, msg: u8, payload: Bytes, decode: impl Fn(&[u8]) -> Result<(T, u8, String)>) -> Result<T> {
        let mut last = anyhow!("no manager address");
        for addr in &self.addrs {
            match self.pool.call_timeout(addr, msg, payload.clone(), Duration::from_secs(10)).await {
                Ok(bytes) => {
                    let (v, code, message) = decode(&bytes)?;
                    if code == manager_rpc::CODE_OK {
                        return Ok(v);
                    }
                    last = anyhow!("manager {addr}: code {code}: {message}");
                    if code != manager_rpc::CODE_NOT_LEADER {
                        break;
                    }
                }
                Err(e) => last = e.context(format!("manager {addr}")),
            }
        }
        Err(last)
    }

    async fn regions(&self) -> Result<manager_rpc::GetRegionsResp> {
        self.call(manager_rpc::MSG_GET_REGIONS, Bytes::new(), |b| {
            let r: manager_rpc::GetRegionsResp = rkyv_decode(b).map_err(|e| anyhow!(e))?;
            let (code, msg) = (r.code, r.message.clone());
            Ok((r, code, msg))
        })
        .await
    }

    /// PSes heard from in the last 10 s: `(ps_id, secs ago)`.
    async fn live_ps(&self) -> Result<Vec<(u64, u64)>> {
        let o = self
            .call(
                manager_rpc::MSG_GET_CLUSTER_OVERVIEW,
                rkyv_encode(&manager_rpc::GetClusterOverviewReq {}),
                |b| {
                    let r: manager_rpc::GetClusterOverviewResp = rkyv_decode(b).map_err(|e| anyhow!(e))?;
                    let (code, msg) = (r.code, r.message.clone());
                    Ok((r, code, msg))
                },
            )
            .await?;
        Ok(o.ps_servers
            .iter()
            .filter(|p| p.last_heartbeat_secs_ago < LIVE_PS_HEARTBEAT_SECS)
            .map(|p| (p.ps_id, p.last_heartbeat_secs_ago))
            .collect())
    }
}

async fn sst_cmd(p: &ConvertParams) -> Result<()> {
    let Some(secret) = p.cluster_secret_file.get() else {
        bail!("--cluster-secret-file is required: every server refuses a peer without it");
    };
    autumn_rpc::peer_auth::ClusterSecret::from_file(&secret)
        .and_then(autumn_rpc::peer_auth::install)
        .map_err(|e| anyhow!("--cluster-secret-file: {e}"))?;
    let manager = p.manager.get();
    let (dry_run, parallel, parts, max_extent) =
        (p.dry_run.get(), p.parallel.get(), p.part.get(), p.max_extent_size_bytes.get());
    let mgr = Mgr {
        pool: Rc::new(ConnPool::new()),
        addrs: manager.split(',').map(|s| normalize_endpoint(s.trim())).collect(),
    };

    // A PS that reopens a partition takes its owner lock and fences this run.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let live = mgr.live_ps().await.context("cluster overview")?;
        if live.is_empty() {
            break;
        }
        if Instant::now() >= deadline {
            bail!("partition servers still heartbeating (ps_id, secs ago): {live:?}; stop every autumn-ps first");
        }
        compio::time::sleep(Duration::from_secs(2)).await;
    }

    let regions = mgr.regions().await.context("get regions")?;
    let mut todo: Vec<(u64, [u64; 3])> = regions
        .regions
        .iter()
        .filter(|(id, _)| parts.is_empty() || parts.contains(id))
        .map(|(id, r)| (*id, [r.log_stream, r.row_stream, r.meta_stream]))
        .collect();
    todo.sort_unstable();
    if !parts.is_empty() && todo.len() != parts.len() {
        bail!("some --part ids are not partitions of this cluster");
    }
    println!(
        "sst: {} partitions{}",
        todo.len(),
        if dry_run { " (dry run: reads only)" } else { "" }
    );

    let (manager, pool) = (&manager, &mgr.pool);
    let mut results: Vec<(u64, Result<autumn_partition_server::sst_convert::PartReport>)> = stream::iter(todo)
        .map(|(id, streams)| async move {
            let r = autumn_partition_server::sst_convert::convert_partition(
                manager,
                pool.clone(),
                id,
                streams,
                max_extent,
                dry_run,
            )
            .await;
            (id, r)
        })
        .buffer_unordered(parallel)
        .collect()
        .await;
    results.sort_by_key(|(id, _)| *id);

    let (mut converted, mut already, mut bytes, mut failed) = (0usize, 0usize, 0u64, 0usize);
    for (id, r) in results {
        match r {
            Ok(r) => {
                println!(
                    "part {id}: converted {} SSTs ({} bytes), {} already v3, dropped {} row extents",
                    r.converted, r.bytes, r.already, r.truncated_extents
                );
                converted += r.converted;
                already += r.already;
                bytes += r.bytes;
            }
            Err(e) => {
                failed += 1;
                println!("part {id}: FAILED: {e:#}");
            }
        }
    }
    println!("sst: {converted} SSTs converted ({bytes} bytes), {already} already v3, {failed} partitions failed");
    if failed > 0 {
        bail!("{failed} partitions failed; fix the cause and rerun (converted ones are skipped)");
    }
    Ok(())
}
