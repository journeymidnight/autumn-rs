//! `autumn-convert` — the persisted-format conversions a release needs, one
//! subcommand each, run once against a stopped cluster. A subcommand is
//! deleted in a later release. The conversions themselves live beside the
//! formats they produce (`autumn_partition_server::checkpoint_convert`).
//!
//! ```text
//! autumn-convert checkpoint-kind --manager HOST:PORT --cluster-secret-file F
//!                    [--dry-run] [--part ID]... [--keep-newest ID]... [--parallel N]
//!                    [--max-extent-size-bytes N]
//!     every PS stopped, manager and extent nodes up: checkpoint kind 0 → normal
//! ```

use std::process::ExitCode;
use std::rc::Rc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use futures::stream::{self, StreamExt};

use autumn_params::convert::ConvertParams;
use autumn_partition_server::checkpoint_convert::{convert_partition, Outcome};
use autumn_rpc::manager_rpc::{self, rkyv_decode, rkyv_encode};
use autumn_stream::{normalize_endpoint, ConnPool};

const USAGE: &str = "usage: autumn-convert <checkpoint-kind> [flags]";

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
        "checkpoint-kind" => rt.block_on(checkpoint_kind_cmd(&p)),
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

async fn checkpoint_kind_cmd(p: &ConvertParams) -> Result<()> {
    let Some(secret) = p.cluster_secret_file.get() else {
        bail!("--cluster-secret-file is required: every server refuses a peer without it");
    };
    autumn_rpc::peer_auth::ClusterSecret::from_file(&secret)
        .and_then(autumn_rpc::peer_auth::install)
        .map_err(|e| anyhow!("--cluster-secret-file: {e}"))?;
    let manager = p.manager.get();
    let (dry_run, parallel, parts, max_extent) =
        (p.dry_run.get(), p.parallel.get(), p.part.get(), p.max_extent_size_bytes.get());
    let keep_newest = p.keep_newest.get();
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
    let mut todo: Vec<(u64, u64)> = regions
        .regions
        .iter()
        .filter(|(id, _)| parts.is_empty() || parts.contains(id))
        .map(|(id, r)| (*id, r.meta_stream))
        .collect();
    todo.sort_unstable();
    if !parts.is_empty() && todo.len() != parts.len() {
        bail!("some --part ids are not partitions of this cluster");
    }
    println!(
        "checkpoint-kind: {} partitions{}",
        todo.len(),
        if dry_run { " (dry run: reads only)" } else { "" }
    );

    if let Some(id) = keep_newest.iter().find(|id| !todo.iter().any(|(t, _)| t == *id)) {
        bail!("--keep-newest {id} is not a partition being converted");
    }
    let (manager, pool, keep_newest) = (&manager, &mgr.pool, &keep_newest);
    let mut results: Vec<(u64, Result<Outcome>)> = stream::iter(todo)
        .map(|(id, meta)| async move {
            let keep = keep_newest.contains(&id);
            (id, convert_partition(manager, pool.clone(), id, meta, max_extent, dry_run, keep).await)
        })
        .buffer_unordered(parallel)
        .collect()
        .await;
    results.sort_by_key(|(id, _)| *id);

    let (mut converted, mut already, mut failed) = (0usize, 0usize, 0usize);
    for (id, r) in results {
        match r {
            Ok(Outcome::Converted) => {
                converted += 1;
                println!("part {id}: converted");
            }
            Ok(Outcome::Cut) => {
                converted += 1;
                println!("part {id}: cut the records before the converted one");
            }
            Ok(Outcome::Already | Outcome::Empty) => already += 1,
            Err(e) => {
                failed += 1;
                println!("part {id}: FAILED: {e:#}");
            }
        }
    }
    println!("checkpoint-kind: {converted} converted, {already} already done or empty, {failed} failed");
    if failed > 0 {
        bail!("{failed} partitions failed; fix the cause and rerun (converted ones are skipped)");
    }
    Ok(())
}
