//! The partition half of `autumn-convert checkpoint-kind`: a stopped
//! partition's checkpoint from before 0.3.0 (kind 0) to a normal record
//! (`CHECKPOINT_KIND_NORMAL`). Delete this module with that subcommand.
//!
//! Per partition: take the owner lock (fencing any PS still holding it, as an
//! open does), fence and seal the meta tail, append the record with its kind
//! set and every other field unchanged, read it back from its extent, then cut
//! the meta stream before it. A partition with more than one record (a merge
//! whose survivor has not opened since, or a cut the manager deferred) is
//! refused: which record supersedes which is what the kind says, and a kind-0
//! record does not say it. A merge's sources are opened once on the old
//! release (that open publishes one record and cuts the rest); a partition's
//! own records left by a deferred cut are named by the operator
//! (`--keep-newest`), and the newest is converted.
//!
//! A rerun finds the new record last (a run stopped before its cut) and makes
//! only the cut.

use std::rc::Rc;

use anyhow::{bail, Context, Result};

use autumn_rpc::partition_rpc::{rkyv_encode, TableLocations, CHECKPOINT_KIND_NORMAL};
use autumn_stream::{ConnPool, StreamClient};

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// No checkpoint yet: nothing to convert.
    Empty,
    /// The last record already has a kind.
    Already,
    /// Appended the record with its kind (`dry_run`: would append).
    Converted,
    /// A stopped run's record is last; cut the kind-0 records before it
    /// (`dry_run`: would cut). A 0.3.0 PS refuses the partition until then.
    Cut,
}

/// The last valid record of each non-empty meta extent, with that extent, in
/// stream order. A non-empty extent with no valid record is refused.
async fn read_records(sc: &StreamClient, meta_stream: u64) -> Result<Vec<(u64, TableLocations)>> {
    let info = sc.get_stream_info(meta_stream).await?;
    let mut out = Vec::new();
    for &eid in &info.extent_ids {
        let (payload, _) = sc.read_bytes_from_extent(eid, 0, 0).await?;
        if payload.is_empty() {
            continue;
        }
        let t = crate::decode_last_table_checkpoint(&payload).with_context(|| {
            format!("meta extent {eid} holds {} bytes and no valid checkpoint record", payload.len())
        })?;
        out.push((eid, t));
    }
    Ok(out)
}

fn encode(t: &TableLocations) -> Vec<u8> {
    let payload = rkyv_encode(t);
    let mut rec = Vec::with_capacity(4 + payload.len());
    rec.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    rec.extend_from_slice(&payload);
    rec
}

fn same(a: &TableLocations, b: &TableLocations) -> bool {
    (a.vp_extent_id, a.vp_offset, a.kind, &a.fence_floors) == (b.vp_extent_id, b.vp_offset, b.kind, &b.fence_floors)
        && a.locs.iter().map(|l| (l.extent_id, l.offset, l.len)).eq(b.locs.iter().map(|l| (l.extent_id, l.offset, l.len)))
}

/// Truncate, retried while the manager answers a transient conflict: split
/// siblings share meta extents, and converted side by side their cuts race on
/// those extents' `refs`.
async fn truncate(sc: &StreamClient, stream_id: u64, extent_id: u64) -> Result<()> {
    let mut attempt = 0;
    loop {
        match sc.truncate(stream_id, extent_id).await {
            Ok(_) => return Ok(()),
            Err(e) if attempt < 20 && crate::manager_says_retry_later(&e) => {
                attempt += 1;
                compio::time::sleep(std::time::Duration::from_millis(100 * attempt)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

async fn owner_client(
    manager_endpoint: &str,
    pool: Rc<ConnPool>,
    part_id: u64,
    meta_stream: u64,
    max_extent_size: u64,
    dry_run: bool,
) -> Result<Rc<StreamClient>> {
    let owner_key = format!("partition/{part_id}");
    if dry_run {
        // Reads only: no owner lock, nothing fenced.
        return StreamClient::new_with_owner_epoch(manager_endpoint, owner_key, 0, max_extent_size, pool).await;
    }
    let sc = StreamClient::connect(manager_endpoint, owner_key, max_extent_size, pool)
        .await
        .context("owner lock")?;
    sc.fence_tail(meta_stream, sc.owner_epoch())
        .await
        .with_context(|| format!("fence meta stream {meta_stream}"))?;
    sc.seal_tail(meta_stream)
        .await
        .with_context(|| format!("seal meta stream {meta_stream}"))?;
    Ok(sc)
}

/// Convert one partition's checkpoint. Every PS must be stopped: one that
/// reopens the partition takes its owner lock and fences this run.
pub async fn convert_partition(
    manager_endpoint: &str,
    pool: Rc<ConnPool>,
    part_id: u64,
    meta_stream: u64,
    max_extent_size: u64,
    dry_run: bool,
    // The operator's word that several records are this partition's own.
    keep_newest: bool,
) -> Result<Outcome> {
    let sc = owner_client(manager_endpoint, pool, part_id, meta_stream, max_extent_size, dry_run).await?;
    let records = read_records(&sc, meta_stream).await.context("read checkpoint")?;
    let (last_eid, last) = match records.last() {
        None => return Ok(Outcome::Empty),
        Some((eid, r)) => (*eid, r.clone()),
    };
    if last.kind != 0 {
        // A stopped run's record, or a partition opened since: only the cut
        // of the records before a normal one may be left.
        if last.kind == CHECKPOINT_KIND_NORMAL && records.len() > 1 {
            if !dry_run {
                truncate(&sc, meta_stream, last_eid)
                    .await
                    .context("truncate meta stream (rerun to finish)")?;
            }
            return Ok(Outcome::Cut);
        }
        return Ok(Outcome::Already);
    }
    if records.len() > 1 && !keep_newest {
        bail!(
            "{} checkpoint records, (meta extent, cursor, tables): {:?}; this converts a \
             partition holding one. A merge's sources (the survivor has not opened since the \
             merge): open it once on the old release, stop it, rerun. Records of this partition \
             alone (a deferred cut; the old release would load both): rerun with \
             --keep-newest {part_id}",
            records.len(),
            records
                .iter()
                .map(|(e, r)| (*e, (r.vp_extent_id, r.vp_offset), r.locs.len()))
                .collect::<Vec<_>>()
        );
    }
    if dry_run {
        return Ok(Outcome::Converted);
    }
    let new = TableLocations {
        kind: CHECKPOINT_KIND_NORMAL,
        ..last
    };
    let r = sc.append(meta_stream, &encode(&new)).await.context("publish checkpoint")?;
    // Read it back from its extent, the way recovery will, before cutting.
    let (got, _) = sc.read_bytes_from_extent(r.extent_id, 0, 0).await?;
    let back = crate::decode_last_table_checkpoint(&got).context("re-read checkpoint")?;
    if !same(&back, &new) {
        bail!("meta extent {} does not read back as the record just published; nothing was cut", r.extent_id);
    }
    let info = sc.get_stream_info(meta_stream).await?;
    if info.extent_ids.first() != Some(&r.extent_id) {
        truncate(&sc, meta_stream, r.extent_id)
            .await
            .context("truncate meta stream (rerun to finish)")?;
    }
    Ok(Outcome::Converted)
}

/// The reverse, for tests: the partition's last record appended again as
/// kind 0 (to a fresh meta extent: the tail is sealed first) and, with `cut`,
/// the meta stream cut before it, as a pre-0.3.0 build leaves it.
#[doc(hidden)]
pub async fn downgrade_partition_for_tests(
    manager_endpoint: &str,
    pool: Rc<ConnPool>,
    part_id: u64,
    meta_stream: u64,
    cut: bool,
) -> Result<()> {
    let sc = owner_client(manager_endpoint, pool, part_id, meta_stream, 16 << 30, false).await?;
    let records = read_records(&sc, meta_stream).await?;
    let Some((_, last)) = records.last() else {
        bail!("no checkpoint to downgrade");
    };
    let old = TableLocations {
        kind: 0,
        ..last.clone()
    };
    let r = sc.append(meta_stream, &encode(&old)).await?;
    let info = sc.get_stream_info(meta_stream).await?;
    if cut && info.extent_ids.first() != Some(&r.extent_id) {
        truncate(&sc, meta_stream, r.extent_id).await?;
    }
    Ok(())
}
