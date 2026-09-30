//! `convert_sst` — rewrite every SST of a stopped cluster from MetaBlock v1 to
//! v2, once, by hand; then delete this tool together with
//! `autumn_partition_server::sst_convert`.
//!
//! v2 adds `num_entries` / `num_deletions` to the MetaBlock (the PS compacts a
//! partition whose tombstones pass TiKV's rule). The data blocks do not change.
//! A PS of this build refuses a v1 SST, so the upgrade is:
//!
//! 1. stop every `autumn-ps` (the manager and the extent nodes stay up);
//! 2. `convert_sst --manager HOST:PORT`;
//! 3. start the new `autumn-ps`.
//!
//! Per partition it takes the partition's owner lock (as a PS open does, which
//! also fences any PS still holding it), reads the checkpoint, and for every
//! listed SST that is still v1: reads it, rebuilds it from its data blocks with
//! the current builder, checks the rebuild holds the same key range and
//! sequence, and appends it to the row stream. Then it publishes one checkpoint
//! naming the rebuilt SSTs (every other field unchanged) and truncates the row
//! stream before the first extent that checkpoint references — the same rule
//! the PS uses after a compaction.
//!
//! Interrupted runs resume. A checkpoint is rewritten only after all its SSTs
//! are appended, so a rerun either finds the old checkpoint and converts again
//! (earlier appends are simply unreferenced) or finds the new one, whose SSTs
//! are all v2, and only repeats the truncate. The MetaBlock's version field
//! says which; nothing is guessed from the bytes.
//!
//! A split child whose SSTs are shared with its sibling gets its own rebuilt
//! copy; the shared extents are released when both have been truncated.
//!
//! ```text
//! convert_sst --manager HOST:PORT[,HOST:PORT] [--dry-run] [--part ID]...
//!             [--parallel N] [--max-extent-size-bytes N]
//! ```

use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use futures::stream::{self, StreamExt};

use autumn_partition_server::sst_convert::{rebuild_sst, NOT_INTACT_FRAME};
use autumn_rpc::manager_rpc::{self, rkyv_decode, rkyv_encode};
use autumn_rpc::partition_rpc::{SstLocation, TableLocations};
use autumn_stream::{normalize_endpoint, ConnPool, StreamClient};

const MAGIC: u32 = 0x4155_3742;
const META_V1: u16 = 1;
const META_V2: u16 = 2;
/// The manager evicts a PS it has not heard from in 10 s; anything fresher is
/// taken as still running.
const LIVE_PS_HEARTBEAT_SECS: u64 = 10;
/// The PS default for `--max-extent-size-bytes`.
const DEFAULT_MAX_EXTENT_SIZE: u64 = 16 * 1024 * 1024 * 1024;

// ---------------------------------------------------------------------------
// MetaBlock v1, as the previous build wrote it.
// ---------------------------------------------------------------------------

struct MetaV1 {
    /// (relative_offset, block_len, base_key) per data block.
    blocks: Vec<(u32, u32, Vec<u8>)>,
    smallest_key: Vec<u8>,
    biggest_key: Vec<u8>,
    estimated_size: u64,
    seq_num: u64,
    vp_extent_id: u64,
    vp_offset: u64,
    vp_deps: Vec<u64>,
    discards: HashMap<u64, i64>,
    /// Absent in the oldest v1 SSTs.
    min_expires_at: Option<u64>,
}

struct Cursor<'a> {
    data: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.at + n > self.data.len() {
            bail!("MetaBlock v1 truncated at offset {} (need {n})", self.at);
        }
        let s = &self.data[self.at..self.at + n];
        self.at += n;
        Ok(s)
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn remaining(&self) -> usize {
        self.data.len() - self.at
    }
}

/// The version of a MetaBlock (its bytes including the trailing CRC).
fn meta_version(meta: &[u8]) -> Result<u16> {
    if meta.len() < 10 {
        bail!("MetaBlock too short: {} bytes", meta.len());
    }
    let magic = u32::from_le_bytes(meta[0..4].try_into().unwrap());
    if magic != MAGIC {
        bail!("MetaBlock magic mismatch: {magic:#x}");
    }
    Ok(u16::from_le_bytes(meta[4..6].try_into().unwrap()))
}

fn decode_meta_v1(meta: &[u8]) -> Result<MetaV1> {
    let (payload, crc) = meta.split_at(meta.len() - 4);
    let stored = u32::from_le_bytes(crc.try_into().unwrap());
    let computed = crc32c::crc32c(payload);
    if stored != computed {
        bail!("MetaBlock CRC mismatch: stored={stored:#x} computed={computed:#x}");
    }
    let mut c = Cursor { data: payload, at: 0 };
    if c.u32()? != MAGIC {
        bail!("MetaBlock magic mismatch");
    }
    let version = c.u16()?;
    if version != META_V1 {
        bail!("expected MetaBlock v1, found v{version}");
    }
    let n = c.u32()? as usize;
    let mut blocks = Vec::with_capacity(n);
    for _ in 0..n {
        let klen = c.u16()? as usize;
        let key = c.take(klen)?.to_vec();
        let rel = c.u32()?;
        let len = c.u32()?;
        blocks.push((rel, len, key));
    }
    let bloom_len = c.u32()? as usize;
    c.take(bloom_len)?;
    let sk_len = c.u16()? as usize;
    let smallest_key = c.take(sk_len)?.to_vec();
    let bk_len = c.u16()? as usize;
    let biggest_key = c.take(bk_len)?.to_vec();
    let estimated_size = c.u64()?;
    let seq_num = c.u64()?;
    let vp_extent_id = c.u64()?;
    let vp_offset = c.u64()?;
    let ndeps = c.u32()? as usize;
    let mut vp_deps = Vec::with_capacity(ndeps);
    for _ in 0..ndeps {
        vp_deps.push(c.u64()?);
    }
    c.take(1)?; // compression_type
    let mut discards = HashMap::new();
    if c.remaining() >= 4 {
        let nd = c.u32()? as usize;
        for _ in 0..nd {
            let eid = c.u64()?;
            let sz = c.u64()? as i64;
            discards.insert(eid, sz);
        }
    }
    let min_expires_at = if c.remaining() >= 8 { Some(c.u64()?) } else { None };
    if c.remaining() != 0 {
        bail!("MetaBlock v1: {} trailing bytes", c.remaining());
    }
    Ok(MetaV1 {
        blocks,
        smallest_key,
        biggest_key,
        estimated_size,
        seq_num,
        vp_extent_id,
        vp_offset,
        vp_deps,
        discards,
        min_expires_at,
    })
}

/// Split an SST's bytes into its MetaBlock (with CRC, without the trailing
/// `meta_len`) and everything before it.
fn split_sst(sst: &[u8]) -> Result<(&[u8], &[u8])> {
    if sst.len() < 8 {
        bail!("SST too short: {} bytes", sst.len());
    }
    let n = sst.len();
    let meta_len = u32::from_le_bytes(sst[n - 4..].try_into().unwrap()) as usize;
    if meta_len == 0 || meta_len + 4 > n {
        bail!("invalid meta_len={meta_len} for an SST of {n} bytes");
    }
    let meta_start = n - 4 - meta_len;
    Ok((&sst[..meta_start], &sst[meta_start..n - 4]))
}

/// Rebuild a v1 SST as v2. Fails unless the rebuild covers the same key range
/// and sequence as the v1 MetaBlock claims.
fn convert_sst_bytes(sst: &[u8]) -> Result<(Vec<u8>, u64, u64)> {
    let (data, meta) = split_sst(sst)?;
    let m = decode_meta_v1(meta)?;
    let mut blocks = Vec::with_capacity(m.blocks.len());
    for (rel, len, key) in &m.blocks {
        let (start, end) = (*rel as usize, *rel as usize + *len as usize);
        if end > data.len() {
            bail!("block at {rel}+{len} past the data region ({} bytes)", data.len());
        }
        blocks.push((Bytes::copy_from_slice(&data[start..end]), key.clone()));
    }
    let r = rebuild_sst(&blocks, m.vp_extent_id, m.vp_offset, m.discards)?;
    if r.smallest_key != m.smallest_key || r.biggest_key != m.biggest_key || r.seq_num != m.seq_num {
        bail!(
            "rebuild disagrees with the v1 MetaBlock: seq {} vs {}, key range differs: {}",
            r.seq_num,
            m.seq_num,
            r.smallest_key != m.smallest_key || r.biggest_key != m.biggest_key
        );
    }
    let mut deps = m.vp_deps.clone();
    deps.sort_unstable();
    if r.vp_deps != deps {
        bail!("rebuild disagrees with the v1 MetaBlock on vp_deps");
    }
    if m.min_expires_at.is_some_and(|v| v != r.min_expires_at) {
        bail!("rebuild disagrees with the v1 MetaBlock on min_expires_at");
    }
    if r.estimated_size != m.estimated_size {
        eprintln!(
            "  note: estimated_size {} -> {} (recomputed from the entries)",
            m.estimated_size, r.estimated_size
        );
    }
    Ok((r.bytes, r.num_entries, r.num_deletions))
}

// ---------------------------------------------------------------------------
// Checkpoints (the PS's `read_all_table_locations` / `save_table_locs_raw`).
// ---------------------------------------------------------------------------

/// The last valid `[len u32][rkyv TableLocations]` record in a meta extent,
/// and whether every frame decoded with no partial tail — the same verdict as
/// the PS's `decode_last_table_checkpoint_with_health`.
fn last_table_locations(mut buf: &[u8]) -> (Option<TableLocations>, bool) {
    let mut last = None;
    let mut intact = true;
    while buf.len() >= 4 {
        let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
        if 4 + len > buf.len() {
            intact = false;
            break;
        }
        match rkyv_decode::<TableLocations>(&buf[4..4 + len]) {
            Ok(t) => last = Some(t),
            Err(_) => intact = false,
        }
        buf = &buf[4 + len..];
    }
    (last, intact)
}

/// The last checkpoint of every non-empty meta extent, and whether the whole
/// stream was intact.
async fn read_checkpoints(sc: &StreamClient, meta_stream: u64) -> Result<(Vec<TableLocations>, bool)> {
    let info = sc.get_stream_info(meta_stream).await?;
    let mut out = Vec::new();
    let mut intact = true;
    for &eid in &info.extent_ids {
        let (payload, _) = sc.read_bytes_from_extent(eid, 0, 0).await?;
        if payload.is_empty() {
            continue;
        }
        let (last, ok) = last_table_locations(&payload);
        intact &= ok && last.is_some();
        out.extend(last);
    }
    Ok((out, intact))
}

/// Append the checkpoint — followed, in the SAME append, by
/// `NOT_INTACT_FRAME` when the stream it replaces was not intact, so recovery
/// keeps distrusting the covered-prefix marker — then keep only the extent the
/// append landed in.
///
/// A non-intact stream may end inside a frame whose length prefix runs past
/// the end; appended after it, the new frames would be read as that frame's
/// body. So the tail is rolled first and the new frames start a fresh extent.
/// Truncating to the append's own extent (not the stream's last) keeps them
/// even if the append rolled the tail behind itself. Returns the extent.
async fn publish_checkpoint(
    sc: &StreamClient,
    meta_stream: u64,
    t: &TableLocations,
    intact: bool,
) -> Result<u64> {
    if !intact {
        sc.seal_and_roll_tail(meta_stream).await.context("roll meta tail")?;
    }
    let payload = rkyv_encode(t);
    let mut rec = Vec::with_capacity(4 + payload.len() + 4 + NOT_INTACT_FRAME.len());
    rec.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    rec.extend_from_slice(&payload);
    if !intact {
        rec.extend_from_slice(&(NOT_INTACT_FRAME.len() as u32).to_le_bytes());
        rec.extend_from_slice(NOT_INTACT_FRAME);
    }
    let r = sc.append(meta_stream, &rec).await?;
    let info = sc.get_stream_info(meta_stream).await?;
    if info.extent_ids.first() != Some(&r.extent_id) {
        sc.truncate(meta_stream, r.extent_id).await?;
    }
    Ok(r.extent_id)
}

/// Same checkpoint content: SST list and replay cursor.
fn same_checkpoint(a: &TableLocations, b: &TableLocations) -> bool {
    let key = |t: &TableLocations| {
        t.locs.iter().map(|l| (l.extent_id, l.offset, l.len)).collect::<Vec<_>>()
    };
    key(a) == key(b) && (a.vp_extent_id, a.vp_offset) == (b.vp_extent_id, b.vp_offset)
}

// ---------------------------------------------------------------------------
// Manager calls.
// ---------------------------------------------------------------------------

struct Mgr {
    pool: Rc<ConnPool>,
    addrs: Vec<String>,
    endpoint: String,
}

impl Mgr {
    /// Try each manager until one answers with something other than NotLeader.
    async fn call<T>(
        &self,
        msg: u8,
        payload: Bytes,
        decode: impl Fn(&[u8]) -> Result<(T, u8, String)>,
    ) -> Result<T> {
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

    async fn live_ps(&self) -> Result<Vec<(u64, u64)>> {
        let o = self
            .call(
                manager_rpc::MSG_GET_CLUSTER_OVERVIEW,
                rkyv_encode(&manager_rpc::GetClusterOverviewReq {}),
                |b| {
                    let r: manager_rpc::GetClusterOverviewResp =
                        rkyv_decode(b).map_err(|e| anyhow!(e))?;
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

    async fn owner_lock(&self, key: &str) -> Result<i64> {
        self.call(
            manager_rpc::MSG_ACQUIRE_OWNER_LOCK,
            rkyv_encode(&manager_rpc::AcquireOwnerLockReq { owner_key: key.to_string() }),
            |b| {
                let r: manager_rpc::AcquireOwnerLockResp = rkyv_decode(b).map_err(|e| anyhow!(e))?;
                Ok((r.owner_epoch, r.code, r.message))
            },
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// One partition.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct PartReport {
    converted: usize,
    already_v2: usize,
    bytes: u64,
    entries: u64,
    deletions: u64,
    truncated_extents: usize,
}

async fn convert_partition(
    mgr: &Mgr,
    part_id: u64,
    row_stream: u64,
    meta_stream: u64,
    max_extent_size: u64,
    dry_run: bool,
) -> Result<PartReport> {
    let owner_key = format!("partition/{part_id}");
    let epoch = mgr.owner_lock(&owner_key).await.context("owner lock")?;
    let sc = StreamClient::new_with_owner_epoch(
        &mgr.endpoint,
        owner_key,
        epoch,
        max_extent_size,
        mgr.pool.clone(),
    )
    .await
    .context("stream client")?;
    for sid in [row_stream, meta_stream] {
        sc.commit_length(sid).await.with_context(|| format!("commit_length stream {sid}"))?;
        if !dry_run {
            sc.fence_tail(sid, epoch).await.with_context(|| format!("fence stream {sid}"))?;
        }
    }

    let (mut ckpts, intact) = read_checkpoints(&sc, meta_stream).await.context("read checkpoint")?;
    let mut report = PartReport::default();
    let ckpt = match ckpts.len() {
        0 => return Ok(report),
        1 => ckpts.pop().unwrap(),
        n => bail!(
            "{n} checkpoint records — a merge whose survivor has not opened since, or \
             a convert_sst run stopped between publishing and truncating: start the \
             previous autumn-ps once (it reads v2 too) so it publishes one, stop it, rerun"
        ),
    };

    let mut new_locs: Vec<SstLocation> = Vec::with_capacity(ckpt.locs.len());
    for loc in &ckpt.locs {
        if loc.len < 8 {
            bail!("SST extent={} offset={}: length {} too short", loc.extent_id, loc.offset, loc.len);
        }
        let (tail, _) = sc.read_bytes_from_extent(loc.extent_id, loc.offset + loc.len - 4, 4).await?;
        if tail.len() < 4 {
            bail!("SST extent={} offset={}: short tail read", loc.extent_id, loc.offset);
        }
        let meta_len = u32::from_le_bytes(tail[..4].try_into().unwrap()) as u64;
        if meta_len == 0 || meta_len + 4 > loc.len {
            bail!("SST extent={} offset={}: invalid meta_len {meta_len}", loc.extent_id, loc.offset);
        }
        let (meta, _) = sc
            .read_bytes_from_extent(loc.extent_id, loc.offset + loc.len - 4 - meta_len, meta_len)
            .await?;
        match meta_version(&meta)? {
            META_V2 => {
                report.already_v2 += 1;
                new_locs.push(loc.clone());
            }
            META_V1 => {
                let (sst, _) = sc.read_bytes_from_extent(loc.extent_id, loc.offset, loc.len).await?;
                if sst.len() as u64 != loc.len {
                    bail!("SST extent={} offset={}: short read {} of {}", loc.extent_id, loc.offset, sst.len(), loc.len);
                }
                let (bytes, entries, deletions) = convert_sst_bytes(&sst).with_context(|| {
                    format!("SST extent={} offset={} len={}", loc.extent_id, loc.offset, loc.len)
                })?;
                report.converted += 1;
                report.bytes += bytes.len() as u64;
                report.entries += entries;
                report.deletions += deletions;
                if dry_run {
                    new_locs.push(loc.clone());
                    continue;
                }
                let r = sc.append(row_stream, &bytes).await.context("append rebuilt SST")?;
                new_locs.push(SstLocation {
                    extent_id: r.extent_id,
                    offset: r.offset,
                    len: r.end - r.offset,
                });
            }
            v => bail!("SST extent={} offset={}: unknown MetaBlock version {v}", loc.extent_id, loc.offset),
        }
    }
    if dry_run {
        return Ok(report);
    }
    if report.converted > 0 {
        let new_ckpt = TableLocations { locs: new_locs.clone(), ..ckpt.clone() };
        publish_checkpoint(&sc, meta_stream, &new_ckpt, intact)
            .await
            .context("publish checkpoint")?;
        // Read it back the way recovery will before cutting anything: the row
        // truncate below drops the v1 SSTs, which is only safe once recovery
        // is certain to load the checkpoint naming their replacements.
        let (back, back_intact) = read_checkpoints(&sc, meta_stream).await.context("re-read checkpoint")?;
        if back.len() != 1 || !same_checkpoint(&back[0], &new_ckpt) || back_intact != intact {
            bail!(
                "the meta stream does not read back as the checkpoint just published \
                 ({} records, intact {back_intact}); the row stream is left untouched",
                back.len()
            );
        }
    }

    // Drop the row-stream prefix no listed SST lives in. Like the PS's
    // `row_truncate_point`, cut nothing if a listed SST's extent is not in the
    // stream at all.
    let keep: HashSet<u64> = new_locs.iter().map(|l| l.extent_id).collect();
    let info = sc.get_stream_info(row_stream).await?;
    if let Some(missing) = keep.iter().find(|e| !info.extent_ids.contains(e)) {
        bail!("checkpoint lists extent {missing}, which the row stream does not hold; not truncating");
    }
    let cut = if keep.is_empty() {
        info.extent_ids.len().saturating_sub(1)
    } else {
        info.extent_ids
            .iter()
            .position(|e| keep.contains(e))
            .ok_or_else(|| anyhow!("no SST of the new checkpoint is in the row stream"))?
    };
    if cut > 0 {
        sc.truncate(row_stream, info.extent_ids[cut]).await.context("truncate row stream")?;
        report.truncated_extents = cut;
    }
    Ok(report)
}

// ---------------------------------------------------------------------------

async fn run(
    mgr: Mgr,
    parts: Vec<u64>,
    parallel: usize,
    max_extent_size: u64,
    dry_run: bool,
) -> Result<()> {
    // Every PS must be down: a PS that reopens a partition bumps its owner lock
    // and fences this tool mid-partition.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let live = mgr.live_ps().await.context("cluster overview")?;
        if live.is_empty() {
            break;
        }
        if Instant::now() >= deadline {
            bail!("partition servers still heartbeating (ps_id, secs ago): {live:?} — stop every autumn-ps first");
        }
        compio::time::sleep(Duration::from_secs(2)).await;
    }

    let regions = mgr.regions().await.context("get regions")?;
    let mut todo: Vec<(u64, u64, u64)> = regions
        .regions
        .iter()
        .filter(|(id, _)| parts.is_empty() || parts.contains(id))
        .map(|(id, r)| (*id, r.row_stream, r.meta_stream))
        .collect();
    todo.sort_unstable();
    if !parts.is_empty() && todo.len() != parts.len() {
        bail!("some --part ids are not partitions of this cluster");
    }
    println!(
        "convert_sst: {} partitions{}",
        todo.len(),
        if dry_run { " (dry run: no data is written)" } else { "" }
    );

    let mgr = &mgr;
    let results: Vec<(u64, Result<PartReport>)> = stream::iter(todo)
        .map(|(id, row, meta)| async move {
            (id, convert_partition(mgr, id, row, meta, max_extent_size, dry_run).await)
        })
        .buffer_unordered(parallel)
        .collect()
        .await;

    let mut failed = 0;
    let mut total = PartReport::default();
    let mut results = results;
    results.sort_by_key(|(id, _)| *id);
    for (id, r) in results {
        match r {
            Ok(r) => {
                println!(
                    "part {id}: converted {} SSTs ({} bytes, {} entries, {} tombstones), {} already v2, dropped {} row extents",
                    r.converted, r.bytes, r.entries, r.deletions, r.already_v2, r.truncated_extents
                );
                total.converted += r.converted;
                total.already_v2 += r.already_v2;
                total.bytes += r.bytes;
            }
            Err(e) => {
                failed += 1;
                println!("part {id}: FAILED: {e:#}");
            }
        }
    }
    println!(
        "convert_sst: {} SSTs converted ({} bytes), {} already v2, {failed} partitions failed",
        total.converted, total.bytes, total.already_v2
    );
    if failed > 0 {
        bail!("{failed} partitions failed; fix the cause and rerun (converted ones are skipped)");
    }
    Ok(())
}

fn main() -> Result<()> {
    let mut manager = String::new();
    let mut dry_run = false;
    let mut parts = Vec::new();
    let mut parallel = 4usize;
    let mut max_extent_size = DEFAULT_MAX_EXTENT_SIZE;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--manager" => manager = it.next().context("--manager needs a value")?,
            "--dry-run" => dry_run = true,
            "--part" => parts.push(it.next().context("--part needs an id")?.parse()?),
            "--parallel" => parallel = it.next().context("--parallel needs a value")?.parse()?,
            "--max-extent-size-bytes" => {
                max_extent_size = it.next().context("--max-extent-size-bytes needs a value")?.parse()?
            }
            other => bail!("unknown argument {other}"),
        }
    }
    if manager.is_empty() {
        bail!("--manager is required");
    }
    if parallel == 0 {
        bail!("--parallel must be at least 1");
    }
    let addrs: Vec<String> = manager.split(',').map(|s| normalize_endpoint(s.trim())).collect();
    compio::runtime::Runtime::new()?.block_on(async move {
        let mgr = Mgr {
            pool: Rc::new(ConnPool::new()),
            addrs,
            endpoint: manager,
        };
        run(mgr, parts, parallel, max_extent_size, dry_run).await
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A v1 SST exactly as the previous build wrote it: the current builder's
    /// output with the MetaBlock re-encoded as v1 (no counts).
    fn v1_sst(v2: &[u8]) -> Vec<u8> {
        let (data, meta) = split_sst(v2).unwrap();
        let payload = &meta[..meta.len() - 4];
        let mut v1 = payload[..payload.len() - 16].to_vec();
        v1[4..6].copy_from_slice(&META_V1.to_le_bytes());
        let crc = crc32c::crc32c(&v1);
        v1.extend_from_slice(&crc.to_le_bytes());
        let mut out = data.to_vec();
        out.extend_from_slice(&v1);
        out.extend_from_slice(&(v1.len() as u32).to_le_bytes());
        out
    }

    /// The tool's own logic (version dispatch, v1 parse, rebuild, compare) on
    /// an SST with no entries; multi-block rebuilds are covered by
    /// `sst_convert`'s own test and end to end by the conversion system test.
    fn sample_v2() -> Vec<u8> {
        let mut discards = HashMap::new();
        discards.insert(42, 4096);
        rebuild_sst(&[], 7, 11, discards).unwrap().bytes
    }

    #[test]
    fn an_empty_v1_sst_converts_and_a_v2_one_is_recognised() {
        let v2 = sample_v2();
        let (_, meta) = split_sst(&v2).unwrap();
        assert_eq!(meta_version(meta).unwrap(), META_V2);
        let v1 = v1_sst(&v2);
        let (_, meta1) = split_sst(&v1).unwrap();
        assert_eq!(meta_version(meta1).unwrap(), META_V1);
        let m = decode_meta_v1(meta1).unwrap();
        assert_eq!((m.vp_extent_id, m.vp_offset), (7, 11));
        let (out, entries, deletions) = convert_sst_bytes(&v1).unwrap();
        assert_eq!(out, v2);
        assert_eq!((entries, deletions), (0, 0));
    }

    #[test]
    fn checkpoint_record_round_trips() {
        let t = TableLocations {
            locs: vec![SstLocation { extent_id: 3, offset: 4, len: 5 }],
            vp_extent_id: 9,
            vp_offset: 10,
            log_extent_count: 2,
            fence_floors: vec![(1, 2)],
        };
        let payload = rkyv_encode(&t);
        let mut rec = Vec::new();
        for _ in 0..2 {
            rec.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            rec.extend_from_slice(&payload);
        }
        let (back, intact) = last_table_locations(&rec);
        let back = back.unwrap();
        assert_eq!(back.locs[0].len, 5);
        assert_eq!(back.fence_floors, vec![(1, 2)]);
        assert!(intact);

        // An undecodable frame after it (the tool's own marker, a legacy
        // companion, bit rot) keeps the checkpoint but is not intact.
        rec.extend_from_slice(&(NOT_INTACT_FRAME.len() as u32).to_le_bytes());
        rec.extend_from_slice(NOT_INTACT_FRAME);
        let (back, intact) = last_table_locations(&rec);
        assert_eq!(back.unwrap().vp_offset, 10);
        assert!(!intact);
        // A partial tail is not intact either.
        let (_, intact) = last_table_locations(&rec[..rec.len() - 3]);
        assert!(!intact);
    }
}
