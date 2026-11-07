//! One-off recovery for a checkpoint that names row-stream extents already
//! deleted by an unsafe truncate. Dropping SST locations loses any data held
//! only in those SSTs; this tool requires explicit IDs and an acknowledgement.

use std::collections::HashSet;
use std::rc::Rc;

use anyhow::{anyhow, bail, Context, Result};
use autumn_rpc::manager_rpc::{rkyv_decode, rkyv_encode};
use autumn_rpc::partition_rpc::TableLocations;
use autumn_stream::{ConnPool, StreamClient};

struct Args {
    manager: String,
    part_id: u64,
    row_stream: u64,
    meta_stream: u64,
    meta_extent: u64,
    expected_vp_eid: u64,
    expected_vp_off: u64,
    expected_locs: usize,
    drop_extents: HashSet<u64>,
    apply: bool,
}

fn parse_args() -> Result<Args> {
    let mut args = std::env::args().skip(1);
    let mut manager = None;
    let mut part_id = None;
    let mut row_stream = None;
    let mut meta_stream = None;
    let mut meta_extent = None;
    let mut expected_vp_eid = None;
    let mut expected_vp_off = None;
    let mut expected_locs = None;
    let mut drop_extents = HashSet::new();
    let mut apply = false;
    let mut ack_data_loss = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--manager" => manager = Some(args.next().context("--manager value")?),
            "--part" => part_id = Some(args.next().context("--part value")?.parse()?),
            "--row-stream" => {
                row_stream = Some(args.next().context("--row-stream value")?.parse()?)
            }
            "--meta-stream" => {
                meta_stream = Some(args.next().context("--meta-stream value")?.parse()?)
            }
            "--meta-extent" => {
                meta_extent = Some(args.next().context("--meta-extent value")?.parse()?)
            }
            "--expected-vp" => {
                let value = args.next().context("--expected-vp value")?;
                let (eid, off) = value.split_once(':').context("--expected-vp EID:OFFSET")?;
                expected_vp_eid = Some(eid.parse()?);
                expected_vp_off = Some(off.parse()?);
            }
            "--expected-locs" => {
                expected_locs = Some(args.next().context("--expected-locs value")?.parse()?)
            }
            "--drop-extent" => {
                drop_extents.insert(args.next().context("--drop-extent value")?.parse()?);
            }
            "--apply" => apply = true,
            "--ack-data-loss" => ack_data_loss = true,
            _ => bail!("unknown argument: {arg}"),
        }
    }
    if drop_extents.is_empty() {
        bail!("at least one --drop-extent is required");
    }
    if apply && !ack_data_loss {
        bail!("--apply requires --ack-data-loss");
    }
    Ok(Args {
        manager: manager.context("--manager required")?,
        part_id: part_id.context("--part required")?,
        row_stream: row_stream.context("--row-stream required")?,
        meta_stream: meta_stream.context("--meta-stream required")?,
        meta_extent: meta_extent.context("--meta-extent required")?,
        expected_vp_eid: expected_vp_eid.context("--expected-vp required")?,
        expected_vp_off: expected_vp_off.context("--expected-vp required")?,
        expected_locs: expected_locs.context("--expected-locs required")?,
        drop_extents,
        apply,
    })
}

fn last_checkpoint(data: &[u8]) -> Result<TableLocations> {
    let mut offset = 0usize;
    let mut last = None;
    while offset + 4 <= data.len() {
        let len = u32::from_le_bytes(data[offset..offset + 4].try_into()?) as usize;
        let end = offset
            .checked_add(4)
            .and_then(|n| n.checked_add(len))
            .context("checkpoint length overflow")?;
        if end > data.len() {
            break;
        }
        if let Ok(record) = rkyv_decode::<TableLocations>(&data[offset + 4..end]) {
            last = Some(record);
        }
        offset = end;
    }
    last.context("no valid TableLocations checkpoint")
}

async fn run(args: Args) -> Result<()> {
    let pool = Rc::new(ConnPool::new());
    let client = StreamClient::connect(
        &args.manager,
        format!("partition/{}", args.part_id),
        16 * 1024 * 1024 * 1024,
        pool,
    )
    .await?;
    let meta = client.get_stream_info(args.meta_stream).await?;
    if meta.extent_ids != [args.meta_extent] {
        bail!("meta stream has unexpected extents: {:?}", meta.extent_ids);
    }
    let row = client.get_stream_info(args.row_stream).await?;
    let row_eids: HashSet<u64> = row.extent_ids.iter().copied().collect();
    if args.drop_extents.iter().any(|id| row_eids.contains(id)) {
        bail!(
            "a requested drop extent still belongs to row stream: {:?}",
            row.extent_ids
        );
    }
    let (data, _) = client
        .read_bytes_from_extent(args.meta_extent, 0, 0)
        .await?;
    let mut ckpt = last_checkpoint(&data)?;
    if ckpt.vp_extent_id != args.expected_vp_eid
        || ckpt.vp_offset != args.expected_vp_off
        || ckpt.locs.len() != args.expected_locs
    {
        bail!(
            "checkpoint changed: vp={}:{} locs={}",
            ckpt.vp_extent_id,
            ckpt.vp_offset,
            ckpt.locs.len()
        );
    }
    let previous_count = ckpt.locs.len();
    let found_drop: HashSet<u64> = ckpt
        .locs
        .iter()
        .map(|loc| loc.extent_id)
        .filter(|id| args.drop_extents.contains(id))
        .collect();
    if found_drop != args.drop_extents {
        bail!("requested drop extents not all in checkpoint: found={found_drop:?}");
    }
    ckpt.locs
        .retain(|loc| !args.drop_extents.contains(&loc.extent_id));
    if ckpt
        .locs
        .iter()
        .any(|loc| !row_eids.contains(&loc.extent_id))
    {
        bail!("checkpoint contains another SST extent absent from row stream");
    }
    for loc in &ckpt.locs {
        if loc.len < 4 {
            bail!("invalid SST length: {loc:?}");
        }
        client
            .read_bytes_from_extent(loc.extent_id, loc.offset + loc.len - 4, 4)
            .await
            .with_context(|| {
                format!(
                    "preflight SST in extent {} at {}",
                    loc.extent_id, loc.offset
                )
            })?;
    }
    println!(
        "partition={} row_stream={} meta_stream={} checkpoint_vp={}:{}",
        args.part_id, args.row_stream, args.meta_stream, ckpt.vp_extent_id, ckpt.vp_offset
    );
    println!(
        "row extents={:?}; SSTs {} -> {}; dropping {:?}",
        row.extent_ids,
        previous_count,
        ckpt.locs.len(),
        args.drop_extents
    );
    if !args.apply {
        println!("dry run: no checkpoint appended");
        return Ok(());
    }
    let encoded = rkyv_encode(&ckpt);
    let mut record = Vec::with_capacity(4 + encoded.len());
    record.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
    record.extend_from_slice(&encoded);
    let result = client.append(args.meta_stream, &record).await?;
    println!(
        "appended replacement checkpoint at extent={} offset={} end={}",
        result.extent_id, result.offset, result.end
    );
    // The PS unions the last record of EVERY meta extent; a record that rolled
    // into a new extent would leave the stale one in 1063 still being read.
    if result.extent_id != args.meta_extent {
        bail!(
            "replacement landed in extent {} instead of {}; the stale checkpoint is still read",
            result.extent_id,
            args.meta_extent
        );
    }
    let (updated_data, _) = client
        .read_bytes_from_extent(result.extent_id, 0, 0)
        .await?;
    let verified = last_checkpoint(&updated_data)?;
    if verified.locs.len() != ckpt.locs.len()
        || verified
            .locs
            .iter()
            .any(|loc| args.drop_extents.contains(&loc.extent_id))
    {
        return Err(anyhow!("post-append checkpoint verification failed"));
    }
    println!(
        "verified replacement checkpoint with {} SSTs",
        verified.locs.len()
    );
    Ok(())
}

fn main() {
    let result = parse_args().and_then(|args| {
        let rt = compio::runtime::Runtime::new().context("compio runtime")?;
        rt.block_on(run(args))
    });
    if let Err(error) = result {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}
