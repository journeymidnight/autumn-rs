use autumn_rpc::partition_rpc::TableLocations;
use std::collections::BTreeMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).ok_or("missing path")?;
    let data = std::fs::read(path)?;
    let mut offset = 0usize;
    let mut count = 0usize;
    let mut last = None;
    while offset + 4 <= data.len() {
        let len = u32::from_le_bytes(data[offset..offset + 4].try_into()?) as usize;
        if offset + 4 + len > data.len() {
            break;
        }
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(len);
        aligned.extend_from_slice(&data[offset + 4..offset + 4 + len]);
        match rkyv::from_bytes::<TableLocations, rancor::Error>(&aligned) {
            Ok(record) => {
                count += 1;
                let mut ids: Vec<u64> = record.locs.iter().map(|loc| loc.extent_id).collect();
                ids.sort_unstable();
                ids.dedup();
                println!(
                    "record={count} offset={offset} vp={}:{} locs={} extents={ids:?}",
                    record.vp_extent_id,
                    record.vp_offset,
                    record.locs.len()
                );
                last = Some(record);
            }
            Err(error) => eprintln!("invalid checkpoint at {offset}: {error}"),
        }
        offset += 4 + len;
    }
    println!("records={count} bytes={offset}/{}", data.len());
    let record = last.ok_or("no checkpoint")?;
    println!(
        "vp={}:{} log_extent_count={} locs={}",
        record.vp_extent_id,
        record.vp_offset,
        record.log_extent_count,
        record.locs.len()
    );
    let mut by_extent = BTreeMap::<u64, Vec<(u64, u64)>>::new();
    for loc in record.locs {
        by_extent
            .entry(loc.extent_id)
            .or_default()
            .push((loc.offset, loc.len));
    }
    for (extent_id, locs) in by_extent {
        println!("extent={extent_id} locs={locs:?}");
    }
    Ok(())
}
