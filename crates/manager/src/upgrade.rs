//! Conversions `autumn-convert` runs against a stopped cluster's etcd. Each
//! one holds the old form it reads; the new form is this crate's own record,
//! so a conversion writes exactly what the manager decodes. Delete a
//! conversion with its `autumn-convert` subcommand.

use rkyv::{Archive, Deserialize, Serialize};

use std::collections::HashMap;

use crate::persist::records::{ExtentRecord, RegionRecord, StreamRecord};
use crate::persist::{self, PersistRecord, PERSIST_HEADER_LEN, PERSIST_MAGIC};

/// `extents/<id>`: format 1, with `vp_table_refs` after `refs`, to format 2.
pub mod extent_records {
    use super::*;

    pub const PREFIX: &str = "extents/";

    /// `ExtentRecord` at format 1. rkyv's layout depends on the field list, so
    /// this decodes format-1 bodies exactly.
    #[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
    pub struct ExtentRecordV1 {
        pub extent_id: u64,
        pub replicates: Vec<u64>,
        pub parity: Vec<u64>,
        pub eversion: u64,
        pub refs: u64,
        pub vp_table_refs: u64,
        pub sealed_length: u64,
        pub sealed: bool,
        pub avali: u32,
        pub replicate_disks: Vec<u64>,
        pub parity_disks: Vec<u64>,
        pub ec_converted: bool,
    }

    pub enum Step {
        /// Already format 2.
        Already,
        /// The format-2 value to write in place of the old one.
        Convert(Vec<u8>),
    }

    /// One `extents/<id>` value. Refuses an extent at `refs == 0` whose
    /// `vp_table_refs` is not 0: only that count kept it, and under format 2
    /// the orphan sweep deletes it. (With `refs > 0` the count changes
    /// nothing.)
    pub fn convert(key: &str, value: &[u8]) -> Result<Step, String> {
        let v1 = match decode(key, value)? {
            Decoded::V2 => return Ok(Step::Already),
            Decoded::V1(v1) => v1,
        };
        if v1.refs == 0 && v1.vp_table_refs != 0 {
            return Err(format!(
                "{key}: in no stream (refs = 0) and kept only by vp_table_refs = {}; \
                 format 2 has no such count, so the orphan sweep would delete it. \
                 Nothing can tell whether a value still lives there: if it holds \
                 nothing you need, delete the key by hand and rerun",
                v1.vp_table_refs
            ));
        }
        let v2 = ExtentRecord {
            extent_id: v1.extent_id,
            replicates: v1.replicates,
            parity: v1.parity,
            eversion: v1.eversion,
            refs: v1.refs,
            sealed_length: v1.sealed_length,
            sealed: v1.sealed,
            avali: v1.avali,
            replicate_disks: v1.replicate_disks,
            parity_disks: v1.parity_disks,
            ec_converted: v1.ec_converted,
        };
        Ok(Step::Convert(persist::encode(&v2)))
    }

    pub(super) enum Decoded {
        V1(ExtentRecordV1),
        V2,
    }

    pub(super) fn decode(key: &str, value: &[u8]) -> Result<Decoded, String> {
        if value.len() < PERSIST_HEADER_LEN || value[0..4] != PERSIST_MAGIC {
            return Err(format!("{key}: not an enveloped manager record"));
        }
        let (record_type, version) = (value[4], value[5]);
        if record_type != ExtentRecord::RECORD_TYPE {
            return Err(format!(
                "{key}: record type {record_type}, not an extent ({})",
                ExtentRecord::RECORD_TYPE
            ));
        }
        match version {
            2 => Ok(Decoded::V2),
            1 => autumn_rpc::manager_rpc::rkyv_decode(&value[PERSIST_HEADER_LEN..])
                .map(Decoded::V1)
                .map_err(|e| format!("{key}: format-1 body does not decode: {e}")),
            v => Err(format!("{key}: extent record format {v}; this converts 1 to 2")),
        }
    }
}

/// The check `autumn-convert extent-records` makes before writing anything,
/// while the old release can still run: `autumn-convert sst` converts a
/// partition whose meta stream holds ONE checkpoint record, so a meta stream
/// with more than one extent that is open or sealed non-empty (each such extent
/// may carry a record) is refused here, where opening the partition on the old
/// release still fixes it.
pub mod meta_streams {
    use super::*;

    pub const REGIONS: &str = "regions/";
    pub const STREAMS: &str = "streams/";

    /// `(part_id, meta_stream, extents that may hold a record)` for every
    /// partition with more than one such extent. Each input is a prefix's
    /// `(key, value)` pairs; extents may be at either format.
    pub fn multi_record(
        regions: &[(String, Vec<u8>)],
        streams: &[(String, Vec<u8>)],
        extents: &[(String, Vec<u8>)],
    ) -> Result<Vec<(u64, u64, Vec<u64>)>, String> {
        let mut stream_extents: HashMap<u64, Vec<u64>> = HashMap::new();
        for (k, v) in streams {
            let s: StreamRecord = persist::decode(k, v)?;
            stream_extents.insert(s.stream_id, s.extent_ids);
        }
        // extent_id -> (sealed, sealed_length)
        let mut state: HashMap<u64, (bool, u64)> = HashMap::new();
        for (k, v) in extents {
            let (id, st) = match extent_records::decode(k, v)? {
                extent_records::Decoded::V1(e) => (e.extent_id, (e.sealed, e.sealed_length)),
                extent_records::Decoded::V2 => {
                    let e: ExtentRecord = persist::decode(k, v)?;
                    (e.extent_id, (e.sealed, e.sealed_length))
                }
            };
            state.insert(id, st);
        }
        let mut out = Vec::new();
        for (k, v) in regions {
            let r: RegionRecord = persist::decode(k, v)?;
            let ids = stream_extents
                .get(&r.meta_stream)
                .cloned()
                .ok_or_else(|| format!("{k}: meta stream {} has no streams/ record", r.meta_stream))?;
            // Unknown extent: count it, the cautious way.
            let may_hold: Vec<u64> = ids
                .into_iter()
                .filter(|id| state.get(id).is_none_or(|&(sealed, len)| !sealed || len > 0))
                .collect();
            if may_hold.len() > 1 {
                out.push((r.part_id, r.meta_stream, may_hold));
            }
        }
        out.sort_unstable();
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::extent_records::{convert, ExtentRecordV1, Step};
    use super::*;

    /// `persist::freeze`'s extent fixture as format 1 wrote it (the bytes
    /// recorded before format 2), `vp_table_refs` = 5.
    const EXTENT_V1: &str = "41554d47040103000000000000000100000000000000040000000000000009000000000000000b000000000000000c000000000000000d000000000000000e00000000000000e8e7e6e5e4e3e2e1b8ffffff03000000c8ffffff010000000700000000000000020000000000000005000000000000000000000004000000010000000b000000a0ffffff03000000b0ffffff010000000000000000000000";

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }

    fn v1_value(v1: &ExtentRecordV1) -> Vec<u8> {
        let mut out = PERSIST_MAGIC.to_vec();
        out.push(ExtentRecord::RECORD_TYPE);
        out.push(1);
        out.extend_from_slice(&autumn_rpc::manager_rpc::rkyv_encode(v1));
        out
    }

    #[test]
    fn a_format_1_extent_converts_to_what_the_manager_decodes() {
        let v1 = ExtentRecordV1 {
            extent_id: 77,
            replicates: vec![3, 1, 4],
            parity: vec![9],
            eversion: 7,
            refs: 2,
            vp_table_refs: 0,
            sealed_length: 1 << 34,
            sealed: true,
            avali: 0b1011,
            replicate_disks: vec![11, 12, 13],
            parity_disks: vec![14],
            ec_converted: false,
        };
        let Step::Convert(v2) = convert("extents/77", &v1_value(&v1)).unwrap() else {
            panic!("a format-1 value must convert");
        };
        assert_eq!(v2[5], 2);
        let back: ExtentRecord = persist::decode("extents/77", &v2).unwrap();
        assert_eq!(back.extent_id, 77);
        assert_eq!(back.replicates, vec![3, 1, 4]);
        assert_eq!(back.parity, vec![9]);
        assert_eq!((back.eversion, back.refs, back.sealed_length), (7, 2, 1 << 34));
        assert!(back.sealed && !back.ec_converted);
        assert_eq!(back.avali, 0b1011);
        assert_eq!((back.replicate_disks, back.parity_disks), (vec![11, 12, 13], vec![14]));
        assert!(matches!(convert("extents/77", &v2), Ok(Step::Already)));
    }

    /// The recorded format-1 bytes decode as `ExtentRecordV1`: the vendored
    /// form is the one that was on disk.
    #[test]
    fn the_vendored_form_reads_the_recorded_format_1_bytes() {
        let raw = unhex(EXTENT_V1);
        let v1: ExtentRecordV1 =
            autumn_rpc::manager_rpc::rkyv_decode(&raw[PERSIST_HEADER_LEN..]).unwrap();
        assert_eq!(v1.extent_id, 0xE1E2E3E4E5E6E7E8);
        assert_eq!((v1.refs, v1.vp_table_refs), (2, 5));
        assert_eq!(v1.sealed_length, 16 * 1024 * 1024 * 1024);
        assert_eq!(v1.replicates, vec![3, 1, 4]);
        // refs > 0: the count changes nothing, so it converts.
        assert!(matches!(convert("extents/x", &raw), Ok(Step::Convert(_))));
    }

    #[test]
    fn an_extent_kept_only_by_vp_table_refs_is_refused() {
        let v = ExtentRecordV1 { extent_id: 3, refs: 0, vp_table_refs: 2, ..Default::default() };
        let err = convert("extents/3", &v1_value(&v)).err().expect("refused");
        assert!(err.contains("kept only by vp_table_refs = 2"), "{err}");
        let v = ExtentRecordV1 { extent_id: 3, refs: 0, vp_table_refs: 0, ..Default::default() };
        assert!(matches!(convert("extents/3", &v1_value(&v)), Ok(Step::Convert(_))));
    }

    #[test]
    fn a_meta_stream_that_may_hold_two_records_is_reported() {
        use crate::persist::records::{RegionRecord, StreamRecord};
        let region = |part: u64, meta: u64| {
            let r = RegionRecord { part_id: part, meta_stream: meta, ..Default::default() };
            (format!("regions/{part}"), persist::encode(&r))
        };
        let stream = |id: u64, extents: Vec<u64>| {
            let s = StreamRecord { stream_id: id, extent_ids: extents, ..Default::default() };
            (format!("streams/{id}"), persist::encode(&s))
        };
        let ext = |id: u64, sealed: bool, len: u64| {
            let e = ExtentRecordV1 { extent_id: id, sealed, sealed_length: len, refs: 1, ..Default::default() };
            (format!("extents/{id}"), v1_value(&e))
        };
        let regions = vec![region(1, 10), region(2, 20), region(3, 30)];
        // 1: one open tail. 2: a sealed-empty extent and an open tail. 3: a
        // sealed non-empty extent and an open tail — two records possible.
        let streams = vec![stream(10, vec![100]), stream(20, vec![200, 201]), stream(30, vec![300, 301])];
        let extents = vec![ext(100, false, 0), ext(200, true, 0), ext(201, false, 0), ext(300, true, 77), ext(301, false, 0)];
        let got = meta_streams::multi_record(&regions, &streams, &extents).unwrap();
        assert_eq!(got, vec![(3, 30, vec![300, 301])]);
    }

    #[test]
    fn other_records_and_formats_are_refused() {
        let mut raw = v1_value(&ExtentRecordV1::default());
        raw[4] = ExtentRecord::RECORD_TYPE + 1;
        assert!(convert("extents/1", &raw).is_err());
        raw[4] = ExtentRecord::RECORD_TYPE;
        raw[5] = 3;
        assert!(convert("extents/1", &raw).is_err());
        assert!(convert("extents/1", b"bare").is_err());
    }
}
