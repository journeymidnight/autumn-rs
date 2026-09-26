//! Segmented files: a file whose bytes are a map of logical ranges onto
//! immutable data objects (`schema::Segment`, `schema::SegmentMap`).
//!
//! Why the layout exists: a multipart upload must complete WITHOUT reading,
//! copying or rewriting any part's bytes. Each part is written once as its
//! own data object; completing the upload only publishes a map from the
//! object's logical offsets onto those parts. The same map makes every later
//! change cheap in the same way — a write replaces the covered range with a
//! new data object, a truncate clips the map — so nothing ever materializes
//! the whole file.
//!
//! This module holds the pure map arithmetic and the data-object key layout;
//! the I/O around it (writing objects and pages, loading maps) lives beside it
//! and takes a `ClusterClient`, so front-ends without an `FsState` (the
//! `autumnfs` CLI) read and write the same layout.

use std::rc::Rc;

use anyhow::{anyhow, Result};
use bytes::Bytes;

use autumn_client::{ClusterClient, WriteLease};

use crate::key;
use crate::schema::{
    self, Segment, SegmentMap, StripeLayout, INLINE_SEGMENTS, MAX_EXTENT, SEGMENT_PAGE,
};

/// Replace `[off, off+len)` of a sorted, non-overlapping map with `new`
/// (`None` leaves a hole). `new`, if given, must cover exactly that range.
/// Segments cut by the range keep only their uncovered ends, with `data_off`
/// advanced for a right-hand remainder — no data is read or moved.
pub fn splice(segs: &[Segment], off: u64, len: u64, new: Option<Segment>) -> Vec<Segment> {
    debug_assert!(new.as_ref().is_none_or(|n| n.off == off && n.len == len));
    let end = off.saturating_add(len);
    let mut out = Vec::with_capacity(segs.len() + 2);
    let mut placed = new.is_none();
    for s in segs {
        if s.end() <= off {
            out.push(s.clone());
            continue;
        }
        if s.off >= end {
            if !placed {
                out.push(new.clone().expect("unplaced"));
                placed = true;
            }
            out.push(s.clone());
            continue;
        }
        if s.off < off {
            out.push(Segment { len: off - s.off, ..s.clone() });
        }
        if !placed {
            out.push(new.clone().expect("unplaced"));
            placed = true;
        }
        if s.end() > end {
            out.push(Segment {
                off: end,
                len: s.end() - end,
                data_off: s.data_off + (end - s.off),
                ..s.clone()
            });
        }
    }
    if !placed {
        out.push(new.expect("unplaced"));
    }
    out
}

/// The map truncated to `size`: segments at or past it dropped, a straddler
/// shortened. A later extension reads as a hole, never as the cut-off bytes.
pub fn clip(segs: &[Segment], size: u64) -> Vec<Segment> {
    splice(segs, size, u64::MAX - size, None)
}

/// The segments intersecting `[start, end)`.
pub fn overlapping(segs: &[Segment], start: u64, end: u64) -> &[Segment] {
    if start >= end {
        return &segs[..0];
    }
    let first = segs.partition_point(|s| s.end() <= start);
    let last = segs.partition_point(|s| s.off < end);
    &segs[first..last.max(first)]
}

/// One extent slice of a segmented read: the data object's key, the range
/// inside that extent's value, and where it lands in the read buffer.
#[derive(Debug, PartialEq, Eq)]
pub struct DataRead {
    pub key: Vec<u8>,
    pub offset: u32,
    pub length: u32,
    pub dest_offset: usize,
}

/// Plan the extent reads for `[start, end)` of a file with map `segs`. Holes
/// produce nothing (the caller's zero-filled buffer is their answer). Every
/// planned slice lies inside a dense data object, so a missing or short value
/// is corruption and the caller must fail rather than zero-fill.
pub fn plan_reads(segs: &[Segment], start: u64, end: u64) -> Vec<DataRead> {
    let mut out = Vec::new();
    for s in overlapping(segs, start, end) {
        let a = start.max(s.off);
        let b = end.min(s.end());
        let unit = s.unit as u64;
        let mut pos = s.data_off + (a - s.off);
        let data_end = s.data_off + (b - s.off);
        while pos < data_end {
            let unit_off = pos / unit * unit;
            let n = data_end.min(unit_off + unit) - pos;
            out.push(DataRead {
                key: key::data_extent_key(s.data_ino, unit_off, s.lanes, s.unit),
                offset: (pos - unit_off) as u32,
                length: n as u32,
                dest_offset: (a - start + (pos - (s.data_off + (a - s.off)))) as usize,
            });
            pos += n;
        }
    }
    out
}

/// The geometry new data objects are written with: the filesystem's declared
/// lane count, one `MAX_EXTENT` per unit.
pub fn object_geometry(declared: &StripeLayout) -> (u8, u32) {
    (declared.lanes, MAX_EXTENT as u32)
}

/// A data object's extents: `(offset, key)` for every `unit` step of `len`.
pub fn object_extents(data_ino: u64, len: u64, lanes: u8, unit: u32) -> Vec<(u64, Vec<u8>)> {
    let unit64 = unit as u64;
    (0..len.div_ceil(unit64))
        .map(|i| {
            let off = i * unit64;
            (off, key::data_extent_key(data_ino, off, lanes, unit))
        })
        .collect()
}

/// Every data object a map references.
pub fn referenced_objects(segs: &[Segment]) -> std::collections::BTreeSet<u64> {
    segs.iter().map(|s| s.data_ino).collect()
}

// ── I/O ────────────────────────────────────────────────────────────────────

/// Write `data` as data object `data_ino` (dense, `unit`-sized extents from
/// offset 0), stamped with `lease`. The caller has recorded the object
/// somewhere a reclaimer can find it BEFORE calling this.
pub async fn write_object(
    client: &ClusterClient,
    data_ino: u64,
    data: Bytes,
    lanes: u8,
    unit: u32,
    lease: WriteLease,
) -> Result<()> {
    let extents = object_extents(data_ino, data.len() as u64, lanes, unit);
    let items: Vec<(&[u8], Bytes, u64)> = extents
        .iter()
        .map(|(off, k)| {
            let start = *off as usize;
            let end = (start + unit as usize).min(data.len());
            (k.as_slice(), data.slice(start..end), 0u64)
        })
        .collect();
    for r in client.put_many_fenced(&items, lease).await {
        r.map_err(|e| anyhow!("data object {data_ino}: {e}"))?;
    }
    Ok(())
}

/// Delete data object `data_ino` of `len` bytes. Idempotent.
pub async fn delete_object(
    client: &ClusterClient,
    data_ino: u64,
    len: u64,
    lanes: u8,
    unit: u32,
    lease: WriteLease,
) -> Result<()> {
    let extents = object_extents(data_ino, len, lanes, unit);
    let keys: Vec<&[u8]> = extents.iter().map(|(_, k)| k.as_slice()).collect();
    for r in client.delete_many_fenced(&keys, lease).await {
        r.map_err(|e| anyhow!("delete data object {data_ino}: {e}"))?;
    }
    Ok(())
}

/// Whether a map of `count` segments is stored as pages.
pub fn is_paged(count: usize) -> bool {
    count > INLINE_SEGMENTS
}

/// Build the stored form of `segs`: inline when small, else written as pages
/// under `map_id` (a fresh, never-reused id — pages are immutable; ignored
/// for an inline map).
pub async fn store_map(
    client: &ClusterClient,
    segs: Vec<Segment>,
    map_id: u64,
    lease: WriteLease,
) -> Result<SegmentMap> {
    let count = segs.len() as u64;
    if !is_paged(segs.len()) {
        return Ok(SegmentMap {
            inline: segs,
            map_id: 0,
            page_starts: Vec::new(),
            count,
        });
    }
    let pages: Vec<Vec<Segment>> = segs.chunks(SEGMENT_PAGE).map(<[Segment]>::to_vec).collect();
    let page_starts = pages.iter().map(|p| p[0].off).collect();
    let keys: Vec<Vec<u8>> = (0..pages.len())
        .map(|i| key::segment_page_key(map_id, i as u32))
        .collect();
    let items: Vec<(&[u8], Bytes, u64)> = keys
        .iter()
        .zip(&pages)
        .map(|(k, p)| (k.as_slice(), Bytes::from(schema::encode_segment_page(p)), 0u64))
        .collect();
    for r in client.put_many_fenced(&items, lease).await {
        r.map_err(|e| anyhow!("segment map {map_id}: {e}"))?;
    }
    Ok(SegmentMap {
        inline: Vec::new(),
        map_id,
        page_starts,
        count,
    })
}

/// Delete `count` pages of map `map_id`. Idempotent.
pub async fn delete_pages(client: &ClusterClient, map_id: u64, count: u32, lease: WriteLease) -> Result<()> {
    let keys: Vec<Vec<u8>> = (0..count).map(|i| key::segment_page_key(map_id, i)).collect();
    let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    for r in client.delete_many_fenced(&refs, lease).await {
        r.map_err(|e| anyhow!("delete segment map {map_id}: {e}"))?;
    }
    Ok(())
}

/// Immutable map pages already fetched, keyed by `(map_id, page)`.
pub type PageCache = std::collections::HashMap<(u64, u32), Rc<Vec<Segment>>>;

/// Pages kept per cache before it is cleared: 256 pages of up to 1024
/// segments is about 10 MiB per session. Pages never change, so the only
/// cost of clearing is a refetch.
const PAGE_CACHE_CAP: usize = 256;

/// The segments of `map` intersecting `[start, end)`, fetching only the pages
/// that range touches. A missing page is corruption, never an empty range.
pub async fn load_range(
    client: &ClusterClient,
    cache: &mut PageCache,
    map: &SegmentMap,
    start: u64,
    end: u64,
) -> Result<Vec<Segment>> {
    if map.map_id == 0 {
        let segs = overlapping(&map.inline, start, end).to_vec();
        for s in &segs {
            s.checked().map_err(|e| anyhow!(e))?;
        }
        return Ok(segs);
    }
    if start >= end || map.page_starts.is_empty() {
        return Ok(Vec::new());
    }
    // The page holding `start` is the last one starting at or before it.
    let first = map.page_starts.partition_point(|&p| p <= start).saturating_sub(1);
    let last = map.page_starts.partition_point(|&p| p < end);
    let mut out = Vec::new();
    let mut missing: Vec<u32> = Vec::new();
    for i in first..last.max(first + 1).min(map.page_starts.len()) {
        if !cache.contains_key(&(map.map_id, i as u32)) {
            missing.push(i as u32);
        }
    }
    if !missing.is_empty() {
        if cache.len() + missing.len() > PAGE_CACHE_CAP {
            cache.clear();
        }
        let keys: Vec<Vec<u8>> = missing.iter().map(|&i| key::segment_page_key(map.map_id, i)).collect();
        let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        for (i, v) in missing.iter().zip(client.get_many(&refs).await) {
            let bytes = v
                .map_err(|e| anyhow!("segment map {} page {i}: {e}", map.map_id))?
                .ok_or_else(|| anyhow!("segment map {} page {i} is missing", map.map_id))?;
            let page = schema::decode_segment_page(&bytes)
                .map_err(|e| anyhow!("segment map {} page {i}: {e}", map.map_id))?;
            for s in &page {
                s.checked().map_err(|e| anyhow!("segment map {} page {i}: {e}", map.map_id))?;
            }
            cache.insert((map.map_id, *i), Rc::new(page));
        }
    }
    for i in first..last.max(first + 1).min(map.page_starts.len()) {
        let page = &cache[&(map.map_id, i as u32)];
        out.extend_from_slice(overlapping(page, start, end));
    }
    Ok(out)
}

/// Every segment of `map` (all pages). For reclaim and rewrites of the map.
pub async fn load_all(client: &ClusterClient, cache: &mut PageCache, map: &SegmentMap) -> Result<Vec<Segment>> {
    load_range(client, cache, map, 0, u64::MAX).await
}

// ── operations on a file, over an `FsState` ──────────────────────────────────

use crate::schema::SegcRecord;
use crate::state::FsState;

/// Geometry for new data objects: the declared lane count (read once per
/// session), one `MAX_EXTENT` per unit.
async fn geometry(state: &mut FsState) -> Result<(u8, u32)> {
    if state.stripe_geom.is_none() {
        state.stripe_geom = Some(crate::geom::read_stripe_geom(&state.client).await?);
    }
    Ok(object_geometry(state.stripe_geom.as_ref().expect("just set")))
}

/// Record that `file_ino` created `id`, BEFORE writing it.
async fn record(state: &mut FsState, file_ino: u64, id: u64, rec: &SegcRecord, lease: WriteLease) -> Result<()> {
    state
        .kv_put_fenced(&key::segc_key(file_ino, id), &schema::encode_segc(rec), lease)
        .await
}

/// A data object written as a stream: each unit goes out the moment it
/// fills, up to [`STREAM_INFLIGHT`] at once, with no barrier between them, so
/// the writes overlap the arrival of the rest of the body and a large body is
/// never held in memory.
///
/// The record at `record_key` bounds what may exist: no data put is issued
/// past the length the record durably allows, so whoever undoes or reclaims
/// the object deletes everything that can be there. The record never allows
/// more than `STREAM_INFLIGHT` units past the end written, whatever the writer
/// declares — reclaiming an object walks every key its record allows, so a
/// record taken from an untrusted length could name billions — and it is
/// raised again once half that headroom is used (every fifth unit). A
/// declared length ([`ObjectStream::declare`]) caps it: a body within that
/// window is recorded once, in the background while its first unit is still
/// arriving. Only one raise is ever in flight, so raises land in order. A
/// CRC32C of the bytes is kept on the way through.
pub struct ObjectStream {
    pub data_ino: u64,
    pub lanes: u8,
    pub unit: u32,
    client: Rc<ClusterClient>,
    record_key: Vec<u8>,
    lease: WriteLease,
    /// The length the durable record allows.
    recorded: u64,
    /// The length the writer said the object will have.
    declared: Option<u64>,
    /// The record raise in flight, and the length it raises to.
    raising: Option<(u64, compio::runtime::JoinHandle<Result<()>>)>,
    /// Data puts in flight, oldest first.
    inflight: std::collections::VecDeque<compio::runtime::JoinHandle<Result<()>>>,
    /// The first failure; every later call returns it.
    failed: Option<String>,
    pending: bytes::BytesMut,
    /// Bytes handed to data puts.
    written: u64,
    crc: u32,
}

/// Data puts in flight per stream (one unit each).
const STREAM_INFLIGHT: usize = 8;

fn task_result<T, E: std::fmt::Debug>(r: std::result::Result<Result<T>, E>, what: &str) -> Result<T> {
    r.unwrap_or_else(|e| Err(anyhow!("{what} task: {e:?}")))
}

impl ObjectStream {
    /// A new, empty object whose record will live at `record_key`.
    pub async fn new(state: &mut FsState, record_key: impl FnOnce(u64) -> Vec<u8>, lease: WriteLease) -> Result<Self> {
        let (lanes, unit) = geometry(state).await?;
        let data_ino = crate::meta::alloc_inode(state).await?;
        Ok(ObjectStream {
            data_ino,
            lanes,
            unit,
            client: state.client.clone(),
            record_key: record_key(data_ino),
            lease,
            recorded: 0,
            declared: None,
            raising: None,
            inflight: std::collections::VecDeque::new(),
            failed: None,
            pending: bytes::BytesMut::new(),
            written: 0,
            crc: 0,
        })
    }

    /// The object will be `len` bytes: start recording it now (up to the
    /// window), in the background, so the first data put does not wait for
    /// it. A body that turns out longer still works (the record is raised
    /// again); a shorter one leaves the record over-long until
    /// [`ObjectStream::finish`] trims it, and an over-long record only makes a
    /// delete touch keys that are not there.
    pub fn declare(&mut self, len: u64) {
        self.declared = Some(len);
        let to = self.record_target(self.written);
        if to > self.recorded && self.raising.is_none() {
            self.raising = Some((to, self.spawn_record(to)));
        }
    }

    /// What to raise the record to for writes up to `end`: the window past
    /// it, but no further than a declared length that covers it.
    fn record_target(&self, end: u64) -> u64 {
        let window = end + STREAM_INFLIGHT as u64 * self.unit as u64;
        match self.declared {
            Some(n) if n >= end => n.min(window),
            _ => window,
        }
    }

    fn spawn_record(&self, len: u64) -> compio::runtime::JoinHandle<Result<()>> {
        let rec = SegcRecord::Object { len, lanes: self.lanes, unit: self.unit };
        let (client, key, lease) = (self.client.clone(), self.record_key.clone(), self.lease);
        compio::runtime::spawn(async move {
            client
                .put_fenced(&key, &schema::encode_segc(&rec), lease)
                .await
                .map_err(|e| anyhow!("KV put: {e}"))
        })
    }

    /// Wait until the record durably allows `end`, starting a raise if the
    /// one in flight (if any) does not reach it. The next raise starts once
    /// half the window is used, so it lands before the writes need it.
    async fn ensure_recorded(&mut self, end: u64) -> Result<()> {
        while end > self.recorded {
            match self.settle_raise().await {
                Some(r) => r?,
                None => {
                    let to = self.record_target(end);
                    self.raising = Some((to, self.spawn_record(to)));
                }
            }
        }
        let half = STREAM_INFLIGHT as u64 * self.unit as u64 / 2;
        let to = self.record_target(end);
        if self.raising.is_none() && self.recorded - end < half && to > self.recorded {
            self.raising = Some((to, self.spawn_record(to)));
        }
        Ok(())
    }

    // A handle is awaited where it sits and removed only once it settled:
    // dropped at that await (a request cancelled), it would cancel its task,
    // and `settle` could no longer wait for a put already on the wire.

    /// Wait for the oldest data put; `None` when none is in flight.
    async fn settle_oldest(&mut self) -> Option<Result<()>> {
        let h = self.inflight.front_mut()?;
        let r = task_result(h.await, "data put");
        self.inflight.pop_front();
        Some(r)
    }

    /// Wait for the record raise in flight; `None` when there is none.
    async fn settle_raise(&mut self) -> Option<Result<()>> {
        let (to, h) = self.raising.as_mut()?;
        let to = *to;
        let r = task_result(h.await, "record");
        self.raising = None;
        if r.is_ok() {
            self.recorded = to;
        }
        Some(r)
    }

    fn check(&self) -> Result<()> {
        match &self.failed {
            Some(e) => Err(anyhow!("data object {}: {e}", self.data_ino)),
            None => Ok(()),
        }
    }

    /// Record the first failure, then let everything in flight settle before
    /// returning it.
    async fn fail(&mut self, e: anyhow::Error) -> anyhow::Error {
        self.failed.get_or_insert_with(|| format!("{e:#}"));
        self.settle().await;
        e
    }

    /// Wait for every put and record raise in flight, whatever their
    /// outcome. An undo must come after this: it deletes what the record
    /// names, and a put still in flight could land after that delete.
    pub async fn settle(&mut self) {
        while let Some(r) = self.settle_oldest().await {
            if let Err(e) = r {
                tracing::debug!(data_ino = self.data_ino, error = %e, "a data put failed while settling");
            }
        }
        if let Some(Err(e)) = self.settle_raise().await {
            tracing::debug!(data_ino = self.data_ino, error = %e, "a record raise failed while settling");
        }
    }

    /// Put `chunk` (at most one unit) at the current end, after the oldest
    /// put when [`STREAM_INFLIGHT`] are already out.
    async fn issue(&mut self, chunk: Bytes) -> Result<()> {
        if self.inflight.len() >= STREAM_INFLIGHT {
            if let Some(Err(e)) = self.settle_oldest().await {
                return Err(self.fail(e).await);
            }
        }
        let end = self.written + chunk.len() as u64;
        if let Err(e) = self.ensure_recorded(end).await {
            return Err(self.fail(e).await);
        }
        let k = key::data_extent_key(self.data_ino, self.written, self.lanes, self.unit);
        let (client, lease, data_ino) = (self.client.clone(), self.lease, self.data_ino);
        self.inflight.push_back(compio::runtime::spawn(async move {
            let r = client.put_many_fenced(&[(k.as_slice(), chunk, 0u64)], lease).await;
            match r.into_iter().next() {
                Some(Ok(())) => Ok(()),
                Some(Err(e)) => Err(anyhow!("data object {data_ino}: {e}")),
                None => Err(anyhow!("data object {data_ino}: no reply")),
            }
        }));
        self.written = end;
        Ok(())
    }

    /// Append `data`. Needs only the client this stream was made with, not
    /// the `FsState`, so a caller that shares one state between many
    /// requests (the S3 gateway) can stream a body without holding the state
    /// for the network writes. A put that fails is reported by a later call.
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        self.check()?;
        self.crc = crc32c::crc32c_append(self.crc, data);
        self.pending.extend_from_slice(data);
        let unit = self.unit as usize;
        while self.pending.len() >= unit {
            let chunk = self.pending.split_to(unit).freeze();
            self.issue(chunk).await?;
        }
        Ok(())
    }

    /// Write the rest and wait for every put. The record is trimmed to the
    /// exact length when it allows more, which only happens once every put
    /// has landed. Returns `(length, crc32c)`; calling it again is a no-op.
    pub async fn finish(&mut self) -> Result<(u64, u32)> {
        self.check()?;
        let rest = self.pending.split().freeze();
        if !rest.is_empty() {
            self.issue(rest).await?;
        }
        while let Some(r) = self.settle_oldest().await {
            if let Err(e) = r {
                return Err(self.fail(e).await);
            }
        }
        if let Some(Err(e)) = self.settle_raise().await {
            return Err(self.fail(e).await);
        }
        if self.recorded > self.written {
            let rec = SegcRecord::Object { len: self.written, lanes: self.lanes, unit: self.unit };
            self.client
                .put_fenced(&self.record_key, &schema::encode_segc(&rec), self.lease)
                .await
                .map_err(|e| anyhow!("KV put: {e}"))?;
            self.recorded = self.written;
        }
        Ok((self.written, self.crc))
    }

    /// The segment mapping `[off, off+len)` of a file onto the whole object.
    pub fn segment(&self, off: u64) -> Segment {
        Segment { off, len: self.written, data_ino: self.data_ino, data_off: 0, lanes: self.lanes, unit: self.unit }
    }
}

/// Write `data` as a new data object owned by `file_ino`, recorded first.
/// Returns the segment mapping `[off, off+len)` of the file onto it.
pub async fn new_object(
    state: &mut FsState,
    file_ino: u64,
    off: u64,
    data: Bytes,
    lease: WriteLease,
) -> Result<Segment> {
    let (lanes, unit) = geometry(state).await?;
    let data_ino = crate::meta::alloc_inode(state).await?;
    let len = data.len() as u64;
    record(state, file_ino, data_ino, &SegcRecord::Object { len, lanes, unit }, lease).await?;
    write_object(&state.client, data_ino, data, lanes, unit, lease).await?;
    Ok(Segment { off, len, data_ino, data_off: 0, lanes, unit })
}

/// Store `segs` as `file_ino`'s next map, recording a paged map's pages
/// before writing them.
pub async fn publish_map(state: &mut FsState, file_ino: u64, segs: Vec<Segment>, lease: WriteLease) -> Result<SegmentMap> {
    let map_id = if is_paged(segs.len()) {
        let id = crate::meta::alloc_inode(state).await?;
        let count = segs.len().div_ceil(SEGMENT_PAGE) as u32;
        record(state, file_ino, id, &SegcRecord::Pages { count }, lease).await?;
        id
    } else {
        0
    };
    store_map(&state.client, segs, map_id, lease).await
}

/// A segmented file is changed only under this session's WRITE lease. The
/// lease is what keeps a reclaim (EXCLUSIVE) away while the new objects are
/// written but the map naming them is not yet published; without it the
/// reclaim would see them as garbage.
fn held_write_lease(state: &FsState, ino: u64) -> Result<WriteLease> {
    let lease = state.write_lease_for(ino);
    if lease.inode_hint == 0 {
        return Err(anyhow!("EBUSY: segmented file {ino} is changed only under a WRITE lease"));
    }
    Ok(lease)
}

/// Whether a change of `ino` by this session needs [`hold_transient_write`]:
/// a segmented file this session holds nothing on.
pub async fn needs_transient_write(state: &mut FsState, ino: u64) -> Result<bool> {
    if state.held_leases.borrow().contains_key(&ino) {
        return Ok(false);
    }
    // Cached, so the caller's own reads of a plain file cost nothing more.
    crate::write::ensure_inode_cached(state, ino).await?;
    Ok(state.inodes.get(&ino).is_some_and(|is| is.meta.segments.is_some()))
}

/// Take a WRITE lease on `ino` for one change by a session that has not
/// opened it (a path truncate). Conflict is EBUSY, as for an open. The cached
/// inode is dropped: it may predate another session's change, and a map
/// clipped from a stale copy would, once published, have the reclaim delete
/// that session's objects. The caller reads it again under the lease.
pub async fn hold_transient_write(state: &mut FsState, ino: u64) -> Result<()> {
    use autumn_client::lease::{self, AcquireResult};
    let cluster = state.client.clone();
    let id = state.client_id.clone();
    let epoch = match lease::acquire(&cluster, &id, ino, autumn_rpc::manager_rpc::LEASE_MODE_WRITE)
        .await
        .map_err(|e| anyhow!("write lease {ino}: {e}"))?
    {
        AcquireResult::Granted(info) => info.version,
        _ => return Err(anyhow!("EBUSY: segmented file {ino} is held by another client")),
    };
    state.held_leases.borrow_mut().insert(
        ino,
        crate::state::FuseLease {
            writer_refs: 0,
            reader_refs: 0,
            mode: autumn_rpc::manager_rpc::LEASE_MODE_WRITE,
            lease_epoch: epoch,
            revoked: false,
        },
    );
    state.inodes.remove(&ino);
    state.dirty_inodes.remove(&ino);
    Ok(())
}

/// Give back what `hold_transient_write` took. What the change left
/// unreferenced is reclaimed now if nobody else holds the file, else by the
/// sweep.
pub async fn drop_transient_write(state: &mut FsState, ino: u64) {
    state.held_leases.borrow_mut().remove(&ino);
    if let Err(e) = autumn_client::lease::release(&state.client, &state.client_id, ino).await {
        tracing::warn!(ino, error = %e, "releasing a transient write lease failed; TTL revoke is the backstop");
    }
    if state.segment_garbage.contains(&ino) {
        if let Err(e) = reclaim_live(state, ino).await {
            tracing::warn!(ino, error = %e, "segment reclaim after truncate failed; the sweep retries");
        }
    }
}

/// Write `data` at `off` of segmented file `ino`: the range becomes a new
/// data object spliced into the map. Nothing existing is read or copied.
/// Updates the cached meta (map, size, generation) and marks it dirty; the
/// caller's inode put publishes it.
pub async fn write_file_range(state: &mut FsState, ino: u64, off: u64, data: &[u8]) -> Result<()> {
    if data.is_empty() {
        return Ok(());
    }
    let map = state
        .inodes
        .get(&ino)
        .and_then(|is| is.meta.segments.clone())
        .ok_or_else(|| anyhow!("ino {ino} is not a segmented file"))?;
    let lease = held_write_lease(state, ino)?;
    // Marked before anything is written: whatever of this change never gets
    // published (a crash, a failed put) is then found by the sweep.
    mark_garbage(state, ino, lease).await?;
    let seg = new_object(state, ino, off, Bytes::copy_from_slice(data), lease).await?;
    let old = load_all(&state.client, &mut state.segment_pages, &map).await?;
    let len = seg.len;
    let segs = splice(&old, off, len, Some(seg));
    let new_map = publish_map(state, ino, segs, lease).await?;
    let is = state.inodes.get_mut(&ino).expect("cached above");
    is.meta.segments = Some(new_map);
    is.meta.size = is.meta.size.max(off + len);
    is.meta.generation += 1;
    is.dirty = true;
    state.dirty_inodes.insert(ino);
    Ok(())
}

/// Clip segmented file `ino`'s map to `new_size` in the cached meta. The
/// caller sets the size and puts the inode.
pub async fn truncate_file(state: &mut FsState, ino: u64, new_size: u64) -> Result<()> {
    let map = state
        .inodes
        .get(&ino)
        .and_then(|is| is.meta.segments.clone())
        .ok_or_else(|| anyhow!("ino {ino} is not a segmented file"))?;
    let lease = held_write_lease(state, ino)?;
    let old = load_all(&state.client, &mut state.segment_pages, &map).await?;
    let segs = clip(&old, new_size);
    let new_map = if segs == old {
        map
    } else {
        mark_garbage(state, ino, lease).await?;
        publish_map(state, ino, segs, lease).await?
    };
    let is = state.inodes.get_mut(&ino).expect("cached above");
    is.meta.segments = Some(new_map);
    is.meta.generation += 1;
    Ok(())
}

/// Note that `ino` may have garbage, for the sweep and for this session's
/// last close of it. Written before every change, not only one known to drop
/// something: an object written but never published is garbage too, and
/// nothing else would ever look for it. One put per (session, file).
async fn mark_garbage(state: &mut FsState, ino: u64, lease: WriteLease) -> Result<()> {
    if !state.segment_garbage.contains(&ino) {
        state.kv_put_fenced(&key::segment_garbage_key(ino), b"1", lease).await?;
        // Remembered only once it is in KV: a failed put must be retried by
        // the next change, not skipped.
        state.segment_garbage.insert(ino);
    }
    Ok(())
}

/// Reclaim what a LIVE segmented file no longer references, once no other
/// client holds it. The map is read from KV, never from a cache: another
/// session may have published a newer map whose objects a stale one would
/// condemn. Returns `false` (the marker stays for the next sweep) while
/// another client holds any lease on the file — an open writer is mid-change,
/// a reader may still hold an older map.
pub async fn reclaim_live(state: &mut FsState, ino: u64) -> Result<bool> {
    // Forgotten whatever the outcome: the KV marker is what the sweep follows
    // from here, and another session may reclaim and unmark it once this one
    // lets go, after which a remembered entry would skip this session's next
    // mark.
    state.segment_garbage.remove(&ino);
    use autumn_client::lease::{self, AcquireResult};
    let cluster = state.client.clone();
    let id = state.client_id.clone();
    let epoch = match lease::acquire(&cluster, &id, ino, autumn_rpc::manager_rpc::LEASE_MODE_EXCLUSIVE)
        .await
        .map_err(|e| anyhow!("reclaim {ino}: {e}"))?
    {
        AcquireResult::Granted(info) => info.version,
        _ => return Ok(false),
    };
    let lease = WriteLease { inode_hint: ino, lease_epoch: epoch };
    let res = async {
        // Gone altogether: its tombstone's reclaim owns everything now.
        if let Some(bytes) = state.kv_get_opt(&key::inode_key(ino)).await? {
            let m = schema::decode_inode_meta(&bytes).map_err(|e| anyhow!("inode {ino}: {e}"))?;
            reclaim(state, ino, m.segments.as_ref(), lease).await?;
        }
        state.kv_delete_fenced(&key::segment_garbage_key(ino), lease).await?;
        anyhow::Ok(true)
    }
    .await;
    if let Err(e) = lease::release(&cluster, &id, ino).await {
        tracing::warn!(ino, error = %e, "reclaim: releasing the exclusive lease failed; TTL revoke is the backstop");
    }
    res
}

/// Reclaim every marked live file no other client holds. Cheap when there
/// are none: one empty range scan.
pub async fn sweep_garbage(state: &mut FsState) -> Result<usize> {
    let prefix = key::segment_garbage_prefix();
    let from = state.garbage_sweep_from.take().unwrap_or_else(|| prefix.clone());
    let (keys, has_more) = state.kv_range_page(&prefix, &from, 1024).await?;
    // One page per tick; the next resumes after it, and the one after the
    // last page starts over.
    state.garbage_sweep_from = match (has_more, keys.last()) {
        (true, Some(last)) => Some(crate::dir::name_successor(last)),
        _ => None,
    };
    let mut n = 0;
    for k in keys {
        let Some(ino) = key::parse_segment_garbage_key(&k) else { continue };
        // Open here: this session's last close reclaims it.
        if state.held_leases.borrow().contains_key(&ino) {
            continue;
        }
        match reclaim_live(state, ino).await {
            Ok(true) => n += 1,
            Ok(false) => {}
            Err(e) => tracing::warn!(ino, error = %e, "segment reclaim failed; retried next sweep"),
        }
    }
    Ok(n)
}

/// Delete everything `file_ino` has created that `current` (its map now, or
/// `None` once the file is gone) no longer names, with their records.
/// Records of what the map still names are kept, so a later change can
/// reclaim it. Judging against the current map alone is what makes this
/// correct after any number of changes and any crash point.
///
/// The caller must hold the file exclusively (an EXCLUSIVE lease, or the
/// file unreachable and unopened): a reader still holding an older map would
/// otherwise lose data under it.
pub async fn reclaim(state: &mut FsState, file_ino: u64, current: Option<&SegmentMap>, lease: WriteLease) -> Result<usize> {
    let (live_objects, live_map) = match current {
        Some(map) => (
            referenced_objects(&load_all(&state.client, &mut state.segment_pages, map).await?),
            map.map_id,
        ),
        None => (Default::default(), 0),
    };
    reclaim_except(&state.client, file_ino, &live_objects, live_map, lease).await
}

/// Delete every object and map page `file_ino` recorded except the objects
/// in `live_objects` and the pages of `live_map` (0: none), with their
/// records. Needs only the client.
pub async fn reclaim_except(
    client: &ClusterClient,
    file_ino: u64,
    live_objects: &std::collections::BTreeSet<u64>,
    live_map: u64,
    lease: WriteLease,
) -> Result<usize> {
    let prefix = key::segc_prefix(file_ino);
    let mut from = prefix.clone();
    let mut reclaimed = 0;
    loop {
        let r = client.range(&prefix, &from, 1024).await.map_err(|e| anyhow!("KV range: {e}"))?;
        let has_more = r.has_more;
        let keys: Vec<Vec<u8>> = r.entries.into_iter().map(|e| e.key).collect();
        let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        let values = client.get_many(&refs).await;
        let mut done: Vec<&[u8]> = Vec::new();
        for (k, v) in keys.iter().zip(values) {
            let Some((_, id)) = key::parse_segc_key(k) else { continue };
            let Some(bytes) = v.map_err(|e| anyhow!("reclaim record {file_ino}/{id}: {e}"))? else { continue };
            let rec = schema::decode_segc(&bytes).map_err(|e| anyhow!("reclaim record {file_ino}/{id}: {e}"))?;
            match rec {
                SegcRecord::Object { len, lanes, unit } if !live_objects.contains(&id) => {
                    delete_object(client, id, len, lanes, unit, lease).await?;
                }
                SegcRecord::Pages { count } if id != live_map => {
                    delete_pages(client, id, count, lease).await?;
                }
                _ => continue,
            }
            done.push(k);
        }
        // A record goes only after what it names, so a crash in between
        // leaves the record to retry.
        for r in client.delete_many_fenced(&done, lease).await {
            r.map_err(|e| anyhow!("reclaim records of {file_ino}: {e}"))?;
        }
        reclaimed += done.len();
        match (has_more, keys.last()) {
            (true, Some(last)) => from = crate::dir::name_successor(last),
            _ => break,
        }
    }
    Ok(reclaimed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(off: u64, len: u64, data_ino: u64, data_off: u64) -> Segment {
        Segment { off, len, data_ino, data_off, lanes: 4, unit: 16 }
    }

    /// Expand a map to the per-byte (data_ino, data_off) it names, for
    /// brute-force comparison.
    fn bytes_of(segs: &[Segment], size: u64) -> Vec<Option<(u64, u64)>> {
        (0..size)
            .map(|b| {
                segs.iter()
                    .find(|s| s.off <= b && b < s.end())
                    .map(|s| (s.data_ino, s.data_off + (b - s.off)))
            })
            .collect()
    }

    #[test]
    fn splice_matches_a_byte_model_everywhere() {
        let base = vec![seg(0, 10, 1, 0), seg(10, 10, 2, 5), seg(25, 5, 3, 0)];
        for off in 0..32u64 {
            for len in 1..(34 - off) {
                let new = seg(off, len, 9, 100);
                let got = splice(&base, off, len, Some(new));
                let mut want = bytes_of(&base, 40);
                for b in off..off + len {
                    want[b as usize] = Some((9, 100 + (b - off)));
                }
                assert_eq!(bytes_of(&got, 40), want, "off={off} len={len}");
                assert!(got.windows(2).all(|w| w[0].end() <= w[1].off), "sorted, disjoint");
                // A hole instead.
                let got = splice(&base, off, len, None);
                let mut want = bytes_of(&base, 40);
                for b in off..off + len {
                    want[b as usize] = None;
                }
                assert_eq!(bytes_of(&got, 40), want, "hole off={off} len={len}");
            }
        }
    }

    #[test]
    fn clip_then_extend_exposes_no_old_bytes() {
        let base = vec![seg(0, 10, 1, 0), seg(10, 10, 2, 0)];
        let clipped = clip(&base, 13);
        assert_eq!(clipped, vec![seg(0, 10, 1, 0), seg(10, 3, 2, 0)]);
        // After growing back to 20 the bytes 13.. are a hole, not object 2.
        assert_eq!(bytes_of(&clipped, 20)[13..], vec![None; 7][..]);
    }

    #[test]
    fn read_plans_cover_exactly_the_requested_bytes() {
        let map = vec![seg(0, 10, 1, 0), seg(10, 10, 2, 5), seg(25, 40, 3, 7)];
        for start in 0..70u64 {
            for end in start..70u64 {
                let mut buf = vec![None; (end - start) as usize];
                for r in plan_reads(&map, start, end) {
                    assert!(r.offset as u64 + r.length as u64 <= 16, "inside one unit");
                    for i in 0..r.length as usize {
                        assert!(buf[r.dest_offset + i].is_none(), "no overlap");
                        buf[r.dest_offset + i] = Some((r.key.clone(), r.offset as usize + i));
                    }
                }
                let want = bytes_of(&map, 70)[start as usize..end as usize].to_vec();
                for (i, w) in want.iter().enumerate() {
                    match (w, &buf[i]) {
                        (None, None) => {}
                        (Some((ino, doff)), Some((k, in_off))) => {
                            let unit_off = doff / 16 * 16;
                            assert_eq!(k, &key::data_extent_key(*ino, unit_off, 4, 16));
                            assert_eq!(*in_off as u64, doff - unit_off);
                        }
                        other => panic!("start={start} end={end} byte {i}: {other:?}"),
                    }
                }
            }
        }
    }

    #[test]
    fn object_lanes_rotate_with_the_object() {
        // Consecutive single-unit objects (5 MiB parts) spread across lanes.
        let lanes: std::collections::BTreeSet<u8> =
            (100..124u64).map(|ino| key::data_extent_key(ino, 0, 24, 8 << 20)[1]).collect();
        assert_eq!(lanes.len(), 24);
        assert_eq!(object_extents(7, 0, 4, 16), vec![]);
        assert_eq!(object_extents(7, 33, 4, 16).len(), 3);
    }
}
