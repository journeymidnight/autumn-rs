//! Apache object_store backend for Autumn's ordered KV service.
//!
//! Each upload writes immutable, generation-qualified 4 MiB chunks. A small
//! metadata KV publishes the object atomically; conditional puts use the PS's
//! compare-and-put operation. Readers retain their metadata snapshot across
//! replacement and deletion. Payload reclamation requires a quiescent vacuum.
mod bridge;

use std::collections::BTreeSet;
use std::fmt;
use std::ops::Range;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::{
    stream::{self, BoxStream},
    FutureExt, StreamExt, TryStreamExt,
};
use object_store::{path::Path, *};
use serde::{Deserialize, Serialize};

const CHUNK_SIZE: usize = 4 * 1024 * 1024;
const PAGE_SIZE: u32 = 256;
const META_LIMIT: usize = 64 * 1024;

pub(crate) fn error(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Error {
    Error::Generic {
        store: "Autumn",
        source: source.into(),
    }
}

fn missing(path: &Path) -> Error {
    Error::NotFound {
        path: path.to_string(),
        source: "object does not exist".into(),
    }
}

fn conflict(path: &Path, create: bool) -> Error {
    if create {
        Error::AlreadyExists {
            path: path.to_string(),
            source: "object already exists".into(),
        }
    } else {
        Error::Precondition {
            path: path.to_string(),
            source: "object generation changed".into(),
        }
    }
}

fn check_attributes(attributes: &Attributes) -> Result<()> {
    if !attributes.is_empty() {
        return Err(Error::NotSupported {
            source: "Autumn object attributes are not supported".into(),
        });
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Metadata {
    format: u8,
    generation: String,
    modified: DateTime<Utc>,
    /// Multipart part lengths; each part is stored as fixed 4 MiB chunks.
    parts: Vec<u64>,
}

impl Metadata {
    fn new(parts: Vec<u64>) -> Self {
        Self {
            format: 1,
            generation: uuid::Uuid::new_v4().to_string(),
            modified: Utc::now(),
            parts,
        }
    }
    fn size(&self) -> u64 {
        self.parts.iter().sum()
    }
    fn encode(&self) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec(self).map_err(error)?;
        if bytes.len() > META_LIMIT {
            return Err(error("too many multipart parts (metadata exceeds 64 KiB)"));
        }
        Ok(bytes)
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        let meta: Self = serde_json::from_slice(bytes).map_err(error)?;
        if meta.format != 1
            || uuid::Uuid::parse_str(&meta.generation).is_err()
            || meta
                .parts
                .iter()
                .try_fold(0u64, |sum, &n| sum.checked_add(n))
                .is_none()
        {
            return Err(error("invalid Autumn object metadata"));
        }
        Ok(meta)
    }
    fn object_meta(&self, location: Path) -> ObjectMeta {
        ObjectMeta {
            location,
            last_modified: self.modified,
            size: self.size(),
            e_tag: Some(self.generation.clone()),
            version: None,
        }
    }
    fn result(&self) -> PutResult {
        PutResult {
            e_tag: Some(self.generation.clone()),
            version: None,
            extensions: Default::default(),
        }
    }
}

fn meta_key(path: &Path) -> Vec<u8> {
    format!("m/{path}").into_bytes()
}
fn chunk_key(generation: &str, part: usize, chunk: u64) -> Vec<u8> {
    format!("d/{generation}/{part:08x}/{chunk:016x}").into_bytes()
}

/// A Send + Sync object store backed by a scoped Autumn client.
///
/// The scope must belong to an existing Autumn namespace and be dedicated to
/// this format. Multiple independent instances can share the same scope.
#[derive(Clone)]
pub struct AutumnObjectStore {
    bridge: bridge::Bridge,
    scope: String,
}

impl fmt::Debug for AutumnObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AutumnObjectStore")
            .field("scope", &self.scope)
            .finish()
    }
}
impl fmt::Display for AutumnObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Autumn({})", self.scope)
    }
}

impl AutumnObjectStore {
    /// Connect using the manager address and an existing namespace/sub-prefix.
    pub async fn connect(manager: impl Into<String>, scope: impl Into<String>) -> Result<Self> {
        Self::connect_inner(manager.into(), scope.into(), None).await
    }

    /// Connect with the credential returned by Autumn's principal provisioning.
    pub async fn connect_with_credential(
        manager: impl Into<String>,
        scope: impl Into<String>,
        principal: impl Into<String>,
        credential: Vec<u8>,
    ) -> Result<Self> {
        Self::connect_inner(
            manager.into(),
            scope.into(),
            Some((principal.into(), credential)),
        )
        .await
    }

    async fn connect_inner(
        manager: String,
        scope: String,
        credential: Option<(String, Vec<u8>)>,
    ) -> Result<Self> {
        let bridge = bridge::Bridge::connect(manager, scope.clone(), credential).await?;
        Ok(Self { bridge, scope })
    }

    async fn read_metadata(&self, path: &Path) -> Result<Option<(Metadata, Vec<u8>)>> {
        let key = meta_key(path);
        self.bridge
            .call(move |c| async move { c.get(&key).await }.boxed_local())
            .await?
            .map(|raw| Ok((Metadata::decode(&raw)?, raw)))
            .transpose()
    }

    async fn publish(&self, path: &Path, meta: &Metadata, mode: PutMode) -> Result<PutResult> {
        let bytes = meta.encode()?;
        let key = meta_key(path);
        match mode {
            PutMode::Overwrite => {
                self.bridge
                    .call(move |c| async move { c.put(&key, &bytes).await }.boxed_local())
                    .await?;
            }
            mode => {
                let create = matches!(mode, PutMode::Create);
                let expected = match mode {
                    PutMode::Create => None,
                    PutMode::Update(version) => {
                        if version.version.is_some() || version.e_tag.is_none() {
                            return Err(error("Update requires the ETag returned by Autumn; version IDs are unsupported"));
                        }
                        let (current, raw) = self
                            .read_metadata(path)
                            .await?
                            .ok_or_else(|| conflict(path, false))?;
                        if version.e_tag.as_deref() != Some(&current.generation) {
                            return Err(conflict(path, false));
                        }
                        Some(raw)
                    }
                    PutMode::Overwrite => unreachable!(),
                };
                let success = self
                    .bridge
                    .call(move |c| {
                        async move { c.compare_put(&key, expected.as_deref(), &bytes).await }
                            .boxed_local()
                    })
                    .await?;
                if !success {
                    // A routing retry after a lost ACK may observe our own
                    // committed generation. Only that unique generation is success.
                    let committed = self
                        .read_metadata(path)
                        .await?
                        .is_some_and(|(current, _)| current.generation == meta.generation);
                    if !committed {
                        return Err(conflict(path, create));
                    }
                }
            }
        }
        Ok(meta.result())
    }

    async fn write_part(&self, generation: String, part: usize, payload: PutPayload) -> Result<()> {
        self.bridge
            .call(move |c| {
                async move {
                    let mut pending = BytesMut::new();
                    let mut index = 0;
                    for mut bytes in payload {
                        while !bytes.is_empty() {
                            // Already aligned buffers take the zero-copy bulk path.
                            let chunk = if pending.is_empty() && bytes.len() >= CHUNK_SIZE {
                                Some(bytes.split_to(CHUNK_SIZE))
                            } else {
                                let n = (CHUNK_SIZE - pending.len()).min(bytes.len());
                                pending.extend_from_slice(&bytes.split_to(n));
                                if pending.len() == CHUNK_SIZE {
                                    Some(pending.split().freeze())
                                } else {
                                    None
                                }
                            };
                            if let Some(chunk) = chunk {
                                c.put_bulk(&chunk_key(&generation, part, index), chunk)
                                    .await?;
                                index += 1;
                            }
                        }
                    }
                    if !pending.is_empty() {
                        c.put_bulk(&chunk_key(&generation, part, index), pending.freeze())
                            .await?;
                    }
                    Ok(())
                }
                .boxed_local()
            })
            .await
    }

    fn read_chunks(&self, meta: Metadata, range: Range<u64>) -> BoxStream<'static, Result<Bytes>> {
        let mut pieces = Vec::new();
        let mut start = 0;
        for (part, &length) in meta.parts.iter().enumerate() {
            let end = start + length;
            let lo = range.start.max(start);
            let hi = range.end.min(end);
            if lo < hi {
                let mut offset = lo - start;
                while offset < hi - start {
                    let chunk = offset / CHUNK_SIZE as u64;
                    let within = (offset % CHUNK_SIZE as u64) as u32;
                    let len = ((hi - start - offset).min(CHUNK_SIZE as u64 - within as u64)) as u32;
                    pieces.push((chunk_key(&meta.generation, part, chunk), within, len));
                    offset += len as u64;
                }
            }
            start = end;
        }
        let bridge = self.bridge.clone();
        stream::iter(pieces)
            .map(move |(key, offset, len)| {
                let bridge = bridge.clone();
                async move {
                    let bytes = bridge
                        .call(move |c| {
                            async move {
                                c.get_range_pooled(&key, offset, len)
                                    .await
                                    .map(|v| v.map(|b| b.freeze()))
                            }
                            .boxed_local()
                        })
                        .await?
                        .ok_or_else(|| error("published object chunk is missing"))?;
                    if bytes.len() != len as usize {
                        return Err(error("published object chunk is truncated"));
                    }
                    Ok(bytes)
                }
            })
            .buffered(4)
            .boxed()
    }

    fn scan(
        &self,
        prefix: Vec<u8>,
        start: Vec<u8>,
    ) -> BoxStream<'static, Result<(Vec<u8>, Vec<u8>)>> {
        let bridge = self.bridge.clone();
        stream::try_unfold(
            (bridge, prefix, start, false),
            |(bridge, prefix, cursor, done)| async move {
                if done {
                    return Ok::<_, Error>(None);
                }
                let p = prefix.clone();
                let cursor = cursor.max(prefix.clone());
                let page = bridge
                    .call(move |c| {
                        async move {
                            let mut page = c.range(&p, &cursor, PAGE_SIZE).await?;
                            // Autumn RANGE deliberately returns keys only. Fetch this page's
                            // small metadata in one batch, without fetching payload chunks.
                            if p.starts_with(b"m/") {
                                let keys: Vec<_> =
                                    page.entries.iter().map(|e| e.key.as_slice()).collect();
                                let values = c.get_many(&keys).await;
                                for (entry, value) in page.entries.iter_mut().zip(values) {
                                    entry.value = value?.unwrap_or_default();
                                }
                            }
                            Ok(page)
                        }
                        .boxed_local()
                    })
                    .await?;
                let mut next = page
                    .entries
                    .last()
                    .map(|e| e.key.clone())
                    .unwrap_or_default();
                next.push(0);
                let done = !page.has_more || page.entries.is_empty();
                let metadata = prefix.starts_with(b"m/");
                let entries = stream::iter(
                    page.entries
                        .into_iter()
                        .filter(move |e| !metadata || !e.value.is_empty())
                        .map(|e| Ok((e.key, e.value))),
                );
                Ok(Some((entries, (bridge, prefix, next, done))))
            },
        )
        .try_flatten()
        .boxed()
    }

    /// Reclaim unpublished, replaced and deleted payloads. ALL readers and
    /// writers of this scope must be stopped for the entire call, including
    /// readers holding a GetResult stream. This is an offline operation.
    pub async fn vacuum_quiescent(&self) -> Result<u64> {
        let mut live = BTreeSet::new();
        let mut metas = self.scan(b"m/".to_vec(), Vec::new());
        while let Some((_, value)) = metas.try_next().await? {
            live.insert(Metadata::decode(&value)?.generation);
        }
        let mut count = 0;
        let mut chunks = self.scan(b"d/".to_vec(), Vec::new());
        while let Some((key, _)) = chunks.try_next().await? {
            let generation = std::str::from_utf8(&key)
                .map_err(error)?
                .split('/')
                .nth(1)
                .ok_or_else(|| error("invalid chunk key"))?;
            if !live.contains(generation) {
                self.bridge
                    .call(move |c| async move { c.delete(&key).await }.boxed_local())
                    .await?;
                count += 1;
            }
        }
        Ok(count)
    }
}

#[async_trait]
impl ObjectStore for AutumnObjectStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        check_attributes(&options.attributes)?;
        let meta = Metadata::new(vec![payload.content_length() as u64]);
        self.write_part(meta.generation.clone(), 0, payload).await?;
        self.publish(path, &meta, options.mode).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        check_attributes(&options.attributes)?;
        Ok(Box::new(Upload {
            store: self.clone(),
            path: path.clone(),
            meta: Metadata::new(Vec::new()),
            done: Vec::new(),
            closed: false,
        }))
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        if options.version.is_some() {
            return Err(Error::NotSupported {
                source: "historical version reads are unsupported".into(),
            });
        }
        let (meta, _) = self
            .read_metadata(path)
            .await?
            .ok_or_else(|| missing(path))?;
        let object_meta = meta.object_meta(path.clone());
        options.check_preconditions(&object_meta)?;
        let range = match options.range {
            Some(range) => range.as_range(meta.size()).map_err(error)?,
            None => 0..meta.size(),
        };
        let payload = if options.head {
            stream::empty().boxed()
        } else {
            self.read_chunks(meta, range.clone())
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(payload),
            meta: object_meta,
            range,
            attributes: Attributes::default(),
            extensions: Default::default(),
        })
    }

    async fn get_ranges(&self, path: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let (meta, _) = self
            .read_metadata(path)
            .await?
            .ok_or_else(|| missing(path))?;
        let mut results = Vec::with_capacity(ranges.len());
        for range in ranges {
            if range.start > range.end || range.end > meta.size() {
                return Err(error("range is outside the object"));
            }
            let chunks: Vec<_> = self
                .read_chunks(meta.clone(), range.clone())
                .try_collect()
                .await?;
            let mut bytes = BytesMut::new();
            for chunk in chunks {
                bytes.extend_from_slice(&chunk);
            }
            results.push(bytes.freeze());
        }
        Ok(results)
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let store = self.clone();
        paths
            .map(move |path| {
                let store = store.clone();
                async move {
                    let path = path?;
                    let key = meta_key(&path);
                    store
                        .bridge
                        .call(move |c| async move { c.delete(&key).await }.boxed_local())
                        .await?;
                    Ok(path)
                }
            })
            .buffered(16)
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.list_with_offset(prefix, &Path::default())
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.cloned().unwrap_or_default();
        let mut start = if offset.as_ref().is_empty() {
            Vec::new()
        } else {
            meta_key(offset)
        };
        if !start.is_empty() {
            start.push(0);
        }
        let mut scan_prefix = meta_key(&prefix);
        if !prefix.as_ref().is_empty() {
            scan_prefix.push(b'/');
        }
        self.scan(scan_prefix, start)
            .try_filter_map(move |(key, bytes)| {
                let prefix = prefix.clone();
                async move {
                    let path = Path::parse(std::str::from_utf8(&key[2..]).map_err(error)?)?;
                    let matches = prefix.as_ref().is_empty()
                        || path.as_ref().starts_with(&format!("{prefix}/"));
                    if matches {
                        Ok(Some(Metadata::decode(&bytes)?.object_meta(path)))
                    } else {
                        Ok(None)
                    }
                }
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix = prefix.cloned().unwrap_or_default();
        let mut objects = Vec::new();
        let mut common_prefixes = BTreeSet::new();
        let mut entries = self.list(Some(&prefix));
        while let Some(meta) = entries.try_next().await? {
            let rest = if prefix.as_ref().is_empty() {
                meta.location.as_ref()
            } else if meta.location == prefix {
                ""
            } else {
                &meta.location.as_ref()[prefix.as_ref().len() + 1..]
            };
            if let Some((directory, _)) = rest.split_once('/') {
                common_prefixes.insert(Path::parse(if prefix.as_ref().is_empty() {
                    directory.to_owned()
                } else {
                    format!("{prefix}/{directory}")
                })?);
            } else {
                objects.push(meta);
            }
        }
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
            extensions: Default::default(),
        })
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let (source, _) = self
            .read_metadata(from)
            .await?
            .ok_or_else(|| missing(from))?;
        // A new generation is necessary even for identical bytes: ETags identify
        // publications, and source deletion must not invalidate the copy.
        let mut target = Metadata::new(source.parts.clone());
        target.modified = Utc::now();
        let mut offset = 0;
        for (part, &length) in source.parts.iter().enumerate() {
            let chunks = self.read_chunks(source.clone(), offset..offset + length);
            let store = self.clone();
            let generation = target.generation.clone();
            chunks
                .enumerate()
                .map(move |(chunk, bytes)| {
                    let store = store.clone();
                    let generation = generation.clone();
                    async move {
                        let value = bytes?;
                        let key = chunk_key(&generation, part, chunk as u64);
                        store
                            .bridge
                            .call(move |c| {
                                async move { c.put_bulk(&key, value).await }.boxed_local()
                            })
                            .await
                    }
                })
                .buffered(4)
                .try_collect::<Vec<_>>()
                .await?;
            offset += length;
        }
        self.publish(
            to,
            &target,
            match options.mode {
                CopyMode::Create => PutMode::Create,
                CopyMode::Overwrite => PutMode::Overwrite,
            },
        )
        .await?;
        Ok(())
    }
}

#[derive(Debug)]
struct Upload {
    store: AutumnObjectStore,
    path: Path,
    meta: Metadata,
    done: Vec<Arc<AtomicBool>>,
    closed: bool,
}

#[async_trait]
impl MultipartUpload for Upload {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        if self.closed {
            return async { Err(error("upload is closed")) }.boxed();
        }
        let part = self.meta.parts.len();
        self.meta.parts.push(payload.content_length() as u64);
        let done = Arc::new(AtomicBool::new(false));
        self.done.push(done.clone());
        let store = self.store.clone();
        let generation = self.meta.generation.clone();
        async move {
            store.write_part(generation, part, payload).await?;
            done.store(true, Ordering::Release);
            Ok(())
        }
        .boxed()
    }
    async fn complete(&mut self) -> Result<PutResult> {
        if self.closed {
            return Err(error("upload is closed"));
        }
        if self.done.iter().any(|done| !done.load(Ordering::Acquire)) {
            return Err(error("upload has unfinished or failed parts"));
        }
        let result = self
            .store
            .publish(&self.path, &self.meta, PutMode::Overwrite)
            .await?;
        self.closed = true;
        Ok(result)
    }
    async fn abort(&mut self) -> Result<()> {
        if self.closed {
            return Err(error("upload is closed"));
        }
        self.closed = true;
        // Unpublished chunks are reclaimed by vacuum_quiescent, including any
        // part already in flight when abort was called.
        Ok(())
    }
}
