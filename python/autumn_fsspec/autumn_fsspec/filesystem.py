"""``AutumnFileSystem`` — an fsspec filesystem over the autumn KV client.

Registered under the ``autumn`` protocol, so ``fsspec.filesystem("autumn",
manager=...)`` and ``autumn://bucket/path`` URLs resolve here, and libraries
that speak fsspec — HuggingFace ``datasets`` / ``huggingface_hub``, pandas,
pyarrow — read and write autumn directly.

Design (see ``_layout``): a file is a small JSON *manifest* key plus N *chunk*
keys of ``chunk_size`` bytes (default 8 MiB, matching autumn-fuse's
``MAX_EXTENT`` and comfortably under the 64 MiB inline-put cap). Directories
are implicit (derived from descendant manifests, s3fs-style), with an optional
explicit marker so empty dirs created via ``makedirs`` still exist.

Reads fan a multi-chunk range out through ``batch_get_into`` (pipelined,
zero-copy-eligible since chunks are ≥ 64 KiB) into exactly-sized buffers whose
lengths are known from the manifest.
"""

from __future__ import annotations

import os
import time

from fsspec.spec import AbstractBufferedFile, AbstractFileSystem

from . import _bridge, _layout

DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MiB — autumn-fuse MAX_EXTENT; ZC-eligible
_SCAN_PAGE = 1024  # manifests per range() page during listing


class AutumnFileSystem(AbstractFileSystem):
    """fsspec filesystem backed by an autumn cluster.

    Parameters
    ----------
    manager : str
        Comma-separated ``host:port`` list of autumn manager(s). Falls back to
        the ``AUTUMN_MANAGER`` environment variable. (There is no way to encode
        the manager in an ``autumn://`` URL — the netloc/path is the object
        path — so pass it via ``storage_options``.)
    root : str, optional
        A path prefix ("bucket") transparently prepended to every path. Lets
        several independent namespaces share one cluster. Default "".
    transport : str, optional
        ``"ucx"`` | ``"tcp"``. Process-global (``autumn.set_transport``); set it
        before the first client in the process connects. Default: leave autumn's
        own default (tcp) untouched.
    chunk_size : int, optional
        Bytes per data chunk for files written through this fs. Default 8 MiB.
    """

    protocol = "autumn"
    root_marker = ""

    def __init__(
        self,
        manager=None,
        root="",
        transport=None,
        chunk_size=DEFAULT_CHUNK_SIZE,
        _client=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.manager = manager or os.environ.get("AUTUMN_MANAGER")
        self.root = (root or "").strip("/")
        self.chunk_size = int(chunk_size)

        if _client is not None:
            self._client = _client  # injected (tests / advanced embedding)
            return

        if not self.manager:
            raise ValueError(
                "AutumnFileSystem needs a manager address — pass manager=... in "
                "storage_options or set AUTUMN_MANAGER"
            )
        import autumn

        if transport:
            try:
                autumn.set_transport(transport)
            except Exception as e:  # warn, never hard-fail on transport (UCX rule)
                import warnings

                warnings.warn(f"autumn.set_transport({transport!r}) failed: {e!r}")
        self._client = _bridge.run(lambda: autumn.Client.connect(self.manager))

    # ── path handling ──────────────────────────────────────────────────────

    @classmethod
    def _strip_protocol(cls, path):
        path = super()._strip_protocol(path)
        return path.lstrip("/")

    def _norm(self, path):
        return self._strip_protocol(path).strip("/")

    # ── KV helpers (all block on the bridge loop) ──────────────────────────

    def _get_manifest(self, path):
        blob = _bridge.run(lambda: self._client.get(_layout.manifest_key(self.root, path)))
        return None if blob is None else _layout.parse_manifest(blob)

    def _scan_keys(self, prefix):
        """Yield every KEY under `prefix`, paginated.

        autumn's `range` is a keys-only scan (the server never ships values in
        a range response — rpc_handlers.rs), so listing reads keys here and
        fetches the manifests it needs with `get`/`_multi_get`."""
        start = b""
        while True:
            batch = _bridge.run(lambda s=start: self._client.range(prefix, s, _SCAN_PAGE))
            if not batch:
                return
            for key, _val in batch:
                yield bytes(key)
            if len(batch) < _SCAN_PAGE:
                return
            start = bytes(batch[-1][0]) + b"\x00"  # lossless successor cursor

    def _multi_get(self, keys):
        """Fetch several keys concurrently (one pipelined round of gets on the
        compio thread). Returns values aligned to `keys` (None for missing)."""
        if not keys:
            return []
        import asyncio

        async def _gather():
            return await asyncio.gather(*[self._client.get(k) for k in keys])

        return _bridge.run(_gather)

    def _has_children(self, path):
        prefix = _layout.children_prefix(self.root, path)
        batch = _bridge.run(lambda: self._client.range(prefix, b"", 1))
        return len(batch) > 0

    # ── metadata surface ───────────────────────────────────────────────────

    def info(self, path, **kwargs):
        path = self._norm(path)
        if path == "":
            return {"name": "", "size": 0, "type": "directory"}
        m = self._get_manifest(path)
        if m is not None:
            if m["t"] == "f":
                return {
                    "name": path,
                    "size": int(m["s"]),
                    "type": "file",
                    "chunk_size": int(m["cs"]),
                    "mtime": m.get("m"),
                }
            return {"name": path, "size": 0, "type": "directory"}
        if self._has_children(path):
            return {"name": path, "size": 0, "type": "directory"}
        raise FileNotFoundError(path)

    def ls(self, path, detail=True, **kwargs):
        path = self._norm(path)
        prefix = _layout.children_prefix(self.root, path)
        dirs = set()  # child names proven to be directories (have descendants)
        direct = {}  # child name -> its own manifest key (a file or empty-dir marker)
        for key in self._scan_keys(prefix):
            rel = key[len(prefix):].decode("utf-8")
            if not rel:
                continue
            seg, sep, _rest = rel.partition("/")
            child = f"{path}/{seg}" if path else seg
            if sep:  # a descendant lives deeper → `child` is a directory
                dirs.add(child)
            else:  # `key` is `child`'s own manifest; fetch it to learn type/size
                direct[child] = key

        names = list(direct)
        blobs = self._multi_get([direct[n] for n in names])
        children = {}
        for name, blob in zip(names, blobs):
            if blob is None:  # raced a delete
                continue
            m = _layout.parse_manifest(blob)
            if m["t"] == "f" and name not in dirs:
                children[name] = {
                    "name": name,
                    "size": int(m["s"]),
                    "type": "file",
                    "mtime": m.get("m"),
                }
            else:
                children[name] = {"name": name, "size": 0, "type": "directory"}
        for d in dirs:
            children.setdefault(d, {"name": d, "size": 0, "type": "directory"})

        if not children:
            # No descendants: `path` is either a file, an empty explicit dir, or
            # absent. info() distinguishes (and raises FileNotFoundError).
            self_info = self.info(path) if path else {"name": "", "type": "directory"}
            if self_info["type"] == "file":
                return [self_info] if detail else [self_info["name"]]
            return []

        result = sorted(children.values(), key=lambda d: d["name"])
        return result if detail else [d["name"] for d in result]

    # ── reads ──────────────────────────────────────────────────────────────

    def cat_file(self, path, start=None, end=None, **kwargs):
        path = self._norm(path)
        m = self._get_manifest(path)
        if m is None or m["t"] != "f":
            raise FileNotFoundError(path)
        size, cs, n = int(m["s"]), int(m["cs"]), int(m["n"])

        s = 0 if start is None else (start if start >= 0 else size + start)
        e = size if end is None else (end if end >= 0 else size + end)
        s = max(0, min(s, size))
        e = max(s, min(e, size))
        if e <= s:
            return b""

        first, last = s // cs, (e - 1) // cs
        idxs = list(range(first, last + 1))
        sizes = [cs if i < n - 1 else size - (n - 1) * cs for i in idxs]
        data = self._read_chunks(path, idxs, sizes)
        off = s - first * cs
        return data[off : off + (e - s)]

    def _read_chunks(self, path, idxs, sizes):
        """Fetch `idxs` chunks (each of known exact `sizes`) and concatenate.

        Uses `batch_get_into` — one pipelined, zero-copy-eligible round trip
        into pre-sized buffers. Any chunk the batch reports as a miss (size
        drift / transient) falls back to a plain `get`."""
        keys = [_layout.chunk_key(self.root, path, i) for i in idxs]
        bufs = [bytearray(sz) for sz in sizes]
        oks = _bridge.run(lambda: self._client.batch_get_into(keys, bufs))
        out = bytearray()
        for j, ok in enumerate(oks):
            if ok:
                out += bufs[j]
                continue
            v = _bridge.run(lambda k=keys[j]: self._client.get(k))
            if v is None:
                raise IOError(f"missing chunk {idxs[j]} of {path!r}")
            out += bytes(v)
        return bytes(out)

    # ── writes ─────────────────────────────────────────────────────────────

    def pipe_file(self, path, value, **kwargs):
        """Write a whole small/medium object in one shot (used by fsspec's
        `cat`/`pipe`). Splits into chunk_size pieces + writes the manifest."""
        path = self._norm(path)
        old_n = self._file_nchunks(path)  # for stale-tail reaping on overwrite
        cs = self.chunk_size
        n = 0
        for off in range(0, len(value), cs):
            chunk = bytes(value[off : off + cs])
            key = _layout.chunk_key(self.root, path, n)
            _bridge.run(lambda k=key, c=chunk: self._client.put(k, c))
            n += 1
        self._write_manifest(path, len(value), cs, n)
        self._reap_chunks(path, n, old_n)

    def _file_nchunks(self, path):
        """Chunk count of the file currently at `path` (0 if absent/dir)."""
        m = self._get_manifest(path)
        return int(m["n"]) if m is not None and m["t"] == "f" else 0

    def _reap_chunks(self, path, keep_n, old_n):
        """Delete data chunks with index in [keep_n, old_n) — the tail an
        overwrite left unreferenced. No-op for the common write-once /
        grow cases (old_n <= keep_n)."""
        if old_n <= keep_n:
            return
        keys = [_layout.chunk_key(self.root, path, i) for i in range(keep_n, old_n)]

        async def _del_all():
            import asyncio

            await asyncio.gather(*[self._client.delete(k) for k in keys])

        _bridge.run(_del_all)

    def _put_chunk(self, path, idx, data):
        key = _layout.chunk_key(self.root, path, idx)
        _bridge.run(lambda: self._client.put(key, bytes(data)))

    def _write_manifest(self, path, size, chunk_size, nchunks):
        key = _layout.manifest_key(self.root, path)
        blob = _layout.file_manifest(size, chunk_size, nchunks, time.time())
        _bridge.run(lambda: self._client.put(key, blob))
        self.invalidate_cache(self._parent(path))

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        path = self._norm(path)
        if "r" in mode:
            m = self._get_manifest(path)
            if m is None or m["t"] != "f":
                raise FileNotFoundError(path)
            return AutumnBufferedFile(
                self,
                path,
                mode=mode,
                size=int(m["s"]),
                block_size=block_size or int(m["cs"]),
                cache_options=cache_options,
            )

        # Write modes. Honor fsspec/stdio semantics rather than treating every
        # non-read mode as a plain overwrite:
        #   xb — exclusive create: fail if the object already exists
        #   ab — append: continue writing at the end of the existing object
        #   wb — overwrite (default)
        if not autocommit:
            # fsspec transactions (`with fs.transaction:`) expect deferred
            # commit + discard-on-failure. This backend writes chunks to the
            # final keys as it goes — promising transactional semantics we
            # don't have would be a silent lie (coco P3).
            raise NotImplementedError("autumn:// does not support transactions (autocommit=False)")
        existing = self._get_manifest(path)
        if "x" in mode and existing is not None:
            raise FileExistsError(path)
        append = "a" in mode
        if append and existing is not None and existing["t"] == "f":
            # keep the existing chunk_size so the uniform-chunk layout holds
            block_size = int(existing["cs"])
        return AutumnBufferedFile(
            self,
            path,
            mode=mode,
            block_size=block_size or self.chunk_size,
            cache_options=cache_options,
            append=append,
        )

    # ── namespace mutation ─────────────────────────────────────────────────

    def makedirs(self, path, exist_ok=True):
        path = self._norm(path)
        if not path:
            return
        # File and dir manifests share one key — an unconditional put would
        # CLOBBER an existing file's manifest (data invisible + orphan
        # chunks, coco P2). Check what's there first.
        existing = self._get_manifest(path)
        if existing is not None:
            if existing["t"] == "f":
                raise NotADirectoryError(path)
            if not exist_ok:
                raise FileExistsError(path)
            return  # already a directory
        key = _layout.manifest_key(self.root, path)
        _bridge.run(lambda: self._client.put(key, _layout.dir_manifest(time.time())))
        self.invalidate_cache(self._parent(path))

    def mkdir(self, path, create_parents=True, **kwargs):
        # POSIX mkdir: an existing target is an error (makedirs is the
        # exist_ok variant). Implicit dirs mean create_parents is moot.
        self.makedirs(path, exist_ok=False)

    def rm_file(self, path):
        path = self._norm(path)
        _bridge.run(lambda: self._client.batch_delete(_layout.chunk_prefix(self.root, path)))
        _bridge.run(lambda: self._client.delete(_layout.manifest_key(self.root, path)))
        self.invalidate_cache(self._parent(path))

    def rm(self, path, recursive=False, maxdepth=None):
        paths = path if isinstance(path, list) else [path]
        for p in paths:
            p = self._norm(p)
            if recursive:
                meta_pref, data_pref = _layout.subtree_prefixes(self.root, p)
                _bridge.run(lambda pr=meta_pref: self._client.batch_delete(pr))
                _bridge.run(lambda pr=data_pref: self._client.batch_delete(pr))
            self.rm_file(p)
        self.invalidate_cache()

    def rmdir(self, path):
        self.rm(path, recursive=True)

    def created(self, path):
        return self.modified(path)

    def modified(self, path):
        info = self.info(path)
        mtime = info.get("mtime")
        if mtime is None:
            raise FileNotFoundError(path)
        from datetime import datetime, timezone

        return datetime.fromtimestamp(mtime, tz=timezone.utc)


class AutumnBufferedFile(AbstractBufferedFile):
    """Buffered read/write file. Reads delegate ranged fetches to
    ``cat_file``; writes accumulate and are emitted as fixed ``chunk_size``
    chunks so the offset arithmetic in ``cat_file`` stays exact."""

    def __init__(
        self, fs, path, mode="rb", block_size=None, cache_options=None, size=None, append=False, **kwargs
    ):
        self._chunk_size = block_size
        self._append = append
        super().__init__(
            fs,
            path,
            mode=mode,
            block_size=block_size,
            cache_options=cache_options or {},
            size=size,
            **kwargs,
        )

    # reads
    def _fetch_range(self, start, end):
        return self.fs.cat_file(self.path, start, end)

    # writes
    def _initiate_upload(self):
        cs = self._chunk_size
        old = self.fs._get_manifest(self.path)
        self._old_n = int(old["n"]) if old is not None and old["t"] == "f" else 0
        if self._append and old is not None and old["t"] == "f":
            # Resume at the end of the existing object. Re-load the trailing
            # partial chunk into the pending buffer so the uniform-chunk-size
            # invariant (every chunk but the last is exactly `cs`) still holds.
            size = int(old["s"])
            full = size // cs
            tail = size - full * cs
            if tail:
                last = self.fs._read_chunks(self.path, [full], [tail])
                self._pending = bytearray(last)
                self._nchunks = full
                self._total = full * cs
            else:
                self._pending = bytearray()
                self._nchunks = full
                self._total = size
        else:
            self._pending = bytearray()
            self._nchunks = 0
            self._total = 0

    def _upload_chunk(self, final=False):
        cs = self._chunk_size
        self._pending += self.buffer.getvalue()
        while len(self._pending) >= cs:
            self.fs._put_chunk(self.path, self._nchunks, self._pending[:cs])
            del self._pending[:cs]
            self._nchunks += 1
            self._total += cs
        if final:
            if self._pending:
                self.fs._put_chunk(self.path, self._nchunks, self._pending)
                self._total += len(self._pending)
                self._nchunks += 1
                self._pending = bytearray()
            self.fs._write_manifest(self.path, self._total, cs, self._nchunks)
            # reap chunks orphaned by an overwrite that shrank the file
            self.fs._reap_chunks(self.path, self._nchunks, self._old_n)
        return True
