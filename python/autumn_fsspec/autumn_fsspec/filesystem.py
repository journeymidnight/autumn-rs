"""``AutumnFileSystem`` — an fsspec filesystem over the SHARED autumn inode layout.

Registered under the ``autumn`` protocol, so ``fsspec.filesystem("autumn",
manager=...)`` and ``autumn://bucket/path`` URLs resolve here, and libraries
that speak fsspec — HuggingFace ``datasets`` / ``huggingface_hub``, pandas,
pyarrow — read and write autumn directly.

F-FS-UNIFY M3: this is a thin facade over ``autumn.Fs`` (the PyO3 binding to
the shared fuser-free filesystem core, M2) — the SAME inode/dirent/extent layout
the ``autumn-fuse`` kernel mount uses. A file written here is visible and
byte-identical through a fuse mount (and vice versa). Real POSIX directories
(no s3fs-style implicit-dir emulation), real ``readdir`` (no keys-only-range
manifest dance), shared metadata.

The retired F-FSSPEC-1 layout (a private ``fs/``-prefixed, path-keyed chunk +
manifest scheme on the KV client) is GONE — it was deliberately separate from
the fuse namespace, so the two saw different files. This unifies them.

``autumn.Fs`` is synchronous (a dedicated compio worker owns the connection and
each method blocks), so this facade needs no asyncio bridge.
"""

from __future__ import annotations

import os

from fsspec.spec import AbstractBufferedFile, AbstractFileSystem

# Shared-core constants (mirror crates/fuse schema).
ROOT_INO = 1
DT_DIR = 4
DT_REG = 8
DT_LNK = 10

DEFAULT_BLOCK_SIZE = 8 * 1024 * 1024  # 8 MiB — matches autumn-fuse MAX_EXTENT
# Back-compat alias (was the chunk size of the retired layout).
DEFAULT_CHUNK_SIZE = DEFAULT_BLOCK_SIZE


class AutumnFileSystem(AbstractFileSystem):
    """fsspec filesystem backed by the shared autumn inode layout via ``autumn.Fs``.

    Parameters
    ----------
    manager : str
        Comma-separated ``host:port`` list of autumn manager(s). Falls back to
        the ``AUTUMN_MANAGER`` environment variable. (There is no way to encode
        the manager in an ``autumn://`` URL — the netloc/path is the object
        path — so pass it via ``storage_options``.)
    root : str, optional
        A path prefix ("bucket") — really just a subdirectory — transparently
        prepended to every path so several namespaces can share one cluster.
    transport : str, optional
        ``"ucx"`` | ``"tcp"``. Process-global (``autumn.set_transport``); set it
        before the first client in the process connects.
    host : str, optional
        Daemon lease identity host label (``DaemonClientId::new_fuse``).
    chunk_size / block_size : int, optional
        Default write block size for buffered files. Default 8 MiB.
    _fs : object, optional
        Injected ``autumn.Fs``-compatible backend (tests / advanced embedding).
    """

    protocol = "autumn"
    root_marker = ""

    def __init__(
        self,
        manager=None,
        root="",
        transport=None,
        host=None,
        chunk_size=DEFAULT_BLOCK_SIZE,
        _fs=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.root = (root or "").strip("/")
        self.block_size = int(chunk_size)

        if _fs is not None:
            self._fs = _fs  # injected backend (FakeFs offline / advanced embedding)
            return

        self.manager = manager or os.environ.get("AUTUMN_MANAGER")
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
        self._fs = autumn.Fs.connect(self.manager, host=host)

    # ── path handling ──────────────────────────────────────────────────────

    @classmethod
    def _strip_protocol(cls, path):
        path = super()._strip_protocol(path)
        return path.lstrip("/")

    def _norm(self, path):
        return self._strip_protocol(path).strip("/")

    def _full(self, path):
        """fs-relative path → full inode-tree path (with the bucket root)."""
        p = path.strip("/")
        if self.root:
            return f"{self.root}/{p}" if p else self.root
        return p

    def _split(self, path):
        """fs-relative path → (parent_rel, leaf_name)."""
        p = path.strip("/")
        parent, _sep, name = p.rpartition("/")
        return parent, name

    def _resolve(self, path):
        """fs-relative path → inode number, or None if absent."""
        full = self._full(path)
        if full == "":
            return ROOT_INO
        return self._fs.resolve(full)

    def _mkdirs(self, path):
        """Ensure every component of the (fs-relative) directory `path` exists;
        return the leaf directory's inode. Missing components are created.

        The ``lookup``→``mkdir`` sequence is not atomic, so a concurrent creator
        of the same parent (e.g. two writers under a shared new directory) can
        lose the race; treat ``mkdir``'s EEXIST idempotently by re-looking up."""
        full = self._full(path)
        ino = ROOT_INO
        if full == "":
            return ino
        for comp in full.split("/"):
            if not comp:
                continue
            child = self._fs.lookup(ino, comp)
            if child is None:
                try:
                    ino = self._fs.mkdir(ino, comp)
                except RuntimeError:
                    child = self._fs.lookup(ino, comp)  # raced → re-resolve
                    if child is None:
                        raise
                    cino, kind = child
                    if kind != DT_DIR:
                        raise NotADirectoryError(comp)
                    ino = cino
            else:
                cino, kind = child
                if kind != DT_DIR:
                    raise NotADirectoryError(comp)
                ino = cino
        return ino

    # ── metadata surface ───────────────────────────────────────────────────

    def _info_from_ino(self, name, ino):
        a = self._fs.getattr(ino)
        if a["type"] == "directory":
            return {"name": name, "size": 0, "type": "directory", "mtime": a.get("mtime")}
        return {
            "name": name,
            "size": int(a["size"]),
            "type": "file",
            "mtime": a.get("mtime"),
        }

    def info(self, path, **kwargs):
        path = self._norm(path)
        if path == "":
            # The fs root is a virtual directory that always exists, even before
            # a `root=` bucket dir has been physically created by a first write.
            return {"name": "", "size": 0, "type": "directory", "mtime": None}
        ino = self._resolve(path)
        if ino is None:
            raise FileNotFoundError(path)
        return self._info_from_ino(path, ino)

    def ls(self, path, detail=True, **kwargs):
        path = self._norm(path)
        ino = self._resolve(path)
        if ino is None:
            # A not-yet-created `root=` bucket lists as an empty root (don't
            # raise, and don't leak the cluster ROOT).
            if path == "":
                return []
            raise FileNotFoundError(path)
        a = self._fs.getattr(ino)
        if a["type"] != "directory":
            # ls of a file returns just that file (fsspec convention).
            self_info = self._info_from_ino(path, ino)
            return [self_info] if detail else [self_info["name"]]

        out = []
        for name, cino, kind in self._fs.readdir(ino):
            child = f"{path}/{name}" if path else name
            if kind == DT_DIR:
                out.append({"name": child, "size": 0, "type": "directory"})
            else:
                out.append(self._info_from_ino(child, cino))
        out.sort(key=lambda d: d["name"])
        return out if detail else [d["name"] for d in out]

    # ── reads ──────────────────────────────────────────────────────────────

    def cat_file(self, path, start=None, end=None, **kwargs):
        path = self._norm(path)
        ino = self._resolve(path)
        if ino is None:
            raise FileNotFoundError(path)
        a = self._fs.getattr(ino)
        if a["type"] == "directory":
            raise IsADirectoryError(path)
        size = int(a["size"])

        s = 0 if start is None else (start if start >= 0 else size + start)
        e = size if end is None else (end if end >= 0 else size + end)
        s = max(0, min(s, size))
        e = max(s, min(e, size))
        if e <= s:
            return b""
        # `Fs.read`'s size arg is u32 (the fuse read-size contract), so a range
        # larger than 4 GiB (whole-model reads) must be issued in bounded steps
        # and concatenated — a single `read(ino, s, e - s)` would overflow the
        # PyO3 u32 conversion. `block_size` (≤ 1 GiB cap) bounds each RPC.
        step = min(self.block_size, 1 << 30)
        out = bytearray()
        off = s
        while off < e:
            chunk = self._fs.read(ino, off, min(step, e - off))
            if not chunk:
                break  # defensive: EOF short read (shouldn't happen within [s,e))
            out += chunk
            off += len(chunk)
        return bytes(out)

    # ── writes ─────────────────────────────────────────────────────────────

    def _ensure_file_ino(self, path):
        """Resolve (or create) the file inode at fs-relative `path`, creating
        parent directories as needed. Returns the inode; raises if `path` is a
        directory.

        NOTE (F-FS-UNIFY M4): writes here are NOT yet fenced by a per-inode write
        lease — two writers (fsspec↔fsspec or fsspec↔fuse mount) to the same
        inode can interleave truncate/write/flush and lose data or mix content.
        M3 delivers shared visibility + read/write on one layout; the
        `acquire`/`heartbeat`/`release` lease wiring around the write lifecycle
        (and coherence) lands in M4. Single-writer use (datasets prep, uploads)
        is safe today."""
        parent, name = self._split(path)
        if not name:
            raise IsADirectoryError(path)
        parent_ino = self._mkdirs(parent)
        child = self._fs.lookup(parent_ino, name)
        if child is None:
            try:
                return self._fs.create(parent_ino, name)
            except RuntimeError:
                child = self._fs.lookup(parent_ino, name)  # raced → re-resolve
                if child is None:
                    raise
        ino, kind = child
        if kind == DT_DIR:
            raise IsADirectoryError(path)
        return ino

    def _acquire_write(self, ino):
        """F-FS-UNIFY M4: take the per-inode WRITE lease before mutating `ino`
        so two writers (fsspec↔fsspec or fsspec↔fuse mount) can't corrupt each
        other. A conflict (another client holds it) surfaces as BlockingIOError.
        The binding's background task heartbeats the lease for long writes."""
        try:
            self._fs.acquire(ino, "w")
        except RuntimeError as e:
            raise BlockingIOError(f"write lease unavailable (another writer?): {e}")

    def _release_write(self, ino):
        """Release the WRITE lease and evict the inode from the binding cache
        (close-to-open coherence for the next cross-client read)."""
        try:
            self._fs.release(ino)
        finally:
            self._fs.forget(ino)

    def _verify_write_lease(self, ino):
        """Confirm we still hold the WRITE lease before a durable commit. A
        background invalidation poll can drop it (preemption / a manager blip
        that released it), after which a write would be UN-fenced — so gate the
        `flush` on a live heartbeat and raise if it's gone (coco M4 P1)."""
        try:
            live = self._fs.heartbeat(ino)
        except RuntimeError as e:
            raise IOError(f"write-lease heartbeat failed for inode {ino}: {e}")
        if not live:
            raise IOError(f"lost the write lease for inode {ino} (preempted/expired)")

    def pipe_file(self, path, value, **kwargs):
        """Write a whole object in one shot (fsspec's ``cat``/``pipe``)."""
        path = self._norm(path)
        ino = self._ensure_file_ino(path)
        self._acquire_write(ino)
        try:
            self._fs.truncate(ino, 0)  # clear any prior content (exact overwrite)
            if value:
                self._fs.write(ino, 0, bytes(value))
            self._verify_write_lease(ino)  # gate the durable commit on a live lease
            self._fs.flush(ino)
        finally:
            self._release_write(ino)
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
            ino = self._resolve(path)
            if ino is None:
                raise FileNotFoundError(path)
            a = self._fs.getattr(ino)
            if a["type"] == "directory":
                raise IsADirectoryError(path)
            return AutumnBufferedFile(
                self,
                path,
                mode=mode,
                size=int(a["size"]),
                block_size=block_size or self.block_size,
                cache_options=cache_options,
            )

        # Write modes. Honor fsspec/stdio semantics:
        #   xb — exclusive create (fail if exists); ab — append; wb — overwrite.
        if not autocommit:
            # This backend writes straight to the inode as it goes; promising
            # deferred-commit transactional semantics we don't have would be a
            # silent lie.
            raise NotImplementedError(
                "autumn:// does not support transactions (autocommit=False)"
            )
        if "x" in mode and self._resolve(path) is not None:
            raise FileExistsError(path)
        return AutumnBufferedFile(
            self,
            path,
            mode=mode,
            block_size=block_size or self.block_size,
            cache_options=cache_options,
            append=("a" in mode),
        )

    # ── namespace mutation ─────────────────────────────────────────────────

    def makedirs(self, path, exist_ok=True):
        path = self._norm(path)
        if not path:
            return  # the root always exists
        ino = self._resolve(path)
        if ino is not None:
            if self._fs.getattr(ino)["type"] != "directory":
                raise NotADirectoryError(path)
            if not exist_ok:
                raise FileExistsError(path)
            return
        self._mkdirs(path)
        self.invalidate_cache(self._parent(path))

    def mkdir(self, path, create_parents=True, **kwargs):
        # POSIX-ish: an existing target errors (makedirs is the exist_ok variant).
        if create_parents:
            self.makedirs(path, exist_ok=False)
            return
        # create_parents=False: the parent must already exist; create only the leaf.
        path = self._norm(path)
        parent, name = self._split(path)
        if not name:
            raise FileExistsError(path)
        parent_ino = self._resolve(parent)
        if parent_ino is None:
            raise FileNotFoundError(parent)
        if self._fs.getattr(parent_ino)["type"] != "directory":
            raise NotADirectoryError(parent)
        existing = self._fs.lookup(parent_ino, name)
        if existing is not None:
            raise NotADirectoryError(path) if existing[1] != DT_DIR else FileExistsError(path)
        self._fs.mkdir(parent_ino, name)
        self.invalidate_cache(self._parent(path))

    def rm_file(self, path):
        path = self._norm(path)
        parent, name = self._split(path)
        parent_ino = self._resolve(parent)
        if parent_ino is None:
            raise FileNotFoundError(path)
        self._fs.unlink(parent_ino, name)
        self.invalidate_cache(self._parent(path))

    def _rmtree(self, path, ino):
        """Recursively remove a directory subtree, children first, then itself."""
        for name, cino, kind in self._fs.readdir(ino):
            child = f"{path}/{name}" if path else name
            if kind == DT_DIR:
                self._rmtree(child, cino)
            else:
                self.rm_file(child)
        parent, name = self._split(path)
        parent_ino = self._resolve(parent)
        if parent_ino is not None and name:  # never remove the fs root itself
            self._fs.rmdir(parent_ino, name)

    def rm(self, path, recursive=False, maxdepth=None):
        paths = path if isinstance(path, list) else [path]
        for p in paths:
            p = self._norm(p)
            ino = self._resolve(p)
            if ino is None:
                continue  # already gone
            if self._fs.getattr(ino)["type"] == "directory":
                if not recursive:
                    raise IsADirectoryError(p)
                self._rmtree(p, ino)
            else:
                self.rm_file(p)
        self.invalidate_cache()

    def rmdir(self, path):
        self.rm(path, recursive=True)

    def mv(self, path1, path2, recursive=None, maxdepth=None, **kwargs):
        """Rename within the same cluster via a real inode ``rename`` (atomic
        dirent swap), instead of fsspec's default copy+delete."""
        p1, p2 = self._norm(path1), self._norm(path2)
        op, on = self._split(p1)
        np_, nn = self._split(p2)
        op_ino = self._resolve(op)
        if op_ino is None or not on:
            raise FileNotFoundError(path1)
        np_ino = self._mkdirs(np_)
        self._fs.rename(op_ino, on, np_ino, nn)
        self.invalidate_cache()

    def created(self, path):
        return self.modified(path)

    def modified(self, path):
        mtime = self.info(path).get("mtime")
        if mtime is None:
            raise FileNotFoundError(path)
        from datetime import datetime, timezone

        return datetime.fromtimestamp(mtime, tz=timezone.utc)


class AutumnBufferedFile(AbstractBufferedFile):
    """Buffered read/write file over an inode. Reads delegate ranged fetches to
    ``cat_file``; writes stream sequential blocks straight into the inode via
    ``Fs.write`` at a running offset (``Fs`` itself coalesces into extents)."""

    def __init__(
        self,
        fs,
        path,
        mode="rb",
        block_size=None,
        cache_options=None,
        size=None,
        append=False,
        **kwargs,
    ):
        self._append = append
        self._ino = None
        self._off = 0
        self._lease_held = False
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
        f = self.fs
        self._ino = f._ensure_file_ino(f._norm(self.path))
        f._acquire_write(self._ino)  # M4: WRITE lease held across the upload
        self._lease_held = True
        try:
            if self._append:
                self._off = int(f._fs.getattr(self._ino)["size"])
            else:
                f._fs.truncate(self._ino, 0)  # overwrite: exact-size semantics
                self._off = 0
        except BaseException:  # init failed after acquire → don't leak the lease
            self._lease_held = False
            f._release_write(self._ino)
            raise

    def _upload_chunk(self, final=False):
        data = self.buffer.getvalue()
        if data:
            n = self.fs._fs.write(self._ino, self._off, bytes(data))
            self._off += n
        if final:
            try:
                self.fs._verify_write_lease(self._ino)  # gate the durable commit
                self.fs._fs.flush(self._ino)
            finally:
                if self._lease_held:
                    self._lease_held = False
                    self.fs._release_write(self._ino)
            self.fs.invalidate_cache(self.fs._parent(self.fs._norm(self.path)))
        return True

    def close(self):
        # Catch-all so a WRITE lease is never leaked on an error path that
        # bypassed the final `_upload_chunk` (coco M4 P2: a leaked lease is
        # heartbeated forever, blocking other writers). Idempotent with the
        # normal-path release above via the `_lease_held` flag.
        try:
            super().close()
        finally:
            if self._lease_held and self._ino is not None:
                self._lease_held = False
                try:
                    self.fs._release_write(self._ino)
                except Exception:
                    pass
