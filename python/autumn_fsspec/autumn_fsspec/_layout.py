"""On-KV key layout + manifest encoding for the autumn fsspec adapter.

autumn's data plane is a flat, ordered byte-key/byte-value store
(`put/get/delete/range`). A POSIX-ish filesystem is layered on top with a
**self-contained namespace** — deliberately independent of `autumn-fuse`'s
inode-keyed layout (`[0x03][ino][off]`), which needs the fuse daemon's inode
allocator + lease coordination. This adapter is a pure client (no daemon), so
it keys everything by **path**, the way s3fs/gcsfs do. A model written through
the fuse mount is therefore NOT visible here (different keying), and vice
versa — two independent doors to the same cluster capacity, not the same files.

**Reserved namespace `fs/`.** Every all-in-one surface that shares the one
cluster keyspace carves out a prefix so a `range` scan of one never returns
another's keys: `autumn-fuse` owns the low binary bytes `0x01`–`0x04`,
`autumn-kvcache` uses `kvc/`, `autumn-memory` uses `doc/`/`ivf/`/`meta/`. This
adapter uses **`fs/`** (an ASCII prefix like the others — crucially NOT the
`0x01`/`0x02` bytes fuse keys inodes/dirents with, so fsspec and a fuse mount
can safely coexist on the same cluster). Two logical key spaces under it:

    manifest : key = b"fs/m/" + full_path                         -> JSON
    chunk    : key = b"fs/d/" + full_path + 0x00 + u64_be(idx)    -> bytes

`full_path` is the fs `root` (optional bucket) joined with the user path, e.g.
root="models", path="llama/a.bin" -> "models/llama/a.bin". Because `full_path`
sorts lexically, a `range` over `b"fs/m/" + dir + "/"` yields every descendant
manifest of `dir` in path order — the basis for `ls`.

Manifests are tiny JSON (so `range`-based listing only ever ships small values;
the bulk bytes live under the DATA prefix and are fetched by exact key):

    file:  {"t":"f", "s":size, "cs":chunk_size, "n":nchunks, "m":mtime}
    dir:   {"t":"d", "m":mtime}

The `0x00` separator before the chunk index keeps a file "a"'s chunks from
colliding with a sibling "ab"'s (prefix "a" would otherwise match both), and
lets a whole file's chunks be dropped with one `batch_delete([0x02]+path+0x00)`.
"""

from __future__ import annotations

import json
import struct

# Reserved `fs/` namespace (see module docstring): an ASCII prefix like
# kvcache's `kvc/` and memory's `doc/`, chosen so fsspec keys never collide
# with autumn-fuse's `0x01`/`0x02` inode/dirent keys on a shared cluster.
META = b"fs/m/"  # manifest keys
DATA = b"fs/d/"  # data-chunk keys
_SEP = b"\x00"


def full_path(root: str, path: str) -> str:
    """Join the fs `root` (bucket, may be "") with a user `path` into the
    root-inclusive path used for key construction. Result has no leading/
    trailing slash and no empty segments."""
    segs = []
    for part in (root, path):
        if part:
            segs.extend(s for s in part.split("/") if s)
    return "/".join(segs)


def manifest_key(root: str, path: str) -> bytes:
    return META + full_path(root, path).encode("utf-8")


def children_prefix(root: str, path: str) -> bytes:
    """`range` prefix that matches every manifest strictly *below* `path`.

    For the fs root (full == "") this is bare `META`, i.e. list everything.
    Otherwise it is `META + full + "/"`, so the entry for `path` itself is
    excluded and only descendants match."""
    full = full_path(root, path)
    return META + (full + "/").encode("utf-8") if full else META


def chunk_key(root: str, path: str, idx: int) -> bytes:
    return DATA + full_path(root, path).encode("utf-8") + _SEP + struct.pack(">Q", idx)


def chunk_prefix(root: str, path: str) -> bytes:
    """Prefix matching all data chunks of a single file (for `batch_delete`)."""
    return DATA + full_path(root, path).encode("utf-8") + _SEP


def subtree_prefixes(root: str, path: str):
    """(meta, data) `batch_delete` prefixes covering everything strictly below
    `path` — used for recursive directory removal.

    Empty full path (the fs root) → the bare namespace prefixes; appending
    "/" there would produce `fs/m//`, which matches nothing (coco P2)."""
    full = full_path(root, path)
    if not full:
        return META, DATA
    tail = (full + "/").encode("utf-8")
    return META + tail, DATA + tail


def file_manifest(size: int, chunk_size: int, nchunks: int, mtime: float) -> bytes:
    return json.dumps(
        {"t": "f", "s": int(size), "cs": int(chunk_size), "n": int(nchunks), "m": mtime}
    ).encode("utf-8")


def dir_manifest(mtime: float) -> bytes:
    return json.dumps({"t": "d", "m": mtime}).encode("utf-8")


def parse_manifest(blob: bytes) -> dict:
    return json.loads(bytes(blob).decode("utf-8"))
