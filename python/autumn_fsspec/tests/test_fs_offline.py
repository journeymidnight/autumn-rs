"""Offline unit tests for AutumnFileSystem over an in-memory FakeKV.

Exercises the full filesystem surface (chunking, ranged reads, ls/info,
mkdir/rm) with no cluster, so it runs anywhere. Run:

    cd python/autumn_fsspec && python -m pytest tests/test_fs_offline.py -q
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from autumn_fsspec import AutumnFileSystem  # noqa: E402
from autumn_fsspec import _layout  # noqa: E402
from fake_kv import FakeKV  # noqa: E402

CS = 16  # tiny chunk size to exercise multi-chunk paths cheaply


def make_fs(root=""):
    return AutumnFileSystem(
        _client=FakeKV(), root=root, chunk_size=CS, skip_instance_cache=True
    )


# ── key layout ──────────────────────────────────────────────────────────────


def test_layout_keys_and_prefixes():
    assert _layout.manifest_key("", "a/b.txt") == b"fs/m/a/b.txt"
    assert _layout.manifest_key("bkt", "a/b.txt") == b"fs/m/bkt/a/b.txt"
    assert _layout.chunk_key("", "a", 3) == b"fs/d/a\x00" + (3).to_bytes(8, "big")
    # children prefix excludes the node itself; root lists all manifests
    assert _layout.children_prefix("", "dir") == b"fs/m/dir/"
    assert _layout.children_prefix("", "") == b"fs/m/"
    # sibling "a" chunks never match "ab"'s chunk prefix
    assert not _layout.chunk_key("", "ab", 0).startswith(_layout.chunk_prefix("", "a"))
    # reserved `fs/` namespace — must NOT collide with autumn-fuse's 0x01/0x02
    # (inode/dirent) keys, so fsspec + a fuse mount can share one cluster
    for k in (
        _layout.manifest_key("", "x"),
        _layout.chunk_key("", "x", 0),
        _layout.children_prefix("", ""),
    ):
        assert k.startswith(b"fs/")
        assert k[0] not in (0x01, 0x02, 0x03, 0x04)


# ── round-trip across size boundaries ───────────────────────────────────────


@pytest.mark.parametrize(
    "size",
    [0, 1, CS - 1, CS, CS + 1, 2 * CS, 3 * CS + 5, 10 * CS],
)
def test_write_read_roundtrip(size):
    fs = make_fs()
    data = bytes((i * 7 + 3) & 0xFF for i in range(size))
    with fs.open("bkt/blob.bin", "wb") as f:
        f.write(data)

    assert fs.info("bkt/blob.bin")["size"] == size
    assert fs.cat_file("bkt/blob.bin") == data
    with fs.open("bkt/blob.bin", "rb") as f:
        assert f.read() == data

    # expected chunk count in the manifest
    m = fs._get_manifest("bkt/blob.bin")
    exp_n = 0 if size == 0 else (size + CS - 1) // CS
    assert m["n"] == exp_n and m["cs"] == CS


def test_ranged_reads():
    fs = make_fs()
    data = bytes(range(256)) * 4  # 1024 bytes, many chunks
    fs.pipe_file("d/x.bin", data)
    assert fs.cat_file("d/x.bin", 0, 10) == data[0:10]
    assert fs.cat_file("d/x.bin", 5, 5) == b""
    assert fs.cat_file("d/x.bin", CS - 3, CS + 3) == data[CS - 3 : CS + 3]  # crosses chunk
    assert fs.cat_file("d/x.bin", 1000, 2000) == data[1000:]  # clamps to size
    # negative offsets (fsspec allows) → tail
    assert fs.cat_file("d/x.bin", -8) == data[-8:]
    # random-access read via file handle + seek
    with fs.open("d/x.bin", "rb") as f:
        f.seek(500)
        assert f.read(40) == data[500:540]


def test_write_in_many_small_writes():
    fs = make_fs()
    parts = [b"hello ", b"world", b"!" * 100, b"", b"tail"]
    with fs.open("d/streamed.txt", "wb") as f:
        for p in parts:
            f.write(p)
    assert fs.cat_file("d/streamed.txt") == b"".join(parts)


# ── namespace: ls / info / dirs ─────────────────────────────────────────────


def test_ls_info_and_implicit_dirs():
    fs = make_fs()
    fs.pipe_file("models/llama/config.json", b"{}")
    fs.pipe_file("models/llama/model.bin", b"x" * (CS * 2 + 7))
    fs.pipe_file("models/readme.md", b"hi")

    names = fs.ls("models", detail=False)
    assert sorted(names) == ["models/llama", "models/readme.md"]

    detail = {d["name"]: d for d in fs.ls("models")}
    assert detail["models/llama"]["type"] == "directory"
    assert detail["models/readme.md"]["type"] == "file"
    assert detail["models/readme.md"]["size"] == 2

    assert fs.info("models/llama")["type"] == "directory"
    assert fs.info("models/llama/model.bin")["size"] == CS * 2 + 7
    assert fs.isdir("models/llama")
    assert fs.isfile("models/llama/config.json")
    assert fs.exists("models/llama/config.json")
    assert not fs.exists("models/nope")

    # ls of a file returns just that file
    assert fs.ls("models/readme.md", detail=False) == ["models/readme.md"]

    # find walks the whole subtree
    found = set(fs.find("models"))
    assert found == {
        "models/llama/config.json",
        "models/llama/model.bin",
        "models/readme.md",
    }


def test_makedirs_empty_dir_and_missing():
    fs = make_fs()
    fs.makedirs("empty/dir")
    assert fs.isdir("empty/dir")
    assert fs.ls("empty/dir", detail=False) == []
    with pytest.raises(FileNotFoundError):
        fs.info("does/not/exist")


def test_makedirs_never_clobbers_a_file():
    """coco P2: file + dir manifests share a key — makedirs on an existing
    FILE must refuse, not overwrite the file's manifest."""
    fs = make_fs()
    fs.pipe_file("data.bin", b"payload" * 10)
    with pytest.raises(NotADirectoryError):
        fs.makedirs("data.bin", exist_ok=True)
    with pytest.raises(NotADirectoryError):
        fs.mkdir("data.bin")
    # the file survived untouched
    assert fs.cat_file("data.bin") == b"payload" * 10
    assert fs.info("data.bin")["type"] == "file"

    # dir semantics: mkdir on existing dir raises, makedirs(exist_ok) returns
    fs.makedirs("d")
    fs.makedirs("d", exist_ok=True)
    with pytest.raises(FileExistsError):
        fs.makedirs("d", exist_ok=False)
    with pytest.raises(FileExistsError):
        fs.mkdir("d")


def test_rm_root_recursive_clears_namespace():
    """coco P2: subtree_prefixes at the fs root must match the bare
    namespace (`fs/m/`), not the nothing-matching `fs/m//`."""
    fs = make_fs()
    fs.pipe_file("a/x.bin", b"1" * (CS + 1))
    fs.pipe_file("b.bin", b"2")
    fs.rm("", recursive=True)
    assert fs.find("") == []
    # no manifest or chunk keys survive in the namespace
    assert not any(k.startswith(b"fs/") for k in fs._client.store)


def test_transactions_refused():
    """coco P3: we don't implement deferred commit — promising it silently
    would be a lie. autocommit=False must raise."""
    fs = make_fs()
    with pytest.raises(NotImplementedError):
        fs._open("t.bin", mode="wb", autocommit=False)


def test_rm_file_and_recursive():
    fs = make_fs()
    fs.pipe_file("a/b/c1.bin", b"1" * (CS + 1))
    fs.pipe_file("a/b/c2.bin", b"2" * (CS + 1))
    fs.pipe_file("a/keep.bin", b"k")

    fs.rm_file("a/b/c1.bin")
    assert not fs.exists("a/b/c1.bin")
    # its chunks are gone too (no orphans)
    assert not any(
        k.startswith(_layout.chunk_prefix("", "a/b/c1.bin")) for k in fs._client.store
    )

    fs.rm("a/b", recursive=True)
    assert not fs.exists("a/b/c2.bin")
    assert fs.exists("a/keep.bin")
    # only keep.bin's keys survive under a/
    live = [k for k in fs._client.store if k[1:].startswith(b"a/")]
    assert all(b"keep.bin" in k for k in live)


def _chunk_keys(fs, path):
    return [k for k in fs._client.store if k.startswith(_layout.chunk_prefix(fs.root, path))]


def test_overwrite_reaps_stale_tail_chunks():
    fs = make_fs()
    big = bytes(range(256)) * 4  # 1024 B → many chunks
    fs.pipe_file("d/f.bin", big)
    n_big = len(_chunk_keys(fs, "d/f.bin"))
    assert n_big > 1

    # overwrite with a smaller value → old higher-index chunks must be reaped
    fs.pipe_file("d/f.bin", b"tiny")
    assert fs.cat_file("d/f.bin") == b"tiny"
    assert len(_chunk_keys(fs, "d/f.bin")) == 1  # no orphaned tail chunks

    # same via the buffered writer
    with fs.open("d/f.bin", "wb") as w:
        w.write(big)
    assert len(_chunk_keys(fs, "d/f.bin")) == n_big
    with fs.open("d/f.bin", "wb") as w:
        w.write(b"x" * (CS + 1))  # 2 chunks
    assert fs.cat_file("d/f.bin") == b"x" * (CS + 1)
    assert len(_chunk_keys(fs, "d/f.bin")) == 2


def test_exclusive_create_mode():
    fs = make_fs()
    with fs.open("d/new.bin", "xb") as w:
        w.write(b"first")
    assert fs.cat_file("d/new.bin") == b"first"
    with pytest.raises(FileExistsError):
        fs.open("d/new.bin", "xb")


@pytest.mark.parametrize("tail0", [CS, CS - 3, 2 * CS + 5])  # exact / partial / multi
def test_append_mode(tail0):
    fs = make_fs()
    first = bytes((i * 3) & 0xFF for i in range(tail0))
    second = bytes((i * 5 + 1) & 0xFF for i in range(CS + 7))
    with fs.open("d/a.bin", "wb") as w:
        w.write(first)
    with fs.open("d/a.bin", "ab") as w:
        w.write(second)
    assert fs.cat_file("d/a.bin") == first + second
    assert fs.info("d/a.bin")["size"] == tail0 + len(second)
    # append to a non-existent path behaves like a create
    with fs.open("d/fresh.bin", "ab") as w:
        w.write(b"hello")
    assert fs.cat_file("d/fresh.bin") == b"hello"


def test_root_bucket_namespacing():
    fs_a = make_fs(root="tenantA")
    fs_b = make_fs(root="tenantB")
    fs_a._client = fs_b._client = FakeKV()  # share one KV, isolate by root
    fs_a.pipe_file("shared.txt", b"A")
    fs_b.pipe_file("shared.txt", b"B")
    assert fs_a.cat_file("shared.txt") == b"A"
    assert fs_b.cat_file("shared.txt") == b"B"
    assert fs_a.ls("", detail=False) == ["shared.txt"]  # root listing scoped to A
