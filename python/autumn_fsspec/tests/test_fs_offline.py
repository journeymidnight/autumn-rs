"""Offline unit tests for AutumnFileSystem over an in-memory FakeFs.

F-FS-UNIFY M3: the facade now sits on the shared inode layout (``autumn.Fs``),
so offline runs the SAME facade code path as a live cluster, backed by a
Python inode tree (``FakeFs``). No cluster needed. Run:

    cd python/autumn_fsspec && python -m pytest tests/test_fs_offline.py -q
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from autumn_fsspec import AutumnFileSystem  # noqa: E402
from fake_fs import FakeFs  # noqa: E402

CS = 16  # tiny block size to exercise multi-block write/read paths cheaply


def make_fs(root="", backend=None):
    return AutumnFileSystem(
        _fs=backend or FakeFs(), root=root, chunk_size=CS, skip_instance_cache=True
    )


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
    assert fs.info("bkt/blob.bin")["type"] == "file"
    assert fs.cat_file("bkt/blob.bin") == data
    with fs.open("bkt/blob.bin", "rb") as f:
        assert f.read() == data


def test_ranged_reads():
    fs = make_fs()
    data = bytes(range(256)) * 4  # 1024 bytes
    fs.pipe_file("d/x.bin", data)
    assert fs.cat_file("d/x.bin", 0, 10) == data[0:10]
    assert fs.cat_file("d/x.bin", 5, 5) == b""
    assert fs.cat_file("d/x.bin", CS - 3, CS + 3) == data[CS - 3 : CS + 3]  # crosses a block
    assert fs.cat_file("d/x.bin", 1000, 2000) == data[1000:]  # clamps to size
    assert fs.cat_file("d/x.bin", -8) == data[-8:]  # negative offset → tail
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


def test_ls_info_and_dirs():
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
    assert fs.isdir("empty")  # a real intermediate directory now exists
    assert fs.ls("empty/dir", detail=False) == []
    with pytest.raises(FileNotFoundError):
        fs.info("does/not/exist")


def test_makedirs_never_clobbers_a_file():
    """A dir must not overwrite a file at the same path (and vice versa)."""
    fs = make_fs()
    fs.pipe_file("data.bin", b"payload" * 10)
    with pytest.raises(NotADirectoryError):
        fs.makedirs("data.bin", exist_ok=True)
    with pytest.raises(NotADirectoryError):
        fs.mkdir("data.bin")
    assert fs.cat_file("data.bin") == b"payload" * 10  # survived untouched
    assert fs.info("data.bin")["type"] == "file"

    # dir semantics: mkdir on existing dir raises; makedirs(exist_ok) returns
    fs.makedirs("d")
    fs.makedirs("d", exist_ok=True)
    with pytest.raises(FileExistsError):
        fs.makedirs("d", exist_ok=False)
    with pytest.raises(FileExistsError):
        fs.mkdir("d")


def test_rm_root_recursive_clears_namespace():
    fs = make_fs()
    fs.pipe_file("a/x.bin", b"1" * (CS + 1))
    fs.pipe_file("b.bin", b"2")
    fs.rm("", recursive=True)
    assert fs.find("") == []
    assert fs.ls("", detail=False) == []


def test_transactions_refused():
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

    fs.rm("a/b", recursive=True)
    assert not fs.exists("a/b/c2.bin")
    assert not fs.exists("a/b")  # the directory itself is gone
    assert fs.exists("a/keep.bin")


def test_overwrite_shrink_exact():
    fs = make_fs()
    big = bytes(range(256)) * 4  # 1024 B
    fs.pipe_file("d/f.bin", big)
    assert fs.info("d/f.bin")["size"] == len(big)

    # overwrite with a much smaller value → exact size + content, no stale tail
    fs.pipe_file("d/f.bin", b"tiny")
    assert fs.cat_file("d/f.bin") == b"tiny"
    assert fs.info("d/f.bin")["size"] == 4

    # same via the buffered writer, then overwrite-shrink again
    with fs.open("d/f.bin", "wb") as w:
        w.write(big)
    assert fs.info("d/f.bin")["size"] == len(big)
    with fs.open("d/f.bin", "wb") as w:
        w.write(b"x" * (CS + 1))
    assert fs.cat_file("d/f.bin") == b"x" * (CS + 1)
    assert fs.info("d/f.bin")["size"] == CS + 1


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


def test_mv_rename():
    fs = make_fs()
    fs.pipe_file("src/a.bin", b"payload" * 5)
    fs.mv("src/a.bin", "dst/b.bin")
    assert not fs.exists("src/a.bin")
    assert fs.cat_file("dst/b.bin") == b"payload" * 5


def test_virtual_root_on_unwritten_bucket():
    """coco P2: a `root=` bucket that no write has created yet must still list
    as an empty root (not FileNotFoundError), without leaking the cluster root."""
    fs = make_fs(root="tenantX")
    assert fs.info("")["type"] == "directory"
    assert fs.ls("", detail=False) == []
    assert fs.exists("")  # the fs root always "exists"


def test_mkdir_create_parents_false():
    """coco P3: mkdir(create_parents=False) must NOT auto-create parents."""
    fs = make_fs()
    with pytest.raises(FileNotFoundError):
        fs.mkdir("a/b", create_parents=False)  # parent `a` absent
    fs.mkdir("a")
    fs.mkdir("a/b", create_parents=False)  # parent exists now → ok
    assert fs.isdir("a/b")
    with pytest.raises(FileExistsError):
        fs.mkdir("a/b", create_parents=False)  # already exists


def test_root_bucket_namespacing():
    backend = FakeFs()  # one shared cluster, isolated by root
    fs_a = make_fs(root="tenantA", backend=backend)
    fs_b = make_fs(root="tenantB", backend=backend)
    fs_a.pipe_file("shared.txt", b"A")
    fs_b.pipe_file("shared.txt", b"B")
    assert fs_a.cat_file("shared.txt") == b"A"
    assert fs_b.cat_file("shared.txt") == b"B"
    assert fs_a.ls("", detail=False) == ["shared.txt"]  # root listing scoped to A
