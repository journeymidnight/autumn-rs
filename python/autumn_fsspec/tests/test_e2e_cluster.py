"""End-to-end tests against a LIVE autumn cluster, using the real `autumn`
PyO3 client (not FakeKV). Confirms the real client's range/batch_get_into/head
semantics match what the adapter assumes.

Requires: a running cluster + the `autumn` extension importable, and
    AUTUMN_MANAGER=host:port   (comma-separated managers)
set in the environment. Skips otherwise. Run:

    AUTUMN_MANAGER=127.0.0.1:20001 python -m pytest tests/test_e2e_cluster.py -q
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MANAGER = os.environ.get("AUTUMN_MANAGER")
if not MANAGER:
    pytest.skip("set AUTUMN_MANAGER to run live-cluster e2e", allow_module_level=True)
autumn = pytest.importorskip("autumn")

from autumn_fsspec import AutumnFileSystem  # noqa: E402

# unique root per run so repeated runs don't collide
ROOT = "fsspec_e2e/" + os.urandom(4).hex()


@pytest.fixture(scope="module")
def fs():
    f = AutumnFileSystem(manager=MANAGER, root=ROOT, chunk_size=1 << 20, skip_instance_cache=True)
    yield f
    try:
        f.rm(ROOT, recursive=True)
    except Exception:
        pass


@pytest.mark.parametrize("size", [0, 1, (1 << 20) - 1, 1 << 20, (1 << 20) + 123, 5 << 20])
def test_roundtrip_sizes(fs, size):
    data = os.urandom(size)
    path = f"blobs/b_{size}.bin"
    with fs.open(path, "wb") as w:
        w.write(data)
    assert fs.info(path)["size"] == size
    assert fs.cat_file(path) == data
    # a cross-chunk ranged read
    if size > 10:
        assert fs.cat_file(path, size // 3, size // 3 + 7) == data[size // 3 : size // 3 + 7]


def test_ls_find_rm(fs):
    fs.pipe_file("tree/a/1.txt", b"one")
    fs.pipe_file("tree/a/2.txt", b"two")
    fs.pipe_file("tree/b.txt", b"bee")
    assert sorted(fs.ls("tree", detail=False)) == ["tree/a", "tree/b.txt"]
    assert set(fs.find("tree")) == {"tree/a/1.txt", "tree/a/2.txt", "tree/b.txt"}
    fs.rm("tree/a", recursive=True)
    assert not fs.exists("tree/a/1.txt")
    assert fs.exists("tree/b.txt")


def test_overwrite_append_exclusive_live(fs):
    # overwrite-shrink must yield the exact new size + content (the inode
    # truncate reaps the stale tail extents; no manifest/chunk bookkeeping now)
    big = os.urandom(3 << 20)  # 3 MiB, spans multiple 8 MiB-capped extents
    fs.pipe_file("w/f.bin", big)
    assert fs.info("w/f.bin")["size"] == len(big)
    assert fs.cat_file("w/f.bin") == big
    with fs.open("w/f.bin", "wb") as w:
        w.write(b"small")
    assert fs.cat_file("w/f.bin") == b"small"
    assert fs.info("w/f.bin")["size"] == 5

    # append across a chunk boundary
    a1 = os.urandom((1 << 20) - 10)
    a2 = os.urandom((1 << 20) + 20)
    with fs.open("w/app.bin", "wb") as w:
        w.write(a1)
    with fs.open("w/app.bin", "ab") as w:
        w.write(a2)
    assert fs.cat_file("w/app.bin") == a1 + a2

    # exclusive create
    with pytest.raises(FileExistsError):
        fs.open("w/f.bin", "xb")


def test_cross_facade_coherence(fs):
    """F-FS-UNIFY M4: a write through one facade is seen by an INDEPENDENT
    facade on the same cluster/root — write-lease + forget-on-release give
    close-to-open coherence over the shared inode layout."""
    other = AutumnFileSystem(manager=MANAGER, root=ROOT, skip_instance_cache=True)
    other.pipe_file("coh/x.bin", b"first")
    assert fs.cat_file("coh/x.bin") == b"first"
    # overwrite through `other`; `fs` (which never cached this inode — it only
    # reads) must observe the new bytes + size, not the stale first write.
    other.pipe_file("coh/x.bin", b"second-and-longer")
    assert fs.cat_file("coh/x.bin") == b"second-and-longer"
    assert fs.info("coh/x.bin")["size"] == len(b"second-and-longer")


def test_datasets_roundtrip_live(fs):
    ds_mod = pytest.importorskip("datasets")
    so = {"manager": MANAGER, "root": ROOT, "skip_instance_cache": True}
    ds = ds_mod.Dataset.from_dict({"id": list(range(200)), "t": [f"x{i}" for i in range(200)]})
    ds.save_to_disk("autumn://live_ds", storage_options=so)
    back = ds_mod.load_from_disk("autumn://live_ds", storage_options=so)
    assert back.to_dict() == ds.to_dict()
