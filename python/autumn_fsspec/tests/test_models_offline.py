"""Offline test for the model-transfer helpers (`upload`/`materialize`) over
the shared inode backing — exercises fsspec's recursive `put`/`get` on the
facade (directory tree walk, auto-mkdir, byte-exact round-trip). No cluster."""

from __future__ import annotations

import hashlib
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from autumn_fsspec import AutumnFileSystem, models  # noqa: E402
from fake_fs import FakeFs  # noqa: E402


def _sha(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def test_upload_materialize_roundtrip(tmp_path):
    fs = AutumnFileSystem(_fs=FakeFs(), skip_instance_cache=True)
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    files = {
        "config.json": b"{}",
        "sub/model.bin": os.urandom(200_000),  # multi-extent-eligible
        "readme.md": b"hi",
    }
    for rel, data in files.items():
        p = src / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    models.upload(str(src), "models/m1", fs=fs)
    assert set(fs.find("models/m1")) == {
        "models/m1/config.json",
        "models/m1/sub/model.bin",
        "models/m1/readme.md",
    }

    models.materialize("models/m1", str(dst), fs=fs)
    for rel in files:
        assert (dst / rel).exists()
        assert _sha(src / rel) == _sha(dst / rel)
