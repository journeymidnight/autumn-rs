"""Prove the adapter satisfies HuggingFace `datasets` end-to-end — save_to_disk
+ load_from_disk over an `autumn://` URL — with no cluster (FakeKV injected via
storage_options). This is the acceptance test for "python datasets can load
data directly from autumn".
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import autumn_fsspec  # noqa: E402,F401  registers the "autumn" protocol
from fake_fs import FakeFs  # noqa: E402

datasets = pytest.importorskip("datasets")


def test_datasets_save_and_load_roundtrip():
    backend = FakeFs()  # one shared in-memory cluster for save + load
    so = {"_fs": backend, "skip_instance_cache": True, "chunk_size": 1 << 20}

    ds = datasets.Dataset.from_dict(
        {
            "id": list(range(500)),
            "text": [f"row-{i} " + "lorem ipsum " * (i % 7) for i in range(500)],
            "score": [i * 0.5 for i in range(500)],
        }
    )

    ds.save_to_disk("autumn://data/my_ds", storage_options=so)

    loaded = datasets.load_from_disk("autumn://data/my_ds", storage_options=so)
    assert len(loaded) == 500
    assert loaded[0]["text"].startswith("row-0")
    assert loaded[499]["id"] == 499
    assert loaded.column_names == ["id", "text", "score"]
    # content equality across the whole table
    assert loaded.to_dict() == ds.to_dict()


def test_load_dataset_from_json_data_files():
    """`load_dataset('json', data_files='autumn://...')` — the other common
    entry point (reading raw data files a user uploaded to autumn)."""
    import json

    backend = FakeFs()
    so = {"_fs": backend, "skip_instance_cache": True}
    import fsspec

    fs = fsspec.filesystem("autumn", **so)
    rows = [{"q": f"question {i}", "a": f"answer {i}"} for i in range(50)]
    with fs.open("raw/qa.jsonl", "wb") as f:
        f.write(("\n".join(json.dumps(r) for r in rows)).encode("utf-8"))

    ds = datasets.load_dataset(
        "json", data_files="autumn://raw/qa.jsonl", storage_options=so, split="train"
    )
    assert len(ds) == 50
    assert ds[0]["q"] == "question 0"
    assert ds[49]["a"] == "answer 49"
