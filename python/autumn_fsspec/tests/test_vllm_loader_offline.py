"""Offline tests for the safetensors header parser / read planner used by the
prototype streaming loader. Builds a real safetensors byte layout by hand (no
torch needed) and checks the plan matches, then round-trips it through the
FakeKV-backed fs via `iter_tensors`.
"""

from __future__ import annotations

import json
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from autumn_fsspec import AutumnFileSystem  # noqa: E402
from autumn_fsspec import vllm_loader as vl  # noqa: E402
from fake_fs import FakeFs  # noqa: E402


def build_safetensors(tensors):
    """tensors: dict name -> (dtype, shape, raw_bytes). Returns file bytes in
    the real safetensors layout."""
    header = {}
    body = bytearray()
    for name, (dtype, shape, raw) in tensors.items():
        b = len(body)
        body += raw
        header[name] = {"dtype": dtype, "shape": shape, "data_offsets": [b, len(body)]}
    header["__metadata__"] = {"format": "pt"}
    hj = json.dumps(header).encode("utf-8")
    return struct.pack("<Q", len(hj)) + hj + bytes(body)


def test_parse_and_plan():
    tensors = {
        "a.weight": ("F32", [2, 2], bytes(range(16))),
        "b.bias": ("F16", [4], bytes(range(8))),
    }
    blob = build_safetensors(tensors)

    first8 = blob[:8]
    total = vl.header_probe_len(first8)
    header, data_start = vl.parse_safetensors_header(blob[:total])
    plans = vl.plan_reads(header, data_start)

    assert [p["name"] for p in plans] == ["a.weight", "b.bias"]  # offset order
    assert plans[0]["shape"] == [2, 2] and plans[0]["dtype"] == "F32"
    # extracted ranges recover the exact tensor bytes
    assert blob[plans[0]["begin"] : plans[0]["end"]] == bytes(range(16))
    assert blob[plans[1]["begin"] : plans[1]["end"]] == bytes(range(8))
    assert "__metadata__" not in {p["name"] for p in plans}


def test_iter_tensors_over_fs():
    fs = AutumnFileSystem(_fs=FakeFs(), chunk_size=13, skip_instance_cache=True)
    tensors = {
        "w1": ("F32", [8], os.urandom(32)),
        "w2": ("BF16", [3, 5], os.urandom(30)),
    }
    blob = build_safetensors(tensors)
    fs.pipe_file("m/model.safetensors", blob)

    got = {name: raw for name, _dt, _sh, raw in vl.iter_tensors(fs, "m/model.safetensors")}
    assert got["w1"] == tensors["w1"][2]
    assert got["w2"] == tensors["w2"][2]
