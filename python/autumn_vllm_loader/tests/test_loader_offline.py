"""Offline unit tests for the autumn vLLM loader's pure pieces (safetensors
header parse + per-tensor read planning) — no cluster, no vLLM, no GPU. The
full vLLM integration is exercised by `tests/run_vllm_e2e.sh`."""

import json
import struct

from autumn_vllm_loader import loader


class FakeFs:
    """Minimal `autumn.Fs`-shaped stub over an in-memory bytes blob keyed by ino."""

    def __init__(self, blobs):
        self._blobs = blobs  # {ino: bytes}

    def read_into(self, ino, off, buf):
        data = self._blobs[ino]
        mv = memoryview(buf)
        n = min(len(mv), max(0, len(data) - off))
        mv[:n] = data[off : off + n]
        return n


def _make_safetensors(tensors):
    """tensors: {name: (dtype_str, shape, nbytes)} -> a safetensors blob whose
    header is real; tensor data is zero-filled (we only parse the header)."""
    header = {}
    off = 0
    for name, (dt, shape, nbytes) in tensors.items():
        header[name] = {"dtype": dt, "shape": list(shape), "data_offsets": [off, off + nbytes]}
        off += nbytes
    hj = json.dumps(header).encode("utf-8")
    return struct.pack("<Q", len(hj)) + hj + b"\x00" * off


def test_shard_plans_parses_header_and_offsets():
    blob = _make_safetensors(
        {
            "layer.0.w": ("BF16", (8, 8), 128),
            "layer.1.w": ("F32", (4,), 16),
            "__metadata__": ("", (), 0),  # skipped
        }
    )
    fs = FakeFs({7: blob})
    plans = loader._shard_plans(fs, 7)
    names = {p[0] for p in plans}
    assert names == {"layer.0.w", "layer.1.w"}, names  # __metadata__ dropped
    by_name = {p[0]: p for p in plans}
    data_start = 8 + len(blob) - 128 - 16 - (8 + struct.unpack("<Q", blob[:8])[0] - 8) + 0
    # data_start = 8 + header_len; validate begin/end are absolute + contiguous
    hlen = struct.unpack("<Q", blob[:8])[0]
    ds = 8 + hlen
    assert by_name["layer.0.w"][3:5] == (ds + 0, ds + 128)
    assert by_name["layer.1.w"][3:5] == (ds + 128, ds + 144)
    assert by_name["layer.0.w"][1] == "BF16" and by_name["layer.0.w"][2] == (8, 8)
    # plans sorted by offset
    assert [p[3] for p in plans] == sorted(p[3] for p in plans)


def test_read_exact_loops_to_fill():
    fs = FakeFs({1: bytes(range(256)) * 4})
    got = loader._read_exact(fs, 1, 10, 100)
    assert bytes(got) == (bytes(range(256)) * 4)[10:110]


def test_dtype_map_covers_common():
    for k in ("F32", "F16", "BF16", "I64", "U8"):
        assert k in loader._ST_DT
