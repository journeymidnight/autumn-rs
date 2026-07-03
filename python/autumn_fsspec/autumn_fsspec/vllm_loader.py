"""PROTOTYPE — a vLLM model loader that streams safetensors weights straight
from autumn via the zero-copy KV client, instead of copying to local disk.

STATUS: prototype / unverified on GPU. The offline-testable piece (parsing a
safetensors header + planning the byte reads) has unit coverage; the
vLLM/torch/GPU integration path has NOT been run here (no GPU + vLLM in this
env). Treat the vLLM wiring as a design skeleton to validate on a GPU box.

Rationale (docs/model_loading.md §7, option #1): vLLM/SGLang share a loader
model with a plugin seam — ``vllm.model_executor.model_loader.register_model_loader``
lets an out-of-tree ``BaseModelLoader`` subclass own weight loading. autumn's
strength (large-value RDMA/UCX ``get_into`` into pinned host buffers) is exactly
the mechanism Run:ai Model Streamer implements by hand: saturate storage reads
into a pinned buffer and overlap them with the H2D copy. This module reads each
tensor's byte range from autumn (zero-copy-eligible, ≥ 64 KiB) so the read
overlaps naturally with the per-tensor device copy vLLM already does.

The safetensors format: an 8-byte little-endian header length, then a JSON
header ``{name: {dtype, shape, data_offsets:[begin,end]}, "__metadata__":{...}}``,
then the raw tensor bytes; ``data_offsets`` are relative to the end of the
header. We parse the header (cheap ``cat_file`` of just the head), then fetch
each tensor's byte range on demand. See ``plan_reads`` (offline-tested).
"""

from __future__ import annotations

import json
import struct

_HEADER_LEN_SIZE = 8


def parse_safetensors_header(head_bytes):
    """Given at least the first ``8 + header_len`` bytes of a safetensors file,
    return ``(header_dict, data_start)`` where ``data_start`` is the absolute
    file offset where tensor data begins (== 8 + header_len)."""
    if len(head_bytes) < _HEADER_LEN_SIZE:
        raise ValueError("safetensors: truncated header length")
    (header_len,) = struct.unpack("<Q", bytes(head_bytes[:_HEADER_LEN_SIZE]))
    end = _HEADER_LEN_SIZE + header_len
    if len(head_bytes) < end:
        raise ValueError(
            f"safetensors: need {end} header bytes, have {len(head_bytes)}"
        )
    header = json.loads(bytes(head_bytes[_HEADER_LEN_SIZE:end]).decode("utf-8"))
    return header, end


def plan_reads(header, data_start):
    """Turn a parsed header into an ordered list of tensor read plans:
    ``[{name, dtype, shape, begin, end}]`` with absolute file offsets. Skips
    the ``__metadata__`` pseudo-entry. Sorted by file offset so reads are
    sequential-friendly."""
    plans = []
    for name, spec in header.items():
        if name == "__metadata__":
            continue
        b, e = spec["data_offsets"]
        plans.append(
            {
                "name": name,
                "dtype": spec["dtype"],
                "shape": spec["shape"],
                "begin": data_start + b,
                "end": data_start + e,
            }
        )
    plans.sort(key=lambda p: p["begin"])
    return plans


def header_probe_len(first8):
    """From the first 8 bytes, how many total bytes to read to have the full
    header (``8 + header_len``)."""
    (header_len,) = struct.unpack("<Q", bytes(first8[:_HEADER_LEN_SIZE]))
    return _HEADER_LEN_SIZE + header_len


def iter_tensors(fs, path):
    """Yield ``(name, dtype, shape, raw_bytes)`` for every tensor in the
    safetensors object at ``path`` on an ``AutumnFileSystem`` ``fs``, fetching
    each tensor's byte range on demand via ``cat_file`` (zero-copy-eligible).

    This is the storage-read half of a streaming loader; a real vLLM loader
    wraps each ``raw_bytes`` in a ``torch.frombuffer(...).view(dtype).reshape``
    and copies to the device, overlapping the next tensor's fetch."""
    first8 = fs.cat_file(path, 0, _HEADER_LEN_SIZE)
    total = header_probe_len(first8)
    head = fs.cat_file(path, 0, total)
    header, data_start = parse_safetensors_header(head)
    for p in plan_reads(header, data_start):
        raw = fs.cat_file(path, p["begin"], p["end"])
        yield p["name"], p["dtype"], p["shape"], raw


# ── vLLM plugin seam (skeleton; validate on a GPU box) ──────────────────────
#
# def register():
#     from vllm.model_executor.model_loader import register_model_loader
#     from vllm.model_executor.model_loader.base_loader import BaseModelLoader
#
#     @register_model_loader("autumn")
#     class AutumnModelLoader(BaseModelLoader):
#         def download_model(self, model_config): ...      # no-op / prefetch
#         def load_weights(self, model, model_config):
#             fs = AutumnFileSystem(manager=..., skip_instance_cache=True)
#             # enumerate shard objects (index.json) under the model path,
#             # then for each: torch tensors from iter_tensors(fs, shard) →
#             # model.load_weights(...) with read/H2D overlap.
#
# Then: `vllm serve autumn://models/llama --load-format autumn` (+ manager via
# --model-loader-extra-config). The same class also works under SGLang, which
# reuses vLLM's loader registry.
