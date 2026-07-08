"""`register_model_loader("autumn")` — a vLLM model loader that streams
safetensors weights straight out of autumn into the model, over the zero-copy
`autumn.Fs.read_into` seam (+ batched EN direct-read).

vLLM contract (v0.24): `BaseModelLoader` requires `download_model` +
`load_weights`; the inherited `load_model` creates the model on the target
device, then calls our `load_weights(model, model_config)`. We build a
`(name, tensor)` iterator reading each tensor's byte range from autumn and hand
it to `model.load_weights(...)` — the same shape as `DefaultModelLoader`
(vLLM does the device placement + sharding). Weights are enumerated from the
`*.safetensors` shards in the autumn directory (`model_loader_extra_config`),
so config.json + tokenizer still come from vLLM's `model=` path (the standard
"weights from a streaming backend, config local" split).

`model_loader_extra_config` keys:
  manager (required)  — autumn manager `host:port` (comma-separated ok)
  path (required)     — directory in autumn holding the model's *.safetensors
  transport           — "tcp" (default) | "ucx"
  direct_read         — bool, EN-direct read (default True; topology-dependent)
  n_workers           — parallel reader threads (default 4)
  prefetch            — bounded (name,tensor) queue depth (default 8)
"""

from __future__ import annotations

import json
import queue
import struct
import threading

import torch
from vllm.model_executor.model_loader import register_model_loader
from vllm.model_executor.model_loader.base_loader import BaseModelLoader

import autumn

ROOT_INO = 1
_HDR = 8  # safetensors 8-byte little-endian header length prefix

# safetensors dtype string -> torch dtype
_ST_DT = {
    "F64": torch.float64, "F32": torch.float32, "F16": torch.float16,
    "BF16": torch.bfloat16, "I64": torch.int64, "I32": torch.int32,
    "I16": torch.int16, "I8": torch.int8, "U8": torch.uint8, "BOOL": torch.bool,
}


def _read_exact(fs, ino, off, n):
    """Fill an n-byte bytearray from autumn at [off, off+n) (read_into caps at
    1 GiB/call, so loop)."""
    buf = bytearray(n)
    got = 0
    while got < n:
        k = fs.read_into(ino, off + got, memoryview(buf)[got:])
        if k == 0:
            raise RuntimeError(f"short read at ino={ino} off={off + got}")
        got += k
    return buf


def _shard_plans(fs, ino):
    """Parse one safetensors shard's header → [(name, dtype, shape, begin, end)]
    with absolute file offsets, sorted by offset."""
    head8 = _read_exact(fs, ino, 0, _HDR)
    (hlen,) = struct.unpack("<Q", bytes(head8))
    full = _read_exact(fs, ino, 0, _HDR + hlen)
    hdr = json.loads(bytes(full[_HDR:_HDR + hlen]).decode("utf-8"))
    data_start = _HDR + hlen
    plans = []
    for name, spec in hdr.items():
        if name == "__metadata__":
            continue
        b, e = spec["data_offsets"]
        plans.append((name, spec["dtype"], tuple(spec["shape"]),
                      data_start + b, data_start + e))
    plans.sort(key=lambda p: p[3])
    return plans


@register_model_loader("autumn")
class AutumnModelLoader(BaseModelLoader):
    def __init__(self, load_config):
        super().__init__(load_config)
        cfg = load_config.model_loader_extra_config or {}
        self.manager = cfg["manager"]
        self.path = cfg["path"].strip("/")
        self.direct_read = bool(cfg.get("direct_read", True))
        self.n_workers = int(cfg.get("n_workers", 4))
        self.prefetch = int(cfg.get("prefetch", 8))
        transport = cfg.get("transport")
        if transport:
            # Process-global; must precede the first connect (this loader is the
            # only autumn client in the vLLM process).
            autumn.set_transport(transport)

    # Weights stream at load_weights time; nothing to pre-download.
    def download_model(self, model_config) -> None:  # noqa: D401
        pass

    def load_weights(self, model, model_config) -> None:
        model.load_weights(self._iter_weights())

    def _iter_weights(self):
        """Yield (name, cpu_tensor) for every tensor across all *.safetensors
        shards in the autumn `path`, read via `read_into` into pinned host
        buffers by `n_workers` parallel readers (read_into releases the GIL, so
        the threads read concurrently) feeding a bounded prefetch queue —
        reads run ahead of vLLM's per-weight consumption (H2D + shard placement)."""
        fs0 = autumn.Fs.connect(self.manager, direct_read=self.direct_read)
        dir_ino = fs0.resolve("/" + self.path)
        if dir_ino is None:
            raise FileNotFoundError(f"autumn path not found: {self.path}")
        shards = sorted(
            name for (name, _cino, _kind) in fs0.readdir(dir_ino)
            if name.endswith(".safetensors")
        )
        if not shards:
            raise FileNotFoundError(f"no *.safetensors under autumn:{self.path}")

        # Build the full work list: (shard_ino, name, dtype, shape, begin, end).
        work = []
        for shard in shards:
            sino = fs0.resolve("/" + self.path + "/" + shard)
            for (name, dt, shape, b, e) in _shard_plans(fs0, sino):
                work.append((sino, name, dt, shape, b, e))

        q: queue.Queue = queue.Queue(maxsize=self.prefetch)
        SENTINEL = object()
        errbox = []

        def reader(my_work):
            try:
                fs = autumn.Fs.connect(self.manager, direct_read=self.direct_read)
                for (sino, name, dt, shape, b, e) in my_work:
                    ln = e - b
                    buf = torch.empty(ln, dtype=torch.uint8, pin_memory=True)
                    npv = buf.numpy()
                    o = 0
                    while o < ln:
                        o += fs.read_into(sino, b + o, npv[o:ln])
                    t = buf.view(_ST_DT[dt]).reshape(shape)
                    q.put((name, t))
            except Exception as ex:  # surface reader failure to the consumer
                errbox.append(ex)
            finally:
                q.put(SENTINEL)

        k = max(1, min(self.n_workers, len(work)))
        chunks = [work[i::k] for i in range(k)]
        threads = [threading.Thread(target=reader, args=(c,), daemon=True) for c in chunks]
        for t in threads:
            t.start()

        done = 0
        while done < k:
            item = q.get()
            if item is SENTINEL:
                done += 1
                continue
            yield item
        for t in threads:
            t.join()
        if errbox:
            raise errbox[0]
