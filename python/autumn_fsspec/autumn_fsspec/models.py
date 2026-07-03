"""Model & dataset transfer helpers on top of ``AutumnFileSystem``.

Research bottom line (see ``docs/model_loading.md``): fsspec feeds HuggingFace
``datasets`` directly, but ``transformers.from_pretrained`` / vLLM / SGLang do
**not** load weights through an arbitrary fsspec protocol — they want a *local
path* (or a purpose-built streaming loader). So the practical, zero-engine-code
paths to serve a model that lives in autumn are:

  1. **Materialize to local NVMe**, then ``vllm serve <local_dir>`` — universal,
     full local speed. ``materialize()`` below.
  2. **FUSE mount** (``autumn-fuse``) + force the loader's *eager* read
     strategy to dodge the mmap-over-FUSE page-fault penalty. (docs recipe.)
  3. **Custom vLLM loader** streaming via autumn's zero-copy ``get_into`` — the
     high-throughput path; prototype in ``vllm_loader.py``.

These helpers cover (1) plus a ``load_safetensors`` escape hatch for reading a
state_dict straight from autumn without any mount.
"""

from __future__ import annotations

from .filesystem import AutumnFileSystem


def _resolve_fs(fs, manager, root, chunk_size):
    if fs is not None:
        return fs
    kw = {} if chunk_size is None else {"chunk_size": chunk_size}
    return AutumnFileSystem(manager=manager, root=root, skip_instance_cache=True, **kw)


def upload(local_dir, dest, fs=None, manager=None, root="", chunk_size=None):
    """Upload a local model/dataset directory tree into autumn under ``dest``.

    Equivalent to ``fs.put(local_dir, dest, recursive=True)`` with the
    contents-into-directory mapping made explicit. Returns ``dest``.
    """
    fs = _resolve_fs(fs, manager, root, chunk_size)
    fs.put(local_dir.rstrip("/") + "/", dest.rstrip("/") + "/", recursive=True)
    return dest


def materialize(src, local_dir, fs=None, manager=None, root=""):
    """Download an autumn directory (a model/checkpoint) to ``local_dir`` so a
    stock loader can use it: ``vllm serve <local_dir>`` /
    ``AutoModel.from_pretrained(local_dir)``.

    Uses the trailing-slash "contents-of" mapping so ``local_dir`` ends up with
    the model's files at its top level (config.json, *.safetensors, …), not
    nested under an extra directory. Returns ``local_dir``.
    """
    import os

    fs = _resolve_fs(fs, manager, root, None)
    os.makedirs(local_dir, exist_ok=True)
    fs.get(src.rstrip("/") + "/", local_dir.rstrip("/") + "/", recursive=True)
    return local_dir


def load_safetensors(path, fs=None, manager=None, root="", device="cpu"):
    """Load a ``.safetensors`` file straight from autumn into a state_dict,
    with no local file and no mount.

    ``safetensors``' mmap path needs a real local path; the in-memory
    ``safetensors.torch.load(bytes)`` API does not — so we ``cat_file`` the
    object and deserialize from bytes. Trade-off: the whole file lands in host
    RAM (no lazy slicing). For multi-shard models, prefer ``materialize()``.
    """
    from safetensors.torch import load as st_load

    fs = _resolve_fs(fs, manager, root, None)
    data = fs.cat_file(path)
    sd = st_load(bytes(data))
    if device and device != "cpu":
        sd = {k: v.to(device) for k, v in sd.items()}
    return sd
