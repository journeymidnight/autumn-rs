"""autumn-fsspec: an fsspec filesystem over the autumn-rs partition layer.

Import side effect: registers the ``autumn`` protocol with fsspec, so

    import autumn_fsspec                       # registers "autumn://"
    import fsspec
    fs = fsspec.filesystem("autumn", manager="127.0.0.1:9001")
    fs.pipe_file("bucket/hello.txt", b"hi")
    fs.cat_file("bucket/hello.txt")            # -> b"hi"

and any fsspec-aware library (HuggingFace ``datasets`` / ``huggingface_hub``,
pandas, pyarrow) can read/write ``autumn://…`` URLs given
``storage_options={"manager": "..."}``.

The extra ``[project.entry-points."fsspec.specs"]`` in ``pyproject.toml`` lets
fsspec discover the protocol lazily even without importing this package first.
"""

from __future__ import annotations

from .filesystem import DEFAULT_CHUNK_SIZE, AutumnBufferedFile, AutumnFileSystem
from .models import load_safetensors, materialize, upload

__all__ = [
    "AutumnFileSystem",
    "AutumnBufferedFile",
    "DEFAULT_CHUNK_SIZE",
    "upload",
    "materialize",
    "load_safetensors",
]

try:
    import fsspec

    fsspec.register_implementation("autumn", AutumnFileSystem, clobber=True)
except Exception:  # fsspec optional at import time; entry-point still registers it
    pass
