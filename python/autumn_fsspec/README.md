# autumn-fsspec

An [fsspec](https://filesystem-spec.readthedocs.io/) filesystem (`autumn://`)
for **models, datasets and checkpoints** on autumn-rs — so Python data tooling
loads straight from the cluster.

```python
import autumn_fsspec                      # registers the "autumn" protocol
import fsspec

fs = fsspec.filesystem("autumn", manager="127.0.0.1:9001")
fs.pipe_file("bucket/hello.txt", b"hi")
fs.cat_file("bucket/hello.txt")           # -> b"hi"

# HuggingFace datasets, directly:
import datasets
ds = datasets.load_dataset("json", data_files="autumn://raw/data.jsonl",
                           storage_options={"manager": "127.0.0.1:9001"})
ds.save_to_disk("autumn://prepared/ds", storage_options={"manager": "127.0.0.1:9001"})
back = datasets.load_from_disk("autumn://prepared/ds",
                               storage_options={"manager": "127.0.0.1:9001"})
```

It is a **thin facade** over `autumn.Fs` (the PyO3 binding to autumn's shared
filesystem core) — no daemon, no new server wire, no data-plane of its own.

### Relationship to `autumn-fuse` — the *same* files (F-FS-UNIFY M3)

`autumn-fuse` (native POSIX mount) and this adapter are **two front-ends onto
one filesystem.** As of F-FS-UNIFY M3 they share the **same inode layout** — the
same `[0x01]` inode / `[0x02]` dirent / `[0x03]` extent keys, driven by the same
Rust core. A model written through the fuse mount **is** visible and
byte-identical at the corresponding `autumn://` path, and vice versa. (The
earlier F-FSSPEC-1 adapter used a private, path-keyed `fs/` namespace — separate
files, no interop; that layout is retired.)

Concurrent writers to one file (fsspec↔fsspec or fsspec↔fuse mount) are fenced
by a **per-inode WRITE lease** (F-FS-UNIFY M4) — a write acquires the lease
(conflict ⇒ `BlockingIOError`), heartbeats it for the duration, and releases on
close; reads are close-to-open coherent. Single-writer flows (dataset prep,
uploads) never see a conflict.

## How it works

The facade maps fsspec paths onto real inodes via `autumn.Fs`:

- **`info`/`ls`** → `resolve(path)` + `readdir` — real POSIX directories and
  dirents (no s3fs-style implicit-dir emulation, no keys-only-range manifest scan).
- **`cat_file`/read** → `resolve` + `read(ino, off, len)` (extent reads are
  zero-copy-eligible ≥ 64 KiB).
- **write/`pipe_file`** → auto-`mkdir` the parent chain + `create`/`resolve`,
  then stream `write(ino, off, …)` (the core coalesces into ≤ 8 MiB extents) +
  `flush`. Overwrite truncates first for exact-size semantics.
- **`mkdir`/`rm`/`mv`** → `mkdir` / `unlink`+`rmdir` (recursive) / `rename`
  (atomic dirent swap).

## Constructor / storage_options

| option | meaning |
|---|---|
| `manager` | `host:port[,host:port…]` of autumn manager(s). Falls back to `$AUTUMN_MANAGER`. (Can't go in the URL — the URL is the object path.) |
| `root` | optional path prefix ("bucket") — really a subdirectory — for namespace isolation |
| `transport` | `"ucx"` / `"tcp"` — process-global; set before first connect |
| `host` | daemon lease-identity host label |
| `chunk_size` | default write block size (default 8 MiB) |

## Models → vLLM / SGLang

fsspec feeds `datasets`, but **not** `transformers.from_pretrained` / vLLM /
SGLang weight loading (those want a *local path* or a purpose-built streaming
loader). Helpers for the model paths (see `docs/model_loading.md` for the full
ranked analysis):

```python
import fsspec
fs = fsspec.filesystem("autumn", manager="127.0.0.1:9001")

# upload a model into autumn (trailing "/" on both = copy CONTENTS into the dir):
fs.put("/local/llama-3-8b/", "models/llama-3-8b/", recursive=True)

# Path A — materialize to local NVMe, then serve unmodified (universal):
fs.get("models/llama-3-8b/", "/scratch/llama/", recursive=True)
#   vllm serve /scratch/llama

# Path B — read a state_dict straight from autumn (no mount, whole file → RAM):
from safetensors.torch import load as st_load
sd = st_load(bytes(fs.cat_file("models/llama-3-8b/model-00001-of-00002.safetensors")))
```

Path C — a custom vLLM `--load-format autumn` streaming loader over autumn's
zero-copy reads — ships as the separate **`autumn_vllm_loader`** package,
verified end-to-end on GPU (byte-exact vs the default loader).

## Install & test

```bash
cd python && maturin build --release && pip install target/wheels/autumn-*.whl  # the `autumn` SDK
pip install -e python/autumn_fsspec[datasets]

# offline (no cluster — a Python inode tree, FakeFs, backs the same facade code):
python -m pytest python/autumn_fsspec/tests/test_fs_offline.py \
                 python/autumn_fsspec/tests/test_datasets_offline.py -q

# live (self-contained — boots an isolated cluster, builds the wheel, runs the
# live suite against the autumn.Fs backing, tears down):
bash python/autumn_fsspec/tests/run_fsspec_e2e.sh

# or against an already-running cluster:
AUTUMN_MANAGER=127.0.0.1:9001 python -m pytest \
    python/autumn_fsspec/tests/test_e2e_cluster.py -q
```
