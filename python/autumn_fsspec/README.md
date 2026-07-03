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

It is a **pure client** on autumn's Python KV SDK (the `autumn` PyO3 extension)
— no daemon, no new server wire, no data-plane of its own. It sits alongside
`autumn-fuse` (native POSIX mount) as the *programmatic* file surface.

### Relationship to `autumn-fuse`

They are **two independent doors to the same cluster, not the same files.**
`autumn-fuse` keys files by **inode** (`[0x03][ino][off]`, with a path→inode
tree managed by the fuse daemon); this adapter keys by **path** (under the
reserved `fs/` namespace). A model written through the fuse mount is *not*
visible via an `autumn://` path lookup, and vice versa — pick one door per
dataset/model. The two use disjoint key namespaces (`fs/` here vs fuse's
`0x01`–`0x04`), so a fuse mount and fsspec can **safely share one cluster**
without cross-contaminating each other's listings. (True read/write interop —
fsspec speaking fuse's inode layout — would require reproducing the daemon's
inode allocator + lease/fencing coordination and is intentionally out of scope.)

## How it works

autumn's data plane is a flat, ordered byte-KV store. A file becomes:

| key | value |
|---|---|
| `\x01` + path | tiny JSON **manifest** `{t,s(ize),cs(chunk),n(chunks),m(time)}` |
| `\x02` + path + `\x00` + u64(idx) | one **data chunk** (default 8 MiB) |

- **8 MiB chunks** match `autumn-fuse`'s `MAX_EXTENT`, are zero-copy-eligible
  (≥ 64 KiB), and stay under the 64 MiB inline-put cap — so files of any size
  work despite the SDK exposing only inline puts.
- **Directories are implicit** (derived from descendant manifests, s3fs-style),
  with an optional explicit marker so empty dirs created by `makedirs` exist.
- **Reads** fan a multi-chunk range out through `batch_get_into` into
  exactly-sized buffers (lengths known from the manifest) — one pipelined,
  zero-copy round trip.
- **Listing** uses autumn's keys-only `range` scan plus a pipelined multi-`get`
  of just the direct children's manifests (autumn `range` never ships values).

## Constructor / storage_options

| option | meaning |
|---|---|
| `manager` | `host:port[,host:port…]` of autumn manager(s). Falls back to `$AUTUMN_MANAGER`. (Can't go in the URL — the URL is the object path.) |
| `root` | optional path prefix ("bucket") for namespace isolation |
| `transport` | `"ucx"` / `"tcp"` — process-global; set before first connect |
| `chunk_size` | bytes per data chunk (default 8 MiB) |

## Models → vLLM / SGLang

fsspec feeds `datasets`, but **not** `transformers.from_pretrained` / vLLM /
SGLang weight loading (those want a *local path* or a purpose-built streaming
loader). Helpers for the model paths (see `docs/model_loading.md` for the full
ranked analysis):

```python
import autumn_fsspec as af

af.upload("/local/llama-3-8b", "models/llama-3-8b", manager="127.0.0.1:9001")

# Path A — materialize to local NVMe, then serve unmodified (universal):
af.materialize("models/llama-3-8b", "/scratch/llama", manager="127.0.0.1:9001")
#   vllm serve /scratch/llama

# Path B — read a state_dict straight from autumn (no mount, whole file → RAM):
sd = af.load_safetensors("models/llama-3-8b/model-00001-of-00002.safetensors",
                         manager="127.0.0.1:9001")
```

`autumn_fsspec.vllm_loader` sketches Path C — a custom vLLM `--load-format
autumn` streaming loader over autumn's zero-copy reads (prototype; validate on
a GPU box).

## Install & test

```bash
cd python && maturin build --release && pip install target/wheels/autumn-*.whl  # the `autumn` SDK
pip install -e python/autumn_fsspec[datasets]

# offline (no cluster):
python -m pytest python/autumn_fsspec/tests/test_fs_offline.py \
                 python/autumn_fsspec/tests/test_datasets_offline.py \
                 python/autumn_fsspec/tests/test_vllm_loader_offline.py -q

# live (needs a cluster):
AUTUMN_MANAGER=127.0.0.1:9001 python -m pytest \
    python/autumn_fsspec/tests/test_e2e_cluster.py -q
```
