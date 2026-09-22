# LanceDB over Autumn FUSE

This example uses the community `lancedb` Python package through Autumn's FUSE
mount. It contains no LanceDB fork, custom object-store provider, or Rust
binding. LanceDB sees a normal local filesystem path.

## Install

Create an isolated environment and install the published community package:

```sh
uv venv examples/lancedb/.venv
uv pip install --python examples/lancedb/.venv/bin/python lancedb
```

## Mount Autumn

Build `autumn-fuse`, create a mount point, and mount the namespace that will hold
the database. Use an empty directory dedicated to this test.

```sh
cargo build --release -p autumn-fuse --bin autumn-fuse
mkdir -p /tmp/autumn-lancedb
target/release/autumn-fuse \
  --manager 127.0.0.1:9001 \
  --mountpoint /tmp/autumn-lancedb \
  --transport tcp
```

If the cluster requires authentication, pass the same credential options you
normally use with `autumn-fuse`. Keep the mount running while LanceDB is in use.

## Connect from Python

The application code is ordinary LanceDB code:

```python
import lancedb

db = lancedb.connect("/tmp/autumn-lancedb/lancedb")
table = db.create_table(
    "vectors",
    [{"id": 42, "vector": [42.0, 1.0, 2.0, 3.0]}],
)
print(table.search([42.0, 1.0, 2.0, 3.0]).limit(1).to_list())
```

Run the complete smoke test with a `file://` URI inside the mount:

```sh
uv run --no-project \
  --python examples/lancedb/.venv/bin/python \
  examples/lancedb/fuse_demo.py file:///tmp/autumn-lancedb/lancedb
```

The test checks the create-only hard-link operation Lance uses to publish local
manifests, then exercises table creation, append, vector search, deletion, a
concurrent reader/writer, and cleanup. It creates a unique table name, but the
target directory should still be dedicated test storage.

Unmount that exact mount point after all LanceDB processes have exited:

```sh
umount /tmp/autumn-lancedb
```

On Linux, use `fusermount3 -u /tmp/autumn-lancedb` when required by the local
FUSE setup. See [the validation notes](../../docs/lancedb_validation.md) and
[operations guide](../../docs/ops.md) for the tested boundary.
