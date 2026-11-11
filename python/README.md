# autumn-rs (Python)

The Python client for [autumn-rs](https://github.com/journeymidnight/autumn-rs),
a distributed storage engine for AI data. It is a native extension (PyO3); the
import name is `autumn`.

```bash
pip install autumn-rs
```

One abi3 wheel per architecture serves every CPython >= 3.11 on Linux
(manylinux 2.28, x86_64 and aarch64). It speaks TCP everywhere, and RDMA
through the host's UCX (>= 1.17, `libucp.so.0` on the loader path) after
`autumn.set_transport("ucx")`; UCX is loaded only then.

```python
import asyncio, autumn

async def main():
    c = await autumn.Client.connect("127.0.0.1:9001", "kvc")
    await c.put(b"mykey", b"hello")
    print(await c.get(b"mykey"))

asyncio.run(main())
```

- `autumn.Client`: async KV (`put`, `get`, `delete`, `range`, `head`, batches).
- `autumn.BatchClient`: blocking batched reads and writes straight into your
  buffers, with the GIL released. This is what the KV-cache adapters use.
- `autumn.Fs`: the same `fs/` tree the FUSE mount and the S3 gateway serve,
  without a mount.

A namespace must exist before it serves keys, and a cluster with authorization
on needs `principal=` and `credential=` on connect. See the autumn-rs
[README](https://github.com/journeymidnight/autumn-rs#readme) and `docs/ops.md`.

Build from source, in this directory:

```bash
maturin build --release --features abi3   # one wheel for CPython >= 3.11
maturin build --release              # bound to this interpreter, any CPython >= 3.8
maturin build --release --features abi3,ucx   # what is published: TCP, and UCX when present
```

License: Apache-2.0.
