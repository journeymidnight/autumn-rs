# autumn-rs

**One storage engine for AI data — models, datasets, vector tables and
inference KV cache on one data plane.**

autumn-rs is a distributed storage engine in Rust: replicated, append-only
extents under an ordered key-value store with partition split and merge, with
a manager on etcd. Files, KV pairs and S3 objects all land in the same extents.

<p align="center">
  <img src="assets/architecture.svg" alt="autumn-rs architecture: a POSIX mount, an S3 endpoint and KV-cache adapters over one partition layer and one stream layer" width="860">
</p>

- **One copy of the data, three ways in.** An object written over S3 is a file
  to the mount.
  - POSIX mount: `autumn-fuse`, or `autumn.Fs` in Python
  - KV client: CLI, Rust, Python
  - S3 endpoint: `autumn-s3`
- **A KV-cache tier for inference engines.** Prefix caches survive restarts and
  are shared across instances.
  - an LMCache L2 plugin
  - `autumn_kvcache` adapters: SGLang HiCache L3, vLLM connector
  - an optional DRAM tier in the partition server: pages are acknowledged from
    memory and written to disk behind
- **Fast.** Thread-per-core on io_uring, zero-copy RPC, optional UCX/RDMA.
  - 8 MiB writes from a client on a second host: 1,821 MB/s over UCX, 414 MB/s
    over TCP
  - 8 MiB reads over UCX: ~87% of 100 GbE
  - 4 KiB KV on Kubernetes (3 PS, 5 EN on local NVMe, TCP, 64 keys per
    request): ~162K writes/s at p99 0.86 ms, ~507K reads/s at p99 0.38 ms
- **Durable without a local WAL.** The replicated extents are the journal.
  - sealed extents are erasure-coded in the background
  - `autumn-op repair` and `scrub` rebuild degraded or rotted copies
- **Elastic and operable.**
  - partitions split and merge under an auto-policy controller
  - a web dashboard; `autumn-op status` / `health` / `df`
  - Prometheus `/metrics` on every server
  - opt-in authorization with Ed25519 capability tokens

## Try it with docker compose

A one-host cluster from the published image, on a Docker host with a Linux
kernel ≥ 5.15: etcd, a manager, three extent nodes, a partition server and the
dashboard. Authz is off, there is one manager, and the nodes share one disk.

```bash
docker compose -f deploy/compose/docker-compose.yml up -d
docker compose -f deploy/compose/docker-compose.yml run --rm cli \
    sh -c 'echo hello > /tmp/v && autumn-client --manager manager:9001 --namespace kvc put greeting /tmp/v \
           && autumn-client --manager manager:9001 --namespace kvc get greeting'
# the dashboard: http://127.0.0.1:8799
docker compose -f deploy/compose/docker-compose.yml down -v       # stop and wipe
```

The cluster takes a few seconds to bootstrap after `up -d` returns; until it
has, the `put` fails with `routing error: key is out of range`. Run it again.

## Run it from source

Linux ≥ 5.15 (io_uring), Rust ≥ 1.95, `etcd` ≥ 3.5 in PATH; on x86_64 a CPU
with SSE4.2. Without systemd (containers, non-root) the deployer runs plain
processes.

```bash
cargo build --release -p autumn-server
cd deploy/baremetal
./autumn-deploy -t topology-singlehost.conf start   # etcd + manager + 3 ENs + 1 PS, bootstrapped
./autumn-deploy -t topology-singlehost.conf status

# Nothing is served until its namespace exists: enable what you use.
AO="../../target/release/autumn-op --cluster-secret-file $HOME/.autumn-deploy/cluster.secret --manager 127.0.0.1:9001"
$AO namespace-create --name fs --parts 6     # files: autumn-fuse / autumnfs / autumn.Fs / autumn-s3
$AO namespace-create --name kvc --parts 16   # KV cache: sglang / vLLM adapters

./autumn-deploy -t topology-singlehost.conf destroy --wipe   # tear down
```

Authz is on by default: `start` writes one credential per namespace under
`$HOME/.autumn-deploy/authz/`, and the clients below pass it.

## Use it

### KV

- CLI: `autumn-client put | get | ls | put-stream` against a namespace.
- Rust: `autumn_client::ClusterClient` in `crates/client`.
- Python: `pip install autumn-rs`, then `autumn.Client`; see
  [`python/README.md`](python/README.md).

```bash
AC="./target/release/autumn-client --manager 127.0.0.1:9001 --namespace kvc \
    --credential-file $HOME/.autumn-deploy/authz/kvc.cred"
echo hello | $AC put mykey /dev/stdin
$AC get mykey                                       # → hello
```

### Files

- `autumn-fuse`: a POSIX mount for models, datasets and checkpoints.
- Reads go through the kernel page cache and the daemon's own parallel
  readahead.
- A HuggingFace cache works as is, symbolic links included:
  `hf download <repo> --cache-dir /mnt/autumn/hf`.
- The same tree without a mount: `autumn.Fs.connect("127.0.0.1:9001")` in
  Python, `autumnfs` in the shell.

```bash
cargo build --release -p autumn-fuse           # needs libfuse3-dev
mkdir -p /mnt/autumn
./target/release/autumn-fuse --manager 127.0.0.1:9001 --mountpoint /mnt/autumn \
    --transport tcp --credential-file $HOME/.autumn-deploy/authz/fs.cred &
cp model.safetensors /mnt/autumn/              # a regular filesystem, backed by the cluster
```

### S3: model weights and vector tables

- `autumn-s3` serves the file tree as an S3 endpoint: reads, writes, copy,
  delete, multipart, conditional writes.
- A bucket is a first-level directory, created beforehand.
- Model weights: point vLLM / SGLang's stock `--load-format runai_streamer` at
  it, with no engine patches.
- Vector tables: run the stock LanceDB client against it.

```bash
FS="--manager 127.0.0.1:9001 --credential-file $HOME/.autumn-deploy/authz/fs.cred"
./target/release/autumnfs $FS mkdir /tables
./target/release/autumn-s3 $FS --port 9100 &
cd examples/lancedb-s3 && uv sync
uv run python workload.py http://127.0.0.1:9100 tables demo    # create, append, search, optimize, verify
```

### Inference KV cache

- LMCache (and vLLM through it): the autumn L2 plugin in
  [LMCache/lmcache_l2_plugins](https://github.com/LMCache/lmcache_l2_plugins).

  ```bash
  pip install "lmcache-l2-plugins[autumn] @ git+https://github.com/LMCache/lmcache_l2_plugins"
  ```

- SGLang HiCache: the
  [`autumn_kvcache`](https://github.com/journeymidnight/autumn_kvcache) adapter.
- Both are pure Python on the `autumn` package, with no extra daemon.
- To serve pages from DRAM, start the partition server with
  `--mem-tier-bytes <N> --mem-tier-prefix kvc/`. A crash loses only what had
  not landed on disk yet, so keep it to data that can be recomputed.

## License

Apache-2.0 — see [`LICENSE`](LICENSE).
