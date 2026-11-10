# Roadmap

What we plan to work on, grouped by area. Nothing here has a date. An item
moves when someone measures that it matters; several start with a measurement
rather than a design. If one of these is blocking you, say so in an issue:
that is the strongest signal for what comes next.

## Operability and deployment

- **etcd with authentication and TLS.** The manager talks plaintext gRPC to
  etcd today, with no user, password or certificates, so it cannot use a
  locked-down etcd.
- **Host names in server flags.** Clients resolve `host:port`; the manager,
  partition server and extent node still take IP literals, and the deployment
  scripts resolve names for them.
- **Extent node shards follow the cores by default.** Shard count is set by
  hand today and has to agree with the cores the node is pinned to.
- **Disk rebalancing inside a node.** New extents go to the least-loaded disk,
  but nothing moves existing extents off a disk that filled up first.
- **One placement policy for allocation and recovery.** The two paths rank
  candidate nodes differently, which shows up when nodes are drained one by
  one.
- **An RDMA (UCX) container image.** The published image is TCP-only; UCX
  needs a source build today.
- **Authentication on the dashboard.** It has none, so it has to sit behind a
  port-forward or a gateway.

## Performance

- **Smaller SST blocks for small random reads.** When the working set exceeds
  the block cache, each 4 KiB read fetches a whole block; read throughput then
  drops to a fraction of what the memtable serves. Measure block sizes against
  read, write and compaction cost first.
- **Background EC must not starve foreground writes.** Converting one large
  extent to erasure coding can push the coordinating node's append latency into
  hundreds of milliseconds.
- **A newer async runtime.** Move to a newer compio and use its zero-copy send
  and multishot receive where they cut kernel CPU on the TCP path.
- **Give the memory tier's arena back.** Large memory-tier values live in
  registered chunks that are never unmapped, so after a burst of large values
  the tier keeps its whole budget mapped and, under UCX, pinned.

## KV cache

- **A thin pooled cache tier above the partition servers** (write-through,
  selective persistence), for KV-cache workloads that want DRAM-speed reads
  across nodes. Design only, after the current adapters are saturated.

## Files (FUSE mount)

- **Daemon-side prefetch for sequential reads**, so a single reader gets the
  mount's parallel bandwidth without depending on the kernel's readahead
  window.
- **Large-IO tuning**: writeback cache and splice, to cut FUSE overhead on big
  sequential IO.
- **FUSE over io_uring and passthrough reads** on Linux 6.x kernels that
  support them.

## Partitioning

- **A split signal for partitions that grow by large values.** Splitting is
  driven by LSM size and load; a partition whose bytes sit in large values
  behind pointers can keep growing with nothing advising a split.

## Not planned

- **Multiple sites.** autumn-rs assumes one site and one flat network:
  clients dial partition servers and extent nodes directly.
