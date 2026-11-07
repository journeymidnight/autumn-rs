# scripts/

Test and verification drivers. None of them is needed to run a cluster; most
back a section of [`docs/ops.md`](../docs/ops.md), which has the expected
output and the environment each one needs. Most boot a throwaway cluster with
[`cluster.sh`](../cluster.sh) (data under `AUTUMN_DATA_ROOT`, default
`/tmp/autumn-rs`) and need a Linux host with `etcd` in PATH; the FUSE ones need
`libfuse3` and a user who may mount.

## Chaos and fault injection

| Script | What it proves |
|---|---|
| `transport_chaos.sh tcp\|ucx` | a 3-EN, 2-PS cluster keeps every acked write through process kills, respawns and PS failover, per transport |
| `manager_ha_chaos.sh tcp\|ucx` | a standby manager takes over while an acked-write loop runs |
| `etcd_chaos.sh [cluster]` | the manager survives etcd kill / outage / restart; `cluster` runs a 3-member etcd and kills one member first |
| `crosshost_chaos.sh tcp\|ucx` | the same faults across two real hosts (`AUTUMN_LOCAL_IP`, `AUTUMN_REMOTE_IP`, `AUTUMN_REMOTE_RUN`, …) |
| `vphead_chaos.sh` | the in-process `system_chaos` harness over several random seeds (`VPHEAD_SEEDS`) |
| `decommission_chaos.sh` | the full nemesis set, ending in a node decommission. With `AUTUMN_CHAOS_DECOMMISSION_KILL=1` (stop the node, then remove) it drains; the default live-node decommission is a known open failure the script's header describes |
| `selfheal_chaos.sh` | a bit-flipped WAL replica is healed from clean copies at partition open |
| `enospc_chaos.sh` | disk-full on one EN under live writes: classified, routed around, recovered (root, loop mounts) |
| `reshard_chaos.sh` | changing an EN's shard count keeps all data and moves no bytes |
| `kvcache_chaos.sh` | the Python KV-cache adapter under failover (needs the `autumn_kvcache` checkout) |
| `g2_crash_consistency.sh` | power-loss consistency on a LazyFS mount: only fsync'd data survives, and that is enough |

## FUSE mount

| Script | What it proves |
|---|---|
| `fuse_chaos.sh` | a file workload through the mount survives PS / manager / daemon restarts |
| `fuse_en_restart_chaos.sh` | an EN kill + restart loses or corrupts nothing seen through the mount |
| `fuse_rmw_chaos.sh` | partial in-place overwrites read-modify-write correctly (regression guard) |
| `fuse_page_cache.sh` | the page cache is kept across opens and never serves stale bytes |
| `fuse_inval_deadlock.sh` | a kernel cache invalidation cannot wedge the mount |
| `fuse_dead_peer_chaos.sh` | a peer that stops answering while TCP stays up does not wedge reads (uses `freeze_proxy.py`) |

## Rolling restart, wire compatibility, EC

| Script | What it proves |
|---|---|
| `rolling_restart.sh` | same-binary rolling restart of a running `cluster.sh` cluster with convergence gates and zero loss |
| `client_window_verify.sh` | a client below the wire ceiling works until the floor is raised past it |
| `ec_convert_progress.sh` | an EC conversion on a live 4-EN cluster, with its progress watched |
| `ec_crosshost_verify.sh` | the EC copy-on-write line across two hosts (`AUTUMN_XHOST_LOCAL`, `AUTUMN_XHOST_REMOTE`, …) |

## S3 gateway and memory tier

| Script | What it checks |
|---|---|
| `s3_write_check.py` | the write APIs with a real S3 SDK (boto3) against a live gateway |
| `s3_stall_check.py` | a large DELETE / AbortMultipartUpload does not hold up the gateway worker |
| `s3_put_bench.py` | PUT/GET throughput by concurrency, byte-compared |
| `mem_tier_hot_set.py` | hot values stay in the PS memory tier while cold ones stream past |

## Tools

| Script | Use |
|---|---|
| `check_roce.sh` | which RDMA device / GID / IP to give UCX (`--listen-candidates`) |
| `freeze_proxy.py` | a TCP relay whose flows freeze on SIGUSR1 (both sockets stay open) — the dead-peer shape |
| `manual_stream_test.sh smoke\|etcd\|all` | stream-layer smoke test: create, append, commit length, truncate, punchhole |
| `benchmark_append_stream.sh` | the stream-layer append benchmarks |
