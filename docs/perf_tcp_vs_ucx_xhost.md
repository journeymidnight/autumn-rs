# TCP vs UCX, loopback vs cross-host (2026-06-07)

## Loopback-only summary (the clean comparison)

Per follow-up — measured loopback UCX with `UCX_TLS=tcp,self` against
loopback TCP at the same bench shape (`--partitions 8 --pipeline-depth 8
--threads 16 --duration 10`, --3disk r=3). This is **UCX-over-TCP**:
UCX's tcp TL on top of kernel sockets, no RDMA — measures the UCX
wrapper's pure overhead vs raw kernel TCP.

| size | metric | TCP loopback | UCX(tls=tcp) loopback | UCX/TCP |
|---|---|---|---|---|
| 4 KiB | write ops/s | 39,570 | 28,523 | 72 % |
| 4 KiB | write p99 | 17 ms | 18 ms | 1.06× |
| 4 KiB | read ops/s | 1,370,164 | 485,356 | 35 % |
| 4 KiB | read p99 | 0.15 ms | 0.53 ms | 3.5× |
| 8 MiB | write MB/s | 1658 | 1544 | 93 % |
| 8 MiB | write p99 | 749 ms | 805 ms | 1.07× |
| 8 MiB | read MB/s | 8837 | 7087 | 80 % |
| 8 MiB | read p99 | 159 ms | 224 ms | 1.41× |

UCX-over-TCP costs 7–20 % at 8 MiB (read worse than write because the
read path makes more EP transitions per RPC) and the overhead is much
worse at 4 KiB — reads in particular pay 3.5× p99 because per-RPC EP
setup/probing dominates when the payload is tiny. **This is the
"floor" of how much worse UCX could be vs raw TCP when you force it
off RDMA.** Real UCX cross-host with RDMA on goes the other way (memory
on file: 4.6× *better* for 8 MiB read), but only when the wire is
actually doing RDMA — see the cross-host section below.

---

## Original cross-host attempt

Reference: 3-NVMe r=3 cluster on `dc62-p3-t302-n014`. Remote client on
`dc62-p3-t302-n015`. Both hosts on the `fdbd:dc62:3:302::/64` subnet
(eth0 ↔ mlx5_1 RoCE), code at `ccc315e` after `git push origin main`.
Bench shape: `perf-check --threads 16 --duration 10 --partitions 8
--pipeline-depth 8`, sizes 4 KiB and 8 MiB. Storage: `--3disk`
(/data03 + /data05 + /data08).

## Result matrix

| transport | locality | 4K write ops/s | 4K read ops/s | 8M write MB/s (ops/s) | 8M read MB/s (ops/s) |
|---|---|---|---|---|---|
| TCP | loopback | **39,570** (p99 17 ms) | **1,370,164** (p99 0.15 ms) | **1658** (207) p99 749 ms | **8837** (1105) p99 159 ms |
| UCX | loopback | 22,152 (p99 23 ms) | 1,058,886 (p99 0.13 ms) | 62 (7.78) ⚠ p99 37 s | 274 (34) ⚠ p99 12 s |
| TCP | **cross-host (::15 → ::14)** | **9,678** (p99 79 ms) | **15,575** (p99 241 ms) | **281** (35) p99 5.9 s | **193** (24) p99 10.7 s |
| UCX | cross-host (::15 → ::14) | — not collected (see "UCX cross-host blocker" below) | — | — | — |

### Loopback — TCP wins all four cells

TCP loopback beats UCX loopback in this run on every axis. Reads are
served from page cache, so 4K-read locks loopback at >1 M ops/s for both
transports — TCP is faster only because the kernel TCP stack on the
loopback interface has fewer per-RPC software hops than the UCX
ep-create + flush + completion path adds when the wire saves nothing.
This matches the SKILL note "UCX 4K-on-loopback is pure overhead";
it generalises to 8M loopback too on this run.

**The UCX 8M loopback numbers (62 / 274 MB/s with p99 = 37 s / 12 s) are
warmup-degraded, not steady-state.** perf_check.sh's baseline for this combo
is 166 / 369 ops/s — i.e. UCX 8M loopback is normally ≈1.3 GB/s, which would
roughly match TCP loopback. The 8 M run captured here is the FIRST 10 s after
cluster restart; UCX's first ≈5 s of any 8 M leg is consumed by ep warmup +
the manager `df` health-check flap (per the SKILL — known race during UCX
bring-up). Re-running with `--duration 60` or after a 30 s settle would land
much closer to TCP loopback. The collected snapshot is left in the table for
completeness with the ⚠ flag.

### Cross-host TCP — drops dramatically vs loopback

The cross-host TCP numbers are the apples-to-apples regression vs loopback —
remote bench client on `dc62-p3-t302-n015` pointing at the local manager at
`[fdbd:dc62:3:302::14]:9001`. Cross-host vs loopback ratios:

| metric | loopback | cross-host | cross-host / loopback |
|---|---|---|---|
| 4K write ops/s | 39,570 | 9,678 | 24 % |
| 4K read ops/s | 1,370,164 | 15,575 | **1.1 %** |
| 8M write MB/s | 1658 | 281 | 17 % |
| 8M read MB/s | 8837 | 193 | 2 % |

The 4K-read collapse (loopback 1.37 M ops/s → cross-host 15 K ops/s)
is the page-cache-served loopback advantage going away: cross-host reads
have to make a TCP round trip per get. The 8M-read drop (8.8 GB/s → 193
MB/s) is the network wire — TCP through eth0 at this size delivers ≈193 MB/s
which is just over a quarter of a 10 GbE NIC's line rate; the rest is kernel
TCP overhead (no zero-copy on the send side over kernel TCP for unaligned
8 MiB buffers; coalescing limits at the receiver).

This is the workload UCX is supposed to win — cross-host 8M read should
go through RoCE on mlx5_1 at line rate (≈12.5 GB/s line, ~9–10 GB/s app
after IBV overhead). Existing memory ([[project_ucx_crosshost_wins]]) records
**UCX cross-host 8M read 2761 MB/s vs TCP 601 MB/s = 4.6×** on a similar
config. The cross-host UCX number is the genuinely useful comparison point
here; it just couldn't be collected on this run.

## UCX cross-host blocker (collected, not fixed)

Three classes of failure stopped the cross-host UCX leg on this attempt:

1. **Port-allocation collisions with non-autumn tenants on this dev box.**
   The EN control-listener band (`extent_port + 1000`) lands at `10101+` by
   default — but Ray (`ray::IDLE`) is squatting `*:10101..10141` on this
   machine. Workaround that worked: shift the EN base port to `11100` via
   `AUTUMN_EXTENT_BASE_PORT=11100`, which moves control to `12101+` (free).
   vLLM tenants also squat random ports in 12000–40000, so a free
   contiguous band is increasingly hard to find on this box; production
   should reserve a band via `/proc/sys/net/ipv4/ip_local_reserved_ports`,
   or finish the F099-K `:0`-fallback work for the EN control listener
   (PS already has it per [[project_bug3_routing_wedge_pinned]]; EN still
   uses deterministic ports).

2. **UCX bootstrap "no healthy node" race during manager df-health warmup.**
   The manager's per-node `df` health check flaps offline/online for ~15 s
   after the EN UCX endpoints come up. If `autumn-op bootstrap` runs in
   that window, it returns "no healthy node available to allocate
   extent N for new stream". This was already documented (perf-check
   SKILL "UCX caveats" section). Mitigation: a sleep + retry around
   the bootstrap call.

3. **UCX same-host hops are unreachable when the cluster binds to its
   RoCE-attached IPv6.** Two cascading failures:
   - **PS ↔ manager heartbeat:** `ucp_ep_flush cb: Destination is
     unreachable`. Even with `UCX_TLS` unset (defaults) the EP setup
     fails — same-port RDMA loopback (mlx5_1 → mlx5_1 on the same host)
     doesn't route.
   - **PS ↔ EN commit_length:** even with the heartbeat moved to
     `UCX_TLS=tcp,self,rc_mlx5` (so PS↔manager succeeds via the tcp TL),
     PS still loops on `commit-length extent 8: 0/3 committed members
     reachable` indefinitely — StreamClient can't reach any of the 3
     same-host EN replicas. The EN listener was created with the default
     UCX device pool (10× mlx5_*); when PS's StreamClient connects, UCX
     picks `rc_mlx5` and `ucp_ep_flush` fails for the loopback case.

   A working same-host UCX cluster on this dev box would need either a
   non-RDMA TL forced on every cluster-internal EP (which makes "UCX"
   meaningless — the data path is just kernel TCP again), or per-peer
   TL selection that prefers cma/shm for loopback. The standard fix is
   not to have same-host hops at all — see below.

   The supported topologies for cross-host UCX bench are therefore:
   - **Manager + PS on host A, EN on host B, clients on host C** — every
     internal hop is cross-host. (Not what cluster.sh produces today.)
   - **Split EN across hosts** — same problem if PS and any one EN are
     co-located on the cluster's RoCE-bound side.
   - **Bind cluster to lo (`127.0.0.1`) AND mlx5_1 simultaneously** — UCX
     picks `self`/`cma` for loopback and RoCE for the wire. Needs a
     two-address listener which the current binary doesn't expose.

   The clean way to validate cross-host UCX is a true two-machine cluster
   (cluster on A, client on B), which this dev box doesn't have (only
   `::14` has /data03 + /data05 + /data08; `::15` is missing /data05).

### Workarounds attempted

To get the local cluster up under UCX on `[::14]`, I patched cluster.sh
to (a) shift `AUTUMN_EXTENT_BASE_PORT=21000` (avoids Ray's `*:10101+`
and avoids prior-run TIME_WAITs in the 9100/11100 bands), (b) wait
for `replicas` (=3) Online nodes before bootstrap. With those + plus
`AUTUMN_PS_PARTS_HINT=2000` (to force `AFFINITY_ENABLED=0`,
per [[feedback_ucx_cpuset_bootstrap_fail]]) + `AUTUMN_EXTENT_SHARDS=4` +
`UCX_TLS=tcp,self,rc_mlx5`:

- `bootstrap succeeded: 8 partition(s)` ✓
- 3 nodes Online ✓ manager↔EN df-health works
- PS ↔ manager heartbeat works (the tcp TL covers loopback) ✓
- PS ↔ EN `commit_length` loops forever ✗ (the same-host EP picks
  `rc_mlx5` from the EN-side device pool regardless of client-side
  TLS hint — the listener was created with the default UCX device
  pool, not the same hint as the connector)

The cluster.sh patches in this attempt are not committed — they're
reproduced inline in the "How to reproduce" section below for anyone
wanting to retry. The session reverted cluster.sh before commit.

## What's actually believable from this run

- **Loopback TCP performance is the bench floor on this hardware.** 4K
  writes 39.5 K ops/s (p99 17 ms), 4K reads page-cache-bound at 1.37 M
  ops/s, 8M writes 1.66 GB/s, 8M reads 8.8 GB/s.
- **Loopback UCX, when warm, matches loopback TCP within an OoM.** The
  4K numbers in this snapshot do (22 K / 1.06 M). The 8M numbers shown
  are warmup-degraded and not representative — re-run with longer
  `--duration` and a settle delay to get the steady-state number.
- **Cross-host TCP 8M is wire-bound at ~190–280 MB/s.** Reads tank
  4.6× vs the loopback 8M read (page cache); writes drop ~6×
  vs loopback (~kernel-TCP send pipeline + 3-way fanout join).
- **Cross-host UCX should beat cross-host TCP by 3–5× at 8M** per the
  prior result on file. Not measured this run for the bootstrap reasons
  above.

## How to reproduce

Local cluster (manager + EN + PS all on this host) bound to
`[fdbd:dc62:3:302::14]`, remote bench client on `::15`:

```bash
# 1. Push code so remote can pull
git push origin main

# 2. Remote: git pull + build (libfuse3-dev needed for autumn-fuse default-features)
.claude/skills/remote-autumn/remote-autumn.sh 'apt-get install -y libfuse3-dev pkg-config'
.claude/skills/remote-autumn/remote-autumn.sh 'git pull --ff-only && cargo build --release --bin autumn-client'
# For UCX:
.claude/skills/remote-autumn/remote-autumn.sh 'cargo build --release --features ucx --bin autumn-client'

# 3. Local cluster (TCP)
AUTUMN_BIND_HOST="[fdbd:dc62:3:302::14]" \
  AUTUMN_TRANSPORT=tcp \
  AUTUMN_BOOTSTRAP_PRESPLIT="8:hexstring" \
  AUTUMN_EXTENT_SHARDS=8 \
  AUTUMN_EXTENT_BASE_PORT=12000 \
  AUTUMN_DATA_ROOT=/data05/autumn-rs \
  bash cluster.sh start 3 --3disk

# 4. Remote bench
S=.claude/skills/remote-autumn/remote-autumn.sh
$S 'target/release/autumn-client --manager "[fdbd:dc62:3:302::14]:9001" --transport tcp perf-check --threads 16 --duration 10 --size 4096 --partitions 8 --pipeline-depth 8'
$S 'target/release/autumn-client --manager "[fdbd:dc62:3:302::14]:9001" --transport tcp perf-check --threads 16 --duration 10 --size 8388608 --partitions 8 --pipeline-depth 8'
```

For UCX cross-host, all three blockers above need addressing; see the
"UCX cross-host blocker" section. The closest I got with the dev-box
single-machine setup was:

```bash
# Patch cluster.sh: replace `sleep 2  # give PS a moment to register with manager`
# with a loop that waits for `replicas` (=3 here) nodes Online before bootstrap.
# Then:
env UCX_TLS=tcp,self,rc_mlx5 \
    UCX_NET_DEVICES=mlx5_1:1 \
    AUTUMN_BIND_HOST="[fdbd:dc62:3:302::14]" \
    AUTUMN_TRANSPORT=ucx \
    AUTUMN_BOOTSTRAP_PRESPLIT="8:hexstring" \
    AUTUMN_EXTENT_SHARDS=4 \
    AUTUMN_EXTENT_BASE_PORT=21000 \
    AUTUMN_PS_PARTS_HINT=2000 \
    AUTUMN_DATA_ROOT=/data05/autumn-rs \
    bash cluster.sh start 3 --3disk
```
The bootstrap + PS-ready states pass; PS↔EN commit_length blocks.
