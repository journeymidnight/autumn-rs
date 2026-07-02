# Bare-metal deployment (`autumn-deploy`)

`deploy/baremetal/autumn-deploy` is the deployment path for autumn on physical
servers — single-host or multi-host. It replaces the ad-hoc `start.sh` /
`stop.sh`. (`cluster.sh` stays as the dev/chaos/perf **test** harness; it needs
raw process kill/restart for fault injection and is not a deployment tool.)

It reads a declarative `topology.conf`, distributes the binaries, and manages the
cluster with one of two service backends, chosen automatically:

- **systemd** — Linux host booted with systemd and root/passwordless-sudo.
  Renders one unit per instance (`Restart=on-failure`, `LimitMEMLOCK=infinity`,
  boot-time enable). Production.
- **process** — anything else (macOS, containers, non-root). Backgrounded
  process + pidfile + logfile under `DATA_ROOT` — the same semantics `start.sh`
  had. This is the laptop / CI path.

Force one with `SERVICE_BACKEND=systemd|process` in the topology (`auto` is the
default).

## Requirements

- The autumn binaries built locally: `cargo build --release -p autumn-server`
  (autumn-deploy pushes them to each host's `DEPLOY_DIR/bin`).
- `etcd` ≥ 3.5 on each `ETCD_HOSTS` host (in `PATH` or `DEPLOY_DIR/bin/etcd`).
- `bash` ≥ 4 on the host running autumn-deploy. **macOS ships bash 3.2** as
  `/bin/bash` — `brew install bash` and run with that (`/opt/homebrew/bin/bash
  autumn-deploy …` or put it first in `PATH`).
- For multi-host: passwordless SSH from the deploy host to every `SSH_USER@host`.
- Linux ≥ 5.15 on the hosts that run the binaries (compio uses io_uring; it
  falls back to kqueue on macOS, so a Mac can run a local test cluster, but
  `autumn-fuse` and UCX/RDMA are Linux-only).

## Addressing rules (why the topology uses IPs, not names)

The autumn binaries parse every address with `SocketAddr::parse` — **IP literals
only, no DNS**. autumn-deploy resolves a name once for SSH, but what it bakes
into `--advertise` / `--manager` must be an IP. Two identity constraints follow:

- **Extent nodes**: the manager keys an EN's identity on its advertise address
  and an EN never re-registers a new address on restart. So an EN's host must be
  a **stable IP**. A rescheduled EN with a new IP becomes a phantom new node.
- **Partition servers**: the PS re-registers each partition's address on every
  open, so a PS can move to a new IP and heal its own routing. Per-partition
  listener ports are dynamic (base + monotonic ordinal); flat routable
  networking between hosts carries them.

- **Managers**: stateless (all state in etcd). PS + clients receive the FULL
  manager list and rotate on `NOT_LEADER` (the only failover path — the protocol
  carries no leader hint). An EN receives a SINGLE manager address (its
  background calls don't comma-split), so multi-manager failover for EN
  housekeeping RPCs is limited to whichever manager you list first.

## topology.conf

`deploy/baremetal/topology.conf` (multi-host) and `topology-singlehost.conf`
(single-host, replaces start.sh) are commented examples. Shape:

```bash
DEPLOY_DIR=/opt/autumn-rs           # binaries + rendered units per host
DATA_ROOT=/var/lib/autumn-rs        # etcd data, PS state, logs, pids
TRANSPORT=tcp                       # tcp | ucx
SERVICE_BACKEND=auto                # auto | systemd | process
SSH_USER=root

ETCD_HOSTS=(10.0.0.1)               # member m: client 2379+m*10, peer 2380+m*10
MANAGER_HOSTS=(10.0.0.1)            # >1 = HA (shared etcd, leader election)
MANAGER_PORT=9001

EN_BASE_PORT=9101
EN_NODES=(                          # each: "HOST|DATADIR[,DATADIR2,...]"
  "10.0.0.4|/data03/autumn"
  "10.0.0.5|/data03/autumn"
  "10.0.0.6|/data03/autumn"
)

PS_HOSTS=(10.0.0.9)
PS_BASE_PORT=9301
PS_PORT_STRIDE=200                  # headroom for per-partition listeners

REPLICATION=""  EC_LOG=""  EC_ROW=""  PRESPLIT=""   # "" = auto by node count
```

Instance derivation: `en<i>` binds `EN_BASE_PORT+i`; `ps<i>` gets `psid i+1` and
base `PS_BASE_PORT+i*PS_PORT_STRIDE`. Run multiple isolated clusters on one host
by also setting `ETCD_CLIENT_BASE_PORT` / `ETCD_PEER_BASE_PORT`.

## Commands

```bash
cd deploy/baremetal
./autumn-deploy -t topology.conf check      # validate topology + SSH + binaries + backend
./autumn-deploy -t topology.conf deploy     # push binaries to DEPLOY_DIR/bin on every host
./autumn-deploy -t topology.conf start      # ordered bring-up (below); runs deploy if needed
./autumn-deploy -t topology.conf status
./autumn-deploy -t topology.conf restart [etcd|manager|en|ps]   # role or whole cluster
./autumn-deploy -t topology.conf stop [etcd|manager|en|ps]
./autumn-deploy -t topology.conf destroy [--wipe]               # stop (+ delete data)
```

`start` ordering (ported from cluster.sh's proven bring-up, with the same
guards): **etcd → wait client port → managers → wait leader (`autumn-op info`)
→ per-EN idempotent `autumn-op format` + launch EN → wait N ENs Online →
streams-guarded `autumn-op bootstrap` → PS → wait a partition is served.** The
bootstrap guard makes `start` idempotent: on an already-bootstrapped cluster it
skips bootstrap and preserves data, so `stop` + `start` is a safe restart.

## systemd notes

Rendered units live at `/etc/systemd/system/autumn-<inst>.service` (e.g.
`autumn-manager0`, `autumn-en0`, `autumn-ps0`). Inspect the exact command with
`systemctl cat autumn-ps0`; follow logs with `journalctl -u autumn-ps0 -f`.
`TimeoutStopSec=70` gives the PS time to drain imm to `row_stream` on stop.

## Manual verification

Single-host, process backend (no root, no systemd needed):

```bash
cargo build --release -p autumn-server
cd deploy/baremetal
# point EN_NODES at scratch dirs if you don't have /data* disks
./autumn-deploy -t topology-singlehost.conf start
../../target/release/autumn-op --manager 127.0.0.1:9001 info      # 3 nodes, 1 partition
AC="../../target/release/autumn-client --manager 127.0.0.1:9001"
echo hello | $AC put mykey /dev/stdin && $AC get mykey            # → hello
./autumn-deploy -t topology-singlehost.conf stop
./autumn-deploy -t topology-singlehost.conf start                # must SKIP bootstrap
$AC get mykey                                                     # → hello (data survived)
./autumn-deploy -t topology-singlehost.conf destroy --wipe
```

Multi-host adds: passwordless SSH to each host, IP literals in the topology, and
(for production) systemd on the hosts. `check` verifies reachability + backend +
binary presence before any process is started.
