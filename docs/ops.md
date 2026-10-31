# autumn-rs — Operations & Manual Verification Manual

This is the operator/developer runbook: per-feature **manual verification steps**
(kept executable — repo rule: every feature keeps its manual-verify steps alive
here), observability, chaos suites, and CLI reference. For the user-facing intro
see [`README.md`](../README.md); for architecture see [`CLAUDE.md`](../CLAUDE.md)
and the per-crate `crates/*/CLAUDE.md`.

- [Binaries & ports](#binaries--ports)
- [Deployment](#deployment) (bare metal, Kubernetes)
- [Cluster secret](#cluster-secret)
- [Fuse daemon runbook](#fuse-daemon-runbook)
- [Cluster capacity — `autumn-op df`](#cluster-capacity--autumn-op-df)
- [Prometheus /metrics](#prometheus-metrics)
- [Disk-full (ENOSPC) behavior](#disk-full-enospc-behavior)
- [WAL replay self-heal](#wal-replay-self-heal-log_stream-bit-rot--truncated-replica)
- [Write outcomes and the tail seal on open](#write-outcomes-and-the-tail-seal-on-open)
- [Read route-around for Suspected nodes](#read-route-around-for-suspected-nodes)
- [Direct read on EC extents](#direct-read-on-ec-extents)
- [A/B-ing a wire-path change](#ab-ing-a-wire-path-change-and-the-three-traps-that-fake-the-answer)
- [Data-plane authz setup](#data-plane-authz-setup)
- [CLI cheatsheet](#cli-cheatsheet)
- [Chaos suites](#chaos-suites)
- [Rolling restart & upgrade versioning](#rolling-restart--upgrade-versioning)
- [Test matrix](#test-matrix)
- [Inode-lease + close-to-open coherence (in flight)](#inode-lease--close-to-open-coherence-in-flight)

## Binaries & ports

### Core-path performance validation

Replica CRC reuse and its isolated CRC CPU benchmark are summarized in
[`perf_baseline.md`](perf_baseline.md) ("Findings that hold"). Verify
`Cpus_allowed_list` in `/proc/<ps-pid>/task/<tid>/status` for P-log and P-sst:
the intended CPUs in startup logs are insufficient to establish actual pinning.

Use an isolated cluster and dedicated data directories. Keep RF, partition layout,
CPU affinity, UCX library/transport settings and dataset identical between builds.
Wait for the PS log's `partition server serving` marker: the first listener can
bind while other partitions are still replaying. Do not interpret that partial
startup as a benchmark result.

```sh
cargo test -p autumn-transport --features ucx --test init
cargo test -p autumn-client --lib
cargo test -p autumn-partition-server --lib
cargo test -p autumn-stream --test extent_pipeline --test extent_append_semantics
cargo test -p autumn-fs -p autumn-fuse --lib
cargo test -p autumn-manager --test system_fuse_read --test system_fuse_ns -- --include-ignored --test-threads=1

# Requires a registered bench namespace. Load the fixed 256-key set once.
cargo bench -p autumn-server --features ucx --bench core_path -- 127.0.0.1:29001 tcp 8388608 8 8 load
cargo bench -p autumn-server --features ucx --bench core_path -- 127.0.0.1:29001 tcp 8388608 8 8 write
cargo bench -p autumn-server --features ucx --bench core_path -- 127.0.0.1:29001 tcp 8388608 8 8 read
cargo bench -p autumn-server --features ucx --bench core_path -- 127.0.0.1:29001 tcp 8388608 8 8 direct
cargo bench -p autumn-fs --bench read_plan -- 127.0.0.1:29001 2000
```

For UCX use the RoCE address, `ucx` argument and explicitly pinned `UCX_NET_DEVICES`;
record `ucx_info -v`. Repeat read-only runs after writes/flushes settle. The fixed
benchmark samples one in 16 operation latencies. The read-plan benchmark uses
synthetic cached maps, so its timing measures planning only, not file throughput.
`perf-check --partitions` does not create partitions and its read dataset depends
on the preceding write count; retain it as an end-to-end smoke test, but use the
fixed-key benchmark for comparisons sensitive to cache/SST state.

| Binary | Default port | Role |
|---|---|---|
| `autumn-manager-server` | 9001 | Control plane (streams, partitions, recovery) |
| `autumn-extent-node` | 9101+ | Data plane (raw extent files on disk) |
| `autumn-ps` | 9201 binary default; deployments use 9301 (+ per-partition) | LSM partition server |
| `autumn-client` | — | Data-plane CLI (put/get/del/head/ls/perf-check/perf-clean) |
| `autumn-op` | — | Admin CLI (bootstrap/split/merge/compact/gc/info/df/format) |
| `autumn-stream-cli` | — | Low-level stream debugging |
| `autumn-fuse` | — | FUSE mount of the `fs/` namespace (entrypoint role `fuse`) |
| `autumn-s3` | 9000; examples use 9100 | S3-compatible gateway over the shared `fs/` tree |
| `autumn-dashboard` | 8799 | Standalone web UI (drives the cluster via `autumn-op`) |

All of the above ship in the container image (`deploy/docker/Dockerfile`);
`entrypoint.sh` dispatches `manager|extent-node|ps|bootstrap|fuse`, and anything
else is exec'd verbatim, so `autumn-dashboard` / `autumn-s3` / the CLIs run as
plain commands.

`autumn-client --help` / `autumn-op --help` lists subcommands. (The standalone
Python `python/dashboard/` was retired 2026-07-04 — folded into the manager; see
"Web dashboard + auto-policy controller" below.)

## Deployment

Two deploy paths ship: `deploy/baremetal/autumn-deploy` for physical servers and
`deploy/k8s/` (kustomize) for Kubernetes. `cluster.sh` is the dev/chaos/perf test
harness, not a deployment tool.

Constraints both paths are shaped around:

- **No DNS in the binaries.** Every address goes through `SocketAddr::parse`, so
  flags carry IP literals (IPv6 bracketed). autumn-deploy resolves names once for
  SSH; the k8s entrypoint resolves Service names with `getent`.
- **EN identity is the `node_uuid`** minted at `format` and kept in the data dir.
  The EN self-registers its current address and every shard port at each start,
  so an address change updates the same node.
- **The PS re-registers each partition's address on every open**, so a PS can
  move. Per-partition listener ports are dynamic and reached over flat routable
  networking, so clients run where they can reach PS addresses directly (on k8s:
  in-cluster; NodePort/LoadBalancer cannot represent per-partition ports).
- **Managers are stateless** (state in etcd). PS and clients take the full manager
  list and rotate on `NOT_LEADER`.
- **`bootstrap` is not idempotent**; both paths guard it by checking for existing
  streams (`autumn-op info --full`), so re-running a start is safe.
- **Every server needs the cluster secret** (`--cluster-secret-file`); see
  [Cluster secret](#cluster-secret). Both paths arm data-plane authz by default;
  see [Enabling authz](#enabling-authz).
- **EN data dirs must accept O_DIRECT** (append bursts ≥ 1 MiB go direct on
  Linux), or the EN refuses to start. `AUTUMN_EXTENT_DIRECT_IO=0` passes
  `--no-direct-io` instead; see [Direct I/O on the extent nodes](#direct-io-on-the-extent-nodes).
- **Run more ENs than the replication factor**: with `#EN == RF` one EN down stops
  writes (no fresh replica set can form); reads tolerate a down replica.
- **UCX needs UCX ≥ 1.17** on every host running a UCX process; see
  [UCX library](#ucx-library).

### UCX library

Two UCX defects shaped this section: the first killed the PS under 4 KiB UCX
reads, the second stalled 8 MiB UCX writes.

**1. Registration-cache race (UCX < 1.17).** Before 1.17, UCX's registration
cache let a `munmap`/`madvise` on one thread (an allocator purge) deregister a
cached region while a worker thread was taking it for a zero-copy send. The NIC
then fails the send and UCX aborts the process: `Local protection error ... SEND
[inl len 10] [va .. len 4096 lkey ..]`, a segfault, or `rcache.c:427 Assertion
'region->refcount == 0' failed`. The PS died this way under 4 KiB UCX reads (2
of 4 perf-check runs on 1.16; 0 of 14 on 1.20.1). Every UCX process (servers,
clients, fuse, Python `set_transport("ucx")`) refuses an older library at start:

```
autumn-transport: UCX 1.16.0 is too old: before 1.17 its registration cache can deregister memory under an in-flight send ...   exit=2
```

**2. Uninitialised status on an immediately completed stream recv (release
builds up to 1.20.1).** When the data is already queued, UCX completes
`ucp_stream_recv_nbx` inside the call and hands the callback `req->status`, a
field release builds never set for a stream recv (`stream_recv.c`, assert-only).
A recycled request reports whatever it held: `UCS_INPROGRESS` on the PS's
8 MiB receives 3–5 times per 10 s run, other stale values on clients. The PS
treated it as an error and ended the connection without a word on the wire (no
`ucp_ep_close`), so the client waited out its 8 s keepalive: 8 MiB writes ran
at ~75% of baseline with p99 10–12 s, and clients logged `rpc client reader
exited ... ucp_stream_recv cb: Unknown error 16/48/80`. The Ubuntu 24.04 package is built with assertions and never showed it. The transport no longer
asks for `UCP_OP_ATTR_FLAG_NO_IMM_CMPL` on receives: queued data is then
unpacked in place with no request, which also stops the request leak that path
had (those requests were never marked completed). A stock 1.17+ library works
unpatched; the fix also belongs upstream.

Ubuntu 24.04 packages 1.16, so install from source (needs the rdma-core
`libibverbs` headers and `librdmacm-dev`; connections by address go through
rdmacm). Plain `configure-release` is fine: assertions are not needed.

```bash
ucx_info -v                                   # Library version the binaries load
curl -LO https://github.com/openucx/ucx/releases/download/v1.20.1/ucx-1.20.1.tar.gz
tar xzf ucx-1.20.1.tar.gz && cd ucx-1.20.1
./contrib/configure-release --prefix=/usr/local --enable-mt --with-verbs --with-rdmacm \
    --without-cuda --without-rocm --without-java --without-go
make -j"$(nproc)" && sudo make install && sudo ldconfig
ucx_info -d | grep -E 'Connection manager: rdmacm|Transport: rc_mlx5' | sort -u
cd <autumn-rs> && cargo clean -p ucx-sys-mini   # regenerate bindings against /usr/local
cargo build --release -p autumn-server --features ucx
ldd target/release/autumn-ps | grep libucs    # → /usr/local/lib/libucs.so.0
```

The regression tests need a RoCE host (`AUTUMN_UCX_TEST_BIND='[<roce-ip>]:0'`,
`UCX_TLS=rc_mlx5,ud_mlx5,tcp,self`, `UCX_NET_DEVICES=<dev>:1`):

```bash
# 1: 60 s of 64 KiB rendezvous sends while other threads release pages.
#    Against a 1.16 library (e.g. via LD_LIBRARY_PATH) it segfaults in about
#    half the runs (2 of 4); run it a few times.
cargo test --release -p autumn-transport --features ucx --test ucx_rcache_race -- --nocapture
# 2: 200 messages queued before the receiver posts a recv; fails if any recv
#    completed through UCX's request callback (it does with NO_IMM_CMPL).
cargo test --release -p autumn-transport --features ucx --test ucx_immediate_recv
# 3: a recv future dropped mid-await (UCX cannot cancel stream operations): its
#    pool slab stays out of the pool until the bytes land, then is reused, and a
#    later read on that connection fails instead of losing bytes.
cargo test --release -p autumn-transport --features ucx --test ucx_dropped_recv
# 4: a stream send out of .rodata (unregistrable) arrives: stream sends go out
#    of the registered pool, never the caller's memory.
cargo test --release -p autumn-transport --features ucx --test ucx_send_staged
```

Perf-check on UCX 1.20.1 vs the 2026-09-28 baselines (system 1.16), over a
day of runs on a shared host: 8 MiB write 266–282 ops/s (baseline 269), 11.1 s
per 10 s run and no keepalive closes; 8 MiB read 740–950 (622); 4 KiB read
0.88–1.25 M (0.96 M); 4 KiB write 13–19 K (18 K), the same band the 1.16
library showed the same day.

### Bare metal (`autumn-deploy`)

Requirements: `cargo build --release -p autumn-server` on the driver host;
`etcd` ≥ 3.5 on each `ETCD_HOSTS` host (in `PATH` or `DEPLOY_DIR/bin/etcd`);
`bash` ≥ 4 on the driver (macOS ships 3.2: `brew install bash`); passwordless SSH
to every `SSH_USER@host` for multi-host; Linux ≥ 5.15 on server hosts (io_uring;
macOS runs a local test cluster on kqueue, but `autumn-fuse` and UCX are
Linux-only); UCX ≥ 1.17 on every host for `TRANSPORT=ucx` ([UCX library](#ucx-library)).

Service backend (`SERVICE_BACKEND=auto|systemd|process`): **systemd** when the host
runs systemd and the user is root or has passwordless sudo (one unit per instance,
`Restart=on-failure`, `LimitMEMLOCK=infinity`, `TimeoutStopSec=70` so the PS can
drain on stop); otherwise **process** (background process + pidfile + logfile under
`DATA_ROOT`).

`deploy/baremetal/topology.conf` (multi-host) and `topology-singlehost.conf` are
commented examples:

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

`en<i>` binds `EN_BASE_PORT+i`; `ps<i>` gets psid `i+1` and base
`PS_BASE_PORT+i*PS_PORT_STRIDE`. Several clusters on one host also set
`ETCD_CLIENT_BASE_PORT` / `ETCD_PEER_BASE_PORT`. An EN takes a single manager
address, so EN housekeeping RPCs go to the first manager listed.

```bash
cd deploy/baremetal
./autumn-deploy -t topology.conf check      # topology + SSH + binaries + backend
./autumn-deploy -t topology.conf deploy     # push binaries to DEPLOY_DIR/bin
./autumn-deploy -t topology.conf start      # ordered bring-up; deploys if needed
./autumn-deploy -t topology.conf status
./autumn-deploy -t topology.conf restart [etcd|manager|en|ps]
./autumn-deploy -t topology.conf stop [etcd|manager|en|ps]
./autumn-deploy -t topology.conf destroy [--wipe]
```

`start` order: etcd → managers → wait for a leader → per-EN idempotent
`autumn-op format` + launch → wait for N ENs Online → guarded `bootstrap` → PS →
wait for a served partition. On a bootstrapped cluster it skips bootstrap, so
`stop` + `start` is a safe restart.

- **Cluster secret**: generated once at `~/.autumn-deploy/cluster.secret`
  (override `AUTUMN_CLUSTER_SECRET_FILE`), copied to `$DATA_ROOT/cluster.secret`
  (mode 600) on every server host and passed to every server and `autumn-op` call;
  `restart` re-copies it. Two clusters from one driver share it unless that
  variable differs.
- **Authz**: a signing key under `~/.autumn-deploy/authz/` (override
  `AUTUMN_AUTHZ_DIR`), generated once and reused so minted credentials keep
  working; after bootstrap one principal per family is minted: `fs.cred` (grant
  `fs/`), `kvc.cred` (grant `kvc/`). `AUTUMN_AUTH_SIGNING_KEY_FILE` brings your own
  key (you then mint credentials); `AUTUMN_AUTH_DISABLE=1` runs authz off.

  ```bash
  autumn-op --cluster-secret-file ~/.autumn-deploy/cluster.secret --manager $M info
  autumnfs    --manager $M --credential-file ~/.autumn-deploy/authz/fs.cred put F /F
  autumn-fuse --manager $M --credential-file ~/.autumn-deploy/authz/fs.cred --mountpoint /mnt/x
  # kvcache: extra_config auth_credential_file=~/.autumn-deploy/authz/kvc.cred
  ```
- **UCX** (`TRANSPORT=ucx`): hosts must be RoCE NIC IPs (a loopback host is
  refused unless the topology sets `UCX_TLS` itself). Every instance gets
  `UCX_TLS=rc_mlx5,ud_mlx5,tcp,self` and a pinned `UCX_NET_DEVICES` (default
  `mlx5_1:1`, a non-GPU NIC); topology values override. Positive lists only;
  never `posix`/`cma` (the posix large-message path stalls concurrent ≥ 64 KiB
  transfers).
- **systemd**: units at `/etc/systemd/system/autumn-<inst>.service`
  (`autumn-manager0`, `autumn-en0`, `autumn-ps0`); `systemctl cat autumn-ps0`,
  `journalctl -u autumn-ps0 -f`.

Verification, single host, process backend (no root, no systemd):

```bash
cargo build --release -p autumn-server
cd deploy/baremetal
# point EN_NODES at scratch dirs if there are no /data* disks
./autumn-deploy -t topology-singlehost.conf start
../../target/release/autumn-op --cluster-secret-file ~/.autumn-deploy/cluster.secret \
    --manager 127.0.0.1:9001 info                                # 3 nodes, 1 partition
# KV commands need BOTH a --namespace (every write declares its scope) and a
# credential (the data plane rejects a connection with no capability token:
# "permission denied: key requires a capability token"). `start` mints the
# per-family credentials; the principal travels inside the file, so there is no
# --principal flag. $HOME, not ~: a tilde does not expand inside double quotes
# and is not re-expanded out of $AC, so the client would get a literal path.
AC="../../target/release/autumn-client --manager 127.0.0.1:9001 --namespace kvc \
    --credential-file $HOME/.autumn-deploy/authz/kvc.cred"
echo hello | $AC put mykey /dev/stdin && $AC get mykey            # → hello
./autumn-deploy -t topology-singlehost.conf stop
./autumn-deploy -t topology-singlehost.conf start                # must SKIP bootstrap
$AC get mykey                                                     # → hello (data survived)
./autumn-deploy -t topology-singlehost.conf destroy --wipe
```

### Kubernetes (`deploy/k8s`)

One image (`deploy/docker/Dockerfile`) serves every role;
`deploy/docker/entrypoint.sh` dispatches on the first argument and does the k8s
glue (DNS → IP, env → flags, bring-up guards). The image is TCP-only (UCX would
need `hostNetwork`, an RDMA device plugin and memlock).

| Manifest | Kind(s) | Notes |
|---|---|---|
| `namespace.yaml` | Namespace `autumn` | |
| `storageclass.yaml` | StorageClass `autumn-en-local` | local disk for ENs |
| `configmap.yaml` | ConfigMap | shared env; `AUTUMN_EXPECT_NODES` must equal the EN count |
| `etcd.yaml` | StatefulSet + headless Service | 1 member, PVC on the default (network) class |
| `manager.yaml` | StatefulSet + ClusterIP + headless Services | readiness = leader-gated `autumn-op info`, so the ClusterIP reaches the leader |
| `extent-node.yaml` | headless Service | ENs are per-EN Deployments from `deploy/scripts/en-workload.sh`; advertise the pod IP |
| `partition-server.yaml` | StatefulSet + headless Service | advertises the pod IP; no PVC (its state is in the streams) |
| `bootstrap-job.yaml` | Job | guarded, run-once |

Apply order does not matter: each role waits for the manager leader, and the
bootstrap Job waits for `AUTUMN_EXPECT_NODES` ENs Online. All pods set
`enableServiceLinks: false` (the injected `AUTUMN_MANAGER_PORT=tcp://…` would
collide with the entrypoint's own variable).

**Storage.** ENs use local disk (`autumn-en-local`, `WaitForFirstConsumer`, which
pins an EN pod to its node; `reclaimPolicy: Retain`): autumn replicates itself, so
a self-replicating network volume would multiply the copies. The shipped class
uses `rancher.io/local-path`; for dedicated NVMe use static `local` PVs:

```yaml
# storageclass.yaml: provisioner: kubernetes.io/no-provisioner, then one PV per EN disk
apiVersion: v1
kind: PersistentVolume
metadata: { name: autumn-en-node1-nvme0 }
spec:
  capacity: { storage: 3Ti }
  accessModes: ["ReadWriteOnce"]
  storageClassName: autumn-en-local
  local: { path: /mnt/nvme0/autumn }
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - { key: kubernetes.io/hostname, operator: In, values: ["node1"] }
```

etcd uses the cluster's default network-durable class (`storageClassName`
omitted), so a single member survives node loss by re-attaching its volume. With
no default StorageClass the etcd PVC stays Pending; name a class in the overlay.
For control-plane HA run 3 etcd members with `--initial-cluster`.

**Build and deploy.**

```bash
docker build -f deploy/docker/Dockerfile -t autumn-rs:latest .
kind load docker-image autumn-rs:latest            # kind; else push to a registry
# base kustomization only (the vke overlay's deploy.sh creates it):
autumn-op gen-cluster-secret > cluster.secret
kubectl -n autumn create secret generic autumn-cluster-secret --from-file=cluster.secret
kubectl apply -k deploy/k8s                        # or: kubectl apply -k deploy/overlays/vke
kubectl -n autumn wait --for=condition=complete job/autumn-bootstrap --timeout=300s
kubectl -n autumn get pods,svc
```

The manifests use `autumn-rs:latest` + `IfNotPresent`, which suits kind/minikube;
on a shared cluster pin an immutable tag or digest (`images[].newTag`), or nodes
keep a stale `:latest`. Run `bash deploy/validate.sh` before applying (YAML,
cross-references, entrypoint dispatch, kustomization consistency, `bash -n`).

**Cluster secret and authz.** Every manager, PS and EN pod, the bootstrap Job and
the dashboard mount Secret `autumn-cluster-secret` (key `cluster.secret`, at
`/etc/autumn/cluster/`); `AUTUMN_CLUSTER_SECRET_FILE` in the ConfigMap points the
entrypoint at it. The fuse and S3 roles hold no secret. `deploy/overlays/vke/deploy.sh`
creates the cluster secret and the `autumn-authz` signing-key Secret once and never
rotates them; the manager mounts `autumn-authz` optionally and engages authz only
when it is present (`AUTUMN_AUTH_DISABLE=1` in the ConfigMap turns it off). Mint a
client credential:

```bash
kubectl -n autumn get secret autumn-cluster-secret \
    -o jsonpath='{.data.cluster\.secret}' | base64 -d > cluster.secret
autumn-op --manager <mgr> --cluster-secret-file cluster.secret \
    principal-create --principal app --grant fs/ > app.cred
kubectl -n autumn create secret generic autumn-credential --from-file=credential=app.cred
```

Client pods mount `autumn-credential` and pass `--credential-file` (native),
`auth_credential_file` (kvcache) or `credential=` (PyO3).

#### Per-cluster overlay

Keep `deploy/k8s` generic; put cluster specifics in an overlay listing `../../k8s`
as a resource. `deploy/overlays/vke` is a worked Volcengine example:

| Overlay knob | Why it is cluster-specific |
|---|---|
| `images[].newName/newTag` | registry + immutable tag/digest |
| `images[]` etcd mirror | nodes may not reach `quay.io` |
| `nodeSelector` patch | node pool (kernel ≥ 5.15 for io_uring) |
| etcd `storageClassName` + size | network-durable class; provider minimum size (Volcengine ESSD 20Gi) |
| EN count + `AUTUMN_EXPECT_NODES` | sizing; keep the two equal |

#### Scaling

- **Extent nodes**: one Deployment per EN. Add with
  `deploy/scripts/en-workload.sh apply <ordinal>` and raise `AUTUMN_EXPECT_NODES`.
  Retire with `deploy/scripts/en-decommission.sh <ordinal>` (fence → wait for
  rebuild → remove → delete the workload); deleting the workload directly drops a
  replica of every shard the node held. See
  [Node decommission runbook](#node-decommission-runbook-fence--drain--remove).
- **Partition servers**: raise `partition-server.yaml` `replicas` (psid =
  ordinal+1). Throughput scales with partitions, not workers per PS.
- **Managers**: raise `manager.yaml` `replicas` (3 for HA).

#### Multi-shard extent nodes

One shard per EN serves all its extent traffic from one io_uring core; under
durable RF=3 writes that core becomes the limit as partitions grow.
`AUTUMN_EXTENT_SHARDS=N` (default 1) runs N shards (`shard = extent_id % N`) on
the container's first N allowed cores (`Cpus_allowed_list`;
`AUTUMN_EXTENT_CPUSET` overrides). Shard `i` binds data port `9101 + i*10` and
control port `10101 + i*10`; ENs self-register every shard port, so there is no
Service port list. `deploy/overlays/vke` sets 4 shards and 4 CPU per EN.
Changing N on a running cluster is a stop-the-world reshard (restart all ENs
together; no bytes move): see
[Resharding an extent node](#resharding-an-extent-node--changing-its-shard-count)
and `scripts/reshard_chaos.sh`. A tmpfs `emptyDir` refuses O_DIRECT before Linux
6.6 and only pretends from 6.6 on; use a real filesystem or
`AUTUMN_EXTENT_DIRECT_IO=0`.

#### Using the cluster

Clients run in-cluster and take IP literals, so resolve the manager Service first:

```bash
kubectl -n autumn run kv --image=autumn-rs:latest --restart=Never -it --rm \
  --command -- bash -lc '
    M=$(getent hosts autumn-manager | awk "{print \$1}")
    echo hi > /tmp/v
    # --namespace is required for every KV command. With authz enabled add
    # --credential-file <the .cred for that family> to BOTH commands below,
    # or they answer "permission denied: key requires a capability token";
    # mount the credential into the pod and give its path here.
    autumn-client --manager $M:9001 --namespace kvc put k /tmp/v
    autumn-client --manager $M:9001 --namespace kvc get k'
```

Verification on kind/minikube: build and deploy as above; `kubectl -n autumn get
pods` shows etcd, manager, the ENs and ps-0 Running and Ready, and the round trip
above prints `hi`.

## Parameters (flags)

Every flag of every binary is declared in `crates/params/src/<role>.rs`, in
one struct per binary: ports, addresses, files, ids and switches as well as
the tunables. One parser reads them; no binary reads a tunable from the
environment. `--help` lists each flag with its default and range, and `runtime`
on a parameter that can also be changed while the process runs. Integer values
accept `64M` / `1G` / `512k`; `--flag=value` works as well as `--flag value`.

```sh
target/release/autumn-ps --help | less
# A bad or out-of-range value, a missing required flag, a removed or unknown
# flag exits 2 at once, before anything starts:
target/release/autumn-manager-server --recovery-max-global 0; echo "exit=$?"
#   error: --recovery-max-global 0: 0 is outside [1, 100000]   exit=2
target/release/autumn-manager-server --port x; echo "exit=$?"
#   error: --port x: "x" is not a number                        exit=2
target/release/autumn-ps --port 9201; echo "exit=$?"
#   error: --psid is required                                   exit=2
target/release/autumn-extent-node --data /x --shards 2; echo "exit=$?"
#   error: --shards was removed; pass --cpuset <SPEC> ...       exit=2
target/release/autumn-manager-server --no-such-flag 1; echo "exit=$?"
#   error: unknown argument "--no-such-flag"                    exit=2
cargo test -p autumn-server --test param_flags
```

autumn-op's global flags (`--manager`, `--json`, `--wait`, `--timeout`,
`--cluster-secret-file`, `--credential-file`, client parameters) stand anywhere
on the line, before or after the command (`autumn-op compact 7 --wait --json`).
autumn-client's and autumnfs's go before the command.

`--policy-fast-mode` on the manager sets the short policy windows first, so an
explicit flag after it still wins (`--policy-fast-mode --gc-cooldown-secs 600`).

The client-side binaries take flags the same way: autumn-fuse, autumnfs and
autumn-s3 accept their own set plus the file-system layer (`--write-buf-extents`,
`--lease-heartbeat-secs`, ...) and client SDK sets (`--rpc-timeout-ms`,
`--batch-get-concurrency`, `--ucx-regpool-cap-bytes`, ...); autumn-client and
autumn-op accept the client SDK set (autumn-op anywhere on the line); gallery and
autumn-dashboard their own. autumn-fuse and autumnfs name their lease identity
with `--host` (default: the machine's host name; `HOSTNAME` is not read).

```sh
target/release/autumn-fuse --help | tail -40          # fuse + fs + client parameters
target/release/autumnfs --put-depth 0 ls; echo "exit=$?"      # exit=2
cargo test -p autumn-server --test param_flags
cargo test -p autumn-fuse --test param_flags
cargo test -p gallery --test param_flags
```

The PS and EN fetch the authz config every `--config-poll-secs` (30 s). A write to a
namespace created since the last fetch is not held back for that long: the PS answers
the first attempt `Unavailable`, fetches at once, and the SDK's retry is admitted.
Check it with `cargo test -p autumn-manager --test system_namespace_layer_a --
--include-ignored` (`a_namespace_created_after_the_ps_started_is_writable_at_once`
runs the PS at a one-hour poll).

### Changing a parameter on the running cluster

A parameter marked `runtime` can be overridden without a restart. The override
is stored by the manager (etcd, survives a leader change) and wins over the flag;
unsetting it returns every process to its own flag value. The keys are
`<role>.<flag>`: `manager.recovery-max-global`, `ps.gc-rate-bytes-per-sec`,
`en.recovery-parallelism`, ... The manager applies its own at once; each PS and EN
picks it up at its next config poll (`--config-poll-secs`, 30 s).

```sh
AO="target/release/autumn-op --cluster-secret-file $SECRET --manager $MGR"
$AO config keys                                   # every runtime key, default, range
$AO config set en.recovery-parallelism 4 --by alice
$AO config set ps.gc-rate-bytes-per-sec 32M --by alice
$AO config list                                   # key, value, set_by, set_at
$AO config set en.recovery-parallelism 0 --by alice    # refused: outside [1, 16]
$AO config set ps.compact-max-files 2 --by alice       # refused: below compact-min-files
sleep 31; grep "runtime overrides installed" $LOGS/ps*.log $LOGS/node*.log
$AO config unset en.recovery-parallelism --by alice
cargo test -p autumn-manager --test runtime_config_etcd
```

The PS and EN log `runtime overrides installed` with the set they applied, and
`runtime override refused` for an entry they cannot apply (e.g. one that breaks a
cross-field check against that process's own flags); the other entries still
apply. Every `config set` / `unset` is in the audit log
(`$AO audit-log --op 18`). An extent node takes `--manager` as one address or
a comma-separated list; each poll (authz and runtime config, and its reconcile,
extent-info and recovery-validation calls) tries the addresses in turn, starting
from the one that answered last, and moves on after a transport error (a
timeout included) or a NOT_LEADER answer. A standby manager answers the stream,
extent and node lookups and the commit-length check NOT_LEADER: its metadata is
what it replayed when it started. To check it on a two-manager cluster:

```bash
# EN started with --manager m2:9001,m1:9001 (the standby first), m1 leading:
$AO config set en.recovery-parallelism 5 --by alice
sleep 31; grep "runtime overrides installed" $LOGS/node*.log   # recovery_parallelism 5
kill <m1-pid>; sleep 15                                         # m2 takes over
$AO config set en.recovery-parallelism 3 --by alice
sleep 31; grep "runtime overrides installed" $LOGS/node*.log   # now 3
grep "authz config poll failed" $LOGS/node*.log                 # nothing at WARN
cargo test -p autumn-manager --test en_multi_manager_etcd
```

Removed PS flags are refused by name (exit 2): `--fg-qps-quota`,
`--gc-debt-high-bytes`, `--compact-pending-high-bytes`, `--gc-cooldown-secs`,
`--compact-cooldown-secs` (they were never read; the advisory thresholds are the
manager's), `--min-pipeline-batch`, `--conn-threads`. `--data` was never a PS
flag and now fails too. Former env knobs are flags: the manager's recovery caps
(`--recovery-max-global` / `-per-source` / `-per-target`), node-suspected
timeout, in-flight sweep and connect / etcd timeouts; the EN's
`--prealloc-bytes`, `--ec-stripe-bytes`, `--ec-recovery-stripe-bytes`.

## Cluster secret

Every manager, PS and extent node must be started with the same
`--cluster-secret-file <PATH>`; without it the binary exits at once (exit 2,
`--cluster-secret-file is required`). A connection that declares itself a
cluster member (Peer) or an operator tool (Admin) proves it holds the secret
right after VERSION_HELLO (PEER_AUTH, HMAC challenge-response, both ways); a
connection that cannot is closed before any request is read. Clients (SDK, fuse,
kvcache, S3 gateway, `autumn-client`) never hold it — they authenticate with a
data-plane credential when authz is on. Design: `docs/cluster_secret_design.md`.

**autumn-op connects as an operator, so every command that talks to a cluster
needs `--cluster-secret-file`** (before or after the subcommand), read-only ones
included. The examples in this manual leave it out, and often `--manager` too:

```bash
DR=${AUTUMN_DATA_ROOT:-/tmp/autumn-rs}       # cluster.sh keeps the secret in $DR/cluster.secret
AO=(autumn-op --cluster-secret-file "$DR/cluster.secret" --manager 127.0.0.1:9001)
"${AO[@]}" info
```

The admin token is gone: operator-only manager RPCs (fence / remove / merge /
bootstrap / principal / namespace / presplit / op-submit / auto-policy …) are served only on
an Admin connection, and an Admin connection exists only with the secret.
`--admin-token` / `--admin-token-file` are refused by name on the manager and
autumn-op (autumn-dashboard rejects them as unknown flags).

**Generate and distribute:**

```bash
autumn-op gen-cluster-secret > cluster.secret      # 64 hex chars; needs no cluster
chmod 600 cluster.secret                            # same file on every node and operator host
autumn-manager-server --cluster-secret-file cluster.secret …
autumn-extent-node    --cluster-secret-file cluster.secret …
autumn-ps             --cluster-secret-file cluster.secret …
```

`cluster.sh` generates `$DATA_ROOT/cluster.secret` on first start (or uses
`AUTUMN_CLUSTER_SECRET_FILE`) and passes it everywhere; `reset` wipes it with the
data root.

**Verify** (dev cluster; each step must hold):

```bash
bash cluster.sh reset 3
DR=${AUTUMN_DATA_ROOT:-/tmp/autumn-rs}
autumn-op --manager 127.0.0.1:9001 info              # refused: "... requires the cluster secret;
                                                     #  start this process with --cluster-secret-file"
autumn-op --cluster-secret-file "$DR/cluster.secret" --manager 127.0.0.1:9001 info   # works
autumn-op gen-cluster-secret > /tmp/wrong.secret
autumn-op --cluster-secret-file /tmp/wrong.secret --manager 127.0.0.1:9001 info      # refused:
                                                     #  "... the two hold different secrets"
grep "PEER_AUTH refused" /tmp/autumn-rs-logs/manager.log   # the refusal, with the caller's address
```

A server logs every refusal at WARN with the caller's address, declared role and
service: `PEER_AUTH refused a connection holding a different cluster secret` (a
process configured with another secret) and `PEER_AUTH: connection gave no
cluster-secret proof` (a process with none, which gives up after the challenge).
Grep both to find a misconfigured process after a rollout.

The dialing side decides by the address it dialed, never by what the other end
claims to be. A PS or EN refused by one of its `--manager` addresses is not a
member of this cluster: it logs `PEER_AUTH failed against this process's
manager ... exiting` at ERROR (with `peer=<addr>`) and exits with status 1.
Refused by any other address (an extent node, a partition server, a stranger on
a member's address), it logs `PEER_AUTH failed: the peer holds a different
cluster secret; treating it as unreachable` at ERROR and keeps running; that
node is handled like any unreachable one. If the dialer was the misconfigured
one after all, its next call to the manager is refused and it exits then. The
manager never exits on a refusal; autumn-op only prints the error.

`cargo test -p autumn-server --test cluster_secret` runs the same checks against
the real binaries (manager and EN, Peer and Admin, no / wrong / right secret),
plus both dialer outcomes: an EN whose manager comes back with another secret
exits, and a manager refused by a stranger on an EN's address keeps running,
whether the stranger claims to be an extent node or a manager.

**Upgrading a cluster that used the admin token** is a stop-the-world change
(wire 52, and every Peer/Admin connection now runs PEER_AUTH):

1. Generate one secret and put it on every manager, PS, EN and operator host
   (in k8s: the `autumn-cluster-secret` Secret, see [Deployment](#deployment)).
2. Stop everything. Replace the binaries, `autumn-op` and `autumn-dashboard`.
3. Start with `--cluster-secret-file` on every server; drop `--admin-token[-file]`
   from the manager, autumn-op and dashboard command lines (they are refused).
4. Embedded clients inside the client window keep working. On a cluster with
   authz on, a client built before this change does not send CLIENT_AUTH to the
   EN, so its direct reads fall back to the PS proxy (slower, still correct)
   until it is rebuilt.

**Rotation** needs a full stop as well: a process knows one secret. Replace the
file everywhere, then restart everything.

## Async ops (op-ledger)

The seven long-running ops — `split` / `merge` / `rebalance` / `compact` / `gc` /
`forcegc` / `force-ec-convert` — are **asynchronous and uniform**: `autumn-op`
submits each to the leader's op-ledger and prints an `op_id` immediately instead
of blocking. This recovers the failure reason that the old fire-and-forget
`compact`/`gc` dropped — every op is queryable, including its error.

```bash
autumn-op compact 7                         # → submitted compact op <ID>
autumn-op ops status <ID>                   # pending|running|succeeded|failed|unknown (+ error/message)
autumn-op ops list --active                 # everything still in flight
autumn-op ops list --kind gc --limit 20     # recent gc ops
autumn-op gc 7 --wait --timeout 300         # block until terminal; non-zero exit on failure
```

- **`--wait [--timeout SECS]`** (global, default 600) blocks until the op reaches
  a terminal state and exits on its real outcome — for scripts (and `presplit`
  internally) that need the blocking error. Without it, poll `ops status`.
- **Where outcomes come from**: merge/rebalance close in-process on the
  leader; a split closes on its PS's reply or on its commit (see "A split whose
  reply did not arrive" below); compact/gc/forcegc run on the PS and report their terminal outcome +
  error back on the 5 s load heartbeat (so terminal state appears within
  ~5–10 s); ec-convert closes when the conversion applies.

### Watching a split or merge while it runs

Both **freeze the partitions for their whole duration** — writes stop — so the
question during one is "which step is it on, and has it stopped moving". They
report a PHASE on the load heartbeat:

```bash
autumn-op ops status <ID>        # progress_done / progress_total = phase / phases
```

| | split (6) | merge (4) |
|---|---|---|
| 1 | accepted; waiting on the maintenance gate, choosing a split key | owner lock held |
| 2 | frozen — writes stop here | both partitions frozen |
| 3 | drained (compaction + GC + flush) | both taken over, all six `commit_length`s captured |
| 4 | `commit_length` captured | metadata merge committed |
| 5 | metadata cut committed | — |
| 6 | unfrozen | — |

Phases, not bytes, because the steps cost wildly different amounts: the gate
wait, the median scan and the drain dominate, and a byte counter that sits still
through them reads as a hang.

**Phase 1 is where a split waits, and it is not frozen there** — it is queued
behind an in-flight compaction or GC on the partition, behind the PS-wide
compaction slots (`--major-compact-parallelism`, default 4: busy compactions of
OTHER partitions on the same PS hold a split here too), or scanning every SST
key for the median (no `--at`). `autumn-op ops --active` shows which: a
compact/gc on the same partition or PS, or none (the scan). A split sitting at 1 is normal and costs nothing but time.
**Phase 2 and 3 are the frozen ones**, and they are bounded: `FREEZE_TTL` is
30 s, so a split cannot sit frozen for minutes. If you see a partition frozen
longer than that, the freeze is orphaned, not slow.

**A split whose reply did not arrive.** The manager waits 60 s for the PS's
answer. Past that, `ops status` stays `running` with "outcome unknown (RPC
timed out after 60s)" — the PS keeps running the split, so it may still
commit. Do not resubmit to "retry" it: a resubmit attaches to the same op.
It ends on a fact: `succeeded` when the split commits ("split part P in two
(new part R)"), `failed` with the PS's own reason when it gives up, `failed`
"partition P was reopened" when its owner epoch moves, or `failed` "no load
report … has named this split for 30 s" when nothing on the PS is running it.
Once the manager says `failed`, that split can no longer commit (the manager
refuses an ended op at commit). A second split on a partition whose split is
still pending is refused by the PS at once (`split already in progress`).

To see it on a dev cluster, make the split outlive 60 s at phase 1 (queued
behind a long compaction on the same partition), then:
```bash
"${AO[@]}" ops status <ID>    # running … outcome unknown (RPC timed out after 60s) …
"${AO[@]}" split <PID>        # prints the SAME op id: attached, not a second split
"${AO[@]}" ops status <ID>    # → succeeded "split part <PID> in two (new part <R>)"
"${AO[@]}" info                 # one more partition, not two
```
The automated form (2 s timeout, split held at its commit point) is
`cargo test -p autumn-manager --test split_op_outcome`.

**A split or merge waits for EC conversions and rebuilds before it freezes.**
Its commit refuses while any extent of the partitions' streams is being
EC-converted or rebuilt, and those run for minutes. So the manager waits
first, with writes flowing, and `ops status` reads `running … waiting, writes
not frozen: ec conversion in flight on extent N` (or `recovery in flight`).
After 10 min it gives up: `failed … for over 600 s; split abandoned before
freezing writes`. While a split or merge is pending (waiting or running),
nothing new starts on its extents: `force-ec-convert` answers "… is being
split or merged; retry after it completes", gc / compact / forcegc for the
partitions are refused, scrub skips them, rebuilds and catch-ups wait, and a
sibling partition's GC punch on shared extents backs off. A second split or
merge of the same partition is refused at once. The auto-policy does not wait:
its split is refused with `…; split deferred` and retried after its cooldown.

To see it on a dev cluster with an EC-policy stream, start a conversion and
split while it runs:
```bash
"${AO[@]}" force-ec-convert --extent <EID>   # a sealed extent of partition <PID>
"${AO[@]}" split <PID>                       # → op id
"${AO[@]}" ops status <ID>                   # running … waiting, writes not frozen: ec conversion …
"${AC[@]}" put <key-in-PID> /etc/hostname    # succeeds: not frozen
"${AO[@]}" ops status <ID>                   # succeeded once the conversion lands
```
If an EC conversion or rebuild still starts after the wait, the split's PS
aborts its commit at once and unfreezes (`failed … ec conversion in flight on
extent N; retry split`) instead of holding writes frozen for ~20 s. The
automated form is `cargo test -p autumn-manager --test topology_waits_for_extent_ops`.

Samples arrive on the 5 s heartbeat, so a split that finishes in under ~5 s may
show no intermediate phase at all — that is not a fault.

To see "slow" and "stuck" tell themselves apart on a dev cluster (3 ENs at RF3,
so every EN holds a replica), stall one EN while the op runs. Keep the stall under
the 30 s `FREEZE_TTL`, and for a merge under the ~10 s soft timeout too, or the EN
turns suspected and the merge's new log tail cannot be placed:

```bash
DR=${AUTUMN_DATA_ROOT:-/tmp/autumn-rs}
AO=(autumn-op --cluster-secret-file "$DR/cluster.secret")
EN=$(cat "$DR/pids/node2.pid")
kill -STOP $EN; "${AO[@]}" split <PID>         # or: merge <S> <V>; prints "submitted … op <ID>"
"${AO[@]}" ops status <ID>                     # sits at 1/6 or 2/6 (merge: 1/4) while stopped
kill -CONT $EN                                 # → succeeded 6/6 (4/4) within a second
```
Measured 2026-09-27: split held 1/6–2/6 through a 15 s stall and finished 6/6
0.3 s after SIGCONT; merge held 1/4 through 7 s and finished 4/4. Split's phases
come from the PS (`set_maintenance_phase`), merge's from the manager's
orchestrator; with either report removed, the same run shows no phase at all.

**A partition reload with many client connections no longer stops its PS.**
A PS that stops heartbeating right after `reloading partition N due to region
change` while its process stays alive was the reload's connection fan-out
deadlocking the PS main thread (over 64 connections on one partition). Check:
`cargo test -p autumn-manager --test partition_reload_many_connections`
(300 idle connections, a range change, the PS must stay registered and the
partition reopen; ~15 s).

**Extent allocation refuses a stream that changed under it.** An allocation
whose stream record changed while it was creating the new extent's files
(extents added or removed, or its EC shape changed) is refused — the message
reads `membership changed during alloc_extent` either way — and the writer
retries with a fresh view. One whose partition changed owner meanwhile is refused with
`owner_epoch mismatch`: the old PS does not retry, it gives the partition up
and reopens it under a fresh epoch. Check: `cargo test -p autumn-manager --test alloc_extent_races` (~6 s).

**A merge freeze no longer stops a partition behind a compaction.** The
freeze waits for the partition's maintenance gate with writes still flowing,
and halts writes only once it holds it. If a long major compaction holds the
gate past the manager's 30 s freeze call, the merge fails ("freeze rpc ...
timed out"), the manager sends `freeze=false` to every side it sent a freeze to
(also to one whose answer never came), which ends the wait, and the policy retries later.
Symptom of the old behaviour: PUTs and the freeze to one partition both
unanswered while `ops --active` showed a compaction on it. Check:
`cargo test -p autumn-manager --test merge_freeze_waits_for_gate` (~2 s).

**A merge takes both partitions over before it measures them.** After the
freeze it acquires each partition's owner lock and fences its stream tails, so a
PS that restarts, gets unfrozen by a stale rollback, or outlives `FREEZE_TTL`
cannot ack a write the merge will seal away: the merge is refused
(`merge source reopened: ...`, `tail moved from fenced extent ...`,
`partition N took writes after its freeze drain`, or the generic CAS-conflict
precondition) and is retried later. A merge refused after the takeover (any
of those reasons) leaves both partitions fenced — one refused at the freeze
step fences nothing: the FIRST write to each fails (not acked; clients retry), the partition
logs `fenced (LockedByOther) — poisoning partition for fresh-epoch reopen` and
serves again after its next region sync. Check: after a refused merge, a
`put` then `get` of a key in each partition's range succeeds (allow one retried
`put`). The
races themselves are driven by
`cargo test -p autumn-manager --test system_merge_freeze_races -- --test-threads=1`
(PS restart, stale unfreeze before and after the takeover, commit after
`FREEZE_TTL`, a lost freeze reply — the side must take writes again at once,
not after the 30 s TTL — and a merge through a real etcd; ~2 min). Wire 61: the manager and
every PS must run the same build.

**`cannot split: partition has overlapping keys`** is not an error to chase: the
PS refuses to split while the LSM still has overlapping key ranges
(`has_overlap != 0`), which a MAJOR compaction resolves. You should not see the
auto-policy hit it any more — the flag reaches the manager on the load heartbeat
and `policy-candidates` emits `major compaction before split` in place
of the split, so run that compaction (or let a policy with the `compact` switch
on run it) and the split advisory returns on a later tick. `autumn-op info
--part <PID> --detail` prints `has_overlap` with the same note.

**What "split finished" means.** The op reaching `succeeded` means the metadata
cut is in effect and the new partition is being served — `autumn-op info` shows
one more partition immediately. It does NOT mean the data is physically
separated: the children share extents copy-on-write, so all of them keep
reporting the parent's size until compaction and GC reclaim what each no longer
needs. Do not wait for the sizes to fall, and do not re-split because they have
not: the size a split is judged on is the child's LSM, not the carried extent
footprint (see the next paragraph), and the children are in split cooldown for
an hour besides.

**What makes the policy recommend a split at all.** Three bottlenecks, each with
the metric that measures it — request rate (`req_per_sec`, one partition is one
thread), byte rate (`write+read_bytes_per_sec`, one partition is one log_stream),
and LSM size (`size_bytes`, which is what a key-range cut halves). **Carried
bytes are NOT one of them.** A large-value partition keeps its payload in the
shared log_stream, a CoW split leaves both children pointing at the same extents,
and only a major compaction separates them — so a partition holding 73 GiB behind
a 0 MiB LSM with no traffic is not a split candidate however large it looks in
`info`. Carried bytes remain the FLOOR under the rate triggers (nothing under
1 GiB is worth cutting) and the VETO on merge (two fat partitions must not become
one). If a partition you expect to split is silent in `policy-candidates`, read
its `lsm` and its bytes/sec before its total size:

```bash
autumn-op --manager $MGR --json info --part <PID> --detail \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(
      "lsm", d["size_bytes"], "iops", d["req_per_sec"],
      "B/s", d["write_bytes_per_sec"]+d["read_bytes_per_sec"],
      "has_overlap", d["has_overlap"])'
```
- **Auto-dispatched ops are tracked too.** Extent **recovery** (replica rebuild)
  is never submitted by an operator — it appears in the ledger on its own when
  the recovery loop dispatches it:
  ```bash
  autumn-op ops list --kind recovery
  # op 1170…  recovery  running   target=0->12  ERROR[4]: only 2/3 shards available (peer 83: short read)
  # op 1170…  recovery  succeeded target=0->10  recovered slot onto node 1
  ```
  A recovery **stays `running` while it retries** (the loop backs off but never
  gives up), carrying the **last** failure reason + `error_code` —
  so a repair that is looping instead of converging is visible per-extent, not
  just in aggregate `recovery-stats`.
- **Failover honesty**: the live ledger is leader-local (in-memory, cap 256).
  After a leader change, `ops status <old-id>` answers `unknown` (never a false
  `running`); durable terminal history is in `autumn-op audit-log`. A PS-executed
  op (compact/gc/forcegc) flips to `unknown` within one load report (~5 s) once
  its partition is reopened (PS restart / move / merge) — "partition N was
  reopened (owner epoch A -> B)…" — and a resubmit then starts a new op. One
  whose outcome never arrives without a reopen flips to `unknown` after 30 min.
  Manual check: `autumn-op compact <PART>` on a partition large enough to take
  a while, restart its PS while `ops status <ID>` shows a percentage, then
  `ops status <ID>` → `unknown … reopened …` within ~10 s of the PS serving
  again, and `autumn-op compact <PART>` returns a new op id.
- **Dedup**: re-submitting the same target while one is in flight returns the
  existing `op_id` ("attached") rather than double-dispatching.

## Auto-policy controller (in the manager)

The manager only *emits* advisories (pure mechanism); the leader-fenced
**auto-policy controller** *decides + actuates* per an active policy. It runs
in-process — one crash-safe, leader-owned task (it survives as long as the leader
does). It is **leader-only** (never runs on a follower) and has two modes,
`Off` and `Armed`. `Armed` actuates — the **mode is the whole gate** (arming is
per-policy; there is no separate process-wide flag). What a policy would act on
is the advisory list (`autumn-op policy-candidates`, the dashboard's
advisories), shown in either mode; there is no observe mode. Config is
persisted to etcd (`autoPolicy/config` + `autoPolicy/cooldowns`, leader-fenced)
so the active policy survives leader failover. A `mode = 1` left by a build
that had observe mode makes the manager refuse to lead
(`autoPolicy/config mode 1 ... run migratev1_v2`); `migratev1_v2` turns it off.

**Boot default.** The DEPLOY layer (entrypoint / autumn-deploy / k8s) seeds
`--auto-policy-default balanced`, which is seeded **Armed** — so a production
cluster boots running the `balanced` policy (GC + compaction + EC + region
rebalance — no split/merge) and actuates on its own, no extra flag. The seed
fires only on a FRESH cluster (no persisted `autoPolicy/config`) and is in-memory,
so the first operator change persists over it and a `deactivate` survives failover
(never re-seeded). `AUTUMN_AUTO_POLICY_DEFAULT=<preset|off>` changes or disables
it. **cluster.sh / chaos / perf leave it OFF** (they never set the env), so
dev/test behaviour is unchanged. Headless control:

```bash
autumn-op auto-policy status                 # mode + active + presets
autumn-op policy-candidates                  # what a policy would act on, whatever the mode
autumn-op ops list --active                  # an armed policy's actions: requested_by=auto-policy
autumn-op ops history                        # ... and how they ended, refusals included
autumn-op auto-policy start aggressive       # select + Armed (actuate)
autumn-op auto-policy deactivate             # mode → Off
```

Presets (safest → most aggressive): `gc-only`, `maintenance`, `space-reclaim`,
`balanced`, `aggressive`.

**Verify that an armed policy's actions are ops.** On a cluster with GC debt
over `gc_debt_high` on some partition:

```bash
autumn-op policy-candidates                       # a gc row for <PID>
autumn-op --json ops list --active | grep auto-policy   # → nothing while Off
autumn-op auto-policy start gc-only
autumn-op --json ops list | grep -A3 '"requested_by": "auto-policy"'   # a gc on <PID>
autumn-op ops history --kind gc                   # its outcome once it ends
```

The dashboard's Logs tab shows the same rows marked `auto-policy`; there is no
separate action log. Automated: `cargo test -p autumn-manager --lib
candidate_to_submit a_policy_repair_op`, `cargo test -p autumn-manager --test
extent_repair` (the armed repair policy leaves a SUCCEEDED `auto-policy` repair
op in the ledger).

## Web dashboard (server component)

The dashboard is a separate process built by the `autumn-server` package,
`crates/server/src/bin/autumn_dashboard` (the `autumn-dashboard` binary), which holds no cluster
state and drives the cluster ONLY through `autumn-op` (so the wire schema stays
in one place). It requires `--cluster-secret-file` and forwards the path to every
`autumn-op` call (autumn-op connects as an operator, which the manager refuses
without the secret, read-only calls included).

```bash
# Build both formal server binaries.
cargo build -p autumn-server --bin autumn-dashboard --bin autumn-op
# autumn-op must be on PATH (or pass --autumn-op /path/to/autumn-op).
autumn-dashboard \
  --manager 127.0.0.1:9001 \
  --cluster-secret-file /etc/autumn/cluster.secret \
  --port 8799                        # → http://<host>:8799/

# k8s (vke overlay ships it as its own Deployment + internal ClusterIP):
kubectl -n autumn port-forward svc/autumn-dashboard 8799:8799   # → http://localhost:8799/
```

**Seven tabs**, with the vital signs (topology / capacity / throughput /
controller) pinned above all of them. The tab is in the URL hash (`#nodes`), so
a view is linkable.

| Tab | What it answers |
|-----|-----------------|
| Overview | status bar (the `autumn-op status` counts against expected members, each member not up named), keyspace ribbon, fleet health roll-up, space + amplification, top advisories, what is running |
| Partitions | PS-scoped partition list + the lazy per-partition drawer (load metrics + extents) |
| Servers | every partition server member (an evicted one stays listed until `ps-remove`), with heartbeat, load and its partitions |
| Nodes | every extent node, with a **per-disk** table — capacity, online, faulted |
| Policy | advisories with their full reasoning, the controller, the policy editor |
| Logs | every op — the operator's and the auto-policy's alike — running, then durable outcomes, each marked with who asked |
| Metrics | charts of every PS's `/metrics`, scraped by the dashboard and kept in its memory, for the cluster or one PS; the hottest partitions now |

Manual actions map to the allow-listed `autumn-op` subcommands (`split` / `gc` /
`compact` / `merge` / `force-ec-convert` / `rebalance`).

Partition rows list `start` (inclusive) and `end` (exclusive) on two lines, each
cut in the middle (`…`) to fit the column. To check the layout, use neighboring
ranges with a long common prefix, resize the window, and scroll to the last
partition: each endpoint shows its head and its distinguishing tail, every row
is the same height and none overlap. Hovering a row or opening its drawer shows
the full range, including escaped binary key bytes.

The 2026-10-09 VKE fault test (two EN outages, two manager failovers, splits
and merges under load) ended with full acknowledged-data readback: zero missing
keys or mismatches. It also found the standby-promotion heartbeat bug, since
fixed (`ps_members_etcd::a_standby_promoted_late_keeps_the_heartbeating_fleet`).

**Metrics tab.** The one thing the dashboard does without `autumn-op`: it
scrapes each PS's `/metrics` at the address the PS reports on its heartbeat
(every PS serves one, by default at its `--port` + 500). `--metrics-interval-secs`
(10) and `--metrics-retention-secs` (3600) set the cadence and how much history
it keeps; the history is memory only and starts empty after a dashboard restart.
A PS's series are PS-wide; under the charts, "hottest partitions" is the
overview's current per-partition load, top 10 by iops (no per-partition history).

```bash
./cluster.sh start 3
# Each PS's reported endpoint (null = started with --metrics-port 0):
autumn-op --manager $MGR --json overview | python3 -c '
import json,sys
for p in json.load(sys.stdin)["ps_servers"]: print("PS", p["ps_id"], p["metrics_addr"])'
autumn-dashboard --manager $MGR --cluster-secret-file $SECRET --metrics-interval-secs 2 &
# After a few rounds, the history (scope=ps&id=N narrows it to one PS):
curl -s localhost:8799/api/metrics | python3 -c '
import json,sys; m=json.load(sys.stdin)
print(len(m["t"]), "rounds;", m["ps"]); print(sorted(m["series"])[:12])'
```

Run a perf-check write then read; on the Metrics tab requests/s and value
bytes/s rise, the block cache hit ratio settles, SST tables grow with flushes,
and a small `--flush-mem-bytes` shows minor compactions keeping the count down.
A PS with no endpoint is named on the tab ("no /metrics endpoint reported").
A scrape that fails names its PS and the error under the scope picker. To check
an endpoint by hand from the dashboard's host, send each request in two pieces
back to back (the second lands after the server's first read, as across nodes);
every reply must come back whole — an endpoint that answers before the end of
the request head resets most of these with errno 104:

```bash
python3 - PS_HOST 9801 <<'PY'
import socket,sys
for _ in range(8):
    c=socket.create_connection((sys.argv[1],int(sys.argv[2])))
    c.sendall(b"GET /metrics HTTP/1.0\r\nHost: "); c.sendall(b"x\r\n\r\n")
    r=b"".join(iter(lambda:c.recv(65536),b"")); print(r.split(b"\r\n")[0], len(r))
PY
```

**Manual check of the two tabs that exist for facts a roll-up cannot carry:**

```bash
# Nodes: each node's disks, individually. A node with disks [empty, full, full]
# rolls up as two-thirds free, so the per-disk row is the only place "this one
# is full" or "its own node calls it bad" can appear.
autumn-op --manager $MGR --json overview | python3 -c '
import json,sys
for n in json.load(sys.stdin)["nodes"]:
    print("node", n["node_id"], n["address"])
    for d in n["disks"]:
        print("   disk", d["disk_id"], "online" if d["online"] else "OFFLINE",
              "FAULTED" if d["faulted"] else "", d["free"], "/", d["total"])'

# Servers: the REGISTERED fleet. A PS serving no partition, or one that stopped
# heartbeating, appears here and nowhere else (the partition list can only show
# a PS that owns something). last_heartbeat_secs_ago = null means no heartbeat
# entry at all — defensive only, since replay and registration both seed one.
autumn-op --manager $MGR --json overview | python3 -c '
import json,sys
for p in json.load(sys.stdin)["ps_servers"]:
    print("PS", p["ps_id"], p["addr"], "parts", p["n"],
          "heartbeat", p["last_heartbeat_secs_ago"],
          "ready" if p["ready"] else "open %s/%s" % (p["open_count"], p["partition_count"]))'
```

**Split is refused on an un-separated CoW child, and the page says so first.**
After a split, both children share the parent's SSTs — which carry keys outside
each child's range — and `handle_split_part` refuses (`cannot split: partition
has overlapping keys`) until a MAJOR compaction rewrites them. The partition
drawer shows that precondition before the Split button is clicked, and the
auto-policy emits the compaction *in place of* the split, so the op history
stops filling with one refused split per window:

```bash
autumn-op --manager $MGR --json info --part <PID> --detail | grep has_overlap
# 1 → the Policy tab shows "major compaction before split".
#     After `autumn-op compact <PID>` clears it, the split advisory returns.
# NOTE: this candidate is a COMPACT, so a policy with `split` on and `compact`
#       off filters it out and nothing happens — visible, unlike a refusal loop.
```

**Auto-rebalance switch (Phase B).** A 6th policy switch,
`rebalance`, arms the automatic version of `autumn-op rebalance` (see "Rebalancing
region→PS assignment" below). When enabled + Armed, the controller emits a
cluster-level advisory whenever the per-PS partition-count spread exceeds
`rebalance_gap_threshold` (default 2) and actuates it by moving a bounded batch
(`rebalance_max_moves_per_tick`, default 4) per tick — gradual convergence, not a
storm. It is OFF in the conservative presets, ON in `balanced` + `aggressive`.
Custom-policy switches (incl. `rebalance`) are persisted in `autoPolicy/config`
(rkyv), so they survive leader failover; a pre-Phase-B config decodes unchanged
(the `switches` Vec is variable-length — an absent 6th switch reads as off).
The advisory THRESHOLDS live in the in-memory `PolicyConfig` (compiled defaults +
runtime override), like every other advisory threshold — not persisted.

**Verify leader-failover of the active policy** (the crash-safety guarantee):

```bash
# with an etcd-backed cluster:
autumn-op auto-policy start gc-only                # → mode=armed active=gc-only
kill -9 <leader-manager-pid>                        # crash the leader
# after the etcd lease expires (~10 s) a new leader wins + replays from etcd:
autumn-op auto-policy status                         # → STILL mode=armed active=gc-only
```

**HTTP access:** the dashboard has no per-request authentication or TLS.
Anyone who can reach it can submit controls with the cluster secret the
dashboard holds; the secret protects the manager RPC, not the dashboard caller. The VKE overlay
also publishes all paths through APIG Ingress, so its ClusterIP Service does
not imply private access. Preserve network controls or bind `--listen 127.0.0.1`
and tunnel. HTTP authentication is an existing non-goal retained in this move.

**Policy feedback:** rejected writes show the manager's reason and retain the
editor input; failed status queries display `unknown`. Each policy's **Start**
button selects that named policy and runs it after confirmation, even if none
was previously selected. **Stop** means Off. Status labels are Running /
Stopped. `auto-policy start` selects the policy and arms it in one request,
persisted as one config: if it fails, the previous name and mode are still in
place. Check it against etcd:

```bash
etcdctl get autoPolicy/config -w json | jq '.kvs[0].version'   # N
autumn-op auto-policy start maintenance
etcdctl get autoPolicy/config -w json | jq '.kvs[0].version'   # N+1
autumn-op auto-policy status                                  # mode=armed active=maintenance
cargo test -p autumn-manager --test auto_policy_start_etcd
```

autumn-op connects as an operator, which needs the cluster's exact wire
version, so it is rebuilt with the cluster for wire 73.

The first Policy panel is **Operational advisories**, separate from the
controller's selectable policies. A hot/cold row is information-only: over the
last five one-minute samples, partitions on one PS remained at least 10x apart
in QPS (hot side at least 10,000 QPS) or carried size (large side at least
25 GiB). It names both sides and the measured ratio. It does not map to an
operation; investigate whether a hot/large partition should split or eligible
cold/small neighbors should merge.

**Automated verification** (local isolated processes, also run in CI):

```bash
cargo build -p autumn-server --bins
node crates/server/src/bin/autumn_dashboard/tests/render_check.js
node crates/server/src/bin/autumn_dashboard/tests/tabs_smoke.js
node crates/server/src/bin/autumn_dashboard/tests/policy_controls.js
bash crates/server/src/bin/autumn_dashboard/tests/api_contract.sh
```

The API harness requires `etcd` and Python 3, discovers Cargo's target directory
(or accepts `AUTUMN_BIN_DIR`), allocates temporary data and a free port band,
and terminates only children it spawned. It checks served HTML bytes, disk/PS
fields, partition detail, nonempty durable operation history, policy
create/start/stop/delete with all switches off, invalid payloads, and
manager failure propagation.

## Fuse daemon runbook

autumn-fuse is a **consumer** — a POSIX filesystem client that runs on the
application node and talks to a *running* cluster's manager; it is not part of
cluster deployment. Start it directly:

```bash
cargo build --release -p autumn-fuse        # add --features ucx for a UCX cluster
MP=/mnt/autumn
mkdir -p "$MP"

# --transport MUST match the cluster's transport: the fuse daemon is a data-plane
# client (process-global), so a tcp fuse cannot reach a ucx cluster.
nohup ./target/release/autumn-fuse \
    --manager 127.0.0.1:9001 \
    --mountpoint "$MP" \
    --transport tcp \
    > /tmp/autumn-fuse.log 2>&1 &

# Verify it actually mounted — a bad --manager / transport mismatch makes the
# daemon exit within ~1 s, and without this check you'd think it succeeded.
sleep 1
mountpoint -q "$MP" && echo "mounted" || { echo "FAILED — see log:"; tail -20 /tmp/autumn-fuse.log; }

ls "$MP"; echo hi > "$MP"/x; cat "$MP"/x      # → hi
fusermount3 -u "$MP"                          # unmount (needs the `fuse3` package)
sleep 3; pgrep -x autumn-fuse || echo "daemon exited"   # it must; it used to stay up forever
```

The unmount ending the daemon is a test too (real mount; needs `/dev/fuse`,
`fusermount3`, and `cargo build -p autumn-fuse` first):

```bash
cargo build -p autumn-fuse        # the test runs the binary in target/debug
cargo test -p autumn-manager --features fuse-tests --test fuse_umount_exit
#   → the_daemon_exits_after_a_plain_unmount ... ok   (before the fix: "still running 10 s after the unmount")
```

A mount whose lease the manager forgot (its heartbeats came later than the
TTL) must drop what it cached about the inode — a writer that closes meanwhile
is not pushed to it any more — and, if it was the writer, must not flush its
dirty data over the writer that took the lease after it. In-process, two
mounts, ~30 s:

```bash
cargo test -p autumn-manager --features fuse-tests --test fuse_lease_loss
#   → a_lease_the_manager_forgot_invalidates_the_cached_inode ... ok
#     (before the fix: GETATTR answered the pre-loss size 0 for a 4096-byte file)
#   → a_writer_whose_lease_was_forgotten_does_not_flush_over_the_next_writer ... ok
#     (before the fix: once A's lease entry was gone — a re-open refused under B's lease,
#      or a close — A's flush went out anonymous and the file became A's 4096 bytes;
#      now A's fsync reports "write fenced" and B's 100 bytes stay)
```

A manager restart starts lease versions over, so a mount re-opening an inode it
still caches cannot rely on "newer version → reload"; the Open adopts the meta
it has just read. etcd-backed manager restarted in place; ~20 s:

```bash
cargo test -p autumn-manager --features fuse-tests --test fuse_lease_restart
#   → a_reopened_inode_takes_the_current_meta_after_a_manager_restart ... ok
#     (before the fix: cached version 5, re-acquired 2, GETATTR answered 0 for 4096 bytes)
```

If a previous daemon died it can leave a stale mount (`ls` reports "Transport
endpoint is not connected"); clear it before re-mounting with
`fusermount3 -u "$MP"` (or `umount -l "$MP"`).

**The mount is scoped to the WHOLE `fs/` namespace.**
`autumn-fuse` (and `autumnfs`, and the PyO3 `autumn.Fs.connect(...)`) takes no
scope flag — every inode/dirent/extent key lands under `fs/…` (one global tree).
A fuse mount, `autumnfs`, and the PyO3 client **all see the SAME filesystem**. To
run isolated filesystems in one cluster use DISTINCT NAMESPACES (`fsA`/`fsB`, each
`namespace-create`d). Inode numbers are cluster-unique (a single global counter,
etcd `autumn-rs/fs/next_inode`); a `schema_version` stamp makes a future
incompatible layout fail loud rather than mount empty
(docs/key_namespace_split_design.md §8).

**UCX (RDMA):** with `--transport ucx`, export the UCX env before launching (the
UCX C library reads it directly): a positive `UCX_TLS` list — never `^` negation —
and a pinned RoCE device, e.g.

```bash
export UCX_TLS=rc_mlx5,ud_mlx5,tcp,self       # NEVER add posix/cma (2026-07-03: the posix
                                              # large-message path stalls concurrent >=64K
                                              # transfers — 3s timeout storms)
export UCX_NET_DEVICES=mlx5_1:1               # verify: scripts/check_roce.sh --listen-candidates
ulimit -l unlimited                           # ibv_reg_mr pins registered buffers
```

UCX_TLS rule (one rule, 2026-07-03): UCX clusters bind **RoCE NIC IPs**
(127.0.0.1 is not an RDMA device address) and use
`UCX_TLS=rc_mlx5,ud_mlx5,tcp,self` + a pinned `UCX_NET_DEVICES` — the single
list serves both intra-host (rc loopback in the HCA) and cross-host traffic.
`cluster.sh` / `autumn-deploy` apply this automatically for `TRANSPORT=ucx`
and refuse a loopback bind (legacy shm-only loopback needs an explicit
`UCX_TLS=posix,cma,tcp,self` and has ≥64K transfers known-broken — the
loopback chaos harnesses set it themselves). Explicit env always wins.

**In Kubernetes**, the shipped image carries `autumn-fuse` and the entrypoint
dispatches it as the `fuse` role. Two shapes work, and the one to reach for
first is the single-container form — `autumn-fuse` and the app in ONE container,
sharing a mount namespace, with no volume and no propagation at all:

```yaml
containers:
  - name: app
    image: <CR>/autumn-rs:<tag>          # or your app image + the two binaries
    command: ["/bin/sh", "-c"]
    args:
      - |
        autumn-fuse --manager "$AUTUMN_MANAGER" --mountpoint /mnt/autumn &
        # Wait via /proc/mounts, never `mountpoint -q`: that stats the path, and
        # a half-started mount whose daemon then died blocks stat() forever.
        until grep -qs " /mnt/autumn fuse" /proc/mounts; do sleep 0.2; done
        exec <your app>
    env:
      - { name: AUTUMN_MANAGER, value: autumn-manager:9001 }
      - { name: AUTUMN_CREDENTIAL_FILE, value: /etc/autumn/cred/fs.cred }
    securityContext:
      privileged: true            # or capabilities.add:[SYS_ADMIN] + /dev/fuse device
    volumeMounts:
      - { name: cred, mountPath: /etc/autumn/cred, readOnly: true }
```

It costs privileged on the whole container; prefer the S3 gateway for clients
that already speak S3.

**The sidecar form needs something from the NODE that many clusters do not
give you.** Check before designing around it — from a privileged pod:

```bash
grep -c "shared:" /proc/self/mountinfo    # 0 => mountPropagation will not work here
```

On the cluster this was written against that returns **0**: kubelet is not
configured with a shared root mount, so `Bidirectional` / `HostToContainer` are
accepted by the API server and simply not honoured on the node — the FUSE mount
comes up `private`, and the app container sees an empty directory with no error
anywhere. Only use the sidecar below when that check returns non-zero.

```yaml
containers:
  - name: fuse
    image: <CR>/autumn-rs:<tag>
    args: ["fuse"]
    env:
      - { name: AUTUMN_FUSE_MOUNTPOINT, value: /mnt/autumn }
      - { name: AUTUMN_CREDENTIAL_FILE, value: /etc/autumn/cred/fs.cred }
    securityContext:
      privileged: true            # or capabilities.add:[SYS_ADMIN] + /dev/fuse device
    volumeMounts:
      - { name: mnt, mountPath: /mnt/autumn, mountPropagation: Bidirectional }
      - { name: cred, mountPath: /etc/autumn/cred, readOnly: true }
  - name: app
    volumeMounts:
      - { name: mnt, mountPath: /mnt/autumn, mountPropagation: HostToContainer }
volumes:
  - { name: mnt, emptyDir: {} }
```

The propagation pair is what makes the sidecar's mount visible to the app
container; without it — or on a node that does not honour it — the app sees an
empty directory.

**Know what `Bidirectional` costs you before reaching for it.** It exists to let
the sidecar's mount escape into the host mount namespace so the app container
can see it — which also means a mount that outlives the pod. Pair that with a
daemon that dies without unmounting and the leak is node-level, not pod-level:
every `stat()` crossing the corpse blocks in uninterruptible sleep, a container
runtime stats mount points on every sandbox create and teardown, and the node
stops being able to start any container while everything already running carries
on normally. Only a reboot or a host-side `umount -l` clears it. We lost five
nodes to exactly this shape. `MountOption::AutoUnmount` (now always set, see
`crates/fuse/src/main.rs`) is what closes it — fusermount3 drops the mount as
soon as the daemon's fd closes, SIGKILL included. **Do not run a build without
that fix under `Bidirectional`.** AutoUnmount makes fuser add `allow_other`, so a
daemon running as non-root needs `user_allow_other` in `/etc/fuse.conf` (as root,
as in our images, it does not).

The single-container form above avoids this escape entirely: nothing propagates,
so nothing can outlive the pod. That is the second reason to prefer it.

**Acceptance test for the mount-leak fix — run this on any new cluster before
trusting it with real work.** The failure it guards against cost us five nodes,
and nothing in the normal happy path exercises it: the daemon has to die
*ungracefully* for the bug to show. Kill it the way Kubernetes actually kills
things.

```bash
# 1. Mount, on a node you can afford to lose.
kubectl -n autumn run fusekill --restart=Never \
  --image=<CR>/autumn-rs:<tag> --overrides='{"spec":{"nodeName":"<NODE>",
  "containers":[{"name":"f","image":"<CR>/autumn-rs:<tag>","securityContext":
  {"privileged":true},"command":["bash","-lc"],"args":["autumn-fuse --manager
  $M --mountpoint /mnt/autumn --transport tcp --credential-file
  /etc/autumn/cred/fs.cred & sleep 3600"]}]}}'
kubectl -n autumn exec fusekill -- sh -c 'grep autumn /proc/mounts'   # mounted

# 2. Kill it the worst way — SIGKILL, no grace, so no trap and no Drop run.
kubectl -n autumn delete pod fusekill --force --grace-period=0

# 3. The node must still be able to start a container. This is the whole test.
kubectl -n autumn run afterkill --restart=Never --image=<CR>/autumn-rs:<tag> \
  --overrides='{"spec":{"nodeName":"<NODE>"}}' --command -- sh -c 'echo NODE_OK'
kubectl -n autumn get pod afterkill -w
```

`afterkill` reaching `Completed` within a minute or so is a pass. If it sits in
`ContainerCreating` with NO kubelet events at all — no `Pulling`, no `Created` —
the mount leaked and that node is wedged: `kubectl exec` into unrelated pods
there will hang next, and only a reboot (or the surgical unwedge below) clears
it. Note that `kubectl get nodes` will keep saying `Ready` the whole time, and
every already-running pod keeps serving, so the node looks fine.

**Telling a STALLED FUSE read from a merely slow one — without touching the
mount.** They look identical in a consumer's log: a large read simply produces
no output either way. The obvious check is the trap, because `ls`, `stat`, `du`
or anything else that walks the mountpoint is precisely what blocks forever if
the daemon has stopped answering, so the diagnostic joins the casualties and
takes your shell with it. Sample the reader's byte counter instead:

```bash
# In the pod, on the process doing the reading (not the mount):
for i in 1 2 3; do grep read_bytes /proc/<pid>/io; sleep 10; done
```

Growing = slow, not stuck. Frozen with the process alive = stuck; then confirm
from the daemon side with `grep fuse /proc/self/mountinfo` and
`cat /sys/fs/fuse/connections/*/waiting`, and look for the reader in state `D`.
The general rule this comes from, which cost us hours today in another form:
**a live PID is not progress.** A frozen `hf download` held its PID for 26
minutes while transferring nothing, and reporting an ETA off process liveness
would have been wrong by hours. Trust byte counters, not process existence.

**Surgical unwedge, no reboot:** on the node, `ls /sys/fs/fuse/connections/` —
each directory is a live connection; one with `waiting` > 0 and no `autumn-fuse`
process behind it is the corpse. `echo 1 > /sys/fs/fuse/connections/<N>/abort`
aborts it, and every process blocked on it gets an error instead of staying in
uninterruptible sleep. Confirm the shape first with `grep fuse /proc/self/mountinfo`
(never `stat` the path) and `cat /proc/<pid>/stack` on any process in state `D`
— expect `fuse_*` / `request_wait_answer` frames.

The entrypoint clears a stale mount before mounting, and it deliberately does
NOT use `mountpoint -q`: that stats the path, which is the one thing guaranteed
to hang on a stale FUSE mount, so the recovery check would itself wedge the
replacement container. It reads `/proc/mounts` (pure VFS metadata, never blocks)
and unmounts lazily (`fusermount3 -uz`). Env → flag: `AUTUMN_MANAGER`, `AUTUMN_FUSE_MOUNTPOINT`,
`AUTUMN_CREDENTIAL_FILE`, `AUTUMN_FUSE_DIRECT_READ`, `AUTUMN_FUSE_ALLOW_OTHER`.

**Reads go through the kernel page cache, and cached pages survive a reopen**
(`FOPEN_KEEP_CACHE`) unless the file may have changed: an open that finds this
mount already holding the file's lease keeps them (other clients' writes reach a
lease holder as invalidations); an open without one reads the file's content
generation from the PS and keeps them only if it is the one seen last time. So a
second load of the same weights is served from memory, and `MAP_SHARED` mmaps
(Python's `mmap.mmap(fd, 0)`) work.

**Readahead window.** The daemon never touches it: the mount starts with the
kernel default, 128 KiB, and raising it is an operator step. FUSE INIT can only
lower the window, so the one way up is the mount's
`/sys/class/bdi/<dev>/read_ahead_kb` — which a container's `/sys` refuses unless
the pod is privileged, so the daemon leaves it alone rather than fail or guess.
An mmap loader (safetensors `load_file`) has about one window in flight per
faulting thread, so on a network path the window is most of its throughput.
Measured at 4 ms per read: a loader pinned to one core got 108 MiB/s at 128 KiB,
720-740 at 2 MiB and 823 at 4 MiB; the same loader on nine cores got 1095-1334
at 2 MiB but only 124-188 at 4 MiB — the daemon received the same bytes as ~10x
as many ~36 KiB READs (why they fragment is not established). So 2 MiB is the
value to set, not larger, and it is not proven safe for every model: if a load
is far slower than expected, compare the daemon's READ count against the file
size. With no network latency 2 MiB costs 12-22% on nine cores against
128 KiB-1 MiB.

Set it after the mount is up, from wherever `/sys` is writable — the host, or a
privileged pod (`kubectl exec <pod> -- grep ' /sys ' /proc/mounts` says `rw` or
`ro`). Touch the mountpoint first: the kernel sets the window from the INIT
reply, which overwrites anything written before the daemon answered.

```bash
MP=/mnt/autumn
stat "$MP" >/dev/null                                    # INIT has been answered
DEV=$(awk -v m="$MP" '$5==m{print $3}' /proc/self/mountinfo | tail -1)
echo 2048 > /sys/class/bdi/$DEV/read_ahead_kb
cat /sys/class/bdi/$DEV/read_ahead_kb                    # 2048
```

It lasts as long as that mount: a restarted daemon is a new mount with a new
`<dev>` back at 128 KiB, so the step belongs wherever the mount is (re)made. Run
it in the daemon's container, or from the host through the daemon's own mount
namespace (a different container, or the host, may not see the mount at all):

```bash
PID=<autumn-fuse pid>                  # one per mount; pgrep -x autumn-fuse lists them all
stat /proc/$PID/root/mnt/autumn >/dev/null
DEV=$(awk '$5=="/mnt/autumn"{print $3}' /proc/$PID/mountinfo | tail -1)
echo 2048 > /sys/class/bdi/$DEV/read_ahead_kb   # the bdi is one kernel object
```

Without it everything works; mmap
loads are just slower, and the daemon's own readahead (`--prefetch-mem-mb`,
below) still covers sequential readers.

**`--prefetch-mem-mb` (default 1024; `0` turns it off)** is the daemon's own
readahead: when a file is read in sequence it fetches the blocks ahead of the
reader in parallel and answers the kernel's READs from memory. It is a cap, not
a reservation (a block is freed once read, after 5 s unread, when its file
closes, or when the file's content changes; a block that does not fit is simply
not fetched). It bounds the prefetched blocks only — reads the daemon serves
from the cluster hold their own buffers (measured: budget 128 MiB, RSS peak
287 MiB; budget 1024 MiB, six parallel readers, RSS peak 578 MiB). Measured at 4 ms per
read: single-stream `dd` 0.69 → 1.7 GB/s; vLLM loading Qwen3-VL-4B 7.9 → 4.8 s;
the vLLM-Omni load path on a bf16 DiT 1.05 → 1.44 GiB/s, **1.96 GiB/s with
`--disable-multithread-weight-load`**. It makes some loads slower: with no
network latency, and for fp32 checkpoints converted on the CPU (a multi-threaded
copy). Turn it off there.

**Loading weights for vLLM-Omni (MiniMax-H3, Wan):** a bf16 checkpoint loads
fastest with `vllm serve … --omni --disable-multithread-weight-load` (the
daemon's readahead then sees one sequential stream). Measured on real
MiniMax-H3 (4×H200, TP4, `--task-type fl2va`, 4 ms per read, data put with
`autumnfs put` into 24 lanes): model loading 364 s before the page-cache change,
243 s with it, 203 s with daemon readahead, 197 s adding
`--disable-multithread-weight-load` (local NVMe: 132 s). Without daemon
readahead that flag makes it slower (286 s). An fp32 checkpoint loaded
as bf16 is bound by the fault pattern of the CPU conversion (measured
~330 MiB/s at 4 ms per read) — warm the page cache first, on the same mount:

```bash
find /mnt/autumn/<model_dir> -name '*.safetensors' | xargs -P8 -n1 cat > /dev/null
```

Measured: 53 GiB warmed in 18 s (3.0 GiB/s), then loaded in 9 s. The pages count
against the memory cgroup of the process that reads them.

**`O_DIRECT` opens DO work** (measured 2026-09-01, VKE, kernel 5.15):

```bash
dd if=<file on mount> of=/dev/null bs=4096 count=1 iflag=direct   # OK
dd if=<file on mount> of=/dev/null bs=8M   count=1 iflag=direct   # OK
```

An `O_DIRECT` reader bypasses the page cache and gets no readahead: each read
is one round trip, so it needs large reads or concurrency to be fast.

### `--direct-read` — bypass the PS for large reads

Add `--direct-read` to the mount to make whole-extent reads (≥ 64 KiB) read
STRAIGHT from an extent node instead of proxying through the PS — a cross-host
throughput win for large-file / model serving (the PS NIC egress leaves the read
path). **Topology-dependent, default OFF**: the fuse host must be able to reach
EN *data* ports, which a hardened deploy often keeps on a PS-only subnet. It is
SAFE to enable even if some ENs are unreachable — every read falls back to the
PS proxy (one redirect RTT + fallback per extent), so correctness never depends
on it.

```bash
nohup ./target/release/autumn-fuse \
    --manager 127.0.0.1:9001 --mountpoint "$MP" --transport tcp \
    --direct-read \
    > /tmp/autumn-fuse.log 2>&1 &
sleep 1; mountpoint -q "$MP" && echo mounted   # log prints "direct-read enabled ..."

# Byte-identical vs proxy: write a >64 KiB file, read it back, diff.
head -c 5242880 /dev/urandom > /tmp/blob            # 5 MiB (multi-extent)
cp /tmp/blob "$MP"/blob
cmp /tmp/blob "$MP"/blob && echo "direct-read OK: byte-identical"
```

Verify the bypass actually engaged: with `--direct-read` a large read shows
`autumn_ps_read_bytes` on the PS staying flat (the value bytes don't traverse
the PS) while the EN's `MSG_READ_BYTES` traffic rises; without it the PS
read-bytes counter tracks the read. The same flag exists on every direct-read
frontend, all DEFAULT ON now (2026-07-09): fuse `--direct-read` (default true),
python `BatchClient(manager, ..., direct=True)`, `autumn.Fs.connect(...,
direct_read=True)`, kvcache `AutumnKVConnector` (`extra_config.direct_read`),
the `autumn-s3` gateway (`--direct-read`). Mixed-size batches route per
item — sub-64 KiB values still go through the PS; on a topology where ENs aren't
client-reachable each item falls back to the proxy and the client logs one WARN.

## Python `autumn.Fs` — shared inode-layout binding

A file replaced by an S3 PUT (or deleted) while another name still links it
keeps that name readable whatever point its publisher dies at, and a publisher
that wakes after another gateway recovered its session is fenced out. Check:
`cargo test -p autumn-manager --test system_publish`.

Namespace operations are shared by the mount, `autumn.Fs` and the S3 gateway
(`autumn_fs::dir`), so they hold across clients: two clients creating one name
get one file and one `EEXIST`; `mv src dst` onto a non-empty directory fails
`ENOTEMPTY` (a file onto a directory `EISDIR`, a directory onto a file
`ENOTDIR`) with both sides unchanged; a `mkdir` and another client's `mkdir` or
`rm` in the same directory do not overwrite each other's link count (under heavy
contention an update can still be given up, logged as `directory inode kept
changing`). Directory times have one-second resolution: a create, unlink or link
moves the parent's mtime and ctime only when they are not already in the current
wall-clock second, as on a filesystem with 1 s timestamps (clients with skewed
clocks write it every time). This holds for the mount and `autumn.Fs`; an S3 PUT
does not touch its directory's times. A file's link count moves only by
compare-and-set: an `ln` racing an `rm` of the same file from another client
either leaves the new name reading the file or fails `ENOENT`, and a writer's
flush or a `chmod` never puts back a link count another client changed. Check:
`cargo test -p autumn-manager --test system_fs_namespace --test system_fs_links`.
A file given as a parent (a raw inode number through `autumn.Fs`) is refused
`ENOTDIR` by mkdir, create, link and rename, and nothing is written under it.
The `autumnfs` CLI goes through the same code: `mkdir` and `put` onto an existing
name fail and change nothing, as do `mkdir` and `touch` under a file (ENOTDIR)
(`cargo test -p autumn-server --test autumnfs_cli`).
On a mount: `mkdir -p d/x s; mv -T s d` → `Directory not empty`, and `ls d/x`
still works.

`autumn.Fs` is a PyO3 binding over the **same** `autumn-fs` crate the
`autumn-fuse` mount runs on (inode/dirent/extent layout) — it's the programmatic
file surface (the `autumn-s3` gateway reads model weights through it). Headless
correctness (self-contained isolated memory-mode cluster — builds the wheel,
boots manager+EN+PS, drives the full `Fs` surface + a cross-instance byte-exact
check, tears down):

```bash
cargo build --workspace                    # debug binaries first
bash python/tests/run_fs_e2e.sh
#   → "PY M2 CROSS-INSTANCE byte-exact OK", "===== fs-e2e exit: 0 ====="

# M4 — lease fencing + cross-client coherence (two Fs clients):
bash python/tests/run_fs_lease_e2e.sh
#   → "PY M4 fencing OK", "PY M4 coherence OK", "===== fs-lease-e2e exit: 0 ====="
```

M4 write-fencing: `autumn.Fs` clients and a fuse mount both take the same
per-inode WRITE lease around writes (via `lease_tasks.rs`), so concurrent writers
to one inode conflict instead of corrupting each other; reads are close-to-open
coherent (fresh-read + `forget`-on-release). Behavior-preservation gate for the
`dispatch` Create/Unlink/init_root refactor + the M4 `lease_tasks` extraction
(the binding shares those core steps): the fuse e2e suite must stay green —
`cargo test -p autumn-manager --test system_fuse_read --test fuse_lease_1
--test fuse_lease_2 --test system_fuse_eof_clobber
--test system_fuse_flush_error_sticky --test system_fuse_release_best_effort
-- --test-threads=1`.

Build the server binaries FIRST — `cargo build -p autumn-server --bins`. Two of
these tests spawn `target/debug/autumn-ps` as a child process, and `crates/manager`
does not depend on `autumn-server`, so `cargo test -p autumn-manager` will not
build it: a clean checkout panics at `spawn autumn-ps`, and a dirty one silently
runs against whatever stale binary is lying there.

The last three drive `FsState` directly rather than a kernel mount, and each is
red without its fix: `system_fuse_eof_clobber` asserts a read AT EOF leaves an
unpublished size intact (the cache is legitimately LARGER than KV mid-write) —
its SECOND half, that a stale-SMALL cache is still corrected, is a preservation
guard rather than a reproduction — and the pre-fix run never reaches it at all,
aborting at the earlier assertion, so "green either way" is a claim about what
it guards, not a measurement;
`system_fuse_flush_error_sticky` holds three tests, one per way the sticky
write-back error used to be consumed by a caller that could not retire it — a
logging-only flush, the read-after-write barrier, and the write path's own gap
flush and truncate (the rule: only FUSE_FLUSH, FSYNC and the PyO3 flush retire
one, because those are Linux's retirement points); and
`system_fuse_release_best_effort` pins the case that is easiest to get wrong —
a NON-revoked RELEASE answers the kernel with EIO yet still may not consume the
record, because fuser drops a release error before it reaches `close()`.

## Cluster capacity — `autumn-op df`

Ceph-`ceph df`-style aggregate capacity. RAW and autumn `physical_used` are
summed from every extent node's `df` report. RAW total/free are statvfs capacity
truth; `physical_used` is the diagnostic sum of extent file lengths (replicas,
EC shards and open tails). `STORED(sealed)` is the manager's de-amplified Σ distinct
`sealed_length`. Because EC makes usable LOGICAL capacity a RANGE (cold EC
1.25–1.33× vs hot 3-replica), `df` shows raw-capacity `AMPLIFICATION`
(`raw used / logical size`) plus the writable estimate as a range
`[raw_free/3 .. raw_free/best_ec]`:

```bash
autumn-op --manager 127.0.0.1:9001 df          # human-readable
autumn-op --manager 127.0.0.1:9001 --json df   # for scripts

# Sanity-check against the EN filesystems:
#   RAW total/free  ≈  Σ `df -h` of each EN data dir
#   PHYS_USED       ≈  Σ `du -sb` of each EN extent dir
```

The same snapshot backs FUSE `statfs`: `df -h <mountpoint>` reflects real
backend capacity (conservatively, at the 3-replica factor) instead of a fixed
placeholder.

### Amplification in `df` = raw used / logical extent size

`amplification` = `(raw_total - raw_free) /
(logical_stored_sealed + logical_open_tail)`. The numerator is the capacity
actually consumed on the EN filesystems. The denominator is one de-amplified
copy of every live extent: distinct sealed extent size plus committed open
extent size. A 4+1-only layout is therefore about 1.25×, three replicas about
3×, and a mixture should sit between them on dedicated EN filesystems.

`physical_used` remains visible as `extent_files`: it sums EN-maintained file
lengths and is useful for diagnosis, but sparse/punched-file accounting may
diverge from statvfs capacity consumption, so it is not the amp numerator. A
high raw amp can also include non-Autumn bytes when EN data directories share a
filesystem with other workloads. The human `df` prints `sealed + open = size`.

### `amplification` far above the replication factor — the sealed-empty leak

If `df` shows amplification many times the replication factor and the number does
NOT come down after GC, suspect leaked sealed-empty extents. The shape is an
extent that is still a member of a stream, `sealed = true` with
`sealed_length = 0`, referenced by no ValuePointer, SST or checkpoint — so it is
counted in nothing and reclaimed by nothing. It cost one live cluster 10.4 TB
against 222 GB of logical data (47x) before the writer-side reclaim landed.

The manager runs a leader-only backstop for it (`sealed_empty_sweep_loop`, every
60 s). Watch it work:

```bash
# on the LEADER (a follower's sweep is a no-op by design)
grep 'sealed-empty sweep' <manager log>
#   ... reclaimed leaked non-tail members   stream_id=.. count=..
#   ... deferring                           (an op claimed the extent mid-sweep)
#   ... skipping reclaim, investigate       (a stream lists one extent twice)
```

Two things to expect rather than escalate. It reclaims at most **64 extents per
tick**, so a cluster carrying a large pre-fix backlog drains over hours, not
minutes — deliberately, because each reclaim is an etcd CAS plus a delete fanout
and the foreground path matters more than the backlog. And `deferring` is normal:
a recovery or EC conversion holding the extent gets it back on a later tick.

The `skipping reclaim, investigate` line is NOT routine. It means a stream's
membership names the same extent more than once, which is a refs-accounting bug;
the sweep refuses that extent rather than under-decrementing it into a different
kind of orphan. Capture the stream and extent ids from the log before anything
else touches them.

### WAL debt (dead large-value bytes) in `df`

`df` also prints `WAL debt: <bytes> dead (<pct>% of footprint, GC-reclaimable;
incl. open-tail)` and, in `--json`, `logical_wal_debt` + `wal_debt_ratio`. This is
the reclaimable garbage in `log_stream` — large values that were overwritten by a
newer version or deleted, still occupying replicas until GC punches them. It is
`Σ (gc_debt + open-tail dead)` across partitions, split at each partition's
replay floor:

- **`gc_debt_bytes`** = dead bytes in sealed log extents strictly before the
  replay floor: what GC may take now, and what the GC advisory fires on.
- **`open_tail_dead_bytes`** = dead bytes in the floor extent, the extents after
  it and the open tail. Recovery replays them, so GC refuses them; they move to
  `gc_debt` once a flush or a compaction moves the checkpoint past them. A
  partition whose garbage sits there shows `gc_debt = 0` and is still counted
  here. (Before, that garbage was `gc_debt`, the advisory dispatched GC, and GC
  answered "no eligible extents to reclaim" every cooldown.)

Check one partition: `autumn-op --json info --part P --detail` shows both
gauges; with a large `open_tail_dead_bytes` and `gc_debt_bytes = 0`, run
`autumn-op compact P` (a major compaction flushes, then moves the checkpoint
off a sealed extent's end) and the bytes become debt on the next GC tick.

Both are DERIVED each PS GC tick from the persisted SST discard maps (no bespoke
counter, no write-path cost) so they survive PS restart exactly like `gc_debt`.
Do NOT read `footprint − data` as debt — `size_bytes` is SST-only and excludes
live VP value bytes, so it would flag a healthy VP partition as ~all-debt. A high
`wal_debt_ratio` is the signal to run `compact` + `gc`/`forcegc` to reclaim.

### Shared extents: why the space has not come back

A CoW split leaves parent and child referencing the same extents. The extent
view names the holders rather than only counting them:

```
autumn-op info --full
  extent 14: size=4.0 GB, ..., refs=2 (streams=2), ..., shared by parts [13, 19]
             — freed only once ALL of them GC it; whatever is dead here is
               owed by each separately
```

Two things follow, and both are routine sources of "GC ran and nothing was
freed":

- The file is unlinked at `refs == 0`. One holder dropping its reference takes
  `refs` from 2 to 1 and returns **no space**; the other holder has to drop it
  too. For a log extent that means GC on both sides, and GC halves the ratio
  gate for `refs > 1` precisely because that first, apparently useless rewrite
  is what makes the extent independently owned. A shared row or meta extent is
  released by compaction's head truncate instead, not by GC.
- On a LOG extent, each holder counts its dead bytes in **its own** `gc_debt`,
  so adding per-partition debt across a split pair does not give physical
  bytes. That over-count is correct per partition — each really does owe the
  rewrite — and de-duplicating it would strand the extent forever. `df` is
  unaffected: it walks extents, not partitions, so its `WAL debt` counts each
  extent once.

If `refs` is larger than the number of partitions listed, a stream referencing
the extent belongs to no live region — an orphaned stream, or the refcount gap
the unscoped view tags `(refs-leak)`. Read `refs=N (streams=M)` there first.

The same list is on the dashboard's partition drawer, per extent chip.

### Verifying the GC advisory and GC selection agree

The advisory fires on absolute dead bytes (`gc_debt_high`, default 1 GiB) and
selection must judge by the same number. To re-check this on a live cluster you
have to build an extent that **only the absolute arm can select** — otherwise
the run passes without the plumbing under test being involved at all.

Mind the halving. A knob-less (standing) dispatch fills `gc_stream_debt` from
`gc_debt_high` as well as the floor, so once the stream's dead bytes cross
1 GiB the per-extent ratio gate is **halved to 0.2**, not 0.4. An extent at,
say, ratio 0.30 is then taken by the RATIO arm and proves nothing. The target
is `dead ≥ 1 GiB` (clears the floor) with `dead/sealed_length < 0.2` (under the
halved gate) — which needs an extent above ~5.4 GiB, and is the shape of the
original incident (3.12 GiB dead in 16.00 GiB = 0.195):

```bash
# Default everything, including the 16 GiB extent seal size.
AUTUMN_DATA_ROOT=/data05/autumn-gcverify bash cluster.sh reset 3
AO=(autumn-op --manager 127.0.0.1:9001 --cluster-secret-file <DATA_ROOT>/cluster.secret)
AC=(autumn-client --manager 127.0.0.1:9001 --namespace bench)

# 14 x 1.2 GiB seals the first log extent at 16.0 GB.
for i in $(seq 1 14); do "${AC[@]}" put-stream key$i 1.2GiB-file; done
"${AC[@]}" put-stream key1 other-file   # overwrite 2 of them: old chunks deleted => 2.4 GiB dead
"${AC[@]}" put-stream key2 other-file
"${AC[@]}" put-stream key20 any-file    # push the WAL past a flush boundary
"${AO[@]}" compact <PART> --wait        # deleted chunk VPs become discards

"${AO[@]}" info --part <PART> --full    # discards: extent N = 2576980376
autumn-op df                            # WAL debt: 2.4 GB dead
# 2576980376 / 17179869184 = 0.15 — under the halved gate, over the 1 GiB floor.
```

Then run the two judgements, which differ ONLY in whether a floor is carried:

- **Control** — `autumn-op gc <PART> --ratio 0.4 --stream-debt 1073741824`.
  Any named knob makes it an override, so it carries **no floor**, while
  `--stream-debt` reproduces the halving a standing dispatch would apply. It
  must answer `no eligible extents to reclaim`: at ratio 0.15 neither the 0.4
  gate nor the halved 0.2 gate can take this extent. Without this control the
  next step proves nothing. `df` must still read `2.4 GB` afterwards — an
  operator's one-off override must never redefine the standing gauge.
- **Standing** — `autumn-op gc <PART>` (no knobs). The manager fills the floor
  from `policy.gc_debt_high` and marks it standing, so the ABSOLUTE arm — the
  only one left — takes it: `GC: punched extent N, moved <M> entries` in the PS
  log, the extent leaves `info --full`, and `df` drops to `0 B`. Reclaiming a
  16 GiB extent relocates ~14.6 GiB of live values; budget ~7 min.

GC refuses to punch an extent it could not read completely: the PS log shows
`run_gc extent N: short read at <off>: wanted W, got G; refusing to punch` (or
`scanned X of L bytes` / `trailing bytes ... refusing to punch`) and the extent
stays in `info --full`. It means a replica's `.dat` is shorter than the sealed
length (lost tail, wiped file, torn write). Nothing is lost by the refusal:
restore or rebuild the short replica from a healthy copy, and a later GC cycle
collects the extent. Manual check, on a test cluster: seal a log
extent, stop its nodes, truncate each replica's `extent-<id>.dat`, start the
nodes again on the same dirs, `autumn-op gc <PART>`; the extent must still be
listed. Automated: `cargo test -p autumn-manager --test system_gc_truncated_replica`.

Unattended, the same thing happens through `auto-policy start gc-only`: the
advisory needs the debt sustained over 5 buckets (~5 min at the
default 60 s bucket), logs `GC primary=<PART> ... reason='gc_debt_bytes>...
sustained 5m'`, and the armed controller dispatches it on a following tick.

### Verifying that an emptied partition reclaims its space unattended

A delete frees nothing until a major compaction drops the value it killed and
records the discard GC reads. On an idle partition the tombstones stay in the
memtable, so the policy advises that compaction from the partition's
`unsettled_deletes` once no new delete has arrived for the whole window
(`N deletes not yet compacted, none new in 5m`), and the PS flushes its
memtable before compacting. Check it end to end, deleting EVERYTHING — the
compaction then keeps no entry, and its discards must still reach GC:

```bash
AUTUMN_DATA_ROOT=/data05/<scratch> AUTUMN_EXTENT_BASE_PORT=21000 bash cluster.sh reset 3
AO=(autumn-op --manager 127.0.0.1:9001 --cluster-secret-file <DATA_ROOT>/cluster.secret)
AC=(autumn-client --manager 127.0.0.1:9001 --namespace bench)

head -c $((64<<20)) /dev/urandom > /tmp/v64
for i in $(seq 1 270); do "${AC[@]}" put big$i /tmp/v64; done   # seals a 16 GiB log extent
for i in $(seq 1 270); do "${AC[@]}" del big$i; done
"${AO[@]}" auto-policy start aggressive

"${AO[@]}" --json info --part <PART> --detail   # unsettled_deletes: 270, gc_debt_bytes: 0
"${AO[@]}" policy-candidates                    # after ~5 min: major <PART> "270 deletes not yet compacted ..."
```

Do NOT use `put-stream` for this: `autumn-client del` on a streamed key deletes
only its 45-byte head and leaves every chunk key live.

Expected, measured on a 3-EN cluster (270 × 64 MiB + 200 small keys, all
deleted): the SETTLE compaction ~4.5 min after the last delete
(`compact part N: major, input=17 tables, output=1 tables, kept=0,
discarded=940` — the one output is the entry-less SST carrying the discards);
`unsettled_deletes` 0, `gc_debt_bytes` 16 GiB, and `est_live` = sealed + open
tail − gc_debt − open_tail_dead = 0 against an LSM of 0; the GC advisory 5 min
later and `GC: punched extent N, moved 0 entries` ~4 min after that. No split
candidate at any point. Before the fix this partition kept `est_live` at
16.88 GiB with no candidate of any kind, indefinitely.

### Minor compaction: the partition compacts its own tables

Nothing outside the PS asks for a minor compaction. Every flush asks for one
when the exploring policy (HBase's) finds a window of 3-10 consecutive tables,
and the 5-7 s tick asks again; a row-stream head extent pinned by an old table
is rewritten once its live tables are under 30% of it. Tuning (all `autumn-ps`
flags, sizes in bytes; defaults shown): `--compact-min-files 3`,
`--compact-max-files 10`, `--compact-ratio 1.2`, `--compact-min-size-bytes
1048576` (1 MiB), `--compact-large-size-bytes 134217728` (128 MiB; tables this
large never enter a minor window), `--compact-blocking-files 16`,
`--compact-max-sst-bytes 1073741824` (1 GiB output SSTs, at least 2 × the
flush size; each running compaction holds one in memory). An SST is built and
appended in `--sst-segment-bytes` buffers (default 4 MiB, 1–128 MiB;
cluster.sh: `AUTUMN_PS_SST_SEGMENT_BYTES`), never joined into one. A major leaves about one table
per 1 GiB of data. A partition of small SSTs (large
values in the log stream) stays at a handful of tables; one of flush-sized
tables (4K inline values, 256 MiB SSTs) keeps one table per flush until a
major — merging those into full tables cost 4K writes ~15%, so it is not done.

Check it on a scratch cluster with 4 MiB memtables, so 4 KiB inline writes
flush every ~1000 keys (each table ~4 MiB, far below `large_size`):

```bash
AUTUMN_DATA_ROOT=/data05/<scratch> AUTUMN_EXTENT_BASE_PORT=21000 \
  AUTUMN_PS_FLUSH_MEM_BYTES=$((4<<20)) bash cluster.sh reset 3
AO=(autumn-op --manager 127.0.0.1:9001 --cluster-secret-file <DATA_ROOT>/cluster.secret)
AC=(autumn-client --manager 127.0.0.1:9001)

"${AC[@]}" perf-check --threads 16 --duration 30 --size 4096 --partitions 8 --pipeline-depth 8
grep 'minor compaction:' /tmp/autumn-rs-logs/ps.log | tail -3
"${AO[@]}" info --part <PART> | grep -c 'sst\['     # tables listed now
```

Large values (above 4 KiB) go to the log stream and add only ~60 B to the
memtable, so `put` of big files does not make flushes.

Expected: `minor compaction: N → M tables` lines with `trigger="flush"` (or
`tick`) while the writes run, every one with M < N (an `N → N` line is a
selection bug), merging by tiers: windows of ~12, ~24, ~44, ~92 MB, each table
growing until it reaches `large_size` (measured: 150 minors in a 30 s run, a
partition with 230 MB of SSTs listed 2-3 tables). A window that keeps
re-merging one growing table with every new flush means `min_size` is set far
above the flush SSTs. `blocking` in a line means a window was forced at 16
tables.

After upgrading from a build that ordered tables by seq: a partition
merged under it and minor-compacted since may hold a checkpoint whose table
order lets an older copy shadow a newer one (a tied seq after the merge). The
new build keeps the stored order, so run `autumn-op compact <PART>` (a major
rewrites every table) on partitions merged before the upgrade.

The reclaim (`trigger="reclaim"`) shows up only once a row extent is
sealed with an old table in it, so on a 16 GiB extent it is rare; the
automated check is `cargo test -p autumn-manager --test
system_minor_compaction_triggers`.

### Per-partition size in `autumn-op info`

The cluster overview's per-partition size = the manager's authoritative
Σ `sealed_length` **plus** the PS-reported open-tail committed bytes (log + row +
meta open tails). Without the open-tail term a major-compacted or log-heavy
partition — whose data lives entirely in OPEN extents (manager `sealed_length` =
0) — renders `0 B` despite holding GBs. The open-tail bytes come from the PS's
5 s load report (refreshed by a throttled 30 s probe), so the overview is a
periodic rollup that can lag a live compaction by a few seconds:

```bash
autumn-op --manager 127.0.0.1:9001 info            # overview: size incl. open tails
autumn-op --manager 127.0.0.1:9001 info --part 17  # EXACT size (probes the EN live)
```

For an idle partition the two match to the byte; for one actively
GC/compacting they differ transiently — `info --part` is authoritative.
If an idle partition's overview size stays above the `info --part` sum and the
gap matches open tails listed as `0 B`, `info --part` could not probe those
tails: it asks the extent's first replica on the shard that owns it, and keeps
`0 B` when that replica is down or does not hold the extent. Check the node with
`autumn-op --json list-nodes` (state, `shard_ports`).

## Tuning `--max-extent-size-bytes` — reclamation granularity vs metadata

`autumn-ps --max-extent-size-bytes` (default **16 GiB**, clamp [1 GiB, 64 GiB])
is the tail-extent seal threshold. It is set per-PS and applies to **all three
streams** (log / row / meta) of every partition on that PS. It trades
**space-reclamation granularity** against **manager/etcd metadata pressure** —
bigger extents = fewer extents = less metadata, but coarser and more-delayed
space return to the EN disks.

Why the two streams react differently:

- **`log_stream`** (large values / VP records) reclaims via GC **`punch_holes`**,
  which is **per-extent** — GC relocates the still-live VPs off an extent, then
  frees *that specific* extent (not just the oldest). Coarser extents mean GC
  relocates more bytes per reclaim, but it can still target any sealed extent.
- **`row_stream`** (SSTables) reclaims **only** via `truncate`, a **prefix**
  operation: it frees the *oldest* extents, and only once **every** SST inside
  them has been compacted away (live data merged into newer SSTs). You cannot
  free a middle extent, and you cannot truncate the current tail. So a partition
  whose whole row_stream still fits in one 16 GiB extent needs a roll before
  truncate can reclaim SST space. **Major compaction now rolls first**, even
  for one SST; its output goes into a fresh extent. Minor compactions still
  rely on natural rolls, so dead SST bytes can accumulate up to ~one extent.
  A full 16 GiB extent holds ~128 × 128 MB
  SSTs; clearing it takes ~26 minor-compaction rounds (`COMPACT_N=5`/round) plus
  the matching write amplification.

Pick by workload:

| Workload | row_stream footprint | Recommendation |
|---|---|---|
| **Large-value** (fuse / model files / kvcache, most values > 4 KiB) | tiny — SSTs hold only VP pointers + small inline; data lives in log_stream (per-extent GC) | **keep 16 GiB** (or larger). row_stream space-amp is a non-issue; fewer extents wins. |
| **Small-value, high churn** (all values inline < 4 KiB, heavy overwrite/delete) | row_stream IS the data; dead SST bytes pile up | **lower to 1–4 GiB.** row_stream truncates far sooner; log_stream GC also gets finer-grained. Cost: more extents → more manager/etcd metadata + more append RPCs. |
| **Mixed / unsure** | — | leave the 16 GiB default; only lower if `autumn-op df` amplification or a partition's `info --part` shows row_stream disk held well above its live size for a sustained period. |

How to see whether it's biting you: `autumn-op df` reports raw-capacity/logical-size
amplification; a single partition's held-vs-live gap shows in `autumn-op info
--part <ID>` (live size probes the EN). If a small-value partition's on-disk
row_stream sits far above its live SST bytes and stays there across several
compaction cycles, it is holding an un-truncatable extent's worth of dead SSTs —
lower `--max-extent-size-bytes` (whole-cluster restart to apply; it is a
per-process flag, not runtime-tunable). The `--admission-compact-rate-bytes-per-sec`
knob governs how fast compaction *does* that reclamation work, independently of
the granularity the extent size sets.

Note: this is a per-PS **restart** flag (no online change); changing it does not
rewrite existing extents — only new tail rolls use the new size, so the effect
phases in as old extents are compacted/truncated away.

## Prometheus /metrics

Every PS and EN serves a Prometheus text endpoint at `/metrics` by default, on
its own thread and compio runtime pinned to no core — zero interaction with
the data-plane runtimes. The manager serves none: its state (leadership,
fleet, in-flight ops) is what `autumn-op status` / `ops` report.

| Process | Port | Series |
|---|---|---|
| EN | `--port` + 500 (cluster.sh: `960<i>`) | append batches, bytes and time |
| PS | `--port` + 500 (cluster.sh: 9801) | PS-wide only, no partition label (table below) |

`--metrics-port <P>` moves a process's endpoint (0 turns it off) and
`--metrics-listen <H>` pins its bind host (it is unauthenticated; the target
list then carries that host, so a loopback-pinned endpoint is reachable only
from the same machine). A taken port is logged as an ERROR and the server
keeps serving. Per-partition detail is not exported: it goes to the manager
on the load report, for scheduling (the dashboard's "hottest partitions").
Aggregate across servers in PromQL, e.g. `sum(rate(autumn_ps_requests_total[1m]))`
or `max(autumn_ps_partition_sst_tables_max)`.

**Discovery.** `autumn-op info --file-sd` prints every live PS and EN endpoint
as Prometheus `file_sd` JSON, labelled `autumn_role` (`ps` | `en`) and `ps_id` /
`node_id` (a server that reported no endpoint, or an evicted PS, is left out).
Pick one:
- deploy tooling writes a static `scrape_configs` from the hosts it started;
- cron refreshes a `file_sd` file, which Prometheus re-reads on change:

  ```bash
  # every minute; write then rename so Prometheus never reads half a file.
  # It follows the leader, so list every manager.
  autumn-op --manager mgr1:9001,mgr2:9001,mgr3:9001 --cluster-secret-file $SECRET \
    info --file-sd > /etc/prometheus/autumn.json.tmp \
    && mv /etc/prometheus/autumn.json.tmp /etc/prometheus/autumn.json
  ```
  ```yaml
  scrape_configs:
    - job_name: autumn
      file_sd_configs:
        - files: [/etc/prometheus/autumn.json]
  ```
- on Kubernetes, `kubernetes_sd_configs` with pod annotations.

```bash
AUTUMN_TRANSPORT=tcp ./cluster.sh start 3
autumn-op --manager 127.0.0.1:9001 --cluster-secret-file $SECRET info --file-sd
# [{"targets":["127.0.0.1:9801"],"labels":{"autumn_role":"ps","ps_id":"1"}}, {"targets":["127.0.0.1:9601"],"labels":{"autumn_role":"en","node_id":...}}, ...]
curl -s http://127.0.0.1:9801/metrics   # PS
curl -s http://127.0.0.1:9601/metrics   # EN 1
```

`/metrics` carries performance series only. State lives in `autumn-op`: disk
online / faulted / full per node (`autumn-op list-nodes`, the `FULL` column, and
`--json overview` `nodes[].disks[]`), extent counts (`overview`), fleet and
leadership (`status`), in-flight ops (`ops`). Refusals of a wrong cluster secret
show in the refusing server's log ("PEER_AUTH refused a connection holding a
different cluster secret").

PS latency histograms (LAT-1): `autumn_ps_write_duration_seconds` (group-commit end-to-end across ALL write ops — Put/Delete —
observed per batched op from the already-measured WriteLoopMetrics
— zero added hot-path timing) and `autumn_ps_get_duration_seconds` (inline
serve incl. VP resolve) — Prometheus histogram exposition, PS-wide,
buckets 0.5ms..250ms. A/B perf-checked (4K, p8, d8): no write regression.

PS series (each summed over the PS's partitions; counters stay monotonic when a
partition moves away or reopens, and reset only when the PS restarts):

| Metric | Means |
|---|---|
| `autumn_ps_partitions` | partitions open on this PS |
| `autumn_ps_block_cache_{hits,misses}_total`, `_bytes`, `_capacity_bytes` | the PS's SST block cache (`--sst-block-cache-bytes`) |
| `autumn_ps_get_sst_checks_total{result}` | per point lookup, SSTs `skipped` by key range or bloom, read and found (`hit`), read and absent (`false_positive` of the bloom) |
| `autumn_ps_vp_reads_total`, `_vp_read_bytes_total` | large values the PS read from the log stream (no PS cache on this path) |
| `autumn_ps_sst_tables`, `_memtable_bytes`, `_imm_memtables`, `_size_bytes` | the LSMs' shape now |
| `autumn_ps_partition_sst_tables_max` | the most SST tables any one partition has |
| `autumn_ps_flushes_total`, `_flush_bytes_total` | committed flushes |
| `autumn_ps_minor_compactions_total{trigger}` | minors by cause: `flush`, `tick`, `blocking`, `reclaim` |
| `autumn_ps_major_compactions_total`, `_compaction_write_bytes_total{kind}` | majors, and what compactions wrote |
| `autumn_ps_requests_total`, `_write_bytes_total`, `_read_bytes_total` | requests, value bytes written / served |

Buffer pool, on the PS (`autumn_ps_regpool_*`) and the EN (`autumn_en_regpool_*`)
alike, process-wide (see "Registered buffer pool budget"):

| Metric | Means |
|---|---|
| `_regpool_pool_bytes`, `_registered_bytes`, `_in_use_bytes` | chunk bytes, of them registered (UCX), handed out now |
| `_regpool_overflow_bytes`, `_over_cap_total`, `_overflow_chunks_total` | overflow chunks alive, acquires served past the cap, overflow chunks made |
| `_regpool_out_of_pool_total`, `_register_failed_total` | heap buffers (over 256 MiB, or an overflow mapping failed); chunks UCX would not register |
| `_regpool_use_{quota,in_use,high_water}_bytes{use}` | per use (`receive`, `reply`, `staging`): its share, held now, most held |
| `_regpool_use_{waits,timeouts,refused,too_large}_total{use}`, `_use_wait_seconds_total{use}` | waits and their time, waits run out, refusals with no wait, requests over a thread's share |
| `_ucx_am_sent_bytes_total{memh}` | bytes sent as UCX active messages with the buffer's registration (`yes`) or for UCX to look up itself (`no`: only values over 256 MiB) |
| `_ucx_am_staged_bytes_total` | bytes copied into a pool buffer first because they came without a registration (heap values, background data) |

Manual verify: write a few keys with `autumn-client put`, then confirm
`autumn_ps_requests_total` increments and
`autumn_en_append_bytes_total` grows. Read them back twice: the second read's
blocks come from the cache (`hits` grows), and each GET adds to
`get_sst_checks_total` once its key is flushed out of the memtable. A `flush`
bumps `flushes_total` and `sst_tables`; with small flushes (`--flush-mem-bytes`
4 MiB) and a perf-check write run, `minor_compactions_total{trigger="flush"}`
climbs and `sst_tables` stays low. Every PS and EN lists the `*_regpool_*`
series; on an idle cluster the refusal, wait and overflow counters are 0.
`autumn-op split` a partition: the
counters keep climbing through it (no reset). Notes: PS and EN render at
scrape time from their atomics (EN per-shard gauges refresh every 2 s); PS
counters reset on PS restart
(normal Prometheus counter semantics — use `rate()`).

## Disk-full (ENOSPC) behavior

A capacity error (ENOSPC/EDQUOT) on any EN write marks the disk **Full**,
distinct from **Faulted** (any other I/O error, permanent until restart):
a Full disk keeps serving reads and existing extents but hosts no NEW
extents, and **self-heals** back to Online within ~2 s of free space
returning above 5% of the disk (GC or operator cleanup — no process
restart needed). Watch the `FULL` column of `autumn-op list-nodes` (or
`full` on the disk rows of `autumn-op --json overview`; the dashboard's Nodes
tab marks the disk full). The manager additionally soft-avoids allocating onto
nodes whose best disk has < `--min-alloc-free-bytes` free (default
256 MiB; 0 disables; cluster.sh env `AUTUMN_MGR_MIN_ALLOC_FREE_BYTES`).

E2E test (root, loop mounts): `./scripts/enospc_chaos.sh` — EN1 on a
512 MB loopback ext4 fills under live 1 MB puts; asserts Full-not-Faulted
classification, write failover to the other ENs, 2 s self-heal after
space frees, and byte-exact readback of every ACKed key. This harness
caught a real silent-corruption bug on its first pass: the batched append
used a raw `pwritev` and treated a SHORT write (the POSIX behavior when
some bytes fit) as success — a partial value was ACKed and read back
zero-padded. Fixed with the write-all form; the invariant is documented
in `crates/stream/CLAUDE.md` note 25a.

## Direct I/O on the extent nodes

On by default on Linux (off elsewhere): append bursts of 1 MiB and more write
their 4 KiB-aligned part with O_DIRECT; the sub-block tail and every smaller
burst stay buffered, and every burst is still fsynced before it is ACKed.
`--no-direct-io` turns it off (page cache for every write);
`AUTUMN_EXTENT_DIRECT_IO=0` makes `cluster.sh`, the k8s entrypoint and
`autumn-deploy` pass it, and `perf_check.sh --shm` sets it itself. The `.dat` size stays byte-exact (it is the extent's length after a
restart). Design: `crates/stream/CLAUDE.md`, "Direct I/O for large bursts".

What to expect (3-node RF3 cluster on one host, NVMe with power-loss
protection, ext4, 8 partitions, 2 shards per EN with their io_uring workers
confined to those cores, same-period A/B):

| | `--no-direct-io` | direct (default) |
|---|---|---|
| 8 MiB write, 16 clients x depth 8 | 1296-1317 MB/s | 2000-2026 MB/s (+54%) |
| 1 MiB write, 1 client, depth 1 | 308-323 MB/s, p50 2.8 ms | 429-432 MB/s, p50 2.0 ms |
| 8 MiB write, 1 client, depth 1 | 371-375 MB/s, p50 19.5 ms | 514-520 MB/s, p50 14 ms |
| 8 MiB write, 2 clients, depth 1 | 647-653 MB/s | 989-1017 MB/s |
| EN CPU at full load | 4.2 cores | 3.7 cores |
| reading 8 MiB values right after writing them | 8.1 GB/s | 6.3 GB/s (from disk, not page cache) |
| 4 KiB write | within noise | within noise (bursts rarely reach 1 MiB) |

The cost is read-after-write: turn it off where freshly written data is read
back at once. Numbers taken before the io_uring workers were confined to
`--cpuset` showed buffered ahead by 4-7.5% — it was then using ~4 cores outside
the ENs' cpusets; compare only runs from the same build.

Requirements and failure modes:
- Linux only; elsewhere it is always off.
- At startup the EN opens each data dir's `disk_id` read-only with O_DIRECT
  (nothing is written). A filesystem without O_DIRECT (tmpfs before Linux 6.6,
  such as `/dev/shm` or a k8s `emptyDir` with `medium: Memory`) stops the EN with
  `direct I/O: cannot open <dir>/disk_id with O_DIRECT (... needs
  --no-direct-io)`; start it with `--no-direct-io` there. The cost is one
  open() per disk per shard. With `--no-direct-io` nothing is checked.
  The EN logs `autumn-extent-node ready` only after every shard passed this
  check and bound its listeners; `cluster.sh` waits for that line and stops at
  once with `nodeN (pid ...) exited during startup` when the EN died instead.
  The same applies to scratch data: `cluster.sh` defaults to `/tmp/autumn-rs`
  and several integration tests put EN data under the system temp dir, so on a
  host whose `/tmp` is tmpfs on a kernel before 6.6 point `AUTUMN_DATA_ROOT` /
  `TMPDIR` at a real filesystem, or set `AUTUMN_EXTENT_DIRECT_IO=0`.
- A filesystem that accepts O_DIRECT and quietly buffers it (ext4
  `data=journal`, tmpfs from Linux 6.6) passes the check; confirm the direct path
  is really taken (below).
- A direct write failing at runtime is logged as `O_DIRECT append burst at
  <offset>: ...`.

Verify on a running EN:

```bash
# 1. it is on: one line per shard (absent under --no-direct-io)
grep 'direct-io on' /tmp/autumn-rs-logs/node1.log
# 2. writes really go direct: count kernel direct-IO calls per process while
#    writing large values (>= 1 MiB); the EN pids should show thousands
bpftrace -e 'kprobe:__iomap_dio_rw { @[pid, comm] = count(); } interval:s:10 { exit(); }' &
autumn-client --manager 127.0.0.1:9001 perf-check --threads 16 --duration 8 --size 8388608
# 3. bytes survive a restart: put odd sizes, restart the whole cluster, compare
for sz in 5000 1048577 3145851 17829887; do head -c $sz /dev/urandom > /tmp/v$sz
  autumn-client --manager 127.0.0.1:9001 --namespace bench put dio-$sz /tmp/v$sz; done
./cluster.sh restart 3 --3disk   # wait for 'partition server serving'
for sz in 5000 1048577 3145851 17829887; do
  autumn-client --manager 127.0.0.1:9001 --namespace bench get dio-$sz | cmp - /tmp/v$sz && echo ok $sz; done
```

When comparing buffered and direct yourself, alternate the two in the same
period (A/B/A/B): this host's NVMe throughput drifts by tens of percent over
hours (IOMMU IOVA allocator state), which swamps the difference.

## SST block rot (row stream)

A row-stream copy that rotted stays in service until a scrub finds it, so the
PS reads around it: an SST block that fails its CRC is fetched again from the
other copies and the first that decodes is served. Watch the PS log for
`SST block failed to decode; served from another copy` (`served_from` names
the copy, `rotted_replicas` the replicas that failed). For a sealed replicated
extent those replicas are reported and isolated (an open tail is read around
but not reported: isolating it needs a seal first, and the scrub covers it
once it seals) (manager log: `isolated
corrupt replica(s) a PS reported`); then `autumn-op info --extent <ID>` shows
the slot dark and recovery rebuilds it. For an EC extent `served_from` is
`EcWithoutShard { shard: i }`: shard `i` is the culprit; run
`autumn-op scrub <ID>` to have its node confirm and isolate it.

Manual check (3 ENs, row stream RF 2): write and flush a few thousand small
keys, roll the row tail so its extent seals, flip one byte in every 4 KiB of
the first half of one replica's `extent-<ID>.dat` (or of `extent-<ID>.shard0`
after `force-ec-convert`), restart the PS so its block cache is cold, and read
every key back: all must return their values. The automated version is
`cargo test -p autumn-manager --test system_sst_block_rot`.

## WAL replay self-heal (log_stream bit-rot / truncated replica)

Partition open replays `log_stream`. If a sealed extent's serving replica
returns a **corrupt** record (per-record CRC / length mismatch) or a
**truncated** committed window (short read on a record boundary), recovery no
longer fails-and-wedges: it re-reads the SAME committed window from the other
*eligible* replicas, continues replay from the first that decodes clean, and
reports the bad replica(s) to the manager — which clears their `avali` bit and
bumps the extent eversion (so every PS refetches and stops serving from them)
**before** the partition serves. Fully automatic, no operator action. Watch the
PS log for `WAL self-heal: ... recovered the window from a clean replica` and
`isolated corrupt log_stream replica(s) via the manager`. An **OPEN-tail**
content corruption is sealed-and-rolled first (`WAL self-heal A4: sealed-and-rolled
the corrupt OPEN log_stream tail`) — frozen at the committed length via the lenient-seal
probe, then isolated in the same pass like a sealed extent. Still fails the open
loud (data lives on a healthy replica → recover / retry) for: an all-replicas-bad
extent, or an open tail that is **truncated** below the committed prefix (sealing
there could drop acked data — a separate lenient-seal edge; **the seal must be
lenient**: the seal path accepts a lenient/committed-length freeze rather than
demanding byte-perfect tails). EC extents route shard repair
through recovery, not this path. End-to-end fault injection lives in
`scripts/selfheal_chaos.sh` (3-EN cluster, flip one byte of slot[0]'s extent
`.dat`, restart → assert self-heal + byte-exact reads incl. the corrupted-value
key + slot isolated; plus an all-replicas-corrupt fail-loud negative). That
harness caught a real read-path bug on its first run: the avali isolation filter
was wired only into the copy read path, so the two VP-value fast paths
(`read_value_into_pooled` bulk proxy + `extent_read_descriptor` client-direct)
still served the bit-rotted-but-isolated replica — now both filter
`eligible_replica_slots`. Design: `docs/wal_selfheal_design.md`.

### Compaction never strands un-flushed writes past the replay-start

Each SSTable records a `vp_head` = the `log_stream` position recovery replays
FROM. A major compaction rewrites every SSTable, so whatever `vp_head` it stamps
becomes the whole partition's replay-start after the next restart. It stamps the
**MAX over the input SSTs' vp_heads** (the newest input's content boundary), NOT
the live write cursor — the cursor sits PAST writes that are acked + durable in
`log_stream` but still only in the active memtable (un-flushed), and stamping it
would drop those writes out of the replay window (silent loss on a crash between
the compaction and the next flush). MAX keeps the replay-start behind the
un-flushed tail while still advancing it past the fully-merged log region so GC
can reclaim there. No operator action; automatic. Regression:
`crates/manager/tests/system_compact_unflushed_vp_head.rs` (writes A→flush,
B→flush, C→NO flush, major-compact, crash, reopen → all of A/B/C must read back).
The MAX above is correct only because each SST's `vp_head` is now its true
content boundary: a flush stamps the position captured when the memtable was
FROZEN (`rotate_active`), not the live cursor at flush-claim (which foreground
writes could push ahead of that SST's content — a flush-race that stranded the
un-flushed tail before crash). Regression:
`crates/manager/tests/system_flush_race_vp_head.rs`. And on RESTART, recovery
seeds the write cursor `p.vp` to the committed log TAIL (not the replay start),
so the recovered active memtable also rotates with a forward boundary and the GC
floor advances for an idle-restarted partition — closing the "compact-then-GC
still won't reclaim" case. Guard:
`crates/manager/tests/system_recovery_vp_seed.rs`. The vp_head is now a true
content boundary on every path (flush, compaction, and recovery).

### A compaction never lowers the partition's seq

Recovery skips WAL records at or below the loaded SSTs' max seq and starts the
partition's seq counter there. A major compaction that drops a key's puts and
its newest tombstone used to write an SST whose seq was only the newest entry it
kept, below the dropped records that are still in `log_stream`. The next open
then gave a new put of that key a seq below the old tombstone, and an open that
had to replay the whole log (its checkpoint cursor gone) let the tombstone hide
the acknowledged put. A compaction's last output SST now carries the newest seq
of its inputs. No operator action. Verify:

```bash
cargo test -p autumn-manager --test system_compact_seq_below_dropped
```

Three cases: a major compaction that keeps one key, one that keeps nothing, and
a control without compaction; all must pass.

Data written before this fix: a partition that ran a major compaction under an
older build may still hold SSTs whose seq is below records in its log, and a
later compaction cannot raise them (it takes the inputs' already-lowered seq).
The loss needs an open whose replay walks flushed log, which happens only when
the checkpoint cursor names an extent no longer in the log stream (replay then
starts at an SST's older stamp, or with none resolving walks the whole log and
logs `replaying the WHOLE log stream`). The exposure ends once GC has punched
the log extents that hold the old tombstones.

### How much WAL a partition open replays — and why a clean restart replays ~none

Recovery replays the log from the **checkpoint's cursor** (the `vp` in the
meta-stream `TableLocations` record), not from the oldest SST's stamp. A clean
stop (SIGTERM: the drain flushes every memtable and writes a checkpoint naming
the log tail) therefore replays close to nothing; a crash replays only what was
written since the last flush. Compaction never moves the checkpoint's cursor
back. Every open logs what it did:

```bash
grep "log replay done" <ps.log>
# part_id=21 start_extent=30 start_offset=63966480 extents=1 bytes=0 records_kept=0 records_covered=0 elapsed_ms=0
```

`bytes` is WAL read; `records_kept` went into the memtable (not yet in any SST);
`records_covered` were already in SSTs and were read for nothing. A large
`records_covered` after a clean stop would mean the start was pulled back again.
The drain's own outcome is in the stopping PS's log, one line per partition:
`graceful shutdown: drained`, `graceful shutdown: flush failed ... replay on
restart: <error>`, or `graceful shutdown: drain timed out`. After
`graceful shutdown: complete` the stopping PS logs no `opening partition` /
`reloading partition` line: region sync stops for the drain, after waiting for
a pass already running. The one exception is a pass that outlived the drain's
deadline, announced by `an in-flight region sync did not finish; draining
without it`; it may still open a partition afterwards, and the WAL covers it.
Any other reopen there means the drain closed a partition and region sync
started it again.

Manual check (any cluster): put a few keys and stop/start the PS once (the drain
makes an early SST), put ~60 x 1 MiB, SIGTERM the PS, start it, then:
`grep "log replay done" <ps.log> | tail` must show `bytes` near 0 for the
partition holding the data. Regression tests:
`crates/manager/tests/system_restart_replay_cursor.rs`,
`crates/manager/tests/system_ps_shutdown_region_sync.rs`.

### Row-stream truncation never drops an SST the checkpoint lists

After a compaction the PS drops the row-stream extents ahead of the first extent
any live SST sits in (stream order), and logs it:

```bash
grep "row stream: dropped the extents" <ps.log>   # part_id, row_stream_id, before=<extent>
```

It used to derive the cut from the ORDER of its table list, which after a minor
compaction or a merge is not stream order, and could drop extents live SSTs were
in. The symptom is a partition that fails to open on a missing row extent while
its checkpoint still lists SSTs there. To check a partition by hand, compare the
checkpoint's SST extents with the row stream:

```bash
autumn-op --manager <MGR> info --part <P> --json   # extents[] with role "row"
# checkpoint SSTs: the last TableLocations record in the meta stream
# (tests: support::decode_last_table_locations). Every locs[].extent_id must
# be one of those row extents.
```

Data in SSTs whose extents were already deleted is gone; repairing such a
partition means rewriting its checkpoint without them (an audited, one-off
operation, not something the PS does). Regression tests:
`background::compaction_truncate_tests`,
`crates/manager/tests/system_row_truncate_live_refs.rs`.

A manual `autumn-op --manager <MGR> compact <P> --wait` flushes the memtable,
rolls the row tail on P-sst, and rewrites even a single SST. Once its checkpoint
is durable, unreferenced prefix extents can be dropped. A queued flush may pin
an older extent until it commits; no-op compactions also retry safe truncation.
A failed roll aborts the major before output is written. Repeated majors rewrite
the live SSTs each time, so use them for reclamation rather than polling them.

Regression (real manager/EN/PS, with reopen and a paused concurrent flush):

```bash
cargo test -p autumn-manager --test system_row_truncate_live_refs --test system_row_truncate_queued_flush -- --test-threads=1
```

### A failed checkpoint changes nothing (flush or compaction)

A flush or compaction appends the checkpoint that names its new tables BEFORE
the partition starts serving them. If the append fails, the PS logs the error
(`compaction:` for a compaction, `background flush commit error:` for a
flush) and keeps serving exactly what the durable checkpoint lists: a flush's
memtable stays queued and is flushed again, and the SST it had uploaded, or a
compaction's output, is left as unreferenced bytes in the row stream, which a
later compaction's row truncate drops. Nothing to do; a crash at any point recovers every acknowledged
write.

This order matters. When memory changed first, a failed flush checkpoint
followed by a failed compaction checkpoint let GC delete log that recovery
still needed, and a crash lost acknowledged small writes and brought deletes
back. Verify (failpoints in a SIGKILLed PS, GC between the failure and the
crash):

```bash
cargo build -p autumn-server --bins
cargo test -p autumn-manager --test system_compact_checkpoint_fail
```

To check a live partition after such a failure, compare the checkpoint's SST
extents with the row stream as in the previous section: every `locs[].extent_id`
must still be a row extent.

### A GC that skips an extent with an op in flight is CORRECT

Nothing preempts an op in flight on an extent — a recovery, an EC conversion —
and GC's punch is no exception. A GC run asks the manager first and skips such
an extent (PS log `GC: skipping extents with an op in flight`, the extent ids
listed) with a 30 s soft cooldown, so its live values are not relocated only to
have the punch refused; the op says `skipped [..]`, or ends FAILED "deferred: an
op is in flight on extent(s) [..]" when nothing else was left. It is collected
by a later run once the op ends.
Manual check: on a partition with a sealed log extent E, `forcegc` the
partition while an op is in flight on E (`autumn-op ops list --active` shows a
recovery or ec-convert entry naming E) — the PS log shows the skip, E stays in
the log stream and the log tail does not grow; once the op ends the next
`forcegc` reclaims E. Regression: `cargo test -p autumn-manager --test
gc_waits_for_extent_ops`.

### Reading GC replay-floor protection — a skipped `forcegc` is usually CORRECT

GC protects any NON-EMPTY `log_stream` extent that sits AT/BEFORE the recovery
replay floor (`MIN` over every live SST's `vp_head` position). If you `forcegc`
such an extent it is refused — this is the replay-floor safety guard, **not a bug**: recovery
replays the log from `floor_extent` forward, so punching it could drop un-flushed
writes. How to tell CORRECT-protection from a real problem:

- **The PS log** now names it: `GC: protected extent(s) ... part_id=P
  protected=[E] floor_extent=F floor_pos=N pinned_by_sst_vp_extent=S` — the
  extent recovery replays FROM is `F`, pinned by SST whose vp_head is `S`.
- **`autumn-op info --part P`** shows `replay_floor = extent F (pos N)`, the
  `vp_seed(tail)`, and each SST's `vp_head` (the one that `← pins floor` is the
  lagging SST). If `floor_extent == the extent you tried to forcegc`, that extent
  IS the replay start — protection is correct.
- **`autumn-op forcegc P E`** returns a synchronous advisory when `E` is inside
  the replay window (which extents, and why), instead of you having to grep the PS
  log.

To actually reclaim a protected extent, **advance the floor**: run a MAJOR
compaction (`autumn-op compact P`) so every live SST's `vp_head` moves past that
extent (a lagging CoW-shared SST from a split is the usual cause), then re-issue
`forcegc`. If the floor is that extent because the last flush ended exactly at
its end (it was the log tail then, and nothing was written since), the same
compaction moves the checkpoint cursor to the next extent once the log has
rolled — see the next section. If the extent is still the log tail, nothing
can be reclaimed from it: GC never takes the tail.

### A compaction moves the checkpoint off a sealed extent's end

With no write after the last flush, the checkpoint cursor sits at the end of
the extent that was the log tail then. After the log rolls (failover seal,
split, 16 GiB roll, `MSG_ROLL_TAILS`; a plain restart keeps the open tail)
that extent is sealed, holds nothing to replay,
and used to stay protected forever. Any compaction now republishes the cursor
as `(next extent, 0)`; GC then takes the old extent like any other. Manual
check on a partition with no writes:

```bash
$AO --json info --part P | jq '.extents[] | select(.role=="log") | {extent_id, size, open}'
$AO compact P                     # wait for it to finish (ops status / --wait)
$AO forcegc P <old extent>         # no "replay floor" advisory any more
$AO --json info --part P | jq '.extents[] | select(.role=="log") | .extent_id'   # gone
```

The PS log no longer names it in `GC: protected extent(s)`. Not moved: a
cursor in the open tail (nothing to free), or one short of a sealed extent's
end (records after it are replayed on reopen).

Regressions: `RUST_MIN_STACK=8388608 cargo test --release -p autumn-manager
--test system_compact_advance_anchor --test system_compact_advance_anchor_minor
--test system_compact_swept_anchor --test system_compact_recount_discard`.

### Recovery is BOUNDED and reopens in parallel

A partition's reopen time is bounded by the un-flushed **log** window, NOT the
dataset size — if a full-takeover reopen (all a dead PS's partitions land on one
survivor) is slow, that's a symptom to investigate, not "the dataset is just big".
Three properties enforce this (2026-07-13):

- **Bounded replay window (BUG1).** The `MAX_WAL_GAP` (1 GiB default) force-rotate now
  measures the un-flushed **log bytes** (value included), not the memtable
  footprint. Before the fix, a large-value (VP) workload kept only ~24-byte
  pointers in the memtable, so the gap never tripped and the log_stream replay
  window grew with the dataset. If reopen replay is still large, check
  `autumn-op info --part P` `vp_seed(tail)` vs `replay_floor` spread — a wide
  spread means flushes are lagging (slow P-sst / row_stream), not a recovery bug.
- **Parallel reopen (BUG3).** `sync_regions_once` opens up to 64 partitions
  concurrently (each recovers on its own OS thread/core). A 32-partition takeover
  recovers in ~single-partition time, not ×32. In the PS log you'll see all
  `opening partition P` lines close together, then `partition P opened` as each
  finishes — interleaved, not strictly sequential.
- **Tighter GC reclaim (BUG2).** GC may now raise its replay floor to the newest
  **durably-ACKed flush checkpoint** vp (not just the MIN over all live SSTs'
  vp_heads), so the fully-flushed prefix `[oldest-SST-vp, newest-flush-vp)` is
  reclaimable without waiting for a major compaction to advance every SST's
  vp_head. The recovery replay-start is UNCHANGED (safe by design — the recovery
  code was deliberately not touched); it self-tightens once GC punches the covered
  prefix. `autumn-op info --part P` still shows the conservative MIN `replay_floor`
  (display-only); the effective GC floor can be higher. **No operator action** —
  this just means less lingering log debt on write-heavy partitions between major
  compactions.

### GC auto-reclaims empty sealed log extents from split/merge churn

Frequent split/merge mints **empty sealed** `log_stream` tail extents
(`sealed_length == 0`). These are free to reclaim (`punch_holes`, no data
movement) but used to STARVE under Auto GC: candidates sort by reclaimable-bytes
DESC (empties last) and shared the 3-per-tick rewrite budget with big candidates.
Auto GC now gives empties a separate, larger per-tick budget (`MAX_GC_EMPTY_ONCE
= 32`), so they drain on their own within a GC tick or two — no operator action.
If you see empty sealed log extents lingering (`autumn-op info --part P` → a
`role:log, open:false, size:0` extent that is NOT the tail), a manual `autumn-op
forcegc P <extent>` still punches it immediately. NOTE: a split/merge-sealed empty
can occasionally be stale-cached-as-open on the PS and skipped until its cache
refreshes (a read / restart) — a `forcegc` that logs "not authoritatively sealed
yet" is that case; re-issue after a moment.

### GC reclaims log extents of small values (inline-value WAL)

A value of at most 4 KiB is stored inline in the SST; its WAL record in the
log_stream is dead once the memtable is flushed. The flush records those bytes
in the SST's discard map, so a sealed log extent full of small values reaches
the GC ratio by itself — no delete or compaction needed. Check on a partition:

```bash
$AO --json info --part P --full | jq '.discards'   # [{extent_id, bytes}, …]
$AO --json info --part P | jq '.extents[] | select(.role=="log") | {extent_id, size, open}'
```

A sealed log extent whose `discards` entry is close to its `size` is taken by
the next auto GC (or `autumn-op gc P`). Extents written by a PS older than this
change carry no such record (discard 0 however dead) until a major compaction
(`autumn-op compact P`) re-counts them: it sets each sealed log extent's discard
before the replay floor to its size minus the live values it keeps there, after
which the next auto GC takes it. `autumn-op forcegc P <e1> <e2> <e3>` (3 per
op, only extents before the replay floor) still works without it. Before
forcegc, confirm the partition really holds no live large values
there — `info --part P --detail` `size_bytes` small AND the range holds no
ValuePointer data. A range covering `fs/\x03…` (file data chunks) is mostly live,
and forcegc there only rewrites it (seen on the VKE cluster: three 17 GB extents,
~2000 live 8 MiB chunks relocated each, nothing freed).

Regression: `RUST_MIN_STACK=16777216 cargo test --release -p autumn-manager --test
system_gc_inline_wal` (needs `cargo build -p autumn-server --bins` for the killed
child PS; the debug part-* thread overflows its default 2 MiB stack).

## Write outcomes and the tail seal on open

Design: `docs/seal_length_design.md`. A partition seals its open tails (the
copies that answer decide the length) before it replays them; a write whose log
append was sent and given up is answered `OutcomeUnknown` and the partition
reopens, which settles it; once read, the answer never changes. A copy left out
of a seal is caught up by `re_avali` when its node is back (whole-extent copy).

Three extent nodes and three replicas, so one node down leaves no room for a
new tail:

```bash
export AUTUMN_DATA_ROOT=/data05/autumn-outcome AUTUMN_EXTENT_BASE_PORT=20000
./cluster.sh reset 3
AC=(target/release/autumn-client --manager 127.0.0.1:9001 --namespace mem/demo)
echo v0 | "${AC[@]}" put k0 /dev/stdin            # ok
./cluster.sh stop-node 3
echo v1 | "${AC[@]}" put k1 /dev/stdin            # Error: put: write outcome unknown (it may or may not have been applied): …
"${AC[@]}" get k1                                 # v1 — the record reached the two live copies; the reopen kept it
echo v2 | "${AC[@]}" put k2 /dev/stdin            # Error: put: connection error: … this write was not applied: … not enough nodes
kill -9 "$(cat $AUTUMN_DATA_ROOT/pids/ps.pid)"; ./cluster.sh start-ps
"${AC[@]}" get k1                                 # v1 again: the answer never changes
grep -c "seal_tail: sealed the open tail for replay" /tmp/autumn-rs-logs/manager.log   # > 0
./cluster.sh start-node 3; sleep 15
grep "re_avali:" /tmp/autumn-rs-logs/node3.log     # "copied the whole extent from a peer … local_len=48 sealed_length=96"
grep "caught a behind copy" /tmp/autumn-rs-logs/manager.log
./cluster.sh clean; rm -rf "$AUTUMN_DATA_ROOT"
```

`re_avali` logs which way it went: "the copy already holds the sealed length;
nothing copied" or "copied the whole extent from a peer … elapsed_ms". It never
copies only the missing tail: past the last acknowledged commit a copy left out
may hold bytes that differ from the sealed content, so its prefix is not
trusted. The length check behind "nothing copied" has the same blind spot
(BUG-SEAL-DIVERGENT-UNACKED-BYTES in `feature_list.md`, unreproduced).

## Read route-around for Suspected nodes

When the manager marks an EN **Suspected** (df heartbeats lapsed past the soft
timeout, ~10 s), the READ path proactively avoids it — not just allocation. For
**replicated** extents the client tries healthy replicas first and only falls
back to the suspected one if every healthy replica fails (suspected ≠ dead, and a
sealed extent's committed bytes are on every replica). For **EC** extents a
suspected data shard is reconstructed straight from parity (read K healthy shards
+ parity) instead of issuing a doomed shard read and waiting for it to time out.
This is a soft latency optimization layered on the existing failover — correctness
never depends on it, so a stale view only costs a little extra latency/parity
traffic, never data.

No new config or wire types: the client polls the existing
`autumn-op list-node-states` data (`MSG_LIST_NODE_STATES`) in the background,
TTL-gated at 2 s and never on the read's critical path. Because the refresh is
non-blocking, the avoidance is a **steady-state, self-healing** optimization, not
a per-read guarantee: the very first read after a node flips to `Suspected` (e.g.
on a previously-idle client) uses the current snapshot and only *kicks* the
refresh, so that one read can still pay a single timeout if it lands on the flaky
node — every read after the ~2 s refresh routes around it. This never regresses
the pre-existing reactive failover; it just removes the repeated per-read timeout
under sustained load. **Manual check:** on a 3-EN replicated cluster, `kill` one
EN; after the manager flips it to `Suspected` (`autumn-op info` /
`list-node-states`) and a couple seconds of read traffic, `get` of keys whose
extent has a replica on the dead node is served by a healthy replica instead of
stalling for the per-RPC timeout on every read.

## Stale owner-epoch fence self-heal (BUG-MGR-RETRY-CLASS)

A PS partition whose stream client holds a stale per-partition `owner_epoch`
(classic cause: a rebalance moved the partition and the old holder kept
serving, or any newer `acquire_owner_lock` on the same `partition/<id>` key)
is rejected by the manager with `CODE_PRECONDITION`
("owner_key=partition/N owner_epoch mismatch, expected X, got Y") on every
`alloc_new_extent`. Pre-fix symptoms: writes to ONE partition take ~15 s each
(20×500 ms futile manager retries + open overhead; `autumnfs put` = 45 s for
3 keys), reads stay fast, PS log shows the same `got Y` number forever.

Post-fix behavior (what to verify):
1. The first fenced manager call FAILS FAST (log: `"... got a deterministic
   manager error, failing fast"` + `"stream_alloc_extent fenced
   (LockedByOther): ..."`) — no 20-retry storm.
2. The PS poisons the partition (`"... fenced (LockedByOther) —
   poisoning partition for fresh-epoch reopen"` or `"LockedByOther detected,
   poisoning partition"`), its thread exits.
3. Within one region-sync tick (~2 s) the PS logs
   `"partition <id> thread exited (fence poison or crash) — dropping handle;
   region map decides reopen-with-fresh-epoch vs release"`, then either
   reopens it (still assigned here → fresh epoch, writes succeed) or leaves
   it closed (rebalanced away → the new owner serves it).

**Manual check** (any cluster): find a partition's owner key epoch, bump it
behind the PS's back, then write through it:
```bash
# bump the epoch for partition/17 behind the serving PS's back (manager CLI
# acquires the same owner lock the PS holds):
autumn-stream-cli --manager <mgr:9001> acquire-owner-lock partition/17   # if unavailable,
# any partition move (autumn-op rebalance 1) exercises the same path on the OLD PS.
# then:
time autumn-client --manager <mgr:9001> put <key-in-that-partition> v
# expect: first write may error/redirect once; within ~2-4 s writes to that
# partition succeed at normal latency (NOT 15 s each / NOT stuck forever).
# PS log shows the three-step sequence above, and the "got <epoch>" number
# CHANGES after the reopen (fresh epoch) instead of repeating.
```

## Migrating the extent nodes off the StatefulSet

One-time, for a cluster whose ENs still run as the `autumn-en` StatefulSet. The
destination is one Deployment per EN mounting the same PVC — same `node_uuid`,
same data, independently removable.

**The hazard this order avoids: two EN processes on one data directory.** The EN
takes no lock on its data dir, the PV is node-pinned, and ReadWriteOnce is a
NODE-level guarantee — two pods on the SAME node may mount one claim
simultaneously. So a Deployment must never be created while that ordinal's old
pod is still running: both would self-register the same `node_uuid`, overwrite
each other's advertised address, and write the same files.

```bash
# 1. Let go of the pods without stopping them. --cascade=orphan leaves every EN
#    running and untouched; only the controller goes away. PVCs are not touched
#    by this at all.
kubectl -n autumn delete sts autumn-en --cascade=orphan

# 2. One EN at a time. A merely-absent node does NOT trigger recovery -- the
#    recovery loop rebuilds fenced slots, corrupt slots and slots on a disk its
#    node reports faulted, not slots of a node that is briefly down -- so no
#    fence or maintenance window is needed for a restart this short.
for n in 0 5 6 7 8 9 10; do
  kubectl -n autumn delete pod "autumn-en-$n" --wait=true      # must be GONE,
  kubectl -n autumn wait --for=delete "pod/autumn-en-$n" --timeout=120s || true
  AUTUMN_EN_IMAGE=<registry>/autumn-rs:<tag> \
  AUTUMN_EN_CPU=4 AUTUMN_EN_NODESELECTOR=autumn-node=true \
    deploy/scripts/en-workload.sh apply "$n"                    # ...before this
  kubectl -n autumn rollout status "deploy/autumn-en-$n" --timeout=300s
done

# 3. The cluster must not have noticed. Same node_ids, same shard counts.
autumn-op info | awk '$1=="node"'
```

Do NOT substitute `kubectl apply` of a rendered set for step 2's loop: applying
all the Deployments at once creates every new pod while every old pod is still
running, which is exactly the overlap above.

If a step fails partway, the safe state is "old pod deleted, Deployment not yet
created" — nothing is lost, the EN is simply down, and re-running the apply for
that ordinal brings it back on its own PVC.

## Running a throwaway cluster inside a pod

`cluster.sh` is the only harness with raw kill/restart semantics, which is what
fault injection needs — but it wants Linux and a scratch disk, and the things
worth injecting faults into (EC rebuilds, recovery stalls) must not be injected
into a cluster holding real data. Running it inside a pod on the same cluster
gives both. Three things are not obvious:

- **The image has no `etcd` and no `nc`.** Run etcd as a second container in the
  pod (same network namespace, so `127.0.0.1:2379` is simply there), put a stub
  `etcd` early in PATH so `start_proc` has something to launch, and write a
  four-line `nc` that uses bash's `/dev/tcp` — `wait_port` only ever asks
  "is this port open".
- **`cluster.sh reset` cannot reset an etcd it does not own.** It wipes
  `$AUTUMN_DATA_ROOT`, so with etcd in a sidecar the node data goes and the
  metadata stays: the next cluster inherits extents whose files no longer
  exist, and recoveries fail with `reopen sealed extent N: No such file or
  directory` for reasons that have nothing to do with what you are testing.
  Delete and recreate the whole pod between runs.
- **`--max-extent-size-bytes` is how you get a sealed extent of a chosen size.**
  EC conversion refuses an open extent, and the default threshold is large
  enough that a test would have to write for a long time to roll one.

`AUTUMN_PS_MAX_EXTENT_SIZE_BYTES=4831838208 bash cluster.sh reset 6` then gives
a 4.5 GiB sealed extent, whose 4+1 shard is 1.125 GiB — big enough that a
rebuild takes long enough to watch, and small enough to fit twice on a 40 GiB
scratch disk.

## Node classes: what a node or disk may take part in

Every extent-work path (allocation, recovery, EC, scrub) judges a node by one
class (`crates/manager/src/node_class.rs`,
`docs/extent_op_arbitration_design.md` §1), built from its auto-state and its
override:

| `list-nodes` shows | class | new data | work it is in | its copies |
|---|---|---|---|---|
| Online, no override | health | yes | continues | stay |
| Suspend (not heard from since registration or since this manager became leader) | suspend | no | waits for its first df | stay |
| Suspected, or override maintenance | suspected | no | **cancelled** | Suspected: moved on a repair request (the `repair` policy files one after the grace); maintenance: moved only if you run `autumn-op repair` |
| override fenced, or removed | fence | no | **cancelled** | moved at once |

A disk takes its node's class, except that a disk its node reports Faulted is
fence (its copies move) and one it reports Full takes no new data. A df that
merely times out says nothing about the disks.

Maintenance means "this node is coming back": nothing is rebuilt off it by
policy, so the extents with a copy there run one copy short for as long as the
override stands. `autumn-op maintenance` on a fenced node is refused — clear the
fence first with `autumn-op unfence` (a node carries one override).

A registered node that never answers a df becomes Suspected after the soft
timeout (10 s), with no last heartbeat: that empty heartbeat is what tells
"never started" from "was running and stopped". Right after a manager failover
every node shows Suspend for one df round (~2 s); nothing is cancelled then.

**Manual check** (any running cluster): `autumn-op maintenance <id> --reason x
--by you` on a node holding replicas, then `autumn-op ops list --active` — any
recovery executing on it and any EC conversion targeting it closes FAILED within
a few seconds, naming the node; `autumn-op fence-node <id> --reason x --by you
--force` followed by `autumn-op maintenance <id> --reason x --by you` is refused
(PRECONDITION) and `list-nodes` still shows it fenced.

## Node decommission runbook (fence → drain → remove)

Retiring an EN is operator-driven (HDFS-decommission style). The manager never
auto-removes a node; you fence it, the system drains it, `remove` gates on the
drain being complete.

```bash
AO=(./target/release/autumn-op --manager 127.0.0.1:9001)

"${AO[@]}" fence-node 56 --reason "retiring" --by you   # 1. fence
"${AO[@]}" info                                          # 2. watch shard count → 0
"${AO[@]}" remove 56 --by you                            # 3. remove (server-side gated)
```

Step 1 refuses (PRECONDITION) unless recovery could actually move every slot off
the node: each extent on it needs a target that holds none of its other slots
and is not fenced, in maintenance or suspected; one such target must report
room for that extent's shard; and the nodes that may receive the slots must
together report 1.2x the bytes to move. A node that has not answered `df` yet
(right after registration, or unreachable) counts as having no room. The message
names the extent, the byte shortfall, or the nodes that have not reported
capacity yet (a freshly elected manager needs one df round, a few seconds,
before a non-force fence can pass). Open tails count as 0 bytes — they are
sealed at their real length only as the fence drains them — so a node holding
many large open tails needs more headroom than the check asks for. Re-sending
a fence for a node already fenced skips the check. `--force` skips the check — for when
the loss of redundancy is intended (e.g. the node is already dead and there is
no spare); the slots then stay degraded until a spare appears.

On Kubernetes, `deploy/scripts/en-decommission.sh <ordinal>` does exactly the
above and then deletes the workload — in that order, waiting at each gate:

```bash
deploy/scripts/en-decommission.sh --dry-run 7   # resolve + preflight only
deploy/scripts/en-decommission.sh 7             # fence → drain → remove → delete
```

It resolves the pod to its `node_id` through the pod IP, because that is the
only join key that exists: the EN advertises its own pod IP, and the manager
knows nothing about ordinals or workload names. It refuses when fewer nodes
than the replica count would remain (fenced nodes are hard-excluded from
placement, so the cluster would refuse new extent allocation — loudly, but only
once something tries to write). It keeps the PVC unless `--delete-pvc`.

Deleting the workload FIRST is the mistake this wraps: it looks like it worked,
and silently costs a replica of every shard the node still held.

What fencing triggers (all automatic):

- **No new data**: Fenced (and Maintenance / auto-Suspected) nodes are
  hard-excluded from every placement path — new extents, fallback walks,
  recovery targets, EC parity. Unlike soft excludes this is never backfilled;
  a cluster left with fewer eligible nodes than the replica count refuses
  allocation loudly rather than placing data on a draining node.
  (Availability note: a 3-EN RF-3 cluster with one *Suspected* node blocks new
  extent allocation until it heals — seconds — or is fenced.)
- **Sealed extents**: the recovery loop rebuilds every sealed extent's fenced
  slots onto healthy nodes. Includes sealed-EMPTY
  extents (0-byte membership swap).

**A single FAILED DISK needs no fence.** An extent node that hits a
non-capacity I/O error marks that disk `Faulted` and reports it `online: false`
on its next `df`; the recovery loop then rebuilds that disk's **sealed**
replicas elsewhere, without an operator and without touching the node's other
disks. Fencing the node for one bad disk moves every disk's data — on a
four-disk machine, four times the repair the failure called for.

Open tails on the faulted disk are NOT rolled: the drain sweep
(`drain_fenced_open_tails`) is fence-only. They roll on their own as the
partition writes, and are rebuilt once sealed.

Watch for `df: node reports this disk FAULTED` in the manager log, then the
usual recovery ops in `autumn-op ops list --active`.

A disk is judged on a failed write, including an extent file it could not
create: allocation picks a disk by load alone, so a disk that takes no new
files would otherwise be picked again and again while every allocation on the
node failed over elsewhere and nothing reported it. The chaos harness drives
this end to end (`diskfault`, below).

`Faulted` is sticky until the EN process restarts. A **replaced** disk is a new
device: re-run `autumn-op format` on it (new `disk_uuid` → new `disk_id`) and
restart the EN. Note the EN refuses to start if a configured `--data` dir
cannot be opened, so pull the dead dir out of the list first if the hardware is
gone.

Errors that indict the PROCESS rather than the device — running out of file
descriptors or memory — leave disk health untouched entirely (logged as
`write failed on process-level exhaustion`). The failing operation is still
rejected; the disk is simply not blamed for it. Nothing escalates on its own
from there, so **if that warning repeats, restart the EN** — a persistent
descriptor shortage is a leak or a misconfigured `RLIMIT_NOFILE`, not something
the node recovers from by itself.

A disk that is merely **`Full`** reports `online: true` and triggers NOTHING —
it stops taking new extents, keeps serving reads, and self-heals once free
space is back above 5%. That distinction is what keeps a cluster running low on
space from rebuilding itself.

**A node that is merely ABSENT still triggers nothing.** The rebuild reads a
fact only the node itself can report about one of its own disks, not the
node-wide `online` bit that a `df` timeout also clears — so the rolling-restart
procedure above stands unchanged.
- **Open tails**: recovery only rebuilds sealed extents, so the manager's drain
  sweep (every 2 s tick, 30 s per-partition cooldown) asks the owning PS to
  seal + roll any OPEN tail with a fenced replica (`MSG_ROLL_TAILS`). On a
  SERVING partition the roll quiesces the live stream writer first (SealCommit
  handshake) and seals at its exact all-replica-acked commit, then redirects
  the writer onto a fresh tail on healthy nodes — so a busy partition drains
  without losing acked writes (a bare probe-seal behind a live writer was the
  cause of the split-child `stale_vp_offset_past_sealed_length` wedge / silent
  stale-read family; regression `system_roll_tails_live_writer`). With no live
  writer it seals by lenient probe (a dead fenced replica doesn't block). The
  next recovery tick rebuilds the now-sealed extent; an idle partition
  therefore drains with no client writes. The PS defers the roll while the
  partition is frozen for a split/merge (retried after the freeze).

Watching progress:

```bash
"${AO[@]}" info                       # per-node shard counts → 0 = drained
"${AO[@]}" extent-health --node 56 --all   # what's left + sealed state per extent
"${AO[@]}" recovery-stats             # in-flight rebuilds + backoff reasons
```

`remove <id>` is safe to run early — it refuses with the blocking extent ids
until the node is fully drained, and prints `remove: ok` only when the manager
has verified no extent / EC-marker references remain. After remove, the node_id
is tombstoned (same address cannot re-register); stop the EN process.

**Drain-never-completes checklist (root cause):** the
drain's last mile is the manager LEARNING that a rebuild finished — the EN
reports completed recoveries only in its `df` response, and the manager's df
goes to the node's **control address = advertise_host:--control-port, or
advertise_host:(advertise_port+1000) without the flag**. An explicit
`--control-port` is both the port the node binds and the port it registers, so
behind a proxy it must be the same number on both sides.
If anything sits between the manager and an EN (proxy, NAT, port forward), it
MUST forward the control port alongside the data port, or every df fails
silently: recoveries complete on the target ENs but are never applied, the
fenced node's slots never rewrite, and `remove` blocks forever while
`extent-health` shows the same blocking extents each probe. Symptoms of this
wiring failure: all nodes `Suspected` with no last heartbeat (`list-nodes`), and
`recovery-stats` re-dispatching the same extent to a new candidate every
stale-sweep interval until every candidate refuses `extent already exists`
(re-dispatch to a candidate holding a verified-complete copy self-heals by
adopting it — but delivery still needs a working df channel).

Dead-EN notes (fence a node that's already unreachable):

- Everything above still works — seal probes and recovery just skip the dead
  replica. The failure modes that DON'T self-resolve are loud, never silent:
  an extent whose replicas are ALL unreachable refuses to seal
  (`Precondition`, sweep WARNs every cooldown), and a rebuild with no
  reachable source keeps retrying with the reason visible in
  `recovery-stats`'s backoff table.
- A fenced node that is still ALIVE drains faster (it serves as a recovery
  source). `cluster.sh` is fence-agnostic: `start`/`restart` launch fenced ENs
  normally (registration is one-time at `format`; only RE-registration of a
  fenced/removed node is refused).
- If a partition has no serving PS, its tails can't be rolled until the
  rebalancer assigns one (the sweep WARNs per cooldown). Manager-unilateral
  seal is a recorded follow-up, not built.

### EN identity is a UUID, not an address

An extent node's stable identity is a **UUID**, decoupled from its network
address — the same split the PS already has (`ps_id` vs advertise address).
`autumn-op format` mints a UUID v4 once and stamps it into a `node_uuid`
sentinel file in **every** `--data` dir (reused verbatim on a re-format, so a
re-format keeps the same `node_id`). It rides on `MSG_REGISTER_NODE`, and the
manager keys the node by it:

- **IP / shard-port change keeps the `node_id`.** A node that comes back at a
  different address (k8s pod reschedule) or with a changed shard-port layout is
  recognised by its UUID — the manager updates the routing address in place
  instead of minting a duplicate node. `list-nodes` shows the same `node_id`.
- **The fence / decommission tombstone is keyed by the UUID and survives
  removal.** A fenced/decommissioned node returning under its own UUID — at
  *any* address, and even after `remove` deleted its node record — is refused.
  Clear it with `autumn-op unfence <id>` (which now also lifts the
  `decommissioned/` tombstone) before it can rejoin, or wipe its data dirs for
  a fresh identity.
- **One address hosts exactly one node.** A *different* UUID registering at an
  address a live node already holds is **refused** (`CODE_PRECONDITION`) — two
  records at one address would make one physical EN two failure domains. To
  recycle a pod IP for a genuinely new node, `fence` + `remove` the old node
  first (freeing its address); the fresh UUID is then accepted.
- **Legacy (uuid-less) nodes are adopted.** A node that first registered before
  M0 (empty UUID) adopts the UUID on its next register at the same address.

The full design (including the k8s topology and the phased milestones) is in
[`en_dynamic_shard_design.md`](en_dynamic_shard_design.md). **Deploy note:** the
`node_uuid` field is in-struct on the persisted `MgrNodeInfo`, so this is a
same-commit stop-world upgrade: stop every role, swap binaries, start. There is
no rolling upgrade and no rollback across it. **M0 shipped NO migration for
pre-`node_uuid` rows** — a manager replaying them fails the rkyv decode and
refuses to lead (fail-loud, never mis-read). Production etcd is never wiped, so
a cluster predating M0 needs a one-shot migration written before it can upgrade;
a dev cluster rebuilds from empty instead.

### Resharding an extent node — changing its shard count

An EN's shard count = the number of io_uring cores it runs (one shard per core),
sized by `--cpuset` (`shard_count = cpuset_len`). Each shard `i` listens on
`--port + i*--shard-stride` and owns the extents where `extent_id % shard_count
== i`. Because the on-disk layout is hashed by `crc32c(extent_id)` (NOT by
shard) and all shards share the data dirs, **a reshard moves ZERO bytes on
disk** — only ownership/routing remaps by the new modulus.

Resharding is **stop-the-world for that node** (design decision #4): the EN
re-reports its live `shard_ports[]` to the manager on startup (needs
`--advertise`; the manager keys by `node_uuid` and updates the location in
place), so a restart with a different core count is the whole mechanism — no
`autumn-op format` re-run, no data migration.

```bash
# 1. Note the current shard count.
autumn-op --manager <MGR> list-nodes        # SHARDS column

# 2. Stop the EN process (SIGTERM). Its extents stay on disk untouched.
#    (Its slots go Suspected within ~2 s; reads/writes route to replicas.)

# 3. Restart the EN with the NEW core count. `--advertise` MUST be set so it
#    self-registers the new shard ports. Example: 2 -> 4 shards.
autumn-extent-node --data <DIRS> --port 9101 --manager <MGR> \
    --advertise <IP>:9101 --cpuset 0-3        # 4 cores = 4 shards

# 4. Verify the manager picked up the new layout (SHARDS should now read 4,
#    and the node returns to Online after its first df ~2 s later).
autumn-op --manager <MGR> list-nodes
```

Requirements / caveats:
- **The new shard ports (`port + i*stride`) must be free** on the host. On k8s
  the pod's Service must expose exactly `shard_count` data+control ports — that
  Service-port generation is a deploy-layer follow-up; on
  bare-metal / `cluster.sh` the ports just need to be unbound.
- **`--advertise` is what enables self-registration.** Without it the EN keeps
  the `format`-stamped location and the shard count stays frozen (pre-M1
  behavior). `cluster.sh` passes it automatically.
- Per-EN: shard count is independent per node — you can reshard one EN without
  touching the others (its extents remap under the new modulus; siblings are
  unaffected).
- A returning EN under its own `node_uuid` reuses its `node_id`; the manager's
  df-echo check (M1b) WARNs if the stored location drifts and refuses to serve
  an imposter that reused the node's IP under a different uuid.

## Fleet status (`autumn-op status`)

```text
$ autumn-op status
Manager   leader 1 / standby 0   (1 expected)
PS        Ready 0/1
EN        Online 2/3
Extent    clean 0 / degraded 0 / unavailable 0   (0 sealed)
Recovery  inflight 0
sampled   2026-10-07 06:45:34 UTC by manager 1 (127.0.0.1:9001); oldest EN df 0s
  PS 1 127.0.0.1:9301  evicted 0s
  EN 3 127.0.0.1:20002  suspected 12s
```

Every denominator is the EXPECTED set: manager members (`manager-remove`),
PS members (`ps-remove`), registered extent nodes (`remove`). A server that
stopped stays counted, as absent / evicted / suspected, until it is back or
removed. Each member that is not up gets a line with its state and age (time
since it left, or since its last heartbeat / `df`). `--json` gives the same
data with every member; the dashboard's Overview tab shows the same counts as
its status bar. Only the leader answers; anything else fails with
`not leader` rather than print an old view, and so does a leader that cannot
read `managerAlive/` from etcd. Right after a leader change an extent node
reads `unknown (no df yet)` until it answers this leader's `df` (≤ 2 s per
node when it is up): the new leader has no first-hand word on it yet.

Manual check: `cluster.sh reset 3` → `status` shows `Ready 1/1`, `Online 3/3`;
`kill -9` one extent node and the PS (by PID) → within ~12 s `PS Ready 0/1`
with the PS `evicted`, `EN Online 2/3` with that node `suspected`.

## Retiring a partition server (`ps-remove`)

The manager remembers every psid that has ever registered (`psMembers/` in
etcd) and counts the fleet against that set. A PS that stops heartbeating is
evicted after 10 s — its partitions move to the others — but it stays a
member, listed as `evicted Ns ago`, so the fleet reads `2/3`, not `2/2`.

```bash
AO=(./target/release/autumn-op --cluster-secret-file "$DR/cluster.secret" --manager 127.0.0.1:9001)
"${AO[@]}" info                         # partition servers: ... evicted 37s ago
"${AO[@]}" ps-remove 7 --by you         # only once PS 7 is stopped AND evicted
```

`ps-remove` refuses (`code=3 precondition failed: ps 7 is registered at ...`,
exit 2) while the PS is in the live registry: stop it and wait the 10 s
eviction first. Unknown ids answer `code=1 not found`. A removed psid that
starts again simply rejoins. Each attempt lands in `audit-log` as `remove_ps`.

**psid uniqueness is yours to guarantee.** The manager does not judge
registrations: two processes started with the same `--psid` are counted as one
PS, the later registration's address wins, and nothing reports an error.

Manual check (real binaries, debug build is fine): start etcd + manager + two
PS (`--psid 1`, `--psid 2`), `kill -9` PS 2, wait 14 s → `info` shows PS 2
`evicted`; `ps-remove 1` → refused; restart the manager → PS 2 still listed as
evicted; `ps-remove 2` → `remove: ok`; again → `not found`; `info` lists PS 1
only. Do not `pkill -f` by a pattern your own shell command contains — it kills
the shell; take PIDs from `pgrep autumn-manager` / `pgrep -x autumn-ps`.

## Manager ids and `manager-remove`

With `--etcd` every manager needs `--manager-id <N>` (non-zero, unique per
manager; it exits 2 without one). `cluster.sh` uses 1, `autumn-deploy` the
manager's index + 1, the container entrypoint `AUTUMN_MANAGER_ID` or the
StatefulSet ordinal + 1. While it runs a manager holds `managerAlive/<id>` on
its own 10 s etcd lease; the leader keeps every id it has seen in
`managerMembers/` — the expected manager count.

- **A second process with a held id waits** — it does not listen, replay or
  campaign — and logs `manager id is held by another process; waiting
  manager_id=N holder=<addr>` every second. A restarted manager waits at most
  10 s for its predecessor's lease, so a restarted manager opens its port up
  to ~12 s late (`cluster.sh` waits 30 s for it).
- **The recorded address is the `--listen` address.** A manager listening on
  `0.0.0.0` is listed and named in conflict logs as `0.0.0.0:<port>`; the
  manager has no `--advertise`.
- **An etcd blip does not stop a manager.** A manager that lost its lease
  claims the id again; it exits only if another process took the id in the
  meantime (`manager id taken by another process while this one lost its
  lease; exiting`).
- **Retiring a manager:** stop it, then
  `"${AO[@]}" manager-remove <id> --by you`. Refused (`code=3`) while
  `managerAlive/<id>` exists, i.e. until its lease lapses (≤ 10 s); unknown ids
  answer `code=1 not found`. Audited as `remove_manager`.

Manual check of the duplicate and exit paths (etcd's JSON gateway, no
etcdctl needed): start etcd on `$E` and manager A with `--manager-id 1`; start
B with the same id on another port → B's log repeats the "held by another
process" line and its port is closed. Then take A's id from under it:

```bash
K=$(printf managerAlive/1 | base64)
LEASE=$(curl -s $E/v3/kv/range -d "{\"key\":\"$K\"}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["kvs"][0]["lease"])')
V=$(printf 'someone-else\n10.0.0.9:9001' | base64 -w0)
curl -s $E/v3/lease/revoke -d "{\"ID\":\"$LEASE\"}"; curl -s $E/v3/kv/put -d "{\"key\":\"$K\",\"value\":\"$V\"}"
```

Within ~5 s A logs `manager id keepalive failed; reclaiming` then the
`taken by another process ... exiting` line, and its process is gone. The put
has no lease, so it holds id 1 forever: delete it before starting anything as
id 1 again (`curl -s $E/v3/kv/deleterange -d "{\"key\":\"$K\"}"`). Revoking
the lease WITHOUT the put instead shows A reclaim the id (`manager id
reclaimed`) and keep running. Kill test processes by the PIDs you started
(`$!`), not `pkill -f`: a pattern your own command line contains kills your
shell.

## A promoted standby keeps the live PS fleet

A newly promoted manager gives every PS it replays a fresh 10 s heartbeat window,
however long it waited as a standby. The test (needs `etcd` on `PATH`; ~45 s):

```bash
cargo test -p autumn-manager --test ps_members_etcd
```

On a cluster: leave the standby up longer than 10 s, then kill the leader. In the
new leader's log, `PS <id> heartbeat timed out` may name only a PS that has
actually stopped, and only 10 s or more after the promotion. `"${AO[@]}" info`
should still list every live PS with its partitions. A failure shows every PS
timing out within ~2 s of the promotion and all partitions moving to the first
PS to re-register.

## An owner epoch answered out of order does not move memory back

Two acquires of one owner key can be answered out of commit order: the etcd
client re-sends a request after its timeout (`--etcd-request-timeout-ms`) on a
new connection while the old connection still delivers the other acquire's
reply. The manager's in-memory epoch must end at etcd's latest write, or the
leader fences the real owner until the next failover. The test (needs `etcd`,
`toxiproxy-server` and `toxiproxy-cli` on `PATH`; ~2 s) holds the replies with
a toxiproxy latency toxic and releases them after the re-send:

```bash
cargo test -p autumn-manager --test owner_epoch_etcd_reorder -- --nocapture
#   → first=9 a=12 b=11 etcd: ... mod_rev=12 ... memory=12   (memory=11 before the fix)
```

The same re-send can land a whole-record write late. `update_stream_ec` is
value-CAS'd against the record it read, so a tail allocated while it was in
flight survives the next leader's replay (same binaries on `PATH`; ~15 s, it
includes a leader change):

```bash
cargo test -p autumn-manager --test update_stream_ec_cas -- --nocapture
#   → U answered ...: code 3 precondition failed: stream changed concurrently (CAS conflict) ...
#   → replayed: [4, 5] ec 1/0          ([4] before the fix: the tail was gone)
```

A txn whose reply or request was lost is settled, not re-sent blind: the
manager moves its leader fence, reads the written keys back and re-sends only
what provably did not land (same binaries; ~25 s for both cases):

```bash
cargo test -p autumn-manager --test etcd_resend_after_timeout -- --nocapture
#   → first alloc at ~1.0 s: code 0      (code 3 and every later alloc refused before the fix)
#   → cycle N: etcd mod_rev=<settled>    (the late copy committed over it in ~40% of runs
#                                         without the fence move; five cycles per run)
```

## fs stripe geometry: lanes vs partitions

Large-file striping spreads one file's extents across N **lanes** so a single
write escapes the one-partition/one-log_stream ceiling. The key idea is that
**lanes and partitions are separate decisions**:

* **lanes** = the KEY LAYOUT. `lane = (offset / unit) % lanes`, encoded high in
  the key so it dominates routing. Default **24** (`DEFAULT_STRIPE_LANES`) —
  every file is striped whether or not anyone ran presplit.
* **partitions** = PLACEMENT. A partition owns a *contiguous run* of lanes.

Striping unconditionally is what makes placement changeable later: a file
written on a 1-partition fs already has its extents sorted by lane, so a split at
a lane boundary gives it parallelism **retroactively** — no data rewrite, no
re-stamping. (Before this, an fs that was never presplit wrote legacy keys that
sit in lane 0 forever; growing to 24 partitions did nothing for them.)

24 is over-provisioned on purpose. Any partition count that **divides** 24 —
1, 2, 3, 4, 6, 8, 12, 24 — distributes every file evenly, so the lane count is a
permanent constant instead of a function of cluster size. That is why a file's
stripe width never needs to widen.

Two pieces of state, different jobs:

* `fs/[0x04]stripe_geom` — the declared geometry `{lanes, unit_bytes}`. What NEW
  files get stamped with. Absent ⇒ the 24-lane default.
* `InodeMeta.stripe` — each file's ACTUAL geometry, immutable once written. Reads
  consult only this, never the cluster's current shape, so any
  split/merge/rebalance leaves existing files correct.

```bash
# Declare 24 lanes and cut 6 partitions (6 divides 24 → 4 lanes each).
$AO presplit --namespace fs --lanes 24 --parts 6
# → declared fs stripe geometry: 24 lanes × 8 MiB units
# → presplit /fs: 5/5 cut points applied

$AO presplit --namespace fs --lanes 24 --parts 5     # rejected:
# parts must DIVIDE lanes ... Divisors of 24: 1, 2, 3, 4, 6, 8, 12, 24
```

`--parts` omitted ⇒ one partition per lane. `--lanes 1` turns striping off.

**Presplit an EMPTY keyspace, before loading data.** A data-bearing partition
can't be re-split until major compaction clears CoW out-of-range keys, so cuts
land only partially (`has_overlap`) if you load first.

### Declared boundaries: split there first, never merge there

`presplit` records the intended cut points on the namespace registry row. That record drives BOTH halves of a symmetric rule:

* **merge refuses** to erase a declared boundary (`--force` to override). This
  matters because an EMPTY lane partition is a perfect auto-merge candidate
  (cold, tiny, zero QPS), and the window where lanes sit empty is exactly the
  reset → presplit → first-upload sequence. Merging one away is silent: every
  LATER large file just stripes narrower, with no error anywhere.
* **auto-split snaps** to the declared boundary nearest the middle of the
  partition, instead of the PS's median user key (which for fs lands *inside* a
  lane and breaks the whole-lane invariant). Once a partition holds no declared
  boundary, it falls back to median — an intra-lane inode split, which is the
  right cut at that point.

So you don't strictly have to run presplit at all: declare the points and the
cluster walks itself toward that layout as load grows. (Auto-split is local and
reactive, so it converges on "each partition owns a run of whole lanes", not on a
perfectly even parts-divides-lanes split — that evenness is a planned,
presplit-time property.)

```bash
$AO merge 12 13
# → refusing to merge 13 into 12: the boundary between them is a presplit point
#   declared for namespace 'fs' ... Re-run with --force if that is intended.
$AO merge 12 13 --force          # deliberate
```

The protection is generic — kvc hash buckets and mem agent cuts get it too; the
manager never learns what a "lane" is.

Notes:
* Striped WRITES are an `autumnfs` capability. A fuse mount reads and removes
  striped files correctly but **refuses** to write or truncate one (by design);
  write large files with `autumnfs put`.
* The download read window scales with the file's lane count (`get_window_extents`),
  because a window of W consecutive extents only spans W consecutive lanes —
  a fixed window would have quietly lost read parallelism once lanes were
  over-provisioned relative to partitions.

### Merge refuses a partition that still carries its parent's tables

After a split, each child keeps referencing the parent's SSTs, which hold keys on
BOTH sides of the cut (`has_overlap = 1`). A merge refuses while EITHER side is in
that state: the merged partition would re-expose one side's stale copies over the
other's own history — values from before the split, and keys deleted after it.
Only a major compaction of that side separates it. The auto-policy does this by
itself (it advises `major compaction before merge` for each overlapping side);
by hand:

```bash
$AO info --part 12 --detail | grep has_overlap     # 1 = still carries parent tables
$AO --wait merge 12 13
# → cannot merge: partition has overlapping keys (CoW tables from a split); major-compact it first
$AO --wait compact 12 ; $AO --wait compact 13     # both sides, not just the survivor
$AO info --part 13 --detail | grep has_overlap     # 0 on both (load heartbeat, ~5 s lag)
$AO --wait merge 12 13
```

Regression: `cargo test -p autumn-manager --test system_merge still_carrying`.

## Inspecting authz: who exists and what may they touch

`principal-create` / `principal-delete` shipped without a listing, so until now
answering "which principals exist and what are they granted" meant either
`ls $DATA_ROOT/authz/*.cred` (only what cluster.sh's turnkey path happened to
write — nothing an operator minted by hand) or an etcd key scan
(`etcdctl get --prefix --keys-only principal/`), which shows names
but NOT grants because the value is rkyv.

```bash
$AO principal-list
# NAME                 GRANTS
# fs                   fs/
# kvc                  kvc/
# mem                  mem/

$AO principal-list --json     # [{"name":"fs","grants":["fs/"]}, ...]
```

Read-only and leader-routed (rotates on NOT_LEADER). It never prints credential
material: the response row type
carries only `(name, grants)` — `credential_hash` is not a field on it, so there
is no flag or future edit that can make it leak. A lost credential is re-minted
(`principal-create` again, which rotates), never recovered.

The namespace-side counterpart is `namespace-list` (registry rows: name / prefix
/ presplit / created_at).

## Reads ahead of large writes on one PS connection

A client's reads to a partition (HEAD, which `BatchClient.exists` uses, and the
redirect and range lookups) take the RPC client's urgent lane, and each PS
connection holds at most `--ps-send-window` (client parameter, default 3, 0 =
off) large requests (64 KiB and up) unanswered; further large ones wait in the
client. Without the window a client putting large values filled the PS's
admission slots on the connection (4 of them then), and a probe from the same client waited for a
put to commit. kvcache load, UCX, one BatchClient putting 128 x 16 MiB pages
while it probes: `exists` p99 270 ms → 8.6 ms; 8 ranks of 16 MiB puts: one
partition 0.44–0.57 → 0.82 GB/s, 16 partitions 2.32–2.63 → 2.54–2.74 GB/s.
perf-check (interleaved against the build without it): no cell slower; UCX
one-partition writes faster (8M ~490 → ~650 MB/s, 256K ~565 → ~740). Keep the
window under the PS's `--conn-inflight-cap`.

```bash
cargo test -p autumn-rpc --lib an_urgent_request_overtakes_queued_ones
cargo test -p autumn-rpc --lib the_send_window_holds_large_requests_but_not_urgent_ones
python3 - <<'PY'
import threading, time, numpy as np, autumn
bc = autumn.BatchClient("127.0.0.1:9001", 16, scope="kvc")
keys = [f"m/kv/p{i}".encode() for i in range(128)]
bc.put_from(keys, [np.zeros(4096, np.uint8)] * 128, 600)
page = np.zeros(16 << 20, np.uint8); stop = threading.Event()
def writer(r=0):
    while not stop.is_set():
        bc.put_from([f"m/kv/w{r}/{i}".encode() for i in range(64)], [page] * 64, 600); r += 1
t = threading.Thread(target=writer); t.start(); time.sleep(0.5)
lat = []
for _ in range(100):
    t0 = time.perf_counter(); assert all(bc.exists(keys)); lat.append((time.perf_counter() - t0) * 1e3)
stop.set(); t.join(); lat.sort(); print(f"exists p50 {lat[50]:.1f} ms  p99 {lat[98]:.1f} ms")  # p99 tens of ms, not hundreds
PY
```

## Per-connection admission and the registered pool

A PS admits `--conn-inflight-cap` requests per client connection (default 16);
with the slots full it stops reading the connection. The admitted requests of
all of a partition's connections are what one group-commit batch can hold, so
the cap bounds small-write batching: at 4, with the perf bench's two writing
connections per partition, every batch was 8 ops and 4 KiB writes stopped near
8K ops/s per partition whatever the client's depth.

Every large value in flight (a GET answered from the log, a bulk PUT, 64 KiB and
up) sits in a buffer from the process's pool (registered under UCX),
`--ucx-regpool-cap-bytes` for the whole process (default 16 GiB; the PS's
partitions, the EN's shards and a client's workers share it). Past the pool a
budgeted value is refused or waits; one taken unbudgeted comes from an overflow
chunk, mapped, faulted in and registered when it is made and freed when its
last buffer returns, and the throughput falls with that churn.
Size the pool on every side for the largest values in flight at once:

    pool / 5 >= connections per partition x --conn-inflight-cap x largest value

(received values have 40% of the pool and one partition at most half of that).
kvcache under a TP8 engine: 8 connections x 16 x 16 MiB pages = 2 GiB per
partition, so a pool a little over 10 GiB (11 GiB: the shares round down). A
value past a partition's share the PS refuses `Unavailable` (the SDK retries);
an extent node takes its appends unbudgeted, the PS already bounds them. The PS checks this at start (TCP or
UCX) and logs a WARN when a fifth of `--ucx-regpool-cap-bytes` < `--conn-inflight-cap`
x 128 MiB (8 connections x 16 MiB); it cannot see connection counts or value sizes, so a
larger deployment must still be sized by hand. The extent node holding a
partition's log extent needs the same headroom for the PS's concurrent reads,
and the client for its own concurrency (`batch_get_concurrency` x value size).
The pool grows in chunks (64 MiB, larger for a larger request) as in-flight
data needs them and never gives one back, so the process keeps its in-flight
peak pinned (on TCP: allocated, only the touched pages resident).

Measured with the earlier per-thread pool, so each "pool" below was one
partition / shard / client thread's cap (one host, UCX, one partition, 16 MiB
values, 4 threads x `--bulk 32`, so the PS holds 4 connections x the cap of
reads):

| `--conn-inflight-cap` | per-thread pool (PS / EN / client) | 16 MiB reads |
|---|---|---|
| 4 | 512 MiB | 8.5–9.2 GB/s |
| 16 | 512 MiB | 6.0–6.3 GB/s |
| 32 | 512 MiB | 3.0–3.7 GB/s (PS partition thread 0.54 cores registering) |
| 32 | 4 GiB on the PS only | 5.6 GB/s |
| 32 | 4 GiB everywhere | 8.5 GB/s |
| 16 | 2 GiB (the defaults then) | 8.8 GB/s |

TCP registers nothing: the same reads went 1.37 → 1.88 GB/s from cap 4 to 32.
`cluster.sh` passes `AUTUMN_UCX_REGPOOL_CAP_BYTES` to the PS and the extent
nodes; the client takes `--ucx-regpool-cap-bytes`. The value is now the whole
process's: a deployment that set it per thread (e.g. 2 GiB) must raise it to
cover all its busy partitions or shards at once. Check the warning (2 GiB is
under one partition's 32 x 128 MiB):

```bash
AUTUMN_TRANSPORT=ucx AUTUMN_BIND_HOST='[<roce-ip>]' UCX_TLS=rc_mlx5,ud_mlx5,tcp,self \
  UCX_NET_DEVICES=<dev>:1 AUTUMN_PS_CONN_INFLIGHT_CAP=32 AUTUMN_UCX_REGPOOL_CAP_BYTES=$((2<<30)) \
  bash cluster.sh start 3
grep 'is below --conn-inflight-cap' /tmp/autumn-rs-logs/ps.log   # one WARN
```

## Memory tier for KV cache (`--mem-tier-bytes`, `--mem-tier-prefix`)

Off by default. `autumn-ps --mem-tier-bytes 256G --mem-tier-prefix kvc/` (or
`AUTUMN_PS_MEM_TIER_BYTES` + `AUTUMN_PS_MEM_TIER_PREFIX` with `cluster.sh`)
holds every put of a key under the prefix — single puts of any size, and batched
puts (`put_many`, `BatchClient.put_from`: values under 64 KiB packed per
partition) — in the PS's
DRAM and acknowledges them without writing the log; reads look there first.
**What is held is not durable**: a PS restart, a partition reopen, split, merge
or move loses it, and a key that also had an older durable value reads that
value afterwards. Use it only for data that can be recomputed (the KV cache);
everything else should stay outside the prefix. The budget is for the whole PS;
a partition that needs room evicts its own write bursts, a burst at a time
(CLOCK: a new burst gets a full turn of the hand; only a read saves one after
that), and a put that still does not fit is written durably as usual (a
partition that filled its share and went idle keeps it). A batch is held only as a
whole: one fenced or uncovered op, or a batch the budget cannot take, makes the
whole batch durable (an op over the 64 MiB value cap gets the batch refused, as
without the tier).
Fenced puts (file-system leases) are always durable, and a value over the 64 MiB
value cap is refused as without the tier. A write that could not fit even with
its partition's held entries all evicted goes durable without evicting them. `RANGE` does not list keys held only in memory. Watch
`autumn_ps_mem_tier_*` on the PS `/metrics`.

```bash
cargo test -p autumn-partition-server --lib mem_tier
cargo test -p autumn-manager --test mem_tier
# live: start with the tier on, put a covered key, read it, restart the PS, read again
AUTUMN_PS_MEM_TIER_BYTES=$((64<<30)) AUTUMN_PS_MEM_TIER_PREFIX=kvc/ bash cluster.sh start 3
curl -s http://127.0.0.1:9801/metrics | grep autumn_ps_mem_tier   # bytes / hits / evictions
# eviction keeps a hot set read every round: 8 GiB tier, 1 GiB hot, 2 GiB written
# per round; run once from empty and once after --pre 6144 (steady state). Every
# round must print 512/512 (exit 1 otherwise); evicted bytes climb meanwhile.
AUTUMN_PS_MEM_TIER_BYTES=$((8<<30)) AUTUMN_PS_MEM_TIER_PREFIX=kvc/ bash cluster.sh start 3
python3 scripts/mem_tier_hot_set.py --manager 127.0.0.1:9001
python3 scripts/mem_tier_hot_set.py --manager 127.0.0.1:9001 --pre 6144
```

## KV-cache reads land straight in the caller's pages

`BatchClient.get_into` (the kvcache adapter's read) receives each value of
64 KiB and up directly into the destination page: no copy out of a pool
buffer. From PS memory, one worker thread reads about 5.4 GB/s on loopback TCP
(3.0 before); eight about 27–30 (18). Over UCX against a wire-76 or later cluster the
value comes as an active message that UCX moves by rendezvous into the page
with no receive copy: one worker ~42 GB/s at ~4% CPU (6.4–6.9 with the stream
receive, which costs a core one copy into memory not in cache — `ucx_perftest
-t stream_bw -s 1073741824`: 7.5 GB/s); more workers add nothing there. A read that times
out in the middle of a value closes that PS connection; the other reads on it
retry. To check: the tests below, then a live read compared byte for byte.

```bash
cargo test -p autumn-rpc --lib -- call_into_dest in_place after_a_timeout a_pooled_receive
cargo test -p autumn-manager --test get_into_raw
AUTUMN_UCX_TEST_BIND='[<roce-ip>]:0' UCX_TLS=rc_mlx5,ud_mlx5,tcp,self UCX_NET_DEVICES=<dev>:1 \
  cargo test -p autumn-transport --features ucx --test ucx_recv_data --test ucx_am -- --test-threads=1
# live (TCP cluster up, kvc namespace): put 128 x 16 MiB pages, get_into, compare
python3 - <<'EOF'
import numpy as np, autumn, time
n, page = 128, 16 << 20
bc = autumn.BatchClient("127.0.0.1:9001", 64, False, scope="kvc")
src = np.random.randint(0, 255, n * page, dtype=np.uint8); dst = np.zeros_like(src)
keys = [f"check/{i}".encode() for i in range(n)]
assert all(bc.put_from(keys, [src[i*page:(i+1)*page] for i in range(n)], 3600))
t = time.time(); ok = bc.get_into(keys, [dst[i*page:(i+1)*page] for i in range(n)])
print(all(ok), (dst == src).all(), f"{n*page/(time.time()-t)/1e9:.1f} GB/s")
EOF
```

## Registered buffer pool budget (`--ucx-regpool-cap-bytes`)

The process's pooled receive/send buffers live in chunks of at most this many
bytes in total (default 16 GiB, at least 64 MiB; sizing against the PS's admission:
"Per-connection admission and the registered pool"). Chunks are made as
in-flight data needs them (64 MiB each, a power of two up to 256 MiB for a
larger request), registered once under UCX, and kept; on TCP they are not
registered and only touched pages are resident. At the cap a thread shares
another thread's chunk; when no chunk has room, an unbudgeted buffer comes from
an overflow chunk outside the cap, registered when made and freed when its last
buffer returns (budgeted ones are refused or wait instead). The perf-check
client prints `regpool: ... over_cap=N overflow_made=M ... pool_bytes=
registered_bytes= in_use_bytes=` for its own process; a nonzero `over_cap` means
the working set does not fit the budget, and `overflow_made` close to it means
overflow chunks are made and freed over and over. Inside the cap each use has a share —
receive 40%, reply 40%, staging 20% — and one thread (a partition, a shard) at
most half of a share, so the cap also bounds a single value: the smaller of
256 MiB and a fifth of the cap for a received or read value (16 GiB: 256 MiB;
1 GiB: about 200 MiB). The client also prints one line per share, `regpool
reply: high_water= waits= timeouts= refused= too_large=`; the PS and the extent
node log the same per share (`receive=`, `reply=`, `staging=`) with
`--regpool-log-interval-secs N` (`AUTUMN_REGPOOL_LOG_INTERVAL_SECS` in
cluster.sh). On the PS and the client a nonzero `refused` is values a read loop
answered `Unavailable` at once for want of a buffer (the SDK retries, the PS
reads through its copy path; a small reply already in memory is taken anyway
and not counted). On an extent node, bulk reads answered `Unavailable` count in
`reply` `timeouts` (it waited up to 1 s) or `refused` (its batch had already
waited the 1 s out); its appends are never refused and never counted as refused.
`too_large` counts values over a thread's share that a refusing or waiting
caller met (taken unbudgeted).

```bash
# a full pool refuses and every request still completes (in-process, TCP)
cargo test -p autumn-manager --test pool_full_liveness -- --nocapture
UCX_TLS=rc_mlx5,ud_mlx5,tcp,self UCX_NET_DEVICES=<dev>:1 \
  cargo test -p autumn-transport --features ucx --test ucx_regpool_cap --test ucx_regpool_late_init
# allocation cost and registered bytes against another build of the pool (copy
# the bench file there); REGPOOL_XTHREAD=0 drops every buffer on its own thread
UCX_TLS=rc_mlx5,ud_mlx5,tcp,self UCX_NET_DEVICES=<dev>:1 \
  taskset -c <idle cores> cargo bench -p autumn-transport --features ucx --bench regpool_alloc
```

## Caller memory over UCX: registered regions and the staging copy

Memory UCX sends must carry a registration autumn made: a pool buffer's, or a
region the caller registered. Anything else is copied into a pool buffer at the
send (`*_ucx_am_staged_bytes_total`), so UCX never registers memory itself — a
read-only mapping (`np.memmap(mode='r')`) would hang its send. Python:

```python
import autumn, numpy as np
autumn.set_transport("ucx")
bc = autumn.BatchClient("[<roce-ip>]:9001", scope="kvc/")
pool = np.empty(32 << 20, np.uint8)          # long-lived pages (kvcache L2)
region = autumn.register(pool)                # once; read-only buffers are refused
pages = [memoryview(pool[i << 21:(i + 1) << 21]) for i in range(16)]
before = autumn.ucx_am_counters()
bc.put_from([f"kvc/p/{i}".encode() for i in range(16)], pages, 0, region)
print(autumn.ucx_am_counters(), before)       # staged unchanged: zero-copy
bc.put_from([b"kvc/q"], [memoryview(np.zeros(2 << 20, np.uint8))])  # staged += 2 MiB
region.unregister()
```

`put_from` returns only once the transport no longer reads any of the buffers
(a send that outlived a timeout included), so they may be reused at once. A
registered page is read by UCX until its send completes: if the PS dies in the
middle, UCX fails the send once it notices (at once for a killed process,
through its keepalive for a lost host), which gives the page back, and
`put_from` ends as the put does (an error, or a retry that succeeded).
Unregistered pages are copied at the send and do not wait on the peer. Check:
overwrite the pages right after `put_from` returns, then `get_into` them — the
stored values are the old bytes.

Reads into caller memory follow the same rule: `bc.get_into(keys, pages,
region)` receives straight into pages inside `region`; pages outside any
region land in a pool buffer and are copied in (`landed` in
`autumn.ucx_am_counters()`):

```python
dest = np.zeros(16 << 24, np.uint8); r = autumn.register(dest)
pages = [memoryview(dest[i << 24:(i + 1) << 24]) for i in range(16)]
c0 = autumn.ucx_am_counters()
bc.get_into([f"kvc/p/{i}".encode() for i in range(16)], pages, r)
print(autumn.ucx_am_counters()["landed"] - c0["landed"])   # 0; without r: 256 MiB
```

## UCX peer failure: a closed or dead peer fails its transfers

Every UCX endpoint runs with peer error handling (`UCP_ERR_HANDLING_MODE_PEER`).
A peer that closes its connection or dies fails every request on the endpoint —
a send UCX was reading from caller memory, a value being received, a stream
read — and the endpoint is closed (`FORCE`) when its connection is dropped.
The callers log what their requests got (`rpc client reader exited ...
Connection reset by remote peer`, `append soft-error`); the endpoint's own
report is at debug (`RUST_LOG=autumn_transport=debug`, `ucx endpoint failed`).

Tests (RoCE host):

```bash
AUTUMN_UCX_TEST_BIND='[<roce-ip>]:0' UCX_TLS=rc_mlx5,ud_mlx5,tcp,self UCX_NET_DEVICES=<dev>:1 \
  cargo test -p autumn-transport --features ucx --test ucx_dead_peer --test ucx_am_frame --test ucx_dropped_recv
# ucx_dead_peer: a child process holding a lent value is SIGKILLed; the value
# comes back and the value awaited from it fails (prints how long it took)
```

Live (UCX cluster, `kvc` namespace): `put_from` of registered pages while the
PS is killed. The call in flight returns (measured: 13.4 s, 10 of 16 pages
written); it never blocks. A client built before peer error handling cannot
connect to a cluster with it: rebuild the wheel and the SDK binaries with the
cluster.

```python
import autumn, numpy as np, time
autumn.set_transport("ucx")
bc = autumn.BatchClient("[<roce-ip>]:9001", scope="kvc/")
pool = np.random.randint(0, 255, 16 << 24, dtype=np.uint8)
region = autumn.register(pool)
pages = [memoryview(pool[i << 24:(i + 1) << 24]) for i in range(16)]
for r in range(100):            # kill -9 "$(cat $AUTUMN_DATA_ROOT/pids/ps.pid)" meanwhile
    t = time.time()
    try: ok = sum(bc.put_from([f"kvc/live/{i}".encode() for i in range(16)], pages, 0, region))
    except Exception as e: ok = e
    print(r, f"{time.time() - t:.1f}s", ok)   # every round prints; none hangs
```

## Large values over UCX as frame values (wire 77)

On UCX, every value part of 64 KiB and up that crosses a hop as part of a frame
travels beside it as an active message by rendezvous, so the receiving NIC
writes it into a pool buffer and no receiving CPU copies it: client → PS bulk
writes, PS → extent node appends, extent node → PS reads, extent node → client
direct reads, PS → client bulk reads. Smaller parts, TCP, and any peer below
wire 77 keep the stream exactly as before. An append sent this way carries its
payload's crc32c, which the extent node recomputes before writing; a mismatch
fails that replica's append (the stream client retries on a new extent). Each
process logs once when it first receives one: `received a frame value by UCX
rendezvous (first in this process)`.

```bash
cargo test -p autumn-rpc --lib -- am_frame prepared_payload frame_value
AUTUMN_UCX_TEST_BIND='[<roce-ip>]:0' UCX_TLS=rc_mlx5,ud_mlx5,tcp,self UCX_NET_DEVICES=<dev>:1 \
  cargo test -p autumn-transport --features ucx --test ucx_am_frame
# needs the RoCE bind and this UCX_TLS: the read burst only goes red over a
# transport that registers memory (rc_mlx5), never over `self` or shared memory;
# it also checks the node's read replies were sent with their slab's memh (65, 0)
AUTUMN_TEST_UCX_BIND='[<roce-ip>]:0' UCX_TLS=rc_mlx5,ud_mlx5,tcp,self UCX_NET_DEVICES=<dev>:1 \
  cargo test -p autumn-stream --features autumn-rpc/ucx --test prepared_append
# frame values from pool slabs with their memh vs through UCX's cache; build
# first, then pin to idle cores on the NIC's NUMA node (each sender has its own
# receiver thread). One sender: the arms match. Eight: "slab" needs a fraction of
# the sender cpu/msg of "rcache" (64 KiB ~4 vs ~10+ µs).
cargo bench -p autumn-transport --features ucx --bench am_memh --no-run
AUTUMN_UCX_TEST_BIND='[<roce-ip>]:0' UCX_TLS=rc_mlx5,ud_mlx5,tcp,self UCX_NET_DEVICES=<dev>:1 \
  AM_MEMH_SENDERS=8 taskset -c <16 idle cores> cargo bench -p autumn-transport --features ucx --bench am_memh
# live (UCX cluster up, namespace bench created): sizes around the 64 KiB cut, compared byte for byte
AC=(target/release/autumn-client --manager "[<roce-ip>]:9001" --transport ucx --namespace bench/perf)
for n in 1000 65535 65536 100000 1048583 8388608 34603017; do
  head -c $n /dev/urandom > /tmp/v$n; "${AC[@]}" put fv$n /tmp/v$n >/dev/null
  "${AC[@]}" get fv$n | cmp - /tmp/v$n && echo "ok $n"; done
grep -l "frame value by UCX rendezvous" /tmp/autumn-rs-logs/*.log   # ps.log and every node*.log
```

## One extent read message (wire 78)

The partition server reads extent nodes with `MSG_READ`: a range and a mode
(exact, up to, to end); an exact read the replica cannot serve in full is
refused, never answered short. Wire 78 changes only what cluster members say
to each other, so upgrade the manager, partition servers and extent nodes
together (stop all, start all); clients of wire 43 and up keep working, and
their direct reads still use `READ_BYTES_BULK`. To check:

```bash
cargo test -p autumn-stream --test extent_append_semantics -- read_serves_each_mode_as_it_says
cargo test -p autumn-rpc --lib -- an_ok_read_reply_ctrl
```

## Write options and conditional writes (wire 75)

Every SDK write takes `WriteOptions { ttl, lease }`: `put`, `put_many` and
`compare_put` honour both, `delete` / `delete_many` the lease. `put` picks the
transport by size (inline below 64 KiB, zero-copy `MSG_PUT_BULK` above);
`put_many` applies one TTL to the batch. `compare_put(key, expected, value, opts)`
is the one conditional write (`MSG_COMPARE_WRITE_BULK`): `expected = None` means
"only if absent", `value = None` deletes, the value has no 64 KiB cap. With
`expected = None` the PS checks only that the key is there (no value read). The
value is shipped even when the key is present, so it is no substitute for an
`exists` probe when most keys are already stored (8 ranks x 64 x 16 MiB over UCX,
all present: 350 ms vs 1 ms probe). A wire-75 client needs a wire-75 cluster; the
PS still serves the older conditional opcodes for older clients.

```bash
cargo test -p autumn-manager --test write_options   # TTL and lease through every write; conditional cases
cargo test -p autumn-partition-server --lib compare_write_bulk_tail_is_recvd_into_a_pooled_buffer
```

## autumn-kvcache model identity

vLLM-connector KV keys are `kvc/{model_scope}/vllm/...`, where the model scope
is `{model}_{fingerprint}_{tp...}` (`build_model_scope`). The
`{model}` segment is the autumn **weights-path basename** (e.g. `qwen7b` from
`model_loader_extra_config.path=models/qwen7b`), NOT the constant `/model-cfg`
config dir that several models can share — so the readable
segment ALONE distinguishes models even if the fingerprint ever degrades
(2026-08-11: keys now read `qwen7b_<fp>_0_1`, not the old collision-prone
`model-cfg_<fp>_0_1`). The 12-hex fingerprint carries the model's real identity
(arch shape + weights source + optional `model_id`; see
`autumn_kvcache/_identity.py`, in the autumn_kvcache repo). Before both, every model
served via the fixed local config dir shared ONE model scope and cross-read KV
(live 2026-07: Qwen2.5-7B/32B both under `kvc/model-cfg_0_1/`).

**Load is fail-closed (BUG-KVC-LOAD-ATOMIC, 2026-08-11).** When the scheduler
admits a request on the `__present__` marker but the worker cannot load EVERY
layer (TTL grace breach / model identity mismatch / backend fault), the connector now
injects NO KV for that request and reports its blocks via
`get_block_ids_with_load_errors()` so vLLM re-runs normal prefill. Previously it
injected the layers that loaded and skipped the rest → the request decoded on a
mix of loaded + uninitialised paged KV and emitted **silent garbage** (the live
symptom: `external KV load miss after positive presence` on layer 0..N). If you
see that warning now it is followed by a recompute, not a wrong answer. The fingerprint also folds in the two **layout
versions** — the running vLLM version (full `x.y.z`) and the connector's own
`VLLM_KV_STORAGE_FORMAT` (`_keys.py`) — so the same model on a
layout-incompatible stack never shares a model scope. Operational consequence:
**every vLLM upgrade (patch releases included) moves the model scope and
cold-invalidates the whole vLLM pool** — expected, one-time re-warm; the old
scope's keys need the same manual reclaim as below.

**`--kv-cache-dtype` is part of the identity too** (added 2026-07-22): the
connector stores raw KV bytes and reinterprets them with the *current* runtime
dtype, and `CacheConfig.cache_dtype` is independent of the model dtype. The
silent case is a same-itemsize flip — `fp8_e4m3` ↔ `fp8_e5m2` are both one byte,
so nothing errors and the KV is just wrong. `cache_dtype` (plus
`kv_cache_dtype_skip_layers`) therefore splits the model scope. **Changing
`--kv-cache-dtype` moves the model scope and cold-invalidates the pool**, same as a
vLLM upgrade. Note this also means the FIRST deploy carrying this change starts
from a cold vLLM pool even with no config change, because the fingerprint gained
a source — orphaned old-scope keys reclaim exactly as below.

```bash
# Offline unit tests (no cluster / engine / native module):
cd ../autumn_kvcache && uv run --with pytest python -m pytest tests/test_model_identity.py -q

# Manual verify on a live deployment: the connector logs its model scope +
# identity sources at startup — two DIFFERENT models must log two different scopes:
#   AutumnKVConnector role=... model_scope=qwen7b_<fp>_0_1 ... identity={'layers': 28, ...}
# and the stored keys must not share a model-scope prefix:
#   (autumn-client / python) list keys under kvc/ — one prefix per model.

# Upgrade note: the fingerprint changed every vLLM-pool key → old-scope keys
# (e.g. kvc/model-cfg_0_1/vllm/...) are orphaned; with ttl_secs=0 they never
# expire. Reclaim manually when convenient (venv with the autumn wheel):
#   python - <<'EOF'
#   import asyncio, autumn
#   async def main():
#       c = await autumn.Client.connect("MGR:9001")
#       print("deleted:", await c.batch_delete(b"kvc/model-cfg_0_1/vllm/"))
#   asyncio.run(main())
#   EOF
# The load-miss-after-marker warning now states the plausible causes given the
# TTL config (ttl=0 ⇒ never blames TTL; points at a model identity mismatch).
```

### External hit rate & the kill switch (BUG-KVC-NO-HIT)

The vLLM connector is an **L3 behind vLLM's own local prefix cache** (GPU + host
RAM). vLLM matches the local cache first and asks the connector only for tokens
*beyond* the local match, so:

- **same engine, repeated prompt** ⇒ local cache serves it ⇒ external is
  (correctly) never loaded ⇒ `External prefix cache hit rate: 0.0%`. **Expected,
  not a bug** — judge the connector by cross-instance / post-restart hit rate.
- **restarted or different engine, or after local eviction** ⇒ local cache is
  cold ⇒ the connector loads the prefix from autumn and skips prefill (measured
  ~3–4× TTFT win on a 1.3 k-token prefix).

Two changes killed the "kvc grows 20 GB while hit rate is 0%, prefill stalls"
symptom:

- **Almost everything is asynchronous.** On the forward pass `save_kv_layer`
  does ONLY the cheap GPU-side gather (a standalone tensor, no CPU sync). The
  D2H `.cpu()` copy, the **store-dedup probe**, the durable `put_from`, and the
  `__present__` marker all run on a background thread (a CUDA event orders the
  D2H after the gather; the marker publishes only after every layer ACKs). So a
  genuinely-new prefix no longer blocks prefill on the durable write, and a
  repeat is deduped in the background. Measured **no-hit overhead: TTFT +≈6–7 ms
  / TPOT ≈0** on both TCP and UCX (transport-independent, since the network work
  is off the critical path) — down from +≈148 ms when the D2H was synchronous.
- Staging: the in-flight background jobs hold *standalone GPU tensors* until
  their D2H runs (bounded per step by vLLM's token budget, and by
  `_MAX_INFLIGHT_SAVES` across steps); over the cap a save is dropped (a later
  request re-saves — pure cache).

Verify on a live deployment: a same-prompt request on a **freshly restarted**
engine (or after `reset_prefix_cache()`) should log an external hit and a much
lower TTFT than the cold-cluster first request; the first cold request returns
before its KV is durable, and the kvc partition's `live_size` grows in step with
distinct prefixes, not requests.

## Data-plane authz setup

Server-side key-range authorization for registered namespaces
(`data_plane_authz_design.md`): the manager acts as a KDC that mints
short-TTL Ed25519 capability tokens; the PS verifies them per connection
(`CLIENT_AUTH`) and enforces per request. **OPT-IN** — with no signing key
configured nothing changes (fuse / kvcache / perf-check / chaos all run
authz-off, anonymous, zero hot-path cost).

### Turnkey dev cluster with authz (`AUTUMN_AUTH=1`)

`cluster.sh` auto-provisions the whole authz bring-up so the examples work
end-to-end. `AUTUMN_AUTH=1` generates a signing key under
`$DATA_ROOT/authz/`, registers the `gallery` namespace, and mints credentials
for `fs/`, `kvc/` and `gallery/`:

```bash
AUTUMN_AUTH=1 ./cluster.sh reset 5      # → $DATA_ROOT/authz/{signing.key,fs.cred,kvc.cred,gallery.cred}

# gallery (scope gallery/) — Scoped client, credential via a flag:
./target/release/gallery --manager 127.0.0.1:9001 \
  --credential-file /tmp/autumn-rs/authz/gallery.cred
```

The example binds its namespace scope (`--scope`, default `gallery`) and
authenticates with `--credential-file`; the SDK auto-mints short-TTL tokens. It
reads no environment variable; `gallery --help` lists its listen address
(`--listen` / `--port`, default 0.0.0.0:5001) and tunables.

```bash
# 1) Generate a signing key (LOCAL, no cluster needed):
./target/release/autumn-op gen-signing-key --kid 1 > /path/signing.key

# 2) Start the cluster with authz enabled (cluster.sh env→flag translation;
#    every keyed op then needs a token — there is no protected-prefix list):
AUTUMN_AUTH_SIGNING_KEY_FILE=/path/signing.key \
  bash cluster.sh start 4

# 3) Create a PRINCIPAL (admin; credential printed ONCE as principal:/credential:
#    two lines — redirect straight to a credential file).
#    Keys are `{ns}/…`; a grant is a whole namespace (`fs/`)
#    or an in-namespace sub-prefix (`mem/acme/`):
AO="./target/release/autumn-op --cluster-secret-file /tmp/autumn-rs/cluster.secret --manager 127.0.0.1:9001"
$AO principal-create --principal acme --grant mem/acme/ > /path/acme.cred
# `--cluster-secret-file` and `--credential-file` work BEFORE or AFTER the
# subcommand — position does not matter.

# 4) Use it from the SDK (auto-mints + renews tokens, sends CLIENT_AUTH on each
#    PS connection and each extent-node direct-read connection; principal read from
#    the credential file):
#      ClusterClient::connect_with_credential(mgr, "mem/acme", principal, secret)
#    Cross-scope or anonymous access to any key fails PermissionDenied.

# Ops: mint a token by hand / revoke a principal:
$AO mint-token --principal acme --credential-file /path/acme.cred
$AO principal-delete --principal acme   # stops renewal; token dies at exp
# Key rotation: add a higher kid line to signing.key, restart the manager,
# wait a TTL, then mark the old line "disabled" (PS rejects it per request).
```

## CLI cheatsheet

```bash
AC="./target/release/autumn-client --manager 127.0.0.1:9001"
AO="./target/release/autumn-op     --manager 127.0.0.1:9001"

# Data plane
echo body | $AC put KEY /dev/stdin       # write
$AC get KEY                              # read
$AC head KEY                             # size only
$AC del KEY                              # delete
$AC ls --prefix p/ --limit 100           # scan
$AC put-stream KEY /path/to/big.bin      # chunked stripe-put for large values
$AC perf-check --threads 16 --size 4096 --duration 10 --partitions 8
$AC perf-clean --dry-run                 # count what perf-check / ycsb left (bench/perf)
$AC perf-clean [--parallel 8]            # delete it; each bench partition range in
                                         # parallel, one delete_many per 4096-key page.
$AC perf-clean --dry-run                 # expect 0 afterwards
# The deletes leave tombstones and dead values. A PS major-compacts a partition
# itself once its SSTs hold >= 10000 tombstones and >= 30% of entries (checked
# every --deletion-compact-check-secs, 300 s by default) — the memtable is not
# counted, so small cleans wait for the next flush. To reclaim now:
$AO compact PART_ID

# At-rest content check (see "Scrub" below): extents / a partition / everything
$AO scrub EXT_ID... | --part PART_ID | --all [--wait]

# SST block cache (paged SSTs; SST data blocks no longer RAM-resident)
# PS flag: autumn-ps --sst-block-cache-bytes N   (cluster.sh: AUTUMN_SST_BLOCK_CACHE_BYTES, default 512MB)
# Manual check: write >> RAM dataset, kill -TERM the PS, restart, then
#   `$AC get KEY` must byte-match and idle PS RSS stays at the replay-window
#   bound (GBs), not O(dataset). Recovery must log `open_partition: ready`
#   for every partition with no `stale_vp_offset_past_sealed_length` retries.
# Eviction is CLOCK; a sequential read must not fall off once the cache fills.
# Manual check: cluster.sh with AUTUMN_SST_BLOCK_CACHE_BYTES=33554432,
#   AUTUMN_PS_FLUSH_MEM_BYTES=8388608, AUTUMN_BOOTSTRAP_PRESPLIT=4 (cores
#   pinned away from other tenants); write 1M 200 B keys under bench/perf in
#   RANDOM order, then read them in key order with 8 readers and print ops/s
#   per 5% of each reader's slice. Expect a flat rate (~3.5K/reader from one
#   Python client); before the fix it fell from ~3.4K to ~210 after ~15%.
#   Same after `$AO merge 13 <V> --force` down to one partition (32 SSTs).
#   SSTs written by this build size their bloom filter from their key count.
# async SST iteration (no whole-SST materialization for range/compact/split)
# Manual check: on a multi-GB dataset, `$AO compact PART_ID` must log
#   "compact part N: ... output=..." and `$AC ls --prefix p/` must return
#   correct entries, while PS RSS stays bounded during both (read side =
#   8MiB windows, not Σ SST bytes). Striped keys (put-stream) byte-compare
#   via `$AC get-stream --out F KEY` (plain `get` returns the 45-byte
#   stripe meta by design).
# u64 offset widening — extents may exceed 4 GiB (default seal 16 GiB)
# PS flag: autumn-ps --max-extent-size-bytes N   (default 16 GiB, clamp [1,64] GiB)
# Manual check: into ONE partition, put-stream a > 4.3 GiB value (4 MiB chunks
#   accumulate in one log_stream extent so later chunk VPs sit at byte offset
#   > u32::MAX). `$AC get-stream --out F KEY` must byte-match (sha256) — this
#   reads chunks via the now-u64 `ReadBytesReq.offset`. Then kill -9 the PS,
#   restart, and `get-stream` again must match (recovery replays SST + WAL with
#   u64 offsets). EC: with 16 GiB extents a shard exceeds 4 GiB, served via
#   per-shard chunked reads (no `payload_len: u32` overflow). EC convert is
#   ALSO chunked (stripe-wise encode + offset-tagged WriteShard streaming):
#   peak RAM = (K+M)x64MiB regardless of extent size. Manual check: seal a
#   >1 GiB extent (writes roll it), `autumn-op set-stream-ec --stream S --ec
#   3+1` then `force-ec-convert --extent E`, confirm the EN logs "phase 1
#   (prepare) complete ... (chunked)" and then "EC shards staged on every
#   target; awaiting the manager's layout flip" (there is NO commit phase —
#   see the EC copy-on-write section below), then `get-stream` the value back
#   -> sha256 must match (chunk-encoded shards are byte-identical to a
#   whole-extent encode). Start the EN with a small `--ec-stripe-bytes`
#   (1 MiB minimum) to force many stripes on a smaller extent. Repro script: the isolated memory-mode loopback recipe (manager
#   w/o --etcd, 4 single-shard ENs, 1 PS) used in dev.
# EC never targets an unusable node, it substitutes one: put an EN that holds a
#   replica of a sealed extent E into maintenance (or stop it and wait for it to
#   show Suspected), keep one spare health node, then
#   `$AO force-ec-convert --extent E --wait` succeeds and `$AO info` shows E's
#   layout without that node (the spare holds its shard); the node's own copy is
#   dropped by its reconcile when it is back. With no spare it fails with "not
#   enough health nodes" and no EC marker is left. While any op is in flight on E
#   (a recovery, a delete) it fails with "has an in-flight <op>"; nothing
#   preempts an op in flight, GC's punch included. Right after a manager
#   failover a replica on a node not heard from yet makes it wait ("not heard
#   from yet this term") for one df round.

# Admin / observability
$AO info                                 # nodes / extents / streams / partitions
$AO bootstrap --replication 3+0          # --presplit RETIRED; use `presplit --namespace <NS>` after
$AO split PART_ID                         # or: split PART --namespace <ns[/sub]> --at <suffix>
$AO merge SURVIVOR_PART_ID VICTIM_PART_ID # add --force to cross a declared presplit boundary
$AO rebalance [MAX_MOVES]                 # re-spread partitions across PS
$AO compact PART_ID
$AO gc --ratio 0.4 PART_ID                # NB: gc flags come BEFORE the partition id
$AO gc --dead-bytes 1GiB PART_ID          # absolute floor: take an extent holding >=1 GiB dead
#   whatever its ratio. A 16 GiB log extent with 3 GiB dead is ratio 0.195 — under
#   the 0.4 gate and under the 0.2 it becomes with stream-debt relief — so ratio
#   alone never reclaims it while `gc_debt` keeps the advisory firing. The
#   controller now sends this automatically, sourced from the policy's
#   `gc_debt_high`, so both ends judge on the same number. A shared extent
#   (`refs > 1`) additionally gets the ratio bar halved: its FILE is only freed at
#   `refs == 0`, so collecting one side's reference is what lets it become
#   independently owned — though `refs` is read from a cache the PS does not
#   invalidate on split or on a sibling's punch, so it can lag.
#   NOTE: a bare `$AO gc PART` (and the dashboard's GC button) now carry the
#   cluster's STANDING policy — when the request names no gc knobs at all, the
#   manager fills the floor in from its own `gc_debt_high`, so "just GC this
#   partition" collects what the advisory fired on. Naming ANY knob
#   (--ratio / --dead-bytes / --max-size / --stream-debt / --empty-only) makes
#   it an OVERRIDE:
#   it runs exactly as asked, but it deliberately does NOT redefine what that
#   partition's `gc_debt_bytes` gauge means for every later tick.
#   COST of that floor: it is a PER-EXTENT number taken from a partition-total
#   threshold, so at defaults (gc_debt_high 1 GiB, --max-extent-size-bytes
#   16 GiB) an extent qualifies at 6.25% dead — up to ~15 GiB of live data
#   relocated per 1 GiB reclaimed, x --gc-max-extents-per-run=3 per dispatch, throttled by
#   the 128 MiB/s GC admission cap. Safe (relocate-then-punch, never a torn
#   read), but a bare `gc PART` on a healthy partition used to be ~a no-op and
#   can now be a multi-GiB rewrite. Under --policy-fast-mode the floor is 1 MiB,
#   so dev/chaos clusters will GC any extent holding >=1 MiB dead.
#   Check what GC would actually take:
#     $AO --json info --part PART --detail | grep gc_debt_bytes
$AO policy-candidates                    # advisory engine output (split/merge/gc/compact/EC)

# Cluster lifecycle (subshells so cwd stays at the repo root for ./cluster.sh)
(cd deploy/baremetal && ./autumn-deploy -t topology-singlehost.conf start)         # deploy path
(cd deploy/baremetal && ./autumn-deploy -t topology-singlehost.conf destroy --wipe) # tear down + wipe
./cluster.sh start 3                     # TEST harness only: 3-replica + auto-EC + chaos hooks
```

Extent refcount integrity (MERGE-REFS-LEAK class) is asserted by the in-process
chaos verify phase's STORAGE-ACCOUNTING invariants (see [Chaos suites](#chaos-suites))
— it reads the manager's etcd at a pinned revision and cross-checks every
extent's `refs` against live stream membership.

## Explicit split point — `autumn-op split --at`

`split PART_ID` with no extra flags lets the PS pick the median of the live keys.
To cut at an **operator-chosen** point — e.g. to pre-split an empty / near-empty
partition, or put two sub-scopes in different partitions — name the point on the
CLI. The user-facing form speaks a **scope + suffix**, never raw prefix bytes (the
partition layer stays namespace-agnostic; the CLI assembles the key and the wire
carries only raw bytes).

`--namespace` takes a SCOPE: a namespace (`kvc`) or an in-namespace sub-scope
(`kvc/acme`, `bench/perf`), each `/`-separated segment matching `[a-z0-9._-]+` —
the same convention as `autumn-client --namespace`. Cut key = `{scope}/` ++ suffix.

```bash
# Cut exactly at "kvc/acme/" — splits that sub-scope (and everything sorting
# >= it) off into a new partition. Empty/omitted suffix = the boundary itself.
$AO split PART_ID --namespace kvc/acme --at ""

# Cut at a text suffix -> key = "kvc/acme/" ++ "vllm/v1/80". Equivalent:
#   --namespace kvc --at acme/vllm/v1/80
$AO split PART_ID --namespace kvc/acme --at vllm/v1/80

# Binary suffix (e.g. an fs extent/inode prefix) via hex -> key = "fs/" ++ 0x0103ff.
$AO split PART_ID --namespace fs --at-hex 0103ff

# ADMIN escape hatch only: a whole raw key, no scope assembly. Operators should
# NOT hand-build prefixes. (hex below = "kvc/acme/".)
$AO split PART_ID --at-raw-hex 6b76632f61636d652f
```

Rules & behavior:
- The assembled key must land **strictly inside** the target partition's
  `[start, end)` (equal to `start`, equal to/`>=` `end`, or out of range are all
  rejected). The CLI does a friendly pre-check (readable error naming your
  scope/suffix); the **PS is the authoritative validator**.
- With an explicit `--at`, an **empty or near-empty** partition can be split
  (the `>= 2 keys` gate is skipped) — this is the presplit primitive: cut an
  empty range into two empty children. Without `--at`, an empty partition is
  still refused (`< 2 keys`).
- `--at`/`--at-hex` require `--namespace`; `--at-raw-hex` is mutually exclusive
  with all of them. A malformed scope (uppercase, empty segment, …) is refused
  before any RPC.

Manual verification (memory-mode loopback recipe, no etcd):
```bash
# 1. Bring up a 1-manager / 2-EN / 1-PS loopback cluster (see the dev recipe).
# 2. Create an EMPTY partition covering the keyspace, then split it at a
#    sub-scope boundary and confirm the region count goes 1 -> 2 with the new
#    boundary == the assembled key:
$AO --json info | jq '.partitions | length'          # -> 1
$AO split <PART> --namespace kvc/acme --at ""
$AO --json info | jq '.partitions | length'          # -> 2
$AO --json info | jq -r '.partitions[].range_start'  # one range starts at kvc/acme/
# 3. Negative: a point outside the range is rejected up front:
$AO split <PART> --namespace zzz/zzz --at "" ; echo "exit=$?"  # non-zero
```

## Namespace-aware presplit — `autumn-op presplit`

A raw-byte uniform split is **namespace-blind**: after key-namespacing every real key
sits in the `fs/…` / `kvc/…` / `mem/…` byte sliver,
so uniform splitting over the whole 0x00..0xff space collapses everything into
one or two partitions (live: 19 GB fs on a single partition, 30 empty). That is
why `bootstrap --presplit` was retired. `presplit` instead splits a `{scope}/`
keyspace along the namespace's **natural high-entropy dimension** (built on the
`split --at` primitive). `--namespace` takes a scope as in `split`: the FIRST
segment picks the rule (`fs` | `kvc` | `mem` | anything else = uniform hex
`--count N`), the whole scope is the cut prefix, and the declared points are
recorded on the first segment's namespace row. `fs` refuses a sub-scope (one tree).

```bash
# fs — split by INODE (the fs data key is [0x03][ino BE][off BE]). Give the exact
# inodes (each safetensors shard = one inode = one partition), or a --count.
$AO presplit --namespace fs --fs-inos 4,5,6,7,8
$AO presplit --namespace fs --count 8            # → inodes 1..7

# kvc — split by CONTENT HASH (sha256 hexdigest). --hash-prefix is REQUIRED: it is
# the RELATIVE prefix from the namespace root down to just before the hash hex, and
# it is per-MODEL, so there is no default. The vLLM connector stores
#   kvc/{model_scope}/vllm/v1/{hash}/{layer}
# → the hash is under `{model_scope}/vllm/v1/`, NOT directly under `vllm/`. Find the
# exact model scope from a live key: `autumn-client --namespace kvc ls`.
$AO presplit --namespace kvc --count 8 --hash-prefix "qwen3-8b_a1b2_0_1/vllm/v1/"
# same cuts:  --namespace kvc/qwen3-8b_a1b2_0_1 --count 8 --hash-prefix "vllm/v1/"
# sglang keys are {model_scope}/{pool}/{hash} → pass "<model_scope>/<pool>/".

# mem — split by AGENT.
$AO presplit --namespace mem --agents alice,bob,carol

# any other namespace (or sub-scope) — uniform hex split under the scope; this is
# what cluster.sh / perf_check.sh run for the bench keyspace.
$AO presplit --namespace bench/perf --count 8

# fs --lanes N [--parts P] — split fs for large-file striping. LANES is the key
# layout (24 by default, a permanent constant), PARTS is how many partitions to
# create (must divide lanes; omit = one per lane). See the "fs stripe geometry:
# lanes vs partitions" section above for the full model + the sacred-boundary
# merge guard. The boundaries are RECORDED (protected) as part of the presplit.
$AO presplit --namespace fs --lanes 24 --parts 6
```

### Stripe one large file across lanes (break the single-partition ceiling)

A single file = one inode = key-contiguous `[0x03][ino][off]` → ONE partition → ONE
log_stream. So a single file's write/read is capped by one stream's bandwidth
(measured ~220 MB/s single-connection, ~350 MB/s single-partition on fast NVMe;
disk/CPU are NOT the limit). To go faster, STRIPE the file across N lane partitions:

**Geometry is DECLARED, not auto-detected** (see the "fs
stripe geometry" section above for the full model). `presplit --lanes N` writes
the fs-wide `[0x04]stripe_geom`; every new file stamps that geometry into its own
`InodeMeta.stripe` at create (immutable), whether or not the partitions were cut
yet. There is no 64 MiB threshold and no per-upload flag — striping is on for the
whole fs once declared (default 24 lanes even with no presplit; declare `--lanes 1`
to turn it off).

```bash
# 1. Declare + cut on the EMPTY fs (before ingest). --parts spreads the lanes over
#    P partitions (must divide lanes). The boundaries are recorded so the merge
#    guard protects them.
$AO presplit --namespace fs --lanes 24 --parts 6
$AO info | grep part          # → 6 fs lane partitions, spread across PSs

# 2. Just upload. Every file's extents round-robin across the declared lanes:
#    extent e's offset o → lane (o/unit)%lanes, key [0x03][lane][ino][off];
#    autumnfs's concurrent batch_put fans them out → parallel across the lane PSs.
autumnfs --manager <mgr> put ./checkpoint.safetensors /ckpt
autumnfs --manager <mgr> get /ckpt ./out   # reader reads the file's own stamp
```

- The reader consults ONLY the file's stamped `InodeMeta.stripe` (never the current
  cluster shape), so old / non-striped files stay correct with **no migration**, and
  a later re-split at a lane boundary gives an existing file parallelism
  retroactively. `unit = MAX_EXTENT` (8 MiB) in v1 = each extent its own lane (max
  spread). MAX_EXTENT sweep (3-disk rig, EN-CPU-bound): striped-write peaks near
  4 MiB (~340 MB/s) and DECLINES for bigger extents (8→330, 16→298, 32→291); don't
  go above 8 MiB.
- **Scaling is bounded by whichever saturates first**: the lane PSs, the ENs' data
  plane, or the autumnfs client pipeline (window=8 + sync read barrier — a single
  file may not fully drive many lanes; running few parallel uploads or a deeper
  client pipeline closes the gap). Give ENs enough cores + put replicas on separate
  disks/hosts so the per-stream ceiling is high.
- **fuse mount**: READS and DELETES (unlink/rename-over) striped files correctly
  (an autumnfs-striped file is fully readable + removable via a mount on the same
  cluster). fuse WRITE/TRUNCATE of a striped file is refused fail-loud for now
  (streaming writes don't know the final size up front, so fuse can't decide the
  stripe geometry at create) — use `autumnfs put` to (re)write large striped files.
  Schema is **v3** (`InodeMeta.stripe`) — a stop-world reset from v2 (no in-place
  migration).

**CRITICAL — presplit the EMPTY keyspace BEFORE loading data.** A data-bearing
partition can't be split repeatedly: after the first CoW split, parent+child
share extents and the child's SSTs hold out-of-range keys, so the PS rejects the
next split with `precondition failed: cannot split: partition has overlapping
keys` until a major compaction clears them. `presplit` applies what it can and
prints the skipped points + this hint; the PS auto-major-compacts, so re-running
after a bit converges — but the correct order is **presplit first, then ingest.**
(Another known post-heavy-split hygiene item: manager `part_addrs` can go stale
after lots of split/merge — `merge X Y` says `partition X not served by this
P-log`; delete all PS pods in parallel to rebuild.)

## Chaos suites

### Page cache: no stale reads across mounts (`fuse_page_cache.sh`)

Two mounts of one cluster, A reads and B writes, every rewrite the same size (so
only the content generation tells old from new):

```bash
AUTUMN_DATA_ROOT=/data05/autumn-pc ./scripts/fuse_page_cache.sh
```

Pass = every line `ok`, then `PASS`: SHARED (a `MAP_SHARED` mmap works), KEEP
(pages read through A are still resident after A reopens), REOPEN (B rewrites
while A has the file closed — no lease, no invalidation — and A's next open reads
the new bytes), HELD (A holds an fd; B rewrites; A's same fd sees the new bytes
within 5 s), TAIL (A holds an fd read to EOF; B appends; A's same fd reads past
the old EOF within 5 s — the kernel learns the size only from GETATTR), LOCAL (two fds and a mapping on one mount agree), MMAPW (bytes stored
through A's writable shared mapping are what B reads), PREFETCH (A reads 16 MiB
of a 64 MiB file in sequence so the daemon prefetches ahead; B rewrites it; A's
same fd reads the new bytes past 16 MiB within 3 s). To see it discriminate,
point `FUSE_BIN` at a build whose `open_keeps_page_cache` ignores the generation
(`lease_was_held || recorded.is_some()`): REOPEN and MMAPW fail with old bytes;
on one whose `meta::get_inode` ignores `meta_invalidated`, TAIL stays at the old EOF;
on one whose `PrefetchCache` ignores the generation in both `admit` and `lookup`,
PREFETCH reads the old bytes (either check alone still passes: the other one
drops the old blocks first).
The script stops this tree's cluster (`cluster.sh stop` kills every
`target/release` cluster process), so run nothing else from the tree meanwhile.

### Readahead under latency (safetensors load)

Emulate a network path without touching the host's `lo`: run the cluster in a
netns and delay only its veth.

```bash
ip netns add autumnra
ip link add veth-ra0 type veth peer name veth-ra1
ip link set veth-ra1 netns autumnra
ip addr add 10.231.0.1/24 dev veth-ra0 && ip link set veth-ra0 up
ip netns exec autumnra sh -c 'ip addr add 10.231.0.2/24 dev veth-ra1; ip link set veth-ra1 up; ip link set lo up'
AUTUMN_BIND_HOST=10.231.0.2 AUTUMN_EXTENT_BASE_PORT=20000 ip netns exec autumnra bash cluster.sh start 3
./target/release/autumn-fuse --manager 10.231.0.2:9001 --mountpoint /mnt/autumn-ra --transport tcp &
tc qdisc add dev veth-ra0 root netem delay 1ms limit 100000
ip netns exec autumnra tc qdisc add dev veth-ra1 root netem delay 1ms limit 100000
dd if=/mnt/autumn-ra/<file> of=/dev/null bs=64K count=200 iflag=direct   # ~4 ms per read
```

Then, per window: `echo <kb> > /sys/class/bdi/<dev>/read_ahead_kb`, evict the
file (`os.posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED)`), and time
`safetensors.torch.load_file` plus a copy of every tensor (`load_file` alone is
lazy — it maps, and reads nothing until a tensor is touched). Run it both with
the loader pinned to one core (`taskset -c N`) and on several: torch copies with
several threads, and the two want different windows. Expect at 1 ms one-way,
one core: 128 KiB ≈ 108 MiB/s, 2 MiB ≈ 720-740; nine cores: 2 MiB ≈ 1100-1330,
4 MiB collapses to ~150 (the daemon then sees ~10x more, ~36 KiB READs); a second
load without evicting reads nothing through the daemon. Remove with `tc qdisc del`,
`ip netns del autumnra`.

### Kernel cache invalidation must not wedge a mount (`fuse_inval_deadlock.sh`)

A mount drops the kernel's page cache for a file when another client's writer
closes it. The kernel serves that by locking every cached page of the file, and a
page under readahead stays locked until the mount answers its read — so the
invalidation must never be sent from the thread that answers reads. The script
makes that race as likely as it gets: one mount re-faults a private mmap of a
64 MiB file in a loop, dropping the cache between passes so each pass goes
through readahead, while a second mount opens the
same file for write and closes it as fast as it can, one WriterClosed event per
close.

```bash
# ~3 min per run. Mounts /mnt/autumn-fuse-inval-r and /mnt/autumn-fuse-inval-w.
AUTUMN_DATA_ROOT=/data05/autumn-inval ./scripts/fuse_inval_deadlock.sh
# Reads prepared AND executed on the dispatcher (the shape most likely to wedge):
READ_IO_THREADS=0 AUTUMN_DATA_ROOT=/data05/autumn-inval ./scripts/fuse_inval_deadlock.sh
```

Pass = first a probe: one writer close must take the file's page cache on the
reader mount from ~100% resident to ≤10% within 5 s (mincore; fuser reports a notify for an
inode the kernel does not know as success, so only the page cache can show that
notifies land). Then the race: the reader's pass counter never stalls for 15 s,
every pass hashes to the seeded file, the writer keeps closing, the reader mount
logs at least 1000 invalidation events, and no kernel notify fails. A/B an older
build with `FUSE_BIN=<path>`. Healthy shape over 60 s: ~170 passes, ~150 k events. A wedge
prints the reader mount's threads: `autumn-fuse-com  D  folio_wait_bit_common`
plus the reader in D with `filemap_fault` in its stack is this deadlock.

The script then sends SIGTERM to the reader daemon `TERM_ROUNDS` times (default
8, each on a fresh reader mount) while the race still runs: every daemon must be
fully gone within `TERM_SECS` (30), its mount and FUSE connection with it. The
daemon drains the notify in flight before exiting; a build without that (A/B via
`FUSE_BIN`) left a zombie in 4 of 10 rounds of the same load, and failed this
script in round 1 or 2 — one round is not enough to tell the two apart. Each
round first waits for the fresh mount to log 100 invalidation events. Log lines
of a clean shutdown: `shutting down: draining kernel invalidations` →
`flushing dirty inodes` → `unmounting`. A SIGTERM during startup (connecting,
waiting for the cluster, up to ~60 s) exits only once startup finishes; a
second SIGTERM does nothing — escalate with SIGKILL, which is safe then because
no invalidation runs before startup completes.

SIGKILL skips that drain. A daemon SIGKILLed while a notify waits on a readahead
page stays a zombie (a thread in D at `folio_wait_bit_common`, main thread `Z`)
and its mount keeps no server behind it — so stop mounts with SIGTERM (what
Kubernetes sends first), and keep the pod's grace period long enough for the
dirty-inode flush. The script tears down by aborting the connection first. To
clear a zombie by hand:

```bash
mountpoint -q /sys/fs/fuse/connections || mount -t fusectl none /sys/fs/fuse/connections
# The minor from mountinfo — NOT `mountpoint -d`/stat, which blocks on a wedged mount.
# For a detached mount, match the connection dir's ctime to the daemon's start time.
awk -v m=<mountpoint> '$5 == m { print $3 }' /proc/self/mountinfo   # -> 0:<minor>
echo 1 > /sys/fs/fuse/connections/<minor>/abort   # the zombie exits at once
```

### Dead peer behind a healthy connection (`fuse_dead_peer_chaos.sh`)

A connection whose peer stopped answering while TCP still reports it ESTABLISHED
must not wedge fuse reads. The client closes a connection that has shown no sign
of life for 8–10 s (TCP; ~10–12 s on UCX) — no byte back, and on TCP no new ACKs
while more of its bytes wait unsent behind them; it pings after 2 s of silence —
and logs
`rpc peer stopped answering … addr=<peer>`; that line should name only peers you
actually faulted.

```bash
# TCP: both scenarios on one cluster (~6 min). Needs python3; mounts /mnt/autumn-fuse-dpd.
AUTUMN_DATA_ROOT=/data05/autumn-dpd ./scripts/fuse_dead_peer_chaos.sh
# Every read pass reads all files concurrently, so the fault is detected through
# live traffic, not in a quiet gap.
#   mgr-freeze: the mount reaches the manager through scripts/freeze_proxy.py; its
#               open flows are frozen (sockets kept open, nothing forwarded) and every
#               partition is split, so reads need a region refresh over a dead flow.
#   en-stop:    SIGSTOP one extent node for 90 s.
# UCX (en-stop only — the relay cannot carry UCX); build with the ucx feature first:
cargo build --release -p autumn-server -p autumn-fuse --bins --features autumn-server/ucx,autumn-fuse/ucx
AUTUMN_BIND_HOST='[<RoCE IP>]' AUTUMN_TRANSPORT=ucx ./scripts/fuse_dead_peer_chaos.sh
```

Pass = every read after the 30 s grace window succeeds sha-exact, and every file
reads back sha-exact once the fault is lifted. Expected shape: mgr-freeze worst
read ~10 s, then sub-second; en-stop worst read ~18 s. A mgr-freeze run where every
read is `ERR` after 30 s is the wedge this guards against. The script kills only
what it started.

```bash
# PS-failover chaos (2 PSes, kill one -> partitions must migrate, zero loss):
cargo test -p autumn-manager --test system_ps_failover_chaos
# system_chaos itself: in-process manager, real subprocess ENs + etcd +
# toxiproxy, and the PS as a real `autumn-ps` child (log: <log dir>/ps-91.log).
# Two nemesis actions restart it: `psterm` (SIGTERM -> drain -> exit -> start
# -> wait until every partition is open; a drain over 150 s fails the round)
# and `pskill` (SIGKILL -> start -> wait). Before verify it is crash-restarted
# once more, so every partition reopens from what is durable after the round;
# one that is not open within 120 s fails the round. After every nemesis step
# and before verify, a checkpoint check reads each partition's checkpoints the
# way recovery does (last record of every meta-stream extent) and fails the
# round if one lists an SST in an extent the row stream no longer has (the
# production loss: 28 of 37 SSTs in truncated extents). The summary line
# "PS restarts completed ... row streams reached N extent(s)" says how much of
# that the round exercised; N = 1 means no cut could have been tested.
# After a `psterm` whose drain flushed everything (no drain warning in the PS
# log), every partition the new process opens must replay <= 1 MiB of WAL
# ("log replay done ... bytes=N"); more means recovery started from a cursor
# older than the drain's checkpoint (the 190 s production reopen). Summary:
# "replay after clean graceful stops: checked N restart(s), most any partition
# replayed B bytes". Measured: HEAD 0 bytes over 12 restarts; the PS before
# 0a7e85d replayed up to 69 MB on the same seed, growing with every restart.
# Before every restart and after the final one, once the PS is ready (every
# partition open at its current epoch, so a merge survivor has reopened), each
# meta stream must hold ONE checkpoint record: a merge splices in one per
# source and the survivor's open replaces them. Two left behind mean every
# later open replays the victim's WAL (926 KB measured before the fix).
# While the writers run their flushes merge the records within seconds, so a
# round with `merge` enabled also merges once after the writers stop (splitting
# first if one partition is left, compacting until the merge is accepted): no
# flush follows it, and the check after the final crash restart sees what the
# survivor's open alone left. A PS with that step disabled fails there on every
# seed tried ("part N is open with 2 checkpoint records").
# Deterministic form: cargo test -p autumn-manager --test system_merge_single_checkpoint
# Besides checking that the open publishes one checkpoint and the next restart
# replays nothing, this snapshots replay_read_bytes immediately before the merge:
# the merge reopen must read <64 KiB, not the victim's ~2 MiB of sealed,
# checkpoint-covered prefix extents. The test explicitly rolls each source WAL
# twice and asserts that both contain at least three extents before merge. The
# merge freeze drain writes each source's checkpoint at its committed log end,
# and the merged open starts replay at the latest of those cursors, so on a live
# merge expect `recover_partition: replay plan built` with `n_meta_records=2`
# and the following `log replay done ... bytes=N` near zero. A source cursor
# no longer in the log (its empty tail was reclaimed) is ignored; replay then
# reads from an earlier cursor (or the whole log) and skips everything already
# flushed — slower, not wrong. That skip is by the union of both sources' max
# seq, sound only because the drain left nothing unflushed; cargo test -p
# autumn-manager --test system_merge_replay_reachability drives both edges: a
# drain flush whose checkpoint fails (the merge must be refused, the survivor
# must take writes at once, an immediate retry must flush before merging, and
# nothing acked is lost) and a merged open with both
# source cursors reclaimed (whole-log replay, every acked write reads back).
# A compaction after the merge must not move the cursor back: the merged
# record's cursor is the log tail, past every SST header, and GC's floor sits
# there. Manual check: after `autumn-op merge` and `autumn-op compact <PART>`,
# the PS log's `ckpt_trace` "checkpoint published" lines for that meta stream
# show a vp_extent_id/vp_offset that never goes back, and the next restart's
# `log replay done ... bytes=N` stays near zero. Deterministic form: cargo test
# -p autumn-manager --test system_merge_single_checkpoint
# a_compaction_after_a_merge_keeps_the_checkpoint_cursor (and --test
# system_compact_ckpt_cursor for a flush that rolls the log mid-compaction).
# Every merge goes through MSG_MERGE_PARTITIONS (`autumn-op merge`, the
# policy); the raw merge opcode 0x34 is retired and refused.
# Split does the same: its drain always writes a log-end checkpoint, so a child
# replays only its own writes; cargo test -p autumn-manager --test
# system_split_fence_floor checks that a WAL-only lease fence bump survives it.
# Two more actions shape the row stream: `rollrow` seals and rolls every
# partition's row tail (the fence-drain path), `flushburst` flushes every
# partition 8 times so the PS's own size-tiered trim (past 32 SSTs) runs.
# Actions may repeat in AUTUMN_CHAOS_ACTIONS to weight them. "PS drain
# warnings" counts partitions a graceful stop left unflushed (one PS log line
# per partition; the WAL replays them; counted, not failed).
# Run an older PS against today's checks with AUTUMN_CHAOS_PS_BIN=<path>.
# The PS runs with --flush-mem-bytes 256 KiB (AUTUMN_CHAOS_PS_FLUSH_BYTES; 0 =
# the PS default, 256 MiB). Every compaction size scales from it, so small
# chaos values reach the shapes production reaches with 256 MiB memtables.
# Before the workload a bulk phase (AUTUMN_CHAOS_BULK=1, the default) writes
# 6000 cold keys `c000000..c005999` in 6 bursts of about one memtable, flushes
# each burst, and rolls the row tail every 2: large SSTs spread over several
# row extents, which size-tiered compaction then skips while the workload's
# small flushes pile up. That is the production shape of the truncation bug
# fixed in 8a4b12a. Measured with the mix below, seeds 1-3: the parent of
# 8a4b12a gets a CHECKPOINT VIOLATION on all three (on seed 3 a partition then
# stops reopening); HEAD gets 0 violations, 0 bytes replayed, and no lost key.
# A final crash restart that leaves a partition closed fails the round at once
# (verify cannot read it).
cargo build --workspace --bins
AUTUMN_CHAOS_SEED=1 AUTUMN_CHAOS_DURATION_SECS=180 AUTUMN_CHAOS_NEMESIS_INTERVAL_MS=1000 \
  AUTUMN_CHAOS_ACTIONS=rollrow,flush,flushburst,rollrow,flush,flushburst,rollrow,flushburst,compact,split,merge,pskill,psterm \
  cargo test -p autumn-manager --test system_chaos chaos_real -- --ignored --nocapture
# vp_head multi-seed chaos: several seeds through the
# same system_chaos harness,
# nemesis focused on split/merge/compact/FORCEGC (+ gc/flush/EN-kill). forcegc
# bypasses the discard-ratio gate to punch specific sealed extents -> the maximal
# stress on the PS replay-floor guard; a wrong vp_head would let it punch a live
# extent = loss. Every acked put verified byte-exact per seed; PLUS a
# positive-reclaim check (verify_gc_reclaim): a final quiesce -> compact ->
# force-GC MUST physically DELETE extents (else the floor is stuck), and the
# punch pass is re-verified loss-free + leak-free.
# The physical half has two windows: a copy on a node that was a MEMBER before
# the delete must be unlinked within 30 s (the delete reaches it); a copy on a
# FORMER member (a node recovery replaced, typically after killfence) is reached
# only by that node's orphan reconcile, so it gets 30 s + one reconcile interval
# (5 min). A failure names each file as `on members` or `on former members`.
# Members are taken from the snapshot before force-GC; a recovery that applies
# after it is mis-booked as a member and fails at 30 s (look for `recovery
# applied ... extent_id=N` after the snapshot; not seen in seeds 23/7/101). A
# former-member failure at ~330 s can also be one failed reconcile round: grep
# that EN's log for `reconcile failed (will retry next sweep)`.
./scripts/vphead_chaos.sh                              # 6 default seeds
VPHEAD_SEEDS="1 42 777" AUTUMN_CHAOS_DURATION_SECS=60 ./scripts/vphead_chaos.sh
#   (system_chaos's own action name for force GC is `forcegc`; AUTUMN_CHAOS_ACTIONS
#    to bisect, e.g. AUTUMN_CHAOS_ACTIONS=split,forcegc)
# Full-set + node DECOMMISSION chaos: same system_chaos
# harness but with the FULL nemesis set — including the ones vphead omits:
# fence (MSG_FENCE_NODE/clear), killfence (kill-then-fence), ec (convert-under-
# load), partition + latency (toxiproxy net faults). THEN a terminal one-shot:
# after the nemesis loop stops and the cluster heals, one EN is permanently
# removed the HDFS way (fence -> drain + fenced-slot recovery relocate
# every extent off it -> MSG_REMOVE_NODE refuses until fully drained, tombstones
# the address), and the per-key/range/accounting verify proves NO loss with the
# node gone. Removal is a TERMINAL one-shot, NOT a per-cycle nemesis action
# (non-reversible: a permanent node loss injected every cycle would starve the
# cluster below quorum). Needs 6 ENs (removes 1, must leave >= K+M):
./scripts/decommission_chaos.sh                        # full set + remove, 3 seeds
AUTUMN_CHAOS_DECOMMISSION=0 ./scripts/decommission_chaos.sh   # full set, no remove


#   (any run of the base test can add the terminal remove with
#    AUTUMN_CHAOS_DECOMMISSION=1 AUTUMN_CHAOS_NUM_ENS=6)
# Transport-layer chaos (real cluster.sh cluster; E1 EN kill+respawn, E2 PS
# kill -> migrate, E3 PS respawn, E4 manager kill+respawn, E5 PS +
# manager double-kill inside the eviction window -> the interrupted eviction
# must converge and partitions FAIL BACK to the survivor; every ACKed
# write verified afterwards):
AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/transport_chaos.sh tcp
AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/transport_chaos.sh ucx   # needs --features autumn-server/ucx binaries
# (ucx note: a node killed -9 leaves its port in TIME_WAIT ~60s; the UCX
#  listener now retries bind through that window instead of exiting.)
# E6: CHAOS_ROUNDS=N CHAOS_SEED=S randomized repeated kill rounds.
# E7: split + mid-flight PS kill; merge + mid-freeze manager kill.
# Kvcache-interface chaos: python L3 backend under PS/manager kill
#   (NOTE: rebuild the wheel after ANY rust wire change — maturin build
#    --release + pip reinstall; a stale wheel mis-encodes requests):
#   ./scripts/kvcache_chaos.sh
# Fuse-interface chaos: file workload through the mount under
#   PS-kill / manager-kill / fuse-kill+remount + T1 truncate-shrink crash:
#   ./scripts/fuse_chaos.sh
# Fuse RMW corruption guard (RMW-GET-SWALLOW, 2026-06-23): partial in-place
#   overwrite during a PS kill+restart; a swallowed RMW read-error would zero
#   the untouched bytes of a file on a *successful* write. Single-PS (no
#   migration) so the verify read can't wedge:
#   AUTUMN_DATA_ROOT=/data05/autumn-rmw ./scripts/fuse_rmw_chaos.sh
# Fuse EN (data-plane) restart integrity (2026-06-23): kill+restart each EN;
#   durable + RMW files stay byte-exact across replica failover + rejoin.
#   (EN restart is CORRECT — no data loss. WRITES stall during EN-down ONLY at
#    exactly RF=3 ENs = capacity exhaustion, NOT a failover-latency bug: with
#    >RF ENs writes never stall; reads tolerate a down replica at any size.
#    See fuse CLAUDE.md "Restart behaviour".):
#   AUTUMN_DATA_ROOT=/data05/autumn-eni ./scripts/fuse_en_restart_chaos.sh
# Cross-host chaos (real network ::14+::15, remote via ssh):
#   ./scripts/crosshost_chaos.sh tcp | ucx
# Multi-manager HA chaos: leader kill -> standby takeover, PS kill under
# the new leader, old leader rejoins as follower; zero ACKed-write loss:
AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/manager_ha_chaos.sh tcp
AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/manager_ha_chaos.sh ucx
# (Notes: manager restart used to black-hole client routing — part_addrs
#  is in-memory; the PS now re-reports it every ~2s sync tick. Ownership
#  failback used to wedge forever — owner_epoch now bumps on every acquire.)
#
# In-process kill+split+merge+EC+fence chaos (manager + PS in the test process,
# EN as subprocesses spawned from target/debug — `cargo build --workspace` first).
# The test provisions its OWN throwaway etcd on random loopback ports (it does
# NOT use 127.0.0.1:2379 and cannot touch another cluster's etcd); it needs the
# `etcd`, `toxiproxy-server` and `toxiproxy-cli` binaries in PATH (overrides:
# AUTUMN_TEST_ETCD_BIN / AUTUMN_TEST_TOXIPROXY_SERVER / _CLI). The
# zero-data-loss invariant test — finds GC/seal/split data-loss + write-wedge:
AUTUMN_CHAOS_SEED=583 AUTUMN_CHAOS_DURATION_SECS=45 AUTUMN_CHAOS_NEMESIS_INTERVAL_MS=1500 \
  cargo test -p autumn-manager --test system_chaos \
  chaos_real_kill_split_merge_ec_fence_no_data_loss -- --nocapture --ignored
#   knobs: AUTUMN_CHAOS_SEED, _DURATION_SECS (default: two passes over the
#          actions, at least 30), _NEMESIS_INTERVAL_MS (3000), _NUM_ENS,
#          _DISKS_PER_EN (2), _EC_K/_EC_M, _ACTIONS (split,merge,ec,fence,flush,
#          compact,gc,forcegc,kill,killfence,partition,latency,corrupt,psterm,
#          pskill,rollrow,flushburst,diskfault).
#   COVERAGE GATE: every enabled action must run OK at least once. Past the
#   duration the round runs on, up to six times it, until each has run OK or
#   been tried four more times; one that never ran OK fails the round ("COVERAGE:
#   ..."), and so does a writer, reader or nemesis task that panicked. CI runs one full round per push.
#   HISTORY: three writers (two of them on the same keys) issue puts, deletes,
#   puts that expire in 2-5 s and puts that outlive the round; every write is
#   recorded with when it was invoked and acknowledged. A read must return a
#   value some write may have left: the newest acknowledged one, one acked
#   concurrently with it, or one whose outcome the client never learned
#   (timeout, lost connection). An unknown outcome never erases an acknowledged
#   write. The range check also refuses a key that must be deleted or expired.
#   MULTI-DISK: every EN is formatted with _DISKS_PER_EN directories (one
#   tempdir per node, one subdir per disk) and started with a comma-separated
#   --data list. `diskfault` (one-shot) breaks the disk holding most witness
#   copies (below) on the last eligible two-disk node: each of its hash
#   directories is replaced by a regular file (root ignores chmod), then one
#   witness extent there is fenced on that EN directly — the fence persists its
#   `.meta`, the write fails, and the node reports the disk faulted (waiting for
#   an allocation to land there is luck: the EN steers new extents to its other
#   disk). At the end the node must still be a
#   member and unfenced, every sealed extent that had a copy there must have
#   been rebuilt elsewhere (within 120 s), and no copy on its other disks may
#   have moved. 16 sealed extents of a stream no partition owns (the witness
#   stream) are written at the start, so the check always has extents GC and
#   compaction cannot delete, and fencing one locks out no writer;
#   a round in which every one was deleted fails rather than passing unchecked.
#   No other nemesis touches that node afterwards. `Full` is still reached only
#   by enospc_chaos.sh. Set _DISKS_PER_EN=1 to A/B against one disk.
#   verdict-gate: a real bug = `mismatches>0` OR a not_found that REPRODUCES on
#   DRAINED ports. A burst of not_found with `mismatches=0` after back-to-back
#   runs is almost always loopback PORT EXHAUSTION (cumulative TIME-WAIT) — a
#   wedged partition with no part_addr — NOT data loss. DRAIN-GATE before each
#   run: wait until `ss -tan | grep -c TIME-WAIT` < 4000 (see memory
#   project_chaos_long_soak_port_exhaustion). seed=583 = the GC stale-cache
#   big-value-loss regression guard (BUG-GC-STALE-CACHE); seed=603 (under
#   AUTUMN_CHAOS_NEMESIS_INTERVAL_MS=1500, 45s) = the seal-and-roll non-
#   idempotent-retry split-child-open wedge guard (BUG-IDEMPOTENT-ROLL).
#   STORAGE-ACCOUNTING invariants (beyond user data): the verify phase also
#   reads the manager's etcd (extents//streams/) at a single pinned revision and
#   asserts, for every extent, `refs == #streams listing it` + `vp_table_refs==0`
#   + no dangling membership — catching the extent-10 orphan / CoW double-free /
#   GC-leak classes that the per-key/range checkers can't see. Pure-logic unit
#   tests run in plain `cargo test` (no cluster): `... --test system_chaos
#   accounting_checker_tests`.
# extent delete carries a target identity
# What to expect: an EN that refuses a delete logs
#   `delete_extent addressed to a DIFFERENT node — refusing` at WARN, with
#   `for_node` / `this_node` uuids. Seeing this means a manager is retrying a
#   delete against an address now owned by a different node — almost always a
#   torn-down cluster whose persisted retries are still running against a host
#   that a NEW cluster reuses. Nothing was deleted; the correct action is to
#   stop the old manager, not to clear the warning.
# Manual check: start a node with `--advertise` (so it has a uuid), allocate an
#   extent, then send a delete naming a different uuid — the file must survive
#   and the WARN must appear; naming the node's own uuid must unlink it.
#   Unit-level equivalent:
#   `cargo test -p autumn-stream --lib delete_extent_refuses`.
# EC staging seal is durable (.meta payload_location)
# What to expect: after the manager flips an extent's layout to a shard file,
#   the owning EN persists that in `.meta` byte 41 and refuses any further
#   WriteShard for it — `write_shard from a SUPERSEDED conversion attempt` at
#   WARN, or a bare refusal once sealed. On boot the EN logs
#   `EC staging sealed on load ...` with the count it re-derived. A count of 0
#   on a node that holds shard files means the flip never reached its `.meta`
#   (check for the quarantine warning next to it) — the seal then holds only in
#   memory until the next reconcile round re-persists it.
# Manual check: convert an extent, confirm `.meta` byte 41 == 1 on a target
#   (`xxd -s 41 -l 1 <disk>/<hash>/extent-<id>.meta`), restart that EN, and
#   confirm the boot log reports it sealed. Unit-level equivalent:
#   `cargo test -p autumn-stream --lib the_ec_staging_seal_survives_a_restart`.
# A given-up conversion's staging is reclaimed by the reconcile sweep
# What to expect: when a conversion fails or is abandoned the layout still says
#   `.dat`, so the shards its participants already staged are named by nothing.
#   The next reconcile round on each holder (immediately at boot, then every
#   5 min) unlinks them — EN log `reconcile: dropped a shard file this node
#   should not hold` with `extent_id` / `shard_index`. `df` stops counting the
#   bytes in the same step. The `.dat` is untouched and the extent stays
#   readable; a later conversion attempt simply stages afresh. (Before this,
#   the residue was reclaimed only by RESTARTING the EN — the marker that held
#   cleanup off is in memory — so a long-lived node kept it for months.)
#   Staging under a marker the manager still holds is not touched: it gives no
#   verdict at all while an op is in flight, and a node skips a verdict asked
#   for before its staging arrived (`reconcile: skipping cleanup — this verdict
#   says .dat while an attempt has staged shards here` at DEBUG). So residue
#   that survives one round is normal only if a conversion is running;
#   `extent-<id>.shard<N>` files still present on a quiet extent after two
#   rounds mean the node is not reconciling — check that it registered
#   (`reconcile: cannot identify the reporting node` on the manager).
#   ONE EXPECTED ERROR: a cancel can land while the cancelled attempt is still
#   streaming, and a reclaim then removes its staging. Its next stripe logs
#   `write_shard <id>/<n>: staging file vanished before stripe @<off> — this
#   attempt's staging was clobbered; refusing to recreate it with holes` at
#   ERROR. Beside an `EC conversion attempt cancelled` line for the same extent
#   that is BENIGN and is the system working: the refusal is what stops a
#   zero-holed shard being built, and a cancelled attempt could not have
#   committed anyway. The same ERROR with NO cancel for that extent is a real
#   problem — something deleted staging out from under a live conversion.
#   A parity target that staged but never became a member gets no placement at
#   all (placements are member-only), so its residue is collected by the
#   non-member garbage leg after its three-round grace (~15 min), not the next
#   round.
# Manual check: `find <disk> -name 'extent-<id>.shard*'` on a participant after
#   a conversion you let fail (e.g. stop one target until the coordinator gives
#   up), then again after the node's next sweep — the file must be gone and
#   `autumn-client` must still read the extent. Unit-level equivalent:
#   `cargo test -p autumn-stream --test placement_cleanup`.
# op observability: live progress + durable history
# Two questions, two sources. `autumn-op ops list --active` reads the LEADER's
#   in-memory ledger — the only place a running op's progress exists, and it
#   dies with the leader. `autumn-op ops history` reads the etcd-backed log —
#   the only place a terminal op's FAILURE REASON survives.
# A memory-only manager (no --etcd) persists NO history: `ops history` now fails
#   loudly with "no durable store" rather than printing an empty list, because
#   an empty list there reads as "nothing failed".
# The dashboard shows both at GET /api/ops; it asks the leader through
#   autumn-op, never a file.
# Which kinds report progress: gc + forcegc (extent bytes scanned), compact
#   (SST data blocks merged) — sampled by the PS onto its load heartbeat; and
#   ec-convert (shard bytes encoded) + recovery (bytes copied) — sampled by the
#   EXTENT NODE onto `df`, keyed by extent_id since the node never learns the
#   op id. split/merge/rebalance are single-step and carry NO progress by
#   design; their result is in the leader log ("op succeeded" / "op FAILED"),
#   the audit trail and `ops history`.
# Manual check: `$AO ops list --active` during a large compact/gc, or during an
#   `$AO force-ec-convert` / a node rebuild, must show a percentage AND the
#   magnitude IN THE UNIT THAT KIND MEASURES — `11.1 GiB / 16.0 GiB` for
#   gc/forcegc/ec-convert/recovery, a plain count of SST data blocks for
#   compact, of phases for split/merge. An eleven-digit byte count
#   (`11895046144 / 17179981824`) is the bug this check exists to catch: the
#   wire carries raw counts on purpose and the RENDERER owes them a unit.
#   After it finishes, `$AO ops history --limit 5` must carry its outcome, with
#   the error text in full for a failure. A finished op must stop reporting a
#   percentage — a repair frozen at a stale 75% is worse than none.
#   Automated equivalent (isolated cluster + etcd, asserts the endpoint shape):
#   `bash crates/server/src/bin/autumn_dashboard/tests/api_contract.sh`.
#   Dashboard: the panel shows the same numbers — verified live through
#   GET /api/ops during a conversion (18.6% → 37.2% → 55.8% → 74.4%, then
#   `succeeded 100%` in history). Without a cluster:
#   `node crates/server/src/bin/autumn_dashboard/tests/render_check.js` (panel functions lifted out
#   of the page) and `node crates/server/src/bin/autumn_dashboard/tests/tabs_smoke.js` (every tab
#   rendered under a DOM stub).
#   LIVE EC-conversion progress: `bash scripts/ec_convert_progress.sh` — spins a
#   4-EN cluster (EC 3+1 needs four targets), rolls a 1 GiB log extent, converts
#   it and polls once a second. Measured 2026-08-28 on loopback: samples appear
#   ~5 s in (the marker is acquired before encoding starts, so `--` first),
#   advance one 64 MiB stripe at a time, and land on 100% at SUCCEEDED. The
#   denominator is THIS node's shard, ceil(extent / K), not the whole extent.
#   EC REBUILD progress (a recovery whose extent is ec_converted; the kind is
#   still `recovery` — it is one RecoveryTask, so it lands on the same ledger
#   entry a replica rebuild would): the EN samples once per 64 MiB stripe,
#   AFTER the stripe is decoded and written — `done` = bytes of the rebuilt
#   shard on disk, `total` = the exact shard length. A stripe needs K peers
#   before it can be decoded, so nothing moves while peers are being read;
#   with a 1 GiB shard expect ~16 steps, each landing on a 64 MiB boundary.
#   Reading it in `$AO ops list --active`:
#     `recovery running` with no ratio      an attempt just started: the slot
#                                           is `0/0` until the EN has resolved
#                                           the extent (manager round-trip,
#                                           then the recovery permit — with
#                                           the EN's `--recovery-parallelism`
#                                           at its default of 2 a queued
#                                           rebuild can sit here behind two
#                                           others; that is a queue, not a
#                                           stall);
#     `0 / <shard bytes>`                   the walk began and the first
#                                           stripe's K peers have not all
#                                           answered yet;
#     a ratio on a 64 MiB boundary that     the STALL shape. One of the K
#     stops moving while the op stays       peers this stripe needs is not
#     RUNNING                               answering (or answers short — the
#                                           EN refuses a short stripe rather
#                                           than decode it). The EN's own
#                                           failure text names every peer it
#                                           tried and why, and it arrives on
#                                           the next df heartbeat — the entry
#                                           carries it as `ERROR[4]: …` while
#                                           still RUNNING. `kubectl logs <en>`
#                                           is now only for the HISTORY (the
#                                           ledger keeps the last reason, not
#                                           every attempt's).
#                                           A 4-hour zero-byte rebuild now
#                                           shows as `0 / N` for four hours,
#                                           not as a bare `running`;
#     a ratio that drops back to 0          the attempt failed and the EN's
#                                           retry loop (10 tries, 10 s apart)
#                                           started the next one — the
#                                           partial shard was unlinked, so the
#                                           bytes really are gone. The EN
#                                           retracts the ratio the moment the
#                                           attempt fails, so it is `0/0`
#                                           between attempts, then `0 / N`
#                                           once the new attempt has resolved
#                                           the extent. Ten of these and the
#                                           EN gives up: the slot empties, the
#                                           manager's marker survives and it
#                                           re-dispatches — the ledger entry
#                                           stays RUNNING and starts over.
#   Not observable here: whether the K peers are SLOW rather than dead — a
#   stripe that takes ten minutes and one that never completes look the same
#   until the next boundary lands. Two samples a stripe apart in time tell
#   them apart; one does not.
# WHY the reason is on the heartbeat and not only in the retry response.
#   A node's failure used to reach the manager only in its answer to the NEXT
#   dispatch, and re-dispatch runs on exponential backoff — so on a repair that
#   had been failing for a while, the reason could be minutes or hours stale,
#   and "failing" and "merely slow" read identically until the retry came
#   round. `DfResp.op_failures` carries it every 2 s instead. It can only
#   UPDATE an entry the ledger already has RUNNING: a report for an op the
#   manager is not tracking is describing something already closed, and an
#   entry conjured from one would have no marker behind it and nothing to ever
#   close it.
# AFTER A MANAGER RESTART the age is real, not reset. Replayed entries take
#   their `started_at` from the etcd marker, which records when the work began
#   and outlives the leader that started it. What does NOT survive is the
#   history: the ledger holds the last reason, and the manager's own
#   dispatch-failure counters start from zero (they live in the rate limiter's
#   in-memory backoff table, so the backoff restarts too — a badly-behaved
#   extent gets hit hard once more before it backs off again).
# chaos: pacing between runs is MANDATORY
# One `system_chaos` run burns ~50k loopback ephemeral ports, and TIME-WAIT
#   decays over ~60 s each — so back-to-back runs hit EADDRNOTAVAIL mid-run and
#   the verify reads that as a wedged partition: `not_found` on nearly every
#   key with `mismatches=0`. That shape is port exhaustion, NOT data loss (real
#   loss shows mismatches, or a SUBSET of not_founds). Gate each run on
#   `ss -tan | grep -c TIME-WAIT` < ~2000 and WAIT for the drain, don't skip.
# chaos: the EcConvert nemesis needs something to seal an extent first — it
#   logs "skipped — no sealed extents" otherwise, so an `ec`-only action list
#   converts nothing. Pair it with split/merge/gc (they roll streams), e.g.
#   `AUTUMN_CHAOS_ACTIONS=ec,kill,split,merge,gc`.
# Known harness note: only a ROLL seals an extent — restarting the PS replays
#   and keeps appending to the same open tail. And `autumn-client perf-check`
#   does not exit reliably once the log extent rolls (the cluster is fine
#   through it: the roll completes, the new tail's replicas agree, the manager
#   keeps probing) — the script caps it with `timeout -s KILL`.

# AT-REST ROT (`corrupt`): the one fault the other nemeses cannot produce.
# Every other action stops a process or cuts a link — faults the system is told
# about. This one flips bytes in ONE replica's `.dat` on disk and tells nobody:
# the file keeps its length and the extent keeps its eversion, so no error is
# raised anywhere and reads still succeed. At the end of the round it scrubs
# every rotted extent (`autumn-op scrub` path) and asserts the damaged node
# reported it (`SCRUB FOUND CONTENT ROT` in that EN's log) — not that a rebuild
# happened, because a fence in the same round rebuilds the same extents for
# unrelated reasons.
#
# It seals a tail itself (`MSG_ROLL_TAILS`) when no sealed extent exists yet,
# scrubs candidates first so their checksums are recorded, and only rots a copy
# that has them (`extent-{id}.ck` present) — rot before the first record is
# trust-on-first-use and undetectable by design. Isolated round, nothing else
# can drive a rebuild:
AUTUMN_CHAOS_ACTIONS=corrupt AUTUMN_CHAOS_DURATION_SECS=60 \
  cargo test -p autumn-manager --test system_chaos -- --nocapture --ignored
# The detection scrub names only rotted extents that still exist (a deleted one
# makes the manager refuse the whole op) and the check polls the op. If the
# rotted extent was EC-converted meanwhile, the check follows the bytes to the
# head of extent-{id}.shard0 on slot 0. The EC coordinator itself checks the
# .dat it encodes against its .ck: a rotted source logs
# `EC CONVERT FOUND CONTENT ROT` on that EN, the manager logs
# `EC source rotted: released the marker` and isolates the slot
# (source="ec_convert"); the conversion is refused until it is rebuilt. With
# `corrupt` and `ec` in one round (the full set) both paths are exercised:
AUTUMN_CHAOS_SEED=23 AUTUMN_CHAOS_DURATION_SECS=180 AUTUMN_CHAOS_NEMESIS_INTERVAL_MS=1000 \
  cargo test -p autumn-manager --test system_chaos chaos_real -- --nocapture --ignored
# Deterministic: cargo test -p autumn-manager --test scrub_on_demand

# AFTER ANY EN-SIDE CHANGE, rebuild the WHOLE workspace before running chaos:
# the harness spawns `target/debug/autumn-extent-node`, and `cargo build -p
# autumn-stream` rebuilds the library WITHOUT relinking that binary — so the
# ENs keep running the old code while the test binary has the new. Confirm with
# a string only the new code contains, e.g.
#   cargo build --workspace
#   strings -a target/debug/autumn-extent-node | grep -c '<a new log message>'
```

## Rolling restart & upgrade versioning

**Deploy note (2026-08-27, MVCC internal-key comparator):** the partition
layer's internal-key encoding is `user_key ++ BE(u64::MAX - seq)` ordered by a
user-key-first comparator (no `0x00` separator byte). SSTs and WAL records
written under the older separator encoding are NOT readable — present keys
come back not-found and replay mis-splits keys. There is no migration: a
cluster carrying pre-change partition data must be rebuilt from empty
(`cluster.sh reset` on dev). Same-commit stop-world deploys after that
boundary are unaffected.

Same-binary rolling restart of a live cluster — one process at a time, a
convergence gate + per-partition write-liveness probe between every step,
fail-stop on the first gate that doesn't converge. Order: EN one-by-one →
PS → manager (most-depended-on end first).

```bash
# cluster must already be running (any cluster.sh start/reset shape)
bash scripts/rolling_restart.sh
# knobs: ROLL_GATE_TIMEOUT (180s), ROLL_HB_FRESH_SECS (10), ROLL_LIVENESS_TRIES (30)
# pass the same AUTUMN_DATA_ROOT / AUTUMN_TRANSPORT the cluster was started with
```

Manual verification:

```bash
bash cluster.sh reset 3                       # or: AUTUMN_BOOTSTRAP_PRESPLIT=4:hexstring bash cluster.sh reset 3
bash scripts/rolling_restart.sh               # expect: ... ROLLING RESTART COMPLETE ... zero loss
```

What it asserts per step: EN back `Online` with fresh heartbeat + recovery
drained (`recovery-stats` 0 inflight / 0 backoff); PS has every partition
routed (`info` shows no `ps=unknown`; the authoritative per-partition gate is
the liveness probe — one provably-in-range key per partition); manager answers
`info` again (leader re-elected from etcd replay) with all nodes Online.
Before the roll it seeds one 1 KiB key per partition + a 12 MiB striped value;
after the roll all are content-verified (zero ACKed loss). Probe keys are
namespaced `<range-prefix>__autumn-roll-<runid>-*` and deleted on exit; a
flock on `$AUTUMN_DATA_ROOT/rolling_restart.lock` rejects concurrent rolls.
Verified 2026-06-12 on a 3-EN/4-partition cluster under continuous external
writes: 191/191 ACKed keys survived.

`cluster.sh` provides the manager per-process subcommands for this:
`start-manager` / `stop-manager` / `restart-manager` (etcd state replay makes
a manager bounce a safe rolling step).

### Rebalancing region→PS assignment after a restart

**Symptom:** after a restart (especially a k8s rolling `kubectl apply`, which
bounces the PS pods one at a time) `autumn-op info` shows **all partitions
serving from one PS**, the others idle. This is expected, not a bug: the
region→PS assignment is **sticky in etcd** — the manager keeps a region on its
currently-registered PS and only reassigns regions whose PS is *unregistered*.
An eviction window during the restart (the PS being bounced misses its 10 s
heartbeat) moves its regions to whichever PS is up; when it comes back its old
regions are already sticky elsewhere. **A PS restart or a manager restart does
NOT re-spread them** (both keep the sticky assignment).

**Fix — actively re-spread with one command:**

```bash
autumn-op --manager <MGR> rebalance            # move as many as needed to balance
autumn-op --manager <MGR> rebalance 5           # throttle: at most 5 moves this call
autumn-op --manager <MGR> rebalance --json      # machine-readable {moved, moves[]}
```

The manager reassigns partitions until no move would place one strictly better
(see "Which PS a partition goes on" below). Among PS started without `--cpuset`
that is plain count balancing, gap ≤ 1 (like HBase `SimpleLoadBalancer` /
TiKV-PD `balance-region`); a `--cpuset` PS is filled up to its core slots and no
further while any capacity-unknown PS remains. Each move rewrites the region's `ps_id`; the old PS's
`region_sync_loop` closes the partition and the new PS opens it (~2 s tick +
that partition's recover_partition). The key RANGE doesn't change (no
`region_epoch` bump); clients re-resolve the moved partition's listener via the
refreshed `part_addr` and the SDK's routing-miss retry absorbs the brief
per-partition reopen window. **Throttle with `[MAX_MOVES]`** on a large cluster
so the target PSes aren't hit by a reopen storm all at once — run it a few times,
or once unbounded on an idle cluster.

Verify:

```bash
autumn-op --manager <MGR> info | grep '  part' | awk '{print $4}' | sort | uniq -c
# expect the counts spread across all PS addresses, gap <= 1
```

Idempotent: re-running on an already-balanced cluster reports `0 moves`. (An
automatic version — the dashboard auto-policy `rebalance` switch — is
Phase B, not yet shipped.)

### Which PS a partition goes on (core slots)

A PS started with `--cpuset <N cores>` has `N` core slots, one per partition (a
soft figure: the PS pins no thread, and past it partitions share the cores). A new partition, a split's right child,
and a partition whose PS was evicted all go to, in order:

1. a `--cpuset` PS with a free slot — the most free slots first (absolute
   count, not fraction: a 1-slot PS is used only once the larger ones are
   down to 1 free);
2. a PS started without `--cpuset` — the fewest partitions first;
3. only if every PS is a full `--cpuset` PS: the one it overfills least; that
   PS opens it, its partitions sharing the cores.
   `rebalance` never moves a partition onto a full `--cpuset` PS.

See the slots:

```bash
autumn-op --manager <MGR> info            # "partition servers:" → "ps 2 ... 1/2 slots"
autumn-op --manager <MGR> --json info     # ps_servers[].slot_cap (null = no --cpuset)
```

The state after `slots` says whether the PS serves what it was assigned:

| state | meaning |
|-------|---------|
| `ready` | its last heartbeat (< 6 s old) reported every assigned partition open at its current epoch |
| `opening n/m` | `n` of its `m` partitions are open; the rest are recovering, refused (over its cpuset), or not yet reloaded after a split |
| `awaiting report` | registered, but no heartbeat since (just started, or this manager just became leader) |
| `silent Ns` | no heartbeat for `N` ≥ 6 s; the manager evicts it at 10 s |

A heartbeat alone is not readiness: the PS starts beating before its partitions
replay their logs. Wait for `ready` (`--json info`: `ps_servers[].ready`) before
driving traffic after a start. `cluster.sh` start / `start-ps` and
`autumn-deploy` do; `cluster.sh` FAILS the start (after 120 s, printing the
`partition servers:` lines) when the PS cannot open every partition. The rest of
the cluster is left running for inspection; `cluster.sh stop` cleans up. A PS stopped with SIGTERM reports nothing open as it starts
draining; one killed with `kill -9` still reads `ready` until its replacement
registers or its last heartbeat is 6 s old.

`?` / `null` means the PS has no `--cpuset`, or the manager has not heard its
heartbeat since becoming leader. The caps live only in manager memory, so for
~2 s after a manager restart or failover every PS shows `?` and is placed as
capacity-unknown; the next heartbeat restores them. A slot is one core of the
PS's cpuset per partition; past its slots a PS still opens every partition and
split, the partitions sharing the cores (see "A PS with more partitions than
cores").

Manual verification (throwaway etcd + manager + 1 EN + 3 PS; PS1 `--cpuset`
1 core = 1 slot, PS2 2 cores = 2 slots, PS3 none):

```bash
autumn-op --manager $MGR bootstrap --replication 1+0   # → on PS2 (2 free)
autumn-op --manager $MGR split <P> --at-raw-hex 6d     # child → PS1 (1 free each, lower id)
# kill -9 the manager, restart it on the same etcd, wait 3 s
autumn-op --manager $MGR info                          # PS1 1/1, PS2 1/2, PS3 0/?
autumn-op --manager $MGR split <P> --at-raw-hex 66     # child → PS2 (free slot beats empty PS3)
# kill -9 PS1, wait 15 s for eviction
autumn-op --manager $MGR info                          # PS1's partition → PS3 (PS2 is full)
```

Manual verification of readiness (throwaway etcd + manager + 1 EN + PS1 with
`--cpuset` 2 cores = 2 slots):

```bash
autumn-op --manager $MGR bootstrap --replication 1+0   # PS1: opening 0/1 → ready
autumn-op --manager $MGR split <P> --at-raw-hex 6d     # PS1: opening 0/2 → opening 1/2 → ready
# SIGTERM PS1 and wait for it to exit
autumn-op --manager $MGR info                          # PS1: opening 0/2 (not ready)
# start PS1 again                                      # awaiting report / opening → ready
# kill -9 PS1, wait 7 s
autumn-op --manager $MGR info                          # PS1: silent 7s
# start PS1 with --cpuset of 1 core (1 slot)
autumn-op --manager $MGR info                          # PS1 2/1 slots, ready: both partitions share the core
```

### RPC version checks and general upgrade procedure

The connection contract is specified in [cluster_version_design.md](cluster_version_design.md).
`VERSION_HELLO` (opcode `0xF0`, magic `AUPH`, bootstrap version 1) is mandatory
on manager, PS and EN connections. Deploy this first bootstrap migration with
stopworld, updating all callers. The release's rolling-upgrade rehearsal must
validate the dependency order and ACKed-data recovery before using it in production.

Every connection completes a stable Hello before business payload decoding:
internal peers and admin tools require equal `WIRE_VERSION`; clients must lie
inside `[MIN_CLIENT_WIRE_VERSION, WIRE_VERSION]`. Retain the existing client
window and change its lower bound only when actual client compatibility is
removed. A mismatch reports the local/remote versions, connection role and
reason, rather than appearing as an unexplained business decode failure.
A decode error after successful Hello remains a separate protocol error;
matching version numbers cannot compensate for a forgotten wire bump.
The refusing server logs a WARN `VERSION_HELLO refused a version mismatch`
with the peer address, its declared role and versions; grep for
`refused a version mismatch` to find the stale binary (servers built before the
handshake's rename log it as `PROTOCOL_HELLO refused a version mismatch`).

After the Hello, a client connection to the manager or an extent node, and a
partition-server client until a CLIENT_AUTH succeeds while authz is on, may
only send frames up to 1 MiB: a larger header drops the connection before the
server reserves anything for it (logged at DEBUG as `payload too large`, so
`RUST_LOG=debug` to see it). Peer and admin connections, authenticated clients
and every client of a PS with authz off are not limited. Check: `cargo test -p autumn-manager --test frame_limit_admission`.

`autumn-op` connects as an ADMIN peer, so it needs the exact `WIRE_VERSION` of
the manager it talks to: during a wire-changing rollout use the old release's
`autumn-op` until the manager is replaced and the new one afterwards (step 5
below runs with the new one).

Every client built before `VERSION_HELLO` is refused whatever its wire number,
so the first Hello deployment also rebuilds every embedded client: fuse
daemons, the S3 gateway, Python wheels in inference pods, benchmark tools. An
old one reports a closed connection, not a version message, because it never
sees the Hello reply.

The implementation removes the manager's persisted `cluster_version` latch,
its startup/replay checks, query/bump RPCs, and `autumn-op cluster-version` /
`upgrade-version`. There is no final cluster bump after replacing binaries.
Frozen response fields retain their encoding as reserved placeholders, and
retired opcode numbers are not reused. Retain `cluster_id` and ownership
fencing. Removal of the latch does not prove old binaries can read newer data.

**Preparation, before any instance is stopped:**

1. Distribute the builds/images and check configuration. State whether wire,
   client surface or any persistent format/semantics changed. Analyze an actual
   persistent change separately; rolling upgrade is allowed only if that
   release's new/old readers and writers can coexist safely. The first unified
   Hello deployment uses the agreed stopworld procedure and updates every
   server, SDK and tool; later wire changes may use rolling replacement.
2. Record the active policy name and mode, then deactivate it. Use the existing
   admin credentials when required:

   ```bash
   autumn-op --manager "$MGR" auto-policy status
   autumn-op --manager "$MGR" auto-policy deactivate
   autumn-op --manager "$MGR" auto-policy status  # confirm mode=off
   ```

   Off is persisted and survives manager failover. Stop new manual/dashboard/
   scheduled management submissions too. This stops policy actuation; it does
   not cancel already submitted operations or disable all background workers.
3. Wait for submitted compaction, EC, GC, split/merge and rebalance operations
   to finish; also check recovery and pending work. Check terminal outcomes,
   not merely disappearance from the active list:

   ```bash
   autumn-op --manager "$MGR" ops list --active
   autumn-op --manager "$MGR" ops history --limit 50
   autumn-op --manager "$MGR" info
   ```

   PS expiry/deletion-triggered compactions can run independently of the
   manager's policy and may have no submitted op ID. Inspect their metrics/logs
   and include local background work in the per-instance drain. An empty live
   ledger alone is not proof that every worker is idle; leader changes also
   replace the live ledger. Do not proceed merely because a wait timed out.

**Replacement and recovery:**

1. Drain each affected instance, close its old connections, replace the binary
   and preserve its identity/data directories. Follow the dependency order or
   coordinated batches verified for this release; do not assume one universal
   manager → PS → EN order. Prevent an old process from being restarted
   automatically. Coordinate manager leadership changes and check that policy
   remains Off after failover.
2. With the same wire, wait for Hello, registration, recovery and business Ready
   before advancing. With a changed wire, an upgraded node may wait for peers
   that have not been upgraded. Advance on verified local initialization plus
   an explicit expected version-mismatch wait; waiting for every instance to
   become fully Ready before touching its dependencies can deadlock the rollout.
   Identity, storage and unexpected initialization errors are not this wait.
3. Different-wire internal RPCs fail before business decoding. Request failure,
   timeout, lost response, an unknown write outcome and temporary service
   unavailability are accepted during replacement. **Already acknowledged
   durable writes must remain readable after recovery.** Preserve the existing
   ownership fences, commit/checkpoint rules and WAL recovery; do not blindly
   replay a non-idempotent write whose response was lost. If drain cannot finish
   because a dependency already changed wire, follow the release's verified
   recovery procedure rather than treating it as a successful drain.
4. Verify leader, registration, ownership, partition readiness, replica/EC
   health, reads/writes, direct reads and previously acknowledged data. Current
   stream append waits for every replica, so a rolling EN restart can interrupt
   writes even while a majority remains alive. Measure read and write disruption
   separately; shared manager/EN dependencies can affect many partitions.
5. After all required nodes and business paths recover, restore the original
   policy name and mode. For example, if it was `balanced` and Armed:

   ```bash
   autumn-op --manager "$MGR" auto-policy start balanced
   autumn-op --manager "$MGR" auto-policy status
   ```

   Leave a previous Off state Off. Resume external management submissions only
   after recovery checks pass. On an upgrade failure keep policy Off while
   following the release's recovery/rollback procedure.

Pausing policy and waiting for maintenance reduce interference and outage time;
they do not by themselves prove data safety. A persistent-format change may
require stopping only its affected writers or stopworld, as determined by that
specific change. Do not automatically clear persisted history or other etcd
prefixes for a wire bump; handle each changed stored layout explicitly.

The existing client-window exercise uses real builds at different versions:

```bash
scripts/client_window_verify.sh
cargo test -p autumn-rpc --test client_surface_freeze
cargo test -p autumn-rpc --test negotiation_freeze
```

The script needs a hello-capable client build below the current ceiling. Until
one exists (every hello-capable build is at the ceiling) it prints "nothing to
prove" and exits 0. Unified Hello additionally needs real two-build
rolling-upgrade verification: version rejection before decode, reconnection,
mismatch waiting, leader failover, request failures and recovered acknowledged
data.

#### Converting the manager's persisted records — DONE, converter deleted

The manager's own etcd records were split out of the wire schema so they carry
their own version instead of borrowing `WIRE_VERSION`
(`crates/manager/CLAUDE.md`, "Persisted records"). Each split record is stored
inside an envelope, `[AUMG][record_type][format_version]`, and the servers speak
exactly one shape: **a value without the envelope is refused and the manager will
not take leadership.** There is no dual-read, by design.

Nine prefixes were covered: `mgr_audit_log/`, `tenantAccount/` (the principal
DB, since moved to `principal/` by `migratev1_v2`), `namespace/`,
`extents/`, `streams/`, `nodes/`, `disks/`, `partitions/`, `regions/`. Every
other persisted key (`opLog/`, `extent_inflight/`, `extentLayout/`,
`extentCorrupt/`, `node_override/`, `inode_leases/`, `autoPolicy/*`, …) is NOT
enveloped and stays bare.

**The one-shot converter `migratev0_v1` was run against the single production
cluster on 2026-09-20 and then DELETED, along with the `autumn-etcd` dependency
it had added to `crates/server`.** That is the contract a converter is held to —
it leaves no residue, because a converter kept around is how a one-time
migration turns back into the permanent compatibility code the rule exists to
avoid. `git show fb47730e:crates/server/src/bin/migratev0_v1.rs` has it, and the
section below records what it did on the live cluster.

**There is nothing left to run here.** A cluster restored from a pre-2026-09-20
etcd snapshot would need the converter again — recover it from git and rebuild
it; do not write a fresh one from memory. What it did, for whoever has to:
`migratev0_v1 --etcd <http://host:2379> [--dry-run]`, walking the nine prefixes
and printing a per-prefix `converted=/already=` summary. It is idempotent (an
already-enveloped value is skipped), so an interrupted run is simply re-run, and
`--dry-run` writes nothing. The section below calls those the "steps above".


#### What the conversion looked like on the real cluster (2026-09-20)

The VKE cluster went `aef13927` -> `e7a554fa`, wire 43 -> 45, in one window.
Recorded here because three things cost time that the steps above do not warn
about.

**Count the keys BEFORE the dry-run.** The summary line only tells you the tool
converted something; it cannot tell you it converted the right cluster's
something. `etcdctl get <prefix> --prefix --keys-only | grep -c` over the nine
prefixes predicted 455, the dry-run said 455, and the two agreeing is the check.
Afterwards the re-run said `0 converted / 455 already`, and
`regions/<id>` began `41554d47 09 01` — `AUMG`, type 9, format 1.

**Wiping `opLog/` is conditional, not automatic.** The advice above assumes the
wire bump moved `OpRecord`. Diff it first: across 43 -> 45 it was byte-identical,
the prefix was left alone, and `ops history` afterwards decoded all 21 records
with no skip warnings. Diff the other bare-persisted types the same way —
`MgrExtentInflightRecord` and `MgrAutoPolicyEntry` are rkyv; `extentLayout/` and
`extentCorrupt/` are raw bytes and can never be affected.

**A stop-the-world restart concentrates every partition on one PS.** This is
structural: `handle_register_ps` runs `rebalance_regions` when the FIRST PS
registers, and with one PS in `ps_nodes` every region whose owner has not come
back yet is reassigned to it. An even 8/9/7 became 24/0/0. The cure is
`autumn-op rebalance`, which moves at most 4 regions per op — four passes to
reach 8/8/8.

```bash
# repeat until it reports "moved 0"
autumn-op --manager $MGR:9001 --cluster-secret-file F --wait rebalance 0
```

**Read the spread from `--json`, not from `info`.** A rebalance DELETES the
stale per-partition listener addr and the new PS re-registers it a little later,
so the text view lags: it showed 10/6/8 while `--json info` already showed
8/8/8. The same lag makes partition SIZES read low for a few minutes after the
partition servers come up — two partitions read 48.1 GB and 16.5 GB mid-open and
were back at 62.3 and 31.2 GB shortly after. Neither is data loss; do not act on
either until the numbers settle.

One more, for whoever compares namespaces afterwards: a bare `autumn-client ls`
scans from the namespace head and does not continue into later partitions, so it
returns nothing when the head is empty even though keys exist further on. Use
`ls --prefix` before concluding a namespace was emptied.

#### `migratev1_v2` — the tenant removal and the observe mode (to wire 60, stop the world)

Three etcd changes ship with it (the tenant removal was wire 59, the observe
mode's removal wire 60):

- the principal account DB moves from `tenantAccount/<name>` to `principal/<name>`.
  The value is copied unchanged (same record type 2, format 1).
- `namespace/<name>` goes from format 1 to 2: `owner_tenant` is dropped. It fed
  only the protected-prefix list, which no PS read.
- `autoPolicy/config` in mode 1 (observe) goes to mode 0 (off), the active
  policy still selected. A wire-60 manager refuses to lead on mode 1
  (`autoPolicy/config mode 1 is not a mode this build has ... run
  migratev1_v2`). After the restart, `auto-policy start <name>` runs it if it
  should run.

A manager from wire 59 on refuses to lead while any namespace record is still v1, with
`namespace/fs: namespace record is at format version 1, this binary speaks 2.
The converter has not run`. The built-in `fs`/`kvc`/`mem` rows exist on every
bootstrapped cluster, so an unconverted cluster always stops there instead of
starting with principals missing. Wire 58 and 60 cannot run together, so stop
everything.

```bash
# 1. pause policy, wait for dispatched ops (general procedure above), then stop
#    every manager, PS and EN.
# 2. count what will be converted, then dry-run; on a first run the counts match
#    moved= and converted= (after an interrupted run some show as already=).
etcdctl get tenantAccount/ --prefix --keys-only | grep -c .
etcdctl get namespace/ --prefix --keys-only | grep -c .
cargo run --release --bin migratev1_v2 -- --etcd http://ETCD:2379 --dry-run
#    tenantAccount/ -> principal/  moved=N already=0
#    namespace/<name>: dropping owner_tenant "..."   (one line per owned namespace)
#    autoPolicy/config: observe mode -> off      (or: nothing to convert)
#    namespace/ v1 -> v2  converted=M already=0
# 3. convert, then re-run: the re-run must report moved=0 and converted=0, with
#    already=M for namespaces.
cargo run --release --bin migratev1_v2 -- --etcd http://ETCD:2379
cargo run --release --bin migratev1_v2 -- --etcd http://ETCD:2379
# 4. start the wire-60 binaries and check:
autumn-op --cluster-secret-file F --manager MGR:9001 principal-list   # same names + grants
autumn-op --cluster-secret-file F --manager MGR:9001 namespace-list   # no OWNER column
autumn-op --cluster-secret-file F --manager MGR:9001 auto-policy status   # mode off or armed
autumn-op --cluster-secret-file F --manager MGR:9001 mint-token --principal P --credential HEX
```

The tool refuses while any manager is up (the leader key or any
`managerAlive/<id>` is present); a manager that just exited leaves its keys for
up to 10 s until its lease expires — wait and re-run. It moves
accounts first (one txn per account: create `principal/<n>` if absent, delete
`tenantAccount/<n>`) and namespaces last, each write a compare-and-put, so a run
interrupted anywhere is simply re-run. Credentials keep working: the account
bytes, and so the credential hash, are unchanged. After the production cluster
has converted, delete `crates/server/src/bin/migratev1_v2.rs`, its `[[bin]]`
entry and the `autumn-etcd` dependency in `crates/server/Cargo.toml`.

Operator-visible changes in the same release: `namespace-create`, `split` and
`presplit` no longer take `--tenant` (`split`/`presplit --namespace` take a scope
such as `bench/perf`), `mint-token` takes only `--principal`, the manager no
longer takes `--auth-protected-prefix`, and the kvcache `auth_tenant` config key
is gone (use `auth_credential_file` and optionally `auth_principal`).

#### Rolling BACK onto data a newer binary wrote

Rollback is unsupported, and two persisted values now say so out loud rather
than degrading into serving the wrong bytes. Both concern the per-extent
payload location — which FILE holds an extent's bytes, `extent-{id}.dat` or
`extent-{id}.shard{i}` — because reading that wrong is a whole value served
from a shard, silently.

- **The manager refuses leadership** if `extentLayout/<id>` names a location
  this binary does not have. The log line names the key and the byte:
  `extentLayout/512 names payload location 2, which this build does not have`.
  There is no repair from the old binary — the extent's bytes really are
  somewhere it cannot address. Go forward to the binary that wrote it.
- **An extent node quarantines the extent** if its `.meta` names one, logging
  `META-FAILCLOSED: .meta names a payload location this build does not have`.
  Reads and appends on that extent are refused so the client fails over to
  another replica; everything else on the node keeps serving.

Neither can fire on a cluster that has only ever run one binary version
forward: there are exactly two locations today, so no in-tree writer can
produce a third byte. What they are for is the day a third is added.

v28 changed the FRAME layer itself (one uniform shape:
`[header][ctrl_len][ctrl][crc][value]`, crc over header+ctrl, raw value tails
uncrc'd). Deploy note: a pre-v28 binary against a v28 peer fails at the FIRST
frame with a loud `frame CRC mismatch` connection error — it never reaches the
GetClusterId version handshake, so expect transport-level errors (not the
"wire-schema mismatch" message) in a mixed deploy. Same-commit deploys are
unaffected.

Bump discipline lives in `crates/rpc/src/lib.rs`, and it is MANUAL. The
fingerprint registry that used to fail `cargo test -p autumn-rpc` on any
wire-schema edit is GONE, so nothing detects a forgotten `WIRE_VERSION`
bump: two binaries claiming the same version with different layouts will
handshake happily and then decode each other's bytes as garbage. Edit any rkyv
wire struct ⇒ bump `WIRE_VERSION` yourself, and raise
`MIN_CLIENT_WIRE_VERSION` too if the change breaks the client-facing surface —
that one is what forces every image carrying an embedded client to be rebuilt.
Adding a message TYPE is not a bump (an old peer that never sends it cannot be
affected by its existence), but a new client-facing one must be classified in
`crates/rpc/src/client_hello.rs` or it lands outside the window silently.

The CLIENT-facing half of that is not manual. Every request and response form
behind a client-surface msg_type has its encoding recorded — plus the
capability claims the SDK decodes out of its own token, the extent-node
direct-read forms (`--direct-read` is on by default), and the numbering of
`StatusCode` and both `CODE_*` families — so editing one in place goes red
rather than through:

```bash
cargo test -p autumn-rpc --test client_surface_freeze
cargo test -p autumn-rpc --test negotiation_freeze
# Red means a client-facing break. The fix is a NEW msg_type carrying the new
# form with the old struct untouched — NOT an edit to the recorded bytes, and
# not a MIN_CLIENT_WIRE_VERSION raise unless you mean to rebuild every image
# carrying an embedded client. Read the failing file's header; it says so.
# A msg_type added to a client-surface set with nothing recorded — in EITHER
# direction — also fails here, so the freeze cannot fall behind a growing
# surface.
#
# If EVERY value moved at once you did not change a struct: rkyv's archived
# format did. That is a break of all forms simultaneously, so it is a
# MIN_CLIENT_WIRE_VERSION raise and a rebuild of every embedded client, with
# the whole table re-recorded in that commit. The file's header says so.
```

Internal wire layouts are not frozen by the client-surface tests. Bump
WIRE_VERSION when an internal layout or incompatible protocol meaning changes;
wire-changing rolling replacement may have failed RPCs and temporary
unavailability, following the general upgrade procedure above. No cluster bump
is part of the target flow. Rollback depends on the actual stored data and
protocol compatibility of the release, not a cluster_version latch.

## Direct read on EC extents

`--direct-read` reads a value straight from an extent node instead of proxying
through the partition server. It used to refuse EC-converted extents outright,
which is why the flag quietly stopped applying on any cluster with EC armed —
bootstrap arms it from four extent nodes up, so that is the common case.

It now reads EC extents by fetching only the DATA SHARDS the value covers. No
Reed-Solomon decode happens on the client: the payload is a plain concatenation
`shard0 ‖ … ‖ shard_{k-1}` with `shard_size = ceil(sealed_length / k)`, so a
byte range is a contiguous run of shard sub-ranges. Decoding is for
RECONSTRUCTING a shard whose node will not answer — the client cannot, so any
failure falls back to the partition server, which can.

**Do not expect a fan-out speedup.** `shard_size` is per EXTENT, and extents
seal at 16 GiB by default (1 GiB floor), so at k=4 each shard is gigabytes. A
typical 8 MiB value lands in ONE shard — measured on a live 3+1 cluster, a
667 KB value read `shards_read=1`. The point of this is that direct read keeps
working on an EC cluster, not that it goes faster.

Whether it is actually in play is otherwise unanswerable from outside, since a
decline and a success both end in correct bytes. The client says so once:

```
EC direct read active: reading data shards straight from their nodes
  extent_id=16 data_shards=3 shards_read=1
```

and warns once if it ever falls back (`EC direct read fell back to the PS
proxy`). Declines are logged by the PARTITION SERVER at debug, with the reason:
a data shard's node is Suspected (transient), or the shards are not in shard
files (a pre-CoW conversion, permanent for that extent).

To exercise it by hand, note two traps that make a test pass without touching
the path at all:

- **`autumnfs` takes `--direct-read` too** (default true, same as a mount). It
  reads the KV layout directly rather than through the fuse core, so it does not
  inherit the mount's setting — it has its own flag, and until it did, every
  `autumnfs get`/`cat` proxied through the partition server no matter how the
  cluster was configured.
- **Measure a read with `cat > /dev/null`, never `get <local-file>`.** The
  download's own write to local disk caps the whole thing at ~800 MiB/s and
  hides everything above it. Measured on a 4 GiB file, EC 2+1, loopback TCP:
  `get` to a file 797 MiB/s, `cat > /dev/null` 769 (direct off) and 1552-2053
  (direct on) — i.e. the file write erased a 2x difference and, compared
  against a fuse `dd of=/dev/null`, inverted which client looked faster.
- **Striping can put every value under the threshold.** Direct read engages at
  64 KiB. A 300 KB file striped across 24 lanes stores ~12.5 KB per value and
  never qualifies; size the file so `size / lanes >= 64 KiB`.

```bash
# 4 nodes → log stream EC 3+1 by default
AUTUMN_DATA_ROOT=/data08/ec ./cluster.sh reset 4
autumnfs --manager 127.0.0.1:9001 put big.bin /ec/big.bin      # >= 1.5 MiB
# Seal its log extent: restart the PS at the 1 GiB floor and fill past it,
# since there is no operator command that seals a partition's tail.
autumn-ps --psid 1 ... --max-extent-size-bytes 1073741824
autumn-op --cluster-secret-file <F> force-ec-convert --extent <ID>
autumn-op --cluster-secret-file <F> info --part <PID>   # wait for "ec":true
autumn-s3 --manager 127.0.0.1:9001 --port 9100 --direct-read true &
curl -s http://127.0.0.1:9100/ec/big.bin -o out.bin && cmp big.bin out.bin
```

Killing a node that holds one of the data shards must keep the read
byte-correct — it declines to the proxy, which rebuilds the shard from parity.

## A/B-ing a wire-path change (and the three traps that fake the answer)

A change that only moves bytes around — zero-copy vs copy, inline vs iovec —
cannot be judged from one number. The recipe that produced the
`MSG_BATCH_PUT_BULK` decision:

```bash
# Two binaries differing ONLY in the branch under test, against ONE cluster.
cargo build --release -p autumn-server                      # variant A (new path)
cp target/release/autumn-client /tmp/ac-new
# edit the selection to force the old path, rebuild, snapshot as /tmp/ac-old
# (for UCX add --features autumn-server/ucx to BOTH builds)

AUTUMN_DATA_ROOT=/data08/autumn-bulk AUTUMN_EXTENT_BASE_PORT=20000 \
  AUTUMN_EXTENT_SHARDS=8 ./cluster.sh reset 1
/tmp/ac-new ... perf-check --bulk 64 --size 32768     # WARMUP, discard
/tmp/ac-old ... perf-check --bulk 64 --size 32768     # alternate, 2+ samples each
/tmp/ac-new ... perf-check --bulk 64 --size 32768
```

**Confirming the PS actually took the zero-copy recv path.** A/B numbers can
move for reasons unrelated to the branch you edited, so check the branch
directly: the partition server logs one line the first time a bulk write tail
lands in a pooled buffer.

```bash
grep -m1 "PS write-recv bulk engaged" /tmp/autumn-rs-logs/ps.log
#  ... msg_type=90 transport="tcp(pooled)"   (90 = 0x5A = MSG_BATCH_PUT_BULK,
#                                             81 = 0x51 = MSG_PUT_BULK)
```

It is logged ONCE per process, so absence after a long run means the path never
engaged — most often because the tail was under 64 KiB, or because authz is on
(which skips this fast path deliberately, so the gate can enforce uniformly on
the normal decode path).

**Trap 1 — the first run after `cluster.sh reset` is garbage.** Observed 42
ops/s and 5.33 MB/s on runs that repeated at 25 688 ops/s and 447 MB/s moments
later. Always warm up and discard; never compare a post-reset run against a warm
one.

**Trap 2 — pick an operating point where the resource under test is the
bottleneck.** 4 KiB batch writes sit on the single-partition ~30k ops/s ceiling
(that ceiling was measured at 64-byte values, so it is op-bound, not byte-bound):
zero-copy measured 103.7 vs 103.8 MB/s there — a true null that says nothing
about copies. The same change is +16% at 32 KiB. Before believing a null result,
check ops/s against the known ceiling.

**Trap 3 — loopback TCP is the transport where zero-copy matters least.** The
same change is ~+61% over RoCE at 4 KiB, the size where TCP showed nothing. To
get real RDMA on ONE host, bind a RoCE NIC IP — `rc` loops back inside the HCA:

```bash
AUTUMN_BIND_HOST="[fdbd:dc62:3:300::14]" AUTUMN_TRANSPORT=ucx \
  UCX_NET_DEVICES=mlx5_2:1 ./cluster.sh reset 1      # derives rc_mlx5,ud_mlx5,tcp,self
# client must carry the SAME UCX_TLS + UCX_NET_DEVICES and the RoCE manager addr
```

Do NOT reach for `UCX_TLS=posix,cma,tcp,self` to get UCX on 127.0.0.1: it is the
legacy escape hatch, `cluster.sh` warns that ≥64 KiB transfers are known-broken
there, and a batched frame is ≥64 KiB by construction — both arms of the A/B
land in the broken region and the numbers mean nothing. `mlx5_2` is the storage
NIC here; `mlx5_1` is the GPU's, so pinning `mlx5_2:1` also keeps the bench off
the tenant's card.

Byte correctness is a separate question from throughput, and `perf-check` does
not check it — it never compares what it read against what it wrote. Cover a new
write path with a system test that reads every value back:

```bash
cargo test -p autumn-manager --test system_putstream put_many
#   → put_many_small_values_take_the_batched_bulk_path ... ok
#   → put_many_mixed_size_duplicate_key_keeps_last_input ... ok   (same key, small + bulk value, last input wins)
```

## Test matrix

```bash
cargo test --workspace --exclude ucx-sys-mini --lib         # all crate unit tests
cargo test -p autumn-stream --lib                            # stream layer only
cargo test -p autumn-partition-server --lib                  # partition server only
cargo test -p autumn-manager                                 # integration (needs etcd)
```

Manager integration tests under `crates/manager/tests/` cover split / merge / chaos / crash
recovery, and all run in CI except three opt-ins marked `#[ignore]`: the full
chaos harness (`system_chaos`), the >1 GiB GC-floor test
(`system_gc_floor_durable_ckpt`) and the append benchmark (`append_benchmark`,
run by `scripts/benchmark_append_stream.sh`). Run one by name with
`-- --ignored`.

GC data-integrity regression (full VP-identity liveness — a superseded older
version of a key in the same sealed extent must NOT revive over the newer one):

```bash
cargo test -p autumn-manager --test system_gc_multiversion_same_extent
```

**Write-pipeline changes (e.g. natural batching) are verified with the perf matrix**
(`perf/perf_check.sh` builds release, starts a fresh 3-replica cluster, runs 4K and 8M
and compares each leg against `perf/perf_baseline_<transport>_p8_d8_s<size>.json`; a leg
passes when ops/s ≥ 80% of baseline and p99 ≤ 2×). Between a leg's write and read
phases the script compacts every partition (perf-check `--before-read`, which runs
`autumn-op compact <part> --wait`), so the reads come from SSTs. Before that, a read
leg read whatever the write phase had left in the memtable, which moved with the
write throughput: at one `--conn-inflight-cap` the 4K read was 562–568K, at another
412–423K, while reads of the same fixed data were equal. With the compaction the 4K
read no longer moves with the write volume (TCP, 2026-10-28: 10 s of writes 247K/232K,
20 s 268K/249K): the read set is always far past the 512 MiB block cache, so the leg
measures SST reads that mostly miss the cache. TCP and UCX run as two invocations,
because UCX — on one host as on many — binds a RoCE NIC IP with
`UCX_TLS=rc_mlx5,ud_mlx5,tcp,self` and a pinned `UCX_NET_DEVICES` (the script refuses
UCX on 127.0.0.1; there is no loopback UCX configuration):

```bash
export AUTUMN_DATA_ROOT=/data05/autumn-rs AUTUMN_EXTENT_SHARDS=8   # plus the cpusets below
./perf/perf_check.sh --3disk --partitions 8 --tcp
AUTUMN_BIND_HOST='[fdbd:dc62:3:300::14]' UCX_NET_DEVICES=mlx5_2:1 \
  ./perf/perf_check.sh --3disk --partitions 8 --ucx      # eth1 = mlx5_2, the storage NIC
```

To compare one fixed key set read from the memtable and from SSTs, start a cluster the
way the script does (it tears its own down) and run `ycsb` twice, the second time with
`--before-run` compacting every partition (the memtable holds 256 MiB per partition, so
keep the set under that):

```bash
bash cluster.sh start 3
ao="target/release/autumn-op --cluster-secret-file $AUTUMN_DATA_ROOT/cluster.secret --manager 127.0.0.1:9001"
$ao namespace-create --name bench && $ao presplit --namespace bench/perf --count 8
compact="for p in \$($ao info | awk '/^  part/{print \$2}'); do $ao compact \$p --wait || exit 1; done"
for hook in "" "$compact"; do
  target/release/autumn-client --manager 127.0.0.1:9001 ycsb --threads 16 --duration 15 \
    --size 4096 --partitions 8 --pipeline-depth 8 --read-ratio 1.0 --key-dist uniform \
    --records 5000 ${hook:+--before-run "$hook"}
done
```

`--records` is per thread: 5000 × 16 × 4 KiB fits the 512 MiB block cache, 25000 does
not. Measured 2026-10-28 (8 partitions, cluster cpuset-isolated): a set that fits reads
from SSTs at 85% (TCP) / 98% (UCX) of the memtable rate; a set four times the cache at
15% / 20%, the cost of fetching a ~62 KiB block from an extent node for each 4 KiB value.

The committed baselines (2026-09-28) were taken this way with direct I/O on (the EN
default) and the io_uring workers confined: TCP with the cluster on node-1 cores
(EN 48-55/56-63/64-71, PS 72-87), UCX on the NIC's node 0 (EN 8-15/16-23/24-31,
PS 32-47); each file is the median-write run of three. TCP 4K 54.9K write / 583K
read ops/s, 8M 2.4 GB/s write / 7.5 GB/s read; UCX 4K 18.3K / 964K, 8M 2.2 / 5.0
GB/s — UCX 4K writes are slow because every small append pays an rc round trip,
which the old posix-shm baselines hid. These baselines predate the compaction
between the phases: their 4K reads were mostly memtable reads, so a 4K read leg
now fails against them until they are re-recorded on an idle host. Batch sizing is adaptive and needs no tuning
knob (the old `--min-pipeline-batch` PS flag is refused).

**Pin the cluster away from the tenants first.** This box is shared with
inference jobs (sglang, Ray), and a perf number taken while the cluster shares
cores with them is not comparable to anything — not to the committed baseline,
not to a run an hour later. On 2026-09-24 a full day of 4K-write samples
(33-48K ops/s, "a regression") came from a cluster pinned to NUMA node 0 next to
sglang's schedulers and a 100%-busy python; the same binaries on quiet node-1
cores gave 67-84K. Contention also distorts the batch-size signal below: a
descheduled loop finds both connections' frames piled up and reports a bigger
batch than it would on quiet cores. Before every perf run:

```bash
# 30 s of per-CPU busy% — any core above a few percent belongs to someone else
awk 'NR==FNR{if($1~/^cpu[0-9]/){a[$1]=$2+$3+$4+$6+$7+$8; t[$1]=$2+$3+$4+$5+$6+$7+$8}; next}
     $1~/^cpu[0-9]/{n=substr($1,4)+0; b=$2+$3+$4+$6+$7+$8; tt=$2+$3+$4+$5+$6+$7+$8;
     u=100*(b-a[$1])/(tt-t[$1]); if(u>3) printf "%d:%.0f%% ", n, u}' /proc/stat <(sleep 30; cat /proc/stat); echo
# the tenants' hot threads, where they run and what they are allowed to run on
ps -eLo pcpu,psr,tid,comm --sort=-pcpu | awk '$1>20' | head -20
taskset -pc <tid>
# NUMA halves and hyperthread siblings: keep EN and PS on ONE node, and never
# put two of our processes on the two siblings of one physical core
lscpu | grep 'NUMA node[0-9]'; cat /sys/devices/system/cpu/cpu0/topology/thread_siblings_list
```

**Where `taskset` on the launcher lands things.** The children inherit its mask.
The auto layout (no `AUTUMN_*_CPUSET`) is carved out of that mask: `taskset -c
60-95 cluster.sh start 3` puts the ENs on 60, 61, 62… and the PS after them.
`start-node`/`restart-node`/`start-ps` reuse those cores from the snapshot;
`start`/`restart`/`reset` lay the cluster out afresh from the mask of the shell
that runs them, so wrap those in the same `taskset`. Explicit
`AUTUMN_EN{i}_CPUSET` / `AUTUMN_PS_CPUSET` are used as given, even outside the
mask: the EN shards `sched_setaffinity` there, and the PS moves its main thread
(so every PS thread) onto its cpuset at start, as `taskset` would, while
everything else — the EN main and accept threads, the manager, etcd — stays on
the launcher's cores. So with an explicit layout, do not wrap the launcher. A
core outside the process's cgroup stops the EN or the PS at start. Check after every start that the threads really are where the layout
says:

```bash
for p in $(pgrep -x autumn-extent-n; pgrep -x autumn-ps); do
  for t in /proc/$p/task/*; do echo "$p $(cat $t/comm) $(awk '/Cpus_allowed_list/{print $2}' $t/status)"; done
done | grep -E ' (extent-shard-[0-9]+|autumn-extent-n|part-[0-9]+(-sst)?) '
# autumn-extent-n is pinned only in a single-shard EN (its runtime is the main
# thread). Every PS thread (part-N, part-N-sst, autumn-ps, ...) lists the whole
# PS --cpuset: the PS pins nothing.
```

The io_uring worker threads of those runtimes (`iou-wrk-*`) must be inside the
same cores. They exist only once the ring has punted work (buffered writes,
fsync), so check while the cluster is writing:

```bash
for p in $(pgrep -x autumn-extent-n; pgrep -x autumn-ps); do
  for t in /proc/$p/task/*; do c=$(cat $t/comm); [[ $c == iou-wrk* ]] &&
    echo "$p $c $(awk '/Cpus_allowed_list/{print $2}' $t/status)"; done
done | sort -k3 | uniq -c -f2
# every list inside that process's --cpuset. A whole NUMA node (e.g.
# 48-95,144-191) means the registration failed: look for "cannot confine
# io_uring worker threads" in the log (Linux < 5.14, or --cpuset cores outside
# the process's cgroup).
```

Without this, an EN's page-cache copy and writeback ran on up to four more
cores than its `--cpuset` — buffered write throughput measured before the fix
(~2.0 GB/s for 3 ENs x 2 cores) is not comparable with after (~1.27 GB/s).

Then hand `cluster.sh` an explicit layout on the quiet node (EN cpuset length
must equal `AUTUMN_EXTENT_SHARDS`; the PS pins nothing and runs every thread
anywhere in its cpuset, so give it one core per partition plus spare (cluster.sh
gives two) on ONE NUMA node; ranges may be comma lists). The layout that worked on 2026-09-25 with sglang pinned to
node 0 (`0-47,96-143`) and its stragglers on 54/58/70/71/76/77/82/94 of node 1:

```bash
export AUTUMN_EN1_CPUSET=59-66 AUTUMN_EN2_CPUSET=83-90 AUTUMN_EN3_CPUSET=72-75,78-81 \
       AUTUMN_PS_CPUSET=48-53,55-57,67-69,91-93,95 AUTUMN_EXTENT_SHARDS=8
```

The bench client stays unpinned (a pinned loopback client is its own artifact),
so read ops/s move with where the client lands relative to the PS's node
(≈1.3M with the PS on node 0, ≈0.5M on node 1 on this box): compare reads only
within one layout. Interleave A and B runs (A B A B …) so tenant drift hits both
sides alike, and record the tenants' hot threads at the start and end of every
run next to the numbers.

**Check the group-commit batch size, not just ops/s.** On this host a single
perf-check sample of 4K write ops/s swings 30-50% run to run (same binary), so
ops/s alone cannot tell a write-pipeline regression from noise; the batch size
can. While a 4K write leg runs, the PS logs one `partition write summary` line
per partition per second; the ops-weighted `avg_batch_size` over the write phase
is the number to compare:

```bash
sed 's/\x1b\[[0-9;]*m//g' /tmp/autumn-rs-logs/ps.log | grep 'partition write summary' \
  | awk '{for(i=1;i<=NF;i++){split($i,a,"="); if(a[1]=="ops")o=a[2]; if(a[1]=="avg_batch_size")b=a[2]}
          if(o+0>50){O+=o; B+=b*o}} END{printf "avg_batch=%.2f over %d ops\n", B/O, O}'
```

With the default bench (16 threads × depth 8 over 8 partitions, 2 connections per
partition, `--conn-inflight-cap` 16) expect **≈13.3 on TCP and ≈10.2–10.6 on UCX**
over two runs each, out of 16 in flight (each connection holds its whole depth
of 8). At a cap of 4 it was 8.00 (both connections' admitted ops in one append),
and 4 meant the loop launched on one connection's worth — the fragmentation that
`MIN_PIPELINED_BATCH` + `CoalesceWindow` in `partition_loop` exist to prevent
(with the tenants pinned away: 4.4 → 8.00, 56K → 74K 4K-write ops/s; see the
partition-server guide, "Natural batching").
Copy `ps.log` out before the next leg starts — `perf_check.sh` wipes
`/tmp/autumn-rs-logs/` between legs.

### A PS with more partitions than cores

A PS pins no thread: `--cpuset` confines the whole process, like `taskset`,
and the kernel places P-logs, P-ssts and the rest. With more partitions than
cores they share them; nothing refuses an open or a split. Whether the cpuset
is big enough shows on the PS's `/metrics` (service port + 500):
`autumn_ps_plog_threads`, `autumn_ps_plog_cpu_ratio` (P-log CPU time over wall
time, summed) and `autumn_ps_plog_run_delay_ratio` (time the P-logs were
runnable but waiting for a CPU, over wall time, summed). A run_delay ratio
climbing toward the CPU ratio means the P-logs queue for cores: grow the cpuset
or move partitions. Keep the cpuset on one NUMA node.

Manual verification (throwaway etcd + manager + 1 EN, replication 1, PS with
`--cpuset A,B` and `--port $PS`):

```bash
autumn-op --manager $MGR bootstrap --replication 1+0          # partition P
for at in 80 40 20 10; do autumn-op --manager $MGR split P --at-raw-hex $at --wait; done
p=$(pgrep -x autumn-ps)
for t in /proc/$p/task/*; do awk '/Cpus_allowed_list/{print $2}' $t/status; done | sort | uniq -c
# one line: every thread allowed exactly A,B
curl -s localhost:$((PS + 500))/metrics | grep autumn_ps_plog_
# autumn_ps_plog_threads 5, plus the two ratios
```

## Inode-lease + close-to-open coherence (in flight)

Multi-mount / multi-daemon coherence for `autumn-fuse` and
`autumn-ioring-daemon` runs through a JuiceFS-style inode lease served
by the manager. Design and invariants:
[`fs_lease_design.md`](fs_lease_design.md).

Landed so far:
- **Manager lease state** — manager state + 4 RPCs (`MSG_*_LEASE` /
  `MSG_POLL_INVALIDATIONS` = `0x46`–`0x49`), writer-lease etcd
  persistence under `inode_leases/<ino>`, TTL revoke loop.
- **Daemon lease acquire** — autumn-ioring-daemon Open acquires (and
  Close releases) a write/read lease per inode. `RING_VERSION 1→2`:
  the Open SQE's flags byte now carries `LEASE_MODE_READ` (1) /
  `LEASE_MODE_WRITE` (2). A v1 client (flags=0) is interpreted as
  WRITE — the safe default. Two concurrent writers on the same
  inode (different daemons OR different sessions of the same
  daemon) get `libc::EBUSY` on the second Open.
- **Invalidation long-poll** — long-poll invalidation channel.
  `MSG_POLL_INVALIDATIONS` blocks up to 10 s when the inbox is
  empty (manager parks a waker); a writer-close pushed by ANOTHER
  daemon fires the waker so the reader sees the event in ms, not
  via a retry tick. Daemon spawns a persistent
  `session_invalidation_poll_loop`; on transport error or overflow
  sentinel it wholesale-invalidates the session cache and drops its read
  leases, keeping its write leases (the heartbeat still renews them).
  Check: `cargo test -p autumn-manager --features fuse-tests --test
  fuse_lease_drain` (needs `toxiproxy-server` on `PATH`).
- **Lease versions across a manager restart** — each leader reserves 2^40
  lease versions before it serves, so a version handed out after a restart
  or failover is above every one before it (the PS's per-inode write fence
  is durable). Each election moves the key up by 2^40 (1099511627776):
  `etcdctl get autumn-rs/fs/lease_version_floor -w json | jq -r '.kvs[0].value' | base64 -d | od -An -tu8 --endian=big`.
  The leader logs `lease versions reserved for this term` with the base.
  Every manager must run a build that reserves: an older one ignores the key
  and starts versions at 1 again.
  Check: `cargo test -p autumn-manager --features fuse-tests --test
  fuse_lease_restart` (needs `etcd` on `PATH`).
- **Lease records written in order** — a write grant, a writer close and
  the TTL sweep of one inode take turns, and an expired writer is not renewed
  by its heartbeat, so a slow etcd write cannot land after a later one and
  leave `inode_leases/<ino>` naming a holder the leader no longer has (a
  failover would hand the inode to that record). Check: `cargo test -p
  autumn-manager --test lease_grant_stale_txn` (needs `etcd` on `PATH`, ~40 s).
- **Close-to-open coherence** — `OpenedExtents.lease_version` populated
  from the AcquireLease response; per-session `InvalidationMap`
  bumped by the poll loop. Read SQE arm calls `cache_is_stale`
  and on stale invokes `fuse_read::reload_extents` to re-fetch
  the inode meta + extent map before serving — close-to-open
  coherence end-to-end. **Phase 1 complete.**

Smoke-tests (no cluster boot required):

```bash
# Manager-side state machine + RPC contract.
cargo test -p autumn-manager --lib inode_lease

# Daemon-side lease helpers + two-daemon conflict / read-coexistence /
# version monotonicity / heartbeat round-trip.
cargo test -p autumn-manager --test ioring_lease_2

# Long-poll: writer-close wakes a parked reader in ms (3 tests; the
# idle-timeout case waits the full 10s LONG_POLL_WAIT — ~30 s total).
cargo test -p autumn-manager --test ioring_lease_3

# Close-to-open cache invalidation: per-ino floor bumps on
# WriterClosed; reader's stale-cache predicate flips; overflow
# sentinel surfaces (overflow test takes ~10 s for its 1025 cycles).
cargo test -p autumn-manager --test ioring_lease_4

# BUG-LEASE-2 storage fencing (needs built binaries; boots a cluster):
# Phase 1 — stale-epoch MSG_PUT rejected with CODE_FENCED; anonymous
# (inode_hint=0) writes bypass.
cargo test -p autumn-manager --test bug_lease_2_storage_fencing
# Phase 2 — the floor SURVIVES a PS kill -9 + restart, on both recovery
# paths (WAL OP_FENCE_BUMP replay; TableLocations.fence_floors checkpoint
# after a flush), and MSG_PUT_BULK (the fuse/ioring large-write path) is
# fenced too. Manual check: write at epoch 1 then 5 for one ino, kill
# the PS, restart, retry epoch 1 → must get CODE_FENCED.
cargo test -p autumn-manager --test bug_lease_2_phase2_persistence
```

Daemon manual exercise (against a real cluster):

```bash
# Start a one-node cluster (cluster.sh reset 1) then a daemon:
cargo run -p autumn-ioring --features daemon --bin autumn-ioring-daemon -- \
  --manager 127.0.0.1:9001 --socket /tmp/ring.sock --runtimes 1

# Two test apps each call IoRingClient::submit with
#   Sqe { opcode: Opcode::Open, lease_mode: SQE_LEASE_MODE_WRITE, ... }
# against the same path → second CQE.result == -libc::EBUSY.
```

Phase 1 is complete. Future work tracked under separate features:
- **fuse mount lease + cache invalidation** — autumn-fuse mount opt-in: open/release
  call lease::acquire/release; kernel attribute cache invalidated
  via `fuser::notify_inval_inode`.
- **Force-revoke / writer revoke** — force-revoke / writer revoke protocol so
  "another daemon needs to write NOW" doesn't have to wait for
  the current writer to close.

## Zero-copy model load

Serve a model that lives in autumn straight into GPU memory via the pinned
zero-copy read seam (`autumn.Fs.read_into`) + batched EN direct-read, at
≈Run:ai-Model-Streamer throughput. The loader pipeline: parse the safetensors
header → per tensor `read_into` a **CUDA-pinned** host buffer (double-buffered)
→ async H2D overlapped with the next read. Storage reads go direct to the extent
nodes (`autumn.Fs.connect(direct_read=True)`); descriptors resolve in ONE PS
round-trip per file (`MSG_GET_REDIRECT_MANY`), so the ~N-extent reads fan across
all ENs with the PS off the metadata path.

**Build note (UCX):** binaries + wheel need `--features ucx` for
`--transport ucx`. The wheel MUST be built `--skip-auditwheel` (bundling UCX
libs segfaults — UCX `dlopen`s its transport modules from the system install;
the client must link **system** UCX like the daemons).

**A/B vs Model Streamer (intra-host UCX, GPU host):**
```bash
# cluster bound to a RoCE NIC IP (NOT loopback), UCX positive-list env from cluster.sh
AUTUMN_BIND_HOST="[<roce-nic-ip>]" AUTUMN_TRANSPORT=ucx \
  AUTUMN_DATA_ROOT=/data/autumn-ucx bash cluster.sh start 4
# client pinned to the SAME NIC (both-ends rule); run on a free GPU
AUTUMN_MANAGER="[<roce-nic-ip>]:9001" CUDA_VISIBLE_DEVICES=<free-gpu> \
  UCX_TLS=rc_mlx5,ud_mlx5,tcp,self UCX_NET_DEVICES=mlx5_1:1 \
  python3 remote_bench.py     # set_transport("ucx"); upload model; A/B loader vs runai
```
Expect: **byte-exact** (loaded tensors == safetensors ground truth) and autumn
EN-direct at ~80% of Model Streamer's local-page-cache number at K≈4 (the fair
comparison is vs Model-Streamer-from-remote-storage, where autumn/RDMA wins).
The `Fs.read_into` seam alone (no GPU) is checkable headless with a `bytearray`
dest: `fs.read_into(ino, off, memoryview(buf))` byte-equals `fs.read(ino, off, n)`.

## Enabling authz

**Deploy layer = ON by default.** Both deploy paths arm
data-plane authz automatically. **Protect-everything:** with a signing key
present, EVERY keyed op requires a token — there is no protected-prefix list and
anonymous connections are denied; a principal's credential grants key prefixes —
a whole namespace (`fs/`) or an in-namespace sub-prefix (`mem/app/`). Keys are
`{ns}/…`.

- **`deploy/baremetal/autumn-deploy start`** generates a signing key once (reused
  across re-deploys — rotating invalidates every credential),
  distributes the key to every manager host, and after bootstrap mints per-family
  principal credentials to `~/.autumn-deploy/authz/*.cred`. Clients pass
  `--credential-file ~/.autumn-deploy/authz/fs.cred` (the principal name is read
  from the file — no `--principal` flag).
- **k8s** (`deploy/overlays/vke/deploy.sh`) generates the `autumn-authz` Secret
  (signing key) once and the manager StatefulSet mounts it (the
  signing key alone arms protect-everything — no prefix list). Mint a client
  credential + Secret with the manual steps below.
- **Escape hatch:** `AUTUMN_AUTH_DISABLE=1` (both paths) runs authz-OFF — for
  local debugging. The dev/test harness (`cluster.sh`, `scripts/*_chaos.sh`)
  never sets `AUTUMN_AUTH_*`, so it is authz-OFF unconditionally.
- **Native clients** all take `--credential-file <path>` (NO `--principal` —
  the principal identity travels IN the file): `autumn-fuse`,
  `autumnfs`, `autumn-client`. The file is the two-line `principal:`/`credential:`
  form `autumn-op principal-create` prints (or `<name>\n<hex>`); the hex decodes
  to the raw bytes the manager hashed.

### Manual runbook (custom principals, or a non-deploy setup)

Client-side wiring: PyO3 `Client.connect(scope=,principal=,credential=)` and
`BatchClient(scope=,principal=,credential=)`. Everything below is the OPERATIONAL enablement
for a principal the deploy layer did NOT auto-provision. Gradual-rollout axis:
credentials first (steps 1–2 are harmless with authz off), then the signing key
(step 3), which arms enforcement of every keyed op at once.

```bash
# 1. one-time: signing key (KEEP SAFE; k8s: put it in a Secret)
autumn-op gen-signing-key > /secrets/autumn-auth-signing.key

# 2. create the PRINCIPAL. Grant an in-namespace sub-prefix (`mem/app/`) or a
#    whole namespace (`fs/`). principal-create prints the two-line
#    principal:/credential: form (shown ONCE) — redirect it STRAIGHT to the
#    credential file (the reader parses the name + hex from it):
autumn-op --manager $M --cluster-secret-file /secrets/cluster.secret \
    principal-create --principal app --grant "mem/app/" \
  > /secrets/app.cred

# 3. ARM: manager gets --auth-signing-key-file (or env
#    AUTUMN_AUTH_SIGNING_KEY_FILE via entrypoint). PROTECT-EVERYTHING: the signing
#    key alone arms enforcement of EVERY keyed op — there is no
#    protected-prefix list. Restart manager; the PS and EN pick it up at their next authz fetch (`--config-poll-secs`, 30 s) — until then they do not enforce.

# 4. Verify mint works. Minting is signed with that key, so before step 3 it
#    is refused ("authz not enabled / no signing key on this manager").
#    --principal is optional: the file names it. Enforcement is already live;
#    if this fails, roll back (below) before clients start failing.
autumn-op --manager $M --cluster-secret-file /secrets/cluster.secret \
    mint-token --credential-file /secrets/app.cred   # must print a token

# 5. Verify enforcement: a credential-less write must fail, while the scoped
#    client carrying app.cred succeeds.
printf data > /tmp/authz-value
autumn-client --manager $M --namespace mem put x /tmp/authz-value  # expect PermissionDenied
autumn-client --manager $M --namespace mem/app \
    --credential-file /secrets/app.cred put x /tmp/authz-value
```

Rollback = remove the signing-key flag and restart the manager (no key =
authz fully off). Failure modes: a client missing its credential fails ALL
mem/ writes with PermissionDenied (terminal, not retried — that is the
fail-loud design); manager unreachable > TTL−300 s → token renewal fails →
writes rejected until the manager returns (enforcement adds a
grace-window=TTL availability dependency of the data plane on the manager).

## G2 — power-loss crash-consistency test (LazyFS, single machine)

Verifies the core durability contract: **every write the client got an ACK for
survives a power loss**, and recovery never fails-loud spuriously or serves
garbage. Uses [LazyFS](https://github.com/dsrhaslab/lazyfs) — a userspace FUSE
filesystem that only persists `fsync`'d data; its `clear-cache` command drops
everything not yet fsync'd = a power cut at that instant. **No kernel module**
(dm-log-writes needs `dm_log_writes.ko`, absent in this container; LazyFS is the
userspace equivalent). autumn's io_uring write path works on the FUSE backend.

Scope: single-node **RF1** cluster with the data plane (`AUTUMN_DATA_ROOT`) on
the LazyFS mount; **etcd is bind-mounted OFF LazyFS** so only autumn's
data-plane durability is under test (control plane assumed on its own durable
quorum).

```bash
# 0. Build LazyFS once (userspace; needs libfuse3-dev + cmake + g++):
git clone --recurse-submodules https://github.com/dsrhaslab/lazyfs /opt/lazyfs
(cd /opt/lazyfs/lazyfs/libs/libpcache && ./build.sh)   # or cmake -S . -B build && cmake --build build
(cd /opt/lazyfs/lazyfs/lazyfs        && ./build.sh)    # → /opt/lazyfs/lazyfs/lazyfs/build/lazyfs
# The harness auto-discovers /opt/lazyfs, ~/lazyfs, ../lazyfs; else set LAZYFS_BIN=<path>/lazyfs.

# 1. Run it (quiesced crash — coalescer settles, then power loss):
cargo build --release --workspace          # harness uses release binaries
scripts/g2_crash_consistency.sh
#   → "VERDICT: PASS — every acked write survived power loss, recovery clean" (exit 0)

# 2. Immediate crash (power loss the instant after the last ACK — probes any
#    ACK-before-fsync window; PASS proves synchronous durability):
scripts/g2_crash_consistency.sh --immediate

# Knobs: --keys N (small values) / --big M (2 MiB values; default 70×2 MiB =
# 140 MiB > MAX_WAL_GAP 128 MiB → forces a rotate+flush so recovery exercises the
# checkpoint-reload path too, not just WAL replay). Env: LAZYFS_BIN, G2_WORK,
# N_SMALL, N_BIG, BIG_BYTES, QUIESCE, MAX_WAL_GAP.
```

What it asserts, per acked key: present after restart + byte-identical (SHA-256);
counts LOST (acked→gone) and CORRUPT (bad bytes); scans PS/EN/manager logs for
fail-loud markers (`WAL-FAILSTOP`, `invalid meta`, `StaleVpOffset`,
`failed to open partition`, `panicked`). PASS requires 0 lost, 0 corrupt, and
`survived == acked`. The `open_partition: ready … tables=N sst_readers=N
max_seq=…` line in the summary confirms which recovery path ran (tables>0 =
checkpoint reload + WAL replay; tables=0 = pure WAL replay).

Mechanism has teeth: a standalone check (write file A with `fsync`, file B
without, `clear-cache`) shows A survives and B is dropped — so a real durability
gap would surface as LOST keys.

## Chaos: reading a failure

`cargo test -p autumn-manager --test system_chaos -- --ignored --nocapture`

The report is ordered so the first thing you read is the cause, not the symptom:

1. **`write failures by reason`** — the workload's rejected writes, tallied by
   PS code / RPC error / routing failure. A chaos workload MUST tolerate failed
   writes (that is the point of a nemesis), so the tally is the only thing that
   separates "faults are landing" from "nothing works".
2. **`WORKLOAD ACKED NOTHING`** — a hard failure. Every per-key invariant is
   vacuous over an empty expectation set, so a run that wrote nothing would
   otherwise report `0 mismatches, 0 not_found` and pass. If you see this, fix
   the workload before reading anything below it.
3. **A refusal naming one frame's payload ceiling** — a reply grew past what
   the wire format can express, so the server refused instead of building it.
   Five producers can reach that size and each says so in its own words: an
   extent-node read (`read range exceeds one frame's payload ceiling`), a
   `get_many` batch (`batch of N keys exceeds one frame's payload ceiling`), a
   `copy of N bytes from extent E`, a redirect-many item (`batch reply reached
   one frame's payload ceiling`), and the group-commit append, which splits
   silently and launches the remainder as the next batch. Most degrade rather
   than fail — the batch retries per key, a declined redirect item is proxied,
   the append splits — but the extent-node read refusal IS an error to a caller
   that does not chunk (`ec_read_full`, `ec_reconstruct_shard_subrange`), which
   is the honest outcome: those bytes cannot be delivered in one frame. In a DEBUG
   build the encoder also panics on such a frame (`frame payload is N bytes,
   over the wire format's ...`); in release it does not, deliberately, because
   `panic = "abort"` would turn a remote request into a dead node.
4. **`WHY:`** — a scan of the EN subprocess logs for fail-loud markers
   (`WAL-FAILSTOP`, `META-FAILCLOSED`, quarantine, stale VP, refused EC
   completions, superseded attempts, disk-offline, supervised-loop panics).
   **Their absence is the sharper finding**: the invariant broke while every
   layer believed it was fine. `logs:` gives the directory to dig in.
5. The per-category counts and samples.

Manager and PS run in-process, so their tracing goes to the test's own stderr,
not to `logs:`. Only EN logs are on disk — which is the right surface anyway,
since recovery, EC conversion, quarantine and disk health all live there.

**The trap this encodes.** For five weeks `system_chaos` reported "all
invariants OK" while every single write was rejected with `NamespaceUnknown`:
Layer-A namespace validation is always on, and the test wrote bare keys. Nothing
caught it because a chaos workload is *supposed* to swallow write failures —
the tolerance that makes it correct is what let 100% rejection look like normal
nemesis pressure. Ordinary tests were never exposed: `support::ps_put` retries
and then panics, so a rejected write fails loudly there.

Chaos keys are `mem/{b|q}{kid:06}` under the built-in `mem` namespace
(`CHAOS_NS`). A new chaos scenario must namespace its keys or every write will
be refused.

## EC copy-on-write conversion — what an operator sees

Conversion is **copy-on-write**: the EN stages each shard as an ADDITIVE file
`extent-{id}.shard{i}` and never touches the `.dat` it was derived from. The
manager's layout flip is the **only** commit point. There is no per-node commit,
no rename, and no intent marker — an abandoned attempt costs a delete of files
no reader is pointed at.

**The life of one conversion**, and where to look if it stalls:

| stage | evidence |
|---|---|
| dispatched | manager marker in `autumn-op extent-health` / `list-ec-inflight-markers` |
| staging | EN log `EC 2PC phase 1 (prepare) complete ... (chunked)` |
| staged | EN log `EC shards staged on every target; awaiting the manager's layout flip` |
| committed | `autumn-op ops list --kind ec` → `succeeded`; the marker drains |
| reclaimed | EN log `reconcile: reclaimed the pre-conversion .dat; this node now serves its shard` |

The last row lags the others by up to one reconcile sweep (5 min, or immediately
on EN restart). Until it happens the extent occupies BOTH forms — that is
expected, not a leak.

On-disk, a converted extent should end as exactly one `extent-{id}.shard{i}` per
member, each `sealed_length / K` bytes, plus `.meta`. The coordinator also keeps
`extent-{id}.ec.prepared` (16 bytes), which records which ATTEMPT staged the
shards; it is current-scheme state, not residue.

```bash
find <data-dir> -name 'extent-<ID>.*' -printf '%f(%s) '
```

**If a conversion never reaches `succeeded`:**

- `ops list --kind ec` shows the reason a conversion ended. An attempt that fails
  (a participant refused or was unreachable, the coordinator's own step failed)
  is cancelled at once and the op closes FAILED naming the node (`node N failed
  the conversion: …`); the auto-policy proposes the extent again with a fresh
  attempt. A dispatch that only times out keeps the attempt.
- A marker any of whose targets — or the disk a target writes to — is
  suspected, in maintenance, fenced or gone is cancelled automatically (the op
  closes FAILED naming the node or disk), and the auto-policy proposes the
  extent again once its replicas are usable. A `Suspend` node (not heard from
  yet) is waited for, not cancelled on.
- A completion report from a superseded attempt is refused by nonce, logged as
  `ec_done is from a DIFFERENT conversion attempt than the live marker`. That is
  the system protecting itself, not an error to chase.

## EC copy-on-write conversion — cross-host verification

`scripts/ec_crosshost_verify.sh` exercises the whole EC conversion line across
TWO machines, which is the shape single-host loopback cannot test: manager + PS
+ EN0 on this host, EN1 + EN2 on the peer, `2+1` erasure coding, so shards fan
out over the network and two of the three holders are remote.

```bash
# Build first — the peer's release tree is whatever was last shipped to it, and
# the script scp's these binaries over.
cargo build --release --workspace
bash scripts/ec_crosshost_verify.sh
```

Edit `L6` / `R6` at the top for your two hosts; the peer is reached through
`.claude/skills/remote-autumn/remote-autumn.sh` (ssh -p 2222).

What it asserts, in order: the conversion op reaches `succeeded`; all 8 × 64 KiB
values read back byte-identical **after the layout flip**; every EN restarts and
its reconcile reclaims the pre-conversion `.dat` **on both hosts**; the same
values still read back byte-identical with **no `.dat` anywhere in the cluster**.
PASS requires all four. A converted extent should end as one `.shard{i}` per
node at `sealed_length / K` bytes.

Three traps this script exists to encode, all of which cost a run to find:

- **`--listen` defaults to `0.0.0.0`** on both the EN and the PS — the IPv4
  wildcard, which refuses the IPv6 address they advertise. Pass `--listen <v6>`
  explicitly on every node or the manager's `df` never connects and every node
  sits `Suspected`. (When `df` has never once succeeded, `list-nodes` prints
  `HB_AGO`/`SUSP_AGE` in the hundreds of seconds — that is an absent baseline,
  not stale state. Don't chase it.)
- **A peer data dir keeps its `cluster_id`.** Re-running against a fresh manager
  makes `autumn-op format` refuse to join a different cluster — that guard
  working, not a failure. Wipe the peer dirs between runs.
- **Restart the ENs only.** The PS serves the reads being verified; killing it
  makes the final check fail as `connect PS … failed`, which reads like a
  data-plane break and is not one.

## FS schema version

The current filesystem schema is v4 (segmented files and content generations).
Clients refuse a mismatched stamp or a populated tree with no stamp. The
one-off v3 → v4 converter has been removed from the current build and image;
legacy data needs the migration procedure from the corresponding historical
release before a current client can mount it. Never overwrite the stamp to
bypass this check.

## S3 gateway — reading and writing autumn over S3

`autumn-s3` is an unauthenticated S3 endpoint over the `fs/` tree. It began so
SGLang and FreeToken — neither of which has a loader plugin seam — could use
their built-in `--load-format runai_streamer` to stream weights concurrently,
with no engine patches; it now also writes (PUT, Copy, Delete, DeleteObjects,
multipart, conditional writes and reads), so a stock S3 client such as
LanceDB's can keep its data on autumn. `aws s3` and every other S3 client work
against it too. An object written through it is a file to a fuse mount and to
`autumn.Fs`, and the other way round.

Buckets are the first level under `fs/`: `s3://models/llama/x.safetensors` is
autumn `fs/models/llama/x.safetensors`.

```bash
# 1. Run it next to the engine (per-GPU-node sidecar keeps the RDMA hop long
#    and the HTTP hop on loopback).
autumn-s3 --manager 127.0.0.1:9001 --port 9100 \
          --credential-file /secrets/fs.cred      # omit when authz is off
# --workers N (default 8, capped at core count) — accept threads, SO_REUSEPORT.
# One thread caps an AWS-CRT client at ~40% of the read path; the knee is at 4.

# 2. Smoke it with the aws CLI. The credentials are DUMMY — the gateway never
#    looks at the Authorization header — but the SDK's credential chain runs
#    BEFORE the request is sent, so they must be set to something.
export AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x AWS_EC2_METADATA_DISABLED=true
aws --endpoint-url http://127.0.0.1:9100 s3 ls
aws --endpoint-url http://127.0.0.1:9100 s3 ls s3://models/llama/
aws --endpoint-url http://127.0.0.1:9100 s3 cp s3://models/llama/config.json -

# Bucket existence probe. A pre-created `fs/models` directory answers 200;
# a missing bucket answers 404.
curl -s -o /dev/null -w '%{http_code}\n' -I http://127.0.0.1:9100/models

# A page counts both files and child prefixes toward max-keys. Keys come back
# in S3 byte order (`d.txt` before `d/`, `d0` after it) with no directory-size
# or walk cap; each page resumes from its token rather than re-walking the tree.
# When testing a large directory, follow NextContinuationToken until
# IsTruncated is false and compare against the expected key set.
aws --endpoint-url http://127.0.0.1:9100 s3api list-objects-v2 \
    --bucket models --delimiter / --max-keys 2

# 3. Ranged read (what the streamer actually issues) must answer 206 with an
#    exact Content-Range:
curl -s -D- -o /dev/null -H 'Range: bytes=0-7' \
     http://127.0.0.1:9100/models/llama/model-00001.safetensors
#   → HTTP/1.1 206 Partial Content
#     content-range: bytes 0-7/<size>

# 4. Verify the streamer path itself (no GPU needed). This is the exact code
#    SGLang's runai loader runs:
pip install runai-model-streamer-s3     # the AWS-SDK plugin; NOT in the base package
env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy \
    AWS_ENDPOINT_URL=http://127.0.0.1:9100 \
    AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x AWS_EC2_METADATA_DISABLED=true \
    python3 -c "
from runai_model_streamer import list_safetensors, SafetensorsStreamer
with SafetensorsStreamer() as st:
    st.stream_files(list_safetensors('s3://models/llama'))
    print(sorted(n for n, _ in st.get_tensors()))"

# 5. Serve with SGLang (same env; the proxy unset matters here too):
export AWS_ENDPOINT_URL=http://127.0.0.1:9100
python -m sglang.launch_server --model-path s3://models/llama \
       --load-format runai_streamer
# vLLM takes the same URL; on vLLM prefer --load-format autumn (native, RDMA
# zero-copy) unless you are A/B-ing the two.
```

### Writing through the gateway

```bash
export AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x AWS_EC2_METADATA_DISABLED=true
E=http://127.0.0.1:9100
# The bucket is a first-level directory and must exist; the gateway does not
# create buckets. Parent directories under it are created on write.
autumnfs --manager 127.0.0.1:9001 mkdir /tables
aws --endpoint-url $E s3 cp ./data.lance s3://tables/t1/data/0.lance     # multipart above 8 MiB
aws --endpoint-url $E s3api put-object --bucket tables --key t1/_versions/1.manifest \
    --body m.bin --if-none-match '*'          # 412 PreconditionFailed if it exists
aws --endpoint-url $E s3 rm s3://tables/t1/data/0.lance
```

### LanceDB acceptance workload

The maintained example uses the stock, pinned Python client and exercises
create, append, multipart, reopen, vector search, filtering, concurrent
commits, optimize/vacuum, byte verification and drop:

```bash
cd examples/lancedb-s3
uv sync
uv run python workload.py http://127.0.0.1:9100 tables smoke
# → WORKLOAD OK
```

See [`../examples/lancedb-s3/README.md`](../examples/lancedb-s3/README.md) for
the request-trace recorder and compatibility contract.

Each gateway runs one reclaimer thread with its own client identity. A
DELETE, an overwrite or an Abort only records what to reclaim and returns; the
reclaimer deletes the bytes right away, off the serving workers' locks. A file
deleted while a GET streams it is reclaimed when that GET's pin is released,
about 2 s after the GET ends. Every `--sweep-interval-secs` (default 30; `0`
turns off only these periodic sweeps) the same thread also takes over the
publishing sessions of a gateway that died and finishes or undoes their
half-done writes, and reclaims whatever the hand-offs could not: aborted or
completed uploads, unlinked files another client was still holding, and
segment garbage. Every gateway may run them.

A CompleteMultipartUpload retried after it succeeded (a lost reply) gets the
same 200 and ETag for an hour, as from S3, even if the object has since been
replaced or deleted.

Statuses a client should expect beyond the usual: `412 PreconditionFailed` for
a failed `If-None-Match: *` / `If-Match`; `409 ConditionalRequestConflict` when
a mount has the file open for writing, or another Complete of the same upload
is running; `503 SlowDown` for a GET while a mount writes the file in place
(SDKs retry it). Not supported: UploadPartCopy, ListParts,
ListMultipartUploads, versioning, ACLs, object metadata (`Content-Type`,
`x-amz-meta-*` are not stored), virtual-host addressing (use path-style, which
is what `--endpoint-url` selects), and SigV4 verification. Anything else
answers `NotImplemented`.

**Verify the write APIs with a real SDK.** Needs a running cluster, a gateway
and an existing bucket; `uv` fetches boto3. Every API above is checked, with
the error codes the SDK parses, plus a racing-creators round that must leave
exactly one winner (the pattern LanceDB commits with):

```bash
autumnfs --manager 127.0.0.1:9001 mkdir /s3check
autumn-s3 --manager 127.0.0.1:9001 --port 9100 --sweep-interval-secs 10 &
env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy \
    uv run --with boto3 python scripts/s3_write_check.py --endpoint http://127.0.0.1:9100 --bucket s3check
#   → "all checks passed"; --race-only runs just the racing round
```

It includes the retried Complete: after success, again with `If-None-Match: *`,
and after the object was replaced, each time the original ETag.

**Verify a delete does not hold up the worker.** One worker, so every request
shares its lock; the periodic sweeps are pushed out of the way, so only the
hand-off can reclaim:

```bash
autumnfs --manager 127.0.0.1:9001 mkdir /stall
autumn-s3 --manager 127.0.0.1:9001 --port 9100 --workers 1 --sweep-interval-secs 3600 &
NO_PROXY=127.0.0.1 no_proxy=127.0.0.1 \
    uv run --with boto3 python scripts/s3_stall_check.py --endpoint http://127.0.0.1:9100 --bucket stall
#   → DELETE of a 1000 MiB object and Abort of a 200-part upload in a few ms, and
#     "PASS worst HEAD during the deletes" (local 3-EN cluster: < 3 ms, idle 1 ms;
#     with the deletes under the lock the HEAD waited 30 ms / 58 ms)
AC=(autumn-client --manager 127.0.0.1:9001 --namespace fs)
for p in $'\x04rmtomb/' $'\x04mpu/' $'\x04mpa/' $'\x04pend/'; do "${AC[@]}" ls --prefix "$p" --limit 1000 | wc -l; done
#   → all 0 a second later: the reclaimer deleted everything without a sweep
```

**Measure PUT throughput, and see where a PUT spends its time.** Concurrent
PUTs are bounded by the partitions the `fs/` lanes live on, so look at those
first; a single stream is bounded by receiving its body plus writing its last
8 MiB unit.

```bash
autumn-op --manager 127.0.0.1:9001 info        # how many partitions own fs/[0x03][lane]?
# On a fresh cluster, BEFORE loading data (a data-bearing partition refuses):
autumn-op --manager 127.0.0.1:9001 --wait presplit --namespace fs --lanes 24 --parts 4 \
    --cluster-secret-file $DATA_ROOT/cluster.secret
autumnfs --manager 127.0.0.1:9001 mkdir /bench
RUST_LOG=info,autumn_s3::write=debug autumn-s3 --manager 127.0.0.1:9001 --port 9100 > s3.log 2>&1 &
for c in 1 8 32; do
  NO_PROXY=127.0.0.1 python3 scripts/s3_put_bench.py --conc $c --count $((c<8?16:c*8)) --prefix c$c
done
#   local 3-EN, 4 lane partitions: conc 1 ~270 MiB/s, conc 8 ~430, conc 32 ~550
#   (with fs/ in ONE partition conc 8 stays ~290 — the partition, not the gateway)
grep 'PUT breakdown' s3.log | tail -3
#   begin/flush/lock/finish/publish/body in ms; for 16 MiB the metadata steps
#   (begin + finish + publish) total about 1 ms, the rest is body and data.
```

The per-PUT win of the streaming pipeline shows only against the previous
binary on the same cluster, alternated (ABAB), each gateway stopped by its PID:
SO_REUSEPORT lets a stale gateway on the same port keep taking connections,
and `pkill -x autumn-s3` does not match a binary renamed `autumn-s3.old`.

**Verify a gateway crash leaves nothing behind.** Start a large PUT (and an
UploadPart) that sends slowly, `kill -9` the gateway mid-body, restart it, and
wait past the 30 s session lease plus one sweep. The gateway log shows
`recovered dead publishing sessions`; the object was never published (HEAD
404); the upload is still usable; and a scan of the `fs/` superblock records
(`[0x04]pend/`, `[0x04]mpa/`, `[0x04]segc/`) and data keys (`[0x03]`) is back to
what it was before the PUT started — the half-written body is deleted, not
just forgotten.

**Verify a GET keeps its object.** Stream a 40 MiB GET slowly and, from other
connections, DELETE the key and PUT a new object at it mid-body. The GET must
still return all 40 MiB byte-identical; the deleted data is reclaimed a few
seconds after the GET ends (its `[0x04]rmtomb/` record disappears).

Multipart and conditional publish live in `autumn-fs`; their system tests need
no gateway and no libfuse (the run sleeps ~33 s so a session lease expires):

```bash
cargo test -p autumn-manager --test system_multipart --test system_publish -- --include-ignored
```

`system_multipart` proves CompleteMultipartUpload is metadata-only: it deletes
every part body before Complete, then checks that no `[0x03]` data key changed
and that the file's map names the parts' own objects. It also covers the
Complete/Abort race, a part that lands after Abort, a publish whose outcome is
unknown (left for recovery, not undone), and a dead session's part and
Complete.

Gotchas:
- **`HTTP_PROXY` silently swallows the streamer.** The Run:ai streamer's S3
  backend is aws-c-s3 (the CRT client), which honours `HTTP_PROXY`/`HTTPS_PROXY`
  and **ignores `NO_PROXY`** — verified: with `NO_PROXY` already listing
  `127.0.0.1`, every read still went to the proxy and came back
  `AWS_ERROR_S3_INTERNAL_ERROR` / "File access error", with no socket ever
  opened to the gateway. UNSET the proxy variables for the engine process:
  ```bash
  env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy python -m sglang.launch_server ...
  ```
  The boto3-side listing is unaffected (it does honour `NO_PROXY`), so the
  symptom is "the model directory lists fine, then every weight read fails".
- **The CRT sends absolute-form request lines** (`GET http://host:port/bucket/key`)
  rather than origin-form. The gateway handles both; a reverse proxy in front of
  it may not.
- **path-style only.** A client configured for virtual-host addressing resolves
  `bucket.host` and never reaches the gateway.
- **An undelimited listing walks the tree.** `aws s3 ls --recursive` from a
  bucket root visits every directory below it, a page at a time; prefer a
  prefix.
- **Listing costs one inode lookup per key** (for size/mtime). Fine for a model
  directory; not a directory-crawler substitute.

## Rolling the extent nodes while recovery is running

A rolling restart of the EN StatefulSet interrupts any EC rebuild whose peers
sit on a pod being replaced. The rebuild reads `data_shards` peers per 64 MiB
stripe, so a peer that goes away mid-shard costs the attempt:

```
recovery task failed, retrying in 10s extent_id=69 attempt=1
  error=EC recovery: only 3/4 shards available for extent 69
        at [3087007744, 3154116608) of 4294996716:
        shard 1 (node 7 at 192.168.2.65:9131): Connection refused (os error 111)
```

That is the roll doing it, not a fault — `Connection refused` on a shard port
whose pod is `Terminating` is the expected shape. Recovery retries ten times,
ten seconds apart, so the rebuild survives a roll; it just pays for it. Since a
rebuild restarts from zero (there is no resume — see the ledger for why), the
cost is the whole shard, which on a full extent is `sealed_length / K`, GiB.

So: check for active recovery before rolling, and prefer to let it drain.

```bash
kubectl -n autumn exec autumn-manager-0 -- \
  autumn-op --manager 127.0.0.1:9001 ops list --active
```

Empty, or nothing with `recovery`, means a roll costs nothing. If a rebuild IS
running and the roll cannot wait, it is safe — just expect the affected extents
to start over and the roll to be followed by several minutes of repair.

Update order is EN first, then manager: the EN carries the recovery logic and a
mixed pair handshakes fine as long as both binaries were built from commits
carrying the SAME `WIRE_VERSION` (a version bump is a stop-the-world
roll instead — see the wire lockstep note). Check with
`grep WIRE_VERSION crates/rpc/src/lib.rs` on both commits; nothing computes a
fingerprint to check it for you.
`podManagementPolicy: Parallel` on the EN set affects scaling only; updates
still go one pod at a time, highest ordinal first.

### Verifying a recovery fix actually took

Watch the manager stop refusing, per extent, rather than trusting the pod count:

```bash
# Which extents are being refused, and how often (should go to zero):
kubectl -n autumn logs autumn-manager-0 --since=60s \
  | grep -oE "extent [0-9]+ already exists" | sort | uniq -c

# The EN side of the same moment:
kubectl -n autumn logs autumn-en-6 --tail=200 | grep -i require_recovery

# And that the rebuild REPORTED done, rather than just leaving the active list:
kubectl -n autumn exec autumn-manager-0 -- \
  autumn-op --manager 127.0.0.1:9001 ops history | grep recovery
```

The last one matters: an op leaving `ops list --active` proves only that it
stopped, not that it succeeded. `ops history` says `succeeded` and names the
node the slot landed on.


## SST format and deletion-triggered compaction

The current PS reads only MetaBlock v2, which records entry and tombstone
counts. Older formats are rejected with the actual and supported versions.
The one-off v1 → v2 converter has been removed from the build and image.
Legacy data needs the conversion procedure from the corresponding historical
release before a current PS can open it.

Each PS schedules a major compaction when SSTs hold >= 10 000 tombstones
and tombstones are >= 30% of entries, checked every
`--deletion-compact-check-secs` (default 300). The log reports
`tombstones reached the deletion-trigger rule; scheduling a major compaction`
with the counts. Verify with:

```bash
cargo test -p autumn-manager --test system_deletion_triggered_compaction
```

## Compio runtime upgrade verification

Build the workspace and standalone Python binding with Rust 1.95 or newer.
Compio 0.19.2 and cyper 0.9 must be resolved together; do not mix the old cyper
family into a process using the new runtime. Check both dependency trees:

```sh
cargo tree -i compio
cargo tree --manifest-path python/Cargo.toml -i compio
cargo check --workspace --all-targets --features autumn-server/ucx
cargo check --manifest-path python/Cargo.toml --features ucx
cargo test --workspace --lib --features autumn-server/ucx -- --test-threads=1
cargo test -p autumn-transport --features ucx --test zerocopy_tcp -- --test-threads=1
AUTUMN_TEST_ZEROCOPY=1 cargo test -p autumn-stream --test prepared_append
cargo build -p autumn-server --bins
cargo test -p autumn-manager --test system_fuse_read --test system_fuse_eof_clobber \
  --test system_fuse_flush_error_sticky --test system_fuse_release_best_effort \
  -- --test-threads=1
```

For UCX, run prepared_append with `--features autumn-rpc/ucx` and
`AUTUMN_TEST_UCX_BIND='[<RoCE-IP>]:0'`, plus the deployment's UCX_TLS and
UCX_NET_DEVICES. This validates actual disk bytes and append offsets.
The zerocopy option does not change UCX sends.

`autumn-ps --tcp-zerocopy-min-bytes N` opts replica append TCP sends into
zerocopy at N complete-frame bytes. Every star-replicated append is a prepared
send, so N alone decides which appends qualify — it is the only size cut on
this write path. Default 0 disables it and is the rollback switch. Send completion and buffer release are distinct; the writer waits for
both before reuse. No wire or persisted-format version changes are involved.
Restart with 0 to return to ordinary sends. Keep the old binaries and lockfiles
for a dependency rollback; stop PS and wait for drain before stopping EN/manager.

For CPU comparisons, core_path accepts `AUTUMN_PERF_PIDS=/path/pids.json`, a
JSON object mapping process labels to numeric PIDs. Snapshots bracket only the
timed, drained operation window after independent warmup; derive CPU seconds/GiB
from completed ops times value size. These process counters exclude independent
kernel workers, so do not describe them as whole-machine CPU efficiency.

The isolated receive/scheduler experiment is:

```sh
cargo bench -p autumn-transport --bench compio_features -- ordinary default 1048576
cargo bench -p autumn-transport --bench compio_features -- managed default 1048576
cargo bench -p autumn-transport --bench compio_features -- multi default 1048576
cargo bench -p autumn-transport --bench compio_features -- poll-first default 1048576
cargo bench -p autumn-transport --bench compio_features -- ordinary single 1048576
cargo bench -p autumn-transport --bench compio_features -- ordinary defer 1048576
```

The experiment uses CPUs 40/42 and 512 MiB per warmup/measured transfer. It
reports combined process user/system CPU and receiver-confirmed transfer time.
It does not configure server runtimes. SQPOLL is available only as an explicit
`sqpoll` experiment; its kernel-thread CPU must be measured separately before
making any efficiency claim. Compio's ordinary receive already applies adaptive
poll-first internally, so include it in the pure-upgrade comparison.


### Controlled runtime comparison after compio migration

Use the fixed-work harness in `perf/controlled_validation/README.md` when
validating CPU efficiency and partition scaling. It documents the H200-1 test
layout, archived baseline restoration, host perf/tracefs requirements, synchronized
measurement windows, three rotated repetitions and separate diagnostic runs.
The older two-second core_path samples are observations, not this acceptance.

Build `controlled_path` with identical source and Rust 1.95 against each runtime
version. Validate the recorded topology and per-partition operation counts before
using a result. Every successful trial saves evidence, stops its services and
reclaims its marked dataset; a failed trial remains for diagnosis. Archive/commit
results before removing the final source/build tree. Do not remove the archived
baseline evidence until all comparison work is complete.


## Receive-copy accounting per link

Answers "which process copies a value, how many times, and where": per receiving
process, TCP kernel copy (`skb_copy_datagram_iter`), UCX Stream unpack (memcpy
returning into libucp/libuct) and application memcpy resolved to its Rust call
site. Run on H200-1 inside `dongmao-autumn`; details in
`perf/receive_copies/README.md`.

```sh
# tracefs inside the container (unmount when the task ends)
mountpoint -q /sys/kernel/tracing || mount -t tracefs nodev /sys/kernel/tracing
# binaries: bin/<base|new>/{autumn-*,controlled_path} under /data08/autumn-receive-copies
cargo +1.95.0 build --release -p autumn-server --features ucx --bins --bench controlled_path
python3 perf/receive_copies/copytrace.py --version new --transport tcp --repeat 1
python3 perf/receive_copies/copytrace.py --version new --transport ucx --repeat 1
python3 perf/receive_copies/analyze.py /data08/autumn-receive-copies/results > summary.json
# throughput/CPU: untraced windows of several seconds, ABBA order
python3 perf/receive_copies/copytrace.py --long --version base --transport tcp --repeat 1
python3 perf/receive_copies/cpu.py /data08/autumn-receive-copies/results > cpu.json
```

Expected after the decoder read-window change, per logical byte on each
receiving process: transport copy 1.0 (TCP kernel or UCX unpack — a registered
`memh` does not remove the UCX one); extent-node application copy ≈0 at 64 KiB
and 8 MiB (≤0.06x per replica), ≈0.45x per replica for 1 MiB appends on TCP
(`try_decode` reserving an append whose start arrived in the previous 512 KiB
window); partition server and client application copy = the value prefix already
in the 64 KiB window (1.0x for a 64 KiB value, 0.06x at 1 MiB, 0.008x at 8 MiB),
plus ~0.15x of non-value `partition_loop` moves and ~0.14x benchmark buffer
handling at 64 KiB that are the same in both builds.
A `FrameDecoder::feed` frame under `handle_connection`, `handle_ps_connection`
or `read_loop` in the analyzer's `sites` means a receive loop copies again.

`ClusterClient::get` (bench mode `get`, add `--sizes 4096,65536,1048576,8388608
--only 8388608:get,...`) is served by `MSG_GET_BULK`: the PS shows no full-value
application copy and the client exactly one (`get_range_core`'s `to_vec`).

Traced throughput is not a result (a uprobe fires on every memcpy). Keep the
attribution threshold below one UCX AM fragment (1 KiB): UCX Stream receives
return one fragment per call. After the run, confirm no `autumn-receive-copies`
data directory remains on /data03, /data05 or /data08.

## Verifying that evicted RPC connections close

A client evicts a pooled connection on a timeout, and the manager, PS and stream
pools do the same. Dropping the last `Rc<RpcClient>` must close that connection:
both background tasks are cancelled, the socket half each owned is dropped, and
the peer receives a FIN. Before this, the reader could not end on its own (nor a
writer with frames queued against a peer that stopped reading), so an evicted
connection stayed ESTABLISHED on both sides — with its queued request
values still pinned — until the process exited.

Check it on a live cluster from the client side, e.g. during a PS restart or any
window that makes the SDK evict and reconnect:

```sh
# Connections from this client to the partition servers.
ss -tnp state established "( dport = :<ps-port> )" | grep <client-pid>
# Count them before and after the eviction window; the count must return to the
# number of ACTIVE connections, not keep growing with each reconnect.
ls -l /proc/<client-pid>/fd | grep -c socket
```

A socket that stays ESTABLISHED to a peer the client no longer talks to, or an
fd count that rises with every reconnect, means a connection was abandoned
instead of closed. The same count on the PS side should fall as clients let go.

The regression test is `cargo test -p autumn-rpc --test client_teardown --
--test-threads=1`: it drives timeouts against a peer that never reads, then
asserts the request buffers are freed, no ESTABLISHED socket outlives its client,
and the peer sees EOF.

## Verifying connection reuse after status refusals

A server status error is a completed RPC: FailedPrecondition (stale epoch),
NotFound, Unavailable and other status codes must not cause a TCP reconnect.
Routing retries still refresh the region map. Local deadlines, EOF, bad frames
and I/O failures must evict. Identity changes/token renewal still reconnect to
bind the new token. A partition reload closes the retired instance's existing
connections on the server, so they cannot keep serving a frozen old instance.

Run on Linux with Rust 1.95+:

    cargo test -p autumn-client -p autumn-stream -p autumn-manager --lib connection_tests
    cargo test -p autumn-manager --test system_status_connection_reuse -- --nocapture
    cargo test -p autumn-rpc --test client_teardown
    cargo test -p autumn-stream --test conn_pool_pin

The split/merge test starts a real manager, two extent nodes and a PS, then
counts transparent TCP proxy accepts against the surviving partition after
each topology change. Each phase sends six stale-epoch refusals interleaved
with six successful reads: expect one TCP connection per phase. It also checks
that a client with cached pre-change routing refreshes and writes successfully,
and that both partitions' values survive the merge. A merge's brief body-level
write refusal is polled for readiness; its retry policy is unchanged here.

For ablation, temporarily make RpcError::is_connection_error return true for
Status: the same phases report seven accepts each and the reuse assertions
fail. Restore the classification afterwards. Removing the PS connection's
shared shutdown wait independently strands the post-merge writer on the old
frozen instance; the bounded merge-readiness assertion fails.
## Verifying write+read dual open on a mount (faststart / atomic-save shape)

One process legitimately holds a write fd and a read fd on the same file:
ffmpeg's MP4 faststart keeps the write handle open and reopens the file
`O_RDONLY` to move the moov atom, and Lance's writer reads the previous manifest
while writing the new one. The mount used to refuse the second open with EBUSY
(`Device or resource busy`), which failed every ComfyUI SaveVideo to an autumn
mount.

Check it against a live mount:

```sh
# 1. The shape itself: a read open while a write fd is held.
python3 - /mnt/autumn <<'PY'
import os, sys
p = os.path.join(sys.argv[1], "dual.bin")
fw = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
os.write(fw, b"hello")
fr = os.open(p, os.O_RDONLY)   # EBUSY before the per-role lease refcounts
os.close(fr); os.close(fw)
print("dual open OK")
PY

# 2. The real path it was reported from.
ffmpeg -nostdin -y -f lavfi -i testsrc=duration=2:size=320x240:rate=10 \
       -c:v libx264 -pix_fmt yuv420p -movflags +faststart /mnt/autumn/faststart.mp4
ffprobe -loglevel error -show_entries format=duration /mnt/autumn/faststart.mp4

# 3. A reader outliving the writer must NOT keep the inode's writer slot.
#    The append has to come from a SECOND MOUNT: the manager's writer slot is
#    per CLIENT, so a same-mount append is idempotent and proves nothing.
#    Mount a second daemon first:
#      autumn-fuse --manager 127.0.0.1:9001 --mountpoint /mnt/autumn-b &
python3 - /mnt/autumn <<'PY' &
import os, sys, time
p = os.path.join(sys.argv[1], "tailf.log")
fw = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
os.write(fw, b"line one\n")
fr = os.open(p, os.O_RDONLY)   # the reader that outlives the writer
os.close(fw)                    # last write fd closes -> downgrade
time.sleep(20)                  # hold the read fd open
os.close(fr)
PY
sleep 3
echo "line two" >> /mnt/autumn-b/tailf.log && echo "second mount appended after downgrade OK"
wait
```

The downgrade logs one line per inode; its absence means the writer slot was
never handed back (or the close was not the last write fd):

```sh
grep 'lease downgrade: writer slot released' /var/log/autumn-fuse.log
```

A failed last-writer flush deliberately does NOT downgrade — the mount keeps the
write lease so its retried size stays fence-stamped, and the slot goes back when
the last read fd closes. That case logs `lease downgrade: release failed` only if
the release RPC itself failed.

A refusal shows up in the daemon log as `lease mode mismatch`; that string must
never appear — grep it when a mount reports EBUSY on open:

```sh
grep 'lease mode mismatch' /var/log/autumn-fuse.log   # expect no hits
```

Cross-machine: a read-only opener on another mount is a separate lease client.
It never blocks this mount's writer (the manager conflicts writers only with a
different client's writer), and it receives a `WriterClosed` invalidation when
this mount's last write fd closes — including the downgrade case, where the
mount hands the writer slot back but keeps reading.

## A disk fault is confirmed by a self-check before the cluster acts on it

A `Faulted` verdict now migrates the whole disk, so an extent node proves the
disk is really gone before reporting it: any error the classifier calls `Media`
triggers one small write + fsync into the failing extent's own hash directory.
The probe passing means the write that failed was transient — the disk keeps
its health and the operation still fails for its caller. `Capacity`
(ENOSPC/EDQUOT → `Full`) and `Process` (EMFILE/ENFILE/ENOMEM) never probe:
neither is a statement about the device.

    cargo test -p autumn-stream --lib enospc_disk_health

What an operator sees in the EN log, at WARN:

    write failed but disk write/fsync self-check passed; retaining disk health
    disk write/fsync self-check failed                     (then, at ERROR)
    disk fault confirmed by write/fsync self-check

Only the last one reaches the manager (`df` reports `online: false`) and starts
a rebuild. A stuck device does not stall error handling: the probe — open,
unlink, write, file fsync and directory fsync — is bounded at 2 s in total and
a timeout counts as a confirmed fault.

For ablation, make the `Media` arm of `mark_disk_error_for_extent` fault the
disk without probing: `a_handle_shortage_leaves_the_disk_online` then reports
`Faulted` for a transient EIO, and `disk_probe_timeout_faults_without_blocking_
error_handling` loses its 2 s bound.

## An obsolete recovery marker is released on the tick, without a TTL

Fencing a node creates one recovery marker per slot it holds. Unfencing it
makes that work pointless — the slot is healthy again — and nothing used to
release those markers: they held rate-limiter slots for the life of the leader
and logged nothing while re-dispatching every 2 s.

    AO=(./target/release/autumn-op --manager 127.0.0.1:9001)

    "${AO[@]}" fence-node 5 --reason "marker check" --by you
    "${AO[@]}" recovery-stats        # global/per_source/per_target now non-zero
    "${AO[@]}" unfence 5 --by you
    sleep 3
    "${AO[@]}" recovery-stats        # back to 0 inflight, 0 per-source, 0 per-target

Repeat ten times: nothing accumulates. The release is a state predicate, never
a timeout — a marker is dropped only when that slot genuinely no longer needs
rebuilding (the node is registered, Online and un-overridden, its `avali` bit is
set, the slot is not marked corrupt, and its disk is neither faulted nor
offline). Disk faults, dark slots and corruption keep their markers, which is
why unfencing a node whose disk is bad releases nothing.

The same unfence also ends what a FAILED rebuild left behind. A dispatch that
fails (no reachable target) drains its marker but leaves a backoff entry and a
RUNNING recovery op; once the slot no longer needs a rebuild, the next tick
drops that backoff and closes the op as SUCCEEDED, "no rebuild needed any
more: …". To see it, fence a node while every other node is unreachable or
full, wait for `recovery-stats` to show a backoff row and `ops list --kind
recovery` a running entry, then unfence:

    "${AO[@]}" recovery-stats        # no backoff rows
    "${AO[@]}" ops list --kind recovery   # the entry is succeeded, "no rebuild needed any more"
    "${AO[@]}" health                # 0 degraded, 0 recovering — all three agree

A late completion of the released attempt is still refused with "recovery
attempt changed"; that is the guard working, not a stuck repair.
`cargo test -p autumn-manager --lib an_unfence_ends` is the deterministic form.

A marker that spins without progressing is now visible: the re-dispatch logs
refusals, undecodable replies and unreachable targets at WARN (they were all
`debug!`, so a manager at INFO showed a 2 s loop as complete silence).

One case deliberately keeps its markers, and `recovery-stats` showing a
non-zero count there is correct, not a leak:

- **Right after a leader change**, until the source node's first `df` reaches
  the new leader. `faulted_disks` is emptied at promotion (election is
  in-process, so last term's entries would otherwise survive) while the
  persisted disk record replays as `online: true`, so a faulted disk briefly
  looks healthy; releasing then would discard a real, possibly mid-copy rebuild.
  It clears on that node's next `df` — normally a tick or two, longer if the
  node sits late in an iteration where an unreachable peer ahead of it spends
  its 5 s timeout.

    cargo test -p autumn-manager --lib extent_inflight
    cargo test -p autumn-manager --test node_lifecycle --test system_recovery_loop_drives

For ablation, make `release_recovery_markers_for_healthy_slots` return an empty
vec: `recovery_marker_unfence_releases_only_a_healthy_slot` fails with the
marker still held after the unfence.

## A replica that missed the seal is caught up in place

A log extent may be sealed while only one of its replicas answers: that
replica holds every acked byte, so its length becomes `sealed_length`. The
other replicas come back SHORTER and with their `avali` bit clear. The recovery
loop does not move them; once a node is Online again it sends `re_avali`, the
node copies the missing bytes from the replica that answered, and the slot is
marked available. There is no mode to choose: the loop moves a copy to another
node only for a fenced node, a corrupt slot, or a disk its own node reports
faulted.

    cargo test -p autumn-manager --test single_wal_replica_survivor

The test writes record 1 to three replicas, stops two of their nodes, lets
record 2 reach only the third, crashes the writer, opens the stream with only
the survivor up (one answer seals the extent at 8192), then restarts the two
nodes holding 4096 bytes. Within 60 s both copies must be 8192 bytes, every
slot available, and every replica must read back records 1 and 2.

Ablation: make the in-place branch in `recovery_dispatch_tick` never run (for
example prefix its condition with `false &&`). The test fails with
`avali 0b100 (want 0b111), their copies [Some(4096), Some(4096)]`.

The same file's `a_catch_up_never_copies_from_a_dark_replica` pins where the
bytes come from: the first member in slot order rots at full length and is
reported corrupt. While the one good copy is away the returning replica must
stay behind; once it is back the replica is filled from it. Ablation: let
`copy_sources` in `crates/stream/src/extent_node.rs` keep corrupt-marked slots;
the test fails with `with C away, B was filled from A's corrupt copy`.
`a_fenced_sole_seal_member_is_rebuilt_from_its_own_copy` pins the other
side: the one member that answered the seal is fenced while the others are
away, and every slot must end at the full seal with the record only it held.
Ablations: keep only lit members (no source, never replaced), or exclude the
replaced slot from the rebuild's sources (`stream_extent_from_sources`'s
best-effort list) — the rebuild reconciles down to 4096 and the test fails with
`avali 0b10 (want 0b111)`.

Catch-ups run in the background, at most 8 at once, and never on an extent
with another op in flight (a sibling slot's rebuild pinned the eversion the
catch-up would bump): `cargo test -p autumn-manager --lib
a_behind_slot_waits_for_the_op_in_flight_on_its_extent`.

On a live cluster, `autumn-op extent-health` lists a behind slot as
`avali=false` while its node is `auto=online`; the leader logs
`caught a behind copy up to the sealed length in place` when it finishes, or
`could not catch a behind copy up in place` with the reason (retried with
backoff, cap 300 s).

## Extent health at a glance (`autumn-op health`)

`autumn-op health` answers what `ceph -s` answers for placement groups: is
every sealed extent fully replicated, and if not, which ones are worst.

    autumn-op --cluster-secret-file $SECRET --manager $MGR health        # --json goes before `health`
    extents: 812 sealed, 21 open (open tails are not classified)
    810 clean, 2 degraded (128.0 MiB), 1 with no redundancy left, 0 unavailable, 1 recovering
    slots not serving: 2 unreachable, 1 behind
    worst extents:
      extent 77  1/3 serving (needs 1)  recovering  64.0 MiB  slot1 node 5 unreachable 812s; slot2 node 6 behind

There is no one-word verdict: the counts are the answer (`autumn-op status`
puts them beside the fleet counts).
- `unavailable` — fewer serving copies than a read needs (1 for a replicated
  extent, the data-shard count for an EC one).
- `degraded` — short a copy but still readable.
- slot states: `behind` (node answers, copy missed the seal — caught up in
  place automatically), `unreachable` (node not Online or disk offline),
  `maintenance`, `fenced`, `corrupt`, `disk-faulted` (the last three are being
  rebuilt automatically).
- `--detail N` names the N worst extents (default 10); `--json` is the shape
  the dashboard reads (`/api/overview` → `extent_health`). The seconds after
  each slot are how long this leader has seen it not serving, measured on the
  60 s policy tick and restarted at a leader change.

It is the leader's own view, not a poll of the nodes, which has three
consequences worth knowing:
- One `df` that times out (5 s) marks that node's disks offline at once, so
  its slots read `unreachable` and its extents count as degraded until the
  next successful `df` — a loaded node can flicker degraded for a few seconds
  while `list-nodes` still says Online.
- Right after a leader change, until each node's first `df` reaches the new
  leader (about 15 s), a dead node's copies still read as serving.
- `fenced`, `corrupt` and `disk-faulted` count as not serving even when the
  copy still answers reads (they are being moved off), so `unavailable` can mean
  "every copy of this extent is going away", not only "unreadable now".

The dashboard's Overview → Status bar shows the counts beside the fleet; the
Fleet panel turns the same summary into alert rows
(red: unavailable or no redundancy left; amber: degraded; green: rebuilding).

Verify:

    cargo test -p autumn-manager --lib extent_health
    cargo test -p autumn-manager --test extent_health_summary
    cargo test -p autumn-server --bin autumn-op health
    node crates/server/src/bin/autumn_dashboard/tests/render_check.js
    crates/server/src/bin/autumn_dashboard/tests/api_contract.sh

`extent_health_summary` stops the node holding one replica of a sealed RF 3
extent: within seconds the summary counts it degraded and names it,
`2/3 serving`, with the node's slot `unreachable`; restarting the node brings
it back to 0 degraded. Ablation: make `classify_slot` treat every node as
reachable — the test times out waiting for the degraded count. `render_check.js`
fails if the degraded row is not rendered (ablation: skip that row).

## Rebuilding degraded copies without a fence (`autumn-op repair`)

The recovery loop moves a copy on its own only off a fenced node, a corrupt
slot or a faulted disk — a node that stopped answering may be back in seconds.
When it is not coming back soon, move its copies without fencing it:

    autumn-op ... repair 77 81           # these extents' degraded slots
    autumn-op ... repair --node 5        # every degraded slot on node 5
    autumn-op ... --wait repair 77       # block until the request is recorded

See and withdraw standing requests:

    autumn-op ... health                 # "[repair requested]" per slot, and the count
    autumn-op ... repair --cancel 77     # withdraw extent 77's requests
    autumn-op ... repair --cancel --node 5

A cancel restarts those slots' degraded clocks, so the repair policy waits a
full `--repair-grace-secs` before proposing them again (switch the policy's
`repair` off to stop it for good; with a grace of 0 the next pass re-requests
them). A rebuild already dispatched runs to
completion.

The op succeeds once the requests are recorded (`ops status` shows "requested
a rebuild of N slot(s) on M extent(s)", plus what was skipped and why — a
healthy or open extent, one with no copy left to read from); the rebuilds then
run as `recovery` entries in `ops list`. Requests are persisted and survive a
leader change; one is cleared when its rebuild lands, and WITHDRAWN when the
slot's node answers again before that (the leader has heard its `df` since it
took over) and its copy serves — so a node that comes back keeps the copies
not yet moved, and a leader change alone withdraws nothing. A copy that is
behind on a node that answers is caught up in place first; the request moves
it only if the node answers that it has no such extent (a wiped node
rejoined) — a slow or failing catch-up just retries.
The node stays in the cluster (unlike fence); copies already moved stay moved.

Automatically: the `repair` auto-policy switch (on in `maintenance`,
`balanced` and `aggressive`) proposes one advisory per node whose slots have
been degraded at least `--repair-grace-secs` (manager flag, default 600) —
visible in `autumn-op policy-candidates` and the dashboard's Policy tab as
`repair node N` — and, when the policy is Armed, submits `repair --node N` as
an op (`requested_by=auto-policy`, in `ops list` / `ops history`) that
requests only the slots past the grace (while Off, the advisory is all there
is). The dashboard's Fleet panel also offers a Repair button for
the worst readable degraded extent that still has a slot nobody asked to
rebuild; once every listed slot is marked `(repair requested)` there is no
button, only `Cancel repair`.

Verify:

    cargo test -p autumn-manager --test extent_repair
    cargo test -p autumn-manager --lib extent_repair
    cargo test -p autumn-manager --lib extent_health
    cargo test -p autumn-manager --lib auto_policy

`extent_repair` runs three real-EN scenarios: an operator repair moves the copy
of a stopped node onto the spare and the node is not fenced; a repair-only
policy (grace 2 s), selected but Off, advises without moving anything, then
rebuilds when Armed; a request recorded on one leader is served by the next after a
spare joins (real etcd, two managers); a node that returns before its requests
could be served keeps its copies when the spare comes back. Ablations, each
reddening exactly its scenario: make `slot_verdict` ignore `repair_requested`
(the first three), make `install_replayed_repair_slots` drop what it replays
(the failover one), make `repair_candidates` return nothing (the policy one),
never withdraw a request (the returning-node one), make `cancel_repair` skip
`withdraw_repairs` or `summarize` never set `repair_requested` (the
`a_standing_repair_request_is_shown_and_can_be_cancelled` one).

## Scrub: checking sealed copies at rest (`autumn-op scrub`)

Nothing on the read, write, seal, conversion or repair path checks content. A
scrub does, when asked: the manager names each lit copy of each sealed extent
in scope, the node holding it reads and hashes it locally, and only outcomes
come back. A copy's first scrub records its checksums (`extent-{id}.ck` for a
replica's `.dat`, `extent-{id}.shard{i}.ck` for an EC shard); later scrubs
compare against them. Design: `docs/integrity_design.md`.

    "${AO[@]}" scrub 1234 1235          # these extents
    "${AO[@]}" scrub --part 7           # every extent of partition 7
    "${AO[@]}" scrub --all --wait       # the whole cluster, and wait for it
    "${AO[@]}" ops status <OP_ID>       # progress = files reported / dispatched

The op's message counts files: clean, recorded for the first time, rotted,
skipped, failed, plus what planning left out (open or empty extents, an op in
flight, dark slots, nodes not online). It SUCCEEDS whatever it found and FAILS
only if a file could not be checked. A rotted copy is also reported to the
manager, which isolates the slot (never the last replica; never below K EC
shards) and rebuilds it elsewhere:

    grep 'SCRUB FOUND CONTENT ROT' <that EN's log>   # the finding, per file
    "${AO[@]}" health                                # the slot shows corrupt, then rebuilds

Pacing is on each extent node: `autumn-extent-node --scrub-bytes-per-sec N`
(default 8 MiB/s per shard, so N shards read up to N times that; `0` =
unpaced). A node that restarts mid-scrub loses its queue; the files it held are
reported FAILED on its next `df` ("no longer has it queued"), so the op ends
FAILED — submit it again. An op whose nodes all stop answering ends as UNKNOWN
after two hours.

Weekly: the auto-policy `scrub` switch (on in `maintenance`, `balanced`,
`aggressive`) submits `scrub --all` at most once every 7 days; the cooldown is
persisted, so a manager failover does not restart the week:

    "${AO[@]}" auto-policy start balanced
    "${AO[@]}" policy-candidates        # a `scrub cluster` row only when it is due
    "${AO[@]}" ops list --kind scrub

Trust-on-first-use: bytes already damaged before a copy's first scrub are
recorded as they are, so scrub new data early. Until a scrub finds rot, reads,
recovery and EC conversion use the damaged copy like any other.

Automated:

    cargo test -p autumn-stream --lib scrub
    cargo test -p autumn-stream --test payload_location a_scrub_request
    cargo test -p autumn-manager --lib extent_scrub
    cargo test -p autumn-manager --test scrub_on_demand

Manual, on a dev cluster with a sealed extent `E` held by node `N`:

    "${AO[@]}" scrub E --wait                      # "... recorded for the first time"
    ls <N's data dir>/*/extent-E.ck
    python3 -c "import sys; f=open(sys.argv[1],'r+b'); f.seek(1<<20|512); b=f.read(1); f.seek(1<<20|512); f.write(bytes([b[0]^1]))" <path>/extent-E.dat
    "${AO[@]}" scrub E --wait                      # "... 1 rotted ... isolated for rebuild"
    "${AO[@]}" health                              # N's slot of E: corrupt, then rebuilt elsewhere
    "${AO[@]}" scrub E --wait                      # the rebuilt copy: recorded for the first time

## A bulk read's refusal keeps its status code

`MSG_READ_BYTES` refuses a mis-routed read with `FailedPrecondition` and a
message naming the owning shard. `MSG_READ_BYTES_BULK` used to flatten every
such refusal into `CODE_ERROR "extent unavailable"`, so a client could not tell
a routing error from a genuinely unavailable extent — and the refresh-and-retry
fallback that keys on the error TYPE became dead code on that path, with the
whole unit suite and a byte-for-byte e2e run green.

    cargo test -p autumn-stream --test shards a_read_addressed_to_the_wrong_shard_is_refused
    cargo test -p autumn-client --lib connection_tests

The shard test drives both arms against a two-shard node: the routed address
serves the bytes, the base address refuses, and the bulk refusal must arrive as
a typed `FailedPrecondition` whose message still contains `belongs to shard`.

For ablation, restore `bulk_read_head(CODE_ERROR, "extent unavailable")` in the
bulk `get_extent` error arm: the test fails with `code: 4` and the flattened
message.


## Recovery attempt and node retirement verification (wire 46)

Stop all manager/PS/EN roles before upgrading from wire 45, then restart them
with wire 46. Existing client binaries in the 43..46 window remain compatible.
Existing manager record formats are unchanged; do not wipe etcd or run the old
record converter. New Recovery markers atomically create recoveryAttempt/<id>
(type 10, format 1). A pre-upgrade Recovery marker without this snapshot is
cancelled and re-derived on the next dispatch tick. An unreadable snapshot or
snapshot/marker revision mismatch refuses replay; restore a consistent backup
and investigate instead of deleting individual keys.

Fence cancels recoveries targeting the node. If cancellation cannot persist,
Remove reports those extents in blocking_marker_extent_ids until cleanup can
retry. If a recovery commit wins first, Remove instead reports the resulting
membership in blocking_extent_ids. A delayed completion cannot reintroduce a
removed node or disk.

Run on a host with Rust and etcd (or set AUTUMN_TEST_ETCD_BIN):

~~~sh
cargo test -p autumn-manager --lib
cargo test -p autumn-stream -p autumn-rpc --lib
cargo test -p autumn-manager --test recovery_attempt --test system_extent_recovery --test node_lifecycle --test apply_done_atomicity -- --include-ignored --test-threads=1
~~~

The restart test terminates and joins the target's whole runtime before starting
a new one over the same directory. The etcd test checks atomic creation/deletion,
replay, and same-assignment A/B reissue. Unit barriers suspend Recovery before
commit and after its transaction response while Fence/Remove run concurrently.


## Gallery HTTP cache validation

Run `cargo test -p gallery` for conditional-response and router-policy tests.
With a gallery already listening at `http://localhost:5001`, use a disposable
filename for the upload/delete sequence below (it deliberately replaces and
then removes that filename). Python 3 standard library is sufficient:

```bash
python3 - <<'PYTHON'
import urllib.request as request
import urllib.error
import time

base = "http://localhost:5001"
name = "gallery-cache-verification.txt"

def call(path, method="GET", headers=None, data=None):
    req = request.Request(base + path, data=data, headers=headers or {}, method=method)
    try:
        response = request.urlopen(req)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, response.headers, response.read()

def upload(content):
    boundary = "gallery-cache-check-boundary"
    data = (f'--{boundary}\r\nContent-Disposition: form-data; name="file"; '
            f'filename="{name}"\r\nContent-Type: text/plain\r\n\r\n').encode()
    data += content + f"\r\n--{boundary}--\r\n".encode()
    status, headers, _ = call("/put/", "POST",
        {"Content-Type": f"multipart/form-data; boundary={boundary}"}, data)
    assert status == 200 and headers["Cache-Control"] == "no-store"

for path in ["/", "/static/app.css", "/static/app.js"]:
    status, headers, _ = call(path)
    assert status == 200 and headers["Cache-Control"] == "no-cache"
    assert call(path, headers={"If-None-Match": headers["ETag"]})[0] == 304
upload(b"old")
status, headers, data = call("/get/" + name)
assert status == 200 and data == b"old"
modified = headers["Last-Modified"]
conditional = {"If-Modified-Since": modified}
assert call("/get/" + name, headers=conditional)[0] == 304
status, headers, _ = call("/del/" + name, "DELETE")
assert status == 200 and headers["Cache-Control"] == "no-store"
assert call("/get/" + name, headers=conditional)[0] == 404
time.sleep(1.1)  # Last-Modified and uploaded_at intentionally have second precision.
upload(b"new")
status, headers, data = call("/get/" + name, headers=conditional)
assert status == 200 and data == b"new" and headers["Last-Modified"] != modified
status, _, data = call("/get/" + name,
    headers={"Range": "bytes=0-0", "If-Range": modified})
assert status == 200 and data == b"new"
status, headers, data = call("/get/" + name,
    headers={"Range": "bytes=0-0", "If-Range": headers["Last-Modified"]})
assert status == 206 and data == b"n" and headers["Content-Range"] == "bytes 0-0/3"
assert call("/get/temporary.mp4")[0] == 404
assert call("/list/")[1]["Cache-Control"] == "no-store"
assert call("/del/" + name, "DELETE")[0] == 200
print("gallery cache checks passed")
PYTHON
```

For a real uploaded image, repeat the Last-Modified/If-Modified-Since check
against `/thumb/<name>` (including an SVG and a malformed image that triggers
original fallback). For a completed video, check `/hls/<name>/index.m3u8` and
one listed TS segment. Each initial 200 must carry `no-cache` and the shared
transcode time; a conditional request returns 304. Video originals always
return 404 from `/get/`.
