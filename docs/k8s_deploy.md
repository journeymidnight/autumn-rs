# Kubernetes deployment

`deploy/k8s/` is a kustomize base that runs a full autumn cluster on Kubernetes:
etcd + manager + extent-nodes + partition-server + a one-shot bootstrap Job. One
container image (`deploy/docker/Dockerfile`) serves every role;
`deploy/docker/entrypoint.sh` dispatches on the first arg and does all the
k8s-specific glue (DNS→IP resolution, env→flag translation, bring-up guards) so
the Rust binaries stay env-free and take only CLI flags.

## Why the manifests look the way they do

The binaries impose three hard constraints (verified against the code); the
manifests are shaped around them:

1. **No DNS in the binaries.** Every address goes through `SocketAddr::parse`
   (IP literals only). The entrypoint resolves Service names to ClusterIPs with
   `getent` and passes IPs as flags. IPv6 pod/Service IPs work (bracketed).

2. **Extent-node identity is a stable `node_uuid`, not its address**
   (F-EN-DYNSHARD). The `node_uuid` is minted once at `format` and persisted on
   the PVC; the EN **self-registers its live pod IP + every shard port at each
   startup** under that uuid. A pod IP change on reschedule just updates the
   same identity's location and routing follows — so the EN advertises its
   **pod IP** (Downward API `status.podIP`), exactly like the PS. The manager
   and PS dial the registered `pod_ip:shard_port` directly (pod IPs are routable
   cluster-wide under every CNI). **No per-pod ClusterIP Service exists** — the
   old `autumn-en-<ordinal>` VIPs and their hand-maintained shard-port lists are
   deleted, not worked around.

3. **`bootstrap` is not idempotent.** A second run creates duplicate streams. It
   runs as a **Job** (not a Deployment); the entrypoint guards it by checking
   `autumn-op info --full` for existing streams and skips if present. Safe to
   re-apply.

Two constraints work in our favor:

- **PS re-registers per-partition addresses on every open**, so the PS can
  advertise its **pod IP** (downward API `status.podIP`) — a reschedule heals
  itself. Per-partition ports are dynamic, so they are not enumerated in a
  Service; in-cluster clients reach them over flat pod networking.
- **Manager is stateless.** Its readiness probe is the leader-gated `autumn-op
  info`, so only the current leader is an endpoint of the `autumn-manager`
  ClusterIP Service — in-cluster callers reach the leader directly and never
  handle `NOT_LEADER`. Scale `replicas` to 3 for HA; the losers stay
  `Ready=false` until they win the etcd lease.

## Components

| Manifest | Kind(s) | Notes |
|---|---|---|
| `namespace.yaml` | Namespace `autumn` | |
| `storageclass.yaml` | StorageClass `autumn-en-local` | local disk for ENs (see Storage) |
| `configmap.yaml` | ConfigMap | shared env; `AUTUMN_EXPECT_NODES` MUST equal EN replicas |
| `etcd.yaml` | StatefulSet + headless Service | 1 member; PVC 1Gi on default (network) class |
| `manager.yaml` | StatefulSet + ClusterIP + headless Services | leader-gated readiness |
| `extent-node.yaml` | StatefulSet + headless Service | advertises pod IP; PVC 20Gi/pod on local disk |
| `partition-server.yaml` | StatefulSet + headless Service | advertises pod IP; no PVC |
| `bootstrap-job.yaml` | Job | guarded, run-once |

Apply order does not matter — each role's entrypoint waits for the manager
leader (and the bootstrap Job also waits for `AUTUMN_EXPECT_NODES` ENs Online).

## Storage

The two stateful roles have opposite storage needs, so they use opposite classes.

**Extent nodes → LOCAL disk** (`storageClassName: autumn-en-local`, see
`storageclass.yaml`). autumn already replicates across ENs itself (RF=3 + EC), so
extent data must not sit on a *self-replicating* network volume — that would
double-replicate (autumn 3× × EBS/Ceph 3× = 9×) and add a network hop under the
storage system. `volumeBindingMode: WaitForFirstConsumer` binds the local volume
on the node the pod lands on, which **pins the EN pod to that node**. If the node
is permanently lost the EN's local copy goes with it, but RF=3 keeps the data on
the other ENs and the manager re-replicates — the same failure model as the
bare-metal `/data*` NVMe layout. `reclaimPolicy: Retain` so a deleted PVC never
auto-wipes extents.

The shipped class uses the `rancher.io/local-path` dynamic provisioner
(pre-installed on kind/minikube). For **dedicated production NVMe**, use static
`local` volumes instead — one PV per node/disk, pinned by nodeAffinity:

```yaml
# storageclass.yaml → change the provisioner:
#   provisioner: kubernetes.io/no-provisioner
# then pre-create one PV per EN disk:
apiVersion: v1
kind: PersistentVolume
metadata: { name: autumn-en-node1-nvme0 }
spec:
  capacity: { storage: 3Ti }
  accessModes: ["ReadWriteOnce"]
  storageClassName: autumn-en-local
  local: { path: /mnt/nvme0/autumn }          # the disk mount on that node
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - { key: kubernetes.io/hostname, operator: In, values: ["node1"] }
```

**etcd → NETWORK durable storage** (the cluster default class — EBS / PD / Azure
Disk on a cloud; `storageClassName` deliberately omitted). This lets a **single**
etcd member survive node loss cheaply: if the node dies the pod reschedules
(same AZ) and the network volume re-attaches — no 3-member quorum needed. etcd
metadata is small (1Gi), so one member on a network volume is the resource-saving
choice. For control-plane HA instead, run 3 replicas (each on its own network
volume) with the multi-member `--initial-cluster`.

The PS uses no PVC — its local state (WAL) is recoverable from the streams on
restart, so the pod's ephemeral filesystem is fine.

## Build the image

```bash
# From the repo root (.dockerignore keeps target/ out of the context):
docker build -f deploy/docker/Dockerfile -t autumn-rs:latest .

# kind: load it into the cluster; or push to a registry your nodes can pull.
kind load docker-image autumn-rs:latest
```

The image is TCP-only. UCX/RDMA is intentionally not built in — it needs
`hostNetwork` + an RDMA device plugin + `IPC_LOCK`/memlock, out of scope for v1.

**Image tag / pull policy.** The manifests use `autumn-rs:latest` with
`imagePullPolicy: IfNotPresent` — correct for **kind/minikube**, where you
`kind load` the image locally and there is no registry to pull from. For a
**shared cluster** this combination can run a stale binary (a rebuilt `:latest`
won't be re-pulled onto a node that already has one): pin an immutable tag or
digest instead, via the kustomization `images[].newTag` (e.g. `newTag: v1.2.3`)
or `@sha256:…`. Prefer immutable tags in production; treat `latest` as dev-only.

## Deploy

`deploy/k8s` is a **generic reference** (placeholder image `autumn-rs:latest`,
3 ENs, no StorageClass/node assumptions). On kind/minikube you can apply it
directly. On a real cluster, apply a thin **overlay** that layers the
cluster-specific values instead (see below):

```bash
kubectl apply -k deploy/k8s              # kind/minikube (after `kind load`)
# or:
kubectl apply -k deploy/overlays/vke     # a real cluster (worked example)
kubectl -n autumn get pods,svc
kubectl -n autumn wait --for=condition=complete job/autumn-bootstrap --timeout=300s
```

## Authz (ON by default)

`deploy/overlays/vke/deploy.sh` provisions data-plane authz automatically
(F-AUTHZ-BUILTIN): it generates the `autumn-authz` Secret (signing key + admin
token) **once — never rotated here** (rotating invalidates every minted
credential) — and the manager StatefulSet mounts it (`optional`, at
`/etc/autumn/authz`). The ConfigMap sets `AUTUMN_AUTH_PROTECTED_PREFIXES=fs/ kvc/
mem/`; the entrypoint engages authz only when the Secret is actually present
(`-s` gate), so a cluster deployed without it — or with
`AUTUMN_AUTH_DISABLE=1` in the ConfigMap — runs authz-OFF instead of
crash-looping.

Mint a client credential + Secret (once the cluster is up):

```bash
# admin token lives in the autumn-authz Secret
ADMIN=$(kubectl -n autumn get secret autumn-authz -o jsonpath='{.data.admin\.token}' | base64 -d)
# mint a 'default'-tenant credential (run against the manager Service).
# --admin-token-file reads the token from a file (process substitution keeps it
# out of argv); --admin-token would take the /dev/fd path as the literal token.
autumn-op --manager <mgr> tenant-create --tenant default \
    --prefix fs/default/ --prefix kvc/default/ --prefix mem/default/ \
    --admin-token-file <(printf %s "$ADMIN") | awk '/^credential:/{print $2}' > default.cred
kubectl -n autumn create secret generic autumn-credential --from-file=credential=default.cred
```

Client pods mount `autumn-credential` and pass `--credential-file` (native
clients) / `auth_credential_file` (kvcache) / `credential=` (PyO3). Full
runbook: `docs/ops.md` "Enabling authz".

## Per-cluster overlay

Anything that differs between clusters does **not** belong in the base — keep
`deploy/k8s` generic and put the specifics in an overlay that lists `../../k8s`
as a resource. `deploy/overlays/vke` is a worked Volcengine (VKE) example;
copy it for a new cluster and adjust:

| Overlay knob | Why it's cluster-specific |
|---|---|
| `images[].newName/newTag` | your registry + immutable tag/digest |
| `images[]` etcd mirror | nodes may not reach `quay.io` directly (VKE → daocloud mirror) |
| `nodeSelector` patch | your node pool (VKE pins to kernel-≥5.15 nodes labeled `autumn-node=true`; autumn needs io_uring) |
| etcd `storageClassName` + size | your network-durable class; mind provider **minimum volume size** (Volcengine EBS ESSD = 20Gi, so the base 1Gi is patched up) |
| EN `replicas` + `AUTUMN_EXPECT_NODES` | your sizing (keep the two counts equal; no Services to add — ENs advertise pod IPs) |

One thing that trips up real clusters, already handled in the base so every
overlay inherits it: **`enableServiceLinks: false`** on all pods (K8s injects
`AUTUMN_MANAGER_PORT=tcp://…` from the `autumn-manager` Service, colliding with
the entrypoint's own `AUTUMN_MANAGER_PORT` and panicking the manager).

If your cluster has **no default StorageClass**, the etcd PVC (which omits
`storageClassName` in the base to inherit the default) stays Pending — the
overlay must name a class explicitly, as the VKE example does.

## Scaling

- **Extent nodes**: bump `extent-node.yaml` StatefulSet `replicas` to N and set
  `AUTUMN_EXPECT_NODES: "N"` in the ConfigMap. That's it — ENs advertise their
  pod IPs and self-register, so there are no per-pod Services to add (F-EN-DYNSHARD
  M2). Provision **more ENs than the replication factor** — with `#EN == RF` a
  single EN down wedges writes (can't form a fresh replica set); reads tolerate a
  down replica at any size.
- **Partition servers**: raise `partition-server.yaml` `replicas`; each pod's
  `psid` is `ordinal+1`. More partitions (not more workers per PS) is how you
  scale throughput.
- **Managers**: raise `manager.yaml` `replicas` (3 for HA). No other change.

## Multi-shard extent nodes

By default each EN runs **one shard** (`--cpuset 0`) — a single io_uring core
serves all its extent traffic. Under sustained durable writes (RF=3 + fsync),
adding partitions scales write throughput until that **one EN core** becomes the
wall (benchmarked here: write flattens ~65k ops/s past 16 partitions). Giving
each EN more shards spreads its extents (`shard = extent_id % N`) across N cores.

The entrypoint exposes this via **`AUTUMN_EXTENT_SHARDS`** (default 1). When > 1
it sizes the EN to cores `0..N-1`. F-EN-DYNSHARD M1c/M2: `format` is
identity-only now — the EN binary itself self-registers all N shard ports at
startup (via its own `--advertise`, which now carries the **pod IP**), and the
manager/PS dial `pod_ip:shard_port` directly. Shard `i` binds data port
`9101 + i*10` and control port `10101 + i*10`. **There is no Service port list
to keep in lockstep** — that was the whole point of M2. `deploy/overlays/vke`
is a worked 4-shard example: it sets `AUTUMN_EXTENT_SHARDS: "4"` and requests 4
CPU per EN. That's the complete change.

Changing the shard count for a **running** cluster is a **stop-the-world reshard**
(F-EN-DYNSHARD M3, ownership = `extent_id % shard_count` remaps globally): edit
`AUTUMN_EXTENT_SHARDS`, then restart all ENs together. Zero bytes move on disk
(the file layout is hash-subdir'd, shard-independent); only routing remaps. See
the reshard runbook in `docs/ops.md` and `scripts/reshard_chaos.sh`.

## Using the cluster

v1 clients run **inside** the cluster (they dial per-partition PS pod IPs
directly over flat pod networking). Example throwaway client pod:

```bash
kubectl -n autumn run kv --image=autumn-rs:latest --restart=Never -it --rm \
  --command -- bash -lc '
    echo hi > /tmp/v;
    autumn-client --manager autumn-manager:9001 put k /tmp/v;
    autumn-client --manager autumn-manager:9001 get k'
```

Wait — `autumn-client` also parses `--manager` as an IP, so pass the resolved
ClusterIP, or run through the entrypoint which resolves it. For scripted use,
resolve first: `M=$(getent hosts autumn-manager | awk '{print $1}')` then
`autumn-client --manager $M:9001 …`.

Exposing the data plane to **out-of-cluster** clients is not supported in v1:
per-partition ports are dynamic and clients dial pod IPs, which L4
indirection (NodePort/LoadBalancer) cannot represent. Run the client, FUSE
mount, or kvcache adapter as in-cluster workloads.

## Manual verification (kind / minikube)

No docker/kubectl toolchain ships in this repo's CI box, so this is the
documented manual path:

```bash
kind create cluster
docker build -f deploy/docker/Dockerfile -t autumn-rs:latest .
kind load docker-image autumn-rs:latest
kubectl apply -k deploy/k8s
kubectl -n autumn wait --for=condition=complete job/autumn-bootstrap --timeout=300s
kubectl -n autumn get pods            # etcd/manager/en-0..2/ps-0 all Running+Ready
# round-trip (see "Using the cluster" above)
```

Before applying, run the clusterless checks: `bash deploy/validate.sh` (YAML
well-formedness, selector/serviceName/port cross-refs, entrypoint role dispatch,
kustomization resource + image consistency, `bash -n` on the shell glue).
