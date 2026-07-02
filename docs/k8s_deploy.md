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

2. **Extent-node identity is its advertise address, fixed for life.** The
   manager keys an EN on its advertise address and the EN never re-registers a
   new one. A pod IP changes on reschedule → phantom node. So **each EN gets its
   own ClusterIP Service** (`autumn-en-<ordinal>`, a stable VIP) and advertises
   that, not its pod IP. Each per-pod Service exposes **both** ports the manager
   dials: `9101` (data) and `10101` (control = data+1000, used for df/recovery).

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
| `configmap.yaml` | ConfigMap | shared env; `AUTUMN_EXPECT_NODES` MUST equal EN replicas |
| `etcd.yaml` | StatefulSet + headless Service | 1 member default; PVC 1Gi |
| `manager.yaml` | StatefulSet + ClusterIP + headless Services | leader-gated readiness |
| `extent-node.yaml` | StatefulSet + headless + N per-pod ClusterIP Services | PVC 20Gi/pod |
| `partition-server.yaml` | StatefulSet + headless Service | advertises pod IP |
| `bootstrap-job.yaml` | Job | guarded, run-once |

Apply order does not matter — each role's entrypoint waits for the manager
leader (and the bootstrap Job also waits for `AUTUMN_EXPECT_NODES` ENs Online).

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

```bash
kubectl apply -k deploy/k8s
kubectl -n autumn get pods,svc
kubectl -n autumn wait --for=condition=complete job/autumn-bootstrap --timeout=300s
```

## Scaling

- **Extent nodes**: bump `extent-node.yaml` StatefulSet `replicas` to N, add a
  per-pod `autumn-en-<i>` ClusterIP Service for each new ordinal, and set
  `AUTUMN_EXPECT_NODES: "N"` in the ConfigMap. Provision **more ENs than the
  replication factor** — with `#EN == RF` a single EN down wedges writes (can't
  form a fresh replica set); reads tolerate a down replica at any size.
- **Partition servers**: raise `partition-server.yaml` `replicas`; each pod's
  `psid` is `ordinal+1`. More partitions (not more workers per PS) is how you
  scale throughput.
- **Managers**: raise `manager.yaml` `replicas` (3 for HA). No other change.

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
