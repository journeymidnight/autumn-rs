#!/usr/bin/env bash
# Render one Kubernetes workload per extent node.
#
# WHY NOT A StatefulSet. An EN's identity is the `node_uuid` persisted on its
# PVC — not its ordinal, not its address (the EN self-registers its live
# address every startup). A StatefulSet ties that identity to an ordinal and
# then offers exactly one removal verb, `scale`, which removes from the TOP.
# The two orderings are unrelated, so the verb cannot express "retire THIS
# node": on a cluster whose live ENs were 0,5,6,7,8,9,10, scaling 11 -> 5 would
# have destroyed six ENs holding data and kept four that held none.
#
# One Deployment per EN makes the k8s object and the cluster member the same
# thing. Retiring one is `en-decommission.sh`, which fences it, waits for its
# shards to be re-replicated elsewhere, removes it from the cluster, and only
# then deletes the workload.
#
# Each Deployment is `strategy: Recreate`, and that is load-bearing rather than
# taste: the PVC is ReadWriteOnce on a local disk pinned to one node, so a
# RollingUpdate's surge pod would wait forever for a volume the pod it is
# replacing still holds.
#
# Usage:
#   en-workload.sh render <ordinal>...     # manifests to stdout
#   en-workload.sh apply  <ordinal>...     # render | kubectl apply -f -
#
# Environment:
#   AUTUMN_NS          namespace (default: autumn)
#   AUTUMN_EN_IMAGE    image (default: autumn-rs:latest — the base placeholder;
#                      deploy.sh injects the real one, see the overlay)
#   AUTUMN_EN_STORAGE  PVC request (default: 20Gi). Ignored by an existing PVC:
#                      a claim is never resized by re-applying it.
#   AUTUMN_EN_CPU      cpu request (default: 1). One io_uring core per shard is
#                      the sizing rule; no limit, so the EN can still burst.
#   AUTUMN_EN_NODESELECTOR  one `key=value` (default: none). Overlays that keep
#                      autumn off some nodes set it here rather than patching,
#                      because these workloads are not kustomize resources.
set -euo pipefail

NS="${AUTUMN_NS:-autumn}"
IMAGE="${AUTUMN_EN_IMAGE:-autumn-rs:latest}"
STORAGE="${AUTUMN_EN_STORAGE:-20Gi}"
CPU="${AUTUMN_EN_CPU:-1}"
SELECTOR="${AUTUMN_EN_NODESELECTOR:-}"
if [ -n "$SELECTOR" ]; then
  case "$SELECTOR" in *=*) ;; *) echo "AUTUMN_EN_NODESELECTOR must be key=value" >&2; exit 2;; esac
  SEL_YAML="$(printf '\n      nodeSelector:\n        %s: "%s"' "${SELECTOR%%=*}" "${SELECTOR#*=}")"
else
  SEL_YAML=""
fi

render_one() {
  local n="$1"
  cat <<YAML
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data-autumn-en-${n}
  namespace: ${NS}
  labels:
    app.kubernetes.io/name: autumn
    app.kubernetes.io/component: extent-node
spec:
  accessModes: ["ReadWriteOnce"]
  # Local disk — autumn replicates across ENs itself, so extent data must NOT
  # sit on a self-replicating network volume. See storageclass.yaml.
  storageClassName: autumn-en-local
  resources:
    requests:
      storage: ${STORAGE}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: autumn-en-${n}
  namespace: ${NS}
  labels:
    app.kubernetes.io/name: autumn
    app.kubernetes.io/component: extent-node
    autumn.dev/en-ordinal: "${n}"
spec:
  replicas: 1
  # See the header: RWO on a node-pinned local volume cannot be surged.
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app.kubernetes.io/name: autumn
      app.kubernetes.io/component: extent-node
      autumn.dev/en-ordinal: "${n}"
  template:
    metadata:
      labels:
        app.kubernetes.io/name: autumn
        app.kubernetes.io/component: extent-node
        autumn.dev/en-ordinal: "${n}"
    spec:${SEL_YAML}
      terminationGracePeriodSeconds: 30
      enableServiceLinks: false   # binaries use ConfigMap+DNS, not service-link env vars
      # Spread ENs across nodes so replicated copies don't share a failure
      # domain. Preferred, not required, so a single-node dev cluster still
      # schedules them. An EN whose PVC already exists is pinned by the
      # volume's node affinity regardless of what this expresses.
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              podAffinityTerm:
                topologyKey: kubernetes.io/hostname
                labelSelector:
                  matchLabels:
                    app.kubernetes.io/name: autumn
                    app.kubernetes.io/component: extent-node
      containers:
        - name: extent-node
          image: ${IMAGE}
          imagePullPolicy: IfNotPresent
          args: ["extent-node"]
          envFrom:
            - configMapRef: { name: autumn-config }
          env:
            # Advertise this pod's OWN IP. The EN self-registers its location
            # under a stable node_uuid every startup, so a rescheduled pod's
            # fresh IP updates the same identity.
            - name: AUTUMN_ADVERTISE_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
            - name: AUTUMN_EXTENT_DATA
              value: /data/autumn
          ports:
            - { name: data, containerPort: 9101 }
            - { name: control, containerPort: 10101 }
          readinessProbe:
            tcpSocket: { port: 9101 }
            initialDelaySeconds: 5
            periodSeconds: 5
          volumeMounts:
            - { name: data, mountPath: /data }
          resources:
            requests: { cpu: "${CPU}", memory: "1Gi" }
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: data-autumn-en-${n}
YAML
}

cmd="${1:-}"; shift || true
[ $# -gt 0 ] || { echo "usage: $(basename "$0") {render|apply} <ordinal>..." >&2; exit 2; }
for n in "$@"; do
  case "$n" in ''|*[!0-9]*) echo "not an ordinal: $n" >&2; exit 2;; esac
done

case "$cmd" in
  render) for n in "$@"; do render_one "$n"; done ;;
  apply)  for n in "$@"; do render_one "$n"; done | kubectl apply -f - ;;
  *) echo "usage: $(basename "$0") {render|apply} <ordinal>..." >&2; exit 2 ;;
esac
