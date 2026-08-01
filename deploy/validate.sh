#!/usr/bin/env bash
# validate.sh — clusterless validation of the autumn-rs deploy artifacts.
#
# No docker / kubectl / cluster required. Checks:
#   - shell scripts parse (bash -n): entrypoint.sh, autumn-deploy
#   - every k8s manifest is well-formed (apiVersion/kind/metadata.name)
#   - StatefulSet/Job selector.matchLabels ⊆ template labels (k8s hard rule)
#   - each StatefulSet.serviceName resolves to a headless Service
#   - the EN StatefulSet advertises its pod IP (AUTUMN_ADVERTISE_IP ← status.podIP;
# M2 — no per-pod ClusterIP Services anymore)
#   - Service ports line up with the entrypoint's role port defaults
#   - kustomization.resources all exist; image name matches
#
# Run: bash deploy/validate.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0
note() { echo "  $*"; }
bad()  { echo "  FAIL: $*"; fail=1; }

echo "== shell syntax =="
for s in "$HERE/docker/entrypoint.sh" "$HERE/baremetal/autumn-deploy"; do
    if bash -n "$s" 2>/dev/null; then note "OK  $(basename "$s")"; else bad "bash -n $(basename "$s")"; fi
done

echo "== bare-metal topology examples source cleanly =="
for t in "$HERE/baremetal/topology.conf" "$HERE/baremetal/topology-singlehost.conf"; do
    if bash -n "$t" 2>/dev/null; then note "OK  $(basename "$t")"; else bad "bash -n $(basename "$t")"; fi
done

echo "== k8s manifests =="
python3 - "$HERE/k8s" <<'PY' || fail=1
import sys, os, glob
try:
    import yaml
except ImportError:
    print("  FAIL: pyyaml not installed (pip install pyyaml)"); sys.exit(1)

kdir = sys.argv[1]
docs = []           # (file, doc)
for f in sorted(glob.glob(os.path.join(kdir, "*.yaml"))):
    if os.path.basename(f) == "kustomization.yaml":
        continue
    with open(f) as fh:
        for d in yaml.safe_load_all(fh):
            if d:
                docs.append((os.path.basename(f), d))

rc = 0
def bad(m):
    global rc; print(f"  FAIL: {m}"); rc = 1
def ok(m):
    print(f"  OK  {m}")

# index services + workloads + storageclasses
services = {}       # name -> doc
workloads = []      # (kind, doc)
storageclasses = set()
CLUSTER_SCOPED = ("Namespace", "StorageClass", "ClusterRole", "ClusterRoleBinding")
for f, d in docs:
    kind = d.get("kind"); name = (d.get("metadata") or {}).get("name")
    if not d.get("apiVersion") or not kind:
        bad(f"{f}: missing apiVersion/kind"); continue
    if kind != "Namespace" and not name:
        bad(f"{f}: missing metadata.name")
    if kind == "StorageClass":
        storageclasses.add(name)
    # Cluster-scoped kinds have no namespace — skip the namespace check.
    if kind in CLUSTER_SCOPED:
        continue
    ns = (d.get("metadata") or {}).get("namespace")
    if ns != "autumn":
        bad(f"{f}/{name}: namespace should be 'autumn', got {ns!r}")
    if kind == "Service":
        services[name] = d
    if kind in ("StatefulSet", "Job", "Deployment"):
        workloads.append((kind, d))

# volumeClaimTemplate storageClassName (when set) must resolve to a StorageClass
# shipped in the base (an omitted class = cluster default, which is allowed).
for kind, d in workloads:
    if kind != "StatefulSet":
        continue
    for vct in (d["spec"].get("volumeClaimTemplates") or []):
        scn = (vct.get("spec") or {}).get("storageClassName")
        if scn is None:
            ok(f"{d['metadata']['name']}: PVC uses the cluster default StorageClass")
        elif scn in storageclasses:
            ok(f"{d['metadata']['name']}: PVC storageClassName '{scn}' is defined")
        else:
            bad(f"{d['metadata']['name']}: PVC storageClassName '{scn}' not defined in the base")

# selector.matchLabels subset of template labels (k8s rejects otherwise)
for kind, d in workloads:
    name = d["metadata"]["name"]
    tmpl_labels = (((d["spec"].get("template") or {}).get("metadata") or {}).get("labels") or {})
    if kind == "Job":
        # Jobs auto-generate a selector; only require template labels exist.
        if not tmpl_labels:
            bad(f"{name}: Job template has no labels")
        else:
            ok(f"{name}: template labels present")
        continue
    sel = ((d["spec"].get("selector") or {}).get("matchLabels") or {})
    if not sel:
        bad(f"{name}: no spec.selector.matchLabels"); continue
    missing = {k: v for k, v in sel.items() if tmpl_labels.get(k) != v}
    if missing:
        bad(f"{name}: selector not a subset of template labels: {missing}")
    else:
        ok(f"{name}: selector ⊆ template labels")
    # serviceName must be a headless Service
    svc_name = d["spec"].get("serviceName")
    if kind == "StatefulSet":
        if svc_name not in services:
            bad(f"{name}: serviceName '{svc_name}' has no Service")
        elif services[svc_name]["spec"].get("clusterIP") != "None":
            bad(f"{name}: serviceName '{svc_name}' is not headless (clusterIP: None)")
        else:
            ok(f"{name}: serviceName '{svc_name}' is a headless Service")

# M2: the EN advertises its OWN pod IP (Downward-API
# status.podIP) and self-registers under a stable node_uuid — there are NO
# per-pod ClusterIP Services anymore. Validate the pod-IP wiring instead:
# the EN StatefulSet must inject AUTUMN_ADVERTISE_IP from status.podIP.
# (Any leftover per-pod Service is still sanity-checked to target a real
# ordinal, but it is no longer required.)
en_ss = next((d for k, d in workloads if k == "StatefulSet" and d["metadata"]["name"] == "autumn-en"), None)
if en_ss:
    replicas = en_ss["spec"].get("replicas", 1)
    valid_pods = {f"autumn-en-{i}" for i in range(replicas)}
    perpod = [s for n, s in services.items()
              if (s["spec"].get("selector") or {}).get("statefulset.kubernetes.io/pod-name")]
    for s in perpod:
        pod = s["spec"]["selector"]["statefulset.kubernetes.io/pod-name"]
        if pod not in valid_pods:
            bad(f"per-pod Service {s['metadata']['name']} targets '{pod}' outside replicas={replicas}")
        else:
            ok(f"per-pod Service {s['metadata']['name']} → {pod}")
    # EN must advertise its pod IP via the Downward API (the M2 identity model).
    containers = en_ss["spec"]["template"]["spec"].get("containers", [])
    adv = None
    for c in containers:
        for e in (c.get("env") or []):
            if e.get("name") == "AUTUMN_ADVERTISE_IP":
                adv = e
    fp = (((adv or {}).get("valueFrom") or {}).get("fieldRef") or {}).get("fieldPath")
    if fp == "status.podIP":
        ok("autumn-en advertises pod IP (AUTUMN_ADVERTISE_IP ← status.podIP)")
    else:
        bad("autumn-en StatefulSet must set AUTUMN_ADVERTISE_IP from fieldRef status.podIP")

# port defaults must match the entrypoint role ports
def svc_ports(n):
    return {p["port"] for p in services.get(n, {}).get("spec", {}).get("ports", [])}
expect = {
    "autumn-manager": {9001},
    "autumn-ps":      {9301},
}
for n, ports in expect.items():
    if n in services and not ports <= svc_ports(n):
        bad(f"Service {n} ports {svc_ports(n)} missing {ports}")
    elif n in services:
        ok(f"Service {n} exposes {ports}")
# EN per-pod + headless must expose data 9101 AND control 10101
for n, s in services.items():
    if (s.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component") == "extent-node"):
        if not {9101, 10101} <= svc_ports(n):
            bad(f"EN Service {n} must expose 9101 (data) + 10101 (control), got {svc_ports(n)}")
        else:
            ok(f"EN Service {n} exposes data+control")

sys.exit(rc)
PY

echo "== kustomization =="
python3 - "$HERE/k8s" <<'PY' || fail=1
import sys, os, yaml
kdir = sys.argv[1]
with open(os.path.join(kdir, "kustomization.yaml")) as f:
    k = yaml.safe_load(f)
rc = 0
for r in k.get("resources", []):
    if not os.path.exists(os.path.join(kdir, r)):
        print(f"  FAIL: kustomization resource missing: {r}"); rc = 1
    else:
        print(f"  OK  resource {r}")
imgs = {i["name"] for i in k.get("images", [])}
if "autumn-rs" not in imgs:
    print("  FAIL: kustomization does not pin the 'autumn-rs' image"); rc = 1
else:
    print("  OK  image 'autumn-rs' pinned")
sys.exit(rc)
PY

echo "== role args match entrypoint dispatch =="
python3 - "$HERE" <<'PY' || fail=1
import sys, os, glob, yaml, re
here = sys.argv[1]
# roles the entrypoint dispatches on
ep = open(os.path.join(here, "docker", "entrypoint.sh")).read()
roles = set(re.findall(r'^\s*(manager|extent-node|ps|bootstrap)\)\s', ep, re.M))
rc = 0
for f in glob.glob(os.path.join(here, "k8s", "*.yaml")):
    for d in yaml.safe_load_all(open(f)):
        if not d: continue
        spec = (((d.get("spec") or {}).get("template") or {}).get("spec")) or d.get("spec")
        for c in ((spec or {}).get("containers") or []):
            if c.get("image", "").startswith("autumn-rs"):
                a = (c.get("args") or [None])[0]
                if a not in roles:
                    print(f"  FAIL: {os.path.basename(f)} container arg '{a}' not an entrypoint role {sorted(roles)}"); rc=1
                else:
                    print(f"  OK  {os.path.basename(f)}: role '{a}'")
sys.exit(rc)
PY

echo
if (( fail )); then echo "VALIDATION FAILED"; exit 1; else echo "VALIDATION OK"; fi
