#!/usr/bin/env bash
# validate.sh — clusterless validation of the autumn-rs deploy artifacts.
#
# No docker / kubectl / cluster required. Checks:
#   - shell scripts parse (bash -n): entrypoint.sh, autumn-deploy
#   - every k8s manifest is well-formed (apiVersion/kind/metadata.name)
#   - StatefulSet/Job selector.matchLabels ⊆ template labels (k8s hard rule)
#   - each StatefulSet.serviceName resolves to a headless Service
#   - the rendered EN Deployment advertises its pod IP (AUTUMN_ADVERTISE_IP ←
#     status.podIP) and mounts its own PVC by name — no per-pod Services
#   - shell scripts parse: entrypoint.sh, autumn-deploy, the EN scripts
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
for s in "$HERE/docker/entrypoint.sh" "$HERE/baremetal/autumn-deploy" \
         "$HERE/scripts/en-workload.sh" "$HERE/scripts/en-decommission.sh"; do
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
# The extent nodes are not manifests -- they are rendered per EN by
# deploy/scripts/en-workload.sh -- so validate the renderer's output instead.
# This used to read the `autumn-en` StatefulSet, and when the ENs stopped being
# one the whole check silently became a no-op while still printing OK.
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

echo "== rendered extent node =="
# The ENs are not kustomize resources; the renderer is the only place their pod
# spec exists, so it is what has to be checked.
en_rendered="$(mktemp)"
bash "$HERE/scripts/en-workload.sh" render 7 > "$en_rendered"
python3 - "$en_rendered" <<'ENPY' || fail=1
import sys
try:
    import yaml
except ImportError:
    print("  FAIL: pyyaml not installed"); sys.exit(1)
with open(sys.argv[1]) as fh:
    docs = [d for d in yaml.safe_load_all(fh) if d]
bad_n = 0
def bad(m):
    global bad_n; print(f"  FAIL: {m}"); bad_n = 1
def ok(m): print(f"  {m}")

dep = next((d for d in docs if d["kind"] == "Deployment"), None)
pvc = next((d for d in docs if d["kind"] == "PersistentVolumeClaim"), None)
if not dep or not pvc:
    bad("renderer must emit both a Deployment and a PersistentVolumeClaim"); sys.exit(1)

# k8s hard rule, and easy to break by hand-editing the template.
sel = dep["spec"]["selector"]["matchLabels"]
lbl = dep["spec"]["template"]["metadata"]["labels"]
if all(lbl.get(k) == v for k, v in sel.items()):
    ok("selector.matchLabels subset of template labels")
else:
    bad("Deployment selector.matchLabels is not a subset of the template labels")

# Identity: the EN self-registers under the node_uuid on its PVC and advertises
# whatever pod IP it currently has. Both halves must be right, or a rescheduled
# pod either loses its identity or advertises an address nothing can dial.
c = dep["spec"]["template"]["spec"]["containers"][0]
adv = next((e for e in (c.get("env") or []) if e["name"] == "AUTUMN_ADVERTISE_IP"), None)
if ((adv or {}).get("valueFrom", {}).get("fieldRef", {}).get("fieldPath")) == "status.podIP":
    ok("advertises pod IP (AUTUMN_ADVERTISE_IP <- status.podIP)")
else:
    bad("EN must set AUTUMN_ADVERTISE_IP from fieldRef status.podIP")

vols = {v["name"]: v for v in dep["spec"]["template"]["spec"]["volumes"]}
claim = vols.get("data", {}).get("persistentVolumeClaim", {}).get("claimName")
if claim == pvc["metadata"]["name"]:
    ok(f"mounts its own claim by name ({claim})")
else:
    bad(f"Deployment mounts {claim!r}, renderer emitted claim {pvc['metadata']['name']!r}")

# A surge pod cannot exist: ReadWriteOnce on a node-pinned local volume means
# the replacement would wait forever for a disk its predecessor still holds.
if dep["spec"].get("strategy", {}).get("type") == "Recreate":
    ok("strategy Recreate (RWO on a node-pinned volume cannot be surged)")
else:
    bad("EN Deployment must use strategy Recreate")

sys.exit(bad_n)
ENPY
rm -f "$en_rendered"

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
