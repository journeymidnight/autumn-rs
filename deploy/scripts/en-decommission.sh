#!/usr/bin/env bash
# Retire one extent node: fence -> wait for the drain -> remove -> delete the
# workload. This is the runbook in docs/ops.md ("Node decommission runbook")
# with the waiting and the checks done for you; it invents no new mechanism.
#
# The order is the whole point, and every step gates the next:
#   fence   the manager stops placing new data on the node and starts
#           rebuilding its sealed slots elsewhere;
#   drain   its shard count falls to 0 (watch it, don't assume it);
#   remove  server-side gated -- it REFUSES, listing the blocking extents,
#           until the manager has verified nothing references the node;
#   delete  only now is the workload's data redundant everywhere else.
#
# Deleting the workload first would look like it worked and would silently cost
# a replica of every shard the node still held.
#
# Usage:
#   en-decommission.sh [options] <ordinal>
#
# Options:
#   --yes             skip the confirmation prompt
#   --delete-pvc      also delete the PVC (default: keep it; see below)
#   --timeout <sec>   give up waiting for the drain (default: 7200)
#   --dry-run         print what would happen, change nothing
#
# The PVC is KEPT by default. Its StorageClass is `Retain`, so deleting the
# claim does not erase the disk -- but it does release a PV that nothing will
# reclaim automatically, and the node_id is tombstoned by `remove` so the data
# can never be re-adopted anyway. Keeping it leaves the evidence in place for
# as long as you want it; delete it deliberately, once.
set -euo pipefail

NS="${AUTUMN_NS:-autumn}"
MGR_POD="${AUTUMN_MANAGER_POD:-autumn-manager-0}"
ADMIN_TOKEN="${AUTUMN_ADMIN_TOKEN_FILE:-/etc/autumn/authz/admin.token}"
# Minimum healthy nodes that must REMAIN. Placement hard-excludes fenced nodes,
# so a cluster left with fewer eligible nodes than the replica count refuses
# new extent allocation -- loudly, but only once you try to write.
MIN_REMAINING="${AUTUMN_MIN_REMAINING_NODES:-3}"

YES=0; DELETE_PVC=0; DRY=0; TIMEOUT=7200
while [ $# -gt 0 ]; do
  case "$1" in
    --yes) YES=1; shift ;;
    --delete-pvc) DELETE_PVC=1; shift ;;
    --dry-run) DRY=1; shift ;;
    --timeout) TIMEOUT="$2"; shift 2 ;;
    -h|--help) sed -n '1,30p' "$0"; exit 0 ;;
    -*) echo "unknown option: $1" >&2; exit 2 ;;
    *) ORDINAL="$1"; shift ;;
  esac
done
[ -n "${ORDINAL:-}" ] || { echo "usage: $(basename "$0") [options] <ordinal>" >&2; exit 2; }
case "$ORDINAL" in ''|*[!0-9]*) echo "not an ordinal: $ORDINAL" >&2; exit 2;; esac

WORKLOAD="autumn-en-${ORDINAL}"
PVC="data-autumn-en-${ORDINAL}"
say() { printf '\n=== %s\n' "$*"; }
ao() { kubectl -n "$NS" exec "$MGR_POD" -- autumn-op --manager 127.0.0.1:9001 "$@"; }
ao_admin() {
  kubectl -n "$NS" exec "$MGR_POD" -- \
    autumn-op --manager 127.0.0.1:9001 --admin-token-file "$ADMIN_TOKEN" "$@"
}

# --- find the pod ---------------------------------------------------------
# By label, because a Deployment's pod is `autumn-en-7-<rs>-<pod>`, not
# `autumn-en-7`. More than one match means two processes are on this EN's disk
# (the EN takes no lock on its data dir) — refuse rather than retire under it.
PODS="$(kubectl -n "$NS" get pod \
  -l "app.kubernetes.io/component=extent-node,autumn.dev/en-ordinal=${ORDINAL}" \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"
if [ -z "$PODS" ]; then
  # Not migrated yet: a StatefulSet pod carries no ordinal label and is named
  # for its ordinal.
  if kubectl -n "$NS" get pod "$WORKLOAD" >/dev/null 2>&1; then
    if kubectl -n "$NS" get sts autumn-en >/dev/null 2>&1; then
      echo "$WORKLOAD is still owned by the autumn-en StatefulSet." >&2
      echo "  A StatefulSet cannot retire one ordinal -- \`scale\` only removes from the top," >&2
      echo "  which on this cluster deletes the WRONG nodes. Migrate to per-EN Deployments" >&2
      echo "  first (docs/ops.md, \"Migrating the extent nodes off the StatefulSet\")." >&2
      exit 1
    fi
    PODS="$WORKLOAD"
  fi
fi
[ -n "$PODS" ] || { echo "no pod found for extent node ${ORDINAL}" >&2; exit 1; }
if [ "$(printf '%s\n' "$PODS" | wc -l | tr -d ' ')" -gt 1 ]; then
  echo "refusing: more than one pod is running extent node ${ORDINAL}:" >&2
  printf '  %s\n' $PODS >&2
  echo "  Two processes on one data dir corrupt it; the EN takes no lock." >&2
  exit 1
fi
POD="$PODS"

# --- resolve the pod to the cluster's node_id -----------------------------
# The EN advertises its own pod IP, so the pod IP is the join key. Matching on
# anything else (ordinal, workload name) would be guessing: the manager knows
# nothing about either.
POD_IP="$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.podIP}' 2>/dev/null || true)"
[ -n "$POD_IP" ] || { echo "$POD has no pod IP (not running?) -- refusing to guess its node_id" >&2; exit 1; }

INFO="$(ao info)"
NODE_LINE="$(printf '%s\n' "$INFO" | awk -v ip="$POD_IP:" '$1=="node" && $3 ~ ("^" ip) {print}')"
[ -n "$NODE_LINE" ] || { echo "no cluster node advertises $POD_IP -- already removed?" >&2; exit 1; }
NODE_ID="$(printf '%s\n' "$NODE_LINE" | awk '{print $2}')"
SHARDS="$(printf '%s\n' "$NODE_LINE" | awk '{print $4}')"
# `info` prints: node <id> <addr> <n> extent shards  <auto> <override>[note].
# Count only nodes that are BOTH auto-Online and un-overridden: an already
# fenced node still prints Online in the auto column, so a looser match would
# count nodes that are themselves draining as available capacity.
ONLINE="$(printf '%s\n' "$INFO" | awk '$1=="node" && $7=="Online" && $8=="-"' | wc -l | tr -d ' ')"

say "target"
printf '  pod        %s (%s)\n  node_id    %s\n  shards     %s\n  healthy    %s -> %s after\n' \
  "$POD" "$POD_IP" "$NODE_ID" "$SHARDS" "$ONLINE" "$((ONLINE - 1))"

if [ "$((ONLINE - 1))" -lt "$MIN_REMAINING" ]; then
  echo "refusing: only $((ONLINE - 1)) node(s) would remain, below AUTUMN_MIN_REMAINING_NODES=$MIN_REMAINING" >&2
  exit 1
fi

if [ "$DRY" = 1 ]; then say "dry run -- stopping here"; exit 0; fi
if [ "$YES" != 1 ]; then
  printf '\nretire node %s (%s)? this moves %s shards. [y/N] ' "$NODE_ID" "$POD" "$SHARDS"
  read -r a; case "$a" in y|Y) ;; *) echo "aborted"; exit 1 ;; esac
fi

# --- 1. fence -------------------------------------------------------------
say "1/4 fence node $NODE_ID"
ao_admin fence-node "$NODE_ID" --reason "decommission $POD" --by "${USER:-en-decommission.sh}"

# --- 2. drain -------------------------------------------------------------
# Shard count -> 0 is the drain. `ops list` carries live byte progress for the
# rebuilds themselves, which is what separates "slow" from "stuck".
say "2/4 wait for the drain (shard count -> 0, timeout ${TIMEOUT}s)"
START=$(date +%s); LAST=""
while :; do
  LEFT="$(ao info 2>/dev/null | awk -v n="$NODE_ID" '$1=="node" && $2==n {print $4}')"
  [ -n "$LEFT" ] || { echo "  node $NODE_ID no longer listed -- treating as drained"; break; }
  [ "$LEFT" = "0" ] && { echo "  drained"; break; }
  NOW=$(date +%s); EL=$((NOW - START))
  if [ "$EL" -ge "$TIMEOUT" ]; then
    echo "  still $LEFT shard(s) after ${EL}s -- NOT removing. Diagnose with:" >&2
    echo "    autumn-op extent-health --node $NODE_ID --all" >&2
    echo "    autumn-op recovery-stats" >&2
    echo "    autumn-op ops list --active     # per-extent byte progress" >&2
    exit 1
  fi
  [ "$LEFT" != "$LAST" ] && { printf '  %4ds  %s shards left\n' "$EL" "$LEFT"; LAST="$LEFT"; }
  sleep 10
done

# --- 3. remove ------------------------------------------------------------
# Server-side gated: it refuses with the blocking extent ids until the manager
# has verified nothing references the node. Zero shards in `info` and a clean
# `remove` are not the same check, so let the server have the last word.
say "3/4 remove node $NODE_ID from the cluster"
ao_admin remove "$NODE_ID" --by "${USER:-en-decommission.sh}"

# --- 4. delete the workload ----------------------------------------------
say "4/4 delete the workload"
kubectl -n "$NS" delete deployment "$WORKLOAD" --ignore-not-found
if [ "$DELETE_PVC" = 1 ]; then
  kubectl -n "$NS" delete pvc "$PVC" --ignore-not-found
else
  printf '  PVC %s kept. Delete it deliberately when you no longer want the evidence:\n    kubectl -n %s delete pvc %s\n' "$PVC" "$NS" "$PVC"
fi

say "done -- node $NODE_ID retired"
ao info | awk '$1=="node"'
