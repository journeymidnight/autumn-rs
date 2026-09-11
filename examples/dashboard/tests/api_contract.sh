#!/usr/bin/env bash
# API contract check for the dashboard's panels, against a REAL cluster.
#
# Spins an ISOLATED single-node cluster (own etcd, own ports, own data dir, TWO
# disks on the node), submits one op, and asserts the shape each tab renders
# from, end to end:
#   /api/ops            - the two lists + the progress counts and failure reason
#   /api/overview       - ps_servers (the Servers tab) and per-node `disks`
#                         (the Nodes tab), neither derivable from anything the
#                         page already had
#   /api/partition/<id> - has_overlap, the precondition the drawer warns on
#
# A field check here is worth more than a unit test: every one of these crosses
# the rkyv wire, the manager compose, and the autumn-op subprocess, and a
# missing key renders as a silently blank panel.
#
# etcd is REQUIRED, and that is the point: a memory-only manager persists no
# terminal outcomes, so `history` would come back empty and the ops field
# assertion would pass vacuously.
#
#   cargo build --workspace     # debug binaries
#   bash examples/dashboard/tests/api_contract.sh
set -u
cd /data/dongmao_dev/autumn-rs
BIN=target/debug
W=/tmp/ops-contract; PB=21000
MGR="127.0.0.1:$((PB+1))"; EN=$((PB+101)); PS=$((PB+201)); DASH=$((PB+301))
rm -rf "$W"; mkdir -p "$W/en0/d0" "$W/en0/d1" "$W/ps1"

# A previous run that died before its trap fired leaves an etcd squatting on
# these ports, and the only symptom is "address already in use" from a process
# that is not this run's. Clear our OWN port band first — matched by port, never
# by process name, so this cannot reach a real cluster's etcd.
for p in $((PB+401)) $((PB+402)) $((PB+1)) $((PB+101)) $((PB+201)) $((PB+301)); do
  owner=$(ss -ltnp 2>/dev/null | grep ":$p " | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2)
  [ -n "${owner:-}" ] && { echo "[pre] freeing :$p (pid $owner)"; kill -9 "$owner" 2>/dev/null; }
done
sleep 1
PIDS=(); cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; }
trap cleanup EXIT
wait_port(){ for _ in $(seq 1 25); do ss -ltn 2>/dev/null | grep -q ":$1\b" && return 0; sleep 1; done; return 1; }

ETCD_PORT=$((PB+401))
etcd --name opsctr --data-dir "$W/etcd" \
  --listen-client-urls "http://127.0.0.1:$ETCD_PORT" --advertise-client-urls "http://127.0.0.1:$ETCD_PORT" \
  --listen-peer-urls "http://127.0.0.1:$((ETCD_PORT+1))" --initial-advertise-peer-urls "http://127.0.0.1:$((ETCD_PORT+1))" \
  --initial-cluster "opsctr=http://127.0.0.1:$((ETCD_PORT+1))" >"$W/etcd.log" 2>&1 & PIDS+=($!)
wait_port $ETCD_PORT || { echo FAIL-etcd; tail -5 "$W/etcd.log"; exit 1; }
"$BIN/autumn-manager-server" --port $((PB+1)) --listen 127.0.0.1 --admin-token opstok --etcd "127.0.0.1:$ETCD_PORT" >"$W/mgr.log" 2>&1 & PIDS+=($!)
wait_port $((PB+1)) || { echo FAIL-mgr; tail -5 "$W/mgr.log"; exit 1; }
# TWO disks: a node-level rollup describes one disk perfectly well, so a
# one-disk node cannot show whether the per-disk rows are real.
"$BIN/autumn-op" --admin-token opstok --manager "$MGR" format "$W/en0/d0" "$W/en0/d1" >"$W/fmt.log" 2>&1 || { echo FAIL-format; cat "$W/fmt.log"; exit 1; }
"$BIN/autumn-extent-node" --data "$W/en0/d0,$W/en0/d1" --port $EN --manager "$MGR" --cpuset 0 \
  --advertise "127.0.0.1:$EN" --listen 127.0.0.1 >"$W/en.log" 2>&1 & PIDS+=($!)
wait_port $EN || { echo FAIL-en; tail -5 "$W/en.log"; exit 1; }
sleep 3
"$BIN/autumn-op" --admin-token opstok --manager "$MGR" bootstrap --replication 1+0 >"$W/boot.log" 2>&1 || { echo FAIL-boot; cat "$W/boot.log"; exit 1; }
"$BIN/autumn-ps" --psid 1 --port $PS --manager "$MGR" --data "$W/ps1" \
  --listen 127.0.0.1 --advertise "127.0.0.1:$PS" >"$W/ps.log" 2>&1 & PIDS+=($!)
wait_port $PS || { echo FAIL-ps; tail -5 "$W/ps.log"; exit 1; }
sleep 4

echo "--- submit a compact op (gives live + history something to show) ---"
"$BIN/autumn-op" --admin-token opstok --manager "$MGR" compact 1 2>&1 | tail -2

"$BIN/autumn-dashboard" --manager "$MGR" --autumn-op "$BIN/autumn-op" \
  --port $DASH --listen 127.0.0.1 --admin-token opstok >"$W/dash.log" 2>&1 & PIDS+=($!)
wait_port $DASH || { echo FAIL-dash; tail -20 "$W/dash.log"; exit 1; }
sleep 8
echo "--- GET / (the page itself) ---"
# `static/index.html` is baked in with include_str!, so a binary built before a
# page edit serves the OLD markup and every browser check silently tests the
# wrong file. Compare what is served against what is on disk.
curl -s "http://127.0.0.1:$DASH/" > "$W/index.html" || { echo FAIL-curl; exit 1; }
if ! cmp -s "$W/index.html" examples/dashboard/static/index.html; then
  echo "FAIL-stale-page: the served page differs from static/index.html — rebuild autumn-dashboard"
  exit 1
fi
grep -q 'role="tablist"' "$W/index.html" || { echo "FAIL-no-tabs"; exit 1; }
echo "PAGE OK: served bytes match static/index.html ($(wc -c <"$W/index.html") bytes)"

echo "--- GET /api/ops ---"
curl -s "http://127.0.0.1:$DASH/api/ops" > "$W/ops.json" || { echo FAIL-curl; exit 1; }
head -c 900 "$W/ops.json"; echo
python3 - "$W/ops.json" <<'PY'
import json,sys
v=json.load(open(sys.argv[1]))
assert set(["live","history","history_error"]) <= set(v), f"missing keys: {list(v)}"
assert isinstance(v["live"], list) and isinstance(v["history"], list), "live/history must be arrays"
allops = v["live"] + v["history"]
if allops:
    need = {"op_id","kind","state","progress_done","progress_total","started_at","finished_at"}
    missing = need - set(allops[0])
    assert not missing, f"op record missing fields: {missing}"
print(f"OPS CONTRACT OK: live={len(v['live'])} history={len(v['history'])} history_error={v['history_error']}")
PY

echo "--- GET /api/overview ---"
curl -s "http://127.0.0.1:$DASH/api/overview" > "$W/overview.json" || { echo FAIL-curl; exit 1; }
python3 - "$W/overview.json" <<'PYOV'
import json,sys
v=json.load(open(sys.argv[1]))
assert not v.get("errors"), f"overview reported errors: {v['errors']}"

# Servers tab: the REGISTERED fleet, not the fleet derived from the partitions.
ps = v.get("ps_servers")
assert isinstance(ps, list) and ps, f"ps_servers missing/empty: {sorted(v)}"
need = {"ps_id","addr","last_heartbeat_secs_ago","partition_count","n","size",
        "req_per_sec","write_bytes_per_sec","read_bytes_per_sec","total_extents"}
missing = need - set(ps[0])
assert not missing, f"ps_servers row missing fields: {missing}"
hb = ps[0]["last_heartbeat_secs_ago"]
assert hb is not None and hb < 60, f"a live PS must have a fresh heartbeat, got {hb}"

# Nodes tab: the per-disk rows, which the node-level rollup cannot express.
nodes = v.get("nodes") or []
assert nodes, "no nodes in overview"
disks = nodes[0].get("disks")
assert isinstance(disks, list) and len(disks) == 2, f"expected the node's 2 disks, got {disks}"
dneed = {"disk_id","uuid","total","free","extent_bytes","reported","online","faulted"}
dmissing = dneed - set(disks[0])
assert not dmissing, f"disk row missing fields: {dmissing}"
assert all(d["reported"] for d in disks), f"a live node describes all its disks: {disks}"
assert all(d["online"] and not d["faulted"] for d in disks), f"healthy disks expected: {disks}"
assert all(d["total"] > 0 for d in disks), f"statvfs capacity expected: {disks}"
assert len({d["disk_id"] for d in disks}) == 2, "the two disks must have distinct ids"
assert all(d["uuid"] for d in disks), "each disk carries the uuid `format` stamped"
print(f"OVERVIEW CONTRACT OK: ps_servers={len(ps)} nodes={len(nodes)} disks={len(disks)}")
PYOV

echo "--- GET /api/partition/<id> ---"
PID=$(python3 -c "import json;print((json.load(open('$W/overview.json'))['partitions'] or [{}])[0].get('part_id',''))")
[ -n "$PID" ] || { echo FAIL-no-partition; exit 1; }
curl -s "http://127.0.0.1:$DASH/api/partition/$PID" > "$W/part.json" || { echo FAIL-curl; exit 1; }
python3 - "$W/part.json" <<'PYPART'
import json,sys
v=json.load(open(sys.argv[1]))
# has_overlap is the precondition the drawer warns on BEFORE the Split button is
# clicked; a freshly bootstrapped partition has never CoW-split, so it is 0.
assert "has_overlap" in v, f"partition detail missing has_overlap: {sorted(v)}"
assert v["has_overlap"] == 0, f"a never-split partition must not report overlap: {v['has_overlap']}"
assert isinstance(v.get("extents"), list), "the drawer needs the extent list"
print(f"PARTITION CONTRACT OK: part has_overlap={v['has_overlap']} extents={len(v['extents'])}")
PYPART
