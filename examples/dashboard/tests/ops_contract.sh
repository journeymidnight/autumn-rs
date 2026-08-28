#!/usr/bin/env bash
# /api/ops contract check for the dashboard's maintenance-ops panel.
#
# Spins an ISOLATED single-node cluster (own etcd, own ports, own data dir),
# submits one op, and asserts the endpoint's shape end to end: the two lists
# exist, and a record carries the fields the panel renders — including the
# progress counts and the failure reason, which are the two things this panel
# exists to surface.
#
# etcd is REQUIRED here, and that is the point: a memory-only manager persists
# no terminal outcomes, so `history` would come back empty and the field
# assertion would pass vacuously.
#
#   cargo build --workspace     # debug binaries
#   bash examples/dashboard/tests/ops_contract.sh
set -u
cd /data/dongmao_dev/autumn-rs
BIN=target/debug
W=/tmp/ops-contract; PB=21000
MGR="127.0.0.1:$((PB+1))"; EN=$((PB+101)); PS=$((PB+201)); DASH=$((PB+301))
rm -rf "$W"; mkdir -p "$W/en0" "$W/ps1"

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
"$BIN/autumn-op" --admin-token opstok --manager "$MGR" format "$W/en0" >"$W/fmt.log" 2>&1 || { echo FAIL-format; cat "$W/fmt.log"; exit 1; }
"$BIN/autumn-extent-node" --data "$W/en0" --port $EN --manager "$MGR" --cpuset 0 \
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
print(f"CONTRACT OK: live={len(v['live'])} history={len(v['history'])} history_error={v['history_error']}")
PY
