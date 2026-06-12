#!/usr/bin/env bash
# kvcache_chaos.sh — data-plane INTERFACE chaos: the python kvcache L3
# backend (sglang HiCache path, no sglang needed) under failover (F275).
#
# Boots a 2-PS cluster (tcp), runs python/autumn_kvcache/tests/
# chaos_workload.py (continuous batch_set_v1 + readback-verify through the
# python bridge), and injects faults:
#   K1: kill -9 the partition-holding PS → migration; the workload's OK
#       stream must resume; zero MISMATCH lines (ACKed pages never wrong)
#   K2: kill -9 the manager + exact-cmdline respawn → workload resumes
# Final: a FRESH python process re-reads EVERY manifested round byte-exact.
#
# Usage: AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/kvcache_chaos.sh
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MGR="127.0.0.1:9001"
WORK="$(mktemp -d /tmp/kvcache_chaos.XXXXXX)"
PSBIN="$ROOT/target/release/autumn-ps"
AO="$ROOT/target/release/autumn-op"
FAIL=0

say()  { echo "[kvc $(date +%H:%M:%S)] $*"; }
fail() { echo "[kvc $(date +%H:%M:%S)] FAIL: $*"; FAIL=1; }

export AUTUMN_DATA_ROOT="${AUTUMN_DATA_ROOT:-/data05/autumn-rs}"
say "cleaning + starting cluster"
for pid in $(ps -eo pid,comm | awk '$2 ~ /^(autumn-|etcd)/ {print $1}'); do kill -9 "$pid" 2>/dev/null; done
sleep 2
for i in $(seq 1 35); do
    busy=$(ss -tan 2>/dev/null | grep -cE ':(9001|9301|9351|2000[0-9]) ') || true
    [ "${busy:-0}" = "0" ] && break
    sleep 2
done
rm -rf "$AUTUMN_DATA_ROOT" /tmp/autumn-rs
env AUTUMN_EXTENT_BASE_PORT=20000 \
    AUTUMN_BOOTSTRAP_PRESPLIT="4:hexstring" \
    AUTUMN_TRANSPORT=tcp \
    bash "$ROOT/cluster.sh" start 3 > "$WORK/cluster.log" 2>&1
grep -q "bootstrap succeeded" "$WORK/cluster.log" || { echo "cluster start failed"; tail -10 "$WORK/cluster.log"; exit 1; }
setsid nohup "$PSBIN" --psid 2 --port 9351 --manager "$MGR" \
    --listen 127.0.0.1 --advertise 127.0.0.1:9351 --transport tcp \
    > "$WORK/ps2.log" 2>&1 < /dev/null &
sleep 5

AOC=(timeout 20 "$AO" --manager "$MGR" --transport tcp)

# ── workload (python L3 backend) ────────────────────────────────────────────
say "starting kvcache python workload"
: > "$WORK/manifest.txt"
( cd "$ROOT/python/autumn_kvcache" && \
  AUTUMN_KVCACHE_ENDPOINT="$MGR" CHAOS_STOP_FILE="$WORK/stop" CHAOS_MANIFEST="$WORK/manifest.txt" \
  python3 -m tests.chaos_workload > "$WORK/workload.log" 2>&1 ) &
WL_PID=$!

ok_count() {
    # NB: grep -c prints "0" AND exits 1 on zero matches — a `|| echo 0`
    # tail would double-print ("0\n0") and blow up arithmetic.
    local c
    c=$(grep -c "^OK " "$WORK/workload.log" 2>/dev/null) || true
    echo "${c:-0}"
}
wait_progress() { # wait until OK count grows past current within deadline
    local tag="$1" base now deadline=$((SECONDS + 90))
    base=$(ok_count)
    while [ $SECONDS -lt $deadline ]; do
        now=$(ok_count)
        [ "$now" -gt "$((base + 3))" ] && { say "[$tag] workload progressing ($base -> $now)"; return 0; }
        sleep 2
    done
    fail "[$tag] workload made no progress in 90s"
}
wait_progress "baseline"

# ── K1: kill the partition-holding PS under live kvcache traffic ───────────
p1=$("${AOC[@]}" info 2>/dev/null | grep -c "ps=127.0.0.1:9301") || p1=0
if [ "${p1:-0}" -ge 1 ]; then VID=1; VPORT=9301; else VID=2; VPORT=9351; fi
VPID=$(pgrep -f -- "--psid $VID .*--transport tcp" | head -1)
say "K1: kill -9 holder PS$VID pid=${VPID:-?}"
[ -n "$VPID" ] && kill -9 "$VPID"
wait_progress "after-PS-kill"
say "K1: respawn PS$VID"
setsid nohup "$PSBIN" --psid "$VID" --port "$VPORT" --manager "$MGR" \
    --listen 127.0.0.1 --advertise "127.0.0.1:$VPORT" --transport tcp \
    > "$WORK/ps${VID}_respawn.log" 2>&1 < /dev/null &
sleep 5

# ── K2: manager kill + respawn under live kvcache traffic ──────────────────
MPID=$(pgrep -f autumn-manager-server | head -1)
MCMD=$(tr '\0' ' ' < "/proc/$MPID/cmdline")
say "K2: kill -9 manager pid=$MPID"
kill -9 "$MPID"
sleep 6
setsid nohup $MCMD > "$WORK/mgr_respawn.log" 2>&1 < /dev/null &
wait_progress "after-mgr-kill"

# ── stop + in-run corruption check + fresh-process full verify ──────────────
touch "$WORK/stop"
wait "$WL_PID" 2>/dev/null
say "workload: $(tail -1 "$WORK/workload.log")"
mm=$(grep -c "^MISMATCH" "$WORK/workload.log") || mm=0
[ "${mm:-0}" = "0" ] || fail "in-run MISMATCH lines: $mm (ACKed pages corrupted)"

say "final verify: fresh process re-reads every manifested round"
if ( cd "$ROOT/python/autumn_kvcache" && \
     AUTUMN_KVCACHE_ENDPOINT="$MGR" python3 -m tests.chaos_workload --verify "$WORK/manifest.txt" \
     > "$WORK/verify.log" 2>&1 ); then
    say "final verify: $(tail -1 "$WORK/verify.log")"
else
    fail "final verify failed: $(tail -3 "$WORK/verify.log" | tr '\n' ' ')"
fi

for pid in $(ps -eo pid,comm | awk '$2 ~ /^(autumn-|etcd)/ {print $1}'); do kill -9 "$pid" 2>/dev/null; done
total=$(wc -l < "$WORK/manifest.txt")
if [ $FAIL -eq 0 ]; then say "PASS ($total rounds, work dir $WORK kept)"; else say "FAILED — logs in $WORK"; fi
exit $FAIL
