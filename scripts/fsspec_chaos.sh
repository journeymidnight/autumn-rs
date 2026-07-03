#!/usr/bin/env bash
# fsspec_chaos.sh — data-plane INTERFACE chaos: autumn-fsspec under failover.
#
# Boots a 2-PS cluster.sh cluster (tcp), runs a continuous file workload
# through `AutumnFileSystem` (pipe_file/cat_file/rm — the chunked-layout
# fsspec → SDK → PS → EN path), and injects faults:
#   F1: kill -9 the psid-1 PS → partitions migrate to PS2; workload OK
#       lines must resume; then respawn PS1
#   F2: kill -9 the manager + exact-cmdline respawn → workload resumes
# Final: fresh-process verification of EVERY manifested file (byte-exact +
# find/info consistency) + a post-settle WRITE-LIVENESS probe.
#
# Timeout-uncertainty ([[feedback_chaos_timeout_uncertain]]): ops that raise
# during fault windows are dropped from the manifest — only ACKed writes are
# integrity-checked. MISMATCH/VERIFY-FAIL = real corruption = FAIL.
#
# Usage: AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/fsspec_chaos.sh
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MGR="127.0.0.1:9001"
WORK="$(mktemp -d /tmp/fsspec_chaos.XXXXXX)"
PSBIN="$ROOT/target/release/autumn-ps"
AO="$ROOT/target/release/autumn-op"
FAIL=0

say()  { echo "[fsspec $(date +%H:%M:%S)] $*"; }
fail() { echo "[fsspec $(date +%H:%M:%S)] FAIL: $*"; FAIL=1; }

export AUTUMN_DATA_ROOT="${AUTUMN_DATA_ROOT:-/data05/autumn-rs}"
say "cleaning + starting cluster (logs: $WORK)"
for pid in $(ps -eo pid,comm | awk '$2 ~ /^(autumn-|etcd)/ {print $1}'); do kill -9 "$pid" 2>/dev/null; done
sleep 2
say "draining cluster ports"
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

# ── workload ────────────────────────────────────────────────────────────────
MANIFEST="$WORK/manifest.txt"
STOP="$WORK/stop"
WLOG="$WORK/workload.log"
say "starting fsspec workload"
( cd "$ROOT/python/autumn_fsspec" && \
  env AUTUMN_MANAGER="$MGR" CHAOS_STOP_FILE="$STOP" \
      python3 tests/chaos_workload.py "$MANIFEST" > "$WLOG" 2>&1 ) &
WPID=$!

wait_ok() { # wait_ok <label> <timeout_s>: an OK line must appear within budget
    local before after deadline
    before=$(grep -c '^OK ' "$WLOG" 2>/dev/null || true)
    deadline=$(( $(date +%s) + $2 ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        after=$(grep -c '^OK ' "$WLOG" 2>/dev/null || true)
        [ "${after:-0}" -gt "${before:-0}" ] && { say "$1: OK progressing (${before}→${after})"; return 0; }
        sleep 1
    done
    fail "$1: no OK progress within $2 s"
    return 1
}

wait_ok "warmup" 60 || { touch "$STOP"; exit 1; }
sleep 5

# ── F1: kill -9 PS1 (partition holder) → migration → respawn ──────────────
PS1PID=$(pgrep -f -- "--psid 1 .*--transport tcp" | head -1)
say "F1: kill -9 PS1 pid=${PS1PID:-?}"
[ -n "${PS1PID:-}" ] && kill -9 "$PS1PID"
wait_ok "F1 post-PS1-kill (migration)" 120
say "F1: respawn PS1"
setsid nohup "$PSBIN" --psid 1 --port 9301 --manager "$MGR" \
    --listen 127.0.0.1 --advertise 127.0.0.1:9301 --transport tcp \
    > "$WORK/ps1_respawn.log" 2>&1 < /dev/null &
sleep 5
wait_ok "F1 post-PS1-respawn" 60

# ── F2: kill -9 manager + exact-cmdline respawn ────────────────────────────
MPID=$(pgrep -f autumn-manager-server | head -1)
MCMD=$(tr '\0' ' ' < "/proc/$MPID/cmdline")
say "F2: kill -9 manager pid=$MPID"
kill -9 "$MPID"
sleep 3
say "F2: respawn manager: $MCMD"
setsid nohup $MCMD > "$WORK/manager_respawn.log" 2>&1 < /dev/null &
wait_ok "F2 post-manager-respawn" 120

# ── stop workload + final verification ─────────────────────────────────────
sleep 10
say "stopping workload"
touch "$STOP"
wait "$WPID" 2>/dev/null
WEXIT=$?
tail -1 "$WLOG"
[ "$WEXIT" = "0" ] || fail "workload exited $WEXIT (MISMATCH during run)"
grep -q '^MISMATCH ' "$WLOG" && fail "MISMATCH lines present (corruption)"

NFILES=$(wc -l < "$MANIFEST" 2>/dev/null || echo 0)
say "final verify: $NFILES manifested files, fresh process"
( cd "$ROOT/python/autumn_fsspec" && \
  env AUTUMN_MANAGER="$MGR" python3 tests/chaos_workload.py --verify "$MANIFEST" \
  > "$WORK/verify.log" 2>&1 )
VEXIT=$?
tail -1 "$WORK/verify.log"
[ "$VEXIT" = "0" ] || { fail "final verify failed"; grep '^VERIFY-FAIL' "$WORK/verify.log" | head -5; }

# ── post-settle write-liveness probe ([[project_chaos_writeliveness_check]]) ─
say "write-liveness probe"
( cd "$ROOT/python/autumn_fsspec" && env AUTUMN_MANAGER="$MGR" python3 - <<'PY'
import sys, os
sys.path.insert(0, ".")
from autumn_fsspec import AutumnFileSystem
fs = AutumnFileSystem(manager=os.environ["AUTUMN_MANAGER"], root="fsspec_chaos", skip_instance_cache=True)
fs.pipe_file("liveness/post.bin", b"alive" * 1000)
assert fs.cat_file("liveness/post.bin") == b"alive" * 1000
print("WRITE-LIVENESS OK")
PY
) || fail "post-settle write-liveness probe failed"

ERRS=$(grep -c '^ERR ' "$WLOG" 2>/dev/null || true)
say "summary: manifested=$NFILES transient_errs=${ERRS:-0} (transient errs during fault windows are EXPECTED)"
if [ "$FAIL" = "0" ]; then
    say "=== FSSPEC CHAOS PASS ==="
else
    say "=== FSSPEC CHAOS FAIL ==="
fi
exit "$FAIL"
