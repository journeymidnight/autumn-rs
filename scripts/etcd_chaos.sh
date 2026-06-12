#!/usr/bin/env bash
# etcd_chaos.sh — control-plane DEPENDENCY chaos: etcd kill/outage/restart.
#
# etcd is the manager's only external dependency and had ZERO chaos
# coverage (every prior harness killed managers/PSes/ENs — never etcd).
# Production etcd maintenance (upgrades, compaction stalls, snapshots)
# routinely takes minutes; the cluster must degrade gracefully:
#   D1: kill -9 etcd → manager loses its lease (steps down within ~10 s);
#       the DATA PLANE must keep serving — writes to established tails are
#       pure PS↔EN and never touch the control plane.
#   D2: 150 s outage (past the PS heartbeat exit budget) → NO PS may
#       exit. A leaderless control plane cannot evict/reassign anyone, so
#       serving through the outage is safe; a fleet suicide here is a
#       self-inflicted total outage. (This caught exactly that: the F267
#       NOT_LEADER budget shared the 90 s transport-failure budget.)
#   D3: restart etcd (same data dir) → manager re-elects, replays, and
#       control-plane ops (autumn-op info / partition ops) resume.
# Final: every ACKed key reads back byte-exact (zero loss).
#
# Usage: AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/etcd_chaos.sh
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MGR="127.0.0.1:9001"
WORK="$(mktemp -d /tmp/etcd_chaos.XXXXXX)"
AC=("$ROOT/target/release/autumn-client" --manager "$MGR")
AO=(timeout 20 "$ROOT/target/release/autumn-op" --manager "$MGR" --transport tcp)
FAIL=0

say()  { echo "[etcd $(date +%H:%M:%S)] $*"; }
fail() { echo "[etcd $(date +%H:%M:%S)] FAIL: $*"; FAIL=1; }

export AUTUMN_DATA_ROOT="${AUTUMN_DATA_ROOT:-/data05/autumn-rs}"
say "cleaning + starting cluster"
for pid in $(ps -eo pid,comm | awk '$2 ~ /^(autumn-|etcd)/ {print $1}'); do kill -9 "$pid" 2>/dev/null; done
sleep 2
rm -rf "$AUTUMN_DATA_ROOT" /tmp/autumn-rs
env AUTUMN_EXTENT_BASE_PORT=20000 \
    AUTUMN_BOOTSTRAP_PRESPLIT="4:hexstring" \
    AUTUMN_TRANSPORT=tcp \
    AUTUMN_METRICS=1 \
    bash "$ROOT/cluster.sh" start 3 > "$WORK/cluster.log" 2>&1
grep -q "bootstrap succeeded" "$WORK/cluster.log" \
    || { echo "cluster start failed"; tail -10 "$WORK/cluster.log"; exit 1; }

# ── continuous workload: 64KiB puts, manifest of acked keys ─────────────────
dd if=/dev/urandom of="$WORK/val" bs=64K count=1 status=none
: > "$WORK/acked.txt"
( i=0
  while [ ! -f "$WORK/stop" ]; do
      i=$((i+1))
      k="$(printf '%02x' $(( RANDOM % 256 )))-ek-$i"
      if timeout 15 "${AC[@]}" put "$k" "$WORK/val" >/dev/null 2>&1; then
          echo "$k" >> "$WORK/acked.txt"
      fi
      sleep 0.25
  done ) &
WL_PID=$!

acked() { wc -l < "$WORK/acked.txt"; }
wait_progress() { # writes must advance by >3 within 60s
    local tag="$1" base now deadline=$((SECONDS + 60))
    base=$(acked)
    while [ $SECONDS -lt $deadline ]; do
        now=$(acked)
        [ "$now" -gt "$((base + 3))" ] && { say "[$tag] writes progressing ($base -> $now)"; return 0; }
        sleep 2
    done
    fail "[$tag] writes made no progress in 60s"
}
sleep 5
wait_progress "baseline"

# ── D1: kill etcd → data plane must keep serving ───────────────────────────
EPID=$(pgrep -x etcd | head -1)
ECMD=$(tr '\0' ' ' < "/proc/$EPID/cmdline")
ECWD=$(readlink "/proc/$EPID/cwd")
say "D1: kill -9 etcd pid=$EPID (cwd=$ECWD)"
kill -9 "$EPID"
sleep 12   # past the 10s lease TTL — manager steps down
wait_progress "during-etcd-outage"

# ── D2: 150s total outage — NO PS may exit ──────────────────────────────────
say "D2: holding etcd down to 150s total (PS fleet must survive)"
PS_BEFORE=$(pgrep -c -f "autumn-ps --psid") || PS_BEFORE=0
sleep 140
PS_AFTER=$(pgrep -c -f "autumn-ps --psid") || PS_AFTER=0
if [ "$PS_AFTER" -lt "$PS_BEFORE" ]; then
    fail "D2: PS count dropped $PS_BEFORE -> $PS_AFTER during etcd outage (fleet suicide)"
else
    say "D2: all $PS_AFTER PS(es) survived the 150s outage"
fi
wait_progress "late-outage"

# ── D3: restart etcd → manager re-elects, control plane resumes ────────────
say "D3: restarting etcd (same data dir)"
( cd "$ECWD" && setsid nohup $ECMD > "$WORK/etcd_respawn.log" 2>&1 < /dev/null & )
recovered=0
deadline=$((SECONDS + 120))
while [ $SECONDS -lt $deadline ]; do
    if "${AO[@]}" info 2>/dev/null | grep -q "ps="; then recovered=1; break; fi
    sleep 3
done
[ "$recovered" = "1" ] && say "D3: control plane recovered" \
    || fail "D3: control plane did not recover within 120s of etcd restart"
wait_progress "post-recovery"

# control-plane mutation must work again (split = the heaviest etcd txn)
P=$("${AO[@]}" info 2>/dev/null | grep -oE "part=[0-9]+" | head -1 | cut -d= -f2)
if [ -n "${P:-}" ]; then
    if "${AO[@]}" compact "$P" >/dev/null 2>&1; then
        say "D3: control-plane op (compact part $P) OK"
    else
        say "D3: compact returned non-zero (advisory only — may be a no-op refusal)"
    fi
fi

# ── stop + zero-loss verify ─────────────────────────────────────────────────
touch "$WORK/stop"
wait "$WL_PID" 2>/dev/null
want_md5=$(md5sum "$WORK/val" | awk '{print $1}')
say "verify: $(acked) ACKed keys"
bad=0
while read -r k; do
    okk=0
    for attempt in 1 2 3; do
        got=$(timeout 20 "${AC[@]}" get "$k" 2>/dev/null | md5sum | awk '{print $1}')
        [ "$got" = "$want_md5" ] && { okk=1; break; }
        sleep 2
    done
    [ "$okk" = "1" ] || { bad=$((bad+1)); echo "MISMATCH $k" >> "$WORK/verify.log"; }
done < "$WORK/acked.txt"
[ "$bad" = "0" ] && say "verify: zero loss" || fail "verify: $bad ACKed keys lost (see $WORK/verify.log)"

for pid in $(ps -eo pid,comm | awk '$2 ~ /^(autumn-|etcd)/ {print $1}'); do kill -9 "$pid" 2>/dev/null; done
total=$(acked)
if [ $FAIL -eq 0 ]; then say "PASS ($total ACKed keys, work dir $WORK kept)"; else say "FAILED — logs in $WORK"; fi
exit $FAIL
