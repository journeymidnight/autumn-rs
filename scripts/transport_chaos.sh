#!/usr/bin/env bash
# transport_chaos.sh — transport-layer chaos for tcp|ucx (F264 iteration 2).
#
# Boots a real 3-EN cluster via cluster.sh under the given transport, adds a
# SECOND PS (psid 2), then injects faults while a write loop runs:
#   E1: kill -9 one extent-node, respawn it with its exact original cmdline
#   E2: kill -9 PS 1 → partitions must MIGRATE to PS 2 (manager eviction)
#   E3: respawn PS 1 (same psid) → cluster must stay consistent
#
# Invariants checked (exit nonzero on any violation):
#   - every pre-seeded key (small / 8 KiB VP / 12 MiB put-stream) stays
#     byte-exact after every event
#   - every ACKed write from the background loop is present afterwards
#     (CLI exit 0 = acked; non-zero = uncertain, dropped from expectations)
#   - write liveness on both keyspace halves post-failover
#
# Usage:
#   AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/transport_chaos.sh tcp
#   AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/transport_chaos.sh ucx
# (ucx needs binaries built with --features autumn-server/ucx; the script
#  sets the loopback-safe positive UCX_TLS list itself.)
set -u

T="${1:-tcp}"
case "$T" in tcp|ucx) ;; *) echo "usage: $0 tcp|ucx"; exit 2;; esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
AC="$ROOT/target/release/autumn-client"
AO="$ROOT/target/release/autumn-op"
PSBIN="$ROOT/target/release/autumn-ps"
MGR="127.0.0.1:9001"
WORK="$(mktemp -d /tmp/transport_chaos.XXXXXX)"
FAIL=0

say()  { echo "[chaos-$T] $*"; }
fail() { echo "[chaos-$T] FAIL: $*"; FAIL=1; }

# ── boot ────────────────────────────────────────────────────────────────────
if [ "$T" = ucx ]; then
    # Loopback-safe POSITIVE transport list (never '^' negation — repo rule).
    export UCX_TLS="${UCX_TLS:-posix,cma,tcp,self}"
fi
export AUTUMN_DATA_ROOT="${AUTUMN_DATA_ROOT:-/data05/autumn-rs}"
say "cleaning + starting cluster (transport=$T, data=$AUTUMN_DATA_ROOT)"
for pid in $(ps -eo pid,comm | awk '$2 ~ /^(autumn-|etcd)/ {print $1}'); do kill -9 "$pid" 2>/dev/null; done
sleep 2
if [ "$T" = ucx ]; then
    # UCX listeners carry no SO_REUSEADDR: TIME_WAIT sockets from a
    # previous incarnation block rebinding for up to ~60s. Drain before
    # starting (the in-binary bind retry covers mid-run restarts; this
    # covers the cluster-boot path whose readiness probe would time out
    # first).
    say "ucx: draining TIME_WAIT on cluster ports"
    for i in $(seq 1 35); do
        tw=$(ss -tan state time-wait 2>/dev/null | grep -cE ':(9001|9301|9351|2000[0-9]) ') || true
        [ "${tw:-0}" = "0" ] && break
        sleep 2
    done
fi
rm -rf "$AUTUMN_DATA_ROOT" /tmp/autumn-rs
env AUTUMN_EXTENT_BASE_PORT=20000 \
    AUTUMN_BOOTSTRAP_PRESPLIT="4:hexstring" \
    AUTUMN_TRANSPORT="$T" \
    bash "$ROOT/cluster.sh" start 3 > "$WORK/cluster.log" 2>&1
if ! grep -q "bootstrap succeeded" "$WORK/cluster.log"; then
    echo "cluster start failed:"; tail -20 "$WORK/cluster.log"; exit 1
fi
sleep 5

# Second PS (psid 2) — the migration target. Distinct port band.
setsid nohup "$PSBIN" --psid 2 --port 9351 --manager "$MGR" \
    --listen 127.0.0.1 --advertise 127.0.0.1:9351 --transport "$T" \
    > "$WORK/ps2.log" 2>&1 < /dev/null &
sleep 3

CLI=("$AC" --manager "$MGR" --transport "$T")

# ── seed + manifest ─────────────────────────────────────────────────────────
say "seeding keys"
mkdir -p "$WORK/seed"
seed_keys=()
for i in $(seq 0 39); do
    if [ $((i % 2)) -eq 0 ]; then k="a-seed-$i"; else k="z-seed-$i"; fi
    if [ $((i % 10)) -eq 0 ]; then
        head -c 8192 /dev/urandom > "$WORK/seed/$k"      # VP-sized
    else
        echo "seed-value-$i" > "$WORK/seed/$k"
    fi
    "${CLI[@]}" put "$k" "$WORK/seed/$k" >/dev/null 2>&1 || { fail "seed put $k"; }
    seed_keys+=("$k")
done
head -c $((12*1024*1024)) /dev/urandom > "$WORK/seed/bigstripe"
"${CLI[@]}" put-stream bigstripe "$WORK/seed/bigstripe" >/dev/null 2>&1 || fail "seed put-stream"

verify_seeds() {
    local tag="$1" bad=0
    for k in "${seed_keys[@]}"; do
        if ! "${CLI[@]}" get "$k" 2>/dev/null | cmp -s - "$WORK/seed/$k"; then
            fail "[$tag] seed key $k mismatch/missing"; bad=1
        fi
    done
    "${CLI[@]}" get-stream --out "$WORK/big.out" bigstripe >/dev/null 2>&1
    cmp -s "$WORK/big.out" "$WORK/seed/bigstripe" || { fail "[$tag] bigstripe mismatch"; bad=1; }
    [ $bad -eq 0 ] && say "[$tag] seed verify OK (${#seed_keys[@]} keys + 12MiB stripe)"
}

# ── background ACKed-write loop ─────────────────────────────────────────────
say "starting write loop"
: > "$WORK/acked.txt"
(
    i=0
    while [ -f "$WORK/run" ] || [ ! -f "$WORK/stop" ]; do
        [ -f "$WORK/stop" ] && break
        if [ $((i % 2)) -eq 0 ]; then k="a-loop-$i"; else k="z-loop-$i"; fi
        if "${CLI[@]}" put "$k" <(echo "loop-$i") >/dev/null 2>&1; then
            echo "$k loop-$i" >> "$WORK/acked.txt"
        fi
        i=$((i+1))
    done
) &
LOOP_PID=$!

# ── E1: kill + respawn one EN ───────────────────────────────────────────────
sleep 4
EN_PID=$(ps -eo pid,comm | awk '$2=="autumn-extent-n"{print $1}' | head -1)
EN_CMD=$(tr '\0' ' ' < "/proc/$EN_PID/cmdline")
say "E1: kill -9 EN pid=$EN_PID"
kill -9 "$EN_PID"
sleep 8
say "E1: respawn EN: $EN_CMD"
setsid nohup $EN_CMD > "$WORK/en_respawn.log" 2>&1 < /dev/null &
sleep 6
verify_seeds "after-EN-kill-restart"

# ── E2: kill PS 1 → migration to PS 2 ──────────────────────────────────────
PS1_PID=$(pgrep -f -- "--psid 1 .*--transport $T" | head -1)
say "E2: kill -9 PS1 pid=$PS1_PID"
kill -9 "$PS1_PID"
deadline=$((SECONDS + 60))
while [ $SECONDS -lt $deadline ]; do
    left=$("$AO" --manager "$MGR" info 2>/dev/null | grep -c "ps=127.0.0.1:9301") || true
    if [ "${left:-1}" = "0" ]; then break; fi
    sleep 2
done
left=$("$AO" --manager "$MGR" info 2>/dev/null | grep -c "ps=127.0.0.1:9301") || true
if [ "${left:-1}" != "0" ]; then
    fail "E2: partitions did NOT migrate off PS1 within 60s"
    "$AO" --manager "$MGR" info 2>/dev/null | grep "^  part" | head -8
else
    say "E2: all partitions migrated to PS2"
fi
sleep 5
verify_seeds "after-PS1-kill"
# write liveness on both halves post-failover
for k in a-live-1 z-live-1; do
    ok=0
    for try in $(seq 1 30); do
        if "${CLI[@]}" put "$k" <(echo live) >/dev/null 2>&1; then ok=1; break; fi
        sleep 2
    done
    [ $ok -eq 1 ] || fail "E2: write liveness wedged on $k"
done
say "E2: write liveness OK"

# ── E3: respawn PS1 ─────────────────────────────────────────────────────────
say "E3: respawn PS1"
setsid nohup "$PSBIN" --psid 1 --port 9301 --manager "$MGR" \
    --listen 127.0.0.1 --advertise 127.0.0.1:9301 --transport "$T" \
    > "$WORK/ps1_respawn.log" 2>&1 < /dev/null &
sleep 8
verify_seeds "after-PS1-respawn"

# ── stop loop + verify every ACKed write ────────────────────────────────────
touch "$WORK/stop"
wait "$LOOP_PID" 2>/dev/null
total=$(wc -l < "$WORK/acked.txt")
say "write loop done: $total acked writes; verifying all"
bad=0
while read -r k v; do
    got=$("${CLI[@]}" get "$k" 2>/dev/null)
    if [ "$got" != "$v" ]; then
        fail "ACKED write lost/mismatch: $k (want '$v' got '$got')"
        bad=$((bad+1)); [ $bad -ge 5 ] && { fail "(more suppressed)"; break; }
    fi
done < "$WORK/acked.txt"
[ $bad -eq 0 ] && say "all $total ACKed writes intact"

# ── teardown ────────────────────────────────────────────────────────────────
for pid in $(ps -eo pid,comm | awk '$2 ~ /^(autumn-|etcd)/ {print $1}'); do kill -9 "$pid" 2>/dev/null; done
if [ $FAIL -eq 0 ]; then
    say "PASS (work dir $WORK kept for inspection)"
else
    say "FAILED — logs in $WORK"
fi
exit $FAIL
