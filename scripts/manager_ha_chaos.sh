#!/usr/bin/env bash
# manager_ha_chaos.sh — multi-manager HA chaos for tcp|ucx (F267).
#
# Boots a real cluster.sh cluster (manager1 @9001 + 3 EN), adds a STANDBY
# manager2 @9002 (same etcd), and replaces the PSes with ones that carry the
# full manager list "9001,9002". Then, while an ACKed-write loop runs:
#   H1: kill -9 manager1 (the leader) → manager2 must win the election
#       (etcd lease TTL ~10 s), PSes rotate to it, part_addrs self-heals on
#       the new leader (F265), clients keep reading/writing — zero loss
#   H2: kill -9 the partition-holding PS with ONLY manager2 alive → the
#       new leader's eviction/rebalance must migrate everything to the
#       survivor PS
#   H3: respawn manager1 → it must come back as a FOLLOWER (F149 fence —
#       no split-brain writes), cluster keeps serving
# Afterwards every ACKed write is verified byte-exact.
#
# Usage:
#   AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/manager_ha_chaos.sh tcp
#   AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/manager_ha_chaos.sh ucx
set -u

T="${1:-tcp}"
case "$T" in tcp|ucx) ;; *) echo "usage: $0 tcp|ucx"; exit 2;; esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
AC="$ROOT/target/release/autumn-client"
AO="$ROOT/target/release/autumn-op"
PSBIN="$ROOT/target/release/autumn-ps"
MGRBIN="$ROOT/target/release/autumn-manager-server"
MGRS="127.0.0.1:9001,127.0.0.1:9002"
WORK="$(mktemp -d /tmp/manager_ha_chaos.XXXXXX)"
FAIL=0

say()  { echo "[ha-$T $(date +%H:%M:%S)] $*"; }
fail() { echo "[ha-$T $(date +%H:%M:%S)] FAIL: $*"; FAIL=1; }

if [ "$T" = ucx ]; then
    export UCX_TLS="${UCX_TLS:-posix,cma,tcp,self}"
fi
export AUTUMN_DATA_ROOT="${AUTUMN_DATA_ROOT:-/data05/autumn-rs}"
say "cleaning + starting cluster (transport=$T)"
for pid in $(ps -eo pid,comm | awk '$2 ~ /^(autumn-|etcd)/ {print $1}'); do kill -9 "$pid" 2>/dev/null; done
sleep 2
say "draining cluster ports"
for i in $(seq 1 35); do
    busy=$(ss -tan 2>/dev/null | grep -cE ':(9001|9002|9301|9351|2000[0-9]) ') || true
    [ "${busy:-0}" = "0" ] && break
    sleep 2
done
rm -rf "$AUTUMN_DATA_ROOT" /tmp/autumn-rs
env AUTUMN_EXTENT_BASE_PORT=20000 \
    AUTUMN_BOOTSTRAP_PRESPLIT="4:hexstring" \
    AUTUMN_TRANSPORT="$T" \
    bash "$ROOT/cluster.sh" start 3 > "$WORK/cluster.log" 2>&1
if ! grep -q "bootstrap succeeded" "$WORK/cluster.log"; then
    echo "cluster start failed:"; tail -20 "$WORK/cluster.log"; exit 1
fi
sleep 3

# Standby manager2 @9002 (same etcd) — must stay follower while mgr1 leads.
say "starting standby manager2 @9002"
setsid nohup "$MGRBIN" --port 9002 --etcd 127.0.0.1:2379 \
    --listen 127.0.0.1 --transport "$T" > "$WORK/mgr2.log" 2>&1 < /dev/null &
sleep 3

# Replace cluster.sh's PS1 (single manager addr) with our own PS1+PS2 that
# carry the FULL manager list — required for rotate_manager to have a
# second address to fail over to.
PS1_PID=$(pgrep -f -- "--psid 1 " | head -1)
[ -n "$PS1_PID" ] && kill -9 "$PS1_PID"
sleep 2
for ps in "1 9301" "2 9351"; do
    set -- $ps
    setsid nohup "$PSBIN" --psid "$1" --port "$2" --manager "$MGRS" \
        --listen 127.0.0.1 --advertise "127.0.0.1:$2" --transport "$T" \
        > "$WORK/ps$1.log" 2>&1 < /dev/null &
done
sleep 8

CLI=(timeout 20 "$AC" --manager "$MGRS" --transport "$T")
CLIS=(timeout 90 "$AC" --manager "$MGRS" --transport "$T")
ao_info() { # ao_info <mgr_addr> — bounded info probe against one manager
    timeout 20 "$AO" --manager "$1" --transport "$T" info 2>/dev/null
}

# ── seed ────────────────────────────────────────────────────────────────────
say "seeding keys"
mkdir -p "$WORK/seed"
seed_keys=()
for i in $(seq 0 39); do
    if [ $((i % 2)) -eq 0 ]; then k="a-seed-$i"; else k="z-seed-$i"; fi
    if [ $((i % 10)) -eq 0 ]; then head -c 8192 /dev/urandom > "$WORK/seed/$k"
    else echo "seed-value-$i" > "$WORK/seed/$k"; fi
    "${CLI[@]}" put "$k" "$WORK/seed/$k" >/dev/null 2>&1 || fail "seed put $k"
    seed_keys+=("$k")
done
head -c $((12*1024*1024)) /dev/urandom > "$WORK/seed/bigstripe"
"${CLIS[@]}" put-stream bigstripe "$WORK/seed/bigstripe" >/dev/null 2>&1 || fail "seed put-stream"

verify_seeds() {
    local tag="$1" bad=0
    for k in "${seed_keys[@]}"; do
        if ! "${CLI[@]}" get "$k" 2>/dev/null | cmp -s - "$WORK/seed/$k"; then
            fail "[$tag] seed key $k mismatch/missing"; bad=1
        fi
    done
    rm -f "$WORK/big.out"
    if ! "${CLIS[@]}" get-stream --out "$WORK/big.out" bigstripe >/dev/null 2>&1; then
        fail "[$tag] bigstripe get-stream failed/timeout"; bad=1
    elif ! cmp -s "$WORK/big.out" "$WORK/seed/bigstripe"; then
        fail "[$tag] bigstripe mismatch"; bad=1
    fi
    [ $bad -eq 0 ] && say "[$tag] seed verify OK (${#seed_keys[@]} keys + 12MiB stripe)"
}

write_liveness() {
    local tag="$1" k ok try
    for k in "a-live-$tag" "z-live-$tag"; do
        ok=0
        for try in $(seq 1 45); do
            if "${CLI[@]}" put "$k" <(echo live) >/dev/null 2>&1; then ok=1; break; fi
            sleep 2
        done
        [ $ok -eq 1 ] || fail "[$tag] write liveness wedged on $k"
    done
    [ $FAIL -eq 0 ] && say "[$tag] write liveness OK"
}

parts_on() { # parts_on <mgr> <ps_port>
    local out
    out=$(ao_info "$1")
    [ -n "$out" ] || return 1
    printf '%s\n' "$out" | grep -c "ps=127.0.0.1:$2" || true
}

# ── background ACKed-write loop ─────────────────────────────────────────────
say "starting write loop"
: > "$WORK/acked.txt"
(
    i=0
    while [ ! -f "$WORK/stop" ]; do
        if [ $((i % 2)) -eq 0 ]; then k="a-loop-$i"; else k="z-loop-$i"; fi
        if "${CLI[@]}" put "$k" <(echo "loop-$i") >/dev/null 2>&1; then
            echo "$k loop-$i" >> "$WORK/acked.txt"
        fi
        i=$((i+1))
    done
) &
LOOP_PID=$!
sleep 4
verify_seeds "baseline"

# ── H1: kill the LEADER (manager1) → standby must take over ────────────────
MGR1_PID=$(pgrep -f -- "--port 9001 .*--transport $T" | head -1)
[ -n "$MGR1_PID" ] || MGR1_PID=$(pgrep -f "autumn-manager-server --port 9001" | head -1)
MGR1_CMD=$(tr '\0' ' ' < "/proc/$MGR1_PID/cmdline")
say "H1: kill -9 leader manager1 pid=$MGR1_PID"
kill -9 "$MGR1_PID"
# manager2 must take over: leader lease TTL ~10s + election retry ~2s.
# Takeover signal = an END-TO-END write routed via manager2 ONLY: needs
# its routing answered, its part_addrs view healed (F265 self-heal must
# follow the PS manager rotation — F267), and a PS serving. An info-only
# probe is vacuous: get_regions is ungated and a follower answers from
# replay-at-startup state.
deadline=$((SECONDS + 120))
ok=0
while [ $SECONDS -lt $deadline ]; do
    if timeout 20 "$AC" --manager 127.0.0.1:9002 --transport "$T" \
        put ha-takeover-probe <(echo x) >/dev/null 2>&1; then ok=1; break; fi
    sleep 2
done
[ $ok -eq 1 ] && say "H1: manager2 took over (end-to-end write via 9002 OK)" \
              || fail "H1: manager2 did not take over within 120s"
sleep 5
verify_seeds "after-leader-kill"
write_liveness "h1"

# ── H2: kill the holder PS with only manager2 alive ────────────────────────
p1=$(parts_on 127.0.0.1:9002 9301) || p1=0
p2=$(parts_on 127.0.0.1:9002 9351) || p2=0
if [ "${p1:-0}" -ge "${p2:-0}" ]; then vid=1; vport=9301; sport=9351; else vid=2; vport=9351; sport=9301; fi
VPID=$(pgrep -f -- "--psid $vid .*--transport $T" | head -1)
say "H2: holder=PS$vid ($p1/$p2 on 9301/9351); kill -9 pid=${VPID:-?}"
[ -n "$VPID" ] && kill -9 "$VPID"
deadline=$((SECONDS + 90))
while [ $SECONDS -lt $deadline ]; do
    left=$(parts_on 127.0.0.1:9002 "$vport")
    [ "${left:-1}" = "0" ] && break
    sleep 2
done
left=$(parts_on 127.0.0.1:9002 "$vport")
[ "${left:-1}" = "0" ] && say "H2: partitions migrated to PS-survivor (:$sport)" \
                       || fail "H2: partitions did not migrate off PS$vid under manager2"
sleep 3
verify_seeds "after-H2-ps-kill"
write_liveness "h2"
say "H2: respawn PS$vid"
setsid nohup "$PSBIN" --psid "$vid" --port "$vport" --manager "$MGRS" \
    --listen 127.0.0.1 --advertise "127.0.0.1:$vport" --transport "$T" \
    > "$WORK/ps${vid}_respawn.log" 2>&1 < /dev/null &
sleep 6

# ── H3: respawn manager1 → must rejoin as FOLLOWER, no split-brain ──────────
say "H3: respawn manager1"
setsid nohup $MGR1_CMD > "$WORK/mgr1_respawn.log" 2>&1 < /dev/null &
sleep 15
# manager2 must STILL be the leader: its info keeps showing partitions and
# the cluster keeps serving. (A split-brain write from manager1 would trip
# the F149 fence; data-plane verifies below catch any divergence.)
out=$(ao_info 127.0.0.1:9002)
if [ -n "$out" ] && printf '%s' "$out" | grep -q "^  part"; then
    say "H3: manager2 still serving after manager1 rejoin"
else
    fail "H3: manager2 stopped serving after manager1 rejoin"
fi
verify_seeds "after-mgr1-rejoin"
write_liveness "h3"

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
