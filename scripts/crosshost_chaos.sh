#!/usr/bin/env bash
# crosshost_chaos.sh — CROSS-HOST chaos for tcp|ucx (F272).
#
# Topology (real network, two hosts):
#   LOCAL  (fdbd:dc62:3:302::14): etcd + manager:9001 + EN x2 (21001/21002)
#                                  + PS1:9301
#   REMOTE (fdbd:dc62:3:302::15): EN x1 (21003) + PS2:9351   (via ssh)
# Replication=3 over 3 ENs → EVERY extent spans the wire.
#
# Events, while an ACKed-write loop runs locally:
#   X1: kill -9 the REMOTE EN (ssh) + respawn → cross-network replica
#       recovery; seeds stay byte-exact
#   X2: kill -9 LOCAL PS1 → partitions must MIGRATE ACROSS HOSTS to the
#       remote PS2; write liveness on both keyspace halves
#   X3: kill -9 REMOTE PS2 (ssh) → cross-host FAILBACK to local PS1
#   X4: kill -9 the manager + exact-cmdline respawn with remote
#       components live → routing self-heals (F265/F267 over real RTT)
# Afterwards every ACKed write is verified byte-exact.
#
# Usage:
#   ./scripts/crosshost_chaos.sh tcp
#   ./scripts/crosshost_chaos.sh ucx   # pins UCX_NET_DEVICES=mlx5_1:1 both ends
set -u

T="${1:-tcp}"
case "$T" in tcp|ucx) ;; *) echo "usage: $0 tcp|ucx"; exit 2;; esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RA="$ROOT/.claude/skills/remote-autumn/remote-autumn.sh"
LIP="fdbd:dc62:3:302::14"
RIP="fdbd:dc62:3:302::15"
MGR="[$LIP]:9001"
RROOT="/data/dongmao_dev/autumn-rs"
LDATA="/data05/autumn-rs-xh"
RDATA="/data03/autumn-rs-xh"
AC="$ROOT/target/release/autumn-client"
AO="$ROOT/target/release/autumn-op"
WORK="$(mktemp -d /tmp/crosshost_chaos.XXXXXX)"
FAIL=0

say()  { echo "[xh-$T $(date +%H:%M:%S)] $*"; }
fail() { echo "[xh-$T $(date +%H:%M:%S)] FAIL: $*"; FAIL=1; }
REM()  { "$RA" "$@"; }

if [ "$T" = ucx ]; then
    # Cross-host UCX REQUIRES the RoCE device pinned on BOTH ends
    # (10 devices → auto-select hangs) + positive TLS list.
    export UCX_NET_DEVICES="mlx5_1:1"
    export UCX_TLS="rc_mlx5,ud_mlx5,tcp,self"
    UCX_REMOTE_ENV="UCX_NET_DEVICES=mlx5_1:1 UCX_TLS=rc_mlx5,ud_mlx5,tcp,self"
else
    UCX_REMOTE_ENV=""
fi

# ── cleanup both hosts ──────────────────────────────────────────────────────
say "cleaning both hosts"
for pid in $(ps -eo pid,comm | awk '$2 ~ /^(autumn-|etcd)/ {print $1}'); do kill -9 "$pid" 2>/dev/null; done
REM "for pid in \$(ps -eo pid,stat,comm | awk '\$3 ~ /^autumn-/ && \$2 !~ /Z/ {print \$1}'); do kill -9 \$pid 2>/dev/null; done; rm -rf $RDATA; mkdir -p $RDATA/d1; echo remote-clean"
sleep 2
rm -rf "$LDATA" /tmp/autumn-rs; mkdir -p "$LDATA/d1" "$LDATA/d2"

# ── etcd + manager (local) ──────────────────────────────────────────────────
say "starting etcd + manager on $LIP"
setsid nohup etcd --data-dir "$LDATA/etcd" \
    --listen-client-urls http://127.0.0.1:2379 \
    --advertise-client-urls http://127.0.0.1:2379 \
    > "$WORK/etcd.log" 2>&1 < /dev/null &
for i in $(seq 1 30); do curl -s http://127.0.0.1:2379/health >/dev/null 2>&1 && break; sleep 1; done
setsid nohup "$ROOT/target/release/autumn-manager-server" \
    --port 9001 --etcd 127.0.0.1:2379 --listen "$LIP" --transport "$T" \
    > "$WORK/manager.log" 2>&1 < /dev/null &
sleep 3

AOC=(timeout 20 "$AO" --manager "$MGR" --transport "$T")
CLI=(timeout 20 "$AC" --manager "$MGR" --transport "$T")
CLIS=(timeout 90 "$AC" --manager "$MGR" --transport "$T")

# Readiness gates — NEVER rely on fixed sleeps: under ucx the manager's
# listener can spend ~90s in the F264 TIME_WAIT bind retry (cross-host
# TIME_WAIT from a previous round on the same [::14]:9001). The first
# ucx round was stillborn end-to-end because every stage marched past a
# not-yet-listening manager (format/bootstrap/seeds all failed, ENs and
# PS1 fail-fasted on registration).
wait_mgr() {
    local tag="$1" deadline=$((SECONDS + 180))
    while [ $SECONDS -lt $deadline ]; do
        if [ -n "$("${AOC[@]}" info 2>/dev/null)" ]; then say "[$tag] manager ready"; return 0; fi
        sleep 2
    done
    fail "[$tag] manager not ready within 180s"; return 1
}
wait_nodes() { # wait_nodes <count>
    local want="$1" deadline=$((SECONDS + 120))
    while [ $SECONDS -lt $deadline ]; do
        n=$("${AOC[@]}" info 2>/dev/null | grep -c "^  node ") || n=0
        [ "${n:-0}" -ge "$want" ] && { say "$n nodes registered"; return 0; }
        sleep 2
    done
    fail "only ${n:-0}/$want nodes registered within 120s"; return 1
}

# ── format + start ENs: 2 local + 1 remote ──────────────────────────────────
wait_mgr "boot"
say "formatting + starting ENs (2 local + 1 remote)"
"${AOC[@]}" format --listen "[$LIP]:21001" --advertise "[$LIP]:21001" "$LDATA/d1" >/dev/null 2>&1 || fail "format local d1"
"${AOC[@]}" format --listen "[$LIP]:21002" --advertise "[$LIP]:21002" "$LDATA/d2" >/dev/null 2>&1 || fail "format local d2"
REM "cd $RROOT && timeout 20 ./target/release/autumn-op --manager '$MGR' --transport $T format --listen '[$RIP]:21003' --advertise '[$RIP]:21003' $RDATA/d1 >/dev/null 2>&1 && echo fmt-ok || echo fmt-FAIL" | grep -q fmt-ok || fail "format remote d1"

start_local_en() { # port datadir logname
    setsid nohup "$ROOT/target/release/autumn-extent-node" \
        --port "$1" --data "$2" --manager "$MGR" --listen "$LIP" \
        --transport "$T" --cpuset 0-0 > "$WORK/$3" 2>&1 < /dev/null &
}
start_remote_en() {
    # ssh can linger after spawning a remote daemon (sshd holds the
    # channel) — background the WHOLE ssh locally and just wait a beat.
    REM "cd $RROOT && env $UCX_REMOTE_ENV setsid nohup ./target/release/autumn-extent-node --port 21003 --data $RDATA/d1 --manager '$MGR' --listen $RIP --transport $T --cpuset 0-0 > /tmp/xh_en_remote.log 2>&1 < /dev/null &" >> "$WORK/remote_cmds.log" 2>&1 &
    sleep 3
}
start_local_en 21001 "$LDATA/d1" en1.log
start_local_en 21002 "$LDATA/d2" en2.log
start_remote_en
sleep 5

# ── bootstrap (replication 3 over 3 ENs) + PSes ─────────────────────────────
wait_nodes 3
say "bootstrap + starting PS1 (local) / PS2 (remote)"
"${AOC[@]}" bootstrap --replication 3+0 --presplit 4:hexstring >/dev/null 2>&1 || fail "bootstrap"
setsid nohup "$ROOT/target/release/autumn-ps" --psid 1 --port 9301 --manager "$MGR" \
    --listen "$LIP" --advertise "[$LIP]:9301" --transport "$T" \
    > "$WORK/ps1.log" 2>&1 < /dev/null &
REM "cd $RROOT && env $UCX_REMOTE_ENV setsid nohup ./target/release/autumn-ps --psid 2 --port 9351 --manager '$MGR' --listen $RIP --advertise '[$RIP]:9351' --transport $T > /tmp/xh_ps2.log 2>&1 < /dev/null &" >> "$WORK/remote_cmds.log" 2>&1 &
sleep 8

parts_on() { # parts_on <hostport-grep>
    local out
    out=$("${AOC[@]}" info 2>/dev/null)
    [ -n "$out" ] || return 1
    printf '%s\n' "$out" | grep -c "ps=.*$1" || true
}
wait_mgr_ready() {
    local tag="$1" deadline=$((SECONDS + 150))
    while [ $SECONDS -lt $deadline ]; do
        if [ -n "$("${AOC[@]}" info 2>/dev/null)" ]; then say "[$tag] manager ready"; return 0; fi
        sleep 2
    done
    fail "[$tag] manager did not become ready within 150s"
}

# ── seed + verify helpers + write loop ──────────────────────────────────────
say "seeding keys"
mkdir -p "$WORK/seed"; seed_keys=()
for i in $(seq 0 39); do
    if [ $((i % 2)) -eq 0 ]; then k="a-seed-$i"; else k="z-seed-$i"; fi
    if [ $((i % 10)) -eq 0 ]; then head -c 8192 /dev/urandom > "$WORK/seed/$k"; else echo "seed-value-$i" > "$WORK/seed/$k"; fi
    "${CLI[@]}" put "$k" "$WORK/seed/$k" >/dev/null 2>&1 || fail "seed put $k"
    seed_keys+=("$k")
done
head -c $((12*1024*1024)) /dev/urandom > "$WORK/seed/bigstripe"
"${CLIS[@]}" put-stream bigstripe "$WORK/seed/bigstripe" >/dev/null 2>&1 || fail "seed put-stream"

verify_seeds() {
    local tag="$1" bad=0
    for k in "${seed_keys[@]}"; do
        if ! "${CLI[@]}" get "$k" 2>/dev/null | cmp -s - "$WORK/seed/$k"; then
            sleep 5
            if ! "${CLI[@]}" get "$k" 2>/dev/null | cmp -s - "$WORK/seed/$k"; then
                fail "[$tag] seed key $k mismatch/missing (after retry)"; bad=1
            fi
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

say "starting write loop"
: > "$WORK/acked.txt"
( i=0
  while [ ! -f "$WORK/stop" ]; do
      if [ $((i % 2)) -eq 0 ]; then k="a-loop-$i"; else k="z-loop-$i"; fi
      if "${CLI[@]}" put "$k" <(echo "loop-$i") >/dev/null 2>&1; then echo "$k loop-$i" >> "$WORK/acked.txt"; fi
      i=$((i+1))
  done ) &
LOOP_PID=$!
sleep 4
verify_seeds "baseline"

# ── X1: kill remote EN + respawn (cross-network replica) ────────────────────
say "X1: kill -9 REMOTE EN"
REM "pkill -9 -f 'autumn-extent-node --port 21003'; echo killed"
sleep 8
say "X1: respawn remote EN"
start_remote_en
sleep 6
verify_seeds "after-remote-EN-kill"

# ── X2: kill local PS1 → CROSS-HOST migration to remote PS2 ────────────────
PS1_PID=$(pgrep -f -- "--psid 1 .*--transport $T" | head -1)
say "X2: kill -9 local PS1 pid=$PS1_PID → partitions must migrate to REMOTE PS2"
[ -n "$PS1_PID" ] && kill -9 "$PS1_PID"
deadline=$((SECONDS + 90))
while [ $SECONDS -lt $deadline ]; do
    left=$(parts_on "::14]:93")
    [ "${left:-1}" = "0" ] && break
    sleep 2
done
left=$(parts_on "::14]:93")
[ "${left:-1}" = "0" ] && say "X2: all partitions migrated CROSS-HOST to PS2" \
                       || fail "X2: partitions did not migrate off local PS1"
sleep 3
verify_seeds "after-crosshost-migration"
write_liveness "x2"

# ── X3: kill remote PS2 → cross-host FAILBACK to local PS1 ─────────────────
say "X3: respawn local PS1, then kill REMOTE PS2 → failback"
setsid nohup "$ROOT/target/release/autumn-ps" --psid 1 --port 9301 --manager "$MGR" \
    --listen "$LIP" --advertise "[$LIP]:9301" --transport "$T" \
    > "$WORK/ps1_respawn.log" 2>&1 < /dev/null &
sleep 6
REM "pkill -9 -f 'autumn-ps --psid 2'; echo killed"
deadline=$((SECONDS + 90))
while [ $SECONDS -lt $deadline ]; do
    left=$(parts_on "::15]:93")
    [ "${left:-1}" = "0" ] && break
    sleep 2
done
left=$(parts_on "::15]:93")
[ "${left:-1}" = "0" ] && say "X3: partitions failed back CROSS-HOST to local PS1" \
                       || fail "X3: partitions did not fail back off remote PS2"
sleep 3
verify_seeds "after-crosshost-failback"
write_liveness "x3"

# ── X4: manager kill + respawn with remote components live ─────────────────
MGR_PID=$(pgrep -f autumn-manager-server | head -1)
MGR_CMD=$(tr '\0' ' ' < "/proc/$MGR_PID/cmdline")
say "X4: kill -9 manager pid=$MGR_PID"
kill -9 "$MGR_PID"
sleep 6
say "X4: respawn manager"
setsid nohup $MGR_CMD > "$WORK/mgr_respawn.log" 2>&1 < /dev/null &
wait_mgr "X4"
verify_seeds "after-mgr-kill-restart"
write_liveness "x4"

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

# ── teardown both hosts ─────────────────────────────────────────────────────
for pid in $(ps -eo pid,comm | awk '$2 ~ /^(autumn-|etcd)/ {print $1}'); do kill -9 "$pid" 2>/dev/null; done
REM "for pid in \$(ps -eo pid,stat,comm | awk '\$3 ~ /^autumn-/ && \$2 !~ /Z/ {print \$1}'); do kill -9 \$pid 2>/dev/null; done; echo remote-teardown"
if [ $FAIL -eq 0 ]; then say "PASS (work dir $WORK kept)"; else say "FAILED — logs in $WORK (+ remote /tmp/xh_*.log)"; fi
exit $FAIL
