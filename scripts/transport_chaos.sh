#!/usr/bin/env bash
# transport_chaos.sh — transport-layer chaos for tcp|ucx (F264 iteration 2).
#
# Boots a real 3-EN cluster via cluster.sh under the given transport, adds a
# SECOND PS (psid 2), then injects faults while a write loop runs:
#   E1: kill -9 one extent-node, respawn it with its exact original cmdline
#   E2: kill -9 PS 1 → partitions must MIGRATE to PS 2 (manager eviction)
#   E3: respawn PS 1 (same psid) → cluster must stay consistent
#   E4: kill -9 the MANAGER mid-writes, respawn with exact cmdline →
#       control plane must come back (ucx: TIME_WAIT rebind path) and
#       writes must resume on both keyspace halves
#   E5: kill -9 the partition-holding PS, then 3s later (inside the
#       eviction window) kill -9 the manager too → after the manager
#       respawns, the interrupted eviction/rebalance must still converge:
#       all partitions migrate to the survivor PS with zero data loss
#   E6: CHAOS_ROUNDS (default 4) randomized rounds (seed CHAOS_SEED,
#       default $$): each kills a random victim (EN / holder PS /
#       manager), converges, respawns, verifies — stresses CUMULATIVE
#       state (repeated owner-epoch failback, part_addrs churn, port
#       ordinal growth) that one-shot events can't reach
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

say()  { echo "[chaos-$T $(date +%H:%M:%S)] $*"; }
fail() { echo "[chaos-$T $(date +%H:%M:%S)] FAIL: $*"; FAIL=1; }

# ── boot ────────────────────────────────────────────────────────────────────
if [ "$T" = ucx ]; then
    # Loopback-safe POSITIVE transport list (never '^' negation — repo rule).
    export UCX_TLS="${UCX_TLS:-posix,cma,tcp,self}"
fi
export AUTUMN_DATA_ROOT="${AUTUMN_DATA_ROOT:-/data05/autumn-rs}"
say "cleaning + starting cluster (transport=$T, data=$AUTUMN_DATA_ROOT)"
for pid in $(ps -eo pid,comm | awk '$2 ~ /^(autumn-|etcd)/ {print $1}'); do kill -9 "$pid" 2>/dev/null; done
sleep 2
# Drain cluster ports for BOTH transports before boot. ucx: its listener
# carries no SO_REUSEADDR, so predecessor TIME_WAIT blocks rebinding up
# to ~60s (the in-binary bind retry covers mid-run restarts; this covers
# cluster boot, whose readiness probe can't wait that long). tcp: a just
# kill -9'd ucx manager's kernel-side socket/RDMA teardown can lag a few
# seconds, leaving 9001 actively held — a back-to-back tcp boot then
# fails EADDRINUSE despite REUSEADDR (observed running ucx->tcp rounds
# 8s apart). Wait until the ports carry NO socket in any state.
say "draining cluster ports"
for i in $(seq 1 35); do
    busy=$(ss -tan 2>/dev/null | grep -cE ':(9001|9301|9351|2000[0-9]) ') || true
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
sleep 5

# Second PS (psid 2) — the migration target. Distinct port band.
setsid nohup "$PSBIN" --psid 2 --port 9351 --manager "$MGR" \
    --listen 127.0.0.1 --advertise 127.0.0.1:9351 --transport "$T" \
    > "$WORK/ps2.log" 2>&1 < /dev/null &
sleep 3

# Every CLI call is timeout-bounded: an un-answered TCP connection
# otherwise hangs ~28 min in kernel retransmit (observed in the F265
# round), freezing the harness and hiding the event timeline.
CLI=(timeout 20 "$AC" --manager "$MGR" --transport "$T")
CLIS=(timeout 90 "$AC" --manager "$MGR" --transport "$T") # stream ops (12 MiB)
# --transport is REQUIRED: autumn-op defaults to tcp and a tcp autumn-op
# cannot reach a ucx manager — every ucx-round probe returned empty
# output before this, making E2's old migration check pass vacuously.
AOC=(timeout 20 "$AO" --manager "$MGR" --transport "$T")  # control-plane probes

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
"${CLIS[@]}" put-stream bigstripe "$WORK/seed/bigstripe" >/dev/null 2>&1 || fail "seed put-stream"

verify_seeds() {
    local tag="$1" bad=0
    for k in "${seed_keys[@]}"; do
        if ! "${CLI[@]}" get "$k" 2>/dev/null | cmp -s - "$WORK/seed/$k"; then
            # One retry after 5s: a verify right after a kill can race the
            # survivor's partition opens (observed: a single seed read
            # failed mid-convergence; the very next round read all 40
            # fine). Real loss fails both attempts.
            sleep 5
            if ! "${CLI[@]}" get "$k" 2>/dev/null | cmp -s - "$WORK/seed/$k"; then
                fail "[$tag] seed key $k mismatch/missing (after retry)"; bad=1
            fi
        fi
    done
    # rm first: a leftover big.out from the previous verify would make a
    # failed/timed-out get-stream look like a pass (coco P2).
    rm -f "$WORK/big.out"
    if ! "${CLIS[@]}" get-stream --out "$WORK/big.out" bigstripe >/dev/null 2>&1; then
        fail "[$tag] bigstripe get-stream failed/timeout"; bad=1
    elif ! cmp -s "$WORK/big.out" "$WORK/seed/bigstripe"; then
        fail "[$tag] bigstripe mismatch"; bad=1
    fi
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
    left=$("${AOC[@]}" info 2>/dev/null | grep -c "ps=127.0.0.1:9301") || true
    if [ "${left:-1}" = "0" ]; then break; fi
    sleep 2
done
left=$("${AOC[@]}" info 2>/dev/null | grep -c "ps=127.0.0.1:9301") || true
if [ "${left:-1}" != "0" ]; then
    fail "E2: partitions did NOT migrate off PS1 within 60s"
    "${AOC[@]}" info 2>/dev/null | grep "^  part" | head -8
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

# ── helpers for control-plane chaos ─────────────────────────────────────────
wait_mgr_ready() {
    # 150s: the respawned manager must wait out the predecessor's etcd
    # leader lease AND (ucx) the listener bind retry through TIME_WAIT —
    # ~65-90s total (F264 budget is 3s x 30). Readiness = info PRODUCED
    # OUTPUT: autumn-op's exit code is not a health signal (it exits
    # non-zero on benign per-partition discard-fetch warnings, e.g. when
    # probing ucx partition listeners over tcp).
    local tag="$1" deadline=$((SECONDS + 150))
    while [ $SECONDS -lt $deadline ]; do
        if [ -n "$("${AOC[@]}" info 2>/dev/null)" ]; then
            say "[$tag] manager ready"; return 0
        fi
        sleep 2
    done
    fail "[$tag] manager did not become ready within 150s"
    return 1
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

parts_on() {
    # Distinguish "probe failed" (return 1, no output) from a genuine 0
    # count — folding failures into 0 could pick the wrong E5 victim or
    # fake convergence (coco P2). Probe success = info produced output;
    # do NOT trust the exit code (non-zero on benign discard-fetch
    # warnings against ucx partition listeners).
    local out
    out=$("${AOC[@]}" info 2>/dev/null)
    [ -n "$out" ] || return 1
    printf '%s\n' "$out" | grep -c "ps=127.0.0.1:$1" || true
}

# ── E4: kill + respawn the manager mid-writes ───────────────────────────────
MGR_PID=$(pgrep -f autumn-manager-server | head -1)
MGR_CMD=$(tr '\0' ' ' < "/proc/$MGR_PID/cmdline")
say "E4: kill -9 manager pid=$MGR_PID"
kill -9 "$MGR_PID"
sleep 6
say "E4: respawn manager: $MGR_CMD"
setsid nohup $MGR_CMD > "$WORK/mgr_respawn.log" 2>&1 < /dev/null &
wait_mgr_ready "E4"
verify_seeds "after-mgr-kill-restart"
write_liveness "e4"

# ── E5: kill holder PS, then kill manager INSIDE the eviction window ────────
# Holder pick must come from a SUCCESSFUL probe with a non-zero total —
# a failed/empty probe folded into 0/0 would default to the wrong victim
# and fake the convergence check (coco P2).
P1_CNT=""; P2_CNT=""
for try in $(seq 1 15); do
    P1_CNT=$(parts_on 9301) && P2_CNT=$(parts_on 9351) \
        && [ $((${P1_CNT:-0} + ${P2_CNT:-0})) -gt 0 ] && break
    P1_CNT=""; P2_CNT=""
    sleep 2
done
if [ -z "$P1_CNT" ]; then
    fail "E5: could not determine partition holder (probe failed or 0/0)"
    P1_CNT=0; P2_CNT=0
fi
if [ "${P1_CNT:-0}" -ge "${P2_CNT:-0}" ]; then
    HOLDER_PSID=1; HOLDER_PORT=9301; SURV_PORT=9351
else
    HOLDER_PSID=2; HOLDER_PORT=9351; SURV_PORT=9301
fi
HOLDER_PID=$(pgrep -f -- "--psid $HOLDER_PSID .*--transport $T" | head -1)
say "E5: holder=PS$HOLDER_PSID ($P1_CNT/$P2_CNT parts on 9301/9351); kill -9 PS pid=$HOLDER_PID"
kill -9 "$HOLDER_PID"
sleep 3
MGR_PID=$(pgrep -f autumn-manager-server | head -1)
say "E5: kill -9 manager pid=$MGR_PID (mid-eviction)"
kill -9 "$MGR_PID"
sleep 4
say "E5: respawn manager"
setsid nohup $MGR_CMD > "$WORK/mgr_respawn2.log" 2>&1 < /dev/null &
wait_mgr_ready "E5"
deadline=$((SECONDS + 90))
while [ $SECONDS -lt $deadline ]; do
    left=$(parts_on "$HOLDER_PORT")
    [ "${left:-1}" = "0" ] && break
    sleep 2
done
left=$(parts_on "$HOLDER_PORT")
if [ "${left:-1}" != "0" ]; then
    fail "E5: eviction did not converge after manager restart ($left parts still on :$HOLDER_PORT)"
    "${AOC[@]}" info 2>/dev/null | grep "^  part" | head -8
else
    say "E5: interrupted eviction converged — all partitions on :$SURV_PORT"
fi
sleep 5
verify_seeds "after-E5-double-kill"
write_liveness "e5"
say "E5: respawn PS$HOLDER_PSID"
setsid nohup "$PSBIN" --psid "$HOLDER_PSID" --port "$HOLDER_PORT" --manager "$MGR" \
    --listen 127.0.0.1 --advertise "127.0.0.1:$HOLDER_PORT" --transport "$T" \
    > "$WORK/ps${HOLDER_PSID}_respawn2.log" 2>&1 < /dev/null &
sleep 8
verify_seeds "after-E5-ps-respawn"

# ── E6: randomized repeated kill rounds ─────────────────────────────────────
# Each round kills a random victim (EN / holder PS / manager), waits for
# convergence, respawns, and verifies. Repeated cycles stress CUMULATIVE
# state the one-shot events E1-E5 cannot: repeated ownership failback
# (owner_epoch must keep bumping, F265), part_addrs churn across many
# reopen waves, PS port-ordinal growth, region_epoch growth.
ROUNDS="${CHAOS_ROUNDS:-4}"
SEED="${CHAOS_SEED:-$$}"
RANDOM=$SEED
say "E6: $ROUNDS randomized rounds (seed=$SEED)"
for r in $(seq 1 "$ROUNDS"); do
    # F269: victims 3/4 are best-effort ORCHESTRATION ops (split/merge),
    # not kills — interleaved with kill rounds they compound state that
    # one-shot events never reach (deep split trees, repeated CoW chains,
    # dozens of owner-epoch bumps). Op rejections (has_overlap, non-
    # adjacent pair, <2 keys) are expected chaos noise, NOT failures.
    case $((RANDOM % 5)) in
        0) victim=en ;;
        1) victim=ps ;;
        2) victim=mgr ;;
        3) victim=split ;;
        4) victim=merge ;;
    esac
    say "E6.$r: victim=$victim"
    case $victim in
        split)
            mapfile -t CUR < <("${AOC[@]}" info 2>/dev/null | sed -n 's/^  part \([0-9]*\):.*/\1/p' | sort -n)
            if [ "${#CUR[@]}" -gt 0 ]; then
                tgt="${CUR[$((RANDOM % ${#CUR[@]}))]}"
                say "E6.$r: split part $tgt (best-effort)"
                "${AOC[@]}" split "$tgt" >/dev/null 2>&1 || say "E6.$r: split rejected (ok)"
                sleep 4
            fi
            ;;
        merge)
            mapfile -t CUR < <("${AOC[@]}" info 2>/dev/null | sed -n 's/^  part \([0-9]*\):.*/\1/p' | sort -n)
            if [ "${#CUR[@]}" -ge 2 ]; then
                i=$((RANDOM % (${#CUR[@]} - 1)))
                say "E6.$r: merge ${CUR[$((i+1))]} into ${CUR[$i]} (best-effort)"
                "${AOC[@]}" merge "${CUR[$i]}" "${CUR[$((i+1))]}" >/dev/null 2>&1 || say "E6.$r: merge rejected (ok)"
                sleep 4
            fi
            ;;
        en)
            EN_PID=$(ps -eo pid,comm | awk '$2=="autumn-extent-n"{print $1}' | head -1)
            EN_CMD=$(tr '\0' ' ' < "/proc/$EN_PID/cmdline")
            kill -9 "$EN_PID"
            sleep 6
            setsid nohup $EN_CMD > "$WORK/e6_${r}_en.log" 2>&1 < /dev/null &
            sleep 6
            ;;
        ps)
            # Kill whichever PS currently holds the most partitions; its
            # partitions must migrate to the other PS (repeated failback).
            p1=$(parts_on 9301) || p1=0; p2=$(parts_on 9351) || p2=0
            if [ "${p1:-0}" -ge "${p2:-0}" ]; then vid=1; vport=9301; sport=9351; else vid=2; vport=9351; sport=9301; fi
            VPID=$(pgrep -f -- "--psid $vid .*--transport $T" | head -1)
            say "E6.$r: kill PS$vid pid=${VPID:-?} ($p1/$p2 on 9301/9351)"
            [ -n "$VPID" ] && kill -9 "$VPID"
            deadline=$((SECONDS + 90))
            while [ $SECONDS -lt $deadline ]; do
                left=$(parts_on "$vport")
                [ "${left:-1}" = "0" ] && break
                sleep 2
            done
            left=$(parts_on "$vport")
            [ "${left:-1}" = "0" ] || fail "E6.$r: partitions did not migrate off PS$vid"
            setsid nohup "$PSBIN" --psid "$vid" --port "$vport" --manager "$MGR" \
                --listen 127.0.0.1 --advertise "127.0.0.1:$vport" --transport "$T" \
                > "$WORK/e6_${r}_ps${vid}.log" 2>&1 < /dev/null &
            sleep 6
            ;;
        mgr)
            MPID=$(pgrep -f autumn-manager-server | head -1)
            MCMD=$(tr '\0' ' ' < "/proc/$MPID/cmdline")
            kill -9 "$MPID"
            sleep 4
            setsid nohup $MCMD > "$WORK/e6_${r}_mgr.log" 2>&1 < /dev/null &
            wait_mgr_ready "E6.$r"
            ;;
    esac
    verify_seeds "E6.$r-$victim"
done
write_liveness "e6"

# ── E7: split/merge orchestration racing kills (F268) ───────────────────────
# E7a: issue a SPLIT on the partition holding the a-* keys, then kill -9
#      the hosting PS mid-flight (~0.7s in). Whatever the split's fate
#      (committed → both children reopen on the survivor; aborted → the
#      parent reopens), every seed key must stay readable and writable.
# E7b: issue an F185 orchestrated MERGE (two adjacent empty partitions),
#      then kill -9 the manager ~0.3s in — inside the freeze → etcd-commit
#      window. The PS-side FREEZE_TTL (30s) is the designed backstop;
#      after the manager respawns the cluster must serve again with the
#      merge either fully committed or fully rolled back.
# E6's merge ops (F269) can shrink the partition count below the
# original 4 — adapt: need >= 2; the a-* keys live in the upper-middle
# band, approximated by the 2/3 index in id (= key) order.
mapfile -t E7PARTS < <("${AOC[@]}" info 2>/dev/null | sed -n 's/^  part \([0-9]*\):.*/\1/p' | sort -n)
if [ "${#E7PARTS[@]}" -lt 2 ]; then
    fail "E7: could not enumerate >=2 partitions (got ${#E7PARTS[@]})"
else
    SPLIT_PART="${E7PARTS[$(( (${#E7PARTS[@]} * 2) / 3 ))]}"
    # Hosting PS: per-partition port < 9351 ⇒ PS1 band, else PS2 band.
    SP_PORT=$("${AOC[@]}" info 2>/dev/null | sed -n "s/^  part ${SPLIT_PART}: ps=127.0.0.1:\([0-9]*\).*/\1/p" | head -1)
    if [ -n "$SP_PORT" ] && [ "$SP_PORT" -lt 9351 ]; then SP_PSID=1; SP_BASE=9301; else SP_PSID=2; SP_BASE=9351; fi
    say "E7a: split part $SPLIT_PART (on PS$SP_PSID) + kill PS mid-flight"
    ( "${AOC[@]}" split "$SPLIT_PART" >/dev/null 2>&1 ) &
    sleep 0.7
    SP_PID=$(pgrep -f -- "--psid $SP_PSID .*--transport $T" | head -1)
    [ -n "$SP_PID" ] && kill -9 "$SP_PID"
    deadline=$((SECONDS + 90))
    while [ $SECONDS -lt $deadline ]; do
        left=$(parts_on "$SP_BASE")
        [ "${left:-1}" = "0" ] && break
        sleep 2
    done
    say "E7a: respawn PS$SP_PSID"
    setsid nohup "$PSBIN" --psid "$SP_PSID" --port "$SP_BASE" --manager "$MGR" \
        --listen 127.0.0.1 --advertise "127.0.0.1:$SP_BASE" --transport "$T" \
        > "$WORK/e7a_ps${SP_PSID}.log" 2>&1 < /dev/null &
    sleep 8
    verify_seeds "after-E7a-split-kill"
    write_liveness "e7a"

    MERGE_S="${E7PARTS[0]}"; MERGE_V="${E7PARTS[1]}"
    say "E7b: merge $MERGE_V into $MERGE_S + kill manager mid-freeze"
    ( "${AOC[@]}" merge "$MERGE_S" "$MERGE_V" >/dev/null 2>&1 ) &
    sleep 0.3
    MPID=$(pgrep -f autumn-manager-server | head -1)
    MCMD=$(tr '\0' ' ' < "/proc/$MPID/cmdline")
    kill -9 "$MPID"
    sleep 4
    say "E7b: respawn manager"
    setsid nohup $MCMD > "$WORK/e7b_mgr.log" 2>&1 < /dev/null &
    wait_mgr_ready "E7b"
    # FREEZE_TTL is 30s — give the frozen PSes time to self-unfreeze if
    # the merge never committed, then demand full service.
    sleep 5
    verify_seeds "after-E7b-merge-kill"
    write_liveness "e7b"
fi

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
