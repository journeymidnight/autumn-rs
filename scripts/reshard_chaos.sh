#!/usr/bin/env bash
# reshard_chaos.sh — F-EN-DYNSHARD M3: does changing an extent-node's shard
# count (a "reshard") preserve ALL data with ZERO movement?
#
# A reshard is stop-the-world PER NODE: stop the EN, restart it with a different
# core count (`--cpuset`), and it self-registers its new `shard_ports[]` (M1a).
# The manager keys by node_uuid and updates the location in place (M0), so
# routing (`shard_addr_for_extent` = extent_id % shard_count) remaps — but the
# on-disk layout is hashed by crc32c(extent_id), NOT by shard, so NO bytes move;
# a different shard just opens the same file.
#
# This test proves the correctness half: write a known corpus, then reshard
# 2->4->1 following the design §4 STOP-THE-WORLD runbook (stop PS -> stop+restart
# EN with a new --cpuset -> restart PS with a fresh StreamClient), and after each
# reshard read EVERY key back byte-exact. The PS restart is load-bearing, not
# incidental: the EN read/append path REJECTS a wrong-shard request (only
# ALLOC/RECOVERY/DELETE forward), so the routing cache must be rebuilt — which is
# exactly why the runbook restarts PSes. Single EN, RF=1, so the reshard is
# isolated from replication; un-flushed data rides log_stream (WAL) and replays
# on PS reopen, so stopping the PS loses nothing.
#
# Env traps handled: AUTUMN_EXTENT_BASE_PORT=20000 dodges Ray's 10002-19999
# worker ports (which squat the default EN control base 10101); TCP transport
# avoids the UCX loopback df-flap.
#
# Usage: AUTUMN_DATA_ROOT=/data05/autumn-reshard ./scripts/reshard_chaos.sh
set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MGR="127.0.0.1:9001"
BIN="$ROOT/target/release"
CLIENT="$BIN/autumn-client"
OP="$BIN/autumn-op"
WORK="$(mktemp -d /tmp/reshard.XXXXXX)"
FAIL=0
NKEYS="${RESHARD_NKEYS:-300}"
EN_PORT=20001
# Cpuset (=> shard count) for each phase. Defaults use high cores to dodge a
# busy low-core tenant (Ray) on a shared box; override on a clean/small host,
# e.g. RESHARD_CPUSET2=0-1 RESHARD_CPUSET4=0-3 RESHARD_CPUSET1=0.
CPUSET2="${RESHARD_CPUSET2:-180-181}"   # initial 2 shards
CPUSET4="${RESHARD_CPUSET4:-180-183}"   # reshard to 4
CPUSET1="${RESHARD_CPUSET1:-180}"       # reshard to 1
say(){ echo "[reshard $(date +%H:%M:%S)] $*"; }
fail(){ echo "[reshard $(date +%H:%M:%S)] FAIL: $*"; FAIL=1; }

kill_all(){ for pid in $(ps -eo pid,comm | awk '$2 ~ /^(autumn-|etcd)/ {print $1}'); do kill -9 "$pid" 2>/dev/null; done; }
trap 'kill_all' EXIT

# --- shard count of node 1 as the manager sees it (M1c list-nodes echo) ---
shard_count(){ "$OP" --manager "$MGR" --json list-nodes 2>/dev/null \
  | python3 -c 'import json,sys
try:
  d=json.load(sys.stdin); print(d[0]["shard_count"] if d else -1)
except Exception: print(-1)'; }

# --- relaunch the EN with a new cpuset (=> new shard count), same everything else ---
relaunch_en(){  # $1 = cpuset spec, e.g. "180-183"
  local newcpu="$1" epid cmd i
  epid=$(pgrep -f "autumn-extent-node --port $EN_PORT" | head -1)
  [ -z "$epid" ] && { fail "EN not running before reshard to $newcpu"; return 1; }
  cmd=$(tr '\0' ' ' < "/proc/$epid/cmdline")
  # drop the existing --cpuset <spec> (2 tokens); we re-add a fresh one.
  cmd=$(echo "$cmd" | sed -E 's/--cpuset [0-9,-]+ ?//')
  say "reshard: kill EN pid=$epid, relaunch --cpuset $newcpu"
  kill -9 "$epid"
  for i in $(seq 1 30); do pgrep -f "autumn-extent-node --port $EN_PORT" >/dev/null || break; sleep 0.5; done
  sleep 1  # let the old shard listeners' TIME_WAIT settle before rebind
  setsid nohup $cmd --cpuset "$newcpu" > "$WORK/en_cpuset_${newcpu//,/_}.log" 2>&1 </dev/null & disown
  for i in $(seq 1 40); do pgrep -f "autumn-extent-node --port $EN_PORT" >/dev/null && break; sleep 1; done
  sleep 6  # let it self-register + the manager df tick pick up the new shard_ports
}

# Stop-the-world reshard, matching design §4 runbook: R1 stop PS (quiesce
# writers; un-flushed data is in log_stream/WAL, replays on reopen) -> R2/R4
# stop+restart the EN with the new shard count -> R5/R6 manager reconciles via
# the EN's self-register (verify list-nodes) -> R7 restart PS (fresh
# StreamClient -> new routing; the read/append path REJECTS wrong-shard rather
# than forwarding, so fresh routing is required) -> R8 read back every key.
reshard_to(){  # $1 = cpuset, $2 = expected shard count, $3 = tag
  local newcpu="$1" want="$2" tag="$3" ppid pcmd sc
  ppid=$(pgrep -f "autumn-ps --psid" | head -1)
  [ -z "$ppid" ] && { fail "[$tag] PS not running"; return 1; }
  pcmd=$(tr '\0' ' ' < "/proc/$ppid/cmdline")
  say "[$tag] R1: stop PS pid=$ppid (quiesce)"
  kill -9 "$ppid"; sleep 2
  relaunch_en "$newcpu"                          # R2 + R4
  sc=$(shard_count); say "[$tag] shard count now: $sc"   # R5/R6
  [ "$sc" = "$want" ] || fail "[$tag] expected $want shards, got $sc"
  say "[$tag] R7: restart PS (fresh routing)"
  setsid nohup $pcmd > "$WORK/ps_${tag}.log" 2>&1 </dev/null & disown
  # Wait for the PS to re-register + reopen the partition (replay) before reading.
  local ready=0
  for i in $(seq 1 40); do
    if "$CLIENT" --manager "$MGR" get "k1" >/dev/null 2>&1; then ready=1; break; fi
    sleep 1
  done
  [ "$ready" = 1 ] || fail "[$tag] PS did not become readable within 40 s after restart"
  read_verify "$tag"                              # R8
}

read_verify(){  # $1 = tag
  local tag="$1" bad=0 got g
  while read -r want key; do
    "$CLIENT" --manager "$MGR" get "$key" > "$WORK/got" 2>/dev/null
    g=$(sha256sum "$WORK/got" | cut -d' ' -f1)
    if [ "$g" != "$want" ]; then bad=$((bad+1)); [ $bad -le 5 ] && fail "[$tag] $key mismatch (got sha $g)"; fi
  done < "$WORK/expected.sha"
  if [ $bad -eq 0 ]; then say "[$tag] read-back OK — all $NKEYS keys byte-exact"; else fail "[$tag] $bad/$NKEYS keys wrong"; fi
}

# ============================ bring-up ============================
say "clean + start (1 EN, 2 shards, RF=1, base port 20000, TCP; work=$WORK)"
kill_all
for i in $(seq 1 35); do b=$(ss -tan 2>/dev/null | grep -cE ':(9001|9301|2000[0-9]|2001[0-9]|2002[0-9]|2003[0-9]) ') || true; [ "${b:-0}" = 0 ] && break; sleep 2; done
export AUTUMN_DATA_ROOT="${AUTUMN_DATA_ROOT:-/tmp/autumn-reshard}"
rm -rf "$AUTUMN_DATA_ROOT"
env AUTUMN_EXTENT_BASE_PORT=20000 AUTUMN_TRANSPORT=tcp \
    AUTUMN_EXTENT_SHARDS=2 AUTUMN_EN1_CPUSET="$CPUSET2" \
    AUTUMN_DATA_ROOT="$AUTUMN_DATA_ROOT" \
    bash "$ROOT/cluster.sh" start 1 > "$WORK/cluster.log" 2>&1
if ! grep -q "bootstrap succeeded" "$WORK/cluster.log"; then
  echo "cluster start FAILED"; tail -30 "$WORK/cluster.log"; exit 1
fi
sleep 3

# ============================ seed corpus ============================
say "writing $NKEYS keys (every 5th is 8 KiB -> large-value VP in log_stream)"
: > "$WORK/expected.sha"
for i in $(seq 1 "$NKEYS"); do
  if (( i % 5 == 0 )); then sz=8192; else sz=200; fi
  yes "k$i-payload-$RANDOM" | head -c "$sz" > "$WORK/v"
  if ! "$CLIENT" --manager "$MGR" put "k$i" "$WORK/v" >/dev/null 2>&1; then fail "put k$i"; fi
  echo "$(sha256sum "$WORK/v" | cut -d' ' -f1) k$i" >> "$WORK/expected.sha"
done
say "seeded; forcing a flush window"; sleep 3

sc=$(shard_count); say "shard count before reshard: $sc"
[ "$sc" = 2 ] || fail "expected 2 shards before reshard, got $sc"
read_verify baseline

# ============ stop-the-world reshard 2 -> 4, then 4 -> 1 ============
reshard_to "$CPUSET4" 4 "2to4"
reshard_to "$CPUSET1" 1 "4to1"

# ============================ verdict ============================
if [ $FAIL -eq 0 ]; then say "PASS — reshard 2->4->1 preserved all $NKEYS keys, zero movement ($WORK)"; else say "FAILED ($WORK)"; fi
exit $FAIL
