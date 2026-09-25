#!/usr/bin/env bash
# fuse_dead_peer_chaos.sh — a connection whose peer stopped answering, while
# TCP says everything is fine, must not wedge fuse reads.
#
# That is the shape of the 2026-09-22 incident: fuse reads answered EIO after
# 30 s for hours; /proc/net/tcp showed every connection ESTABLISHED with empty
# queues and no retransmits.
#
# Two scenarios, both on one cluster (SCENARIOS picks; default both on TCP):
#
#   mgr-freeze  The mount reaches the manager through scripts/freeze_proxy.py.
#               Every flow open at the freeze stops forwarding with its sockets
#               left open; a NEW connection gets through. Then every partition
#               is split, so each worker's routing is stale and the next read of
#               a moved key needs a region refresh from the manager — over the
#               frozen connection. The refresh used the SDK's full 30 s and so
#               always lost the race to fuse's 30 s REPLY_TIMEOUT: the call was
#               cancelled before its own timeout could evict the connection, and
#               every later read met the same dead connection. TCP only (the
#               relay cannot carry UCX).
#
#   en-stop     SIGSTOP one extent node: its kernel keeps ACKing and even
#               completes new handshakes. The manager marks the node Suspected
#               once its heartbeats stop, so descriptors stop naming it — this
#               one recovers through that path too and is a no-regression
#               check, not a discriminating one.
#
# Invariants per scenario:
#   LIVE:   once GRACE_SECS have passed since the fault, every read of every
#           file succeeds with the right sha256.
#   INTACT: after the fault is lifted, every file reads back sha-exact.
#
# Kills only what it started (cluster.sh pid files, its own fuse and relay), so
# a mount or cluster belonging to another tree on this host is left alone.
#
# Usage: AUTUMN_DATA_ROOT=/data05/autumn-dpd ./scripts/fuse_dead_peer_chaos.sh
#   SCENARIOS="mgr-freeze en-stop" FAULT_SECS=90 GRACE_SECS=30
#   AUTUMN_TRANSPORT=ucx AUTUMN_BIND_HOST='[<RoCE IP>]'  (en-stop only)
set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$ROOT/target/release"
MGR="${AUTUMN_BIND_HOST:-127.0.0.1}:9001"; MNT="${MNT:-/mnt/autumn-fuse-dpd}"
TRANSPORT="${AUTUMN_TRANSPORT:-tcp}"
if [ "$TRANSPORT" = tcp ]; then SCENARIOS="${SCENARIOS:-mgr-freeze en-stop}"; else SCENARIOS="${SCENARIOS:-en-stop}"; fi
FAULT_SECS="${FAULT_SECS:-90}"; GRACE_SECS="${GRACE_SECS:-30}"
NFILES="${NFILES:-6}"; FILE_MIB="${FILE_MIB:-16}"
PROXY_PORT="${PROXY_PORT:-19001}"
WORK="$(mktemp -d /tmp/fuse_dpd.XXXXXX)"; FAIL=0; FUSE_PID=""; PROXY_PID=""; STOPPED_PID=""
export AUTUMN_DATA_ROOT="${AUTUMN_DATA_ROOT:-/data05/autumn-dpd}"
say(){ echo "[dpd $(date +%H:%M:%S)] $*"; }
fail(){ echo "[dpd $(date +%H:%M:%S)] FAIL: $*"; FAIL=1; }
umnt(){ local i; for i in 1 2 3 4 5 6; do grep -q " $MNT " /proc/mounts || return 0; umount -l "$MNT" 2>/dev/null; sleep 0.2; done; }
cleanup(){
  [ -n "$STOPPED_PID" ] && kill -CONT "$STOPPED_PID" 2>/dev/null
  [ -n "$FUSE_PID" ] && kill -9 "$FUSE_PID" 2>/dev/null
  [ -n "$PROXY_PID" ] && kill -9 "$PROXY_PID" 2>/dev/null
  umnt; bash "$ROOT/cluster.sh" stop > /dev/null 2>&1
}
trap cleanup EXIT
case " $SCENARIOS " in *" mgr-freeze "*) [ "$TRANSPORT" = tcp ] || { echo "mgr-freeze needs TCP"; exit 2; };; esac

say "starting cluster (3 EN, presplit 4, transport=$TRANSPORT, scenarios: $SCENARIOS; work=$WORK)"
umnt; bash "$ROOT/cluster.sh" stop > /dev/null 2>&1
rm -rf "$AUTUMN_DATA_ROOT"
env AUTUMN_EXTENT_BASE_PORT=20000 AUTUMN_BOOTSTRAP_PRESPLIT="4:hexstring" AUTUMN_TRANSPORT="$TRANSPORT" \
  bash "$ROOT/cluster.sh" start 3 > "$WORK/cluster.log" 2>&1
grep -q "bootstrap succeeded" "$WORK/cluster.log" || { echo "cluster start failed"; tail -20 "$WORK/cluster.log"; exit 1; }
sleep 3
OP=("$BIN/autumn-op" --manager "$MGR" --admin-token-file "$AUTUMN_DATA_ROOT/authz/admin.token")

MOUNT_MGR="$MGR"
if [ "$TRANSPORT" = tcp ]; then
  python3 "$SCRIPT_DIR/freeze_proxy.py" "$PROXY_PORT" "$MGR" > "$WORK/proxy.log" 2>&1 &
  PROXY_PID=$!
  sleep 0.5
  MOUNT_MGR="127.0.0.1:$PROXY_PORT"
fi
mkdir -p "$MNT"
# The fuse is launched here, not by cluster.sh, so it needs the same UCX env
# cluster.sh gives its own children (one positive TLS list, pinned device).
[ "$TRANSPORT" = ucx ] && export UCX_TLS="${UCX_TLS:-rc_mlx5,ud_mlx5,tcp,self}" UCX_NET_DEVICES="${UCX_NET_DEVICES:-mlx5_1:1}"
setsid nohup "$BIN/autumn-fuse" --manager "$MOUNT_MGR" --mountpoint "$MNT" --transport "$TRANSPORT" \
  > "$WORK/fuse.log" 2>&1 </dev/null &
FUSE_PID=$!
for _ in $(seq 1 30); do mountpoint -q "$MNT" && break; sleep 1; done
mountpoint -q "$MNT" || { fail "mount"; exit 1; }

: > "$WORK/want.sha"
for i in $(seq 1 "$NFILES"); do
  head -c $((FILE_MIB * 1048576)) /dev/urandom > "$WORK/f$i.src"
  cp "$WORK/f$i.src" "$MNT/f$i.bin" || fail "seed f$i"
  echo "$(sha256sum < "$WORK/f$i.src" | cut -d' ' -f1) f$i.bin" >> "$WORK/want.sha"
  rm -f "$WORK/f$i.src"
done
sync
say "seeded $NFILES x ${FILE_MIB} MiB"

# One pass over every file, all files CONCURRENTLY; prints
# "<secs> <ok|BAD|ERR> <name>" per read. Concurrent on purpose: several requests
# then share each read worker's connections, and a frozen peer's kernel ACKs
# every one of them — the fault has to be detected through that traffic, not in
# the quiet gap a one-at-a-time reader would leave.
read_one(){
  local want="$1" name="$2" t0 t1 got dt
  t0=$(date +%s.%N)
  got=$(timeout 120 sha256sum "$MNT/$name" 2>/dev/null | cut -d' ' -f1)
  t1=$(date +%s.%N)
  dt=$(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.2f", b - a }')
  if [ -z "$got" ]; then echo "$dt ERR $name"
  elif [ "$got" != "$want" ]; then echo "$dt BAD $name"
  else echo "$dt ok $name"; fi
}
read_pass(){
  local d pids=() want name; d=$(mktemp -d "$WORK/pass.XXXXXX")
  while read -r want name; do read_one "$want" "$name" > "$d/$name" & pids+=($!); done < "$WORK/want.sha"
  # The reads only: a bare `wait` would also wait for the relay and the mount.
  wait "${pids[@]}"
  cat "$d"/*; rm -rf "$d"
}

# Read continuously for FAULT_SECS from T_FAULT, then judge the reads that
# started after the grace window. $1 = scenario tag.
read_through_fault(){
  local tag="$1" out="$WORK/$1.txt"
  : > "$out"
  while [ $(( $(date +%s) - T_FAULT )) -lt "$FAULT_SECS" ]; do
    read_pass | while read -r secs verdict name; do
      echo "$(( $(date +%s) - T_FAULT )) $secs $verdict $name"
    done >> "$out"
  done
  say "[$tag] reads during the fault ($(wc -l < "$out") total):"
  awk -v g="$GRACE_SECS" '{ph = ($1 < g) ? "grace" : "after"; n[ph]++; if ($3 != "ok") bad[ph]++;
    if ($2 > mx[ph]) mx[ph] = $2} END {for (p in n) printf "  %-5s reads=%d failed=%d max=%.1fs\n", p, n[p], bad[p]+0, mx[p]}' "$out"
  local after_n after_bad
  after_n=$(awk -v g="$GRACE_SECS" '$1 >= g' "$out" | wc -l)
  after_bad=$(awk -v g="$GRACE_SECS" '$1 >= g && $3 != "ok"' "$out" | wc -l)
  [ "$after_n" -gt 0 ] || fail "[$tag] no read completed after the grace window"
  [ "$after_bad" -eq 0 ] || fail "[$tag] $after_bad reads failed after the ${GRACE_SECS}s grace window"
}

intact(){
  local tag="$1"
  read_pass > "$WORK/$tag-after.txt"
  grep -qv " ok " "$WORK/$tag-after.txt" && fail "[$tag] after the fault: $(grep -v ' ok ' "$WORK/$tag-after.txt" | head -3)"
  say "[$tag] after the fault: $(grep -c ' ok ' "$WORK/$tag-after.txt")/$NFILES files sha-exact"
}

read_pass > "$WORK/baseline.txt"
grep -qv " ok " "$WORK/baseline.txt" && fail "baseline read: $(grep -v ' ok ' "$WORK/baseline.txt" | head -3)"
say "baseline: max $(sort -n "$WORK/baseline.txt" | tail -1 | cut -d' ' -f1)s per ${FILE_MIB} MiB file"

for scenario in $SCENARIOS; do
  case "$scenario" in
  mgr-freeze)
    kill -USR1 "$PROXY_PID"; sleep 0.2
    say "[mgr-freeze] $(tail -1 "$WORK/proxy.log") between the mount and the manager"
    T_FAULT=$(date +%s)
    PARTS=$("${OP[@]}" --json info | python3 -c 'import json,sys; print(" ".join(str(p["part_id"]) for p in json.load(sys.stdin)["partitions"]))')
    for p in $PARTS; do
      # A partition holding no keys has nothing to split, and nothing to read.
      if ! "${OP[@]}" split "$p" --wait > "$WORK/split_$p.log" 2>&1; then
        grep -q "fewer than 2 in-range keys" "$WORK/split_$p.log" \
          || fail "[mgr-freeze] split $p: $(tail -1 "$WORK/split_$p.log")"
      fi
    done
    say "[mgr-freeze] split partitions: $PARTS -> $("${OP[@]}" --json info | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["partitions"]))') partitions"
    read_through_fault mgr-freeze
    # Lift: restarting the relay closes the frozen flows, which any client
    # notices without help.
    kill -9 "$PROXY_PID"; wait "$PROXY_PID" 2>/dev/null
    python3 "$SCRIPT_DIR/freeze_proxy.py" "$PROXY_PORT" "$MGR" >> "$WORK/proxy.log" 2>&1 &
    PROXY_PID=$!
    sleep 5
    intact mgr-freeze
    ;;
  en-stop)
    EP=20001
    STOPPED_PID=$(pgrep -f "$BIN/autumn-extent-node --port $EP" | head -1)
    [ -n "$STOPPED_PID" ] || { fail "no EN on :$EP"; continue; }
    say "[en-stop] SIGSTOP EN :$EP pid=$STOPPED_PID for ${FAULT_SECS}s (grace ${GRACE_SECS}s)"
    kill -STOP "$STOPPED_PID"
    T_FAULT=$(date +%s)
    read_through_fault en-stop
    kill -CONT "$STOPPED_PID"; STOPPED_PID=""
    sleep 5
    intact en-stop
    ;;
  *) fail "unknown scenario $scenario" ;;
  esac
done

[ $FAIL -eq 0 ] && say "PASS ($WORK)" || say "FAILED ($WORK)"
exit $FAIL
