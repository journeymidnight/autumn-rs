#!/usr/bin/env bash
# fuse_en_restart_chaos.sh — does EN (data-plane) kill+restart corrupt/lose
# fuse data?  Complements fuse_chaos.sh (PS/manager/fuse restart) and
# fuse_rmw_chaos.sh (RMW-GET-SWALLOW). An EN kill does NOT migrate partitions
# (the EN is the data plane, not the partition owner), so the stream layer just
# fails over reads/writes to the surviving replicas.
#
# Focus = INTEGRITY (correctness), not availability: each EN is killed with a
# SHORT down window (respawn after 2 s) and every verify runs once the cluster
# is HEALTHY again, so the ~30 s in-flight stall that an EN kill causes (the
# dead replica isn't fast-failed within the rpc/bridge 30 s window) does not
# dominate the run. See fuse CLAUDE.md "EN restart" note for that availability
# characteristic — it is slow-but-correct, not data loss.
#
# Invariants after every EN kill+restart (and a final remount):
#   DURABLE READ:  write-once+fsync'd files (4 KiB .. 10 MiB incl. multi-extent)
#                  read back sha-exact.
#   RMW INTEGRITY: files repeatedly partial-overwritten (mount + a local MIRROR
#                  advanced in lockstep on each successful write) stay == mirror.
#
# Usage: AUTUMN_DATA_ROOT=/data05/autumn-eni ./scripts/fuse_en_restart_chaos.sh
set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MGR="127.0.0.1:9001"; MNT="/mnt/autumn-fuse-eni"
WORK="$(mktemp -d /tmp/fuse_eni.XXXXXX)"; FAIL=0
say(){ echo "[eni $(date +%H:%M:%S)] $*"; }
fail(){ echo "[eni $(date +%H:%M:%S)] FAIL: $*"; FAIL=1; }
umnt(){ local i; for i in 1 2 3 4 5 6; do grep -q " $MNT " /proc/mounts || return 0; umount -l "$MNT" 2>/dev/null; sleep 0.2; done; }
mnt(){ mkdir -p "$MNT"; setsid nohup "$ROOT/target/release/autumn-fuse" --manager "$MGR" --mountpoint "$MNT" --transport tcp > "$WORK/fuse_$1.log" 2>&1 </dev/null &
  local i; for i in $(seq 1 30); do mountpoint -q "$MNT" && return 0; sleep 1; done; fail "mount $1"; return 1; }

export AUTUMN_DATA_ROOT="${AUTUMN_DATA_ROOT:-/data05/autumn-eni}"
say "starting cluster (3 EN, presplit 4; work=$WORK)"; umnt
for pid in $(ps -eo pid,comm | awk '$2 ~ /^(autumn-|etcd)/ {print $1}'); do kill -9 "$pid" 2>/dev/null; done; sleep 2
for i in $(seq 1 35); do b=$(ss -tan 2>/dev/null|grep -cE ':(9001|9301|2000[0-9]) ')||true; [ "${b:-0}" = 0 ]&&break; sleep 2; done
rm -rf "$AUTUMN_DATA_ROOT" /tmp/autumn-rs
env AUTUMN_EXTENT_BASE_PORT=20000 AUTUMN_BOOTSTRAP_PRESPLIT="4:hexstring" AUTUMN_TRANSPORT=tcp bash "$ROOT/cluster.sh" start 3 > "$WORK/cluster.log" 2>&1
grep -q "bootstrap succeeded" "$WORK/cluster.log" || { echo "cluster start failed"; tail -20 "$WORK/cluster.log"; exit 1; }
sleep 3; mnt boot || exit 1

: > "$WORK/durable.sha"
for sz in 4096 65536 262144 1048576 4194304 10485760; do
    n="d$sz"; head -c "$sz" /dev/urandom > "$WORK/$n.src"; cp "$WORK/$n.src" "$MNT/$n.bin"
    echo "$(sha256sum < "$WORK/$n.src"|cut -d' ' -f1) $n.bin" >> "$WORK/durable.sha"
done
for i in 1 2 3 4; do head -c 2097152 /dev/urandom > "$MNT/r$i.bin"; done; sync
for i in 1 2 3 4; do timeout 30 cp "$MNT/r$i.bin" "$WORK/r$i.mirror"; done
say "seeded 6 durable + 4 RMW files"

verify(){ local tag="$1" bad=0
  while read -r want name; do local g; g=$(timeout 30 sha256sum "$MNT/$name" 2>/dev/null|cut -d' ' -f1)
    [ "$g" != "$want" ]&&{ fail "[$tag] durable $name mismatch"; bad=$((bad+1)); }; done < "$WORK/durable.sha"
  for i in 1 2 3 4; do timeout 30 cmp -s "$MNT/r$i.bin" "$WORK/r$i.mirror" || { fail "[$tag] RMW r$i != mirror"; bad=$((bad+1)); }; done
  [ $bad -eq 0 ]&&say "[$tag] integrity OK (6 durable + 4 RMW == mirror)"; }
# CLEAN RMW overwrites on a HEALTHY cluster — mount + mirror in lockstep.
rmw_clean(){ local i blk; for i in 1 2 3 4; do for _ in 1 2; do blk=$((RANDOM%32)); head -c 65536 /dev/urandom > "$WORK/p"
    if timeout 30 dd if="$WORK/p" of="$MNT/r$i.bin" bs=65536 seek="$blk" count=1 conv=notrunc 2>/dev/null; then
      dd if="$WORK/p" of="$WORK/r$i.mirror" bs=65536 seek="$blk" count=1 conv=notrunc 2>/dev/null
    else fail "rmw_clean dd r$i failed on HEALTHY cluster"; fi; done; done; }
verify baseline; rmw_clean; verify after-clean-rmw

en_ports=(20001 20002 20003)
for r in 1 2 3 4; do
  ep=${en_ports[$(( r % 3 ))]}; EPID=$(pgrep -f "autumn-extent-node --port $ep"|head -1)
  if [ -n "$EPID" ]; then
    ECMD=$(tr '\0' ' ' < "/proc/$EPID/cmdline")
    say "round $r: kill -9 EN :$ep pid=$EPID, respawn in 2s (short down)"
    kill -9 "$EPID"; sleep 2
    setsid nohup $ECMD > "$WORK/en_${ep}_r$r.log" 2>&1 </dev/null &
    for i in $(seq 1 30); do pgrep -f "autumn-extent-node --port $ep" >/dev/null && break; sleep 1; done
    sleep 12   # let the EN rejoin + replicas resync
    verify "r$r-read"; rmw_clean; verify "r$r-rmw"
  fi
done
say "final: remount + verify"; FP=$(pgrep -f "autumn-fuse --manager"|head -1); [ -n "$FP" ]&&kill -9 "$FP"; sleep 1; umnt; mnt final || exit 1
verify final
umnt; for pid in $(ps -eo pid,comm | awk '$2 ~ /^(autumn-|etcd)/ {print $1}'); do kill -9 "$pid" 2>/dev/null; done
[ $FAIL -eq 0 ]&&say "PASS ($WORK)"||say "FAILED ($WORK)"; exit $FAIL
