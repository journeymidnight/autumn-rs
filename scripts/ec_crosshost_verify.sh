#!/usr/bin/env bash
# Cross-host verification of the v29 EC copy-on-write line.
#
# Everything in v29 (attempt nonce, named payload files, shard-holder
# lifecycle, the scheme switch, file-granular reconcile) has so far been
# verified only on single-host loopback. This project has repeatedly been
# bitten by things loopback hides — the SO_RCVBUF window freeze that cost 6x
# cross-host write throughput was invisible on one box.
#
# Shape: manager + PS + EN0 on ::14 (this host), EN1 + EN2 on ::15. EC 2+1, so
# the conversion fans shards ACROSS the network and two of the three shard
# holders are remote. Then: convert, read back, restart every EN to force the
# reconcile, and verify the pre-conversion .dat is reclaimed on BOTH hosts.
set -u
ROOT=/data/dongmao_dev/autumn-rs
BIN=$ROOT/target/release
WD=/tmp/claude-0/-data-dongmao-dev-autumn-rs/f4f9e821-5881-4019-b33f-100fb011951a/scratchpad/xhost
RWD=/tmp/autumn-xhost
L6=fdbd:dc62:3:302::14
R6=fdbd:dc62:3:302::15
RUN="$ROOT/.claude/skills/remote-autumn/remote-autumn.sh"

MGR=30801; PS=30821
EN0=30811            # local
EN1=30812; EN2=30813 # remote

rm -rf "$WD"; mkdir -p "$WD"/{en0,ps1}
TOK="$WD/admin.token"; head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$TOK"
PIDS=()

remote() { timeout 180 "$RUN" "$1" 2>&1; }
cleanup() {
  echo "--- cleanup ---"
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
  sleep 1
  for p in "${PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null; done
  remote "pkill -f 'autumn-extent-node --data $RWD' 2>/dev/null; true" >/dev/null
}
trap cleanup EXIT
OP(){ "$BIN/autumn-op" --manager "[$L6]:$MGR" --admin-token-file "$TOK" "$@"; }

echo "=== ship v29 binaries to ::15 (its release tree is months stale) ==="
remote "rm -rf $RWD/en1 $RWD/en2 && mkdir -p $RWD/bin $RWD/en1 $RWD/en2" >/dev/null
scp -q -P 2222 "$BIN/autumn-extent-node" "$BIN/autumn-op" "root@[$R6]:$RWD/bin/" \
  || { echo "SCP FAIL"; exit 1; }
remote "cd $RWD/bin && ls -la autumn-extent-node | awk '{print \$5, \$9}' && ldd autumn-extent-node >/dev/null && echo ldd-ok"

echo "=== manager on ::14 ==="
"$BIN/autumn-manager-server" --port $MGR --listen "$L6" --admin-token-file "$TOK" \
  > "$WD/manager.log" 2>&1 & PIDS+=($!)
sleep 3

echo "=== format + start EN0 (::14), EN1/EN2 (::15) ==="
"$BIN/autumn-op" --manager "[$L6]:$MGR" format "$WD/en0" > "$WD/format0.log" 2>&1 \
  || { echo "FORMAT0 FAIL"; cat "$WD/format0.log"; exit 1; }
"$BIN/autumn-extent-node" --data "$WD/en0" --port $EN0 --manager "[$L6]:$MGR" \
  --listen "$L6" --advertise "[$L6]:$EN0" --cpuset 100 > "$WD/en0.log" 2>&1 & PIDS+=($!)

for i in 1 2; do
  p=$([ $i = 1 ] && echo $EN1 || echo $EN2)
  remote "cd $RWD && ./bin/autumn-op --manager '[$L6]:$MGR' format $RWD/en$i > $RWD/format$i.log 2>&1 && echo FORMAT$i-OK" | tail -1
  remote "cd $RWD && setsid nohup ./bin/autumn-extent-node --data $RWD/en$i --port $p \
      --manager '[$L6]:$MGR' --listen '$R6' --advertise '[$R6]:$p' --cpuset $((100+i)) \
      > $RWD/en$i.log 2>&1 < /dev/null & sleep 1; echo EN$i-STARTED" | tail -1
done
sleep 4
echo "--- nodes as the manager sees them ---"
OP list-nodes 2>&1 | head -6

echo "=== bootstrap RF1 + log-ec 2+1 ==="
OP bootstrap --replication 1+0 --log-ec 2+1 > "$WD/bootstrap.log" 2>&1 \
  || { echo BOOTSTRAP-FAIL; cat "$WD/bootstrap.log"; exit 1; }
"$BIN/autumn-ps" --psid 1 --port $PS --manager "[$L6]:$MGR" --data "$WD/ps1" \
  --listen "$L6" --advertise "[$L6]:$PS" > "$WD/ps.log" 2>&1 & PIDS+=($!)
sleep 4

PID=$(OP --json info 2>/dev/null | python3 -c 'import sys,json;print(json.load(sys.stdin)["partitions"][0]["part_id"])' 2>/dev/null)
echo "partition = $PID"

echo "=== write 8 x 64 KiB (values land in the log stream behind VPs) ==="
head -c 65536 /dev/urandom > "$WD/val.bin"
SRC_SHA=$(sha256sum "$WD/val.bin" | cut -d' ' -f1)
for k in $(seq 1 8); do
  "$BIN/autumn-client" --manager "[$L6]:$MGR" --namespace bench put "k$k" "$WD/val.bin" \
    > "$WD/put.log" 2>&1 || { echo "PUT k$k FAIL"; cat "$WD/put.log"; exit 1; }
done
sleep 1

echo "=== split to seal the log tail ==="
OP split "$PID" --at-raw-hex 80 --wait --timeout 60 2>&1 | tail -1
sleep 2

SEALED=$(OP --json info --part "$PID" 2>/dev/null | python3 -c '
import sys,json
d=json.load(sys.stdin)
print(" ".join(str(e["extent_id"]) for e in d.get("extents",[]) if not e.get("open") and not e.get("ec")))
' 2>/dev/null)
echo "sealed non-EC extents: $SEALED"

echo "=== force-ec-convert (shards fan ACROSS hosts) ==="
converted=""
for eid in $SEALED; do
  out=$(OP force-ec-convert --extent "$eid" --wait --timeout 180 2>&1); rc=$?
  echo "  extent $eid rc=$rc: $(echo "$out" | tail -1)"
  if [ $rc -eq 0 ] && echo "$out" | grep -q succeeded; then converted=$eid; break; fi
done
[ -n "$converted" ] || { echo "RESULT: FAIL — no extent converted"; exit 1; }
echo "############ CONVERTED extent $converted ############"

echo "--- on-disk shape across BOTH hosts (pre-cleanup) ---"
echo "  ::14 en0: $(find "$WD/en0" -name "extent-$converted.*" -printf '%f(%s) ' 2>/dev/null)"
remote "for i in 1 2; do printf '  ::15 en%s: ' \$i; find $RWD/en\$i -name 'extent-$converted.*' -printf '%f(%s) ' 2>/dev/null; echo; done" | grep '::15'

echo "=== read back after the flip ==="
rf=0
for k in $(seq 1 8); do
  "$BIN/autumn-client" --manager "[$L6]:$MGR" --namespace bench get-stream --out "$WD/got.bin" "k$k" >/dev/null 2>&1 \
    || { echo "  GET k$k FAILED"; rf=1; continue; }
  [ "$(sha256sum "$WD/got.bin" | cut -d' ' -f1)" = "$SRC_SHA" ] || { echo "  GET k$k CORRUPT"; rf=1; }
done
[ $rf = 0 ] && echo "  all 8 byte-identical across hosts after the flip" \
            || { echo "RESULT: FAIL — post-flip cross-host read broken"; exit 1; }

echo "=== restart every EN so the reconcile runs at once (cleanup) ==="
# Restart the ENs ONLY. The PS must stay up — it is what serves the reads
# being verified below. (An earlier version of this line killed PIDS[-1],
# which IS the PS, and then reported the reads as broken.)
for q in $(pgrep -f "autumn-extent-node --data $WD/en0"); do kill "$q" 2>/dev/null; done
remote "pkill -f 'autumn-extent-node --data $RWD' 2>/dev/null; true" >/dev/null
sleep 2
"$BIN/autumn-extent-node" --data "$WD/en0" --port $EN0 --manager "[$L6]:$MGR" \
  --listen "$L6" --advertise "[$L6]:$EN0" --cpuset 100 > "$WD/en0.restart.log" 2>&1 & PIDS+=($!)
for i in 1 2; do
  p=$([ $i = 1 ] && echo $EN1 || echo $EN2)
  remote "cd $RWD && setsid nohup ./bin/autumn-extent-node --data $RWD/en$i --port $p \
      --manager '[$L6]:$MGR' --listen '$R6' --advertise '[$R6]:$p' --cpuset $((100+i)) \
      > $RWD/en$i.restart.log 2>&1 < /dev/null & sleep 1; echo EN$i-RESTARTED" | tail -1
done
echo "--- waiting for all 3 ENs to come back Online ---"
back=0
for _ in $(seq 1 30); do
  n=$(OP --json list-nodes 2>/dev/null | python3 -c '
import sys,json
d=json.load(sys.stdin)
ns=d if isinstance(d,list) else d.get("nodes",[])
print(sum(1 for x in ns if str(x.get("auto_state","")).lower().startswith("online")))
' 2>/dev/null || echo 0)
  [ "${n:-0}" -ge 3 ] && { back=1; break; }
  sleep 2
done
OP list-nodes 2>&1 | head -5
[ "$back" = 1 ] || echo "  WARNING: not all ENs returned Online — reads below may fail for that reason"
sleep 3

echo "--- on-disk shape across BOTH hosts (post-cleanup) ---"
L_DAT=$(find "$WD/en0" -name "extent-$converted.dat" 2>/dev/null | wc -l)
echo "  ::14 en0: $(find "$WD/en0" -name "extent-$converted.*" -printf '%f(%s) ' 2>/dev/null)"
R_OUT=$(remote "for i in 1 2; do printf '  ::15 en%s: ' \$i; find $RWD/en\$i -name 'extent-$converted.*' -printf '%f(%s) ' 2>/dev/null; echo; done")
echo "$R_OUT" | grep '::15'
R_DAT=$(remote "find $RWD/en1 $RWD/en2 -name 'extent-$converted.dat' 2>/dev/null | wc -l" | tail -1 | tr -d ' \r')

if [ "$L_DAT" = 0 ] && [ "${R_DAT:-9}" = 0 ]; then
  echo "  every pre-conversion .dat reclaimed on BOTH hosts"
else
  echo "RESULT: FAIL — .dat survived (local=$L_DAT remote=$R_DAT)"; exit 1
fi

echo "=== read back again, served purely from shard files across hosts ==="
rf=0
for k in $(seq 1 8); do
  if ! "$BIN/autumn-client" --manager "[$L6]:$MGR" --namespace bench get-stream --out "$WD/got2.bin" "k$k" > "$WD/get2.err" 2>&1; then
    echo "  GET k$k FAILED: $(tail -2 "$WD/get2.err" | tr '\n' ' ')"; rf=1; continue
  fi
  [ "$(sha256sum "$WD/got2.bin" | cut -d' ' -f1)" = "$SRC_SHA" ] || { echo "  GET k$k CORRUPT"; rf=1; }
done
[ $rf = 0 ] && echo "  all 8 still byte-identical with no .dat anywhere in the cluster" \
            || {
  echo "--- diagnostics ---"
  echo "  local en0 restart log:"; tail -4 "$WD/en0.restart.log" 2>/dev/null | sed 's/^/    /'
  remote "for i in 1 2; do echo \"  ::15 en\$i restart log:\"; tail -4 $RWD/en\$i.restart.log 2>/dev/null | sed 's/^/    /'; done"
  echo "  ps log:"; tail -6 "$WD/ps.log" 2>/dev/null | sed 's/^/    /'
  echo "RESULT: FAIL — post-cleanup cross-host read broken"; exit 1; }

echo "RESULT: PASS"
