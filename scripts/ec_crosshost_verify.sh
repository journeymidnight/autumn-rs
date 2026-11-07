#!/usr/bin/env bash
# Cross-host verification of the v29 EC copy-on-write line.
#
# Everything in v29 (attempt nonce, named payload files, shard-holder
# lifecycle, the scheme switch, file-granular reconcile) is otherwise covered
# only on single-host loopback, which hides real network behaviour — e.g. an
# SO_RCVBUF window freeze that costs 6x cross-host write throughput is
# invisible on one host.
#
# Shape: manager + PS + EN0 on local (this host), EN1 + EN2 on remote. EC 2+1, so
# the conversion fans shards ACROSS the network and two of the three shard
# holders are remote. Then: convert, read back, restart every EN to force the
# reconcile, and verify the pre-conversion .dat is reclaimed on BOTH hosts.
set -u
# Two-host script. Host specifics come from the environment:
#   AUTUMN_XHOST_LOCAL / AUTUMN_XHOST_REMOTE  the two hosts' IPv6 addresses
#   AUTUMN_XHOST_SSH_PORT (default 22), AUTUMN_XHOST_USER (default root):
#                      how ssh/scp reach the remote host
#   AUTUMN_XHOST_WD    local scratch dir (default: a fresh mktemp -d)
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN=$ROOT/target/release
WD="${AUTUMN_XHOST_WD:-$(mktemp -d /tmp/autumn-xhost-local.XXXXXX)}"
RWD=/tmp/autumn-xhost
L6="${AUTUMN_XHOST_LOCAL:?set AUTUMN_XHOST_LOCAL (this host's IPv6)}"
R6="${AUTUMN_XHOST_REMOTE:?set AUTUMN_XHOST_REMOTE (the other host's IPv6)}"
SSH_PORT="${AUTUMN_XHOST_SSH_PORT:-22}"
SSH_USER="${AUTUMN_XHOST_USER:-root}"

MGR=30801; PS=30821
EN0=30811            # local
EN1=30812; EN2=30813 # remote

rm -rf "$WD"; mkdir -p "$WD"/{en0,ps1}
# One cluster secret for every server and autumn-op on both hosts.
SECRET="$WD/cluster.secret"; RSECRET="$RWD/cluster.secret"
( umask 077; head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$SECRET" )
PIDS=()

remote() { timeout 180 ssh -p "$SSH_PORT" "$SSH_USER@$R6" "$1" 2>&1; }
cleanup() {
  echo "--- cleanup ---"
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
  sleep 1
  for p in "${PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null; done
  remote "pkill -f 'autumn-extent-node --data $RWD' 2>/dev/null; true" >/dev/null
}
trap cleanup EXIT
OP(){ "$BIN/autumn-op" --manager "[$L6]:$MGR" --cluster-secret-file "$SECRET" "$@"; }

echo "=== ship binaries to the remote host ==="
remote "rm -rf $RWD/en1 $RWD/en2 && mkdir -p $RWD/bin $RWD/en1 $RWD/en2" >/dev/null
scp -q -P "$SSH_PORT" "$BIN/autumn-extent-node" "$BIN/autumn-op" "$SSH_USER@[$R6]:$RWD/bin/" \
  || { echo "SCP FAIL"; exit 1; }
scp -q -P "$SSH_PORT" "$SECRET" "$SSH_USER@[$R6]:$RSECRET" || { echo "SCP SECRET FAIL"; exit 1; }
remote "cd $RWD/bin && ls -la autumn-extent-node | awk '{print \$5, \$9}' && ldd autumn-extent-node >/dev/null && echo ldd-ok"

echo "=== manager on local ==="
"$BIN/autumn-manager-server" --port $MGR --listen "$L6" --cluster-secret-file "$SECRET" \
  > "$WD/manager.log" 2>&1 & PIDS+=($!)
sleep 3

echo "=== format + start EN0 (local), EN1/EN2 (remote) ==="
OP format "$WD/en0" > "$WD/format0.log" 2>&1 \
  || { echo "FORMAT0 FAIL"; cat "$WD/format0.log"; exit 1; }
"$BIN/autumn-extent-node" --data "$WD/en0" --port $EN0 --manager "[$L6]:$MGR" \
  --listen "$L6" --advertise "[$L6]:$EN0" --cpuset 100 --cluster-secret-file "$SECRET" \
  > "$WD/en0.log" 2>&1 & PIDS+=($!)

for i in 1 2; do
  p=$([ $i = 1 ] && echo $EN1 || echo $EN2)
  remote "cd $RWD && ./bin/autumn-op --manager '[$L6]:$MGR' --cluster-secret-file $RSECRET format $RWD/en$i > $RWD/format$i.log 2>&1 && echo FORMAT$i-OK" | tail -1
  remote "cd $RWD && setsid nohup ./bin/autumn-extent-node --data $RWD/en$i --port $p \
      --manager '[$L6]:$MGR' --listen '$R6' --advertise '[$R6]:$p' --cpuset $((100+i)) \
      --cluster-secret-file $RSECRET \
      > $RWD/en$i.log 2>&1 < /dev/null & sleep 1; echo EN$i-STARTED" | tail -1
done
sleep 4
echo "--- nodes as the manager sees them ---"
OP list-nodes 2>&1 | head -6

echo "=== bootstrap RF1 + log-ec 2+1 ==="
OP bootstrap --replication 1+0 --log-ec 2+1 > "$WD/bootstrap.log" 2>&1 \
  || { echo BOOTSTRAP-FAIL; cat "$WD/bootstrap.log"; exit 1; }
"$BIN/autumn-ps" --psid 1 --port $PS --manager "[$L6]:$MGR" \
  --listen "$L6" --advertise "[$L6]:$PS" --cluster-secret-file "$SECRET" > "$WD/ps.log" 2>&1 & PIDS+=($!)
sleep 4
# namespace-create cuts at bench/, which needs the partition open on the PS.
for _ in $(seq 60); do OP info 2>/dev/null | grep -q 'slots  ready' && break; sleep 1; done
OP namespace-create --name bench >/dev/null 2>&1 || { echo NAMESPACE-FAIL; exit 1; }

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
echo "  local en0: $(find "$WD/en0" -name "extent-$converted.*" -printf '%f(%s) ' 2>/dev/null)"
remote "for i in 1 2; do printf '  remote en%s: ' \$i; find $RWD/en\$i -name 'extent-$converted.*' -printf '%f(%s) ' 2>/dev/null; echo; done" | grep 'remote'

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
  --listen "$L6" --advertise "[$L6]:$EN0" --cpuset 100 --cluster-secret-file "$SECRET" \
  > "$WD/en0.restart.log" 2>&1 & PIDS+=($!)
for i in 1 2; do
  p=$([ $i = 1 ] && echo $EN1 || echo $EN2)
  remote "cd $RWD && setsid nohup ./bin/autumn-extent-node --data $RWD/en$i --port $p \
      --manager '[$L6]:$MGR' --listen '$R6' --advertise '[$R6]:$p' --cpuset $((100+i)) \
      --cluster-secret-file $RSECRET \
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
echo "  local en0: $(find "$WD/en0" -name "extent-$converted.*" -printf '%f(%s) ' 2>/dev/null)"
R_OUT=$(remote "for i in 1 2; do printf '  remote en%s: ' \$i; find $RWD/en\$i -name 'extent-$converted.*' -printf '%f(%s) ' 2>/dev/null; echo; done")
echo "$R_OUT" | grep 'remote'
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
  remote "for i in 1 2; do echo \"  remote en\$i restart log:\"; tail -4 $RWD/en\$i.restart.log 2>/dev/null | sed 's/^/    /'; done"
  echo "  ps log:"; tail -6 "$WD/ps.log" 2>/dev/null | sed 's/^/    /'
  echo "RESULT: FAIL — post-cleanup cross-host read broken"; exit 1; }

echo "RESULT: PASS"
