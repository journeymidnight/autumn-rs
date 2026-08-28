#!/usr/bin/env bash
# LIVE check: run an EC conversion on a real cluster and watch its progress.
#
# Spins an isolated 4-EN cluster (EC 3+1 needs four shard targets) with its own
# etcd and port band, fills until the 1 GiB log extent ROLLS — only a roll seals
# an extent; a PS restart just replays and keeps appending to the same open tail
# — then converts the sealed extent and polls `ops status` once a second.
#
# Expected shape (measured 2026-08-28, 1 GiB extent, EC 3+1, loopback):
#     t+ 5s  running    --      0/0            marker acquired, encoding not started
#     t+ 6s  running   18.3%   67108864/366304837
#     t+10s  running   36.6%  134217728/366304837
#     ...                                      one 64 MiB stripe per sample
#     t+22s  succeeded 100.0%  366304837/366304837
# The denominator is THIS node's shard (ceil(extent / K)), not the extent.
#
#   cargo build --workspace
#   bash scripts/ec_convert_progress.sh
set -u
cd /data/dongmao_dev/autumn-rs
BIN=target/debug
W=/tmp/ec-prog; PB=22000; TOK=ectok
MGR="127.0.0.1:$((PB+1))"; PS=$((PB+201)); DASH=$((PB+301)); ETCD=$((PB+401))
AO="$BIN/autumn-op --admin-token $TOK --manager $MGR"
rm -rf "$W"; mkdir -p "$W"
PIDS=(); cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; }
trap cleanup EXIT
for p in $ETCD $((ETCD+1)) $((PB+1)) $PS $DASH 22101 22102 22103 22104; do
  o=$(ss -ltnp 2>/dev/null | grep ":$p " | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2)
  [ -n "${o:-}" ] && kill -9 "$o" 2>/dev/null
done; sleep 1
wp(){ for _ in $(seq 1 30); do ss -ltn 2>/dev/null | grep -q ":$1\b" && return 0; sleep 1; done; return 1; }

etcd --name ecp --data-dir "$W/etcd" \
  --listen-client-urls "http://127.0.0.1:$ETCD" --advertise-client-urls "http://127.0.0.1:$ETCD" \
  --listen-peer-urls "http://127.0.0.1:$((ETCD+1))" --initial-advertise-peer-urls "http://127.0.0.1:$((ETCD+1))" \
  --initial-cluster "ecp=http://127.0.0.1:$((ETCD+1))" >"$W/etcd.log" 2>&1 & PIDS+=($!)
wp $ETCD || { echo FAIL-etcd; exit 1; }
"$BIN/autumn-manager-server" --port $((PB+1)) --listen 127.0.0.1 --admin-token $TOK \
  --etcd "127.0.0.1:$ETCD" >"$W/mgr.log" 2>&1 & PIDS+=($!)
wp $((PB+1)) || { echo FAIL-mgr; tail -5 "$W/mgr.log"; exit 1; }

# EC 3+1 needs FOUR shard targets, so four extent nodes.
for i in 1 2 3 4; do
  mkdir -p "$W/en$i"
  $AO format "$W/en$i" >"$W/fmt$i.log" 2>&1 || { echo "FAIL-format$i"; cat "$W/fmt$i.log"; exit 1; }
  "$BIN/autumn-extent-node" --data "$W/en$i" --port $((22100+i)) --manager "$MGR" --cpuset $((i-1)) \
    --advertise "127.0.0.1:$((22100+i))" --listen 127.0.0.1 >"$W/en$i.log" 2>&1 & PIDS+=($!)
  wp $((22100+i)) || { echo "FAIL-en$i"; tail -5 "$W/en$i.log"; exit 1; }
done
sleep 4
$AO bootstrap --replication 3+0 >"$W/boot.log" 2>&1 || { echo FAIL-boot; cat "$W/boot.log"; exit 1; }
"$BIN/autumn-ps" --psid 1 --port $PS --manager "$MGR" --data "$W/ps1" \
  --listen 127.0.0.1 --advertise "127.0.0.1:$PS" \
  --max-extent-size-bytes $((1024*1024*1024)) >"$W/ps.log" 2>&1 & PIDS+=($!)
wp $PS || { echo FAIL-ps; tail -5 "$W/ps.log"; exit 1; }
sleep 5

# Layer-A namespace registry is active whenever it is non-empty, and bootstrap
# always seeds fs/kvc/mem — so an unregistered first key segment is rejected
# even with authz off. perf-check writes under `bench/perf`.
$AO namespace-create --name bench --admin-token $TOK >"$W/ns.log" 2>&1 || true

# Only a ROLL seals an extent — a PS restart replays and keeps appending to the
# same open tail (verified). So cap extents at 1 GiB and write just past it:
# exactly one roll, which leaves one big SEALED extent to convert.
echo "--- fill until one 1 GiB roll ---"
# HARD timeout on the bench client: perf-check does not exit reliably once the
# log extent rolls. The cluster stays healthy through it (the roll completes,
# the new tail's replicas agree, the manager keeps probing) — the CLIENT is what
# hangs. All this step owes us is a rolled, sealed extent, so cap it and move on.
timeout -s KILL 35 "$BIN/autumn-client" --manager "$MGR" perf-check --threads 8 \
  --duration 16 --size 8388608 \
  --partitions 1 --pipeline-depth 4 >"$W/fill.log" 2>&1 || true
grep -E "Total data|Ops/sec" "$W/fill.log" | head -2
sleep 5

OPJ="$BIN/autumn-op --admin-token $TOK --json --manager $MGR"

echo "--- find the biggest SEALED extent ---"
$OPJ overview >"$W/ov.json" 2>&1
PARTS=$(python3 -c "
import json;d=json.load(open('$W/ov.json'))
print(' '.join(str(p['part_id']) for p in d.get('partitions',[])[:40]))" 2>/dev/null)
echo "partitions: $PARTS"
: > "$W/cand.txt"
for pid in $PARTS; do
  $OPJ info --part "$pid" >"$W/info_$pid.json" 2>/dev/null
  python3 - "$W/info_$pid.json" "$pid" >>"$W/cand.txt" 2>/dev/null <<'PY2'
import json,sys
try: d=json.load(open(sys.argv[1]))
except Exception: raise SystemExit
log_stream = d.get("log_stream_id", 0)
for e in d.get("extents") or []:
    # `open` false = sealed; `ec` true = already converted.
    if not e.get("open", True) and not e.get("ec") and e.get("role") == "log":
        print(sys.argv[2], e.get("extent_id"), e.get("size", 0), log_stream)
PY2
done
read -r PID EXT BYTES STREAM < <(sort -k3 -nr "$W/cand.txt" | head -1)
echo "chosen: part=$PID extent=$EXT bytes=$BYTES stream=$STREAM"
[ -z "${EXT:-}" ] && {
  echo "FAIL: no sealed extent. extents seen:"
  for f in "$W"/info_*.json; do python3 -c "
import json,sys
d=json.load(open(sys.argv[1]))
for e in d.get('extents') or []:
    print('   ', {k:e.get(k) for k in ('extent_id','role','open','ec','size')})" "$f"; done
  exit 1; }

echo "--- set stream EC 3+1 + force convert ---"
$AO set-stream-ec --stream "$STREAM" --ec 3+1 2>&1 | tail -2
OUT=$($AO force-ec-convert --extent "$EXT" 2>&1); echo "$OUT"
OPID=$(echo "$OUT" | grep -oE '[0-9]{6,}' | head -1)
echo "op_id=$OPID"

echo "--- poll progress ---"
for i in $(seq 1 90); do
  R=$($OPJ ops status "$OPID" 2>/dev/null)
  echo "$R" | python3 -c "
import json,sys
try: o=json.load(sys.stdin)['ops'][0]
except Exception: raise SystemExit
d,t,st=o.get('progress_done',0),o.get('progress_total',0),o.get('state')
pct = f'{d/t*100:5.1f}%' if t else '   -- '
print(f'  t+{sys.argv[1]:>3}s  state={st:<9} {pct}  {d}/{t}')
" "$i"
  echo "$R" | grep -q '"state": "succeeded"\|"state": "failed"' && break
  sleep 1
done
echo "--- final ---"
$OPJ ops status "$OPID" 2>/dev/null | head -c 400; echo
